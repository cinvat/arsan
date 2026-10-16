package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/cinvat/arsan/internal/app"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var controllerCmd = &cobra.Command{
	Use:   "controller",
	Short: "Run the control plane",
	Long: `Run the control plane: placement, the instance state machine, and node
liveness.

The controller takes no HTTP requests. Work arrives over the command bus, which
the api publishes to, so the two are separate processes in production and
neither imports the other's libraries.`,
	PreRunE: func(cmd *cobra.Command, _ []string) error {
		return viper.BindPFlags(cmd.Flags())
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		log := newLogger()

		commands, err := bus.ConnectNATS(viper.GetString("bus.url"), 10*time.Second)
		if err != nil {
			return err
		}
		defer commands.Close()

		store, err := mustStore()
		if err != nil {
			return err
		}

		c, err := app.NewController(app.Config{
			SchedulerMaxAttempts:   viper.GetInt("scheduler.max-attempts"),
			ComputeSweepInterval:   viper.GetDuration("compute.sweep-interval"),
			ComputeDriverOpTimeout: viper.GetDuration("compute.driver-operation-timeout"),
		}, store, commands, log)
		if err != nil {
			return err
		}

		if err := subscribe(ctx, c, log); err != nil {
			return err
		}

		log.Info("controller started", "bus", viper.GetString("bus.url"))
		return c.Run(ctx, log)
	},
}

// subscribe registers one handler per command topic.
//
// Every handler is idempotent. JetStream redelivers a message whose ack never
// arrived, and a crash between "side effect done" and the ack runs it again, so
// the compare-and-swap transitions are what make a repeat a no-op.
func subscribe(ctx context.Context, c *app.Controller, log app.Logger) error {
	type handler struct {
		topic string
		fn    func(context.Context, bus.Command) error
	}

	handlers := []handler{
		{bus.TopicNodeRegistered, onNodeRegistered(c, log)},
		{bus.TopicInstanceCreate, onInstanceCreate(c, log)},
		{bus.TopicInstanceDelete, onInstanceDelete(c, log)},
		{bus.TopicInstanceStart, onInstanceStart(c, log)},
		{bus.TopicInstanceStop, onInstanceStop(c, log)},
		{bus.TopicInstanceMigrate, onInstanceMigrate(c, log)},
		{bus.TopicInstanceAttachDisk, onInstanceAttachDisk(c, log)},
		{bus.TopicInstanceDetachDisk, onInstanceDetachDisk(c, log)},
		{bus.TopicInstanceAttachCdrom, onInstanceAttachCdrom(c, log)},
		{bus.TopicInstanceDetachCdrom, onInstanceDetachCdrom(c, log)},
		{bus.TopicInstanceSetBoot, onInstanceSetBoot(c, log)},
	}

	for _, h := range handlers {
		// No stop is kept: the subscriptions live as long as the process, and
		// commands.Close (deferred by the caller) stops them all. A
		// `defer stop()` here would fire when this function returns —
		// milliseconds after subscribing — and silently deafen the
		// controller. That is exactly what it did.
		if _, err := c.Commands.Subscribe(ctx, h.topic, h.fn); err != nil {
			return err
		}
		log.Info("subscribed", "topic", h.topic)
	}
	return nil
}

// onNodeRegistered records the agent's endpoint and probes it.
//
// Until the probe answers, the node is NOTREADY and the scheduler's state
// filter excludes it, so nothing can be placed on a host the controller has not
// yet reached. Readiness is set by the probe, never by the api and never by the
// arrival of this command.
func onNodeRegistered(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		if err := app.CheckAgent(c.Store, cmd.Target); err != nil {
			// Retryable: the node row is written, so a later delivery can
			// still register the driver. A NAK here is correct.
			return err
		}
		node, err := c.Store.GetNode(ctx, cmd.Target)
		if err != nil {
			return nil // gone; nothing to announce
		}
		log.Info("node probed", "node", node.ID, "state", node.State,
			"reason", node.NotReadyReason)
		return nil
	}
}

// onInstanceCreate builds an instance the api has already persisted as PENDING.
//
// It re-reads the record rather than trusting anything in the command, so a
// redelivery after the instance has already advanced does not rebuild it.
func onInstanceCreate(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		inst, err := c.Store.Get(ctx, cmd.Target)
		if err != nil {
			log.Error("build: read failed", "instance", cmd.Target, "err", err)
			return err
		}
		if inst.State != core.StatePending {
			// Already claimed by an earlier delivery. Ack, so this becomes a
			// no-op instead of a second build.
			return nil
		}
		if err := c.Compute.Build(ctx, inst); err != nil {
			log.Error("build failed", "instance", inst.ID, "err", err)
			return err
		}
		return nil
	}
}

func init() {
	controllerCmd.Flags().String("bus.url", "nats://127.0.0.1:4222", "NATS URL for the command bus")
	controllerCmd.Flags().String("db", "", "shared database: postgres:// URL, SQLite file path, or empty for memory (dev only)")
	controllerCmd.Flags().Int("scheduler.max-attempts", 5, "placement attempts before giving up")
	controllerCmd.Flags().Duration("compute.sweep-interval", 30*time.Second, "how often to reconcile stranded instances")
	controllerCmd.Flags().Duration("compute.driver-operation-timeout", 5*time.Minute, "deadline for a single compute engine call")

	rootCmd.AddCommand(controllerCmd)
}

// A missing instance means the work is already done: the delete raced a
// redelivery, or the row is gone. Ack it instead of failing forever.
func notFoundIsGone(err error) error {
	if err != nil && errors.Is(err, core.ErrInstanceNotFound) {
		return nil
	}
	return err
}

// An illegal transition is permanent: the state machine rejected the command
// for this state, and redelivering it changes nothing. Log it and ack, so one
// mistimed request does not wedge the consumer behind endless redelivery.
func invalidIsParked(log app.Logger, what, id string, err error) error {
	if err != nil && errors.Is(err, core.ErrInvalidTransition) {
		log.Warn("command parked: illegal transition", "command", what, "instance", id, "err", err)
		return nil
	}
	return notFoundIsGone(err)
}

func onInstanceDelete(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		if err := c.Compute.Delete(ctx, cmd.Target); err != nil {
			log.Error("delete failed", "instance", cmd.Target, "err", err)
			return notFoundIsGone(err)
		}
		return nil
	}
}

func onInstanceStart(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		if err := c.Compute.Start(ctx, cmd.Target); err != nil {
			log.Error("start failed", "instance", cmd.Target, "err", err)
			return invalidIsParked(log, "start", cmd.Target, err)
		}
		return nil
	}
}

func onInstanceStop(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		if err := c.Compute.Stop(ctx, cmd.Target); err != nil {
			log.Error("stop failed", "instance", cmd.Target, "err", err)
			return invalidIsParked(log, "stop", cmd.Target, err)
		}
		return nil
	}
}

func onInstanceMigrate(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		var payload struct {
			TargetNodeID string `json:"target_node_id"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			log.Error("migrate dropped: bad payload", "instance", cmd.Target, "err", err)
			return nil
		}
		if payload.TargetNodeID == "" {
			log.Error("migrate dropped: no target", "instance", cmd.Target)
			return nil
		}
		if err := c.Compute.Migrate(ctx, cmd.Target, payload.TargetNodeID); err != nil {
			log.Error("migrate failed", "instance", cmd.Target, "err", err)
			return invalidIsParked(log, "migrate", cmd.Target, err)
		}
		return nil
	}
}

// Storage handlers decode their payload once and hand the rest to compute.
// Bad payloads are dropped (a poison message must not spin forever);
// illegal states are parked; a missing instance means the work is gone.
func onInstanceAttachDisk(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		var payload core.CreateDiskRequest
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			log.Error("attach-disk dropped: bad payload", "instance", cmd.Target, "err", err)
			return nil
		}
		if err := c.Compute.AttachDisk(ctx, cmd.Target, payload); err != nil {
			log.Error("attach-disk failed", "instance", cmd.Target, "err", err)
			return invalidIsParked(log, "attach-disk", cmd.Target, err)
		}
		return nil
	}
}

func onInstanceDetachDisk(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		var payload struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil || payload.Name == "" {
			log.Error("detach-disk dropped: bad payload", "instance", cmd.Target, "err", err)
			return nil
		}
		if err := c.Compute.DetachDisk(ctx, cmd.Target, payload.Name); err != nil {
			log.Error("detach-disk failed", "instance", cmd.Target, "err", err)
			return invalidIsParked(log, "detach-disk", cmd.Target, err)
		}
		return nil
	}
}

func onInstanceAttachCdrom(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		var payload struct {
			Image string `json:"image"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil || payload.Image == "" {
			log.Error("attach-cdrom dropped: bad payload", "instance", cmd.Target, "err", err)
			return nil
		}
		if err := c.Compute.AttachCdrom(ctx, cmd.Target, payload.Image); err != nil {
			log.Error("attach-cdrom failed", "instance", cmd.Target, "err", err)
			return invalidIsParked(log, "attach-cdrom", cmd.Target, err)
		}
		return nil
	}
}

func onInstanceDetachCdrom(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		if err := c.Compute.DetachCdrom(ctx, cmd.Target); err != nil {
			log.Error("detach-cdrom failed", "instance", cmd.Target, "err", err)
			return invalidIsParked(log, "detach-cdrom", cmd.Target, err)
		}
		return nil
	}
}

func onInstanceSetBoot(c *app.Controller, log app.Logger) func(context.Context, bus.Command) error {
	return func(ctx context.Context, cmd bus.Command) error {
		var payload struct {
			Device string `json:"device"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil || payload.Device == "" {
			log.Error("set-boot dropped: bad payload", "instance", cmd.Target, "err", err)
			return nil
		}
		if err := c.Compute.SetBootDevice(ctx, cmd.Target, payload.Device); err != nil {
			log.Error("set-boot failed", "instance", cmd.Target, "err", err)
			return invalidIsParked(log, "set-boot", cmd.Target, err)
		}
		return nil
	}
}
