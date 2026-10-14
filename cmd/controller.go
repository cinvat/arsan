package cmd

import (
	"context"
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

		c, err := app.NewController(app.Config{
			SchedulerMaxAttempts:   viper.GetInt("scheduler.max-attempts"),
			ComputeSweepInterval:   viper.GetDuration("compute.sweep-interval"),
			ComputeDriverOpTimeout: viper.GetDuration("compute.driver-operation-timeout"),
		}, commands, log)
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
	}

	for _, h := range handlers {
		stop, err := c.Commands.Subscribe(ctx, h.topic, h.fn)
		if err != nil {
			return err
		}
		defer stop()
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
		log.Info("node up", "node", node.ID, "address", node.Address, "driver", node.Driver)
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
	controllerCmd.Flags().Int("scheduler.max-attempts", 5, "placement attempts before giving up")
	controllerCmd.Flags().Duration("compute.sweep-interval", 30*time.Second, "how often to reconcile stranded instances")
	controllerCmd.Flags().Duration("compute.driver-operation-timeout", 5*time.Minute, "deadline for a single compute engine call")

	rootCmd.AddCommand(controllerCmd)
}
