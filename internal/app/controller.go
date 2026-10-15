// Package app wires the api and the controller separately.
//
// The two are deliberately NOT built together here. The api assembles a store,
// a bus and the REST surface; the controller assembles the store, the bus and
// the business-logic services. Neither imports the other's libraries, which is
// what makes "api never places" structural instead of conventional.
//
// In production they are separate processes talking over NATS and PostgreSQL.
// A single binary with cobra subcommands is only how they are started here.
package app

import (
	"context"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/compute"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver/remote"
	"github.com/cinvat/arsan/internal/scheduler"
	"github.com/cinvat/arsan/internal/store/memory"
)

// Config is the runtime configuration, read from viper by the caller.
type Config struct {
	SchedulerMaxAttempts   int
	ComputeSweepInterval   time.Duration
	ComputeDriverOpTimeout time.Duration
}

// Logger is the narrow logging surface services accept.
type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Controller is the business-logic half: placement, the instance state machine,
// and node liveness.
type Controller struct {
	Store     *memory.Store
	Scheduler *scheduler.Scheduler
	Compute   *compute.Service
	Commands  bus.Bus
	log       Logger
}

// NewController assembles the control plane and subscribes it to the command
// bus. It touches no controller library from the api's side, because the api
// never calls it.
func NewController(cfg Config, commands bus.Bus, log Logger) (*Controller, error) {
	store := memory.New()

	c := &Controller{
		Store:    store,
		Commands: commands,
		Scheduler: scheduler.New(scheduler.Config{
			Instances:   store,
			Nodes:       store,
			DriverFor:   drivers(commands),
			MaxAttempts: cfg.SchedulerMaxAttempts,
			Log:         log,
		}),
		Compute: compute.New(compute.Config{
			Instances:              store,
			Nodes:                  store,
			DriverFor:              drivers(commands),
			DriverOperationTimeout: cfg.ComputeDriverOpTimeout,
			SweepInterval:          cfg.ComputeSweepInterval,
			Log:                    log,
		}),
		log: log,
	}
	return c, nil
}

// drivers resolves a node's driver by dialing that node's agent.
//
// The controller does not run a compute engine. It has no libvirt, no local
// KVM, nothing to place a VM on directly: every VM lives on some agent, and the
// controller's entire job is to choose which agent and then command it. So this
// returns a core.Driver that forwards to the agent's gRPC service, which is
// where the real engine (fake, kvm, esxi) actually runs.
//
// There is deliberately no controller-side engine choice. The engine is a
// property of the node, declared by the agent when it bootstrapped and recorded
// on the node record. A controller flag naming an engine would be a second copy
// of a fact the database already holds, and the two would drift.
//
// A node's address is read from the node record rather than from an in-process
// cache: the cache is filled only by whichever controller consumed
// drivers resolves a node's driver. The node id IS the address
// (agent.<node-id>), so there is nothing to look up, cache, or rediscover:
// a controller that has never seen this node commands it as well as one that
// just consumed its registration.
func drivers(commands bus.Bus) func(string) (core.Driver, error) {
	return func(nodeID string) (core.Driver, error) {
		return remote.New(nodeID, commands), nil
	}
}

// CheckAgent verifies the node record exists before probing it.
//
// It deliberately does NOT change readiness. The bus command only tells the
// controller an agent exists; whether it can actually run a VM is settled by
// ProbeAgent, and conflating the two would make a host READY on the strength of
// a registration it has not yet answered.
func CheckAgent(store *memory.Store, nodeID string) error {
	_, err := store.GetNode(context.Background(), nodeID)
	return err
}

// ProbeAgent asks an agent whether it can actually host a VM, and records the
// answer.
//
// The probe calls usage rather than a dedicated health method, because usage
// is a real round trip through the compute engine: a host whose libvirt socket
// is unreachable is NOTREADY for VMs, even though its agent process is
// perfectly alive. A liveness check that only proved the process was up would
// put such a host into the scheduler's eligible set.
//
// A passing probe also fetches what the agent runs and what its host holds,
// and records it on the node. That is how the controller learns capacity: the
// agent never describes itself at bootstrap.
//
// It returns nil whether the answer was yes or no; the outcome is recorded on
// the node. A caller that wants to know should read the node back.
func (c *Controller) ProbeAgent(ctx context.Context, nodeID string) error {
	d, err := c.driverForNode(nodeID)
	if err != nil {
		return c.setReadiness(ctx, nodeID, err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	usage, err := d.Usage(probeCtx)
	if err != nil {
		return c.setReadiness(ctx, nodeID, err)
	}
	// The probe is also the agent's only chance to report usage, since the
	// agent bootstraps once and does not otherwise talk to the api. Recording
	// it here keeps node.Usage honest without a second, redundant call.
	if _, err := c.Store.RecordUsage(ctx, nodeID, usage, time.Now().UTC()); err != nil {
		c.log.Warn("record usage", "node", nodeID, "err", err)
	}
	// Capacity and engine arrive here, not at bootstrap: the agent should not
	// have to know its host to join, and whatever it knew would be stale the
	// moment the host changed.
	if rd, ok := d.(*remote.Driver); ok {
		if engine, total, err := rd.Info(probeCtx); err == nil {
			if _, err := c.Store.SetNodeSpec(ctx, nodeID, engine, total); err != nil {
				c.log.Warn("record spec", "node", nodeID, "err", err)
			}
		} else {
			c.log.Warn("fetch spec", "node", nodeID, "err", err)
		}
	}
	return c.setReadiness(ctx, nodeID, nil)
}

// setReadiness records the probe outcome. An operator-disabled node is left
// alone: readiness is about the host, and the drain decision was theirs.
func (c *Controller) setReadiness(ctx context.Context, nodeID string, cause error) error {
	store := c.Store
	node, err := store.GetNode(ctx, nodeID)
	if err != nil {
		return err
	}
	if !node.Enabled {
		// Keep whatever reason is already recorded rather than replacing it
		// with a probe failure, which would misattribute an operator's
		// decision to a broken agent.
		return nil
	}
	if cause == nil {
		if node.State.Ready() && node.NotReadyReason == "" {
			return nil // already ready for the same reason
		}
		if node.State == core.NodeNotReady {
			c.log.Info("node ready", "node", nodeID)
		}
		_, err = store.SetReadiness(ctx, nodeID, core.NodeReady, "")
		return err
	}
	reason := cause.Error()
	if node.State == core.NodeNotReady && node.NotReadyReason == reason {
		return nil // unchanged; do not log the same failure every sweep
	}
	if _, err := store.SetReadiness(ctx, nodeID, core.NodeNotReady, reason); err != nil {
		return err
	}
	c.log.Error("node not ready", "node", nodeID, "reason", reason)
	return nil
}

// driverForNode resolves a node's driver. It is the same lookup the
// scheduler and compute service use; the probe just calls it directly.
func (c *Controller) driverForNode(nodeID string) (core.Driver, error) {
	return drivers(c.Commands)(nodeID)
}

const probeTimeout = 10 * time.Second

// Run starts the background loops and blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context, log Logger) error {
	go func() {
		if err := c.ProbeLoop(ctx); err != nil && ctx.Err() == nil {
			log.Error("probe loop stopped", "err", err)
		}
	}()
	go func() {
		if err := c.Compute.SweepLoop(ctx); err != nil && ctx.Err() == nil {
			log.Error("sweep loop stopped", "err", err)
		}
	}()
	<-ctx.Done()
	return nil
}

// ProbeInterval is how often agents are re-probed. Readiness is not a
// one-time event: a host that answered at registration can stop answering, and
// without re-probing it would stay READY and keep attracting VMs it cannot run.
const ProbeInterval = 15 * time.Second

// ProbeLoop probes every node until ctx is cancelled.
func (c *Controller) ProbeLoop(ctx context.Context) error {
	t := time.NewTicker(ProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := c.ProbeAll(ctx); err != nil && ctx.Err() == nil {
				c.log.Error("probe sweep", "err", err)
			}
		}
	}
}

// ProbeAll probes every node once. A failure on one node must not stop the
// sweep, or a single unreachable agent would freeze every other node's
// readiness.
func (c *Controller) ProbeAll(ctx context.Context) error {
	nodes, err := c.Store.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("probe sweep: %w", err)
	}
	for _, n := range nodes {
		if err := c.ProbeAgent(ctx, n.ID); err != nil && ctx.Err() == nil {
			c.log.Error("probe", "node", n.ID, "err", err)
		}
	}
	return nil
}
