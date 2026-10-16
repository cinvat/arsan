package compute

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/core"
)

const (
	DefaultDriverOpTimeout = 5 * time.Minute
	DefaultSweepInterval   = 30 * time.Second
	DefaultStaleAfter      = 2 * time.Minute
)

type DriverFor func(nodeID string) (core.Driver, error)

type Config struct {
	Instances              core.InstanceStore
	Nodes                  core.NodeStore
	Scheduler              core.Scheduler
	DriverFor              DriverFor
	DriverOperationTimeout time.Duration
	SweepInterval          time.Duration
	StaleAfter             time.Duration
	Log                    Logger
}

type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type Service struct {
	cfg Config
}

func New(cfg Config) *Service {
	if cfg.DriverOperationTimeout == 0 {
		cfg.DriverOperationTimeout = DefaultDriverOpTimeout
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = DefaultSweepInterval
	}
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = DefaultStaleAfter
	}
	return &Service{cfg: cfg}
}

func (s *Service) Get(ctx context.Context, id string) (core.Instance, error) {
	return s.cfg.Instances.Get(ctx, id)
}

func (s *Service) List(ctx context.Context) ([]core.Instance, error) {
	return s.cfg.Instances.List(ctx)
}

func (s *Service) Delete(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.NodeID != "" {
		// Tear the guest down on its host first. Engine Delete destroys a
		// running domain (one that ignored ACPI shutdown included) and
		// undefines it; a missing domain is success, so a redelivered
		// command converges instead of wedging. Anything else fails the
		// command and redelivers: cleaning the row while the guest lives
		// would strand a domain no record points at.
		if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
			return d.Delete(dctx, id)
		}); err != nil && !errors.Is(err, core.ErrInstanceNotFound) {
			return err
		}
	}
	// The row goes away but the claim must come back first, or every deleted
	// VM permanently shrinks its node.
	if err := s.cfg.Instances.ReleaseReservations(ctx, inst.ID, inst.NodeID); err != nil {
		s.cfg.Log.Error("reservation leak", "instance", id, "node", inst.NodeID, "err", err)
	}
	return s.cfg.Instances.Delete(ctx, id)
}

// withEngine resolves the engine for the node holding id and runs fn against
// it, bounded by the operation timeout.
func (s *Service) withEngine(ctx context.Context, nodeID string, fn func(context.Context, core.Driver) error) error {
	d, err := s.cfg.DriverFor(nodeID)
	if err != nil {
		return err
	}
	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()
	return fn(dctx, d)
}

func (s *Service) Start(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateShutoff {
		return fmt.Errorf("start: %w", core.ErrInvalidTransition)
	}
	// Boot it where it already lives: the claim and placement survived the
	// stop, so there is nothing to schedule.
	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.Start(dctx, id)
	}); err != nil {
		return err
	}
	_, err = s.cfg.Instances.Transition(ctx, id, core.StateShutoff, core.StateActive)
	return err
}

func (s *Service) Stop(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateActive {
		return fmt.Errorf("stop: %w", core.ErrInvalidTransition)
	}

	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.Stop(dctx, id)
	}); err != nil {
		return err
	}

	_, err = s.cfg.Instances.Transition(ctx, id, core.StateActive, core.StateShutoff)
	return err
}

func (s *Service) Migrate(ctx context.Context, id, targetNodeID string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateActive {
		return fmt.Errorf("migrate: %w", core.ErrInvalidTransition)
	}

	return s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.Migrate(dctx, id, targetNodeID)
	})
}

// Build drives a PENDING instance to ACTIVE: schedule, claim, create on the
// engine, start, activate.
//
// This is the entry point for a command-bus handler. It is idempotent in the way
// the whole service is: the caller is expected to have re-read the record, and
// the compare-and-swap transitions reject a build whose instance has already
// advanced past PENDING.
func (s *Service) Build(ctx context.Context, inst core.Instance) error {
	return s.build(ctx, inst.ID)
}

func (s *Service) build(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get %s: %w", id, err)
	}

	placed, err := s.cfg.Scheduler.Schedule(ctx, core.PlacementRequest{
		InstanceID: inst.ID,
		Flavor:     inst.Flavor,
		ImageRef:   inst.ImageRef,
	})
	if err != nil {
		s.fail(ctx, id, core.StatePending, err)
		return err
	}

	inst, err = s.cfg.Instances.Transition(ctx, id, core.StatePending, core.StateScheduling)
	if err != nil {
		return fmt.Errorf("claim %s: %w", id, err)
	}
	inst.NodeID = placed.NodeID
	inst.Generation = placed.Generation
	// A dedicated placement carries exclusive CPUs; they live on the instance
	// so teardown frees exactly them, and the engine pins to exactly them.
	inst.PinnedCPUs = placed.PinnedCPUs
	inst, err = s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return fmt.Errorf("record placement: %w", err)
	}

	if err := s.withEngine(ctx, placed.NodeID, func(dctx context.Context, d core.Driver) error {
		if _, err := d.Create(dctx, core.CreateRequest{
			InstanceID: id, NodeID: placed.NodeID, Name: inst.Name,
			Flavor: inst.Flavor, PinnedCPUs: inst.PinnedCPUs, ImageRef: inst.ImageRef,
		}); err != nil {
			return err
		}
		return d.Start(dctx, id)
	}); err != nil {
		return s.fail(ctx, id, core.StateScheduling, err)
	}

	if _, err := s.cfg.Instances.Transition(ctx, id, core.StateScheduling, core.StateBuilding); err != nil {
		return fmt.Errorf("enter building: %w", err)
	}

	_, err = s.cfg.Instances.Transition(ctx, id, core.StateBuilding, core.StateActive)
	return err
}

func (s *Service) fail(ctx context.Context, id string, from core.InstanceState, cause error) error {
	inst, err := s.cfg.Instances.Transition(ctx, id, from, core.StateError)
	if err != nil {
		return fmt.Errorf("mark error: %w", err)
	}
	if err := s.cfg.Instances.ReleaseReservations(ctx, inst.ID, inst.NodeID); err != nil {
		s.cfg.Log.Error("reservation leak", "instance", id, "node", inst.NodeID, "err", err)
	}
	return fmt.Errorf("build %s: %w", id, cause)
}

func (s *Service) SweepLoop(ctx context.Context) error {
	t := time.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := s.Reconcile(ctx); err != nil {
				s.cfg.Log.Error("reconcile", "err", err)
			}
		}
	}
}

func (s *Service) Reconcile(ctx context.Context) error {
	all, err := s.cfg.Instances.List(ctx)
	if err != nil {
		return fmt.Errorf("reconcile list: %w", err)
	}

	for _, i := range all {
		if i.State != core.StateScheduling && i.State != core.StateBuilding {
			continue
		}
		if time.Since(i.UpdatedAt) < s.cfg.StaleAfter {
			continue
		}

		if _, err := s.cfg.Instances.Transition(ctx, i.ID, i.State, core.StatePending); err != nil {
			s.cfg.Log.Warn("sweep", "instance", i.ID, "err", err)
			continue
		}
		if err := s.cfg.Instances.ReleaseReservations(ctx, i.ID, i.NodeID); err != nil {
			s.cfg.Log.Error("sweep release", "instance", i.ID, "err", err)
		}
		// Re-drive the build here rather than publishing: the retry path owns
		// everything Build needs, and the event it used to emit had no
		// subscribers, so a reset instance sat PENDING forever.
		if err := s.build(ctx, i.ID); err != nil {
			s.cfg.Log.Warn("sweep rebuild", "instance", i.ID, "err", err)
		}
	}
	return nil
}
