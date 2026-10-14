package compute

import (
	"context"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/message"
)

const (
	MaxRebuildAttempts       = 10
	DefaultDriverOpTimeout   = 5 * time.Minute
	DefaultSweepInterval     = 30 * time.Second
	DefaultStaleAfter        = 2 * time.Minute
	DefaultMaxRebuildBackoff = 5 * time.Minute
)

type DriverFor func(nodeID string) (core.Driver, error)

type Config struct {
	Instances              core.InstanceStore
	Nodes                  core.NodeStore
	Bus                    *message.Bus
	Scheduler              core.Scheduler
	DriverFor              DriverFor
	DriverOperationTimeout time.Duration
	SweepInterval          time.Duration
	StaleAfter             time.Duration
	MaxRebuildBackoff      time.Duration
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
	if cfg.MaxRebuildBackoff == 0 {
		cfg.MaxRebuildBackoff = DefaultMaxRebuildBackoff
	}
	return &Service{cfg: cfg}
}

func (s *Service) Create(ctx context.Context, req core.CreateInstanceRequest) (core.Instance, error) {
	inst := core.Instance{
		ID:        newID(),
		Name:      req.Name,
		Flavor:    req.Flavor,
		ImageRef:  req.ImageRef,
		State:     core.StatePending,
		CreatedAt: time.Now().UTC(),
	}
	created, err := s.cfg.Instances.Create(ctx, inst)
	if err != nil {
		return core.Instance{}, fmt.Errorf("create instance: %w", err)
	}
	s.cfg.Bus.Publish(message.Event{Topic: message.TopicInstanceCreated, Payload: created.ID})
	return created, nil
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
	if inst.State == core.StateActive || inst.State == core.StateBuilding {
		// Best-effort stop before delete
		if d, err := s.cfg.DriverFor(inst.NodeID); err == nil {
			_ = d.Stop(ctx, id)
		}
	}
	return s.cfg.Instances.Delete(ctx, id)
}

func (s *Service) Start(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateShutoff {
		return fmt.Errorf("start: %w", core.ErrInvalidTransition)
	}
	return s.build(ctx, id)
}

func (s *Service) Stop(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateActive {
		return fmt.Errorf("stop: %w", core.ErrInvalidTransition)
	}

	d, err := s.cfg.DriverFor(inst.NodeID)
	if err != nil {
		return err
	}
	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()
	if err := d.Stop(dctx, id); err != nil {
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

	d, err := s.cfg.DriverFor(inst.NodeID)
	if err != nil {
		return err
	}
	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()
	return d.Migrate(dctx, id, targetNodeID)
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
	inst, err = s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return fmt.Errorf("record placement: %w", err)
	}

	d, err := s.cfg.DriverFor(placed.NodeID)
	if err != nil {
		return s.fail(ctx, id, core.StateScheduling, err)
	}

	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()

	if _, err := d.Create(dctx, core.CreateRequest{
		InstanceID: id, NodeID: placed.NodeID, Name: inst.Name,
		Flavor: inst.Flavor, ImageRef: inst.ImageRef,
	}); err != nil {
		return s.fail(ctx, id, core.StateScheduling, err)
	}

	if _, err := s.cfg.Instances.Transition(ctx, id, core.StateScheduling, core.StateBuilding); err != nil {
		return fmt.Errorf("enter building: %w", err)
	}

	if err := d.Start(dctx, id); err != nil {
		return s.fail(ctx, id, core.StateBuilding, err)
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
		s.cfg.Bus.Publish(message.Event{Topic: message.TopicInstanceCreated, Payload: i.ID})
	}
	return nil
}

func (s *Service) HandleNodeDown(ctx context.Context, failedNodeID string) error {
	all, err := s.cfg.Instances.List(ctx)
	if err != nil {
		return fmt.Errorf("list instances: %w", err)
	}

	for _, inst := range all {
		if inst.NodeID != failedNodeID {
			continue
		}
		if inst.State == core.StateShutoff || inst.State == core.StateError {
			continue
		}
		if err := s.rebuild(ctx, inst, failedNodeID); err != nil {
			s.cfg.Log.Error("rebuild", "instance", inst.ID, "err", err)
		}
	}
	return nil
}

func (s *Service) rebuild(ctx context.Context, inst core.Instance, failedNodeID string) error {
	if wait := backoff(inst.Generation); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	if err := s.cfg.Instances.ReleaseReservations(ctx, inst.ID, failedNodeID); err != nil {
		return fmt.Errorf("release: %w", err)
	}

	if _, err := s.cfg.Instances.Transition(ctx, inst.ID, inst.State, core.StatePending); err != nil {
		return fmt.Errorf("reset to pending: %w", err)
	}

	_, err := s.cfg.Scheduler.Schedule(ctx, core.PlacementRequest{
		InstanceID:     inst.ID,
		Flavor:         inst.Flavor,
		ImageRef:       inst.ImageRef,
		ExcludeNodeIDs: []string{failedNodeID},
		Generation:     inst.Generation + 1,
	})
	return err
}

func backoff(generation int) time.Duration {
	d := time.Duration(1<<min(generation, 8)) * time.Second
	return min(d, 5*time.Minute)
}

func newID() string {
	return fmt.Sprintf("i-%d", time.Now().UnixNano())
}
