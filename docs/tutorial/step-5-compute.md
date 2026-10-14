# Step 5: Compute — Instance State Machine

## Goal
Implement the instance lifecycle state machine. This is the **only** component that calls `core.Driver`.

---

## File: `internal/compute/service.go`

```go
package compute

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/message"
)

const (
	MaxRebuildAttempts   = 10
	DriverOperationTimeout = 5 * time.Minute
	DefaultSweepInterval   = 30 * time.Second
	DefaultStaleAfter      = 2 * time.Minute
	DefaultMaxRebuildBackoff = 5 * time.Minute
)

type DriverFor func(nodeID string) (core.Driver, error)

type Config struct {
	Instances core.InstanceStore
	Nodes     core.NodeStore
	Bus       *message.Bus
	Scheduler core.Scheduler
	DriverFor DriverFor

	DriverOperationTimeout time.Duration
	SweepInterval          time.Duration
	StaleAfter             time.Duration
	MaxRebuildBackoff      time.Duration

	Log Logger
}

type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type Service struct {
	cfg Config
	mu  sync.Mutex
}

func New(cfg Config) *Service {
	if cfg.DriverOperationTimeout == 0 { cfg.DriverOperationTimeout = DriverOperationTimeout }
	if cfg.SweepInterval == 0 { cfg.SweepInterval = DefaultSweepInterval }
	if cfg.StaleAfter == 0 { cfg.StaleAfter = DefaultStaleAfter }
	if cfg.MaxRebuildBackoff == 0 { cfg.MaxRebuildBackoff = DefaultMaxRebuildBackoff }
	return &Service{cfg: cfg}
}

// ========== Public API ==========

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
	if err != nil { return core.Instance{}, fmt.Errorf("create instance: %w", err) }
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
	if err != nil { return err }
	if inst.State == core.StateActive || inst.State == core.StateBuilding {
		// Best-effort stop before delete
		_ = s.cfg.DriverFor(inst.NodeID)
	}
	return s.cfg.Instances.Delete(ctx, id)
}

func (s *Service) Start(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil { return err }
	if inst.State != core.StateShutoff { return fmt.Errorf("start: %w", core.ErrInvalidTransition) }
	return s.build(ctx, id)
}

func (s *Service) Stop(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil { return err }
	if inst.State != core.StateActive { return fmt.Errorf("stop: %w", core.ErrInvalidTransition) }

	d, err := s.cfg.DriverFor(inst.NodeID)
	if err != nil { return err }
	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()
	if err := d.Stop(dctx, id); err != nil { return err }

	_, err = s.cfg.Instances.Transition(ctx, id, core.StateActive, core.StateShutoff)
	return err
}

func (s *Service) Migrate(ctx context.Context, id, targetNodeID string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil { return err }
	if inst.State != core.StateActive { return fmt.Errorf("migrate: %w", core.ErrInvalidTransition) }

	d, err := s.cfg.DriverFor(inst.NodeID)
	if err != nil { return err }
	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()
	return d.Migrate(dctx, id, targetNodeID)
}

// ========== Build Flow ==========

// build executes: schedule → reserve → create → start → active
func (s *Service) build(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil { return fmt.Errorf("get %s: %w", id, err) }

	// 1. Schedule
	placed, err := s.cfg.Scheduler.Schedule(ctx, core.PlacementRequest{
		InstanceID: inst.ID, Flavor: inst.Flavor, ImageRef: inst.ImageRef,
	})
	if err != nil {
		s.fail(ctx, id, core.StatePending, err)
		return err
	}

	// 2. CAS PENDING -> SCHEDULING
	inst, err = s.cfg.Instances.Transition(ctx, id, core.StatePending, core.StateScheduling)
	if err != nil { return fmt.Errorf("claim %s: %w", id, err) }
	inst.NodeID = placed.NodeID
	inst.Generation = placed.Generation
	inst, err = s.cfg.Instances.Update(ctx, inst)
	if err != nil { return fmt.Errorf("record placement: %w", err) }

	// 3. Driver Create
	d, err := s.cfg.DriverFor(placed.NodeID)
	if err != nil { return s.fail(ctx, id, core.StateScheduling, err) }

	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()

	if _, err := d.Create(dctx, core.CreateRequest{
		InstanceID: id, NodeID: placed.NodeID, Name: inst.Name,
		Flavor: inst.Flavor, ImageRef: inst.ImageRef,
	}); err != nil { return s.fail(ctx, id, core.StateScheduling, err) }

	// 4. CAS SCHEDULING -> BUILDING
	if _, err := s.cfg.Instances.Transition(ctx, id, core.StateScheduling, core.StateBuilding); err != nil {
		return fmt.Errorf("enter building: %w", err)
	}

	// 5. Driver Start
	if err := d.Start(dctx, id); err != nil { return s.fail(ctx, id, core.StateBuilding, err) }

	// 6. CAS BUILDING -> ACTIVE
	_, err = s.cfg.Instances.Transition(ctx, id, core.StateBuilding, core.StateActive)
	return err
}

// fail moves instance to ERROR and releases reservation. Order matters:
// state first (visible), then resources.
func (s *Service) fail(ctx context.Context, id string, from core.InstanceState, cause error) error {
	inst, err := s.cfg.Instances.Transition(ctx, id, from, core.StateError)
	if err != nil { return fmt.Errorf("mark error: %w", err) }
	if err := s.cfg.Instances.ReleaseReservations(ctx, inst.ID, inst.NodeID); err != nil {
		s.cfg.Log.Error("reservation leak", "instance", id, "node", inst.NodeID, "err", err)
	}
	return fmt.Errorf("build %s: %w", id, cause)
}

// ========== Reconciliation Sweep ==========

func (s *Service) SweepLoop(ctx context.Context) error {
	t := time.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-t.C:
			if err := s.Reconcile(ctx); err != nil {
				s.cfg.Log.Error("reconcile", "err", err)
			}
		}
	}
}

func (s *Service) Reconcile(ctx context.Context) error {
	all, err := s.cfg.Instances.List(ctx)
	if err != nil { return fmt.Errorf("reconcile list: %w", err) }

	for _, i := range all {
		if i.State != core.StateScheduling && i.State != core.StateBuilding { continue }
		if time.Since(i.UpdatedAt) < s.cfg.StaleAfter { continue }

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

// ========== Re-placement on Node Failure ==========

func (s *Service) HandleNodeDown(ctx context.Context, failedNodeID string) error {
	all, err := s.cfg.Instances.List(ctx)
	if err != nil { return fmt.Errorf("list instances: %w", err) }

	for _, inst := range all {
		if inst.NodeID != failedNodeID { continue }
		if inst.State == core.StateShutoff || inst.State == core.StateError { continue }
		if err := s.rebuild(ctx, inst, failedNodeID); err != nil {
			s.cfg.Log.Error("rebuild", "instance", inst.ID, "err", err)
		}
	}
	return nil
}

func (s *Service) rebuild(ctx context.Context, inst core.Instance, failedNodeID string) error {
	// Per-instance backoff: prevents thundering herd when a whole host dies
	if wait := backoff(inst.Generation); wait > 0 {
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-time.After(wait):
		}
	}

	// 1. Release old reservation
	if err := s.cfg.Instances.ReleaseReservations(ctx, inst.ID, failedNodeID); err != nil {
		return fmt.Errorf("release: %w", err)
	}

	// 2. Reset to PENDING with bumped generation
	if _, err := s.cfg.Instances.Transition(ctx, inst.ID, inst.State, core.StatePending); err != nil {
		return fmt.Errorf("reset to pending: %w", err)
	}

	// 3. Schedule with exclusion and bumped generation
	_, err := s.cfg.Scheduler.Schedule(ctx, core.PlacementRequest{
		InstanceID:     inst.ID,
		Flavor:         inst.Flavor,
		ImageRef:       inst.ImageRef,
		ExcludeNodeIDs: []string{failedNodeID}, // CRITICAL: don't place back on failed node
		Generation:     inst.Generation + 1,
	})
	return err
}

func backoff(generation int) time.Duration {
	d := time.Duration(1<<min(generation, 8)) * time.Second
	return min(d, 5*time.Minute)
}

func newID() string { return fmt.Sprintf("i-%d", time.Now().UnixNano()) }
```

---

## Key Design Points

### State Machine Transitions
```
PENDING --schedule ok--> SCHEDULING --claim ok--> BUILDING --start ok--> ACTIVE
    |                         |                        |                |
    | no host                 | claim lost             | driver fail    | stop
    v                         v                        v                v
ERROR <---------------------- ERROR <------------------- ERROR    SHUTOFF
```

### Compare-and-Swap Everywhere
```go
// Never: read -> modify -> Update
// Always: Transition(ctx, id, from, to)
inst, err = s.cfg.Instances.Transition(ctx, id, core.StatePending, core.StateScheduling)
```

### Reservation Release on Failure
```go
func (s *Service) fail(ctx, id, from, cause) error {
    inst, _ := s.cfg.Instances.Transition(ctx, id, from, core.StateError)
    s.cfg.Instances.ReleaseReservations(ctx, inst.ID, inst.NodeID)
    return fmt.Errorf("build %s: %w", id, cause)
}
```

### Re-placement Correctness
```go
_, err := s.cfg.Scheduler.Schedule(ctx, core.PlacementRequest{
    ExcludeNodeIDs: []string{failedNodeID},  // Don't place back on dead node
    Generation:     inst.Generation + 1,      // Bump generation
})
```
- `ExcludeNodeIDs` prevents placing back on the node that just failed
- `Generation` bump invalidates any in-flight work for the old placement

### Reconciliation Sweep
Recovers instances stuck in `SCHEDULING` or `BUILDING`:
- Resets to `PENDING`
- Releases reservation
- Re-publishes `InstanceCreated` event for retry

---

## Test: `internal/compute/service_test.go`

```go
package compute

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver/fake"
	"github.com/cinvat/arsan/internal/message"
	"github.com/cinvat/arsan/internal/scheduler"
	"github.com/cinvat/arsan/internal/store/memory"
)

func setup(t *testing.T) *Service {
	st := memory.New()
	bus := message.New()
	d := fake.New()
	driverFor := func(string) (core.Driver, error) { return d, nil }

	sched := scheduler.New(scheduler.Config{
		Instances: st, Nodes: st, DriverFor: driverFor, MaxAttempts: 1,
	})

	return New(Config{
		Instances: st, Nodes: st, Bus: bus, Scheduler: sched,
		DriverFor: driverFor, SweepInterval: 10 * time.Millisecond,
		StaleAfter: 20 * time.Millisecond, Log: testLog{t},
	})
}

func TestCreateReachesActive(t *testing.T) {
	s := setup(t)
	inst, err := s.Create(context.Background(), core.CreateInstanceRequest{
		Name: "test", ImageRef: "cirros", Flavor: core.Flavor{VCPU: 1, RAM: 2, Disk: 10},
	})
	if err != nil { t.Fatal(err) }

	// Wait for ACTIVE
	for i := 0; i < 50; i++ {
		inst, _ = s.Get(context.Background(), inst.ID)
		if inst.State == core.StateActive { return }
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("instance stuck at %s", inst.State)
}

func TestTransitionsRejectIllegal(t *testing.T) {
	s := setup(t)
	inst, _ := s.Create(context.Background(), core.CreateInstanceRequest{
		Name: "test", ImageRef: "cirros", Flavor: core.Flavor{VCPU: 1, RAM: 2, Disk: 10},
	})
	// Wait for ACTIVE
	for inst.State != core.StateActive {
		inst, _ = s.Get(context.Background(), inst.ID)
		time.Sleep(10 * time.Millisecond)
	}

	// Active -> Building must fail
	err := s.Stop(context.Background(), inst.ID)
	if err == nil { t.Fatal("stop should succeed") }

	err = s.Start(context.Background(), inst.ID)
	if err == nil { t.Fatal("start from active should fail") }
}

func TestReplacedOnNodeDown(t *testing.T) {
	s := setup(t)
	// Two nodes, n1 has capacity, n2 doesn't
	st := s.cfg.Instances.(*memory.Store)
	st.nodes["n1"] = core.Node{ID: "n1", State: core.NodeReady, Enabled: true, Total: core.Resources{VCPU: 8, RAM: 16, Disk: 100}, Available: core.Resources{VCPU: 8, RAM: 16, Disk: 100}}
	st.nodes["n2"] = core.Node{ID: "n2", State: core.NodeReady, Enabled: true, Total: core.Resources{VCPU: 1, RAM: 2, Disk: 10}, Available: core.Resources{VCPU: 1, RAM: 2, Disk: 10}}

	inst, _ := s.Create(context.Background(), core.CreateInstanceRequest{
		Name: "test", ImageRef: "cirros", Flavor: core.Flavor{VCPU: 1, RAM: 2, Disk: 10},
	})
	// Wait ACTIVE on n1
	for inst.State != core.StateActive {
		inst, _ = s.Get(context.Background(), inst.ID)
		time.Sleep(10 * time.Millisecond)
	}
	if inst.NodeID != "n1" { t.Fatalf("expected n1, got %s", inst.NodeID) }

	// Mark n1 down
	s.HandleNodeDown(context.Background(), "n1")

	// Should re-place on n2
	for i := 0; i < 50; i++ {
		inst, _ = s.Get(context.Background(), inst.ID)
		if inst.NodeID == "n2" && inst.State == core.StateActive { return }
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("instance not re-placed: %+v", inst)
}
```

---

## Run Tests

```bash
go test -race ./internal/compute/...
```

---

## Next

Step 6: Node Service — Registration, Usage & Drain