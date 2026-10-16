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
)

const (
	DriverOperationTimeout = 5 * time.Minute
	DefaultSweepInterval   = 30 * time.Second
	DefaultStaleAfter      = 2 * time.Minute
)

type DriverFor func(nodeID string) (core.Driver, error)

type Config struct {
	Instances core.InstanceStore
	Nodes     core.NodeStore
	Scheduler core.Scheduler
	DriverFor DriverFor

	DriverOperationTimeout time.Duration
	SweepInterval          time.Duration
	StaleAfter             time.Duration

	Log Logger
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
	if cfg.DriverOperationTimeout == 0 { cfg.DriverOperationTimeout = DriverOperationTimeout }
	if cfg.SweepInterval == 0 { cfg.SweepInterval = DefaultSweepInterval }
	if cfg.StaleAfter == 0 { cfg.StaleAfter = DefaultStaleAfter }
	return &Service{cfg: cfg}
}

// ========== Public API ==========
//
// There is deliberately no Create here. The api persists the PENDING row
// through the records layer (which mints the id and enqueues over NATS); a
// second Create on this service would mint a second id scheme and publish to
// a bus nobody subscribes to. This service builds, stops, starts, deletes and
// migrates — it does not originate instances.

func (s *Service) Get(ctx context.Context, id string) (core.Instance, error) {
	return s.cfg.Instances.Get(ctx, id)
}

func (s *Service) List(ctx context.Context) ([]core.Instance, error) {
	return s.cfg.Instances.List(ctx)
}

func (s *Service) Delete(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil { return err }
	if inst.State == core.StateRunning || inst.State == core.StateBuilding {
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
	if inst.State != core.StateRunning { return fmt.Errorf("stop: %w", core.ErrInvalidTransition) }

	d, err := s.cfg.DriverFor(inst.NodeID)
	if err != nil { return err }
	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()
	if err := d.Stop(dctx, id); err != nil { return err }

	_, err = s.cfg.Instances.Transition(ctx, id, core.StateRunning, core.StateShutoff)
	return err
}

func (s *Service) Migrate(ctx context.Context, id, targetNodeID string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil { return err }
	if inst.State != core.StateRunning { return fmt.Errorf("migrate: %w", core.ErrInvalidTransition) }

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

	// 6. CAS BUILDING -> RUNNING
	_, err = s.cfg.Instances.Transition(ctx, id, core.StateBuilding, core.StateRunning)
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
		// Re-drive the build here rather than publishing: the retry path owns
		// everything Build needs.
		if err := s.build(ctx, i.ID); err != nil {
			s.cfg.Log.Warn("sweep rebuild", "instance", i.ID, "err", err)
		}
	}
	return nil
}

// ========== Re-placement on Node Failure ==========
//
// There is deliberately no HandleNodeDown. An earlier version had one, but it
// never ran (nothing published its trigger) and its body was broken: it
// discarded the schedule result, leaking the fresh claim, and stranded the
// instance PENDING. Host-failure recovery needs a real design — probably the
// probe firing it, with generation-aware placement — not a resurrection of
// that function.

```

---

## Key Design Points

### State Machine Transitions
```
PENDING --schedule ok--> SCHEDULING --claim ok--> BUILDING --start ok--> RUNNING
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
	"github.com/cinvat/arsan/internal/scheduler"
	"github.com/cinvat/arsan/internal/store/memory"
)

func setup(t *testing.T) (*Service, *memory.Store) {
	st := memory.New()
	ctx := context.Background()
	if _, err := st.Register(ctx, core.Node{
		ID: "n-1", Name: "compute-1", Enabled: true,
		Total: core.Resources{VCPU: 8, RAM: 16, Disk: 100},
	}); err != nil {
		t.Fatal(err)
	}
	// The probe owns readiness; the test stands in for it.
	if _, err := st.SetReadiness(ctx, "n-1", core.NodeReady, ""); err != nil {
		t.Fatal(err)
	}
	// One shared engine: a fresh stateful fake per call would forget the
	// instances the previous call created.
	engine := fake.New()
	driverFor := func(string) (core.Driver, error) { return engine, nil }
	sched := scheduler.New(scheduler.Config{
		Instances: st, Nodes: st, DriverFor: driverFor, Log: quiet{},
	})
	svc := New(Config{
		Instances: st, Nodes: st, Scheduler: sched,
		DriverFor: driverFor, Log: quiet{},
	})
	return svc, st
}

func TestBuildLifecycle(t *testing.T) {
	svc, st := setup(t)
	inst, err := st.Create(context.Background(), core.Instance{
		ID: "i-1", Name: "vm1",
		Flavor:   core.Flavor{Name: "m1", VCPU: 1, RAM: 2, Disk: 10},
		ImageRef: "cirros", State: core.StatePending,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Build(context.Background(), inst); err != nil {
		t.Fatalf("build: %v", err)
	}
	got, _ := st.Get(context.Background(), "i-1")
	if got.State != core.StateRunning { t.Fatalf("state = %s, want RUNNING", got.State) }
}

func TestDeleteReleasesCapacity(t *testing.T) {
	svc, st := setup(t)
	// ... build i-1 as above, then:
	if err := svc.Delete(context.Background(), "i-1"); err != nil { t.Fatal(err) }
	n, _ := st.GetNode(context.Background(), "n-1")
	if n.Available != n.Total {
		t.Fatalf("available = %+v, want the claim returned", n.Available)
	}
}

func TestStopStart(t *testing.T) {
	svc, st := setup(t)
	// ... build i-1 to RUNNING as above, then:
	if err := svc.Stop(context.Background(), "i-1"); err != nil { t.Fatal(err) }
	if got, _ := st.Get(context.Background(), "i-1"); got.State != core.StateShutoff {
		t.Fatalf("state = %s, want SHUTOFF", got.State)
	}
	// Starting boots the VM where it already lives; it must not reschedule.
	if err := svc.Start(context.Background(), "i-1"); err != nil { t.Fatal(err) }
	if got, _ := st.Get(context.Background(), "i-1"); got.State != core.StateRunning {
		t.Fatalf("state = %s, want RUNNING", got.State)
	}
}

// There is no TestReplacedOnNodeDown. Host-failure recovery was removed with
// HandleNodeDown: it never ran and its body was broken (it discarded the
// schedule result, leaking the fresh claim). When it comes back — probably
// fired by the probe, generation-aware — this is where its test goes.
```

---

## Run Tests

```bash
go test -race ./internal/compute/...
```

---

## Next

Step 6: Node Service — Registration, Usage & Drain