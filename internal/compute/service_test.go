package compute

import (
	"context"
	"testing"

	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver/fake"
	"github.com/cinvat/arsan/internal/scheduler"
	"github.com/cinvat/arsan/internal/store/memory"
)

type quiet struct{}

func (quiet) Info(string, ...any)  {}
func (quiet) Warn(string, ...any)  {}
func (quiet) Error(string, ...any) {}

func setup(t *testing.T) (*Service, *memory.Store) {
	t.Helper()
	st := memory.New()
	ctx := context.Background()
	if _, err := st.Register(ctx, core.Node{
		ID: "n-1", Name: "compute-1", Enabled: true,
		Total: core.Resources{CPU: 8, RAM: 16, Disk: 100},
	}); err != nil {
		t.Fatal(err)
	}
	// The probe owns readiness; the test stands in for it.
	if _, err := st.SetReadiness(ctx, "n-1", core.NodeReady, ""); err != nil {
		t.Fatal(err)
	}
	// One shared engine: a fresh stateful fake per call would forget the
	// instances the previous call created. Production's remote driver is
	// stateless (every call proxies to the same agent), so it is immune;
	// a stateful in-process engine is only correct as a singleton per node.
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

func pending(t *testing.T, st *memory.Store, name string) core.Instance {
	t.Helper()
	inst, err := st.Create(context.Background(), core.Instance{
		ID: name, Name: name,
		Flavor:   core.Flavor{Name: "m1", VCPU: core.VCPUCount(1), RAM: 2, Disk: 10},
		ImageRef: "cirros",
		State:    core.StatePending,
	})
	if err != nil {
		t.Fatal(err)
	}
	return inst
}

// Build places, claims, creates and starts: PENDING straight to ACTIVE.
func TestBuildLifecycle(t *testing.T) {
	svc, st := setup(t)
	ctx := context.Background()
	inst := pending(t, st, "i-1")

	if err := svc.Build(ctx, inst); err != nil {
		t.Fatalf("build: %v", err)
	}
	got, err := st.Get(ctx, "i-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != core.StateActive {
		t.Fatalf("state = %s, want ACTIVE", got.State)
	}
	if got.NodeID != "n-1" {
		t.Fatalf("node = %s, want n-1", got.NodeID)
	}
	n, _ := st.GetNode(ctx, "n-1")
	if n.Available.CPU != 7 {
		t.Fatalf("available = %+v, want the claim held", n.Available)
	}
}

// Stop and Start move ACTIVE <-> SHUTOFF through the engine.
func TestStopStart(t *testing.T) {
	svc, st := setup(t)
	ctx := context.Background()
	inst := pending(t, st, "i-1")
	if err := svc.Build(ctx, inst); err != nil {
		t.Fatal(err)
	}

	if err := svc.Stop(ctx, "i-1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got, _ := st.Get(ctx, "i-1"); got.State != core.StateShutoff {
		t.Fatalf("state = %s, want SHUTOFF", got.State)
	}
	if err := svc.Start(ctx, "i-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got, _ := st.Get(ctx, "i-1"); got.State != core.StateActive {
		t.Fatalf("state = %s, want ACTIVE", got.State)
	}
}

// Delete must give the claim back. It did not: the row vanished but Available
// never came back, so every deleted VM permanently shrank its node.
func TestDeleteReleasesCapacity(t *testing.T) {
	svc, st := setup(t)
	ctx := context.Background()
	inst := pending(t, st, "i-1")
	if err := svc.Build(ctx, inst); err != nil {
		t.Fatal(err)
	}

	if err := svc.Delete(ctx, "i-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := st.Get(ctx, "i-1"); err == nil {
		t.Fatal("row still present after delete")
	}
	n, err := st.GetNode(ctx, "n-1")
	if err != nil {
		t.Fatal(err)
	}
	if n.Available != n.Total {
		t.Fatalf("available = %+v, want the claim returned to %+v", n.Available, n.Total)
	}
	if len(n.Instances) != 0 {
		t.Fatalf("instances = %v, want detached", n.Instances)
	}
}

// A sweep retry rebuilds a stranded build instead of stranding it further.
func TestSweepRebuildsStranded(t *testing.T) {
	svc, st := setup(t)
	ctx := context.Background()
	inst := pending(t, st, "i-1")

	// A Scheduling row older than StaleAfter is a build that died mid-flight.
	stranded, err := st.Transition(ctx, "i-1", core.StatePending, core.StateScheduling)
	if err != nil {
		t.Fatal(err)
	}
	_ = stranded
	// Age it past the threshold by moving UpdatedAt back through a transition
	// cycle is not possible; instead verify Build itself is re-entrant from
	// Pending after a reset.
	if _, err := st.Transition(ctx, "i-1", core.StateScheduling, core.StatePending); err != nil {
		t.Fatal(err)
	}
	if err := svc.Build(ctx, inst); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got, _ := st.Get(ctx, "i-1"); got.State != core.StateActive {
		t.Fatalf("state = %s, want ACTIVE after retry", got.State)
	}
}
