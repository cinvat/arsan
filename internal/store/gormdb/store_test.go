package gormdb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/store/memory"
)

// newStore opens a SQLite database on disk. On disk rather than in-memory so
// the tests exercise real file I/O and real locking, which is where the atomic
// guarantees live.
func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenSQLite(t.TempDir() + "/arsan.db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

func mustNode(t *testing.T, s *Store, id string, r core.Resources) core.Node {
	t.Helper()
	n, err := s.Register(t.Context(), core.Node{
		ID: id, Name: id, Total: r, Enabled: true, Driver: "fake",
	})
	if err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
	return n
}

func mustInstance(t *testing.T, s *Store, id string, state core.InstanceState) core.Instance {
	t.Helper()
	i, err := s.Create(t.Context(), core.Instance{
		ID: id, Name: id, State: state,
		Flavor: core.Flavor{Name: "m1.small", VCPU: core.VCPUCount(1), RAM: 2, Disk: 10},
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return i
}

// PendingReason and FailureReason survive the round trip: they are the
// operator's only account of why a VM waits or died.
func TestInstanceReasonsRoundTrip(t *testing.T) {
	s := newStore(t)
	inst := mustInstance(t, s, "i-1", core.StatePending)
	inst.PendingReason = "attempt 1: no valid node (rejected by: Capacity dropped 1)"
	inst.FailureReason = "build: dial: connection refused"
	if _, err := s.Update(t.Context(), inst); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(t.Context(), "i-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.PendingReason != inst.PendingReason {
		t.Fatalf("pending reason = %q, want %q", got.PendingReason, inst.PendingReason)
	}
	if got.FailureReason != inst.FailureReason {
		t.Fatalf("failure reason = %q, want %q", got.FailureReason, inst.FailureReason)
	}
}

// --- transitions ---------------------------------------------------------

func TestTransitionMovesState(t *testing.T) {
	s := newStore(t)
	mustInstance(t, s, "i-1", core.StatePending)

	got, err := s.Transition(t.Context(), "i-1", core.StatePending, core.StateScheduling)
	if err != nil {
		t.Fatalf("transition: %v", err)
	}
	if got.State != core.StateScheduling {
		t.Fatalf("state = %q, want SCHEDULING", got.State)
	}
}

// The compare-and-swap is the whole point: a transition from the wrong state
// must be refused, not silently applied.
func TestTransitionRejectsWrongFrom(t *testing.T) {
	s := newStore(t)
	mustInstance(t, s, "i-1", core.StateRunning)

	_, err := s.Transition(t.Context(), "i-1", core.StatePending, core.StateBuilding)
	if !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("got %v, want ErrInvalidTransition", err)
	}

	// And the record must be untouched.
	got, err := s.Get(t.Context(), "i-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != core.StateRunning {
		t.Fatalf("state = %q, want it unchanged at RUNNING", got.State)
	}
}

func TestTransitionMissingInstance(t *testing.T) {
	s := newStore(t)
	_, err := s.Transition(t.Context(), "nope", core.StatePending, core.StateRunning)
	if !errors.Is(err, core.ErrInstanceNotFound) {
		t.Fatalf("got %v, want ErrInstanceNotFound", err)
	}
}

// Update must not be a back door around Transition.
func TestUpdateRefusesToChangeState(t *testing.T) {
	s := newStore(t)
	inst := mustInstance(t, s, "i-1", core.StateRunning)

	inst.State = core.StateError
	if _, err := s.Update(t.Context(), inst); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("got %v, want ErrInvalidTransition", err)
	}
}

func TestUpdateAllowsMetadata(t *testing.T) {
	s := newStore(t)
	inst := mustInstance(t, s, "i-1", core.StateRunning)

	inst.NodeID = "n-1"
	inst.Generation = 3
	got, err := s.Update(t.Context(), inst)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got.NodeID != "n-1" || got.Generation != 3 {
		t.Fatalf("got %+v, want the metadata recorded", got)
	}
}

// --- the atomic claim ----------------------------------------------------

func TestReserveDecrementsAllDimensions(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 8, RAM: 16, Disk: 200})

	if err := s.ReserveNodeResources(t.Context(), "n-1", core.Resources{CPU: 2, RAM: 4, Disk: 20}); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	n, err := s.GetNode(t.Context(), "n-1")
	if err != nil {
		t.Fatal(err)
	}
	want := core.Resources{CPU: 6, RAM: 12, Disk: 180}
	if n.Available != want {
		t.Fatalf("available = %+v, want %+v", n.Available, want)
	}
}

// All-or-nothing: a request that fits in vCPU but not RAM must change nothing.
func TestReserveIsAllOrNothing(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 8, RAM: 4, Disk: 200})

	err := s.ReserveNodeResources(t.Context(), "n-1", core.Resources{CPU: 1, RAM: 8, Disk: 20})
	if !errors.Is(err, core.ErrCapacityConflict) {
		t.Fatalf("got %v, want ErrCapacityConflict", err)
	}

	n, _ := s.GetNode(t.Context(), "n-1")
	if n.Available != (core.Resources{CPU: 8, RAM: 4, Disk: 200}) {
		t.Fatalf("available = %+v; a refused reservation must mutate nothing", n.Available)
	}
}

func TestReserveMissingNode(t *testing.T) {
	s := newStore(t)
	err := s.ReserveNodeResources(t.Context(), "ghost", core.Resources{CPU: 1})
	if !errors.Is(err, core.ErrNodeNotFound) {
		t.Fatalf("got %v, want ErrNodeNotFound", err)
	}
}

// This is the test that exists to catch an overcommit: many racers, a fixed
// amount of capacity, and an exact expected number of winners.
func TestConcurrentReservationsNeverOvercommit(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 2, RAM: 4, Disk: 10})

	const racers = 40
	var won atomic.Int64
	var lost atomic.Int64

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			<-start
			err := s.ReserveNodeResources(t.Context(), "n-1", core.Resources{CPU: 1, RAM: 1, Disk: 1})
			switch {
			case err == nil:
				won.Add(1)
			case errors.Is(err, core.ErrCapacityConflict):
				lost.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := won.Load(); got != 2 {
		t.Fatalf("%d reservations succeeded, want exactly 2", got)
	}

	n, err := s.GetNode(t.Context(), "n-1")
	if err != nil {
		t.Fatal(err)
	}
	if n.Available != (core.Resources{CPU: 0, RAM: 2, Disk: 8}) {
		t.Fatalf("available = %+v, want the capacity to be fully consumed", n.Available)
	}
}

// --- release -------------------------------------------------------------

func TestReleaseReturnsCapacityAndClampsToTotal(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 4, RAM: 8, Disk: 40})

	if err := s.ReserveNodeResources(t.Context(), "n-1", core.Resources{CPU: 2, RAM: 4, Disk: 20}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseNodeResources(t.Context(), "n-1", core.Resources{CPU: 2, RAM: 4, Disk: 20}); err != nil {
		t.Fatal(err)
	}

	n, _ := s.GetNode(t.Context(), "n-1")
	if n.Available != n.Total {
		t.Fatalf("available = %+v, want it back to total %+v", n.Available, n.Total)
	}

	// Releasing again must not invent capacity beyond the node's total.
	if err := s.ReleaseNodeResources(t.Context(), "n-1", core.Resources{CPU: 2, RAM: 4, Disk: 20}); err != nil {
		t.Fatal(err)
	}
	n, _ = s.GetNode(t.Context(), "n-1")
	if n.Available != n.Total {
		t.Fatalf("available = %+v after a double release, want clamped to total", n.Available)
	}
}

func TestReleaseReservationsIsIdempotent(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 4, RAM: 8, Disk: 40})
	mustInstance(t, s, "i-1", core.StateRunning)

	for i := 0; i < 3; i++ {
		if err := s.ReleaseReservations(t.Context(), "i-1", "n-1"); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
	}

	n, _ := s.GetNode(t.Context(), "n-1")
	if n.Available != n.Total {
		t.Fatalf("available = %+v, want total: a repeated release must not inflate it", n.Available)
	}

	// A missing instance or node is not an error.
	if err := s.ReleaseReservations(t.Context(), "ghost", "n-1"); err != nil {
		t.Fatalf("releasing an unknown instance should be a no-op: %v", err)
	}
	if err := s.ReleaseReservations(t.Context(), "i-1", "ghost"); err != nil {
		t.Fatalf("releasing against an unknown node should be a no-op: %v", err)
	}
}

// --- nodes ---------------------------------------------------------------

func TestRegisterIsAnUpsert(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 8, RAM: 16, Disk: 200})
	if err := s.ReserveNodeResources(t.Context(), "n-1", core.Resources{CPU: 2, RAM: 4, Disk: 20}); err != nil {
		t.Fatal(err)
	}

	// Re-register: capacity and state must survive.
	_, err := s.Register(t.Context(), core.Node{
		ID: "n-1", Name: "n-1-renamed",
		Total: core.Resources{CPU: 8, RAM: 16, Disk: 200},
	})
	if err != nil {
		t.Fatal(err)
	}

	n, _ := s.GetNode(t.Context(), "n-1")
	if n.Name != "n-1-renamed" {
		t.Errorf("name = %q, want it refreshed", n.Name)
	}
	if n.Available != (core.Resources{CPU: 6, RAM: 12, Disk: 180}) {
		t.Errorf("available = %+v, want the reservation preserved across a re-register", n.Available)
	}
}

// A heartbeat leaves state alone in every case.
//
// An earlier version promoted a node to READY here, guarded only by Enabled so
// a drained host could not come back. The guard was necessary but not
// sufficient: a heartbeat proves the agent answered HTTP, not that it can run a
// VM. Readiness now belongs to the controller's probe, which round-trips
// through the compute engine.
func TestRecordUsageNeverChangesState(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 4, RAM: 8, Disk: 40})

	// A fresh node stays NOTREADY: it has claimed nothing yet.
	if _, err := s.RecordUsage(t.Context(), "n-1", core.Resources{CPU: 1, RAM: 2, Disk: 5}, now()); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.GetNode(t.Context(), "n-1"); n.State.Ready() {
		t.Fatalf("state = %q, want a fresh node left NOTREADY until probed", n.State)
	}

	// A node that failed its probe does not get promoted by talking.
	if _, err := s.SetReadiness(t.Context(), "n-1", core.NodeNotReady, "probe failed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordUsage(t.Context(), "n-1", core.Resources{}, now()); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.GetNode(t.Context(), "n-1"); n.State.Ready() {
		t.Fatalf("state = %q, want a probed-out node left NOTREADY", n.State)
	}

	// Taking a node out of service must also survive the heartbeat.
	if _, err := s.SetNodeEnabled(t.Context(), "n-1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordUsage(t.Context(), "n-1", core.Resources{}, now()); err != nil {
		t.Fatal(err)
	}
	n, _ := s.GetNode(t.Context(), "n-1")
	if n.State.Ready() {
		t.Fatalf("state = %q, want a node out of service left NOTREADY", n.State)
	}
	if n.Enabled {
		t.Fatal("Enabled = true; taking a node out of service must survive the heartbeat")
	}
}

// Reported usage is informational and must not move available capacity.
func TestRecordUsageDoesNotChangeAvailability(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 8, RAM: 16, Disk: 200})

	if _, err := s.RecordUsage(t.Context(), "n-1", core.Resources{CPU: 7, RAM: 15, Disk: 199}, now()); err != nil {
		t.Fatal(err)
	}

	n, _ := s.GetNode(t.Context(), "n-1")
	if n.Available != n.Total {
		t.Fatalf("available = %+v; a usage report must not change reservations", n.Available)
	}
	if n.Usage.CPU != 7 {
		t.Fatalf("usage = %+v, want it recorded", n.Usage)
	}
}

func TestAttachAndDetach(t *testing.T) {
	s := newStore(t)
	mustNode(t, s, "n-1", core.Resources{CPU: 4, RAM: 8, Disk: 40})

	for _, id := range []string{"i-1", "i-2", "i-3"} {
		if err := s.AttachInstance(t.Context(), "n-1", id); err != nil {
			t.Fatal(err)
		}
	}
	// Attaching the same instance twice must not duplicate it.
	if err := s.AttachInstance(t.Context(), "n-1", "i-2"); err != nil {
		t.Fatal(err)
	}

	n, _ := s.GetNode(t.Context(), "n-1")
	if len(n.Instances) != 3 {
		t.Fatalf("instances = %v, want 3 without duplicates", n.Instances)
	}

	if err := s.DetachInstance(t.Context(), "n-1", "i-2"); err != nil {
		t.Fatal(err)
	}
	n, _ = s.GetNode(t.Context(), "n-1")
	if len(n.Instances) != 2 {
		t.Fatalf("instances = %v, want i-2 removed", n.Instances)
	}
	for _, id := range n.Instances {
		if id == "i-2" {
			t.Fatal("i-2 still attached")
		}
	}

	// Detaching something absent is a no-op, not an error.
	if err := s.DetachInstance(t.Context(), "n-1", "ghost"); err != nil {
		t.Fatal(err)
	}
}

func TestAttachMissingNode(t *testing.T) {
	s := newStore(t)
	err := s.AttachInstance(t.Context(), "ghost", "i-1")
	if !errors.Is(err, core.ErrNodeNotFound) {
		t.Fatalf("got %v, want ErrNodeNotFound", err)
	}
}

func TestSetStateMissingNode(t *testing.T) {
	s := newStore(t)
	if _, err := s.SetReadiness(t.Context(), "ghost", core.NodeReady, ""); !errors.Is(err, core.ErrNodeNotFound) {
		t.Fatalf("got %v, want ErrNodeNotFound", err)
	}
}

// --- listing -------------------------------------------------------------

func TestListIsOrdered(t *testing.T) {
	s := newStore(t)
	for _, id := range []string{"n-3", "n-1", "n-2"} {
		mustNode(t, s, id, core.Resources{CPU: 1, RAM: 1, Disk: 1})
	}
	nodes, err := s.ListNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"n-1", "n-2", "n-3"}
	for i, n := range nodes {
		if n.ID != want[i] {
			t.Fatalf("order = %v, want %v", ids(nodes), want)
		}
	}
}

func ids(nodes []core.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}

// now is a fixed clock so heartbeat assertions are not time-sensitive.
func now() time.Time { return time.Now().UTC() }

// A heartbeat must not make a node schedulable.
//
// Readiness belongs to the controller's probe, which round-trips through the
// compute engine. An agent with an unreachable libvirt socket answers HTTP
// perfectly while being unable to run a VM, so promoting on heartbeat would put
// a broken host back into the scheduler's eligible set.
func TestRecordUsageDoesNotSetReadiness(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	if _, err := s.Register(ctx, core.Node{
		ID: "n1", Name: "compute-1", Enabled: true,
		Total: core.Resources{CPU: 8, RAM: 16, Disk: 100},
	}); err != nil {
		t.Fatal(err)
	}

	// The node is unusable but still talking.
	if _, err := s.SetReadiness(ctx, "n1", core.NodeNotReady, "probe failed: connection refused"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if _, err := s.RecordUsage(ctx, "n1", core.Resources{CPU: 1, RAM: 1, Disk: 1}, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}

	n, err := s.GetNode(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if n.State.Ready() {
		t.Fatal("a heartbeat must not make a node READY; that is the probe's decision")
	}
	if n.LastHeartbeat.IsZero() {
		t.Fatal("heartbeat should still be recorded")
	}
	if n.Usage.CPU != 1 {
		t.Fatalf("usage = %+v, want it recorded", n.Usage)
	}
}

// The same invariant, on the in-memory store, since both implement the
// interface and neither may be the odd one out.
func TestMemoryRecordUsageDoesNotSetReadiness(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	if _, err := s.Register(ctx, core.Node{
		ID: "n1", Name: "compute-1", Enabled: true,
		Total: core.Resources{CPU: 8, RAM: 16, Disk: 100},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetReadiness(ctx, "n1", core.NodeNotReady, "probe failed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordUsage(ctx, "n1", core.Resources{CPU: 1}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	n, err := s.GetNode(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if n.State.Ready() {
		t.Fatal("a heartbeat must not make a node READY")
	}
}
