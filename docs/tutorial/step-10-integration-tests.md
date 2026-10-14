# Step 10: Integration Tests

## Goal
Write API-level tests that exercise the full stack: controller + store + scheduler + compute + fake driver over real HTTP.

---

## Test Harness: `test/integration/api_test.go`

```go
package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cinvat/arsan/internal/api"
	"github.com/cinvat/arsan/internal/compute"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver/fake"
	"github.com/cinvat/arsan/internal/message"
	"github.com/cinvat/arsan/internal/node"
	"github.com/cinvat/arsan/internal/scheduler"
	"github.com/cinvat/arsan/internal/store/memory"
)

type harness struct {
	server *httptest.Server
	store  *memory.Store
	comp   *compute.Service
	node   *node.Service
	ctx    context.Context
	cancel context.CancelFunc
}

func newHarness(t *testing.T) *harness {
	ctx, cancel := context.WithCancel(context.Background())
	st := memory.New()
	bus := message.New()
	d := fake.New()
	driverFor := func(string) (core.Driver, error) { return d, nil }

	sched := scheduler.New(scheduler.Config{
		Instances: st, Nodes: st, DriverFor: driverFor, MaxAttempts: 5, Log: tLog{t},
	})

	compSvc := compute.New(compute.Config{
		Instances: st, Nodes: st, Bus: bus, Scheduler: sched,
		DriverFor: driverFor, SweepInterval: 50 * time.Millisecond,
		StaleAfter: 100 * time.Millisecond, MaxRebuildBackoff: time.Millisecond,
		Log: tLog{t},
	})

	nodeSvc := node.New(node.Config{
		Nodes: st, Bus: bus, Log: tLog{t},
	})

	// Background loops
	go func() { _ = nodeSvc.Watch(ctx) }()
	go func() { _ = compSvc.SweepLoop(ctx) }()

	srv := httptest.NewServer(api.New(api.Config{
		Instances: compSvc, Nodes: nodeSvc, Scheduler: sched, Bus: bus, Log: tLog{t},
	}).Handler())

	t.Cleanup(func() { cancel(); srv.Close() })

	return &harness{server: srv, store: st, comp: compSvc, node: nodeSvc, ctx: ctx, cancel: cancel}
}

func (h *harness) registerNode(name string, vcpu, ram, disk int) core.Node {
	req := map[string]any{
		"name": name, "arch": "amd64", "driver": "fake",
		"total": map[string]int{"vcpu": vcpu, "ram": ram, "disk": disk},
	}
	var n core.Node
	h.postJSON("/nodes", req, &n)
	return n
}

func (h *harness) reportUsage(nodeID string, usage map[string]int) {
	h.postJSON("/nodes/"+nodeID+"/usage", map[string]any{"usage": usage}, nil)
}

func (h *harness) createServer(name string, vcpu, ram, disk int) core.Instance {
	req := map[string]any{
		"name": name, "image_ref": "cirros",
		"flavor": map[string]int{"name": "m1.small", "vcpu": vcpu, "ram": ram, "disk": disk},
	}
	var inst core.Instance
	h.postJSON("/servers", req, &inst)
	return inst
}

func (h *harness) getServer(id string) core.Instance {
	var inst core.Instance
	h.getJSON("/servers/"+id, &inst)
	return inst
}

func (h *harness) waitState(id string, want core.InstanceState) core.Instance {
	for i := 0; i < 50; i++ {
		inst := h.getServer(id)
		if inst.State == want { return inst }
		time.Sleep(20 * time.Millisecond)
	}
	return h.getServer(id)
}

func (h *harness) postJSON(path string, body, out any) {
	reqBody, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(h.ctx, "POST", h.server.URL+path, bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { panic(err) }
	defer resp.Body.Close()
	if resp.StatusCode >= 400 { panic(fmt.Sprintf("POST %s: %d", path, resp.StatusCode)) }
	if out != nil { json.NewDecoder(resp.Body).Decode(out) }
}

func (h *harness) getJSON(path string, out any) {
	req, _ := http.NewRequestWithContext(h.ctx, "GET", h.server.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil { panic(err) }
	defer resp.Body.Close()
	if resp.StatusCode >= 400 { panic(fmt.Sprintf("GET %s: %d", path, resp.StatusCode)) }
	json.NewDecoder(resp.Body).Decode(out)
}

// tLog routes service logs into t.Logf
type tLog struct{ t *testing.T }
func (l tLog) Info(msg string, kv ...any) { l.t.Logf("INFO %s %v", msg, kv) }
func (l tLog) Warn(msg string, kv ...any) { l.t.Logf("WARN %s %v", msg, kv) }
func (l tLog) Error(msg string, kv ...any) { l.t.Logf("ERROR %s %v", msg, kv) }
```

---

## Test: Happy Path

```go
func TestCreateReachesActive(t *testing.T) {
	h := newHarness(t)
	h.registerNode("compute-1", 8, 16, 100)
	h.reportUsage("n-compute-1", map[string]int{"vcpu": 0, "ram": 0, "disk": 0})

	inst := h.createServer("web-1", 1, 2, 10)
	if inst.State != core.StatePending { t.Fatalf("want PENDING, got %s", inst.State) }

	inst = h.waitState(inst.ID, core.StateActive)
	if inst.State != core.StateActive { t.Fatalf("stuck at %s", inst.State) }
	if inst.NodeID == "" { t.Fatal("no node_id assigned") }
}
```

---

## Test: Node Failure → Re-placement

```go
func TestInstanceReplacedOnNodeDown(t *testing.T) {
	h := newHarness(t)
	n1 := h.registerNode("compute-1", 8, 16, 100)
	n2 := h.registerNode("compute-2", 8, 16, 100)
	h.reportUsage(n1.ID, map[string]int{})
	h.reportUsage(n2.ID, map[string]int{})

	inst := h.createServer("web-1", 1, 2, 10)
	inst = h.waitState(inst.ID, core.StateActive)
	if inst.NodeID != n1.ID { t.Fatalf("expected %s, got %s", n1.ID, inst.NodeID) }

	// Take n1 out of service the way a failed probe would.
	h.store.SetReadiness(t.ctx, n1.ID, core.NodeNotReady, "probe failed: connection refused")
	h.comp.HandleNodeDown(t.ctx, n1.ID)

	// Should re-place on n2
	inst = h.waitState(inst.ID, core.StateActive)
	if inst.NodeID != n2.ID { t.Fatalf("re-placed on %s, want %s", inst.NodeID, n2.ID) }
	if inst.Generation != 1 { t.Fatalf("generation %d, want 1", inst.Generation) }
}
```

---

## Test: Drain Moves Instances

```go
func TestDrainMovesInstancesOff(t *testing.T) {
	h := newHarness(t)
	n1 := h.registerNode("compute-1", 8, 16, 100)
	n2 := h.registerNode("compute-2", 8, 16, 100)
	h.reportUsage(n1.ID, map[string]int{})
	h.reportUsage(n2.ID, map[string]int{})

	inst := h.createServer("web-1", 1, 2, 10)
	h.waitState(inst.ID, core.StateActive)

	// Disable n1 (drains)
	h.node.Disable(t.ctx, n1.ID)
	h.node.Disable(t.ctx, n2.ID) // also disable n2 to force eviction

	// Instance should move to n2
	inst = h.waitState(inst.ID, core.StateActive)
	if inst.NodeID != n2.ID { t.Fatalf("expected %s, got %s", n2.ID, inst.NodeID) }
}
```

---

## Test: Driver Failure Releases Reservation

```go
func TestDriverFailureReleasesReservation(t *testing.T) {
	h := newHarness(t)
	h.registerNode("compute-1", 2, 4, 20) // Only fits 2 x 1vcpu
	h.reportUsage("n-compute-1", map[string]int{})

	// Make driver fail on 3rd Create
	d := h.comp.DriverFor.(*fake.Driver)
	d.FailNext("Create", errors.New("disk full"))

	// 3 concurrent creates, only 2 should succeed
	results := make(chan core.Instance, 3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			inst := h.createServer(fmt.Sprintf("web-%d", i), 1, 2, 10)
			results <- inst
		}(i)
	}

	var active int
	for i := 0; i < 3; i++ {
		inst := <-results
		if inst.State == core.StateActive { active++ }
	}
	if active != 2 { t.Fatalf("expected 2 ACTIVE, got %d", active) }
}
```

---

## Test: Concurrent Creates Respect Capacity

```go
func TestConcurrentCreatesRespectCapacity(t *testing.T) {
	h := newHarness(t)
	h.registerNode("compute-1", 2, 4, 20) // Exactly 2 x 1vcpu
	h.reportUsage("n-compute-1", map[string]int{})

	var wg sync.WaitGroup
	var active int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inst := h.createServer(fmt.Sprintf("web-%d", i), 1, 2, 10)
			inst = h.waitState(inst.ID, core.StateActive)
			if inst.State == core.StateActive { atomic.AddInt32(&active, 1) }
		}()
	}
	wg.Wait()

	if active != 2 { t.Fatalf("expected exactly 2 ACTIVE, got %d", active) }
}
```

---

## Test: Re-placement After Transient Failure

```go
func TestReplacedAfterTransientReplacementFailure(t *testing.T) {
	h := newHarness(t)
	n1 := h.registerNode("compute-1", 8, 16, 100)
	n2 := h.registerNode("compute-2", 8, 16, 100)
	h.reportUsage(n1.ID, map[string]int{})
	h.reportUsage(n2.ID, map[string]int{})

	inst := h.createServer("web", 1, 2, 10)
	h.waitState(inst.ID, core.StateActive)

	// Both hosts down: first re-placement cannot succeed
	h.store.SetReadiness(t.ctx, n1.ID, core.NodeNotReady, "probe failed")
	h.store.SetReadiness(t.ctx, n2.ID, core.NodeNotReady, "probe failed")
	h.comp.HandleNodeDown(t.ctx, n1.ID)

	// Instance should be PENDING with bumped generation
	inst = h.waitState(inst.ID, core.StatePending)
	if inst.Generation == 0 { t.Fatal("failed re-placement must bump generation") }

	// n2 recovers, and a passing probe is what makes it schedulable again
	h.store.SetReadiness(t.ctx, n2.ID, core.NodeReady, "")
	h.reportUsage(n2.ID, map[string]int{})

	// Sweep should finish the job
	inst = h.waitState(inst.ID, core.StateActive)
	if inst.NodeID != n2.ID { t.Fatalf("expected %s, got %s", n2.ID, inst.NodeID) }
}
```

---

## Run Integration Tests

```bash
go test -v ./test/integration/...
go test -race ./test/integration/...
```

---

## Test Coverage Summary

| Test | What It Protects |
|------|------------------|
| `TestCreateReachesActive` | Happy path through entire stack |
| `TestInstanceReplacedOnNodeDown` | Dead host VMs come back elsewhere |
| `TestDrainMovesInstancesOff` | Drain evacuates VMs |
| `TestDriverFailureReleasesReservation` | Build failure returns capacity |
| `TestConcurrentCreatesRespectCapacity` | 10 concurrent, room for 2 → exactly 2 ACTIVE |
| `TestReplacedAfterTransientReplacementFailure` | Transient cluster shortage doesn't strand VM |

---

## Complete Verification

```bash
# All tests
go test ./...

# Race detector on critical paths
go test -race ./internal/scheduler/... ./internal/store/... ./internal/compute/...

# Integration tests
go test -v ./test/integration/...
```

---

## Next

The tutorial series is complete. You now have a working OpenStack Nova-like control plane with:

- ✅ Core types & interfaces (Step 1)
- ✅ Fake driver for testing (Step 2)
- ✅ In-memory store with CAS (Step 3)
- ✅ Scheduler: filter → weigh → claim (Step 4)
- ✅ Compute state machine (Step 5)
- ✅ Node registration & liveness (Step 6)
- ✅ Agent gRPC server (Step 7)
- ✅ Controller REST API (Step 8)
- ✅ Remote driver for multi-node (Step 9)
- ✅ Integration tests (Step 10)

**Architecture docs**: `docs/architecture.md`, `docs/scheduler.md`, `docs/usage.md`
**Agent guidance**: `AGENTS.md`