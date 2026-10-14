# Step 4: Scheduler — Filter → Weigh → Claim

## Goal
Implement the placement logic: filter eligible hosts, weigh them, then atomically claim capacity.

---

## File: `internal/scheduler/scheduler.go`

```go
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cinvat/arsan/internal/core"
	"github.com/samber/lo"
)

const DefaultMaxAttempts = 5

var MaxPlacementAttempts = DefaultMaxAttempts

type DriverFor func(nodeID string) (core.Driver, error)

type Config struct {
	Instances   core.InstanceStore
	Nodes       core.NodeStore
	DriverFor   DriverFor
	MaxAttempts int
	Log         Logger
}

type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type Scheduler struct {
	instances   core.InstanceStore
	nodes       core.NodeStore
	driverFor   DriverFor
	maxAttempts int
	log         Logger

	requestFilters []core.RequestFilter
	hostFilters    []core.NodeFilter
	weighers       []core.Weigher
}

func New(cfg Config) *Scheduler {
	max := cfg.MaxAttempts
	if max < 1 { max = DefaultMaxAttempts }
	s := &Scheduler{
		instances:   cfg.Instances,
		nodes:       cfg.Nodes,
		driverFor:   cfg.DriverFor,
		maxAttempts: max,
		log:         cfg.Log,
	}
	s.requestFilters = []core.RequestFilter{
		FlavorFilter{}, ExcludeNodesFilter{},
	}
	s.hostFilters = []core.NodeFilter{
		StateFilter{},      // 1. READY and enabled only
		CapacityFilter{},   // 2. available >= flavor
		ArchFilter{},       // 3. arch match
		GroupFilter{},      // 4. anti-affinity
		DriverValidateFilter{DriverFor: cfg.DriverFor}, // 5. driver.Validate
	}
	s.weighers = []core.Weigher{
		FreeRAMWeigher{}, FreeCPUWeigher{}, FewestInstancesWeigher{},
	}
	return s
}

// Schedule is the single code path for both initial placement and re-placement.
func (s *Scheduler) Schedule(ctx context.Context, req core.PlacementRequest) (core.Placement, error) {
	if req.InstanceID == "" {
		return core.Placement{}, fmt.Errorf("schedule: %w", core.ErrInvalidRequest)
	}

	var lastConflict string
	for attempt := 1; attempt <= s.maxAttempts; attempt++ {
		req.Attempt = attempt

		if err := s.runRequestFilters(ctx, req); err != nil {
			return core.Placement{}, err
		}

		hosts, err := s.nodes.ListNodes(ctx)
		if err != nil {
			return core.Placement{}, fmt.Errorf("list nodes: %w", err)
		}

		eligible, err := s.runNodeFilters(ctx, req, hosts)
		if err != nil {
			return core.Placement{}, fmt.Errorf("filter hosts: %w", err)
		}
		if len(eligible) == 0 {
			return core.Placement{}, fmt.Errorf("attempt %d: %w (rejected: %s)",
				attempt, core.ErrNoValidNode, strings.Join(s.rejectReasons(req, hosts), ", "))
		}

		weighed, err := s.weigh(ctx, req, eligible)
		if err != nil { return core.Placement{}, fmt.Errorf("weigh: %w", err) }
		winner, runnerUp := best(weighed)

		// THE CLAIM — only write the scheduler makes
		want := req.Flavor.Resources()
		if err := s.nodes.ReserveNodeResources(ctx, winner.NodeID, want); err != nil {
			if errors.Is(err, core.ErrCapacityConflict) {
				lastConflict = fmt.Sprintf("attempt %d: node %s no longer fits", attempt, winner.NodeID)
				s.log.Warn("claim lost, retrying", "attempt", attempt, "node", winner.NodeID)
				continue
			}
			return core.Placement{}, fmt.Errorf("claim %s: %w", winner.NodeID, err)
		}
		if err := s.nodes.AttachInstance(ctx, winner.NodeID, req.InstanceID); err != nil {
			// Rollback reservation on attach failure
			_ = s.nodes.ReleaseNodeResources(ctx, winner.NodeID, want)
			return core.Placement{}, fmt.Errorf("attach: %w", err)
		}

		return core.Placement{
			InstanceID: req.InstanceID, NodeID: winner.NodeID,
			Generation: req.Generation, Attempt: attempt,
			Reason: explain(winner, runnerUp), RunnerUp: runnerUp,
		}, nil
	}

	return core.Placement{}, fmt.Errorf("after %d attempts: %w (%s)",
		s.maxAttempts, core.ErrNoValidNode, lastConflict)
}

func (s *Scheduler) runRequestFilters(ctx context.Context, req core.PlacementRequest) error {
	for _, f := range s.requestFilters {
		if err := f.FilterRequest(ctx, req); err != nil {
			return fmt.Errorf("request filter %s: %w", f.Name(), err)
		}
	}
	return nil
}

func (s *Scheduler) runNodeFilters(ctx context.Context, req core.PlacementRequest, hosts []core.Node) ([]core.Node, error) {
	cur := hosts
	for _, f := range s.hostFilters {
		next, err := f.FilterNodes(ctx, req, cur)
		if err != nil { return nil, fmt.Errorf("host filter %s: %w", f.Name(), err) }
		cur = next
	}
	return cur, nil
}

func (s *Scheduler) weigh(ctx context.Context, req core.PlacementRequest, hosts []core.Node) ([]WeighedHost, error) {
	out := make([]WeighedHost, 0, len(hosts))
	for _, h := range hosts {
		wh := WeighedHost{NodeID: h.ID, Detail: make(map[string]int, len(s.weighers))}
		for _, w := range s.weighers {
			v, err := w.WeightNode(ctx, req, h)
			if err != nil {
				s.log.Warn("weigher skipped", "weigher", w.Name(), "node", h.ID, "err", err)
				continue
			}
			if v < 0 { continue } // negative = bug, ignore
			wh.Score += v
			wh.Detail[w.Name()] = v
		}
		out = append(out, wh)
	}
	return out, nil
}

type WeighedHost struct {
	NodeID string
	Score  int
	Detail map[string]int
}

func best(hosts []WeighedHost) (WeighedHost, string) {
	sorted := lo.Map(hosts, func(h WeighedHost, _ int) WeighedHost { return h })
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Score != sorted[j].Score { return sorted[i].Score > sorted[j].Score }
		return sorted[i].NodeID < sorted[j].NodeID // deterministic tie-break
	})
	runnerUp := ""
	if len(sorted) > 1 {
		runnerUp = fmt.Sprintf("%s (score %d)", sorted[1].NodeID, sorted[1].Score)
	}
	return sorted[0], runnerUp
}

func (s *Scheduler) rejectReasons(req core.PlacementRequest, hosts []core.Node) []string {
	if len(hosts) == 0 { return []string{"no nodes registered"} }
	var out []string
	for _, f := range s.hostFilters {
		kept, _ := f.FilterNodes(context.Background(), req, hosts)
		if len(kept) < len(hosts) {
			out = append(out, fmt.Sprintf("%s dropped %d", f.Name(), len(hosts)-len(kept)))
		}
	}
	return out
}

func explain(w WeighedHost, runnerUp string) string {
	keys := lo.Keys(w.Detail)
	sort.Strings(keys)
	parts := lo.Map(keys, func(k string, _ int) string { return fmt.Sprintf("%s=%d", k, w.Detail[k]) })
	s := fmt.Sprintf("%s won with %s (total %d)", w.NodeID, strings.Join(parts, " "), w.Score)
	if runnerUp != "" { s += "; next best " + runnerUp }
	return s
}
```

---

## Filters — `internal/scheduler/filter_node.go`

```go
package scheduler

import (
	"context"

	"github.com/cinvat/arsan/internal/core"
)

type StateFilter struct{}
func (StateFilter) Name() string { return "NodeState" }
func (StateFilter) FilterNodes(_ context.Context, _ core.PlacementRequest, hosts []core.Node) ([]core.Node, error) {
	out := hosts[:0:0]
	for _, h := range hosts {
		if h.State == core.NodeReady && h.Enabled { out = append(out, h) }
	}
	return out, nil
}

type CapacityFilter struct{}
func (CapacityFilter) Name() string { return "Capacity" }
func (CapacityFilter) FilterNodes(_ context.Context, req core.PlacementRequest, hosts []core.Node) ([]core.Node, error) {
	want := req.Flavor.Resources()
	out := hosts[:0:0]
	for _, h := range hosts {
		if h.Available.Fits(want) { out = append(out, h) }
	}
	return out, nil
}

type ArchFilter struct{}
func (ArchFilter) Name() string { return "Arch" }
func (ArchFilter) FilterNodes(_ context.Context, req core.PlacementRequest, hosts []core.Node) ([]core.Node, error) {
	out := hosts[:0:0]
	for _, h := range hosts {
		if h.Arch == req.Flavor.Name || h.Arch == "" { out = append(out, h) } // simplified
	}
	return out, nil
}

type GroupFilter struct{}
func (GroupFilter) Name() string { return "Group" }
func (GroupFilter) FilterNodes(_ context.Context, req core.PlacementRequest, hosts []core.Node) ([]core.Node, error) {
	// Anti-affinity: exclude nodes with instances from same group
	return hosts, nil // TODO
}

// DriverValidateFilter — the one filter that calls the driver
type DriverValidateFilter struct{ DriverFor func(nodeID string) (core.Driver, error) }
func (DriverValidateFilter) Name() string { return "DriverValidate" }
func (f DriverValidateFilter) FilterNodes(ctx context.Context, req core.PlacementRequest, hosts []core.Node) ([]core.Node, error) {
	out := make([]core.Node, 0, len(hosts))
	for _, h := range hosts {
		d, err := f.DriverFor(h.ID)
		if err != nil { continue } // node unreachable → exclude
		if err := d.Validate(ctx, req.Flavor); err != nil { continue }
		out = append(out, h)
	}
	return out, nil
}
```

---

## Weighers — `internal/scheduler/weigh.go`

```go
package scheduler

import (
	"context"

	"github.com/cinvat/arsan/internal/core"
)

type FreeRAMWeigher struct{}
func (FreeRAMWeigher) Name() string { return "FreeRAM" }
func (FreeRAMWeigher) WeightNode(_ context.Context, _ core.PlacementRequest, h core.Node) (int, error) {
	return h.Available.RAM, nil // MiB
}

type FreeCPUWeigher struct{}
func (FreeCPUWeigher) Name() string { return "FreeCPU" }
func (FreeCPUWeigher) WeightNode(_ context.Context, _ core.PlacementRequest, h core.Node) (int, error) {
	return h.Available.VCPU * 1024, nil
}

type FewestInstancesWeigher struct{}
func (FewestInstancesWeigher) Name() string { return "FewestInstances" }
func (FewestInstancesWeigher) WeightNode(_ context.Context, _ core.PlacementRequest, h core.Node) (int, error) {
	return 100 - len(h.Instances), nil // spread load
}
```

---

## Key Design Points

### Claim is the Decision
```go
// Everything before ReserveNodeResources is a PROPOSAL
// The claim makes it real
if err := s.nodes.ReserveNodeResources(ctx, winner.NodeID, want); err != nil {
    if errors.Is(err, core.ErrCapacityConflict) { continue } // retry with fresh data
    return err
}
```

### Lost Claims Retry (Bounded)
- A lost claim = our timing problem, not the user's
- Retry with **fresh node list** (re-read on each attempt)
- Bounded at `MaxPlacementAttempts = 5`

### Deterministic Tie-Breaking
```go
sort.Slice(sorted, func(i, j int) bool {
    if sorted[i].Score != sorted[j].Score { return sorted[i].Score > sorted[j].Score }
    return sorted[i].NodeID < sorted[j].NodeID // stable
})
```
Without this, winner depends on map iteration order → retries reshuffle cluster.

### Per-Node Errors Are Swallowed
```go
// In DriverValidateFilter:
if err := d.Validate(ctx, req.Flavor); err != nil { continue } // exclude, don't abort
```
A broken driver on one node must not block the whole cluster.

---

## Test: `internal/scheduler/scheduler_test.go`

```go
package scheduler

import (
	"context"
	"testing"

	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/store/memory"
)

func TestSchedulePicksBestFittingHost(t *testing.T) {
	s := memory.New()
	s.nodes["n1"] = core.Node{ID: "n1", State: core.NodeReady, Enabled: true, Available: core.Resources{VCPU: 4, RAM: 8, Disk: 50}}
	s.nodes["n2"] = core.Node{ID: "n2", State: core.NodeReady, Enabled: true, Available: core.Resources{VCPU: 8, RAM: 16, Disk: 100}}
	driverFor := func(string) (core.Driver, error) { return fake.New(), nil }

	sched := New(Config{Instances: s, Nodes: s, DriverFor: driverFor})
	placed, err := sched.Schedule(context.Background(), core.PlacementRequest{
		InstanceID: "i-1", Flavor: core.Flavor{VCPU: 2, RAM: 4, Disk: 20},
	})
	if err != nil { t.Fatal(err) }
	if placed.NodeID != "n2" { t.Fatalf("got %s, want n2", placed.NodeID) }
}

func TestScheduleNoValidHost(t *testing.T) {
	s := memory.New()
	s.nodes["n1"] = core.Node{ID: "n1", State: core.NodeReady, Enabled: true, Available: core.Resources{VCPU: 1, RAM: 1, Disk: 1}}
	driverFor := func(string) (core.Driver, error) { return fake.New(), nil }

	sched := New(Config{Instances: s, Nodes: s, DriverFor: driverFor})
	_, err := sched.Schedule(context.Background(), core.PlacementRequest{
		InstanceID: "i-1", Flavor: core.Flavor{VCPU: 2, RAM: 4, Disk: 20},
	})
	if !errors.Is(err, core.ErrNoValidNode) { t.Fatalf("want ErrNoValidNode, got %v", err) }
}

func TestBestIsDeterministic(t *testing.T) {
	hosts := []WeighedHost{{NodeID: "n2", Score: 5}, {NodeID: "n1", Score: 5}}
	for i := 0; i < 100; i++ {
		got, _ := best(hosts)
		if got.NodeID != "n1" { t.Fatalf("iteration %d: got %s, want n1", i, got.NodeID) }
	}
}
```

---

## Run Tests

```bash
go test -race ./internal/scheduler/...
```

---

## Next

Step 5: Compute — Instance State Machine