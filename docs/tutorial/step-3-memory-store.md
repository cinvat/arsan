# Step 3: In-Memory Store

## Goal
Implement `core.InstanceStore` and `core.NodeStore` with thread-safe, CAS-based operations. The in-memory store is used for all tests and local development.

---

## File: `internal/store/memory/store.go`

```go
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/samber/lo"
)

type Store struct {
	mu        sync.RWMutex
	instances map[string]core.Instance
	nodes     map[string]core.Node
}

func New() *Store {
	return &Store{
		instances: make(map[string]core.Instance),
		nodes:     make(map[string]core.Node),
	}
}

// ========== Instance Operations ==========

func (s *Store) Create(ctx context.Context, i core.Instance) (core.Instance, error) {
	if err := ctx.Err(); err != nil { return core.Instance{}, err }
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.instances[i.ID]; ok {
		return core.Instance{}, fmt.Errorf("instance %s exists: %w", i.ID, core.ErrInvalidRequest)
	}
	now := time.Now().UTC()
	i.CreatedAt, i.UpdatedAt = now, now
	s.instances[i.ID] = i
	return i, nil
}

func (s *Store) Get(ctx context.Context, id string) (core.Instance, error) {
	if err := ctx.Err(); err != nil { return core.Instance{}, err }
	s.mu.RLock()
	defer s.mu.RUnlock()
	i, ok := s.instances[id]
	if !ok { return core.Instance{}, fmt.Errorf("get %s: %w", id, core.ErrInstanceNotFound) }
	return i, nil
}

func (s *Store) List(ctx context.Context) ([]core.Instance, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := lo.Values(s.instances)
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

func (s *Store) Update(ctx context.Context, i core.Instance) (core.Instance, error) {
	if err := ctx.Err(); err != nil { return core.Instance{}, err }
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.instances[i.ID]
	if !ok { return core.Instance{}, fmt.Errorf("update %s: %w", i.ID, core.ErrInstanceNotFound) }
	if cur.State != i.State {
		return core.Instance{}, fmt.Errorf("update %s would change state %s->%s: %w",
			i.ID, cur.State, i.State, core.ErrInvalidTransition)
	}
	i.CreatedAt = cur.CreatedAt
	i.UpdatedAt = time.Now().UTC()
	s.instances[i.ID] = i
	return i, nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil { return err }
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.instances, id)
	return nil
}

// Transition is the ONLY way instance state changes.
// Compare-and-swap on `from`. Returns ErrInvalidTransition if current state != from.
func (s *Store) Transition(ctx context.Context, id string, from, to core.InstanceState) (core.Instance, error) {
	if err := ctx.Err(); err != nil { return core.Instance{}, err }
	s.mu.Lock()
	defer s.mu.Unlock()

	i, ok := s.instances[id]
	if !ok { return core.Instance{}, fmt.Errorf("transition %s: %w", id, core.ErrInstanceNotFound) }
	if i.State != from {
		return core.Instance{}, fmt.Errorf("transition %s %s->%s: %w",
			id, i.State, to, core.ErrInvalidTransition)
	}
	i.State = to
	i.UpdatedAt = time.Now().UTC()
	s.instances[id] = i
	return i, nil
}

func (s *Store) ReleaseReservations(ctx context.Context, instanceID, nodeID string) error {
	if err := ctx.Err(); err != nil { return err }
	s.mu.Lock()
	defer s.mu.Unlock()

	i, ok := s.instances[instanceID]
	if !ok || nodeID == "" { return nil }

	n, ok := s.nodes[nodeID]
	if !ok { return nil }

	n.Available = n.Available.Add(i.Flavor.Resources())
	if n.Available.VCPU > n.Total.VCPU { n.Available.VCPU = n.Total.VCPU }
	if n.Available.RAM > n.Total.RAM { n.Available.RAM = n.Total.RAM }
	if n.Available.Disk > n.Total.Disk { n.Available.Disk = n.Total.Disk }
	n.Instances = lo.Filter(n.Instances, func(s string, _ int) bool { return s != instanceID })
	s.nodes[nodeID] = n
	return nil
}

// ========== Node Operations ==========

func (s *Store) Register(ctx context.Context, n core.Node) (core.Node, error) {
	if err := ctx.Err(); err != nil { return core.Node{}, err }
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.nodes[n.ID]; ok {
		existing.Name = n.Name
		existing.Address = n.Address
		s.nodes[n.ID] = existing
		return existing, nil
	}
	// NOTREADY until the controller's probe succeeds. A new node has
	// proved nothing yet, and a schedulable node nobody has reached is the
	// exact window placement must not be able to use.
	n.State = core.NodeNotReady
	n.Available = n.Total
	n.Instances = nil
	s.nodes[n.ID] = n
	return n, nil
}

func (s *Store) GetNode(ctx context.Context, id string) (core.Node, error) {
	if err := ctx.Err(); err != nil { return core.Node{}, err }
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	if !ok { return core.Node{}, fmt.Errorf("get node %s: %w", id, core.ErrNodeNotFound) }
	return n, nil
}

func (s *Store) ListNodes(ctx context.Context) ([]core.Node, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := lo.Values(s.nodes)
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

func (s *Store) SetState(ctx context.Context, id string, state core.NodeState) (core.Node, error) {
	if err := ctx.Err(); err != nil { return core.Node{}, err }
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok { return core.Node{}, fmt.Errorf("set state %s: %w", id, core.ErrNodeNotFound) }
	n.State = state
	s.nodes[id] = n
	return n, nil
}

func (s *Store) RecordUsage(ctx context.Context, id string, usage core.Resources, at time.Time) (core.Node, error) {
	if err := ctx.Err(); err != nil { return core.Node{}, err }
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok { return core.Node{}, fmt.Errorf("record usage %s: %w", id, core.ErrNodeNotFound) }
	n.LastHeartbeat = at
	// Readiness is not set here: a heartbeat proves the agent is talking, not
	// that it can run a VM. The controller decides that, by probing.
	n.Usage = usage
	s.nodes[id] = n
	return n, nil
}

// ReserveNodeResources is the single atomic write the scheduler makes.
// Check ALL dimensions before mutating ANY — a partial reservation would overcommit.
func (s *Store) ReserveNodeResources(ctx context.Context, nodeID string, r core.Resources) error {
	if err := ctx.Err(); err != nil { return err }
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[nodeID]
	if !ok { return fmt.Errorf("reserve %s: %w", nodeID, core.ErrNodeNotFound) }
	if !n.Available.Fits(r) {
		return fmt.Errorf("reserve %s (avail %+v): %w", nodeID, n.Available, core.ErrCapacityConflict)
	}
	n.Available = n.Available.Sub(r)
	s.nodes[nodeID] = n
	return nil
}

func (s *Store) ReleaseNodeResources(ctx context.Context, nodeID string, r core.Resources) error {
	if err := ctx.Err(); err != nil { return err }
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok { return nil }
	n.Available = n.Available.Add(r)
	if n.Available.VCPU > n.Total.VCPU { n.Available.VCPU = n.Total.VCPU }
	if n.Available.RAM > n.Total.RAM { n.Available.RAM = n.Total.RAM }
	if n.Available.Disk > n.Total.Disk { n.Available.Disk = n.Total.Disk }
	s.nodes[nodeID] = n
	return nil
}

func (s *Store) AttachInstance(ctx context.Context, nodeID, instanceID string) error {
	if err := ctx.Err(); err != nil { return err }
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok { return fmt.Errorf("attach %s to %s: %w", instanceID, nodeID, core.ErrNodeNotFound) }
	n.Instances = append(n.Instances, instanceID)
	s.nodes[nodeID] = n
	return nil
}

func (s *Store) DetachInstance(ctx context.Context, nodeID, instanceID string) error {
	if err := ctx.Err(); err != nil { return err }
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok { return nil }
	n.Instances = lo.Filter(n.Instances, func(s string, _ int) bool { return s != instanceID })
	s.nodes[nodeID] = n
	return nil
}
```

---

## Key Design Points

### CAS Transitions
```go
func (s *Store) Transition(ctx, id, from, to) (Instance, error) {
    // ... lock ...
    if i.State != from {
        return Instance{}, ErrInvalidTransition
    }
    i.State = to
    // ...
}
```
**Never** read instance, modify, then Update. Always use `Transition` for state changes.

### Atomic Reserve (All-or-Nothing)
```go
func (s *Store) ReserveNodeResources(ctx, nodeID, r) error {
    // Check ALL dimensions before mutating ANY
    if !n.Available.Fits(r) { return ErrCapacityConflict }
    n.Available = n.Available.Sub(r)
}
```
This mirrors SQL `WHERE available_vcpu >= $1 AND available_ram >= $2 ...`.

### Idempotent Release
```go
func (s *Store) ReleaseReservations(ctx, instanceID, nodeID) error {
    // Returns nil if instance/node missing — safe to call multiple times
}
```

---

## Test: `internal/store/memory/store_test.go`

```go
package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cinvat/arsan/internal/core"
)

func TestReserveNoOvercommit(t *testing.T) {
	s := New()
	s.nodes["n1"] = core.Node{
		ID: "n1", State: core.NodeReady,
		Available: core.Resources{VCPU: 2, RAM: 4, Disk: 10},
	}

	const workers = 50
	var ok atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			err := s.ReserveNodeResources(context.Background(), "n1",
				core.Resources{VCPU: 1, RAM: 1, Disk: 1})
			if err == nil { ok.Add(1) }
		}()
	}
	wg.Wait()
	if got := ok.Load(); got != 2 { t.Fatalf("reserved %d, want 2", got) }
}

func TestTransitionRejectsWrongFrom(t *testing.T) {
	s := New()
	s.instances["i1"] = core.Instance{ID: "i1", State: core.StateRunning}
	_, err := s.Transition(context.Background(), "i1", core.StatePending, core.StateBuilding)
	if err == nil || !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestReleaseReservationsIdempotent(t *testing.T) {
	s := New()
	// Should not panic or error
	s.ReleaseReservations(context.Background(), "missing", "n1")
}
```

---

## Run Tests

```bash
go test -race ./internal/store/memory/...
```

---

## Next

Step 4: Scheduler — Filter → Weigh → Claim