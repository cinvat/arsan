package memory

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/samber/lo"
)

type Store struct {
	mu        sync.RWMutex
	instances map[string]core.Instance
	nodes     map[string]core.Node
	flavors   map[string]core.Flavor
}

func New() *Store {
	return &Store{
		instances: make(map[string]core.Instance),
		nodes:     make(map[string]core.Node),
		flavors:   make(map[string]core.Flavor),
	}
}

func (s *Store) CreateFlavor(ctx context.Context, f core.Flavor) (core.Flavor, error) {
	if err := ctx.Err(); err != nil {
		return core.Flavor{}, err
	}
	if f.Name == "" {
		return core.Flavor{}, fmt.Errorf("flavor name required: %w", core.ErrInvalidRequest)
	}
	if err := f.VCPU.Validate(); err != nil {
		return core.Flavor{}, err
	}
	if f.RAM <= 0 || f.Disk <= 0 {
		return core.Flavor{}, fmt.Errorf("flavor must have positive ram and disk: %w", core.ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.flavors[f.Name]; ok {
		return core.Flavor{}, fmt.Errorf("flavor %s exists: %w", f.Name, core.ErrInvalidRequest)
	}
	s.flavors[f.Name] = f
	return f, nil
}

func (s *Store) GetFlavor(ctx context.Context, name string) (core.Flavor, error) {
	if err := ctx.Err(); err != nil {
		return core.Flavor{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.flavors[name]
	if !ok {
		return core.Flavor{}, fmt.Errorf("get flavor %s: %w", name, core.ErrFlavorNotFound)
	}
	return f, nil
}

func (s *Store) ListFlavors(ctx context.Context) ([]core.Flavor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.SortedFunc(maps.Values(s.flavors),
		func(a, b core.Flavor) int { return cmp.Compare(a.Name, b.Name) }), nil
}

func (s *Store) DeleteFlavor(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// A flavor in use must not vanish: existing VMs hold only a snapshot, and
	// deleting the definition would leave their sizing unexplained.
	for _, i := range s.instances {
		if i.Flavor.Name == name {
			return fmt.Errorf("flavor %s in use: %w", name, core.ErrInvalidRequest)
		}
	}
	delete(s.flavors, name)
	return nil
}

func (s *Store) Create(ctx context.Context, i core.Instance) (core.Instance, error) {
	if err := ctx.Err(); err != nil {
		return core.Instance{}, err
	}
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
	if err := ctx.Err(); err != nil {
		return core.Instance{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	i, ok := s.instances[id]
	if !ok {
		return core.Instance{}, fmt.Errorf("get %s: %w", id, core.ErrInstanceNotFound)
	}
	return i, nil
}

func (s *Store) List(ctx context.Context) ([]core.Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.SortedFunc(maps.Values(s.instances),
		func(a, b core.Instance) int { return cmp.Compare(a.ID, b.ID) }), nil
}

func (s *Store) Update(ctx context.Context, i core.Instance) (core.Instance, error) {
	if err := ctx.Err(); err != nil {
		return core.Instance{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.instances[i.ID]
	if !ok {
		return core.Instance{}, fmt.Errorf("update %s: %w", i.ID, core.ErrInstanceNotFound)
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.instances, id)
	return nil
}

func (s *Store) Transition(ctx context.Context, id string, from, to core.InstanceState) (core.Instance, error) {
	if err := ctx.Err(); err != nil {
		return core.Instance{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	i, ok := s.instances[id]
	if !ok {
		return core.Instance{}, fmt.Errorf("transition %s: %w", id, core.ErrInstanceNotFound)
	}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	i, ok := s.instances[instanceID]
	if !ok || nodeID == "" {
		return nil
	}
	n, ok := s.nodes[nodeID]
	if !ok {
		return nil
	}
	n.Available = n.Available.AddCapped(i.Flavor.Resources(), n.Total)
	// Free the instance's exclusive CPUs, if it held any. Count restoration
	// above and pin release here are the two halves of one teardown; the
	// instance record carries the pins so a retry frees exactly what was held.
	if len(i.PinnedCPUs) > 0 {
		free := make(map[int]bool, len(i.PinnedCPUs))
		for _, c := range i.PinnedCPUs {
			free[c] = true
		}
		n.PinnedCPUs = lo.Filter(n.PinnedCPUs, func(c int, _ int) bool { return !free[c] })
	}
	n.Instances = lo.Filter(n.Instances, func(s string, _ int) bool { return s != instanceID })
	s.nodes[nodeID] = n
	return nil
}

func (s *Store) Register(ctx context.Context, n core.Node) (core.Node, error) {
	if err := ctx.Err(); err != nil {
		return core.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.nodes[n.ID]; ok {
		existing.Name = n.Name
		s.nodes[n.ID] = existing
		return existing, nil
	}
	n.State = core.NodeNotReady
	// A fresh node has proved nothing yet. Naming that, rather than leaving
	// a bare NOTREADY, tells an operator it is waiting rather than broken.
	n.NotReadyReason = "registered, awaiting first probe"
	n.Available = n.Total
	n.Instances = nil
	s.nodes[n.ID] = n
	return n, nil
}

func (s *Store) GetNode(ctx context.Context, id string) (core.Node, error) {
	if err := ctx.Err(); err != nil {
		return core.Node{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	if !ok {
		return core.Node{}, fmt.Errorf("get node %s: %w", id, core.ErrNodeNotFound)
	}
	return n, nil
}

func (s *Store) ListNodes(ctx context.Context) ([]core.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.SortedFunc(maps.Values(s.nodes),
		func(a, b core.Node) int { return cmp.Compare(a.ID, b.ID) }), nil
}

// SetNodeEnabled takes a node in or out of service without touching its state.
func (s *Store) SetNodeEnabled(ctx context.Context, id string, enabled bool) (core.Node, error) {
	if err := ctx.Err(); err != nil {
		return core.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return core.Node{}, fmt.Errorf("set enabled %s: %w", id, core.ErrNodeNotFound)
	}
	n.Enabled = enabled
	s.nodes[id] = n
	return n, nil
}

// SetNodeSpec records the engine and capacity the probe reported.
func (s *Store) SetNodeSpec(ctx context.Context, id string, engine string, total core.Resources) (core.Node, error) {
	if err := ctx.Err(); err != nil {
		return core.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return core.Node{}, fmt.Errorf("set spec %s: %w", id, core.ErrNodeNotFound)
	}
	n.Driver = engine
	// Available is Total minus what reservations hold. The held amount
	// survives a capacity change: a host that grew makes room, one that
	// shrank keeps its claims (Available floors at zero, never negative).
	held := n.Total.Sub(n.Available)
	n.Total = total
	n.Available = core.Resources{
		CPU:  max(total.CPU-held.CPU, 0),
		RAM:  max(total.RAM-held.RAM, 0),
		Disk: max(total.Disk-held.Disk, 0),
	}
	s.nodes[id] = n
	return n, nil
}

// SetReadiness records readiness and the reason it is not ready.
func (s *Store) SetReadiness(ctx context.Context, id string, state core.NodeState, reason string) (core.Node, error) {
	if err := ctx.Err(); err != nil {
		return core.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return core.Node{}, fmt.Errorf("set readiness %s: %w", id, core.ErrNodeNotFound)
	}
	n.State = state
	n.NotReadyReason = reason
	s.nodes[id] = n
	return n, nil
}

func (s *Store) RecordUsage(ctx context.Context, id string, usage core.Resources, at time.Time) (core.Node, error) {
	if err := ctx.Err(); err != nil {
		return core.Node{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return core.Node{}, fmt.Errorf("record usage %s: %w", id, core.ErrNodeNotFound)
	}
	n.LastHeartbeat = at
	// A heartbeat records usage; it does not set readiness. Readiness is the
	// controller's probe to decide, because only the probe round-trips
	// through the compute engine.
	n.Usage = usage
	s.nodes[id] = n
	return n, nil
}

func (s *Store) ClaimCPUs(ctx context.Context, nodeID string, count int) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, fmt.Errorf("claim %d cpus: %w", count, core.ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("claim cpus %s: %w", nodeID, core.ErrNodeNotFound)
	}
	cpus, err := core.FreeCPUs(n.Total.CPU, n.PinnedCPUs, count)
	if err != nil {
		return nil, err
	}
	n.PinnedCPUs = append(slices.Clone(n.PinnedCPUs), cpus...)
	n.Available.CPU -= count
	s.nodes[nodeID] = n
	return cpus, nil
}

func (s *Store) ReleaseCPUs(ctx context.Context, nodeID string, cpus []int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return nil
	}
	free := make(map[int]bool, len(cpus))
	for _, c := range cpus {
		free[c] = true
	}
	kept := n.PinnedCPUs[:0:0]
	released := 0
	for _, c := range n.PinnedCPUs {
		if free[c] {
			released++
			continue
		}
		kept = append(kept, c)
	}
	n.PinnedCPUs = kept
	n.Available = n.Available.AddCapped(core.Resources{CPU: released}, n.Total)
	s.nodes[nodeID] = n
	return nil
}

func (s *Store) ReserveNodeResources(ctx context.Context, nodeID string, r core.Resources) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("reserve %s: %w", nodeID, core.ErrNodeNotFound)
	}
	if !n.Available.Fits(r) {
		return fmt.Errorf("reserve %s (avail %+v): %w", nodeID, n.Available, core.ErrCapacityConflict)
	}
	n.Available = n.Available.Sub(r)
	s.nodes[nodeID] = n
	return nil
}

func (s *Store) ReleaseNodeResources(ctx context.Context, nodeID string, r core.Resources) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return nil
	}
	n.Available = n.Available.AddCapped(r, n.Total)
	s.nodes[nodeID] = n
	return nil
}

func (s *Store) AttachInstance(ctx context.Context, nodeID, instanceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("attach %s to %s: %w", instanceID, nodeID, core.ErrNodeNotFound)
	}
	n.Instances = append(n.Instances, instanceID)
	s.nodes[nodeID] = n
	return nil
}

func (s *Store) DetachInstance(ctx context.Context, nodeID, instanceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return nil
	}
	n.Instances = lo.Filter(n.Instances, func(s string, _ int) bool { return s != instanceID })
	s.nodes[nodeID] = n
	return nil
}
