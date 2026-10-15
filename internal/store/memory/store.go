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
}

func New() *Store {
	return &Store{
		instances: make(map[string]core.Instance),
		nodes:     make(map[string]core.Node),
	}
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
		existing.Address = n.Address
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
