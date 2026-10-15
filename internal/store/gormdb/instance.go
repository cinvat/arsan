package gormdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ---------------------------------------------------------------- instances

func (s *Store) Create(ctx context.Context, i core.Instance) (core.Instance, error) {
	row := fromInstance(i)
	now := time.Now().UTC()
	if row.CreatedAt.IsZero() {
		row.CreatedAt = now
	}
	row.UpdatedAt = now

	if err := s.withTimeout(ctx).Create(&row).Error; err != nil {
		return core.Instance{}, fmt.Errorf("create instance %s: %w", i.ID, err)
	}
	return toInstance(row), nil
}

func (s *Store) Get(ctx context.Context, id string) (core.Instance, error) {
	var row instanceRow
	if err := s.withTimeout(ctx).First(&row, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.Instance{}, fmt.Errorf("get instance %s: %w", id, core.ErrInstanceNotFound)
		}
		return core.Instance{}, fmt.Errorf("get instance %s: %w", id, err)
	}
	return toInstance(row), nil
}

func (s *Store) List(ctx context.Context) ([]core.Instance, error) {
	var rows []instanceRow
	if err := s.withTimeout(ctx).Order("id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list instances: %w", err)
	}
	out := make([]core.Instance, 0, len(rows))
	for _, r := range rows {
		out = append(out, toInstance(r))
	}
	return out, nil
}

// Update refuses to change state, mirroring the memory store. State moves only
// through Transition; allowing it here would make a stale struct silently
// revert a concurrent transition.
func (s *Store) Update(ctx context.Context, i core.Instance) (core.Instance, error) {
	var cur instanceRow
	if err := s.withTimeout(ctx).First(&cur, "id = ?", i.ID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.Instance{}, fmt.Errorf("update instance %s: %w", i.ID, core.ErrInstanceNotFound)
		}
		return core.Instance{}, fmt.Errorf("update instance %s: %w", i.ID, err)
	}
	if cur.State != string(i.State) {
		return core.Instance{}, fmt.Errorf("update instance %s would change state %s->%s: %w",
			i.ID, cur.State, i.State, core.ErrInvalidTransition)
	}

	row := fromInstance(i)
	row.State = cur.State // never written; kept for clarity
	row.CreatedAt = cur.CreatedAt
	row.UpdatedAt = time.Now().UTC()
	if err := s.withTimeout(ctx).Save(&row).Error; err != nil {
		return core.Instance{}, fmt.Errorf("update instance %s: %w", i.ID, err)
	}
	return toInstance(row), nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	if err := s.withTimeout(ctx).Delete(&instanceRow{}, "id = ?", id).Error; err != nil {
		return fmt.Errorf("delete instance %s: %w", id, err)
	}
	return nil
}

// Transition is the only way instance state changes. The compare-and-swap is
// the WHERE clause: a concurrent transition means no rows are updated, which is
// ErrInvalidTransition.
func (s *Store) Transition(ctx context.Context, id string, from, to core.InstanceState) (core.Instance, error) {
	now := time.Now().UTC()
	res := s.withTimeout(ctx).Model(&instanceRow{}).
		Where("id = ? AND state = ?", id, string(from)).
		Updates(map[string]any{"state": string(to), "updated_at": now})

	if res.Error != nil {
		return core.Instance{}, fmt.Errorf("transition %s: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		// Either it does not exist, or it is not in `from`. Distinguish, so
		// the caller can tell "gone" from "someone else got there first".
		var cur instanceRow
		err := s.withTimeout(ctx).First(&cur, "id = ?", id).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.Instance{}, fmt.Errorf("transition %s: %w", id, core.ErrInstanceNotFound)
		}
		return core.Instance{}, fmt.Errorf("transition %s %s->%s (found %s): %w",
			id, from, to, cur.State, core.ErrInvalidTransition)
	}

	return s.Get(ctx, id)
}

// ReleaseReservations returns an instance's flavor to its node and detaches it.
// Idempotent: a missing instance or node is not an error, because a leaked
// reservation slowly overcommits the cluster and releasing must always succeed.
func (s *Store) ReleaseReservations(ctx context.Context, instanceID, nodeID string) error {
	if nodeID == "" {
		return nil
	}
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		var inst instanceRow
		if err := tx.First(&inst, "id = ?", instanceID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		var node nodeRow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&node, "id = ?", nodeID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}

		// Return the instance's own flavor, not the node's current
		// availability, so a double release cannot inflate a node past its
		// total. clampAdd is the second line of defence.
		updates := map[string]any{
			"available_vcpu": clampAdd("available_vcpu", "total_vcpu", inst.FlavorVCPU),
			"available_ram":  clampAdd("available_ram", "total_ram", inst.FlavorRAM),
			"available_disk": clampAdd("available_disk", "total_disk", inst.FlavorDisk),
			"updated_at":     time.Now().UTC(),
		}
		if err := tx.Model(&nodeRow{}).Where("id = ?", nodeID).Updates(updates).Error; err != nil {
			return err
		}
		return detach(tx, nodeID, instanceID)
	})
	if err != nil {
		return fmt.Errorf("release reservations %s/%s: %w", instanceID, nodeID, err)
	}
	return nil
}

// -------------------------------------------------------------------- nodes

func (s *Store) Register(ctx context.Context, n core.Node) (core.Node, error) {
	var existing nodeRow
	err := s.withTimeout(ctx).First(&existing, "id = ?", n.ID).Error
	switch {
	case err == nil:
		// Re-registration is a heartbeat of sorts: keep state and
		// reservations, refresh the descriptive fields.
		updates := map[string]any{
			"name": n.Name, "address": n.Address, "updated_at": time.Now().UTC(),
		}
		if err := s.withTimeout(ctx).Model(&nodeRow{}).
			Where("id = ?", n.ID).Updates(updates).Error; err != nil {
			return core.Node{}, fmt.Errorf("register node %s: %w", n.ID, err)
		}
		if err := s.withTimeout(ctx).First(&existing, "id = ?", n.ID).Error; err != nil {
			return core.Node{}, fmt.Errorf("register node %s: %w", n.ID, err)
		}
		return toNode(existing), nil

	case errors.Is(err, gorm.ErrRecordNotFound):
		row := fromNode(n)
		row.State = string(core.NodeNotReady)
		// A new node starts fully available; reservations subtract from here.
		row.AvailableVCPU = n.Total.VCPU
		row.AvailableRAM = n.Total.RAM
		row.AvailableDisk = n.Total.Disk
		now := time.Now().UTC()
		row.CreatedAt, row.UpdatedAt = now, now
		if err := s.withTimeout(ctx).Create(&row).Error; err != nil {
			return core.Node{}, fmt.Errorf("register node %s: %w", n.ID, err)
		}
		return toNode(row), nil

	default:
		return core.Node{}, fmt.Errorf("register node %s: %w", n.ID, err)
	}
}

func (s *Store) GetNode(ctx context.Context, id string) (core.Node, error) {
	var row nodeRow
	if err := s.withTimeout(ctx).First(&row, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.Node{}, fmt.Errorf("get node %s: %w", id, core.ErrNodeNotFound)
		}
		return core.Node{}, fmt.Errorf("get node %s: %w", id, err)
	}
	return toNode(row), nil
}

func (s *Store) ListNodes(ctx context.Context) ([]core.Node, error) {
	var rows []nodeRow
	if err := s.withTimeout(ctx).Order("id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	out := make([]core.Node, 0, len(rows))
	for _, r := range rows {
		out = append(out, toNode(r))
	}
	return out, nil
}

// SetNodeEnabled takes a node in or out of service without touching its state,
// so an operator's decision survives a heartbeat.
func (s *Store) SetNodeEnabled(ctx context.Context, id string, enabled bool) (core.Node, error) {
	res := s.withTimeout(ctx).Model(&nodeRow{}).Where("id = ?", id).
		Updates(map[string]any{"enabled": enabled, "updated_at": time.Now().UTC()})
	if res.Error != nil {
		return core.Node{}, fmt.Errorf("set enabled %s: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return core.Node{}, fmt.Errorf("set enabled %s: %w", id, core.ErrNodeNotFound)
	}
	return s.GetNode(ctx, id)
}

// SetReadiness records readiness and the reason it is not ready.
func (s *Store) SetReadiness(ctx context.Context, id string, state core.NodeState, reason string) (core.Node, error) {
	// A reason is only meaningful for NOTREADY; storing one on a READY node
	// would leave a stale explanation behind for the next failure.
	if state.Ready() {
		reason = ""
	}
	res := s.withTimeout(ctx).Model(&nodeRow{}).Where("id = ?", id).
		Updates(map[string]any{
			"state":            string(state),
			"not_ready_reason": reason,
			"updated_at":       time.Now().UTC(),
		})
	if res.Error != nil {
		return core.Node{}, fmt.Errorf("set readiness %s: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return core.Node{}, fmt.Errorf("set readiness %s: %w", id, core.ErrNodeNotFound)
	}
	return s.GetNode(ctx, id)
}

// Heartbeat records liveness and reported usage. Reported usage is
// informational: available capacity is derived from reservations, so a stale or
// wrong report cannot cause an overcommit.
func (s *Store) RecordUsage(ctx context.Context, id string, usage core.Resources, at time.Time) (core.Node, error) {
	var row nodeRow
	if err := s.withTimeout(ctx).First(&row, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.Node{}, fmt.Errorf("heartbeat %s: %w", id, core.ErrNodeNotFound)
		}
		return core.Node{}, fmt.Errorf("heartbeat %s: %w", id, err)
	}

	// A heartbeat records usage. It does NOT set readiness.
	//
	// Readiness belongs to the controller's probe, which round-trips through
	// the compute engine. A heartbeat only proves the agent answered HTTP,
	// and an agent with an unreachable libvirt socket answers HTTP perfectly
	// while being unable to run a VM — so promoting on heartbeat would undo
	// the probe and put a broken host back into the eligible set.
	updates := map[string]any{
		"last_heartbeat": at,
		"usage_vcpu":     usage.VCPU,
		"usage_ram":      usage.RAM,
		"usage_disk":     usage.Disk,
		"updated_at":     time.Now().UTC(),
	}
	if err := s.withTimeout(ctx).Model(&nodeRow{}).
		Where("id = ?", id).Updates(updates).Error; err != nil {
		return core.Node{}, fmt.Errorf("heartbeat %s: %w", id, err)
	}
	return s.GetNode(ctx, id)
}

// ReserveNodeResources is the single write the scheduler makes.
//
// The guard is in the WHERE clause so the check and the decrement are one
// atomic statement. RowsAffected == 0 is the lost claim. Doing this in Go would
// let two requests each fit in vCPU but neither in RAM.
func (s *Store) ReserveNodeResources(ctx context.Context, nodeID string, r core.Resources) error {
	res := s.withTimeout(ctx).Model(&nodeRow{}).
		Where("id = ? AND available_vcpu >= ? AND available_ram >= ? AND available_disk >= ?",
			nodeID, r.VCPU, r.RAM, r.Disk).
		Updates(map[string]any{
			"available_vcpu": gorm.Expr("available_vcpu - ?", r.VCPU),
			"available_ram":  gorm.Expr("available_ram - ?", r.RAM),
			"available_disk": gorm.Expr("available_disk - ?", r.Disk),
			"updated_at":     time.Now().UTC(),
		})

	if res.Error != nil {
		return fmt.Errorf("reserve node %s: %w", nodeID, res.Error)
	}
	if res.RowsAffected == 0 {
		// Not enough capacity, or no such node. Distinguish so the caller
		// can retry the former and fail on the latter.
		var row nodeRow
		err := s.withTimeout(ctx).First(&row, "id = ?", nodeID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("reserve node %s: %w", nodeID, core.ErrNodeNotFound)
		}
		return fmt.Errorf("reserve node %s (available vcpu=%d ram=%d disk=%d): %w",
			nodeID, row.AvailableVCPU, row.AvailableRAM, row.AvailableDisk, core.ErrCapacityConflict)
	}
	return nil
}

func (s *Store) ReleaseNodeResources(ctx context.Context, nodeID string, r core.Resources) error {
	// Clamp to the node's total so a double release cannot invent capacity.
	res := s.withTimeout(ctx).Model(&nodeRow{}).Where("id = ?", nodeID).
		Updates(map[string]any{
			"available_vcpu": clampAdd("available_vcpu", "total_vcpu", r.VCPU),
			"available_ram":  clampAdd("available_ram", "total_ram", r.RAM),
			"available_disk": clampAdd("available_disk", "total_disk", r.Disk),
			"updated_at":     time.Now().UTC(),
		})
	if res.Error != nil {
		return fmt.Errorf("release node %s: %w", nodeID, res.Error)
	}
	return nil
}

func (s *Store) AttachInstance(ctx context.Context, nodeID, instanceID string) error {
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		var row nodeRow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&row, "id = ?", nodeID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w", core.ErrNodeNotFound)
			}
			return err
		}
		ids := splitList(row.Instances)
		for _, id := range ids {
			if id == instanceID {
				return nil // already attached
			}
		}
		ids = append(ids, instanceID)
		return tx.Model(&nodeRow{}).Where("id = ?", nodeID).
			Update("instances", joinList(ids)).Error
	})
	if err != nil {
		return fmt.Errorf("attach %s to %s: %w", instanceID, nodeID, err)
	}
	return nil
}

func (s *Store) DetachInstance(ctx context.Context, nodeID, instanceID string) error {
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		return detach(tx, nodeID, instanceID)
	})
	if err != nil {
		return fmt.Errorf("detach %s from %s: %w", instanceID, nodeID, err)
	}
	return nil
}

// detach removes one instance ID from a node's list. Caller holds the
// transaction.
func detach(tx *gorm.DB, nodeID, instanceID string) error {
	var row nodeRow
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		First(&row, "id = ?", nodeID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil // nothing to detach from
		}
		return err
	}
	ids := splitList(row.Instances)
	kept := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != instanceID {
			kept = append(kept, id)
		}
	}
	return tx.Model(&nodeRow{}).Where("id = ?", nodeID).
		Update("instances", joinList(kept)).Error
}

var (
	_ core.InstanceStore = (*Store)(nil)
	_ core.NodeStore     = (*Store)(nil)
)
