package gormdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/samber/lo"
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
		// total. clampAdd is the second line of defence. The count comes
		// from the topology product: flavor_vcpu is the legacy column.
		count := inst.FlavorSockets * inst.FlavorCores * inst.FlavorThreads
		if count <= 0 {
			count = inst.FlavorVCPU
		}
		disk := 0
		for _, att := range decodeAttachments(inst.AttachmentsJSON) {
			var vol volumeRow
			if err := tx.First(&vol, "id = ?", att.VolumeID).Error; err != nil {
				continue // row gone: its bytes were already accounted elsewhere
			}
			disk += vol.SizeMiB
		}
		updates := map[string]any{
			"available_vcpu": clampAdd("available_vcpu", "total_vcpu", count),
			"available_ram":  clampAdd("available_ram", "total_ram", inst.FlavorRAM),
			"available_disk": clampAdd("available_disk", "total_disk", disk),
			"updated_at":     time.Now().UTC(),
		}
		// Free the instance's exclusive CPUs alongside its count. The pins
		// ride on the instance row, so a retried release frees exactly once:
		// unknown CPUs are simply not present anymore.
		if inst.PinnedCPUs != "" {
			var node nodeRow
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				First(&node, "id = ?", nodeID).Error; err != nil {
				if !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
			} else {
				free := make(map[int]bool)
				for _, c := range splitInts(inst.PinnedCPUs) {
					free[c] = true
				}
				var kept []int
				for _, c := range splitInts(node.PinnedCPUs) {
					if !free[c] {
						kept = append(kept, c)
					}
				}
				updates["pinned_cpus"] = joinInts(kept)
			}
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
		// Re-registration keeps state, spec and reservations; only the name
		// and the reported IP are refreshed. An agent restarting must not
		// reset a host mid-build.
		updates := map[string]any{
			"name": n.Name, "updated_at": time.Now().UTC(),
		}
		if n.IP != "" {
			updates["ip"] = n.IP
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
		row.NotReadyReason = "registered, awaiting first probe"
		// A new node starts fully available; reservations subtract from here.
		row.AvailableVCPU = n.Total.CPU
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

// SetNodeSpec records the engine and capacity the probe reported. Available
// keeps what reservations hold: Total moves, held stays, floored at zero.
func (s *Store) SetNodeSpec(ctx context.Context, id string, engine string, total core.Resources) (core.Node, error) {
	var row nodeRow
	if err := s.withTimeout(ctx).First(&row, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.Node{}, fmt.Errorf("set spec %s: %w", id, core.ErrNodeNotFound)
		}
		return core.Node{}, fmt.Errorf("set spec %s: %w", id, err)
	}
	heldVCPU := row.TotalVCPU - row.AvailableVCPU
	heldRAM := row.TotalRAM - row.AvailableRAM
	heldDisk := row.TotalDisk - row.AvailableDisk
	updates := map[string]any{
		"driver":         engine,
		"total_vcpu":     total.CPU,
		"total_ram":      total.RAM,
		"total_disk":     total.Disk,
		"available_vcpu": max(total.CPU-heldVCPU, 0),
		"available_ram":  max(total.RAM-heldRAM, 0),
		"available_disk": max(total.Disk-heldDisk, 0),
		"updated_at":     time.Now().UTC(),
	}
	if err := s.withTimeout(ctx).Model(&nodeRow{}).Where("id = ?", id).Updates(updates).Error; err != nil {
		return core.Node{}, fmt.Errorf("set spec %s: %w", id, err)
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
		"usage_vcpu":     usage.CPU,
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
			nodeID, r.CPU, r.RAM, r.Disk).
		Updates(map[string]any{
			"available_vcpu": gorm.Expr("available_vcpu - ?", r.CPU),
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
			"available_vcpu": clampAdd("available_vcpu", "total_vcpu", r.CPU),
			"available_ram":  clampAdd("available_ram", "total_ram", r.RAM),
			"available_disk": clampAdd("available_disk", "total_disk", r.Disk),
			"updated_at":     time.Now().UTC(),
		})
	if res.Error != nil {
		return fmt.Errorf("release node %s: %w", nodeID, res.Error)
	}
	return nil
}

// ClaimCPUs exclusively reserves count host CPUs inside one transaction: the
// row lock serializes competing dedicated placements, so two winners cannot
// hold the same CPU. The count decrement rides in the same write, keeping
// Available.CPU the single capacity truth.
func (s *Store) ClaimCPUs(ctx context.Context, nodeID string, count int) ([]int, error) {
	if count <= 0 {
		return nil, fmt.Errorf("claim %d cpus: %w", count, core.ErrInvalidRequest)
	}
	var cpus []int
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		var row nodeRow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&row, "id = ?", nodeID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w", core.ErrNodeNotFound)
			}
			return err
		}
		var err error
		cpus, err = core.FreeCPUs(row.TotalVCPU, splitInts(row.PinnedCPUs), count)
		if err != nil {
			return err
		}
		held := splitInts(row.PinnedCPUs)
		updates := map[string]any{
			"pinned_cpus":    joinInts(append(held, cpus...)),
			"available_vcpu": gorm.Expr("available_vcpu - ?", count),
			"updated_at":     time.Now().UTC(),
		}
		return tx.Model(&nodeRow{}).Where("id = ?", nodeID).Updates(updates).Error
	})
	if err != nil {
		return nil, fmt.Errorf("claim cpus %s: %w", nodeID, err)
	}
	return cpus, nil
}

// ReleaseCPUs frees a previous claim and restores the count. Unknown CPUs are
// ignored, so a repeated release is a no-op rather than invented capacity.
func (s *Store) ReleaseCPUs(ctx context.Context, nodeID string, cpus []int) error {
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		var row nodeRow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&row, "id = ?", nodeID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		free := make(map[int]bool, len(cpus))
		for _, c := range cpus {
			free[c] = true
		}
		var kept []int
		released := 0
		for _, c := range splitInts(row.PinnedCPUs) {
			if free[c] {
				released++
				continue
			}
			kept = append(kept, c)
		}
		updates := map[string]any{
			"pinned_cpus":    joinInts(kept),
			"available_vcpu": clampAdd("available_vcpu", "total_vcpu", released),
			"updated_at":     time.Now().UTC(),
		}
		return tx.Model(&nodeRow{}).Where("id = ?", nodeID).Updates(updates).Error
	})
	if err != nil {
		return fmt.Errorf("release cpus %s: %w", nodeID, err)
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
	kept := lo.Reject(splitList(row.Instances), func(id string, _ int) bool {
		return id == instanceID
	})
	return tx.Model(&nodeRow{}).Where("id = ?", nodeID).
		Update("instances", joinList(kept)).Error
}

// ------------------------------------------------------------------ flavors

func (s *Store) CreateFlavor(ctx context.Context, f core.Flavor) (core.Flavor, error) {
	if f.Name == "" {
		return core.Flavor{}, fmt.Errorf("flavor name required: %w", core.ErrInvalidRequest)
	}
	if err := f.VCPU.Validate(); err != nil {
		return core.Flavor{}, err
	}
	if f.RAM <= 0 {
		return core.Flavor{}, fmt.Errorf("flavor must have positive ram: %w", core.ErrInvalidRequest)
	}
	row := fromFlavor(f)
	if err := s.withTimeout(ctx).Create(&row).Error; err != nil {
		return core.Flavor{}, fmt.Errorf("create flavor %s: %w", f.Name, err)
	}
	return f, nil
}

func (s *Store) GetFlavor(ctx context.Context, name string) (core.Flavor, error) {
	var row flavorRow
	if err := s.withTimeout(ctx).First(&row, "name = ?", name).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.Flavor{}, fmt.Errorf("get flavor %s: %w", name, core.ErrFlavorNotFound)
		}
		return core.Flavor{}, fmt.Errorf("get flavor %s: %w", name, err)
	}
	return toFlavor(row), nil
}

func (s *Store) ListFlavors(ctx context.Context) ([]core.Flavor, error) {
	var rows []flavorRow
	if err := s.withTimeout(ctx).Order("name").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list flavors: %w", err)
	}
	return lo.Map(rows, func(r flavorRow, _ int) core.Flavor { return toFlavor(r) }), nil
}

func (s *Store) DeleteFlavor(ctx context.Context, name string) error {
	var n int64
	if err := s.withTimeout(ctx).Model(&instanceRow{}).Where("flavor_name = ?", name).Count(&n).Error; err != nil {
		return fmt.Errorf("delete flavor %s: %w", name, err)
	}
	if n > 0 {
		return fmt.Errorf("flavor %s in use: %w", name, core.ErrInvalidRequest)
	}
	if err := s.withTimeout(ctx).Delete(&flavorRow{}, "name = ?", name).Error; err != nil {
		return fmt.Errorf("delete flavor %s: %w", name, err)
	}
	return nil
}

var (
	_ core.InstanceStore = (*Store)(nil)
	_ core.NodeStore     = (*Store)(nil)
	_ core.FlavorStore   = (*Store)(nil)
)
