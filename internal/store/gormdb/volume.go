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

// ------------------------------------------------------------------ volumes

type volumeRow struct {
	ID                  string `gorm:"primaryKey;size:64"`
	Name                string `gorm:"size:255;index"`
	Namespace           string `gorm:"size:63;index"`
	SizeMiB             int    `gorm:"column:size_mib"`
	Image               string `gorm:"size:512"`
	Cdrom               bool
	BootIndex           *int
	DeleteOnTermination bool   `gorm:"column:delete_on_termination"`
	State               string `gorm:"size:32;index"`
	AttachedTo          string `gorm:"size:64;index"`
	NodeID              string `gorm:"size:64;index"`
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (volumeRow) TableName() string { return "volumes" }

func toVolume(r volumeRow) core.Volume {
	return core.Volume{
		ID: r.ID, Name: r.Name, Namespace: r.Namespace,
		SizeMiB: r.SizeMiB, Image: r.Image, Cdrom: r.Cdrom,
		BootIndex: r.BootIndex, DeleteOnTermination: r.DeleteOnTermination,
		State: core.VolumeState(r.State), AttachedTo: r.AttachedTo, NodeID: r.NodeID,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

func fromVolume(v core.Volume) volumeRow {
	return volumeRow{
		ID: v.ID, Name: v.Name, Namespace: v.Namespace,
		SizeMiB: v.SizeMiB, Image: v.Image, Cdrom: v.Cdrom,
		BootIndex: v.BootIndex, DeleteOnTermination: v.DeleteOnTermination,
		State: string(v.State), AttachedTo: v.AttachedTo, NodeID: v.NodeID,
		CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt,
	}
}

func (s *Store) CreateVolume(ctx context.Context, v core.Volume) (core.Volume, error) {
	if v.Name == "" {
		return core.Volume{}, fmt.Errorf("volume name required: %w", core.ErrInvalidRequest)
	}
	if err := core.ValidateName(v.Name); err != nil {
		return core.Volume{}, err
	}
	if v.SizeMiB <= 0 && !v.Cdrom && v.Image == "" {
		return core.Volume{}, fmt.Errorf("volume %q needs a size or a base image: %w", v.Name, core.ErrInvalidRequest)
	}
	if v.Cdrom {
		v.DeleteOnTermination = false
	}
	if v.State == "" {
		v.State = core.VolumeAvailable
	}
	row := fromVolume(v)
	now := time.Now().UTC()
	if row.CreatedAt.IsZero() {
		row.CreatedAt = now
	}
	row.UpdatedAt = now
	if err := s.withTimeout(ctx).Create(&row).Error; err != nil {
		return core.Volume{}, fmt.Errorf("create volume %s: %w", v.ID, err)
	}
	return toVolume(row), nil
}

func (s *Store) GetVolume(ctx context.Context, id string) (core.Volume, error) {
	var row volumeRow
	if err := s.withTimeout(ctx).First(&row, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.Volume{}, fmt.Errorf("get volume %s: %w", id, core.ErrVolumeNotFound)
		}
		return core.Volume{}, fmt.Errorf("get volume %s: %w", id, err)
	}
	return toVolume(row), nil
}

func (s *Store) ListVolumes(ctx context.Context) ([]core.Volume, error) {
	var rows []volumeRow
	if err := s.withTimeout(ctx).Order("id").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}
	return lo.Map(rows, func(r volumeRow, _ int) core.Volume { return toVolume(r) }), nil
}

func (s *Store) DeleteVolume(ctx context.Context, id string) error {
	var row volumeRow
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&row, "id = ?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if volumeRowState(row) == core.VolumeInUse {
			return fmt.Errorf("delete volume %s: attached to %s: %w", id, row.AttachedTo, core.ErrInvalidRequest)
		}
		return tx.Delete(&volumeRow{}, "id = ?", id).Error
	})
	if err != nil {
		return fmt.Errorf("delete volume %s: %w", id, err)
	}
	return nil
}

func volumeRowState(r volumeRow) core.VolumeState { return core.VolumeState(r.State) }

// AttachVolume is compare-and-swap on status under a row lock: the second of
// two racing claims sees in-use and loses with ErrCapacityConflict.
func (s *Store) AttachVolume(ctx context.Context, id, instanceID string) (core.Volume, error) {
	var row volumeRow
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&row, "id = ?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w", core.ErrVolumeNotFound)
			}
			return err
		}
		if volumeRowState(row) != core.VolumeAvailable {
			if row.AttachedTo == instanceID {
				return nil // already ours (inline-created, or a redelivery)
			}
			return fmt.Errorf("attach volume %s: held by %s: %w", id, row.AttachedTo, core.ErrCapacityConflict)
		}
		return tx.Model(&volumeRow{}).Where("id = ? AND state = ?", id, string(core.VolumeAvailable)).
			Updates(map[string]any{
				"state": string(core.VolumeInUse), "attached_to": instanceID,
				"updated_at": time.Now().UTC(),
			}).Error
	})
	if err != nil {
		return core.Volume{}, fmt.Errorf("attach volume %s: %w", id, err)
	}
	return s.GetVolume(ctx, id)
}

func (s *Store) DetachVolume(ctx context.Context, id string) (core.Volume, error) {
	var row volumeRow
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&row, "id = ?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w", core.ErrVolumeNotFound)
			}
			return err
		}
		if volumeRowState(row) != core.VolumeInUse {
			return fmt.Errorf("detach volume %s: not attached: %w", id, core.ErrInvalidRequest)
		}
		return tx.Model(&volumeRow{}).Where("id = ?", id).
			Updates(map[string]any{
				"state": string(core.VolumeAvailable), "attached_to": "",
				"updated_at": time.Now().UTC(),
			}).Error
	})
	if err != nil {
		return core.Volume{}, fmt.Errorf("detach volume %s: %w", id, err)
	}
	return s.GetVolume(ctx, id)
}

// PinVolume records first provisioning: set NodeID iff unset (compare-and-
// swap under a row lock). Re-pinning elsewhere conflicts instead of moving
// files that do not move.
func (s *Store) PinVolume(ctx context.Context, id, nodeID string) (core.Volume, error) {
	var row volumeRow
	err := s.withTimeout(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&row, "id = ?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w", core.ErrVolumeNotFound)
			}
			return err
		}
		if row.NodeID != "" && row.NodeID != nodeID {
			return fmt.Errorf("pin volume %s: files live on %s: %w", id, row.NodeID, core.ErrCapacityConflict)
		}
		return tx.Model(&volumeRow{}).Where("id = ?", id).
			Updates(map[string]any{"node_id": nodeID, "updated_at": time.Now().UTC()}).Error
	})
	if err != nil {
		return core.Volume{}, fmt.Errorf("pin volume %s: %w", id, err)
	}
	return s.GetVolume(ctx, id)
}

func (s *Store) SetVolumeBootIndex(ctx context.Context, id string, index *int) (core.Volume, error) {
	res := s.withTimeout(ctx).Model(&volumeRow{}).Where("id = ?", id).
		Updates(map[string]any{"boot_index": index, "updated_at": time.Now().UTC()})
	if res.Error != nil {
		return core.Volume{}, fmt.Errorf("set boot %s: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return core.Volume{}, fmt.Errorf("set boot %s: %w", id, core.ErrVolumeNotFound)
	}
	return s.GetVolume(ctx, id)
}

var (
	_ core.VolumeStore = (*Store)(nil)
)
