// Package gormdb is the durable store, backed by SQL through GORM.
//
// # Dialects
//
// One implementation serves both dialects, because the semantics that matter
// here are SQL semantics, not PostgreSQL semantics:
//
//	OpenPostgres(dsn)  production, shared across hosts
//	OpenSQLite(path)   tests, a file on disk
//
// SQLite is not a toy stand-in here: the atomicity claim, the compare-and-swap
// transitions and the guarded reservation are all exercised against it, so a
// regression in those is caught by `go test` rather than in production.
//
// # The one thing that must not move to Go
//
// ReserveNodeResources must stay all-or-nothing. Check every dimension before
// mutating any of them, in one statement, guarded by the WHERE clause:
//
//	UPDATE nodes SET available_vcpu = available_vcpu - ?, ...
//	WHERE id = ? AND available_vcpu >= ? AND available_ram >= ? AND available_disk >= ?
//
// RowsAffected == 0 means the claim was lost, which surfaces as
// ErrCapacityConflict. Doing this in Go instead would reintroduce the
// overcommit that ErrCapacityConflict exists to prevent.
package gormdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Store implements core.InstanceStore and core.NodeStore over SQL.
type Store struct {
	db *gorm.DB
}

// OpenPostgres connects to PostgreSQL and migrates.
func OpenPostgres(dsn string) (*Store, error) {
	return open(postgres.Open(dsn))
}

// OpenSQLite connects to a SQLite database and migrates.
//
// SQLite serialises writers, so concurrent writers contend on a lock. Without
// the busy timeout they fail immediately with SQLITE_BUSY, which would turn a
// legitimate lost claim into a spurious error rather than a retry.
//
// A dialect difference worth naming: the row locks taken with clause.Locking
// are a PostgreSQL mechanism. SQLite serialises writes at the database level
// instead, so the same code is correct there without them — which is why the
// tests can exercise the atomic claim against SQLite and still be evidence for
// the PostgreSQL path.
func OpenSQLite(path string) (*Store, error) {
	return open(sqlite.Open(path + "?_busy_timeout=5000&_journal_mode=WAL"))
}

func open(dialector gorm.Dialector) (*Store, error) {
	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("gormdb: open: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

// DB exposes the handle for callers that need their own transaction.
func (s *Store) DB() *gorm.DB { return s.db }

// clampAdd builds a portable "add, but never exceed the column's ceiling".
//
// LEAST is Postgres and MySQL; SQLite only has the scalar MIN, and MIN with
// two arguments means something else there. CASE is the one form every dialect
// agrees on.
//
// The column names are literals written in this file, never caller input, which
// is what makes the fmt.Sprintf safe; anything interpolated from a caller would
// make it an injection point.
func clampAdd(column, ceiling string, delta int) clause.Expr {
	return gorm.Expr(
		fmt.Sprintf("CASE WHEN %s + ? > %s THEN %s ELSE %s + ? END", column, ceiling, ceiling, column),
		delta, delta)
}

// ------------------------------------------------------------------ models

type instanceRow struct {
	ID            string `gorm:"primaryKey;size:64"`
	Name          string `gorm:"size:255;index"`
	FlavorName    string `gorm:"size:64"`
	FlavorVCPU    int    `gorm:"column:flavor_vcpu"`
	FlavorRAM     int    `gorm:"column:flavor_ram"`
	FlavorDisk    int    `gorm:"column:flavor_disk"`
	ImageRef      string `gorm:"size:512"`
	State         string `gorm:"size:32;index"`
	NodeID        string `gorm:"size:64;index"`
	Generation    int
	FailureReason string `gorm:"size:1024"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (instanceRow) TableName() string { return "instances" }

// Column names are declared explicitly. GORM's default snake-casing turns
// `UsageVCPU` into `usage_v_cpu`, but the keys passed to Updates and the text
// inside gorm.Expr are used verbatim — so a derived name and a hand-written one
// silently disagree, and the failure surfaces as "no such column" at runtime
// rather than at build time. Naming every column keeps the DDL and the raw SQL
// reading the same one.
type nodeRow struct {
	ID             string `gorm:"primaryKey;size:64"`
	Name           string `gorm:"size:255"`
	Arch           string `gorm:"size:32"`
	State          string `gorm:"size:32;index"`
	Enabled        bool
	Driver         string `gorm:"size:32"`
	TotalVCPU      int    `gorm:"column:total_vcpu"`
	TotalRAM       int    `gorm:"column:total_ram"`
	TotalDisk      int    `gorm:"column:total_disk"`
	AvailableVCPU  int    `gorm:"column:available_vcpu"`
	AvailableRAM   int    `gorm:"column:available_ram"`
	AvailableDisk  int    `gorm:"column:available_disk"`
	UsageVCPU      int    `gorm:"column:usage_vcpu"`
	UsageRAM       int    `gorm:"column:usage_ram"`
	UsageDisk      int    `gorm:"column:usage_disk"`
	Instances      string `gorm:"type:text"` // comma-separated
	LastHeartbeat  time.Time
	NotReadyReason string `gorm:"size:512"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (nodeRow) TableName() string { return "nodes" }

func (s *Store) migrate() error {
	if err := s.db.AutoMigrate(&instanceRow{}, &nodeRow{}); err != nil {
		return fmt.Errorf("gormdb: migrate: %w", err)
	}
	return nil
}

// ------------------------------------------------------------- conversions

func toInstance(r instanceRow) core.Instance {
	return core.Instance{
		ID:            r.ID,
		Name:          r.Name,
		Flavor:        core.Flavor{Name: r.FlavorName, VCPU: r.FlavorVCPU, RAM: r.FlavorRAM, Disk: r.FlavorDisk},
		ImageRef:      r.ImageRef,
		State:         core.InstanceState(r.State),
		NodeID:        r.NodeID,
		Generation:    r.Generation,
		FailureReason: r.FailureReason,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.UpdatedAt,
	}
}

func fromInstance(i core.Instance) instanceRow {
	return instanceRow{
		ID:            i.ID,
		Name:          i.Name,
		FlavorName:    i.Flavor.Name,
		FlavorVCPU:    i.Flavor.VCPU,
		FlavorRAM:     i.Flavor.RAM,
		FlavorDisk:    i.Flavor.Disk,
		ImageRef:      i.ImageRef,
		State:         string(i.State),
		NodeID:        i.NodeID,
		Generation:    i.Generation,
		FailureReason: i.FailureReason,
		CreatedAt:     i.CreatedAt,
		UpdatedAt:     i.UpdatedAt,
	}
}

func toNode(r nodeRow) core.Node {
	var ids []string
	if r.Instances != "" {
		ids = splitList(r.Instances)
	}
	return core.Node{
		ID:             r.ID,
		Name:           r.Name,
		Arch:           r.Arch,
		State:          core.NodeState(r.State),
		Enabled:        r.Enabled,
		Driver:         r.Driver,
		Total:          core.Resources{VCPU: r.TotalVCPU, RAM: r.TotalRAM, Disk: r.TotalDisk},
		Available:      core.Resources{VCPU: r.AvailableVCPU, RAM: r.AvailableRAM, Disk: r.AvailableDisk},
		Usage:          core.Resources{VCPU: r.UsageVCPU, RAM: r.UsageRAM, Disk: r.UsageDisk},
		Instances:      ids,
		LastHeartbeat:  r.LastHeartbeat,
		NotReadyReason: r.NotReadyReason,
	}
}

func fromNode(n core.Node) nodeRow {
	return nodeRow{
		ID:             n.ID,
		Name:           n.Name,
		Arch:           n.Arch,
		State:          string(n.State),
		Enabled:        n.Enabled,
		Driver:         n.Driver,
		TotalVCPU:      n.Total.VCPU,
		TotalRAM:       n.Total.RAM,
		TotalDisk:      n.Total.Disk,
		AvailableVCPU:  n.Available.VCPU,
		AvailableRAM:   n.Available.RAM,
		AvailableDisk:  n.Available.Disk,
		UsageVCPU:      n.Usage.VCPU,
		UsageRAM:       n.Usage.RAM,
		UsageDisk:      n.Usage.Disk,
		Instances:      joinList(n.Instances),
		LastHeartbeat:  n.LastHeartbeat,
		NotReadyReason: n.NotReadyReason,
	}
}

// ----------------------------------------------------------------- helpers

func joinList(ids []string) string {
	return strings.Join(ids, ",")
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// withTimeout binds a store call to the caller's context, so cancellation and
// deadlines propagate instead of a query outliving its request.
func (s *Store) withTimeout(ctx context.Context) *gorm.DB {
	return s.db.WithContext(ctx)
}
