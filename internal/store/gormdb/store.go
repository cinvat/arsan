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
	"encoding/json"
	"fmt"
	"strconv"
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
	ID              string `gorm:"primaryKey;size:64"`
	Name            string `gorm:"size:255;index"`
	Namespace       string `gorm:"size:63;index"`
	FlavorName      string `gorm:"size:64"`
	FlavorVCPU      int    `gorm:"column:flavor_vcpu"` // legacy count; topology columns below are canonical
	FlavorSockets   int    `gorm:"column:flavor_sockets"`
	FlavorCores     int    `gorm:"column:flavor_cores"`
	FlavorThreads   int    `gorm:"column:flavor_threads"`
	FlavorDedicated bool   `gorm:"column:flavor_dedicated"`
	FlavorRAM       int    `gorm:"column:flavor_ram"`
	FlavorDisk      int    `gorm:"column:flavor_disk"`
	PinnedCPUs      string `gorm:"column:pinned_cpus;type:text"`      // comma-separated, dedicated only
	AttachmentsJSON string `gorm:"column:attachments_json;type:text"` // volume attachments, JSON
	ImageRef        string `gorm:"size:512"`
	State           string `gorm:"size:32;index"`
	NodeID          string `gorm:"size:64;index"`
	Generation      int
	PendingReason   string `gorm:"size:1024"`
	FailureReason   string `gorm:"size:1024"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
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
	IP             string `gorm:"size:64"`
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
	Instances      string `gorm:"type:text"`                    // comma-separated
	PinnedCPUs     string `gorm:"column:pinned_cpus;type:text"` // comma-separated exclusive CPUs
	LastHeartbeat  time.Time
	NotReadyReason string `gorm:"size:512"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (nodeRow) TableName() string { return "nodes" }

type flavorRow struct {
	Name      string `gorm:"primaryKey;size:64"`
	VCPU      int    `gorm:"column:vcpu"` // legacy count; topology columns below are canonical
	Sockets   int    `gorm:"column:sockets"`
	Cores     int    `gorm:"column:cores"`
	Threads   int    `gorm:"column:threads"`
	Dedicated bool   `gorm:"column:dedicated"`
	RAM       int    `gorm:"column:ram"`
	Disk      int    `gorm:"column:disk"`
}

func toFlavor(r flavorRow) core.Flavor {
	vcpu := core.VCPU{Sockets: r.Sockets, Cores: r.Cores, Threads: r.Threads, Dedicated: r.Dedicated}
	if vcpu.Count() <= 0 && r.VCPU > 0 {
		vcpu = core.VCPUCount(r.VCPU)
	}
	return core.Flavor{Name: r.Name, VCPU: vcpu, RAM: r.RAM}
}

func fromFlavor(f core.Flavor) flavorRow {
	return flavorRow{
		Name: f.Name, VCPU: f.VCPU.Count(),
		Sockets: f.VCPU.Sockets, Cores: f.VCPU.Cores,
		Threads: f.VCPU.Threads, Dedicated: f.VCPU.Dedicated,
		RAM: f.RAM,
	}
}

func (flavorRow) TableName() string { return "flavors" }

func (s *Store) migrate() error {
	if err := s.db.AutoMigrate(&instanceRow{}, &nodeRow{}, &flavorRow{}, &volumeRow{}); err != nil {
		return fmt.Errorf("gormdb: migrate: %w", err)
	}
	return nil
}

// ------------------------------------------------------------- conversions

func toInstance(r instanceRow) core.Instance {
	vcpu := core.VCPU{
		Sockets:   r.FlavorSockets,
		Cores:     r.FlavorCores,
		Threads:   r.FlavorThreads,
		Dedicated: r.FlavorDedicated,
	}
	if vcpu.Count() <= 0 && r.FlavorVCPU > 0 {
		// Rows written before topology columns existed carry only the
		// count; read them back as one socket of N cores, shared.
		vcpu = core.VCPUCount(r.FlavorVCPU)
	}
	return core.Instance{
		ID:            r.ID,
		Name:          r.Name,
		Namespace:     r.Namespace,
		Flavor:        core.Flavor{Name: r.FlavorName, VCPU: vcpu, RAM: r.FlavorRAM},
		PinnedCPUs:    splitInts(r.PinnedCPUs),
		Volumes:       decodeAttachments(r.AttachmentsJSON),
		ImageRef:      r.ImageRef,
		State:         core.InstanceState(r.State),
		NodeID:        r.NodeID,
		Generation:    r.Generation,
		PendingReason: r.PendingReason,
		FailureReason: r.FailureReason,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.UpdatedAt,
	}
}

func fromInstance(i core.Instance) instanceRow {
	return instanceRow{
		ID:              i.ID,
		Name:            i.Name,
		Namespace:       i.Namespace,
		FlavorName:      i.Flavor.Name,
		FlavorVCPU:      i.Flavor.VCPU.Count(),
		FlavorSockets:   i.Flavor.VCPU.Sockets,
		FlavorCores:     i.Flavor.VCPU.Cores,
		FlavorThreads:   i.Flavor.VCPU.Threads,
		FlavorDedicated: i.Flavor.VCPU.Dedicated,
		FlavorRAM:       i.Flavor.RAM,
		PinnedCPUs:      joinInts(i.PinnedCPUs),
		AttachmentsJSON: encodeAttachments(i.Volumes),
		ImageRef:        i.ImageRef,
		State:           string(i.State),
		NodeID:          i.NodeID,
		Generation:      i.Generation,
		PendingReason:   i.PendingReason,
		FailureReason:   i.FailureReason,
		CreatedAt:       i.CreatedAt,
		UpdatedAt:       i.UpdatedAt,
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
		IP:             r.IP,
		Arch:           r.Arch,
		State:          core.NodeState(r.State),
		Enabled:        r.Enabled,
		Driver:         r.Driver,
		Total:          core.Resources{CPU: r.TotalVCPU, RAM: r.TotalRAM, Disk: r.TotalDisk},
		Available:      core.Resources{CPU: r.AvailableVCPU, RAM: r.AvailableRAM, Disk: r.AvailableDisk},
		Usage:          core.Resources{CPU: r.UsageVCPU, RAM: r.UsageRAM, Disk: r.UsageDisk},
		Instances:      ids,
		PinnedCPUs:     splitInts(r.PinnedCPUs),
		LastHeartbeat:  r.LastHeartbeat,
		NotReadyReason: r.NotReadyReason,
	}
}

func fromNode(n core.Node) nodeRow {
	return nodeRow{
		ID:             n.ID,
		Name:           n.Name,
		IP:             n.IP,
		Arch:           n.Arch,
		State:          string(n.State),
		Enabled:        n.Enabled,
		Driver:         n.Driver,
		TotalVCPU:      n.Total.CPU,
		TotalRAM:       n.Total.RAM,
		TotalDisk:      n.Total.Disk,
		AvailableVCPU:  n.Available.CPU,
		AvailableRAM:   n.Available.RAM,
		AvailableDisk:  n.Available.Disk,
		UsageVCPU:      n.Usage.CPU,
		UsageRAM:       n.Usage.RAM,
		UsageDisk:      n.Usage.Disk,
		Instances:      joinList(n.Instances),
		PinnedCPUs:     joinInts(n.PinnedCPUs),
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

// joinInts/splitInts persist CPU sets the same comma-separated way node
// membership is stored: one text column, no join table, parsed whole.
func joinInts(nums []int) string {
	parts := make([]string, 0, len(nums))
	for _, n := range nums {
		parts = append(parts, strconv.Itoa(n))
	}
	return strings.Join(parts, ",")
}

// encodeAttachments/decodeAttachments persist the VM's volume links as one
// JSON document. Links only ever travel whole (read, modified, written
// back) and the volume rows carry the sizes, so no join table: a corrupt
// document reads as "no attachments", never a half-attached one.
func encodeAttachments(atts []core.VolumeAttachment) string {
	if len(atts) == 0 {
		return ""
	}
	raw, err := json.Marshal(atts)
	if err != nil {
		return ""
	}
	return string(raw)
}

func decodeAttachments(s string) []core.VolumeAttachment {
	if s == "" {
		return nil
	}
	var out []core.VolumeAttachment
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return out
}

func splitInts(s string) []int {
	if s == "" {
		return nil
	}
	var out []int
	for _, p := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			continue
		}
		out = append(out, n)
	}
	return out
}

// withTimeout binds a store call to the caller's context, so cancellation and
// deadlines propagate instead of a query outliving its request.
func (s *Store) withTimeout(ctx context.Context) *gorm.DB {
	return s.db.WithContext(ctx)
}
