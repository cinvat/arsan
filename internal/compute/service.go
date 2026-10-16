package compute

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/core"
)

const (
	DefaultDriverOpTimeout = 5 * time.Minute
	DefaultSweepInterval   = 30 * time.Second
	DefaultStaleAfter      = 2 * time.Minute
)

type DriverFor func(nodeID string) (core.Driver, error)

type Config struct {
	Instances              core.InstanceStore
	Nodes                  core.NodeStore
	Scheduler              core.Scheduler
	DriverFor              DriverFor
	DriverOperationTimeout time.Duration
	SweepInterval          time.Duration
	StaleAfter             time.Duration
	Log                    Logger
}

type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type Service struct {
	cfg Config
}

func New(cfg Config) *Service {
	if cfg.DriverOperationTimeout == 0 {
		cfg.DriverOperationTimeout = DefaultDriverOpTimeout
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = DefaultSweepInterval
	}
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = DefaultStaleAfter
	}
	return &Service{cfg: cfg}
}

func (s *Service) Get(ctx context.Context, id string) (core.Instance, error) {
	return s.cfg.Instances.Get(ctx, id)
}

func (s *Service) List(ctx context.Context) ([]core.Instance, error) {
	return s.cfg.Instances.List(ctx)
}

func (s *Service) Delete(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.NodeID != "" {
		// Tear the guest down on its host first. Engine Delete destroys a
		// running domain (one that ignored ACPI shutdown included) and
		// undefines it; a missing domain is success, so a redelivered
		// command converges instead of wedging. Anything else fails the
		// command and redelivers: cleaning the row while the guest lives
		// would strand a domain no record points at.
		if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
			return d.Delete(dctx, id)
		}); err != nil && !errors.Is(err, core.ErrInstanceNotFound) {
			return err
		}
	}
	// The row goes away but the claim must come back first, or every deleted
	// VM permanently shrinks its node.
	if err := s.cfg.Instances.ReleaseReservations(ctx, inst.ID, inst.NodeID); err != nil {
		s.cfg.Log.Error("reservation leak", "instance", id, "node", inst.NodeID, "err", err)
	}
	return s.cfg.Instances.Delete(ctx, id)
}

// withEngine resolves the engine for the node holding id and runs fn against
// it, bounded by the operation timeout.
func (s *Service) withEngine(ctx context.Context, nodeID string, fn func(context.Context, core.Driver) error) error {
	d, err := s.cfg.DriverFor(nodeID)
	if err != nil {
		return err
	}
	dctx, cancel := context.WithTimeout(ctx, s.cfg.DriverOperationTimeout)
	defer cancel()
	return fn(dctx, d)
}

func (s *Service) Start(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateShutoff {
		return fmt.Errorf("start: %w", core.ErrInvalidTransition)
	}
	// Boot it where it already lives: the claim and placement survived the
	// stop, so there is nothing to schedule.
	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.Start(dctx, id)
	}); err != nil {
		return err
	}
	_, err = s.cfg.Instances.Transition(ctx, id, core.StateShutoff, core.StateRunning)
	return err
}

func (s *Service) Stop(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateRunning {
		return fmt.Errorf("stop: %w", core.ErrInvalidTransition)
	}

	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.Stop(dctx, id)
	}); err != nil {
		return err
	}

	_, err = s.cfg.Instances.Transition(ctx, id, core.StateRunning, core.StateShutoff)
	return err
}

// createRequestOf renders the engine request from a record: full flavor,
// full disk set, pins. Attach and detach mutate the record first, so by the
// time the engine is called the request already describes the desired state.
func createRequestOf(inst core.Instance) core.CreateRequest {
	return core.CreateRequest{
		InstanceID: inst.ID, NodeID: inst.NodeID, Name: inst.Name, Namespace: inst.Namespace,
		Flavor: inst.Flavor, Disks: inst.Disks, CdromImage: inst.CdromImage, BootDevice: inst.BootDevice,
		PinnedCPUs: inst.PinnedCPUs, ImageRef: inst.ImageRef,
	}
}

// liveStates are the states a disk may be attached to or detached from. Boot
// switches are define-time and need SHUTOFF, enforced by SetBootDevice.
func liveStates(state core.InstanceState) bool {
	return state == core.StateRunning || state == core.StateShutoff
}

// AttachDisk validates, records, then converges: live hotplug when the guest
// runs, redefine when it does not. A failed converge reverts the record, so
// the next attempt starts from the truth rather than a half-applied wish.
func (s *Service) AttachDisk(ctx context.Context, id string, disk core.CreateDiskRequest) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if !liveStates(inst.State) {
		return fmt.Errorf("attach disk in state %s: %w", inst.State, core.ErrInvalidTransition)
	}
	if err := core.ValidateName(disk.Name); err != nil {
		return err
	}
	for _, d := range inst.Disks {
		if d.Name == disk.Name {
			return fmt.Errorf("attach disk %q: already attached: %w", disk.Name, core.ErrInvalidRequest)
		}
	}
	if disk.Size.MiB() <= 0 {
		return fmt.Errorf("attach disk %q: size is required: %w", disk.Name, core.ErrInvalidRequest)
	}
	maxIndex := 0
	for _, d := range inst.Disks {
		maxIndex = max(maxIndex, d.Index)
	}
	attached := core.Disk{Name: disk.Name, Index: maxIndex + 1, SizeMiB: disk.Size.MiB(), Image: disk.Image}
	inst.Disks = append(inst.Disks, attached)
	updated, err := s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return err
	}
	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.AttachDisk(dctx, createRequestOf(updated), attached)
	}); err != nil {
		if rerr := s.revertDisks(ctx, id, inst.Disks[:len(inst.Disks)-1]); rerr != nil {
			s.cfg.Log.Error("attach revert", "instance", id, "err", rerr)
		}
		return err
	}
	return nil
}

// revertDisks restores a previous disk set after a failed converge. Best
// effort by design: the record is already newer than the hypervisor, and a
// failed revert only widens that gap by one update.
func (s *Service) revertDisks(ctx context.Context, id string, disks []core.Disk) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	inst.Disks = disks
	_, err = s.cfg.Instances.Update(ctx, inst)
	return err
}

// DetachDisk unplugs one extra disk and deletes its file. The root overlay
// (index 0) is not listed and cannot be detached: it is the VM's disk,
// not an attachment.
func (s *Service) DetachDisk(ctx context.Context, id, name string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if !liveStates(inst.State) {
		return fmt.Errorf("detach disk in state %s: %w", inst.State, core.ErrInvalidTransition)
	}
	var removed core.Disk
	kept := inst.Disks[:0:0]
	for _, d := range inst.Disks {
		if d.Name == name {
			removed = d
			continue
		}
		kept = append(kept, d)
	}
	if removed.Name == "" {
		return fmt.Errorf("detach disk %q: not attached: %w", name, core.ErrInstanceNotFound)
	}
	saved := inst.Disks
	inst.Disks = kept
	updated, err := s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return err
	}
	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.DetachDisk(dctx, createRequestOf(updated), removed)
	}); err != nil {
		if rerr := s.revertDisks(ctx, id, saved); rerr != nil {
			s.cfg.Log.Error("detach revert", "instance", id, "err", rerr)
		}
		return err
	}
	return nil
}

// AttachCdrom plugs the iso read-only. At most one: detach first to swap
// rescue images, explicitly rather than silently replacing boot media.
func (s *Service) AttachCdrom(ctx context.Context, id, image string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if !liveStates(inst.State) {
		return fmt.Errorf("attach cdrom in state %s: %w", inst.State, core.ErrInvalidTransition)
	}
	if image == "" {
		return fmt.Errorf("attach cdrom: image required: %w", core.ErrInvalidRequest)
	}
	if inst.CdromImage != "" {
		return fmt.Errorf("attach cdrom: %q already attached, detach first: %w", inst.CdromImage, core.ErrInvalidRequest)
	}
	inst.CdromImage = image
	updated, err := s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return err
	}
	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.AttachCdrom(dctx, createRequestOf(updated))
	}); err != nil {
		inst.CdromImage = ""
		if _, rerr := s.cfg.Instances.Update(ctx, inst); rerr != nil {
			s.cfg.Log.Error("attach cdrom revert", "instance", id, "err", rerr)
		}
		return err
	}
	return nil
}

// DetachCdrom removes the iso. The file is never deleted: isos are shared,
// like base images.
func (s *Service) DetachCdrom(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if !liveStates(inst.State) {
		return fmt.Errorf("detach cdrom in state %s: %w", inst.State, core.ErrInvalidTransition)
	}
	if inst.CdromImage == "" {
		return fmt.Errorf("detach cdrom: none attached: %w", core.ErrInvalidRequest)
	}
	removed := inst.CdromImage
	inst.CdromImage = ""
	updated, err := s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return err
	}
	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.DetachCdrom(dctx, createRequestOf(updated), removed)
	}); err != nil {
		inst.CdromImage = removed
		if _, rerr := s.cfg.Instances.Update(ctx, inst); rerr != nil {
			s.cfg.Log.Error("detach cdrom revert", "instance", id, "err", rerr)
		}
		return err
	}
	return nil
}

// SetBootDevice selects what boots next: disk or cdrom. Boot order lives in
// the defined XML, so this is a stopped redefine — never a live change.
func (s *Service) SetBootDevice(ctx context.Context, id, device string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateShutoff {
		return fmt.Errorf("set boot in state %s: stop the VM first: %w", inst.State, core.ErrInvalidTransition)
	}
	if device != core.BootDisk && device != core.BootCDROM {
		return fmt.Errorf("boot device %q: want disk or cdrom: %w", device, core.ErrInvalidRequest)
	}
	if device == core.BootCDROM && inst.CdromImage == "" {
		return fmt.Errorf("boot cdrom without an attached iso: %w", core.ErrInvalidRequest)
	}
	inst.BootDevice = device
	updated, err := s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return err
	}
	return s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.UpdateDisks(dctx, createRequestOf(updated))
	})
}

func (s *Service) Migrate(ctx context.Context, id, targetNodeID string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateRunning {
		return fmt.Errorf("migrate: %w", core.ErrInvalidTransition)
	}

	return s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.Migrate(dctx, id, targetNodeID)
	})
}

// Build drives a PENDING instance to RUNNING: schedule, claim, create on the
// engine, start, activate.
//
// This is the entry point for a command-bus handler. It is idempotent in the way
// the whole service is: the caller is expected to have re-read the record, and
// the compare-and-swap transitions reject a build whose instance has already
// advanced past PENDING.
func (s *Service) Build(ctx context.Context, inst core.Instance) error {
	return s.build(ctx, inst.ID)
}

func (s *Service) build(ctx context.Context, id string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get %s: %w", id, err)
	}

	placed, err := s.cfg.Scheduler.Schedule(ctx, core.PlacementRequest{
		InstanceID: inst.ID,
		Flavor:     inst.Flavor,
		Disks:      inst.Disks,
		ImageRef:   inst.ImageRef,
		Generation: inst.Generation,
	})
	if err != nil {
		return s.pending(ctx, inst, err)
	}

	inst, err = s.cfg.Instances.Transition(ctx, id, core.StatePending, core.StateScheduling)
	if err != nil {
		return fmt.Errorf("claim %s: %w", id, err)
	}
	inst.NodeID = placed.NodeID
	inst.Generation = placed.Generation
	// A dedicated placement carries exclusive CPUs; they live on the instance
	// so teardown frees exactly them, and the engine pins to exactly them.
	inst.PinnedCPUs = placed.PinnedCPUs
	inst, err = s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return fmt.Errorf("record placement: %w", err)
	}

	if err := s.withEngine(ctx, placed.NodeID, func(dctx context.Context, d core.Driver) error {
		if _, err := d.Create(dctx, core.CreateRequest{
			InstanceID: id, NodeID: placed.NodeID, Name: inst.Name, Namespace: inst.Namespace,
			Flavor: inst.Flavor, Disks: inst.Disks, CdromImage: inst.CdromImage, BootDevice: inst.BootDevice,
			PinnedCPUs: inst.PinnedCPUs, ImageRef: inst.ImageRef,
		}); err != nil {
			return err
		}
		return d.Start(dctx, id)
	}); err != nil {
		return s.fail(ctx, id, core.StateScheduling, err)
	}

	if _, err := s.cfg.Instances.Transition(ctx, id, core.StateScheduling, core.StateBuilding); err != nil {
		return fmt.Errorf("enter building: %w", err)
	}

	_, err = s.cfg.Instances.Transition(ctx, id, core.StateBuilding, core.StateRunning)
	return err
}

// pending records why placement did not happen and leaves the instance
// queued. Unlike fail, nothing is broken: the sweep retries on its own, the
// generation counts attempts, and PendingReason tells an operator what the
// next attempt is waiting on. A failed bus publish still surfaces, because
// the caller was promised a 202 for queued work.
func (s *Service) pending(ctx context.Context, inst core.Instance, cause error) error {
	inst.PendingReason = cause.Error()
	inst.Generation++
	if _, err := s.cfg.Instances.Update(ctx, inst); err != nil {
		return fmt.Errorf("record pending %s: %w", inst.ID, err)
	}
	return fmt.Errorf("build %s: %w", inst.ID, cause)
}

func (s *Service) fail(ctx context.Context, id string, from core.InstanceState, cause error) error {
	inst, err := s.cfg.Instances.Transition(ctx, id, from, core.StateError)
	if err != nil {
		return fmt.Errorf("mark error: %w", err)
	}
	if err := s.cfg.Instances.ReleaseReservations(ctx, inst.ID, inst.NodeID); err != nil {
		s.cfg.Log.Error("reservation leak", "instance", id, "node", inst.NodeID, "err", err)
	}
	return fmt.Errorf("build %s: %w", id, cause)
}

func (s *Service) SweepLoop(ctx context.Context) error {
	t := time.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := s.Reconcile(ctx); err != nil {
				s.cfg.Log.Error("reconcile", "err", err)
			}
		}
	}
}

func (s *Service) Reconcile(ctx context.Context) error {
	all, err := s.cfg.Instances.List(ctx)
	if err != nil {
		return fmt.Errorf("reconcile list: %w", err)
	}

	for _, i := range all {
		if i.State != core.StateScheduling && i.State != core.StateBuilding {
			continue
		}
		if time.Since(i.UpdatedAt) < s.cfg.StaleAfter {
			continue
		}

		if _, err := s.cfg.Instances.Transition(ctx, i.ID, i.State, core.StatePending); err != nil {
			s.cfg.Log.Warn("sweep", "instance", i.ID, "err", err)
			continue
		}
		// A reset is a fresh queueing: the old wait reason belonged to an
		// attempt that died, and the rebuild records its own. Re-read first:
		// Update refuses state changes, and i still carries the stale one.
		if cur, err := s.cfg.Instances.Get(ctx, i.ID); err == nil {
			cur.PendingReason = ""
			if _, err := s.cfg.Instances.Update(ctx, cur); err != nil {
				s.cfg.Log.Warn("sweep clear reason", "instance", i.ID, "err", err)
			}
		}
		if err := s.cfg.Instances.ReleaseReservations(ctx, i.ID, i.NodeID); err != nil {
			s.cfg.Log.Error("sweep release", "instance", i.ID, "err", err)
		}
		// Re-drive the build here rather than publishing: the retry path owns
		// everything Build needs, and the event it used to emit had no
		// subscribers, so a reset instance sat PENDING forever.
		if err := s.build(ctx, i.ID); err != nil {
			s.cfg.Log.Warn("sweep rebuild", "instance", i.ID, "err", err)
		}
	}
	return nil
}
