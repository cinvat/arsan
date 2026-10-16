package compute

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/samber/lo"
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
	Volumes                core.VolumeStore
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
	// Free the volumes: detach everything, then delete the records that
	// asked to die with the VM. Shared volumes survive as available rows;
	// cdroms always survive (their delete flag is forced off).
	for _, att := range inst.Volumes {
		v, err := s.cfg.Volumes.GetVolume(ctx, att.VolumeID)
		if err != nil {
			continue // row already gone: nothing to free
		}
		if v.State == core.VolumeInUse && v.AttachedTo == id {
			if _, err := s.cfg.Volumes.DetachVolume(ctx, v.ID); err != nil {
				s.cfg.Log.Error("volume leak", "instance", id, "volume", v.ID, "err", err)
			}
		}
		if v.DeleteOnTermination {
			if err := s.cfg.Volumes.DeleteVolume(ctx, v.ID); err != nil {
				s.cfg.Log.Error("volume leak", "instance", id, "volume", v.ID, "err", err)
			}
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

// volumeRows resolves an instance's attachments to their volume rows. A
// missing row is a hard error: the attachment names storage that no longer
// exists, and guessing would boot the wrong disk.
func (s *Service) volumeRows(ctx context.Context, inst core.Instance) ([]core.Volume, error) {
	out := make([]core.Volume, 0, len(inst.Volumes))
	for _, att := range inst.Volumes {
		v, err := s.cfg.Volumes.GetVolume(ctx, att.VolumeID)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// createRequestOf renders the engine request from a record plus its volume
// rows. It re-reads volumes every call so attach/detach between scheduling
// and define converge to the latest truth.
func (s *Service) createRequestOf(ctx context.Context, inst core.Instance) (core.CreateRequest, error) {
	attached := make([]core.AttachedVolume, 0, len(inst.Volumes))
	for _, att := range inst.Volumes {
		v, err := s.cfg.Volumes.GetVolume(ctx, att.VolumeID)
		if err != nil {
			return core.CreateRequest{}, err
		}
		attached = append(attached, core.AttachedVolume{Volume: v, Device: att.Device})
	}
	return core.CreateRequest{
		InstanceID: inst.ID, NodeID: inst.NodeID, Name: inst.Name, Namespace: inst.Namespace,
		Flavor: inst.Flavor, Volumes: attached,
		PinnedCPUs: inst.PinnedCPUs, ImageRef: inst.ImageRef,
	}, nil
}

// liveStates are the states a volume may be attached to or detached from.
// Boot order itself is define-time and needs SHUTOFF.
func liveStates(state core.InstanceState) bool {
	return state == core.StateRunning || state == core.StateShutoff
}

// freeDevice assigns the lowest free virtio letter: vda, vdb, … Detaches
// never rename survivors, so letters stay stable across the VM's life.
func freeDevice(used []core.VolumeAttachment) string {
	taken := map[byte]bool{}
	for _, att := range used {
		if len(att.Device) == 3 && att.Device[:2] == "vd" {
			taken[att.Device[2]] = true
		}
	}
	for c := byte('a'); c <= byte('z'); c++ {
		if !taken[c] {
			return "vd" + string(c)
		}
	}
	return ""
}

// AttachVolume validates, records, then converges: live hotplug when the
// guest runs, redefine when it does not. A failed converge reverts the
// record and frees the volume, so the next attempt starts from the truth
// rather than a half-applied wish.
func (s *Service) AttachVolume(ctx context.Context, id, volumeID string, bootIndex *int) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if !liveStates(inst.State) {
		return fmt.Errorf("attach volume in state %s: %w", inst.State, core.ErrInvalidTransition)
	}
	vol, err := s.cfg.Volumes.GetVolume(ctx, volumeID)
	if err != nil {
		return err
	}
	if vol.Namespace != inst.Namespace {
		return fmt.Errorf("attach volume %s: cross-namespace: %w", volumeID, core.ErrInvalidRequest)
	}
	for _, att := range inst.Volumes {
		if att.VolumeID == volumeID {
			return fmt.Errorf("attach volume %s: already attached: %w", volumeID, core.ErrInvalidRequest)
		}
	}
	if bootIndex != nil {
		for _, att := range inst.Volumes {
			if att.BootIndex != nil {
				return fmt.Errorf("attach volume %s: boot taken, move it with set-boot first: %w", volumeID, core.ErrInvalidRequest)
			}
		}
	}
	device := freeDevice(inst.Volumes)
	if device == "" {
		return fmt.Errorf("attach volume %s: no free device letters: %w", volumeID, core.ErrInvalidRequest)
	}
	saved := inst.Volumes
	inst.Volumes = append(inst.Volumes, core.VolumeAttachment{VolumeID: volumeID, Device: device, BootIndex: bootIndex})
	updated, err := s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return err
	}
	if _, err := s.cfg.Volumes.AttachVolume(ctx, volumeID, id); err != nil {
		_ = s.revertVolumes(ctx, id, saved)
		return err
	}
	req, err := s.createRequestOf(ctx, updated)
	if err != nil {
		_, _ = s.cfg.Volumes.DetachVolume(ctx, volumeID)
		_ = s.revertVolumes(ctx, id, saved)
		return err
	}
	attached := core.AttachedVolume{Volume: vol, Device: device}
	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.AttachVolume(dctx, req, attached)
	}); err != nil {
		_, _ = s.cfg.Volumes.DetachVolume(ctx, volumeID)
		if rerr := s.revertVolumes(ctx, id, saved); rerr != nil {
			s.cfg.Log.Error("attach revert", "instance", id, "err", rerr)
		}
		return err
	}
	return nil
}

// revertVolumes restores a previous attachment list after a failed converge.
// Best effort by design: the record is already newer than the hypervisor,
// and a failed revert only widens that gap by one update.
func (s *Service) revertVolumes(ctx context.Context, id string, atts []core.VolumeAttachment) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	inst.Volumes = atts
	_, err = s.cfg.Instances.Update(ctx, inst)
	return err
}

// DetachVolume unplugs the volume and frees it back to available storage.
// Files are never deleted here: only VM delete with DeleteOnTermination
// removes storage.
func (s *Service) DetachVolume(ctx context.Context, id, volumeID string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if !liveStates(inst.State) {
		return fmt.Errorf("detach volume in state %s: %w", inst.State, core.ErrInvalidTransition)
	}
	removed, found := lo.Find(inst.Volumes, func(att core.VolumeAttachment) bool {
		return att.VolumeID == volumeID
	})
	if !found {
		return fmt.Errorf("detach volume %s: not attached: %w", volumeID, core.ErrInstanceNotFound)
	}
	kept := lo.Reject(inst.Volumes, func(att core.VolumeAttachment, _ int) bool {
		return att.VolumeID == volumeID
	})
	vol, err := s.cfg.Volumes.GetVolume(ctx, volumeID)
	if err != nil {
		return err
	}
	saved := inst.Volumes
	inst.Volumes = kept
	updated, err := s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return err
	}
	if _, err := s.cfg.Volumes.DetachVolume(ctx, volumeID); err != nil {
		_ = s.revertVolumes(ctx, id, saved)
		return err
	}
	req, err := s.createRequestOf(ctx, updated)
	if err != nil {
		return err
	}
	if err := s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.DetachVolume(dctx, req, core.AttachedVolume{Volume: vol, Device: removed.Device})
	}); err != nil {
		return err
	}
	return nil
}

// SetBootVolume moves the single boot position to another attached volume:
// boot_index 0 there, cleared everywhere else, on both rows and mirrors.
// Boot order lives in the defined XML, so this is a stopped redefine —
// never a live change. Rescue is attach a cdrom volume, set boot to it,
// start.
func (s *Service) SetBootVolume(ctx context.Context, id, volumeID string) error {
	inst, err := s.cfg.Instances.Get(ctx, id)
	if err != nil {
		return err
	}
	if inst.State != core.StateShutoff {
		return fmt.Errorf("set boot in state %s: stop the VM first: %w", inst.State, core.ErrInvalidTransition)
	}
	if !lo.ContainsBy(inst.Volumes, func(att core.VolumeAttachment) bool {
		return att.VolumeID == volumeID
	}) {
		return fmt.Errorf("set boot %s: not attached: %w", volumeID, core.ErrInvalidRequest)
	}
	for _, att := range inst.Volumes {
		var want *int
		if att.VolumeID == volumeID {
			zero := 0
			want = &zero
		}
		if _, err := s.cfg.Volumes.SetVolumeBootIndex(ctx, att.VolumeID, want); err != nil {
			return err
		}
	}
	inst.Volumes = lo.Map(inst.Volumes, func(att core.VolumeAttachment, _ int) core.VolumeAttachment {
		att.BootIndex = nil
		if att.VolumeID == volumeID {
			zero := 0
			att.BootIndex = &zero
		}
		return att
	})
	updated, err := s.cfg.Instances.Update(ctx, inst)
	if err != nil {
		return err
	}
	req, err := s.createRequestOf(ctx, updated)
	if err != nil {
		return err
	}
	return s.withEngine(ctx, inst.NodeID, func(dctx context.Context, d core.Driver) error {
		return d.UpdateVolumes(dctx, req)
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

	volumes, err := s.volumeRows(ctx, inst)
	if err != nil {
		return fmt.Errorf("resolve volumes %s: %w", id, err)
	}
	placed, err := s.cfg.Scheduler.Schedule(ctx, core.PlacementRequest{
		InstanceID: inst.ID,
		Flavor:     inst.Flavor,
		Volumes:    volumes,
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

	req, err := s.createRequestOf(ctx, inst)
	if err != nil {
		return fmt.Errorf("resolve volumes %s: %w", id, err)
	}
	if err := s.withEngine(ctx, placed.NodeID, func(dctx context.Context, d core.Driver) error {
		if _, err := d.Create(dctx, req); err != nil {
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
	// Free what the placement claimed: volumes first so a retry starts
	// unattached, then the node counts. Inline-created volumes stay on disk
	// as reusable available rows rather than leaking attached.
	for _, att := range inst.Volumes {
		if _, derr := s.cfg.Volumes.DetachVolume(ctx, att.VolumeID); derr != nil {
			s.cfg.Log.Error("reservation leak", "instance", id, "volume", att.VolumeID, "err", derr)
		}
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
		// TERMINATING is deletion in progress: drive the teardown on every
		// sweep, with no staleness gate. Delete is idempotent, so this is
		// safe to repeat, and it is the only thing that can finish a delete
		// whose agent is unreachable — the record must not outlive the
		// intent just because a host is off.
		if i.State == core.StateTerminating {
			if err := s.Delete(ctx, i.ID); err != nil {
				s.cfg.Log.Warn("sweep teardown", "instance", i.ID, "err", err)
			}
			continue
		}
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
