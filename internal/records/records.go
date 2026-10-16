// Package records is the api's CRUD layer.
//
// It exists so the api has no dependency on the controller's services. Two
// types, not one: core.InstanceService and core.NodeService both declare
// Get(ctx, id) and List(ctx) with different result types, so no single type can
// satisfy both.
//
// Nothing here decides anything. Records are validated, written, and read;
// anything that requires a judgement — where a VM runs, whether a transition is
// legal, whether a node is healthy — is published to the command bus for the
// controller. The one exception is instance creation, because a PENDING row is
// a fact about the request, not a decision: it records that the user asked.
package records

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/pkg/util"
)

// Nodes implements core.NodeService over a node store.
type Nodes struct {
	Store core.NodeStore
}

// Register upserts a node. Only the bootstrap endpoint reaches it, and that
// endpoint authenticates before calling.
func (s *Nodes) Register(ctx context.Context, req core.RegisterNodeRequest) (core.Node, error) {
	if req.Name == "" {
		return core.Node{}, fmt.Errorf("name required: %w", core.ErrInvalidRequest)
	}
	return s.Store.Register(ctx, core.Node{
		ID:      NodeID(req.Name),
		Name:    req.Name,
		IP:      req.IP,
		Enabled: true,
	})
}

// NodeID derives a node's stable ID from its name.
//
// "Compute Node 1" and "compute-node-1" must resolve to the same node, or one
// host registers twice and its reservations are split across two records.
func NodeID(name string) string { return "n-" + util.Slug(name) }

// RecordUsage stores what an agent reports it is using. It does not change
// readiness: that is the controller's probe, and nothing here may promote a node
// the probe has not cleared.
func (s *Nodes) RecordUsage(ctx context.Context, nodeID string, usage core.Resources) error {
	_, err := s.Store.RecordUsage(ctx, nodeID, usage, time.Now().UTC())
	return err
}

func (s *Nodes) Get(ctx context.Context, id string) (core.Node, error) {
	return s.Store.GetNode(ctx, id)
}

func (s *Nodes) List(ctx context.Context) ([]core.Node, error) {
	return s.Store.ListNodes(ctx)
}

// Enable returns a node to service. Readiness itself is decided by the next
// probe, not here: re-enabling a node that is still unreachable would put it
// back in the scheduler's eligible set on the strength of an operator's intent.
func (s *Nodes) Enable(ctx context.Context, id string) error {
	_, err := s.Store.SetNodeEnabled(ctx, id, true)
	return err
}

// Disable takes a node out of service. It stops receiving new VMs immediately
// and no probe may undo it. The reason names the operator, so a later probe
// failure cannot misdescribe their decision as a broken agent.
func (s *Nodes) Disable(ctx context.Context, id string) error {
	if _, err := s.Store.SetNodeEnabled(ctx, id, false); err != nil {
		return err
	}
	_, err := s.Store.SetReadiness(ctx, id, core.NodeNotReady, "disabled by operator")
	return err
}

// Flavors implements core.FlavorService. Flavors are immutable definitions;
// there is no update, only create and delete (delete refused while in use).
type Flavors struct {
	Store core.FlavorStore
}

func (s *Flavors) Create(ctx context.Context, req core.CreateFlavorRequest) (core.Flavor, error) {
	return s.Store.CreateFlavor(ctx, core.Flavor{Name: req.Name, VCPU: req.VCPU, RAM: req.Memory.MiB()})
}

func (s *Flavors) Get(ctx context.Context, name string) (core.Flavor, error) {
	return s.Store.GetFlavor(ctx, name)
}

func (s *Flavors) List(ctx context.Context) ([]core.Flavor, error) {
	return s.Store.ListFlavors(ctx)
}

func (s *Flavors) Delete(ctx context.Context, name string) error {
	return s.Store.DeleteFlavor(ctx, name)
}

// resolveVolumes turns block-device specs into volume rows plus the
// instance's attachment list. Inline entries provision new rows; volume_id
// entries claim prepared ones, which must be available.
//
// Rules, Nova-style: image_ref alone auto-creates a boot root (it may not
// combine with an explicit boot volume); without image_ref the specs must
// carry exactly one boot_index: 0 entry; device names run vda, vdb… in
// attach order and never move.
func (s *Instances) resolveVolumes(ctx context.Context, instID string, req core.CreateInstanceRequest) ([]core.VolumeAttachment, error) {
	if s.Volumes == nil {
		return nil, fmt.Errorf("volume store not configured: %w", core.ErrInvalidRequest)
	}
	var volumes []core.Volume
	bootable := 0
	attach := func(v core.Volume) {
		volumes = append(volumes, v)
		if v.BootIndex != nil {
			bootable++
		}
	}
	for i, spec := range req.Volumes {
		if spec.VolumeID != "" {
			if spec.Name != "" || spec.Size.MiB() != 0 || spec.Image != "" || spec.Cdrom || spec.BootIndex != nil || spec.DeleteOnTermination {
				return nil, fmt.Errorf("volume entry %d: volume_id is exclusive with inline fields: %w", i, core.ErrInvalidRequest)
			}
			v, err := s.Volumes.GetVolume(ctx, spec.VolumeID)
			if err != nil {
				return nil, err
			}
			if v.Namespace != req.Namespace {
				return nil, fmt.Errorf("volume %s: cross-namespace attach: %w", v.ID, core.ErrInvalidRequest)
			}
			if v.State != core.VolumeAvailable {
				return nil, fmt.Errorf("volume %s: held by %s: %w", v.ID, v.AttachedTo, core.ErrCapacityConflict)
			}
			attach(v)
			continue
		}
		if err := core.ValidateName(spec.Name); err != nil {
			return nil, fmt.Errorf("volume entry %d: %w", i, err)
		}
		if spec.Size.MiB() <= 0 && !spec.Cdrom && spec.Image == "" {
			return nil, fmt.Errorf("volume %q needs a size or a base image: %w", spec.Name, core.ErrInvalidRequest)
		}
		v, err := s.Volumes.CreateVolume(ctx, core.Volume{
			ID:   "vol-" + util.NewID(""),
			Name: spec.Name, Namespace: req.Namespace,
			SizeMiB: spec.Size.MiB(), Image: spec.Image, Cdrom: spec.Cdrom,
			BootIndex: spec.BootIndex, DeleteOnTermination: spec.DeleteOnTermination && !spec.Cdrom,
		})
		if err != nil {
			return nil, err
		}
		// Inline volumes are ours from birth: hold them now so a concurrent
		// create cannot claim the row before placement does.
		if _, err := s.Volumes.AttachVolume(ctx, v.ID, instID); err != nil {
			return nil, err
		}
		v.State = core.VolumeInUse
		v.AttachedTo = instID
		attach(v)
		continue
	}
	if req.ImageRef != "" {
		for _, v := range volumes {
			if v.BootIndex != nil {
				return nil, fmt.Errorf("image_ref with an explicit boot volume is ambiguous: %w", core.ErrInvalidRequest)
			}
		}
		zero := 0
		root, err := s.Volumes.CreateVolume(ctx, core.Volume{
			ID:   "vol-" + util.NewID(""),
			Name: req.Name + "-root", Namespace: req.Namespace,
			Image: req.ImageRef, BootIndex: &zero, DeleteOnTermination: true,
		})
		if err != nil {
			return nil, err
		}
		if _, err := s.Volumes.AttachVolume(ctx, root.ID, instID); err != nil {
			return nil, err
		}
		root.State = core.VolumeInUse
		root.AttachedTo = instID
		volumes = append([]core.Volume{root}, volumes...)
		bootable++
	}
	if bootable == 0 && len(volumes) > 0 {
		zero := 0
		volumes[0].BootIndex = &zero
		if _, err := s.Volumes.SetVolumeBootIndex(ctx, volumes[0].ID, &zero); err != nil {
			return nil, err
		}
		bootable = 1
	}
	if bootable > 1 {
		return nil, fmt.Errorf("at most one boot volume per VM, use set-boot to move it: %w", core.ErrInvalidRequest)
	}
	attachments := make([]core.VolumeAttachment, 0, len(volumes))
	for i, v := range volumes {
		attachments = append(attachments, core.VolumeAttachment{
			VolumeID: v.ID, Device: fmt.Sprintf("vd%c", 'a'+byte(i)), BootIndex: v.BootIndex,
		})
	}
	return attachments, nil
}

// Volumes implements core.VolumeService: named, independent block devices.
type Volumes struct {
	Store core.VolumeStore
}

func (s *Volumes) Create(ctx context.Context, req core.CreateVolumeRequest) (core.Volume, error) {
	if err := core.ValidateName(req.Name); err != nil {
		return core.Volume{}, err
	}
	if req.Namespace == "" {
		req.Namespace = core.DefaultNamespace
	}
	if err := core.ValidateNamespace(req.Namespace); err != nil {
		return core.Volume{}, err
	}
	if req.Size.MiB() <= 0 && !req.Cdrom && req.Image == "" {
		return core.Volume{}, fmt.Errorf("volume needs a size or a base image: %w", core.ErrInvalidRequest)
	}
	return s.Store.CreateVolume(ctx, core.Volume{
		ID:   "vol-" + util.NewID(""),
		Name: req.Name, Namespace: req.Namespace,
		SizeMiB: req.Size.MiB(), Image: req.Image, Cdrom: req.Cdrom,
		BootIndex: req.BootIndex, DeleteOnTermination: req.DeleteOnTermination && !req.Cdrom,
	})
}

func (s *Volumes) Get(ctx context.Context, id string) (core.Volume, error) {
	return s.Store.GetVolume(ctx, id)
}

func (s *Volumes) List(ctx context.Context) ([]core.Volume, error) {
	return s.Store.ListVolumes(ctx)
}

func (s *Volumes) Delete(ctx context.Context, id string) error {
	return s.Store.DeleteVolume(ctx, id)
}

// Instances implements core.InstanceService over an instance store and the bus.
type Instances struct {
	Store core.InstanceStore
	// Flavors resolves FlavorRef at create time. Nil keeps legacy inline-only
	// behavior (tests that construct Instances directly).
	Flavors core.FlavorStore
	// Volumes resolves volume references at create time and owns the
	// lifecycle of inline-provisioned ones.
	Volumes core.VolumeStore
	Bus     bus.Bus
}

// Create validates, persists a PENDING instance, and asks the controller to
// build it.
//
// The PENDING row is not a decision. It records that a user asked for a VM;
// whether and where it runs is the controller's answer. If the publish fails
// the record stays PENDING and the reconciliation sweep picks it up, so the
// caller is told the truth either way.
func (s *Instances) Create(ctx context.Context, req core.CreateInstanceRequest) (core.Instance, error) {
	if err := core.ValidateName(req.Name); err != nil {
		return core.Instance{}, err
	}
	if req.Namespace == "" {
		req.Namespace = core.DefaultNamespace
	}
	if err := core.ValidateNamespace(req.Namespace); err != nil {
		return core.Instance{}, err
	}
	if req.ImageRef == "" && len(req.Volumes) == 0 {
		return core.Instance{}, fmt.Errorf("image_ref or volumes required: %w", core.ErrInvalidRequest)
	}
	// Names are unique per namespace: two VMs cannot share a hypervisor
	// domain name, and the domain name is namespace-name.
	existing, err := s.Store.List(ctx)
	if err != nil {
		return core.Instance{}, err
	}
	for _, e := range existing {
		if e.Namespace == req.Namespace && e.Name == req.Name {
			return core.Instance{}, fmt.Errorf("virtualmachine %s/%s exists: %w",
				req.Namespace, req.Name, core.ErrInvalidRequest)
		}
	}
	flavor := req.Flavor
	if req.FlavorRef != "" {
		if s.Flavors == nil {
			return core.Instance{}, fmt.Errorf("flavor_ref %q: flavor store not configured: %w", req.FlavorRef, core.ErrInvalidRequest)
		}
		f, err := s.Flavors.GetFlavor(ctx, req.FlavorRef)
		if err != nil {
			return core.Instance{}, err
		}
		flavor = f
	}
	if err := flavor.VCPU.Validate(); err != nil {
		return core.Instance{}, err
	}
	if flavor.RAM <= 0 {
		return core.Instance{}, fmt.Errorf("flavor must have positive ram: %w", core.ErrInvalidRequest)
	}
	instID := util.NewID("i")
	attachments, err := s.resolveVolumes(ctx, instID, req)
	if err != nil {
		return core.Instance{}, err
	}

	now := time.Now().UTC()
	inst := core.Instance{
		ID:        instID,
		Name:      req.Name,
		Namespace: req.Namespace,
		Flavor:    flavor,
		Volumes:   attachments,
		ImageRef:  req.ImageRef,
		State:     core.StatePending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	created, err := s.Store.Create(ctx, inst)
	if err != nil {
		return core.Instance{}, err
	}

	if err := s.Bus.Publish(ctx, bus.Command{
		ID:     util.NewID("cmd"),
		Topic:  bus.TopicInstanceCreate,
		Target: created.ID,
	}); err != nil {
		// The record exists but nothing has been asked to act on it. Report
		// the failure rather than a 202 for work nobody was asked to do; the
		// sweep will retry.
		return core.Instance{}, fmt.Errorf("enqueue %s: %w", bus.TopicInstanceCreate, err)
	}
	return created, nil
}

func (s *Instances) Get(ctx context.Context, id string) (core.Instance, error) {
	return s.Store.Get(ctx, id)
}

func (s *Instances) List(ctx context.Context) ([]core.Instance, error) {
	return s.Store.List(ctx)
}

// Delete publishes a command rather than removing the row. The record has to
// outlive the request so the controller can tear the VM down and release its
// reservation; deleting it here would strand both.
func (s *Instances) Delete(ctx context.Context, id string) error {
	return s.command(ctx, bus.TopicInstanceDelete, id)
}

// Start and Stop publish commands for the same reason: the api does not decide
// whether an instance is in a state that permits the change. The controller's
// compare-and-swap rejects the command if it is not.
func (s *Instances) Start(ctx context.Context, id string) error {
	return s.command(ctx, bus.TopicInstanceStart, id)
}

func (s *Instances) Stop(ctx context.Context, id string) error {
	return s.command(ctx, bus.TopicInstanceStop, id)
}

// AttachVolume enqueues an attach of a prepared volume, optionally taking
// the boot position. DetachVolume frees it back to available storage; only
// VM delete with DeleteOnTermination removes files.
func (s *Instances) AttachVolume(ctx context.Context, id, volumeID string, bootIndex *int) error {
	if volumeID == "" {
		return fmt.Errorf("volume id required: %w", core.ErrInvalidRequest)
	}
	payload, err := json.Marshal(map[string]any{"volume_id": volumeID, "boot_index": bootIndex})
	if err != nil {
		return err
	}
	return s.publish(ctx, bus.TopicInstanceAttachVolume, id, payload)
}

func (s *Instances) DetachVolume(ctx context.Context, id, volumeID string) error {
	if volumeID == "" {
		return fmt.Errorf("volume id required: %w", core.ErrInvalidRequest)
	}
	payload, err := json.Marshal(map[string]string{"volume_id": volumeID})
	if err != nil {
		return err
	}
	return s.publish(ctx, bus.TopicInstanceDetachVolume, id, payload)
}

// SetBootVolume moves the single boot position to another attached volume.
// Rescue is attach (a cdrom volume), set boot to it, start — detach and
// delete it when done.
func (s *Instances) SetBootVolume(ctx context.Context, id, volumeID string) error {
	if volumeID == "" {
		return fmt.Errorf("volume id required: %w", core.ErrInvalidRequest)
	}
	payload, err := json.Marshal(map[string]string{"volume_id": volumeID})
	if err != nil {
		return err
	}
	return s.publish(ctx, bus.TopicInstanceSetBoot, id, payload)
}

func (s *Instances) Migrate(ctx context.Context, id, targetNodeID string) error {
	if targetNodeID == "" {
		return fmt.Errorf("target_node_id required: %w", core.ErrInvalidRequest)
	}
	// The target travels in the payload: the command carries the target it
	// concerns plus its own data, never a snapshot of the record.
	payload, err := json.Marshal(map[string]string{"target_node_id": targetNodeID})
	if err != nil {
		return err
	}
	return s.publish(ctx, bus.TopicInstanceMigrate, id, payload)
}

func (s *Instances) command(ctx context.Context, topic, target string) error {
	return s.publish(ctx, topic, target, nil)
}

func (s *Instances) publish(ctx context.Context, topic, target string, payload []byte) error {
	if _, err := s.Store.Get(ctx, target); err != nil {
		// 404 before publishing, so a client does not get a 202 for a
		// command that can only fail.
		return err
	}
	if err := s.Bus.Publish(ctx, bus.Command{
		ID:      util.NewID("cmd"),
		Topic:   topic,
		Target:  target,
		Payload: payload,
	}); err != nil {
		return fmt.Errorf("enqueue %s: %w", topic, err)
	}
	return nil
}

var (
	_ core.NodeService     = (*Nodes)(nil)
	_ core.InstanceService = (*Instances)(nil)
	_ core.FlavorService   = (*Flavors)(nil)
)

// ErrUnimplemented marks a CRUD method the api deliberately does not perform.
// It is never returned by anything the api exposes.
var ErrUnimplemented = errors.New("records: not implemented by the api")
