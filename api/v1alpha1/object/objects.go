// Package object holds the kloudapi object contract: the uniform envelope
// (apiVersion, kind, metadata, spec, status) every resource API speaks, plus
// the converters between it and arsan's internal core types.
//
// It lives in its own package because both the version group (v1alpha1) and
// its handlers need it, and handlers may not import their parent.
package object

import (
	"github.com/cinvat/arsan/internal/core"
)

// APIVersion is the groupVersion this object model is served under.
const APIVersion = "v1alpha1"

// The kloudapi object contract.
//
// Every resource this API serves is an "object": a uniform envelope of
// apiVersion, kind, metadata and a resource-specific spec (desired state) and
// status (observed state). Clients address objects by metadata.name within
// metadata.namespace, never by the internal id, which lives in status. This is
// what lets kloudctl treat every kind the same way, kubectl-style, with no
// per-kind knowledge.
//
// kloudapi is the origin that defines this contract; this service conforms to
// it. The envelope is deliberately minimal — no resourceVersion, no managed
// fields — because it is not Kubernetes, only Kubernetes-shaped enough for a
// client to create, apply, patch and delete uniformly.

// ObjectMeta is the client-visible identity of an object: the address a client
// uses, and nothing internal.
type ObjectMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// APIResourceList is defined in discovery.go; the object kinds below are the
// concrete shapes it advertises.

// --------------------------------------------------------------------- flavors

// FlavorSpec is a flavor's desired sizing. Memory is a Quantity ("32MB") so it
// reads with its unit; VCPU carries the topology. Name is set on the stored
// flavor and, for a VM, echoes the flavor the instance was created with.
type FlavorSpec struct {
	Name   string        `json:"name,omitempty"`
	VCPU   core.VCPU     `json:"vcpu"`
	Memory core.Quantity `json:"memory"`
}

// Flavor is a named, immutable sizing.
type Flavor struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   ObjectMeta `json:"metadata"`
	Spec       FlavorSpec `json:"spec"`
}

// NewFlavor renders a stored flavor as an object.
func NewFlavor(f core.Flavor) Flavor {
	return Flavor{
		APIVersion: APIVersion,
		Kind:       "Flavor",
		Metadata:   ObjectMeta{Name: f.Name},
		Spec:       FlavorSpec{Name: f.Name, VCPU: f.VCPU, Memory: core.Quantity(f.RAM)},
	}
}

// ToCreateRequest converts a flavor object into the service's create request.
func (f Flavor) ToCreateRequest() core.CreateFlavorRequest {
	return core.CreateFlavorRequest{Name: f.Metadata.Name, VCPU: f.Spec.VCPU, Memory: f.Spec.Memory}
}

// --------------------------------------------------------------------- volumes

// VolumeSource is a volume's desired identity: its namespace-local name and
// how it is provisioned. The same shape is reused inside a VM's spec, so a
// volume is spelled the same whether it is created on its own or inline.
type VolumeSource struct {
	// VolumeID claims a prepared volume instead of provisioning one. It is
	// exclusive with the inline fields below.
	VolumeID string `json:"volume_id,omitempty"`
	Name     string `json:"name,omitempty"`
	// Size is a Quantity ("50GB"); zero with an image means "base virtual
	// size".
	Size  core.Quantity `json:"size,omitempty"`
	Image string        `json:"image,omitempty"`
	Cdrom bool          `json:"cdrom,omitempty"`
	// BootIndex orders boot: 0 boots first, nil means not bootable.
	BootIndex           *int `json:"boot_index,omitempty"`
	DeleteOnTermination bool `json:"delete_on_termination,omitempty"`
}

// VolumeStatus is a volume's observed state.
type VolumeStatus struct {
	ID                  string `json:"id"`
	State               string `json:"state"`
	SizeMiB             int    `json:"sizeMiB,omitempty"`
	AttachedTo          string `json:"attachedTo,omitempty"`
	NodeID              string `json:"nodeId,omitempty"`
	DeleteOnTermination bool   `json:"deleteOnTermination,omitempty"`
}

// Volume is a named, independent block device.
type Volume struct {
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"`
	Metadata   ObjectMeta   `json:"metadata"`
	Spec       VolumeSource `json:"spec"`
	Status     VolumeStatus `json:"status"`
}

// NewVolume renders a stored volume as an object. The spec carries the
// provisioned facts (size, image); volume_id only means something on create.
func NewVolume(v core.Volume) Volume {
	return Volume{
		APIVersion: APIVersion,
		Kind:       "Volume",
		Metadata:   ObjectMeta{Name: v.Name, Namespace: v.Namespace},
		Spec: VolumeSource{
			Size:                core.Quantity(v.SizeMiB),
			Image:               v.Image,
			Cdrom:               v.Cdrom,
			BootIndex:           v.BootIndex,
			DeleteOnTermination: v.DeleteOnTermination,
		},
		Status: VolumeStatus{
			ID:                  v.ID,
			State:               string(v.State),
			SizeMiB:             v.SizeMiB,
			AttachedTo:          v.AttachedTo,
			NodeID:              v.NodeID,
			DeleteOnTermination: v.DeleteOnTermination,
		},
	}
}

// ToCreateRequest converts a volume object into the service's create request.
func (v Volume) ToCreateRequest() core.CreateVolumeRequest {
	return core.CreateVolumeRequest{
		Name:                v.Metadata.Name,
		Namespace:           v.Metadata.Namespace,
		Size:                v.Spec.Size,
		Image:               v.Spec.Image,
		Cdrom:               v.Spec.Cdrom,
		BootIndex:           v.Spec.BootIndex,
		DeleteOnTermination: v.Spec.DeleteOnTermination,
	}
}

// ToVolumeSpec renders a volume source as the create-time block-device entry a
// VM's spec carries.
func (s VolumeSource) ToVolumeSpec() core.VolumeSpec {
	return core.VolumeSpec{
		VolumeID:            s.VolumeID,
		Name:                s.Name,
		Size:                s.Size,
		Image:               s.Image,
		Cdrom:               s.Cdrom,
		BootIndex:           s.BootIndex,
		DeleteOnTermination: s.DeleteOnTermination,
	}
}

// --------------------------------------------------------------------- nodes

// NodeStatus is a node's whole observed state. A node has no spec: it is
// registered by its agent and described by the controller's probe, never
// desired by a client.
type NodeStatus struct {
	ID             string         `json:"id"`
	Arch           string         `json:"arch,omitempty"`
	State          string         `json:"state"`
	Enabled        bool           `json:"enabled"`
	Driver         string         `json:"driver,omitempty"`
	IP             string         `json:"ip,omitempty"`
	Total          core.Resources `json:"total"`
	Available      core.Resources `json:"available"`
	PinnedCPUs     []int          `json:"pinnedCPUs,omitempty"`
	Instances      []string       `json:"instances,omitempty"`
	NotReadyReason string         `json:"notReadyReason,omitempty"`
}

// Node is a physical host.
type Node struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   ObjectMeta `json:"metadata"`
	Status     NodeStatus `json:"status"`
}

// NewNode renders a stored node as an object.
func NewNode(n core.Node) Node {
	return Node{
		APIVersion: APIVersion,
		Kind:       "Node",
		Metadata:   ObjectMeta{Name: n.Name},
		Status: NodeStatus{
			ID: n.ID, Arch: n.Arch, State: string(n.State), Enabled: n.Enabled,
			Driver: n.Driver, IP: n.IP, Total: n.Total, Available: n.Available,
			PinnedCPUs: n.PinnedCPUs, Instances: n.Instances,
			NotReadyReason: n.NotReadyReason,
		},
	}
}

// ------------------------------------------------------------ virtualmachines

// VirtualMachineSpec is a VM's desired state. Image and volumes are set at
// create; the flavor may be patched (a resize), which the engine applies with
// a redefine — a reboot. Volumes are attached and detached through their own
// subresources after create.
type VirtualMachineSpec struct {
	// Image boots from an image: a root volume is created over it, boot index
	// 0, delete on termination.
	Image string `json:"image,omitempty"`
	// FlavorRef names a stored flavor; Flavor is the inline sizing fallback.
	// FlavorRef wins when both are set.
	FlavorRef string      `json:"flavorRef,omitempty"`
	Flavor    *FlavorSpec `json:"flavor,omitempty"`
	// Volumes attaches storage at create: inline sources to provision, or
	// {volume_id} to claim a prepared volume. Managed via subresources after.
	Volumes []VolumeSource `json:"volumes,omitempty"`
}

// VolumeAttachmentStatus is a VM's view of one attached volume.
type VolumeAttachmentStatus struct {
	VolumeID  string `json:"volumeId"`
	Device    string `json:"device,omitempty"`
	BootIndex *int   `json:"bootIndex,omitempty"`
}

// VirtualMachineStatus is a VM's observed state, owned by the controller.
type VirtualMachineStatus struct {
	ID            string                   `json:"id"`
	State         string                   `json:"state"`
	NodeID        string                   `json:"nodeId,omitempty"`
	ImageRef      string                   `json:"imageRef,omitempty"`
	Flavor        FlavorSpec               `json:"flavor"`
	Volumes       []VolumeAttachmentStatus `json:"volumes,omitempty"`
	PinnedCPUs    []int                    `json:"pinnedCPUs,omitempty"`
	PendingReason string                   `json:"pendingReason,omitempty"`
	FailureReason string                   `json:"failureReason,omitempty"`
}

// VirtualMachine is a guest.
type VirtualMachine struct {
	APIVersion string               `json:"apiVersion"`
	Kind       string               `json:"kind"`
	Metadata   ObjectMeta           `json:"metadata"`
	Spec       VirtualMachineSpec   `json:"spec"`
	Status     VirtualMachineStatus `json:"status"`
}

// NewVirtualMachine renders a stored instance as an object. The flavor in
// spec is the current sizing (what a resize would change); the flavor snapshot
// the instance keeps is echoed in status.
func NewVirtualMachine(i core.Instance) VirtualMachine {
	vols := make([]VolumeAttachmentStatus, 0, len(i.Volumes))
	for _, a := range i.Volumes {
		vols = append(vols, VolumeAttachmentStatus{VolumeID: a.VolumeID, Device: a.Device, BootIndex: a.BootIndex})
	}
	return VirtualMachine{
		APIVersion: APIVersion,
		Kind:       "VirtualMachine",
		Metadata:   ObjectMeta{Name: i.Name, Namespace: i.Namespace},
		Spec: VirtualMachineSpec{
			Image:  i.ImageRef,
			Flavor: &FlavorSpec{Name: i.Flavor.Name, VCPU: i.Flavor.VCPU, Memory: core.Quantity(i.Flavor.RAM)},
		},
		Status: VirtualMachineStatus{
			ID: i.ID, State: string(i.State), NodeID: i.NodeID, ImageRef: i.ImageRef,
			Flavor:  FlavorSpec{Name: i.Flavor.Name, VCPU: i.Flavor.VCPU, Memory: core.Quantity(i.Flavor.RAM)},
			Volumes: vols, PinnedCPUs: i.PinnedCPUs,
			PendingReason: i.PendingReason, FailureReason: i.FailureReason,
		},
	}
}

// ToCreateRequest converts a VM object into the service's create request.
func (v VirtualMachine) ToCreateRequest() core.CreateInstanceRequest {
	req := core.CreateInstanceRequest{
		Name: v.Metadata.Name, Namespace: v.Metadata.Namespace,
		ImageRef: v.Spec.Image, FlavorRef: v.Spec.FlavorRef,
	}
	if v.Spec.Flavor != nil {
		req.Flavor = core.Flavor{VCPU: v.Spec.Flavor.VCPU, RAM: v.Spec.Flavor.Memory.MiB()}
	}
	for _, vol := range v.Spec.Volumes {
		req.Volumes = append(req.Volumes, vol.ToVolumeSpec())
	}
	return req
}
