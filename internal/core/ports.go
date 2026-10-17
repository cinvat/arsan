package core

import (
	"context"
	"time"
)

// ========== Driver Interface ==========

type Driver interface {
	Name() string
	Validate(ctx context.Context, f Flavor) error
	Create(ctx context.Context, req CreateRequest) (InstanceInfo, error)
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
	Reboot(ctx context.Context, id string) error
	Migrate(ctx context.Context, id, targetNodeID string) error
	Usage(ctx context.Context) (Resources, error)
	// Domains reports the hypervisor's observed guest states, keyed by
	// instance id. It is the reconcile input: control-plane states are never
	// reported, and guests the driver does not know are simply absent.
	Domains(ctx context.Context) ([]DomainStatus, error)
	// UpdateVolumes redefines a stopped domain with a new volume set.
	UpdateVolumes(ctx context.Context, req CreateRequest) error
	// AttachVolume provisions the volume's file and plugs it in: live plus
	// config when the guest runs, redefine when it does not.
	AttachVolume(ctx context.Context, req CreateRequest, vol AttachedVolume) error
	// DetachVolume unplugs the volume and deletes its file when the volume
	// says DeleteOnTermination. The record already dropped it; the file
	// delete always happens, so a shutoff detach cannot strand storage.
	DetachVolume(ctx context.Context, req CreateRequest, vol AttachedVolume) error
}

// DomainStatus is one observed guest: the id arsan defined it under and its
// steady guest state (RUNNING, SHUTOFF, SHUTDOWN, PAUSED or CRASHED).
type DomainStatus struct {
	InstanceID string
	State      InstanceState
}

// AttachedVolume is what the engine renders: the volume row plus the device
// name and boot position the attachment carries.
type AttachedVolume struct {
	Volume
	Device string
}

type CreateRequest struct {
	InstanceID string
	NodeID     string
	Name       string
	Namespace  string
	Flavor     Flavor
	// Volumes carries the VM's storage in boot/device order: root overlay,
	// data overlays and blank disks, and read-only cdroms, each with the
	// device name the attachment assigned.
	Volumes []AttachedVolume
	// PinnedCPUs carries the scheduler's exclusive CPUs for a dedicated
	// flavor. Empty for shared flavors; the engine pins only what is here.
	PinnedCPUs []int
	ImageRef   string
}

type InstanceInfo struct {
	ID      string
	NodeID  string
	Driver  string
	Started bool
}

// ========== Store Interfaces ==========

type InstanceStore interface {
	Create(ctx context.Context, i Instance) (Instance, error)
	Get(ctx context.Context, id string) (Instance, error)
	List(ctx context.Context) ([]Instance, error)
	Update(ctx context.Context, i Instance) (Instance, error)
	Delete(ctx context.Context, id string) error

	// Transition is the ONLY way instance state changes.
	// Compare-and-swap on `from` — fails with ErrInvalidTransition if not in `from`.
	Transition(ctx context.Context, id string, from, to InstanceState) (Instance, error)

	ReleaseReservations(ctx context.Context, instanceID, nodeID string) error
}

type NodeStore interface {
	Register(ctx context.Context, n Node) (Node, error)
	GetNode(ctx context.Context, id string) (Node, error)
	ListNodes(ctx context.Context) ([]Node, error)
	// SetNodeEnabled takes a node in or out of service. It is separate from
	// state because with only READY and NOTREADY, "operator disabled it" and
	// "it stopped answering" would be indistinguishable — and a probe must not
	// override an operator's decision.
	SetNodeEnabled(ctx context.Context, id string, enabled bool) (Node, error)
	// SetNodeSpec records what the agent runs and what its host holds, as
	// reported by the controller's probe. Bootstrap never carries this: the
	// agent should not have to know its host to join.
	SetNodeSpec(ctx context.Context, id string, engine string, total Resources) (Node, error)
	// SetReadiness sets a node's readiness together with the reason it is not
	// ready. Passing an empty reason marks it READY.
	SetReadiness(ctx context.Context, id string, state NodeState, reason string) (Node, error)
	// RecordUsage stores the resources an agent reports it is using.
	//
	// It does not touch readiness. Readiness is the controller's probe to
	// decide, and this is only the agent telling the api what it is using.
	RecordUsage(ctx context.Context, id string, usage Resources, at time.Time) (Node, error)

	// Atomic reserve — all-or-nothing. Returns ErrCapacityConflict without mutating if any dimension insufficient.
	ReserveNodeResources(ctx context.Context, nodeID string, r Resources) error
	ReleaseNodeResources(ctx context.Context, nodeID string, r Resources) error

	// ClaimCPUs exclusively reserves count host CPUs and returns which.
	// First-fit over the node's free set; the count decrement rides along so
	// Available.CPU stays the single capacity truth. Lost claims report
	// ErrCapacityConflict without mutating, like ReserveNodeResources.
	ClaimCPUs(ctx context.Context, nodeID string, count int) ([]int, error)
	// ReleaseCPUs frees a previous claim, restoring the count. Unknown CPUs
	// are ignored so a repeated release is a no-op.
	ReleaseCPUs(ctx context.Context, nodeID string, cpus []int) error

	AttachInstance(ctx context.Context, nodeID, instanceID string) error
	DetachInstance(ctx context.Context, nodeID, instanceID string) error
}

// ========== Service Interfaces ==========

type CreateInstanceRequest struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	// ImageRef boots from an image: the api auto-creates a root volume over
	// it (boot_index 0, delete on termination), Nova image-boot style. Omit
	// it when Volumes already carries the boot entry.
	ImageRef string `json:"image_ref"`
	// FlavorRef names a stored flavor. It is the preferred way to size a VM;
	// the instance keeps a snapshot of the flavor's resources at create time,
	// so later flavor edits do not resize existing VMs.
	FlavorRef string `json:"flavor_ref"`
	// Flavor is the legacy inline sizing, kept for back-compat. When FlavorRef
	// is set it wins and Flavor is ignored.
	Flavor Flavor `json:"flavor"`
	// Volumes attaches storage at create: inline definitions to provision,
	// or {volume_id} to claim a prepared one. A rescue disk is just another
	// entry here, booted with SetBootVolume.
	Volumes []VolumeSpec `json:"volumes"`
}

// VolumeSpec is one block_device_mapping entry, Nova-style: either an
// existing volume by id, or an inline definition to provision. Source is
// exactly one of image (base/iso to overlay), volume (attach prepared), or
// neither (blank disk).
type VolumeSpec struct {
	VolumeID string   `json:"volume_id"`
	Name     string   `json:"name"`
	Size     Quantity `json:"size"`
	Image    string   `json:"image"`
	Cdrom    bool     `json:"cdrom"`
	// BootIndex orders boot: 0 boots first. At most one bootable per VM;
	// use SetBootVolume to move it.
	BootIndex *int `json:"boot_index"`
	// DeleteOnTermination removes the file with the record on VM delete.
	// Detaching never deletes.
	DeleteOnTermination bool `json:"delete_on_termination"`
}

// VolumeStore persists named volumes. Attach and Detach are compare-and-swap
// on status: only an available volume attaches, only an attached one detaches,
// so two VMs cannot hold the same disk.
type VolumeStore interface {
	CreateVolume(ctx context.Context, v Volume) (Volume, error)
	GetVolume(ctx context.Context, id string) (Volume, error)
	ListVolumes(ctx context.Context) ([]Volume, error)
	DeleteVolume(ctx context.Context, id string) error
	AttachVolume(ctx context.Context, id, instanceID string) (Volume, error)
	DetachVolume(ctx context.Context, id string) (Volume, error)
	// PinVolume records first provisioning: set NodeID iff unset (compare-
	// and-swap). A pinned volume only schedules on its node; re-pinning a
	// volume pinned elsewhere reports a conflict instead of moving files
	// that do not move.
	PinVolume(ctx context.Context, id, nodeID string) (Volume, error)
	SetVolumeBootIndex(ctx context.Context, id string, index *int) (Volume, error)
}

type CreateVolumeRequest struct {
	Name                string   `json:"name"`
	Namespace           string   `json:"namespace"`
	Size                Quantity `json:"size"`
	Image               string   `json:"image"`
	Cdrom               bool     `json:"cdrom"`
	BootIndex           *int     `json:"boot_index"`
	DeleteOnTermination bool     `json:"delete_on_termination"`
}

type VolumeService interface {
	Create(ctx context.Context, req CreateVolumeRequest) (Volume, error)
	Get(ctx context.Context, id string) (Volume, error)
	List(ctx context.Context) ([]Volume, error)
	Delete(ctx context.Context, id string) error
}

// FlavorStore persists named flavors. Flavors are immutable after creation:
// resizing is a new flavor, never an update, so a VM's snapshot stays valid.
type FlavorStore interface {
	CreateFlavor(ctx context.Context, f Flavor) (Flavor, error)
	GetFlavor(ctx context.Context, name string) (Flavor, error)
	ListFlavors(ctx context.Context) ([]Flavor, error)
	DeleteFlavor(ctx context.Context, name string) error
}

type CreateFlavorRequest struct {
	Name   string   `json:"name"`
	VCPU   VCPU     `json:"vcpu"`
	Memory Quantity `json:"memory"`
}

type FlavorService interface {
	Create(ctx context.Context, req CreateFlavorRequest) (Flavor, error)
	Get(ctx context.Context, name string) (Flavor, error)
	List(ctx context.Context) ([]Flavor, error)
	Delete(ctx context.Context, name string) error
}

// UpdateInstanceRequest carries the mutable options a patch may change on a
// running VM. Today that is the flavor: a resize, applied by redefining the
// domain on the engine, which restarts the guest. Exactly one of FlavorRef
// (a stored flavor) or Flavor (inline) is set.
type UpdateInstanceRequest struct {
	FlavorRef string
	Flavor    *Flavor
}

type InstanceService interface {
	Create(ctx context.Context, req CreateInstanceRequest) (Instance, error)
	Get(ctx context.Context, id string) (Instance, error)
	List(ctx context.Context) ([]Instance, error)
	Delete(ctx context.Context, id string) error
	// Update applies mutable options (a resize). It is asynchronous like the
	// other lifecycle verbs: the api records the wish and the controller
	// redefines the domain.
	Update(ctx context.Context, id string, req UpdateInstanceRequest) error
	// Reboot restarts a running guest, keeping its definition and disks.
	Reboot(ctx context.Context, id string) error
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Migrate(ctx context.Context, id, targetNodeID string) error
	AttachVolume(ctx context.Context, id, volumeID string, bootIndex *int) error
	DetachVolume(ctx context.Context, id, volumeID string) error
	SetBootVolume(ctx context.Context, id, volumeID string) error
}

type PlacementRequest struct {
	InstanceID     string
	Flavor         Flavor
	Volumes        []Volume
	ImageRef       string
	NodeID         string
	ExcludeNodeIDs []string
	Generation     int
	Attempt        int
}

type Placement struct {
	InstanceID string
	NodeID     string
	// PinnedCPUs carries a dedicated claim's exclusive CPUs. The compute
	// service records them on the instance and hands them to the engine.
	PinnedCPUs []int
	Generation int
	Attempt    int
	Reason     string
	RunnerUp   string
}

type Scheduler interface {
	Schedule(ctx context.Context, req PlacementRequest) (Placement, error)
}

type RegisterNodeRequest struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
}

type NodeService interface {
	Register(ctx context.Context, req RegisterNodeRequest) (Node, error)
	RecordUsage(ctx context.Context, nodeID string, usage Resources) error
	Get(ctx context.Context, id string) (Node, error)
	List(ctx context.Context) ([]Node, error)
	Enable(ctx context.Context, id string) error
	Disable(ctx context.Context, id string) error
}

// ========== Request/Host Filter/Weigher ==========

type RequestFilter interface {
	Name() string
	FilterRequest(ctx context.Context, req PlacementRequest) error
}

type NodeFilter interface {
	Name() string
	FilterNodes(ctx context.Context, req PlacementRequest, nodes []Node) ([]Node, error)
}

type Weigher interface {
	Name() string
	WeightNode(ctx context.Context, req PlacementRequest, node Node) (int, error)
}
