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
	// UpdateDisks redefines a stopped domain with a new disk set: attach
	// and detach on SHUTOFF, and boot-device switches, converge here.
	UpdateDisks(ctx context.Context, req CreateRequest) error
	// AttachDisk provisions one extra disk and plugs it in: live plus
	// config when the guest runs, redefine when it does not.
	AttachDisk(ctx context.Context, req CreateRequest, disk Disk) error
	// DetachDisk unplugs one extra disk and deletes its file. The record
	// already dropped it; the file delete always happens, so a shutoff
	// detach cannot strand storage.
	DetachDisk(ctx context.Context, req CreateRequest, disk Disk) error
	// AttachCdrom plugs the iso read-only; DetachCdrom removes it. Boot
	// order itself only changes at define time: attach, set boot to cdrom
	// (a stopped redefine), start.
	AttachCdrom(ctx context.Context, req CreateRequest) error
	DetachCdrom(ctx context.Context, req CreateRequest, image string) error
}

// DomainStatus is one observed guest: the id arsan defined it under and its
// steady guest state (RUNNING, SHUTOFF, SHUTDOWN, PAUSED or CRASHED).
type DomainStatus struct {
	InstanceID string
	State      InstanceState
}

type CreateRequest struct {
	InstanceID string
	NodeID     string
	Name       string
	Namespace  string
	Flavor     Flavor
	// Disks, CdromImage and BootDevice carry the VM's storage: extras indexed
	// from 1, an optional read-only iso, and which of disk/cdrom boots.
	Disks      []Disk
	CdromImage string
	BootDevice string
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
	ImageRef  string `json:"image_ref"`
	// FlavorRef names a stored flavor. It is the preferred way to size a VM;
	// the instance keeps a snapshot of the flavor's resources at create time,
	// so later flavor edits do not resize existing VMs.
	FlavorRef string `json:"flavor_ref"`
	// Flavor is the legacy inline sizing, kept for back-compat. When FlavorRef
	// is set it wins and Flavor is ignored.
	Flavor Flavor `json:"flavor"`
	// Disks are extra data volumes (root is the image overlay, always index
	// 0). CdromImage attaches an iso read-only. BootDevice is BootDisk or
	// BootCDROM and selects what boots; a rescue is attach-cdrom plus boot
	// cdrom, then start.
	Disks      []CreateDiskRequest `json:"disks"`
	CdromImage string              `json:"cdrom_image"`
	BootDevice string              `json:"boot_device"`
}

// CreateDiskRequest sizes one extra data disk. Size is required ("10GB");
// Image optionally names another base to overlay instead of provisioning
// blank. Indexes are assigned at create, starting at 1.
type CreateDiskRequest struct {
	Name  string   `json:"name"`
	Size  Quantity `json:"size"`
	Image string   `json:"image"`
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

type InstanceService interface {
	Create(ctx context.Context, req CreateInstanceRequest) (Instance, error)
	Get(ctx context.Context, id string) (Instance, error)
	List(ctx context.Context) ([]Instance, error)
	Delete(ctx context.Context, id string) error
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Migrate(ctx context.Context, id, targetNodeID string) error
	AttachDisk(ctx context.Context, id string, disk CreateDiskRequest) error
	DetachDisk(ctx context.Context, id, name string) error
	AttachCdrom(ctx context.Context, id, image string) error
	DetachCdrom(ctx context.Context, id string) error
	SetBootDevice(ctx context.Context, id, device string) error
}

type PlacementRequest struct {
	InstanceID     string
	Flavor         Flavor
	Disks          []Disk
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
