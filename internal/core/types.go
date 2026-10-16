package core

import "time"

// InstanceState is the lifecycle of a VM: arsan's control-plane states for
// work it has not finished, libvirt's guest states for what the hypervisor
// reports.
//
// Control-plane states (arsan decides): PENDING waits for placement and
// carries PendingReason while it waits, like a pod with a scheduling gate;
// SCHEDULING and BUILDING are in-flight; ERROR is a control-plane failure
// with FailureReason and is terminal until an operator acts.
//
// Guest states (the hypervisor decides, the probe observes): RUNNING,
// SHUTOFF, SHUTDOWN (guest shutting down, transient), PAUSED (observed only —
// there are no pause/resume ops yet) and CRASHED. BLOCKED folds into RUNNING
// and PMSUSPENDED into PAUSED; NOSTATE is never stored.
type InstanceState string

const (
	StatePending    InstanceState = "PENDING"
	StateScheduling InstanceState = "SCHEDULING"
	StateBuilding   InstanceState = "BUILDING"
	StateRunning    InstanceState = "RUNNING"
	StateShutoff    InstanceState = "SHUTOFF"
	StateShutdown   InstanceState = "SHUTDOWN"
	StatePaused     InstanceState = "PAUSED"
	StateCrashed    InstanceState = "CRASHED"
	StateError      InstanceState = "ERROR"
)

// Steady reports whether the state is a settled guest state. Only steady
// states converge both ways during reconcile; transient and terminal states
// are owned by whoever put the record there, and the probe leaves them alone.
func (s InstanceState) Steady() bool {
	switch s {
	case StateRunning, StateShutoff, StateShutdown, StatePaused, StateCrashed:
		return true
	default:
		return false
	}
}

// Resources describe a node: CPU count, RAM in MiB, Disk in MiB.
// Nodes have CPUs; virtual machines have VCPUs (core.Flavor). The scheduler
// matches flavor VCPU against node CPU one-for-one.
//
// MiB (not GiB) because tiny guests exist: a RouterOS VM runs in 32MiB,
// which GiB granularity cannot express. Sizes enter the system as "32MB"/"2GB"
// strings (core.Quantity) and are stored here canonicalized to MiB.
type Resources struct {
	CPU  int
	RAM  int
	Disk int
}

// AddCapped returns capacity with ret released, never exceeding total.
// Releasing more than was reserved (a retried delete, a stale report) must
// not invent capacity the host never had.
func (r Resources) AddCapped(ret, total Resources) Resources {
	out := r.Add(ret)
	if out.CPU > total.CPU {
		out.CPU = total.CPU
	}
	if out.RAM > total.RAM {
		out.RAM = total.RAM
	}
	if out.Disk > total.Disk {
		out.Disk = total.Disk
	}
	return out
}

func (r Resources) Fits(other Resources) bool {
	return r.CPU >= other.CPU && r.RAM >= other.RAM && r.Disk >= other.Disk
}

func (r Resources) Sub(other Resources) Resources {
	return Resources{CPU: r.CPU - other.CPU, RAM: r.RAM - other.RAM, Disk: r.Disk - other.Disk}
}

func (r Resources) Add(other Resources) Resources {
	return Resources{CPU: r.CPU + other.CPU, RAM: r.RAM + other.RAM, Disk: r.Disk + other.Disk}
}

// Flavor sizes a VM's compute: vCPU topology and RAM in MiB. Storage is not
// a flavor concern: VMs take volumes, Nova-style, each an individually
// named, sized and (de)attachable record.
type Flavor struct {
	Name string
	VCPU VCPU
	RAM  int
}

// Volume states, Cinder-style: free to claim, or held by one instance.
type VolumeState string

const (
	VolumeAvailable VolumeState = "available"
	VolumeInUse     VolumeState = "in-use"
)

// Volume is a first-class named disk, independent of any VM: create it from
// a base image, blank, or an iso (cdrom), attach it to one VM at a time by
// id, detach it, delete it. The hypervisor file derives from its identity
// and is deleted with it exactly when DeleteOnTermination says so.
type Volume struct {
	ID        string
	Name      string
	Namespace string
	// SizeMiB sizes the qcow2. Zero on an image source means "base virtual
	// size", resolved by the engine at provisioning.
	SizeMiB int
	// Image names the base to overlay, or an iso for a cdrom. Empty
	// provisions a blank disk.
	Image string
	// Cdrom marks a read-only iso attachment. Isos are shared and never
	// deleted; DeleteOnTermination is forced false for them.
	Cdrom bool
	// BootIndex orders boot: 0 boots first, then 1, and so on; nil means
	// not bootable. Unset entries boot after indexed ones, in attach order.
	BootIndex *int
	// DeleteOnTermination removes the file with the record on VM delete.
	// Detaching never deletes: it only frees the volume back to available.
	DeleteOnTermination bool
	State               VolumeState
	// AttachedTo names the holder, empty when available. One holder at a
	// time: the second claim loses the compare-and-swap.
	AttachedTo string
	// NodeID pins the volume to the node that first provisioned it. Local
	// files do not migrate, so a pinned volume only schedules there — the
	// Cinder pool-affinity equivalent for node-local storage. Empty until
	// the first claim pins it.
	NodeID    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// VolumeAttachment is the VM side of the link: which volume, as which
// device, in which boot position.
type VolumeAttachment struct {
	VolumeID string
	Device   string
	// BootIndex mirrors the volume's at attach time, so the domain renders
	// without re-reading the volume row on every op.
	BootIndex *int
}

func (f Flavor) Resources() Resources {
	return Resources{CPU: f.VCPU.Count(), RAM: f.RAM}
}

// VolumeResources sums attached volumes for placement: every byte a VM can
// write is reserved up front, thin or not.
func VolumeResources(volumes []Volume) Resources {
	var out Resources
	for _, v := range volumes {
		out.Disk += v.SizeMiB
	}
	return out
}

type Instance struct {
	ID string
	// Name and Namespace identify the VM like a pod: the hypervisor domain
	// is named "<namespace>-<name>", never the internal id.
	Name      string
	Namespace string
	Flavor    Flavor
	// Volumes lists attached volume ids with their device and boot order.
	// The record of sizes and images lives on the volume rows, not here.
	Volumes    []VolumeAttachment
	ImageRef   string
	State      InstanceState
	NodeID     string
	Generation int
	// PinnedCPUs holds the exclusive host CPUs for a dedicated flavor,
	// claimed at placement and freed on teardown. Empty for shared flavors.
	PinnedCPUs []int
	// PendingReason says why a PENDING instance is still waiting (no valid
	// node, capacity lost, …), cleared when placement succeeds. FailureReason
	// is the ERROR counterpart: control-plane failures live there, waiting
	// lives here, so a pending VM never looks broken and a broken one never
	// looks merely queued.
	PendingReason string
	FailureReason string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NodeState is a node's readiness, modelled on a Kubernetes node's Ready
// condition: a node is either usable or it is not.
//
// The difference from a Kubernetes node object is that arsan tracks no other
// conditions. Memory pressure, disk pressure and PID pressure are all reasons
// a host cannot accept a VM, and all of them collapse into NOTREADY. Two states
// is deliberate: every additional state is another one the scheduler's filter
// has to reason about, and nothing here can act on the difference.
type NodeState string

const (
	// NodeReady means the node answered a health check and can host VMs.
	NodeReady NodeState = "READY"
	// NodeNotReady means it has not answered, or was taken out of service. A
	// newly registered node is NOTREADY until it answers.
	NodeNotReady NodeState = "NOTREADY"
)

// Ready reports whether the node may host VMs.
func (n NodeState) Ready() bool { return n == NodeReady }

type Node struct {
	ID      string
	Name    string
	Arch    string
	State   NodeState
	Enabled bool
	// Driver and Total are filled by the controller's probe, never by
	// bootstrap: the agent joins knowing nothing but the token.
	Driver string
	// IP is the agent's primary address as reported at bootstrap, for an
	// operator reading the API. Routing never uses it: the bus address is
	// agent.<node-id>, so a stale or NAT'd IP cannot misdirect commands.
	IP        string
	Total     Resources
	Available Resources
	Usage     Resources
	Instances []string
	// PinnedCPUs are the host CPUs exclusively held by dedicated flavors.
	// Available.CPU already excludes them; this is the record of which.
	PinnedCPUs    []int
	LastHeartbeat time.Time
	// NotReadyReason explains a NOTREADY state, for an operator reading the
	// API. It is cleared when the node comes back.
	//
	// With only two states, "not ready" is one word that has to cover every
	// cause — an agent that will not answer, an engine that is broken, a host
	// an operator drained — and only the reason tells them which.
	NotReadyReason string
}
