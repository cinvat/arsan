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
// a flavor concern: a VM takes zero or more disks plus an optional cdrom,
// each an individually named, sized and (de)attachable volume.
type Flavor struct {
	Name string
	VCPU VCPU
	RAM  int
}

// Disk is one extra data disk on a VM, snapshotted at attach time. The root
// disk is implicit — a thin overlay of the VM's image, always index 0 — so
// extras start at 1. Index is assigned at attach and never reused, which is
// what keeps detached gaps from renaming the survivors' files.
type Disk struct {
	// Name is operator-chosen ("data", "logs"), DNS_LABEL like everything
	// else a human types. Index is the stable file number.
	Name  string
	Index int
	// SizeMiB sizes a blank qcow2. With Image set it caps an overlay of that
	// base instead; zero means "base virtual size", resolved by the engine.
	SizeMiB int
	// Image names another base to overlay. Empty provisions a blank disk.
	Image string
}

// Boot devices, selected per VM. Rescue is attach-cdrom plus boot cdrom,
// then start.
const (
	BootDisk  = "disk"
	BootCDROM = "cdrom"
)

func (f Flavor) Resources() Resources {
	return Resources{CPU: f.VCPU.Count(), RAM: f.RAM}
}

// DiskResources sums extra disks for placement. The root overlay is thin —
// it shares its base's clusters until written — so it reserves nothing and
// only declared extras count against node disk.
func DiskResources(disks []Disk) Resources {
	var out Resources
	for _, d := range disks {
		out.Disk += d.SizeMiB
	}
	return out
}

type Instance struct {
	ID string
	// Name and Namespace identify the VM like a pod: the hypervisor domain
	// is named "<namespace>-<name>", never the internal id.
	Name       string
	Namespace  string
	Flavor     Flavor
	// Disks are the extra data volumes, snapshotted with their attach-time
	// size and backing. The root overlay (image) and cdrom ride alongside.
	Disks      []Disk
	CdromImage string
	BootDevice string
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
