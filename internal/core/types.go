package core

import "time"

type InstanceState string

const (
	StatePending    InstanceState = "PENDING"
	StateScheduling InstanceState = "SCHEDULING"
	StateBuilding   InstanceState = "BUILDING"
	StateActive     InstanceState = "ACTIVE"
	StateShutoff    InstanceState = "SHUTOFF"
	StateError      InstanceState = "ERROR"
)

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

// Flavor sizes a VM: vCPU topology, RAM in MiB, Disk in MiB.
type Flavor struct {
	Name string
	VCPU VCPU
	RAM  int
	Disk int
}

func (f Flavor) Resources() Resources {
	return Resources{CPU: f.VCPU.Count(), RAM: f.RAM, Disk: f.Disk}
}

type Instance struct {
	ID         string
	Name       string
	Flavor     Flavor
	ImageRef   string
	State      InstanceState
	NodeID     string
	Generation int
	// PinnedCPUs holds the exclusive host CPUs for a dedicated flavor,
	// claimed at placement and freed on teardown. Empty for shared flavors.
	PinnedCPUs    []int
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
	Driver    string
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
