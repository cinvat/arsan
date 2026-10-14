# Step 1: Core Types & Interfaces

## Goal
Define the domain types and interfaces that the entire system depends on. `internal/core` has **zero dependencies** — everything else imports from it.

---

## Files to Create

```
internal/core/
├── types.go      # Domain types
├── errors.go     # Sentinel errors
└── ports.go      # Interfaces (Driver, Store, Services)
```

---

## 1.1 Domain Types — `internal/core/types.go`

```go
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

type Resources struct {
	VCPU int
	RAM  int
	Disk int
}

func (r Resources) Fits(other Resources) bool {
	return r.VCPU >= other.VCPU && r.RAM >= other.RAM && r.Disk >= other.Disk
}

func (r Resources) Sub(other Resources) Resources {
	return Resources{VCPU: r.VCPU - other.VCPU, RAM: r.RAM - other.RAM, Disk: r.Disk - other.Disk}
}

type Flavor struct {
	Name string
	VCPU int
	RAM  int
	Disk int
}

func (f Flavor) Resources() Resources {
	return Resources{VCPU: f.VCPU, RAM: f.RAM, Disk: f.Disk}
}

type Instance struct {
	ID            string
	Name          string
	Flavor        Flavor
	ImageRef      string
	State         InstanceState
	NodeID        string
	Generation    int
	FailureReason string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type NodeState string

// Two states only. "Registered", "up", "down", "disabled" and "unregistered"
// all collapse into these two, because each extra state multiplies the
// transitions that have to be handled correctly and each one is a chance to
// leave a node schedulable when it should not be.
//
// The cost is that NOTREADY alone cannot say *why*, which is what
// Node.Enabled and NotReadyReason are for.
const (
	NodeReady    NodeState = "READY"
	NodeNotReady NodeState = "NOTREADY"
)

type Node struct {
	ID            string
	Name          string
	Arch          string
	State         NodeState
	Enabled       bool
	Driver        string
	Address       string
	Total         Resources
	Available     Resources
	Usage         Resources
	Instances     []string
	LastHeartbeat time.Time

	// NotReadyReason explains a NOTREADY state. With only two states, one word
	// has to cover every fault — an agent that will not answer, a broken
	// engine, a host an operator drained — and only the reason tells them
	// which. Cleared on recovery; left alone on a disabled node.
	NotReadyReason string
}
```

---

## 1.2 Sentinel Errors — `internal/core/errors.go`

```go
package core

import "errors"

var (
	ErrInstanceNotFound   = errors.New("instance not found")
	ErrNodeNotFound       = errors.New("node not found")
	ErrInvalidTransition  = errors.New("invalid state transition")
	ErrNoValidNode        = errors.New("no valid host")
	ErrRequestRejected    = errors.New("request rejected by scheduler")
	ErrCapacityConflict   = errors.New("capacity conflict")
	ErrInvalidRequest     = errors.New("invalid request")
	ErrUnsupportedPlatform = errors.New("hypervisor not available on this platform")
)
```

**Rule**: All errors must be `errors.Is`-comparable. Wrap with `%w` to preserve the sentinel through layers.

---

## 1.3 Interfaces (Ports) — `internal/core/ports.go`

```go
package core

import "context"

// ========== Driver ==========

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
}

type CreateRequest struct {
	InstanceID string
	NodeID     string
	Name       string
	Flavor     Flavor
	ImageRef   string
}

type InstanceInfo struct {
	ID      string
	NodeID  string
	Driver  string
	Started bool
}

// ========== Instance Store ==========

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

// ========== Node Store ==========

type NodeStore interface {
	Register(ctx context.Context, n Node) (Node, error)
	GetNode(ctx context.Context, id string) (Node, error)
	ListNodes(ctx context.Context) ([]Node, error)
	SetState(ctx context.Context, id string, s NodeState) (Node, error)
	RecordUsage(ctx context.Context, id string, usage Resources, at time.Time) (Node, error)

	// Atomic reserve — all-or-nothing. Returns ErrCapacityConflict without mutating if any dimension insufficient.
	ReserveNodeResources(ctx context.Context, nodeID string, r Resources) error
	ReleaseNodeResources(ctx context.Context, nodeID string, r Resources) error

	AttachInstance(ctx context.Context, nodeID, instanceID string) error
	DetachInstance(ctx context.Context, nodeID, instanceID string) error
}

// ========== Service Interfaces (what callers depend on) ==========

type CreateInstanceRequest struct {
	Name     string `json:"name"`
	ImageRef string `json:"image_ref"`
	Flavor   Flavor `json:"flavor"`
}

type InstanceService interface {
	Create(ctx context.Context, req CreateInstanceRequest) (Instance, error)
	Get(ctx context.Context, id string) (Instance, error)
	List(ctx context.Context) ([]Instance, error)
	Delete(ctx context.Context, id string) error
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Migrate(ctx context.Context, id, targetNodeID string) error
}

type PlacementRequest struct {
	InstanceID     string
	Flavor         Flavor
	ImageRef       string
	NodeID         string
	ExcludeNodeIDs []string
	Generation     int
	Attempt        int
}

type Placement struct {
	InstanceID string
	NodeID     string
	Generation int
	Attempt    int
	Reason     string
	RunnerUp   string
}

type Scheduler interface {
	Schedule(ctx context.Context, req PlacementRequest) (Placement, error)
}

type RegisterNodeRequest struct {
	Name    string    `json:"name"`
	Arch    string    `json:"arch"`
	Driver  string    `json:"driver"`
	Address string    `json:"address"`
	Total   Resources `json:"total"`
}

type NodeService interface {
	Register(ctx context.Context, req RegisterNodeRequest) (Node, error)
	RecordUsage(ctx context.Context, nodeID string, usage Resources) error
	Get(ctx context.Context, id string) (Node, error)
	List(ctx context.Context) ([]Node, error)
	Enable(ctx context.Context, id string) error
	Disable(ctx context.Context, id string) error
}
```

---

## Key Principles

| Principle | Application |
|-----------|-------------|
| **Core is dependency-free** | No imports from `internal/*` |
| **Interfaces at consumer** | `Driver` implemented by drivers, used by `compute` |
| **No driver types leak** | Translate at boundaries (e.g., `CreateRequest` vs driver-specific) |
| **State transitions are CAS** | `Transition(ctx, id, from, to)` validates `from` |
| **Errors are sentinels** | `errors.Is(err, core.ErrNoValidNode)` works across layers |

---

## Verify

```bash
go build ./internal/core/...
```

No output = success.

---

## Next

Step 2: Fake Driver Implementation