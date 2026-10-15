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

	AttachInstance(ctx context.Context, nodeID, instanceID string) error
	DetachInstance(ctx context.Context, nodeID, instanceID string) error
}

// ========== Service Interfaces ==========

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
