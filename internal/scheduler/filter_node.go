package scheduler

import (
	"context"
	"fmt"

	"github.com/cinvat/arsan/internal/core"
	"github.com/samber/lo"
)

type StateFilter struct{}

func (StateFilter) Name() string { return "NodeState" }

func (StateFilter) FilterNodes(_ context.Context, _ core.PlacementRequest, nodes []core.Node) ([]core.Node, error) {
	out := nodes[:0:0]
	for _, n := range nodes {
		if n.State.Ready() && n.Enabled {
			out = append(out, n)
		}
	}
	return out, nil
}

type CapacityFilter struct{}

func (CapacityFilter) Name() string { return "Capacity" }

func (CapacityFilter) FilterNodes(_ context.Context, req core.PlacementRequest, nodes []core.Node) ([]core.Node, error) {
	want := req.Flavor.Resources()
	out := nodes[:0:0]
	for _, n := range nodes {
		if n.Available.Fits(want) {
			out = append(out, n)
		}
	}
	return out, nil
}

type ArchFilter struct{}

func (ArchFilter) Name() string { return "Arch" }

func (ArchFilter) FilterNodes(_ context.Context, req core.PlacementRequest, nodes []core.Node) ([]core.Node, error) {
	out := nodes[:0:0]
	for _, n := range nodes {
		if n.Arch == "" || n.Arch == "amd64" { // Simplified for now
			out = append(out, n)
		}
	}
	return out, nil
}

type GroupFilter struct{}

func (GroupFilter) Name() string { return "Group" }

func (GroupFilter) FilterNodes(_ context.Context, req core.PlacementRequest, nodes []core.Node) ([]core.Node, error) {
	// TODO: anti-affinity groups. Not yet implemented, so this is a pass-through.
	return nodes, nil
}

// ExcludeNodesFilter drops nodes the caller has ruled out.
//
// Re-placement puts the node that just failed in ExcludeNodeIDs. Without this
// the scheduler will happily return a VM to the machine that lost it, because
// that node still reads as UP until its heartbeat times out.
//
// This is a NodeFilter rather than a RequestFilter for a structural reason: a
// request filter sees only the request, and excluding a node requires the node
// list.
type ExcludeNodesFilter struct{}

func (ExcludeNodesFilter) Name() string { return "ExcludeNodes" }

func (ExcludeNodesFilter) FilterNodes(_ context.Context, req core.PlacementRequest, nodes []core.Node) ([]core.Node, error) {
	if len(req.ExcludeNodeIDs) == 0 {
		return nodes, nil
	}
	out := nodes[:0:0]
	for _, n := range nodes {
		if !lo.Contains(req.ExcludeNodeIDs, n.ID) {
			out = append(out, n)
		}
	}
	return out, nil
}

type DriverValidateFilter struct {
	DriverFor func(nodeID string) (core.Driver, error)
	Log       Logger
}

func (DriverValidateFilter) Name() string { return "DriverValidate" }

func (f DriverValidateFilter) FilterNodes(ctx context.Context, req core.PlacementRequest, nodes []core.Node) ([]core.Node, error) {
	out := make([]core.Node, 0, len(nodes))
	for _, n := range nodes {
		d, err := f.DriverFor(n.ID)
		if err != nil {
			// A driver we cannot even construct is an infrastructure fault, not
			// a node that failed a placement test. Dropping it silently made an
			// unreachable agent look exactly like a node that lacked capacity,
			// so an operator saw "no eligible nodes" with nothing to act on.
			if f.Log != nil {
				f.Log.Error("node excluded: cannot resolve driver", "node", n.ID, "err", err)
			}
			continue
		}
		if err := d.Validate(ctx, req.Flavor); err != nil {
			// This one is a genuine placement decision, so it stays quiet:
			// normal filtering is not worth a log line per node per request.
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

type FlavorFilter struct{}

func (FlavorFilter) Name() string { return "Flavor" }

func (FlavorFilter) FilterRequest(_ context.Context, req core.PlacementRequest) error {
	if req.Flavor.VCPU <= 0 || req.Flavor.RAM <= 0 || req.Flavor.Disk <= 0 {
		return fmt.Errorf("flavor must have positive vcpu, ram and disk: %w", core.ErrInvalidRequest)
	}
	return nil
}
