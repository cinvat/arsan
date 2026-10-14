package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cinvat/arsan/internal/core"
	"github.com/samber/lo"
)

const DefaultMaxAttempts = 5

var MaxPlacementAttempts = DefaultMaxAttempts

type DriverFor func(nodeID string) (core.Driver, error)

type Config struct {
	Instances   core.InstanceStore
	Nodes       core.NodeStore
	DriverFor   DriverFor
	MaxAttempts int
	Log         Logger
}

type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type Scheduler struct {
	instances   core.InstanceStore
	nodes       core.NodeStore
	driverFor   DriverFor
	maxAttempts int
	log         Logger

	requestFilters []core.RequestFilter
	nodeFilters    []core.NodeFilter
	weighers       []core.Weigher
}

func New(cfg Config) *Scheduler {
	max := cfg.MaxAttempts
	if max < 1 {
		max = DefaultMaxAttempts
	}
	s := &Scheduler{
		instances:   cfg.Instances,
		nodes:       cfg.Nodes,
		driverFor:   cfg.DriverFor,
		maxAttempts: max,
		log:         cfg.Log,
	}
	s.requestFilters = []core.RequestFilter{
		FlavorFilter{},
	}
	s.nodeFilters = []core.NodeFilter{
		ExcludeNodesFilter{}, // before the rest: a ruled-out node is never reconsidered
		StateFilter{},        // rejects the most nodes for the least work
		CapacityFilter{},
		ArchFilter{},
		GroupFilter{},
		DriverValidateFilter{DriverFor: cfg.DriverFor, Log: cfg.Log},
	}
	s.weighers = []core.Weigher{
		FreeRAMWeigher{},
		FreeCPUWeigher{},
		FewestInstancesWeigher{},
	}
	return s
}

func (s *Scheduler) Schedule(ctx context.Context, req core.PlacementRequest) (core.Placement, error) {
	if req.InstanceID == "" {
		return core.Placement{}, fmt.Errorf("schedule: %w", core.ErrInvalidRequest)
	}

	var lastConflict string
	for attempt := 1; attempt <= s.maxAttempts; attempt++ {
		req.Attempt = attempt

		if err := s.runRequestFilters(ctx, req); err != nil {
			return core.Placement{}, err
		}

		nodes, err := s.nodes.ListNodes(ctx)
		if err != nil {
			return core.Placement{}, fmt.Errorf("list nodes: %w", err)
		}

		eligible, err := s.runNodeFilters(ctx, req, nodes)
		if err != nil {
			return core.Placement{}, fmt.Errorf("filter nodes: %w", err)
		}
		if len(eligible) == 0 {
			return core.Placement{}, fmt.Errorf(
				"attempt %d: %w (rejected by: %s)", attempt,
				core.ErrNoValidNode, strings.Join(s.rejectReasons(req, nodes), ", "))
		}

		weighed, err := s.weigh(ctx, req, eligible)
		if err != nil {
			return core.Placement{}, fmt.Errorf("weigh: %w", err)
		}
		winner, runnerUp := best(weighed)

		want := req.Flavor.Resources()
		if err := s.nodes.ReserveNodeResources(ctx, winner.NodeID, want); err != nil {
			if errors.Is(err, core.ErrCapacityConflict) {
				lastConflict = fmt.Sprintf("attempt %d: node %s no longer fits", attempt, winner.NodeID)
				s.log.Warn("claim lost, retrying", "attempt", attempt, "node", winner.NodeID)
				continue
			}
			return core.Placement{}, fmt.Errorf("claim %s: %w", winner.NodeID, err)
		}
		if err := s.nodes.AttachInstance(ctx, winner.NodeID, req.InstanceID); err != nil {
			_ = s.nodes.ReleaseNodeResources(ctx, winner.NodeID, want)
			return core.Placement{}, fmt.Errorf("attach instance: %w", err)
		}

		return core.Placement{
			InstanceID: req.InstanceID,
			NodeID:     winner.NodeID,
			Generation: req.Generation,
			Attempt:    attempt,
			Reason:     explain(winner, runnerUp),
			RunnerUp:   runnerUp,
		}, nil
	}

	return core.Placement{}, fmt.Errorf("after %d attempts: %w (%s)",
		s.maxAttempts, core.ErrNoValidNode, lastConflict)
}

func (s *Scheduler) runRequestFilters(ctx context.Context, req core.PlacementRequest) error {
	for _, f := range s.requestFilters {
		if err := f.FilterRequest(ctx, req); err != nil {
			return fmt.Errorf("request filter %s: %w", f.Name(), err)
		}
	}
	return nil
}

func (s *Scheduler) runNodeFilters(ctx context.Context, req core.PlacementRequest, nodes []core.Node) ([]core.Node, error) {
	cur := nodes
	for _, f := range s.nodeFilters {
		next, err := f.FilterNodes(ctx, req, cur)
		if err != nil {
			return nil, fmt.Errorf("node filter %s: %w", f.Name(), err)
		}
		cur = next
	}
	return cur, nil
}

func (s *Scheduler) weigh(ctx context.Context, req core.PlacementRequest, nodes []core.Node) ([]WeighedHost, error) {
	out := make([]WeighedHost, 0, len(nodes))
	for _, n := range nodes {
		wh := WeighedHost{NodeID: n.ID, Detail: make(map[string]int, len(s.weighers))}
		for _, w := range s.weighers {
			v, err := w.WeightNode(ctx, req, n)
			if err != nil {
				s.log.Warn("weigher skipped", "weigher", w.Name(), "node", n.ID, "err", err)
				continue
			}
			if v < 0 {
				continue
			}
			wh.Score += v
			wh.Detail[w.Name()] = v
		}
		out = append(out, wh)
	}
	return out, nil
}

type WeighedHost struct {
	NodeID string
	Score  int
	Detail map[string]int
}

func best(nodes []WeighedHost) (WeighedHost, string) {
	sorted := lo.Map(nodes, func(n WeighedHost, _ int) WeighedHost { return n })
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Score != sorted[j].Score {
			return sorted[i].Score > sorted[j].Score
		}
		return sorted[i].NodeID < sorted[j].NodeID
	})
	runnerUp := ""
	if len(sorted) > 1 {
		runnerUp = fmt.Sprintf("%s (score %d)", sorted[1].NodeID, sorted[1].Score)
	}
	return sorted[0], runnerUp
}

func (s *Scheduler) rejectReasons(req core.PlacementRequest, nodes []core.Node) []string {
	if len(nodes) == 0 {
		return []string{"no nodes registered"}
	}
	var out []string
	for _, f := range s.nodeFilters {
		kept, err := f.FilterNodes(context.Background(), req, nodes)
		if err != nil {
			continue
		}
		if len(kept) < len(nodes) {
			out = append(out, fmt.Sprintf("%s dropped %d", f.Name(), len(nodes)-len(kept)))
		}
	}
	return out
}

func explain(w WeighedHost, runnerUp string) string {
	keys := lo.Keys(w.Detail)
	sort.Strings(keys)
	parts := lo.Map(keys, func(k string, _ int) string {
		return fmt.Sprintf("%s=%d", k, w.Detail[k])
	})
	s := fmt.Sprintf("%s won with %s (total %d)", w.NodeID, strings.Join(parts, " "), w.Score)
	if runnerUp != "" {
		s += "; next best " + runnerUp
	}
	return s
}
