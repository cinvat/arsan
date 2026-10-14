package scheduler

import (
	"context"

	"github.com/cinvat/arsan/internal/core"
)

type FreeRAMWeigher struct{}

func (FreeRAMWeigher) Name() string { return "FreeRAM" }

func (FreeRAMWeigher) WeightNode(_ context.Context, _ core.PlacementRequest, n core.Node) (int, error) {
	return n.Available.RAM, nil
}

type FreeCPUWeigher struct{}

func (FreeCPUWeigher) Name() string { return "FreeCPU" }

func (FreeCPUWeigher) WeightNode(_ context.Context, _ core.PlacementRequest, n core.Node) (int, error) {
	return n.Available.VCPU * 1024, nil
}

type FewestInstancesWeigher struct{}

func (FewestInstancesWeigher) Name() string { return "FewestInstances" }

func (FewestInstancesWeigher) WeightNode(_ context.Context, _ core.PlacementRequest, n core.Node) (int, error) {
	return 100 - len(n.Instances), nil
}
