package remote

import (
	"context"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/agent"
	"github.com/cinvat/arsan/internal/core"
)

const defaultTimeout = 30 * time.Second

type Params struct {
	NodeID string
	Addr   string
}

type Driver struct {
	client agent.ComputeNodeAgentClient
	nodeID string
	name   string
}

func New(nodeID, addr string) (*Driver, error) {
	client, err := agent.NewClient(addr)
	if err != nil {
		return nil, fmt.Errorf("dial agent %s: %w", addr, err)
	}
	return &Driver{client: client, nodeID: nodeID, name: "remote"}, nil
}

func (d *Driver) Name() string { return d.name }

func (d *Driver) Validate(ctx context.Context, f core.Flavor) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Validate(ctx, &agent.ValidateRequest{Flavor: agent.FromCoreFlavor(f)})
	if err != nil {
		return err
	}
	if !resp.CanRun {
		return fmt.Errorf("validate: %s", resp.Message)
	}
	return nil
}

func (d *Driver) Create(ctx context.Context, req core.CreateRequest) (core.InstanceInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Create(ctx, &agent.CreateRequest{
		InstanceID: req.InstanceID,
		NodeID:     req.NodeID,
		Name:       req.Name,
		Flavor:     agent.FromCoreFlavor(req.Flavor),
		ImageRef:   req.ImageRef,
	})
	if err != nil {
		return core.InstanceInfo{}, err
	}
	if !resp.Success {
		return core.InstanceInfo{}, fmt.Errorf("create: %s", resp.Message)
	}
	return core.InstanceInfo{
		ID:      resp.Info.ID,
		NodeID:  resp.Info.NodeID,
		Driver:  resp.Info.Driver,
		Started: resp.Info.Started,
	}, nil
}

func (d *Driver) Start(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Start(ctx, &agent.StartRequest{InstanceID: id})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("start: %s", resp.Message)
	}
	return nil
}

func (d *Driver) Stop(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Stop(ctx, &agent.StopRequest{InstanceID: id})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("stop: %s", resp.Message)
	}
	return nil
}

func (d *Driver) Delete(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Delete(ctx, &agent.DeleteRequest{InstanceID: id})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("delete: %s", resp.Message)
	}
	return nil
}

func (d *Driver) Reboot(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Reboot(ctx, &agent.RebootRequest{InstanceID: id})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("reboot: %s", resp.Message)
	}
	return nil
}

func (d *Driver) Migrate(ctx context.Context, id, targetNodeID string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Migrate(ctx, &agent.MigrateRequest{
		InstanceID:   id,
		TargetNodeID: targetNodeID,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("migrate: %s", resp.Message)
	}
	return nil
}

func (d *Driver) Usage(ctx context.Context) (core.Resources, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Usage(ctx, &agent.UsageRequest{})
	if err != nil {
		return core.Resources{}, err
	}
	if !resp.Success {
		return core.Resources{}, fmt.Errorf("usage: %s", resp.Message)
	}
	return agent.ToCoreResources(resp.Usage), nil
}

func (d *Driver) Close() error {
	return d.client.Close()
}
