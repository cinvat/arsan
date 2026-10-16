package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/agent"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
)

const defaultTimeout = 30 * time.Second

// Driver implements core.Driver by asking the agent over NATS. It holds no
// connection of its own: the node id is the address (agent.<node-id>), so
// there is nothing to dial, cache, or rediscover when an agent moves.
type Driver struct {
	commands bus.Bus
	nodeID   string
	name     string
}

func New(nodeID string, commands bus.Bus) *Driver {
	return &Driver{commands: commands, nodeID: nodeID, name: "remote"}
}

func (d *Driver) Name() string { return d.name }

// call sends one engine op and decodes the answer. A timeout means the agent
// did not answer, which the caller records as NOTREADY or a failed operation.
func (d *Driver) call(ctx context.Context, req agent.Request) (agent.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	raw, err := json.Marshal(req)
	if err != nil {
		return agent.Response{}, err
	}
	data, err := d.commands.Request(ctx, agent.Subject(d.nodeID), raw)
	if err != nil {
		return agent.Response{}, err
	}
	var resp agent.Response
	if err := json.Unmarshal(data, &resp); err != nil {
		return agent.Response{}, fmt.Errorf("remote: bad reply: %w", err)
	}
	if !resp.OK {
		return agent.Response{}, fmt.Errorf("%s: %s", req.Op, resp.Error)
	}
	return resp, nil
}

func (d *Driver) Validate(ctx context.Context, f core.Flavor) error {
	_, err := d.call(ctx, agent.Request{Op: agent.OpValidate, Flavor: f})
	return err
}

func (d *Driver) Create(ctx context.Context, req core.CreateRequest) (core.InstanceInfo, error) {
	resp, err := d.call(ctx, agent.Request{
		Op: agent.OpCreate, InstanceID: req.InstanceID, NodeID: req.NodeID,
		Name: req.Name, Flavor: req.Flavor, PinnedCPUs: req.PinnedCPUs, ImageRef: req.ImageRef,
	})
	if err != nil {
		return core.InstanceInfo{}, err
	}
	return resp.Info, nil
}

func (d *Driver) Start(ctx context.Context, id string) error {
	_, err := d.call(ctx, agent.Request{Op: agent.OpStart, InstanceID: id})
	return err
}

func (d *Driver) Stop(ctx context.Context, id string) error {
	_, err := d.call(ctx, agent.Request{Op: agent.OpStop, InstanceID: id})
	return err
}

func (d *Driver) Delete(ctx context.Context, id string) error {
	_, err := d.call(ctx, agent.Request{Op: agent.OpDelete, InstanceID: id})
	return err
}

func (d *Driver) Reboot(ctx context.Context, id string) error {
	_, err := d.call(ctx, agent.Request{Op: agent.OpReboot, InstanceID: id})
	return err
}

func (d *Driver) Migrate(ctx context.Context, id, targetNodeID string) error {
	_, err := d.call(ctx, agent.Request{
		Op: agent.OpMigrate, InstanceID: id, TargetNodeID: targetNodeID,
	})
	return err
}

func (d *Driver) Usage(ctx context.Context) (core.Resources, error) {
	resp, err := d.call(ctx, agent.Request{Op: agent.OpUsage})
	if err != nil {
		return core.Resources{}, err
	}
	return resp.Usage, nil
}

func (d *Driver) Domains(ctx context.Context) ([]core.DomainStatus, error) {
	resp, err := d.call(ctx, agent.Request{Op: agent.OpDomains})
	if err != nil {
		return nil, err
	}
	return resp.Domains, nil
}

// Info asks the agent what it runs and what its host holds. The controller
// calls this after the probe succeeds and records the answer on the node, so
// the agent never has to describe itself at bootstrap.
func (d *Driver) Info(ctx context.Context) (engine string, total core.Resources, err error) {
	resp, err := d.call(ctx, agent.Request{Op: agent.OpInfo})
	if err != nil {
		return "", core.Resources{}, err
	}
	return resp.Engine, resp.Total, nil
}
