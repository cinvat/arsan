# Step 9: Remote Driver

## Goal
Implement the `remote` driver that runs on the controller and forwards `core.Driver` calls to the agent via gRPC.

---

## File: `internal/driver/remote/driver.go`

```go
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
	if err != nil { return nil, fmt.Errorf("dial agent %s: %w", addr, err) }
	return &Driver{client: client, nodeID: nodeID, name: "remote"}, nil
}

func (d *Driver) Name() string { return d.name }

func (d *Driver) Validate(ctx context.Context, f core.Flavor) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Validate(ctx, &agent.ValidateRequest{Flavor: agent.FromCoreFlavor(f)})
	if err != nil { return err }
	if !resp.CanRun { return fmt.Errorf("validate: %s", resp.Message) }
	return nil
}

func (d *Driver) Create(ctx context.Context, req core.CreateRequest) (core.InstanceInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Create(ctx, &agent.CreateRequest{
		InstanceID: req.InstanceID, NodeID: req.NodeID, Name: req.Name,
		Flavor: agent.FromCoreFlavor(req.Flavor), ImageRef: req.ImageRef,
	})
	if err != nil { return core.InstanceInfo{}, err }
	if !resp.Success { return core.InstanceInfo{}, fmt.Errorf("create: %s", resp.Message) }
	return core.InstanceInfo{ID: resp.Info.ID, NodeID: resp.Info.NodeID, Driver: resp.Info.Driver, Started: resp.Info.Started}, nil
}

func (d *Driver) Start(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Start(ctx, &agent.StartRequest{InstanceID: id})
	if err != nil { return err }
	if !resp.Success { return fmt.Errorf("start: %s", resp.Message) }
	return nil
}

func (d *Driver) Stop(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Stop(ctx, &agent.StopRequest{InstanceID: id})
	if err != nil { return err }
	if !resp.Success { return fmt.Errorf("stop: %s", resp.Message) }
	return nil
}

func (d *Driver) Delete(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Delete(ctx, &agent.DeleteRequest{InstanceID: id})
	if err != nil { return err }
	if !resp.Success { return fmt.Errorf("delete: %s", resp.Message) }
	return nil
}

func (d *Driver) Reboot(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Reboot(ctx, &agent.RebootRequest{InstanceID: id})
	if err != nil { return err }
	if !resp.Success { return fmt.Errorf("reboot: %s", resp.Message) }
	return nil
}

func (d *Driver) Migrate(ctx context.Context, id, targetNodeID string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Migrate(ctx, &agent.MigrateRequest{InstanceID: id, TargetNodeID: targetNodeID})
	if err != nil { return err }
	if !resp.Success { return fmt.Errorf("migrate: %s", resp.Message) }
	return nil
}

func (d *Driver) Usage(ctx context.Context) (core.Resources, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	resp, err := d.client.Usage(ctx, &agent.UsageRequest{})
	if err != nil { return core.Resources{}, err }
	if !resp.Success { return core.Resources{}, fmt.Errorf("usage: %s", resp.Message) }
	return agent.ToCoreResources(resp.Usage), nil
}

func (d *Driver) Close() error { return d.client.Close() }
```

---

## The Engine Registry: `internal/driver/driver.go`

Engines are registered by name, and the registry holds **only engines**:

```go
var (
	mu       sync.Mutex
	registry = map[string]Factory{}
)

func Register(name string, f Factory) { mu.Lock(); defer mu.Unlock(); registry[name] = f }

func New(name string) (core.Driver, error) {
	mu.Lock()
	f, ok := registry[name]
	mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown driver %q, available %v: %w", name, Names(), core.ErrRequestRejected)
	}
	d, err := f()
	if err != nil { return nil, fmt.Errorf("driver %q: %w", name, err) }
	return d, nil
}
```

**A node id is not an engine name.** An earlier version overloaded this same
`New(name)` to accept either — an engine name *or* a node id, backed by a
`remoteParams` map keyed by node id. It read as though the controller were
choosing an engine per VM, and it made `remote` look like a third engine beside
`fake` and `kvm`. It is not: it is the transport below, and the registry no
longer needs to know about it.

---

## Wire in the Controller: `internal/app/controller.go`

The controller resolves a node's driver by dialing that node's agent:

```go
func drivers(store *memory.Store) func(string) (core.Driver, error) {
	return func(nodeID string) (core.Driver, error) {
		node, err := store.GetNode(context.Background(), nodeID)
		if err != nil { return nil, fmt.Errorf("node %s: %w", nodeID, err) }
		if node.Address == "" { return nil, fmt.Errorf("node %s has no agent address", nodeID) }
		return remote.New(nodeID, node.Address)
	}
}
```

That factory is handed to the scheduler and the compute service, so both just
call `DriverFor(nodeID)` and get something satisfying `core.Driver` — the real
engine is three hops away, on the agent.

**There is no `agent.nodes` config.** The agent address comes from the
bootstrap request and is recorded on the node. A flag would be a second copy of
a fact the database already holds, and the two would drift the moment an agent
moved ports.

**Why read the store on every use rather than cache it?** A process-global cache
would be filled only by whichever controller consumed `node.registered` — with
a shared durable consumer, the other controller never fills it. It is also empty
after a restart, once the command has been acked. Either way a controller that
trusted the cache would fail to resolve a node that was perfectly healthy, and
`DriverValidateFilter` drops hosts it cannot resolve a driver for, so it would
present as "no eligible hosts" with nothing to act on.

---

## Run It

```bash
# api
./arsan api --addr=:8080

# controller: no --driver flag. It has no engine to pick.
./arsan controller --bus.url=nats://127.0.0.1:4222

# each agent declares its own engine
./arsan agent --api.url=http://127.0.0.1:8080 --node.name=compute-1 \
  --node.advertise=host1:9090 --node.vcpu=8 --node.ram=16 --node.disk=200 \
  --driver=kvm --agent.addr=:9090 --bootstrap-token-file=/etc/arsan/token
```

The `--driver` flag belongs to the **agent**. It names the engine the agent will
use to build real VMs; `--driver=kvm` there means the agent calls libvirt. The
controller has no equivalent flag because it has no engine — its whole job is to
choose which agent and command it.

---

## Key Points

| Aspect | Implementation |
|--------|----------------|
| **Engine runs on** | the agent, named by the agent's `--driver` |
| **Controller's driver** | `remote.Driver` — forwards each call over gRPC |
| **Address resolution** | node record, read per use |
| **Timeouts** | 30s default per gRPC call |
| **Error handling** | Agent errors wrapped, `ErrCapacityConflict` preserved |

---

## Next

Step 10: Integration Tests
