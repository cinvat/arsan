# Step 9: Remote Driver

## Goal
`remote` is not an engine — it is the controller's NATS client to an agent,
shaped as a `core.Driver` so the scheduler and compute service never know the
VM is remote.

---

## File: `internal/driver/remote/driver.go`

```go
package remote

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

// call sends one engine op and decodes the answer. A timeout means the agent
// did not answer, which the caller records as NOTREADY or a failed operation.
func (d *Driver) call(ctx context.Context, req agent.Request) (agent.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	raw, _ := json.Marshal(req)
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

// Info asks the agent what it runs and what its host holds. The controller
// calls this after the probe succeeds and records the answer on the node.
func (d *Driver) Info(ctx context.Context) (engine string, total core.Resources, err error) {
	resp, err := d.call(ctx, agent.Request{Op: agent.OpInfo})
	if err != nil {
		return "", core.Resources{}, err
	}
	return resp.Engine, resp.Total, nil
}
```

Every op — validate, create, start, stop, delete, reboot, migrate, usage — is
one `call` with its op name. An earlier version did this over gRPC with a
hand-written client, a hand-written server descriptor, and a JSON codec to
make the two agree; the transport moved to NATS request/reply so there is one
transport to run, secure, and reason about.

---

## Wire in the Controller: `internal/app/controller.go`

```go
// drivers resolves a node's driver. The node id IS the address
// (agent.<node-id>), so there is nothing to look up, cache, or rediscover.
func drivers(commands bus.Bus) func(string) (core.Driver, error) {
	return func(nodeID string) (core.Driver, error) {
		return remote.New(nodeID, commands), nil
	}
}
```

That factory is handed to the scheduler and the compute service, so both just
call `DriverFor(nodeID)` and get something satisfying `core.Driver` — the real
engine is one NATS request away, on the agent.

---

## Run It

```sh
./arsan api --addr=:8080 --bootstrap.token=s3cret
./arsan controller
./arsan agent --driver=kvm -t s3cret
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
| **Controller's driver** | `remote.Driver` — one NATS request per call |
| **Addressing** | the node id is the address (`agent.<node-id>`) |
| **Timeouts** | 30s default per call; silence means NOTREADY |
| **Unanswered** | timeout error, never a cached failure |

---

## Next

Step 10: Integration Tests
