# Step 7: Agent Worker

## Goal
Run the worker that lives on each compute node: answer engine calls on the bus
and bootstrap over REST. No gRPC anywhere — one transport for everything.

---

## Protocol: `internal/agent/worker.go`

One envelope, one op, JSON over NATS request/reply. The agent subscribes to
`agent.<node-id>`; the node id is the address, so there is nothing to dial.

```go
package agent

// Subject is where one agent listens: agent.<node-id>.
func Subject(nodeID string) string { return "agent." + nodeID }

type Operation string

const (
	OpInfo     Operation = "info" // what it runs, what its host holds
	OpValidate Operation = "validate"
	OpCreate   Operation = "create"
	OpStart    Operation = "start"
	OpStop     Operation = "stop"
	OpDelete   Operation = "delete"
	OpReboot   Operation = "reboot"
	OpMigrate  Operation = "migrate"
	OpUsage    Operation = "usage"
)

type Request struct {
	Op           Operation   `json:"op"`
	InstanceID   string      `json:"instance_id,omitempty"`
	NodeID       string      `json:"node_id,omitempty"`
	Name         string      `json:"name,omitempty"`
	Flavor       core.Flavor `json:"flavor,omitempty"`
	ImageRef     string      `json:"image_ref,omitempty"`
	TargetNodeID string      `json:"target_node_id,omitempty"`
}

type Response struct {
	OK     bool              `json:"ok"`
	Error  string            `json:"error,omitempty"`
	Info   core.InstanceInfo `json:"info,omitempty"`
	Usage  core.Resources    `json:"usage,omitempty"`
	Engine string            `json:"engine,omitempty"`
	Total  core.Resources    `json:"total,omitempty"`
}
```

The worker owns nothing but the engine:

```go
func Serve(ctx context.Context, commands *bus.NATS, nodeID string, drv core.Driver) error {
	w := NewWorker(nodeID, drv)
	stop, err := commands.Answer(ctx, Subject(nodeID), w.Handle)
	if err != nil {
		return err
	}
	defer stop()
	<-ctx.Done()
	return ctx.Err()
}
```

Identity is not part of this protocol. The agent sends it over REST at
bootstrap, and readiness comes from the controller probing `usage`. Adding
either here would put identity and liveness back inside the engine path, where
they do not belong.

---

## Bootstrap client: `internal/agent/bootstrap.go`

One POST, token plus name, no session:

```go
type BootstrapRequest struct {
	Token string `json:"token"`
	Name  string `json:"name"`
}
```

Capacity, engine and usage are fetched by the controller after the probe
succeeds, so the agent never describes a host it may not know yet. A refused
token is not retried; an unreachable api is.

---

## CLI Command: `cmd/agent.go`

```go
// The token is the only thing the agent must know to join. Capacity,
// engine and usage are fetched by the controller after the probe
// succeeds, so nothing here describes the host.
token := viper.GetString("bootstrap-token")

drv, err := driver.New(driverName)  // any registered engine; no switch

// Register over REST, like a kubelet registering its Node object.
// The request carries nothing but identity.
node, err := agent.RetryBootstrap(ctx, func(c context.Context) (core.Node, error) {
	return agent.Register(c, viper.GetString("api.url"), "v1alpha1", token,
		agent.BootstrapRequest{Name: name})
}, bootstrapAttempts, bootstrapBackoff)

return agent.Serve(ctx, commands, node.ID, drv)
```

The node's identity defaults to the hostname — like a kubelet reporting the
node it runs on. Flags left: `--node.name`, `--driver`, `--bus.url`,
`--api.url`, `-t/--bootstrap-token`. There is no listen address, no advertise
address, no capacity flags, and no token file: a string is enough.

---

## Run the Agent

```bash
# Terminal 1: the api, which holds the bootstrap token
./arsan api --addr=:8080 --bootstrap.token=s3cret

# Terminal 2: start the agent
./arsan agent --driver=fake -t s3cret
```

---

## Next

Step 8: API & Versioning
