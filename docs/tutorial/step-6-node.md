# Step 6: Node Service — Registration, Usage & Drain

## Goal
Implement node registration, usage reporting, and enable/disable. **Readiness is
not decided here** — the controller probes the agent over NATS and sets it.

## Terminology

Following the Kubernetes model, since that is the shape this mirrors:

| Term | Means |
|------|-------|
| **Node** | the machine: capacity, readiness, address. Identified by hostname. |
| **Agent** | the process running on a node. Owns the compute engine. |
| **Engine** | what actually creates VMs: `fake`, `kvm`, `esxi`. Only agents have one. |

The agent is named after its node, exactly as a kubelet reports the node it
runs on. There is no separate "host" concept — that was a third word for Node
and has been removed.

---

## File: `internal/records/records.go` (node half)

There is no node service. An earlier `internal/node` package held registration
and a liveness loop, but nothing ever called the loop and the api needed only
CRUD — so the package was deleted and the api's records layer owns the node
record outright. The controller owns everything else, through the probe below.

```go
package records

// Nodes implements core.NodeService over a node store.
type Nodes struct {
	Store core.NodeStore
}

// Register upserts a node. Only the bootstrap endpoint reaches it, and that
// endpoint authenticates before calling.
func (s *Nodes) Register(ctx context.Context, req core.RegisterNodeRequest) (core.Node, error) {
	if req.Name == "" {
		return core.Node{}, fmt.Errorf("name required: %w", core.ErrInvalidRequest)
	}
	return s.Store.Register(ctx, core.Node{
		ID:      NodeID(req.Name),
		Name:    req.Name,
		Arch:    req.Arch,
		Driver:  req.Driver,
		Address: req.Address,
		Total:   req.Total,
		Enabled: true,
	})
}

// NodeID derives a node's stable ID from its name.
//
// "Compute Node 1" and "compute-node-1" must resolve to the same node, or one
// host registers twice and its reservations are split across two records.
func NodeID(name string) string { return "n-" + util.Slug(name) }

// RecordUsage stores what the agent reports it is using. It deliberately does
// NOT set readiness: the controller decides that by probing the agent over
// NATS, and nothing on this path may promote a node the probe has not cleared.
func (s *Service) RecordUsage(ctx context.Context, nodeID string, usage core.Resources) error {
	node, err := s.cfg.Nodes.RecordUsage(ctx, nodeID, usage, time.Now().UTC())
	if err != nil { return err }
	return node, nil
}

func (s *Service) Get(ctx context.Context, id string) (core.Node, error) {
	return s.cfg.Nodes.GetNode(ctx, id)
}

func (s *Service) List(ctx context.Context) ([]core.Node, error) {
	return s.cfg.Nodes.ListNodes(ctx)
}

func (s *Service) Enable(ctx context.Context, id string) error {
	node, err := s.cfg.Nodes.GetNode(ctx, id)
	if err != nil { return err }
	if _, err := s.cfg.Nodes.SetState(ctx, id, node.State); err != nil { return err }
	return nil
}

func (s *Service) Disable(ctx context.Context, id string) error {
	node, err := s.cfg.Nodes.GetNode(ctx, id)
	if err != nil { return err }
	if _, err := s.cfg.Nodes.SetNodeEnabled(ctx, id, false); err != nil {
		return fmt.Errorf("disable %s: %w", id, err)
	}
	if _, err := s.cfg.Nodes.SetReadiness(ctx, id, core.NodeNotReady, "disabled by operator"); err != nil {
		return fmt.Errorf("disable %s: %w", id, err)
	}
	s.cfg.Log.Info("node disabled", "node", id, "was", node.State)
	return nil
}

// A node is enabled when it is registered; disabling clears the flag and
// records why, so an operator reading the API can tell a drained host from a
// broken one.
//
// There is deliberately no timestamp-based liveness loop here. A missed
// missing heartbeat says the agent stopped calling, which is not the same as
// "this node cannot run a VM" — and no agent calls at all, since it bootstraps
// once and afterwards only answers on the bus. The controller's probe answers the
// question that actually matters, and it answers it through the compute engine.

func (s *Service) Drained(ctx context.Context, id string) ([]string, error) {
	n, err := s.cfg.Nodes.GetNode(ctx, id)
	if err != nil { return nil, err }
	return n.Instances, nil
}

func (s *Service) info(msg string, kv ...any)   { if s.cfg.Log != nil { s.cfg.Log.Info(msg, kv...) } }
func (s *Service) warn(msg string, kv ...any)   { if s.cfg.Log != nil { s.cfg.Log.Warn(msg, kv...) } }
func (s *Service) error(msg string, kv ...any)  { if s.cfg.Log != nil { s.cfg.Log.Error(msg, kv...) } }

func slug(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	s = strings.ReplaceAll(s, " ", "-")
	return "n-" + s
}
```

---

## Key Design Points

### Registration vs Readiness
```go
// Register: node comes up as NOTREADY
// Probe (controller -> agent, over NATS): transitions to READY
```
A node that cannot be reached should never be schedulable.

These are deliberately separate. Registration is the node *claiming* to exist;
readiness is the controller *observing* that it can be reached and driven. The
controller probes on the agent's `Usage` RPC, which round-trips through the
compute engine — so a host whose libvirt socket is unreachable is correctly
NOTREADY even though its agent process is alive.

Probing repeats, because a node that answered at registration can stop
answering. Without a repeat it would stay READY and keep attracting VMs it
cannot run.

**There is no heartbeat loop, and no timestamp check.** An earlier version aged
nodes out on `LastHeartbeat`, but no agent ever sends a heartbeat — it
bootstraps once and afterwards only answers on the bus — so every node silently aged
out of service about 30 seconds after registering. Readiness is the probe and
nothing else.

### Probing Is Separate from Registration
```go
func (c *Controller) ProbeAll(ctx context.Context) error {
    for _, n := range nodes {
        c.ProbeAgent(ctx, n.ID)   // usage over NATS
    }
}
```
A **failed probe** triggers re-placement, not the failed call itself. This keeps
detection and reaction separate for testability.

The reason is recorded on the node, so an operator sees *why* rather than a bare
NOTREADY.

### Detection and Reaction Stay Separate

A failed probe records NOTREADY and the reason; it does not itself rebuild
anything. That separation is what keeps the probe testable — and re-placement
on node failure is currently the missing feature (see step 5), not something
the probe does inline.

### Usage Is Recorded, Availability Is Derived
```go
func (s *Store) RecordUsage(ctx, id, usage, at) (core.Node, error) {
    n.Usage = usage   // informational
    n.LastHeartbeat = at  // for an operator, not for readiness
    // Available is NOT touched: it comes from reservations
}
```
Reported usage is informational; available capacity is derived from reservations
so a stale or wrong report cannot cause an overcommit.

`RecordUsage` never touches `state`. There were once three copies of "a beat
means READY" — in the memory store, the gormdb store, and `internal/records` —
and the last one was still promoting nodes on a beat, silently undoing the
probe. The name now says what it does.

---

## Tests

The node service only registers, records usage and drains — it does not decide
readiness, so most readiness tests belong to the controller.

```go
// internal/records and internal/store/gormdb carry these; the node record
// is owned by the api side, so its tests live there, not in a service.
func TestRegisterLeavesNodeNotReady(t *testing.T) {
	s := setup(t)
	n, err := s.Register(context.Background(), core.RegisterNodeRequest{
		Name: "compute-1", Driver: "fake", Address: "127.0.0.1:9090",
		Total: core.Resources{VCPU: 8, RAM: 16, Disk: 100},
	})
	if err != nil { t.Fatal(err) }

	// Registering proves nothing about the agent. It comes up NOTREADY.
	if n.State.Ready() { t.Fatalf("state %s, want NOTREADY until probed", n.State) }
	if n.NotReadyReason == "" { t.Fatal("a NOTREADY node should say why") }
}

func TestRegisterRequiresDriver(t *testing.T) {
	s := setup(t)
	// The controller has no engine to offer as a default, so a node that did
	// not declare one cannot be placed on.
	if _, err := s.Register(context.Background(), core.RegisterNodeRequest{
		Name: "compute-1", Total: core.Resources{VCPU: 8, RAM: 16, Disk: 100},
	}); err == nil {
		t.Fatal("expected an error when the agent declares no engine")
	}
}

func TestRecordUsageDoesNotChangeReadiness(t *testing.T) {
	s := setup(t)
	n, _ := s.Register(context.Background(), core.RegisterNodeRequest{
		Name: "compute-1", Driver: "fake", Total: core.Resources{VCPU: 8, RAM: 16, Disk: 100},
	})
	if err := s.RecordUsage(context.Background(), n.ID, core.Resources{VCPU: 2}); err != nil {
		t.Fatal(err)
	}

	n, _ = s.Get(context.Background(), n.ID)
	if n.State.Ready() { t.Fatal("reporting usage must not make a node schedulable") }
	if n.Usage.VCPU != 2 { t.Fatalf("usage = %+v, want it recorded", n.Usage) }
	// Available comes from reservations, never from a usage report.
	if n.Available != n.Total {
		t.Fatalf("available = %+v, want it untouched at %+v", n.Available, n.Total)
	}
}

func TestDisableRecordsTheOperatorAsTheReason(t *testing.T) {
	s := setup(t)
	n, _ := s.Register(context.Background(), core.RegisterNodeRequest{
		Name: "compute-1", Driver: "fake", Total: core.Resources{VCPU: 8, RAM: 16, Disk: 100},
	})
	if err := s.Disable(context.Background(), n.ID); err != nil { t.Fatal(err) }

	n, _ = s.Get(context.Background(), n.ID)
	if n.State.Ready() { t.Fatal("a drained node must not be schedulable") }
	if n.Enabled { t.Fatal("Enabled = true after Disable") }
}
```

The probe and its interaction with drain live in `internal/app`, because the
probe is the controller's:

```go
// internal/app/controller_test.go
func TestProbeUnreachableAgentRecordsReason(t *testing.T)      // NOTREADY + a reason
func TestProbeLeavesDisabledNodeAlone(t *testing.T)            // drain survives the probe
func TestProbeTakesAReadyNodeBackOut(t *testing.T)             // readiness is not one-shot
func TestNodeStaysReadyAcrossProbeSweeps(t *testing.T)         // no timestamp ageing
func TestDriverResolvesAfterRestartWithEmptyRegistry(t *testing.T)
```

---

## Run Tests

```bash
go test -race ./internal/records/... ./internal/store/...
```

---

## Next

Step 7: Agent Worker