# Business logics

How arsan decides things, and who is allowed to decide them.

This document covers four things that are architectural rather than local:

1. [Agent bootstrap](#1-agent-bootstrap) — a shared token, exchanged once
2. [Pluggable scheduler](#2-pluggable-scheduler) — the contract a scheduler must honor
3. [API as CRUD, logic in the controller](#3-api-as-crud-logic-in-the-controller)
4. [Subcommand independence](#4-subcommand-independence)

> **One rule governs changes to this document:** when the business logic is
> ambiguous, propose and ask. Do not pick an interpretation and encode it.

Two constraints shape everything below, and both are deliberate:

- **The command bus is NATS JetStream** (`internal/bus`). No Kafka, no RabbitMQ.
  JetStream is chosen for acknowledged, persisted delivery rather than throughput.
- **PostgreSQL via GORM** for anything that must outlive a process or be shared
  across hosts (`internal/store/gormdb`). SQLite backs the same store for
  tests, and the in-memory store is for unit tests.

---

## 0. Ownership: who decides what

The single most important rule in this codebase. If you cannot answer "which
subcommand owns this decision", the code is in the wrong place.

| Decision | Owner | Everyone else |
|---|---|---|
| Is this request well-formed? | `api` | trusts it |
| Persist the requested record | `api` | reads it |
| Which host runs this VM? | `controller` | asks the queue |
| Build / start / stop the domain | `controller` | asks the agent |
| Move a VM off a dead host | `controller` | never invents a new one |
| Reserve capacity | `controller` (via scheduler) | reads only |
| Touch libvirt / vSphere | `agent` | never imports a hypervisor |
| Node identity and credentials | `controller` (bootstrap) | agent presents a token |

The rule that follows from the table: **`api` never places, `controller` never
serves HTTP, `agent` never decides anything.**

---

## 1. Agent bootstrap

A compute node registers itself over **REST**, the way a kubelet registers its
Node object with the API server. It is not a gRPC call, and it is not the
controller.

### Why REST, and why not self-assertion

A kubelet `POST`s its own Node object. That works because the API server cannot
dial a kubelet, so identity has to be pushed inward. arsan is the same way round
from the node's perspective: the agent may sit behind NAT, and it has to be
able to reach the control plane on its own.

The part worth copying from Kubernetes is not the verb, it is that the node
**proves** who it is rather than being believed.

### One shared token

```
controller                  api                       agent
   │                          │                          │
   │ --bootstrap.token-file   │                          │
   │   (one secret, 0600)     │                          │
   │                          │                          │
   │                     POST /v1alpha1/nodes            │
   │                          │◀──── {token, name,        │
   │                          │      arch, driver,        │
   │                          │      address, total} ─────┤
   │                          │                          │
   │                          │  constant-time compare   │
   │                          │  reject or accept        │
   │                          │  session ends           │
   │                          ├──── 201 {node} ──────────┤
   │                          │                          │
   │                          │  upsert to node list     │
   │                          │                          │
   │                          │── publish node.registered ▶│ bus
   │◀─────────────────────────│                          │
   │ dial agent at the address│                          │
   │ it declared              │                          │
```

No token table. There is nothing to store, because the api compares against the
single value it already holds in its own configuration.

### Why one token is acceptable

A shared token is normally a bad idea: it is a cluster-wide bearer secret. That
objection assumes a long-lived reusable credential, which this is not.

- it is presented **once per agent start**, at bootstrap only
- **rotation is restarting the api** with a new token, which invalidates every
  outstanding bootstrap at once
- a bootstrapped agent never presents it again

The residual risk is narrow and named plainly: anyone who reads the token from
the api's config, its environment, or a leaked file can join as a node until
rotation. Keeping that acceptable:

- generate with `crypto/rand`, not a human-chosen string
- pass by **file**, never a flag — a flag is world-readable through `ps`
- the file is checked for mode and a group- or world-readable token is refused
- rotate on any suspicion

A stored token table would fix that residual risk at the cost of state that can
leak, drift, and go stale. For a control plane that does not otherwise need a
secrets table, that trade is not worth making.

### The exchange

One HTTP request, one response, no session:

```http
POST /v1alpha1/nodes
Content-Type: application/json

{
  "token":   "<shared bootstrap secret>",
  "name":    "compute-1",
  "arch":    "amd64",
  "driver":  "kvm",
  "address": "10.0.0.11:9090",
  "total":   {"vcpu": 8, "ram": 16, "disk": 200}
}
```

`201 Created` with the stored node, or `401 Unauthorized`.

The agent sends `address` on **every** bootstrap, so it re-announces its
endpoint each time it starts. That is what lets the controller dial it without a
second discovery step, and it means a node whose port changed re-registers
correctly.

The HTTP session ends at the response. Everything after is the controller
dialling the agent's gRPC server for engine calls.

### Rules

- **Constant-time comparison.** A byte-wise compare leaks the secret's length
  and a prefix, one byte at a time. Use `crypto/subtle`.
- **Refuse a loose file mode.** `cmd/secret.go` rejects a token file readable by
  group or other.
- **The api owns authentication.** Validating a credential is authn, which is
  the api's job. It is not a business decision and never reaches the bus.
- **A bootstrapped agent holds no credential afterwards.** The agent is the
  gRPC server; the controller initiates those calls, so there is nothing left to
  authenticate.
- **An upsert, never a replace.** Re-registering refreshes address and capacity
  and leaves state and reservations alone, so an agent restarting mid-build does
  not reset its host.

### Registration does not wait for delivery

The api upserts the node as `NOTREADY` and publishes **without waiting for the
ack**, then returns `200`. This is safe because of what `NOTREADY` means:

```
upsert node (NOTREADY)         ← written, and NOT schedulable
      │
publish cmd.node.registered   ← does not block on the ack
      │
200 OK                        ← the agent may start serving
```

Nothing can be placed on a `NOTREADY` node, so a lost announcement costs nothing
that matters. Blocking on the ack would make an agent's start-up time depend on
the queue's latency in exchange for no safety gain.

The agent is READY only once it answers a health check over gRPC:

```
POST /nodes ──▶ NOTREADY ──▶ controller probes over gRPC ──▶ READY
                    │                    │
                    │                    └── no answer ──▶ NOTREADY + reason
                    └── never probed ──▶ NOTREADY + reason
```

The **controller** probes, not the agent: the controller already dials every
agent for engine calls, so probing costs a round trip it is already paying and
needs no new transport in the other direction.

The probe calls the agent's `Usage` RPC rather than adding a dedicated health
method. `Usage` is a real round trip *through the compute engine*, so a host
whose libvirt socket is unreachable is correctly NOTREADY even though its agent
process is alive. A liveness check that only proved the process was up would
place such a host into the scheduler's eligible set.

A node is added to the list as soon as it registers and becomes usable only once
it proves it can run a VM. Those are deliberately separate: one is a claim, the
other is evidence.

Probing repeats every `ProbeInterval`, so a node that stops answering comes back
out of the eligible set instead of quietly attracting VMs it cannot run.

### Node states

`READY` and `NOTREADY`, modelled on a Kubernetes node's Ready condition:

| State | Meaning |
|---|---|
| `READY` | answered a health check; may host VMs |
| `NOTREADY` | has not answered, or was taken out of service |

Two states is a deliberate simplification. A Kubernetes node also tracks memory,
disk and PID pressure; all of those are reasons a host cannot take a VM, and all
of them collapse into `NOTREADY` here. Nothing in arsan can act on the
difference, and every extra state is another one the scheduler's filter has to
reason about.

### NOTREADY carries a reason

`Node.NotReadyReason` says *why*, because a bare `NOTREADY` covers too many
distinct faults for an operator to act on: an agent that will not answer, an
engine that is broken, a host an operator drained.

```json
{ "id": "n-compute-1", "state": "NOTREADY",
  "not_ready_reason": "rpc error: code = Unavailable desc = connection refused" }
```

Three rules:

- **The reason is cleared on recovery.** A stale explanation outliving the fault
  it described is worse than none.
- **An unchanged failure does not rewrite it**, so a sweep every 15s does not
  churn the record or the logs.
- **A disabled node keeps its own reason.** Probing an operator-drained host
  fails, but attributing that failure to the agent would misdescribe a decision
  the operator made.

### Enabled versus readiness

With two states, "stopped answering" and "operator disabled it" share
`NOTREADY`. `Node.Enabled` separates them, and it is why a probe will not
resurrect a drained host:

```go
if !node.Enabled {
    return nil   // the operator's decision stands
}
```

`Disable` clears `Enabled`; a probe only sets `READY` when it is still true.

### No address flags

Because the agent declares its endpoint at bootstrap, the controller needs no
`--agent.nodes`. It looks the address up from the node record.

A flag would be a second copy of a fact the database already holds, and the two
would drift the first time an agent restarted on a different port. Resolution
goes through the node record, and a node that has not registered has no driver
and is correctly unplaceable.

### What the agent does, in order

```
1. net.Listen(gRPC port)        ← binds before anything else
2. POST /v1alpha1/nodes         ← bootstrap with the token
3. grpcServer.Serve(...)        ← now reachable
```

The order is deliberate. `net.Listen` binds the port, so a controller dialling
between step 1 and step 3 has its connection queued in the backlog rather than
refused. Bootstrapping first would open a window where the controller has been
told this agent exists but cannot reach it.

The agent re-bootstraps on every start, which is how a changed port or changed
capacity reaches the control plane without an operator editing anything.

A refused token is **not** retried: waiting cannot make it valid, and an agent
looping on a `401` looks alive while being permanently unable to join. An
unreachable api **is** retried, because that is a startup ordering problem.

### Token generation

```sh
# Once, on the api host.
arsan apitoken > /etc/arsan/bootstrap.token && chmod 0600 /etc/arsan/bootstrap.token

arsan api --bootstrap.token-file /etc/arsan/bootstrap.token

# Copy to each agent host; read there, then delete.
scp /etc/arsan/bootstrap.token compute-1:/root/
arsan agent --node.id=n1 --bootstrap.token-file /root/bootstrap.token
```

---

## 2. Pluggable scheduler

The scheduler is a registry entry, exactly like a compute engine. Someone with a
better placement policy adds a package and a flag value; nobody edits the
controller.

### The interface

Already declared in `internal/core/ports.go`, and deliberately narrow:

```go
type Scheduler interface {
    Schedule(ctx context.Context, req PlacementRequest) (Placement, error)
}
```

Compute depends on this, never on `*scheduler.Scheduler`. A replacement is
invisible to everything upstream.

### Registry

Mirrors `internal/driver`:

```go
scheduler.Register("filter-weigh-claim", func() (core.Scheduler, error) { ... })
scheduler.Names()   // for `arsan controller --help` and error messages
scheduler.New(name)
```

Selected by flag on the subcommand that places:

```sh
arsan controller --scheduler=filter-weigh-claim   # default
arsan controller --scheduler=packing-tight
```

Unknown name must fail at startup with the list of names that do exist, the same
way `driver.New` does. A controller that starts and then discovers it cannot
place anything is worse than one that refuses to start.

### The contract

A replacement scheduler is untrusted code running inside the claim path. These
are the properties the rest of the system assumes, and a scheduler that breaks
any of them is a bug, not a preference:

1. **Decide, do not apply.** `Schedule` returns a decision. It must not
   transition instance state, and it must not call a compute engine except
   `Driver.Validate`.
2. **The claim is the only write.** Capacity is taken with
   `NodeStore.ReserveNodeResources`, which is all-or-nothing. A scheduler that
   reserves resource-by-resource reintroduces the overcommit the store exists
   to prevent.
3. **A lost claim is not a user error.** `ErrCapacityConflict` from the claim
   means someone else won the race. Retry with fresh data, bounded. Returning
   it to the caller as a failure surfaces our timing problem as the user's.
4. **Honor `ExcludeNodeIDs`.** Re-placement puts the host that just died in
   here. A scheduler that ignores it will put VMs straight back onto the
   failing machine.
5. **Deterministic.** Identical inputs must produce an identical decision, with
   ties broken on node ID. Without this, every retry reshuffles the cluster and
   placement stops being explainable.
6. **`ErrNoValidNode` survives.** The API maps that sentinel to 409 and
   everything else to 500. A scheduler that returns a fresh `errors.New` for
   "nothing fits" destroys that distinction and the client gets a useless 500.
7. **Respect `ctx`.** Every call has a deadline. A scheduler that blocks forever
   wedges the instance state machine.

### Why the interface is this small

`Placement` carries `Reason` and `RunnerUp`. A replacement is expected to fill
them in — "why this host?" must stay answerable from the placement alone, without
re-running the decision and hoping for the same answer.

---

## 3. API as CRUD, logic in the controller

The api validates and persists records, and authenticates nodes. It never
places, never transitions an instance, and **never imports the controller's
libraries**. When it needs a decision, it publishes a command.

### The separation is structural, not a convention

This is enforced by the import graph, not by discipline:

```
$ go list -deps ./api/... | grep cinvat
github.com/cinvat/arsan/api
github.com/cinvat/arsan/api/v1alpha1
github.com/cinvat/arsan/api/v1alpha1/handler
github.com/cinvat/arsan/internal/bus
github.com/cinvat/arsan/internal/core
github.com/cinvat/arsan/pkg/util
```

No `scheduler`, no `compute`, no `node`, no `driver`. The api *cannot* place a
VM because the code that places VMs is not on its import path. A test asserting
this is worth more than a comment asking people not to.

### Request lifecycle

```
POST /v1alpha1/servers
        │
        ▼
  api: validate → insert Instance(PENDING)      ← CRUD
        │
        ├── publish instance.create{id}         ← NATS JetStream, acked
        ▼
  202 Accepted {id, state: PENDING}              ← not "created"
        │
        │  controller consumes, on its own schedule:
        ▼
  re-read instance → still PENDING?
        │  no ──► Ack (repeat is a no-op)
        ▼  yes
  schedule → claim → Transition(SCHEDULING)
        → engine Create → Transition(BUILDING)
        → engine Start → Transition(ACTIVE)
        │
        └── Ack                                      ← retires the command
```

The client polls `GET /servers/{id}`. It does not subscribe, and nothing about
the request depends on the api staying up.

The `PENDING` row is not a decision. It records that a user asked for a VM;
where it runs is the controller's answer.

### Placement is read, never recomputed

`GET /servers/{id}` returns what the scheduler wrote. The scheduler updates the
instance record with `Placement.Reason` and `RunnerUp` when it places, so "why
this host?" is answerable from the record alone.

The api must not expose a placement endpoint that calls the scheduler. A GET
that recomputes is a GET with side effects, and one whose answer may differ from
the decision that was actually applied.

### The bus is NATS JetStream

`internal/bus` publishes over JetStream rather than core NATS, and the reason is
durability rather than features:

- **Persisted before acknowledged.** `Publish` returns only once the server has
  stored the command, so a `202` means "queued", not "hopefully".
- **Durable consumers.** A controller restart resumes from its position rather
  than replaying the stream.
- **Explicit ack, bounded redelivery.** A command is retired only when its
  handler says so. After `MaxDeliver` it is marked delivered and left visible,
  so a permanently failing command stops spinning and becomes something an
  operator can see.

Core NATS is fire-and-forget and cannot survive a restart mid-build, which is
exactly the case that matters.

### Command catalog

| Command | Effect in controller |
|---|---|
| `node.registered` | a node exists and can be dialled at the address it declared |
| `instance.create` | build an instance the api has persisted as `PENDING` |
| `instance.delete` | tear down, then release the reservation |
| `instance.start` | `SHUTOFF` → build |
| `instance.stop` | `ACTIVE` → `SHUTOFF`, reservation retained |
| `instance.migrate` | move to a named target host |
| `node.reclaim` | operator-driven re-placement (drain) |

### Handler contract

Every handler re-reads the record and acts on current state:

```go
func onInstanceCreate(ctx context.Context, cmd bus.Command) error {
    inst, err := store.Get(ctx, cmd.Target)
    if err != nil {
        return err // retried
    }
    if inst.State != core.StatePending {
        return nil // an earlier delivery claimed it; Ack makes this a no-op
    }
    if err := compute.Build(ctx, inst); err != nil {
        return err // NAK, redelivered after the lease
    }
    return nil // Ack
}
```

Returning `nil` acks. Returning an error naks, and JetStream redelivers. A
crash between "side effect done" and the ack runs the handler again, and the
compare-and-swap transitions are what make that a no-op rather than a second
build.

`cmd.Attempt` bounds the retries; handlers park work they cannot complete
rather than failing forever, since a command that retries indefinitely looks
exactly like one making progress.

### What the api is still allowed to do

CRUD is not a lesser role:

- reject a malformed flavor before anything is written
- create the `PENDING` instance record
- authenticate a node at bootstrap
- serve reads, including `?state=`
- translate `ErrNoValidNode` and friends onto status codes

### Response codes

| Code | Meaning |
|---|---|
| `202` | queued for the controller; poll for the outcome |
| `400` | the request is wrong — the client fixes it |
| `401` | bootstrap token missing or invalid |
| `404` | no such instance or node |
| `409` | placement conflict, invalid transition, capacity conflict |
| `5xx` | arsan is wrong |
| `503` | the bus is unreachable, so the command was not queued |

That last row matters: publishing a command that never reached the bus is not a
`202`. Reporting success for work nobody was asked to do is the failure mode a
queue introduces, and it has to be named.

---

## 4. Subcommand independence

One binary, three cobra subcommands. In production they are separate
executables; the shared entry point is only how they are started from this
repository. What makes that possible is that they share **no libraries**, only a
database and a bus.

### What each owns

| | `api` | `controller` | `agent` |
|---|---|---|---|
| HTTP listener | yes | no | no |
| Bootstrap authentication | yes | no | no |
| CRUD records | yes | reads, transitions | no |
| Scheduler | no | yes | no |
| Instance lifecycle | no | yes | no |
| Node liveness | no | yes | reports |
| Hypervisor | no | no | yes |

### What crosses between them

| From → To | Channel | Carries |
|---|---|---|
| `api` → `controller` | NATS JetStream | commands |
| `agent` → `api` | HTTP, once | bootstrap token, identity, gRPC address |
| `controller` → `agent` | gRPC | engine calls and the health check that makes a node READY |
| all → database | SQL | records |

No other channel, and no direct library calls in either direction.

### Sharing, and where it lives

| Location | Holds | Why there |
|---|---|---|
| `pkg/util` | ID generation, slugs — no opinion about VMs | genuinely generic helpers |
| `internal/core` | domain types and interfaces | arsan's own concepts, not a library's |
| `internal/bus` | the command bus | both halves need it, and Go permits sharing `internal/` within a module |

`pkg/` holds only what has no opinion about the domain. Domain types and the bus
are shared too, but they are arsan's own concepts, and putting them under `pkg/`
would suggest a general-purpose package that nothing else should depend on.

### Process topology

Because the bus is a network service, `api` and `controller` are genuinely
separable — nothing about them requires co-location.

The store still has one caveat worth stating:

| Store | Can api and controller run apart? |
|---|---|
| `memory` | No. Each process holds its own copy, so the api would serve records the controller has never seen. |
| `postgres` | Yes. State is shared; the bus bridges the rest. |

So the memory store is a development convenience, not a deployment mode, and
postgres is what makes the split real. Until it lands, `arsan api` is the
single process to run.

The rule that follows either way: **a subcommand may never assume it is the only
reader.**

---

## 5. Invariants that survive all of this

Refactoring the boundaries must not break these. They are the properties the
tests actually protect.

1. **State changes only through compare-and-swap.**
   `Transition(ctx, id, from, to)` is the only path, and it validates `from`.
   Never read a struct, mutate it, and `Update` — the store rejects a state
   change through `Update` precisely to make that impossible.

2. **The claim is atomic and all-or-nothing.**
   `ReserveNodeResources` checks every dimension before mutating any. Two
   requests that each fit in vCPU but neither in RAM must both fail.

3. **A failed re-placement leaves the instance `PENDING` with a bumped
   generation.** That marker is what `Reconcile` retries on. Without it, a
   transient cluster-wide shortage strands the VM permanently.

4. **Sentinel errors survive every layer.**
   `ErrNoValidNode`, `ErrCapacityConflict`, `ErrInstanceNotFound` must stay
   `errors.Is`-comparable from the store up through the queue to the HTTP
   response. A flattened error turns an actionable 409 into a 500.

5. **Engine operations are idempotent.**
   Compute retries. `Create`, `Start`, and `Delete` are safe to call twice —
   and `Delete` must not error on an instance that is already gone, or recovery
   becomes an infinite failure loop.

6. **Lost events are recoverable.**
   The queue may drop or reorder. A dropped command is corrected by the
   reconciliation sweep. Nothing may depend on receiving every message.

7. **Reservation is atomic, in SQL, not in Go.**
   `ReserveNodeResources` is one `UPDATE` whose WHERE clause carries the
   capacity guard, and `RowsAffected == 0` is `ErrCapacityConflict`. Checking
   the dimensions in Go and then writing reintroduces the overcommit the store
   exists to prevent.

---

## 6. Open questions

Decisions that the current shape does not settle, recorded so they are not
rediscovered:

- **Liveness: connection state or a heartbeat message?** *Settled: connection
  state.* The controller probes the agent over gRPC and readiness follows the
  answer. A heartbeat message was tried and removed: no agent sent one, so the
  timestamp aged every node out of service about 30s after it registered.
  Nothing in this design believes a timestamp.
- **Do `api` and `controller` keep the in-memory store together, or is
  postgres required for the split?** The memory store gives each process its
  own copy, so the split only works on a shared store.
- **Does the bootstrap token need a TTL?** It does not, because rotation is
  restarting the api. Adding a stored expiry would reintroduce the token table
  this design exists to avoid.
- **Does `api` still run loops in-process?** Once the postgres store is wired,
  no. Keeping both modes invites the memory-store trap above.
- **Flavor persistence.** Flavors are currently passed inline on
  `POST /servers`. A `flavors` table with named references would make them
  shareable and versionable; the API is already the right place for it.
