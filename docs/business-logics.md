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
   │ --bootstrap.token        │                          │
   │   (one secret)           │                          │
   │                          │                          │
   │                     POST /v1alpha1/nodes            │
   │                          │◀──── {token, name} ──────┤
   │                          │                          │
   │                          │  constant-time compare   │
   │                          │  reject or accept        │
   │                          │  session ends           │
   │                          ├──── 200 {node} ──────────┤
   │                          │                          │
   │                          │  upsert to node list     │
   │                          │                          │
   │                          │── publish node.registered ▶│ bus
   │◀─────────────────────────│                          │
   │ ask agent at agent.<id>  │                          │
   │ (usage, then info)       │                          │
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
  "token": "<shared bootstrap secret>",
  "name":  "compute-1"
}
```

`200 OK` with the stored node, or `401 Unauthorized`.

That is the whole request. Capacity, engine and usage are fetched by the
controller after the probe succeeds, so the agent never describes a host it
may not know yet — and there is nothing to go stale when the host changes.

The HTTP session ends at the response. Everything after reaches the agent over
the bus at `agent.<node-id>`.

### Rules

- **Constant-time comparison.** A byte-wise compare leaks the secret's length
  and a prefix, one byte at a time. Use `crypto/subtle`.
- **The api owns authentication.** Validating a credential is authn, which is
  the api's job. It is not a business decision and never reaches the bus.
- **A bootstrapped agent holds no credential afterwards.** Nothing after
  bootstrap authenticates; the agent answers on the bus and the controller
  asks.
- **An upsert, never a replace.** Re-registering leaves state, spec and
  reservations alone, so an agent restarting mid-build does not reset its
  host.

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

The agent is READY only once it answers the probe over NATS:

```
POST /nodes ──▶ NOTREADY ──▶ controller probes ──▶ READY
                    │              (usage, then info)
                    │                    │
                    │                    └── no answer ──▶ NOTREADY + reason
                    └── never probed ──▶ NOTREADY + reason
```

The **controller** probes, not the agent: it already asks every agent for
engine calls, so probing costs a round trip it is already paying and needs no
new transport in the other direction. An earlier version did this over gRPC;
the transport moved to NATS request/reply so there is one transport to run,
secure, and reason about.

The probe asks `usage` rather than a dedicated health method. Usage is a real
round trip *through the compute engine*, so a host whose libvirt socket is
unreachable is correctly NOTREADY even though its agent process is alive. A
liveness check that only proved the process was up would place such a host
into the scheduler's eligible set.

A passing probe then asks `info` — what the agent runs, what its host holds —
and records it on the node. Bootstrap carries none of this.

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
1. POST /v1alpha1/nodes         ← bootstrap with the token
2. serve agent.<node-id>        ← answer engine calls on the bus
```

There is no listen-before-bootstrap dance any more: nothing dials the agent,
so there is no port to bind and no window where the controller knows about an
agent it cannot reach. If the agent is not serving, the request times out and
the node is NOTREADY — that is the whole failure model.

The agent re-bootstraps on every start. Capacity and engine are re-fetched by
the probe anyway, so a changed host reaches the control plane without an
operator editing anything.

A refused token is **not** retried: waiting cannot make it valid, and an agent
looping on a `401` looks alive while being permanently unable to join. An
unreachable api **is** retried, because that is a startup ordering problem.

### Token generation

```sh
# Once, on the api host: any random string. There is no generator command;
# openssl is fine.
TOKEN=$(openssl rand -hex 32)

arsan api --bootstrap.token="$TOKEN"

# Same value on each agent host.
arsan agent -t "$TOKEN" --driver=kvm
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
POST /v1alpha1/virtualmachines
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
        → engine Start → Transition(RUNNING)
        │
        └── Ack                                      ← retires the command
```

The client polls `GET /virtualmachines/{id}`. It does not subscribe, and nothing about
the request depends on the api staying up.

The `PENDING` row is not a decision. It records that a user asked for a VM;
where it runs is the controller's answer.

### Placement is read, never recomputed

`GET /virtualmachines/{id}` returns what the scheduler wrote. The scheduler updates the
instance record with `Placement.Reason` and `RunnerUp` when it places, so "why
this host?" is answerable from the record alone.

The api must not expose a placement endpoint that calls the scheduler. A GET
that recomputes is a GET with side effects, and one whose answer may differ from
the decision that was actually applied.

### Instance states

States are split by who owns them. The control plane owns `PENDING`,
`SCHEDULING`, `BUILDING`, `TERMINATING` and `ERROR`; the hypervisor owns
`RUNNING`, `SHUTOFF`, `SHUTDOWN`, `PAUSED` and `CRASHED`. The guest names
mirror libvirt's `virDomainState` one-for-one, except `BLOCKED` (folds into
`RUNNING` — it is running but waiting on I/O) and `PMSUSPENDED` (folds into
`PAUSED`), which are too fine to act on, and `NOSTATE`, which is never
stored: it means libvirt itself does not know.

A placement failure queues instead of erroring. The VM stays `PENDING` with
`PendingReason` naming the wait ("no valid node (rejected by: Capacity…)"),
the generation counts attempts, and the sweep retries. `ERROR` with
`FailureReason` is reserved for control-plane failures. A pending VM never
looks broken and a broken one never looks merely queued — the kubectl pod
analogy, where unschedulable is `Pending`, not `Failed`.

Observed state converges on every passing probe: the controller lists the
engine's domains and transitions steady records toward them, so a `virsh
destroy` behind arsan's back flips `RUNNING` to `SHUTOFF` within one probe
interval. Only steady states move, and only between steady states; `PENDING`,
`SCHEDULING`, `BUILDING`, `TERMINATING` and `ERROR` belong to whoever put
the record there, and hand-made domains (no record) are ignored. Absence is
also an answer — the probe just round-tripped that engine — except when the
listing itself fails, which converges nothing rather than
mass-shutting-off on a partial answer. A lost compare-and-swap is skipped,
not forced: whoever moved the record holds the newer truth.

Delete is mark-then-teardown: the api transitions the row to `TERMINATING`
synchronously, so the intent is visible even when the agent's host is off,
and the controller (command handler plus the sweep, which drives
`TERMINATING` rows on every pass with no staleness gate) tears down
asynchronously until the row can go away. Engine `Delete` is idempotent, so
redelivered commands and sweep retries converge instead of wedging, and a
re-delete re-publishes rather than erroring.

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
| `instance.delete` | tear down a `TERMINATING` row, then release the reservation |
| `instance.start` | `SHUTOFF` → build |
| `instance.stop` | `RUNNING` → `SHUTOFF`, reservation retained |
| `instance.migrate` | move to a named target host |
| `instance.attach-volume` / `detach-volume` | converge one volume, live if running |
| `instance.set-boot` | move the boot index; SHUTOFF only, redefines |
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
| `agent` → `api` | HTTP, once | bootstrap token, identity |
| `controller` → `agent` | NATS request/reply | engine calls, usage, and the `info` that makes a node READY |
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

3. **A failed placement leaves the instance `PENDING` with a reason and a
   bumped generation.** That marker is what `Reconcile` retries on, and the
   reason is what an operator reads instead of guessing. Without it, a
   transient cluster-wide shortage strands the VM permanently — and without
   the reason, visibly queued work looks broken.

4. **Sentinel errors survive every layer.**
   `ErrNoValidNode`, `ErrCapacityConflict`, `ErrInstanceNotFound` must stay
   `errors.Is`-comparable from the store up through the queue to the HTTP
   response. A flattened error turns an actionable 409 into a 500.

5. **Engine operations are idempotent.**
   Compute retries. `Create`, `Start`, and `Delete` are safe to call twice —
   and `Delete` must not error on an instance that is already gone, or recovery
   becomes an infinite failure loop. Storage ops are record-first with revert
   on engine failure, and file deletion ignores missing files, so a retried
   detach cannot strand storage nor fail on it.

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
  state.* The controller probes the agent over NATS and readiness follows the
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
- **Flavor persistence.** *Settled: named flavors.* `POST /flavors` defines
  immutable sizings (`vcpu` topology plus `memory` as `32MB`/`2GB`
  quantities); `POST /virtualmachines` takes `flavor_ref` and snapshots the
  resources onto the instance, so later flavor edits never resize existing
  VMs. The legacy inline `flavor` still decodes for old clients.
- **Storage.** *Settled: volumes are first-class records, Nova-style.* A
  flavor sizes compute only; block devices live in a `Volume` table
  (name, size, base image or blank, cdrom flag, boot index,
  delete-on-termination) and attach to one VM at a time by id, like
  `block_device_mapping_v2` with `source_type` image/blank/volume and a
  single boot index 0. The scheduler reserves their bytes; attach is
  compare-and-swap on `available`. The engine provisions one qcow2 overlay
  per volume next to its base and lists every owned file in domain metadata,
  so Delete removes exactly what it created — bases and isos are shared and
  never removed. Attach/detach works live on a running guest through
  `virDomainAttachDeviceFlags`; only the boot move needs a stopped redefine.
  Detach never deletes: only VM delete with delete-on-termination removes
  the file.
