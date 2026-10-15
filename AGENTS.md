# AGENTS.md

Guidance for coding agents working in the arsan repository.

## What this is

`arsan` is a from-scratch Go implementation of a IaaS
control plane and a compute agent. It schedules VMs onto physical nodes and drives them through a
lifecycle via pluggable compute engines.

**One binary, three cobra subcommands:** `api`, `controller`, `agent`.

**Compute engines:** `fake` (in-memory, every platform), `kvm` (native
`libvirt.org/go/libvirt` bindings, Linux), `esxi` (VMware ESXi, future).

## Layout

```
main.go                  the only main package; calls cmd.Execute()
cmd/                     cobra subcommands (package cmd)
  root.go                root command, --config, viper wiring, shared helpers
  api.go                 `arsan api` — serves the versioned REST API
  controller.go          `arsan controller` — control plane loops, no HTTP
  agent.go               `arsan agent` — compute node agent (NATS worker)
api/                     versioned REST surface
  api.go                 Register(Config) *gin.Engine; mounts each version
  v1alpha1/              one directory per API version
    v1alpha1.go          version group + route table
    handler/             HTTP handlers for this version
internal/app             builds the object graph; both api and controller use it
internal/core            domain types + interfaces (dependency-free)
internal/bus             command bus (NATS JetStream)
internal/records         api-side CRUD layer; decides nothing
pkg/util                 shared helpers with no domain opinions
internal/scheduler       filters, weighers, claim
internal/compute         instance lifecycle state machine (build/stop/start/delete/migrate/sweep)
internal/store/memory    in-memory store (CAS transitions)
internal/store/gormdb    durable store (GORM; postgres or sqlite)
internal/driver          core.Driver registry + fake/kvm/remote
internal/agent           agent worker: NATS request/reply protocol + REST bootstrap client
```

## Key Principles

1. **`internal/core` is dependency-free** — domain types and interfaces only
2. **Interfaces at the consumer** — `core.Driver` implemented by engines, consumed by compute
3. **No engine types leak upward** — translate at boundaries
4. **State transitions are compare-and-swap** — `Transition(ctx, id, from, to)` validates `from`
5. **Filter → weigh → claim** — atomic reservation, lost claims retry (bounded)
6. **Errors are sentinels** — `errors.Is`-comparable, wrapped with `%w`
7. **Everything takes context** — timeouts on all I/O paths
8. **Idempotent engine ops** — Create/Start/Delete safe to retry

## Stack

| Concern | Library | Rule |
|---------|---------|------|
| CLI | `spf13/cobra` | subcommands in `cmd/`; `main.go` is a shim |
| Config | `spf13/viper` | precedence: flags > `ARSAN_*` env > config file > defaults |
| HTTP | `github.com/gin-gonic/gin` | REST API, versioned under `/v1alpha1` |
| Bus | NATS JetStream + core request/reply | commands are durable; engine calls are request/reply |
| Utils | `github.com/samber/lo` | slice/map helpers |
| Database | `gorm.io/gorm` + `gorm.io/driver/postgres`, `gorm.io/driver/sqlite` | PostgreSQL in production, SQLite for tests |
| Bus | `github.com/nats-io/nats.go` (JetStream) | api ↔ controller commands; no Kafka/RabbitMQ |
| Tests | stdlib only | `httptest`, no assertion lib |

**No Kafka, RabbitMQ or Redis.** The command bus is NATS JetStream
(`internal/bus`), chosen for acknowledged and persisted delivery rather than
throughput: `Publish` returns only once the server has stored the command, so a
`202` means "queued".

JetStream also lets `api` and `controller` be genuinely separate processes,
which is the production topology. The `bus.Bus` interface is the seam if that
ever needs replacing.

## Subcommand separation (enforced, not aspirational)

`api` and `controller` must never import each other's libraries. Anything the
api needs decided goes through the bus.

```
go list -deps ./api/... | grep cinvat
# must show only: api, api/v1alpha1, handler, internal/bus, internal/core, pkg/util
```

No `scheduler`, no `compute`, no `node`, no `driver`. If a change makes the api
depend on any of them, the change is wrong — move the decision behind a command.
This is a one-line check and it is worth running on every diff that touches
`api/` or `cmd/`.

## Agent bootstrap

An agent registers itself over **REST**, like a kubelet registering its Node
object. `POST /v1alpha1/nodes` with a shared bootstrap token and its identity
— nothing else. Capacity, engine and usage are fetched by the controller after
the probe succeeds, so the agent never describes a host it may not know yet.

There is one token, held in the api's configuration (`--bootstrap.token`).
No token table. Rotation is restarting the api. See `docs/business-logics.md`
§1.

**Registration does not wait for the ack.** The api upserts the node as
`NOTREADY`, publishes without blocking, and returns 200. That is safe because
`NOTREADY` is excluded from placement, so a lost announcement cannot put a VM on
a host nobody was told about. No outbox table is needed.

**Node states are `READY` and `NOTREADY`** only. The **controller** probes the
agent over NATS (`usage`, which round-trips through the engine) and sets
readiness from the answer; the api never sets it, and neither does the arrival of
a bootstrap command. Probing repeats so a node that stops answering drops back
out. A passing probe also fetches the engine and capacity (`info`) and records
them on the node.

`Node.NotReadyReason` says why — with one state covering every fault, an
operator needs the reason. It is cleared on recovery and left alone on a node
an operator disabled. Because two states cannot distinguish "stopped answering"
from "operator disabled", `Node.Enabled` carries that distinction and a probe
must not override it.

Agents have no addresses. The node id **is** the address (`agent.<node-id>`
on the bus), so there is nothing to configure, cache, or rediscover when an
agent moves. Do **not** add address flags: they would be a second copy of a
fact nobody needs to hold.


**The controller does not run a compute engine.** It has no libvirt and no local
KVM. Every VM lives on some agent; the controller's whole job is to pick which
agent and command it. So there is no `--driver` on `arsan controller`, and
`driver.New` is only ever called by an agent to build the engine it will run.

`internal/driver/remote` is not an engine. It implements `core.Driver` by
asking the agent over NATS, so the scheduler and compute service need not know
whether a VM is local or on another host. An earlier version did this over
gRPC; the transport moved to NATS request/reply so there is one transport to
run, secure, and reason about.

A node's engine and capacity are reported by the agent when the controller asks
(`info`, after the probe succeeds) and recorded on the node record. They never
travel at bootstrap. There is no controller-side engine default, because the
controller has no engine to default to.

**There is no heartbeat, and no timestamp-based liveness.** The agent
bootstraps once over REST and afterwards only answers on the bus. An earlier
design aged nodes out on `LastHeartbeat`; since nothing ever sent a heartbeat,
every node silently left service about 30s after registering. Readiness is the
probe, and only the probe.

`Store.RecordUsage` records what an agent reports it is using and must not write
`state`. Availability comes from reservations, never from a usage report, so a
stale report cannot cause an overcommit.

The agent's worker answers engine operations only. If you find yourself adding
identity or liveness to its protocol, the design has drifted: identity arrives
at bootstrap, liveness is the controller's probe.

## CLI conventions

**Flags live on the command that uses them.** Use `cmd.Flags()`, not
`PersistentFlags()` — a persistent flag on a subcommand leaks into every other
subcommand's help output and invites cross-command coupling. The only flag on
`rootCmd` is `--config`, because every subcommand reads a config file.

Each subcommand binds its own flags in `PreRunE`:

```go
PreRunE: func(cmd *cobra.Command, _ []string) error {
    return viper.BindPFlags(cmd.Flags())
},
```

Read values with the package-level getters — `viper.GetString("driver")`,
`viper.GetInt("scheduler.max-attempts")`, `viper.GetDuration(...)`. Do **not**
do `v := viper.GetViper()` and pass `v` around; it adds indirection and the
global getters are the intended API.

There is no config module. `viper` is used directly from `cmd/`.

## API versioning

`api/` is versioned by directory. `api.Register(cfg)` mounts each version onto
its own path group, so v1alpha1 is served at `/v1alpha1/...`.

- A **new version** means a new package (`api/v1beta1/`) registered in
  `api/api.go`. Never edit `v1alpha1` to change its shape.
- Handlers depend on the `core` interfaces, never on a concrete service.
- `internal/api/` does not exist. Do not create it; it duplicates `api/`.

## Build & Test

```sh
go build ./... && go vet ./... && go test ./...
# kvm engine (needs libvirt headers; Linux): go build -tags kvm ./...
go test -race ./internal/scheduler/... ./internal/store/...
go build -o arsan .
```

## Run

```sh
# REST API (CRUD + commands onto the bus; no placement, no engine)
./arsan api --addr=:8080 --bootstrap.token=s3cret

# Control plane loops only, no HTTP
./arsan controller

# Compute node agent: token in, everything else learned by asking.
./arsan agent --driver=fake -t s3cret
```

## Non-Negotiables

1. Never `Update` a struct read before `Transition` — use `Transition`'s return value
2. Failed re-placement must leave the instance `PENDING` with a bumped generation
3. `-race` on scheduler and store is required, not optional
4. All tests run against the `fake` engine — no KVM/libvirt/ESXi in tests
5. Do not add a config module, a second REST router, or a second main package
6. **ReserveNodeResources is one atomic statement.** In SQL the guard lives in
   the WHERE clause and RowsAffected == 0 is `ErrCapacityConflict`. Moving the
   check into Go reintroduces the overcommit.
7. **Bus handlers are idempotent.** JetStream redelivers anything not acked, and
   a crash between the side effect and the ack runs the handler again.
   Compare-and-swap transitions are what make a repeat a no-op.
8. **A failed bus publish is never a `202`.** The command was never queued, so
   the client gets `503` and can retry.
9. **Ambiguous business logic: propose and ask.** Do not pick an interpretation
   and encode it. `docs/business-logics.md` records the reasoning behind each
   decision so it does not have to be re-derived.
