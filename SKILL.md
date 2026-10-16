# arsan agent skill

How to work in this repo without breaking its invariants. AGENTS.md is
normative; this file is the day-to-day checklist. When they disagree,
AGENTS.md wins.

## Use samber/lo everywhere able

`github.com/samber/lo` is the house style for slice and map operations.
Reach for it first:

- filter / reject: `lo.Filter`, `lo.Reject` (capacity checks, readiness lists)
- membership: `lo.Contains`, `lo.ContainsBy` (idempotency guards, dedup)
- find-or-fail: `lo.Find` (returns the zero value plus `false`)
- transform: `lo.Map` (rendering, conversions like rows to domain types)
- count / verify: `lo.CountBy`, `lo.EveryBy`
- best-effort cleanup: `lo.ForEach` with ignored errors (detach on lost claims)
- defaults: `lo.Ternary`, `lo.CoalesceOrEmpty` (namespace and display fallbacks)

Do NOT use it where it hides control flow: loops with early `return` on
error, transactions, anything where the error value matters, and test files
(which stay literal on purpose). A plain loop with side effects is better
than a clever one-liner that swallows a failure.

## Before every diff

```sh
go build ./... && go vet ./... && go test ./...
go test -race ./internal/scheduler/... ./internal/store/...
go build -o arsan .
```

`api/` must never import the controller's libraries:

```sh
go list -deps ./api/... | grep cinvat
# allowed: api, api/v1alpha1, handler, internal/bus, internal/core, pkg/util
```

## Non-negotiables (from AGENTS.md, repeated because they bite)

1. State changes go through `Transition` (compare-and-swap on `from`).
2. Failed re-placement leaves the instance `PENDING` with a bumped
   generation — never `ERROR` for "no room yet".
3. `ReserveNodeResources` stays one atomic SQL statement; the guard lives
   in the WHERE clause.
4. Bus handlers are idempotent: redelivery after a crash must be a no-op.
5. A failed bus publish is never a `202`.
6. Ask before encoding ambiguous business logic; record the reasoning in
   `docs/business-logics.md`.

## Layout cheat sheet

One binary, three subcommands (`api`, `controller`, `agent`). `internal/core`
is dependency-free types only. `compute` drives the instance state machine;
`scheduler` filters → weighs → claims; `driver` engines implement
`core.Driver` (fake everywhere, kvm on Linux). Stores: memory (dev/tests)
and gormdb (postgres/SQLite). Manifests live in `arsanctl/examples/`.
