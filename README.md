# arsan

From-scratch Go implementation of a IaaS control plane. Schedules VMs onto physical nodes and drives them through a lifecycle via pluggable compute engines.

One binary, three subcommands: `api`, `controller`, `agent`.

## Architecture

```
POST /v1alpha1/virtualmachines → api writes PENDING → NATS → 202 ("queued, not running")
controller consumes → schedule (filter → weigh → claim) → NATS request → agent's engine → libvirt
```

- **api** — REST only (`/v1alpha1`). CRUD + publish commands. No scheduler, no engine.
- **controller** — consumes NATS commands, probes agents, places VMs. No HTTP, no engine of its own.
- **agent** — runs on each compute node. Owns the real engine (`fake`, `kvm`). Bootstraps over REST with only a token, answers on the bus.

Terms follow Kubernetes: a **Node** is the machine (identity defaults to hostname), an **Agent** is the process on it, an **Engine** is what creates VMs.

## Quickstart

```sh
go build -o arsan .

# NATS (command bus)
/opt/homebrew/bin/nats-server -js &

# API (holds the bootstrap token)
./arsan api --addr=:8080 --bootstrap.token=s3cret

# Controller
./arsan controller

# Agent (declares its own engine)
./arsan agent --driver=fake -t s3cret

# Create a VM
curl -X POST localhost:8080/v1alpha1/virtualmachines \
  -H 'content-type: application/json' \
  -d '{"name":"vm1","image_ref":"cirros","flavor":{"name":"m1","vcpu":1,"ram":2,"disk":10}}'
```

## Docs

- `AGENTS.md` — rules for working in this repo
- `docs/business-logics.md` — design decisions and why
- `docs/tutorial/` — step-by-step build guide
