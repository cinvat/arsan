# Quickstart

One binary, three processes, one transport (NATS). Start NATS first — everything
else dials out to it.

```sh
go build -o arsan .
nats-server -js &

./arsan api --addr=:8080 --bootstrap.token=s3cret &
./arsan controller &
./arsan agent --driver=fake -t s3cret &
```

## Sequence 1: an agent joins

```
agent ──POST /v1alpha1/nodes {token, name}──▶ api
api: token ok → upsert n-<name> as NOTREADY ("awaiting first probe") → 200
api ──node.registered──▶ bus ──▶ controller
controller: probe agent.<id> (usage) ──▶ answers?
  yes → fetch info (engine, capacity) → record spec → READY
  no  → NOTREADY + reason (timeout is the whole failure model)
```

The agent sends nothing but the token and its name. Capacity, engine and usage
are fetched after the probe succeeds. Check:

```sh
curl -s localhost:8080/v1alpha1/nodes
```

## Sequence 2: a VM is created

```
you ──POST /v1alpha1/servers──▶ api
api: validate → write PENDING → publish instance.create → 202 ("queued", not running)
controller: filter (READY+enabled → capacity → arch) → weigh → claim (atomic)
  → NATS create → agent's engine defines the domain
  → NATS start → boots it → ACTIVE
```

```sh
curl -X POST localhost:8080/v1alpha1/servers \
  -H 'content-type: application/json' \
  -d '{"name":"vm1","image_ref":"cirros","flavor":{"name":"m1","vcpu":1,"ram":2,"disk":10}}'

curl -s localhost:8080/v1alpha1/servers
```

If placement fails, the instance lands in ERROR and the claim is released. If a
build dies mid-flight (Scheduling/Building unmodified for 2 minutes), the
sweep resets it to PENDING and re-drives the build itself.

## Sequence 3: lifecycle

```
stop/start/delete/migrate → api validates → command on the bus → 202
controller: compare-and-swap the state (ACTIVE→SHUTOFF, …) → NATS op → agent's engine
```

Delete releases the reservation before removing the row, so capacity comes
back. A command for a missing instance acks (work already done); an illegal
transition parks with a log instead of redelivering forever.

## Sequence 4: a node goes quiet

The probe runs every 15s. A node that stops answering drops READY → NOTREADY
with the timeout as the reason — no timestamps, no heartbeats. Draining is the
operator's version of the same: disable the node and no probe may undo it.

## Real KVM

```sh
# Linux with libvirt: install headers once, then build with the tag.
go build -tags kvm ./...
./arsan agent --driver=kvm -t s3cret
```

`image_ref` must name a qcow2 file already on the host. The engine reports its
own capacity, so there are no `--node.vcpu`-style flags.
