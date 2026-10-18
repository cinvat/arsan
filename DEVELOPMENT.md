# Developing an apiService for kloudapi

kloudapi is an aggregation origin: it fronts versioned downstream IaaS APIs
(arsan is the reference) behind one address. To sit behind it, a service
implements two contracts:

1. **Routing** — it owns a path prefix and serves every path beneath it.
   kloudapi forwards requests verbatim and never rewrites what it does not
   understand.
2. **Discovery** — it serves an `APIResourceList` naming its kinds, so
   clients can turn a manifest's `apiVersion`/`kind` into a collection path
   without inventing a mapping.

Nothing else is required. kloudapi stores no records, makes no decisions,
and adds no schema of its own; CRUD and judgement stay in your service.

Shared types live in `api/core/v1` (`TypeMeta`, `ObjectMeta`, `APIResource`,
`APIResourceList`); import them instead of redefining the contract.

The examples in this repo are the contract made concrete:

- `apiservices.example.yaml` — how the origin registers you
- `examples/apiresourcelist.json` — the discovery document you serve
- `examples/*.yaml` — manifests the CLI sends

## 1. Register with the origin

The operator lists your service in the `--apiservice` file:

```yaml
apiservices:
  - name: arsan
    versions: [v1alpha1]
    prefix: /
    target: http://127.0.0.1:8080
```

| Field | Meaning |
|-------|---------|
| `name` | Logical service name; appears in `/apis` and ties discovery to routing. |
| `versions` | API versions you serve, e.g. `[v1alpha1]`. Discovery is fetched per version. |
| `prefix` | Path prefix you own. `/` is the transparent fallback; longer prefixes win. |
| `target` | Origin requests are proxied to, `http(s)://host:port`. No trailing path. |

A duplicate prefix replaces the earlier entry (last one wins). The origin
reads this file once at startup; changing it needs a restart.

## 2. Routing contract

- **You own every path under your prefix.** kloudapi forwards path, query and
  body byte-for-byte. If a request reaches you, the path is exactly what the
  client sent.
- **Longest prefix wins.** `/` is always the fallback, never a shadow. A
  group mounted at `/billing` receives `/billing/...` untouched.
- **You must serve your mount path.** A group that answers `/apis` but not its
  prefix is a registration bug, not something kloudapi papers over.
- **The group and version live in the path**, not in a header or body. arsan
  at prefix `/` serving `v1alpha1` exposes everything under `/v1alpha1/...`.

## 3. Discovery contract

Serve a Kubernetes-style `APIResourceList` (JSON) at your **groupVersion
path**: your mount prefix joined with the version.

| Prefix | Version | GroupVersion path you serve |
|--------|---------|-----------------------------|
| `/` | `v1alpha1` | `/v1alpha1` |
| `/billing` | `v1` | `/billing/v1` |

Response shape (`examples/apiresourcelist.json`):

```json
{
  "groupVersion": "v1alpha1",
  "resources": [
    {"name": "nodes", "singularName": "node", "namespaced": false,
     "kind": "Node", "verbs": ["get", "list", "watch"]},
    {"name": "virtualmachines", "singularName": "virtualmachine",
     "namespaced": true, "kind": "VirtualMachine",
     "verbs": ["get", "list", "watch", "create", "patch", "delete"]}
  ]
}
```

| Field | Meaning |
|-------|---------|
| `name` | Plural resource name; the URL segment for the collection. |
| `singularName` | Singular form, for client aliases (`virtualmachine`). |
| `kind` | The `kind:` a manifest uses; the join key for clients. |
| `namespaced` | Whether records carry a `Namespace` (i.e. `metadata.namespace`). |
| `verbs` | Advisory list of supported operations. |

kloudapi fetches this at startup and folds the kinds into `/apis`. Discovery
is **best-effort**: a group that does not answer still routes; it is just
absent from `/apis` resources, and the origin warns at startup. It is not
proxied per request, so a resource-list change needs a restart.

Routing still never keys on `kind`; this document only tells clients where
each kind lives.

## 4. URL layout

Given a resource `name` from discovery, clients address it as
`<groupVersion>/<name>`:

| Operation | Method + path |
|-----------|---------------|
| List | `GET /v1alpha1/virtualmachines` |
| Create | `POST /v1alpha1/virtualmachines` |
| Get one | `GET /v1alpha1/virtualmachines/{id}` |
| Patch | `PATCH /v1alpha1/virtualmachines/{id}` |
| Delete | `DELETE /v1alpha1/virtualmachines/{id}` |
| Watch | `GET /v1alpha1/virtualmachines?watch=true` |

Subresources are yours to define (arsan uses
`/virtualmachines/{id}/volumes`, `/virtualmachines/{id}/boot` and
`/virtualmachines/{id}/reboot`). kloudapi passes them through unchanged.

arsan also exposes a VNC console over WebSocket at
`/virtualmachines/{id}/console`, bridging to the VM's VNC port on its node.

## 5. Object envelope

Every object is a Kubernetes-style envelope. kloudapi never rewrites the
body, so the shape you write is the shape the client decodes.

```json
{
  "apiVersion": "v1alpha1",
  "kind": "VirtualMachine",
  "metadata": {
    "name": "web1",
    "namespace": "demo"
  },
  "spec": {
    "image": "https://example.com/base.qcow2",
    "flavorRef": "m1.small",
    "volumes": []
  },
  "status": {
    "id": "b3f1...",
    "state": "Running",
    "nodeId": "n1",
    "flavor": {"name": "m1.small", "vcpu": {"sockets": 1, "cores": 1, "threads": 1}, "memory": "64MB"}
  }
}
```

| Branch | Owner | Notes |
|--------|-------|-------|
| `apiVersion` / `kind` | client writes, server echoes | Identifies the type; `kind` is the manifest's `kind`. |
| `metadata.name` | client writes (required) | Human-readable address; unique within a namespace for namespaced kinds. |
| `metadata.namespace` | client writes (namespaced kinds) | Empty for cluster-scoped kinds; defaults to `default` in the CLI. |
| `spec` | client writes | Desired state; you define its fields. |
| `status` | server writes | Observed state, assigned ids, reasons; clients must not send it on create. |

## 6. Payload shapes

| Case | Expected response |
|------|-------------------|
| List | A JSON **array** of envelope objects, HTTP 200. Not `{"items": [...]}`. |
| Get / create | A single envelope object. |
| Patch | `PATCH` with `application/merge-patch+json`; any 2xx, body ignored or echoed. |
| Delete | Any 2xx; body ignored. |
| Error | A 4xx/5xx; kloudapi forwards the status and body as-is. |

Creation is `POST` with an envelope body (`apiVersion`, `kind`, `metadata`,
`spec`) and returns the created object with its `status` filled in. The body
shape is yours beyond the envelope; kloudctl builds it per kind from the
manifest's `spec` plus `metadata` (`name`, `namespace`). Field names in
responses drive display.

## 7. Watch protocol

Watch is one endpoint returning **NDJSON**: one JSON object per line,
newline-terminated and flushed, held open for the life of the watch.

```
GET /v1alpha1/nodes?watch=true
```

Each line is keyed by the **collection name** (the plural from discovery),
and the values are full envelope objects:

```json
{"nodes":[{"apiVersion":"v1alpha1","kind":"Node","metadata":{"name":"n1"},"status":{"state":"Ready"}}]}
{"nodes":[{"apiVersion":"v1alpha1","kind":"Node","metadata":{"name":"n1"},"status":{"state":"NotReady"}}]}
```

- Level-triggered: send a snapshot when state changes, nothing while idle.
- Sending an initial snapshot is expected.
- Keep the connection open; do not close after one line.
- kloudapi streams the response through untouched, so a `Flush()` after each
  line is what makes it live.

## 8. Namespaces

- A `namespaced: true` resource carries `metadata.namespace` on every record.
- An empty or absent `metadata.namespace` is treated as `default` by the CLI.
- `namespaced: false` resources (nodes, flavors) must not carry
  `metadata.namespace`; the CLI lists them whole.

## 9. HTTP and transport

- kloudapi proxies to you over **HTTP/1.1** (stdlib transport). Accept
  HTTP/1.1; h2c is optional. For watch, support a long-lived streaming
  response.
- kloudctl talks to kloudapi over prior-knowledge **h2c HTTP/2**; you never
  speak to the CLI directly.
- No TLS, auth, or rewriting is required in v1. Credentials pass through
  untouched; auth, if it comes, is its own layer.
- Never synthesize success for a downstream failure — a failed apiService is a
  `502/503`, and honest errors beat hidden ones.

## 10. Display (optional)

kloudctl renders curated columns for the kinds it knows (`Node`, `Flavor`,
`Volume`, `VirtualMachine`); any other kind gets columns inferred from the
JSON keys of the first object. Matching these conventional field locations
keeps curated output working:

| Kind | Fields the CLI reads |
|------|----------------------|
| All | `metadata.name`, and `metadata.namespace` when namespaced |
| Node | `status.state`, `status.driver`, `status.ip`, `status.total`/`status.available` (`cpu`,`ram`,`disk`), `status.pinnedCpus`, `status.notReadyReason`, `status.enabled` |
| Flavor | `spec.vcpu` (`sockets`,`cores`,`threads`,`dedicated`), `spec.memory` (MiB/GB) |
| Volume | `spec.size`, `spec.image`, `spec.cdrom`, `status.state`, `status.attachedTo`, `status.deleteOnTermination` |
| VirtualMachine | `status.state`, `status.nodeId`, `status.imageRef`, `status.flavor`, `status.volumes`, `status.pinnedCpus`, `status.pendingReason`, `status.failureReason` |

The internal address is `status.id` (a UUID the server assigns); users type
`metadata.name`, and the CLI resolves it to `status.id` for routing.

## 11. Checklist

1. Serve your mount path and everything under it.
2. Serve `GET <prefix>/<version>` returning an `APIResourceList`.
3. Address collections at `<groupVersion>/<name>`; return JSON arrays for
   lists and envelope objects for items.
4. Stream `GET <collection>?watch=true` as flushed, line-per-snapshot NDJSON
   keyed by the collection name, with full envelopes.
5. Accept `POST`/`PATCH` envelopes and return the server-filled envelope.
6. Register the group in the `--apiservice` file and restart the origin.

```sh
# from the repo root, with your service on :8080
./kloudapi --addr=:8081 --apiservice apiservices.example.yaml
curl -s localhost:8081/apis                 # your group, and its resources
curl -s localhost:8081/v1alpha1             # your APIResourceList
curl -sN localhost:8081/v1alpha1/nodes?watch=true   # your NDJSON stream
(cd kloudctl && go run . get nodes)
(cd kloudctl && go run . api-resources)     # the kinds the origin routes
```
