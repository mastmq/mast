<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mastmq/.github/main/assets/banner.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/mastmq/.github/main/assets/banner-light.png">
    <img src="https://raw.githubusercontent.com/mastmq/.github/main/assets/banner.png" alt="mast — multi-tenant MQTT broker built on core NATS" width="820">
  </picture>
</div>

> **Status: early, but it runs.** A single `mast` process starts an embedded nats-server, terminates MQTT, and moves messages end to end with tenant isolation, wildcards, and shared subscriptions. Persistence is not wired yet, so QoS is 0 across the bridge, sessions do not survive a restart, and retained messages are node-local. See [what works](#what-works).

## What works

Verified by the end-to-end tests in `internal/infra/broker`, which drive a real MQTT client against a real node, and by a manual three-process cluster of one `core` and two `edge` nodes:

- Publish and subscribe across the NATS fabric, including `+` and `#` wildcards
- `foo/#` matching `foo` itself, which needs the second subscription `FilterSubjects` opens
- Topics containing `.`, a leading `/`, empty levels, and non-ASCII, all arriving byte-identical
- **Tenant isolation** — two tenants subscribing to the same topic never see each other's traffic
- **Shared subscriptions** — `$share/<group>/<filter>` maps onto a NATS queue group, and each message reaches exactly one member. Note that a queue group serves a local member first, so work stays on the producer's node rather than spreading round-robin across the group; see [ARCHITECTURE.md](docs/ARCHITECTURE.md) for why and what to do if that matters
- One NATS subscription per distinct filter rather than per device, released when the last subscriber disconnects
- **Cross-node delivery** — edges join the core as leaf nodes, and a message published on one edge reaches a subscriber on another, with tenant isolation holding across the boundary

- **HTTP authentication and authorization** — post the MQTT fields to your own service and let it name the tenant and rule on each topic

Not yet: QoS 1 and 2 across the bridge, persistent sessions, offline queues, cluster-wide retained messages, and per-tenant quotas. Those all need the KV-backed session store, which is the next piece.

## Why

Every open-source MQTT broker that does real multi-tenancy makes you give something up. EMQX adopted the Business Source License in 5.9, and forming a cluster from multiple nodes now requires a paid license key. BifroMQ has genuine native multi-tenancy but runs on the JVM. VerneMQ's source is Apache 2.0 while its official packages and Docker images are governed by a EULA that charges for commercial use. RabbitMQ gives you vhosts but supports neither QoS 2 nor shared subscriptions. Mosquitto, NanoMQ, FlashMQ and mochi-mqtt do not cluster at all.

mast is the combination that does not currently exist off the shelf: MQTT 5 with shared subscriptions, tenant isolation with per-tenant limits, free clustering, no JVM, and a single binary you can also run standalone at a customer site.

## Design

**Core NATS does routing. A key-value store does keys.** Live fanout rides core NATS, which handles tens of millions of subjects at roughly 1GB of memory per million subscriptions. Durable state — sessions, retained messages, bounded offline queues — lives in a handful of JetStream KV buckets. What mast deliberately never does is create a JetStream consumer per subscription per session: consumers are Raft state machines, a server holds on the order of 2k of them, and 300k devices would need hundreds of thousands. Persistent state scales as keys, not as consensus groups.

**One binary, three roles.** The MQTT tier scales with connection count and restarts on every deploy; the storage tier wants stable Raft peers and a volume. Coupling them means Raft membership churn on every autoscale event. Separating them into different binaries means operating two things. mast separates them into roles instead.

| Role | Job | Shape |
| --- | --- | --- |
| `all-in-one` | both, file-backed, no cluster | default; an edge site or a laptop |
| `core` | Raft, JetStream KV, no MQTT listeners | 3–5 stable pods with volumes |
| `edge` | MQTT listeners, joins the core as a leaf node | stateless, autoscaled |

You do not need the split until you are big. Up to roughly ten nodes, run every process identical and let them mesh.

**The topic codec is reversible.** `internal/domain/topic` maps MQTT topics onto NATS subjects of the form `t.<tenant>.<level>.<level>...`, percent-escaping anything NATS reserves and writing an empty level as `%`. Every legal MQTT topic round-trips byte-identical, which is the property that makes the mapping safe to bake into stored subjects, authorization rules, and retained-message keys. This is why mast does not reuse the mapping nats-server applies to its own MQTT listener: that one turns a `.` into `//` and gives a leading `/` an empty token, and it is not reversible. A fuzz test guards the round-trip property and runs in CI.

## Quick start

```console
$ go build ./cmd/mast
$ ./mast config show          # resolved configuration as JSON
$ ./mast --role=core          # not runnable yet; reports the plan and exits
```

## Authentication and authorization

mast offers two backends, and they compose: authentication can be a callback or a locally verified token, while authorization independently goes to a policy service or nowhere.

| | `auth.mode = "http"` | `auth.mode = "jwt"` |
| --- | --- | --- |
| Cost per CONNECT | a network round trip | a signature check |
| Reconnect storm | your service absorbs it | nothing to absorb |
| Revocation | immediate | when the token expires |
| Needs | a reachable service | key distribution |

Neither is better. A fleet that reconnects in bursts wants JWT; a deployment that must revoke a device now wants HTTP. Most end up verifying tokens locally and still asking a policy service about topics, which is what setting `auth.mode = "jwt"` alongside `auth.http.authz_url` does. See [`configs/config.jwt.toml`](configs/config.jwt.toml).

### Callback

Set `auth.mode = "http"` and mast asks your service both questions over plain JSON.

On CONNECT it posts the connect fields to `authn_url`:

```json
{"client_id": "dev-1", "username": "u", "password": "p", "remote_addr": "10.0.0.1:52000", "protocol_version": 5, "clean_start": true}
```

and expects a tenant back:

```json
{"allow": true, "tenant": "acme"}
```

That tenant is authoritative for the whole connection — it is what every topic is mounted under and what every later authorization question carries — so a client can never influence it again.

On each publish and subscribe it posts to `authz_url`:

```json
{"tenant": "acme", "client_id": "dev-1", "username": "u", "remote_addr": "10.0.0.1:52000", "topic": "a/b", "action": "publish"}
```

and expects `{"allow": true}`. Leave `authz_url` empty and an authenticated connection may use any topic inside its own tenant, which is a coherent posture when the tenant mount is boundary enough.

### Tokens

Set `auth.mode = "jwt"` and mast verifies the token itself. `auth.jwt.algorithms` is required and has no default: accepting whatever algorithm a token asks for is how `alg: "none"` and RSA-to-HMAC confusion attacks work, so the allowlist is mandatory rather than inferred. An expiry claim is also required — a bearer credential that never expires cannot be revoked by a broker that only checks signatures.

The tenant comes from `auth.jwt.tenant_claim`, and `auth.jwt.superuser_claim` bypasses authorization the way EMQX's `is_superuser` does.

### Both

Two things worth knowing before you point this at production. Authorization is asked **on every publish**, so it is cached with a TTL — set `cache_ttl = "0s"` if decisions must take effect instantly, and accept a network round trip per message. And authentication **always fails closed** whatever `on_error` says, because admitting a connection whose tenant is unknown would mean inventing an isolation boundary; `on_error` governs authorization only.

## Configuration

Defaults, then an optional TOML file, then environment variables, then explicitly-set flags. See [`configs/config.example.toml`](configs/config.example.toml) — every value in it is the built-in default, so an empty file behaves exactly like no file.

Environment variables are prefixed `MAST__` and nest with a double underscore, so `MAST__MQTT__ADDR=":8883"` sets `mqtt.addr`.

## Development

```console
$ just            # list recipes
$ just test       # go test -race with coverage
$ just fuzz 60s   # fuzz the topic codec
$ just lint       # golangci-lint
```

## License

Apache 2.0. See [LICENSE](LICENSE).
