# mast

Multi-tenant MQTT broker built on core NATS. One binary, one artifact, from a single edge box to a clustered fleet.

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
