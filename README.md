# mast

Multi-tenant MQTT broker built on core NATS. One binary, one artifact, from a single edge box to a clustered fleet.

> **Status: early.** The topic codec, configuration, and command tree are in place and tested. The runtime — embedded nats-server, the mochi-mqtt bridge, and the KV-backed session store — is not written yet. `mast` currently resolves its configuration, reports the role it would run, and exits.

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
