<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mastmq/.github/main/assets/banner.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/mastmq/.github/main/assets/banner-light.png">
    <img src="https://raw.githubusercontent.com/mastmq/.github/main/assets/banner.png" alt="mast — multi-tenant MQTT broker built on core NATS" width="820">
  </picture>
</div>

<p align="center">
  <a href="https://mastmq.github.io/"><b>Website</b></a>
  &nbsp;·&nbsp; <a href="https://github.com/mastmq/mast">Broker</a>
  &nbsp;·&nbsp; <a href="https://github.com/mastmq/docs">Docs</a>
  &nbsp;·&nbsp; <a href="https://github.com/mastmq/charts">Helm charts</a>
  &nbsp;·&nbsp; <a href="https://github.com/mastmq/ansible">Ansible</a>
  &nbsp;·&nbsp; <a href="https://github.com/mastmq/bench">Benchmarks</a>
</p>

> **Status: early, but it runs.** A single `mast` process starts an embedded nats-server, terminates MQTT, and moves messages end to end with tenant isolation, wildcards, and shared subscriptions. QoS 0 through 2, retained messages, wills and persistent sessions all work, and a session follows a device between nodes and keeps collecting its messages when the edge it last used is restarted. QoS 1 and 2 hold across nodes too: a QoS 1 or 2 publish is stored on a replicated stream before it is acknowledged. Two limits are worth knowing before you read further: shared subscriptions spanning nodes are still best-effort, and a node that dies after it has taken a message can lose it, which includes what is published to a device still connected to an edge that is killed outright ([delivery guarantees](https://github.com/mastmq/docs/blob/main/guides/delivery-guarantees.md)). See [what works](#what-works).

## What works

Verified by the end-to-end tests in `internal/infra/broker`, which drive a real MQTT client against a real node, and by a manual three-process cluster of one `core` and two `edge` nodes:

- Publish and subscribe across the NATS fabric, including `+` and `#` wildcards
- `foo/#` matching `foo` itself, which needs the second subscription `FilterSubjects` opens
- Topics containing `.`, a leading `/`, empty levels, and non-ASCII, all arriving byte-identical
- **Tenant isolation** — two tenants subscribing to the same topic never see each other's traffic
- **Shared subscriptions** — `$share/<group>/<filter>` maps onto a NATS queue group, and each message reaches exactly one member. Note that a queue group serves a local member first, so work stays on the producer's node rather than spreading round-robin across the group; see [ARCHITECTURE.md](docs/ARCHITECTURE.md) for why and what to do if that matters
- One NATS subscription per distinct filter rather than per device, released when the last subscriber disconnects
- **Cross-node delivery** — edges join the core as leaf nodes, and a message published on one edge reaches a subscriber on another, with tenant isolation holding across the boundary. QoS 0 crosses on core NATS; QoS 1 and 2 are stored on one replicated JetStream stream before the publisher is acknowledged, and each node reads it through one consumer of its own, so a node cut off from the core for a moment catches up rather than losing what was published for its clients. [Delivery guarantees](https://github.com/mastmq/docs/blob/main/guides/delivery-guarantees.md) sets out exactly what each QoS promises and where it still does not

- **MQTT 5 message properties** — content type, response topic, correlation data, payload format, user properties and message expiry travel with the message across the fabric and through retained messages and offline queues, so MQTT 5 request/response works between nodes. An expired retained or queued message is not delivered
- **HTTP authentication and authorization** — post the MQTT fields to your own service and let it name the tenant and rule on each topic
- **Portable sessions** — subscriptions live in a shared bucket and the offline queue on a shared stream, filled by the core tier, so a device that reconnects to a different node finds its session and whatever QoS 1 or 2 traffic it missed, even when the edge it last used has since been restarted. Verified by a cluster test and by killing cores and edges under load in a three-core, three-edge cluster
- **Cross-node takeover** — a second connection with an existing client id disconnects the first wherever in the cluster it is, which MQTT requires and a single node cannot do alone. Client ids are scoped to their tenant, so one tenant cannot displace another's device by reusing an id

- **Per-tenant limits** — a connection limit counted across the cluster, a publish rate per node, and the size of each absent session's queue, with defaults in config, overrides per tenant, and overrides from the authentication backend. Over the limit a client is told with MQTT 5 reason codes, or the nearest thing MQTT 3.1.1 has; see [tenant limits](#tenant-limits)

Not yet: per-tenant caps on the rate of new connections, on inbound bytes and on stored sessions and retained messages, full QoS for shared subscriptions whose members span nodes, and queueing for a connected device between its edge being killed outright and its reconnecting elsewhere. Each of these has an issue, and the [roadmap](ROADMAP.md) lists them with the rest of what comes next.

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

**The topic codec is reversible.** `internal/domain/topic` maps MQTT topics onto NATS subjects of the form `t.<tenant>.<level>.<level>...`, escaping everything outside `[A-Za-z0-9_-]` as `=` plus two hex digits and writing an empty level as a bare `=`. The escape is `=` rather than `%` because a subject is also used as a JetStream KV key, and a KV key may not contain `%`. Every legal MQTT topic round-trips byte-identical, which is the property that makes the mapping safe to bake into stored subjects, authorization rules, and retained-message keys. This is why mast does not reuse the mapping nats-server applies to its own MQTT listener: that one turns a `.` into `//` and gives a leading `/` an empty token, and it is not reversible. A fuzz test guards the round-trip property and runs in CI.

## Quick start

```console
$ docker run -p 1883:1883 ghcr.io/mastmq/mast     # all-in-one, one tenant, every topic allowed
```

Or from source, which is the same binary:

```console
$ go build ./cmd/mast
$ ./mast                      # all-in-one: MQTT and storage in one process
$ ./mast config show          # the resolved configuration as JSON
$ ./mast --role=core          # Raft and the KV buckets; wants a volume
$ ./mast --role=edge          # MQTT only, joins the cores as a leaf node
```

The default `static` auth mode admits everyone into one tenant, which is right for a laptop and wrong for anything reachable by someone else; see [authentication and authorization](#authentication-and-authorization). For a real deployment use the [Helm chart](https://github.com/mastmq/charts) on Kubernetes, or the [Ansible collection](https://github.com/mastmq/ansible) on virtual machines and bare metal, which installs the release tarballs under systemd.

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

## Tenant limits

Three limits, under `[limits]` in the config. Each one's scope is where it can be enforced without a storage write on the hot path:

| Limit | Scope | Over the limit |
| --- | --- | --- |
| `max_connections` | cluster-wide, approximate | CONNACK 0x97 Quota exceeded (MQTT 5), 0x03 Server unavailable (3.1.1) |
| `publish_rate`, `publish_burst` | per tenant, per node | QoS 0 dropped; MQTT 5 QoS 1 gets PUBACK 0x96 Message rate too high; MQTT 5 QoS 2 and 3.1.1 QoS 1/2 are disconnected |
| `offline_queue` | per absent session | the oldest queued messages are discarded |

`offline_queue` defaults to 100 and applies with no `[limits]` section at all. Before limits existed, a device that came back to the node it left was resent everything mochi held for it, up to mochi's inflight maximum of thousands, while a device that came back on another node got the stream's newest 100. Now both get the newest 100. Set `offline_queue` higher, under `[limits]` or for one tenant, to keep more.

The connection count is shared by every node publishing its per-tenant counts on core NATS every `sync_interval` (2s), so connections arriving on several nodes within one interval can overshoot the limit by that many. Nothing goes through JetStream: a strict count would put a replicated write on every CONNECT, which is what a reconnect storm cannot pay. A device taking over its own client id on the same node is always admitted.

`[limits.tenants.<id>]` overrides the defaults field by field. The authentication backend can override both, because it is the system that knows what each tenant has paid for: a `limits` object in the HTTP reply, or the JWT claim named by `auth.jwt.limits_claim`, with the same keys:

```json
{"allow": true, "tenant": "acme", "limits": {"max_connections": 5000, "publish_rate": 200, "offline_queue": 50}}
```

The newest statement wins, per tenant per node. A reply or token that says nothing about limits leaves the tenant's overrides as they were; it is not a statement that the tenant has none. A JWT's `iat` orders it against the tenant's other devices' tokens, so a device still holding a token from before the tenant's plan changed cannot put the old limits back; a token without `iat` never replaces one with it. An HTTP reply is dated when it arrives. Once set, an override from authentication stays until a newer one replaces it, so lifting it takes a newer token or reply that states the configured value. Because each node keeps what it last heard, two nodes can enforce different limits for a tenant until each has seen a connection with the newest token. An override can lower a tenant's offline queue but not raise it past the largest value in the config, which is what the core tells the stream to keep per session. `mast_limit_exceeded_total{tenant, limit}` counts what each limit refused, and `obs.max_tenant_labels` (1000) bounds how many tenants get a label of their own; the rest are counted as `~other`.

## Configuration

Defaults, then an optional TOML file, then environment variables, then explicitly-set flags. See [`configs/config.example.toml`](configs/config.example.toml) — every value in it is the built-in default, so an empty file behaves exactly like no file.

Environment variables are prefixed `MAST__` and nest with a double underscore, so `MAST__MQTT__ADDR=":8883"` sets `mqtt.addr`.

## Health and metrics

The observability listener (`obs.addr`, `127.0.0.1:9090` by default) serves `/metrics`, `/healthz`, `/readyz` and `/debug/pprof`.

`/healthz` is liveness: it answers 200 for as long as the process runs and answers. `/readyz` answers 200 only when the node can serve, and 503 with the reason when it cannot. An edge is ready when it is serving MQTT and its leaf connection to the core is up; a core or an all-in-one node is ready when its embedded nats-server and JetStream are, by nats-server's own health check. Point a readiness probe at `/readyz` and a liveness probe at `/healthz`, never the other way round: an edge whose core is away is not fixed by a restart, and a liveness probe on `/readyz` would restart every edge in the fleet whenever the core tier is.

## Verifying a release

From v0.3.2, every image pushed to `ghcr.io/mastmq/mast` is signed keylessly with cosign, from the workflow that built it, and carries an SBOM and SLSA provenance. Check the signature of a release image against the identity of the workflow and the tag:

```console
$ cosign verify ghcr.io/mastmq/mast:0.3.2 \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    --certificate-identity https://github.com/mastmq/mast/.github/workflows/docker.yaml@refs/tags/v0.3.2
$ docker buildx imagetools inspect ghcr.io/mastmq/mast:0.3.2 --format '{{ json .SBOM }}'
$ docker buildx imagetools inspect ghcr.io/mastmq/mast:0.3.2 --format '{{ json .Provenance }}'
```

From the same release, the archives and the checksum file carry GitHub build provenance. Verify a downloaded file with the GitHub CLI:

```console
$ gh attestation verify mast_0.3.2_linux_amd64.tar.gz -R mastmq/mast
$ sha256sum -c --ignore-missing mast_0.3.2_checksums.txt
```

## Development

```console
$ just            # list recipes
$ just test       # go test -race with coverage
$ just fuzz 60s   # fuzz the topic codec
$ just lint       # golangci-lint
```

Questions go to [Discussions](https://github.com/mastmq/mast/discussions). [CONTRIBUTING.md](https://github.com/mastmq/.github/blob/main/CONTRIBUTING.md) covers building and testing every repository in the org, commits and style; [SECURITY.md](https://github.com/mastmq/.github/blob/main/SECURITY.md) covers reporting a vulnerability privately. What is planned is in the [roadmap](ROADMAP.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
