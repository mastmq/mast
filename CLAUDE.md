# mast

A multi-tenant MQTT broker built on core NATS. One Go binary that embeds `nats-server`, terminates MQTT with mochi-mqtt, and moves messages between the two.

Apache 2.0, org `mastmq`, module `github.com/mastmq/mast`, Go 1.27.

## The constraint that overrides everything

**Never create a JetStream consumer per subscription, per session, or per device.**

JetStream consumers are Raft state machines. A NATS server holds on the order of 2k HA assets. `nats-server`'s own MQTT implementation creates a durable consumer per subscription in every persistent session, which at the 300k-device target would need ~600k consumers — two to three orders of magnitude past what the design supports. That single fact is why mast exists as a frontend rather than as `nats-server` configuration.

The rule that falls out: **live delivery rides core NATS; durable state scales as keys, not as consensus groups.** Sessions, retained messages and offline queues live in a handful of JetStream KV buckets. An offline device's queue is read on reconnect with a direct get — no consumer is created, ever.

If a change you are about to make adds a consumer on a per-client or per-subscription path, it is the wrong change. Say so rather than making it.

## Layout and import direction

Follows golang-standards/project-layout. The three `internal/` trees have strict directionality:

| Tree | May import | Contains |
| --- | --- | --- |
| `internal/cmd` | `domain` and `infra` | the urfave/cli v3 command tree |
| `internal/infra` | `domain` only | everything that touches the network, disk, clock or a third-party client |
| `internal/domain` | **nothing else in this project** | pure logic, unit-testable with no I/O |

Litmus test for `domain`: can the package be tested with no network, no filesystem, no clock, no external client? If a file in `domain` needs `nats-io`, `mochi-mqtt` or `net/http`, it is misfiled and belongs in `internal/infra/<name>`.

### Package map

| Package | Job |
| --- | --- |
| `domain/topic` | the MQTT-topic ↔ NATS-subject codec. The most safety-critical package in the repo |
| `domain/tenant` | `ID`, `Credentials`, `Identity`, and the `Resolver` / `Policy` interfaces |
| `infra/bridge` | the mochi-mqtt hook that is the whole broker. `bridge.go` is ~770 lines; `subs.go` is the refcounted NATS subscription registry |
| `infra/broker` | assembles a node from natsd + store + bridge + mqttd. Home of the end-to-end tests |
| `infra/natsd` | the embedded `nats-server` and the in-process `nats.Conn` |
| `infra/store` | the JetStream KV wrapper: retained, sessions, offline queues |
| `infra/mqttd` | the mochi-mqtt server, listeners and TLS |
| `infra/auth` | picks the backend named in config and returns a `Resolver` + `Policy` |
| `infra/httpauth` | the callback backend, with a TTL cache. Speaks both the mast wire and the EMQX v5 wire |
| `infra/jwtauth` | local token verification |
| `infra/config` | koanf: defaults → TOML → env → flags |
| `infra/obs` | Prometheus metrics, health, pprof |

## Invariants

These are properties the tests and the docs both depend on. Breaking one is a data-loss or isolation bug, not a style question.

**The topic codec is total and reversible.** Every legal MQTT topic round-trips through `EncodeTopic` / `DecodeTopic` byte-identical. This is what makes the mapping safe to bake into stored subjects, authorization rules and retained-message keys. `FuzzRoundTrip` guards it and runs in CI on every push. A change here that breaks the round trip silently corrupts stored state.

**An encoded subject is also a valid JetStream KV key.** That is why the escape character is `=` and the allowlist is `[A-Za-z0-9_-]` — a KV key may only contain `[-/_=.a-zA-Z0-9]`. One encoding instead of two that must agree. Do not "improve" it to `%`.

**The tenant comes from authentication, never from the wire.** `OnACLCheck` validates against the tenant resolved at CONNECT. A client can never influence its own tenant after that point, and there is no code path where a topic, a header or a property may name one.

**Authentication always fails closed**, whatever `auth.http.on_error` says. Admitting a connection whose tenant is unknown means inventing an isolation boundary. `on_error` governs authorization only.

**The hot publish path does zero KV operations.** `OnPublish` on a non-retained message touches only `EncodeTopic` and `nc.PublishMsg`. The only KV call sites in the bridge are `PutRetained` and `MatchRetained`. Adding a store call to the non-retained path would change the cost model the whole design rests on — if you think one is needed, raise it rather than adding it.

**Metric cardinality is per tenant, never per device.** 300k label series will take down Prometheus before they take down the broker.

## Delivery guarantees — the current truth

There are three hops, and the middle one is the weak one.

| Hop | Guarantee |
| --- | --- |
| device → ingress node | real MQTT QoS 0/1/2 |
| ingress → owning node (core NATS) | **at-most-once** |
| owning node → subscriber | real MQTT QoS 0/1/2 |

The ingress node acks the publisher *before* the message crosses the fabric hop, so cross-node QoS 1 and 2 are best-effort. Within a single node the guarantee is real.

The fix is designed and not implemented: request-reply on the downlink hop so the owning node acknowledges acceptance before the ingress PUBACKs, at the cost of a round trip. It is [#11](https://github.com/mastmq/mast/issues/11), and the open question is what "the owning node" means under fan-out — NATS request-reply returns the first responder, and the ingress does not know how many nodes hold interest.

This is written up in [`mastmq/docs/guides/delivery-guarantees.md`](https://github.com/mastmq/docs/blob/main/guides/delivery-guarantees.md). **If you change delivery semantics, that guide, `docs/ARCHITECTURE.md` and the README status block all have to move together.** Stale claims here have bitten twice already.

## mochi-mqtt hook notes

Return codes on `OnPublish` are not interchangeable:

- `packets.CodeSuccessIgnore` — skips local delivery and local retain, but **still sends the PUBACK**. This is what the bridge returns on the normal path, because delivery happens via NATS instead.
- `packets.ErrRejectPacket` — swallows the PUBACK. A client that gets this hangs waiting for an ack.

`cl.Net.Inline` marks the server's own injected messages. The bridge returns early on those or it loops.

**A node receives one message once per NATS subscription it matches**, and it holds one per distinct filter, so overlapping filters (`a/#` beside `a/b/c`) mean several copies. Each used to run mochi's whole local fan-out, which delivered duplicates to everyone and put a message in front of two members of one share group. `onNATSMessage` now drops repeat plain copies by the `Mast-Id` header, and routes a queue copy to its own group only through `OnSelectSubscribers`, carrying the route in `Properties.ServerReference` because mochi never encodes that property on a PUBLISH. mochi calls `OnSelectSubscribers` only when a shared subscriber matches, which is why a queue copy with no local member is dropped before injection. Anything delivered to one client — a retained replay, a drained queue — goes through `deliverTo`, never `server.Publish`, which fans out to the whole node.

`$share/<group>/<filter>` maps onto a NATS queue group. A queue group **serves a local member first** — this is NATS geo-affinity, not a bug. The MQTT guarantee (exactly one group member) holds; the round-robin-across-the-fleet behaviour an MQTT user expects does not. Verified on a real three-node cluster. Documented, not hidden.

## The MQTT library is ours now

mast imports `github.com/mastmq/mochi/v2`, not `github.com/mochi-mqtt/server/v2`. [`mastmq/mochi`](https://github.com/mastmq/mochi) is a detached fork of mochi-mqtt/server, MIT, republished under our own module path because upstream stopped merging: `main` has not moved since 2025-03-01 and 47 pull requests are open, including four that fix the deadlock in #9.

`v2.7.10` is upstream `v2.7.9` plus that one fix. **Keep it that way.** A patch to MQTT behaviour belongs in `mastmq/mochi` with a line in its `FORK.md`, not as a workaround here, and every line we do not have to carry is a line that costs nothing when upstream revives.

Two consequences worth remembering. `go get -u ./...` will not pull mochi-mqtt updates any more, because we no longer depend on that path — watch upstream by hand. And `.golangci.yml`'s `exhaustruct_v5` ignore pattern is `^github\.com/mastmq/mochi/.*`; it has to move with the module path or every mochi struct literal starts reporting.

## Session state

Three hooks carry it, and the order they run in is the whole trick.

`OnConnectAuthenticate` mounts the client id, claims the session and — on a clean start — drops the durable state. It cannot restore, because mochi has not added the client yet: registering the subscriptions there works, and then the replayed backlog publishes into a topic index whose subscriber is not in the client map, so every queued message lands on the floor. That was a real bug and the test caught it.

`OnSessionEstablished` restores, because it runs after `Clients.Add` and after the CONNACK. It re-registers each stored filter with `server.Topics.Subscribe` **and** `cl.State.Subscriptions.Add` — both, exactly as mochi's own storage reload does, because a message matches nothing on the way out if the two disagree — then re-acquires the NATS subscriptions and drains the queue. A session mochi inherited locally is left alone: it already has subscriptions and richer inflight state than the bucket holds.

`OnQosPublish` queues. It fires when a QoS 1 or 2 packet enters a client's inflight, which mochi does *before* deciding it cannot write to the connection, so it is the one place an absent client's message can be caught. **The offline check must mirror mochi's own** — `cl.Net.Conn == nil || cl.Closed()` — because a client that sent DISCONNECT with a session to keep is not marked closed, and that is precisely the client whose messages need keeping.

QoS 0 is deliberately never queued: MQTT allows discarding it for an absent client, and persisting it turns fire-and-forget into storage that outlives the thing it described. A corollary that cost an hour: **queueing follows the subscription's QoS, not the publisher's.** A QoS 0 subscriber gets nothing kept however the publisher sent it, which is why `subscribeQoS` exists in the tests.

## The session control plane

`mast.session.<tenant>.<client-id>` is the only control-plane subject, and it is deliberately **outside** the `t.` namespace that carries tenant traffic. Every filter a client can subscribe to is mounted under its tenant and encoded to `t.<tenant>....`, so no client — wildcard or otherwise — can read a notice or forge one. Keep it that way: a control subject reachable from a `#` subscription is a control subject a tenant owns.

Both tokens go through `topic.EncodeToken`, which is the single-token form of the topic codec. A client id comes off the wire and may hold `.`, `*` or `>`; unescaped, a device could choose which subject its notice landed on.

A notice carries the **connection's** owner token, not the node's. The node that publishes is also subscribed, so it has to recognise its own notice, and identifying by node would make two connections on one node indistinguishable. `NodeID` exists only so a log line can say where a client went, and it is the embedded server's id rather than the configured name — the name defaults to the role, so every edge pod would answer to `mast-edge`.

A failed claim logs and continues rather than refusing the connection. Without it a client can end up live in two places, which is the bug this prevents; refusing outright would turn duplicate delivery into an outage.

## Configuration

koanf, layered: hardcoded defaults → optional TOML → environment → explicitly-set flags.

Env vars are prefixed `MAST__` and nest with a double underscore: `MAST__MQTT__ADDR=":8883"` sets `mqtt.addr`. List settings are comma-separated.

`configs/config.example.toml` must stay exhaustive and must contain **only built-in defaults**, so an empty file behaves exactly like no file. Adding a config field means adding it there in the same change.

## Testing

```console
$ just test       # go test -race with coverage
$ just fuzz 60s   # the topic codec round trip
$ just lint
```

The end-to-end tests in `internal/infra/broker` drive a real Paho client against a real node with a real embedded NATS server. `parity_test.go` is the MQTT conformance surface — QoS 0–2, retained, wills, persistent sessions.

**Broker subtests are deliberately not parallel.** They share one node on purpose, because standing up a NATS server per case is not what they are testing. Running them in parallel made `a/#` swallow the `a/+/c` case's publish, and the suite went green on the wrong message. `paralleltest` and `tparallel` are disabled for `_test.go` for exactly this reason — do not "fix" it by adding `t.Parallel()`.

**[#9](https://github.com/mastmq/mast/issues/9) was an upstream deadlock, and is fixed.** `go test -race ./...` used to hang intermittently in the broker package. The goroutine dump, captured 2026-09-22, showed a recursive read lock in mochi-mqtt v2.7.9: `Clients.GetByListener` held `RLock` and then called `Clients.Len`, which takes `RLock` again, so a `Clients.Delete` from `attachClient` arriving between the two wedged all three. The trigger was a client connecting while the server closed, which is also why it presented in production as a pod that would not terminate.

Upstream has carried it as an open issue since December 2025 with four unmerged pull requests, so the fix lives in [`mastmq/mochi`](https://github.com/mastmq/mochi) instead — see the next section.

### Tests bind port zero

Every test asks for `127.0.0.1:0` and reads back what was bound, through `Broker.ObsAddr()`. Reserving a port with `freeAddr` and releasing it so the broker can take it always leaves a window another test can win — that is a narrowed race, not a fixed one, and it only became visible once `obs.Serve` started failing loudly on a bad bind instead of logging.

### Never settle on a constant

`waitForLeaf(func() bool { return true })` returns on its first check and waits for nothing. Two cluster tests used it as a "let interest propagate" step and passed for weeks on an empty machine, then failed under the load of a full `-race` run. Settle by proving the thing happened: `awaitInterest` round-trips a probe message, and `awaitMetric` reads the counter that says the work is done.

That matters most across nodes. A publisher's PUBACK comes from its own ingress node and says nothing about whether the node owning an absent session has finished writing to the bucket, so a test that reconnects straight after publishing is racing a write it cannot observe.

## Linting

golangci-lint v2, `default: all`. The disable list is deliberately short and **every entry needs a one-line why**. Suppress per-line with `//nolint:<linter> // <reason>` rather than disabling globally.

Two traps that cost real time:

**`exhaustruct` was renamed in v2.13.0.** `exhaustruct_v5` replaces it; with `default: all` both would run and double-report, so the old one is disabled. Existing `//nolint:exhaustruct` comments are dead — the suffix is required.

**The `embedlit` / `exhaustruct` deadlock (Go 1.27).** `modernize`'s `embedlit` suggests hoisting promoted fields into the outer struct literal; exhaustruct v5.0.3 then *panics* on that literal. A panicking analyzer aborts the whole run and **cannot be suppressed with `//nolint`**, because suppression is applied after analysis. Keep the nested literal and suppress the suggestion instead.

## CI

| Workflow | Does |
| --- | --- |
| `test.yaml` | lint, test + codecov, `govulncheck`, **fuzz on every push**, then build |
| `docker.yaml` | multi-tag ghcr image. PRs build but do not publish |
| `codeql.yml` | weekly security scan |

`govulncheck` runs on every push, not only on dependency bumps — a dependency can become vulnerable without a line of our code changing. It fails only on *reachable* vulnerabilities; treating unreachable ones as failure is how a red pipeline stops meaning anything.

Images are tagged `type=semver,pattern={{version}}`, `{{major}}.{{minor}}`, branch and sha. A `v*` tag publishes the release image.

## Docs that must stay in sync

This repo is one of six. A behaviour change usually touches more than one.

| Change | Also update |
| --- | --- |
| delivery semantics | `docs/ARCHITECTURE.md`, `mastdocs/guides/delivery-guarantees.md`, README status block, website |
| a config field | `configs/config.example.toml`, the Helm chart in `mastmq/charts`, the chart README |
| what works / what does not | the README status blockquote **and** the "Not yet" line — they have gone stale together twice |
| a design decision | `docs/ARCHITECTURE.md` gets the *why*; the README gets the summary |

`docs/ARCHITECTURE.md` records why mast is shaped the way it is, against a target of ~300k concurrent devices. Several decisions in it would be wrong at a very different scale, and it says so. Keep that framing.

## Open issues to know about

| | |
| --- | --- |
| ~~[#8](https://github.com/mastmq/mast/issues/8)~~ | **fixed.** Sessions persist to KV and follow a device between nodes; QoS 1/2 for an absent client queues and replays |
| ~~[#9](https://github.com/mastmq/mast/issues/9)~~ | **fixed**, by moving to `mastmq/mochi` v2.7.10 |
| [#11](https://github.com/mastmq/mast/issues/11) | cross-node QoS 1/2 are best-effort |
| ~~[#13](https://github.com/mastmq/mast/issues/13)~~ | **fixed.** Cross-node takeover, via the `mast.session.*` control plane |
| ~~[#12](https://github.com/mastmq/mast/issues/12)~~ | **fixed.** `natsd` registers the asynchronous handlers and publishes `mast_nats_slow_consumers_total`. Any increase means this node discarded messages it had already acknowledged |

## Conventions

Conventional commits with a scope where one applies: `feat(auth):`, `fix(obs):`, `docs:`, `ci:`, `build(deps):`. The body explains *why*, in prose, one paragraph per line.

Markdown is one paragraph per line — never hard-wrap prose. `MD013` is off in `.markdownlint.yaml` for that reason.

Every exported identifier gets a godoc comment starting with its name; `revive`'s `exported` rule enforces it. Package docs go above the `package` clause in the primary file.
