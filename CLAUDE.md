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
| `domain/liveness` | the heartbeat tracker: which nodes are dead, decided with the clock passed in |
| `domain/tenant` | `ID`, `Credentials`, `Identity`, the `Resolver` / `Policy` interfaces, and `Limits` / `Overrides` |
| `domain/quota` | the token bucket and the census that adds up a tenant's connections across nodes |
| `domain/admin` | the admin API's view types, its control-plane subjects, and the merge that pages a tenant's sessions across nodes and the store |
| `infra/bridge` | the mochi-mqtt hook that is the whole broker. `bridge.go` is ~770 lines; `subs.go` is the refcounted NATS subscription registry |
| `infra/broker` | assembles a node from natsd + store + bridge + mqttd. Home of the end-to-end tests |
| `infra/natsd` | the embedded `nats-server` and the in-process `nats.Conn` |
| `infra/store` | JetStream: the retained and sessions buckets, the offline queue stream, the QoS stream, batched direct gets |
| `infra/offline` | the queuer: keeps QoS 1/2 for absent sessions, on cores |
| `infra/mqttd` | the mochi-mqtt server, listeners and TLS |
| `infra/auth` | picks the backend named in config and returns a `Resolver` + `Policy` |
| `infra/httpauth` | the callback backend, with a TTL cache. Speaks both the mast wire and the EMQX v5 wire |
| `infra/jwtauth` | local token verification |
| `infra/config` | koanf: defaults → TOML → env → flags |
| `infra/limits` | per-tenant limits on one node, and the `mast.limits.connections` reports that make the connection limit cluster-wide |
| `infra/obs` | Prometheus metrics, `/healthz` (liveness), `/readyz` (readiness: an edge's leaf link, a core's JetStream), pprof |
| `infra/admin` | the admin HTTP API: bearer token from a file, scatter and gather over `mast.admin.>`, the stored half from `store` |
| `infra/tracing` | builds the OpenTelemetry tracer provider and OTLP/HTTP exporter. The spans themselves are in `bridge/trace.go` |
| `infra/memlimit` | sets GOMEMLIMIT from the container's cgroup limit, which Go does not read by itself |

`deployments/fire` is the Docker cluster and the chaos drills: three cores, three edges, SIGKILLs. A change to delivery or storage is not done until it has been through it, at least four runs against a baseline build; its README says how.

## Invariants

These are properties the tests and the docs both depend on. Breaking one is a data-loss or isolation bug, not a style question.

**The topic codec is total and reversible.** Every legal MQTT topic round-trips through `EncodeTopic` / `DecodeTopic` byte-identical. This is what makes the mapping safe to bake into stored subjects, authorization rules and retained-message keys. `FuzzRoundTrip` guards it and runs in CI on every push. A change here that breaks the round trip silently corrupts stored state.

**An encoded subject is also a valid JetStream KV key.** That is why the escape character is `=` and the allowlist is `[A-Za-z0-9_-]` — a KV key may only contain `[-/_=.a-zA-Z0-9]`. One encoding instead of two that must agree. Do not "improve" it to `%`.

**The tenant comes from authentication, never from the wire.** `OnACLCheck` validates against the tenant resolved at CONNECT. A client can never influence its own tenant after that point, and there is no code path where a topic, a header or a property may name one.

**Authentication always fails closed**, whatever `auth.http.on_error` says. Admitting a connection whose tenant is unknown means inventing an isolation boundary. `on_error` governs authorization only.

**The hot QoS 0 publish path does zero storage operations.** `OnPublish` on a non-retained QoS 0 message touches only `EncodeTopic` and `nc.PublishMsg`. QoS 1 and 2 are the one deliberate exception: they are written to the durable stream before they are acknowledged, because that acknowledgement is a promise core NATS alone cannot keep (#11). Adding a store call to the QoS 0 path would change the cost model the whole design rests on — if you think one is needed, raise it rather than adding it.

**Tracing off costs a comparison.** The bridge's tracer is nil when `tracing.enabled` is false, and every traced path checks for nil before it does anything else. Do not replace that with a no-op tracer: extracting a parent and building a context allocate whether or not anything records them, and the QoS 0 path is judged in allocations. `BenchmarkPublishQoS0` against the previous build is the proof, and `BenchmarkPublishQoS0Traced` is what turning it on costs.

**Metric cardinality is per tenant, never per device.** 300k label series will take down Prometheus before they take down the broker. And per tenant is bounded too: every tenant label goes through `Metrics.Tenant`, which folds every tenant past `obs.max_tenant_labels` into `~other`. A new per-tenant metric that skips it is unbounded again.

**A tenant limit never touches storage on a hot path.** The connection count crosses nodes as core NATS reports, not as a KV counter; the publish rate is a per-node token bucket. "Tenant limits" in `docs/ARCHITECTURE.md` says why each scope is what it is.

## Delivery guarantees — the current truth

There are three hops. The middle one is where a clustered broker earns or loses its guarantees.

| Hop | Guarantee |
| --- | --- |
| device → ingress node | real MQTT QoS 0/1/2 |
| ingress → owning node, QoS 0 (core NATS) | **at-most-once**, by contract |
| ingress → owning node, QoS 1/2 (durable stream) | **at-least-once** to every node with a subscriber |
| owning node → subscriber | real MQTT QoS 0/1/2 |

A QoS 1 or 2 publish is stored on the `mast_qos` stream, replicated, before the ingress PUBACKs or PUBRECs ([#11](https://github.com/mastmq/mast/issues/11), fixed). Every node reads the stream through **one consumer of its own** — per node, never per subscription, which is within the rule above — and the plain core-NATS copy of the same message, marked `Mast-Durable`, is dropped by a node that reads the stream. If the store fails the publish is refused: a failing reason code for MQTT 5, a closed connection for MQTT 3 so it resends.

Five things learned by failing, all in `store.ConsumeDurable`. **A missed heartbeat replaces the consumer**: the consumer is ephemeral on one core, and when that core died the client only stopped hearing heartbeats and pulled from nothing forever, which a SIGKILLed core did to every reader whose consumer it held. **An ordered consumer does not work**: a node cut off from the core keeps its own NATS connection, so what the core sent it in the gap is lost in transit and an unacknowledged consumer counts it delivered, catching up only when a later message shows the gap. **The consumer acknowledges, and cumulatively**: `AckAll` every 64 messages or 100ms; per-message acks roughly doubled the core's work and let 10k msg/s build a 400ms backlog. **Cumulative acks swallow a gap**: acknowledging the first delivery after a loss acknowledges the lost ones too, so under steady traffic redelivery never happened — only the tail of a quiet burst, which is all `TestClusterQoS1SurvivesALeafOutage` exercises, and the in-process `DropLeaf` closes cleanly so it never loses anything in flight. A delivery that arrives out of turn (its consumer sequence skips) is therefore neither handled nor acknowledged; the node replaces its consumer from the last stream sequence it handled, forever if it has to, since a node that stops reading still drops the plain copies. `TestDurableGapIsReplayed` guards it. **Redeliveries are real**, so the bridge drops repeats by `Mast-Id` through the same `seen` window the plain copies use — the ack wait (3s) has to stay inside that window.

Still best-effort: shared subscriptions across nodes (served by the core-NATS queue copy), a node dying after it has taken a message (for a killed edge's persistent sessions, only what it took more than `replayLead` before its last heartbeat), and a node cut off for longer than `fabric.durable_max_age`. A killed edge's sessions are no longer a gap: see "A killed edge" below. `TestClusterQoS1SurvivesALeafOutage` is the regression test for #11: it cuts an edge off with `natsd.DropLeaf` and requires exactly twenty of twenty messages.

This is written up in [`mastmq/docs/guides/delivery-guarantees.md`](https://github.com/mastmq/docs/blob/main/guides/delivery-guarantees.md). **If you change delivery semantics, that guide, `docs/ARCHITECTURE.md` and the README status block all have to move together.** Stale claims here have bitten twice already.

## mochi-mqtt hook notes

Return codes on `OnPublish` are not interchangeable:

- `packets.CodeSuccessIgnore` — skips local delivery and local retain, but **still sends the PUBACK**. This is what the bridge returns on the normal path, because delivery happens via NATS instead.
- `packets.ErrRejectPacket` — swallows the PUBACK. A client that gets this hangs waiting for an ack.

`cl.Net.Inline` marks the server's own injected messages. The bridge returns early on those or it loops.

**A node receives one message once per NATS subscription it matches**, and it holds one per distinct filter, so overlapping filters (`a/#` beside `a/b/c`) mean several copies. Each used to run mochi's whole local fan-out, which delivered duplicates to everyone and put a message in front of two members of one share group. `onNATSMessage` now drops repeat plain copies by the `Mast-Id` header, and routes a queue copy to its own group only through `OnSelectSubscribers`, carrying the route in `Properties.ServerReference` because mochi never encodes that property on a PUBLISH. Upstream mochi calls `OnSelectSubscribers` only when a shared subscriber matches; ours (v2.7.16) calls it for every publish, because the hook is also where the bridge applies what mochi gets wrong about an injected message: No Local, which mochi checks against the inline client instead of the publisher (the publisher's mounted id rides as `Mast-Origin`, MQTT 5 publishers only), and the rule that a filter starting with a wildcard never matches a `$` topic, which mochi applies to the tenant level of a mounted topic (`excludeDollar`). A queue copy with no local member is still dropped before injection, because without a shared subscriber it would fall through to the plain ones.

**Every path that matches a topic has to agree with mochi's index**, quirks included, because that index decides live delivery: the queuer's index (`offline/index.go`) and `topic.Matches` are both held to mochi's answers by `TestIndexMatchesLikeMochi`. mochi's `+/#` not matching `a` is one quirk kept on purpose. Anything delivered to one client — a retained replay, a drained queue — goes through `deliverTo`, never `server.Publish`, which fans out to the whole node.

**MQTT 5 properties ride the `Mast-Props` header** as JSON of `store.Properties`, the same type the buckets store, and only when a message has any, so MQTT 3 traffic pays nothing. Topic alias and subscription identifier are per connection and deliberately not carried. A stored message keeps `StoredAt`, because an expiry interval counts from publication: `deliverTo` dates the packet from then so the subscriber is told what is left, and the store drops (and for retained, deletes) what has expired. The v3 paho client cannot see properties at all, which is why `v5_test.go` uses `paho.golang`.

`$share/<group>/<filter>` maps onto a NATS queue group. A queue group **serves a local member first** — this is NATS geo-affinity, not a bug. The MQTT guarantee (exactly one group member) holds; the round-robin-across-the-fleet behaviour an MQTT user expects does not. Verified on a real three-node cluster. Documented, not hidden.

## The MQTT library is ours now

mast imports `github.com/mastmq/mochi/v2`, not `github.com/mochi-mqtt/server/v2`. [`mastmq/mochi`](https://github.com/mastmq/mochi) is a detached fork of mochi-mqtt/server, MIT, republished under our own module path because upstream stopped merging: `main` has not moved since 2025-03-01 and 47 pull requests are open, including four that fix the deadlock in #9.

mast is on `v2.7.16`: upstream `v2.7.9` plus the short list in `FORK.md` — the deadlock, the listener fixes, user properties on a PUBLISH surviving a client's Request Problem Information = 0, acks coalesced into one write while more input is buffered (upstream #552), `OnDisconnect` on a failed CONNACK or resend, `Options.ClientNetWriteTimeout` (upstream #531), and `OnSelectSubscribers` on every publish. **mast does not set `ClientNetWriteTimeout` yet**, so it still has no write deadline: a client that stops reading holds its client lock, which blocks QoS 1 deliveries to it and a takeover of its session, until the connection dies. Setting it is a config field, so it moves across the chart and the role too. That last one is #18: mochi wrote every PUBACK, PUBREC, PUBREL and PUBCOMP with a syscall of its own, and QoS 2 at 100k deliveries a second fell seconds behind on it. What it cannot batch is a subscriber receiving sparsely, so a QoS 2 delivery still costs about 1.6× the broker CPU of QoS 1 and QoS 2's fan-out ceiling is about half of QoS 1's — the protocol's two-packets-per-delivery, not something to fix here. **Keep the list short.** A patch to MQTT behaviour belongs in `mastmq/mochi` with a line in its `FORK.md`, not as a workaround here, and every line we do not have to carry is a line that costs nothing when upstream revives.

Two consequences worth remembering. `go get -u ./...` will not pull mochi-mqtt updates any more, because we no longer depend on that path — watch upstream by hand. And `.golangci.yml`'s `exhaustruct_v5` ignore pattern is `^github\.com/mastmq/mochi/.*`; it has to move with the module path or every mochi struct literal starts reporting.

## Session state

Three hooks carry it, and the order they run in is the whole trick.

`OnConnectAuthenticate` mounts the client id, claims the session and — on a clean start — drops the durable state. It cannot restore, because mochi has not added the client yet: registering the subscriptions there works, and then the replayed backlog publishes into a topic index whose subscriber is not in the client map, so every queued message lands on the floor. That was a real bug and the test caught it.

`OnSessionEstablished` restores, because it runs after `Clients.Add` and after the CONNACK. It re-registers each stored filter with `server.Topics.Subscribe` **and** `cl.State.Subscriptions.Add` — both, exactly as mochi's own storage reload does, because a message matches nothing on the way out if the two disagree — then re-acquires the NATS subscriptions and drains the queue. A session mochi inherited locally is left alone: it already has subscriptions and richer inflight state than the bucket holds.

**The core tier queues for absent sessions** (`internal/infra/offline`). Each core's queuer watches the sessions bucket for records with `present` false and reads `mast_qos` through one consumer of its own, writing matches to the `mast_offline` stream, one subject per session (`o.<key>`, newest 100). Every writer names a message `<Mast-Id>:<key>` and the stream deduplicates, so the queuers and the edge store it once. Before this, only the edge a device disconnected from queued for it, and a rolling restart of the edges lost every message published to a sleeping device — 10,000 of 10,000 in the drill. `present` is written on every resume, and `MarkAway` refuses to overwrite a record another connection owns — judged by the owner token the connection itself claimed, not whatever the client id's claim says by the time its disconnect is noticed — and never recreates a deleted one. **`MarkAway` reads the record from the stream leader, not with the bucket's `Get`**: a KV get is a direct get any replica answers, a replica can be a moment behind, and a device that subscribed and disconnected at once was looked up on one that had not seen the subscribe's write, taken for deleted, and left marked held — twenty sleepers in five thousand lost everything after their edge was killed. `DeleteSession` still reads with `Get` to keep clean connects off the leader, so a clean start racing a just-written record can leave it behind; that is a known gap. The queuer's index is its own (`index.go`), not mochi's: mochi's cost 2.8 KB per away session and three cores holding 150,000 were killed at 1 GiB. Every core holds every away session, so its index is sized for the fleet. The queuer pipelines its writes and `ConsumeDurableFlushed` waits for them before each ack; waiting per message left it twenty seconds behind a burst.

**A killed edge is noticed by its silence** (`domain/liveness`, `offline/nodes.go`). Every node serving MQTT beats on `mast.node.<server-id>` every second and names itself in every held record (`Session.Node`); each core's queuer declares a node dead after ten silent seconds, indexes the sessions it still holds as away, and replays `mast_qos` from five seconds before the last beat with batched direct gets (`ReplayDurable`, which is why `mast_qos` has `AllowDirect`). It also replays, up to the moment each moved, the sessions that moved off the dead node in that window — the usual case, since a device whose connection resets is on another edge within seconds — and the new edge's 30s straggler check delivers them; shorten that check or lengthen the threshold and that stops working. **The takeover is in memory only, never written to the bucket**, so a false positive (a slow or cut-off edge) costs duplicates and ends at the node's next beat, rather than recording live devices as gone. A late check (the core itself stalled) declares nobody, a core whose sessions stream leader cannot hear a majority of its replicas declares nobody (`store.Reachable`; reaching a leader is not enough, since a minority core may lead), **and neither guard may move a node's last-heard time** — `liveness` keeps it apart from the threshold's clock, because the replay starts from it, and a node shutting down cleanly says goodbye so a rolling deploy is not replayed like a crash. `TestClusterKilledEdgeSessionsAreQueuedFor` and `TestClusterDeviceLeavingAKilledEdgeGetsTheGap` kill an edge with `Broker.Kill` (`export_test.go`), which stops the fabric before MQTT so no away record escapes; `Close` cannot stand in for it.

**Edges never create or configure storage** — `store.Bind`, not `store.Open`. They used to, with their own `core.replicas`, which defaults to 1 on an edge, so every chart-deployed cluster ran R1. Cores provision in the background (they cannot before quorum) and reconcile replicas every minute.

**A drain reads, delivers, then trims only what the device has acknowledged** — packet ids gone from mochi's inflight, waited for in the background — and stops at the first message it cannot send, and the edge looks again at 5s and 30s for writes a queuer was still retrying when the device arrived. Trimming once the backlog was written to the socket lost it whenever the connection dropped before the PUBACKs and the device came back on another node. Batched direct gets check `Nats-Last-Sequence` continuity, so a reply dropped by a lagging subscription is an error, not a short batch the trim then removes. A failed write is retried under its id — a lost ack from a dead stream leader once stalled the in-order queuer for its whole thirty-second deadline.

`OnQosPublish` queues. It fires when a QoS 1 or 2 packet enters a client's inflight, which mochi does *before* deciding it cannot write to the connection, so it is the one place an absent client's message can be caught. **The offline check must mirror mochi's own** — `cl.Net.Conn == nil || cl.Closed()` — because a client that sent DISCONNECT with a session to keep is not marked closed, and that is precisely the client whose messages need keeping.

**Only a session that outlives its connection is stored** — v3 not clean, v5 with a non-zero expiry (`outlivesConnection`, mochi's own rule inverted). Every bucket write is a replicated write on the core, and a clean session used to cost one per SUBSCRIBE plus blind deletes on CONNECT and DISCONNECT. `DeleteSession` reads before it deletes for the same reason: a read is served by any replica without consensus, a delete of nothing still writes a tombstone.

**The sessions bucket's TTL counts from the last write**, not from disconnect, so a stored session is rewritten at DISCONNECT and refreshed while connected once it is half an expiry old (`refreshSessions`). Before that, a device holding one connection for longer than `session.expiry` lost its stored session while using it, and came back from its next move between nodes with no subscriptions. A restored session is dated from its stored `UpdatedAt`, so a reconnect storm does not trigger a storm of rewrites.

QoS 0 is deliberately never queued: MQTT allows discarding it for an absent client, and persisting it turns fire-and-forget into storage that outlives the thing it described. A corollary that cost an hour: **queueing follows the subscription's QoS, not the publisher's.** A QoS 0 subscriber gets nothing kept however the publisher sent it, which is why `subscribeQoS` exists in the tests.

## The session control plane

`mast.session.<tenant>.<client-id>` is the only control-plane subject, and it is deliberately **outside** the `t.` namespace that carries tenant traffic. Every filter a client can subscribe to is mounted under its tenant and encoded to `t.<tenant>....`, so no client — wildcard or otherwise — can read a notice or forge one. Keep it that way: a control subject reachable from a `#` subscription is a control subject a tenant owns.

Both tokens go through `topic.EncodeToken`, which is the single-token form of the topic codec. A client id comes off the wire and may hold `.`, `*` or `>`; unescaped, a device could choose which subject its notice landed on.

A notice carries the **connection's** owner token, not the node's. The node that publishes is also subscribed, so it has to recognise its own notice, and identifying by node would make two connections on one node indistinguishable. `NodeID` exists only so a log line can say where a client went, and it is the embedded server's id rather than the configured name — the name defaults to the role, so every edge pod would answer to `mast-edge`.

**A node holds one subscription to the control plane, `mast.session.>`, not one per client.** The per-client version was the obvious shape and cost a goroutine, a SUB round trip and a cluster-wide interest update per CONNECT, held for as long as the session lived — an edge with 25k sleeping persistent sessions held 25k subscriptions for them. Now every notice in the cluster reaches every node and a map lookup on the subject decides whether it matters, before the JSON is even decoded. `BenchmarkConnect` reports `nats-subs/client`; it must stay at zero. The bench repo's RESULTS.md once recorded this design as 6.5× slower in a two-edge storm; the 2026-10-05 entry there shows that was the harness, and that a storm comparison needs exact-name kills of the previous brokers and a minute of idle between runs before its numbers mean anything.

`mast.admin.>` is the admin API's control plane, under the same rule: outside `t.`, one subscription per node (`bridge/admin.go`), tenant and client id in the body rather than the subject. A list or stats request waits `admin.gather` for every node; a get or a disconnect is answered only by the node holding the client. An admin disconnect is `DisconnectClient` with *administrative action*, deliberately not a session notice — a takeover empties the session for a new owner that here does not exist. Every answer is one NATS message, so it must fit `max_payload`: a list reply is filled to the budget and marked truncated (`admin.EncodeList`), `admin.Merge` ends the page at the lowest cut, and an answer that cannot be sent becomes an `admin.Failure`. A refused reply is indistinguishable from an absent node, which is how a page once lost a node's clients for good. See `docs/ARCHITECTURE.md`.

A failed claim logs and continues rather than refusing the connection. Without it a client can end up live in two places, which is the bug this prevents; refusing outright would turn duplicate delivery into an outage.

## Configuration

koanf, layered: hardcoded defaults → optional TOML → environment → explicitly-set flags.

Env vars are prefixed `MAST__` and nest with a double underscore: `MAST__MQTT__ADDR=":8883"` sets `mqtt.addr`. List settings are comma-separated.

`configs/config.example.toml` must stay exhaustive and must contain **only built-in defaults**, so an empty file behaves exactly like no file. Adding a config field means adding it there in the same change.

## Testing

```console
$ just test       # go test -race with coverage
$ just fuzz 60s   # the topic codec round trip
$ just bench      # the hot paths; BenchmarkConnect must report 0 nats-subs/client
$ just lint
```

The benchmarks are how a performance claim is checked, and the numbers in a commit message come from them. `BenchmarkPublishQoS0` drives a real client through a real node and is dominated by loopback syscalls, so it catches a regression in allocations rather than in nanoseconds; the micro-benchmarks in `topic` and `bridge` are where a change to the hot path shows.

The end-to-end tests in `internal/infra/broker` drive a real Paho client against a real node with a real embedded NATS server. `parity_test.go` is the MQTT conformance surface — QoS 0–2, retained, wills, persistent sessions. `conformance_test.go` pins each gap the Paho interoperability suite found (#35–#39, #42), so one cannot come back without a test failing in `go test` rather than only in the slower conformance workflow.

Five things the bridge does to mochi for those, each easy to undo by accident. A live message is injected with retain unset, or mochi keeps it in its own retained index and replays it beside the bucket's copy, so `OnPacketEncode` sets the flag back for a subscriber that asked for Retain As Published; `OnSubscribe` rewrites Retain Handling 1 to 2 for a filter the client already holds, because only mochi's own retained replay was told whether it existed. It sets `NoInheritedPropertiesOnAck`, or every PUBACK echoes the publisher's user properties (#39); the injector's protocol level is 5, because mochi expires an inflight message only when the packet was injected at level 5 (#36); and a will with a Will Delay Interval waits in `bridge/will.go`'s timers, cancelled by a connection to the session on this node (`claimSession`) or another (`onSessionNotice`), because mochi calls `OnWill` when the connection drops rather than when the delay ends (#38). The timer is in memory, so a delayed will does not survive its node stopping or dying inside the delay.

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
| `docker.yaml` | multi-tag ghcr image for linux/amd64 and linux/arm64, cross-compiled in the build stage rather than emulated, with an SBOM and `mode=max` provenance, and the pushed digest signed keylessly with cosign. PRs build but do not publish |
| `release.yaml` | linux amd64/arm64 tarballs and a checksum file on the GitHub release, for `mastmq/ansible`, each with a GitHub build-provenance attestation. The file names are that collection's interface |
| `codeql.yml` | weekly security scan |
| `conformance.yaml` | the Eclipse Paho interoperability suite, MQTT 3.1.1 and 5, against an all-in-one node, on push, PRs and weekly; `just conformance` runs the same script. Known failures are expected failures in `test/conformance/run.py`, each with its issue, and one that starts passing turns the run red until its entry goes |

`govulncheck` runs on every push, not only on dependency bumps — a dependency can become vulnerable without a line of our code changing. It fails only on *reachable* vulnerabilities; treating unreachable ones as failure is how a red pipeline stops meaning anything.

Images are tagged `type=semver,pattern={{version}}`, `{{major}}.{{minor}}`, branch and sha. A `v*` tag publishes the release image.

## Docs that must stay in sync

This repo is one of six. A behaviour change usually touches more than one.

| Change | Also update |
| --- | --- |
| delivery semantics | `docs/ARCHITECTURE.md`, `mastdocs/guides/delivery-guarantees.md`, README status block, website |
| a config field | `configs/config.example.toml`, the Helm chart in `mastmq/charts`, the chart README, the Ansible role in `mastmq/ansible` |
| what works / what does not | the README status blockquote **and** the "Not yet" line — they have gone stale together twice |
| a design decision | `docs/ARCHITECTURE.md` gets the *why*; the README gets the summary |

`docs/ARCHITECTURE.md` records why mast is shaped the way it is, against a target of ~300k concurrent devices. Several decisions in it would be wrong at a very different scale, and it says so. Keep that framing.

## Open issues to know about

| | |
| --- | --- |
| ~~[#8](https://github.com/mastmq/mast/issues/8)~~ | **fixed.** Sessions persist to KV and follow a device between nodes; QoS 1/2 for an absent client queues and replays |
| ~~[#9](https://github.com/mastmq/mast/issues/9)~~ | **fixed**, by moving to `mastmq/mochi` v2.7.10 |
| ~~[#11](https://github.com/mastmq/mast/issues/11)~~ | **fixed.** Cross-node QoS 1/2 ride the durable stream; shared groups across nodes are still best-effort |
| ~~[#13](https://github.com/mastmq/mast/issues/13)~~ | **fixed.** Cross-node takeover, via the `mast.session.*` control plane |
| ~~[#12](https://github.com/mastmq/mast/issues/12)~~ | **fixed.** `natsd` registers the asynchronous handlers and publishes `mast_nats_slow_consumers_total`. Any increase means this node discarded messages it had already acknowledged |

## Conventions

Conventional commits with a scope where one applies: `feat(auth):`, `fix(obs):`, `docs:`, `ci:`, `build(deps):`. The body explains *why*, in prose, one paragraph per line.

Markdown is one paragraph per line — never hard-wrap prose. `MD013` is off in `.markdownlint.yaml` for that reason.

Every exported identifier gets a godoc comment starting with its name; `revive`'s `exported` rule enforces it. Package docs go above the `package` clause in the primary file.
