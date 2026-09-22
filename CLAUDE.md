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

`$share/<group>/<filter>` maps onto a NATS queue group. A queue group **serves a local member first** — this is NATS geo-affinity, not a bug. The MQTT guarantee (exactly one group member) holds; the round-robin-across-the-fleet behaviour an MQTT user expects does not. Verified on a real three-node cluster. Documented, not hidden.

## The MQTT library is ours now

mast imports `github.com/mastmq/mochi/v2`, not `github.com/mochi-mqtt/server/v2`. [`mastmq/mochi`](https://github.com/mastmq/mochi) is a detached fork of mochi-mqtt/server, MIT, republished under our own module path because upstream stopped merging: `main` has not moved since 2025-03-01 and 47 pull requests are open, including four that fix the deadlock in #9.

`v2.7.10` is upstream `v2.7.9` plus that one fix. **Keep it that way.** A patch to MQTT behaviour belongs in `mastmq/mochi` with a line in its `FORK.md`, not as a workaround here, and every line we do not have to carry is a line that costs nothing when upstream revives.

Two consequences worth remembering. `go get -u ./...` will not pull mochi-mqtt updates any more, because we no longer depend on that path — watch upstream by hand. And `.golangci.yml`'s `exhaustruct_v5` ignore pattern is `^github\.com/mastmq/mochi/.*`; it has to move with the module path or every mochi struct literal starts reporting.

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
| [#8](https://github.com/mastmq/mast/issues/8) | sessions do not survive a restart. `store.PutSession`, `Enqueue` and `Drain` are **written but not wired to the bridge** |
| ~~[#9](https://github.com/mastmq/mast/issues/9)~~ | **fixed**, by moving to `mastmq/mochi` v2.7.10 |
| [#11](https://github.com/mastmq/mast/issues/11) | cross-node QoS 1/2 are best-effort |
| ~~[#12](https://github.com/mastmq/mast/issues/12)~~ | **fixed.** `natsd` registers the asynchronous handlers and publishes `mast_nats_slow_consumers_total`. Any increase means this node discarded messages it had already acknowledged |

## Conventions

Conventional commits with a scope where one applies: `feat(auth):`, `fix(obs):`, `docs:`, `ci:`, `build(deps):`. The body explains *why*, in prose, one paragraph per line.

Markdown is one paragraph per line — never hard-wrap prose. `MD013` is off in `.markdownlint.yaml` for that reason.

Every exported identifier gets a godoc comment starting with its name; `revive`'s `exported` rule enforces it. Package docs go above the `package` clause in the primary file.
