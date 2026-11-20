# Roadmap

What comes next for mast, across the `mastmq` repositories. Each item has an issue, and the issue is where the detail, the discussion and the status live; this page only says what the item is and why it matters. There are no dates: an item is done when its issue closes.

A gap listed here is still a gap. [Delivery guarantees](https://github.com/mastmq/docs/blob/main/guides/delivery-guarantees.md) says exactly what the broker promises today, and the [README](README.md#what-works) says what works and what does not yet.

## The broker

**Per-tenant limits** ([#25](https://github.com/mastmq/mast/issues/25)). Tenants are isolated but not limited: one tenant can take more than its share of a node in connections, publish rate or stored state. Limits per tenant, enforced without consensus state per tenant, are what make the multi-tenant claim complete.

**Dead-edge detection** ([#26](https://github.com/mastmq/mast/issues/26)). Done, not yet in a release: every node beats on the control plane, the cores declare an edge dead after ten silent seconds and queue for the sessions it held, and what was published since its last heartbeat is replayed into their queues — for a device that has already moved to another edge too. [ARCHITECTURE.md](docs/ARCHITECTURE.md#a-node-that-dies-without-a-word) has the design.

**Shared subscriptions across nodes** ([#27](https://github.com/mastmq/mast/issues/27)). A `$share` group whose members span nodes is served on core NATS, so the cross-node hop is at-most-once and distribution is local-first. Serving groups from the replicated stream needs a consumer per group, bounded, which is a different design.

**Admin API** ([#28](https://github.com/mastmq/mast/issues/28)). An HTTP API, off by default, to list and inspect a tenant's sessions, disconnect a client and read a tenant's counts from any node.

**OpenTelemetry tracing** ([#29](https://github.com/mastmq/mast/issues/29)). One trace for one message, from the publisher's listener through the fabric to the subscriber's session, at close to no cost when sampled out.

## Releases and CI

**Signed images, SBOM and provenance** ([#30](https://github.com/mastmq/mast/issues/30)). Done in v0.3.2: the image is signed with cosign and carries an SBOM and build provenance, and each release archive has a GitHub attestation. [Verifying a release](README.md#verifying-a-release) shows how to check them.

**MQTT conformance in CI** ([#31](https://github.com/mastmq/mast/issues/31)). Our own parity tests cover what we thought to test. An independent conformance suite in CI would catch what we did not, and turn a claim of conformance into evidence.

## Deploying it

**Grafana dashboard and alerts in the chart** ([charts#3](https://github.com/mastmq/charts/issues/3)). The chart already ships a ServiceMonitor; a dashboard and a PrometheusRule built on the metrics mast exports would turn them into something an operator can read.

**values.schema.json** ([charts#4](https://github.com/mastmq/charts/issues/4)). A mistyped values key is silently ignored today. A schema makes Helm reject it at install time.

**Artifact Hub listing** ([charts#5](https://github.com/mastmq/charts/issues/5)). The chart is published to `oci://ghcr.io/mastmq/charts` and is not listed where people search for charts.

**Ansible Galaxy publishing** ([ansible#3](https://github.com/mastmq/ansible/issues/3)). The collection installs from git today. Publishing it to Galaxy on each tag gives versions to pin and a normal `requirements.yml` entry.

## Around it

**Multi-pod load generation** ([bench#1](https://github.com/mastmq/bench/issues/1)). Every benchmark runs from one pod, which becomes the limit before the broker does. Until load comes from several pods, no result from the harness is mast's ceiling.

**Website deploy through Actions** ([mastmq.github.io#2](https://github.com/mastmq/mastmq.github.io/issues/2)). The site is served from committed build output, a stopgap from when no token could write a workflow. Building it in Actions removes the output from git and the chance of it drifting from the source.

Something missing from this list? Open an issue, or start a thread in [Discussions](https://github.com/mastmq/mast/discussions/categories/ideas).
