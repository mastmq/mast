# Architecture

This document records why mast is shaped the way it is. The decisions below were made against a target of roughly 300k concurrent devices across many tenants, and several of them would be wrong at a much smaller or much larger scale.

## The constraint that drives everything

JetStream consumers are Raft state machines. A NATS server holds on the order of 2k HA assets, and very high consumer counts are explicitly not what they are built for. nats-server's own MQTT implementation creates a durable consumer per subscription in every persistent session, so 300k devices with two subscriptions each would need around 600k consumers — two to three orders of magnitude past the design. That single fact rules out the built-in MQTT listener at this scale, and it also rules out any design of our own that puts per-device state in a consumer.

Core NATS has the opposite profile. It handles tens of millions of subjects, builds interest only for subjects that have subscribers, propagates interest only to servers that need it, and costs roughly 1GB of memory per million subscriptions. Live delivery therefore rides core NATS, always, and never a JetStream consumer.

The rule that falls out: **persistent state scales as keys, not as consensus groups.** Sessions, retained messages, and bounded offline queues go in a small number of JetStream KV buckets — three buckets, not 600k consumers. An offline device's queue is read on reconnect with a direct get; no consumer is created, ever.

## Why roles instead of two binaries

The MQTT tier and the storage tier have opposite operational requirements.

| | edge | core |
| --- | --- | --- |
| scales with | connection count | data volume |
| lifecycle | ephemeral, restarts constantly | stable, long-lived peers |
| state | none | volume, Raft quorum |
| failure cost | reconnect storm | stream unavailability |

Run them in one process at scale and every deploy becomes Raft membership churn, while allocation churn from tens of thousands of connections produces GC pauses that make Raft miss heartbeats — spurious leader elections caused by the network layer. Run them as two binaries and you have two things to build, version, and operate.

One binary with a role flag gets both properties. It is the Consul server/agent model. It also preserves the single-binary story that makes mast deployable at a customer site with no dependencies, which is the thing none of the alternatives offer.

## Topology

Edge processes embed nats-server as a **leaf node** rather than joining the route mesh. A NATS cluster is a full mesh and its traffic grows quadratically in node count, so an autoscaling group of 12–30 edge pods is the wrong shape for route peering. Leaf nodes exist for exactly this: the cluster sees one leaf connection rather than the individual clients.

Embedding also gives the bridge an in-process connection over `net.Pipe` instead of a loopback socket, and lets two devices on the same pod exchange a message without it leaving the process.

## Tenancy

Tenancy is enforced in two places. The subject namespace is `t.<tenant>.<...>`, and the `OnACLCheck` hook validates every publish and subscribe against the tenant resolved at authentication — never against anything the client sent on the wire.

Two designs are possible for the NATS layer, and mast starts with the first. A single account with subject prefixing is cheap and scales to tens of thousands of tenants, with isolation enforced by our code. An account per tenant has NATS enforce isolation itself and brings per-tenant JetStream quotas for free, but costs a connection per tenant per pod, capping out in the low thousands. The tenant resolver is an interface so individual tenants can be promoted to the second model later.

Per-tenant quotas, metering and rate limits live at the connection lifecycle in the edge process. This is the capability that has no home in `nats-server` and is the main reason mast exists as a frontend rather than as configuration.

## Known sharp edges

**Cross-node QoS 1 is not end-to-end QoS 1.** If the ingress pod PUBACKs a device and then the owning pod crashes mid-delivery, the message is gone although both ends performed QoS 1. The fix is request-reply on the downlink hop so the owning node acknowledges acceptance before the ingress PUBACKs; the cost is a round trip. Until that is implemented the guarantee is "QoS 1 within a node, best-effort across nodes" and must be documented as such.

**Correlated events hurt more than throughput.** 300k devices at one message per minute is 5k msg/s, which is nothing. A deploy that reconnects 25k devices per pod, 25k simultaneous authentications, 25k will messages fired by a dying pod, or a `#` subscribe against a large tenant's retained set — those are what break. Ingress accept rate limiting, locally verifiable credentials rather than a synchronous IdP call per connect, and staggered pod restarts are requirements, not optimizations.

**Metric cardinality must be per tenant, never per device.** 300k-series labels will take down Prometheus before they take down the broker.
