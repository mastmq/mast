# Architecture

This document records why mast is shaped the way it is. The decisions below were made against a target of roughly 300k concurrent devices across many tenants, and several of them would be wrong at a much smaller or much larger scale.

## The constraint that drives everything

JetStream consumers are Raft state machines. A NATS server holds on the order of 2k HA assets, and very high consumer counts are explicitly not what they are built for. nats-server's own MQTT implementation creates a durable consumer per subscription in every persistent session, so 300k devices with two subscriptions each would need around 600k consumers — two to three orders of magnitude past the design. That single fact rules out the built-in MQTT listener at this scale, and it also rules out any design of our own that puts per-device state in a consumer.

Core NATS has the opposite profile. It handles tens of millions of subjects, builds interest only for subjects that have subscribers, propagates interest only to servers that need it, and costs roughly 1GB of memory per million subscriptions. Live delivery therefore rides core NATS, always, and never a JetStream consumer.

The rule that falls out: **persistent state scales as keys, not as consensus groups.** Sessions and retained messages go in two JetStream KV buckets, and bounded offline queues in one stream with a subject per session — a handful of streams, not 600k consumers. An offline device's queue, and a new subscriber's retained messages, are read with batched direct gets; no consumer is created, ever. Retained replay once listed keys through a KV watch, which is an ordered consumer per SUBSCRIBE, and a storm of 1,500 subscribers took two minutes and timed out on 576 of them; the direct get serves the same storm in under nine seconds.

A queue was first one KV entry holding the whole list, rewritten under its revision per message. Each append cost the size of the queue, payloads became base64 inside JSON, and a hundred ordinary messages could outgrow a NATS message, after which every append for that device failed. A subject per session with `MaxMsgsPerSubject` makes an append one small write and leaves discarding the oldest to the server.

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

## Who owns storage

Only a node carrying JetStream — a core, or an all-in-one node — creates or configures a bucket or stream. An edge binds to what exists and waits until it does. Edges used to create them with their own `core.replicas`, which nothing sets on an edge and which defaults to 1, so every cluster deployed by the chart ran its sessions, retained messages and QoS stream unreplicated whatever the cores were configured with. Each core checks the replica counts once a minute, so an edge of an older version cannot leave the cluster that way during an upgrade.

Upgrading a cluster to this layout goes cores first: a new edge waits for streams only a new core creates. In between, two things are worth knowing. An old edge writes session records with no `present` field, which a new core reads as away, so every core queues for every device connected to an old edge until that edge is replaced — extra writes and, when those devices next reconnect, up to a hundred repeats each, but nothing lost; reading such records as held instead would have lost the sleepers of an edge restarted mid-upgrade, which is the loss the queuer exists to stop. And what an old edge queued in the legacy `mast_offline` bucket is delivered from there on the device's next connection and removed only once delivered.

## Liveness is not readiness

`/healthz` and `/readyz` answer different questions, and the difference matters most on an edge. An edge whose leaf connection to the core is down is alive and cannot serve: it can terminate MQTT, but nothing it accepts reaches a subscriber on another node and nothing durable can be stored. A restart does not bring the core back, so `/healthz` stays 200; a load balancer should stop sending it devices, so `/readyz` goes to 503 until the link is back. A liveness probe that failed on a lost core would restart every edge at once, which turns a core outage into a reconnect storm the moment the core returns.

A core or an all-in-one node is ready when nats-server's own health check says it is: accepting connections, JetStream enabled, and in a cluster a meta leader and current streams. mast reuses that check rather than writing its own, because nats-server already knows what JetStream needs and a second implementation would drift from it. The chart's core readiness probe asks the NATS monitor the same question.

## Absent sessions are the core's job

An absent persistent session collects its QoS 1 and 2 messages on the core tier, not on the edge it last used. Each core runs a queuer: one watch on the sessions bucket tells it who is away and what they subscribed to, and one consumer per core on the QoS stream tells it every message. It writes each match to the session's queue under a name every writer derives from the publish's id, so the cores and the edge, all writing the same message, store it once. That is one more reader of the whole QoS stream per core — the cost the next section already names, measured at roughly doubling the cores' CPU under QoS 1 load — and it is what lets a session outlive its edge, which is the tier that restarts on every deploy. Before it, a rolling restart of the edges dropped every message published to a sleeping device.

The queuer handles messages in order but does not wait for one message's writes before reading the next; the stream is acknowledged only after every write started so far is stored, and a write that keeps failing holds the acknowledgement rather than being given up. Waiting per message bounded it at one replicated round trip per message, and a burst of 100,000 left it twenty seconds behind, long enough for devices to wake before their messages were queued. Because it can be behind, it decides whether a device was away by when the stream stored a message, not by what the sessions bucket says when it gets there; a session that comes back stays in its index until the queuer has read past that moment.

Every core holds every away session in memory, so the queuer's index is sized for the fleet, not the node. It is not mochi's topic index, which the first version used: mochi allocates three maps for every node of its tree, device topics give nearly every device nodes of its own, and at 2.8 KB per session three cores holding 150,000 seeded sleepers were killed at their 1 GiB limit. The queuer's own index keeps children and subscribers in short slices and costs about 430 bytes per session; with 300,000 sleepers each core loaded them in about ten seconds, reloaded them in three after a restart, and settled near 650 MB, half of it garbage the collector had not yet reached. mast also sets the Go memory limit from the container's, which Go does not read by itself; without it the heap may grow to twice what is live whatever the container allows. The index is still pointer-dense, and marking it is work: on a core capped at one CPU, 300,000 away sessions took QoS 1 p99 from about 4ms to 60–80ms, because the collector shared the only processor with message routing. At two CPUs per core the tail was back to 5–9ms. A core at that scale wants at least two CPUs; an index the collector does not have to scan is the fix if that ever stops being enough.

## A node that dies without a word

A node that shuts down records each lasting session it holds as away, and the queuer takes it from there. A node that is killed outright — SIGKILL, an OOM kill, a VM that disappears — records nothing, so its sessions stayed on record as held by connections that no longer existed, no core kept their messages, and everything published to those devices until each reconnected somewhere was dropped. That was the one gap a healthy cluster still had for a persistent session.

Every node that serves MQTT now publishes an empty heartbeat on `mast.node.<server-id>` every second, outside the `t.` namespace like every control subject, and every held session record names the node that holds it. Each core's queuer listens on `mast.node.*`, and a node silent for ten seconds is declared dead: the sessions it still holds are put in the queuer's index as away, and what the durable stream stored since five seconds before its last heartbeat is replayed for them with batched direct gets — no consumer — because the queuer read those messages live while the sessions still looked held, and a takeover that queued only from then on would lose exactly the seconds after a crash. The server id is the right name because a restart replaces it: an edge that comes straight back is a new node, and the old id never beats again. A record written by an edge older than this names no node and is never judged, so during an upgrade the old edges keep the old gap until they are replaced.

The takeover lives in each core's memory; no record is rewritten. Writing the sessions away in the bucket was the obvious shape, and it turns a false positive into a cluster-wide fact: a node that was only slow, or cut off for longer than the threshold, would have devices still connected to it recorded as gone, and would have to win every record back. Held in memory, a false positive costs queue writes for devices that also received their messages live — delivered twice at their next reconnect, which at-least-once allows — and it ends the moment the node beats again, when the queuer hands its sessions back. Three guards keep false positives rare. Ten missed heartbeats is longer than a leaf reconnect and far longer than any collector pause. A core that is itself stalled sees every node silent at once, so a check that comes late restarts every node's clock instead of judging. And a core cut off from the cluster hears no one, so before declaring anyone dead it asks the sessions stream for its leader, which a core in a minority cannot reach.

The common case is not the device that stays away. A device on a killed edge sees its connection reset and reconnects to another edge within a second or two, long before the threshold, and is restored there with nothing queued, while what was published between its last delivery and its new subscription was kept by nobody. So the queuer remembers, for half a minute, which node each held session last moved off, and when that node is declared dead the replay covers those sessions too, up to the moment each moved. The edge it moved to looks at the queue again thirty seconds after the connect, which is after the replay has landed. A node shutting down cleanly says goodbye on the same subject once it has closed its clients, so a rolling deploy, where every device moves before its old edge's away record lands, is not replayed as if it were a crash.

The memory cost is a key and a shared node name per held session — about a hundred bytes, thirty megabytes per core at the 300k-device target. Each core detects, reads and replays on its own, as each already queues on its own, and the queue stream stores what they all write once.

## Tenancy

Tenancy is enforced in two places. The subject namespace is `t.<tenant>.<...>`, and the `OnACLCheck` hook validates every publish and subscribe against the tenant resolved at authentication — never against anything the client sent on the wire.

Two designs are possible for the NATS layer, and mast starts with the first. A single account with subject prefixing is cheap and scales to tens of thousands of tenants, with isolation enforced by our code. An account per tenant has NATS enforce isolation itself and brings per-tenant JetStream quotas for free, but costs a connection per tenant per pod, capping out in the low thousands. The tenant resolver is an interface so individual tenants can be promoted to the second model later.

Per-tenant quotas, metering and rate limits live at the connection lifecycle in the edge process. This is the capability that has no home in `nats-server` and is the main reason mast exists as a frontend rather than as configuration.

## Known sharp edges

**Cross-node QoS 1 and 2 ride a stream, one consumer per node.** The fabric between nodes is core NATS, which is at-most-once, and an ingress node used to PUBACK before the message had crossed it — so a leaf reconnect or a slow consumer silently lost QoS 1 messages both ends had acknowledged (#11). A QoS 1 or 2 publish is now written to one replicated JetStream stream before the ingress acknowledges it, and every node reads that stream through a single consumer of its own, acknowledging what it has handed to its MQTT layer and receiving again anything it did not acknowledge. That is a consumer per *node*, which stays within the rule this design rests on: never per subscription, session or device.

Request-reply on the downlink hop was the first design, and it was dropped because it cannot answer fan-out: NATS returns the first responder, and the ingress has no way to know how many nodes hold interest. Storing first sidesteps the question — the ingress needs one acknowledgement, from the stream, however many nodes will read it. The costs are a replicated write on every QoS 1 or 2 publish, which is why QoS 0 is untouched, and every node reading all QoS 1 and 2 traffic whether or not it has a subscriber for it. At the 300k-device target that second cost is the one to watch; if it bites, the answer is filtering the per-node consumer, not a consumer per filter.

What is still weaker than MQTT users assume: a shared subscription spanning nodes is served by a NATS queue group on core NATS, so its cross-node hop is still at-most-once; and a node that dies holding a message it has taken but not delivered loses it — for a persistent session on a killed edge, only what the edge took more than five seconds before its last heartbeat and had still not written to the device, since the cores replay from there. A device on an edge that is killed outright is queued for once the cores have gone ten seconds without its edge's heartbeat, and what was published in those seconds is replayed into its queue; a device that reconnects elsewhere sooner receives that part about thirty seconds after its connect rather than at it, and one whose edge was only slow, not dead, may receive some messages twice. All of this is in the delivery guarantees guide.

A per-node consumer is ephemeral and lives on one core. When that core dies, the client is told only that heartbeats stopped, and it went on pulling from a consumer that no longer existed: a core killed under load silently stopped every reader whose consumer it held, edges' bridges included. A missed heartbeat now replaces the consumer from the last message handled.

**Shared subscriptions are exactly-once but not round-robin.** `$share` maps onto a NATS queue group, and a queue group serves a local member first: a leaf node routes to its own members if it has any and only goes upstream when it has none. Verified on a real three-node cluster — with one worker on each of two edge nodes, all twenty jobs went to the worker on whichever node the producer was connected to, and none were duplicated. The MQTT guarantee holds, because each message is still delivered exactly once to exactly one group member. The distribution is not what an MQTT user expects from `$share`, which is usually read as round-robin across the whole group.

This is NATS geo-affinity working as designed, not a defect, and for an edge deployment it is the behavior you want: work stays near the producer instead of crossing the fabric. It is the wrong behavior for a worker pool that is meant to share load evenly across nodes. If even distribution turns out to matter, the fix is not to fight the queue group but to stop relying on it for fan-out — route shared subscriptions through a work-queue stream instead. Until then it is documented, not hidden.

**Correlated events hurt more than throughput.** 300k devices at one message per minute is 5k msg/s, which is nothing. A deploy that reconnects 25k devices per pod, 25k simultaneous authentications, 25k will messages fired by a dying pod, or a `#` subscribe against a large tenant's retained set — those are what break. Ingress accept rate limiting, locally verifiable credentials rather than a synchronous IdP call per connect, and staggered pod restarts are requirements, not optimizations.

**Metric cardinality must be per tenant, never per device.** 300k-series labels will take down Prometheus before they take down the broker.
