# Soak log

One section per round. Each section has what ran, the numbers, and what the numbers mean against `docs/guides/delivery-guarantees.md`.

## Round 1, 2026-11-21, 07:09 to 07:20 Madrid

The round started at 07:09, and the workflow stops at 07:30. So it ran two scenarios, not 45 minutes of load. The stack ran mast main 32ad734 (`mast:soak`).

**Rolling core restart under QoS 2.** The test sent 10 publishers at 100 msg/s on edge-0 to 5 subscribers on edge-1, for 210 s, through `./soak.sh load 2`. It ran `docker compose restart` on core-1, core-2 and core-0, 40 s apart. mastbench reported 210,000 sent and 613,770 of 1,050,000 deliveries (41.55 % "loss"). p50 was 717 µs, p99 830 ms and the maximum 2.0 s. The loss number does not measure the broker. mastbench clients have auto-reconnect off. The broker disconnected publishers whose QoS 2 publish it refused, and mastbench still counted their unsent messages as sent. Prometheus agrees: `mast_messages_in_total` on edge-0 was 1000/s, then 900/s from 06:11:23 UTC (one publisher refused during the core-2 restart), then 0 from 06:12:00 UTC (all ten refused when core-0 stopped). Out on edge-1 followed in and then stopped. No acknowledged publish was shown to be lost. The refused publishes were never acknowledged, so the delivery guarantees allow this.

**The defect it found.** core-0 held the JetStream metadata and stream leaderships when it stopped. A graceful core shutdown calls `ns.Shutdown()` without lame-duck mode or a leader step-down. For about 1 s, every durable publish failed with `nats: no response from stream`, although two cores stayed up the whole time. A 3.1.1 client can only be refused by a disconnect, so every core deploy disconnects every QoS 1/2 publisher that has a publish in flight at that moment. `MastDurablePublishFailures` fired for mast-edge-soak-0. Filed as [mast#44](https://github.com/mastmq/mast/issues/44) and not fixed tonight. The fix changes shutdown ordering in a Raft cluster and needs a cluster test, which did not fit in the time left. The "dropping session ... no responders" warnings at the same moment are the clean-session delete when a disconnected client leaves. They fail during the leader gap and do no harm for clean clients.

**Baseline QoS 1 after the restarts, no faults.** 10 publishers at 100 msg/s to 5 subscribers, 60 s: 60,000 sent, 300,000 of 300,000 delivered, 0.00 % loss. p50 1.5 ms, p95 3.1 ms, p99 5.0 ms, max 48 ms. mastbench does not report duplicates for throughput runs, so this run does not count duplicates.

**Goroutines and memory.** Before the round: edges 43 goroutines each, RSS 40 to 43 MB. Cores 94 to 98 goroutines, RSS 46 to 52 MB. After the round: edges 43 goroutines, RSS 42 to 46 MB. Cores 91 to 108 goroutines, RSS 74 to 113 MB. The cores restarted during the round, so their figures are not a leak measurement. The edges did not grow.

**Not covered this round:** edge kill under load with a client that reconnects, a network partition, limits with a measured neighbour, admin API under load, sleeping sessions and a reconnect storm, retained messages and wills across a core restart, tracing on vs off, alerts resolving.
