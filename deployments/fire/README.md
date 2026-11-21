# fire

A three-core, three-edge mast cluster in Docker, and the drills that break it on purpose. Everything in the 2026-11 storage change that unit and cluster tests never showed was found here: edges creating every stream at one replica, a core with no client address that never formed a cluster, a node's durable consumer silently dying with the core that held it, and an offline queue that lost 10,000 of 10,000 messages to a rolling edge restart. The in-process cluster tests cannot find these, because they never SIGKILL anything and never place consumers on a core that then dies.

The results are on [the website](https://mastmq.github.io/#under-fire).

## What is here

| File | What |
| --- | --- |
| `compose.yaml` | three cores (R3) and three edges, each limited to one CPU and 1 GiB |
| `fire.sh` | helpers every script sources: `image`, `tools`, `up`, `down`, `drill`, `mb`, `stats`, `consumers`, `reads` |
| `drill/` | the drill tool. It counts by payload, not by volume: every message names its device and sequence, so a report says which acknowledged messages never arrived and which arrived twice. A module of its own so the broker's lint rules and dependencies stay out of it |
| `Dockerfile` | an image holding the drill and [`mastbench`](https://github.com/mastmq/bench), run inside the compose network |
| `drill-rolling-edges.sh` | persistent devices asleep while every edge restarts in turn, then woken on a different edge |
| `drill-kill-sleepers.sh` | sleeping devices, with a core and two edges SIGKILLed while their messages are published |
| `drill-live.sh` | connected devices under steady QoS 1 while an edge and a core are killed and another edge restarts |
| `drill-scale.sh` | a fleet's worth of sleeping sessions — 300,000 by default, written straight into the sessions bucket by `drill seed` because one laptop cannot hold that many sockets — then load, a core restart, queueing for them, and 2,000 real sleepers among them |
| `load.sh` | QoS 1 throughput, fan-out, a connect storm and a retained storm, with CPU sampled mid-run and JetStream API calls counted |

## Running it

From bash, at this directory:

```console
$ source fire.sh
$ image                  # mast:fire from the working tree
$ tools                  # mastfire-tools
$ ./drill-rolling-edges.sh mast:fire 2000
$ ./load.sh mast:fire
$ down
```

To compare against another build, build it from a ref into its own tag. A build from before the core queuer (`26bf10a`) never logs that it is queueing, so tell `up` not to wait for it:

```console
$ image mast:base 3ae6982
$ WAIT_QUEUERS=0 ./drill-rolling-edges.sh mast:base 2000
```

`CORE_CPUS` and `GOGC` in the environment of `up` change the cores' CPU limit and every node's GC target; the scale drill's tail latency was a one-CPU core sharing its processor with the collector, which is how both came to be knobs.

While a cluster is up, `reads` prints per node what each reader took off `mast_qos`, what it queued and how many sessions it holds as away, and `consumers` lists `mast_qos`'s consumers, which must be one per node and never more. Read both before guessing why something was lost.

## Things learned running it

**A total does not say which kill lost it.** `drill live` prints each lost message's device and publish time, and the drills are scripts, so split one: for dead-edge detection (#34), `drill-live.sh` with only its edge kill went from 64 to 1,556 lost on `main` to none in three runs of three, while the core kill and the graceful restart alone lost the same handful on both builds — every lost message published within one tenth of a second, about ten seconds after the graceful restart (#40). The full drill had hidden that behind one number per run. The edge kill now costs about 2,300 duplicates and a p99 of about 38 seconds instead, since a device that already moved receives the gap through its edge's straggler check.

**Run every delivery drill at least four times and against a baseline.** Loss under chaos depends on which core happened to hold which consumer when it died; one failure surfaced once in four runs. A single green run proves nothing, and a number with no baseline beside it cannot say whether the change helped.

**Load runs inside the network, never through a published port.** On colima a published port is a userspace proxy, and that is what gets measured. `run` puts the generator on the `mastfire` network with two CPUs of its own.

**One laptop has one shared pool of ephemeral ports.** Past roughly 16k clients a run reports lost connections that are the host's, not the broker's. The bench repo's CLAUDE.md has the detail.

**colima does not mount paths outside your home directory**, which is why the tools are baked into an image and the drill's state lives in the `mastfire-state` volume rather than a bind mount.

**Give the cluster a minute between runs.** `up` tears down and recreates it, but `load.sh` still idles twenty seconds between scenarios so one run's teardown is not measured as the next one's start.
