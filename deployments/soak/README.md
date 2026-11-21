# soak

A mast stack in Docker meant to stay up for days: three cores, two edges and a standalone broker, with authentication by token, two tenants with limits, the admin API, traces through an OpenTelemetry collector into Jaeger, Prometheus with the chart's alert rules, and Grafana with the chart's dashboard. Where `deployments/fire` is for short, violent drills that end with `down`, this one is for leaving load on it overnight and coming back to see what the dashboard, the alerts and the traces say.

Three defects were found while building it, each now fixed with a regression test: spans were posted to `/` and every collector answered 404 (the documented `endpoint` form had never exported anything), an over-rate MQTT 5 QoS 1 publish got a PUBACK with 0x96, which MQTT 5 does not allow there and which mosquitto treats as a protocol error, and every edge counted every QoS 1 and 2 message in the cluster as a message out.

## Starting it

From bash, at this directory:

```console
$ ./soak.sh image        # mast:soak from this checkout
$ ./soak.sh up           # secrets on first run, then the stack, then waits for all six nodes
$ ./soak.sh tools        # mastbench from the bench repo's main, for load
$ ./smoke.sh             # every feature end to end, about two minutes
```

`./soak.sh help` lists every helper; `source soak.sh` makes them shell functions. `./soak.sh down` stops everything and deletes the volumes (Prometheus' history and Grafana's state).

`up` writes `secrets/jwt.secret` and `secrets/admin.token` once, with `openssl rand`; git ignores the folder. They are readable by any local user (mode 644), because the broker runs as the image's nonroot user, not as you.

To soak another build, `./soak.sh image mast:other` and `MAST_IMAGE=mast:other ./soak.sh up`.

## What runs and where

Every published port is bound to 127.0.0.1; the defaults can be moved with the variable named.

| Port | Service | Variable |
| --- | --- | --- |
| 1883 | MQTT on edge-0 | `EDGE0_MQTT_PORT` |
| 1884 | MQTT on edge-1 | `EDGE1_MQTT_PORT` |
| 1885 | MQTT on the standalone broker | `STANDALONE_MQTT_PORT` |
| 8080 | admin API on edge-0, for the whole cluster | `ADMIN_PORT` |
| 8081 | admin API on the standalone broker | `STANDALONE_ADMIN_PORT` |
| 3000 | Grafana, opening on the mast dashboard, no login | `GRAFANA_PORT` |
| 9090 | Prometheus, with the alerts under Alerts | `PROMETHEUS_PORT` |
| 16686 | Jaeger, service `mast` | `JAEGER_PORT` |

Inside the `mastsoak` network every node also serves `:9090` (metrics, `/healthz`, `/readyz`, pprof), `:8222` (the NATS monitor) and `:8080` (admin), and each edge has an unauthenticated internal listener on `:1884` that puts every client in tenant `default`. That listener is what load uses, because a token per client is work for the generator rather than the broker.

`mast.toml` is the configuration every node shares; `compose.yaml` sets each node's role and cluster wiring on top of it with `MAST__` variables.

## Tenants and tokens

Authentication is `jwt` with HS256 and the tenant in the `tenant` claim. `./soak.sh token acme` prints a token valid for 30 days; it goes in the MQTT username.

| Tenant | Limits |
| --- | --- |
| `acme` | 5,000 connections, 5,000 publishes a second per node |
| `tiny` | 3 connections, 5 publishes a second with a burst of 5, 10 queued messages per absent session |
| anything else | unlimited |

```console
$ mosquitto_sub -h 127.0.0.1 -p 1884 -V 5 -u "$(./soak.sh token acme)" -t 'sensors/#' -q 1
$ mosquitto_pub -h 127.0.0.1 -p 1883 -V 5 -u "$(./soak.sh token acme)" -t sensors/a -m 21 -q 1
```

Through a published port is fine for poking at it. For anything measured, run the client inside the network (`./soak.sh mosq mosquitto_pub -h edge-0 ...`), since on colima a published port is a userspace proxy.

## The admin API

```console
$ TOKEN=$(cat secrets/admin.token)
$ curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/v1/tenants/acme/sessions
$ curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/v1/tenants/acme/stats
$ curl -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/v1/tenants/acme/sessions/<client-id>/disconnect
```

Any node answers for the whole cluster; edge-0 is the one published.

## Load

`mastbench` from [mastmq/bench](https://github.com/mastmq/bench), built into `mastfire-tools` by `./soak.sh tools`. `mb` runs one generator inside the network; `load N` splits one run over N generator containers, worker 0 printing the merged result:

```console
$ ./soak.sh load 2 throughput --broker tcp://edge-0:1884 --sub-broker tcp://edge-1:1884 \
    --qos 1 --publishers 10 --rate 100 --subscribers 5 --duration 60s --prefix soak
$ ./soak.sh mb fanout --broker tcp://edge-0:1884 --sub-broker tcp://edge-1:1884 --qos 1 --subscribers 2000 --rate 20 --duration 10m --prefix soak
$ ./soak.sh mb connect --broker tcp://edge-1:1884 --clients 5000 --hold 30m --prefix soak
```

For a soak rather than a benchmark, set `--duration` in hours and watch Grafana. The bench repo's CLAUDE.md says how not to fool yourself: stay under about 16k clients on one host, and leave a minute between runs.

`TRACE_RATIO` (0.05) is the share of publishes traced. Leave it low under load, or the collector and Jaeger become what is measured.

## Faults

The things worth doing to it while load runs, and what each should show. `smoke.sh` checks the edge kill, the loss of every core and the limits; the other rows are what the design says, for a soak round to confirm or refute.

| Fault | Command | Should show |
| --- | --- | --- |
| kill an edge | `docker kill mastsoak-edge-0-1`, later `docker start mastsoak-edge-0-1` | the cores declare it dead after ten silent seconds and queue for its persistent sessions; a device resuming on edge-1 gets what it missed |
| restart an edge | `docker compose restart edge-1` | a graceful goodbye, no dead-node replay |
| lose a core | `docker kill mastsoak-core-2-1` | quorum holds with two; `MastCoreNotReady` pending, then firing after 10 minutes |
| lose every core | `docker compose stop core-0 core-1 core-2`, then `start` | edges answer `/readyz` 503 within a second; QoS 1/2 publishes are refused (MQTT 5 reason 0x83) and `MastDurablePublishFailures` fires |
| freeze a node | `docker pause mastsoak-edge-1-1`, then `unpause` | like a kill to the cores, but the node comes back with its connections; duplicates, not loss |
| over a limit | `./smoke.sh limits` | 0x97 on the fourth `tiny` connection and on a QoS 1 publish past the rate |

`./soak.sh alerts` lists what Prometheus has pending or firing. The fire drills (`deployments/fire`) count loss message by message and are the place for a delivery claim; this stack is for watching the whole system behave over time.

## How it is wired, and why

**The alerts are the chart's.** `./soak.sh rules` renders `templates/prometheusrule.yaml` from a `mastmq/charts` checkout beside this repo (`CHARTS` to point elsewhere) with `ci/monitoring-values.yaml` and `ci/standalone-monitoring-values.yaml`, extracts the rule groups into `prometheus/rules/`, copies `dashboards/mast.json` into `grafana/dashboards/`, and checks the result with promtool. The rendered files are committed so the stack starts without the chart; re-run it when the chart changes.

**Each target carries the labels its pod would have under `helm install mast -n soak`**: `namespace`, `service` and `pod`, set in `prometheus/prometheus.yml`. The rules and the dashboard select on exactly those, so they run unchanged. Edge pods are named like a Deployment's (`mast-edge-soak-0`) because `MastEdgeCannotReachCore` matches that shape; `node` is the compose service name. The standalone broker is `namespace="soak-standalone"`.

**Readiness alerts read kube-state-metrics** in Kubernetes. Here the blackbox exporter probes each node's `/readyz` and `prometheus/rules/readiness.yaml` records `kube_pod_status_ready` from it, so `MastCoreNotReady`, `MastEdgeCannotReachCore` and `MastNotReady` fire as they would in a cluster.

**The collector sits between mast and Jaeger** rather than mast exporting to Jaeger's own OTLP port, because that is the shape of a real deployment and where sampling or a second backend would go.

**No restart policy brings back a node a fault stopped.** mast nodes restart only on failure, so a `docker kill` stays killed until you start it, and `docker compose stop` stays stopped.
