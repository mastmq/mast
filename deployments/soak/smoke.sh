#!/usr/bin/env bash
# Smoke-tests every feature of a running soak stack end to end, and prints
# the numbers. It kills an edge and stops the cores on the way, and leaves
# the stack as it found it. Run from bash: ./smoke.sh [STEP...], where the
# steps are qos dead limits admin trace readyz dashboard; none means all.
source "$(dirname "$0")/soak.sh"
# soak.sh is strict; a smoke test must report every step rather than stop at
# the first, and `docker logs ... | grep -q` fails under pipefail whenever
# grep matches before docker logs has finished writing.
set +e +o pipefail
RUN=$(date +%s)
ACME=$(token acme)
TINY=$(token tiny)
MOSQ=eclipse-mosquitto:2.1.4-alpine
FAILED=0

say() { printf '%s\n' "$*"; }
pass() { say "  PASS $*"; }
fail() { say "  FAIL $*"; FAILED=$((FAILED + 1)); }

# sub NAME HOST TOKEN ARGS... — a mosquitto_sub in the background, waited on
# until its SUBACK, so a publish after it cannot race the subscription. It
# gets a terminal (-t) because without one it buffers its output, and the
# SUBACK would show only when it exits.
sub() {
  local name=$1 host=$2 tok=$3 i; shift 3
  docker rm -f "$name" >/dev/null 2>&1
  docker run -d -t --name "$name" --network mastsoak "$MOSQ" mosquitto_sub -h "$host" -u "$tok" -d "$@" >/dev/null
  for i in $(seq 1 50); do docker logs "$name" 2>&1 | grep -q "received SUBACK" && return 0; sleep 0.2; done
  say "  $name never got a SUBACK"; docker logs "$name" 2>&1 | tail -3
}

# got NAME — waits for the subscriber to exit and prints "total unique".
got() {
  docker wait "$1" >/dev/null 2>&1
  docker logs "$1" 2>/dev/null | tr -d '\r' | grep -E '^[0-9]+$' | sort | awk '{n++; if ($0 != last) u++; last=$0} END {print n+0, u+0}'
  docker rm -f "$1" >/dev/null 2>&1
}

pubn() { # pubn HOST TOKEN QOS TOPIC N — publish 1..N on one connection
  seq 1 "$5" | docker run --rm -i --network mastsoak "$MOSQ" mosquitto_pub -h "$1" -V 5 -u "$2" -q "$3" -t "$4" -l -i "smoke-pub-$RUN-$RANDOM"
}

wait_node() { # wait_node NODE CODE SECONDS — prints the seconds until /readyz answered CODE
  local t0=$SECONDS
  while [ $((SECONDS - t0)) -lt "$3" ]; do [ "$(ready "$1")" = "$2" ] && { echo $((SECONDS - t0)); return 0; }; sleep 0.5; done
  echo never; return 1
}

step_qos() {
  say "## QoS 0/1/2: 200 messages published on edge-0, subscribed on edge-1 (tenant acme)"
  local q r
  for q in 0 1 2; do
    sub "smoke-qos$q" edge-1 "$ACME" -V 5 -q $q -t "smoke/qos$q/$RUN" -i "smoke-qos$q-$RUN" -C 200 -W 30
    pubn edge-0 "$ACME" $q "smoke/qos$q/$RUN" 200
    r=$(got "smoke-qos$q")
    say "  qos $q: received ${r% *} of 200, ${r#* } unique"
    [ "${r#* }" = 200 ] && pass "qos $q" || fail "qos $q"
  done
}

step_dead() {
  say "## A killed edge: a connected and a sleeping persistent session on edge-0, docker kill, 100 QoS 1 each published at once, then both resume on edge-1"
  local live="smoke-live-$RUN" sleep="smoke-sleep-$RUN" r t0
  # The sleeper subscribes and leaves at once (-E): its session is away
  # before the kill. The live one is still connected when the edge dies.
  docker run --rm --network mastsoak "$MOSQ" mosquitto_sub -h edge-0 -u "$ACME" -V 5 -c -x 3600 -q 1 -i "$sleep" -t "smoke/sleep/$RUN" -E
  sub smoke-live edge-0 "$ACME" -V 5 -c -x 3600 -q 1 -i "$live" -t "smoke/live/$RUN"
  docker kill mastsoak-edge-0-1 >/dev/null
  docker rm -f smoke-live >/dev/null 2>&1 # it would reconnect to edge-0 when it comes back
  t0=$SECONDS
  pubn edge-1 "$ACME" 1 "smoke/live/$RUN" 100
  pubn edge-1 "$ACME" 1 "smoke/sleep/$RUN" 100
  say "  published 200 within $((SECONDS - t0))s of the kill; waiting 20s for the cores to declare edge-0 dead"
  sleep 20
  docker compose logs --no-log-prefix --since 40s core-0 core-1 core-2 2>/dev/null | grep -iE "dead|silent" | head -3 | sed 's/^/    /'
  for s in live sleep; do
    local id="smoke-$s-$RUN"
    docker rm -f "smoke-resume-$s" >/dev/null 2>&1
    docker run -d -t --name "smoke-resume-$s" --network mastsoak "$MOSQ" mosquitto_sub -h edge-1 -u "$ACME" -V 5 -c -x 3600 -q 1 -i "$id" -t "smoke/$s/$RUN" -W 15 >/dev/null
  done
  for s in live sleep; do
    r=$(got "smoke-resume-$s")
    say "  $s session: received ${r% *} after resuming on edge-1, ${r#* } unique of 100"
    [ "${r#* }" = 100 ] && pass "$s session after the kill" || fail "$s session after the kill"
  done
  docker start mastsoak-edge-0-1 >/dev/null
  say "  edge-0 restarted, ready after $(wait_node edge-0 200 90)s"
  # The sessions are left to expire; clear them so a rerun starts clean.
  for s in live sleep; do docker run --rm --network mastsoak "$MOSQ" mosquitto_sub -h edge-1 -u "$ACME" -V 5 -i "smoke-$s-$RUN" -t x -E >/dev/null 2>&1; done
}

step_limits() {
  say "## Tenant limits: tiny has publish_rate 5, burst 5, max_connections 3"
  local out ok refused
  out=$(docker run --rm --network mastsoak "$MOSQ" mosquitto_pub -h edge-0 -V 5 -u "$TINY" -i "tiny-q1-$RUN" -q 1 -t r/x -m hi --repeat 12 --repeat-delay 0 -d 2>&1)
  ok=$(grep -c "PUBACK (Mid: [0-9]*, RC:0)" <<<"$out"); refused=$(grep -c "RC:151" <<<"$out")
  say "  MQTT 5 QoS 1, 12 at once: $ok PUBACK success, $refused PUBACK 0x97 Quota exceeded"
  [ "$ok" = 5 ] && [ "$refused" = 7 ] && pass "QoS 1 over the rate gets 0x97" || fail "QoS 1 over the rate: $out"
  sleep 2
  out=$(docker run --rm --network mastsoak "$MOSQ" mosquitto_pub -h edge-0 -V 5 -u "$TINY" -i "tiny-q2-$RUN" -q 2 -t r/x -m hi --repeat 12 --repeat-delay 0 -d 2>&1)
  say "  MQTT 5 QoS 2, 12 at once: $(grep -o 'Received DISCONNECT ([0-9]*)' <<<"$out" | head -1)"
  grep -q "Received DISCONNECT (150)" <<<"$out" && pass "QoS 2 over the rate is disconnected with 0x96" || fail "QoS 2 over the rate: $out"
  sleep 2
  out=$(docker run --rm --network mastsoak "$MOSQ" mosquitto_pub -h edge-0 -V 311 -u "$TINY" -i "tiny-v3-$RUN" -q 1 -t r/x -m hi --repeat 12 --repeat-delay 0 -d 2>&1)
  say "  MQTT 3.1.1 QoS 1, 12 at once: $(grep -c 'received PUBACK' <<<"$out") PUBACK, then: $(grep -iE 'error|lost' <<<"$out" | head -1)"
  [ "$(grep -c 'received PUBACK' <<<"$out")" -lt 12 ] && pass "3.1.1 QoS 1 over the rate loses its connection" || fail "3.1.1 over the rate: $out"
  local i
  for i in 1 2 3; do sub "smoke-tiny-$i" edge-0 "$TINY" -V 5 -i "tiny-hold-$i-$RUN" -t hold; done
  sleep 5 # two sync intervals, so edge-1 has heard edge-0's count
  out=$(docker run --rm --network mastsoak "$MOSQ" mosquitto_sub -h edge-1 -V 5 -u "$TINY" -i "tiny-4th-$RUN" -t hold -E 2>&1)
  say "  a 4th MQTT 5 connection, on edge-1: $out"
  grep -q "Quota exceeded" <<<"$out" && pass "connection limit across edges, MQTT 5 0x97" || fail "connection limit, v5"
  out=$(docker run --rm --network mastsoak "$MOSQ" mosquitto_sub -h edge-1 -V 311 -u "$TINY" -i "tiny-4th-v3-$RUN" -t hold -E 2>&1)
  say "  a 4th MQTT 3.1.1 connection, on edge-1: $out"
  grep -qi "unavailable" <<<"$out" && pass "connection limit across edges, 3.1.1 0x03" || fail "connection limit, v3"
  for i in 1 2 3; do docker rm -f "smoke-tiny-$i" >/dev/null 2>&1; done
}

step_admin() {
  say "## Admin API on edge-0 (127.0.0.1:${ADMIN_PORT:-8080}) over a client on edge-1"
  local id="smoke-admin-$RUN" tok out code
  tok=$(cat "$SOAK/secrets/admin.token")
  sub smoke-admin edge-1 "$ACME" -V 5 -i "$id" -t "smoke/admin/$RUN"
  out=$(curl -s -m 5 -H "Authorization: Bearer $tok" "http://127.0.0.1:${ADMIN_PORT:-8080}/v1/tenants/acme/sessions")
  say "  sessions: $(python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("sessions",[])), "listed, nodes answering:", d.get("nodes"), "| ours:", [s for s in d.get("sessions",[]) if s.get("client_id")==sys.argv[1]])' "$id" <<<"$out" 2>&1)"
  grep -q "\"$id\"" <<<"$out" && pass "the list shows a client on the other edge" || fail "list: $out"
  say "  stats: $(curl -s -m 5 -H "Authorization: Bearer $tok" "http://127.0.0.1:${ADMIN_PORT:-8080}/v1/tenants/acme/stats")"
  code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 -H "Authorization: Bearer nope-nope-nope-nope" "http://127.0.0.1:${ADMIN_PORT:-8080}/v1/tenants/acme/stats")
  [ "$code" = 401 ] && pass "a wrong token gets 401" || fail "a wrong token got $code"
  code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 -X POST -H "Authorization: Bearer $tok" "http://127.0.0.1:${ADMIN_PORT:-8080}/v1/tenants/acme/sessions/$id/disconnect")
  sleep 1
  say "  disconnect: HTTP $code; the client heard: $(docker logs smoke-admin 2>&1 | grep -o 'Received DISCONNECT ([0-9]*)' | head -1)"
  docker logs smoke-admin 2>&1 | grep -q "Received DISCONNECT (152)" && pass "disconnected with 0x98 Administrative action" || fail "admin disconnect: $(docker logs smoke-admin 2>&1 | tail -3)"
  docker rm -f smoke-admin >/dev/null 2>&1
}

step_trace() {
  say "## A trace across two nodes in Jaeger (sampler ratio ${TRACE_RATIO:-0.05})"
  sub smoke-trace edge-1 "$ACME" -V 5 -q 1 -t "smoke/trace/$RUN" -i "smoke-trace-$RUN" -C 300 -W 30
  pubn edge-0 "$ACME" 1 "smoke/trace/$RUN" 300
  got smoke-trace >/dev/null
  sleep 8 # the exporter batches every 5s, then the collector
  local from to # date -r means a file to GNU date and seconds to BSD date
  from=$(python3 -c 'import datetime as d; print((d.datetime.now(d.UTC) - d.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
  to=$(python3 -c 'import datetime as d; print((d.datetime.now(d.UTC) + d.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
  curl -s -m 10 "http://127.0.0.1:${JAEGER_PORT:-16686}/api/v3/traces?query.service_name=mast&query.start_time_min=$from&query.start_time_max=$to&query.num_traces=200" | python3 -c '
import json, sys, collections
d = json.load(sys.stdin)
traces = collections.defaultdict(list)
for rs in d.get("result", {}).get("resourceSpans", []):
    node = next((a["value"]["stringValue"] for a in rs["resource"]["attributes"] if a["key"] == "service.instance.id"), "?")
    for ss in rs["scopeSpans"]:
        for s in ss["spans"]:
            traces[s["traceId"]].append((node, s["name"]))
multi = {t: v for t, v in traces.items() if len({n for n, _ in v}) >= 2}
print("  %d traces in the last five minutes, %d with spans from two or more nodes" % (len(traces), len(multi)))
for t, v in list(multi.items())[:1]:
    print("  e.g. trace", t, ":", ", ".join("%s@%s" % (s, n) for n, s in sorted(v)))
sys.exit(0 if multi else 1)' && pass "a trace spans two nodes" || fail "no trace spans two nodes"
}

step_readyz() {
  say "## /readyz on edge-0 while every core is stopped, and a QoS 1 publish meanwhile"
  say "  before: $(ready edge-0)"
  docker compose stop -t 5 core-0 core-1 core-2 >/dev/null 2>&1
  local t; t=$(wait_node edge-0 503 60)
  say "  cores stopped; edge-0 answered 503 after ${t}s: $(docker run --rm --network mastsoak curlimages/curl:8.11.1 -s -m 3 http://edge-0:9090/readyz)"
  [ "$t" != never ] && pass "readyz 503 without the cores" || fail "readyz stayed 200 without the cores"
  local out; out=$(docker run --rm --network mastsoak "$MOSQ" mosquitto_pub -h edge-0 -V 5 -u "$ACME" -i "smoke-nocore-$RUN" -q 1 -t smoke/nocore -m x --repeat 3 -d 2>&1)
  say "  QoS 1 with no core: $(grep -oE 'RC:[0-9]+|failed: [^.]*' <<<"$out" | sort | uniq -c | tr '\n' ' ')"
  docker compose start core-0 core-1 core-2 >/dev/null 2>&1
  t=$(wait_node edge-0 200 120)
  say "  cores started; edge-0 answered 200 after ${t}s"
  [ "$t" != never ] && pass "readyz back to 200" || fail "readyz never came back"
  wait_ready >/dev/null
  say "## The alert that publish caused (Prometheus scrapes and evaluates every 15s)"
  local i a
  for i in $(seq 1 12); do
    a=$(alerts 2>/dev/null | grep "firing MastDurablePublishFailures")
    [ -n "$a" ] && break; sleep 10
  done
  say "  $(alerts 2>/dev/null | tr '\n' ';')"
  [ -n "$a" ] && pass "MastDurablePublishFailures fired: $a" || fail "MastDurablePublishFailures did not fire"
}

step_dashboard() {
  say "## The chart's dashboard in Grafana: every panel's queries through Grafana's Prometheus proxy"
  local g="http://127.0.0.1:${GRAFANA_PORT:-3000}" uid
  # Two panels count what no other step causes: a refused login and a
  # retained replay. One of each, then two scrapes' wait.
  docker run --rm --network mastsoak "$MOSQ" mosquitto_sub -h edge-0 -V 5 -u not-a-token -i "smoke-badauth-$RUN" -t x -E >/dev/null 2>&1
  docker run --rm --network mastsoak "$MOSQ" mosquitto_pub -h edge-0 -V 5 -u "$ACME" -i "smoke-ret-$RUN" -q 1 -r -t "smoke/retained" -m "$RUN"
  docker run --rm --network mastsoak "$MOSQ" mosquitto_sub -h edge-1 -V 5 -u "$ACME" -i "smoke-retsub-$RUN" -t "smoke/retained" -C 1 -W 5 >/dev/null
  sleep 35
  uid=$(curl -s -m 5 "$g/api/search?type=dash-db&query=mast" | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["uid"])')
  curl -s -m 10 "$g/api/dashboards/uid/$uid" | G=$g python3 -c '
import json, os, sys, urllib.parse, urllib.request
d = json.load(sys.stdin)["dashboard"]
empty, total = [], 0
for p in d["panels"]:
    if p.get("type") == "row":
        continue
    total += 1
    n = 0
    for t in p.get("targets", []):
        q = t["expr"].replace("$namespace", "soak").replace("$pod", ".*").replace("$tenant", ".*").replace("$__rate_interval", "2m").replace("$__range", "1h")
        url = os.environ["G"] + "/api/datasources/proxy/uid/prometheus/api/v1/query?" + urllib.parse.urlencode({"query": q})
        n += len(json.load(urllib.request.urlopen(url))["data"]["result"])
    print("  %-45s %d series" % (p["title"], n))
    if n == 0:
        empty.append(p["title"])
print("  %d of %d panels have data; empty: %s" % (total - len(empty), total, ", ".join(empty) or "none"))
sys.exit(1 if len(empty) > total // 4 else 0)' && pass "the dashboard has data" || fail "most panels are empty"
}

steps=${*:-qos dead limits admin trace readyz dashboard}
for s in $steps; do "step_$s"; done
say "## $FAILED failed"
exit $FAILED
