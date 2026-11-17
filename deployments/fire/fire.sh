#!/usr/bin/env bash
# Helpers for the chaos drills and load runs against the mastfire compose
# cluster. Sourced by the drill scripts; source it yourself to poke at a
# running cluster (stats, consumers, reads).
set -euo pipefail
FIRE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO="$(cd "$FIRE/../.." && pwd)"
export COMPOSE_FILE="$FIRE/compose.yaml"

# A build from before the core queuer (26bf10a) never logs it, so waiting for
# it would wait forever. Set WAIT_QUEUERS=0 when drilling such a baseline.
WAIT_QUEUERS=${WAIT_QUEUERS:-1}

image() { # image [TAG] [REF] — build a mast image, from the working tree or from a git ref
  local tag=${1:-mast:fire} ref=${2:-}
  if [ -z "$ref" ]; then
    docker build -q -f "$REPO/build/package/Dockerfile" -t "$tag" "$REPO"
    return
  fi
  # A baseline from a worktree, so the working tree you are testing is untouched.
  local wt; wt=$(mktemp -d)
  git -C "$REPO" worktree add -q --detach "$wt" "$ref"
  docker build -q -f "$wt/build/package/Dockerfile" -t "$tag" "$wt"
  git -C "$REPO" worktree remove --force "$wt"
}

tools() { # tools — the drill and mastbench, in an image that runs inside the cluster network
  docker build -q -t mastfire-tools "$FIRE"
}

up() { # up IMAGE — a fresh cluster, waited on until every edge serves and every core queues
  docker compose down -v -t 1 >/dev/null 2>&1 || true
  docker volume rm mastfire-state >/dev/null 2>&1 || true
  MAST_IMAGE="$1" docker compose up -d >/dev/null 2>&1
  local i
  for i in $(seq 1 180); do
    if [ "$(edges_ready)" = 3 ] && { [ "$WAIT_QUEUERS" = 0 ] || [ "$(queuers_ready)" -ge 3 ]; }; then
      echo "cluster $1 ready after ${i}s"; sleep 3; return 0
    fi
    sleep 1
  done
  echo "cluster $1 never became ready"; docker compose ps -a; return 1
}

down() { docker compose down -v -t 1; docker volume rm mastfire-state >/dev/null 2>&1 || true; }

edges_ready() { local n=0 e; for e in edge-0 edge-1 edge-2; do
  docker compose logs --no-log-prefix "$e" 2>/dev/null | grep -q "cross-node QoS 1 and 2 ride the durable stream" && \
  [ "$(docker inspect -f '{{.State.Running}}' "mastfire-$e-1" 2>/dev/null)" = true ] && n=$((n+1)); done; echo $n; }

queuers_ready() { docker compose logs --no-log-prefix core-0 core-1 core-2 2>/dev/null | grep -c "queueing for absent sessions.*away" || true; }

wait_edge() { # wait_edge NAME — until the edge's current run serves again
  local i since; for i in $(seq 1 90); do
    since=$(docker inspect -f '{{.State.StartedAt}}' "mastfire-$1-1")
    docker logs --since "$since" "mastfire-$1-1" 2>&1 | grep -q "cross-node QoS 1 and 2 ride the durable stream" && return 0
    sleep 1; done; echo "$1 did not come back"; return 0; }

# Load runs inside the cluster network, never through a published port: on
# colima a port-forward is a userspace proxy, and it is what you would measure.
run() { # run BINARY ARGS...
  docker run --rm --network mastfire --user 0:0 -v mastfire-state:/state --cpus 2 mastfire-tools "/x/$1" "${@:2}"; }
drill() { run drill "$@"; }
mb() { run mastbench "$@"; }

stats() { docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' | grep mastfire | sort; }

consumers() { # consumers [CORE] — mast_qos's consumers: there must be one per node, never more
  docker run --rm --network mastfire curlimages/curl:8.11.1 -s -m 5 "${1:-core-0}:8222/jsz?consumers=true" | python3 -c '
import json,sys
d=json.load(sys.stdin)
for a in d.get("account_details",[]):
  for s in a["stream_detail"]:
    if s["name"]!="mast_qos": continue
    print("mast_qos last_seq", s["state"]["last_seq"])
    for c in s.get("consumer_detail",[]):
      print(" ", c["name"], "host", c.get("cluster",{}).get("leader"), "pending", c["num_pending"], "ack_pending", c["num_ack_pending"], "delivered", c["delivered"]["stream_seq"], "ackfloor", c["ack_floor"]["stream_seq"])
'; }

reads() { # reads — per node: what each reader took off mast_qos, what was queued, who is away
  local c; for c in core-0 core-1 core-2 edge-0 edge-1 edge-2; do printf "  %s " $c
  docker run --rm --network mastfire curlimages/curl:8.11.1 -s -m 5 "$c:9090/metrics" | grep -E '^mast_(durable_read_total|offline_queued_total|queuer_away_sessions)' | sed 's/mast_//; s/_total//' | tr '\n' ' ' || true; echo; done; }
