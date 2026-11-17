#!/usr/bin/env bash
# The core tier holding a fleet-sized number of sleeping sessions: how long
# a queuer takes to load them, what they cost in memory and in CPU per
# message, and whether real sleepers among them still get everything.
source "$(dirname "$0")/fire.sh"
IMG=${1:?usage: $0 IMAGE [SESSIONS]}; S=${2:-300000}

away() { docker run --rm --network mastfire curlimages/curl:8.11.1 -s -m 5 "$1:9090/metrics" | awk '/^mast_queuer_away_sessions /{print int($2)}'; }
# until_away CORE N — wait for the core's queuer to hold N, or report the
# core's death: a core killed for memory restarts and starts loading again.
until_away() { local i; for i in $(seq 1 300); do
  [ "$(away "$1" 2>/dev/null)" -ge "$2" ] 2>/dev/null && return 0; sleep 1; done
  echo "  $1 never held $2 away (holds $(away "$1"), restarts=$(docker inspect -f '{{.RestartCount}}' "mastfire-$1-1"))"; return 0; }
oomed() { colima ssh -- sudo dmesg 2>/dev/null | grep -c "Killed process" || true; }

up "$IMG"
k0=$(oomed)
echo "## idle, empty"; stats

echo "## seeding $S away sessions"
t0=$(date +%s); drill seed --sessions "$S" --prefix sc
for c in core-0 core-1 core-2; do
  until_away "$c" "$S"
  echo "  $c holds $(away $c) away, $(( $(date +%s) - t0 ))s after the seed began"
done
sleep 10; echo "## idle, $S away"; stats

echo "## restarting core-1 with $S away"
docker restart -t 10 mastfire-core-1-1 >/dev/null; t0=$(date +%s)
until_away core-1 "$S"
echo "  core-1 queueing for $(away core-1) again after $(( $(date +%s) - t0 ))s"
docker logs mastfire-core-1-1 2>&1 | grep "queueing for absent sessions" | tail -1

sleep 20
echo "## throughput qos1: 20 publishers x 250 msg/s, matched against $S away sessions"
( sleep 15; echo "  mid-run:"; stats | sed 's/^/    /' ) &
mb throughput --broker tcp://edge-0:1883 --sub-broker tcp://edge-1:1883 --qos 1 --publishers 20 --rate 250 --subscribers 5 --duration 30s --prefix sl | tail -6; wait

sleep 10
echo "## qos1 to the seeded sessions themselves: 1,000 msg/s for 30s"
( sleep 15; echo "  mid-run:"; stats | sed 's/^/    /' ) &
drill seedpub --sessions "$S" --rate 1000 --duration 30s --publishers 20 --prefix sc; wait

echo "## 2,000 real sleepers among them"
drill setup --devices 2000 --prefix sr
sleep 3
drill publish --devices 2000 --per-device 5 --prefix sr
sleep 3
drill verify --devices 2000 --prefix sr --wait 30s
stats
for c in core-0 core-1 core-2; do echo "  $c restarts=$(docker inspect -f '{{.RestartCount}}' "mastfire-$c-1")"; done
echo "  processes the kernel killed for memory during the run: $(( $(oomed) - k0 ))"
