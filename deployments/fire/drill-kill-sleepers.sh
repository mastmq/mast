#!/usr/bin/env bash
# Sleeping devices, with a core and two edges killed hard while traffic flows.
source "$(dirname "$0")/fire.sh"
IMG=${1:?usage: $0 IMAGE}; N=${2:-5000}; M=${3:-20}
up "$IMG"
drill setup --devices "$N" --prefix d2
sleep 3
echo "before publish:"; reads
drill publish --devices "$N" --per-device "$M" --prefix d2 --publishers 40 &
P=$!
sleep 2; docker kill mastfire-core-1-1 >/dev/null; echo "killed core-1 (SIGKILL)"
sleep 1; docker kill mastfire-edge-1-1 >/dev/null; echo "killed edge-1 (SIGKILL)"
wait $P
docker start mastfire-core-1-1 mastfire-edge-1-1 >/dev/null; wait_edge edge-1; echo "core-1 and edge-1 back"
docker kill mastfire-edge-2-1 >/dev/null; docker start mastfire-edge-2-1 >/dev/null; wait_edge edge-2; echo "edge-2 killed and back"
sleep 3
sleep 10; reads
drill verify --devices "$N" --prefix d2 --wait 60s
stats
