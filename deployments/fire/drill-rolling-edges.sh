#!/usr/bin/env bash
# Devices asleep across a rolling deploy of every edge.
source "$(dirname "$0")/fire.sh"
IMG=${1:?usage: $0 IMAGE}; N=${2:-2000}
up "$IMG"
drill setup --devices "$N" --prefix d1
sleep 3
for e in edge-0 edge-1 edge-2; do
  docker restart -t 2 "mastfire-$e-1" >/dev/null; wait_edge "$e" || echo "$e did not come back"
  echo "rolled $e"
done
sleep 2
drill publish --devices "$N" --per-device 5 --prefix d1
sleep 3
drill verify --devices "$N" --prefix d1 --wait 30s
