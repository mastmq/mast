#!/usr/bin/env bash
# Load: the same four runs against one build, each after an idle gap.
source "$(dirname "$0")/fire.sh"
IMG=${1:?usage: $0 IMAGE}
api() { docker run --rm --network mastfire curlimages/curl:8.11.1 -s -m 5 "$1:8222/jsz" | python3 -c 'import json,sys; print(json.load(sys.stdin)["api"]["total"])'; }
apis() { echo $(( $(api core-0) + $(api core-1) + $(api core-2) )); }
mid() { sleep "$1"; echo "  mid-run:"; stats | sed 's/^/    /'; }
up "$IMG"; sleep 20
echo "## throughput qos1: 20 publishers x 250 msg/s on edge-0, 5 subscribers on edge-1"
mid 15 & mb throughput --broker tcp://edge-0:1883 --sub-broker tcp://edge-1:1883 --qos 1 --publishers 20 --rate 250 --subscribers 5 --duration 30s --prefix l1 | tail -6; wait
sleep 20
echo "## fanout qos1: 2000 subscribers across edges, 20 msg/s"
mid 15 & mb fanout --broker tcp://edge-2:1883 --sub-broker tcp://edge-1:1883 --qos 1 --subscribers 2000 --rate 20 --duration 30s --prefix l2 | tail -6; wait
sleep 20
echo "## connect storm: 6000 clients on edge-0"
mb connect --broker tcp://edge-0:1883 --clients 6000 --hold 5s --prefix l3 | tail -6
sleep 20
echo "## retained storm: 2000 retained topics, 1500 readers subscribing to the wildcard at once"
a0=$(apis); drill retained --devices 1500 --topics 2000 --prefix l4 --concurrency 300; a1=$(apis)
echo "  jetstream api calls on the cores during the storm: $((a1-a0))"
