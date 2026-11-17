#!/usr/bin/env bash
# Connected devices under steady QoS 1 traffic while nodes die.
source "$(dirname "$0")/fire.sh"
IMG=${1:?usage: $0 IMAGE}; N=${2:-3000}; R=${3:-2000}
up "$IMG"
drill live --devices "$N" --rate "$R" --duration 60s --wait 60s --prefix d3 --publishers 40 &
P=$!
sleep 25; docker kill mastfire-edge-0-1 >/dev/null; echo "t+15 SIGKILL edge-0"
sleep 5;  docker start mastfire-edge-0-1 >/dev/null; echo "t+20 edge-0 started"
sleep 10; docker kill mastfire-core-2-1 >/dev/null; echo "t+30 SIGKILL core-2"
sleep 10; docker start mastfire-core-2-1 >/dev/null; echo "t+40 core-2 started"
sleep 5;  docker restart -t 5 mastfire-edge-1-1 >/dev/null; echo "t+45 graceful restart edge-1"
wait $P
stats
