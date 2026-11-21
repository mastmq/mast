#!/usr/bin/env bash
# The soak stack's helpers. Run `./soak.sh <command>`, or source it to call
# the functions from a shell. `./soak.sh help` lists them.
set -euo pipefail
SOAK="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO="$(cd "$SOAK/../.." && pwd)"
CHARTS="${CHARTS:-$(cd "$REPO/.." && pwd)/charts}"
export COMPOSE_FILE="$SOAK/compose.yaml"
export COMPOSE_PROJECT_NAME=mastsoak

init() { # init — the JWT secret and the admin token in secrets/, once; git ignores the folder
  mkdir -p "$SOAK/secrets"
  # Readable by the container's nonroot user, which is not the host's.
  [ -s "$SOAK/secrets/jwt.secret" ] || { openssl rand -hex 32 >"$SOAK/secrets/jwt.secret"; chmod 644 "$SOAK/secrets/jwt.secret"; }
  [ -s "$SOAK/secrets/admin.token" ] || { openssl rand -hex 24 >"$SOAK/secrets/admin.token"; chmod 644 "$SOAK/secrets/admin.token"; }
}

image() { # image [TAG] — build mast from this checkout's working tree
  docker build -q -f "$REPO/build/package/Dockerfile" -t "${1:-mast:soak}" "$REPO"
}

up() { # up — start or update the stack, then wait until every node is ready
  init
  docker compose up -d --remove-orphans
  wait_ready
}

down() { # down — stop the stack and delete its volumes
  docker compose down -v -t 5
}

ready() { # ready NODE — print the node's /readyz status code
  docker run --rm --network mastsoak curlimages/curl:8.11.1 -s -o /dev/null -w '%{http_code}' -m 3 "http://$1:9090/readyz" || true
}

wait_ready() { # wait_ready — until every mast node answers /readyz with 200
  local i n c
  for i in $(seq 1 120); do
    n=0; for c in core-0 core-1 core-2 edge-0 edge-1 standalone; do [ "$(ready $c)" = 200 ] && n=$((n + 1)); done
    if [ $n = 6 ]; then echo "all six nodes ready after ${i}s"; return 0; fi
    sleep 1
  done
  echo "not every node became ready:"; for c in core-0 core-1 core-2 edge-0 edge-1 standalone; do echo "  $c $(ready $c)"; done; return 1
}

token() { # token TENANT [DAYS] — an HS256 token naming TENANT, for the MQTT username
  python3 - "$SOAK/secrets/jwt.secret" "$1" "${2:-30}" <<'EOF'
import base64, hashlib, hmac, json, sys, time
secret = open(sys.argv[1], "rb").read().strip()
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
now = int(time.time())
head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
body = b64(json.dumps({"tenant": sys.argv[2], "iat": now, "exp": now + 86400 * int(sys.argv[3])}).encode())
sig = b64(hmac.new(secret, f"{head}.{body}".encode(), hashlib.sha256).digest())
print(f"{head}.{body}.{sig}")
EOF
}

admin() { # admin METHOD PATH [NODE] — call the admin API from inside the network, e.g. admin GET /v1/tenants/acme/sessions
  docker run --rm --network mastsoak -v "$SOAK/secrets:/s:ro" curlimages/curl:8.11.1 -s -m 10 -X "$1" \
    -H "Authorization: Bearer $(cat "$SOAK/secrets/admin.token")" "http://${3:-edge-0}:8080$2"
}

# Load runs inside the network, never through a published port: on colima a
# published port is a userspace proxy, and it is what you would measure.
tools() { # tools [REF] — mastbench at the bench repo's REF (main), in the image fire.sh uses
  # Resolved to a commit here, because the build caches by argument: a
  # literal "main" would be built once and never again.
  local rev; rev=$(git ls-remote https://github.com/mastmq/bench "${1:-main}" | cut -f1)
  docker build -q --build-arg BENCH_VERSION="${rev:-${1:-main}}" -t mastfire-tools "$REPO/deployments/fire"
}

mb() { # mb ARGS... — mastbench inside the network, e.g. mb throughput --broker tcp://edge-0:1884 ...
  docker run --rm --network mastsoak --cpus "${MB_CPUS:-2}" mastfire-tools /x/mastbench "$@"
}

load() { # load WORKERS SCENARIO ARGS... — one mastbench run split over WORKERS containers; worker 0 prints the merged result
  local n=$1 run i; shift
  run="soak-$(date +%s)"
  for i in $(seq 1 $((n - 1))); do
    docker run -d --rm --name "mastsoak-load-$i" --network mastsoak --cpus "${MB_CPUS:-2}" mastfire-tools \
      /x/mastbench "$@" --workers "$n" --worker-index "$i" --run-id "$run" --trust-clocks >/dev/null
  done
  # One host, one clock, so every worker's latency counts (--trust-clocks).
  mb "$@" --workers "$n" --worker-index 0 --run-id "$run" --trust-clocks
}

mosq() { # mosq PROGRAM ARGS... — mosquitto_pub or mosquitto_sub inside the network
  docker run --rm -i --network mastsoak eclipse-mosquitto:2.1.4-alpine "$@"
}

stats() { docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' | grep mastsoak | sort; }

alerts() { # alerts — every alert Prometheus has pending or firing
  docker run --rm --network mastsoak curlimages/curl:8.11.1 -s -m 5 http://prometheus:9090/api/v1/alerts |
    python3 -c 'import json,sys
for a in json.load(sys.stdin)["data"]["alerts"]: print(a["state"], a["labels"]["alertname"], a["labels"].get("pod", a["labels"].get("namespace", "")))'
}

rules() { # rules — re-render the alerts and copy the dashboard from the charts checkout ($CHARTS)
  local chart="$CHARTS/charts/mast"
  [ -d "$chart" ] || { echo "no chart at $chart; set CHARTS"; return 1; }
  render() { # render NAMESPACE VALUES OUT
    {
      echo "# Rendered by ./soak.sh rules from mastmq/charts $(git -C "$CHARTS" rev-parse --short HEAD): do not edit."
      echo "# helm template mast charts/mast -n $1 -f charts/mast/ci/$2"
      helm template mast "$chart" -n "$1" -f "$chart/ci/$2" -s templates/prometheusrule.yaml | yq '{"groups": .spec.groups}'
    } >"$3"
  }
  render soak monitoring-values.yaml "$SOAK/prometheus/rules/mast-cluster.yaml"
  render soak-standalone standalone-monitoring-values.yaml "$SOAK/prometheus/rules/mast-standalone.yaml"
  # The two renders name their groups alike. Prometheus accepts that across
  # files, but its rules page then shows two groups of one name.
  yq -i '.groups[].name |= . + ".standalone"' "$SOAK/prometheus/rules/mast-standalone.yaml"
  cp "$chart/dashboards/mast.json" "$SOAK/grafana/dashboards/mast.json"
  docker run --rm -v "$SOAK/prometheus:/etc/prometheus:ro" --entrypoint promtool prom/prometheus:v3.13.2 check config /etc/prometheus/prometheus.yml
}

help() { grep -E '^[a-z_]+\(\) \{ #' "${BASH_SOURCE[0]:-$0}" | sed -E 's/^[a-z_]+\(\) \{ # /  /'; }

if [ "${BASH_SOURCE[0]:-$0}" = "$0" ]; then
  cmd=${1:-help}; shift || true
  "$cmd" "$@"
fi
