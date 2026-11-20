#!/usr/bin/env bash
# Run the Eclipse Paho interoperability suite, MQTT 3.1.1 and 5, against one
# all-in-one mast. Used by `just conformance` and by conformance.yaml, so a
# failure in CI reproduces locally with the same command.
#
# usage: test/conformance/run.sh [path to a mast binary]
#
# Without a binary it builds one. The suite is fetched at PAHO_REF into
# .conformance/, next to the broker's store and logs.
set -euo pipefail

# The suite has no releases. Pin a commit so a change upstream cannot turn
# the run red without a change here; move it on purpose and read the diff.
PAHO_REF=fdab9d2b1ce125385135e5d9f282a0ad315268cc

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
work=${CONFORMANCE_DIR:-$root/.conformance}
# Not 1883: a developer machine often has a broker there already, and the
# suite would pass or fail against that one instead.
port=${CONFORMANCE_PORT:-18830}
authz_port=$((port + 1))
obs_port=$((port + 2))
monitor_port=$((port + 3))
python=${PYTHON:-python3}

mkdir -p "$work"
suite="$work/paho.mqtt.testing"
if [ "$(git -C "$suite" rev-parse HEAD 2>/dev/null)" != "$PAHO_REF" ]; then
  rm -rf "$suite"
  git init -q "$suite"
  git -C "$suite" fetch -q --depth 1 https://github.com/eclipse/paho.mqtt.testing.git "$PAHO_REF"
  git -C "$suite" checkout -q FETCH_HEAD
fi

mast=${1:-}
if [ -z "$mast" ]; then
  mast="$work/mast"
  (cd "$root" && go build -o "$mast" ./cmd/mast)
fi

rm -rf "$work/data"
pids=()
# shellcheck disable=SC2329 # invoked by the trap below
stop() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap stop EXIT

"$python" -I "$here/authz.py" "$authz_port" &
pids+=($!)

# http auth rather than static, because the subscribe-failure tests need one
# filter refused and only a policy service can refuse it. See authz.py.
MAST__MQTT__ADDR="127.0.0.1:$port" \
MAST__OBS__ADDR="127.0.0.1:$obs_port" \
MAST__NATS__MONITOR_ADDR="127.0.0.1:$monitor_port" \
MAST__CORE__STORE_DIR="$work/data" \
MAST__LOG__LEVEL=warn \
MAST__AUTH__MODE=http \
MAST__AUTH__HTTP__AUTHN_URL="http://127.0.0.1:$authz_port/authn" \
MAST__AUTH__HTTP__AUTHZ_URL="http://127.0.0.1:$authz_port/authz" \
  "$mast" --role all-in-one >"$work/mast.log" 2>&1 &
pids+=($!)

ready=""
for _ in $(seq 1 60); do
  if curl -fs "http://127.0.0.1:$obs_port/healthz" >/dev/null &&
    curl -fs -X POST -H 'Content-Type: application/json' -d '{}' \
      "http://127.0.0.1:$authz_port/authn" >/dev/null &&
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    ready=1
    break
  fi
  sleep 0.5
done
if [ -z "$ready" ]; then
  echo "mast or the policy service did not come up" >&2
  cat "$work/mast.log" >&2
  exit 1
fi

status=0
for version in v3 v5; do
  echo "::group::Paho interoperability, $version"
  "$python" -I "$here/run.py" "$suite" "$version" 127.0.0.1 "$port" || status=1
  echo "::endgroup::"
done

if [ "$status" -ne 0 ]; then
  echo "--- mast log ---" >&2
  tail -n 200 "$work/mast.log" >&2
fi
exit "$status"
