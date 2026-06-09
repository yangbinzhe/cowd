#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0978_PORT:-18698}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0978-iacc-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0978-iacc.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.78 IACC metric scenario" >&2
  exit 1
fi

if [[ ! -x "$BIN" ]]; then
  echo "cowd binary not found at $BIN; build it first or set COWD_BIN" >&2
  exit 1
fi

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
ln -s "$ROOT/webui" "$WORKDIR/webui"

cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: false
gateway:
  enabled: true
  sessionReset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: $PORT
      auth:
        enabled: false
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..100}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -fsS "$BASE_URL/healthz" | rg -q '"gateway":"daemon-http-gateway"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":7'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0978","session_id":"session-v0978","facts":[{"fact_id":"fact-v0978-plan-a","snapshot_id":"snapshot-v0978-plan-a","fact_type":"plan.weekly_demand","entity_refs":["product:server-a"],"metric_key":"plan_bom_delta","dimensions":{"week":"2026-W24"},"measures":{"demand_qty":100},"source_ref":"file:weekly-plan-a","confidence":0.8},{"fact_id":"fact-v0978-plan-b","snapshot_id":"snapshot-v0978-plan-b","fact_type":"plan.weekly_demand","entity_refs":["product:server-a"],"metric_key":"plan_bom_delta","dimensions":{"week":"2026-W24"},"measures":{"demand_qty":140},"source_ref":"file:weekly-plan-b","confidence":0.9}]}' \
  | rg -q '"ingested":2'

recompute_json="$(curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST)"
printf '%s' "$recompute_json" | rg -q '"kind":"iacc.metrics.recompute"'
printf '%s' "$recompute_json" | rg -q '"metric_state_count":1'
printf '%s' "$recompute_json" | rg -q '"change_count":1'
printf '%s' "$recompute_json" | rg -q '"value":240'

curl -fsS "$BASE_URL/api/iacc/metrics" | rg -q '"metric_id":"plan_bom_delta"'
curl -fsS "$BASE_URL/api/iacc/metrics/plan_bom_delta" | rg -q '"value":240'
curl -fsS "$BASE_URL/api/iacc/changes" | rg -q '"change_type":"metric_delta"'
curl -fsS "$BASE_URL/api/iacc/attention/hot" | rg -q '"metric_delta_detected"'

test -f "$WORKDIR/.cowd/iacc.sqlite"
