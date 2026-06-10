#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0985_PORT:-18705}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0985-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0985-iacc.XXXXXX")"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  for _ in {1..10}; do
    if rm -rf "$TMP_DIR" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  rm -rf "$TMP_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.85 IACC server manufacturing scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":15'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"server_manufacturing_domain_pack"'

domain_json="$(curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing")"
printf '%s' "$domain_json" | rg -q '"domain_id":"server_manufacturing"'
printf '%s' "$domain_json" | rg -q '"entity_types":'
printf '%s' "$domain_json" | rg -q '"scenarios":'

seed_json="$(curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST)"
printf '%s' "$seed_json" | rg -q '"domain_id":"server_manufacturing"'
printf '%s' "$seed_json" | rg -q '"entity_count":14'
printf '%s' "$seed_json" | rg -q '"relation_count":13'
printf '%s' "$seed_json" | rg -q '"metric_definition_count":6'
printf '%s' "$seed_json" | rg -q '"fact_count":5'
printf '%s' "$seed_json" | rg -q '"scenario_count":3'

resolved_json="$(curl -fsS "$BASE_URL/api/iacc/entities/resolve-source-key" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0985-resolve","session_id":"session-v0985","source_system":"plm","source_key":"GPU_H100_80GB"}')"
printf '%s' "$resolved_json" | rg -q '"entity_id":"entity-component-gpu-h100"'

impact_json="$(curl -fsS "$BASE_URL/api/iacc/entities/entity-component-gpu-h100/impact-path")"
printf '%s' "$impact_json" | rg -q 'entity-order-co-2026-0001'
printf '%s' "$impact_json" | rg -q '"relation_type":"reserved_for"'

metrics_json="$(curl -fsS "$BASE_URL/api/iacc/metrics")"
printf '%s' "$metrics_json" | rg -q '"metric_id":"material_shortage_risk"'
printf '%s' "$metrics_json" | rg -q '"metric_id":"work_center_load"'

recompute_json="$(curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST)"
printf '%s' "$recompute_json" | rg -q '"metric_id":"material_shortage_risk"'
printf '%s' "$recompute_json" | rg -q '"metric_id":"order_delivery_risk"'
printf '%s' "$recompute_json" | rg -q '"attention_count":'

curl -fsS "$BASE_URL/api/iacc/attention/hot" | rg -q '"metric_delta_detected"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"entity_count":14'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"relation_count":13'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"fact_count":5'

test -f "$WORKDIR/.cowd/iacc.sqlite"
