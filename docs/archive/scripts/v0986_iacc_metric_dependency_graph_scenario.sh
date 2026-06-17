#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0986_PORT:-18706}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0986-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0986-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.86 IACC metric dependency scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":17'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"metric_dependency_graph"'

curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"metric_dependency_count":5'

lineage_json="$(curl -fsS "$BASE_URL/api/iacc/metrics/supplier_commit_variance/lineage")"
printf '%s' "$lineage_json" | rg -q '"metric_id":"supplier_commit_variance"'
printf '%s' "$lineage_json" | rg -q '"downstream_metric_id":"material_shortage_risk"'
printf '%s' "$lineage_json" | rg -q 'order_delivery_risk'

affected_json="$(curl -fsS "$BASE_URL/api/iacc/metric-dependencies/affected-by-fact-type" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0986-fact-impact","session_id":"session-v0986","fact_type":"supply.commit_variance"}')"
printf '%s' "$affected_json" | rg -q '"supplier_commit_variance"'
printf '%s' "$affected_json" | rg -q '"material_shortage_risk"'
printf '%s' "$affected_json" | rg -q '"order_delivery_risk"'

curl -fsS "$BASE_URL/api/iacc/metric-dependencies/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0986-manual","session_id":"session-v0986","dependency":{"upstream_metric_id":"material_shortage_risk","downstream_metric_id":"revenue_at_risk","dependency_type":"material_availability_to_revenue","entity_relation_type":"reserved_for","required_fact_types":["supply.material_shortage","finance.revenue_at_risk"],"confidence":0.78,"notes":"manual dependency added by v0.9.86 scenario"}}' \
  | rg -q '"downstream_metric_id":"revenue_at_risk"'

curl -fsS "$BASE_URL/api/iacc/metrics/material_shortage_risk/lineage" | rg -q '"revenue_at_risk"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"metric_dependency_count":6'

test -f "$WORKDIR/.cowd/iacc.sqlite"
