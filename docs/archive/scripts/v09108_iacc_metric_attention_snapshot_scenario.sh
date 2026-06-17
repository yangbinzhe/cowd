#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09108_PORT:-18728}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09108-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09108-iacc.XXXXXX")"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.108 IACC metric attention scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"metric_attention_plan"'

curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

plan_json="$(curl -fsS "$BASE_URL/api/iacc/metrics/attention-plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09108-plan","session_id":"session-v09108","trigger_fact_type":"supply.material_shortage","entity_scope":"component:gpu-v09108","period":"2026-W33","limit":6}')"
printf '%s' "$plan_json" | rg -q '"kind":"iacc.metric_attention.plan"'
printf '%s' "$plan_json" | rg -q '"material_shortage_risk"'
printf '%s' "$plan_json" | rg -q '"selected_metric_ids"'

snapshot_json="$(curl -fsS "$BASE_URL/api/iacc/metrics/snapshots/materialize" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09108-snapshot","session_id":"session-v09108","metric_ids":["material_shortage_risk","order_delivery_risk","work_center_load"],"scope_ref":"component:gpu-v09108"}')"
printf '%s' "$snapshot_json" | rg -q '"kind":"iacc.metric_snapshot"'
printf '%s' "$snapshot_json" | rg -q '"metric_ids"'
printf '%s' "$snapshot_json" | rg -q '"scope_ref":"component:gpu-v09108"'

curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"metric_snapshot_count":1'

test -f "$WORKDIR/.cowd/iacc.sqlite"

