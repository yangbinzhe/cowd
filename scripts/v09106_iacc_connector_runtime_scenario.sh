#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09106_PORT:-18726}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09106-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09106-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.106 IACC connector runtime scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"connector_runtime"'

curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

curl -fsS "$BASE_URL/api/iacc/source-packs/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09106-source","session_id":"session-v09106","source_pack":{"source_pack_id":"source-pack-v09106-dbview","source_name":"mes_work_order_view","owner":"manufacturing-data-owner","access_mode":"db_view","refresh_mode":"hourly_delta","entity_mappings":[{"source_entity":"work_order","iacc_entity_type":"work_order","source_key_field":"wo_no"}],"fact_mappings":[{"source_table":"mes_work_order_load","fact_type":"manufacturing.capacity_load","metric_key":"work_center_load","entity_ref_fields":["work_center_ref","product_ref"],"measure_fields":["load_hours"],"dedup_key":"wo_no+operation+week","delta_signature":"wo_no+operation+week+load_hours"}],"reconciliation_rules":["mes_work_order_view_wins_for_wip"],"quality_rules":["wo_no_required","operation_required","load_hours_non_negative"],"freshness_sla":"PT1H","security_policy":"internal_operational"}}' \
  | rg -q '"source_pack_id":"source-pack-v09106-dbview"'

plan_json="$(curl -fsS "$BASE_URL/api/iacc/source-packs/source-pack-v09106-dbview/connector-runs/plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09106-plan","session_id":"session-v09106","run":{"run_id":"connector-run-v09106-plan","resource_ref":"dbview://mes/work_order_load","partition_ref":"2026-W33","expected_rows":250000,"credential_ref":"secret://mes-readonly","checksum":"sha256:v09106"}}')"
printf '%s' "$plan_json" | rg -q '"kind":"iacc.connector_run.plan"'
printf '%s' "$plan_json" | rg -q '"connector_kind":"database_view_connector"'
printf '%s' "$plan_json" | rg -q '"status":"planned"'
printf '%s' "$plan_json" | rg -q '"work_center_load"'

run_json="$(curl -fsS "$BASE_URL/api/iacc/source-packs/source-pack-v09106-dbview/connector-runs/run" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09106-run","session_id":"session-v09106","run":{"run_id":"connector-run-v09106-run","resource_ref":"dbview://mes/work_order_load","partition_ref":"2026-W33","expected_rows":250000,"credential_ref":"secret://mes-readonly","checksum":"sha256:v09106"}}')"
printf '%s' "$run_json" | rg -q '"kind":"iacc.connector_run"'
printf '%s' "$run_json" | rg -q '"status":"completed"'
printf '%s' "$run_json" | rg -q '"retryable":false'
printf '%s' "$run_json" | rg -q '"score":1.0'

curl -fsS "$BASE_URL/api/iacc/connector-runs/connector-run-v09106-run" | rg -q '"connector-run-v09106-run"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"connector_run_count":2'

test -f "$WORKDIR/.cowd/iacc.sqlite"

