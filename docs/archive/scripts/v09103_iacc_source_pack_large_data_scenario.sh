#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09103_PORT:-18723}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09103-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09103-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.103 IACC source pack scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"source_onboarding_pack"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"source_pack_delta_plan"'

curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

source_pack_json="$(curl -fsS "$BASE_URL/api/iacc/source-packs/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09103","session_id":"session-v09103","source_pack":{"source_pack_id":"source-pack-v09103-erp","source_name":"erp_weekly_material_export","owner":"ops-data-steward","access_mode":"batch_file","refresh_mode":"weekly_snapshot","entity_mappings":[{"source_entity":"material","iacc_entity_type":"component","source_key_field":"component_code"}],"fact_mappings":[{"source_table":"weekly_material_shortage","fact_type":"supply.material_shortage","metric_key":"material_shortage_risk","entity_ref_fields":["component_ref","product_ref"],"measure_fields":["short_qty"],"dedup_key":"component_ref+week","delta_signature":"component_ref+week+short_qty"}],"reconciliation_rules":["erp_weekly_snapshot_wins_for_material_shortage"],"quality_rules":["component_ref_required","week_required","short_qty_non_negative"],"freshness_sla":"P7D","security_policy":"internal_operational"}}')"
printf '%s' "$source_pack_json" | rg -q '"source_pack_id":"source-pack-v09103-erp"'

curl -fsS "$BASE_URL/api/iacc/source-packs/source-pack-v09103-erp" | rg -q '"source_name":"erp_weekly_material_export"'
curl -fsS "$BASE_URL/api/iacc/source-packs/source-pack-v09103-erp/validate" -X POST | rg -q '"status":"ready"'

curl -fsS "$BASE_URL/api/iacc/source-packs/source-pack-v09103-erp/ingest-file" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09103-ingest","session_id":"session-v09103","facts":[{"fact_id":"fact-v09103-shortage-001","snapshot_id":"snapshot-v09103-week33","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v09103","product:server-v09103"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W33"},"measures":{"short_qty":410},"source_ref":"source-pack:source-pack-v09103-erp","confidence":0.91},{"fact_id":"fact-v09103-shortage-002","snapshot_id":"snapshot-v09103-week34","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v09103","product:server-v09103"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W34"},"measures":{"short_qty":220},"source_ref":"source-pack:source-pack-v09103-erp","confidence":0.9}]}' \
  | rg -q '"ingested":2'

delta_json="$(curl -fsS "$BASE_URL/api/iacc/source-packs/source-pack-v09103-erp/delta-plan" -X POST)"
printf '%s' "$delta_json" | rg -q '"kind":"iacc.source_pack.delta_plan"'
printf '%s' "$delta_json" | rg -q '"material_shortage_risk"'

plan_json="$(curl -fsS "$BASE_URL/api/iacc/compute/jobs/plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09103-compute","session_id":"session-v09103","job":{"job_id":"compute-job-v09103-source-pack","trigger_fact_type":"supply.material_shortage","trigger_fact_refs":["source-pack:source-pack-v09103-erp"],"entity_scope":"component:gpu-v09103","period":"2026-W33..2026-W34"}}')"
job_id="$(printf '%s' "$plan_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["plan"]["job"]["job_id"])')"
curl -fsS "$BASE_URL/api/iacc/compute/jobs/$job_id/run" -X POST | rg -q '"status":"completed"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"source_pack_count":1'

test -f "$WORKDIR/.cowd/iacc.sqlite"
