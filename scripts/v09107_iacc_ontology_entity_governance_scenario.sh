#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09107_PORT:-18727}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09107-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09107-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.107 IACC ontology scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"server_manufacturing_ontology"'
curl -fsS "$BASE_URL/api/iacc/ontology/server-manufacturing" | rg -q '"ontology_id":"server_manufacturing_ontology"'
curl -fsS "$BASE_URL/api/iacc/ontology/server-manufacturing" | rg -q '"metric_id":"material_shortage_risk"'
curl -fsS "$BASE_URL/api/iacc/ontology/server-manufacturing/seed" -X POST | rg -q '"concept_id":"component"'

curl -fsS "$BASE_URL/api/iacc/entities/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09107-left","session_id":"session-v09107","entity":{"entity_id":"entity-v09107-component-plm","entity_type":"component","canonical_key":"gpu_h100_80gb","display_name":"GPU H100 80GB","source_keys":[{"source_system":"plm","source_key":"GPU_H100_80GB"}],"attributes":{"commodity":"gpu"},"confidence":0.91}}' \
  | rg -q '"entity-v09107-component-plm"'

curl -fsS "$BASE_URL/api/iacc/entities/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09107-right","session_id":"session-v09107","entity":{"entity_id":"entity-v09107-component-erp","entity_type":"component","canonical_key":"gpu_h100_80gb_erp","display_name":"GPU H100 80GB","source_keys":[{"source_system":"erp","source_key":"GPU-H100-80GB"}],"attributes":{"commodity":"gpu"},"confidence":0.86}}' \
  | rg -q '"entity-v09107-component-erp"'

candidate_json="$(curl -fsS "$BASE_URL/api/iacc/entities/match-candidate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09107-match","session_id":"session-v09107","left_entity_id":"entity-v09107-component-plm","right_entity_id":"entity-v09107-component-erp"}')"
printf '%s' "$candidate_json" | rg -q '"kind":"iacc.entity.match_candidate"'
printf '%s' "$candidate_json" | rg -q '"same_display_name"'
candidate_id="$(printf '%s' "$candidate_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["candidate"]["candidate_id"])')"

curl -fsS "$BASE_URL/api/iacc/entities/conflict-decision" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09107-decision","session_id":"session-v09107","candidate_id":"'"$candidate_id"'","survivor_entity_id":"entity-v09107-component-plm","retired_entity_id":"entity-v09107-component-erp","survivorship_rule":"plm_wins_for_component_master","notes":"PLM is authoritative for engineering part master."}' \
  | rg -q '"survivorship_rule":"plm_wins_for_component_master"'

curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"ontology_pack_count":1'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"entity_match_candidate_count":1'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"entity_conflict_decision_count":1'

test -f "$WORKDIR/.cowd/iacc.sqlite"

