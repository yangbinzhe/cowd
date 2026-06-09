#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0984_PORT:-18704}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0984-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0984-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.84 IACC entity relation scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":11'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"entity_relation_network"'

component_json="$(curl -fsS "$BASE_URL/api/iacc/entities/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0984-component-erp","session_id":"session-v0984","entity":{"entity_type":"component","canonical_key":"GPU-H100","display_name":"GPU H100","source_keys":[{"source_system":"ERP","source_key":"MAT-GPU-H100","source_ref":"connector:erp:material"}],"attributes":{"family":"gpu"},"confidence":0.96}}')"
component_id="$(printf '%s' "$component_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["entity"]["entity_id"])')"

merged_json="$(curl -fsS "$BASE_URL/api/iacc/entities/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0984-component-plm","session_id":"session-v0984","entity":{"entity_type":"Component","canonical_key":"gpu-h100","display_name":"H100 accelerator","source_keys":[{"source_system":"PLM","source_key":"GPU_H100_80GB","source_ref":"connector:plm:item"}],"attributes":{"thermal_design":"high"},"confidence":0.91}}')"
merged_id="$(printf '%s' "$merged_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["entity"]["entity_id"])')"
test "$component_id" = "$merged_id"
printf '%s' "$merged_json" | rg -q '"source_keys":'

resolved_json="$(curl -fsS "$BASE_URL/api/iacc/entities/resolve-source-key" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0984-resolve","session_id":"session-v0984","source_system":"plm","source_key":"GPU_H100_80GB"}')"
resolved_id="$(printf '%s' "$resolved_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["entity"]["entity_id"])')"
test "$resolved_id" = "$component_id"

product_id="$(curl -fsS "$BASE_URL/api/iacc/entities/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0984-product","session_id":"session-v0984","entity":{"entity_type":"product","canonical_key":"SERVER-AI-8GPU","display_name":"AI Server 8GPU","attributes":{"product_family":"server"},"confidence":0.95}}' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["entity"]["entity_id"])')"
order_id="$(curl -fsS "$BASE_URL/api/iacc/entities/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0984-order","session_id":"session-v0984","entity":{"entity_type":"customer_order","canonical_key":"CO-2026-0001","display_name":"Customer order CO-2026-0001","attributes":{"priority":"strategic"},"confidence":0.92}}' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["entity"]["entity_id"])')"

curl -fsS "$BASE_URL/api/iacc/relations/upsert" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0984-requires\",\"session_id\":\"session-v0984\",\"relation\":{\"relation_type\":\"requires\",\"from_entity_id\":\"$product_id\",\"to_entity_id\":\"$component_id\",\"attributes\":{\"qty_per\":8},\"confidence\":0.97}}" \
  | rg -q '"relation_type":"requires"'

curl -fsS "$BASE_URL/api/iacc/relations/upsert" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0984-order\",\"session_id\":\"session-v0984\",\"relation\":{\"relation_type\":\"reserved_for\",\"from_entity_id\":\"$order_id\",\"to_entity_id\":\"$product_id\",\"attributes\":{\"week\":\"2026-W30\"},\"confidence\":0.9}}" \
  | rg -q '"relation_type":"reserved_for"'

curl -fsS "$BASE_URL/api/iacc/entities/$component_id/relations" | rg -q '"relation_type":"requires"'
impact_json="$(curl -fsS "$BASE_URL/api/iacc/entities/$component_id/impact-path")"
printf '%s' "$impact_json" | rg -q "$order_id"
printf '%s' "$impact_json" | rg -q '"hops":'

curl -fsS "$BASE_URL/api/iacc/entities" | rg -q "$component_id"
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"entity_count":3'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"relation_count":2'

test -f "$WORKDIR/.cowd/iacc.sqlite"
