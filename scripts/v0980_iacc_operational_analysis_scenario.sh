#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0980_PORT:-18700}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0980-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0980-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.80 IACC operational analysis scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"incident_operational_analysis"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0980","session_id":"session-v0980","facts":[{"fact_id":"fact-v0980-shortage-a","snapshot_id":"snapshot-v0980-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0980","product:server-v0980"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W28"},"measures":{"short_qty":240},"source_ref":"connector:erp:material-shortage","confidence":0.91}]}' \
  | rg -q '"ingested":1'

recompute_json="$(curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST)"
printf '%s' "$recompute_json" | rg -q '"change_count":1'

attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_id="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0980\",\"session_id\":\"session-v0980\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.80 GPU shortage operational incident\"}" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0980\",\"session_id\":\"session-v0980\",\"title\":\"GPU shortage operational incident\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"

analysis_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST)"
analysis_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["analysis_id"])')"
printf '%s' "$analysis_json" | rg -q '"cause_type":"supply_constraint"'
printf '%s' "$analysis_json" | rg -q '"impact_type":"material_availability_risk"'
printf '%s' "$analysis_json" | rg -q '"action_type":"supplier_recovery"'
printf '%s' "$analysis_json" | rg -q '"status":"ready_for_review"'

curl -fsS "$BASE_URL/api/iacc/analyses/$analysis_id" | rg -q "$analysis_id"
curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id" | rg -q '"status":"analyzed"'
curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id" | rg -q '"attribution_candidates"'
curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id" | rg -q '"impact_paths"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"analysis_count":1'

test -f "$WORKDIR/.cowd/iacc.sqlite"
