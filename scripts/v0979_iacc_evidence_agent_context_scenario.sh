#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0979_PORT:-18699}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0979-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0979-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.79 IACC evidence/agent/context scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":6'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"incident_agent_graph"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0979","session_id":"session-v0979","facts":[{"fact_id":"fact-v0979-plan-a","snapshot_id":"snapshot-v0979-plan-a","fact_type":"plan.weekly_demand","entity_refs":["product:server-a","site:plant-a"],"metric_key":"plan_bom_delta","dimensions":{"week":"2026-W24","bom":"server-a-rack"},"measures":{"demand_qty":100},"source_ref":"file:weekly-plan-a","confidence":0.8},{"fact_id":"fact-v0979-plan-b","snapshot_id":"snapshot-v0979-plan-b","fact_type":"plan.weekly_demand","entity_refs":["product:server-a","site:plant-a"],"metric_key":"plan_bom_delta","dimensions":{"week":"2026-W24","bom":"server-a-rack"},"measures":{"demand_qty":175},"source_ref":"file:weekly-plan-b","confidence":0.9}]}' \
  | rg -q '"ingested":2'

recompute_json="$(curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST)"
printf '%s' "$recompute_json" | rg -q '"metric_state_count":1'
printf '%s' "$recompute_json" | rg -q '"change_count":1'

attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_json="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0979\",\"session_id\":\"session-v0979\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.79 IACC weekly BOM demand incident\"}")"
packet_id="$(printf '%s' "$packet_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"
printf '%s' "$packet_json" | rg -q '"metric_evidence"'
printf '%s' "$packet_json" | rg -q '"change_evidence"'
printf '%s' "$packet_json" | rg -q '"confidence"'

curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id/context" | rg -q '"kind":"iacc.evidence.context_item"'
curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id/context" | rg -q "iacc:evidence:$packet_id"

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0979\",\"session_id\":\"session-v0979\",\"title\":\"weekly BOM demand change incident\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"
task_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["task"]["id"])')"
printf '%s' "$incident_json" | rg -q '"id":"iacc_researcher"'
printf '%s' "$incident_json" | rg -q '"id":"iacc_reviewer"'
printf '%s' "$incident_json" | rg -q '"id":"iacc_merger"'
printf '%s' "$incident_json" | rg -q "iacc:evidence:$packet_id"

curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id" | rg -q "$task_id"
curl -fsS "$BASE_URL/api/tasks/$task_id/agent-graph" | rg -q '"iacc_researcher"'

test -f "$WORKDIR/.cowd/iacc.sqlite"
