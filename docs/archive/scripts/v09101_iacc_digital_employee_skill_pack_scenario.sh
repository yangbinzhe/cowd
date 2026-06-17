#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09101_PORT:-18721}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09101-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09101-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.101 IACC skill pack scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"server_manufacturing_skill_pack"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"incident_skill_agent_graph"'
curl -fsS "$BASE_URL/api/iacc/skills" | rg -q '"skill_id":"supply-risk-analyst"'
curl -fsS "$BASE_URL/api/iacc/skills/supply-risk-analyst" | rg -q '"role":"Supply Risk Analyst"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09101","session_id":"session-v09101","facts":[{"fact_id":"fact-v09101-shortage-a","snapshot_id":"snapshot-v09101-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v09101","product:server-v09101"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W31"},"measures":{"short_qty":220},"source_ref":"connector:erp:material-shortage","confidence":0.93}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'
attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_id="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v09101\",\"session_id\":\"session-v09101\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.101 skill pack GPU shortage incident\"}" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"
incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v09101\",\"session_id\":\"session-v09101\",\"title\":\"GPU shortage skill pack incident\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"

plan_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/skills/plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09101-plan","session_id":"session-v09101","limit":3}')"
printf '%s' "$plan_json" | rg -q '"kind":"iacc.skill.plan"'
printf '%s' "$plan_json" | rg -q '"supply-risk-analyst"'
printf '%s' "$plan_json" | rg -q '"iacc_skill_supply_risk_analyst"'

run_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/skills/supply-risk-analyst/run" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09101-run","session_id":"session-v09101"}')"
printf '%s' "$run_json" | rg -q '"kind":"iacc.skill.run"'
printf '%s' "$run_json" | rg -q '"status":"completed"'
printf '%s' "$run_json" | rg -q '"supplier_recovery"'
printf '%s' "$run_json" | rg -q '"result":"Supply Risk Analyst prepared governed analysis'

test -f "$WORKDIR/.cowd/iacc.sqlite"
