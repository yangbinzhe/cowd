#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09100_PORT:-18720}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09100-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09100-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.100 IACC memory case playbook scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":14'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"memory_case_promotion"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"playbook_recommendation"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09100","session_id":"session-v09100","facts":[{"fact_id":"fact-v09100-shortage-a","snapshot_id":"snapshot-v09100-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v09100","product:server-v09100"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W30"},"measures":{"short_qty":310},"source_ref":"connector:erp:material-shortage","confidence":0.94}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_id="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v09100\",\"session_id\":\"session-v09100\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.100 GPU shortage reusable case\"}" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v09100\",\"session_id\":\"session-v09100\",\"title\":\"GPU shortage reusable case\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"

analysis_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST)"
analysis_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["analysis_id"])')"
action_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["recommended_actions"][0]["action_id"])')"

execution_json="$(curl -fsS "$BASE_URL/api/iacc/analyses/$analysis_id/actions/$action_id/execute" \
  -H 'content-type: application/json' \
  -d '{"mode":"commit","operator_id":"user:ops-planner","note":"queue supplier recovery action for reusable case"}')"
execution_id="$(printf '%s' "$execution_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["execution"]["execution_id"])')"

curl -fsS "$BASE_URL/api/iacc/executions/$execution_id/feedback" \
  -H 'content-type: application/json' \
  -d '{"outcome":"resolved","note":"supplier commit secured and reusable pattern validated","metric_delta":-310}' \
  | rg -q '"status":"feedback_resolved"'

promotion_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/cases/promote" -X POST)"
printf '%s' "$promotion_json" | rg -q '"kind":"iacc.memory_case.promotion"'
printf '%s' "$promotion_json" | rg -q '"outcome":"resolved"'
case_id="$(printf '%s' "$promotion_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["memory_case"]["case_id"])')"
playbook_id="$(printf '%s' "$promotion_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["playbook"]["playbook_id"])')"

curl -fsS "$BASE_URL/api/iacc/cases/$case_id" | rg -q "$case_id"
curl -fsS "$BASE_URL/api/iacc/cases/search?q=shortage" | rg -q "$case_id"
curl -fsS "$BASE_URL/api/iacc/playbooks/$playbook_id" | rg -q "$playbook_id"

second_incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v09100-b\",\"session_id\":\"session-v09100\",\"title\":\"GPU shortage recurring case\",\"evidence_packet_id\":\"$packet_id\"}")"
second_incident_id="$(printf '%s' "$second_incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"
curl -fsS "$BASE_URL/api/iacc/incidents/$second_incident_id/analyze" -X POST | rg -q '"kind":"iacc.operational_analysis"'
curl -fsS "$BASE_URL/api/iacc/incidents/$second_incident_id/playbooks/recommend" \
  -H 'content-type: application/json' \
  -d '{"limit":3}' \
  | rg -q "$playbook_id"

curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"memory_case_count":1'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"playbook_count":1'

test -f "$WORKDIR/.cowd/iacc.sqlite"
