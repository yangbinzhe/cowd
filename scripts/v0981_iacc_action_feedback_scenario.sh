#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0981_PORT:-18701}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0981-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0981-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.81 IACC action feedback scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"schema_version":5'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"action_execution_feedback"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0981","session_id":"session-v0981","facts":[{"fact_id":"fact-v0981-shortage-a","snapshot_id":"snapshot-v0981-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0981","product:server-v0981"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W29"},"measures":{"short_qty":260},"source_ref":"connector:erp:material-shortage","confidence":0.93}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_id="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0981\",\"session_id\":\"session-v0981\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.81 GPU shortage closed-loop incident\"}" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0981\",\"session_id\":\"session-v0981\",\"title\":\"GPU shortage closed-loop incident\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"

analysis_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST)"
analysis_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["analysis_id"])')"
action_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["recommended_actions"][0]["action_id"])')"

execution_json="$(curl -fsS "$BASE_URL/api/iacc/analyses/$analysis_id/actions/$action_id/execute" \
  -H 'content-type: application/json' \
  -d '{"mode":"commit","operator_id":"user:ops-planner","note":"queue supplier recovery action"}')"
execution_id="$(printf '%s' "$execution_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["execution"]["execution_id"])')"
printf '%s' "$execution_json" | rg -q '"status":"queued_for_human_review"'
printf '%s' "$execution_json" | rg -q '"receipt_kind":"iacc.action.execution"'

feedback_json="$(curl -fsS "$BASE_URL/api/iacc/executions/$execution_id/feedback" \
  -H 'content-type: application/json' \
  -d '{"outcome":"resolved","note":"supplier commit secured and shortage cleared","metric_delta":-260}')"
printf '%s' "$feedback_json" | rg -q '"status":"feedback_resolved"'
printf '%s' "$feedback_json" | rg -q '"outcome":"resolved"'

curl -fsS "$BASE_URL/api/iacc/executions/$execution_id" | rg -q "$execution_id"
curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id" | rg -q '"status":"closed"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"execution_count":1'

test -f "$WORKDIR/.cowd/iacc.sqlite"
