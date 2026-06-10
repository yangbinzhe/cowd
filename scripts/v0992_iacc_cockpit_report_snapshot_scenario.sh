#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0992_PORT:-18712}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0992-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0992-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.92 IACC cockpit report snapshot scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":12'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_report_snapshot"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"scheduled_report_foundation"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0992","session_id":"session-v0992","facts":[{"fact_id":"fact-v0992-shortage-a","snapshot_id":"snapshot-v0992-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0992","product:server-v0992"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W33"},"measures":{"short_qty":340},"source_ref":"connector:erp:material-shortage","confidence":0.94}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_id="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0992\",\"session_id\":\"session-v0992\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.92 GPU shortage report incident\"}" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"
curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id/quality-gate" -X POST | rg -q '"decision":"review"'

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0992\",\"session_id\":\"session-v0992\",\"title\":\"GPU shortage report incident\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"
analysis_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST)"
analysis_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["analysis_id"])')"
action_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["recommended_actions"][0]["action_id"])')"
curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id/quality-gate" -X POST | rg -q '"decision":"pass"'

curl -fsS "$BASE_URL/api/iacc/analyses/$analysis_id/actions/$action_id/execute" \
  -H 'content-type: application/json' \
  -d '{"mode":"commit","operator_id":"user:ops-planner","note":"queue report-visible recovery action"}' \
  | rg -q '"status":"queued_for_human_review"'

curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0992","session_id":"session-v0992","profile":{"profile_id":"cockpit-profile-v0992-ops","owner_ref":"user:ops-planner","display_name":"Ops planner report cockpit","focus_refs":["component:gpu-v0992"],"focus_metric_ids":["material_shortage_risk"],"thresholds":{"material_shortage_risk":{"critical":100,"warning":40}},"template_id":"ops.default","cadence":"daily"}}' \
  | rg -q '"profile_id":"cockpit-profile-v0992-ops"'

report_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/cockpit-profile-v0992-ops/reports/generate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0992-report","session_id":"session-v0992","report":{"report_id":"cockpit-report-v0992-daily","cadence":"daily","delivery_ref":"channel://feishu/user/ops-planner","note":"daily report snapshot"}}')"
printf '%s' "$report_json" | rg -q '"kind":"iacc.cockpit.report"'
printf '%s' "$report_json" | rg -q '"report_id":"cockpit-report-v0992-daily"'
printf '%s' "$report_json" | rg -q '"status":"generated"'
printf '%s' "$report_json" | rg -q '"projection"'
printf '%s' "$report_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["report"]["projection"]["profile"]["profile_id"] == "cockpit-profile-v0992-ops"; assert len(d["report"]["projection"]["widgets"]) == 4'

curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0992-daily" | rg -q '"delivery_ref":"channel://feishu/user/ops-planner"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_report_count":1'
test -f "$WORKDIR/.cowd/iacc.sqlite"
