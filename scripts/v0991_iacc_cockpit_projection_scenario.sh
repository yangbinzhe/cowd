#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0991_PORT:-18711}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0991-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0991-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.91 IACC cockpit projection scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"personal_cockpit_projection"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_profile_thresholds"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0991","session_id":"session-v0991","facts":[{"fact_id":"fact-v0991-shortage-a","snapshot_id":"snapshot-v0991-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0991","product:server-v0991"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W32"},"measures":{"short_qty":320},"source_ref":"connector:erp:material-shortage","confidence":0.94}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_id="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0991\",\"session_id\":\"session-v0991\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.91 GPU shortage cockpit incident\"}" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"

curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id/quality-gate" -X POST | rg -q '"decision":"review"'

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0991\",\"session_id\":\"session-v0991\",\"title\":\"GPU shortage cockpit incident\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"

analysis_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST)"
analysis_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["analysis_id"])')"
action_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["recommended_actions"][0]["action_id"])')"
curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id/quality-gate" -X POST | rg -q '"decision":"pass"'

curl -fsS "$BASE_URL/api/iacc/analyses/$analysis_id/actions/$action_id/execute" \
  -H 'content-type: application/json' \
  -d '{"mode":"commit","operator_id":"user:ops-planner","note":"queue cockpit-visible recovery action"}' \
  | rg -q '"status":"queued_for_human_review"'

profile_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0991","session_id":"session-v0991","profile":{"profile_id":"cockpit-profile-v0991-ops","owner_ref":"user:ops-planner","display_name":"Ops planner cockpit","focus_refs":["component:gpu-v0991"],"focus_metric_ids":["material_shortage_risk"],"thresholds":{"material_shortage_risk":{"critical":100,"warning":40}},"template_id":"ops.default","cadence":"daily"}}')"
printf '%s' "$profile_json" | rg -q '"profile_id":"cockpit-profile-v0991-ops"'
curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/cockpit-profile-v0991-ops" | rg -q '"owner_ref":"user:ops-planner"'

projection_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/cockpit-profile-v0991-ops/projection")"
printf '%s' "$projection_json" | rg -q '"kind":"iacc.cockpit.projection"'
printf '%s' "$projection_json" | rg -q '"attention_queue"'
printf '%s' "$projection_json" | rg -q '"quality_gate_status"'
printf '%s' "$projection_json" | rg -q '"action_execution_status"'
printf '%s' "$projection_json" | rg -q '"focus_thresholds"'
printf '%s' "$projection_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); widgets={w["widget_type"]:w for w in d["projection"]["widgets"]}; assert widgets["attention_queue"]["data"]["count"] >= 1; assert widgets["quality_gate_status"]["data"]["pass_count"] >= 1; assert widgets["action_execution_status"]["data"]["active_count"] >= 1; assert widgets["focus_thresholds"]["status"] == "configured"'

curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_profile_count":1'
test -f "$WORKDIR/.cowd/iacc.sqlite"
