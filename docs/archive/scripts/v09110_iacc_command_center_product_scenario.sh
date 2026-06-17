#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09110_PORT:-18730}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09110-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09110-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.110 IACC command center scenario" >&2
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

curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":17'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"command_center_live"'
curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

profile_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09110-profile","session_id":"session-v09110","profile":{"profile_id":"profile-v09110-ops","owner_ref":"user:ops-planner","display_name":"Ops planner","focus_refs":["component:gpu-v09110"],"focus_metric_ids":["material_shortage_risk","order_delivery_risk"],"cadence":"daily","thresholds":{"material_shortage_risk":{"critical":100},"order_delivery_risk":{"critical":0.65}},"template_id":"ops.default"}}')"
printf '%s' "$profile_json" | rg -q '"kind":"iacc.cockpit.profile"'

packet_json="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09110-packet","session_id":"session-v09110","problem_statement":"Delivery risk for command center live view"}')"
packet_id="$(printf '%s' "$packet_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["packet"]["packet_id"])')"

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09110-incident","session_id":"session-v09110","title":"Command center live incident","evidence_packet_id":"'"$packet_id"'"}')"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["incident"]["incident_id"])')"

curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST >/dev/null

skill_run_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/skills/supply-risk-analyst/run" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09110-run","session_id":"session-v09110"}')"
printf '%s' "$skill_run_json" | rg -q '"kind":"iacc.skill.run"'

live_json="$(curl -fsS "$BASE_URL/api/iacc/command-center/live")"
printf '%s' "$live_json" | rg -q '"kind":"iacc.command_center.live"'
printf '%s' "$live_json" | rg -q '"incident_queue"'
printf '%s' "$live_json" | rg -q '"skill_queue"'

incidents_json="$(curl -fsS "$BASE_URL/api/iacc/incidents")"
printf '%s' "$incidents_json" | rg -q '"kind":"iacc.incident.list"'
printf '%s' "$incidents_json" | rg -q '"Command center live incident"'

projection_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/profile-v09110-ops/projection")"
printf '%s' "$projection_json" | rg -q '"kind":"iacc.cockpit.projection"'
printf '%s' "$projection_json" | rg -q '"focus_thresholds"'

test -f "$WORKDIR/.cowd/iacc.sqlite"
