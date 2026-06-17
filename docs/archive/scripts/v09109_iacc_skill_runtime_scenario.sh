#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09109_PORT:-18729}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09109-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09109-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.109 IACC skill runtime scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":17'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"skill_execution_record"'

curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

packet_json="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09109-packet","session_id":"session-v09109","problem_statement":"GPU shortage and delivery risk for server build plan"}')"
printf '%s' "$packet_json" | rg -q '"kind":"iacc.evidence.packet"'
packet_id="$(printf '%s' "$packet_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["packet"]["packet_id"])')"

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09109-incident","session_id":"session-v09109","title":"GPU shortage and delivery risk","evidence_packet_id":"'"$packet_id"'"}')"
printf '%s' "$incident_json" | rg -q '"kind":"iacc.incident"'
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["incident"]["incident_id"])')"

curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST | rg -q '"kind":"iacc.operational_analysis"'

plan_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/skills/plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09109-plan","session_id":"session-v09109","limit":3}')"
printf '%s' "$plan_json" | rg -q '"kind":"iacc.skill.plan"'
printf '%s' "$plan_json" | rg -q '"supply-risk-analyst"'

run_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/skills/supply-risk-analyst/run" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09109-run","session_id":"session-v09109"}')"
printf '%s' "$run_json" | rg -q '"kind":"iacc.skill.run"'

skill_run_id="$(printf '%s' "$run_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["skill_run"]["execution_id"])')"

curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/skills" | rg -q '"kind":"iacc.skill.run_list"'
curl -fsS "$BASE_URL/api/iacc/skill-runs/$skill_run_id" | rg -q '"kind":"iacc.skill.run"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"skill_execution_count":1'

test -f "$WORKDIR/.cowd/iacc.sqlite"
