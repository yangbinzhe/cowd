#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_SAME_SESSION_SYNC_PORT:-18676}"
BASE_URL="http://127.0.0.1:$PORT"
TMUX_SESSION="cowd-same-session-sync-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-same-session-sync.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
API_TOKEN="same-session-sync-$$_credential"
SESSION_ID="same-session-session-$$"
TUI_OBSERVER_ID="tui:same-session-writer-$$"
WEBUI_OBSERVER_ID="webui:same-session-reader-$$"
OWNER="principal:local-human:observer:$TUI_OBSERVER_ID"
TASK_OBJECTIVE="same-session same-session sync task $$"
TASK_ID="task-same-session-sync-$$"
TASK_TURN_ID="turn-same-session-sync-$$"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-same-session-scenario}"

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$TMUX_SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}

print_logs() {
  echo "----- gateway log -----" >&2
  sed -n '1,220p' "$LOG" >&2 || true
  echo "-----------------------" >&2
}

on_error() {
  local status=$?
  echo "same-session multi-surface sync failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for same-session sync scenario" >&2
    exit 1
  fi
done

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

cd "$ROOT"
if [[ "${COWD_SCENARIO_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build -p cli
fi
if [[ ! -x "$BIN" ]]; then
  echo "missing cowd binary at $BIN; run cargo build -p cli first" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "$SCENARIO_API_KEY"
    protocol: "anthropic"
    models:
      - "claude-sonnet-4-6"
permissions:
  default_mode: "danger-full-access"
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
        enabled: true
        token: "$API_TOKEN"
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$TMUX_SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..100}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

curl -fsS "$BASE_URL/health" >/dev/null

curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/ensure" \
  -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet-4-6"}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is True and data.get("session_id") == sys.argv[1], data' "$SESSION_ID"
curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/attach" \
  -H "x-cowd-observer-id: $TUI_OBSERVER_ID" \
  -H 'x-cowd-surface-id: tui' \
  -H 'content-type: application/json' \
  -d '{"surface":"tui","role":"writer"}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is True, data'
curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/attach" \
  -H "x-cowd-observer-id: $WEBUI_OBSERVER_ID" \
  -H 'x-cowd-surface-id: webui' \
  -H 'content-type: application/json' \
  -d '{"surface":"webui","role":"reader"}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is True, data'
curl -fsS "$BASE_URL/api/sessions/$SESSION_ID/lifecycle" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); items=data.get("snapshot",{}).get("attachments",[]); assert data.get("ok") is True and len(items) == 2 and {item["actor"]["surface"] for item in items} == {"tui","webui"}, data'
curl -fsS -X POST "$BASE_URL/api/runtime/session-leases/acquire" \
  -H "x-cowd-observer-id: $TUI_OBSERVER_ID" \
  -H 'x-cowd-surface-id: tui' \
  -H 'content-type: application/json' \
  -d "{\"session_id\":\"$SESSION_ID\",\"mode\":\"collaborative\"}" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is True, data'
curl -fsS "$BASE_URL/api/runtime/snapshot" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert sys.argv[1] in (data.get("sessions") or []), data' "$SESSION_ID"
MISSION_ID=""
for _ in {1..120}; do
  MISSION_ID="$(curl -fsS "$BASE_URL/api/mission/projection" \
    | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data.get("mission",{}).get("mission_id") or "")')"
  [[ -n "$MISSION_ID" ]] && break
  sleep 0.25
done
[[ -n "$MISSION_ID" ]]
curl -fsS -X POST "$BASE_URL/api/tasks/start" \
  -H 'content-type: application/json' \
  -d "{\"task_id\":\"$TASK_ID\",\"mission_id\":\"$MISSION_ID\",\"source_session_id\":\"$SESSION_ID\",\"source_turn_id\":\"$TASK_TURN_ID\",\"objective\":\"$TASK_OBJECTIVE\",\"yolo_mode\":true}" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("task_id") == sys.argv[1] and data.get("objective") == sys.argv[2] and data.get("status") == "running", data' "$TASK_ID" "$TASK_OBJECTIVE"

LEASES_JSON="$TMP_DIR/session-leases.json"
TASKS_JSON="$TMP_DIR/tasks.json"
curl -fsS "$BASE_URL/api/runtime/session-leases" -o "$LEASES_JSON"
python3 - "$LEASES_JSON" "$SESSION_ID" "$OWNER" <<'PY'
import json
import sys
path, session_id, owner = sys.argv[1:4]
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)
text = json.dumps(data, ensure_ascii=False)
if session_id not in text or owner not in text:
    print(text, file=sys.stderr)
    raise SystemExit(1)
PY

curl -fsS "$BASE_URL/api/tasks" -o "$TASKS_JSON"
python3 - "$TASKS_JSON" "$TASK_OBJECTIVE" <<'PY'
import json
import sys
path, objective = sys.argv[1:3]
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)
tasks = data.get("tasks") or []
if not any(task.get("objective") == objective and task.get("status") == "running" for task in tasks):
    print(json.dumps(data, ensure_ascii=False, indent=2), file=sys.stderr)
    raise SystemExit(1)
PY

tmux kill-session -t "$TMUX_SESSION" >/dev/null 2>&1 || true
echo "same-session multi-surface sync scenario passed"
