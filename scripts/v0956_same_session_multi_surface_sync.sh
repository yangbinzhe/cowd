#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0956_PORT:-18676}"
BASE_URL="http://127.0.0.1:$PORT"
TMUX_SESSION="cowd-v0956-sync-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0956-sync.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
SOCKET="$TMP_DIR/cowd.sock"
LOG="$TMP_DIR/gateway.log"
SESSION_ID="v0956-session-$$"
OWNER="tui:v0956-$$"
TASK_OBJECTIVE="v0956 same-session sync task $$"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-v0956-scenario}"

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
  echo "v0.9.56 same-session multi-surface sync failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for v0.9.56 same-session sync scenario" >&2
    exit 1
  fi
done

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

cd "$ROOT"
if [[ "${COWD_SCENARIO_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build -p cowd-cli
fi
if [[ ! -x "$BIN" ]]; then
  echo "missing cowd binary at $BIN; run cargo build -p cowd-cli first" >&2
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

tmux new-session -d -s "$TMUX_SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export COWD_DAEMON_SOCKET='$SOCKET' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..100}; do
  if [[ -S "$SOCKET" ]] && curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

[[ -S "$SOCKET" ]]
curl -fsS "$BASE_URL/health" >/dev/null

python3 - "$SOCKET" "$SESSION_ID" "$OWNER" "$TASK_OBJECTIVE" <<'PY'
import json
import socket
import sys

sock_path, session_id, owner, objective = sys.argv[1:5]

def request(payload):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(sock_path)
        client.sendall(json.dumps(payload).encode("utf-8") + b"\n")
        chunks = []
        while not chunks or not chunks[-1].endswith(b"\n"):
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
        data = json.loads(b"".join(chunks).decode("utf-8").strip())
        if not data.get("ok"):
            raise SystemExit(json.dumps(data, ensure_ascii=False))
        return data

request({"cmd": "ensure_session", "protocol_version": 1, "session_id": session_id, "model": "claude-sonnet-4-6"})
request({"cmd": "acquire_session_lease", "protocol_version": 1, "session_id": session_id, "owner": owner, "mode": "collaborative"})
snapshot = request({"cmd": "runtime_snapshot", "protocol_version": 1})
assert session_id in snapshot.get("sessions", []), snapshot
assert any(item.get("owner") == owner for item in snapshot.get("leases", {}).get("items", [])), snapshot
task = request({"cmd": "task_start", "protocol_version": 1, "objective": objective, "yolo_mode": True})
assert task.get("task", {}).get("objective") == objective, task
PY

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
echo "v0.9.56 same-session multi-surface sync scenario passed"
