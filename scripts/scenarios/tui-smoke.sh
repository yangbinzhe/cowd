#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_TUI_SMOKE_PORT:-18671}"
BASE_URL="http://127.0.0.1:$PORT"
GATEWAY_SESSION="cowd-tui-smoke-gateway-$$"
SESSION="cowd-tui-smoke-$$"
TUI_RUNTIME_SESSION="cowd-tui-smoke-runtime-$$_session"
TMP_DIR="$(mktemp -d /tmp/cowd-tui-smoke.XXXXXX)"
CAPTURE="$TMP_DIR/pane.txt"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
WORKSPACE="$TMP_DIR/workspace"
GATEWAY_LOG="$TMP_DIR/gateway.log"
SMOKE_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-tui-smoke}"
API_TOKEN="tui-smoke-$$_credential"

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
    tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

for cmd in tmux curl rg ss; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for TUI smoke test" >&2
    exit 1
  fi
done

if [[ "${COWD_TUI_SMOKE_SKIP_BUILD:-0}" != "1" ]]; then
  # The smoke path exercises the real Ratatui surface.  A default CLI build
  # intentionally omits that surface, so never let an earlier minimal build
  # turn this acceptance scenario into a false negative.
  (cd "$ROOT" && cargo build -p cli --features full)
fi

if [[ ! -x "$BIN" ]]; then
  echo "missing cowd binary at $BIN; run cargo build -p cli --features full first" >&2
  exit 1
fi
if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$CONFIG_HOME" "$HOME_DIR/.cowd" "$WORKSPACE"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "$SMOKE_API_KEY"
    protocol: "anthropic"
    models:
      - "claude-sonnet-4-6"
permissions:
  default_mode: "danger-full-access"
memory:
  enabled: false
gateway:
  enabled: true
  session_reset: "none"
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
mkdir -p "$WORKSPACE/.cowd"
cp "$CONFIG_HOME/config.yaml" "$WORKSPACE/.cowd/config.yaml"

tmux new-session -d -s "$GATEWAY_SESSION" -c "$WORKSPACE" \
  "exec env COWD_CONFIG_HOME='$CONFIG_HOME' \
    HOME='$HOME_DIR' \
    '$BIN' gateway run >'$GATEWAY_LOG' 2>&1"

for _ in {1..100}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -fsS "$BASE_URL/health" >/dev/null

tmux new-session -d -s "$SESSION" -x 120 -y 36 -c "$WORKSPACE" \
  "exec env COWD_CONFIG_HOME='$CONFIG_HOME' \
    COWD_API_TOKEN='$API_TOKEN' \
    COWD_GATEWAY_URL='$BASE_URL' \
    HOME='$HOME_DIR' \
    ANTHROPIC_API_KEY='$SMOKE_API_KEY' \
    COWD_DISABLE_DAEMON_AUTOSTART=1 \
    COWD_TUI_ACCESSIBILITY=1 \
    TERM=xterm-256color \
    timeout 45s '$BIN' --yolo --model claude-sonnet-4-6 --session '$TUI_RUNTIME_SESSION'"

attached=0
for _ in {1..80}; do
  if curl -fsS "$BASE_URL/api/sessions/$TUI_RUNTIME_SESSION" \
    | python3 -c 'import json,sys; data=json.load(sys.stdin); sys.exit(data.get("id") != sys.argv[1])' "$TUI_RUNTIME_SESSION" \
      2>/dev/null; then
    attached=1
    break
  fi
  tmux has-session -t "$SESSION" >/dev/null 2>&1 || break
  sleep 0.25
done

if [[ "$attached" != "1" ]]; then
  echo "TUI smoke test did not attach its session to the Gateway" >&2
  tmux capture-pane -pt "$SESSION" -S -200 >"$CAPTURE" 2>/dev/null || true
  sed -n '1,160p' "$CAPTURE" >&2
  sed -n '1,160p' "$GATEWAY_LOG" >&2
  exit 1
fi

sleep 0.5
tmux send-keys -t "$SESSION" -l '/tasks start --yolo TUI smoke durable task'
tmux send-keys -t "$SESSION" C-m
sleep 0.25
tmux send-keys -t "$SESSION" C-m

task_started=0
for _ in {1..80}; do
  tmux capture-pane -pt "$SESSION" -S -200 >"$CAPTURE" 2>/dev/null || true
  if curl -fsS "$BASE_URL/api/tasks" | python3 -c '
import json, sys
tasks = json.load(sys.stdin).get("tasks") or []
sys.exit(not any(
    task.get("objective") == "TUI smoke durable task"
    and task.get("yolo_mode") is True
    and task.get("status") == "running"
    for task in tasks
))
' 2>/dev/null; then
    task_started=1
    break
  fi
  sleep 0.25
done

if [[ "$task_started" != "1" ]]; then
  echo "TUI smoke test did not create a Gateway-owned durable YOLO task through the TUI command" >&2
  sed -n '1,200p' "$CAPTURE" >&2
  exit 1
fi

if ! curl -fsS "$BASE_URL/api/slash/history" | python3 -c '
import json, sys
history = json.load(sys.stdin).get("history") or []
sys.exit(not any(
    entry.get("slash") == "/tasks"
    and entry.get("status") == "complete"
    and entry.get("data", {}).get("dispatch") == "task_service"
    for entry in history
))
' 2>/dev/null; then
  echo "TUI smoke test did not record the Gateway task-service slash receipt" >&2
  exit 1
fi

tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
echo "TUI smoke test passed"
exit 0
