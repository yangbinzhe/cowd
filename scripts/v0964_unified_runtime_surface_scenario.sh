#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0964_PORT:-18684}"
BASE_URL="http://127.0.0.1:$PORT"
CHROMIUM="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-/snap/bin/chromium}"
GATEWAY_SESSION="cowd-v0964-gateway-$$"
TUI_SESSION="cowd-v0964-tui-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0964-unified.XXXXXX)"
FAILED=0
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
SOCKET="$TMP_DIR/cowd.sock"
GATEWAY_LOG="$TMP_DIR/gateway.log"
TUI_CAPTURE="$TMP_DIR/tui-pane.txt"
TUI_EXIT="$TMP_DIR/tui-exit.txt"
TUI_STDERR="$TMP_DIR/tui-stderr.txt"
TUI_RUNNER="$TMP_DIR/run-tui.sh"
SESSION_ID="v0964-session-$$"
TASK_OBJECTIVE="v0.9.64 real unified runtime surface scenario $$"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-v0964-scenario}"

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_V0964_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving v0.9.64 scenario temp dir: $TMP_DIR" >&2
    return
  fi
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
    tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}

print_logs() {
  echo "----- scenario temp dir -----" >&2
  echo "$TMP_DIR" >&2
  echo "----- tmux sessions -----" >&2
  tmux ls 2>/dev/null | sed -n '1,80p' >&2 || true
  echo "----- tui exit -----" >&2
  sed -n '1,20p' "$TUI_EXIT" >&2 || true
  echo "----- tui stderr -----" >&2
  sed -n '1,160p' "$TUI_STDERR" >&2 || true
  echo "----- gateway log -----" >&2
  sed -n '1,260p' "$GATEWAY_LOG" >&2 || true
  echo "----- tui capture -----" >&2
  sed -n '1,260p' "$TUI_CAPTURE" >&2 || true
  echo "-----------------------" >&2
}

on_error() {
  local status=$?
  FAILED=1
  echo "v0.9.64 unified runtime surface scenario failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss sqlite3 npm; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for v0.9.64 unified scenario" >&2
    exit 1
  fi
done

if [[ ! -x "$CHROMIUM" ]]; then
  echo "chromium executable not found at $CHROMIUM" >&2
  exit 1
fi

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

cd "$ROOT"
cargo build -p cowd-cli --no-default-features

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
ln -s "$ROOT/webui" "$WORKDIR/webui"

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
  enabled: true
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

tmux new-session -d -s "$GATEWAY_SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export COWD_DAEMON_SOCKET='$SOCKET' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$GATEWAY_LOG' 2>&1\""

for _ in {1..120}; do
  if [[ -S "$SOCKET" ]] && curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

[[ -S "$SOCKET" ]]
curl -fsS "$BASE_URL/health" >/dev/null

python3 - "$SOCKET" "$SESSION_ID" <<'PY'
import json
import socket
import sys

sock_path, session_id = sys.argv[1:3]

def request(payload):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(sock_path)
        client.sendall(json.dumps(payload).encode("utf-8") + b"\n")
        data = b""
        while not data.endswith(b"\n"):
            chunk = client.recv(65536)
            if not chunk:
                break
            data += chunk
        response = json.loads(data.decode("utf-8").strip())
        if not response.get("ok"):
            raise SystemExit(json.dumps(response, ensure_ascii=False))
        return response

request({"cmd": "ensure_session", "protocol_version": 1, "session_id": session_id, "model": "claude-sonnet-4-6"})
request({"cmd": "acquire_session_lease", "protocol_version": 1, "session_id": session_id, "owner": "script:v0964-live", "mode": "collaborative"})
snapshot = request({"cmd": "runtime_snapshot", "protocol_version": 1})
assert session_id in snapshot.get("sessions", []), snapshot
PY

cat >"$TUI_RUNNER" <<EOF
#!/usr/bin/env bash
set +e
cd "$WORKDIR" || exit 90
export COWD_CONFIG_HOME="$CONFIG_HOME"
export COWD_DAEMON_SOCKET="$SOCKET"
export HOME="$HOME_DIR"
export ANTHROPIC_API_KEY="$SCENARIO_API_KEY"
export COWD_DISABLE_DAEMON_AUTOSTART=1
export COWD_TUI_ACCESSIBILITY=1
export COWD_TUI_SKIP_RAW_MODE=1
export RUST_BACKTRACE=1
export TERM=xterm-256color
timeout 18s "$BIN" --session "$SESSION_ID" --yolo --model claude-sonnet-4-6 2>"$TUI_STDERR"
status=\$?
printf '%s\n' "\$status" >"$TUI_EXIT"
printf '\n__COWD_TUI_EXIT__%s\n' "\$status"
sleep 60
EOF
chmod +x "$TUI_RUNNER"

tmux new-session -d -s "$TUI_SESSION" -x 150 -y 44 "$TUI_RUNNER"
TUI_PANE="$TUI_SESSION:0.0"

for _ in {1..80}; do
  tmux capture-pane -pt "$TUI_PANE" -S -320 >"$TUI_CAPTURE" 2>/dev/null || true
  if rg -q "Daemon control connected|Daemon session (created|attached)|Daemon session lease acquired" "$TUI_CAPTURE"; then
    break
  fi
  sleep 0.25
done

tmux capture-pane -pt "$TUI_PANE" -S -320 >"$TUI_CAPTURE"
rg -q "Daemon control connected" "$TUI_CAPTURE"
rg -q "Daemon session (created|attached)" "$TUI_CAPTURE"
rg -q "Daemon session lease acquired" "$TUI_CAPTURE"
if [[ -s "$TUI_EXIT" ]] && ! rg -q '^(0|124)$' "$TUI_EXIT"; then
  exit 1
fi
if rg -q "__COWD_TUI_EXIT__[1-9][0-9]*|panic|backtrace|thread .* panicked|failed to initialize terminal|Run cowd --help" "$TUI_CAPTURE"; then
  exit 1
fi

(cd "$ROOT/webui" && \
  env COWD_WEBUI_BASE_URL="$BASE_URL" \
    COWD_V0964_SESSION_ID="$SESSION_ID" \
    COWD_V0964_TASK_OBJECTIVE="$TASK_OBJECTIVE" \
    PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$CHROMIUM" \
    npx playwright test unified-runtime.live.e2e.spec.js \
      --config=playwright.live.config.js \
      --browser=chromium)

curl -fsS "$BASE_URL/api/runtime/control-plane" >"$TMP_DIR/control-plane.json"
curl -fsS "$BASE_URL/api/runtime/session-leases" >"$TMP_DIR/leases.json"
curl -fsS "$BASE_URL/api/tasks" >"$TMP_DIR/tasks.json"
curl -fsS "$BASE_URL/api/memory/status" >"$TMP_DIR/memory.json"
curl -fsS "$BASE_URL/api/connectors/summary" >"$TMP_DIR/connectors.json"

rg -q "$SESSION_ID" "$TMP_DIR/leases.json"
rg -q "$TASK_OBJECTIVE" "$TMP_DIR/tasks.json"
rg -q "runtime_control_plane" "$TMP_DIR/control-plane.json"
rg -q "connector_summary" "$TMP_DIR/connectors.json"

sqlite3 "$CONFIG_HOME/tasks.db" "SELECT record_json FROM tasks;" | rg -q "$TASK_OBJECTIVE"
sqlite3 "$CONFIG_HOME/tasks.db" "SELECT record_json FROM tasks;" | rg -q "accepted by v0.9.64 real unified scenario"

if [[ "${COWD_V0964_REAL_CONNECTOR_PROVIDER:-}" == "feishu.readonly" ]]; then
  curl -fsS "$BASE_URL/api/connectors/services/feishu.readonly/tools" >"$TMP_DIR/feishu-tools.json"
  rg -q "service.feishu.docx.read" "$TMP_DIR/feishu-tools.json"
fi

tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
echo "v0.9.64 unified runtime surface scenario passed"
