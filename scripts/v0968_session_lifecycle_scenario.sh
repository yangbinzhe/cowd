#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0968_PORT:-18688}"
BASE_URL="http://127.0.0.1:$PORT"
GATEWAY_SESSION="cowd-v0968-gateway-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0968-session-lifecycle.XXXXXX)"
FAILED=0
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
SOCKET="$TMP_DIR/cowd.sock"
GATEWAY_LOG="$TMP_DIR/gateway.log"
SESSION_ID="v0968-session-$$"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-v0968-scenario}"

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_V0968_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving v0.9.68 scenario temp dir: $TMP_DIR" >&2
    return
  fi
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}

print_logs() {
  echo "----- scenario temp dir -----" >&2
  echo "$TMP_DIR" >&2
  echo "----- gateway log -----" >&2
  sed -n '1,260p' "$GATEWAY_LOG" >&2 || true
  echo "-----------------------" >&2
}

on_error() {
  local status=$?
  FAILED=1
  echo "v0.9.68 session lifecycle scenario failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for v0.9.68 session lifecycle scenario" >&2
    exit 1
  fi
done

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
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
first = request({"cmd": "session.attach", "protocol_version": 1, "session_id": session_id, "actor_id": "tui:v0968", "surface": "tui", "role": "writer"})
assert first["event"]["sequence"] == 0, first
second = request({"cmd": "session.attach", "protocol_version": 1, "session_id": session_id, "actor_id": "web:v0968", "surface": "webui", "role": "reader"})
assert second["event"]["sequence"] == 1, second
snapshot = request({"cmd": "session.lifecycle", "protocol_version": 1, "session_id": session_id})
assert snapshot["snapshot"]["state"] == "attached", snapshot
assert len(snapshot["snapshot"]["attachments"]) == 2, snapshot
detached = request({"cmd": "session.detach", "protocol_version": 1, "session_id": session_id, "actor_id": "tui:v0968"})
assert detached["snapshot"]["state"] == "attached", detached
assert len(detached["snapshot"]["attachments"]) == 1, detached
replay = request({"cmd": "session.replay", "protocol_version": 1, "session_id": session_id, "from_sequence": 0, "limit": 20})
assert replay["ok"] is True, replay
runtime = request({"cmd": "runtime.snapshot", "protocol_version": 1})
assert session_id in runtime.get("sessions", []), runtime
assert any(item["session_id"] == session_id for item in runtime.get("lifecycle", [])), runtime
PY

tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
echo "v0.9.68 session lifecycle scenario passed"
