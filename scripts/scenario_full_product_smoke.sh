#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="${1:-${COWD_INSTALL_DIR:-}}"
if [[ -n "$INSTALL_DIR" ]]; then
  BIN="${COWD_BIN:-$INSTALL_DIR/bin/cowd}"
else
  TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
  BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
fi

PORT="${COWD_RELEASE_SMOKE_PORT:-18695}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-release-smoke-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-release-smoke.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
SOCKET="$TMP_DIR/cowd.sock"
LOG="$TMP_DIR/gateway.log"
SMOKE_ID="release-smoke-$$"
PRINCIPAL="user:$SMOKE_ID"
GRANT_ID="grant-$SMOKE_ID"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}

print_logs() {
  echo "----- release smoke temp dir -----" >&2
  echo "$TMP_DIR" >&2
  echo "----- gateway log -----" >&2
  sed -n '1,260p' "$LOG" >&2 || true
  echo "-----------------------" >&2
}

on_error() {
  local status=$?
  echo "release full-product smoke failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss sqlite3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for release full-product smoke" >&2
    exit 1
  fi
done

if [[ ! -x "$BIN" ]]; then
  echo "missing executable cowd binary at $BIN" >&2
  exit 1
fi
if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"

cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "${ANTHROPIC_API_KEY:-test-dummy-key-for-release-smoke}"
    protocol: "anthropic"
    models:
      - "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: true
  store:
    sqlite_path: "$TMP_DIR/memory.db"
    blob_dir: "$TMP_DIR/blobs"
    enable_vector_index: false
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
    export COWD_DAEMON_SOCKET='$SOCKET' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..120}; do
  if [[ -S "$SOCKET" ]] && curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

[[ -S "$SOCKET" ]]
curl -fsS "$BASE_URL/health" >/dev/null
curl -fsS "$BASE_URL/healthz" >/dev/null
curl -fsS "$BASE_URL/readyz" | rg -q '"ready":true'
curl -fsS "$BASE_URL/api/webui/manifest" | rg -q '"config_key":"gateway.webui_dir"'

python3 - "$SOCKET" "$SMOKE_ID" <<'PY'
import json
import socket
import sys

sock_path, session_id = sys.argv[1:3]

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
request({"cmd": "acquire_session_lease", "protocol_version": 1, "session_id": session_id, "owner": "release-smoke", "mode": "collaborative"})
task = request({"cmd": "task_start", "protocol_version": 1, "objective": "release full product smoke", "yolo_mode": True})
assert task.get("task", {}).get("status") == "running", task
snapshot = request({"cmd": "runtime_snapshot", "protocol_version": 1})
assert session_id in snapshot.get("sessions", []), snapshot
PY

curl -fsS "$BASE_URL/api/context/current?q=release%20full%20product&session_id=$SMOKE_ID" \
  | rg -q '"session_id"\s*:\s*"'"$SMOKE_ID"'"|release full product'

curl -fsS "$BASE_URL/api/memory/L3" \
  -H 'content-type: application/json' \
  -d '{"title":"RELEASE_SMOKE_MEMORY","content":"Release smoke validates memory runtime wiring.","tags":["release-smoke"],"category":"Reference","priority":"High"}' \
  | rg -q '"id"'
curl -fsS "$BASE_URL/api/memory/runtime" | rg -q '"runtime"'

curl -fsS "$BASE_URL/api/connectors/summary" | rg -q '"kind"\s*:\s*"connector_summary"'
curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d "{\"id\":\"$GRANT_ID\",\"principal_id\":\"$PRINCIPAL\",\"capability\":\"service.mock.docs.read\",\"grant_type\":\"single_use\",\"created_by\":\"release-smoke\"}" \
  | rg -q "\"$GRANT_ID\""
curl -fsS "$BASE_URL/api/connectors/services/mock.docs/execute" \
  -H 'content-type: application/json' \
  -d "{\"actor_principal\":\"$PRINCIPAL\",\"source_channel\":\"channel://tui/release\",\"session_id\":\"$SMOKE_ID\",\"tool_id\":\"service.mock.docs.read\",\"resource_id\":\"release-doc-$SMOKE_ID\",\"title\":\"Release Smoke Doc\",\"mode\":\"commit\",\"idempotency_key\":\"release-$SMOKE_ID\"}" \
  | rg -q '"status"\s*:\s*"ok"'
curl -fsS "$BASE_URL/api/cross-plane/audit" | rg -q "\"consumed_grant_id\"\\s*:\\s*\"$GRANT_ID\""

sqlite3 "$WORKDIR/.cowd/resource-directory.sqlite" "SELECT reference FROM connector_resources;" \
  | rg -q "service://mock.docs/document/release-doc-$SMOKE_ID"

echo "release full-product smoke passed"
