#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_RUNTIME_SURFACE_PORT:-18684}"
BASE_URL="http://127.0.0.1:$PORT"
GATEWAY_SESSION="cowd-runtime-surface-gateway-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-runtime-surface.XXXXXX)"
FAILED=0
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
GATEWAY_LOG="$TMP_DIR/gateway.log"
SESSION_ID="runtime-surface-session-$$"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-runtime-surface-scenario}"

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_RUNTIME_SURFACE_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving runtime surface scenario temp dir: $TMP_DIR" >&2
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
  echo "----- tmux sessions -----" >&2
  tmux ls 2>/dev/null | sed -n '1,80p' >&2 || true
  echo "----- gateway log -----" >&2
  sed -n '1,260p' "$GATEWAY_LOG" >&2 || true
  echo "----- captured json -----" >&2
  for json in "$TMP_DIR"/*.json; do
    [[ -f "$json" ]] || continue
    echo "### $(basename "$json")" >&2
    python3 -m json.tool "$json" 2>/dev/null | sed -n '1,120p' >&2 || sed -n '1,120p' "$json" >&2
  done
  echo "-----------------------" >&2
}

on_error() {
  local status=$?
  FAILED=1
  echo "runtime surface scenario failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss sqlite3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for runtime surface unified scenario" >&2
    exit 1
  fi
done

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

cd "$ROOT"
if [[ "${COWD_SCENARIO_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build -p cli --no-default-features
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
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$GATEWAY_LOG' 2>&1\""

for _ in {1..120}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -fsS "$BASE_URL/health" >/dev/null

curl -fsS "$BASE_URL/api/sessions/$SESSION_ID/ensure" \
  -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet-4-6"}' >"$TMP_DIR/ensure-session.json"
curl -fsS "$BASE_URL/api/runtime/session-leases/acquire" \
  -H 'content-type: application/json' \
  -d "{\"session_id\":\"$SESSION_ID\",\"owner\":\"script:runtime-surface-live\",\"mode\":\"collaborative\"}" \
  >"$TMP_DIR/acquire-lease.json"
python3 - "$TMP_DIR/acquire-lease.json" "$SESSION_ID" <<'PY'
import json, sys
data=json.load(open(sys.argv[1]))
assert data.get("ok") is True, data
assert data.get("session_id") == sys.argv[2], data
PY
curl -fsS "$BASE_URL/api/tasks/start" \
  -H 'content-type: application/json' \
  -d '{"objective":"runtime surface scenario validates control plane","yolo_mode":true}' \
  >"$TMP_DIR/start-task.json"
python3 - "$TMP_DIR/start-task.json" <<'PY'
import json, sys
data=json.load(open(sys.argv[1]))
assert data.get("status") == "running", data
PY

curl -fsS "$BASE_URL/api/runtime/control-plane" >"$TMP_DIR/control-plane.json"
curl -fsS "$BASE_URL/api/runtime/session-leases" >"$TMP_DIR/leases.json"
curl -fsS "$BASE_URL/api/tasks" >"$TMP_DIR/tasks.json"
curl -fsS "$BASE_URL/api/memory/status" >"$TMP_DIR/memory.json"
curl -fsS "$BASE_URL/api/connectors/summary" >"$TMP_DIR/connectors.json"

rg -q "$SESSION_ID" "$TMP_DIR/leases.json"
rg -q "tasks" "$TMP_DIR/tasks.json"
rg -q "runtime_control_plane" "$TMP_DIR/control-plane.json"
rg -q "connector_summary" "$TMP_DIR/connectors.json"

TASK_DB="$CONFIG_HOME/storage/tasks.sqlite"
if [[ ! -f "$TASK_DB" ]]; then
  TASK_DB="$CONFIG_HOME/tasks.db"
fi
sqlite3 "$TASK_DB" "SELECT COUNT(*) FROM tasks;" | rg -q '^[1-9][0-9]*$'

if [[ "${COWD_RUNTIME_SURFACE_REAL_CONNECTOR_PROVIDER:-}" == "feishu.readonly" ]]; then
  curl -fsS "$BASE_URL/api/connectors/services/feishu.readonly/tools" >"$TMP_DIR/feishu-tools.json"
  rg -q "service.feishu.docx.read" "$TMP_DIR/feishu-tools.json"
fi

tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
echo "runtime surface scenario passed"
