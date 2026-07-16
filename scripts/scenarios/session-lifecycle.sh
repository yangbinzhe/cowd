#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_SESSION_LIFECYCLE_PORT:-18688}"
BASE_URL="http://127.0.0.1:$PORT"
GATEWAY_SESSION="cowd-session-lifecycle-gateway-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-session-lifecycle.XXXXXX)"
FAILED=0
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
GATEWAY_LOG="$TMP_DIR/gateway.log"
API_TOKEN="session-lifecycle-$$_credential"
SESSION_ID="session-lifecycle-session-$$"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-session-lifecycle-scenario}"

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_SESSION_LIFECYCLE_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving session lifecycle scenario temp dir: $TMP_DIR" >&2
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
  echo "session lifecycle scenario failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for session lifecycle scenario" >&2
    exit 1
  fi
done

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
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

curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/ensure" \
  -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet-4-6"}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is True and data.get("session_id") == sys.argv[1], data' "$SESSION_ID"
for attachment in '{"surface":"tui","role":"writer"}' '{"surface":"webui","role":"reader"}'; do
  curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/attach" \
    -H 'content-type: application/json' \
    -d "$attachment" \
    | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is True, data'
done
curl -fsS "$BASE_URL/api/sessions/$SESSION_ID/lifecycle" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); snapshot=data.get("snapshot",{}); assert data.get("ok") is True and snapshot.get("state") == "attached" and len(snapshot.get("attachments",[])) == 2, data'
curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/detach" \
  -H 'content-type: application/json' \
  -d '{"surface":"tui"}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); snapshot=data.get("snapshot",{}); assert data.get("ok") is True and snapshot.get("state") == "attached" and len(snapshot.get("attachments",[])) == 1 and snapshot["attachments"][0]["actor"]["surface"] == "webui", data'
curl -fsS "$BASE_URL/api/sessions/$SESSION_ID/replay?from_sequence=0&limit=20" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is True and data.get("total", 0) >= 3, data'
curl -fsS "$BASE_URL/api/runtime/snapshot" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert sys.argv[1] in (data.get("sessions") or []), data' "$SESSION_ID"

tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
echo "session lifecycle scenario passed"
