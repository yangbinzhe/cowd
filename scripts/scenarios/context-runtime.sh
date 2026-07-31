#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_CONTEXT_RUNTIME_PORT:-18692}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-context-runtime-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-context-runtime.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
API_TOKEN="context-runtime-$$_credential"
FAILED=0

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_CONTEXT_RUNTIME_KEEP_TMP:-0}" == "1" ]]; then
    echo "preserving context runtime temp dir: $TMP_DIR" >&2
    return
  fi
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}

on_error() {
  local status=$?
  FAILED=1
  echo "context runtime scenario failed with status $status" >&2
  echo "----- context runtime temp dir -----" >&2
  echo "$TMP_DIR" >&2
  echo "----- gateway log -----" >&2
  sed -n '1,260p' "$LOG" >&2 || true
  echo "-----------------------" >&2
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for context runtime scenario" >&2
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
  scenario:
    base_url: "http://127.0.0.1:1"
    api_key: "context-runtime-provider-key"
    protocol: "completions"
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

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..80}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

context_json="$(curl -fsS "$BASE_URL/api/context/current?q=delegate%20review&session_id=context-runtime-session&profile=yolo&agent_id=reviewer&agent_task=review%20context")"
printf '%s' "$context_json" | rg -q '"snapshot"'
printf '%s' "$context_json" | rg -q '"budget_explanation"'
printf '%s' "$context_json" | rg -q '"agent_view"'
printf '%s' "$context_json" | rg -q '"child_agent_id":"reviewer"'
printf '%s' "$context_json" | rg -q '"stable_head_hash"'
printf '%s' "$context_json" | rg -q '"allocations"'
printf '%s' "$context_json" | rg -q '"profile":"YoloGoal"'
printf '%s' "$context_json" | rg -q '"profile":"SubAgent"'

stable_hash="$(printf '%s' "$context_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["snapshot"]["stable_head_hash"])')"
agent_hash="$(printf '%s' "$context_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["agent_view"]["envelope"]["diagnostics"]["stable_head_hash"])')"
if [[ "$stable_hash" != "$agent_hash" ]]; then
  echo "agent view did not preserve stable head hash" >&2
  exit 1
fi

curl -fsS "$BASE_URL/readyz" | rg -q '"ready":true'
