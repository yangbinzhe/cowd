#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0972_PORT:-18692}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0972-context-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0972-context.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.72 context scenario" >&2
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

for _ in {1..80}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

context_json="$(curl -fsS "$BASE_URL/api/context/current?q=delegate%20review&session_id=v0972-session&profile=yolo&agent_id=reviewer&agent_task=review%20context")"
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

curl -fsS "$BASE_URL/runtime/context/inspect" | rg -q '<title>Cowd Web UI</title>'
