#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0945_PORT:-18671}"
BASE_URL="http://127.0.0.1:$PORT"
CHROMIUM="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-/snap/bin/chromium}"
TMUX_SESSION="cowd-v0945-three-plane-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0945-three-plane.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-v0945-scenario}"

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
  echo "v0.9.45 three-plane session scenario failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss sqlite3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for v0.9.45 three-plane session scenario" >&2
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
cargo build -p cowd-cli

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
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..80}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

if ! curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
  print_logs
  exit 1
fi

(cd "$ROOT/webui" && \
  env COWD_WEBUI_BASE_URL="$BASE_URL" \
    PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$CHROMIUM" \
    npx playwright test connector-console.live.e2e.spec.js \
      --config=playwright.live.config.js \
      --browser=chromium)

sqlite3 "$WORKDIR/.cowd/resource-directory.sqlite" "SELECT title || ' ' || reference FROM connector_resources;" \
  | rg -q "v0945-webui-doc"
rg -q "v0945-webui-live" "$CONFIG_HOME/cross-plane/control-state.json"
rg -q "session_id" "$CONFIG_HOME/cross-plane/control-state.json"

echo "v0.9.45 three-plane WebUI/API/daemon same-session scenario passed"
