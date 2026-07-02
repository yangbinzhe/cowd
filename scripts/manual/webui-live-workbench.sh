#!/usr/bin/env bash
set -euo pipefail

SURFACE_WEBUI_DIR="${COWD_SURFACE_WEBUI_DIR:-}"

if [[ -z "$SURFACE_WEBUI_DIR" ]]; then
  echo "live WebUI workbench scenario moved to cowd-edge; set COWD_SURFACE_WEBUI_DIR to surfaces/webui"
  exit 0
fi

if [[ ! -d "$SURFACE_WEBUI_DIR" ]]; then
  echo "COWD_SURFACE_WEBUI_DIR does not exist: $SURFACE_WEBUI_DIR" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_WEBUI_LIVE_PORT:-18669}"
BASE_URL="http://127.0.0.1:$PORT"
CHROMIUM="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-/snap/bin/chromium}"
SESSION="cowd-edge-webui-live-$$"
WORKDIR="$(mktemp -d -t cowd-edge-webui-live-workspace-XXXXXX)"
CONFIG_HOME="$(mktemp -d -t cowd-edge-webui-live-config-XXXXXX)"
HOME_DIR="$(mktemp -d -t cowd-edge-webui-live-home-XXXXXX)"
LOG="$WORKDIR/gateway.log"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORKDIR" "$CONFIG_HOME" "$HOME_DIR"
}
trap cleanup EXIT

for cmd in tmux ss curl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for live WebUI scenario" >&2
    exit 1
  fi
done

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

cd "$ROOT"
cargo build -p cli

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: false
gateway:
  enabled: true
  webui_dir: "$SURFACE_WEBUI_DIR/dist"
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

curl -fsS "$BASE_URL/readyz" | rg -q '"ready":true'

(
  cd "$SURFACE_WEBUI_DIR"
  env COWD_WEBUI_BASE_URL="$BASE_URL" \
    PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$CHROMIUM" \
    npx playwright test tasks-workbench.live.e2e.spec.js \
      --config=playwright.live.config.js \
      --browser=chromium
)

echo "live WebUI workbench scenario passed"
