#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_GATEWAY_WEBUI_CONTRACT_PORT:-18690}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-gateway-webui-contract-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-gateway-webui-contract.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
FAILED=0
API_TOKEN="gateway-webui-contract-$$_credential"
AUTH_BROKER_BIN="${COWD_AUTH_BROKER_BIN:-$TARGET_ROOT/debug/cowd-auth-broker}"

# Every request, including public probes, carries the temporary scenario
# credential. Protected routes therefore exercise Gateway's production
# fail-closed authentication path rather than a test-only bypass.
curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_GATEWAY_WEBUI_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving gateway webui contract temp dir: $TMP_DIR" >&2
    return
  fi
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

on_error() {
  local status=$?
  FAILED=1
  echo "gateway webui contract scenario failed with status $status" >&2
  echo "----- temp dir -----" >&2
  echo "$TMP_DIR" >&2
  echo "----- gateway log -----" >&2
  sed -n '1,260p' "$LOG" >&2 || true
  echo "----- healthz -----" >&2
  curl -sS "$BASE_URL/healthz" >&2 || true
  echo >&2
  echo "----- readyz -----" >&2
  curl -sS "$BASE_URL/readyz" >&2 || true
  echo >&2
  echo "----- webui manifest -----" >&2
  curl -sS "$BASE_URL/api/webui/manifest" >&2 || true
  echo >&2
  exit "$status"
}
trap on_error ERR

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for gateway webui contract scenario" >&2
  exit 1
fi

if [[ ! -x "$AUTH_BROKER_BIN" ]]; then
  echo "cowd-auth-broker is required at $AUTH_BROKER_BIN" >&2
  exit 1
fi

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
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
        enabled: true
        token: "$API_TOKEN"
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export COWD_AUTH_BROKER_BIN='$AUTH_BROKER_BIN' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..80}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -fsS "$BASE_URL/healthz" | rg -q '"gateway":"gateway-runtime-host"'
curl -fsS "$BASE_URL/readyz" | rg -q '"ready":true'
curl -fsS "$BASE_URL/api/webui/manifest" | rg -q '"kind":"cowd.webui.manifest"'
curl -fsS "$BASE_URL/readyz" | rg -q '"ready":true'
curl -sS "$BASE_URL/manifest.json" | rg -q '"error":"webui_not_configured"'

if [[ ! -f "$LOG" ]]; then
  echo "gateway log file was not created" >&2
  sed -n '1,200p' "$LOG" >&2 || true
  exit 1
fi
