#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09111_PORT:-18731}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09111-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09111-iacc.XXXXXX")"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
AUTH_TOKEN="v09111-governance-token"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.111 IACC production governance scenario" >&2
  exit 1
fi

if [[ ! -x "$BIN" ]]; then
  echo "cowd binary not found at $BIN; build it first or set COWD_BIN" >&2
  exit 1
fi

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
ln -s "$ROOT/webui" "$WORKDIR/webui"
ln -s "$ROOT/docs" "$WORKDIR/docs"

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
        token: "$AUTH_TOKEN"
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..100}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

auth_header=(-H "Authorization: Bearer $AUTH_TOKEN")

health_json="$(curl -fsS "${auth_header[@]}" "$BASE_URL/api/iacc/health")"
printf '%s' "$health_json" | rg -q '"production_governance_bundle"'

governance_json="$(curl -fsS "${auth_header[@]}" "$BASE_URL/api/iacc/production/governance")"
printf '%s' "$governance_json" | rg -q '"kind":"iacc.production_governance"'
printf '%s' "$governance_json" | rg -q '"status":"ready"'
printf '%s' "$governance_json" | rg -q '"audit_export_surface":true'
printf '%s' "$governance_json" | rg -q '"cross_plane_audit_surface":true'
printf '%s' "$governance_json" | rg -q '"runbook_present":true'
printf '%s' "$governance_json" | rg -q '"health_capability_present":true'

test -f "$ROOT/docs/operator/iacc-production-runbook.md"

printf '\nIACC v0.9.111 production governance gate passed.\n'
