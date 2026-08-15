#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_SKILL_SURFACE_PORT:-18756}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-skill-surface-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-skill-surface.XXXXXX")"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
API_TOKEN="skill-surface-$$_credential"

cleanup() {
  tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

on_error() {
  local status=$?
  echo "skill surface scenario failed with status $status" >&2
  sed -n '1,260p' "$LOG" >&2 || true
  exit "$status"
}
trap on_error ERR

command -v tmux >/dev/null 2>&1 || { echo "tmux is required" >&2; exit 1; }
[[ -x "$BIN" ]] || { echo "cowd binary not found at $BIN" >&2; exit 1; }
if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd/skills/release" "$CONFIG_HOME" "$HOME_DIR/.cowd"
cat >"$WORKDIR/.cowd/skills/release/SKILL.md" <<'EOF'
---
name: release
description: Prepare changelog and publish release tags
tags: [git, release]
related_skills: [test]
---
# Release
EOF

cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  scenario:
    base_url: "http://127.0.0.1:1"
    api_key: "skill-surface-provider-key"
    protocol: "completions"
    models: ["claude-sonnet-4-6"]
permissions:
  default_mode: "danger-full-access"
memory:
  enabled: false
gateway:
  enabled: true
  session_reset: "none"
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
  "bash -lc \"cd '$WORKDIR' && COWD_CONFIG_HOME='$CONFIG_HOME' HOME='$HOME_DIR' '$BIN' gateway run >'$LOG' 2>&1\""
for _ in {1..100}; do
  curl -fsS -H "Authorization: Bearer $API_TOKEN" "$BASE_URL/health" >/dev/null 2>&1 && break
  sleep 0.25
done

catalog="$(curl -fsS -H "Authorization: Bearer $API_TOKEN" "$BASE_URL/api/skills/catalog")"
rg -q '"kind":"skills.catalog"' <<<"$catalog"
rg -q '"id":"local:release"' <<<"$catalog"
projection="$(curl -fsS -H "Authorization: Bearer $API_TOKEN" "$BASE_URL/api/skills/projection?surface=webui&query=prepare%20git%20release%20changelog")"
rg -q '"kind":"skills.projection"' <<<"$projection"
rg -q '"name":"release"' <<<"$projection"
detail="$(curl -fsS -H "Authorization: Bearer $API_TOKEN" "$BASE_URL/api/skills/local:release")"
rg -q '"kind":"skills.detail"' <<<"$detail"

echo "skill surface scenario passed"
