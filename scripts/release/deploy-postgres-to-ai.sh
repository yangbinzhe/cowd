#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/release/cowd}"
AI_ROOT="${COWD_AI_ROOT:-$HOME/AI}"
INSTALL_DIR="${COWD_INSTALL_DIR:-$AI_ROOT}"
INSTALLED_BIN="$INSTALL_DIR/cowd"

if [[ ! -x "$BIN" ]]; then
  echo "missing release candidate: $BIN" >&2
  exit 1
fi

candidate_version="$("$BIN" --version | awk '$1 == "Version" {print $2; exit}')"
workspace_version="$(awk -F '"' '/^version = / {print $2; exit}' "$ROOT/Cargo.toml")"
if [[ -z "$candidate_version" || "$candidate_version" != "$workspace_version" ]]; then
  echo "release candidate version mismatch: expected $workspace_version, got ${candidate_version:-unknown}" >&2
  exit 1
fi

if [[ -x "$INSTALLED_BIN" ]]; then
  "$INSTALLED_BIN" gateway stop >/dev/null 2>&1 || true
fi

COWD_BIN="$BIN" \
  COWD_AI_ROOT="$AI_ROOT" \
  COWD_INSTALL_DIR="$INSTALL_DIR" \
  "$ROOT/scripts/release/install-debug-to-ai.sh" --print-path-only >/dev/null

"$INSTALLED_BIN" storage upgrade
"$INSTALLED_BIN" gateway start
"$INSTALLED_BIN" gateway status
"$INSTALLED_BIN" gateway doctor

echo "PostgreSQL release deployment completed: $workspace_version"
