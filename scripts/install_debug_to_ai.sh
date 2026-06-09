#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
AI_ROOT="${COWD_AI_ROOT:-$HOME/AI}"
VERSION="$(awk -F '"' '/^version = / {print $2; exit}' "$ROOT/Cargo.toml")"
STAMP="${COWD_INSTALL_STAMP:-$(date +%Y%m%d-%H%M%S)}"
INSTALL_DIR="${COWD_INSTALL_DIR:-$AI_ROOT/cowd-debug-$VERSION-$STAMP}"
UPDATE_CURRENT=0
PRINT_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --current)
      UPDATE_CURRENT=1
      shift
      ;;
    --print-path-only)
      PRINT_ONLY=1
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: scripts/install_debug_to_ai.sh [--current] [--print-path-only]

Installs the already-built debug cowd binary and static WebUI into ~/AI.

Environment:
  CARGO_TARGET_DIR  target directory containing debug/cowd
  COWD_BIN          explicit cowd binary path
  COWD_AI_ROOT      install root, default ~/AI
  COWD_INSTALL_DIR  explicit install directory
EOF
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      exit 2
      ;;
  esac
done

if [[ ! -x "$BIN" ]]; then
  echo "missing executable cowd binary at $BIN" >&2
  exit 1
fi

if [[ -e "$INSTALL_DIR" ]]; then
  echo "install directory already exists: $INSTALL_DIR" >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR/bin" "$INSTALL_DIR/webui" "$INSTALL_DIR/docs" "$AI_ROOT"
cp "$BIN" "$INSTALL_DIR/bin/cowd"
chmod +x "$INSTALL_DIR/bin/cowd"

if [[ -d "$ROOT/webui" ]]; then
  for item in \
    index.html \
    api.js \
    boot.js \
    commands.js \
    manifest.json \
    messages.js \
    panels.js \
    sessions.js \
    state.js \
    style.css \
    sw.js \
    ui.js \
    workspace.js \
    assets
  do
    if [[ -e "$ROOT/webui/$item" ]]; then
      cp -a "$ROOT/webui/$item" "$INSTALL_DIR/webui/"
    fi
  done
fi
if [[ -d "$ROOT/docs/plans" ]]; then
  cp -a "$ROOT/docs/plans" "$INSTALL_DIR/docs/"
fi

cat >"$INSTALL_DIR/install.json" <<EOF
{
  "version": "$VERSION",
  "installed_at": "$(date -Iseconds)",
  "source_root": "$ROOT",
  "binary": "$INSTALL_DIR/bin/cowd"
}
EOF

if [[ "$UPDATE_CURRENT" == "1" ]]; then
  ln -sfn "$INSTALL_DIR" "$AI_ROOT/cowd-debug-current"
fi

if [[ "$PRINT_ONLY" == "1" ]]; then
  printf '%s\n' "$INSTALL_DIR"
else
  echo "installed cowd debug build: $INSTALL_DIR"
  if [[ "$UPDATE_CURRENT" == "1" ]]; then
    echo "current symlink: $AI_ROOT/cowd-debug-current"
  fi
fi
