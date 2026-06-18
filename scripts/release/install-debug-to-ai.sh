#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
AI_ROOT="${COWD_AI_ROOT:-$HOME/AI}"
VERSION="$(awk -F '"' '/^version = / {print $2; exit}' "$ROOT/Cargo.toml")"
STAMP="${COWD_INSTALL_STAMP:-$(date +%Y%m%d-%H%M%S)}"
INSTALL_DIR="${COWD_INSTALL_DIR:-$AI_ROOT}"
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
Usage: scripts/release/install-debug-to-ai.sh [--current] [--print-path-only]

Installs the already-built debug cowd binary into ~/AI.

Environment:
  CARGO_TARGET_DIR  target directory containing debug/cowd
  COWD_BIN          explicit cowd binary path
  COWD_AI_ROOT      install root, default ~/AI
  COWD_INSTALL_DIR  explicit install directory, default ~/AI
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

mkdir -p "$INSTALL_DIR" "$INSTALL_DIR/docs" "$AI_ROOT"
cp "$BIN" "$INSTALL_DIR/cowd"
chmod +x "$INSTALL_DIR/cowd"

cat >"$INSTALL_DIR/install.json" <<EOF
{
  "version": "$VERSION",
  "installed_at": "$(date -Iseconds)",
  "source_root": "$ROOT",
  "binary": "$INSTALL_DIR/cowd"
}
EOF

if [[ "$UPDATE_CURRENT" == "1" ]]; then
  ln -sfn "$INSTALL_DIR/cowd" "$AI_ROOT/cowd-debug-current"
fi

if [[ "$PRINT_ONLY" == "1" ]]; then
  printf '%s\n' "$INSTALL_DIR"
else
  echo "installed cowd debug build: $INSTALL_DIR/cowd"
  echo "WebUI assets are external; configure gateway.webui_dir to enable browser UI."
  if [[ "$UPDATE_CURRENT" == "1" ]]; then
    echo "current symlink: $AI_ROOT/cowd-debug-current"
  fi
fi
