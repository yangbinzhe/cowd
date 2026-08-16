#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
LAUNCHER_BIN="${COWD_LAUNCHER_BIN:-$TARGET_ROOT/debug/managed-worker-launcher}"
CONFIG_HOME="${COWD_CONFIG_HOME:-$HOME/.cowd}"
VERSION="$(awk -F '"' '/^version = / {print $2; exit}' "$ROOT/Cargo.toml")"
INSTALL_DIR="${COWD_INSTALL_DIR:-$CONFIG_HOME/bin}"
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

Installs the already-built debug cowd binary into ~/.cowd/bin.

Environment:
  CARGO_TARGET_DIR  target directory containing debug/cowd
  COWD_BIN          explicit cowd binary path
  COWD_LAUNCHER_BIN explicit managed-worker-launcher binary path
  COWD_CONFIG_HOME  Cowd home, default ~/.cowd
  COWD_INSTALL_DIR  explicit install directory, default ~/.cowd/bin
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
if [[ ! -x "$LAUNCHER_BIN" ]]; then
  echo "missing managed-worker-launcher binary at $LAUNCHER_BIN" >&2
  exit 1
fi
BIN_VERSION="$("$BIN" --version | awk '$1 == "Version" {print $2; exit}')"
if [[ "$BIN_VERSION" != "$VERSION" ]]; then
  echo "cowd binary version mismatch: expected $VERSION, got ${BIN_VERSION:-unknown}" >&2
  exit 1
fi
mkdir -p "$INSTALL_DIR" "$INSTALL_DIR/docs"
INSTALL_TMP="$(mktemp "$INSTALL_DIR/.cowd.install.XXXXXX")"
trap 'rm -f "$INSTALL_TMP"' EXIT
install -m 0755 "$BIN" "$INSTALL_TMP"
mv -f "$INSTALL_TMP" "$INSTALL_DIR/cowd"
trap - EXIT
LAUNCHER_TMP="$(mktemp "$INSTALL_DIR/.managed-worker-launcher.install.XXXXXX")"
trap 'rm -f "$LAUNCHER_TMP"' EXIT
install -m 0500 "$LAUNCHER_BIN" "$LAUNCHER_TMP"
mv -f "$LAUNCHER_TMP" "$INSTALL_DIR/managed-worker-launcher"
trap - EXIT
rm -f \
  "$INSTALL_DIR/cowd-auth-broker" \
  "$INSTALL_DIR/cowd-sandbox-launcher" \
  "$INSTALL_DIR"/.cowd-auth-broker.prev-* \
  "$INSTALL_DIR"/.cowd-sandbox-launcher.prev-*

cat >"$INSTALL_DIR/install.json" <<EOF
{
  "schema_version": 1,
  "install_root": "$INSTALL_DIR",
  "version": "$VERSION",
  "installed_at": "$(date -Iseconds)",
  "source_root": "$ROOT",
  "binary": "$INSTALL_DIR/cowd",
  "managed_worker_launcher": "$INSTALL_DIR/managed-worker-launcher",
  "process_model": "single_binary_multi_process"
}
EOF

if [[ "$UPDATE_CURRENT" == "1" ]]; then
  ln -sfn "$INSTALL_DIR/cowd" "$INSTALL_DIR/cowd-debug-current"
fi

if [[ "$PRINT_ONLY" == "1" ]]; then
  printf '%s\n' "$INSTALL_DIR"
else
  echo "installed cowd debug build: $INSTALL_DIR/cowd"
  echo "Edge, APP, and WebUI artifacts share this install root."
  echo "Run ../cowd-edge/install.sh to populate edge/, connectors/, and webui/."
  if [[ "$UPDATE_CURRENT" == "1" ]]; then
    echo "current symlink: $INSTALL_DIR/cowd-debug-current"
  fi
fi
