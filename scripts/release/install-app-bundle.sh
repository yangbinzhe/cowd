#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/release/install-app-bundle.sh BUNDLE_DIR [INSTALL_NAME]

Installs one already-built, signed Cowd APP bundle below the canonical user
installation root. The bundle remains an immutable directory; Runtime data is
stored separately below COWD_CONFIG_HOME.

Environment:
  COWD_CONFIG_HOME  Cowd home, default ~/.cowd
  COWD_INSTALL_DIR  canonical install root, default ~/.cowd/bin
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi
if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 2
fi

BUNDLE_DIR="$(cd "$1" && pwd)"
INSTALL_NAME="${2:-$(basename "$BUNDLE_DIR")}"
CONFIG_HOME="${COWD_CONFIG_HOME:-$HOME/.cowd}"
INSTALL_ROOT="${COWD_INSTALL_DIR:-$CONFIG_HOME/bin}"
APPS_ROOT="$INSTALL_ROOT/apps"

if [[ ! "$INSTALL_NAME" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "invalid APP install name: $INSTALL_NAME" >&2
  exit 2
fi
if [[ ! -f "$BUNDLE_DIR/app.json" || ! -d "$BUNDLE_DIR/bin" ]]; then
  echo "APP bundle must contain app.json and bin/: $BUNDLE_DIR" >&2
  exit 1
fi
if find "$BUNDLE_DIR" -type l -print -quit | grep -q .; then
  echo "APP bundle contains a symbolic link and cannot be installed" >&2
  exit 1
fi

mkdir -p "$APPS_ROOT"
STAGE="$(mktemp -d "$APPS_ROOT/.cowd-app.install.XXXXXX")"
BACKUP="$APPS_ROOT/.${INSTALL_NAME}.previous.$$"
cleanup() { rm -rf "$STAGE" "$BACKUP"; }
trap cleanup EXIT
cp -a "$BUNDLE_DIR/." "$STAGE/"

if [[ -e "$APPS_ROOT/$INSTALL_NAME" ]]; then
  mv "$APPS_ROOT/$INSTALL_NAME" "$BACKUP"
fi
mv "$STAGE" "$APPS_ROOT/$INSTALL_NAME"
rm -rf "$BACKUP"
trap - EXIT

printf 'installed APP bundle: %s\n' "$APPS_ROOT/$INSTALL_NAME"
