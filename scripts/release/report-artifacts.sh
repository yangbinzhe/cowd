#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="${1:-}"
OUT="${2:-}"

if [[ -z "$INSTALL_DIR" || ! -d "$INSTALL_DIR" ]]; then
  echo "usage: scripts/release/report-artifacts.sh <install-dir> [out.md]" >&2
  exit 2
fi

if [[ -z "$OUT" ]]; then
  OUT="$INSTALL_DIR/artifacts.md"
fi

mkdir -p "$(dirname "$OUT")"

size_bytes() {
  du -sb "$1" 2>/dev/null | awk '{print $1}' || echo 0
}

human_size() {
  du -sh "$1" 2>/dev/null | awk '{print $1}' || echo 0
}

{
  echo "# Cowd Release Artifact Report"
  echo
  echo "- install dir: \`$INSTALL_DIR\`"
  echo "- generated at: \`$(date -Iseconds)\`"
  echo "- total size: \`$(human_size "$INSTALL_DIR")\` (\`$(size_bytes "$INSTALL_DIR")\` bytes)"
  echo "- binary size: \`$(human_size "$INSTALL_DIR/bin/cowd")\` (\`$(size_bytes "$INSTALL_DIR/bin/cowd")\` bytes)"
  echo "- docs size: \`$(human_size "$INSTALL_DIR/docs")\` (\`$(size_bytes "$INSTALL_DIR/docs")\` bytes)"
  echo "- webui: external; configure \`gateway.webui_dir\` to serve a built cowd-webui dist"
  echo
  echo "## Top Directories"
  echo
  echo '```text'
  du -h -d 2 "$INSTALL_DIR" 2>/dev/null | sort -hr | sed -n '1,40p'
  echo '```'
  echo
  echo "## Largest Files"
  echo
  echo '```text'
  find "$INSTALL_DIR" -type f -printf '%s\t%p\n' 2>/dev/null | sort -nr | sed -n '1,40p' | awk '{printf "%.2f MiB\t%s\n", $1/1048576, $2}'
  echo '```'
} >"$OUT"

echo "artifact report: $OUT"
