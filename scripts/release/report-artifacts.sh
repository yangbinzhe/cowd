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
  if [[ ! -e "$1" ]]; then
    printf '0\n'
    return
  fi
  local value
  value="$(du -sb -- "$1" 2>/dev/null | awk 'NR == 1 {print $1}')"
  printf '%s\n' "${value:-0}"
}

human_bytes() {
  numfmt --to=iec-i --suffix=B "$1" 2>/dev/null || printf '%s bytes\n' "$1"
}

OWNED_PATHS=(
  "$INSTALL_DIR/cowd"
  "$INSTALL_DIR/install.json"
  "$INSTALL_DIR/docs"
)
TOTAL_BYTES=0
for path in "${OWNED_PATHS[@]}"; do
  TOTAL_BYTES=$((TOTAL_BYTES + $(size_bytes "$path")))
done
BINARY_BYTES="$(size_bytes "$INSTALL_DIR/cowd")"
DOCS_BYTES="$(size_bytes "$INSTALL_DIR/docs")"

{
  echo "# Cowd Release Artifact Report"
  echo
  echo "- install dir: \`$INSTALL_DIR\`"
  echo "- generated at: \`$(date -Iseconds)\`"
  echo "- owned total size: \`$(human_bytes "$TOTAL_BYTES")\` (\`$TOTAL_BYTES\` bytes)"
  echo "- binary size: \`$(human_bytes "$BINARY_BYTES")\` (\`$BINARY_BYTES\` bytes)"
  echo "- process model: \`single_binary_multi_process\`"
  echo "- docs size: \`$(human_bytes "$DOCS_BYTES")\` (\`$DOCS_BYTES\` bytes)"
  echo "- webui: external; configure \`gateway.webui_dir\` to serve a built cowd-edge/surfaces/webui dist"
  echo
  echo "## Owned Paths"
  echo
  echo '```text'
  {
    for path in "${OWNED_PATHS[@]}"; do
      [[ -e "$path" ]] && du -h -d 2 -- "$path" 2>/dev/null || true
    done
  } | sort -hr | sed -n '1,40p'
  echo '```'
  echo
  echo "## Largest Files"
  echo
  echo '```text'
  {
    for path in "${OWNED_PATHS[@]}"; do
      if [[ -f "$path" ]]; then
        printf '%s\t%s\n' "$(stat -c '%s' -- "$path")" "$path"
      elif [[ -d "$path" ]]; then
        find "$path" -type f -printf '%s\t%p\n' 2>/dev/null || true
      fi
    done
  } | sort -nr | sed -n '1,40p' | awk -F '\t' '{printf "%.2f MiB\t%s\n", $1/1048576, $2}'
  echo '```'
} >"$OUT"

echo "artifact report: $OUT"
