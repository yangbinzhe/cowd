#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:-${CARGO_TARGET_DIR:-target}}"

if [[ ! -e "$TARGET_DIR" ]]; then
  echo "target dir not found: $TARGET_DIR"
  exit 0
fi

echo "Target: $TARGET_DIR"
echo
echo "Summary:"
du -sh "$TARGET_DIR" 2>/dev/null || true
echo
echo "Top directories:"
du -h -d 2 "$TARGET_DIR" 2>/dev/null | sort -h | tail -40 || true
echo
echo "Largest files:"
find "$TARGET_DIR" -type f -printf '%s %p\n' 2>/dev/null \
  | sort -nr \
  | head -40 \
  | numfmt --field=1 --to=iec || true

