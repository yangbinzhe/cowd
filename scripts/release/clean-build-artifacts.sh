#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${COWD_TMP_ROOT:-/tmp}"

usage() {
  cat <<'EOF'
Usage: scripts/release/clean-build-artifacts.sh [--repo-target] [--tmp]

Removes cowd build/test artifacts. By default it removes cowd-owned /tmp
targets and runtime temp files. Use --repo-target to also remove ./target.
EOF
}

CLEAN_REPO_TARGET=0
CLEAN_TMP=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-target)
      CLEAN_REPO_TARGET=1
      shift
      ;;
    --tmp)
      CLEAN_TMP=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$CLEAN_TMP" -eq 1 ]]; then
  find "$TMP_ROOT" -maxdepth 1 \( \
    -name 'cowd-*' -o \
    -name 'cowd_*' \
  \) -exec rm -rf {} + 2>/dev/null || true
fi

if [[ "$CLEAN_REPO_TARGET" -eq 1 ]]; then
  rm -rf "$ROOT/target"
fi

echo "cleaned cowd build artifacts"
