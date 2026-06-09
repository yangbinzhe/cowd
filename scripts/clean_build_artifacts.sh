#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: scripts/clean_build_artifacts.sh [--repo-target] [--tmp]

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
  find /tmp -maxdepth 1 \( \
    -name 'cowd-target-*' -o \
    -name 'cowd-fix-target' -o \
    -name 'cowd-validation-*' -o \
    -name 'cowd-api-*' -o \
    -name 'cowd-cli-*' -o \
    -name 'cowd-connector-*' -o \
    -name 'cowd-context-*' -o \
    -name 'cowd-feishu-*' -o \
    -name 'cowd-l4-*' -o \
    -name 'cowd-memory-*' -o \
    -name 'cowd-memory-panel-*' -o \
    -name 'cowd-native-*' -o \
    -name 'cowd-output-format-*' -o \
    -name 'cowd-resource-*' -o \
    -name 'cowd-resume-*' -o \
    -name 'cowd-sqlite-*' -o \
    -name 'cowd-state-*' -o \
    -name 'cowd-status-*' -o \
    -name 'cowd-storage-*' -o \
    -name 'cowd-tui-*' -o \
    -name 'cowd-webui-*' -o \
    -name 'cowd_*' \
  \) -exec rm -rf {} + 2>/dev/null || true
fi

if [[ "$CLEAN_REPO_TARGET" -eq 1 ]]; then
  rm -rf "$ROOT/target"
fi

echo "cleaned cowd build artifacts"
