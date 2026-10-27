#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "--packages-for" ]]; then
  shift
  exec python3 scripts/test/cargo-selection.py owners "$@"
fi
if [[ "${1:-}" == "--dry-run" ]]; then
  exec python3 scripts/test/cargo-selection.py plan --base "${2:-${COWD_CHANGED_BASE:-HEAD}}"
fi
base="${1:-${COWD_CHANGED_BASE:-HEAD}}"
report="${COWD_TEST_SELECTION_REPORT:-$(python3 scripts/test/cargo-selection.py report)}"
exec python3 scripts/test/cargo-selection.py run --base "$base" --output "$report"
