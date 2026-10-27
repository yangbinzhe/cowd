#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
report="${COWD_TEST_SELECTION_REPORT:-$(python3 scripts/test/cargo-selection.py report)}"

run_development_diagnostic() {
  local script="$1"
  local status
  if bash "$script"; then
    echo "[quick] development diagnostic PASS: $script (not test acceptance)" >&2
  else
    status=$?
    echo "[quick] development diagnostic FAIL: $script exit=$status (advisory; continuing tests)" >&2
  fi
}

if [[ "${COWD_QUICK_DEVELOPMENT_CHECKS:-0}" == "1" ]]; then
  run_development_diagnostic scripts/architecture/check-boundaries.sh
  run_development_diagnostic scripts/test/governance-gate.sh
else
  echo "[quick] development diagnostics SKIP: architecture/governance not executed; set COWD_QUICK_DEVELOPMENT_CHECKS=1 to observe them" >&2
fi

echo "[quick] discover and test small boundary targets with all features"
exec python3 scripts/test/cargo-selection.py run --quick --output "$report"
