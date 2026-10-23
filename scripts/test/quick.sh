#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
report="${COWD_TEST_SELECTION_REPORT:-$(python3 scripts/test/cargo-selection.py report)}"

echo "[quick] cargo fmt --all --check"
cargo fmt --all --check

echo "[quick] static architecture and test-governance gates"
bash scripts/architecture/check-boundaries.sh
bash scripts/test/governance-gate.sh

echo "[quick] discover and test small boundary targets with all features"
exec python3 scripts/test/cargo-selection.py run --quick --output "$report"
