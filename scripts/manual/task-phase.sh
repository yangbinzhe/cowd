#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT"

report="$(python3 scripts/test/cargo-selection.py report)"
python3 scripts/test/cargo-selection.py run --package gateway --lib \
  --filter task_api_records_phase_artifacts_and_review --nocapture --output "$report"

echo "task phase metadata/API checks passed"
