#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

report="$(python3 scripts/test/cargo-selection.py report)"
python3 scripts/test/cargo-selection.py run --package runtime --lib --release \
  --filter completion_pump_saturates_sixty_four_independent_work_items \
  --ignored --entry scripts/test/runtime-execution-performance.sh \
  --nocapture --test-threads 1 --output "$report"
