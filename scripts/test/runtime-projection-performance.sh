#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

tests=(
  paired_foreground_probe_with_and_without_projector_is_bounded
  paired_foreground_probe_during_projector_catchup_is_bounded
)

for test_name in "${tests[@]}"; do
  report="$(python3 scripts/test/cargo-selection.py report)"
  python3 scripts/test/cargo-selection.py run --package runtime --lib --release \
    --filter "$test_name" --ignored --entry scripts/test/runtime-projection-performance.sh \
    --nocapture --test-threads 1 --output "$report"
done
