#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

tests=(
  bench_recall_latency_1k_entries
  bench_get_entry_latency
  bench_prepare_context_cached_p95_under_300ms
)

for test_name in "${tests[@]}"; do
  report="$(python3 scripts/test/cargo-selection.py report)"
  python3 scripts/test/cargo-selection.py run --package memory --test performance_bench \
    --release --filter "$test_name" --ignored --entry scripts/test/memory-performance.sh \
    --nocapture --test-threads 1 --output "$report"
done
