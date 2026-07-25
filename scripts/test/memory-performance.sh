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
  cargo test --release -p memory --test performance_bench "$test_name" \
    -- --ignored --nocapture --test-threads=1
done
