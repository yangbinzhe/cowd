#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

tests=(
  paired_foreground_probe_with_and_without_projector_is_bounded
  paired_foreground_probe_during_projector_catchup_is_bounded
)

for test_name in "${tests[@]}"; do
  cargo test --release -p runtime --lib "$test_name" \
    -- --ignored --nocapture --test-threads=1
done
