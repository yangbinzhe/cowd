#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

cargo test --release -p runtime --lib \
  completion_pump_saturates_sixty_four_independent_work_items \
  -- --ignored --nocapture --test-threads=1
