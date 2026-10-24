#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT"

report="$(python3 scripts/test/cargo-selection.py report)"
python3 scripts/test/cargo-selection.py run --package gateway --lib \
  --filter memory_without_config_returns_disabled --nocapture --output "$report"

echo "memory degraded scenario passed"
