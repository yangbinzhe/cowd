#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/media/yi/Datas/workspace/.cowd-build/cowd-context-runtime-lean-spike-target}"

cd "$ROOT"

report="$(python3 scripts/test/cargo-selection.py report)"
python3 scripts/test/cargo-selection.py run --package runtime --lib \
  --filter stable_head_comparison --nocapture --output "$report"
report="$(python3 scripts/test/cargo-selection.py report)"
python3 scripts/test/cargo-selection.py run --package runtime --lib \
  --filter lean_probe --nocapture --output "$report"
cargo check -p gateway

echo "context runtime lean spike passed"
