#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

MODE="${1:-baseline}"
OUTPUT="${2:-test-reports/performance-v0.9.711/$MODE.json}"
case "$MODE" in
  baseline|candidate) ;;
  *) echo "usage: $0 <baseline|candidate> [output.json]" >&2; exit 2 ;;
esac

export CARGO_INCREMENTAL=0
export RUST_BACKTRACE=0
cargo test -p gateway active_session_register_remove_microbench_gate \
  -- --ignored --nocapture --test-threads=1
python3 tests/performance-v0.9.711/compare.py run \
  --manifest tests/performance-v0.9.711/manifest.yaml \
  --mode "$MODE" \
  --output "$OUTPUT"
