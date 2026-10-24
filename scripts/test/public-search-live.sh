#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

report="$(python3 scripts/test/cargo-selection.py report)"
python3 scripts/test/cargo-selection.py run --package tools --lib \
  --filter search::tests::live_no_key_sources_cover_code_research_and_knowledge \
  --ignored --exact --entry scripts/test/public-search-live.sh --nocapture --output "$report"
