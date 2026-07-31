#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

cargo test -p tools \
  search::tests::live_no_key_sources_cover_code_research_and_knowledge \
  -- --ignored --exact --nocapture
