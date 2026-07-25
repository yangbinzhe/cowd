#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

cargo test -p harness-contract -p harness-eval --all-targets
cargo test -p runtime --lib
cargo test -p tools --test ai_harness_tool_closure
scripts/architecture/check-boundaries.sh
