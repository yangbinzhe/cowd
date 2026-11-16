#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

cargo test -p harness-contract -p harness-eval --all-targets
# Runtime's PostgreSQL-backed unit contracts create several pools per test.
# Keep this lane below the shared fixture's connection ceiling while retaining
# concurrent coverage for the pure in-process contracts.
runtime_test_threads="${COWD_AI_HARNESS_TEST_THREADS:-4}"
cargo test -p runtime --lib -- --test-threads="$runtime_test_threads"
cargo test -p tools --test ai_harness_tool_closure
scripts/architecture/check-boundaries.sh
