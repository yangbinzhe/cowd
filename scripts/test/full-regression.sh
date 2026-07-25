#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

TEST_TMP_ROOT="${COWD_TEST_TMP_ROOT:-/tmp/cowd-test-$$}"
mkdir -p "$TEST_TMP_ROOT"
export TMPDIR="$TEST_TMP_ROOT"

cleanup() {
  rm -rf "$TEST_TMP_ROOT"
}
trap cleanup EXIT INT TERM

echo "[full-regression] cargo fmt --all --check"
cargo fmt --all --check

echo "[full-regression] cargo test --workspace --all-targets"
cargo test --workspace --all-targets

echo "[full-regression] isolated process-global Gateway tests"
"$ROOT/scripts/test/gateway-global-env.sh"
