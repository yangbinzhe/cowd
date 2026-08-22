#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

TEST_TMP_ROOT="${COWD_TEST_TMP_ROOT:-/tmp/cowd-test-$$}"
mkdir -p "$TEST_TMP_ROOT"
export TMPDIR="$TEST_TMP_ROOT"

cleanup() {
  # Bundle and sandbox tests deliberately make fixture trees read-only.  They
  # remain owned by this test process, so restore owner write bits before
  # removal; otherwise a successful regression leaves an ever-growing /tmp
  # tree and can make the enclosing EXIT trap report a false failure.
  chmod -R u+rwX "$TEST_TMP_ROOT" 2>/dev/null || true
  rm -rf "$TEST_TMP_ROOT"
}
trap cleanup EXIT INT TERM

echo "[full-regression] cargo fmt --all --check"
cargo fmt --all --check

echo "[full-regression] build the verified Cowd sandbox launcher"
cargo build -p cli --features full -p managed-worker-launcher
export COWD_SANDBOX_LAUNCHER_BINARY="${CARGO_TARGET_DIR:-$ROOT/target}/debug/cowd"

echo "[full-regression] cargo test --workspace --all-targets"
cargo test --workspace --all-targets

echo "[full-regression] isolated process-global Gateway tests"
"$ROOT/scripts/test/gateway-global-env.sh"

echo "[full-regression] standalone reference Bundle and generic APP proxy"
"$ROOT/scripts/test/reference-app.sh"
