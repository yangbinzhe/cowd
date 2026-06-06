#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/cowd-context-runtime-lean-spike-target}"

cd "$ROOT"

cargo test -p runtime stable_head_comparison -- --nocapture
cargo test -p runtime lean_probe -- --nocapture
cargo check -p cowd-cli

echo "context runtime lean spike passed"
