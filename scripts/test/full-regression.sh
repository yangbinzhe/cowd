#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo "[full-regression] cargo fmt --all --check"
cargo fmt --all --check

echo "[full-regression] cargo check --workspace --all-targets"
cargo check --workspace --all-targets

echo "[full-regression] cargo test --workspace --all-targets -- --test-threads=1"
cargo test --workspace --all-targets -- --test-threads=1

