#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo "[quick] cargo fmt --all --check"
cargo fmt --all --check

echo "[quick] cargo check --workspace --all-targets"
cargo check --workspace --all-targets

echo "[quick] architecture boundary gates"
cargo test -p gateway --test gateway_runtimehost_architecture
cargo test -p runtime --test runtime_module_architecture
cargo test -p memory --test memory_module_architecture

echo "[quick] focused small boundary crates"
cargo test -p harness-contract --all-targets
cargo test -p model-protocol --all-targets
cargo test -p surface --all-targets
cargo test -p matrix-core --all-targets

