#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo "[quick] cargo fmt --all --check"
cargo fmt --all --check

echo "[quick] cargo check --workspace --all-targets"
cargo check --workspace --all-targets

echo "[quick] static architecture and test-governance gates"
bash scripts/architecture/check-boundaries.sh
bash scripts/test/governance-gate.sh

echo "[quick] focused small boundary crates"
cargo test -p harness-contract --all-targets
cargo test -p model-protocol --all-targets
cargo test -p surface --all-targets
cargo test -p matrix-core --all-targets
