#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

cargo test -p ai-strategy -p ai-context -p ai-growth -p ai-eval --all-targets
cargo test -p runtime --test ai_harness_e2e
cargo test -p memory memory_pulse
scripts/architecture/check-boundaries.sh
