#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT"

cargo test -p gateway task_kernel_records_phase_artifacts_and_review -- --nocapture
cargo test -p gateway task_api_records_phase_artifacts_and_review -- --nocapture

echo "task phase scenario passed"
