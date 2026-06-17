#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

cargo test -p cowd-cli memory_without_config_returns_disabled -- --nocapture

echo "memory degraded scenario passed"
