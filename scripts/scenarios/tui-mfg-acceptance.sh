#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

exec "$ROOT/scripts/scenarios/with-mfg-surface-lane.sh" backend -- \
  cargo run --manifest-path tests/interactive/Cargo.toml -- tui_mfg_operations
