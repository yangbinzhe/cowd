#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# Advisory only. Keep the full compiler-version-sensitive style debt visible
# without forcing behavior-changing mechanical rewrites into release patches.
exec cargo clippy --workspace --all-targets -- \
  -W clippy::all \
  -W clippy::pedantic
