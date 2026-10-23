#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ "$#" == 0 ]]; then
  set -- plan
fi
exec python3 scripts/test/cargo-selection.py "$@"
