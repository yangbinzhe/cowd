#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if rg -n 'lifecycle_owner\s*:' crates/runtime/src crates/harness-eval/src --glob '*.rs'; then
  echo "legacy lifecycle_owner field remains" >&2
  exit 1
else
  status=$?
  if [[ "$status" -ne 1 ]]; then
    echo "ERROR scanning legacy lifecycle_owner field (rg exit $status)" >&2
    exit 1
  fi
fi

cargo test -p runtime --test runtime_capability_authority
cargo xtask architecture duplicate-authority --check
