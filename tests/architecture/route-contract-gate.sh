#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if rg 'generate_route_registry|parse_routes|gateway_route_registry\.rs' \
  crates/gateway/build.rs crates/gateway/src/api_routes; then
  echo "legacy Gateway source parser remains" >&2
  exit 1
fi
if rg '\.route\(\s*"' crates/gateway/src/api_routes --glob '*.rs' -U; then
  echo "literal Axum route registration remains" >&2
  exit 1
fi
for file in \
  crates/tui/src/gateway/gateway_client.rs \
  crates/tui/src/gateway/runner.rs \
  crates/tui/src/components/gateway_panel.rs; do
  if sed '/^#\[cfg(test)\]/,$d' "$file" | rg '"/api/'; then
    echo "production TUI API literal remains in $file" >&2
    exit 1
  fi
done

cargo test -p surface gateway_api --lib
cargo test -p gateway --features test-support --test route_contract_parity
