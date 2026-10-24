#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

reject_matches() {
  local message="$1" status
  shift
  if rg "$@"; then
    echo "$message" >&2
    exit 1
  else
    status=$?
    if [[ "$status" -ne 1 ]]; then
      echo "ERROR scanning $message (rg exit $status)" >&2
      exit 1
    fi
  fi
}

reject_matches "legacy Gateway source parser remains" \
  'generate_route_registry|parse_routes|gateway_route_registry\.rs' \
  crates/gateway/build.rs crates/gateway/src/api_routes
reject_matches "literal Axum route registration remains" \
  '\.route\(\s*"' crates/gateway/src/api_routes --glob '*.rs' -U
for file in \
  crates/tui/src/gateway/gateway_client.rs \
  crates/tui/src/gateway/runner.rs \
  crates/tui/src/components/gateway_panel.rs; do
  if sed '/^#\[cfg(test)\]/,$d' "$file" | rg '"/api/'; then
    stage_status=("${PIPESTATUS[@]}")
  else
    stage_status=("${PIPESTATUS[@]}")
  fi
  if [[ "${stage_status[0]}" -ne 0 || "${stage_status[1]}" -gt 1 ]]; then
    echo "ERROR scanning production TUI API literals in $file (sed exit ${stage_status[0]}, rg exit ${stage_status[1]})" >&2
    exit 1
  fi
  if [[ "${stage_status[1]}" -eq 0 ]]; then
    echo "production TUI API literal remains in $file" >&2
    exit 1
  fi
done

cargo test -p surface gateway_api --lib
cargo test -p gateway --features test-support --test route_contract_parity
