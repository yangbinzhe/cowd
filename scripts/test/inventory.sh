#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

count_matches() {
  local pattern="$1"
  shift
  rg -n "$pattern" "$@" --glob '*.rs' 2>/dev/null | wc -l | tr -d ' '
}

workspace_tests="$(count_matches '#\[(tokio::)?test\]' crates)"
gateway_tests="$(count_matches '#\[(tokio::)?test\]' crates/gateway)"
ignored_total="$(count_matches '#\[ignore' crates)"
gateway_serial="$(rg -n '#\[ignore = "serial global env/provider test; run scripts/test/gateway-global-env.sh"\]' crates/gateway --glob '*.rs' | wc -l | tr -d ' ')"
provider_live="$(rg -n '#\[ignore = "requires COWD_AI_HARNESS_LIVE=1' crates/provider --glob '*.rs' | wc -l | tr -d ' ')"
postgres_live="$(rg -n '#\[ignore = "requires (an )?isolated COWD_TEST_POSTGRES' crates --glob '*.rs' | wc -l | tr -d ' ')"
memory_performance="$(rg -n '#\[ignore = "run scripts/test/memory-performance.sh' crates/memory --glob '*.rs' | wc -l | tr -d ' ')"
lark_live="$(rg -n '#\[ignore = "run scripts/test/lark-live.sh' crates --glob '*.rs' | wc -l | tr -d ' ')"
public_search_live="$(rg -n '#\[ignore = "run scripts/test/public-search-live.sh' crates --glob '*.rs' | wc -l | tr -d ' ')"
interactive_modules="$(find tests/interactive/src/scenarios -maxdepth 1 -type f -name '*.rs' ! -name mod.rs | wc -l | tr -d ' ')"

cat <<EOF
workspace_rust_test_attributes=$workspace_tests
gateway_rust_test_attributes=$gateway_tests
ignored_total=$ignored_total
ignored_gateway_serial_global=$gateway_serial
ignored_provider_live=$provider_live
ignored_postgres_contract=$postgres_live
ignored_memory_performance=$memory_performance
ignored_lark_live=$lark_live
ignored_public_search_live=$public_search_live
interactive_manual_modules=$interactive_modules
EOF
