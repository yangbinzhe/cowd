#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

count_tests() {
  cargo test "$@" -- --list 2>/dev/null \
    | awk 'BEGIN {tests=0} /: test$/ {tests++} END {print tests}'
}

workspace_tests="$(count_tests --workspace --no-default-features)"
gateway_tests="$(count_tests -p gateway --lib --no-default-features)"
gateway_ignored="$(rg -n '#\[ignore' crates/gateway/src/main.rs | wc -l | tr -d ' ')"
gateway_serial_global="$(
  rg -n 'serial global env/provider test; run scripts/test/gateway-global-env.sh' crates/gateway/src/main.rs \
    | wc -l \
    | tr -d ' '
)"

cat <<EOF
workspace_rust_test_entries=$workspace_tests
gateway_lib_test_entries=$gateway_tests
gateway_ignored_attributes=$gateway_ignored
gateway_serial_global_entries=$gateway_serial_global
EOF
