#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

fail=0

check_empty() {
  local name="$1"
  shift
  local output
  set +e
  output="$("$@" 2>&1)"
  local status=$?
  set -e
  if [[ "$status" -eq 0 && -n "$output" ]]; then
    echo "FAIL $name"
    echo "$output"
    fail=1
  elif [[ "$status" -gt 1 ]]; then
    echo "ERROR $name"
    echo "$output"
    fail=1
  else
    echo "PASS $name"
  fi
}

check_empty "cli business command names" \
  rg -n "\\b(run|chat|prompt|mcp serve)\\b" crates/cli/src/main.rs --glob '*.rs'

check_empty "cli business modules" \
  rg -n "auth|session|memory|matrix|mfg|agent|daemon" crates/cli/src/main.rs --glob '*.rs'

check_empty "daemon business management" \
  rg -n "daemon status|daemon start$|daemon stop|daemon restart|daemon_client|UnixStream|socket business" crates --glob '*.rs' --glob '!**/tests/**'

check_empty "tui direct business dependencies" \
  rg -n "(^|[^[:alnum:]_:])runtime::|use runtime::|use app_mfg::|app_mfg::|use matrix_core::|matrix_core::|use matrix_repository::|matrix_repository::|use storage::|storage::|rusqlite|use tools::|tools::|use memory::|memory::|use command_contract::|command_contract::|use command_service::|command_service::" crates/tui/src --glob '*.rs' --glob '!lib.rs' --glob '!boundary_policy.rs'

check_empty "runtime entrypoint reverse dependencies" \
  rg -n "gateway::|tui::|cli::|app_mfg::" crates/runtime/src --glob '*.rs'

check_empty "compat harness workspace residue" \
  rg -n "compat-harness|compat_harness" Cargo.toml crates/*/Cargo.toml crates --glob '*.rs' --glob '!**/tests/**'

check_empty "api crate naming residue" \
  rg -n "crates/api|api::" crates --glob '*.rs' --glob 'Cargo.toml'

check_empty "AI bin install residue" \
  rg -n "~/AI/bin|AI/bin" README.md docs scripts crates --glob '*.rs' --glob '*.md' --glob '*.sh' --glob 'Cargo.toml' --glob '!scripts/architecture/check-boundaries.sh'

echo "Checking cargo entrypoint dependency summaries"
cargo tree -p cli --depth 1 --no-default-features

if cargo tree -p tui --no-default-features | rg "(^|[ ├└│─])((runtime|matrix-core|matrix-repository|storage|tools|command-contract|command-service) v|memory|app-mfg|app_mfg|rusqlite)"; then
  echo "FAIL tui forbidden dependency tree"
  fail=1
else
  echo "PASS tui forbidden dependency tree"
fi

if cargo tree -p app-mfg --no-default-features | rg "(^|[ ├└│─])(runtime v|gateway v|gateway =)"; then
  echo "FAIL app-mfg forbidden dependency tree"
  fail=1
else
  echo "PASS app-mfg forbidden dependency tree"
fi

if cargo tree -p matrix-core --no-default-features | rg "(^|[ ├└│─])((runtime|gateway|app-mfg) v|app_mfg|mfg)"; then
  echo "FAIL matrix forbidden dependency tree"
  fail=1
else
  echo "PASS matrix forbidden dependency tree"
fi

exit "$fail"
