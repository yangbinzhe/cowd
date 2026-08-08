#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

failures=0
fail() {
  printf 'test governance: %s\n' "$*" >&2
  failures=$((failures + 1))
}

version="$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)"
inventory_version="$(sed -n 's/^release_version: //p' tests/test-governance/test-inventory.yaml)"
[[ "$inventory_version" == "$version" ]] \
  || fail "inventory version $inventory_version does not match workspace $version"

if rg -n 'delete-candidate|planned[_-]v?[0-9]|planned_change|Compatibility aliases|gateway-slow|unit-fast' \
  tests/test-governance scripts/validate.sh scripts/test scripts/ci .github \
  --glob '!governance-gate.sh'; then
  fail "retired test states or aliases remain"
fi

if rg -n '/api/commands(?:/execute)?|crates/(runtime|meta)/src/platform/feishu' \
  tests/interactive --glob '*.rs' --glob '*.md'; then
  fail "interactive suite still references removed APIs or source paths"
fi

ignored_lines="$(rg -n '#\[ignore' crates --glob '*.rs' || true)"
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  case "$line" in
    *gateway-global-env.sh*|*COWD_AI_HARNESS_LIVE=1*|*COWD_TEST_POSTGRES_URL*|*memory-performance.sh*|*runtime-projection-performance.sh*|*lark-live.sh*|*public-search-live.sh*) ;;
    *) fail "ignored test has no canonical runner classification: $line" ;;
  esac
done <<<"$ignored_lines"

while IFS=: read -r file line_number marker; do
  [[ -n "$file" && -n "$line_number" ]] || continue
  test_name="$(
    sed -n "$((line_number + 1)),$((line_number + 3))p" "$file" \
      | sed -nE 's/^[[:space:]]*(async[[:space:]]+)?fn[[:space:]]+([A-Za-z0-9_]+).*/\2/p' \
      | head -1
  )"
  case "$marker" in
    *gateway-global-env.sh*) runner="scripts/test/gateway-global-env.sh" ;;
    *COWD_AI_HARNESS_LIVE=1*) runner="scripts/ci/ai-harness-live-provider.sh" ;;
    *COWD_TEST_POSTGRES_URL*) runner="scripts/test/postgres-contract.sh" ;;
    *memory-performance.sh*) runner="scripts/test/memory-performance.sh" ;;
    *runtime-projection-performance.sh*) runner="scripts/test/runtime-projection-performance.sh" ;;
    *lark-live.sh*) runner="scripts/test/lark-live.sh" ;;
    *public-search-live.sh*) runner="scripts/test/public-search-live.sh" ;;
    *) continue ;;
  esac
  if [[ -z "$test_name" ]] || ! rg -q "\\b${test_name}\\b" "$runner"; then
    fail "ignored test is not named by canonical runner: $file:$line_number ${test_name:-<unknown>}"
  fi
done <<<"$ignored_lines"

[[ ! -e crates/gateway/tests/gateway_route_source_architecture.rs ]] \
  || fail "retired route source-shape test returned"
[[ ! -e crates/gateway/tests/gateway_runtimehost_architecture.rs ]] \
  || fail "retired heavy Gateway architecture integration test returned"
[[ ! -e crates/memory/tests/memory_module_architecture.rs ]] \
  || fail "retired memory source-layout test returned"
[[ ! -e crates/memory/tests/memory_dependency_boundary.rs ]] \
  || fail "retired manifest-text memory dependency test returned"
[[ ! -e crates/runtime/tests/structured_mfg_boundary.rs ]] \
  || fail "duplicated MFG source-shape boundary test returned"
[[ ! -e crates/tui/src/app_core/action_coverage.rs ]] \
  || fail "retired TUI source-shape action inventory returned"
[[ ! -e scripts/test/gateway-slow.sh ]] \
  || fail "retired Gateway slow alias returned"
for retired in \
  crates/memory/tests/agent_scope_test.rs \
  crates/memory/tests/conflict_test.rs \
  crates/memory/tests/persistence_test.rs \
  crates/memory/tests/token_benchmark_test.rs
do
  [[ ! -e "$retired" ]] || fail "retired weak or duplicated Memory test returned: $retired"
done
for retired in \
  scripts/ci/core.sh \
  scripts/ci/full.sh \
  scripts/ci/release.sh \
  scripts/ci/scenario.sh \
  scripts/ci/segmented.sh \
  scripts/ci/serial-global.sh \
  scripts/scenarios/runtime-execution-core.sh \
  scripts/scenarios/session-lifecycle.sh \
  scripts/v9-terminal-gate.sh \
  scripts/v9-performance-gate.sh \
  scripts/v9-fake-openai-provider.mjs \
  scripts/fixtures/v9-performance-config.yaml
do
  [[ ! -e "$retired" ]] || fail "retired monolithic V9 gate returned: $retired"
done

if rg -n 'RED Tests|SHOULD FAIL|This fails because|not yet wired|当前.*FAIL' \
  crates tests --glob '*.rs'; then
  fail "stale development-phase assertions remain in active tests"
fi

if rg -n 'let (_|_has_field) = ctx\.code_context|simulated_context.*Relevant Code Symbols' \
  crates tests --glob '*.rs'; then
  fail "non-asserting code-context probe or hard-coded benchmark returned"
fi

if rg -n 'skipping real PostgreSQL|PostgreSQL .* skipped|real_postgres_[A-Za-z0-9_]*when_configured' \
  crates --glob '*.rs'; then
  fail "real PostgreSQL tests must be explicit ignored contracts, never silent passes"
fi

if rg -U -n '#\[(?:tokio::)?test[^\]]*\][\s\S]{0,400}?if std::env::var_os\([^\)]*\)\.is_none\(\) \{\s*return;' \
  crates --glob '*.rs'; then
  fail "environment-dependent live tests must be explicit ignored contracts, never silent passes"
fi

if rg -U -n '#\[test\][\s\S]{0,300}?if !bwrap_available\(\) \{\s*return;' \
  crates --glob '*.rs'; then
  fail "kernel sandbox tests must run on Linux or be explicitly classified, never silently pass"
fi

if rg -U -n '#\[test\][\s\S]{0,400}?if !?(?:has_upstream_fixture|paths\.[A-Za-z_]+\(\)\.is_file)\([^\)]*\)? \{\s*return;' \
  crates --glob '*.rs'; then
  fail "external source fixtures must be explicit manual contracts, never silent unit-test passes"
fi

if rg -n -- '--v56[6-9]|--v57[0-2]' scripts/architecture/check-boundaries.sh; then
  fail "historical storage transition modes returned"
fi

if rg -n 'include_str!\(\"tui\.rs\"\)' tests/interactive/src/tui.rs; then
  fail "interactive TUI tests inspect their own source"
fi

if rg -n 'include_str!\(\"gateway_client\.rs\"\)|tools_executor_does_not_own_agent_lifecycle|tools_crate_does_not_own_runtime_control_plane_registries|tools_crate_does_not_depend_on_provider_directly|gateway_api_inventory_migrates_legacy_control_and_projection_methods|evolution_gateway_api_inventory_exposes_runtime_evolution_controls|managed_agent_gateway_api_inventory_exposes_runtime_owned_controls' \
  crates --glob '*.rs'; then
  fail "retired source-shape unit tests returned"
fi

if rg -n 'integration_diff_viewer_component_exists|integration_accessibility_labels|integration_catch_render_panic|integration_profiler_frame_skip|integration_animation_engine_tick_and_get|integration_config_migration_format|integration_high_contrast_wcag_audit|integration_spinner_rotation' \
  crates/tui/src/integration/tui_integration_tests.rs; then
  fail "retired duplicated or non-behavioral TUI integration tests returned"
fi

mapped_packages="$(
  scripts/test/changed-crates.sh --packages-for \
    crates/gateway/src/main.rs \
    crates/matrix/core/src/lib.rs \
    crates/skill/service/src/lib.rs \
    apps/mfg/source.lock.toml
)"
for package in cowd-product-apps gateway matrix-core skill; do
  if ! grep -qx "$package" <<<"$mapped_packages"; then
    fail "changed-crates metadata mapping omitted $package"
  fi
done

if [[ "$failures" -ne 0 ]]; then
  exit 1
fi
echo "test governance gate passed"
