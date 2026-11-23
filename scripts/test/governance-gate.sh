#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

failures=0
fail() {
  printf 'test governance: %s\n' "$*" >&2
  failures=$((failures + 1))
}

reject_matches() {
  local message="$1" status
  shift
  if rg "$@"; then
    fail "$message"
  else
    status=$?
    if [[ "$status" -ne 1 ]]; then
      fail "ERROR scanning $message (rg exit $status)"
    fi
  fi
}

require_match() {
  local message="$1" status
  shift
  # Read all inputs: -q can hide a later file's read error after a match.
  if rg "$@" > /dev/null; then
    :
  else
    status=$?
    if [[ "$status" -eq 1 ]]; then
      fail "$message"
    else
      fail "ERROR scanning $message (rg exit $status)"
    fi
  fi
}

read_metadata() {
  local name="$1" status
  shift
  if "$@"; then
    return 0
  else
    status=$?
    printf 'test governance: ERROR reading %s (exit %s)\n' "$name" "$status" >&2
    return 2
  fi
}

if version="$(read_metadata 'workspace version' sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' Cargo.toml)"; then
  version="${version%%$'\n'*}"
else
  exit 1
fi
if inventory_version="$(read_metadata 'inventory version' sed -n 's/^release_version: //p' tests/test-governance/test-inventory.yaml)"; then :; else exit 1; fi
if readme_version="$(read_metadata 'README version' sed -n 's/^当前治理版本：`\([^`]*\)`.*/\1/p' tests/test-governance/README.md)"; then :; else exit 1; fi
if release_authority="$(read_metadata 'release authority' sed -n 's/^release_authority: //p' tests/test-governance/test-inventory.yaml)"; then :; else exit 1; fi
if release_evidence="$(read_metadata 'release evidence' sed -n 's/^release_evidence: //p' tests/test-governance/test-inventory.yaml)"; then :; else exit 1; fi
[[ "$inventory_version" == "$version" ]] \
  || fail "inventory version $inventory_version does not match workspace $version"
[[ "$readme_version" == "$version" ]] \
  || fail "governance README version $readme_version does not match workspace $version"
[[ -f "$release_authority" ]] \
  || fail "release authority is missing: ${release_authority:-<unset>}"
[[ -f "$release_evidence" ]] \
  || fail "release evidence is missing: ${release_evidence:-<unset>}"
[[ "$release_authority" == *"v${version}.md" ]] \
  || fail "release authority does not identify workspace version $version: $release_authority"
[[ "$release_evidence" == *"v${version}.md" ]] \
  || fail "release evidence does not identify workspace version $version: $release_evidence"
if [[ -f "$release_evidence" ]]; then
  require_match "release evidence is not closed with 'Release status: passed'" \
    '^Release status: passed$' "$release_evidence"
fi

reject_matches "retired test states or aliases remain" \
  -n 'delete-candidate|planned[_-]v?[0-9]|planned_change|Compatibility aliases|gateway-slow|unit-fast' \
  tests/test-governance scripts/validate.sh scripts/test scripts/ci .github \
  --glob '!governance-gate.sh'

reject_matches "interactive suite still references removed APIs or source paths" \
  -n '/api/commands(?:/execute)?|crates/(runtime|meta)/src/platform/feishu' \
  tests/interactive --glob '*.rs' --glob '*.md'

if ignored_lines="$(rg -n '#\[ignore' crates --glob '*.rs')"; then
  :
else
  status=$?
  ignored_lines=''
  if [[ "$status" -ne 1 ]]; then
    fail "ERROR enumerating ignored tests (rg exit $status)"
  fi
fi
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  case "$line" in
    *gateway-global-env.sh*|*COWD_AI_HARNESS_LIVE=1*|*COWD_TEST_POSTGRES_URL*|*memory-performance.sh*|*runtime-projection-performance.sh*|*runtime-execution-performance.sh*|*tests/performance/run.sh*|*reference-app.sh*|*reference-app-performance.sh*|*lark-live.sh*|*public-search-live.sh*) ;;
    *) fail "ignored test has no canonical runner classification: $line" ;;
  esac
done <<<"$ignored_lines"

while IFS=: read -r file line_number marker; do
  [[ -n "$file" && -n "$line_number" ]] || continue
  if test_lines="$(sed -n "$((line_number + 1)),$((line_number + 3))p" "$file")"; then
    if test_name="$(sed -nE 's/^[[:space:]]*(async[[:space:]]+)?fn[[:space:]]+([A-Za-z0-9_]+).*/\2/p' <<<"$test_lines")"; then
      test_name="${test_name%%$'\n'*}"
    else
      fail "ERROR extracting ignored test name: $file:$line_number (sed exit $?)"
      continue
    fi
  else
    fail "ERROR reading ignored test: $file:$line_number (sed exit $?)"
    continue
  fi
  case "$marker" in
    *gateway-global-env.sh*) runner="scripts/test/gateway-global-env.sh" ;;
    *COWD_AI_HARNESS_LIVE=1*) runner="scripts/ci/ai-harness-live-provider.sh" ;;
    *COWD_TEST_POSTGRES_URL*) runner="scripts/test/postgres-contract.sh" ;;
    *memory-performance.sh*) runner="scripts/test/memory-performance.sh" ;;
    *runtime-projection-performance.sh*) runner="scripts/test/runtime-projection-performance.sh" ;;
    *runtime-execution-performance.sh*) runner="scripts/test/runtime-execution-performance.sh" ;;
    *tests/performance/run.sh*) runner="tests/performance/run.sh" ;;
    *reference-app.sh*) runner="scripts/test/reference-app.sh" ;;
    *reference-app-performance.sh*) runner="scripts/test/reference-app-performance.sh" ;;
    *lark-live.sh*) runner="scripts/test/lark-live.sh" ;;
    *public-search-live.sh*) runner="scripts/test/public-search-live.sh" ;;
    *) continue ;;
  esac
  if [[ -z "$test_name" ]]; then
    fail "ignored test is not named by canonical runner: $file:$line_number ${test_name:-<unknown>}"
  else
    require_match "ignored test is not named by canonical runner: $file:$line_number ${test_name:-<unknown>}" \
      "\\b${test_name}\\b" "$runner"
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

reject_matches "stale development-phase assertions remain in active tests" \
  -n 'RED Tests|SHOULD FAIL|This fails because|not yet wired|当前.*FAIL' \
  crates tests --glob '*.rs'

reject_matches "non-asserting code-context probe or hard-coded benchmark returned" \
  -n 'let (_|_has_field) = ctx\.code_context|simulated_context.*Relevant Code Symbols' \
  crates tests --glob '*.rs'

reject_matches "real PostgreSQL tests must be explicit ignored contracts, never silent passes" \
  -n 'skipping real PostgreSQL|PostgreSQL .* skipped|real_postgres_[A-Za-z0-9_]*when_configured' \
  crates --glob '*.rs'

reject_matches "environment-dependent live tests must be explicit ignored contracts, never silent passes" \
  -U -n '#\[(?:tokio::)?test[^\]]*\][\s\S]{0,400}?if std::env::var_os\([^\)]*\)\.is_none\(\) \{\s*return;' \
  crates --glob '*.rs'

reject_matches "kernel sandbox tests must run on Linux or be explicitly classified, never silently pass" \
  -U -n '#\[test\][\s\S]{0,300}?if !bwrap_available\(\) \{\s*return;' \
  crates --glob '*.rs'

reject_matches "external source fixtures must be explicit manual contracts, never silent unit-test passes" \
  -U -n '#\[test\][\s\S]{0,400}?if !?(?:has_upstream_fixture|paths\.[A-Za-z_]+\(\)\.is_file)\([^\)]*\)? \{\s*return;' \
  crates --glob '*.rs'

reject_matches "historical storage transition modes returned" \
  -n -- '--v56[6-9]|--v57[0-2]' scripts/architecture/check-boundaries.sh

reject_matches "interactive TUI tests inspect their own source" \
  -n 'include_str!\(\"tui\.rs\"\)' tests/interactive/src/tui.rs

reject_matches "retired source-shape unit tests returned" \
  -n 'include_str!\(\"gateway_client\.rs\"\)|tools_executor_does_not_own_agent_lifecycle|tools_crate_does_not_own_runtime_control_plane_registries|tools_crate_does_not_depend_on_provider_directly|gateway_api_inventory_migrates_legacy_control_and_projection_methods|evolution_gateway_api_inventory_exposes_runtime_evolution_controls|managed_agent_gateway_api_inventory_exposes_runtime_owned_controls' \
  crates --glob '*.rs'

reject_matches "retired duplicated or non-behavioral TUI integration tests returned" \
  -n 'integration_diff_viewer_component_exists|integration_accessibility_labels|integration_catch_render_panic|integration_profiler_frame_skip|integration_animation_engine_tick_and_get|integration_config_migration_format|integration_high_contrast_wcag_audit|integration_spinner_rotation' \
  crates/tui/src/integration/tui_integration_tests.rs

for retired in apps/catalog.toml apps/mfg/source.lock.toml crates/product-apps crates/app-sdk surfaces/webui/apps.generated.ts; do
  if [[ -e "$retired" ]]; then
    fail "retired static APP composition returned: $retired"
  fi
done

[[ -x scripts/test/reference-app.sh ]] || fail "reference Bundle regression runner is missing or not executable"
require_match "reference Bundle runner is not wired into validation" \
  'reference-app\.sh' .github/workflows/ci.yml scripts/test/full-regression.sh scripts/validate.sh

# Metadata discovery may invoke Cargo. It is admitted only after every static
# precondition above has completed successfully, including existence reads.
if [[ "$failures" -eq 0 ]]; then
  if mapped_packages="$(
    scripts/test/changed-crates.sh --packages-for \
      crates/gateway/src/main.rs \
      crates/matrix/core/src/lib.rs \
      crates/skill/service/src/lib.rs
  )"; then
    for package in gateway matrix-core skill; do
      if grep -qx "$package" <<<"$mapped_packages"; then
        :
      else
        status=$?
        if [[ "$status" -eq 1 ]]; then
          fail "changed-crates metadata mapping omitted $package"
        else
          fail "ERROR checking changed-crates metadata mapping for $package (grep exit $status)"
        fi
      fi
    done
  else
    fail "ERROR reading changed-crates metadata mapping (exit $?)"
  fi
fi

if [[ "$failures" -ne 0 ]]; then
  exit 1
fi
echo "test governance gate passed"
