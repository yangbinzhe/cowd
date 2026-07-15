#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LANE="${1:-${COWD_VALIDATION_LANE:-contract}}"
MANUAL_TARGET="${2:-}"

case "$LANE" in
  quick) LANE="quick" ;;
  changed|changed-crates) LANE="changed-crates" ;;
  full-regression) LANE="full-regression" ;;
  unit-fast|fast) LANE="unit-fast" ;;
  contract|core) LANE="contract" ;;
  scenario|live) LANE="scenario" ;;
  serial-global|global|serial) LANE="serial-global" ;;
  surface) LANE="surface" ;;
  release) LANE="release" ;;
  all|full) LANE="all" ;;
  manual) LANE="manual" ;;
  -h|--help|help)
    cat <<'EOF'
Usage: scripts/validate.sh [quick|changed-crates|full-regression|unit-fast|contract|serial-global|scenario|surface|release|all|manual <name>]

Lanes:
  quick      current default edit gate: fmt, workspace check, architecture gates, small boundary crates
  changed-crates  precise touched-crate gate; base defaults to HEAD or COWD_CHANGED_BASE
  full-regression final Rust regression: fmt, workspace check, workspace all-target tests single-threaded
  unit-fast  edit feedback: fmt, light crates, targeted heavy-crate probes
  contract   package/API/CLI contracts without browser or tmux scenarios
  serial-global  tests that mutate process-global env/cwd/provider/session state
  scenario   5 golden paths: gateway, session, memory, tool, skill+mfg
  surface    3 surface control points: CLI minimal, TUI projection, WebUI gateway
  release    install artifact smoke; deep scenario checks stay in scenario/manual
  all        contract + serial-global + scenario
  manual     run one manual script from scripts/manual

Governance:
  tests/test-governance/test-inventory.yaml classifies default, manual,
  nightly, and delete-candidate checks. Interactive/live/LLM/visual tests
  are not release gates unless explicitly promoted by that inventory.

Compatibility aliases:
  changed -> changed-crates, fast -> unit-fast, core -> contract, live -> scenario, full -> all

Manual targets:
  agent-graph
  context-runtime
  context-runtime-lean-spike
  memory-degraded
  same-session-multi-surface-sync
  task-phase
  tui-smoke
  report-build-size
  webui-live-workbench
EOF
    exit 0
    ;;
  *)
    echo "unknown validation lane: $LANE" >&2
    exit 2
    ;;
esac

STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT_DIR="${COWD_REPORT_DIR:-test-reports/validation-$STAMP}"
mkdir -p "$REPORT_DIR/logs"
: > "$REPORT_DIR/commands.tsv"

tmp_available_kb() {
  df -Pk /tmp | awk 'NR==2 {print $4}'
}

select_target_dir() {
  if [[ -n "${COWD_TARGET_DIR:-}" ]]; then
    echo "$COWD_TARGET_DIR"
  elif [[ "$(tmp_available_kb)" -ge "${COWD_MIN_TMP_KB:-8388608}" ]]; then
    echo "/tmp/cowd-target-$STAMP-$$"
  else
    echo "$ROOT/target"
  fi
}

export CARGO_TARGET_DIR="$(select_target_dir)"
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-12}"
export COWD_SCENARIO_SKIP_BUILD=1
TMP_AVAILABLE_KB_AT_START="$(tmp_available_kb)"
KEEP_TARGET="${COWD_KEEP_TARGET:-0}"
INSTALL_DIR_FILE="$REPORT_DIR/install-dir.txt"

cleanup_on_exit() {
  if [[ "$KEEP_TARGET" != "1" && "$CARGO_TARGET_DIR" == /tmp/cowd-target-* ]]; then
    rm -rf "$CARGO_TARGET_DIR"
  fi
}
trap cleanup_on_exit EXIT

bytes_or_zero() {
  du -sb "$1" 2>/dev/null | awk '{print $1}' || echo 0
}

run_step() {
  local name="$1"
  shift

  local log="$REPORT_DIR/logs/$name.log"
  local time_log="$REPORT_DIR/logs/$name.time"
  local start_epoch start_iso before_size status end_epoch end_iso after_size elapsed

  start_epoch="$(date +%s)"
  start_iso="$(date -Iseconds)"
  before_size="$(bytes_or_zero "$CARGO_TARGET_DIR")"
  echo "[$start_iso] START $name" | tee "$log"

  set +e
  /usr/bin/time \
    -f 'TIME_REAL_SECONDS=%e\nTIME_USER_SECONDS=%U\nTIME_SYS_SECONDS=%S\nMAX_RSS_KB=%M' \
    -o "$time_log" \
    "$@" >>"$log" 2>&1
  status=$?
  set -e

  end_epoch="$(date +%s)"
  end_iso="$(date -Iseconds)"
  after_size="$(bytes_or_zero "$CARGO_TARGET_DIR")"
  elapsed=$((end_epoch - start_epoch))

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$status" "$elapsed" "$before_size" "$after_size" "$start_iso" "$end_iso" \
    >> "$REPORT_DIR/commands.tsv"
  echo "[$end_iso] END $name status=$status elapsed=${elapsed}s target_before=${before_size} target_after=${after_size}" >> "$log"
  return 0
}

write_report() {
  local report="$REPORT_DIR/report.md"
  local install_dir=""
  [[ -f "$INSTALL_DIR_FILE" ]] && install_dir="$(cat "$INSTALL_DIR_FILE")"
  {
    echo "# Cowd Validation - $STAMP"
    echo
    echo "- workspace: \`$ROOT\`"
    echo "- lane: \`$LANE\`"
    echo "- target: \`$CARGO_TARGET_DIR\`"
    echo "- install dir: \`${install_dir:-not-installed}\`"
    echo "- cargo incremental: \`${CARGO_INCREMENTAL}\`"
    echo "- cargo jobs: \`${CARGO_BUILD_JOBS}\`"
    echo "- /tmp available KB at start: \`${TMP_AVAILABLE_KB_AT_START}\`"
    echo
    echo "## Commands"
    echo
    echo "| step | status | seconds | target before | target after |"
    echo "| --- | ---: | ---: | ---: | ---: |"
    awk -F '\t' '{printf "| `%s` | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5}' "$REPORT_DIR/commands.tsv"
    echo
    echo "## Failures"
    echo
    if awk -F '\t' '$2 != 0 {found=1} END {exit !found}' "$REPORT_DIR/commands.tsv"; then
      awk -F '\t' '$2 != 0 {print "- `" $1 "` exited with status " $2}' "$REPORT_DIR/commands.tsv"
      echo
      for log in "$REPORT_DIR"/logs/*.log; do
        if rg -q "FAILED|panicked|error:|timed out|No space|failed with status|missing|port .* already" "$log"; then
          echo "### $(basename "$log")"
          echo '```text'
          rg -n "FAILED|panicked|error:|timed out|No space|failed with status|missing|port .* already|test result:" "$log" | tail -100 || true
          echo '```'
          echo
        fi
      done
    else
      echo "No command failures."
    fi
  } > "$report"
  echo "report: $report"
}

fail_if_needed() {
  write_report
  if awk -F '\t' '$2 != 0 {found=1} END {exit found ? 0 : 1}' "$REPORT_DIR/commands.tsv"; then
    exit 1
  fi
}

run_unit_fast() {
  run_quick
}

run_quick() {
  run_step quick_gate bash scripts/test/quick.sh
}

run_changed_crates() {
  run_step changed_crates_gate bash scripts/test/changed-crates.sh "${COWD_CHANGED_BASE:-HEAD}"
}

run_full_regression() {
  run_step full_regression_gate bash scripts/test/full-regression.sh
}

run_contract() {
  run_step cargo_fmt cargo fmt --all --check
  run_step cargo_check_workspace cargo check --workspace --all-targets
  for pkg in approval cli connector fact-kernel harness-contract harness-eval matrix-core model-protocol plugins provider session skill storage surface tools; do
    run_step "cargo_test_$pkg" cargo test -p "$pkg" --all-targets
  done
  run_step cargo_test_runtime_architecture cargo test -p runtime --test runtime_module_architecture
  run_step cargo_test_gateway_architecture cargo test -p gateway --test gateway_runtimehost_architecture
  run_step cargo_test_memory_architecture cargo test -p memory --test memory_module_architecture
}

run_serial_global() {
  run_step gateway_global_env bash scripts/test/gateway-global-env.sh
}

run_scenario() {
  run_step cargo_build_cli cargo build -p cli -p sandbox-launcher --no-default-features
  run_step cargo_build_auth_broker cargo build -p auth-broker
  export COWD_BIN="$CARGO_TARGET_DIR/debug/cowd"
  export COWD_AUTH_BROKER_BIN="$CARGO_TARGET_DIR/debug/cowd-auth-broker"
  run_step ai_harness bash scripts/ci/ai-harness.sh
  run_step gateway_baseline bash scripts/scenarios/gateway-webui-contract.sh
  run_step session_runtime bash scripts/scenarios/runtime-surface.sh
  run_step memory_context bash scripts/scenarios/memory-runtime.sh
  run_step tool_permission bash scripts/scenarios/channel-permission.sh
  run_step skill_mfg bash scripts/scenarios/skill-surface-unification.sh
}

run_surface() {
  run_step cargo_build_cli cargo build -p cli -p sandbox-launcher --no-default-features
  export COWD_BIN="$CARGO_TARGET_DIR/debug/cowd"
  run_step cli_minimal_contract cargo test -p cli --test output_format_contract --no-default-features -- --nocapture --test-threads=1
  run_step tui_projection_smoke bash scripts/scenarios/tui-interaction-quality.sh
  run_step webui_gateway_contract bash scripts/scenarios/gateway-webui-contract.sh
}

run_release() {
  if [[ "${COWD_RELEASE_SKIP_TMP_CLEAN:-0}" == "1" ]]; then
    run_step clean_tmp bash -lc 'echo "skipped tmp cleanup for target reuse"'
  else
    run_step clean_tmp bash scripts/release/clean-build-artifacts.sh --tmp
  fi
  run_step cargo_fmt cargo fmt --check
  run_step cargo_build_debug cargo build -p cli -p sandbox-launcher --no-default-features
  run_step install_debug bash -lc 'scripts/release/install-debug-to-ai.sh --current --print-path-only | tee "$0"' "$INSTALL_DIR_FILE"
  local install_dir
  install_dir="$(cat "$INSTALL_DIR_FILE")"
  export COWD_BIN="$install_dir/cowd"
  run_step artifact_report bash scripts/release/report-artifacts.sh "$install_dir" "$REPORT_DIR/artifacts.md"
  run_step setup_smoke bash -lc '"$COWD_BIN" --version && "$COWD_BIN" doctor'
  run_step full_product_smoke timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/scenarios/full-product-smoke.sh "$install_dir"
  run_step tui_daemon_attach timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/scenarios/tui-daemon-attach.sh
}

run_manual() {
  case "$MANUAL_TARGET" in
    agent-graph)
      run_step manual_agent_graph bash scripts/scenarios/agent-graph.sh
      ;;
    context-runtime)
      run_step manual_context_runtime bash scripts/scenarios/context-runtime.sh
      ;;
    context-runtime-lean-spike)
      run_step manual_context_runtime_lean_spike bash scripts/manual/context-runtime-lean-spike.sh
      ;;
    memory-degraded)
      run_step manual_memory_degraded bash scripts/manual/memory-degraded.sh
      ;;
    same-session-multi-surface-sync)
      run_step manual_same_session_multi_surface_sync bash scripts/scenarios/same-session-multi-surface-sync.sh
      ;;
    task-phase)
      run_step manual_task_phase bash scripts/manual/task-phase.sh
      ;;
    tui-smoke)
      run_step manual_tui_smoke bash scripts/scenarios/tui-smoke.sh
      ;;
    report-build-size)
      run_step manual_report_build_size bash scripts/manual/report-build-size.sh
      ;;
    webui-live-workbench)
      run_step manual_webui_live_workbench bash scripts/manual/webui-live-workbench.sh
      ;;
    *)
      echo "unknown manual validation target: ${MANUAL_TARGET:-<missing>}" >&2
      echo "run scripts/validate.sh --help for available manual targets" >&2
      exit 2
      ;;
  esac
}

case "$LANE" in
  quick) run_quick ;;
  changed-crates) run_changed_crates ;;
  full-regression) run_full_regression ;;
  unit-fast) run_unit_fast ;;
  contract) run_contract ;;
  serial-global) run_serial_global ;;
  scenario) run_scenario ;;
  surface) run_surface ;;
  release) run_release ;;
  manual) run_manual ;;
  all)
    run_contract
    run_serial_global
    run_scenario
    ;;
esac

fail_if_needed
