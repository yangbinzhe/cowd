#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LANE="${1:-${COWD_VALIDATION_LANE:-contract}}"

case "$LANE" in
  unit-fast|fast) LANE="unit-fast" ;;
  contract|core) LANE="contract" ;;
  scenario|live) LANE="scenario" ;;
  release) LANE="release" ;;
  all|full) LANE="all" ;;
  -h|--help|help)
    cat <<'EOF'
Usage: scripts/validate.sh [unit-fast|contract|scenario|release|all]

Lanes:
  unit-fast  edit feedback: fmt, light crates, targeted heavy-crate probes, WebUI unit
  contract   package/API/CLI contracts without browser or tmux scenarios
  scenario   one debug build plus daemon/TUI/WebUI scenario contracts
  release    clean build, install to ~/AI/cowd-debug-current, and release smoke
  all        contract + scenario

Compatibility aliases:
  fast -> unit-fast, core -> contract, live -> scenario, full -> all
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
  run_step cargo_fmt cargo fmt --check
  run_step cargo_test_plugins cargo test -p plugins --no-default-features -- --nocapture
  run_step cargo_test_telemetry cargo test -p telemetry --no-default-features -- --nocapture
  run_step cargo_test_commands cargo test -p commands --no-default-features -- --nocapture
  run_step cargo_test_memory_tuner cargo test -p cowd-memory performance_monitor::tests::test_tuner --no-default-features -- --nocapture
  run_step cargo_test_runtime_worker_state cargo test -p runtime worker_boot::tests::emit_state_file_writes_worker_status_on_transition --no-default-features -- --nocapture
  run_step webui_npm_test bash -lc 'cd webui && npm test'
}

run_contract() {
  run_step cargo_fmt cargo fmt --check
  for pkg in api commands compat-harness cowd-memory mock-anthropic-service plugins runtime telemetry tools; do
    run_step "cargo_test_$pkg" cargo test -p "$pkg" --no-default-features -- --nocapture
  done
  run_step cargo_test_cowd_cli_setup timeout "${COWD_CLI_TEST_TIMEOUT_SECS:-240}" \
    cargo test -p cowd-cli setup --no-default-features -- --nocapture --test-threads=1
  run_step cargo_test_cowd_cli_gateway timeout "${COWD_CLI_TEST_TIMEOUT_SECS:-240}" \
    cargo test -p cowd-cli gateway::tests:: --no-default-features -- --nocapture --test-threads=1
  run_step cargo_test_cowd_cli_runtime_control timeout "${COWD_CLI_TEST_TIMEOUT_SECS:-240}" \
    cargo test -p cowd-cli runtime_control --no-default-features -- --nocapture --test-threads=1
  run_step cargo_test_cowd_cli_connector timeout "${COWD_CLI_TEST_TIMEOUT_SECS:-240}" \
    cargo test -p cowd-cli connector --no-default-features -- --nocapture --test-threads=1
  run_step webui_npm_test bash -lc 'cd webui && npm test'
}

run_scenario() {
  run_step cargo_build_cli cargo build -p cowd-cli --no-default-features
  export COWD_BIN="$CARGO_TARGET_DIR/debug/cowd"
  run_step tui_smoke bash scripts/tui_smoke.sh
  run_step unified_runtime_surface bash scripts/v0964_unified_runtime_surface_scenario.sh
  run_step session_lifecycle bash scripts/v0968_session_lifecycle_scenario.sh
  run_step agent_graph_contract bash scripts/v0971_agent_graph_scenario.sh
  run_step context_runtime_contract bash scripts/v0972_context_runtime_scenario.sh
  run_step memory_runtime_contract bash scripts/v0973_memory_runtime_scenario.sh
  run_step channel_permission_contract bash scripts/v0974_channel_permission_scenario.sh
}

run_release() {
  if [[ "${COWD_RELEASE_SKIP_TMP_CLEAN:-0}" == "1" ]]; then
    run_step clean_tmp bash -lc 'echo "skipped tmp cleanup for target reuse"'
  else
    run_step clean_tmp bash scripts/clean_build_artifacts.sh --tmp
  fi
  run_step cargo_fmt cargo fmt --check
  run_step cargo_build_debug cargo build -p cowd-cli --no-default-features
  run_step install_debug bash -lc 'scripts/install_debug_to_ai.sh --current --print-path-only | tee "$0"' "$INSTALL_DIR_FILE"
  local install_dir
  install_dir="$(cat "$INSTALL_DIR_FILE")"
  export COWD_BIN="$install_dir/bin/cowd"
  run_step artifact_report bash scripts/report_release_artifacts.sh "$install_dir" "$REPORT_DIR/artifacts.md"
  run_step setup_smoke bash -lc '"$COWD_BIN" --version && "$COWD_BIN" setup'
  run_step full_product_smoke timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/scenario_full_product_smoke.sh "$install_dir"
  run_step tui_daemon_attach timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/v0952_tui_daemon_attach_scenario.sh
  run_step channel_permission_contract timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/v0974_channel_permission_scenario.sh
}

case "$LANE" in
  unit-fast) run_unit_fast ;;
  contract) run_contract ;;
  scenario) run_scenario ;;
  release) run_release ;;
  all)
    run_contract
    run_scenario
    ;;
esac

fail_if_needed
