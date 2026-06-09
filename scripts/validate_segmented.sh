#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STAMP="$(date +%Y%m%d-%H%M%S)"
SCOPE="${1:-${COWD_VALIDATION_SCOPE:-full}}"
REPORT_DIR="${COWD_REPORT_DIR:-test-reports/validation-$STAMP}"
mkdir -p "$REPORT_DIR/logs"
: > "$REPORT_DIR/commands.tsv"

case "$SCOPE" in
  fast|core|full|live) ;;
  -h|--help|help)
    cat <<'EOF'
Usage: scripts/validate_segmented.sh [fast|core|full|live]

Scopes:
  fast  quick local checks for edit feedback
  core  core Rust package checks plus WebUI unit tests
  full  full segmented Rust/WebUI validation, default
  live  scenario scripts for daemon/TUI/WebUI runtime surfaces
EOF
    exit 0
    ;;
  *)
    echo "unknown validation scope: $SCOPE" >&2
    exit 2
    ;;
esac

tmp_available_kb() {
  df -Pk /tmp | awk 'NR==2 {print $4}'
}

select_target_dir() {
  if [[ -n "${COWD_TARGET_DIR:-}" ]]; then
    echo "$COWD_TARGET_DIR"
    return
  fi
  if [[ "$(tmp_available_kb)" -ge "${COWD_MIN_TMP_KB:-8388608}" ]]; then
    echo "/tmp/cowd-target-$STAMP-$$"
  else
    echo "$ROOT/target"
  fi
}

export CARGO_TARGET_DIR="$(select_target_dir)"
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
TMP_AVAILABLE_KB_AT_START="$(tmp_available_kb)"

KEEP_TARGET="${COWD_KEEP_TARGET:-0}"

cleanup_target() {
  if [[ "$KEEP_TARGET" != "1" ]]; then
    rm -rf "$CARGO_TARGET_DIR"
  fi
}

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
  local clean_after="$2"
  shift 2

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

  if [[ "$clean_after" == "clean" ]]; then
    cleanup_target
  fi

  return 0
}

write_report() {
  local report="$REPORT_DIR/report.md"
  {
    echo "# Cowd Segmented Validation - $STAMP"
    echo
    echo "- workspace: \`$ROOT\`"
    echo "- target: \`$CARGO_TARGET_DIR\`"
    echo "- scope: \`$SCOPE\`"
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
        if rg -q "FAILED|panicked|error:|timed out|has been running|No space" "$log"; then
          echo "### $(basename "$log")"
          echo '```text'
          rg -n "FAILED|panicked|error:|timed out|has been running|No space|test result:" "$log" | tail -80 || true
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

run_step cargo_fmt keep cargo fmt --check

case "$SCOPE" in
  fast)
    run_step cargo_test_plugins clean cargo test -p plugins --no-default-features -- --nocapture
    run_step cargo_test_telemetry clean cargo test -p telemetry --no-default-features -- --nocapture
    run_step cargo_test_memory_tuner clean cargo test -p cowd-memory performance_monitor::tests::test_tuner --no-default-features -- --nocapture
    run_step cargo_test_runtime_worker_state clean cargo test -p runtime worker_boot::tests::emit_state_file_writes_worker_status_on_transition --no-default-features -- --nocapture
    run_step webui_npm_test keep bash -lc 'cd webui && npm test'
    ;;
  core)
    for pkg in api commands cowd-memory runtime tools; do
      run_step "cargo_test_$pkg" clean cargo test -p "$pkg" --no-default-features -- --nocapture
    done
    run_step cargo_test_cowd-cli_core clean timeout "${COWD_CLI_TEST_TIMEOUT_SECS:-240}" \
      cargo test -p cowd-cli gateway::tests:: --no-default-features -- --nocapture --test-threads=1
    run_step webui_npm_test keep bash -lc 'cd webui && npm test'
    ;;
  full)
    for pkg in api commands compat-harness cowd-memory mock-anthropic-service plugins runtime telemetry tools; do
      run_step "cargo_test_$pkg" clean cargo test -p "$pkg" --no-default-features -- --nocapture
    done
    run_step cargo_test_cowd-cli clean timeout "${COWD_CLI_TEST_TIMEOUT_SECS:-360}" \
      cargo test -p cowd-cli --no-default-features -- --nocapture --test-threads=1
    run_step cargo_build_cli clean cargo build -p cowd-cli --no-default-features
    run_step webui_npm_test keep bash -lc 'cd webui && npm test'
    run_step webui_e2e keep bash -lc 'cd webui && PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-/snap/bin/chromium}" npm run test:e2e'
    ;;
  live)
    run_step cargo_build_cli keep cargo build -p cowd-cli --no-default-features
    run_step tui_smoke keep bash scripts/tui_smoke.sh
    run_step webui_live_workbench keep bash scripts/webui_live_workbench_scenario.sh
    run_step unified_runtime_surface keep bash scripts/v0964_unified_runtime_surface_scenario.sh
    run_step tui_daemon_attach keep bash scripts/v0952_tui_daemon_attach_scenario.sh
    run_step same_session_multi_surface keep bash scripts/v0956_same_session_multi_surface_sync.sh
    run_step session_lifecycle keep bash scripts/v0968_session_lifecycle_scenario.sh
    run_step gateway_webui_contract keep bash scripts/v0970_gateway_webui_contract_scenario.sh
    run_step agent_graph_contract keep bash scripts/v0971_agent_graph_scenario.sh
    run_step context_runtime_contract keep bash scripts/v0972_context_runtime_scenario.sh
    run_step tui_interaction_quality keep bash scripts/v0963_tui_interaction_quality_gate.sh
    ;;
esac

write_report

if awk -F '\t' '$2 != 0 {found=1} END {exit found ? 0 : 1}' "$REPORT_DIR/commands.tsv"; then
  exit 1
fi
