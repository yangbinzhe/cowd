#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT_DIR="${COWD_REPORT_DIR:-test-reports/release-$STAMP}"
mkdir -p "$REPORT_DIR/logs"
: > "$REPORT_DIR/commands.tsv"

tmp_available_kb() {
  df -Pk /tmp | awk 'NR==2 {print $4}'
}

select_target_dir() {
  if [[ -n "${COWD_TARGET_DIR:-}" ]]; then
    echo "$COWD_TARGET_DIR"
  elif [[ "$(tmp_available_kb)" -ge "${COWD_MIN_TMP_KB:-8388608}" ]]; then
    echo "/tmp/cowd-target-release-$STAMP-$$"
  else
    echo "$ROOT/target"
  fi
}

export CARGO_TARGET_DIR="$(select_target_dir)"
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-12}"
export COWD_SCENARIO_SKIP_BUILD=1
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
    echo "# Cowd Release Validation - $STAMP"
    echo
    echo "- workspace: \`$ROOT\`"
    echo "- target: \`$CARGO_TARGET_DIR\`"
    echo "- install dir: \`${install_dir:-not-installed}\`"
    echo "- cargo incremental: \`${CARGO_INCREMENTAL}\`"
    echo "- cargo jobs: \`${CARGO_BUILD_JOBS}\`"
    echo "- /tmp available KB at start: \`$(tmp_available_kb)\`"
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
  echo "release report: $report"
}

if [[ "${COWD_RELEASE_SKIP_TMP_CLEAN:-0}" == "1" ]]; then
  run_step clean_tmp bash -lc 'echo "skipped tmp cleanup for target reuse"'
else
  run_step clean_tmp bash scripts/clean_build_artifacts.sh --tmp
fi
run_step cargo_fmt cargo fmt --check
run_step cargo_build_debug cargo build -p cowd-cli --no-default-features

run_step install_debug bash -lc 'scripts/install_debug_to_ai.sh --current --print-path-only | tee "$0"' "$INSTALL_DIR_FILE"
INSTALL_DIR="$(cat "$INSTALL_DIR_FILE")"
export COWD_BIN="$INSTALL_DIR/bin/cowd"

run_step artifact_report bash scripts/report_release_artifacts.sh "$INSTALL_DIR" "$REPORT_DIR/artifacts.md"
run_step full_product_smoke timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/scenario_full_product_smoke.sh "$INSTALL_DIR"
run_step tui_daemon_attach timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/v0952_tui_daemon_attach_scenario.sh
run_step same_session_multi_surface timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/v0956_same_session_multi_surface_sync.sh
run_step memory_runtime_contract timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/v0973_memory_runtime_scenario.sh
run_step channel_permission_contract timeout "${COWD_RELEASE_STEP_TIMEOUT_SECS:-240}" bash scripts/v0974_channel_permission_scenario.sh

write_report

if awk -F '\t' '$2 != 0 {found=1} END {exit found ? 0 : 1}' "$REPORT_DIR/commands.tsv"; then
  exit 1
fi
