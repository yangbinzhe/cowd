#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

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

KEEP_TARGET="${COWD_KEEP_TARGET:-0}"

cleanup_target() {
  if [[ "$KEEP_TARGET" != "1" ]]; then
    rm -rf "$CARGO_TARGET_DIR"
  fi
}

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

for pkg in api commands compat-harness cowd-memory mock-anthropic-service plugins runtime telemetry tools; do
  run_step "cargo_test_$pkg" clean cargo test -p "$pkg" --no-default-features -- --nocapture
done

run_step cargo_test_cowd-cli clean timeout "${COWD_CLI_TEST_TIMEOUT_SECS:-360}" \
  cargo test -p cowd-cli --no-default-features -- --nocapture --test-threads=1

run_step cargo_build_cli clean cargo build -p cowd-cli --no-default-features
run_step webui_npm_test keep bash -lc 'cd webui && npm test'
run_step webui_e2e keep bash -lc 'cd webui && PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-/snap/bin/chromium}" npm run test:e2e'

write_report

if awk -F '\t' '$2 != 0 {found=1} END {exit found ? 0 : 1}' "$REPORT_DIR/commands.tsv"; then
  exit 1
fi

