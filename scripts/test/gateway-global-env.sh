#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

tests=(
  user_defined_aliases_resolve_before_provider_dispatch
  yolo_flag_forces_danger_full_access_and_marks_tui_mode
  yolo_system_prompt_adds_continuous_execution_instruction
  setup_report_and_json_are_redacted_and_actionable
  resolve_tui_model_ignores_provider_specific_model_environment
  resolve_tui_model_returns_default_when_env_unset_and_no_config
)

if [[ "$#" -gt 0 ]]; then
  tests=("$@")
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT_DIR="${COWD_REPORT_DIR:-test-reports/gateway-global-env-$STAMP}"
mkdir -p "$REPORT_DIR/logs"
: > "$REPORT_DIR/commands.tsv"

status=0
for test_name in "${tests[@]}"; do
  if [[ ! "$test_name" =~ ^[a-zA-Z0-9_:]+$ ]]; then
    echo "invalid global-env test name: $test_name" >&2
    exit 2
  fi
  qualified_name="$test_name"
  [[ "$qualified_name" == *::* ]] || qualified_name="tests::$qualified_name"
  echo "==> gateway global-env test: ${test_name}"
  log="$REPORT_DIR/logs/${test_name}.log"
  time_log="$REPORT_DIR/logs/${test_name}.time"
  set +e
  /usr/bin/time \
    -f 'TIME_REAL_SECONDS=%e\nTIME_USER_SECONDS=%U\nTIME_SYS_SECONDS=%S\nMAX_RSS_KB=%M' \
    -o "$time_log" \
    cargo test --locked -p gateway --lib "${qualified_name}" --no-default-features --quiet -- --exact --ignored --test-threads=1 \
    >"$log" 2>&1
  test_status=$?
  set -e
  if [[ "$test_status" -eq 0 ]] && ! rg -q '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "$log"; then
    echo "global-env gate did not execute exactly one passing test: $qualified_name" >> "$log"
    test_status=1
  fi
  real_seconds="$(awk -F= '$1 == "TIME_REAL_SECONDS" {print $2}' "$time_log")"
  printf '%s\t%s\t%s\n' "$test_name" "$test_status" "$real_seconds" >> "$REPORT_DIR/commands.tsv"
  echo "    status=${test_status} real=${real_seconds}s"
  if [[ "$test_status" -ne 0 ]]; then
    status="$test_status"
    tail -80 "$log"
  fi
done

{
  echo "# Gateway Global-Env Tests"
  echo
  echo "- report dir: \`$REPORT_DIR\`"
  echo
  echo "| test | status | real seconds |"
  echo "| --- | ---: | ---: |"
  awk -F '\t' '{printf "| `%s` | %s | %s |\n", $1, $2, $3}' "$REPORT_DIR/commands.tsv"
} > "$REPORT_DIR/report.md"

echo "report: $REPORT_DIR/report.md"
exit "$status"
