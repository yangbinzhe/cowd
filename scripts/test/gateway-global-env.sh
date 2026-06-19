#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

tests=(
  user_defined_aliases_resolve_before_provider_dispatch
  yolo_flag_forces_danger_full_access_and_marks_repl_mode
  yolo_mode_creates_and_reuses_durable_task
  yolo_system_prompt_adds_continuous_execution_instruction
  setup_report_and_json_are_redacted_and_actionable
  resolve_repl_model_falls_back_to_anthropic_model_env_when_default
  resolve_repl_model_returns_default_when_env_unset_and_no_config
  resume_diff_command_renders_report_for_saved_session
  resume_session_switch_updates_outcome_session_and_path
  tui_sidebar_switch_replaces_live_runtime_session
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
  echo "==> gateway global-env test: ${test_name}"
  log="$REPORT_DIR/logs/${test_name}.log"
  time_log="$REPORT_DIR/logs/${test_name}.time"
  set +e
  /usr/bin/time \
    -f 'TIME_REAL_SECONDS=%e\nTIME_USER_SECONDS=%U\nTIME_SYS_SECONDS=%S\nMAX_RSS_KB=%M' \
    -o "$time_log" \
    cargo test -p gateway --lib "${test_name}" --no-default-features --quiet -- --ignored --test-threads=1 \
    >"$log" 2>&1
  test_status=$?
  set -e
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
