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

for test_name in "${tests[@]}"; do
  echo "==> gateway slow test: ${test_name}"
  cargo test -p gateway --lib "${test_name}" --no-default-features -- --ignored --test-threads=1
done
