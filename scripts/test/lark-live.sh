#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ "${COWD_LIVE_LARK_SKILL_TEST:-0}" != "1" ]]; then
  echo "COWD_LIVE_LARK_SKILL_TEST=1 is required" >&2
  exit 2
fi
if [[ "${COWD_LIVE_LARK_CLI_TOOL_TEST:-0}" != "1" ]]; then
  echo "COWD_LIVE_LARK_CLI_TOOL_TEST=1 is required" >&2
  exit 2
fi

cargo test -p runtime \
  live_cowd_owned_lark_cli_executes_inside_hardened_sandbox \
  -- --ignored --exact --nocapture
cargo test -p gateway \
  live_cowd_lark_skills_are_discovered_and_selected_by_runtime \
  -- --ignored --exact --nocapture --test-threads=1
cargo test -p gateway \
  live_configured_bot_executes_official_cli_without_forwarding_app_secret \
  -- --ignored --exact --nocapture --test-threads=1
