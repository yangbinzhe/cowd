#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/cowd-v0963-tui-quality-target}"

cd "$ROOT"

run() {
  echo "+ $*"
  "$@"
}

run env CARGO_TARGET_DIR="$TARGET_DIR" cargo test -p cowd-cli \
  render_bridge_projects_runtime_command_center_to_gateway_tab --no-default-features
run env CARGO_TARGET_DIR="$TARGET_DIR" cargo test -p cowd-cli \
  renders_every_sidebar_tab_in_wide_and_compact_layouts --no-default-features
run env CARGO_TARGET_DIR="$TARGET_DIR" cargo test -p cowd-cli \
  render_shows_connector_console_state --no-default-features

if [[ "${COWD_TUI_RUN_TMUX_SMOKE:-0}" == "1" ]]; then
  run cargo build -p cowd-cli --no-default-features
  run scripts/tui_smoke.sh
fi
