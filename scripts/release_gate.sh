#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEBUI="$ROOT/webui"
CHROMIUM="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-/snap/bin/chromium}"
PORT_PATTERN=':(18642|18643|18652|18662|18663|18664|18665|18666|18667|18668|9237|9238|9239|9240|9241|8642)\b'
TMUX_PATTERN='cowd-|webui-|phase5|message100k|prepare|audit|workspace|profile'

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_in_webui() {
  printf '\n==> (webui) %s\n' "$*"
  (cd "$WEBUI" && "$@")
}

check_no_test_ports() {
  printf '\n==> checking release-gate test ports\n'
  if ss -ltnp | rg "$PORT_PATTERN"; then
    echo "release gate found leftover test listeners" >&2
    return 1
  fi
}

check_no_test_tmux() {
  printf '\n==> checking release-gate tmux sessions\n'
  if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux not installed; skipping tmux session check"
    return 0
  fi
  if ! tmux ls >/tmp/cowd-release-gate-tmux.txt 2>/dev/null; then
    return 0
  fi
  if rg "$TMUX_PATTERN" /tmp/cowd-release-gate-tmux.txt; then
    echo "release gate found leftover test tmux sessions" >&2
    return 1
  fi
}

main() {
  cd "$ROOT"

  run cargo test -p cowd-cli api_routes -- --nocapture
  run cargo test -p cowd-cli tui::components::session_sidebar::tests:: -- --nocapture
  run cargo test -p cowd-memory store::session::tests -- --nocapture
  run cargo test -p cowd-memory --test prepare_context_test -- --nocapture
  run cargo test -p cowd-memory --test performance_bench -- --nocapture
  run cargo test -p cowd-cli session_kernel -- --nocapture
  run cargo test -p cowd-cli task -- --nocapture
  run scripts/task_phase_scenario.sh
  run scripts/memory_degraded_scenario.sh
  run cargo test -p cowd-cli --test resume_slash_commands resume_latest_restores_the_most_recent_managed_session -- --nocapture
  run cargo test -p cowd-cli yolo -- --nocapture
  run cargo test -p cowd-cli cli_session_sync_replaces_store_messages_and_events -- --nocapture
  run cargo check -p cowd-cli
  run cargo build -p cowd-cli
  run scripts/tui_smoke.sh

  run_in_webui npm test
  run_in_webui env PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$CHROMIUM" npm run test:e2e

  check_no_test_ports
  check_no_test_tmux

  printf '\nrelease gate passed\n'
}

main "$@"
