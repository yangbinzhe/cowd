#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEBUI="$ROOT/webui"
PORT_PATTERN=':(18642|18643|18652|18662|18663|18664|18665|18666|18667|18668|18669|9237|9238|9239|9240|9241|8642)\b'
TMUX_PATTERN='cowd-|webui-|phase5|message100k|prepare|audit|workspace|profile|v0815'

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/cowd-v0815-target}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_webui() {
  printf '\n==> (webui) %s\n' "$*"
  (cd "$WEBUI" && "$@")
}

check_no_test_ports() {
  printf '\n==> checking v0.8.15 test ports\n'
  if ss -ltnp | rg "$PORT_PATTERN"; then
    echo "v0.8.15 eval found leftover test listeners" >&2
    return 1
  fi
}

check_no_test_tmux() {
  printf '\n==> checking v0.8.15 tmux sessions\n'
  if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux not installed; skipping tmux check"
    return 0
  fi
  if ! tmux ls >/tmp/cowd-v0815-tmux.txt 2>/dev/null; then
    return 0
  fi
  if rg "$TMUX_PATTERN" /tmp/cowd-v0815-tmux.txt; then
    echo "v0.8.15 eval found leftover test tmux sessions" >&2
    return 1
  fi
}

main() {
  cd "$ROOT"

  run scripts/context_runtime_lean_spike.sh
  run cargo test -p runtime agent_collaboration -- --nocapture
  run cargo test -p runtime context_runtime -- --nocapture
  run cargo test -p cowd-memory maintenance -- --nocapture
  run cargo test -p cowd-cli session_kernel -- --nocapture
  run cargo test -p cowd-cli task -- --nocapture
  run cargo test -p cowd-cli api_routes::tests::session_runs_route_reads_runtime_run_events_only -- --nocapture
  run cargo test -p cowd-cli api_routes::tests::memory_maintenance_scan_and_transition -- --nocapture
  run cargo check -p cowd-cli

  if [ -f "$WEBUI/package.json" ]; then
    run_webui npm test
  fi

  run git diff --check
  check_no_test_ports
  check_no_test_tmux

  printf '\nv0.8.15 evaluation baseline passed\n'
}

main "$@"
