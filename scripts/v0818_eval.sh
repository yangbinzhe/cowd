#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEBUI="$ROOT/webui"
PORT_PATTERN=':(18642|18643|18652|18662|18663|18664|18665|18666|18667|18668|18669|9237|9238|9239|9240|9241|8642)\b'
TMUX_PATTERN='cowd-|webui-|phase5|message100k|prepare|audit|workspace|profile|v0815|v0816|v0817|v0818'

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/media/yi/Datas/workspace/.cowd-v0818-target}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_webui() {
  printf '\n==> (webui) %s\n' "$*"
  (cd "$WEBUI" && "$@")
}

check_version_contract() {
  printf '\n==> checking v0.8.18 version contract\n'
  cargo metadata --no-deps --format-version 1 | rg '"version":"0\.8\.18"' >/dev/null
  if rg -n '当前版本 v0\.8\.17|version = "0\.8\.17"' Cargo.toml README.md; then
    echo "v0.8.18 eval found stale active version text" >&2
    return 1
  fi
}

check_no_test_ports() {
  printf '\n==> checking v0.8.18 test ports\n'
  if ss -ltnp | rg "$PORT_PATTERN"; then
    echo "v0.8.18 eval found leftover test listeners" >&2
    return 1
  fi
}

check_no_test_tmux() {
  printf '\n==> checking v0.8.18 tmux sessions\n'
  if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux not installed; skipping tmux check"
    return 0
  fi
  if ! tmux ls >/tmp/cowd-v0818-tmux.txt 2>/dev/null; then
    return 0
  fi
  if rg "$TMUX_PATTERN" /tmp/cowd-v0818-tmux.txt; then
    echo "v0.8.18 eval found leftover test tmux sessions" >&2
    return 1
  fi
}

maybe_live_scenarios() {
  if [ "${COWD_V0818_LIVE:-0}" != "1" ]; then
    printf '\n==> skipping optional live tmux/browser scenarios (set COWD_V0818_LIVE=1)\n'
    return 0
  fi

  if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux not installed; skipping optional live scenarios"
    return 0
  fi

  run cargo build -p cowd-cli
  export COWD_BIN="${COWD_BIN:-$CARGO_TARGET_DIR/debug/cowd}"

  run scripts/tui_smoke.sh
  if [ -x scripts/webui_live_workbench_scenario.sh ]; then
    run scripts/webui_live_workbench_scenario.sh
  fi
}

main() {
  cd "$ROOT"

  check_version_contract
  run cargo test -p cowd-memory memory_pulse -- --nocapture
  run cargo test -p runtime agent_workgraph -- --nocapture
  run cargo test -p cowd-cli runtime_closed_loop -- --nocapture
  run cargo test -p cowd-memory runtime_event -- --nocapture
  run cargo test -p runtime context_policy_ -- --nocapture
  run cargo test -p cowd-cli runtime_ -- --nocapture
  run cargo check -p cowd-cli

  if [ -f "$WEBUI/package.json" ]; then
    run_webui npm test
  fi

  maybe_live_scenarios
  run git diff --check
  check_no_test_ports
  check_no_test_tmux

  printf '\nv0.8.18 runtime closed-loop evaluation passed\n'
}

main "$@"
