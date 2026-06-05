#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/debug/cowd"
SESSION="cowd-tui-smoke-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-tui-smoke.XXXXXX)"
CAPTURE="$TMP_DIR/pane.txt"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
WORKSPACE="$TMP_DIR/workspace"
SMOKE_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-tui-smoke}"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux not installed; skipping TUI smoke test"
  exit 0
fi

if [[ ! -x "$BIN" ]]; then
  echo "missing cowd binary at $BIN; run cargo build -p cowd-cli first" >&2
  exit 1
fi

mkdir -p "$CONFIG_HOME" "$HOME_DIR/.cowd" "$WORKSPACE"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "$SMOKE_API_KEY"
    protocol: "anthropic"
    models:
      - "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: false
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"

tmux new-session -d -s "$SESSION" -x 120 -y 36 \
  "bash -lc \"cd '$WORKSPACE' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    export ANTHROPIC_API_KEY='$SMOKE_API_KEY' && \
    export COWD_DISABLE_DAEMON_AUTOSTART=1 && \
    export COWD_TUI_ACCESSIBILITY=1 && \
    export COWD_TUI_SKIP_RAW_MODE=1 && \
    export TERM=xterm-256color && \
    timeout 20s '$BIN' --yolo --model claude-sonnet-4-6; \
    status=\\\$?; printf '\\n__COWD_EXIT__%s\\n' \\\"\\\$status\\\"; sleep 20\""

for _ in {1..40}; do
  tmux capture-pane -pt "$SESSION" -S -200 >"$CAPTURE" 2>/dev/null || true
  if rg -q "YOLO|continuous|Cowd|COWD" "$CAPTURE"; then
    break
  fi
  sleep 0.25
done

tmux capture-pane -pt "$SESSION" -S -200 >"$CAPTURE"

if rg -q "__COWD_EXIT__[1-9]" "$CAPTURE"; then
  echo "TUI exited before rendering successfully" >&2
  sed -n '1,160p' "$CAPTURE" >&2
  exit 1
fi

if ! rg -q "YOLO|continuous|Cowd|COWD" "$CAPTURE"; then
  echo "TUI smoke test did not observe expected startup content" >&2
  sed -n '1,120p' "$CAPTURE" >&2
  exit 1
fi

if rg -qi "panic|backtrace|thread .* panicked|failed to initialize terminal|没有为模型|Run cowd --help" "$CAPTURE"; then
  echo "TUI smoke test observed a startup failure" >&2
  sed -n '1,160p' "$CAPTURE" >&2
  exit 1
fi

tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
echo "TUI smoke test passed"
exit 0
