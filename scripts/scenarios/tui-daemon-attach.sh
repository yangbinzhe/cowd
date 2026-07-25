#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_TUI_DAEMON_ATTACH_PORT:-18672}"
BASE_URL="http://127.0.0.1:$PORT"
GATEWAY_SESSION="cowd-tui-gateway-attach-gateway-$$"
TUI_SESSION="cowd-tui-gateway-attach-tui-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-tui-gateway-attach.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
GATEWAY_LOG="$TMP_DIR/gateway.log"
TUI_CAPTURE="$TMP_DIR/tui-pane.txt"
TUI_RUNTIME_SESSION="tui-daemon-attach-$$_session"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-tui-gateway-scenario}"
API_TOKEN="tui-daemon-attach-$$_credential"
FAILED=0

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_TUI_DAEMON_ATTACH_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving TUI Gateway attach temp dir: $TMP_DIR" >&2
    return
  fi
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
    tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
  fi
  for _ in {1..5}; do
    rm -rf "$TMP_DIR" 2>/dev/null && return 0
    sleep 0.1
  done
  rm -rf "$TMP_DIR" 2>/dev/null || true
}

print_logs() {
  echo "----- gateway log -----" >&2
  sed -n '1,220p' "$GATEWAY_LOG" >&2 || true
  echo "----- tui capture -----" >&2
  sed -n '1,220p' "$TUI_CAPTURE" >&2 || true
  echo "-----------------------" >&2
}

on_error() {
  local status=$?
  FAILED=1
  echo "TUI Gateway attach scenario failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl rg ss; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for TUI Gateway attach scenario" >&2
    exit 1
  fi
done

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

cd "$ROOT"
if [[ "${COWD_SCENARIO_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build -p cli --features full
fi
if [[ ! -x "$BIN" ]]; then
  echo "missing cowd binary at $BIN; run cargo build -p cli --features full first" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "$SCENARIO_API_KEY"
    protocol: "anthropic"
    models:
      - "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: false
gateway:
  enabled: true
  sessionReset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: $PORT
      auth:
        enabled: true
        token: "$API_TOKEN"
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$GATEWAY_SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$GATEWAY_LOG' 2>&1\""

for _ in {1..100}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

curl -fsS "$BASE_URL/health" >/dev/null
curl -fsS "$BASE_URL/api/cowd/projection?surface=tui" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("surface") == "tui", data; assert data.get("contract_version") == "cowd.projection.v1", data; assert data.get("capabilities"), data'
curl -fsS "$BASE_URL/api/cowd/surfaces" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("webui_tui_full_parity") is True, data; assert data.get("cli_is_minimal_control") is True, data; assert (data.get("tui") or {}).get("role") == "console_full_capability", data'
curl -fsS "$BASE_URL/api/cowd/release-gate" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); checks=data.get("checks") or []; assert any(item.get("check_id") == "surface.webui_tui.parity" and item.get("status") == "pass" for item in checks), data'

curl -fsS -X POST "$BASE_URL/api/connectors/services/local.docs/execute" \
  -H 'Content-Type: application/json' \
  --data "{\"tool_id\":\"service.local.docs.read\",\"resource_id\":\"tui-daemon-tui-doc\",\"title\":\"tui-daemon TUI Attach Doc\",\"mode\":\"commit\",\"idempotency_key\":\"tui-daemon-$$\"}" \
  >/dev/null

tmux new-session -d -s "$TUI_SESSION" -x 140 -y 42 \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    export COWD_API_TOKEN='$API_TOKEN' && \
    export ANTHROPIC_API_KEY='$SCENARIO_API_KEY' && \
    export COWD_GATEWAY_URL='$BASE_URL' && \
    export COWD_DISABLE_DAEMON_AUTOSTART=1 && \
    export COWD_TUI_ACCESSIBILITY=1 && \
    export COWD_TUI_SKIP_RAW_MODE=1 && \
    export TERM=xterm-256color && \
    timeout 14s '$BIN' --yolo --model claude-sonnet-4-6 --session '$TUI_RUNTIME_SESSION'; \
    status=\\\$?; printf '\\n__COWD_TUI_EXIT__%s\\n' \\\"\\\$status\\\"; sleep 8\""

attached=0
for _ in {1..32}; do
  projection_json=""
  if projection_json="$(curl -fsS "$BASE_URL/api/sessions/$TUI_RUNTIME_SESSION/projection" 2>/dev/null)" \
    && printf '%s' "$projection_json" \
      | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("session_id") == sys.argv[1], data' "$TUI_RUNTIME_SESSION"; then
    attached=1
    break
  fi
  tmux has-session -t "$TUI_SESSION" >/dev/null 2>&1 || break
  sleep 0.25
done

[[ "$attached" == "1" ]]
tmux has-session -t "$TUI_SESSION" >/dev/null
tmux capture-pane -pt "$TUI_SESSION" -S -260 >"$TUI_CAPTURE" 2>/dev/null || true
if rg -q "__COWD_TUI_EXIT__[1-9]|panic|backtrace|thread .* panicked|failed to initialize terminal|Run cowd --help" "$TUI_CAPTURE"; then
  exit 1
fi

curl -fsS "$BASE_URL/api/connectors/resources" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); text=json.dumps(data); assert "tui-daemon-tui-doc" in text or "tui-daemon TUI Attach Doc" in text, data'

tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
tmux kill-session -t "$GATEWAY_SESSION" >/dev/null 2>&1 || true
echo "TUI Gateway attach scenario passed"
