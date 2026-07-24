#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
EXPECTED_GIT_SHA="$(git -C "$ROOT" rev-parse --short=8 HEAD)"
EXPECTED_BUILD_DATE="${COWD_V584_EXPECTED_BUILD_DATE:-$(date -u +%Y-%m-%d)}"
GATEWAY_PORT="${COWD_V584_GATEWAY_PORT:-18783}"
PROVIDER_PORT="${COWD_V584_PROVIDER_PORT:-18784}"
BASE_URL="http://127.0.0.1:$GATEWAY_PORT"
TOKEN="v584-tui-acceptance-token"
MODEL="cowd-v584-acceptance"
SCREEN_PREFIX="cowd-v584-$PPID-$$"
SESSION_A="v584-session-a-$$"
SESSION_B="v584-session-b-$$"
SESSION_LONG="v584-session-long-$$"
SESSION_10K="v584-session-10k-$$"
NONCE_A="V584-NONCE-A-$$"
NONCE_B="V584-NONCE-B-$$"
ARTIFACT_ROOT="${COWD_V584_ARTIFACT_DIR:-$ROOT/../plan/0724-Cowd-V584-TUI生产终局交付/artifacts}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
ARTIFACT_DIR="$ARTIFACT_ROOT/$RUN_ID"
RUNTIME_DIR="$(mktemp -d /tmp/cowd-v584-tui.XXXXXX)"
CONFIG_HOME="$RUNTIME_DIR/config"
HOME_DIR="$RUNTIME_DIR/home"
WORKSPACE="$RUNTIME_DIR/workspace"
PROVIDER_LOG="$ARTIFACT_DIR/provider-requests.jsonl"
PROVIDER_STDOUT="$ARTIFACT_DIR/provider.log"
GATEWAY_LOG="$ARTIFACT_DIR/gateway.log"
SESSION_STREAM_LOG="$ARTIFACT_DIR/session-a-stream.sse"
WEBUI_STREAM_LOG="$ARTIFACT_DIR/webui-session-a-stream.sse"
SUMMARY="$ARTIFACT_DIR/summary.tsv"
PERFORMANCE="$ARTIFACT_DIR/performance.tsv"
GATEWAY_PID=""
PROVIDER_PID=""
SESSION_STREAM_PID=""
WEBUI_STREAM_PID=""
declare -A TUI_SCREEN=()
declare -A TUI_DRIVER_PID=()
declare -A TUI_UTF8_LOG=()

auth_curl() {
  command curl -fsS -H "Authorization: Bearer $TOKEN" "$@"
}

monotonic_ms() {
  python3 -c 'import time; print(time.monotonic_ns() // 1_000_000)'
}

cleanup() {
  local target
  for target in "${!TUI_SCREEN[@]}"; do
    stop_tui "$target"
  done
  if [[ -n "$SESSION_STREAM_PID" ]]; then
    kill "$SESSION_STREAM_PID" >/dev/null 2>&1 || true
    wait "$SESSION_STREAM_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$WEBUI_STREAM_PID" ]]; then
    kill "$WEBUI_STREAM_PID" >/dev/null 2>&1 || true
    wait "$WEBUI_STREAM_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$GATEWAY_PID" ]]; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$PROVIDER_PID" ]]; then
    kill "$PROVIDER_PID" >/dev/null 2>&1 || true
    wait "$PROVIDER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$RUNTIME_DIR"
}
trap cleanup EXIT

fail() {
  printf 'FAIL\t%s\n' "$*" | tee -a "$SUMMARY" >&2
  exit 1
}

pass() {
  printf 'PASS\t%s\n' "$*" | tee -a "$SUMMARY"
}

on_unexpected_error() {
  local status="$1"
  local line="$2"
  local command="$3"
  trap - ERR
  printf 'FAIL\tunexpected shell error at line %s (exit %s): %s\n' \
    "$line" "$status" "$command" | tee -a "$SUMMARY" >&2
  exit "$status"
}
trap 'on_unexpected_error "$?" "$LINENO" "$BASH_COMMAND"' ERR

capture() {
  local target="$1"
  local name="$2"
  local session="${TUI_SCREEN[$target]:-}"
  [[ -n "$session" ]] || return 1
  for _ in {1..10}; do
    rm -f "$ARTIFACT_DIR/$name.txt"
    if screen -S "$session" -X hardcopy -h "$ARTIFACT_DIR/$name.txt" \
      >/dev/null 2>&1 \
      && [[ -f "$ARTIFACT_DIR/$name.txt" ]]; then
      return 0
    fi
    tui_alive "$target" || return 1
    sleep 0.02
  done
  return 1
}

capture_utf8() {
  local target="$1"
  local name="$2"
  local session="${TUI_SCREEN[$target]:-}"
  local log_path="${TUI_UTF8_LOG[$target]:-}"
  [[ -n "$session" && -n "$log_path" && -f "$log_path" ]] || return 1
  screen -S "$session" -X logfile flush 0 >/dev/null 2>&1 || return 1
  python3 - "$log_path" "$ARTIFACT_DIR/$name.txt" <<'PY'
import pathlib
import re
import sys

raw = pathlib.Path(sys.argv[1]).read_bytes().decode("utf-8", errors="replace")
raw = re.sub(r"\x1b\][^\x07]*(?:\x07|\x1b\\)", "", raw)
raw = re.sub(r"\x1b[P^_].*?\x1b\\", "", raw, flags=re.DOTALL)
raw = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", raw)
raw = re.sub(r"\x1b[@-_]", "", raw)
text = "".join(
    character
    if character in "\n\r\t" or ord(character) >= 32
    else " "
    for character in raw
)
pathlib.Path(sys.argv[2]).write_text(text.replace("\r", "\n"), encoding="utf-8")
PY
}

wait_capture() {
  local target="$1"
  local pattern="$2"
  local name="$3"
  for _ in {1..240}; do
    if ! capture "$target" "$name"; then
      tui_alive "$target" || return 1
      sleep 0.05
      continue
    fi
    if rg -q "$pattern" "$ARTIFACT_DIR/$name.txt"; then
      return 0
    fi
    tui_alive "$target" || return 1
    sleep 0.25
  done
  return 1
}

message_page() {
  local session_id="$1"
  local output="$2"
  auth_curl "$BASE_URL/api/sessions/$session_id/messages?from_seq=0&limit=500" >"$output"
}

messages_contain() {
  local session_id="$1"
  local needle="$2"
  auth_curl \
    "$BASE_URL/api/sessions/$session_id/messages?from_seq=0&limit=500" \
    | python3 -c '
import json, sys
page = json.load(sys.stdin)
needle = sys.argv[1]
text = json.dumps(page.get("messages", []), ensure_ascii=False)
raise SystemExit(0 if needle in text else 1)
' "$needle"
}

wait_message() {
  local session_id="$1"
  local needle="$2"
  for _ in {1..320}; do
    if messages_contain "$session_id" "$needle" 2>/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

wait_execution_status() {
  local session_id="$1"
  local expected="$2"
  local output="$3"
  for _ in {1..320}; do
    if auth_curl "$BASE_URL/api/sessions/$session_id/execution" >"$output" 2>/dev/null \
      && python3 - "$output" "$expected" <<'PY'
import json
import sys

projection = json.load(open(sys.argv[1], encoding="utf-8"))
raise SystemExit(0 if projection.get("latest_status") == sys.argv[2] else 1)
PY
    then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

start_gateway() {
  env \
    COWD_CONFIG_HOME="$CONFIG_HOME" \
    COWD_LOG_STDERR=1 \
    HOME="$HOME_DIR" \
    "$BIN" gateway run >>"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  for _ in {1..240}; do
    if auth_curl "$BASE_URL/health" >/dev/null 2>&1; then
      return 0
    fi
    kill -0 "$GATEWAY_PID" >/dev/null 2>&1 || return 1
    sleep 0.25
  done
  return 1
}

stop_gateway() {
  if [[ -n "$GATEWAY_PID" ]]; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" >/dev/null 2>&1 || true
    GATEWAY_PID=""
  fi
}

start_tui() {
  local target="$1"
  local session_id="$2"
  local observer_id="$3"
  local width="$4"
  local height="$5"
  local screen_name="$SCREEN_PREFIX-$target"
  local driver_log="$ARTIFACT_DIR/$target-screen-driver.log"
  local utf8_log="$ARTIFACT_DIR/$target-screen-utf8.log"
  [[ -z "${TUI_SCREEN[$target]:-}" ]] \
    || fail "TUI PTY target $target is already active"
  (
    cd "$WORKSPACE"
    exec screen \
      -D -m \
      -L \
      -Logfile "$utf8_log" \
      -S "$screen_name" \
      -c /dev/null \
      -T xterm-256color \
      -U \
      -h 2000 \
      env \
        COWD_CONFIG_HOME="$CONFIG_HOME" \
        COWD_API_TOKEN="$TOKEN" \
        COWD_GATEWAY_URL="$BASE_URL" \
        COWD_TUI_OBSERVER_ID="$observer_id" \
        COWD_DISABLE_DAEMON_AUTOSTART=1 \
        HOME="$HOME_DIR" \
        TERM=xterm-256color \
        "$BIN" --yolo --model "$MODEL" --session "$session_id"
  ) >>"$driver_log" 2>&1 &
  TUI_SCREEN["$target"]="$screen_name"
  TUI_DRIVER_PID["$target"]=$!
  TUI_UTF8_LOG["$target"]="$utf8_log"
  for _ in {1..80}; do
    if screen -S "$screen_name" -Q windows >/dev/null 2>&1; then
      screen -S "$screen_name" -X logfile flush 0 >/dev/null 2>&1
      screen -S "$screen_name" -X width "$width" "$height" \
        >/dev/null 2>&1
      return 0
    fi
    tui_alive "$target" || return 1
    sleep 0.05
  done
  return 1
}

tui_alive() {
  local target="$1"
  local pid="${TUI_DRIVER_PID[$target]:-}"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

send_raw() {
  local target="$1"
  local bytes="$2"
  local session="${TUI_SCREEN[$target]:-}"
  [[ -n "$session" ]] || return 1
  screen -S "$session" -X stuff "$bytes" >/dev/null 2>&1
}

resize_tui() {
  local target="$1"
  local width="$2"
  local height="$3"
  local session="${TUI_SCREEN[$target]:-}"
  [[ -n "$session" ]] || return 1
  screen -S "$session" -X width "$width" "$height" >/dev/null 2>&1
}

tui_process_pid() {
  local target="$1"
  local driver_pid="${TUI_DRIVER_PID[$target]:-}"
  [[ -n "$driver_pid" ]] || return 1
  pgrep -P "$driver_pid" -x cowd | head -1
}

stop_tui() {
  local target="$1"
  local session="${TUI_SCREEN[$target]:-}"
  local pid="${TUI_DRIVER_PID[$target]:-}"
  [[ -n "$session" && -n "$pid" ]] || return 0
  screen -S "$session" -X stuff $'\003' >/dev/null 2>&1 || true
  for _ in {1..40}; do
    kill -0 "$pid" >/dev/null 2>&1 || break
    sleep 0.1
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    screen -S "$session" -X stuff $'\003' >/dev/null 2>&1 || true
    sleep 0.2
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    screen -S "$session" -X quit >/dev/null 2>&1 || true
  fi
  for _ in {1..20}; do
    kill -0 "$pid" >/dev/null 2>&1 || break
    sleep 0.05
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  for _ in {1..20}; do
    kill -0 "$pid" >/dev/null 2>&1 || break
    sleep 0.05
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -KILL "$pid" >/dev/null 2>&1 || true
  fi
  wait "$pid" >/dev/null 2>&1 || true
  unset 'TUI_SCREEN[$target]'
  unset 'TUI_DRIVER_PID[$target]'
  unset 'TUI_UTF8_LOG[$target]'
}

send_prompt() {
  local target="$1"
  local prompt="$2"
  send_raw "$target" "$prompt"
  send_raw "$target" $'\r'
}

for command in screen curl node python3 rg ss pgrep getconf sqlite3; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "$command is required for V584 TUI production acceptance" >&2
    exit 1
  }
done
[[ -x "$BIN" ]] || {
  echo "missing executable $BIN; build cowd with --features full first" >&2
  exit 1
}
mkdir -p "$ARTIFACT_DIR" "$CONFIG_HOME" "$HOME_DIR/.cowd" "$WORKSPACE/.cowd"
: >"$PROVIDER_LOG"
: >"$PROVIDER_STDOUT"
: >"$GATEWAY_LOG"
: >"$SUMMARY"
: >"$PERFORMANCE"
ss -ltn | rg -q ":($GATEWAY_PORT|$PROVIDER_PORT)\\b" \
  && fail "acceptance ports are already occupied"

node --check "$ROOT/scripts/fixtures/v584-tui-provider.mjs" \
  >"$ARTIFACT_DIR/provider-node-check.txt" 2>&1
[[ -z "$(git -C "$ROOT" status --porcelain)" ]] \
  || fail "production acceptance requires a clean committed source tree"
cargo test -p tui e10_ -- --test-threads=1 \
  >"$ARTIFACT_DIR/e10-fail-closed-unit-gates.txt" 2>&1
rg -q 'test result: ok\. 7 passed; 0 failed' \
  "$ARTIFACT_DIR/e10-fail-closed-unit-gates.txt" \
  || fail "E10 source gate did not execute exactly seven fail-closed tests"
"$BIN" --version >"$ARTIFACT_DIR/cowd-version.txt" 2>&1
python3 - \
  "$ARTIFACT_DIR/cowd-version.txt" \
  "$EXPECTED_GIT_SHA" \
  "$EXPECTED_BUILD_DATE" <<'PY'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
expected_sha, expected_date = sys.argv[2:]
assert re.search(r"(?m)^\s*Version\s+0\.9\.585\s*$", text), text
assert re.search(rf"(?m)^\s*Git SHA\s+{re.escape(expected_sha)}\s*$", text), text
assert re.search(rf"(?m)^\s*Build date\s+{re.escape(expected_date)}\s*$", text), text
PY
pass "binary reports version 0.9.588 from commit $EXPECTED_GIT_SHA built $EXPECTED_BUILD_DATE"

cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "$MODEL"
model_context_windows:
  $MODEL: 16384
providers:
  v584_acceptance:
    base_url: "http://127.0.0.1:$PROVIDER_PORT/v1"
    api_key: "local-fixture-key"
    protocol: "completions"
    models:
      - "$MODEL"
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: false
gateway:
  enabled: true
  session_reset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: $GATEWAY_PORT
      auth:
        enabled: true
        token: "$TOKEN"
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKSPACE/.cowd/config.yaml"

env \
  COWD_V584_PROVIDER_PORT="$PROVIDER_PORT" \
  COWD_V584_PROVIDER_LOG="$PROVIDER_LOG" \
  node "$ROOT/scripts/fixtures/v584-tui-provider.mjs" \
  >"$PROVIDER_STDOUT" 2>&1 &
PROVIDER_PID=$!
for _ in {1..120}; do
  curl -fsS "http://127.0.0.1:$PROVIDER_PORT/health" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$PROVIDER_PORT/health" >/dev/null \
  || fail "deterministic provider did not start"
start_gateway || fail "isolated Gateway did not become healthy"
pass "isolated provider and Gateway are healthy"

start_tui writer "$SESSION_A" "tui:v584-writer" 120 40
wait_capture writer "$MODEL" boot \
  || fail "writer TUI did not render the requested model"
rg -q '0\.9\.585' "$ARTIFACT_DIR/boot.txt" \
  || fail "writer TUI did not render version 0.9.588"
rg -q 'idle|ready' "$ARTIFACT_DIR/boot.txt" \
  || fail "writer TUI did not expose an idle/ready state"
capture_utf8 writer boot-utf8 \
  || fail "writer TUI UTF-8 transcript could not be captured"
rg -q 'ctx[[:space:]]+—|context[[:space:]]+—' "$ARTIFACT_DIR/boot-utf8.txt" \
  || fail "unknown new-session context metric was not rendered as —"
send_raw writer $'\020'
wait_capture writer ' Command Palette ' boot-palette \
  || fail "new-session action palette did not respond"
send_raw writer $'\033'
sleep 0.1
capture writer boot-palette-closed \
  || fail "new-session screen could not be captured after closing the palette"
rg -q ' Command Palette ' "$ARTIFACT_DIR/boot-palette-closed.txt" \
  && fail "Escape did not close the command palette"
resize_tui writer 90 32
sleep 0.2
capture writer boot-resized \
  || fail "new-session resized screen could not be captured"
rg -q "$MODEL" "$ARTIFACT_DIR/boot-resized.txt" \
  || fail "new-session resize lost the requested model"
resize_tui writer 120 40
pass "E1 new-session version/model/idle/unknown metrics and palette/resize interaction are visible"
curl -fsSN -H "Authorization: Bearer $TOKEN" \
  --max-time 90 "$BASE_URL/api/sessions/$SESSION_A/stream" \
  >"$SESSION_STREAM_LOG" 2>&1 &
SESSION_STREAM_PID=$!

tui_pid="$(tui_process_pid writer || true)"
[[ -n "$tui_pid" ]] || fail "writer TUI child process could not be identified"
ticks_before="$(awk '{print $14 + $15}' "/proc/$tui_pid/stat")"
idle_started_ms="$(monotonic_ms)"
sleep 2
ticks_after="$(awk '{print $14 + $15}' "/proc/$tui_pid/stat")"
idle_finished_ms="$(monotonic_ms)"
clock_ticks="$(getconf CLK_TCK)"
idle_cpu_percent="$(
  python3 -c '
import sys
before, after, hz, started, finished = map(float, sys.argv[1:])
elapsed = max(0.001, (finished - started) / 1000.0)
print(f"{((after - before) / hz) / elapsed * 100.0:.3f}")
' "$ticks_before" "$ticks_after" "$clock_ticks" "$idle_started_ms" "$idle_finished_ms"
)"
printf 'idle_cpu_percent\t%s\n' "$idle_cpu_percent" >>"$PERFORMANCE"
python3 -c 'import sys; assert float(sys.argv[1]) < 5.0, sys.argv[1]' "$idle_cpu_percent" \
  || fail "idle TUI CPU is ${idle_cpu_percent}%, above the 5% production gate"
pass "E9 idle event-driven redraw CPU is ${idle_cpu_percent}%"

send_prompt writer "V584_TURN_1 remember $NONCE_A and acknowledge it exactly"
wait_message "$SESSION_A" "V584-TURN1-ACK nonce=$NONCE_A" \
  || fail "turn 1 terminal answer was not durably stored"
wait_capture writer "V584-TURN1-ACK nonce=$NONCE_A" turn1 \
  || fail "turn 1 answer was not rendered in TUI"
rg -q '[[:digit:]][[:digit:]]* queued:' "$ARTIFACT_DIR/turn1.txt" \
  && fail "terminal turn still rendered its already-consumed input as queued"
message_page "$SESSION_A" "$ARTIFACT_DIR/messages-after-turn1.json"
pass "turn 1 reached provider, durable history and terminal rendering"

stop_tui writer
start_tui writer "$SESSION_A" "tui:v584-writer-restart" 120 40
wait_capture writer "V584-TURN1-ACK nonce=$NONCE_A" restart-history \
  || fail "restart did not hydrate the prior assistant answer"
pass "E2 restart hydrated durable history before the next turn"

send_prompt writer "V584_TURN_2 recall the prior nonce from actual provider history"
wait_message "$SESSION_A" "V584-TURN2-ACK recalled=$NONCE_A provider_user_history=2" \
  || fail "turn 2 provider request did not contain the actual prior user history"
wait_capture writer "V584-TURN2-ACK recalled=$NONCE_A" turn2 \
  || fail "turn 2 answer was not rendered"
[[ "$(rg -o 'V584-TURN2-ACK' "$ARTIFACT_DIR/turn2.txt" | wc -l)" == "1" ]] \
  || fail "turn 2 answer rendered more than once in the live transcript"
pass "E2 multi-turn causal history is current and not shifted by one answer"

start_tui long-wrap "$SESSION_LONG" "tui:v584-long-wrap" 120 40
wait_capture long-wrap "$MODEL" long-wrap-boot \
  || fail "independent long-response session did not start"
send_prompt long-wrap "V584_LONG_WRAP render the deterministic width fixture"
wait_message "$SESSION_LONG" "END-OF-LONG-RESPONSE" \
  || fail "long response was not durably completed"
for size in 40x24 60x30 90x32 120x40 200x52; do
  width="${size%x*}"
  height="${size#*x}"
  resize_tui long-wrap "$width" "$height"
  sleep 0.35
  send_raw long-wrap $'\033'
  send_raw long-wrap $'\033[F'
  sleep 0.15
  capture long-wrap "width-$size-tail" \
    || fail "long-response tail could not be captured at terminal size $size"
  cp "$ARTIFACT_DIR/width-$size-tail.txt" "$ARTIFACT_DIR/width-$size.txt"
  rg -q 'END-OF-LONG-RESPONSE' "$ARTIFACT_DIR/width-$size.txt" \
    || fail "long-response tail disappeared at terminal size $size"
  [[ "$(rg -o 'END-OF-LONG-RESPONSE' "$ARTIFACT_DIR/width-$size.txt" | wc -l)" == "1" ]] \
    || fail "long response rendered more than once at terminal size $size"
  send_raw long-wrap $'\033[H'
  sleep 0.15
  capture long-wrap "width-$size-head" \
    || fail "long-response head could not be captured at terminal size $size"
  rg -q 'V584-LONG-BEGIN' "$ARTIFACT_DIR/width-$size-head.txt" \
    || fail "long-response Home navigation could not reach its head at $size"
  rg -q 'ROW-00.*中文|ROW-00' "$ARTIFACT_DIR/width-$size-head.txt" \
    || fail "long-response first CJK/URL/JSON/code matrix row was not reachable at $size"
  send_raw long-wrap $'\033[F'
  sleep 0.15
  capture long-wrap "width-$size-return-tail" \
    || fail "long-response return tail could not be captured at terminal size $size"
  rg -q 'ROW-47|END-OF-LONG-RESPONSE' "$ARTIFACT_DIR/width-$size-return-tail.txt" \
    || fail "long-response End navigation did not return to the canonical tail at $size"
done
stop_tui long-wrap
pass "E4 long Chinese/URL/JSON/code/emoji content has reachable head/tail and stable scroll bounds at all target widths"

resize_tui writer 120 40
slow_started_ms="$(monotonic_ms)"
send_prompt writer "V584_SLOW_STATUS prove active execution is visible"
partial_visible=0
for _ in {1..100}; do
  capture writer slow-in-progress \
    || fail "slow-turn in-progress screen could not be captured"
  if rg -q 'V584-SLOW-BEGIN' "$ARTIFACT_DIR/slow-in-progress.txt"; then
    partial_visible=1
    break
  fi
  sleep 0.05
done
[[ "$partial_visible" == "1" ]] || fail "slow turn emitted no visible partial response"
slow_partial_ms="$(( $(monotonic_ms) - slow_started_ms ))"
printf 'input_to_first_visible_partial_ms\t%s\n' "$slow_partial_ms" >>"$PERFORMANCE"
(( slow_partial_ms < 2500 )) \
  || fail "input-to-first-visible-partial latency was ${slow_partial_ms}ms"
rg -q 'V584-SLOW-BEGIN' "$ARTIFACT_DIR/slow-in-progress.txt" \
  || fail "slow turn emitted no visible partial response"
if rg -q 'V584-SLOW-END' "$ARTIFACT_DIR/slow-in-progress.txt"; then
  fail "slow-turn evidence was captured only after completion"
fi
rg -q 'submitting|context|model|thinking|finalizing|running|streaming|响应中|执行中' \
  "$ARTIFACT_DIR/slow-in-progress.txt" \
  || fail "slow turn had no visible active status"
wait_message "$SESSION_A" "V584-SLOW-END" \
  || fail "slow streaming response did not complete"
wait_capture writer 'V584-SLOW-END' slow-complete \
  || fail "slow streaming completion was not rendered"
capture_utf8 writer slow-complete-utf8 \
  || fail "slow completion UTF-8 transcript could not be captured"
auth_curl "$BASE_URL/api/sessions/$SESSION_A/execution" \
  >"$ARTIFACT_DIR/slow-execution-index.json"
slow_execution_id="$(
  python3 - "$ARTIFACT_DIR/slow-execution-index.json" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], encoding="utf-8"))
execution_id = value.get("latest_execution_id")
assert isinstance(execution_id, str) and execution_id, value
print(execution_id)
PY
)"
auth_curl "$BASE_URL/api/runtime/executions/$slow_execution_id?detail_scope=full" \
  >"$ARTIFACT_DIR/slow-execution-projection.json"
rg -q "$MODEL" "$ARTIFACT_DIR/slow-complete.txt" \
  || fail "effective model disappeared after a real turn"
python3 - \
  "$ARTIFACT_DIR/slow-execution-projection.json" \
  "$ARTIFACT_DIR/slow-complete.txt" \
  "$MODEL" <<'PY'
import json
import sys

projection = json.load(open(sys.argv[1], encoding="utf-8"))
screen = open(sys.argv[2], encoding="latin-1").read()
expected_model = sys.argv[3]
live = projection["live"]
usage = live["context_usage"]
metrics = live["metrics"]

def fmt_tokens(value):
    value = int(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 10_000:
        return f"{value / 1_000:.1f}k"
    if value >= 1_000:
        return f"{value // 1_000}k"
    return str(value)

assert live["status"] == "complete", live
assert usage["model"] == expected_model, usage
assert usage["window_tokens"] == 16_384, usage
assert usage["window_source"] == "configured", usage
assert usage["input_source"] == "provider_actual", usage
used = int(usage["input_tokens"])
window = int(usage["window_tokens"])
remaining = int(usage["remaining_tokens"])
percent = int(usage["usage_percent_bp"]) / 100
assert remaining == window - used, usage
assert f"ctx {fmt_tokens(used)} /{fmt_tokens(window)} {percent:.0f}% rem {fmt_tokens(remaining)}" in screen, (
    usage,
    screen,
)
assert (
    f"in {fmt_tokens(metrics['input_tokens'])} · "
    f"out {fmt_tokens(metrics['output_tokens'])} · "
    f"total {fmt_tokens(metrics['total_tokens'])}"
) in screen, (metrics, screen)
assert (
    f"tools {metrics['tool_calls']} · "
    f"memory {metrics['memory_recalls']}/{metrics['memory_evidence']} · "
    f"approvals {metrics['approvals']} · files {metrics['files_touched']}"
) in screen, (metrics, screen)
PY
pass "E6 active progress/model status is visible; terminal model/context/token/tool/memory/approval metrics exactly match the canonical execution projection"
pass "E9 input-to-first-visible-partial latency is ${slow_partial_ms}ms"

start_tui observer "$SESSION_A" "tui:v584-observer" 120 40
wait_capture observer 'V584-SLOW-END' observer-attached \
  || fail "second Surface did not hydrate the shared durable session"
WEBUI_OBSERVER_ID="webui:v584-browser-$$"
auth_curl \
  -H 'x-cowd-surface-id: webui' \
  -H "x-cowd-observer-id: $WEBUI_OBSERVER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$SESSION_A/attach" \
  -d '{"surface":"webui","role":"writer"}' \
  >"$ARTIFACT_DIR/webui-writer-attach.json"
auth_curl \
  -H 'x-cowd-surface-id: webui' \
  -H "x-cowd-observer-id: $WEBUI_OBSERVER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/runtime/session-leases/acquire" \
  -d "{\"session_id\":\"$SESSION_A\",\"mode\":\"collaborative\"}" \
  >"$ARTIFACT_DIR/webui-writer-lease.json"
curl -fsSN \
  -H "Authorization: Bearer $TOKEN" \
  -H 'x-cowd-surface-id: webui' \
  -H "x-cowd-observer-id: $WEBUI_OBSERVER_ID" \
  --max-time 90 "$BASE_URL/api/sessions/$SESSION_A/stream" \
  >"$WEBUI_STREAM_LOG" 2>&1 &
WEBUI_STREAM_PID=$!

send_prompt writer "V584_OBSERVER_SYNC from TUI writer to WebUI observer"
wait_message "$SESSION_A" "V584-TUI-TO-WEBUI-ACK" \
  || fail "TUI-originated turn was not durably completed"
for _ in {1..240}; do
  rg -q 'V584-TUI-TO-WEBUI-ACK' "$WEBUI_STREAM_LOG" && break
  sleep 0.25
done
rg -q 'V584-TUI-TO-WEBUI-ACK' "$WEBUI_STREAM_LOG" \
  || fail "the real WebUI session stream did not observe TUI-originated progress"

auth_curl \
  -H 'x-cowd-surface-id: webui' \
  -H "x-cowd-observer-id: $WEBUI_OBSERVER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$SESSION_A/messages" \
  -d "{\"content\":\"V584_OBSERVER_SYNC from WebUI writer to TUI observer\",\"resource_ids\":[],\"idempotency_key\":\"webui-v584-$$\"}" \
  >"$ARTIFACT_DIR/webui-send-receipt.json"
wait_message "$SESSION_A" "V584-WEBUI-TO-TUI-ACK" \
  || fail "WebUI-originated turn was not durably completed"
wait_capture observer 'V584-WEBUI-TO-TUI-ACK' webui-to-tui \
  || fail "the real TUI observer did not display the WebUI-originated answer"
for _ in {1..240}; do
  rg -q 'V584-WEBUI-TO-TUI-ACK' "$WEBUI_STREAM_LOG" && break
  sleep 0.25
done
rg -q 'V584-WEBUI-TO-TUI-ACK' "$WEBUI_STREAM_LOG" \
  || fail "the WebUI stream did not observe its own canonical terminal result"

auth_curl \
  -H 'x-cowd-surface-id: webui' \
  -H "x-cowd-observer-id: $WEBUI_OBSERVER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/runtime/session-leases/release" \
  -d "{\"session_id\":\"$SESSION_A\"}" \
  >"$ARTIFACT_DIR/webui-writer-release.json"
auth_curl \
  -H 'x-cowd-surface-id: webui' \
  -H "x-cowd-observer-id: $WEBUI_OBSERVER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$SESSION_A/detach" \
  -d '{"surface":"webui"}' \
  >"$ARTIFACT_DIR/webui-writer-detach.json"
kill "$WEBUI_STREAM_PID" >/dev/null 2>&1 || true
wait "$WEBUI_STREAM_PID" >/dev/null 2>&1 || true
WEBUI_STREAM_PID=""

WEBUI_READER_ID="webui:v584-reader-$$"
auth_curl \
  -H 'x-cowd-surface-id: webui' \
  -H "x-cowd-observer-id: $WEBUI_READER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$SESSION_A/attach" \
  -d '{"surface":"webui","role":"reader"}' \
  >"$ARTIFACT_DIR/webui-reader-attach.json"
reader_mutation_status="$(
  command curl -sS \
    -o "$ARTIFACT_DIR/webui-reader-mutation.json" \
    -w '%{http_code}' \
    -H "Authorization: Bearer $TOKEN" \
    -H 'x-cowd-surface-id: webui' \
    -H "x-cowd-observer-id: $WEBUI_READER_ID" \
    -H 'content-type: application/json' \
    -X POST "$BASE_URL/api/sessions/$SESSION_A/messages" \
    -d '{"content":"reader mutation must be denied","resource_ids":[],"idempotency_key":"webui-reader-denied"}'
)"
[[ "$reader_mutation_status" == "403" ]] \
  || fail "WebUI reader mutation was not rejected with HTTP 403"
auth_curl \
  -H 'x-cowd-surface-id: webui' \
  -H "x-cowd-observer-id: $WEBUI_READER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$SESSION_A/detach" \
  -d '{"surface":"webui"}' \
  >"$ARTIFACT_DIR/webui-reader-detach.json"
send_prompt writer "V584_OBSERVER_SYNC after WebUI disconnect"
wait_message "$SESSION_A" "V584-WEBUI-DISCONNECT-ACK" \
  || fail "disconnecting the WebUI observer interrupted the TUI session"
pass "E8 real TUI plus a WebUI-protocol observer share terminal progress, enforce reader policy and disconnect independently"

auth_curl "$BASE_URL/api/runtime/session-leases" \
  >"$ARTIFACT_DIR/session-a-collaborative-leases.json"
python3 - "$ARTIFACT_DIR/session-a-collaborative-leases.json" "$SESSION_A" <<'PY'
import json
import sys

projection = json.load(open(sys.argv[1], encoding="utf-8"))
session_id = sys.argv[2]
leases = [
    lease for lease in projection.get("leases", [])
    if lease.get("session_id") == session_id
]
assert len(leases) == 2, f"expected exactly two live Surface leases, got {leases}"
assert all(lease.get("mode") == "collaborative" for lease in leases), leases
owners = {lease.get("owner", "") for lease in leases}
assert any(owner.endswith(":observer:tui:v584-writer-restart") for owner in owners), owners
assert any(owner.endswith(":observer:tui:v584-observer") for owner in owners), owners
PY
send_prompt writer "V584_OBSERVER_SYNC publish one answer to both terminals"
writer_partial=0
observer_partial=0
for _ in {1..80}; do
  capture writer writer-sync-progress \
    || fail "writer collaborative progress screen could not be captured"
  capture observer observer-sync-progress \
    || fail "observer collaborative progress screen could not be captured"
  if rg -q 'V584-OBSERVER-SYNC-BEGIN' "$ARTIFACT_DIR/writer-sync-progress.txt"; then
    writer_partial=1
  fi
  if rg -q 'V584-OBSERVER-SYNC-BEGIN' "$ARTIFACT_DIR/observer-sync-progress.txt"; then
    observer_partial=1
  fi
  if [[ "$writer_partial" == "1" && "$observer_partial" == "1" ]]; then
    break
  fi
  sleep 0.05
done
[[ "$writer_partial" == "1" && "$observer_partial" == "1" ]] \
  || fail "collaborative Surfaces did not both render the same in-progress delta"
if rg -q 'V584-OBSERVER-SYNC-ACK' \
  "$ARTIFACT_DIR/writer-sync-progress.txt" \
  "$ARTIFACT_DIR/observer-sync-progress.txt"; then
  fail "collaborative progress evidence was captured only after terminal completion"
fi
wait_message "$SESSION_A" "V584-OBSERVER-SYNC-ACK" \
  || fail "writer sync answer was not stored"
wait_capture writer 'V584-OBSERVER-SYNC-ACK' writer-sync \
  || fail "writer did not show sync answer"
wait_capture observer 'V584-OBSERVER-SYNC-ACK' observer-sync \
  || fail "observer did not receive the live answer"
pass "E8 two collaborative Surfaces share one session and see matching pre-terminal progress plus one canonical terminal"

start_tui session-b "$SESSION_B" "tui:v584-session-b" 90 32
wait_capture session-b "$MODEL" session-b-boot \
  || fail "second independent session did not start"
send_prompt session-b "V584_TURN_1 remember $NONCE_B and acknowledge it exactly"
wait_message "$SESSION_B" "V584-TURN1-ACK nonce=$NONCE_B" \
  || fail "second session did not complete independently"
message_page "$SESSION_A" "$ARTIFACT_DIR/messages-session-a-before-reconnect.json"
message_page "$SESSION_B" "$ARTIFACT_DIR/messages-session-b.json"
rg -q "$NONCE_B" "$ARTIFACT_DIR/messages-session-a-before-reconnect.json" \
  && fail "session B content leaked into session A"
rg -q "$NONCE_A" "$ARTIFACT_DIR/messages-session-b.json" \
  && fail "session A content leaked into session B"
pass "E8 simultaneous sessions remain causally and visually isolated"

stop_gateway
wait_capture writer \
  'reconnect:[[:digit:]]+@|Gateway session stream interrupted; reconnecting with durable hydration' \
  e7-gateway-down \
  || fail "Gateway loss did not become visible in the active TUI"
start_gateway || fail "Gateway did not restart on the same durable store"
send_prompt writer "V584_OBSERVER_SYNC after reconnect"
wait_message "$SESSION_A" "V584-RECONNECT-ACK" \
  || fail "TUI did not recover mutation capability after Gateway restart"
wait_capture observer 'V584-RECONNECT-ACK' observer-reconnected \
  || fail "observer did not recover after Gateway restart"
pass "E7 Gateway restart preserves session and restores both Surfaces"

send_prompt writer "V584_VALID_DSML invoke one exposed read-only resource-list tool"
wait_message "$SESSION_A" "V584-DSML-TOOL-COMPLETE" \
  || fail "valid DSML did not become a real tool call followed by a terminal answer"
wait_capture writer 'V584-DSML-TOOL-COMPLETE' valid-dsml \
  || fail "valid DSML tool completion was not rendered"
capture_utf8 writer valid-dsml-utf8 \
  || fail "valid DSML UTF-8 transcript could not be captured"
message_page "$SESSION_A" "$ARTIFACT_DIR/valid-dsml-messages.json"
auth_curl "$BASE_URL/api/sessions/$SESSION_A/projection" \
  >"$ARTIFACT_DIR/valid-dsml-projection.json"
python3 - \
  "$ARTIFACT_DIR/valid-dsml-messages.json" \
  "$ARTIFACT_DIR/valid-dsml-projection.json" \
  "$ARTIFACT_DIR/valid-dsml.txt" \
  "$ARTIFACT_DIR/valid-dsml-utf8.txt" <<'PY'
import json
import sys

messages = json.load(open(sys.argv[1], encoding="utf-8")).get("messages", [])
projection = json.load(open(sys.argv[2], encoding="utf-8"))
screen = open(sys.argv[3], encoding="latin-1").read()
utf8_transcript = open(sys.argv[4], encoding="utf-8").read()

tool_uses = []
tool_results = []
for message in messages:
    blocks = message.get("blocks", [])
    for block in blocks if isinstance(blocks, list) else []:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "tool_use":
            tool_uses.append(block)
        elif block.get("type") == "tool_result":
            tool_results.append(block)

assert len(tool_uses) == 1, f"expected one durable tool_use, got {tool_uses}"
assert len(tool_results) == 1, f"expected one durable tool_result, got {tool_results}"
tool_use = tool_uses[0]
tool_result = tool_results[0]
instance_id = tool_use.get("cowd_tool_instance_id")
assert instance_id, tool_use
assert tool_result.get("cowd_tool_instance_id") == instance_id, (tool_use, tool_result)
assert tool_result.get("is_error") is False, tool_result

name = str(tool_use.get("name") or "")
assert name, tool_use
summary = projection.get("tool_summary", {})
timeline = projection.get("tool_timeline", [])
assert summary.get("count") == 1, summary
assert summary.get("by_name", {}).get(name) == 1, summary
assert len(timeline) == 1, timeline
assert timeline[0].get("tool_instance_id") == instance_id, timeline
assert timeline[0].get("status") == "completed", timeline
assert name in screen, f"missing compact tool card name for {name}"
assert "exit:0" in screen, "compact tool card did not show successful completion"
assert "🔧" in utf8_transcript, "compact tool card did not emit its tool icon"
assert "✅" in utf8_transcript, "compact tool card did not emit its completion icon"
PY
pass "E5 valid DSML becomes a governed tool event without protocol text"

invalid_provider_requests_before="$(
  python3 - "$PROVIDER_LOG" <<'PY'
import json
import sys

print(sum(
    1
    for line in open(sys.argv[1], encoding="utf-8")
    if line.strip()
    and isinstance(json.loads(line).get("messages"), list)
))
PY
)"
send_prompt writer "V584_INVALID_DSML verify fail-closed protocol handling"
wait_message "$SESSION_A" "Execution blocked" \
  || fail "repeated invalid DSML produced no causally-linked blocked terminal message"
wait_execution_status \
  "$SESSION_A" error "$ARTIFACT_DIR/invalid-dsml-execution.json" \
  || fail "materialized invalid DSML terminal did not retain the canonical Error state"
wait_capture writer 'Execution blocked' invalid-dsml \
  || fail "blocked invalid-DSML outcome was not visible in the TUI"
message_page "$SESSION_A" "$ARTIFACT_DIR/messages-final.json"
if rg -q 'DSML｜｜tool_calls|DSML｜｜invoke' \
  "$ARTIFACT_DIR"/boot.txt \
  "$ARTIFACT_DIR"/turn*.txt \
  "$ARTIFACT_DIR"/width-*.txt \
  "$ARTIFACT_DIR"/slow-*.txt \
  "$ARTIFACT_DIR"/observer-*.txt \
  "$ARTIFACT_DIR"/writer-*.txt \
  "$ARTIFACT_DIR"/valid-dsml.txt \
  "$ARTIFACT_DIR"/invalid-dsml.txt \
  "$ARTIFACT_DIR"/messages-final.json; then
  fail "raw DSML protocol text leaked into durable/UI output"
fi
pass "E5 repeated malformed/unexposed DSML fails closed as Error without raw protocol leakage"

python3 - \
  "$ARTIFACT_DIR/messages-final.json" \
  "$PROVIDER_LOG" \
  "$NONCE_A" \
  "$invalid_provider_requests_before" <<'PY'
import json
import sys

message_path, provider_path, nonce, invalid_start = sys.argv[1:]
invalid_start = int(invalid_start)
page = json.load(open(message_path, encoding="utf-8"))
messages = page.get("messages", [])
ids = [message.get("id") for message in messages]
seqs = [message.get("sequence") for message in messages]
assert len(ids) == len(set(ids)), "durable message ids are not unique"
assert seqs == list(range(len(seqs))), f"message sequences are not contiguous: {seqs}"
rendered = json.dumps(messages, ensure_ascii=False)
assert rendered.count(f"V584-TURN1-ACK nonce={nonce}") == 1, "turn 1 answer duplicated"
assert rendered.count(f"V584-TURN2-ACK recalled={nonce}") == 1, "turn 2 answer duplicated"

def message_text(message):
    return "\n".join(
        block.get("text", "")
        for block in message.get("blocks", [])
        if isinstance(block, dict) and isinstance(block.get("text"), str)
    )

invalid_users = [
    message for message in messages
    if message.get("role") == "user"
    and "V584_INVALID_DSML" in message_text(message)
]
assert len(invalid_users) == 1, f"invalid ingress was not committed exactly once: {invalid_users}"
invalid_user = invalid_users[0]
invalid_turn_id = invalid_user["blocks"][0].get("cowd_turn_id")
invalid_ingress_id = invalid_user["blocks"][0].get("cowd_turn_ingress_message_id")
assert invalid_turn_id and invalid_ingress_id == invalid_user.get("id"), invalid_user
invalid_assistants = [
    message for message in messages
    if message.get("role") == "assistant"
    and any(
        isinstance(block, dict)
        and block.get("cowd_turn_id") == invalid_turn_id
        and block.get("cowd_turn_ingress_message_id") == invalid_ingress_id
        for block in message.get("blocks", [])
    )
]
assert len(invalid_assistants) == 1, (
    f"invalid turn did not produce exactly one causally-linked terminal: {invalid_assistants}"
)
blocked_text = message_text(invalid_assistants[0])
assert "Execution blocked" in blocked_text, blocked_text
assert "V584-DSML-TOOL-COMPLETE" not in blocked_text, blocked_text

requests = [
    record
    for line in open(provider_path, encoding="utf-8")
    if line.strip()
    for record in [json.loads(line)]
    if isinstance(record.get("messages"), list)
]
turn2 = next(
    record
    for record in requests
    if any("V584_TURN_2" in item.get("text", "") for item in record["messages"])
)
user_texts = [
    item["text"]
    for item in turn2["messages"]
    if item.get("role") == "user"
    and not item.get("text", "").startswith("## Runtime context data\n")
]
assert len(user_texts) == 2, f"turn 2 provider user history is not exact: {user_texts}"
assert nonce in user_texts[0], "turn 1 nonce missing from provider history"
assert "V584_TURN_2" in user_texts[1], "current turn missing from provider request"

valid_index = next(
    index
    for index, record in enumerate(requests)
    if any("V584_VALID_DSML" in item.get("text", "") for item in record["messages"])
)
assert requests[valid_index]["exposed_tools"], "valid DSML request exposed no tools"
assert any(
    any(item.get("role") == "tool" for item in record["messages"])
    for record in requests[valid_index + 1 :]
), "valid DSML never produced a provider-visible tool result"

invalid_requests = [
    record for record in requests[invalid_start:]
    if any(
        "V584_INVALID_DSML" in item.get("text", "")
        for item in record["messages"]
    )
]
assert len(invalid_requests) == 2, (
    f"invalid turn must make exactly the initial and one recovery request: {len(invalid_requests)}"
)
for record in invalid_requests:
    assert sum(
        item.get("role") == "user"
        and item.get("text", "").strip()
            == "V584_INVALID_DSML verify fail-closed protocol handling"
        for item in record["messages"]
    ) == 1, "current invalid ingress is missing or duplicated in a provider request"
    system_text = "\n".join(
        item.get("text", "")
        for item in record["messages"]
        if item.get("role") == "system"
    )
    assert (
        '"intent_preview":"V584_INVALID_DSML verify fail-closed protocol handling"'
        in system_text
    ), "runtime execution decision is not bound to the current invalid objective"
assert any(
    item.get("role") == "user"
    and item.get("text", "").startswith("## Runtime context data\n")
    and "source_id: runtime-provider-recovery:" in item.get("text", "")
    and "Never print tool-protocol markup as prose" in item.get("text", "")
    for item in invalid_requests[1]["messages"]
), "the second invalid request did not carry the governed recovery directive"
PY
pass "durable ids/sequences and both provider attempts preserve exact current-turn causality"

auth_curl "$BASE_URL/api/sessions/$SESSION_A/execution" \
  >"$ARTIFACT_DIR/invalid-dsml-execution-final.json"
python3 - "$ARTIFACT_DIR/invalid-dsml-execution-final.json" <<'PY'
import json
import sys

execution = json.load(open(sys.argv[1], encoding="utf-8"))
assert execution.get("latest_status") == "error", execution
assert execution.get("terminal_ref"), execution
PY
pass "durable materialization cannot reclassify the blocked execution as Complete"

auth_curl "$BASE_URL/api/sessions/$SESSION_A/projection" \
  >"$ARTIFACT_DIR/session-a-projection.json"
auth_curl "$BASE_URL/api/sessions/$SESSION_A/execution" \
  >"$ARTIFACT_DIR/session-a-execution-index.json"
auth_curl "$BASE_URL/api/runtime/status" \
  >"$ARTIFACT_DIR/runtime-status.json"

stop_tui session-b
stop_tui observer
stop_tui writer
stop_gateway
pass "all core terminal clients exited through their normal shutdown path"

OLD_SESSION_ID="$SESSION_A"
SOURCE_SESSION_DB="$CONFIG_HOME/storage/session.sqlite"
[[ -f "$SOURCE_SESSION_DB" ]] \
  || fail "isolated acceptance history fixture is unavailable at $SOURCE_SESSION_DB"
CORE_CONFIG_FILE="$CONFIG_HOME/config.yaml"
OLD_CONFIG_HOME="$RUNTIME_DIR/old-config"
OLD_HOME_DIR="$RUNTIME_DIR/old-home"
OLD_WORKSPACE="$RUNTIME_DIR/old-workspace"
mkdir -p "$OLD_CONFIG_HOME/storage" "$OLD_HOME_DIR/.cowd" "$OLD_WORKSPACE/.cowd"
cp "$CORE_CONFIG_FILE" "$OLD_CONFIG_HOME/config.yaml"
cp "$CORE_CONFIG_FILE" "$OLD_HOME_DIR/.cowd/config.yaml"
cp "$CORE_CONFIG_FILE" "$OLD_WORKSPACE/.cowd/config.yaml"
sqlite3 "$SOURCE_SESSION_DB" \
  ".backup '$OLD_CONFIG_HOME/storage/session.sqlite'"
CONFIG_HOME="$OLD_CONFIG_HOME"
HOME_DIR="$OLD_HOME_DIR"
WORKSPACE="$OLD_WORKSPACE"
start_gateway || fail "old-session clone Gateway did not become healthy"

message_page "$OLD_SESSION_ID" "$ARTIFACT_DIR/old-session-messages-before.json"
python3 - "$ARTIFACT_DIR/old-session-messages-before.json" <<'PY'
import json
import sys

page = json.load(open(sys.argv[1], encoding="utf-8"))
messages = page.get("messages", [])
assert page.get("total", 0) >= 2, f"isolated history fixture is unexpectedly empty: {page}"
assert len(messages) == page.get("total"), "the fixture must fit one deterministic page"
sequences = [item.get("sequence") for item in messages]
assert sequences == list(range(sequences[0], sequences[0] + len(sequences))), sequences
assert len({item.get("id") for item in messages}) == len(messages)
PY
old_fragment="$(
  python3 - "$ARTIFACT_DIR/old-session-messages-before.json" <<'PY'
import json
import re
import sys

messages = json.load(open(sys.argv[1], encoding="utf-8")).get("messages", [])
for message in reversed(messages):
    if message.get("role") != "assistant":
        continue
    text = json.dumps(message.get("blocks", []), ensure_ascii=False)
    tokens = re.findall(r"[\w\u3400-\u9fff-]{8,}", text)
    if tokens:
        print(tokens[-1][-24:])
        break
PY
)"
[[ -n "$old_fragment" ]] || fail "old-session assistant fragment could not be derived"

auth_curl \
  -H 'x-cowd-surface-id: tui' \
  -H 'x-cowd-observer-id: tui:v584-old-blocker' \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$OLD_SESSION_ID/attach" \
  -d '{"surface":"tui","role":"writer"}' \
  >"$ARTIFACT_DIR/old-session-blocker-attach.json"
rg -q '"ok":true' "$ARTIFACT_DIR/old-session-blocker-attach.json" \
  || fail "old-session clone blocker attachment failed"
auth_curl \
  -H 'x-cowd-surface-id: tui' \
  -H 'x-cowd-observer-id: tui:v584-old-blocker' \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/runtime/session-leases/acquire" \
  -d "{\"session_id\":\"$OLD_SESSION_ID\",\"mode\":\"exclusive\"}" \
  >"$ARTIFACT_DIR/old-session-blocker-lease.json"
rg -q '"ok":true' "$ARTIFACT_DIR/old-session-blocker-lease.json" \
  || fail "old-session clone blocker lease failed"

provider_requests_before_old="$(wc -l <"$PROVIDER_LOG")"
start_tui old-reader "$OLD_SESSION_ID" "tui:v584-old-reader" 120 40
wait_capture old-reader 'read-only|lease unavailable|writer lease' old-session-screen \
  || fail "old session did not attach in visibly non-mutating mode"
sleep 2
capture old-reader old-session-hydrated \
  || fail "old-session hydrated screen could not be captured"
rg -Fq "$old_fragment" "$ARTIFACT_DIR/old-session-hydrated.txt" \
  || fail "old-session durable assistant tail was not rendered by TUI"
message_page "$OLD_SESSION_ID" "$ARTIFACT_DIR/old-session-messages-after.json"
cmp -s \
  "$ARTIFACT_DIR/old-session-messages-before.json" \
  "$ARTIFACT_DIR/old-session-messages-after.json" \
  || fail "read-only old-session attach changed durable messages"
provider_requests_after_old="$(wc -l <"$PROVIDER_LOG")"
[[ "$provider_requests_before_old" == "$provider_requests_after_old" ]] \
  || fail "read-only old-session attach unexpectedly called the provider"
stop_tui old-reader
pass "E3 isolated current-run durable clone cold-starts read-only with no execution or message mutation"

stop_gateway

sqlite3 "$OLD_CONFIG_HOME/storage/session.sqlite" <<SQL
PRAGMA foreign_keys = ON;
INSERT INTO sessions (
  session_id, platform, chat_id, user_id, model, created_at, last_activity,
  message_count, reset_policy, metadata_json, input_tokens, output_tokens,
  estimated_cost_usd, status, created_at_ms, updated_at_ms
)
SELECT
  '$SESSION_10K', platform, '$SESSION_10K', user_id, '$MODEL',
  created_at, last_activity, 10000, reset_policy, metadata_json,
  500000, 500000, 0.0, 'active', created_at_ms, updated_at_ms
FROM sessions
WHERE session_id = '$OLD_SESSION_ID';

WITH RECURSIVE seq(n) AS (
  VALUES(0)
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 9999
)
INSERT INTO messages (
  stable_message_id, session_id, sequence, role, content_json, blocks_count,
  tool_use_id, tool_name, token_usage_json, created_at_ms
)
SELECT
  printf('v584-10k-%05d-$$', n),
  '$SESSION_10K',
  n,
  CASE WHEN n % 2 = 0 THEN 'user' ELSE 'assistant' END,
  json_array(json_object(
    'type', 'text',
    'text', printf(
      'V584-10K-%s-%05d durable history payload 中文 wrapping search',
      CASE
        WHEN n = 0 THEN 'EARLY'
        WHEN n = 9999 THEN 'TAIL'
        ELSE 'ROW'
      END,
      n
    )
  )),
  1,
  NULL,
  NULL,
  CASE
    WHEN n % 2 = 1 THEN json_object(
      'input_tokens', 50,
      'output_tokens', 50,
      'cache_creation_input_tokens', 0,
      'cache_read_input_tokens', 0
    )
    ELSE NULL
  END,
  1700000000000 + n
FROM seq;
SQL
sqlite3 "$OLD_CONFIG_HOME/storage/session.sqlite" \
  "SELECT message_count || ':' || (SELECT COUNT(*) FROM messages WHERE session_id = '$SESSION_10K') FROM sessions WHERE session_id = '$SESSION_10K';" \
  >"$ARTIFACT_DIR/10k-sqlite-count.txt"
rg -qx '10000:10000' "$ARTIFACT_DIR/10k-sqlite-count.txt" \
  || fail "10k durable session fixture was not materialized exactly"

start_gateway || fail "10k-session clone Gateway did not become healthy"
auth_curl \
  -H 'x-cowd-surface-id: tui' \
  -H 'x-cowd-observer-id: tui:v584-10k-blocker' \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$SESSION_10K/attach" \
  -d '{"surface":"tui","role":"writer"}' \
  >"$ARTIFACT_DIR/10k-blocker-attach.json"
auth_curl \
  -H 'x-cowd-surface-id: tui' \
  -H 'x-cowd-observer-id: tui:v584-10k-blocker' \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/runtime/session-leases/acquire" \
  -d "{\"session_id\":\"$SESSION_10K\",\"mode\":\"exclusive\"}" \
  >"$ARTIFACT_DIR/10k-blocker-lease.json"
rg -q '"ok":true' "$ARTIFACT_DIR/10k-blocker-lease.json" \
  || fail "10k clone blocker lease failed"

tenk_started_ms="$(monotonic_ms)"
start_tui tenk-reader "$SESSION_10K" "tui:v584-10k-reader" 120 40
wait_capture tenk-reader 'V584-10K-TAIL-09999' 10k-tail \
  || fail "10k TUI did not hydrate and render the durable tail"
tenk_tail_ms="$(( $(monotonic_ms) - tenk_started_ms ))"
tenk_tui_pid="$(tui_process_pid tenk-reader || true)"
[[ -n "$tenk_tui_pid" ]] || fail "10k TUI child process could not be identified"
tenk_rss_kib="$(awk '/VmRSS:/ {print $2}' "/proc/$tenk_tui_pid/status")"
printf '10k_tail_visible_ms\t%s\n10k_rss_kib\t%s\n' \
  "$tenk_tail_ms" "$tenk_rss_kib" >>"$PERFORMANCE"
(( tenk_tail_ms < 15000 )) \
  || fail "10k durable tail took ${tenk_tail_ms}ms to become visible"
(( tenk_rss_kib < 524288 )) \
  || fail "10k TUI RSS is ${tenk_rss_kib}KiB, above the 512MiB gate"

tenk_search_started_ms="$(monotonic_ms)"
send_raw tenk-reader $'\006'
send_raw tenk-reader 'V584-10K-EARLY-00000'
send_raw tenk-reader $'\r'
wait_capture tenk-reader 'V584-10K-EARLY-00000 durable history payload' 10k-search \
  || fail "10k TUI search could not reach the earliest durable message"
tenk_search_ms="$(( $(monotonic_ms) - tenk_search_started_ms ))"
printf '10k_search_visible_ms\t%s\n' "$tenk_search_ms" >>"$PERFORMANCE"
(( tenk_search_ms < 2500 )) \
  || fail "10k history search took ${tenk_search_ms}ms"
send_raw tenk-reader $'\033[F'
wait_capture tenk-reader 'V584-10K-TAIL-09999' 10k-return-tail \
  || fail "10k TUI could not return to the durable tail"
stop_tui tenk-reader
pass "E9 real 10k-session PTY tail/search/RSS gates passed (${tenk_tail_ms}ms/${tenk_search_ms}ms/${tenk_rss_kib}KiB)"

stop_gateway

start_gateway || fail "E10 baseline-history Gateway did not become healthy"
bad_token_status="$(
  command curl -sS \
    -o "$ARTIFACT_DIR/e10-unauthorized.json" \
    -w '%{http_code}' \
    -H 'Authorization: Bearer deliberately-wrong-token' \
    "$BASE_URL/api/sessions/$OLD_SESSION_ID/messages?from_seq=0&limit=10"
)"
[[ "$bad_token_status" == "401" ]] \
  || fail "E10 invalid credential did not fail closed with HTTP 401"
E10_READER_ID="tui:v584-e10-reader"
auth_curl \
  -H 'x-cowd-surface-id: tui' \
  -H "x-cowd-observer-id: $E10_READER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$OLD_SESSION_ID/attach" \
  -d '{"surface":"tui","role":"reader"}' \
  >"$ARTIFACT_DIR/e10-reader-attach.json"
e10_reader_lease_status="$(
  command curl -sS \
    -o "$ARTIFACT_DIR/e10-reader-lease-forbidden.json" \
    -w '%{http_code}' \
    -H "Authorization: Bearer $TOKEN" \
    -H 'x-cowd-surface-id: tui' \
    -H "x-cowd-observer-id: $E10_READER_ID" \
    -H 'content-type: application/json' \
    -X POST "$BASE_URL/api/runtime/session-leases/acquire" \
    -d "{\"session_id\":\"$OLD_SESSION_ID\",\"mode\":\"collaborative\"}"
)"
[[ "$e10_reader_lease_status" == "403" ]] \
  || fail "E10 reader attachment was not rejected with HTTP 403 at the writer-lease boundary"
auth_curl "$BASE_URL/api/runtime/session-leases" \
  >"$ARTIFACT_DIR/e10-leases-after-reader-403.json"
python3 - \
  "$ARTIFACT_DIR/e10-leases-after-reader-403.json" \
  "$OLD_SESSION_ID" \
  "$E10_READER_ID" <<'PY'
import json
import sys

projection = json.load(open(sys.argv[1], encoding="utf-8"))
session_id = sys.argv[2]
observer_id = sys.argv[3]
owners = {
    lease.get("owner", "")
    for lease in projection.get("leases", [])
    if lease.get("session_id") == session_id
}
assert not any(owner.endswith(f":observer:{observer_id}") for owner in owners), owners
PY
auth_curl \
  -H 'x-cowd-surface-id: tui' \
  -H "x-cowd-observer-id: $E10_READER_ID" \
  -H 'content-type: application/json' \
  -X POST "$BASE_URL/api/sessions/$OLD_SESSION_ID/detach" \
  -d '{"surface":"tui"}' \
  >"$ARTIFACT_DIR/e10-reader-detach.json"

start_tui e10-history "$OLD_SESSION_ID" "tui:v584-e10-history" 120 40
wait_capture e10-history "$old_fragment" e10-valid-history-boot \
  || fail "E10 baseline durable history did not hydrate before corruption"
sqlite3 "$OLD_CONFIG_HOME/storage/session.sqlite" <<SQL
DROP TABLE IF EXISTS v584_e10_message_backup;
CREATE TABLE v584_e10_message_backup AS
SELECT stable_message_id, content_json
FROM messages
WHERE session_id = '$OLD_SESSION_ID'
ORDER BY sequence
LIMIT 1;
DROP TRIGGER IF EXISTS messages_fts_au;
UPDATE messages
SET content_json = '{malformed-v584-e10'
WHERE stable_message_id = (
  SELECT stable_message_id FROM v584_e10_message_backup LIMIT 1
);
SQL
malformed_status="$(
  command curl -sS \
    -o "$ARTIFACT_DIR/e10-malformed-history.json" \
    -w '%{http_code}' \
    -H "Authorization: Bearer $TOKEN" \
    "$BASE_URL/api/sessions/$OLD_SESSION_ID/messages?from_seq=0&limit=500"
)"
[[ "$malformed_status" == "500" ]] \
  || fail "E10 malformed durable message did not produce the canonical HTTP 500 boundary"
malformed_offset_status="$(
  command curl -sS \
    -o "$ARTIFACT_DIR/e10-malformed-history-offset.json" \
    -w '%{http_code}' \
    -H "Authorization: Bearer $TOKEN" \
    "$BASE_URL/api/sessions/$OLD_SESSION_ID/messages?offset=0&limit=500"
)"
[[ "$malformed_offset_status" == "500" ]] \
  || fail "E10 malformed durable message did not fail the TUI offset-history boundary"
send_prompt e10-history "/history latest"
send_prompt e10-history "/activity"
wait_capture e10-history \
  'Loading older durable history failed|Gateway API returned 500|malformed content|500 Internal Server Error' \
  e10-malformed-visible \
  || fail "E10 malformed history failure was not visible in the interactive Activity surface"
rg -q '\{malformed-v584-e10' "$ARTIFACT_DIR/e10-malformed-visible.txt" \
  && fail "E10 malformed stored payload leaked into the chat transcript"
stop_tui e10-history
stop_gateway

sqlite3 "$OLD_CONFIG_HOME/storage/session.sqlite" <<SQL
UPDATE messages
SET content_json = (
  SELECT content_json
  FROM v584_e10_message_backup
  WHERE v584_e10_message_backup.stable_message_id = messages.stable_message_id
)
WHERE stable_message_id IN (
  SELECT stable_message_id FROM v584_e10_message_backup
);
DROP TABLE v584_e10_message_backup;
CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
    VALUES ('delete', old.id, old.session_id, old.role, NULL, old.tool_name);
    INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
    VALUES (new.id, new.session_id, new.role,
            (SELECT group_concat(json_extract(value,'$.text'),' ') FROM json_each(new.content_json) WHERE json_extract(value,'$.type')='text'),
            new.tool_name);
END;
SQL
start_gateway || fail "E10 repaired-history Gateway did not become healthy"
start_tui e10-recovered "$OLD_SESSION_ID" "tui:v584-e10-recovered" 120 40
wait_capture e10-recovered "$old_fragment" e10-history-recovered \
  || fail "E10 repaired durable history did not recover through a normal TUI restart"
stop_tui e10-recovered
stop_gateway
pass "E10 real 401, reader-role 403 with no writer lease, malformed-history visibility and recovery passed; authority-generation late-result rejection, closed-session draft restoration, projection revoke/unknown-event and transcript isolation passed fail-closed gates"

pass "all terminal clients and isolated Gateways exited through normal shutdown paths"
[[ -z "$(git -C "$ROOT" status --porcelain)" ]] \
  || fail "production acceptance changed the committed source tree"
[[ "$(git -C "$ROOT" rev-parse --short=8 HEAD)" == "$EXPECTED_GIT_SHA" ]] \
  || fail "source HEAD changed while production acceptance was running"
printf 'ARTIFACT_DIR\t%s\n' "$ARTIFACT_DIR" | tee -a "$SUMMARY"
echo "V584 TUI production acceptance passed: $ARTIFACT_DIR"
