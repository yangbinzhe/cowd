#!/usr/bin/env bash
set -euo pipefail

# V9's terminal gate runs against an isolated Gateway configuration. It never
# uses an already-running user service and never treats unavailable live-model
# evidence as a successful skip.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EDGE_ROOT="${COWD_EDGE_ROOT:-$(cd "$ROOT/../cowd-edge" && pwd)}"
WEBUI_ROOT="$EDGE_ROOT/surfaces/webui"
SOURCE_CONFIG_HOME="${COWD_EVAL_CONFIG_HOME:?set COWD_EVAL_CONFIG_HOME to a configuration directory with provider settings}"
PORT="${COWD_V9_GATEWAY_PORT:-8764}"
BASE_URL="http://127.0.0.1:${PORT}"
TEMP_HOME="$(mktemp -d "${TMPDIR:-/tmp}/cowd-v9-gate.XXXXXX")"
GATEWAY_LOG="$TEMP_HOME/gateway.log"
GATEWAY_PID=""
TUI_SESSION="cowd-v9-tui-$$"

cleanup() {
  tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
  if [[ -n "$GATEWAY_PID" ]]; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" 2>/dev/null || true
  fi
  rm -rf "$TEMP_HOME"
}
trap cleanup EXIT INT TERM

start_gateway() {
  COWD_CONFIG_HOME="$TEMP_HOME" ./target/debug/cowd gateway run --port "$PORT" >"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  for _ in $(seq 1 80); do
    if curl --fail --silent --show-error "$BASE_URL/healthz" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  cat "$GATEWAY_LOG" >&2
  return 1
}

stop_gateway() {
  if [[ -n "$GATEWAY_PID" ]]; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" >/dev/null 2>&1 || true
    GATEWAY_PID=""
  fi
}

for file in config.yaml models.yaml credentials.json; do
  if [[ -f "$SOURCE_CONFIG_HOME/$file" ]]; then
    cp "$SOURCE_CONFIG_HOME/$file" "$TEMP_HOME/$file"
  fi
done
for directory in profiles plugins; do
  if [[ -d "$SOURCE_CONFIG_HOME/$directory" ]]; then
    cp -a "$SOURCE_CONFIG_HOME/$directory" "$TEMP_HOME/$directory"
  fi
done
[[ -f "$TEMP_HOME/config.yaml" ]] || { echo "missing $SOURCE_CONFIG_HOME/config.yaml" >&2; exit 1; }

# A named live-model evaluation must prove that the requested model actually
# answered. Production keeps its configured fallback chain; this isolated test
# copy intentionally has no fallback candidates so an unavailable requested
# model is a visible evaluation failure, never a silent model substitution.
perl -0pi -e 's/^fallbacks:\n(?:^[ \t-].*\n)*/fallbacks: []\n/m' "$TEMP_HOME/config.yaml"

# The copied test configuration receives a private port and storage root.
# Provider credentials remain local to the ephemeral test directory and are
# never printed by this script. Gateway auth remains enabled when configured:
# the evaluator receives the same ephemeral bearer token instead of weakening
# the production request path.
sed -i "0,/^    port: [0-9][0-9]*/s//    port: ${PORT}/" "$TEMP_HOME/config.yaml"
sed -i "s|^  store_path: .*|  store_path: ${TEMP_HOME}/memory|" "$TEMP_HOME/config.yaml"
sed -i '/^[[:space:]]*webui_dir:[[:space:]]*/d' "$TEMP_HOME/config.yaml"
sed -i "/^gateway:[[:space:]]*$/a\\  webui_dir: \"${WEBUI_ROOT}/dist\"" "$TEMP_HOME/config.yaml"
EVAL_TOKEN="${COWD_EVAL_GATEWAY_TOKEN:-$(sed -n '/auth:/,/platform_type:/ { s/^[[:space:]]*token:[[:space:]]*//p; }' "$TEMP_HOME/config.yaml" | head -1)}"
[[ -n "$EVAL_TOKEN" ]] || { echo "missing Gateway API token; set COWD_EVAL_GATEWAY_TOKEN or configure api_server.auth.token" >&2; exit 1; }

cd "$ROOT"
cargo build -p cli -p sandbox-launcher --features tui-surface
cargo build -p harness-eval
start_gateway

cargo test -p gateway --test gateway_runtimehost_architecture
cargo test -p gateway --test gateway_route_source_architecture
cargo test -p harness-eval --test architecture_dependencies
cargo test -p runtime execution_projection --lib
cargo test -p tui gateway_sse --lib
cargo test -p harness-eval --lib

(
  cd "$WEBUI_ROOT"
  COWD_GATEWAY_URL="$BASE_URL" COWD_API_TOKEN="$EVAL_TOKEN" npm run generate:api
  git diff --exit-code -- src/generated/gateway-api.ts
  git ls-files --error-unmatch src/generated/gateway-api.ts >/dev/null
  npm run test:unit
  npm run test:i18n
  npm run build
)

# Restart the isolated Gateway only after the freshly generated static bundle
# exists. This proves its actual static resource registration rather than a
# Vite development server or a stale in-memory directory snapshot.
stop_gateway
start_gateway
curl --fail --silent --show-error "$BASE_URL/index.html" | grep -q '<!doctype html'
curl --fail --silent --show-error "$BASE_URL/chat" | grep -q '<!doctype html'
(
  cd "$WEBUI_ROOT"
  COWD_E2E_GATEWAY_URL="$BASE_URL" COWD_E2E_GATEWAY_TOKEN="$EVAL_TOKEN" npm run test:e2e
)

# This is a real terminal surface gate, not a mocked client test. The TUI must
# attach to the isolated Gateway and render a Gateway-derived projection before
# it is stopped. Command receipt behavior remains covered by the TUI protocol
# integration suite above; no provider work is started from this smoke gate.
command -v tmux >/dev/null || { echo "tmux is required for the V9 terminal gate" >&2; exit 1; }
TUI_SESSION_RESPONSE="$(curl --fail --silent --show-error \
  -H "Authorization: Bearer ${EVAL_TOKEN}" \
  -H 'content-type: application/json' \
  -d '{"model":"'"${COWD_EVAL_MODEL:-deepseek-v4-flash}"'"}' \
  "$BASE_URL/api/sessions")"
TUI_SESSION_ID="$(node -e 'const value = JSON.parse(process.argv[1]); const id = value.id || value.session_id; if (!id) process.exit(1); process.stdout.write(id)' "$TUI_SESSION_RESPONSE")"
tmux new-session -d -s "$TUI_SESSION" \
  "COWD_GATEWAY_URL='$BASE_URL' COWD_API_TOKEN='$EVAL_TOKEN' '$ROOT/target/debug/cowd' tui --model '${COWD_EVAL_MODEL:-deepseek-v4-flash}' --session '$TUI_SESSION_ID'"
sleep 2
# Drive a real TUI-originated turn, then a canonical execution command. The
# terminal must never mutate lifecycle locally: its visible receipt comes from
# Gateway and the projection stream supplies the subsequent state refresh.
tmux send-keys -t "$TUI_SESSION" "Use a read-only tool to inspect Cargo.toml, then report the package version." Enter
# Gateway accepts the turn asynchronously. Give the terminal's durable event
# bridge enough time to receive the admission graph before issuing its control
# command; this is intentionally a bounded surface readiness wait, not a
# Runtime completion deadline.
sleep 5
tmux send-keys -t "$TUI_SESSION" "/execution cancel" Enter
sleep 2
TUI_CAPTURE="$(tmux capture-pane -pt "$TUI_SESSION" -S -200)"
if grep -qiE 'gateway.*(unavailable|failed|error)|connection refused' <<<"$TUI_CAPTURE"; then
  printf '%s\n' "$TUI_CAPTURE" >&2
  echo "TUI failed to attach to the isolated Gateway" >&2
  exit 1
fi
tmux kill-session -t "$TUI_SESSION"

EVAL_OUTPUT="$TEMP_HOME/harness-eval.stdout"
EVAL_ARCHIVE_ROOT="${COWD_V9_EVAL_ARCHIVE_DIR:-$ROOT/target/v9-eval-artifacts}"
COWD_EVAL_GATEWAY_URL="$BASE_URL" \
  COWD_CONFIG_HOME="$TEMP_HOME" \
  COWD_API_TOKEN="$EVAL_TOKEN" \
COWD_EVAL_REAL_MODEL=1 \
  cargo run -p harness-eval -- deep-real --provider "${COWD_EVAL_MODEL:-deepseek-v4-flash}" --budget full --allow-real-model | tee "$EVAL_OUTPUT"
EVAL_REPORT="$(sed -n 's/^json: //p' "$EVAL_OUTPUT" | tail -1)"
[[ -n "$EVAL_REPORT" && -f "$EVAL_REPORT" ]] || {
  echo "harness-eval did not emit a readable report path" >&2
  exit 1
}
mkdir -p "$EVAL_ARCHIVE_ROOT"
EVAL_RUN_DIR="$(dirname "$EVAL_REPORT")"
EVAL_ARCHIVE_DIR="$EVAL_ARCHIVE_ROOT/$(basename "$EVAL_RUN_DIR")"
if [[ -e "$EVAL_ARCHIVE_DIR" ]]; then
  EVAL_ARCHIVE_DIR="${EVAL_ARCHIVE_DIR}-$(date +%s)"
fi
cp -a "$EVAL_RUN_DIR" "$EVAL_ARCHIVE_DIR"
node -e '
const fs = require("fs");
const path = require("path");
const report = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
const live = report.live_gateway_scenarios || {};
const packageRoot = report.result_package_dir;
const suitePath = packageRoot && path.join(packageRoot, "live-scenarios", "suite.json");
const suite = suitePath && fs.existsSync(suitePath)
  ? JSON.parse(fs.readFileSync(suitePath, "utf8"))
  : {};
const scenarios = Array.isArray(suite.scenarios) ? suite.scenarios : [];
const complete = scenarios.length >= 3 && scenarios.every((scenario) =>
  scenario.status === "passed" && scenario.production_trace && scenario.trace?.length);
if (report.status !== "passed" || live.status !== "passed" || !complete) {
  console.error(JSON.stringify({
    report_status: report.status,
    live_status: live.status,
    scenario_count: scenarios.length,
    complete_live_traces: complete,
    suite_path: suitePath,
  }));
  process.exit(1);
}
' "$EVAL_REPORT"

# Direct-path performance is assessed separately against the last immutable
# baseline. The helper starts two fresh isolated Gateways and retains every
# one of the alternating pairs, including provider failures. Release uses 20
# pairs: with a remote provider, p95 over only five observations is the single
# slowest response and cannot distinguish an isolated provider jitter event
# from a regression. This raises sampling power without changing the p50/p95
# regression thresholds or dropping any observation.
COWD_EVAL_CONFIG_HOME="$SOURCE_CONFIG_HOME" \
  COWD_EVAL_MODEL="${COWD_EVAL_MODEL:-deepseek-v4-flash}" \
  COWD_V9_PERFORMANCE_PAIRS="${COWD_V9_PERFORMANCE_PAIRS:-20}" \
  COWD_V9_PERFORMANCE_ARCHIVE_DIR="$EVAL_ARCHIVE_ROOT/performance" \
  "$ROOT/scripts/v9-performance-gate.sh"

echo "V9 terminal gate passed against ${BASE_URL}; evaluation package: ${EVAL_ARCHIVE_DIR}"
