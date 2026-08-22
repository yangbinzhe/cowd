#!/usr/bin/env bash
set -euo pipefail

# Run the deep-real Harness lane through an isolated, short-lived Gateway.
# The Qwen credential stays in DASHSCOPE_API_KEY; the generated config holds
# only the explicit env: reference understood by provider::ProviderClient.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CALLER_HOME="$HOME"
CARGO_HOME_DIR="${CARGO_HOME:-$CALLER_HOME/.cargo}"
RUSTUP_HOME_DIR="${RUSTUP_HOME:-$CALLER_HOME/.rustup}"
MODEL="${COWD_EVAL_MODEL:-qwen3.7-plus}"
BIN="${COWD_BIN:-$ROOT/target/debug/cowd}"
PORT="${COWD_EVAL_GATEWAY_PORT:-18657}"
BASE_URL="http://127.0.0.1:$PORT"
TOKEN="harness-eval-real-qwen-${RANDOM}-${RANDOM}"
RUN_ROOT="$(mktemp -d /tmp/cowd-real-qwen-gateway.XXXXXX)"
EVIDENCE_ROOT="${COWD_AI_HARNESS_REPORT_DIR:-$(mktemp -d /tmp/cowd-real-qwen-evidence.XXXXXX)}"
CONFIG_HOME="$RUN_ROOT/config"
ISOLATED_HOME="$RUN_ROOT/home"
# Evaluation must inspect the actual source tree. Isolation applies to the
# Gateway's config, credentials, authentication token and durable state, not
# to the read-only source fixture presented to the model.
WORKSPACE="$ROOT"
GATEWAY_LOG="$RUN_ROOT/gateway.log"
GATEWAY_PID=""

cleanup() {
  local status=$?
  if [[ -n "$GATEWAY_PID" ]]; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" >/dev/null 2>&1 || true
  fi
  if [[ "$status" -ne 0 || "${COWD_EVAL_KEEP_GATEWAY_ARTIFACTS:-}" == "1" ]]; then
    printf 'isolated Gateway artifacts: %s\n' "$RUN_ROOT" >&2
  else
    rm -rf "$RUN_ROOT"
  fi
  printf 'Harness evidence: %s\n' "$EVIDENCE_ROOT" >&2
}
trap cleanup EXIT

[[ -n "${DASHSCOPE_API_KEY:-}" ]] || {
  echo 'DASHSCOPE_API_KEY must be set for the real Qwen evaluation.' >&2
  exit 2
}
command -v curl >/dev/null || { echo 'curl is required.' >&2; exit 2; }
command -v ss >/dev/null || { echo 'ss is required.' >&2; exit 2; }
if ss -ltn | rg -q ":${PORT}\\b"; then
  echo "isolated Gateway port $PORT is already in use" >&2
  exit 2
fi
# The Gateway is the system under test.  Building only harness-eval after the
# Gateway starts leaves an old `target/debug/cowd` serving the scenario and
# silently evaluates a previous candidate.  The default binary is therefore
# rebuilt before every real run.  An explicit COWD_BIN remains an operator
# supplied immutable artifact and must already be executable.
if [[ -z "${COWD_BIN:-}" ]]; then
  cargo build -p cli --bin cowd
elif [[ ! -x "$BIN" ]]; then
  echo "COWD_BIN is not executable: $BIN" >&2
  exit 2
fi
GATEWAY_BINARY_SHA256="$(sha256sum "$BIN" | awk '{print $1}')"
printf 'Gateway binary sha256: %s\n' "$GATEWAY_BINARY_SHA256" >&2

mkdir -p "$CONFIG_HOME" "$ISOLATED_HOME/.cowd"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "$MODEL"
providers:
  dashscope:
    base_url: "${DASHSCOPE_BASE_URL:-https://dashscope.aliyuncs.com/compatible-mode/v1}"
    api_key: "env:DASHSCOPE_API_KEY"
    protocol: completions
    models:
      - "$MODEL"
permissions:
  default_mode: danger-full-access
memory:
  enabled: false
storage:
  backend: sqlite
gateway:
  enabled: true
  session_reset: none
  platforms:
    - platformType: api_server
      enabled: true
      host: 127.0.0.1
      port: $PORT
      auth:
        enabled: true
        token: "$TOKEN"
EOF

(
  cd "$WORKSPACE"
  env COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$ISOLATED_HOME" COWD_LOG_STDERR=1 \
    "$BIN" gateway run >"$GATEWAY_LOG" 2>&1 &
  echo $! >"$RUN_ROOT/gateway.pid"
)
GATEWAY_PID="$(<"$RUN_ROOT/gateway.pid")"
for _ in {1..240}; do
  if curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/healthz" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$GATEWAY_PID" >/dev/null 2>&1 || {
    sed -n '1,240p' "$GATEWAY_LOG" >&2 || true
    exit 1
  }
  sleep 0.25
done
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/healthz" >/dev/null

cd "$ROOT"
env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  HOME="$ISOLATED_HOME" \
  CARGO_HOME="$CARGO_HOME_DIR" \
  RUSTUP_HOME="$RUSTUP_HOME_DIR" \
  COWD_API_TOKEN="$TOKEN" \
  COWD_EVAL_GATEWAY_URL="$BASE_URL" \
  COWD_EVAL_REAL_MODEL=1 \
  COWD_EVAL_BINARY_SHA256="$GATEWAY_BINARY_SHA256" \
  COWD_AI_HARNESS_REPORT_DIR="$EVIDENCE_ROOT" \
  timeout "${COWD_EVAL_TIMEOUT_SECS:-900}s" \
  cargo run -p harness-eval -- deep-real --provider "$MODEL" --budget full --allow-real-model
