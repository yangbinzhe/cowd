#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_MEMORY_RUNTIME_PORT:-18693}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-memory-runtime-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-memory-runtime.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
API_TOKEN="memory-runtime-$$_credential"

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for memory runtime scenario" >&2
  exit 1
fi


if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  scenario:
    base_url: "http://127.0.0.1:1"
    api_key: "memory-runtime-provider-key"
    protocol: "completions"
    models:
      - "claude-sonnet-4-6"
permissions:
  default_mode: "danger-full-access"
memory:
  enabled: true
  store:
    sqlite_path: "$TMP_DIR/memory.db"
    blob_dir: "$TMP_DIR/blobs"
    enable_vector_index: false
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

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..100}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -fsS "$BASE_URL/api/memory/L3" \
  -H 'content-type: application/json' \
  -d '{"title":"MEMORY_RUNTIME_ALPHA","content":"MEMORY_RUNTIME_ALPHA is the effective runtime memory orientation.","tags":["memory-runtime"],"category":"Reference","priority":"High"}' \
  | rg -q '"id"'

for idx in 1 2 3; do
  curl -fsS "$BASE_URL/api/memory/L3" \
    -H 'content-type: application/json' \
    -d "{\"title\":\"Memory runtime large doc $idx\",\"content\":\"$(printf 'large runtime memory body %.0s' {1..120})\",\"tags\":[\"memory-runtime-cluster\"],\"category\":\"Reference\",\"priority\":\"Normal\"}" \
    >/dev/null
done

context_json="$(curl -fsS "$BASE_URL/api/context/current?q=MEMORY_RUNTIME_ALPHA&session_id=memory-runtime")"
printf '%s' "$context_json" | rg -q '"source":"synthetic"|"source":"runtime"'
printf '%s' "$context_json" | rg -q 'MEMORY_RUNTIME_ALPHA'

runtime_json="$(curl -fsS "$BASE_URL/api/memory/runtime")"
printf '%s' "$runtime_json" | rg -q '"runtime"'
printf '%s' "$runtime_json" | rg -q '"cluster_count"'
printf '%s' "$runtime_json" | rg -q '"total_selected"'

clusters_json="$(curl -fsS "$BASE_URL/api/memory/clusters?limit=8")"
printf '%s' "$clusters_json" | rg -q '"memory-runtime-cluster"'
printf '%s' "$clusters_json" | rg -q '"truncated":true'

memory_id="$(curl -fsS "$BASE_URL/api/memory/search?q=MEMORY_RUNTIME_ALPHA" | python3 -c 'import json,sys; d=json.load(sys.stdin); rows=d.get("results") or d.get("memories") or []; print((rows[0].get("id") if rows else ""))')"
if [[ -z "$memory_id" ]]; then
  echo "memory search did not return created entry" >&2
  exit 1
fi
curl -fsS "$BASE_URL/api/memory/lifecycle/$memory_id" | rg -q '"events"'

curl -fsS "$BASE_URL/readyz" | rg -q '"ready":true'
