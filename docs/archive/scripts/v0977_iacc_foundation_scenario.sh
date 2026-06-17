#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0977_PORT:-18697}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0977-iacc-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0977-iacc.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.77 IACC foundation scenario" >&2
  exit 1
fi

if [[ ! -x "$BIN" ]]; then
  echo "cowd binary not found at $BIN; build it first or set COWD_BIN" >&2
  exit 1
fi

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
ln -s "$ROOT/webui" "$WORKDIR/webui"

cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "claude-sonnet-4-6"
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
        enabled: false
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

curl -fsS "$BASE_URL/healthz" | rg -q '"gateway":"daemon-http-gateway"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"kind":"iacc.health"'

ingest_json="$(curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0977","session_id":"session-v0977","facts":[{"fact_id":"fact-v0977-gpu-shortage","snapshot_id":"snapshot-v0977-week24","fact_type":"supply.material_shortage","entity_refs":["component:gpu-a"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W24"},"measures":{"short_qty":42},"source_ref":"connector:mock.docs:gpu-shortage","confidence":0.91}]}')"
printf '%s' "$ingest_json" | rg -q '"kind":"iacc.fact.ingest"'
printf '%s' "$ingest_json" | rg -q '"ingested":1'
attention_id="$(printf '%s' "$ingest_json" | sed -n 's/.*"attention_id":"\([^"]*\)".*/\1/p')"

curl -fsS "$BASE_URL/api/iacc/attention/hot" | rg -q "$attention_id"

evidence_json="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"attention_id\":\"$attention_id\",\"problem_statement\":\"GPU shortage may affect server shipments\"}")"
printf '%s' "$evidence_json" | rg -q '"kind":"iacc.evidence.packet"'
packet_id="$(printf '%s' "$evidence_json" | sed -n 's/.*"packet_id":"\([^"]*\)".*/\1/p')"

curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id" | rg -q "$packet_id"
test -f "$WORKDIR/.cowd/iacc.sqlite"
