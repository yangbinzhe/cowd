#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0974_PORT:-18694}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0974-channel-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0974-channel.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
SUFFIX="v0974-$$"
PRINCIPAL="user:$SUFFIX"
GRANT_ID="grant-$SUFFIX"
CAPABILITY="service.mock.docs.read"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.74 channel permission scenario" >&2
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

curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d "{\"id\":\"$GRANT_ID\",\"principal_id\":\"$PRINCIPAL\",\"capability\":\"$CAPABILITY\",\"account_id\":null,\"target_ref\":null,\"resource_ref\":null,\"source_channel\":null,\"grant_type\":\"single_use\",\"expires_at\":null,\"remaining_uses\":null,\"created_by\":\"v0974\",\"approval_id\":null}" \
  | rg -q "\"$GRANT_ID\""

ACTION="{\"actor_principal\":\"$PRINCIPAL\",\"source_channel\":\"channel://wechat/chat/$SUFFIX\",\"session_id\":\"session-$SUFFIX\",\"requested_capability\":\"$CAPABILITY\",\"provider_account\":\"mock.docs\",\"target_ref\":null,\"resource_ref\":null,\"risk\":\"medium\",\"data_classification\":\"internal\",\"identity_trust\":\"verified\"}"

curl -fsS "$BASE_URL/api/cross-plane/action/preflight" \
  -H 'content-type: application/json' \
  -d "$ACTION" | rg -q '"decision"\s*:\s*"allow"'

curl -fsS "$BASE_URL/api/connectors/services/mock.docs/execute" \
  -H 'content-type: application/json' \
  -d "{\"actor_principal\":\"$PRINCIPAL\",\"source_channel\":\"channel://wechat/chat/$SUFFIX\",\"session_id\":\"session-$SUFFIX\",\"tool_id\":\"$CAPABILITY\",\"resource_id\":\"doc-dry-$SUFFIX\",\"title\":\"Channel Permission Dry Run\",\"mode\":\"dry_run\",\"idempotency_key\":\"dry-$SUFFIX\"}" \
  | rg -q '"status"\s*:\s*"dry_run"'

curl -fsS "$BASE_URL/api/cross-plane/policy/simulate" \
  -H 'content-type: application/json' \
  -d "$ACTION" | rg -q '"decision"\s*:\s*"allow"'

curl -fsS "$BASE_URL/api/connectors/services/mock.docs/execute" \
  -H 'content-type: application/json' \
  -d "{\"actor_principal\":\"$PRINCIPAL\",\"source_channel\":\"channel://wechat/chat/$SUFFIX\",\"session_id\":\"session-$SUFFIX\",\"tool_id\":\"$CAPABILITY\",\"resource_id\":\"doc-$SUFFIX\",\"title\":\"Channel Permission\",\"mode\":\"commit\",\"idempotency_key\":\"commit-$SUFFIX\"}" \
  | rg -q '"status"\s*:\s*"ok"'

grants_json="$(curl -fsS "$BASE_URL/api/cross-plane/grants")"
printf '%s' "$grants_json" | rg -q "\"id\"\\s*:\\s*\"$GRANT_ID\""
printf '%s' "$grants_json" | rg -q '"remaining_uses"\s*:\s*0'

audit_json="$(curl -fsS "$BASE_URL/api/cross-plane/audit")"
printf '%s' "$audit_json" | rg -q "\"consumed_grant_id\"\\s*:\\s*\"$GRANT_ID\""
printf '%s' "$audit_json" | rg -q '"remaining_uses_after"\s*:\s*0'

curl -fsS "$BASE_URL/runtime/context/inspect" | rg -q '<title>Cowd Web UI</title>'
