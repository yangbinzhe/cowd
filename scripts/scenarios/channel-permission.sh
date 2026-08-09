#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_CHANNEL_PERMISSION_PORT:-18694}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-channel-permission-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-channel-permission.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
SUFFIX="channel-permission-$$"
# Gateway derives this stable value from the authenticated local authority;
# scenarios must never inject an actor through an action payload.
PRINCIPAL="principal:local-human"
GRANT_ID="grant-$SUFFIX"
CAPABILITY="service.local.docs.read"
API_TOKEN="channel-permission-$$_credential"

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  status=$?
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  if [[ "$status" -ne 0 && -s "$LOG" ]]; then
    echo "channel permission gateway log:" >&2
    tail -200 "$LOG" >&2
  fi
  rm -rf "$TMP_DIR"
  return "$status"
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for channel permission scenario" >&2
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
    api_key: "channel-permission-provider-key"
    protocol: "completions"
    models:
      - "claude-sonnet-4-6"
permissions:
  default_mode: "danger-full-access"
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

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

gateway_ready=0
for _ in {1..100}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    gateway_ready=1
    break
  fi
  sleep 0.25
done
if [[ "$gateway_ready" -ne 1 ]]; then
  echo "channel permission gateway did not become healthy at $BASE_URL" >&2
  exit 1
fi

curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d "{\"id\":\"$GRANT_ID\",\"principal_id\":\"$PRINCIPAL\",\"capability\":\"$CAPABILITY\",\"account_id\":null,\"target_ref\":null,\"resource_ref\":null,\"source_channel\":null,\"grant_type\":\"single_use\",\"expires_at\":null,\"remaining_uses\":null,\"created_by\":\"channel-permission\",\"approval_id\":null}" \
  | rg -q "\"$GRANT_ID\""

ACTION="{\"source_channel\":\"channel://wechat/chat/$SUFFIX\",\"session_id\":\"session-$SUFFIX\",\"requested_capability\":\"$CAPABILITY\",\"provider_account\":\"local.docs\",\"target_ref\":null,\"resource_ref\":null,\"risk\":\"medium\",\"data_classification\":\"internal\",\"identity_trust\":\"verified\"}"

curl -fsS "$BASE_URL/api/cross-plane/action/preflight" \
  -H 'content-type: application/json' \
  -d "$ACTION" | rg -q '"decision"\s*:\s*"allow"'

curl -fsS "$BASE_URL/api/connectors/services/local.docs/execute" \
  -H 'content-type: application/json' \
  -d "{\"source_channel\":\"channel://wechat/chat/$SUFFIX\",\"session_id\":\"session-$SUFFIX\",\"tool_id\":\"$CAPABILITY\",\"resource_id\":\"doc-dry-$SUFFIX\",\"title\":\"Channel Permission Dry Run\",\"mode\":\"dry_run\",\"idempotency_key\":\"dry-$SUFFIX\"}" \
  | rg -q '"status"\s*:\s*"dry_run"'

curl -fsS "$BASE_URL/api/cross-plane/policy/simulate" \
  -H 'content-type: application/json' \
  -d "$ACTION" | rg -q '"decision"\s*:\s*"allow"'

curl -fsS "$BASE_URL/api/connectors/services/local.docs/execute" \
  -H 'content-type: application/json' \
  -d "{\"source_channel\":\"channel://wechat/chat/$SUFFIX\",\"session_id\":\"session-$SUFFIX\",\"tool_id\":\"$CAPABILITY\",\"resource_id\":\"doc-$SUFFIX\",\"title\":\"Channel Permission\",\"mode\":\"commit\",\"idempotency_key\":\"commit-$SUFFIX\"}" \
  | rg -q '"status"\s*:\s*"executed"'

grants_json="$(curl -fsS "$BASE_URL/api/cross-plane/grants")"
printf '%s' "$grants_json" | rg -q "\"id\"\\s*:\\s*\"$GRANT_ID\""
printf '%s' "$grants_json" | rg -q '"remaining_uses"\s*:\s*0'

audit_json="$(curl -fsS "$BASE_URL/api/cross-plane/audit")"
printf '%s' "$audit_json" | rg -q "\"consumed_grant_id\"\\s*:\\s*\"$GRANT_ID\""
printf '%s' "$audit_json" | rg -q '"remaining_uses_after"\s*:\s*0'

ready_json=""
for _ in {1..100}; do
  ready_json="$(curl -sS "$BASE_URL/readyz")"
  if printf '%s' "$ready_json" | rg -q '"ready":true'; then
    break
  fi
  sleep 0.25
done
if ! printf '%s' "$ready_json" | rg -q '"ready":true'; then
  echo "channel permission gateway is not ready: $ready_json" >&2
  exit 1
fi
