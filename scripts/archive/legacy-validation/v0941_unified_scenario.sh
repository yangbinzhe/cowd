#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0941_PORT:-18670}"
BASE_URL="http://127.0.0.1:$PORT"
TMUX_SESSION="cowd-v0941-unified-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-v0941-unified.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
PRINCIPAL="user:v0941"
IDENTITY_REF="channel://scenario/user/v0941?email=v0941@example.test"
OWNER="scenario:v0941"
RESOURCE_ID="v0941-doc-$$"
TITLE="Unified Scenario Doc $$"
IDEMPOTENCY_KEY="v0941-$$"
SCENARIO_API_KEY="${ANTHROPIC_API_KEY:-test-dummy-key-for-v0941-scenario}"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$TMUX_SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
on_error() {
  local status=$?
  echo "v0.9.41 unified scenario failed with status $status" >&2
  print_logs
  exit "$status"
}
trap cleanup EXIT
trap on_error ERR

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required for v0.9.41 unified scenario" >&2
    exit 1
  fi
}

request_json() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local out="$4"
  local code
  if [[ -n "$body" ]]; then
    code="$(curl -sS -o "$out" -w '%{http_code}' -X "$method" "$BASE_URL$path" \
      -H 'Content-Type: application/json' \
      --data "$body")"
  else
    code="$(curl -sS -o "$out" -w '%{http_code}' -X "$method" "$BASE_URL$path")"
  fi
  if [[ "$code" -lt 200 || "$code" -ge 300 ]]; then
    echo "$method $path returned HTTP $code" >&2
    sed -n '1,200p' "$out" >&2 || true
    return 1
  fi
}

assert_json() {
  local file="$1"
  local check="$2"
  python3 - "$file" "$check" <<'PY'
import json
import sys

path, check = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)

def fail(message):
    print(f"{check}: {message}", file=sys.stderr)
    print(json.dumps(data, ensure_ascii=False, indent=2), file=sys.stderr)
    sys.exit(1)

if check == "session":
    if not data.get("id"):
        fail("session id missing")
elif check == "identity":
    item = data.get("identity") or {}
    if item.get("trust") != "verified":
        fail("verified identity not persisted")
elif check == "grant":
    item = data.get("grant") or {}
    if item.get("capability") != "service.mock.docs.read" or item.get("grant_type") != "persistent":
        fail("mock docs persistent grant missing")
elif check == "lease_acquire":
    if data.get("ok") is not True:
        fail("lease acquire did not return ok=true")
    lease = data.get("lease") or data
    if lease.get("owner") != "scenario:v0941":
        fail("lease owner mismatch")
elif check == "connector_execution":
    receipt = data.get("receipt") or {}
    result = data.get("result") or {}
    resource = result.get("resource") or {}
    if data.get("kind") != "connector_service_execution":
        fail("unexpected connector execution kind")
    if data.get("service") != "mock.docs":
        fail("unexpected connector service")
    if receipt.get("status") != "executed":
        fail("mock docs commit was not executed")
    if receipt.get("decision", {}).get("decision") != "allow":
        fail("policy decision did not allow execution")
    if data.get("resource_persisted") is not True:
        fail("resource was not persisted")
    if not resource.get("reference", "").startswith("service://mock.docs/document/"):
        fail("resource reference missing")
elif check == "control_plane":
    connectors = (data.get("components") or {}).get("connectors") or data.get("connectors") or {}
    summary = connectors.get("summary") or {}
    if summary.get("account_count", summary.get("accounts", 0)) < 1:
        fail("connector summary not projected")
    session = (data.get("components") or {}).get("session") or {}
    leases = session.get("leases") or data.get("session_leases") or {}
    if leases.get("total", leases.get("active", 0)) < 1:
        fail("active session lease not projected")
elif check == "resources":
    resources = data.get("resources") or []
    if data.get("limit", 0) > 200:
        fail("resource page limit was not clamped")
    if not any("v0941-doc-" in (item.get("reference") or "") for item in resources):
        fail("persisted v0941 resource not found")
elif check == "audit":
    records = data.get("records") or []
    for record in records:
        action = record.get("action") or {}
        evidence = record.get("evidence") or {}
        connector = evidence.get("connector_context") or {}
        if action.get("requested_capability") == "service.mock.docs.read" and connector.get("capability_id") == "service.mock.docs.read":
            break
    else:
        fail("connector-aware audit evidence not found")
elif check == "lease_release":
    if data.get("ok") is not True:
        fail("lease release did not return ok=true")
else:
    fail(f"unknown check {check}")
PY
}

print_logs() {
  echo "----- gateway log -----" >&2
  sed -n '1,220p' "$LOG" >&2 || true
  echo "-----------------------" >&2
}

require_cmd tmux
require_cmd curl
require_cmd python3
require_cmd rg
require_cmd ss

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

cd "$ROOT"
cargo build -p cowd-cli

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
        enabled: false
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$TMUX_SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..80}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

if ! curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
  print_logs
  exit 1
fi

SESSION_JSON="$TMP_DIR/session.json"
IDENTITY_JSON="$TMP_DIR/identity.json"
GRANT_JSON="$TMP_DIR/grant.json"
LEASE_JSON="$TMP_DIR/lease.json"
EXEC_JSON="$TMP_DIR/execution.json"
CONTROL_JSON="$TMP_DIR/control-plane.json"
RESOURCES_JSON="$TMP_DIR/resources.json"
AUDIT_JSON="$TMP_DIR/audit.json"
RELEASE_JSON="$TMP_DIR/release.json"

request_json POST /api/sessions '{"model":"claude-sonnet-4-6"}' "$SESSION_JSON"
assert_json "$SESSION_JSON" session
SESSION_ID="$(python3 - "$SESSION_JSON" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["id"])
PY
)"

request_json POST /api/cross-plane/identities \
  "{\"id\":\"idb-v0941-$$\",\"principal_id\":\"$PRINCIPAL\",\"identity_ref\":\"$IDENTITY_REF\",\"trust\":\"verified\",\"source\":\"scenario\",\"created_at\":\"2026-06-08T00:00:00Z\",\"expires_at\":null}" \
  "$IDENTITY_JSON"
assert_json "$IDENTITY_JSON" identity

request_json POST /api/cross-plane/grants \
  "{\"id\":\"grant-v0941-$$\",\"principal_id\":\"$PRINCIPAL\",\"capability\":\"service.mock.docs.read\",\"account_id\":null,\"target_ref\":null,\"resource_ref\":null,\"source_channel\":null,\"grant_type\":\"persistent\",\"expires_at\":null,\"remaining_uses\":null,\"created_by\":\"v0941-scenario\",\"approval_id\":null}" \
  "$GRANT_JSON"
assert_json "$GRANT_JSON" grant

request_json POST /api/runtime/session-leases/acquire \
  "{\"session_id\":\"$SESSION_ID\",\"owner\":\"$OWNER\",\"mode\":\"collaborative\"}" \
  "$LEASE_JSON"
assert_json "$LEASE_JSON" lease_acquire

request_json POST /api/connectors/services/mock.docs/execute \
  "{\"actor_principal\":\"$PRINCIPAL\",\"actor_identity_ref\":\"$IDENTITY_REF\",\"source_channel\":\"channel://scenario/chat/v0941\",\"session_id\":\"$SESSION_ID\",\"tool_id\":\"service.mock.docs.read\",\"resource_id\":\"$RESOURCE_ID\",\"title\":\"$TITLE\",\"mode\":\"commit\",\"idempotency_key\":\"$IDEMPOTENCY_KEY\"}" \
  "$EXEC_JSON"
assert_json "$EXEC_JSON" connector_execution

request_json GET /api/runtime/control-plane "" "$CONTROL_JSON"
assert_json "$CONTROL_JSON" control_plane

request_json GET "/api/connectors/resources?q=v0941-doc&limit=999&offset=0" "" "$RESOURCES_JSON"
assert_json "$RESOURCES_JSON" resources

request_json GET /api/cross-plane/audit "" "$AUDIT_JSON"
assert_json "$AUDIT_JSON" audit

request_json POST /api/runtime/session-leases/release \
  "{\"session_id\":\"$SESSION_ID\",\"owner\":\"$OWNER\"}" \
  "$RELEASE_JSON"
assert_json "$RELEASE_JSON" lease_release

echo "v0.9.41 unified daemon/API/session/connector/audit scenario passed"
