#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0996_PORT:-18716}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0996-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0996-iacc.XXXXXX")"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  for _ in {1..10}; do
    if rm -rf "$TMP_DIR" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  rm -rf "$TMP_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.96 IACC report delivery retry state scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":15'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_report_delivery_retry_state"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0996","session_id":"session-v0996","facts":[{"fact_id":"fact-v0996-shortage-a","snapshot_id":"snapshot-v0996-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0996","product:server-v0996"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W37"},"measures":{"short_qty":450},"source_ref":"connector:erp:material-shortage","confidence":0.94}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0996","session_id":"session-v0996","profile":{"profile_id":"cockpit-profile-v0996-ops","owner_ref":"user:ops-planner","display_name":"Ops planner retry cockpit","focus_refs":["component:gpu-v0996"],"focus_metric_ids":["material_shortage_risk"],"thresholds":{"material_shortage_risk":{"critical":100,"warning":40}},"template_id":"ops.default","cadence":"daily"}}' \
  | rg -q '"profile_id":"cockpit-profile-v0996-ops"'

curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/cockpit-profile-v0996-ops/reports/generate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0996-report","session_id":"session-v0996","report":{"report_id":"cockpit-report-v0996-daily","cadence":"daily","delivery_ref":"channel://feishu/user/ops-planner","note":"retry state report snapshot"}}' \
  | rg -q '"report_id":"cockpit-report-v0996-daily"'

blocked_body="$(python3 - <<'PY'
import json

print(json.dumps({
    "mode": "dry_run",
    "idempotency_key": "v0996-report-delivery-blocked",
    "actor_principal": "user:ops-planner",
    "actor_identity_ref": "iacc://operator/ops-planner",
    "channel": "feishu",
    "template_id": "ops.alert.compact",
    "target_ref": "channel://feishu/user/ops-planner"
}))
PY
)"

blocked_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0996-daily/deliver" \
  -H 'content-type: application/json' \
  -d "$blocked_body")"
printf '%s' "$blocked_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["status"] == "blocked"; assert d["dispatch_status"] == "policy_blocked"; assert d["report"]["status"] == "delivery_blocked"; assert len(d["report"]["delivery_receipts"]) == 1'

state_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0996-daily/delivery-state")"
printf '%s' "$state_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); s=d["delivery_state"]; assert s["classification"] == "policy_blocked"; assert s["retryable"] is False; assert s["attempt_count"] == 1'

curl -fsS "$BASE_URL/api/cross-plane/identities" \
  -H 'content-type: application/json' \
  -d '{"id":"idb-v0996-ops-planner","principal_id":"user:ops-planner","identity_ref":"iacc://operator/ops-planner","trust":"verified","source":"v0996-scenario","created_at":"2026-06-10T00:00:00Z","expires_at":null}' \
  | rg -q '"principal_id":"user:ops-planner"'

curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d '{"id":"grant-v0996-feishu-send","principal_id":"user:ops-planner","capability":"channel.feishu.send_text","account_id":null,"target_ref":null,"resource_ref":null,"source_channel":null,"grant_type":"persistent","expires_at":null,"remaining_uses":null,"created_by":"v0996-scenario","approval_id":null}' \
  | rg -q '"capability":"channel.feishu.send_text"'

retry_body="$(python3 - <<'PY'
import json

print(json.dumps({
    "force": True,
    "mode": "dry_run",
    "actor_principal": "user:ops-planner",
    "actor_identity_ref": "iacc://operator/ops-planner",
    "channel": "feishu",
    "template_id": "ops.alert.compact"
}))
PY
)"

retry_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0996-daily/delivery/retry" \
  -H 'content-type: application/json' \
  -d "$retry_body")"
printf '%s' "$retry_json" | rg -q '"kind":"iacc.cockpit.report_delivery_retry"'
printf '%s' "$retry_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["before_state"]["classification"] == "policy_blocked"; assert d["after_state"]["classification"] == "dry_run_planned"; assert d["after_state"]["attempt_count"] == 2; assert d["delivery"]["status"] == "planned"; assert d["delivery"]["idempotent_replay"] is False; assert len(d["delivery"]["report"]["delivery_receipts"]) == 2'

curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0996-daily/delivery-state" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["delivery_state"]["classification"] == "dry_run_planned"; assert d["delivery_state"]["attempt_count"] == 2'

test -f "$WORKDIR/.cowd/iacc.sqlite"
test -f "$CONFIG_HOME/cross-plane/control-state.json"
