#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0994_PORT:-18714}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0994-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0994-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.94 IACC report payload template scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":13'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_report_delivery_bridge"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_report_payload_templates"'

curl -fsS "$BASE_URL/api/cross-plane/identities" \
  -H 'content-type: application/json' \
  -d '{"id":"idb-v0994-ops-planner","principal_id":"user:ops-planner","identity_ref":"iacc://operator/ops-planner","trust":"verified","source":"v0994-scenario","created_at":"2026-06-10T00:00:00Z","expires_at":null}' \
  | rg -q '"principal_id":"user:ops-planner"'

curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d '{"id":"grant-v0994-feishu-send","principal_id":"user:ops-planner","capability":"channel.feishu.send_text","account_id":null,"target_ref":null,"resource_ref":null,"source_channel":null,"grant_type":"persistent","expires_at":null,"remaining_uses":null,"created_by":"v0994-scenario","approval_id":null}' \
  | rg -q '"capability":"channel.feishu.send_text"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0994","session_id":"session-v0994","facts":[{"fact_id":"fact-v0994-shortage-a","snapshot_id":"snapshot-v0994-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0994","product:server-v0994"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W35"},"measures":{"short_qty":390},"source_ref":"connector:erp:material-shortage","confidence":0.94}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0994","session_id":"session-v0994","profile":{"profile_id":"cockpit-profile-v0994-ops","owner_ref":"user:ops-planner","display_name":"Ops planner payload cockpit","focus_refs":["component:gpu-v0994"],"focus_metric_ids":["material_shortage_risk"],"thresholds":{"material_shortage_risk":{"critical":100,"warning":40}},"template_id":"ops.default","cadence":"daily"}}' \
  | rg -q '"profile_id":"cockpit-profile-v0994-ops"'

curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/cockpit-profile-v0994-ops/reports/generate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0994-report","session_id":"session-v0994","report":{"report_id":"cockpit-report-v0994-daily","cadence":"daily","delivery_ref":"channel://feishu/user/ops-planner","note":"payload template report snapshot"}}' \
  | rg -q '"report_id":"cockpit-report-v0994-daily"'

deliver_body="$(python3 - <<'PY'
import json

print(json.dumps({
    "mode": "dry_run",
    "idempotency_key": "v0994-report-delivery-cockpit-report-v0994-daily",
    "actor_principal": "user:ops-planner",
    "actor_identity_ref": "iacc://operator/ops-planner",
    "channel": "feishu",
    "template_id": "ops.alert.compact",
    "target_ref": "channel://feishu/user/ops-planner"
}))
PY
)"

delivery_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0994-daily/deliver" \
  -H 'content-type: application/json' \
  -d "$deliver_body")"
printf '%s' "$delivery_json" | rg -q '"kind":"iacc.cockpit.report_delivery"'
printf '%s' "$delivery_json" | rg -q '"idempotent_replay":false'
printf '%s' "$delivery_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); p=d["delivery_payload"]; r=d["cross_plane_execution_receipt"]; assert p["channel"] == "feishu"; assert p["template_id"] == "ops.alert.compact"; assert p["requested_capability"] == "channel.feishu.send_text"; assert p["resource_ref"].startswith("text://"); assert "payload_kind:text" in p["constraints"]; assert "target_ref_present" in p["constraints"]; assert r["action"]["resource_ref"] == p["resource_ref"]; assert r["action"]["requested_capability"] == p["requested_capability"]; assert r["action"]["target_ref"] == p["target_ref"]; assert d["report"]["status"] == "delivery_planned"; assert len(d["report"]["delivery_receipts"]) == 1'
receipt_id="$(printf '%s' "$delivery_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["cross_plane_execution_receipt"]["id"])')"

curl -fsS "$BASE_URL/api/cross-plane/action/executions" | rg -q "$receipt_id"

replay_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0994-daily/deliver" \
  -H 'content-type: application/json' \
  -d "$deliver_body")"
python3 - "$receipt_id" "$replay_json" <<'PY'
import json
import sys

d = json.loads(sys.argv[2])
assert d["idempotent_replay"] is True
assert d["cross_plane_execution_receipt"]["id"] == sys.argv[1]
assert d["delivery_payload"]["template_id"] == "ops.alert.compact"
assert len(d["report"]["delivery_receipts"]) == 1
PY

test -f "$WORKDIR/.cowd/iacc.sqlite"
test -f "$CONFIG_HOME/cross-plane/control-state.json"
