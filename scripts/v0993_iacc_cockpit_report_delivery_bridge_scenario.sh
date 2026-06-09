#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0993_PORT:-18713}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0993-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0993-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.93 IACC cockpit report delivery bridge scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":11'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_report_snapshot"'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_report_delivery_bridge"'

curl -fsS "$BASE_URL/api/cross-plane/identities" \
  -H 'content-type: application/json' \
  -d '{"id":"idb-v0993-ops-planner","principal_id":"user:ops-planner","identity_ref":"iacc://operator/ops-planner","trust":"verified","source":"v0993-scenario","created_at":"2026-06-09T00:00:00Z","expires_at":null}' \
  | rg -q '"principal_id":"user:ops-planner"'

curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d '{"id":"grant-v0993-feishu-send","principal_id":"user:ops-planner","capability":"channel.feishu.send_text","account_id":null,"target_ref":null,"resource_ref":null,"source_channel":null,"grant_type":"persistent","expires_at":null,"remaining_uses":null,"created_by":"v0993-scenario","approval_id":null}' \
  | rg -q '"capability":"channel.feishu.send_text"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0993","session_id":"session-v0993","facts":[{"fact_id":"fact-v0993-shortage-a","snapshot_id":"snapshot-v0993-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0993","product:server-v0993"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W34"},"measures":{"short_qty":360},"source_ref":"connector:erp:material-shortage","confidence":0.94}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_id="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0993\",\"session_id\":\"session-v0993\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.93 GPU shortage report delivery incident\"}" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"
curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id/quality-gate" -X POST | rg -q '"decision":"review"'

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0993\",\"session_id\":\"session-v0993\",\"title\":\"GPU shortage report delivery incident\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"
analysis_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST)"
analysis_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["analysis_id"])')"
action_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["recommended_actions"][0]["action_id"])')"
curl -fsS "$BASE_URL/api/iacc/evidence/$packet_id/quality-gate" -X POST | rg -q '"decision":"pass"'

curl -fsS "$BASE_URL/api/iacc/analyses/$analysis_id/actions/$action_id/execute" \
  -H 'content-type: application/json' \
  -d '{"mode":"commit","operator_id":"user:ops-planner","note":"queue report-visible recovery action"}' \
  | rg -q '"status":"queued_for_human_review"'

curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0993","session_id":"session-v0993","profile":{"profile_id":"cockpit-profile-v0993-ops","owner_ref":"user:ops-planner","display_name":"Ops planner delivery cockpit","focus_refs":["component:gpu-v0993"],"focus_metric_ids":["material_shortage_risk"],"thresholds":{"material_shortage_risk":{"critical":100,"warning":40}},"template_id":"ops.default","cadence":"daily"}}' \
  | rg -q '"profile_id":"cockpit-profile-v0993-ops"'

report_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/cockpit-profile-v0993-ops/reports/generate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0993-report","session_id":"session-v0993","report":{"report_id":"cockpit-report-v0993-daily","cadence":"daily","delivery_ref":"channel://feishu/user/ops-planner","note":"daily delivery report snapshot"}}')"
printf '%s' "$report_json" | rg -q '"kind":"iacc.cockpit.report"'
printf '%s' "$report_json" | rg -q '"report_id":"cockpit-report-v0993-daily"'
printf '%s' "$report_json" | rg -q '"status":"generated"'

deliver_body="$(python3 - <<'PY'
import json

print(json.dumps({
    "mode": "dry_run",
    "idempotency_key": "v0993-report-delivery-cockpit-report-v0993-daily",
    "actor_principal": "user:ops-planner",
    "actor_identity_ref": "iacc://operator/ops-planner",
    "target_ref": "channel://feishu/user/ops-planner"
}))
PY
)"

delivery_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0993-daily/deliver" \
  -H 'content-type: application/json' \
  -d "$deliver_body")"
printf '%s' "$delivery_json" | rg -q '"kind":"iacc.cockpit.report_delivery"'
printf '%s' "$delivery_json" | rg -q '"status":"planned"'
printf '%s' "$delivery_json" | rg -q '"dispatch_status":"dry_run"'
printf '%s' "$delivery_json" | rg -q '"idempotent_replay":false'
printf '%s' "$delivery_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["report"]["status"] == "delivery_planned"; assert len(d["report"]["delivery_receipts"]) == 1'
receipt_id="$(printf '%s' "$delivery_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["cross_plane_execution_receipt"]["id"])')"

curl -fsS "$BASE_URL/api/cross-plane/action/executions" | rg -q "$receipt_id"

replay_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0993-daily/deliver" \
  -H 'content-type: application/json' \
  -d "$deliver_body")"
printf '%s' "$replay_json" | rg -q '"idempotent_replay":true'
python3 - "$receipt_id" "$replay_json" <<'PY'
import json
import sys

d = json.loads(sys.argv[2])
assert d["cross_plane_execution_receipt"]["id"] == sys.argv[1]
assert d["report"]["status"] == "delivery_planned"
assert len(d["report"]["delivery_receipts"]) == 1
PY

curl -fsS "$BASE_URL/api/iacc/cockpit/reports/cockpit-report-v0993-daily" | rg -q '"delivery_planned"'
test -f "$WORKDIR/.cowd/iacc.sqlite"
test -f "$CONFIG_HOME/cross-plane/control-state.json"
