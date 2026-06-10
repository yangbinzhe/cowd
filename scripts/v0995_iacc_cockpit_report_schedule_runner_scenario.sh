#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0995_PORT:-18715}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0995-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0995-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.95 IACC report schedule runner scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cockpit_report_schedule_runner"'

curl -fsS "$BASE_URL/api/cross-plane/identities" \
  -H 'content-type: application/json' \
  -d '{"id":"idb-v0995-ops-planner","principal_id":"user:ops-planner","identity_ref":"iacc://operator/ops-planner","trust":"verified","source":"v0995-scenario","created_at":"2026-06-10T00:00:00Z","expires_at":null}' \
  | rg -q '"principal_id":"user:ops-planner"'

curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d '{"id":"grant-v0995-feishu-send","principal_id":"user:ops-planner","capability":"channel.feishu.send_text","account_id":null,"target_ref":null,"resource_ref":null,"source_channel":null,"grant_type":"persistent","expires_at":null,"remaining_uses":null,"created_by":"v0995-scenario","approval_id":null}' \
  | rg -q '"capability":"channel.feishu.send_text"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0995","session_id":"session-v0995","facts":[{"fact_id":"fact-v0995-shortage-a","snapshot_id":"snapshot-v0995-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0995","product:server-v0995"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W36"},"measures":{"short_qty":420},"source_ref":"connector:erp:material-shortage","confidence":0.94}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0995-daily","session_id":"session-v0995","profile":{"profile_id":"cockpit-profile-v0995-ops","owner_ref":"user:ops-planner","display_name":"Ops planner schedule cockpit","focus_refs":["component:gpu-v0995"],"focus_metric_ids":["material_shortage_risk"],"thresholds":{"material_shortage_risk":{"critical":100,"warning":40}},"template_id":"ops.default","cadence":"daily"}}' \
  | rg -q '"profile_id":"cockpit-profile-v0995-ops"'

curl -fsS "$BASE_URL/api/iacc/cockpit/profiles/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0995-weekly","session_id":"session-v0995","profile":{"profile_id":"cockpit-profile-v0995-weekly","owner_ref":"user:ops-planner","display_name":"Ops planner weekly cockpit","focus_refs":["component:gpu-v0995"],"focus_metric_ids":["material_shortage_risk"],"thresholds":{},"template_id":"ops.default","cadence":"weekly"}}' \
  | rg -q '"profile_id":"cockpit-profile-v0995-weekly"'

schedule_body="$(python3 - <<'PY'
import json

print(json.dumps({
    "request_id": "v0995",
    "session_id": "session-v0995",
    "cadence": "daily",
    "limit": 10,
    "report_id_prefix": "cockpit-report-v0995",
    "delivery_ref": "channel://feishu/user/ops-planner",
    "deliver": True,
    "mode": "dry_run",
    "actor_principal": "user:ops-planner",
    "actor_identity_ref": "iacc://operator/ops-planner",
    "channel": "feishu",
    "template_id": "ops.alert.compact"
}))
PY
)"

run_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/schedules/run" \
  -H 'content-type: application/json' \
  -d "$schedule_body")"
printf '%s' "$run_json" | rg -q '"kind":"iacc.cockpit.report_schedule_run"'
printf '%s' "$run_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["matched_profile_count"] == 1; assert d["generated_report_count"] == 1; assert d["delivery_count"] == 1; item=d["items"][0]; assert item["profile_id"] == "cockpit-profile-v0995-ops"; assert item["report"]["report_id"] == "cockpit-report-v0995-cockpit-profile-v0995-ops"; assert item["report"]["status"] == "delivery_planned"; assert len(item["report"]["delivery_receipts"]) == 1; delivery=item["delivery"]; assert delivery["idempotent_replay"] is False; assert delivery["delivery_payload"]["template_id"] == "ops.alert.compact"; assert delivery["cross_plane_execution_receipt"]["status"] == "planned"'
receipt_id="$(printf '%s' "$run_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["delivery"]["cross_plane_execution_receipt"]["id"])')"

curl -fsS "$BASE_URL/api/cross-plane/action/executions" | rg -q "$receipt_id"

replay_json="$(curl -fsS "$BASE_URL/api/iacc/cockpit/reports/schedules/run" \
  -H 'content-type: application/json' \
  -d "$schedule_body")"
python3 - "$receipt_id" "$replay_json" <<'PY'
import json
import sys

d = json.loads(sys.argv[2])
assert d["matched_profile_count"] == 1
assert d["delivery_count"] == 1
delivery = d["items"][0]["delivery"]
assert delivery["idempotent_replay"] is True
assert delivery["cross_plane_execution_receipt"]["id"] == sys.argv[1]
assert len(d["items"][0]["report"]["delivery_receipts"]) == 1
PY

test -f "$WORKDIR/.cowd/iacc.sqlite"
test -f "$CONFIG_HOME/cross-plane/control-state.json"
