#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0990_PORT:-18710}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0990-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0990-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.90 IACC cross-plane action bridge scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"expected_schema_version":9'
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"cross_plane_action_bridge"'

curl -fsS "$BASE_URL/api/cross-plane/identities" \
  -H 'content-type: application/json' \
  -d '{"id":"idb-v0990-ops-planner","principal_id":"user:ops-planner","identity_ref":"iacc://operator/ops-planner","trust":"verified","source":"v0990-scenario","created_at":"2026-06-09T00:00:00Z","expires_at":null}' \
  | rg -q '"principal_id":"user:ops-planner"'

curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d '{"id":"grant-v0990-feishu-send","principal_id":"user:ops-planner","capability":"channel.feishu.send_text","account_id":null,"target_ref":null,"resource_ref":null,"source_channel":null,"grant_type":"persistent","expires_at":null,"remaining_uses":null,"created_by":"v0990-scenario","approval_id":null}' \
  | rg -q '"capability":"channel.feishu.send_text"'

curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0990","session_id":"session-v0990","facts":[{"fact_id":"fact-v0990-shortage-a","snapshot_id":"snapshot-v0990-shortage-a","fact_type":"supply.material_shortage","entity_refs":["component:gpu-v0990","product:server-v0990"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W31"},"measures":{"short_qty":300},"source_ref":"connector:erp:material-shortage","confidence":0.94}]}' \
  | rg -q '"ingested":1'

curl -fsS "$BASE_URL/api/iacc/metrics/recompute" -X POST | rg -q '"change_count":1'

attention_id="$(curl -fsS "$BASE_URL/api/iacc/attention/hot" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["items"][0]["attention_id"])')"
packet_id="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0990\",\"session_id\":\"session-v0990\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"v0.9.90 GPU shortage bridge incident\"}" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["packet"]["packet_id"])')"

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"v0990\",\"session_id\":\"session-v0990\",\"title\":\"GPU shortage bridge incident\",\"evidence_packet_id\":\"$packet_id\"}")"
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["incident"]["incident_id"])')"

analysis_json="$(curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST)"
analysis_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["analysis_id"])')"
action_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["analysis"]["recommended_actions"][0]["action_id"])')"

execution_json="$(curl -fsS "$BASE_URL/api/iacc/analyses/$analysis_id/actions/$action_id/execute" \
  -H 'content-type: application/json' \
  -d '{"mode":"commit","operator_id":"user:ops-planner","note":"queue supplier recovery through cross-plane bridge"}')"
execution_id="$(printf '%s' "$execution_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["execution"]["execution_id"])')"

bridge_body="$(python3 - "$execution_id" <<'PY'
import json
import sys

execution_id = sys.argv[1]
print(json.dumps({
    "mode": "dry_run",
    "idempotency_key": f"v0990-iacc-bridge-{execution_id}",
    "actor_principal": "user:ops-planner",
    "actor_identity_ref": "iacc://operator/ops-planner",
    "target_ref": "channel://feishu/user/ops-planner",
    "resource_ref": "text://IACC v0.9.90 supplier recovery task"
}))
PY
)"

bridge_json="$(curl -fsS "$BASE_URL/api/iacc/executions/$execution_id/cross-plane/execute" \
  -H 'content-type: application/json' \
  -d "$bridge_body")"
printf '%s' "$bridge_json" | rg -q '"kind":"iacc.cross_plane_action_bridge"'
printf '%s' "$bridge_json" | rg -q '"status":"planned"'
printf '%s' "$bridge_json" | rg -q '"dispatch_status":"dry_run"'
printf '%s' "$bridge_json" | rg -q '"idempotent_replay":false'
printf '%s' "$bridge_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["execution"]["status"] == "cross_plane_planned"; assert len(d["execution"]["cross_plane_receipts"]) == 1'
receipt_id="$(printf '%s' "$bridge_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["cross_plane_execution_receipt"]["id"])')"

curl -fsS "$BASE_URL/api/cross-plane/action/executions" | rg -q "$receipt_id"

replay_json="$(curl -fsS "$BASE_URL/api/iacc/executions/$execution_id/cross-plane/execute" \
  -H 'content-type: application/json' \
  -d "$bridge_body")"
printf '%s' "$replay_json" | rg -q '"idempotent_replay":true'
python3 - "$receipt_id" "$replay_json" <<'PY'
import json
import sys

d = json.loads(sys.argv[2])
assert d["cross_plane_execution_receipt"]["id"] == sys.argv[1]
assert len(d["execution"]["cross_plane_receipts"]) == 1
PY

curl -fsS "$BASE_URL/api/iacc/executions/$execution_id" | rg -q '"cross_plane_planned"'
test -f "$WORKDIR/.cowd/iacc.sqlite"
test -f "$CONFIG_HOME/cross-plane/control-state.json"
