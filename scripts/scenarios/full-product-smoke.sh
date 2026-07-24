#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INSTALL_DIR="${1:-${COWD_INSTALL_DIR:-}}"
if [[ -n "$INSTALL_DIR" ]]; then
  BIN="${COWD_BIN:-$INSTALL_DIR/cowd}"
else
  TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
  BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
fi

PORT="${COWD_RELEASE_SMOKE_PORT:-18695}"
PROVIDER_PORT="${COWD_RELEASE_SMOKE_PROVIDER_PORT:-18696}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-release-smoke-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-release-smoke.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
SMOKE_ID="release-smoke-$$"
PRINCIPAL="principal:local-human"
GRANT_ID="grant-$SMOKE_ID"
API_TOKEN="release-smoke-$$_credential"
SMOKE_OBSERVER_ID="release-smoke:$$_writer"
FAILED=0

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_RELEASE_SMOKE_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving release smoke temp dir: $TMP_DIR" >&2
    return
  fi
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}

print_logs() {
  echo "----- release smoke temp dir -----" >&2
  echo "$TMP_DIR" >&2
  echo "----- gateway log -----" >&2
  sed -n '1,260p' "$LOG" >&2 || true
  echo "-----------------------" >&2
}

on_error() {
  local status=$?
  FAILED=1
  echo "release full-product smoke failed with status $status" >&2
  print_logs
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for cmd in tmux curl python3 rg ss; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for release full-product smoke" >&2
    exit 1
  fi
done

if [[ ! -x "$BIN" ]]; then
  echo "missing executable cowd binary at $BIN" >&2
  exit 1
fi
if ss -ltnp | rg -q ":$PORT\\b|:$PROVIDER_PORT\\b"; then
  echo "release smoke port $PORT or provider port $PROVIDER_PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd/skills/release-smoke" "$CONFIG_HOME" "$HOME_DIR/.cowd"
cat >"$WORKDIR/.cowd/skills/release-smoke/SKILL.md" <<'EOF'
---
name: Release Smoke
description: Validate release evidence before reporting completion.
version: 1.0.0
---

# Release Smoke

Require a concrete evidence check before declaring a release scenario complete.
EOF

cat >"$TMP_DIR/mock_provider.py" <<'PY'
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_POST(self):
        if self.path not in ("/chat/completions", "/v1/chat/completions"):
            self.send_error(404)
            return
        size = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(size) or b"{}")
        model = request.get("model", "release-smoke-model")
        chunks = [
            {
                "id": "release-smoke",
                "object": "chat.completion.chunk",
                "model": model,
                "choices": [{"index": 0, "delta": {"content": "Release smoke completed."}, "finish_reason": None}],
            },
            {
                "id": "release-smoke",
                "object": "chat.completion.chunk",
                "model": model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            },
        ]
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        for chunk in chunks:
            self.wfile.write(("data: " + json.dumps(chunk) + "\n\n").encode())
            self.wfile.flush()
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
PY

cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "release-smoke-model"
providers:
  release_smoke:
    base_url: "http://127.0.0.1:$PROVIDER_PORT"
    api_key: "release-smoke-provider-key"
    protocol: "completions"
    models:
      - "release-smoke-model"
permissions:
  defaultMode: "dontAsk"
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
    (python3 '$TMP_DIR/mock_provider.py' '$PROVIDER_PORT' & \
    '$BIN' gateway run) >'$LOG' 2>&1\""

for _ in {1..120}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -fsS "$BASE_URL/health" >/dev/null
curl -fsS "$BASE_URL/healthz" >/dev/null
curl -fsS "$BASE_URL/readyz" | rg -q '"ready":true'
curl -fsS "$BASE_URL/api/webui/manifest" | rg -q '"config_key":"gateway.webui_dir"'

curl -fsS -X POST "$BASE_URL/api/sessions/$SMOKE_ID/ensure" \
  -H 'content-type: application/json' \
  -d '{"model":"release-smoke-model"}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is True and data.get("session_id") == sys.argv[1], data' "$SMOKE_ID"
curl -fsS -X POST "$BASE_URL/api/sessions/$SMOKE_ID/attach" \
  -H "x-cowd-observer-id: $SMOKE_OBSERVER_ID" \
  -H 'x-cowd-surface-id: release-smoke' \
  -H 'content-type: application/json' \
  -d '{"surface":"release-smoke","role":"writer"}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is not False, data'
curl -fsS -X POST "$BASE_URL/api/runtime/session-leases/acquire" \
  -H "x-cowd-observer-id: $SMOKE_OBSERVER_ID" \
  -H 'x-cowd-surface-id: release-smoke' \
  -H 'content-type: application/json' \
  -d "{\"session_id\":\"$SMOKE_ID\",\"mode\":\"collaborative\"}" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("ok") is not False, data'
curl -fsS -X POST "$BASE_URL/api/tasks/start" \
  -H 'content-type: application/json' \
  -d '{"objective":"release full product smoke","yolo_mode":true}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("status") == "running", data'
curl -fsS "$BASE_URL/api/runtime/snapshot" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert sys.argv[1] in (data.get("sessions") or []), data' "$SMOKE_ID"

curl -fsS -X POST "$BASE_URL/api/sessions/$SMOKE_ID/messages" \
  -H 'content-type: application/json' \
  -d '{"content":"Run the release smoke evidence check using the available release skill.","idempotency_key":"release-smoke-skill-activation"}' \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("status") == "accepted", data'
skill_activation_observed=0
for _ in {1..120}; do
  timeline_json="$(curl -fsS "$BASE_URL/api/runtime/timeline?session_id=$SMOKE_ID&from_seq=0&limit=200" || true)"
  if printf '%s' "$timeline_json" \
    | python3 -c 'import json,sys; events=json.load(sys.stdin).get("events", []); activation=any(e.get("kind")=="skill_candidates" and e.get("payload",{}).get("source")=="conversation_runtime.skill_activation" and e.get("payload",{}).get("selected")=="release-smoke" for e in events); bridge=any(e.get("kind")=="skill_memory_candidate" and e.get("payload",{}).get("source")=="conversation_runtime.skill_memory_candidate" and e.get("payload",{}).get("selected")=="release-smoke" for e in events); raise SystemExit(0 if activation and bridge else 1)'; then
    skill_activation_observed=1
    break
  fi
  sleep 0.25
done
[[ "$skill_activation_observed" == "1" ]]
curl -fsS "$BASE_URL/api/runtime/timeline?session_id=$SMOKE_ID&from_seq=0&limit=200" \
  | python3 -c 'import json,sys; events=json.load(sys.stdin).get("events", []); assert any(e.get("kind")=="skill_candidates" and e.get("payload",{}).get("source")=="conversation_runtime.skill_activation" and e.get("payload",{}).get("selected")=="release-smoke" for e in events), events; assert any(e.get("kind")=="skill_memory_candidate" and e.get("payload",{}).get("source")=="conversation_runtime.skill_memory_candidate" and e.get("payload",{}).get("selected")=="release-smoke" for e in events), events'

fact_json="$(curl -fsS "$BASE_URL/api/matrix/facts/ingest" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"release-smoke-fact\",\"session_id\":\"$SMOKE_ID\",\"facts\":[{\"fact_id\":\"fact-$SMOKE_ID\",\"snapshot_id\":\"snapshot-$SMOKE_ID\",\"fact_type\":\"supply.material_shortage\",\"entity_refs\":[\"component:gpu-a\"],\"metric_key\":\"material_shortage_risk\",\"dimensions\":{\"week\":\"2026-W24\"},\"measures\":{\"short_qty\":42},\"source_ref\":\"connector:local.docs:gpu-shortage\",\"confidence\":0.91}]}")"
attention_id="$(printf '%s' "$fact_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data["attention"][0]["attention_id"])')"

curl -fsS "$BASE_URL/api/matrix/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"release-smoke-evidence\",\"session_id\":\"$SMOKE_ID\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"Release smoke validates structured evidence and outcome timeline\"}" \
  | rg -q '"kind":"matrix.evidence.packet"'
curl -fsS "$BASE_URL/api/runtime/timeline?session_id=$SMOKE_ID&from_seq=0&limit=200" \
  | rg -q '"kind":"execution.outcome"'

release_gate_json="$(curl -fsS "$BASE_URL/api/cowd/release-gate")"
printf '%s' "$release_gate_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); checks={item.get("check_id"): item.get("status") for item in data.get("checks", [])}; assert data.get("status")=="pass", data; required=["structured_data.indexes.ready","structured_data.watermark.persistent","execution_outcome.timeline.available"]; missing=[item for item in required if checks.get(item)!="pass"]; assert not missing, f"release gate checks not passing: {missing}"'

curl -fsS "$BASE_URL/api/context/current?q=release%20full%20product&session_id=$SMOKE_ID" \
  | rg -q '"session_id"\s*:\s*"'"$SMOKE_ID"'"|release full product'

curl -fsS "$BASE_URL/api/memory/L3" \
  -H 'content-type: application/json' \
  -d '{"title":"RELEASE_SMOKE_MEMORY","content":"Release smoke validates memory runtime wiring.","tags":["release-smoke"],"category":"Reference","priority":"High"}' \
  | rg -q '"id"'
curl -fsS "$BASE_URL/api/memory/runtime" | rg -q '"runtime"'

curl -fsS "$BASE_URL/api/connectors/summary" | rg -q '"kind"\s*:\s*"connector_summary"'
curl -fsS "$BASE_URL/api/cross-plane/grants" \
  -H 'content-type: application/json' \
  -d "{\"id\":\"$GRANT_ID\",\"principal_id\":\"$PRINCIPAL\",\"capability\":\"service.local.docs.read\",\"grant_type\":\"single_use\",\"created_by\":\"release-smoke\"}" \
  | rg -q "\"$GRANT_ID\""
curl -fsS "$BASE_URL/api/connectors/services/local.docs/execute" \
  -H 'content-type: application/json' \
  -d "{\"source_channel\":\"channel://tui/release\",\"session_id\":\"$SMOKE_ID\",\"tool_id\":\"service.local.docs.read\",\"resource_id\":\"release-doc-$SMOKE_ID\",\"title\":\"Release Smoke Doc\",\"mode\":\"commit\",\"idempotency_key\":\"release-$SMOKE_ID\"}" \
  | rg -q '"status"\s*:\s*"executed"'
curl -fsS "$BASE_URL/api/cross-plane/audit" | rg -q "\"consumed_grant_id\"\\s*:\\s*\"$GRANT_ID\""

curl -fsS "$BASE_URL/api/connectors/resources" \
  | rg -q "release-doc-$SMOKE_ID|Release Smoke Doc"

echo "release full-product smoke passed"
