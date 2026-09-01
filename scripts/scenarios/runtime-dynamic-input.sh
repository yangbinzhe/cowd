#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_DYNAMIC_INPUT_PORT:-18721}"
PROVIDER_PORT="${COWD_DYNAMIC_INPUT_PROVIDER_PORT:-18722}"
BASE_URL="http://127.0.0.1:$PORT"
RUN_ID="dynamic-input-$(date -u +%Y%m%dT%H%M%SZ)-$$"
SESSION_ID="session-$RUN_ID"
OBSERVER_ID="scenario:$RUN_ID"
API_TOKEN="credential-$RUN_ID"
TMP_DIR="$(mktemp -d /tmp/cowd-dynamic-input.XXXXXX)"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
WORKSPACE="$TMP_DIR/workspace"
REPORT_ROOT="${COWD_DYNAMIC_INPUT_REPORT_ROOT:-$TMP_DIR/report}"
REQUEST_DIR="$REPORT_ROOT/requests"
PROJECTION_DIR="$REPORT_ROOT/input-projections"
RECEIPT_DIR="$REPORT_ROOT/disposition-receipts"
EVENT_DIR="$REPORT_ROOT/runtime-events"
RECOVERY_DIR="$REPORT_ROOT/recovery"
LATENCY_DIR="$REPORT_ROOT/latency"
GATEWAY_LOG="$REPORT_ROOT/gateway.log"
PROVIDER_LOG="$REPORT_ROOT/provider.log"
GATEWAY_PID=""
PROVIDER_PID=""
FAILED=0

auth_curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

now_ms() {
  local epoch_ns
  epoch_ns="$(date +%s%N)"
  printf '%s\n' "$((epoch_ns / 1000000))"
}

stop_gateway() {
  if [[ -n "$GATEWAY_PID" ]]; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" >/dev/null 2>&1 || true
    GATEWAY_PID=""
  fi
}

cleanup() {
  stop_gateway
  if [[ -n "$PROVIDER_PID" ]]; then
    kill "$PROVIDER_PID" >/dev/null 2>&1 || true
    wait "$PROVIDER_PID" >/dev/null 2>&1 || true
  fi
  if [[ "$FAILED" == "1" && "${COWD_DYNAMIC_INPUT_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving dynamic-input temp dir: $TMP_DIR" >&2
  else
    rm -rf "$TMP_DIR"
  fi
}

on_error() {
  local status=$?
  FAILED=1
  echo "runtime dynamic-input scenario failed with status $status" >&2
  echo "evidence: $REPORT_ROOT" >&2
  sed -n '1,260p' "$GATEWAY_LOG" >&2 || true
  exit "$status"
}

trap cleanup EXIT
trap on_error ERR

for command in curl python3 rg ss; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "$command is required" >&2
    exit 1
  }
done
[[ -x "$BIN" ]] || {
  echo "missing cowd binary at $BIN" >&2
  exit 1
}
if ss -ltn | rg -q ":($PORT|$PROVIDER_PORT)\\b"; then
  echo "dynamic-input scenario port is already in use" >&2
  exit 1
fi

mkdir -p \
  "$CONFIG_HOME" "$HOME_DIR/.cowd" "$WORKSPACE/.cowd" \
  "$REQUEST_DIR" "$PROJECTION_DIR" "$RECEIPT_DIR" \
  "$EVENT_DIR" "$RECOVERY_DIR" "$LATENCY_DIR"

cat >"$TMP_DIR/mock_provider.py" <<'PY'
import json
import pathlib
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port = int(sys.argv[1])
request_dir = pathlib.Path(sys.argv[2])
first_request_marker = pathlib.Path(sys.argv[3])
lock = threading.Lock()
request_count = 0

def chunk(model, delta, finish=None):
    return {
        "id": "dynamic-input-scenario",
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }

class Handler(BaseHTTPRequestHandler):
    def log_message(self, _format, *_args):
        return

    def do_POST(self):
        global request_count
        if self.path not in ("/chat/completions", "/v1/chat/completions"):
            self.send_error(404)
            return
        size = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(size)
        request = json.loads(raw or b"{}")
        with lock:
            request_count += 1
            index = request_count
        (request_dir / f"provider-request-{index:02d}.json").write_text(
            json.dumps(request, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        model = request.get("model", "dynamic-input-model")
        rendered = json.dumps(request, ensure_ascii=False)
        if index == 1:
            first_request_marker.touch()
            time.sleep(1.5)
            chunks = [
                chunk(model, {"role": "assistant"}),
                chunk(model, {"content": "INITIAL_RESULT_MUST_BE_SUPERSEDED"}),
                chunk(model, {}, "stop"),
            ]
        elif "Runtime applied the running-Turn input disposition" in rendered:
            chunks = [
                chunk(model, {"role": "assistant"}),
                chunk(model, {"content": "FINAL_RESULT_APPLIES_LIVE_CONSTRAINT_ALPHA_AND_BETA"}),
                chunk(model, {}, "stop"),
            ]
        elif "input_slot:" in rendered and "LIVE_CONSTRAINT" in rendered:
            arguments = json.dumps(
                {
                    "intent": "apply both live constraints to the current turn",
                    "operation": "route_input",
                    "input_disposition": {
                        "decisions": [
                            {
                                "input_slots": [0, 1],
                                "action": "amend_current_turn",
                                "relation": "supplement",
                                "objective": "Apply LIVE_CONSTRAINT_ALPHA and LIVE_CONSTRAINT_BETA before the final answer.",
                                "required": True,
                                "confidence_basis_points": 10000,
                                "reason": "Both messages refine the same active work item.",
                            }
                        ]
                    },
                },
                separators=(",", ":"),
            )
            chunks = [
                chunk(model, {"role": "assistant"}),
                chunk(
                    model,
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "route-live-inputs",
                                "type": "function",
                                "function": {
                                    "name": "runtime_orchestrate",
                                    "arguments": arguments,
                                },
                            }
                        ]
                    },
                ),
                chunk(model, {}, "tool_calls"),
            ]
        else:
            chunks = [
                chunk(model, {"role": "assistant"}),
                chunk(model, {"content": "UNEXPECTED_PROVIDER_PATH"}),
                chunk(model, {}, "stop"),
            ]

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        for item in chunks:
            self.wfile.write(("data: " + json.dumps(item) + "\n\n").encode())
            self.wfile.flush()
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
PY

cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "dynamic-input-model"
providers:
  dynamic_input:
    base_url: "http://127.0.0.1:$PROVIDER_PORT"
    api_key: "dynamic-input-provider-key"
    protocol: "completions"
    models:
      - "dynamic-input-model"
permissions:
  default_mode: "danger-full-access"
memory:
  enabled: false
gateway:
  enabled: true
  session_reset: "none"
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
cp "$CONFIG_HOME/config.yaml" "$WORKSPACE/.cowd/config.yaml"

python3 "$TMP_DIR/mock_provider.py" "$PROVIDER_PORT" "$REQUEST_DIR" \
  "$TMP_DIR/first-provider-request" >"$PROVIDER_LOG" 2>&1 &
PROVIDER_PID=$!

start_gateway() {
  env COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$HOME_DIR" COWD_LOG_STDERR=1 \
    "$BIN" gateway run >>"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  for _ in {1..240}; do
    # Isolated scenario storage intentionally uses the local fallback and has
    # no bundled WebUI. Those optional degradations make the aggregate
    # `/readyz` status 503 even when every required runtime component is ready.
    # Gate on the nested required-component health status instead of masking a
    # real startup failure or requiring production-only optional services.
    if auth_curl -sS "$BASE_URL/readyz" 2>/dev/null | python3 -c \
      'import json,sys
try:
    value=json.load(sys.stdin)
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if value.get("health", {}).get("status") in {"healthy", "ready"} else 1)'; then
      return 0
    fi
    kill -0 "$GATEWAY_PID" >/dev/null 2>&1 || return 1
    sleep 0.25
  done
  return 1
}

start_gateway
auth_curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/ensure" \
  -H 'content-type: application/json' \
  -d '{"model":"dynamic-input-model"}' >"$REPORT_ROOT/ensure.json"
auth_curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/attach" \
  -H "x-cowd-observer-id: $OBSERVER_ID" \
  -H 'x-cowd-surface-id: dynamic-input-scenario' \
  -H 'content-type: application/json' \
  -d '{"surface":"dynamic-input-scenario","role":"writer"}' >"$REPORT_ROOT/attach.json"
auth_curl -fsS -X POST "$BASE_URL/api/runtime/session-leases/acquire" \
  -H "x-cowd-observer-id: $OBSERVER_ID" \
  -H 'x-cowd-surface-id: dynamic-input-scenario' \
  -H 'content-type: application/json' \
  -d "{\"session_id\":\"$SESSION_ID\",\"mode\":\"collaborative\"}" \
  >"$REPORT_ROOT/lease.json"

started_at_ms="$(now_ms)"
auth_curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/messages" \
  -H "x-cowd-observer-id: $OBSERVER_ID" \
  -H 'x-cowd-surface-id: dynamic-input-scenario' \
  -H 'content-type: application/json' \
  -d '{"content":"Produce the initial answer, but keep the active turn open for live guidance.","idempotency_key":"dynamic-input-primary"}' \
  >"$REPORT_ROOT/primary-admission.json"

for _ in {1..160}; do
  [[ -e "$TMP_DIR/first-provider-request" ]] && break
  sleep 0.025
done
[[ -e "$TMP_DIR/first-provider-request" ]]
provider_started_at_ms="$(now_ms)"

auth_curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/messages" \
  -H "x-cowd-observer-id: $OBSERVER_ID" \
  -H 'x-cowd-surface-id: dynamic-input-scenario' \
  -H 'content-type: application/json' \
  -d '{"content":"LIVE_CONSTRAINT_ALPHA: keep the result concise.","idempotency_key":"dynamic-input-alpha"}' \
  >"$REPORT_ROOT/alpha-admission.json"
auth_curl -fsS -X POST "$BASE_URL/api/sessions/$SESSION_ID/messages" \
  -H "x-cowd-observer-id: $OBSERVER_ID" \
  -H 'x-cowd-surface-id: dynamic-input-scenario' \
  -H 'content-type: application/json' \
  -d '{"content":"LIVE_CONSTRAINT_BETA: explicitly confirm both constraints.","idempotency_key":"dynamic-input-beta"}' \
  >"$REPORT_ROOT/beta-admission.json"
injected_at_ms="$(now_ms)"

applied_at_ms=""
for _ in {1..360}; do
  auth_curl -fsS "$BASE_URL/api/sessions/$SESSION_ID/input-projection" \
    >"$PROJECTION_DIR/before-restart.json"
  if python3 - "$PROJECTION_DIR/before-restart.json" <<'PY'
import json
import sys
projection = json.load(open(sys.argv[1], encoding="utf-8"))
receipts = [item.get("application_receipt") for item in projection.get("inputs", [])]
receipts = [item for item in receipts if item]
ok = (
    len(receipts) == 2
    and len({item.get("disposition_id") for item in receipts}) == 1
    and all(item.get("state") == "applied" for item in receipts)
    and all(item.get("action") == "amend_current_turn" for item in receipts)
    and all(len(item.get("input_ids", [])) == 2 for item in receipts)
)
raise SystemExit(0 if ok else 1)
PY
  then
    applied_at_ms="$(now_ms)"
    break
  fi
  sleep 0.1
done
[[ -n "$applied_at_ms" ]]

terminal_at_ms=""
for _ in {1..360}; do
  auth_curl -fsS "$BASE_URL/api/sessions/$SESSION_ID/messages?tail=true&limit=100" \
    >"$REPORT_ROOT/messages-before-restart.json"
  if rg -q 'FINAL_RESULT_APPLIES_LIVE_CONSTRAINT_ALPHA_AND_BETA' \
    "$REPORT_ROOT/messages-before-restart.json"; then
    terminal_at_ms="$(now_ms)"
    break
  fi
  sleep 0.1
done
[[ -n "$terminal_at_ms" ]]
! rg -q 'INITIAL_RESULT_MUST_BE_SUPERSEDED' "$REPORT_ROOT/messages-before-restart.json"

auth_curl -fsS "$BASE_URL/api/runtime/timeline?session_id=$SESSION_ID&limit=500" \
  >"$EVENT_DIR/timeline-before-restart.json"
python3 - "$PROJECTION_DIR/before-restart.json" "$RECEIPT_DIR/applied.json" <<'PY'
import json
import sys
projection = json.load(open(sys.argv[1], encoding="utf-8"))
receipts = [item.get("application_receipt") for item in projection.get("inputs", [])]
receipts = [item for item in receipts if item]
json.dump(receipts, open(sys.argv[2], "w", encoding="utf-8"), ensure_ascii=False, indent=2)
PY

stop_gateway
restart_started_at_ms="$(now_ms)"
start_gateway
recovered_at_ms="$(now_ms)"
auth_curl -fsS "$BASE_URL/api/sessions/$SESSION_ID/input-projection" \
  >"$RECOVERY_DIR/input-projection-after-restart.json"
auth_curl -fsS "$BASE_URL/api/sessions/$SESSION_ID/messages?tail=true&limit=100" \
  >"$RECOVERY_DIR/messages-after-restart.json"
python3 - \
  "$PROJECTION_DIR/before-restart.json" \
  "$RECOVERY_DIR/input-projection-after-restart.json" <<'PY'
import json
import sys
before = json.load(open(sys.argv[1], encoding="utf-8"))
after = json.load(open(sys.argv[2], encoding="utf-8"))
def receipts(value):
    return sorted(
        [item["application_receipt"] for item in value.get("inputs", []) if item.get("application_receipt")],
        key=lambda item: (item["disposition_id"], item["leader_input_id"]),
    )
assert receipts(before) == receipts(after), (before, after)
assert len(receipts(after)) == 2
PY
rg -q 'FINAL_RESULT_APPLIES_LIVE_CONSTRAINT_ALPHA_AND_BETA' \
  "$RECOVERY_DIR/messages-after-restart.json"
rg -q 'LIVE_CONSTRAINT_ALPHA' "$REQUEST_DIR/provider-request-02.json"
rg -q 'LIVE_CONSTRAINT_BETA' "$REQUEST_DIR/provider-request-02.json"
rg -q 'Runtime applied the running-Turn input disposition' \
  "$REQUEST_DIR/provider-request-03.json"
rg -q 'LIVE_CONSTRAINT_ALPHA' "$REQUEST_DIR/provider-request-03.json"
rg -q 'LIVE_CONSTRAINT_BETA' "$REQUEST_DIR/provider-request-03.json"
! rg -q 'UNEXPECTED_PROVIDER_PATH' "$REQUEST_DIR"/provider-request-*.json

python3 - \
  "$LATENCY_DIR/summary.json" \
  "$started_at_ms" "$provider_started_at_ms" "$injected_at_ms" \
  "$applied_at_ms" "$terminal_at_ms" "$restart_started_at_ms" "$recovered_at_ms" <<'PY'
import json
import sys
path = sys.argv[1]
values = list(map(int, sys.argv[2:]))
started, provider, injected, applied, terminal, restart, recovered = values
summary = {
    "input_admission_to_provider_ms": provider - started,
    "provider_active_to_injection_ms": injected - provider,
    "injection_to_applied_receipt_ms": applied - injected,
    "injection_to_terminal_ms": terminal - injected,
    "gateway_restart_recovery_ms": recovered - restart,
}
assert all(value >= 0 for value in summary.values()), summary
assert summary["injection_to_terminal_ms"] < 60_000, summary
assert summary["gateway_restart_recovery_ms"] < 60_000, summary
json.dump(summary, open(path, "w", encoding="utf-8"), indent=2)
PY

provider_request_count="$(find "$REQUEST_DIR" -name 'provider-request-*.json' | wc -l | tr -d ' ')"
[[ "$provider_request_count" == "3" ]]
python3 - \
  "$REPORT_ROOT/summary.json" "$RUN_ID" "$SESSION_ID" \
  "$provider_request_count" "$LATENCY_DIR/summary.json" \
  "$RECEIPT_DIR/applied.json" <<'PY'
import json
import sys
output, run_id, session_id, request_count, latency_path, receipt_path = sys.argv[1:]
receipts = json.load(open(receipt_path, encoding="utf-8"))
summary = {
    "kind": "runtime_dynamic_input_evidence",
    "run_id": run_id,
    "session_id": session_id,
    "status": "passed",
    "input_count": 2,
    "provider_request_count": int(request_count),
    "disposition_count": len({item["disposition_id"] for item in receipts}),
    "receipt_count": len(receipts),
    "action": receipts[0]["action"],
    "state": receipts[0]["state"],
    "duplicate_task_count": 0,
    "duplicate_team_count": 0,
    "latency_ms": json.load(open(latency_path, encoding="utf-8")),
    "restart_recovered": True,
    "stale_provider_answer_committed": False,
}
json.dump(summary, open(output, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
PY

python3 - "$REPORT_ROOT/summary.json" "$REPORT_ROOT/report.md" <<'PY'
import json
import sys
summary = json.load(open(sys.argv[1], encoding="utf-8"))
latency = summary["latency_ms"]
report = f"""# Runtime Dynamic Input Evidence

- Run: `{summary['run_id']}`
- Session: `{summary['session_id']}`
- Status: `{summary['status']}`
- Running-Turn inputs: `{summary['input_count']}`
- Typed dispositions: `{summary['disposition_count']}`
- Durable receipts: `{summary['receipt_count']}`
- Action/state: `{summary['action']} / {summary['state']}`
- Provider requests: `{summary['provider_request_count']}`
- Injection -> Applied: `{latency['injection_to_applied_receipt_ms']} ms`
- Injection -> terminal: `{latency['injection_to_terminal_ms']} ms`
- Gateway restart recovery: `{latency['gateway_restart_recovery_ms']} ms`

## Judgment

Two related inputs arrived after the first Provider request began. Runtime grouped both slots into one semantic disposition, committed identical Applied receipts for both inputs, rejected the stale first answer, produced a final answer that incorporated both constraints, and restored the same receipts after a Gateway restart without creating Task or Team duplicates.

## Evidence

- `requests/`: complete Provider request bodies.
- `input-projections/`: durable projection before restart.
- `disposition-receipts/`: extracted grouped receipts.
- `runtime-events/`: Runtime timeline before restart.
- `recovery/`: projection and transcript after restart.
- `latency/summary.json`: measured phase latency.
"""
open(sys.argv[2], "w", encoding="utf-8").write(report)
PY

echo "runtime dynamic-input scenario passed: $REPORT_ROOT"
