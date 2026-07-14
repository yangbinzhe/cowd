#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_AGENT_GRAPH_PORT:-18691}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-agent-graph-$$"
TMP_DIR="$(mktemp -d /tmp/cowd-agent-graph.XXXXXX)"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
API_TOKEN="agent-graph-$$_credential"
AUTH_BROKER_BIN="${COWD_AUTH_BROKER_BIN:-$TARGET_ROOT/debug/cowd-auth-broker}"

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for agent graph scenario" >&2
  exit 1
fi

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi
if [[ ! -x "$AUTH_BROKER_BIN" ]]; then
  echo "cowd-auth-broker is required at $AUTH_BROKER_BIN" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
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
        enabled: true
        token: "$API_TOKEN"
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export COWD_AUTH_BROKER_BIN='$AUTH_BROKER_BIN' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..80}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

task_json="$(curl -fsS "$BASE_URL/api/tasks/start" \
  -H 'content-type: application/json' \
  --data '{"objective":"multi-agent graph scenario","yolo_mode":true}')"
task_id="$(printf '%s' "$task_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"

phase_json="$(curl -fsS "$BASE_URL/api/tasks/$task_id/phases" \
  -H 'content-type: application/json' \
  --data '{"name":"agent-review","objective":"review graph projection","plan":["create graph"],"acceptance":["graph visible"],"test_commands":["curl agents runs"]}')"
phase_id="$(printf '%s' "$phase_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["phases"][-1]["id"])')"

curl -fsS "$BASE_URL/api/tasks/$task_id/phases/$phase_id/artifacts" \
  -H 'content-type: application/json' \
  --data '{"kind":"test","label":"curl","value":"agent graph visible"}' >/dev/null

curl -fsS "$BASE_URL/api/tasks/$task_id/phases/$phase_id/review" \
  -H 'content-type: application/json' \
  --data '{"result":"accepted by agent graph scenario","completed":true}' >/dev/null

graph_json="$(curl -fsS -X POST "$BASE_URL/api/tasks/$task_id/execution-graph" \
  -H 'content-type: application/json' \
  --data '{
    "objective":"multi-agent graph scenario",
    "nodes":[{
      "id":"agent-review-node",
      "kind":"agent_task",
      "payload_ref":"task-phase:agent-review",
      "executor_kind":"runtime.agent",
      "idempotency_key":"agent-graph-scenario",
      "lease_ref":null,
      "acceptance":{"criteria":["graph visible"],"required_evidence":[],"minimum_score_basis_points":null},
      "retry_policy":{"max_attempts":1,"retryable_failure_kinds":[],"base_backoff_ms":500,"maximum_backoff_ms":30000},
      "resource_scopes":[]
    }],
    "edges":[]
  }')"
printf '%s' "$graph_json" | rg -q '"graph_id":"execution-graph-task-'
printf '%s' "$graph_json" | rg -q '"kind":"agent_task"'
curl -fsS "$BASE_URL/api/agents/execution-graphs" \
  | rg -q '"kind":"execution_graphs"'
curl -fsS "$BASE_URL/api/tasks/$task_id/execution-graph" \
  | rg -q '"agent-review-node"'
