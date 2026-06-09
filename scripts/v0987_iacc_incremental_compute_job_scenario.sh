#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0987_PORT:-18707}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0987-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0987-iacc.XXXXXX")"
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
  echo "tmux is required for v0.9.87 IACC incremental compute scenario" >&2
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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"incremental_compute_job"'

curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

plan_json="$(curl -fsS "$BASE_URL/api/iacc/compute/jobs/plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0987-plan","session_id":"session-v0987","job":{"job_id":"compute-job-v0987-supply-commit","trigger_fact_type":"supply.commit_variance","trigger_fact_refs":["iacc:fact:fact-smfg-commit-gpu-alpha-w30"],"entity_scope":"supplier:supplier-gpu-alpha","period":"2026-W30"}}')"
printf '%s' "$plan_json" | rg -q '"status":"planned"'
printf '%s' "$plan_json" | rg -q '"supplier_commit_variance"'
printf '%s' "$plan_json" | rg -q '"material_shortage_risk"'
printf '%s' "$plan_json" | rg -q '"order_delivery_risk"'

job_id="$(printf '%s' "$plan_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["plan"]["job"]["job_id"])')"

run_json="$(curl -fsS "$BASE_URL/api/iacc/compute/jobs/$job_id/run" -X POST)"
printf '%s' "$run_json" | rg -q '"status":"completed"'
printf '%s' "$run_json" | rg -q '"attempts":1'
printf '%s' "$run_json" | rg -q '"metric_state_count":3'
printf '%s' "$run_json" | rg -q '"change_count":3'

curl -fsS "$BASE_URL/api/iacc/compute/jobs/$job_id" | rg -q '"status":"completed"'
curl -fsS "$BASE_URL/api/iacc/metrics/supplier_commit_variance" | rg -q '"supplier_commit_variance"'
if curl -fsS "$BASE_URL/api/iacc/metrics/work_center_load" >/dev/null 2>&1; then
  echo "work_center_load should not be recomputed by supply.commit_variance job" >&2
  exit 1
fi

curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"compute_job_count":1'

test -f "$WORKDIR/.cowd/iacc.sqlite"
