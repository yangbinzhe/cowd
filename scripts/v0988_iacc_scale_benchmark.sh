#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V0988_PORT:-18708}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v0988-iacc-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v0988-iacc.XXXXXX")"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
PRODUCTS="${COWD_V0988_PRODUCTS:-20}"
WEEKS="${COWD_V0988_WEEKS:-52}"
INGEST_LIMIT_MS="${COWD_V0988_INGEST_LIMIT_MS:-120000}"
COMPUTE_LIMIT_MS="${COWD_V0988_COMPUTE_LIMIT_MS:-120000}"
FACTS_JSON="$TMP_DIR/v0988-facts.json"
BENCHMARK_JSON="$TMP_DIR/iacc-v0988-scale-benchmark.json"

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
  echo "tmux is required for v0.9.88 IACC scale benchmark" >&2
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

python3 - "$PRODUCTS" "$WEEKS" >"$FACTS_JSON" <<'PY'
import json
import sys

products = int(sys.argv[1])
weeks = int(sys.argv[2])
facts = []
for product_idx in range(products):
    component = f"scale-component-{product_idx:03d}"
    for week_idx in range(1, weeks + 1):
        week = f"2026-W{week_idx:02d}"
        facts.append({
            "fact_id": f"fact-v0988-shortage-{product_idx:03d}-{week_idx:02d}",
            "snapshot_id": "snapshot-v0988-scale",
            "fact_type": "supply.material_shortage",
            "entity_refs": [f"component:{component}"],
            "metric_key": "material_shortage_risk",
            "dimensions": {"week": week, "component": component},
            "measures": {"short_qty": (product_idx % 7) + week_idx},
            "source_ref": "benchmark:v0988:generated-supply",
            "confidence": 0.88
        })

json.dump({
    "request_id": "v0988-scale-ingest",
    "session_id": "session-v0988",
    "facts": facts
}, sys.stdout)
PY

generated_count="$(python3 - "$FACTS_JSON" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    print(len(json.load(handle)["facts"]))
PY
)"
expected_count=$((PRODUCTS * WEEKS))
test "$generated_count" -eq "$expected_count"

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
curl -fsS "$BASE_URL/api/iacc/health" | rg -q '"incremental_compute_job"'

curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

ingest_start="$(python3 -c 'import time; print(int(time.time() * 1000))')"
ingest_json="$(curl -fsS "$BASE_URL/api/iacc/facts/ingest" \
  -H 'content-type: application/json' \
  --data @"$FACTS_JSON")"
ingest_end="$(python3 -c 'import time; print(int(time.time() * 1000))')"
ingest_ms=$((ingest_end - ingest_start))
printf '%s' "$ingest_json" | rg -q "\"ingested\":$generated_count"
if (( ingest_ms > INGEST_LIMIT_MS )); then
  echo "v0.9.88 ingest exceeded limit: ${ingest_ms}ms > ${INGEST_LIMIT_MS}ms" >&2
  exit 1
fi

plan_json="$(curl -fsS "$BASE_URL/api/iacc/compute/jobs/plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v0988-plan","session_id":"session-v0988","job":{"job_id":"compute-job-v0988-supply-shortage","trigger_fact_type":"supply.material_shortage","trigger_fact_refs":["benchmark:v0988:generated-supply"],"entity_scope":"component:*","period":"2026-W01..2026-W52"}}')"
printf '%s' "$plan_json" | rg -q '"status":"planned"'
printf '%s' "$plan_json" | rg -q '"material_shortage_risk"'
printf '%s' "$plan_json" | rg -q '"order_delivery_risk"'
job_id="$(printf '%s' "$plan_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["plan"]["job"]["job_id"])')"

compute_start="$(python3 -c 'import time; print(int(time.time() * 1000))')"
run_json="$(curl -fsS "$BASE_URL/api/iacc/compute/jobs/$job_id/run" -X POST)"
compute_end="$(python3 -c 'import time; print(int(time.time() * 1000))')"
compute_ms=$((compute_end - compute_start))
printf '%s' "$run_json" | rg -q '"status":"completed"'
metric_state_count="$(printf '%s' "$run_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["job"]["result_summary"]["metric_state_count"])')"
if (( metric_state_count < generated_count )); then
  echo "metric_state_count $metric_state_count is below generated_count $generated_count" >&2
  exit 1
fi
if (( compute_ms > COMPUTE_LIMIT_MS )); then
  echo "v0.9.88 compute exceeded limit: ${compute_ms}ms > ${COMPUTE_LIMIT_MS}ms" >&2
  exit 1
fi

if curl -fsS "$BASE_URL/api/iacc/metrics/work_center_load" >/dev/null 2>&1; then
  echo "work_center_load should not be recomputed by supply.material_shortage benchmark job" >&2
  exit 1
fi

fact_count="$(curl -fsS "$BASE_URL/api/iacc/health" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["fact_count"])')"
if (( fact_count < generated_count )); then
  echo "fact_count $fact_count is below generated_count $generated_count" >&2
  exit 1
fi

python3 - "$BENCHMARK_JSON" "$generated_count" "$metric_state_count" "$ingest_ms" "$compute_ms" <<'PY'
import json
import sys

path, generated_count, metric_state_count, ingest_ms, compute_ms = sys.argv[1:]
payload = {
    "kind": "iacc.v0988.scale_benchmark",
    "generated_fact_count": int(generated_count),
    "metric_state_count": int(metric_state_count),
    "ingest_ms": int(ingest_ms),
    "compute_ms": int(compute_ms),
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
PY
cp "$BENCHMARK_JSON" "$WORKDIR/.cowd/iacc-v0988-scale-benchmark.json"
test -f "$WORKDIR/.cowd/iacc-v0988-scale-benchmark.json"
