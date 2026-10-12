#!/usr/bin/env bash
# Replay run one31's spinning signature against the deep monitor and prove the
# escalation stops it.
#
# Run one31 printed the same finding ten times across six minutes while the run
# burned its whole window: the detector saw it, nothing counted it, and the
# harness stop gate demanded an event silence the loop never produced. This
# replay rebuilds that event shape in a disposable PostgreSQL (docker), runs the
# monitor three times with a state file, and asserts the third report escalates
# - the condition the harness now acts on.
#
# Usage:
#   scripts/diagnostics/monitor-escalation-replay.sh
#   COWD_MONITOR_REPLAY_PG_URL=postgresql://... scripts/diagnostics/monitor-escalation-replay.sh
#
# Exits non-zero if the escalation does not fire on the third report, if a
# convergence milestone does not reset it, or if the harness jq path does not
# read the field the monitor writes.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MONITOR="$ROOT/scripts/diagnostics/deep-run-monitor.py"
CONTAINER="cowd-monitor-replay-$$"
PG_URL="${COWD_MONITOR_REPLAY_PG_URL:-}"
STARTED_CONTAINER=0
STATE_DIR="$(mktemp -d /tmp/cowd-monitor-replay.XXXXXX)"
STATE_FILE="$STATE_DIR/state.json"

cleanup() {
  if [[ "$STARTED_CONTAINER" == "1" ]]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
  rm -rf "$STATE_DIR"
}
trap cleanup EXIT

if [[ -z "$PG_URL" ]]; then
  command -v docker >/dev/null 2>&1 || {
    echo "docker not available; set COWD_MONITOR_REPLAY_PG_URL to an isolated PostgreSQL" >&2
    exit 2
  }
  PORT=$(( 25400 + RANDOM % 1000 ))
  docker run -d --rm --name "$CONTAINER" \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=replay \
    -p "127.0.0.1:$PORT:5432" postgres:16-alpine >/dev/null
  STARTED_CONTAINER=1
  PG_URL="postgresql://postgres:postgres@127.0.0.1:$PORT/replay"
  # Wait for a real query, not pg_isready: the image's entrypoint runs a
  # temporary server during initialization, pg_isready succeeds against it, and
  # the first statement then dies with "server closed the connection
  # unexpectedly". Only a SELECT that returns separates the two phases.
  for _ in $(seq 1 120); do
    psql "$PG_URL" -tAc "SELECT 1" >/dev/null 2>&1 && break
    sleep 0.5
  done
fi

psql_q() { psql "$PG_URL" -v ON_ERROR_STOP=1 -q "$@"; }

# The event store's real shape (crates/runtime-postgres/src/lib.rs). The monitor
# reads only runtime_events; runtime_commits exists for the foreign key.
psql_q <<'SQL'
CREATE TABLE IF NOT EXISTS runtime_commits (
  commit_cursor BIGSERIAL PRIMARY KEY,
  transaction_id TEXT NOT NULL UNIQUE,
  request_hash TEXT NOT NULL,
  created_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_events (
  event_id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  sequence BIGINT NOT NULL,
  scope TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT,
  actor TEXT,
  payload JSONB NOT NULL,
  refs JSONB NOT NULL,
  created_at_ms BIGINT NOT NULL,
  commit_cursor BIGINT NOT NULL,
  transaction_id TEXT NOT NULL REFERENCES runtime_commits(transaction_id),
  transaction_index BIGINT NOT NULL,
  schema_version BIGINT DEFAULT 1,
  idempotency_key TEXT
);
SQL

# one31's signature: three artifact references, each read thirteen times, every
# call a read_ref so both the repeated-input and the redundant-read findings see
# them - exactly the two findings the live run printed. Reviews happened while
# the loop ran (objective_reviews 2 -> 3 -> 4), and events kept flowing, which
# is why the old event-silence gate never stopped it.
seed_loop() {
  local now_ms base i n=0
  now_ms=$(( $(date +%s) * 1000 ))
  base=$(( now_ms - 360000 ))
  psql_q -c "INSERT INTO runtime_commits (transaction_id, request_hash, created_at_ms) VALUES ('replay-tx','hash',$now_ms)"
  local refs=(
    83a19d51623c6e8987c1ac51
    d19f537ffb5070bccb9249f3
    46df7642c86b72c61dccb17c
  )
  # The content is a JSON string holding JSON text, so its quotes are escaped
  # once for SQL and once for JSON; the monitor reads it back as exactly the
  # `{"reference": "artifact:..."}` string the live run's findings quoted.
  local payload_call
  for round in 1 2 3 4 5 6 7 8 9 10 11 12 13; do
    for ref in "${refs[@]}"; do
      payload_call='{"kind":"tool_call","content":"{\"reference\": \"artifact:'"$ref"'\"}","tool_name":"read_ref"}'
      n=$(( n + 1 ))
      psql_q -c "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index) VALUES ('e$n','s',1,'conversation','model.item_completed','completed','root','$payload_call','[]',$(( base + n * 1000 )),1,'replay-tx',0)"
      n=$(( n + 1 ))
      psql_q -c "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index) VALUES ('e$n','s',1,'conversation','tool.invocation.completed','completed','root','{\"reference\":\"artifact:x\"}','[]',$(( base + n * 1000 )),1,'replay-tx',0)"
    done
  done
  for review in 1 2 3; do
    n=$(( n + 1 ))
    psql_q -c "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index) VALUES ('e$n','s',1,'agentic','agentic.action_applied','applied','root','{\"envelope\":{\"action\":{\"kind\":\"objective_review\",\"input\":{\"decision\":\"accept\"}}}}','[]',$(( base + n * 1000 )),1,'replay-tx',0)"
  done
  # The loop is live right now: the newest events are seconds old, so the
  # event-liveness signal stays green while nothing converges.
  n=$(( n + 1 ))
  payload_call='{"kind":"tool_call","content":"{\"reference\": \"artifact:83a19d51623c6e8987c1ac51\"}","tool_name":"read_ref"}'
  psql_q -c "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index) VALUES ('e$n','s',1,'conversation','model.item_completed','completed','root','$payload_call','[]',$(( now_ms - 3000 )),1,'replay-tx',0)"
}

seed_convergence() {
  local now_ms n
  now_ms=$(( $(date +%s) * 1000 ))
  n=$(( $(psql "$PG_URL" -tAc "SELECT count(*) FROM runtime_events") + 1000 ))
  psql_q -c "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index) VALUES ('c$n','s',1,'agentic','agentic.action_applied','applied','root','{\"envelope\":{\"action\":{\"kind\":\"objective_complete_request\",\"input\":{}}}}','[]',$now_ms,1,'replay-tx',0)"
  n=$(( n + 1 ))
  psql_q -c "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index) VALUES ('c$n','s',1,'agentic','agentic.verdict.recorded','recorded','root','{\"satisfied\":true}','[]',$now_ms,1,'replay-tx',0)"
}

field() { printf '%s' "$1" | jq -r "$2"; }

seed_loop
failures=0
previous_repeat=0
for round in 1 2 3; do
  report="$(python3 "$MONITOR" "$PG_URL" --investigate --state-file "$STATE_FILE")"
  state="$(field "$report" '.conclusion.state')"
  repeat="$(field "$report" '.verification.repeat_count')"
  escalate="$(field "$report" '.verification.escalate')"
  # The exact jq path the harness reads; a drift between the two layers is the
  # failure this whole patch exists to remove.
  harness_escalate="$(printf '%s' "$report" | jq -r '.verification.escalate // false')"
  echo "round $round: state=$state repeat_count=$repeat escalate=$escalate"
  if [[ "$state" != "SPINNING" ]]; then
    echo "FAIL round $round: the one31 signature must read as SPINNING" >&2
    failures=$(( failures + 1 ))
  fi
  if [[ "$repeat" -le "$previous_repeat" ]]; then
    echo "FAIL round $round: repeat_count must grow ($previous_repeat -> $repeat)" >&2
    failures=$(( failures + 1 ))
  fi
  if [[ "$harness_escalate" != "$escalate" ]]; then
    echo "FAIL round $round: harness jq path disagrees with the monitor ($harness_escalate vs $escalate)" >&2
    failures=$(( failures + 1 ))
  fi
  if [[ "$round" -lt 3 && "$escalate" == "true" ]]; then
    echo "FAIL round $round: escalation fired before the threshold" >&2
    failures=$(( failures + 1 ))
  fi
  if [[ "$round" -eq 3 ]]; then
    if [[ "$escalate" != "true" ]]; then
      echo "FAIL round 3: the confirmed repeat must escalate" >&2
      failures=$(( failures + 1 ))
    fi
    # The harness stop condition, evaluated exactly as the scenario script does.
    if [[ "$state" == "SPINNING" && "$escalate" == "true" ]]; then
      echo "round 3: harness stop path 1 fires (same finding, $repeat consecutive reports, no convergence)"
    else
      echo "FAIL round 3: the harness stop condition did not fire" >&2
      failures=$(( failures + 1 ))
    fi
  fi
  previous_repeat="$repeat"
done

seed_convergence
report="$(python3 "$MONITOR" "$PG_URL" --investigate --state-file "$STATE_FILE")"
state="$(field "$report" '.conclusion.state')"
repeat="$(field "$report" '.verification.repeat_count')"
echo "after convergence: state=$state repeat_count=$repeat"
if [[ "$state" != "TERMINAL" ]]; then
  echo "FAIL: a satisfied verdict must read as TERMINAL" >&2
  failures=$(( failures + 1 ))
fi
if [[ "$repeat" != "0" ]]; then
  echo "FAIL: a terminal run has nothing to repeat" >&2
  failures=$(( failures + 1 ))
fi

if [[ "$failures" -gt 0 ]]; then
  echo "monitor escalation replay: FAILED ($failures)" >&2
  exit 1
fi
echo "monitor escalation replay: PASSED"
