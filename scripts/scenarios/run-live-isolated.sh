#!/usr/bin/env bash
# Run ONE live scenario against a FRESH PostgreSQL database.
#
# Each run provisions a uniquely named database so cross-run fixture pollution
# cannot make results non-reproducible. By DEFAULT the database is RETAINED
# afterwards so a failure can be investigated in detail (logs + mid-process
# content) and the run can be resumed from a breakpoint or from any point
# (Cowd natively recovers the durable execution graph from the same database
# and session). Set COWD_EVAL_KEEP_DB=0 to drop it as before.
#
# Generic by design: point COWD_PG_BASE_URL at any maintenance database (any
# host/port/user).
#
# Usage:
#   COWD_PG_BASE_URL='postgres://user:pass@host:port/postgres' \
#   COWD_EVAL_LIVE_SCENARIOS=live_tool_evidence \
#   (optional) COWD_EVAL_SCENARIO_TIMEOUT_SECS=900 \
#   (optional) COWD_EVAL_KEEP_DB=0   # drop the fresh database afterwards \
#   bash scripts/scenarios/run-live-isolated.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

: "${COWD_PG_BASE_URL:=postgres://postgres:cowd-owned-local-fixture@127.0.0.1:32772/postgres}"
: "${COWD_EVAL_LIVE_SCENARIOS:?set COWD_EVAL_LIVE_SCENARIOS to exactly one scenario id}"

# Retention policy: keep the fresh database ONLY for long, complex full-flow
# scenarios, which can break mid-run and then need the database for resume and
# troubleshooting. Light/temporary scenario runs are cleaned up so databases
# never accumulate. Override explicitly with COWD_EVAL_KEEP_DB=1 (force keep)
# or COWD_EVAL_KEEP_DB=0 (force drop).
if [[ -z "${COWD_EVAL_KEEP_DB:-}" ]]; then
  case "$COWD_EVAL_LIVE_SCENARIOS" in
    live_autonomous_collaboration_deepseek|live_qwen38_large_scale_collaboration|live_group_theory_ai_research_simulation)
      COWD_EVAL_KEEP_DB=1
      ;;
    *)
      COWD_EVAL_KEEP_DB=0
      ;;
  esac
fi

command -v psql >/dev/null || { echo 'psql is required to provision the fresh eval database' >&2; exit 2; }

# A retained database can be reused to continue an earlier run instead of
# provisioning a fresh one. A reused database is NEVER dropped, regardless of
# COWD_EVAL_KEEP_DB, so a long complex run can always be resumed again.
REUSED_DB=0
if [[ -n "${COWD_EVAL_REUSE_DB_URL:-}" ]]; then
  DB_URL="$COWD_EVAL_REUSE_DB_URL"
  DB_NAME="${DB_URL##*/}"
  REUSED_DB=1
else
  DB_NAME="cowd_eval_$(date +%s)_$RANDOM"
  DB_URL="${COWD_PG_BASE_URL%/*}/$DB_NAME"
fi

cleanup() {
  local status=$?
  trap - EXIT
  if [[ "$REUSED_DB" == "1" || "$COWD_EVAL_KEEP_DB" == "1" ]]; then
    echo "RETAINED eval database: $DB_NAME" >&2
    echo "  url: $DB_URL" >&2
    echo "  inspect: psql \"$DB_URL\"" >&2
    echo "  resume : rerun with COWD_EVAL_REUSE_DB_URL=\"$DB_URL\"" >&2
  else
    psql "$COWD_PG_BASE_URL" -v ON_ERROR_STOP=0 \
      -c "DROP DATABASE IF EXISTS \"$DB_NAME\" WITH (FORCE)" >/dev/null 2>&1 || true
    echo "dropped fresh eval database: $DB_NAME" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

if [[ "$REUSED_DB" == "0" ]]; then
  psql "$COWD_PG_BASE_URL" -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE \"$DB_NAME\"" >/dev/null
  echo "created fresh eval database: $DB_NAME" >&2
else
  echo "reusing retained eval database: $DB_NAME" >&2
fi

export COWD_TEST_POSTGRES_URL="$DB_URL"
bash scripts/scenarios/harness-eval-real-qwen.sh
