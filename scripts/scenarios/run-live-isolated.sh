#!/usr/bin/env bash
# Run ONE live scenario against a FRESH, disposable PostgreSQL database, then
# drop it. This removes the cross-run fixture pollution that made results
# non-reproducible when a shared database was reused.
#
# Generic by design: point COWD_PG_BASE_URL at any maintenance database (any
# host/port/user); the wrapper creates a uniquely named database, runs the
# standard scenario harness against it, and always drops it afterwards.
#
# Usage:
#   COWD_PG_BASE_URL='postgres://user:pass@host:port/postgres' \
#   COWD_EVAL_LIVE_SCENARIOS=live_tool_evidence \
#   (optional) COWD_EVAL_SCENARIO_TIMEOUT_SECS=900 \
#   bash scripts/scenarios/run-live-isolated.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

: "${COWD_PG_BASE_URL:=postgres://postgres:cowd-owned-local-fixture@127.0.0.1:32772/postgres}"
: "${COWD_EVAL_LIVE_SCENARIOS:?set COWD_EVAL_LIVE_SCENARIOS to exactly one scenario id}"

command -v psql >/dev/null || { echo 'psql is required to provision the fresh eval database' >&2; exit 2; }

DB_NAME="cowd_eval_$(date +%s)_$RANDOM"
DB_URL="${COWD_PG_BASE_URL%/*}/$DB_NAME"

cleanup() {
  local status=$?
  trap - EXIT
  psql "$COWD_PG_BASE_URL" -v ON_ERROR_STOP=0 \
    -c "DROP DATABASE IF EXISTS \"$DB_NAME\" WITH (FORCE)" >/dev/null 2>&1 || true
  echo "dropped fresh eval database: $DB_NAME" >&2
  exit "$status"
}
trap cleanup EXIT

psql "$COWD_PG_BASE_URL" -v ON_ERROR_STOP=1 \
  -c "CREATE DATABASE \"$DB_NAME\"" >/dev/null
echo "created fresh eval database: $DB_NAME" >&2

export COWD_TEST_POSTGRES_URL="$DB_URL"
bash scripts/scenarios/harness-eval-real-qwen.sh
