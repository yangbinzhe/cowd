#!/usr/bin/env bash
set -euo pipefail

# G68: pg_fixture_missing_database_fails
# Run one PostgreSQL integration group in a fixture-owned schema. The caller
# supplies a disposable database URL; this script never prints it and never
# mutates public or any schema it did not create itself.

: "${COWD_TEST_POSTGRES_URL:?set COWD_TEST_POSTGRES_URL to an isolated disposable PostgreSQL database}"
if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi
command -v psql >/dev/null 2>&1 || {
  echo "psql is required for PostgreSQL contract tests" >&2
  exit 2
}

BASE_POSTGRES_URL="$COWD_TEST_POSTGRES_URL"
TEST_SCHEMA="cowdtest_${PPID}_$$_${RANDOM}"
if [[ ! "$TEST_SCHEMA" =~ ^[a-z][a-z0-9_]{0,62}$ ]]; then
  echo "generated PostgreSQL test schema is invalid" >&2
  exit 2
fi

psql "$BASE_POSTGRES_URL" -v ON_ERROR_STOP=1 \
  -c "CREATE SCHEMA \"$TEST_SCHEMA\"" >/dev/null
cleanup() {
  psql "$BASE_POSTGRES_URL" -v ON_ERROR_STOP=1 \
    -c "DROP SCHEMA IF EXISTS \"$TEST_SCHEMA\" CASCADE" >/dev/null || true
}
trap cleanup EXIT INT TERM

case "$BASE_POSTGRES_URL" in
  *\?*) URL_SEPARATOR='&' ;;
  *) URL_SEPARATOR='?' ;;
esac
export COWD_TEST_POSTGRES_URL="${BASE_POSTGRES_URL}${URL_SEPARATOR}options=-csearch_path%3D${TEST_SCHEMA}%2Cpublic"
"$@"
