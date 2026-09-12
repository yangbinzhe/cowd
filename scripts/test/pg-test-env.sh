#!/usr/bin/env bash
set -euo pipefail

# G68: pg_fixture_missing_database_fails
# Validate the test environment. Isolation belongs to each Rust fixture's
# explicit scoped_namespace and registered cleanup; URL search_path options
# are overridden by PostgresExecutor and cannot supply that isolation.
: "${COWD_TEST_POSTGRES_URL:?set COWD_TEST_POSTGRES_URL to an isolated disposable PostgreSQL database}"
if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi
command -v psql >/dev/null 2>&1 || {
  echo "psql is required for PostgreSQL contract tests" >&2
  exit 2
}
psql "$COWD_TEST_POSTGRES_URL" -v ON_ERROR_STOP=1 -c 'SELECT 1' >/dev/null
exec "$@"
