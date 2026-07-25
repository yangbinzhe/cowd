#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

: "${COWD_TEST_POSTGRES_URL:?set COWD_TEST_POSTGRES_URL to an isolated disposable database}"

logs="$(mktemp -d)"
trap 'rm -rf "$logs"' EXIT

run_test() {
  local package="$1"
  local test_name="$2"
  shift 2
  local log="$logs/${package}-${test_name}.log"
  echo "[postgres-contract] ${package} :: ${test_name}"
  cargo test -p "$package" "$@" --lib "$test_name" \
    -- --ignored --nocapture --test-threads=1 2>&1 | tee "$log"
  if ! rg -q 'test result: ok\. 1 passed; 0 failed;' "$log"; then
    echo "PostgreSQL contract did not execute exactly one passing test: ${package} :: ${test_name}" >&2
    exit 1
  fi
}

run_test fact-postgres real_postgres_reopens_and_serializes_competing_fact_upserts
run_test fact-postgres real_sqlite_to_postgres_copy_is_digest_exact_and_reopens
run_test surface-postgres real_postgres_preserves_contract_and_serializes_competing_delivery_claims
run_test gateway sqlite_snapshot_copies_to_postgres_with_exact_digest
run_test memory-postgres real_postgres_memory_roundtrip
run_test matrix-repository real_postgres_adapter_preserves_matrix_snapshot
run_test cowd-product-apps real_postgres_provision
run_test runtime-postgres postgres_runtime_event_store_preserves_fences_outbox_restart_and_runtime_composition
run_test runtime-postgres postgres_task_store_preserves_migration_restart_and_per_task_concurrency
run_test runtime-postgres postgres_artifact_repository_matches_sqlite_selector_and_scope_contract
run_test approval real_postgres_copy_reopens_with_matching_digest
run_test connector-postgres postgres_resource_directory_migrates_restarts_and_copies_real_database
run_test session-postgres postgres_adapter_real_copy_fences_and_injected_facade
run_test session-postgres postgres_terminal_transcript_preserves_published_cursor_and_is_idempotent
run_test session-postgres postgres_concurrent_store_startup_serializes_preflight_and_migrations

if [[ -n "${COWD_TEST_POSTGRES_TARGET_URL:-}" ]]; then
  run_test surface-postgres real_postgres_to_postgres_quiesced_copy_is_digest_exact_and_target_only
else
  echo "[postgres-contract] target URL absent; cross-database copy not requested"
fi
