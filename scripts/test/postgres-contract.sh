#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

: "${COWD_TEST_POSTGRES_URL:?set COWD_TEST_POSTGRES_URL to an isolated disposable database}"

logs="$(mktemp -d)"
trap 'rm -rf "$logs"' EXIT

run_lib_test() {
  local package="$1"
  local test_name="$2"
  local log="$logs/${package}-${test_name}.log"
  echo "[postgres-contract] ${package} :: ${test_name}"
  cargo test -p "$package" --lib "$test_name" \
    -- --ignored --nocapture --test-threads=1 2>&1 | tee "$log"
  if ! rg -q 'test result: ok\. 1 passed; 0 failed;' "$log"; then
    echo "PostgreSQL contract did not execute exactly one passing test: ${package} :: ${test_name}" >&2
    exit 1
  fi
}

run_integration_test() {
  local package="$1"
  local target="$2"
  local test_name="$3"
  local log="$logs/${package}-${target}-${test_name}.log"
  echo "[postgres-contract] ${package} :: ${target} :: ${test_name}"
  cargo test -p "$package" --test "$target" "$test_name" \
    -- --ignored --nocapture --test-threads=1 2>&1 | tee "$log"
  if ! rg -q 'test result: ok\. 1 passed; 0 failed;' "$log"; then
    echo "PostgreSQL contract did not execute exactly one passing test: ${package} :: ${target} :: ${test_name}" >&2
    exit 1
  fi
}

run_lib_test fact-postgres real_postgres_reopens_and_serializes_competing_fact_upserts
run_lib_test fact-postgres real_sqlite_to_postgres_copy_is_digest_exact_and_reopens
run_lib_test surface-postgres real_postgres_preserves_contract_and_serializes_competing_delivery_claims
run_lib_test gateway sqlite_snapshot_copies_to_postgres_with_exact_digest
run_lib_test memory-postgres real_postgres_memory_roundtrip
run_lib_test matrix-repository real_postgres_adapter_preserves_matrix_snapshot
run_lib_test cowd-product-apps real_postgres_provision
run_lib_test runtime-postgres postgres_runtime_event_store_preserves_fences_outbox_restart_and_runtime_composition
run_lib_test runtime-postgres postgres_task_store_preserves_migration_restart_and_per_task_concurrency
run_lib_test runtime-postgres postgres_artifact_repository_matches_sqlite_selector_and_scope_contract
run_lib_test approval real_postgres_copy_reopens_with_matching_digest
run_lib_test connector-postgres postgres_resource_directory_migrates_restarts_and_copies_real_database

run_lib_test session-postgres existing_postgres_outbox_schema_migrates_claim_fence_epoch_in_place
run_lib_test session-postgres postgres_activation_index_and_manifest_repair_match_sqlite_semantics
run_lib_test session-postgres postgres_adapter_real_copy_fences_and_injected_facade
run_lib_test session-postgres postgres_fenced_terminal_commit_matches_sqlite_atomic_identity_contract
run_lib_test session-postgres postgres_terminal_commit_and_generation_advance_share_one_lock_order
run_lib_test session-postgres postgres_branch_command_commits_every_artifact_or_nothing
run_lib_test session-postgres postgres_lifecycle_intent_recovers_each_phase_and_commits_one_tombstone
run_lib_test session-postgres postgres_delete_lifecycle_recovers_stable_phases_and_commits_one_tombstone
run_lib_test session-postgres postgres_durable_input_contract_is_fenced_ordered_and_auditable
run_lib_test session-postgres postgres_runtime_failure_retry_and_terminal_statuses_are_real
run_lib_test session-postgres postgres_v8_migrates_legacy_runtime_rows_in_place
run_lib_test session-postgres postgres_concurrent_store_startup_serializes_preflight_and_migrations

run_integration_test session-postgres shared_backend_contract_test postgres_input_generation_and_claim_fence_contract
run_integration_test session-postgres shared_backend_contract_test postgres_lifecycle_contract
run_integration_test session-postgres shared_backend_contract_test postgres_branch_contract
run_integration_test session-postgres shared_backend_contract_test postgres_domain_event_idempotency_and_kind_query_contract
run_integration_test session-postgres shared_backend_contract_test postgres_application_execution_32_way_semantic_idempotency_contract

if [[ -n "${COWD_TEST_POSTGRES_TARGET_URL:-}" ]]; then
  run_lib_test surface-postgres real_postgres_to_postgres_quiesced_copy_is_digest_exact_and_target_only
else
  echo "[postgres-contract] target URL absent; cross-database copy not requested"
fi
