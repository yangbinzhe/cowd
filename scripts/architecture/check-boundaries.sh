#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

fail=0

check_empty() {
  local name="$1"
  shift
  local output
  set +e
  output="$("$@" 2>&1)"
  local status=$?
  set -e
  if [[ "$status" -eq 0 && -n "$output" ]]; then
    echo "FAIL $name"
    echo "$output"
    fail=1
  elif [[ "$status" -gt 1 ]]; then
    echo "ERROR $name"
    echo "$output"
    fail=1
  else
    echo "PASS $name"
  fi
}

if [[ "${1:-}" == "--v566-sqlite" ]]; then
  # This focused gate is intentionally independent from older whole-repository
  # architecture checks. It verifies the V566 ownership boundary without
  # claiming unrelated historical checks have passed.
  check_empty "V566 durable owners must use StorageRuntime executors" \
    bash -c 'for file in \
      crates/connector/src/lib.rs \
      crates/matrix/repository/src/sqlite_repository.rs \
      crates/runtime/src/recovery/runtime_event_store.rs \
      crates/runtime/src/mission/task.rs \
      crates/runtime/src/context/reality_recall_port.rs \
      crates/gateway/src/surface_host/message_store.rs \
      crates/gateway/src/services/growth_service.rs \
      crates/gateway/src/infrastructure/gateway_health.rs; do
      awk "/#\[cfg\(test\)\]/{exit} {print FILENAME \":\" FNR \":\" \$0}" "$file"
    done | rg -n "Connection::open\\(|SqliteConnectionManager::file|Mutex<Connection>|SqliteConnectionFactory" || true'
  exit "$fail"
fi

if [[ "${1:-}" == "--v567-postgres" ]]; then
  # V567 validates the first complete PostgreSQL domain without pretending
  # that the global deployment switch is ready. Connector keeps a backend-free
  # port; its PostgreSQL driver lives in the dedicated adapter crate.
  check_empty "V567 Gateway must not name the concrete SQLite directory" \
    rg -n '\bSqliteResourceDirectory\b|resource_directory_path' \
      crates/gateway/src/services crates/gateway/src/api_routes --glob '*.rs'
  check_empty "V567 global PostgreSQL switch must remain unavailable" \
    rg -n 'StorageBackendKind::Postgres' crates/gateway crates/runtime crates/cli --glob '*.rs'
  check_empty "V567 connector manifest must not depend on PostgreSQL" \
    rg -n '^postgres\s*=|^tokio-postgres\s*=|^r2d2_postgres\s*=' crates/connector/Cargo.toml
  check_empty "V567 PostgreSQL adapter crate must exist" \
    bash -c 'if [[ ! -f crates/connector-postgres/Cargo.toml || ! -f crates/connector-postgres/src/lib.rs ]]; then echo missing; fi'
  exit "$fail"
fi

if [[ "${1:-}" == "--v568-runtime-port" ]]; then
  # V568 moves Runtime callers behind the durable event-store façade. The
  # SQLite adapter may remain in its owner module until V569 adds PostgreSQL;
  # no other Runtime production module may learn its concrete type or SQL API.
  check_empty "V568 runtime callers must not name SQLite event adapter" \
    bash -c 'rg -n "SqliteRuntimeEventStore|RuntimeEventStoreBackend" crates/runtime/src --glob "*.rs" | rg -v "^crates/runtime/src/recovery/runtime_event_store.rs:" | rg -v "/tests?/" | rg -v "/tests\\.rs:" || true'
  check_empty "V568 runtime callers must not use event-store file paths" \
    rg -n 'RuntimeEventStore.*\.path\(|event_store\.path\(' crates/runtime/src --glob '*.rs' --glob '!**/tests/**' --glob '!**/tests.rs'
  check_empty "V568 public event adapter must not leak outside owner module" \
    bash -c 'rg -n "pub struct SqliteRuntimeEventStore|pub enum RuntimeEventStoreBackend" crates/runtime/src/recovery/runtime_event_store.rs || true'
  exit "$fail"
fi

if [[ "${1:-}" == "--v569-runtime-postgres" ]]; then
  # V569 keeps PostgreSQL infrastructure outside Runtime while proving a full
  # adapter can be composed explicitly. A deployment-wide backend toggle is
  # still forbidden until every required domain has migrated.
  check_empty "V569 runtime normal dependency tree must not include PostgreSQL driver" \
    bash -c 'cargo tree -p runtime --edges normal 2>/dev/null | rg "(postgres|r2d2_postgres)" || true'
  check_empty "V569 runtime manifest must not name PostgreSQL driver" \
    rg -n '^postgres\s*=|^tokio-postgres\s*=|^r2d2_postgres\s*=' crates/runtime/Cargo.toml
  check_empty "V569 global PostgreSQL switch must remain unavailable" \
    rg -n 'StorageBackendKind::Postgres|control_plane_backend.*postgres' crates/gateway crates/runtime crates/cli --glob '*.rs'
  check_empty "V569 dedicated runtime PostgreSQL adapter crate must exist" \
    bash -c 'if [[ ! -f crates/runtime-postgres/Cargo.toml || ! -f crates/runtime-postgres/src/lib.rs ]]; then echo missing; fi'
  check_empty "V569 RuntimeServices must retain explicit event backend injection" \
    bash -c 'if ! rg -q "pub fn runtime_event_store\(" crates/runtime/src/execution_core/services.rs; then echo missing; fi'
  exit "$fail"
fi

if [[ "${1:-}" == "--v570-task-postgres" ]]; then
  # V570 moves Task lifecycle authority out of the process-local Vec/Mutex and
  # behind the Runtime task backend port. PostgreSQL remains an explicitly
  # composed adapter, never a deployment-wide backend toggle.
  check_empty "V570 runtime task must not retain in-memory authority or full-table rewrite" \
    bash -c 'awk "/#\[cfg\(test\)\]/{exit} {print}" crates/runtime/src/mission/task.rs | rg -n "Mutex<TaskStore>|struct TaskStore[[:space:]\{]|fn persist\(|DELETE FROM tasks" || true'
  check_empty "V570 runtime normal dependency tree must not include PostgreSQL driver" \
    bash -c 'cargo tree -p runtime --edges normal 2>/dev/null | rg "(postgres|r2d2_postgres)" || true'
  check_empty "V570 Gateway normal dependency tree must not include PostgreSQL driver" \
    bash -c 'cargo tree -p gateway --edges normal 2>/dev/null | rg "(postgres|r2d2_postgres)" || true'
  check_empty "V570 task adapter and explicit Gateway composition must exist" \
    bash -c 'missing=""; rg -q "pub struct PostgresTaskStore" crates/runtime-postgres/src/lib.rs || missing="$missing postgres-task"; rg -q "pub trait TaskStoreBackend" crates/runtime/src/mission/task.rs || missing="$missing task-port"; rg -q "from_runtime_kernel" crates/gateway/src/kernel/task_kernel.rs || missing="$missing gateway-injection"; if [[ -n "$missing" ]]; then echo "$missing"; fi'
  check_empty "V570 global PostgreSQL switch must remain unavailable" \
    rg -n 'StorageBackendKind::Postgres|control_plane_backend.*postgres' crates/gateway crates/runtime crates/cli --glob '*.rs'
  exit "$fail"
fi

if [[ "${1:-}" == "--v571-surface-ledger-port" ]]; then
  # V571 moves Surface durable facts and operations behind a storage-neutral,
  # object-safe contract. SQLite remains a private Gateway adapter; PostgreSQL
  # composition and any deployment switch are intentionally deferred.
  check_empty "V571 surface contract must own the durable ledger port" \
    bash -c 'if ! rg -q "pub trait SurfaceMessageLedger" crates/surface/src/message_ledger.rs; then echo missing; fi'
  check_empty "V571 host and managed transports must not retain concrete SQLite ledger types" \
    bash -c 'rg -n "Arc<SqliteSurfaceMessageStore>" crates/gateway/src/surface_host --glob "*.rs" | rg -v "#\[cfg\(test\)\]" || true'
  check_empty "V571 Gateway must not redefine surface durable DTOs" \
    rg -n 'pub\(crate\) struct Surface(TurnCorrelation|InboxRecord|OutboxRecord|TriggerEventRecord|DeliveryEvent|MessageSnapshot)' crates/gateway/src/surface_host/message_store.rs
  check_empty "V571 nonfallible ledger projections must remain test-only" \
    bash -c 'awk "/^\/\/\/ Gateway.s current SQLite implementation/{exit} {print}" crates/gateway/src/surface_host/message_store.rs | rg -n "fn (get_outbox_by_delivery|due_retry_deliveries|due_trigger_event_retries|get_inbox_message|list_inbox|list_outbox|list_all_inbox|list_all_outbox|list_trigger_events|list_delivery_events|snapshot)\(.*-> (Option<|Vec<|SurfaceMessageSnapshot)" || true'
  check_empty "V571 Surface and Gateway normal dependency trees must stay PostgreSQL-free" \
    bash -c '(cargo tree -p surface --edges normal; cargo tree -p gateway --edges normal) 2>/dev/null | rg "(postgres|r2d2_postgres)" || true'
  check_empty "V571 global PostgreSQL switch must remain unavailable" \
    rg -n 'StorageBackendKind::Postgres|control_plane_backend.*postgres' crates/gateway crates/runtime crates/cli --glob '*.rs'
  exit "$fail"
fi

if [[ "${1:-}" == "--v572-surface-ledger-postgres" ]]; then
  # V572 introduces a complete PostgreSQL implementation of the V571 port.
  # The adapter may depend on the driver, while the contract and Gateway
  # production graph must stay database-driver-free until a separately
  # approved deployment-wide cutover.
  check_empty "V572 dedicated Surface PostgreSQL adapter must exist" \
    bash -c 'if [[ ! -f crates/surface-postgres/Cargo.toml || ! -f crates/surface-postgres/src/lib.rs ]]; then echo missing; fi'
  check_empty "V572 Surface and Gateway normal dependency trees must stay PostgreSQL-free" \
    bash -c '(cargo tree -p surface --edges normal; cargo tree -p gateway --edges normal) 2>/dev/null | rg "(postgres|r2d2_postgres)" || true'
  check_empty "V572 Surface contract must not name a PostgreSQL driver" \
    rg -n '^postgres\s*=|^tokio-postgres\s*=|^r2d2_postgres\s*=' crates/surface/Cargo.toml
  check_empty "V572 Gateway production source must not compose the PostgreSQL adapter" \
    bash -c 'awk "/#\[cfg\(test\)\]/{exit} {print FILENAME \":\" FNR \":\" \$0}" crates/gateway/src/surface_host/message_store.rs | rg -n "surface_postgres|PostgresSurfaceMessageLedger" || true'
  check_empty "V572 migration carrier must be owned by the Surface contract" \
    bash -c 'if ! rg -q "SurfaceMessageLedgerMigrationSnapshot" crates/surface/src/message_ledger.rs; then echo missing; fi'
  check_empty "V572 global PostgreSQL switch must remain unavailable" \
    rg -n 'StorageBackendKind::Postgres|control_plane_backend.*postgres' crates/gateway crates/runtime crates/cli --glob '*.rs'
  exit "$fail"
fi

check_empty "cli business command names" \
  rg -n "\\b(run|chat|prompt|mcp serve)\\b" crates/cli/src/main.rs --glob '*.rs'

check_empty "cli business modules" \
  rg -n "use .*\\b(auth|session|memory|matrix|mfg|agent|daemon)\\b|\\b(auth|session|memory|matrix|mfg|agent|daemon)::|mod (auth|session|memory|matrix|mfg|agent|daemon)" crates/cli/src/main.rs --glob '*.rs'

check_empty "daemon business management" \
  rg -n "daemon status|daemon start$|daemon stop|daemon restart|daemon_client|socket business" crates --glob '*.rs' --glob '!**/tests/**' --glob '!crates/auth-broker/**' --glob '!crates/sandbox-launcher/**'

check_empty "tui direct business dependencies" \
  rg -n "(^|[^[:alnum:]_:])runtime::|use runtime::|use app_mfg::|app_mfg::|use matrix_core::|matrix_core::|use matrix_repository::|matrix_repository::|use storage::|storage::|rusqlite|use tools::|tools::|use memory::|memory::|use command_contract::|command_contract::|use command_service::|command_service::" crates/tui/src --glob '*.rs' --glob '!lib.rs' --glob '!boundary_policy.rs'

check_empty "runtime entrypoint reverse dependencies" \
  bash -c 'rg -n "gateway::|tui::|cli::|app_mfg::" crates/runtime/src --glob "*.rs" | rg -v "\"(gateway|tui|cli|app_mfg)::" || true'

check_empty "compat harness workspace residue" \
  rg -n "compat-harness|compat_harness" Cargo.toml crates/*/Cargo.toml crates --glob '*.rs' --glob '!**/tests/**'

check_empty "api crate naming residue" \
  rg -n "crates/api|api::" crates --glob '*.rs' --glob 'Cargo.toml'

check_empty "obsolete internal cowd package alias residue" \
  bash -c 'rg -n "cowd_memory|cowd_storage|mod cowd_|pub mod cowd_" crates --glob "*.rs" | rg -v "^crates/runtime/src/lib.rs:[0-9]+:(pub mod cowd_(dirs|event);|pub use cowd_(dirs|event)(::|;|\\{))" || true'

check_empty "mixed matrix mfg route module residue" \
  bash -c 'mixed="matrix_""mfg"; file="crates/gateway/src/api_routes/${mixed}_routes.rs"; if test -e "$file"; then echo "$file"; fi; rg -n "${mixed}_routes|cowd_${mixed}|${mixed}" crates/gateway/src crates/gateway/tests --glob "*.rs" || true'

check_empty "matrix routes must stay free of mfg application semantics" \
  rg -n "Mfg|mfg|manufacturing|cockpit|incident|playbook|skill" \
    crates/gateway/src/api_routes/matrix_routes.rs

check_empty "mfg routes must not keep legacy matrix handler names" \
  bash -c 'file="crates/gateway/src/api_routes/mfg_routes.rs"; if [[ -f "$file" ]]; then rg -n "matrix_(production_governance|command_center|decision_trace|server_manufacturing)|matrix_health_capabilities" "$file"; fi'

check_empty "AI bin install residue" \
  rg -n "~/AI/bin|AI/bin" README.md docs scripts crates --glob '*.rs' --glob '*.md' --glob '*.sh' --glob 'Cargo.toml' --glob '!scripts/architecture/check-boundaries.sh'

check_empty "gateway routes must not own business/storage/runtime internals" \
  bash -c 'while IFS= read -r file; do awk "/#\\[cfg\\(test\\)\\]/{exit} {print FILENAME \":\" FNR \":\" \$0}" "$file"; done < <(rg --files crates/gateway/src/api_routes -g "*.rs" -g "!cross_plane_routes.rs") | rg -n "MatrixSqliteRepository|MfgStore::open\\(|Store::open\\(|rusqlite|Connection::open|SqliteConnectionManager::file|ConfigLoader::default_for|CrossPlaneAction|CrossPlaneAuditRecord|CrossPlaneExecutionReceipt" || true'

check_empty "non-cross-plane routes must not call cross-plane route control helpers" \
  rg -n "cross_plane_routes::(cross_plane_control|save_cross_plane_state|ensure_cross_plane_loaded|decide_connector_action)" \
    crates/gateway/src/api_routes --glob '*.rs' --glob '!cross_plane_routes.rs'

check_empty "cross-plane audit and receipt construction owner" \
  rg -n "CrossPlaneAuditRecord::new|CrossPlaneExecutionReceipt::new" \
    crates/gateway/src --glob '*.rs' --glob '!**/services/cross_plane_service.rs'

check_empty "agent service must not route through slash handlers" \
  rg -n "handle_agents_slash_command" crates/gateway/src/services/agent_service.rs

check_empty "gateway main must use agent service owner" \
  rg -n "handle_agents_slash_command" crates/gateway/src/main.rs

check_empty "gateway main must use skill service owner" \
  rg -n "handle_skills_slash_command" crates/gateway/src/main.rs

check_empty "gateway main must not own static config or tool entry handlers" \
  rg -n "fn print_static_config_command|fn print_static_tool_command|tools::mvp_tool_specs" crates/gateway/src/main.rs

check_empty "gateway main must not own static bootstrap or version projections" \
  rg -n "fn print_bootstrap_plan|fn render_version_report|fn version_json_value|BootstrapPlan::claude_code_default" crates/gateway/src/main.rs

check_empty "gateway main must not own system prompt projection" \
  rg -n "fn print_system_prompt|kind\": \"system-prompt\"" crates/gateway/src/main.rs

check_empty "gateway main must not own install entry implementation" \
  rg -n "fn run_install|cowd-gateway\\.service|Permissions::from_mode" crates/gateway/src/main.rs

check_empty "gateway main must not own init entry implementation" \
  rg -n "fn run_init|fn init_claude_md|fn init_json_value|initialize_repo" crates/gateway/src/main.rs

check_empty "gateway main must not own plugin entry implementation" \
  bash -c 'awk "/#\\[cfg\\(test\\)\\]/{exit} {print}" crates/gateway/src/main.rs | rg -n "handle_plugins_slash_command|build_plugin_manager|PluginManagerConfig" || true'

check_empty "gateway main must not own env entry implementation" \
  rg -n "fn resolve_model_alias_with_config\\(|fn config_default_model_alias\\(|fn config_model_alias\\(|fn parse_permission_mode_arg\\(|fn default_permission_mode\\(|fn resolve_repl_model\\(" crates/gateway/src/main.rs

check_empty "gateway main must not own plugin command entry projection" \
  rg -n "fn execute_plugin_command\\(|fn print_plugin_command\\(" crates/gateway/src/main.rs

check_empty "gateway main must not own mcp command projection" \
  rg -n "fn handle_mcp_slash_command|fn render_mcp_|McpOAuthConfig|ScopedMcpServerConfig|McpServerConfig" crates/gateway/src/main.rs

check_empty "gateway main must not directly resolve skill invocation" \
  rg -n "resolve_skill_invocation" crates/gateway/src/main.rs

check_empty "gateway main must not own setup workspace entry projection" \
  rg -n "fn render_setup_report|fn render_setup_json|fn setup_snapshot|struct SetupItem|struct SetupSnapshot" crates/gateway/src/main.rs

check_empty "gateway main must not own config or memory entry projections" \
  rg -n "fn render_config_report|fn render_config_json|fn render_memory_report|fn render_memory_json" crates/gateway/src/main.rs

check_empty "gateway main must not own diff entry projection" \
  rg -n "fn render_diff_report\\(|fn render_diff_report_for\\(|fn render_diff_json_for\\(|fn run_git_diff_command_in\\(" crates/gateway/src/main.rs

check_empty "gateway main must not own status or sandbox entry projections" \
  rg -n "fn print_status_snapshot\\(|fn status_json_value\\(|fn status_context\\(|fn status_context_for_session\\(|fn format_status_report\\(|fn format_sandbox_report\\(|fn print_sandbox_status_snapshot\\(|fn sandbox_json_value\\(|struct StatusContext|struct StatusUsage|struct GitWorkspaceSummary|fn parse_git_status_metadata\\(|fn parse_git_status_branch\\(|fn parse_git_workspace_summary\\(|fn resolve_git_branch_for\\(|fn run_git_capture_in\\(|fn find_git_root_in\\(|fn parse_git_status_metadata_for\\(" crates/gateway/src/main.rs

check_empty "gateway main must not own local command entry projections" \
  rg -n "fn render_help_topic\\(|fn print_help_topic\\(|fn print_help_to\\(|fn print_help\\(|fn render_teleport_report\\(|fn render_last_tool_debug_report\\(|fn indent_block\\(|fn format_bughunter_report\\(|fn format_ultraplan_report\\(|fn format_pr_report\\(|fn format_issue_report\\(" crates/gateway/src/main.rs

check_empty "gateway main must not own session store entry implementation" \
  rg -n "static UNIFIED_STORE|fn get_unified_store\\(|fn jsonl_sessions_dir\\(|fn session_db_path\\(|fn discover_local_session_import_candidates\\(|fn migrate_session_messages\\(|fn import_local_session_file\\(|fn run_import_session\\(|fn session_to_record\\(|fn sync_cli_session_to_unified_store\\(|fn hydrate_session_from_unified_store\\(|fn load_or_create_live_session\\(|fn create_managed_session_handle\\(|fn resolve_session_reference\\(|fn resolve_managed_session_path\\(|fn list_managed_sessions\\(|fn list_workspace_session_records\\(|fn record_to_summary\\(|fn latest_managed_session\\(|fn load_session_reference\\(|fn delete_managed_session\\(|fn confirm_session_deletion\\(|fn render_session_list\\(|fn format_session_modified_age\\(|fn write_session_clear_backup\\(|fn session_clear_backup_path\\(|struct SessionHandle|struct ManagedSessionSummary|struct LocalSessionImportCandidate" crates/gateway/src/main.rs

check_empty "gateway main must not own session archive entry implementation" \
  rg -n "fn render_export_text\\(|fn resolve_export_path\\(|fn default_export_filename\\(|fn run_export\\(|fn render_session_markdown\\(|fn summarize_tool_payload_for_markdown\\(|fn short_tool_id\\(" crates/gateway/src/main.rs

check_empty "gateway main must not own gateway projection entry implementation" \
  rg -n "enum GatewayTaskSlashCommand|enum GatewayApprovalSlashCommand|enum GatewayContextSlashCommand|enum GatewayCrossPlaneSlashCommand|fn parse_gateway_task_slash_command\\(|fn parse_gateway_approval_slash_command\\(|fn parse_gateway_context_slash_command\\(|fn parse_gateway_cross_plane_slash_command\\(|fn gateway_projection_auth_token\\(|fn running_gateway_client\\(|fn print_gateway_task_status\\(|fn print_gateway_approval_status\\(|fn print_gateway_projection_response\\(" crates/gateway/src/main.rs

check_empty "interactive tests must not pre-kill named tmux sessions" \
  rg -n 'kill-session.*("-t", name|-t[[:space:]]+\$?name)' tests/interactive/src --glob '*.rs'

if [[ -e crates/command/service/src || -e crates/command/service/Cargo.toml ]]; then
  check_empty "command-service must remain declarative" \
    rg -n "runtime::|memory::|matrix::|app_mfg::|plugins::|ConfigLoader|GlobalToolRegistry|Connection::open|SqliteConnectionManager::file|std::fs|fs::|std::env|env::|handle_agents_slash_command|handle_skills_slash_command|resolve_skill_path|resolve_skill_invocation" \
      crates/command/service/src crates/command/service/Cargo.toml --glob '*.rs' --glob 'Cargo.toml'
else
  echo "PASS command-service retired from workspace"
fi

check_empty "gateway services must not become protocol or storage adapters" \
  bash -c 'while IFS= read -r file; do awk "/#\\[cfg\\(test\\)\\]/{exit} {print FILENAME \":\" FNR \":\" \$0}" "$file"; done < <(rg --files crates/gateway/src/services -g "*.rs" -g "!app_host_ports.rs") | rg -n "crate::api_routes::AppState|AppState|axum::|StatusCode|IntoResponse|Json<|Connection::open|SqliteConnectionManager::file|UnifiedSessionStore::open|ConfigLoader::default_for" | rg -v "^crates/gateway/src/services/mfg_service.rs:[0-9]+:.*MfgStore::open_storage_handle" || true'

check_empty "gateway services must not route through slash command execution" \
  rg -n "slash_catalog|handle_.*slash_command|resolve_skill_invocation|resolve_skill_path" crates/gateway/src/services --glob "*.rs"

check_empty "gateway service registry must not own context use cases" \
  rg -n "impl GatewayServices" crates/gateway/src/services/context_service.rs

check_empty "gateway entry must not route skill invocation through slash catalog" \
  rg -n "slash_catalog::resolve_skill_invocation|slash_catalog::resolve_skill_path" crates/gateway/src/entry --glob "*.rs"

check_empty "gateway main must not own business registries" \
  bash -c 'awk "/#\\[cfg\\(test\\)\\]/{exit} {print}" crates/gateway/src/main.rs | rg -n "GlobalToolRegistry|SkillRegistry|PluginManager|CrossPlaneAction|CrossPlaneAuditRecord|CrossPlaneExecutionReceipt|MfgStore::open|MatrixSqliteRepository|UnifiedSessionStore::open|TaskKernel::open|current_tool_registry|build_runtime_plugin_state|RuntimePluginState" || true'

check_empty "matrix core must not contain mfg application semantics" \
  bash -c 'mixed="matrix_""mfg"; reverse="mfg_""matrix"; adapter="Mfg""Matrix""Adapter"; while IFS= read -r file; do awk "/#\\[cfg\\(test\\)\\]/{exit} {print FILENAME \":\" FNR \":\" \$0}" "$file"; done < <(rg --files crates/matrix/core crates/matrix/repository -g "*.rs" -g "Cargo.toml") | rg -n "Mfg|mfg|manufacturing|server_manufacturing|${mixed}|${reverse}|${adapter}" || true'

check_empty "app-mfg must not depend on gateway or runtime internals" \
  bash -c 'root="../cowd-app-mfg"; if [[ ! -d "$root" ]]; then echo "missing sibling cowd-app-mfg"; exit 0; fi; rg -n "gateway::|runtime::|use gateway|use runtime|crate::runtime" "$root/crates" "$root/Cargo.toml" --glob "*.rs" --glob "Cargo.toml" || true'

check_empty "production direct sqlite opens must stay in storage/repository adapters" \
  bash -c 'while IFS= read -r file; do awk "/#\\[cfg\\(test\\)\\]/{exit} !/^[[:space:]]*\\/\\// {print FILENAME \":\" FNR \":\" \$0}" "$file"; done < <(rg --files crates -g "*.rs" -g "!**/tests/**" -g "!crates/storage/**" -g "!crates/connector/src/source.rs" -g "!crates/connector/src/lib.rs" -g "!crates/memory/src/store/**" -g "!crates/memory/src/session/session_store.rs" -g "!crates/memory/src/session/state_rebuilder.rs" -g "!crates/memory/src/knowledge/mod.rs" -g "!crates/memory/src/kernel/cognitive.rs" -g "!crates/memory/src/lifecycle/maintenance.rs" -g "!crates/memory/src/ops/sqlite_persistence.rs" -g "!crates/matrix/repository/**" -g "!crates/runtime/src/recovery/runtime_event_store.rs" -g "!crates/runtime/src/mission/task.rs" -g "!crates/runtime/src/team/team_discovery.rs" -g "!crates/runtime/src/context/artifact.rs" -g "!crates/gateway/src/infrastructure/selected_storage.rs") | rg -n "Connection::open\\(|SqliteConnectionManager::file|TaskKernel::open\\(|UnifiedSessionStore::open\\(|MfgStore::open\\(|SqliteStore::open\\(|MatrixSqliteRepository::open\\(|Store::open\\(" | rg -v "open_in_memory|/tests/|^crates/gateway/src/api_routes/mod.rs:|^crates/gateway/src/kernel/task_kernel.rs:|^crates/gateway/src/kernel/session_kernel.rs:|^crates/gateway/src/main.rs:1[0-9][0-9][0-9][0-9]:|^crates/memory/src/.*:[0-9]+:.*(tmp|test|example)" || true'

check_empty "storage direct-open allowlist must stay empty" \
  bash -c 'if [[ "$(tr -d "[:space:]" < crates/storage/direct-open-allowlist.json)" != "[]" ]]; then cat crates/storage/direct-open-allowlist.json; fi'

echo "Checking cargo entrypoint dependency summaries"
cargo tree -p cli --depth 1 --no-default-features

if cargo tree -p tui --depth 1 --no-default-features | rg "(^|[ ├└│─])((runtime|matrix-core|matrix-repository|storage|tools|command-contract|command-service) v|memory|app-mfg|app_mfg|rusqlite)"; then
  echo "FAIL tui forbidden direct dependency tree"
  fail=1
else
  echo "PASS tui forbidden direct dependency tree"
fi

if [[ -f ../cowd-app-mfg/Cargo.toml ]]; then
  if cargo tree --manifest-path ../cowd-app-mfg/Cargo.toml --workspace --edges normal | rg "(^|[ ├└│─])(runtime v|gateway v|gateway =)"; then
    echo "FAIL app-mfg forbidden dependency tree"
    fail=1
  else
    echo "PASS app-mfg forbidden dependency tree"
  fi
else
  echo "FAIL app-mfg forbidden dependency tree"
  echo "missing sibling cowd-app-mfg workspace"
  fail=1
fi

if cargo tree -p matrix-core --no-default-features | rg "(^|[ ├└│─])((runtime|gateway|app-mfg) v|app_mfg|mfg)"; then
  echo "FAIL matrix forbidden dependency tree"
  fail=1
else
  echo "PASS matrix forbidden dependency tree"
fi

exit "$fail"
