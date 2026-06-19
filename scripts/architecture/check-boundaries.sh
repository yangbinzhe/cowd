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

check_empty "cli business command names" \
  rg -n "\\b(run|chat|prompt|mcp serve)\\b" crates/cli/src/main.rs --glob '*.rs'

check_empty "cli business modules" \
  rg -n "auth|session|memory|matrix|mfg|agent|daemon" crates/cli/src/main.rs --glob '*.rs'

check_empty "daemon business management" \
  rg -n "daemon status|daemon start$|daemon stop|daemon restart|daemon_client|UnixStream|socket business" crates --glob '*.rs' --glob '!**/tests/**'

check_empty "tui direct business dependencies" \
  rg -n "(^|[^[:alnum:]_:])runtime::|use runtime::|use app_mfg::|app_mfg::|use matrix_core::|matrix_core::|use matrix_repository::|matrix_repository::|use storage::|storage::|rusqlite|use tools::|tools::|use memory::|memory::|use command_contract::|command_contract::|use command_service::|command_service::" crates/tui/src --glob '*.rs' --glob '!lib.rs' --glob '!boundary_policy.rs'

check_empty "runtime entrypoint reverse dependencies" \
  rg -n "gateway::|tui::|cli::|app_mfg::" crates/runtime/src --glob '*.rs'

check_empty "compat harness workspace residue" \
  rg -n "compat-harness|compat_harness" Cargo.toml crates/*/Cargo.toml crates --glob '*.rs' --glob '!**/tests/**'

check_empty "api crate naming residue" \
  rg -n "crates/api|api::" crates --glob '*.rs' --glob 'Cargo.toml'

check_empty "internal cowd package alias residue" \
  bash -c 'rg -n "cowd_app_mfg|cowd_memory|cowd_storage|mod cowd_|pub mod cowd_|use cowd_" crates --glob "*.rs" | rg -v "^crates/runtime/src/lib.rs:[0-9]+:(pub mod cowd_(dirs|event);|pub use cowd_(dirs|event)(::|;|\\{))" || true'

check_empty "mixed matrix mfg route module residue" \
  bash -c 'mixed="matrix_""mfg"; file="crates/gateway/src/api_routes/${mixed}_routes.rs"; if test -e "$file"; then echo "$file"; fi; rg -n "${mixed}_routes|cowd_${mixed}|${mixed}" crates/gateway/src crates/gateway/tests --glob "*.rs" || true'

check_empty "matrix routes must stay free of mfg application semantics" \
  rg -n "Mfg|mfg|manufacturing|cockpit|incident|playbook|skill" \
    crates/gateway/src/api_routes/matrix_routes.rs

check_empty "mfg routes must not keep legacy matrix handler names" \
  rg -n "matrix_(production_governance|command_center|decision_trace|server_manufacturing|analysis|action|execution)|matrix_health_capabilities" \
    crates/gateway/src/api_routes/mfg_routes.rs

check_empty "AI bin install residue" \
  rg -n "~/AI/bin|AI/bin" README.md docs scripts crates --glob '*.rs' --glob '*.md' --glob '*.sh' --glob 'Cargo.toml' --glob '!scripts/architecture/check-boundaries.sh'

check_empty "gateway routes must not own business/storage/runtime internals" \
  rg -n "SessionKernel|TaskKernel|UnifiedSessionStore|SmartApprovalGate|CognitiveContextManager|ContextRuntimeKernel|MatrixSqliteRepository|MfgStore::open\\(|Store::open\\(|rusqlite|Connection::open|ConfigLoader|std::fs|fs::|std::env|env::|CrossPlaneAction|CrossPlaneAuditRecord|CrossPlaneExecutionReceipt" \
    crates/gateway/src/api_routes --glob '*.rs' --glob '!cross_plane_routes.rs'

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

check_empty "command-service must remain declarative" \
  rg -n "runtime::|memory::|matrix::|app_mfg::|plugins::|ConfigLoader|GlobalToolRegistry|Connection::open|SqliteConnectionManager::file|std::fs|fs::|std::env|env::|handle_agents_slash_command|handle_skills_slash_command|resolve_skill_path|resolve_skill_invocation" \
    crates/command/service/src crates/command/service/Cargo.toml --glob '*.rs' --glob 'Cargo.toml'

check_empty "gateway main must not own business registries" \
  bash -c 'awk "/#\\[cfg\\(test\\)\\]/{exit} {print}" crates/gateway/src/main.rs | rg -n "GlobalToolRegistry|SkillRegistry|PluginManager|CrossPlaneAction|CrossPlaneAuditRecord|CrossPlaneExecutionReceipt|MfgStore::open|MatrixSqliteRepository|UnifiedSessionStore::open|TaskKernel::open|current_tool_registry|build_runtime_plugin_state|RuntimePluginState" || true'

check_empty "matrix core must not contain mfg application semantics" \
  bash -c 'mixed="matrix_""mfg"; reverse="mfg_""matrix"; adapter="Mfg""Matrix""Adapter"; rg -n "Mfg|mfg|manufacturing|server_manufacturing|${mixed}|${reverse}|${adapter}" crates/matrix/core crates/matrix/repository --glob "*.rs" --glob "Cargo.toml"'

check_empty "app-mfg must not depend on gateway or runtime internals" \
  rg -n "gateway::|runtime::|use gateway|use runtime|crate::runtime" \
    crates/app-mfg/src crates/app-mfg/Cargo.toml --glob '*.rs' --glob 'Cargo.toml'

check_empty "production direct sqlite opens must stay in storage/repository adapters" \
  bash -c 'rg -n "Connection::open\\(|SqliteConnectionManager::file|TaskKernel::open\\(|UnifiedSessionStore::open\\(|MfgStore::open\\(|SqliteStore::open\\(|MatrixSqliteRepository::open\\(|Store::open\\(" crates --glob "*.rs" --glob "!**/tests/**" --glob "!crates/storage/**" --glob "!crates/memory/src/store/sqlite.rs" --glob "!crates/memory/src/store/session.rs" --glob "!crates/memory/src/store/verbatim.rs" --glob "!crates/memory/src/session_store.rs" --glob "!crates/memory/src/sqlite_persistence.rs" --glob "!crates/memory/src/maintenance.rs" --glob "!crates/matrix/repository/**" --glob "!crates/app-mfg/src/repository.rs" --glob "!crates/app-mfg/src/store.rs" | rg -v "open_in_memory|/tests/|^crates/gateway/src/api_routes.rs:|^crates/gateway/src/task_kernel.rs:|^crates/gateway/src/session_kernel.rs:|^crates/gateway/src/main.rs:1[0-9][0-9][0-9][0-9]:|^crates/memory/src/.*:[0-9]+:.*(tmp|test|example|//!|///)" || true'

check_empty "storage direct-open allowlist must stay empty" \
  bash -c 'if [[ "$(tr -d "[:space:]" < crates/storage/direct-open-allowlist.json)" != "[]" ]]; then cat crates/storage/direct-open-allowlist.json; fi'

echo "Checking cargo entrypoint dependency summaries"
cargo tree -p cli --depth 1 --no-default-features

if cargo tree -p tui --no-default-features | rg "(^|[ ├└│─])((runtime|matrix-core|matrix-repository|storage|tools|command-contract|command-service) v|memory|app-mfg|app_mfg|rusqlite)"; then
  echo "FAIL tui forbidden dependency tree"
  fail=1
else
  echo "PASS tui forbidden dependency tree"
fi

if cargo tree -p app-mfg --no-default-features | rg "(^|[ ├└│─])(runtime v|gateway v|gateway =)"; then
  echo "FAIL app-mfg forbidden dependency tree"
  fail=1
else
  echo "PASS app-mfg forbidden dependency tree"
fi

if cargo tree -p matrix-core --no-default-features | rg "(^|[ ├└│─])((runtime|gateway|app-mfg) v|app_mfg|mfg)"; then
  echo "FAIL matrix forbidden dependency tree"
  fail=1
else
  echo "PASS matrix forbidden dependency tree"
fi

exit "$fail"
