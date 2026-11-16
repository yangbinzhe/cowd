#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

fail=0

check_empty() {
  local name="$1"
  shift
  local output status
  if output="$("$@")"; then
    status=0
  else
    status=$?
  fi
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

# Producers return 0 on completion, including an empty enumeration. Unlike rg,
# an awk/cargo/read failure with exit 1 is an error, not an empty scan.
production_sources() {
  local program="$1" files status file
  shift
  if files="$(rg --files "$@")"; then
    :
  else
    status=$?
    if [[ "$status" -eq 1 ]]; then
      return 0
    fi
    echo "source enumeration failed (exit $status)" >&2
    return 2
  fi
  [[ -n "$files" ]] || return 0
  while IFS= read -r file; do
    if awk "$program" "$file"; then
      :
    else
      status=$?
      echo "source extraction failed: $file (exit $status)" >&2
      return 2
    fi
  done <<<"$files"
}

# Each filter completes before the next starts. A later empty result cannot
# erase an earlier error, and no early consumer shutdown creates SIGPIPE.
filter_matches() {
  local matches="$1" exclude="$2" status
  if [[ -z "$matches" ]]; then
    return 1
  fi
  if [[ -z "$exclude" ]]; then
    printf '%s\n' "$matches"
  elif rg -v "$exclude" <<<"$matches"; then
    return 0
  else
    status=$?
    if [[ "$status" -eq 1 ]]; then
      return 1
    fi
    echo "exclusion filter failed (exit $status)" >&2
    return 2
  fi
}

scan_text() {
  local text="$1" pattern="$2" exclude="$3" matches status
  [[ -n "$text" ]] || return 1
  if matches="$(rg -n "$pattern" <<<"$text")"; then
    filter_matches "$matches" "$exclude"
  else
    status=$?
    if [[ "$status" -eq 1 ]]; then
      return 1
    fi
    echo "scan filter failed (exit $status)" >&2
    return 2
  fi
}

scan_command() {
  local pattern="$1" exclude="$2" text status
  shift 2
  if text="$("$@")"; then
    scan_text "$text" "$pattern" "$exclude"
  else
    status=$?
    echo "scan producer failed: $1 (exit $status)" >&2
    return 2
  fi
}

scan_rg_files() {
  local pattern="$1" exclude="$2" matches status
  shift 2
  if matches="$(rg -n "$pattern" "$@")"; then
    filter_matches "$matches" "$exclude"
  else
    status=$?
    if [[ "$status" -eq 1 ]]; then
      return 1
    fi
    echo "file scan failed (exit $status)" >&2
    return 2
  fi
}

scan_gateway_production() {
  scan_command "$1" '' production_sources \
    '/#\[cfg\(test\)\]/{exit} {print FILENAME ":" FNR ":" $0}' \
    crates/gateway/src -g '*.rs' -g '!**/tests/**' -g '!**/tests.rs'
}

allowlist_residue() {
  local path='crates/storage/direct-open-allowlist.json' normalized status
  if normalized="$(tr -d '[:space:]' < "$path")"; then
    :
  else
    status=$?
    echo "allowlist read failed (exit $status)" >&2
    return 2
  fi
  if [[ "$normalized" == '[]' ]]; then
    return 0
  fi
  if [[ -z "$normalized" ]]; then
    echo "allowlist must contain [], not an empty file" >&2
    return 2
  fi
  if cat "$path"; then
    return 0
  else
    status=$?
    echo "allowlist output failed (exit $status)" >&2
    return 2
  fi
}

check_dependency_text() {
  local name="$1" pass_name="$2" pattern="$3" text="$4" matches status
  if [[ -z "$text" ]]; then
    status=1
    matches=''
  elif matches="$(rg "$pattern" <<<"$text")"; then
    status=0
  else
    status=$?
  fi
  if [[ "$status" -eq 0 ]]; then
    printf '%s\n' "$matches"
    echo "FAIL $name"
    fail=1
  elif [[ "$status" -eq 1 ]]; then
    echo "PASS $pass_name"
  else
    echo "ERROR $name"
    fail=1
  fi
}

check_dependency_tree() {
  local name="$1" pass_name="$2" pattern="$3" tree status
  shift 3
  if tree="$(cargo tree "$@")"; then
    check_dependency_text "$name" "$pass_name" "$pattern" "$tree"
  else
    status=$?
    echo "ERROR $name (cargo exit $status)"
    fail=1
  fi
}

check_empty "gateway second AI execution loop" \
  scan_gateway_production 'execute_model_tool_cycle|run_turn_async|run_prompt\(|run_repl\(|mod repl;|livecli'

check_empty "gateway Session ingress must use the public Runtime admission port" \
  scan_command 'runtime::execution_core::graph|ExecutionResource(Kind|Lease|Manager)|ResourceObservation|ResourceResultClass' '' \
    awk '/#\[cfg\(test\)\]/{exit} {print FILENAME ":" FNR ":" $0}' crates/gateway/src/runtime/session_runtime_bridge.rs

check_empty "gateway removed process compatibility paths" \
  scan_gateway_production 'crate::daemon::|DaemonConfig|run_daemon|COWD_AUTH_BROKER_BIN|COWD_INTERNAL_PROCESS_BIN|cowd-auth-broker'

check_empty "gateway direct product APP implementation ownership" \
  scan_gateway_production 'cowd_product_apps::|cowd_app_sdk::|app_mfg::|app_mfg_contract::'

check_empty "built-in channel document operations" \
  rg -n "service\.feishu\.docx|feishu\.readonly|docx:read|doc_ops" \
    crates/surface/src crates/connector/src crates/gateway/src crates/runtime/src crates/tui/src \
    --glob '*.rs' --glob '!**/tests/**'

check_empty "cli business command names" \
  rg -n "\\b(run|chat|prompt|mcp serve)\\b" crates/cli/src/main.rs --glob '*.rs'

check_empty "cli business modules" \
  rg -n "use .*\\b(auth|session|memory|matrix|mfg|agent|daemon)\\b|\\b(auth|session|memory|matrix|mfg|agent|daemon)::|mod (auth|session|memory|matrix|mfg|agent|daemon)" crates/cli/src/main.rs --glob '*.rs'

check_empty "daemon business management" \
  rg -n "daemon status|daemon start$|daemon stop|daemon restart|daemon_client|socket business" crates --glob '*.rs' --glob '!**/tests/**' --glob '!crates/auth-broker/**' --glob '!crates/sandbox-launcher/**'

check_empty "tui direct business dependencies" \
  rg -n "(^|[^[:alnum:]_:])runtime::|use runtime::|use app_mfg::|app_mfg::|use matrix_core::|matrix_core::|use matrix_repository::|matrix_repository::|use storage::|storage::|rusqlite|use tools::|tools::|use memory::|memory::|use command_contract::|command_contract::|use command_service::|command_service::" crates/tui/src --glob '*.rs' --glob '!lib.rs' --glob '!boundary_policy.rs'

check_empty "runtime entrypoint reverse dependencies" \
  scan_rg_files 'gateway::|tui::|cli::|app_mfg::' '"(gateway|tui|cli|app_mfg)::' crates/runtime/src --glob '*.rs'

check_empty "compat harness workspace residue" \
  rg -n "compat-harness|compat_harness" Cargo.toml crates/*/Cargo.toml crates --glob '*.rs' --glob '!**/tests/**'

check_empty "api crate naming residue" \
  rg -n "crates/api|(^|[^[:alnum:]_])api::" crates --glob '*.rs' --glob 'Cargo.toml'

check_empty "obsolete internal cowd package alias residue" \
  scan_rg_files 'cowd_memory|cowd_storage|mod cowd_|pub mod cowd_' '^crates/runtime/src/lib.rs:[0-9]+:(pub mod cowd_(dirs|event);|pub use cowd_(dirs|event)(::|;|\{))' crates --glob '*.rs'

check_empty "AI bin install residue" \
  rg -n "~/AI/bin|AI/bin" README.md docs scripts crates --glob '*.rs' --glob '*.md' --glob '*.sh' --glob 'Cargo.toml' --glob '!scripts/architecture/check-boundaries.sh'

check_empty "gateway routes must not own business/storage/runtime internals" \
  scan_command 'MatrixSqliteRepository|MfgStore::open\(|Store::open\(|rusqlite|Connection::open|SqliteConnectionManager::file|ConfigLoader::default_for|CrossPlaneAction|CrossPlaneAuditRecord|CrossPlaneExecutionReceipt' '' \
    production_sources '/#\[cfg\(test\)\]/{exit} {print FILENAME ":" FNR ":" $0}' \
    crates/gateway/src/api_routes -g '*.rs' -g '!cross_plane_routes.rs' -g '!**/tests/**' -g '!**/tests.rs'

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
  scan_command 'handle_plugins_slash_command|build_plugin_manager|PluginManagerConfig' '' \
    awk '/#\[cfg\(test\)\]/{exit} {print}' crates/gateway/src/main.rs

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

check_empty "gateway services must not route through slash command execution" \
  rg -n "slash_catalog|handle_.*slash_command|resolve_skill_invocation|resolve_skill_path" crates/gateway/src/services --glob "*.rs"

check_empty "gateway service registry must not own context use cases" \
  rg -n "impl GatewayServices" crates/gateway/src/services/context_service.rs

check_empty "gateway entry must not route skill invocation through slash catalog" \
  rg -n "slash_catalog::resolve_skill_invocation|slash_catalog::resolve_skill_path" crates/gateway/src/entry --glob "*.rs"

check_empty "gateway main must not own business registries" \
  scan_command 'GlobalToolRegistry|SkillRegistry|PluginManager|CrossPlaneAction|CrossPlaneAuditRecord|CrossPlaneExecutionReceipt|MfgStore::open|MatrixSqliteRepository|UnifiedSessionStore::open|TaskKernel::open|current_tool_registry|build_runtime_plugin_state|RuntimePluginState' '' \
    awk '/#\[cfg\(test\)\]/{exit} {print}' crates/gateway/src/main.rs

check_empty "matrix core must not contain product APP semantics outside ownership cutover" \
  scan_command 'Mfg|mfg|manufacturing|server_manufacturing' 'crates/matrix/core/src/lib.rs:[0-9]+:.*MfgOwnershipSplitSnapshotV1' \
    production_sources '/#\[cfg\(test\)\]/{exit} {print FILENAME ":" FNR ":" $0}' \
    crates/matrix/core crates/matrix/repository -g '*.rs' -g 'Cargo.toml' -g '!ownership_import.rs' -g '!**/tests/**' -g '!**/tests.rs'

check_empty "production direct sqlite opens must stay in storage/repository adapters" \
  scan_command 'Connection::open\(|SqliteConnectionManager::file|TaskKernel::open\(|UnifiedSessionStore::open\(|MfgStore::open\(|SqliteStore::open\(|MatrixSqliteRepository::open\(' 'open_in_memory|/tests/|^crates/gateway/src/api_routes/mod.rs:|^crates/gateway/src/kernel/task_kernel.rs:|^crates/gateway/src/kernel/session_kernel.rs:|^crates/gateway/src/main.rs:1[0-9][0-9][0-9][0-9]:|^crates/memory/src/.*:[0-9]+:.*(tmp|test|example)' \
    production_sources '/#\[cfg\(test\)\]/{exit} !/^[[:space:]]*\/\// {print FILENAME ":" FNR ":" $0}' \
    crates -g '*.rs' -g '!**/tests/**' -g '!**/tests.rs' -g '!**/*_tests.rs' -g '!**/*_test.rs' -g '!crates/storage/**' -g '!crates/connector/src/source.rs' -g '!crates/connector/src/lib.rs' -g '!crates/session/src/persistence/**' -g '!crates/memory/src/store/**' -g '!crates/memory/src/session/session_store.rs' -g '!crates/memory/src/session/state_rebuilder.rs' -g '!crates/memory/src/knowledge/mod.rs' -g '!crates/memory/src/kernel/cognitive.rs' -g '!crates/memory/src/lifecycle/maintenance.rs' -g '!crates/memory/src/ops/sqlite_persistence.rs' -g '!crates/matrix/repository/**' -g '!crates/runtime/src/recovery/runtime_event_store.rs' -g '!crates/runtime/src/mission/task.rs' -g '!crates/runtime/src/team/team_discovery.rs' -g '!crates/runtime/src/context/artifact.rs' -g '!crates/gateway/src/infrastructure/selected_storage.rs'

check_empty "storage direct-open allowlist must stay empty" \
  allowlist_residue

echo "Checking cargo entrypoint dependency summaries"
if cargo tree -p cli --depth 1 --no-default-features; then
  :
else
  echo "ERROR cli entrypoint dependency summary"
  fail=1
fi

for entrypoint in gateway cli tui cowd-app-host; do
  check_dependency_tree "$entrypoint static product APP dependency" \
    "$entrypoint static product APP dependency absent" \
    '(^|[ ├└│─])(cowd-app-sdk|cowd-product-apps|app-mfg|app-mfg-contract) v' \
    -p "$entrypoint" --edges normal
done

check_dependency_tree 'tui forbidden direct dependency tree' \
  'tui forbidden direct dependency tree' \
  '(^|[ ├└│─])((runtime|matrix-core|matrix-repository|storage|tools|command-contract|command-service) v|memory|app-mfg|app_mfg|rusqlite)' \
  -p tui --depth 1 --no-default-features

check_dependency_tree 'tools forbidden runtime or persistence dependency tree' \
  'tools forbidden runtime or persistence dependency tree' \
  '(^|[ ├└│─])(runtime v|gateway v|provider v|memory v|matrix-core v|matrix-repository v)' \
  -p tools --depth 1 --edges normal

check_dependency_tree 'runtime forbidden transport or Surface dependency tree' \
  'runtime forbidden transport or Surface dependency tree' \
  '(^|[ ├└│─])(gateway v|surface v|connector v|tui v)' \
  -p runtime --depth 1 --edges normal

if memory_tree="$(cargo tree -p memory --depth 1 --edges normal)"; then
  for required in fact-kernel harness-contract storage; do
    if grep -Eq "(^|[ ├└│─])${required} v" <<<"$memory_tree"; then
      echo "PASS memory required dependency: $required"
    else
      status=$?
      if [[ "$status" -eq 1 ]]; then
        echo "FAIL memory required dependency missing: $required"
      else
        echo "ERROR memory required dependency: $required"
      fi
      fail=1
    fi
  done
  check_dependency_text 'memory forbidden runtime, transport, tool, provider, or Matrix dependency tree' \
    'memory forbidden runtime, transport, tool, provider, or Matrix dependency tree' \
    '(^|[ ├└│─])(gateway v|runtime v|surface v|connector v|tui v|provider v|tools v|matrix-core v|matrix-repository v)' \
    "$memory_tree"
else
  echo "ERROR memory dependency tree producer"
  fail=1
fi

check_dependency_tree 'matrix forbidden dependency tree' \
  'matrix forbidden dependency tree' \
  '(^|[ ├└│─])((runtime|gateway|app-mfg) v|app_mfg)' \
  -p matrix-core --no-default-features

exit "$fail"
