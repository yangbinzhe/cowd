use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use harness_contract::skill::{AgentSkillProfile, SkillAdapterKind};
use runtime::{PermissionMode, Session};

use crate::gateway_tool_executor::GatewayToolExecutor;
use crate::runtime_bootstrap::RuntimeBootstrapState;
use crate::runtime_entry::GatewayRuntimeEntry;
use crate::services::runtime_skill_assets_for_workspace;
use crate::{
    filter_tool_specs, inject_auto_resume_context, merge_resume_context_packets, permission_policy,
    runtime_capability_context_item, semantic_checkpoint_resume_context_packet,
    session_db_resume_context_packet, workspace_context_item, AllowedToolSet,
};

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_runtime_entry(
    runtime_services: Arc<runtime::RuntimeServices>,
    provider_registry: Arc<runtime::ProviderRegistry>,
    tool_host: Arc<tools::ToolHost>,
    session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
) -> Result<GatewayRuntimeEntry, Box<dyn std::error::Error>> {
    let runtime_plugin_state = crate::runtime_bootstrap::assemble_runtime_state()?;
    create_runtime_entry_with_bootstrap_state(
        None,
        runtime_services,
        provider_registry,
        tool_host,
        session,
        session_id,
        model,
        system_prompt,
        enable_tools,
        emit_output,
        allowed_tools,
        permission_mode,
        tool_callback,
        stream_callback,
        runtime_plugin_state,
    )
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_runtime_entry_with_session_store(
    session_store: Arc<memory::session_store::UnifiedSessionStore>,
    runtime_services: Arc<runtime::RuntimeServices>,
    provider_registry: Arc<runtime::ProviderRegistry>,
    tool_host: Arc<tools::ToolHost>,
    session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
) -> Result<GatewayRuntimeEntry, Box<dyn std::error::Error>> {
    let runtime_plugin_state = crate::runtime_bootstrap::assemble_runtime_state()?;
    create_runtime_entry_with_bootstrap_state(
        Some(session_store),
        runtime_services,
        provider_registry,
        tool_host,
        session,
        session_id,
        model,
        system_prompt,
        enable_tools,
        emit_output,
        allowed_tools,
        permission_mode,
        tool_callback,
        stream_callback,
        runtime_plugin_state,
    )
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_runtime_entry_with_bootstrap_state(
    session_store: Option<Arc<memory::session_store::UnifiedSessionStore>>,
    runtime_services: Arc<runtime::RuntimeServices>,
    provider_registry: Arc<runtime::ProviderRegistry>,
    tool_host: Arc<tools::ToolHost>,
    mut session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    _enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
    runtime_plugin_state: RuntimeBootstrapState,
) -> Result<GatewayRuntimeEntry, Box<dyn std::error::Error>> {
    session.session_id = session_id.to_string();
    if session.model.is_none() {
        session.model = Some(model.clone());
    }
    let session_resume_packet = merge_resume_context_packets(
        session_db_resume_context_packet(&session),
        session_store.as_ref().and_then(|store| {
            semantic_checkpoint_resume_context_packet(Arc::clone(store), session_id)
        }),
    );
    let RuntimeBootstrapState {
        feature_config,
        tool_registry,
        plugin_registry,
        mcp_state,
    } = runtime_plugin_state;
    plugin_registry.initialize()?;
    let policy = permission_policy(permission_mode, &feature_config, &tool_registry)
        .map_err(std::io::Error::other)?;
    let overrides = feature_config.model_context_windows();
    let model_ctx = runtime::model_context_window_with_overrides(&model, Some(&overrides));
    let workspace_item = workspace_context_item(&session, model_ctx);
    let workspace_root = session
        .workspace_root()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let skill_assets = runtime_skill_assets_for_workspace(&workspace_root);
    runtime_services.replace_skill_catalog(runtime::RuntimeSkillCatalog::new(
        skill_assets.profiles,
        skill_assets.prompt_assets,
    ));
    let skill_catalog = runtime_services.skill_catalog();
    let tool_definitions = filter_tool_specs(&tool_registry, allowed_tools.as_ref());
    let capability_item =
        runtime_capability_context_item(&tool_definitions, allowed_tools.as_ref(), model_ctx);
    let runtime_session_id = session.session_id.clone();
    let tool_executor = std::sync::Arc::new(
        GatewayToolExecutor::from_tool_host(
            allowed_tools.clone(),
            emit_output,
            tool_host,
            mcp_state.clone(),
        )
        .with_runtime_session_id(runtime_session_id)
        .with_runtime_model_lease(model.clone()),
    );
    tool_executor
        .bind_runtime_services(Arc::clone(&runtime_services))
        .map_err(std::io::Error::other)?;
    let primary_binding = primary_turn_binding(&runtime_services, session_id)?;
    let primary_memory_agent_id = primary_binding.instance.instance_id.clone();
    let primary_definition_lineage = Some(
        primary_binding
            .definition_ref
            .definition_id
            .as_str()
            .to_string(),
    );
    let primary_memory_read_scopes = primary_binding.data_lease.read_scopes.clone();
    let runtime = runtime::StandardRuntimeHost::new(runtime::StandardRuntimeHostConfig {
        session,
        provider_registry,
        model: model.clone(),
        tool_definitions,
        tool_executor: tool_executor.clone(),
        permission_policy: policy,
        system_prompt,
        feature_config,
        emit_output,
        stream_callback: stream_callback.clone(),
        tool_callback,
        model_context_window: Some(model_ctx),
        session_store,
        hook_progress_reporter: emit_output.then(|| {
            Box::new(GatewayHookProgressReporter) as Box<dyn runtime::HookProgressReporter>
        }),
        external_context_items: vec![workspace_item, capability_item],
        skill_profiles: skill_catalog.profiles(),
        agent_skill_profile: default_runtime_agent_skill_profile(),
        skill_prompt_assets: skill_catalog.prompt_assets(),
        memory_agent_id: primary_memory_agent_id,
        memory_definition_lineage_id: primary_definition_lineage,
        memory_team_id: None,
        memory_read_scopes: primary_memory_read_scopes,
        reality_binding: Some(primary_binding),
        execution_parent: None,
        runtime_services,
    })
    .map_err(std::io::Error::other)?;
    if let Some(ref tx) = stream_callback {
        let _ = tx.try_send(runtime::CowdEvent::ContextWindow(model_ctx as u64));
    }
    let mut entry = GatewayRuntimeEntry::new(runtime, plugin_registry, mcp_state, false);
    let resume_context_loaded =
        inject_auto_resume_context(&entry, session_resume_packet, session_id);
    entry.set_resume_context_loaded(resume_context_loaded);
    Ok(entry)
}

fn primary_turn_binding(
    runtime_services: &runtime::RuntimeServices,
    session_id: &str,
) -> Result<harness_contract::agent::AgentBindingSnapshot, Box<dyn std::error::Error>> {
    let definition_id = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/explore")
        .map_err(std::io::Error::other)?;
    let mut request = runtime::agent::binding::AgentBindingRequest::new(
        definition_id,
        RevisionSelector::LatestApprovedStable,
        format!("instance:primary:{session_id}"),
        session_id,
        format!("primary-turn:{session_id}"),
    );
    request.granted_capabilities = vec![AgentCapability::Read, AgentCapability::Search];
    Ok(runtime_services
        .compile_agent_binding(request)
        .map(|compiled| compiled.snapshot)
        .map_err(std::io::Error::other)?)
}

fn default_runtime_agent_skill_profile() -> AgentSkillProfile {
    AgentSkillProfile {
        adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
        ..AgentSkillProfile::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_agent_skill_profile_defaults_to_prompt_only() {
        let profile = default_runtime_agent_skill_profile();

        assert_eq!(profile.adapter_ceiling, vec![SkillAdapterKind::PromptOnly]);
    }
}

struct GatewayHookProgressReporter;

impl runtime::HookProgressReporter for GatewayHookProgressReporter {
    fn on_event(&mut self, event: &runtime::HookProgressEvent) {
        match event {
            runtime::HookProgressEvent::Started {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
            runtime::HookProgressEvent::Completed {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook done {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
            runtime::HookProgressEvent::Cancelled {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook cancelled {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
        }
    }
}
