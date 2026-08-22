use std::{path::Path, sync::Arc};

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use harness_contract::skill::{AgentSkillProfile, SkillAdapterKind};
use runtime::Session;

use crate::gateway_tool_executor::GatewayToolExecutor;
use crate::runtime_bootstrap::RuntimeSessionBootstrapSnapshot;
use crate::runtime_entry::GatewayRuntimeEntry;
use crate::{
    filter_tool_specs, inject_auto_resume_context, merge_resume_context_packets,
    permission_policy_with_control, runtime_capability_context_item,
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
    execution_policy: runtime::SessionExecutionPolicy,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<tokio::sync::mpsc::Sender<runtime::CowdEvent>>,
    runtime_session_snapshot: RuntimeSessionBootstrapSnapshot,
    resume_context: Option<runtime::ResumeContextPacket>,
) -> Result<GatewayRuntimeEntry, Box<dyn std::error::Error>> {
    create_runtime_entry_with_bootstrap_state(
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
        execution_policy,
        tool_callback,
        stream_callback,
        runtime_session_snapshot,
        resume_context,
    )
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_runtime_entry_with_bootstrap_state(
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
    execution_policy: runtime::SessionExecutionPolicy,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<tokio::sync::mpsc::Sender<runtime::CowdEvent>>,
    runtime_session_snapshot: RuntimeSessionBootstrapSnapshot,
    resume_context: Option<runtime::ResumeContextPacket>,
) -> Result<GatewayRuntimeEntry, Box<dyn std::error::Error>> {
    session.session_id = session_id.to_string();
    session.model = Some(model.clone());
    let session_resume_packet =
        merge_resume_context_packets(session_db_resume_context_packet(&session), resume_context);
    let RuntimeSessionBootstrapSnapshot {
        feature_config,
        tool_registry,
        plugin_registry,
    } = runtime_session_snapshot;
    plugin_registry.initialize()?;
    let policy_control =
        runtime::permissions::SessionExecutionPolicyControl::from_policy(execution_policy.clone());
    let policy = permission_policy_with_control(policy_control, &feature_config, &tool_registry)
        .map_err(std::io::Error::other)?;
    let overrides = feature_config.model_context_windows();
    let model_ctx = runtime::model_context_window_with_overrides(&model, Some(overrides));
    let workspace_item = workspace_context_item(&session, model_ctx);
    let workspace_root = session
        .workspace_root()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| tool_host.workspace_root().to_path_buf());
    // Skill discovery belongs to Gateway composition/reload. Session
    // activation pins the already-inspected Runtime catalog and never scans
    // package roots on the request path.
    let skill_catalog = runtime_services.skill_catalog();
    let tool_definitions = filter_tool_specs(&tool_registry, allowed_tools.as_ref());
    let capability_item =
        runtime_capability_context_item(&tool_definitions, allowed_tools.as_ref(), model_ctx);
    let runtime_session_id = session.session_id.clone();
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
    let primary_memory_context =
        memory::MemoryTurnContext::new(session_id, primary_memory_agent_id.clone())
            .with_definition_lineage_id(primary_definition_lineage.clone())
            .with_project_id(Some(runtime::memory_project_id_for_workspace(
                &workspace_root,
            )))
            .with_task_id(Some(primary_binding.data_lease.task_id.clone()))
            .with_team_id(primary_binding.data_lease.team_id.clone())
            .with_cognitive_read_scopes(primary_memory_read_scopes.clone());
    let tool_executor = std::sync::Arc::new(
        GatewayToolExecutor::from_tool_host(allowed_tools.clone(), emit_output, tool_host)
            .with_runtime_session_id(runtime_session_id)
            .with_runtime_memory_context(primary_memory_context)
            .with_runtime_model_lease(model.clone())
            .with_runtime_permission_ceiling(execution_policy.permission_mode),
    );
    tool_executor
        .bind_runtime_services(Arc::clone(&runtime_services))
        .map_err(std::io::Error::other)?;
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
        hook_progress_reporter: emit_output.then(|| {
            Box::new(GatewayHookProgressReporter) as Box<dyn runtime::HookProgressReporter>
        }),
        external_context_items: vec![workspace_item, capability_item],
        skill_profiles: skill_catalog.profiles(),
        agent_skill_profile: default_runtime_agent_skill_profile(),
        skill_prompt_assets: skill_catalog.prompt_assets(),
        skill_instruction_source: skill_catalog.instruction_source(),
        memory_agent_id: primary_memory_agent_id,
        memory_definition_lineage_id: primary_definition_lineage,
        memory_team_id: None,
        memory_read_scopes: primary_memory_read_scopes,
        reality_binding: Some(primary_binding),
        execution_identity: None,
        execution_lineage: None,
        execution_parent: None,
        execution_role: runtime::TurnExecutionRole::RootTurn,
        recovered_tool_receipt_count: 0,
        runtime_services,
    })
    .map_err(std::io::Error::other)?;
    if let Some(ref tx) = stream_callback {
        let _ = tx.try_send(runtime::CowdEvent::ContextWindow(model_ctx as u64));
    }
    let mut entry = GatewayRuntimeEntry::new(runtime, plugin_registry, false);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_agent_skill_profile_defaults_to_prompt_only() {
        let profile = default_runtime_agent_skill_profile();

        assert_eq!(profile.adapter_ceiling, vec![SkillAdapterKind::PromptOnly]);
    }
}
