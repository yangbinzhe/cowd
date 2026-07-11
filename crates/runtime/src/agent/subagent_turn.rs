use crate::{
    CancellationToken, HookAbortSignal, PermissionPolicy, ProviderToolDefinition, Session,
    SharedPrompter, StandardRuntimeHost, StandardRuntimeHostConfig, ToolExecutor, TurnSummary,
};

#[derive(Debug, Clone)]
pub struct ProviderSubAgentTurnConfig {
    pub provider_registry: std::sync::Arc<crate::ProviderRegistry>,
    pub model: String,
    pub system_prompt: Vec<String>,
    pub tool_definitions: Vec<ProviderToolDefinition>,
    pub permission_policy: PermissionPolicy,
    pub max_iterations: usize,
    pub cancellation_token: Option<CancellationToken>,
}

pub fn run_provider_subagent_turn<T>(
    config: ProviderSubAgentTurnConfig,
    tool_executor: T,
    prompt: String,
) -> Result<TurnSummary, String>
where
    T: ToolExecutor,
{
    let runtime_services =
        crate::RuntimeServices::in_memory().map_err(|error| error.to_string())?;
    let tool_executor = std::sync::Arc::new(tool_executor);
    let mut runtime = StandardRuntimeHost::new(StandardRuntimeHostConfig {
        runtime_services,
        session: Session::new(),
        provider_registry: config.provider_registry,
        model: config.model.clone(),
        tool_definitions: config.tool_definitions.clone(),
        tool_executor: std::sync::Arc::clone(&tool_executor),
        permission_policy: config.permission_policy,
        system_prompt: config.system_prompt,
        feature_config: crate::RuntimeFeatureConfig::default(),
        emit_output: false,
        stream_callback: None,
        tool_callback: None,
        model_context_window: None,
        session_store: None,
        hook_progress_reporter: None,
        external_context_items: Vec::new(),
        skill_profiles: Vec::new(),
        agent_skill_profile: harness_contract::skill::AgentSkillProfile::default(),
        enable_collaboration: false,
        subagent_model: config.model,
        subagent_tool_definitions: config.tool_definitions,
        subagent_tool_executor: tool_executor,
    })?;
    runtime.set_max_iterations(config.max_iterations);
    if let Some(token) = config.cancellation_token {
        runtime.install_turn_control(token, HookAbortSignal::default());
    }
    let local_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    local_runtime
        .block_on(runtime.submit_turn(&prompt, &SharedPrompter::none()))
        .map_err(|error| error.to_string())
}

#[must_use]
pub fn final_assistant_text(summary: &TurnSummary) -> String {
    summary.final_answer.clone()
}
