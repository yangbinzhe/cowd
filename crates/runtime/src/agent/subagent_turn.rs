use crate::{
    CancellationToken, ConversationRuntime, PermissionPolicy, ProviderRuntimeClient,
    ProviderToolDefinition, Session, SharedPrompter, ToolExecutor, TurnSummary,
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
    let api_client = ProviderRuntimeClient::new(
        config.provider_registry,
        config.model,
        config.tool_definitions,
    )?;
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        api_client,
        tool_executor,
        config.permission_policy,
        config.system_prompt,
    )
    .with_max_iterations(config.max_iterations);
    if let Some(token) = config.cancellation_token {
        runtime = runtime.with_cancellation_token(token);
    }
    let local_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    local_runtime
        .block_on(runtime.run_turn_async(prompt, &SharedPrompter::none()))
        .map_err(|error| error.to_string())
}

#[must_use]
pub fn final_assistant_text(summary: &TurnSummary) -> String {
    summary
        .assistant_messages
        .last()
        .map(|message| {
            message
                .blocks
                .iter()
                .filter_map(|block| match block {
                    crate::ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}
