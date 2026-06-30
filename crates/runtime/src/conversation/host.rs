use std::sync::Arc;

use crate::agent_collaboration::CollaborationContextResult;
use crate::{
    agent, agent_collaboration, joint_problem_solving, model_context_window_with_overrides,
    permissions::SharedPrompter, CompactionConfig, CompactionResult, ContextEnvelope, ContextItem,
    ContextProfile, ConversationMessage, CowdEvent, CowdEventBus, HookAbortSignal,
    HookProgressReporter, PermissionPolicy, ProviderRuntimeClient, ProviderToolDefinition,
    ResumeContextPacket, RuntimeError, RuntimeFeatureConfig, Session, ToolCallback, ToolExecutor,
    TurnSummary,
};
use harness_contract::skill::{AgentSkillProfile, SkillCapabilityProfile};

/// Runtime-owned host for the standard provider-backed conversation engine.
///
/// Gateway supplies service adapters such as tool executors and stream callbacks, but
/// it does not own the provider client or concrete conversation runtime type.
pub struct StandardRuntimeHost<T>
where
    T: ToolExecutor,
{
    runtime: Option<crate::ConversationRuntime<ProviderRuntimeClient, T>>,
}

/// Inputs required to build a standard provider-backed runtime host.
pub struct StandardRuntimeHostConfig<T>
where
    T: ToolExecutor,
{
    pub session: Session,
    pub model: String,
    pub tool_definitions: Vec<ProviderToolDefinition>,
    pub tool_executor: Arc<T>,
    pub permission_policy: PermissionPolicy,
    pub system_prompt: Vec<String>,
    pub feature_config: RuntimeFeatureConfig,
    pub emit_output: bool,
    pub stream_callback: Option<std::sync::mpsc::SyncSender<CowdEvent>>,
    pub tool_callback: Option<Arc<dyn ToolCallback>>,
    pub model_context_window: Option<u32>,
    pub session_store: Option<Arc<memory::session_store::UnifiedSessionStore>>,
    pub hook_progress_reporter: Option<Box<dyn HookProgressReporter>>,
    pub external_context_items: Vec<ContextItem>,
    pub skill_profiles: Vec<SkillCapabilityProfile>,
    pub agent_skill_profile: AgentSkillProfile,
    pub enable_collaboration: bool,
    pub subagent_model: String,
    pub subagent_tool_definitions: Vec<ProviderToolDefinition>,
    pub subagent_tool_executor: Arc<T>,
}

impl<T> StandardRuntimeHost<T>
where
    T: ToolExecutor,
{
    pub fn new(config: StandardRuntimeHostConfig<T>) -> Result<Self, String> {
        let model_context_window = config.model_context_window.unwrap_or_else(|| {
            let overrides = config.feature_config.model_context_windows();
            model_context_window_with_overrides(&config.model, Some(&overrides))
        });
        let mut runtime = crate::ConversationRuntime::new_with_features(
            config.session,
            ProviderRuntimeClient::new(config.model, config.tool_definitions)?
                .with_emit_output(config.emit_output)
                .with_stream_callback(config.stream_callback.clone()),
            config.tool_executor.clone(),
            config.permission_policy,
            config.system_prompt,
            &config.feature_config,
        )
        .with_model_context_window(model_context_window)
        .with_skill_profiles(config.skill_profiles)
        .with_agent_skill_profile(config.agent_skill_profile);

        if let Some(store) = config.session_store {
            runtime = runtime.with_session_store(store);
        }
        if let Some(callback) = config.tool_callback {
            runtime = runtime.with_tool_callback(callback);
        }
        if let Some(reporter) = config.hook_progress_reporter {
            runtime = runtime.with_hook_progress_reporter(reporter);
        }
        runtime = runtime.with_cowd_event_bus(CowdEventBus::new());
        for item in config.external_context_items {
            runtime.push_external_context_item(item);
        }

        if config.enable_collaboration {
            let subagent_model = config.subagent_model;
            let subagent_tool_definitions = config.subagent_tool_definitions;
            let executor = agent::ProductionExecutor::new(
                move || {
                    ProviderRuntimeClient::new(
                        subagent_model.clone(),
                        subagent_tool_definitions.clone(),
                    )
                    .expect("sub-agent provider client creation failed")
                },
                config.subagent_tool_executor.clone(),
            );
            let executor_arc = Arc::new(executor);
            runtime =
                runtime.with_collaboration(agent_collaboration::new_boxed(executor_arc.clone()));
            let jps_pipeline = joint_problem_solving::new_boxed::<
                agent::ProductionExecutor<ProviderRuntimeClient, T>,
            >(executor_arc);
            runtime = runtime.with_jps_pipeline(jps_pipeline);
        }

        Ok(Self {
            runtime: Some(runtime),
        })
    }

    pub fn with_hook_abort_signal(mut self, hook_abort_signal: HookAbortSignal) -> Self {
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before installing hook abort signal");
        self.runtime = Some(runtime.with_hook_abort_signal(hook_abort_signal));
        self
    }

    pub fn install_turn_control(
        &mut self,
        cancellation_token: crate::CancellationToken,
        hook_abort_signal: HookAbortSignal,
    ) {
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before installing turn control");
        self.runtime = Some(
            runtime
                .with_cancellation_token(cancellation_token)
                .with_hook_abort_signal(hook_abort_signal),
        );
    }

    pub fn cowd_bus(&self) -> Option<&CowdEventBus> {
        self.runtime_ref().cowd_bus()
    }

    pub fn set_context_profile(&self, profile: ContextProfile) {
        self.runtime_ref().set_context_profile(profile);
    }

    pub fn max_iterations(&self) -> usize {
        self.runtime_ref().max_iterations()
    }

    pub fn set_max_iterations(&mut self, max_iterations: usize) {
        self.runtime_mut().set_max_iterations(max_iterations);
    }

    pub fn inject_resume_context(&self, packet: ResumeContextPacket) {
        self.runtime_ref().inject_resume_context(packet);
    }

    pub async fn run_turn_async(
        &mut self,
        content: &str,
        prompter: &SharedPrompter,
    ) -> Result<TurnSummary, RuntimeError> {
        self.runtime_mut().run_turn_async(content, prompter).await
    }

    pub async fn append_external_message(
        &self,
        message: ConversationMessage,
    ) -> Result<(), RuntimeError> {
        self.runtime_ref().append_external_message(message).await
    }

    pub fn session(&self) -> Session {
        self.runtime_ref().session()
    }

    pub fn compact_active_session(
        &mut self,
        config: CompactionConfig,
    ) -> (CompactionResult, Session) {
        let result = self.runtime_ref().compact(config);
        if result.removed_message_count > 0 {
            *self.runtime_mut().session_mut() = result.compacted_session.clone();
        }
        let session = self.runtime_ref().session();
        (result, session)
    }

    pub fn active_session_stats_session(&self) -> Session {
        self.runtime_ref().session()
    }

    pub async fn update_session_model(&mut self, model: &str) {
        let mut session = self.runtime_mut().session_mut_async().await;
        session.model = Some(model.to_string());
    }

    pub fn last_context_envelope(&self) -> Option<ContextEnvelope> {
        self.runtime_ref().last_context_envelope()
    }

    pub fn last_context_turn_report(&self) -> Option<harness_contract::context::ContextTurnReport> {
        self.runtime_ref().last_context_turn_report()
    }

    pub fn take_collaboration_result(&self) -> Option<CollaborationContextResult> {
        self.runtime_ref().take_collaboration_result()
    }

    fn runtime_ref(&self) -> &crate::ConversationRuntime<ProviderRuntimeClient, T> {
        self.runtime
            .as_ref()
            .expect("runtime should exist while standard runtime host is alive")
    }

    fn runtime_mut(&mut self) -> &mut crate::ConversationRuntime<ProviderRuntimeClient, T> {
        self.runtime
            .as_mut()
            .expect("runtime should exist while standard runtime host is alive")
    }
}
