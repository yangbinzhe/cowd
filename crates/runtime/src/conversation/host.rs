use std::sync::Arc;

use crate::agent_collaboration::CollaborationContextResult;
use crate::conversation::ApiClient;
use crate::conversation::{ModelStepIntent, ModelToolCall};
use crate::execution_core::graph::executors::ScopedNodeBackend;
use crate::execution_core::{
    ExecutionCompileRequest, ExecutionGraphCompiler, ExecutionGraphReplan, NodeExecutionOutcome,
    NodeExecutionTicket, NodeExecutorError,
};
use crate::{
    agent, agent_collaboration, model_context_window_with_overrides, permissions::SharedPrompter,
    CompactionConfig, CompactionResult, ContentBlock, ContextEnvelope, ContextItem, ContextProfile,
    ContextSourceKind, ConversationMessage, CowdEvent, CowdEventBus, HookAbortSignal,
    HookProgressReporter, PermissionPolicy, ProviderRuntimeClient, ProviderToolDefinition,
    ResumeContextPacket, RuntimeError, RuntimeFeatureConfig, Session, ToolCallback, ToolExecutor,
    TurnSummary,
};
use async_trait::async_trait;
use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionUsage,
};
use harness_contract::skill::{AgentSkillProfile, SkillCapabilityProfile};
use harness_contract::turn::{
    SessionInputEnvelope, SessionInputProjection, SessionInputReceipt, TurnId, TurnInboxSnapshot,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Runtime-owned host for the standard provider-backed conversation engine.
///
/// Gateway supplies service adapters such as tool executors and stream callbacks, but
/// it does not own the provider client or concrete conversation runtime type.
pub struct StandardRuntimeHost<T>
where
    T: ToolExecutor,
{
    runtime: Option<crate::ConversationRuntime<ProviderRuntimeClient, T>>,
    services: Arc<crate::RuntimeServices>,
    approval_gate_slot:
        Arc<std::sync::RwLock<Option<Arc<crate::approval_gate::SmartApprovalGate>>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnIngressRef {
    pub request_id: String,
    pub turn_id: String,
    pub message_id: String,
    pub session_id: String,
}

/// Inputs required to build a standard provider-backed runtime host.
pub struct StandardRuntimeHostConfig<T>
where
    T: ToolExecutor,
{
    pub runtime_services: Arc<crate::RuntimeServices>,
    pub session: Session,
    pub provider_registry: Arc<crate::ProviderRegistry>,
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
        let services = Arc::clone(&config.runtime_services);
        let approval_gate_slot = Arc::new(std::sync::RwLock::new(None));
        let active_model = config.model.clone();
        let model_context_window = config.model_context_window.unwrap_or_else(|| {
            let overrides = config.feature_config.model_context_windows();
            model_context_window_with_overrides(&active_model, Some(&overrides))
        });
        let mut runtime = crate::ConversationRuntime::new_with_features(
            config.session,
            ProviderRuntimeClient::new(
                Arc::clone(&config.provider_registry),
                active_model.clone(),
                config.tool_definitions,
            )?
            .with_emit_output(config.emit_output)
            .with_stream_callback(config.stream_callback.clone()),
            config.tool_executor.clone(),
            config.permission_policy,
            config.system_prompt,
            &config.feature_config,
        )
        .with_model_context_window(model_context_window)
        .with_runtime_event_store(Arc::clone(services.event_store()))
        .with_skill_profiles(config.skill_profiles)
        .with_agent_skill_profile(config.agent_skill_profile);
        runtime.set_active_model(active_model);

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
            let provider_registry = Arc::clone(&config.provider_registry);
            let executor = agent::ProductionExecutor::new(
                move || {
                    ProviderRuntimeClient::new(
                        Arc::clone(&provider_registry),
                        subagent_model.clone(),
                        subagent_tool_definitions.clone(),
                    )
                    .expect("sub-agent provider client creation failed")
                },
                config.subagent_tool_executor.clone(),
            )
            .with_approval_gate_slot(Arc::clone(&approval_gate_slot));
            let executor_arc = Arc::new(executor);
            runtime = runtime.with_collaboration(agent_collaboration::new_boxed(executor_arc));
        }

        Ok(Self {
            runtime: Some(runtime),
            services,
            approval_gate_slot,
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

    pub fn install_approval_gate(
        &mut self,
        approval_gate: Arc<crate::approval_gate::SmartApprovalGate>,
    ) {
        *self
            .approval_gate_slot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&approval_gate));
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before installing approval gate");
        self.runtime = Some(runtime.with_approval_gate(approval_gate));
    }

    pub fn cowd_bus(&self) -> Option<&CowdEventBus> {
        self.runtime_ref().cowd_bus()
    }

    pub fn services(&self) -> &Arc<crate::RuntimeServices> {
        &self.services
    }

    pub fn admit_session_input(
        &self,
        envelope: SessionInputEnvelope,
        state: crate::RuntimeInputState,
    ) -> SessionInputReceipt {
        self.runtime_ref().admit_session_input(envelope, state)
    }

    pub fn session_input_projection(&self) -> SessionInputProjection {
        self.runtime_ref().session_input_projection()
    }

    pub fn active_turn_inbox(&self, turn_id: Option<TurnId>) -> TurnInboxSnapshot {
        self.runtime_ref().active_turn_inbox(turn_id)
    }

    pub fn session_input_stream(&self) -> crate::SessionInputStream {
        self.runtime_ref().session_input_stream()
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

    pub fn replace_external_context_sources(
        &self,
        sources: &[ContextSourceKind],
        items: Vec<ContextItem>,
    ) {
        let runtime = self.runtime_ref();
        for source in sources {
            runtime.clear_external_context_source(*source);
        }
        for item in items {
            runtime.push_external_context_item(item);
        }
    }

    /// Submit a user turn through the canonical ExecutionGraph runner.
    ///
    /// This is the only production entry point that may start provider-backed
    /// turn work. Gateway and Agent backends receive a terminal result emitted
    /// by the synthesize node instead of inspecting the session transcript.
    pub async fn submit_turn(
        &mut self,
        content: &str,
        prompter: &SharedPrompter,
    ) -> Result<TurnSummary, RuntimeError> {
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before submitting a turn");
        let (runtime, result) = submit_owned_conversation_turn_with_ingress(
            runtime,
            Arc::clone(&self.services),
            content,
            prompter,
            None,
        )
        .await;
        self.runtime = Some(runtime);
        result
    }

    pub async fn submit_ingress_turn(
        &mut self,
        content: &str,
        prompter: &SharedPrompter,
        ingress: TurnIngressRef,
    ) -> Result<TurnSummary, RuntimeError> {
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before submitting an ingress turn");
        let (runtime, result) = submit_owned_conversation_turn_with_ingress(
            runtime,
            Arc::clone(&self.services),
            content,
            prompter,
            Some(ingress),
        )
        .await;
        self.runtime = Some(runtime);
        result
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

    pub async fn session_async(&self) -> Session {
        self.runtime_ref().session_async().await
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
        let runtime = self.runtime_mut();
        runtime.set_active_model(model.to_string());
        let mut session = runtime.session_mut_async().await;
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

/// Drive any concrete conversation runtime through the canonical graph owner.
/// Agent backends use this function so they cannot bypass the same Runner used
/// by the primary Gateway runtime.
pub async fn submit_owned_conversation_turn<C, T>(
    runtime: crate::ConversationRuntime<C, T>,
    services: Arc<crate::RuntimeServices>,
    content: &str,
    prompter: &SharedPrompter,
) -> (
    crate::ConversationRuntime<C, T>,
    Result<TurnSummary, RuntimeError>,
)
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    submit_owned_conversation_turn_with_ingress(runtime, services, content, prompter, None).await
}

async fn submit_owned_conversation_turn_with_ingress<C, T>(
    runtime: crate::ConversationRuntime<C, T>,
    services: Arc<crate::RuntimeServices>,
    content: &str,
    prompter: &SharedPrompter,
    ingress: Option<TurnIngressRef>,
) -> (
    crate::ConversationRuntime<C, T>,
    Result<TurnSummary, RuntimeError>,
)
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    let session_id = runtime.session().session_id;
    let runtime = Arc::new(tokio::sync::Mutex::new(runtime));
    let result = async {
        let state = Arc::new(tokio::sync::Mutex::new(TurnGraphState {
            content: content.to_string(),
            prompter: prompter.clone(),
            first_model_step: true,
            next_calls: Vec::new(),
            next_resource_scopes: Vec::new(),
            assistant_messages: Vec::new(),
            tool_results: Vec::new(),
            prompt_cache_events: Vec::new(),
            iterations: 0,
            input_tokens: 0,
            output_tokens: 0,
            wall_duration_ms: 0,
            model: None,
            summary: None,
            failure: None,
            pending_transcript: std::collections::BTreeMap::new(),
            ingress: ingress.clone(),
            session_id: session_id.clone(),
        }));

        let compile_target = crate::execution_core::StrategyDecisionEngine
            .decide(content, Some(runtime.lock().await.context_profile()))
            .compile_target;
        let turn_payload = serde_json::json!({
            "kind": "conversation_turn",
            "session_id": session_id,
            "content": content,
            "compile_target": compile_target,
            "ingress": ingress,
            "idempotency_key": ingress.as_ref().map(|value| value.request_id.as_str()),
        })
        .to_string();
        let mut graph = ExecutionGraphCompiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: content.to_string(),
                payload_ref: turn_payload,
                target: compile_target,
                resource_scopes: Vec::new(),
            })
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if let Some(ingress) = &ingress {
            let compiled_graph_id = graph.id.clone();
            graph.id = crate::session_execution::session_ingress_graph_id(
                &ingress.session_id,
                &ingress.request_id,
                &ingress.turn_id,
            );
            graph.revision = 0;
            graph.node_results.clear();
            graph.recovery_cursor = Default::default();
            let mut remapped_ids = std::collections::BTreeMap::new();
            for node in &mut graph.nodes {
                let suffix = node
                    .id
                    .strip_prefix(&format!("{compiled_graph_id}:"))
                    .unwrap_or(&node.id)
                    .to_string();
                let previous = node.id.clone();
                node.id = format!("{}:{suffix}", graph.id);
                node.idempotency_key = format!("{}:{suffix}", ingress.request_id);
                remapped_ids.insert(previous, node.id.clone());
            }
            for edge in &mut graph.edges {
                if let Some(id) = remapped_ids.get(&edge.from) {
                    edge.from.clone_from(id);
                }
                if let Some(id) = remapped_ids.get(&edge.to) {
                    edge.to.clone_from(id);
                }
            }
            graph.node_statuses.clear();
            graph
                .node_statuses
                .insert(graph.nodes[0].id.clone(), ExecutionNodeStatus::Planned);
            let root_id = graph.nodes[0].id.clone();
            let dispatch_id = format!("{}:session-dispatch", graph.id);
            let mut dispatch = ExecutionNodeSpec::new(
                ExecutionNodeKind::SessionDispatch,
                crate::SESSION_DISPATCH_EXECUTOR,
                format!(
                    "session_ingress:{}",
                    serde_json::to_string(ingress).unwrap_or_default()
                ),
            );
            dispatch.id = dispatch_id.clone();
            dispatch.idempotency_key = format!("{}:dispatch", ingress.request_id);
            graph.nodes.insert(0, dispatch);
            graph.edges.push(ExecutionEdge {
                from: dispatch_id,
                to: root_id,
                kind: ExecutionEdgeKind::DependsOn,
            });
        }
        let inline_kind = "inline_model".to_string();
        let tool_kind = "tool_batch".to_string();
        for node in &mut graph.nodes {
            node.executor_kind = match node.kind {
                harness_contract::execution_graph::ExecutionNodeKind::InlineModel => {
                    inline_kind.clone()
                }
                harness_contract::execution_graph::ExecutionNodeKind::ToolBatch => {
                    tool_kind.clone()
                }
                harness_contract::execution_graph::ExecutionNodeKind::Verify => {
                    if node.executor_kind
                        == crate::execution_core::graph::executors::CompileTargetGuardExecutor::KIND
                    {
                        node.executor_kind.clone()
                    } else {
                        crate::execution_core::graph::executors::VerifyNodeExecutor::KIND
                            .to_string()
                    }
                }
                harness_contract::execution_graph::ExecutionNodeKind::Synthesize => {
                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND
                        .to_string()
                }
                _ => node.executor_kind.clone(),
            };
        }

        let graph_id = graph.id.clone();
        let persisted_graph = services
            .graph_state_store()
            .load_async(&graph_id)
            .await
            .ok();
        services
            .model_step_executor()
            .install_resolver(Arc::new(TurnModelResolver {
                session_id: session_id.clone(),
                runtime: Arc::downgrade(&runtime),
                state: Arc::downgrade(&state),
            }));
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(TurnToolResolver {
                session_id: session_id.clone(),
                runtime: Arc::downgrade(&runtime),
                state: Arc::downgrade(&state),
                services: Arc::downgrade(&services),
            }));
        services
            .agent_task_executor()
            .install_resolver(Arc::new(TurnAgentResolver {
                session_id: session_id.clone(),
                runtime: Arc::downgrade(&runtime),
                services: Arc::downgrade(&services),
            }));
        services
            .synthesize_executor()
            .install_resolver(Arc::new(TurnSynthesizeResolver {
                session_id: session_id.clone(),
                runtime: Arc::downgrade(&runtime),
                state: Arc::downgrade(&state),
                services: Arc::downgrade(&services),
            }));
        let run_result = if persisted_graph.is_some() {
            crate::execution_core::graph::ExecutionGraphRecovery::new(
                services.graph_state_store(),
                services.commit_service(),
                services.executor_registry(),
            )
            .recover(&graph_id)
            .await
            .map_err(|error| RuntimeError::new(error.to_string()))?;
            services.graph_runner().run_until_quiescent(&graph_id).await
        } else {
            services.graph_runner().start(graph).await
        };
        run_result.map_err(|error| RuntimeError::new(error.to_string()))?;
        let mut state = state.lock().await;
        if let Some(error) = state.failure.take() {
            return Err(RuntimeError::new(error));
        }
        let summary = state
            .summary
            .take()
            .ok_or_else(|| RuntimeError::new("execution graph produced no terminal turn result"))?;
        let projection = services
            .graph_runner()
            .projection(&graph_id)
            .await
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if projection.terminal_result_ref.is_none() {
            return Err(RuntimeError::new(
                "execution graph completed without a synthesized terminal result",
            ));
        }
        Ok(summary)
    }
    .await;
    let runtime = Arc::try_unwrap(runtime)
        .unwrap_or_else(|_| panic!("turn executors must release the conversation runtime"))
        .into_inner();
    (runtime, result)
}

struct TurnGraphState {
    content: String,
    prompter: SharedPrompter,
    first_model_step: bool,
    next_calls: Vec<ModelToolCall>,
    next_resource_scopes: Vec<String>,
    assistant_messages: Vec<ConversationMessage>,
    tool_results: Vec<ConversationMessage>,
    prompt_cache_events: Vec<crate::PromptCacheEvent>,
    iterations: usize,
    input_tokens: u64,
    output_tokens: u64,
    wall_duration_ms: u64,
    model: Option<String>,
    summary: Option<TurnSummary>,
    failure: Option<String>,
    pending_transcript: std::collections::BTreeMap<String, Vec<ConversationMessage>>,
    ingress: Option<TurnIngressRef>,
    session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedModelToolCall {
    id: String,
    name: String,
    input: String,
    depends_on: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedToolBatch {
    session_id: String,
    calls: Vec<PersistedModelToolCall>,
}

fn encode_tool_calls(
    session_id: &str,
    calls: &[ModelToolCall],
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&PersistedToolBatch {
        session_id: session_id.to_string(),
        calls: calls
            .iter()
            .map(|call| PersistedModelToolCall {
                id: call.id.clone(),
                name: call.name.clone(),
                input: call.input.clone(),
                depends_on: call.depends_on.clone(),
            })
            .collect(),
    })
}

fn decode_tool_calls(payload: &str) -> Result<Vec<ModelToolCall>, serde_json::Error> {
    serde_json::from_str::<PersistedToolBatch>(payload).map(|batch| {
        batch
            .calls
            .into_iter()
            .map(|call| ModelToolCall {
                id: call.id,
                name: call.name,
                input: call.input,
                depends_on: call.depends_on,
            })
            .collect()
    })
}

fn ticket_session_id(payload: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    value
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            value
                .get("ingress")
                .and_then(|ingress| ingress.get("session_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
}

struct TurnAgentBackend<C: ApiClient, T: ToolExecutor> {
    runtime: Arc<tokio::sync::Mutex<crate::ConversationRuntime<C, T>>>,
    services: Arc<crate::RuntimeServices>,
}

struct TurnModelResolver<C: ApiClient, T: ToolExecutor> {
    session_id: String,
    runtime: std::sync::Weak<tokio::sync::Mutex<crate::ConversationRuntime<C, T>>>,
    state: std::sync::Weak<tokio::sync::Mutex<TurnGraphState>>,
}

impl<C, T> crate::execution_core::graph::executors::ScopedNodeBackendResolver
    for TurnModelResolver<C, T>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        if ticket_session_id(&ticket.payload_ref).as_deref() != Some(&self.session_id) {
            return None;
        }
        Some(Arc::new(TurnModelStepBackend {
            runtime: self.runtime.upgrade()?,
            state: self.state.upgrade()?,
        }))
    }
}

struct TurnToolResolver<C: ApiClient, T: ToolExecutor> {
    session_id: String,
    runtime: std::sync::Weak<tokio::sync::Mutex<crate::ConversationRuntime<C, T>>>,
    state: std::sync::Weak<tokio::sync::Mutex<TurnGraphState>>,
    services: std::sync::Weak<crate::RuntimeServices>,
}

impl<C, T> crate::execution_core::graph::executors::ScopedNodeBackendResolver
    for TurnToolResolver<C, T>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        if ticket_session_id(&ticket.payload_ref).as_deref() != Some(&self.session_id) {
            return None;
        }
        Some(Arc::new(TurnToolBatchBackend {
            runtime: self.runtime.upgrade()?,
            state: self.state.upgrade()?,
            services: self.services.upgrade()?,
        }))
    }
}

struct TurnAgentResolver<C: ApiClient, T: ToolExecutor> {
    session_id: String,
    runtime: std::sync::Weak<tokio::sync::Mutex<crate::ConversationRuntime<C, T>>>,
    services: std::sync::Weak<crate::RuntimeServices>,
}

impl<C, T> crate::execution_core::graph::executors::AgentTaskBackendResolver
    for TurnAgentResolver<C, T>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    fn resolve(
        &self,
        packet: &AgentTaskPacket,
    ) -> Option<Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>> {
        if packet.session_id != self.session_id {
            return None;
        }
        Some(Arc::new(TurnAgentBackend {
            runtime: self.runtime.upgrade()?,
            services: self.services.upgrade()?,
        }))
    }
}

struct TurnSynthesizeResolver<C: ApiClient, T: ToolExecutor> {
    session_id: String,
    runtime: std::sync::Weak<tokio::sync::Mutex<crate::ConversationRuntime<C, T>>>,
    state: std::sync::Weak<tokio::sync::Mutex<TurnGraphState>>,
    services: std::sync::Weak<crate::RuntimeServices>,
}

impl<C, T> crate::execution_core::graph::executors::SynthesizeBackendResolver
    for TurnSynthesizeResolver<C, T>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    fn resolve(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<dyn crate::execution_core::graph::executors::SynthesizeBackend>> {
        if ticket_session_id(&ticket.payload_ref).as_deref() != Some(&self.session_id) {
            return None;
        }
        Some(Arc::new(TurnSynthesizeBackend {
            runtime: self.runtime.upgrade()?,
            state: self.state.upgrade()?,
            services: self.services.upgrade()?,
        }))
    }
}

#[async_trait]
impl<C, T> crate::execution_core::graph::executors::AgentTaskBackend for TurnAgentBackend<C, T>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn execute(&self, packet: AgentTaskPacket) -> Result<AgentReturnPacket, String> {
        let mut config = crate::agent::SubAgentConfig {
            task_description: packet.objective.clone(),
            allowed_tools: packet.allowed_tools.clone(),
            model: Some(packet.model_lease.clone()),
            session_id: Some(packet.session_id.clone()),
            ..crate::agent::SubAgentConfig::default()
        };
        config.ensure_context_lease(&packet.session_id, &packet.agent_id);
        let mut agent = self.runtime.lock().await.create_subagent_runtime(&config);
        let result = agent.run_loop_async(&packet.objective).await;
        let failed = !result.completed_normally;
        let outcome = result.output;
        Ok(AgentReturnPacket {
            run_id: packet.run_id,
            agent_id: packet.agent_id,
            task_id: packet.task_id,
            session_id: packet.session_id,
            mission_id: packet.mission_id,
            team_id: packet.team_id,
            graph_id: packet.graph_id,
            node_id: packet.node_id,
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status: if !failed {
                AgentTerminalStatus::Completed
            } else {
                AgentTerminalStatus::Failed
            },
            outcome,
            acceptance: packet.acceptance,
            evidence_refs: Vec::new(),
            changes: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: result.tokens_used as u64,
            output_tokens: 0,
            model: packet.model_lease.clone(),
            provider: self
                .services
                .provider_registry()
                .pin()
                .provider_name_for_model(&packet.model_lease)
                .unwrap_or_else(|| "environment".to_string()),
            tool_calls: result.tool_call_count as u64,
            failure: failed.then(|| "provider agent did not complete normally".to_string()),
        })
    }
}

struct TurnModelStepBackend<C: ApiClient, T: ToolExecutor> {
    runtime: Arc<tokio::sync::Mutex<crate::ConversationRuntime<C, T>>>,
    state: Arc<tokio::sync::Mutex<TurnGraphState>>,
}

#[async_trait]
impl<C, T> ScopedNodeBackend for TurnModelStepBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let (content, first_step) = {
            let mut state = self.state.lock().await;
            let first = state.first_model_step;
            state.first_model_step = false;
            (state.content.clone(), first)
        };
        let mut runtime = self.runtime.lock().await;
        let transcript_len = runtime.session_async().await.messages.len();
        let result = runtime.execute_model_step(&content, first_step).await;
        rollback_uncommitted_transcript(
            &mut runtime.session_mut_async().await.messages,
            transcript_len,
        );
        drop(runtime);
        match result {
            Ok(step) => {
                let usage = ExecutionUsage {
                    input_tokens: u64::from(step.usage.input_tokens),
                    output_tokens: u64::from(step.usage.output_tokens),
                    cached_tokens: u64::from(step.usage.cache_read_input_tokens)
                        .saturating_add(u64::from(step.usage.cache_creation_input_tokens)),
                    duration_ms: step.wall_duration_ms,
                    tool_calls: 0,
                };
                let mut state = self.state.lock().await;
                state.iterations = state.iterations.saturating_add(1);
                state.input_tokens = state
                    .input_tokens
                    .saturating_add(u64::from(step.usage.input_tokens));
                state.output_tokens = state
                    .output_tokens
                    .saturating_add(u64::from(step.usage.output_tokens));
                state.wall_duration_ms =
                    state.wall_duration_ms.saturating_add(step.wall_duration_ms);
                state.model = step.model;
                state.prompt_cache_events.extend(step.prompt_cache_events);
                state
                    .assistant_messages
                    .push(step.assistant_message.clone());
                let mut committed_messages = Vec::new();
                if first_step {
                    committed_messages.push(ConversationMessage::user_text(content));
                }
                committed_messages.push(step.assistant_message.clone());
                state
                    .pending_transcript
                    .insert(ticket.node_id.clone(), committed_messages);
                let mut committed_result_ref = format!("{}:model-result", ticket.graph_id);
                let next = match step.intent {
                    ModelStepIntent::FinalAnswer { text } => {
                        if text.trim().is_empty() {
                            return Err(NodeExecutorError::Poll {
                                node_id: ticket.node_id.clone(),
                                reason: "model produced an empty FinalAnswer intent".to_string(),
                            });
                        }
                        committed_result_ref = format!(
                            "assistant_json:{}",
                            serde_json::to_string(&text).map_err(|error| {
                                NodeExecutorError::Poll {
                                    node_id: ticket.node_id.clone(),
                                    reason: error.to_string(),
                                }
                            })?
                        );
                        let mut node = dynamic_node(
                            ticket,
                            state.iterations,
                            "synthesize",
                            ExecutionNodeKind::Synthesize,
                            crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
                            "inline_model",
                        );
                        node.executor_kind =
                            crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND
                                .to_string();
                        vec![node]
                    }
                    ModelStepIntent::ToolCalls { calls } => {
                        state.next_resource_scopes = resource_scopes_for_tool_calls(&calls);
                        state.next_calls = calls;
                        let mut tool_node = dynamic_node(
                            ticket,
                            state.iterations,
                            "tools",
                            ExecutionNodeKind::ToolBatch,
                            "tool_batch",
                            "inline_model",
                        );
                        tool_node.payload_ref =
                            encode_tool_calls(&state.session_id, &state.next_calls).map_err(
                                |error| NodeExecutorError::Poll {
                                    node_id: ticket.node_id.clone(),
                                    reason: error.to_string(),
                                },
                            )?;
                        tool_node.resource_scopes = state.next_resource_scopes.clone();
                        vec![
                            tool_node,
                            dynamic_node(
                                ticket,
                                state.iterations,
                                "model",
                                ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "inline_model",
                            ),
                        ]
                    }
                    ModelStepIntent::AgentProposal { calls }
                    | ModelStepIntent::TeamProposal { calls } => {
                        state.next_calls = calls;
                        let mut agent_node = dynamic_node(
                            ticket,
                            state.iterations,
                            "agent-task",
                            ExecutionNodeKind::AgentTask,
                            crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
                            "inline_model",
                        );
                        agent_node.executor_kind =
                            crate::execution_core::graph::executors::AgentTaskExecutor::KIND
                                .to_string();
                        let packet = AgentTaskPacket {
                            run_id: format!("agent-run:{}", agent_node.id),
                            agent_id: format!("agent:{}", agent_node.id),
                            task_id: agent_node.id.clone(),
                            session_id: state.session_id.clone(),
                            mission_id: None,
                            team_id: None,
                            graph_id: ticket.graph_id.clone(),
                            node_id: agent_node.id.clone(),
                            attempt: 1,
                            expected_graph_revision: 0,
                            objective: state.content.clone(),
                            acceptance: agent_node.acceptance.criteria.clone(),
                            constraints: Vec::new(),
                            context_refs: Vec::new(),
                            evidence_refs: Vec::new(),
                            allowed_tools: state
                                .next_calls
                                .iter()
                                .map(|call| call.name.clone())
                                .collect(),
                            allowed_skills: Vec::new(),
                            permission_lease: "turn-permission-lease".to_string(),
                            model_lease: state
                                .model
                                .clone()
                                .unwrap_or_else(|| "active-model".to_string()),
                            budget_lease: harness_contract::context::ContextBudgetLeaseRef::new(
                                format!("budget:{}", agent_node.id),
                                agent_node.id.clone(),
                                "agent_task",
                                0,
                                0,
                            ),
                            idempotency_key: agent_node.idempotency_key.clone(),
                        };
                        agent_node.payload_ref =
                            serde_json::to_string(&packet).map_err(|error| {
                                NodeExecutorError::Poll {
                                    node_id: ticket.node_id.clone(),
                                    reason: error.to_string(),
                                }
                            })?;
                        agent_node.resource_scopes = resource_scopes_for_agent_packet(&packet);
                        vec![
                            agent_node,
                            dynamic_node(
                                ticket,
                                state.iterations,
                                "model",
                                ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "inline_model",
                            ),
                        ]
                    }
                    ModelStepIntent::ApprovalRequired { calls } => {
                        state.next_resource_scopes = resource_scopes_for_tool_calls(&calls);
                        state.next_calls = calls;
                        let mut approval = dynamic_node(
                            ticket,
                            state.iterations,
                            "approval",
                            ExecutionNodeKind::Approval,
                            crate::execution_core::graph::executors::ApprovalNodeExecutor::KIND,
                            "inline_model",
                        );
                        approval.executor_kind =
                            crate::execution_core::graph::executors::ApprovalNodeExecutor::KIND
                                .to_string();
                        approval.payload_ref = serde_json::json!({
                            "action": state.next_calls.iter().map(|call| call.name.as_str()).collect::<Vec<_>>().join(","),
                            "summary": format!("Model requested {} governed call(s)", state.next_calls.len()),
                            "session_id": state.session_id,
                            "evidence_refs": [],
                        }).to_string();
                        let mut tool_node = dynamic_node(
                            ticket,
                            state.iterations,
                            "tools",
                            ExecutionNodeKind::ToolBatch,
                            "tool_batch",
                            "inline_model",
                        );
                        tool_node.payload_ref =
                            encode_tool_calls(&state.session_id, &state.next_calls).map_err(
                                |error| NodeExecutorError::Poll {
                                    node_id: ticket.node_id.clone(),
                                    reason: error.to_string(),
                                },
                            )?;
                        tool_node.resource_scopes = state.next_resource_scopes.clone();
                        vec![
                            approval,
                            tool_node,
                            dynamic_node(
                                ticket,
                                state.iterations,
                                "model",
                                ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "inline_model",
                            ),
                        ]
                    }
                    ModelStepIntent::Replan { reason } => {
                        state.content.push_str("\n\nRuntime replan guidance: ");
                        state.content.push_str(&reason);
                        vec![dynamic_node(
                            ticket,
                            state.iterations,
                            "model",
                            ExecutionNodeKind::InlineModel,
                            "inline_model",
                            "inline_model",
                        )]
                    }
                };
                let edges = dynamic_edges(&ticket.node_id, &next);
                Ok(
                    NodeExecutionOutcome::new(completed_result(Some(committed_result_ref), usage))
                        .with_replan(ExecutionGraphReplan {
                            nodes: next,
                            edges,
                            reason: "provider intent advanced the turn graph".to_string(),
                        }),
                )
            }
            Err(error) => {
                self.state.lock().await.failure = Some(error.to_string());
                Err(NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: error.to_string(),
                })
            }
        }
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let messages = self
            .state
            .lock()
            .await
            .pending_transcript
            .remove(&ticket.node_id)
            .unwrap_or_default();
        self.runtime
            .lock()
            .await
            .session_mut_async()
            .await
            .messages
            .extend(messages);
        Ok(())
    }
}

struct TurnToolBatchBackend<C: ApiClient, T: ToolExecutor> {
    runtime: Arc<tokio::sync::Mutex<crate::ConversationRuntime<C, T>>>,
    state: Arc<tokio::sync::Mutex<TurnGraphState>>,
    services: Arc<crate::RuntimeServices>,
}

#[async_trait]
impl<C, T> ScopedNodeBackend for TurnToolBatchBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let (calls, prompter, iteration) = {
            let mut state = self.state.lock().await;
            (
                std::mem::take(&mut state.next_calls),
                state.prompter.clone(),
                state.iterations,
            )
        };
        let calls = if calls.is_empty() {
            decode_tool_calls(&ticket.payload_ref).map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("tool batch persistent payload is invalid: {error}"),
            })?
        } else {
            calls
        };
        if calls.is_empty() {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "tool batch has no model-requested calls".to_string(),
            });
        }
        let result = if let Some(host) = self.services.tool_execution_host() {
            let messages = calls
                .iter()
                .map(|call| {
                    let outcome = host.execute_runtime_tool(&crate::RuntimeToolExecutionRequest {
                        idempotency_key: format!("{}:{}", ticket.idempotency_key, call.id),
                        tool_use_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        input: call.input.clone(),
                        category: crate::tool_orchestrator::ToolSafetyCategory::from_tool_name(
                            &call.name,
                        ),
                    });
                    ConversationMessage::tool_result(
                        outcome.tool_use_id,
                        outcome.tool_name,
                        outcome.output.or(outcome.error).unwrap_or_default(),
                        outcome.status != crate::RuntimeToolExecutionStatus::Executed,
                    )
                })
                .collect::<Vec<_>>();
            crate::conversation::ToolBatchStepResult {
                failed: messages
                    .iter()
                    .flat_map(|message| &message.blocks)
                    .filter(|block| {
                        matches!(block, ContentBlock::ToolResult { is_error: true, .. })
                    })
                    .count(),
                messages,
            }
        } else {
            let mut runtime = self.runtime.lock().await;
            let transcript_len = runtime.session_async().await.messages.len();
            let result = runtime
                .execute_tool_batch_step(&calls, &prompter, iteration)
                .await;
            // The legacy conversation engine writes tool messages eagerly. Roll them
            // back until the graph transition commits; after_commit publishes them.
            rollback_uncommitted_transcript(
                &mut runtime.session_mut_async().await.messages,
                transcript_len,
            );
            drop(runtime);
            result.map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?
        };
        let tool_calls = result.messages.len() as u64;
        let mut state = self.state.lock().await;
        state
            .pending_transcript
            .insert(ticket.node_id.clone(), result.messages.clone());
        state.tool_results.extend(result.messages);
        Ok(NodeExecutionOutcome::new(completed_result(
            Some(format!("{}:tool-results:{tool_calls}", ticket.graph_id)),
            ExecutionUsage {
                tool_calls,
                ..ExecutionUsage::default()
            },
        )))
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let messages = self
            .state
            .lock()
            .await
            .pending_transcript
            .remove(&ticket.node_id)
            .unwrap_or_default();
        self.runtime
            .lock()
            .await
            .session_mut_async()
            .await
            .messages
            .extend(messages);
        Ok(())
    }
}

struct TurnSynthesizeBackend<C: ApiClient, T: ToolExecutor> {
    runtime: Arc<tokio::sync::Mutex<crate::ConversationRuntime<C, T>>>,
    state: Arc<tokio::sync::Mutex<TurnGraphState>>,
    services: Arc<crate::RuntimeServices>,
}

#[async_trait]
impl<C, T> crate::execution_core::graph::executors::SynthesizeBackend
    for TurnSynthesizeBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let projection = self
            .services
            .graph_runner()
            .projection(&ticket.graph_id)
            .await
            .map_err(|error| error.to_string())?;
        let final_answer = projection
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::InlineModel)
            .filter_map(|node| node.result_ref.as_deref())
            .filter_map(|result_ref| result_ref.strip_prefix("assistant_json:"))
            .last()
            .ok_or_else(|| "synthesize has no committed FinalAnswer result_ref".to_string())
            .and_then(|encoded| {
                serde_json::from_str::<String>(encoded).map_err(|error| error.to_string())
            })?;
        let digest = format!("{:x}", Sha256::digest(final_answer.as_bytes()));
        let ingress = self.state.lock().await.ingress.clone();
        let mut outcome = NodeExecutionOutcome::new(completed_result(
            Some(format!("turn-result:{}:{digest}", ticket.graph_id)),
            ExecutionUsage::default(),
        ));
        if let Some(ingress) = ingress {
            let terminal = crate::runtime_event_store::SessionTerminalInput {
                terminal_id: format!("turn-terminal:{}", ingress.request_id),
                message_id: format!("assistant:{}", ingress.message_id),
                session_id: ingress.session_id,
                payload_ref: format!(
                    "assistant_json:{}",
                    serde_json::to_string(&final_answer).unwrap_or_default()
                ),
            };
            outcome
                .domain_events
                .push(crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: format!("session-terminal:{}", ingress.request_id),
                        scope: crate::RuntimeEventScope::SessionCommand,
                        kind: "runtime.session.terminal_requested".to_string(),
                        status: Some("pending_delivery".to_string()),
                        actor: Some("SynthesizeNodeExecutor".to_string()),
                        refs: vec![crate::RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: ticket.graph_id.clone(),
                        }],
                        payload: serde_json::to_value(&terminal).unwrap_or_default(),
                    },
                    idempotency_key: Some(ticket.idempotency_key.clone()),
                    schema_version: 1,
                });
        }
        Ok(outcome)
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), String> {
        let projection = self
            .services
            .graph_runner()
            .projection(&ticket.graph_id)
            .await
            .map_err(|error| error.to_string())?;
        let final_answer = projection
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::InlineModel)
            .filter_map(|node| node.result_ref.as_deref())
            .filter_map(|result_ref| result_ref.strip_prefix("assistant_json:"))
            .last()
            .ok_or_else(|| "synthesize has no committed FinalAnswer result_ref".to_string())
            .and_then(|encoded| {
                serde_json::from_str::<String>(encoded).map_err(|error| error.to_string())
            })?;
        let (
            content,
            assistant_messages,
            tool_results,
            prompt_cache_events,
            iterations,
            model,
            input_tokens,
            output_tokens,
            wall_duration_ms,
        ) = {
            let mut state = self.state.lock().await;
            (
                state.content.clone(),
                std::mem::take(&mut state.assistant_messages),
                std::mem::take(&mut state.tool_results),
                std::mem::take(&mut state.prompt_cache_events),
                state.iterations,
                state.model.clone(),
                state.input_tokens,
                state.output_tokens,
                state.wall_duration_ms,
            )
        };
        let summary = self
            .runtime
            .lock()
            .await
            .finalize_graph_turn(
                &content,
                final_answer,
                assistant_messages,
                tool_results,
                prompt_cache_events,
                iterations,
                model,
                input_tokens,
                output_tokens,
                wall_duration_ms,
            )
            .await
            .map_err(|error| error.to_string())?;
        self.state.lock().await.summary = Some(summary);
        Ok(())
    }
}

fn dynamic_node(
    ticket: &NodeExecutionTicket,
    iteration: usize,
    suffix: &str,
    kind: ExecutionNodeKind,
    executor_prefix: &str,
    _scoped_model_kind: &str,
) -> ExecutionNodeSpec {
    let id = format!("{}:{iteration}:{suffix}", ticket.graph_id);
    ExecutionNodeSpec {
        id: id.clone(),
        kind,
        payload_ref: ticket.payload_ref.clone(),
        executor_kind: executor_prefix.to_string(),
        idempotency_key: format!("{id}:attempt"),
        lease_ref: None,
        acceptance: Default::default(),
        retry_policy: Default::default(),
        resource_scopes: Vec::new(),
    }
}

/// The legacy conversation engine appends provider/tool messages during the
/// effect call. They are deliberately removed until Runner commits the node;
/// `after_commit` is the only publisher to the parent transcript.
fn rollback_uncommitted_transcript(messages: &mut Vec<ConversationMessage>, committed_len: usize) {
    messages.truncate(committed_len);
}

fn resource_scopes_for_tool_calls(calls: &[ModelToolCall]) -> Vec<String> {
    use crate::tool_dispatch::ToolRequest;
    let requests = calls
        .iter()
        .map(|call| ToolRequest {
            tool_use_id: call.id.clone(),
            tool_name: call.name.clone(),
            input: call.input.clone(),
            depends_on: call.depends_on.clone(),
        })
        .collect::<Vec<_>>();
    let plan = crate::tool_execution_plan::ToolExecutionPlan::from_requests(&requests);
    let mut scopes = Vec::new();
    for task in plan.tasks {
        let access = if task.purity == crate::tool_execution_plan::ToolPurity::ReadOnlyIdempotent {
            "read"
        } else {
            "write"
        };
        scopes.extend(
            task.resource_scope
                .paths
                .into_iter()
                .map(|path| format!("{access}:{path}")),
        );
        if task.resource_scope.network {
            scopes.push("network:*".to_string());
        }
        if task.resource_scope.unknown {
            scopes.push("write:.".to_string());
        }
    }
    let mut paths = std::collections::BTreeMap::<String, bool>::new();
    let mut other = Vec::new();
    for scope in scopes {
        if let Some(path) = scope.strip_prefix("write:") {
            paths.insert(path.to_string(), true);
        } else if let Some(path) = scope.strip_prefix("read:") {
            paths.entry(path.to_string()).or_insert(false);
        } else {
            other.push(scope);
        }
    }
    other.extend(
        paths
            .into_iter()
            .map(|(path, write)| format!("{}:{path}", if write { "write" } else { "read" })),
    );
    other.sort();
    other.dedup();
    other
}

fn resource_scopes_for_agent_packet(packet: &AgentTaskPacket) -> Vec<String> {
    let mut scopes = vec![format!("session:{}", packet.session_id)];
    scopes.extend(
        packet
            .constraints
            .iter()
            .filter_map(|constraint| constraint.strip_prefix("resource:").map(str::to_owned)),
    );
    if packet
        .constraints
        .iter()
        .any(|constraint| constraint == "worktree_isolation")
    {
        scopes.push("worktree:.".to_string());
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

fn dynamic_edges(from: &str, nodes: &[ExecutionNodeSpec]) -> Vec<ExecutionEdge> {
    let mut previous = from.to_string();
    nodes
        .iter()
        .map(|node| {
            let edge = ExecutionEdge {
                from: previous.clone(),
                to: node.id.clone(),
                kind: ExecutionEdgeKind::DependsOn,
            };
            previous.clone_from(&node.id);
            edge
        })
        .collect()
}

fn completed_result(result_ref: Option<String>, usage: ExecutionUsage) -> ExecutionNodeResult {
    ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref,
        evidence_refs: Vec::new(),
        failure: None,
        usage,
        finished_at_ms: crate::tool_invocation::now_ms(),
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use futures::stream::{self, Stream};

    use super::*;
    use crate::conversation::{ApiRequest, AssistantEvent, ToolError};

    #[derive(Clone)]
    struct FinalAnswerClient;

    impl ApiClient for FinalAnswerClient {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(stream::iter(vec![
                Ok(AssistantEvent::TextDelta("terminal answer".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]))
        }
    }

    struct NoopToolExecutor;

    impl ToolExecutor for NoopToolExecutor {
        fn execute(&self, name: &str, _input: &str) -> Result<String, ToolError> {
            Err(ToolError::new(format!("unexpected tool call: {name}")))
        }
    }

    #[derive(Clone)]
    struct TwoToolClient {
        requests: usize,
        executed: Arc<AtomicUsize>,
        executions_seen_before_second_model: Arc<AtomicUsize>,
    }

    impl ApiClient for TwoToolClient {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.requests += 1;
            if self.requests == 1 {
                assert_eq!(self.executed.load(Ordering::SeqCst), 0);
                Box::pin(stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "read-1".to_string(),
                        name: "read_file".to_string(),
                        input: r#"{"path":"src/lib.rs"}"#.to_string(),
                    }),
                    Ok(AssistantEvent::ToolUse {
                        id: "write-1".to_string(),
                        name: "write_file".to_string(),
                        input: r#"{"path":"src/lib.rs","content":"updated"}"#.to_string(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]))
            } else {
                self.executions_seen_before_second_model
                    .store(self.executed.load(Ordering::SeqCst), Ordering::SeqCst);
                Box::pin(stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("done once".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }
    }

    struct RecordingToolExecutor {
        executed: Arc<AtomicUsize>,
        order: Arc<Mutex<Vec<String>>>,
    }

    impl ToolExecutor for RecordingToolExecutor {
        fn execute(&self, name: &str, _input: &str) -> Result<String, ToolError> {
            self.order.lock().unwrap().push(name.to_string());
            self.executed.fetch_add(1, Ordering::SeqCst);
            Ok(format!("{name} complete"))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec!["read_file".to_string(), "write_file".to_string()]
        }

        fn classify_tool_safety(
            &self,
            name: &str,
            _input: &str,
        ) -> Option<crate::tool_orchestrator::ToolSafetyCategory> {
            Some(if name == "write_file" {
                crate::tool_orchestrator::ToolSafetyCategory::WriteLocal
            } else {
                crate::tool_orchestrator::ToolSafetyCategory::ReadOnly
            })
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn owned_turn_runs_through_graph_and_returns_synthesized_result() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let runtime = crate::ConversationRuntime::new(
            Session::new(),
            FinalAnswerClient,
            NoopToolExecutor,
            PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
            vec!["answer directly".to_string()],
        )
        .without_memory();

        let (_runtime, result) = submit_owned_conversation_turn(
            runtime,
            Arc::clone(&services),
            "answer once",
            &SharedPrompter::none(),
        )
        .await;
        let summary = result.expect("turn result");

        assert_eq!(summary.final_answer, "terminal answer");
        let events = services.event_store().all_events(100).expect("events");
        assert!(events
            .iter()
            .any(|event| event.kind == "execution_graph.planned"));
        assert!(events.iter().any(|event| {
            event.kind == "execution_graph.node_transitioned"
                && event.payload.to_string().contains("turn-result:")
        }));
        assert_eq!(
            events
                .iter()
                .filter_map(|event| {
                    serde_json::from_value::<crate::execution_core::graph::ExecutionGraphEvent>(
                        event.payload.clone(),
                    )
                    .ok()
                })
                .filter(|event| matches!(
                    event,
                    crate::execution_core::graph::ExecutionGraphEvent::NodeTransitioned {
                        result: Some(result),
                        ..
                    } | crate::execution_core::graph::ExecutionGraphEvent::NodeTransitionedAndReplanned {
                        result,
                        ..
                    } if result
                        .result_ref
                        .as_deref()
                        .is_some_and(|value| value.contains("assistant_json:")
                            && value.contains("terminal answer"))
                ))
                .count(),
            1,
            "FinalAnswer must be committed exactly once before Synthesize"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn model_step_only_plans_then_runner_executes_dependent_tool_wave_once() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let executed = Arc::new(AtomicUsize::new(0));
        let executions_seen_before_second_model = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(Mutex::new(Vec::new()));
        let runtime = crate::ConversationRuntime::new(
            Session::new(),
            TwoToolClient {
                requests: 0,
                executed: Arc::clone(&executed),
                executions_seen_before_second_model: Arc::clone(
                    &executions_seen_before_second_model,
                ),
            },
            RecordingToolExecutor {
                executed: Arc::clone(&executed),
                order: Arc::clone(&order),
            },
            PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
            vec!["use requested tools".to_string()],
        )
        .without_memory();

        let (_runtime, result) = submit_owned_conversation_turn(
            runtime,
            Arc::clone(&services),
            "read then update src/lib.rs",
            &SharedPrompter::none(),
        )
        .await;
        let summary = result.expect("turn result");

        assert_eq!(executed.load(Ordering::SeqCst), 2);
        assert_eq!(
            executions_seen_before_second_model.load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            order.lock().unwrap().as_slice(),
            ["read_file", "write_file"]
        );
        assert_eq!(summary.tool_results.len(), 2);
        assert_eq!(summary.final_answer, "done once");
        assert_eq!(
            summary
                .assistant_messages
                .iter()
                .flat_map(|message| message.blocks.iter())
                .filter(|block| matches!(block, crate::ContentBlock::Text { text } if text == "done once"))
                .count(),
            1
        );
        let events = services.event_store().all_events(200).expect("events");
        assert!(events
            .iter()
            .any(|event| event.kind == "execution_graph.node_transitioned_and_replanned"));
    }

    #[test]
    fn dynamic_tool_nodes_preserve_file_resource_scopes() {
        let same_file = resource_scopes_for_tool_calls(&[
            ModelToolCall {
                id: "write-a".into(),
                name: "write_file".into(),
                input: r#"{"path":"src/lib.rs","content":"a"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "write-b".into(),
                name: "edit_file".into(),
                input: r#"{"path":"src/lib.rs","old_string":"a","new_string":"b"}"#.into(),
                depends_on: Vec::new(),
            },
        ]);
        assert_eq!(same_file, vec!["write:src/lib.rs"]);

        let different_files = resource_scopes_for_tool_calls(&[
            ModelToolCall {
                id: "write-a".into(),
                name: "write_file".into(),
                input: r#"{"path":"src/a.rs","content":"a"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "write-b".into(),
                name: "write_file".into(),
                input: r#"{"path":"src/b.rs","content":"b"}"#.into(),
                depends_on: Vec::new(),
            },
        ]);
        assert_eq!(different_files, vec!["write:src/a.rs", "write:src/b.rs"]);
    }

    #[test]
    fn uncommitted_transcript_entries_are_rolled_back_to_commit_boundary() {
        let mut messages = vec![
            ConversationMessage::user_text("committed"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "provider effect".to_string(),
            }]),
            ConversationMessage::tool_result("tool", "write", "done", false),
        ];
        rollback_uncommitted_transcript(&mut messages, 1);
        assert_eq!(messages, vec![ConversationMessage::user_text("committed")]);
    }
}
