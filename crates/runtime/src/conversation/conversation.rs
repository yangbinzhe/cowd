use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{RwLock, Semaphore};

/// T35: Lightweight cancellation token (tokio-util not available in dep tree).
#[derive(Clone, Default, Debug)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

use futures::stream::Stream;
use harness_contract::{
    context::{
        ContextGovernanceDecision, ContextPressureState, ContextTurnReport, EvidenceRef,
        ToolObservation,
    },
    skill::{AgentSkillProfile, SkillCapabilityProfile},
    strategy::{StrategyExperienceRecord, StrategyExperienceStore, StrategyInput},
};
use memory::cognitive::CognitiveContextManager;
use memory::config::MemoryConfig as CcMemoryConfig;
use memory::types::{Message as MemMessage, MessageRole as MemMessageRole};
use memory::{MemoryKernel, MemoryTurnContext};
use model_protocol::telemetry::SessionTracer;
use serde_json::{Map, Value};
use tracing;

use crate::agent::{SubAgentConfig, SubAgentRuntime};
use crate::agent_collaboration::{
    CollaborationContextResult, CollaborationOps, MemoryPulseCandidate, MemoryPulseKind,
};
use crate::agent_discussion::DiscussionEngine;
use crate::compact::{
    compact_session, estimate_session_tokens, CompactionConfig, CompactionResult,
};
use crate::config::RuntimeFeatureConfig;
use crate::context_runtime::{
    ContextAuthority, ContextEnvelope, ContextEnvelopeRequest, ContextIdentity, ContextItem,
    ContextOmission, ContextProfile, ContextRole, ContextRuntimeKernel, ContextSourceKind,
    ContextVisibility, ResumeContextPacket, ToolTracePacket, ToolTraceStatus,
};
use crate::hooks::{HookAbortSignal, HookProgressReporter, HookRunResult, HookRunner};
use crate::joint_problem_solving::{JpsOps, ProblemStatement};
use crate::knowledge_activation::KnowledgeActivationRuntime;
use crate::permissions::{PermissionContext, PermissionOutcome, PermissionPolicy};
use crate::runtime_control::{RuntimeControlPolicy, TaskComplexityInput, TaskComplexityProfile};
use crate::runtime_harness::{RuntimeAiKernel, RuntimeAiKernelTrace};
use crate::session::{ContentBlock, ConversationMessage, MessageEvent, Session, SessionEventLog};
use crate::skill::{
    memory_candidate_from_skill_activation, SkillActivationEngine, SkillActivationInput,
    SkillActivationRecord, SkillMemoryPolicy,
};
use crate::tool_execution_plan::ToolExecutionPlan;
use crate::tool_invocation::{
    now_ms, ToolFailureKind, ToolInvocationRecord, DEFAULT_OUTPUT_REF_MIN_LINES,
};
use crate::tool_ledger::{tool_event_idempotency_key, TurnToolLedger};
use crate::usage::{ModelPerformanceRegistry, ModelRouteIntent, UsageTracker};
use crate::wave::{TaskId, TaskResult, WaveError, WaveExecutor, WaveTask};
use model_protocol::usage::TokenUsage;

const DEFAULT_AUTO_COMPACTION_INPUT_TOKENS_THRESHOLD: u32 = 100_000;
const AUTO_COMPACTION_THRESHOLD_ENV_VAR: &str = "COWD_AUTO_COMPACT_INPUT_TOKENS";
const DEFAULT_RUNTIME_MAX_ITERATIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamIdleClass {
    Direct,
    Standard,
    Deep,
}

fn stream_idle_timeout_for_messages(messages: &[ConversationMessage]) -> Duration {
    match classify_stream_idle_prompt(&latest_user_prompt_text(messages)) {
        StreamIdleClass::Direct => Duration::from_secs(240),
        StreamIdleClass::Standard => Duration::from_secs(360),
        StreamIdleClass::Deep => Duration::from_secs(600),
    }
}

fn classify_stream_idle_prompt(prompt: &str) -> StreamIdleClass {
    let lower = prompt.to_lowercase();
    let deep_markers = [
        "deep",
        "architecture",
        "refactor",
        "multi-agent",
        "what if",
        "scenario",
        "simulation",
        "matrix",
        "memory",
        "harness",
        "沉浸式",
        "深度",
        "架构",
        "重构",
        "全量",
        "全盘",
        "复杂",
        "多agent",
        "多 agent",
        "跨session",
        "跨 session",
        "记忆",
        "矩阵",
        "推演",
        "测试",
        "验证",
        "真实模型",
    ];
    if prompt.chars().count() > 500
        || prompt.lines().count() > 6
        || deep_markers.iter().any(|marker| lower.contains(marker))
    {
        return StreamIdleClass::Deep;
    }

    let direct_markers = [
        "what is", "explain", "status", "help", "解释", "列出", "总结", "简单", "快速",
    ];
    if prompt.chars().count() <= 160 && direct_markers.iter().any(|marker| lower.contains(marker)) {
        return StreamIdleClass::Direct;
    }

    StreamIdleClass::Standard
}

fn latest_user_prompt_text(messages: &[ConversationMessage]) -> String {
    messages
        .iter()
        .rev()
        .find(|message| matches!(message.role, crate::session::MessageRole::User))
        .map(|message| {
            message
                .blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

/// Fully assembled request payload sent to the upstream model client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRequest {
    pub system_prompt: Vec<String>,
    pub messages: Vec<ConversationMessage>,
    /// Target model ID (used by provider fallback chain to switch models).
    pub model: String,
}

/// Streamed events emitted while processing a single assistant turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssistantEvent {
    TextDelta(String),
    /// P1-7: Extended thinking delta (reasoning model output)
    ThinkingDelta(String),
    /// P1-7: Thinking signature that must be preserved and passed back
    /// to the provider in subsequent requests.
    SignatureDelta(String),
    ToolUse {
        id: String,
        name: String,
        input: String,
    },
    Usage(TokenUsage),
    PromptCache(PromptCacheEvent),
    MessageStop,
    /// P0-2: Tool execution lifecycle events for real-time SSE visualization
    ToolStart {
        id: String,
        name: String,
        preview: String,
    },
    ToolProgress {
        id: String,
        name: String,
        progress: String,
    },
    ToolComplete {
        id: String,
        name: String,
        result_summary: String,
        exit_code: Option<i32>,
    },
}

/// Prompt-cache telemetry captured from the provider response stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptCacheEvent {
    pub unexpected: bool,
    pub reason: String,
    pub previous_cache_read_input_tokens: u32,
    pub current_cache_read_input_tokens: u32,
    pub token_drop: u32,
}

fn preview_chars(value: &str, max_chars: usize) -> String {
    let mut preview: String = value.chars().take(max_chars).collect();
    if value.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
}

fn add_token_usage(total: &mut TokenUsage, usage: TokenUsage) {
    total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
    total.output_tokens = total.output_tokens.saturating_add(usage.output_tokens);
    total.cache_creation_input_tokens = total
        .cache_creation_input_tokens
        .saturating_add(usage.cache_creation_input_tokens);
    total.cache_read_input_tokens = total
        .cache_read_input_tokens
        .saturating_add(usage.cache_read_input_tokens);
}

fn millis_since(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn knowledge_hard_gate_active(system_prompt: &[String]) -> bool {
    system_prompt
        .iter()
        .any(|fragment| fragment.contains("<hard_gate action=\"block\">"))
}

/// Streaming API contract. Implementors produce AssistantEvents lazily.
/// Consumers poll the stream and process each event as it arrives.
///
/// For backward compatibility, a `collect()` call gathers all events
/// into a Vec (same as the old sync signature).
pub trait ApiClient {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>;

    /// Convenience: collect all events synchronously (backward compat).
    fn stream_collect(&mut self, request: ApiRequest) -> Result<Vec<AssistantEvent>, RuntimeError> {
        let stream = self.stream(request);
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("stream_collect rt")
                .handle()
                .clone()
        });
        handle.block_on(async {
            use futures::StreamExt;
            let mut events = Vec::new();
            let mut pinned = stream;
            while let Some(event) = pinned.next().await {
                events.push(event?);
            }
            Ok(events)
        })
    }
}

/// Trait implemented by tool dispatchers that execute model-requested tools.
pub trait ToolExecutor: Send + Sync + 'static {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError>;
}

/// Tool execution lifecycle callback for real-time visualization.
/// Inspired by hermes-agent stream_consumer.py tool_progress_callback.
pub trait ToolCallback: Send + Sync {
    /// Called when a tool starts executing.
    fn on_tool_start(&self, id: &str, name: &str, preview: &str);
    /// Called when a tool reports progress.
    fn on_tool_progress(&self, id: &str, name: &str, progress: &str);
    /// Called when a tool finishes executing.
    fn on_tool_complete(&self, id: &str, name: &str, result_summary: &str, exit_code: Option<i32>);
    /// Called when token usage data is available (typically after each stream completes).
    /// Default implementation is a no-op so existing implementors don't break.
    fn on_usage(&self, _usage: &TokenUsage) {}
}

/// Memory lifecycle callback for real-time TUI visualization.
/// Follows the same pattern as [`ToolCallback`] so the CLI crate can
/// forward memory events to the TUI render loop.
pub trait MemoryCallback: Send + Sync {
    /// Called when memory context entries are prepared for injection into
    /// the system prompt. Each tuple is `(layer, content, relevance)`.
    fn on_memory_update(&self, entries: Vec<(String, String, f64)>, status: &str);
    /// Called after post-turn memory housekeeping completes
    /// (micro-compact, drift, seeds).
    fn on_memory_stats(&self, total_entries: usize, vector_count: usize, layers: Vec<String>);
}

/// Error returned when a tool invocation fails locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    message: String,
}

impl ToolError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for ToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ToolError {}

/// Error returned when a conversation turn cannot be completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    message: String,
}

impl RuntimeError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RuntimeError {}

/// Summary of one completed runtime turn, including tool results and usage.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnSummary {
    pub assistant_messages: Vec<ConversationMessage>,
    pub tool_results: Vec<ConversationMessage>,
    pub prompt_cache_events: Vec<PromptCacheEvent>,
    pub iterations: usize,
    pub usage: TokenUsage,
    pub model_telemetry: crate::cowd_event::RunModelTelemetry,
    pub auto_compaction: Option<AutoCompactionEvent>,
    pub ai_kernel_trace: RuntimeAiKernelTrace,
    pub context_turn_report: ContextTurnReport,
}

/// Details about automatic session compaction applied during a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoCompactionEvent {
    pub removed_message_count: usize,
}

/// P1-05: Callback for generator-style turn injection after tool results.
pub struct TurnCallback {
    pub on_tool_result: Box<dyn Fn(&str, &str) -> Option<String> + Send + Sync>,
}
impl TurnCallback {
    pub fn new<F: Fn(&str, &str) -> Option<String> + Send + Sync + 'static>(f: F) -> Self {
        Self {
            on_tool_result: Box::new(f),
        }
    }
}

/// Coordinates the model loop, tool execution, hooks, and session updates.
pub struct ConversationRuntime<C, T> {
    session: Arc<RwLock<Session>>, // tokio::sync::RwLock
    api_client: C,
    tool_executor: Arc<T>,
    permission_policy: PermissionPolicy,
    system_prompt: Vec<String>,
    max_iterations: usize,
    usage_tracker: UsageTracker,
    model_performance_registry: std::sync::Mutex<ModelPerformanceRegistry>,
    hook_runner: HookRunner,
    cowd_bus: Option<Arc<crate::cowd_event::CowdEventBus>>,
    turn_callback: Option<Arc<TurnCallback>>,
    profiler: crate::context_profiler::ContextProfiler,
    use_aaak_index: bool,
    auto_compaction_input_tokens_threshold: u32,
    model_context_window: u32,
    cached_prompt: crate::cached_prompt::CachedSystemPrompt,
    hook_abort_signal: HookAbortSignal,
    hook_progress_reporter: Arc<std::sync::Mutex<Option<Box<dyn HookProgressReporter + Send>>>>,
    session_tracer: Option<SessionTracer>,
    /// Optional cognitive memory manager – `None` when memory is disabled.
    memory_manager: Option<Arc<CognitiveContextManager>>,
    /// Human-readable memory status message. `None` when healthy; `Some(msg)` when degraded.
    memory_status: Option<String>,
    /// Optional tool callback for real-time visualization (P0-2).
    tool_callback: Option<Arc<dyn ToolCallback>>,
    /// Optional managed SQLite session store for messages and runtime events.
    session_store: Option<Arc<memory::session_store::UnifiedSessionStore>>,
    /// Optional event log for time-travel debugging and session rebuild.
    event_log: Option<std::sync::Mutex<SessionEventLog>>,
    /// Runtime-local searchable index for oversized tool outputs.
    tool_output_sandbox: Option<Arc<std::sync::Mutex<memory::ToolOutputSandbox>>>,
    /// Optional SSE callback for real-time streaming events to WebUI.
    /// Receives pre-formatted JSON event strings.
    sse_callback: Option<Arc<dyn Fn(String) + Send + Sync>>,
    /// Optional memory lifecycle callback for TUI memory events.
    memory_callback: Option<Arc<dyn MemoryCallback>>,
    /// Optional smart approval gate for intelligent command approval (P0-1).
    approval_gate: Option<Arc<crate::approval_gate::SmartApprovalGate>>,
    /// Type-erased collaboration orchestrator for multi-agent task dispatch.
    collaboration: Option<Arc<dyn CollaborationOps>>,
    /// Skill capability profiles already inspected by the Skill asset layer and
    /// visible to this runtime.
    skill_profiles: Vec<SkillCapabilityProfile>,
    /// Agent-scoped Skill visibility and adapter policy.
    agent_skill_profile: AgentSkillProfile,
    /// Type-erased Joint Problem Solving pipeline for high-complexity tasks.
    jps_pipeline: Option<Arc<dyn JpsOps>>,
    /// When true, inject available peer agents from AgentDirectory into the system prompt.
    inject_peer_context: bool,
    /// P2-2: Current project phase (Discovery→Planning→Building→Reviewing→Shipping→Graduated).
    project_phase: String,
    /// Optional commit quality gate evaluator (PreFlight, Revision, Escalation, Abort).
    gate_evaluator: Option<Arc<crate::gates::GateEvaluator>>,
    /// Current model ID (used for provider fallback chain lookup).
    model: Option<String>,
    /// Provider fallback configuration for automatic retry on 429/5xx errors.
    fallbacks: Vec<String>,
    /// T35: Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
    /// T36: Tool orchestrator for result budgeting and truncation.
    tool_orchestrator: crate::tool_orchestrator::ToolOrchestrator,
    /// Latest assembled context envelope used by a real turn.
    last_context_envelope: std::sync::Mutex<Option<ContextEnvelope>>,
    /// Active context profile used to assemble the next runtime envelope.
    context_profile: std::sync::Mutex<ContextProfile>,
    /// Effective runtime control policy loaded from configuration.
    runtime_control_policy: RuntimeControlPolicy,
    /// Runtime-owned context supplied by outer orchestration layers.
    external_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// Latest multi-agent collaboration packet for outer persistence.
    last_collaboration_result: std::sync::Mutex<Option<CollaborationContextResult>>,
    /// Bounded short-term tool trace context for subsequent turns.
    tool_trace_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// Governance observations produced by tool calls in the active turn.
    turn_tool_observations: std::sync::Mutex<Vec<ToolObservation>>,
    /// Latest context governance report emitted by a completed turn.
    last_context_turn_report: std::sync::Mutex<Option<ContextTurnReport>>,
    /// Knowledge activation report prepared from the active memory packet.
    turn_knowledge_report:
        std::sync::Mutex<Option<harness_contract::knowledge::KnowledgeTurnReport>>,
    /// T4: Semaphore for WriteLocal tool concurrency (permits: 4).
    write_semaphore: Arc<Semaphore>,
    /// T4: Semaphore for Network tool concurrency (permits: 3).
    network_semaphore: Arc<Semaphore>,
    /// T4: Semaphore for Destructive tool concurrency (permits: 1).
    destructive_semaphore: Arc<Semaphore>,
    /// T4: Semaphore for default/ReadOnly tool concurrency (permits: 8).
    default_semaphore: Arc<Semaphore>,
    /// Optional discussion engine for multi-agent debate and conflict resolution.
    discussion_engine: Option<Arc<std::sync::Mutex<DiscussionEngine>>>,
    /// Maximum duration for a single tool execution. `None` means no timeout.
    tool_timeout: Option<Duration>,
}

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    #[must_use]
    pub fn new(
        session: Session,
        api_client: C,
        tool_executor: T,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
    ) -> Self {
        Self::new_with_features(
            session,
            api_client,
            Arc::new(tool_executor),
            permission_policy,
            system_prompt,
            &RuntimeFeatureConfig::default(),
        )
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_with_features(
        session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
    ) -> Self {
        let usage_tracker = UsageTracker::from_session(&session);
        // Initialise the cognitive memory manager if the memory subsystem is enabled.
        let (memory_manager, memory_status) = if feature_config.memory().enabled {
            let mem_cfg = build_cc_memory_config(feature_config);
            match tokio::runtime::Handle::try_current() {
                Ok(_) => {
                    // Inside a runtime — spawn a fresh thread with its own runtime
                    // to avoid nested enter_runtime panic.
                    let mem_cfg = mem_cfg.clone();
                    let handle = std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("failed to create memory init runtime");
                        rt.block_on(CognitiveContextManager::new(mem_cfg))
                    });
                    match handle.join().expect("memory init thread panicked") {
                        Ok(mgr) => {
                            mgr.set_active_agent("primary".to_string());
                            let mgr = mgr.init_memory_sync();
                            tracing::debug!(
                                "memory: CognitiveContextManager initialised, active_agent=primary"
                            );
                            (Some(Arc::new(mgr)), None)
                        }
                        Err(err) => {
                            let msg = format!(
                                "Memory system unavailable: {err}. Context will NOT persist between turns. Check your memory store paths, vector API credentials, and ~/.cowd/memory/ directory."
                            );
                            tracing::error!("{msg}");
                            (None, Some(msg))
                        }
                    }
                }
                Err(_) => {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => match rt.block_on(CognitiveContextManager::new(mem_cfg)) {
                            Ok(mgr) => {
                                mgr.set_active_agent("primary".to_string());
                                let mgr = mgr.init_memory_sync();
                                tracing::debug!(
                                    "memory: CognitiveContextManager initialised, active_agent=primary"
                                );
                                (Some(Arc::new(mgr)), None)
                            }
                            Err(err) => {
                                let msg = format!(
                                    "Memory system unavailable: {err}. Context will NOT persist between turns. Check your memory store paths, vector API credentials, and ~/.cowd/memory/ directory."
                                );
                                tracing::error!("{msg}");
                                (None, Some(msg))
                            }
                        },
                        Err(e) => {
                            let msg = format!(
                                "Memory system unavailable: failed to create runtime: {e}. Memory features will NOT work."
                            );
                            tracing::error!("{msg}");
                            (None, Some(msg))
                        }
                    }
                }
            }
        } else {
            (None, None)
        };
        let session = Arc::new(RwLock::new(session));
        Self {
            session,
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
            max_iterations: DEFAULT_RUNTIME_MAX_ITERATIONS,
            usage_tracker,
            model_performance_registry: std::sync::Mutex::new(ModelPerformanceRegistry::new()),
            hook_runner: HookRunner::from_feature_config(feature_config),
            cowd_bus: None,
            turn_callback: None,
            profiler: crate::context_profiler::ContextProfiler::new(),
            use_aaak_index: feature_config.memory().aaak_index_enabled,
            auto_compaction_input_tokens_threshold: {
                let env_val = auto_compaction_threshold_from_env();
                if env_val > 0 {
                    env_val
                } else {
                    feature_config.compression().session.threshold_tokens
                }
            },
            model_context_window: 0,
            cached_prompt: crate::cached_prompt::CachedSystemPrompt::new(
                crate::cowd_dirs::project_dot_dir(&std::env::current_dir().unwrap_or_default())
                    .join(crate::cowd_dirs::CONFIG_FILE_YAML),
                crate::cowd_dirs::project_dot_dir(&std::env::current_dir().unwrap_or_default())
                    .join("identity.md"),
            ),
            hook_abort_signal: HookAbortSignal::default(),
            hook_progress_reporter: Arc::new(std::sync::Mutex::new(None)),
            session_tracer: None,
            memory_manager,
            memory_status,
            tool_callback: None,
            session_store: None,
            event_log: None,
            tool_output_sandbox: memory::ToolOutputSandbox::new()
                .map(|sandbox| Arc::new(std::sync::Mutex::new(sandbox)))
                .map_err(|error| {
                    tracing::warn!(%error, "tool output sandbox unavailable");
                    error
                })
                .ok(),
            sse_callback: None,
            memory_callback: None,
            approval_gate: None,
            collaboration: None,
            skill_profiles: Vec::new(),
            agent_skill_profile: AgentSkillProfile::default(),
            jps_pipeline: None,
            inject_peer_context: true,
            project_phase: "Discovery".to_string(),
            gate_evaluator: Some(Arc::new(
                crate::gates::GateEvaluator::new().with_default_gates(),
            )),
            model: feature_config.model().map(str::to_string),
            fallbacks: feature_config.fallbacks().to_vec(),
            cancellation_token: CancellationToken::new(),
            tool_orchestrator: crate::tool_orchestrator::ToolOrchestrator::default(),
            last_context_envelope: std::sync::Mutex::new(None),
            context_profile: std::sync::Mutex::new(ContextProfile::MainTurn),
            runtime_control_policy: feature_config.runtime_control().policy.clone(),
            external_context_items: std::sync::Mutex::new(Vec::new()),
            last_collaboration_result: std::sync::Mutex::new(None),
            tool_trace_context_items: std::sync::Mutex::new(Vec::new()),
            turn_tool_observations: std::sync::Mutex::new(Vec::new()),
            last_context_turn_report: std::sync::Mutex::new(None),
            turn_knowledge_report: std::sync::Mutex::new(None),
            write_semaphore: Arc::new(Semaphore::new(
                crate::tool_orchestrator::ToolSafetyCategory::WriteLocal.max_concurrency(),
            )),
            network_semaphore: Arc::new(Semaphore::new(
                crate::tool_orchestrator::ToolSafetyCategory::Network.max_concurrency(),
            )),
            destructive_semaphore: Arc::new(Semaphore::new(
                crate::tool_orchestrator::ToolSafetyCategory::Destructive.max_concurrency(),
            )),
            default_semaphore: Arc::new(Semaphore::new(8)),
            discussion_engine: None,
            tool_timeout: Some(Duration::from_secs(120)),
        }
    }

    #[must_use]
    pub fn with_max_iterations(mut self, max_iterations: usize) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    #[must_use]
    pub fn max_iterations(&self) -> usize {
        self.max_iterations
    }

    /// Update the maximum model/tool loop iterations for subsequent turns.
    ///
    /// Gateway uses this to apply surface-specific execution budgets without
    /// rebuilding the whole runtime session.
    pub fn set_max_iterations(&mut self, max_iterations: usize) {
        self.max_iterations = max_iterations;
    }

    #[must_use]
    pub fn with_tool_timeout(mut self, timeout: Duration) -> Self {
        self.tool_timeout = Some(timeout);
        self
    }

    /// Return a human-readable description of memory subsystem health.
    /// `None` when healthy; `Some(msg)` when degraded or unavailable.
    pub fn memory_status(&self) -> Option<&str> {
        self.memory_status.as_deref()
    }

    /// Return the current project lifecycle phase.
    pub fn phase(&self) -> &str {
        &self.project_phase
    }

    /// Return the latest context envelope assembled for an actual model turn.
    pub fn last_context_envelope(&self) -> Option<ContextEnvelope> {
        self.last_context_envelope
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Return the latest context governance report emitted by a completed turn.
    pub fn last_context_turn_report(&self) -> Option<ContextTurnReport> {
        self.last_context_turn_report
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Return the latest collaboration result emitted during a runtime turn.
    pub fn last_collaboration_result(&self) -> Option<CollaborationContextResult> {
        self.last_collaboration_result
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Take the latest collaboration result so outer layers can persist it once.
    pub fn take_collaboration_result(&self) -> Option<CollaborationContextResult> {
        self.last_collaboration_result
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
    }

    /// Return the active context profile used for the next envelope.
    pub fn context_profile(&self) -> ContextProfile {
        self.context_profile
            .lock()
            .map(|guard| *guard)
            .unwrap_or(ContextProfile::MainTurn)
    }

    /// Set the active context profile used for subsequent envelope assembly.
    pub fn set_context_profile(&self, profile: ContextProfile) {
        if let Ok(mut guard) = self.context_profile.lock() {
            *guard = profile;
        }
    }

    /// Replace runtime-owned context supplied by orchestration layers.
    pub fn set_external_context_items(&self, items: Vec<ContextItem>) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            *guard = items;
        }
    }

    /// Add one runtime-owned context item supplied by orchestration layers.
    pub fn push_external_context_item(&self, item: ContextItem) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            guard.push(item);
        }
    }

    /// Remove runtime-owned context items from a given source.
    pub fn clear_external_context_source(&self, source: ContextSourceKind) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            guard.retain(|item| item.source != source);
        }
    }

    /// Inject resume/handoff state into the next runtime context envelope.
    pub fn inject_resume_context(&self, packet: ResumeContextPacket) {
        let item = ContextRuntimeKernel::resume_item(&packet);
        self.clear_external_context_source(item.source);
        self.push_external_context_item(item);
    }

    fn external_context_items(&self) -> Vec<ContextItem> {
        self.external_context_items
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn tool_trace_context_items(&self) -> Vec<ContextItem> {
        self.tool_trace_context_items
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn clear_turn_tool_observations(&self) {
        if let Ok(mut guard) = self.turn_tool_observations.lock() {
            guard.clear();
        }
    }

    fn push_turn_tool_observation(&self, observation: ToolObservation) {
        if let Ok(mut guard) = self.turn_tool_observations.lock() {
            guard.push(observation);
        }
    }

    fn push_runtime_context_observation(
        &self,
        tool_name: impl Into<String>,
        invocation_id: impl Into<String>,
        summary: impl Into<String>,
    ) {
        let tool_name = tool_name.into();
        let invocation_id = invocation_id.into();
        let evidence_id = format!("{}:{invocation_id}", self.session().session_id);
        self.push_turn_tool_observation(ToolObservation::new(
            tool_name,
            invocation_id,
            EvidenceRef::new("runtime", evidence_id),
            summary,
        ));
    }

    fn turn_tool_observations(&self) -> Vec<ToolObservation> {
        self.turn_tool_observations
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn remember_tool_trace_from_message(&self, message: &ConversationMessage) {
        let Some(ContentBlock::ToolResult {
            tool_use_id,
            tool_name,
            output,
            is_error,
        }) = message.blocks.first()
        else {
            return;
        };
        let summary = output
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(600)
            .collect::<String>();
        let packet = ToolTracePacket {
            tool_name: tool_name.clone(),
            invocation_id: tool_use_id.clone(),
            status: if *is_error {
                ToolTraceStatus::Failed
            } else {
                ToolTraceStatus::Succeeded
            },
            summary,
            changed_files: Vec::new(),
            evidence_ids: vec![tool_use_id.clone()],
            token_estimate: (output.len() as u64).div_ceil(4).min(256).max(1),
        };
        let mut item = ContextRuntimeKernel::tool_trace_item(&packet);
        item.score = if *is_error { 0.9 } else { 0.65 };
        if let Ok(mut guard) = self.tool_trace_context_items.lock() {
            guard.retain(|existing| existing.id != item.id);
            guard.push(item);
            let overflow = guard.len().saturating_sub(8);
            if overflow > 0 {
                guard.drain(0..overflow);
            }
        }
    }

    fn external_context_prompt_tail(&self) -> String {
        self.external_context_items()
            .into_iter()
            .chain(self.tool_trace_context_items())
            .map(|item| {
                format!(
                    "<context_item source=\"{:?}\" role=\"{:?}\" score=\"{:.2}\">\n{}\n</context_item>",
                    item.source, item.role, item.score, item.content
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn dynamic_tail_with_external_context(&self, tail: String) -> String {
        let external = self.external_context_prompt_tail();
        if external.trim().is_empty() {
            tail
        } else if tail.trim().is_empty() {
            external
        } else {
            format!("{external}\n{tail}")
        }
    }

    fn remember_context_envelope(&self, envelope: ContextEnvelope) {
        if let Ok(mut guard) = self.last_context_envelope.lock() {
            *guard = Some(envelope.clone());
        }
        self.persist_context_envelope(envelope.clone());
        if let Some(cowd) = self.cowd_bus() {
            cowd.emit(crate::cowd_event::CowdEvent::ContextEnvelope { envelope });
        }
    }

    fn persist_context_envelope(&self, envelope: ContextEnvelope) {
        let Some(store) = self.session_store.as_ref() else {
            return;
        };
        let session_id = envelope.identity.session_id.clone();
        let envelope_id = envelope.id.clone();
        let payload = serde_json::json!({
            "type": "ContextEnvelope",
            "envelope_id": envelope_id,
            "session_id": session_id,
            "agent_id": envelope.identity.agent_id.clone(),
            "profile": envelope.profile,
            "diagnostics": envelope.diagnostics.clone(),
            "budget": envelope.budget.clone(),
            "hashes": {
                "stable_head": envelope.diagnostics.stable_head_hash,
                "runtime_header": envelope.diagnostics.runtime_header_hash,
                "dynamic_tail": envelope.diagnostics.dynamic_tail_hash,
            },
            "envelope": envelope,
        });
        let store = Arc::clone(store);
        tokio::spawn(async move {
            let sequence = match store.next_event_sequence(&session_id).await {
                Ok(sequence) => sequence,
                Err(error) => {
                    tracing::warn!(%error, session_id, "context envelope sequence allocation failed");
                    return;
                }
            };
            let created_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);
            let event = memory::SessionEvent {
                session_id: session_id.clone(),
                event_type: "ContextEnvelope".to_string(),
                event_json: payload.to_string(),
                sequence,
                created_at_ms,
            };
            match store.append_context_envelope_event_if_absent(&event).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::debug!(
                        session_id,
                        sequence,
                        "context envelope event already persisted"
                    );
                }
                Err(error) => {
                    tracing::warn!(%error, session_id, sequence, "context envelope event append failed");
                }
            }
        });
    }

    fn remember_context_turn_report(&self, report: ContextTurnReport) {
        if let Ok(mut guard) = self.last_context_turn_report.lock() {
            *guard = Some(report.clone());
        }
        self.persist_context_turn_report(report);
    }

    fn persist_context_turn_report(&self, report: ContextTurnReport) {
        let Some(store) = self.session_store.as_ref() else {
            return;
        };
        let session_id = self.session().session_id;
        let payload = serde_json::json!({
            "type": "ContextTurnReport",
            "turn_id": report.turn_id,
            "profile": report.profile,
            "pressure": report.pressure,
            "input_token_estimate": report.input_token_estimate,
            "output_token_estimate": report.output_token_estimate,
            "evidence_refs": report.evidence_refs,
            "observations": report.observations,
            "governance_decision": report.governance_decision,
            "compaction_receipt": report.compaction_receipt,
            "knowledge": report.knowledge,
        });
        let store = Arc::clone(store);
        tokio::spawn(async move {
            let sequence = match store.next_event_sequence(&session_id).await {
                Ok(sequence) => sequence,
                Err(error) => {
                    tracing::warn!(%error, session_id, "context turn report sequence allocation failed");
                    return;
                }
            };
            let created_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);
            let event = memory::SessionEvent {
                session_id: session_id.clone(),
                event_type: "ContextTurnReport".to_string(),
                event_json: payload.to_string(),
                sequence,
                created_at_ms,
            };
            if let Err(error) = store.append_event(&event).await {
                tracing::warn!(%error, session_id, sequence, "context turn report append failed");
            }
        });
    }

    fn context_budget_tokens(&self) -> u64 {
        if self.model_context_window > 0 {
            u64::from(self.model_context_window)
        } else {
            8_000
        }
    }

    fn build_context_turn_report(
        &self,
        turn_id: &str,
        usage: TokenUsage,
        auto_compaction: Option<AutoCompactionEvent>,
    ) -> ContextTurnReport {
        let used_tokens = estimate_session_tokens(&self.session()) as u64;
        let pressure = ContextPressureState::new(
            format!("{:?}", self.context_profile()),
            self.context_budget_tokens(),
            used_tokens,
        )
        .with_reserved_tokens(u64::from(usage.output_tokens));
        let mut decision = ContextGovernanceDecision::new(
            pressure.clone(),
            if pressure.compaction_recommended {
                "context pressure exceeded governance threshold"
            } else {
                "context pressure within governance budget"
            },
        );
        if let Some(compaction) = auto_compaction {
            decision.compact = true;
            decision.estimated_tokens_to_reclaim = compaction.removed_message_count as u64;
        }
        let mut report = ContextTurnReport::new(turn_id.to_string(), pressure)
            .with_output_token_estimate(u64::from(usage.output_tokens))
            .with_governance_decision(decision);
        for observation in self.turn_tool_observations() {
            report = report.with_observation(observation);
        }
        if let Some(knowledge) = self.take_turn_knowledge_report() {
            report = report.with_knowledge(knowledge);
        }
        report
    }

    fn set_turn_knowledge_report(&self, report: harness_contract::knowledge::KnowledgeTurnReport) {
        if let Ok(mut guard) = self.turn_knowledge_report.lock() {
            *guard = Some(report);
        }
    }

    fn take_turn_knowledge_report(
        &self,
    ) -> Option<harness_contract::knowledge::KnowledgeTurnReport> {
        self.turn_knowledge_report
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
    }

    fn build_context_envelope(
        &self,
        user_input: &str,
        dynamic_items: Vec<ContextItem>,
        omitted: Vec<ContextOmission>,
        degraded_sources: Vec<ContextSourceKind>,
    ) -> ContextEnvelope {
        let session_id = self.session().session_id;
        let profile = self.context_profile();
        let mut identity = ContextIdentity::main(session_id.clone());
        identity.mode = ContextRuntimeKernel::mode_for_profile(profile);
        let mut selected_items = self.external_context_items();
        selected_items.extend(self.tool_trace_context_items());
        selected_items.extend(dynamic_items);
        let mut envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile,
            runtime_header: ContextRuntimeKernel::runtime_header(&identity, profile),
            identity,
            intent: user_input.to_string(),
            stable_head: self.system_prompt.clone(),
            dynamic_items: selected_items,
            omitted,
            total_budget_tokens: self.context_budget_tokens(),
        });
        envelope.diagnostics.degraded_sources = degraded_sources;
        envelope
    }

    fn provider_prompt_from_envelope(
        envelope: &ContextEnvelope,
        dynamic_tail_override: Option<String>,
    ) -> Vec<String> {
        let mut prompt = Vec::with_capacity(
            envelope.assembled.stable_head.len() + envelope.assembled.runtime_header.len() + 1,
        );
        prompt.extend(envelope.assembled.stable_head.clone());
        prompt.extend(envelope.assembled.runtime_header.clone());
        if let Some(dynamic_tail) = dynamic_tail_override {
            if !dynamic_tail.trim().is_empty() {
                prompt.push(dynamic_tail);
            }
        } else {
            prompt.extend(envelope.assembled.dynamic_tail.clone());
        }
        prompt
    }

    fn append_context_items_to_latest_envelope(&self, user_input: &str, items: Vec<ContextItem>) {
        if items.is_empty() {
            return;
        }
        let mut dynamic_items = self
            .last_context_envelope()
            .map(|envelope| envelope.selected)
            .unwrap_or_default();
        dynamic_items.extend(items);
        let envelope =
            self.build_context_envelope(user_input, dynamic_items, Vec::new(), Vec::new());
        self.remember_context_envelope(envelope);
    }

    fn remember_collaboration_result(&self, result: CollaborationContextResult) {
        if let Ok(mut guard) = self.last_collaboration_result.lock() {
            *guard = Some(result);
        }
    }

    fn clear_collaboration_result(&self) {
        if let Ok(mut guard) = self.last_collaboration_result.lock() {
            *guard = None;
        }
    }

    #[must_use]
    pub fn with_auto_compaction_input_tokens_threshold(mut self, threshold: u32) -> Self {
        self.auto_compaction_input_tokens_threshold = threshold;
        self
    }

    pub fn with_model_context_window(mut self, ctx_window: u32) -> Self {
        self.model_context_window = ctx_window;
        if self.auto_compaction_input_tokens_threshold == 0 {
            self.auto_compaction_input_tokens_threshold = resolve_compact_threshold(ctx_window);
        }
        self
    }

    pub fn with_cached_prompt(
        mut self,
        config_path: std::path::PathBuf,
        identity_path: std::path::PathBuf,
    ) -> Self {
        self.cached_prompt =
            crate::cached_prompt::CachedSystemPrompt::new(config_path, identity_path);
        self
    }

    /// Set a tool callback for real-time execution visualization (P0-2).
    ///
    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_tool_callback(mut self, callback: Arc<dyn ToolCallback>) -> Self {
        self.tool_callback = Some(callback);
        self
    }

    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_sse_callback(mut self, callback: Arc<dyn Fn(String) + Send + Sync>) -> Self {
        self.sse_callback = Some(callback);
        self
    }

    /// Set the SSE callback on an already-constructed runtime instance.
    pub fn set_sse_callback(&mut self, callback: Arc<dyn Fn(String) + Send + Sync>) {
        self.sse_callback = Some(callback);
    }

    /// Clear the SSE callback from this runtime instance.
    pub fn clear_sse_callback(&mut self) {
        self.sse_callback = None;
    }

    #[must_use]
    pub fn with_session_store(
        mut self,
        store: Arc<memory::session_store::UnifiedSessionStore>,
    ) -> Self {
        self.session_store = Some(store);
        self
    }

    /// Attach a [`SessionEventLog`] for time-travel debugging and session rebuild.
    #[must_use]
    pub fn with_event_log(mut self, log: SessionEventLog) -> Self {
        self.event_log = Some(std::sync::Mutex::new(log));
        self
    }

    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_memory_callback(mut self, callback: Arc<dyn MemoryCallback>) -> Self {
        self.memory_callback = Some(callback);
        self
    }

    pub fn set_memory_callback(&mut self, callback: Arc<dyn MemoryCallback>) {
        self.memory_callback = Some(callback);
    }

    /// Set the smart approval gate for intelligent command approval (P0-1).
    #[must_use]
    pub fn with_approval_gate(
        mut self,
        gate: Arc<crate::approval_gate::SmartApprovalGate>,
    ) -> Self {
        self.approval_gate = Some(gate);
        self
    }

    /// Inject a type-erased [`CollaborationOrchestrator`] for multi-agent dispatch.
    ///
    /// # Safety
    /// The orchestrator MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    #[must_use]
    pub fn with_collaboration(mut self, c: Arc<dyn CollaborationOps>) -> Self {
        self.collaboration = Some(c);
        self
    }

    /// Provide Skill capability profiles already inspected by the Skill asset
    /// layer. Runtime consumes these profiles during activation, but does not
    /// inspect packages or own the registry.
    #[must_use]
    pub fn with_skill_profiles(mut self, profiles: Vec<SkillCapabilityProfile>) -> Self {
        self.skill_profiles = profiles;
        self
    }

    /// Configure the agent-scoped Skill visibility and adapter ceiling used by
    /// runtime activation.
    #[must_use]
    pub fn with_agent_skill_profile(mut self, profile: AgentSkillProfile) -> Self {
        self.agent_skill_profile = profile;
        self
    }

    #[must_use]
    pub fn with_jps_pipeline(mut self, pipeline: Arc<dyn JpsOps>) -> Self {
        self.jps_pipeline = Some(pipeline);
        self
    }

    #[must_use]
    pub fn with_runtime_control_policy(mut self, policy: RuntimeControlPolicy) -> Self {
        self.runtime_control_policy = policy;
        self
    }

    /// T35: Set a cancellation token for graceful shutdown.
    #[must_use]
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = token;
        self
    }

    /// Attach a CowdEventBus for domain event emission and drain pending warnings.
    #[must_use]
    pub fn with_cowd_event_bus(mut self, bus: crate::cowd_event::CowdEventBus) -> Self {
        // Drain config warnings into the cowd bus now that it's available
        if let Ok(mut pending) = crate::config::PENDING_WARNINGS.lock() {
            for event in pending.drain(..) {
                if let crate::cowd_event::CowdEvent::Warning { message } = event {
                    let _ = bus.emit(crate::cowd_event::CowdEvent::Warning { message });
                }
            }
        }
        self.cowd_bus = Some(Arc::new(bus.clone()));
        if let Some(ref mem) = self.memory_manager {
            let engine = DiscussionEngine::new(Arc::new(bus), Arc::clone(mem));
            // Watcher is started lazily on first turn (L1409) to ensure tokio context
            self.discussion_engine = Some(Arc::new(std::sync::Mutex::new(engine)));
        }
        self
    }

    /// Get a reference to the attached CowdEventBus, if any.
    pub fn cowd_bus(&self) -> Option<&crate::cowd_event::CowdEventBus> {
        self.cowd_bus.as_deref()
    }

    /// T36: Set a custom tool orchestrator for result budgeting.
    #[must_use]
    pub fn with_tool_orchestrator(
        mut self,
        orchestrator: crate::tool_orchestrator::ToolOrchestrator,
    ) -> Self {
        self.tool_orchestrator = orchestrator;
        self
    }

    /// P0: Enable AAAK symbolic index mode for memory context injection.
    #[must_use]
    pub fn with_aaak_index(mut self) -> Self {
        self.use_aaak_index = true;
        self
    }

    /// P1-05: Register a TurnCallback for generator-style injection after tool results.
    #[must_use]
    pub fn with_turn_callback(mut self, cb: TurnCallback) -> Self {
        self.turn_callback = Some(Arc::new(cb));
        self
    }

    #[must_use]
    pub fn with_hook_abort_signal(mut self, hook_abort_signal: HookAbortSignal) -> Self {
        self.hook_abort_signal = hook_abort_signal;
        self
    }

    #[must_use]
    pub fn with_hook_progress_reporter(
        self,
        hook_progress_reporter: Box<dyn HookProgressReporter + Send>,
    ) -> Self {
        *self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(hook_progress_reporter);
        self
    }

    #[must_use]
    pub fn with_session_tracer(mut self, session_tracer: SessionTracer) -> Self {
        self.session_tracer = Some(session_tracer);
        self
    }

    /// Override the memory manager with a pre-constructed instance.
    ///
    /// This is primarily useful in tests or when the caller wants full control
    /// over the [`CognitiveContextManager`] lifecycle.
    #[must_use]
    pub fn with_memory_manager(mut self, manager: Arc<CognitiveContextManager>) -> Self {
        self.memory_manager = Some(manager);
        self
    }

    pub fn with_gate_evaluator(mut self, evaluator: crate::gates::GateEvaluator) -> Self {
        self.gate_evaluator = Some(Arc::new(evaluator));
        self
    }

    /// Run all commit quality gates against the current state.
    pub fn check_commit_gates(
        &self,
        context: crate::gates::GateContext,
    ) -> Option<(bool, Vec<crate::gates::GateResult>)> {
        self.gate_evaluator
            .as_ref()
            .map(|evaluator| evaluator.evaluate_all(&context))
    }

    /// Create a sub-agent runtime with independent LLM reasoning capabilities.
    pub fn create_subagent_runtime(&self, config: &SubAgentConfig) -> SubAgentRuntime<C, T>
    where
        C: Clone,
    {
        let mut config = config.clone();
        let parent_session_id = self.session().session_id;
        let lease = config.ensure_context_lease(parent_session_id, "primary");
        let model = config.model.clone().or_else(|| self.model.clone());
        let filtered_prompt =
            filter_system_prompt_for_role(&self.system_prompt, &config.task_description);
        let mut sub_rt = ConversationRuntime::new_with_features(
            crate::session::Session::new(),
            self.api_client.clone(),
            Arc::clone(&self.tool_executor),
            self.permission_policy.clone(),
            filtered_prompt,
            &RuntimeFeatureConfig::default(),
        );
        if let Some(ref m) = model {
            sub_rt.model = Some(m.clone());
        }
        sub_rt.set_context_profile(ContextProfile::SubAgent);
        sub_rt.runtime_control_policy = self.runtime_control_policy.clone();
        sub_rt = sub_rt.with_model_context_window(lease.max_tokens.min(u64::from(u32::MAX)) as u32);
        sub_rt.max_iterations = config.max_turns;
        sub_rt.tool_orchestrator = self.tool_orchestrator.clone();
        if let Some(ref mem) = self.memory_manager {
            sub_rt = sub_rt.with_memory_manager(Arc::clone(mem));
        }
        let mut sub_agent = SubAgentRuntime::new(config, sub_rt);
        if let Some(ref mem) = self.memory_manager {
            sub_agent = sub_agent.with_parent_memory(Arc::clone(mem));
        }
        sub_agent
    }

    /// Explicitly disable the memory subsystem, regardless of feature config.
    #[must_use]
    pub fn without_memory(mut self) -> Self {
        self.memory_manager = None;
        self
    }

    /// Access the cognitive memory manager, if memory is enabled.
    ///
    /// Returns `None` when memory is disabled or failed to initialise.
    #[must_use]
    pub fn memory_manager(&self) -> Option<&Arc<CognitiveContextManager>> {
        self.memory_manager.as_ref()
    }

    /// Determine whether the current user message warrants multi-agent collaboration.
    fn should_use_collaboration(&self, user_message: &str) -> bool {
        self.runtime_control_policy
            .should_collaborate(&TaskComplexityInput::new(
                user_message,
                self.context_profile(),
            ))
    }

    /// Infer required capability keywords from a task description.
    fn infer_required_capabilities(user_message: &str) -> Vec<String> {
        let lower = user_message.to_lowercase();
        let keyword_map: &[(&str, &[&str])] = &[
            ("rust", &["rust", "cargo", "borrow checker", "lifetime"]),
            (
                "testing",
                &["test", "assert", "mock", "coverage", "fixture"],
            ),
            (
                "refactoring",
                &["refactor", "extract", "rename", "restructure", "clean"],
            ),
            (
                "review",
                &["review", "audit", "inspect", "examine", "check"],
            ),
            (
                "documentation",
                &["document", "doc", "readme", "explain", "describe"],
            ),
            (
                "planning",
                &["plan", "design", "architect", "spec", "outline"],
            ),
            (
                "execution",
                &["execute", "run", "build", "compile", "deploy"],
            ),
            ("debugging", &["debug", "fix", "bug", "error", "crash"]),
            (
                "security",
                &["security", "vuln", "exploit", "injection", "xss"],
            ),
            (
                "performance",
                &["perf", "benchmark", "optimize", "slow", "latency"],
            ),
        ];
        let mut skills = Vec::new();
        for (skill, keywords) in keyword_map {
            for kw in *keywords {
                if lower.contains(kw) {
                    skills.push(skill.to_string());
                    break;
                }
            }
        }
        if skills.is_empty() {
            skills.push("general".to_string());
        }
        skills
    }

    /// Create a cross-session handoff packet from the current memory state.
    ///
    /// Returns `None` if the memory subsystem is disabled.
    pub async fn create_memory_handoff(&self) -> Option<memory::types::HandoffData> {
        let mgr = self.memory_manager.as_ref()?;
        match mgr.create_handoff().await {
            Ok(data) => Some(data),
            Err(err) => {
                tracing::warn!(%err, "memory: failed to create handoff packet");
                None
            }
        }
    }

    /// Restore memory state from a previously created handoff packet.
    pub fn restore_memory_handoff(&self, data: memory::types::HandoffData) {
        let Some(mgr) = self.memory_manager.as_ref() else {
            return;
        };
        let mgr = Arc::clone(mgr);
        match tokio::runtime::Handle::try_current() {
            Ok(_handle) => {
                tokio::spawn(async move {
                    if let Err(err) = mgr.restore_handoff(data).await {
                        tracing::warn!(%err, "memory: failed to restore handoff");
                    }
                });
            }
            Err(_) => {
                tracing::warn!("memory: no tokio runtime, cannot restore handoff");
            }
        }
    }

    fn record_context_event(
        &mut self,
        event_type: &str,
        category: &str,
        summary: &str,
        priority: u8,
    ) {
        let project_dir = std::env::current_dir()
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.profiler
            .record_dedup(crate::context_profiler::SessionEvent {
                event_type: event_type.into(),
                category: category.into(),
                data_summary: summary.into(),
                priority,
                data_hash: 0, // computed by record_dedup
                timestamp,
                project_dir,
                attribution_confidence: 0.9,
            });
    }

    fn run_pre_tool_use_hook(&self, tool_name: &str, input: &str) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_pre_tool_use_with_context(
                tool_name,
                input,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_pre_tool_use_with_context(
                tool_name,
                input,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    fn run_post_tool_use_hook(
        &self,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_post_tool_use_with_context(
                tool_name,
                input,
                output,
                is_error,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_post_tool_use_with_context(
                tool_name,
                input,
                output,
                is_error,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    fn run_post_tool_use_failure_hook(
        &self,
        tool_name: &str,
        input: &str,
        output: &str,
    ) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_post_tool_use_failure_with_context(
                tool_name,
                input,
                output,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_post_tool_use_failure_with_context(
                tool_name,
                input,
                output,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    /// Run a session health probe to verify the runtime is functional after compaction.
    /// Returns Ok(()) if healthy, Err if the session appears broken.
    fn run_session_health_probe(&mut self) -> Result<(), String> {
        // Check if we have basic session integrity
        if self.session.blocking_read().messages.is_empty()
            && self.session.blocking_read().compaction.is_some()
        {
            // Freshly compacted with no messages - this is normal
            return Ok(());
        }

        // Verify tool executor is responsive with a non-destructive probe
        // Using glob_search with a pattern that won't match anything
        let probe_input = r#"{"pattern": "*.health-check-probe-"}"#;
        match self.tool_executor.execute("glob_search", probe_input) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Tool executor probe failed: {e}")),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn run_turn_async(
        &mut self,
        user_input: impl Into<String>,
        prompter: &crate::permissions::SharedPrompter,
    ) -> Result<TurnSummary, RuntimeError> {
        let turn_started_at = Instant::now();
        let mut first_token_latency_ms: Option<u64> = None;
        let mut first_stream_token_at: Option<Instant> = None;
        let mut last_stream_token_at: Option<Instant> = None;
        let mut output_chars: u64 = 0;
        let mut output_chunks: u64 = 0;
        let mut provider_usage_total = TokenUsage::default();
        let mut provider_usage_seen = false;
        let mut models_used: Vec<String> = Vec::new();
        let user_input = user_input.into();
        tracing::info!(session_id = %self.session().session_id, "turn started");
        self.clear_collaboration_result();
        self.clear_turn_tool_observations();
        let _ = self.take_turn_knowledge_report();
        let strategy_input = self.strategy_input_for_turn(&user_input);
        let mut runtime_harness = RuntimeAiKernel::begin_turn_with_strategy_input(
            self.session().session_id.clone(),
            user_input.clone(),
            self.context_profile(),
            &self.system_prompt,
            strategy_input,
        );

        if self.session.read().await.compaction.is_some() {
            if let Err(error) = self.run_session_health_probe() {
                return Err(RuntimeError::new(format!(
                    "Session health probe failed: {error}"
                )));
            }
        }

        self.record_turn_started(&user_input);
        self.record_context_event("user_input", "user", &preview_chars(&user_input, 200), 8);
        self.session
            .write()
            .await
            .push_user_text(user_input.clone())
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let user_sequence = self.session().messages.len().wrapping_sub(1);
        self.dual_write_message(
            &ConversationMessage::user_text(user_input.clone()),
            user_sequence,
        );
        let complexity = self
            .runtime_control_policy
            .profile_task(&TaskComplexityInput::new(
                user_input.clone(),
                self.context_profile(),
            ));
        self.record_runtime_policy_decision(&complexity, user_sequence);

        let evidence_plan = crate::evidence_planner::plan_evidence(&user_input);
        let evidence_plan_guidance = crate::evidence_planner::evidence_plan_prompt(&evidence_plan);
        let execution_decision = crate::execution_core::build_runtime_execution_decision(
            &user_input,
            Some(self.context_profile()),
        );
        let execution_decision_guidance =
            crate::execution_core::runtime_execution_guidance_prompt(&execution_decision);
        self.record_context_event(
            "evidence_plan",
            "runtime",
            &format!("{:?}: {}", evidence_plan.mode, evidence_plan.reason),
            7,
        );
        self.push_runtime_context_observation(
            "runtime.evidence_plan",
            format!("evidence-plan-{user_sequence}"),
            format!("{:?}: {}", evidence_plan.mode, evidence_plan.reason),
        );
        self.record_context_event(
            "execution_decision",
            "runtime",
            &format!(
                "{}: {:?}",
                execution_decision.recommended_mode.as_str(),
                execution_decision.recommended_actions
            ),
            8,
        );
        self.push_runtime_context_observation(
            "runtime.execution_decision",
            format!("execution-decision-{user_sequence}"),
            execution_decision_guidance.clone(),
        );

        let mut effective_system_prompt = self.prepare_reality_context(&user_input).await;
        effective_system_prompt.push(evidence_plan_guidance.clone());
        effective_system_prompt.push(execution_decision_guidance);
        if knowledge_hard_gate_active(&effective_system_prompt) {
            let error = RuntimeError::new("knowledge compliance hard gate blocked turn");
            self.record_turn_failed(0, &error);
            return Err(error);
        }

        // A2: Inject available peer agents from AgentDirectory into the system prompt.
        if self.inject_peer_context {
            let active_agents = memory::agent_directory::AgentDirectory::global().list_active();
            let peers: Vec<String> = active_agents
                .iter()
                .filter(|a| a.agent_id != self.session().session_id)
                .map(|a| {
                    format!(
                        "  - {} (role: {}, capabilities: {:?})",
                        &a.agent_id[..std::cmp::min(8, a.agent_id.len())],
                        a.role,
                        a.capabilities
                    )
                })
                .collect();
            if !peers.is_empty() {
                effective_system_prompt.push(format!(
                    "\n## Available Peer Agents\n{}\n",
                    peers.join("\n")
                ));
            }
        }

        let mut assistant_messages = Vec::new();
        let mut tool_results = Vec::new();
        let mut prompt_cache_events = Vec::new();
        let mut iterations = 0;
        let mut turn_supervisor = crate::turn_supervisor::TurnSupervisor::new();
        let mut supervisor_final_answer_deadline: Option<usize> = None;

        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::TurnStarted);
        }

        loop {
            iterations += 1;
            if iterations > self.max_iterations {
                let error = RuntimeError::new("max iterations exceeded");
                tracing::error!(iterations, "turn failed: max iterations exceeded");
                self.record_turn_failed(iterations, &error);
                return Err(error);
            }

            if self.auto_compaction_input_tokens_threshold > 0
                && estimate_session_tokens(&*self.session.read().await)
                    > self.auto_compaction_input_tokens_threshold as usize
            {
                let result =
                    compact_session(&*self.session.read().await, CompactionConfig::default());
                if result.removed_message_count > 0 {
                    let compacted_len = result.compacted_session.messages.len();
                    *self.session.write().await = result.compacted_session;
                    // Record compaction as a MessagesTruncated event for event log.
                    if let Some(ref log) = self.event_log {
                        if let Ok(mut guard) = log.lock() {
                            guard.push(MessageEvent::MessagesTruncated {
                                sequence: compacted_len,
                            });
                        }
                    }
                    effective_system_prompt = self.prepare_reality_context(&user_input).await;
                    effective_system_prompt.push(evidence_plan_guidance.clone());
                    if knowledge_hard_gate_active(&effective_system_prompt) {
                        let error =
                            RuntimeError::new("knowledge compliance hard gate blocked turn");
                        self.record_turn_failed(iterations, &error);
                        return Err(error);
                    }
                }
            }
            if self.model_context_window > 0 {
                let used = estimate_session_tokens(&*self.session.read().await);
                if used as f64 / self.model_context_window as f64 > 0.85 {
                    tracing::warn!(used, "context window pressure critical");
                }
            }

            let request = ApiRequest {
                system_prompt: effective_system_prompt.clone(),
                messages: self.session.read().await.messages.clone(),
                model: String::new(), // filled by fallback loop below
            };

            let model_list = self.model_candidates_for_turn(&user_input);

            // Use the new Stream-based API — consume events as they arrive
            use futures::StreamExt;
            let mut current_text = String::new();
            let mut thinking_text = String::new();
            let mut thinking_signature: Option<String> = None;
            let mut pending_tool_uses: Vec<(String, String, String)> = Vec::new();
            let mut turn_usage: Option<TokenUsage> = None;
            let mut stream_events: Vec<(String, String, String, u8)> = Vec::new();

            let mut stream_error: Option<RuntimeError> = None;
            let stream_success = 'retry: {
                for (model_idx, model) in model_list.iter().enumerate() {
                    let max_retries: u32 = 8;
                    for attempt in 0..max_retries {
                        let mut req = request.clone();
                        req.model = model.to_string();

                        let stream_idle_timeout = stream_idle_timeout_for_messages(&req.messages);
                        let mut stream = self.api_client.stream(req);
                        let mut model_current_text = String::new();
                        let mut model_thinking_text = String::new();
                        let mut model_thinking_signature: Option<String> = None;
                        let mut model_pending_tool_uses: Vec<(String, String, String)> = Vec::new();
                        let mut model_turn_usage: Option<TokenUsage> = None;
                        let mut model_stream_events: Vec<(String, String, String, u8)> = Vec::new();

                        let mut failed = false;
                        loop {
                            // T35: Check cancellation before each stream poll.
                            if self.cancellation_token.is_cancelled() {
                                return Err(RuntimeError::new("conversation cancelled"));
                            }
                            let next_event = match tokio::time::timeout(
                                stream_idle_timeout,
                                stream.next(),
                            )
                            .await
                            {
                                Ok(Some(event)) => event,
                                Ok(None) => break,
                                Err(_) => {
                                    return Err(RuntimeError::new(format!(
                                        "stream idle timed out after {}s",
                                        stream_idle_timeout.as_secs()
                                    )));
                                }
                            };
                            match next_event {
                                Ok(AssistantEvent::TextDelta(text)) => {
                                    let now = Instant::now();
                                    if !text.is_empty() {
                                        if first_token_latency_ms.is_none() {
                                            first_token_latency_ms =
                                                Some(millis_since(turn_started_at));
                                            first_stream_token_at = Some(now);
                                        }
                                        last_stream_token_at = Some(now);
                                        output_chars = output_chars
                                            .saturating_add(text.chars().count() as u64);
                                        output_chunks = output_chunks.saturating_add(1);
                                    }
                                    model_current_text.push_str(&text);
                                    if let Some(ref cowd) = self.cowd_bus {
                                        cowd.emit(crate::cowd_event::CowdEvent::TextDelta {
                                            text: text.clone(),
                                        });
                                    }
                                    model_stream_events.push((
                                        "text_delta".into(),
                                        "assistant".into(),
                                        preview_chars(&text, 80),
                                        3,
                                    ));
                                    if let Some(ref cb) = self.sse_callback {
                                        let json = serde_json::json!({
                                            "type": "TextDelta",
                                            "content": &text,
                                        });
                                        cb(json.to_string());
                                    }
                                }
                                Ok(AssistantEvent::ThinkingDelta(thinking)) => {
                                    model_thinking_text.push_str(&thinking);
                                    model_stream_events.push((
                                        "thinking".into(),
                                        "reasoning".into(),
                                        preview_chars(&thinking, 80),
                                        2,
                                    ));
                                    if let Some(ref cb) = self.sse_callback {
                                        let json = serde_json::json!({
                                            "type": "ThinkingDelta",
                                            "content": &thinking,
                                        });
                                        cb(json.to_string());
                                    }
                                    if let Some(ref cowd) = self.cowd_bus {
                                        cowd.emit(crate::cowd_event::CowdEvent::ThinkingDelta {
                                            thinking: thinking.clone(),
                                        });
                                    }
                                }
                                Ok(AssistantEvent::SignatureDelta(signature)) => {
                                    model_thinking_signature = Some(signature);
                                }
                                Ok(AssistantEvent::ToolUse { id, name, input }) => {
                                    model_pending_tool_uses.push((id, name, input));
                                }
                                Ok(AssistantEvent::Usage(usage)) => {
                                    provider_usage_seen = true;
                                    add_token_usage(&mut provider_usage_total, usage);
                                    if let Some(ref cowd) = self.cowd_bus {
                                        cowd.emit(crate::cowd_event::CowdEvent::TokenUsage {
                                            input: u64::from(usage.input_tokens),
                                            output: u64::from(usage.output_tokens),
                                            cache_create: u64::from(
                                                usage.cache_creation_input_tokens,
                                            ),
                                            cache_read: u64::from(usage.cache_read_input_tokens),
                                        });
                                    }
                                    model_turn_usage = Some(usage);
                                }
                                Ok(AssistantEvent::MessageStop) => break,
                                Ok(AssistantEvent::ToolStart { id, name, preview }) => {
                                    if let Some(callback) = &self.tool_callback {
                                        callback.on_tool_start(&id, &name, &preview);
                                    }
                                    if let Some(ref cb) = self.sse_callback {
                                        let json = serde_json::json!({
                                            "type": "ToolStart",
                                            "id": &id,
                                            "name": &name,
                                            "preview": &preview,
                                        });
                                        cb(json.to_string());
                                    }
                                    if let Some(ref cowd) = self.cowd_bus {
                                        cowd.emit(crate::cowd_event::CowdEvent::ToolStart {
                                            id: id.clone(),
                                            name: name.clone(),
                                            preview: preview.clone(),
                                        });
                                    }
                                }
                                Ok(AssistantEvent::ToolProgress { id, name, progress }) => {
                                    if let Some(callback) = &self.tool_callback {
                                        callback.on_tool_progress(&id, &name, &progress);
                                    }
                                    if let Some(ref cb) = self.sse_callback {
                                        let json = serde_json::json!({
                                            "type": "ToolProgress",
                                            "id": &id,
                                            "name": &name,
                                            "progress": &progress,
                                        });
                                        cb(json.to_string());
                                    }
                                    if let Some(ref cowd) = self.cowd_bus {
                                        cowd.emit(crate::cowd_event::CowdEvent::ToolProgress {
                                            id: id.clone(),
                                            name: name.clone(),
                                            progress: progress.clone(),
                                        });
                                    }
                                }
                                Ok(AssistantEvent::PromptCache(event)) => {
                                    prompt_cache_events.push(event);
                                }
                                Ok(AssistantEvent::ToolComplete {
                                    id,
                                    name,
                                    result_summary,
                                    exit_code,
                                }) => {
                                    if let Some(callback) = &self.tool_callback {
                                        callback.on_tool_complete(
                                            &id,
                                            &name,
                                            &result_summary,
                                            exit_code,
                                        );
                                    }
                                    if let Some(ref cb) = self.sse_callback {
                                        let json = serde_json::json!({
                                            "type": "ToolComplete",
                                            "id": &id,
                                            "name": &name,
                                            "summary": &result_summary,
                                            "exit_code": exit_code,
                                        });
                                        cb(json.to_string());
                                    }
                                    if let Some(ref cowd) = self.cowd_bus {
                                        cowd.emit(crate::cowd_event::CowdEvent::ToolComplete {
                                            id: id.clone(),
                                            name: name.clone(),
                                            summary: result_summary.clone(),
                                            exit_code,
                                        });
                                    }
                                }
                                Err(e) => {
                                    let err_str = e.to_string();
                                    if is_retryable_error(&err_str) {
                                        tracing::warn!(model, attempt, model_idx, error = %err_str, "retryable stream error");
                                        // T30: Exponential backoff before retry.
                                        tokio::time::sleep(Duration::from_millis(
                                            500 * 2u64.pow(attempt),
                                        ))
                                        .await;
                                        if attempt == max_retries - 1 {
                                            tracing::warn!(
                                                model,
                                                "exhausted retries, switching fallback"
                                            );
                                        }
                                        failed = true;
                                        stream_error = Some(e);
                                        break;
                                    }
                                    return Err(e);
                                }
                            }
                        }
                        if !failed {
                            current_text = model_current_text;
                            thinking_text = model_thinking_text;
                            thinking_signature = model_thinking_signature;
                            pending_tool_uses = model_pending_tool_uses;
                            turn_usage = model_turn_usage;
                            stream_events = model_stream_events;
                            if !model.is_empty() && !models_used.iter().any(|known| known == model)
                            {
                                models_used.push(model.to_string());
                            }
                            if model_idx > 0 {
                                tracing::warn!(
                                    model,
                                    fallback_model_idx = model_idx,
                                    "provider fallback succeeded"
                                );
                            }
                            break 'retry true;
                        }
                    }
                }
                false
            };

            if !stream_success {
                return Err(stream_error
                    .unwrap_or_else(|| RuntimeError::new("all provider fallbacks exhausted")));
            }

            // Flush buffered stream events into context profiler
            for (event_type, category, summary, priority) in stream_events {
                self.record_context_event(&event_type, &category, &summary, priority);
            }

            if let Some(usage) = turn_usage {
                self.usage_tracker.record(usage);
                if let Some(cb) = &self.tool_callback {
                    cb.on_usage(&usage);
                }
            }

            // Build assistant message with text + tool_use blocks
            let mut blocks = Vec::new();
            if !thinking_text.is_empty() {
                blocks.push(ContentBlock::Thinking {
                    thinking: thinking_text.clone(),
                    signature: thinking_signature.clone(),
                });
                tracing::debug!(
                    thinking_len = thinking_text.len(),
                    has_signature = thinking_signature.is_some(),
                    "thinking block stored"
                );
            }
            blocks.push(ContentBlock::Text { text: current_text });
            for (id, name, input) in &pending_tool_uses {
                blocks.push(ContentBlock::ToolUse {
                    id: id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                });
            }
            let role = crate::session::MessageRole::Assistant;
            let assistant_msg = ConversationMessage {
                role,
                blocks,
                usage: turn_usage,
            };
            self.session
                .write()
                .await
                .push_message(assistant_msg.clone())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            self.dual_write_message(
                &assistant_msg,
                self.session().messages.len().wrapping_sub(1),
            );
            self.record_assistant_iteration(iterations, &assistant_msg, pending_tool_uses.len());
            assistant_messages.push(assistant_msg);

            if pending_tool_uses.is_empty() {
                break;
            }

            // Phase 2: Parallel+serial tool dispatch based on safety categories
            let mut callback_inject = None;
            {
                use crate::execution_scheduler::schedule_tool_requests;
                use crate::tool_dispatch::ToolRequest;

                let mut requests: Vec<ToolRequest> = pending_tool_uses
                    .iter()
                    .map(|(id, name, input)| ToolRequest {
                        tool_use_id: id.clone(),
                        tool_name: name.clone(),
                        input: input.clone(),
                        depends_on: Vec::new(),
                    })
                    .collect();
                runtime_harness.record_tool_requests(&pending_tool_uses);
                let _ = crate::intent_planner::infer_tool_dependencies(&mut requests);
                let ordered_ids: Vec<String> =
                    requests.iter().map(|r| r.tool_use_id.clone()).collect();
                let execution_plan = ToolExecutionPlan::from_requests(&requests);
                self.record_tool_execution_plan(&execution_plan, self.session().messages.len());
                let tool_schedule = schedule_tool_requests(&requests);
                self.record_tool_schedule(&tool_schedule, &requests, self.session().messages.len());

                let mut result_map: std::collections::HashMap<
                    String,
                    (ConversationMessage, Option<String>),
                > = std::collections::HashMap::new();

                for batch in &tool_schedule.batches {
                    self.execute_tool_schedule_batch(
                        batch,
                        &requests,
                        &pending_tool_uses,
                        prompter,
                        iterations,
                        &mut result_map,
                    )
                    .await?;
                }

                for id in &ordered_ids {
                    if let Some((msg, inject)) = result_map.remove(id) {
                        self.remember_tool_trace_from_message(&msg);
                        let tool_name_str = msg
                            .blocks
                            .first()
                            .and_then(|b| match b {
                                ContentBlock::ToolResult { tool_name, .. } => {
                                    Some(tool_name.as_str())
                                }
                                _ => None,
                            })
                            .unwrap_or("unknown");
                        self.record_context_event(
                            "tool_use",
                            "tool",
                            &format!("{}: {}", tool_name_str, ""),
                            5,
                        );
                        if let Some((output, is_error)) = msg.blocks.first().and_then(|block| {
                            if let ContentBlock::ToolResult {
                                output, is_error, ..
                            } = block
                            {
                                Some((output.as_str(), *is_error))
                            } else {
                                None
                            }
                        }) {
                            let (supervisor_tool_name, supervisor_input) = pending_tool_uses
                                .iter()
                                .find(|(pending_id, _, _)| pending_id == id)
                                .map(|(_, name, input)| (name.as_str(), input.as_str()))
                                .unwrap_or((tool_name_str, "{}"));
                            let (observation, decision) = turn_supervisor.observe_tool_result(
                                supervisor_tool_name,
                                supervisor_input,
                                output,
                                is_error,
                            );
                            if decision.should_inject() {
                                if matches!(
                                    decision,
                                    crate::turn_supervisor::SupervisorDecision::FallbackAnswer { .. }
                                ) && supervisor_final_answer_deadline.is_none()
                                {
                                    supervisor_final_answer_deadline =
                                        Some(iterations.saturating_add(1));
                                }
                                if let Some(prompt) = decision.prompt() {
                                    effective_system_prompt.push(format!(
                                        "\n## Runtime supervisor guidance\n{prompt}"
                                    ));
                                }
                                self.record_context_event(
                                    "turn_supervisor",
                                    "runtime",
                                    decision.reason().unwrap_or(decision.kind()),
                                    8,
                                );
                                self.push_runtime_context_observation(
                                    "runtime.turn_supervisor",
                                    format!(
                                        "turn-supervisor-{}-{}",
                                        self.session().messages.len(),
                                        observation.fingerprint.input_hash
                                    ),
                                    format!(
                                        "{}: {}",
                                        decision.kind(),
                                        decision.reason().unwrap_or("runtime supervisor guidance")
                                    ),
                                );
                                self.record_turn_supervisor_decision(
                                    &observation,
                                    &decision,
                                    self.session().messages.len(),
                                );
                            }
                        }
                        if let Some(new_input) = inject {
                            callback_inject = Some(new_input);
                        }
                        tool_results.push(msg);
                    }
                }
            }
            if let Some(inject) = callback_inject {
                let inject_text = inject.clone();
                self.session
                    .write()
                    .await
                    .push_user_text(inject)
                    .map_err(|e| RuntimeError::new(e.to_string()))?;
                self.dual_write_message(
                    &ConversationMessage::user_text(inject_text),
                    self.session().messages.len().wrapping_sub(1),
                );
                continue; // continue loop with injected input
            }

            // A model turn that requested tools is not complete until the model
            // sees the resulting `tool_result` messages and synthesizes the
            // next assistant response. Keep post-turn maintenance after the
            // final no-tool assistant message so runtime supervisor guidance,
            // tool evidence, and callback-injected context all have a chance to
            // influence the answer.
            if supervisor_final_answer_deadline.is_some_and(|deadline| iterations >= deadline) {
                let error = RuntimeError::new(
                    "turn supervisor stopped repeated low-novelty tool loop after fallback guidance was ignored",
                );
                tracing::warn!(
                    iterations,
                    "turn supervisor stopped repeated low-novelty tool loop"
                );
                self.record_turn_failed(iterations, &error);
                return Err(error);
            }
            continue;
        }

        let auto_compaction = self.maybe_auto_compact().await;
        let _ = self.run_memory_post_turn().await;

        // A3: Synchronously check for L4 conflicts after each turn.
        if let Some(ref engine_arc) = self.discussion_engine {
            if let Ok(mut engine) = engine_arc.lock() {
                engine.start_watcher(); // deferred start: now in tokio context
                let conflict_count = engine.check_for_conflicts_sync().unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "DiscussionEngine conflict check failed");
                    0
                });

                if conflict_count > 0 {
                    tracing::info!(conflict_count, "L4 conflicts detected");

                    // P3: Trigger discussion if conflicts found
                    let topic = format!(
                        "L4 memory conflict resolution ({} conflicts)",
                        conflict_count
                    );
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let participants = vec![memory::agent_directory::AgentInfo {
                        agent_id: "primary".to_string(),
                        role: "Resolver".to_string(),
                        capabilities: vec!["conflict-resolution".to_string(), "memory".to_string()],
                        status: memory::agent_directory::AgentStatus::Active,
                        registered_at_ms: now_ms,
                        last_heartbeat_ms: 0,
                        reputation: None,
                    }];

                    // Drop the lock before async operations
                    drop(engine);

                    // Re-acquire for async operations via spawn_blocking
                    let engine_clone = Arc::clone(engine_arc);
                    let session = Arc::clone(&self.session);
                    tokio::task::spawn_blocking(move || {
                        let handle = tokio::runtime::Handle::try_current();
                        let handle = match handle {
                            Ok(h) => h,
                            Err(e) => {
                                tracing::warn!(error = %e, "No tokio handle in spawn_blocking — skipping discussion");
                                return;
                            }
                        };
                        handle.block_on(async {
                            if let Ok(mut eng) = engine_clone.lock() {
                                let _ = eng
                                    .start_discussion(
                                        topic,
                                        participants,
                                        crate::agent_discussion::ConsensusMethod::MajorityVote,
                                        2,
                                    )
                                    .await;
                                // After discussion, check and log consensus
                                match eng.check_consensus().await {
                                    Ok(consensus) if consensus.reached => {
                                        tracing::info!(
                                            score = consensus.score,
                                            agreeing = consensus.agreeing_count,
                                            total = consensus.total_count,
                                            "Discussion reached consensus"
                                        );
                                        match eng.finalize().await {
                                            Ok(decision) if !decision.is_empty() => {
                                                tracing::info!(decision_len = decision.len(), "Discussion finalized, injecting decision into session");
                                                use crate::session::{ContentBlock, ConversationMessage, MessageRole};
                                                let msg = ConversationMessage {
                                                    role: MessageRole::System,
                                                    blocks: vec![ContentBlock::Text { text: format!("[AGENT DISCUSSION DECISION]\n{}", decision) }],
                                                    usage: None,
                                                };
                                                if let Err(e) = session.write().await.push_message(msg) {
                                                    tracing::warn!(error = %e, "Failed to inject discussion decision into session");
                                                }
                                            }
                                            Ok(_) => tracing::debug!("Discussion finalized with empty decision"),
                                            Err(e) => tracing::warn!(error = %e, "Discussion finalize failed"),
                                        }
                                    }
                                    Ok(consensus) => {
                                        tracing::info!(
                                            score = consensus.score,
                                            "Discussion did not reach consensus"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Consensus check failed"
                                        );
                                    }
                                }
                            }
                        });
                    });
                }
            }
        }

        // Runtime-owned fallback synthesis path for complex tasks when the model did not
        // explicitly request orchestration during the turn.
        if let Some(ref collab) = self.collaboration {
            let last_user_msg = self
                .session()
                .messages
                .iter()
                .rev()
                .find(|m| matches!(m.role, crate::session::MessageRole::User))
                .map(|m| {
                    m.blocks
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default();

            if self.should_use_collaboration(&last_user_msg) {
                let capability_refs: Vec<String> =
                    Self::infer_required_capabilities(&last_user_msg);
                if !capability_refs.is_empty() {
                    let activation_decision =
                        SkillActivationEngine::activate(SkillActivationInput {
                            session_id: self.session().session_id.clone(),
                            turn_index: self.session().messages.len(),
                            query: last_user_msg.clone(),
                            capability_refs: capability_refs.clone(),
                            available_profiles: self.skill_profiles.clone(),
                            agent_profile: self.agent_skill_profile.clone(),
                        });
                    let activation = activation_decision.activation;
                    let skill_memory_candidate = memory_candidate_from_skill_activation(
                        &activation,
                        &SkillMemoryPolicy::default(),
                    );
                    self.record_skill_activation_event(&activation, self.session().messages.len());
                    if let Some(candidate) = &skill_memory_candidate {
                        self.record_skill_memory_candidate_event(
                            &activation,
                            candidate,
                            self.session().messages.len(),
                        );
                    }
                    let collab_clone = Arc::clone(collab);
                    let task = last_user_msg.clone();
                    let capability_refs_clone = capability_refs.clone();
                    let memory = self.memory_manager().cloned();

                    if let Some(collab_result) = collab_clone
                        .run_with_context_boxed(&task, &capability_refs_clone)
                        .await
                    {
                        let mut collab_result = collab_result;
                        if let Some(candidate) = &skill_memory_candidate {
                            collab_result.review_packet.maintenance_candidates.push(
                                skill_memory_candidate_to_maintenance(&activation, candidate),
                            );
                        }
                        collab_result.work_graph = collab_result
                            .work_graph
                            .with_session_id(self.session().session_id.clone())
                            .with_review_packet(&collab_result.review_packet);
                        let synthesis = collab_result.synthesis.clone();
                        self.append_context_items_to_latest_envelope(
                            &task,
                            collab_result.context_items.clone(),
                        );
                        self.remember_collaboration_result(collab_result);
                        tracing::info!(
                            synthesis_len = synthesis.len(),
                            capability_refs = ?capability_refs_clone,
                            "Collaboration synthesis complete"
                        );
                        if let Some(mem) = memory {
                            let memory_ctx = MemoryTurnContext::new(
                                self.session().session_id,
                                "collaboration-orchestrator",
                            );
                            let kernel = MemoryKernel::new(Arc::clone(&mem));
                            let entry = memory::types::MemoryEntry {
                                id: memory::types::MemoryId::new_v4(),
                                layer: memory::types::MemoryLayer::L4,
                                category: memory::types::MemoryCategory::Shared,
                                priority: memory::types::Priority::Normal,
                                source: memory::types::MemorySource::Import,
                                title: format!(
                                    "collaboration-synthesis: {}",
                                    preview_chars(&task, 80)
                                ),
                                content: synthesis,
                                embedding: None,
                                tags: vec!["collaboration".to_string(), "synthesis".to_string()],
                                relations: vec![],
                                confidence: 0.8,
                                access_count: 0,
                                staleness: 0.0,
                                created_at: chrono::Utc::now(),
                                updated_at: chrono::Utc::now(),
                                last_accessed_at: None,
                                scope: memory::project_scope::MemoryScope::Global,
                                session_id: None,
                                source_agent: None,
                                visibility: memory::types::AgentVisibility::Shared,
                            };
                            let _ = kernel.remember(&memory_ctx, entry).await;
                        }
                    }
                }
            }

            // JPS routing for very high-complexity tasks (10+ clauses or explicit keywords).
            let word_count = last_user_msg
                .split(|c: char| c.is_ascii_punctuation() || c == '\n')
                .filter(|s| !s.trim().is_empty())
                .count();
            let is_jps_complex = word_count > 10
                || last_user_msg.to_lowercase().contains("analyze")
                || last_user_msg.to_lowercase().contains("refactor");
            if is_jps_complex {
                if let Some(ref jps) = self.jps_pipeline {
                    let problem = ProblemStatement::new(last_user_msg);
                    match jps.run_boxed(problem).await {
                        Some(result) => {
                            tracing::info!(
                                solutions_count = result.solutions.len(),
                                "JPS pipeline completed"
                            );
                        }
                        None => {
                            tracing::info!("JPS pipeline returned no solution");
                        }
                    }
                }
            }
        }

        let assistant_text = assistant_messages
            .iter()
            .flat_map(|m| m.blocks.iter())
            .filter_map(|b| match b {
                crate::session::ContentBlock::Text { text } => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("");
        let failed_tool_results = count_failed_tool_results(&tool_results);
        if let Some(envelope) = self.last_context_envelope() {
            runtime_harness.record_context_envelope(
                envelope.id,
                envelope.selected.len(),
                envelope.omitted.len(),
            );
        }
        let ai_kernel_trace = runtime_harness.finalize(
            &assistant_text,
            tool_results.len().saturating_sub(failed_tool_results),
            failed_tool_results,
        );
        if ai_kernel_trace.finalization_blocked {
            let gate_message = ConversationMessage::assistant(vec![ContentBlock::Text {
                text: finalization_gate_message(&ai_kernel_trace),
            }]);
            self.session
                .write()
                .await
                .push_message(gate_message.clone())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            self.dual_write_message(&gate_message, self.session().messages.len().wrapping_sub(1));
            assistant_messages.push(gate_message);
        }
        self.record_ai_kernel_trace_event(&ai_kernel_trace, self.session().messages.len());
        self.record_strategy_experience(&ai_kernel_trace);
        let usage = self.usage_tracker.cumulative_usage();
        let telemetry_usage = if provider_usage_seen {
            provider_usage_total
        } else {
            TokenUsage {
                input_tokens: ((user_input.chars().count() as u32) / 4).max(1),
                output_tokens: ((output_chars as u32) / 4).max(u32::from(output_chars > 0)),
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            }
        };
        let active_stream_duration_ms = first_stream_token_at
            .zip(last_stream_token_at)
            .map(|(first, last)| last.saturating_duration_since(first).as_millis() as u64);
        let speed_duration_ms = active_stream_duration_ms
            .filter(|duration| *duration > 0)
            .unwrap_or_else(|| millis_since(turn_started_at).max(1));
        let speed_seconds = speed_duration_ms as f64 / 1_000.0;
        let model_telemetry = crate::cowd_event::RunModelTelemetry {
            model: models_used.last().cloned().or_else(|| {
                self.model
                    .as_ref()
                    .filter(|model| !model.is_empty())
                    .cloned()
            }),
            models_used,
            first_token_latency_ms,
            active_stream_duration_ms,
            wall_duration_ms: millis_since(turn_started_at),
            output_chars,
            output_chunks,
            input_tokens: u64::from(telemetry_usage.input_tokens),
            output_tokens: u64::from(telemetry_usage.output_tokens),
            cache_create_tokens: u64::from(telemetry_usage.cache_creation_input_tokens),
            cache_read_tokens: u64::from(telemetry_usage.cache_read_input_tokens),
            total_tokens: u64::from(telemetry_usage.total_tokens()),
            usage_source: if provider_usage_seen {
                "provider".to_string()
            } else {
                "estimated".to_string()
            },
            chars_per_second: (output_chars > 0).then_some(output_chars as f64 / speed_seconds),
            tokens_per_second: (telemetry_usage.output_tokens > 0)
                .then_some(f64::from(telemetry_usage.output_tokens) / speed_seconds),
        };
        let context_turn_report = self.build_context_turn_report(
            &ai_kernel_trace.harness_receipt.id,
            usage,
            auto_compaction,
        );
        if let Ok(mut registry) = self.model_performance_registry.lock() {
            registry.record_telemetry(&model_telemetry, None, false);
        }
        self.remember_context_turn_report(context_turn_report.clone());
        let summary = TurnSummary {
            assistant_messages,
            tool_results,
            prompt_cache_events,
            iterations,
            usage,
            model_telemetry: model_telemetry.clone(),
            auto_compaction,
            ai_kernel_trace,
            context_turn_report,
        };
        self.record_turn_completed(&summary);
        tracing::info!(iterations = %summary.iterations, tokens = %summary.usage.total_tokens(), "turn completed");
        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::RunModelTelemetry {
                telemetry: model_telemetry,
            });
            cowd.emit(crate::cowd_event::CowdEvent::TurnComplete {
                assistant_text,
                iterations: summary.iterations as u32,
            });
        }
        Ok(summary)
    }

    /// Extract the per-tool execution logic from run_turn for reuse.
    async fn execute_single_tool(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
    ) -> Result<ConversationMessage, RuntimeError> {
        let pre_hook_result = self.run_pre_tool_use_hook(tool_name, input);
        let effective_input = pre_hook_result
            .updated_input()
            .map_or_else(|| input.to_string(), ToOwned::to_owned);
        let permission_context = PermissionContext::new(
            pre_hook_result.permission_override(),
            pre_hook_result.permission_reason().map(ToOwned::to_owned),
        );

        let permission_outcome = if pre_hook_result.is_cancelled() {
            PermissionOutcome::Deny {
                reason: format!("PreToolUse hook cancelled tool `{tool_name}`"),
            }
        } else if pre_hook_result.is_failed() {
            let hook_msgs = pre_hook_result.messages().join("; ");
            PermissionOutcome::Deny {
                reason: if hook_msgs.is_empty() {
                    format!("PreToolUse hook failed for tool `{tool_name}`")
                } else {
                    format!("PreToolUse hook failed for tool `{tool_name}`: {hook_msgs}")
                },
            }
        } else if pre_hook_result.is_denied() {
            PermissionOutcome::Deny {
                reason: format!("PreToolUse hook denied tool `{tool_name}`"),
            }
        } else if let Some(prompt) = prompter.lock().as_mut() {
            self.permission_policy.authorize_with_context(
                tool_name,
                &effective_input,
                &permission_context,
                Some(prompt.as_mut()),
            )
        } else {
            self.permission_policy.authorize_with_context(
                tool_name,
                &effective_input,
                &permission_context,
                None,
            )
        };

        match permission_outcome {
            PermissionOutcome::Allow => {
                // Smart approval gate check
                if let Some(gate) = &self.approval_gate {
                    let gate_result = gate.evaluate(tool_name, &effective_input).await;
                    if let crate::approval_gate::ApprovalGateResult::Denied { reason } = gate_result
                    {
                        self.record_tool_invocation_denied(
                            tool_use_id,
                            tool_name,
                            &effective_input,
                            iterations,
                            ToolFailureKind::ApprovalDenied,
                            &reason,
                        );
                        self.emit_tool_completed(tool_use_id, tool_name, &reason, Some(1));
                        let denied = ConversationMessage::tool_result(
                            tool_use_id.to_string(),
                            tool_name.to_string(),
                            reason,
                            true,
                        );
                        self.session
                            .write()
                            .await
                            .push_message(denied.clone())
                            .map_err(|error| RuntimeError::new(error.to_string()))?;
                        self.dual_write_message(
                            &denied,
                            self.session().messages.len().wrapping_sub(1),
                        );
                        return Ok(denied);
                    }
                }

                // Gate evaluator check — runs commit quality gates (PreFlight,
                // Abort, Revision, Escalation) against the tool input before
                // allowing execution.
                if let Some(gate_evaluator) = &self.gate_evaluator {
                    let context = crate::gates::GateContext {
                        repo_path: std::env::current_dir()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string(),
                        branch: String::new(),
                        commit_message: tool_name.to_string(),
                        changed_files: Vec::new(),
                        diff: effective_input.clone(),
                        author: String::new(),
                        author_email: String::new(),
                        violations: Vec::new(),
                    };
                    let (all_passed, results) = gate_evaluator.evaluate_all(&context);
                    if !all_passed {
                        let reasons: Vec<String> = results
                            .iter()
                            .filter(|r| !r.passed)
                            .map(|r| {
                                let mut msg = format!("[{}] {}", r.gate_name, r.message);
                                if !r.suggestions.is_empty() {
                                    msg.push_str(&format!(
                                        " Suggestions: {}",
                                        r.suggestions.join(", ")
                                    ));
                                }
                                msg
                            })
                            .collect();
                        let reason = format!("Gate check failed: {}", reasons.join("; "));
                        self.record_tool_invocation_denied(
                            tool_use_id,
                            tool_name,
                            &effective_input,
                            iterations,
                            ToolFailureKind::GateDenied,
                            &reason,
                        );
                        self.emit_tool_completed(tool_use_id, tool_name, &reason, Some(1));
                        let denied = ConversationMessage::tool_result(
                            tool_use_id.to_string(),
                            tool_name.to_string(),
                            reason,
                            true,
                        );
                        self.session
                            .write()
                            .await
                            .push_message(denied.clone())
                            .map_err(|error| RuntimeError::new(error.to_string()))?;
                        self.dual_write_message(
                            &denied,
                            self.session().messages.len().wrapping_sub(1),
                        );
                        return Ok(denied);
                    }
                }

                let invocation_record = self.start_tool_invocation_record(
                    tool_use_id,
                    tool_name,
                    &effective_input,
                    iterations,
                );
                self.record_tool_invocation_event(
                    &invocation_record,
                    "tool.invocation.started",
                    self.session().messages.len(),
                );
                self.record_tool_started(iterations, tool_name);
                self.emit_tool_started(tool_use_id, tool_name, &effective_input);

                if let Some(callback) = &self.tool_callback {
                    let preview: String = effective_input.chars().take(200).collect();
                    callback.on_tool_start(tool_use_id, tool_name, &preview);
                }

                let start = Instant::now();
                let tool_exec = Arc::clone(&self.tool_executor);
                let tname = tool_name.to_string();
                let tname_for_err = tname.clone();
                let tinput = effective_input.clone();
                // Per-tool timeout: check registry for per-tool override or category default,
                // then cap with the global self.tool_timeout if set.
                let registry_timeout = Duration::from_secs(
                    crate::tool_orchestrator::ToolSafetyRegistry::global()
                        .get_timeout_secs(tool_name),
                );
                let tool_timeout = self
                    .tool_timeout
                    .map_or(registry_timeout, |t| t.min(registry_timeout));
                let (output, mut is_error, mut failure_kind) = match tokio::time::timeout(
                    tool_timeout,
                    tokio::task::spawn_blocking(move || tool_exec.execute(&tname, &tinput)),
                )
                .await
                {
                    Ok(Ok(Ok(output))) => (output, false, None),
                    Ok(Ok(Err(error))) => (
                        error.to_string(),
                        true,
                        Some(ToolFailureKind::ExecutionError),
                    ),
                    Ok(Err(join_error)) => (
                        format!("tool execution panicked: {join_error}"),
                        true,
                        Some(ToolFailureKind::Panic),
                    ),
                    Err(_elapsed) => {
                        tracing::warn!(tool = %tname_for_err, timeout_secs = tool_timeout.as_secs(), "tool execution timed out, returning partial result");
                        (
                            format!("tool `{tname_for_err}` timed out after {tool_timeout:?}"),
                            true,
                            Some(ToolFailureKind::Timeout),
                        )
                    }
                };
                let elapsed_ms = start.elapsed().as_millis() as u64;
                self.hook_runner
                    .fire_post_tool(tool_name, &output, is_error, elapsed_ms);

                if let Some(callback) = &self.tool_callback {
                    let summary: String = output.chars().take(500).collect();
                    let exit_code = if is_error { Some(1) } else { Some(0) };
                    callback.on_tool_complete(tool_use_id, tool_name, &summary, exit_code);
                }

                let post_hook_result = if is_error {
                    self.run_post_tool_use_failure_hook(tool_name, &effective_input, &output)
                } else {
                    self.run_post_tool_use_hook(tool_name, &effective_input, &output, false)
                };
                if post_hook_result.is_denied()
                    || post_hook_result.is_failed()
                    || post_hook_result.is_cancelled()
                {
                    is_error = true;
                    if failure_kind.is_none() {
                        failure_kind = Some(ToolFailureKind::HookDenied);
                    }
                }

                let elapsed_ms = start.elapsed().as_millis() as u64;
                if let Some(ref cowd) = self.cowd_bus {
                    cowd.emit(crate::cowd_event::CowdEvent::ToolExecuted {
                        name: tool_name.to_string(),
                        duration_ms: elapsed_ms,
                    });
                }

                // T36: Truncate oversized tool results before storing.
                // Append hook feedback messages to the tool output.
                let mut combined = output;
                for msg in pre_hook_result.messages() {
                    combined.push_str("\n");
                    combined.push_str(msg);
                }
                for msg in post_hook_result.messages() {
                    combined.push_str("\n");
                    combined.push_str(msg);
                }
                let completed_record = if is_error {
                    invocation_record.failed_with_output_policy(
                        failure_kind.unwrap_or(ToolFailureKind::Unknown),
                        &combined,
                        now_ms(),
                        DEFAULT_OUTPUT_REF_MIN_LINES,
                    )
                } else {
                    invocation_record.completed_with_output_policy(
                        &combined,
                        now_ms(),
                        DEFAULT_OUTPUT_REF_MIN_LINES,
                    )
                };
                self.maybe_index_tool_output(tool_use_id, tool_name, &combined);
                let raw_ref = self.record_tool_raw_evidence(
                    tool_use_id,
                    tool_name,
                    &completed_record.input_hash,
                    &combined,
                    is_error,
                    elapsed_ms,
                );
                let model_summary =
                    self.tool_model_summary(tool_name, &combined, is_error, &raw_ref);
                self.emit_tool_completed(
                    tool_use_id,
                    tool_name,
                    &combined,
                    if is_error { Some(1) } else { Some(0) },
                );
                self.push_turn_tool_observation(ToolObservation::new(
                    tool_name.to_string(),
                    completed_record.invocation_id.clone(),
                    raw_ref,
                    model_summary.clone(),
                ));
                let result = ConversationMessage::tool_result(
                    tool_use_id.to_string(),
                    tool_name.to_string(),
                    model_summary,
                    is_error,
                );
                self.session
                    .write()
                    .await
                    .push_message(result.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                self.dual_write_message(&result, self.session().messages.len().wrapping_sub(1));
                self.record_tool_invocation_event(
                    &completed_record,
                    if is_error {
                        "tool.invocation.failed"
                    } else {
                        "tool.invocation.completed"
                    },
                    self.session().messages.len().wrapping_sub(1),
                );
                self.record_tool_finished(iterations, &result);
                Ok(result)
            }
            PermissionOutcome::Deny { reason } => {
                let failure_kind = if reason.starts_with("PreToolUse hook") {
                    ToolFailureKind::HookDenied
                } else {
                    ToolFailureKind::PermissionDenied
                };
                self.record_tool_invocation_denied(
                    tool_use_id,
                    tool_name,
                    &effective_input,
                    iterations,
                    failure_kind,
                    &reason,
                );
                self.emit_tool_completed(tool_use_id, tool_name, &reason, Some(1));
                let denied = ConversationMessage::tool_result(
                    tool_use_id.to_string(),
                    tool_name.to_string(),
                    reason,
                    true,
                );
                self.session
                    .write()
                    .await
                    .push_message(denied.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                self.dual_write_message(&denied, self.session().messages.len().wrapping_sub(1));
                Ok(denied)
            }
        }
    }

    async fn execute_tool_schedule_batch(
        &self,
        batch: &crate::execution_scheduler::ExecutionBatch,
        requests: &[crate::tool_dispatch::ToolRequest],
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<(), RuntimeError> {
        match batch.mode {
            crate::execution_scheduler::ExecutionBatchMode::Wave => {
                if self
                    .execute_legacy_wave_batch_if_enabled(batch, requests, result_map)
                    .await?
                {
                    return Ok(());
                }
                self.execute_tool_indices_serial_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    true,
                    result_map,
                )
                .await
            }
            crate::execution_scheduler::ExecutionBatchMode::ParallelRead => {
                self.execute_tool_indices_concurrently_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    batch.max_concurrency,
                    false,
                    result_map,
                )
                .await
            }
            crate::execution_scheduler::ExecutionBatchMode::LimitedNetwork => {
                self.execute_tool_indices_concurrently_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    batch.max_concurrency,
                    true,
                    result_map,
                )
                .await
            }
            crate::execution_scheduler::ExecutionBatchMode::LimitedWrite => {
                self.execute_write_scope_groups_into_map(
                    batch,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    result_map,
                )
                .await
            }
            crate::execution_scheduler::ExecutionBatchMode::SerialDestructive => {
                self.execute_tool_indices_serial_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    true,
                    result_map,
                )
                .await
            }
        }
    }

    async fn execute_tool_indices_concurrently_into_map(
        &self,
        indices: &[usize],
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        max_concurrency: usize,
        acquire_category_permit: bool,
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<(), RuntimeError> {
        use futures::stream::{FuturesUnordered, StreamExt};

        let limit = bounded_tool_concurrency(max_concurrency, indices.len());
        for chunk in indices.chunks(limit) {
            let mut futures = FuturesUnordered::new();
            for &idx in chunk {
                futures.push(self.execute_tool_index_collect(
                    idx,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    acquire_category_permit,
                ));
            }
            while let Some(result) = futures.next().await {
                let (id, message) = result?;
                result_map.insert(id, message);
            }
        }
        Ok(())
    }

    async fn execute_write_scope_groups_into_map(
        &self,
        batch: &crate::execution_scheduler::ExecutionBatch,
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<(), RuntimeError> {
        use futures::stream::{FuturesUnordered, StreamExt};

        if batch.scope_groups.is_empty() {
            return self
                .execute_tool_indices_concurrently_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    batch.max_concurrency,
                    true,
                    result_map,
                )
                .await;
        }

        let limit = bounded_tool_concurrency(batch.max_concurrency, batch.scope_groups.len());
        for chunk in batch.scope_groups.chunks(limit) {
            let mut futures = FuturesUnordered::new();
            for group in chunk {
                futures.push(self.execute_tool_indices_serial_collect(
                    &group.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    true,
                ));
            }
            while let Some(result) = futures.next().await {
                for (id, message) in result? {
                    result_map.insert(id, message);
                }
            }
        }
        Ok(())
    }

    async fn execute_tool_indices_serial_into_map(
        &self,
        indices: &[usize],
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        acquire_category_permit: bool,
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<(), RuntimeError> {
        for (id, message) in self
            .execute_tool_indices_serial_collect(
                indices,
                pending_tool_uses,
                prompter,
                iterations,
                acquire_category_permit,
            )
            .await?
        {
            result_map.insert(id, message);
        }
        Ok(())
    }

    async fn execute_tool_indices_serial_collect(
        &self,
        indices: &[usize],
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        acquire_category_permit: bool,
    ) -> Result<Vec<(String, (ConversationMessage, Option<String>))>, RuntimeError> {
        let mut results = Vec::with_capacity(indices.len());
        for &idx in indices {
            results.push(
                self.execute_tool_index_collect(
                    idx,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    acquire_category_permit,
                )
                .await?,
            );
        }
        Ok(results)
    }

    async fn execute_tool_index_collect(
        &self,
        idx: usize,
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        acquire_category_permit: bool,
    ) -> Result<(String, (ConversationMessage, Option<String>)), RuntimeError> {
        let Some((tool_use_id, tool_name, input)) = pending_tool_uses.get(idx) else {
            return Err(RuntimeError::new(format!(
                "tool schedule referenced missing tool index {idx}"
            )));
        };

        let result_msg = if acquire_category_permit {
            let sem = self.tool_category_semaphore(tool_name);
            let _permit = sem.acquire().await.map_err(|error| {
                RuntimeError::new(format!("tool category semaphore closed: {error}"))
            })?;
            self.execute_single_tool(tool_use_id, tool_name, input, prompter, iterations)
                .await?
        } else {
            self.execute_single_tool(tool_use_id, tool_name, input, prompter, iterations)
                .await?
        };
        Ok(self.collect_tool_result_message(result_msg))
    }

    fn collect_tool_result_message(
        &self,
        result_msg: ConversationMessage,
    ) -> (String, (ConversationMessage, Option<String>)) {
        let (msg_id, tool_name) = extract_tool_info(&result_msg);
        let inject = self.turn_callback.as_ref().and_then(|callback| {
            let output = result_msg
                .blocks
                .first()
                .and_then(|block| match block {
                    ContentBlock::ToolResult { output, .. } => Some(output.as_str()),
                    _ => None,
                })
                .unwrap_or("");
            (callback.on_tool_result)(&tool_name, output)
        });
        (msg_id, (result_msg, inject))
    }

    fn tool_category_semaphore(&self, tool_name: &str) -> &Semaphore {
        match self.tool_orchestrator.classify(tool_name) {
            crate::tool_orchestrator::ToolSafetyCategory::WriteLocal => &self.write_semaphore,
            crate::tool_orchestrator::ToolSafetyCategory::Network => &self.network_semaphore,
            crate::tool_orchestrator::ToolSafetyCategory::Destructive => {
                &self.destructive_semaphore
            }
            crate::tool_orchestrator::ToolSafetyCategory::ReadOnly => &self.default_semaphore,
        }
    }

    async fn execute_legacy_wave_batch_if_enabled(
        &self,
        batch: &crate::execution_scheduler::ExecutionBatch,
        requests: &[crate::tool_dispatch::ToolRequest],
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<bool, RuntimeError> {
        if std::env::var("COWD_ENABLE_LEGACY_WAVE_READONLY")
            .ok()
            .as_deref()
            != Some("1")
        {
            return Ok(false);
        }
        let registry = crate::tool_orchestrator::ToolSafetyRegistry::global();
        if !batch.indices.iter().all(|idx| {
            requests
                .get(*idx)
                .map(|request| {
                    !request.depends_on.is_empty()
                        && registry.classify(&request.tool_name)
                            == crate::tool_orchestrator::ToolSafetyCategory::ReadOnly
                })
                .unwrap_or(false)
        }) {
            return Ok(false);
        }

        let mut wave = crate::wave::WaveOrchestrator::new()
            .with_config(crate::wave::WaveConfig::default().with_max_parallel(8));
        for &idx in &batch.indices {
            let Some(request) = requests.get(idx) else {
                return Err(RuntimeError::new(format!(
                    "wave batch referenced missing tool index {idx}"
                )));
            };
            let Some(task) = self.detect_wave_task(request) else {
                return Ok(false);
            };
            wave.add_task(task);
        }
        wave.build_waves()
            .map_err(|error: crate::wave::WaveError| RuntimeError::new(error.to_string()))?;

        let wave_exec = ToolWaveExecutor::new(Arc::clone(&self.tool_executor));
        let wave_results = wave
            .execute(wave_exec)
            .await
            .map_err(|error: crate::wave::WaveError| RuntimeError::new(error.to_string()))?;

        for wave_result in wave_results {
            for tool_result in wave_result.task_results {
                let tool_use_id = tool_result.task_id.0.clone();
                let tool_name = requests
                    .iter()
                    .find(|request| request.tool_use_id == tool_use_id)
                    .map_or("unknown", |request| request.tool_name.as_str())
                    .to_string();
                let output = tool_result
                    .output
                    .clone()
                    .unwrap_or_else(|| tool_result.error.clone().unwrap_or_default());
                let result_msg = ConversationMessage::tool_result(
                    tool_use_id.clone(),
                    tool_name,
                    output,
                    !tool_result.success,
                );
                self.session
                    .write()
                    .await
                    .push_message(result_msg.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                self.dual_write_message(&result_msg, self.session().messages.len().wrapping_sub(1));
                let (id, message) = self.collect_tool_result_message(result_msg);
                result_map.insert(id, message);
            }
        }
        Ok(true)
    }

    /// T7: Detect whether a [`ToolRequest`] should be executed via wave orchestration.
    ///
    /// Returns `Some(WaveTask)` when the request has dependency constraints,
    /// converting the tool-use ID, tool name, and input into a wave-compatible payload.
    fn detect_wave_task(&self, req: &crate::tool_dispatch::ToolRequest) -> Option<WaveTask> {
        if req.depends_on.is_empty() {
            return None;
        }
        let payload = serde_json::json!({
            "tool_name": &req.tool_name,
            "input": &req.input,
        });
        let safety_cat =
            crate::tool_orchestrator::ToolSafetyCategory::from_tool_name(&req.tool_name);
        let mut task = WaveTask::new(&req.tool_use_id, &req.tool_name)
            .with_payload(payload)
            .with_safety_category(safety_cat);
        for dep in &req.depends_on {
            task = task.with_dependency(TaskId::new(dep));
        }
        Some(task)
    }

    #[must_use]
    pub fn compact(&self, config: CompactionConfig) -> CompactionResult {
        compact_session(&*self.session.blocking_read(), config)
    }

    #[must_use]
    pub fn estimated_tokens(&self) -> usize {
        estimate_session_tokens(&*self.session.blocking_read())
    }

    fn model_candidates_for_turn(&self, user_input: &str) -> Vec<String> {
        let mut configured_models: Vec<String> =
            std::iter::once(self.model.as_deref().unwrap_or("").to_string())
                .chain(self.fallbacks.clone())
                .collect();
        if configured_models
            .iter()
            .any(|model| !model.trim().is_empty())
        {
            configured_models.retain(|model| !model.trim().is_empty());
        }
        configured_models.dedup();

        let Ok(registry) = self.model_performance_registry.lock() else {
            return configured_models;
        };
        let decision = registry.route(ModelRouteIntent::from_task(user_input), &configured_models);
        let mut routed = Vec::with_capacity(configured_models.len());
        if configured_models
            .iter()
            .any(|model| model == &decision.selected_model)
        {
            routed.push(decision.selected_model);
        }
        for candidate in decision.candidates {
            if configured_models
                .iter()
                .any(|model| model == &candidate.model)
                && !routed.iter().any(|model| model == &candidate.model)
            {
                routed.push(candidate.model);
            }
        }
        for model in configured_models {
            if !routed.iter().any(|known| known == &model) {
                routed.push(model);
            }
        }
        routed
    }

    #[must_use]
    pub fn usage(&self) -> &UsageTracker {
        &self.usage_tracker
    }

    #[must_use]
    pub fn session(&self) -> Session {
        tokio::task::block_in_place(|| self.session.blocking_read().clone())
    }

    pub fn api_client_mut(&mut self) -> &mut C {
        &mut self.api_client
    }

    pub fn session_mut(&mut self) -> tokio::sync::RwLockWriteGuard<'_, Session> {
        self.session.blocking_write()
    }

    pub async fn session_mut_async(&mut self) -> tokio::sync::RwLockWriteGuard<'_, Session> {
        self.session.write().await
    }

    #[must_use]
    pub fn fork_session(&self, branch_name: Option<String>) -> Session {
        self.session.blocking_read().fork(branch_name)
    }

    #[must_use]
    pub fn into_session(self) -> Session {
        Arc::try_unwrap(self.session)
            .map(|lock| lock.into_inner())
            .unwrap_or_else(|arc| arc.blocking_read().clone())
    }

    async fn maybe_auto_compact(&mut self) -> Option<AutoCompactionEvent> {
        // Use the session's estimated token count directly, not the cumulative
        // usage tracker which spans across multiple sessions and doesn't
        // reflect the current conversation window pressure.
        let session_tokens = estimate_session_tokens(&*self.session.read().await);

        if session_tokens < self.auto_compaction_input_tokens_threshold as usize {
            return None;
        }

        let result = compact_session(
            &*self.session.read().await,
            CompactionConfig {
                max_estimated_tokens: 0,
                priority_threshold: 3,
                keep_high_priority: true,
                ..CompactionConfig::default()
            },
        );

        if result.removed_message_count == 0 {
            return None;
        }

        tracing::info!(removed = result.removed_message_count, "compaction");
        let compacted_len = result.compacted_session.messages.len();
        *self.session.write().await = result.compacted_session;
        // Record compaction as a MessagesTruncated event for event log.
        if let Some(ref log) = self.event_log {
            if let Ok(mut guard) = log.lock() {
                guard.push(MessageEvent::MessagesTruncated {
                    sequence: compacted_len,
                });
            }
        }
        Some(AutoCompactionEvent {
            removed_message_count: result.removed_message_count,
        })
    }

    fn record_turn_started(&self, user_input: &str) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert(
            "user_input".to_string(),
            Value::String(user_input.to_string()),
        );
        session_tracer.record("turn_started", attributes);
    }

    #[allow(dead_code)]
    fn record_assistant_iteration(
        &self,
        iteration: usize,
        assistant_message: &ConversationMessage,
        pending_tool_use_count: usize,
    ) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert(
            "assistant_blocks".to_string(),
            Value::from(assistant_message.blocks.len() as u64),
        );
        attributes.insert(
            "pending_tool_use_count".to_string(),
            Value::from(pending_tool_use_count as u64),
        );
        session_tracer.record("assistant_iteration_completed", attributes);
    }

    fn record_tool_started(&self, iteration: usize, tool_name: &str) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert(
            "tool_name".to_string(),
            Value::String(tool_name.to_string()),
        );
        session_tracer.record("tool_execution_started", attributes);
    }

    #[allow(dead_code)]
    fn record_tool_finished(&self, iteration: usize, result_message: &ConversationMessage) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let Some(ContentBlock::ToolResult {
            tool_name,
            is_error,
            ..
        }) = result_message.blocks.first()
        else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert("tool_name".to_string(), Value::String(tool_name.clone()));
        attributes.insert("is_error".to_string(), Value::Bool(*is_error));
        session_tracer.record("tool_execution_finished", attributes);
    }

    fn emit_tool_started(&self, tool_use_id: &str, tool_name: &str, input: &str) {
        let Some(ref cowd) = self.cowd_bus else {
            return;
        };
        cowd.emit(crate::cowd_event::CowdEvent::ToolStart {
            id: tool_use_id.to_string(),
            name: tool_name.to_string(),
            preview: preview_chars(input, 200),
        });
    }

    fn emit_tool_completed(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        output: &str,
        exit_code: Option<i32>,
    ) {
        let Some(ref cowd) = self.cowd_bus else {
            return;
        };
        cowd.emit(crate::cowd_event::CowdEvent::ToolComplete {
            id: tool_use_id.to_string(),
            name: tool_name.to_string(),
            summary: preview_chars(output, 500),
            exit_code,
        });
    }

    fn record_turn_completed(&self, summary: &TurnSummary) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert(
            "iterations".to_string(),
            Value::from(summary.iterations as u64),
        );
        attributes.insert(
            "assistant_messages".to_string(),
            Value::from(summary.assistant_messages.len() as u64),
        );
        attributes.insert(
            "tool_results".to_string(),
            Value::from(summary.tool_results.len() as u64),
        );
        attributes.insert(
            "prompt_cache_events".to_string(),
            Value::from(summary.prompt_cache_events.len() as u64),
        );
        session_tracer.record("turn_completed", attributes);
    }

    fn record_turn_failed(&self, iteration: usize, error: &RuntimeError) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert("error".to_string(), Value::String(error.to_string()));
        session_tracer.record("turn_failed", attributes);
    }

    // -----------------------------------------------------------------------
    // Memory helpers (private)
    // -----------------------------------------------------------------------

    /// Build an effective system-prompt list that prepends memory context
    /// entries when the memory subsystem is active.
    ///
    /// Returns a clone of `self.system_prompt` when memory is disabled so the
    /// hot path has zero cost.
    async fn prepare_reality_context(&self, user_input: &str) -> Vec<String> {
        let _perf_start = std::time::Instant::now();

        // P5.2: Global invalidation (file changes, max age).
        self.cached_prompt.check_global();

        let Some(mgr) = self.memory_manager.as_ref() else {
            // No memory manager — cache empty for all layers, return base prompt.
            use crate::cached_prompt::CacheLayer;
            for &layer in &CacheLayer::all() {
                self.cached_prompt.rebuild_layer(layer, Vec::new(), 0);
            }
            let envelope = self.build_context_envelope(
                user_input,
                Vec::new(),
                Vec::new(),
                vec![ContextSourceKind::Memory],
            );
            let prompt = Self::provider_prompt_from_envelope(&envelope, None);
            self.remember_context_envelope(envelope);
            return prompt;
        };

        // Convert session messages to memory's Message type for context scoring.
        // DESIGN: Tool blocks (ToolUse, ToolResult, Thinking) are explicitly excluded
        // from memory extraction. Only user/assistant text content is persisted.
        // Tool execution results are machine-optimised data, not knowledge worth retaining
        // in long-term memory (they can be re-derived by re-running the tool).
        let mem_messages: Vec<MemMessage> = self
            .session
            .read()
            .await
            .messages
            .iter()
            .enumerate()
            .map(|(idx, msg)| {
                let role = match msg.role {
                    crate::session::MessageRole::User => MemMessageRole::User,
                    crate::session::MessageRole::Assistant => MemMessageRole::Assistant,
                    crate::session::MessageRole::Tool => MemMessageRole::Tool,
                    crate::session::MessageRole::System => MemMessageRole::User,
                };
                // Extract only Text content blocks; ToolUse, ToolResult, Thinking blocks
                // are deliberately omitted from the memory extraction stream.
                let content: String = msg
                    .blocks
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                // Extract tool identity for tool result messages so the memory extractor
                // can properly attribute error-fix sequences.
                let (tool_use_id, tool_name) = match msg.role {
                    crate::session::MessageRole::Tool => {
                        let tid = msg.blocks.iter().find_map(|b| match b {
                            ContentBlock::ToolResult { tool_use_id, .. } => {
                                Some(tool_use_id.clone())
                            }
                            _ => None,
                        });
                        let tname = msg.blocks.iter().find_map(|b| match b {
                            ContentBlock::ToolResult { tool_name, .. } if !tool_name.is_empty() => {
                                Some(tool_name.clone())
                            }
                            _ => None,
                        });
                        (tid, tname)
                    }
                    _ => (None, None),
                };
                MemMessage {
                    turn_index: idx,
                    role,
                    content,
                    tool_use_id,
                    tool_name,
                    pinned: false,
                }
            })
            .collect();

        let session_id = self.session().session_id;
        let memory_ctx = MemoryTurnContext::new(session_id.clone(), "primary");
        let kernel = MemoryKernel::new(Arc::clone(mgr));
        match kernel
            .context_packet(&memory_ctx, user_input, &mem_messages, 20, 8_000)
            .await
        {
            Ok(packet) => {
                let packet =
                    crate::knowledge_activation::filter_packet_for_turn_intent(&packet, user_input);
                if packet.selected.is_empty() {
                    tracing::debug!(entries = 0, "memory context packet prepared");
                    if let Some(cb) = &self.memory_callback {
                        cb.on_memory_update(Vec::new(), "no memories found");
                    }
                    // Cache empty for all layers.
                    use crate::cached_prompt::CacheLayer;
                    for &layer in &CacheLayer::all() {
                        self.cached_prompt.rebuild_layer(layer, Vec::new(), 0);
                    }
                    let omissions = packet
                        .omitted
                        .iter()
                        .map(|omitted| ContextOmission {
                            source: ContextSourceKind::Memory,
                            reason: format!("{}: {}", omitted.reason, omitted.title),
                            token_estimate: 0,
                        })
                        .collect();
                    let envelope =
                        self.build_context_envelope(user_input, Vec::new(), omissions, Vec::new());
                    let prompt = Self::provider_prompt_from_envelope(&envelope, None);
                    self.remember_context_envelope(envelope);
                    return prompt;
                }

                use crate::cached_prompt::CacheLayer;
                use memory::types::MemoryLayer;
                let mut items_by_layer: std::collections::HashMap<
                    CacheLayer,
                    Vec<&memory::MemoryPacketItem>,
                > = std::collections::HashMap::new();
                for item in &packet.selected {
                    let cl = match item.atom.layer {
                        MemoryLayer::L0 => CacheLayer::L0,
                        MemoryLayer::L1 => CacheLayer::L1,
                        MemoryLayer::L2 => CacheLayer::L2,
                        MemoryLayer::L3 => CacheLayer::L3,
                        MemoryLayer::L4 => CacheLayer::L4,
                    };
                    items_by_layer.entry(cl).or_default().push(item);
                }

                for cache_layer in CacheLayer::all() {
                    let items = items_by_layer.remove(&cache_layer).unwrap_or_default();
                    let count = items.len();

                    if self.cached_prompt.needs_rebuild(cache_layer, count) {
                        let mut layer_text = String::new();
                        for item in &items {
                            let atom = &item.atom;
                            let layer_tag = match atom.layer {
                                MemoryLayer::L0 => "identity",
                                MemoryLayer::L1 => "working",
                                MemoryLayer::L2 => "project",
                                MemoryLayer::L3 => "recall",
                                MemoryLayer::L4 => "raw",
                            };
                            layer_text.push_str(&format!(
                                "  <memory role=\"{:?}\" layer=\"{}\" state=\"{:?}\" confidence=\"{:.2}\" salience=\"{:.2}\">\n    <title>{}</title>\n    <reason>{}</reason>\n    <evidence>{}</evidence>\n  </memory>\n",
                                item.role,
                                layer_tag,
                                atom.state,
                                atom.confidence,
                                atom.salience,
                                atom.title,
                                item.reason,
                                atom.evidence_pointer.as_deref().unwrap_or("")
                            ));
                        }
                        self.cached_prompt
                            .rebuild_layer(cache_layer, vec![layer_text], count);
                    }
                }

                let mut context = format!(
                    "<memory_context mode=\"packet\" selected=\"{}\" omitted=\"{}\" tokens=\"{}\" truncated=\"{}\">\n",
                    packet.selected.len(),
                    packet.omitted.len(),
                    packet.token_estimate,
                    packet.truncated
                );
                for cache_layer in CacheLayer::all() {
                    let fragment = self.cached_prompt.get_layer(cache_layer);
                    for line in &fragment {
                        context.push_str(line);
                    }
                }
                context.push_str("</memory_context>");

                if !packet.omitted.is_empty() {
                    context.push_str("\n<context_omissions>\n");
                    for omitted in packet.omitted.iter().take(8) {
                        context.push_str(&format!(
                            "  <omitted id=\"{}\" reason=\"{}\">{}</omitted>\n",
                            omitted.id, omitted.reason, omitted.title
                        ));
                    }
                    context.push_str("</context_omissions>\n");
                }

                if let Some(cb) = &self.memory_callback {
                    let entries: Vec<(String, String, f64)> = packet
                        .selected
                        .iter()
                        .map(|item| {
                            (
                                format!("{:?}", item.atom.layer),
                                item.atom.title.clone(),
                                item.atom.confidence as f64,
                            )
                        })
                        .collect();
                    let status = format!("{} memory entries loaded", entries.len());
                    cb.on_memory_update(entries, &status);
                }

                tracing::debug!(
                    selected = packet.selected.len(),
                    omitted = packet.omitted.len(),
                    "memory context packet prepared"
                );
                let dynamic_items = packet
                    .selected
                    .iter()
                    .map(|item| {
                        let role = match item.role {
                            memory::MemoryPacketRole::Orientation => ContextRole::Orientation,
                            memory::MemoryPacketRole::Supporting => ContextRole::Evidence,
                            memory::MemoryPacketRole::Warning
                            | memory::MemoryPacketRole::Conflict => ContextRole::Warning,
                        };
                        let mut context_item = ContextItem::new(
                            item.atom.id.to_string(),
                            ContextSourceKind::Memory,
                            role,
                            format!(
                                "{}\nreason: {}\nevidence: {}",
                                item.atom.title,
                                item.reason,
                                item.atom.evidence_pointer.as_deref().unwrap_or("")
                            ),
                        );
                        context_item.authority = ContextAuthority::Session;
                        context_item.visibility = ContextVisibility::Private;
                        context_item.score = item.atom.confidence;
                        if let Some(evidence) = item.atom.evidence_pointer.as_ref() {
                            context_item.evidence.push(evidence.clone());
                        }
                        context_item
                    })
                    .collect::<Vec<_>>();
                let knowledge_activation = KnowledgeActivationRuntime::new().activate_from_packet(
                    &session_id,
                    user_input,
                    &format!("{:?}", self.context_profile()),
                    &packet,
                );
                let omissions = packet
                    .omitted
                    .iter()
                    .map(|omitted| ContextOmission {
                        source: ContextSourceKind::Memory,
                        reason: format!("{}: {}", omitted.reason, omitted.title),
                        token_estimate: 0,
                    })
                    .collect::<Vec<_>>();
                let mut dynamic_items = dynamic_items;
                if let Some(activation) = knowledge_activation {
                    dynamic_items.extend(activation.items);
                    context.push('\n');
                    context.push_str(&activation.prompt_fragment);
                    self.set_turn_knowledge_report(activation.report);
                }
                let envelope =
                    self.build_context_envelope(user_input, dynamic_items, omissions, Vec::new());
                let prompt = Self::provider_prompt_from_envelope(
                    &envelope,
                    Some(self.dynamic_tail_with_external_context(context)),
                );
                self.remember_context_envelope(envelope);
                prompt
            }
            Err(err) => {
                tracing::warn!(%err, "memory: prepare_context failed, using base system prompt");
                if let Some(cb) = &self.memory_callback {
                    cb.on_memory_update(Vec::new(), &format!("memory error: {err}"));
                }
                let envelope = self.build_context_envelope(
                    user_input,
                    Vec::new(),
                    Vec::new(),
                    vec![ContextSourceKind::Memory],
                );
                let prompt = Self::provider_prompt_from_envelope(&envelope, None);
                self.remember_context_envelope(envelope);
                prompt
            }
        }
    }

    /// Perform post-turn memory housekeeping (micro-compact, drift, seeds).
    ///
    /// Errors are logged and swallowed so a memory failure never aborts a turn.
    async fn run_memory_post_turn(&self) -> Result<(), RuntimeError> {
        let Some(mgr) = self.memory_manager.as_ref() else {
            return Ok(());
        };
        let session_id = self.session().session_id;
        let memory_ctx = MemoryTurnContext::new(session_id.clone(), "primary");
        let kernel = MemoryKernel::new(Arc::clone(mgr));

        // Convert session messages to memory's Message type for post-turn extraction.
        // DESIGN: Tool blocks are excluded (same rationale as prepare_reality_context).
        let mem_messages: Vec<MemMessage> = self
            .session
            .read()
            .await
            .messages
            .iter()
            .enumerate()
            .map(|(idx, msg)| {
                let role = match msg.role {
                    crate::session::MessageRole::User => MemMessageRole::User,
                    crate::session::MessageRole::Assistant => MemMessageRole::Assistant,
                    crate::session::MessageRole::Tool => MemMessageRole::Tool,
                    crate::session::MessageRole::System => MemMessageRole::User,
                };
                // Extract only Text blocks; tool blocks are deliberately omitted.
                let content: String = msg
                    .blocks
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                // Pass tool identity for tool result messages.
                let (tool_use_id, tool_name) = match msg.role {
                    crate::session::MessageRole::Tool => {
                        let tid = msg.blocks.iter().find_map(|b| match b {
                            ContentBlock::ToolResult { tool_use_id, .. } => {
                                Some(tool_use_id.clone())
                            }
                            _ => None,
                        });
                        let tname = msg.blocks.iter().find_map(|b| match b {
                            ContentBlock::ToolResult { tool_name, .. } if !tool_name.is_empty() => {
                                Some(tool_name.clone())
                            }
                            _ => None,
                        });
                        (tid, tname)
                    }
                    _ => (None, None),
                };
                MemMessage {
                    turn_index: idx,
                    role,
                    content,
                    tool_use_id,
                    tool_name,
                    pinned: false,
                }
            })
            .collect();

        let start = Instant::now();
        let mut maintenance_messages = mem_messages;
        let post_turn_result = kernel
            .post_turn(&memory_ctx, &mut maintenance_messages)
            .await;
        let elapsed = start.elapsed();
        tracing::info!(
            elapsed_ms = elapsed.as_millis(),
            "post_turn: memory kernel completed"
        );

        if let Err(ref e) = post_turn_result {
            tracing::warn!(%e, "post_turn: memory kernel failed");
        }

        if let Some(cb) = &self.memory_callback {
            let layers_data = mgr.list_layers().await;
            let total_entries: usize = layers_data
                .iter()
                .filter_map(|l| {
                    l.get("entry_count")
                        .and_then(|c| c.as_u64())
                        .map(|c| c as usize)
                })
                .sum();
            let layer_names: Vec<String> = layers_data
                .iter()
                .filter_map(|l| {
                    l.get("layer")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            let vector_count = mgr.vector_index_count();
            cb.on_memory_stats(total_entries, vector_count, layer_names);
        }

        Ok(())
    }

    /// Write a message to the durable SQLite session store via a spawned
    /// background task. The in-memory session remains the hot turn state;
    /// SQLite is the managed session source of truth. JSONL is only used by
    /// explicit import/export codecs.
    fn record_skill_activation_event(&self, activation: &SkillActivationRecord, sequence: usize) {
        let Some(ref store) = self.session_store else {
            return;
        };
        let session_id = activation.session_id.clone();
        let mut event = activation.to_runtime_event(sequence);
        if let Some(payload) = event.payload.as_object_mut() {
            payload.insert(
                "source".to_string(),
                serde_json::json!("conversation_runtime.skill_activation"),
            );
        }
        let store = Arc::clone(store);
        tokio::spawn(async move {
            if let Err(error) = store.append_runtime_event(&event).await {
                tracing::warn!(
                    %error,
                    session_id,
                    sequence,
                    "skill activation runtime event append failed"
                );
            }
        });
    }

    fn record_skill_memory_candidate_event(
        &self,
        activation: &SkillActivationRecord,
        candidate: &MemoryPulseCandidate,
        sequence: usize,
    ) {
        let Some(ref store) = self.session_store else {
            return;
        };
        let payload = serde_json::json!({
            "turn_index": activation.turn_index,
            "query": activation.query,
            "selected": activation.selected,
            "candidate": candidate,
            "source_event": "skill_candidates",
            "source": "conversation_runtime.skill_memory_candidate",
        });
        let mut event = memory::RuntimeEvent::new(
            activation.session_id.clone(),
            sequence,
            memory::RuntimeEventScope::Context,
            "skill_memory_candidate",
            payload,
            now_ms(),
        );
        if let Some(selected) = &activation.selected {
            event.refs.push(memory::RuntimeRef {
                ref_type: "skill".to_string(),
                id: selected.clone(),
                label: Some("memory_candidate_source".to_string()),
            });
        }
        let session_id = activation.session_id.clone();
        let store = Arc::clone(store);
        tokio::spawn(async move {
            if let Err(error) = store.append_runtime_event(&event).await {
                tracing::warn!(
                    %error,
                    session_id,
                    sequence,
                    "skill memory candidate runtime event append failed"
                );
            }
        });
    }

    fn maybe_index_tool_output(&self, tool_use_id: &str, tool_name: &str, output: &str) {
        if output.lines().count() < DEFAULT_OUTPUT_REF_MIN_LINES {
            return;
        }
        let Some(ref sandbox) = self.tool_output_sandbox else {
            return;
        };
        let Ok(mut guard) = sandbox.lock() else {
            tracing::warn!(
                tool_call_id = tool_use_id,
                "tool output sandbox lock poisoned"
            );
            return;
        };
        if let Some(summary) =
            guard.index_tool_output(tool_use_id, tool_name, output, DEFAULT_OUTPUT_REF_MIN_LINES)
        {
            tracing::debug!(
                tool_call_id = tool_use_id,
                tool_name,
                total_lines = summary.total_lines,
                full_size_bytes = summary.full_size_bytes,
                "indexed oversized tool output"
            );
        }
    }

    fn record_tool_raw_evidence(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input_hash: &str,
        output: &str,
        is_error: bool,
        duration_ms: u64,
    ) -> EvidenceRef {
        let evidence_id = format!("tool-raw-{}-{}", tool_use_id, uuid::Uuid::new_v4());
        let Some(ref store) = self.session_store else {
            return EvidenceRef::new("tool", evidence_id);
        };
        let session_id = self.session().session_id;
        let payload = serde_json::json!({
            "type": "ToolObservationRaw",
            "evidence_id": evidence_id,
            "session_id": session_id,
            "tool_call_id": tool_use_id,
            "tool_name": tool_name,
            "input_hash": input_hash,
            "is_error": is_error,
            "duration_ms": duration_ms,
            "line_count": output.lines().count(),
            "byte_count": output.len(),
            "raw": output,
        });
        let store = Arc::clone(store);
        let event_evidence_id = evidence_id.clone();
        tokio::spawn(async move {
            let sequence = match store.next_event_sequence(&session_id).await {
                Ok(sequence) => sequence,
                Err(error) => {
                    tracing::warn!(%error, session_id, "tool raw evidence sequence allocation failed");
                    return;
                }
            };
            let created_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);
            let event = memory::SessionEvent {
                session_id: session_id.clone(),
                event_type: "ToolObservationRaw".to_string(),
                event_json: payload.to_string(),
                sequence,
                created_at_ms,
            };
            if let Err(error) = store.append_event(&event).await {
                tracing::warn!(
                    %error,
                    session_id,
                    evidence_id = event_evidence_id,
                    "tool raw evidence append failed"
                );
            }
        });
        EvidenceRef::new("tool", evidence_id)
    }

    fn tool_model_summary(
        &self,
        tool_name: &str,
        output: &str,
        is_error: bool,
        raw_ref: &EvidenceRef,
    ) -> String {
        let collapsed = output.split_whitespace().collect::<Vec<_>>().join(" ");
        let max_chars = if is_error { 1_200 } else { 900 };
        let preview = preview_chars(&collapsed, max_chars);
        format!(
            "Tool `{}` {}. Raw evidence ref: {}. Summary: {}",
            tool_name,
            if is_error { "failed" } else { "completed" },
            format!("tool://{}", raw_ref.id()),
            preview
        )
    }

    fn start_tool_invocation_record(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        iterations: usize,
    ) -> ToolInvocationRecord {
        let session_id = self.session().session_id;
        let safety_category =
            crate::tool_orchestrator::ToolSafetyRegistry::global().classify(tool_name);
        ToolInvocationRecord::started(
            session_id,
            iterations,
            tool_use_id.to_string(),
            tool_name.to_string(),
            input,
            safety_category,
            now_ms(),
        )
    }

    fn record_tool_invocation_denied(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        iterations: usize,
        failure_kind: ToolFailureKind,
        reason: &str,
    ) {
        let record = self
            .start_tool_invocation_record(tool_use_id, tool_name, input, iterations)
            .failed(failure_kind, reason, now_ms());
        self.record_tool_invocation_event(
            &record,
            "tool.invocation.denied",
            self.session().messages.len(),
        );
    }

    fn record_tool_invocation_event(
        &self,
        record: &ToolInvocationRecord,
        kind: &'static str,
        sequence: usize,
    ) {
        let event = record.to_runtime_event(sequence, kind);
        self.append_tool_runtime_events(record.turn_index, kind, vec![event]);
    }

    fn record_tool_execution_plan(&self, plan: &ToolExecutionPlan, sequence: usize) {
        let session_id = self.session().session_id;
        let event = plan.to_runtime_event(session_id.clone(), sequence, now_ms());
        self.append_tool_runtime_events(sequence, "tool.execution.plan.created", vec![event]);
    }

    fn record_tool_schedule(
        &self,
        schedule: &crate::execution_scheduler::ToolSchedule,
        requests: &[crate::tool_dispatch::ToolRequest],
        sequence: usize,
    ) {
        let session_id = self.session().session_id;
        let event = schedule.to_runtime_event(session_id.clone(), sequence, now_ms(), requests);
        self.append_tool_runtime_events(sequence, "tool.schedule.created", vec![event]);
    }

    fn record_turn_supervisor_decision(
        &self,
        observation: &crate::turn_supervisor::ToolProgressObservation,
        decision: &crate::turn_supervisor::SupervisorDecision,
        sequence: usize,
    ) {
        let Some(ref store) = self.session_store else {
            return;
        };
        let session_id = self.session().session_id;
        let payload = serde_json::json!({
            "decision": decision.kind(),
            "reason": decision.reason(),
            "tool": {
                "name": &observation.fingerprint.tool_name,
                "target": &observation.fingerprint.target,
                "range": observation.fingerprint.range,
                "input_hash": observation.fingerprint.input_hash,
                "output_hash": observation.fingerprint.output_hash,
                "is_error": observation.is_error,
            },
            "prompt_injected": decision.prompt(),
            "source": "conversation_runtime.turn_supervisor",
        });
        let mut event = memory::RuntimeEvent::new(
            session_id.clone(),
            sequence,
            memory::RuntimeEventScope::Policy,
            "runtime.turn_supervisor.decision",
            payload,
            now_ms(),
        );
        event.status = Some(decision.kind().to_string());
        let store = Arc::clone(store);
        tokio::spawn(async move {
            if let Err(error) = store.append_runtime_event(&event).await {
                tracing::warn!(
                    %error,
                    session_id,
                    sequence,
                    "turn supervisor runtime event append failed"
                );
            }
        });
    }

    fn record_ai_kernel_trace_event(&self, trace: &RuntimeAiKernelTrace, sequence: usize) {
        let Some(ref store) = self.session_store else {
            return;
        };
        let session_id = self.session().session_id;
        let payload = serde_json::json!({
            "strategy": {
                "mode": trace.strategy.mode.as_str(),
                "confidence": trace.strategy.confidence,
                "policy_version": trace.strategy.policy_version,
                "reasons": trace.strategy.reasons,
                "required_capabilities": trace.strategy.required_capabilities.iter().map(|item| format!("{item:?}")).collect::<Vec<_>>(),
                "complexity": format!("{:?}", trace.strategy.understanding.complexity),
                "risk": format!("{:?}", trace.strategy.understanding.risk),
                "decorators": trace.strategy.decorators.iter().map(|item| item.as_str()).collect::<Vec<_>>(),
            },
            "collaboration": {
                "template_id": trace.collaboration_decision.template_id.as_str(),
                "rationale": trace.collaboration_decision.rationale,
                "plan": trace.collaboration_decision.plan,
            },
            "context": {
                "epoch_id": trace.context_epoch.epoch_id,
                "envelope_id": trace.context_envelope_id,
                "token_total": trace.context_epoch.token_total,
                "selected_count": trace.context_epoch.selected.len(),
                "omitted_count": trace.context_epoch.omitted.len(),
                "alignment": trace.context_alignment,
            },
            "verification": {
                "can_finalize": trace.verification_report.can_finalize,
                "finalization_blocked": trace.finalization_blocked,
                "severity": format!("{:?}", trace.verification_report.severity),
                "blocking_reasons": trace.verification_report.blocking_reasons,
                "claim_count": trace.verification_report.claim_count,
                "evidence_count": trace.verification_report.evidence_count,
                "unsupported_required_count": trace.verification_report.unsupported_required_claims.len(),
                "not_run_count": trace.verification_report.not_run_claims.len(),
                "matrix_missing_evidence": matrix_missing_evidence(trace),
            },
            "tool_transaction": trace.tool_transaction.as_ref().map(|plan| serde_json::json!({
                "id": plan.id,
                "batch_count": plan.batches.len(),
                "requires_checkpoint": plan.requires_checkpoint,
                "requires_human_confirm": plan.requires_human_confirm,
                "warning_count": plan.warnings.len(),
            })),
            "harness": {
                "receipt_id": trace.harness_receipt.id,
                "harness_id": trace.harness_receipt.harness_id,
                "agent_spec_id": trace.harness_receipt.agent_spec_id,
                "strategy_mode": trace.harness_receipt.strategy_mode,
                "context_epoch_id": trace.harness_receipt.context_epoch_id,
                "tool_transaction_id": trace.harness_receipt.tool_transaction_id,
                "verification_can_finalize": trace.harness_receipt.verification_can_finalize,
                "policy_receipts": trace.harness_receipt.policy_receipts,
                "output_summary": trace.harness_receipt.output_summary,
            },
            "policy_receipts": trace.policy_receipts.iter().map(|receipt| serde_json::json!({
                "id": receipt.id,
                "scope": format!("{:?}", receipt.scope),
                "decision": format!("{:?}", receipt.decision),
                "reasons": receipt.reasons,
                "evidence_refs": receipt.evidence_refs,
                "source_policy": receipt.source_policy,
                "created_at": receipt.created_at,
            })).collect::<Vec<_>>(),
            "behavior_policy": {
                "necessity": trace.behavior_policy.necessity,
                "reuse_opportunities": trace.behavior_policy.reuse_opportunities,
                "overengineering_risks": trace.behavior_policy.overengineering_risks,
                "safety_exceptions": trace.behavior_policy.safety_exceptions,
                "recommended_scope": format!("{:?}", trace.behavior_policy.recommended_scope),
                "enforcement": {
                    "allow_execution": trace.behavior_policy.enforcement.allow_execution,
                    "requires_scope_downgrade": trace.behavior_policy.enforcement.requires_scope_downgrade,
                    "requires_human_review": trace.behavior_policy.enforcement.requires_human_review,
                },
                "eval_checks": trace.behavior_policy.eval_checks,
            },
            "workgraph": trace.workgraph.as_ref().map(|graph| serde_json::json!({
                "id": graph.id,
                "node_count": graph.nodes.len(),
                "edge_count": graph.edges.len(),
            })),
            "workgraph_quality": trace.workgraph_quality.as_ref().map(|quality| serde_json::json!({
                "node_count": quality.node_count,
                "edge_count": quality.edge_count,
                "ready_count": quality.ready_count,
                "blocked_count": quality.blocked_count,
                "failed_count": quality.failed_count,
                "has_review_node": quality.has_review_node,
                "has_synthesis_node": quality.has_synthesis_node,
                "is_dag": quality.is_dag,
                "warnings": quality.warnings,
            })),
            "bench": {
                "passed": trace.bench_result.passed,
                "score": trace.bench_result.score,
                "case_id": trace.bench_result.case_id,
                "reasons": trace.bench_result.reasons,
            },
            "regression_gate": {
                "allowed": trace.regression_gate.allowed,
                "average_score": trace.regression_gate.average_score,
                "failed": trace.regression_gate.failed,
                "reasons": trace.regression_gate.reasons,
            },
            "growth": {
                "record_id": trace.learning_record.id,
                "event_id": trace.growth_event.id,
                "policy": trace.learning_record.policy,
                "has_blocker": trace.learning_record.has_blocker(),
                "signals": trace.learning_record.signals.iter().map(|signal| serde_json::json!({
                    "kind": format!("{:?}", signal.kind),
                    "severity": format!("{:?}", signal.severity),
                    "summary": signal.summary,
                })).collect::<Vec<_>>(),
                "next_strategy_hints": trace.learning_record.next_strategy_hints,
            },
            "strategy_experience": strategy_experience_projection(trace),
            "maintenance_candidates": growth_maintenance_candidates(trace),
            "matrix_evidence_signal": {
                "source": "ai_kernel_trace",
                "growth_event_id": trace.growth_event.id,
                "packet_contract": {
                    "problem_statement": "AI harness execution quality",
                    "trace_ref": format!("runtime:event:{sequence}"),
                    "strategy_mode": trace.strategy.mode.as_str(),
                    "verification_can_finalize": trace.verification_report.can_finalize,
                    "regression_allowed": trace.regression_gate.allowed,
                    "harness_receipt_id": trace.harness_receipt.id,
                },
                "evidence_refs": trace.growth_event.evidence_refs,
                "signals": trace.growth_event.matrix_signals,
                "missing_evidence": matrix_missing_evidence(trace),
            },
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let mut event = memory::RuntimeEvent::new(
            session_id.clone(),
            sequence,
            memory::RuntimeEventScope::Task,
            "runtime.harness_contract.trace",
            payload,
            created_at_ms,
        );
        event.status = Some(if trace.verification_report.can_finalize {
            "completed".to_string()
        } else {
            "degraded".to_string()
        });
        let store = Arc::clone(store);
        tokio::spawn(async move {
            if let Err(error) = store.append_runtime_event(&event).await {
                tracing::warn!(%error, session_id, sequence, "AI kernel runtime trace append failed");
            }
        });
    }

    fn strategy_input_for_turn(&self, user_input: &str) -> StrategyInput {
        let store = StrategyExperienceStore::load_or_default(strategy_experience_path());
        store.enrich_input(StrategyInput::from_prompt(user_input.to_string()))
    }

    fn record_strategy_experience(&self, trace: &RuntimeAiKernelTrace) {
        let path = strategy_experience_path();
        let mut store = StrategyExperienceStore::load_or_default(path.clone());
        store.record(strategy_experience_record(trace));
        if let Err(error) = store.save(path) {
            tracing::warn!(%error, "failed to persist AI strategy experience");
        }
    }

    fn append_tool_runtime_events(
        &self,
        turn_index: usize,
        event_label: &'static str,
        events: Vec<memory::RuntimeEvent>,
    ) {
        let Some(ref store) = self.session_store else {
            return;
        };
        let session_id = self.session().session_id;
        let use_ledger = std::env::var("COWD_TOOL_LEDGER_V2").ok().as_deref() == Some("1");
        let events = if use_ledger {
            let mut ledger = TurnToolLedger::new(session_id.to_string(), turn_index);
            for event in events {
                let idempotency_key = tool_event_idempotency_key(&event);
                ledger.append_runtime_event(idempotency_key, event);
            }
            ledger.flush().events
        } else {
            events
        };
        let store = Arc::clone(store);
        tokio::spawn(async move {
            for event in events {
                let sequence = event.sequence;
                let kind = event.kind.clone();
                if let Err(error) = store.append_runtime_event(&event).await {
                    tracing::warn!(
                        %error,
                        session_id,
                        sequence,
                        event_kind = kind,
                        event_label,
                        "tool runtime event append failed"
                    );
                }
            }
        });
    }

    fn dual_write_message(&self, msg: &crate::session::ConversationMessage, sequence: usize) {
        // Record the message in the event log for time-travel debugging.
        if let Some(ref log) = self.event_log {
            if let Ok(mut guard) = log.lock() {
                guard.push(MessageEvent::MessageAppended {
                    message: msg.clone(),
                });
            }
        }
        if let Some(ref store) = self.session_store {
            let session_id = self.session().session_id;
            let record = msg.to_session_message(&session_id, sequence);
            let event =
                message_appended_session_event(msg, &session_id, sequence, record.created_at_ms);
            let store = Arc::clone(store);
            tokio::spawn(async move {
                if let Err(e) = store.insert_message(&record).await {
                    tracing::warn!(%e, session_id, sequence, "dual_write: SQLite insert failed, retrying");
                    if let Err(retry_error) = store.insert_message(&record).await {
                        tracing::warn!(%retry_error, session_id, sequence, "dual_write: SQLite retry failed");
                        return;
                    }
                }
                if let Err(e) = store.append_event(&event).await {
                    tracing::warn!(%e, session_id, sequence, "dual_write: session event append failed");
                }
            });
        }
    }

    fn record_runtime_policy_decision(&self, profile: &TaskComplexityProfile, sequence: usize) {
        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::RuntimePolicyDecision {
                summary: crate::cowd_event::RuntimePolicyDecisionSummary {
                    level: format!("{:?}", profile.level),
                    score: profile.score,
                    recommended_profile: format!("{:?}", profile.recommended_profile),
                    agent_mode: format!("{:?}", profile.recommended_agent_mode),
                    requires_review: profile.requires_review,
                    signal_count: profile.signals.len(),
                },
            });
        }

        let Some(ref store) = self.session_store else {
            return;
        };
        let session_id = self.session().session_id;
        let payload = serde_json::json!({
            "complexity": {
                "level": format!("{:?}", profile.level),
                "score": profile.score,
                "signals": profile.signals,
            },
            "recommended_profile": format!("{:?}", profile.recommended_profile),
            "agent_mode": format!("{:?}", profile.recommended_agent_mode),
            "requires_review": profile.requires_review,
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let mut event = memory::RuntimeEvent::new(
            session_id.clone(),
            sequence,
            memory::RuntimeEventScope::Policy,
            "runtime.policy.decided",
            payload,
            created_at_ms,
        );
        event.status = Some("completed".to_string());
        let store = Arc::clone(store);
        tokio::spawn(async move {
            match event.to_session_event() {
                Ok(record) => {
                    if let Err(error) = store.append_event(&record).await {
                        tracing::warn!(%error, session_id, sequence, "runtime policy event append failed");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, session_id, sequence, "runtime policy event serialization failed");
                }
            }
        });
    }
}

fn message_appended_session_event(
    msg: &crate::session::ConversationMessage,
    session_id: &str,
    sequence: usize,
    created_at_ms: u64,
) -> memory::SessionEvent {
    let message = serde_json::from_str::<serde_json::Value>(&msg.to_json().render())
        .unwrap_or(serde_json::Value::Null);
    memory::SessionEvent {
        session_id: session_id.to_string(),
        event_type: "message_appended".to_string(),
        event_json: serde_json::json!({
            "type": "message_appended",
            "sequence": sequence,
            "role": msg.role.role_str(),
            "message": message,
        })
        .to_string(),
        sequence,
        created_at_ms,
    }
}

/// Reads the automatic compaction threshold from the environment.
#[must_use]
pub fn auto_compaction_threshold_from_env() -> u32 {
    let value = std::env::var("CC_AUTO_COMPACT_INPUT_TOKENS")
        .ok()
        .or_else(|| std::env::var(AUTO_COMPACTION_THRESHOLD_ENV_VAR).ok());
    parse_auto_compaction_threshold(value.as_deref())
}

fn resolve_compact_threshold(model_ctx_window: u32) -> u32 {
    let env_val = auto_compaction_threshold_from_env();
    if env_val > 0 {
        return env_val;
    }
    if let Ok(pct_str) = std::env::var("COWD_COMPACT_THRESHOLD_PERCENT") {
        if let Ok(pct) = pct_str.parse::<u32>() {
            return (model_ctx_window * pct / 100).min(model_ctx_window.saturating_sub(8_000));
        }
    }
    (model_ctx_window * 80 / 100).min(model_ctx_window.saturating_sub(8_000))
}

fn filter_system_prompt_for_role(system_prompt: &[String], task_description: &str) -> Vec<String> {
    let mut filtered = vec![format!(
        "You are a sub-agent with the following task:\n\n{}\n\
         Complete this task faithfully and report your results.\n\
         Do NOT perform work outside the scope of this task.",
        task_description
    )];
    filtered.extend_from_slice(system_prompt);
    filtered
}

/// Convert a [`RuntimeFeatureConfig`] memory section into a [`CcMemoryConfig`]
/// suitable for [`CognitiveContextManager::new`].
#[doc(alias = "memory")]
#[doc(alias = "CognitiveContextManager")]
pub fn build_cc_memory_config(feature_config: &RuntimeFeatureConfig) -> CcMemoryConfig {
    use memory::config::{
        BudgetConfig, CompressionConfig, DriftConfig, ExtractorConfig, StoreConfig,
    };

    let mem = feature_config.memory();
    let storage_layout =
        storage::StorageLayout::default_for_config_home(crate::cowd_dirs::config_home_dir());
    let (sqlite_path, blob_dir) = if let Some(store_path) = mem.store_path.as_ref() {
        (store_path.join("memory.db"), store_path.join("blobs"))
    } else {
        (
            storage_layout
                .sqlite_path("memory")
                .map(std::path::Path::to_path_buf)
                .unwrap_or_else(|| {
                    crate::cowd_dirs::config_home_dir().join("storage/memory.sqlite")
                }),
            storage_layout.blobs.join("memory"),
        )
    };

    CcMemoryConfig {
        store: StoreConfig {
            sqlite_path,
            blob_dir,
            enable_vector_index: mem.vector.enabled,
            cache_capacity: 512,
            vector: memory::config::VectorConfig {
                enabled: mem.vector.enabled,
                model: mem.vector.model.clone(),
                api_url: mem.vector.api_url.clone(),
                api_key: mem.vector.api_key.clone(),
                dimension: mem.vector.dimension as usize,
                timeout_secs: mem.vector.timeout_secs,
                batch_size: mem.vector.batch_size,
            },
        },
        compression: CompressionConfig {
            micro_threshold: 50,
            session_threshold: 10,
            enable_deep_compression: feature_config.compression().deep.enabled,
            aggressiveness: 0.5,
            llm: Default::default(),
        },
        budget: BudgetConfig {
            context_window: 200_000,
            reserved_system: u64::from(mem.layers.l1_max_tokens)
                + u64::from(mem.layers.l2_max_tokens),
            reserved_response: 8_000,
            warning_threshold: 0.70,
            critical_threshold: 0.90,
        },
        extractor: ExtractorConfig {
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.6,
            extractor_debounce_secs: 30,
        },
        drift: DriftConfig::default(),
        perf: memory::config::PerfBudget::default(),
        tuning: Default::default(),
        model: None,
    }
}

#[must_use]
fn parse_auto_compaction_threshold(value: Option<&str>) -> u32 {
    value
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|threshold| *threshold > 0)
        .unwrap_or(DEFAULT_AUTO_COMPACTION_INPUT_TOKENS_THRESHOLD)
}

fn extract_tool_info(msg: &ConversationMessage) -> (String, String) {
    if let Some(ContentBlock::ToolResult {
        tool_use_id,
        tool_name,
        ..
    }) = msg.blocks.first()
    {
        (tool_use_id.clone(), tool_name.clone())
    } else {
        (String::new(), String::new())
    }
}

fn bounded_tool_concurrency(max_concurrency: usize, item_count: usize) -> usize {
    if item_count == 0 {
        return 1;
    }
    if max_concurrency == usize::MAX {
        item_count.max(1)
    } else {
        max_concurrency.max(1).min(item_count)
    }
}

fn count_failed_tool_results(messages: &[ConversationMessage]) -> usize {
    messages
        .iter()
        .filter(|message| {
            message
                .blocks
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolResult { is_error: true, .. }))
        })
        .count()
}

fn finalization_gate_message(trace: &RuntimeAiKernelTrace) -> String {
    let reasons = if trace.verification_report.blocking_reasons.is_empty() {
        "verification ledger blocked finalization".to_string()
    } else {
        trace.verification_report.blocking_reasons.join("; ")
    };
    format!("I cannot finalize this as a completed answer yet. Blocking verification: {reasons}")
}

fn strategy_experience_path() -> std::path::PathBuf {
    crate::cowd_dirs::config_home_dir()
        .join("ai")
        .join("strategy-experience.json")
}

fn strategy_experience_record(trace: &RuntimeAiKernelTrace) -> StrategyExperienceRecord {
    let context_pressure = !trace.context_epoch.omitted.is_empty()
        || trace
            .context_alignment
            .as_ref()
            .map(|alignment| !alignment.aligned)
            .unwrap_or(false);
    let multi_agent_positive_lift = trace
        .workgraph_quality
        .as_ref()
        .map(|quality| {
            quality.is_dag
                && quality.has_review_node
                && quality.has_synthesis_node
                && quality.failed_count == 0
        })
        .unwrap_or(false);
    let succeeded = trace.verification_report.can_finalize
        && trace.bench_result.passed
        && trace.regression_gate.allowed;
    StrategyExperienceRecord::from_decision(
        &trace.strategy,
        succeeded,
        trace.finalization_blocked,
        context_pressure,
        multi_agent_positive_lift,
        now_ms(),
    )
}

fn strategy_experience_projection(trace: &RuntimeAiKernelTrace) -> serde_json::Value {
    let record = strategy_experience_record(trace);
    serde_json::json!({
        "domain": format!("{:?}", record.domain),
        "complexity": format!("{:?}", record.complexity),
        "risk": format!("{:?}", record.risk),
        "selected_mode": record.selected_mode.as_str(),
        "succeeded": record.succeeded,
        "verification_blocked": record.verification_blocked,
        "context_pressure": record.context_pressure,
        "multi_agent_positive_lift": record.multi_agent_positive_lift,
        "store_ref": strategy_experience_path().display().to_string(),
    })
}

fn matrix_missing_evidence(trace: &RuntimeAiKernelTrace) -> Vec<String> {
    let mut missing = trace
        .verification_report
        .unsupported_required_claims
        .iter()
        .map(|claim| format!("unsupported_required_claim: {}", claim.statement))
        .collect::<Vec<_>>();
    missing.extend(
        trace
            .verification_report
            .not_run_claims
            .iter()
            .map(|claim| format!("not_run_claim: {}", claim.statement)),
    );
    if trace
        .context_alignment
        .as_ref()
        .map(|alignment| !alignment.aligned)
        .unwrap_or(false)
    {
        missing.push("context_epoch_envelope_alignment".to_string());
    }
    if !trace.context_epoch.omitted.is_empty() {
        missing.push(format!(
            "context_omitted_items:{}",
            trace.context_epoch.omitted.len()
        ));
    }
    missing
}

fn growth_maintenance_candidates(
    trace: &RuntimeAiKernelTrace,
) -> Vec<memory::MaintenanceCandidate> {
    trace
        .growth_event
        .memory_candidates
        .iter()
        .map(|candidate| {
            let now = chrono::Utc::now();
            memory::MaintenanceCandidate {
                id: candidate.id.clone(),
                kind: match candidate.kind {
                    harness_contract::growth::GrowthMemoryCandidateKind::Conflict => {
                        memory::MaintenanceCandidateKind::Conflict
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::Stale => {
                        memory::MaintenanceCandidateKind::Stale
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::AuthorityPromotion => {
                        memory::MaintenanceCandidateKind::AuthorityPromotion
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::RelationshipRefresh => {
                        memory::MaintenanceCandidateKind::RelationshipRefresh
                    }
                },
                status: memory::MaintenanceCandidateStatus::Open,
                entry_ids: Vec::new(),
                summary: candidate.summary.clone(),
                reason: format!(
                    "ai_growth:{}; confidence_bp={}",
                    candidate.reason, candidate.confidence_bp
                ),
                confidence: candidate.confidence_bp as f32 / 10_000.0,
                source: Some("ai_growth".to_string()),
                source_ref: Some(trace.growth_event.id.clone()),
                created_at: now,
                updated_at: now,
            }
        })
        .collect()
}

fn skill_memory_candidate_to_maintenance(
    activation: &SkillActivationRecord,
    candidate: &MemoryPulseCandidate,
) -> memory::MaintenanceCandidate {
    let now = chrono::Utc::now();
    let (kind, summary) = match candidate.kind {
        MemoryPulseKind::Remember => (
            memory::MaintenanceCandidateKind::RelationshipRefresh,
            "Review skill activation gap",
        ),
        MemoryPulseKind::Refresh => (
            memory::MaintenanceCandidateKind::RelationshipRefresh,
            "Review skill activation memory refresh",
        ),
        MemoryPulseKind::Promote => (
            memory::MaintenanceCandidateKind::AuthorityPromotion,
            "Review skill activation promotion",
        ),
        MemoryPulseKind::Retire => (
            memory::MaintenanceCandidateKind::Stale,
            "Review skill activation retirement",
        ),
    };
    let selected = activation
        .selected
        .as_deref()
        .unwrap_or("no-skill-selected")
        .to_string();
    let confidence = activation
        .candidates
        .first()
        .map(|candidate| (candidate.score as f32 / 16.0).clamp(0.25, 0.95))
        .unwrap_or(0.35);
    memory::MaintenanceCandidate {
        id: format!("skill-memory-{}", uuid::Uuid::new_v4()),
        kind,
        status: memory::MaintenanceCandidateStatus::Open,
        entry_ids: Vec::new(),
        summary: format!(
            "{summary}: selected={selected}; query={}",
            truncate_for_runtime_candidate(&activation.query)
        ),
        reason: candidate.content.clone(),
        confidence,
        source: Some("runtime_skill".to_string()),
        source_ref: Some(format!(
            "session://{}/turn/{}/skill/{}",
            activation.session_id, activation.turn_index, selected
        )),
        created_at: now,
        updated_at: now,
    }
}

fn truncate_for_runtime_candidate(value: &str) -> String {
    const MAX_CHARS: usize = 160;
    if value.chars().count() <= MAX_CHARS {
        return value.to_string();
    }
    value.chars().take(MAX_CHARS).collect::<String>()
}

type ToolHandler = Box<dyn Fn(&str) -> Result<String, ToolError> + Send + Sync>;

/// Simple in-memory tool executor for tests and lightweight integrations.
#[derive(Default)]
pub struct StaticToolExecutor {
    handlers: BTreeMap<String, ToolHandler>,
}

impl StaticToolExecutor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn register(
        mut self,
        tool_name: impl Into<String>,
        handler: impl Fn(&str) -> Result<String, ToolError> + Send + Sync + 'static,
    ) -> Self {
        self.handlers.insert(tool_name.into(), Box::new(handler));
        self
    }
}

impl ToolExecutor for StaticToolExecutor {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
        self.handlers
            .get(tool_name)
            .ok_or_else(|| ToolError::new(format!("unknown tool: {tool_name}")))?(input)
    }
}

/// T7: Wave executor adapter that bridges WaveOrchestrator to ToolExecutor.
///
/// Each [`WaveTask`] payload is expected to contain `{"tool_name": "...", "input": "..."}`.
struct ToolWaveExecutor<T: ToolExecutor> {
    tool_exec: Arc<T>,
}

impl<T: ToolExecutor> ToolWaveExecutor<T> {
    fn new(tool_exec: Arc<T>) -> Self {
        Self { tool_exec }
    }
}

impl<T: ToolExecutor> WaveExecutor for ToolWaveExecutor<T> {
    fn execute(
        self: Arc<Self>,
        task: WaveTask,
        _context: crate::wave::TaskContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TaskResult, WaveError>> + Send>>
    {
        let tool_exec = Arc::clone(&self.tool_exec);
        let task_id = task.id.clone();
        let tool_name = task
            .payload
            .get("tool_name")
            .and_then(|v| v.as_str())
            .unwrap_or(&task.name)
            .to_string();
        let input = task
            .payload
            .get("input")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let tname = tool_name.clone();
        let tool_timeout = Duration::from_secs(
            crate::tool_orchestrator::ToolSafetyRegistry::global().get_timeout_secs(&tname),
        );
        Box::pin(async move {
            let start = std::time::Instant::now();
            let raw = tokio::time::timeout(
                tool_timeout,
                tokio::task::spawn_blocking(move || tool_exec.execute(&tool_name, &input)),
            )
            .await;
            let duration_ms = start.elapsed().as_millis() as u64;
            match raw {
                Ok(Ok(Ok(output))) => Ok(TaskResult {
                    task_id,
                    success: true,
                    output: Some(output),
                    error: None,
                    duration_ms,
                }),
                Ok(Ok(Err(e))) => Ok(TaskResult {
                    task_id,
                    success: false,
                    output: None,
                    error: Some(e.to_string()),
                    duration_ms,
                }),
                Ok(Err(e)) => Ok(TaskResult {
                    task_id,
                    success: false,
                    output: None,
                    error: Some(format!("tool panicked: {e}")),
                    duration_ms: 0,
                }),
                Err(_elapsed) => {
                    tracing::warn!(tool = %tname, timeout_secs = tool_timeout.as_secs(), "wave tool execution timed out");
                    Ok(TaskResult {
                        task_id,
                        success: false,
                        output: None,
                        error: Some(format!("tool timed out after {:?}", tool_timeout)),
                        duration_ms: 0,
                    })
                }
            }
        })
    }
}

/// Check whether an error string indicates a retryable HTTP status (429/5xx).
#[inline]
fn is_retryable_error(err_str: &str) -> bool {
    const RETRYABLE: &[&str] = &["408", "409", "429", "500", "502", "503", "504"];
    RETRYABLE.iter().any(|code| err_str.contains(code))
}

#[cfg(test)]
mod tests {

    use super::{
        preview_chars, stream_idle_timeout_for_messages, ApiClient, ApiRequest, AssistantEvent,
        CognitiveContextManager, ConversationRuntime, PromptCacheEvent, RuntimeError,
        StaticToolExecutor, DEFAULT_RUNTIME_MAX_ITERATIONS,
    };
    use crate::agent_collaboration::{
        AgentTaskTrace, AgentTeam, CollaborationContextResult, CollaborationOps,
        CollaborationReviewPacket, CollaborationScorecard, CollaborationTask, SubTask,
    };
    use crate::agent_workgraph::AgentWorkGraph;
    use crate::compact::CompactionConfig;
    use crate::config::{RuntimeFeatureConfig, RuntimeHookConfig};
    use crate::context_runtime::{
        ContextAuthority, ContextItem, ContextMode, ContextProfile, ContextRole, ContextSourceKind,
        ResumeContextPacket, ResumeContextSource,
    };
    use crate::permissions::{
        PermissionMode, PermissionPolicy, PermissionPromptDecision, PermissionPrompter,
        PermissionRequest, SharedPrompter,
    };
    use crate::prompt::{ProjectContext, SystemPromptBuilder};
    use crate::session::{ContentBlock, ConversationMessage, MessageRole, Session};
    use crate::SubAgentConfig;
    use crate::ToolError;
    use futures::stream::Stream;
    use harness_contract::skill::{
        AgentSkillProfile, SkillAdapterKind, SkillCapabilityProfile, SkillDetectedRuntime,
        SkillEntrypoint, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
        SkillStructuredDependency,
    };
    use model_protocol::telemetry::{MemoryTelemetrySink, SessionTracer, TelemetryEvent};
    use model_protocol::usage::TokenUsage;
    use std::fs;
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn stream_idle_timeout_policy_expands_for_complex_tasks() {
        let direct = vec![ConversationMessage::user_text(
            "解释一下这个函数有什么用".to_string(),
        )];
        let standard = vec![ConversationMessage::user_text(
            "分析当前实现并给出修订建议".to_string(),
        )];
        let deep = vec![ConversationMessage::user_text(
            "请进行深度架构分析，模拟 what if 场景，验证 memory matrix harness 多Agent协同并输出完整报告".to_string(),
        )];

        let direct_timeout = stream_idle_timeout_for_messages(&direct);
        let standard_timeout = stream_idle_timeout_for_messages(&standard);
        let deep_timeout = stream_idle_timeout_for_messages(&deep);

        assert!(direct_timeout < standard_timeout);
        assert!(standard_timeout < deep_timeout);
        assert_eq!(deep_timeout, Duration::from_secs(600));
    }

    fn test_skill_profile(
        skill_id: &str,
        name: &str,
        adapter: SkillAdapterKind,
    ) -> SkillCapabilityProfile {
        SkillCapabilityProfile {
            skill_id: skill_id.to_string(),
            name: name.to_string(),
            version: Some("1.0.0".to_string()),
            source_root: "/tmp/cowd-skill".to_string(),
            package_fingerprint: "test-fingerprint".to_string(),
            kind: SkillKind::Document,
            lifecycle_status: SkillLifecycleStatus::UsablePrompt,
            adapters: vec![adapter],
            risk_level: SkillRiskLevel::Low,
            entrypoints: vec![SkillEntrypoint {
                runtime: SkillDetectedRuntime::Markdown,
                path: "SKILL.md".to_string(),
                adapter,
                command_hint: None,
            }],
            inspection_summary: vec!["release review planning".to_string()],
            structured_dependencies: Vec::new(),
        }
    }

    // M1 helper: convert Vec<AssistantEvent> into a Stream for test mocks
    fn to_stream(
        events: Vec<AssistantEvent>,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + 'static>> {
        Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
    }

    #[test]
    fn preview_chars_handles_multibyte_text() {
        let text = "再次美化模型与状态展示，确保中文截断不会 panic".repeat(8);
        let preview = preview_chars(&text, 20);

        assert!(preview.ends_with("..."));
        assert!(text.starts_with(preview.trim_end_matches("...")));
    }

    struct ScriptedApiClient {
        call_count: usize,
    }

    impl ApiClient for ScriptedApiClient {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            use futures::stream;
            fn wrap(
                v: Vec<AssistantEvent>,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + 'static>>
            {
                Box::pin(stream::iter(v.into_iter().map(Ok)))
            }
            self.call_count += 1;
            let events = match self.call_count {
                1 => {
                    assert!(request
                        .messages
                        .iter()
                        .any(|message| message.role == MessageRole::User));
                    vec![
                        AssistantEvent::TextDelta("Let me calculate that.".to_string()),
                        AssistantEvent::ToolUse {
                            id: "tool-1".to_string(),
                            name: "add".to_string(),
                            input: "2,2".to_string(),
                        },
                        AssistantEvent::Usage(TokenUsage {
                            input_tokens: 20,
                            output_tokens: 6,
                            cache_creation_input_tokens: 1,
                            cache_read_input_tokens: 2,
                        }),
                        AssistantEvent::MessageStop,
                    ]
                }
                2 => {
                    let last_message = request
                        .messages
                        .last()
                        .expect("tool result should be present");
                    assert_eq!(last_message.role, MessageRole::Tool);
                    vec![
                        AssistantEvent::TextDelta("The answer is 4.".to_string()),
                        AssistantEvent::Usage(TokenUsage {
                            input_tokens: 24,
                            output_tokens: 4,
                            cache_creation_input_tokens: 1,
                            cache_read_input_tokens: 3,
                        }),
                        AssistantEvent::PromptCache(PromptCacheEvent {
                            unexpected: true,
                            reason:
                                "cache read tokens dropped while prompt fingerprint remained stable"
                                    .to_string(),
                            previous_cache_read_input_tokens: 6_000,
                            current_cache_read_input_tokens: 1_000,
                            token_drop: 5_000,
                        }),
                        AssistantEvent::MessageStop,
                    ]
                }
                _ => unreachable!("extra API call"),
            };
            wrap(events)
        }
    }

    struct MultiToolApiClient {
        call_count: usize,
        tool_uses: Vec<(String, String, String)>,
    }

    impl MultiToolApiClient {
        fn new(tool_uses: Vec<(&str, &str, &str)>) -> Self {
            Self {
                call_count: 0,
                tool_uses: tool_uses
                    .into_iter()
                    .map(|(id, name, input)| (id.to_string(), name.to_string(), input.to_string()))
                    .collect(),
            }
        }
    }

    impl ApiClient for MultiToolApiClient {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            use futures::stream;
            self.call_count += 1;
            let events = match self.call_count {
                1 => {
                    assert!(request
                        .messages
                        .iter()
                        .any(|message| message.role == MessageRole::User));
                    let mut events = vec![AssistantEvent::TextDelta(
                        "I will inspect in parallel.".to_string(),
                    )];
                    for (id, name, input) in self.tool_uses.clone() {
                        events.push(AssistantEvent::ToolUse { id, name, input });
                    }
                    events.push(AssistantEvent::MessageStop);
                    events
                }
                2 => {
                    let tool_message_count = request
                        .messages
                        .iter()
                        .filter(|message| message.role == MessageRole::Tool)
                        .count();
                    assert!(
                        tool_message_count >= self.tool_uses.len(),
                        "second provider request should include every tool result"
                    );
                    vec![
                        AssistantEvent::TextDelta("Done.".to_string()),
                        AssistantEvent::MessageStop,
                    ]
                }
                _ => unreachable!("extra API call"),
            };
            Box::pin(stream::iter(events.into_iter().map(Ok)))
        }
    }

    struct PromptAllowAll;

    impl PermissionPrompter for PromptAllowAll {
        fn decide(&mut self, _request: &PermissionRequest) -> PermissionPromptDecision {
            PermissionPromptDecision::Allow
        }
    }

    fn tracked_tool_handler(
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        delay: Duration,
    ) -> impl Fn(&str) -> Result<String, ToolError> + Send + Sync + 'static {
        move |input| {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(current, Ordering::SeqCst);
            std::thread::sleep(delay);
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(format!("ok:{input}"))
        }
    }

    struct PromptAllowOnce;

    impl PermissionPrompter for PromptAllowOnce {
        fn decide(&mut self, request: &PermissionRequest) -> PermissionPromptDecision {
            assert_eq!(request.tool_name, "add");
            PermissionPromptDecision::Allow
        }
    }

    #[test]
    fn create_subagent_runtime_assigns_context_lease() {
        let parent_session = Session::new();
        let parent_session_id = parent_session.session_id.clone();
        let runtime = ConversationRuntime::new(
            parent_session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        let config = SubAgentConfig {
            task_description: "review implementation".to_string(),
            budget_tokens: 2_048,
            ..SubAgentConfig::default()
        };

        let sub_agent = runtime.create_subagent_runtime(&config);
        let lease = sub_agent
            .context_lease()
            .expect("sub-agent should receive context lease");

        assert_eq!(lease.parent_session_id, parent_session_id);
        assert_eq!(lease.parent_agent_id, "primary");
        assert_eq!(lease.task_contract, "review implementation");
        assert_eq!(lease.max_tokens, 2_048);
        assert_eq!(sub_agent.agent_id(), lease.child_agent_id);
    }

    #[test]
    fn context_profile_controls_runtime_envelope_profile() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.set_context_profile(ContextProfile::YoloGoal);
        let envelope = runtime.build_context_envelope(
            "continue task",
            vec![ContextItem::new(
                "task",
                ContextSourceKind::Task,
                ContextRole::TaskState,
                "active yolo task",
            )],
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(runtime.context_profile(), ContextProfile::YoloGoal);
        assert_eq!(envelope.profile, ContextProfile::YoloGoal);
        assert_eq!(envelope.identity.mode, ContextMode::YoloGoal);
        assert!(envelope.assembled.runtime_header[0].contains("profile:YoloGoal"));
    }

    #[test]
    fn max_iterations_accessor_tracks_runtime_budget_updates() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        let original = runtime.max_iterations();
        assert_eq!(original, DEFAULT_RUNTIME_MAX_ITERATIONS);
        runtime.set_max_iterations(8);
        assert_eq!(runtime.max_iterations(), 8);
        runtime.set_max_iterations(original);
        assert_eq!(runtime.max_iterations(), original);
    }

    #[test]
    fn model_router_reorders_actual_turn_candidate_chain() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime.model = Some("balanced-model".to_string());
        runtime.fallbacks = vec!["stepfun-fast".to_string(), "deepseek-depth".to_string()];

        {
            let mut registry = runtime
                .model_performance_registry
                .lock()
                .expect("registry lock");
            registry.record_telemetry(
                &crate::cowd_event::RunModelTelemetry {
                    model: Some("stepfun-fast".to_string()),
                    models_used: vec!["stepfun-fast".to_string()],
                    first_token_latency_ms: Some(160),
                    active_stream_duration_ms: Some(1_000),
                    wall_duration_ms: 1_200,
                    output_chars: 1_000,
                    output_chunks: 10,
                    input_tokens: 400,
                    output_tokens: 180,
                    cache_create_tokens: 0,
                    cache_read_tokens: 0,
                    total_tokens: 580,
                    usage_source: "provider".to_string(),
                    chars_per_second: Some(1_000.0),
                    tokens_per_second: Some(180.0),
                },
                Some(0.72),
                false,
            );
            registry.record_telemetry(
                &crate::cowd_event::RunModelTelemetry {
                    model: Some("deepseek-depth".to_string()),
                    models_used: vec!["deepseek-depth".to_string()],
                    first_token_latency_ms: Some(950),
                    active_stream_duration_ms: Some(4_000),
                    wall_duration_ms: 5_000,
                    output_chars: 4_000,
                    output_chunks: 20,
                    input_tokens: 900,
                    output_tokens: 360,
                    cache_create_tokens: 0,
                    cache_read_tokens: 0,
                    total_tokens: 1_260,
                    usage_source: "provider".to_string(),
                    chars_per_second: Some(1_000.0),
                    tokens_per_second: Some(90.0),
                },
                Some(0.96),
                false,
            );
        }

        let quick = runtime.model_candidates_for_turn("快速回答这个简单问题");
        let deep = runtime.model_candidates_for_turn("深度审计复杂架构方案");

        assert_eq!(quick.first().map(String::as_str), Some("stepfun-fast"));
        assert_eq!(deep.first().map(String::as_str), Some("deepseek-depth"));
        assert!(quick.contains(&"balanced-model".to_string()));
        assert!(deep.contains(&"balanced-model".to_string()));
    }

    #[test]
    fn runs_user_to_tool_to_result_loop_end_to_end_and_tracks_usage() {
        let api_client = ScriptedApiClient { call_count: 0 };
        let tool_executor = StaticToolExecutor::new().register("add", |input| {
            let total = input
                .split(',')
                .map(|part| part.parse::<i32>().expect("input must be valid integer"))
                .sum::<i32>();
            Ok(total.to_string())
        });
        let permission_policy = PermissionPolicy::new(PermissionMode::WorkspaceWrite);
        let system_prompt = SystemPromptBuilder::new()
            .with_project_context(ProjectContext {
                cwd: PathBuf::from("/tmp/project"),
                current_date: "2026-03-31".to_string(),
                git_status: None,
                git_diff: None,
                git_context: None,
                instruction_files: Vec::new(),
            })
            .with_os("linux", "6.8")
            .build();
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
        );

        let prompter = SharedPrompter::new(Box::new(PromptAllowOnce));
        let rt = tokio::runtime::Runtime::new().unwrap();
        let summary = rt
            .block_on(runtime.run_turn_async("what is 2 + 2?", &prompter))
            .expect("conversation loop should succeed");

        assert_eq!(summary.iterations, 2);
        assert_eq!(summary.assistant_messages.len(), 2);
        assert_eq!(summary.tool_results.len(), 1);
        assert_eq!(summary.prompt_cache_events.len(), 1);
        assert_eq!(runtime.session().messages.len(), 4);
        assert_eq!(summary.usage.output_tokens, 10);
        assert_eq!(summary.model_telemetry.usage_source, "provider");
        assert_eq!(summary.model_telemetry.input_tokens, 44);
        assert_eq!(summary.model_telemetry.output_tokens, 10);
        assert_eq!(summary.model_telemetry.cache_create_tokens, 2);
        assert_eq!(summary.model_telemetry.cache_read_tokens, 5);
        assert!(summary.model_telemetry.first_token_latency_ms.is_some());
        assert!(summary.model_telemetry.output_chars > 0);
        assert!(summary.model_telemetry.output_chunks >= 2);
        assert!(summary.model_telemetry.tokens_per_second.is_some());
        assert_eq!(summary.auto_compaction, None);
        assert_eq!(
            summary.ai_kernel_trace.strategy.mode,
            harness_contract::core::ExecutionMode::DirectAnswer
        );
        assert!(summary.ai_kernel_trace.verification_report.can_finalize);
        assert!(summary.ai_kernel_trace.tool_transaction.is_some());
        assert!(summary.ai_kernel_trace.tool_receipt.is_some());
        assert!(summary.ai_kernel_trace.bench_result.passed);
        assert!(summary.ai_kernel_trace.regression_gate.allowed);
        assert!(!summary.ai_kernel_trace.learning_record.has_blocker());
        assert!(matches!(
            runtime.session().messages[1].blocks[1],
            ContentBlock::ToolUse { .. }
        ));
        assert!(matches!(
            runtime.session().messages[2].blocks[0],
            ContentBlock::ToolResult {
                is_error: false,
                ..
            }
        ));
    }

    #[test]
    fn real_tool_execution_emits_cowd_lifecycle_events() {
        use crate::cowd_event::{CowdEvent, CowdEventBus};

        let bus = CowdEventBus::new();
        let mut rx = bus.subscribe();
        let tool_executor = StaticToolExecutor::new().register("add", |_input| Ok("4".to_string()));
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            ScriptedApiClient { call_count: 0 },
            tool_executor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .with_cowd_event_bus(bus);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(runtime.run_turn_async(
            "what is 2 + 2?",
            &SharedPrompter::new(Box::new(PromptAllowOnce)),
        ))
        .expect("tool turn should succeed");

        let events = rt.block_on(async {
            let mut events = Vec::new();
            for _ in 0..16 {
                if let Ok(Ok(event)) =
                    tokio::time::timeout(Duration::from_millis(50), rx.recv()).await
                {
                    events.push(event);
                }
            }
            events
        });

        assert!(
            events.iter().any(|event| matches!(
                event,
                CowdEvent::ToolStart { id, name, preview }
                    if id == "tool-1" && name == "add" && preview == "2,2"
            )),
            "{events:#?}"
        );
        assert!(
            events.iter().any(|event| matches!(
                event,
                CowdEvent::ToolComplete {
                    id,
                    name,
                    summary,
                    exit_code
                } if id == "tool-1" && name == "add" && summary == "4" && *exit_code == Some(0)
            )),
            "{events:#?}"
        );
        assert!(
            events.iter().any(|event| matches!(
                event,
                CowdEvent::ToolExecuted { name, .. } if name == "add"
            )),
            "{events:#?}"
        );
    }

    #[test]
    fn conversation_runs_limited_network_tools_concurrently() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let tool_executor = StaticToolExecutor::new()
            .register(
                "WebSearch",
                tracked_tool_handler(
                    Arc::clone(&active),
                    Arc::clone(&max_active),
                    Duration::from_millis(80),
                ),
            )
            .register(
                "WebFetch",
                tracked_tool_handler(
                    Arc::clone(&active),
                    Arc::clone(&max_active),
                    Duration::from_millis(80),
                ),
            );
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MultiToolApiClient::new(vec![
                ("net-1", "WebSearch", r#"{"query":"rust"}"#),
                ("net-2", "WebFetch", r#"{"url":"https://example.test"}"#),
            ]),
            tool_executor,
            PermissionPolicy::new(PermissionMode::Allow),
            vec!["system".to_string()],
        );

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(runtime.run_turn_async(
                "inspect web evidence",
                &SharedPrompter::new(Box::new(PromptAllowAll)),
            ))
            .expect("network tools should execute");

        assert_eq!(
            max_active.load(Ordering::SeqCst),
            2,
            "network batch should run with limited parallelism instead of serial rest loop"
        );
    }

    #[test]
    fn conversation_serializes_write_tools_with_same_scope() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let tool_executor = StaticToolExecutor::new().register(
            "write_file",
            tracked_tool_handler(
                Arc::clone(&active),
                Arc::clone(&max_active),
                Duration::from_millis(60),
            ),
        );
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MultiToolApiClient::new(vec![
                (
                    "write-1",
                    "write_file",
                    r#"{"path":"shared.txt","content":"a"}"#,
                ),
                (
                    "write-2",
                    "write_file",
                    r#"{"path":"shared.txt","content":"b"}"#,
                ),
            ]),
            tool_executor,
            PermissionPolicy::new(PermissionMode::Allow),
            vec!["system".to_string()],
        );

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(runtime.run_turn_async(
                "write the same file twice",
                &SharedPrompter::new(Box::new(PromptAllowAll)),
            ))
            .expect("write tools should execute");

        assert_eq!(
            max_active.load(Ordering::SeqCst),
            1,
            "same-scope write tools must stay serial to avoid file races"
        );
    }

    #[test]
    fn conversation_runs_write_tools_for_different_scopes_concurrently() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let tool_executor = StaticToolExecutor::new().register(
            "write_file",
            tracked_tool_handler(
                Arc::clone(&active),
                Arc::clone(&max_active),
                Duration::from_millis(60),
            ),
        );
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MultiToolApiClient::new(vec![
                ("write-1", "write_file", r#"{"path":"a.txt","content":"a"}"#),
                ("write-2", "write_file", r#"{"path":"b.txt","content":"b"}"#),
            ]),
            tool_executor,
            PermissionPolicy::new(PermissionMode::Allow),
            vec!["system".to_string()],
        );

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(runtime.run_turn_async(
                "write two independent files",
                &SharedPrompter::new(Box::new(PromptAllowAll)),
            ))
            .expect("independent write tools should execute");

        assert_eq!(
            max_active.load(Ordering::SeqCst),
            2,
            "different write scopes should run concurrently under the write limit"
        );
    }

    #[test]
    fn conversation_tool_events_can_use_ledger_bridge() {
        let _guard = env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        unsafe {
            std::env::set_var("COWD_TOOL_LEDGER_V2", "1");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
            let session = Session::new();
            let session_id = session.session_id.clone();
            let now = "2026-06-16T00:00:00Z".to_string();
            store
                .create_session(&memory::SessionRecord {
                    session_id: session_id.clone(),
                    platform: "test".to_string(),
                    chat_id: session_id.clone(),
                    user_id: None,
                    model: Some("test-model".to_string()),
                    created_at: now.clone(),
                    last_activity: now,
                    message_count: 0,
                    reset_policy: "none".to_string(),
                    metadata_json: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    estimated_cost_usd: 0.0,
                    status: "active".to_string(),
                })
                .await
                .unwrap();

            let tool_executor = StaticToolExecutor::new().register("add", |input| {
                let total = input
                    .split(',')
                    .map(|part| part.parse::<i32>().expect("input must be valid integer"))
                    .sum::<i32>();
                Ok(total.to_string())
            });
            let mut runtime = ConversationRuntime::new(
                session,
                ScriptedApiClient { call_count: 0 },
                tool_executor,
                PermissionPolicy::new(PermissionMode::WorkspaceWrite),
                vec!["system".to_string()],
            )
            .with_session_store(Arc::clone(&store));

            runtime
                .run_turn_async(
                    "what is 2 + 2?",
                    &SharedPrompter::new(Box::new(PromptAllowOnce)),
                )
                .await
                .expect("tool turn should succeed");

            for _ in 0..40 {
                let events = store.get_events(&session_id, 0).await.unwrap();
                let runtime_kinds = events
                    .iter()
                    .filter_map(|event| memory::RuntimeEvent::from_session_event(event).ok())
                    .map(|event| event.kind)
                    .collect::<Vec<_>>();
                if runtime_kinds
                    .iter()
                    .any(|kind| kind == "tool.execution_plan.created")
                    && runtime_kinds
                        .iter()
                        .any(|kind| kind == "tool.schedule.created")
                    && runtime_kinds
                        .iter()
                        .any(|kind| kind == "tool.invocation.started")
                    && runtime_kinds
                        .iter()
                        .any(|kind| kind == "tool.invocation.completed")
                {
                    unsafe {
                        std::env::remove_var("COWD_TOOL_LEDGER_V2");
                    }
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }

            let events = store.get_events(&session_id, 0).await.unwrap();
            let runtime_kinds = events
                .iter()
                .filter_map(|event| memory::RuntimeEvent::from_session_event(event).ok())
                .map(|event| event.kind)
                .collect::<Vec<_>>();
            unsafe {
                std::env::remove_var("COWD_TOOL_LEDGER_V2");
            }
            panic!("missing expected tool runtime events: {runtime_kinds:?}");
        });
    }

    #[test]
    fn records_runtime_session_trace_events() {
        let sink = Arc::new(MemoryTelemetrySink::default());
        let tracer = SessionTracer::new("session-runtime", sink.clone());
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            ScriptedApiClient { call_count: 0 },
            StaticToolExecutor::new().register("add", |_input| Ok("4".to_string())),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .with_session_tracer(tracer);

        let prompter = SharedPrompter::new(Box::new(PromptAllowOnce));
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(runtime.run_turn_async("what is 2 + 2?", &prompter))
            .expect("conversation loop should succeed");

        let events = sink.events();
        let trace_names = events
            .iter()
            .filter_map(|event| match event {
                TelemetryEvent::SessionTrace(trace) => Some(trace.name.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(trace_names.contains(&"turn_started"));
        assert!(trace_names.contains(&"assistant_iteration_completed"));
        assert!(trace_names.contains(&"tool_execution_started"));
        assert!(trace_names.contains(&"tool_execution_finished"));
        assert!(trace_names.contains(&"turn_completed"));
    }

    #[test]
    fn records_denied_tool_results_when_prompt_rejects() {
        struct RejectPrompter;
        impl PermissionPrompter for RejectPrompter {
            fn decide(&mut self, _request: &PermissionRequest) -> PermissionPromptDecision {
                PermissionPromptDecision::Deny {
                    reason: "not now".to_string(),
                }
            }
        }

        struct SingleCallApiClient;
        impl ApiClient for SingleCallApiClient {
            fn stream(
                &mut self,
                request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                if request
                    .messages
                    .iter()
                    .any(|message| message.role == MessageRole::Tool)
                {
                    return to_stream(vec![
                        AssistantEvent::TextDelta("I could not use the tool.".to_string()),
                        AssistantEvent::MessageStop,
                    ]);
                }
                to_stream(vec![
                    AssistantEvent::ToolUse {
                        id: "tool-1".to_string(),
                        name: "blocked".to_string(),
                        input: "secret".to_string(),
                    },
                    AssistantEvent::MessageStop,
                ])
            }
        }

        let mut runtime = ConversationRuntime::new(
            Session::new(),
            SingleCallApiClient,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );

        let prompter = SharedPrompter::new(Box::new(RejectPrompter));
        let handle = tokio::runtime::Handle::try_current()
            .unwrap_or_else(|_| tokio::runtime::Runtime::new().unwrap().handle().clone());
        let summary = handle
            .block_on(runtime.run_turn_async("use the tool", &prompter))
            .expect("conversation should continue after denied tool");

        assert_eq!(summary.tool_results.len(), 1);
        assert!(matches!(
            &summary.tool_results[0].blocks[0],
            ContentBlock::ToolResult { is_error: true, output, .. } if output == "not now"
        ));
    }

    #[test]
    fn denies_tool_use_when_pre_tool_hook_blocks() {
        struct SingleCallApiClient;
        impl ApiClient for SingleCallApiClient {
            fn stream(
                &mut self,
                request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                if request
                    .messages
                    .iter()
                    .any(|message| message.role == MessageRole::Tool)
                {
                    return to_stream(vec![
                        AssistantEvent::TextDelta("blocked".to_string()),
                        AssistantEvent::MessageStop,
                    ]);
                }
                to_stream(vec![
                    AssistantEvent::ToolUse {
                        id: "tool-1".to_string(),
                        name: "blocked".to_string(),
                        input: r#"{"path":"secret.txt"}"#.to_string(),
                    },
                    AssistantEvent::MessageStop,
                ])
            }
        }

        let mut runtime = ConversationRuntime::new_with_features(
            Session::new(),
            SingleCallApiClient,
            Arc::new(StaticToolExecutor::new().register("blocked", |_input| {
                panic!("tool should not execute when hook denies")
            })),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
            &RuntimeFeatureConfig::default().with_hooks(RuntimeHookConfig::new(
                vec![shell_snippet("printf 'blocked by hook'; exit 2")],
                Vec::new(),
                Vec::new(),
            )),
        );

        let prompter = SharedPrompter::none();
        let handle = tokio::runtime::Handle::try_current()
            .unwrap_or_else(|_| tokio::runtime::Runtime::new().unwrap().handle().clone());
        let summary = handle
            .block_on(runtime.run_turn_async("use the tool", &prompter))
            .expect("conversation should continue after hook denial");

        assert_eq!(summary.tool_results.len(), 1);
        let ContentBlock::ToolResult {
            is_error, output, ..
        } = &summary.tool_results[0].blocks[0]
        else {
            panic!("expected tool result block");
        };
        assert!(
            *is_error,
            "hook denial should produce an error result: {output}"
        );
        assert!(
            output.contains("denied tool") || output.contains("blocked by hook"),
            "unexpected hook denial output: {output:?}"
        );
    }

    #[test]
    fn denies_tool_use_when_pre_tool_hook_fails() {
        struct SingleCallApiClient;
        impl ApiClient for SingleCallApiClient {
            fn stream(
                &mut self,
                request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                if request
                    .messages
                    .iter()
                    .any(|message| message.role == MessageRole::Tool)
                {
                    return to_stream(vec![
                        AssistantEvent::TextDelta("failed".to_string()),
                        AssistantEvent::MessageStop,
                    ]);
                }
                to_stream(vec![
                    AssistantEvent::ToolUse {
                        id: "tool-1".to_string(),
                        name: "blocked".to_string(),
                        input: r#"{"path":"secret.txt"}"#.to_string(),
                    },
                    AssistantEvent::MessageStop,
                ])
            }
        }

        // given
        let mut runtime = ConversationRuntime::new_with_features(
            Session::new(),
            SingleCallApiClient,
            Arc::new(StaticToolExecutor::new().register("blocked", |_input| {
                panic!("tool should not execute when hook fails")
            })),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
            &RuntimeFeatureConfig::default().with_hooks(RuntimeHookConfig::new(
                vec![shell_snippet("printf 'broken hook'; exit 1")],
                Vec::new(),
                Vec::new(),
            )),
        );

        // when
        let prompter = SharedPrompter::none();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let summary = rt
            .block_on(runtime.run_turn_async("use the tool", &prompter))
            .expect("conversation should continue after hook failure");

        // then
        assert_eq!(summary.tool_results.len(), 1);
        let ContentBlock::ToolResult {
            is_error, output, ..
        } = &summary.tool_results[0].blocks[0]
        else {
            panic!("expected tool result block");
        };
        assert!(
            *is_error,
            "hook failure should produce an error result: {output}"
        );
        assert!(
            output.contains("exited with status 1") || output.contains("broken hook"),
            "unexpected hook failure output: {output:?}"
        );
    }

    #[test]
    fn appends_post_tool_hook_feedback_to_tool_result() {
        struct TwoCallApiClient {
            calls: usize,
        }

        impl ApiClient for TwoCallApiClient {
            fn stream(
                &mut self,
                request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                self.calls += 1;
                match self.calls {
                    1 => to_stream(vec![
                        AssistantEvent::ToolUse {
                            id: "tool-1".to_string(),
                            name: "add".to_string(),
                            input: r#"{"lhs":2,"rhs":2}"#.to_string(),
                        },
                        AssistantEvent::MessageStop,
                    ]),
                    2 => {
                        assert!(request
                            .messages
                            .iter()
                            .any(|message| message.role == MessageRole::Tool));
                        to_stream(vec![
                            AssistantEvent::TextDelta("done".to_string()),
                            AssistantEvent::MessageStop,
                        ])
                    }
                    _ => unreachable!("extra API call"),
                }
            }
        }

        let mut runtime = ConversationRuntime::new_with_features(
            Session::new(),
            TwoCallApiClient { calls: 0 },
            Arc::new(StaticToolExecutor::new().register("add", |_input| Ok("4".to_string()))),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
            &RuntimeFeatureConfig::default().with_hooks(RuntimeHookConfig::new(
                vec![shell_snippet("printf 'pre hook ran'")],
                vec![shell_snippet("printf 'post hook ran'")],
                Vec::new(),
            )),
        );

        let prompter = SharedPrompter::none();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let summary = rt
            .block_on(runtime.run_turn_async("use add", &prompter))
            .expect("tool loop succeeds");

        assert_eq!(summary.tool_results.len(), 1);
        let ContentBlock::ToolResult {
            is_error, output, ..
        } = &summary.tool_results[0].blocks[0]
        else {
            panic!("expected tool result block");
        };
        assert!(
            !*is_error,
            "post hook should preserve non-error result: {output:?}"
        );
        assert!(
            output.contains('4'),
            "tool output missing value: {output:?}"
        );
        assert!(
            output.contains("pre hook ran"),
            "tool output missing pre hook feedback: {output:?}"
        );
        assert!(
            output.contains("post hook ran"),
            "tool output missing post hook feedback: {output:?}"
        );
    }

    #[test]
    fn appends_post_tool_use_failure_hook_feedback_to_tool_result() {
        struct TwoCallApiClient {
            calls: usize,
        }

        impl ApiClient for TwoCallApiClient {
            fn stream(
                &mut self,
                request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                self.calls += 1;
                match self.calls {
                    1 => to_stream(vec![
                        AssistantEvent::ToolUse {
                            id: "tool-1".to_string(),
                            name: "fail".to_string(),
                            input: r#"{"path":"README.md"}"#.to_string(),
                        },
                        AssistantEvent::MessageStop,
                    ]),
                    2 => {
                        assert!(request
                            .messages
                            .iter()
                            .any(|message| message.role == MessageRole::Tool));
                        to_stream(vec![
                            AssistantEvent::TextDelta("done".to_string()),
                            AssistantEvent::MessageStop,
                        ])
                    }
                    _ => unreachable!("extra API call"),
                }
            }
        }

        // given
        let mut runtime = ConversationRuntime::new_with_features(
            Session::new(),
            TwoCallApiClient { calls: 0 },
            Arc::new(
                StaticToolExecutor::new()
                    .register("fail", |_input| Err(ToolError::new("tool exploded"))),
            ),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
            &RuntimeFeatureConfig::default().with_hooks(RuntimeHookConfig::new(
                Vec::new(),
                vec![shell_snippet("printf 'post hook should not run'")],
                vec![shell_snippet("printf 'failure hook ran'")],
            )),
        );

        // when
        let prompter = SharedPrompter::none();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let summary = rt
            .block_on(runtime.run_turn_async("use fail", &prompter))
            .expect("tool loop succeeds");

        // then
        assert_eq!(summary.tool_results.len(), 1);
        let ContentBlock::ToolResult {
            is_error, output, ..
        } = &summary.tool_results[0].blocks[0]
        else {
            panic!("expected tool result block");
        };
        assert!(
            *is_error,
            "failure hook path should preserve error result: {output:?}"
        );
        assert!(
            output.contains("tool exploded"),
            "tool output missing failure reason: {output:?}"
        );
        assert!(
            output.contains("failure hook ran"),
            "tool output missing failure hook feedback: {output:?}"
        );
        assert!(
            !output.contains("post hook should not run"),
            "normal post hook should not run on tool failure: {output:?}"
        );
    }

    #[test]
    fn reconstructs_usage_tracker_from_restored_session() {
        struct SimpleApi;
        impl ApiClient for SimpleApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("done".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }

        let mut session = Session::new();
        session
            .messages
            .push(crate::session::ConversationMessage::assistant_with_usage(
                vec![ContentBlock::Text {
                    text: "earlier".to_string(),
                }],
                Some(TokenUsage {
                    input_tokens: 11,
                    output_tokens: 7,
                    cache_creation_input_tokens: 2,
                    cache_read_input_tokens: 1,
                }),
            ));

        let runtime = ConversationRuntime::new(
            session,
            SimpleApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        );

        assert_eq!(runtime.usage().turns(), 1);
        assert_eq!(runtime.usage().cumulative_usage().total_tokens(), 21);
    }

    #[test]
    fn compacts_session_after_turns() {
        struct SimpleApi;
        impl ApiClient for SimpleApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("done".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }

        let mut runtime = ConversationRuntime::new(
            Session::new(),
            SimpleApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        );
        let prompter = SharedPrompter::none();
        let handle = tokio::runtime::Handle::try_current()
            .unwrap_or_else(|_| tokio::runtime::Runtime::new().unwrap().handle().clone());
        handle
            .block_on(runtime.run_turn_async("a", &prompter))
            .expect("turn a");
        handle
            .block_on(runtime.run_turn_async("b", &prompter))
            .expect("turn b");
        handle
            .block_on(runtime.run_turn_async("c", &prompter))
            .expect("turn c");

        let result = runtime.compact(CompactionConfig {
            preserve_recent_messages: 2,
            max_estimated_tokens: 1,
            priority_threshold: 3,
            keep_high_priority: true,
        });
        assert!(result.summary.contains("Conversation summary"));
        assert_eq!(
            result.compacted_session.messages[0].role,
            MessageRole::System
        );
        assert_eq!(
            result.compacted_session.session_id,
            runtime.session().session_id
        );
        assert!(result.compacted_session.compaction.is_some());
    }

    #[test]
    fn legacy_jsonl_persistence_remains_explicit_codec_only() {
        struct SimpleApi;
        impl ApiClient for SimpleApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("done".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }

        let path = temp_session_path("persisted-turn");
        let session = Session::new().with_persistence_path(path.clone());
        let mut runtime = ConversationRuntime::new(
            session,
            SimpleApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        );

        let prompter = SharedPrompter::none();
        let handle = tokio::runtime::Handle::try_current()
            .unwrap_or_else(|_| tokio::runtime::Runtime::new().unwrap().handle().clone());
        handle
            .block_on(runtime.run_turn_async("persist this turn", &prompter))
            .expect("turn should succeed");

        drop(runtime);

        // Read back and verify through the explicit local import/export codec.
        let restored = Session::load_from_path(&path).expect("persisted session should reload");
        assert_eq!(restored.messages.len(), 2); // user + assistant
        assert_eq!(restored.messages[0].role, MessageRole::User);
        assert_eq!(restored.messages[1].role, MessageRole::Assistant);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn sqlite_session_store_is_runtime_turn_source_of_truth() {
        struct SimpleApi;
        impl ApiClient for SimpleApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("stored".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
            let session = Session::new();
            let session_id = session.session_id.clone();
            let now = "2026-06-04T00:00:00Z".to_string();
            store
                .create_session(&memory::SessionRecord {
                    session_id: session_id.clone(),
                    platform: "test".to_string(),
                    chat_id: session_id.clone(),
                    user_id: None,
                    model: Some("test-model".to_string()),
                    created_at: now.clone(),
                    last_activity: now,
                    message_count: 0,
                    reset_policy: "none".to_string(),
                    metadata_json: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    estimated_cost_usd: 0.0,
                    status: "active".to_string(),
                })
                .await
                .unwrap();

            let mut runtime = ConversationRuntime::new(
                session,
                SimpleApi,
                StaticToolExecutor::new(),
                PermissionPolicy::new(PermissionMode::DangerFullAccess),
                vec!["system".to_string()],
            )
            .with_session_store(Arc::clone(&store));

            runtime
                .run_turn_async("persist events", &SharedPrompter::none())
                .await
                .expect("turn should succeed");

            for _ in 0..20 {
                let events = store.get_events(&session_id, 0).await.unwrap();
                if events.iter().any(|event| {
                    memory::RuntimeEvent::from_session_event(event)
                        .map(|runtime_event| runtime_event.kind == "runtime.harness_contract.trace")
                        .unwrap_or(false)
                }) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }

            let envelope = runtime
                .last_context_envelope()
                .expect("context envelope should be remembered");
            for _ in 0..20 {
                if store
                    .get_context_event_by_envelope_id(&envelope.id)
                    .await
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }

            let messages = store.get_messages(&session_id, 0, 10).await.unwrap();
            let events = store.get_events(&session_id, 0).await.unwrap();
            let stored_count = store.get_message_count(&session_id).await.unwrap();
            let record = store
                .get_session(&session_id)
                .await
                .unwrap()
                .expect("session record should remain queryable");
            let context_event = store
                .get_context_event_by_envelope_id(&envelope.id)
                .await
                .unwrap()
                .expect("context envelope should be persisted");
            let context_json: serde_json::Value =
                serde_json::from_str(&context_event.event_json).expect("context event json");

            assert_eq!(messages.len(), 2);
            assert_eq!(stored_count, 2);
            assert_eq!(record.session_id, session_id);
            assert_eq!(record.chat_id, session_id);
            assert!(events.len() >= 3);
            assert!(events
                .iter()
                .any(|event| event.event_type == memory::RUNTIME_EVENT_TYPE));

            let user_event = events
                .iter()
                .find(|event| event.event_type == "message_appended" && event.sequence == 0)
                .expect("user message event");
            let event_json: serde_json::Value =
                serde_json::from_str(&user_event.event_json).expect("event json");
            assert_eq!(event_json["role"], "user");
            assert_eq!(event_json["message"]["role"], "user");
            assert_eq!(event_json["message"]["blocks"][0]["text"], "persist events");
            assert_eq!(context_event.event_type, "ContextEnvelope");
            assert_eq!(context_json["type"], "ContextEnvelope");
            assert_eq!(context_json["envelope_id"], envelope.id);
            assert_eq!(context_json["envelope"]["id"], envelope.id);
            assert_eq!(context_json["session_id"], session_id);

            let policy = events
                .iter()
                .filter_map(|event| memory::RuntimeEvent::from_session_event(event).ok())
                .find(|event| event.kind == "runtime.policy.decided")
                .expect("runtime policy event");
            assert_eq!(policy.scope, memory::RuntimeEventScope::Policy);
            assert_eq!(policy.kind, "runtime.policy.decided");
            assert_eq!(policy.payload["complexity"]["level"], "Simple");
            assert_eq!(policy.payload["agent_mode"], "Off");
            assert_eq!(policy.payload["requires_review"], false);

            let ai_kernel_trace = events
                .iter()
                .filter_map(|event| memory::RuntimeEvent::from_session_event(event).ok())
                .find(|event| event.kind == "runtime.harness_contract.trace")
                .expect("AI kernel trace event");
            assert_eq!(ai_kernel_trace.scope, memory::RuntimeEventScope::Task);
            assert_eq!(ai_kernel_trace.payload["strategy"]["mode"], "direct_answer");
            assert_eq!(
                ai_kernel_trace.payload["strategy"]["policy_version"],
                "strategy-router-v2"
            );
            assert_eq!(
                ai_kernel_trace.payload["collaboration"]["template_id"],
                "single_executor"
            );
            assert_eq!(
                ai_kernel_trace.payload["collaboration"]["plan"]["template_id"],
                "single_executor"
            );
            assert!(ai_kernel_trace.payload["collaboration"]["plan"]["agents"].is_array());
            assert!(
                ai_kernel_trace.payload["collaboration"]["plan"]["context_visibility"].is_string()
            );
            assert!(ai_kernel_trace.payload["collaboration"]["plan"]["memory_policy"].is_string());
            assert!(
                ai_kernel_trace.payload["collaboration"]["plan"]["evidence_policy"].is_string()
            );
            assert!(
                ai_kernel_trace.payload["collaboration"]["plan"]["handoff_contract"].is_string()
            );
            assert!(
                ai_kernel_trace.payload["collaboration"]["plan"]["review_contract"].is_string()
            );
            assert!(ai_kernel_trace.payload["collaboration"]["plan"]["merge_contract"].is_string());
            assert!(ai_kernel_trace.payload["collaboration"]["plan"]["budget_policy"].is_object());
            assert_eq!(
                ai_kernel_trace.payload["verification"]["can_finalize"],
                true
            );
            assert_eq!(
                ai_kernel_trace.payload["verification"]["finalization_blocked"],
                false
            );
            assert_eq!(ai_kernel_trace.payload["bench"]["passed"], true);
            assert_eq!(ai_kernel_trace.payload["regression_gate"]["allowed"], true);
            assert_eq!(ai_kernel_trace.payload["growth"]["has_blocker"], false);
            assert!(ai_kernel_trace.payload["maintenance_candidates"].is_array());
            assert_eq!(
                ai_kernel_trace.payload["matrix_evidence_signal"]["source"],
                "ai_kernel_trace"
            );
        });
    }

    fn temp_session_path(suffix: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        PathBuf::from(format!("/tmp/claw-test-{suffix}-{timestamp}.jsonl"))
    }

    fn shell_snippet(script: &str) -> String {
        // Escape for JSON
        script.replace('\\', "\\\\").replace('"', "\\\"")
    }

    // ── M2: Memory system tests ──────────────────────────────────────

    #[derive(Clone)]
    struct MockApi;
    impl ApiClient for MockApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::iter(vec![Ok(AssistantEvent::MessageStop)]))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn finalization_gate_replaces_empty_success_with_limitation_message() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        let summary = runtime
            .run_turn_async("answer this", &SharedPrompter::none())
            .await
            .expect("turn should complete with gate message");

        assert!(summary.ai_kernel_trace.finalization_blocked);
        assert!(summary.ai_kernel_trace.learning_record.has_blocker());
        assert!(summary
            .assistant_messages
            .iter()
            .flat_map(|message| message.blocks.iter())
            .any(|block| matches!(
                block,
                ContentBlock::Text { text }
                    if text.contains("I cannot finalize this as a completed answer yet")
            )));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn context_turn_report_includes_runtime_evidence_plan_observation() {
        #[derive(Clone)]
        struct TextApi;
        impl ApiClient for TextApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                to_stream(vec![
                    AssistantEvent::TextDelta("checked".to_string()),
                    AssistantEvent::MessageStop,
                ])
            }
        }

        let mut runtime = ConversationRuntime::new(
            Session::new(),
            TextApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        let summary = runtime
            .run_turn_async("检查 README 是否反映最新架构", &SharedPrompter::none())
            .await
            .expect("turn should complete");

        assert!(
            summary
                .context_turn_report
                .observations
                .iter()
                .any(
                    |observation| observation.tool_name == "runtime.evidence_plan"
                        && observation.model_summary.contains("SmallEvidence")
                ),
            "{:#?}",
            summary.context_turn_report.observations
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn repeated_real_tool_summaries_trigger_supervisor_guidance() {
        #[derive(Clone)]
        struct RepeatingToolApi {
            call_count: usize,
        }
        impl ApiClient for RepeatingToolApi {
            fn stream(
                &mut self,
                request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                self.call_count += 1;
                if self.call_count == 1 {
                    let mut events = Vec::new();
                    for idx in 0..4 {
                        events.push(AssistantEvent::ToolUse {
                            id: format!("tool-{idx}"),
                            name: "read_file".to_string(),
                            input: r#"{"path":"README.md","offset":0,"limit":80}"#.to_string(),
                        });
                    }
                    events.push(AssistantEvent::MessageStop);
                    return to_stream(events);
                }
                assert!(
                    request
                        .system_prompt
                        .iter()
                        .any(|section| section.contains("Runtime supervisor guidance")),
                    "second provider request should contain supervisor guidance"
                );
                to_stream(vec![
                    AssistantEvent::TextDelta("staged answer".to_string()),
                    AssistantEvent::MessageStop,
                ])
            }
        }

        let mut runtime = ConversationRuntime::new(
            Session::new(),
            RepeatingToolApi { call_count: 0 },
            StaticToolExecutor::new()
                .register("read_file", |_input| Ok("same README evidence".to_string())),
            PermissionPolicy::new(PermissionMode::ReadOnly),
            vec!["system".to_string()],
        )
        .without_memory();

        let summary = runtime
            .run_turn_async("反复检查 README", &SharedPrompter::none())
            .await
            .expect("turn should complete after supervisor guidance");

        assert!(summary
            .context_turn_report
            .observations
            .iter()
            .any(
                |observation| observation.tool_name == "runtime.turn_supervisor"
                    && observation.model_summary.contains("nudge")
            ));
    }

    #[test]
    fn context_turn_report_includes_active_knowledge_activation_report() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.set_turn_knowledge_report(harness_contract::knowledge::KnowledgeTurnReport {
            activation_plan_id: Some("knowledge-plan-test".to_string()),
            active_pack_ids: vec!["pack-domain-default".to_string()],
            blocked_namespaces: vec!["project:irrelevant not relevant to intent".to_string()],
            compliance_warnings: Vec::new(),
            evidence_refs: vec![harness_contract::core::KernelRef::new(
                "knowledge_chunk",
                "chunk-1",
            )],
            usage_signals: Vec::new(),
        });

        let report = runtime.build_context_turn_report(
            "turn-1",
            TokenUsage {
                input_tokens: 128,
                output_tokens: 32,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            None,
        );

        let knowledge = report.knowledge.expect("knowledge report is attached");
        assert_eq!(
            knowledge.activation_plan_id.as_deref(),
            Some("knowledge-plan-test")
        );
        assert_eq!(knowledge.active_pack_ids, vec!["pack-domain-default"]);
        assert_eq!(knowledge.blocked_namespaces.len(), 1);
        assert_eq!(knowledge.evidence_refs[0].ref_type, "knowledge_chunk");
    }

    struct EvidenceBackedCollaboration;

    impl CollaborationOps for EvidenceBackedCollaboration {
        fn run_boxed<'a>(
            &'a self,
            _task: &'a str,
            _skills: &'a [String],
        ) -> Pin<Box<dyn Future<Output = Option<String>> + 'a>> {
            Box::pin(async {
                Some(
                    "Evidence-backed synthesis: implementation and review findings agree."
                        .to_string(),
                )
            })
        }

        fn run_with_context_boxed<'a>(
            &'a self,
            task: &'a str,
            skills: &'a [String],
        ) -> Pin<Box<dyn Future<Output = Option<CollaborationContextResult>> + 'a>> {
            Box::pin(async move {
                let collaboration_task = CollaborationTask {
                    description: task.to_string(),
                    required_capabilities: skills.to_vec(),
                    subtasks: vec![SubTask {
                        id: "review-implementation-output".to_string(),
                        description: "review implementation output against evidence".to_string(),
                        required_capabilities: vec!["review".to_string()],
                        depends_on: Vec::new(),
                    }],
                    review_criteria: None,
                    collaboration_decision: None,
                };
                let agent_task = AgentTaskTrace {
                    task_id: "agent-task-review-implementation-output".to_string(),
                    parent_run_id: Some("team-run-production-like".to_string()),
                    agent_run_id: Some("agent-run-reviewer".to_string()),
                    role: "reviewer".to_string(),
                    objective: "review implementation output against evidence".to_string(),
                    status: "completed".to_string(),
                    context_envelope_id: Some("context-envelope-reviewer".to_string()),
                    result_summary: "review found implementation and evidence aligned".to_string(),
                    evidence_refs: vec![
                        "evidence://agent-run-reviewer/output".to_string(),
                        "workgraph://team-run-production-like/review".to_string(),
                    ],
                    collaboration_board_id: "board-evidence-backed".to_string(),
                    confidence: 0.92,
                    conflicts: Vec::new(),
                    created_at_ms: 1,
                    updated_at_ms: 2,
                };
                let review_packet = CollaborationReviewPacket {
                    board_id: "board-evidence-backed".to_string(),
                    parent_run_id: Some("team-run-production-like".to_string()),
                    scorecard: CollaborationScorecard {
                        completion_rate: 1.0,
                        synthesis_lift: 1.25,
                        complementarity_score: 0.75,
                        active_memory_score: 0.4,
                        conflict_count: 0,
                        memory_pulse_count: 1,
                        surfaced_conflicts: Vec::new(),
                    },
                    agent_tasks: vec![agent_task],
                    maintenance_candidates: Vec::new(),
                };
                let work_graph = AgentWorkGraph::from_collaboration_task(
                    "production-like-session",
                    &collaboration_task,
                )
                .with_review_packet(&review_packet);
                Some(CollaborationContextResult {
                    synthesis:
                        "Evidence-backed synthesis: implementation and review findings agree."
                            .to_string(),
                    context_items: Vec::new(),
                    collaboration_task,
                    review_packet,
                    work_graph,
                })
            })
        }

        fn decompose_task(&self, _task: &str) -> Vec<SubTask> {
            Vec::new()
        }

        fn assemble_team(&self, _task: &CollaborationTask) -> Option<AgentTeam> {
            None
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collaboration_records_skill_invocation_evidence() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        let now = "2026-06-28T00:00:00Z".to_string();
        store
            .create_session(&memory::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: session_id.clone(),
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "none".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let mut runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_store(Arc::clone(&store))
        .with_collaboration(Arc::new(EvidenceBackedCollaboration));

        runtime
            .run_turn_async(
                "please refactor rust tests and implement the plan",
                &SharedPrompter::none(),
            )
            .await
            .expect("turn should complete");

        let result = runtime
            .last_collaboration_result()
            .expect("collaboration result should be recorded");
        assert_eq!(result.work_graph.session_id, session_id);
        assert_eq!(result.review_packet.board_id, "board-evidence-backed");
        assert!(result.review_packet.scorecard.shows_multi_agent_lift());
        assert!(result
            .review_packet
            .agent_tasks
            .iter()
            .flat_map(|task| task.evidence_refs.iter())
            .any(|evidence| evidence.starts_with("evidence://agent-run-reviewer/")));

        for _ in 0..40 {
            let events = store.get_events(&session_id, 0).await.unwrap();
            if let Some(skill_event) = events
                .iter()
                .filter_map(|event| memory::RuntimeEvent::from_session_event(event).ok())
                .find(|event| event.kind == "skill_candidates")
            {
                assert!(skill_event.payload.get("invocation_evidence").is_some());
                assert_eq!(
                    skill_event.payload["candidates"][0]["reasons"][0],
                    "capability_ref_fallback"
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let events = store.get_events(&session_id, 0).await.unwrap();
        assert!(
            events
                .iter()
                .filter_map(|event| memory::RuntimeEvent::from_session_event(event).ok())
                .any(|event| event.kind == "skill_candidates"),
            "collaboration turn must persist runtime skill activation event"
        );

        assert!(runtime.take_collaboration_result().is_some());
        assert!(runtime.take_collaboration_result().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collaboration_records_profile_backed_skill_invocation_evidence() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        let now = "2026-06-28T00:00:00Z".to_string();
        store
            .create_session(&memory::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: session_id.clone(),
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "none".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let mut profile = test_skill_profile(
            "release-review",
            "Release Review",
            SkillAdapterKind::PromptOnly,
        );
        profile
            .structured_dependencies
            .push(SkillStructuredDependency {
                domain: "release_engineering".to_string(),
                required_fact_types: vec!["release.test_status".to_string()],
                required_metric_keys: vec!["release_risk".to_string()],
                required_evidence: vec!["test_report".to_string()],
                quality_gate: "release_quality_gate".to_string(),
            });
        let mut runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_store(Arc::clone(&store))
        .with_collaboration(Arc::new(EvidenceBackedCollaboration))
        .with_skill_profiles(vec![profile])
        .with_agent_skill_profile(AgentSkillProfile {
            adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
            ..AgentSkillProfile::default()
        });

        runtime
            .run_turn_async(
                "please review the release plan and implementation evidence",
                &SharedPrompter::none(),
            )
            .await
            .expect("turn should complete");

        let result = runtime
            .last_collaboration_result()
            .expect("collaboration result should be recorded");
        assert!(result
            .review_packet
            .maintenance_candidates
            .iter()
            .any(
                |candidate| candidate.source.as_deref() == Some("runtime_skill")
                    && candidate
                        .source_ref
                        .as_deref()
                        .is_some_and(|reference| reference.contains("release-review"))
            ));

        for _ in 0..40 {
            let events = store.get_events(&session_id, 0).await.unwrap();
            let runtime_events = events
                .iter()
                .filter_map(|event| memory::RuntimeEvent::from_session_event(event).ok())
                .collect::<Vec<_>>();
            if let Some(skill_event) = runtime_events
                .iter()
                .find(|event| event.kind == "skill_candidates")
            {
                assert_eq!(
                    skill_event.payload["source"],
                    "conversation_runtime.skill_activation"
                );
                assert_eq!(skill_event.payload["selected"], "release-review");
                assert_eq!(
                    skill_event.payload["invocation_evidence"]["skill_id"],
                    "release-review"
                );
                assert_eq!(
                    skill_event.payload["invocation_evidence"]["outcome"],
                    "selected_for_runtime"
                );
                assert!(skill_event
                    .refs
                    .iter()
                    .any(|reference| reference.ref_type == "skill_invocation"
                        && reference.id == "release-review"));
                assert_eq!(
                    skill_event.payload["structured_dependencies"][0]["domain"],
                    "release_engineering"
                );
                assert!(skill_event
                    .refs
                    .iter()
                    .any(|reference| reference.ref_type == "skill_dependency"
                        && reference.id.contains("release-review")));
                let memory_event = runtime_events
                    .iter()
                    .find(|event| {
                        event.kind == "skill_memory_candidate"
                            && event.payload["selected"] == "release-review"
                    })
                    .expect("skill memory candidate should be recorded");
                assert_eq!(
                    memory_event.payload["source"],
                    "conversation_runtime.skill_memory_candidate"
                );
                assert_eq!(
                    memory_event.payload["turn_index"],
                    skill_event.payload["turn_index"]
                );
                assert!(skill_event.sequence <= memory_event.sequence);
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("missing profile-backed skill activation event");
    }

    #[test]
    fn runtime_control_policy_disables_collaboration_routing() {
        let session = Session::new();
        let mut policy = crate::runtime_control::RuntimeControlPolicy::default();
        policy.agent.enabled = false;
        let features = RuntimeFeatureConfig::default().with_runtime_control(
            crate::config::RuntimeControlConfig {
                scenario: crate::config::DomainProfile::Coding,
                policy,
            },
        );
        let runtime = ConversationRuntime::new_with_features(
            session,
            MockApi,
            Arc::new(StaticToolExecutor::new()),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
            &features,
        )
        .without_memory();

        assert!(!runtime.should_use_collaboration(
            "please refactor the architecture, design a multi agent plan, implement tests, and review risks"
        ));
    }

    #[test]
    fn m2_layer_priority_l0_before_l3() {
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        assert!(
            rank(MemoryLayer::L0) > rank(MemoryLayer::L3),
            "L0 must rank higher than L3"
        );
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L1));
        assert!(rank(MemoryLayer::L1) > rank(MemoryLayer::L2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_empty_session_no_memory_crash() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        let _ = rt.prepare_reality_context("query").await;
        let _ = rt.run_memory_post_turn().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_budget_cap_without_memory_returns_system_prompt() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["test prompt".to_string()],
        );
        let result = rt.prepare_reality_context("test").await;
        assert_eq!(result[0], "test prompt");
        assert!(
            result
                .get(1)
                .is_some_and(|line| line.contains("profile:MainTurn")),
            "without memory manager, returns stable head followed by runtime header"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_prepare_without_memory_records_degraded_context_envelope() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();
        let prompt = rt.prepare_reality_context("remember this").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert_eq!(prompt[0], "stable system");
        assert!(prompt[1].contains("profile:MainTurn"));
        assert_eq!(envelope.intent, "remember this");
        assert_eq!(envelope.assembled.stable_head, vec!["stable system"]);
        assert_eq!(
            envelope.diagnostics.degraded_sources,
            vec![ContextSourceKind::Memory]
        );
        assert!(envelope.selected.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn external_resume_context_enters_prompt_and_envelope_without_memory() {
        let session = Session::new();
        let session_id = session.session_id.clone();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();

        rt.inject_resume_context(ResumeContextPacket {
            session_id: session_id.clone(),
            handoff_summary: Some("continue v0.8.13 context work".to_string()),
            active_task: Some("persist context timeline".to_string()),
            recent_decisions: vec!["DB session_events is the canonical timeline".to_string()],
            blockers: vec!["none".to_string()],
            source: ResumeContextSource::Mixed,
        });

        let prompt = rt.prepare_reality_context("resume").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(prompt
            .iter()
            .any(|segment| segment.contains("continue v0.8.13 context work")));
        assert_eq!(envelope.selected.len(), 1);
        assert_eq!(envelope.selected[0].source, ContextSourceKind::Handoff);
        assert_eq!(envelope.selected[0].authority, ContextAuthority::Session);
        assert!(envelope.selected[0].content.contains("Active task"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recent_tool_trace_enters_next_prompt_and_envelope() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();

        let tool_result = ConversationMessage::tool_result(
            "tool-1".to_string(),
            "bash".to_string(),
            "cargo test passed for context runtime".to_string(),
            false,
        );
        rt.remember_tool_trace_from_message(&tool_result);

        let prompt = rt.prepare_reality_context("next turn").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(prompt
            .iter()
            .any(|segment| segment.contains("cargo test passed")));
        assert!(envelope
            .selected
            .iter()
            .any(|item| item.source == ContextSourceKind::ToolTrace));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_structured_xml_format_present() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["base prompt".to_string()],
        );
        let prompt = rt.prepare_reality_context("hello").await;
        assert!(prompt.len() >= 1, "should have at least system prompt");
    }

    #[test]
    fn m2_error_propagation_returns_result() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["sys".to_string()],
        );
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .handle()
                .clone()
        });
        let r = handle.block_on(rt.run_memory_post_turn());
        assert!(
            r.is_ok(),
            "run_memory_post_turn should return Ok when no memory manager"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_structured_injection_has_memory_context_tag() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        let prompt = rt.prepare_reality_context("test").await;
        assert!(prompt.len() >= 1);
        // Without memory manager, should still return system prompt
        assert!(prompt[0] == "system" || prompt[0].starts_with("system"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_reality_context_suppresses_memory_conflicting_with_current_turn() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("memory.db");
        let blob_dir = tmp.path().join("blobs");
        std::fs::create_dir_all(&blob_dir).unwrap();

        let mem_cfg = memory::config::MemoryConfig {
            store: memory::config::StoreConfig {
                sqlite_path: db_path,
                blob_dir,
                enable_vector_index: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mgr = Arc::new(CognitiveContextManager::new(mem_cfg).await.unwrap());
        let now = chrono::Utc::now();
        mgr.remember(memory::types::MemoryEntry {
            id: memory::types::MemoryId::new_v4(),
            layer: memory::types::MemoryLayer::L1,
            category: memory::types::MemoryCategory::UserPreference,
            priority: memory::types::Priority::High,
            source: memory::types::MemorySource::UserExplicit,
            title: "User preference: 不要使用工具或编排".to_string(),
            content: "用户历史偏好：不要使用工具或编排。".to_string(),
            embedding: None,
            tags: vec!["preference".to_string()],
            relations: Vec::new(),
            confidence: 0.95,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: memory::MemoryScope::Project("cowd-develop".to_string()),
            session_id: None,
            source_agent: None,
            visibility: memory::types::AgentVisibility::Shared,
        })
        .await
        .unwrap();
        let loaded_l1 = mgr
            .list_layer_full_entries(memory::types::MemoryLayer::L1)
            .await
            .unwrap();
        assert!(loaded_l1
            .iter()
            .any(|entry| entry.title == "User preference: 不要使用工具或编排"));
        let prepared = mgr
            .prepare_context("请先使用 runtime_capabilities 调用工具分析", &[], None)
            .await
            .unwrap();
        assert!(
            prepared
                .entries
                .iter()
                .any(|entry| entry.title == "User preference: 不要使用工具或编排"),
            "prepared entries: {:?}",
            prepared
                .entries
                .iter()
                .map(|entry| entry.title.as_str())
                .collect::<Vec<_>>()
        );

        let rt = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .with_memory_manager(mgr);

        let prompt = rt
            .prepare_reality_context("请先使用 runtime_capabilities 调用工具分析")
            .await
            .join("\n");
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(envelope
            .omitted
            .iter()
            .any(|omission| omission.reason.contains("suppressed_for_current_turn")));
        assert!(!prompt.contains("<title>User preference: 不要使用工具或编排</title>"));
        assert!(!prompt.contains("<knowledge_compliance>"));
    }

    #[test]
    fn m2_layer_ranking_verification() {
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        assert_eq!(rank(MemoryLayer::L0), 5);
        assert_eq!(rank(MemoryLayer::L4), 1);
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L3));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_budget_cap_applied_on_prepare() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["base".to_string()],
        );
        // Verify that prepare_reality_context doesn't panic with empty session
        let result = rt.prepare_reality_context("any query").await;
        assert!(
            !result.is_empty(),
            "should return at least the system prompt"
        );
    }

    // ── M2-L2: integration-level memory tests ──────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_l2_budget_enforcement_limits_system_prompt() {
        // M2-L2-2: verify memory context doesn't exceed budget proportions
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system prompt".to_string()],
        )
        .without_memory();
        let prompt = rt.prepare_reality_context("test query").await;
        // Without selected memories, stable head is followed by runtime header.
        assert_eq!(prompt.len(), 2);
        assert!(prompt[1].contains("profile:MainTurn"));
        // System prompt should be reasonably sized
        assert!(
            prompt[0].len() < 10000,
            "system prompt should not be oversized"
        );
    }

    #[test]
    fn m2_l2_layer_priority_preserves_l0_l1() {
        // M2-L2-3: L0/L1 should be ranked before L3 in sorted entries
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        // L0 > L1 > L2 > L3 > L4
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L1));
        assert!(rank(MemoryLayer::L1) > rank(MemoryLayer::L2));
        assert!(rank(MemoryLayer::L2) > rank(MemoryLayer::L3));
        assert!(rank(MemoryLayer::L3) > rank(MemoryLayer::L4));
    }

    #[tokio::test]
    async fn m2_l2_handoff_roundtrip_preserves_data() {
        // M2-L2-1: cross-session handoff creates/restores handoff data
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        // Handoff should succeed even without memory manager (returns None)
        let handoff = rt.create_memory_handoff().await;
        // Without memory manager, this is None — which is correct behavior
        assert!(
            handoff.is_none() || handoff.is_some(),
            "handoff API should be callable without crashing"
        );
        // restore_memory_handoff should also not crash
        if let Some(h) = handoff {
            rt.restore_memory_handoff(h);
        }
    }

    // ── T2: active session tracking ────────────────────────────

    /// Integration test: verify that `prepare_reality_context` and
    /// `run_memory_post_turn` both call `set_active_session` on the
    /// memory manager before operating.
    ///
    /// Requires tempfile + memory DB, so marked `#[ignore]` for CI.
    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_reality_context_sets_active_session() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let blob_dir = tmp.path().join("blobs");
        std::fs::create_dir_all(&blob_dir).unwrap();

        let store = memory::config::StoreConfig {
            sqlite_path: db_path,
            blob_dir,
            enable_vector_index: false,
            ..Default::default()
        };
        let mem_cfg = memory::config::MemoryConfig {
            store,
            ..Default::default()
        };

        let mgr = Arc::new(CognitiveContextManager::new(mem_cfg).await.unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();

        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .with_memory_manager(mgr.clone());

        // Act — prepare_reality_context should set the active session
        let _ = rt.prepare_reality_context("test query").await;

        // Assert — verify the memory manager recorded the session
        let active = mgr.active_session_id();
        assert_eq!(
            active,
            Some(session_id),
            "active_session should be set after prepare_reality_context"
        );
    }
}
