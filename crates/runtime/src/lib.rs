#![deny(deprecated)]
#![deny(unused_imports)]
//! Core runtime primitives for the `cowd` CLI and supporting crates.
//!
//! This crate owns session persistence, permission evaluation, prompt assembly,
//! MCP plumbing, tool-facing file operations, and the core conversation loop
//! that drives interactive and one-shot turns.

#![deny(deprecated)]

pub mod cowd_dirs;
pub use cowd_dirs::expand_tilde;
mod bash;
pub mod bash_validation;
mod bootstrap;
pub mod branch_lock;
mod compact;
mod config;
pub mod config_validate;
mod conversation;
pub mod doc_ingestion;
pub mod error;
mod file_ops;
pub mod gates;
mod git_context;
pub mod green_contract;
pub mod storage;
pub mod wave;
pub use green_contract::GreenLevel;
mod hooks;
mod json;
pub use json::JsonValue;
pub mod context_profiler;
pub mod context_runtime;
pub mod effect;
mod lane_events;
pub mod lifecycle_hooks;
pub mod lsp_client;
mod mcp;
mod mcp_client;
pub mod mcp_lifecycle_hardened;
pub mod mcp_server;
mod mcp_stdio;
pub mod mcp_tool_bridge;
mod oauth;
pub mod permission_enforcer;
pub mod permissions;
pub mod platform;
pub mod plugin_lifecycle;
mod policy_engine;
mod prompt;
pub mod provider_pool;
pub mod recovery_recipes;
mod remote;
pub mod runtime_control;
pub mod sandbox;
mod session;
pub use session::workspace_sessions_dir;
pub mod session_control;
pub mod session_lifecycle;
#[allow(deprecated)]
pub use session_control::SessionStore;
pub mod agent;
pub mod agent_collaboration;
pub mod agent_discussion;
pub mod agent_workgraph;
pub mod approval_gate;
pub mod cowd_event;
pub mod joint_problem_solving;
pub mod mirror;
pub mod model_registry;
pub mod pairing;
pub mod profile;
pub mod provider_registry;
mod sse;
pub mod stale_base;
pub mod stale_branch;
pub mod summary_compression;
pub mod task_packet;
pub mod task_registry;
pub mod team_cron_registry;
pub mod team_discovery;
pub mod tool_dispatch;
pub mod tool_orchestrator;
pub mod trust_resolver;
mod usage;
pub mod worker_boot;

pub use agent::{
    ProductionExecutor, SubAgentConfig, SubAgentError, SubAgentExecutor, SubAgentProgressCallback,
    SubAgentResult, SubAgentToolMode,
};
pub use agent_collaboration::{
    AgentTaskTrace, AgentTeam, CollaborationBoard, CollaborationOps, CollaborationOrchestrator,
    CollaborationReviewPacket, CollaborationScorecard, CollaborationTask, MemoryPulseCandidate,
    MemoryPulseKind, SharedBoardEntry, SubTask,
};
pub use agent_discussion::{
    ConsensusMethod, ConsensusResult, Contribution, Discussion, DiscussionEngine, DiscussionPhase,
};
pub use agent_workgraph::{
    AgentWorkGraph, WorkGraphEdge, WorkGraphEdgeKind, WorkGraphNode, WorkGraphNodeKind,
    WorkGraphRef, WorkGraphStatus,
};
pub use bash::{BashCommandInput, BashCommandOutput, execute_bash};
pub use bootstrap::{BootstrapPhase, BootstrapPlan};
pub use branch_lock::{BranchLockCollision, BranchLockIntent, detect_branch_lock_collisions};
pub use compact::{
    CompactionConfig, CompactionResult, compact_session, estimate_session_tokens,
    format_compact_summary, get_compact_continuation_message, should_compact,
};
pub use config::{
    ApprovalConfig, COWD_SETTINGS_SCHEMA_NAME, CompressionConfig, ConfigEntry, ConfigError,
    ConfigLoader, ConfigSource, DomainProfile, GateAutoFixConfig, GatewayConfig,
    McpConfigCollection, McpManagedProxyServerConfig, McpOAuthConfig, McpRemoteServerConfig,
    McpSdkServerConfig, McpServerConfig, McpStdioServerConfig, McpTransport,
    McpWebSocketServerConfig, MemoryConfig, OAuthConfig, PlatformConfig as GatewayPlatformConfig,
    ProviderConfig, ProvidersConfig, ResolvedPermissionMode, RuntimeConfig, RuntimeControlConfig,
    RuntimeFeatureConfig, RuntimeHookConfig, RuntimePermissionRuleConfig, RuntimePluginConfig,
    ScopedMcpServerConfig, SessionResetPolicy,
};
pub use config_validate::{
    ConfigDiagnostic, DiagnosticKind, ValidationResult, check_unsupported_format,
    format_diagnostics, validate_config_file,
};
pub use conversation::{
    ApiClient, ApiRequest, AssistantEvent, AutoCompactionEvent, ConversationRuntime,
    MemoryCallback, PromptCacheEvent, RuntimeError, StaticToolExecutor, ToolCallback, ToolError,
    ToolExecutor, TurnSummary, auto_compaction_threshold_from_env, build_cc_memory_config,
};
pub use cowd_event::{
    CowdEvent, CowdEventBus, RuntimePolicyDecisionSummary, RuntimeWorkGraphSummary,
};
pub use doc_ingestion::{
    ClassificationResult, ConflictStrategy, DocumentCategory, DocumentClassifier, DocumentIngestor,
    DocumentMetadata, IngestionResult,
};
pub use file_ops::{
    EditFileOutput, GlobSearchOutput, GrepSearchInput, GrepSearchOutput, ReadFileOutput,
    StructuredPatchHunk, TextFilePayload, WriteFileOutput, edit_file, glob_search, grep_search,
    read_file, write_file,
};
pub use gates::{
    AbortGate, ApprovalGate, AutoFixer, EscalationGate, FixStrategy, Gate, GateAction, GateContext,
    GateError, GateEvaluator, GateResult, HardStop, ImpactRiskLevel, ImpactSummary, PreFlightCheck,
    PreFlightGate, RevisionCheck, RevisionGate, ViolationSeverity, ViolationType,
};
pub use git_context::{GitCommitEntry, GitContext};
pub use hooks::{
    HOOK_PREVIEW_CHAR_LIMIT, HookAbortSignal, HookEvent, HookProgressEvent, HookProgressReporter,
    HookRunResult, HookRunner, format_hook_output,
};
pub use joint_problem_solving::{
    AgentDiscussion, DiscussionTurn, JpsOps, PhaseStatus, PipelineResult, ProblemSolvingConfig,
    ProblemSolvingPipeline, ProblemStatement, Solution, SolutionEvaluation, SolutionScore,
};
pub use lane_events::{
    LaneCommitProvenance, LaneEvent, LaneEventBlocker, LaneEventName, LaneEventStatus,
    LaneFailureClass, dedupe_superseded_commit_events,
};
pub use mcp::{
    mcp_server_signature, mcp_tool_name, mcp_tool_prefix, normalize_name_for_mcp,
    scoped_mcp_config_hash, unwrap_ccr_proxy_url,
};
pub use mcp_client::{
    McpClientAuth, McpClientBootstrap, McpClientTransport, McpManagedProxyTransport,
    McpRemoteTransport, McpSdkTransport, McpStdioTransport,
};
pub use mcp_lifecycle_hardened::{
    McpDegradedReport, McpErrorSurface, McpFailedServer, McpLifecyclePhase, McpLifecycleState,
    McpLifecycleValidator, McpPhaseResult,
};
pub use mcp_server::{MCP_SERVER_PROTOCOL_VERSION, McpServer, McpServerSpec, ToolCallHandler};
pub use mcp_stdio::{
    JsonRpcError, JsonRpcId, JsonRpcRequest, JsonRpcResponse, ManagedMcpTool, McpDiscoveryFailure,
    McpInitializeClientInfo, McpInitializeParams, McpInitializeResult, McpInitializeServerInfo,
    McpListResourcesParams, McpListResourcesResult, McpListToolsParams, McpListToolsResult,
    McpReadResourceParams, McpReadResourceResult, McpResource, McpResourceContents,
    McpServerManager, McpServerManagerError, McpStdioProcess, McpTool, McpToolCallContent,
    McpToolCallParams, McpToolCallResult, McpToolDiscoveryReport, UnsupportedMcpServer,
    spawn_mcp_stdio_process,
};
pub use model_registry::{
    CircularAliasError, ModelInfo, ModelRegistry, ModelRegistryError, ModelResolver, Pricing,
    global_registry,
};
pub use oauth::{
    OAuthAuthorizationRequest, OAuthCallbackParams, OAuthRefreshRequest, OAuthTokenExchangeRequest,
    OAuthTokenSet, PkceChallengeMethod, PkceCodePair, clear_oauth_credentials, code_challenge_s256,
    credentials_path, generate_pkce_pair, generate_state, load_oauth_credentials,
    loopback_redirect_uri, parse_oauth_callback_query, parse_oauth_callback_request_target,
    save_oauth_credentials,
};
pub use permissions::{
    PermissionContext, PermissionMode, PermissionOutcome, PermissionOverride, PermissionPolicy,
    PermissionPromptDecision, PermissionPrompter, PermissionRequest, SharedPrompter,
};
pub use plugin_lifecycle::{
    DegradedMode, DiscoveryResult, PluginHealthcheck, PluginLifecycle, PluginLifecycleEvent,
    PluginState, ResourceInfo, ServerHealth, ServerStatus, ToolInfo,
};
pub use policy_engine::{
    DiffScope, LaneBlocker, LaneContext, PolicyAction, PolicyCondition, PolicyEngine, PolicyRule,
    ReconcileReason, ReviewStatus, evaluate,
};
pub use profile::{Profile, ProfileManager, ProfileMeta};
pub use prompt::{
    ContextFile, FRONTIER_MODEL_NAME, ProjectContext, PromptBuildError,
    SYSTEM_PROMPT_DYNAMIC_BOUNDARY, SystemPromptBuilder, load_system_prompt, prepend_bullets,
};
pub use provider_registry::{
    init_global_providers, list_all_models, list_all_providers, list_models_for_provider,
    resolve_global_provider,
};
pub use recovery_recipes::{
    EscalationPolicy, FailureScenario, RecoveryContext, RecoveryEvent, RecoveryRecipe,
    RecoveryResult, RecoveryStep, attempt_recovery, recipe_for,
};
pub use remote::{
    DEFAULT_REMOTE_BASE_URL, DEFAULT_SESSION_TOKEN_PATH, DEFAULT_SYSTEM_CA_BUNDLE, NO_PROXY_HOSTS,
    RemoteSessionContext, UPSTREAM_PROXY_ENV_KEYS, UpstreamProxyBootstrap, UpstreamProxyState,
    inherited_upstream_proxy_env, no_proxy_list, read_token, upstream_proxy_ws_url,
};
pub use sandbox::{
    ContainerEnvironment, FilesystemIsolationMode, LinuxSandboxCommand, SandboxConfig,
    SandboxDetectionInputs, SandboxRequest, SandboxStatus, build_linux_sandbox_command,
    detect_container_environment, detect_container_environment_from, resolve_sandbox_status,
    resolve_sandbox_status_for_request,
};
pub use session::{
    ContentBlock, ConversationMessage, MessageEvent, MessageRole, Session, SessionCompaction,
    SessionError, SessionEventLog, SessionFork, SessionPromptEntry,
};
pub use sse::{IncrementalSseParser, SseEvent};
pub use stale_base::{
    BaseCommitSource, BaseCommitState, check_base_commit, format_stale_base_warning,
    read_cowd_base_file, resolve_expected_base,
};
pub use stale_branch::{
    BranchFreshness, StaleBranchAction, StaleBranchEvent, StaleBranchPolicy, apply_policy,
    check_freshness,
};
pub use task_packet::{
    TaskPacket, TaskPacketValidationError, TaskScope, ValidatedPacket, validate_packet,
};
pub use team_discovery::{DiscoveredTeam, PersistedTeam, TeamDiscoveryProtocol};
pub use trust_resolver::{TrustConfig, TrustDecision, TrustEvent, TrustPolicy, TrustResolver};
pub use usage::{
    ModelPricing, TokenUsage, UsageCostEstimate, UsageTracker, format_usd, pricing_for_model,
};
pub use wave::{
    DependencyGraph, ErrorPolicy, TaskContext, TaskId, TaskResult, TaskStatus, Wave, WaveConfig,
    WaveError, WaveExecutor, WaveOrchestrator, WaveResult, WaveStatus, WaveTask,
};
pub use worker_boot::{
    StartupEvidenceBundle, StartupFailureClassification, Worker, WorkerEvent, WorkerEventKind,
    WorkerEventPayload, WorkerFailure, WorkerFailureKind, WorkerPromptTarget, WorkerReadySnapshot,
    WorkerRegistry, WorkerStatus, WorkerTrustResolution,
};

pub mod cached_prompt;
pub mod prompt_cache;

pub use cached_prompt::CachedSystemPrompt;
pub use context_runtime::{
    AgentContextLease, AgentReturnPacket, AgentReturnRequirement, AssembledContext,
    ContextAuthority, ContextBudgetReport, ContextDegradationPath, ContextDiagnostics,
    ContextEnvelope, ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextLeanProbe,
    ContextLease, ContextMode, ContextOmission, ContextPolicyAction, ContextPolicyDecision,
    ContextPolicyProposal, ContextPressureLevel, ContextProfile, ContextRole, ContextRuntimeKernel,
    ContextSourceKind, ContextVisibility, ResumeContextPacket, ResumeContextSource,
    StableHeadComparison, ToolTracePacket, ToolTraceStatus, WorkspacePacket,
};
pub use prompt_cache::{
    CacheBreakEvent, CacheUsage, PromptCache, PromptCacheConfig, PromptCachePaths,
    PromptCacheRecord, PromptCacheStats, RequestFingerprintHashes, hash_serializable,
    now_unix_secs, request_hash_hex_from_fnv, sanitize_path_segment, stable_hash_bytes,
};
pub use runtime_control::{
    AgentControlPolicy, AgentMode, ComplexityLevel, ComplexitySignal, ComplexityThresholds,
    ContextControlPolicy, MemoryControlPolicy, ObservabilityPolicy, PermissionControlPolicy,
    RuntimeControlPolicy, TaskComplexityInput, TaskComplexityProfile, TaskControlPolicy,
};
#[cfg(test)]
pub(crate) fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
