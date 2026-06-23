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
pub mod capability;
mod compact;
mod config;
pub mod config_validate;
pub mod control_plane;
mod conversation;
pub mod doc_ingestion;
pub mod error;
mod file_ops;
pub mod gates;
mod git_context;
pub mod graph_contract;
pub mod green_contract;
pub mod wave;
pub use green_contract::GreenLevel;
pub mod global_approval_queue;
mod hooks;
mod json;
pub use json::JsonValue;
pub mod context_profiler;
pub mod context_runtime;
pub mod effect;
pub mod execution_outcome;
mod lane_events;
pub mod lifecycle_hooks;
pub mod lsp_client;
mod mcp;
mod mcp_client;
pub mod mcp_lifecycle_hardened;
pub mod mcp_server;
mod mcp_stdio;
pub mod mcp_tool_bridge;
pub mod mission_runtime;
pub mod permission_enforcer;
pub mod permissions;
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
pub mod agent;
pub mod agent_backend;
pub mod agent_collaboration;
pub mod agent_discussion;
pub mod agent_kernel;
pub mod agent_lifecycle;
pub mod agent_protocol;
pub mod agent_workgraph;
pub mod ai_kernel;
pub mod approval_gate;
pub mod autonomy_profile;
pub mod checkpoint;
pub mod collaboration_template;
pub mod context_fanout;
pub mod cowd_event;
pub mod cross_plane_policy;
pub mod execution_scheduler;
pub mod intent_planner;
pub mod joint_problem_solving;
pub mod mutation_plan;
pub mod pairing;
pub mod profile;
pub mod projection;
pub mod provider_registry;
pub mod provider_runtime_client;
pub mod quality_gate;
pub mod release_gate;
pub mod session_lifecycle;
pub mod skill_activation;
pub mod skill_dependency;
pub mod skill_memory;
pub mod source_self_audit;
mod sse;
pub mod stale_base;
pub mod stale_branch;
pub mod steward_agent;
pub mod structured_data;
pub mod subagent_turn;
pub mod summary_compression;
pub mod surface_contract;
pub mod task_packet;
pub mod task_registry;
pub mod team_cron_registry;
pub mod team_discovery;
pub mod team_runtime;
pub mod tool_cache;
pub mod tool_dispatch;
pub mod tool_execution_plan;
pub mod tool_invocation;
pub mod tool_ledger;
pub mod tool_memory;
pub mod tool_orchestrator;
pub mod trust_resolver;
mod usage;
pub mod worker_boot;

pub use agent::{
    ProductionExecutor, SubAgentConfig, SubAgentError, SubAgentExecutor, SubAgentProgressCallback,
    SubAgentResult, SubAgentToolMode,
};
pub use agent_backend::{
    AgentExecutionBackendKind, AgentExecutionCommand, AgentExecutionCommandKind,
    AgentExecutionCommandReceipt, AgentExecutionEventEnvelope,
};
pub use agent_collaboration::{
    AgentTaskTrace, AgentTeam, CollaborationBoard, CollaborationOps, CollaborationOrchestrator,
    CollaborationReviewPacket, CollaborationScorecard, CollaborationTask, MemoryPulseCandidate,
    MemoryPulseKind, SharedBoardEntry, SubTask,
};
pub use agent_discussion::{
    ConsensusMethod, ConsensusResult, Contribution, Discussion, DiscussionEngine, DiscussionPhase,
};
pub use agent_kernel::{AgentGraphError, AgentRunGraph};
pub use agent_lifecycle::{
    agent_store_dir, build_agent_system_prompt, classify_lane_failure, derive_agent_state,
    global_agent_lifecycle_service, iso8601_now, maybe_commit_provenance, normalize_subagent_type,
    persist_agent_terminal_state, prepare_agent_job, resolve_agent_model, slugify_agent_name,
    spawn_provider_agent, AgentCommandReceipt, AgentJob, AgentLifecycleEvent,
    AgentLifecycleService, AgentSnapshot, SpawnAgentRequest, DEFAULT_AGENT_MAX_ITERATIONS,
    DEFAULT_AGENT_MODEL, DEFAULT_AGENT_SYSTEM_DATE,
};
pub use agent_protocol::{
    AgentEvidence, AgentMergeDecision, AgentMessage, AgentNodeStatus, AgentReview, AgentRole,
    AgentTaskNode, ReviewVerdict,
};
pub use agent_workgraph::{
    AgentWorkGraph, WorkGraphEdge, WorkGraphEdgeKind, WorkGraphNode, WorkGraphNodeKind,
    WorkGraphRef, WorkGraphStatus,
};
pub use ai_kernel::{RuntimeAiKernel, RuntimeAiKernelTrace};
pub use autonomy_profile::{
    ApprovalPolicy as AutonomyApprovalPolicy, AutonomyBudget, AutonomyDecision,
    AutonomyDecisionInput, AutonomyDecisionKind, AutonomyProfileCatalog, AutonomyProfileId,
    AutonomyProfileSpec, InterruptionPolicy as AutonomyInterruptionPolicy,
};
pub use bash::{execute_bash, BashCommandInput, BashCommandOutput};
pub use bootstrap::{BootstrapPhase, BootstrapPlan};
pub use branch_lock::{detect_branch_lock_collisions, BranchLockCollision, BranchLockIntent};
pub use checkpoint::{
    checkpoint_create, checkpoint_diff, checkpoint_list, checkpoint_restore, CheckpointCreateInput,
    CheckpointDiffInput, CheckpointDiffOutput, CheckpointListOutput, CheckpointRestoreInput,
    CheckpointSummary,
};
pub use collaboration_template::{
    BudgetPolicy, CollaborationContextVisibility, CollaborationDecision, CollaborationPlan,
    CollaborationPlanAgent, CollaborationRoleSpec, CollaborationTemplate,
    CollaborationTemplateCatalog, CollaborationTemplateId, CollaborationTemplateMatcher,
};
pub use compact::{
    compact_session, estimate_session_tokens, format_compact_summary,
    get_compact_continuation_message, should_compact, CompactionConfig, CompactionResult,
};
pub use config::{
    ApprovalConfig, CompressionConfig, ConfigEntry, ConfigError, ConfigLoader, ConfigSource,
    DomainProfile, GateAutoFixConfig, GatewayConfig, McpConfigCollection,
    McpManagedProxyServerConfig, McpOAuthConfig, McpRemoteServerConfig, McpSdkServerConfig,
    McpServerConfig, McpStdioServerConfig, McpTransport, McpWebSocketServerConfig, MemoryConfig,
    PlatformConfig as GatewayPlatformConfig, ResolvedPermissionMode, RuntimeConfig,
    RuntimeControlConfig, RuntimeFeatureConfig, RuntimeHookConfig, RuntimePermissionRuleConfig,
    RuntimePluginConfig, ScopedMcpServerConfig, SessionResetPolicy, COWD_SETTINGS_SCHEMA_NAME,
};
pub use config_validate::{
    check_unsupported_format, format_diagnostics, validate_config_file, ConfigDiagnostic,
    DiagnosticKind, ValidationResult,
};
pub use context_fanout::{plan_context_fanout, ContextFanoutPlan, FanoutToolCall};
pub use control_plane::{global_runtime_control_plane, global_task_registry, RuntimeControlPlane};
pub use conversation::{
    auto_compaction_threshold_from_env, build_cc_memory_config, ApiClient, ApiRequest,
    AssistantEvent, AutoCompactionEvent, CancellationToken, ConversationRuntime, MemoryCallback,
    PromptCacheEvent, RuntimeError, StaticToolExecutor, ToolCallback, ToolError, ToolExecutor,
    TurnSummary,
};
pub use cowd_event::{
    CowdEvent, CowdEventBus, RuntimePolicyDecisionSummary, RuntimeWorkGraphSummary,
};
pub use cross_plane_policy::{
    ConnectorActionContext, ConnectorDecisionEvidence, CrossPlaneAction, CrossPlaneAuditRecord,
    CrossPlaneControlPlane, CrossPlaneDecisionEvidence, CrossPlaneDispatchOutcome,
    CrossPlaneDispatchTarget, CrossPlaneExecutionReceipt, CrossPlaneGrant,
    CrossPlaneIdentityBinding, CrossPlaneOutboundMessagePlan, CrossPlanePolicyConfig,
    CrossPlanePolicyDecision, CrossPlanePolicyEngine, CrossPlaneResolvedIdentity,
    CrossPlaneSummary, GrantType, IdentityTrust, PolicyDecisionKind,
};
pub use doc_ingestion::{
    ClassificationResult, ConflictStrategy, DocumentCategory, DocumentClassifier, DocumentIngestor,
    DocumentMetadata, IngestionResult,
};
pub use file_ops::{
    edit_file, glob_search, grep_search, read_file, write_file, EditFileOutput, GlobSearchOutput,
    GrepSearchInput, GrepSearchOutput, ReadFileOutput, StructuredPatchHunk, TextFilePayload,
    WriteFileOutput,
};
pub use gates::{
    AbortGate, ApprovalGate, AutoFixer, EscalationGate, FixStrategy, Gate, GateAction, GateContext,
    GateError, GateEvaluator, GateResult, HardStop, ImpactRiskLevel, ImpactSummary, PreFlightCheck,
    PreFlightGate, RevisionCheck, RevisionGate, ViolationSeverity, ViolationType,
};
pub use git_context::{GitCommitEntry, GitContext};
pub use global_approval_queue::{
    global_approval_queue, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy,
    GlobalApprovalDecision, GlobalApprovalDecisionReceipt, GlobalApprovalQueue,
    GlobalApprovalRequest, GlobalApprovalStatus, SubmitGlobalApprovalRequest,
};
pub use hooks::{
    format_hook_output, HookAbortSignal, HookEvent, HookProgressEvent, HookProgressReporter,
    HookRunResult, HookRunner, HOOK_PREVIEW_CHAR_LIMIT,
};
pub use intent_planner::{classify_intent, IntentPlan, TaskIntent};
pub use joint_problem_solving::{
    AgentDiscussion, DiscussionTurn, JpsOps, PhaseStatus, PipelineResult, ProblemSolvingConfig,
    ProblemSolvingPipeline, ProblemStatement, Solution, SolutionEvaluation, SolutionScore,
};
pub use lane_events::{
    dedupe_superseded_commit_events, LaneCommitProvenance, LaneEvent, LaneEventBlocker,
    LaneEventName, LaneEventStatus, LaneFailureClass,
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
pub use mcp_server::{McpServer, McpServerSpec, ToolCallHandler, MCP_SERVER_PROTOCOL_VERSION};
pub use mcp_stdio::{
    spawn_mcp_stdio_process, JsonRpcError, JsonRpcId, JsonRpcRequest, JsonRpcResponse,
    ManagedMcpTool, McpDiscoveryFailure, McpInitializeClientInfo, McpInitializeParams,
    McpInitializeResult, McpInitializeServerInfo, McpListResourcesParams, McpListResourcesResult,
    McpListToolsParams, McpListToolsResult, McpReadResourceParams, McpReadResourceResult,
    McpResource, McpResourceContents, McpServerManager, McpServerManagerError, McpStdioProcess,
    McpTool, McpToolCallContent, McpToolCallParams, McpToolCallResult, McpToolDiscoveryReport,
    UnsupportedMcpServer,
};
pub use mission_runtime::{
    global_mission_runtime, MissionCommandReceipt, MissionEvent, MissionProjection, MissionRuntime,
    MissionSessionSnapshot, MissionSessionStatus, StartMissionSessionRequest,
};
pub use mutation_plan::{
    apply_mutations, preview_mutations, FileMutationApplied, FileMutationPreview,
    MutationApplyInput, MutationApplyOutput, MutationEdit, MutationPreview, MutationPreviewInput,
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
    evaluate, DiffScope, LaneBlocker, LaneContext, PolicyAction, PolicyCondition, PolicyEngine,
    PolicyRule, ReconcileReason, ReviewStatus,
};
pub use profile::{Profile, ProfileManager, ProfileMeta};
pub use prompt::{
    load_system_prompt, prepend_bullets, ContextFile, ProjectContext, PromptBuildError,
    SystemPromptBuilder, SYSTEM_PROMPT_DYNAMIC_BOUNDARY,
};
pub use provider_registry::{
    init_global_providers, list_all_models, list_all_providers, list_models_for_provider,
    resolve_global_provider,
};
pub use provider_runtime_client::{
    push_provider_output_block, ProviderOutputContentBlock, ProviderRuntimeClient,
    ProviderToolDefinition,
};
pub use recovery_recipes::{
    attempt_recovery, recipe_for, EscalationPolicy, FailureScenario, RecoveryContext,
    RecoveryEvent, RecoveryRecipe, RecoveryResult, RecoveryStep,
};
pub use remote::{
    inherited_upstream_proxy_env, no_proxy_list, read_token, upstream_proxy_ws_url,
    RemoteSessionContext, UpstreamProxyBootstrap, UpstreamProxyState, DEFAULT_REMOTE_BASE_URL,
    DEFAULT_SESSION_TOKEN_PATH, DEFAULT_SYSTEM_CA_BUNDLE, NO_PROXY_HOSTS, UPSTREAM_PROXY_ENV_KEYS,
};
pub use sandbox::{
    build_linux_sandbox_command, detect_container_environment, detect_container_environment_from,
    resolve_sandbox_status, resolve_sandbox_status_for_request, ContainerEnvironment,
    FilesystemIsolationMode, LinuxSandboxCommand, SandboxConfig, SandboxDetectionInputs,
    SandboxRequest, SandboxStatus,
};
pub use session::{
    ContentBlock, ConversationMessage, MessageEvent, MessageRole, Session, SessionCompaction,
    SessionError, SessionEventLog, SessionFork, SessionPromptEntry,
};
pub use skill_activation::{RuntimeSkillCandidate, SkillActivationRecord};
pub use skill_memory::{memory_candidate_from_skill_activation, SkillMemoryPolicy};
pub use source_self_audit::{
    RuntimeSourceSelfAudit, SourceRepairAction, SourceSelfAuditCheck, SourceSelfAuditReport,
};
pub use sse::{IncrementalSseParser, SseEvent};
pub use stale_base::{
    check_base_commit, format_stale_base_warning, read_cowd_base_file, resolve_expected_base,
    BaseCommitSource, BaseCommitState,
};
pub use stale_branch::{
    apply_policy, check_freshness, BranchFreshness, StaleBranchAction, StaleBranchEvent,
    StaleBranchPolicy,
};
pub use steward_agent::{
    StewardActionRequest, StewardActionStatus, StewardAgent, StewardDecisionRecord,
};
pub use subagent_turn::{
    final_assistant_text, run_provider_subagent_turn, ProviderSubAgentTurnConfig,
};
pub use task_packet::{
    validate_packet, TaskPacket, TaskPacketValidationError, TaskScope, ValidatedPacket,
};
pub use task_registry::{Task, TaskMessage, TaskRegistry, TaskStatus as RegistryTaskStatus};
pub use team_cron_registry::{CronEntry, CronRegistry, Team, TeamRegistry};
pub use team_discovery::{DiscoveredTeam, PersistedTeam, TeamDiscoveryProtocol};
pub use team_runtime::{
    global_team_runtime_service, StartTeamRuntimeRequest, TeamRuntimeAgent,
    TeamRuntimeCommandReceipt, TeamRuntimeEvent, TeamRuntimeService, TeamRuntimeSnapshot,
    TeamRuntimeStatus,
};
pub use tool_execution_plan::{ToolExecutionMode, ToolExecutionPlan, ToolExecutionPlanTask};
pub use tool_invocation::{
    now_ms as tool_invocation_now_ms, ToolFailureKind, ToolInvocationRecord, ToolInvocationStatus,
    ToolOutputRef, DEFAULT_OUTPUT_REF_MIN_LINES,
};
pub use tool_memory::{memory_candidate_from_tool_invocation, ToolMemoryCandidatePolicy};
pub use tool_orchestrator::{
    tool_execution_profile, ToolCachePolicy, ToolExecutionProfile, ToolSafetyCategory,
};
pub use trust_resolver::{TrustConfig, TrustDecision, TrustEvent, TrustPolicy, TrustResolver};
pub use usage::{pricing_for_model, UsageTracker};
pub use wave::{
    DependencyGraph, ErrorPolicy, TaskContext, TaskId, TaskResult, TaskStatus, Wave, WaveConfig,
    WaveError, WaveExecutor, WaveOrchestrator, WaveResult, WaveStatus, WaveTask,
};
pub use worker_boot::{
    StartupEvidenceBundle, StartupFailureClassification, Worker, WorkerEvent, WorkerEventKind,
    WorkerEventPayload, WorkerFailure, WorkerFailureKind, WorkerPromptTarget, WorkerReadySnapshot,
    WorkerRegistry, WorkerStatus, WorkerTaskReceipt, WorkerTrustResolution,
};

pub mod cached_prompt;
pub use cached_prompt::CachedSystemPrompt;
pub use context_runtime::{
    AgentContextLease, AgentContextView, AgentReturnPacket, AgentReturnRequirement,
    AssembledContext, ContextAuthority, ContextBudgetAllocation, ContextBudgetExplanation,
    ContextBudgetReport, ContextCacheStabilityReport, ContextDegradationPath, ContextDiagnostics,
    ContextEnvelope, ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextLeanProbe,
    ContextLease, ContextMode, ContextModeCoverageEntry, ContextModeCoverageReport,
    ContextOmission, ContextPolicyAction, ContextPolicyDecision, ContextPolicyProposal,
    ContextPressureLevel, ContextProfile, ContextRole, ContextRuntimeKernel, ContextSegmentChange,
    ContextSegmentKind, ContextSegmentSnapshot, ContextSnapshot, ContextSnapshotDiff,
    ContextSourceKind, ContextVisibility, ResumeContextPacket, ResumeContextSource,
    StableHeadComparison, ToolTracePacket, ToolTraceStatus, WorkspacePacket,
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
