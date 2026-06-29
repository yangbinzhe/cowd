#![deny(deprecated)]
#![deny(unused_imports)]
//! Core runtime primitives for the `cowd` CLI and supporting crates.
//!
//! This crate owns session persistence, permission evaluation, prompt assembly,
//! MCP plumbing, tool-facing file operations, and the core conversation loop
//! that drives interactive and one-shot turns.

#[path = "infrastructure/cowd_dirs.rs"]
pub mod cowd_dirs;
pub use cowd_dirs::expand_tilde;
#[path = "infrastructure/bash.rs"]
mod bash;
#[path = "infrastructure/bash_validation.rs"]
pub mod bash_validation;
#[path = "infrastructure/bootstrap.rs"]
mod bootstrap;
#[path = "session/branch_lock.rs"]
pub mod branch_lock;
#[path = "infrastructure/capability.rs"]
pub mod capability;
#[path = "infrastructure/capability_manifest.rs"]
pub mod capability_manifest;
#[path = "conversation/compact.rs"]
mod compact;
#[path = "infrastructure/config.rs"]
mod config;
#[path = "infrastructure/config_validate.rs"]
pub mod config_validate;
#[path = "infrastructure/control_plane.rs"]
pub mod control_plane;
#[path = "conversation/conversation.rs"]
mod conversation;
#[path = "infrastructure/error.rs"]
pub mod error;
#[path = "tooling/file_ops.rs"]
mod file_ops;
#[path = "policy/gates.rs"]
pub mod gates;
#[path = "infrastructure/git_context.rs"]
mod git_context;
#[path = "infrastructure/graph_contract.rs"]
pub mod graph_contract;
#[path = "policy/green_contract.rs"]
pub mod green_contract;
#[path = "infrastructure/wave.rs"]
pub mod wave;
pub use green_contract::GreenLevel;
#[path = "approval/global_approval_queue.rs"]
pub mod global_approval_queue;
#[path = "infrastructure/hooks.rs"]
mod hooks;
#[path = "infrastructure/json.rs"]
mod json;
pub use json::JsonValue;
#[path = "context/budget_policy.rs"]
pub mod budget_policy;
#[path = "context/context_profiler.rs"]
pub mod context_profiler;
#[path = "context/context_runtime.rs"]
pub mod context_runtime;
#[path = "infrastructure/execution_outcome.rs"]
pub mod execution_outcome;
#[path = "context/knowledge_activation.rs"]
pub mod knowledge_activation;
#[path = "context/knowledge_compliance.rs"]
pub mod knowledge_compliance;
#[path = "infrastructure/lane_events.rs"]
mod lane_events;
#[path = "infrastructure/lifecycle_hooks.rs"]
pub mod lifecycle_hooks;
#[path = "infrastructure/mcp.rs"]
mod mcp;
#[path = "infrastructure/mcp_client.rs"]
mod mcp_client;
#[path = "infrastructure/mcp_lifecycle_hardened.rs"]
pub mod mcp_lifecycle_hardened;
#[path = "infrastructure/mcp_server.rs"]
pub mod mcp_server;
#[path = "infrastructure/mcp_stdio.rs"]
mod mcp_stdio;
#[path = "infrastructure/mcp_tool_bridge.rs"]
pub mod mcp_tool_bridge;
#[path = "mission/mission_control.rs"]
pub mod mission_control;
#[path = "mission/mission_evidence.rs"]
pub mod mission_evidence;
#[path = "mission/mission_runtime.rs"]
pub mod mission_runtime;
pub mod module_map;
#[path = "policy/permission_enforcer.rs"]
pub mod permission_enforcer;
#[path = "policy/permissions.rs"]
pub mod permissions;
#[path = "infrastructure/plugin_lifecycle.rs"]
pub mod plugin_lifecycle;
#[path = "policy/policy_engine.rs"]
mod policy_engine;
#[path = "conversation/prompt.rs"]
mod prompt;
#[path = "provider/provider_pool.rs"]
pub mod provider_pool;
#[path = "recovery/recovery.rs"]
pub mod recovery;
#[path = "recovery/recovery_recipes.rs"]
pub mod recovery_recipes;
#[path = "infrastructure/remote.rs"]
mod remote;
#[path = "infrastructure/runtime_control.rs"]
pub mod runtime_control;
#[path = "infrastructure/sandbox.rs"]
pub mod sandbox;
#[path = "session/session.rs"]
mod session;
pub use session::workspace_sessions_dir;
#[path = "agent/agent.rs"]
pub mod agent;
#[path = "agent/agent_backend.rs"]
pub mod agent_backend;
#[path = "agent/agent_collaboration.rs"]
pub mod agent_collaboration;
#[path = "agent/agent_discussion.rs"]
pub mod agent_discussion;
#[path = "agent/agent_event_bus.rs"]
pub mod agent_event_bus;
#[path = "agent/agent_kernel.rs"]
pub mod agent_kernel;
#[path = "agent/agent_lifecycle.rs"]
pub mod agent_lifecycle;
#[path = "agent/agent_mailbox.rs"]
pub mod agent_mailbox;
#[path = "agent/agent_protocol.rs"]
pub mod agent_protocol;
#[path = "agent/agent_workgraph.rs"]
pub mod agent_workgraph;
#[path = "approval/approval_gate.rs"]
pub mod approval_gate;
#[path = "policy/autonomy_profile.rs"]
pub mod autonomy_profile;
#[path = "session/checkpoint.rs"]
pub mod checkpoint;
#[path = "agent/collaboration_template.rs"]
pub mod collaboration_template;
#[path = "context/context_fanout.rs"]
pub mod context_fanout;
#[path = "infrastructure/cowd_event.rs"]
pub mod cowd_event;
#[path = "policy/cross_plane_policy.rs"]
pub mod cross_plane_policy;
#[path = "infrastructure/eval_gate.rs"]
pub mod eval_gate;
#[path = "context/evidence_planner.rs"]
pub mod evidence_planner;
pub mod execution_core;
#[path = "infrastructure/execution_scheduler.rs"]
pub mod execution_scheduler;
#[path = "agent/intent_planner.rs"]
pub mod intent_planner;
#[path = "agent/joint_problem_solving.rs"]
pub mod joint_problem_solving;
#[path = "infrastructure/lane_completion.rs"]
pub mod lane_completion;
#[path = "infrastructure/mutation_plan.rs"]
pub mod mutation_plan;
pub mod orchestration;
#[path = "agent/pairing.rs"]
pub mod pairing;
#[path = "infrastructure/profile.rs"]
pub mod profile;
#[path = "infrastructure/projection.rs"]
pub mod projection;
#[path = "provider/provider_registry.rs"]
pub mod provider_registry;
#[path = "provider/provider_runtime_client.rs"]
pub mod provider_runtime_client;
#[path = "infrastructure/quality_gate.rs"]
pub mod quality_gate;
#[path = "infrastructure/release_gate.rs"]
pub mod release_gate;
#[path = "recovery/runtime_event_replay.rs"]
pub mod runtime_event_replay;
#[path = "recovery/runtime_event_store.rs"]
pub mod runtime_event_store;
#[path = "mission/runtime_harness.rs"]
pub mod runtime_harness;
#[path = "session/session_execution.rs"]
pub mod session_execution;
#[path = "session/session_lifecycle.rs"]
pub mod session_lifecycle;
#[path = "session/session_relation_graph.rs"]
pub mod session_relation_graph;
#[path = "skill/mod.rs"]
pub mod skill;
#[path = "recovery/source_self_audit.rs"]
pub mod source_self_audit;
#[path = "conversation/sse.rs"]
mod sse;
#[path = "infrastructure/stale_base.rs"]
pub mod stale_base;
#[path = "infrastructure/stale_branch.rs"]
pub mod stale_branch;
#[path = "steward/steward_agent.rs"]
pub mod steward_agent;
#[path = "steward/steward_runtime.rs"]
pub mod steward_runtime;
#[path = "steward/steward_scheduler.rs"]
pub mod steward_scheduler;
pub mod structured_data;
#[path = "agent/subagent_turn.rs"]
pub mod subagent_turn;
#[path = "conversation/summary_compression.rs"]
pub mod summary_compression;
#[path = "infrastructure/surface_contract.rs"]
pub mod surface_contract;
#[path = "mission/task.rs"]
pub mod task;
#[path = "mission/task_packet.rs"]
pub mod task_packet;
#[path = "mission/task_registry.rs"]
pub mod task_registry;
#[path = "team/team_cron_registry.rs"]
pub mod team_cron_registry;
#[path = "team/team_discovery.rs"]
pub mod team_discovery;
#[path = "team/team_execution.rs"]
pub mod team_execution;
#[path = "team/team_runtime.rs"]
pub mod team_runtime;
#[path = "tooling/tool_cache.rs"]
pub mod tool_cache;
#[path = "tooling/tool_dispatch.rs"]
pub mod tool_dispatch;
#[path = "tooling/tool_execution_plan.rs"]
pub mod tool_execution_plan;
#[path = "tooling/tool_invocation.rs"]
pub mod tool_invocation;
#[path = "tooling/tool_ledger.rs"]
pub mod tool_ledger;
#[path = "tooling/tool_memory.rs"]
pub mod tool_memory;
#[path = "tooling/tool_orchestrator.rs"]
pub mod tool_orchestrator;
#[path = "policy/trust_resolver.rs"]
pub mod trust_resolver;
#[path = "conversation/turn_supervisor.rs"]
pub mod turn_supervisor;
#[path = "provider/usage.rs"]
mod usage;
#[path = "infrastructure/worker_boot.rs"]
pub mod worker_boot;

pub use agent::{
    ProductionExecutor, SubAgentConfig, SubAgentError, SubAgentExecutor, SubAgentProgressCallback,
    SubAgentResult, SubAgentToolMode,
};
pub use agent_backend::{
    AgentExecutionBackendKind, AgentExecutionCommand, AgentExecutionCommandKind,
    AgentExecutionCommandReceipt, AgentExecutionEventEnvelope, AgentProcessJsonlSpec,
};
pub use agent_collaboration::{
    AgentTaskTrace, AgentTeam, CollaborationBoard, CollaborationContextResult, CollaborationOps,
    CollaborationOrchestrator, CollaborationReviewPacket, CollaborationScorecard,
    CollaborationTask, MemoryPulseCandidate, MemoryPulseKind, SharedBoardEntry, SubTask,
};
pub use agent_discussion::{
    ConsensusMethod, ConsensusResult, Contribution, Discussion, DiscussionEngine, DiscussionPhase,
};
pub use agent_event_bus::{global_agent_event_bus, AgentEventBus, AgentProgressEvent};
pub use agent_kernel::{AgentGraphError, AgentRunGraph};
pub use agent_lifecycle::{
    agent_store_dir, build_agent_system_prompt, classify_lane_failure, derive_agent_state,
    global_agent_lifecycle_service, iso8601_now, maybe_commit_provenance, normalize_subagent_type,
    persist_agent_terminal_state, prepare_agent_job, resolve_agent_model, slugify_agent_name,
    spawn_provider_agent, AgentCommandReceipt, AgentJob, AgentLifecycleEvent,
    AgentLifecycleService, AgentSnapshot, SpawnAgentRequest, DEFAULT_AGENT_MAX_ITERATIONS,
    DEFAULT_AGENT_MODEL, DEFAULT_AGENT_SYSTEM_DATE,
};
pub use agent_mailbox::{
    global_agent_task_mailbox, AgentTask, AgentTaskMailboxService, AgentTaskReceipt,
    AgentTaskStatus,
};
pub use agent_protocol::{
    AgentEvidence, AgentMergeDecision, AgentMessage, AgentNodeStatus, AgentReview, AgentRole,
    AgentTaskNode, ReviewVerdict,
};
pub use agent_workgraph::{
    AgentWorkGraph, WorkGraphEdge, WorkGraphEdgeKind, WorkGraphNode, WorkGraphNodeKind,
    WorkGraphRef, WorkGraphStatus,
};
pub use autonomy_profile::{
    ApprovalPolicy as AutonomyApprovalPolicy, AutonomyBudget, AutonomyDecision,
    AutonomyDecisionInput, AutonomyDecisionKind, AutonomyProfileCatalog, AutonomyProfileId,
    AutonomyProfileSpec, InterruptionPolicy as AutonomyInterruptionPolicy,
};
pub use bash::{execute_bash, BashCommandInput, BashCommandOutput};
pub use bootstrap::{BootstrapPhase, BootstrapPlan};
pub use branch_lock::{detect_branch_lock_collisions, BranchLockCollision, BranchLockIntent};
pub use capability_manifest::{
    runtime_capabilities_response, runtime_capabilities_response_with_detail,
    runtime_capability_primer, RuntimeCapability, RuntimeCapabilityManifest,
};
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
    CowdEvent, CowdEventBus, RunModelTelemetry, RuntimePolicyDecisionSummary,
    RuntimeWorkGraphSummary,
};
pub use cross_plane_policy::{
    ConnectorActionContext, ConnectorDecisionEvidence, CrossPlaneAction, CrossPlaneAuditRecord,
    CrossPlaneControlPlane, CrossPlaneDecisionEvidence, CrossPlaneDispatchOutcome,
    CrossPlaneDispatchTarget, CrossPlaneExecutionReceipt, CrossPlaneGrant,
    CrossPlaneIdentityBinding, CrossPlaneOutboundMessagePlan, CrossPlanePolicyConfig,
    CrossPlanePolicyDecision, CrossPlanePolicyEngine, CrossPlaneResolvedIdentity,
    CrossPlaneSummary, GrantType, IdentityTrust, PolicyDecisionKind,
};
pub use evidence_planner::{
    evidence_plan_prompt, plan_evidence, EvidenceAcquisitionMode, EvidencePlan,
};
pub use execution_core::{
    build_runtime_execution_decision, execution_mode_catalog_response, rewoo_plan_for_intent,
    runtime_execution_guidance_prompt, runtime_orchestration_action_guidance,
    runtime_orchestration_actions, tool_dag_from_rewoo, DeliberationMode, DeliberationPlan,
    ExecutionModeCatalog, ReflexionRecord, ReflexionTrigger, RewooEvidencePlan,
    RewooEvidenceResult, RewooEvidenceStep, RewooObservation, RewooSolverContract,
    RuntimeEvidenceSummary, RuntimeExecutionActionHint, RuntimeExecutionBinding,
    RuntimeExecutionDecision, RuntimeExecutionModeCandidate, RuntimeExecutionModeSpec,
    RuntimeExecutionReportSpec, ToolDagEdge, ToolDagEdgeKind, ToolDagPlan, ToolDagSafetySummary,
    ToolDagTask,
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
#[path = "conversation/host.rs"]
pub mod host;
pub use host::{StandardRuntimeHost, StandardRuntimeHostConfig};
pub use intent_planner::{classify_intent, IntentPlan, TaskIntent};
pub use joint_problem_solving::{
    AgentDiscussion, DiscussionTurn, JpsOps, PhaseStatus, PipelineResult, ProblemSolvingConfig,
    ProblemSolvingPipeline, ProblemStatement, Solution, SolutionEvaluation, SolutionScore,
};
pub use lane_events::{
    dedupe_superseded_commit_events, LaneCommitProvenance, LaneEvent, LaneEventBlocker,
    LaneEventName, LaneEventStatus, LaneFailureClass,
};
pub use runtime_harness::{RuntimeAiKernel, RuntimeAiKernelTrace};

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
pub use mission_control::{
    MissionControlAction, MissionControlAgentNode, MissionControlApprovalNode,
    MissionControlCommand, MissionControlCommandReceipt, MissionControlCommandStatus,
    MissionControlCommandTarget, MissionControlEventDigest, MissionControlEventLine,
    MissionControlProjection, MissionControlRuntime, MissionControlSessionNode,
    MissionControlStewardNode, MissionControlSummary, MissionControlTeamNode, MissionWorkspace,
};
pub use mission_evidence::{global_mission_evidence_bus, MissionEvidenceBus, MissionEvidenceRef};
pub use mission_runtime::{
    global_mission_runtime, MissionCommandReceipt, MissionEvent, MissionProjection,
    MissionRoutedCommand, MissionRuntime, MissionSessionCommand, MissionSessionCommandKind,
    MissionSessionCommandStatus, MissionSessionCommandSummary, MissionSessionSnapshot,
    MissionSessionStatus, StartMissionSessionRequest,
};
pub use module_map::{
    runtime_module_map, runtime_module_names_by_domain, RuntimeDomain, RuntimeModuleDescriptor,
};
pub use mutation_plan::{
    apply_mutations, preview_mutations, FileMutationApplied, FileMutationPreview,
    MutationApplyInput, MutationApplyOutput, MutationEdit, MutationPreview, MutationPreviewInput,
};
pub use orchestration::{
    handle_runtime_orchestration_request, plan_runtime_collaboration_decision,
    runtime_orchestration_response, RuntimeOrchestrationAction, RuntimeOrchestrationDecision,
    RuntimeOrchestrationRequest, RuntimeOrchestrationResult,
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
pub use provider::{detect_provider_kind, model_context_window_with_overrides, ProviderKind};
pub use provider_registry::{
    init_global_providers, list_all_models, list_all_providers, list_models_for_provider,
    resolve_global_provider,
};
pub use provider_runtime_client::{
    push_provider_output_block, ProviderOutputContentBlock, ProviderRuntimeClient,
    ProviderToolDefinition,
};
pub use recovery::{
    RecoveryAppliedAction, RecoveryExecutionReport, RecoveryExecutor, RecoveryFailedAction,
    RecoveryPlan, RecoveryPlanner, RecoverySkippedAction,
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
pub use runtime_event_replay::{
    RuntimeEventReplayer, RuntimeRecoveryAction, RuntimeRecoveryActionKind, RuntimeReplayReport,
};
pub use runtime_event_store::{
    global_runtime_event_store, record_runtime_event, DurableRuntimeEvent, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
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
pub use session_execution::{
    CrossSessionBridgeReceipt, CrossSessionMessage, SessionCommandDispatchReceipt,
    SessionDispatchMode, SessionExecutionPlane, SessionExecutionPolicy, SessionExecutionReport,
    SessionExecutionSkip, SessionLeaseState,
};
pub use session_relation_graph::{
    global_session_relation_graph, SessionProxy, SessionRelation, SessionRelationGraph,
    SessionRelationKind, SessionRouteCommand, SessionRouteReceipt,
};
pub use skill::{
    memory_candidate_from_skill_activation, RuntimeSkillCandidate, SkillActivationRecord,
    SkillMemoryPolicy,
};
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
pub use steward_runtime::{
    global_steward_runtime_service, StartStewardRuntimeRequest, StewardEvent, StewardHandoffReport,
    StewardLoopReport, StewardRuntimeProjection, StewardRuntimeService, StewardSession,
    StewardStatus, TickStewardRuntimeRequest,
};
pub use steward_scheduler::{
    global_steward_decision_ledger, StewardDecisionLedger, StewardDecisionLedgerRecord,
    StewardScheduler, StewardSchedulerConfig, StewardSchedulerHandoffSummary,
    StewardSchedulerProjection, StewardSchedulerTickReport,
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
pub use team_execution::{
    CollaborationTemplateRuntimeSpec, TeamExecutionDependency, TeamExecutionLoop,
    TeamExecutionPlan, TeamExecutionReport, TeamExecutionRoleSpec,
};
pub use team_runtime::{
    global_team_runtime_service, CollaborationAgentRunProjection, CollaborationRunProjection,
    StartTeamRuntimeAgentRequest, StartTeamRuntimeRequest, TeamRuntimeAgent,
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
pub use turn_supervisor::{
    fingerprint_tool_call, SupervisorDecision, ToolCallFingerprint, ToolProgressObservation,
    TurnSupervisor,
};
pub use usage::{
    pricing_for_model, ModelPerformanceRegistry, ModelPerformanceStats, ModelRouteCandidate,
    ModelRouteDecision, ModelRouteIntent, UsageTracker,
};
pub use wave::{
    DependencyGraph, ErrorPolicy, TaskContext, TaskId, TaskResult, TaskStatus, Wave, WaveConfig,
    WaveError, WaveExecutor, WaveOrchestrator, WaveResult, WaveStatus, WaveTask,
};
pub use worker_boot::{
    StartupEvidenceBundle, StartupFailureClassification, Worker, WorkerEvent, WorkerEventKind,
    WorkerEventPayload, WorkerFailure, WorkerFailureKind, WorkerPromptTarget, WorkerReadySnapshot,
    WorkerRegistry, WorkerStatus, WorkerTaskReceipt, WorkerTrustResolution,
};

#[path = "conversation/cached_prompt.rs"]
pub mod cached_prompt;
pub use budget_policy::{
    clamp_context_budget_ratio_bp, resolve_compact_threshold, resolve_context_budget_tokens,
    MemoryBudgetLease, RuntimeBudgetInputs, RuntimeBudgetPlan, RuntimeControlBudgetLease,
    ToolResultBudgetLease, DEFAULT_CONTEXT_BUDGET_RATIO_BP, DEFAULT_SUBAGENT_BUDGET_TOKENS,
};
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
