#![deny(deprecated)]
#![deny(unused_imports)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
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
#[path = "approval/approval_queue.rs"]
pub mod approval_queue;
#[path = "infrastructure/hooks.rs"]
mod hooks;
#[path = "infrastructure/json.rs"]
mod json;
pub use json::JsonValue;
#[path = "context/budget_policy.rs"]
pub mod budget_policy;
#[path = "context/evidence/mod.rs"]
pub mod context_evidence;
#[path = "context/ledger/mod.rs"]
pub mod context_ledger;
#[path = "context/context_profiler.rs"]
pub mod context_profiler;
#[path = "context/context_runtime.rs"]
pub mod context_runtime;
#[path = "context/tool_exposure.rs"]
pub mod context_tool_exposure;
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
#[path = "agent/managed_agent.rs"]
pub mod managed_agent;
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
#[path = "mission/command_router.rs"]
pub mod mission_command_router;
#[path = "mission/mission_control.rs"]
pub mod mission_control;
#[path = "mission/mission_evidence.rs"]
pub mod mission_evidence;
#[path = "mission/mission_runtime.rs"]
pub mod mission_runtime;
#[path = "mission/runtime_port.rs"]
pub mod mission_runtime_port;
#[path = "mission/mission_schedule.rs"]
pub mod mission_schedule;
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
#[path = "conversation/prompt_assembly.rs"]
mod prompt_assembly;
#[path = "provider/provider_pool.rs"]
pub mod provider_pool;
#[path = "recovery/recovery.rs"]
pub mod recovery;
#[path = "recovery/recovery_recipes.rs"]
pub mod recovery_recipes;
#[path = "infrastructure/remote.rs"]
mod remote;
#[path = "context/resources/mod.rs"]
pub mod resources;
#[path = "infrastructure/runtime_control.rs"]
pub mod runtime_control;
#[path = "infrastructure/sandbox.rs"]
pub mod sandbox;
#[path = "session/session.rs"]
mod session;
pub use session::workspace_sessions_dir;
#[path = "agent/agent.rs"]
pub mod agent;
#[path = "agent/agent_capability.rs"]
pub mod agent_capability;
#[path = "agent/catalog.rs"]
pub mod agent_catalog;
#[path = "agent/evaluation.rs"]
pub mod agent_evaluation;
#[path = "agent/in_process_worker.rs"]
pub mod agent_in_process_worker;
#[path = "agent/model_selector.rs"]
pub mod agent_model_selector;
#[path = "agent/process_jsonl_adapter.rs"]
pub mod agent_process_jsonl_adapter;
#[path = "agent/result_validator.rs"]
pub mod agent_result_validator;
#[path = "agent/run_handle.rs"]
pub mod agent_run_handle;
#[path = "agent/runtime.rs"]
pub mod agent_runtime;
#[path = "approval/approval_gate.rs"]
pub mod approval_gate;
#[path = "policy/autonomy_profile.rs"]
pub mod autonomy_profile;
#[path = "session/checkpoint.rs"]
pub mod checkpoint;
#[path = "agent/collaboration_template.rs"]
pub mod collaboration_template;
#[path = "conflict/conflict_arbiter.rs"]
pub mod conflict_arbiter;
#[path = "context/context_fanout.rs"]
pub mod context_fanout;
#[path = "infrastructure/cowd_event.rs"]
pub mod cowd_event;
#[path = "policy/cross_plane_policy.rs"]
pub mod cross_plane_policy;
#[path = "agent/definition_registry.rs"]
pub mod definition_registry;
pub use definition_registry::AgentDefinitionDraftReceipt;
#[path = "infrastructure/eval_gate.rs"]
pub mod eval_gate;
#[path = "context/evidence_planner.rs"]
pub mod evidence_planner;
#[path = "evolution/mod.rs"]
pub mod evolution;
#[path = "execution_core/mod.rs"]
pub mod execution_core;
#[path = "execution_core/execution_live.rs"]
pub mod execution_live;
#[path = "projection/mod.rs"]
pub mod execution_projection;
#[path = "infrastructure/execution_scheduler.rs"]
pub mod execution_scheduler;
#[path = "context/fact_extraction.rs"]
pub mod fact_extraction;
#[path = "session/input_classifier.rs"]
pub mod input_classifier;
#[path = "agent/intent_planner.rs"]
pub mod intent_planner;
#[path = "infrastructure/lane_completion.rs"]
pub mod lane_completion;
#[path = "session/mission_command_interpreter.rs"]
pub mod mission_command_interpreter;
#[path = "infrastructure/mutation_plan.rs"]
pub mod mutation_plan;
#[path = "orchestration/mod.rs"]
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
#[path = "provider/transport_policy.rs"]
pub mod provider_transport_policy;
#[path = "infrastructure/quality_gate.rs"]
pub mod quality_gate;
#[path = "context/reality_decision.rs"]
pub mod reality_decision;
#[path = "context/reality_recall_port.rs"]
pub mod reality_recall_port;
#[path = "infrastructure/release_gate.rs"]
pub mod release_gate;
#[path = "recovery/runtime_event_replay.rs"]
pub mod runtime_event_replay;
#[cfg(feature = "test-fixtures")]
#[path = "recovery/runtime_event_store.rs"]
pub mod runtime_event_store;
#[cfg(not(feature = "test-fixtures"))]
#[path = "recovery/runtime_event_store.rs"]
pub(crate) mod runtime_event_store;
#[path = "mission/runtime_harness.rs"]
pub mod runtime_harness;
#[path = "security/mod.rs"]
pub mod security;
#[path = "session/session_execution.rs"]
pub mod session_execution;
#[path = "session/session_input.rs"]
pub mod session_input;
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
pub mod structured_data;
#[path = "conversation/summary_compression.rs"]
pub mod summary_compression;
#[path = "infrastructure/surface_contract.rs"]
pub mod surface_contract;
#[path = "mission/task.rs"]
pub mod task;
#[path = "mission/task_packet.rs"]
pub mod task_packet;
#[path = "team/agent_selector.rs"]
pub mod team_agent_selector;
#[path = "team/agent_task.rs"]
pub mod team_agent_task;
#[path = "team/definition/mod.rs"]
pub mod team_definition;
#[path = "team/instantiation.rs"]
pub mod team_instantiation;
#[path = "team/l4_promotion.rs"]
pub mod team_l4_promotion;
#[path = "team/legacy_import.rs"]
pub mod team_legacy_import;
#[path = "team/profile_migration.rs"]
pub mod team_profile_migration;
#[path = "team/projection.rs"]
pub mod team_projection;
#[path = "team/result_reducer.rs"]
pub mod team_result_reducer;
#[path = "team/team_runtime.rs"]
pub mod team_runtime;
#[path = "team/working_state.rs"]
pub mod team_working_state;
#[path = "tooling/tool_dispatch.rs"]
pub mod tool_dispatch;
#[path = "tooling/tool_execution_plan.rs"]
pub mod tool_execution_plan;
#[path = "tooling/tool_host.rs"]
pub mod tool_host;
#[path = "tooling/tool_invocation.rs"]
pub mod tool_invocation;
#[path = "tooling/tool_memory.rs"]
pub mod tool_memory;
#[path = "tooling/tool_orchestrator.rs"]
pub mod tool_orchestrator;
#[path = "tooling/tool_policy.rs"]
pub mod tool_policy;
#[path = "policy/trust_resolver.rs"]
pub mod trust_resolver;
#[path = "conversation/turn_inbox.rs"]
pub mod turn_inbox;
#[path = "upgrade/mod.rs"]
pub mod upgrade;
#[path = "provider/usage.rs"]
mod usage;

pub use agent::binding::{
    AgentBindingCompiler, AgentBindingError, AgentBindingRequest, CompiledAgentBinding,
};
pub use agent::{
    SubAgentConfig, SubAgentError, SubAgentExecutor, SubAgentProgressCallback, SubAgentResult,
    SubAgentToolMode,
};
pub use agent_capability::{
    AgentCapabilityRequest, ResolvedAgentCapability, resolve_agent_capability,
};
pub use agent_catalog::{AgentCatalog, AgentCatalogEntry};
pub use agent_evaluation::{AgentRunEvaluation, AgentSelfModel, project_self_models};
pub use agent_in_process_worker::InProcessAgentWorker;
pub use agent_model_selector::{AgentModelSelection, AgentModelSelectionError, AgentModelSelector};
pub use agent_process_jsonl_adapter::{ProcessJsonlAdapter, ProcessJsonlSpec};
pub use agent_result_validator::{AgentResultValidationError, validate_agent_return};
pub use agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
pub use agent_runtime::{
    AgentRunSnapshot, AgentRuntime, AgentRuntimeBackend, AgentRuntimeResolver,
    LegacyAgentImportReport, LegacyAgentStateRecord,
};
pub use approval_queue::{
    ApprovalDecisionCommand, ApprovalQueue, ApprovalSource, ApprovalSourceKind,
    ApprovalTimeoutPolicy, GlobalApprovalDecisionReceipt, GlobalApprovalRequest,
    GlobalApprovalStatus, SubmitGlobalApprovalRequest,
};
pub use autonomy_profile::{
    ApprovalPolicy as AutonomyApprovalPolicy, AutonomyBudget, AutonomyDecision,
    AutonomyDecisionInput, AutonomyDecisionKind, AutonomyProfileCatalog, AutonomyProfileId,
    AutonomyProfileSpec, InterruptionPolicy as AutonomyInterruptionPolicy,
};
pub use bash::{BashCommandInput, BashCommandOutput, execute_bash};
pub use bootstrap::{BootstrapPhase, BootstrapPlan};
pub use branch_lock::{BranchLockCollision, BranchLockIntent, detect_branch_lock_collisions};
pub use capability_manifest::{
    RuntimeActionContract, RuntimeCapability, RuntimeCapabilityCatalog, RuntimeCapabilityManifest,
    RuntimeOperation, RuntimeOperationGroup, RuntimeTemplateSummary, runtime_capabilities_response,
    runtime_capabilities_response_with_detail, runtime_capabilities_response_with_leased_decision,
    runtime_capabilities_response_with_leased_decision_and_tools, runtime_capability_primer,
};
pub use checkpoint::{
    CheckpointCreateInput, CheckpointDiffInput, CheckpointDiffOutput, CheckpointListOutput,
    CheckpointRestoreInput, CheckpointSummary, checkpoint_create, checkpoint_diff, checkpoint_list,
    checkpoint_restore,
};
pub use collaboration_template::{
    CollaborationDecision, CollaborationTemplateId, CollaborationTemplateMatcher,
};
pub use compact::{
    CompactionConfig, CompactionResult, estimate_session_tokens, format_compact_summary,
    get_compact_continuation_message, should_compact,
};
pub use config::{
    ApprovalConfig, COWD_SETTINGS_SCHEMA_NAME, CompressionConfig,
    ConfigDiagnostic as RuntimeConfigDiagnostic, ConfigDiagnosticSeverity, ConfigEntry,
    ConfigError, ConfigLoadResult, ConfigLoader, ConfigSource, DomainProfile, GateAutoFixConfig,
    GatewayCapacityConfig, GatewayConfig, McpConfigCollection, McpManagedProxyServerConfig,
    McpOAuthConfig, McpRemoteServerConfig, McpSdkServerConfig, McpServerConfig,
    McpStdioServerConfig, McpTransport, McpWebSocketServerConfig, MemoryConfig,
    PlatformConfig as GatewayPlatformConfig, ResolvedPermissionMode, RuntimeConfig,
    RuntimeControlConfig, RuntimeFeatureConfig, RuntimeHookConfig, RuntimePermissionRuleConfig,
    RuntimePluginConfig, ScopedMcpServerConfig, SessionResetPolicy, redact_serde_json,
};
pub use config_validate::{
    ConfigDiagnostic, DiagnosticKind, ValidationResult, check_unsupported_format,
    format_diagnostics, validate_config_file,
};
pub use conflict_arbiter::{
    ConflictArbiter, ConflictDecisionKind, ConflictResolutionReceipt, ConflictResolutionRequest,
    ConflictSeverity, ConflictSourceKind,
};
pub use context_evidence::{
    AuditProjection, ModelReceipt, audit_projection as project_evidence_audit,
};
pub use context_fanout::{ContextFanoutPlan, FanoutToolCall, plan_context_fanout};
pub use context_tool_exposure::{ToolExposurePlanner, ToolExposurePolicy, ToolExposureState};
pub use conversation::{
    ApiClient, ApiRequest, AssistantEvent, AutoCompactionEvent, CancellationToken,
    ConversationRuntime, MemoryCallback, PromptCacheEvent, ProviderContextInventory, RuntimeError,
    StaticToolExecutor, ToolCallback, ToolError, ToolExecutor, TurnSummary, build_cc_memory_config,
    image_user_message_from_path,
};
pub use cowd_event::{
    CowdEvent, CowdEventBus, CowdExecutionContext, CowdExecutionScope, RunModelTelemetry,
    RuntimeExecutionGraphSummary, RuntimePolicyDecisionSummary,
};
pub use cross_plane_policy::{
    ConnectorActionContext, ConnectorDecisionEvidence, CrossPlaneAction, CrossPlaneAuditRecord,
    CrossPlaneControlPlane, CrossPlaneDecisionEvidence, CrossPlaneDispatchOutcome,
    CrossPlaneDispatchTarget, CrossPlaneExecutionReceipt, CrossPlaneGrant,
    CrossPlaneIdentityBinding, CrossPlaneOutboundMessagePlan, CrossPlanePolicyConfig,
    CrossPlanePolicyDecision, CrossPlanePolicyEngine, CrossPlaneResolvedIdentity,
    CrossPlaneSummary, GrantType, IdentityTrust, PolicyDecisionKind,
};
pub use definition_registry::{
    DefinitionRegistryError, RuntimeDefinitionRegistry, RuntimeTeamTemplateCatalogEntry,
};
pub use evidence_planner::{
    EvidenceAcquisitionMode, EvidencePlan, evidence_plan_prompt, plan_evidence,
};
pub use execution_core::{
    CrossPlaneRuntimeError, CrossPlaneRuntimeService, DeliberationMode, DeliberationPlan,
    ExecutionCommitService, ExecutionCompileRequest, ExecutionGraphCompiler, ExecutionGraphHost,
    ExecutionGraphHostReceipt, ExecutionGraphRunner, ExecutionGraphStateStore,
    ExecutionPatternCatalog, ExecutionStartupRecoveryError, ExecutionStartupRecoveryRecord,
    ExecutionStartupRecoveryReport, ReflexionRecord, ReflexionTrigger, RewooEvidencePlan,
    RewooEvidenceResult, RewooEvidenceStep, RewooObservation, RewooSolverContract,
    RuntimeActionSelectionReport, RuntimeCompileTarget, RuntimeEventReader, RuntimeEvidenceSummary,
    RuntimeExecutionActionHint, RuntimeExecutionDecision, RuntimeExecutionPatternCandidate,
    RuntimeExecutionPatternSpec, RuntimeExecutionReportSpec, RuntimeServices,
    RuntimeServicesBuilder, RuntimeServicesError, SessionTerminalDeliveryPort,
    StrategyDecisionEngine, StrategyLease, StrategyResourceHealth, TaskLifecycleEvent,
    TaskLifecycleKind, ToolDagEdge, ToolDagEdgeKind, ToolDagPlan, ToolDagSafetySummary,
    ToolDagTask, TurnStrategyActualOutcome, TurnStrategyDecisionState, TurnStrategyDecisionStatus,
    action_selection_report_for_decision, build_runtime_action_selection_report,
    build_runtime_execution_decision, execution_pattern_catalog_response, rewoo_plan_for_intent,
    runtime_execution_guidance_prompt, runtime_execution_guidance_prompt_with_tool_exposure,
    runtime_orchestration_action_guidance, runtime_orchestration_actions, tool_dag_from_rewoo,
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
pub use harness_contract::agent::AgentLifecycleEvent;
pub use hooks::{
    HOOK_PREVIEW_CHAR_LIMIT, HookAbortSignal, HookEvent, HookProgressEvent, HookProgressReporter,
    HookRunResult, HookRunner, format_hook_output,
};
pub use team_agent_task::{
    AgentTask, AgentTaskCompletionReceipt, AgentTaskOutcome, AgentTaskQualityStatus,
    AgentTaskStatus,
};
#[path = "conversation/host.rs"]
pub mod host;
pub use host::{
    StandardRuntimeHost, StandardRuntimeHostConfig, TurnIngressRef, submit_owned_conversation_turn,
};
pub use input_classifier::{RuntimeInputState, classify_session_input};
pub use intent_planner::{IntentPlan, TaskIntent, classify_intent};
pub use lane_events::{
    LaneCommitProvenance, LaneEvent, LaneEventBlocker, LaneEventName, LaneEventStatus,
    LaneFailureClass, dedupe_superseded_commit_events,
};
pub use managed_agent::{
    FencedEffectOutboxRecord, FencedEffectStatus, ManagedAgentDispatchReport,
    ManagedAgentDispatcher, ManagedAgentEffectPermit, ManagedAgentHealth, ManagedAgentHealthStatus,
    ManagedAgentInvocation, ManagedAgentInvocationStatus, ManagedAgentInvocationTrigger,
    ManagedAgentRuntimeDispatchReport,
};
pub use runtime_harness::{RuntimeAiKernel, RuntimeAiKernelTrace};

pub(crate) use evolution::EvolutionCandidateRegistration;
pub use evolution::{
    CanaryObservationReport, CanaryRolloutPolicy, EvaluationDirection,
    EvaluationPolicyChangeIntent, EvaluationPolicyChangeReview, EvolutionCandidateIntent,
    EvolutionCandidateKind, EvolutionCandidateLifecycle, EvolutionCandidateSubject,
    EvolutionCapabilityGoal, EvolutionComparisonDimension, EvolutionComparisonReportV2,
    EvolutionDiagnosis, EvolutionDiagnosisEngine, EvolutionDiagnosisStore, EvolutionEvalRunner,
    EvolutionGovernanceCandidate, EvolutionGovernanceError, EvolutionGovernanceService,
    EvolutionLifecycleDraft, EvolutionLifecycleService, EvolutionMission, EvolutionMissionStatus,
    EvolutionMissionStore, EvolutionPlanDraft, EvolutionProposal, EvolutionProposalKind,
    EvolutionProposalRisk, EvolutionProposalStore, EvolutionReleaseAssignment,
    EvolutionRootCauseKind, EvolutionSignal, EvolutionSignalCollector, EvolutionSignalInput,
    EvolutionSignalSeverity, EvolutionSignalSource, EvolutionSignalStore, EvolutionSignalType,
    EvolutionSkillDraft, EvolutionTriageCluster, EvolutionTriageService, ReleaseChangeAction,
    ReleaseChangeRequest, ReleaseChangeReview, ReleaseChangeReviewClass,
    ReleaseChangeReviewDecision, ReleaseChangeReviewStatus, candidate_kind_from_proposal,
    candidate_kinds_from_root_cause,
};
#[cfg(feature = "test-fixtures")]
pub use execution_core::RuntimeFixtureEventPort;
pub use harness_contract::turn::{
    InputRelationKind, InputRelationProposal, SessionDispatchAction, SessionDispatchCommand,
    SessionDispatchReceipt, SessionHandoff, SessionResultPacket,
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
pub use mission_command_interpreter::{
    MissionCommandExecutionReceipt, MissionCommandInterpretRequest, MissionCommandInterpretation,
    MissionCommandInterpreter, MissionCommandTargetKind, MissionInterpretedCommand,
};
pub use mission_command_router::execute_mission_command;
pub use mission_control::{
    MissionControlAction, MissionControlAgentNode, MissionControlApprovalNode,
    MissionControlCommand, MissionControlCommandReceipt, MissionControlCommandStatus,
    MissionControlCommandTarget, MissionControlEventDigest, MissionControlEventLine,
    MissionControlProjection, MissionControlRuntime, MissionControlSessionNode,
    MissionControlSummary, MissionControlTeamNode, MissionWorkspace,
};
pub use mission_evidence::{MissionEvidenceBus, MissionEvidenceRef};
pub use mission_runtime::{
    MissionEvent, MissionProjection, MissionRuntime, MissionSessionSnapshot,
    MissionSessionStateReceipt, MissionSessionStatus, StartMissionSessionRequest,
};
pub use mission_runtime_port::MissionRuntimePort;
pub use mission_schedule::{
    CreateMissionScheduleRequest, MissionScheduleDispatchReport, MissionScheduleStore,
    MissionScheduleTickReport, UpdateMissionScheduleRequest,
};
pub use module_map::{
    RuntimeDomain, RuntimeModuleDescriptor, runtime_module_map, runtime_module_names_by_domain,
};
pub use mutation_plan::{
    FileMutationApplied, FileMutationPreview, MutationApplyInput, MutationApplyOutput,
    MutationEdit, MutationPreview, MutationPreviewInput, apply_mutations, preview_mutations,
};
pub use orchestration::{
    CompiledOrchestration, RuntimeOrchestrationAction, RuntimeOrchestrationConstraints,
    RuntimeOrchestrationDecision, RuntimeOrchestrationRequest, RuntimeOrchestrationResult,
    handle_runtime_orchestration_request, handle_runtime_orchestration_request_with_decision,
    runtime_orchestration_response, runtime_orchestration_response_with_decision,
    submit_runtime_orchestration_request,
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
    COWD_IDENTITY_CONTRACT_VERSION, ContextFile, CowdIdentityContract, ProjectContext,
    PromptBuildError, SYSTEM_PROMPT_DYNAMIC_BOUNDARY, SystemPromptBuilder, load_system_prompt,
    prepend_bullets,
};
pub use prompt_assembly::{PromptAssembly, PromptContextPacket};
pub use provider::{ProviderKind, detect_provider_kind, model_context_window_with_overrides};
pub use provider_registry::{
    ProviderRegistry, ProviderRegistryDiagnostics, ProviderRegistryRejected,
    ProviderRegistrySnapshot, ProviderRegistryUpdate,
};
pub use provider_runtime_client::{
    ProviderOutputContentBlock, ProviderRuntimeClient, ProviderToolDefinition,
    push_provider_output_block,
};
pub use provider_transport_policy::ProviderTransportPolicy;
pub use reality_decision::{
    RealityContextBudgetPlan, RealityFactPlan, RealityFactPlanItem, RealityKnowledgeDecision,
    RealityMemoryDecision, RealityRecallQualityReport, RealityRuntimeDecision,
};
pub use reality_recall_port::{
    MatrixScenarioPort, MatrixScenarioStartRequest, RealityRecallPort, RealityRecallReport,
    RealityRecallSourceStatus,
};
pub use recovery::{
    RecoveryAppliedAction, RecoveryExecutionReport, RecoveryExecutor, RecoveryFailedAction,
    RecoveryPlan, RecoveryPlanner, RecoverySkippedAction,
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
pub use resources::{
    MAX_RESOURCE_BYTES, ResourceCapabilityIndex, ResourceCapabilitySnapshot, ResourceEnvelope,
    ResourceEvidence, ResourceHint, ResourceKind, ResourcePromptHint, ResourceStore,
    register_resource_from_path, render_resource_context_markdown, resource_hint,
};
pub use runtime_event_replay::{
    RuntimeEventReplayer, RuntimeRecoveryAction, RuntimeRecoveryActionKind,
    RuntimeRecoveryCandidate, RuntimeReplayReport, candidate_from_action,
};
#[cfg(not(feature = "test-fixtures"))]
#[allow(unused_imports)]
pub(crate) use runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventStore,
    RuntimeTransactionEventInput,
};
pub use runtime_event_store::{
    DurableRuntimeEvent, RuntimeEventRef, RuntimeEventScope, RuntimeEventStoreError,
    RuntimeSessionOutboxFailureClass, RuntimeSessionOutboxHealth, RuntimeSessionOutboxRecord,
    SessionTerminalInput,
};
#[cfg(feature = "test-fixtures")]
pub use runtime_event_store::{RuntimeEventInput, RuntimeEventStore};
pub use sandbox::{
    ContainerEnvironment, FilesystemIsolationMode, SandboxConfig, SandboxDetectionInputs,
    SandboxRequest, SandboxStatus, detect_container_environment, detect_container_environment_from,
    resolve_sandbox_status, resolve_sandbox_status_for_request,
};
pub use security::{
    DecisionLeaseExpectation, PrincipalVerificationError, PrincipalVerifier, VerifiedDecisionLease,
    VerifiedPrincipal,
};
pub use session::{
    ContentBlock, ConversationMessage, MessageEvent, MessageRole, Session, SessionCompaction,
    SessionError, SessionEventLog, SessionFork, SessionPromptEntry,
};
pub use session_execution::{
    SESSION_DISPATCH_EXECUTOR, SessionDispatchMode, SessionExecutionPolicy,
    SessionHandoffResolution, SessionIngressExecutionReceipt, SessionIngressExecutor,
    SessionInputRouteReceipt, SessionInputRouteReport, SessionInputRouter, SessionInputRouterError,
    SessionRecoveryCandidate, session_ingress_graph_id,
};
pub use session_input::{SessionInputRecord, SessionInputStream};
pub use session_relation_graph::{
    SessionProxy, SessionRelation, SessionRelationGraph, SessionRelationKind, SessionRouteCommand,
    SessionRouteReceipt,
};
pub use skill::{
    RuntimeSkillCandidate, RuntimeSkillCatalog, RuntimeSkillPromptAsset, SkillActivationRecord,
    SkillMemoryPolicy, memory_candidate_from_skill_activation,
    skill_memory_candidate_session_event,
};
pub use source_self_audit::{
    RuntimeSourceSelfAudit, SourceRepairAction, SourceSelfAuditCheck, SourceSelfAuditReport,
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
pub use steward_agent::{
    StewardActionRequest, StewardActionStatus, StewardAgent, StewardDecisionRecord,
};
pub use task_packet::{
    TaskPacket, TaskPacketValidationError, TaskScope, ValidatedPacket, validate_packet,
};
pub use team_agent_selector::AgentSelector;
pub use team_instantiation::{ResolvedRoleSlot, TeamInstantiation, TeamInstantiationService};
pub use team_l4_promotion::{
    L4CandidateLifecycle, L4PromotionCandidate, L4PromotionReceipt, L4PromotionService,
};
pub use team_legacy_import::LegacyTeamImportReport;
pub use team_profile_migration::LegacyTeamProfileMigrationReport;
pub use team_projection::{TeamProjection, TeamProjectionReader};
pub use team_result_reducer::TeamResultReducer;
pub use team_runtime::TeamRuntime;
pub use team_working_state::{
    FocusOverlapAssessment, TeamWorkingState, TeamWorkingStateEntry, TeamWorkingStateKind,
};
pub use tool_execution_plan::{ToolExecutionMode, ToolExecutionPlan, ToolExecutionPlanTask};
pub use tool_host::{
    RuntimeExecutionHost, RuntimeToolExecutionOutcome, RuntimeToolExecutionRequest,
    RuntimeToolExecutionStatus,
};
pub use tool_invocation::{
    DEFAULT_OUTPUT_REF_MIN_LINES, ToolFailureKind, ToolInvocationRecord, ToolInvocationStatus,
    ToolOutputRef, now_ms as tool_invocation_now_ms,
};
pub use tool_memory::{ToolMemoryCandidatePolicy, memory_candidate_from_tool_invocation};
pub use tool_orchestrator::{
    ToolCachePolicy, ToolExecutionProfile, ToolSafetyCategory, classify_tool_request,
    tool_execution_profile,
};
pub use tool_policy::{ToolExecutionPolicyDecision, ToolPolicy, ToolPolicyError};
pub use trust_resolver::{TrustConfig, TrustDecision, TrustEvent, TrustPolicy, TrustResolver};
pub use upgrade::{
    ClosureUpgradeInventoryCollector, LEGACY_EXECUTION_IMPORTED, LegacyExecutionImportError,
    LegacyExecutionImportReceipt, LegacyExecutionImporter, UPGRADE_RECOVERY_REQUIRED,
    UpgradeCarrierRecord, UpgradeCarrierStatus, UpgradeCleanShutdownReceipt, UpgradeCoordinator,
    UpgradeDispositionReceipt, UpgradeError, UpgradeInventory, UpgradeInventoryCollector,
    UpgradeMaintenanceSnapshot,
};
pub use usage::{
    ModelPerformanceRegistry, ModelPerformanceStats, ModelRouteCandidate, ModelRouteDecision,
    ModelRouteIntent, UsageTracker, pricing_for_model,
};
pub use wave::{
    DependencyGraph, ErrorPolicy, TaskContext, TaskId, TaskResult, TaskStatus, Wave, WaveConfig,
    WaveError, WaveExecutor, WaveOrchestrator, WaveResult, WaveStatus, WaveTask,
};

pub use budget_policy::{
    DEFAULT_SUBAGENT_BUDGET_TOKENS, DEFAULT_SUBSYSTEM_BUDGET_RATIO_BP, MemoryBudgetLease,
    RuntimeBudgetInputs, RuntimeBudgetPlan, RuntimeControlBudgetLease, ToolOutputBudgetLease,
    clamp_context_budget_ratio_bp, resolve_context_budget_tokens,
};
pub use context_runtime::{
    AgentContextLease, AgentContextView, AgentReturnContextProjection, AgentReturnRequirement,
    AssembledContext, ContextAuthority, ContextBudgetAllocation, ContextBudgetExplanation,
    ContextBudgetReport, ContextCacheStabilityReport, ContextDegradationPath, ContextDiagnostics,
    ContextEnvelope, ContextEnvelopeRequest, ContextEpochReport, ContextIdentity, ContextItem,
    ContextLeanProbe, ContextLease, ContextMode, ContextModeCoverageEntry,
    ContextModeCoverageReport, ContextOmission, ContextPolicyAction, ContextPolicyDecision,
    ContextPolicyProposal, ContextPressureLevel, ContextProfile, ContextRole, ContextRuntimeKernel,
    ContextSegmentChange, ContextSegmentKind, ContextSegmentSnapshot, ContextSnapshot,
    ContextSnapshotDiff, ContextSourceKind, ContextSourceLifecycle, ContextSourceRef,
    ContextVisibility, ResumeContextPacket, ResumeContextSource, StableHeadComparison,
    ToolTracePacket, ToolTraceStatus, WorkspacePacket,
};
pub use runtime_control::{
    AgentControlPolicy, ContextControlPolicy, MemoryControlPolicy, MissionSchedulePolicy,
    ObservabilityPolicy, PermissionControlPolicy, RuntimeControlPolicy, TaskControlPolicy,
};
#[cfg(test)]
pub(crate) fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
