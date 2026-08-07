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
#[path = "infrastructure/git_context.rs"]
mod git_context;
#[path = "infrastructure/graph_contract.rs"]
pub mod graph_contract;
#[path = "policy/green_contract.rs"]
pub mod green_contract;
#[path = "infrastructure/wave.rs"]
pub mod wave;
pub use green_contract::GreenLevel;
#[path = "approval/coordinator.rs"]
pub mod approval_coordinator;
#[path = "approval/approval_queue.rs"]
pub mod approval_queue;
#[path = "infrastructure/hooks.rs"]
mod hooks;
#[path = "infrastructure/json.rs"]
mod json;
pub use json::JsonValue;
#[path = "context/adaptive_allocator.rs"]
pub mod adaptive_context;
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
#[path = "context/knowledge_activation.rs"]
pub mod knowledge_activation;
#[path = "context/knowledge_compliance.rs"]
pub mod knowledge_compliance;
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
#[path = "policy/policy_engine.rs"]
mod policy_engine;
#[path = "conversation/prompt.rs"]
mod prompt;
#[path = "conversation/prompt_assembly.rs"]
mod prompt_assembly;
#[path = "recovery/recovery.rs"]
pub mod recovery;
#[path = "recovery/recovery_recipes.rs"]
pub mod recovery_recipes;
#[path = "infrastructure/remote.rs"]
mod remote;
#[path = "conversation/request_compiler.rs"]
mod request_compiler;
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
#[path = "context/artifact.rs"]
pub mod artifact;
#[path = "policy/authorization_negotiator.rs"]
pub mod authorization_negotiator;
#[path = "policy/autonomy_profile.rs"]
pub mod autonomy_profile;
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
#[path = "session/history.rs"]
mod session_history;
#[cfg(test)]
#[path = "agent/test_support.rs"]
mod test_support;
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
#[path = "context/fact_extraction.rs"]
pub mod fact_extraction;
#[path = "tooling/governed_tool_executor.rs"]
pub mod governed_tool_executor;
#[path = "tooling/governed_tool_plan.rs"]
pub mod governed_tool_plan;
#[path = "session/input_classifier.rs"]
pub mod input_classifier;
#[path = "agent/intent_planner.rs"]
pub mod intent_planner;
#[path = "recovery/knowledge_candidate_projector.rs"]
pub mod knowledge_candidate_projector;
#[path = "infrastructure/lane_completion.rs"]
pub mod lane_completion;
#[path = "session/mission_command_interpreter.rs"]
pub mod mission_command_interpreter;
#[path = "orchestration/mod.rs"]
pub mod orchestration;
#[path = "recovery/outcome_projector.rs"]
pub mod outcome_projector;
#[path = "agent/pairing.rs"]
pub mod pairing;
#[path = "infrastructure/profile.rs"]
pub mod profile;
#[path = "infrastructure/projection.rs"]
pub mod projection;
#[path = "provider/outcome_selector.rs"]
pub mod provider_outcome_selector;
#[path = "provider/provider_registry.rs"]
pub mod provider_registry;
#[path = "provider/provider_resources.rs"]
pub mod provider_resources;
#[path = "provider/provider_runtime_client.rs"]
pub mod provider_runtime_client;
#[path = "provider/transcript_seal.rs"]
pub mod provider_transcript;
#[path = "provider/transport_policy.rs"]
pub mod provider_transport_policy;
#[path = "provider/transport_pool.rs"]
pub mod provider_transport_pool;
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
#[path = "provider/memory_summarizer.rs"]
pub mod runtime_memory_summarizer;
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
#[path = "session/session_runtime_port.rs"]
pub mod session_runtime_port;
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
#[path = "tooling/tool_execution_plane.rs"]
pub mod tool_execution_plane;
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
    resolve_agent_capability, AgentCapabilityRequest, ResolvedAgentCapability,
};
pub use agent_catalog::{AgentCatalog, AgentCatalogEntry};
pub use agent_evaluation::{project_self_models, AgentRunEvaluation, AgentSelfModel};
pub use agent_in_process_worker::InProcessAgentWorker;
pub use agent_model_selector::{AgentModelSelection, AgentModelSelectionError, AgentModelSelector};
pub use agent_process_jsonl_adapter::{ProcessJsonlAdapter, ProcessJsonlSpec};
pub use agent_result_validator::{validate_agent_return, AgentResultValidationError};
pub use agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
pub use agent_runtime::{
    AgentRunSnapshot, AgentRuntime, AgentRuntimeBackend, AgentRuntimeResolver,
    LegacyAgentImportReport, LegacyAgentStateRecord,
};
pub use approval_coordinator::{
    task_risk_for_effect, ApprovalCoordinator, ApprovalPendingHook, ApprovalResolution,
    ApprovalWaitRegistry,
};
pub use approval_queue::{
    ApprovalApplicationSource, ApprovalDecisionCommand, ApprovalGrant, ApprovalGrantScope,
    ApprovalGrantStatus, ApprovalQueue, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy,
    GlobalApprovalDecisionReceipt, GlobalApprovalRequest, GlobalApprovalStatus,
    SubmitGlobalApprovalRequest,
};
pub use authorization_negotiator::{AuthorizationNegotiator, AuthorizationRequest};
pub use autonomy_profile::{
    ApprovalPolicy as AutonomyApprovalPolicy, AutonomyBudget, AutonomyDecision,
    AutonomyDecisionInput, AutonomyDecisionKind, AutonomyProfileCatalog, AutonomyProfileId,
    AutonomyProfileSpec, InterruptionPolicy as AutonomyInterruptionPolicy,
};
pub use bootstrap::{BootstrapPhase, BootstrapPlan};
pub use branch_lock::{detect_branch_lock_collisions, BranchLockCollision, BranchLockIntent};
pub use capability_manifest::{
    runtime_capabilities_response, runtime_capabilities_response_with_detail,
    runtime_capabilities_response_with_leased_decision,
    runtime_capabilities_response_with_leased_decision_and_tools, runtime_capability_primer,
    RuntimeActionContract, RuntimeCapability, RuntimeCapabilityCatalog, RuntimeCapabilityManifest,
    RuntimeOperation, RuntimeOperationGroup, RuntimeTemplateSummary,
};
pub use collaboration_template::{
    CollaborationDecision, CollaborationTemplateId, CollaborationTemplateMatcher,
};
pub use compact::{
    estimate_session_tokens, format_compact_summary, get_compact_continuation_message,
    should_compact, CompactionConfig, CompactionResult,
};
pub use config::{
    redact_serde_json, AppStartupConfig, ApprovalConfig, AppsConfig, ArtifactStorageConfig,
    CompressionConfig, ConfigDiagnostic as RuntimeConfigDiagnostic, ConfigDiagnosticSeverity,
    ConfigEntry, ConfigError, ConfigLoadResult, ConfigLoader, ConfigSource, DomainProfile,
    GateAutoFixConfig, GatewayCapacityConfig, GatewayConfig, GatewayLiveConfig,
    GatewayPresenceConfig, GatewayTranslationConfig, McpConfigCollection,
    McpManagedProxyServerConfig, McpOAuthConfig, McpRemoteServerConfig, McpSdkServerConfig,
    McpServerConfig, McpStdioServerConfig, McpTransport, McpWebSocketServerConfig, MemoryConfig,
    PlatformConfig as GatewayPlatformConfig, PostgresTopologyConfig, ResolvedPermissionMode,
    RoutingMode, RuntimeConfig, RuntimeControlConfig, RuntimeFeatureConfig, RuntimeHookConfig,
    RuntimePermissionRuleConfig, RuntimePluginConfig, ScopedMcpServerConfig, SessionRecoveryConfig,
    SessionResetPolicy, SessionStorageExecutionConfig, StorageBackendSelection,
    StorageTopologyConfig, COWD_SETTINGS_SCHEMA_NAME,
};
pub use config_validate::{
    check_unsupported_format, format_diagnostics, validate_config_file, ConfigDiagnostic,
    DiagnosticKind, ValidationResult,
};
pub use conflict_arbiter::{
    ConflictArbiter, ConflictDecisionKind, ConflictResolutionReceipt, ConflictResolutionRequest,
    ConflictSeverity, ConflictSourceKind,
};
pub use context_evidence::{
    audit_projection as project_evidence_audit, AuditProjection, ModelReceipt,
};
pub use context_fanout::{plan_context_fanout, ContextFanoutPlan, FanoutToolCall};
pub use context_tool_exposure::{ToolExposurePlanner, ToolExposurePolicy, ToolExposureState};
pub use conversation::{
    build_cc_memory_config, image_user_message_from_path, memory_project_id_for_workspace,
    ApiClient, ApiClientStream, ApiRequest, AssistantEvent, AssistantItemKind, AutoCompactionEvent,
    CancellationToken, ConversationRuntime, MemoryCallback, ProviderContextInventory, RuntimeError,
    SessionReadHead, StaticToolExecutor, ToolCallback, ToolError, ToolExecutor, TurnSummary,
};
pub use cowd_event::{
    AgentLifecyclePhase, CausalItemIdentity, CausalItemKind, CowdEvent, CowdEventBus,
    CowdExecutionContext, CowdExecutionLineage, CowdExecutionScope, RunModelTelemetry,
    RuntimeExecutionGraphSummary, RuntimePolicyDecisionSummary,
};
pub use cross_plane_policy::{
    ConnectorActionContext, ConnectorDecisionEvidence, CrossPlaneAction, CrossPlaneAuditRecord,
    CrossPlaneControlPlane, CrossPlaneDecisionEvidence, CrossPlaneDecisionKind,
    CrossPlaneDispatchOutcome, CrossPlaneDispatchTarget, CrossPlaneExecutionReceipt,
    CrossPlaneGrant, CrossPlaneIdentityBinding, CrossPlaneOutboundMessagePlan,
    CrossPlanePolicyConfig, CrossPlanePolicyDecision, CrossPlanePolicyEngine,
    CrossPlaneResolvedIdentity, CrossPlaneSummary, GrantType, IdentityTrust,
};
pub use definition_registry::{
    DefinitionRegistryError, RuntimeDefinitionRegistry, RuntimeTeamTemplateCatalogEntry,
};
pub use evidence_planner::{
    evidence_plan_prompt, plan_evidence, EvidenceAcquisitionMode, EvidencePlan,
};
pub use execution_core::{
    action_selection_report_for_decision, build_runtime_action_selection_report,
    build_runtime_execution_decision, execution_pattern_catalog_response, rewoo_plan_for_intent,
    runtime_execution_guidance_prompt, runtime_execution_guidance_prompt_with_tool_exposure,
    runtime_orchestration_action_guidance, runtime_orchestration_actions, tool_intents_from_rewoo,
    CalibrationOutcomeImportReceipt, CrossPlaneRuntimeError, CrossPlaneRuntimeService,
    DeliberationMode, DeliberationPlan, ExecutionCommitService, ExecutionCompileRequest,
    ExecutionGraphCompiler, ExecutionGraphHost, ExecutionGraphHostReceipt,
    ExecutionGraphStateStore, ExecutionPatternCatalog, ExecutionStartupRecoveryError,
    ExecutionStartupRecoveryRecord, ExecutionStartupRecoveryReport, LegacyOutcomeImportReceipt,
    OutcomeRecordReceipt, ReflexionRecord, ReflexionTrigger, RewooEvidencePlan,
    RewooEvidenceResult, RewooEvidenceStep, RewooObservation, RewooSolverContract,
    RuntimeActionSelectionReport, RuntimeCompileTarget, RuntimeEventReader, RuntimeEvidenceSummary,
    RuntimeExecutionActionHint, RuntimeExecutionDecision, RuntimeExecutionHealth,
    RuntimeExecutionOwnerReport, RuntimeExecutionPatternCandidate, RuntimeExecutionPatternSpec,
    RuntimeExecutionReportSpec, RuntimeExecutionShutdownReport, RuntimeExecutionSupervisor,
    RuntimeServices, RuntimeServicesBuilder, RuntimeServicesError, RuntimeWorkAdmissionReceipt,
    SessionTerminalDeliveryPort, StrategyDecisionEngine, StrategyLease, StrategyResourceHealth,
    ToolIntentDependency, ToolIntentDependencyKind, ToolIntentGraph, ToolIntentNode,
    TurnStrategyActualOutcome, TurnStrategyDecisionState, TurnStrategyDecisionStatus,
};
pub use git_context::{GitCommitEntry, GitContext};
pub use harness_contract::agent::AgentLifecycleEvent;
pub use hooks::{
    format_hook_output, HookAbortSignal, HookEvent, HookProgressEvent, HookProgressReporter,
    HookRunResult, HookRunner, HOOK_PREVIEW_CHAR_LIMIT,
};
pub use team_agent_task::{
    AgentTask, AgentTaskCompletionReceipt, AgentTaskOutcome, AgentTaskQualityStatus,
    AgentTaskStatus,
};
#[path = "conversation/host.rs"]
pub mod host;
pub use host::{
    submit_owned_conversation_turn, StandardRuntimeHost, StandardRuntimeHostConfig, TurnIngressRef,
};
pub use input_classifier::{classify_session_input, RuntimeInputState};
pub use intent_planner::{classify_intent, IntentPlan, TaskIntent};
pub use managed_agent::{
    FencedEffectOutboxRecord, FencedEffectStatus, ManagedAgentDispatchReport,
    ManagedAgentDispatcher, ManagedAgentEffectPermit, ManagedAgentHealth, ManagedAgentHealthStatus,
    ManagedAgentInvocation, ManagedAgentInvocationStatus, ManagedAgentInvocationTrigger,
    ManagedAgentRuntimeDispatchReport,
};
pub use runtime_harness::{RuntimeAiKernel, RuntimeAiKernelTrace};
pub use session_execution::{
    SessionExecutionFence, SessionExecutionFencePhase, SessionExecutionFenceSnapshot,
};
pub use session_runtime_port::{
    RuntimeContextEnvelopeRecord, RuntimeSessionEvent, RuntimeSessionEventKind,
    RuntimeSessionEventReceipt, RuntimeSessionEventRef, RuntimeSessionIngressCommand,
    RuntimeSessionInputAdmission, RuntimeSessionInputRecord, RuntimeSessionInputStatus,
    RuntimeSessionRecord, SessionRuntimeIngressPort, SessionRuntimeJournalPort,
    SessionRuntimeQueryPort,
};

pub use artifact::{
    ArtifactError, ArtifactGcPort, ArtifactGcReport, ArtifactMetadataPort,
    ArtifactMetadataRepository, ArtifactObjectRecord, ArtifactObjectTier, ArtifactReadPort,
    ArtifactRecord, ArtifactStore, ArtifactStoreConfig, ArtifactStoreStats, ArtifactWriteSink,
    SqliteArtifactRepository, ARTIFACT_PERMANENT_PIN_UNTIL_MS, ARTIFACT_STAGING_PIN_TTL_MS,
};
pub(crate) use evolution::EvolutionCandidateRegistration;
pub use evolution::{
    candidate_kind_from_proposal, candidate_kinds_from_root_cause, CanaryObservationReport,
    CanaryRolloutPolicy, EvaluationDirection, EvaluationPolicyChangeIntent,
    EvaluationPolicyChangeReview, EvolutionCandidateIntent, EvolutionCandidateKind,
    EvolutionCandidateLifecycle, EvolutionCandidateSubject, EvolutionCapabilityGoal,
    EvolutionComparisonDimension, EvolutionComparisonReportV2, EvolutionDiagnosis,
    EvolutionDiagnosisEngine, EvolutionEvalRunner, EvolutionEvaluationReadiness,
    EvolutionGovernanceCandidate, EvolutionGovernanceError, EvolutionGovernanceService,
    EvolutionHypothesis, EvolutionLifecycleDraft, EvolutionLifecycleService, EvolutionMission,
    EvolutionMissionStatus, EvolutionPlanDraft, EvolutionProjectorHealth, EvolutionProposal,
    EvolutionProposalKind, EvolutionProposalRisk, EvolutionReleaseAssignment,
    EvolutionRootCauseKind, EvolutionSignal, EvolutionSignalInput, EvolutionSignalSeverity,
    EvolutionSignalSource, EvolutionSignalType, EvolutionSkillDraft, EvolutionTriageCluster,
    EvolutionTriageService, ReleaseChangeAction, ReleaseChangeRequest, ReleaseChangeReview,
    ReleaseChangeReviewClass, ReleaseChangeReviewDecision, ReleaseChangeReviewStatus,
};
#[cfg(feature = "test-fixtures")]
pub use execution_core::RuntimeFixtureEventPort;
pub use governed_tool_executor::{
    GovernedToolAdmission, GovernedToolExecutionContext, GovernedToolExecutionReport,
    GovernedToolExecutor, GovernedToolFuture, GovernedToolTaskOutcome, GovernedToolTaskTerminal,
};
pub use governed_tool_plan::{
    GovernedToolCompilation, GovernedToolCompileError, GovernedToolCompileRejection,
    GovernedToolCompiler, GovernedToolExecutionMode, GovernedToolPlan, GovernedToolPlanTask,
    ValidatedGovernedToolDag,
};
pub use harness_contract::mission::{
    MissionCommand, MissionCommandAction, MissionCommandReceipt, MissionCommandSagaPhase,
    MissionCommandSagaRecord, MissionCommandTarget, MissionControlActionReadiness,
    MissionControlAgentNode, MissionControlApprovalNode, MissionControlEventDigest,
    MissionControlEventLine, MissionControlMissionSummary, MissionControlProjection,
    MissionControlReadiness, MissionControlSessionNode, MissionControlSummary,
    MissionControlTeamNode, MissionMaterializedSnapshot, MissionProjectionDelta,
    MissionWorkspaceProjection,
};
pub use harness_contract::turn::{
    InputRelationKind, InputRelationProposal, SessionDispatchAction, SessionDispatchCommand,
    SessionDispatchReceipt, SessionHandoff, SessionResultPacket,
};
pub use knowledge_candidate_projector::KnowledgeCandidateProjector;
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
pub use mission_command_interpreter::{
    MissionCommandExecutionReceipt, MissionCommandInterpretRequest, MissionCommandInterpretation,
    MissionCommandInterpreter, MissionCommandTargetKind, MissionInterpretedCommand,
};
pub use mission_command_router::{
    commit_mission_effect, commit_mission_receipt, execute_mission_command,
    execute_reserved_runtime_effect, finalize_mission_command, mission_command_saga,
    reject_mission_command, reserve_mission_command,
};
pub use mission_control::MissionControlRuntime;
pub use mission_evidence::{MissionEvidenceBus, MissionEvidenceRef};
pub use mission_runtime::{MissionProjection, MissionRuntime};
pub use mission_runtime_port::{MissionRuntimePort, TaskRuntimePort};
pub use mission_schedule::{
    CreateMissionScheduleRequest, MissionScheduleDispatchReport, MissionScheduleStore,
    MissionScheduleTickReport, UpdateMissionScheduleRequest,
};
pub use module_map::{
    runtime_module_map, runtime_module_names_by_domain, RuntimeDomain, RuntimeModuleDescriptor,
};
pub use orchestration::{
    handle_runtime_orchestration_request, handle_runtime_orchestration_request_with_decision,
    runtime_orchestration_response, runtime_orchestration_response_with_decision,
    submit_runtime_orchestration_request, CapabilityRecipeId, CompiledOrchestration,
    GraphMutationProposal, GraphSemanticNode, RuntimeControlKind, RuntimeControlRequest,
    RuntimeControlScope, RuntimeOrchestrationBinding, RuntimeOrchestrationCommand,
    RuntimeOrchestrationConstraints, RuntimeOrchestrationDecision, RuntimeOrchestrationOperation,
    RuntimeOrchestrationResult, RuntimeStateSnapshot, SemanticFocus,
};
pub use outcome_projector::{
    OutcomeProjectionCheckpoint, OutcomeProjectionDlqEntry, OutcomeProjectionHealth,
    OutcomeProjector, OutcomeReadSnapshot, OutcomeSegmentSnapshot,
};
pub use permissions::{
    PermissionContext, PermissionMode, PermissionOutcome, PermissionOverride, PermissionPolicy,
    PermissionPromptDecision, PermissionPrompter, PermissionRequest, SharedPrompter,
};
pub use policy_engine::{
    evaluate, DiffScope, LaneBlocker, LaneContext, PolicyAction, PolicyCondition, PolicyEngine,
    PolicyRule, ReconcileReason, ReviewStatus,
};
pub use profile::{Profile, ProfileManager, ProfileMeta};
pub use prompt::{
    load_system_prompt, prepend_bullets, runtime_clock_section, ContextFile, CowdIdentityContract,
    ProjectContext, PromptBuildError, SystemPromptBuilder, COWD_IDENTITY_CONTRACT_VERSION,
    SYSTEM_PROMPT_DYNAMIC_BOUNDARY,
};
pub use prompt_assembly::{PromptAssembly, PromptContextPacket};
pub use provider::{detect_provider_kind, model_context_window_with_overrides, ProviderKind};
pub use provider_outcome_selector::{
    select_provider_from_outcome_snapshot, ProviderSelectionCandidateReceipt,
    ProviderSelectionReceipt,
};
pub use provider_registry::{
    ProviderRegistry, ProviderRegistryDiagnostics, ProviderRegistryRejected,
    ProviderRegistrySnapshot, ProviderRegistryUpdate,
};
pub use provider_resources::{
    ProviderAccountPolicy, ProviderModelPolicy, ProviderQuotaPolicy, ProviderResourceConfig,
    ProviderResourceGeneration,
};
pub use provider_runtime_client::{
    push_provider_output_block, ProviderClientTemplateCache, ProviderClientTemplateCacheStats,
    ProviderControlCompletion, ProviderOutputContentBlock, ProviderRequestEvidenceContext,
    ProviderRuntimeClient, ProviderToolDefinition, ProviderWireEvidence,
    ProviderWireEvidenceWriter,
};
pub use provider_runtime_client::{ProviderRequestContext, ResolvedProviderProfile};
pub use provider_transport_policy::ProviderTransportPolicy;
pub use provider_transport_pool::{
    ProviderTransportPool, ProviderTransportPoolStats, TransportProfileFingerprint,
};
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
    attempt_recovery, recipe_for, EscalationPolicy, FailureScenario, RecoveryContext,
    RecoveryEvent, RecoveryRecipe, RecoveryResult, RecoveryStep,
};
pub use remote::{
    inherited_upstream_proxy_env, no_proxy_list, read_token, upstream_proxy_ws_url,
    RemoteSessionContext, UpstreamProxyBootstrap, UpstreamProxyState, DEFAULT_REMOTE_BASE_URL,
    DEFAULT_SESSION_TOKEN_PATH, DEFAULT_SYSTEM_CA_BUNDLE, NO_PROXY_HOSTS, UPSTREAM_PROXY_ENV_KEYS,
};
pub use request_compiler::{PreparedRequestBasis, PreparedRequestCompiler, RequestCompilerStats};
pub use resources::{
    register_resource_from_path, render_resource_context_markdown, resource_hint,
    ResourceCapabilityIndex, ResourceCapabilitySnapshot, ResourceEvidence, ResourceHint,
    ResourceKind, ResourceMigrationOptions, ResourceMigrationReport, ResourceProjection,
    ResourcePromptHint, ResourceStore,
};
pub use runtime_event_replay::{
    candidate_from_action, RuntimeEventReplayer, RuntimeRecoveryAction, RuntimeRecoveryActionKind,
    RuntimeRecoveryCandidate, RuntimeReplayReport,
};
pub use runtime_event_store::{
    decode_session_terminal_artifact_ref, encode_session_terminal_artifact_ref,
    AppendTransactionReceipt, AppendTransactionRequest, CommittedEventBatch,
    CommittedStreamRevision, DurableRuntimeEvent, ExpectedStreamRevision,
    RuntimeDecisionLeaseSnapshot, RuntimeEventCommitSnapshot, RuntimeEventInput,
    RuntimeEventRecord, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeEventStoreBackend, RuntimeEventStoreError, RuntimeEventStoreResult,
    RuntimeEventStoreSnapshot, RuntimeEventStreamHeadSnapshot,
    RuntimeEventTransactionStreamSnapshot, RuntimeProjectionCheckpoint,
    RuntimeSessionOutboxFailureClass, RuntimeSessionOutboxHealth, RuntimeSessionOutboxRecord,
    RuntimeSessionTerminalFenceAdoption, RuntimeTransactionEventInput, SessionTerminalInput,
};
pub use runtime_memory_summarizer::RuntimeMemorySummarizer;
pub use sandbox::{
    detect_container_environment, detect_container_environment_from, resolve_sandbox_status,
    resolve_sandbox_status_for_request, ContainerEnvironment, FilesystemIsolationMode,
    SandboxConfig, SandboxDetectionInputs, SandboxRequest, SandboxStatus,
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
    session_ingress_graph_id, SessionDispatchMode, SessionExecutionPolicy,
    SessionHandoffResolution, SessionIngressExecutionReceipt, SessionIngressExecutor,
    SessionInputRouteReceipt, SessionInputRouteReport, SessionInputRouter, SessionInputRouterError,
    SessionRecoveryCandidate, SESSION_DISPATCH_EXECUTOR,
};
pub use session_history::{
    HistoryCursor, HistoryView, HistoryWeight, SessionHistory, SessionHistoryConfig,
};
pub use session_input::{SessionInputRecord, SessionInputStream};
pub use session_relation_graph::{
    SessionProxy, SessionRelation, SessionRelationGraph, SessionRelationKind, SessionRouteCommand,
    SessionRouteReceipt,
};
pub use skill::{
    memory_candidate_from_skill_activation, skill_memory_candidate_session_event,
    RuntimeSkillCandidate, RuntimeSkillCatalog, RuntimeSkillInstructionSource,
    RuntimeSkillPromptAsset, SkillActivationRecord, SkillInvocation, SkillMemoryPolicy,
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
pub use task::{
    TaskAggregate, TaskAggregateService, TaskCommandOutcome, TaskEvidenceOutboxRecord,
    TaskExecutionPolicy, TaskGraphRef, TaskMutation, TaskMutationResult, TaskPhase,
    TaskPhaseArtifact, TaskPhaseStatus, TaskPhaseTerminalReceipt, TaskSpec,
    TaskStatus as MissionTaskStatus, TaskStoreBackend, TaskStoreSnapshot,
};
pub use team_agent_selector::AgentSelector;
pub use team_instantiation::{ResolvedRoleSlot, TeamInstantiation, TeamInstantiationService};
pub use team_l4_promotion::{
    KnowledgeCandidateProjection, L4CandidateLifecycle, L4PromotionCandidate, L4PromotionReceipt,
    L4PromotionService,
};
pub use team_legacy_import::LegacyTeamImportReport;
pub use team_profile_migration::LegacyTeamProfileMigrationReport;
pub use team_projection::{TeamProjection, TeamProjectionReader};
pub use team_result_reducer::TeamResultReducer;
pub use team_runtime::TeamRuntime;
pub use team_working_state::{
    FocusOverlapAssessment, TeamWorkingState, TeamWorkingStateEntry, TeamWorkingStateKind,
    TeamWorkingStatePublishRequest, TeamWorkingStateReadRequest, TeamWorkingStateVisibility,
};
pub use tool_execution_plane::{
    ToolExecutionAdmission, ToolExecutionPlane, ToolExecutionPlaneError, ToolExecutionPlaneStats,
};
pub use tool_host::{
    RuntimeExecutionHost, RuntimeToolExecutionOutcome, RuntimeToolExecutionRequest,
    RuntimeToolExecutionStatus,
};
pub use tool_invocation::{
    now_ms as tool_invocation_now_ms, ToolFailureKind, ToolInvocationRecord, ToolInvocationStatus,
    ToolOutputRef, DEFAULT_OUTPUT_REF_MIN_LINES,
};
pub use tool_memory::{memory_candidate_from_tool_invocation, ToolMemoryCandidatePolicy};
pub use tool_orchestrator::{
    classify_tool_request, tool_execution_profile, ToolCachePolicy, ToolExecutionProfile,
    ToolSafetyCategory,
};
pub use tool_policy::{ToolExecutionPolicyDecision, ToolPolicy, ToolPolicyError};
pub use trust_resolver::{TrustConfig, TrustDecision, TrustEvent, TrustPolicy, TrustResolver};
pub use upgrade::{
    ClosureUpgradeInventoryCollector, LegacyExecutionImportError, LegacyExecutionImportReceipt,
    LegacyExecutionImporter, UpgradeCarrierRecord, UpgradeCarrierStatus,
    UpgradeCleanShutdownReceipt, UpgradeCoordinator, UpgradeDispositionReceipt, UpgradeError,
    UpgradeInventory, UpgradeInventoryCollector, UpgradeMaintenanceSnapshot,
    LEGACY_EXECUTION_IMPORTED, UPGRADE_RECOVERY_REQUIRED,
};
pub use usage::{pricing_for_model, UsageTracker};
pub use wave::{
    DependencyGraph, ErrorPolicy, TaskContext, TaskId, TaskResult, TaskStatus, Wave, WaveConfig,
    WaveError, WaveExecutor, WaveOrchestrator, WaveResult, WaveStatus, WaveTask,
};

pub use adaptive_context::{
    ContextAllocation, ContextAllocationReport, ContextAllocator, ContextDemand, ContextResolution,
};
pub use budget_policy::{
    clamp_context_budget_ratio_bp, resolve_context_budget_tokens, MemoryBudgetLease,
    ProviderOutputBudget, ProviderOutputBudgetInputs, RuntimeBudgetInputs, RuntimeBudgetPlan,
    RuntimeControlBudgetLease, ToolOutputBudgetLease, DEFAULT_SUBAGENT_BUDGET_TOKENS,
    DEFAULT_SUBSYSTEM_BUDGET_RATIO_BP,
};
pub use context_runtime::{
    context_authority_for_reality_boundary, AgentContextLease, AgentContextView,
    AgentReturnContextProjection, AgentReturnRequirement, AssembledContext, ContextAuthority,
    ContextBudgetAllocation, ContextBudgetExplanation, ContextBudgetReport,
    ContextCacheStabilityReport, ContextDegradationPath, ContextDiagnostics, ContextEnvelope,
    ContextEnvelopeRequest, ContextEpochReport, ContextIdentity, ContextItem, ContextLeanProbe,
    ContextLease, ContextMode, ContextModeCoverageEntry, ContextModeCoverageReport,
    ContextOmission, ContextPolicyAction, ContextPolicyDecision, ContextPolicyProposal,
    ContextPressureLevel, ContextProfile, ContextRenderManifest, ContextRole, ContextRuntimeKernel,
    ContextSegmentChange, ContextSegmentKind, ContextSegmentSnapshot, ContextSnapshot,
    ContextSnapshotDiff, ContextSourceKind, ContextSourceLifecycle, ContextSourceRef,
    ContextVisibility, PersistedContextEnvelope, ResumeContextPacket, ResumeContextSource,
    StableHeadComparison, ToolTracePacket, ToolTraceStatus, WorkspacePacket,
    CONTEXT_RENDER_FORMATTER_VERSION, PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION,
};
pub use runtime_control::{
    AgentControlPolicy, ContextControlPolicy, MemoryControlPolicy, MissionSchedulePolicy,
    ObservabilityPolicy, RuntimeControlPolicy, TaskControlPolicy,
};
#[cfg(test)]
pub(crate) fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
