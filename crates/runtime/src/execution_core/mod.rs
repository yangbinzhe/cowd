//! Runtime execution core.
//!
//! This module turns the existing harness contracts, evidence planner, tool
//! scheduler, collaboration templates, and turn supervisor into model-visible
//! execution capabilities. Gateway can expose these capabilities, but runtime
//! remains the owner of mode selection and orchestration semantics.

pub mod budget;
pub mod cross_plane;
pub mod deliberation;
pub mod evidence;
pub mod evidence_writer;
pub mod goal;
pub mod graph;
pub mod hot_state;
pub mod model_affordance;
pub mod model_work;
pub mod orchestration_binding;
pub mod outcome_service;
pub mod pattern_catalog;
pub mod performance;
pub mod protocols;
pub mod reflexion;
pub mod report;
pub mod rewoo_plan;
pub mod safety_fuse;
pub mod services;
pub mod strategy_decision;
mod supervisor;
pub mod tool_intents;

pub use budget::{
    DurableProviderBudgetReservation, ParentExecutionBudgetLedger, ParentExecutionBudgetSnapshot,
};
pub use cross_plane::{CrossPlaneRuntimeError, CrossPlaneRuntimeService};
pub use deliberation::{DeliberationMode, DeliberationPlan};
pub use evidence::RuntimeEvidenceSummary;
pub use evidence_writer::ResourceEvidenceWriterHealth;
pub use goal::{policy::InterventionPolicy, GoalProgressReducer, GoalProjection, GoalStore};
pub use graph::{
    ExecutionCommitService, ExecutionCompileError, ExecutionCompileRequest, ExecutionGraphCompiler,
    ExecutionGraphHost, ExecutionGraphHostReceipt, ExecutionGraphReplan, ExecutionGraphStateStore,
    ExecutionRunnerError, ExecutionStateStoreError, NodeExecutionContext, NodeExecutionOutcome,
    NodeExecutionTicket, NodeExecutor, NodeExecutorError, NodeExecutorRegistry, ScopedNodeBackend,
    ScopedNodeExecutor,
};
pub use hot_state::{
    DerivedMaterialization, DerivedMaterializerHealth, HotMemoryBudget, HotSessionSnapshot,
    HotStateConfig, HotStateHealth, HotStateMemoryConfig, HotStateMetricsSnapshot,
    LiveCheckpointConfig, RuntimeHotStatePlane,
};
pub use model_affordance::{
    runtime_execution_guidance_prompt, runtime_execution_guidance_prompt_with_tool_exposure,
};
pub use model_work::{
    ImmutableWorkKey, InFlightCoalescer, ModelWorkCompileError, ModelWorkEstimate,
    ModelWorkEstimateInput, ModelWorkGraphCompiler, ModelWorkGraphEstimator, ModelWorkNode,
    ModelWorkPlan, ModelWorkReducer, ModelWorkReductionInput, ModelWorkTopology, ReducedModelWork,
};
pub use orchestration_binding::{
    runtime_orchestration_action_guidance, runtime_orchestration_actions,
};
pub use outcome_service::{
    CalibrationOutcomeImportReceipt, LegacyOutcomeImportReceipt, OutcomeRecordReceipt,
    OutcomeService, OUTCOME_EVENT_KIND, OUTCOME_IMPORT_EVENT_KIND,
};
pub use pattern_catalog::{
    execution_pattern_catalog_response, ExecutionPatternCatalog, RuntimeCompileTarget,
    RuntimeExecutionPatternSpec,
};
pub use protocols::{
    compile_debate, compile_incident, compile_jps, compile_review_fix, validate_protocol_graph,
    validate_protocol_registry, validate_protocol_request, validate_protocol_spec,
    DebateProtocolCompiler, IncidentProtocolCompiler, JpsProtocolCompiler, OutputSpec,
    ProtocolAvailability, ProtocolCompileError, ProtocolCompileRequest, ProtocolExecutorKind,
    ProtocolId, ProtocolRef, ProtocolRegistry, ProtocolResultReducer, ProtocolSpec,
    ProtocolValidationError, RepairPolicy, RepairTrigger, ReviewFixProtocolCompiler,
    RoleDependencyKind, RoleDependencySpec, RoleSpec, StopPolicy,
};
pub use reflexion::{ReflexionRecord, ReflexionTrigger};
pub use report::RuntimeExecutionReportSpec;
pub use rewoo_plan::{
    rewoo_plan_for_intent, rewoo_plan_for_intent_with_evidence_plan, RewooEvidencePlan,
    RewooEvidenceResult, RewooEvidenceStep, RewooObservation, RewooSolverContract,
};
pub use safety_fuse::{ExecutionBudgetLease, SafetyFuseDecision, SafetyFusePolicy};
#[cfg(feature = "test-fixtures")]
pub use services::RuntimeFixtureEventPort;
pub use services::{
    ExecutionStartupRecoveryError, ExecutionStartupRecoveryRecord, ExecutionStartupRecoveryReport,
    RuntimeEventReader, RuntimeServices, RuntimeServicesBuilder, RuntimeServicesError,
    SessionTerminalDeliveryPort,
};
pub use strategy_decision::{
    action_selection_report_for_decision, build_runtime_action_selection_report,
    build_runtime_execution_decision, RuntimeActionSelectionReport, RuntimeExecutionActionHint,
    RuntimeExecutionDecision, RuntimeExecutionPatternCandidate, StrategyDecisionEngine,
    StrategyLease, StrategyResourceHealth, TurnStrategyActualOutcome, TurnStrategyDecisionState,
    TurnStrategyDecisionStatus,
};
pub use supervisor::{
    RuntimeExecutionHealth, RuntimeExecutionOwnerReport, RuntimeExecutionShutdownReport,
    RuntimeExecutionSupervisor, RuntimeWorkAdmissionReceipt,
};
pub use tool_intents::{
    tool_intents_from_rewoo, ToolIntentDependency, ToolIntentDependencyKind, ToolIntentGraph,
    ToolIntentNode,
};
