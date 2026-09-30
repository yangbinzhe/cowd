//! Runtime execution core.
//!
//! This module turns the existing harness contracts, evidence planner, tool
//! scheduler, and turn supervisor into model-visible execution capabilities.
//! Gateway can expose these capabilities, while the model retains semantic
//! control and Runtime owns environment admission and execution safety.

pub mod budget;
pub mod cross_plane;
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
pub mod report;
pub mod safety_fuse;
pub mod services;
pub mod strategy_decision;
mod supervisor;

pub use budget::{
    DurableProviderBudgetReservation, ParentExecutionBudgetLedger, ParentExecutionBudgetSnapshot,
};
pub use cross_plane::{CrossPlaneRuntimeError, CrossPlaneRuntimeService};
pub use evidence::RuntimeEvidenceSummary;
pub use evidence_writer::ResourceEvidenceWriterHealth;
pub use goal::{
    policy::InterventionPolicy, supervisor::ObjectiveReconcileDecision,
    supervisor::ObjectiveSupervisor, GoalProgressReducer, GoalProjection, GoalStore,
};
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
    runtime_execution_guidance_prompt_with_tool_exposure_mode,
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
pub use report::RuntimeExecutionReportSpec;
pub use safety_fuse::{ComplexityBudget, ExecutionBudgetLease, SafetyFuseDecision, SafetyFusePolicy};
#[cfg(feature = "test-fixtures")]
pub use services::RuntimeFixtureEventPort;
pub use services::{
    ExecutionStartupRecoveryError, ExecutionStartupRecoveryRecord, ExecutionStartupRecoveryReport,
    RuntimeEventReader, RuntimeServices, RuntimeServicesBuilder, RuntimeServicesError,
    SessionTerminalDeliveryPort,
};
pub use strategy_decision::{
    build_runtime_execution_decision, RuntimeExecutionDecision, RuntimeExecutionPatternCandidate,
    StrategyDecisionEngine, StrategyLease, StrategyResourceHealth, TurnStrategyActualOutcome,
    TurnStrategyDecisionState, TurnStrategyDecisionStatus,
};
pub use supervisor::{
    RuntimeExecutionHealth, RuntimeExecutionOwnerReport, RuntimeExecutionShutdownReport,
    RuntimeExecutionSupervisor, RuntimeWorkAdmissionReceipt,
};
