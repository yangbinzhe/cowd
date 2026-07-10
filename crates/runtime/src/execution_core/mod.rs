//! Runtime execution core.
//!
//! This module turns the existing harness contracts, evidence planner, tool
//! scheduler, collaboration templates, and turn supervisor into model-visible
//! execution capabilities. Gateway can expose these capabilities, but runtime
//! remains the owner of mode selection and orchestration semantics.

pub mod deliberation;
pub mod evidence;
pub mod model_affordance;
pub mod orchestration_binding;
pub mod pattern_catalog;
pub mod reflexion;
pub mod report;
pub mod rewoo_plan;
pub mod strategy_decision;
pub mod tool_dag;

pub use deliberation::{DeliberationMode, DeliberationPlan};
pub use evidence::RuntimeEvidenceSummary;
pub use model_affordance::runtime_execution_guidance_prompt;
pub use orchestration_binding::{
    runtime_orchestration_action_guidance, runtime_orchestration_actions,
};
pub use pattern_catalog::{
    execution_pattern_catalog_response, ExecutionPatternCatalog, RuntimeCompileTarget,
    RuntimeExecutionPatternSpec,
};
pub use reflexion::{ReflexionRecord, ReflexionTrigger};
pub use report::RuntimeExecutionReportSpec;
pub use rewoo_plan::{
    rewoo_plan_for_intent, rewoo_plan_for_intent_with_evidence_plan, RewooEvidencePlan,
    RewooEvidenceResult, RewooEvidenceStep, RewooObservation, RewooSolverContract,
};
pub use strategy_decision::{
    action_selection_report_for_decision, build_runtime_action_selection_report,
    build_runtime_execution_decision, RuntimeActionSelectionReport, RuntimeExecutionActionHint,
    RuntimeExecutionDecision, RuntimeExecutionPatternCandidate, StrategyDecisionEngine,
    StrategyLease, StrategyResourceHealth,
};
pub use tool_dag::{
    tool_dag_from_rewoo, ToolDagEdge, ToolDagEdgeKind, ToolDagPlan, ToolDagSafetySummary,
    ToolDagTask,
};
