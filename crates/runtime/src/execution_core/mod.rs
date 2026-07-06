//! Runtime execution core.
//!
//! This module turns the existing harness contracts, evidence planner, tool
//! scheduler, collaboration templates, and turn supervisor into model-visible
//! execution capabilities. Gateway can expose these capabilities, but runtime
//! remains the owner of mode selection and orchestration semantics.

pub mod deliberation;
pub mod evidence;
pub mod mode_catalog;
pub mod model_affordance;
pub mod orchestration_binding;
pub mod reflexion;
pub mod report;
pub mod rewoo_plan;
pub mod strategy_matcher;
pub mod tool_dag;

pub use deliberation::{DeliberationMode, DeliberationPlan};
pub use evidence::RuntimeEvidenceSummary;
pub use mode_catalog::{
    execution_mode_catalog_response, ExecutionModeCatalog, RuntimeExecutionBinding,
    RuntimeExecutionModeSpec,
};
pub use model_affordance::runtime_execution_guidance_prompt;
pub use orchestration_binding::{
    runtime_orchestration_action_guidance, runtime_orchestration_actions,
};
pub use reflexion::{ReflexionRecord, ReflexionTrigger};
pub use report::RuntimeExecutionReportSpec;
pub use rewoo_plan::{
    rewoo_plan_for_intent, RewooEvidencePlan, RewooEvidenceResult, RewooEvidenceStep,
    RewooObservation, RewooSolverContract,
};
pub use strategy_matcher::{
    build_runtime_action_selection_report, build_runtime_execution_decision,
    RuntimeActionSelectionReport, RuntimeExecutionActionHint, RuntimeExecutionDecision,
    RuntimeExecutionModeCandidate,
};
pub use tool_dag::{
    tool_dag_from_rewoo, ToolDagEdge, ToolDagEdgeKind, ToolDagPlan, ToolDagSafetySummary,
    ToolDagTask,
};
