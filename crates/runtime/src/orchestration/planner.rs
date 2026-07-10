use harness_contract::strategy::{decide_strategy, StrategyInput};
use serde::{Deserialize, Serialize};

use crate::execution_core::deliberation::DeliberationPlan;
use crate::execution_core::pattern_catalog::ExecutionPatternCatalog;
use crate::execution_core::rewoo_plan::{rewoo_plan_for_intent, RewooEvidencePlan};
use crate::execution_core::strategy_decision::{
    build_runtime_execution_decision, RuntimeExecutionDecision,
};
use crate::execution_core::tool_dag::{tool_dag_from_rewoo, ToolDagPlan};
use crate::orchestration::request::RuntimeOrchestrationRequest;
use crate::{CollaborationDecision, CollaborationTemplateMatcher};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeOrchestrationPlan {
    pub execution_decision: RuntimeExecutionDecision,
    pub pattern_catalog: ExecutionPatternCatalog,
    pub rewoo_plan: RewooEvidencePlan,
    pub tool_dag: ToolDagPlan,
    pub deliberation_plan: DeliberationPlan,
}

#[must_use]
pub fn plan_runtime_collaboration_decision(intent: &str) -> CollaborationDecision {
    let strategy = decide_strategy(&StrategyInput::from_prompt(intent.to_string()));
    CollaborationTemplateMatcher::default().decide(intent, &strategy)
}

#[must_use]
pub fn plan_runtime_orchestration(
    request: &RuntimeOrchestrationRequest,
) -> RuntimeOrchestrationPlan {
    let execution_decision = build_runtime_execution_decision(&request.intent, None);
    let rewoo_plan = rewoo_plan_for_intent(&request.intent);
    let tool_dag = tool_dag_from_rewoo(&rewoo_plan);
    RuntimeOrchestrationPlan {
        execution_decision,
        pattern_catalog: ExecutionPatternCatalog::current(),
        deliberation_plan: DeliberationPlan::for_objective(&request.intent),
        rewoo_plan,
        tool_dag,
    }
}
