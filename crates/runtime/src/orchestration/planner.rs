use harness_contract::core::{ExecutionModifier, ExecutionPattern, TaskComplexity, TaskRisk};
use harness_contract::strategy::{
    understand, StrategyInput, StrategyProposal, TaskDuration, TaskUnderstanding,
};
use serde::{Deserialize, Serialize};

use crate::execution_core::deliberation::DeliberationPlan;
use crate::execution_core::pattern_catalog::ExecutionPatternCatalog;
use crate::execution_core::rewoo_plan::{
    rewoo_plan_for_intent_with_evidence_plan, RewooEvidencePlan,
};
use crate::execution_core::strategy_decision::{
    RuntimeExecutionDecision, StrategyDecisionEngine, StrategyResourceHealth,
};
use crate::execution_core::tool_intents::{tool_intents_from_rewoo, ToolIntentGraph};
use crate::orchestration::request::RuntimeOrchestrationRequest;
use crate::{CollaborationDecision, CollaborationTemplateMatcher};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeOrchestrationPlan {
    pub execution_decision: RuntimeExecutionDecision,
    pub model_proposal: Option<StrategyProposal>,
    pub collaboration_decision: CollaborationDecision,
    pub pattern_catalog: ExecutionPatternCatalog,
    pub rewoo_plan: RewooEvidencePlan,
    pub tool_intents: ToolIntentGraph,
    pub deliberation_plan: DeliberationPlan,
}

#[must_use]
fn collaboration_decision_for_execution(
    intent: &str,
    decision: &RuntimeExecutionDecision,
) -> CollaborationDecision {
    CollaborationTemplateMatcher.decide(intent, &decision.strategy)
}

#[must_use]
pub fn plan_runtime_orchestration(
    request: &RuntimeOrchestrationRequest,
) -> RuntimeOrchestrationPlan {
    plan_runtime_orchestration_with_decision(request, None)
}

#[must_use]
pub fn plan_runtime_orchestration_with_decision(
    request: &RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> RuntimeOrchestrationPlan {
    plan_runtime_orchestration_with_decision_and_resources(
        request,
        leased_decision,
        StrategyResourceHealth::default(),
    )
}

#[must_use]
pub(crate) fn plan_runtime_orchestration_with_decision_and_resources(
    request: &RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    resource_health: StrategyResourceHealth,
) -> RuntimeOrchestrationPlan {
    let model_proposal = strategy_proposal_from_request(request);
    let mut strategy_input = StrategyInput::from_prompt(request.intent.clone())
        .with_explicit_write(request.constraints.requires_write.unwrap_or(false));
    if let Some(risk) = request
        .constraints
        .risk
        .as_deref()
        .and_then(parse_task_risk)
    {
        strategy_input = strategy_input.with_risk_override(risk);
    }
    if let Some(proposal) = model_proposal.clone() {
        strategy_input = strategy_input.with_proposal(proposal);
    }
    let understanding =
        understanding_with_action_signal(understand(&strategy_input), request.action);
    strategy_input = strategy_input.with_understanding(understanding);
    let execution_decision = leased_decision.cloned().unwrap_or_else(|| {
        StrategyDecisionEngine.decide_with_input(strategy_input, None, resource_health)
    });
    let collaboration_decision =
        collaboration_decision_for_execution(&request.intent, &execution_decision);
    let evidence_plan = crate::evidence_planner::plan_evidence_with_understanding(
        &request.intent,
        &execution_decision.strategy.understanding,
    );
    let rewoo_plan = rewoo_plan_for_intent_with_evidence_plan(&request.intent, evidence_plan);
    let tool_intents = tool_intents_from_rewoo(&rewoo_plan);
    RuntimeOrchestrationPlan {
        execution_decision,
        model_proposal,
        collaboration_decision,
        pattern_catalog: ExecutionPatternCatalog::current(),
        deliberation_plan: DeliberationPlan::for_objective(&request.intent),
        rewoo_plan,
        tool_intents,
    }
}

fn understanding_with_action_signal(
    mut understanding: TaskUnderstanding,
    action: crate::orchestration::request::RuntimeOrchestrationAction,
) -> TaskUnderstanding {
    use crate::orchestration::request::RuntimeOrchestrationAction as Action;

    match action {
        Action::RequestParallelTools | Action::RequestRewooEvidence => {
            understanding.requests_parallelism = true;
            promote_complexity(&mut understanding, TaskComplexity::Moderate);
        }
        Action::RequestTeam => {
            if !understanding.forbids_team {
                understanding.requests_multi_agent = true;
                understanding.requests_parallelism = true;
                understanding.independent_workstreams =
                    understanding.independent_workstreams.max(2);
                promote_complexity(&mut understanding, TaskComplexity::Complex);
            }
        }
        Action::RequestDeliberation => {
            understanding.requests_deliberation = true;
            understanding.uncertainty = understanding.uncertainty.max(6);
            promote_complexity(&mut understanding, TaskComplexity::Complex);
        }
        Action::RequestBackgroundReview | Action::DispatchSession => {
            understanding.requests_background = true;
            understanding.estimated_duration = TaskDuration::LongRunning;
            promote_complexity(&mut understanding, TaskComplexity::Complex);
        }
        Action::RequestSubagent | Action::RequestVerification | Action::RequestReflexionRetry => {
            promote_complexity(&mut understanding, TaskComplexity::Moderate);
        }
        Action::PlanOnly | Action::RequestRiskGate => {}
    }
    understanding
}

fn promote_complexity(understanding: &mut TaskUnderstanding, minimum: TaskComplexity) {
    let rank = |complexity| match complexity {
        TaskComplexity::Trivial => 0,
        TaskComplexity::Simple => 1,
        TaskComplexity::Moderate => 2,
        TaskComplexity::Complex => 3,
        TaskComplexity::Strategic => 4,
    };
    if rank(understanding.complexity) < rank(minimum) {
        understanding.complexity = minimum;
    }
}

fn parse_task_risk(value: &str) -> Option<TaskRisk> {
    match value.trim().to_ascii_lowercase().as_str() {
        "low" => Some(TaskRisk::Low),
        "medium" => Some(TaskRisk::Medium),
        "high" => Some(TaskRisk::High),
        "critical" => Some(TaskRisk::Critical),
        _ => None,
    }
}

fn strategy_proposal_from_request(
    request: &RuntimeOrchestrationRequest,
) -> Option<StrategyProposal> {
    use crate::orchestration::request::RuntimeOrchestrationAction as Action;

    let pattern = match request.action {
        Action::PlanOnly => return None,
        Action::RequestParallelTools | Action::RequestRewooEvidence => ExecutionPattern::Explore,
        Action::RequestSubagent | Action::RequestVerification | Action::RequestReflexionRetry => {
            ExecutionPattern::Execute
        }
        Action::RequestDeliberation => ExecutionPattern::Deliberate,
        Action::RequestTeam => ExecutionPattern::Collaborate,
        Action::RequestBackgroundReview | Action::DispatchSession => ExecutionPattern::Supervise,
        Action::RequestRiskGate => ExecutionPattern::Execute,
    };
    let mut modifiers = Vec::new();
    if matches!(
        request.action,
        Action::RequestParallelTools | Action::RequestTeam | Action::RequestDeliberation
    ) {
        modifiers.push(ExecutionModifier::Parallel);
    }
    if request.action == Action::RequestBackgroundReview {
        modifiers.push(ExecutionModifier::Background);
    }
    Some(StrategyProposal {
        pattern,
        modifiers,
        template: request.template_hint.clone(),
        confidence: 85,
        rationale: request.reason.clone().unwrap_or_else(|| {
            format!(
                "model selected runtime action `{}`",
                request.action.as_str()
            )
        }),
    })
}
