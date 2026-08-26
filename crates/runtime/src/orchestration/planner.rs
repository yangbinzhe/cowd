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
use crate::orchestration::request::{
    CapabilityRecipeId, RuntimeOrchestrationCommand, RuntimeOrchestrationOperation,
};
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
    request: &RuntimeOrchestrationCommand,
) -> RuntimeOrchestrationPlan {
    plan_runtime_orchestration_with_decision(request, None)
}

#[must_use]
pub fn plan_runtime_orchestration_with_decision(
    request: &RuntimeOrchestrationCommand,
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
    request: &RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
    resource_health: StrategyResourceHealth,
) -> RuntimeOrchestrationPlan {
    let understanding = leased_decision
        .map(|decision| decision.strategy.understanding.clone())
        .unwrap_or_else(|| understand_runtime_orchestration_request(request));
    plan_runtime_orchestration_with_understanding(
        request,
        leased_decision,
        resource_health,
        understanding,
    )
}

pub(crate) fn understand_runtime_orchestration_request(
    request: &RuntimeOrchestrationCommand,
) -> TaskUnderstanding {
    let (strategy_input, _) = strategy_input_for_request(request);
    understanding_with_proposal_signal(understand(&strategy_input), request)
}

pub(crate) fn plan_runtime_orchestration_with_understanding(
    request: &RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
    resource_health: StrategyResourceHealth,
    understanding: TaskUnderstanding,
) -> RuntimeOrchestrationPlan {
    let (mut strategy_input, model_proposal) = strategy_input_for_request(request);
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

fn strategy_input_for_request(
    request: &RuntimeOrchestrationCommand,
) -> (StrategyInput, Option<StrategyProposal>) {
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
    (strategy_input, model_proposal)
}

fn understanding_with_proposal_signal(
    mut understanding: TaskUnderstanding,
    request: &RuntimeOrchestrationCommand,
) -> TaskUnderstanding {
    let Some(proposal) = request.proposal.as_ref() else {
        return understanding;
    };
    let team_count = proposal
        .nodes
        .iter()
        .filter(|node| node.recipe == CapabilityRecipeId::Team)
        .map(|node| usize::from(node.multiplicity))
        .sum::<usize>();
    let agent_instances = proposal
        .nodes
        .iter()
        .filter(|node| {
            matches!(
                node.recipe,
                CapabilityRecipeId::Agent | CapabilityRecipeId::Review
            )
        })
        .map(|node| usize::from(node.multiplicity))
        .sum::<usize>();
    let total_instances = proposal
        .nodes
        .iter()
        .map(|node| usize::from(node.multiplicity))
        .sum::<usize>();
    if (team_count > 0 || agent_instances >= 2) && !understanding.forbids_team {
        understanding.required_team_count = understanding
            .required_team_count
            .max(u8::try_from(team_count).unwrap_or(u8::MAX));
        understanding.requests_multi_agent = true;
        understanding.requests_parallelism |=
            team_count > 1 || agent_instances > 1 || total_instances > 1;
        understanding.independent_workstreams = understanding.independent_workstreams.max(
            u8::try_from(total_instances.max(agent_instances).max(team_count)).unwrap_or(u8::MAX),
        );
        promote_complexity(&mut understanding, TaskComplexity::Complex);
    } else if proposal.nodes.iter().any(|node| {
        matches!(
            node.recipe,
            CapabilityRecipeId::Agent | CapabilityRecipeId::Review
        )
    }) {
        promote_complexity(&mut understanding, TaskComplexity::Moderate);
    }
    if proposal
        .nodes
        .iter()
        .any(|node| node.recipe == CapabilityRecipeId::Review)
    {
        understanding.requests_deliberation = true;
        understanding.uncertainty = understanding.uncertainty.max(4);
    }
    if request.operation == RuntimeOrchestrationOperation::Revise {
        understanding.estimated_duration = TaskDuration::LongRunning;
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
    request: &RuntimeOrchestrationCommand,
) -> Option<StrategyProposal> {
    if matches!(
        request.operation,
        RuntimeOrchestrationOperation::Inspect
            | RuntimeOrchestrationOperation::Control
            | RuntimeOrchestrationOperation::RouteInput
    ) {
        return None;
    }
    let proposal = request.proposal.as_ref()?;
    let agent_instances = proposal
        .nodes
        .iter()
        .filter(|node| {
            matches!(
                node.recipe,
                CapabilityRecipeId::Agent | CapabilityRecipeId::Review
            )
        })
        .map(|node| usize::from(node.multiplicity))
        .sum::<usize>();
    let pattern = if proposal
        .nodes
        .iter()
        .any(|node| node.recipe == CapabilityRecipeId::Team)
        || agent_instances >= 2
    {
        ExecutionPattern::Collaborate
    } else if proposal
        .nodes
        .iter()
        .any(|node| node.recipe == CapabilityRecipeId::Review)
    {
        ExecutionPattern::Deliberate
    } else if proposal
        .nodes
        .iter()
        .any(|node| node.recipe == CapabilityRecipeId::SessionDispatch)
    {
        ExecutionPattern::Supervise
    } else {
        ExecutionPattern::Execute
    };
    let mut modifiers = Vec::new();
    if proposal.nodes.len() > 1 || proposal.nodes.iter().any(|node| node.multiplicity > 1) {
        modifiers.push(ExecutionModifier::Parallel);
    }
    if pattern == ExecutionPattern::Supervise {
        modifiers.push(ExecutionModifier::Background);
    }
    Some(StrategyProposal {
        pattern,
        modifiers,
        template: proposal.nodes.iter().find_map(|node| node.template.clone()),
        confidence: 85,
        rationale: proposal.reason.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::{
        GraphMutationProposal, GraphSemanticNode, RuntimeOrchestrationConstraints,
    };
    use harness_contract::execution_graph::{
        ExecutionCompletionContract, ExecutionDependencyPolicy,
    };

    fn agent_node(node_id: &str) -> GraphSemanticNode {
        GraphSemanticNode {
            node_id: node_id.to_string(),
            recipe: CapabilityRecipeId::Agent,
            objective: format!("independent evidence for {node_id}"),
            depends_on: Vec::new(),
            multiplicity: 1,
            focuses: Vec::new(),
            managed_agent_escalation:
                harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
            template: None,
            target_session_id: None,
            output_artifacts: vec![format!("{node_id}_report")],
            evidence_contract: vec!["evidence".to_string()],
            required_evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            required: true,
            dependency: ExecutionDependencyPolicy::default(),
            cancellation_group: None,
        }
    }

    #[test]
    fn multiple_agent_nodes_are_a_collaboration_proposal() {
        let request = RuntimeOrchestrationCommand {
            intent: "使用两个 Agent 并行检查 Moon，不修改文件".to_string(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            lineage: None,
            mission_id: None,
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "multi-agent".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![agent_node("inventory"), agent_node("logs")],
                completion: ExecutionCompletionContract::default(),
                collaboration_program: None,
                collaboration_escalation: None,
                retired_collaboration_instance_ids: Vec::new(),
                reason: "two independent Agent roles".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),
            collaboration_intent: None,
            collaboration_semantic_intent: None,

            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints::default(),
            surface: None,
        };

        let proposal = strategy_proposal_from_request(&request).expect("strategy proposal");
        let understanding = understanding_with_proposal_signal(
            understand(&StrategyInput::from_prompt(request.intent.clone())),
            &request,
        );

        assert_eq!(proposal.pattern, ExecutionPattern::Collaborate);
        assert!(understanding.requests_multi_agent);
        assert!(understanding.requests_parallelism);
        assert_eq!(understanding.required_team_count, 0);
        assert!(understanding.independent_workstreams >= 2);
        assert!(!understanding.requires_write);
    }

    #[test]
    fn team_multiplicity_becomes_structured_required_cardinality() {
        let mut team = agent_node("research");
        team.recipe = CapabilityRecipeId::Team;
        team.multiplicity = 3;
        let request = RuntimeOrchestrationCommand {
            intent: "perform independent research".to_string(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            lineage: None,
            mission_id: None,
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "three-teams".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![team],
                completion: ExecutionCompletionContract::default(),
                collaboration_program: None,
                collaboration_escalation: None,
                retired_collaboration_instance_ids: Vec::new(),
                reason: "three independent teams".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),
            collaboration_intent: None,
            collaboration_semantic_intent: None,

            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints::default(),
            surface: None,
        };

        let understanding = understanding_with_proposal_signal(
            understand(&StrategyInput::from_prompt(&request.intent)),
            &request,
        );

        assert_eq!(understanding.required_team_count, 3);
    }
}
