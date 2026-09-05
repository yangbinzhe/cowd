use harness_contract::agent_action::{AgentActionEnvelope, ObjectiveCompleteRequestInput};
use harness_contract::goal::{
    ObjectiveEvidenceRequirement, ObjectiveObligation, ObjectiveObligationState,
};

use super::program::{
    AgenticCompletionRequestProjection, AgenticProgramProjection, AgenticProgramStatus,
    AgenticTaskStatus,
};
use super::AgentActionService;

pub(crate) fn completion_gap(
    projection: &AgenticProgramProjection,
    input: &ObjectiveCompleteRequestInput,
) -> Option<String> {
    if projection.teams.len() < usize::from(projection.required_team_count) {
        return Some(format!(
            "required_teams_missing:expected={},actual={}",
            projection.required_team_count,
            projection.teams.len()
        ));
    }
    let incomplete_teams = projection
        .teams
        .values()
        .filter(|team| {
            team.member_ids.is_empty()
                || !team.task_ids.iter().any(|task_id| {
                    projection
                        .tasks
                        .get(task_id)
                        .is_some_and(|task| task.status != AgenticTaskStatus::Superseded)
                })
        })
        .map(|team| team.team_id.clone())
        .collect::<Vec<_>>();
    if !incomplete_teams.is_empty() {
        return Some(format!(
            "teams_without_members_or_work:{}",
            incomplete_teams.join(",")
        ));
    }
    if !projection.artifacts.contains_key(&input.final_artifact_ref) {
        return Some("final_artifact_not_committed".to_string());
    }
    if !input.unresolved.is_empty() {
        return Some("completion_has_unresolved_items".to_string());
    }
    let tasks_with_unresolved = projection
        .tasks
        .values()
        .filter(|task| task.status != AgenticTaskStatus::Superseded && !task.unresolved.is_empty())
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    if !tasks_with_unresolved.is_empty() {
        return Some(format!(
            "tasks_have_unresolved_items:{}",
            tasks_with_unresolved.join(",")
        ));
    }
    let incomplete = projection
        .tasks
        .values()
        .filter(|task| {
            task.status != AgenticTaskStatus::Superseded
                && task.status != AgenticTaskStatus::Accepted
        })
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    if !incomplete.is_empty() {
        return Some(format!("tasks_not_accepted:{}", incomplete.join(",")));
    }
    if !projection.tasks.values().any(|task| {
        task.status == AgenticTaskStatus::Accepted
            && task.artifact_refs.contains(&input.final_artifact_ref)
    }) {
        return Some("final_artifact_not_accepted_by_task_review".to_string());
    }
    let Some(final_artifact) = projection.artifacts.get(&input.final_artifact_ref) else {
        return Some("final_artifact_not_committed".to_string());
    };
    let uncovered_teams = projection
        .teams
        .values()
        .filter(|team| {
            !team.task_ids.iter().any(|task_id| {
                projection.tasks.get(task_id).is_some_and(|task| {
                    task.status == AgenticTaskStatus::Accepted
                        && task.artifact_refs.iter().any(|artifact_ref| {
                            artifact_ref == &input.final_artifact_ref
                                || final_artifact.relates_to.contains(artifact_ref)
                        })
                })
            })
        })
        .map(|team| team.team_id.clone())
        .collect::<Vec<_>>();
    if !uncovered_teams.is_empty() {
        return Some(format!(
            "final_artifact_does_not_integrate_required_teams:{}",
            uncovered_teams.join(",")
        ));
    }
    let unverified = projection
        .tasks
        .values()
        .filter(|task| {
            task.status != AgenticTaskStatus::Superseded
                && (task.claimant.is_none()
                    || task.reviewed_by.is_none()
                    || task.claimant.as_deref() == task.reviewed_by.as_deref())
        })
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    if !unverified.is_empty() {
        return Some(format!(
            "tasks_without_independent_execution_and_review:{}",
            unverified.join(",")
        ));
    }
    let evidence = input
        .evidence_refs
        .iter()
        .chain(
            projection
                .tasks
                .values()
                .flat_map(|task| task.evidence_refs.iter()),
        )
        .collect::<Vec<_>>();
    if !projection.tasks.is_empty() && evidence.is_empty() {
        return Some("completion_has_no_evidence".to_string());
    }
    None
}

pub(crate) fn apply_completion_request(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &ObjectiveCompleteRequestInput,
    revision: u64,
) {
    projection.status = AgenticProgramStatus::CompletionRequested;
    projection.final_artifact_ref = Some(input.final_artifact_ref.clone());
    projection.unresolved.clone_from(&input.unresolved);
    projection.completion_request = Some(AgenticCompletionRequestProjection {
        action_id: envelope.action_id.clone(),
        requested_by: envelope.actor.actor_id.clone(),
        program_revision: revision,
        final_artifact_ref: input.final_artifact_ref.clone(),
        evidence_refs: input.evidence_refs.clone(),
        unresolved: input.unresolved.clone(),
    });
}

/// Reduce a model-authored completion request into the one authoritative
/// Objective terminal. Program is only a consumer of the resulting GoalStore
/// verdict; it cannot promote itself from request fields.
pub(crate) fn reconcile_completion_request(
    actions: &AgentActionService,
    supervisor: &crate::execution_core::goal::ObjectiveSupervisor,
    program_id: &str,
) -> Result<Option<AgenticProgramProjection>, String> {
    let projection = actions
        .project(program_id)
        .map_err(|error| error.to_string())?;
    if projection.status == AgenticProgramStatus::Verified {
        return Ok(Some(projection));
    }
    if projection.status != AgenticProgramStatus::CompletionRequested {
        return Ok(None);
    }
    let request = projection
        .completion_request
        .as_ref()
        .ok_or_else(|| "completion_requested Program has no durable request".to_string())?;
    let root_execution_id = projection
        .root_execution_id
        .as_deref()
        .ok_or_else(|| "completion request has no root execution binding".to_string())?;
    let goal_id = format!("goal:{root_execution_id}");
    if let Some(goal) = supervisor.goal_store().get(&goal_id)? {
        if goal.terminal.is_some() {
            return actions
                .bind_objective_verdict(program_id, &goal)
                .map(Some)
                .map_err(|error| error.to_string());
        }
    }

    let obligations = projection
        .tasks
        .values()
        .filter(|task| task.status != AgenticTaskStatus::Superseded)
        .map(|task| ObjectiveObligation {
            obligation_id: format!("agentic-task:{}", task.task_id),
            required: true,
            success_predicate: task.acceptance.clone(),
            producer: Default::default(),
            evidence_requirement: ObjectiveEvidenceRequirement {
                required_artifact_kinds: Vec::new(),
                independent_verifier_required: true,
                reread_required: false,
            },
            state: if task.status == AgenticTaskStatus::Accepted {
                ObjectiveObligationState::Satisfied
            } else {
                ObjectiveObligationState::Open
            },
            artifact_refs: task.artifact_refs.clone(),
            evidence_refs: task.evidence_refs.clone(),
            reread_receipts: Vec::new(),
            verifier_decision: task
                .reviewed_by
                .as_ref()
                .map(|reviewer| format!("accepted_by:{reviewer}")),
            diagnostic_code: None,
        })
        .collect::<Vec<_>>();
    let mut evidence_refs = request.evidence_refs.clone();
    evidence_refs.push(format!("execution_graph:{root_execution_id}"));
    for task in projection.tasks.values() {
        evidence_refs.extend(task.evidence_refs.iter().cloned());
        evidence_refs.extend(task.supersede_evidence_refs.iter().cloned());
    }
    evidence_refs.sort();
    evidence_refs.dedup();
    let terminal_fence = format!(
        "agentic-objective:{program_id}:request:{}",
        request.program_revision
    );
    match supervisor.reconcile(
        &goal_id,
        request.program_revision,
        &terminal_fence,
        obligations,
        evidence_refs,
        Vec::new(),
        false,
        format!(
            "Agentic Program `{program_id}` completion request `{}` passed Objective supervision",
            request.action_id
        ),
    )? {
        crate::execution_core::goal::ObjectiveReconcileDecision::Terminal(goal) => actions
            .bind_objective_verdict(program_id, &goal)
            .map(Some)
            .map_err(|error| error.to_string()),
        crate::execution_core::goal::ObjectiveReconcileDecision::Waiting { .. }
        | crate::execution_core::goal::ObjectiveReconcileDecision::ReplanRequired { .. } => {
            Ok(Some(projection))
        }
    }
}
