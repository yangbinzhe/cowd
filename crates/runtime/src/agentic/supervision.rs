use harness_contract::agent_action::{AgentActionEnvelope, ObjectiveCompleteRequestInput};
use harness_contract::goal::{
    ObjectiveEvidenceRequirement, ObjectiveObligation, ObjectiveObligationState,
};
use std::collections::BTreeSet;

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
    // Task acceptance is the independent reviewer's completion verdict.
    // A Task's `unresolved` field remains a durable disclosure for the
    // supervisor and final synthesis; it is not a second completion veto.
    // Objective-level blockers belong exclusively to `input.unresolved`.
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
    let integrated_tasks = integrated_accepted_tasks(projection, &input.final_artifact_ref);
    let uncovered_teams = projection
        .teams
        .values()
        .filter(|team| {
            !team
                .task_ids
                .iter()
                .any(|task_id| integrated_tasks.contains(task_id))
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

/// Resolve the accepted Task lineage represented by a final artifact.
///
/// Models should express synthesis as an ordinary Task dependency graph. The
/// supervisor starts only from accepted Tasks that actually submitted the
/// final Artifact, then follows accepted `depends_on` edges transitively.
/// A dependency may have been durably superseded; in that case the walk follows
/// its same-Team replacement lineage and counts only accepted successors.
/// `Artifact.relates_to` is useful semantic metadata but is model-authored, so
/// it can never prove cross-Team integration by itself.
fn integrated_accepted_tasks(
    projection: &AgenticProgramProjection,
    final_artifact_ref: &str,
) -> BTreeSet<String> {
    let mut integrated = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut pending = projection
        .tasks
        .values()
        .filter(|task| {
            task.status == AgenticTaskStatus::Accepted
                && task
                    .artifact_refs
                    .iter()
                    .any(|artifact_ref| artifact_ref == final_artifact_ref)
        })
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();

    while let Some(task_ref) = pending.pop() {
        if !visited.insert(task_ref.clone()) {
            continue;
        }
        let Some(task) = projection.tasks.get(&task_ref) else {
            continue;
        };
        match task.status {
            AgenticTaskStatus::Accepted => {
                integrated.insert(task.task_id.clone());
                pending.extend(task.depends_on.iter().cloned());
            }
            AgenticTaskStatus::Superseded => {
                pending.extend(task.replacement_task_refs.iter().cloned());
            }
            _ => {}
        }
    }

    integrated
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agentic::program::{AgenticArtifactProjection, AgenticTaskProjection};

    fn accepted_task(
        task_id: &str,
        team_id: &str,
        depends_on: Vec<&str>,
        artifact_refs: Vec<&str>,
    ) -> AgenticTaskProjection {
        AgenticTaskProjection {
            task_id: task_id.to_string(),
            team_id: team_id.to_string(),
            title: task_id.to_string(),
            objective: task_id.to_string(),
            acceptance: "reviewed".to_string(),
            required_capabilities: Vec::new(),
            depends_on: depends_on.into_iter().map(str::to_string).collect(),
            status: AgenticTaskStatus::Accepted,
            claimant: Some("agent:author".to_string()),
            claim_generation: 1,
            claim_execution_id: Some("graph:author".to_string()),
            claimed_at_ms: Some(1),
            lease_expires_at_ms: Some(2),
            artifact_refs: artifact_refs.into_iter().map(str::to_string).collect(),
            evidence_refs: vec!["evidence:review".to_string()],
            unresolved: Vec::new(),
            review_reason: Some("accepted".to_string()),
            reviewed_by: Some("agent:reviewer".to_string()),
            failed_attempts: 0,
            review_generation: 1,
            failed_review_attempts: 0,
            last_failure: None,
            replacement_task_refs: Vec::new(),
            supersede_evidence_refs: Vec::new(),
            superseded_reason: None,
            superseded_by: None,
        }
    }

    #[test]
    fn final_artifact_inherits_team_coverage_from_task_dependency_dag() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert(
            "task:research".to_string(),
            accepted_task(
                "task:research",
                "team:research",
                Vec::new(),
                vec!["artifact:research"],
            ),
        );
        projection.tasks.insert(
            "task:experiment".to_string(),
            accepted_task(
                "task:experiment",
                "team:experiment",
                Vec::new(),
                vec!["artifact:experiment"],
            ),
        );
        projection.tasks.insert(
            "task:synthesis".to_string(),
            accepted_task(
                "task:synthesis",
                "team:integration",
                vec!["task:research", "task:experiment"],
                vec!["artifact:final"],
            ),
        );
        projection.artifacts.insert(
            "artifact:final".to_string(),
            AgenticArtifactProjection {
                artifact_ref: "artifact:final".to_string(),
                content_ref: "workspace://report.html".to_string(),
                kind: "final-report".to_string(),
                title: "Final report".to_string(),
                relates_to: vec!["task:synthesis".to_string()],
                committed_by: "agent:author".to_string(),
            },
        );

        assert_eq!(
            integrated_accepted_tasks(&projection, "artifact:final"),
            BTreeSet::from([
                "task:experiment".to_string(),
                "task:research".to_string(),
                "task:synthesis".to_string(),
            ])
        );
    }

    #[test]
    fn unaccepted_dependency_cannot_supply_transitive_team_coverage() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        let mut unaccepted = accepted_task(
            "task:unaccepted",
            "team:research",
            vec!["task:hidden"],
            vec!["artifact:research"],
        );
        unaccepted.status = AgenticTaskStatus::Submitted;
        projection
            .tasks
            .insert("task:unaccepted".to_string(), unaccepted);
        projection.tasks.insert(
            "task:hidden".to_string(),
            accepted_task(
                "task:hidden",
                "team:hidden",
                Vec::new(),
                vec!["artifact:hidden"],
            ),
        );
        projection.tasks.insert(
            "task:synthesis".to_string(),
            accepted_task(
                "task:synthesis",
                "team:integration",
                vec!["task:unaccepted"],
                vec!["artifact:final"],
            ),
        );
        projection.artifacts.insert(
            "artifact:final".to_string(),
            AgenticArtifactProjection {
                artifact_ref: "artifact:final".to_string(),
                content_ref: "workspace://report.html".to_string(),
                kind: "final-report".to_string(),
                title: "Final report".to_string(),
                relates_to: vec!["task:synthesis".to_string()],
                committed_by: "agent:author".to_string(),
            },
        );

        assert_eq!(
            integrated_accepted_tasks(&projection, "artifact:final"),
            BTreeSet::from(["task:synthesis".to_string()])
        );
    }

    #[test]
    fn superseded_dependency_contributes_coverage_through_accepted_replacement() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.required_team_count = 2;
        projection.teams.insert(
            "team:research".to_string(),
            crate::agentic::program::AgenticTeamProjection {
                team_id: "team:research".to_string(),
                name: "Research".to_string(),
                mission: "Research".to_string(),
                objective: Some("Research".to_string()),
                topic_ref: "topic:research".to_string(),
                created_by: "root".to_string(),
                member_ids: vec!["agent:research".to_string()],
                task_ids: vec!["task:source".to_string(), "task:replacement".to_string()],
            },
        );
        projection.teams.insert(
            "team:integration".to_string(),
            crate::agentic::program::AgenticTeamProjection {
                team_id: "team:integration".to_string(),
                name: "Integration".to_string(),
                mission: "Integration".to_string(),
                objective: Some("Integration".to_string()),
                topic_ref: "topic:integration".to_string(),
                created_by: "root".to_string(),
                member_ids: vec!["agent:integration".to_string()],
                task_ids: vec!["task:synthesis".to_string()],
            },
        );
        let mut superseded = accepted_task("task:source", "team:research", Vec::new(), Vec::new());
        superseded.status = AgenticTaskStatus::Superseded;
        superseded.claimant = None;
        superseded.reviewed_by = None;
        superseded.replacement_task_refs = vec!["task:replacement".to_string()];
        projection
            .tasks
            .insert("task:source".to_string(), superseded);
        projection.tasks.insert(
            "task:replacement".to_string(),
            accepted_task(
                "task:replacement",
                "team:research",
                Vec::new(),
                vec!["artifact:research"],
            ),
        );
        projection.tasks.insert(
            "task:synthesis".to_string(),
            accepted_task(
                "task:synthesis",
                "team:integration",
                vec!["task:source"],
                vec!["artifact:final"],
            ),
        );
        projection.artifacts.insert(
            "artifact:final".to_string(),
            AgenticArtifactProjection {
                artifact_ref: "artifact:final".to_string(),
                content_ref: "workspace://report.html".to_string(),
                kind: "final-report".to_string(),
                title: "Final report".to_string(),
                relates_to: Vec::new(),
                committed_by: "agent:integration".to_string(),
            },
        );
        let request = ObjectiveCompleteRequestInput {
            final_artifact_ref: "artifact:final".to_string(),
            evidence_refs: vec!["tool://verified".to_string()],
            unresolved: Vec::new(),
        };

        assert_eq!(completion_gap(&projection, &request), None);
        assert_eq!(
            integrated_accepted_tasks(&projection, "artifact:final"),
            BTreeSet::from(["task:replacement".to_string(), "task:synthesis".to_string(),])
        );
    }

    #[test]
    fn superseded_replacement_cycle_cannot_hang_or_forge_coverage() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.required_team_count = 2;
        projection.teams.insert(
            "team:a".to_string(),
            crate::agentic::program::AgenticTeamProjection {
                team_id: "team:a".to_string(),
                name: "A".to_string(),
                mission: "A".to_string(),
                objective: Some("A".to_string()),
                topic_ref: "topic:a".to_string(),
                created_by: "root".to_string(),
                member_ids: vec!["agent:a".to_string()],
                task_ids: vec![
                    "task:first".to_string(),
                    "task:second".to_string(),
                    "task:other".to_string(),
                ],
            },
        );
        projection.teams.insert(
            "team:integration".to_string(),
            crate::agentic::program::AgenticTeamProjection {
                team_id: "team:integration".to_string(),
                name: "Integration".to_string(),
                mission: "Integration".to_string(),
                objective: Some("Integration".to_string()),
                topic_ref: "topic:integration".to_string(),
                created_by: "root".to_string(),
                member_ids: vec!["agent:integration".to_string()],
                task_ids: vec!["task:synthesis".to_string()],
            },
        );
        let mut first = accepted_task("task:first", "team:a", Vec::new(), Vec::new());
        first.status = AgenticTaskStatus::Superseded;
        first.replacement_task_refs = vec!["task:second".to_string()];
        let mut second = accepted_task("task:second", "team:a", Vec::new(), Vec::new());
        second.status = AgenticTaskStatus::Superseded;
        second.replacement_task_refs = vec!["task:first".to_string()];
        projection.tasks.insert("task:first".to_string(), first);
        projection.tasks.insert("task:second".to_string(), second);
        projection.tasks.insert(
            "task:other".to_string(),
            accepted_task("task:other", "team:a", Vec::new(), vec!["artifact:other"]),
        );
        projection.tasks.insert(
            "task:synthesis".to_string(),
            accepted_task(
                "task:synthesis",
                "team:integration",
                vec!["task:first"],
                vec!["artifact:final"],
            ),
        );
        projection.artifacts.insert(
            "artifact:final".to_string(),
            AgenticArtifactProjection {
                artifact_ref: "artifact:final".to_string(),
                content_ref: "workspace://report.html".to_string(),
                kind: "final-report".to_string(),
                title: "Final report".to_string(),
                relates_to: Vec::new(),
                committed_by: "agent:integration".to_string(),
            },
        );

        assert_eq!(
            integrated_accepted_tasks(&projection, "artifact:final"),
            BTreeSet::from(["task:synthesis".to_string()])
        );
        assert_eq!(
            completion_gap(
                &projection,
                &ObjectiveCompleteRequestInput {
                    final_artifact_ref: "artifact:final".to_string(),
                    evidence_refs: vec!["tool://verified".to_string()],
                    unresolved: Vec::new(),
                },
            ),
            Some("final_artifact_does_not_integrate_required_teams:team:a".to_string())
        );
    }

    #[test]
    fn model_authored_artifact_relations_cannot_forge_team_coverage() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert(
            "task:unrelated".to_string(),
            accepted_task(
                "task:unrelated",
                "team:unrelated",
                Vec::new(),
                vec!["artifact:unrelated"],
            ),
        );
        projection.tasks.insert(
            "task:synthesis".to_string(),
            accepted_task(
                "task:synthesis",
                "team:integration",
                Vec::new(),
                vec!["artifact:final"],
            ),
        );
        projection.artifacts.insert(
            "artifact:final".to_string(),
            AgenticArtifactProjection {
                artifact_ref: "artifact:final".to_string(),
                content_ref: "workspace://report.html".to_string(),
                kind: "final-report".to_string(),
                title: "Final report".to_string(),
                relates_to: vec!["task:unrelated".to_string()],
                committed_by: "agent:author".to_string(),
            },
        );

        assert_eq!(
            integrated_accepted_tasks(&projection, "artifact:final"),
            BTreeSet::from(["task:synthesis".to_string()]),
            "model-authored artifact metadata must not substitute for a real Task dependency"
        );
    }
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
