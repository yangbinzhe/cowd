use harness_contract::agent_action::{
    AgentActionEnvelope, AgentAttemptMode, TaskAttemptFailInput, TaskClaimInput, TaskPublishInput,
    TaskReleaseInput, TaskReviewDecision, TaskReviewInput, TaskSubmitInput, TaskSupersedeInput,
};

use super::program::{AgenticProgramProjection, AgenticTaskProjection, AgenticTaskStatus};

// Claims use a short rolling fence. Runtime renews the fence while the
// corresponding physical Agent graph is active; after a crash the absence of
// heartbeats makes the work reclaimable without imposing a task-quality
// deadline on healthy long-running work.
pub(crate) const CLAIM_LEASE_MS: u64 = 90 * 1_000;

pub(crate) fn apply_task_publish(
    projection: &mut AgenticProgramProjection,
    _envelope: &AgentActionEnvelope,
    input: &TaskPublishInput,
    entity_ref: Option<&str>,
) {
    let Some(task_id) = entity_ref else {
        return;
    };
    projection.tasks.insert(
        task_id.to_string(),
        AgenticTaskProjection {
            task_id: task_id.to_string(),
            team_id: input.team_ref.clone(),
            title: input.title.trim().to_string(),
            objective: input.objective.trim().to_string(),
            acceptance: input.acceptance.trim().to_string(),
            required_capabilities: input.required_capabilities.clone(),
            depends_on: input.depends_on.clone(),
            status: AgenticTaskStatus::Published,
            claimant: None,
            claim_generation: 0,
            claim_execution_id: None,
            claimed_at_ms: None,
            lease_expires_at_ms: None,
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            review_reason: None,
            reviewed_by: None,
            failed_attempts: 0,
            review_generation: 0,
            failed_review_attempts: 0,
            last_failure: None,
            replacement_task_refs: Vec::new(),
            supersede_evidence_refs: Vec::new(),
            superseded_reason: None,
            superseded_by: None,
        },
    );
    if let Some(team) = projection.teams.get_mut(&input.team_ref) {
        team.task_ids.push(task_id.to_string());
    }
}

pub(crate) fn apply_task_attempt_fail(
    projection: &mut AgenticProgramProjection,
    input: &TaskAttemptFailInput,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        task.last_failure = Some(input.reason.clone());
        match input.mode {
            AgentAttemptMode::Execute => {
                task.failed_attempts = task.failed_attempts.saturating_add(1);
                task.claimant = None;
                task.claim_execution_id = None;
                task.claimed_at_ms = None;
                task.lease_expires_at_ms = None;
                task.status = if input.retryable && task.failed_attempts < 3 {
                    AgenticTaskStatus::Rework
                } else {
                    // Exhausting the automatic retry window is a scheduling
                    // checkpoint, not a business terminal. Returning the work
                    // to Published lets the root change the assignee, split
                    // the work or publish replacement work without making one
                    // provider/tool failure permanently Block the Program.
                    AgenticTaskStatus::Published
                };
            }
            AgentAttemptMode::Review => {
                task.failed_review_attempts = task.failed_review_attempts.saturating_add(1);
                task.review_generation = task.review_generation.saturating_add(1);
                task.status = if input.retryable && task.failed_review_attempts < 3 {
                    AgenticTaskStatus::Submitted
                } else {
                    // Stop automatic review redispatch at the retry boundary.
                    // The root can invite a new reviewer or replan the work;
                    // only ObjectiveSupervisor may create a business terminal.
                    AgenticTaskStatus::Published
                };
            }
        }
    }
}

pub(crate) fn apply_task_claim(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskClaimInput,
    applied_at_ms: u64,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        let renewal = task.status == AgenticTaskStatus::Claimed
            && task.claimant.as_deref()
                == envelope
                    .actor
                    .agent_id
                    .as_deref()
                    .or(Some(envelope.actor.actor_id.as_str()))
            && task.claim_execution_id == envelope.actor.execution_id;
        if !renewal {
            task.claim_generation = task.claim_generation.saturating_add(1);
        }
        task.status = AgenticTaskStatus::Claimed;
        task.claimant = Some(
            envelope
                .actor
                .agent_id
                .clone()
                .unwrap_or_else(|| envelope.actor.actor_id.clone()),
        );
        task.claim_execution_id
            .clone_from(&envelope.actor.execution_id);
        task.claimed_at_ms = Some(applied_at_ms);
        task.lease_expires_at_ms = Some(applied_at_ms.saturating_add(CLAIM_LEASE_MS));
    }
}

pub(crate) fn apply_task_release(
    projection: &mut AgenticProgramProjection,
    input: &TaskReleaseInput,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        task.status = AgenticTaskStatus::Published;
        task.claimant = None;
        task.claim_execution_id = None;
        task.claimed_at_ms = None;
        task.lease_expires_at_ms = None;
    }
}

pub(crate) fn apply_task_supersede(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskSupersedeInput,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        task.status = AgenticTaskStatus::Superseded;
        task.claimant = None;
        task.claim_execution_id = None;
        task.claimed_at_ms = None;
        task.lease_expires_at_ms = None;
        task.replacement_task_refs
            .clone_from(&input.replacement_task_refs);
        task.supersede_evidence_refs
            .clone_from(&input.evidence_refs);
        task.superseded_reason = Some(input.reason.clone());
        task.superseded_by = Some(envelope.actor.actor_id.clone());
    }
}

/// A superseded dependency is satisfied only when every concrete successor is
/// itself accepted (or recursively superseded by accepted successors). This
/// preserves downstream dependencies without rewriting already-journaled Task
/// contracts and never treats retirement itself as successful work.
pub(crate) fn task_dependency_satisfied(
    projection: &AgenticProgramProjection,
    task_ref: &str,
) -> bool {
    fn visit(
        projection: &AgenticProgramProjection,
        task_ref: &str,
        visiting: &mut std::collections::BTreeSet<String>,
    ) -> bool {
        let Some(task) = projection.tasks.get(task_ref) else {
            return false;
        };
        match task.status {
            AgenticTaskStatus::Accepted => true,
            AgenticTaskStatus::Superseded => {
                if task.replacement_task_refs.is_empty() || !visiting.insert(task_ref.to_string()) {
                    return false;
                }
                let satisfied = task
                    .replacement_task_refs
                    .iter()
                    .all(|replacement| visit(projection, replacement, visiting));
                visiting.remove(task_ref);
                satisfied
            }
            _ => false,
        }
    }

    visit(projection, task_ref, &mut std::collections::BTreeSet::new())
}

pub(crate) fn task_depends_on(
    projection: &AgenticProgramProjection,
    task_ref: &str,
    dependency_ref: &str,
) -> bool {
    fn visit(
        projection: &AgenticProgramProjection,
        task_ref: &str,
        dependency_ref: &str,
        visiting: &mut std::collections::BTreeSet<String>,
    ) -> bool {
        let Some(task) = projection.tasks.get(task_ref) else {
            return false;
        };
        if task.depends_on.iter().any(|item| item == dependency_ref) {
            return true;
        }
        if !visiting.insert(task_ref.to_string()) {
            return false;
        }
        let found = task
            .depends_on
            .iter()
            .any(|dependency| visit(projection, dependency, dependency_ref, visiting));
        visiting.remove(task_ref);
        found
    }

    visit(
        projection,
        task_ref,
        dependency_ref,
        &mut std::collections::BTreeSet::new(),
    )
}

pub(crate) fn apply_task_submit(
    projection: &mut AgenticProgramProjection,
    input: &TaskSubmitInput,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        task.status = AgenticTaskStatus::Submitted;
        task.artifact_refs.clone_from(&input.artifact_refs);
        task.evidence_refs.clone_from(&input.evidence_refs);
        task.unresolved.clone_from(&input.unresolved);
    }
}

pub(crate) fn apply_task_review(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskReviewInput,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        task.reviewed_by = Some(envelope.actor.actor_id.clone());
        task.review_reason = Some(input.reason.clone());
        task.evidence_refs
            .extend(input.evidence_refs.iter().cloned());
        task.evidence_refs.sort();
        task.evidence_refs.dedup();
        match input.decision {
            TaskReviewDecision::Accept => task.status = AgenticTaskStatus::Accepted,
            TaskReviewDecision::Challenge | TaskReviewDecision::Rework => {
                task.status = AgenticTaskStatus::Rework;
                task.claimant = None;
                task.claim_execution_id = None;
                task.claimed_at_ms = None;
                task.lease_expires_at_ms = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::agent_action::{
        AgentAction, AgentActorBinding, AgentActorKind, TaskClaimInput,
    };

    use super::*;

    fn claim_envelope(execution_id: &str) -> AgentActionEnvelope {
        AgentActionEnvelope {
            action_id: format!("claim:{execution_id}"),
            actor: AgentActorBinding {
                objective_id: "objective".to_string(),
                program_id: "program".to_string(),
                session_id: "session".to_string(),
                turn_id: "turn".to_string(),
                root_execution_id: Some("root".to_string()),
                required_team_count: 1,
                objective_summary: "long-running task".to_string(),
                model_lease: "model".to_string(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: Vec::new(),
                actor_id: "agent".to_string(),
                kind: AgentActorKind::Agent,
                execution_id: Some(execution_id.to_string()),
                team_id: Some("team".to_string()),
                agent_id: Some("agent".to_string()),
            },
            expected_revision: None,
            action: AgentAction::TaskClaim(TaskClaimInput {
                task_ref: "task".to_string(),
                reason: None,
            }),
        }
    }

    #[test]
    fn same_execution_claim_heartbeat_renews_without_advancing_generation() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert(
            "task".to_string(),
            AgenticTaskProjection {
                task_id: "task".to_string(),
                team_id: "team".to_string(),
                title: "long task".to_string(),
                objective: "finish without an arbitrary wall timeout".to_string(),
                acceptance: "durable evidence".to_string(),
                required_capabilities: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Published,
                claimant: None,
                claim_generation: 0,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 0,
                failed_review_attempts: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
            },
        );
        let envelope = claim_envelope("execution");
        let input = match &envelope.action {
            AgentAction::TaskClaim(input) => input,
            _ => unreachable!(),
        };
        apply_task_claim(&mut projection, &envelope, input, 10);
        let first_expiry = projection.tasks["task"]
            .lease_expires_at_ms
            .expect("initial lease");
        apply_task_claim(&mut projection, &envelope, input, first_expiry - 1);
        let task = &projection.tasks["task"];
        assert_eq!(task.claim_generation, 1);
        assert_eq!(task.claim_execution_id.as_deref(), Some("execution"));
        assert!(task.lease_expires_at_ms.expect("renewed lease") > first_expiry);
    }

    #[test]
    fn superseded_dependency_waits_for_every_split_successor() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        let base = AgenticTaskProjection {
            task_id: "source".to_string(),
            team_id: "team".to_string(),
            title: "source".to_string(),
            objective: "objective".to_string(),
            acceptance: "accepted evidence".to_string(),
            required_capabilities: Vec::new(),
            depends_on: Vec::new(),
            status: AgenticTaskStatus::Superseded,
            claimant: None,
            claim_generation: 0,
            claim_execution_id: None,
            claimed_at_ms: None,
            lease_expires_at_ms: None,
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            review_reason: None,
            reviewed_by: None,
            failed_attempts: 1,
            review_generation: 0,
            failed_review_attempts: 0,
            last_failure: Some("disproved".to_string()),
            replacement_task_refs: vec!["part-a".to_string(), "part-b".to_string()],
            supersede_evidence_refs: vec!["tool://failure".to_string()],
            superseded_reason: Some("split".to_string()),
            superseded_by: Some("lead".to_string()),
        };
        projection.tasks.insert("source".to_string(), base.clone());
        for (id, status) in [
            ("part-a", AgenticTaskStatus::Accepted),
            ("part-b", AgenticTaskStatus::Published),
        ] {
            let mut successor = base.clone();
            successor.task_id = id.to_string();
            successor.status = status;
            successor.replacement_task_refs.clear();
            successor.supersede_evidence_refs.clear();
            successor.superseded_reason = None;
            successor.superseded_by = None;
            projection.tasks.insert(id.to_string(), successor);
        }

        assert!(!task_dependency_satisfied(&projection, "source"));
        projection.tasks.get_mut("part-b").expect("part b").status = AgenticTaskStatus::Accepted;
        assert!(task_dependency_satisfied(&projection, "source"));
    }
}
