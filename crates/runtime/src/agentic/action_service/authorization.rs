use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActorKind, TaskAttemptFailInput, TaskClaimInput,
    TaskPublishInput, TaskReleaseInput, TaskReviewInput, TaskSubmitInput, TaskSupersedeInput,
};

use crate::agentic::program::{AgenticProgramProjection, AgenticTaskStatus};

use super::{actor_agent_id, evidence_artifact_selector, is_durable_evidence_ref};

pub(super) fn validate_transition(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    now_ms: u64,
) -> Option<(&'static str, String)> {
    if projection.status == crate::agentic::program::AgenticProgramStatus::CompletionRequested
        && !matches!(envelope.action, AgentAction::StateInspect(_))
    {
        return Some((
            "objective_verdict_pending",
            "the completion request is durable and awaiting ObjectiveSupervisor verdict"
                .to_string(),
        ));
    }
    if matches!(
        projection.status,
        crate::agentic::program::AgenticProgramStatus::Verified
            | crate::agentic::program::AgenticProgramStatus::Blocked
    ) && !matches!(envelope.action, AgentAction::StateInspect(_))
    {
        return Some((
            "program_terminal",
            "a verified or blocked Program is immutable; start a new Objective to do more work"
                .to_string(),
        ));
    }
    match &envelope.action {
        AgentAction::StateInspect(_) => None,
        AgentAction::TeamCreate(_) => {
            if matches!(
                envelope.actor.kind,
                AgentActorKind::Root | AgentActorKind::Supervisor
            ) {
                None
            } else {
                Some((
                    "team_create_not_delegated",
                    "only the root or Program supervisor may create a new Team scope".to_string(),
                ))
            }
        }
        AgentAction::AgentInvite(input) => {
            if !projection.teams.contains_key(&input.team_ref) {
                return Some(("team_not_found", input.team_ref.clone()));
            }
            if matches!(
                envelope.actor.kind,
                AgentActorKind::Agent | AgentActorKind::TeamLead
            ) && envelope.actor.team_id.as_deref() != Some(input.team_ref.as_str())
            {
                return Some((
                    "cross_team_roster_mutation_not_delegated",
                    input.team_ref.clone(),
                ));
            }
            None
        }
        AgentAction::TaskPublish(input) => validate_task_publish(projection, envelope, input),
        AgentAction::TaskClaim(input) => validate_task_claim(projection, envelope, input, now_ms),
        AgentAction::TaskRelease(input) => validate_task_release(projection, envelope, input),
        AgentAction::TaskSupersede(input) => validate_task_supersede(projection, envelope, input),
        AgentAction::TaskAttemptFail(input) => {
            validate_task_attempt_failure(projection, envelope, input)
        }
        AgentAction::TaskSubmit(input) => {
            validate_task_submission(projection, envelope, input, now_ms)
        }
        AgentAction::TaskReview(input) => validate_task_review(projection, envelope, input),
        AgentAction::MessagePublish(input) => {
            let program_topic = format!("topic:{}", projection.program_id);
            let team_topic_exists = projection
                .teams
                .values()
                .any(|team| team.topic_ref == input.topic_ref);
            if input.topic_ref != program_topic && !team_topic_exists {
                return Some(("topic_not_found", input.topic_ref.clone()));
            }
            None
        }
        AgentAction::ArtifactCommit(input) => {
            if input.content_ref == "preceding_content" {
                return Some((
                    "content_ref_unresolved",
                    "Gateway must persist preceding content before Runtime action".to_string(),
                ));
            }
            None
        }
        AgentAction::ObjectiveCompleteRequest(input) => {
            if !matches!(
                envelope.actor.kind,
                AgentActorKind::Root | AgentActorKind::Supervisor
            ) {
                return Some((
                    "objective_completion_not_delegated",
                    "only the root or Objective supervisor may request Program completion"
                        .to_string(),
                ));
            }
            crate::agentic::supervision::completion_gap(projection, input)
                .map(|gap| ("objective_not_verifiable", gap))
        }
    }
}

fn validate_task_publish(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskPublishInput,
) -> Option<(&'static str, String)> {
    if !projection.teams.contains_key(&input.team_ref) {
        return Some(("team_not_found", input.team_ref.clone()));
    }
    if matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) && envelope.actor.team_id.as_deref() != Some(input.team_ref.as_str())
    {
        return Some((
            "cross_team_work_mutation_not_delegated",
            input.team_ref.clone(),
        ));
    }
    input
        .depends_on
        .iter()
        .find(|task| !projection.tasks.contains_key(*task))
        .map(|missing| ("dependency_not_found", missing.clone()))
}

fn validate_task_claim(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskClaimInput,
    now_ms: u64,
) -> Option<(&'static str, String)> {
    let Some(task) = projection.tasks.get(&input.task_ref) else {
        return Some(("task_not_found", input.task_ref.clone()));
    };
    let expired_claim = task.status == AgenticTaskStatus::Claimed
        && task
            .lease_expires_at_ms
            .is_some_and(|expires| expires <= now_ms);
    let same_execution_renewal = task.status == AgenticTaskStatus::Claimed
        && task.claimant.as_deref() == actor_agent_id(envelope)
        && task.claim_execution_id == envelope.actor.execution_id;
    if !matches!(
        task.status,
        AgenticTaskStatus::Published | AgenticTaskStatus::Rework
    ) && !expired_claim
        && !same_execution_renewal
    {
        return Some(("task_not_claimable", format!("{:?}", task.status)));
    }
    if !matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) {
        return Some((
            "actor_cannot_execute_task",
            "task claims require a roster-bound Agent".to_string(),
        ));
    }
    if let Some(waiting) = task.depends_on.iter().find(|dependency| {
        !crate::agentic::work_market::task_dependency_satisfied(projection, dependency)
    }) {
        return Some(("dependency_not_accepted", waiting.clone()));
    }
    let Some(agent_id) = envelope.actor.agent_id.as_ref() else {
        return Some(("actor_not_bound", "agent_id".to_string()));
    };
    let Some(member) = projection.agents.get(agent_id) else {
        return Some(("actor_not_in_roster", agent_id.clone()));
    };
    if member.team_id != task.team_id {
        return Some(("task_outside_actor_team", task.team_id.clone()));
    }
    if task
        .required_capabilities
        .iter()
        .any(|capability| !member.required_capabilities.contains(capability))
    {
        return Some((
            "actor_missing_capability",
            task.required_capabilities.join(","),
        ));
    }
    None
}

fn validate_task_release(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskReleaseInput,
) -> Option<(&'static str, String)> {
    let Some(task) = projection.tasks.get(&input.task_ref) else {
        return Some(("task_not_found", input.task_ref.clone()));
    };
    if task.status != AgenticTaskStatus::Claimed {
        return Some(("task_not_claimed", input.task_ref.clone()));
    }
    let supervisor_recovery = envelope.actor.kind == AgentActorKind::Supervisor;
    if !supervisor_recovery && task.claimant.as_deref() != actor_agent_id(envelope) {
        return Some(("task_claimant_mismatch", input.task_ref.clone()));
    }
    if task.claim_execution_id != envelope.actor.execution_id {
        return Some(("task_claim_fence_mismatch", input.task_ref.clone()));
    }
    None
}

fn validate_task_supersede(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskSupersedeInput,
) -> Option<(&'static str, String)> {
    let Some(task) = projection.tasks.get(&input.task_ref) else {
        return Some(("task_not_found", input.task_ref.clone()));
    };
    if !matches!(
        envelope.actor.kind,
        AgentActorKind::Root | AgentActorKind::TeamLead | AgentActorKind::Supervisor
    ) {
        return Some((
            "task_supersede_not_delegated",
            "only the root, owning Team lead, or Program supervisor may retire work".to_string(),
        ));
    }
    if envelope.actor.kind == AgentActorKind::TeamLead
        && envelope.actor.team_id.as_deref() != Some(task.team_id.as_str())
    {
        return Some((
            "cross_team_work_mutation_not_delegated",
            task.team_id.clone(),
        ));
    }
    if !matches!(
        task.status,
        AgenticTaskStatus::Published | AgenticTaskStatus::Rework | AgenticTaskStatus::Blocked
    ) {
        return Some(("task_not_supersedable", format!("{:?}", task.status)));
    }
    if task.status == AgenticTaskStatus::Published
        && task.failed_attempts == 0
        && task.failed_review_attempts == 0
    {
        return Some((
            "task_has_no_failed_or_challenged_attempt",
            "fresh work cannot be retired merely to bypass its acceptance obligation".to_string(),
        ));
    }
    for replacement_ref in &input.replacement_task_refs {
        let Some(replacement) = projection.tasks.get(replacement_ref) else {
            return Some(("replacement_task_not_found", replacement_ref.clone()));
        };
        if replacement.team_id != task.team_id {
            return Some(("replacement_task_outside_team", replacement_ref.clone()));
        }
        if replacement.status == AgenticTaskStatus::Superseded {
            return Some((
                "replacement_task_already_superseded",
                replacement_ref.clone(),
            ));
        }
        if crate::agentic::work_market::task_depends_on(
            projection,
            replacement_ref,
            &input.task_ref,
        ) {
            return Some((
                "replacement_depends_on_superseded_task",
                replacement_ref.clone(),
            ));
        }
    }
    input
        .evidence_refs
        .iter()
        .find(|reference| !is_durable_evidence_ref(reference))
        .map(|reference| ("unsupported_evidence_ref", reference.clone()))
}

fn validate_task_attempt_failure(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskAttemptFailInput,
) -> Option<(&'static str, String)> {
    if envelope.actor.kind != AgentActorKind::Supervisor {
        return Some((
            "attempt_failure_requires_supervisor",
            input.task_ref.clone(),
        ));
    }
    let Some(task) = projection.tasks.get(&input.task_ref) else {
        return Some(("task_not_found", input.task_ref.clone()));
    };
    match input.mode {
        harness_contract::agent_action::AgentAttemptMode::Execute => {
            if task.status != AgenticTaskStatus::Claimed {
                return Some(("task_not_claimed", input.task_ref.clone()));
            }
            if task.claim_execution_id.as_deref() != Some(input.execution_id.as_str()) {
                return Some(("task_claim_fence_mismatch", input.task_ref.clone()));
            }
        }
        harness_contract::agent_action::AgentAttemptMode::Review => {
            if task.status != AgenticTaskStatus::Submitted {
                return Some(("task_not_submitted", input.task_ref.clone()));
            }
        }
    }
    None
}

fn validate_task_submission(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskSubmitInput,
    now_ms: u64,
) -> Option<(&'static str, String)> {
    if !matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) {
        return Some((
            "actor_cannot_submit_task",
            "task submissions require the roster-bound claimant".to_string(),
        ));
    }
    let Some(task) = projection.tasks.get(&input.task_ref) else {
        return Some(("task_not_found", input.task_ref.clone()));
    };
    if task.status != AgenticTaskStatus::Claimed {
        return Some(("task_not_claimed", input.task_ref.clone()));
    }
    if task.claimant.as_deref() != actor_agent_id(envelope) {
        return Some(("task_claimant_mismatch", input.task_ref.clone()));
    }
    if task.claim_execution_id != envelope.actor.execution_id {
        return Some(("task_claim_fence_mismatch", input.task_ref.clone()));
    }
    if task
        .lease_expires_at_ms
        .is_none_or(|expires| expires <= now_ms)
    {
        return Some(("task_claim_expired", input.task_ref.clone()));
    }
    if let Some(missing) = input
        .artifact_refs
        .iter()
        .find(|artifact| !projection.artifacts.contains_key(*artifact))
    {
        return Some(("artifact_not_committed", missing.clone()));
    }
    if let Some(unowned) = input.artifact_refs.iter().find(|artifact_ref| {
        projection
            .artifacts
            .get(*artifact_ref)
            .is_some_and(|artifact| {
                artifact.committed_by != envelope.actor.actor_id
                    || !artifact.relates_to.contains(&input.task_ref)
            })
    }) {
        return Some(("artifact_not_bound_to_claimant_and_task", unowned.clone()));
    }
    if input.artifact_refs.is_empty() {
        return Some((
            "task_submission_has_no_artifact",
            "commit substantive work, then submit its compact artifact reference".to_string(),
        ));
    }
    if input.evidence_refs.is_empty() {
        return Some((
            "task_submission_has_no_evidence",
            "submit at least one durable tool:// or artifact:// evidence reference".to_string(),
        ));
    }
    if let Some(reference) = input
        .evidence_refs
        .iter()
        .find(|reference| !is_durable_evidence_ref(reference))
    {
        return Some(("unsupported_evidence_ref", reference.clone()));
    }
    let cites_submitted_artifact = input.evidence_refs.iter().any(|reference| {
        evidence_artifact_selector(reference).is_some_and(|selector| {
            input.artifact_refs.iter().any(|artifact_ref| {
                projection
                    .artifacts
                    .get(artifact_ref)
                    .is_some_and(|artifact| artifact.content_ref == selector)
            })
        })
    });
    if !cites_submitted_artifact {
        return Some((
            "submission_evidence_not_bound_to_artifact",
            input.task_ref.clone(),
        ));
    }
    None
}

fn validate_task_review(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskReviewInput,
) -> Option<(&'static str, String)> {
    if !matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) {
        return Some((
            "actor_cannot_review_task",
            "a roster-bound independent Agent must review".to_string(),
        ));
    }
    let Some(task) = projection.tasks.get(&input.task_ref) else {
        return Some(("task_not_found", input.task_ref.clone()));
    };
    if task.status != AgenticTaskStatus::Submitted {
        return Some(("task_not_submitted", input.task_ref.clone()));
    }
    if task.claimant.as_deref() == actor_agent_id(envelope) {
        return Some(("self_review_forbidden", input.task_ref.clone()));
    }
    let Some(reviewer_id) = envelope.actor.agent_id.as_ref() else {
        return Some(("actor_not_bound", "agent_id".to_string()));
    };
    if !projection.agents.contains_key(reviewer_id) {
        return Some(("actor_not_in_roster", reviewer_id.clone()));
    }
    if input.evidence_refs.is_empty() {
        return Some((
            "review_has_no_evidence",
            "independent review must cite inspected tool:// or artifact:// evidence".to_string(),
        ));
    }
    if let Some(reference) = input
        .evidence_refs
        .iter()
        .find(|reference| !is_durable_evidence_ref(reference))
    {
        return Some(("unsupported_evidence_ref", reference.clone()));
    }
    let cites_submitted_artifact = input.evidence_refs.iter().any(|reference| {
        evidence_artifact_selector(reference).is_some_and(|selector| {
            task.artifact_refs.iter().any(|artifact_ref| {
                projection
                    .artifacts
                    .get(artifact_ref)
                    .is_some_and(|artifact| artifact.content_ref == selector)
            })
        })
    });
    if !cites_submitted_artifact {
        return Some((
            "review_did_not_inspect_submitted_artifact",
            input.task_ref.clone(),
        ));
    }
    None
}
