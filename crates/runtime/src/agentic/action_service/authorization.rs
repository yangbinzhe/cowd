use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActorKind, TaskAttemptDispatchInput,
    TaskAttemptFailInput, TaskClaimInput, TaskPublishInput, TaskReleaseInput, TaskReviewInput,
    TaskSubmitInput, TaskSupersedeInput, TaskWithdrawInput,
};

use crate::agentic::program::{AgenticProgramProjection, AgenticTaskStatus, AgenticTeamLifecycle};

use super::{actor_agent_id, is_durable_evidence_ref};

pub(super) fn validate_transition(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    now_ms: u64,
    graphs: Option<&crate::ExecutionGraphStateStore>,
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
            | crate::agentic::program::AgenticProgramStatus::Partial
            | crate::agentic::program::AgenticProgramStatus::Blocked
            | crate::agentic::program::AgenticProgramStatus::Failed
            | crate::agentic::program::AgenticProgramStatus::Cancelled
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
            ) || super::delegation::can_create_team(graphs, projection, envelope)
            {
                None
            } else {
                Some((
                    "team_create_not_delegated",
                    "Team creation requires a current immutable execution grant in the bound Team"
                        .to_string(),
                ))
            }
        }
        AgentAction::AgentInvite(input) => {
            let Some(team) = projection.teams.get(&input.team_ref) else {
                return Some(("team_not_found", input.team_ref.clone()));
            };
            if team.lifecycle != AgenticTeamLifecycle::Active {
                return Some(("team_not_accepting_members", input.team_ref.clone()));
            }
            if matches!(
                envelope.actor.kind,
                AgentActorKind::Agent | AgentActorKind::TeamLead
            ) && !super::delegation::can_organize_team(projection, envelope, &input.team_ref)
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
        AgentAction::TaskWithdraw(input) => validate_task_withdraw(projection, envelope, input),
        AgentAction::TaskAttemptDispatch(input) => {
            validate_task_attempt_dispatch(projection, envelope, input)
        }
        AgentAction::TaskAttemptFail(input) => {
            validate_task_attempt_failure(projection, envelope, input)
        }
        AgentAction::TaskSubmit(input) => {
            validate_task_submission(projection, envelope, input, now_ms)
        }
        AgentAction::TaskReview(input) => validate_task_review(projection, envelope, input),
        AgentAction::MessagePublish(input) => {
            if let Some(error) =
                super::super::issues::validate_dispositions(projection, envelope, input)
            {
                return Some(error);
            }
            let program_topic = format!("topic:{}", projection.program_id);
            let team_topic_exists = projection
                .teams
                .values()
                .any(|team| team.topic_ref == input.topic_ref);
            if input.topic_ref != program_topic && !team_topic_exists {
                return Some(("topic_not_found", input.topic_ref.clone()));
            }
            if let Some(intent) = input.intent.as_ref() {
                if intent.kind == harness_contract::agent_action::TaskIntentKind::RequestHelp
                    && input
                        .summary
                        .as_deref()
                        .is_none_or(|text| text.trim().is_empty())
                    && input.content_ref.is_none()
                    && intent.reason_ref.is_none()
                {
                    return Some(("coordination_reason_required", intent.task_ref.clone()));
                }
                let Some(task) = projection.tasks.get(&intent.task_ref) else {
                    return Some(("task_not_found", intent.task_ref.clone()));
                };
                if intent.kind == harness_contract::agent_action::TaskIntentKind::Offer
                    && (!matches!(
                        envelope.actor.kind,
                        AgentActorKind::Agent | AgentActorKind::TeamLead
                    ) || envelope.actor.execution_id.is_none()
                        || envelope.actor.agent_id.as_deref()
                            != Some(envelope.actor.actor_id.as_str())
                        || !projection.agent_is_active_in(&envelope.actor.actor_id, &task.team_id))
                {
                    return Some(("offer_requires_bound_team_agent", intent.task_ref.clone()));
                }
                if intent.kind == harness_contract::agent_action::TaskIntentKind::Decline {
                    if !matches!(
                        envelope.actor.kind,
                        AgentActorKind::Agent | AgentActorKind::TeamLead
                    ) {
                        return Some(("decline_requires_bound_agent", intent.task_ref.clone()));
                    }
                    let attempt = envelope
                        .actor
                        .execution_id
                        .as_ref()
                        .and_then(|id| task.active_attempts.get(id));
                    let current = attempt.is_some_and(|attempt| {
                        Some(attempt.agent_id.as_str()) == envelope.actor.agent_id.as_deref()
                            && attempt.agent_id == envelope.actor.actor_id
                            && attempt.mode
                                == harness_contract::agent_action::AgentAttemptMode::Execute
                            && attempt.generation == task.claim_generation
                            && projection.agent_is_active_in(&attempt.agent_id, &task.team_id)
                    });
                    if !current
                        || !matches!(
                            task.status,
                            AgenticTaskStatus::Published | AgenticTaskStatus::Rework
                        )
                    {
                        return Some((
                            "decline_opportunity_fence_mismatch",
                            intent.task_ref.clone(),
                        ));
                    }
                    if input
                        .summary
                        .as_deref()
                        .is_none_or(|reason| reason.trim().is_empty())
                        && input.content_ref.is_none()
                        && intent.reason_ref.is_none()
                    {
                        return Some(("decline_reason_required", intent.task_ref.clone()));
                    }
                }
            }
            None
        }
        AgentAction::ArtifactCommit(input) => {
            if !input.content_ref.starts_with("artifact://") {
                return Some((
                    "content_ref_unresolved",
                    "Commit a non-empty deliverable: use artifact_publish with a non-empty file, or select a current_message_block:<index> that actually contains the findings. An empty or absent Text block is never committed.".to_string(),
                ));
            }
            None
        }
        AgentAction::ObjectiveReview(_)
            if super::delegation::can_invoke_bound_action(
                graphs,
                projection,
                envelope,
                "objective_review",
            ) =>
        {
            None
        }
        AgentAction::ObjectiveUpdate(_) | AgentAction::ObjectiveReview(_) => {
            if matches!(
                envelope.actor.kind,
                AgentActorKind::Root | AgentActorKind::Supervisor
            ) {
                None
            } else {
                Some((
                    "objective_mutation_not_delegated",
                    "only the root or Objective supervisor may mutate the user Objective"
                        .to_string(),
                ))
            }
        }
        AgentAction::MembershipUpdate(input) => {
            let Some(team) = projection.teams.get(&input.team_ref) else {
                return Some(("membership_target_not_found", input.team_ref.clone()));
            };
            if !projection.agents.contains_key(&input.agent_ref) {
                return Some(("membership_target_not_found", input.agent_ref.clone()));
            }
            if matches!(
                input.operation,
                harness_contract::agent_action::MembershipOperation::Join
            ) && team.lifecycle != AgenticTeamLifecycle::Active
            {
                return Some(("team_not_accepting_members", input.team_ref.clone()));
            }
            if matches!(
                envelope.actor.kind,
                AgentActorKind::Root | AgentActorKind::Supervisor
            ) || envelope.actor.agent_id.as_deref() == Some(input.agent_ref.as_str())
            {
                None
            } else {
                Some(("membership_update_not_delegated", input.agent_ref.clone()))
            }
        }
        AgentAction::TeamUpdate(input) => {
            let Some(team) = projection.teams.get(&input.team_ref) else {
                return Some(("team_not_found", input.team_ref.clone()));
            };
            if matches!(
                envelope.actor.kind,
                AgentActorKind::Root | AgentActorKind::Supervisor
            ) || (envelope.actor.kind == AgentActorKind::TeamLead
                && envelope.actor.team_id.as_deref() == Some(team.team_id.as_str()))
            {
                None
            } else {
                Some(("team_update_not_delegated", input.team_ref.clone()))
            }
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

fn validate_task_withdraw(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskWithdrawInput,
) -> Option<(&'static str, String)> {
    let Some(task) = projection.tasks.get(&input.task_ref) else {
        return Some(("task_not_found", input.task_ref.clone()));
    };
    if matches!(
        task.status,
        AgenticTaskStatus::Accepted | AgenticTaskStatus::Superseded | AgenticTaskStatus::Withdrawn
    ) {
        return Some(("task_not_withdrawable", format!("{:?}", task.status)));
    }
    if matches!(
        envelope.actor.kind,
        AgentActorKind::Root | AgentActorKind::Supervisor
    ) || (matches!(
        envelope.actor.kind,
        AgentActorKind::TeamLead | AgentActorKind::Agent
    ) && envelope.actor.team_id.as_deref() == Some(task.team_id.as_str()))
    {
        None
    } else {
        Some(("task_withdraw_not_delegated", task.team_id.clone()))
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
    if projection
        .teams
        .get(&input.team_ref)
        .is_some_and(|team| team.lifecycle != AgenticTeamLifecycle::Active)
    {
        return Some(("team_not_accepting_work", input.team_ref.clone()));
    }
    if matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) && !super::delegation::can_organize_team(projection, envelope, &input.team_ref)
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
    if projection.execution_is_coordinator(envelope.actor.execution_id.as_deref()) {
        return Some(("coordination_cannot_claim_task", input.task_ref.clone()));
    }
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
    if !projection.agents.contains_key(agent_id) {
        return Some(("actor_not_in_roster", agent_id.clone()));
    }
    if !projection.agent_is_active_in(agent_id, &task.team_id) {
        return Some(("task_outside_actor_team", task.team_id.clone()));
    }
    if let Some(execution_id) = envelope.actor.execution_id.as_deref() {
        if projection.declined_task_opportunity(&input.task_ref, agent_id, None, Some(execution_id))
        {
            return Some(("task_opportunity_declined", input.task_ref.clone()));
        }
    }
    if !same_execution_renewal {
        if let Some(active) = projection.tasks.values().find(|candidate| {
            candidate.task_id != input.task_ref
                && candidate.status == AgenticTaskStatus::Claimed
                && candidate.claimant.as_deref() == Some(agent_id.as_str())
                && candidate.claim_execution_id == envelope.actor.execution_id
        }) {
            return Some((
                "execution_already_claims_task",
                format!(
                    "physical Agent execution is already bound to active Task {}",
                    active.task_id
                ),
            ));
        }
    }
    // Roster capability labels are semantic matching hints. Physical Agent
    // definitions, tools, permissions, and resource scopes are bound by the
    // trusted dispatcher; exact model-authored string equality is neither an
    // authorization primitive nor a valid reason to reject an Agent claim.
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
    if let Some(attempt) = envelope
        .actor
        .execution_id
        .as_ref()
        .and_then(|id| task.active_attempts.get(id))
        .filter(|attempt| {
            attempt.mode == harness_contract::agent_action::AgentAttemptMode::Coordination
        })
    {
        if envelope.actor.kind == AgentActorKind::Supervisor {
            return None;
        }
        if actor_agent_id(envelope) != Some(attempt.agent_id.as_str()) {
            return Some(("coordination_actor_mismatch", input.task_ref.clone()));
        }
        let Some((_, wake)) = projection.coordination_wake(attempt.generation) else {
            return Some(("coordination_wake_missing", input.task_ref.clone()));
        };
        if !projection.coordination_replied(
            &wake.entry_id,
            &attempt.execution_id,
            &attempt.agent_id,
        ) {
            return Some(("coordination_reply_required", wake.entry_id.clone()));
        }
        return None;
    }
    if !matches!(
        task.status,
        AgenticTaskStatus::Claimed | AgenticTaskStatus::CancelRequested
    ) {
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
        AgenticTaskStatus::Published
            | AgenticTaskStatus::Rework
            | AgenticTaskStatus::Blocked
            | AgenticTaskStatus::Claimed
            | AgenticTaskStatus::Submitted
    ) {
        return Some(("task_not_supersedable", format!("{:?}", task.status)));
    }
    for replacement_ref in &input.replacement_task_refs {
        let Some(replacement) = projection.tasks.get(replacement_ref) else {
            return Some(("replacement_task_not_found", replacement_ref.clone()));
        };
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
    let registered_attempt = task
        .active_attempts
        .get(&input.execution_id)
        .is_some_and(|attempt| attempt.mode == input.mode);
    match input.mode {
        harness_contract::agent_action::AgentAttemptMode::Coordination => {
            if !registered_attempt {
                return Some(("coordination_attempt_missing", input.execution_id.clone()));
            }
        }
        harness_contract::agent_action::AgentAttemptMode::Execute => {
            if task.status == AgenticTaskStatus::CancelRequested
                && (registered_attempt
                    || task.claim_execution_id.as_deref() == Some(input.execution_id.as_str()))
            {
                return None;
            }
            if task.status == AgenticTaskStatus::Claimed
                && task.claim_execution_id.as_deref() != Some(input.execution_id.as_str())
            {
                return Some(("task_claim_fence_mismatch", input.task_ref.clone()));
            }
            if !matches!(
                task.status,
                AgenticTaskStatus::Published
                    | AgenticTaskStatus::Rework
                    | AgenticTaskStatus::Claimed
            ) {
                return Some(("task_not_claimed", input.task_ref.clone()));
            }
            if task.status != AgenticTaskStatus::Claimed && !registered_attempt {
                return Some(("task_not_claimed", input.task_ref.clone()));
            }
            if !registered_attempt
                && task.claim_execution_id.as_deref() != Some(input.execution_id.as_str())
            {
                return Some(("task_claim_fence_mismatch", input.task_ref.clone()));
            }
        }
        harness_contract::agent_action::AgentAttemptMode::Review => {
            if task.status == AgenticTaskStatus::CancelRequested && registered_attempt {
                return None;
            }
            // Pre-outbox journals can contain a submitted Task with no
            // recorded review graph.  Preserve that deterministic recovery
            // path; newly dispatched reviews always have a matching record.
            if task.status != AgenticTaskStatus::Submitted
                || (!registered_attempt && !task.active_attempts.is_empty())
            {
                return Some(("task_not_submitted", input.task_ref.clone()));
            }
        }
    }
    None
}

fn validate_task_attempt_dispatch(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskAttemptDispatchInput,
) -> Option<(&'static str, String)> {
    if envelope.actor.kind != AgentActorKind::Supervisor {
        return Some((
            "attempt_dispatch_requires_supervisor",
            input.task_ref.clone(),
        ));
    }
    let Some(task) = projection.tasks.get(&input.task_ref) else {
        return Some(("task_not_found", input.task_ref.clone()));
    };
    if !projection.agents.contains_key(&input.agent_ref) {
        return Some(("attempt_agent_not_in_roster", input.agent_ref.clone()));
    }
    let Some(team_id) =
        (if input.mode == harness_contract::agent_action::AgentAttemptMode::Coordination {
            projection.coordination_team_id_for(&input.agent_ref, input.generation)
        } else {
            projection.dispatch_team_id_for(
                &input.agent_ref,
                task,
                input.mode == harness_contract::agent_action::AgentAttemptMode::Review,
            )
        })
    else {
        return Some(("attempt_membership_inactive", input.agent_ref.clone()));
    };
    if input.membership_id != AgenticProgramProjection::membership_id(&input.agent_ref, team_id) {
        return Some(("attempt_membership_mismatch", input.agent_ref.clone()));
    }
    let ready = match input.mode {
        harness_contract::agent_action::AgentAttemptMode::Coordination => projection
            .coordination_wake(input.generation)
            .is_some_and(|(_, wake)| {
                wake.intent
                    .as_ref()
                    .is_some_and(|intent| intent.task_ref == input.task_ref)
                    && projection.coordination_request_current(wake)
                    && wake.coordination.is_none()
            }),
        harness_contract::agent_action::AgentAttemptMode::Execute => {
            matches!(
                task.status,
                AgenticTaskStatus::Published | AgenticTaskStatus::Rework
            )
        }
        harness_contract::agent_action::AgentAttemptMode::Review => {
            task.status == AgenticTaskStatus::Submitted
        }
    };
    if !ready {
        return Some(("task_not_dispatchable", format!("{:?}", task.status)));
    }
    let current_generation = match input.mode {
        harness_contract::agent_action::AgentAttemptMode::Coordination => input.generation,
        harness_contract::agent_action::AgentAttemptMode::Execute => task.claim_generation,
        harness_contract::agent_action::AgentAttemptMode::Review => task.review_generation,
    };
    if input.generation != current_generation {
        return Some(("attempt_generation_stale", input.task_ref.clone()));
    }
    if input.mode == harness_contract::agent_action::AgentAttemptMode::Execute
        && projection.declined_task_opportunity(
            &input.task_ref,
            &input.agent_ref,
            Some(input.generation),
            None,
        )
    {
        return Some(("task_opportunity_declined", input.task_ref.clone()));
    }
    if task
        .active_attempts
        .values()
        .any(|attempt| attempt.mode == input.mode && attempt.generation == input.generation)
    {
        return Some((
            "attempt_generation_already_dispatched",
            input.task_ref.clone(),
        ));
    }
    if projection.tasks.values().any(|candidate| {
        candidate
            .active_attempts
            .values()
            .any(|attempt| attempt.agent_id == input.agent_ref)
            || (candidate.status == AgenticTaskStatus::Claimed
                && candidate.claimant.as_deref() == Some(input.agent_ref.as_str()))
    }) {
        return Some(("agent_has_active_opportunity", input.agent_ref.clone()));
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
                    || artifact.claim_execution_id.as_deref() != task.claim_execution_id.as_deref()
                    || artifact.claim_generation != Some(task.claim_generation)
            })
    }) {
        return Some(("artifact_not_bound_to_active_claim", unowned.clone()));
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
    // The submitted collaboration artifact is already authenticated above by
    // claimant, Task and durable content.  `evidence_refs` are supporting
    // observations (tool receipts, sources, test runs), not a second encoding
    // of the artifact-content selector.  Requiring the model to copy that
    // internal selector into both fields leaks storage topology and turns an
    // otherwise valid submission into a brittle join operation.
    None
}

fn validate_task_review(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskReviewInput,
) -> Option<(&'static str, String)> {
    if projection.execution_is_coordinator(envelope.actor.execution_id.as_deref()) {
        return Some(("coordination_cannot_review_task", input.task_ref.clone()));
    }
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
    if task.claim_execution_id.is_some() && task.claim_execution_id == envelope.actor.execution_id {
        return Some((
            "self_review_forbidden",
            "producer and reviewer share a physical execution".into(),
        ));
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
    // Review authority is bound to a different roster Agent and the submitted
    // artifact set is part of the Task projection.  Supporting evidence is
    // validated at the trusted Gateway boundary; forcing the reviewer to echo
    // a storage selector does not prove inspection and only couples the model
    // contract to ArtifactStore internals.
    None
}
