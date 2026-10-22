use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActorKind, TaskAttemptDispatchInput,
    TaskAttemptFailInput, TaskClaimInput, TaskPublishInput, TaskReleaseInput, TaskReviewDecision,
    TaskReviewInput, TaskSubmitInput, TaskSupersedeInput, TaskWithdrawInput,
};

use crate::agentic::program::{AgenticProgramProjection, AgenticTaskStatus, AgenticTeamLifecycle};

use super::{actor_agent_id, is_durable_evidence_ref};

fn unsupported_evidence_ref(reference: &str) -> (&'static str, String) {
    (
        "unsupported_evidence_ref",
        format!(
            "Evidence must use a durable tool:// or artifact:// observation. Action receipt: IDs are not evidence. Remove the unsupported entry, keep valid observations, and retry this same bound action. Unsupported reference: {reference}"
        ),
    )
}

pub(super) fn validate_transition(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    now_ms: u64,
    graphs: Option<&crate::ExecutionGraphStateStore>,
) -> Option<(&'static str, String)> {
    validate_transition_with_completion_context(projection, envelope, now_ms, graphs, None)
}

pub(super) fn validate_transition_with_completion_context(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    now_ms: u64,
    graphs: Option<&crate::ExecutionGraphStateStore>,
    completion: Option<&crate::agentic::review_evidence::CompletionAdmissionContext>,
) -> Option<(&'static str, String)> {
    validate_transition_with_admission_contexts(
        projection, envelope, now_ms, graphs, completion, None,
    )
}

pub(super) fn validate_transition_with_admission_contexts(
    projection: &AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    now_ms: u64,
    graphs: Option<&crate::ExecutionGraphStateStore>,
    completion: Option<&crate::agentic::review_evidence::CompletionAdmissionContext>,
    retirement: Option<&super::RuntimeRetirementAdmission>,
) -> Option<(&'static str, String)> {
    let retiring = retirement.is_some_and(|context| context.covers(projection, envelope));
    if projection.status == crate::agentic::program::AgenticProgramStatus::CompletionRequested
        && !matches!(envelope.action, AgentAction::StateInspect(_))
        && !retiring
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
        && !retiring
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
            if let Err(reason) = projection.validate_team_quantity_contract() {
                return Some(("team_quantity_unknown", reason));
            }
            // Minimums leave additional Teams free; exact zero forbids the first.
            if let Some(ceiling) = projection.exact_team_count {
                if projection.teams.len() >= usize::from(ceiling) {
                    return Some((
                        "team_ceiling_reached",
                        format!(
                            "the user specified {ceiling} Team(s) and {} already exist",
                            projection.teams.len()
                        ),
                    ));
                }
            }
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
                    crate::agentic::order_contract::ARTIFACT_CONTENT_REF_CONTRACT.to_string(),
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
            // An accepted exception exit is a judgement about the goal, not a
            // completion claim: the verifier still reports the remaining gap,
            // but Runtime records the request as a human-review Partial rather
            // than forcing the model to keep working forever. `apply_locked`
            // has already rejected it while the Program was not quiescent.
            if completion.is_some_and(|context| !context.matches_envelope(envelope)) {
                return Some((
                    "objective_not_verifiable",
                    "completion_context_envelope_mismatch".into(),
                ));
            }
            match crate::agentic::supervision::completion_gap_with_context(
                projection, input, completion,
            ) {
                Some(gap)
                    if input.blocked
                        && crate::agentic::action_service::agentic_program_is_quiescent(
                            projection,
                        ) =>
                {
                    Some((
                        "objective_blocked_for_human_review",
                        format!("the Objective cannot be completed: {gap}"),
                    ))
                }
                other => other.map(|gap| ("objective_not_verifiable", gap)),
            }
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
    // A renewal is free today, so a Task that never advances can be re-claimed
    // forever: run mon7's final task burned fourteen minutes that way. Past the
    // bound the claim is refused with the two actions that actually move it.
    if expired_claim || same_execution_renewal {
        if let Some(bound) = crate::agentic::work_market::claim_renewal_bound() {
            if task.claim_renewal_streak >= bound {
                return Some((
                    "task_reclaim_without_progress",
                    format!(
                        "task {} has been claimed {} times in a row with no new deliverable and no \
                         status change; the artifacts already committed for it are [{}]. Submit a \
                         NEW deliverable, or release the task with task_release so the work can \
                         move - re-claiming it again cannot advance the objective.",
                        input.task_ref,
                        task.claim_renewal_streak,
                        task.artifact_refs.join(", ")
                    ),
                ));
            }
        }
    }
    if !matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) {
        // Name the actor, the rule and the way out. The message used to be a
        // bare status - "task claims require a roster-bound Agent" - and a root
        // model that called `task_claim` directly could not tell from it who was
        // refused or what to do instead: one live run emitted "任务领取因
        // roster-bound Agent 失败" four times in a row and never moved. The
        // refusal twenty lines above already sets the standard - it names the
        // task, the count, the artifacts and the two actions that advance it.
        let kind = format!("{:?}", envelope.actor.kind);
        let actor = &envelope.actor.actor_id;
        let team = task.team_id.as_str();
        return Some((
            "actor_cannot_execute_task",
            format!(
                "task {} cannot be claimed by actor `{actor}` (kind {kind}): a claim may only be \
                 issued by an Agent or TeamLead that is a member of Team `{team}`. You are not \
                 that Actor, so do not issue `task_claim` at all - Runtime admits an executor and \
                 an independent reviewer for a Published Task on its own, from the Team's roster. \
                 Make sure the Team has a free member (`agent_invite` into `{team}`), then leave \
                 the claim to Runtime and continue with `state_inspect` or the next independent \
                 piece of work.",
                input.task_ref
            ),
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
        .map(|reference| unsupported_evidence_ref(reference))
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
        return Some((
            "artifact_not_bound_to_active_claim",
            format!(
                "{unowned}: artifact_refs must contain only your own deliverables bound to this Task and active claim. Peer-produced dependency artifacts are supporting evidence, not your new deliverable. Read their retained content and cite the returned durable tool:// receipts or artifact:// content selectors in evidence_refs; keep depends_on for upstream work. {}",
                super::super::order_contract::TASK_REPORT_SUBMISSION_CONTRACT
            ),
        ));
    }
    // A one-shot deliverable is an equally real Task result: the host has
    // already persisted its non-empty content, so the canonical artifact is
    // synthesized at apply time instead of requiring a separate
    // `artifact_commit` model step.
    let one_shot_deliverable = input
        .deliverable
        .as_ref()
        .and_then(|deliverable| deliverable.resolved_content_ref.as_deref())
        .is_some_and(|content_ref| content_ref.starts_with("artifact://"));
    if input.artifact_refs.is_empty() && !one_shot_deliverable {
        return Some((
            "task_submission_has_no_artifact",
            "Write a non-empty report as normal response text, then use task_submit.deliverable to select it or the latest saved report from this same Task, Agent, and attempt. For file or multipart deliverables, use artifact_commit first and submit its compact artifact reference. Empty or invalid report selections remain rejected.".to_string(),
        ));
    }
    if input.evidence_refs.is_empty() && !one_shot_deliverable {
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
        return Some(unsupported_evidence_ref(reference));
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
    // A rework must name the declared criterion it failed.
    //
    // Nothing validated what a reviewer demanded. One live run's reviewer required
    // "documented domains" the isolated workspace could not supply, and another
    // required a mathematically correct counterexample construction the operator
    // never asked for; both were accepted, and the author was sent back to redo work
    // the operator's own criteria did not require. The rework loop that ended those
    // runs was manufactured here, not by the author.
    //
    // The Task declares `acceptance_checks`, and `objective_review` already carries a
    // `criterion_ref` for exactly this purpose. `task_review` was the one verdict that
    // could not say what it was judging, so a reviewer substituted its own standard
    // for the operator's. An accept needs no anchor - accepting is never an overreach -
    // but a rework sends someone back to work, and that has to be answerable to the
    // declared criteria.
    if input.decision == TaskReviewDecision::Rework && !task.acceptance_checks.is_empty() {
        let anchored = task.acceptance_checks.iter().any(|check| {
            let needle = check.trim();
            // A check is prose, so match on a distinctive run rather than the whole
            // string: a reviewer paraphrases, but it cannot paraphrase away the
            // subject of the criterion.
            needle.len() >= 12 && input.reason.contains(&needle[..12])
                || needle.len() < 12 && input.reason.contains(needle)
        });
        if !anchored {
            let declared = task
                .acceptance_checks
                .iter()
                .map(|check| format!("- {check}"))
                .collect::<Vec<_>>()
                .join("\n");
            return Some((
                "rework_without_declared_criterion",
                format!(
                    "a rework must name the declared acceptance criterion it failed. \
                     This Task declares:\n{declared}\n\
                     Quote the criterion you are rejecting. If your objection is \
                     outside these criteria, it may be recorded in `reason` alongside \
                     an accept as a non-blocking observation - it cannot force the \
                     author to redo work the operator did not ask for."
                ),
            ));
        }
    }
    if let Some(reference) = input
        .evidence_refs
        .iter()
        .find(|reference| !is_durable_evidence_ref(reference))
    {
        return Some(unsupported_evidence_ref(reference));
    }
    // Review authority is bound to a different roster Agent and the submitted
    // artifact set is part of the Task projection.  Supporting evidence is
    // validated at the trusted Gateway boundary; forcing the reviewer to echo
    // a storage selector does not prove inspection and only couples the model
    // contract to ArtifactStore internals.
    None
}

#[cfg(test)]
mod reclaim_discipline_tests {
    use super::*;
    use harness_contract::agent_action::{AgentActorBinding, AgentActorKind, TaskClaimInput};

    #[test]
    fn artifact_content_ref_feedback_is_generic_and_preserves_refusal_662() {
        use harness_contract::agent_action::ArtifactCommitInput;

        let projection = AgenticProgramProjection::empty("program", "objective");
        for content_ref in [
            "message://current",
            "current_message_block:0",
            "current_message_block:999999",
            "file://workspace/result.txt",
            "",
        ] {
            let mut request = envelope("content-ref");
            request.action = AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: content_ref.to_string(),
                kind: "result".to_string(),
                title: "Existing deliverable".to_string(),
                relates_to: Vec::new(),
            });
            let before = projection.clone();
            let (code, detail) = validate_transition(&projection, &request, 10_000, None)
                .expect("an unresolved content selector must remain rejected");
            assert_eq!(code, "content_ref_unresolved");
            assert_eq!(detail, crate::agentic::order_contract::ARTIFACT_CONTENT_REF_CONTRACT);
            for required in [
                "message://current",
                "non-empty normal Text in this same model response",
                "current_message_block:<zero-based-index>",
                "real non-empty Text block",
                "out-of-range block stays unresolved and rejected",
                "artifact_publish",
                "artifact://",
                "not an implicit file read",
                "Never select tool parameters or hidden reasoning",
                "does not require creating a Task report",
            ] {
                assert!(detail.contains(required), "{required}: {detail}");
            }
            assert!(!detail.contains("For an ordinary Task report"));
            assert!(!detail.contains("task_submit.deliverable"));
            assert_eq!(projection, before);
        }
    }

    #[test]
    fn artifact_content_ref_feedback_does_not_change_durable_selector_gate_662() {
        use harness_contract::agent_action::ArtifactCommitInput;

        let projection = AgenticProgramProjection::empty("program", "objective");
        let mut request = envelope("content-ref");
        request.action = AgentAction::ArtifactCommit(ArtifactCommitInput {
            content_ref: "artifact://retained-content".to_string(),
            kind: "result".to_string(),
            title: "Existing deliverable".to_string(),
            relates_to: Vec::new(),
        });
        assert!(validate_transition(&projection, &request, 10_000, None).is_none());
        assert!(crate::agentic::order_contract::TASK_REPORT_SUBMISSION_CONTRACT
            .starts_with("For an ordinary Task report"));
    }


    fn envelope(execution_id: &str) -> AgentActionEnvelope {
        AgentActionEnvelope {
            action_id: format!("claim:{}", execution_id),
            actor: AgentActorBinding {
                objective_id: "objective".to_string(),
                program_id: "program".to_string(),
                session_id: "session".to_string(),
                turn_id: "turn".to_string(),
                root_execution_id: Some("root".to_string()),
                required_team_count: 1,
                exact_team_count: None,
                objective_summary: "finish".to_string(),
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
    fn projection_with_streak(streak: u32) -> AgenticProgramProjection {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.agents.insert(
            "agent".to_string(),
            crate::agentic::program::AgentMemberProjection {
                agent_id: "agent".to_string(),
                display_name: String::new(),
                role: "worker".to_string(),
                mission: String::new(),
                required_capabilities: Vec::new(),
                invited_by: "root".to_string(),
                membership_ids: Vec::new(),
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            },
        );
        projection.tasks.insert(
            "task".to_string(),
            crate::agentic::program::AgenticTaskProjection {
                task_id: "task".to_string(),
                team_id: "team".to_string(),
                title: "final synthesis".to_string(),
                objective: "finish".to_string(),
                acceptance: "durable evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                required_capabilities: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Claimed,
                claimant: Some("agent".to_string()),
                claim_generation: 1,
                claim_execution_id: Some("exec-1".to_string()),
                claimed_at_ms: Some(1_000),
                lease_expires_at_ms: Some(1_000),
                claim_renewal_streak: streak,
                active_attempts: Default::default(),
                artifact_refs: vec!["artifact:f6ef9d7b4065b9aaa56ad6b7".to_string()],
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
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
        projection
    }
    #[test]
    fn runtime_retirement_requires_owner_admission_not_a_supervisor_label() {
        use crate::agentic::program::{AgenticProgramStatus, AgenticTaskRetirement};
        let mut request = envelope("exec-1");
        request.action = AgentAction::TaskAttemptFail(TaskAttemptFailInput {
            task_ref: "task".into(),
            execution_id: "exec-1".into(),
            mode: harness_contract::agent_action::AgentAttemptMode::Execute,
            reason: "physical callback".into(),
            retryable: true,
        });
        let mut projection = projection_with_streak(0);
        let task = projection.tasks.get_mut("task").unwrap();
        task.status = AgenticTaskStatus::CancelRequested;
        task.pending_retirement = Some(AgenticTaskRetirement::Withdrawn);
        for status in [
            AgenticProgramStatus::CompletionRequested,
            AgenticProgramStatus::Verified,
        ] {
            projection.status = status;
            for kind in [
                AgentActorKind::Root,
                AgentActorKind::Agent,
                AgentActorKind::Supervisor,
            ] {
                request.actor.kind = kind;
                request.actor.actor_id = "runtime.program-supervisor".into();
                assert!(validate_transition(&projection, &request, 10_000, None).is_some());
                assert!(validate_transition_with_admission_contexts(
                    &projection,
                    &request,
                    10_000,
                    None,
                    None,
                    None
                )
                .is_some());
            }
        }
    }

    #[test]
    fn runtime_retirement_admission_is_exact_request_and_program_only() {
        let mut request = envelope("exec-1");
        request.actor.kind = AgentActorKind::Supervisor;
        request.actor.actor_id = "runtime.program-supervisor".into();
        request.actor.agent_id = None;
        request.actor.team_id = None;
        request.action = AgentAction::TaskAttemptFail(TaskAttemptFailInput {
            task_ref: "task".into(),
            execution_id: "exec-1".into(),
            mode: harness_contract::agent_action::AgentAttemptMode::Execute,
            reason: "physical callback".into(),
            retryable: true,
        });
        let mut projection = projection_with_streak(0);
        let context = super::super::RuntimeRetirementAdmission {
            request_digest: crate::agentic::review_evidence::proof_value_digest(&request),
            program_id: projection.program_id.clone(),
            program_revision: projection.revision,
            sources: Vec::new(),
        };
        assert!(context.covers(&projection, &request));
        let mut foreign = request.clone();
        foreign.actor.execution_id = Some("other-physical-graph".into());
        assert!(!context.covers(&projection, &foreign));
        foreign = request.clone();
        foreign.action = AgentAction::TaskClaim(claim_input());
        assert!(!context.covers(&projection, &foreign));
        projection.revision += 1;
        assert!(!context.covers(&projection, &request));
        projection.revision -= 1;
        projection.program_id = "foreign".into();
        assert!(!context.covers(&projection, &request));
    }
    #[test]
    fn a_reclaim_without_progress_is_refused_past_the_bound() {
        let projection = projection_with_streak(3);
        let refusal = validate_task_claim(&projection, &envelope("exec-1"), &claim_input(), 10_000);
        let (code, detail) = refusal.expect("the re-claim must be refused");
        assert_eq!(code, "task_reclaim_without_progress");
        assert!(
            detail.contains("artifact:f6ef9d7b4065b9aaa56ad6b7"),
            "{detail}"
        );
        assert!(detail.contains("task_release"), "{detail}");
    }
    #[test]
    fn a_reclaim_below_the_bound_still_passes() {
        let projection = projection_with_streak(2);
        let refusal = validate_task_claim(&projection, &envelope("exec-1"), &claim_input(), 10_000);
        assert!(
            refusal
                .as_ref()
                .map_or(true, |(c, _)| *c != "task_reclaim_without_progress"),
            "{refusal:?}"
        );
    }
    fn claim_input() -> TaskClaimInput {
        TaskClaimInput {
            task_ref: "task".to_string(),
            reason: None,
        }
    }

    /// A root that claims a Task must be told who it is, what the rule is, and
    /// what to do instead.
    ///
    /// The message used to be "task claims require a roster-bound Agent" and
    /// nothing else. One live run read it as an unexplained failure and emitted
    /// "任务领取因 roster-bound Agent 失败" four times in a row without ever
    /// changing its approach - a status, not a way out.
    #[test]
    fn a_root_claim_is_refused_with_the_actor_the_rule_and_the_way_out() {
        let mut root = envelope("exec-root");
        root.actor.kind = AgentActorKind::Root;
        root.actor.actor_id = "root:session".to_string();
        root.actor.agent_id = None;
        root.actor.team_id = None;
        let refusal =
            validate_task_claim(&projection_with_streak(0), &root, &claim_input(), 10_000);
        let (code, detail) = refusal.expect("a root claim must be refused");
        assert_eq!(code, "actor_cannot_execute_task");
        assert!(detail.contains("root:session"), "who was refused: {detail}");
        assert!(detail.contains("Root"), "what kind of actor: {detail}");
        assert!(
            detail.contains("team"),
            "which Team owns the Task: {detail}"
        );
        assert!(
            detail.contains("agent_invite"),
            "the reachable next step: {detail}"
        );
        assert!(
            detail.contains("Runtime admits"),
            "the model must know the claim is not its job: {detail}"
        );
    }
}
