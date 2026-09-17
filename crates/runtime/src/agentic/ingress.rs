//! Trusted Agent-action ingress.
//!
//! Gateway, native agents and Process bridges may parse model-facing action
//! DTOs, but only this Runtime boundary verifies that references are readable
//! durable evidence before the Program journal is mutated.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActionObservation, AgentActionStatus, AgentActorBinding,
    AgentActorKind, AgentAttemptMode, TaskAttemptFailInput,
};

use crate::RuntimeServices;

/// A positive review verdict is rejected as "source changed; retry" while the
/// underlying effect/strategy revision is being rewritten by concurrent
/// writers. That is optimistic-concurrency, not a business failure, so the
/// model is asked to retry. But a *wedged* active review graph can be rejected
/// with the same retry class forever: the rejection never reaches
/// `TaskAttemptFail`, so `failed_review_attempts` never increments and the
/// bounded review-attempt terminal (G56/G57 reviewed-graph stall) is never
/// reached.
///
/// This streak is process-local liveness bookkeeping keyed by
/// `(program, task/criterion, reviewing execution)`. After
/// `MAX_REVIEW_SOURCE_CHANGED_RETRIES` same-class rejections on one attempt the
/// Runtime settles a durable `TaskAttemptFail`, which the existing
/// `MAX_TASK_REVIEW_ATTEMPTS` bound turns into `Blocked` and lets the root
/// converge with an explicit terminal instead of waiting forever.
const MAX_REVIEW_SOURCE_CHANGED_RETRIES: u32 = 3;

static REVIEW_SOURCE_CHANGED_STREAK: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Retry-class review rejection caused by a concurrently-changing source. These
/// strings are authored by Runtime (`review_evidence/policy.rs`), never by a
/// provider or model.
fn is_review_source_changed_retry(error: &str) -> bool {
    (error.contains("review policy source changed")
        || error.contains("effect review source changed")
        || error.contains("delegated effect source changed"))
        && error.contains("retry")
}

fn review_attempt_key(envelope: &AgentActionEnvelope) -> String {
    let focus = match &envelope.action {
        AgentAction::TaskReview(input) => input.task_ref.clone(),
        AgentAction::ObjectiveReview(input) => input.criterion_ref.clone(),
        _ => String::new(),
    };
    format!(
        "{}:{}:{}",
        envelope.actor.program_id,
        focus,
        envelope.actor.execution_id.as_deref().unwrap_or("root")
    )
}

fn bump_review_streak(key: &str) -> u32 {
    let mut streaks = REVIEW_SOURCE_CHANGED_STREAK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = streaks.entry(key.to_string()).or_insert(0);
    *entry = entry.saturating_add(1);
    *entry
}

fn clear_review_streak(key: &str) {
    REVIEW_SOURCE_CHANGED_STREAK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(key);
}

/// Supervisor actor for a Runtime-authored attempt settlement. Mirrors the
/// actor the physical dispatch path uses so the durable journal records the
/// settlement as Runtime-owned.
fn runtime_supervisor_actor(
    program: &crate::AgenticProgramProjection,
    execution_id: Option<String>,
) -> AgentActorBinding {
    AgentActorBinding {
        objective_id: program.objective_id.clone(),
        program_id: program.program_id.clone(),
        session_id: program.session_id.clone(),
        turn_id: program.turn_id.clone(),
        root_execution_id: program.root_execution_id.clone(),
        required_team_count: program.required_team_count,
        objective_summary: program.objective_summary.clone(),
        model_lease: program.model_lease.clone(),
        permission_ceiling: Some(program.permission_ceiling),
        resource_scopes: program.resource_scopes.clone(),
        actor_id: "runtime.program-supervisor".to_string(),
        kind: AgentActorKind::Supervisor,
        execution_id,
        team_id: None,
        agent_id: None,
    }
}

impl RuntimeServices {
    /// Validate evidence selectors in the authenticated actor scope. This is
    /// intentionally an async preflight: storage reads happen before the
    /// ActionService acquires the Program stream lock.
    pub async fn validate_agent_action_evidence(
        &self,
        actor: &harness_contract::agent_action::AgentActorBinding,
        action: &AgentAction,
    ) -> Result<(), String> {
        // All content selectors that enter a durable action use the same
        // authenticated readability check.  Unknown logical IDs (Task/Team
        // IDs, capability IDs, etc.) deliberately remain the reducer's
        // responsibility; only content-shaped refs can reach ArtifactStore.
        let mut evidence_refs = Vec::<&String>::new();
        match action {
            AgentAction::TaskSubmit(input) => {
                evidence_refs.extend(&input.artifact_refs);
                evidence_refs.extend(&input.evidence_refs);
            }
            AgentAction::TaskReview(input) => evidence_refs.extend(&input.evidence_refs),
            AgentAction::TaskSupersede(input) => evidence_refs.extend(&input.evidence_refs),
            AgentAction::TaskWithdraw(input) => {
                evidence_refs.push(&input.reason_ref);
                evidence_refs.extend(&input.evidence_refs);
            }
            AgentAction::ObjectiveUpdate(input) => {
                if let Some(statement_ref) = &input.statement_ref {
                    evidence_refs.push(statement_ref);
                }
                evidence_refs.extend(&input.source_refs);
                if let Some(reason_ref) = &input.reason_ref {
                    evidence_refs.push(reason_ref);
                }
            }
            AgentAction::ObjectiveReview(input) => {
                evidence_refs.extend(&input.result_refs);
                evidence_refs.extend(&input.evidence_refs);
                evidence_refs.push(&input.reason_ref);
            }
            AgentAction::MembershipUpdate(input) => {
                if let Some(reason_ref) = &input.reason_ref {
                    evidence_refs.push(reason_ref);
                }
            }
            AgentAction::TeamUpdate(input) => {
                if let Some(mission_ref) = &input.mission_ref {
                    evidence_refs.push(mission_ref);
                }
                if let Some(reason_ref) = &input.reason_ref {
                    evidence_refs.push(reason_ref);
                }
            }
            AgentAction::MessagePublish(input) => {
                for disposition in &input.issue_dispositions {
                    evidence_refs.push(&disposition.reason_ref);
                    evidence_refs.extend(&disposition.evidence_refs);
                }
                if let Some(content_ref) = &input.content_ref {
                    evidence_refs.push(content_ref);
                }
                evidence_refs.extend(&input.refs);
                if let Some(intent) = &input.intent {
                    if let Some(reason_ref) = &intent.reason_ref {
                        evidence_refs.push(reason_ref);
                    }
                }
            }
            AgentAction::ObjectiveCompleteRequest(input) => {
                evidence_refs.extend(&input.result_refs);
                evidence_refs.extend(&input.evidence_refs);
            }
            _ => {}
        }
        for evidence_ref in evidence_refs {
            let artifact = if let Some(evidence_id) = evidence_ref.strip_prefix("tool://") {
                let access = self
                    .session_evidence_access(&actor.session_id, evidence_id)
                    .await
                    .map_err(|error| format!(
                        "{} could not resolve evidence {evidence_ref} through the authenticated Session journal: {error}",
                        action.kind()
                    ))?
                    .ok_or_else(|| format!(
                        "{} evidence {evidence_ref} has no canonical durable receipt in Session {}; use a tool:// reference returned by a completed tool call in this Session",
                        action.kind(), actor.session_id
                    ))?;
                let artifact = self
                    .artifact_store()
                    .resolve(&access.retrieval_selector)
                    .map_err(|error| {
                        format!(
                            "{} evidence {evidence_ref} points to missing durable content: {error}",
                            action.kind()
                        )
                    })?;
                if artifact.sha256 != access.sha256
                    || artifact.bytes != access.bytes
                    || artifact.media_type != access.media_type
                    || artifact.visibility_scope != access.visibility_scope
                {
                    return Err(format!(
                        "{} evidence {evidence_ref} failed durable receipt integrity validation",
                        action.kind()
                    ));
                }
                artifact
            } else if evidence_ref.starts_with("artifact://") {
                self.artifact_store()
                    .resolve(evidence_ref)
                    .map_err(|error| {
                        format!(
                            "{} evidence {evidence_ref} points to missing durable content: {error}",
                            action.kind()
                        )
                    })?
            } else {
                // Non-content references are validated by their individual
                // action reducers; do not invent a second registry here.
                continue;
            };
            let session_scope = format!("session:{}", actor.session_id);
            if artifact.visibility_scope != "public"
                && artifact.visibility_scope != session_scope
                && !actor
                    .resource_scopes
                    .iter()
                    .any(|scope| scope == &artifact.visibility_scope)
            {
                return Err(format!(
                    "{} evidence {evidence_ref} is not readable in Session {}",
                    action.kind(),
                    actor.session_id
                ));
            }
            self.artifact_store()
                .read(
                    &artifact,
                    &artifact.visibility_scope,
                    Some(0..artifact.bytes.min(1)),
                )
                .await
                .map_err(|error| {
                    format!(
                        "{} evidence {evidence_ref} is not readable: {error}",
                        action.kind()
                    )
                })?;
        }
        Ok(())
    }

    /// The sole public Program mutation ingress. actor is a trusted Runtime
    /// binding constructed from Session/graph/run lineage, never a model JSON
    /// object. Gateway only transports its DTO to this boundary.
    pub async fn submit_agent_action(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, String> {
        envelope.validate().map_err(|error| error.to_string())?;
        let actions = self.agent_action_service();
        if let Some(replay) = actions
            .replay_if_applied(envelope)
            .map_err(|error| error.to_string())?
        {
            return Ok(replay);
        }
        self.validate_agent_action_evidence(&envelope.actor, &envelope.action)
            .await?;
        let positive_review = matches!(&envelope.action,
            AgentAction::ObjectiveReview(input) if input.decision == harness_contract::agent_action::ObjectiveReviewDecision::Satisfied)
            || matches!(&envelope.action, AgentAction::TaskReview(input)
                if input.decision == harness_contract::agent_action::TaskReviewDecision::Accept);
        let review = if positive_review {
            // Resolve both the result set and its manifest from one projection.
            let program = actions
                .project(&envelope.actor.program_id)
                .map_err(|error| error.to_string())?;
            let results = match &envelope.action {
                AgentAction::ObjectiveReview(input) => &input.result_refs,
                AgentAction::TaskReview(input) => {
                    &program
                        .tasks
                        .get(&input.task_ref)
                        .ok_or("review Task not found")?
                        .artifact_refs
                }
                _ => unreachable!("positive review classification"),
            };
            let key = review_attempt_key(envelope);
            match self
                .independent_result_review(
                    &envelope.actor,
                    &program,
                    results,
                    match &envelope.action {
                        AgentAction::ObjectiveReview(input) => Some(input.criterion_ref.as_str()),
                        _ => None,
                    },
                )
                .await
            {
                Ok(review) => {
                    clear_review_streak(&key);
                    Some(review)
                }
                Err(error) => {
                    if is_review_source_changed_retry(&error) {
                        let streak = bump_review_streak(&key);
                        if streak >= MAX_REVIEW_SOURCE_CHANGED_RETRIES {
                            clear_review_streak(&key);
                            // The active review graph is wedged on a retry-class
                            // rejection that never becomes a durable failure.
                            // Settle a durable TaskAttemptFail so the bounded
                            // review-attempt policy can close the Task.
                            let settlement = self
                                .settle_wedged_review_attempt(envelope, &program, &error)
                                .await;
                            return match settlement {
                                Ok(()) => Err(format!(
                                    "review attempt settled after {streak} source-changed rejections: {error}"
                                )),
                                Err(settle_error) => Err(format!(
                                    "{error}; runtime review-attempt settlement failed: {settle_error}"
                                )),
                            };
                        }
                    } else {
                        clear_review_streak(&key);
                    }
                    return Err(error);
                }
            }
        } else {
            None
        };
        let verification = review.as_ref().map(|review| &review.verification);
        if matches!(
            envelope.action,
            AgentAction::ObjectiveUpdate(_) | AgentAction::ObjectiveReview(_)
        ) {
            let producers = review
                .as_ref()
                .map_or(&[][..], |review| review.producers.as_slice());
            let prepared = self.goal_store().prepare_agentic_objective_action(
                envelope,
                producers,
                verification,
            )?;
            actions
                .apply_with_goal_event(
                    envelope,
                    prepared.stream_id,
                    prepared.expected_stream_revision,
                    prepared.event,
                    verification,
                )
                .map_err(|error| error.to_string())
        } else if let Some(verification) = verification {
            actions
                .apply_verified_review(envelope, verification)
                .map_err(|error| error.to_string())
        } else {
            actions.apply(envelope).map_err(|error| error.to_string())
        }
    }

    /// Settle the exact reviewed Task as a durable review-attempt failure so the
    /// bounded `MAX_TASK_REVIEW_ATTEMPTS` policy can reach its terminal. Only
    /// delegated Task review is settled; objective review is not a Task attempt.
    async fn settle_wedged_review_attempt(
        &self,
        envelope: &AgentActionEnvelope,
        program: &crate::AgenticProgramProjection,
        reason: &str,
    ) -> Result<(), String> {
        let AgentAction::TaskReview(input) = &envelope.action else {
            return Ok(());
        };
        let execution_id = envelope
            .actor
            .execution_id
            .clone()
            .ok_or("review attempt settlement requires the reviewing execution id")?;
        let fail = AgentActionEnvelope {
            action_id: format!(
                "runtime-wedged-review-settle:{}:{}:{}",
                program.program_id, input.task_ref, execution_id
            ),
            actor: runtime_supervisor_actor(program, Some(execution_id.clone())),
            expected_revision: None,
            action: AgentAction::TaskAttemptFail(TaskAttemptFailInput {
                task_ref: input.task_ref.clone(),
                execution_id,
                mode: AgentAttemptMode::Review,
                reason: reason.to_string(),
                retryable: true,
            }),
        };
        // Break the static recursion between submit_agent_action -> settle ->
        // submit_agent_action (the TaskAttemptFail action never re-enters the
        // positive-review branch, so this cannot loop at runtime).
        let observation = Box::pin(self.submit_agent_action(&fail)).await?;
        if observation.status != AgentActionStatus::Applied {
            return Err(observation
                .error
                .map(|error| error.code)
                .unwrap_or_else(|| "unknown".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn review_source_changed_retry_matches_only_runtime_retry_messages() {
        assert!(is_review_source_changed_retry(
            "effect review source changed; retry current effects"
        ));
        assert!(is_review_source_changed_retry(
            "delegated effect source changed; retry current effects"
        ));
        assert!(is_review_source_changed_retry(
            "review policy source changed; retry against current strategy and tool plans"
        ));
        // Unrelated rejections must not be treated as the wedged retry class.
        assert!(!is_review_source_changed_retry("review Task not found"));
        assert!(!is_review_source_changed_retry(
            "Artifact catalog source changed; start a fresh discovery"
        ));
    }

    #[test]
    fn review_source_changed_streak_reaches_the_bound() {
        let key = "program:task:execution-ingress-test";
        clear_review_streak(key);
        for expected in 1..=MAX_REVIEW_SOURCE_CHANGED_RETRIES {
            assert_eq!(bump_review_streak(key), expected);
        }
        clear_review_streak(key);
        // The streak restarts after a settlement or a clean verdict.
        assert_eq!(bump_review_streak(key), 1);
        clear_review_streak(key);
    }
}
