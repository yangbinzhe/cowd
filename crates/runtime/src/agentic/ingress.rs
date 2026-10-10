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
    // A reviewer that has not opened the submitted body is a retry class too.
    // Bounding it prevents an artifact whose body cannot be read from looping
    // forever; after the bound the attempt settles and the Task can be
    // reworked or reassigned to a reviewer that reads it.
    if error.contains("review_requires_current_physical_reads") {
        return true;
    }
    // A review attempt whose generation no longer matches an already-reviewed or
    // reworked Task is stale; bound it the same way so it settles instead of
    // looping, and a fresh attempt can be dispatched.
    if error.contains("review physical attempt is stale") {
        return true;
    }
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
        exact_team_count: None,
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
        // Every unresolvable content ref is reported together with the
        // canonical receipt Runtime recorded for its tool call. A model can
        // corrupt the opaque content suffix of an otherwise real call id (one
        // live run cited `tool-raw-...-6` for the durable
        // `tool-raw-...-2c77431438342406`); refusing without naming the
        // canonical id leaves it no recovery path, and the repetition guard
        // then refuses its retries, so the Task can never be submitted.
        let mut unresolvable: Vec<String> = Vec::new();
        for evidence_ref in evidence_refs {
            let artifact = if let Some(evidence_id) = evidence_ref.strip_prefix("tool://") {
                // A model cites the action-id shape it was handed
                // (`tool-raw-<call_id>:<attempt>`); durable evidence ids never
                // carry a colon. The attempt index cannot select different
                // content - the call id alone names the one recorded receipt -
                // so resolve that shape instead of refusing it. One live run
                // lost its whole review Task to exactly this shape mismatch.
                let mut resolved = self
                    .session_evidence_access(&actor.session_id, evidence_id)
                    .await;
                if matches!(resolved, Ok(None)) {
                    let attempt_stripped = evidence_id
                        .rsplit_once(':')
                        .filter(|(_, suffix)| {
                            !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit())
                        })
                        .map(|(stem, _)| stem.to_string());
                    if let Some(stem) = attempt_stripped {
                        let stripped = self.session_evidence_access(&actor.session_id, &stem).await;
                        if stripped.is_ok() {
                            resolved = stripped;
                        }
                    }
                }
                let access = match resolved {
                    Ok(Some(access)) => access,
                    Ok(None) => {
                        let hint = self
                            .canonical_tool_evidence_hint(&actor.session_id, evidence_id)
                            .await;
                        let mut message = format!(
                            "{} evidence {evidence_ref} has no canonical durable receipt in Session {}; use a tool:// reference returned by a completed tool call in this Session",
                            action.kind(),
                            actor.session_id
                        );
                        if let Some(canonical) = hint {
                            message.push_str(&format!(
                                ". The durable receipt Runtime recorded for this tool call is tool://{canonical}; copy that exact reference"
                            ));
                        }
                        unresolvable.push(message);
                        continue;
                    }
                    Err(error) => {
                        return Err(format!(
                            "{} could not resolve evidence {evidence_ref} through the authenticated Session journal: {error}",
                            action.kind()
                        ));
                    }
                };
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
                // Runtime spells a one-shot Task deliverable `artifact:<24 hex>`.
                // A model that adds the separator itself produces a selector the
                // ArtifactStore never issued, and this validation then refuses a
                // perfectly good reference. The Program projection holds the
                // mapping from that logical key to the real selector.
                match self.artifact_store().resolve(evidence_ref) {
                    Ok(artifact) => artifact,
                    Err(store_error) => {
                        let logical = format!(
                            "artifact:{}",
                            evidence_ref.trim_start_matches("artifact://")
                        );
                        let mapped = self
                            .agent_action_service()
                            .project_if_exists(&actor.program_id)
                            .ok()
                            .flatten()
                            .and_then(|program| {
                                program
                                    .artifacts
                                    .get(&logical)
                                    .map(|artifact| artifact.content_ref.clone())
                            });
                        match mapped {
                            Some(content_ref) => self.artifact_store().resolve(&content_ref).map_err(
                                |error| {
                                    format!(
                                        "{} evidence {evidence_ref} points to missing durable content: {error}",
                                        action.kind()
                                    )
                                },
                            )?,
                            None => {
                                return Err(format!(
                                    "{} evidence {evidence_ref} points to missing durable content: {store_error}",
                                    action.kind()
                                ))
                            }
                        }
                    }
                }
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
        if !unresolvable.is_empty() {
            return Err(unresolvable.join("; "));
        }
        Ok(())
    }

    /// The canonical durable receipt id for a tool call whose cited content
    /// suffix is corrupted. The stable call-id prefix still selects exactly
    /// one recorded receipt (an ambiguous prefix resolves to nothing), so the
    /// hint can only hand back what Runtime durably recorded - it can never
    /// invent an id, and the caller's action is still rejected.
    async fn canonical_tool_evidence_hint(
        &self,
        session_id: &str,
        evidence_id: &str,
    ) -> Option<String> {
        let (call_id, _suffix) = evidence_id.rsplit_once('-')?;
        if call_id.is_empty() {
            return None;
        }
        let access = self
            .session_evidence_access(session_id, call_id)
            .await
            .ok()
            .flatten()?;
        let canonical = access.evidence_ref.id;
        (canonical != evidence_id).then_some(canonical)
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
        // The physical-read guard must cover every verdict, not only accept:
        // otherwise a reviewer can return rework from metadata alone, never open
        // the submitted body, and the Task can never be independently accepted.
        let is_review = positive_review || matches!(&envelope.action, AgentAction::TaskReview(_));
        let review = if is_review {
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
                _ => unreachable!("review classification"),
            };
            if matches!(
                &envelope.action,
                AgentAction::ObjectiveReview(input)
                    if input.decision == harness_contract::agent_action::ObjectiveReviewDecision::Satisfied
            ) {
                validate_current_objective_review_scope(&program, results)?;
            }
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
                    // Only a positive verdict carries the verification forward;
                    // a negative verdict already satisfied the physical-read
                    // guard above and applies as before.
                    positive_review.then_some(review)
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
                                    "review attempt settled after {streak} retryable rejections: {error}"
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

    /// When the Agent-first collaboration is objectively complete but the root
    /// never requested completion (a weak model can finish its final answer
    /// without emitting the action), submit the mechanical completion request on
    /// the root's behalf so the existing durable reconciliation produces the
    /// Objective terminal.
    ///
    /// Safety: the actor is built only from Program lineage (never model JSON),
    /// the action still crosses `submit_agent_action` authorization, and nothing
    /// is submitted unless `completion_gap` confirms the objective is genuinely
    /// completable — a real gap is never masked. Returns `true` only when a
    /// request is durably applied or an existing one was driven to reconcile.
    pub async fn drive_agentic_objective_completion(
        &self,
        session_id: &str,
        turn_id: &str,
        root_execution_id: &str,
    ) -> Result<bool, String> {
        let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
        let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
        let Some(program) = self
            .agent_action_service()
            .project_if_exists(&program_id)
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        // Only the Program that owns this exact root terminal is eligible.
        if program.objective_id != objective_id
            || program.session_id != session_id
            || program.turn_id != turn_id
            || program.root_execution_id.as_deref() != Some(root_execution_id)
            || !matches!(
                program.status,
                crate::AgenticProgramStatus::Open
                    | crate::AgenticProgramStatus::CompletionRequested
            )
        {
            return Ok(false);
        }
        // A request already exists: drive reconciliation rather than overwriting
        // the single completion-request slot.
        if program.completion_request.is_some() {
            self.reconcile_agentic_completion_request(&program_id)?;
            return Ok(true);
        }
        // Cite exactly the accepted work; an empty result set is not completable.
        let Some(input) = completion_objective_input(&program) else {
            return Ok(false);
        };
        // The single authoritative gate: never force an incomplete objective.
        if let Some(gap) = crate::agentic::supervision::completion_gap(&program, &input) {
            tracing::info!(
                program_id = %program_id,
                accepted_tasks = program
                    .tasks
                    .values()
                    .filter(|task| task.status == crate::AgenticTaskStatus::Accepted)
                    .count(),
                result_refs = input.result_refs.len(),
                gap = %gap,
                "drive_agentic_objective_completion declined: objective not objectively complete"
            );
            return Ok(false);
        }
        let actor = harness_contract::agent_action::AgentActorBinding {
            objective_id: program.objective_id.clone(),
            program_id: program.program_id.clone(),
            session_id: program.session_id.clone(),
            turn_id: program.turn_id.clone(),
            root_execution_id: program.root_execution_id.clone(),
            required_team_count: program.required_team_count,
            exact_team_count: None,
            objective_summary: program.objective_summary.clone(),
            model_lease: program.model_lease.clone(),
            permission_ceiling: Some(program.permission_ceiling),
            resource_scopes: program.resource_scopes.clone(),
            actor_id: format!("root:{session_id}"),
            kind: harness_contract::agent_action::AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        let envelope = AgentActionEnvelope {
            action_id: format!("objective-complete-request:runtime:{}", program.revision),
            actor,
            expected_revision: None,
            action: AgentAction::ObjectiveCompleteRequest(input),
        };
        let observation = self.submit_agent_action(&envelope).await?;
        if observation.status != harness_contract::agent_action::AgentActionStatus::Applied {
            return Ok(false);
        }
        // Reduce the request now so the deterministic Objective verdict is
        // durable before the caller presents; the durable lane would also do
        // this, but doing it here removes an asynchronous window.
        self.reconcile_agentic_completion_request(&program_id)?;
        Ok(true)
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

/// Build the Objective completion input a root would cite: every accepted
/// Task's durable artifacts and evidence, with the most cross-Team-integrated
/// artifact first (the completion gap treats the first known artifact as the
/// primary integration proof). Returns `None` when no accepted artifact exists,
/// because an empty result set is not completable. Shared by the completion
/// drive and the completion-gap query so their judgement cannot drift.
fn completion_objective_input(
    program: &crate::AgenticProgramProjection,
) -> Option<harness_contract::agent_action::ObjectiveCompleteRequestInput> {
    let mut result_refs = Vec::new();
    let mut evidence_refs = Vec::new();
    for task in program.tasks.values() {
        if task.status != crate::AgenticTaskStatus::Accepted {
            continue;
        }
        result_refs.extend(task.artifact_refs.iter().cloned());
        evidence_refs.extend(task.evidence_refs.iter().cloned());
    }
    result_refs.sort_by_key(|reference| {
        std::cmp::Reverse(objective_artifact_integration_score(program, reference))
    });
    result_refs.dedup();
    evidence_refs.sort();
    evidence_refs.dedup();
    if result_refs.is_empty() {
        return None;
    }
    Some(
        harness_contract::agent_action::ObjectiveCompleteRequestInput {
            result_refs,
            evidence_refs,
            unresolved: Vec::new(),
            blocked: false,
            blocked_reason: None,
            human_review_required: false,
            recommended_next_actions: Vec::new(),
        },
    )
}

/// The completion gap that currently blocks an Objective, if any. Read-only:
/// used to decide whether a root's success presentation must be returned to
/// continue working instead of failing the turn.
pub fn agentic_program_completion_gap(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
) -> Option<String> {
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    let program = services
        .agent_action_service()
        .project_if_exists(&program_id)
        .ok()
        .flatten()?;
    let root_execution_id = program.root_execution_id.as_deref()?;
    let goal_id = format!("goal:{root_execution_id}");
    if let Some(goal) = services.goal_store().projection(&goal_id).ok().flatten() {
        let bound_to_turn = goal.goal.execution_binding.as_ref().is_some_and(|binding| {
            binding.objective_id == objective_id
                && binding.agentic_program_id == program.program_id
                && binding.session_id == session_id
                && binding.turn_id == turn_id
                && binding.root_execution_id == root_execution_id
        });
        if bound_to_turn && goal.goal.completion == harness_contract::goal::GoalCompletion::Open {
            let current_reviews_satisfied = current_reviews_satisfy_the_goal(&goal.goal);
            let uncovered_result_refs = uncovered_delivered_result_refs(&program, &goal.goal);
            if let Some(gap) = objective_completion_waiting_gap(
                goal.goal.waiting.as_ref(),
                current_reviews_satisfied,
                &uncovered_result_refs,
            ) {
                return Some(gap);
            }
        }
    }
    if program.status != crate::AgenticProgramStatus::Open {
        return None;
    }
    let input = completion_objective_input(&program)?;
    crate::agentic::supervision::completion_gap(&program, &input)
}

/// Whether the Goal's own records say every current criterion is semantically
/// reviewed and satisfied. Extracted so the Program projection and the ingress
/// gap path cannot disagree about what "satisfied" means.
pub(crate) fn current_reviews_satisfy_the_goal(
    goal: &harness_contract::goal::GoalContract,
) -> bool {
    goal.blockers.is_empty()
        && goal.unresolved.is_empty()
        && goal.obligations.iter().all(|obligation| {
            !obligation.required
                || obligation.state == harness_contract::goal::ObjectiveObligationState::Satisfied
        })
        && goal
            .criteria
            .iter()
            .all(|criterion| match criterion.status {
                harness_contract::goal::AcceptanceStatus::Satisfied => goal
                    .reviews
                    .iter()
                    .rev()
                    .find(|review| review.criterion_ref == criterion.id)
                    .is_some_and(|review| {
                        review.decision == "satisfied"
                            && review.spec_revision == goal.spec_revision
                            && review.input_manifest_digest == goal.spec_digest
                    }),
                harness_contract::goal::AcceptanceStatus::Waived => true,
                _ => false,
            })
}

/// Delivered result references no current review covers.
///
/// One live loop had the root cite four artifacts per request while the
/// recorded review covered three; nothing told the model which ref was
/// missing, so it re-issued the same partial review until the observation
/// window expired. Extracted so the Program projection derives the same set
/// from the same rule instead of freezing whatever the reopen event happened
/// to say.
pub(crate) fn uncovered_delivered_result_refs(
    program: &crate::AgenticProgramProjection,
    goal: &harness_contract::goal::GoalContract,
) -> Vec<String> {
    let intent = goal.user_intent_criterion_id.clone().unwrap_or_default();
    let covered = goal
        .reviews
        .iter()
        .rev()
        .find(|review| review.criterion_ref == intent)
        .map(|review| review.result_refs.clone())
        .unwrap_or_default();
    // The objective reviewer's own report is evidence of review, not a
    // delivered result: naming it as uncovered sends the model into an
    // unbounded review-regress. Filter it out with the same delivered-set rule
    // the conclusion gate uses.
    let goal_artifacts = goal
        .evidence_refs
        .iter()
        .filter(|reference| reference.starts_with("artifact:"))
        .cloned()
        .collect::<Vec<_>>();
    let delivered =
        crate::execution_core::goal::delivered_result_refs(program, goal, &goal_artifacts);
    goal.evidence_refs
        .iter()
        .filter(|reference| reference.starts_with("artifact:"))
        .filter(|reference| delivered.contains(reference))
        .filter(|reference| !covered.contains(reference))
        .cloned()
        .collect::<Vec<_>>()
}

/// A `completion_gap` that no longer describes reality must not be reported as
/// one. Shared with the Program projection so a satisfied review clears the
/// gap everywhere at once instead of only on the ingress path.
pub(crate) fn objective_completion_waiting_gap(
    waiting: Option<&harness_contract::goal::WaitDescriptor>,
    current_reviews_satisfied: bool,
    uncovered_result_refs: &[String],
) -> Option<String> {
    let waiting = waiting.filter(|waiting| waiting.reason_code == "completion_gap")?;
    let review_only = !waiting.required_refs.is_empty()
        && waiting.required_refs.iter().all(|reference| {
            reference.starts_with("original_intent_review_required:")
                || reference.starts_with("criterion_review_required:")
                || reference.starts_with("criterion_unsatisfied:")
                || reference
                    == "cannot satisfy a goal until every current criterion has semantic review or required durable evidence"
        });
    if current_reviews_satisfied && review_only {
        return None;
    }
    let mut gap = format!("objective_completion_gap:{}", waiting.reason);
    if !uncovered_result_refs.is_empty() {
        gap.push_str(&format!(
            "; uncovered_result_refs: {}",
            uncovered_result_refs.join(", ")
        ));
    }
    Some(gap)
}

/// A satisfied ObjectiveReview is the single semantic proof consumed by the
/// completion gate. Require it to cover the full result set of the active
/// completion request; otherwise the Program would durably accept a partial
/// verdict that can never satisfy `review_covers_current_goal`.
fn validate_current_objective_review_scope(
    program: &crate::AgenticProgramProjection,
    review_result_refs: &[String],
) -> Result<(), String> {
    let Some(request) = program.completion_request.as_ref() else {
        return Ok(());
    };
    let missing = request
        .result_refs
        .iter()
        .filter(|reference| !review_result_refs.contains(reference))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    Err(format!(
        "objective_review_incomplete_current_results: this satisfied verdict must cover every result in the active completion request; missing refs: {}. Read each missing result with evidence_retrieve and complete all returned next_request reads, then submit one objective_review containing the full current result_refs set. Do not repeat a partial satisfied review.",
        missing.join(", ")
    ))
}

/// Count the distinct Teams a submitted artifact integrates: the accepted Task
/// whose `artifact_refs` contain it plus that Task's transitive accepted
/// `depends_on`. Mirrors `agentic::supervision::integrated_accepted_tasks`, so
/// the Objective completion request can lead with the synthesis artifact.
fn objective_artifact_integration_score(
    program: &crate::AgenticProgramProjection,
    artifact_ref: &str,
) -> usize {
    let mut teams = std::collections::BTreeSet::new();
    let mut visited = std::collections::BTreeSet::new();
    let mut pending = program
        .tasks
        .values()
        .filter(|task| {
            task.status == crate::AgenticTaskStatus::Accepted
                && task
                    .artifact_refs
                    .iter()
                    .any(|reference| reference == artifact_ref)
        })
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    while let Some(task_ref) = pending.pop() {
        if !visited.insert(task_ref.clone()) {
            continue;
        }
        let Some(task) = program.tasks.get(&task_ref) else {
            continue;
        };
        if task.status != crate::AgenticTaskStatus::Accepted {
            continue;
        }
        teams.insert(task.team_id.clone());
        pending.extend(task.depends_on.iter().cloned());
    }
    teams.len()
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
    fn objective_review_gap_is_returned_to_the_root_continuation() {
        let waiting = harness_contract::goal::WaitDescriptor {
            reason_code: "completion_gap".into(),
            reason: "original_intent_review_required:user_intent".into(),
            required_refs: vec!["original_intent_review_required:user_intent".into()],
            generation: 36,
        };
        assert_eq!(
            objective_completion_waiting_gap(Some(&waiting), false, &[]).as_deref(),
            Some("objective_completion_gap:original_intent_review_required:user_intent")
        );
        let external_wait = harness_contract::goal::WaitDescriptor {
            reason_code: "waiting_external_decision".into(),
            ..waiting.clone()
        };
        assert_eq!(
            objective_completion_waiting_gap(Some(&external_wait), false, &[]),
            None
        );
        assert_eq!(
            objective_completion_waiting_gap(Some(&waiting), true, &[]),
            None
        );
        // Uncovered delivered results are named so the model can complete the
        // review instead of re-issuing the same partial one.
        assert_eq!(
            objective_completion_waiting_gap(
                Some(&waiting),
                false,
                &["artifact:missing".to_string()]
            )
            .as_deref(),
            Some(
                "objective_completion_gap:original_intent_review_required:user_intent; \
                 uncovered_result_refs: artifact:missing"
            )
        );
    }

    #[test]
    fn satisfied_objective_review_must_cover_the_active_completion_result_set() {
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        program.completion_request = Some(crate::AgenticCompletionRequestProjection {
            action_id: "complete".into(),
            requested_by: "root".into(),
            program_revision: 7,
            result_refs: vec!["artifact:a".into(), "artifact:b".into()],
            primary_artifact_ref: Some("artifact:a".into()),
            evidence_refs: vec![],
            unresolved: vec![],
        });

        let error = validate_current_objective_review_scope(&program, &["artifact:a".into()])
            .expect_err("a partial satisfied review cannot verify this request");
        assert!(error.contains("artifact:b"));
        assert!(error.contains("evidence_retrieve"));
        validate_current_objective_review_scope(
            &program,
            &["artifact:a".into(), "artifact:b".into()],
        )
        .expect("a complete review scope is allowed");
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

    fn task_fixture(
        task_id: &str,
        team_id: &str,
        status: crate::AgenticTaskStatus,
        depends_on: Vec<&str>,
        artifact_refs: Vec<&str>,
    ) -> crate::AgenticTaskProjection {
        crate::AgenticTaskProjection {
            task_id: task_id.to_string(),
            team_id: team_id.to_string(),
            title: task_id.to_string(),
            objective: "o".into(),
            acceptance: "a".into(),
            acceptance_checks: Vec::new(),
            required_capabilities: Vec::new(),
            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
            depends_on: depends_on.into_iter().map(str::to_string).collect(),
            status,
            claimant: None,
            claim_generation: 0,
            claim_execution_id: None,
            claimed_at_ms: None,
            lease_expires_at_ms: None,
            active_attempts: Default::default(),
            artifact_refs: artifact_refs.into_iter().map(str::to_string).collect(),
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
        }
    }

    #[test]
    fn synthesis_artifact_outranks_a_single_team_artifact() {
        use crate::AgenticTaskStatus::Accepted;
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        for (id, team, artifact) in [
            ("task:a", "team:a", "artifact:a"),
            ("task:b", "team:b", "artifact:b"),
            ("task:c", "team:c", "artifact:c"),
        ] {
            program.tasks.insert(
                id.to_string(),
                task_fixture(id, team, Accepted, vec![], vec![artifact]),
            );
        }
        program.tasks.insert(
            "task:synthesis".to_string(),
            task_fixture(
                "task:synthesis",
                "team:s",
                Accepted,
                vec!["task:a", "task:b", "task:c"],
                vec!["artifact:final"],
            ),
        );
        // A per-team artifact covers one team; the synthesis covers four.
        assert_eq!(
            objective_artifact_integration_score(&program, "artifact:a"),
            1
        );
        assert_eq!(
            objective_artifact_integration_score(&program, "artifact:final"),
            4
        );
    }

    #[test]
    fn completion_input_leads_with_the_synthesis_and_is_none_without_artifacts() {
        use crate::AgenticTaskStatus::Accepted;
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        program.tasks.insert(
            "task:empty".to_string(),
            task_fixture("task:empty", "team:a", Accepted, vec![], vec![]),
        );
        assert!(completion_objective_input(&program).is_none());

        program.tasks.insert(
            "task:audit".to_string(),
            task_fixture("task:audit", "team:a", Accepted, vec![], vec!["artifact:a"]),
        );
        program.tasks.insert(
            "task:syn".to_string(),
            task_fixture(
                "task:syn",
                "team:s",
                Accepted,
                vec!["task:audit"],
                vec!["artifact:final"],
            ),
        );
        let input = completion_objective_input(&program).expect("completion input");
        assert_eq!(
            input.result_refs.first().map(String::as_str),
            Some("artifact:final"),
            "the cross-Team synthesis artifact must lead result_refs"
        );
    }

    #[tokio::test]
    async fn drive_completion_declines_without_a_program() {
        let services = crate::RuntimeServices::in_memory().expect("services");
        let driven = services
            .drive_agentic_objective_completion("session", "turn", "graph")
            .await
            .expect("no error");
        assert!(!driven, "no Program means no completion to drive");
    }

    /// A model can corrupt the opaque content suffix of an otherwise real
    /// tool-call id (one live run cited `tool-raw-...-6` for the durable
    /// `tool-raw-...-2c77431438342406`). The refusal must stay a refusal -
    /// accepting the invented id would break evidence integrity - but it must
    /// name the canonical receipt so the model can repair in one round instead
    /// of guessing variants until the repetition guard blocks it.
    #[tokio::test]
    async fn corrupted_evidence_suffix_is_refused_with_the_canonical_receipt_named() {
        use harness_contract::agent_action::{AgentActorKind, TaskSubmitInput};
        use harness_contract::reality::EvidenceRef;

        let canonical = "tool-raw-chatcmpl-tool-aa0d514a557763e5-2c77431438342406";
        let temp = tempfile::tempdir().expect("temp");
        let store = std::sync::Arc::new(crate::test_support::session_store());
        store
            .create_session(&session::SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "evidence-hint".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-10-10T00:00:00Z".to_string(),
                last_activity: "2026-10-10T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".to_string(),
            })
            .await
            .expect("session");
        let artifacts = std::sync::Arc::new(
            crate::ArtifactStore::for_test(
                temp.path().to_path_buf(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        );
        let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
        let facade = crate::context_evidence::raw::RawEvidenceFacade::new(
            crate::context_evidence::raw::SessionPortRawEvidenceStore::new(
                ports.clone(),
                artifacts,
            ),
        );
        let access = facade
            .persist(crate::context_evidence::raw::RawEvidenceWrite {
                evidence_ref: EvidenceRef::observed("tool", canonical),
                session_id: "s1".to_string(),
                media_type: "text/plain".to_string(),
                visibility_scope: "session:s1".to_string(),
                payload: "reviewed body".to_string(),
                metadata: serde_json::json!({
                    "tool_name": "read_ref",
                    "tool_call_id": "chatcmpl-tool-aa0d514a557763e5",
                }),
            })
            .await
            .expect("durable evidence");
        assert_eq!(access.evidence_ref.id, canonical);

        let workspace = temp.path().join("ws");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let services = crate::RuntimeServices::test_builder(temp.path(), workspace)
            .session_ports(ports.clone(), ports.clone(), ports.clone(), ports)
            .build()
            .expect("services");
        let actor = harness_contract::agent_action::AgentActorBinding {
            objective_id: "objective".to_string(),
            program_id: "program".to_string(),
            session_id: "s1".to_string(),
            turn_id: "turn".to_string(),
            root_execution_id: None,
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: "review".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: vec![],
            actor_id: "agent:1".to_string(),
            kind: AgentActorKind::Agent,
            execution_id: None,
            team_id: None,
            agent_id: Some("agent:1".to_string()),
        };
        let action = AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: "task:1".to_string(),
            deliverable: None,
            artifact_refs: vec![],
            evidence_refs: vec!["tool://tool-raw-chatcmpl-tool-aa0d514a557763e5-6".to_string()],
            unresolved: vec![],
        });
        let error = services
            .validate_agent_action_evidence(&actor, &action)
            .await
            .expect_err("a corrupted content suffix must never resolve");
        assert!(
            error.contains("tool://tool-raw-chatcmpl-tool-aa0d514a557763e5-6"),
            "{error}"
        );
        assert!(
            error.contains(&format!("tool://{canonical}")),
            "the refusal must name the canonical receipt: {error}"
        );
        assert!(error.contains("copy that exact reference"), "{error}");
        // The action-id shape a tool result hands the model
        // (`tool-raw-<call_id>:<attempt>`) names the same durable receipt: the
        // attempt index cannot select different content, so it must resolve
        // instead of costing the model its review Task.
        let attempt_form = AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: "task:1".to_string(),
            deliverable: None,
            artifact_refs: vec![],
            evidence_refs: vec![format!("tool://tool-raw-chatcmpl-tool-aa0d514a557763e5:8")],
            unresolved: vec![],
        });
        // The test's Runtime and the evidence facade use separate artifact
        // stores, so a resolved shape fails later at content resolution. That
        // is the proof we need: the refusal is no longer "no canonical durable
        // receipt" - the action-id shape resolved to the recorded receipt.
        let attempt_error = services
            .validate_agent_action_evidence(&actor, &attempt_form)
            .await
            .expect_err("the test stores are separate, so content resolution fails");
        assert!(
            !attempt_error.contains("no canonical durable receipt"),
            "the action-id shape must resolve to the durable receipt: {attempt_error}"
        );
        assert!(
            attempt_error.contains("points to missing durable content"),
            "{attempt_error}"
        );
    }
}
