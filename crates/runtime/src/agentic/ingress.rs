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

// Internal domain boundary: member submissions are never converted into a
// semantic success merely to share the single-action control flow.
enum OriginalIngressResult {
    Single(AgentActionObservation),
    SingleChecked(Result<AgentActionObservation, crate::AgentActionServiceError>),
    PreparedMember(PreparedMemberChecked),
}
enum PreparedMemberChecked {
    Committed(std::sync::Arc<crate::OriginalCommittedMember>, bool),
    NoCommittedReceipt(AgentActionObservation),
    Error(crate::AgentActionServiceError),
}
enum OriginalIngressMember {
    Prepared(crate::step_snapshot::OriginalPreparedMemberCapability),
}
impl OriginalIngressResult {
    fn into_single(self) -> Result<AgentActionObservation, String> {
        match self {
            Self::Single(observation) => Ok(observation),
            Self::SingleChecked(result) => result.map_err(|error| error.to_string()),
            Self::PreparedMember(_) => Err("unknown: member result reached single-action caller".into()),
        }
    }
    fn into_single_typed(self) -> Result<AgentActionObservation, crate::AgentActionServiceError> {
        match self {
            Self::Single(observation) => Ok(observation),
            Self::SingleChecked(result) => result,
            Self::PreparedMember(_) => Err(crate::AgentActionServiceError::MemberUnknown(
                "member result reached single-action caller".into(),
            )),
        }
    }
}

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
/// Budget for a refusal whose cause is a step the reviewer has not taken yet.
///
/// A precondition is not a failure: "you have not read the body" is a step that
/// was never executed, not an attempt that went wrong. Counting it against the
/// failure budget punished a reviewer for the framework's own silence about what
/// to read - one live reviewer was settled twice, and each settlement forced a
/// fresh attempt that started from zero reads again, which made the loop longer
/// the tighter the budget was.
///
/// The bound stays because the original concern is real: an artifact whose body
/// genuinely cannot be read must not loop forever. Twelve is enough to read
/// every delivered result of a normal request and then submit.
const MAX_REVIEW_PRECONDITION_RETRIES: u32 = 12;
const MAX_REVIEW_PROGRAM_REBUILDS: u32 = 3;

static REVIEW_SOURCE_CHANGED_STREAK: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Retry-class review rejection caused by a concurrently-changing source. These
/// strings are authored by Runtime (`review_evidence/policy.rs`), never by a
/// provider or model.
/// A refusal whose cause is a step the caller has not taken yet.
///
/// "You have not read the delivered results" is not a failed attempt; it is an
/// unexecuted step. It carries its own, larger budget so a reviewer is never
/// settled for the framework's own silence about what to read.
fn is_review_precondition_refusal(error: &str) -> bool {
    error.contains("review_requires_current_physical_reads")
}

/// A refusal whose cause is the content or the attempt itself. These are real
/// failures and keep the tight budget.
fn is_review_content_refusal(error: &str) -> bool {
    // A review attempt whose generation no longer matches an already-reviewed or
    // reworked Task is stale; bound it so it settles instead of looping, and a
    // fresh attempt can be dispatched.
    error.contains("review physical attempt is stale")
        || ((error.contains("review policy source changed")
            || error.contains("effect review source changed")
            || error.contains("delegated effect source changed"))
            && error.contains("retry"))
}

fn is_review_source_changed_retry(error: &str) -> bool {
    is_review_precondition_refusal(error)
        || is_review_content_refusal(error)
        || is_review_source_drift(error)
}

/// A refusal whose cause is a source head that moved while the review was
/// reading it.
///
/// Every string below is authored by Runtime's own read fences
/// (`review_evidence/results.rs`, `review_evidence/policy.rs`,
/// `review_evidence/current_review.rs`), never by a provider or a model, and
/// each of them still refuses. The class only decides the bounded retry
/// budget and how the attempt settles: a drifting source that matched no
/// classifier used to clear the streak on every rejection, so
/// `failed_review_attempts` never incremented and the bounded review-attempt
/// terminal was never reached.
fn is_review_source_drift(error: &str) -> bool {
    error.contains("result metadata source changed during resolution")
        || error.contains("result metadata source moved backwards during resolution")
        || error.contains("proof source changed during validation")
        || error.contains("proof source changed while paging")
        || error.contains("proof source changed at final page boundary")
        || error.contains("completed review witness source changed during authentication")
        || error.contains("current Session head moved backwards while reading original material")
        || error.contains("original Session material cut or current head differs")
}

fn is_review_program_advanced(error: &str) -> bool {
    error.starts_with("result Program changed during original producer resolution:")
        || error.starts_with("result metadata changed after original effect enumeration:")
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
        original_execution: None,
        objective_id: program.objective_id.clone(),
        program_id: program.program_id.clone(),
        session_id: program.session_id.clone(),
        turn_id: program.turn_id.clone(),
        root_execution_id: program.root_execution_id.clone(),
        required_team_count: program.required_team_count,
        exact_team_count: program.exact_team_count,
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
    /// Pre-grant replay reads the original semantic transaction only. The
    /// Host must separately recover the original physical ToolBatch receipt.
    pub(crate) fn verify_original_root_replay(
        &self, witness: &crate::conversation::tool_plane::OriginalRootReplayWitness<'_>,
    ) -> Result<crate::agentic::OriginalReplayedAction, String> {
        if witness.original_action_event().kind != "agentic.action_applied" {
            return Err("unknown: original completion rejection replay consumer unavailable".into());
        }
        self.agent_action_service().replay_single_original(witness).map_err(|error| error.to_string())
    }

    /// Private authenticated-user cause. Public model ObjectiveUpdate still
    /// takes prepare_agentic_objective_action and cannot replace user_intent.
    pub(crate) fn commit_user_input_goal_revision(
        &self,
        original: &harness_contract::goal::GoalContract,
        frame: &crate::execution_core::goal::UserGoalInputFrame,
        prepared: &crate::execution_core::goal::PreparedGoalMutation,
        program_revision: u64,
        graph_revision: u64,
        artifact_ref: &str,
    ) -> Result<(), String> {
        use harness_contract::agent_action::{ObjectiveUpdateInput, ObjectiveUpdateOperation};
        let binding = original
            .execution_binding
            .as_ref()
            .ok_or("user Goal binding missing")?;
        frame.validate(binding)?;
        let program = self
            .agent_action_service()
            .project_if_exists(&binding.agentic_program_id)
            .map_err(|e| e.to_string())?
            .ok_or("user Goal Program missing")?;
        if program.session_id != binding.session_id
            || program.turn_id != binding.turn_id
            || program.root_execution_id.as_deref() != Some(binding.root_execution_id.as_str())
        {
            return Err("user Goal Program lineage mismatch".into());
        }
        let envelope = AgentActionEnvelope {
            action_id: format!("user-goal:{}:{}", original.id, frame.digest()?),
            actor: AgentActorBinding {
                original_execution: None,
                objective_id: program.objective_id.clone(),
                program_id: program.program_id.clone(),
                session_id: program.session_id.clone(),
                turn_id: program.turn_id.clone(),
                root_execution_id: program.root_execution_id.clone(),
                required_team_count: program.required_team_count,
                exact_team_count: program.exact_team_count,
                objective_summary: program.objective_summary.clone(),
                model_lease: program.model_lease.clone(),
                permission_ceiling: Some(program.permission_ceiling),
                resource_scopes: program.resource_scopes.clone(),
                actor_id: format!("{}:root", program.program_id),
                kind: AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
            expected_revision: Some(program_revision),
            action: AgentAction::ObjectiveUpdate(ObjectiveUpdateInput {
                criterion_ref: original.user_intent_criterion_id.clone(),
                operation: ObjectiveUpdateOperation::Replace,
                statement_ref: Some(artifact_ref.into()),
                source_refs: vec![artifact_ref.into()],
                reason_ref: Some(artifact_ref.into()),
                evidence_requirements: vec![],
            }),
        };
        let result = self
            .agent_action_service()
            .apply_with_goal_event_sources(
                &envelope,
                prepared.stream_id.clone(),
                prepared.expected_stream_revision,
                prepared.event.clone(),
                None,
                &[crate::runtime_event_store::ExpectedStreamRevision {
                    stream_id: binding.root_execution_id.clone(),
                    expected_revision: graph_revision,
                }],
            )
            .map_err(|e| e.to_string())?;
        if result.status != AgentActionStatus::Applied {
            return Err(format!("user Goal Program rejected: {:?}", result));
        }
        Ok(())
    }

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
                let resolved = self
                    .session_evidence_access(&actor.session_id, evidence_id)
                    .await;
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
                        let mapped = self
                            .agent_action_service()
                            .project_if_exists(&actor.program_id)
                            .map_err(|error| format!("load evidence Program: {error}"))?
                            .and_then(|program| {
                                program
                                    .artifact_content_selector(evidence_ref)
                                    .map(str::to_string)
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
                                    "{} evidence {evidence_ref} points to missing durable content: {store_error}. {}",
                                    action.kind(),
                                    super::order_contract::ARTIFACT_CONTENT_REF_CONTRACT
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

    /// Delegated ToolHost entry. The capability exists only during the
    /// original admitted call and binds the exact action or batch member.
    pub async fn submit_delegated_tool_action(
        &self,
        capability: &super::action_invocation::VerifiedActionInvocation,
        envelope: &AgentActionEnvelope,
        tool: &str,
        original_input: &serde_json::Value,
    ) -> Result<AgentActionObservation, crate::AgentActionServiceError> {
        capability.validate_envelope(envelope, tool, original_input)
            .map_err(|error| crate::AgentActionServiceError::EventStore(error.to_string()))?;
        // Preserve checked-transaction uncertainty across the original caller.
        self.submit_agent_action_from_owner(envelope, false, false, Some((capability, tool, original_input)), None)
            .await.map_err(crate::AgentActionServiceError::EventStore)?.into_single_typed()
    }

    /// The candidate carries no grant. The original action transaction decides
    /// member uniqueness and legacy ambiguity while holding the original owners.
    pub async fn submit_agent_action_member(
        &self, envelope: &AgentActionEnvelope,
        capability: crate::step_snapshot::OriginalPreparedMemberCapability,
    ) -> crate::tool_host::PreparedMemberSubmitOutcome {
        use crate::tool_host::{OriginalBatchMemberStop, OriginalBatchMemberStopReason as Stop, PreparedMemberSubmitOutcome as Outcome};
        let coordinate = capability.coordinate();
        let reservation = capability.original_reservation();
        let effect_fence = capability.original_effect_fence();
        let make_stop = |coordinate, reason| OriginalBatchMemberStop::publish_with_owners(
            coordinate, reason, std::sync::Arc::clone(&reservation), std::sync::Arc::clone(&effect_fence));
        if let Err(error) = capability.matches_action_envelope(envelope) {
            return Outcome::Stopped(make_stop(coordinate, Stop::BeforeOutcomeUnknown(error.to_string())));
        }
        let result = match self.submit_agent_action_from_owner(envelope, false, false, None,
            Some(OriginalIngressMember::Prepared(capability))).await {
            Ok(result) => result,
            Err(error) => return Outcome::Stopped(make_stop(coordinate, Stop::BeforeOutcomeUnknown(error))),
        };
        match result {
            OriginalIngressResult::PreparedMember(PreparedMemberChecked::Committed(handle, true)) => {
                match crate::tool_host::OriginalMemberSubmitOutcome::from_original_submission(
                    super::action_service::OriginalMemberSubmission::Committed(handle), self.event_store(), envelope) {
                    crate::tool_host::OriginalMemberSubmitOutcome::Committed(member) => {
                        if matches!(member.observation(), crate::tool_host::OriginalMemberObservation::Unavailable) {
                            Outcome::Stopped(make_stop(coordinate, Stop::CommittedObservationUnavailable(member)))
                        } else { Outcome::Committed(member) }
                    }
                    crate::tool_host::OriginalMemberSubmitOutcome::CommittedUnbound(handle) =>
                        Outcome::Stopped(make_stop(coordinate, Stop::CommittedUnbound(handle))),
                    crate::tool_host::OriginalMemberSubmitOutcome::NoCommittedReceipt(observation) =>
                        Outcome::Stopped(make_stop(coordinate, Stop::UnknownNoReceipt(observation))),
                }
            },
            OriginalIngressResult::PreparedMember(PreparedMemberChecked::Committed(handle, false)) =>
                Outcome::Stopped(make_stop(coordinate, Stop::CommittedUnbound(
                    crate::tool_host::OriginalMemberUnboundCommit { handle, reason: "original committed member awaits owner reconciliation".into() }))),
            OriginalIngressResult::PreparedMember(PreparedMemberChecked::NoCommittedReceipt(observation)) =>
                Outcome::Stopped(make_stop(coordinate, Stop::UnknownNoReceipt(observation))),
            OriginalIngressResult::PreparedMember(PreparedMemberChecked::Error(error)) => {
                let reason = match self.event_store().original_member_noncommit(&reservation, &effect_fence) {
                    Ok(Some(terminal)) => Stop::ConfirmedNoncommit(terminal),
                    // Missing, ambiguous or inaccessible custody remains the original Unknown.
                    Ok(None) | Err(_) => Stop::BeforeOutcomeError(error),
                };
                Outcome::Stopped(make_stop(coordinate, reason))
            },
            _ => Outcome::Stopped(make_stop(coordinate, Stop::BeforeOutcomeUnknown("unexpected original member result".into()))),
        }
    }

    // Only the original Runtime dispatch/recovery owners call this entry.
    pub(super) async fn submit_runtime_supervisor_action(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, String> {
        if envelope.actor.kind != AgentActorKind::Supervisor || envelope.actor.original_execution.is_some() {
            return Err("invalid Runtime supervisor action".into());
        }
        self.submit_agent_action_from_owner(envelope, false, true, None, None).await?.into_single()
    }

    // Only the Runtime completion owner below can select the internal path;
    // caller-controlled actor fields are not proof of that provenance.
    async fn submit_agent_action_from_owner(
        &self,
        envelope: &AgentActionEnvelope,
        internal_completion: bool,
        internal_supervisor: bool,
        owner_call: Option<(&super::action_invocation::VerifiedActionInvocation, &str, &serde_json::Value)>,
        member_call: Option<OriginalIngressMember>,
    ) -> Result<OriginalIngressResult, String> {
        envelope.validate().map_err(|error| error.to_string())?;
        if !internal_completion && !internal_supervisor && member_call.is_none() {
            let (capability, tool, input) = owner_call.ok_or("original single-call capability missing")?;
            capability.validate_envelope(envelope, tool, input).map_err(str::to_string)?;
        }
        if !internal_completion && !internal_supervisor && envelope.actor.original_execution.is_none() {
            return Err("public agent action requires original authenticated execution".to_string());
        }
        self.ensure_session_recovery_admitted(&envelope.actor.session_id)
            .map_err(|error| error.to_string())?;
        let actions = self.agent_action_service();
        if member_call.is_none() && owner_call.is_none() {
            if let Some(replay) = actions
                .replay_if_applied(envelope)
                .map_err(|error| error.to_string())?
            {
                return Ok(OriginalIngressResult::Single(replay));
            }
        }
        if envelope.actor.kind == AgentActorKind::Root && envelope.actor.original_execution.is_none()
            && !(internal_completion && matches!(&envelope.action, AgentAction::ObjectiveCompleteRequest(_)))
        {
            return Err("new root action requires original physical execution".to_string());
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
            let key = review_attempt_key(envelope);
            let mut program = actions
                .project(&envelope.actor.program_id)
                .map_err(|error| error.to_string())?;
            let mut rebuilds = 0u32;
            loop {
                // Resolve both the result set and its manifest from one projection.
                let results = match &envelope.action {
                    AgentAction::ObjectiveReview(input) => input.result_refs.clone(),
                    AgentAction::TaskReview(input) => program
                        .tasks
                        .get(&input.task_ref)
                        .ok_or("review Task not found")?
                        .artifact_refs
                        .clone(),
                    _ => unreachable!("review classification"),
                };
                if matches!(
                    &envelope.action,
                    AgentAction::ObjectiveReview(input)
                        if input.decision
                            == harness_contract::agent_action::ObjectiveReviewDecision::Satisfied
                ) {
                    validate_current_objective_review_scope(&program, &results)?;
                }
                let built = self
                    .independent_result_review_with_evidence(
                        &envelope.actor,
                        &program,
                        &results,
                        match &envelope.action {
                            AgentAction::ObjectiveReview(input) => {
                                Some(input.criterion_ref.as_str())
                            }
                            _ => None,
                        },
                        match &envelope.action {
                            AgentAction::TaskReview(input) => &input.evidence_refs,
                            AgentAction::ObjectiveReview(input) => &input.evidence_refs,
                            _ => unreachable!("review classification"),
                        },
                        Some(&crate::agentic::review_evidence::CurrentReviewContext {
                            action_id: &envelope.action_id,
                            action: &envelope.action,
                            actor: &envelope.actor,
                        }),
                    )
                    .await;
                match built {
                    Ok(review) => {
                        clear_review_streak(&key);
                        break Some(review);
                    }
                    Err(error) if is_review_program_advanced(&error) => {
                        rebuilds += 1;
                        if rebuilds > MAX_REVIEW_PROGRAM_REBUILDS {
                            clear_review_streak(&key);
                            return Err(format!(
                                "review could not reach a stable Program cut after \
                                 {MAX_REVIEW_PROGRAM_REBUILDS} rebuilds: {error}"
                            ));
                        }
                        program = actions
                            .project(&envelope.actor.program_id)
                            .map_err(|projection_error| projection_error.to_string())?;
                        continue;
                    }
                    Err(error) => {
                        if is_review_source_changed_retry(&error) {
                            // A precondition the caller has not satisfied yet is not
                            // a failed attempt, so it draws on its own budget. Both
                            // classes still settle: neither may loop forever.
                            let bound = if is_review_precondition_refusal(&error) {
                                MAX_REVIEW_PRECONDITION_RETRIES
                            } else {
                                MAX_REVIEW_SOURCE_CHANGED_RETRIES
                            };
                            let streak = bump_review_streak(&key);
                            if streak >= bound {
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
            }
        } else {
            None
        };
        // Recheck after the asynchronous evidence/review work. A must still
        // consume this owner in its checked+cancellable transaction; this
        // process-local recheck is not a commit fence.
        if let Some((capability, tool, input)) = owner_call {
            capability.validate_envelope(envelope, tool, input).map_err(str::to_string)?;
        }
        let verification = review.as_ref().map(|review| &review.verification);
        if let Some(member) = member_call {
            let goal = if matches!(envelope.action, AgentAction::ObjectiveUpdate(_) | AgentAction::ObjectiveReview(_)) {
                let producers = review.as_ref().map_or(&[][..], |review| review.producers.as_slice());
                let prepared = self.goal_store().prepare_agentic_objective_action(envelope, producers, verification)?;
                Some((prepared.stream_id, prepared.expected_stream_revision, prepared.event))
            } else {
                None
            };
            let mut streams = vec![format!("agentic-program:{}", envelope.actor.program_id)];
            streams.push(format!("session:{}", envelope.actor.session_id));
            if matches!(envelope.action, AgentAction::TeamCreate(_))
                || matches!(&envelope.action, AgentAction::TaskPublish(input) if !input.obligation_refs.is_empty()) {
                if let Some(root) = envelope.actor.root_execution_id.as_deref() {
                    streams.push(format!("goal:goal:{root}"));
                }
            }
            if let Some((stream, _, _)) = &goal { streams.push(stream.clone()); }
            if let Some(physical) = &envelope.actor.original_execution { streams.push(physical.graph_id.clone()); }
            if let Some(proof) = verification {
                if let Some(inputs) = &proof.input_set { streams.extend(inputs.source_revisions.keys().cloned()); }
                streams.extend(proof.result_source_revisions.keys().cloned());
                if !proof.independence_required || proof.effect_manifest_digest.is_some() {
                    streams.push(format!("session:{}", envelope.actor.session_id));
                    streams.extend(proof.effect_source_refs.iter().cloned());
                }
            }
            return match member {
                OriginalIngressMember::Prepared(candidate) => {
                    candidate.matches_action_envelope(envelope).map_err(|error| error.to_string())?;
                    let no_receipt = std::cell::RefCell::new(None);
                    let committed = candidate.commit_checked(self, &streams, |held, sources, member, authority, reservation, effect_fence| {
                        let (control, claim, cursor, outer_hook) = if let Some(host) = member.host() {
                            let (control, claim, cursor) = host.locked_commit_context()
                                .map_err(|error| {
                                    crate::AgentActionServiceError::MemberUnknown(error)
                                })?;
                            let hook = host.original_outer_hook_owner()
                                .map_err(|error| {
                                    crate::AgentActionServiceError::MemberUnknown(error)
                                })?;
                            (control, claim, cursor, Some(hook))
                        } else if let Some(process) = member.process() {
                            let (control, claim, cursor) = process.locked_commit_context()
                                .map_err(|error| {
                                    crate::AgentActionServiceError::MemberUnknown(error)
                                })?;
                            (control, claim, cursor, None)
                        } else {
                            return Err(crate::AgentActionServiceError::MemberUnknown(
                                "original member owner is unavailable".into()));
                        };
                        let authorization = authority.staged_authorization_for_original_action()
                            .map_err(|error| {
                                crate::AgentActionServiceError::MemberUnknown(error)
                            })?.authorization;
                        // New authority events are part of A's checked append;
                        // there are no prior consumed member events to invent.
                        let applied = actions.apply_member_locked(envelope, goal, verification, sources, held, control,
                            &claim, cursor, member.prepared(), &authorization, member.ticket(), &[], authority, member, reservation, effect_fence, outer_hook.as_ref())
                            .map_err(|error| {
                                error
                            })?;
                        match applied {
                            super::action_service::OriginalMemberSubmission::Committed(handle) => {
                                Ok(handle)
                            },
                            super::action_service::OriginalMemberSubmission::NoCommittedReceipt(observation) => {
                                *no_receipt.borrow_mut() = Some(observation);
                                Err(crate::AgentActionServiceError::MemberUnknown("original member has no committed receipt".into()))
                            }
                        }
                    });
                    let result = match (committed, no_receipt.into_inner()) {
                        (Ok((handle, applied)), _) => PreparedMemberChecked::Committed(handle, applied),
                        (Err(_), Some(observation)) => PreparedMemberChecked::NoCommittedReceipt(observation),
                        (Err(error), None) => PreparedMemberChecked::Error(error),
                    };
                    Ok(OriginalIngressResult::PreparedMember(result))
                }
            };
        }
        if let Some((capability, tool, input)) = owner_call {
            let root = capability.original_root_owner().map_err(str::to_string)?;
            let goal = if matches!(envelope.action, AgentAction::ObjectiveUpdate(_) | AgentAction::ObjectiveReview(_)) {
                let producers = review.as_ref().map_or(&[][..], |review| review.producers.as_slice());
                let prepared = self.goal_store().prepare_agentic_objective_action(envelope, producers, verification)?;
                Some((prepared.stream_id, prepared.expected_stream_revision, prepared.event))
            } else { None };
            let mut streams = vec![format!("agentic-program:{}", envelope.actor.program_id)];
            if matches!(envelope.action, AgentAction::TeamCreate(_))
                || matches!(&envelope.action, AgentAction::TaskPublish(input) if !input.obligation_refs.is_empty()) {
                if let Some(root) = envelope.actor.root_execution_id.as_deref() {
                    streams.push(format!("goal:goal:{root}"));
                }
            }
            if let Some((stream, _, _)) = &goal { streams.push(stream.clone()); }
            if let Some(physical) = &envelope.actor.original_execution { streams.push(physical.graph_id.clone()); }
            if let Some(proof) = verification {
                if let Some(inputs) = &proof.input_set { streams.extend(inputs.source_revisions.keys().cloned()); }
                streams.extend(proof.result_source_revisions.keys().cloned());
                if !proof.independence_required || proof.effect_manifest_digest.is_some() {
                    streams.push(format!("session:{}", envelope.actor.session_id));
                    streams.extend(proof.effect_source_refs.iter().cloned());
                }
            }
            if let Some(root) = root {
                return root.with_single_checked(self, &streams, |original| {
                    let result = actions.apply_single_locked(envelope, goal, verification, capability, tool, input, original);
                    let result = result.and_then(|submission| {
                        let committed = submission.committed.is_some();
                        if let Some(handle) = submission.committed {
                            original.request().tool_progress.record_original_single(handle)
                                .map_err(crate::AgentActionServiceError::MemberUnknown)?;
                        }
                        match submission.observation {
                            Err(error) if committed => Err(crate::AgentActionServiceError::MemberUnknown(
                                format!("original action committed; observation unavailable: {error}"))),
                            result => result,
                        }
                    });
                    Ok(OriginalIngressResult::SingleChecked(result))
                });
            }
            return capability.with_original_delegated_checked(self, &streams, |delegated| {
                let original = crate::conversation::tool_plane::OriginalSingleCheckedInputs::Delegated(delegated);
                let result = actions.apply_single_locked(envelope, goal, verification, capability, tool, input, &original);
                let result = result.and_then(|submission| {
                    let committed = submission.committed.is_some();
                    if let Some(handle) = submission.committed {
                        original.request().tool_progress.record_original_single(handle)
                            .map_err(crate::AgentActionServiceError::MemberUnknown)?;
                    }
                    match submission.observation {
                        Err(error) if committed => Err(crate::AgentActionServiceError::MemberUnknown(
                            format!("original action committed; observation unavailable: {error}"))),
                        result => result,
                    }
                });
                Ok(OriginalIngressResult::SingleChecked(result))
            });
        }
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
        } else if internal_completion {
            actions.apply_runtime_root_completion(envelope).map_err(|error| error.to_string())
        } else if internal_supervisor {
            actions.apply_runtime_supervisor(envelope).map_err(|error| error.to_string())
        } else {
            actions.apply(envelope).map_err(|error| error.to_string())
        }.map(OriginalIngressResult::Single)
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
        // The Goal's own gate is stricter than the Program's rung gate. A Goal
        // that still requires an Objective review reopens every request the
        // Program considers closed, so submitting one here produces a
        // framework-authored request the Supervisor immediately reopens while
        // the root never receives the turn that would close the gap. One live
        // run burned three such requests (`objective-complete-request:runtime:*`)
        // and 56M tokens this way, with the model making no tool call after the
        // first reopen. Decline instead, so control stays with the only actor
        // that can publish the missing review.
        if let Some(gap) = crate::agentic::agentic_program_completion_gap(self, session_id, turn_id)
        {
            tracing::info!(
                program_id = %program_id,
                gap = %gap,
                "drive_agentic_objective_completion declined: the Goal still records a completion gap"
            );
            return Ok(false);
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
            original_execution: None,
            objective_id: program.objective_id.clone(),
            program_id: program.program_id.clone(),
            session_id: program.session_id.clone(),
            turn_id: program.turn_id.clone(),
            root_execution_id: program.root_execution_id.clone(),
            required_team_count: program.required_team_count,
            exact_team_count: program.exact_team_count,
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
        let observation = self.submit_agent_action_from_owner(&envelope, true, false, None, None).await?.into_single()?;
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
        let observation = Box::pin(self.submit_runtime_supervisor_action(&fail)).await?;
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
    let program = match services
        .agent_action_service()
        .project_if_exists(&program_id)
    {
        Ok(Some(program)) => program,
        Ok(None) => return None,
        Err(error) => return Some(format!("completion_assessment_unknown:{error}")),
    };
    match crate::execution_core::goal::assessment::read_bound_goal_assessment(
        services.event_store(),
        &program,
    ) {
        Ok(assessment) => {
            if let Some(gap) = assessment.gap() {
                return Some(gap.to_string());
            }
        }
        Err(error) => return Some(format!("completion_assessment_unknown:{error}")),
    }
    if program.status != crate::AgenticProgramStatus::Open {
        return None;
    }
    let input = completion_objective_input(&program)?;
    crate::agentic::supervision::completion_gap(&program, &input)
}

#[cfg(test)]
use crate::execution_core::goal::assessment::objective_completion_waiting_gap;

/// A satisfied ObjectiveReview is the single semantic proof consumed by the
/// completion gate. Require it to cover the full result set of the active
/// completion request; otherwise the Program would durably accept a partial
/// verdict that can never satisfy `validate_review_coverage`.
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
        assert!(is_review_program_advanced(
            "result metadata changed after original effect enumeration: result=artifact:one source=session:one expected=1 observed=2; rebuild the review"
        ));
        assert!(is_review_program_advanced(
            "result Program changed during original producer resolution: source agentic-program:one is at revision 2 but the reviewed Program projection is at 1"
        ));
        for error in [
            "result metadata changed after original effect enumeration",
            "actual result input source moved during enumeration",
            "primary acquisition source changed during recovery",
            "publication source changed during original review discovery",
            "primary acquisition source changed during discovery",
            "proof artifact identity is ambiguous",
            "review_requires_effect_observation:artifact:new",
        ] {
            assert!(!is_review_program_advanced(error), "{error}");
        }
        assert!(is_review_source_changed_retry(
            "effect review source changed; retry current effects"
        ));
        assert!(is_review_source_changed_retry(
            "delegated effect source changed; retry current effects"
        ));
        assert!(is_review_source_changed_retry(
            "review policy source changed; retry against current strategy and tool plans"
        ));
        // A source head that moved while the review was reading it still
        // refuses; it now reaches the bounded settlement instead of clearing
        // the streak on every rejection.
        for error in [
            "result metadata source changed during resolution; rebuild the review; source=session:one expected=1 actual=2 phase=result_source_pin",
            "result metadata source moved backwards during resolution; rebuild the review; source=session:one expected=2 actual=1 phase=result_source_pin",
            "proof source changed during validation",
            "proof source changed during validation; retry current inputs; source=goal:x expected=1 actual=2 phase=final_source_fence; advanced=1 head=2 []",
            "proof source changed while paging; retry current inputs",
            "proof source changed at final page boundary; retry current inputs",
            "completed review witness source changed during authentication",
            "current Session head moved backwards while reading original material",
            "original Session material cut or current head differs",
        ] {
            assert!(is_review_source_drift(error), "{error}");
            assert!(is_review_source_changed_retry(error), "{error}");
        }
        // Unrelated rejections must not be treated as the wedged retry class.
        assert!(!is_review_source_changed_retry("review Task not found"));
        for error in [
            "review Task not found",
            "Artifact catalog source changed; start a fresh discovery",
            "result publication does not match its original physical packet/claim",
            "review physical attempt is stale",
        ] {
            assert!(!is_review_source_drift(error), "{error}");
        }
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
            criterion_source: Default::default(),
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
            claim_renewal_streak: 0,
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

    #[test]
    fn a_precondition_refusal_draws_on_its_own_budget() {
        // "You have not read the body" is an unexecuted step, not a failed
        // attempt. One live reviewer was settled twice for it, and each
        // settlement forced a fresh attempt that started from zero reads again -
        // the tighter the budget, the longer the loop.
        assert!(is_review_precondition_refusal(
            "review_requires_current_physical_reads:artifact:a"
        ));
        assert!(
            !is_review_content_refusal("review_requires_current_physical_reads:artifact:a"),
            "a precondition must not be counted as a content failure"
        );
        // Content failures keep the tight budget.
        assert!(is_review_content_refusal(
            "review physical attempt is stale or owns different results"
        ));
        assert!(!is_review_precondition_refusal(
            "review physical attempt is stale or owns different results"
        ));
        assert!(is_review_source_changed_retry(
            "review_requires_current_physical_reads:artifact:a"
        ));
        // Both classes stay bounded: neither may loop forever.
        const _: () = assert!(MAX_REVIEW_PRECONDITION_RETRIES > MAX_REVIEW_SOURCE_CHANGED_RETRIES);
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
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
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
            original_execution: None,
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
        let attempt_form = AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: "task:1".to_string(),
            deliverable: None,
            artifact_refs: vec![],
            evidence_refs: vec![format!("tool://tool-raw-chatcmpl-tool-aa0d514a557763e5:8")],
            unresolved: vec![],
        });
        let attempt_error = services
            .validate_agent_action_evidence(&actor, &attempt_form)
            .await
            .expect_err("an invented attempt suffix must not alias another receipt");
        assert!(
            attempt_error.contains("no canonical durable receipt"),
            "{attempt_error}"
        );
        assert!(
            !attempt_error.contains("points to missing durable content"),
            "{attempt_error}"
        );
    }
}

/// The logical-key fallback must not launder an unknown reference. A model
/// that cites `artifact://<24 hex>` for an artifact no Program holds is still
/// refused with the store's own reason, so its mistake stays visible instead
/// of becoming an unexplained read failure.
#[tokio::test]
async fn action_evidence_validation_still_refuses_an_unmapped_logical_key() {
    let services = crate::RuntimeServices::in_memory().expect("runtime");
    let actor = AgentActorBinding {
        original_execution: None,
        objective_id: "objective-1".into(),
        program_id: "program-1".into(),
        session_id: "session-1".into(),
        turn_id: "turn-1".into(),
        root_execution_id: Some("root-1".into()),
        required_team_count: 0,
        exact_team_count: None,
        objective_summary: "test objective".into(),
        model_lease: "test".into(),
        permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
        resource_scopes: vec![],
        actor_id: "agent:1".into(),
        kind: AgentActorKind::Agent,
        execution_id: None,
        team_id: None,
        agent_id: Some("agent:1".into()),
    };
    let action = AgentAction::TaskReview(harness_contract::agent_action::TaskReviewInput {
        operator_requirement: None,
        task_ref: "task:1".into(),
        decision: harness_contract::agent_action::TaskReviewDecision::Accept,
        reason: "checked".into(),
        evidence_refs: vec!["artifact://0123456789abcdef01234567".into()],
    });
    let error = services
        .validate_agent_action_evidence(&actor, &action)
        .await
        .expect_err("an unmapped logical key must never resolve");
    assert!(
        error.contains("points to missing durable content"),
        "the store's own reason must survive: {error}"
    );
}
