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

/// Created only by the original review-cut owner after its bound is exhausted.
pub(super) struct ReviewCutSettlement {
    program_id: String,
    task_ref: String,
    execution_id: String,
    node_id: String,
    attempt: u32,
    used: u32,
    ticket: super::review_evidence::ReviewRepinTicket,
}

impl ReviewCutSettlement {
    pub(super) fn graph_id(&self) -> &str { &self.execution_id }
    pub(super) fn node_id(&self) -> &str { &self.node_id }
    pub(super) fn attempt(&self) -> u32 { self.attempt }

    pub(super) fn validate_active(&self, program: &crate::AgenticProgramProjection) -> Result<(), &'static str> {
        let task = program.tasks.get(&self.task_ref).ok_or("review_cut_settlement_task_missing")?;
        let attempt = task.active_attempts.get(&self.execution_id).ok_or("review_cut_settlement_attempt_missing")?;
        if program.program_id != self.program_id
            || task.status != super::AgenticTaskStatus::Submitted
            || attempt.execution_id != self.execution_id || attempt.mode != AgentAttemptMode::Review
            || attempt.generation != task.review_generation
        {
            return Err("review_cut_settlement_attempt_changed");
        }
        Ok(())
    }

    pub(super) fn event_metadata(&self, fail: &AgentActionEnvelope) -> Result<serde_json::Value, &'static str> {
        let AgentAction::TaskAttemptFail(input) = &fail.action else { return Err("review_cut_settlement_wrong_action"); };
        if fail.actor.kind != AgentActorKind::Supervisor
            || fail.actor.actor_id != "runtime.program-supervisor"
            || fail.actor.program_id != self.program_id
            || fail.actor.execution_id.as_deref() != Some(self.execution_id.as_str())
            || input.execution_id != self.execution_id || input.task_ref != self.task_ref
            || input.mode != AgentAttemptMode::Review || self.node_id.is_empty() || self.attempt == 0
            || self.used <= MAX_REVIEW_CUT_REBUILDS || !self.ticket.movement().rebuildable()
            || self.ticket.attempt() != self.attempt
            || self.ticket.review_ref() != super::review_evidence::node_review_ref(&self.execution_id, &self.node_id)
        {
            return Err("review_cut_settlement_owner_mismatch");
        }
        Ok(serde_json::json!({"kind":"review_cut_exhausted_v1", "program_id":self.program_id,
            "task_ref":self.task_ref, "execution_id":self.execution_id, "node_id":self.node_id, "attempt":self.attempt,
            "rebuilds_used":self.used, "rebuild_bound":MAX_REVIEW_CUT_REBUILDS}))
    }
}

const MAX_REVIEW_PROGRAM_REBUILDS: u32 = 3;

// Test-only bridge for the graph owner's original pause/recovery/resume fixture.
#[cfg(test)]
pub(crate) fn gb3_settle_synthetic_old_cut_for_graph_test(
    store: std::sync::Arc<crate::RuntimeEventStore>, graph_id: &str, node_id: &str, attempt: u32,
) -> Result<AgentActionObservation, crate::AgentActionServiceError> {
    let service = crate::AgentActionService::new(std::sync::Arc::clone(&store))
        .with_graph_store(crate::ExecutionGraphStateStore::new(store));
    let mut program = crate::AgenticProgramProjection::empty("gb3-graph-test", "objective");
    program.session_id = "session".into();
    program.turn_id = "turn".into();
    let actor = runtime_supervisor_actor(&program, Some(graph_id.into()));
    let reason = "result metadata source changed during resolution; rebuild the review; source=stream expected=4 actual=5 phase=result_source_pin";
    let ticket = review_repin_ticket_for(graph_id, node_id, attempt, reason)
        .expect("synthetic review ticket fixture");
    let origin = ReviewCutSettlement { program_id:program.program_id, task_ref:"task".into(),
        execution_id:graph_id.into(), node_id:node_id.into(), attempt, used:3, ticket };
    let fail = AgentActionEnvelope { action_id:"gb3-old-attempt".into(), actor, expected_revision:None,
        action:AgentAction::TaskAttemptFail(TaskAttemptFailInput { task_ref:"task".into(),
            execution_id:graph_id.into(), mode:AgentAttemptMode::Review, reason:"window".into(), retryable:true }) };
    service.apply_runtime_supervisor_review_cut(&fail, &origin)
}

/// T8 bounds: how far an evidence refusal hint may read into the Session
/// journal to name a repair, and how many candidate receipts one
/// disambiguation hint may name. The scan is a hint-side read with no
/// acceptance meaning; the bounds keep a long Session's refusal short.
const MAX_EVIDENCE_HINT_RECEIPT_SCAN: usize = 512;
const MAX_EVIDENCE_HINT_CANDIDATES: usize = 8;

/// T1b budget: how many times one physical review ticket and attempt may re-drive
/// its own evidence build at a fresh source cut before the dispatch stops and
/// names the gap.
///
/// It is the one review-rebuild budget in the dispatch lane because it
/// buys a different thing. (032 K3: the retry-class streak budget this was
/// separate from is deleted by the capability ledger's adjudication - drift
/// and precondition refusals without a rebuild ticket settle through the
/// caller's typed path now, and this ticket ledger is the one review-rebuild
/// budget.) A rebuild consumes a named record the review evidence domain
/// hands over (the refused source, the cut it pinned, the head the store
/// reported) and re-reads at that head. Two is the bound the plan sets: one
/// rebuild proves the window can be reached, a second proves it is moving, and a
/// third is the loop the ledger used to call an unexplained wedge.
pub(super) const MAX_REVIEW_CUT_REBUILDS: u32 = 2;
/// Process-local rebuild bookkeeping keyed by the dispatching node's own
/// lineage `{graph}/{node}/{attempt}`: after the merge of 032 K3 (A6/A7/A8)
/// this is the one review-rebuild budget authority in the dispatch lane. The
/// retry-class streak ledger it used to mirror is deleted with the ledger's
/// adjudication; drift and precondition refusals without a rebuild ticket
/// now settle through the caller's existing typed path instead of a second
/// counter.
static REVIEW_CUT_REBUILDS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn is_review_program_advanced(error: &super::review_evidence::ReviewEvidenceError) -> bool {
    error.is_program_advanced()
}

fn bump_review_rebuild(key: &str) -> u32 {
    let mut rebuilds = REVIEW_CUT_REBUILDS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = rebuilds.entry(key.to_string()).or_insert(0);
    *entry = entry.saturating_add(1);
    *entry
}

fn clear_review_rebuild(key: &str) {
    REVIEW_CUT_REBUILDS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(key);
}

/// The dispatch identity a rebuild budget counts against: the physical node that
/// owns this review action. A Runtime-authored or root action without a physical
/// owner has no rebuild budget at all, so it keeps the path it has today.
fn review_rebuild_key_for(graph_id: &str, node_id: &str, attempt: u32) -> String {
    format!("{}/{attempt}", super::review_evidence::node_review_ref(graph_id, node_id))
}

fn review_rebuild_key(envelope: &AgentActionEnvelope) -> Option<String> {
    let physical = envelope.actor.original_execution.as_ref()?;
    Some(review_rebuild_key_for(
        &physical.graph_id,
        &physical.node_id,
        physical.attempt,
    ))
}

/// The named rebuild record behind a refusal, if this refusal carries one.
///
/// The grammar belongs to the review evidence domain (T1a), and it is strict: a
/// reason that does not match exactly is not a rebuild request. The dispatch half
/// therefore never grows a second parser - it asks for the ticket, and if the
/// sentence reached it wrapped by an outer layer it retries once from the first
/// occurrence of the sentence so a refusal is not silently demoted to today's
/// settle path. If T1a later exposes a search entry, this is the one place that
/// changes.
#[cfg(test)]
fn review_repin_ticket_for(
    graph_id: &str,
    node_id: &str,
    attempt: u32,
    reason: &str,
) -> Option<super::review_evidence::ReviewRepinTicket> {
    let review_ref = super::review_evidence::node_review_ref(graph_id, node_id);
    // Try the reason verbatim first, then the sentence sliced out of a wrapped
    // refusal. A missing slice is not an error: it only means there is nothing
    // further to try.
    let candidates = [Some(reason.to_string()), own_pin_sentence(reason)]
        .into_iter()
        .flatten();
    for candidate in candidates {
        if let Some(ticket) =
            super::review_evidence::ReviewRepinTicket::from_refusal(&review_ref, attempt, &candidate)
        {
            return Some(ticket);
        }
    }
    None
}

fn review_repin_ticket(
    envelope: &AgentActionEnvelope,
    error: &super::review_evidence::ReviewEvidenceError,
) -> Option<super::review_evidence::ReviewRepinTicket> {
    let physical = envelope.actor.original_execution.as_ref()?;
    error.review_ticket(&super::review_evidence::node_review_ref(&physical.graph_id, &physical.node_id), physical.attempt)
}

/// Slice out the pin sentence a wrapped refusal still carries verbatim. Anything
/// that is not one of the two shapes yields `None`.
#[cfg(test)]
fn own_pin_sentence(reason: &str) -> Option<String> {
    // 032 K4b (A2): the two pin shapes are recognised by their own codes; the
    // slice is taken from whatever sentence projection carries them. Every
    // producer of these two refusals emits the full "rebuild the review;
    // source=" continuation, so code-matched starts coincide with the old
    // prefix-matched ones on the whole corpus (pinned by the dispatch tests).
    for prefix in [
        crate::agentic::review_evidence::reason_code::ReviewRefusalCode::ResultSourceMovedBackwardsDuringResolution
            .sentence_lead(),
        crate::agentic::review_evidence::reason_code::ReviewRefusalCode::ResultSourceChangedDuringResolution
            .sentence_lead(),
    ] {
        if let Some(start) = reason.find(prefix) {
            let tail = &reason[start..];
            let end = tail
                .find(" phase=result_source_pin")
                .map(|end| end + " phase=result_source_pin".len())
                .unwrap_or(tail.len());
            return Some(tail[..end].to_string());
        }
    }
    None
}

/// The one spelling of the rebuild bound.
///
/// Both review-window sentences used to word the bound independently, and they
/// drifted (`bounded 2` versus `2 bounded`), which is exactly how a wedge came to
/// read as two different classes depending on which sentence a consumer looked
/// at. One generator, one token: a consumer can match the same literal in the
/// caller-facing reason and in the durable settlement reason.
fn review_rebuild_budget_token() -> String {
    format!("bounded {MAX_REVIEW_CUT_REBUILDS}")
}

/// The terminal reason once the rebuild budget is spent. It names the budget and
/// the ticket's own facts so the durable event carries the cause rather than a
/// batch-level wrapper, and it is deliberately *not* an attempt failure: the
/// window nobody could settle is Unknown, and the dispatch must not re-drive the
/// old effect on the strength of a refused read.
fn review_rebuild_unknown_reason(
    ticket: &super::review_evidence::ReviewRepinTicket,
    used: u32,
    reason: &str,
) -> String {
    format!(
        "review_window_unknown: review cut rebuild exhausted after {used} attempts at {budget}; \
         ticket review_ref={review_ref} attempt={attempt} source={source} \
         pinned_cut={pinned_cut} observed_head={observed_head} movement={movement:?}; \
         last refusal: {reason}",
        budget = review_rebuild_budget_token(),
        review_ref = ticket.review_ref(),
        attempt = ticket.attempt(),
        source = ticket.source(),
        pinned_cut = ticket.pinned_cut(),
        observed_head = ticket.observed_head(),
        movement = ticket.movement(),
    )
}

/// The durable settlement reason for an exhausted rebuild budget.
///
/// It is deliberately *stable* per review lineage: the Task reducer treats an
/// identical `last_failure` as "no new fact" and blocks the Task instead of
/// handing the same refused read another dispatch. The varying evidence (the
/// cut pinned, the head the store reported, the refusal sequence) travels in
/// the caller-facing reason above, which lands in this node's own transcript,
/// while the journal keeps one classifiable sentence per wedge.
fn review_rebuild_settlement_reason(ticket: &super::review_evidence::ReviewRepinTicket) -> String {
    format!(
        "review_window_unknown: review {review_ref} attempt {attempt} source {source} \
         cut rebuilds exhausted at {budget} (grammar result_source_pin)",
        budget = review_rebuild_budget_token(),
        review_ref = ticket.review_ref(),
        attempt = ticket.attempt(),
        source = ticket.source(),
    )
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
                None, &[],
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
                    Ok(Some(access)) => {
                        // The legacy call-id form resolves, but it pins no
                        // content: a submission has to name the full canonical
                        // receipt. The form stays readable - the refusal names
                        // the canonical receipt, so the model repairs in one
                        // round instead of guessing variants.
                        if self.cited_receipt_is_legacy(&actor.session_id, evidence_id).await {
                            let hint = self
                                .canonical_tool_evidence_hint(&actor.session_id, evidence_id)
                                .await;
                            let mut message = format!(
                                "{} evidence {evidence_ref} is the legacy call-id form, which pins no content: name the full canonical receipt (tool-raw-<call>-<content digest>). code={}",
                                action.kind(),
                                harness_contract::refusal_codes::EvidenceRefusalCode::LegacyCallOnlyForm
                                    .code()
                            );
                            if let Some(hint) = hint {
                                message.push_str(&format!(". {hint}"));
                            }
                            unresolvable.push(message);
                            continue;
                        }
                        access
                    }
                    Ok(None) => {
                        match self
                            .resolve_tool_evidence(
                                &actor.session_id,
                                evidence_id,
                                action.kind(),
                            )
                            .await
                        {
                            ToolEvidenceResolution::Resolved(access) => access,
                            ToolEvidenceResolution::Refused(message) => {
                                unresolvable.push(message);
                                continue;
                            }
                        }
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

    /// The repair hint for a cited `tool://` receipt that resolved to nothing.
    ///
    /// The original leg is unchanged: the stable call-id prefix still selects
    /// exactly one recorded receipt (an ambiguous prefix resolves to nothing)
    /// and its sentence is produced character for character. Two further legs
    /// answer the shapes the prefix could not, as measured on the live
    /// escalation - a call id that drifted by one character earned zero hints
    /// across the whole Session, and one content suffix is legitimately
    /// durable under several call ids. Drift within one edit is named as a
    /// suggestion; a shared content suffix lists its candidate receipts by
    /// name instead of picking one. Every leg can only hand back ids Runtime
    /// durably recorded: it can never invent an id, rewrite the citation, or
    /// turn the refusal into acceptance - the caller's action is still
    /// rejected on the same path as before.
    async fn canonical_tool_evidence_hint(
        &self,
        session_id: &str,
        evidence_id: &str,
    ) -> Option<String> {
        let recorded_unique = match evidence_id.rsplit_once('-') {
            Some((call_id, _suffix)) if !call_id.is_empty() => self
                .session_evidence_access(session_id, call_id)
                .await
                .ok()
                .flatten()
                .map(|access| access.evidence_ref.id),
            _ => None,
        };
        if recorded_unique.is_some() {
            return tool_evidence_hint(evidence_id, recorded_unique.as_deref(), &[]);
        }
        let receipts = self
            .session_evidence_receipt_ids(session_id, MAX_EVIDENCE_HINT_RECEIPT_SCAN)
            .await
            .unwrap_or_default();
        tool_evidence_hint(evidence_id, None, &receipts)
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
        self.submit_agent_action_from_owner(envelope, false, false, Some((capability, tool, original_input)), None, None)
            .await.map_err(crate::AgentActionServiceError::EventStore)?.into_single_typed()
    }

    /// Whether the cited id is the legacy call-id form.
    ///
    /// The legacy form resolves - the call's own entry is recorded - but it
    /// pins no content: two receipts of the same call could be behind it. A
    /// submission has to name the full canonical receipt, and the refusal names
    /// it, so the model repairs in one round. The form stays readable
    /// everywhere else.
    async fn cited_receipt_is_legacy(&self, session_id: &str, cited: &str) -> bool {
        let recorded = self
            .session_evidence_receipt_ids(session_id, MAX_EVIDENCE_HINT_RECEIPT_SCAN)
            .await
            .unwrap_or_default();
        if recorded.iter().any(|receipt| receipt == cited) {
            // It is a recorded receipt: the canonical form.
            return false;
        }
        recorded
            .iter()
            .any(|receipt| receipt.len() > cited.len() && receipt.starts_with(cited))
    }

    /// Resolve one cited tool receipt at the submission surface.
    ///
    /// A citation that names no receipt is not always a wrong citation: a model
    /// writes a hex character, a separator or the end of a content suffix
    /// differently, and the receipt it meant is already recorded. The repair
    /// names exactly one receipt, and the content is verified against that
    /// receipt before it is accepted - so the repair moves a citation to the
    /// receipt it was aiming at and never invents one.
    ///
    /// A citation that matches more than one receipt is refused by name: the
    /// model gets the candidates and the whole call, and Runtime does not
    /// choose.
    async fn resolve_tool_evidence(
        &self,
        session_id: &str,
        cited: &str,
        kind: &str,
    ) -> ToolEvidenceResolution {
        let recorded = self
            .session_evidence_receipt_ids(session_id, MAX_EVIDENCE_HINT_RECEIPT_SCAN)
            .await
            .unwrap_or_default();
        match repair_receipt_citation(cited, &recorded) {
            EvidenceCitation::Canonical(canonical) => {
                if canonical != cited {
                    let hint = self.canonical_tool_evidence_hint(session_id, cited).await;
                    return ToolEvidenceResolution::Refused(unrepaired_evidence_refusal(
                        kind, cited, session_id, hint,
                    ));
                }
                match self.session_evidence_access(session_id, &canonical).await {
                    Ok(Some(access)) => ToolEvidenceResolution::Resolved(access),
                    _ => {
                        let hint = self.canonical_tool_evidence_hint(session_id, cited).await;
                        ToolEvidenceResolution::Refused(unrepaired_evidence_refusal(
                            kind, cited, session_id, hint,
                        ))
                    }
                }
            }
            EvidenceCitation::Ambiguous(candidates) => {
                // The ambiguity has one class in one ledger: the typed payload
                // is rendered into the prose, so the sentence a model reads and
                // the record a reader parses cannot drift apart.
                let ambiguity = harness_contract::reference_ambiguity::AmbiguousReference::new(
                    cited,
                    candidates,
                );
                ToolEvidenceResolution::Refused(format!(
                    "{kind} evidence tool://{cited} matches {} recorded receipts in Session {session_id} ({}); cite the full tool:// reference of the intended completed tool call. diagnostics={}",
                    ambiguity.candidate_count(),
                    name_receipt_candidates(
                        &ambiguity
                            .candidates()
                            .iter()
                            .map(String::as_str)
                            .collect::<Vec<_>>()
                    ),
                    serde_json::to_string(&ambiguity.payload_capped(MAX_EVIDENCE_HINT_CANDIDATES))
                        .unwrap_or_default(),
                ))
            }
            EvidenceCitation::Unrepairable => {
                let hint = self.canonical_tool_evidence_hint(session_id, cited).await;
                ToolEvidenceResolution::Refused(unrepaired_evidence_refusal(
                    kind, cited, session_id, hint,
                ))
            }
        }
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
            Some(OriginalIngressMember::Prepared(capability)), None).await {
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
        self.submit_agent_action_from_owner(envelope, false, true, None, None, None).await?.into_single()
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
        review_cut: Option<&ReviewCutSettlement>,
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
                        // A verdict settles the window question for this ticket:
                        // the rebuild budget is bookkeeping for an in-flight
                        // wedge, not a score to carry into the next attempt.
                        if let Some(budget_key) = review_rebuild_key(envelope) {
                            clear_review_rebuild(&budget_key);
                        }
                        break Some(review);
                    }
                    Err(error) if is_review_program_advanced(&error) => {
                        rebuilds += 1;
                        if rebuilds > MAX_REVIEW_PROGRAM_REBUILDS {
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
                        if let Some(ticket) = review_repin_ticket(envelope, &error) {
                            if ticket.movement().rebuildable() {
                                let budget_key = review_rebuild_key(envelope)
                                    .unwrap_or_else(|| ticket.lineage().0.to_string());
                                let used = bump_review_rebuild(&budget_key);
                                if used <= MAX_REVIEW_CUT_REBUILDS {
                                    // T1b: one bounded rebuild of the same review
                                    // task at a fresh cut. The evidence build pins
                                    // the head the store reports now, so the
                                    // re-read is against observed material rather
                                    // than a wider claimed window, and the T1a
                                    // discipline (growth inside the new window is
                                    // refused again, never absorbed) still applies.
                                    program = actions
                                        .project(&envelope.actor.program_id)
                                        .map_err(|projection_error| projection_error.to_string())?;
                                    continue;
                                }
                                clear_review_rebuild(&budget_key);
                                // The window could not be reached inside the
                                // bounded rebuilds. Name it as Unknown and let
                                // the existing attempt reducer close it: this is
                                // a scheduling checkpoint, not a re-drive of the
                                // refused read, and the stable settlement reason
                                // makes a repeat wedge block instead of dispatch
                                // the same refused read again.
                                let settlement = self
                                    .settle_wedged_review_attempt(
                                        envelope,
                                        &program,
                                        &review_rebuild_settlement_reason(&ticket),
                                        &ticket,
                                        used,
                                    )
                                    .await;
                                let unknown = review_rebuild_unknown_reason(&ticket, used, &error.to_string());
                                return Err(match settlement {
                                    Ok(()) => unknown,
                                    Err(settle_error) => format!(
                                        "{unknown}; runtime review-window settlement failed: {settle_error}"
                                    ),
                                });
                            }
                            // A head that moved backwards means the material the
                            // reviewer read may no longer exist. That is not a
                            // rebuildable window, so it keeps its original
                            // refusal and today's settlement path untouched.
                        }
                        // 032 K3 (A6/A7/A9 removal, ledger-adjudicated): no
                        // second retry counter. A refusal that is neither a
                        // ticket-carrying rebuild (above) nor a Program move
                        // (A4, pending K4) returns unchanged here; the
                        // caller's existing typed settlement closes the
                        // attempt, and `failed_review_attempts` bounds it.
                        return Err(error.to_string());
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
        // The named failures of the required writes this review settled as
        // failed. They travel beside the proof, never inside it.
        let required_write_failures: &[serde_json::Value] = review
            .as_ref()
            .map(|review| review.required_write_failures.as_slice())
            .unwrap_or(&[]);
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
                        let applied = actions.apply_member_locked(envelope, goal, verification, required_write_failures, sources, held, control,
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
                    let result = actions.apply_single_locked(envelope, goal, verification, required_write_failures, capability, tool, input, original);
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
                let result = actions.apply_single_locked(envelope, goal, verification, required_write_failures, capability, tool, input, &original);
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
                    required_write_failures,
                )
                .map_err(|error| error.to_string())
        } else if let Some(verification) = verification {
            actions
                .apply_verified_review(envelope, verification, required_write_failures)
                .map_err(|error| error.to_string())
        } else if internal_completion {
            let graph = envelope.actor.root_execution_id.as_deref().ok_or("original root completion graph unavailable")?;
            match self.original_graph_registration_owner(graph, &envelope.actor)? {
                Some(owner) => actions.apply_runtime_root_completion_owned(envelope, &owner),
                None => actions.apply_runtime_root_completion(envelope),
            }.map_err(|error| error.to_string())
        } else if internal_supervisor && matches!(&envelope.action, AgentAction::TaskAttemptDispatch(_)) {
            let program = actions.project(&envelope.actor.program_id).map_err(|error| error.to_string())?;
            let owner = self.original_conclusion_owner(&program)
                .ok_or("original Session supervisor dispatch owner unavailable")?;
            self.with_conclusion_policy(&owner, |_| {
                let turn = harness_contract::turn::TurnId::from_string(owner.claim.turn_id());
                if owner.input.active_turn_id().as_ref() != Some(&turn) {
                    return Err("original Session active turn changed".into());
                }
                let fence = owner.claim.fence()?;
                let cursor = owner.input.highest_consumed_cursor(&turn);
                if owner.input.active_turn_id().as_ref() != Some(&turn) {
                    return Err("original Session active turn changed while reading cursor".into());
                }
                let consumed = match cursor {
                    Some(cursor) if cursor.generation == fence.session_generation =>
                        usize::try_from(cursor.sequence).map_err(|_| "original consumed input sequence overflow")?.max(fence.input_sequence),
                    Some(_) => return Err("original consumed cursor generation mismatch".into()),
                    None => fence.input_sequence,
                };
                actions.apply_runtime_supervisor_dispatch_owned(envelope, &owner, consumed)
                    .map_err(|error| error.to_string())
            })
        } else if internal_supervisor {
            match review_cut {
                Some(origin) => actions.apply_runtime_supervisor_review_cut(envelope, origin),
                None => actions.apply_runtime_supervisor(envelope),
            }.map_err(|error| error.to_string())
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
        let observation = self.submit_agent_action_from_owner(&envelope, true, false, None, None, None).await?.into_single()?;
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
        ticket: &super::review_evidence::ReviewRepinTicket,
        used: u32,
    ) -> Result<(), String> {
        let AgentAction::TaskReview(input) = &envelope.action else {
            return Ok(());
        };
        let execution_id = envelope
            .actor
            .execution_id
            .clone()
            .ok_or("review attempt settlement requires the reviewing execution id")?;
        let physical = envelope.actor.original_execution.as_ref().ok_or("review cut settlement physical owner missing")?;
        if used <= MAX_REVIEW_CUT_REBUILDS || !ticket.movement().rebuildable()
            || ticket.review_ref() != super::review_evidence::node_review_ref(&physical.graph_id, &physical.node_id)
            || ticket.attempt() != physical.attempt || execution_id != physical.graph_id
            || program.program_id != envelope.actor.program_id
        {
            return Err("review cut settlement original ticket mismatch".into());
        }
        let origin = ReviewCutSettlement {
            program_id: program.program_id.clone(), task_ref: input.task_ref.clone(),
            execution_id: execution_id.clone(), node_id: physical.node_id.clone(), attempt: physical.attempt,
            used, ticket: ticket.clone(),
        };
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
        let observation = Box::pin(self.submit_agent_action_from_owner(&fail, false, true, None, None, Some(&origin)))
            .await?.into_single()?;
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

/// Split a receipt reference into the stable call-id part and a trailing
/// content suffix shaped like the one Runtime records: at least eight hex
/// characters after the last `-`. Everything before that `-` is the call id,
/// matching the split the original hint leg already used; without a suffix
/// the whole reference is the call id.
fn receipt_id_parts(reference: &str) -> (&str, Option<&str>) {
    match reference.rsplit_once('-') {
        Some((call, suffix))
            if suffix.len() >= 8 && suffix.chars().all(|c| c.is_ascii_hexdigit()) =>
        {
            (call, Some(suffix))
        }
        Some((call, _suffix)) => (call, None),
        None => (reference, None),
    }
}

/// The refusal for a citation that names no recorded receipt.
fn unrepaired_evidence_refusal(
    kind: &str,
    cited: &str,
    session_id: &str,
    hint: Option<String>,
) -> String {
    let mut message = format!(
        "{kind} evidence tool://{cited} has no canonical durable receipt in Session {session_id}; use a tool:// reference returned by a completed tool call in this Session. code={}",
        harness_contract::refusal_codes::EvidenceRefusalCode::UnrepairedCitation.code()
    );
    if let Some(hint) = hint {
        message.push_str(&format!(". {hint}"));
    }
    message
}

/// What a repair of one cited receipt found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EvidenceCitation {
    /// Exactly one recorded receipt matches the citation under a repairable
    /// shape: the server can name the canonical receipt itself.
    Canonical(String),
    /// More than one recorded receipt matches: the citation does not name one
    /// receipt, and guessing would attach the wrong content.
    Ambiguous(Vec<String>),
    /// No recorded receipt matches.
    Unrepairable,
}

/// The canonical receipt a citation names, repairing the three shapes a model is
/// known to write.
///
/// The shapes are measured, not imagined: a hex character slipped in the
/// content suffix, a separator drifted between the call parts, and the content
/// suffix truncated at either end - one live run cited `tool-raw-...-6` for the
/// durable `tool-raw-...-2c77431438342406`. The call id still has to match:
/// the repair moves a citation to the receipt it was aiming at, it never
/// invents a receipt.
///
/// A citation that matches more than one receipt is ambiguous, and an ambiguous
/// citation is refused by name rather than resolved to a guess.
pub(crate) fn repair_receipt_citation(
    cited: &str,
    recorded: &[String],
) -> EvidenceCitation {
    let (cited_call, cited_suffix) = match cited.rsplit_once('-') {
        Some((call, suffix)) => (call, Some(suffix)),
        None => (cited, None),
    };
    // The legacy call-id form pins no content: repairing it would be choosing
    // which receipt the model meant.
    let Some(cited_suffix) = cited_suffix else {
        return EvidenceCitation::Unrepairable;
    };
    let candidates = recorded
        .iter()
        .filter(|receipt| {
            let (recorded_call, recorded_digest) = receipt_id_parts(receipt);
            let Some(recorded_digest) = recorded_digest else {
                return false;
            };
            call_parts_match(cited_call, recorded_call)
                && digest_parts_match(cited_suffix, recorded_digest)
        })
        .cloned()
        .collect::<Vec<_>>();
    match candidates.len() {
        0 => EvidenceCitation::Unrepairable,
        1 => EvidenceCitation::Canonical(candidates[0].clone()),
        _ => EvidenceCitation::Ambiguous(candidates),
    }
}

/// The call parts of two citations match when they are equal, when only their
/// separators drifted, or when one is a single edit away from the other.
fn call_parts_match(cited: &str, recorded: &str) -> bool {
    cited == recorded
        || cited.replace('_', "-") == recorded.replace('_', "-")
        || within_one_edit(cited, recorded)
}

/// The content suffixes of two citations match when they are equal, when one is
/// the other truncated at either end, or when a single hex character slipped.
fn digest_parts_match(cited: &str, recorded: &str) -> bool {
    cited == recorded
        || cited.starts_with(recorded)
        || recorded.starts_with(cited)
        || cited.ends_with(recorded)
        || recorded.ends_with(cited)
        || hex_one_apart(cited, recorded)
}

/// One hex character differs between two equal-length hex strings.
fn hex_one_apart(left: &str, right: &str) -> bool {
    if left.len() != right.len() || left.is_empty() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .filter(|(left, right)| left != right)
        .count()
        == 1
        && left
            .bytes()
            .zip(right.bytes())
            .all(|(left, right)| left.is_ascii_hexdigit() && right.is_ascii_hexdigit())
}

/// The outcome of resolving one cited tool receipt at the submission surface.
pub(crate) enum ToolEvidenceResolution {
    /// The receipt resolved - exactly, or by a repair that named one receipt.
    Resolved(harness_contract::context::EvidenceAccessRef),
    /// The citation does not pin one durable receipt; the refusal says which
    /// shape it was.
    Refused(String),
}

/// Levenshtein distance at most one between two call ids: one substituted,
/// inserted, or deleted character - the drift shape the escalation ledger
/// measured. An exact match is deliberately not drift: the resolution legs
/// own that case, so a correction hint never suggests a self-rewrite.
/// Receipt call ids are ASCII, so byte indexing is character indexing.
fn within_one_edit(cited: &str, recorded: &str) -> bool {
    match cited.len().abs_diff(recorded.len()) {
        0 => cited
            .bytes()
            .zip(recorded.bytes())
            .filter(|(left, right)| left != right)
            .count()
            == 1,
        1 => {
            let (short, long) = if cited.len() < recorded.len() {
                (cited, recorded)
            } else {
                (recorded, cited)
            };
            let common = short
                .bytes()
                .zip(long.bytes())
                .take_while(|(left, right)| left == right)
                .count();
            short.as_bytes()[common..] == long.as_bytes()[common + 1..]
        }
        _ => false,
    }
}

/// Name candidate receipts, bounded so one refusal cannot sprawl.
fn name_receipt_candidates(candidates: &[&str]) -> String {
    let shown = candidates.len().min(MAX_EVIDENCE_HINT_CANDIDATES);
    let mut names = candidates[..shown]
        .iter()
        .map(|id| format!("tool://{id}"))
        .collect::<Vec<_>>()
        .join(", ");
    if candidates.len() > shown {
        names.push_str(&format!(" (and {} more)", candidates.len() - shown));
    }
    names
}

/// The hint decision itself, split from the journal reads so every shape the
/// refusal can name is directly testable. Leg order: the receipt the call-id
/// prefix resolved (the original sentence, kept character for character),
/// then one-edit call-id drift, then a content suffix recorded under
/// several call ids. A single near neighbour is named as "appears to be";
/// several, or a suffix shared by several, list the candidates instead of
/// guessing. Nothing here rewrites the citation or resolves anything - the
/// refusal lands on the same unresolvable path with one more sentence.
///
/// The drift leg binds the content suffix as well as the call id (R48.3,
/// taken as 甲): resolution of a named receipt does not require the receipt
/// to belong to the citing call (`ingress.rs:569-586` checks content
/// integrity of whatever `tool://` id it is handed), so a candidate within
/// one edit on the call id but a different content suffix could point the
/// model at another call's provenance. Requiring `recorded digest ==
/// cited digest` keeps the suggestion honest: the escalation shape - one
/// character adrift with the suffix intact - still gets corrected, and an
/// unrelated near neighbour without the same content earns nothing.
fn tool_evidence_hint(
    cited: &str,
    recorded_unique: Option<&str>,
    receipts: &[String],
) -> Option<String> {
    if let Some(canonical) = recorded_unique {
        return (canonical != cited).then(|| {
            format!(
                "The durable receipt Runtime recorded for this tool call is tool://{canonical}; copy that exact reference"
            )
        });
    }
    let (cited_call, cited_digest) = receipt_id_parts(cited);
    if cited_call.is_empty() {
        return None;
    }
    let near: Vec<&str> = receipts
        .iter()
        .map(String::as_str)
        .filter(|id| {
            let (recorded_call, recorded_digest) = receipt_id_parts(id);
            cited_digest == recorded_digest
                && within_one_edit(cited_call, recorded_call)
        })
        .collect();
    match near.len() {
        1 => {
            let candidate = near[0];
            let (recorded_call, _) = receipt_id_parts(candidate);
            Some(format!(
                "No receipt matches the cited reference, but the recorded call id {recorded_call} is one edit away from the cited one; the durable receipt appears to be tool://{candidate}; copy that exact reference"
            ))
        }
        0 => {
            let cited_digest = cited_digest?;
            let shared: Vec<&str> = receipts
                .iter()
                .map(String::as_str)
                .filter(|id| receipt_id_parts(id).1 == Some(cited_digest))
                .collect();
            (shared.len() >= 2).then(|| {
                // The ambiguity is typed, not only reported. The prose a model
                // reads is rendered from the same typed payload the durable
                // record carries, so the two cannot drift apart, and a reader
                // downstream can tell this refusal apart from every other
                // unresolvable reference instead of scraping prose for it.
                let ambiguity =
                    harness_contract::reference_ambiguity::AmbiguousReference::new(
                        cited,
                        shared.iter().map(|id| (*id).to_string()).collect(),
                    );
                format!(
                    "No receipt matches the cited reference; the cited content suffix is recorded under {} durable receipts in this Session: {}; cite the full tool:// reference returned by the intended completed tool call; diagnostics={}",
                    ambiguity.candidate_count(),
                    name_receipt_candidates(&shared),
                    serde_json::to_string(&ambiguity.payload_capped(MAX_EVIDENCE_HINT_CANDIDATES))
                        .unwrap_or_default(),
                )
            })
        }
        _ => Some(format!(
            "No receipt matches the cited reference; {} recorded call ids are within one edit of the cited one: {}; cite the full tool:// reference returned by the intended completed tool call",
            near.len(),
            name_receipt_candidates(&near)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent_action::{
        MessagePublishInput, TaskReviewDecision, TaskReviewInput, TeamCreateInput,
    };

    #[test]
    fn p2_untrusted_result_reference_cannot_mint_source_move_ticket() {
        let error = "review result has no Runtime-resolved producer:result metadata source changed during resolution; rebuild the review; source=forged expected=4 actual=5 phase=result_source_pin";
        let error = super::super::review_evidence::ReviewEvidenceError::from(error);
        assert!(error.review_ticket("graph/node", 1).is_none(),
            "an ordinary error containing a model-controlled reference is not a producer pin");
    }

    #[test]
    fn p2_untrusted_result_reference_cannot_consume_program_rebuilds() {
        let error = "review result has no Runtime-resolved producer:result metadata changed after original effect enumeration";
        assert!(!is_review_program_advanced(&error.into()),
            "a model-controlled reference is not an observed Program movement");
    }

    // Real graph and settlement owner; the earlier Program journal is a
    // synthetic fixture. This is not a concurrent attempt-switch control.
    #[test]
    fn gb3_stale_cut_ticket_does_not_settle_another_physical_attempt() {
        use crate::execution_core::graph::{ExecutionCommitService, ExecutionGraphStateStore, ExecutionNodeBinding, NodeExecutionTicket};
        use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus};
        use std::{collections::BTreeMap, sync::Arc};
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let commits = ExecutionCommitService::new(Arc::clone(&store));
        let mut graph = ExecutionGraph::new("GB3 physical attempt fence").with_lineage(ExecutionGraphLineage {
            session_id:"session".into(), turn_id:"turn".into(), root_task_id:"task".into(), task_id:"task".into(), generation:1,
        });
        graph.nodes.push(ExecutionNodeSpec { id: "node".into(), kind: ExecutionNodeKind::InlineModel,
            payload_ref: "payload".into(), executor_kind: "inline_model".into(), idempotency_key: "gb3-attempt".into(),
            lease_ref: None, acceptance: Default::default(), retry_policy: Default::default(), resource_scopes: Vec::new(), work: None });
        let graph = commits.register_graph(graph).unwrap().graph;
        let graph = commits.transition_node(&graph, "node", ExecutionNodeStatus::Ready, None, Vec::new()).unwrap().graph;
        let binding = ExecutionNodeBinding { executor_kind: "inline_model".into(), ticket_idempotency_key: "gb3-attempt".into(),
            attempt: 1, resource_lease_refs: Vec::new(), scope_lease_ref: None, worktree_lease_ref: None };
        let ticket = NodeExecutionTicket { graph_id: graph.id.clone(), node_id: "node".into(), executor_kind: "inline_model".into(),
            service_class: graph.service_class, attempt: 1, idempotency_key: "gb3-attempt".into(), payload_ref: "payload".into() };
        let graph = commits.bind_and_start_nodes(&graph, BTreeMap::from([("node".into(), binding)]), vec![ticket]).unwrap().graph;
        let service = crate::AgentActionService::new(Arc::clone(&store)).with_graph_store(ExecutionGraphStateStore::new(Arc::clone(&store)));
        // Retained Program journal is a synthetic fixture; graph ownership and
        // the settlement call below use their production owners.
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        program.session_id = "session".into();
        program.turn_id = "turn".into();
        let actor = runtime_supervisor_actor(&program, Some(graph.id.clone()));
        let publish: harness_contract::agent_action::TaskPublishInput = serde_json::from_value(serde_json::json!({
            "team_ref":"team", "title":"task", "objective":"read", "acceptance":"cite", "required_capabilities":[]
        })).unwrap();
        let actions = [
            AgentAction::TaskPublish(publish),
            AgentAction::TaskSubmit(harness_contract::agent_action::TaskSubmitInput {
                task_ref:"task".into(), artifact_refs:Vec::new(), evidence_refs:Vec::new(), unresolved:Vec::new(), deliverable:None,
            }),
            AgentAction::TaskAttemptDispatch(harness_contract::agent_action::TaskAttemptDispatchInput {
                task_ref:"task".into(), execution_id:graph.id.clone(), agent_ref:"reviewer".into(), membership_id:"member".into(),
                mode:AgentAttemptMode::Review, generation:0,
            }),
        ];
        let stream = "agentic-program:program";
        let mut events = vec![crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput { stream_id:stream.into(), scope:crate::RuntimeEventScope::Program,
                kind:"agentic.program_opened".into(), status:Some("open".into()), actor:Some("test".into()), refs:Vec::new(),
                payload:serde_json::json!({"program_id":"program", "objective_id":"objective", "session_id":"session", "turn_id":"turn"}) },
            idempotency_key:Some("gb3-program-open".into()), schema_version:1,
        }];
        for (index, action) in actions.into_iter().enumerate() {
            let envelope = AgentActionEnvelope { action_id:format!("gb3-seed-{index}"), actor:actor.clone(), expected_revision:None, action };
            events.push(crate::RuntimeTransactionEventInput { event:crate::RuntimeEventInput {
                stream_id:stream.into(), scope:crate::RuntimeEventScope::Program, kind:"agentic.action_applied".into(),
                status:Some("applied".into()), actor:Some("test".into()), refs:Vec::new(),
                payload:serde_json::json!({"envelope":envelope, "entity_ref":"task"}) },
                idempotency_key:Some(format!("gb3-seed-{index}")), schema_version:1 });
        }
        store.append_transaction(crate::AppendTransactionRequest { transaction_id:"gb3-seed-program".into(),
            expected_streams:vec![crate::ExpectedStreamRevision { stream_id:stream.into(), expected_revision:0 }], events }).unwrap();
        let reason = "result metadata source changed during resolution; rebuild the review; source=stream expected=4 actual=5 phase=result_source_pin";
        let ticket = review_repin_ticket_for(&graph.id, "node", 2, reason).unwrap();
        let origin = ReviewCutSettlement { program_id: "program".into(), task_ref: "task".into(), execution_id: graph.id.clone(),
            node_id: "node".into(), attempt: 2, used: 3, ticket };
        let fail = AgentActionEnvelope { action_id: "gb3-stale".into(), actor, expected_revision: None,
            action: AgentAction::TaskAttemptFail(TaskAttemptFailInput { task_ref: "task".into(), execution_id: graph.id.clone(),
                mode: AgentAttemptMode::Review, reason: "window".into(), retryable: true }) };
        let program_stream = "agentic-program:program";
        let before = store.stream_revision(program_stream).unwrap();
        let error = service.apply_runtime_supervisor_review_cut(&fail, &origin).unwrap_err();
        assert!(error.to_string().contains("review_cut_original_attempt_changed"), "{error}");
        assert_eq!(store.stream_revision(program_stream).unwrap(), before);
        assert_eq!(store.stream_revision(&graph.id).unwrap(), graph.revision);
        let ticket = review_repin_ticket_for(&graph.id, "node", 1, reason).unwrap();
        let origin = ReviewCutSettlement { attempt:1, ticket, ..origin };
        let applied = service.apply_runtime_supervisor_review_cut(&fail, &origin).unwrap();
        assert_eq!(applied.status, harness_contract::agent_action::AgentActionStatus::Applied);
        let committed_revision = store.stream_revision(program_stream).unwrap();
        assert_eq!(committed_revision, before + 1);
        let replay = service.apply_runtime_supervisor_review_cut(&fail, &origin).unwrap();
        assert!(replay.duplicate);
        assert_eq!(store.stream_revision(program_stream).unwrap(), committed_revision);
        let projected = service.project("program").unwrap();
        assert_eq!(projected.tasks["task"].structural_review_stalls, 1);
        assert_eq!(projected.tasks["task"].failed_review_attempts, 0);
    }

    #[test]
    fn gb3_cut_origin_requires_active_original_review_and_exhausted_ticket() {
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        let actor = runtime_supervisor_actor(&program, Some("graph".into()));
        let publish: harness_contract::agent_action::TaskPublishInput = serde_json::from_value(serde_json::json!({
            "team_ref":"team", "title":"task", "objective":"read", "acceptance":"cite", "required_capabilities":[]
        })).unwrap();
        let env = AgentActionEnvelope { action_id:"publish".into(), actor:actor.clone(), expected_revision:None, action:AgentAction::TaskPublish(publish.clone()) };
        super::super::work_market::apply_task_publish(&mut program, &env, &publish, Some("task"));
        let task = program.tasks.get_mut("task").unwrap();
        task.status = super::super::AgenticTaskStatus::Submitted;
        task.review_generation = 4;
        task.active_attempts.insert("graph".into(), super::super::AgenticTaskAttemptProjection {
            execution_id:"graph".into(), agent_id:"agent".into(), membership_id:"member".into(), mode:AgentAttemptMode::Review, generation:4,
        });
        let reason = "result metadata source changed during resolution; rebuild the review; source=stream expected=4 actual=5 phase=result_source_pin";
        let ticket = review_repin_ticket_for("graph", "node", 1, reason).unwrap();
        let mut origin = ReviewCutSettlement { program_id:"program".into(), task_ref:"task".into(), execution_id:"graph".into(), node_id:"node".into(), attempt:1, used:3, ticket };
        let mut fail = AgentActionEnvelope { action_id:"settle".into(), actor, expected_revision:None,
            action:AgentAction::TaskAttemptFail(TaskAttemptFailInput { task_ref:"task".into(), execution_id:"graph".into(), mode:AgentAttemptMode::Review, reason:"window".into(), retryable:true }) };
        assert!(origin.event_metadata(&fail).is_ok());
        assert!(origin.validate_active(&program).is_ok());
        origin.used = 2;
        assert!(origin.event_metadata(&fail).is_err());
        origin.used = 3;
        fail.actor.kind = AgentActorKind::Agent;
        assert!(origin.event_metadata(&fail).is_err());
        program.tasks.get_mut("task").unwrap().review_generation = 5;
        assert!(origin.validate_active(&program).is_err());
        program.tasks.get_mut("task").unwrap().active_attempts.clear();
        assert!(origin.validate_active(&program).is_err());
    }

    /// 033 S7 (supplementary pin): the 032 invariant - a Root actor without
    /// an original physical receipt is refused at the ingress gate - holds
    /// for every program-state action shape, not only for the ones the
    /// action service happens to see.
    ///
    /// This is the pin that 033 S3-D was missing: the effect-class gate in
    /// the action service is masked by this ingress gate, so a narrowing
    /// there changes nothing in the live scene. The four shapes below are
    /// pinned against the ingress path, and the current tree's behaviour is
    /// the pin.
    #[tokio::test]
    async fn s7_a_root_without_a_physical_receipt_is_refused_at_the_ingress_gate() {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let root_actor = AgentActorBinding {
            original_execution: None,
            objective_id: "objective".into(),
            program_id: "program".into(),
            session_id: "session".into(),
            turn_id: "turn".into(),
            root_execution_id: None,
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: "pin the ingress gate".into(),
            model_lease: "test".into(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: "root".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        let shapes: Vec<(&str, AgentAction)> = vec![
            (
                "team-create",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Readers".into(),
                    mission: "read evidence".into(),
                    objective: None,
                }),
            ),
            (
                "task-publish",
                AgentAction::TaskPublish(
                    serde_json::from_value(serde_json::json!({
                        "team_ref": "team",
                        "title": "Evidence",
                        "objective": "read evidence",
                        "acceptance": "cited evidence",
                        "required_capabilities": ["read"]
                    }))
                    .unwrap(),
                ),
            ),
            (
                "message-publish",
                AgentAction::MessagePublish(MessagePublishInput {
                    topic_ref: "topic".into(),
                    summary: Some("progress".into()),
                    content_ref: None,
                    refs: Vec::new(),
                    recipients: Vec::new(),
                    issue_dispositions: Vec::new(),
                    intent: None,
                }),
            ),
            (
                "task-review",
                AgentAction::TaskReview(TaskReviewInput {
                    task_ref: "task".into(),
                    decision: TaskReviewDecision::Rework,
                    reason: "the answer does not cite the declared criterion".into(),
                    operator_requirement: None,
                    evidence_refs: Vec::new(),
                    gaps: Vec::new(),
                    directed_actions: Vec::new(),
                }),
            ),
        ];
        for (shape, action) in shapes {
            let envelope = AgentActionEnvelope {
                action_id: format!("s7-{shape}"),
                actor: root_actor.clone(),
                expected_revision: None,
                action,
            };
            let message = match services
                .submit_agent_action_from_owner(&envelope, false, false, None, None, None)
                .await
            {
                Ok(_) => panic!("{shape}: a Root action without an original physical receipt must be refused at the ingress gate"),
                Err(error) => error,
            };
            // The ingress path refuses it before the Program can see it.
            // Which ingress gate speaks first depends on the caller's
            // provenance: a call that arrives without an admitted capability
            // is refused at the capability gate (ingress.rs:703), and a call
            // that carries its capability but no original physical receipt is
            // refused at the binding gates (ingress.rs:707 for every public
            // actor, ingress.rs:723 for the Root shape). The invariant this
            // pins is the one 033 S3-D missed: the Root shape never reaches
            // the action service from the ingress path without a physical
            // receipt, so a narrowing in the service cannot change the live
            // scene.
            assert!(
                message.contains("original single-call capability missing")
                    || message.contains("original authenticated execution")
                    || message.contains("original physical execution"),
                "{shape}: the ingress gate names the missing binding: {message}"
            );
        }
    }

    #[test]
    fn t_removal_program_move_classification_still_holds() {
        // Display code recognition remains compatible, but ordinary text no
        // longer grants a Program-rebuild budget without the producer's type.
        let enumeration = "result metadata changed after original effect enumeration: result=artifact:one source=session:one expected=1 observed=2; rebuild the review";
        assert_eq!(crate::agentic::review_evidence::reason_code::reason_code_of(enumeration),
            Some(crate::agentic::review_evidence::reason_code::ReviewRefusalCode::ResultMetadataChangedAfterEnumeration));
        for message in [enumeration,
            "result Program changed during original producer resolution: source agentic-program:one is at revision 2 but the reviewed Program projection is at 1",
            "result metadata source changed during resolution; rebuild the review; source=session:one expected=1 actual=2 phase=result_source_pin",
            "task_not_found"] {
            assert!(!is_review_program_advanced(&message.into()));
        }
    }

    /// The three transcription shapes a server repairs are repaired to the one
    /// receipt they name.
    ///
    /// A model writes a hex character, a separator or the end of a content
    /// suffix differently, and the receipt it meant is already recorded. The
    /// repair names exactly one receipt, so the content that is verified is the
    /// content that was meant.
    #[test]
    fn a_transcription_drift_is_repaired_to_the_one_receipt_it_names() {
        let canonical = "tool-raw-chatcmpl-tool-aa0d514a557763e5-2c77431438342406";
        let recorded = vec![canonical.to_string()];
        // The tail of the content suffix was cut.
        assert_eq!(
            repair_receipt_citation("tool-raw-chatcmpl-tool-aa0d514a557763e5-6", &recorded),
            EvidenceCitation::Canonical(canonical.to_string())
        );
        // A hex character slipped.
        assert_eq!(
            repair_receipt_citation(
                "tool-raw-chatcmpl-tool-aa0d514a557763e5-2c77431438342407",
                &recorded
            ),
            EvidenceCitation::Canonical(canonical.to_string())
        );
        // A separator drifted between the call parts.
        assert_eq!(
            repair_receipt_citation(
                "tool-raw-chatcmpl-tool_aa0d514a557763e5-2c77431438342406",
                &recorded
            ),
            EvidenceCitation::Canonical(canonical.to_string())
        );
    }

    /// A citation that matches more than one receipt is refused, not guessed:
    /// the same content can be recorded under more than one canonical id, and
    /// Runtime does not choose which one the model meant.
    #[test]
    fn an_ambiguous_citation_is_refused_rather_than_guessed() {
        let recorded = vec![
            "tool-raw-call-a-2c77431438342406".to_string(),
            "tool-raw-call-a-2c77431438342406112233445566778899".to_string(),
        ];
        match repair_receipt_citation("tool-raw-call-a-2c77", &recorded) {
            EvidenceCitation::Ambiguous(candidates) => assert_eq!(candidates, recorded),
            other => panic!("a two-way match is ambiguous, not a choice: {other:?}"),
        }
    }

    /// The legacy call-id form pins no content, so it is never repaired to a
    /// receipt: repairing it would be choosing which receipt the model meant.
    #[test]
    fn the_legacy_call_id_form_is_never_repaired_to_a_receipt() {
        let recorded = vec![
            "tool-raw-chatcmpl-tool-aa0d514a557763e5-2c77431438342406".to_string(),
        ];
        assert_eq!(
            repair_receipt_citation("tool-raw-chatcmpl-tool-aa0d514a557763e5", &recorded),
            EvidenceCitation::Unrepairable
        );
    }

    /// An invented suffix does not alias another receipt: the repair only moves
    /// a citation to a receipt whose own call id and content suffix match.
    #[test]
    fn an_invented_suffix_does_not_alias_another_receipt() {
        let recorded = vec![
            "tool-raw-chatcmpl-tool-aa0d514a557763e5-2c77431438342406".to_string(),
        ];
        assert_eq!(
            repair_receipt_citation(
                "tool-raw-chatcmpl-tool-aa0d514a557763e5:8",
                &recorded
            ),
            EvidenceCitation::Unrepairable
        );
        // A different call is a different receipt, however close the content.
        assert_eq!(
            repair_receipt_citation(
                "tool-raw-chatcmpl-tool-bb0d514a557763e5-6",
                &recorded
            ),
            EvidenceCitation::Unrepairable
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
        // The content suffix was truncated: the receipt it names is recorded,
        // so the refusal names that canonical receipt instead of guessing which
        // content the model meant.
        let error = services
            .validate_agent_action_evidence(&actor, &action)
            .await
            .expect_err("a mistyped citation must be refused with the receipt named");
        assert!(
            error.contains(canonical),
            "the refusal must name the canonical receipt: {error}"
        );
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

    /// 030 T8 control (1): a one-character drift in the call-id part - the
    /// shape that earned zero hints across the whole escalation Session - is
    /// answered by naming the closest recorded call id and its durable
    /// receipt, never by rewriting or resolving the citation.
    #[test]
    fn t8_a_one_character_call_drift_is_corrected_by_name_never_by_rewriting() {
        let canonical = "tool-raw-chatcmpl-tool-aa0d514a557763e5-2c77431438342406";
        let receipts = vec![canonical.to_string()];
        for (shape, cited) in [
            (
                "substitution",
                "tool-raw-chatcmpl-tool-aa0d514a557763e6-2c77431438342406",
            ),
            (
                "deletion",
                "tool-raw-chatcmpl-tool-aa0d514a557763e-2c77431438342406",
            ),
        ] {
            let hint = tool_evidence_hint(cited, None, &receipts)
                .unwrap_or_else(|| panic!("a {shape} drift must earn a named hint"));
            assert!(
                hint.contains(&format!("appears to be tool://{canonical}")),
                "the hint names the one durable receipt: {hint}"
            );
            assert!(
                hint.contains("tool-raw-chatcmpl-tool-aa0d514a557763e5"),
                "the recorded call id is named as the suggestion: {hint}"
            );
            assert!(hint.contains("copy that exact reference"), "{hint}");
            assert!(
                !hint.contains(cited),
                "the hint never repeats the citation as if it were valid: {hint}"
            );
        }
        // An exact citation is the resolution legs' own case, and a drift
        // wider than one edit is a guess the hint must refuse to make.
        assert!(tool_evidence_hint(canonical, None, &receipts).is_none());
        assert!(tool_evidence_hint(
            "tool-raw-chatcmpl-tool-ffffff514a557763e9-2c77431438342406",
            None,
            &receipts,
        )
        .is_none());
        // R48.3 (甲): a receipt one edit away on the call id but carrying a
        // different content suffix is not suggestion material - copying it
        // would resolve to another call's provenance, and the refusal must
        // not point there.
        assert!(tool_evidence_hint(
            "tool-raw-chatcmpl-tool-aa0d514a557763e6-deadbeefdeadbeef",
            None,
            &receipts,
        )
        .is_none());
    }

    /// 030 T8 control (2): a content suffix that several call ids
    /// legitimately share - the escalation measured one suffix under four
    /// receipts - disambiguates by naming every candidate, never by picking.
    #[test]
    fn t8_a_shared_digest_suffix_lists_candidate_call_ids_by_name() {
        let digest = "3ab2c55c2949bfc1";
        let mut receipts: Vec<String> = ["a", "b", "c", "d"]
            .iter()
            .map(|tail| format!("tool-raw-chatcmpl-tool-000000000000000{tail}-{digest}"))
            .collect();
        receipts.push("tool-raw-chatcmpl-tool-111111111111111f-deadbeefdeadbeef".to_string());
        let cited = "tool-raw-chatcmpl-tool-ffffffffffffffff-3ab2c55c2949bfc1";
        let hint = tool_evidence_hint(cited, None, &receipts)
            .expect("a shared suffix must name its candidates");
        assert!(hint.contains("recorded under 4 durable receipts"), "{hint}");
        for tail in ["a", "b", "c", "d"] {
            assert!(
                hint.contains(&format!(
                    "tool://tool-raw-chatcmpl-tool-000000000000000{tail}-{digest}"
                )),
                "every candidate is named: {hint}"
            );
        }
        assert!(
            !hint.contains("111111111111111f"),
            "an unrelated receipt is not listed: {hint}"
        );
        // A suffix recorded under exactly one receipt is not disambiguation
        // material either: there is nothing to choose between, and the call
        // legs above already had their chance.
        assert!(tool_evidence_hint(
            "tool-raw-chatcmpl-tool-ffffffffffffffff-deadbeefdeadbeef",
            None,
            &receipts,
        )
        .is_none());
        // Naming stays bounded even when the suffix is popular.
        let crowded = (0..9)
            .map(|i| format!("tool-raw-call-{i:02}-{digest}"))
            .collect::<Vec<_>>();
        let hint = tool_evidence_hint("tool-raw-call-xx-3ab2c55c2949bfc1", None, &crowded)
            .expect("nine holders still disambiguate");
        assert!(hint.contains("recorded under 9 durable receipts"), "{hint}");
        assert!(hint.contains("(and 1 more)"), "the list is capped: {hint}");
    }

    /// 030 T8 control (3): the leg that already worked keeps its behavior
    /// exactly - the original sentence, character for character - and two
    /// one-edit neighbours disambiguate by listing instead of guessing.
    #[test]
    fn t8_the_exact_call_leg_keeps_its_original_sentence() {
        let canonical = "tool-raw-chatcmpl-tool-aa0d514a557763e5-2c77431438342406";
        let cited = "tool-raw-chatcmpl-tool-aa0d514a557763e5-6";
        assert_eq!(
            tool_evidence_hint(cited, Some(canonical), &[])
                .expect("the original leg still hints"),
            format!(
                "The durable receipt Runtime recorded for this tool call is tool://{canonical}; copy that exact reference"
            )
        );
        // A citation already equal to the recorded receipt earns no hint.
        assert!(tool_evidence_hint(canonical, Some(canonical), &[]).is_none());
        // Two recorded call ids within one edit list both instead of picking.
        let receipts = vec![
            canonical.to_string(),
            "tool-raw-chatcmpl-tool-aa0d514a557763e4-2c77431438342406".to_string(),
        ];
        let hint = tool_evidence_hint(
            "tool-raw-chatcmpl-tool-aa0d514a557763e7-2c77431438342406",
            None,
            &receipts,
        )
        .expect("an ambiguous drift lists candidates");
        assert!(
            hint.contains("2 recorded call ids are within one edit"),
            "{hint}"
        );
        assert!(hint.contains(&format!("tool://{canonical}")), "{hint}");
        assert!(
            hint.contains("tool://tool-raw-chatcmpl-tool-aa0d514a557763e4-2c77431438342406"),
            "{hint}"
        );
    }

    /// 030 T8: the drift bound is exactly one edit - substitution, insertion,
    /// or deletion - and nothing wider, including no self-match.
    #[test]
    fn t8_within_one_edit_bounds_the_drift() {
        assert!(within_one_edit("abcd", "abce"));
        assert!(within_one_edit("abcd", "abcde"));
        assert!(within_one_edit("abcd", "bcd"));
        assert!(!within_one_edit("abcd", "abcd"));
        assert!(!within_one_edit("abcd", "abde"));
        assert!(!within_one_edit("abcd", "wxyz"));
        assert!(!within_one_edit("abcd", "abcdefgh"));
    }

    /// 030 T8: both drifted shapes - a one-character call-id drift and a
    /// foreign call citing a shared content suffix - earn their hints through
    /// the real validation refusal, which stays a refusal.
    #[tokio::test]
    async fn drifted_call_ids_are_refused_with_the_correction_named() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        use harness_contract::agent_action::{AgentActorKind, TaskSubmitInput};
        use harness_contract::reality::EvidenceRef;

        let canonical = "tool-raw-chatcmpl-tool-aa0d514a557763e5-2c77431438342406";
        let digest = "3ab2c55c2949bfc1";
        let shared: Vec<String> = ["a", "b", "c", "d"]
            .iter()
            .map(|tail| format!("tool-raw-chatcmpl-tool-000000000000000{tail}-{digest}"))
            .collect();
        let temp = tempfile::tempdir().expect("temp");
        let store = std::sync::Arc::new(crate::test_support::session_store());
        store
            .create_session(&session::SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "evidence-hint-drift".to_string(),
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
        for id in std::iter::once(canonical.to_string()).chain(shared.iter().cloned()) {
            facade
                .persist(crate::context_evidence::raw::RawEvidenceWrite {
                    evidence_ref: EvidenceRef::observed("tool", id.clone()),
                    session_id: "s1".to_string(),
                    media_type: "text/plain".to_string(),
                    visibility_scope: "session:s1".to_string(),
                    payload: format!("recorded body for {id}"),
                    metadata: serde_json::json!({
                        "tool_name": "read_ref",
                        "tool_call_id": id,
                    }),
                })
                .await
                .expect("durable evidence");
        }

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
        let drifted = "tool-raw-chatcmpl-tool-aa0d514a557763e6-2c77431438342406";
        let action = AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: "task:1".to_string(),
            deliverable: None,
            artifact_refs: vec![],
            evidence_refs: vec![format!("tool://{drifted}")],
            unresolved: vec![],
        });
        let error = services
            .validate_agent_action_evidence(&actor, &action)
            .await
            .expect_err("a drifted call id must never resolve");
        assert!(error.contains("has no canonical durable receipt"), "{error}");
        assert!(
            error.contains(&format!("appears to be tool://{canonical}")),
            "the refusal names the closest recorded receipt: {error}"
        );
        assert!(error.contains("copy that exact reference"), "{error}");
        // R48.3 (甲) end to end: the same drifted call id with a foreign
        // content suffix earns no suggestion - the refusal stands alone.
        let mismatched = "tool-raw-chatcmpl-tool-aa0d514a557763e6-deadbeefdeadbeef";
        let action = AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: "task:1".to_string(),
            deliverable: None,
            artifact_refs: vec![],
            evidence_refs: vec![format!("tool://{mismatched}")],
            unresolved: vec![],
        });
        let error = services
            .validate_agent_action_evidence(&actor, &action)
            .await
            .expect_err("a drifted call id over a foreign suffix must never resolve");
        assert!(error.contains("has no canonical durable receipt"), "{error}");
        assert!(
            !error.contains("appears to be"),
            "no suggestion across a content mismatch: {error}"
        );

        let foreign = "tool-raw-chatcmpl-tool-ffffffffffffffff-3ab2c55c2949bfc1";
        let action = AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: "task:1".to_string(),
            deliverable: None,
            artifact_refs: vec![],
            evidence_refs: vec![format!("tool://{foreign}")],
            unresolved: vec![],
        });
        let error = services
            .validate_agent_action_evidence(&actor, &action)
            .await
            .expect_err("a shared suffix must never resolve to one call");
        assert!(error.contains("has no canonical durable receipt"), "{error}");
        assert!(
            error.contains("recorded under 4 durable receipts"),
            "the refusal disambiguates: {error}"
        );
        for id in &shared {
            assert!(
                error.contains(&format!("tool://{id}")),
                "every candidate is named: {error}"
            );
        }
        // S2 (G4): the ambiguity is typed, not only reported. Before this
        // change the refusal carried prose alone, so a reader downstream
        // could not tell it apart from any other unresolvable reference and
        // could not parse the candidates without scraping them out of a
        // sentence. The prose a model reads is unchanged; the typed payload
        // rides with it.
        assert!(
            error.contains("\"code\":\"reference_ambiguous\""),
            "the ambiguity refusal is typed: {error}"
        );
        assert!(
            error.contains("\"candidate_count\":4"),
            "the typed payload states how many receipts answer: {error}"
        );
        assert!(
            error.contains("\"cited\""),
            "the typed payload names the citation that was refused: {error}"
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
    gaps: Vec::new(),
    directed_actions: Vec::new(),
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
