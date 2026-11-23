//! Read the existing Runtime strategy and tool journal; do not invent policy.
#[cfg(test)]
use crate::agentic::execution::reviewed_goal_fixture::diagnostic_1120 as obs1120;
use crate::agentic::program::AgenticProgramProjection;
use crate::{ExpectedStreamRevision, RuntimeEventScope, RuntimeEventStore};
use harness_contract::core::{ExecutionModifier, ExecutionPolicyGate, TaskRisk};
use harness_contract::goal::GoalContract;
use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(crate) struct RootReviewPolicySnapshot {
    pub digest: String,
    pub source: ExpectedStreamRevision,
    pub last_effect_cursor: u64,
    pub additional_sources: Vec<ExpectedStreamRevision>,
}

pub(crate) struct ProofInputSnapshot {
    pub work_digest: String,
    pub task_refs: Vec<String>,
    pub artifact_refs: Vec<String>,
    pub reviewer_execution_refs: BTreeSet<String>,
    pub(super) reviewer_packet: Option<harness_contract::agent::AgentTaskPacket>,
    pub retiring_optional_task_refs: BTreeSet<String>,
    pub semantic_digest: String,
    pub policy_digest: String,
    pub effects: RootReviewPolicySnapshot,
    pub sources: Vec<ExpectedStreamRevision>,
    /// The effect identity of the review control call this snapshot exempts.
    /// It is a CANDIDATE that every entry re-binds against the durable
    /// evidence; a snapshot without one exempts nothing.
    pub current_review_effect_key: Option<String>,
    /// The required writes this review settled as terminally failed.
    ///
    /// Each one names the call that ran and failed. A settled pending write
    /// is never a satisfied requirement, and never an effect that did not
    /// happen: the conclusion carries these by name.
    pub terminal_failures: Vec<serde_json::Value>,
    pub(super) dependency_frame: ConclusionMaterialDependencies,
    pub(super) pre_authenticated_session_source: Option<ExpectedStreamRevision>,
}

/// The actual dependencies independently recovered for this proof, not a
/// list supplied by a caller. Its outer prepared proof binds store and use.
pub(crate) struct ConclusionMaterialDependencies {
    session_source: ExpectedStreamRevision,
    historical_session_cut: Option<u64>,
    session_id: String,
    turn_id: String,
    review_targets: Vec<harness_contract::goal::ProofEffectTarget>,
    file_dependencies: Vec<ActualFileDependency>,
    immutable_inputs: Vec<ActualImmutableInput>,
    material_events: Vec<harness_contract::goal::ProofInputEvent>,
    canonical_facts: Vec<serde_json::Value>,
}

impl ConclusionMaterialDependencies {
    #[cfg(test)]
    pub(super) fn empty_for_coverage_test() -> Self {
        Self { session_source: ExpectedStreamRevision { stream_id: String::new(), expected_revision: 0 }, historical_session_cut: None,
            session_id: String::new(), turn_id: String::new(),
            review_targets: Vec::new(), file_dependencies: Vec::new(), immutable_inputs: Vec::new(), material_events: Vec::new(), canonical_facts: Vec::new() }
    }
    pub(crate) fn session_source(&self) -> &ExpectedStreamRevision { &self.session_source }
    pub(crate) fn historical_session_cut(&self) -> Option<u64> { self.historical_session_cut }
    pub(crate) fn review_targets(&self) -> &[harness_contract::goal::ProofEffectTarget] { &self.review_targets }
    pub(crate) fn material_events(&self) -> &[harness_contract::goal::ProofInputEvent] { &self.material_events }
    pub(crate) fn canonical_facts(&self) -> &[serde_json::Value] { &self.canonical_facts }
    pub(crate) fn file_dependencies(&self) -> impl Iterator<Item = (&harness_contract::goal::ProofEffectTarget, u64, u64, &str, &str)> {
        self.file_dependencies.iter().map(|d| (&d.target, d.acquisition_cursor, d.consumer_cursor,
            d.consumer_call_key.as_str(), d.source_sha256.as_str()))
    }
    pub(crate) fn protected_files(&self) -> impl Iterator<Item = (&harness_contract::goal::ProofEffectTarget, &str, u64)> {
        self.file_dependencies.iter().map(|d| (&d.target, d.source_sha256.as_str(), d.source_bytes))
    }
    pub(crate) fn immutable_inputs(&self) -> impl Iterator<Item = (&harness_contract::context::ArtifactRef, &str, u64)> {
        self.immutable_inputs.iter().map(|d| (&d.content, d.reference.as_str(), d.acquisition_cursor))
    }

    // Original material cuts and current mutable controls are kept separate.
}

/// A bounded historical Session view, not a current authorization or tail.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HistoricalSessionCut {
    source: ExpectedStreamRevision,
    observed_revision: u64,
}
impl HistoricalSessionCut {
    pub(super) fn source(&self) -> &ExpectedStreamRevision { &self.source }
    pub(super) fn observed_revision(&self) -> u64 { self.observed_revision }

    /// The only producer of a historical Session cut. It reads the exact
    /// `session:{session_id}` head from the store itself: a caller can name
    /// only the material cut its original ProofInputSet was assembled
    /// against, never report a head. A cut above the durable head is either
    /// a fabricated window or a truncated stream and is refused.
    pub(crate) fn for_live_material(
        store: &crate::RuntimeEventStore,
        session_id: &str,
        cut: u64,
    ) -> Result<Self, String> {
        let session_source = format!("session:{session_id}");
        let head = store
            .stream_revision(&session_source)
            .map_err(|error| error.to_string())?;
        if cut > head {
            return Err(format!(
                "review Session material cut is beyond head; source={session_source} cut={cut} head={head}"
            ));
        }
        Ok(Self {
            source: ExpectedStreamRevision { stream_id: session_source, expected_revision: cut },
            observed_revision: head,
        })
    }
}

impl ProofInputSnapshot {
    pub(crate) fn dependency_frame(&self) -> &ConclusionMaterialDependencies { &self.dependency_frame }
    pub(crate) fn reviewer_matches(
        &self,
        program: &AgenticProgramProjection,
        review: &harness_contract::goal::ObjectiveReviewRecord,
    ) -> bool {
        match review.reviewer_execution_id.as_ref() {
            Some(execution) => self.reviewer_execution_refs.contains(execution),
            None => program.root_execution_id.as_ref().is_some_and(|root| {
                self.reviewer_execution_refs
                    .contains(&format!("root-execution:{root}"))
            }),
        }
    }
}

pub(crate) struct CompletionAdmissionContext {
    program_digest: String,
    request_digest: String,
    envelope_digest: String,
    goal_id: String,
    goal_revision: u64,
    goal_spec_revision: u64,
    goal_spec_digest: String,
    excluded_tasks: BTreeSet<String>,
    sources: Vec<ExpectedStreamRevision>,
}

impl CompletionAdmissionContext {
    pub(crate) fn matches_request(
        &self,
        program: &AgenticProgramProjection,
        input: &harness_contract::agent_action::ObjectiveCompleteRequestInput,
    ) -> bool {
        self.program_digest == super::proof_value_digest(program)
            && self.request_digest == super::proof_value_digest(input)
    }

    pub(crate) fn matches_envelope(
        &self,
        envelope: &harness_contract::agent_action::AgentActionEnvelope,
    ) -> bool {
        self.envelope_digest == super::proof_value_digest(envelope)
    }

    pub(crate) fn matches_goal(&self, goal: &GoalContract, stream_revision: u64) -> bool {
        self.goal_id == goal.id
            && self.goal_revision == stream_revision
            && self.goal_spec_revision == goal.spec_revision
            && self.goal_spec_digest == goal.spec_digest
    }

    pub(crate) fn excludes_task(&self, task: &str) -> bool {
        self.excluded_tasks.contains(task)
    }

    pub(crate) fn sources(&self) -> &[ExpectedStreamRevision] {
        &self.sources
    }
}


/// Reuse the original Goal owner's sealed proof/coverage preparation when
/// the same completion reaches its checked commit. The caller must first
/// verify the prepared seal against this store and Program projection.
pub(crate) fn completion_admission_context_from_prepared(
    program: &AgenticProgramProjection,
    envelope: &harness_contract::agent_action::AgentActionEnvelope,
    prepared: &crate::execution_core::goal::PreparedProgramConclusion,
) -> Result<Option<CompletionAdmissionContext>, String> {
    use harness_contract::agent_action::AgentAction;
    let AgentAction::ObjectiveCompleteRequest(input) = &envelope.action else { return Ok(None); };
    if input.blocked || !program.tasks.values().any(|task| {
        task.status == crate::agentic::program::AgenticTaskStatus::CancelRequested
            && task.pending_retirement == Some(crate::agentic::program::AgenticTaskRetirement::Withdrawn)
    }) { return Ok(None); }
    if envelope.actor.program_id != program.program_id || envelope.actor.objective_id != program.objective_id
        || envelope.actor.session_id != program.session_id || envelope.actor.turn_id != program.turn_id
        || envelope.actor.root_execution_id != program.root_execution_id
        || prepared.goal.session_id != program.session_id {
        return Err("prepared completion context owner differs".into());
    }
    let request = program.completion_request.as_ref().ok_or("prepared completion request missing")?;
    if request.action_id != envelope.action_id || request.requested_by != envelope.actor.actor_id
        || request.result_refs != input.result_refs || request.evidence_refs != input.evidence_refs
        || request.unresolved != input.unresolved {
        return Err("prepared completion context request differs".into());
    }
    let conditions = super::required_review_conditions(&prepared.goal);
    if conditions.is_empty() { return Err("prepared completion context has no required reviews".into()); }
    let mut excluded: Option<BTreeSet<String>> = None;
    let mut used = BTreeSet::new();
    for condition in conditions {
        let review = prepared.goal.reviews.iter().rev().find(|review| review.criterion_ref == condition)
            .ok_or("prepared completion review missing")?;
        let proof = review.verification.as_ref().ok_or("prepared completion review proof missing")?;
        let (index, verified) = prepared.verified_proofs().iter().enumerate()
            .find(|(index, verified)| !used.contains(index) && verified.condition() == condition
                && verified.proof().matches_proof(proof))
            .ok_or("prepared completion review is not sealed")?;
        used.insert(index);
        let retired = &verified.snapshot().retiring_optional_task_refs;
        excluded = Some(match excluded {
            None => retired.clone(),
            Some(previous) => previous.intersection(retired).cloned().collect(),
        });
    }
    if used.len() != prepared.verified_proofs().len() { return Err("prepared completion has unused proofs".into()); }
    Ok(Some(CompletionAdmissionContext {
        program_digest: super::proof_value_digest(program),
        request_digest: super::proof_value_digest(input),
        envelope_digest: super::proof_value_digest(envelope),
        goal_id: prepared.goal.id.clone(),
        goal_revision: prepared.expected_stream_revision,
        goal_spec_revision: prepared.goal.spec_revision,
        goal_spec_digest: prepared.goal.spec_digest.clone(),
        excluded_tasks: excluded.ok_or("prepared completion review set missing")?,
        sources: prepared.source_frame.sources().cloned().collect(),
    }))
}

pub(crate) fn completion_admission_context(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    envelope: &harness_contract::agent_action::AgentActionEnvelope,
    readers: super::ResultContentReaders<'_>,
) -> Result<Option<CompletionAdmissionContext>, String> {
    use harness_contract::agent_action::AgentAction;
    let AgentAction::ObjectiveCompleteRequest(input) = &envelope.action else {
        return Ok(None);
    };
    if input.blocked
        || !program.tasks.values().any(|task| {
            task.status == crate::agentic::program::AgenticTaskStatus::CancelRequested
                && task.pending_retirement
                    == Some(crate::agentic::program::AgenticTaskRetirement::Withdrawn)
        })
    {
        return Ok(None);
    }
    let current = crate::AgentActionService::new(std::sync::Arc::clone(store))
        .project(&program.program_id)
        .map_err(|error| error.to_string())?;
    if &current != program
        || envelope.actor.program_id != program.program_id
        || envelope.actor.objective_id != program.objective_id
        || envelope.actor.session_id != program.session_id
        || envelope.actor.turn_id != program.turn_id
        || envelope.actor.root_execution_id != program.root_execution_id
    {
        return Err("completion context Program/request binding changed".into());
    }
    let root = program
        .root_execution_id
        .as_ref()
        .ok_or("completion context has no bound root")?;
    let projection = crate::execution_core::GoalStore::new(std::sync::Arc::clone(store))
        .projection(&format!("goal:{root}"))?
        .ok_or("completion context has no bound Goal")?;
    let goal = projection.goal;
    let binding = goal
        .execution_binding
        .as_ref()
        .ok_or("completion context has no Goal execution binding")?;
    if goal.completion != harness_contract::goal::GoalCompletion::Open
        || goal.terminal.is_some()
        || binding.agentic_program_id != program.program_id
        || binding.objective_id != program.objective_id
        || binding.root_execution_id != *root
        || binding.session_id != program.session_id
        || binding.turn_id != program.turn_id
    {
        return Err("completion context Goal/request binding changed".into());
    }
    if let Some(request) = &program.completion_request {
        if request.action_id != envelope.action_id
            || request.requested_by != envelope.actor.actor_id
            || request.result_refs != input.result_refs
            || request.evidence_refs != input.evidence_refs
            || request.unresolved != input.unresolved
        {
            return Err("completion context does not match the durable request".into());
        }
    }
    let intent = goal
        .user_intent_criterion_id
        .as_ref()
        .filter(|condition| !condition.is_empty())
        .ok_or("completion context has no original intent condition")?;
    let conditions =
        super::required_review_conditions(&goal)
            .into_iter()
            .map(|condition| {
                let mut results = Vec::new();
                if &condition == intent {
                    results.extend(crate::execution_core::goal::delivered_result_refs(
                        program,
                        &goal,
                        &input.result_refs,
                    ));
                }
                for obligation in goal.obligations.iter().filter(|obligation| {
                    obligation.required && obligation.obligation_id == condition
                }) {
                    results.extend(obligation.artifact_refs.iter().cloned());
                }
                if let Some(review) = goal
                    .reviews
                    .iter()
                    .rev()
                    .find(|review| review.criterion_ref == condition)
                {
                    results.extend(review.result_refs.iter().cloned());
                }
                (condition, results)
            })
            .collect::<Vec<_>>();
    let mut sources = std::collections::BTreeMap::from([
        (
            format!("agentic-program:{}", program.program_id),
            program.revision,
        ),
        (format!("goal:{}", goal.id), projection.stream_revision),
    ]);
    let mut excluded_tasks: Option<BTreeSet<String>> = None;
    for (condition, results) in conditions {
        let review = goal
            .reviews
            .iter()
            .rev()
            .find(|review| review.criterion_ref == condition)
            .ok_or_else(|| format!("completion context requires latest review:{condition}"))?;
        let proof = review
            .verification
            .as_ref()
            .filter(|proof| {
                proof
                    .input_set
                    .as_ref()
                    .is_some_and(|inputs| inputs.version == 2)
            })
            .ok_or_else(|| format!("completion context requires latest v2 proof:{condition}"))?;
        super::validate_explicit_process_evidence(store, proof, &review.evidence_refs)
            .map_err(|error| error.to_string())?;
        let snapshot = validate_applied_proof_inputs(store, program, Some(&goal), proof, readers)?;
        super::validate_review_coverage(
            &goal, program, review, &results, &review.evidence_refs, &snapshot,
        ).map_err(|error| format!("completion context review {condition}: {error}"))?;
        excluded_tasks = Some(match excluded_tasks {
            None => snapshot.retiring_optional_task_refs,
            Some(previous) => previous
                .intersection(&snapshot.retiring_optional_task_refs)
                .cloned()
                .collect(),
        });
        for source in snapshot.sources {
            if sources
                .insert(source.stream_id, source.expected_revision)
                .is_some_and(|revision| revision != source.expected_revision)
            {
                return Err(
                    "completion context required reviews have different source cuts".into(),
                );
            }
        }
    }
    for (source, revision) in &sources {
        if store
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != *revision
        {
            return Err("completion context source changed; rebuild the whole context".into());
        }
    }
    Ok(Some(CompletionAdmissionContext {
        program_digest: super::proof_value_digest(program),
        request_digest: super::proof_value_digest(input),
        envelope_digest: super::proof_value_digest(envelope),
        goal_id: goal.id,
        goal_revision: projection.stream_revision,
        goal_spec_revision: goal.spec_revision,
        goal_spec_digest: goal.spec_digest,
        excluded_tasks: excluded_tasks.ok_or("completion context has no required reviews")?,
        sources: sources
            .into_iter()
            .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                stream_id,
                expected_revision,
            })
            .collect(),
    }))
}

#[cfg_attr(test, track_caller)]
pub(crate) fn validate_proof_inputs(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
) -> Result<ProofInputSnapshot, String> {
    validate_proof_inputs_for_action(store, program, goal, proof, readers, None)
}

/// A persisted review is revalidated against the original action that carries
/// this exact proof. A missing, foreign or ambiguous record grants no exemption.
#[cfg_attr(test, track_caller)]
pub(crate) fn validate_applied_proof_inputs(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
) -> Result<ProofInputSnapshot, String> {
    with_authenticated_applied_review(store, program, proof, |identity| {
        validate_proof_inputs_for_action(store, program, goal, proof, readers, Some(identity))
    })
}

/// Operation-local conclusion witness produced only by the original proof
/// and call owners. Historical material cuts remain distinct from live heads.
#[derive(Clone, PartialEq)]
pub(crate) struct VerifiedConclusionProof {
    carrier: crate::DurableRuntimeEvent,
    original_proof: harness_contract::goal::ObjectiveReviewVerification,
    proof_digest: String,
    historical_cuts: std::collections::BTreeMap<String, u64>,
    current_heads: Vec<ExpectedStreamRevision>,
    historical_calls: Vec<super::AuthenticatedHistoricalPolicyCall>,
}
impl VerifiedConclusionProof {
    pub(crate) fn matches_proof(&self, proof: &harness_contract::goal::ObjectiveReviewVerification) -> bool {
        &self.original_proof == proof
    }
    pub(crate) fn frame_digest(&self) -> String {
        let calls = self.historical_calls.iter().map(super::AuthenticatedHistoricalPolicyCall::binding_digest).collect::<Vec<_>>();
        super::proof_value_digest(&(&self.carrier, &self.proof_digest, &self.historical_cuts, &self.current_heads, calls))
    }
    pub(crate) fn current_heads(&self) -> &[ExpectedStreamRevision] { &self.current_heads }
    /// These are evidence of already completed calls, never authority for the
    /// Supervisor's subsequent Goal/Program conclusion write.
    pub(crate) fn authenticated_historical_calls(&self) -> &[super::AuthenticatedHistoricalPolicyCall] { &self.historical_calls }
}

pub(crate) fn validate_applied_proof_for_conclusion(
    store: &std::sync::Arc<RuntimeEventStore>, program: &AgenticProgramProjection,
    goal: &GoalContract, proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
) -> Result<(ProofInputSnapshot, VerifiedConclusionProof), String> {
    with_authenticated_applied_review(store, program, proof, |identity| {
        let super::current_review::ReviewActionIdentity::Applied(applied) = identity else {
            return Err("conclusion requires an original applied review".into());
        };
        let inputs = proof.input_set.as_ref().ok_or("review original input cuts missing")?;
        let session_source = format!("session:{}", program.session_id);
        let pre_tail = match inputs.source_revisions.get(&session_source) {
            // The producer passes the original Program's Session id and the
            // original ProofInputSet's cut; the head is the constructor's
            // store read, never a caller report.
            Some(cut) => Some(HistoricalSessionCut::for_live_material(
                store, &program.session_id, *cut,
            )?),
            None => None,
        };
        let snapshot = validate_proof_inputs_for_action_with_tail(store, program, Some(goal), proof, readers, Some(identity), pre_tail.as_ref())?;
        let mut matching = applied.program_events.iter().filter(|event|
            event.payload["envelope"]["action_id"].as_str() == Some(applied.action_id));
        let carrier = matching.next().ok_or("original review carrier missing")?.clone();
        if matching.next().is_some() { return Err("original review carrier ambiguous".into()); }
        let envelope: harness_contract::agent_action::AgentActionEnvelope = serde_json::from_value(carrier.payload["envelope"].clone())
            .map_err(|_| "original review envelope malformed")?;
        let mut historical_cuts = inputs.source_revisions.clone();
        for (source, revision) in &proof.result_source_revisions {
            if historical_cuts.insert(source.clone(), *revision).is_some_and(|old| old != *revision) {
                return Err("review original cuts conflict".into());
            }
        }
        let context = super::current_review::CurrentReviewContext {
            action_id: &envelope.action_id, action: &envelope.action, actor: &envelope.actor,
        };
        let call = if let Some(packet) = snapshot.reviewer_packet.as_ref() {
            let mut witness_cuts = historical_cuts.clone();
            let expected_witness_cuts = witness_cuts.clone();
            let call = super::current_review::authenticate_process_review_policy_call(store, program, packet, &context, &mut witness_cuts)?
                .ok_or("original Process review call unavailable")?;
            if witness_cuts != expected_witness_cuts { return Err("original review material cuts changed".into()); }
            call
        } else if carrier.payload.get("member_admission").is_some() {
            super::current_review::authenticate_original_root_member_review(store, &carrier, &context, &historical_cuts)?
        } else {
            super::current_review::authenticate_original_single_review(store, &carrier, &context, &historical_cuts)?
        };
        let mut heads = std::collections::BTreeMap::new();
        let approval_stream = call.approval().map(|approval| format!("approval:{}", approval.approval_id()));
        for source in snapshot.sources.iter().chain(call.current_fences()) {
            // The historical approval cut remains in the proof. Its current
            // control is reconstructed below by its original owner, not by
            // reusing that historical revision as a current fence.
            if approval_stream.as_deref() == Some(source.stream_id.as_str()) { continue; }
            if heads.insert(source.stream_id.clone(), source.expected_revision).is_some_and(|old| old != source.expected_revision) {
                return Err("review current source heads conflict".into());
            }
        }
        if let Some(cut) = &pre_tail {
            if heads.get(&cut.source.stream_id) != Some(&cut.observed_revision) {
                return Err("Session material head changed during proof validation".into());
            }
        }
        // Keep exact existing material controls, rather than treating all
        // Session activity as a material change. Artifact pins and the
        // original Process owner's File leases survive the checked append.
        let dependencies = &snapshot.dependency_frame;
        let frame = readers.process_inputs;
        let file_is_guarded = |dependency: &ActualFileDependency| frame.is_some_and(|frame|
            frame.retains_exact_file(&dependency.target, &dependency.source_sha256, None));
        let pinned_artifact = |content: &harness_contract::context::ArtifactRef| frame.is_some_and(|frame|
            content.selector.starts_with("artifact://") && frame.retains_exact_artifact(content));
        let guarded_file_content = |reference: &str, digest: &str, bytes: u64| {
            dependencies.file_dependencies.iter().any(|dependency|
                dependency.target.path.workspace_relative_path == reference
                    && dependency.source_sha256.trim_start_matches("sha256:") == digest.trim_start_matches("sha256:")
                    && frame.is_some_and(|frame| frame.retains_exact_file(&dependency.target, digest, Some(bytes))))
        };
        let materials_guarded = !proof.effect_source_refs.iter().any(|source| source == &session_source)
            && !call.current_fences().iter().any(|source| source.stream_id == session_source)
            && !proof.reads.is_empty()
            && dependencies.file_dependencies.iter().all(file_is_guarded)
            && dependencies.review_targets.iter().all(|target| dependencies.file_dependencies.iter()
                .any(|dependency| dependency.target == *target && file_is_guarded(dependency)))
            && inputs.result_refs.iter().chain(inputs.artifact_refs.iter())
                .all(|reference| proof.reads.iter().any(|read| &read.result_ref == reference))
            && dependencies.immutable_inputs.iter().all(|input| pinned_artifact(&input.content)
                || guarded_file_content(&input.reference, &input.content.sha256, input.content.bytes))
            && proof.reads.iter().all(|read| frame.is_some_and(|frame|
                frame.retains_pinned_read(&read.content_ref, &read.sha256, read.bytes))
                || guarded_file_content(&read.content_ref, &read.sha256, read.bytes));
        if pre_tail.is_none() && materials_guarded {
            // The same original material decision applies when there was no
            // pre-existing tail: do not turn later protocol activity into a
            // material dependency solely because A saw no prior growth.
            heads.remove(&session_source);
        }
        if let Some(cut) = &pre_tail {
            if materials_guarded {
                heads.remove(&cut.source.stream_id);
            } else if cut.observed_revision != cut.source.expected_revision {
                #[cfg(feature = "test-fixtures")]
                if std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some() {
                    let effect_sources_clear = !proof.effect_source_refs.iter().any(|source| source == &session_source);
                    let call_fences_clear = !call.current_fences().iter().any(|source| source.stream_id == session_source);
                    let files_guarded = dependencies.file_dependencies.iter().all(file_is_guarded);
                    let targets_guarded = dependencies.review_targets.iter().all(|target| dependencies.file_dependencies.iter().any(|item| item.target == *target && file_is_guarded(item)));
                    let refs_covered = inputs.result_refs.iter().chain(inputs.artifact_refs.iter()).all(|reference| proof.reads.iter().any(|read| &read.result_ref == reference));
                    let immutable_guarded = dependencies.immutable_inputs.iter().all(|input| pinned_artifact(&input.content) || guarded_file_content(&input.reference, &input.content.sha256, input.content.bytes));
                    let reads_guarded = proof.reads.iter().all(|read| frame.is_some_and(|frame| frame.retains_pinned_read(&read.content_ref, &read.sha256, read.bytes)) || guarded_file_content(&read.content_ref, &read.sha256, read.bytes));
                    let failed_files: Vec<_> = dependencies.file_dependencies.iter().enumerate().filter_map(|(i, item)| (!file_is_guarded(item)).then_some(i)).take(16).collect();
                    let failed_targets: Vec<_> = dependencies.review_targets.iter().enumerate().filter_map(|(i, target)| (!dependencies.file_dependencies.iter().any(|item| item.target == *target && file_is_guarded(item))).then_some(i)).take(16).collect();
                    let failed_refs: Vec<_> = inputs.result_refs.iter().chain(inputs.artifact_refs.iter()).enumerate().filter_map(|(i, reference)| (!proof.reads.iter().any(|read| &read.result_ref == reference)).then_some(i)).take(16).collect();
                    let failed_immutable: Vec<_> = dependencies.immutable_inputs.iter().enumerate().filter_map(|(i, input)| (!(pinned_artifact(&input.content) || guarded_file_content(&input.reference, &input.content.sha256, input.content.bytes))).then_some(i)).take(16).collect();
                    let failed_reads: Vec<_> = proof.reads.iter().enumerate().filter_map(|(i, read)| (!(frame.is_some_and(|frame| frame.retains_pinned_read(&read.content_ref, &read.sha256, read.bytes)) || guarded_file_content(&read.content_ref, &read.sha256, read.bytes))).then_some(i)).take(16).collect();
                    let read_matches: Vec<_> = proof.reads.iter().take(16).map(|read| {
                        let selected = dependencies.immutable_inputs.iter().filter(|input| input.content.selector == read.content_ref).collect::<Vec<_>>();
                        (selected.len(), selected.iter().filter(|input| input.content.sha256 == read.sha256).count(), selected.iter().filter(|input| input.content.bytes == read.bytes).count(), selected.iter().filter(|input| pinned_artifact(&input.content)).count(), frame.is_some_and(|frame| frame.retains_pinned_read(&read.content_ref, &read.sha256, read.bytes)))
                    }).collect();
                    crate::execution_core::graph::k05_trace_emit(format!("REVIEW_MATERIAL_GUARD effect_sources_clear={effect_sources_clear} call_fences_clear={call_fences_clear} reads_present={} files_guarded={files_guarded} targets_guarded={targets_guarded} refs_covered={refs_covered} immutable_guarded={immutable_guarded} reads_guarded={reads_guarded} file_count={} target_count={} immutable_count={} read_count={} failed_files={failed_files:?} failed_refs={failed_refs:?} failed_targets={failed_targets:?} failed_immutable={failed_immutable:?} failed_reads={failed_reads:?} read_matches={read_matches:?}", !proof.reads.is_empty(), dependencies.file_dependencies.len(), dependencies.review_targets.len(), dependencies.immutable_inputs.len(), proof.reads.len()));
                }
                return Err("review material lacks an exact retained Artifact or File guard".into());
            }

        }
        // Only a grant independently bound to this original review is a
        // candidate for current approval control. A consumed or naturally
        // expired once-grant is historical; an explicit revocation of this
        // exact grant is not silently treated as one of those cases.
        if let Some(approval) = call.approval() {
            let current = crate::ApprovalQueue::capture_proof_control_from_store(
                store, &program.session_id, approval.approval_id(),
            )?;
            if &current.request().context != approval.context() {
                return Err("review approval current owner context differs".into());
            }
            let mut grants = current.grants().iter().filter(|grant|
                grant.grant_id == approval.grant_id() && grant.approval_id == approval.approval_id());
            let grant = grants.next().ok_or("review original approval grant is missing")?;
            if grants.next().is_some() || grant.revoked_at_ms.is_some()
                || grant.status == harness_contract::policy::approval::ApprovalGrantStatus::Revoked {
                return Err("review original approval grant revoked or ambiguous".into());
            }
            let source = current.source();
            if heads.insert(source.stream_id.clone(), source.expected_revision).is_some_and(|old| old != source.expected_revision) {
                return Err("review approval current control conflicts with other source".into());
            }
        }
        let current_heads = heads.into_iter().map(|(stream_id, expected_revision)|
            ExpectedStreamRevision { stream_id, expected_revision }).collect();
        let frame = VerifiedConclusionProof { carrier, original_proof: proof.clone(), proof_digest: super::proof_value_digest(proof),
            historical_cuts, current_heads, historical_calls: vec![call] };
        Ok((snapshot, frame))
    })
}

/// Authenticate once from the original durable action, then consume its
/// borrowed identity synchronously. Neither a stored proof key nor a failed
/// live invocation can enter this persisted-review boundary.
fn with_authenticated_applied_review<T>(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    consume: impl FnOnce(super::current_review::ReviewActionIdentity<'_>) -> Result<T, String>,
) -> Result<T, String> {
    let source = format!("agentic-program:{}", program.program_id);
    let pinned = proof
        .input_set
        .as_ref()
        .and_then(|set| set.source_revisions.get(&source))
        .copied()
        .ok_or("applied proof has no original Program cut")?;
    let events =
        super::current_review::program_events_after_cut(store, &source, pinned, program.revision)?;
    let applied = original_review_action_id(&events, proof)?
        .ok_or("original applied review action missing")?;
    let original = events
        .iter()
        .find(|event| event.payload["envelope"]["action_id"].as_str() == Some(applied.as_str()))
        .ok_or("original applied review record missing")?;
    let envelope: harness_contract::agent_action::AgentActionEnvelope =
        serde_json::from_value(original.payload["envelope"].clone())
            .map_err(|_| "original applied review envelope missing")?;
    if original.scope != RuntimeEventScope::Program
        || original.schema_version != 1
        || envelope.actor.program_id != program.program_id
        || envelope.actor.objective_id != program.objective_id
        || envelope.actor.session_id != program.session_id
        || envelope.actor.turn_id != program.turn_id
        || envelope.actor.root_execution_id != program.root_execution_id
        || !matches!(
            envelope.action,
            harness_contract::agent_action::AgentAction::TaskReview(_)
                | harness_contract::agent_action::AgentAction::ObjectiveReview(_)
        )
    {
        return Err("original applied review identity mismatch".into());
    }
    let indexed = store
        .event_by_idempotency_key(
            &source,
            &crate::agentic::action_service::action_key(&applied),
        )
        .map_err(|error| error.to_string())?
        .ok_or("original applied review primary missing")?;
    if serde_json::to_value(&indexed).map_err(|error| error.to_string())?
        != serde_json::to_value(original).map_err(|error| error.to_string())?
    {
        return Err("original applied review primary/index mismatch".into());
    }
    let context = super::current_review::AppliedReviewContext {
        action_id: &applied,
        program_source: &source,
        pinned,
        through: program.revision,
        program_events: &events,
    };
    consume(super::current_review::ReviewActionIdentity::Applied(
        &context,
    ))
}

/// Re-verify a proof against the LIVE action it is applying, so the review's
/// own in-flight call is resolved again from the durable evidence instead of
/// from anything the proof carries.
#[cfg_attr(test, track_caller)]
pub(crate) fn validate_proof_inputs_for_action(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
    current_review: Option<super::current_review::ReviewActionIdentity<'_>>,
) -> Result<ProofInputSnapshot, String> {
    validate_proof_inputs_for_action_with_tail(store, program, goal, proof, readers, current_review, None)
}

fn validate_proof_inputs_for_action_with_tail(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
    current_review: Option<super::current_review::ReviewActionIdentity<'_>>,
    pre_tail: Option<&HistoricalSessionCut>,
) -> Result<ProofInputSnapshot, String> {
    #[cfg(test)]
    let _observer = obs1120::Frame::enter(serde_json::json!({
        "callsite":format!("{}:{}", std::panic::Location::caller().file(), std::panic::Location::caller().line()),
        "identity":match current_review { Some(super::current_review::ReviewActionIdentity::Live(_))=>"Live",
            Some(super::current_review::ReviewActionIdentity::Applied(_))=>"Applied",None=>"None" },
        "action_id":current_review.map(|identity| identity.action_id()),
        "program":program.program_id,"revision":program.revision,"proof":super::proof_value_digest(proof),
        "carried_key":proof.current_review_effect_key,"reviewer":proof.reviewer_execution_id,
        "criterion":proof.input_set.as_ref().and_then(|inputs| inputs.condition_ref.as_deref())
    }));
    let current = crate::AgentActionService::new(std::sync::Arc::clone(store))
        .project(&program.program_id)
        .map_err(|error| error.to_string())?;
    if current.session_id != program.session_id || current.objective_id != program.objective_id {
        return Err("proof Program identity changed".into());
    }
    // Keep the Goal whose records authenticated the carrier tied to its exact
    // read cut. A later source head cannot certify an earlier projection.
    let current_goal = goal
        .map(|goal| {
            crate::execution_core::GoalStore::new(std::sync::Arc::clone(store)).projection(&goal.id)
        })
        .transpose()?
        .flatten();
    let snapshot = proof_input_snapshot_for_phase(
        store,
        &current,
        current_goal.as_ref().map(|projection| &projection.goal),
        proof,
        None,
        readers,
        current_review,
        pre_tail,
    ).map_err(|error| error.to_string())?;
    if let Some(projection) = &current_goal {
        if !snapshot.sources.iter().any(|source| {
            source.stream_id == format!("goal:{}", projection.goal.id)
                && source.expected_revision == projection.stream_revision
        }) {
            return Err("proof Goal source changed during review authentication".into());
        }
    }
    let inputs = proof.input_set.as_ref().ok_or("missing v2 proof inputs")?;
    let changed = [
        (
            "semantic",
            snapshot.semantic_digest != inputs.semantic_digest,
        ),
        (
            "policy",
            snapshot.policy_digest != inputs.acceptance_policy_digest,
        ),
        ("work", snapshot.work_digest != proof.work_manifest_digest),
        (
            "effects",
            proof.effect_manifest_digest.as_deref() != Some(snapshot.effects.digest.as_str()),
        ),
    ]
    .into_iter()
    .filter_map(|(dimension, differs)| differs.then_some(dimension))
    .collect::<Vec<_>>();
    if !changed.is_empty() {
        let writer = snapshot
            .sources
            .iter()
            .find_map(|source| {
                inputs
                    .source_revisions
                    .get(&source.stream_id)
                    .filter(|expected| **expected != source.expected_revision)
                    .map(|expected| {
                        super::describe_source_advance(store, &source.stream_id, *expected)
                    })
            })
            .unwrap_or_else(|| "writer_advance=not_observed_at_pinned_cuts".into());
        return Err(format!(
            "stale v2 proof: relevant result, source or acceptance policy changed; dimensions={}; {}",
            changed.join(","), writer
        ));
    }
    Ok(snapshot)
}

#[derive(Clone, Copy)]
enum PinnedEventProjection {
    Effects,
    EffectsAndPlans,
    ReceiptIndexEffects,
    ReadDelivery,
}

fn pinned_events(
    store: &RuntimeEventStore,
    source: &str,
    sources: &mut std::collections::BTreeMap<String, u64>,
    since_cursor: u64,
) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
    pinned_stream_events(
        store,
        source,
        sources,
        since_cursor,
        PinnedEventProjection::Effects,
    )
}

fn pinned_stream_events(
    store: &RuntimeEventStore,
    source: &str,
    sources: &mut std::collections::BTreeMap<String, u64>,
    since_cursor: u64,
    projection: PinnedEventProjection,
) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
    pinned_stream_events_with_cut(store, source, sources, since_cursor, projection, None)
}

fn pinned_stream_events_with_cut(
    store: &RuntimeEventStore,
    source: &str,
    sources: &mut std::collections::BTreeMap<String, u64>,
    since_cursor: u64,
    projection: PinnedEventProjection,
    authenticated_tail: Option<&HistoricalSessionCut>,
) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
    let revision = store
        .stream_revision(source)
        .map_err(|error| error.to_string())?;
    if let Some(previous) = sources.insert(source.into(), revision) {
        // Growth on a shared aggregate stream is another actor appending while
        // this proof reads; anything else that moved - including a head that
        // went backwards - is still a change of the source under proof.
        if revision < previous
            || (revision != previous && !super::repin_accepts_moved_head(source, previous, revision))
        {
            return Err("proof source changed during validation".into());
        }
    }
    let mut events = Vec::new();
    let mut through = match authenticated_tail {
        Some(tail) if tail.source.stream_id == source && tail.observed_revision == revision
            && tail.source.expected_revision <= revision => tail.source.expected_revision,
        Some(_) => return Err("original Session material tail does not match current source".into()),
        None => revision,
    };
    loop {
        let page = store.list_stream_before(source, through, 64, 1024 * 1024)?;
        if page.is_empty() {
            if authenticated_tail.is_some() && through > 0 {
                return Err("original Session material page incomplete".into());
            }
            break;
        }
        let current = store.stream_revision(source).map_err(|error| error.to_string())?;
        if current < revision
            || (current != revision && !super::repin_accepts_moved_head(source, revision, current))
        {
            return Err("proof source changed while paging; retry current inputs".into());
        }
        if matches!(projection, PinnedEventProjection::ReceiptIndexEffects) {
            let mut expected = through;
            for event in &page {
                crate::agentic::action_service::validate_receipt_index_record(
                    event, source, expected, revision,
                )?;
                expected = expected
                    .checked_sub(1)
                    .ok_or("receipt-index structural sequence underflow")?;
            }
        }
        if authenticated_tail.is_some() {
            let mut expected = through;
            for event in &page {
                if event.stream_id != source || event.sequence != expected {
                    return Err("original Session material page sequence differs".into());
                }
                expected = expected.checked_sub(1).ok_or("original Session sequence underflow")?;
            }
        }
        through = page
            .last()
            .ok_or("empty proof page")?
            .sequence
            .checked_sub(1)
            .ok_or("invalid proof sequence")?;
        let before_start = page.iter().any(|event| event.commit_cursor < since_cursor);
        for event in page {
            if event.sequence > revision || event.commit_cursor < since_cursor {
                continue;
            }
            if matches!(projection, PinnedEventProjection::ReadDelivery) {
                if matches!(
                    event.kind.as_str(),
                    crate::context_evidence::NATIVE_DELIVERY_KIND
                        | crate::context_evidence::PROCESS_DELIVERY_KIND
                        | "context.nested_read_model_observed"
                ) {
                    events.push(event);
                }
                continue;
            }
            if event.kind.starts_with("execution.agent_tool.")
                && event.payload["effect_kind"] == "read"
            {
                continue;
            }
            if event.kind.starts_with("tool.invocation.") {
                if event.payload["tool_name"]
                    .as_str()
                    .is_some_and(goal_metadata_tool)
                    || super::results::tool_effect_from_store(store, &event)?
                        .is_some_and(|effect| effect.effect_kind == ToolEffectKind::Read)
                {
                    continue;
                }
            } else if event.kind == "tool.execution_plan.created" {
                if !matches!(projection, PinnedEventProjection::EffectsAndPlans) && event.payload["tasks"].as_array().is_some_and(|tasks| {
                    tasks.iter().all(|task| {
                        task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                            || task["effect"]["effect_kind"] == "read"
                    })
                })
                // The review control call is this proof's subject, not
                // background metadata. The proof is assembled while that call
                // is in flight, so this plan is the only record of the exact
                // call that the effect exemption may name.
                && !event.payload["tasks"].as_array().is_some_and(|tasks| {
                    tasks.iter().any(|task| {
                        matches!(
                            task["tool_name"].as_str(),
                            Some(harness_contract::agent_action::TASK_REVIEW_TOOL_ID)
                                | Some(harness_contract::agent_action::OBJECTIVE_REVIEW_TOOL_ID)
                        )
                    })
                }) {
                    continue;
                }
            } else if !event.kind.starts_with("execution.agent_tool.")
                && !event.kind.starts_with("runtime.strategy.")
            {
                continue;
            }
            events.push(event);
        }
        if before_start && !matches!(projection, PinnedEventProjection::ReceiptIndexEffects) {
            break;
        }
    }
    if matches!(projection, PinnedEventProjection::ReceiptIndexEffects) && through != 0 {
        return Err("receipt-index structural history is incomplete".into());
    }
    let current = store.stream_revision(source).map_err(|error| error.to_string())?;
    if current < revision
        || (current != revision && !super::repin_accepts_moved_head(source, revision, current))
    {
        return Err("proof source changed at final page boundary; retry current inputs".into());
    }
    Ok(events)
}

pub(crate) struct VerifiedReadDelivery {
    pub material: crate::context_evidence::ReadDeliveryMaterial,
    pub event: crate::DurableRuntimeEvent,
    pub receipt: crate::DurableRuntimeEvent,
    pub requires_model: bool,
    pub original_scope: Option<harness_contract::goal::ProofEffectScope>,
}

pub(crate) struct VerifiedNestedRead {
    pub parent: crate::context_evidence::ReadDeliveryMaterial,
    pub event: crate::DurableRuntimeEvent,
    pub primary: crate::DurableRuntimeEvent,
    pub index: crate::DurableRuntimeEvent,
    pub body: crate::context_evidence::ReadBodySeal,
    pub extent: crate::context_evidence::ReadExtent,
    pub target: harness_contract::goal::ProofEffectTarget,
    pub step_index: u64,
    pub position: u64,
}

/// Re-authenticate a child delivery against its original parent admission,
/// atomic receipt pair, exact child scope and Runtime-observed wire record.
pub(crate) fn nested_parent_material(payload: &serde_json::Value) -> Result<crate::context_evidence::ReadDeliveryMaterial, String> {
    use crate::context_evidence::{ReadDeliveryGrade, ReadDeliveryRecipient, ReadDeliveryTransport};
    let mut parent = payload["parent"].clone();
    let object = parent.as_object_mut().ok_or("nested parent identity invalid")?;
    if object.contains_key("body") { return Err("nested parent must not claim a read body".into()); }
    object.insert("body".into(), payload["child"]["body"].clone());
    let seal = crate::context_evidence::decode_read_acquisition(&parent).map_err(str::to_string)?;
    let recipient: ReadDeliveryRecipient = serde_json::from_value(payload["recipient"].clone()).map_err(|e| e.to_string())?;
    let transport: ReadDeliveryTransport = serde_json::from_value(payload["transport"].clone()).map_err(|e| e.to_string())?;
    let material = crate::context_evidence::ReadDeliveryMaterial {
        schema_version:1, grade:ReadDeliveryGrade::ProviderModelObserved, acquisition:seal.clone(),
        acquisition_event_id:payload["parent_event_id"].as_str().ok_or("nested primary missing")?.into(),
        acquisition_cursor:payload["parent_cursor"].as_u64().ok_or("nested cursor missing")?,
        index_event_id:payload["index_event_id"].as_str().ok_or("nested index missing")?.into(),
        session_id:seal.session_id.clone(), execution_id:seal.graph_id.clone(), turn_id:seal.step.turn_id.clone(),
        tool_invocation_id:seal.invocation_id.clone(), recipient, presentation_sha256:payload["presentation_sha256"].as_str().ok_or("nested presentation missing")?.into(),
        presentation_bytes:payload["presentation_bytes"].as_u64().ok_or("nested presentation size missing")?,
        body_omitted:true, delivered_extent:None, transport,
    };
    crate::context_evidence::decode_read_delivery(&serde_json::to_value(&material).map_err(|e| e.to_string())?).map_err(str::to_string)?;
    Ok(material)
}

pub(crate) fn verify_nested_read(
    reader: &mut dyn super::read_source::ReadAuthSource,
    event: &crate::DurableRuntimeEvent,
    session: &str,
    turn: &str,
    root: Option<&str>,
    packet: Option<&harness_contract::agent::AgentTaskPacket>,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Option<VerifiedNestedRead>, String> {
    use crate::context_evidence::{ReadDeliveryRecipient, ReadDeliveryTransport, ReadExtent};
    if event.kind != "context.nested_read_model_observed" || event.schema_version != 1
        || event.scope != RuntimeEventScope::Session || event.actor.as_deref() != Some("conversation_runtime")
        || event.status.as_deref() != Some("confirmed") || event.payload["schema_version"] != 1 { return Ok(None); }
    let payload = &event.payload;
    let material = nested_parent_material(payload)?;
    let seal = &material.acquisition;
    if !matches!(seal.tool_name.as_str(), "tool_orchestrate" | "grep_search") || seal.session_id != session || seal.step.turn_id != turn
        || event.stream_id != format!("session:{session}") { return Ok(None); }
    let ReadDeliveryTransport::NativeProviderContinuation { raw_ref, .. } = &material.transport else { return Ok(None); };
    let owner_event = match (&material.recipient, packet) {
        (ReadDeliveryRecipient::Root { owner, .. }, None) => {
            if root != Some(seal.graph_id.as_str()) { return Ok(None); }
            let owner_ref = harness_contract::goal::ProofInputEvent { stream_id:owner.stream_id.clone(), sequence:owner.sequence,
                event_id:owner.event_id.clone(), payload_digest:owner.payload_digest.clone() };
            let owner_event = verify_material_event_with_reader(reader, &owner_ref, sources)?;
            if !crate::context_evidence::root_read_owner_matches(owner, &owner_event, &seal)
                || owner_event.payload["full_output_ref"] != format!("tool://{}", raw_ref.id()) { return Ok(None); }
            Some(owner_event)
        }
        (ReadDeliveryRecipient::AgentTask { identity }, Some(packet)) => {
            if *identity != packet.assignment.execution_identity || seal.graph_id != packet.graph_id()
                || seal.node_id != packet.node_id() || seal.attempt != packet.attempt
                || seal.step.run_ref.as_deref() != Some(packet.run_id())
                || seal.step.actor_ref.as_deref() != Some(packet.agent_id())
                || !packet.allowed_tools.contains(&seal.tool_name)
                || !packet.allowed_tools.iter().any(|t| Some(t.as_str()) == payload["child"]["tool_name"].as_str())
                || !packet.binding.as_ref().is_some_and(|b| b.tool_contract_refs.contains(&seal.tool_name)
                    && b.tool_contract_refs.iter().any(|t| Some(t.as_str()) == payload["child"]["tool_name"].as_str())) { return Ok(None); }
            None
        }
        _ => return Ok(None),
    };
    let primary = reader.event_by_idempotency_key(&seal.primary_stream, &seal.primary_key)?.ok_or("nested primary absent")?;
    let index = reader.event_by_idempotency_key(&seal.index_stream, &seal.index_key)?.ok_or("nested index absent")?;
    let key = seal.primary_key.strip_suffix(":read-receipt").ok_or("nested key invalid")?;
    let origin: harness_contract::step::CallOrigin = serde_json::from_value(seal.origin.clone()).map_err(|e| e.to_string())?;
    if !matches!(origin.source, harness_contract::step::CallSource::Provider { ref attempt_id } if !attempt_id.is_empty())
        || seal.primary_stream != format!("execution-effect:{key}")
        || seal.index_stream != format!("execution-agent-receipts:{}:{}:{}",seal.graph_id,seal.node_id,seal.attempt)
        || seal.index_key != format!("agent-tool-receipt:{key}")
        || primary.idempotency_key.as_deref() != Some(seal.primary_key.as_str())
        || index.idempotency_key.as_deref() != Some(seal.index_key.as_str())
        || primary.payload["tool_use_id"] != seal.invocation_id || primary.payload["tool_name"] != seal.tool_name
        || primary.payload["input_sha256"] != seal.input_sha256
        || primary.payload["replay"]["session_id"] != seal.session_id
        || primary.payload["replay"]["call_id"] != seal.invocation_id
        || primary.payload["replay"]["origin"] != seal.origin
        || !primary.refs.iter().any(|r| r.kind == "execution_graph" && r.id == seal.graph_id)
        || !primary.refs.iter().any(|r| r.kind == "execution_node" && r.id == seal.node_id)
        || !primary.refs.iter().any(|r| r.kind == "agent_attempt" && r.id == seal.attempt.to_string())
    { return Err("nested parent binding invalid".into()); }
    if primary.kind != "execution.read.receipt" || index.kind != "execution.agent_tool.receipt"
        || primary.actor.as_deref() != Some("governed_tool") || index.actor != primary.actor
        || primary.event_id != material.acquisition_event_id || primary.commit_cursor != material.acquisition_cursor
        || index.event_id != material.index_event_id || event.commit_cursor <= primary.commit_cursor
        || primary.payload["outcome"]["status"] != "executed"
        || primary.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD] != index.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD]
    { return Err("nested receipt pair invalid".into()); }
    let descriptor = crate::context_evidence::native_acquisition_descriptor(&primary, &seal).map_err(str::to_string)?;
    if owner_event.as_ref().is_some_and(|owner| owner.commit_cursor <= primary.commit_cursor || event.commit_cursor <= owner.commit_cursor) {
        return Err("nested owner order invalid".into());
    }
    if primary.payload["replay"]["parent_execution"].is_null() {
        if owner_event.is_none() { return Err("root nested owner absent".into()); }
        let intent = reader.event_by_idempotency_key(&seal.primary_stream, &format!("{key}:read-intent"))?.ok_or("nested intent absent")?;
        crate::context_evidence::authenticate_root_read_intent(&intent, &primary, &index, &seal).map_err(str::to_string)?;
        let graph = reader.transaction_stream_revision_exact(&intent.transaction_id, &seal.graph_id)?.ok_or("nested graph admission absent")?;
        if Some(graph.expected_revision) != intent.payload["root_effect_owner"]["graph_revision"].as_u64()
            || graph.expected_revision != graph.committed_revision { return Err("nested graph admission changed".into()); }
    } else if primary.transaction_id != index.transaction_id || primary.commit_cursor != index.commit_cursor
        || index.transaction_index <= primary.transaction_index
        || primary.payload["replay"]["parent_execution"]["execution_id"] != seal.graph_id
        || primary.payload["replay"]["parent_execution"]["node_id"] != seal.node_id
        || primary.payload["replay"]["parent_execution_attempt"] != seal.attempt
        || primary.payload["replay"]["origin"] != seal.origin
        || primary.payload["outcome"] != index.payload["outcome"]
        || primary.payload["effect"] != index.payload["effect"]
        || primary.payload["resolved_effect_scope"] != index.payload["resolved_effect_scope"]
        || !index.refs.iter().any(|r| r.kind == "agent_attempt" && r.id == seal.attempt.to_string())
    { return Err("nested delegated receipt mismatch".into()); }
    let child = &payload["child"];
    let children = primary.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD].as_array().ok_or("nested children absent")?;
    if children.iter().filter(|candidate| *candidate == child).count() != 1 || child["schema_version"] != 1
        || child["parent_tool_use_id"] != seal.invocation_id || child["parent_idempotency_key"] != key
        || child["parent_descriptor_hash"] != descriptor.descriptor_hash
        || !matches!(child["tool_name"].as_str(), Some("read_file" | "grep_search")) { return Err("nested child is foreign".into()); }
    let step_index = child["step_index"].as_u64().ok_or("nested step missing")?;
    let position = child["position"].as_u64().ok_or("nested position missing")?;
    if children.iter().filter(|other| other["step_index"] == step_index && other["position"] == position
        && other["batch_index"] == child["batch_index"] && other["window_index"] == child["window_index"]).count() != 1 { return Err("ambiguous nested position".into()); }
    let original_input = primary.payload["original_input"].as_str().ok_or("nested original input absent")?;
    let source_input = payload["source_input"].as_str().ok_or("nested source input absent")?;
    if crate::context_evidence::read_sha256(original_input.as_bytes()) != seal.input_sha256
        || owner_event.as_ref().is_some_and(|owner| owner.payload["input_hash"] != crate::tool_invocation::stable_hash(source_input))
    { return Err("nested original input changed".into()); }
    let parent_input: serde_json::Value = serde_json::from_str(original_input).map_err(|e| e.to_string())?;
    let strict_input = harness_contract::step::parse_strict_json(original_input).map_err(|e| e.to_string())?;
    let strict_source = harness_contract::step::parse_strict_json(source_input).map_err(|e| e.to_string())?;
    if primary.payload["replay"]["payload_digest"]
        != harness_contract::step::canonical_digest("tool-effect-payload", &strict_input).map_err(|e| e.to_string())?
        || strict_source != strict_input
        || !primary.payload["replay"]["invocation"]["intent"]["normalized_input"].is_null()
        || primary.payload["replay"]["invocation"]["origin"] != seal.origin
        || primary.payload["replay"]["invocation"]["invocation_id"] != seal.invocation_id
    { return Err("nested original compiled input mismatch".into()); }
    let target = if child["tool_name"] == "grep_search" {
        if child["parent_tool_name"] != seal.tool_name || !crate::context_evidence::grep_original_input_matches(&parent_input, child) {
            return Err("grep original input mismatch".into());
        }
        let effect: harness_contract::tool::ToolEffectDescriptor = serde_json::from_value(child["effect"].clone()).map_err(|e| e.to_string())?;
        let scope = serde_json::from_value(primary.payload["resolved_effect_scope"].clone()).map_err(|e| e.to_string())?;
        if seal.tool_name == "grep_search" && child["batch_index"].is_number() {
            crate::context_evidence::grep_read_target_for_parent(&scope, child, &parent_input, &descriptor)
                .ok_or("grep original target unknown")?
        } else if seal.tool_name == "tool_orchestrate" {
            crate::context_evidence::grep_read_target_for_composite(&scope, child, &parent_input, &descriptor)
                .ok_or("grep composite original target unknown")?
        } else {
            if !effect.scopes.iter().all(|s| descriptor.scopes.contains(s)) { return Err("grep original scope mismatch".into()); }
            crate::context_evidence::grep_read_target(&scope, child).ok_or("grep original target unknown")?
        }
    } else {
    let step = parent_input["steps"].as_array().and_then(|steps| steps.get(step_index as usize)).ok_or("nested step absent")?;
    let call = match step["op"].as_str() {
        Some("call") if position == 0 => step,
        Some("batch") => step["calls"].as_array().and_then(|calls| calls.get(position as usize)).ok_or("nested call absent")?,
        _ => return Err("nested call shape unsupported".into()),
    };
    if step["id"] != child["step_id"] || parent_input["steps"].as_array().is_none_or(|steps|
        steps.iter().filter(|candidate| candidate["id"] == step["id"]).count() != 1)
        || call["tool"] != "read_file" || call["input"] != child["input"]
        || crate::context_evidence::read_sha256(&serde_json::to_vec(&child["input"]).map_err(|e| e.to_string())?) != child["input_sha256"] { return Err("nested original input mismatch".into()); }
    let effect: harness_contract::tool::ToolEffectDescriptor = serde_json::from_value(child["effect"].clone()).map_err(|e| e.to_string())?;
    if effect.tool_id != "read_file" || effect.effect_kind != harness_contract::tool::ToolEffectKind::Read || effect.scopes.len() != 1
        || !descriptor.scopes.contains(&effect.scopes[0]) { return Err("nested child descriptor invalid".into()); }
    let scope: harness_contract::goal::ProofEffectScope = serde_json::from_value(primary.payload["resolved_effect_scope"].clone()).map_err(|e| e.to_string())?;
    let mut targets = scope.targets.iter().filter(|target| target.declared_scope == effect.scopes[0]);
    let target = targets.next().ok_or("nested child scope absent")?.clone();
    if (!scope.complete && !crate::context_evidence::original_read_file_domain(&descriptor, &scope))
        || targets.next().is_some() || !target.exact || !target.alias_checked
        || target.access_mode != harness_contract::context::WorkspaceAccessMode::Read
        || target.coverage != harness_contract::context::EvidenceCoverageKind::ScopedContent
        || target.declared_scope.resource != harness_contract::policy::PermissionResource::File
        || target.declared_scope.operation != harness_contract::policy::PermissionOperation::Read
        || target.path.object_kind != harness_contract::context::WorkspaceObjectKind::File { return Err("nested child scope invalid".into()); }
        target
    };
    let body: crate::context_evidence::ReadBodySeal = serde_json::from_value(child["body"].clone()).map_err(|e| e.to_string())?;
    let extent: ReadExtent = serde_json::from_value(payload["delivered_extent"].clone()).map_err(|e| e.to_string())?;
    let ReadExtent::FileLines {start_line,line_count,total_lines,ends_with_newline,body_sha256,body_bytes,source_omitted} = &extent else { return Err("nested extent invalid".into()); };
    let ReadExtent::FileLines {start_line: original_start,line_count: original_count,total_lines: original_total,source_omitted: original_omitted,ends_with_newline: original_newline,body_sha256: original_hash,body_bytes: original_bytes} = &body.extent else { return Err("nested source extent invalid".into()); };
    let prefixes = child["prefixes"].as_array().ok_or("nested prefix seals absent")?;
    let full = *line_count == *original_count && body_sha256 == original_hash
        && *body_bytes == *original_bytes && ends_with_newline == original_newline;
    if *start_line != *original_start || *total_lines != *original_total || (*line_count == 0 && !full) || *line_count > *original_count
        || (!source_omitted && (*original_omitted || *line_count != *original_count))
        || (!full && prefixes.iter().filter(|p| p["line_count"] == *line_count && p["bytes"] == *body_bytes
            && p["sha256"] == *body_sha256 && p["ends_with_newline"] == *ends_with_newline).count() != 1) { return Err("nested delivered prefix mismatch".into()); }
    for source in [&seal.primary_stream,&seal.index_stream,&seal.graph_id] {
        let revision=reader.stream_revision(source)?;
        if sources.insert(source.clone(),revision).is_some_and(|old| old != revision) { return Err("nested source moved".into()); }
    }
    Ok(Some(VerifiedNestedRead { parent:material,event:event.clone(),primary,index,body,extent,target,step_index,position }))
}

#[cfg(test)]
pub(crate) fn verify_nested_read_for_test(
    store: &RuntimeEventStore, event: &crate::DurableRuntimeEvent,
    session: &str, turn: &str, root: Option<&str>,
    packet: Option<&harness_contract::agent::AgentTaskPacket>,
) -> Result<bool, String> {
    let mut reader = super::read_source::StoreReadSource(store);
    let mut sources = std::collections::BTreeMap::new();
    Ok(verify_nested_read(&mut reader, event, session, turn, root, packet, &mut sources)?.is_some())
}

impl VerifiedReadDelivery {
    pub(crate) fn receipt_reference(&self) -> String {
        match &self.material.recipient {
            crate::context_evidence::ReadDeliveryRecipient::Root { owner, .. } => {
                format!("runtime-event:{}", owner.event_id)
            }
            crate::context_evidence::ReadDeliveryRecipient::AgentTask { .. } => format!(
                "{}#{}",
                self.receipt.stream_id,
                self.receipt.payload["sequence"]
                    .as_u64()
                    .unwrap_or_default(),
            ),
        }
    }

    pub(crate) fn body_delivered(&self) -> bool {
        !self.material.body_omitted
            && (!self.requires_model
                || self.material.grade
                    == crate::context_evidence::ReadDeliveryGrade::ProviderModelObserved)
    }
}

pub(crate) struct ReviewDeliverySnapshot {
    pub reads: Vec<VerifiedReadDelivery>,
    pub nested_reads: Vec<VerifiedNestedRead>,
    pub sources: std::collections::BTreeMap<String, u64>,
}

pub(crate) struct DeliveredFileRead {
    pub path: harness_contract::context::WorkspacePathIdentity,
    pub scope: harness_contract::policy::PermissionScope,
    pub cursor: u64,
    pub receipt_refs: BTreeSet<String>,
}

impl ReviewDeliverySnapshot {
    pub(crate) fn complete_file_reads(&self) -> Result<Vec<DeliveredFileRead>, String> {
        use crate::context_evidence::ReadExtent;
        type FileWindow = (
            u64,
            u64,
            u64,
            BTreeSet<String>,
            harness_contract::context::WorkspacePathIdentity,
            harness_contract::policy::PermissionScope,
        );
        type DeliveredPages = (
            u64,
            std::collections::BTreeMap<u64, (String, u64)>,
            BTreeSet<String>,
        );
        let mut output_pages = std::collections::BTreeMap::<(String, u64), DeliveredPages>::new();
        for page in self.reads.iter().filter(|page| page.body_delivered()) {
            let body = &page.material.acquisition.body;
            let ReadExtent::ArtifactChunks {
                total_chunks,
                chunks,
            } = &body.extent
            else {
                continue;
            };
            let stored = output_pages
                .entry((body.source_sha256.clone(), body.source_bytes))
                .or_insert_with(|| {
                    (
                        *total_chunks,
                        std::collections::BTreeMap::new(),
                        BTreeSet::new(),
                    )
                });
            if stored.0 != *total_chunks {
                return Err("delivered original output pages disagree about content extent".into());
            }
            for chunk in chunks {
                let identity = (chunk.body_sha256.clone(), chunk.body_bytes);
                if stored
                    .1
                    .insert(chunk.index, identity.clone())
                    .is_some_and(|old| old != identity)
                {
                    return Err("delivered original output pages conflict at one chunk".into());
                }
            }
            stored.2.insert(page.receipt_reference());
        }
        let mut versions = std::collections::BTreeMap::<String, (u64, Vec<FileWindow>)>::new();
        for read in &self.reads {
            let seal = &read.material.acquisition;
            let ReadExtent::FileLines {
                start_line,
                line_count,
                total_lines,
                source_omitted,
                ..
            } = &seal.body.extent
            else {
                continue;
            };
            let mut references = BTreeSet::from([read.receipt_reference()]);
            if read.requires_model && (*source_omitted || !read.body_delivered()) {
                continue;
            }
            if !read.body_delivered() {
                let Some((total, chunks, receipts)) =
                    output_pages.get(&(seal.body.output_sha256.clone(), seal.body.output_bytes))
                else {
                    continue;
                };
                if *total != chunks.len() as u64 || !chunks.keys().copied().eq(0..*total) {
                    continue;
                }
                references.extend(receipts.iter().cloned());
            }
            let scope = read
                .original_scope
                .as_ref()
                .ok_or("complete reviewer File witness has no original scope v2")?;
            if !scope.complete {
                return Err("complete reviewer File witness has incomplete original scope".into());
            }
            let paths = scope
                .targets
                .iter()
                .filter(|target| {
                    target.access_mode == harness_contract::context::WorkspaceAccessMode::Read
                        && target.path.object_kind
                            == harness_contract::context::WorkspaceObjectKind::File
                        && target.exact
                })
                .map(|target| (target.path.clone(), target.declared_scope.clone()))
                .collect::<Vec<_>>();
            if paths.len() != 1 {
                return Err(
                    "complete reviewer File witness has no unique original canonical path".into(),
                );
            }
            for (path, scope) in paths {
                let key = super::proof_value_digest(&serde_json::json!([
                    path.workspace_id,
                    path.repository_id,
                    path.workspace_relative_path,
                    path.repository_relative_path,
                    seal.body.source_sha256,
                    seal.body.source_bytes,
                    total_lines,
                ]));
                let begin = start_line
                    .checked_sub(1)
                    .ok_or("delivered file range is not one based")?;
                let end = begin
                    .checked_add(*line_count)
                    .ok_or("delivered file range overflow")?;
                if end > *total_lines {
                    return Err("delivered file range exceeds its actual source".into());
                }
                versions
                    .entry(key)
                    .or_insert_with(|| (*total_lines, Vec::new()))
                    .1
                    .push((
                        begin,
                        end,
                        read.material.acquisition_cursor,
                        references.clone(),
                        path,
                        scope,
                    ));
            }
        }
        let mut complete = Vec::new();
        for (_, (total, mut windows)) in versions {
            windows.sort_by_key(|window| std::cmp::Reverse(window.2));
            let mut extents = std::collections::BTreeMap::<u64, u64>::new();
            let mut references = BTreeSet::new();
            for (begin, end, cursor, receipts, path, scope) in windows {
                references.extend(receipts);
                let mut begin = begin;
                let mut end = end;
                if let Some((&start, &stop)) = extents.range(..=begin).next_back() {
                    if stop >= begin {
                        begin = start;
                        end = end.max(stop);
                        extents.remove(&start);
                    }
                }
                loop {
                    let next = extents
                        .range(begin..)
                        .next()
                        .map(|(&start, &stop)| (start, stop));
                    match next {
                        Some((start, stop)) if start <= end => {
                            end = end.max(stop);
                            extents.remove(&start);
                        }
                        _ => break,
                    }
                }
                extents.insert(begin, end);
                if extents.get(&0) == Some(&total) {
                    complete.push(DeliveredFileRead {
                        scope,
                        path,
                        cursor,
                        receipt_refs: references,
                    });
                    break;
                }
            }
        }
        Ok(complete)
    }
}

pub(crate) fn review_delivery_snapshot(
    store: &RuntimeEventStore,
    program: &AgenticProgramProjection,
    packet: Option<&harness_contract::agent::AgentTaskPacket>,
    selected: Option<&[harness_contract::goal::ProofInputEvent]>,
) -> Result<ReviewDeliverySnapshot, super::results::ReviewEvidenceError> {
    let stream = format!("session:{}", program.session_id);
    let mut sources = std::collections::BTreeMap::new();
    let events = match selected {
        Some(selected) => selected
            .iter()
            .filter(|material| material.stream_id == stream)
            .map(|material| verify_material_event(store, material, &mut sources))
            .collect::<Result<Vec<_>, _>>()?,
        None => pinned_stream_events(
            store,
            &stream,
            &mut sources,
            0,
            PinnedEventProjection::ReadDelivery,
        )?,
    };
    review_delivery_snapshot_from_events(
        &mut super::read_source::StoreReadSource(store),
        program,
        packet,
        events,
        sources,
    )
}

pub(crate) fn review_delivery_snapshot_from_events(
    reader: &mut dyn super::read_source::ReadAuthSource,
    program: &AgenticProgramProjection,
    packet: Option<&harness_contract::agent::AgentTaskPacket>,
    events: Vec<crate::DurableRuntimeEvent>,
    sources: std::collections::BTreeMap<String, u64>,
) -> Result<ReviewDeliverySnapshot, super::results::ReviewEvidenceError> {
    read_delivery_snapshot_for_scope(
        reader,
        &program.session_id,
        &program.turn_id,
        program.root_execution_id.as_deref(),
        packet,
        events,
        sources,
    )
}

// A Root read belongs to its authenticated original execution, including plain
// Session turns without an AgenticProgram. Both consumers use the same verifier.
pub(crate) fn read_delivery_snapshot_for_scope(
    reader: &mut dyn super::read_source::ReadAuthSource,
    session_id: &str,
    turn_id: &str,
    root_execution_id: Option<&str>,
    packet: Option<&harness_contract::agent::AgentTaskPacket>,
    events: Vec<crate::DurableRuntimeEvent>,
    mut sources: std::collections::BTreeMap<String, u64>,
) -> Result<ReviewDeliverySnapshot, super::results::ReviewEvidenceError> {
    use crate::context_evidence::{
        ReadDeliveryGrade, ReadDeliveryTransport, NATIVE_DELIVERY_KIND, PROCESS_DELIVERY_KIND,
        READ_ACQUISITION_FIELD,
    };
    let mut reads = Vec::new();
    let mut nested_reads = Vec::new();
    for event in events {
        if event.kind == "context.nested_read_model_observed" {
            if let Some(nested) = verify_nested_read(reader, &event, session_id, turn_id, root_execution_id, packet, &mut sources)? {
                nested_reads.push(nested);
            }
            continue;
        }
        let expected = match event.kind.as_str() {
            NATIVE_DELIVERY_KIND => ("conversation_runtime", "confirmed"),
            PROCESS_DELIVERY_KIND => ("runtime.process_jsonl", "sent"),
            _ => continue,
        };
        if event.scope != RuntimeEventScope::Session
            || event.schema_version != 1
            || event.actor.as_deref() != Some(expected.0)
            || event.status.as_deref() != Some(expected.1)
        {
            continue;
        }
        let material = crate::context_evidence::decode_read_delivery(&event.payload)
            .map_err(str::to_string)?;
        if material.session_id != session_id || material.turn_id != turn_id {
            continue;
        }
        let seal = &material.acquisition;
        let recipient_matches = match (&material.recipient, packet) {
            (
                crate::context_evidence::ReadDeliveryRecipient::AgentTask { identity },
                Some(packet),
            ) => {
                *identity == packet.assignment.execution_identity
                    && seal.graph_id == packet.graph_id()
                    && seal.node_id == packet.node_id()
                    && seal.attempt == packet.attempt
                    && seal.step.run_ref.as_deref() == Some(packet.run_id())
                    && packet.allowed_tools.contains(&seal.tool_name)
                    && packet
                        .binding
                        .as_ref()
                        .is_some_and(|binding| binding.tool_contract_refs.contains(&seal.tool_name))
            }
            (crate::context_evidence::ReadDeliveryRecipient::Root { owner, .. }, None) => {
                let reference = harness_contract::goal::ProofInputEvent {
                    stream_id: owner.stream_id.clone(),
                    sequence: owner.sequence,
                    event_id: owner.event_id.clone(),
                    payload_digest: owner.payload_digest.clone(),
                };
                let original = verify_material_event_with_reader(reader, &reference, &mut sources)?;
                crate::context_evidence::root_read_owner_matches(owner, &original, seal)
                    && root_execution_id == Some(owner.binding.root_execution_id.as_str())
                    && super::binding_is_owned_by_root(&owner.binding, root_execution_id)
            }
            _ => false,
        };
        if !recipient_matches {
            continue;
        }
        let primary = reader
            .event_by_idempotency_key(&seal.primary_stream, &seal.primary_key)?
            .ok_or("delivered read has no original primary receipt")?;
        let receipt = reader
            .event_by_idempotency_key(&seal.index_stream, &seal.index_key)?
            .ok_or("delivered read has no original index receipt")?;
        let seal_value = serde_json::to_value(seal).map_err(|error| error.to_string())?;
        if primary.kind != "execution.read.receipt"
            || receipt.kind != "execution.agent_tool.receipt"
            || primary.actor.as_deref() != Some("governed_tool")
            || receipt.actor != primary.actor
            || primary.scope != RuntimeEventScope::ExecutionNode
            || receipt.scope != primary.scope
            || primary.transaction_id != receipt.transaction_id
            || primary.commit_cursor != receipt.commit_cursor
            || primary.event_id != material.acquisition_event_id
            || primary.commit_cursor != material.acquisition_cursor
            || receipt.event_id != material.index_event_id
            || event.commit_cursor <= primary.commit_cursor
            || primary.payload.get(READ_ACQUISITION_FIELD) != Some(&seal_value)
            || receipt.payload.get(READ_ACQUISITION_FIELD) != Some(&seal_value)
            || primary.payload["outcome"]["status"] != "executed"
            || receipt.payload["outcome"]["status"] != "executed"
            || primary.payload["tool_use_id"] != seal.invocation_id
            || primary.payload["tool_name"] != seal.tool_name
            || primary.payload["input_sha256"] != seal.input_sha256
            || seal.index_stream
                != format!(
                    "execution-agent-receipts:{}:{}:{}",
                    seal.graph_id, seal.node_id, seal.attempt
                )
        {
            return Err(
                "read delivery does not authenticate its original physical acquisition".into(),
            );
        }
        let descriptor = super::results::receipt_effect_descriptor(&primary)?;
        let original_scope = read_original_scope(&primary, &descriptor, &material.acquisition)?;
        if primary.payload["replay"]["parent_execution"].is_null() {
            super::results::authenticate_root_receipt_owner_with_reader(
                reader,
                &primary,
                &mut sources,
            )?;
        }
        if primary.payload["resolved_effect_scope"] != receipt.payload["resolved_effect_scope"] {
            return Err("read delivery scope disagrees with its original primary".into());
        }
        let origin: harness_contract::step::CallOrigin =
            serde_json::from_value(seal.origin.clone()).map_err(|error| error.to_string())?;
        let authenticated_actor = match (
            event.kind.as_str(),
            &material.grade,
            &material.transport,
            &origin.source,
        ) {
            (
                PROCESS_DELIVERY_KIND,
                ReadDeliveryGrade::AgentChannelSend,
                ReadDeliveryTransport::ProcessChannelSend {
                    worker_id,
                    request_id,
                    step_id,
                    request_fingerprint,
                    ..
                },
                harness_contract::step::CallSource::Process {
                    worker_id: origin_worker,
                    ..
                },
            ) => {
                if worker_id != origin_worker
                    || Some(worker_id.as_str()) != seal.step.run_ref.as_deref()
                    || request_id != &seal.invocation_id
                    || step_id != &seal.step.step_id
                {
                    return Err("Process read delivery has a foreign worker or Step".into());
                }
                let packet = packet.ok_or("Process delivery requires an immutable Agent packet")?;
                let identity = serde_json::json!([
                    packet.session_id(),
                    packet.graph_id(),
                    packet.node_id(),
                    packet.run_id(),
                    packet.agent_id(),
                    packet.attempt,
                    packet
                        .binding
                        .as_ref()
                        .map(|binding| binding.binding_digest.as_str()),
                ]);
                let transport_stream = format!(
                    "agent-process-transport:{:x}",
                    Sha256::digest(identity.to_string().as_bytes())
                );
                let transport_key =
                    format!("completed:{:x}", Sha256::digest(request_id.as_bytes()));
                let completed = reader
                    .event_by_idempotency_key(&transport_stream, &transport_key)?
                    .ok_or("Process read delivery lost its original response identity")?;
                let output = completed.payload["response"]["output"]
                    .as_str()
                    .ok_or("Process read response has no original output")?;
                if completed.kind != "agent.process_request_completed"
                    || completed.payload["fingerprint"] != *request_fingerprint
                    || crate::context_evidence::read_sha256(output.as_bytes())
                        != material.presentation_sha256
                    || output.len() as u64 != material.presentation_bytes
                    || completed.commit_cursor >= event.commit_cursor
                {
                    return Err("Process delivered body does not match its actual response".into());
                }
                let revision = reader
                    .stream_revision(&transport_stream)
                    .map_err(|error| error.to_string())?;
                if sources
                    .insert(transport_stream, revision)
                    .is_some_and(|previous| previous != revision)
                {
                    return Err("Process delivery response source changed during validation".into());
                }
                Some(packet.agent_id())
            }
            (
                NATIVE_DELIVERY_KIND,
                ReadDeliveryGrade::ProviderModelObserved,
                ReadDeliveryTransport::NativeProviderContinuation { .. },
                harness_contract::step::CallSource::Provider { .. },
            ) => packet.map(|packet| packet.assignment.execution_identity.principal_id()),
            _ => {
                return Err(
                    "read delivery grade has no matching original executor provenance".into(),
                );
            }
        };
        if authenticated_actor.is_some_and(|actor| seal.step.actor_ref.as_deref() != Some(actor)) {
            continue;
        }
        let requires_model = match packet {
            Some(packet) => read_requires_provider_model(packet, &material.acquisition, &receipt)?,
            None => false,
        };
        for source in [&seal.primary_stream, &seal.index_stream] {
            let revision = reader
                .stream_revision(source)
                .map_err(|error| error.to_string())?;
            if sources
                .insert(source.clone(), revision)
                .is_some_and(|previous| previous != revision)
            {
                return Err("read delivery acquisition source changed during validation".into());
            }
        }
        reads.push(VerifiedReadDelivery {
            material,
            event,
            receipt,
            requires_model,
            original_scope,
        });
    }
    for (source, expected) in &sources {
        if reader
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != *expected
        {
            return Err("read delivery source moved during validation".into());
        }
    }
    Ok(ReviewDeliverySnapshot { reads, nested_reads, sources })
}

#[cfg(test)]
impl crate::RuntimeServices {
    pub(crate) fn assert_read_delivery_actor_matrix_for_test(
        &self,
        program: &AgenticProgramProjection,
        packet: &harness_contract::agent::AgentTaskPacket,
        invocation: &str,
        require_opposite_transport: bool,
    ) {
        use crate::context_evidence::{
            ReadDeliveryGrade, ReadDeliveryTransport, NATIVE_DELIVERY_KIND, PROCESS_DELIVERY_KIND,
            READ_ACQUISITION_FIELD,
        };
        let store = self.event_store();
        assert_ne!(
            packet.assignment.execution_identity.principal_id(),
            packet.agent_id()
        );
        let events = store
            .list_stream(&format!("session:{}", packet.session_id()))
            .unwrap();
        let event = events
            .iter()
            .find(|event| {
                matches!(
                    event.kind.as_str(),
                    NATIVE_DELIVERY_KIND | PROCESS_DELIVERY_KIND
                ) && crate::context_evidence::decode_read_delivery(&event.payload)
                    .is_ok_and(|material| material.tool_invocation_id == invocation)
            })
            .unwrap()
            .clone();
        let material = crate::context_evidence::decode_read_delivery(&event.payload).unwrap();
        let seal = &material.acquisition;
        let primary = store
            .event_by_idempotency_key(&seal.primary_stream, &seal.primary_key)
            .unwrap()
            .unwrap();
        let index = store
            .event_by_idempotency_key(&seal.index_stream, &seal.index_key)
            .unwrap()
            .unwrap();
        let actual = review_delivery_snapshot(store, program, Some(packet), None).unwrap();
        assert!(
            actual
                .reads
                .iter()
                .any(|read| read.event.event_id == event.event_id && read.material == material),
            "real consumer must retain the actual producer delivery"
        );
        let run = |candidate: crate::DurableRuntimeEvent,
                   primary: crate::DurableRuntimeEvent,
                   index: crate::DurableRuntimeEvent,
                   expected_packet: &harness_contract::agent::AgentTaskPacket| {
            let mut reader = super::read_source::KeyOverrideSource {
                source: super::read_source::StoreReadSource(store),
                key: |stream: &str, key: &str| {
                    if stream == seal.primary_stream && key == seal.primary_key {
                        Ok(Some(primary.clone()))
                    } else if stream == seal.index_stream && key == seal.index_key {
                        Ok(Some(index.clone()))
                    } else {
                        store
                            .event_by_idempotency_key(stream, key)
                            .map_err(|error| error.to_string())
                    }
                },
            };
            review_delivery_snapshot_from_events(
                &mut reader,
                program,
                Some(expected_packet),
                vec![candidate],
                std::collections::BTreeMap::new(),
            )
        };
        let accepted = run(event.clone(), primary.clone(), index.clone(), packet).unwrap();
        assert_eq!(accepted.reads.len(), 1);
        assert_eq!(accepted.reads[0].material, material);
        let refused = |label: &str, result: Result<ReviewDeliverySnapshot, super::results::ReviewEvidenceError>| {
            assert!(
                result.is_err()
                    || result
                        .as_ref()
                        .is_ok_and(|snapshot| snapshot.reads.is_empty()),
                "{label}: a foreign delivery entered the consumer read set"
            );
        };
        let physical_error = |label: &str, result: Result<ReviewDeliverySnapshot, super::results::ReviewEvidenceError>| {
            let error = result
                .err()
                .unwrap_or_else(|| panic!("{label}: receipt forgery accepted"));
            assert!(
                error.contains("original physical acquisition"),
                "{label}: {error}"
            );
        };
        let mut wrong_actor = material.clone();
        wrong_actor.acquisition.step.actor_ref = Some(
            match material.grade {
                ReadDeliveryGrade::ProviderModelObserved => packet.agent_id(),
                ReadDeliveryGrade::AgentChannelSend => {
                    packet.assignment.execution_identity.principal_id()
                }
            }
            .to_string(),
        );
        let mut wrong_event = event.clone();
        wrong_event.payload = serde_json::to_value(&wrong_actor).unwrap();
        physical_error(
            "delivery-only actor substitution",
            run(wrong_event.clone(), primary.clone(), index.clone(), packet),
        );
        let mut matching_primary = primary.clone();
        let mut matching_index = index.clone();
        let wrong_seal = serde_json::to_value(&wrong_actor.acquisition).unwrap();
        matching_primary.payload[READ_ACQUISITION_FIELD] = wrong_seal.clone();
        matching_index.payload[READ_ACQUISITION_FIELD] = wrong_seal;
        assert!(crate::context_evidence::decode_read_delivery(&wrong_event.payload).is_ok());
        let rejected = run(wrong_event, matching_primary, matching_index, packet).unwrap();
        assert!(
            rejected.reads.is_empty(),
            "namespace mismatch must fail after matching receipt seals"
        );

        let mut changed = material.clone();
        changed.grade = match material.grade {
            ReadDeliveryGrade::AgentChannelSend => ReadDeliveryGrade::ProviderModelObserved,
            ReadDeliveryGrade::ProviderModelObserved => ReadDeliveryGrade::AgentChannelSend,
        };
        let mut changed_event = event.clone();
        changed_event.payload = serde_json::to_value(&changed).unwrap();
        assert!(
            run(changed_event, primary.clone(), index.clone(), packet).is_err(),
            "grade-only elevation must fail typed transport decoding"
        );
        let mut changed_event = event.clone();
        changed_event.payload["transport"]["kind"] = serde_json::json!(match material.transport {
            ReadDeliveryTransport::NativeProviderContinuation { .. } => "process_channel_send",
            ReadDeliveryTransport::ProcessChannelSend { .. } => "native_provider_continuation",
        });
        assert!(
            run(changed_event, primary.clone(), index.clone(), packet).is_err(),
            "transport discriminator substitution must fail strict decoding"
        );
        let opposite = events
            .iter()
            .filter(|other| other.kind != event.kind)
            .filter_map(|other| crate::context_evidence::decode_read_delivery(&other.payload).ok())
            .find(|other| other.grade != material.grade);
        assert!(
            !require_opposite_transport || opposite.is_some(),
            "NativeSources must exercise the actual opposite-executor transport"
        );
        if let Some(other) = opposite {
            let mut changed = material.clone();
            changed.transport = other.transport.clone();
            let mut changed_event = event.clone();
            changed_event.payload = serde_json::to_value(changed).unwrap();
            assert!(
                run(changed_event, primary.clone(), index.clone(), packet).is_err(),
                "even a real opposite-executor transport cannot be paired with this grade"
            );
            let mut changed = material.clone();
            changed.grade = other.grade;
            changed.transport = other.transport;
            let mut changed_event = event.clone();
            changed_event.payload = serde_json::to_value(&changed).unwrap();
            assert!(crate::context_evidence::decode_read_delivery(&changed_event.payload).is_ok());
            let error = run(changed_event, primary.clone(), index.clone(), packet)
                .err()
                .unwrap();
            assert!(
                error.contains("matching original executor provenance"),
                "{error}"
            );
        }
        let mut changed = material.clone();
        let mut origin: harness_contract::step::CallOrigin =
            serde_json::from_value(changed.acquisition.origin.clone()).unwrap();
        origin.source = match material.grade {
            ReadDeliveryGrade::ProviderModelObserved => {
                harness_contract::step::CallSource::Process {
                    worker_id: packet.run_id().into(),
                    sequence: 1,
                }
            }
            ReadDeliveryGrade::AgentChannelSend => harness_contract::step::CallSource::Provider {
                attempt_id: "foreign-provider-231".into(),
            },
        };
        changed.acquisition.origin = serde_json::to_value(origin).unwrap();
        let mut changed_event = event.clone();
        changed_event.payload = serde_json::to_value(&changed).unwrap();
        physical_error(
            "delivery-only origin substitution",
            run(
                changed_event.clone(),
                primary.clone(),
                index.clone(),
                packet,
            ),
        );
        let mut changed_primary = primary.clone();
        let mut changed_index = index.clone();
        changed_primary.payload[READ_ACQUISITION_FIELD] =
            serde_json::to_value(&changed.acquisition).unwrap();
        changed_index.payload[READ_ACQUISITION_FIELD] =
            serde_json::to_value(&changed.acquisition).unwrap();
        assert!(crate::context_evidence::decode_read_delivery(&changed_event.payload).is_ok());
        let error = run(changed_event, changed_primary, changed_index, packet)
            .err()
            .unwrap();
        assert!(
            error.contains("matching original executor provenance"),
            "{error}"
        );
        let mut changed_event = event.clone();
        let native = event.kind == NATIVE_DELIVERY_KIND;
        changed_event.kind = if native {
            PROCESS_DELIVERY_KIND
        } else {
            NATIVE_DELIVERY_KIND
        }
        .into();
        changed_event.actor = Some(
            if native {
                "runtime.process_jsonl"
            } else {
                "conversation_runtime"
            }
            .into(),
        );
        changed_event.status = Some(if native { "sent" } else { "confirmed" }.into());
        let error = run(changed_event, primary.clone(), index.clone(), packet)
            .err()
            .unwrap();
        assert!(
            error.contains("matching original executor provenance"),
            "{error}"
        );
        let mut wrong_packet = packet.clone();
        wrong_packet.assignment.run_id = "foreign-run-231".into();
        let rejected = run(event.clone(), primary.clone(), index.clone(), &wrong_packet).unwrap();
        assert!(
            rejected.reads.is_empty(),
            "foreign run cannot borrow the original delivery"
        );
        let mut wrong_packet = packet.clone();
        wrong_packet.attempt += 1;
        let rejected = run(event.clone(), primary.clone(), index.clone(), &wrong_packet).unwrap();
        assert!(
            rejected.reads.is_empty(),
            "foreign attempt cannot borrow the original delivery"
        );
        let mut changed = material.clone();
        changed.acquisition.attempt += 1;
        let mut changed_event = event.clone();
        changed_event.payload = serde_json::to_value(&changed).unwrap();
        refused(
            "delivery attempt substitution",
            run(changed_event, primary.clone(), index.clone(), packet),
        );
        let mut changed = material.clone();
        changed.acquisition.step.run_ref = Some("foreign-run-231".into());
        let mut changed_event = event.clone();
        changed_event.payload = serde_json::to_value(changed).unwrap();
        assert!(
            run(changed_event, primary.clone(), index.clone(), packet).is_err(),
            "delivery run must match its typed recipient"
        );

        for field in ["tool_use_id", "tool_name", "input_sha256"] {
            let mut changed_primary = primary.clone();
            changed_primary.payload[field] = serde_json::json!("foreign-receipt-231");
            physical_error(
                field,
                run(event.clone(), changed_primary, index.clone(), packet),
            );
        }
        for primary_side in [true, false] {
            let mut changed_primary = primary.clone();
            let mut changed_index = index.clone();
            let changed = if primary_side {
                &mut changed_primary
            } else {
                &mut changed_index
            };
            changed.payload[READ_ACQUISITION_FIELD]["step"]["actor_ref"] =
                serde_json::json!("foreign-receipt-actor-231");
            physical_error(
                "original seal binding",
                run(event.clone(), changed_primary, changed_index, packet),
            );
        }
        let mut changed_primary = primary.clone();
        changed_primary.event_id.push_str("-foreign");
        physical_error(
            "primary event id",
            run(event.clone(), changed_primary, index.clone(), packet),
        );
        let mut changed_index = index.clone();
        changed_index.event_id.push_str("-foreign");
        physical_error(
            "index event id",
            run(event.clone(), primary.clone(), changed_index, packet),
        );
        let mut changed_index = index.clone();
        changed_index.transaction_id.push_str("-foreign");
        physical_error(
            "same transaction",
            run(event.clone(), primary.clone(), changed_index, packet),
        );
        let mut changed_index = index.clone();
        changed_index.commit_cursor += 1;
        physical_error(
            "same cursor",
            run(event.clone(), primary.clone(), changed_index, packet),
        );
        let mut changed_event = event.clone();
        changed_event.commit_cursor = primary.commit_cursor;
        physical_error(
            "delivery follows acquisition",
            run(changed_event, primary.clone(), index.clone(), packet),
        );
        for (source, revision) in actual.sources {
            assert_eq!(
                store.stream_revision(&source).unwrap(),
                revision,
                "matrix must not write or advance original durable sources"
            );
        }
        let unchanged = review_delivery_snapshot(store, program, Some(packet), None).unwrap();
        assert!(
            unchanged
                .reads
                .iter()
                .any(|read| read.event.event_id == event.event_id && read.material == material),
            "negative projections cannot poison the actual positive"
        );
    }
}

fn read_requires_provider_model(
    packet: &harness_contract::agent::AgentTaskPacket,
    seal: &crate::context_evidence::ReadAcquisitionSeal,
    receipt: &crate::DurableRuntimeEvent,
) -> Result<bool, String> {
    use harness_contract::context::{EvidenceObservationRequirement, EvidenceTargetIdentity};
    if packet.required_acceptance.is_empty()
        && packet.output_acceptance.iter().any(|requirement| {
            matches!(
                &requirement.check,
                harness_contract::agent::OutputAcceptanceCheck::ScopedEvidence { .. }
                    | harness_contract::agent::OutputAcceptanceCheck::WorkspaceChange { .. }
            )
        })
    {
        return Err("read delivery grade lacks the immutable compiled evidence contract".into());
    }
    if seal.tool_name != "read_file" {
        return Ok(false);
    }
    let obligations = packet
        .required_acceptance
        .evidence_obligations
        .iter()
        .filter(|obligation| {
            obligation.observation_requirement == EvidenceObservationRequirement::ProviderModel
        })
        .collect::<Vec<_>>();
    if obligations.is_empty() {
        return Ok(false);
    }
    let scope: harness_contract::goal::ProofEffectScope =
        serde_json::from_value(receipt.payload["resolved_effect_scope"].clone())
            .map_err(|_| "read delivery cannot bind its frozen model-observation scope")?;
    for obligation in obligations {
        match &obligation.target {
            EvidenceTargetIdentity::Workspace { scope: expected } => {
                if scope.targets.iter().any(|target| {
                    target.path.workspace_id == expected.path.workspace_id
                        && target.path.repository_id == expected.path.repository_id
                        && target.path.workspace_relative_path
                            == expected.path.workspace_relative_path
                        && target.path.repository_relative_path
                            == expected.path.repository_relative_path
                }) {
                    return Ok(true);
                }
            }
            EvidenceTargetIdentity::AmbiguousWorkspace { .. }
            | EvidenceTargetIdentity::UnavailableWorkspace { .. } => {
                return Err(
                    "read delivery has an unresolved frozen model-observation target".into(),
                );
            }
            EvidenceTargetIdentity::Network { .. }
            | EvidenceTargetIdentity::LocalNetwork { .. }
            | EvidenceTargetIdentity::ToolExecution { .. } => {}
        }
    }
    Ok(false)
}

pub(crate) fn root_file_acquisition(
    store: &RuntimeEventStore,
    owner: &crate::DurableRuntimeEvent,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(crate::DurableRuntimeEvent, crate::DurableRuntimeEvent), super::results::ReviewEvidenceError> {
    use crate::context_evidence::{RootReadOwner, READ_ACQUISITION_FIELD};
    let binding = owner
        .activity_binding()
        .ok_or("Root read has no original activity owner")?;
    let session_stream = format!("session:{}", binding.session_id);
    let revision = store
        .stream_revision(&session_stream)
        .map_err(|error| error.to_string())?;
    if sources
        .insert(session_stream.clone(), revision)
        .is_some_and(|previous| previous != revision)
    {
        return Err("Root acquisition owner source changed during validation".into());
    }
    let plan_id = owner.payload["governed_plan_id"]
        .as_str()
        .ok_or("Root read has no original governed plan")?;
    let call_id = owner.payload["tool_call_id"]
        .as_str()
        .ok_or("Root read has no original call identity")?;
    let mut after_sequence = 0;
    let mut selected = None;
    loop {
        let page = store.list_stream_after(
            &session_stream,
            after_sequence,
            owner.sequence.saturating_sub(1),
            64,
            4 * 1024 * 1024,
        )?;
        if page.is_empty() {
            break;
        }
        for plan in page {
            after_sequence = plan.sequence;
            if plan.kind != "tool.execution_plan.created" || plan.payload["plan_id"] != plan_id {
                continue;
            }
            if plan.sequence >= owner.sequence
                || plan.actor.as_deref() != Some("conversation_runtime")
                || plan.scope != RuntimeEventScope::Tool
                || plan.status.as_deref() != Some("planned")
                || !plan.refs.iter().any(|reference| {
                    reference.kind == "execution" && reference.id == binding.root_execution_id
                })
                || !plan.refs.iter().any(|reference| {
                    reference.kind == "session" && reference.id == binding.session_id
                })
                || !plan
                    .refs
                    .iter()
                    .any(|reference| reference.kind == "turn" && reference.id == binding.turn_id)
                || !plan.refs.iter().any(|reference| {
                    reference.kind == "governed_tool_plan" && reference.id == plan_id
                })
            {
                return Err("Root read governed plan has a foreign owner or ordering".into());
            }
            let tasks = plan.payload["tasks"]
                .as_array()
                .ok_or("Root read governed plan tasks missing")?;
            let mut matches = tasks.iter().filter(|task| task["tool_call_id"] == call_id);
            let task = matches
                .next()
                .ok_or("Root read original plan lacks its exact call")?;
            if matches.next().is_some()
                || selected.is_some()
                || task["tool_name"] != "read_file"
                || task["effect"]["effect_kind"] != "read"
            {
                return Err(
                    "Root read governed call is ambiguous or not a file acquisition".into(),
                );
            }
            let step: crate::step_snapshot::StepCallBinding =
                serde_json::from_value(task["step_binding"].clone())
                    .map_err(|_| "Root read has no original Step binding")?;
            if step.origin.call_id != call_id
                || step.snapshot.identity.session_id != binding.session_id
                || step.snapshot.identity.turn_id != binding.turn_id
                || step.snapshot.identity.execution_id.as_deref()
                    != Some(binding.root_execution_id.as_str())
            {
                return Err("Root read Step binding crosses its original owner".into());
            }
            selected = Some((
                step,
                task["normalized_input"].clone(),
                task["effect"].clone(),
            ));
        }
    }
    let (step, input, descriptor_value) =
        selected.ok_or("Root read original governed plan disappeared")?;
    let key = step.effect_idempotency_key();
    let primary_stream = format!("execution-effect:{key}");
    let primary = store
        .event_by_idempotency_key(&primary_stream, &format!("{key}:read-receipt"))
        .map_err(|error| error.to_string())?
        .ok_or("Root read original primary receipt missing")?;
    let value = primary
        .payload
        .get(READ_ACQUISITION_FIELD)
        .ok_or("Root read has no original acquisition seal")?;
    let seal = crate::context_evidence::decode_read_acquisition(value).map_err(str::to_string)?;
    let index = store
        .event_by_idempotency_key(&seal.index_stream, &seal.index_key)
        .map_err(|error| error.to_string())?
        .ok_or("Root read original index receipt missing")?;
    let original_owner = RootReadOwner {
        stream_id: owner.stream_id.clone(),
        sequence: owner.sequence,
        event_id: owner.event_id.clone(),
        payload_digest: super::proof_value_digest(&owner.payload),
        binding,
    };
    let origin = serde_json::to_value(&step.origin).map_err(|error| error.to_string())?;
    let input_digest = harness_contract::step::canonical_digest("tool-effect-payload", &input)
        .map_err(|error| error.to_string())?;
    if !crate::context_evidence::root_read_owner_matches(&original_owner, owner, &seal)
        || primary.kind != "execution.read.receipt"
        || index.kind != "execution.agent_tool.receipt"
        || primary.actor.as_deref() != Some("governed_tool")
        || index.actor != primary.actor
        || primary.scope != RuntimeEventScope::ExecutionNode
        || index.scope != primary.scope
        || primary.status.as_deref() != Some("completed")
        || index.status.as_deref() != Some("completed")
        || primary.transaction_id != index.transaction_id
        || primary.commit_cursor != index.commit_cursor
        || primary.commit_cursor >= owner.commit_cursor
        || seal.primary_stream != primary_stream
        || seal.primary_key != format!("{key}:read-receipt")
        || seal.index_stream
            != format!(
                "execution-agent-receipts:{}:{}:{}",
                seal.graph_id, seal.node_id, seal.attempt
            )
        || seal.index_key != format!("agent-tool-receipt:{key}")
        || seal.step != step.snapshot.identity
        || seal.origin != origin
        || primary.payload["replay"]["origin"] != origin
        || primary.payload["replay"]["payload_digest"] != input_digest
        || primary.payload["replay"]["call_id"] != call_id
        || primary.payload["input_sha256"] != seal.input_sha256
        || primary.payload["outcome"]["status"] != "executed"
        || index.payload["outcome"]["status"] != "executed"
        || index.payload.get(READ_ACQUISITION_FIELD) != Some(value)
    {
        return Err("Root file dependency is not its authenticated original acquisition".into());
    }
    let descriptor: ToolEffectDescriptor = serde_json::from_value(descriptor_value.clone())
        .map_err(|_| "Root acquisition descriptor is invalid")?;
    let scope = super::results::require_original_effect_scope(
        &primary.payload["resolved_effect_scope"],
        &descriptor,
    )?;
    if !scope.complete
        || primary.payload["resolved_effect_scope"] != index.payload["resolved_effect_scope"]
        || primary.payload["replay"]["invocation"]["effect"] != descriptor_value
    {
        return Err("Root acquisition scope/descriptor differs from original admission".into());
    }
    if primary.payload["replay"]["parent_execution"].is_null() {
        let intent = store
            .event_by_idempotency_key(&primary_stream, &format!("{key}:read-intent"))
            .map_err(|error| error.to_string())?
            .ok_or("Root read original admission owner missing")?;
        let (admitted, attempt, _) =
            super::results::authenticate_original_root_owner(store, &intent, sources)?;
        if intent.kind != "execution.read.intent"
            || intent.schema_version != 1
            || intent.actor != primary.actor
            || intent.scope != primary.scope
            || intent.commit_cursor >= primary.commit_cursor
            || intent.payload["effect"] != descriptor_value
            || intent.payload["replay"] != primary.payload["replay"]
            || intent.payload["root_effect_owner"] != primary.payload["root_effect_owner"]
            || intent.payload["root_effect_owner"] != index.payload["root_effect_owner"]
            || intent.payload["root_effect_context"] != primary.payload["root_effect_context"]
            || intent.payload["root_effect_context"] != index.payload["root_effect_context"]
            || intent.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
            || admitted.root_execution_id != seal.graph_id
            || admitted.node_id.as_deref() != Some(seal.node_id.as_str())
            || attempt != seal.attempt
            || admitted.session_id != seal.session_id
            || admitted.turn_id != seal.step.turn_id
        {
            return Err("Root read does not authenticate its original caller node/attempt".into());
        }
    } else if primary.payload["replay"]["parent_execution"]["execution_id"] != seal.graph_id
        || primary.payload["replay"]["parent_execution"]["node_id"] != seal.node_id
    {
        return Err("Root ticket acquisition parent differs from original read owner".into());
    }
    for source in [&primary_stream, &seal.index_stream] {
        let revision = store
            .stream_revision(source)
            .map_err(|error| error.to_string())?;
        if sources
            .insert(source.clone(), revision)
            .is_some_and(|previous| previous != revision)
        {
            return Err("Root acquisition source changed during validation".into());
        }
    }
    for (source, expected) in sources.iter() {
        if store
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != *expected
        {
            return Err("Root acquisition source changed before its final cut".into());
        }
    }
    Ok((primary, index))
}

pub(super) fn verify_material_event(
    store: &RuntimeEventStore,
    material: &harness_contract::goal::ProofInputEvent,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<crate::DurableRuntimeEvent, String> {
    verify_material_event_with_reader(
        &mut super::read_source::StoreReadSource(store),
        material,
        sources,
    )
}

pub(super) fn verify_material_event_with_reader(
    reader: &mut dyn super::read_source::ReadAuthSource,
    material: &harness_contract::goal::ProofInputEvent,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<crate::DurableRuntimeEvent, String> {
    if material.sequence == 0 {
        return Err("proof material sequence is missing".into());
    }
    let revision = reader
        .stream_revision(&material.stream_id)
        .map_err(|error| error.to_string())?;
    if sources
        .insert(material.stream_id.clone(), revision)
        .is_some_and(|previous| previous != revision)
    {
        return Err("proof material source changed during validation".into());
    }
    let events = reader.list_stream_after(
        &material.stream_id,
        material.sequence - 1,
        material.sequence,
        1,
        4 * 1024 * 1024,
    )?;
    events
        .into_iter()
        .find(|event| {
            event.sequence == material.sequence
                && event.event_id == material.event_id
                && super::proof_value_digest(&event.payload) == material.payload_digest
        })
        .ok_or_else(|| "proof physically read material changed or disappeared".into())
}

fn canonical_targets_disjoint(
    required: &[harness_contract::goal::ProofEffectTarget],
    targets: &[harness_contract::goal::ProofEffectTarget],
) -> bool {
    !required.is_empty()
        && !targets.is_empty()
        && targets.iter().all(|target| {
            required.iter().all(|dependency| {
                let valid = |path: &str| {
                    !path.is_empty()
                        && path != "."
                        && !path.contains(['*', '?', '[', ']'])
                        && std::path::Path::new(path)
                            .components()
                            .all(|component| matches!(component, std::path::Component::Normal(_)))
                };
                let complete_target = |target: &harness_contract::goal::ProofEffectTarget| {
                    use harness_contract::context::{EvidenceCoverageKind, WorkspaceAccessMode};
                    use harness_contract::policy::{PermissionOperation, PermissionResource};
                    target.declared_scope.resource == PermissionResource::File
                        && match target.declared_scope.operation {
                            PermissionOperation::Read => {
                                target.access_mode == WorkspaceAccessMode::Read
                                    && target.coverage == EvidenceCoverageKind::ScopedContent
                            }
                            PermissionOperation::Write => {
                                target.access_mode == WorkspaceAccessMode::Write
                                    && target.coverage == EvidenceCoverageKind::WriteEffect
                            }
                            _ => false,
                        }
                };
                target.exact
                    && dependency.exact
                    && target.alias_checked
                    && dependency.alias_checked
                    && complete_target(target)
                    && complete_target(dependency)
                    && target.path.workspace_id == dependency.path.workspace_id
                    && target.path.repository_id == dependency.path.repository_id
                    && target.path.object_kind
                        == harness_contract::context::WorkspaceObjectKind::File
                    && dependency.path.object_kind
                        == harness_contract::context::WorkspaceObjectKind::File
                    && target.physical_identity.as_ref().is_some_and(|identity| {
                        !identity.is_empty()
                            && dependency
                                .physical_identity
                                .as_ref()
                                .is_some_and(|other| !other.is_empty() && other != identity)
                    })
                    && valid(&target.path.workspace_relative_path)
                    && valid(&dependency.path.workspace_relative_path)
                    && target.path.workspace_relative_path
                        != dependency.path.workspace_relative_path
                    && !target
                        .path
                        .workspace_relative_path
                        .starts_with(&format!("{}/", dependency.path.workspace_relative_path))
                    && !dependency
                        .path
                        .workspace_relative_path
                        .starts_with(&format!("{}/", target.path.workspace_relative_path))
            })
        })
}

fn canonical_scope_disjoint(
    required: &[harness_contract::goal::ProofEffectTarget],
    scope: &harness_contract::goal::ProofEffectScope,
) -> bool {
    use harness_contract::goal::ProofEffectPurpose;
    if scope.version != 2 || !scope.complete {
        return false;
    }
    let ordinary_path = |path: &str| {
        !path.is_empty()
            && path != "."
            && !path.contains(['*', '?', '[', ']'])
            && std::path::Path::new(path)
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_)))
    };
    let mut targets = Vec::new();
    for target in &scope.targets {
        let purposes = scope
            .scope_purposes
            .iter()
            .filter(|entry| entry.declared_scope == target.declared_scope)
            .collect::<Vec<_>>();
        if purposes.len() != 1 {
            return false;
        }
        let checkpoint_is_unrelated = purposes[0].purpose
            == ProofEffectPurpose::AutoCheckpointMaintenance
            && target.declared_scope.resource == harness_contract::policy::PermissionResource::File
            && target.declared_scope.operation
                == harness_contract::policy::PermissionOperation::Write
            && target.path.object_kind == harness_contract::context::WorkspaceObjectKind::Directory
            && target.access_mode == harness_contract::context::WorkspaceAccessMode::Write
            && target.coverage == harness_contract::context::EvidenceCoverageKind::WriteEffect
            && !required.is_empty()
            && required.iter().all(|dependency| {
                let checkpoint = &target.path.workspace_relative_path;
                let input = &dependency.path.workspace_relative_path;
                target.path.workspace_id == dependency.path.workspace_id
                    && target.path.repository_id == dependency.path.repository_id
                    && ordinary_path(checkpoint)
                    && ordinary_path(input)
                    && checkpoint != input
                    && !input.starts_with(&format!("{checkpoint}/"))
                    && !checkpoint.starts_with(&format!("{input}/"))
            });
        if !checkpoint_is_unrelated {
            targets.push(target.clone());
        }
    }
    canonical_targets_disjoint(required, &targets)
}

fn actual_input_cut_matches(
    expected: Option<u64>,
    revision: u64,
    allow_history_growth: bool,
) -> bool {
    expected.is_some_and(|cut| {
        if allow_history_growth {
            revision >= cut
        } else {
            revision == cut
        }
    })
}

#[cfg(test)]
#[test]
fn r1_581_actual_read_phase_keeps_discovery_exact_and_revalidates_existing_cuts() {
    let store = std::sync::Arc::new(RuntimeEventStore::for_test());
    let mut program = AgenticProgramProjection::empty("phase581", "objective");
    program.session_id = "phase581".into();
    let source = "session:phase581";
    for revision in 1..=2 {
        store
            .append(crate::RuntimeEventInput {
                stream_id: source.into(),
                scope: RuntimeEventScope::Session,
                kind: "runtime.phase581.metadata".into(),
                status: None,
                actor: Some("phase581_test_not_business_authority".into()),
                refs: vec![],
                payload: serde_json::json!({"metadata_revision": revision}),
            })
            .unwrap();
    }
    let readers = super::ResultContentReaders {
        artifacts: None,
        history: None,
        process_inputs: None,
    };
    let original = std::collections::BTreeMap::from([(source.to_string(), 1)]);
    assert!(actual_input_snapshot(&store, &program, &[], readers, Some(&original)).is_err());
    let current =
        actual_input_snapshot_for_phase(&store, &program, &[], readers, Some(&original), true, None)
            .unwrap();
    assert_eq!(current.sources.get(source), Some(&2));
    for invalid in [
        std::collections::BTreeMap::new(),
        std::collections::BTreeMap::from([(source.to_string(), 3)]),
    ] {
        assert!(actual_input_snapshot_for_phase(
            &store,
            &program,
            &[],
            readers,
            Some(&invalid),
            true,
            None,
        )
        .is_err());
    }
    let exact = std::collections::BTreeMap::from([(source.to_string(), 2)]);
    assert!(actual_input_snapshot(&store, &program, &[], readers, Some(&exact)).is_ok());
}

#[cfg(test)]
#[test]
fn actual_input_cut_keeps_exact_discovery_and_allows_only_existing_history_growth() {
    assert!(actual_input_cut_matches(Some(7), 7, false));
    assert!(!actual_input_cut_matches(Some(7), 8, false));
    assert!(actual_input_cut_matches(Some(7), 8, true));
    assert!(!actual_input_cut_matches(Some(7), 6, true));
    assert!(!actual_input_cut_matches(None, 8, true));
    assert!(!actual_input_cut_matches(None, 8, false));
}

#[cfg(test)]
#[test]
fn checkpoint_comparison_preserves_full_scope_and_actual_checkpoint_inputs() {
    let workspace = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(workspace.path().join(".cowd/checkpoints")).unwrap();
    for path in [
        "required.txt",
        "optional.txt",
        ".cowd/checkpoints/input.txt",
    ] {
        std::fs::write(workspace.path().join(path), "actual input").unwrap();
    }
    let host = tools::ToolHost::builtin("proof-purpose", workspace.path());
    let resolver =
        crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path()).unwrap();
    let scope_for = |tool: &str, input: serde_json::Value| {
        let request =
            crate::bound_tool_batch::scope_admission_tests::bound_request(&host, tool, input);
        crate::bound_tool_batch::admitted_effect_scope(
            &request,
            &request.compiled_invocation.as_ref().unwrap().effect,
            Some(host.pin_snapshot()),
            &resolver,
        )
        .unwrap()
    };
    let required = scope_for("read_file", serde_json::json!({"path":"required.txt"}));
    let optional = scope_for(
        "edit",
        serde_json::json!({"action":"write","path":"optional.txt","content":"next"}),
    );
    let original = serde_json::to_value(&optional).unwrap();
    assert!(!canonical_targets_disjoint(
        &required.targets,
        &optional.targets
    ));
    assert!(canonical_scope_disjoint(&required.targets, &optional));
    assert_eq!(serde_json::to_value(&optional).unwrap(), original);
    let overlapping = scope_for(
        "edit",
        serde_json::json!({"action":"write","path":"required.txt","content":"next"}),
    );
    assert!(!canonical_scope_disjoint(&required.targets, &overlapping));
    let checkpoint_input = scope_for(
        "read_file",
        serde_json::json!({"path":".cowd/checkpoints/input.txt"}),
    );
    assert!(!canonical_scope_disjoint(
        &checkpoint_input.targets,
        &optional
    ));
    let mut incomplete = optional.clone();
    incomplete.complete = false;
    assert!(!canonical_scope_disjoint(&required.targets, &incomplete));
    let mut missing = optional.clone();
    missing.scope_purposes.clear();
    assert!(!canonical_scope_disjoint(&required.targets, &missing));
    let mut duplicated = optional.clone();
    duplicated
        .scope_purposes
        .push(duplicated.scope_purposes[0].clone());
    assert!(!canonical_scope_disjoint(&required.targets, &duplicated));
    let mut forged = overlapping.clone();
    for purpose in &mut forged.scope_purposes {
        purpose.purpose = harness_contract::goal::ProofEffectPurpose::AutoCheckpointMaintenance;
    }
    assert!(!canonical_scope_disjoint(&required.targets, &forged));
    let mut explicit = optional.clone();
    for purpose in &mut explicit.scope_purposes {
        purpose.purpose = harness_contract::goal::ProofEffectPurpose::BusinessEffect;
    }
    assert!(!canonical_scope_disjoint(&required.targets, &explicit));
    let mut legacy = optional;
    legacy.version = 1;
    assert!(!canonical_scope_disjoint(&required.targets, &legacy));
}

#[derive(Clone, serde::Serialize)]
pub(super) struct ActualFileDependency {
    pub target: harness_contract::goal::ProofEffectTarget,
    pub acquisition_cursor: u64,
    pub consumer_cursor: u64,
    pub consumer_call_key: String,
    pub source_sha256: String,
    pub source_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(super) struct ActualImmutableInput {
    pub content: harness_contract::context::ArtifactRef,
    pub reference: String,
    pub acquisition: crate::context_evidence::ReadAcquisitionSeal,
    pub acquisition_cursor: u64,
}

impl ActualImmutableInput {
    pub(super) fn from_acquisition(
        store: &std::sync::Arc<RuntimeEventStore>,
        program: &AgenticProgramProjection,
        primary: &crate::DurableRuntimeEvent,
        index: &crate::DurableRuntimeEvent,
        session_events: &[crate::DurableRuntimeEvent],
        packet: Option<&harness_contract::agent::AgentTaskPacket>,
        readers: super::ResultContentReaders<'_>,
        actual: &mut ActualInputSnapshot,
    ) -> Result<Self, super::results::ReviewEvidenceError> {
        let acquisition = crate::context_evidence::decode_read_acquisition(
            &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
        )
        .map_err(str::to_string)?;
        // The acquisition is owned by its original physical producer, not by
        // the consumer whose proof happens to discover it. Recover the packet
        // from the original graph owner when this is an AgentTask acquisition;
        // the existing packet, receipt and ticket checks below remain decisive.
        let original_packet = if packet.is_none() && acquisition.step.run_ref.is_some() {
            Some(super::results::result_packet(store, &acquisition.graph_id, program, &mut actual.sources)?)
        } else {
            None
        };
        let packet = packet.or(original_packet.as_ref());
        let intent = &primary.payload["replay"]["invocation"]["intent"];
        if intent["invocation_id"] != acquisition.invocation_id
            || intent["tool_name"] != acquisition.tool_name
            || primary.stream_id != acquisition.primary_stream
            || primary.idempotency_key.as_deref() != Some(acquisition.primary_key.as_str())
            || primary.payload["input_sha256"] != acquisition.input_sha256
        {
            return Err("immutable acquisition original request binding conflicts".into());
        }
        let key = primary.payload["idempotency_key"].as_str()
            .ok_or("immutable acquisition original call key missing")?;
        if primary.kind != "execution.read.receipt"
            || primary.schema_version != 1
            || primary.actor.as_deref() != Some("governed_tool")
            || primary.scope != RuntimeEventScope::ExecutionNode
            || primary.status.as_deref() != Some("completed")
            || index.kind != "execution.agent_tool.receipt"
            || index.schema_version != 2
            || index.actor != primary.actor || index.scope != primary.scope
            || index.status != primary.status
            || index.transaction_id != primary.transaction_id
            || index.commit_cursor != primary.commit_cursor
            || index.transaction_index <= primary.transaction_index
            || index.stream_id != acquisition.index_stream
            || index.idempotency_key.as_deref() != Some(acquisition.index_key.as_str())
            || index.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
                != primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
            || index.payload["idempotency_key"] != key
            || index.payload["effect_kind"] != "read"
            || crate::ExecutionCommitService::receipt_observation_sequence(index).is_none()
            || primary.payload["outcome"]["status"] != "executed"
            || !primary.payload["outcome"]["error"].is_null()
            || index.payload["outcome"] != primary.payload["outcome"]
            || index.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
            || primary.payload["replay"]["origin"] != acquisition.origin
            || primary.payload["replay"]["call_id"] != acquisition.invocation_id
            || primary.payload["replay"]["invocation"]["invocation_id"] != acquisition.invocation_id
            || primary.payload["replay"]["invocation"]["origin"] != acquisition.origin
            || primary.payload["replay"]["session_id"] != acquisition.session_id
            || acquisition.session_id != program.session_id
            || acquisition.primary_stream != format!("execution-effect:{key}")
            || acquisition.primary_key != format!("{key}:read-receipt")
            || acquisition.index_stream != format!("execution-agent-receipts:{}:{}:{}",
                acquisition.graph_id, acquisition.node_id, acquisition.attempt)
            || acquisition.index_key != format!("agent-tool-receipt:{key}")
        {
            return Err("immutable acquisition primary/index identity conflicts".into());
        }
        let descriptor = super::results::receipt_effect_descriptor(primary)?;
        if descriptor.effect_kind != ToolEffectKind::Read {
            return Err("immutable acquisition is not an original Read".into());
        }
        read_original_scope(primary, &descriptor, &acquisition)?;
        super::results::original_acquisition_output(store, primary, session_events, readers, &mut actual.sources)?;
        let mut original_input = if let Some(raw) = primary.payload.get("original_input") {
            let raw = raw.as_str().ok_or("immutable acquisition original request is not raw input")?;
            let input = harness_contract::step::parse_strict_json(raw)
                .map_err(|error| error.to_string())?;
            if crate::context_evidence::read_sha256(raw.as_bytes()) != acquisition.input_sha256
                || primary.payload["replay"]["payload_digest"]
                    != harness_contract::step::canonical_digest("tool-effect-payload", &input)
                        .map_err(|error| error.to_string())?
            {
                return Err("immutable acquisition original raw/canonical input conflicts".into());
            }
            Some(input)
        } else { None };
        let mut original_owner = None;
        for owner in session_events.iter().filter(|event| {
            event.kind == "tool.invocation.completed"
                && event.payload["tool_call_id"] == acquisition.invocation_id
                && event.payload["tool_name"] == acquisition.tool_name
        }) {
            let Some(task) = super::results::result_tool_plan(session_events, owner)? else {
                continue;
            };
            let step: crate::step_snapshot::StepCallBinding =
                serde_json::from_value(task["step_binding"].clone())
                    .map_err(|_| "immutable acquisition original plan Step missing")?;
            if step.effect_idempotency_key() != key {
                continue;
            }
            let input = &task["normalized_input"];
            if primary.payload["replay"]["payload_digest"]
                != harness_contract::step::canonical_digest("tool-effect-payload", input)
                    .map_err(|error| error.to_string())?
                || serde_json::to_value(&step.origin).map_err(|error| error.to_string())?
                    != acquisition.origin
                || step.snapshot.identity != acquisition.step
                || original_input.as_ref().is_some_and(|previous| previous != input)
            {
                return Err("immutable acquisition original plan/input conflicts".into());
            }
            retain_actual_material(&mut actual.material_events, owner);
            for plan in session_events.iter().filter(|event| {
                event.kind == "tool.execution_plan.created"
                    && event.payload["plan_id"] == owner.payload["governed_plan_id"]
            }) {
                retain_actual_material(&mut actual.material_events, plan);
            }
            original_input = Some(input.clone());
            original_owner = Some((owner, task));
        }
        let original_input = match (original_input, packet) {
            (Some(input), _) => input,
            (None, Some(packet)) => authenticate_unsealed_delegated_input(
                store, primary, index, packet, actual,
            )?,
            (None, None) => return Err("immutable acquisition has no authenticated original request".into()),
        };
        let requested_evidence = original_input["evidence_ref"].as_str();
        let requested_reference = original_input["reference"].as_str();
        if requested_evidence.zip(requested_reference).is_some_and(|(left, right)| left != right) {
            return Err("immutable acquisition original requested references conflict".into());
        }
        if let Some(packet) = packet {
            let source_matches = match acquisition.origin["source"]["kind"].as_str() {
                Some("process") => acquisition.origin["source"]["worker_id"] == packet.run_id()
                    && acquisition.origin["source"]["sequence"].as_u64().is_some_and(|sequence| sequence > 0)
                    && crate::agentic::action_invocation::execution_ids_agree(
                        acquisition.step.execution_id.as_deref(),
                        packet.graph_id(),
                    )
                    && acquisition.step.actor_ref.as_deref() == Some(packet.agent_id())
                    && acquisition.step.attempt == Some(packet.attempt),
                Some("provider") => acquisition.origin["source"]["attempt_id"].as_str().is_some_and(|attempt| !attempt.is_empty())
                    && crate::agentic::action_invocation::execution_ids_agree(
                        acquisition.step.execution_id.as_deref(),
                        packet.run_id(),
                    )
                    && acquisition.step.actor_ref.as_deref() == Some(packet.assignment.execution_identity.principal_id()),
                _ => false,
            };
            if !source_matches || acquisition.graph_id != packet.graph_id()
                || acquisition.node_id != packet.node_id() || acquisition.attempt != packet.attempt
                || acquisition.session_id != packet.session_id()
                || acquisition.step.turn_id != packet.assignment.execution_identity.turn_id().unwrap_or_default()
                || acquisition.step.run_ref.as_deref() != Some(packet.run_id())
                || acquisition.step.attempt.is_some_and(|attempt| attempt != packet.attempt)
                || primary.payload["replay"]["parent_execution"]["execution_id"] != packet.graph_id()
                || primary.payload["replay"]["parent_execution"]["node_id"] != packet.node_id()
            {
                return Err("immutable acquisition original producer Step/packet conflicts".into());
            }
        } else if primary.payload["replay"]["parent_execution"].is_null() {
            super::results::authenticate_root_receipt_owner(store, primary, &mut actual.sources)?;
            let owner = &primary.payload["root_effect_owner"];
            if owner["binding"]["root_execution_id"] != acquisition.graph_id
                || owner["binding"]["node_id"] != acquisition.node_id
                || owner["attempt"].as_u64() != Some(u64::from(acquisition.attempt))
                || acquisition.step.execution_id.as_deref() != Some(acquisition.graph_id.as_str())
                || acquisition.step.run_ref.is_some()
            {
                return Err("immutable acquisition original root owner/attempt conflicts".into());
            }
        } else {
            let (owner, task) = original_owner
                .ok_or("immutable acquisition graph-bound root original plan missing")?;
            let original = super::results::original_tool_primary(store, owner, task, &mut actual.sources)?;
            if original.event_id != primary.event_id
                || acquisition.step.execution_id.as_deref() != Some(acquisition.graph_id.as_str())
                || acquisition.step.run_ref.is_some()
            {
                return Err(format!(
                    "immutable acquisition has no authenticated root producer: call={} primary={} selected={} primary_match={} step_graph_match={} run_ref_present={}",
                    acquisition.invocation_id, primary.event_id, original.event_id,
                    original.event_id == primary.event_id,
                    acquisition.step.execution_id.as_deref() == Some(acquisition.graph_id.as_str()),
                    acquisition.step.run_ref.is_some(),
                ).into());
            }
        }
        for original in super::results::process_ticket_start(store, &acquisition.graph_id,
            &acquisition.node_id, acquisition.attempt, primary)? {
            super::results::pin_result_source(store, &original.stream_id, &mut actual.sources)?;
            retain_actual_material(&mut actual.material_events, &original);
        }
        for original in [primary, index] {
            super::results::pin_result_source(store, &original.stream_id, &mut actual.sources)?;
            retain_actual_material(&mut actual.material_events, original);
        }
        super::results::pin_result_source(store, &acquisition.graph_id, &mut actual.sources)?;
        let requested = original_input["evidence_ref"].as_str()
            .or_else(|| original_input["reference"].as_str())
            .filter(|reference| !reference.is_empty())
            .ok_or("immutable acquisition has no original requested reference")?;
        let source = &acquisition.body.source_ref;
        let requested_identity = super::identity::auxiliary_identity(program, requested)?;
        let source_identity = super::identity::auxiliary_identity(program, source)?;
        if requested.starts_with("artifact:")
            && !requested.starts_with("artifact://")
            && !matches!(&requested_identity, super::identity::MaterialIdentity::Publication(_))
        {
            return Err("immutable acquisition exact publication binding missing".into());
        }
        let source_selector = match &source_identity {
            super::identity::MaterialIdentity::Publication(reference) => {
                program.artifacts[reference].content_ref.as_str()
            }
            _ if source.starts_with("artifact:") && !source.starts_with("artifact://") => {
                return Err("immutable acquisition sealed publication binding missing".into());
            }
            _ => source.as_str(),
        };
        let content = if source_selector.starts_with("artifact://") {
            readers.artifacts.ok_or("immutable acquisition Artifact owner unavailable")?
                .resolve(source_selector).map_err(|error| error.to_string())?
        } else {
            harness_contract::context::ArtifactRef::durable(
                source_selector, &acquisition.body.source_sha256,
                acquisition.body.source_bytes, "application/octet-stream",
                format!("session:{}", acquisition.session_id),
            )
        };
        if content.sha256.trim_start_matches("sha256:")
            != acquisition.body.source_sha256.trim_start_matches("sha256:")
            || content.bytes != acquisition.body.source_bytes
            || (content.visibility_scope != "public"
                && content.visibility_scope != format!("session:{}", acquisition.session_id)
                && packet.is_none_or(|packet| !packet.resource_scopes.contains(&content.visibility_scope)))
        {
            return Err("immutable acquisition conflicts with original content or grant".into());
        }
        if source_selector.starts_with("artifact://") {
            if let Some(frame) = readers.process_inputs {
                frame.retain_artifact(readers.artifacts.ok_or("immutable acquisition Artifact owner unavailable")?, &content)?;
            }
        }
        let mut reference = requested.to_string();
        if let super::identity::MaterialIdentity::Publication(publication) = &requested_identity {
            reference = publication.clone();
            if program.artifacts[&reference].content_ref != content.selector
                || matches!(&source_identity,
                    super::identity::MaterialIdentity::Publication(source) if source != &reference)
            {
                return Err("immutable acquisition exact reference conflicts with original content".into());
            }
        } else if let super::identity::MaterialIdentity::Publication(publication) = &source_identity {
            reference = publication.clone();
            if program.artifacts[&reference].content_ref != content.selector {
                return Err("immutable acquisition sealed publication conflicts with content".into());
            }
        } else if requested == content.selector {
            let mut bound = BTreeSet::new();
            for evidence in packet.into_iter().flat_map(|packet| &packet.evidence_refs) {
                if evidence.retrieval_selector != content.selector {
                    continue;
                }
                if evidence.sha256 != content.sha256 || evidence.bytes != content.bytes
                    || evidence.media_type != content.media_type
                    || evidence.visibility_scope != content.visibility_scope
                {
                    return Err("immutable acquisition packet grant conflicts with original content".into());
                }
                if let super::identity::MaterialIdentity::Publication(publication) =
                    super::identity::auxiliary_identity(program, &evidence.evidence_ref.id)?
                {
                    bound.insert(publication);
                } else if evidence.evidence_ref.ref_type == "agent"
                    || evidence.evidence_ref.id.starts_with("artifact:")
                        && !evidence.evidence_ref.id.starts_with("artifact://")
                {
                    return Err("immutable acquisition packet exact publication binding missing".into());
                }
            }
            if bound.len() > 1 {
                return Err("immutable acquisition packet has ambiguous exact publication bindings".into());
            }
            if let Some(identity) = bound.into_iter().next() {
                if program.artifacts[&identity].content_ref != content.selector {
                    return Err("immutable acquisition packet publication conflicts with content".into());
                }
                reference = identity;
            }
        }
        Ok(Self { content, reference, acquisition, acquisition_cursor: primary.commit_cursor })
    }

    pub(super) fn dependency_identity(
        &self,
        program: &AgenticProgramProjection,
    ) -> Result<Option<String>, String> {
        match super::identity::auxiliary_identity(program, &self.reference)? {
            super::identity::MaterialIdentity::Content(selector) => {
                if selector != self.content.selector {
                    return Err("immutable input content reference conflicts with its acquisition".into());
                }
                Ok(None)
            }
            identity => Ok(Some(identity.reference().to_string())),
        }
    }

    pub(super) fn validate_dependency(
        &self,
        program: &AgenticProgramProjection,
        reference: &str,
        dependency: &super::results::ResolvedReviewResult,
    ) -> Result<(), String> {
        let identity = super::identity::auxiliary_identity(program, reference)?;
        if dependency.reference != reference
            || dependency.content.sha256.trim_start_matches("sha256:")
                != self.content.sha256.trim_start_matches("sha256:")
            || dependency.content.bytes != self.content.bytes
            || (matches!(identity, super::identity::MaterialIdentity::Publication(_)
                | super::identity::MaterialIdentity::Content(_))
                && dependency.content != self.content)
        {
            return Err("immutable dependency conflicts with its authenticated acquisition".into());
        }
        if program.artifacts.contains_key(reference)
            && !dependency.input_boundaries.iter().any(|boundary| {
                boundary.source.stream_id.starts_with("agentic-program:")
                    && boundary.step.is_none()
                    && boundary.cursor < self.acquisition_cursor
            })
        {
            return Err("immutable acquisition predates its exact publication binding".into());
        }
        Ok(())
    }
}

pub(super) struct ActualInputSnapshot {
    pub file_dependencies: Vec<ActualFileDependency>,
    pub immutable_inputs: Vec<ActualImmutableInput>,
    pub sources: std::collections::BTreeMap<String, u64>,
    pub material_events: Vec<harness_contract::goal::ProofInputEvent>,
    pub canonical_facts: Vec<serde_json::Value>,
}

impl ActualInputSnapshot {
    pub(super) fn required_scopes(&self) -> Vec<harness_contract::goal::ProofEffectTarget> {
        let mut targets = Vec::new();
        for dependency in &self.file_dependencies {
            if !targets.contains(&dependency.target) {
                targets.push(dependency.target.clone());
            }
        }
        targets.sort_by_key(super::proof_value_digest);
        targets
    }
}

fn retain_actual_material(
    materials: &mut Vec<harness_contract::goal::ProofInputEvent>,
    event: &crate::DurableRuntimeEvent,
) {
    let reference = harness_contract::goal::ProofInputEvent {
        stream_id: event.stream_id.clone(),
        sequence: event.sequence,
        event_id: event.event_id.clone(),
        payload_digest: super::proof_value_digest(&event.payload),
    };
    if !materials.contains(&reference) {
        materials.push(reference);
    }
}

#[derive(Clone)]
struct ActualFileWindow {
    begin: u64,
    end: u64,
    total: u64,
    dependency: ActualFileDependency,
}

fn complete_actual_file_windows(
    mut windows: Vec<ActualFileWindow>,
) -> Result<ActualFileDependency, String> {
    if windows.is_empty() {
        return Err("actual File input has no acquisition windows".into());
    }
    windows.sort_by_key(|window| std::cmp::Reverse(window.dependency.acquisition_cursor));
    let total = windows[0].total;
    let mut extents = std::collections::BTreeMap::<u64, u64>::new();
    for window in windows {
        if window.total != total || window.begin > window.end || window.end > total {
            return Err("actual File acquisition windows disagree about extent".into());
        }
        let mut begin = window.begin;
        let mut end = window.end;
        if let Some((&previous, &stop)) = extents.range(..=begin).next_back() {
            if stop >= begin {
                begin = previous;
                end = end.max(stop);
                extents.remove(&previous);
            }
        }
        while let Some((&start, &stop)) = extents.range(begin..).next() {
            if start > end {
                break;
            }
            end = end.max(stop);
            extents.remove(&start);
        }
        extents.insert(begin, end);
        if extents.get(&0) == Some(&total) {
            return Ok(window.dependency);
        }
    }
    Err(crate::agentic::fact_admission::refusal(
        harness_contract::refusal_codes::FactAdmissionCode::ScopeIncomplete,
        "proof_actual_input_extent_unsupported: actual File input lacks complete original windows",
        &[crate::agentic::fact_admission::StrategyGap {
            strategy: harness_contract::refusal_codes::FactAdmissionStrategy::Nested,
            missing: "the composite's File window does not cover the original body".to_string(),
        }],
    )
    .into())
}

fn require_actual_materials(
    recovered: &[harness_contract::goal::ProofInputEvent],
    submitted: &[harness_contract::goal::ProofInputEvent],
) -> Result<(), String> {
    if recovered.iter().any(|source| !submitted.contains(source)) {
        return Err(
            "proof omits independently recovered actual acquisition or consumer material".into(),
        );
    }
    Ok(())
}

fn effect_disjoint_from_target(
    target: &harness_contract::goal::ProofEffectTarget,
    scope: Option<&harness_contract::goal::ProofEffectScope>,
) -> bool {
    scope.is_some_and(|scope| canonical_scope_disjoint(std::slice::from_ref(target), scope))
}

fn input_changed_before_use(
    dependency: &ActualFileDependency,
    write_cursor: u64,
    call_key: Option<&str>,
) -> bool {
    write_cursor > dependency.acquisition_cursor
        && write_cursor < dependency.consumer_cursor
        && call_key != Some(dependency.consumer_call_key.as_str())
}

fn output_observation_is_fresh(
    original_effect_cursor: u64,
    read_cursor: u64,
    write_cursors: impl Iterator<Item = u64>,
) -> bool {
    read_cursor > original_effect_cursor
        && write_cursors.into_iter().all(|cursor| read_cursor > cursor)
}

pub(super) fn read_original_scope(
    primary: &crate::DurableRuntimeEvent,
    descriptor: &ToolEffectDescriptor,
    acquisition: &crate::context_evidence::ReadAcquisitionSeal,
) -> Result<Option<harness_contract::goal::ProofEffectScope>, String> {
    if primary.payload["resolved_effect_scope"].is_null()
        && matches!(
            &acquisition.body.extent,
            crate::context_evidence::ReadExtent::ArtifactChunks { .. }
        )
        && descriptor.effect_kind == ToolEffectKind::Read
        && !descriptor
            .scopes
            .iter()
            .any(|scope| scope.resource == harness_contract::policy::PermissionResource::File)
        && !descriptor.uses_network
        && !descriptor.spawns_process
        && !descriptor.mutates_packages
        && !descriptor.mutates_system
    {
        return Ok(None);
    }
    super::results::require_original_effect_scope(
        &primary.payload["resolved_effect_scope"],
        descriptor,
    )
    .map(Some)
}

/// Authenticate the original directory-discovery result as a historical
/// operation. It never proves file contents or the current directory state.
fn original_glob_discovery(
    primary: &crate::DurableRuntimeEvent,
    descriptor: &ToolEffectDescriptor,
    input: Option<&serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    if primary.payload["tool_name"] != "glob_search" {
        return Ok(None);
    }
    if descriptor.tool_id != "glob_search" || descriptor.effect_kind != ToolEffectKind::Read
        || descriptor.uses_network || descriptor.spawns_process
        || descriptor.mutates_packages || descriptor.mutates_system
        || primary.payload["outcome_truncated"] == true
    {
        return Err("original discovery has invalid effect or incomplete output".into());
    }
    let scope = super::results::require_original_effect_scope(
        &primary.payload["resolved_effect_scope"], descriptor,
    )?;
    if !crate::context_evidence::original_read_file_domain(descriptor, &scope) {
        return Err("original discovery File domain is incomplete".into());
    }
    let input = input.ok_or("original discovery input unavailable")?;
    let output = primary.payload["outcome"]["output"].as_str()
        .ok_or("original discovery output unavailable")?;
    let value: serde_json::Value = serde_json::from_str(output)
        .map_err(|_| "original discovery output is not JSON")?;
    let complete = validate_original_glob_pages(input, &value)?;
    Ok(Some(serde_json::json!({"scope":scope, "sha256":crate::context_evidence::read_sha256(output.as_bytes()),
        "bytes":output.len(), "reported_scan_complete":complete})))
}

fn validate_original_glob_pages(input: &serde_json::Value, value: &serde_json::Value) -> Result<bool, String> {
    let patterns = match input.get("patterns") {
        Some(value) => value.as_array().filter(|items| !items.is_empty())
            .ok_or("original discovery patterns invalid")?.iter().collect::<Vec<_>>(),
        None => vec![input],
    };
    let pages: Vec<&serde_json::Value> = if input.get("patterns").is_some() {
        let results = value["results"].as_array().ok_or("original discovery batch missing")?;
        if value["type"] != "glob_search" || results.len() != patterns.len()
            || value["count"].as_u64() != Some(results.len() as u64)
        { return Err("original discovery batch differs from request".into()); }
        let mut pages = Vec::with_capacity(results.len());
        for (position, result) in results.iter().enumerate() {
            if result["index"].as_u64() != Some(position as u64)
                || result["status"] != "success" || !result["error"].is_null()
            { return Err("original discovery batch member unavailable".into()); }
            pages.push(result.get("output").ok_or("original discovery page missing")?);
        }
        pages
    } else { vec![value] };
    let mut complete = true;
    for (pattern, page) in patterns.iter().zip(pages) {
        if page.get("content").is_some() || page.get("file").is_some() {
            return Err("original discovery output is not directory metadata".into());
        }
        let page: tools::file_ops::GlobSearchOutput = serde_json::from_value(page.clone())
            .map_err(|_| "original discovery page invalid")?;
        if pattern.get("pattern").and_then(|value| value.as_str()) != Some(page.pattern.as_str())
            || page.num_files != page.filenames.len()
            || (page.scan_complete && (page.truncated || !page.omissions.is_empty()
                || page.continuation_cursor.is_some() || page.next_request.is_some()))
            || page.coverage["consistency"] != "live_filesystem"
        { return Err("original discovery page disagrees with request or coverage".into()); }
        complete &= page.scan_complete;
    }
    Ok(complete)
}

/// Recover only original child reads whose parent receipt and accepted model
/// delivery can both be authenticated. Missing or dynamic children stay unknown.
fn collect_actual_nested_acquisitions(
    store: &std::sync::Arc<RuntimeEventStore>,
    primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent,
    original_input: Option<&serde_json::Value>,
    session_events: &[crate::DurableRuntimeEvent],
    boundary: &super::results::ResultInputBoundary,
    readers: super::ResultContentReaders<'_>,
    actual: &mut ActualInputSnapshot,
    windows: &mut std::collections::BTreeMap<String, Vec<ActualFileWindow>>,
) -> Result<bool, String> {
    use crate::context_evidence::{ReadExtent, CHILD_READ_ACQUISITIONS_FIELD};
    if primary.payload["tool_name"] != "tool_orchestrate" { return Ok(false); }
    let children = primary.payload[CHILD_READ_ACQUISITIONS_FIELD].as_array()
        .ok_or("original composite child acquisitions unavailable")?;
    if children.is_empty() || primary.payload[CHILD_READ_ACQUISITIONS_FIELD] != index.payload[CHILD_READ_ACQUISITIONS_FIELD]
        || primary.payload["child_read_acquisitions_complete"] != true
        || index.payload["child_read_acquisitions_complete"] != true
        || primary.payload["outcome_truncated"] == true {
        return Err("original composite child acquisitions incomplete".into());
    }
    let descriptor = super::results::receipt_effect_descriptor(primary)?;
    let scope = super::results::require_original_effect_scope(&primary.payload["resolved_effect_scope"], &descriptor)?;
    if !crate::context_evidence::original_read_file_domain(&descriptor, &scope) {
        return Err("original composite File domain is incomplete".into());
    }
    let input = original_input.ok_or("original composite input unavailable")?;
    let raw_input = primary.payload["original_input"].as_str().ok_or("original composite input absent")?;
    let stored: serde_json::Value = harness_contract::step::parse_strict_json(raw_input)
        .map_err(|_| "original composite input invalid")?;
    if &stored != input || primary.payload["input_sha256"] != crate::context_evidence::read_sha256(raw_input.as_bytes()) {
        return Err("original composite input identity changed".into());
    }
    let steps = input["steps"].as_array().ok_or("original composite steps missing")?;
    let raw_output = primary.payload["outcome"]["output"].as_str().ok_or("original composite output missing")?;
    let output: serde_json::Value = serde_json::from_str(raw_output).map_err(|_| "original composite output invalid")?;
    let trace = output["calls"].as_array().ok_or("original composite trace missing")?;
    if trace.len() != steps.len() { return Err("original composite trace incomplete".into()); }
    let mut expected = std::collections::BTreeSet::new();
    let mut ids = std::collections::BTreeSet::new();
    for (step_index, step) in steps.iter().enumerate() {
        let id = step["id"].as_str().filter(|id| !id.is_empty()).ok_or("original composite step identity missing")?;
        if !ids.insert(id) || trace[step_index]["stepId"] != id || trace[step_index]["status"] != "ok"
            || trace[step_index]["op"] != step["op"] {
            return Err("original composite trace identity mismatch".into());
        }
        let calls: Vec<&serde_json::Value> = match step["op"].as_str() {
            Some("call") => vec![step],
            Some("batch") => step["calls"].as_array().ok_or("original composite batch missing")?.iter().collect(),
            _ => return Err("original composite dynamic step is unproven".into()),
        };
        if trace[step_index]["calls"].as_u64() != Some(calls.len() as u64) {
            return Err("original composite execution count mismatch".into());
        }
        for (position, call) in calls.iter().enumerate() {
            match call["tool"].as_str() {
                Some("read_file" | "grep_search") => { expected.insert((step_index as u64, position as u64)); }
                Some("glob_search") => {
                    let member = if step["op"] == "call" {
                        &output["steps"][id]
                    } else {
                        let alias = call["as"].as_str().map(str::to_string)
                            .unwrap_or_else(|| format!("glob_search[{position}]"));
                        if calls.iter().enumerate().filter(|(i, item)| {
                            item["as"].as_str().map(str::to_string).unwrap_or_else(||
                                format!("{}[{i}]", item["tool"].as_str().unwrap_or(""))) == alias
                        }).count() != 1 { return Err("original composite discovery alias is ambiguous".into()); }
                        &output["steps"][id][&alias]
                    };
                    let member = if step["on_error"] == "collect" && step["op"] == "batch" {
                        if member["ok"] != true { return Err("original composite discovery failed".into()); }
                        &member["output"]
                    } else { member };
                    let member_input = call.get("input").ok_or("original composite discovery input missing")?;
                    let complete = validate_original_glob_pages(member_input, member)?;
                    let _ = complete; // Historical discovery; not a completeness proof.
                }
                _ => return Err("original composite child operation is unproven".into()),
            }
        }
    }
    let mut observed = std::collections::BTreeSet::new();
    let mut child_positions = std::collections::BTreeSet::new();
    let mut reader = super::read_source::StoreReadSource(store.as_ref());
    for child in children {
        let step = child["step_index"].as_u64().ok_or("original child step missing")?;
        let position = child["position"].as_u64().ok_or("original child position missing")?;
        let (batch, window) = match child["tool_name"].as_str() {
            Some("read_file") if child["batch_index"].is_null() && child["window_index"].is_null() => (None, None),
            Some("grep_search") => (child["batch_index"].as_u64(), Some(child["window_index"].as_u64()
                .ok_or("original child window missing")?)),
            _ => return Err("original composite child kind unknown".into()),
        };
        if !expected.contains(&(step, position)) || !child_positions.insert((step, position, batch, window)) {
            return Err("original composite child is foreign or duplicated".into());
        }
        let mut confirmed = None;
        for event in session_events.iter().filter(|event|
            event.kind == "context.nested_read_model_observed"
                && event.commit_cursor < boundary.cursor
                && event.payload["parent_event_id"] == primary.event_id
                && event.payload["child"] == *child) {
            let turn = event.payload["parent"]["step"]["turn_id"].as_str()
                .ok_or("nested original turn missing")?;
            let root = boundary.producer.strip_prefix("root-execution:");
            if let Some(read) = verify_nested_read(&mut reader, event, &program_session(primary)?, turn,
                root, boundary.packet.as_ref(), &mut actual.sources)? {
                confirmed = Some(read);
                break;
            }
        }
        let read = confirmed.ok_or("original composite child delivery unavailable")?;
        let ReadExtent::FileLines {start_line, line_count, total_lines, source_omitted, ..} = &read.extent else {
            return Err("original composite child is not a File window".into());
        };
        if *start_line == 0 || (*source_omitted && *start_line == 1 && line_count == total_lines) {
            return Err("original composite child window invalid".into());
        }
        let begin = start_line.checked_sub(1).ok_or("nested window underflow")?;
        let end = begin.checked_add(*line_count).ok_or("nested window overflow")?;
        let dependency = ActualFileDependency {
            target: read.target, acquisition_cursor: primary.commit_cursor,
            consumer_cursor: boundary.cursor, consumer_call_key: boundary.call_key.clone(),
            source_sha256: read.body.source_sha256, source_bytes: read.body.source_bytes,
        };
        if let Some(frame) = readers.process_inputs.filter(|frame| frame.has_original_file_workspace()) {
            frame.retain_original_file(&dependency.target, &dependency.source_sha256, dependency.source_bytes)?;
        }
        let key = super::proof_value_digest(&serde_json::json!([
            dependency.target, boundary.call_key, dependency.source_sha256, dependency.source_bytes, total_lines
        ]));
        windows.entry(key).or_default().push(ActualFileWindow { begin, end, total: *total_lines, dependency });
        retain_actual_material(&mut actual.material_events, &read.event);
        observed.insert((step, position));
    }
    if observed != expected { return Err("original composite child coverage incomplete".into()); }
    Ok(true)
}

#[cfg(test)]
pub(crate) fn collect_actual_nested_from_original_host_for_test(
    store: &std::sync::Arc<RuntimeEventStore>, primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent, session_events: &[crate::DurableRuntimeEvent],
    input: &str, root: &str,
) -> Result<Vec<String>, String> {
    let last_delivery = session_events.iter().filter(|event|
        event.kind == "context.nested_read_model_observed"
            && event.payload["parent_event_id"] == primary.event_id)
        .map(|event| event.commit_cursor).max().ok_or("actual child delivery missing")?;
    let graph_events = store.list_stream(root).map_err(|error| error.to_string())?;
    let consumer = session_events.iter().chain(graph_events.iter())
        .filter(|event| event.commit_cursor > last_delivery)
        .min_by_key(|event| event.commit_cursor).ok_or("actual continuation boundary missing")?;
    let boundary = super::results::ResultInputBoundary {
        producer: format!("root-execution:{root}"), packet: None, step: None,
        cursor: consumer.commit_cursor, call_key: "original-host-test-consumer".into(),
        source: harness_contract::goal::ProofInputEvent {
            stream_id: consumer.stream_id.clone(), sequence: consumer.sequence,
            event_id: consumer.event_id.clone(), payload_digest: super::proof_value_digest(&consumer.payload),
        },
    };
    let mut actual = ActualInputSnapshot {
        file_dependencies: Vec::new(), immutable_inputs: Vec::new(),
        sources: std::collections::BTreeMap::new(), material_events: Vec::new(), canonical_facts: Vec::new(),
    };
    let mut windows = std::collections::BTreeMap::new();
    let input: serde_json::Value = harness_contract::step::parse_strict_json(input)
        .map_err(|error| error.to_string())?;
    if !collect_actual_nested_acquisitions(store, primary, index, Some(&input), session_events,
        &boundary, super::ResultContentReaders::default(), &mut actual, &mut windows)? {
        return Err("original composite was not processed".into());
    }
    let mut paths = windows.into_values().map(complete_actual_file_windows)
        .map(|result| result.map(|dependency| dependency.target.path.workspace_relative_path))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    Ok(paths)
}

fn program_session(primary: &crate::DurableRuntimeEvent) -> Result<String, String> {
    primary.payload["replay"]["session_id"].as_str().filter(|id| !id.is_empty())
        .map(str::to_string).ok_or_else(|| "original composite session missing".into())
}

pub(super) fn verify_actual_read_pair(
    primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent,
) -> Result<ToolEffectDescriptor, String> {
    let descriptor = super::results::receipt_effect_descriptor(primary)?;
    if primary.kind != "execution.read.receipt"
        || primary.schema_version != 1
        || crate::ExecutionCommitService::receipt_observation_sequence(&index).is_none()
        || primary.actor.as_deref() != Some("governed_tool")
        || primary.actor != index.actor
        || primary.scope != RuntimeEventScope::ExecutionNode
        || primary.scope != index.scope
        || index.kind != "execution.agent_tool.receipt"
        || primary.transaction_id != index.transaction_id
        || primary.commit_cursor != index.commit_cursor
        || index.transaction_index <= primary.transaction_index
        || index.payload["effect_kind"] != "read"
        || primary.payload["idempotency_key"] != index.payload["idempotency_key"]
        || primary.status.as_deref() != Some("completed")
        || index.status != primary.status
        || primary.payload["tool_name"] != index.payload["outcome"]["tool_name"]
        || primary.payload["tool_use_id"] != index.payload["outcome"]["tool_use_id"]
        || !primary.payload["effect"].is_null()
            && primary.payload["effect"] != primary.payload["replay"]["invocation"]["effect"]
        || !index.payload["effect"].is_null()
            && index.payload["effect"] != primary.payload["replay"]["invocation"]["effect"]
        || primary.payload["outcome"] != index.payload["outcome"]
        || primary.payload["resolved_effect_scope"] != index.payload["resolved_effect_scope"]
        || primary.payload["outcome"]["status"] != "executed"
        || primary.payload["outcome"]["error"].as_str().is_some()
        || descriptor.effect_kind != ToolEffectKind::Read
    {
        return Err("actual input does not authenticate one strictly earlier acquisition".into());
    }
    Ok(descriptor)
}

// These candidates describe only what the original verifier established.
// Verification errors remain hard errors and cannot become missing strategies.
enum AcquisitionCandidate {
    Proven(harness_contract::refusal_codes::FactAdmissionStrategy),
    Missing(Vec<crate::agentic::fact_admission::StrategyOutcome>),
}

fn collect_actual_acquisition(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent,
    original_input: Option<&serde_json::Value>,
    session_events: &[crate::DurableRuntimeEvent],
    boundary: &super::results::ResultInputBoundary,
    readers: super::ResultContentReaders<'_>,
    actual: &mut ActualInputSnapshot,
    windows: &mut std::collections::BTreeMap<String, Vec<ActualFileWindow>>,
) -> Result<(), super::results::ReviewEvidenceError> {
    // Both production callers propagate any failure and discard their entire
    // local snapshot. Until this decision succeeds it cannot become a proof.
    let result = (|| {
        let candidate = collect_actual_acquisition_candidate(
            store, program, primary, index, original_input, session_events, boundary,
            readers, actual, windows,
        )?;
        let outcomes = match candidate {
            AcquisitionCandidate::Proven(strategy) => vec![crate::agentic::fact_admission::StrategyOutcome::admitted(strategy)],
            AcquisitionCandidate::Missing(outcomes) => outcomes,
        };
        crate::agentic::fact_admission::require(
            &outcomes,
            harness_contract::refusal_codes::FactAdmissionCode::Unproven,
            "proof_actual_input_extent_unsupported: actual read lacks an original physical extent",
        )?;
        Ok(())
    })();
    let result: Result<(), super::results::ReviewEvidenceError> = result;
    if let Err(error) = &result {
        record_review_acquisition_rejection(
            store,
            program,
            primary,
            actual,
            windows.len(),
            readers.process_inputs.is_some(),
            &error.to_string(),
        );
    }
    result
}

/// Persist a refused review acquisition on its own stream.
///
/// The review turn is charged for an attempt it could not complete: the actual
/// input is refused because the read carries no original physical extent, and
/// the attempt is charged to the model instead of to the structural fault. The
/// durable record is the only place that says which read lacked the extent.
///
/// It observes. The stream is its own - never the semantic lane's - and nothing
/// is re-armed by it.
fn record_review_acquisition_rejection(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    primary: &crate::DurableRuntimeEvent,
    actual: &ActualInputSnapshot,
    window_groups: usize,
    frame_owned: bool,
    reason: &str,
) {
    let stream_id = format!(
        "agentic-review-acquisition-rejected:{}",
        program.program_id
    );
    let payload = serde_json::json!({
        "program_id": program.program_id,
        "session_id": program.session_id,
        "disposition": "incomplete_snapshot_not_published",
        "reason": reason,
        "sources": actual.sources.len(),
        "material_events": actual.material_events.len(),
        "canonical_facts": actual.canonical_facts.len(),
        "window_groups": window_groups,
        "frame_cleanup": if frame_owned { "owned_by_frame" } else { "no_frame" },
        "primary_event_id": primary.event_id,
    });
    let request = crate::AppendTransactionRequest {
        transaction_id: format!(
            "agentic-review-acquisition-rejected:{}:{}:{}",
            program.program_id,
            primary.event_id,
            if frame_owned { "owned_by_frame" } else { "no_frame" }
        ),
        expected_streams: Vec::new(),
        events: vec![crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: stream_id.clone(),
                scope: RuntimeEventScope::Relation,
                kind: "agentic.review_acquisition_rejected".into(),
                status: Some("incomplete_snapshot_not_published".into()),
                actor: Some(program.session_id.clone()),
                refs: Vec::new(),
                payload,
            },
            idempotency_key: None,
            schema_version: 1,
        }],
    };
    // The store refuses a transaction that does not declare the current
    // revision of every stream it writes, so the fence is read and retried.
    // The write itself is never retried.
    let mut attempt = 0_u8;
    loop {
        let expected = store.stream_revision(&stream_id).unwrap_or_default();
        let request = crate::AppendTransactionRequest {
            transaction_id: request.transaction_id.clone(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: stream_id.clone(),
                expected_revision: expected,
            }],
            events: request.events.clone(),
        };
        match store.append_transaction(request) {
            Ok(_) => return,
            Err(error) if attempt < 8 && error.to_string().contains("revision") => {
                attempt = attempt.saturating_add(1);
                continue;
            }
            Err(error) => {
                tracing::debug!(
                    program_id = program.program_id,
                    %error,
                    "review acquisition rejection not appended"
                );
                return;
            }
        }
    }
}

fn collect_actual_acquisition_candidate(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent,
    original_input: Option<&serde_json::Value>,
    session_events: &[crate::DurableRuntimeEvent],
    boundary: &super::results::ResultInputBoundary,
    readers: super::ResultContentReaders<'_>,
    actual: &mut ActualInputSnapshot,
    windows: &mut std::collections::BTreeMap<String, Vec<ActualFileWindow>>,
) -> Result<AcquisitionCandidate, super::results::ReviewEvidenceError> {
    use crate::context_evidence::{ReadExtent, READ_ACQUISITION_FIELD};
    let descriptor = verify_actual_read_pair(primary, index)?;
    if primary.commit_cursor >= boundary.cursor {
        return Err("actual input does not authenticate one strictly earlier acquisition".into());
    }
    for event in [primary, index] {
        super::results::pin_result_source(store, &event.stream_id, &mut actual.sources)?;
        retain_actual_material(&mut actual.material_events, event);
    }
    let seal_value = &primary.payload[READ_ACQUISITION_FIELD];
    if primary.payload["tool_name"].as_str() == Some("read_ref") {
        let input = match (original_input, seal_value.is_null()) {
            (Some(input), _) => Some(input),
            (None, true) => return Err("unsealed read_ref original input unavailable".into()),
            (None, false) => None,
        };
        if let Some(input) = input {
            if crate::agentic::action_invocation::read_ref_directory_input(input)?.is_some() {
                if !seal_value.is_null() {
                    return Err("directory metadata has an unexpected acquisition seal".into());
                }
                // Directory state is metadata, not immutable artifact material.
                return Ok(AcquisitionCandidate::Proven(harness_contract::refusal_codes::FactAdmissionStrategy::Directory));
            }
        }
    }
    if seal_value.is_null() {
        // 034 GB-1 (batch 1): the three proof strategies are asked in the
        // fixed order, and the gate records which one answered. When none
        // did, the refusal carries what each of them was missing - one
        // refusal, a complete diagnosis - and keeps the fragmented gate's
        // original wording verbatim at its head.
        // The nested strategy answers first and, when it does, the fact is
        // in: the gate records that answer and the read is admitted. (Losing
        // this early return would let a nested acquisition fall through to
        // the immutable judgment and be refused by a gate it already
        // answered.)
        if collect_actual_nested_acquisitions(store, primary, index, original_input, session_events,
            boundary, readers, actual, windows)? {
            actual.canonical_facts.push(serde_json::json!({
                "grade":"acquired_nested", "producer":boundary.producer,
                "consumer":boundary.call_key, "consumer_cursor":boundary.cursor,
                "acquisition_cursor":primary.commit_cursor, "source":primary.event_id,
                "index":index.event_id,
            }));
            return Ok(AcquisitionCandidate::Proven(harness_contract::refusal_codes::FactAdmissionStrategy::Nested));
        }
        // Primary and index remain in the audited material set above. A
        // discovery page proves neither file contents nor current absence.
        if original_glob_discovery(primary, &descriptor, original_input)?.is_some() {
            return Ok(AcquisitionCandidate::Proven(harness_contract::refusal_codes::FactAdmissionStrategy::Discovery));
        }
        if !immutable_receipt_without_workspace_extent(primary, &descriptor, original_input)? {
            return Ok(AcquisitionCandidate::Missing(vec![
                    crate::agentic::fact_admission::StrategyOutcome::missing(
                        harness_contract::refusal_codes::FactAdmissionStrategy::Nested,
                        "the composite's child acquisitions do not cover this read",
                    ),
                    crate::agentic::fact_admission::StrategyOutcome::missing(
                        harness_contract::refusal_codes::FactAdmissionStrategy::Discovery,
                        "the read is not a discovery page",
                    ),
                    crate::agentic::fact_admission::StrategyOutcome::missing(
                        harness_contract::refusal_codes::FactAdmissionStrategy::Immutable,
                        "the read named a workspace path and its receipt carries no acquisition seal",
                    ),
                ]));
        }
        let output = primary.payload["outcome"]["output"]
            .as_str()
            .ok_or("actual immutable input has no original body")?;
        if primary.payload["outcome_truncated"] == true {
            return Err(
                "actual immutable input body is truncated without an acquisition seal".into(),
            );
        }
        actual.canonical_facts.push(serde_json::json!({
            "grade":"acquired_immutable_receipt", "producer":boundary.producer,
            "consumer":boundary.call_key, "consumer_cursor":boundary.cursor,
            "acquisition_cursor":primary.commit_cursor,
            "source":primary.event_id, "index":index.event_id,
            "scope":primary.payload["resolved_effect_scope"],
            "sha256":crate::context_evidence::read_sha256(output.as_bytes()), "bytes":output.len(),
        }));
        return Ok(AcquisitionCandidate::Proven(harness_contract::refusal_codes::FactAdmissionStrategy::Immutable));
    }
    let seal =
        crate::context_evidence::decode_read_acquisition(seal_value).map_err(str::to_string)?;
    if index.payload.get(READ_ACQUISITION_FIELD) != Some(seal_value)
        || primary.stream_id != seal.primary_stream
        || primary.idempotency_key.as_deref() != Some(seal.primary_key.as_str())
        || index.stream_id != seal.index_stream
        || index.idempotency_key.as_deref() != Some(seal.index_key.as_str())
        || primary.payload["input_sha256"] != seal.input_sha256
        || primary.payload["replay"]["origin"] != seal.origin
        || primary.payload["replay"]["call_id"] != seal.invocation_id
        || primary.payload["tool_name"] != seal.tool_name
    {
        return Err("actual input seal disagrees with original acquisition identity".into());
    }
    if let Some(packet) = &boundary.packet {
        if seal.graph_id != packet.graph_id()
            || seal.node_id != packet.node_id()
            || seal.attempt != packet.attempt
            || seal.session_id != packet.session_id()
            || seal.step.run_ref.as_deref() != Some(packet.run_id())
        {
            return Err("actual input acquisition belongs to another producer attempt".into());
        }
    } else if boundary.producer.strip_prefix("root-execution:") != seal.step.execution_id.as_deref()
    {
        return Err("actual root input belongs to another execution".into());
    }
    let scope = read_original_scope(primary, &descriptor, &seal)?;
    let output = super::results::original_acquisition_output(
        store,
        primary,
        session_events,
        readers,
        &mut actual.sources,
    )?;
    actual.canonical_facts.push(serde_json::json!({
        "grade":"acquired", "producer":boundary.producer, "consumer":boundary.call_key,
        "consumer_step":boundary.step.as_ref().map(|step| &step.origin),
        "consumer_cursor":boundary.cursor, "acquisition_cursor":primary.commit_cursor,
        "source":primary.event_id, "index":index.event_id, "transaction":primary.transaction_id,
        "acquisition":seal, "scope":scope,
        "original_output_sha256":crate::context_evidence::read_sha256(output.as_bytes()),
    }));
    match &seal.body.extent {
        ReadExtent::FileLines {
            start_line,
            line_count,
            total_lines,
            source_omitted,
            ..
        } => {
            let scope = scope.as_ref().ok_or_else(|| {
                crate::agentic::fact_admission::refusal(
                    harness_contract::refusal_codes::FactAdmissionCode::ScopeIncomplete,
                    "actual File input lacks original scope v2",
                    &[crate::agentic::fact_admission::StrategyGap {
                        strategy: harness_contract::refusal_codes::FactAdmissionStrategy::Immutable,
                        missing: "the receipt carries no scope for the body it claims".to_string(),
                    }],
                )
            })?;
            if !scope.complete
                || *start_line == 0
                || *source_omitted && *start_line == 1 && line_count == total_lines
            {
                return Err(crate::agentic::fact_admission::refusal(
                    harness_contract::refusal_codes::FactAdmissionCode::ScopeIncomplete,
                    "proof_actual_input_extent_unsupported: original File body is incomplete",
                    &[crate::agentic::fact_admission::StrategyGap {
                        strategy: harness_contract::refusal_codes::FactAdmissionStrategy::Immutable,
                        missing: "the File body the seal covers is incomplete".to_string(),
                    }],
                )
                .into());
            }
            let targets = scope
                .targets
                .iter()
                .filter(|target| {
                    target.access_mode == harness_contract::context::WorkspaceAccessMode::Read
                        && target.path.object_kind
                            == harness_contract::context::WorkspaceObjectKind::File
                        && target.exact
                })
                .collect::<Vec<_>>();
            if targets.len() != 1 {
                            return Err(crate::agentic::fact_admission::refusal(
                                harness_contract::refusal_codes::FactAdmissionCode::ScopeIncomplete,
                                "proof_actual_input_extent_unsupported: actual File has no unique original canonical target",
                                &[crate::agentic::fact_admission::StrategyGap {
                                    strategy: harness_contract::refusal_codes::FactAdmissionStrategy::Immutable,
                                    missing: "the scope names no unique exact File target".to_string(),
                                }],
                            )
                            .into());
            }
            let begin = start_line
                .checked_sub(1)
                .ok_or("actual File window start underflow")?;
            let end = begin
                .checked_add(*line_count)
                .ok_or("actual File window overflow")?;
            let dependency = ActualFileDependency {
                target: targets[0].clone(),
                acquisition_cursor: primary.commit_cursor,
                consumer_cursor: boundary.cursor,
                consumer_call_key: boundary.call_key.clone(),
                source_sha256: seal.body.source_sha256.clone(),
                source_bytes: seal.body.source_bytes,
            };
            if let Some(frame) = readers.process_inputs.filter(|frame| frame.has_original_file_workspace()) {
                frame.retain_original_file(&dependency.target, &dependency.source_sha256, dependency.source_bytes)?;
            }
            let identity = super::proof_value_digest(&serde_json::json!([
                dependency.target,
                boundary.call_key,
                seal.body.source_sha256,
                seal.body.source_bytes,
                total_lines
            ]));
            windows.entry(identity).or_default().push(ActualFileWindow {
                begin,
                end,
                total: *total_lines,
                dependency,
            });
        }
        ReadExtent::ArtifactChunks { .. } => {
            let input = ActualImmutableInput::from_acquisition(
                store, program, primary, index, session_events,
                boundary.packet.as_ref(), readers, actual,
            ).map_err(|error| {
                if error.to_string().starts_with("immutable acquisition has no authenticated root producer") {
                    super::results::ReviewEvidenceError::from(format!("{error}; caller=actual_boundary producer={:?} packet_present={}",
                        boundary.producer, boundary.packet.is_some()))
                } else { error }
            })?;
            if !actual.immutable_inputs.contains(&input) {
                actual.immutable_inputs.push(input);
            }
        }
    }
    // This admits only the original authenticated acquisition (a window for
    // FileLines); complete_actual_file_windows still decides full coverage.
    Ok(AcquisitionCandidate::Proven(harness_contract::refusal_codes::FactAdmissionStrategy::Sealed))
}

pub(super) fn authenticate_unsealed_delegated_input(
    store: &std::sync::Arc<RuntimeEventStore>,
    primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent,
    packet: &harness_contract::agent::AgentTaskPacket,
    actual: &mut ActualInputSnapshot,
) -> Result<serde_json::Value, super::results::ReviewEvidenceError> {
    use harness_contract::step::CallSource;
    let key = primary.payload["idempotency_key"]
        .as_str()
        .ok_or("actual input original key missing")?;
    let turn = packet
        .assignment
        .execution_identity
        .turn_id()
        .ok_or("actual input producer has no admitted turn")?;
    let graphs = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store));
    super::results::pin_result_source(
        store,
        &format!("execution-lineage:{}", packet.graph_id()),
        &mut actual.sources,
    )?;
    let mut selected = None;
    for link in graphs
        .child_links(packet.graph_id())
        .map_err(|error| error.to_string())?
    {
        if link.parent_node_id != packet.node_id() {
            continue;
        }
        let records = super::results::result_source_events(
            store,
            &link.child_execution_id,
            &mut actual.sources,
        )?;
        let child = graphs
            .load(&link.child_execution_id)
            .map_err(|error| error.to_string())?;
        if !crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(&child) {
            continue;
        }
        let bound =
            crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_graph(
                &child,
            )
            .map_err(|_| "actual input original child payload invalid")?;
        let request = bound.request;
        if request.idempotency_key != key {
            continue;
        }
        if selected.is_some()
            || bound.agent_id != packet.agent_id()
            || bound.run_id != packet.run_id()
            || child.lineage.as_ref().is_none_or(|lineage| {
                lineage.session_id != packet.session_id() || lineage.turn_id != turn
            })
        {
            return Err(
                "actual immutable input child has ambiguous or foreign original ownership".into(),
            );
        }
        let step = request
            .step_binding
            .as_ref()
            .ok_or("actual input original Step missing")?;
        let identity = &step.snapshot.identity;
        let invocation = request
            .compiled_invocation
            .as_ref()
            .ok_or("actual input original compiled invocation missing")?;
        let authorization = request
            .authorization
            .as_ref()
            .ok_or("actual input original grant missing")?;
        if authorization.tool_id != request.tool_name
            || authorization.descriptor_hash != invocation.effect.descriptor_hash
            || authorization.policy_revision != request.policy_revision
            || request.policy_revision == 0
            || authorization.authorization_lease.policy_revision != request.policy_revision
            || authorization.authorization_lease.effect_descriptor_hash
                != invocation.effect.descriptor_hash
            || !authorization
                .authorization_lease
                .permits(&request.tool_name, invocation.effect.required_permission)
            || invocation
                .effect
                .scopes
                .iter()
                .any(|scope| !authorization.authorization_lease.scopes.contains(scope))
            || index.payload["effect_scope"]
                != serde_json::to_value(&authorization.scope).map_err(|error| error.to_string())?
            || !primary.payload["effect"].is_null()
                && primary.payload["effect"]
                    != serde_json::to_value(&invocation.effect)
                        .map_err(|error| error.to_string())?
            || !index.payload["effect"].is_null()
                && index.payload["effect"]
                    != serde_json::to_value(&invocation.effect)
                        .map_err(|error| error.to_string())?
        {
            return Err(
                "actual immutable input original grant or parallel effect descriptor conflicts"
                    .into(),
            );
        }
        let input = harness_contract::step::parse_strict_json(&request.input)
            .map_err(|error| error.to_string())?;
        let mut stored_invocation = invocation.clone();
        stored_invocation.intent.normalized_input = serde_json::Value::Null;
        let source_matches = match &step.origin.source {
            CallSource::Provider { attempt_id } => {
                crate::agentic::action_invocation::execution_ids_agree(
                    identity.execution_id.as_deref(),
                    packet.run_id(),
                )
                    && identity
                        .attempt
                        .is_none_or(|attempt| attempt == packet.attempt)
                    && identity.actor_ref.as_deref()
                        == Some(packet.assignment.execution_identity.principal_id())
                    && attempt_id == &step.snapshot.provider_attempt.attempt_id
            }
            CallSource::Process { worker_id, .. } => {
                crate::agentic::action_invocation::execution_ids_agree(
                    identity.execution_id.as_deref(),
                    packet.graph_id(),
                )
                    && identity.attempt == Some(packet.attempt)
                    && worker_id == packet.run_id()
                    && identity.actor_ref.as_deref() == Some(packet.agent_id())
            }
            _ => false,
        };
        if !source_matches
            || identity.session_id != packet.session_id()
            || identity.turn_id != turn
            || identity.run_ref.as_deref() != Some(packet.run_id())
            || identity.step_id != step.origin.origin_step_id
            || step.origin.catalog_digest != step.snapshot.catalog_digest
            || request.session_id.as_deref() != Some(packet.session_id())
            || request.parent_execution.as_ref().is_none_or(|parent| {
                parent.execution_id != packet.graph_id() || parent.node_id != packet.node_id()
            })
            || request.parent_execution_attempt != Some(packet.attempt)
            || step.effect_idempotency_key() != key
            || request.tool_use_id != step.origin.call_id
            || invocation.invocation_id != request.tool_use_id
            || invocation.intent.invocation_id != request.tool_use_id
            || invocation.intent.tool_name != request.tool_name
            || invocation.origin.as_ref() != Some(&step.origin)
            || invocation.intent.normalized_input != input
            || primary.idempotency_key.as_deref() != Some(format!("{key}:read-receipt").as_str())
            || index.idempotency_key.as_deref()
                != Some(format!("agent-tool-receipt:{key}").as_str())
            || primary.stream_id != format!("execution-effect:{key}")
            || index.stream_id
                != format!(
                    "execution-agent-receipts:{}:{}:{}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                )
            || primary.payload["replay"]["session_id"] != packet.session_id()
            || primary.payload["replay"]["parent_execution"]
                != serde_json::to_value(&request.parent_execution)
                    .map_err(|error| error.to_string())?
            || primary.payload["replay"]["origin"]
                != serde_json::to_value(&step.origin).map_err(|error| error.to_string())?
            || primary.payload["replay"]["call_id"] != request.tool_use_id
            || primary.payload["replay"]["invocation"]
                != serde_json::to_value(&stored_invocation).map_err(|error| error.to_string())?
            || primary.payload["replay"]["payload_digest"]
                != harness_contract::step::canonical_digest("tool-effect-payload", &input)
                    .map_err(|error| error.to_string())?
            || primary.payload["input_sha256"]
                != crate::context_evidence::read_sha256(request.input.as_bytes())
            || primary.payload["tool_name"] != request.tool_name
            || primary.payload["tool_use_id"] != request.tool_use_id
            || crate::ExecutionCommitService::receipt_observation_sequence(&index) != Some(request.observation_wave_sequence)
            || index.payload["outcome"]["tool_use_id"] != request.tool_use_id
            || index.payload["outcome"]["tool_name"] != request.tool_name
            || index.payload["authorized_scopes"]
                != serde_json::to_value(&request.authorized_scopes)
                    .map_err(|error| error.to_string())?
        {
            return Err("actual immutable input differs from its original producer Step/request/compiled identity".into());
        }
        if !primary.payload["resolved_effect_scope"].is_null() {
            crate::execution_core::graph::ExecutionCommitService::validate_proof_bound_tool_request(
                &request, primary, index,
            ).map_err(|error| error.to_string())?;
        }
        let original = records
            .first()
            .ok_or("actual input original child registration missing")?;
        if original.commit_cursor >= primary.commit_cursor {
            return Err(
                "actual input original request was not registered before acquisition".into(),
            );
        }
        retain_actual_material(&mut actual.material_events, original);
        selected = Some(input);
    }
    selected.ok_or_else(|| {
        "actual immutable input has no authenticated original producer request".into()
    })
}

fn authenticate_actual_consumer(
    boundary: &super::results::ResultInputBoundary,
    consumer: &crate::DurableRuntimeEvent,
    session_events: &[crate::DurableRuntimeEvent],
) -> Result<(), String> {
    if consumer.commit_cursor != boundary.cursor {
        return Err("actual input consumer cursor differs from its original source".into());
    }
    match consumer.kind.as_str() {
        "execution.effect.intent" => {
            let origin: harness_contract::step::CallOrigin =
                serde_json::from_value(consumer.payload["replay"]["origin"].clone())
                    .map_err(|_| "actual consumer original call origin missing")?;
            let session = consumer.payload["replay"]["session_id"]
                .as_str()
                .ok_or("actual consumer original session missing")?;
            let key = harness_contract::step::canonical_digest(
                "step-effect-identity",
                &serde_json::json!([session, origin.origin_step_id, origin.call_id]),
            )
            .map_err(|error| error.to_string())?;
            if consumer.actor.as_deref() != Some("governed_tool")
                || boundary.call_key != key
                || consumer.payload["idempotency_key"] != key
                || consumer.payload["replay"]["call_id"] != origin.call_id
                || boundary.step.as_ref().is_some_and(|step| {
                    step.origin != origin
                        || step.effect_idempotency_key() != key
                        || step.snapshot.identity.session_id != session
                })
                || boundary.packet.as_ref().is_some_and(|packet| {
                    session != packet.session_id()
                        || consumer.payload["replay"]["parent_execution"]["execution_id"]
                            != packet.graph_id()
                        || consumer.payload["replay"]["parent_execution"]["node_id"]
                            != packet.node_id()
                })
            {
                return Err(
                    "actual consumer does not match its original admitted call/Step".into(),
                );
            }
        }
        "tool.invocation.completed" => {
            let task = super::results::result_tool_plan(session_events, consumer)?
                .ok_or("actual consumer original Session plan missing")?;
            let step: crate::step_snapshot::StepCallBinding =
                serde_json::from_value(task["step_binding"].clone())
                    .map_err(|_| "actual consumer original Session Step missing")?;
            if boundary.step.as_ref() != Some(&step)
                || boundary.call_key != step.effect_idempotency_key()
            {
                return Err(
                    "actual publication consumer cannot borrow another original call/Step".into(),
                );
            }
        }
        "agentic.action_applied" => {
            let envelope: harness_contract::agent_action::AgentActionEnvelope =
                serde_json::from_value(consumer.payload["envelope"].clone())
                    .map_err(|_| "actual publication original action missing")?;
            let original_producer = match envelope.actor.execution_id.as_deref() {
                Some(execution)
                    if boundary.packet.as_ref().is_some_and(|packet| {
                        super::packet_identity_matches(
                            packet.graph_id(),
                            packet.run_id(),
                            execution,
                        )
                    }) =>
                {
                    boundary.producer.clone()
                }
                None => format!(
                    "root-execution:{}",
                    envelope
                        .actor
                        .root_execution_id
                        .as_deref()
                        .ok_or("actual publication root identity missing")?
                ),
                _ => return Err("actual publication has a foreign original producer".into()),
            };
            if boundary.producer != original_producer
                || boundary.step.is_some()
                || boundary.call_key != format!("publication:{}", consumer.event_id)
            {
                return Err("actual publication cannot change its original input boundary".into());
            }
        }
        _ => return Err("actual input has no supported original consumer boundary".into()),
    }
    Ok(())
}

fn remember_primary_content(
    contents: &mut std::collections::BTreeMap<String, harness_contract::context::ArtifactRef>,
    identity: &str,
    content: &harness_contract::context::ArtifactRef,
) -> Result<(), String> {
    if contents.get(identity).is_some_and(|previous| {
        previous
            .sha256
            .strip_prefix("sha256:")
            .unwrap_or(&previous.sha256)
            != content
                .sha256
                .strip_prefix("sha256:")
                .unwrap_or(&content.sha256)
            || previous.bytes != content.bytes
            || previous.selector != content.selector
            || previous.visibility_scope != content.visibility_scope
            || previous.durability != content.durability
    }) {
        return Err("proof primary input has conflicting original content versions".into());
    }
    contents.insert(identity.to_string(), content.clone());
    Ok(())
}

pub(super) fn primary_producer_immutable_inputs(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    producers: &[super::results::ResultProducer],
    readers: super::ResultContentReaders<'_>,
    expected_cuts: Option<&std::collections::BTreeMap<String, u64>>,
) -> Result<ActualInputSnapshot, super::results::ReviewEvidenceError> {
    primary_producer_immutable_inputs_with_tail(store, program, producers, readers, expected_cuts, None)
}

fn primary_producer_immutable_inputs_with_tail(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    producers: &[super::results::ResultProducer],
    readers: super::ResultContentReaders<'_>,
    expected_cuts: Option<&std::collections::BTreeMap<String, u64>>,
    session_tail: Option<&HistoricalSessionCut>,
) -> Result<ActualInputSnapshot, super::results::ReviewEvidenceError> {
    let mut actual = ActualInputSnapshot {
        file_dependencies: Vec::new(),
        immutable_inputs: Vec::new(),
        sources: std::collections::BTreeMap::new(),
        material_events: Vec::new(),
        canonical_facts: Vec::new(),
    };
    let source = format!("session:{}", program.session_id);
    let events = match session_tail {
        Some(tail) if tail.source.stream_id == source =>
            super::results::result_source_events_at_session_cut(store, &source, &mut actual.sources,
                tail.source.expected_revision, tail.observed_revision)?,
        Some(_) => return Err("primary acquisition Session tail belongs to another source".into()),
        None => super::results::result_source_events(store, &source, &mut actual.sources)?,
    };
    let cut = match expected_cuts {
        Some(cuts) => *cuts
            .get(&source)
            .ok_or("primary acquisition original Session cut missing")?,
        None => actual.sources[&source],
    };
    if cut > actual.sources[&source] {
        return Err("primary acquisition original Session cut exceeds current source".into());
    }
    let events = events
        .into_iter()
        .filter(|event| event.sequence <= cut)
        .collect::<Vec<_>>();
    let mut seen = BTreeSet::new();
    for producer in producers {
        if !seen.insert(producer.execution.clone()) {
            continue;
        }
        let mut owner = program.clone();
        if let Some(packet) = &producer.packet {
            if producer.execution != format!("agent-run:{}", packet.run_id())
                || packet.session_id() != program.session_id
            {
                return Err("primary acquisition producer packet identity conflicts".into());
            }
            owner.turn_id = packet
                .assignment
                .execution_identity
                .turn_id()
                .ok_or("primary acquisition producer turn missing")?
                .to_string();
        } else if let Some(root) = producer.execution.strip_prefix("root-execution:") {
            owner.root_execution_id = Some(root.to_string());
        } else if producer.execution.starts_with("approval-decision:") {
            continue;
        } else {
            return Err("primary acquisition has no authenticated physical producer".into());
        }
        let deliveries = review_delivery_snapshot_from_events(
            &mut super::read_source::StoreReadSource(store),
            &owner,
            producer.packet.as_ref(),
            events.clone(),
            std::collections::BTreeMap::new(),
        )?;
        for (stream, revision) in deliveries.sources {
            if actual
                .sources
                .insert(stream, revision)
                .is_some_and(|old| old != revision)
            {
                return Err("primary acquisition source moved during recovery".into());
            }
        }
        for read in deliveries.reads {
            let seal = &read.material.acquisition;
            let crate::context_evidence::ReadExtent::ArtifactChunks { .. } = &seal.body.extent
            else {
                continue;
            };
            if goal_metadata_tool(&seal.tool_name) {
                continue;
            }
            retain_actual_material(&mut actual.material_events, &read.event);
            retain_actual_material(&mut actual.material_events, &read.receipt);
            let primary = store
                .event_by_idempotency_key(&seal.primary_stream, &seal.primary_key)
                .map_err(|error| error.to_string())?
                .ok_or("primary immutable acquisition original receipt disappeared")?;
            retain_actual_material(&mut actual.material_events, &primary);
            super::results::pin_result_source(store, &primary.stream_id, &mut actual.sources)?;
            let content = ActualImmutableInput::from_acquisition(
                store,
                program,
                &primary,
                &read.receipt,
                &events,
                producer.packet.as_ref(),
                readers,
                &mut actual,
            ).map_err(|error| {
                if error.to_string().starts_with("immutable acquisition has no authenticated root producer") {
                    super::results::ReviewEvidenceError::from(format!("{error}; caller=primary_producer producer={:?} packet_present={}",
                        producer.execution, producer.packet.is_some()))
                } else { error }
            })?;
            if !actual.immutable_inputs.contains(&content) {
                actual.immutable_inputs.push(content);
            }
        }
    }
    Ok(actual)
}

#[cfg(test)]
mod primary_material_621_tests {
    use super::*;

    #[test]
    fn r1_621_primary_identity_retains_distinct_materials_and_rejects_changed_version() {
        let first = harness_contract::context::ArtifactRef::durable(
            "artifact://summary",
            "sha256:summary",
            7,
            "text/plain",
            "session:material",
        );
        let second = harness_contract::context::ArtifactRef::durable(
            "artifact://file",
            "sha256:file",
            11,
            "text/plain",
            "session:material",
        );
        let mut contents = std::collections::BTreeMap::new();
        remember_primary_content(&mut contents, "artifact:summary", &first).unwrap();
        remember_primary_content(&mut contents, "artifact:file", &second).unwrap();
        let expected = std::collections::BTreeMap::from([
            ("artifact:summary".to_string(), first.clone()), ("artifact:file".to_string(), second.clone()),
        ]);
        assert_eq!(contents, expected);
        remember_primary_content(&mut contents, "artifact:summary", &first).unwrap();
        assert_eq!(contents, expected);
        assert!(remember_primary_content(&mut contents, "artifact:summary", &second).is_err());
        let mut changed = first.clone();
        changed.sha256 = "sha256:changed".into();
        assert!(remember_primary_content(&mut contents, "artifact:summary", &changed).is_err());
        assert_eq!(contents["artifact:summary"], first);
    }

    #[test]
    fn r1_621_primary_acquisition_requires_original_session_cut() {
        let store = std::sync::Arc::new(RuntimeEventStore::for_test());
        let mut program = AgenticProgramProjection::empty("material", "objective");
        program.session_id = "material".into();
        let readers = super::super::ResultContentReaders::default();
        assert!(primary_producer_immutable_inputs(
            &store,
            &program,
            &[],
            readers,
            Some(&std::collections::BTreeMap::new())
        )
        .is_err());
        let mut cuts = std::collections::BTreeMap::from([("session:material".into(), 1)]);
        assert!(
            primary_producer_immutable_inputs(&store, &program, &[], readers, Some(&cuts)).is_err()
        );
        cuts.insert("session:material".into(), 0);
        let actual =
            primary_producer_immutable_inputs(&store, &program, &[], readers, Some(&cuts)).unwrap();
        assert!(actual.immutable_inputs.is_empty());
        assert!(actual.required_scopes().is_empty());
    }

    #[test]
    fn r1_621_primary_acquisition_rejects_unowned_producer_without_synthetic_scopes() {
        let store = std::sync::Arc::new(RuntimeEventStore::for_test());
        let mut program = AgenticProgramProjection::empty("material", "objective");
        program.session_id = "material".into();
        let producer = super::super::results::ResultProducer {
            actor: "unknown".into(),
            execution: "unknown".into(),
            packet: None,
            tool: None,
        };
        assert!(primary_producer_immutable_inputs(
            &store,
            &program,
            &[producer],
            super::super::ResultContentReaders::default(),
            None
        )
        .is_err());
    }
}

/// What the proof does with one delegated write it found in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReviewWriteOutcome {
    /// Already settled, or provably disjoint from the reviewed work.
    Settled,
    /// The review's own control call: it is in flight because the review is
    /// performing it, and it can never settle the action it is.
    Exempt,
    /// A write that must be settled before the review may accept.
    Blocked,
}

/// The production decision for one delegated write.
///
/// The exemption is the review's OWN call and nothing else. Its identity is
/// the exact effect identity Runtime resolved from the durable evidence, so
/// a second unfinished review in the same run - a different Step, a
/// different attempt - carries a different identity and still blocks.
fn review_write_outcome(
    current_review_effect_key: Option<&str>,
    identity: &str,
    settled: bool,
    disjoint: bool,
) -> ReviewWriteOutcome {
    if settled || disjoint {
        return ReviewWriteOutcome::Settled;
    }
    if current_review_effect_key == Some(identity) {
        return ReviewWriteOutcome::Exempt;
    }
    ReviewWriteOutcome::Blocked
}

fn is_authenticated_control_effect(
    identity: Option<&str>,
    current_review_effect_key: Option<&str>,
    settled_control_keys: &BTreeSet<String>,
) -> bool {
    identity.filter(|key| !key.is_empty()).is_some_and(|key| {
        current_review_effect_key == Some(key) || settled_control_keys.contains(key)
    })
}

/// The exact effect identity carried by one proof-manifest entry.
///
/// Receipts and intents carry the identity directly. Governed plan tasks carry
/// it through their Step binding. A malformed or absent binding names no
/// authenticated control effect and therefore remains in the manifest.
fn required_effect_identity(payload: &serde_json::Value) -> Option<String> {
    // A governed plan Task's idempotency_key is a legacy plan correlation,
    // not the Runtime effect identity. The original Step is canonical. A
    // malformed claimed Step must never fall back to a control-looking key.
    if let Some(step) = payload.get("step_binding") {
        return serde_json::from_value::<crate::step_snapshot::StepCallBinding>(step.clone())
            .ok()
            .map(|step| step.effect_idempotency_key());
    }
    payload["idempotency_key"]
        .as_str()
        .filter(|identity| !identity.is_empty())
        .map(str::to_owned)
}

/// Remove only effects whose authenticated identity is a settled runtime
/// control. A business write with the same shape, a control-looking payload
/// without an authenticated identity, and every unrelated effect stay in the
/// digest.
fn strip_authenticated_runtime_control_effects(
    required_effects: &mut std::collections::BTreeMap<String, serde_json::Value>,
    runtime_control_keys: &BTreeSet<String>,
    lifecycle_keys: &std::collections::BTreeMap<String, String>,
) {
    required_effects.retain(|event_id, payload| {
        required_effect_identity(payload)
            .or_else(|| {
                (!payload
                    .as_object()
                    .is_some_and(|fields| fields.contains_key("step_binding")))
                .then(|| lifecycle_keys.get(event_id).cloned())
                .flatten()
            })
            .is_none_or(|identity| !runtime_control_keys.contains(&identity))
    });
}

/// Whether this intent is the reviewer's OWN admitted call.
///
/// Two things have to hold, and they are two different facts:
///
/// * the effect identity is the one Runtime resolved for the action being
///   applied - the same call, the same Step, the same model tool call;
/// * the intent was admitted in the reviewer's OWN receipt index stream,
///   which names the graph, the agent node and the attempt. The Step of a
///   call does not carry the attempt on the native provider path, so the
///   attempt is bound here, where the durable evidence actually records it.
fn is_review_own_admitted_call(
    current_review_effect_key: Option<&str>,
    reviewer_receipt_stream: Option<&str>,
    source: &str,
    identity: &str,
) -> bool {
    current_review_effect_key == Some(identity) && reviewer_receipt_stream == Some(source)
}

/// A distinct required criterion may be reviewed after this proof was built.
/// Authenticate its original native record before allowing the complete proof
/// reader to proceed. The current review's own call identity is unchanged.
fn applied_program_tail_preserves_inputs(
    store: &RuntimeEventStore,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    context: &super::current_review::AppliedReviewContext<'_>,
) -> Result<bool, String> {
    if super::current_review::program_cut_absorbs_own_review(
        context.program_events,
        context.program_source,
        context.pinned,
        context.through,
        context.action_id,
    ) {
        return Ok(true);
    }
    let mut authenticated = BTreeSet::new();
    if let Some(goal) = goal {
        for event in context.program_events {
            if event.payload["envelope"]["action_id"].as_str() != Some(context.action_id)
                && other_required_review_is_native(store, program, goal, proof, event)?
            {
                authenticated.insert(event.event_id.clone());
            }
        }
    }
    Ok(super::current_review::program_cut_absorbs_review_records(
        context.program_events,
        context.program_source,
        context.pinned,
        context.through,
        context.action_id,
        &authenticated,
    ))
}

fn other_required_review_is_native(
    store: &RuntimeEventStore,
    program: &AgenticProgramProjection,
    goal: &GoalContract,
    original_proof: &harness_contract::goal::ObjectiveReviewVerification,
    event: &crate::DurableRuntimeEvent,
) -> Result<bool, String> {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, ObjectiveReviewDecision,
    };
    if event.stream_id != format!("agentic-program:{}", program.program_id)
        || event.scope != RuntimeEventScope::Program
        || event.schema_version != 1
        || event.kind != "agentic.action_applied"
        || event.status.as_deref() != Some("applied")
    {
        return Ok(false);
    }
    let Ok(envelope) =
        serde_json::from_value::<AgentActionEnvelope>(event.payload["envelope"].clone())
    else {
        return Ok(false);
    };
    let AgentAction::ObjectiveReview(input) = &envelope.action else {
        return Ok(false);
    };
    let Some(original_condition) = original_proof
        .input_set
        .as_ref()
        .and_then(|set| set.condition_ref.as_deref())
    else {
        return Ok(false);
    };
    let Some(binding) = &goal.execution_binding else {
        return Ok(false);
    };
    if envelope.validate().is_err()
        || envelope.actor.program_id != program.program_id
        || envelope.actor.objective_id != program.objective_id
        || envelope.actor.session_id != program.session_id
        || envelope.actor.turn_id != program.turn_id
        || envelope.actor.root_execution_id != program.root_execution_id
        || event.actor.as_deref() != Some(envelope.actor.actor_id.as_str())
        || binding.agentic_program_id != program.program_id
        || binding.objective_id != program.objective_id
        || binding.session_id != program.session_id
        || goal.session_id != program.session_id
        || goal.id != format!("goal:{}", binding.root_execution_id)
        || binding.turn_id != program.turn_id
        || Some(&binding.root_execution_id) != program.root_execution_id.as_ref()
        || original_condition == input.criterion_ref
        || !super::required_review_conditions(goal).contains(&input.criterion_ref)
    {
        return Ok(false);
    }
    let Ok(proof) = serde_json::from_value::<harness_contract::goal::ObjectiveReviewVerification>(
        event.payload["review_verification"].clone(),
    ) else {
        return Ok(false);
    };
    let Some(inputs) = &proof.input_set else {
        return Ok(false);
    };
    if inputs.version != 2
        || inputs.condition_ref.as_deref() != Some(input.criterion_ref.as_str())
        || proof.goal_spec_revision != Some(goal.spec_revision)
        || proof.goal_spec_digest.as_deref() != Some(goal.spec_digest.as_str())
    {
        return Ok(false);
    }
    let review_id = crate::execution_core::goal::objective_review_record_id(
        &goal.id,
        &envelope.action_id,
        &input.criterion_ref,
    );
    let decision = match input.decision {
        ObjectiveReviewDecision::Satisfied => "satisfied",
        ObjectiveReviewDecision::Gap => "gap",
        ObjectiveReviewDecision::Blocked => "blocked",
    };
    if !goal.reviews.iter().any(|record| {
        record.review_id == review_id
            && record.criterion_ref == input.criterion_ref
            && record.spec_revision == goal.spec_revision
            && record.input_manifest_digest == goal.spec_digest
            && record.reviewer_actor == envelope.actor.actor_id
            && record.reviewer_execution_id == envelope.actor.execution_id
            && record.result_refs == input.result_refs
            && record.evidence_refs == input.evidence_refs
            && record.decision == decision
            && record.verification.as_ref() == Some(&proof)
    }) {
        return Ok(false);
    }
    let indexed = store
        .event_by_idempotency_key(
            &event.stream_id,
            &crate::agentic::action_service::action_key(&envelope.action_id),
        )
        .map_err(|error| error.to_string())?;
    let Some(primary) = indexed else {
        return Ok(false);
    };
    Ok(
        serde_json::to_value(primary).map_err(|error| error.to_string())?
            == serde_json::to_value(event).map_err(|error| error.to_string())?,
    )
}

/// The invocation id of the ORIGINAL applied action that produced a proof.
///
/// The durable action record carries the envelope it applied and the proof it
/// accepted, so the action and the proof are one record. Exactly one applied
/// action may carry a proof: two would make the review's own call ambiguous,
/// and the proof must not pick one.
fn original_review_action_id(
    events: &[crate::DurableRuntimeEvent],
    proof: &harness_contract::goal::ObjectiveReviewVerification,
) -> Result<Option<String>, String> {
    let proof_value = serde_json::to_value(proof).map_err(|error| error.to_string())?;
    let mut found: Option<String> = None;
    for event in events {
        if event.kind != "agentic.action_applied"
            || event.status.as_deref() != Some("applied")
            || event.payload["review_verification"] != proof_value
        {
            continue;
        }
        let envelope: harness_contract::agent_action::AgentActionEnvelope =
            serde_json::from_value(event.payload["envelope"].clone())
                .map_err(|_| "original review action envelope missing")?;
        if event.actor.as_deref() != Some(envelope.actor.actor_id.as_str()) {
            return Err("original review action owner mismatch".into());
        }
        if found.is_some() {
            return Err("original review action is ambiguous".into());
        }
        found = Some(envelope.action_id);
    }
    Ok(found)
}

pub(super) fn actual_input_snapshot(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    boundaries: &[super::results::ResultInputBoundary],
    readers: super::ResultContentReaders<'_>,
    expected_cuts: Option<&std::collections::BTreeMap<String, u64>>,
) -> Result<ActualInputSnapshot, super::results::ReviewEvidenceError> {
    actual_input_snapshot_for_phase(store, program, boundaries, readers, expected_cuts, false, None)
}

pub(super) fn actual_input_snapshot_for_phase(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    boundaries: &[super::results::ResultInputBoundary],
    readers: super::ResultContentReaders<'_>,
    expected_cuts: Option<&std::collections::BTreeMap<String, u64>>,
    allow_history_growth: bool,
    session_tail: Option<&HistoricalSessionCut>,
) -> Result<ActualInputSnapshot, super::results::ReviewEvidenceError> {
    let mut actual = ActualInputSnapshot {
        file_dependencies: Vec::new(),
        immutable_inputs: Vec::new(),
        sources: std::collections::BTreeMap::new(),
        material_events: Vec::new(),
        canonical_facts: Vec::new(),
    };
    for run in super::results::process_runs_for_boundaries(store, boundaries, &mut actual.sources)?
    {
        super::results::validate_process_archives(&run, readers)?;
        for material in super::results::retain_process_execution(store, &run, &mut actual.sources)?
        {
            if !actual.material_events.contains(&material) {
                actual.material_events.push(material);
            }
        }
        actual
            .canonical_facts
            .push(serde_json::json!({"grade":"bounded_process_history","execution":run}));
    }
    let session = format!("session:{}", program.session_id);
    let session_events = match session_tail {
        Some(tail) if tail.source.stream_id == session =>
            super::results::result_source_events_at_session_cut(store, &session, &mut actual.sources,
                tail.source.expected_revision, tail.observed_revision)?,
        Some(_) => return Err("original Session material tail belongs to another source".into()),
        None => super::results::result_source_events(store, &session, &mut actual.sources)?,
    };
    let mut seen_boundaries = BTreeSet::new();
    let mut windows = std::collections::BTreeMap::new();
    for boundary in boundaries {
        if !seen_boundaries.insert((
            boundary.producer.clone(),
            boundary.call_key.clone(),
            boundary.cursor,
        )) {
            continue;
        }
        let consumer = verify_material_event(store, &boundary.source, &mut actual.sources)?;
        authenticate_actual_consumer(boundary, &consumer, &session_events)?;
        retain_actual_material(&mut actual.material_events, &consumer);
        if !consumer.payload["root_effect_owner"].is_null() {
            for original in
                super::results::root_owner_materials(store, &consumer, &mut actual.sources)?
            {
                retain_actual_material(&mut actual.material_events, &original);
            }
        }
        if consumer.payload["effect"]["effect_kind"] == "process" {
            continue;
        }
        if let Some(packet) = &boundary.packet {
            if packet.session_id() != program.session_id
                || boundary.producer != format!("agent-run:{}", packet.run_id())
            {
                return Err("actual input producer is not the original immutable packet".into());
            }
            let source = format!(
                "execution-agent-receipts:{}:{}:{}",
                packet.graph_id(),
                packet.node_id(),
                packet.attempt
            );
            let events = super::results::result_source_events(store, &source, &mut actual.sources)?;
            for event in events {
                if event.kind != "execution.agent_tool.receipt"
                    || event.payload["effect_kind"] != "read"
                    || event.commit_cursor >= boundary.cursor
                {
                    continue;
                }
                if event.payload["outcome"]["status"] != "executed" {
                    if event.payload["outcome"]["tool_name"] == "read_file" {
                                    return Err(crate::agentic::fact_admission::refusal(
                                        harness_contract::refusal_codes::FactAdmissionCode::Unproven,
                                        "proof_actual_input_extent_unsupported: failed File read is not an existence proof",
                                        &[crate::agentic::fact_admission::StrategyGap {
                                            strategy: harness_contract::refusal_codes::FactAdmissionStrategy::Nested,
                                            missing: "the File read failed, so it proves no existence".to_string(),
                                        }],
                                    )
                                    .into());
                    }
                    continue;
                }
                let name = event.payload["outcome"]["tool_name"]
                    .as_str()
                    .unwrap_or_default();
                if goal_metadata_tool(name) {
                    continue;
                }
                let key = event.payload["idempotency_key"]
                    .as_str()
                    .ok_or("actual read has no original identity")?;
                let primary_source = format!("execution-effect:{key}");
                super::results::pin_result_source(store, &primary_source, &mut actual.sources)?;
                let primary = store
                    .event_by_idempotency_key(&primary_source, &format!("{key}:read-receipt"))
                    .map_err(|error| error.to_string())?
                    .ok_or("actual input original primary disappeared")?;
                // 033 S8 (one owner for the seal decision): the effect
                // receipt (the primary) owns the acquisition - it carries the
                // body and the outcome. The observation twin must agree with
                // it. A pair that disagrees is refused by name instead of
                // being routed silently into the other gate, where the read
                // had no home: sealed on one side, unsealed on the other.
                let primary_sealed =
                    !primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null();
                let twin_sealed =
                    !event.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null();
                if primary_sealed != twin_sealed {
                    return Err(crate::agentic::fact_admission::refusal(
                        harness_contract::refusal_codes::FactAdmissionCode::PairDisagrees,
                        "proof_actual_input_extent_unsupported: the read pair disagrees about its acquisition seal",
                        &[],
                    )
                    .into());
                }
                let original_input = if primary_sealed {
                    super::results::authenticate_read_input(
                        store,
                        &event,
                        packet,
                        &mut actual.sources,
                    )?;
                    // A sealed read is judged by its acquisition; its target
                    // identity is not consulted, so no original input is
                    // needed here.
                    None
                } else {
                    // G-c: a delegated member's read carries its original
                    // input in the authenticated child request, and that is
                    // where the same target-identity predicate reads it from.
                    // The program Session stream holds no governed plan task
                    // for a member's read, so looking there attributed every
                    // member read to the declared capability face - which is
                    // what refused a read of a durable record.
                    Some(authenticate_unsealed_delegated_input(
                        store,
                        &primary,
                        &event,
                        packet,
                        &mut actual,
                    )?)
                };
                collect_actual_acquisition(
                    store,
                    program,
                    &primary,
                    &event,
                    original_input.as_ref(),
                    &session_events,
                    boundary,
                    readers,
                    &mut actual,
                    &mut windows,
                )?;
            }
        } else if let Some(root) = boundary.producer.strip_prefix("root-execution:") {
            for event in &session_events {
                if event.kind != "tool.invocation.completed"
                    || event.commit_cursor >= boundary.cursor
                    || !event.activity_binding().is_some_and(|binding| {
                        binding.agent_run_id.is_none()
                            && binding.root_execution_id == root
                            && binding.session_id == program.session_id
                            && super::binding_is_owned_by_root(&binding, Some(root))
                    })
                {
                    continue;
                }
                let effect = super::results::tool_effect_from_store(store, event)?
                    .ok_or("actual root input lacks its original registered descriptor")?;
                if effect.effect_kind != ToolEffectKind::Read
                    || goal_metadata_tool(event.payload["tool_name"].as_str().unwrap_or_default())
                {
                    continue;
                }
                if event.payload["is_error"] == true || event.payload["status"] != "completed" {
                                return Err(crate::agentic::fact_admission::refusal(
                                    harness_contract::refusal_codes::FactAdmissionCode::Unproven,
                                    "proof_actual_input_extent_unsupported: failed root read has no input extent",
                                    &[crate::agentic::fact_admission::StrategyGap {
                                        strategy: harness_contract::refusal_codes::FactAdmissionStrategy::Nested,
                                        missing: "the root read failed, so it carries no input extent".to_string(),
                                    }],
                                )
                                .into());
                }
                let task = super::results::result_tool_plan(&session_events, event)?
                    .ok_or("actual root input original plan missing")?;
                let primary =
                    super::results::original_tool_primary(store, event, task, &mut actual.sources)?;
                let value = &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD];
                let index = if !value.is_null() {
                    let seal = crate::context_evidence::decode_read_acquisition(value)
                        .map_err(str::to_string)?;
                    super::results::pin_result_source_ticketed(
                        store,
                        &super::results::node_review_ref(&seal.graph_id, &seal.node_id),
                        seal.attempt,
                        &seal.index_stream,
                        &mut actual.sources,
                    )
                    .map_err(super::results::ReviewEvidenceError::from_repin)?;
                    store
                        .event_by_idempotency_key(&seal.index_stream, &seal.index_key)
                        .map_err(|error| error.to_string())?
                        .ok_or("actual root input index missing")?
                } else {
                    let (owner, attempt, _) = super::results::original_root_effect_owner(&primary)?;
                    let node = owner
                        .node_id
                        .as_deref()
                        .ok_or("actual root input original node missing")?;
                    let source = format!(
                        "execution-agent-receipts:{}:{node}:{attempt}",
                        owner.root_execution_id
                    );
                    super::results::pin_result_source_ticketed(
                        store,
                        &super::results::node_review_ref(&owner.root_execution_id, node),
                        attempt,
                        &source,
                        &mut actual.sources,
                    )
                    .map_err(super::results::ReviewEvidenceError::from_repin)?;
                    let key = primary.payload["idempotency_key"]
                        .as_str()
                        .ok_or("actual root read identity missing")?;
                    store
                        .event_by_idempotency_key(&source, &format!("agent-tool-receipt:{key}"))
                        .map_err(|error| error.to_string())?
                        .ok_or("actual root immutable input original index missing")?
                };
                if primary.payload["replay"]["parent_execution"].is_null() {
                    let key = primary.payload["idempotency_key"]
                        .as_str()
                        .ok_or("actual root read identity missing")?;
                    let intent = store
                        .event_by_idempotency_key(&primary.stream_id, &format!("{key}:read-intent"))
                        .map_err(|error| error.to_string())?
                        .ok_or("actual root original read intent missing")?;
                    for original in
                        super::results::root_owner_materials(store, &intent, &mut actual.sources)?
                    {
                        retain_actual_material(&mut actual.material_events, &original);
                    }
                }
                retain_actual_material(&mut actual.material_events, event);
                collect_actual_acquisition(
                    store,
                    program,
                    &primary,
                    &index,
                    Some(&task["normalized_input"]),
                    &session_events,
                    boundary,
                    readers,
                    &mut actual,
                    &mut windows,
                )?;
            }
        } else {
            return Err("actual input consumer has no authenticated producer identity".into());
        }
    }
    let mut latest = std::collections::BTreeMap::<String, ActualFileDependency>::new();
    for (_, source_windows) in windows {
        let dependency = complete_actual_file_windows(source_windows)?;
        let identity = super::proof_value_digest(&serde_json::json!([
            dependency.target.path,
            dependency.consumer_call_key,
            dependency.consumer_cursor
        ]));
        let entry = latest.entry(identity).or_insert_with(|| dependency.clone());
        if dependency.acquisition_cursor > entry.acquisition_cursor {
            *entry = dependency;
        }
    }
    actual.file_dependencies = latest.into_values().collect();
    actual
        .file_dependencies
        .sort_by_key(super::proof_value_digest);
    actual
        .immutable_inputs
        .sort_by_key(super::proof_value_digest);
    actual
        .material_events
        .sort_by_key(super::proof_value_digest);
    actual
        .canonical_facts
        .sort_by_key(super::proof_value_digest);
    for (source, revision) in &actual.sources {
        if expected_cuts.is_some_and(|cuts| {
            !actual_input_cut_matches(cuts.get(source).copied(), *revision, allow_history_growth)
        }) {
            return Err(format!(
                "actual input source cut is missing or changed; rebuild the complete review: source={source:?}, expected={:?}, observed={revision}, allow_history_growth={allow_history_growth}",
                expected_cuts.and_then(|cuts| cuts.get(source).copied()),
            ).into());
        }
        let current = store
            .stream_revision(source)
            .map_err(|error| error.to_string())?;
        if current != *revision {
            return Err(format!(
                "actual input source cut is missing or changed; rebuild the complete review: source={source:?}, observed={revision}, current={current}",
            ).into());
        }
    }
    Ok(actual)
}

pub(super) fn validate_current_process_inputs(
    workspace: &std::path::Path,
    run: &super::results::AuthenticatedProcessExecution,
) -> Result<Vec<std::fs::File>, String> {
    #[cfg(target_os = "linux")]
    {
        use std::io::Read;
        use std::os::unix::fs::MetadataExt;
        let root = std::fs::File::from(
            rustix::fs::openat2(
                rustix::fs::CWD,
                workspace,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
                rustix::fs::ResolveFlags::NO_SYMLINKS | rustix::fs::ResolveFlags::NO_MAGICLINKS,
            )
            .map_err(|error| error.to_string())?,
        );
        let root_before = root.metadata().map_err(|error| error.to_string())?;
        let named_root = workspace
            .symlink_metadata()
            .map_err(|error| error.to_string())?;
        if !root_before.is_dir()
            || named_root.file_type().is_symlink()
            || root_before.dev() != named_root.dev()
            || root_before.ino() != named_root.ino()
        {
            return Err("bounded process current workspace identity changed".into());
        }
        let open = |path: &str| -> Result<std::fs::File, String> {
            rustix::fs::openat2(
                &root,
                path,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::CLOEXEC
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK,
                rustix::fs::Mode::empty(),
                rustix::fs::ResolveFlags::BENEATH
                    | rustix::fs::ResolveFlags::NO_SYMLINKS
                    | rustix::fs::ResolveFlags::NO_MAGICLINKS,
            )
            .map(std::fs::File::from)
            .map_err(|error| format!("bounded process current input unavailable: {error}"))
        };
        let mut descriptors = Vec::with_capacity(run.fact.admission.inputs.len() + 1);
        for input in &run.fact.admission.inputs {
            let path = std::path::Path::new(&input.relative_path);
            if path.is_absolute()
                || path
                    .components()
                    .any(|part| !matches!(part, std::path::Component::Normal(_)))
            {
                return Err("bounded process current input is not an exact relative File".into());
            }
            let mut source = open(&input.relative_path)?;
            let before = source.metadata().map_err(|error| error.to_string())?;
            let mtime_ns = before
                .mtime()
                .checked_mul(1_000_000_000)
                .and_then(|value| value.checked_add(before.mtime_nsec()));
            let ctime_ns = before
                .ctime()
                .checked_mul(1_000_000_000)
                .and_then(|value| value.checked_add(before.ctime_nsec()));
            if !before.is_file()
                || before.nlink() != 1
                || before.len() != input.bytes
                || before.len() > 2097152
                || before.dev() != input.device
                || before.ino() != input.inode
                || mtime_ns != Some(input.mtime_ns)
                || ctime_ns != Some(input.ctime_ns)
            {
                return Err("bounded process selected input has changed extent or alias".into());
            }
            let mut bytes = Vec::with_capacity(before.len() as usize);
            source
                .by_ref()
                .take(2097153)
                .read_to_end(&mut bytes)
                .map_err(|error| error.to_string())?;
            let after = source.metadata().map_err(|error| error.to_string())?;
            let current = open(&input.relative_path)?
                .metadata()
                .map_err(|error| error.to_string())?;
            let identity = |metadata: &std::fs::Metadata| {
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.nlink(),
                    metadata.len(),
                    metadata.mtime(),
                    metadata.mtime_nsec(),
                    metadata.ctime(),
                    metadata.ctime_nsec(),
                )
            };
            if identity(&before) != identity(&after)
                || identity(&after) != identity(&current)
                || bytes.len() as u64 != input.bytes
                || super::results::process_sha256(&bytes) != input.sha256
            {
                return Err(
                    "bounded process selected witness is stale for the current full input set"
                        .into(),
                );
            }
            descriptors.push(source);
        }
        let root_after = workspace
            .symlink_metadata()
            .map_err(|error| error.to_string())?;
        if root_after.file_type().is_symlink()
            || root_after.dev() != root_before.dev()
            || root_after.ino() != root_before.ino()
        {
            return Err(
                "bounded process current workspace changed during complete input verification"
                    .into(),
            );
        }
        descriptors.push(root);
        Ok(descriptors)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (workspace, run);
        Err("bounded process current File verification requires Linux bounded FD resolution".into())
    }
}

fn authenticated_process_for_key(
    store: &std::sync::Arc<RuntimeEventStore>,
    key: &str,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Option<super::results::AuthenticatedProcessExecution>, super::results::ReviewEvidenceError> {
    let source = format!("execution-effect:{key}");
    let Some(intent) = store
        .event_by_idempotency_key(&source, &format!("{key}:intent"))
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    if intent.payload["effect"]["effect_kind"] != "process" {
        return Ok(None);
    }
    let primary = store
        .event_by_idempotency_key(&source, &format!("{key}:receipt"))
        .map_err(|error| error.to_string())?
        .ok_or("effect_review_pending: process original terminal missing")?;
    let run = super::results::authenticate_bounded_process(store, &primary)?;
    super::results::process_has_no_external_effects(&run)?;
    super::results::retain_process_execution(store, &run, sources)?;
    Ok(Some(run))
}

fn admitted_intent_scope(
    store: &RuntimeEventStore,
    event: &crate::DurableRuntimeEvent,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<harness_contract::goal::ProofEffectScope, super::results::ReviewEvidenceError> {
    let key = event.payload["idempotency_key"]
        .as_str()
        .ok_or("effect intent lacks original identity")?;
    let source = format!("execution-effect:{key}");
    super::results::pin_result_source(store, &source, sources)?;
    let intent = store
        .event_by_idempotency_key(&source, &format!("{key}:intent"))
        .map_err(|error| error.to_string())?
        .ok_or("effect original admitted intent is missing")?;
    let descriptor: ToolEffectDescriptor = serde_json::from_value(intent.payload["effect"].clone())
        .map_err(|_| "effect intent has no original typed descriptor")?;
    if intent.kind != "execution.effect.intent"
        || intent.actor.as_deref() != Some("governed_tool")
        || intent.schema_version != 1
        || event.schema_version != 1
        || event.kind != "execution.agent_tool.intent"
        || event.actor != intent.actor
        || event.scope != intent.scope
        || event.transaction_id != intent.transaction_id
        || event.commit_cursor != intent.commit_cursor
        || event.payload["root_effect_owner"] != intent.payload["root_effect_owner"]
        || event.payload["root_effect_context"] != intent.payload["root_effect_context"]
        || intent.payload["effect"] != intent.payload["replay"]["invocation"]["effect"]
        || intent.payload["resolved_effect_scope"] != event.payload["resolved_effect_scope"]
        || event.payload["descriptor_hash"] != descriptor.descriptor_hash
        || event.payload["effect_scopes"]
            != serde_json::to_value(&descriptor.scopes).map_err(|error| error.to_string())?
    {
        return Err(
            "effect intent/index scope or classifier disagrees with its original admission".into(),
        );
    }
    if intent.payload["replay"]["parent_execution"].is_null() {
        super::results::authenticate_original_root_owner(store, &intent, sources)?;
    }
    super::results::require_original_effect_scope(
        &intent.payload["resolved_effect_scope"],
        &descriptor,
    )
    .map_err(Into::into)
}

pub(super) fn authenticate_settled_effect(
    store: &std::sync::Arc<RuntimeEventStore>,
    intent: &crate::DurableRuntimeEvent,
    receipt: &crate::DurableRuntimeEvent,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<ToolEffectDescriptor, super::results::ReviewEvidenceError> {
    let key = intent.payload["idempotency_key"]
        .as_str()
        .ok_or("effect identity missing")?;
    let source = format!("execution-effect:{key}");
    super::results::pin_result_source(store, &source, sources)?;
    let primary = store
        .event_by_idempotency_key(&source, &format!("{key}:receipt"))
        .map_err(|error| error.to_string())?
        .ok_or("settled effect original receipt missing")?;
    let admitted = store
        .event_by_idempotency_key(&source, &format!("{key}:intent"))
        .map_err(|error| error.to_string())?
        .ok_or("settled effect original intent missing")?;
    let descriptor: ToolEffectDescriptor =
        serde_json::from_value(admitted.payload["effect"].clone())
            .map_err(|_| "settled effect original descriptor missing")?;
    super::results::require_original_effect_scope(
        &primary.payload["resolved_effect_scope"],
        &descriptor,
    )?;
    if primary.schema_version != 1
        || crate::ExecutionCommitService::receipt_observation_sequence(&receipt).is_none()
        || primary.kind != "execution.effect.receipt"
        || receipt.kind != "execution.agent_tool.receipt"
        || primary.actor.as_deref() != Some("governed_tool")
        || primary.actor != receipt.actor
        || primary.scope != receipt.scope
        || primary.transaction_id != receipt.transaction_id
        || primary.commit_cursor != receipt.commit_cursor
        || primary.payload["replay"] != admitted.payload["replay"]
        || primary.payload["replay"]["invocation"]["effect"] != admitted.payload["effect"]
        || primary.payload["resolved_effect_scope"] != admitted.payload["resolved_effect_scope"]
        || primary.payload["resolved_effect_scope"] != receipt.payload["resolved_effect_scope"]
        || primary.payload["outcome"] != receipt.payload["outcome"]
        || primary.payload["root_effect_owner"] != admitted.payload["root_effect_owner"]
        || primary.payload["root_effect_owner"] != receipt.payload["root_effect_owner"]
        || primary.payload["root_effect_context"] != admitted.payload["root_effect_context"]
        || primary.payload["root_effect_context"] != receipt.payload["root_effect_context"]
    {
        return Err("settled effect original scope/receipt/index conflict".into());
    }
    if primary.payload["replay"]["parent_execution"].is_null() {
        super::results::authenticate_root_receipt_owner(store, &primary, sources)?;
    }
    if descriptor.effect_kind == ToolEffectKind::Process
        || super::results::has_process_fact(&primary)
    {
        let run = super::results::authenticate_bounded_process(store, &primary)?;
        super::results::process_has_no_external_effects(&run)?;
        super::results::retain_process_execution(store, &run, sources)?;
    }
    Ok(descriptor)
}

/// Whether the original input of a read addresses the workspace.
///
/// The governed read that produced a receipt names what it read, and the
/// product already classifies model-facing references
/// (`harness_contract::refs::Address`): `file://` and `dir://` address the
/// workspace, while `tool://`, `artifact://`, `fact://` and the other record
/// schemes address durable records that carry no workspace extent. A legacy
/// bare identifier is resolved by the directory and may name a workspace
/// path, so it stays on the workspace side. `Ok(None)` means the input names
/// no reference at all - the read was not a reference read.
fn original_read_target(input: &serde_json::Value) -> Result<Option<bool>, String> {
    use harness_contract::refs::Address;
    let Some(reference) = input.get("reference").and_then(|value| value.as_str()) else {
        return Ok(None);
    };
    let address = Address::parse(reference)
        .map_err(|error| format!("original read reference is not parseable: {error}"))?;
    Ok(Some(matches!(
        address,
        Address::File(_) | Address::Dir(_) | Address::Bare(_)
    )))
}

fn immutable_receipt_without_workspace_extent(
    primary: &crate::DurableRuntimeEvent,
    descriptor: &ToolEffectDescriptor,
    original_input: Option<&serde_json::Value>,
) -> Result<bool, String> {
    if !primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null()
        || descriptor.effect_kind != ToolEffectKind::Read
        || descriptor.uses_network
        || descriptor.spawns_process
        || descriptor.mutates_packages
        || descriptor.mutates_system
    {
        return Ok(false);
    }
    // What the read actually hit, not what the tool declared: a read whose
    // original input names a workspace path always needs an acquisition
    // seal, while a read that named a durable record carries no workspace
    // extent even though the unified read facade declares a File capability.
    // When the original input cannot be resolved the declared capability face
    // still decides, and that fallback is the stricter legacy judgment.
    let declared_workspace_face = descriptor
        .scopes
        .iter()
        .any(|scope| scope.resource == harness_contract::policy::PermissionResource::File);
    let read_a_workspace_path = match original_input {
        Some(input) => match original_read_target(input)? {
            Some(target) => target,
            None => declared_workspace_face,
        },
        None => declared_workspace_face,
    };
    if read_a_workspace_path {
        return Ok(false);
    }
    if primary.payload["outcome"]["status"] != "executed"
        || primary.payload["outcome"]["error"].as_str().is_some()
        || primary.payload["outcome_truncated"] == true
        || primary.payload["outcome"]["output"].as_str().is_none()
    {
        return Err("actual immutable receipt lacks its complete original acquired body".into());
    }
    if !primary.payload["resolved_effect_scope"].is_null() {
        super::results::require_original_effect_scope(
            &primary.payload["resolved_effect_scope"],
            descriptor,
        )?;
    }
    Ok(true)
}

pub(super) fn original_material_scope(
    store: &std::sync::Arc<RuntimeEventStore>,
    event: &crate::DurableRuntimeEvent,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Option<harness_contract::goal::ProofEffectScope>, super::results::ReviewEvidenceError> {
    if event.kind == "tool.invocation.completed" {
        let session = event.stream_id.clone();
        let events = super::results::result_source_events(store, &session, sources)?;
        let task = super::results::result_tool_plan(&events, event)?
            .ok_or("physical material lacks its original governed plan")?;
        let primary = super::results::original_tool_primary(store, event, task, sources)?;
        let descriptor = super::results::receipt_effect_descriptor(&primary)?;
        if immutable_receipt_without_workspace_extent(
            &primary,
            &descriptor,
            Some(&task["normalized_input"]),
        )? {
            return Ok(None);
        }
        if !primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null() {
            let seal = crate::context_evidence::decode_read_acquisition(
                &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
            )
            .map_err(str::to_string)?;
            return Ok(read_original_scope(&primary, &descriptor, &seal)?);
        }
        if primary.payload["resolved_effect_scope"].is_null()
            && descriptor.effect_kind == ToolEffectKind::Read
            && !descriptor
                .scopes
                .iter()
                .any(|scope| scope.resource == harness_contract::policy::PermissionResource::File)
            && !descriptor.spawns_process
            && !descriptor.uses_network
            && !descriptor.mutates_packages
            && !descriptor.mutates_system
        {
            return Ok(None);
        }
        return super::results::require_original_effect_scope(
            &primary.payload["resolved_effect_scope"],
            &descriptor,
        )
        .map(Some)
        .map_err(Into::into);
    }
    let key = event.payload["idempotency_key"]
        .as_str()
        .ok_or("physical material lacks its original receipt identity")?;
    let source = format!("execution-effect:{key}");
    super::results::pin_result_source(store, &source, sources)?;
    let primary = store
        .event_by_idempotency_key(&source, &format!("{key}:read-receipt"))
        .map_err(|error| error.to_string())?
        .ok_or("physical material original acquisition missing")?;
    if primary.actor != event.actor
        || primary.scope != event.scope
        || primary.transaction_id != event.transaction_id
        || primary.commit_cursor != event.commit_cursor
        || primary.payload["outcome"] != event.payload["outcome"]
        || primary.payload["resolved_effect_scope"] != event.payload["resolved_effect_scope"]
    {
        return Err("physical material primary/index disagree".into());
    }
    let descriptor = super::results::receipt_effect_descriptor(&primary)?;
    // This branch reaches the primary through the agent receipt stream and
    // holds no plan task, so the original input cannot be resolved here: the
    // declared capability face decides, which is the stricter judgment.
    if immutable_receipt_without_workspace_extent(&primary, &descriptor, None)? {
        return Ok(None);
    }
    if primary.payload["resolved_effect_scope"].is_null() {
        let seal = crate::context_evidence::decode_read_acquisition(
            &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
        )
        .map_err(str::to_string)?;
        return Ok(read_original_scope(&primary, &descriptor, &seal)?);
    }
    super::results::require_original_effect_scope(
        &primary.payload["resolved_effect_scope"],
        &descriptor,
    )
    .map(Some)
    .map_err(Into::into)
}

fn require_discovered_graph_genesis(
    store: &RuntimeEventStore,
    graph_id: &str,
    discovery_cursor: u64,
) -> Result<(), String> {
    let genesis = store.list_stream_after(graph_id, 0, 1, 1, 4 * 1024 * 1024)?;
    if genesis.first().is_none_or(|event| event.commit_cursor > discovery_cursor) {
        return Err("proof physical lineage changed during discovery; retry current inputs".into());
    }
    Ok(())
}

/// Build a proof snapshot as a RECORD: no live action, so no exemption is
/// derived and a stored proof can never grant one.
pub(crate) fn proof_input_snapshot(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
) -> Result<ProofInputSnapshot, String> {
    proof_input_snapshot_for_phase(store, program, goal, proof, None, readers, None, None)
        .map_err(|error| error.to_string())
}

/// Build a proof snapshot against the LIVE action being applied.
pub(crate) fn proof_input_snapshot_for_action(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
    current_review: Option<super::current_review::ReviewActionIdentity<'_>>,
) -> Result<ProofInputSnapshot, super::results::ReviewEvidenceError> {
    proof_input_snapshot_for_phase(store, program, goal, proof, None, readers, current_review, None)
}

/// What one receipt's outcome says about the required write it settles.
///
/// The set is closed: the two statuses the existing settled rule owns, the
/// one this review settles as a terminal failure, and nothing else. A status
/// outside the set is named, never guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutcomeClass {
    /// The write succeeded, or was blocked before it ran.
    Succeeded,
    /// The write ran and failed.
    Failed,
    /// There is no receipt yet: the write is still in flight.
    InFlight,
}

fn terminal_outcome_class(status: Option<&str>) -> Result<OutcomeClass, String> {
    match status {
        Some("executed" | "blocked_permission") => Ok(OutcomeClass::Succeeded),
        Some("failed") => Ok(OutcomeClass::Failed),
        Some(status) => Err(format!("outcome status {status} is not one this review can read")),
        None => Ok(OutcomeClass::InFlight),
    }
}

/// Whether one call carries the durable fact of its own refusal.
///
/// The twin is written by the writer that refused the call, on that call's
/// own effect stream, in one transaction with the program event it names.
/// It is read the way the consumer reads it: the twin names the program
/// event, the two share one transaction and one commit cursor, and the
/// receipt names both.
///
/// A single call's twin is addressed by the call's own effect key. A batch
/// member's twin lives on the batch's outer effect stream and is addressed
/// by that outer key, because the member's own key names no member index
/// there.
fn refused_call_is_settled(
    store: &std::sync::Arc<RuntimeEventStore>,
    key: &str,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<TerminalFactRead, super::results::ReviewEvidenceError> {
    let mut candidates = vec![(key.to_string(), format!("agentic-single:{key}"))];
    if let Some((outer, index)) = member_outer_and_index(key) {
        candidates.push((outer, format!("agentic-member:{index}")));
    }
    for (stream_key, idempotency_key) in candidates {
        let stream = format!("execution-effect:{}", stream_key);
        super::results::pin_result_source(store, &stream, sources)
            .map_err(|error| error.with_prefix("refusal twin source read failed: "))?;
        // A read that fails is not a miss: it is named, and the caller keeps
        // the call pending. Only a stream that holds no twin is a miss.
        let Some(twin) = store
            .event_by_idempotency_key(&stream, &idempotency_key)
            .map_err(|error| format!("refusal twin read failed: {error}"))?
        else {
            continue;
        };
        if twin.kind != "agentic.batch_member.index"
            || twin.scope != RuntimeEventScope::ExecutionNode
            || twin.actor.as_deref() != Some("runtime.agentic_action")
            || twin.status.as_deref() != Some("committed")
            || twin.schema_version != 1
            || twin.payload["outer_key"] != stream_key
            || twin.payload["program_event_kind"] != "agentic.completion_rejected"
        {
            return Ok(TerminalFactRead::Unprovable(format!(
                "refusal twin on {stream} does not have the refusal shape"
            )));
        }
        // A member's twin is read through its batch's outer key, so the
        // member it stands for has to be checked here: the member ordinal in
        // this intent's own key, and the member identity the twin names.
        if let Some((_, index)) = member_outer_and_index(key) {
            // The member index is read, not guessed: a value this review
            // cannot read is named, and never read as "another member".
            let member_index = match twin.payload["member_index"].as_u64() {
                Some(value) => value.to_string(),
                None => {
                    return Ok(TerminalFactRead::Unprovable(format!(
                        "refusal twin on {stream} names no member index"
                    )));
                }
            };
            if member_index != index
                || twin.payload["member_identity"]
                    .as_str()
                    .filter(|identity| !identity.is_empty())
                    .is_none()
            {
                return Ok(TerminalFactRead::Unprovable(format!(
                    "refusal twin on {stream} stands for another member than this intent"
                )));
            }
        }
        let Some(program_stream) = twin.payload["program_stream"].as_str() else {
            return Ok(TerminalFactRead::Unprovable(
                "refusal twin names no program event stream".into(),
            ));
        };
        let Some(program_key) = twin.payload["program_event_key"].as_str() else {
            return Ok(TerminalFactRead::Unprovable(
                "refusal twin names no program event key".into(),
            ));
        };
        let Some(program) = store
            .event_by_idempotency_key(program_stream, program_key)
            .map_err(|error| format!("refusal program event read failed: {error}"))?
        else {
            return Ok(TerminalFactRead::Unprovable(
                "refusal twin names a program event that is not there".into(),
            ));
        };
        if program.kind != "agentic.completion_rejected"
            || program.scope != RuntimeEventScope::Program
            || program.status.as_deref() != Some("rejected")
            || program.schema_version != 1
            || program.transaction_id != twin.transaction_id
            || program.commit_cursor != twin.commit_cursor
        {
            return Ok(TerminalFactRead::Unprovable(
                "refusal twin and its program event are not one transaction".into(),
            ));
        }
        let Some(receipt) = store
            .original_transaction_receipt(&twin.transaction_id)
            .map_err(|error| format!("refusal transaction receipt read failed: {error}"))?
        else {
            return Ok(TerminalFactRead::Unprovable(
                "refusal twin has no transaction receipt".into(),
            ));
        };
        if !receipt.event_ids.contains(&twin.event_id)
            || !receipt.event_ids.contains(&program.event_id)
        {
            return Ok(TerminalFactRead::Unprovable(
                "refusal transaction receipt does not name both events".into(),
            ));
        }
        return Ok(TerminalFactRead::Found {
            cursor: twin.commit_cursor,
        });
    }
    Ok(TerminalFactRead::NotFound)
}

/// What one candidate family says about a call's terminal fact.
///
/// `NotFound` is "this family holds nothing here"; `Unprovable` is "this
/// family holds something that cannot be read as the fact". The two are
/// never the same: only `Found` may settle a call, and `Unprovable` keeps
/// the call pending under a named reason.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TerminalFactRead {
    NotFound,
    Unprovable(String),
    Found { cursor: u64 },
}

/// A batch member's own key names its batch's outer key and its member
/// index: `{outer}:{index}`. The index is the trailing number, so the
/// outer key is everything before the last colon.
fn member_outer_and_index(key: &str) -> Option<(String, String)> {
    let (outer, index) = key.rsplit_once(':')?;
    if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((outer.to_string(), index.to_string()))
}

/// How one required write's own durable facts answer whether it is settled.
///
/// Two families can answer it, and they answer different questions: a
/// refusal twin says the call was refused before it took effect, a terminal
/// receipt says the call ran and failed. When both are present for the same
/// call, the facts contradict each other, and the contradiction is named -
/// neither fact is used to settle the call, and neither is silently
/// preferred.
///
/// This is the decision the review makes for one required write, lifted out
/// of the collection so that it can be read - and witnessed - on its own.
enum RequiredWriteSettlement {
    /// The call was refused before it took effect: its refusal twin is the
    /// terminal fact, and the review settles the call on it.
    Refused { cursor: u64 },
    /// The call ran and failed. The named failure carries it: never a
    /// satisfied requirement, and never an effect that did not happen.
    Failed { failure: serde_json::Value },
    /// The call is still pending: nothing terminal settles it.
    Pending,
    /// The refusal fact cannot be read as this call's fact.
    UnreadableRefusal { reason: String },
    /// The two terminal facts contradict each other for the same call.
    Contradiction {
        refusal_cursor: Option<u64>,
        receipt_cursor: Option<u64>,
    },
    /// A terminal failure has no receipt to name.
    MissingReceipt,
}

/// Decide one required write's fate from its own durable facts.
///
/// The decision set is exactly the one the review has always made: a
/// terminal receipt settles a call that ran, a refusal twin settles a call
/// that was refused before it took effect, a call that ran and failed is
/// carried as its own named failure, and a call with no terminal fact stays
/// pending. Nothing here widens what settles a write: a call whose refusal
/// fact cannot be read stays pending under a name, and a refusal fact and a
/// terminal receipt for the same call contradict each other.
fn required_write_settlement(
    store: &std::sync::Arc<RuntimeEventStore>,
    identity: &str,
    intent_payload: &serde_json::Value,
    receipt: Option<(&serde_json::Value, u64)>,
    terminally_failed: bool,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<RequiredWriteSettlement, super::results::ReviewEvidenceError> {
    let candidate = required_write_settlement_candidate(
        store, identity, intent_payload, receipt, terminally_failed, sources,
    )?;
    use crate::agentic::fact_admission::StrategyOutcome;
    use harness_contract::refusal_codes::{FactAdmissionCode, FactAdmissionStrategy};
    let outcome = match &candidate {
        RequiredWriteSettlement::Refused { .. } => StrategyOutcome::admitted(FactAdmissionStrategy::SettledRefusal),
        RequiredWriteSettlement::Pending => StrategyOutcome::missing(FactAdmissionStrategy::SettledRefusal, "no original refusal twin"),
        RequiredWriteSettlement::Failed { .. } => StrategyOutcome::missing(FactAdmissionStrategy::SettledRefusal, "the original call failed"),
        RequiredWriteSettlement::UnreadableRefusal { .. } => StrategyOutcome::missing(FactAdmissionStrategy::SettledRefusal, "the refusal twin is unreadable"),
        RequiredWriteSettlement::Contradiction { .. } => StrategyOutcome::missing(FactAdmissionStrategy::SettledRefusal, "terminal facts contradict"),
        RequiredWriteSettlement::MissingReceipt => StrategyOutcome::missing(FactAdmissionStrategy::SettledRefusal, "the failed call has no receipt"),
    };
    let decision = crate::agentic::fact_admission::admit(
        &[outcome], FactAdmissionCode::Unproven, "original required write refusal is unproven",
    );
    match (&candidate, decision.code) {
        (RequiredWriteSettlement::Refused { .. }, FactAdmissionCode::Admitted) => Ok(candidate),
        (RequiredWriteSettlement::Refused { .. }, _) => Err(decision.refusal_message().into()),
        (_, FactAdmissionCode::Admitted) => Err("non-refusal write was incorrectly admitted as a refusal".into()),
        // Missing refusal is not an error by itself: preserve the original
        // pending, failed, unreadable and contradictory settlement states.
        _ => Ok(candidate),
    }
}

fn required_write_settlement_candidate(
    store: &std::sync::Arc<RuntimeEventStore>,
    identity: &str,
    intent_payload: &serde_json::Value,
    receipt: Option<(&serde_json::Value, u64)>,
    terminally_failed: bool,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<RequiredWriteSettlement, super::results::ReviewEvidenceError> {
    let refusal_cursor = match refused_call_is_settled(store, identity, sources)? {
        TerminalFactRead::Found { cursor } => Some(cursor),
        // Nothing here: this family does not speak for the call. The call
        // stays pending, as it was.
        TerminalFactRead::NotFound => None,
        // Something here that cannot be read as the fact: the call stays
        // pending under a named reason, and it is never treated as if it
        // needed no handling.
        TerminalFactRead::Unprovable(reason) => {
            return Ok(RequiredWriteSettlement::UnreadableRefusal { reason });
        }
    };
    if refusal_cursor.is_some() && terminally_failed {
        return Ok(RequiredWriteSettlement::Contradiction {
            refusal_cursor,
            receipt_cursor: receipt.map(|(_, cursor)| cursor),
        });
    }
    if terminally_failed {
        // The conclusion is this required write's own named failure: it ran
        // and it failed. It is recorded as such - never as a satisfied
        // requirement, and never as an effect that did not happen.
        let Some((receipt_payload, receipt_cursor)) = receipt else {
            return Ok(RequiredWriteSettlement::MissingReceipt);
        };
        return Ok(RequiredWriteSettlement::Failed {
            failure: serde_json::json!({
                "grade": "required_write_terminally_failed",
                "effect_key": identity,
                "tool_use_id": intent_payload["tool_use_id"],
                "tool_name": intent_payload["tool_name"],
                "failure_class": "terminal_failure",
                "error": receipt_payload["outcome"]["error"],
                "receipt_cursor": receipt_cursor,
            }),
        });
    }
    // E-s: a call that was admitted and then refused leaves a durable twin
    // on its own effect stream, written in one transaction with the program
    // event that names it. That twin is the call's terminal fact, so the
    // review settles the call as refused instead of waiting for a receipt
    // that will never come. Only the call that carries its own twin is
    // settled: no other admitted write is.
    if let Some(cursor) = refusal_cursor {
        return Ok(RequiredWriteSettlement::Refused { cursor });
    }
    Ok(RequiredWriteSettlement::Pending)
}

fn proof_input_snapshot_for_phase(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    accepted_policy_digest: Option<&str>,
    readers: super::ResultContentReaders<'_>,
    current_review: Option<super::current_review::ReviewActionIdentity<'_>>,
    pre_tail: Option<&HistoricalSessionCut>,
) -> Result<ProofInputSnapshot, super::results::ReviewEvidenceError> {
    // The named failures this review settles: each required write that ran
    // and failed, with the facts that name it. They are carried to the
    // review's conclusion; they are never a satisfied requirement and never
    // an effect that did not happen.
    let mut terminal_failures: Vec<serde_json::Value> = Vec::new();
    let inputs = proof.input_set.as_ref().ok_or("missing v2 proof inputs")?;
    if inputs.version != 2 {
        return Err("unsupported ProofInputSet version".into());
    }
    if proof.independence_required
        && proof
            .producer_execution_ids
            .iter()
            .any(|producer| producer == &proof.reviewer_execution_id)
    {
        return Err("proof primary influence includes its independent reviewer".into());
    }
    if proof.goal_spec_revision != goal.map(|goal| goal.spec_revision)
        || proof.goal_spec_digest.as_deref() != goal.map(|goal| goal.spec_digest.as_str())
    {
        return Err("proof Goal spec changed".into());
    }
    let dependencies = super::canonical_review_result_refs(program, &inputs.result_refs)?
        .into_iter()
        .chain(inputs.auxiliary_refs.iter().cloned())
        .collect::<Vec<_>>();
    let mut sources = std::collections::BTreeMap::new();
    let (tasks, artifacts, own_work) =
        super::selected_work(store, program, goal, &dependencies, &mut sources)?;
    let mut required_tasks = tasks
        .iter()
        .map(|task| (program.program_id.clone(), task.clone()))
        .collect::<BTreeSet<_>>();
    let mut tasks = tasks.into_iter().collect::<BTreeSet<_>>();
    let mut artifacts = artifacts.into_iter().collect::<BTreeSet<_>>();
    let obligations = goal
        .into_iter()
        .flat_map(|goal| &goal.obligations)
        .filter(|obligation| obligation.required)
        .map(|obligation| obligation.obligation_id.clone())
        .collect::<Vec<_>>();
    if obligations != inputs.required_obligation_refs {
        return Err("proof required obligations changed".into());
    }
    let program_source = format!("agentic-program:{}", program.program_id);
    if inputs.semantic_digest.is_empty() {
        for (source, expected) in &inputs.source_revisions {
            if store
                .stream_revision(source)
                .map_err(|error| error.to_string())?
                != *expected
            {
                return Err("actual read material enumeration cut changed before first snapshot; rebuild the whole review".into());
            }
            if sources
                .insert(source.clone(), *expected)
                .is_some_and(|previous| previous != *expected)
            {
                return Err(
                    "publication source differs from the original proof discovery cut".into(),
                );
            }
        }
    }
    let program_revision = store
        .stream_revision(&program_source)
        .map_err(|error| error.to_string())?;
    if program_revision != program.revision {
        return Err("proof Program source changed; retry current inputs".into());
    }
    if let Some(super::current_review::ReviewActionIdentity::Applied(context)) = current_review {
        let pinned = inputs
            .source_revisions
            .get(&program_source)
            .copied()
            .ok_or("applied proof has no original Program cut")?;
        if !applied_program_tail_preserves_inputs(store, program, goal, proof, context)?
            || context.program_source != program_source
            || context.pinned != pinned
            || context.through != program_revision
        {
            return Err(format!(
                "proof Program source changed; retry current inputs; {}",
                super::describe_source_advance(store, &program_source, pinned)
            ).into());
        }
    }
    sources.insert(program_source, program_revision);
    let mut related_work =
        std::collections::BTreeMap::from([(program.program_id.clone(), own_work)]);
    let mut allowed_programs = BTreeSet::from([program.program_id.clone()]);
    let mut related_roots = BTreeSet::new();
    let mut continuation = program.continuation.clone();
    while let Some(binding) = continuation {
        if !allowed_programs.insert(binding.source_program_id.clone()) {
            return Err("proof has cyclic Program provenance".into());
        }
        let origin = crate::AgentActionService::new(std::sync::Arc::clone(store))
            .project(&binding.source_program_id)
            .map_err(|error| error.to_string())?;
        if origin.session_id != program.session_id
            || origin.objective_id != binding.source_objective_id
            || origin.root_execution_id.as_deref() != Some(binding.source_root_id.as_str())
        {
            return Err("proof Program provenance crosses its authenticated binding".into());
        }
        let source = format!("agentic-program:{}", origin.program_id);
        if store
            .stream_revision(&source)
            .map_err(|error| error.to_string())?
            != origin.revision
        {
            return Err("proof origin Program changed during resolution".into());
        }
        sources.insert(source, origin.revision);
        let (origin_tasks, origin_artifacts, digest) =
            super::selected_work(store, &origin, None, &dependencies, &mut sources)?;
        required_tasks.extend(
            origin_tasks
                .iter()
                .map(|task| (origin.program_id.clone(), task.clone())),
        );
        tasks.extend(origin_tasks);
        artifacts.extend(origin_artifacts);
        related_work.insert(origin.program_id, digest);
        related_roots.insert(binding.source_root_id);
        continuation = origin.continuation;
    }
    let task_refs = tasks.into_iter().collect::<Vec<_>>();
    let artifact_refs = artifacts.into_iter().collect::<Vec<_>>();
    if !inputs.semantic_digest.is_empty()
        && (task_refs != inputs.task_refs || artifact_refs != inputs.artifact_refs)
    {
        return Err("proof required result dependencies changed".into());
    }
    let work_digest = super::proof_value_digest(&related_work);
    if let Some(goal) = goal {
        let goal_source = format!("goal:{}", goal.id);
        sources.insert(
            goal_source.clone(),
            store
                .stream_revision(&goal_source)
                .map_err(|error| error.to_string())?,
        );
    }
    let session = format!("session:{}", program.session_id);
    related_roots.extend(program.root_execution_id.iter().cloned());
    let mut start_cursor = program
        .root_execution_id
        .as_deref()
        .map(|root| store.list_stream_after(root, 0, 1, 1, 4 * 1024 * 1024))
        .transpose()?
        .and_then(|events| events.first().map(|event| event.commit_cursor))
        .unwrap_or_default();
    for root in &related_roots {
        let cursor = store
            .list_stream_after(root, 0, 1, 1, 4 * 1024 * 1024)?
            .first()
            .ok_or("proof origin root genesis missing")?
            .commit_cursor;
        start_cursor = start_cursor.min(cursor);
    }
    let session_events = pinned_stream_events_with_cut(
        store,
        &session,
        &mut sources,
        start_cursor,
        PinnedEventProjection::EffectsAndPlans,
        pre_tail,
    )?;
    let strategy = session_events.iter().find(|event| {
        event.kind.starts_with("runtime.strategy.")
            && event.actor.as_deref() == Some("conversation_runtime.strategy_owner")
            && event.payload["execution_graph_ref"].as_str() == program.root_execution_id.as_deref()
            && event.payload["session_ref"].as_str() == Some(program.session_id.as_str())
            && event.payload["turn_ref"].as_str() == Some(program.turn_id.as_str())
    }).map(|event| serde_json::json!({
        "risk":event.payload["risk"],"modifiers":event.payload["modifiers"],"gates":event.payload["gates"],
    }));
    let requirements = goal
        .into_iter()
        .flat_map(|goal| &goal.obligations)
        .map(|obligation| {
            serde_json::json!({"id":obligation.obligation_id,"required":obligation.required,
            "predicate":obligation.success_predicate,"producer":obligation.producer,
            "evidence":obligation.evidence_requirement})
        })
        .collect::<Vec<_>>();
    let policy_digest = accepted_policy_digest.map(str::to_string).unwrap_or_else(|| super::proof_value_digest(&serde_json::json!({
        "domain":"cowd.acceptance.policy.v2","strategy":strategy,"requirements":requirements,
        "independence_required":proof.independence_required,
    })));
    if accepted_policy_digest.is_none() && !proof.independence_required {
        goal.map(|goal| root_self_review_policy(store, program, goal))
            .transpose()?
            .flatten()
            .ok_or("current acceptance policy requires independent review")?;
    }
    let graphs = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store));
    let discovery_cursor = store
        .durable_commit_cursor()
        .map_err(|error| error.to_string())?;
    let mut pending = program
        .root_execution_id
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    pending.extend(inputs.packet_digests.keys().cloned());
    pending.extend(related_roots.iter().cloned());
    pending.extend(program.tasks.values().flat_map(|task| {
        task.active_attempts
            .values()
            .map(|attempt| attempt.execution_id.as_str())
            .chain(task.claim_execution_id.as_deref())
            .map(|execution| {
                crate::execution_core::graph::graph_id_for_lookup(execution).to_string()
            })
    }));
    let mut visited = BTreeSet::new();
    let candidates = program
        .tasks
        .values()
        .filter(|task| {
            task.status == crate::agentic::program::AgenticTaskStatus::CancelRequested
                && task.pending_retirement
                    == Some(crate::agentic::program::AgenticTaskRetirement::Withdrawn)
                && task
                    .cancel_requested_by
                    .as_ref()
                    .is_some_and(|actor| !actor.is_empty())
                && task
                    .cancel_reason_ref
                    .as_ref()
                    .is_some_and(|reason| !reason.is_empty())
                && !task.cancel_evidence_refs.is_empty()
                && task.claim_generation > 0
                && !task.active_attempts.is_empty()
                && !required_tasks.contains(&(program.program_id.clone(), task.task_id.clone()))
        })
        .map(|task| task.task_id.clone())
        .collect::<BTreeSet<_>>();
    let mut candidate_graphs = std::collections::BTreeMap::<String, BTreeSet<String>>::new();
    let mut candidate_sources = std::collections::BTreeMap::<String, BTreeSet<String>>::new();
    let mut incomplete_candidates = BTreeSet::new();
    let mut positive_effect_candidates = BTreeSet::new();
    let mut graph_tasks = std::collections::BTreeMap::<String, BTreeSet<String>>::new();
    let mut graph_children = std::collections::BTreeMap::<String, Vec<String>>::new();
    let mut graph_parents = std::collections::BTreeMap::new();
    let mut expected_parents = std::collections::BTreeMap::new();
    let mut incomplete_graphs = BTreeSet::new();
    let mut reviewer_execution_refs = BTreeSet::new();
    let mut reviewer_delivery_packet = None;
    let mut bound_tool_graphs = Vec::new();
    let mut delegated_session_packets = std::collections::BTreeMap::new();
    let mut physical_facts = std::collections::BTreeMap::new();
    let mut physical_unknown = BTreeSet::new();
    if let Some(root) = program.root_execution_id.as_ref() {
        let identity = format!("root-execution:{root}");
        if proof.reviewer_execution_id == identity {
            reviewer_execution_refs.insert(identity);
            reviewer_execution_refs.insert(root.clone());
        }
    }
    let mut receipt_sources = proof
        .effect_source_refs
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut required_sources = receipt_sources.clone();
    while let Some(graph_id) = pending.pop() {
        if !visited.insert(graph_id.clone()) {
            continue;
        }
        require_discovered_graph_genesis(store, &graph_id, discovery_cursor)?;
        let graph = graphs.load(&graph_id).map_err(|error| error.to_string())?;
        if graph
            .lineage
            .as_ref()
            .is_none_or(|lineage| lineage.session_id != program.session_id)
        {
            return Err("proof graph crosses its Session".into());
        }
        let revision = store
            .stream_revision(&graph_id)
            .map_err(|error| error.to_string())?;
        if revision != graph.revision {
            return Err("proof physical packet source changed".into());
        }
        if sources
            .insert(graph_id.clone(), revision)
            .is_some_and(|previous| previous != revision)
        {
            return Err(
                "proof physical graph changed after material enumeration; rebuild the whole review"
                    .into(),
            );
        }
        graph_parents.insert(graph_id.clone(), graph.parent_execution.clone());
        let lineage = format!("execution-lineage:{graph_id}");
        sources.insert(
            lineage.clone(),
            store
                .stream_revision(&lineage)
                .map_err(|error| error.to_string())?,
        );
        let children = graphs
            .child_links(&graph_id)
            .map_err(|error| error.to_string())?;
        if children.iter().any(|link| {
            !graph
                .nodes
                .iter()
                .any(|node| node.id == link.parent_node_id)
        }) {
            return Err("proof child lineage has no authenticated parent node".into());
        }
        for link in &children {
            let binding = harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: graph_id.clone(),
                node_id: link.parent_node_id.clone(),
            };
            if link.child_execution_id == graph_id
                || expected_parents
                    .insert(link.child_execution_id.clone(), binding.clone())
                    .is_some_and(|previous| previous != binding)
            {
                return Err("proof child lineage has ambiguous parent ownership".into());
            }
        }
        let children = children
            .into_iter()
            .map(|link| link.child_execution_id)
            .collect::<Vec<_>>();
        pending.extend(children.iter().cloned());
        graph_children.insert(graph_id.clone(), children);
        if related_roots.contains(&graph_id) {
            for (node, attempts) in &graph.recovery_cursor.node_attempts {
                if !graph.nodes.iter().any(|spec| &spec.id == node) {
                    return Err("root effect index has no original graph node".into());
                }
                for attempt in 1..=*attempts {
                    let source = format!("execution-agent-receipts:{graph_id}:{node}:{attempt}");
                    receipt_sources.insert(source.clone());
                    required_sources.insert(source);
                }
            }
        }
        if crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(&graph) {
            bound_tool_graphs.push(graph);
            continue;
        }
        for node in &graph.nodes {
            if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                incomplete_graphs.insert(graph_id.clone());
                continue;
            }
            let packet: harness_contract::agent::AgentTaskPacket =
                serde_json::from_str(&node.payload_ref).map_err(|error| error.to_string())?;
            if packet.binding.as_ref().is_none_or(|binding| {
                matches!(
                    binding.executor,
                    harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }
                )
            }) {
                let identity = format!(
                    "{}:{}:{}:{}",
                    graph_id,
                    node.id,
                    packet.run_id(),
                    packet.attempt
                );
                let lifecycle = crate::agent_runtime::authenticated_run_lifecycle(store, &packet)?;
                for source in &lifecycle.sources {
                    if sources
                        .insert(source.stream_id.clone(), source.expected_revision)
                        .is_some_and(|previous| previous != source.expected_revision)
                    {
                        return Err(
                            "proof physical lifecycle source changed during enumeration".into()
                        );
                    }
                }
                if packet.graph_id() != graph_id
                    || packet.node_id() != node.id
                    || packet
                        .agentic_binding
                        .as_ref()
                        .is_none_or(|binding| !allowed_programs.contains(&binding.program_id))
                    || !lifecycle.no_direct_effects()
                {
                    physical_unknown.insert(identity.clone());
                }
                if graph
                    .recovery_cursor
                    .node_attempts
                    .get(&node.id)
                    .copied()
                    .unwrap_or(packet.attempt)
                    > packet.attempt
                {
                    physical_unknown
                        .insert(format!("{identity}:missing-original-historical-packet"));
                }
                physical_facts.insert(identity, lifecycle.semantic_fact());
            }
            if packet
                .agentic_binding
                .as_ref()
                .is_none_or(|scope| !allowed_programs.contains(&scope.program_id))
            {
                if inputs.packet_digests.contains_key(&graph_id) {
                    return Err("proof selected material packet is outside its authenticated Program lineage".into());
                }
                incomplete_graphs.insert(graph_id.clone());
                continue;
            }
            if packet.graph_id() != graph_id || packet.node_id() != node.id {
                incomplete_graphs.insert(graph_id.clone());
                if inputs.packet_digests.contains_key(&graph_id) {
                    return Err("proof selected physical packet identity invalid".into());
                }
                for attempt in 1..=packet.attempt.max(1).max(
                    graph
                        .recovery_cursor
                        .node_attempts
                        .get(&node.id)
                        .copied()
                        .unwrap_or_default(),
                ) {
                    let source =
                        format!("execution-agent-receipts:{graph_id}:{}:{attempt}", node.id);
                    if store
                        .stream_revision(&source)
                        .map_err(|error| error.to_string())?
                        != 0
                    {
                        return Err("proof invalid physical packet has an effect history".into());
                    }
                    sources.insert(source, 0);
                }
                continue;
            }
            if inputs
                .packet_digests
                .get(&graph_id)
                .is_some_and(|digest| *digest != super::proof_value_digest(&packet))
            {
                return Err("proof immutable packet changed".into());
            }
            for origin in [graph_id.clone(), packet.run_id().to_string()] {
                if delegated_session_packets.insert(origin, packet.clone())
                    .is_some_and(|previous| previous != packet)
                {
                    return Err("delegated plan has ambiguous original physical owner".into());
                }
            }
            let reviewer_packet = inputs.packet_digests.contains_key(&graph_id)
                && proof.reviewer_execution_id == format!("agent-run:{}", packet.run_id());
            if reviewer_packet {
                if !reviewer_execution_refs.is_empty() {
                    return Err("proof reviewer physical identity is ambiguous".into());
                }
                reviewer_execution_refs.insert(packet.graph_id().to_string());
                reviewer_execution_refs.insert(packet.run_id().to_string());
                reviewer_delivery_packet = Some(packet.clone());
            }
            let binding = packet
                .agentic_binding
                .as_ref()
                .ok_or("proof packet binding missing")?;
            let task_identity = match &binding.focus {
                harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
                | harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => {
                    Some((binding.program_id.clone(), task_ref.clone()))
                }
                harness_contract::agent::AgenticExecutionFocus::Coordination { wake_ref }
                    if binding.program_id == program.program_id =>
                {
                    program
                        .coordination_wake_ref(wake_ref)
                        .and_then(|(_, wake)| wake.intent.as_ref())
                        .map(|intent| (binding.program_id.clone(), intent.task_ref.clone()))
                }
                _ => None,
            };
            let required_packet = inputs.packet_digests.contains_key(&graph_id)
                || task_identity
                    .as_ref()
                    .is_some_and(|identity| required_tasks.contains(identity));
            let candidate = task_identity
                .as_ref()
                .filter(|(owner, task)| owner == &program.program_id && candidates.contains(task))
                .map(|(_, task)| task.clone());
            if let Some(task_ref) = &candidate {
                if required_packet {
                    incomplete_candidates.insert(task_ref.clone());
                }
                graph_tasks
                    .entry(graph_id.clone())
                    .or_default()
                    .insert(task_ref.clone());
                candidate_graphs
                    .entry(task_ref.clone())
                    .or_default()
                    .insert(graph_id.clone());
                let task = &program.tasks[task_ref];
                for attempt in task.active_attempts.values().filter(|attempt| {
                    crate::execution_core::graph::graph_id_for_lookup(&attempt.execution_id)
                        == graph_id
                }) {
                    let expected_mode = match &binding.focus {
                        harness_contract::agent::AgenticExecutionFocus::TaskExecute { .. } => {
                            harness_contract::agent_action::AgentAttemptMode::Execute
                        }
                        harness_contract::agent::AgenticExecutionFocus::TaskReview { .. } => {
                            harness_contract::agent_action::AgentAttemptMode::Review
                        }
                        _ => harness_contract::agent_action::AgentAttemptMode::Coordination,
                    };
                    if attempt.agent_id != binding.agent_id
                        || attempt.membership_id != binding.membership_id
                        || attempt.mode != expected_mode
                        || attempt.generation.saturating_add(1) != u64::from(packet.attempt)
                        || (attempt.mode
                            == harness_contract::agent_action::AgentAttemptMode::Execute
                            && task.claim_generation <= attempt.generation)
                        || binding.task_team_id != task.team_id
                    {
                        incomplete_candidates.insert(task_ref.clone());
                    }
                }
            } else {
                incomplete_graphs.insert(graph_id.clone());
            }
            for attempt in 1..=packet.attempt.max(1).max(
                graph
                    .recovery_cursor
                    .node_attempts
                    .get(&node.id)
                    .copied()
                    .unwrap_or_default(),
            ) {
                let source = format!("execution-agent-receipts:{graph_id}:{}:{attempt}", node.id);
                receipt_sources.insert(source.clone());
                if let Some(task_ref) = &candidate {
                    candidate_sources
                        .entry(source.clone())
                        .or_default()
                        .insert(task_ref.clone());
                }
                if required_packet {
                    required_sources.insert(source);
                }
            }
        }
    }
    if inputs
        .packet_digests
        .keys()
        .any(|graph| !visited.contains(graph))
    {
        return Err("proof physical packet missing".into());
    }
    if reviewer_execution_refs.is_empty() {
        return Err("proof reviewer has no authenticated physical identity".into());
    }
    for (child, parent) in expected_parents {
        if graph_parents.get(&child).and_then(Option::as_ref) != Some(&parent) {
            return Err("proof child lineage disagrees with its immutable parent binding".into());
        }
    }
    for child in bound_tool_graphs {
        let child_id = child.id.clone();
        incomplete_graphs.insert(child_id.clone());
        let Some(parent) = child.parent_execution.as_ref() else {
            continue;
        };
        let Some(owners) = graph_tasks
            .get(&parent.execution_id)
            .cloned()
            .filter(|owners| owners.len() == 1)
        else {
            continue;
        };
        let parent_graph = graphs
            .load(&parent.execution_id)
            .map_err(|error| error.to_string())?;
        if sources.get(&parent_graph.id) != Some(&parent_graph.revision)
            || child.lineage != parent_graph.lineage
            || child.service_class != parent_graph.service_class
        {
            continue;
        }
        let Some(parent_node) = parent_graph.nodes.iter().find(|node| {
            node.id == parent.node_id
                && node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
        }) else {
            continue;
        };
        let packet: harness_contract::agent::AgentTaskPacket =
            serde_json::from_str(&parent_node.payload_ref).map_err(|error| error.to_string())?;
        let bound =
            crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_graph(
                &child,
            )
            .map_err(|error| error.to_string())?;
        let request = bound.request;
        let Some(binding) = packet.binding.as_ref() else {
            continue;
        };
        if bound.agent_id != packet.agent_id()
            || bound.run_id != packet.run_id()
            || request.parent_execution.as_ref() != Some(parent)
            || request.parent_execution_attempt != Some(packet.attempt)
            || request.session_id.as_deref() != Some(packet.session_id())
            || request.governed_plan_id != parent.execution_id
            || packet.graph_id() != parent.execution_id
            || packet.node_id() != parent.node_id
            || !packet.allowed_tools.contains(&request.tool_name)
            || !binding.tool_contract_refs.contains(&request.tool_name)
            || !crate::agent::binding::recompute_binding_digest(binding)
                .is_ok_and(|digest| digest == binding.binding_digest)
            || request.idempotency_key.is_empty()
            || request.observation_wave_sequence == 0
        {
            continue;
        }
        let index_source = format!(
            "execution-agent-receipts:{}:{}:{}",
            parent.execution_id, parent.node_id, packet.attempt
        );
        let primary_source = format!("execution-effect:{}", request.idempotency_key);
        for source in [&index_source, &primary_source] {
            let revision = store
                .stream_revision(source)
                .map_err(|error| error.to_string())?;
            if sources
                .insert(source.clone(), revision)
                .is_some_and(|previous| previous != revision)
            {
                return Err("proof bound child source changed during validation".into());
            }
        }
        let intent = store
            .event_by_idempotency_key(
                &primary_source,
                &format!("{}:intent", request.idempotency_key),
            )
            .map_err(|error| error.to_string())?;
        let (primary, index) = if let Some(intent) = intent {
            (
                Some(intent),
                store
                    .event_by_idempotency_key(
                        &index_source,
                        &format!("agent-tool-intent:{}", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?,
            )
        } else {
            (
                store
                    .event_by_idempotency_key(
                        &primary_source,
                        &format!("{}:read-receipt", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?,
                store
                    .event_by_idempotency_key(
                        &index_source,
                        &format!("agent-tool-receipt:{}", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?,
            )
        };
        let (Some(primary), Some(index)) = (primary, index) else {
            continue;
        };
        if crate::execution_core::graph::ExecutionCommitService::validate_proof_bound_tool_request(
            &request, &primary, &index,
        )
        .is_err()
        {
            continue;
        }
        let Some(authorization) = request.authorization.as_ref() else {
            continue;
        };
        let lease = &authorization.authorization_lease;
        if lease.lease_id.is_empty()
            || lease.signature.is_empty()
            || lease.policy_revision != request.policy_revision
            || lease.effect_descriptor_hash != authorization.descriptor_hash
            || lease.status != harness_contract::policy::AuthorizationLeaseStatus::Active
            || lease.remaining_uses != 1
            || !packet.permission_ceiling.permits(lease.ceiling)
        {
            continue;
        }
        let grant_source = format!("authorization-lease:{}", lease.lease_id);
        let grant_revision = store
            .stream_revision(&grant_source)
            .map_err(|error| error.to_string())?;
        if sources
            .insert(grant_source.clone(), grant_revision)
            .is_some_and(|previous| previous != grant_revision)
        {
            return Err("proof original tool grant changed during validation".into());
        }
        let mut after = 0;
        let mut admitted_grant = false;
        while after < grant_revision {
            let events = store.list_stream_after(
                &grant_source,
                after,
                grant_revision,
                64,
                4 * 1024 * 1024,
            )?;
            if events.is_empty() {
                break;
            }
            for event in events {
                after = event.sequence;
                if event.kind != "authorization.lease_transition" {
                    continue;
                }
                let Ok(transition) = serde_json::from_value::<
                    harness_contract::policy::AuthorizationLeaseTransition,
                >(event.payload) else {
                    continue;
                };
                if !matches!(
                    transition.kind,
                    harness_contract::policy::AuthorizationLeaseTransitionKind::Issued
                        | harness_contract::policy::AuthorizationLeaseTransitionKind::Consumed
                ) {
                    continue;
                }
                let mut original = transition.lease;
                original.idempotency_key.clone_from(&lease.idempotency_key);
                original.remaining_uses = lease.remaining_uses;
                original.status = lease.status;
                if &original == lease {
                    admitted_grant = true;
                }
            }
        }
        if !admitted_grant {
            continue;
        }
        graph_tasks.insert(child_id.clone(), owners);
        incomplete_graphs.remove(&child_id);
    }
    for task_ref in &candidates {
        let Some(owned_graphs) = candidate_graphs.get(task_ref) else {
            incomplete_candidates.insert(task_ref.clone());
            continue;
        };
        let task = &program.tasks[task_ref];
        if task.active_attempts.values().any(|attempt| {
            !owned_graphs.contains(crate::execution_core::graph::graph_id_for_lookup(
                &attempt.execution_id,
            ))
        }) || task.claim_execution_id.as_ref().is_some_and(|execution| {
            !owned_graphs.contains(crate::execution_core::graph::graph_id_for_lookup(execution))
        }) {
            incomplete_candidates.insert(task_ref.clone());
        }
        let mut descendants = owned_graphs.iter().cloned().collect::<Vec<_>>();
        let mut checked = BTreeSet::new();
        while let Some(graph) = descendants.pop() {
            if !checked.insert(graph.clone()) {
                continue;
            }
            if incomplete_graphs.contains(&graph)
                || graph_tasks
                    .get(&graph)
                    .is_none_or(|owners| owners.len() != 1 || !owners.contains(task_ref))
            {
                incomplete_candidates.insert(task_ref.clone());
            }
            if let Some(children) = graph_children.get(&graph) {
                descendants.extend(children.iter().cloned());
            }
        }
    }
    let mut actual_boundaries = Vec::new();
    let mut business_outputs = Vec::new();
    // The applied action records are read ONCE, from the Program source this
    // proof already pins. The review's own call is identified from the
    // ORIGINAL action that produced this proof, and the Process body evidence
    // is checked against the same record.
    let program_actions = if proof.current_review_effect_key.is_some()
        || proof.reads.iter().any(|read| {
            read.effect_observation_refs
                .iter()
                .any(|reference| super::is_process_body_receipt(reference))
        }) {
        match current_review {
            Some(super::current_review::ReviewActionIdentity::Applied(context)) => {
                Some(context.program_events.to_vec())
            }
            _ => Some(super::results::result_source_events(
                store,
                &format!("agentic-program:{}", program.program_id),
                &mut sources,
            )?),
        }
    } else {
        None
    };
    if let Some(program_actions) = program_actions.as_deref() {
        let proof_value = serde_json::to_value(proof).map_err(|error| error.to_string())?;
        for event in program_actions {
            if event.kind != "agentic.action_applied"
                || event.status.as_deref() != Some("applied")
                || event.payload["review_verification"] != proof_value
            {
                continue;
            }
            let envelope: harness_contract::agent_action::AgentActionEnvelope =
                serde_json::from_value(event.payload["envelope"].clone())
                    .map_err(|_| "bounded process original review action missing")?;
            if event.actor.as_deref() != Some(envelope.actor.actor_id.as_str()) {
                return Err("bounded process original review action owner mismatch".into());
            }
            let explicit = match &envelope.action {
                harness_contract::agent_action::AgentAction::TaskReview(input) => {
                    &input.evidence_refs
                }
                harness_contract::agent_action::AgentAction::ObjectiveReview(input) => {
                    if inputs.condition_ref.as_deref() != Some(input.criterion_ref.as_str()) {
                        return Err("bounded process original criterion selection mismatch".into());
                    }
                    &input.evidence_refs
                }
                _ => return Err("bounded process selection came from a non-review action".into()),
            };
            super::validate_explicit_process_evidence(store, proof, explicit)?;
        }
    }
    if reviewer_delivery_packet.is_none()
        && !program
            .root_execution_id
            .as_ref()
            .is_some_and(|root| proof.reviewer_execution_id == format!("root-execution:{root}"))
    {
        return Err("proof has no authenticated read-delivery recipient".into());
    }
    // The review is a control action the reviewer performs while this proof is
    // assembled, so its own call is still in flight and the checks below would
    // trip on the review itself. Runtime resolves that ONE call from the
    // evidence this proof already pinned - the Session's governed tool plans -
    // and authenticates it against the TRUSTED reviewer packet the proof just
    // verified, the actor and the live action. No live action means no
    // exemption, and a record can never grant one.
    let reviewer_receipt_stream = reviewer_delivery_packet
        .as_ref()
        .map(super::current_review::reviewer_receipt_stream);
    // The live action resolves the call it is performing; a stored proof
    // carries the identity it was built under as a CANDIDATE, and this entry
    // re-binds that candidate against the same durable evidence. When both
    // are present they have to agree, so a proof can never exempt a call the
    // live action did not perform.
    let live_identity = match (current_review, reviewer_delivery_packet.as_ref()) {
        (Some(super::current_review::ReviewActionIdentity::Live(context)), Some(packet)) => {
            super::current_review::resolve_current_review_effect_key(
                &session_events,
                packet,
                context,
            )?
        }
        _ => None,
    };
    let rebound_identity = match reviewer_delivery_packet.as_ref() {
        Some(packet) => {
            let original = match program_actions.as_deref() {
                Some(events) => original_review_action_id(events, proof)?,
                None => None,
            };
            #[cfg(test)]
            obs1120::record(
                "identity.original",
                serde_json::json!({"original_action_id":original,
                "carried":proof.current_review_effect_key}),
            );
            super::current_review::rebind_current_review_effect_key(
                &session_events,
                packet,
                proof.current_review_effect_key.as_deref(),
                original.as_deref(),
            )?
        }
        None => None,
    };
    // A live action is the authority: the call it performs is the only call
    // that may be exempt, and a carried candidate has to be exactly that
    // call. A live action that authenticates to NOTHING exempts nothing -
    // falling back to the stored candidate would exempt a call this action
    // is not performing.
    #[cfg(test)]
    obs1120::record(
        "identity.candidates",
        serde_json::json!({"live":live_identity,"rebound":rebound_identity,
        "packet":reviewer_delivery_packet.as_ref().map(|packet| serde_json::json!({
            "digest":super::proof_value_digest(packet),"graph":packet.graph_id(),"node":packet.node_id(),
            "attempt":packet.attempt,"scope":packet.agentic_binding.as_ref().map(|binding| &binding.focus)}))}),
    );
    let current_review_effect_key = super::current_review::select_current_review_effect_key(
        current_review,
        live_identity,
        rebound_identity,
    )?;
    #[cfg(test)]
    obs1120::record(
        "identity.selected",
        serde_json::json!({"selected":current_review_effect_key}),
    );
    // Graph lifecycle is not business work, but an unrelated graph advance
    // cannot borrow the review's exemption. Keep the current revision in the
    // original CAS even when this exact retirement has no semantic effect.
    let mut absorbed_graph_sources = std::collections::BTreeMap::new();
    // Graph continuations can be planned after the review's material cut.
    // Keep those original plan records separate from proof material; only the
    // graph owner authenticates a plan selected by a continuation node.
    let mut graph_plan_events = None;
    for (source, pinned) in &inputs.source_revisions {
        if !visited.contains(source) {
            continue;
        }
        let current = *sources
            .get(source)
            .ok_or("proof graph source is not pinned")?;
        if current == *pinned {
            continue;
        }
        if reviewer_delivery_packet
            .as_ref()
            .is_none_or(|packet| packet.graph_id() != source)
        {
            if graph_plan_events.is_none() {
                let mut plans = session_events.clone();
                if let Some(cut) = pre_tail {
                    let later = crate::execution_core::goal::assessment::read_bounded_event_tail(
                        store, &cut.source.stream_id, cut.source.expected_revision, cut.observed_revision,
                    ).map_err(|error| error.to_string())?;
                    plans.extend(later.into_iter().filter(|event| event.kind == "tool.execution_plan.created"));
                }
                graph_plan_events = Some(plans);
            }
            if !super::current_review::graph_tail_preserves_work(
                store,
                source,
                *pinned,
                current,
                graph_plan_events.as_deref().expect("graph plan view initialized"),
            )? {
                return Err(format!(
                    "proof foreign graph work changed; {}",
                    super::describe_source_advance(store, source, *pinned)
                ).into());
            }
            // Parent model/ToolBatch continuations are execution trace. Their
            // real effects and immutable material are checked below; a raw
            // lifecycle revision is not itself a new business effect.
            continue;
        }
        #[cfg(test)]
        obs1120::record(
            "retirement.or",
            serde_json::json!({"source":source,"pinned":pinned,"current":current,
            "short_circuit":current_review_effect_key.is_none()}),
        );
        if current_review_effect_key.is_none()
            || !super::current_review::graph_tail_is_own_review_retirement(
                store,
                program,
                source,
                *pinned,
                current,
                reviewer_delivery_packet.as_ref(),
                &mut sources,
            )?
        {
            return Err(format!(
                "proof graph source changed outside its own review retirement; {}",
                super::describe_source_advance(store, source, *pinned)
            ).into());
        }
        absorbed_graph_sources.insert(source.clone(), *pinned);
    }
    let deliveries = review_delivery_snapshot(
        store,
        program,
        reviewer_delivery_packet.as_ref(),
        Some(&inputs.material_events),
    )?;
    for (source, revision) in &deliveries.sources {
        if sources
            .insert(source.clone(), *revision)
            .is_some_and(|previous| previous != *revision)
        {
            return Err("proof read-delivery source changed during validation".into());
        }
    }
    let delivered_files = deliveries.complete_file_reads()?;
    let mut process_selection = std::collections::BTreeMap::new();
    let mut pending_metadata = Vec::new();
    let mut metadata_identities = std::collections::BTreeMap::new();
    let result_identities = inputs
        .result_refs
        .iter()
        .map(|reference| super::review_result_identity(program, reference))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let read_identities = proof
        .reads
        .iter()
        .map(|read| super::review_result_identity(program, &read.result_ref))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if result_identities != read_identities || read_identities.len() != proof.reads.len() {
        return Err("proof result read set does not match its original result identities".into());
    }
    for read in &proof.reads {
        let mut observed = super::ReadCoverage {
            proof: read.clone(),
            total: None,
            chunks: BTreeSet::new(),
        };
        observed.proof.receipt_refs.clear();
        for delivery in &deliveries.reads {
            observed.observe_delivered(delivery)?;
        }
        if !observed.complete()
            || observed
                .proof
                .receipt_refs
                .into_iter()
                .collect::<BTreeSet<_>>()
                != read.receipt_refs.iter().cloned().collect::<BTreeSet<_>>()
        {
            return Err(
                "proof result lacks its exact complete authenticated reviewer delivery".into(),
            );
        }
        let content = harness_contract::context::ArtifactRef::durable(
            &read.content_ref,
            &read.sha256,
            read.bytes,
            "application/octet-stream",
            format!("session:{}", program.session_id),
        );
        let identity = super::review_result_identity(program, &read.result_ref)?;
        let metadata = super::results::resolve_result_metadata_for_phase(
            store,
            program,
            &identity,
            &content,
            Some(&inputs.source_revisions),
            readers,
            !inputs.semantic_digest.is_empty(),
            pre_tail,
        )?;
        if metadata.kind != read.result_kind {
            return Err(
                "proof result kind disagrees with its independently recovered original metadata"
                    .into(),
            );
        }
        let selected = super::validate_process_claims(
            store,
            &metadata.process_runs,
            &read.effect_observation_refs,
            &deliveries,
        )?;
        for run in &selected {
            super::results::validate_process_archives(run, readers)?;
            super::validate_selected_process_inputs(store, run, readers)?;
        }
        process_selection.insert(read.result_ref.clone(), selected);
        let file_claims = read
            .effect_observation_refs
            .iter()
            .filter(|reference| !super::is_process_body_receipt(reference))
            .cloned()
            .collect::<Vec<_>>();
        super::effect_observation_refs(&metadata, &delivered_files, Some(&file_claims))?;
        if metadata_identities
            .get(&identity)
            .is_some_and(|previous: &harness_contract::context::ArtifactRef| previous != &content)
        {
            return Err("proof duplicate read has conflicting original content identity".into());
        }
        metadata_identities.insert(identity.clone(), metadata.content.clone());
        pending_metadata.push((identity, metadata));
    }
    let primary_refs = result_identities.iter().cloned().collect::<Vec<_>>();
    let (_, primary_artifacts, _) =
        super::selected_work(store, program, goal, &primary_refs, &mut sources)?;
    let mut pending_primary_refs = primary_artifacts.into_iter().collect::<BTreeSet<_>>();
    let mut primary_producers = BTreeSet::new();
    let mut verified_metadata = BTreeSet::new();
    let mut result_effect_metadata = std::collections::BTreeMap::new();
    loop {
        while let Some((identity, mut metadata)) = pending_metadata.pop() {
            metadata.origin.require_strong()?;
            let producer_ids = metadata
                .producers
                .iter()
                .map(|producer| producer.execution.clone())
                .collect::<BTreeSet<_>>();
            let semantic_metadata = serde_json::json!({
                "origin":metadata.origin, "content":metadata.content,
                "producers":producer_ids, "kind":metadata.kind, "cursor":metadata.effect_cursor,
                "scopes":metadata.effect_scopes, "targets":metadata.effect_targets, "unscoped":metadata.unscoped_effect,
                "inputs":metadata.primary_inputs, "process_history":metadata.process_runs,
            });
            if result_effect_metadata
                .get(&identity)
                .is_some_and(|previous| previous != &semantic_metadata)
            {
                return Err(
                    "proof duplicate result has conflicting original identity or provenance".into(),
                );
            }
            if !verified_metadata.insert(identity.clone()) {
                continue;
            }
            if proof.independence_required
                && metadata.producers.iter().any(|producer| {
                    producer.execution == proof.reviewer_execution_id
                        || reviewer_delivery_packet.as_ref().is_some_and(|packet| {
                            packet
                                .agentic_binding
                                .as_ref()
                                .is_some_and(|binding| producer.actor == binding.agent_id)
                        })
                })
            {
                return Err(
                    "proof original primary result influence includes its independent reviewer"
                        .into(),
                );
            }
            let acquired = primary_producer_immutable_inputs_with_tail(
                store,
                program,
                &metadata.producers,
                readers,
                Some(&inputs.source_revisions),
                pre_tail,
            )?;
            require_actual_materials(&acquired.material_events, &inputs.material_events)?;
            for (source, revision) in acquired.sources {
                if sources
                    .insert(source, revision)
                    .is_some_and(|previous| previous != revision)
                {
                    return Err("primary acquisition source changed during proof validation".into());
                }
            }
            for content in acquired.immutable_inputs {
                if !metadata.primary_inputs.contains(&content) {
                    metadata.primary_inputs.push(content);
                }
            }
            actual_boundaries.extend(metadata.input_boundaries.iter().cloned());
            if let Some(cursor) = metadata.effect_cursor {
                business_outputs.extend(
                    metadata
                        .effect_targets
                        .iter()
                        .cloned()
                        .map(|target| (target, cursor)),
                );
            }
            primary_producers.extend(producer_ids.iter().cloned());
            result_effect_metadata.insert(identity.clone(), semantic_metadata);
            for source in metadata
                .effect_sources
                .iter()
                .chain(&metadata.result_sources)
            {
                if sources
                    .insert(source.stream_id.clone(), source.expected_revision)
                    .is_some_and(|previous| previous != source.expected_revision)
                {
                    return Err(
                        "proof original result metadata source changed during validation".into(),
                    );
                }
            }
            for input in &metadata.primary_inputs {
                let Some(reference) = input.dependency_identity(program)? else {
                    continue;
                };
                if !program.artifacts.contains_key(&reference)
                    && !reference.starts_with("tool://")
                    && !reference.starts_with("approval:v1:")
                    && !reference.starts_with("artifact://")
                {
                    return Err("immutable input has no authenticated producer reference".into());
                }
                let recovered = super::results::resolve_result_metadata_for_phase(
                    store,
                    program,
                    &reference,
                    &input.content,
                    Some(&inputs.source_revisions),
                    readers,
                    !inputs.semantic_digest.is_empty(),
                    pre_tail,
                )?;
                input.validate_dependency(program, &reference, &recovered)?;
                recovered.origin.require_strong()?;
                remember_primary_content(&mut metadata_identities, &reference, &recovered.content)?;
                pending_metadata.push((reference, recovered));
            }
            for producer in &metadata.producers {
                let Some(packet) = &producer.packet else {
                    continue;
                };
                if inputs.packet_digests.get(packet.graph_id())
                    != Some(&super::proof_value_digest(packet))
                    || !proof.effect_source_refs.iter().any(|source| {
                        source
                            == &format!(
                                "execution-agent-receipts:{}:{}:{}",
                                packet.graph_id(),
                                packet.node_id(),
                                packet.attempt
                            )
                    })
                {
                    return Err("proof omits or changes an independently recovered original producer packet/effect source".into());
                }

                let Some(binding) = &packet.agentic_binding else {
                    continue;
                };
                let task_ref = match &binding.focus {
                    harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
                    | harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref }
                        if binding.program_id == program.program_id =>
                    {
                        Some(task_ref)
                    }
                    _ => None,
                };
                if let Some(task_ref) = task_ref {
                    let (_, artifacts, _) = super::selected_work(
                        store,
                        program,
                        None,
                        &[task_ref.clone()],
                        &mut sources,
                    )?;
                    pending_primary_refs.extend(artifacts);
                }
            }
            if let Some(artifact) = program.artifacts.get(&identity) {
                for reference in &artifact.relates_to {
                    let reference = super::identity::auxiliary_reference(program, reference)?;
                    if program.artifacts.contains_key(&reference)
                        || reference.starts_with("tool://")
                        || reference.starts_with("approval:v1:")
                        || reference.starts_with("artifact://")
                    {
                        pending_primary_refs.insert(reference);
                    }
                }
            }
        }
        let Some(reference) = pending_primary_refs.pop_first() else {
            break;
        };
        if verified_metadata.contains(&reference) {
            continue;
        }
        let content = match metadata_identities.get(&reference) {
            Some(content) => content.clone(),
            None => {
                let selector = match program.artifacts.get(&reference) {
                    Some(artifact) => artifact.content_ref.as_str(),
                    None if reference.starts_with("tool://")
                        || reference.starts_with("approval:v1:")
                        || reference.starts_with("artifact://") =>
                    {
                        reference.as_str()
                    }
                    None => return Err(
                        "proof primary influence has no authenticated original content identity"
                            .into(),
                    ),
                };
                harness_contract::context::ArtifactRef::durable(
                    selector,
                    "",
                    0,
                    "application/octet-stream",
                    format!("session:{}", program.session_id),
                )
            }
        };
        let metadata = super::results::resolve_result_metadata_for_phase(
            store,
            program,
            &reference,
            &content,
            Some(&inputs.source_revisions),
            readers,
            !inputs.semantic_digest.is_empty(),
            pre_tail,
        )?;
        pending_metadata.push((reference, metadata));
    }
    if primary_producers
        != proof
            .producer_execution_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
        || primary_producers.len() != proof.producer_execution_ids.len()
    {
        return Err(
            "proof producer set disagrees with independently recovered primary result origins"
                .into(),
        );
    }
    let mut reviewer_reads = std::collections::BTreeMap::new();
    for read in delivered_files {
        reviewer_reads
            .entry((
                read.path.workspace_id,
                read.path.repository_id,
                read.path.workspace_relative_path,
            ))
            .and_modify(|cursor: &mut u64| *cursor = (*cursor).max(read.cursor))
            .or_insert(read.cursor);
    }
    let actual = actual_input_snapshot_for_phase(
        store,
        program,
        &actual_boundaries,
        readers,
        Some(&inputs.source_revisions),
        !inputs.semantic_digest.is_empty(),
        pre_tail,
    )?;
    if !inputs.required_scope_complete || actual.required_scopes() != inputs.required_scopes {
        return Err(
            "proof actual dependency set differs from independently recovered acquisitions".into(),
        );
    }
    require_actual_materials(&actual.material_events, &inputs.material_events)?;
    for (source, revision) in &actual.sources {
        if sources
            .insert(source.clone(), *revision)
            .is_some_and(|old| old != *revision)
        {
            return Err("proof actual dependency source changed".into());
        }
    }
    let mut review_targets = inputs.required_scopes.clone();
    for (target, _) in &business_outputs {
        if !review_targets.contains(target) {
            review_targets.push(target.clone());
        }
    }
    for (source, revision) in &inputs.source_revisions {
        if source.starts_with("approval:")
            && proof.result_source_revisions.get(source) != Some(revision)
        {
            return Err("proof external decision source is missing or changed".into());
        }
    }
    for (source, revision) in &proof.result_source_revisions {
        let current = store
            .stream_revision(source)
            .map_err(|error| error.to_string())?;
        let authenticated_session_growth = pre_tail.is_some_and(|tail|
            tail.source.stream_id == *source && tail.source.expected_revision == *revision
                && tail.observed_revision == current);
        if current != *revision && absorbed_graph_sources.get(source) != Some(revision)
            && !authenticated_session_growth {
            return Err("proof external decision source changed".into());
        }
        if sources.insert(source.clone(), current).is_some_and(|previous| previous != current) {
            return Err("proof source changed during validation; retry current inputs".into());
        }
    }
    if !physical_unknown.is_empty() {
        return Err(format!("effect_review_pending: opaque Process history may intersect required work: {physical_unknown:?}").into());
    }
    let mut required_effects = std::collections::BTreeMap::new();
    required_effects.insert(
        "process_physical_envelopes".into(),
        serde_json::to_value(&physical_facts).map_err(|error| error.to_string())?,
    );
    required_effects.insert(
        "actual_input_acquisitions".into(),
        serde_json::to_value(&actual.canonical_facts).map_err(|error| error.to_string())?,
    );
    required_effects.insert(
        "actual_input_dependencies".into(),
        serde_json::to_value(&actual.file_dependencies).map_err(|error| error.to_string())?,
    );
    let mut relevant_writes = Vec::new();
    let mut runtime_control_keys = BTreeSet::new();
    let mut lifecycle_keys = std::collections::BTreeMap::new();
    let mut last_effect_cursor = 0;
    for source in receipt_sources {
        let owners = candidate_sources.get(&source);
        let events = pinned_stream_events(
            store,
            &source,
            &mut sources,
            if owners.is_some() { 0 } else { start_cursor },
            PinnedEventProjection::ReceiptIndexEffects,
        )?;
        let required = required_sources.contains(&source);
        let mut intents = std::collections::BTreeMap::new();
        let mut receipts = std::collections::BTreeMap::new();
        for event in &events {
            if let Some(owners) = owners {
                if !matches!(
                    event.kind.as_str(),
                    "execution.agent_tool.intent" | "execution.agent_tool.receipt"
                ) {
                    incomplete_candidates.extend(owners.iter().cloned());
                }
                if event.actor.as_deref() == Some("governed_tool")
                    && event.payload["effect_kind"] != "read"
                    && event.kind == "execution.agent_tool.intent"
                {
                    let scope = admitted_intent_scope(store, event, &mut sources)?;
                    let process = event.payload["idempotency_key"]
                        .as_str()
                        .map(|key| authenticated_process_for_key(store, key, &mut sources))
                        .transpose()?
                        .flatten();
                    let proven = !required
                        && (process.is_some()
                            || inputs.required_scope_complete
                                && canonical_scope_disjoint(&review_targets, &scope))
                        && event.payload["idempotency_key"]
                            .as_str()
                            .is_some_and(|identity| !identity.is_empty());
                    if proven {
                        positive_effect_candidates.extend(owners.iter().cloned());
                    } else {
                        incomplete_candidates.extend(owners.iter().cloned());
                    }
                } else if event.actor.as_deref() != Some("governed_tool") {
                    incomplete_candidates.extend(owners.iter().cloned());
                }
                if event.kind == "execution.agent_tool.receipt"
                    && event.payload["effect_kind"] != "read"
                    && !events.iter().any(|intent| {
                        intent.kind == "execution.agent_tool.intent"
                            && intent.payload["idempotency_key"].as_str().is_some()
                            && intent.payload["idempotency_key"] == event.payload["idempotency_key"]
                    })
                {
                    incomplete_candidates.extend(owners.iter().cloned());
                }
            }
            if event.actor.as_deref() != Some("governed_tool")
                || event.payload["tool_name"]
                    .as_str()
                    .or_else(|| event.payload["outcome"]["tool_name"].as_str())
                    .is_some_and(goal_metadata_tool)
                || event.payload["effect_kind"] == "read"
            {
                continue;
            }
            let Some(identity) = event.payload["idempotency_key"].as_str() else {
                return Err("proof effect lacks canonical identity".into());
            };
            match event.kind.as_str() {
                "execution.agent_tool.intent" => {
                    intents.insert(identity, event);
                }
                "execution.agent_tool.receipt" => {
                    receipts.insert(identity, event);
                }
                _ => {}
            }
        }
        for (identity, intent) in intents {
            let scope = admitted_intent_scope(store, intent, &mut sources)?;
            if let Some(run) = authenticated_process_for_key(store, identity, &mut sources)? {
                let receipt = receipts
                    .get(identity)
                    .ok_or("effect_review_pending: bounded process index missing")?;
                authenticate_settled_effect(store, intent, receipt, &mut sources)?;
                for material in super::results::retain_process_execution(store, &run, &mut sources)?
                {
                    let event = verify_material_event(store, &material, &mut sources)?;
                    required_effects.insert(event.event_id.clone(), event.payload.clone());
                }
                continue;
            }
            let disjoint = !required
                && inputs.required_scope_complete
                && canonical_scope_disjoint(&review_targets, &scope);
            let settled_descriptor = receipts.get(identity).map(|receipt|
                authenticate_settled_effect(store, intent, receipt, &mut sources)
            ).transpose()?;
            let settled = receipts.get(identity).is_some_and(|receipt| {
                matches!(
                    receipt.payload["outcome"]["status"].as_str(),
                    Some("executed" | "blocked_permission")
                )
            });
            // The status table is one closed set, and the same outcome status
            // is never answered differently in two places: `executed` and
            // `blocked_permission` belong to the existing settled rule,
            // `failed` belongs to this arm, and any other status is named
            // rather than guessed.
            let terminally_failed = match terminal_outcome_class(
                receipts
                    .get(identity)
                    .and_then(|receipt| receipt.payload["outcome"]["status"].as_str()),
            ) {
                Ok(class) => match class {
                    // The existing settled rule owns these two.
                    OutcomeClass::Succeeded => false,
                    // A call that ran and failed is terminal too: the
                    // authenticator above has proved that this receipt
                    // settles this very intent. Such a write is not
                    // satisfied - it failed - but it is no longer pending,
                    // so the review must not wait for a receipt that will
                    // never come.
                    OutcomeClass::Failed => settled_descriptor.is_some(),
                    // No receipt at all: the call is still in flight.
                    OutcomeClass::InFlight => false,
                },
                // A receipt whose status this review cannot read is not a
                // miss and not a failure: it stays pending under a name.
                Err(reason) => {
                    return Err(format!(
                        "effect_review_pending: required write outcome status is not one this review can read; source={source} identity={identity} reason={reason}",
                    ).into());
                }
            };
            // The exemption is the reviewer's OWN admitted call: the intent
            // has to be the one Runtime admitted for THIS graph, node and
            // attempt. The receipt index stream names all three, so an intent
            // with the same effect identity in another stream belongs to
            // another attempt, another node or another graph.
            match review_write_outcome(
                is_review_own_admitted_call(
                    current_review_effect_key.as_deref(),
                    reviewer_receipt_stream.as_deref(),
                    source.as_str(),
                    identity,
                )
                .then_some(identity),
                identity,
                settled,
                disjoint,
            ) {
                ReviewWriteOutcome::Exempt => {
                    // The review is a control action the reviewer performs
                    // while this proof is assembled, so its own intent is
                    // still in flight when the proof checks it. Requiring it
                    // to be settled first is a self-dependency: the review can
                    // never settle the action it is.
                    //
                    // Only that ONE call is exempt, and only because Runtime
                    // resolved its exact effect identity from the durable
                    // evidence and authenticated it against the action being
                    // applied. Another in-flight write by this same reviewer -
                    // even another unfinished review - carries a different
                    // identity and still blocks.
                    continue;
                }
                ReviewWriteOutcome::Blocked => {
                    // Two terminal facts can answer this call, and they answer
                    // different questions: a refusal twin says the call was
                    // refused before it took effect, a terminal receipt says
                    // the call ran and failed. The decision is the one the
                    // review has always made, read here from the call's own
                    // durable facts.
                    let receipt = receipts.get(identity);
                    let settlement = required_write_settlement(
                        store,
                        identity,
                        &intent.payload,
                        receipt.map(|receipt| (&receipt.payload, receipt.commit_cursor)),
                        terminally_failed,
                        &mut sources,
                    )?;
                    match settlement {
                        RequiredWriteSettlement::Refused { .. } => {
                            // The call carries its own refusal twin: it was
                            // refused before it took effect, so it is settled.
                            continue;
                        }
                        RequiredWriteSettlement::Failed { failure } => {
                            // The named failure: this required write ran and
                            // it failed. It is carried, never counted as
                            // satisfied and never as an effect that did not
                            // happen.
                            terminal_failures.push(failure);
                            continue;
                        }
                        RequiredWriteSettlement::UnreadableRefusal { reason } => {
                            return Err(format!(
                                "effect_review_pending: required write refusal fact is not readable; source={source} identity={identity} reason={reason}",
                            ).into());
                        }
                        RequiredWriteSettlement::Contradiction {
                            refusal_cursor,
                            receipt_cursor,
                        } => {
                            return Err(format!(
                                "effect_review_pending: refused and terminally failed facts contradict for the same call; effect_key={identity} intent_transaction={} refusal_cursor={:?} receipt_cursor={:?}",
                                intent.transaction_id,
                                refusal_cursor,
                                receipt_cursor,
                            ).into());
                        }
                        RequiredWriteSettlement::MissingReceipt => {
                            return Err(format!(
                                "effect_review_pending: required write failure has no receipt to name; source={source} identity={identity}",
                            ).into());
                        }
                        RequiredWriteSettlement::Pending => {
                            return Err(format!(
                                "effect_review_pending: unknown required or possibly intersecting delegated write; source={source} identity={identity} tool={} required={required} disjoint={disjoint} receipt_present={} current_review={:?}",
                                intent.payload["tool_name"].as_str().unwrap_or("unknown"),
                                receipts.contains_key(identity),
                                current_review_effect_key,
                            ).into());
                        }
                    }
                }
                ReviewWriteOutcome::Settled => {}
            }
            if settled_descriptor.as_ref().is_some_and(super::results::runtime_control_descriptor) {
                runtime_control_keys.insert(identity.to_string());
            }
            if !disjoint {
                relevant_writes.push((
                    intent.commit_cursor,
                    Some(scope.clone()),
                    Some(identity.to_string()),
                ));
                last_effect_cursor = last_effect_cursor.max(intent.commit_cursor);
                required_effects.insert(intent.event_id.clone(), intent.payload.clone());
                if let Some(receipt) = receipts.get(identity) {
                    relevant_writes.push((
                        receipt.commit_cursor,
                        Some(scope),
                        Some(identity.to_string()),
                    ));
                    last_effect_cursor = last_effect_cursor.max(receipt.commit_cursor);
                    required_effects.insert(receipt.event_id.clone(), receipt.payload.clone());
                }
            }
        }
        for (identity, receipt) in receipts {
            if !events.iter().any(|event| {
                event.kind == "execution.agent_tool.intent"
                    && event.payload["idempotency_key"] == identity
            }) {
                if let Some(owners) = owners {
                    incomplete_candidates.extend(owners.iter().cloned());
                }
                if !matches!(
                    receipt.payload["outcome"]["status"].as_str(),
                    Some("executed" | "blocked_permission")
                ) {
                    return Err("effect_review_pending: unproven orphan effect receipt".into());
                }
                last_effect_cursor = last_effect_cursor.max(receipt.commit_cursor);
                relevant_writes.push((receipt.commit_cursor, None, Some(identity.to_string())));
                required_effects.insert(receipt.event_id.clone(), receipt.payload.clone());
            }
        }
    }
    let mut planned = BTreeSet::new();
    let mut completed = BTreeSet::new();
    for event in &session_events {
        if event.actor.as_deref() != Some("conversation_runtime") {
            continue;
        }
        if event.activity_binding().is_some_and(|binding| {
            binding.session_id != program.session_id
                || !related_roots.contains(&binding.root_execution_id)
                || !super::binding_is_owned_by_root(&binding, Some(&binding.root_execution_id))
        }) {
            continue;
        }
        if event.kind == "tool.execution_plan.created" {
            if let Some(packet) = delegated_plan_callback_owner(
                event,
                &session_events,
                &related_roots,
                &delegated_session_packets,
            ) {
                if let Some(effects) = authenticated_delegated_plan_settled(
                    store,
                    event,
                    &session_events,
                    packet,
                    &mut sources,
                )? {
                    required_effects.extend(effects);
                    continue;
                }
            }
            let Some(tasks) = event.payload["tasks"].as_array() else {
                return Err("proof tool plan lacks admitted effect scopes".into());
            };
            for task in tasks {
                if task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                    || task["effect"]["effect_kind"] == "read"
                {
                    continue;
                }
                let (Some(plan), Some(call)) = (
                    event.payload["plan_id"].as_str(),
                    task["tool_call_id"].as_str(),
                ) else {
                    return Err("proof root effect identity missing".into());
                };
                required_effects.insert(format!("{}:{call}", event.event_id), task.clone());
                let step: crate::step_snapshot::StepCallBinding =
                    serde_json::from_value(task["step_binding"].clone())
                        .map_err(|_| "root effect plan lacks its original Step identity")?;
                let call_key = step.effect_idempotency_key();
                if current_review_effect_key.as_deref() != Some(call_key.as_str()) {
                    // The call that carries the review is the same exact
                    // identity the delegated receipt index resolves, so both
                    // views name ONE call. Every other planned write still has
                    // to settle.
                    planned.insert((plan.to_string(), call.to_string()));
                }
                if authenticated_process_for_key(store, &call_key, &mut sources)?.is_none() {
                    relevant_writes.push((event.commit_cursor, None, Some(call_key)));
                    last_effect_cursor = last_effect_cursor.max(event.commit_cursor);
                }
            }
        } else if event.kind.starts_with("tool.invocation.")
            && !event.payload["tool_name"]
                .as_str()
                .is_some_and(goal_metadata_tool)
            && !super::results::tool_effect_from_store(store, event)?
                .is_some_and(|effect| effect.effect_kind == ToolEffectKind::Read)
        {
            if matches!(
                event.kind.as_str(),
                "tool.invocation.completed" | "tool.invocation.denied"
            ) {
                if let (Some(plan), Some(call)) = (
                    event.payload["governed_plan_id"].as_str(),
                    event.payload["tool_call_id"].as_str(),
                ) {
                    completed.insert((plan.to_string(), call.to_string()));
                }
            }
            if event.kind == "tool.invocation.completed" {
                let task = super::results::result_tool_plan(&session_events, event)?
                    .ok_or("root settled effect has no original plan")?;
                let primary =
                    super::results::original_tool_primary(store, event, task, &mut sources)?;
                let descriptor = super::results::receipt_effect_descriptor(&primary)?;
                let scope = super::results::require_original_effect_scope(
                    &primary.payload["resolved_effect_scope"],
                    &descriptor,
                )?;
                let key = primary.payload["idempotency_key"]
                    .as_str()
                    .ok_or("root effect identity missing")?;
                if primary.payload["outcome"]["status"] == "executed"
                    && super::results::runtime_control_descriptor(&descriptor)
                {
                    runtime_control_keys.insert(key.to_string());
                }
                if authenticated_process_for_key(store, key, &mut sources)?.is_none() {
                    relevant_writes.push((
                        primary.commit_cursor,
                        Some(scope),
                        Some(key.to_string()),
                    ));
                }
                required_effects.insert(primary.event_id.clone(), primary.payload.clone());
            }
            required_effects.insert(event.event_id.clone(), event.payload.clone());
            let call_key = super::results::result_tool_plan(&session_events, event)?
                .map(|task| {
                    serde_json::from_value::<crate::step_snapshot::StepCallBinding>(
                        task["step_binding"].clone(),
                    )
                    .map(|step| step.effect_idempotency_key())
                    .map_err(|_| "root effect lifecycle has an invalid original Step")
                })
                .transpose()?;
            if let Some(key) = &call_key {
                lifecycle_keys.insert(event.event_id.clone(), key.clone());
            }
            let auxiliary = call_key
                .as_ref()
                .map(|key| authenticated_process_for_key(store, key, &mut sources))
                .transpose()?
                .flatten();
            if auxiliary.is_none() {
                relevant_writes.push((event.commit_cursor, None, call_key));
                last_effect_cursor = last_effect_cursor.max(event.commit_cursor);
            }
        }
    }
    if !planned.is_subset(&completed) {
        // The call that carries the review is already out of `planned`: it is
        // the review itself, and the review cannot settle the action it is.
        // Every other planned write still has to settle.
        let pending = planned.difference(&completed).collect::<Vec<_>>();
        if !pending.is_empty() {
            return Err(format!(
                "effect_review_pending: root writes must settle before acceptance; pending={pending:?}; terminal={completed:?}",
            ).into());
        }
    }
    #[cfg(test)]
    if !runtime_control_keys.is_empty() {
        let controls = runtime_control_keys.iter().map(|key| {
            let source = format!("execution-effect:{key}");
            let intent = store.event_by_idempotency_key(&source, &format!("{key}:intent"));
            let receipt = store.event_by_idempotency_key(&source, &format!("{key}:receipt"));
            serde_json::json!({
                "key":key,
                "intent":intent.ok().flatten().map(|event| serde_json::json!({
                    "event_id":event.event_id,"stream":event.stream_id,"cursor":event.commit_cursor,
                    "descriptor":event.payload["effect"],"scope":event.payload["resolved_effect_scope"],
                    "origin":event.payload["replay"]["origin"],
                })),
                "receipt":receipt.ok().flatten().map(|event| serde_json::json!({
                    "event_id":event.event_id,"cursor":event.commit_cursor,"transaction":event.transaction_id,
                    "tool":event.payload["outcome"]["tool_name"],"status":event.payload["outcome"]["status"],
                })),
            })
        }).collect::<Vec<_>>();
        let record = serde_json::json!({
            "kind":"native_authenticated_control_file_view","observation_only":true,
            "controls":controls,"writes_before":relevant_writes,
            "actual_dependencies":actual.file_dependencies,"actual_input_facts":actual.canonical_facts,
            "reviewer_reads":reviewer_reads.iter().map(|(identity, cursor)|
                serde_json::json!({"identity":identity,"cursor":cursor})).collect::<Vec<_>>(),
            "planned":planned,"completed":completed,
        });
        if let Ok(encoded) = serde_json::to_string(&record) {
            if encoded.len() <= 512 * 1024 {
                eprintln!("OWN832_RECORD_BEGIN\n{encoded}\nOWN832_RECORD_END");
            } else {
                eprintln!("OWN832_RECORD_BEGIN\n{}\nOWN832_RECORD_END", serde_json::json!({
                    "kind":"native_control_file_view_omitted","bytes":encoded.len(),"limit":512*1024,
                }));
            }
        }
    }
    relevant_writes.retain(|(_, _, call_key)| {
        !is_authenticated_control_effect(
            call_key.as_deref(),
            current_review_effect_key.as_deref(),
            &runtime_control_keys,
        )
    });
    for dependency in &actual.file_dependencies {
        let target = &dependency.target;
        let identity = (
            target.path.workspace_id.clone(),
            target.path.repository_id.clone(),
            target.path.workspace_relative_path.clone(),
        );
        let read_cursor = reviewer_reads.get(&identity).copied().unwrap_or_default();
        for (cursor, scope, call_key) in &relevant_writes {
            if effect_disjoint_from_target(target, scope.as_ref()) {
                continue;
            }
            if input_changed_before_use(dependency, *cursor, call_key.as_deref()) {
                return Err(format!(
                    "proof_actual_input_stale_before_use: {} acquisition={} consumer={} write={cursor}",
                    target.path.workspace_relative_path, dependency.acquisition_cursor, dependency.consumer_cursor,
                ).into());
            }
            if *cursor >= dependency.acquisition_cursor && *cursor >= read_cursor {
                return Err(format!(
                    "review_requires_effect_observation: {} original_acquisition={} complete_reviewer_read={read_cursor} write={cursor}",
                    target.path.workspace_relative_path, dependency.acquisition_cursor,
                ).into());
            }
        }
    }
    for (target, original_effect_cursor) in &business_outputs {
        let identity = (
            target.path.workspace_id.clone(),
            target.path.repository_id.clone(),
            target.path.workspace_relative_path.clone(),
        );
        let read_cursor = reviewer_reads.get(&identity).copied().unwrap_or_default();
        if !output_observation_is_fresh(
            *original_effect_cursor,
            read_cursor,
            relevant_writes
                .iter()
                .filter(|(_, scope, _)| !effect_disjoint_from_target(target, scope.as_ref()))
                .map(|(cursor, _, _)| *cursor),
        ) {
            return Err(format!(
                "review_requires_effect_observation: business output {} needs a complete later independent File read",
                target.path.workspace_relative_path,
            ).into());
        }
    }
    strip_authenticated_runtime_control_effects(
        &mut required_effects,
        &runtime_control_keys,
        &lifecycle_keys,
    );
    let effect_digest = super::proof_value_digest(&required_effects);
    let semantic_digest = super::proof_value_digest(&serde_json::json!({
        "domain":"cowd.proof.inputs.v2","goal_revision":proof.goal_spec_revision,
        "goal_digest":proof.goal_spec_digest,"condition":inputs.condition_ref,
        "work":work_digest,"policy":policy_digest,"effects":effect_digest,
        "packets":inputs.packet_digests,"materials":inputs.material_events,
        "reads":proof.reads,"result_effect_metadata":result_effect_metadata,"reviewer":proof.reviewer_execution_id,
        "process_selection":process_selection,
        "producers":proof.producer_execution_ids,"required_scopes":inputs.required_scopes,
        "required_scope_complete":inputs.required_scope_complete,
    }));
    for (source, revision) in &sources {
        let actual_revision = store
            .stream_revision(source)
            .map_err(|error| error.to_string())?;
        if actual_revision != *revision {
            return Err(format!(
                "proof source changed during validation; retry current inputs; source={source} expected={revision} actual={actual_revision} phase=final_source_fence; {}",
                super::describe_source_advance(store, source, *revision)
            ).into());
        }
    }
    let session_revision = *sources
        .get(&session)
        .ok_or("proof Session source was not read")?;
    let sources = sources
        .into_iter()
        .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
            stream_id,
            expected_revision,
        })
        .collect::<Vec<_>>();
    let dependency_frame = ConclusionMaterialDependencies {
        session_source: ExpectedStreamRevision { stream_id: session.clone(), expected_revision: session_revision },
        historical_session_cut: inputs.source_revisions.get(&session).copied(),
        session_id: program.session_id.clone(), turn_id: program.turn_id.clone(),
        review_targets,
        file_dependencies: actual.file_dependencies.clone(),
        immutable_inputs: actual.immutable_inputs.clone(),
        material_events: actual.material_events.clone(),
        canonical_facts: actual.canonical_facts.clone(),
    };
    Ok(ProofInputSnapshot {
        work_digest,
        task_refs,
        artifact_refs,
        reviewer_execution_refs,
        reviewer_packet: reviewer_delivery_packet.clone(),
        retiring_optional_task_refs: candidates
            .into_iter()
            .filter(|task| {
                positive_effect_candidates.contains(task) && !incomplete_candidates.contains(task)
            })
            .collect(),
        semantic_digest,
        policy_digest,
        effects: RootReviewPolicySnapshot {
            digest: effect_digest,
            last_effect_cursor,
            source: ExpectedStreamRevision {
                stream_id: session,
                expected_revision: session_revision,
            },
            additional_sources: Vec::new(),
        },
        sources,
        current_review_effect_key,
        terminal_failures,
        dependency_frame,
        pre_authenticated_session_source: pre_tail.map(|tail| tail.source.clone()),
    })
}

fn authenticated_delegated_plan_settled(
    store: &std::sync::Arc<RuntimeEventStore>,
    plan: &crate::DurableRuntimeEvent,
    events: &[crate::DurableRuntimeEvent],
    packet: &harness_contract::agent::AgentTaskPacket,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Option<std::collections::BTreeMap<String, serde_json::Value>>, super::results::ReviewEvidenceError> {
    if plan.actor.as_deref() != Some("conversation_runtime")
        || plan.schema_version != 1
        || plan.scope != RuntimeEventScope::Tool
        || plan.status.as_deref() != Some("planned")
        || plan.stream_id != format!("session:{}", packet.session_id())
    {
        return Ok(None);
    }
    let tasks = plan.payload["tasks"]
        .as_array()
        .ok_or("delegated tool plan lacks its original tasks")?;
    for task in tasks {
        if task["tool_name"].as_str().is_some_and(goal_metadata_tool)
            || task["effect"]["effect_kind"] == "read"
        {
            continue;
        }
        let callbacks = events
            .iter()
            .filter(|terminal| {
                terminal.kind == "tool.invocation.completed"
                    && terminal.payload["governed_plan_id"] == plan.payload["plan_id"]
                    && terminal.payload["tool_call_id"] == task["tool_call_id"]
            })
            .collect::<Vec<_>>();
        if callbacks.len() != 1 {
            return Ok(None);
        }
        let terminal = callbacks[0];
        let Some(binding) = terminal.activity_binding() else {
            return Ok(None);
        };
        let step: crate::step_snapshot::StepCallBinding =
            serde_json::from_value(task["step_binding"].clone())
                .map_err(|_| "delegated callback plan lacks its original Step")?;
        if terminal.actor.as_deref() != Some("conversation_runtime")
            || terminal.schema_version != 1
            || terminal.scope != RuntimeEventScope::Tool
            || terminal.status.as_deref() != Some("completed")
            || terminal.stream_id != plan.stream_id
            || terminal.commit_cursor < plan.commit_cursor
            || binding.session_id != packet.session_id()
            || binding.session_id != step.snapshot.identity.session_id
            || binding.turn_id != step.snapshot.identity.turn_id
            || binding.root_execution_id != packet.graph_id()
            || binding.agent_run_id.as_deref() != Some(packet.run_id())
            || binding.agent_instance_id.as_deref() != Some(packet.agent_id())
            || binding.tool_call_id.as_deref() != task["tool_call_id"].as_str()
            || binding.tool_contract_id.as_deref() != task["tool_name"].as_str()
            || step.snapshot.identity.run_ref.as_deref() != Some(packet.run_id())
            || !crate::agentic::action_invocation::execution_ids_agree(
                step.snapshot.identity.execution_id.as_deref(),
                packet.run_id(),
            )
            || step
                .snapshot
                .identity
                .attempt
                .is_some_and(|attempt| attempt != packet.attempt)
            || step.snapshot.identity.actor_ref.as_deref()
                != Some(packet.assignment.execution_identity.principal_id())
            || !super::binding_is_owned_by_root(&binding, Some(packet.graph_id()))
        {
            return Ok(None);
        }
        super::results::original_tool_primary(store, terminal, task, sources)?;
    }
    Ok(Some(delegated_plan_required_effects(plan)?))
}

/// Project a settled delegated plan at Task granularity.
///
/// The plan itself lives on the Session stream and contains every call in the
/// batch. Its envelope is not an effect: only the non-metadata, non-read Tasks
/// are proof material, and each remains independently addressed by its call.
fn delegated_plan_required_effects(
    plan: &crate::DurableRuntimeEvent,
) -> Result<std::collections::BTreeMap<String, serde_json::Value>, String> {
    let tasks = plan.payload["tasks"]
        .as_array()
        .ok_or("delegated tool plan lacks its original tasks")?;
    tasks
        .iter()
        .filter(|task| {
            !task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                && task["effect"]["effect_kind"] != "read"
        })
        .map(|task| {
            let call = task["tool_call_id"]
                .as_str()
                .ok_or("delegated effect task lacks its call identity")?;
            Ok((format!("{}:{call}", plan.event_id), task.clone()))
        })
        .collect()
}

fn delegated_plan_callback_owner<'packet>(
    plan: &crate::DurableRuntimeEvent,
    events: &[crate::DurableRuntimeEvent],
    related_roots: &BTreeSet<String>,
    packets: &'packet std::collections::BTreeMap<String, harness_contract::agent::AgentTaskPacket>,
) -> Option<&'packet harness_contract::agent::AgentTaskPacket> {
    let tasks = plan.payload["tasks"].as_array()?;
    let owners = events.iter().filter(|event| {
        event.kind == "tool.invocation.completed"
            && event.payload["governed_plan_id"] == plan.payload["plan_id"]
            && tasks.iter().any(|task| task["tool_call_id"] == event.payload["tool_call_id"])
    }).filter_map(|event| event.activity_binding().map(|binding| binding.root_execution_id))
        .collect::<BTreeSet<_>>();
    if owners.len() != 1 { return None; }
    let owner = owners.iter().next()?;
    if related_roots.contains(owner) { return None; }
    packets.get(owner)
}

pub(super) fn goal_metadata_tool(name: &str) -> bool {
    matches!(
        name,
        "objective_update"
            | "objective_review"
            | "objective_complete_request"
            | "state_inspect"
            | "artifact_commit"
            | "artifact_publish"
            | "working_context"
    )
}

fn permits_low_risk_self_review(effect: &ToolEffectDescriptor) -> bool {
    use harness_contract::policy::*;
    use harness_contract::tool::*;
    if effect.effect_kind == ToolEffectKind::Read {
        return true;
    }
    effect.effect_kind == ToolEffectKind::Write
        && effect.required_permission == ToolPermissionMode::WorkspaceWrite
        && matches!(
            effect.idempotency,
            ToolIdempotency::Idempotent | ToolIdempotency::IdempotentWithKey
        )
        && matches!(
            effect.approval_class,
            ToolApprovalClass::None | ToolApprovalClass::Policy
        )
        && !effect.uses_network
        && !effect.spawns_process
        && !effect.mutates_packages
        && !effect.mutates_system
        && matches!(
            effect.assessment.reversibility,
            EffectReversibility::Reversible | EffectReversibility::Compensatable
        )
        && matches!(
            effect.assessment.externality,
            EffectExternality::Internal | EffectExternality::Workspace
        )
        && matches!(
            effect.assessment.data_sensitivity,
            DataClassification::Public | DataClassification::Internal
        )
        && matches!(
            effect.assessment.novelty,
            EffectNovelty::Routine | EffectNovelty::NewTarget
        )
        && matches!(
            effect.assessment.blast_radius,
            EffectBlastRadius::Item | EffectBlastRadius::Workspace
        )
        && !effect.scopes.is_empty()
        && effect.scopes.iter().all(|scope| {
            scope.resource == PermissionResource::File
                && matches!(
                    scope.operation,
                    PermissionOperation::Read | PermissionOperation::Write
                )
                && scope.target.as_deref().is_some_and(|target| {
                    !target.is_empty()
                        && !target.starts_with('~')
                        && !target.contains(['*', '?', '[', ']'])
                        && std::path::Path::new(target)
                            .components()
                            .all(|component| matches!(component, std::path::Component::Normal(_)))
                })
        })
}

pub(crate) fn root_self_review_policy(
    store: &RuntimeEventStore,
    program: &AgenticProgramProjection,
    goal: &GoalContract,
) -> Result<Option<RootReviewPolicySnapshot>, String> {
    if !program.tasks.is_empty()
        || !program.agents.is_empty()
        || !program.teams.is_empty()
        || program.required_team_count != 0
        || goal.participation_requirement.is_some()
        || goal.obligations.iter().any(|obligation| {
            obligation
                .evidence_requirement
                .independent_verifier_required
        })
    {
        return Ok(None);
    }
    let root = program
        .root_execution_id
        .as_deref()
        .ok_or("review policy requires root identity")?;
    let stream_id = format!("session:{}", program.session_id);
    let revision = store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?;
    let goal_events =
        store.list_stream_after(&format!("goal:{}", goal.id), 0, 1, 1, 4 * 1024 * 1024)?;
    let Some(created) = goal_events.first() else {
        return Ok(None);
    };
    let mut through = revision;
    let mut decision = None;
    let mut root_plans = BTreeSet::new();
    let mut safe_plans = BTreeSet::new();
    let mut unsafe_plans = BTreeSet::new();
    let mut unsafe_call = false;
    loop {
        let events = store.list_stream_before(&stream_id, through, 64, 1024 * 1024)?;
        if events.is_empty() {
            break;
        }
        through = events
            .last()
            .ok_or("empty proof page")?
            .sequence
            .checked_sub(1)
            .ok_or("invalid proof sequence")?;
        let mut before_goal = false;
        for event in events {
            if event.sequence > revision {
                continue;
            }
            before_goal |= event.commit_cursor < created.commit_cursor;
            if event.kind.starts_with("runtime.strategy.")
                && event.actor.as_deref() == Some("conversation_runtime.strategy_owner")
                && event.payload["execution_graph_ref"].as_str() == Some(root)
                && event.payload["session_ref"].as_str() == Some(program.session_id.as_str())
                && event.payload["turn_ref"].as_str() == Some(program.turn_id.as_str())
                && decision.is_none()
            {
                decision = Some(event.payload.clone());
            }
            if event.kind == "tool.execution_plan.created"
                && event.actor.as_deref() == Some("conversation_runtime")
            {
                if let (Some(id), Some(tasks)) = (
                    event.payload["plan_id"].as_str(),
                    event.payload["tasks"].as_array(),
                ) {
                    let safe = tasks.iter().all(|task| {
                        task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                            || serde_json::from_value::<ToolEffectDescriptor>(
                                task["effect"].clone(),
                            )
                            .is_ok_and(|effect| permits_low_risk_self_review(&effect))
                    });
                    if safe {
                        safe_plans.insert(id.to_string());
                    } else {
                        unsafe_plans.insert(id.to_string());
                    }
                    if event
                        .refs
                        .iter()
                        .any(|reference| reference.kind == "execution" && reference.id == root)
                    {
                        root_plans.insert(id.to_string());
                    }
                }
            }
            if event.kind.starts_with("tool.invocation.")
                && event.activity_binding().is_some_and(|binding| {
                    binding.root_execution_id == root
                        && binding.session_id == program.session_id
                        && binding.turn_id == program.turn_id
                        && binding.agent_run_id.is_none()
                })
            {
                let name = event.payload["tool_name"].as_str().unwrap_or_default();
                if goal_metadata_tool(name) {
                    continue;
                }
                if let Some(plan) = event.payload["governed_plan_id"].as_str() {
                    root_plans.insert(plan.to_string());
                } else {
                    // Old unclassified effects cannot authorize self review.
                    unsafe_call = true;
                }
            }
        }
        if before_goal && decision.is_some() {
            break;
        }
    }
    if store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?
        != revision
    {
        return Err(
            "review policy source changed; retry against current strategy and tool plans".into(),
        );
    }
    let Some(decision) = decision else {
        return Ok(None);
    };
    let risk = serde_json::from_value::<TaskRisk>(decision["risk"].clone()).ok();
    let modifiers =
        serde_json::from_value::<Vec<ExecutionModifier>>(decision["modifiers"].clone()).ok();
    let gates = serde_json::from_value::<Vec<ExecutionPolicyGate>>(decision["gates"].clone()).ok();
    if risk != Some(TaskRisk::Low)
        || modifiers.as_ref().is_none_or(|values| {
            values.iter().any(|value| {
                matches!(
                    value,
                    ExecutionModifier::WithVerifier | ExecutionModifier::WithReviewer
                )
            })
        })
        || gates
            .as_ref()
            .is_none_or(|values| values.contains(&ExecutionPolicyGate::Approval))
        || unsafe_call
        || root_plans
            .iter()
            .any(|plan| !safe_plans.contains(plan) || unsafe_plans.contains(plan))
    {
        return Ok(None);
    }
    let value = serde_json::json!({"review_policy_version":2,"root":root,"session":program.session_id,"turn":program.turn_id,
        "decision_id":decision["decision_id"],"risk":risk,"modifiers":modifiers,"gates":gates});
    Ok(Some(RootReviewPolicySnapshot {
        digest: format!("{:x}", Sha256::digest(value.to_string().as_bytes())),
        last_effect_cursor: 0,
        additional_sources: Vec::new(),
        source: ExpectedStreamRevision {
            stream_id,
            expected_revision: revision,
        },
    }))
}

/// The existing Session journal owns admitted writes and their outcomes. Reads
/// and Goal metadata may advance its revision without changing this digest.
pub(crate) fn effect_review_snapshot(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    effect_sources: &[String],
) -> Result<RootReviewPolicySnapshot, super::results::ReviewEvidenceError> {
    let stream_id = format!("session:{}", program.session_id);
    // A review that cites delegated effect sources is scoped to those streams.
    // The root Session stream is written by active coordinators and is not part
    // of the reviewed Task's effects, so fencing it made an independent review
    // fail whenever anything else in the Session moved. Only a review with no
    // delegated source (a root/objective-scope review) is fenced on the Session.
    let session_in_scope = effect_sources.is_empty();
    let mut digest = Sha256::new();
    digest.update(
        goal.map_or(program.objective_id.as_str(), |goal| goal.id.as_str())
            .as_bytes(),
    );
    let mut last_effect_cursor = 0;
    let mut planned_effects = BTreeSet::new();
    let mut terminal_calls = BTreeSet::new();
    let mut session_fence: Option<ExpectedStreamRevision> = None;
    let mut additional_sources = Vec::new();
    if session_in_scope {
        let revision = store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let created_cursor = if let Some(goal) = goal {
            store
                .list_stream_after(&format!("goal:{}", goal.id), 0, 1, 1, 4 * 1024 * 1024)?
                .into_iter()
                .next()
                .ok_or("effect review has no durable Goal genesis")?
                .commit_cursor
        } else {
            0
        };
        let mut through = revision;
        loop {
            let events = store.list_stream_before(&stream_id, through, 64, 1024 * 1024)?;
            if events.is_empty() {
                break;
            }
            through = events
                .last()
                .ok_or("empty proof page")?
                .sequence
                .checked_sub(1)
                .ok_or("invalid proof sequence")?;
            let mut before_goal = false;
            for event in events {
                if event.sequence > revision {
                    continue;
                }
                if event.commit_cursor < created_cursor {
                    before_goal = true;
                    continue;
                }
                if event.actor.as_deref() != Some("conversation_runtime") {
                    continue;
                }
                if matches!(
                    event.kind.as_str(),
                    "tool.invocation.completed"
                        | "tool.invocation.failed"
                        | "tool.invocation.denied"
                ) {
                    if let (Some(plan), Some(call)) = (
                        event.payload["governed_plan_id"].as_str(),
                        event.payload["tool_call_id"].as_str(),
                    ) {
                        terminal_calls.insert((plan.to_string(), call.to_string()));
                    }
                }
                if event.kind == "tool.execution_plan.created" {
                    if let (Some(plan), Some(tasks)) = (
                        event.payload["plan_id"].as_str(),
                        event.payload["tasks"].as_array(),
                    ) {
                        for task in tasks {
                            if !task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                                && serde_json::from_value::<ToolEffectKind>(
                                    task["effect"]["effect_kind"].clone(),
                                )
                                .ok()
                                    != Some(ToolEffectKind::Read)
                            {
                                if let Some(call) = task["tool_call_id"].as_str() {
                                    planned_effects.insert((plan.to_string(), call.to_string()));
                                }
                            }
                        }
                    }
                }
                let relevant = if event.kind == "tool.execution_plan.created" {
                    event.payload["tasks"].as_array().is_none_or(|tasks| {
                        tasks.iter().any(|task| {
                            !task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                                && serde_json::from_value::<ToolEffectKind>(
                                    task["effect"]["effect_kind"].clone(),
                                )
                                .ok()
                                    != Some(ToolEffectKind::Read)
                        })
                    })
                } else if event.kind.starts_with("tool.invocation.") {
                    !event.payload["tool_name"]
                        .as_str()
                        .is_some_and(goal_metadata_tool)
                        && !super::results::tool_effect_from_store(store, &event)?
                            .is_some_and(|effect| effect.effect_kind == ToolEffectKind::Read)
                } else {
                    false
                };
                if relevant {
                    let mut cuts = std::collections::BTreeMap::new();
                    let no_external_effect = if event.kind == "tool.execution_plan.created" {
                        let mut bounded_only = true;
                        for task in event.payload["tasks"]
                            .as_array()
                            .ok_or("effect review original tasks missing")?
                        {
                            if task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                                || task["effect"]["effect_kind"] == "read"
                            {
                                continue;
                            }
                            let step: crate::step_snapshot::StepCallBinding =
                                serde_json::from_value(task["step_binding"].clone())
                                    .map_err(|_| "effect review original Step missing")?;
                            if authenticated_process_for_key(
                                store,
                                &step.effect_idempotency_key(),
                                &mut cuts,
                            )?
                            .is_none()
                            {
                                bounded_only = false;
                            }
                        }
                        bounded_only
                    } else {
                        let events =
                            super::results::result_source_events(store, &stream_id, &mut cuts)?;
                        if let Some(task) = super::results::result_tool_plan(&events, &event)? {
                            let step: crate::step_snapshot::StepCallBinding =
                                serde_json::from_value(task["step_binding"].clone())
                                    .map_err(|_| "effect review original Step missing")?;
                            authenticated_process_for_key(
                                store,
                                &step.effect_idempotency_key(),
                                &mut cuts,
                            )?
                            .is_some()
                        } else {
                            false
                        }
                    };
                    for (stream_id, expected_revision) in cuts {
                        additional_sources.push(ExpectedStreamRevision {
                            stream_id,
                            expected_revision,
                        });
                    }
                    if !no_external_effect {
                        last_effect_cursor = last_effect_cursor.max(event.commit_cursor);
                    }
                    digest.update(event.event_id.as_bytes());
                    digest.update(event.payload.to_string().as_bytes());
                }
            }
            if before_goal {
                break;
            }
        }
        if store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?
            != revision
        {
            return Err("effect review source changed; retry current effects".into());
        }
        if !planned_effects.is_subset(&terminal_calls) {
            return Err(
            "effect_review_pending: admitted writes must settle before current target verification"
                .into(),
        );
        }
        session_fence = Some(ExpectedStreamRevision {
            stream_id: stream_id.clone(),
            expected_revision: revision,
        });
    }
    for source in effect_sources.iter().collect::<BTreeSet<_>>() {
        if !source.starts_with("execution-agent-receipts:") {
            return Err("effect review source is not a canonical Agent receipt stream".into());
        }
        let revision = store
            .stream_revision(source)
            .map_err(|error| error.to_string())?;
        let mut pending = BTreeSet::new();
        let mut completed = BTreeSet::new();
        digest.update(source.as_bytes());
        let mut through = revision;
        loop {
            let events = store.list_stream_before(source, through, 64, 1024 * 1024)?;
            if events.is_empty() {
                break;
            }
            through = events
                .last()
                .ok_or("empty proof page")?
                .sequence
                .checked_sub(1)
                .ok_or("invalid proof sequence")?;
            for event in events {
                if event.sequence > revision
                    || event.actor.as_deref() != Some("governed_tool")
                    || !matches!(
                        event.kind.as_str(),
                        "execution.agent_tool.intent" | "execution.agent_tool.receipt"
                    )
                    || event.payload["tool_name"]
                        .as_str()
                        .or_else(|| event.payload["outcome"]["tool_name"].as_str())
                        .is_some_and(goal_metadata_tool)
                    || serde_json::from_value::<ToolEffectKind>(
                        event.payload["effect_kind"].clone(),
                    )
                    .ok()
                        == Some(ToolEffectKind::Read)
                {
                    continue;
                }
                if let Some(key) = event.payload["idempotency_key"].as_str() {
                    if event.kind == "execution.agent_tool.intent" {
                        pending.insert(key.to_owned());
                    } else {
                        completed.insert(key.to_owned());
                    }
                } else if event.kind == "execution.agent_tool.intent" {
                    return Err("effect review intent has no canonical effect identity".into());
                }
                if event.payload["effect_kind"] == "process" {
                    let key = event.payload["idempotency_key"]
                        .as_str()
                        .ok_or("process effect review key missing")?;
                    let mut cuts = std::collections::BTreeMap::new();
                    let run = authenticated_process_for_key(store, key, &mut cuts)?
                        .ok_or("effect_review_pending: process original admission missing")?;
                    digest.update(serde_json::to_vec(&run).map_err(|error| error.to_string())?);
                    for (stream_id, expected_revision) in cuts {
                        additional_sources.push(ExpectedStreamRevision {
                            stream_id,
                            expected_revision,
                        });
                    }
                    digest.update(event.event_id.as_bytes());
                    digest.update(event.payload.to_string().as_bytes());
                    continue;
                }
                last_effect_cursor = last_effect_cursor.max(event.commit_cursor);
                digest.update(event.event_id.as_bytes());
                digest.update(event.payload.to_string().as_bytes());
            }
        }
        if store
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != revision
        {
            return Err("delegated effect source changed; retry current effects".into());
        }
        if !pending.is_subset(&completed) {
            return Err("effect_review_pending: delegated writes must settle before current target verification".into());
        }
        additional_sources.push(ExpectedStreamRevision {
            stream_id: source.clone(),
            expected_revision: revision,
        });
    }
    // The primary fence is the Session stream for a root-scope review, or the
    // first delegated effect source otherwise.
    let source = match session_fence {
        Some(fence) => fence,
        None => {
            let index = additional_sources
                .iter()
                .position(|source| source.stream_id.starts_with("execution-agent-receipts:"))
                .ok_or("effect review has no original receipt fence source")?;
            additional_sources.remove(index)
        }
    };
    Ok(RootReviewPolicySnapshot {
        additional_sources,
        digest: format!("{:x}", digest.finalize()),
        last_effect_cursor,
        source,
    })
}

#[cfg(test)]
mod tests {
    include!("policy_control_identity_tests.rs");
    use super::*;
    use harness_contract::{policy::*, tool::*};

    #[test]
    fn r1_review_control_plan_survives_the_effects_projection() {
        let store = RuntimeEventStore::for_test();
        let plan = |tool_name: &str, effect_kind: &str| crate::RuntimeEventInput {
            stream_id: "session:review-control-plan".into(),
            scope: RuntimeEventScope::Session,
            kind: "tool.execution_plan.created".into(),
            status: Some("planned".into()),
            actor: Some("conversation_runtime".into()),
            refs: vec![],
            payload: serde_json::json!({
                "plan_id": format!("plan-{tool_name}"),
                "tasks": [{
                    "tool_name": tool_name,
                    "tool_call_id": format!("call-{tool_name}"),
                    "effect": {"effect_kind": effect_kind},
                    "normalized_input": {},
                    "step_binding": {},
                }],
            }),
        };
        store.append(plan("objective_review", "write")).unwrap();
        store.append(plan("task_review", "write")).unwrap();
        store.append(plan("objective_update", "write")).unwrap();
        store.append(plan("read_file", "read")).unwrap();

        let mut sources = std::collections::BTreeMap::new();
        let effects =
            pinned_events(&store, "session:review-control-plan", &mut sources, 0).unwrap();
        let kept = effects
            .iter()
            .filter(|event| event.kind == "tool.execution_plan.created")
            .map(|event| {
                event.payload["tasks"][0]["tool_name"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kept,
            BTreeSet::from([
                "objective_review".to_string(),
                "task_review".to_string(),
            ]),
            "only plans carrying a review control call survive an otherwise metadata/read projection",
        );
    }

    #[test]
    fn r1_only_authenticated_runtime_control_effects_leave_the_effect_digest() {
        let control = "settled-runtime-control";
        let business = "ordinary-business-write";
        let unauthenticated_lookalike = "unauthenticated-control-lookalike";
        let mut required = std::collections::BTreeMap::from([
            (
                "control-intent".to_string(),
                serde_json::json!({"idempotency_key": control, "effect_kind": "write"}),
            ),
            (
                "control-receipt".to_string(),
                serde_json::json!({"idempotency_key": control, "effect_kind": "write"}),
            ),
            (
                "business-intent".to_string(),
                serde_json::json!({"idempotency_key": business, "effect_kind": "write"}),
            ),
            (
                "lookalike".to_string(),
                serde_json::json!({
                    "idempotency_key": unauthenticated_lookalike,
                    "effect_kind": "write",
                }),
            ),
            (
                "physical-envelope".to_string(),
                serde_json::json!({"status": "closed"}),
            ),
        ]);
        let controls = BTreeSet::from([control.to_string()]);

        strip_authenticated_runtime_control_effects(
            &mut required,
            &controls,
            &std::collections::BTreeMap::new(),
        );

        assert_eq!(
            required.keys().cloned().collect::<Vec<_>>(),
            vec![
                "business-intent".to_string(),
                "lookalike".to_string(),
                "physical-envelope".to_string(),
            ],
            "the manifest drops only identities already authenticated as controls",
        );
    }

    #[test]
    fn r1_authenticated_control_lifecycle_uses_original_plan_identity() {
        let mut required = std::collections::BTreeMap::from([
            (
                "own-start".into(),
                serde_json::json!({"tool_name":"task_withdraw"}),
            ),
            (
                "own-end".into(),
                serde_json::json!({"tool_name":"task_withdraw"}),
            ),
            (
                "foreign-end".into(),
                serde_json::json!({"tool_name":"task_withdraw"}),
            ),
            (
                "business-end".into(),
                serde_json::json!({"tool_name":"write_file"}),
            ),
        ]);
        let aliases = std::collections::BTreeMap::from([
            ("own-start".into(), "authenticated-control".into()),
            ("own-end".into(), "authenticated-control".into()),
            ("business-end".into(), "business-write".into()),
        ]);
        strip_authenticated_runtime_control_effects(
            &mut required,
            &BTreeSet::from(["authenticated-control".into()]),
            &aliases,
        );
        assert_eq!(
            required.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["business-end", "foreign-end"],
            "a control name grants no exemption; the authenticated original call does"
        );
    }

    #[test]
    fn r1_settled_delegated_plan_keeps_task_effects_not_the_session_envelope() {
        let plan = crate::DurableRuntimeEvent {
            event_id: "plan-event".into(),
            stream_id: "session:delegated".into(),
            sequence: 4,
            scope: RuntimeEventScope::Tool,
            kind: "tool.execution_plan.created".into(),
            status: Some("planned".into()),
            actor: Some("conversation_runtime".into()),
            refs: vec![],
            payload: serde_json::json!({
                "plan_id": "delegated-plan",
                "tasks": [
                    {
                        "tool_name": "task_submit",
                        "tool_call_id": "business-call",
                        "effect": {"effect_kind": "write"},
                    },
                    {
                        "tool_name": "objective_update",
                        "tool_call_id": "metadata-call",
                        "effect": {"effect_kind": "write"},
                    },
                    {
                        "tool_name": "read_file",
                        "tool_call_id": "read-call",
                        "effect": {"effect_kind": "read"},
                    },
                ],
            }),
            created_at_ms: 0,
            commit_cursor: 4,
            transaction_id: "tx".into(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        };

        let effects = delegated_plan_required_effects(&plan).unwrap();
        assert_eq!(
            effects.keys().cloned().collect::<Vec<_>>(),
            vec!["plan-event:business-call".to_string()],
        );
        assert_eq!(
            effects["plan-event:business-call"], plan.payload["tasks"][0],
            "the manifest carries the authenticated Task rather than the whole Session plan",
        );
    }

    /// The review cannot be required to settle the control action it is.
    ///
    /// A reviewer performs the review while the proof is assembled, so that
    /// call's intent is still in flight. Excluding it must be narrow: only
    /// that reviewer's own review call, never another agent's in-flight

    /// The review cannot be required to settle the control action it is.
    ///
    /// The exemption names ONE call, by the exact effect identity Runtime
    /// resolved for the action being applied. A second unfinished review in
    /// the same run - the same tool, the same reviewer, a different Step - has
    /// a different identity and keeps blocking. The old exclusion matched the
    /// tool name and the execution instead, so it skipped both of them.
    #[test]
    fn r1_only_the_applied_review_call_is_exempt() {
        let applied = "effect-identity-of-the-review-being-applied";
        let other = "effect-identity-of-the-second-unfinished-review";
        let settled = BTreeSet::from(["settled-control".to_string()]);
        assert!(is_authenticated_control_effect(Some(applied), Some(applied), &settled));
        assert!(is_authenticated_control_effect(Some("settled-control"), None, &settled));
        assert!(!is_authenticated_control_effect(Some(other), Some(applied), &settled));
        assert!(!is_authenticated_control_effect(Some(applied), None, &settled));
        assert!(!is_authenticated_control_effect(None, Some(applied), &settled));
        assert!(!is_authenticated_control_effect(Some(""), Some(""), &settled));

        // The review's own call is in flight because the review is performing
        // it, and it can never settle the action it is.
        assert_eq!(
            review_write_outcome(Some(applied), applied, false, false),
            ReviewWriteOutcome::Exempt
        );
        // The second unfinished review of the same run is not exempt.
        assert_eq!(
            review_write_outcome(Some(applied), other, false, false),
            ReviewWriteOutcome::Blocked
        );
        // A settled write, and a write that provably does not intersect the
        // reviewed work, are both settled.
        assert_eq!(
            review_write_outcome(Some(applied), other, true, false),
            ReviewWriteOutcome::Settled
        );
        assert_eq!(
            review_write_outcome(Some(applied), other, false, true),
            ReviewWriteOutcome::Settled
        );
        // Without a resolved identity nothing is exempt.
        assert_eq!(
            review_write_outcome(None, applied, false, false),
            ReviewWriteOutcome::Blocked
        );
    }

    /// The attempt is bound by the durable evidence, not by the Step.
    ///
    /// The native provider path does not carry the attempt in the Step
    /// identity, so the attempt comes from the receipt index stream, which
    /// names the graph, the node and the attempt. The same effect identity
    /// admitted in another attempt's stream is another call.
    #[test]
    fn r1_the_review_call_must_be_admitted_in_the_reviewers_own_attempt() {
        let identity = "effect-identity-of-the-review-being-applied";
        let reviewer_stream = "execution-agent-receipts:graph:node:2";
        let other_attempt = "execution-agent-receipts:graph:node:1";
        let other_node = "execution-agent-receipts:graph:other-node:2";
        let other_graph = "execution-agent-receipts:other-graph:node:2";

        assert!(is_review_own_admitted_call(
            Some(identity),
            Some(reviewer_stream),
            reviewer_stream,
            identity
        ));
        // Same call identity, another attempt: not the reviewer's own call.
        assert!(!is_review_own_admitted_call(
            Some(identity),
            Some(reviewer_stream),
            other_attempt,
            identity
        ));
        // Same call identity, another node or another graph.
        assert!(!is_review_own_admitted_call(
            Some(identity),
            Some(reviewer_stream),
            other_node,
            identity
        ));
        assert!(!is_review_own_admitted_call(
            Some(identity),
            Some(reviewer_stream),
            other_graph,
            identity
        ));
        // Another call's identity in the reviewer's own stream.
        assert!(!is_review_own_admitted_call(
            Some(identity),
            Some(reviewer_stream),
            reviewer_stream,
            "effect-identity-of-another-call"
        ));
        // Without a resolved identity, or without a trusted reviewer stream,
        // nothing is the reviewer's own call.
        assert!(!is_review_own_admitted_call(
            None,
            Some(reviewer_stream),
            reviewer_stream,
            identity
        ));
        assert!(!is_review_own_admitted_call(
            Some(identity),
            None,
            reviewer_stream,
            identity
        ));
    }
    #[test]
    fn physical_lineage_genesis_arriving_after_discovery_requires_fresh_cut() {
        use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
        let store = std::sync::Arc::new(RuntimeEventStore::for_test());
        let graph_id = "late-discovered-graph";
        let cut = store.durable_commit_cursor().unwrap();
        let writer = std::sync::Arc::clone(&store);
        store.inject_read_fault_once(graph_id.into(), "list_stream_after", 1, move |_| {
            let mut graph = ExecutionGraph::new("late graph at discovery boundary");
            graph.id = graph_id.into();
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: "lineage-session".into(), turn_id: "lineage-turn".into(),
                root_task_id: "lineage-root".into(), task_id: "lineage-task".into(), generation: 1,
            });
            crate::ExecutionCommitService::new(writer).register_graph(graph).unwrap();
        });
        let error = require_discovered_graph_genesis(&store, graph_id, cut).unwrap_err();
        assert_eq!(error, "proof physical lineage changed during discovery; retry current inputs");
        let fresh_cut = store.durable_commit_cursor().unwrap();
        assert!(fresh_cut > cut);
        require_discovered_graph_genesis(&store, graph_id, fresh_cut)
            .expect("the same durable genesis is visible at a fresh discovery cut");
    }

    #[test]
    fn pinned_stream_rejects_final_page_boundary_race_then_reads_stable_cut() {
        let store = RuntimeEventStore::for_test();
        // A source the review itself judges: growth on it is the object under
        // proof moving, so the fence stays an equality fence. The reviewed
        // Session stream is a shared aggregate stream and is covered by
        // `pinned_stream_accepts_shared_aggregate_growth_while_paging`.
        let source = "agentic-program:actual-final-page-race";
        let append = |store: &RuntimeEventStore| {
            store.append(crate::RuntimeEventInput {
                stream_id: source.into(), scope: RuntimeEventScope::Session,
                kind: "test.source-advance".into(), status: None, actor: None,
                refs: vec![], payload: serde_json::json!({}),
            }).unwrap();
        };
        append(&store);
        // The second revision read verifies the last non-empty page. Advance
        // after it returns, so only the final page-boundary fence detects it.
        store.inject_read_fault_once(source.into(), "stream_revision", 2, move |store| append(store));
        let error = pinned_stream_events(
            &store, source, &mut std::collections::BTreeMap::new(), 0,
            PinnedEventProjection::Effects,
        ).unwrap_err();
        assert_eq!(error, "proof source changed at final page boundary; retry current inputs");
        let mut stable_sources = std::collections::BTreeMap::new();
        pinned_stream_events(&store, source, &mut stable_sources, 0, PinnedEventProjection::Effects)
            .expect("a new read after the source stops advancing must succeed");
        assert_eq!(stable_sources[source], 2);
    }

    #[test]
    fn pinned_stream_rejects_real_paging_race_then_reads_stable_cut() {
        let store = RuntimeEventStore::for_test();
        // See `pinned_stream_rejects_final_page_boundary_race_then_reads_stable_cut`:
        // a judged source keeps the equality fence.
        let source = "agentic-program:actual-paging-race";
        let append = |store: &RuntimeEventStore| {
            store.append(crate::RuntimeEventInput {
                stream_id: source.into(), scope: RuntimeEventScope::Session,
                kind: "test.source-advance".into(), status: None, actor: None,
                refs: vec![], payload: serde_json::json!({}),
            }).unwrap();
        };
        append(&store);
        store.inject_read_fault_once(source.into(), "list_stream_before", 1, move |store| append(store));
        let error = pinned_stream_events(
            &store, source, &mut std::collections::BTreeMap::new(), 0,
            PinnedEventProjection::Effects,
        ).unwrap_err();
        assert_eq!(error, "proof source changed while paging; retry current inputs");
        let mut stable_sources = std::collections::BTreeMap::new();
        pinned_stream_events(&store, source, &mut stable_sources, 0, PinnedEventProjection::Effects)
            .expect("a fresh read after the source stops advancing must succeed");
        assert_eq!(stable_sources[source], 2);
    }

    /// A reader whose head is scripted, so a re-pin can observe a source that
    /// grew or moved backwards without a concurrent writer.
    struct ScriptedHead {
        heads: std::collections::VecDeque<u64>,
    }
    impl super::super::read_source::ReadAuthSource for ScriptedHead {
        fn stream_revision(&mut self, _: &str) -> Result<u64, String> {
            Ok(self.heads.pop_front().expect("scripted head"))
        }
        fn event_by_idempotency_key(&mut self, _: &str, _: &str)
            -> Result<Option<crate::DurableRuntimeEvent>, String> {
            Ok(None)
        }
        fn list_stream_after(&mut self, _: &str, _: u64, _: u64, _: usize, _: usize)
            -> Result<Vec<crate::DurableRuntimeEvent>, String> {
            Ok(Vec::new())
        }
        fn transaction_stream_revision_exact(&mut self, _: &str, _: &str)
            -> Result<Option<crate::CommittedStreamRevision>, String> {
            Ok(None)
        }
        fn resolve_graph_record(&mut self, _: &crate::DurableRuntimeEvent)
            -> Result<serde_json::Value, String> {
            Ok(serde_json::Value::Null)
        }
    }

    /// The re-pin judgment itself. Growth on a shared aggregate stream is
    /// admissible; a head that moved backwards never is; and a source this
    /// review judges keeps its equality fence.
    #[test]
    fn repin_admits_only_shared_aggregate_growth() {
        let pin = |reader: &mut ScriptedHead,
                   source: &str,
                   cuts: &mut std::collections::BTreeMap<String, u64>| {
            super::super::results::pin_result_source_with_reader(reader, source, cuts)
        };
        let mut session_cuts = std::collections::BTreeMap::new();
        let mut grown_session = ScriptedHead { heads: [5_u64, 7].into() };
        assert_eq!(pin(&mut grown_session, "session:one", &mut session_cuts).unwrap(), 5);
        assert_eq!(pin(&mut grown_session, "session:one", &mut session_cuts).unwrap(), 7);
        assert_eq!(session_cuts["session:one"], 7);

        let mut lineage_cuts = std::collections::BTreeMap::new();
        let mut grown_lineage = ScriptedHead { heads: [5_u64, 7].into() };
        assert_eq!(pin(&mut grown_lineage, "execution-lineage:one", &mut lineage_cuts).unwrap(), 5);
        assert_eq!(pin(&mut grown_lineage, "execution-lineage:one", &mut lineage_cuts).unwrap(), 7);
        assert_eq!(lineage_cuts["execution-lineage:one"], 7);

        let mut judged_cuts = std::collections::BTreeMap::new();
        let mut grown_judged = ScriptedHead { heads: [5_u64, 7].into() };
        pin(&mut grown_judged, "agentic-program:one", &mut judged_cuts).unwrap();
        let drift = pin(&mut grown_judged, "agentic-program:one", &mut judged_cuts).unwrap_err();
        // 032 K4b (A2): identity is the code, not the wording; the facts the
        // old verbatim assertion pinned are still asserted individually.
        assert_eq!(
            super::super::reason_code::reason_code_of(&drift.to_string()),
            Some(super::super::reason_code::ReviewRefusalCode::ResultSourceChangedDuringResolution),
            "the drift refusal is named by its own code: {drift}"
        );
        assert!(
            drift.contains("source=agentic-program:one expected=5 actual=7"),
            "the projection still carries the window facts: {drift}"
        );
        assert!(
            drift.contains("phase=result_source_pin"),
            "the projection still carries its phase: {drift}"
        );

        let mut backwards_cuts = std::collections::BTreeMap::new();
        let mut backwards = ScriptedHead { heads: [5_u64, 3].into() };
        pin(&mut backwards, "session:one", &mut backwards_cuts).unwrap();
        let error = pin(&mut backwards, "session:one", &mut backwards_cuts).unwrap_err();
        assert!(error.contains("phase=result_source_pin"), "{error}");
        assert!(error.contains("moved backwards"), "{error}");
        assert!(error.contains("expected=5 actual=3"), "{error}");
    }

    /// G-c skeleton: the delegated member fixture, end to end at function
    /// level. Asserts the member's read authenticates and carries the
    /// original input the same target-identity predicate reads.
    ///
    /// The fixture is the mechanical part: a content-addressed child graph
    /// holding the bound request, its lineage registration, and the
    /// primary/index receipt pair on the streams the member path reads.
    #[test]
    fn g_c_member_read_end_to_end() {
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let session = "gc-session";
        let turn = "gc-turn";
        let graph_id = "gc-graph";
        let node_id = "gc-node";
        let run_id = "run:gc-graph";
        let worker = "gc-worker";
        let principal = "gc-principal";
        let call_id = "gc-call";
        let step_id = "gc-step";

        let mut packet = harness_contract::agent::AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                worker,
                run_id,
                "task:gc",
                session,
                "mission:gc",
                Some("team:gc"),
                graph_id,
                node_id,
            ),
            attempt: 1,
            expected_graph_revision: 1,
            objective: "Read the durable record".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            cohort_prompt_package: None,
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: vec!["read_ref".into()],
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            policy_revision: 1,
            model_lease: "fixture".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "gc-budget",
                worker,
                "task:gc",
                1024,
                4_000_000_000_000,
                1,
            ),
            deadline_at_ms: 4_000_000_000_000,
            binding: None,
            managed_invocation: None,
            idempotency_key: "gc-packet".into(),
            agentic_binding: None,
        };
        packet.agentic_binding = Some(harness_contract::agent::AgenticExecutionBinding {
            program_id: "gc-program".into(),
            agent_id: worker.into(),
            membership_id: "membership:gc".into(),
            team_id: "team:gc".into(),
            task_team_id: "team:gc".into(),
            source_spec_revision: 1,
            task_purpose: None,
            focus: harness_contract::agent::AgenticExecutionFocus::TaskExecute {
                task_ref: "task:gc".into(),
            },
        });

        // Producer paths can refer to the same physical packet more than once.
        let source = format!("execution-agent-receipts:{}:{}:{}", packet.graph_id(), packet.node_id(), packet.attempt);
        let identical = vec![packet.clone(), packet.clone()];
        assert!(super::super::directory_read_packet(&identical, &source).is_ok());
        assert!(super::super::directory_read_packet(&identical, "execution-agent-receipts:missing").is_err());
        let mut changed = packet.clone();
        changed.objective.push_str(" changed");
        assert!(super::super::directory_read_packet(&[packet.clone(), changed], &source).is_err());
        let mut later = packet.clone();
        later.attempt += 1;
        assert!(super::super::directory_read_packet(&[later], &source).is_err());

        // The member's own read: a unified read of a durable record.
        let input = serde_json::json!({"reference": "artifact://art-gc"});
        let input_text = serde_json::to_string(&input).unwrap();
        let step: crate::step_snapshot::StepCallBinding = serde_json::from_value(
            serde_json::json!({
                "origin": {
                    "origin_step_id": step_id,
                    "call_id": call_id,
                    "catalog_digest": "gc-catalog",
                    "source": {"kind": "provider", "attempt_id": "gc-attempt"},
                },
                "snapshot": {
                    "contract_version": 1,
                    "identity": {
                        "step_id": step_id,
                        "session_id": session,
                        // The packet's admitted turn, which the member path
                        // binds the Step identity to.
                        "turn_id": packet.assignment.execution_identity.turn_id().unwrap_or(turn),
                        "execution_id": run_id,
                        // The packet's admitted principal, not a name of our
                        // own choosing: the member path binds the Step
                        // identity to it.
                        "actor_ref": packet.assignment.execution_identity.principal_id(),
                        "run_ref": run_id,
                        "attempt": 1,
                        "goal_ref": null,
                        "program_ref": null,
                        "task_ref": null,
                        "build_ref": "gc-fixture",
                    },
                    "provider_attempt": {
                        "attempt_id": "gc-attempt",
                        "origin_step_id": step_id,
                        "model_profile_digest": "gc-profile",
                        "request_digest": "gc-request",
                    },
                    "catalog_digest": "gc-catalog",
                    "tools": [],
                    "related_revisions": {},
                    "context_refs": [],
                    "expires_at_ms": 4_000_000_000_000i64,
                },
            }),
        )
        .unwrap();
        let key = step.effect_idempotency_key();
        let payload_digest =
            harness_contract::step::canonical_digest("tool-effect-payload", &input).unwrap();
        let input_sha256 = crate::context_evidence::read_sha256(input_text.as_bytes());

        // The descriptor the unified read facade declares: a File capability
        // face whatever the read actually named.
        let effect = ToolEffectDescriptor {
            tool_id: "read_ref".into(),
            descriptor_hash: "gc-descriptor".into(),
            effect_kind: ToolEffectKind::Read,
            idempotency: harness_contract::tool::ToolIdempotency::Idempotent,
            scopes: vec![harness_contract::policy::PermissionScope {
                resource: harness_contract::policy::PermissionResource::File,
                operation: harness_contract::policy::PermissionOperation::Read,
                target: Some(".".into()),
            }],
            required_permission: harness_contract::tool::ToolPermissionMode::ReadOnly,
            approval_class: harness_contract::tool::ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        };
        let lease = harness_contract::policy::AuthorizationLease {
            lease_id: "gc-lease".into(),
            principal_id: principal.into(),
            parent_lease_id: None,
            capability: "read_ref".into(),
            scopes: effect.scopes.clone(),
            ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            issued_at_ms: 0,
            expires_at_ms: 4_000_000_000_000,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: "gc-lease-key".into(),
            policy_revision: 1,
            effect_descriptor_hash: effect.descriptor_hash.clone(),
            signature: "gc-signature".into(),
            status: harness_contract::policy::AuthorizationLeaseStatus::Active,
        };
        let authorization = harness_contract::tool::ToolExecutionAuthorization {
            request_id: "gc-request".into(),
            tool_id: "read_ref".into(),
            descriptor_hash: effect.descriptor_hash.clone(),
            policy_revision: 1,
            scope: effect.scopes[0].clone(),
            authorization_lease: lease,
            timeout_lease: "gc-timeout".into(),
            idempotency_key: None,
        };
        let invocation = harness_contract::tool::GovernedToolInvocation {
            origin: Some(step.origin.clone()),
            contract_version: 1,
            invocation_id: call_id.into(),
            intent: harness_contract::tool::ToolIntent {
                invocation_id: call_id.into(),
                tool_name: "read_ref".into(),
                normalized_input: input.clone(),
            },
            effect: effect.clone(),
            resource_demand: Default::default(),
            explicit_dependencies: Vec::new(),
            compiled_dependencies: Vec::new(),
            catalog_revision: 1,
            descriptor_set_hash: "gc-descriptor-set".into(),
            idempotency_key: key.clone(),
        };
        let request: crate::tool_host::RuntimeToolExecutionRequest =
            serde_json::from_value(serde_json::json!({
                "compiled_invocation": invocation,
                "step_binding": step,
                "governed_plan_id": "gc-plan",
                "governed_plan_revision": 1,
                "observation_wave_sequence": 1,
                "idempotency_key": key,
                "tool_use_id": call_id,
                "tool_name": "read_ref",
                "input": input_text,
                "category": "read_only",
                "authorization": authorization,
                "session_id": session,
                "sandbox_posture": "workspace_write_sandbox",
                "policy_revision": 1,
                "authorized_scopes": [],
                "parent_execution": {
                    "execution_id": graph_id,
                    "node_id": node_id,
                },
                "parent_execution_attempt": packet.attempt,
            }))
            .unwrap();

        // The parent graph the packet was admitted under: the real member
        // path loads it before it reads the member's receipts.
        let mut parent_graph = harness_contract::execution_graph::ExecutionGraph::new("gc parent");
        parent_graph.id = graph_id.into();
        parent_graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: session.into(),
            turn_id: packet
                .assignment
                .execution_identity
                .turn_id()
                .unwrap_or(turn)
                .to_string(),
            root_task_id: "task:gc".into(),
            task_id: "task:gc".into(),
            generation: 1,
        });
        let mut parent_node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::AgentTask,
            "agent_task",
            serde_json::to_string(&packet).unwrap(),
        );
        parent_node.id = node_id.into();
        parent_graph.nodes.push(parent_node);
        crate::ExecutionCommitService::new(std::sync::Arc::clone(&store))
            .register_graph(parent_graph)
            .unwrap();

        // The bound child graph, named by the digest of its own payload.
        // The bound payload is the production persisted encoding, not raw
        // JSON: the decoder reads exactly what the encoder wrote.
        let bound_text = crate::execution_core::graph::executors::agent_tool::encode_bound_agent_tool(
            worker,
            run_id,
            request,
        )
        .unwrap();
        let bound_digest = format!("{:x}", {
            use sha2::Digest;
            sha2::Sha256::digest(bound_text.as_bytes())
        });
        let child_graph_id = format!("agent-tool-batch:{bound_digest}");
        let child_node_id = format!("agent-tool:{bound_digest}");
        let mut child = harness_contract::execution_graph::ExecutionGraph::new("gc child");
        child.id = child_graph_id.clone();
        child.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: graph_id.into(),
            node_id: node_id.into(),
        });
        child.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: session.into(),
            turn_id: packet
                .assignment
                .execution_identity
                .turn_id()
                .unwrap_or(turn)
                .to_string(),
            root_task_id: "task:gc".into(),
            task_id: "task:gc".into(),
            generation: 1,
        });
        let mut child_node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
            "tool_batch",
            bound_text.clone(),
        );
        child_node.id = child_node_id.clone();
        child.nodes.push(child_node);
        crate::ExecutionCommitService::new(std::sync::Arc::clone(&store))
            .register_graph(child)
            .unwrap();
        store
            .append(crate::RuntimeEventInput {
                stream_id: format!("execution-lineage:{graph_id}"),
                scope: RuntimeEventScope::Relation,
                kind: "execution.lineage.child_registered.v1".into(),
                status: Some("registered".into()),
                actor: Some("execution_commit_service".into()),
                refs: vec![],
                payload: serde_json::json!({
                    "parent_execution_id": graph_id,
                    "parent_node_id": node_id,
                    "child_execution_id": child_graph_id,
                    "child_objective": "gc child",
                }),
            })
            .unwrap();

        // The primary/index receipt pair on the streams the member path reads.
        let outcome = serde_json::json!({
            "status": "executed",
            "tool_use_id": call_id,
            "tool_name": "read_ref",
            "output": "durable record body",
        });
        // The replay carries the compiled invocation with its normalized
        // input withheld, exactly as the producer stores it.
        let mut stored_invocation = invocation.clone();
        stored_invocation.intent.normalized_input = serde_json::Value::Null;
        let stored_invocation = serde_json::to_value(&stored_invocation).unwrap();
        let replay = serde_json::json!({
            "session_id": session,
            "parent_execution": {"execution_id": graph_id, "node_id": node_id},
            "origin": step.origin,
            "call_id": call_id,
            "invocation": stored_invocation,
            "payload_digest": payload_digest,
        });
        let primary_payload = serde_json::json!({
            "idempotency_key": key,
            "tool_use_id": call_id,
            "tool_name": "read_ref",
            "input_sha256": input_sha256,
            "outcome": outcome.clone(),
            "replay": replay,
        });
        let index_payload = serde_json::json!({
            "idempotency_key": key,
            "effect_kind": "read",
            "outcome": outcome,
            "authorized_scopes": [],
            "effect_scope": authorization.scope,
            "sequence": 1,
        });
        let receipt = store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: format!("gc-receipt:{key}"),
                expected_streams: vec![
                    crate::ExpectedStreamRevision {
                        stream_id: format!("execution-effect:{key}"),
                        expected_revision: 0,
                    },
                    crate::ExpectedStreamRevision {
                        stream_id: format!(
                            "execution-agent-receipts:{graph_id}:{node_id}:{}",
                            packet.attempt
                        ),
                        expected_revision: 0,
                    },
                ],
                events: vec![
                    crate::RuntimeTransactionEventInput {
                        event: crate::RuntimeEventInput {
                            stream_id: format!("execution-effect:{key}"),
                            scope: RuntimeEventScope::ExecutionNode,
                            kind: "execution.read.receipt".into(),
                            status: Some("completed".into()),
                            actor: Some("governed_tool".into()),
                            refs: vec![],
                            payload: primary_payload,
                        },
                        idempotency_key: Some(format!("{key}:read-receipt")),
                        schema_version: 1,
                    },
                    crate::RuntimeTransactionEventInput {
                        event: crate::RuntimeEventInput {
                            stream_id: format!(
                                "execution-agent-receipts:{graph_id}:{node_id}:{}",
                                packet.attempt
                            ),
                            scope: RuntimeEventScope::ExecutionNode,
                            kind: "execution.agent_tool.receipt".into(),
                            status: Some("completed".into()),
                            actor: Some("governed_tool".into()),
                            refs: vec![],
                            payload: index_payload,
                        },
                        idempotency_key: Some(format!("agent-tool-receipt:{key}")),
                        schema_version: 1,
                    },
                ],
            })
            .unwrap();
        let _ = receipt;
        let events = store
            .list_stream(&format!("execution-effect:{key}"))
            .unwrap();
        let primary = events
            .iter()
            .find(|event| event.kind == "execution.read.receipt")
            .unwrap()
            .clone();
        let index = store
            .list_stream(&format!(
                "execution-agent-receipts:{graph_id}:{node_id}:{}",
                packet.attempt
            ))
            .unwrap()
            .iter()
            .find(|event| event.kind == "execution.agent_tool.receipt")
            .unwrap()
            .clone();

        verify_actual_read_pair(&primary, &index).expect("original Read pair");
        let mut foreign = index.clone();
        foreign.transaction_id.push_str("-foreign");
        assert!(verify_actual_read_pair(&primary, &foreign).is_err());
        let mut changed = index.clone();
        changed.payload["outcome"]["status"] = serde_json::json!("failed");
        assert!(verify_actual_read_pair(&primary, &changed).is_err());

        // The member path authenticates the read's original input from the
        // child request, and that input is what the shared predicate reads.
        let mut actual = ActualInputSnapshot {
            file_dependencies: Vec::new(),
            immutable_inputs: Vec::new(),
            sources: std::collections::BTreeMap::new(),
            material_events: Vec::new(),
            canonical_facts: Vec::new(),
        };
        let member_input = authenticate_unsealed_delegated_input(
            &store,
            &primary,
            &index,
            &packet,
            &mut actual,
        )
        .expect("the member read authenticates its original input");
        assert_eq!(member_input["reference"], "artifact://art-gc");

        let body = serde_json::json!({
            "outcome": {"status": "executed", "output": "original body"},
            "outcome_truncated": false,
        });
        let receipt_of = |payload: &serde_json::Value| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: "session:gc-sealed".into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.read.receipt".into(),
                    status: Some("completed".into()),
                    actor: Some("governed_tool".into()),
                    refs: vec![],
                    payload: payload.clone(),
                })
                .unwrap()
        };

        // G-c-P1: the member's read of a durable record carries no workspace
        // extent, so the actual-input acquisition settles with the immutable
        // receipt fact instead of being refused.
        //
        // The settlement runs through the member path itself. The boundary
        // names the consumer event that path authenticates, so the assertion
        // witnesses the wiring that reads the member's original input - not
        // the shared predicate alone.
        let mut program = AgenticProgramProjection::empty("gc-program", "gc-objective");
        program.session_id = session.into();
        let consumer = store
            .append(crate::RuntimeEventInput {
                stream_id: format!("session:{session}"),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.effect.intent".into(),
                status: Some("completed".into()),
                actor: Some("governed_tool".into()),
                refs: vec![],
                payload: serde_json::json!({
                    "idempotency_key": key,
                    "replay": {
                        "origin": step.origin,
                        "session_id": session,
                        "call_id": call_id,
                        "parent_execution": {"execution_id": graph_id, "node_id": node_id},
                    },
                }),
            })
            .unwrap();
        let boundary = super::super::results::ResultInputBoundary {
            producer: format!("agent-run:{run_id}"),
            packet: Some(packet.clone()),
            step: None,
            cursor: consumer.commit_cursor,
            call_key: key.clone(),
            source: harness_contract::goal::ProofInputEvent {
                stream_id: consumer.stream_id.clone(),
                sequence: consumer.sequence,
                event_id: consumer.event_id.clone(),
                payload_digest: super::super::proof_value_digest(&consumer.payload),
            },
        };
        let mut windows = std::collections::BTreeMap::new();
        crate::agentic::fact_admission::take_test_admissions();
        let settled = super::actual_input_snapshot_for_phase(
            &store,
            &program,
            std::slice::from_ref(&boundary),
            super::super::ResultContentReaders {
                artifacts: None,
                history: None,
                process_inputs: None,
            },
            None,
            false,
            None,
        )
        .expect("a member read of a durable record has no workspace extent");
        assert!(crate::agentic::fact_admission::take_test_admissions().contains(
            &harness_contract::refusal_codes::FactAdmissionStrategy::Immutable),
            "the original immutable read must pass the admission gate");
        assert!(
            settled
                .canonical_facts
                .iter()
                .any(|fact| fact["grade"] == "acquired_immutable_receipt"),
            "the member read of a durable record settles as an immutable receipt: {:?}",
            settled.canonical_facts
        );

        // Classifier-only controls; original request authentication is exercised above.
        for (input, should_succeed) in [
            (Some(serde_json::json!({"reference":"task://task:gc"})), true),
            (Some(serde_json::json!({"reference":"task://"})), false),
            (None, false),
        ] {
            let mut classified = ActualInputSnapshot {
                file_dependencies: Vec::new(), immutable_inputs: Vec::new(),
                sources: std::collections::BTreeMap::new(), material_events: Vec::new(), canonical_facts: Vec::new(),
            };
            let mut classified_windows = std::collections::BTreeMap::new();
            let result = collect_actual_acquisition(&store, &program, &primary, &index,
                input.as_ref(), &[], &boundary,
                super::super::ResultContentReaders { artifacts: None, history: None, process_inputs: None },
                &mut classified, &mut classified_windows);
            assert_eq!(result.is_ok(), should_succeed);
            assert!(classified.immutable_inputs.is_empty() && classified.canonical_facts.is_empty());
            assert!(classified_windows.is_empty());
        }

        // G-c-N2: the same member read, judged as a workspace path, is still
        // refused - the extension did not turn a workspace read into a record.
        let mut refused = ActualInputSnapshot {
            file_dependencies: Vec::new(),
            immutable_inputs: Vec::new(),
            sources: std::collections::BTreeMap::new(),
            material_events: Vec::new(),
            canonical_facts: Vec::new(),
        };
        let capture = super::super::t1_frame_cleanup_tests::EventCapture::default();
        let trace_events = std::sync::Arc::clone(&capture.0);
        let rejection = tracing::subscriber::with_default(capture, || collect_actual_acquisition(
                &store,
                &program,
                &primary,
                &index,
                Some(&serde_json::json!({"reference": "file://workspace/report.txt"})),
                &[],
                &boundary,
                super::super::ResultContentReaders {
                    artifacts: None,
                    history: None,
                    process_inputs: None,
                },
                &mut refused,
                &mut windows,
            ))
            .unwrap_err();
        let trace = trace_events.lock().unwrap().join("\n");
        assert!(!trace.contains("file://workspace/report.txt"));
        // The refusal is durable now. The disposition and the frame ownership
        // are named in the record, on their own stream: the log line is gone,
        // but nothing about the refusal became less diagnosable.
        let refusal_stream = format!(
            "agentic-review-acquisition-rejected:{}",
            program.program_id
        );
        let refusal_events = store.list_stream(&refusal_stream).unwrap();
        assert_eq!(
            refusal_events.len(),
            1,
            "the refusal is persisted once on its own stream: {refusal_events:?}"
        );
        let refusal_record = &refusal_events[0];
        assert_eq!(refusal_record.kind, "agentic.review_acquisition_rejected");
        assert_eq!(
            refusal_record.payload["disposition"],
            "incomplete_snapshot_not_published"
        );
        assert_eq!(refusal_record.payload["frame_cleanup"], "no_frame");

        // The same rejected acquisition with an original frame must remain
        // an error; the frame, not a published snapshot, owns its cleanup.
        let before_events = store.all_events(10_000).unwrap();
        let frame = super::super::ProcessInputFrame::without_process_verifier();
        let capture = super::super::t1_frame_cleanup_tests::EventCapture::default();
        let frame_trace = std::sync::Arc::clone(&capture.0);
        let mut with_frame = ActualInputSnapshot {
            file_dependencies: Vec::new(), immutable_inputs: Vec::new(),
            sources: std::collections::BTreeMap::new(), material_events: Vec::new(), canonical_facts: Vec::new(),
        };
        let mut frame_windows = std::collections::BTreeMap::new();
        let frame_error = tracing::subscriber::with_default(capture, || collect_actual_acquisition(
            &store, &program, &primary, &index,
            Some(&serde_json::json!({"reference": "file://workspace/report.txt"})), &[], &boundary,
            super::super::ResultContentReaders { artifacts: None, history: None, process_inputs: Some(&frame) },
            &mut with_frame, &mut frame_windows,
        )).unwrap_err();
        assert_eq!(frame_error.to_string(), rejection.to_string());
        assert!(frame_error.review_ticket("t1-control", 1).is_none());
        assert!(rejection.review_ticket("t1-control", 1).is_none());
        assert!(with_frame.canonical_facts.is_empty() && frame_windows.is_empty());
        let after_events = store.all_events(10_000).unwrap();
        let count_on = |events: &[crate::DurableRuntimeEvent], stream: &str| {
            events.iter().filter(|event| event.stream_id == stream).count()
        };
        let mut changed = Vec::new();
        for stream in before_events
            .iter()
            .map(|event| event.stream_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
        {
            if stream != refusal_stream
                && count_on(&before_events, &stream) != count_on(&after_events, &stream)
            {
                changed.push(stream);
            }
        }
        assert!(
            changed.is_empty(),
            "a refused local snapshot publishes nothing outside its own refusal stream: {changed:?}"
        );
        let refusal_events = store.list_stream(&refusal_stream).unwrap();
        assert!(
            refusal_events
                .iter()
                .all(|event| event.payload["disposition"]
                    == "incomplete_snapshot_not_published"),
            "every refusal record names the disposition: {refusal_events:?}"
        );
        assert!(
            refusal_events
                .iter()
                .any(|event| event.payload["frame_cleanup"] == "no_frame"),
            "the frame-less refusal is persisted: {refusal_events:?}"
        );
        let frame_trace = frame_trace.lock().unwrap().join("\n");
        assert!(!frame_trace.contains("file://workspace/report.txt"));
        let frame_events = store.list_stream(&refusal_stream).unwrap();
        let frame_record = frame_events.last().unwrap();
        assert!(
            frame_events
                .iter()
                .any(|event| event.payload["frame_cleanup"] == "no_frame"),
            "the frame-less refusal is persisted too: {frame_events:?}"
        );
        assert_eq!(
            frame_record.payload["disposition"],
            "incomplete_snapshot_not_published"
        );
        assert!(
            frame_events
                .iter()
                .any(|event| event.payload["frame_cleanup"] == "owned_by_frame"),
            "the frame-owned refusal is persisted: {frame_events:?}"
        );
        // 034 GB-1 (batch 1 acceptance): the fragmented gate's original
        // wording stays verbatim inside the gate's envelope; the envelope adds
        // the code and what each proof strategy was missing.
        assert!(
            rejection.starts_with(
                "proof_actual_input_extent_unsupported: actual read lacks an original physical extent"
            ),
            "the original refusal wording stays verbatim at the head: {rejection}"
        );
        assert!(rejection.contains("code=fact_unproven"), "{rejection}");
        for strategy in ["nested", "discovery", "immutable"] {
            assert!(
                rejection.contains(&format!("{strategy}(missing=")),
                "the diagnosis names every strategy: {rejection}"
            );
        }
        assert!(
            refused.canonical_facts.is_empty(),
            "a refused member read settles nothing"
        );

        // A directory discovery is an authenticated historical operation,
        // never a FileLines dependency or immutable content.
        let mut discovery_effect = effect.clone();
        discovery_effect.tool_id = "glob_search".into();
        let mut target = actual_dependency_fixture().target;
        target.path.object_kind = harness_contract::context::WorkspaceObjectKind::Directory;
        target.exact = false;
        target.alias_checked = false;
        target.physical_identity = None;
        target.declared_scope = discovery_effect.scopes[0].clone();
        let scope = harness_contract::goal::ProofEffectScope {
            version: 2, descriptor_hash: discovery_effect.descriptor_hash.clone(),
            complete: true, targets: vec![target],
            scope_purposes: vec![harness_contract::goal::ProofEffectScopePurpose {
                declared_scope: discovery_effect.scopes[0].clone(),
                purpose: harness_contract::goal::ProofEffectPurpose::InputRead,
            }],
        };
        let page = serde_json::json!({"basePath":".", "pattern":"*.rs", "scanComplete":true,
            "durationMs":1, "numFiles":1, "filenames":["a.rs"], "truncated":false,
            "coverage":{"consistency":"live_filesystem"}});
        let mut glob_primary = primary.clone();
        let mut glob_index = index.clone();
        glob_primary.payload["tool_name"] = serde_json::json!("glob_search");
        glob_primary.payload["effect"] = serde_json::to_value(&discovery_effect).unwrap();
        glob_primary.payload["replay"]["invocation"]["effect"] = serde_json::to_value(&discovery_effect).unwrap();
        glob_primary.payload["outcome"]["tool_name"] = serde_json::json!("glob_search");
        glob_primary.payload["outcome"]["output"] = serde_json::json!(page.to_string());
        glob_primary.payload["outcome_truncated"] = serde_json::json!(false);
        glob_primary.payload["resolved_effect_scope"] = serde_json::to_value(&scope).unwrap();
        glob_index.payload["outcome"] = glob_primary.payload["outcome"].clone();
        glob_index.payload["effect"] = glob_primary.payload["effect"].clone();
        glob_index.payload["resolved_effect_scope"] = glob_primary.payload["resolved_effect_scope"].clone();
        let mut discovery_actual = ActualInputSnapshot {
            file_dependencies: Vec::new(), immutable_inputs: Vec::new(),
            sources: std::collections::BTreeMap::new(), material_events: Vec::new(), canonical_facts: Vec::new(),
        };
        let mut discovery_windows = std::collections::BTreeMap::new();
        let discovery_input = serde_json::json!({"pattern":"*.rs"});
        crate::agentic::fact_admission::take_test_admissions();
        collect_actual_acquisition(&store, &program, &glob_primary, &glob_index,
            Some(&discovery_input), &[], &boundary,
            super::super::ResultContentReaders { artifacts: None, history: None, process_inputs: None },
            &mut discovery_actual, &mut discovery_windows).expect("original discovery is authenticated");
        assert!(crate::agentic::fact_admission::take_test_admissions().iter().any(|strategy| strategy.code() == "discovery"),
            "authenticated discovery must pass the admission gate");
        assert!(discovery_actual.canonical_facts.is_empty());
        assert_eq!(discovery_actual.material_events.len(), 2);
        assert!(discovery_windows.is_empty());
        assert!(discovery_actual.immutable_inputs.is_empty());
        assert!(original_glob_discovery(&glob_primary, &discovery_effect,
            Some(&serde_json::json!({"pattern":"different"}))).is_err());
        glob_primary.payload["outcome_truncated"] = serde_json::json!(true);
        assert!(original_glob_discovery(&glob_primary, &discovery_effect, Some(&discovery_input)).is_err());

        // A composite without the original complete child ledger cannot be
        // treated as an empty or successful acquisition.
        let mut incomplete_primary = primary.clone();
        incomplete_primary.payload["tool_name"] = serde_json::json!("tool_orchestrate");
        incomplete_primary.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD] =
            serde_json::json!([{"step_index":0,"position":0}]);
        incomplete_primary.payload["child_read_acquisitions_complete"] = serde_json::json!(false);
        let mut incomplete_index = index.clone();
        incomplete_index.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD] =
            incomplete_primary.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD].clone();
        incomplete_index.payload["child_read_acquisitions_complete"] = serde_json::json!(false);
        assert!(collect_actual_nested_acquisitions(&store, &incomplete_primary, &incomplete_index,
            None, &[], &boundary, super::super::ResultContentReaders {
                artifacts: None, history: None, process_inputs: None,
            }, &mut discovery_actual, &mut discovery_windows).is_err());
        assert!(discovery_windows.is_empty());

        // G-c-N3: the red lines. A read that carries an acquisition seal is
        // judged by that seal, never as an immutable receipt; and a read whose
        // original input cannot be resolved keeps the stricter declared
        // capability judgment rather than being waved through.
        let sealed = serde_json::json!({
            "outcome": {"status": "executed", "output": "original body"},
            "outcome_truncated": false,
            "read_acquisition_v1": {"seal": "present"},
        });
        assert!(
            !super::immutable_receipt_without_workspace_extent(
                &receipt_of(&sealed),
                &effect,
                Some(&member_input),
            )
            .unwrap(),
            "a sealed read is judged by its acquisition, not as an immutable receipt"
        );
        assert!(
            !super::immutable_receipt_without_workspace_extent(&receipt_of(&body), &effect, None)
                .unwrap(),
            "an unresolved original input keeps the declared capability judgment"
        );
    }

    /// E-s: the refusal twin is addressed by the call's own effect key, and
    /// a batch member's twin by the batch's outer key, because the member's
    /// own key names no member index on that stream.
    ///
    /// The twin settles the call only when it names the program event it
    /// belongs to and the two share one transaction and one receipt.
    /// The outcome status set is closed: the two statuses the existing
    /// settled rule owns, the one this review settles as a terminal failure,
    /// and nothing else. A status outside the set is named, never guessed.
    #[test]
    fn f24_the_outcome_status_table_is_a_closed_set() {
        assert!(matches!(
            super::terminal_outcome_class(Some("executed")),
            Ok(super::OutcomeClass::Succeeded)
        ));
        assert!(matches!(
            super::terminal_outcome_class(Some("blocked_permission")),
            Ok(super::OutcomeClass::Succeeded)
        ));
        assert!(matches!(
            super::terminal_outcome_class(Some("failed")),
            Ok(super::OutcomeClass::Failed)
        ));
        assert!(matches!(
            super::terminal_outcome_class(None),
            Ok(super::OutcomeClass::InFlight)
        ));
        // Anything outside the set is named, not guessed.
        let unknown = super::terminal_outcome_class(Some("partial")).unwrap_err();
        assert!(unknown.contains("partial"), "{unknown}");
    }

    /// The refusal twin is addressed by the call's own effect key, and a
    /// batch member's twin by the batch's outer key, because the member's
    /// own key names no member index on that stream.
    ///
    /// A member's twin must also stand for this intent's own member: read
    /// through the outer key, another member's twin is not this call's fact.
    #[test]
    fn f24_the_refusal_twin_is_addressed_by_effect_key_and_by_outer_key() {
        let store = std::sync::Arc::new(RuntimeEventStore::for_test());
        let write = |store: &std::sync::Arc<RuntimeEventStore>,
                     stream_key: &str,
                     outer_key: &str,
                     idem: &str,
                     member_index: serde_json::Value,
                     member_identity: serde_json::Value| {
            let program_stream = "agentic-program:f24-program";
            store
                .append_transaction(crate::AppendTransactionRequest {
                    transaction_id: format!("f24-refusal:{idem}"),
                    // The writer declares the current revision of every
                    // stream it writes, the way the rejection writer does;
                    // the store refuses a transaction that does not.
                    expected_streams: vec![
                        crate::ExpectedStreamRevision {
                            stream_id: "agentic-program:f24-program".to_string(),
                            expected_revision: store
                                .stream_revision("agentic-program:f24-program")
                                .expect("program stream revision"),
                        },
                        crate::ExpectedStreamRevision {
                            stream_id: format!("execution-effect:{stream_key}"),
                            expected_revision: store
                                .stream_revision(&format!("execution-effect:{stream_key}"))
                                .expect("effect stream revision"),
                        },
                    ],
                    events: vec![
                        crate::RuntimeTransactionEventInput {
                            event: crate::RuntimeEventInput {
                                stream_id: program_stream.to_string(),
                                scope: RuntimeEventScope::Program,
                                kind: "agentic.completion_rejected".into(),
                                status: Some("rejected".into()),
                                actor: Some("runtime.agentic_action".into()),
                                refs: vec![],
                                payload: serde_json::json!({
                                    "program_id": "f24-program",
                                    "request_action_id": stream_key,
                                    "code": "call_refused_after_admission",
                                }),
                            },
                            // One refusal fact, one program event key.
                            idempotency_key: Some(format!("completion-rejected:{idem}")),
                            schema_version: 1,
                        },
                        crate::RuntimeTransactionEventInput {
                            event: crate::RuntimeEventInput {
                                stream_id: format!("execution-effect:{stream_key}"),
                                scope: RuntimeEventScope::ExecutionNode,
                                kind: "agentic.batch_member.index".into(),
                                status: Some("committed".into()),
                                actor: Some("runtime.agentic_action".into()),
                                refs: vec![],
                                payload: serde_json::json!({
                                    "outer_key": outer_key,
                                    "member_index": member_index,
                                    "member_identity": member_identity,
                                    "ticket": serde_json::Value::Null,
                                    "program_stream": program_stream,
                                    "program_event_kind": "agentic.completion_rejected",
                                    "program_event_key": format!("completion-rejected:{idem}"),
                                    "program_event_index": 0,
                                }),
                            },
                            idempotency_key: Some(idem.to_string()),
                            schema_version: 1,
                        },
                    ],
                })
                .expect("the refusal fact");
        };
        // A single call's twin: addressed by the call's own effect key.
        let key = "f24-effect-key";
        write(
            &store,
            key,
            key,
            &format!("agentic-single:{key}"),
            serde_json::Value::Null,
            serde_json::Value::Null,
        );
        assert!(
            matches!(
                super::refused_call_is_settled(&store, key, &mut std::collections::BTreeMap::new())
                    .expect("settled"),
                super::TerminalFactRead::Found { .. }
            ),
            "a single call's twin settles it by its own effect key"
        );
        // A call with no twin of its own is a miss, not a failure.
        assert!(
            matches!(
                super::refused_call_is_settled(
                    &store,
                    "f24-effect-key-absent",
                    &mut std::collections::BTreeMap::new(),
                )
                .expect("miss"),
                super::TerminalFactRead::NotFound
            ),
            "a call without a twin of its own is a miss"
        );
        // A batch member's twin: addressed by the batch's outer key.
        let outer = "f24-outer-key";
        let member = format!("{outer}:7");
        write(
            &store,
            outer,
            outer,
            "agentic-member:7",
            serde_json::json!(7),
            serde_json::json!("f24-member-identity"),
        );
        // A twin that sits under another member's index but names this
        // member's ordinal is not this intent's fact either.
        write(
            &store,
            outer,
            outer,
            "agentic-member:8",
            serde_json::json!(7),
            serde_json::json!("f24-member-identity"),
        );
        assert!(
            store
                .event_by_idempotency_key(
                    &format!("execution-effect:{member}"),
                    "agentic-member:7",
                )
                .expect("member stream lookup")
                .is_none(),
            "the member's own key names no member index on the outer stream"
        );
        assert!(
            matches!(
                super::refused_call_is_settled(&store, &member, &mut std::collections::BTreeMap::new())
                    .expect("settled"),
                super::TerminalFactRead::Found { .. }
            ),
            "a batch member's twin settles it by the batch's outer key"
        );
        // A member with no twin of its own is a miss.
        let other = format!("{outer}:9");
        assert!(
            matches!(
                super::refused_call_is_settled(&store, &other, &mut std::collections::BTreeMap::new())
                    .expect("read"),
                super::TerminalFactRead::NotFound
            ),
            "a member with no twin of its own is a miss"
        );
        // A twin that names another member is not this intent's fact.
        let other = format!("{outer}:8");
        assert!(
            matches!(
                super::refused_call_is_settled(&store, &other, &mut std::collections::BTreeMap::new())
                    .expect("read"),
                super::TerminalFactRead::Unprovable(_)
            ),
            "another member's twin is not this intent's fact"
        );
    }

    /// Write one refusal fact: the program event that names the refusal and
    /// the twin on the call's own effect stream, in one transaction, the way
    /// the rejection writer writes them.
    fn f24_write_refusal_twin(
        store: &std::sync::Arc<RuntimeEventStore>,
        key: &str,
        twin_idem: &str,
        outer_key: &str,
        member_index: serde_json::Value,
        member_identity: serde_json::Value,
    ) {
        let program_stream = "agentic-program:f24-consume";
        store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: format!("f24-consume-refusal:{twin_idem}"),
                expected_streams: vec![
                    crate::ExpectedStreamRevision {
                        stream_id: program_stream.to_string(),
                        expected_revision: store
                            .stream_revision(program_stream)
                            .expect("program stream revision"),
                    },
                    crate::ExpectedStreamRevision {
                        stream_id: format!("execution-effect:{key}"),
                        expected_revision: store
                            .stream_revision(&format!("execution-effect:{key}"))
                            .expect("effect stream revision"),
                    },
                ],
                events: vec![
                    crate::RuntimeTransactionEventInput {
                        event: crate::RuntimeEventInput {
                            stream_id: program_stream.to_string(),
                            scope: RuntimeEventScope::Program,
                            kind: "agentic.completion_rejected".into(),
                            status: Some("rejected".into()),
                            actor: Some("runtime.agentic_action".into()),
                            refs: vec![],
                            payload: serde_json::json!({
                                "program_id": "f24-consume",
                                "request_action_id": key,
                                "code": "call_refused_after_admission",
                            }),
                        },
                        idempotency_key: Some(format!("completion-rejected:{twin_idem}")),
                        schema_version: 1,
                    },
                    crate::RuntimeTransactionEventInput {
                        event: crate::RuntimeEventInput {
                            stream_id: format!("execution-effect:{key}"),
                            scope: RuntimeEventScope::ExecutionNode,
                            kind: "agentic.batch_member.index".into(),
                            status: Some("committed".into()),
                            actor: Some("runtime.agentic_action".into()),
                            refs: vec![],
                            payload: serde_json::json!({
                                "outer_key": outer_key,
                                "member_index": member_index,
                                "member_identity": member_identity,
                                "ticket": serde_json::Value::Null,
                                "program_stream": program_stream,
                                "program_event_kind": "agentic.completion_rejected",
                                "program_event_key": format!("completion-rejected:{twin_idem}"),
                                "program_event_index": 0,
                            }),
                        },
                        idempotency_key: Some(twin_idem.to_string()),
                        schema_version: 1,
                    },
                ],
            })
            .expect("the refusal fact");
    }

    /// E-s: a required write that was admitted and then refused is settled by
    /// its own refusal twin, so the review stops waiting for a receipt that
    /// will never come.
    #[test]
    fn f24_a_refused_required_write_is_settled_by_its_own_twin() {
        let store = std::sync::Arc::new(RuntimeEventStore::for_test());
        let key = "f24-consume-effect-key";
        f24_write_refusal_twin(
            &store,
            key,
            &format!("agentic-single:{key}"),
            key,
            serde_json::Value::Null,
            serde_json::Value::Null,
        );
        let intent_payload = serde_json::json!({
            "tool_use_id": "f24-consume-call",
            "tool_name": "task_review",
        });
        crate::agentic::fact_admission::take_test_admissions();
        let settlement = super::required_write_settlement(
            &store,
            key,
            &intent_payload,
            None,
            false,
            &mut std::collections::BTreeMap::new(),
        )
        .expect("the settlement decision");
        assert!(crate::agentic::fact_admission::take_test_admissions().iter().any(|strategy| strategy.code() == "settled_refusal"),
            "the authenticated refusal must pass the admission gate");
        assert!(
            matches!(settlement, super::RequiredWriteSettlement::Refused { .. }),
            "a required write that carries its own refusal twin is settled by it"
        );
    }

    /// A required write with no refusal twin of its own stays pending: the
    /// refusal family does not speak for it.
    #[test]
    fn f24_a_required_write_without_a_twin_stays_pending() {
        let store = std::sync::Arc::new(RuntimeEventStore::for_test());
        let intent_payload = serde_json::json!({
            "tool_use_id": "f24-absent-call",
            "tool_name": "task_review",
        });
        let settlement = super::required_write_settlement(
            &store,
            "f24-absent-effect-key",
            &intent_payload,
            None,
            false,
            &mut std::collections::BTreeMap::new(),
        )
        .expect("the settlement decision");
        assert!(
            matches!(settlement, super::RequiredWriteSettlement::Pending),
            "a required write with no terminal fact of its own stays pending"
        );
    }

    /// A required write that ran and failed is carried as its own named
    /// failure - never as a satisfied requirement - and the name is plain
    /// readable.
    #[test]
    fn f24_a_terminally_failed_required_write_carries_its_named_failure() {
        let store = std::sync::Arc::new(RuntimeEventStore::for_test());
        let intent_payload = serde_json::json!({
            "tool_use_id": "f24-failed-call",
            "tool_name": "task_review",
        });
        let receipt_payload = serde_json::json!({
            "outcome": {
                "status": "failed",
                "error": "Runtime did not execute 'task_review': the objective requirement was not met",
            },
        });
        let settlement = super::required_write_settlement(
            &store,
            "f24-failed-effect-key",
            &intent_payload,
            Some((&receipt_payload, 4242u64)),
            true,
            &mut std::collections::BTreeMap::new(),
        )
        .expect("the settlement decision");
        let failure = match settlement {
            super::RequiredWriteSettlement::Failed { failure } => failure,
            _ => panic!("a required write that ran and failed carries its named failure"),
        };
        assert_eq!(failure["grade"], "required_write_terminally_failed");
        assert_eq!(failure["effect_key"], "f24-failed-effect-key");
        assert_eq!(failure["tool_use_id"], "f24-failed-call");
        assert_eq!(failure["tool_name"], "task_review");
        assert_eq!(failure["failure_class"], "terminal_failure");
        assert_eq!(failure["receipt_cursor"], 4242);
        // The name is plain readable: the error text appears as it was.
        let plain = failure["error"].as_str().unwrap_or_default().to_string();
        assert!(
            plain.contains("the objective requirement was not met"),
            "the named failure carries the error text"
        );
    }

    #[test]
    fn f24_a_settlement_unreadable_contradictory_and_missing_receipt_remain_distinct() {
        let intent = serde_json::json!({"tool_use_id":"f24-control", "tool_name":"task_review"});
        let receipt = serde_json::json!({"outcome":{"status":"failed", "error":"original failure"}});
        let check = |store: &std::sync::Arc<RuntimeEventStore>, key: &str, supplied, failed| {
            super::required_write_settlement(store, key, &intent, supplied, failed,
                &mut std::collections::BTreeMap::new()).expect("original settlement decision")
        };
        let missing = std::sync::Arc::new(RuntimeEventStore::for_test());
        crate::agentic::fact_admission::take_test_admissions();
        assert!(matches!(check(&missing, "f24-missing-receipt", None, true),
            super::RequiredWriteSettlement::MissingReceipt));
        assert!(crate::agentic::fact_admission::take_test_admissions().is_empty());

        let unreadable = std::sync::Arc::new(RuntimeEventStore::for_test());
        let foreign = "f24-foreign";
        f24_write_refusal_twin(&unreadable, foreign, &format!("agentic-single:{foreign}"),
            "different-owner", serde_json::Value::Null, serde_json::Value::Null);
        crate::agentic::fact_admission::take_test_admissions();
        match check(&unreadable, foreign, None, false) {
            super::RequiredWriteSettlement::UnreadableRefusal { reason } =>
                assert!(reason.contains("does not have the refusal shape")),
            _ => panic!("foreign refusal twin must remain unreadable"),
        }
        assert!(crate::agentic::fact_admission::take_test_admissions().is_empty());

        let contradictory = std::sync::Arc::new(RuntimeEventStore::for_test());
        let own = "f24-contradiction";
        f24_write_refusal_twin(&contradictory, own, &format!("agentic-single:{own}"),
            own, serde_json::Value::Null, serde_json::Value::Null);
        crate::agentic::fact_admission::take_test_admissions();
        match check(&contradictory, own, Some((&receipt, 42)), true) {
            super::RequiredWriteSettlement::Contradiction { refusal_cursor, receipt_cursor } => {
                assert!(refusal_cursor.is_some());
                assert_eq!(receipt_cursor, Some(42));
            }
            _ => panic!("contradictory terminal facts must remain contradictory"),
        }
        assert!(crate::agentic::fact_admission::take_test_admissions().is_empty());
    }

    /// A node effect is not a call's terminal fact: it carries no outcome.
    #[test]
    fn f24_a_node_effect_is_not_a_settlement_fact() {
        let store = std::sync::Arc::new(RuntimeEventStore::for_test());
        store
            .append(crate::RuntimeEventInput {
                stream_id: "execution-node-effect:abc123:1".into(),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.node_effect".into(),
                status: Some("committed".into()),
                actor: Some("runtime.model_step".into()),
                refs: vec![],
                payload: serde_json::json!({"replan": "retry", "result": "none"}),
            })
            .expect("node effect");
        assert!(
            store
                .event_by_idempotency_key(
                    "execution-node-effect:abc123:1",
                    "agentic-single:node-effect-key",
                )
                .expect("node effect lookup")
                .is_none(),
            "a node effect carries no outcome and is no settlement fact"
        );
    }

    /// G-c: a delegated member's read is judged by the original input its
    /// authenticated child request carries, not by the capability face the
    /// unified read facade declares.
    ///
    /// The member path reads that input from the child request it already
    /// authenticated (the same canonical digest binding the receipt carries),
    /// so a member reading a durable record has no workspace extent while a
    /// member reading a workspace path still needs its acquisition seal.
    #[test]
    fn g_c_member_read_target_decides_whether_the_read_has_a_workspace_extent() {
        let store = RuntimeEventStore::for_test();
        let receipt = |payload: serde_json::Value| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: "session:g-c-member-read".into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.read.receipt".into(),
                    status: Some("completed".into()),
                    actor: Some("governed_tool".into()),
                    refs: vec![],
                    payload,
                })
                .unwrap()
        };
        let descriptor = |scopes: Vec<harness_contract::policy::PermissionScope>| ToolEffectDescriptor {
            tool_id: "read_ref".into(),
            descriptor_hash: "g-c-read-ref-descriptor".into(),
            effect_kind: ToolEffectKind::Read,
            idempotency: harness_contract::tool::ToolIdempotency::Idempotent,
            scopes,
            required_permission: harness_contract::tool::ToolPermissionMode::ReadOnly,
            approval_class: harness_contract::tool::ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        };
        let file_face = vec![harness_contract::policy::PermissionScope {
            resource: harness_contract::policy::PermissionResource::File,
            operation: harness_contract::policy::PermissionOperation::Read,
            target: Some(".".into()),
        }];
        let body = serde_json::json!({
            "outcome": {"status": "executed", "output": "original body"},
            "outcome_truncated": false,
        });
        // The member's authenticated child request input, the shape the member
        // path now feeds to the same predicate the root path uses.
        let member_input = |reference: &str| {
            Some(serde_json::json!({"reference": reference}))
        };

        // A member reading a durable record: no workspace extent.
        assert!(
            super::immutable_receipt_without_workspace_extent(
                &receipt(body.clone()),
                &descriptor(file_face.clone()),
                member_input("artifact://art_74d37ca7").as_ref(),
            )
            .unwrap(),
            "a member read of a durable record has no workspace extent"
        );
        assert!(
            super::immutable_receipt_without_workspace_extent(
                &receipt(body.clone()),
                &descriptor(file_face.clone()),
                member_input("tool://tool-raw-chatcmpl-tool-a815fdd23125e201").as_ref(),
            )
            .unwrap(),
            "a member read of a tool raw record has no workspace extent"
        );
        // A member reading a workspace path still needs its acquisition seal.
        for target in ["file://workspace/report.txt", "dir://workspace"] {
            assert!(
                !super::immutable_receipt_without_workspace_extent(
                    &receipt(body.clone()),
                    &descriptor(file_face.clone()),
                    member_input(target).as_ref(),
                )
                .unwrap(),
                "{target} is a workspace read and keeps its seal requirement"
            );
        }
        // A member read whose child request cannot be authenticated keeps the
        // stricter declared capability judgment: Unknown is not turned into a
        // pass.
        assert!(
            !super::immutable_receipt_without_workspace_extent(
                &receipt(body.clone()),
                &descriptor(file_face.clone()),
                None,
            )
            .unwrap(),
            "an unauthenticated member read keeps the declared capability judgment"
        );
    }

    /// G: the read's own original input decides whether that read carries a
    /// workspace extent, not the capability face the tool declares.
    ///
    /// The unified read facade declares a File capability whatever it reads,
    /// so the declared face refused every durable record a reviewer must
    /// read. A read whose original input names a workspace path still needs
    /// an acquisition seal; a read that named a durable record has no
    /// workspace extent at all.
    #[test]
    fn g_read_target_decides_whether_a_read_has_a_workspace_extent() {
        let store = RuntimeEventStore::for_test();
        let receipt = |payload: serde_json::Value| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: "session:g-read-target".into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.read.receipt".into(),
                    status: Some("completed".into()),
                    actor: Some("governed_tool".into()),
                    refs: vec![],
                    payload,
                })
                .unwrap()
        };
        let descriptor = |scopes: Vec<harness_contract::policy::PermissionScope>| ToolEffectDescriptor {
            tool_id: "read_ref".into(),
            descriptor_hash: "g-read-ref-descriptor".into(),
            effect_kind: ToolEffectKind::Read,
            idempotency: harness_contract::tool::ToolIdempotency::Idempotent,
            scopes,
            required_permission: harness_contract::tool::ToolPermissionMode::ReadOnly,
            approval_class: harness_contract::tool::ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        };
        let file_face = vec![harness_contract::policy::PermissionScope {
            resource: harness_contract::policy::PermissionResource::File,
            operation: harness_contract::policy::PermissionOperation::Read,
            target: Some(".".into()),
        }];
        let body = serde_json::json!({
            "outcome": {"status": "executed", "output": "original body"},
            "outcome_truncated": false,
        });
        let record = serde_json::json!({"reference": "tool://tool-raw-abc"});
        let workspace_file = serde_json::json!({"reference": "file://workspace/report.txt"});
        let workspace_dir = serde_json::json!({"reference": "dir://workspace"});
        let bare = serde_json::json!({"reference": "legacy-identifier"});
        let unknown = serde_json::json!({"reference": "nope://unknown-scheme"});

        // A durable record read through the unified facade: the declared File
        // capability is not what the read hit, and it has no workspace extent.
        assert!(
            super::immutable_receipt_without_workspace_extent(
                &receipt(body.clone()),
                &descriptor(file_face.clone()),
                Some(&record),
            )
            .unwrap(),
            "a read that named a durable record has no workspace extent"
        );
        // A workspace path is still a workspace read and still needs the seal.
        for (target, input) in [
            ("file://workspace/report.txt", &workspace_file),
            ("dir://workspace", &workspace_dir),
        ] {
            assert!(
                !super::immutable_receipt_without_workspace_extent(
                    &receipt(body.clone()),
                    &descriptor(file_face.clone()),
                    Some(input),
                )
                .unwrap(),
                "{target} is a workspace read and has no immutable extent"
            );
        }
        // A legacy bare identifier is resolved by the directory and may name
        // a workspace path, so it stays on the workspace side.
        assert!(
            !super::immutable_receipt_without_workspace_extent(
                &receipt(body.clone()),
                &descriptor(file_face.clone()),
                Some(&bare),
            )
            .unwrap(),
            "a bare identifier may name a workspace path"
        );
        // Without a resolved original input the declared face still decides:
        // the fallback is the stricter legacy judgment, never a relaxation.
        assert!(
            !super::immutable_receipt_without_workspace_extent(
                &receipt(body.clone()),
                &descriptor(file_face.clone()),
                None,
            )
            .unwrap(),
            "an unresolved original input keeps the declared capability judgment"
        );
        // A tool that declares no File capability keeps its legacy judgment.
        assert!(
            super::immutable_receipt_without_workspace_extent(
                &receipt(body.clone()),
                &descriptor(Vec::new()),
                Some(&record),
            )
            .unwrap(),
            "a record read by a non-File tool is still an immutable receipt"
        );
        // A reference that cannot be parsed fails explicitly; it never falls
        // back to treating the read as an immutable receipt.
        assert!(
            super::immutable_receipt_without_workspace_extent(
                &receipt(body.clone()),
                &descriptor(file_face.clone()),
                Some(&unknown),
            )
            .is_err(),
            "an unparseable original reference must fail explicitly"
        );
        // The body and the seal rules are untouched by the target judgment.
        assert_eq!(
            super::immutable_receipt_without_workspace_extent(
                &receipt(serde_json::json!({
                    "outcome": {"status": "executed", "output": "original body"},
                    "outcome_truncated": true,
                })),
                &descriptor(file_face.clone()),
                Some(&record),
            )
            .unwrap_err(),
            "actual immutable receipt lacks its complete original acquired body"
        );
        assert!(
            !super::immutable_receipt_without_workspace_extent(
                &receipt(serde_json::json!({
                    "outcome": {"status": "executed", "output": "original body"},
                    "read_acquisition_v1": {"seal": "present"},
                })),
                &descriptor(file_face.clone()),
                Some(&record),
            )
            .unwrap(),
            "a sealed read is judged by its acquisition, not as an immutable receipt"
        );
    }

    /// A shared aggregate stream growing while the proof pages is another
    /// actor appending, not the material under proof changing: the pinned
    /// window is read unchanged and the read succeeds.
    #[test]
    fn pinned_stream_accepts_shared_aggregate_growth_while_paging() {
        let store = RuntimeEventStore::for_test();
        for source in [
            "session:shared-aggregate-growth",
            "execution-lineage:shared-aggregate-growth",
        ] {
            let append = |store: &RuntimeEventStore| {
                store
                    .append(crate::RuntimeEventInput {
                        stream_id: source.into(),
                        scope: RuntimeEventScope::Session,
                        kind: "test.source-advance".into(),
                        status: None,
                        actor: None,
                        refs: vec![],
                        payload: serde_json::json!({}),
                    })
                    .unwrap();
            };
            append(&store);
            store.inject_read_fault_once(
                source.into(),
                "stream_revision",
                2,
                move |store| append(store),
            );
            let mut sources = std::collections::BTreeMap::new();
            pinned_stream_events(&store, source, &mut sources, 0, PinnedEventProjection::Effects)
                .expect("concurrent appends to a shared aggregate stream must not read as a source change");
            assert_eq!(sources[source], 1, "the proof keeps the head it was assembled against");
        }
    }

    /// The Session cut read keeps the authenticated head as a cut point, not
    /// as the head the stream must still be at: an append that lands while the
    /// original material is read leaves the cut valid and the window unchanged.
    #[test]
    fn session_cut_read_accepts_an_append_that_lands_while_reading() {
        let store = RuntimeEventStore::for_test();
        let source = "session:cut-growth";
        let append = |store: &RuntimeEventStore| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: source.into(),
                    scope: RuntimeEventScope::Session,
                    kind: "test.source-advance".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({}),
                })
                .unwrap();
        };
        append(&store);
        append(&store);
        store.inject_read_fault_once(
            source.into(),
            "list_stream_after",
            1,
            move |store| append(store),
        );
        let mut cuts = std::collections::BTreeMap::new();
        let events = super::super::results::result_source_events_at_session_cut(
            &store,
            source,
            &mut cuts,
            2,
            2,
        )
        .expect("a concurrent append must not invalidate the authenticated Session cut");
        assert_eq!(events.len(), 2, "the read window stops at the historical cut");
        assert_eq!(cuts[source], 3, "the cut records the head the read ended at");
    }

    /// Review-cut closing control: the named producer keeps the head
    /// authoritative. `for_live_material` reads the exact Session stream
    /// itself, so a future cut is refused and a caller's claim about a head
    /// the store never had resolves against the store's head, not the claim.
    /// The struct carries no other constructor and its observed head is
    /// write-private, so there is nothing a caller could self-report.
    #[test]
    fn for_live_material_reads_the_store_head_and_refuses_future_cuts() {
        let store = RuntimeEventStore::for_test();
        let append = |store: &RuntimeEventStore| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: "session:cut-producer".into(),
                    scope: RuntimeEventScope::Session,
                    kind: "test.source-advance".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({}),
                })
                .unwrap();
        };
        append(&store);
        append(&store);
        let cut = HistoricalSessionCut::for_live_material(&store, "cut-producer", 2)
            .expect("the material cut at the durable head is the normal case");
        assert_eq!(cut.source().stream_id, "session:cut-producer");
        assert_eq!(cut.source().expected_revision, 2);
        assert_eq!(cut.observed_revision(), 2, "the head is the store's read, not a report");
        let error = HistoricalSessionCut::for_live_material(&store, "cut-producer", 5)
            .expect_err("a cut above the head must not resolve, whatever the caller claims");
        assert!(
            error.contains("beyond head") && error.contains("cut=5") && error.contains("head=2"),
            "{error}"
        );
        append(&store);
        let grown = HistoricalSessionCut::for_live_material(&store, "cut-producer", 3)
            .expect("the new durable head admits the new material cut");
        assert_eq!(grown.observed_revision(), 3);
        let error = HistoricalSessionCut::for_live_material(&store, "cut-producer", 4)
            .expect_err("a future cut after growth is still refused");
        assert!(error.contains("head=3"), "{error}");
        // A Session stream the store has never seen has head 0: the empty
        // historical window is a legitimate cut, anything above it is not.
        let absent = HistoricalSessionCut::for_live_material(&store, "cut-absent", 0)
            .expect("an empty historical window is a legitimate cut");
        assert_eq!(absent.observed_revision(), 0);
        let error = HistoricalSessionCut::for_live_material(&store, "cut-absent", 1)
            .expect_err("a cut above an absent stream's head is refused");
        assert!(error.contains("head=0"), "{error}");
    }

    /// T1a-R2: the rebuild window keeps the live-material discipline. The head
    /// comes from the store, a Session material cut above the durable head is
    /// still refused after a rebuild, and growth inside the rebuilt window is
    /// refused again instead of being absorbed into the window.
    #[test]
    fn t1a_the_rebuild_window_keeps_the_live_material_discipline() {
        let store = RuntimeEventStore::for_test();
        let append = |store: &RuntimeEventStore, stream: &str, scope| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: stream.into(),
                    scope,
                    kind: "test.source-advance".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({}),
                })
                .unwrap();
        };
        let receipts = "execution-agent-receipts:t1a-rebuild:agent:1";
        let session = "session:t1a-rebuild";
        let review = "agentic-graph:t1a-rebuild/agent:1";
        for _ in 0..2 {
            append(&store, receipts, RuntimeEventScope::ExecutionNode);
        }
        for _ in 0..3 {
            append(&store, session, RuntimeEventScope::Session);
        }
        let mut cuts = std::collections::BTreeMap::new();
        assert_eq!(
            super::super::results::pin_result_source(&store, receipts, &mut cuts)
                .expect("the first pin reads the durable head"),
            2
        );
        append(&store, receipts, RuntimeEventScope::ExecutionNode);
        let rejection = super::super::pin_result_source_ticketed(
            &store, review, 1, receipts, &mut cuts,
        )
        .expect_err("growth on the receipts stream is refused");
        let ticket = rejection.ticket().expect("window refusal").clone();
        assert_eq!((ticket.pinned_cut(), ticket.observed_head()), (2, 3));
        assert_eq!(
            super::super::repin_at_fresh_head(&store, review, 1, &ticket, &mut cuts)
                .expect("the store itself opens the rebuild window"),
            3
        );
        assert_eq!(cuts[receipts], 3);
        // The rebuilt review still cites through the material-cut rule: a cut
        // above the durable head is refused whatever the caller claims.
        let error = HistoricalSessionCut::for_live_material(&store, "t1a-rebuild", 4)
            .expect_err("a material cut above the head stays refused after a rebuild");
        assert!(error.contains("head=3"), "{error}");
        let cut = HistoricalSessionCut::for_live_material(&store, "t1a-rebuild", 3)
            .expect("the durable head admits its own material cut");
        assert_eq!(cut.observed_revision(), 3, "the head is the store's read, not a report");
        // Growth after the window opened is refused again, never absorbed.
        append(&store, receipts, RuntimeEventScope::ExecutionNode);
        let second_rejection = super::super::pin_result_source_ticketed(
            &store, review, 1, receipts, &mut cuts,
        )
        .expect_err("growth inside the rebuilt window is refused");
        let second = second_rejection.ticket().expect("window refusal").clone();
        assert_eq!((second.pinned_cut(), second.observed_head()), (3, 4));
        assert_eq!(second.lineage(), ticket.lineage(), "one review, two refusals");
        assert_eq!(cuts[receipts], 3, "the refused growth does not move the window");
    }

    #[test]
    fn pinned_delivery_projection_preserves_confirmations_across_pages_without_effects() {
        let store = RuntimeEventStore::for_test();
        let source = "session:projection-only";
        let mut expected = Vec::new();
        for position in 0..130 {
            let kind = match position % 3 {
                0 => crate::context_evidence::NATIVE_DELIVERY_KIND,
                1 => crate::context_evidence::PROCESS_DELIVERY_KIND,
                _ => "runtime.strategy.projection_fixture",
            };
            let event = store
                .append(crate::RuntimeEventInput {
                    stream_id: source.into(),
                    scope: RuntimeEventScope::Session,
                    kind: kind.into(),
                    status: None,
                    actor: Some("projection_unit_fixture_not_delivery_authority".into()),
                    refs: vec![],
                    payload: serde_json::json!({"position":position}),
                })
                .unwrap();
            if position % 3 != 2 {
                expected.push(event.event_id);
            }
        }
        let mut sources = std::collections::BTreeMap::new();
        let deliveries = pinned_stream_events(
            &store,
            source,
            &mut sources,
            0,
            PinnedEventProjection::ReadDelivery,
        )
        .unwrap();
        assert_eq!(sources.get(source), Some(&130));
        let mut actual = deliveries
            .into_iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        let effects = pinned_events(&store, source, &mut sources, 0).unwrap();
        assert_eq!(effects.len(), 43);
        assert!(effects
            .iter()
            .all(|event| event.kind.starts_with("runtime.strategy.")));
    }

    #[test]
    fn pinned_delivery_empty_source_keeps_its_original_cut() {
        let store = RuntimeEventStore::for_test();
        // A source the review itself judges: a cut that no longer matches its
        // head is refused. The reviewed Session stream is a shared aggregate
        // stream, where an append that lands between two reads is admissible
        // (`pinned_stream_accepts_shared_aggregate_growth_while_paging`).
        let source = "agentic-program:empty-projection";
        let mut sources = std::collections::BTreeMap::new();
        assert!(pinned_stream_events(
            &store,
            source,
            &mut sources,
            0,
            PinnedEventProjection::ReadDelivery,
        )
        .unwrap()
        .is_empty());
        assert_eq!(sources.get(source), Some(&0));
        store
            .append(crate::RuntimeEventInput {
                stream_id: source.into(),
                scope: RuntimeEventScope::Session,
                kind: "projection_fixture.unrelated_note".into(),
                status: None,
                actor: Some("projection_unit_fixture".into()),
                refs: vec![],
                payload: serde_json::json!({}),
            })
            .unwrap();
        assert!(pinned_stream_events(
            &store,
            source,
            &mut sources,
            0,
            PinnedEventProjection::ReadDelivery,
        )
        .is_err());
    }

    #[test]
    fn completion_context_cannot_cross_request_or_program_snapshot() {
        let mut program = AgenticProgramProjection::empty("program", "objective");
        let request = harness_contract::agent_action::ObjectiveCompleteRequestInput {
            blocked: false,
            blocked_reason: None,
            human_review_required: false,
            recommended_next_actions: vec![],
            result_refs: vec!["tool://result".into()],
            evidence_refs: vec!["tool://evidence".into()],
            unresolved: vec![],
        };
        let context = CompletionAdmissionContext {
            program_digest: crate::agentic::review_evidence::proof_value_digest(&program),
            request_digest: crate::agentic::review_evidence::proof_value_digest(&request),
            envelope_digest: "different-envelope".into(),
            goal_id: "goal:root".into(),
            goal_revision: 1,
            goal_spec_revision: 1,
            goal_spec_digest: "spec".into(),
            excluded_tasks: BTreeSet::from(["optional".into()]),
            sources: vec![],
        };
        assert!(context.matches_request(&program, &request));
        let mut changed = request.clone();
        changed.result_refs.push("tool://new-result".into());
        assert!(!context.matches_request(&program, &changed));
        assert_eq!(
            crate::agentic::supervision::completion_gap_with_context(
                &program,
                &changed,
                Some(&context)
            ),
            Some("completion_context_request_mismatch".into())
        );
        program.objective_summary = "different same-revision Program".into();
        assert!(!context.matches_request(&program, &request));
        assert_eq!(
            crate::agentic::supervision::completion_gap_with_context(
                &program,
                &request,
                Some(&context)
            ),
            Some("completion_context_request_mismatch".into())
        );
    }

    #[test]
    fn completion_coverage_rejects_same_bytes_foreign_identity_and_ambiguous_content() {
        let mut program = AgenticProgramProjection::empty("program", "objective");
        for (reference, producer, execution, generation) in [
            ("artifact:old", "producer:old", "run:old", 1),
            ("artifact:new", "producer:new", "run:new", 2),
        ] {
            program.artifacts.insert(
                reference.into(),
                crate::agentic::program::AgenticArtifactProjection {
                    artifact_ref: reference.into(),
                    content_ref: "artifact://shared-bytes".into(),
                    kind: "report".into(),
                    title: reference.into(),
                    relates_to: vec![format!("task:{generation}")],
                    committed_by: producer.into(),
                    claim_execution_id: Some(execution.into()),
                    claim_generation: Some(generation),
                },
            );
        }
        for (reviewed, required, expected) in [
            ("artifact:old", "artifact:new", false),
            ("artifact:new", "artifact:old", false),
            ("artifact:old", "artifact://shared-bytes", false),
            ("artifact://shared-bytes", "artifact:new", false),
            ("artifact://shared-bytes", "artifact://shared-bytes", false),
            ("artifact:new", "artifact:new", true),
            ("artifact://new", "artifact:new", true),
            ("tool://receipt", "tool://receipt", true),
        ] {
            assert_eq!(
                super::super::coverage::result_refs_cover(&program, &[reviewed.into()], required),
                expected,
                "reviewed={reviewed}, required={required}"
            );
        }
        program.artifacts.remove("artifact:old");
        assert!(super::super::coverage::result_refs_cover(
            &program,
            &["artifact:new".into()],
            "artifact://shared-bytes"
        ));
    }

    #[test]
    fn low_risk_effect_policy_rejects_unbounded_external_unknown_or_independently_gated_writes() {
        let effect = ToolEffectDescriptor {
            tool_id: "edit".into(),
            descriptor_hash: "registered-file-effect".into(),
            effect_kind: ToolEffectKind::Write,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![PermissionScope {
                resource: PermissionResource::File,
                operation: PermissionOperation::Write,
                target: Some("notes/result.txt".into()),
            }],
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: EffectAssessment {
                reversibility: EffectReversibility::Compensatable,
                externality: EffectExternality::Workspace,
                data_sensitivity: DataClassification::Internal,
                novelty: EffectNovelty::Routine,
                blast_radius: EffectBlastRadius::Workspace,
            },
        };
        assert!(permits_low_risk_self_review(&effect));
        for kind in [
            ToolEffectKind::Network,
            ToolEffectKind::Process,
            ToolEffectKind::Package,
            ToolEffectKind::System,
            ToolEffectKind::Destructive,
            ToolEffectKind::Unknown,
        ] {
            assert!(!permits_low_risk_self_review(&ToolEffectDescriptor {
                effect_kind: kind,
                ..effect.clone()
            }));
        }
        for target in [
            "",
            ".",
            "../outside",
            "/etc/config",
            "~/config",
            "workspace/*",
            "a/../b",
            "files/?.txt",
        ] {
            let mut candidate = effect.clone();
            candidate.scopes[0].target = Some(target.into());
            assert!(!permits_low_risk_self_review(&candidate), "{target}");
        }
        let candidates = [
            ToolEffectDescriptor {
                scopes: vec![],
                ..effect.clone()
            },
            ToolEffectDescriptor {
                uses_network: true,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                spawns_process: true,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                mutates_packages: true,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                mutates_system: true,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                idempotency: ToolIdempotency::NonIdempotent,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                idempotency: ToolIdempotency::Unknown,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                approval_class: ToolApprovalClass::User,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                approval_class: ToolApprovalClass::Administrator,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                assessment: EffectAssessment::default(),
                ..effect.clone()
            },
        ];
        for candidate in candidates {
            assert!(!permits_low_risk_self_review(&candidate));
        }
        for assessment in [
            EffectAssessment {
                reversibility: EffectReversibility::Irreversible,
                ..effect.assessment.clone()
            },
            EffectAssessment {
                externality: EffectExternality::ExternalMutation,
                ..effect.assessment.clone()
            },
            EffectAssessment {
                data_sensitivity: DataClassification::Secret,
                ..effect.assessment.clone()
            },
            EffectAssessment {
                novelty: EffectNovelty::NewCapability,
                ..effect.assessment.clone()
            },
            EffectAssessment {
                blast_radius: EffectBlastRadius::Unbounded,
                ..effect.assessment.clone()
            },
        ] {
            assert!(!permits_low_risk_self_review(&ToolEffectDescriptor {
                assessment,
                ..effect.clone()
            }));
        }
    }
    fn actual_dependency_fixture() -> ActualFileDependency {
        ActualFileDependency {
            target: harness_contract::goal::ProofEffectTarget {
                path: serde_json::from_value(serde_json::json!({
                    "workspace_id":"workspace", "repository_id":"repository",
                    "workspace_relative_path":"input.txt", "repository_relative_path":"input.txt", "object_kind":"file"
                })).unwrap(),
                declared_scope: PermissionScope {
                    resource:PermissionResource::File, operation:PermissionOperation::Read, target:Some("input.txt".into()),
                },
                access_mode:harness_contract::context::WorkspaceAccessMode::Read,
                coverage:harness_contract::context::EvidenceCoverageKind::ScopedContent,
                exact:true, physical_identity:Some("physical:input".into()), alias_checked:true,
            },
            acquisition_cursor:10, consumer_cursor:20, consumer_call_key:"consumer".into(), source_sha256:"original-preimage".into(), source_bytes: 0,
        }
    }

    #[test]
    fn actual_windows_keep_original_preuse_cursor_and_reject_incomplete_coverage() {
        let original = actual_dependency_fixture();
        let mut later = original.clone();
        later.acquisition_cursor = 14;
        let first = ActualFileWindow {
            begin: 0,
            end: 2,
            total: 4,
            dependency: original.clone(),
        };
        let second = ActualFileWindow {
            begin: 2,
            end: 4,
            total: 4,
            dependency: later.clone(),
        };
        let dependency = complete_actual_file_windows(vec![first.clone(), second]).unwrap();
        assert_eq!(dependency.acquisition_cursor, 10);
        assert_eq!(dependency.source_sha256, "original-preimage");
        assert!(complete_actual_file_windows(vec![first.clone()]).is_err());
        assert!(complete_actual_file_windows(vec![
            first,
            ActualFileWindow {
                begin: 2,
                end: 4,
                total: 5,
                dependency: later,
            }
        ])
        .is_err());
    }

    #[test]
    fn actual_input_material_omission_is_rejected_even_when_acquisition_and_index_are_both_removed()
    {
        let acquisition = harness_contract::goal::ProofInputEvent {
            stream_id: "execution-effect:read".into(),
            sequence: 1,
            event_id: "acquisition".into(),
            payload_digest: "primary-digest".into(),
        };
        let index = harness_contract::goal::ProofInputEvent {
            stream_id: "execution-agent-receipts:graph:node:1".into(),
            sequence: 1,
            event_id: "index".into(),
            payload_digest: "index-digest".into(),
        };
        let recovered = vec![acquisition.clone(), index.clone()];
        assert!(require_actual_materials(&recovered, &recovered).is_ok());
        assert!(require_actual_materials(&recovered, &[index]).is_err());
        assert!(require_actual_materials(&recovered, &[acquisition]).is_err());
        assert!(require_actual_materials(&recovered, &[]).is_err());
    }

    #[test]
    fn input_before_use_and_output_after_write_have_distinct_boundaries() {
        let dependency = actual_dependency_fixture();
        assert!(input_changed_before_use(&dependency, 15, Some("other")));
        assert!(input_changed_before_use(&dependency, 15, None));
        assert!(!input_changed_before_use(&dependency, 15, Some("consumer")));
        assert!(!input_changed_before_use(&dependency, 10, Some("other")));
        assert!(!input_changed_before_use(&dependency, 20, Some("other")));
        assert!(!input_changed_before_use(&dependency, 25, Some("other")));
        assert!(!output_observation_is_fresh(25, 25, std::iter::empty()));
        assert!(output_observation_is_fresh(25, 26, std::iter::empty()));
        assert!(!output_observation_is_fresh(25, 26, [27].into_iter()));
        assert!(output_observation_is_fresh(25, 28, [27].into_iter()));
    }

    #[test]
    fn output_freshness_is_required_without_any_workspace_input_dependency() {
        let actual = ActualInputSnapshot {
            file_dependencies: Vec::new(),
            immutable_inputs: Vec::new(),
            sources: Default::default(),
            material_events: Vec::new(),
            canonical_facts: Vec::new(),
        };
        assert!(actual.required_scopes().is_empty());
        assert!(!output_observation_is_fresh(12, 0, std::iter::empty()));
        assert!(!output_observation_is_fresh(12, 13, [14].into_iter()));
        assert!(output_observation_is_fresh(12, 15, [14].into_iter()));
    }

    #[test]
    fn immutable_artifact_acquisition_does_not_require_a_workspace_file_scope() {
        let store = RuntimeEventStore::for_test();
        let mut descriptor = tools::ToolHost::builtin("immutable-purpose", std::env::temp_dir())
            .pin_snapshot()
            .describe_effect("read_file", &serde_json::json!({"path":"input.txt"}));
        descriptor.tool_id = "evidence_retrieve".into();
        descriptor.scopes.clear();
        descriptor.effect_kind = ToolEffectKind::Read;
        descriptor.uses_network = false;
        descriptor.spawns_process = false;
        descriptor.mutates_packages = false;
        descriptor.mutates_system = false;
        let mut primary = store
            .append(crate::RuntimeEventInput {
                stream_id: "execution-effect:immutable".into(),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.read.receipt".into(),
                status: Some("completed".into()),
                actor: Some("governed_tool".into()),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .unwrap();
        let mut acquisition: crate::context_evidence::ReadAcquisitionSeal = serde_json::from_value(serde_json::json!({
            "schema_version":1, "primary_stream":primary.stream_id, "primary_key":"immutable:read-receipt",
            "index_stream":"execution-agent-receipts:graph:node:1", "index_key":"agent-tool-receipt:immutable",
            "session_id":"session", "invocation_id":"call", "tool_name":"evidence_retrieve",
            "graph_id":"graph", "node_id":"node", "attempt":1,
            "step":{"step_id":"step","session_id":"session","turn_id":"turn","execution_id":"graph",
                "goal_ref":null,"program_ref":null,"task_ref":null,"build_ref":"build"},
            "origin":{}, "input_sha256":"input-digest",
            "body":{"schema_version":1,"source_ref":"artifact://immutable","source_sha256":"source-digest",
                "source_bytes":1,"output_sha256":"output-digest","output_bytes":1,
                "extent":{"kind":"artifact_chunks","total_chunks":1,
                    "chunks":[{"index":0,"body_sha256":"chunk-digest","body_bytes":1}]}}
        })).unwrap();
        let content = harness_contract::context::ArtifactRef::durable(
            "artifact://immutable", "source-digest", 1, "application/octet-stream", "session:session",
        );
        let mut program = AgenticProgramProjection::empty("program", "objective");
        for reference in ["artifact:body", "artifact:foreign"] {
            program.artifacts.insert(reference.into(), crate::agentic::program::AgenticArtifactProjection {
                artifact_ref: reference.into(), content_ref: content.selector.clone(),
                kind: "report".into(), title: reference.into(), relates_to: Vec::new(),
                committed_by: reference.into(), claim_execution_id: None, claim_generation: None,
            });
        }
        let mut input = ActualImmutableInput {
            content: content.clone(), reference: content.selector.clone(),
            acquisition: acquisition.clone(), acquisition_cursor: primary.commit_cursor,
        };
        assert_eq!(input.dependency_identity(&program).unwrap(), None);
        input.reference = "artifact://unrelated-content".into();
        assert_eq!(
            input.dependency_identity(&program).unwrap_err(),
            "immutable input content reference conflicts with its acquisition",
        );
        input.reference = content.selector.clone();
        let mut dependency = super::super::results::ResolvedReviewResult {
            origin: super::super::results::ResultOrigin::Strong,
            result_sources: Vec::new(),
            reference: content.selector.clone(),
            content: content.clone(),
            kind: harness_contract::goal::GoalResultKind::Content,
            artifact_kinds: BTreeSet::new(),
            producers: Vec::new(),
            effect_cursor: None,
            effect_sources: Vec::new(),
            effect_scopes: Vec::new(),
            effect_targets: Vec::new(),
            unscoped_effect: false,
            primary_inputs: Vec::new(),
            input_boundaries: Vec::new(),
            process_runs: Vec::new(),
        };
        input.validate_dependency(&program, &content.selector, &dependency).unwrap();
        for field in ["selector", "sha256", "bytes", "media_type", "visibility_scope"] {
            let mut other = content.clone();
            match field {
                "selector" => other.selector = "artifact://other".into(),
                "sha256" => other.sha256 = "other-digest".into(),
                "bytes" => other.bytes += 1,
                "media_type" => other.media_type = "text/plain".into(),
                "visibility_scope" => other.visibility_scope = "public".into(),
                _ => unreachable!(),
            }
            assert_eq!(
                input.dependency_identity(&program).unwrap(),
                None,
                "{field} must not infer a publication for an authenticated content input",
            );
            dependency.content = other;
            assert_eq!(
                input.validate_dependency(&program, &content.selector, &dependency).unwrap_err(),
                "immutable dependency conflicts with its authenticated acquisition",
                "{field}",
            );
        }
        dependency.content = content.clone();
        input.validate_dependency(&program, &content.selector, &dependency).unwrap();
        input.reference = "artifact:foreign".into();
        assert_eq!(input.dependency_identity(&program).unwrap(), Some("artifact:foreign".into()));
        assert_eq!(input.acquisition_cursor, primary.commit_cursor);
        assert_eq!(input.acquisition, acquisition);
        assert!(read_original_scope(&primary, &descriptor, &acquisition)
            .unwrap()
            .is_none());
        primary.payload["resolved_effect_scope"] = serde_json::json!({
            "version":2,"descriptor_hash":descriptor.descriptor_hash,"complete":false,"targets":[],"scope_purposes":[]
        });
        let scope = read_original_scope(&primary, &descriptor, &acquisition)
            .unwrap()
            .unwrap();
        assert!(!scope.complete && scope.targets.is_empty());
        primary.payload["resolved_effect_scope"]["version"] = serde_json::json!(1);
        assert!(read_original_scope(&primary, &descriptor, &acquisition).is_err());
        primary.payload["resolved_effect_scope"] = serde_json::Value::Null;
        acquisition.body.extent = crate::context_evidence::ReadExtent::FileLines {
            start_line: 1,
            line_count: 1,
            total_lines: 1,
            ends_with_newline: false,
            body_sha256: "body".into(),
            body_bytes: 1,
            source_omitted: false,
        };
        assert!(read_original_scope(&primary, &descriptor, &acquisition).is_err());
    }

    #[test]
    fn reacquisition_before_use_replaces_cursor_without_relabeling_the_original_preimage() {
        let mut dependency = actual_dependency_fixture();
        assert!(input_changed_before_use(&dependency, 15, Some("other")));
        dependency.acquisition_cursor = 18;
        dependency.source_sha256 = "actually-reacquired-preimage".into();
        assert!(!input_changed_before_use(&dependency, 15, Some("other")));
        assert!(input_changed_before_use(&dependency, 19, Some("other")));
        assert_eq!(dependency.consumer_cursor, 20);
        assert_eq!(dependency.source_sha256, "actually-reacquired-preimage");
    }

    #[test]
    fn unknown_or_directory_effect_bounds_cannot_prove_a_file_dependency_disjoint() {
        let dependency = actual_dependency_fixture();
        assert!(!effect_disjoint_from_target(&dependency.target, None));
        let mut directory = dependency.target.clone();
        directory.path.object_kind = harness_contract::context::WorkspaceObjectKind::Directory;
        directory.exact = false;
        let scope = harness_contract::goal::ProofEffectScope {
            version: 2,
            descriptor_hash: "original".into(),
            complete: true,
            targets: vec![directory],
            scope_purposes: vec![harness_contract::goal::ProofEffectScopePurpose {
                declared_scope: dependency.target.declared_scope.clone(),
                purpose: harness_contract::goal::ProofEffectPurpose::InputRead,
            }],
        };
        assert!(!effect_disjoint_from_target(
            &dependency.target,
            Some(&scope)
        ));
        assert_eq!(scope.targets.len(), 1);
    }





    async fn acquire_original_native_body(
        services: &std::sync::Arc<crate::RuntimeServices>,
        worker: &harness_contract::agent_action::AgentActionEnvelope,
        tool: &str,
        call: &str,
        input: serde_json::Value,
    ) {
        use crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher;
        let graphs = services.graph_state_store();
        let graph = graphs
            .load(worker.actor.execution_id.as_ref().unwrap())
            .unwrap();
        let packet: harness_contract::agent::AgentTaskPacket = serde_json::from_str(
            &graph
                .nodes
                .iter()
                .find(|node| {
                    node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                })
                .unwrap()
                .payload_ref,
        )
        .unwrap();
        let mut request = graphs.child_links(packet.graph_id()).unwrap().into_iter().find_map(|link| {
            let child = graphs.load(&link.child_execution_id).unwrap();
            if !crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(&child) {
                return None;
            }
            let request = crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_graph(&child)
                .unwrap().request;
            (request.tool_name == "read_file").then_some(request)
        }).expect("the paused original Native worker must already have a real read request");
        let lease = services
            .tool_execution_host()
            .unwrap()
            .pin_step_catalog()
            .unwrap();
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == tool)
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(definitions.len(), 1);
        let mut identity = request
            .step_binding
            .as_ref()
            .unwrap()
            .snapshot
            .identity
            .clone();
        identity.step_id = format!("actual-input-catalog:{}", uuid::Uuid::new_v4());
        let step = std::sync::Arc::new(
            crate::step_snapshot::BoundStep::bind(
                identity,
                lease.clone(),
                &definitions,
                &serde_json::json!({}),
                &serde_json::json!({}),
                std::collections::BTreeMap::new(),
                9_007_199_254_740_991,
            )
            .unwrap(),
        );
        let binding = crate::step_snapshot::StepCallBinding::provider(step, call);
        let invocation = binding
            .prepare_invocation(Some(lease), tool, &input)
            .unwrap();
        let policy = crate::PermissionPolicy::new(packet.permission_ceiling);
        let authorization = crate::AuthorizationNegotiator::new().assess_effective(
            &policy,
            &crate::AuthorizationRequest {
                principal_id: packet.assignment.execution_identity.principal_id().into(),
                capability: tool.into(),
                input: input.to_string(),
                idempotency_key: binding.origin.call_id.clone(),
                effect: invocation.effect.clone(),
                parent_ceiling: packet.permission_ceiling,
                parent_lease_id: None,
                policy_revision: request.policy_revision,
                recovery_scope: packet.run_id().into(),
                context: crate::PermissionContext::default(),
                safe_alternatives: Vec::new(),
            },
        );
        request.authorization = Some(
            crate::ToolPolicy
                .authorize(
                    &authorization.effective,
                    &authorization.assessment,
                    &binding.origin.call_id,
                    authorization.assessment.lease.clone().unwrap(),
                    30,
                )
                .unwrap()
                .authorization,
        );
        request.tool_use_id = binding.origin.call_id.clone();
        request.tool_name = tool.into();
        request.input = input.to_string();
        request.idempotency_key = binding.effect_idempotency_key();
        request.observation_wave_sequence = services
            .event_store()
            .list_stream(&format!(
                "execution-agent-receipts:{}:{}:{}",
                packet.graph_id(),
                packet.node_id(),
                packet.attempt
            ))
            .unwrap()
            .iter()
            .filter_map(|event| event.payload["sequence"].as_u64())
            .max()
            .unwrap_or_default()
            + 100;
        request.compiled_invocation = Some(invocation);
        request.step_binding = Some(binding);
        let packed = crate::execution_core::graph::executors::agent_tool::encode_bound_agent_tool(
            packet.agent_id(),
            packet.run_id(),
            request.clone(),
        )
        .unwrap();
        let restored =
            crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool(&packed)
                .unwrap();
        assert_eq!(
            serde_json::to_value(&restored.request).unwrap(),
            serde_json::to_value(&request).unwrap()
        );
        assert_eq!(restored.request.input, request.input);
        assert_eq!(restored.request.step_binding, request.step_binding);
        assert_eq!(restored.request.authorization, request.authorization);
        let outcome = AgentToolBatchDispatcher::new(services, &packet)
            .execute(request.clone())
            .await
            .unwrap();
        assert_eq!(
            outcome.status,
            crate::RuntimeToolExecutionStatus::Executed,
            "{outcome:?}"
        );
        if tool == "tool_search" {
            assert!(crate::context_evidence::read_body_seal(
                tool,
                outcome.output.as_deref().unwrap()
            )
            .is_none());
        }
        let primary = services
            .event_store()
            .event_by_idempotency_key(
                &format!("execution-effect:{}", request.idempotency_key),
                &format!("{}:read-receipt", request.idempotency_key),
            )
            .unwrap()
            .unwrap();
        assert_eq!(primary.payload["resolved_effect_scope"]["version"], 2);
        if tool == "tool_search" {
            assert!(primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null());
            assert_eq!(primary.payload["resolved_effect_scope"]["complete"], false);
        }
    }

    fn real_review_actual_inputs(
        services: &std::sync::Arc<crate::RuntimeServices>,
        program: &AgenticProgramProjection,
        proof: &harness_contract::goal::ObjectiveReviewVerification,
    ) -> (
        Vec<super::super::results::ResultInputBoundary>,
        ActualInputSnapshot,
    ) {
        let history = services.session_history_reader();
        let readers = super::super::ResultContentReaders {
            artifacts: Some(services.artifact_store().as_ref()),
            history: history.as_deref(),
            process_inputs: None,
        };
        let mut boundaries = Vec::new();
        for read in &proof.reads {
            let content = harness_contract::context::ArtifactRef::durable(
                &read.content_ref,
                &read.sha256,
                read.bytes,
                "application/octet-stream",
                format!("session:{}", program.session_id),
            );
            let identity = super::super::review_result_identity(program, &read.result_ref).unwrap();
            let metadata = super::super::results::resolve_result_metadata(
                services.event_store(),
                program,
                &identity,
                &content,
                None,
                readers,
            )
            .unwrap();
            boundaries.extend(metadata.input_boundaries);
        }
        let actual =
            actual_input_snapshot(services.event_store(), program, &boundaries, readers, None)
                .unwrap();
        (boundaries, actual)
    }







}

#[cfg(test)]
#[path = "receipt_index_791_tests.rs"]
mod receipt_index_791_tests;


#[cfg(test)]
mod own872_plan_owner_tests {
    use super::*;


    #[test]
    fn own872_native_plan_owner_does_not_erase_unknown_or_pending_root_writes() {
        let store = crate::RuntimeEventStore::for_test();
        let mut plan = store.append(crate::RuntimeEventInput {
            stream_id: "session:own872".into(),
            scope: RuntimeEventScope::Tool,
            kind: "tool.execution_plan.created".into(),
            status: Some("planned".into()),
            actor: Some("conversation_runtime".into()),
            refs: vec![crate::RuntimeEventRef { kind: "execution".into(), id: "native".into() }],
            payload: serde_json::json!({"tasks":[{"tool_name":"task_claim","effect":{"effect_kind":"write"}}]}),
        }).unwrap();
        let roots = BTreeSet::from(["root".to_string()]);
        let packets = std::collections::BTreeMap::new();
        assert!(delegated_plan_callback_owner(&plan, &[], &roots, &packets).is_none());
        plan.refs.clear();
        assert!(delegated_plan_callback_owner(&plan, &[], &roots, &packets).is_none());
        plan.refs.push(crate::RuntimeEventRef { kind: "tool_call".into(), id: "native-claim".into() });
        assert!(delegated_plan_callback_owner(&plan, &[], &roots, &packets).is_none());
    }
}

#[cfg(test)]
mod read_auth_boundary_tests {
    use super::super::read_source::ReadAuthSource;
    use super::*;
    use crate::{DurableRuntimeEvent, RuntimeEventScope};
    use serde_json::{json, Value};
    use std::collections::BTreeMap;

    // Fixed originals, not validator-produced expectations. No backing store is
    // available to this reader: every nested access must cross the five ports.
    #[derive(Clone, Default)]
    struct Reader {
        keys: BTreeMap<(String, String), DurableRuntimeEvent>,
        records: Vec<DurableRuntimeEvent>,
        body: Value,
        calls: Vec<&'static str>,
        fault: Option<&'static str>,
        fence: u64,
        moved: bool,
    }
    impl Reader {
        fn hit(&mut self, op: &'static str) -> Result<(), String> {
            self.calls.push(op);
            if self.fault == Some(op) {
                Err(format!("reader:{op}"))
            } else {
                Ok(())
            }
        }
    }
    impl ReadAuthSource for Reader {
        fn stream_revision(&mut self, _: &str) -> Result<u64, String> {
            self.hit("revision")?;
            Ok(
                if self.moved && self.calls.iter().filter(|op| **op == "revision").count() > 1 {
                    8
                } else {
                    7
                },
            )
        }
        fn event_by_idempotency_key(
            &mut self,
            stream: &str,
            key: &str,
        ) -> Result<Option<DurableRuntimeEvent>, String> {
            self.hit("key")?;
            Ok(self.keys.get(&(stream.into(), key.into())).cloned())
        }
        fn list_stream_after(
            &mut self,
            stream: &str,
            after: u64,
            through: u64,
            limit: usize,
            bytes: usize,
        ) -> Result<Vec<DurableRuntimeEvent>, String> {
            self.hit("page")?;
            assert_eq!((limit, bytes), (1, 4 * 1024 * 1024));
            Ok(self
                .records
                .iter()
                .filter(|r| r.stream_id == stream && r.sequence > after && r.sequence <= through)
                .take(limit)
                .cloned()
                .collect())
        }
        fn transaction_stream_revision_exact(
            &mut self,
            _: &str,
            stream: &str,
        ) -> Result<Option<crate::CommittedStreamRevision>, String> {
            self.hit("fence")?;
            Ok(Some(crate::CommittedStreamRevision {
                stream_id: stream.into(),
                expected_revision: self.fence,
                committed_revision: self.fence,
            }))
        }
        fn resolve_graph_record(&mut self, _: &DurableRuntimeEvent) -> Result<Value, String> {
            self.hit("body")?;
            Ok(self.body.clone())
        }
    }
    fn event(stream: &str, kind: &str, cursor: u64, payload: Value) -> DurableRuntimeEvent {
        DurableRuntimeEvent {
            event_id: format!("event-{stream}-{cursor}"),
            stream_id: stream.into(),
            sequence: 1,
            scope: RuntimeEventScope::ExecutionNode,
            kind: kind.into(),
            status: Some("completed".into()),
            actor: Some("governed_tool".into()),
            refs: vec![],
            payload,
            created_at_ms: 0,
            commit_cursor: cursor,
            transaction_id: format!("tx-{cursor}"),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        }
    }
    fn root() -> (Reader, DurableRuntimeEvent) {
        use harness_contract::execution_graph::*;
        let binding = json!({"root_execution_id":"root","session_id":"session","turn_id":"test-turn","root_task_id":"task","task_id":"task","activity_id":"activity:execution:root:owner:model","node_id":"node","revision":2,"fence":1,"generation":1});
        let owner = json!({"binding":binding,"attempt":1,"graph_revision":2});
        let mut graph = ExecutionGraph::new("original root");
        graph.id = "root".into();
        graph.revision = 2;
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "model", "{}");
        node.id = "node".into();
        graph.nodes.push(node);
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "session".into(),
            turn_id: "test-turn".into(),
            root_task_id: "task".into(),
            task_id: "task".into(),
            generation: 1,
        });
        graph
            .node_statuses
            .insert("node".into(), ExecutionNodeStatus::Running);
        graph.recovery_cursor.node_attempts.insert("node".into(), 1);
        let body = serde_json::to_value(
            crate::execution_core::graph::ExecutionGraphEvent::Checkpoint {
                cause: "execution_graph.nodes_started".into(),
                graph,
                bindings: BTreeMap::from([(
                    "node".into(),
                    crate::execution_core::graph::ExecutionNodeBinding {
                        executor_kind: "model".into(),
                        ticket_idempotency_key: "original-ticket".into(),
                        attempt: 1,
                        resource_lease_refs: vec![],
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                )]),
                lease_closures: vec![],
            },
        )
        .unwrap();
        let mut running = event(
            "root:node:node",
            "execution_node.transitioned",
            2,
            json!({"_runtime_activity_binding":binding,"graph_id":"root","node_id":"node","graph_revision":2,"to":"running"}),
        );
        running.actor = Some("execution_commit_service".into());
        let mut record = event(
            "root",
            "execution_graph.checkpoint",
            2,
            json!({"body":"external-reference"}),
        );
        record.sequence = 2;
        record.schema_version = 2;
        record.scope = RuntimeEventScope::ExecutionGraph;
        record.actor = Some("execution_commit_service".into());
        let intent = event(
            "effect",
            "execution.read.intent",
            3,
            json!({"root_effect_owner":owner,"root_effect_context":{"execution_id":"root","session_id":"session","turn_id":"test-turn"}}),
        );
        let reader = Reader {
            keys: BTreeMap::from([(("root:node:node".into(), "root:node:2".into()), running)]),
            records: vec![record],
            body,
            fence: 2,
            ..Default::default()
        };
        (reader, intent)
    }
    #[test]
    fn root_nested_reader_authenticates_original_and_rejects_fence_body_and_io() {
        let (reader, intent) = root();
        let mut good = reader.clone();
        let (owner, attempt, revision) =
            super::super::results::authenticate_original_root_owner_with_reader(
                &mut good,
                &intent,
                &mut BTreeMap::new(),
            )
            .unwrap();
        assert_eq!(
            (owner.root_execution_id.as_str(), attempt, revision),
            ("root", 1, 2)
        );
        assert_eq!(
            good.calls,
            vec!["revision", "revision", "key", "fence", "page", "body"]
        );
        let mut wrong = reader.clone();
        wrong.fence = 1;
        assert_eq!(
            super::super::results::authenticate_original_root_owner_with_reader(
                &mut wrong,
                &intent,
                &mut BTreeMap::new()
            )
            .unwrap_err().to_string(),
            "root effect does not authenticate its original Running owner/admission fence"
        );
        let mut wrong = reader.clone();
        wrong.body = json!({"invalid":"graph-body"});
        assert_eq!(
            super::super::results::authenticate_original_root_owner_with_reader(
                &mut wrong,
                &intent,
                &mut BTreeMap::new()
            )
            .unwrap_err().to_string(),
            "original root start graph content is invalid"
        );
        for op in ["revision", "key", "fence", "page", "body"] {
            let mut wrong = reader.clone();
            wrong.fault = Some(op);
            assert_eq!(
                super::super::results::authenticate_original_root_owner_with_reader(
                    &mut wrong,
                    &intent,
                    &mut BTreeMap::new()
                )
                .unwrap_err().to_string(),
                format!("reader:{op}")
            );
            assert_eq!(wrong.calls.last(), Some(&op));
        }
        let mut missing = reader;
        missing.keys.clear();
        assert_eq!(
            super::super::results::authenticate_original_root_owner_with_reader(
                &mut missing,
                &intent,
                &mut BTreeMap::new()
            )
            .unwrap_err().to_string(),
            "original root Running transition missing"
        );
    }
    #[test]
    fn root_receipt_nested_reader_requires_original_index() {
        let (mut reader, mut intent) = root();
        let effect = json!({"effect_kind":"read"});
        let replay = json!({"call_id":"call", "invocation":{"effect":effect}});
        for (key, value) in [
            ("effect", effect.clone()),
            ("replay", replay.clone()),
            ("tool_use_id", json!("call")),
            ("tool_name", json!("read_ref")),
            ("idempotency_key", json!("call-key")),
            ("input_sha256", json!("input")),
        ] {
            intent.payload[key] = value;
        }
        let outcome = json!({"status":"executed","tool_use_id":"call","tool_name":"read_ref"});
        let mut primary = event(
            "effect",
            "execution.read.receipt",
            4,
            json!({"effect":effect,"replay":replay,"outcome":outcome,"tool_use_id":"call","tool_name":"read_ref","idempotency_key":"call-key","input_sha256":"input","root_effect_owner":intent.payload["root_effect_owner"],"root_effect_context":intent.payload["root_effect_context"]}),
        );
        primary.transaction_index = 0;
        let mut index = event(
            "execution-agent-receipts:root:node:1",
            "execution.agent_tool.receipt",
            4,
            primary.payload.clone(),
        );
        index.transaction_index = 1;
        index.payload["effect_kind"] = json!("read");
        index.payload["sequence"] = json!(1);
        reader
            .keys
            .insert(("effect".into(), "call-key:read-intent".into()), intent);
        reader.keys.insert(
            (
                "execution-agent-receipts:root:node:1".into(),
                "agent-tool-receipt:call-key".into(),
            ),
            index,
        );
        let mut good = reader.clone();
        let mut cuts = BTreeMap::new();
        super::super::results::authenticate_root_receipt_owner_with_reader(
            &mut good, &primary, &mut cuts,
        )
        .unwrap();
        assert_eq!(
            cuts.keys().cloned().collect::<Vec<_>>(),
            vec![
                "effect",
                "execution-agent-receipts:root:node:1",
                "root",
                "root:node:node"
            ]
        );
        assert_eq!(good.calls.iter().filter(|op| **op == "key").count(), 3);
        assert!(good.calls.contains(&"body"));
        assert!(good.calls.contains(&"fence"));
        reader.keys.remove(&(
            "execution-agent-receipts:root:node:1".into(),
            "agent-tool-receipt:call-key".into(),
        ));
        assert_eq!(
            super::super::results::authenticate_root_receipt_owner_with_reader(
                &mut reader,
                &primary,
                &mut BTreeMap::new()
            )
            .unwrap_err().to_string(),
            "root receipt original real-node index missing"
        );
    }
    fn agent() -> (
        Reader,
        AgenticProgramProjection,
        harness_contract::agent::AgentTaskPacket,
        DurableRuntimeEvent,
    ) {
        use harness_contract::agent::*;
        let assignment = crate::test_support::agent_assignment(
            None, "agent", "run", "task", "session", "mission", None, "graph", "node",
        );
        let binding = AgentBindingSnapshot {
            binding_id: "binding".into(),
            definition_ref: assignment.definition_ref.clone(),
            definition_digest: "sha256:original".into(),
            instructions: "read".into(),
            instance: AgentInstanceRef {
                instance_id: "agent".into(),
                role_slot_id: None,
            },
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "test".into(),
                allowed_models: vec![],
                fallback_allowed: false,
            },
            effective_capabilities: vec![],
            skill_refs: vec![],
            tool_contract_refs: vec!["read_ref".into()],
            skill_tool_pins: None,
            skill_tool_refs: None,
            data_lease: AgentDataLease {
                session_id: "session".into(),
                task_id: "task".into(),
                team_id: None,
                read_scopes: vec![],
                write_mode: CognitiveWriteMode::None,
                fact_boundaries: vec![],
                fact_refs: vec![],
                matrix_snapshot_refs: vec![],
            },
            release: None,
            evaluation: None,
            display: None,
            binding_digest: "binding-digest".into(),
        };
        let packet = AgentTaskPacket {
            assignment,
            attempt: 1,
            expected_graph_revision: 1,
            objective: "read".into(),
            required_acceptance: Default::default(),
            output_acceptance: vec![],
            cohort_prompt_package: None,
            acceptance: vec![],
            constraints: vec![],
            context_refs: vec![],
            agentic_binding: None,
            evidence_refs: vec![],
            resource_scopes: vec![],
            allowed_tools: vec!["read_ref".into()],
            allowed_skills: vec![],
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            policy_revision: 1,
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "budget",
                "agent",
                "agent",
                1,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: Some(binding),
            managed_invocation: None,
            idempotency_key: "packet".into(),
        };
        let program:AgenticProgramProjection=serde_json::from_value(json!({"program_id":"program","objective_id":"objective","session_id":"session","turn_id":"test-turn","root_execution_id":"root","required_team_count":0,"exact_team_count":null,"objective_summary":"read","model_lease":"model","permission_ceiling":"read-only","resource_scopes":[],"revision":1,"status":"open","teams":{},"agents":{},"tasks":{},"topics":{},"artifacts":{},"final_artifact_ref":null,"unresolved":[]})).unwrap();
        let seal = json!({"schema_version":1,"primary_stream":"primary","primary_key":"read","index_stream":"execution-agent-receipts:graph:node:1","index_key":"index","session_id":"session","invocation_id":"call","tool_name":"read_ref","graph_id":"graph","node_id":"node","attempt":1,"step":{"step_id":"step","session_id":"session","turn_id":"test-turn","execution_id":"graph","run_ref":"run","actor_ref":"agent","build_ref":"test","attempt":null,"epoch":null,"goal_ref":null,"program_ref":null,"task_ref":null},"origin":{"origin_step_id":"step","call_id":"call","catalog_digest":"catalog","source":{"kind":"process","worker_id":"run","sequence":1}},"input_sha256":"input","body":{"schema_version":1,"source_ref":"artifact","source_sha256":"sha256:source","source_bytes":2,"output_sha256":"sha256:output","output_bytes":2,"extent":{"kind":"artifact_chunks","total_chunks":1,"chunks":[]}}});
        let descriptor = json!({"tool_id":"read_ref","descriptor_hash":"descriptor","idempotency":"idempotent","effect_kind":"read","scopes":[],"required_permission":"read-only","approval_class":"none","uses_network":false,"spawns_process":false,"mutates_packages":false,"mutates_system":false});
        let payload = json!({"read_acquisition_v1":seal,"outcome":{"status":"executed"},"tool_use_id":"call","tool_name":"read_ref","input_sha256":"input","replay":{"parent_execution":{},"invocation":{"effect":descriptor}}});
        let primary = event("primary", "execution.read.receipt", 3, payload.clone());
        let index = event(
            "execution-agent-receipts:graph:node:1",
            "execution.agent_tool.receipt",
            3,
            payload,
        );
        let presentation = "OK";
        let delivery = json!({"schema_version":1,"grade":"agent_channel_send","acquisition":seal,"acquisition_event_id":primary.event_id,"acquisition_cursor":3,"index_event_id":index.event_id,"session_id":"session","execution_id":"graph","turn_id":"test-turn","tool_invocation_id":"call","recipient":{"kind":"agent_task","identity":packet.assignment.execution_identity},"presentation_sha256":"sha256:565339bc4d33d72817b583024112eb7f5cdf3e5eef0252d6ec1b9c9a94e12bb3","presentation_bytes":2,"body_omitted":true,"delivered_extent":null,"transport":{"kind":"process_channel_send","worker_id":"run","request_id":"call","sequence":1,"step_id":"step","request_fingerprint":"fingerprint","response_sha256":"response","response_bytes":2}});
        let mut delivered = event(
            "session:session",
            crate::context_evidence::PROCESS_DELIVERY_KIND,
            5,
            delivery,
        );
        delivered.scope = RuntimeEventScope::Session;
        delivered.actor = Some("runtime.process_jsonl".into());
        delivered.status = Some("sent".into());
        let identity = json!([
            "session",
            "graph",
            "node",
            "run",
            "agent",
            1,
            "binding-digest"
        ]);
        let stream = format!(
            "agent-process-transport:{:x}",
            Sha256::digest(identity.to_string().as_bytes())
        );
        let key = format!("completed:{:x}", Sha256::digest(b"call"));
        let transport = event(
            &stream,
            "agent.process_request_completed",
            4,
            json!({"fingerprint":"fingerprint","response":{"output":presentation}}),
        );
        (
            Reader {
                keys: BTreeMap::from([
                    (("primary".into(), "read".into()), primary),
                    (
                        (
                            "execution-agent-receipts:graph:node:1".into(),
                            "index".into(),
                        ),
                        index,
                    ),
                    ((stream, key), transport),
                ]),
                ..Default::default()
            },
            program,
            packet,
            delivered,
        )
    }
    #[test]
    fn agent_reader_keeps_exact_attempt_transport_and_source_fence() {
        let (reader, program, packet, event) = agent();
        let run = |reader: &mut Reader, packet: &harness_contract::agent::AgentTaskPacket| {
            review_delivery_snapshot_from_events(
                reader,
                &program,
                Some(packet),
                vec![event.clone()],
                BTreeMap::new(),
            )
        };
        let mut good = reader.clone();
        let snapshot = run(&mut good, &packet).unwrap();
        assert_eq!(snapshot.reads.len(), 1);
        assert_eq!(snapshot.reads[0].event.event_id, "event-session:session-5");
        assert_eq!(snapshot.sources.len(), 3);
        let mut foreign = packet.clone();
        foreign.attempt = 2;
        let mut r = reader.clone();
        assert!(run(&mut r, &foreign).unwrap().reads.is_empty());
        assert!(r.calls.is_empty());
        let mut wrong = reader.clone();
        for r in wrong
            .keys
            .values_mut()
            .filter(|r| r.kind == "agent.process_request_completed")
        {
            r.payload["response"]["output"] = json!("wrong");
        }
        assert_eq!(
            run(&mut wrong, &packet).err().unwrap().to_string(),
            "Process delivered body does not match its actual response"
        );
        let mut missing = reader.clone();
        missing.keys.remove(&("primary".into(), "read".into()));
        assert_eq!(
            run(&mut missing, &packet).err().unwrap().to_string(),
            "delivered read has no original primary receipt"
        );
        let mut changed = reader.clone();
        changed.moved = true;
        assert_eq!(
            run(&mut changed, &packet).err().unwrap().to_string(),
            "read delivery source moved during validation"
        );
        for op in ["key", "revision"] {
            let mut bad = reader.clone();
            bad.fault = Some(op);
            assert_eq!(
                run(&mut bad, &packet).err().unwrap().to_string(),
                format!("reader:{op}")
            );
        }
    }

    // Reuse the existing physical-delivery reader fixture. A different producer
    // coordinate or selected primary receipt must not be accepted as this read.
    #[test]
    fn agent_delivery_rejects_foreign_producer_coordinates_and_primary() {
        let (reader, program, packet, delivery) = agent();
        let run = |reader: &mut Reader, event: DurableRuntimeEvent| {
            review_delivery_snapshot_from_events(reader, &program, Some(&packet), vec![event], BTreeMap::new())
        };
        let mut good = reader.clone();
        assert_eq!(run(&mut good, delivery.clone()).unwrap().reads.len(), 1);
        for (field, value) in [("graph_id", json!("foreign-graph")), ("node_id", json!("foreign-node")),
            ("attempt", json!(2)), ("session_id", json!("foreign-session"))] {
            let mut changed = delivery.clone();
            changed.payload["acquisition"][field] = value;
            let mut source = reader.clone();
            let result = run(&mut source, changed);
            assert!(!matches!(result, Ok(snapshot) if !snapshot.reads.is_empty()), "foreign {field} accepted");
        }
        let mut substituted = reader.clone();
        substituted.keys.get_mut(&(String::from("primary"), String::from("read"))).unwrap().event_id = "foreign-primary".into();
        let result = run(&mut substituted, delivery);
        assert!(!matches!(result, Ok(snapshot) if !snapshot.reads.is_empty()), "foreign primary accepted");
    }
}

#[cfg(test)]
mod s1_fixture;
