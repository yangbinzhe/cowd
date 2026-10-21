//! Read the existing Runtime strategy and tool journal; do not invent policy.
use crate::agentic::program::AgenticProgramProjection;
use crate::{ExpectedStreamRevision, RuntimeEventStore};
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
    pub retiring_optional_task_refs: BTreeSet<String>,
    pub semantic_digest: String,
    pub policy_digest: String,
    pub effects: RootReviewPolicySnapshot,
    pub sources: Vec<ExpectedStreamRevision>,
}

impl ProofInputSnapshot {
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

fn review_covers_reference(
    program: &AgenticProgramProjection,
    reviewed: &[String],
    reference: &str,
) -> bool {
    let Ok(wanted) = super::review_result_identity(program, reference) else {
        return false;
    };
    reviewed.iter().any(|reviewed| {
        super::review_result_identity(program, reviewed)
            .is_ok_and(|identity| identity == wanted)
    })
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
        let inputs = proof
            .input_set
            .as_ref()
            .ok_or("completion context v2 inputs missing")?;
        super::validate_explicit_process_evidence(store, proof, &review.evidence_refs)?;
        if review.decision != "satisfied"
            || review.spec_revision != goal.spec_revision
            || review.input_manifest_digest != goal.spec_digest
            || inputs.condition_ref.as_deref() != Some(condition.as_str())
            || results.is_empty()
            || review.evidence_refs.is_empty()
            || review.producer_refs.is_empty()
            || (proof.independence_required
                && review.producer_refs.contains(&review.reviewer_actor))
            || proof.producer_execution_ids.is_empty()
            || proof.producer_execution_ids.iter().any(|producer| {
                producer.is_empty()
                    || (proof.independence_required && producer == &proof.reviewer_execution_id)
            })
        {
            return Err(format!(
                "completion context requires current satisfied review:{condition}"
            ));
        }
        for reference in &results {
            if !review_covers_reference(program, &review.result_refs, reference)
                || !proof.reads.iter().any(|read| {
                    review_covers_reference(
                        program,
                        std::slice::from_ref(&read.result_ref),
                        reference,
                    )
                    && !read.sha256.is_empty()
                    && !read.receipt_refs.is_empty()
                    && (read.result_kind != harness_contract::goal::GoalResultKind::ToolEffect
                        || !read.effect_observation_refs.is_empty())
                })
            {
                return Err(format!(
                    "completion context review does not cover result:{reference}"
                ));
            }
        }
        let snapshot = validate_proof_inputs(store, program, Some(&goal), proof, readers)?;
        if !snapshot.reviewer_matches(program, review) {
            return Err("completion context review physical identity changed".into());
        }
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

pub(crate) fn validate_proof_inputs(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
) -> Result<ProofInputSnapshot, String> {
    let current = crate::AgentActionService::new(std::sync::Arc::clone(store))
        .project(&program.program_id)
        .map_err(|error| error.to_string())?;
    if current.session_id != program.session_id || current.objective_id != program.objective_id {
        return Err("proof Program identity changed".into());
    }
    let current_goal = goal
        .map(|goal| {
            crate::execution_core::GoalStore::new(std::sync::Arc::clone(store)).get(&goal.id)
        })
        .transpose()?
        .flatten();
    let snapshot = proof_input_snapshot(store, &current, current_goal.as_ref(), proof, readers)?;
    let inputs = proof.input_set.as_ref().ok_or("missing v2 proof inputs")?;
    if snapshot.semantic_digest != inputs.semantic_digest
        || snapshot.policy_digest != inputs.acceptance_policy_digest
        || snapshot.work_digest != proof.work_manifest_digest
        || proof.effect_manifest_digest.as_deref() != Some(snapshot.effects.digest.as_str())
    {
        return Err("stale v2 proof: relevant result, source or acceptance policy changed".into());
    }
    Ok(snapshot)
}

pub(crate) fn committed_proof_sources(
    store: &std::sync::Arc<RuntimeEventStore>,
    goal: &GoalContract,
    readers: super::ResultContentReaders<'_>,
) -> Result<Vec<ExpectedStreamRevision>, String> {
    let mut sources = std::collections::BTreeMap::new();
    let program = goal
        .execution_binding
        .as_ref()
        .map(|binding| {
            crate::AgentActionService::new(std::sync::Arc::clone(store))
                .project(&binding.agentic_program_id)
        })
        .transpose()
        .map_err(|error| error.to_string())?;
    let program = program
        .as_ref()
        .ok_or("committed A has no Program binding")?;
    if goal
        .user_intent_criterion_id
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Err("committed A missing original intent condition".into());
    }
    for condition in super::required_review_conditions(goal) {
        let review = goal
            .reviews
            .iter()
            .rev()
            .find(|review| review.criterion_ref == condition)
            .ok_or_else(|| format!("committed A missing required review {condition}"))?;
        let proof = review
            .verification
            .as_ref()
            .ok_or_else(|| format!("committed A missing verification {condition}"))?;
        let inputs = proof
            .input_set
            .as_ref()
            .ok_or_else(|| format!("committed A unsupported required review {condition}"))?;
        super::validate_explicit_process_evidence(store, proof, &review.evidence_refs)?;
        if inputs.version != 2
            || inputs.semantic_digest.is_empty()
            || inputs.condition_ref.as_deref() != Some(condition.as_str())
            || review.decision != "satisfied"
            || review.spec_revision != goal.spec_revision
            || review.input_manifest_digest != goal.spec_digest
            || proof.goal_spec_revision != Some(goal.spec_revision)
            || proof.goal_spec_digest.as_deref() != Some(goal.spec_digest.as_str())
            || proof.reviewer_execution_id.is_empty()
            || proof.producer_execution_ids.is_empty()
            || inputs.material_events.is_empty()
            || review.result_refs.is_empty()
            || review.evidence_refs.is_empty()
            || review.producer_refs.is_empty()
            || proof.producer_execution_ids.iter().any(|producer| {
                producer.is_empty()
                    || (proof.independence_required && producer == &proof.reviewer_execution_id)
            })
            || (proof.independence_required
                && review.producer_refs.contains(&review.reviewer_actor))
        {
            return Err(format!(
                "committed A proof identity is invalid: {condition}"
            ));
        }
        let mut results = review.result_refs.clone();
        if goal.user_intent_criterion_id.as_ref() == Some(&condition) {
            let request = program
                .completion_request
                .as_ref()
                .ok_or("committed A missing completion request")?;
            results.extend(crate::execution_core::goal::delivered_result_refs(
                program,
                goal,
                &request.result_refs,
            ));
        }
        for obligation in goal
            .obligations
            .iter()
            .filter(|obligation| obligation.required && obligation.obligation_id == condition)
        {
            results.extend(obligation.artifact_refs.iter().cloned());
        }
        for reference in results {
            let identity = super::review_result_identity(program, &reference)?;
            let covers = |refs: &[String]| {
                refs.iter().any(|reference| {
                    super::review_result_identity(program, reference)
                        .is_ok_and(|current| current == identity)
                })
            };
            if !covers(&review.result_refs)
                || !covers(&inputs.result_refs)
                || !proof.reads.iter().any(|read| {
                    super::review_result_identity(program, &read.result_ref)
                        .is_ok_and(|current| current == identity)
                        && !read.sha256.is_empty()
                        && !read.receipt_refs.is_empty()
                        && (read.result_kind != harness_contract::goal::GoalResultKind::ToolEffect
                            || !read.effect_observation_refs.is_empty())
                })
            {
                return Err(format!(
                    "committed A required result coverage missing: {condition}"
                ));
            }
        }
        let snapshot = proof_input_snapshot_for_phase(
            store,
            program,
            Some(goal),
            proof,
            Some(&inputs.acceptance_policy_digest),
            readers,
        )?;
        if !snapshot.reviewer_matches(program, review)
            || snapshot.semantic_digest != inputs.semantic_digest
            || snapshot.work_digest != proof.work_manifest_digest
            || proof.effect_manifest_digest.as_deref() != Some(snapshot.effects.digest.as_str())
        {
            return Err("committed A relevant effect or material changed; delivery requires authorized recovery".into());
        }
        for source in snapshot.sources {
            if sources
                .insert(source.stream_id, source.expected_revision)
                .is_some_and(|previous| previous != source.expected_revision)
            {
                return Err("committed A current dependency cut changed".into());
            }
        }
        for source in inputs.source_revisions.keys() {
            let current = store
                .stream_revision(source)
                .map_err(|error| error.to_string())?;
            if sources
                .insert(source.clone(), current)
                .is_some_and(|previous| previous != current)
            {
                return Err("committed A source changed between required reviews".into());
            }
        }
        for material in &inputs.material_events {
            verify_material_event(store, material, &mut sources)?;
        }
        for (source, expected) in &proof.result_source_revisions {
            if sources.get(source) != Some(expected) {
                return Err("committed A approval/result source changed; delivery requires authorized recovery".into());
            }
        }
    }
    for (source, expected) in &sources {
        if store
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != *expected
        {
            return Err("committed A source moved while preparing B".into());
        }
    }
    Ok(sources
        .into_iter()
        .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
            stream_id,
            expected_revision,
        })
        .collect())
}

#[derive(Clone, Copy)]
enum PinnedEventProjection {
    Effects,
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
    let revision = store
        .stream_revision(source)
        .map_err(|error| error.to_string())?;
    if sources
        .insert(source.into(), revision)
        .is_some_and(|previous| previous != revision)
    {
        return Err("proof source changed during validation".into());
    }
    let mut events = Vec::new();
    let mut through = revision;
    loop {
        let page = store.list_stream_before(source, through, 64, 1024 * 1024)?;
        if page.is_empty() {
            break;
        }
        if store
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != revision
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
                if event.payload["tasks"].as_array().is_some_and(|tasks| {
                    tasks.iter().all(|task| {
                        task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                            || task["effect"]["effect_kind"] == "read"
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
    if store
        .stream_revision(source)
        .map_err(|error| error.to_string())?
        != revision
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
) -> Result<ReviewDeliverySnapshot, String> {
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
    review_delivery_snapshot_from_events(store, program, packet, events, sources, |stream, key| {
        store
            .event_by_idempotency_key(stream, key)
            .map_err(|error| error.to_string())
    })
}

fn review_delivery_snapshot_from_events(
    store: &RuntimeEventStore,
    program: &AgenticProgramProjection,
    packet: Option<&harness_contract::agent::AgentTaskPacket>,
    events: Vec<crate::DurableRuntimeEvent>,
    mut sources: std::collections::BTreeMap<String, u64>,
    mut original_event: impl FnMut(&str, &str) -> Result<Option<crate::DurableRuntimeEvent>, String>,
) -> Result<ReviewDeliverySnapshot, String> {
    use crate::context_evidence::{
        ReadDeliveryGrade, ReadDeliveryTransport, NATIVE_DELIVERY_KIND, PROCESS_DELIVERY_KIND,
        READ_ACQUISITION_FIELD,
    };
    let mut reads = Vec::new();
    for event in events {
        let expected = match event.kind.as_str() {
            NATIVE_DELIVERY_KIND => ("conversation_runtime", "confirmed"),
            PROCESS_DELIVERY_KIND => ("runtime.process_jsonl", "sent"),
            _ => continue,
        };
        if event.scope != crate::RuntimeEventScope::Session
            || event.schema_version != 1
            || event.actor.as_deref() != Some(expected.0)
            || event.status.as_deref() != Some(expected.1)
        {
            continue;
        }
        let material = crate::context_evidence::decode_read_delivery(&event.payload)
            .map_err(str::to_string)?;
        #[cfg(test)]
        eprintln!(
            "R1_READ_208_CANDIDATE {}",
            serde_json::json!({
                "event": event.event_id,
                "invocation": material.tool_invocation_id,
                "session_matches": material.session_id == program.session_id,
                "turn_matches": material.turn_id == program.turn_id,
                "recipient": material.recipient,
                "grade": material.grade,
                "step": material.acquisition.step,
                "origin": material.acquisition.origin,
                "graph": material.acquisition.graph_id,
                "node": material.acquisition.node_id,
                "attempt": material.acquisition.attempt,
                "expected": packet.map(|packet| serde_json::json!({
                    "identity": packet.assignment.execution_identity,
                    "agent_id": packet.agent_id(),
                    "principal_id": packet.assignment.execution_identity.principal_id(),
                    "graph": packet.graph_id(), "node": packet.node_id(),
                    "attempt": packet.attempt, "run": packet.run_id(),
                    "tool_allowed": packet.allowed_tools.contains(&material.acquisition.tool_name),
                    "tool_bound": packet.binding.as_ref().is_some_and(|binding|
                        binding.tool_contract_refs.contains(&material.acquisition.tool_name)),
                    "recipient_identity_matches": match &material.recipient {
                        crate::context_evidence::ReadDeliveryRecipient::AgentTask { identity } =>
                            *identity == packet.assignment.execution_identity,
                        _ => false,
                    },
                    "graph_matches": material.acquisition.graph_id == packet.graph_id(),
                    "node_matches": material.acquisition.node_id == packet.node_id(),
                    "attempt_matches": material.acquisition.attempt == packet.attempt,
                    "run_matches": material.acquisition.step.run_ref.as_deref() == Some(packet.run_id()),
                    "agent_actor_matches": material.acquisition.step.actor_ref.as_deref() == Some(packet.agent_id()),
                    "principal_actor_matches": material.acquisition.step.actor_ref.as_deref()
                        == Some(packet.assignment.execution_identity.principal_id()),
                })),
            })
        );
        if material.session_id != program.session_id || material.turn_id != program.turn_id {
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
                let original = verify_material_event(store, &reference, &mut sources)?;
                crate::context_evidence::root_read_owner_matches(owner, &original, seal)
                    && program.root_execution_id.as_deref()
                        == Some(owner.binding.root_execution_id.as_str())
                    && super::binding_is_owned_by_root(
                        &owner.binding,
                        program.root_execution_id.as_deref(),
                    )
            }
            _ => false,
        };
        #[cfg(test)]
        eprintln!(
            "R1_READ_208_RECIPIENT {}",
            serde_json::json!({
                "event": event.event_id,
                "invocation": material.tool_invocation_id,
                "matches": recipient_matches,
            })
        );
        if !recipient_matches {
            continue;
        }
        let primary = original_event(&seal.primary_stream, &seal.primary_key)?
            .ok_or("delivered read has no original primary receipt")?;
        let receipt = original_event(&seal.index_stream, &seal.index_key)?
            .ok_or("delivered read has no original index receipt")?;
        let seal_value = serde_json::to_value(seal).map_err(|error| error.to_string())?;
        if primary.kind != "execution.read.receipt"
            || receipt.kind != "execution.agent_tool.receipt"
            || primary.actor.as_deref() != Some("governed_tool")
            || receipt.actor != primary.actor
            || primary.scope != crate::RuntimeEventScope::ExecutionNode
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
            super::results::authenticate_root_receipt_owner(store, &primary, &mut sources)?;
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
                let completed = original_event(&transport_stream, &transport_key)?
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
                let revision = store
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
            let revision = store
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
        if store
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != *expected
        {
            return Err("read delivery source moved during validation".into());
        }
    }
    Ok(ReviewDeliverySnapshot { reads, sources })
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
            review_delivery_snapshot_from_events(
                store,
                program,
                Some(expected_packet),
                vec![candidate],
                std::collections::BTreeMap::new(),
                |stream, key| {
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
            )
        };
        let accepted = run(event.clone(), primary.clone(), index.clone(), packet).unwrap();
        assert_eq!(accepted.reads.len(), 1);
        assert_eq!(accepted.reads[0].material, material);
        let refused = |label: &str, result: Result<ReviewDeliverySnapshot, String>| {
            assert!(
                result.is_err()
                    || result
                        .as_ref()
                        .is_ok_and(|snapshot| snapshot.reads.is_empty()),
                "{label}: a foreign delivery entered the consumer read set"
            );
        };
        let physical_error = |label: &str, result: Result<ReviewDeliverySnapshot, String>| {
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
) -> Result<(crate::DurableRuntimeEvent, crate::DurableRuntimeEvent), String> {
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
                || plan.scope != crate::RuntimeEventScope::Tool
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
        || primary.scope != crate::RuntimeEventScope::ExecutionNode
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
    if material.sequence == 0 {
        return Err("proof material sequence is missing".into());
    }
    let revision = store
        .stream_revision(&material.stream_id)
        .map_err(|error| error.to_string())?;
    if sources
        .insert(material.stream_id.clone(), revision)
        .is_some_and(|previous| previous != revision)
    {
        return Err("proof material source changed during validation".into());
    }
    let events = store.list_stream_after(
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
                scope: crate::RuntimeEventScope::Session,
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
        actual_input_snapshot_for_phase(&store, &program, &[], readers, Some(&original), true)
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
    ) -> Result<Self, String> {
        let acquisition = crate::context_evidence::decode_read_acquisition(
            &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
        )
        .map_err(str::to_string)?;
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
            || primary.scope != crate::RuntimeEventScope::ExecutionNode
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
                    && acquisition.step.execution_id.as_deref() == Some(packet.graph_id())
                    && acquisition.step.actor_ref.as_deref() == Some(packet.agent_id())
                    && acquisition.step.attempt == Some(packet.attempt),
                Some("provider") => acquisition.origin["source"]["attempt_id"].as_str().is_some_and(|attempt| !attempt.is_empty())
                    && acquisition.step.execution_id.as_deref() == Some(packet.run_id())
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
                return Err("immutable acquisition has no authenticated root producer".into());
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
        let source_selector = if source.starts_with("artifact:")
            && !source.starts_with("artifact://art_")
            && !source.starts_with("artifact://sha256:")
        {
            let identity = super::review_result_identity(program, source)?;
            program.artifacts.get(&identity).map_or(source.as_str(), |artifact| {
                artifact.content_ref.as_str()
            })
        } else {
            source.as_str()
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
        let mut reference = requested.to_string();
        let requested_identity = program.artifacts.values().any(|artifact| {
            super::same_reference_spelling(&artifact.artifact_ref, requested)
        });
        let source_identity = program.artifacts.values().any(|artifact| {
            super::same_reference_spelling(&artifact.artifact_ref, source)
        });
        if requested_identity {
            reference = super::review_result_identity(program, requested)?;
            if program.artifacts[&reference].content_ref != content.selector
                || source_identity && super::review_result_identity(program, source)? != reference
            {
                return Err("immutable acquisition exact reference conflicts with original content".into());
            }
        } else if source_identity {
            reference = super::review_result_identity(program, source)?;
            if program.artifacts[&reference].content_ref != content.selector {
                return Err("immutable acquisition sealed publication conflicts with content".into());
            }
        } else if requested.starts_with("artifact:") && !requested.starts_with("artifact://") {
            return Err("immutable acquisition exact publication binding missing".into());
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
                if program.artifacts.values().any(|artifact| {
                    super::same_reference_spelling(&artifact.artifact_ref, &evidence.evidence_ref.id)
                }) {
                    bound.insert(super::review_result_identity(program, &evidence.evidence_ref.id)?);
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
        body: &harness_contract::context::ArtifactRef,
        process_runs: &[super::results::AuthenticatedProcessExecution],
    ) -> Result<Option<String>, String> {
        if self.reference == self.content.selector
            && (&self.content == body || process_runs.iter().any(|run| run.fact.body == self.content))
        {
            return Ok(None);
        }
        super::review_result_identity(program, &self.reference).map(Some)
    }

    pub(super) fn validate_dependency(
        &self,
        program: &AgenticProgramProjection,
        reference: &str,
        dependency: &super::results::ResolvedReviewResult,
    ) -> Result<(), String> {
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
    Err(
        "proof_actual_input_extent_unsupported: actual File input lacks complete original windows"
            .into(),
    )
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

fn collect_actual_acquisition(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent,
    session_events: &[crate::DurableRuntimeEvent],
    boundary: &super::results::ResultInputBoundary,
    readers: super::ResultContentReaders<'_>,
    actual: &mut ActualInputSnapshot,
    windows: &mut std::collections::BTreeMap<String, Vec<ActualFileWindow>>,
) -> Result<(), String> {
    use crate::context_evidence::{ReadExtent, READ_ACQUISITION_FIELD};
    let descriptor = super::results::receipt_effect_descriptor(primary)?;
    if primary.kind != "execution.read.receipt"
        || primary.schema_version != 1
        || crate::ExecutionCommitService::receipt_observation_sequence(&index).is_none()
        || primary.actor.as_deref() != Some("governed_tool")
        || primary.actor != index.actor
        || primary.scope != crate::RuntimeEventScope::ExecutionNode
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
        || primary.commit_cursor >= boundary.cursor
        || descriptor.effect_kind != ToolEffectKind::Read
    {
        return Err("actual input does not authenticate one strictly earlier acquisition".into());
    }
    for event in [primary, index] {
        super::results::pin_result_source(store, &event.stream_id, &mut actual.sources)?;
        retain_actual_material(&mut actual.material_events, event);
    }
    let seal_value = &primary.payload[READ_ACQUISITION_FIELD];
    if seal_value.is_null() {
        if !immutable_receipt_without_workspace_extent(primary, &descriptor)? {
            return Err("proof_actual_input_extent_unsupported: actual read lacks an original physical extent".into());
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
        return Ok(());
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
            let scope = scope
                .as_ref()
                .ok_or("actual File input lacks original scope v2")?;
            if !scope.complete
                || *start_line == 0
                || *source_omitted && *start_line == 1 && line_count == total_lines
            {
                return Err(
                    "proof_actual_input_extent_unsupported: original File body is incomplete"
                        .into(),
                );
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
                return Err("proof_actual_input_extent_unsupported: actual File has no unique original canonical target".into());
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
            };
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
            )?;
            if !actual.immutable_inputs.contains(&input) {
                actual.immutable_inputs.push(input);
            }
        }
    }
    Ok(())
}

fn authenticate_unsealed_delegated_input(
    store: &std::sync::Arc<RuntimeEventStore>,
    primary: &crate::DurableRuntimeEvent,
    index: &crate::DurableRuntimeEvent,
    packet: &harness_contract::agent::AgentTaskPacket,
    actual: &mut ActualInputSnapshot,
) -> Result<serde_json::Value, String> {
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
                identity.execution_id.as_deref() == Some(packet.run_id())
                    && identity
                        .attempt
                        .is_none_or(|attempt| attempt == packet.attempt)
                    && identity.actor_ref.as_deref()
                        == Some(packet.assignment.execution_identity.principal_id())
                    && attempt_id == &step.snapshot.provider_attempt.attempt_id
            }
            CallSource::Process { worker_id, .. } => {
                identity.execution_id.as_deref() == Some(packet.graph_id())
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

#[cfg(test)]
tokio::task_local! {
    static ACTUAL_READ_SOURCE_FAULT: &'static str;
}

#[cfg(test)]
fn actual_read_source_fault(primary: &mut crate::DurableRuntimeEvent) {
    if primary.payload["tool_name"] == "evidence_retrieve" {
        let _ = ACTUAL_READ_SOURCE_FAULT.try_with(|fault| match *fault {
            "artifact-body" => {
                primary.payload["outcome"]["output"] = serde_json::json!("changed original body")
            }
            "artifact-hash" => {
                primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]["body"]
                    ["source_sha256"] = serde_json::json!("sha256:foreign")
            }
            "artifact-chunk" => {
                primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]["body"]["extent"]
                    ["chunks"][0]["body_sha256"] = serde_json::json!("sha256:foreign")
            }
            "artifact-source" => {
                primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]["body"]
                    ["source_ref"] = serde_json::json!("artifact://foreign")
            }
            "artifact-producer" => {
                primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]["step"]
                    ["run_ref"] = serde_json::json!("foreign-run")
            }
            "artifact-attempt" => {
                primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]["attempt"] =
                    serde_json::json!(99)
            }
            _ => {}
        });
        return;
    }
    if primary.payload["tool_name"] != "tool_search" {
        return;
    }
    let _ = ACTUAL_READ_SOURCE_FAULT.try_with(|fault| match *fault {
        "session" => primary.payload["replay"]["session_id"] = serde_json::json!("foreign-session"),
        "run" => {
            primary.payload["replay"]["origin"]["source"] =
                serde_json::json!({"kind":"process","worker_id":"foreign-run","sequence":1})
        }
        "step" => {
            primary.payload["replay"]["origin"]["origin_step_id"] =
                serde_json::json!("foreign-step")
        }
        "call" => primary.payload["replay"]["call_id"] = serde_json::json!("foreign-call"),
        "input" => primary.payload["input_sha256"] = serde_json::json!("sha256:foreign-input"),
        "compiled" => {
            primary.payload["replay"]["invocation"]["invocation_id"] =
                serde_json::json!("foreign-compiled-call")
        }
        _ => {}
    });
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
) -> Result<ActualInputSnapshot, String> {
    let mut actual = ActualInputSnapshot {
        file_dependencies: Vec::new(),
        immutable_inputs: Vec::new(),
        sources: std::collections::BTreeMap::new(),
        material_events: Vec::new(),
        canonical_facts: Vec::new(),
    };
    let source = format!("session:{}", program.session_id);
    let events = super::results::result_source_events(store, &source, &mut actual.sources)?;
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
            store,
            &owner,
            producer.packet.as_ref(),
            events.clone(),
            std::collections::BTreeMap::new(),
            |stream, key| {
                store
                    .event_by_idempotency_key(stream, key)
                    .map_err(|error| error.to_string())
            },
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
                store, program, &primary, &read.receipt, &events,
                producer.packet.as_ref(), readers, &mut actual,
            )?;
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
        assert_eq!(contents.len(), 2);
        remember_primary_content(&mut contents, "artifact:summary", &first).unwrap();
        assert_eq!(contents.len(), 2);
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

/// Whether this planned root call IS the review that is being built.
///
/// The root plan carries the call that performs the review, so while the
/// proof is assembled that call is still pending. The exclusion is the same
/// narrow one the delegated write check applies: the reviewer's own
/// authenticated review call, and only that one.
fn is_current_review_control_step(
    reviewer_execution_id: &str,
    tool_name: &str,
    step: &crate::step_snapshot::StepCallBinding,
) -> bool {
    if !matches!(tool_name, "task_review" | "objective_review") {
        return false;
    }
    let run = reviewer_execution_id
        .strip_prefix("agent-run:")
        .unwrap_or(reviewer_execution_id);
    step.snapshot.identity.execution_id.as_deref() == Some(run)
}

/// Whether this in-flight delegated write IS the review that is being built.
///
/// A reviewer performs the review as a control action, so while the proof is
/// assembled the intent of that very call is still in flight. The proof
/// checks every delegated write that may intersect the required work, and it
/// found this one unsettled, which is a self-dependency rather than a real
/// gap: the review can never settle the action it is.
///
/// The exclusion is deliberately narrow. The call must be the reviewer's own
/// governed call, proven from the execution identity the proof was built
/// under, and it must be a review control action. Another agent's in-flight
/// write, and another write by this same reviewer, are still refused.
fn is_current_review_control_call(
    reviewer_execution_id: &str,
    intent: &crate::DurableRuntimeEvent,
) -> bool {
    let tool = intent.payload["tool_name"].as_str().unwrap_or_default();
    if !matches!(tool, "task_review" | "objective_review") {
        return false;
    }
    let run = reviewer_execution_id
        .strip_prefix("agent-run:")
        .unwrap_or(reviewer_execution_id);
    let graph = run.strip_prefix("run:").unwrap_or(run);
    intent
        .stream_id
        .starts_with(&format!("execution-agent-receipts:{graph}:"))
}

pub(super) fn actual_input_snapshot(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    boundaries: &[super::results::ResultInputBoundary],
    readers: super::ResultContentReaders<'_>,
    expected_cuts: Option<&std::collections::BTreeMap<String, u64>>,
) -> Result<ActualInputSnapshot, String> {
    actual_input_snapshot_for_phase(store, program, boundaries, readers, expected_cuts, false)
}

pub(super) fn actual_input_snapshot_for_phase(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    boundaries: &[super::results::ResultInputBoundary],
    readers: super::ResultContentReaders<'_>,
    expected_cuts: Option<&std::collections::BTreeMap<String, u64>>,
    allow_history_growth: bool,
) -> Result<ActualInputSnapshot, String> {
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
    let session_events =
        super::results::result_source_events(store, &session, &mut actual.sources)?;
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
                        return Err("proof_actual_input_extent_unsupported: failed File read is not an existence proof".into());
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
                #[cfg(test)]
                let primary = {
                    let mut original = primary;
                    actual_read_source_fault(&mut original);
                    original
                };
                if !event.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null() {
                    super::results::authenticate_read_input(
                        store,
                        &event,
                        packet,
                        &mut actual.sources,
                    )?;
                } else {
                    authenticate_unsealed_delegated_input(
                        store,
                        &primary,
                        &event,
                        packet,
                        &mut actual,
                    )?;
                }
                collect_actual_acquisition(
                    store,
                    program,
                    &primary,
                    &event,
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
                    return Err("proof_actual_input_extent_unsupported: failed root read has no input extent".into());
                }
                let task = super::results::result_tool_plan(&session_events, event)?
                    .ok_or("actual root input original plan missing")?;
                let primary =
                    super::results::original_tool_primary(store, event, task, &mut actual.sources)?;
                let value = &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD];
                let index = if !value.is_null() {
                    let seal = crate::context_evidence::decode_read_acquisition(value)
                        .map_err(str::to_string)?;
                    super::results::pin_result_source(
                        store,
                        &seal.index_stream,
                        &mut actual.sources,
                    )?;
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
                    super::results::pin_result_source(store, &source, &mut actual.sources)?;
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
            ));
        }
        let current = store
            .stream_revision(source)
            .map_err(|error| error.to_string())?;
        if current != *revision {
            return Err(format!(
                "actual input source cut is missing or changed; rebuild the complete review: source={source:?}, observed={revision}, current={current}",
            ));
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
) -> Result<Option<super::results::AuthenticatedProcessExecution>, String> {
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
) -> Result<harness_contract::goal::ProofEffectScope, String> {
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
}

fn authenticate_settled_effect(
    store: &std::sync::Arc<RuntimeEventStore>,
    intent: &crate::DurableRuntimeEvent,
    receipt: &crate::DurableRuntimeEvent,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<ToolEffectDescriptor, String> {
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

fn immutable_receipt_without_workspace_extent(
    primary: &crate::DurableRuntimeEvent,
    descriptor: &ToolEffectDescriptor,
) -> Result<bool, String> {
    if !primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null()
        || descriptor.effect_kind != ToolEffectKind::Read
        || descriptor
            .scopes
            .iter()
            .any(|scope| scope.resource == harness_contract::policy::PermissionResource::File)
        || descriptor.uses_network
        || descriptor.spawns_process
        || descriptor.mutates_packages
        || descriptor.mutates_system
    {
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
) -> Result<Option<harness_contract::goal::ProofEffectScope>, String> {
    if event.kind == "tool.invocation.completed" {
        let session = event.stream_id.clone();
        let events = super::results::result_source_events(store, &session, sources)?;
        let task = super::results::result_tool_plan(&events, event)?
            .ok_or("physical material lacks its original governed plan")?;
        let primary = super::results::original_tool_primary(store, event, task, sources)?;
        let descriptor = super::results::receipt_effect_descriptor(&primary)?;
        if immutable_receipt_without_workspace_extent(&primary, &descriptor)? {
            return Ok(None);
        }
        if !primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null() {
            let seal = crate::context_evidence::decode_read_acquisition(
                &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
            )
            .map_err(str::to_string)?;
            return read_original_scope(&primary, &descriptor, &seal);
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
        .map(Some);
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
    if immutable_receipt_without_workspace_extent(&primary, &descriptor)? {
        return Ok(None);
    }
    if primary.payload["resolved_effect_scope"].is_null() {
        let seal = crate::context_evidence::decode_read_acquisition(
            &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
        )
        .map_err(str::to_string)?;
        return read_original_scope(&primary, &descriptor, &seal);
    }
    super::results::require_original_effect_scope(
        &primary.payload["resolved_effect_scope"],
        &descriptor,
    )
    .map(Some)
}

pub(crate) fn proof_input_snapshot(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    readers: super::ResultContentReaders<'_>,
) -> Result<ProofInputSnapshot, String> {
    proof_input_snapshot_for_phase(store, program, goal, proof, None, readers)
}

fn proof_input_snapshot_for_phase(
    store: &std::sync::Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    proof: &harness_contract::goal::ObjectiveReviewVerification,
    accepted_policy_digest: Option<&str>,
    readers: super::ResultContentReaders<'_>,
) -> Result<ProofInputSnapshot, String> {
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
    let dependencies = inputs
        .result_refs
        .iter()
        .chain(&inputs.auxiliary_refs)
        .cloned()
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
    let session_events = pinned_events(store, &session, &mut sources, start_cursor)?;
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
        let genesis = store.list_stream_after(&graph_id, 0, 1, 1, 4 * 1024 * 1024)?;
        if genesis
            .first()
            .is_none_or(|event| event.commit_cursor > discovery_cursor)
        {
            return Err(
                "proof physical lineage changed during discovery; retry current inputs".into(),
            );
        }
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
    if proof.reads.iter().any(|read| {
        read.effect_observation_refs
            .iter()
            .any(|reference| super::is_process_body_receipt(reference))
    }) {
        let proof_value = serde_json::to_value(proof).map_err(|error| error.to_string())?;
        for event in super::results::result_source_events(
            store,
            &format!("agentic-program:{}", program.program_id),
            &mut sources,
        )? {
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
        let metadata = super::results::resolve_result_metadata_for_phase(
            store,
            program,
            &read.result_ref,
            &content,
            Some(&inputs.source_revisions),
            readers,
            !inputs.semantic_digest.is_empty(),
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
        let identity = super::review_result_identity(program, &read.result_ref)?;
        if metadata_identities
            .get(&identity)
            .is_some_and(|previous: &harness_contract::context::ArtifactRef| previous != &content)
        {
            return Err("proof duplicate read has conflicting original content identity".into());
        }
        metadata_identities.insert(identity.clone(), metadata.content.clone());
        pending_metadata.push((identity, metadata));
    }
    let (_, primary_artifacts, _) =
        super::selected_work(store, program, goal, &inputs.result_refs, &mut sources)?;
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
            let acquired = primary_producer_immutable_inputs(
                store,
                program,
                &metadata.producers,
                readers,
                Some(&inputs.source_revisions),
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
                let Some(reference) = input.dependency_identity(program, &metadata.content, &metadata.process_runs)? else {
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
                    let reference = super::review_result_identity(program, reference)?;
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
        if current != *revision {
            return Err("proof external decision source changed".into());
        }
        sources.insert(source.clone(), current);
    }
    if !physical_unknown.is_empty() {
        return Err(format!("effect_review_pending: opaque Process history may intersect required work: {physical_unknown:?}"));
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
        let mut current_control_calls: Vec<String> = Vec::new();
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
            if !settled && !disjoint {
                if is_current_review_control_call(&proof.reviewer_execution_id, &intent) {
                    // The review is a control action the reviewer performs
                    // while this proof is assembled, so its own intent is
                    // still in flight when the proof checks it. Requiring it
                    // to be settled first is a self-dependency: the review can
                    // never settle the action it is. Only that exact call is
                    // excluded, and only after it is authenticated as the
                    // reviewer's own governed call. Every other in-flight
                    // control change, including another write by this same
                    // reviewer, still fails closed.
                    current_control_calls.push(identity.to_string());
                    continue;
                }
                return Err(
                    "effect_review_pending: unknown required or possibly intersecting delegated write"
                        .into(),
                );
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
    let mut current_review_planned: BTreeSet<(String, String)> = BTreeSet::new();
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
                event, &session_events, &related_roots, &delegated_session_packets,
            ) {
                if authenticated_delegated_plan_settled(store, event, &session_events, packet, &mut sources)? {
                    required_effects.insert(event.event_id.clone(), event.payload.clone());
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
                planned.insert((plan.to_string(), call.to_string()));
                required_effects.insert(format!("{}:{call}", event.event_id), task.clone());
                let step: crate::step_snapshot::StepCallBinding =
                    serde_json::from_value(task["step_binding"].clone())
                        .map_err(|_| "root effect plan lacks its original Step identity")?;
                let call_key = step.effect_idempotency_key();
                if is_current_review_control_step(
                    &proof.reviewer_execution_id,
                    task["tool_name"].as_str().unwrap_or_default(),
                    &step,
                ) {
                    current_review_planned.insert((plan.to_string(), call.to_string()));
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
        // The review is a control action the reviewer performs while this
        // proof is assembled, so the call that carries it is still pending
        // here. Requiring it to settle first is a self-dependency. The
        // exclusion is the same narrow one the delegated write check applies:
        // only the reviewer's own authenticated review call, and only the
        // call the review is actually performing. Every other planned write
        // still has to settle.
        let pending = planned
            .difference(&completed)
            .filter(|(plan, call)| {
                !current_review_planned.contains(&(plan.clone(), call.clone()))
            })
            .collect::<Vec<_>>();
        if !pending.is_empty() {
            return Err(format!(
                "effect_review_pending: root writes must settle before acceptance; pending={pending:?}; terminal={completed:?}",
            ));
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
        !call_key.as_ref().is_some_and(|key| runtime_control_keys.contains(key))
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
                ));
            }
            if *cursor >= dependency.acquisition_cursor && *cursor >= read_cursor {
                return Err(format!(
                    "review_requires_effect_observation: {} original_acquisition={} complete_reviewer_read={read_cursor} write={cursor}",
                    target.path.workspace_relative_path, dependency.acquisition_cursor,
                ));
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
            ));
        }
    }
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
        if store
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != *revision
        {
            return Err("proof source changed during validation; retry current inputs".into());
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
    Ok(ProofInputSnapshot {
        work_digest,
        task_refs,
        artifact_refs,
        reviewer_execution_refs,
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
    })
}

fn authenticated_delegated_plan_settled(
    store: &std::sync::Arc<RuntimeEventStore>,
    plan: &crate::DurableRuntimeEvent,
    events: &[crate::DurableRuntimeEvent],
    packet: &harness_contract::agent::AgentTaskPacket,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<bool, String> {
    if plan.actor.as_deref() != Some("conversation_runtime")
        || plan.schema_version != 1 || plan.scope != crate::RuntimeEventScope::Tool
        || plan.status.as_deref() != Some("planned")
        || plan.stream_id != format!("session:{}", packet.session_id())
    {
        return Ok(false);
    }
    let tasks = plan.payload["tasks"].as_array()
        .ok_or("delegated tool plan lacks its original tasks")?;
    for task in tasks {
        if task["tool_name"].as_str().is_some_and(goal_metadata_tool)
            || task["effect"]["effect_kind"] == "read"
        {
            continue;
        }
        let callbacks = events.iter().filter(|terminal| {
            terminal.kind == "tool.invocation.completed"
                && terminal.payload["governed_plan_id"] == plan.payload["plan_id"]
                && terminal.payload["tool_call_id"] == task["tool_call_id"]
        }).collect::<Vec<_>>();
        if callbacks.len() != 1 { return Ok(false); }
        let terminal = callbacks[0];
        let Some(binding) = terminal.activity_binding() else { return Ok(false); };
        let step: crate::step_snapshot::StepCallBinding =
            serde_json::from_value(task["step_binding"].clone())
                .map_err(|_| "delegated callback plan lacks its original Step")?;
        if terminal.actor.as_deref() != Some("conversation_runtime")
            || terminal.schema_version != 1
            || terminal.scope != crate::RuntimeEventScope::Tool
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
            || step.snapshot.identity.execution_id.as_deref() != Some(packet.run_id())
            || step.snapshot.identity.attempt.is_some_and(|attempt| attempt != packet.attempt)
            || step.snapshot.identity.actor_ref.as_deref()
                != Some(packet.assignment.execution_identity.principal_id())
            || !super::binding_is_owned_by_root(&binding, Some(packet.graph_id()))
        {
            return Ok(false);
        }
        super::results::original_tool_primary(store, terminal, task, sources)?;
    }
    Ok(true)
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
) -> Result<RootReviewPolicySnapshot, String> {
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
    use super::*;
    use harness_contract::{policy::*, tool::*};

    /// The review cannot be required to settle the control action it is.
    ///
    /// A reviewer performs the review while the proof is assembled, so that
    /// call's intent is still in flight. Excluding it must be narrow: only
    /// that reviewer's own review call, never another agent's in-flight
    /// write and never another write by the same reviewer.
    #[test]
    fn r1_current_review_control_call_is_excluded_and_nothing_else_is() {
        let reviewer = "agent-run:run:agentic-graph:881f5104981de6ca8fb83addee9d1384e73095156d9cdbf6d53b3f9bfa6e19aa";
        let intent = |stream: &str, tool: &str| crate::DurableRuntimeEvent {
            event_id: "event".into(),
            stream_id: stream.into(),
            sequence: 2,
            scope: crate::RuntimeEventScope::ExecutionNode,
            kind: "execution.agent_tool.intent".into(),
            status: Some("inflight".into()),
            actor: Some("governed_tool".into()),
            refs: Vec::new(),
            payload: serde_json::json!({"tool_name": tool, "sequence": 1}),
            created_at_ms: 0,
            commit_cursor: 0,
            transaction_id: "tx".into(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        };
        let own = "execution-agent-receipts:agentic-graph:881f5104981de6ca8fb83addee9d1384e73095156d9cdbf6d53b3f9bfa6e19aa:agentic-graph:881f5104981de6ca8fb83addee9d1384e73095156d9cdbf6d53b3f9bfa6e19aa:agent:1";
        let foreign = "execution-agent-receipts:agentic-graph:0000000000000000000000000000000000000000000000000000000000000000:agentic-graph:0000000000000000000000000000000000000000000000000000000000000000:agent:1";

        // The reviewer's own review call is the call being reviewed.
        assert!(is_current_review_control_call(reviewer, &intent(own, "task_review")));
        assert!(is_current_review_control_call(
            reviewer,
            &intent(own, "objective_review")
        ));
        // Another write by the same reviewer is not the review.
        assert!(!is_current_review_control_call(reviewer, &intent(own, "edit")));
        // Another agent's review call is not this review.
        assert!(!is_current_review_control_call(
            reviewer,
            &intent(foreign, "task_review")
        ));
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
                    scope: crate::RuntimeEventScope::Session,
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
        let source = "session:empty-projection";
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
                scope: crate::RuntimeEventScope::Session,
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
                review_covers_reference(&program, &[reviewed.into()], required),
                expected,
                "reviewed={reviewed}, required={required}"
            );
        }
        program.artifacts.remove("artifact:old");
        assert!(review_covers_reference(
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
            acquisition_cursor:10, consumer_cursor:20, consumer_call_key:"consumer".into(), source_sha256:"original-preimage".into(),
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
                scope: crate::RuntimeEventScope::ExecutionNode,
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
        assert_eq!(input.dependency_identity(&program, &content, &[]).unwrap(), None);
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
            assert!(input.dependency_identity(&program, &other, &[]).is_err(), "{field}");
        }
        input.reference = "artifact:foreign".into();
        assert_eq!(input.dependency_identity(&program, &content, &[]).unwrap(), Some("artifact:foreign".into()));
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

    async fn real_selected_material_chain_647(mode: u8) {
        use crate::agentic::execution::reviewed_goal_fixture::{
            with_native_task_review_setup, NativeTaskReviewCase,
        };
        use harness_contract::agent_action::{AgentAction, AgentActionEnvelope, AgentActionStatus};
        let material = std::sync::Arc::new(std::sync::Mutex::new(None));
        let retained = std::sync::Arc::clone(&material);
        with_native_task_review_setup(
            NativeTaskReviewCase::default(),
            move |services, _, worker, input| {
                Box::pin(async move {
                    let content = services.artifact_store().write_bytes(
                        harness_contract::context::ArtifactWriteDescriptor {
                            media_type: "text/plain".into(),
                            visibility_scope: format!("session:{}", worker.actor.session_id),
                            expected_bytes: None, original_name: None,
                        }, b"647 actual distinct derived material, not a raw summary alias",
                    ).await.unwrap();
                    let publication = services.submit_agent_action(&AgentActionEnvelope {
                        action_id: "647-derived-material".into(), actor: worker.actor.clone(),
                        expected_revision: None,
                        action: AgentAction::ArtifactCommit(serde_json::from_value(serde_json::json!({
                            "content_ref":content.selector,"kind":"note",
                            "title":"An opaque independent material identity","relates_to":[]
                        })).unwrap()),
                    }).await.unwrap();
                    assert_eq!(publication.status, AgentActionStatus::Applied, "{publication:?}");
                    if mode != 2 {
                        acquire_original_native_body(&services, &worker, "evidence_retrieve",
                            "647-producer-reads-derived-material",
                            serde_json::json!({"evidence_ref":content.selector})).await;
                    }
                    *retained.lock().unwrap() = Some((content, publication.changed_refs[0].clone()));
                    input
                })
            },
            move |services, envelope, donor| {
                Box::pin(async move {
                    let (content, derived_ref) = material.lock().unwrap().take().unwrap();
                    let program = services.agent_action_service()
                        .project(&envelope.actor.program_id).unwrap();
                    let history = services.session_history_reader();
                    let readers = super::super::ResultContentReaders {
                        artifacts: Some(services.artifact_store().as_ref()),
                        history: history.as_deref(), process_inputs: None,
                    };
                    let goal = services.goal_store().get(&format!("goal:{}",
                        envelope.actor.root_execution_id.as_ref().unwrap())).unwrap().unwrap();
                    let mut recovered = Vec::new();
                    for read in &donor.verification.reads {
                        let selected = harness_contract::context::ArtifactRef::durable(
                            &read.content_ref, &read.sha256, read.bytes,
                            "application/octet-stream", format!("session:{}", program.session_id),
                        );
                        recovered.push(super::super::results::resolve_result_metadata(
                            services.event_store(), &program, &read.result_ref,
                            &selected, None, readers).unwrap());
                    }
                    assert!(!recovered.is_empty());
                    assert_eq!(recovered.iter().any(|result| result.primary_inputs.iter().any(|input| input.content == content)),
                        mode != 2, "only the selected producer's real acquisition may close D");
                    let (_, actual) = real_review_actual_inputs(&services, &program, &donor.verification);
                    assert!(actual.required_scopes().iter().any(|target|
                        target.path.workspace_relative_path == "review-effect.txt"));
                    assert!(actual.canonical_facts.iter().any(|fact|
                        fact["grade"] == "acquired"
                            && fact["acquisition"]["body"]["extent"]["kind"] == "file_lines"
                            && fact["scope"]["scope_purposes"].as_array().is_some_and(|purposes|
                                purposes.iter().any(|purpose| purpose["purpose"] == "input_read"))));
                    proof_input_snapshot(services.event_store(), &program, Some(&goal),
                        &donor.verification, readers).unwrap();
                    let selected = &donor.verification.reads[0];
                    let original = vec![selected.result_ref.clone(), selected.content_ref.clone(),
                        derived_ref.clone(), content.selector.clone()];
                    let aliases = super::super::canonical_review_result_refs(&program, &original).unwrap();
                    assert_eq!(aliases.len(), 2);
                    assert!(aliases.contains(&derived_ref));
                    assert_ne!(selected.content_ref, content.selector);
                    assert_eq!(original[0], selected.result_ref);
                    assert_eq!(original[2], derived_ref);
                    if mode == 1 {
                        let mut omitted = donor.verification.clone();
                        omitted.input_set.as_mut().unwrap().required_scopes.clear();
                        assert!(proof_input_snapshot(services.event_store(), &program,
                            Some(&goal), &omitted, readers).is_err());
                        let primary = actual.material_events.iter().find(|material|
                            verify_material_event(services.event_store(), material,
                                &mut std::collections::BTreeMap::new()).is_ok_and(|event|
                                    event.kind == "execution.read.receipt"
                                        && event.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
                                            ["body"]["extent"]["kind"] == "file_lines")).unwrap();
                        let mut omitted = donor.verification.clone();
                        omitted.input_set.as_mut().unwrap().material_events.retain(|material|
                            material.event_id != primary.event_id);
                        assert!(proof_input_snapshot(services.event_store(), &program,
                            Some(&goal), &omitted, readers).is_err());
                        for result in &recovered {
                            let mut cuts = donor.verification.input_set.as_ref().unwrap().source_revisions.clone();
                            cuts.remove(&format!("session:{}", program.session_id));
                            assert!(primary_producer_immutable_inputs(services.event_store(), &program,
                                &result.producers, readers, Some(&cuts)).is_err());
                        }
                        let mut changed = program.clone();
                        changed.artifacts.get_mut(&derived_ref).unwrap().content_ref =
                            selected.content_ref.clone();
                        assert!(proof_input_snapshot(services.event_store(), &changed,
                            Some(&goal), &donor.verification, readers).is_err());
                    }
                })
            },
        ).await;
    }

    #[tokio::test]
    async fn r1_647_selected_real_producer_material_reaches_protected_file_input() {
        real_selected_material_chain_647(0).await;
    }

    #[tokio::test]
    async fn r1_647_real_primary_chain_rejects_omitted_acquisition_cut_scope_and_retargeting() {
        real_selected_material_chain_647(1).await;
    }

    #[tokio::test]
    async fn r1_647_same_producer_unread_material_is_not_a_primary_alias() {
        real_selected_material_chain_647(2).await;
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

    #[tokio::test]
    async fn r1_actual_collector_recovers_omitted_inputs_and_rejects_foreign_unsealed_identity_through_final_proof(
    ) {
        use crate::agentic::execution::reviewed_goal_fixture::{
            with_native_task_review_setup, NativeTaskReviewCase,
        };
        with_native_task_review_setup(
            NativeTaskReviewCase {
                complete_goal: true,
                ..Default::default()
            },
            |services, _, worker, input| {
                Box::pin(async move {
                    acquire_original_native_body(
                        &services,
                        &worker,
                        "tool_search",
                        "actual-input-catalog-body",
                        serde_json::json!({"query":"read_file"}),
                    )
                    .await;
                    let artifact = services
                        .artifact_store()
                        .write_bytes(
                            harness_contract::context::ArtifactWriteDescriptor {
                                media_type: "text/plain".into(),
                                visibility_scope: format!("session:{}", worker.actor.session_id),
                                expected_bytes: None,
                                original_name: None,
                            },
                            b"real immutable producer input",
                        )
                        .await
                        .unwrap();
                    acquire_original_native_body(
                        &services,
                        &worker,
                        "evidence_retrieve",
                        "actual-input-immutable-body",
                        serde_json::json!({"evidence_ref":artifact.selector}),
                    )
                    .await;
                    input
                })
            },
            |services, envelope, donor| {
                Box::pin(async move {
                    let program = services
                        .agent_action_service()
                        .project(&envelope.actor.program_id)
                        .unwrap();
                    let goal = services
                        .goal_store()
                        .get(&format!(
                            "goal:{}",
                            envelope.actor.root_execution_id.as_ref().unwrap()
                        ))
                        .unwrap()
                        .unwrap();
                    let history = services.session_history_reader();
                    let readers = super::super::ResultContentReaders {
                        artifacts: Some(services.artifact_store().as_ref()),
                        history: history.as_deref(),
                        process_inputs: None,
                    };
                    let (boundaries, actual) =
                        real_review_actual_inputs(&services, &program, &donor.verification);
                    assert!(!actual.file_dependencies.is_empty());
                    assert!(actual
                        .canonical_facts
                        .iter()
                        .any(|fact| fact["grade"] == "acquired_immutable_receipt"));
                    assert!(actual
                        .canonical_facts
                        .iter()
                        .any(|fact| fact["grade"] == "acquired"
                            && fact["acquisition"]["body"]["extent"]["kind"] == "artifact_chunks"
                            && fact["scope"]["complete"] == false
                            && fact["scope"]["targets"]
                                .as_array()
                                .is_some_and(Vec::is_empty)));
                    proof_input_snapshot(
                        services.event_store(),
                        &program,
                        Some(&goal),
                        &donor.verification,
                        readers,
                    )
                    .unwrap();
                    let session_events = services
                        .event_store()
                        .list_stream(&format!("session:{}", program.session_id))
                        .unwrap();
                    let publication = session_events
                        .iter()
                        .find(|event| {
                            event.kind == "tool.invocation.completed"
                                && event.payload["tool_call_id"] == "native-publication-small"
                        })
                        .unwrap();
                    let task =
                        super::super::results::result_tool_plan(&session_events, publication)
                            .unwrap()
                            .unwrap();
                    let primary = super::super::results::original_tool_primary(
                        services.event_store(),
                        publication,
                        task,
                        &mut Default::default(),
                    )
                    .unwrap();
                    let body: serde_json::Value = serde_json::from_str(
                        primary.payload["outcome"]["output"].as_str().unwrap(),
                    )
                    .unwrap();
                    let source = body["content_ref"].as_str().unwrap();
                    let content = services.artifact_store().resolve(source).unwrap();
                    let early = super::super::results::resolve_result_metadata(
                        services.event_store(),
                        &program,
                        source,
                        &content,
                        None,
                        readers,
                    )
                    .unwrap();
                    let early_inputs = actual_input_snapshot(
                        services.event_store(),
                        &program,
                        &early.input_boundaries,
                        readers,
                        None,
                    )
                    .unwrap();
                    let catalog = actual
                        .material_events
                        .iter()
                        .find(|material| {
                            let event = verify_material_event(
                                services.event_store(),
                                material,
                                &mut Default::default(),
                            )
                            .unwrap();
                            event.kind == "execution.read.receipt"
                                && event.payload["tool_name"] == "tool_search"
                        })
                        .unwrap();
                    assert!(!early_inputs
                        .canonical_facts
                        .iter()
                        .any(|fact| fact["source"] == catalog.event_id));
                    assert!(actual
                        .canonical_facts
                        .iter()
                        .any(|fact| fact["source"] == catalog.event_id));
                    let primary_ids = actual
                        .material_events
                        .iter()
                        .filter(|material| {
                            verify_material_event(
                                services.event_store(),
                                material,
                                &mut Default::default(),
                            )
                            .unwrap()
                            .kind
                                == "execution.read.receipt"
                        })
                        .map(|material| material.event_id.clone())
                        .collect::<BTreeSet<_>>();
                    let index_ids = actual
                        .material_events
                        .iter()
                        .filter(|material| {
                            let event = verify_material_event(
                                services.event_store(),
                                material,
                                &mut Default::default(),
                            )
                            .unwrap();
                            event.kind == "execution.agent_tool.receipt"
                                && event.payload["effect_kind"] == "read"
                        })
                        .map(|material| material.event_id.clone())
                        .collect::<BTreeSet<_>>();
                    assert!(!primary_ids.is_empty() && !index_ids.is_empty());
                    for mode in ["primary", "index", "both", "both-empty-scopes"] {
                        let mut proof = donor.verification.clone();
                        let inputs = proof.input_set.as_mut().unwrap();
                        inputs.material_events.retain(|material| {
                            !(mode != "index" && primary_ids.contains(&material.event_id)
                                || mode != "primary" && index_ids.contains(&material.event_id))
                        });
                        if mode == "both-empty-scopes" {
                            inputs.required_scopes.clear();
                        }
                        assert!(
                            proof_input_snapshot(
                                services.event_store(),
                                &program,
                                Some(&goal),
                                &proof,
                                readers
                            )
                            .is_err(),
                            "{mode}"
                        );
                        let mut rejected = envelope.clone();
                        rejected.action_id = format!("actual-input-omission:{mode}");
                        let cursor = services.event_store().durable_commit_cursor().unwrap();
                        let before = services.goal_store().get(&goal.id).unwrap().unwrap();
                        let applied = services
                            .agent_action_service()
                            .apply_verified_review(&rejected, &proof)
                            .unwrap();
                        assert_eq!(
                            applied.status,
                            harness_contract::agent_action::AgentActionStatus::Rejected,
                            "{mode}"
                        );
                        assert_eq!(
                            services.event_store().durable_commit_cursor().unwrap(),
                            cursor
                        );
                        assert_eq!(
                            services.goal_store().get(&goal.id).unwrap().unwrap(),
                            before
                        );
                    }
                    for fault in [
                        "session",
                        "run",
                        "step",
                        "call",
                        "input",
                        "compiled",
                        "artifact-body",
                        "artifact-hash",
                        "artifact-chunk",
                        "artifact-source",
                        "artifact-producer",
                        "artifact-attempt",
                    ] {
                        ACTUAL_READ_SOURCE_FAULT
                            .scope(fault, async {
                                assert!(
                                    actual_input_snapshot(
                                        services.event_store(),
                                        &program,
                                        &boundaries,
                                        readers,
                                        None
                                    )
                                    .is_err(),
                                    "{fault}"
                                );
                                assert!(
                                    proof_input_snapshot(
                                        services.event_store(),
                                        &program,
                                        Some(&goal),
                                        &donor.verification,
                                        readers
                                    )
                                    .is_err(),
                                    "{fault}"
                                );
                            })
                            .await;
                    }
                    for boundary in &boundaries {
                        let mut forged = boundary.clone();
                        forged.call_key.push_str(":other-consumer");
                        assert!(actual_input_snapshot(
                            services.event_store(),
                            &program,
                            &[forged],
                            readers,
                            None
                        )
                        .is_err());
                        let mut forged = boundary.clone();
                        forged.cursor += 1;
                        assert!(actual_input_snapshot(
                            services.event_store(),
                            &program,
                            &[forged],
                            readers,
                            None
                        )
                        .is_err());
                    }
                    let accepted = services
                        .agent_action_service()
                        .apply_verified_review(&envelope, &donor.verification)
                        .unwrap();
                    assert_eq!(
                        accepted.status,
                        harness_contract::agent_action::AgentActionStatus::Applied,
                        "{accepted:?}"
                    );
                })
            },
        )
        .await;
    }

    async fn original_root_same_bytes_write(
        services: &std::sync::Arc<crate::RuntimeServices>,
        actor: &harness_contract::agent_action::AgentActorBinding,
        call: &str,
    ) -> u64 {
        let root = actor.root_execution_id.as_deref().unwrap();
        let node = format!("{root}:model");
        let graph = crate::agentic::execution::reviewed_goal_fixture::bind_review_fixture_root(
            services, services.commit_service(),
            services.graph_state_store().load(root).unwrap(), &node,
        );
        let owner = services
            .event_store()
            .latest_for_stream(&format!("{root}:node:{node}"))
            .unwrap()
            .unwrap()
            .activity_binding()
            .unwrap();
        let attempt = graph.recovery_cursor.node_attempts[&node];
        let host = services.tool_execution_host().unwrap();
        let lease = host.pin_step_catalog().unwrap();
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == "edit")
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        let identity = serde_json::from_value(serde_json::json!({
            "step_id":format!("original-root-input-test:{}",uuid::Uuid::new_v4()),
            "session_id":owner.session_id,"turn_id":owner.turn_id,"execution_id":root,
            "actor_ref":actor.actor_id,"attempt":attempt,"build_ref":"original-c-review-fixture",
        }))
        .unwrap();
        let step = std::sync::Arc::new(
            crate::step_snapshot::BoundStep::bind(
                identity,
                lease.clone(),
                &definitions,
                &serde_json::json!({}),
                &serde_json::json!({}),
                Default::default(),
                9_007_199_254_740_991,
            )
            .unwrap(),
        );
        let binding = crate::step_snapshot::StepCallBinding::provider(step, call);
        let content =
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt")).unwrap();
        let input =
            serde_json::json!({"action":"write","path":"review-effect.txt","content":content});
        let invocation = binding
            .prepare_invocation(Some(lease), "edit", &input)
            .unwrap();
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: call.into(),
                tool_name: "edit".into(),
                input: input.to_string(),
                depends_on: vec![],
            },
        );
        request.session_id = Some(owner.session_id.clone());
        request.governed_plan_id = root.into();
        request.governed_plan_revision = 1;
        request.observation_wave_sequence = 50_000;
        request.policy_revision = 1;
        request.idempotency_key = binding.effect_idempotency_key();
        request.compiled_invocation = Some(invocation.clone());
        request.step_binding = Some(binding);
        let policy = crate::PermissionPolicy::new(PermissionMode::WorkspaceWrite);
        request.sandbox_posture = policy.execution_policy_control().snapshot().sandbox_posture;
        let assessed = crate::AuthorizationNegotiator::new().assess_effective(
            &policy,
            &crate::AuthorizationRequest {
                principal_id: actor.actor_id.clone(),
                capability: "edit".into(),
                input: request.input.clone(),
                idempotency_key: call.into(),
                effect: invocation.effect.clone(),
                parent_ceiling: PermissionMode::WorkspaceWrite,
                parent_lease_id: None,
                policy_revision: request.policy_revision,
                recovery_scope: root.into(),
                context: crate::PermissionContext::default(),
                safe_alternatives: vec![],
            },
        );
        request.authorization = Some(
            crate::ToolPolicy
                .authorize(
                    &assessed.effective,
                    &assessed.assessment,
                    call,
                    assessed.assessment.lease.clone().unwrap(),
                    30,
                )
                .unwrap()
                .authorization,
        );
        let (locks, _) = crate::bound_tool_batch::bound_effect_lock_requests(
            services.path_identity_resolver(),
            &invocation.effect,
        )
        .unwrap();
        let _locks = services.scope_locks().acquire(locks, None).await.unwrap();
        let scope = crate::bound_tool_batch::admitted_effect_scope(
            &request,
            &invocation.effect,
            host.pin_step_catalog(),
            services.path_identity_resolver(),
        )
        .unwrap();
        let root_context = crate::CowdExecutionContext {
            execution_id: root.into(),
            session_id: owner.session_id.clone(),
            turn_id: owner.turn_id.clone(),
        };
        services
            .commit_service()
            .begin_root_tool_effect(
                &request,
                &invocation.effect,
                &root_context,
                &scope,
                &owner,
                attempt,
            )
            .unwrap();
        let outcome = host.execute_runtime_tool(&request).await;
        assert_eq!(
            outcome.status,
            crate::RuntimeToolExecutionStatus::Executed,
            "{outcome:?}"
        );
        services
            .commit_service()
            .commit_tool_effect(&request, &invocation.effect, &outcome)
            .unwrap();
        let receipt = services
            .event_store()
            .event_by_idempotency_key(
                &format!("execution-effect:{}", request.idempotency_key),
                &format!("{}:receipt", request.idempotency_key),
            )
            .unwrap()
            .unwrap();
        super::super::results::authenticate_root_receipt_owner(
            services.event_store(),
            &receipt,
            &mut Default::default(),
        )
        .unwrap();
        receipt.commit_cursor
    }

    #[tokio::test]
    async fn r1_actual_collector_separates_preuse_consumer_from_real_reacquisition_and_reaches_goal(
    ) {
        use crate::agentic::execution::reviewed_goal_fixture::{
            with_native_task_review_setup, NativeTaskReviewCase,
        };
        use harness_contract::agent_action::{
            AgentAction, AgentActionEnvelope, ArtifactCommitInput,
        };
        let facts = std::sync::Arc::new(std::sync::Mutex::new(None));
        let setup_facts = facts.clone();
        with_native_task_review_setup(
            NativeTaskReviewCase {
                complete_goal: true,
                commit_report: true,
                ..Default::default()
            },
            move |services, root, worker, mut input| {
                Box::pin(async move {
                    let program = services
                        .agent_action_service()
                        .project(&worker.actor.program_id)
                        .unwrap();
                    let original = program
                        .artifacts
                        .values()
                        .find(|artifact| {
                            artifact.claim_execution_id == worker.actor.execution_id
                                && artifact.relates_to.contains(&input.task_ref)
                        })
                        .unwrap()
                        .content_ref
                        .clone();
                    let write = original_root_same_bytes_write(
                        &services,
                        &root.actor,
                        "actual-preuse-write",
                    )
                    .await;
                    let publish = |id: &str| AgentActionEnvelope {
                        action_id: id.into(),
                        actor: worker.actor.clone(),
                        expected_revision: None,
                        action: AgentAction::ArtifactCommit(ArtifactCommitInput {
                            content_ref: original.clone(),
                            kind: "report".into(),
                            title: id.into(),
                            relates_to: vec![input.task_ref.clone()],
                        }),
                    };
                    let stale = services
                        .submit_agent_action(&publish("actual-preuse-stale-consumer"))
                        .await
                        .unwrap();
                    assert_eq!(
                        stale.status,
                        harness_contract::agent_action::AgentActionStatus::Applied
                    );
                    acquire_original_native_body(
                        &services,
                        &worker,
                        "read_file",
                        "actual-reacquired-before-use",
                        serde_json::json!({"path":"review-effect.txt","complete":true}),
                    )
                    .await;
                    let fresh = services
                        .submit_agent_action(&publish("actual-preuse-fresh-consumer"))
                        .await
                        .unwrap();
                    assert_eq!(
                        fresh.status,
                        harness_contract::agent_action::AgentActionStatus::Applied
                    );
                    input.artifact_refs = fresh.changed_refs.clone();
                    input.deliverable = None;
                    // The stale and fresh consumers commit the same original
                    // content, so a content-level evidence ref is genuinely
                    // ambiguous across their artifacts (the product refuses it,
                    // correctly). Cite the fresh consumer's exact artifact.
                    input.evidence_refs = fresh.changed_refs.clone();
                    *setup_facts.lock().unwrap() = Some((
                        stale.changed_refs[0].clone(),
                        fresh.changed_refs[0].clone(),
                        write,
                    ));
                    input
                })
            },
            move |services, envelope, donor| {
                Box::pin(async move {
                    let (stale, fresh, write) = facts.lock().unwrap().clone().unwrap();
                    let program = services
                        .agent_action_service()
                        .project(&envelope.actor.program_id)
                        .unwrap();
                    let history = services.session_history_reader();
                    let readers = super::super::ResultContentReaders {
                        artifacts: Some(services.artifact_store().as_ref()),
                        history: history.as_deref(),
                        process_inputs: None,
                    };
                    for (reference, after) in [(&stale, false), (&fresh, true)] {
                        let content = services
                            .artifact_store()
                            .resolve(&program.artifacts[reference].content_ref)
                            .unwrap();
                        let metadata = super::super::results::resolve_result_metadata(
                            services.event_store(),
                            &program,
                            reference,
                            &content,
                            None,
                            readers,
                        )
                        .unwrap();
                        let actual = actual_input_snapshot(
                            services.event_store(),
                            &program,
                            &metadata.input_boundaries,
                            readers,
                            None,
                        )
                        .unwrap();
                        assert!(actual
                            .file_dependencies
                            .iter()
                            .any(|dependency| dependency.consumer_cursor > write
                                && (dependency.acquisition_cursor > write) == after));
                    }
                    let error = services
                        .independent_result_review(&envelope.actor, &program, &[stale], None)
                        .await
                        .err()
                        .expect(
                            "the real earlier consumer cannot use a later producer reacquisition",
                        );
                    assert!(
                        error.contains("cites results outside Task")
                            || error.contains("proof_actual_input_stale_before_use"),
                        "{error}"
                    );
                    let goal = services
                        .goal_store()
                        .get(&format!(
                            "goal:{}",
                            envelope.actor.root_execution_id.as_ref().unwrap()
                        ))
                        .unwrap()
                        .unwrap();
                    proof_input_snapshot(
                        services.event_store(),
                        &program,
                        Some(&goal),
                        &donor.verification,
                        readers,
                    )
                    .unwrap();
                    let applied = services
                        .agent_action_service()
                        .apply_verified_review(&envelope, &donor.verification)
                        .unwrap();
                    assert_eq!(
                        applied.status,
                        harness_contract::agent_action::AgentActionStatus::Applied,
                        "{applied:?}"
                    );
                })
            },
        )
        .await;
    }

    #[tokio::test]
    async fn r1_actual_collector_rejects_real_write_after_independent_read_without_faking_postimage(
    ) {
        use crate::agentic::execution::reviewed_goal_fixture::{
            with_native_task_review_async, NativeTaskReviewCase,
        };
        with_native_task_review_async(NativeTaskReviewCase::default(),|services,envelope,_donor| Box::pin(async move {
            let before = services.goal_store().get(&format!("goal:{}",envelope.actor.root_execution_id.as_ref().unwrap())).unwrap().unwrap();
            original_root_same_bytes_write(&services,&envelope.actor,"actual-after-reviewer-read").await;
            let program = services.agent_action_service().project(&envelope.actor.program_id).unwrap();
            let references = match &envelope.action {
                harness_contract::agent_action::AgentAction::TaskReview(input) => program.tasks[&input.task_ref].artifact_refs.clone(),
                _ => unreachable!(),
            };
            let error = services.independent_result_review(&envelope.actor,&program,&references,None).await.err()
                .expect("a fresh collector must reject old independent File observations after a real write");
            assert!(error.contains("review_requires_effect_observation"),"{error}");
            assert!(services.submit_agent_action(&envelope).await.is_err());
            assert_eq!(services.goal_store().get(&before.id).unwrap().unwrap(),before);
            assert!(program.tasks.values().any(|task|task.status == crate::AgenticTaskStatus::Submitted));
        })).await;
    }

    #[tokio::test]
    async fn own877_real_task_review_rejects_pending_and_foreign_control_callback() {
        use crate::agentic::execution::reviewed_goal_fixture::{
            with_native_task_review_async, NativeTaskReviewCase,
        };
        for foreign in [false, true] {
            with_native_task_review_async(NativeTaskReviewCase::default(),
                move |services, envelope, _donor| Box::pin(async move {
                    let program = services.agent_action_service().project(&envelope.actor.program_id).unwrap();
                    let task_ref = match &envelope.action {
                        harness_contract::agent_action::AgentAction::TaskReview(input) => &input.task_ref,
                        _ => unreachable!(),
                    };
                    let before = services.goal_store().get(&format!("goal:{}",
                        envelope.actor.root_execution_id.as_deref().unwrap())).unwrap().unwrap();
                    let store = services.event_store();
                    let events = store.list_stream(&format!("session:{}", program.session_id)).unwrap();
                    let terminal = events.iter().find(|event|
                        event.kind == "tool.invocation.completed"
                            && event.payload["tool_call_id"] == "native-submit").unwrap();
                    let original = events.iter().find(|event|
                        event.kind == "tool.execution_plan.created"
                            && event.payload["plan_id"] == terminal.payload["governed_plan_id"]).unwrap();
                    let mut payload = original.payload.clone();
                    let plan_id = format!("own877-unsettled-control:{}", uuid::Uuid::new_v4());
                    payload["plan_id"] = serde_json::json!(plan_id);
                    store.append(crate::RuntimeEventInput {
                        stream_id: original.stream_id.clone(), scope: original.scope,
                        kind: original.kind.clone(), status: original.status.clone(),
                        actor: original.actor.clone(), refs: original.refs.clone(), payload,
                    }).unwrap();
                    if foreign {
                        let mut payload = terminal.payload.clone();
                        payload["governed_plan_id"] = serde_json::json!(plan_id);
                        store.append(crate::RuntimeEventInput {
                            stream_id: terminal.stream_id.clone(), scope: terminal.scope,
                            kind: terminal.kind.clone(), status: terminal.status.clone(),
                            actor: Some("foreign-runtime".into()), refs: terminal.refs.clone(), payload,
                        }).unwrap();
                    }
                    let current = services.agent_action_service().project(&program.program_id).unwrap();
                    let error = services.independent_result_review(&envelope.actor, &current,
                        &current.tasks[task_ref].artifact_refs, None).await.err()
                        .expect("an original control descriptor must not settle an unauthenticated or missing callback");
                    assert!(error.contains("effect_review_pending"), "foreign={foreign}: {error}");
                    assert!(services.submit_agent_action(&envelope).await.is_err());
                    assert_eq!(services.goal_store().get(&before.id).unwrap().unwrap(), before);
                    assert_eq!(services.agent_action_service().project(&program.program_id).unwrap()
                        .tasks[task_ref].status, crate::AgenticTaskStatus::Submitted);
                })).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_process_explicit_body_selection_preserves_all_versions_through_original_review_and_goal_cas(
    ) {
        crate::agentic::execution::reviewed_goal_fixture::exercise_bounded_process_reviewed_goal_283().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_process_changed_full_input_after_review_is_rejected_at_actual_goal_cas() {
        crate::agentic::execution::reviewed_goal_fixture::exercise_bounded_process_input_changed_before_final_290().await;
    }
}

#[cfg(test)]
#[path = "receipt_index_791_tests.rs"]
mod receipt_index_791_tests;


#[cfg(test)]
mod own872_plan_owner_tests {
    use super::*;

    #[tokio::test]
    async fn own872_real_native_callback_missing_foreign_actor_and_owner_never_settle() {
        let (_workspace, services, owner) =
            crate::agentic::execution::reviewed_goal_fixture::native_raw_owner_fixture_for_test().await;
        let binding = owner.activity_binding().unwrap();
        let graph = services.graph_state_store().load(&binding.root_execution_id).unwrap();
        let packet = graph.nodes.iter().find_map(|node| {
            if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask { return None; }
            serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(&node.payload_ref).ok()
        }).unwrap();
        let store = services.event_store();
        let events = store.list_stream(&owner.stream_id).unwrap();
        let mut checked = BTreeSet::new();
        for plan in events.iter().filter(|event| event.kind == "tool.execution_plan.created") {
            let Some(tasks) = plan.payload["tasks"].as_array() else { continue; };
            for task in tasks.iter().filter(|task|
                matches!(task["tool_name"].as_str(), Some("task_claim" | "task_submit")))
            {
                let packets = std::collections::BTreeMap::from([(packet.graph_id().to_string(), packet.clone())]);
                let roots = BTreeSet::new();
                if !events.iter().any(|event| event.kind == "tool.invocation.completed"
                    && event.payload["governed_plan_id"] == plan.payload["plan_id"]
                    && event.activity_binding().is_some_and(|binding| binding.root_execution_id == packet.graph_id()))
                { continue; }
                let original = delegated_plan_callback_owner(plan, &events, &roots, &packets).unwrap();
                assert!(authenticated_delegated_plan_settled(store, plan, &events, original, &mut Default::default()).unwrap());
                let original_task = plan.payload["tasks"].as_array().unwrap().iter()
                    .find(|original| original["tool_call_id"] == task["tool_call_id"]).unwrap();
                let step: crate::step_snapshot::StepCallBinding =
                    serde_json::from_value(original_task["step_binding"].clone()).unwrap();
                let key = step.effect_idempotency_key();
                let index_source = format!("execution-agent-receipts:{}:{}:{}",
                    packet.graph_id(), packet.node_id(), packet.attempt);
                let intent = store.event_by_idempotency_key(
                    &index_source, &format!("agent-tool-intent:{key}"),
                ).unwrap().unwrap();
                let receipt = store.event_by_idempotency_key(
                    &index_source, &format!("agent-tool-receipt:{key}"),
                ).unwrap().unwrap();
                admitted_intent_scope(store, &intent, &mut Default::default()).unwrap();
                let original = authenticate_settled_effect(
                    store, &intent, &receipt, &mut Default::default(),
                ).unwrap();
                assert!(super::super::results::runtime_control_descriptor(&original));
                assert!(original.scopes.iter().all(|scope|
                    scope.resource == harness_contract::policy::PermissionResource::Tool
                        && scope.operation == harness_contract::policy::PermissionOperation::Control));
                let mut foreign_receipt = receipt.clone();
                foreign_receipt.payload["outcome"]["tool_name"] = serde_json::json!("edit");
                assert!(authenticate_settled_effect(
                    store, &intent, &foreign_receipt, &mut Default::default(),
                ).is_err());
                assert!(task["step_binding"]["snapshot"]["identity"]["attempt"].is_null());
                for fault in ["foreign_attempt", "foreign_provider", "process_source"] {
                    let mut invalid_plan = plan.clone();
                    let task = invalid_plan.payload["tasks"].as_array_mut().unwrap().iter_mut()
                        .find(|changed| changed["tool_call_id"] == task["tool_call_id"]).unwrap();
                    match fault {
                        "foreign_attempt" => task["step_binding"]["snapshot"]["identity"]["attempt"] =
                            serde_json::json!(packet.attempt + 1),
                        "foreign_provider" => task["step_binding"]["origin"]["source"]["attempt_id"] =
                            serde_json::json!("foreign-provider-attempt"),
                        "process_source" => task["step_binding"]["origin"]["source"] = serde_json::json!({
                            "kind":"process","worker_id":packet.run_id(),"sequence":1,
                        }),
                        _ => unreachable!(),
                    }
                    let settled = delegated_plan_callback_owner(&invalid_plan, &events, &roots, &packets)
                        .map(|original| authenticated_delegated_plan_settled(
                            store, &invalid_plan, &events, original, &mut Default::default(),
                        )).unwrap_or(Ok(false));
                    assert!(!matches!(settled, Ok(true)), "{fault}");
                }
                assert!(delegated_plan_callback_owner(plan, &events, &roots, &Default::default()).is_none());
                let position = events.iter().position(|terminal|
                    terminal.kind == "tool.invocation.completed"
                        && terminal.payload["governed_plan_id"] == plan.payload["plan_id"]
                        && terminal.payload["tool_call_id"] == task["tool_call_id"]).unwrap();
                for fault in ["missing", "actor", "owner", "run", "instance", "session", "turn", "schema", "scope", "status"] {
                    let mut invalid = events.clone();
                    match fault {
                        "missing" => { invalid.remove(position); }
                        "actor" => invalid[position].actor = Some("foreign".into()),
                        "schema" => invalid[position].schema_version = 2,
                        "scope" => invalid[position].scope = crate::RuntimeEventScope::Session,
                        "status" => invalid[position].status = Some("running".into()),
                        other => {
                            let field = match other {
                                "owner" => "root_execution_id",
                                "run" => "agent_run_id",
                                "instance" => "agent_instance_id",
                                "session" => "session_id",
                                "turn" => "turn_id",
                                _ => unreachable!(),
                            };
                            invalid[position].payload["_runtime_activity_binding"][field] = serde_json::json!("foreign");
                        }
                    }
                    let settled = delegated_plan_callback_owner(plan, &invalid, &roots, &packets)
                        .map(|original| authenticated_delegated_plan_settled(
                            store, plan, &invalid, original, &mut Default::default(),
                        ).unwrap()).unwrap_or(false);
                    assert!(!settled, "{fault}");
                }
                checked.insert(task["tool_name"].as_str().unwrap().to_string());
            }
        }
        assert_eq!(checked, BTreeSet::from(["task_claim".to_string(), "task_submit".to_string()]));
    }

    #[test]
    fn own872_native_plan_owner_does_not_erase_unknown_or_pending_root_writes() {
        let store = crate::RuntimeEventStore::for_test();
        let mut plan = store.append(crate::RuntimeEventInput {
            stream_id: "session:own872".into(),
            scope: crate::RuntimeEventScope::Tool,
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
