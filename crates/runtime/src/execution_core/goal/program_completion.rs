//! Prepare, but never independently commit, the Goal half of Program closure.
use super::*;
use crate::{AgenticProgramProjection, ExpectedStreamRevision};

pub(crate) struct PreparedProgramConclusion {
    pub goal: GoalContract,
    pub expected_stream_revision: u64,
    pub event: RuntimeTransactionEventInput,
    pub gaps: Vec<String>,
    pub source_frame: AssessmentSourceFrame,
    verified_proofs: Vec<std::sync::Arc<PreparedVerifiedProof>>,
    proof_seal: Option<PreparedProofSeal>,
    pub runtime_trigger: Option<RuntimeObservation>,
    pub rejection_trigger: Option<String>,
}

/// One successful coverage check and its authenticated facts, scoped to this
/// preparation. Neither the snapshot nor the frame can be replaced by a
/// caller after the owner constructs this value.
pub(crate) struct PreparedVerifiedProof {
    condition: String,
    snapshot: crate::agentic::review_evidence::ProofInputSnapshot,
    proof: crate::agentic::review_evidence::VerifiedConclusionProof,
}

impl PreparedVerifiedProof {
    pub(crate) fn condition(&self) -> &str { &self.condition }
    pub(crate) fn snapshot(&self) -> &crate::agentic::review_evidence::ProofInputSnapshot { &self.snapshot }
    pub(crate) fn proof(&self) -> &crate::agentic::review_evidence::VerifiedConclusionProof { &self.proof }
}

struct PreparedProofSeal {
    store: std::sync::Arc<crate::RuntimeEventStore>,
    program: AgenticProgramProjection,
    goal: GoalContract,
    event: RuntimeTransactionEventInput,
    expected_stream_revision: u64,
    gaps: Vec<String>,
    sources: Vec<ExpectedStreamRevision>,
    // Holds the original Process input owner's scope leases and file
    // descriptors through the synchronous conclusion commit.
    _process_inputs: Option<std::sync::Arc<crate::agentic::review_evidence::ProcessInputFrame>>,
}

/// The original, already-authenticated conclusion use and material guards.
/// A weak store prevents a cycle when the original transaction owner retains it.
pub(crate) struct OriginalConclusionProofCustody {
    store: std::sync::Weak<crate::RuntimeEventStore>,
    program_id: String,
    goal: GoalContract,
    verified_proofs: Vec<std::sync::Arc<PreparedVerifiedProof>>,
    frame: Option<std::sync::Arc<crate::agentic::review_evidence::ProcessInputFrame>>,
}

impl OriginalConclusionProofCustody {
    pub(crate) fn matches(&self, store: &std::sync::Arc<crate::RuntimeEventStore>, program_id: &str, goal: &GoalContract) -> bool {
        self.store.upgrade().is_some_and(|owner| std::sync::Arc::ptr_eq(&owner, store))
            && self.program_id == program_id && &self.goal == goal
    }
    pub(crate) fn verified_proofs(&self) -> &[std::sync::Arc<PreparedVerifiedProof>] { &self.verified_proofs }
    pub(crate) fn frame(&self) -> Option<&crate::agentic::review_evidence::ProcessInputFrame> { self.frame.as_deref() }
    pub(crate) fn frame_handle(&self) -> Option<std::sync::Arc<crate::agentic::review_evidence::ProcessInputFrame>> { self.frame.clone() }
    pub(crate) fn current_approval_sources(&self, store: &crate::RuntimeEventStore, session: &str) -> Result<std::collections::BTreeMap<String, u64>, String> {
        let mut sources = std::collections::BTreeMap::new();
        for call in self.verified_proofs.iter().flat_map(|verified| verified.proof().authenticated_historical_calls()) {
            let Some(approval) = call.approval() else { continue; };
            let current = crate::ApprovalQueue::capture_proof_control_from_store(store, session, approval.approval_id())?;
            if &current.request().context != approval.context() { return Err("original approval context changed".into()); }
            let mut grants = current.grants().iter().filter(|grant| grant.grant_id == approval.grant_id() && grant.approval_id == approval.approval_id());
            let grant = grants.next().ok_or("original approval grant missing")?;
            if grants.next().is_some() || grant.revoked_at_ms.is_some() || grant.status == harness_contract::policy::approval::ApprovalGrantStatus::Revoked {
                return Err("original approval revoked or ambiguous".into());
            }
            let source = current.source();
            if sources.insert(source.stream_id.clone(), source.expected_revision).is_some_and(|old| old != source.expected_revision) {
                return Err("original approval owner observations conflict".into());
            }
        }
        Ok(sources)
    }

    /// A Graph material coordinate is immutable, but it must have belonged
    /// to the original checked cut; the terminal Graph CAS is a separate
    /// current-node control, not a replacement for this material identity.
    pub(crate) fn graph_material_in_original_cut(&self, graph: &str, receipt: &crate::CheckedAppendReceipt) -> bool {
        self.verified_proofs.iter().flat_map(|verified| verified.snapshot().dependency_frame().material_events())
            .filter(|event| event.stream_id == graph)
            .all(|event| receipt.validation.sources.iter().any(|source|
                source.stream_id == graph && event.sequence <= source.committed_revision))
    }
    /// Recheck only the exact File dependencies authenticated at A; retain
    /// their original workspace lease and verified descriptors through B.
    pub(crate) fn refresh_original_files(&self) -> Result<(), String> {
        let frame = self.frame.as_deref().ok_or("original material frame unavailable")?;
        for verified in &self.verified_proofs {
            for (target, digest, bytes) in verified.snapshot().dependency_frame().protected_files() {
                frame.retain_original_file(target, digest, bytes)?;
            }
        }
        Ok(())
    }
    /// Check the original already-held guards, without reconstructing proof.
    pub(crate) fn guards_cover(&self) -> bool {
        let Some(frame) = self.frame.as_deref() else { return false; };
        let required = crate::agentic::review_evidence::required_review_conditions(&self.goal);
        let mut used = vec![false; self.verified_proofs.len()];
        for condition in required {
            let Some(review) = self.goal.reviews.iter().rev().find(|review| review.criterion_ref == condition) else { return false; };
            let Some(proof) = review.verification.as_ref() else { return false; };
            let Some((index, verified)) = self.verified_proofs.iter().enumerate().find(|(index, verified)|
                !used[*index] && verified.condition() == condition && verified.proof().matches_proof(proof)) else { return false; };
            used[index] = true;
            for fact in verified.snapshot().dependency_frame().canonical_facts() {
                if fact["grade"] == "bounded_process_history" {
                    let inputs = &fact["execution"]["fact"]["admission"]["inputs"];
                    let Some(inputs) = inputs.as_array() else { return false; };
                    if !inputs.is_empty() {
                        let Some(key) = fact["execution"]["fact"]["admission"]["binding"]["call_key"].as_str() else { return false; };
                        if !frame.retains_process_input_run(key) { return false; }
                    }
                }
            }
            if verified.snapshot().dependency_frame().file_dependencies().any(|(target, _, _, _, digest)|
                !frame.retains_exact_file(target, digest, None)) { return false; }
            if proof.reads.iter().any(|read| {
                if read.content_ref.starts_with("artifact://") {
                    !frame.retains_pinned_read(&read.content_ref, &read.sha256, read.bytes)
                } else {
                    !verified.snapshot().dependency_frame().file_dependencies().any(|(target, _, _, _, digest)|
                        target.path.workspace_relative_path == read.content_ref
                            && digest.trim_start_matches("sha256:") == read.sha256.trim_start_matches("sha256:")
                            && frame.retains_exact_file(target, &read.sha256, Some(read.bytes)))
                }
            }) { return false; }
        }
        used.into_iter().all(|entry| entry)
    }
}

impl PreparedProgramConclusion {
    pub(crate) fn verified_proofs(&self) -> &[std::sync::Arc<PreparedVerifiedProof>] {
        &self.verified_proofs
    }

    /// Reuse the original preparation's guards when a current-control
    /// reader needs them; do not create a second owner frame at commit.
    pub(crate) fn process_input_frame(&self) -> Option<&crate::agentic::review_evidence::ProcessInputFrame> {
        self.proof_seal.as_ref().and_then(|seal| seal._process_inputs.as_deref())
    }

    /// Seal the same owner frame before its original checked transaction.
    pub(crate) fn original_proof_custody(
        &self, store: &std::sync::Arc<crate::RuntimeEventStore>, program: &AgenticProgramProjection,
    ) -> Result<Option<std::sync::Arc<OriginalConclusionProofCustody>>, String> {
        self.verify_prepared_proof_scope(store, program)?;
        let Some(seal) = &self.proof_seal else { return Ok(None); };
        Ok(Some(std::sync::Arc::new(OriginalConclusionProofCustody {
            store: std::sync::Arc::downgrade(store), program_id: program.program_id.clone(),
            goal: seal.goal.clone(), verified_proofs: self.verified_proofs.clone(), frame: seal._process_inputs.clone(),
        })))
    }

    /// Check before commit mutates the source frame. Current controls and
    /// transaction CAS are separate and must still be checked by the owner.
    pub(crate) fn verify_prepared_proof_scope(
        &self, store: &std::sync::Arc<crate::RuntimeEventStore>, program: &AgenticProgramProjection,
    ) -> Result<(), String> {
        let Some(seal) = &self.proof_seal else {
            if self.goal.completion == GoalCompletion::Satisfied || !self.verified_proofs.is_empty() {
                return Err("satisfied conclusion has no prepared proof scope".into());
            }
            return Ok(());
        };
        if !std::sync::Arc::ptr_eq(&seal.store, store) || &seal.program != program
            || seal.goal != self.goal || seal.event != self.event
            || seal.expected_stream_revision != self.expected_stream_revision
            || seal.gaps != self.gaps || self.runtime_trigger.is_some() || self.rejection_trigger.is_some()
            || seal.sources != self.source_frame.sources().cloned().collect::<Vec<_>>() {
            return Err("prepared proof store, target or conclusion changed".into());
        }
        Ok(())
    }
}

const COMPLETION_REOPENED_EVENT_KIND: &str = "agentic.completion_reopened";

const COMPLETION_REJECTED_EVENT_KIND: &str = "agentic.completion_rejected";

fn completion_reopen_bound() -> Result<Option<u64>, String> {
    match std::env::var("COWD_COMPLETION_REOPEN_BOUND") {
        Ok(value) => parse_completion_reopen_bound(&value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!(
            "invalid operator completion resource limit: {error}"
        )),
    }
}

fn parse_completion_reopen_bound(value: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|limit| *limit > 0)
        .ok_or_else(|| {
            "COWD_COMPLETION_REOPEN_BOUND must be an explicit positive resource limit".to_string()
        })
}

fn completion_reopen_count(
    event_store: &crate::RuntimeEventStore,
    program_id: &str,
) -> Result<u64, String> {
    let stream_id = format!("agentic-program:{program_id}");
    let head = event_store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?;
    event_store.count_stream_kinds(
        &stream_id,
        &[
            COMPLETION_REOPENED_EVENT_KIND,
            COMPLETION_REJECTED_EVENT_KIND,
        ],
        head,
    )
}

#[cfg(test)]
#[test]
fn r1_bounded_history_completion_count_is_exact_without_event_hydration() {
    let (store, backend) = crate::content_publication::tests::JournalFault::new_journal();
    let stream = "agentic-program:count";
    let mut expected = 0;
    let events = (0..1000)
        .map(|index| {
            let kind = match index % 7 {
                0 => COMPLETION_REOPENED_EVENT_KIND,
                1 => COMPLETION_REJECTED_EVENT_KIND,
                _ => "agentic.unrelated",
            };
            if index % 7 < 2 {
                expected += 1;
            }
            crate::RuntimeEventInput {
                stream_id: stream.into(),
                scope: crate::RuntimeEventScope::Program,
                kind: kind.into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({"index":index}),
            }
            .into()
        })
        .collect();
    store
        .append_batch_if_revision(stream, 0, "exact-count", events)
        .unwrap();
    backend.reset_history_reads();
    assert_eq!(completion_reopen_count(&store, "count").unwrap(), expected);
    assert_eq!(
        store
            .count_stream_kinds(
                stream,
                &[
                    COMPLETION_REOPENED_EVENT_KIND,
                    COMPLETION_REJECTED_EVENT_KIND
                ],
                0
            )
            .unwrap(),
        0
    );
    assert!(store
        .count_stream_kinds(stream, &[COMPLETION_REOPENED_EVENT_KIND], u64::MAX)
        .is_err());
    assert_eq!(backend.history_reads(), (0, 0));
}

/// The delivered result set: the artifacts accepted work produced. The
/// objective reviewer's own report is evidence of review, not a result, and
/// requiring a review to cover it is an unbounded regress - each review
/// artifact becomes the next uncovered ref, so the Objective can never close.
/// Agents whose Runtime-bound role marks them as the objective reviewer are
/// identified structurally, so this holds regardless of task naming.
pub(crate) fn delivered_result_refs(
    program: &crate::AgenticProgramProjection,
    goal: &GoalContract,
    request_refs: &[String],
) -> Vec<String> {
    // The reviewers are the actors Runtime recorded on the Goal's own review
    // records. That identity is assigned by Runtime and cannot drift: the
    // previous rule matched `agent.role` against the substring "objective
    // review", so a reviewer the model named "independent verifier", "QA" or
    // "事实核验者" was not recognized, its report re-entered the delivered set,
    // and every new review artifact became the next uncovered ref. One live run
    // spun through 14 reviews without a verdict that way.
    //
    // With no recorded review there is no signal, so nothing is excluded:
    // falling back to an explicit gap is safer than silently misjudging the set.
    let objective_reviewers = goal
        .reviews
        .iter()
        .map(|review| review.reviewer_actor.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if objective_reviewers.is_empty() {
        return request_refs.to_vec();
    }
    request_refs
        .iter()
        .filter(|reference| {
            !program.artifacts.values().any(|artifact| {
                artifact.artifact_ref == **reference
                    && objective_reviewers.contains(artifact.committed_by.as_str())
            })
        })
        .cloned()
        .collect()
}

#[cfg(test)]
use crate::agentic::review_evidence::evidence_contains;

impl GoalStore {
    /// Reuse the dispatch owner's stranded facts and the graph's original
    /// bounded replan history. The graph cut joins the conclusion CAS.
    pub(crate) fn exhausted_program_work_cut(
        &self, program: &AgenticProgramProjection,
    ) -> Result<Option<Vec<ExpectedStreamRevision>>, String> {
        let now = crate::tool_invocation::now_ms();
        let Some(root) = program.root_execution_id.as_deref() else { return Ok(None) };
        if self.projection(&format!("goal:{root}"))?.as_ref().is_some_and(|goal|
            goal.goal.completion == GoalCompletion::WaitingExternalDecision
                || goal.goal.waiting.as_ref().is_some_and(|waiting| waiting.reason_code == "waiting_external_decision"))
        { return Ok(None); }
        let state_store = crate::ExecutionGraphStateStore::new(Arc::clone(&self.event_store));
        let root_revision = self.event_store.stream_revision(root).map_err(|error| error.to_string())?;
        let root_graph = match state_store.load(root) {
            Ok(graph) => Some(graph),
            Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. }) => None,
            Err(error) => return Err(error.to_string()),
        };
        if self.event_store.stream_revision(root).map_err(|error| error.to_string())? != root_revision {
            return Ok(None);
        }
        if root_graph.as_ref().is_some_and(|graph| graph.nodes.iter().any(|node| {
            node.executor_kind != crate::execution_core::graph::executors::AgenticProgramWaitExecutor::KIND
                && matches!(graph.node_statuses.get(&node.id), Some(
                    harness_contract::execution_graph::ExecutionNodeStatus::WaitingInput
                    | harness_contract::execution_graph::ExecutionNodeStatus::WaitingApproval
                    | harness_contract::execution_graph::ExecutionNodeStatus::WaitingExternal
                    | harness_contract::execution_graph::ExecutionNodeStatus::Paused))
        })) { return Ok(None); }
        let lineage_stream = crate::execution_core::graph::execution_lineage_stream_id(root);
        let lineage_revision = self.event_store.stream_revision(&lineage_stream).map_err(|error| error.to_string())?;
        let links = state_store.child_links(root).map_err(|error| error.to_string())?;
        if self.event_store.stream_revision(&lineage_stream).map_err(|error| error.to_string())? != lineage_revision {
            return Ok(None);
        }
        let mut cuts = vec![ExpectedStreamRevision { stream_id: lineage_stream, expected_revision: lineage_revision },
            ExpectedStreamRevision { stream_id: root.to_string(), expected_revision: root_revision }];
        for link in links {
            let child_revision = self.event_store.stream_revision(&link.child_execution_id).map_err(|error| error.to_string())?;
            let child = match state_store.load(&link.child_execution_id) {
                Ok(child) => child,
                Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. }) => {
                    cuts.push(ExpectedStreamRevision { stream_id: link.child_execution_id, expected_revision: child_revision });
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            };
            if self.event_store.stream_revision(&child.id).map_err(|error| error.to_string())? != child_revision {
                return Ok(None);
            }
            let child_active = child.nodes.iter().any(|node| !child.node_statuses.get(&node.id)
                .copied().unwrap_or(harness_contract::execution_graph::ExecutionNodeStatus::Planned).is_terminal());
            if crate::execution_core::graph::executors::agentic_program_ids(&child).is_empty()
                && child.nodes.iter().any(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
                && child_active {
                return Ok(None);
            }
            if crate::execution_core::graph::executors::graph_belongs_to_program(&child, &program.program_id) {
                if child_active {
                    return Ok(None);
                }
                cuts.push(ExpectedStreamRevision { stream_id: child.id, expected_revision: child_revision });
            }
        }
        // A Task the dispatch side durably recorded as disconnected has no live
        // work left: it failed, nobody is attempting it, its lease is gone and
        // the root graph is terminal, so no agent will ever pick it up again.
        // It therefore does not keep the Program waiting.
        let disconnected = self.disconnected_task_refs(program)?;
        let unsettled = program.tasks.values().filter(|task| {
            Self::task_blocks_the_program_cut(
                task.status,
                !task.active_attempts.is_empty(),
                task.lease_expires_at_ms.is_some_and(|until| until > now),
                disconnected.contains(&task.task_id),
            )
        }).collect::<Vec<_>>();
        if unsettled.is_empty() { return Ok(Some(cuts)); }
        let stranded = crate::agentic::execution::dispatch::stranded_task_refs(program);
        if unsettled.iter().any(|task| !task.active_attempts.is_empty()
            || task.lease_expires_at_ms.is_some_and(|until| until > now)
            || !stranded.contains(&task.task_id)) { return Ok(None); }
        let Some(graph) = root_graph else { return Ok(None) };
        let resumes = graph.nodes.iter().filter(|node|
            node.executor_kind == crate::execution_core::graph::executors::AgenticProgramWaitExecutor::KIND)
            .filter_map(|node| graph.node_results.get(&node.id))
            .filter_map(|result| result.result_ref.as_deref())
            .filter(|reference| reference.starts_with(&format!("agentic-program-checkpoint:{}:revision:", program.program_id))
                && reference.ends_with(":task_stranded"))
            .count();
        if resumes < crate::execution_core::graph::executors::MAX_TASK_STRAND_RESUMES { return Ok(None); }
        Ok(Some(cuts))
    }

    /// Whether one Task still keeps the Program waiting for work.
    ///
    /// A Task with no active attempt, no live lease and a durable disconnect
    /// record has no live work left: the dispatch side recorded that the root
    /// graph was already terminal, so no agent will ever pick it up again and
    /// nothing will replan it. Every other Task still waits - a live attempt,
    /// a live lease and a Task the dispatcher never recorded a disconnect for
    /// all keep the Program open.
    fn task_blocks_the_program_cut(
        status: crate::AgenticTaskStatus,
        has_active_attempts: bool,
        has_live_lease: bool,
        disconnected: bool,
    ) -> bool {
        if disconnected
            && matches!(status, crate::AgenticTaskStatus::Rework | crate::AgenticTaskStatus::Published)
            && !has_active_attempts && !has_live_lease
        {
            return false;
        }
        has_active_attempts || has_live_lease || matches!(status,
            crate::AgenticTaskStatus::Published | crate::AgenticTaskStatus::Claimed
                | crate::AgenticTaskStatus::Submitted | crate::AgenticTaskStatus::Rework
                | crate::AgenticTaskStatus::CancelRequested)
    }

    /// Whether every Task still in the waiting bucket is durably disconnected:
    /// it failed, has no active attempt and no live lease, and the dispatch
    /// side recorded that the root graph was already terminal. An empty
    /// waiting bucket is not a disconnected Program, and a Task the dispatcher
    /// never recorded a disconnect for is not disconnected either - the
    /// process-local dispatch starvation ledger is never consulted.
    fn waiting_tasks_are_all_durably_disconnected(
        waiting: &[(&str, crate::AgenticTaskStatus, bool, bool)],
        disconnected: &[String],
    ) -> bool {
        if waiting.is_empty() { return false; }
        waiting.iter().all(|(task_id, status, has_active_attempts, has_live_lease)| {
            disconnected.iter().any(|recorded| recorded == task_id)
                && matches!(status, crate::AgenticTaskStatus::Rework | crate::AgenticTaskStatus::Published)
                && !has_active_attempts && !has_live_lease
        })
    }

    /// The durable shape the loop-exit trigger reads: every Task still in the
    /// waiting bucket is disconnected by a durable record. It reads no
    /// process-local state, so the same Program reads the same way after a
    /// restart.
    pub(crate) fn program_tasks_all_durably_disconnected(
        &self,
        program: &AgenticProgramProjection,
    ) -> Result<bool, String> {
        let now = crate::tool_invocation::now_ms();
        let disconnected = self.disconnected_task_refs(program)?;
        let waiting = program.tasks.values()
            .filter(|task| matches!(task.status,
                crate::AgenticTaskStatus::Published | crate::AgenticTaskStatus::Claimed
                    | crate::AgenticTaskStatus::Submitted | crate::AgenticTaskStatus::Rework
                    | crate::AgenticTaskStatus::CancelRequested))
            .map(|task| (task.task_id.as_str(), task.status,
                !task.active_attempts.is_empty(),
                task.lease_expires_at_ms.is_some_and(|until| until > now)))
            .collect::<Vec<_>>();
        Ok(Self::waiting_tasks_are_all_durably_disconnected(&waiting, &disconnected))
    }

    /// Tasks the dispatch side durably recorded as disconnected: the dispatch
    /// was attempted after the root graph had already become terminal, so no
    /// agent can ever pick them up again and nothing will ever replan them.
    ///
    /// The record is a durable event on the program's dispatch stream
    /// (`agentic.dispatch.declined`, written by
    /// `AgentActionService::record_dispatch_decline`), not the process-local
    /// dispatch starvation ledger: the ledger is process memory, and a
    /// conclusion may not depend on it.
    fn disconnected_task_refs(
        &self,
        program: &AgenticProgramProjection,
    ) -> Result<Vec<String>, String> {
        const DISPATCH_DECLINED_EVENT_KIND: &str = "agentic.dispatch.declined";
        const ROOT_GRAPH_TERMINAL_REASON: &str = "root_graph_terminal";
        let now = crate::tool_invocation::now_ms();
        let candidates = program.tasks.values().filter(|task| {
            matches!(task.status, crate::AgenticTaskStatus::Rework | crate::AgenticTaskStatus::Published)
                && task.active_attempts.is_empty()
                && !task.lease_expires_at_ms.is_some_and(|until| until > now)
        }).collect::<Vec<_>>();
        if candidates.is_empty() { return Ok(Vec::new()); }
        let stream_id = format!("agentic-dispatch:{}", program.program_id);
        let revision = self.event_store.stream_revision(&stream_id).map_err(|error| error.to_string())?;
        if revision == 0 { return Ok(Vec::new()); }
        let mut latest = std::collections::BTreeMap::<String, String>::new();
        for event in self.read_conclusion_events(&stream_id, revision)? {
            if event.kind != DISPATCH_DECLINED_EVENT_KIND { continue; }
            let Some(task_ref) = event.payload.get("task_ref").and_then(|value| value.as_str()) else { continue; };
            let Some(reason) = event.payload.get("reason").and_then(|value| value.as_str()) else { continue; };
            latest.insert(task_ref.to_string(), reason.to_string());
        }
        Ok(candidates.into_iter()
            .filter(|task| latest.get(&task.task_id).is_some_and(|reason| reason == ROOT_GRAPH_TERMINAL_REASON))
            .map(|task| task.task_id.clone())
            .collect())
    }

    /// Count the original rejection events after the last verified Goal gain.
    /// A legacy marker without code and action identity cannot prove sameness.
    pub(crate) fn exhausted_completion_rejection(
        &self, program: &AgenticProgramProjection,
    ) -> Result<Option<String>, String> {
        let Some(limit) = completion_reopen_bound()? else { return Ok(None) };
        if program.completion_request.is_some() || self.exhausted_program_work_cut(program)?.is_none() { return Ok(None); }
        let Some(root) = program.root_execution_id.as_deref() else { return Ok(None) };
        let Some(goal) = self.projection(&format!("goal:{root}"))? else { return Ok(None) };
        let mut last_gain = 0;
        let mut seen = std::collections::BTreeSet::new();
        let mut accepted = std::collections::BTreeSet::new();
        for event in self.read_conclusion_events(&format!("goal:{}", goal.goal.id), goal.stream_revision)? {
            if let Some(value) = event.payload.get("goal") {
                let snapshot: GoalContract = serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
                let facts = snapshot.criteria.iter().filter(|criterion| matches!(criterion.status, AcceptanceStatus::Satisfied | AcceptanceStatus::Waived))
                    .map(|criterion| format!("criterion:{}:{:?}", criterion.id, criterion.status))
                    .chain(snapshot.obligations.iter().filter(|obligation| matches!(obligation.state,
                        ObjectiveObligationState::Satisfied | ObjectiveObligationState::Waived))
                        .map(|obligation| format!("obligation:{}:{:?}", obligation.obligation_id, obligation.state)))
                    .collect::<std::collections::BTreeSet<_>>();
                if facts.difference(&accepted).next().is_some() { last_gain = last_gain.max(event.commit_cursor); }
                accepted = facts;
            }
            if event.kind == "goal.observation" {
                {
                    let value = self.event_store.resolve_observation_record(&event)?;
                    let observation: RuntimeObservation = serde_json::from_value(value).map_err(|error| error.to_string())?;
                    if seen.insert(observation.idempotency_fingerprint()) && observation.has_verified_gain() {
                        last_gain = last_gain.max(event.commit_cursor);
                    }
                }
            }
        }
        let events = self.read_conclusion_events(&format!("agentic-program:{}", program.program_id), program.revision)?;
        for event in &events {
            if event.kind == "agentic.action_applied" {
                if let Ok(envelope) = serde_json::from_value::<harness_contract::agent_action::AgentActionEnvelope>(event.payload["envelope"].clone()) {
                    if matches!(envelope.action, harness_contract::agent_action::AgentAction::TaskReview(ref input)
                        if input.decision == harness_contract::agent_action::TaskReviewDecision::Accept) {
                        last_gain = last_gain.max(event.commit_cursor);
                    }
                }
            }
        }
        let mut counts = std::collections::BTreeMap::<String, u64>::new();
        let mut exhausted = None;
        for event in events.into_iter().filter(|event| event.commit_cursor > last_gain && event.kind == COMPLETION_REJECTED_EVENT_KIND) {
            let Some(code) = event.payload.get("code").and_then(serde_json::Value::as_str) else { continue };
            let Some(action) = event.payload.get("action") else { continue };
            let Some(action_id) = event.payload.get("request_action_id").and_then(serde_json::Value::as_str) else { continue };
            let Ok(envelope) = serde_json::from_value::<harness_contract::agent_action::AgentActionEnvelope>(event.payload["envelope"].clone()) else { continue };
            if !matches!(&envelope.action, harness_contract::agent_action::AgentAction::ObjectiveCompleteRequest(_))
                || envelope.action_id != action_id || envelope.actor.program_id != program.program_id
                || envelope.actor.objective_id != program.objective_id
                || envelope.actor.session_id != program.session_id || envelope.actor.turn_id != program.turn_id
                || envelope.actor.root_execution_id != program.root_execution_id
                || event.actor.as_deref() != Some(envelope.actor.actor_id.as_str())
                || event.payload["objective_id"] != program.objective_id
                || serde_json::to_value(&envelope.action).map_err(|error| error.to_string())? != *action
            { continue; }
            let mut normalized = envelope.action.clone();
            if let harness_contract::agent_action::AgentAction::ObjectiveCompleteRequest(input) = &mut normalized {
                input.result_refs.sort();
                input.evidence_refs.sort();
                input.unresolved.sort();
                input.recommended_next_actions.sort();
            }
            let identity = serde_json::to_string(&(code, &event.payload["objective_id"], normalized, &event.payload["gap"]))
                .map_err(|error| error.to_string())?;
            let count = counts.entry(identity).or_default();
            *count = count.saturating_add(1);
            if *count >= limit { exhausted = Some(action_id.to_string()); }
        }
        Ok(exhausted)
    }

    fn read_conclusion_events(&self, stream: &str, through: u64) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        let mut events = Vec::new();
        let mut cursor = 0;
        while cursor < through {
            let page = self.event_store.list_stream_after(stream, cursor, through, 128, usize::MAX)?;
            if page.is_empty() { return Err("conclusion event stream ended before its source fence".into()); }
            for event in page {
                if event.sequence != cursor + 1 { return Err("conclusion event stream has a gap".into()); }
                cursor = event.sequence;
                events.push(event);
            }
        }
        Ok(events)
    }

    pub(crate) fn prepare_rejected_program_conclusion(
        &self, program: &AgenticProgramProjection, action_id: &str,
    ) -> Result<PreparedProgramConclusion, String> {
        if self.exhausted_completion_rejection(program)?.as_deref() != Some(action_id) {
            return Err("completion rejection has not exhausted its original resource bound".into());
        }
        let root = program.root_execution_id.as_deref().ok_or("missing root binding")?;
        let projection = self.projection(&format!("goal:{root}"))?.ok_or("missing bound Goal")?;
        if projection.goal.completion != GoalCompletion::Open || projection.goal.terminal.is_some() {
            return Err("Goal is not open".into());
        }
        let mut source_frame = AssessmentSourceFrame::capture(program, &projection).map_err(|error| error.to_string())?;
        for cut in self.exhausted_program_work_cut(program)?.ok_or("Program has live work")? {
            source_frame.pin(cut).map_err(|error| error.to_string())?;
        }
        let mut goal = projection.goal;
        goal.evidence_refs.extend(projection.progress.evidence_refs.iter().cloned());
        goal.evidence_refs.extend(program.tasks.values()
            .filter(|task| task.status == crate::AgenticTaskStatus::Accepted)
            .flat_map(|task| task.evidence_refs.iter().chain(&task.artifact_refs).cloned()));
        goal.evidence_refs.sort();
        goal.evidence_refs.dedup();
        goal.revision = goal.revision.saturating_add(1);
        goal.completion = GoalCompletion::Blocked;
        goal.phase = "completed".into();
        goal.waiting = None;
        let fence = format!("agentic-objective:{}:runtime:{}", program.program_id, program.revision);
        let rejection = self.event_store.event_by_idempotency_key(
            &format!("agentic-program:{}", program.program_id), &format!("completion-rejected:{action_id}"),
        ).map_err(|error| error.to_string())?.ok_or("completion rejection disappeared")?;
        let code = rejection.payload["code"].as_str().ok_or("completion rejection code missing")?;
        let gap = rejection.payload["gap"].as_str().ok_or("completion rejection reason missing")?;
        let reason = format!("The configured completion resource bound was exhausted by the same durable rejection without verified progress; the objective remains unmet: {code}: {gap}");
        goal.terminal = Some(ObjectiveTerminal { kind: ObjectiveTerminalKind::Blocked,
            terminal_fence: fence.clone(), authority_revision: program.revision, reason: reason.clone(),
            evidence_refs: goal.evidence_refs.clone(), diagnostics: Vec::new(), committed_at_ms: crate::tool_invocation::now_ms() });
        validate_goal(&goal)?;
        let event = goal_event(&goal, "goal.blocked", "blocked", "runtime.objective_supervisor".into(),
            vec![RuntimeEventRef { kind: "program".into(), id: program.program_id.clone() }],
            serde_json::json!({"goal": goal, "rejection_action_id": action_id}), format!("program-conclusion:{fence}"));
        Ok(PreparedProgramConclusion { goal, expected_stream_revision: projection.stream_revision, event,
            gaps: vec![reason], source_frame, verified_proofs: Vec::new(), proof_seal: None, runtime_trigger: None, rejection_trigger: Some(action_id.into()) })
    }

    /// Prepare an honest blocked verdict from an already committed Runtime
    /// observation. The Program owner commits this with the Program verdict.
    pub(crate) fn prepare_runtime_program_conclusion(
        &self,
        program: &AgenticProgramProjection,
        trigger: &RuntimeObservation,
    ) -> Result<PreparedProgramConclusion, String> {
        if program.completion_request.is_some() || self.exhausted_program_work_cut(program)?.is_none() {
            return Err("runtime conclusion cannot replace a completion request or live work".into());
        }
        let root = program.root_execution_id.as_deref().ok_or("missing root binding")?;
        let projection = self.projection(&format!("goal:{root}"))?.ok_or("missing bound Goal")?;
        let committed_trigger = self.event_store.event_by_idempotency_key(
            &format!("goal:{}", projection.goal.id),
            &format!("direct:{}", trigger.idempotency_fingerprint()),
        ).map_err(|error| error.to_string())?.ok_or("runtime stall observation was not committed by its owner")?;
        if projection.goal.completion != GoalCompletion::Open
            || projection.goal.terminal.is_some()
            || trigger.source != "runtime.program_stall"
            || trigger.goal_id() != projection.goal.id
            || trigger.identity.graph_id != root
            || trigger.source_revision != program.revision
            || trigger.result_class != harness_contract::goal::ObservationResultClass::Failed
            || !projection.observations.contains(trigger)
            || committed_trigger.sequence != projection.stream_revision
            || serde_json::from_value::<RuntimeObservation>(self.event_store.resolve_observation_record(&committed_trigger)?)
                .map_err(|error| error.to_string())? != *trigger
        {
            return Err("runtime conclusion trigger is not a committed bound stall observation".into());
        }
        let mut source_frame = AssessmentSourceFrame::capture(program, &projection)
            .map_err(|error| error.to_string())?;
        for cut in self.exhausted_program_work_cut(program)?.ok_or("Program has live work")? {
            source_frame.pin(cut).map_err(|error| error.to_string())?;
        }
        let mut goal = projection.goal;
        goal.evidence_refs.extend(projection.progress.evidence_refs.iter().cloned());
        goal.evidence_refs.extend(program.tasks.values()
            .filter(|task| task.status == crate::AgenticTaskStatus::Accepted)
            .flat_map(|task| task.evidence_refs.iter().chain(&task.artifact_refs).cloned()));
        goal.evidence_refs.sort();
        goal.evidence_refs.dedup();
        goal.revision = goal.revision.saturating_add(1);
        goal.completion = GoalCompletion::Blocked;
        goal.phase = "completed".into();
        goal.waiting = None;
        let fence = format!("agentic-objective:{}:runtime:{}", program.program_id, program.revision);
        goal.terminal = Some(ObjectiveTerminal {
            kind: ObjectiveTerminalKind::Blocked,
            terminal_fence: fence.clone(),
            authority_revision: program.revision,
            reason: trigger.summary.clone(),
            evidence_refs: goal.evidence_refs.clone(),
            diagnostics: Vec::new(),
            committed_at_ms: crate::tool_invocation::now_ms(),
        });
        validate_goal(&goal)?;
        let event = goal_event(&goal, "goal.blocked", "blocked", "runtime.objective_supervisor".into(),
            vec![RuntimeEventRef { kind: "program".into(), id: program.program_id.clone() }],
            serde_json::json!({"goal": goal, "runtime_trigger": trigger.idempotency_fingerprint()}),
            format!("program-conclusion:{fence}"));
        Ok(PreparedProgramConclusion { goal, expected_stream_revision: projection.stream_revision,
            event, gaps: vec![trigger.summary.clone()], source_frame, verified_proofs: Vec::new(), proof_seal: None, runtime_trigger: Some(trigger.clone()), rejection_trigger: None })
    }

    pub(crate) fn prepare_program_conclusion(
        &self,
        program: &AgenticProgramProjection,
    ) -> Result<PreparedProgramConclusion, String> {
        let request = program
            .completion_request
            .as_ref()
            .ok_or("missing completion request")?;
        let root = program
            .root_execution_id
            .as_deref()
            .ok_or("missing root binding")?;
        let projection = self
            .projection(&format!("goal:{root}"))?
            .ok_or("missing bound Goal")?;
        let mut source_frame =
            AssessmentSourceFrame::new(program, &projection).map_err(|error| error.to_string())?;
        let mut goal = projection.goal;
        let mut evidence = goal.evidence_refs.clone();
        evidence.extend(projection.progress.evidence_refs.iter().cloned());
        evidence.extend(request.evidence_refs.iter().cloned());
        evidence.extend(
            goal.obligations
                .iter()
                .flat_map(|obligation| obligation.evidence_refs.iter().cloned()),
        );
        evidence.push(format!("execution_graph:{root}"));
        for task in program.tasks.values() {
            evidence.extend(task.evidence_refs.iter().cloned());
            evidence.extend(task.supersede_evidence_refs.iter().cloned());
        }
        // A review's own evidence is what proves it read the results, and it is
        // produced BY the review: the reviewer's `evidence_retrieve` receipts
        // live in its own Session journal and cannot already be in the
        // Program's accumulated set. Requiring them to be there made the check
        // circular - no review could ever be the thing that adds its own proof,
        // so a satisfied review was rejudged as uncovered and the Objective
        // reopened forever. Each review's evidence was already resolved and
        // verified when the review was applied, so it is known durable here.
        for review in &goal.reviews {
            evidence.extend(review.evidence_refs.iter().cloned());
        }
        let mut evidence = evidence
            .iter()
            .map(|reference| {
                let bound =
                    crate::agentic::review_evidence::bound_artifact_identities(program, reference)?;
                Ok(if bound.is_empty() {
                    vec![reference.clone()]
                } else {
                    bound
                })
            })
            .collect::<Result<Vec<Vec<String>>, String>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        evidence.sort();
        evidence.dedup();
        apply_completion_criterion_state(&mut goal, &projection.progress, &evidence);
        let conditions = crate::agentic::review_evidence::required_review_conditions(&goal);
        let mut proof_inputs = std::collections::BTreeMap::new();
        let mut assessments = std::collections::BTreeMap::new();
        let history = self
            .proof_session_queries
            .as_ref()
            .and_then(|queries| queries.get())
            .and_then(|queries| queries.history_reader());
        let process_frame = Some(self.process_input_verifier.as_ref()
            .map(crate::agentic::review_evidence::ProcessInputVerifier::frame)
            .unwrap_or_else(crate::agentic::review_evidence::ProcessInputFrame::without_process_verifier));
        let readers = crate::agentic::review_evidence::ResultContentReaders {
            artifacts: self.proof_artifacts.as_deref(),
            history: history.as_deref(),
            process_inputs: process_frame.as_ref(),
        };
        for condition in conditions {
            let assessment = (|| {
                let review = goal
                    .reviews
                    .iter()
                    .rev()
                    .find(|review| review.criterion_ref == condition)
                    .ok_or_else(|| "latest review missing".to_string())?;
                let proof = review
                    .verification
                    .as_ref()
                    .ok_or_else(|| "latest review proof missing".to_string())?;
                let (snapshot, verified) = crate::agentic::review_evidence::validate_applied_proof_for_conclusion(
                    &self.event_store,
                    program,
                    &goal,
                    proof,
                    readers,
                )?;
                let mut results = review.result_refs.clone();
                if goal.user_intent_criterion_id.as_ref() == Some(&condition) {
                    results.extend(delivered_result_refs(program, &goal, &request.result_refs));
                }
                for obligation in goal.obligations.iter().filter(|obligation| {
                    obligation.required && obligation.obligation_id == condition
                }) {
                    results.extend(obligation.artifact_refs.iter().cloned());
                }
                crate::agentic::review_evidence::validate_review_coverage(
                    &goal, program, review, &results, &evidence, &snapshot,
                )
                .map_err(|error| error.to_string())?;
                proof_inputs.insert(condition.clone(), (snapshot, verified, review.clone(), results, evidence.clone(),
                    goal.spec_revision, goal.spec_digest.clone()));
                Ok::<(), String>(())
            })();
            if let Err(error) = &assessment {
                if error.contains("effect_review_pending") {
                    return Err(error.clone());
                }
            }
            assessments.insert(condition, assessment);
        }

        // A graph identity proves execution lineage, not semantic coverage of
        // the user's request. Consume the current, independently attributed
        // Objective review rather than manufacturing obligations from Tasks.
        let mut gaps = assessments
            .iter()
            .filter_map(|(condition, outcome)| {
                outcome
                    .as_ref()
                    .err()
                    .map(|error| format!("review_validation_failed:{condition}:{error}"))
            })
            .collect::<Vec<_>>();
        if let Some(participation) = goal.participation_requirement.as_ref() {
            let contributors = program
                .tasks
                .values()
                .filter(|task| {
                    task.status == crate::AgenticTaskStatus::Accepted
                        && task.claimant.is_some()
                        && task.reviewed_by.is_some()
                        && task.claimant != task.reviewed_by
                        && !task.evidence_refs.is_empty()
                })
                .map(|task| &task.team_id)
                .collect::<std::collections::BTreeSet<_>>();
            if contributors.len() < usize::from(participation.minimum_team_count) {
                gaps.push(format!(
                    "original_participation_missing:expected={},actual={}",
                    participation.minimum_team_count,
                    contributors.len()
                ));
            }
        }
        let intent = goal
            .user_intent_criterion_id
            .clone()
            .ok_or("missing original user intent")?;
        // The objective reviewer's own report is evidence OF review, never a
        // delivered result. One live loop cited it in every completion
        // request, so each new review artifact became the next uncovered ref
        // and the collaboration could never close (14 reviews, 0 verdicts).
        // The delivered result set is what accepted work produced; artifacts
        // committed by the Runtime-bound objective reviewer are excluded.
        let reviewed = assessments.get(&intent).is_some_and(Result::is_ok);
        for obligation in &goal.obligations {
            if obligation.required {
                let current = assessments
                    .get(&obligation.obligation_id)
                    .is_some_and(Result::is_ok);
                if !current {
                    gaps.push(format!(
                        "obligation_review_required:{}",
                        obligation.obligation_id
                    ));
                }
            }
        }

        if let Some(criterion) = goal
            .criteria
            .iter_mut()
            .find(|criterion| criterion.id == intent)
        {
            if criterion.status != AcceptanceStatus::Waived {
                criterion.status = if reviewed {
                    AcceptanceStatus::Satisfied
                } else {
                    AcceptanceStatus::Open
                };
            }
        }
        if !reviewed {
            // Name the cited results no current review covers. A live loop had
            // the root cite four artifacts while the recorded review covered
            // three; without the missing refs the model re-issued the same
            // partial review until the observation window expired.
            let covered = goal
                .reviews
                .iter()
                .rev()
                .find(|review| review.criterion_ref == intent)
                .map(|review| review.result_refs.clone())
                .unwrap_or_default();
            let mut gap = format!("original_intent_review_required:{intent}");
            let missing = request
                .result_refs
                .iter()
                .filter(|reference| !covered.contains(reference))
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                gap.push_str(&format!("; uncovered_result_refs: {}", missing.join(", ")));
            }
            // A satisfied review that later work invalidated must say so. Naming
            // only "review required" showed the model the same gap string it had
            // already answered once, so it repeated the same action: one live run
            // satisfied the review, had a Task reworked, and then re-issued the
            // identical request four times because nothing told it the earlier
            // satisfaction no longer held.
            let prior_satisfied =
                goal.reviews.iter().rev().find(|review| {
                    review.criterion_ref == intent && review.decision == "satisfied"
                });
            if let Some(prior) = prior_satisfied {
                if !request
                    .result_refs
                    .iter()
                    .all(|reference| prior.result_refs.contains(reference))
                {
                    gap.push_str(&format!(
                        "; previous_satisfied_review_invalidated: it covered [{}] but the \
                         current delivered set is [{}]; read the new results yourself in this \
                         execution and submit one fresh objective_review covering the full \
                         current set",
                        prior.result_refs.join(", "),
                        request.result_refs.join(", ")
                    ));
                }
            }
            gaps.push(gap);
        }
        let stale_criteria = goal
            .criteria
            .iter()
            .filter(|criterion| {
                criterion.id != intent && criterion.status == AcceptanceStatus::Satisfied
            })
            .filter(|criterion| !assessments.get(&criterion.id).is_some_and(Result::is_ok))
            .map(|criterion| criterion.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for criterion in &mut goal.criteria {
            if stale_criteria.contains(&criterion.id) {
                criterion.status = AcceptanceStatus::Open;
                gaps.push(format!("criterion_review_required:{}", criterion.id));
            }
        }
        for criterion in &goal.criteria {
            if !matches!(
                criterion.status,
                AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
            ) {
                gaps.push(format!("criterion_unsatisfied:{}", criterion.id));
            }
        }
        if let Err(gap) = validate_completion(
            &goal,
            &projection.progress,
            GoalCompletion::Satisfied,
            &evidence,
        ) {
            gaps.push(gap);
        }
        goal.evidence_refs = evidence;
        goal.revision = goal.revision.saturating_add(1);
        let fence = format!(
            "agentic-objective:{}:request:{}",
            program.program_id, request.program_revision
        );
        let reopen_budget = completion_reopen_bound()?
            .map(|limit| {
                completion_reopen_count(&self.event_store, &program.program_id)
                    .map(|count| (limit, count))
            })
            .transpose()?;
        let (kind, status) = if gaps.is_empty() {
            goal.completion = GoalCompletion::Satisfied;
            goal.phase = "completed".into();
            goal.waiting = None;
            goal.terminal = Some(ObjectiveTerminal {
                kind: ObjectiveTerminalKind::Satisfied,
                terminal_fence: fence.clone(),
                authority_revision: request.program_revision,
                reason: "Current original-objective review and durable acceptance evidence are satisfied".into(),
                evidence_refs: goal.evidence_refs.clone(),
                diagnostics: Vec::new(),
                committed_at_ms: crate::tool_invocation::now_ms(),
            });
            ("goal.completed", "satisfied")
        } else if reopen_budget.is_none_or(|(limit, count)| count < limit) {
            goal.phase = "waiting".into();
            goal.waiting = Some(harness_contract::goal::WaitDescriptor {
                reason_code: "completion_gap".into(),
                reason: gaps.join("; "),
                required_refs: gaps.clone(),
                generation: request.program_revision,
            });
            ("goal.completion_waiting", "open")
        } else {
            let (limit, reopen_count) =
                reopen_budget.ok_or("missing explicit completion resource limit")?;
            goal.completion = GoalCompletion::Blocked;
            goal.phase = "completed".into();
            goal.waiting = None;
            goal.terminal = Some(ObjectiveTerminal {
                kind: ObjectiveTerminalKind::Blocked,
                terminal_fence: fence.clone(),
                authority_revision: request.program_revision,
                reason: format!(
                    "operator-configured completion resource limit {limit} exhausted after {reopen_count} rejection(s); objective remains unmet: {}",
                    gaps.join("; ")
                ),
                evidence_refs: goal.evidence_refs.clone(),
                diagnostics: Vec::new(),
                committed_at_ms: crate::tool_invocation::now_ms(),
            });
            ("goal.blocked", "blocked")
        };
        validate_goal(&goal)?;
        let event = goal_event(
            &goal,
            kind,
            status,
            "runtime.objective_supervisor".into(),
            vec![RuntimeEventRef {
                kind: "program".into(),
                id: program.program_id.clone(),
            }],
            serde_json::json!({"goal": goal, "request_revision": request.program_revision}),
            format!("program-conclusion:{fence}"),
        );
        let mut verified_proofs = Vec::new();
        for (condition, (snapshot, verified, reviewed, results, checked_evidence, spec_revision, spec_digest)) in proof_inputs {
            let final_review = goal.reviews.iter().rev().find(|review| review.criterion_ref == condition)
                .ok_or("prepared coverage review disappeared")?;
            let mut final_results = final_review.result_refs.clone();
            if goal.user_intent_criterion_id.as_ref() == Some(&condition) {
                final_results.extend(delivered_result_refs(program, &goal, &request.result_refs));
            }
            for obligation in goal.obligations.iter().filter(|obligation|
                obligation.required && obligation.obligation_id == condition) {
                final_results.extend(obligation.artifact_refs.iter().cloned());
            }
            if final_review != &reviewed || final_results != results || goal.evidence_refs != checked_evidence
                || goal.spec_revision != spec_revision || goal.spec_digest != spec_digest {
                return Err("prepared coverage inputs changed during conclusion preparation".into());
            }
            for source in verified.current_heads() {
                source_frame.pin(source.clone()).map_err(|error| error.to_string())?;
            }
            for (target, digest, bytes) in snapshot.dependency_frame().protected_files() {
                process_frame.as_ref().ok_or("original File frame unavailable")?
                    .retain_original_file(target, digest, bytes)?;
            }
            verified_proofs.push(std::sync::Arc::new(PreparedVerifiedProof {
                condition, snapshot, proof: verified,
            }));
        }
        let proof_seal = Some(PreparedProofSeal {
            store: std::sync::Arc::clone(&self.event_store), program: program.clone(),
            goal: goal.clone(), event: event.clone(), expected_stream_revision: projection.stream_revision,
            gaps: gaps.clone(), sources: source_frame.sources().cloned().collect(),
            _process_inputs: process_frame.map(std::sync::Arc::new),
        });
        Ok(PreparedProgramConclusion {
            goal,
            expected_stream_revision: projection.stream_revision,
            event,
            gaps,
            verified_proofs,
            proof_seal,
            runtime_trigger: None,
            rejection_trigger: None,
            source_frame,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent_action::MessagePublishInput;
    #[test]
    fn the_objective_reviewers_own_report_is_not_a_delivered_result() {
        // Requiring a review to cover its own report is an unbounded regress:
        // every review artifact becomes the next uncovered ref, so the
        // Objective can never close (one live loop: 14 reviews, 0 verdicts).
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        for (id, role) in [
            ("agent:producer", "source reader"),
            ("agent:reviewer", "fresh independent objective reviewer"),
        ] {
            program.agents.insert(
                id.to_string(),
                crate::AgentMemberProjection {
                    agent_id: id.to_string(),
                    display_name: id.to_string(),
                    role: role.to_string(),
                    mission: "work".to_string(),
                    required_capabilities: vec![],
                    invited_by: "root".to_string(),
                    membership_ids: vec![],
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                },
            );
        }
        program.artifacts.insert(
            "artifact:delivered".to_string(),
            crate::AgenticArtifactProjection {
                artifact_ref: "artifact:delivered".to_string(),
                content_ref: "artifact://art_delivered".to_string(),
                kind: "report".to_string(),
                title: "delivered fact".to_string(),
                relates_to: vec![],
                committed_by: "agent:producer".to_string(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        program.artifacts.insert(
            "artifact:review-report".to_string(),
            crate::AgenticArtifactProjection {
                artifact_ref: "artifact:review-report".to_string(),
                content_ref: "artifact://art_review".to_string(),
                kind: "report".to_string(),
                title: "objective review report".to_string(),
                relates_to: vec![],
                committed_by: "agent:reviewer".to_string(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        let goal = harness_contract::goal::GoalContract {
            input_contract: None,
            id: "goal:test".to_string(),
            session_id: "session-test".to_string(),
            objective: "objective".to_string(),
            criteria: Vec::new(),
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: Default::default(),
            user_intent_criterion_id: None,
            source_intent_ref: None,
            execution_binding: None,
            spec_revision: 1,
            spec_digest: String::new(),
            review_refs: Vec::new(),
            reviews: vec![harness_contract::goal::ObjectiveReviewRecord {
                review_id: "objective_review:1".to_string(),
                criterion_ref: "user_intent".to_string(),
                spec_revision: 1,
                input_manifest_digest: String::new(),
                result_refs: Vec::new(),
                evidence_refs: Vec::new(),
                reviewer_actor: "agent:reviewer".to_string(),
                reviewer_execution_id: None,
                producer_refs: Vec::new(),
                decision: "satisfied".to_string(),
                reason_ref: String::new(),
                verification: None,
            }],
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: harness_contract::goal::GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        let delivered = delivered_result_refs(
            &program,
            &goal,
            &[
                "artifact:delivered".to_string(),
                "artifact:review-report".to_string(),
            ],
        );
        assert_eq!(delivered, vec!["artifact:delivered".to_string()]);
    }

    /// The exclusion must not depend on what the reviewer is called. The rule
    /// used to match `agent.role` against the substring "objective review", so
    /// a reviewer the model named "independent verifier", "QA" or "事实核验者"
    /// slipped through, its report re-entered the delivered set, and every new
    /// review artifact became the next uncovered ref. The Goal's own
    /// `reviewer_actor` is assigned by Runtime and cannot drift.
    #[test]
    fn the_reviewers_own_report_is_excluded_whatever_the_role_is_named() {
        for role in [
            "independent verifier",
            "QA",
            "事实核验者",
            "fresh independent objective reviewer",
        ] {
            let mut program = crate::AgenticProgramProjection::empty("program", "objective");
            program.agents.insert(
                "agent:reviewer".to_string(),
                crate::AgentMemberProjection {
                    agent_id: "agent:reviewer".to_string(),
                    display_name: "reviewer".to_string(),
                    role: role.to_string(),
                    mission: "review".to_string(),
                    required_capabilities: vec![],
                    invited_by: "root".to_string(),
                    membership_ids: vec![],
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                },
            );
            program.artifacts.insert(
                "artifact:review-report".to_string(),
                crate::AgenticArtifactProjection {
                    artifact_ref: "artifact:review-report".to_string(),
                    content_ref: "artifact://art_review".to_string(),
                    kind: "report".to_string(),
                    title: "review".to_string(),
                    relates_to: vec![],
                    committed_by: "agent:reviewer".to_string(),
                    claim_execution_id: None,
                    claim_generation: None,
                },
            );
            let goal = harness_contract::goal::GoalContract {
                input_contract: None,
                id: "goal:test".to_string(),
                session_id: "session-test".to_string(),
                objective: "objective".to_string(),
                criteria: Vec::new(),
                constraints: Vec::new(),
                phase: "execution".to_string(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                blockers: Vec::new(),
                scope: Default::default(),
                user_intent_criterion_id: None,
                source_intent_ref: None,
                execution_binding: None,
                spec_revision: 1,
                spec_digest: String::new(),
                review_refs: Vec::new(),
                reviews: vec![harness_contract::goal::ObjectiveReviewRecord {
                    review_id: "objective_review:1".to_string(),
                    criterion_ref: "user_intent".to_string(),
                    spec_revision: 1,
                    input_manifest_digest: String::new(),
                    result_refs: Vec::new(),
                    evidence_refs: Vec::new(),
                    reviewer_actor: "agent:reviewer".to_string(),
                    reviewer_execution_id: None,
                    producer_refs: Vec::new(),
                    decision: "satisfied".to_string(),
                    reason_ref: String::new(),
                    verification: None,
                }],
                waiting: None,
                participation_requirement: None,
                obligations: Vec::new(),
                recovery: None,
                terminal: None,
                completion: harness_contract::goal::GoalCompletion::Open,
                revision: 1,
                user_sequence: 1,
            };
            let delivered =
                delivered_result_refs(&program, &goal, &["artifact:review-report".to_string()]);
            assert!(
                delivered.is_empty(),
                "role {role:?} must not change the exclusion, got {delivered:?}"
            );
        }
    }

    /// With no recorded review there is no structural signal, so nothing is
    /// excluded. Falling back to an explicit gap is safer than silently
    /// misjudging the delivered set.
    #[test]
    fn without_a_recorded_review_nothing_is_excluded() {
        let program = crate::AgenticProgramProjection::empty("program", "objective");
        let goal = harness_contract::goal::GoalContract {
            input_contract: None,
            id: "goal:test".to_string(),
            session_id: "session-test".to_string(),
            objective: "objective".to_string(),
            criteria: Vec::new(),
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: Default::default(),
            user_intent_criterion_id: None,
            source_intent_ref: None,
            execution_binding: None,
            spec_revision: 1,
            spec_digest: String::new(),
            review_refs: Vec::new(),
            reviews: Vec::new(),
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: harness_contract::goal::GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        let refs = vec!["artifact:anything".to_string()];
        assert_eq!(delivered_result_refs(&program, &goal, &refs), refs);
    }

    /// Runtime tags a ref an Agent action changed as `agentic_changed:<ref>`.
    /// A reviewer citing the plain ref is citing the same thing, and rejecting it
    /// kept the Objective open while teaching the model nothing.
    #[test]
    fn a_provenance_tagged_ref_satisfies_the_plain_reference() {
        let evidence = vec![
            "agentic_changed:message:fc83095f58e63aa8cad9b887".to_string(),
            "tool://receipt".to_string(),
        ];
        assert!(evidence_contains(
            &evidence,
            "message:fc83095f58e63aa8cad9b887"
        ));
        assert!(evidence_contains(&evidence, "tool://receipt"));
        assert!(!evidence_contains(&evidence, "message:other"));
    }

    /// A review's own read receipts are produced BY the review, so they cannot
    /// already be in the Program's accumulated set. They were resolved and
    /// verified when the review was applied, so the completion gate must accept
    /// them rather than reopen forever.
    #[test]
    fn a_reviews_own_evidence_counts_toward_the_completion_gate() {
        let goal = harness_contract::goal::GoalContract {
            input_contract: None,
            id: "goal:test".to_string(),
            session_id: "session-test".to_string(),
            objective: "objective".to_string(),
            criteria: Vec::new(),
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: Default::default(),
            user_intent_criterion_id: None,
            source_intent_ref: None,
            execution_binding: None,
            spec_revision: 1,
            spec_digest: String::new(),
            review_refs: Vec::new(),
            reviews: vec![harness_contract::goal::ObjectiveReviewRecord {
                review_id: "objective_review:1".to_string(),
                criterion_ref: "user_intent".to_string(),
                spec_revision: 1,
                input_manifest_digest: String::new(),
                result_refs: Vec::new(),
                evidence_refs: vec!["tool://reviewer-read-receipt".to_string()],
                reviewer_actor: "agent:reviewer".to_string(),
                reviewer_execution_id: None,
                producer_refs: Vec::new(),
                decision: "satisfied".to_string(),
                reason_ref: String::new(),
                verification: None,
            }],
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: harness_contract::goal::GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        // Exactly the accumulation the completion gate performs: the review's
        // own receipts are not in the Program's set until they are added.
        let mut evidence = goal.evidence_refs.clone();
        for review in &goal.reviews {
            evidence.extend(review.evidence_refs.iter().cloned());
        }
        assert!(
            evidence_contains(&evidence, "tool://reviewer-read-receipt"),
            "a review's own durable read receipt must count toward the gate"
        );
    }

    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActionStatus, AgentActorBinding, AgentActorKind,
        ObjectiveCompleteRequestInput, TeamCreateInput,
    };
    use harness_contract::goal::{
        AcceptanceCriterion, GoalExecutionBinding, GoalScope, ObjectiveReviewRecord,
    };

    fn mechanical_fixture(
        historical_review: bool,
    ) -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        crate::AgentActionService,
        AgentActorBinding,
    ) {
        mechanical_fixture_with_request(historical_review, true)
    }

    fn stall_observation(program: &AgenticProgramProjection) -> RuntimeObservation {
        use harness_contract::goal::{ContextDelta, CostDelta, EvidenceDelta,
            InformationGain, ObservationFailureClass, ObservationFreshness, ObservationResultClass,
            ParallelismDelta, RuntimeObservationIdentity, RuntimeObservationKind};
        RuntimeObservation {
            identity: RuntimeObservationIdentity { workspace_id: "workspace-test".into(),
                session_id: program.session_id.clone(), turn_id: Some(program.turn_id.clone()),
                task_id: None, graph_id: program.root_execution_id.clone().unwrap(),
                goal_id: "goal:root-completion".into(), node_id: None },
            kind: RuntimeObservationKind::GraphProgress, source: "runtime.program_stall".into(),
            source_revision: program.revision, freshness: ObservationFreshness { observed_at_ms: 1,
                valid_until_ms: None, policy_revision: "goal-observation-v2".into() },
            summary: "verified budget exhausted".into(), fingerprint: "stall:fixture".into(),
            evidence_refs: vec![], observed_evidence: vec![], criterion_deltas: vec![],
            evidence_delta: EvidenceDelta::default(), effect_deltas: vec![], conflict_deltas: vec![],
            unknown_deltas: vec![], cost_delta: CostDelta::default(), information_gain: InformationGain::default(),
            context_delta: ContextDelta::default(), parallelism_delta: ParallelismDelta::default(),
            result_class: ObservationResultClass::Failed, failure_class: Some(ObservationFailureClass::Unknown),
        }
    }


    #[test]
    fn runtime_stall_does_not_close_published_work() {
        use harness_contract::agent_action::TaskPublishInput;
        let (_, goals, actions, actor) = mechanical_fixture_with_request(false, false);
        let apply = |id: &str, action| actions.apply(&AgentActionEnvelope {
            action_id: id.into(), actor: actor.clone(), expected_revision: None, action,
        }).unwrap();
        let team = apply("wait-team", AgentAction::TeamCreate(TeamCreateInput {
            name: "Waiting team".into(), mission: "Produce the original result".into(), objective: None,
        }));
        assert_eq!(team.status, AgentActionStatus::Applied);
        let task = apply("wait-task", AgentAction::TaskPublish(TaskPublishInput {
            team_ref: team.changed_refs[0].clone(), title: "Produce".into(),
            objective: "Produce the original result".into(), acceptance: "Durable result".into(),
            criterion_source: Default::default(), acceptance_checks: vec![], required_capabilities: vec![],
            depends_on: vec![], obligation_refs: vec![], purpose: Default::default(),
            execution_requirements: vec![], expertise_hints: vec![],
        }));
        assert_eq!(task.status, AgentActionStatus::Applied);
        let program = actions.project(&actor.program_id).unwrap();
        let trigger = stall_observation(&program);
        goals.record_observation(trigger.clone()).unwrap();
        assert!(goals.prepare_runtime_program_conclusion(&program, &trigger).is_err());
        assert!(actions.project(&actor.program_id).unwrap().objective_verdict.is_none());
        assert_eq!(goals.get("goal:root-completion").unwrap().unwrap().completion, GoalCompletion::Open);
    }

    #[test]
    fn runtime_stall_does_not_close_external_decision_wait() {
        let (_, goals, actions, actor) = mechanical_fixture_with_request(false, false);
        let goal = goals.get("goal:root-completion").unwrap().unwrap();
        goals.revise(&goal.id, goal.revision, goal.user_sequence + 1, "external decision pending", |goal| {
            goal.waiting = Some(harness_contract::goal::WaitDescriptor {
                reason_code: "waiting_external_decision".into(), reason: "await the user decision".into(),
                required_refs: vec!["approval:pending".into()], generation: 1,
            });
            vec!["waiting".into()]
        }).unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let trigger = stall_observation(&program);
        goals.record_observation(trigger.clone()).unwrap();
        assert!(goals.prepare_runtime_program_conclusion(&program, &trigger).is_err());
        assert!(actions.project(&actor.program_id).unwrap().objective_verdict.is_none());
        assert_eq!(goals.get("goal:root-completion").unwrap().unwrap().completion, GoalCompletion::Open);
    }


    #[test]
    fn runtime_stall_trigger_cannot_outlive_new_goal_progress() {
        let (_, goals, actions, actor) = mechanical_fixture_with_request(false, false);
        let program = actions.project(&actor.program_id).unwrap();
        let trigger = stall_observation(&program);
        goals.record_observation(trigger.clone()).unwrap();
        let mut gain = stall_observation(&program);
        gain.source = "runtime.tool_batch".into();
        gain.fingerprint = "new-proof-after-stall".into();
        gain.result_class = harness_contract::goal::ObservationResultClass::Succeeded;
        gain.failure_class = None;
        gain.evidence_delta.added = vec!["tool://later-proof".into()];
        gain.information_gain.distinguishing_evidence_refs = vec!["tool://later-proof".into()];
        gain.information_gain.provenance = harness_contract::MeasureProvenance::Observed;
        goals.record_observation(gain).unwrap();
        assert!(goals.prepare_runtime_program_conclusion(&program, &trigger).is_err());
        assert!(actions.project(&actor.program_id).unwrap().objective_verdict.is_none());
        assert_eq!(goals.get("goal:root-completion").unwrap().unwrap().completion, GoalCompletion::Open);
    }



    fn mechanical_fixture_with_request(
        historical_review: bool,
        request_completion: bool,
    ) -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        crate::AgentActionService,
        AgentActorBinding,
    ) {
        mechanical_fixture_with_ids(
            historical_review,
            request_completion,
            "objective-completion".into(),
            "program-completion".into(),
        )
    }

    fn mechanical_fixture_with_ids(
        historical_review: bool,
        request_completion: bool,
        objective_id: String,
        program_id: String,
    ) -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        crate::AgentActionService,
        AgentActorBinding,
    ) {
        let events = Arc::new(RuntimeEventStore::for_test());
        let goals = GoalStore::new(Arc::clone(&events));
        let actions = crate::AgentActionService::new(Arc::clone(&events));
        let actor = AgentActorBinding {
            original_execution: None,
            objective_id,
            program_id,
            session_id: "session-completion".into(),
            turn_id: "turn-completion".into(),
            root_execution_id: Some("root-completion".into()),
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: "Preserve original acceptance".into(),
            model_lease: "test".into(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: vec![],
            actor_id: "root-completion-actor".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        let first = actions
            .apply(&AgentActionEnvelope {
                action_id: "open".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::MessagePublish(MessagePublishInput {
                    topic_ref: format!("topic:{}", actor.program_id),
                    summary: Some("work began".into()),
                    content_ref: None,
                    refs: vec![],
                    recipients: vec![],
                    intent: None,
                    issue_dispositions: vec![],
                }),
            })
            .unwrap();
        assert_eq!(first.status, AgentActionStatus::Applied);
        if request_completion {
            let requested = actions
                .apply(&AgentActionEnvelope {
                    action_id: "request".into(),
                    actor: actor.clone(),
                    expected_revision: None,
                    action: AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                        blocked: false,
                        blocked_reason: None,
                        human_review_required: false,
                        recommended_next_actions: Vec::new(),
                        result_refs: vec!["tool://result".into()],
                        evidence_refs: vec!["tool://evidence".into()],
                        unresolved: vec![],
                    }),
                })
                .unwrap();
            assert_eq!(requested.status, AgentActionStatus::Applied);
        }
        goals
            .create(GoalContract {
                input_contract: None,
                id: "goal:root-completion".into(),
                session_id: actor.session_id.clone(),
                objective: actor.objective_summary.clone(),
                criteria: vec![AcceptanceCriterion {
                    id: "user_intent".into(),
                    statement: actor.objective_summary.clone(),
                    statement_ref: None,
                    source_refs: vec!["session_message:original".into()],
                    required_evidence: vec!["execution_graph:root-completion".into()],
                    status: AcceptanceStatus::Open,
                    waiver: None,
                }],
                constraints: vec![],
                phase: "execution".into(),
                evidence_refs: vec![],
                unresolved: vec![],
                blockers: vec![],
                scope: GoalScope::UserObjective,
                user_intent_criterion_id: Some("user_intent".into()),
                source_intent_ref: Some("session_message:original".into()),
                execution_binding: Some(GoalExecutionBinding {
                    objective_id: actor.objective_id.clone(),
                    session_id: actor.session_id.clone(),
                    turn_id: actor.turn_id.clone(),
                    root_execution_id: "root-completion".into(),
                    agentic_program_id: actor.program_id.clone(),
                }),
                spec_revision: 1,
                spec_digest: "original-spec".into(),
                review_refs: vec![],
                waiting: None,
                participation_requirement: None,
                obligations: vec![],
                recovery: None,
                terminal: None,
                completion: GoalCompletion::Open,
                revision: 1,
                user_sequence: 1,
                reviews: if historical_review {
                    vec![ObjectiveReviewRecord {
                        review_id: "review".into(),
                        criterion_ref: "user_intent".into(),
                        spec_revision: 1,
                        input_manifest_digest: "original-spec".into(),
                        result_refs: vec!["tool://result".into()],
                        evidence_refs: vec!["tool://evidence".into()],
                        reviewer_actor: "reviewer".into(),
                        reviewer_execution_id: Some("review-run".into()),
                        producer_refs: vec!["producer".into()],
                        decision: "satisfied".into(),
                        reason_ref: "tool://evidence".into(),
                        // Mechanical transaction fixture, not physical-read acceptance evidence.
                        verification: Some(harness_contract::goal::ObjectiveReviewVerification {
                            input_set: None,
                            result_source_revisions: Default::default(),
                            independence_required: true,
                            review_policy_digest: None,
                            effect_manifest_digest: None,
                            effect_source_refs: Vec::new(),
                            goal_spec_revision: Some(1),
                            goal_spec_digest: Some("original-spec".into()),
                            work_manifest_digest:
                                crate::agentic::review_evidence::work_manifest_digest(
                                    &actions.project(&actor.program_id).unwrap(),
                                ),
                            reviewer_execution_id: "review-run".into(),
                            producer_execution_ids: vec!["producer-run".into()],
                            current_review_effect_key: None,
                            reads: vec![harness_contract::goal::ObjectiveResultReadProof {
                                result_kind: harness_contract::goal::GoalResultKind::Artifact,
                                effect_observation_refs: vec![],
                                result_ref: "tool://result".into(),
                                content_ref: "artifact://fixture".into(),
                                sha256: "fixture-hash".into(),
                                bytes: 1,
                                receipt_refs: vec!["fixture:read".into()],
                            }],
                        }),
                    }]
                } else {
                    vec![]
                },
            })
            .unwrap();
        (events, goals, actions, actor)
    }

    #[tokio::test]
    async fn historical_delegated_review_without_effect_sources_cannot_verify_goal() {
        let (events, goals, actions, actor) = mechanical_fixture(true);
        let goal = goals.get("goal:root-completion").unwrap().unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let mut review = goal.reviews[0].clone();
        review.verification.as_mut().unwrap().producer_execution_ids =
            vec!["agent-run:historical".into()];
        assert!(crate::agentic::review_evidence::validate_proof_inputs(
            &events,
            &program,
            Some(&goal),
            review.verification.as_ref().unwrap(),
            crate::agentic::review_evidence::ResultContentReaders::default(),
        )
        .is_err());

    }



    #[tokio::test]
    async fn historical_root_review_without_effect_digest_cannot_hide_a_later_effect() {
        let (events, goals, actions, actor) = mechanical_fixture(true);
        // Negative historical corruption only: never used to prove a write or
        // successful review. A legacy unclassified effect must invalidate proof.
        events.append(RuntimeEventInput {
            stream_id: format!("session:{}", actor.session_id), scope: RuntimeEventScope::Session,
            kind: "tool.invocation.completed".into(), status: Some("completed".into()),
            actor: Some("conversation_runtime".into()), refs: vec![],
            payload: serde_json::json!({"tool_name":"unclassified_mutation", "test_fault":"legacy-missing-descriptor"}),
        }).unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&program).unwrap();
        assert!(prepared.goal.terminal.is_none());
        assert!(prepared
            .gaps
            .iter()
            .any(|gap| gap.starts_with("original_intent_review_required:")));

    }











    #[test]
    fn completion_reopen_resource_limit_is_explicit_and_invalid_values_do_not_fail_open() {
        assert_eq!(parse_completion_reopen_bound("7").unwrap(), 7);
        assert!(parse_completion_reopen_bound("0").is_err());
        assert!(parse_completion_reopen_bound("invalid").is_err());
        assert!(parse_completion_reopen_bound("-1").is_err());
    }

    #[test]
    fn completion_reopen_count_is_zero_without_reopen_events() {
        let store = crate::RuntimeEventStore::for_test();
        assert_eq!(
            completion_reopen_count(&store, "absent-program").unwrap(),
            0
        );
    }

    #[test]
    fn a9_disconnected_task_does_not_keep_the_program_waiting() {
        // The dispatch side recorded that the root graph was already terminal:
        // nobody will ever pick this Task up again, so it must not keep the
        // Program waiting.
        assert!(!GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Rework, false, false, true));
        assert!(!GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Published, false, false, true));
    }

    #[test]
    fn a9_task_without_a_disconnect_record_still_keeps_the_program_waiting() {
        // No disconnect record: the Task may still be dispatched, so it waits.
        assert!(GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Rework, false, false, false));
        assert!(GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Published, false, false, false));
    }

    #[test]
    fn a9_active_attempt_or_live_lease_still_keeps_the_program_waiting() {
        // Fail closed: an attempt or a lease is live work, whatever the
        // dispatch side recorded.
        assert!(GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Rework, true, false, true));
        assert!(GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Rework, false, true, true));
        assert!(GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Submitted, true, false, true));
    }

    #[test]
    fn a9_task_outside_the_unsettled_bucket_never_waits() {
        assert!(!GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Blocked, false, false, false));
        assert!(!GoalStore::task_blocks_the_program_cut(
            crate::AgenticTaskStatus::Accepted, false, false, false));
    }


    #[test]
    fn a9_waiting_bucket_all_durably_disconnected_is_the_new_trigger_shape() {
        // The only waiting Task failed, has no attempt and no live lease, and
        // the dispatch side durably recorded the root graph as terminal.
        assert!(GoalStore::waiting_tasks_are_all_durably_disconnected(
            &[("task:one", crate::AgenticTaskStatus::Rework, false, false)],
            &["task:one".to_string()]));
    }

    #[test]
    fn a9_a_stranded_task_without_a_disconnect_record_never_triggers_the_new_arm() {
        // A Task the dispatcher only counted as starved has no durable
        // disconnect record, so the new arm must not fire on it - and the
        // process-local starvation ledger is never consulted.
        assert!(!GoalStore::waiting_tasks_are_all_durably_disconnected(
            &[("task:one", crate::AgenticTaskStatus::Rework, false, false)],
            &[]));
        assert!(!GoalStore::waiting_tasks_are_all_durably_disconnected(
            &[("task:one", crate::AgenticTaskStatus::Rework, false, false)],
            &["task:other".to_string()]));
    }

    #[test]
    fn a9_a_waiting_task_with_an_attempt_or_a_lease_never_triggers_the_new_arm() {
        assert!(!GoalStore::waiting_tasks_are_all_durably_disconnected(
            &[("task:one", crate::AgenticTaskStatus::Rework, true, false)],
            &["task:one".to_string()]));
        assert!(!GoalStore::waiting_tasks_are_all_durably_disconnected(
            &[("task:one", crate::AgenticTaskStatus::Rework, false, true)],
            &["task:one".to_string()]));
        assert!(!GoalStore::waiting_tasks_are_all_durably_disconnected(
            &[("task:one", crate::AgenticTaskStatus::Submitted, false, false)],
            &["task:one".to_string()]));
    }

    #[test]
    fn a9_an_empty_waiting_bucket_never_triggers_the_new_arm() {
        assert!(!GoalStore::waiting_tasks_are_all_durably_disconnected(&[], &["task:one".to_string()]));
    }

}
