use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActionErrorObservation, AgentActionObservation,
    AgentActionStatus, AgentActorKind,
};
use harness_contract::goal::ObjectiveReviewVerification;
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError, RuntimeTransactionEventInput,
};

use super::program::{AgenticProgramProjection, AgenticTaskStatus, AgenticTopicEntryProjection};

mod authorization;
mod delegation;

#[cfg(test)]
mod native_effect_history_tests;

use authorization::{validate_transition, validate_transition_with_admission_contexts};

pub(crate) fn validate_receipt_index_record(
    event: &crate::DurableRuntimeEvent,
    source: &str,
    sequence: u64,
    revision: u64,
) -> Result<(), String> {
    if sequence == 0
        || event.stream_id != source
        || event.sequence != sequence
        || event.sequence > revision
        || (event.kind == "execution.agent_tool.intent" && event.schema_version != 1)
        || (event.kind == "execution.agent_tool.receipt"
            && crate::ExecutionCommitService::receipt_observation_sequence(event).is_none())
        || event.scope != RuntimeEventScope::ExecutionNode
        || event.actor.as_deref() != Some("governed_tool")
    {
        return Err("receipt-index structural identity mismatch".into());
    }
    let identity = event.payload["idempotency_key"]
        .as_str()
        .filter(|identity| !identity.is_empty())
        .ok_or("receipt-index structural payload identity missing")?;
    let prefix = match event.kind.as_str() {
        "execution.agent_tool.intent" => "agent-tool-intent",
        "execution.agent_tool.receipt" => "agent-tool-receipt",
        _ => return Err("receipt-index structural kind unsupported".into()),
    };
    if event.idempotency_key.as_deref() != Some(format!("{prefix}:{identity}").as_str()) {
        return Err("receipt-index structural idempotency identity mismatch".into());
    }
    Ok(())
}

#[cfg(test)]
mod receipt_index_791_tests;

fn retirement_execute_claim_matches(
    task: &super::program::AgenticTaskProjection,
    attempt: &super::program::AgenticTaskAttemptProjection,
) -> bool {
    task.claim_generation > attempt.generation
        && match (task.claim_execution_id.as_deref(), task.claimant.as_deref()) {
            (None, None) => true,
            (Some(execution), Some(agent)) => {
                execution == attempt.execution_id && agent == attempt.agent_id
            }
            _ => false,
        }
}

const ACTION_EVENT_KIND: &str = "agentic.action_applied";
/// Durable record of what the root barrier's resolver decided.
///
/// The resolver used to report only a count, so a barrier that never released could
/// not be distinguished from one that was never asked. In paired run 3 all three
/// Tasks were `accepted`, the barrier was `WaitingExternal`, its payload matched the
/// projection's program_id and root_execution_id word for word, and the resolver's own
/// cursor was level with the event stream head - yet `resolved_waits` was 0 and no
/// `agentic-program-checkpoint` command was ever issued. This record names the
/// disposition, the reason, the Task statuses the resolver actually read, and whether
/// it found a candidate barrier node, so the next run answers the question instead of
/// leaving it to inference.
pub(crate) struct AgenticWaitDisposition<'a> {
    pub program_id: &'a str,
    pub root_execution_id: &'a str,
    pub revision: u64,
    pub disposition: &'static str,
    pub reason: Option<&'static str>,
    pub all_tasks_accepted: bool,
    pub task_statuses: Vec<(String, String)>,
    pub barrier_candidates: usize,
    pub released: usize,
    pub observed_revision: u64,
    /// Every barrier node the resolver saw and why each was rejected. A zero
    /// `barrier_candidates` is otherwise unreadable: it cannot distinguish "the node
    /// was never created" from "the node is there but one of the four admission
    /// conditions was false".
    pub barrier_rejections: serde_json::Value,
}

#[cfg(test)]
thread_local! {
    static CONCLUSION_APPEND_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce(&AppendTransactionRequest)>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
pub(crate) fn with_conclusion_append_hook<Output>(
    hook: impl FnOnce(&AppendTransactionRequest) + 'static,
    commit: impl FnOnce() -> Output,
) -> Output {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            CONCLUSION_APPEND_HOOK.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    CONCLUSION_APPEND_HOOK.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(hook));
    });
    let _reset = Reset;
    commit()
}

impl AgentActionService {
    /// Append one barrier disposition record. It goes on its own stream so it never
    /// advances the semantic Program revision, and its scope is deliberately not
    /// `Program`: the resolver lane's interest is `agentic.action_applied`, and a
    /// diagnostic record must not be able to re-arm that lane.
    pub(crate) fn record_wait_disposition(&self, record: AgenticWaitDisposition<'_>) {
        if self
            .program_recovery_is_excluded(record.program_id)
            .unwrap_or(true)
        {
            return;
        }
        let stream_id = format!("agentic-program-wait:{}", record.program_id);
        // Fence the append on the stream head and retry the fence, not the write.
        // The synchronous reconcile in the tool path and the barrier's own bounded
        // self-rearm run concurrently and both record here, so a single-shot fence
        // silently drops whichever arrives second - and a dropped record is a
        // diagnosis built on an undercount. Nine fixes were derived from exactly
        // that undercount before this was noticed.
        let mut attempt = 0_u8;
        let payload = serde_json::json!({
            "program_id": record.program_id,
            "root_execution_id": record.root_execution_id,
            "revision": record.revision,
            "disposition": record.disposition,
            "reason": record.reason,
            "all_tasks_accepted": record.all_tasks_accepted,
            "task_statuses": record.task_statuses,
            "barrier_candidates": record.barrier_candidates,
            "released": record.released,
            "observed_revision": record.observed_revision,
        });
        let request = AppendTransactionRequest {
            transaction_id: format!(
                "agentic-wait-disposition:{}:{revision}",
                record.program_id,
                revision = record.revision
            ),
            expected_streams: Vec::new(),
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("agentic-program-wait:{}", record.program_id),
                    scope: RuntimeEventScope::Relation,
                    kind: "agentic.program_wait_disposition".to_string(),
                    status: Some(record.disposition.to_string()),
                    actor: Some(record.root_execution_id.to_string()),
                    refs: Vec::new(),
                    payload,
                },
                idempotency_key: None,
                schema_version: 1,
            }],
        };
        loop {
            let expected = self.store.stream_revision(&stream_id).unwrap_or_default();
            let request = AppendTransactionRequest {
                transaction_id: request.transaction_id.clone(),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: expected,
                }],
                events: request.events.clone(),
            };
            match self.store.append_transaction(request) {
                Ok(_) => return,
                Err(error) if attempt < 8 && error.to_string().contains("revision") => {
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(error) => {
                    tracing::debug!(
                        program_id = record.program_id,
                        %error,
                        "agentic barrier disposition record not appended"
                    );
                    return;
                }
            }
        }
    }

    /// Append one dispatch decline record.
    ///
    /// The dispatch machinery declines for nine different reasons and, until now,
    /// left no durable trace of any of them. A Task that sat `Submitted` with no
    /// reviewer was therefore indistinguishable from a Task that was never asked:
    /// both looked like "no review dispatch happened", and the only way to tell
    /// them apart was a log line that lives in a file the evaluation never
    /// surfaces, at a level a release build filters out. Two root causes were
    /// guessed wrong from exactly that silence.
    ///
    /// Same shape and same stream discipline as [`Self::record_wait_disposition`]:
    /// its own stream, `Relation` scope so it cannot re-arm the `Program` lane,
    /// and a fenced append that retries the fence rather than dropping the
    /// second writer.
    pub(crate) fn record_dispatch_decline(&self, record: AgenticDispatchDecline<'_>) {
        if self
            .program_recovery_is_excluded(record.program_id)
            .unwrap_or(true)
        {
            return;
        }
        let stream_id = format!("agentic-dispatch:{}", record.program_id);
        let payload = serde_json::json!({
            "program_id": record.program_id,
            "task_ref": record.task_ref,
            "mode": record.mode,
            "reason": record.reason.as_str(),
            "task_status": record.task_status,
            "eligible_members": record.eligible_members,
            "busy_members": record.busy_members,
            "consecutive_deferrals": record.consecutive_deferrals,
            "review_generation": record.review_generation,
        });
        // One record per distinct (task, mode, reason). `dispatch_ready_agentic_work`
        // runs on every worker exit, so a Program that cannot be dispatched would
        // otherwise emit the same decline forever and bury the one that mattered.
        let idempotency_key = format!(
            "{}:{}:{}:{}",
            record.task_ref,
            record.mode,
            record.reason.as_str(),
            record.task_status
        );
        let mut attempt = 0_u8;
        let request = AppendTransactionRequest {
            transaction_id: format!("agentic-dispatch-decline:{idempotency_key}"),
            expected_streams: Vec::new(),
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: RuntimeEventScope::Relation,
                    kind: DISPATCH_DECLINED_EVENT_KIND.to_string(),
                    status: Some("declined".to_string()),
                    actor: Some("runtime.agentic-dispatch".to_string()),
                    refs: Vec::new(),
                    payload,
                },
                // The deduplication the plan asks for: an unchanged reason is not
                // news, and re-appending it would advance the stream without
                // adding anything a reader could act on.
                idempotency_key: Some(idempotency_key),
                schema_version: 1,
            }],
        };
        loop {
            let expected = self.store.stream_revision(&stream_id).unwrap_or_default();
            let request = AppendTransactionRequest {
                transaction_id: request.transaction_id.clone(),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: expected,
                }],
                events: request.events.clone(),
            };
            match self.store.append_transaction(request) {
                Ok(_) => return,
                Err(error) if attempt < 8 && error.to_string().contains("revision") => {
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(error) => {
                    tracing::debug!(%error, "agentic dispatch decline record not appended");
                    return;
                }
            }
        }
    }
}

/// Why Runtime declined to dispatch one Task in one mode.
///
/// An enum rather than a formatted string: the reader is a query against
/// `runtime_events`, and matching on prose is how the previous two root causes
/// were guessed wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgenticDispatchDeclineReason {
    ProgramNotOpen,
    StarvationBackoff,
    RootGraphTerminal,
    CoordinationWakeAbsent,
    CoordinationIntentMismatch,
    CoordinationNotCurrent,
    CoordinationAlreadySettled,
    ModeStateMismatch,
    NoEligibleMember,
    NoFreeEligibleMember,
    FlightBusy,
}

impl AgenticDispatchDeclineReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProgramNotOpen => "program_not_open",
            Self::StarvationBackoff => "starvation_backoff",
            Self::RootGraphTerminal => "root_graph_terminal",
            Self::CoordinationWakeAbsent => "coordination_wake_absent",
            Self::CoordinationIntentMismatch => "coordination_intent_mismatch",
            Self::CoordinationNotCurrent => "coordination_not_current",
            Self::CoordinationAlreadySettled => "coordination_already_settled",
            Self::ModeStateMismatch => "mode_state_mismatch",
            Self::NoEligibleMember => "no_eligible_member",
            Self::NoFreeEligibleMember => "no_free_eligible_member",
            Self::FlightBusy => "flight_busy",
        }
    }
}

/// One "Runtime did not dispatch this" record.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AgenticDispatchDecline<'a> {
    pub program_id: &'a str,
    pub task_ref: &'a str,
    pub mode: &'static str,
    pub reason: AgenticDispatchDeclineReason,
    /// The Task's status at the moment of the decline. A `mode_state_mismatch` is
    /// unreadable without it: the reader needs to see which status was expected.
    pub task_status: &'a str,
    pub eligible_members: usize,
    pub busy_members: usize,
    pub consecutive_deferrals: usize,
    pub review_generation: u64,
}

/// Runtime declined to dispatch one Task in one mode. Durable for the same reason
/// `agentic.program_wait_disposition` is: a decline nobody can read is a decline
/// nobody can diagnose, and the dispatch path declines for nine reasons.
const DISPATCH_DECLINED_EVENT_KIND: &str = "agentic.dispatch.declined";
const PROGRAM_OPENED_EVENT_KIND: &str = "agentic.program_opened";
const OBJECTIVE_VERDICT_EVENT_KIND: &str = "agentic.objective_verdict_bound";
const COMPLETION_REOPENED_EVENT_KIND: &str = "agentic.completion_reopened";
/// A lightweight in-place claim-lease renewal. Unlike `agentic.action_applied`
/// (a full governed action), this only extends the existing claim's lease, so a
/// long-running Agent does not churn durable actions just to stay alive.
const TASK_LEASE_RENEWED_EVENT_KIND: &str = "agentic.task_lease_renewed";
/// A completion request rejected at *authorization* (before it is applied).
/// These never became a durable reopen, so a Program could loop requesting
/// completion forever without the bounded-convergence reopen count ever
/// seeing it. Recorded durably so that loop is still bounded.
const COMPLETION_REJECTED_EVENT_KIND: &str = "agentic.completion_rejected";
/// A reviewer's rework refused for resting on a reading the operator ruled out.
///
/// Durable for the same reason `agentic.completion_rejected` is: without a record,
/// a reviewer can send the same Task back on the same ruled-out reading once per
/// attempt, forever, and nothing in the Program ever shows it happened. The event
/// is folded into `program_warnings`, which is the field the root model and the
/// monitoring layer already read.
const REVIEW_DISCIPLINE_CONFLICT_EVENT_KIND: &str = "agentic.review_discipline_conflict";
const TOPIC_CURSOR_EVENT_KIND: &str = "agentic.topic_observation_acknowledged";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgenticTopicObservationPage {
    pub entries: Vec<AgenticTopicObservation>,
    pub from_revision: u64,
    pub to_revision: u64,
    pub cursor_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct AgenticTopicObservation {
    pub topic_ref: String,
    #[serde(flatten)]
    pub entry: AgenticTopicEntryProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TopicObservationKind {
    ProviderModel,
    WorkerTransport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgenticTopicObservationAck {
    pub program_id: String,
    pub execution_id: String,
    pub through_revision: u64,
    pub expected_cursor_revision: u64,
    pub observation_kind: TopicObservationKind,
}

#[derive(Debug, Error)]
pub enum AgentActionServiceError {
    #[error(transparent)]
    Store(#[from] RuntimeEventStoreError),
    #[error("agent action event store failed: {0}")]
    EventStore(String),
    #[error("agent action serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("agent action journal is corrupt: {0}")]
    Corrupt(String),
}

#[derive(Clone)]
pub struct AgentActionService {
    store: Arc<RuntimeEventStore>,
    artifacts: Option<Arc<crate::ArtifactStore>>,
    process_input_verifier: Option<crate::agentic::review_evidence::ProcessInputVerifier>,
    proof_session_queries: Option<Arc<OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>>,
    graphs: Option<crate::ExecutionGraphStateStore>,
    read_model: Arc<super::AgenticReadModel>,
}

struct RuntimeRetirementAdmission {
    request_digest: String,
    program_id: String,
    program_revision: u64,
    sources: Vec<ExpectedStreamRevision>,
}

impl RuntimeRetirementAdmission {
    fn covers(
        &self,
        projection: &AgenticProgramProjection,
        envelope: &AgentActionEnvelope,
    ) -> bool {
        self.program_revision == projection.revision
            && self.program_id == projection.program_id
            && projection.program_id == envelope.actor.program_id
            && envelope.actor.kind == AgentActorKind::Supervisor
            && envelope.actor.actor_id == "runtime.program-supervisor"
            && self.request_digest == super::review_evidence::proof_value_digest(envelope)
            && matches!(&envelope.action, AgentAction::TaskAttemptFail(_))
    }
}

impl AgentActionService {
    fn validate_native_retirement_target_guard(
        original: &harness_contract::execution_graph::ExecutionGraph,
        child: &harness_contract::execution_graph::ExecutionGraph,
        guard: &harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<(), String> {
        use crate::execution_core::graph::{ExecutionCompileRequest, ExecutionGraphCompiler};
        use crate::execution_core::RuntimeCompileTarget;
        use harness_contract::execution_graph::ExecutionNodeKind;
        if original.id != child.id
            || original.parent_execution.is_none()
            || original.lineage.is_none()
            || original.parent_execution != child.parent_execution
            || original.lineage != child.lineage
            || original.service_class != child.service_class
            || !original.nodes.iter().any(|node| node == guard)
            || !child.nodes.iter().any(|node| node == guard)
        {
            return Err("runtime retirement Native target guard owner changed".into());
        }
        let base = original
            .nodes
            .iter()
            .find(|node| {
                node.id == format!("{}:model", original.id)
                    && node.kind == ExecutionNodeKind::InlineModel
                    && node.executor_kind == "inline_model"
            })
            .ok_or("runtime retirement Native target guard original model missing")?;
        if guard.payload_ref != base.payload_ref {
            return Err("runtime retirement Native target guard payload changed".into());
        }
        let initial: serde_json::Value =
            serde_json::from_str(&base.payload_ref).map_err(|error| error.to_string())?;
        let target: RuntimeCompileTarget =
            serde_json::from_value(initial["compile_target"].clone())
                .map_err(|error| error.to_string())?;
        if !matches!(
            target,
            RuntimeCompileTarget::EvidenceGraph | RuntimeCompileTarget::ExecutionGraph
        ) {
            return Err("runtime retirement Native target has no compiler guard".into());
        }
        let scopes = original
            .nodes
            .iter()
            .find(|node| node.id == format!("{}:resource-constraint", original.id))
            .map(|node| node.resource_scopes.clone())
            .unwrap_or_default();
        let compiled = ExecutionGraphCompiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: original.objective.clone(),
                payload_ref: base.payload_ref.clone(),
                target,
                resource_scopes: scopes,
            })
            .map_err(|error| error.to_string())?;
        let prefix = format!("{}:", original.id);
        let suffix = guard
            .id
            .strip_prefix(&prefix)
            .ok_or("runtime retirement Native target guard has a foreign node owner")?;
        let mut expected = compiled
            .nodes
            .iter()
            .find(|node| {
                node.id == format!("{}:{suffix}", compiled.id)
                    && node.kind == ExecutionNodeKind::Verify
            })
            .cloned()
            .ok_or("runtime retirement Native target guard is not compiler-authored")?;
        expected.id.clone_from(&guard.id);
        expected.idempotency_key = format!("{}:attempt", guard.id);
        if expected != *guard {
            return Err("runtime retirement Native target guard shape/criterion mismatch".into());
        }
        Ok(())
    }

    fn validate_retired_native_child(
        &self,
        child: &harness_contract::execution_graph::ExecutionGraph,
        packet: &harness_contract::agent::AgentTaskPacket,
        pin: &mut impl FnMut(&str) -> Result<u64, String>,
    ) -> Result<(), String> {
        use crate::execution_core::graph::{
            ExecutionCommitService, ExecutionGraphStateStore, ExecutionNodeBinding,
        };
        use harness_contract::agent::AgentExecutorPolicy;
        use harness_contract::execution_graph::{
            ExecutionNodeKind, ExecutionNodeStatus, ExecutionServiceClass,
        };
        let compiled = packet
            .binding
            .as_ref()
            .ok_or("runtime retirement Native binding missing")?;
        if compiled.executor != AgentExecutorPolicy::CowdNative {
            return Err("runtime retirement non-Native conversation child".into());
        }
        if !matches!(
            child.service_class,
            ExecutionServiceClass::Foreground | ExecutionServiceClass::Background
        ) || child.nodes.is_empty()
        {
            return Err("runtime retirement Native child service class/shape mismatch".into());
        }
        let turn_id = packet
            .assignment
            .execution_identity
            .turn_id()
            .ok_or("runtime retirement Native original turn missing")?;
        let (_, exit_sources) = crate::agent_runtime::confirmed_run_exit(&self.store, packet)?
            .ok_or("runtime retirement Native child has no exact exit")?;
        for source in exit_sources {
            if pin(&source.stream_id)? != source.expected_revision {
                return Err("runtime retirement Native lifecycle changed".into());
            }
        }
        let agent_stream = format!("agent:{}", packet.agent_id());
        let backend = self
            .store
            .event_by_idempotency_key(
                &agent_stream,
                &format!("agent.backend_admitted:{}", packet.run_id()),
            )
            .map_err(|error| error.to_string())?
            .ok_or("runtime retirement Native child has no original backend admission")?;
        let exit = self
            .store
            .event_by_idempotency_key(
                &agent_stream,
                &format!("agent.local_exit:{}", packet.run_id()),
            )
            .map_err(|error| error.to_string())?
            .ok_or("runtime retirement Native child exit missing")?;
        let genesis = self
            .store
            .list_stream_after(&child.id, 0, 1, 1, 4 * 1024 * 1024)?;
        if genesis.first().is_none_or(|event| {
            event.commit_cursor <= backend.commit_cursor
                || event.commit_cursor >= exit.commit_cursor
        }) {
            return Err(
                "runtime retirement Native child is outside original backend lifetime".into(),
            );
        }
        let original = self.store.resolve_graph_record(&genesis[0])?;
        let original: crate::execution_core::graph::ExecutionGraphEvent =
            serde_json::from_value(original).map_err(|error| error.to_string())?;
        let crate::execution_core::graph::ExecutionGraphEvent::Planned { graph: original } =
            original
        else {
            return Err("runtime retirement Native original graph plan missing".into());
        };
        if original.id != child.id
            || original.service_class != child.service_class
            || original.lineage != child.lineage
            || original.parent_execution != child.parent_execution
        {
            return Err("runtime retirement Native original graph ownership changed".into());
        }
        let base = child
            .nodes
            .iter()
            .find(|node| node.id == format!("{}:model", child.id))
            .ok_or("runtime retirement Native original model missing")?;
        if !original.nodes.iter().any(|node| node == base) {
            return Err("runtime retirement Native original model changed".into());
        }
        let initial: serde_json::Value =
            serde_json::from_str(&base.payload_ref).map_err(|error| error.to_string())?;
        if base.kind != ExecutionNodeKind::InlineModel
            || base.executor_kind != "inline_model"
            || initial["kind"] != "conversation_turn"
            || initial["session_id"] != packet.session_id()
            || initial["objective"] != child.objective
        {
            return Err("runtime retirement Native conversation origin mismatch".into());
        }
        let commits = ExecutionCommitService::new(Arc::clone(&self.store));
        let state = ExecutionGraphStateStore::new(Arc::clone(&self.store));
        let mut effect_streams = std::collections::BTreeSet::new();
        for child_node in &child.nodes {
            let status = child
                .node_statuses
                .get(&child_node.id)
                .filter(|status| status.is_terminal())
                .ok_or("runtime retirement Native child node is not terminal")?;
            let expected_executor = match child_node.kind {
                ExecutionNodeKind::InlineModel => "inline_model",
                ExecutionNodeKind::Verify => {
                    Self::validate_native_retirement_target_guard(&original, child, child_node)?;
                    crate::execution_core::graph::executors::CompileTargetGuardExecutor::KIND
                }
                ExecutionNodeKind::ToolBatch => "tool_batch",
                ExecutionNodeKind::Synthesize => "synthesize",
                _ => return Err("runtime retirement Native child has unmodelled node kind".into()),
            };
            if child_node.executor_kind != expected_executor
                || !child_node.id.starts_with(&format!("{}:", child.id))
            {
                return Err("runtime retirement Native child executor/identity mismatch".into());
            }
            if child_node.kind == ExecutionNodeKind::ToolBatch {
                let payload = crate::step_snapshot::decode_persisted_tool_batch_value(
                    &child_node.payload_ref,
                )
                .map_err(|error| error.to_string())?;
                let steps = payload["step_snapshots"]
                    .as_object()
                    .filter(|steps| !steps.is_empty())
                    .ok_or("runtime retirement Native tool Steps missing")?;
                for step in steps.values() {
                    let identity = &step["identity"];
                    if identity["session_id"] != packet.session_id()
                        || identity["turn_id"] != turn_id
                        || identity["execution_id"] != packet.run_id()
                        || identity["run_ref"] != packet.run_id()
                        || identity["task_ref"] != packet.task_id()
                    {
                        return Err(
                            "runtime retirement Native tool Step has a foreign owner".into()
                        );
                    }
                }
                continue;
            }
            if child_node.idempotency_key != format!("{}:attempt", child_node.id) {
                return Err("runtime retirement Native effect key mismatch".into());
            }
            let attempt = child
                .recovery_cursor
                .node_attempts
                .get(&child_node.id)
                .copied()
                .unwrap_or(0);
            if attempt == 0 {
                if *status == ExecutionNodeStatus::Cancelled {
                    continue;
                }
                return Err("runtime retirement Native child has no original attempt".into());
            }
            let binding = state
                .current_binding(child, &child_node.id, attempt)
                .map_err(|error| error.to_string())?;
            let (ticket, original_spec) = state
                .binding_start_ticket(child, &child_node.id, &binding)
                .map_err(|error| error.to_string())?;
            if original_spec != *child_node {
                return Err("runtime retirement Native current attempt spec changed".into());
            }
            let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
            effect_streams.insert(stream);
        }
        self.validate_native_effect_history(child, &commits, effect_streams, pin)?;
        Ok(())
    }

    fn validate_native_effect_history(
        &self,
        child: &harness_contract::execution_graph::ExecutionGraph,
        commits: &crate::execution_core::graph::ExecutionCommitService,
        mut effect_streams: std::collections::BTreeSet<String>,
        pin: &mut impl FnMut(&str) -> Result<u64, String>,
    ) -> Result<(), String> {
        use crate::execution_core::graph::{
            ExecutionEffectState, ExecutionGraphStateStore, ExecutionNodeBinding,
            NodeExecutionTicket,
        };
        use harness_contract::execution_graph::ExecutionNodeKind;
        let state = ExecutionGraphStateStore::new(Arc::clone(&self.store));
        let mut after = 0;
        let mut historical_tickets = BTreeMap::new();
        let mut expected_records = 0usize;
        while after < child.revision {
            let (next, starts) = state
                .binding_starts_page(&child.id, after, child.revision)
                .map_err(|error| error.to_string())?;
            if next <= after {
                return Err("runtime retirement Native start history did not advance".into());
            }
            for (node_id, binding, position) in starts {
                let node =
                    child.nodes.iter().find(|node| node.id == node_id).ok_or(
                        "runtime retirement Native child has an unmodelled historical node",
                    )?;
                let (ticket, original_spec) = state
                    .binding_start_ticket(child, &node_id, &binding)
                    .map_err(|error| error.to_string())?;
                if original_spec != *node {
                    return Err("runtime retirement Native historical spec changed".into());
                }
                let completed = state
                    .committed_binding_result(child, &node_id, &binding)
                    .map_err(|error| error.to_string())?;
                let retirement = state
                    .binding_retirement_record(child, &node_id, &binding)
                    .map_err(|error| error.to_string())?;
                if completed.is_none() && retirement.is_none() {
                    return Err("runtime retirement Native historical attempt is unfinished".into());
                }
                if !state
                    .binding_lease_closure(child, &node_id, &binding, position)
                    .map_err(|error| error.to_string())?
                {
                    return Err(
                        "runtime retirement Native historical callback or physical closure missing"
                            .into(),
                    );
                }
                if node.kind == ExecutionNodeKind::ToolBatch {
                    if let Some(sources) = commits
                        .authenticated_no_dispatch_sources(child, &node_id, &binding)
                        .map_err(|error| error.to_string())?
                    {
                        for source in sources {
                            if pin(&source.stream_id)? != source.expected_revision {
                                return Err(
                                    "runtime retirement Native NoDispatch physical source changed"
                                        .into(),
                                );
                            }
                        }
                    }
                    continue;
                }
                let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
                if historical_tickets.contains_key(&stream) {
                    return Err("runtime retirement Native duplicate historical effect".into());
                }
                let records = if let Some((result, finished)) = completed {
                    match commits
                        .inspect_execution_effect(&ticket)
                        .map_err(|error| error.to_string())?
                    {
                        ExecutionEffectState::Completed(outcome) if outcome.result == result => {}
                        _ => {
                            return Err(
                                "runtime retirement Native historical result conflict".into()
                            )
                        }
                    }
                    let receipt = self
                        .store
                        .event_by_idempotency_key(
                            &stream,
                            &format!("{}:receipt", ticket.idempotency_key),
                        )
                        .map_err(|error| error.to_string())?
                        .ok_or("runtime retirement Native historical receipt missing")?;
                    if receipt.transaction_id != finished.transaction_id
                        || receipt.commit_cursor != finished.commit_cursor
                    {
                        return Err(
                            "runtime retirement Native historical result is not atomic".into()
                        );
                    }
                    2u64
                } else {
                    if !matches!(
                        commits
                            .inspect_execution_effect(&ticket)
                            .map_err(|error| error.to_string())?,
                        ExecutionEffectState::Uncertain
                    ) {
                        return Err("runtime retirement Native retired attempt has no exact original intent".into());
                    }
                    1u64
                };
                if pin(&stream)? != records {
                    return Err(
                        "runtime retirement Native effect head conflicts with original disposition"
                            .into(),
                    );
                }
                expected_records = expected_records
                    .checked_add(usize::try_from(records).map_err(|error| error.to_string())?)
                    .ok_or("runtime retirement Native effect count overflow")?;
                effect_streams.insert(stream.clone());
                historical_tickets.insert(stream, ticket);
            }
            after = next;
        }
        if effect_streams.len() != historical_tickets.len() {
            return Err("runtime retirement Native effect has no canonical start".into());
        }
        let prefix = ExecutionNodeBinding::effect_stream_prefix(&child.id);
        let max_records = expected_records;
        let mut position = None;
        let mut observed = 0usize;
        loop {
            let page = self.store.list_scope_stream_prefix_page_asc(
                RuntimeEventScope::ExecutionNode,
                &prefix,
                position,
                16,
            )?;
            if page.is_empty() {
                break;
            }
            if page.len() > 16 {
                return Err("runtime retirement Native effect page overflow".into());
            }
            for record in page {
                let next = (record.commit_cursor, record.transaction_index);
                if record.commit_cursor == 0
                    || position.is_some_and(|previous| next <= previous)
                    || record.scope != RuntimeEventScope::ExecutionNode
                    || !record.stream_id.starts_with(&prefix)
                {
                    return Err("runtime retirement Native effect page source mismatch".into());
                }
                position = Some(next);
                let ticket = historical_tickets
                    .get(&record.stream_id)
                    .ok_or("runtime retirement Native child has an unmodelled historical effect")?;
                let (suffix, status, sequence) = match record.kind.as_str() {
                    "execution.effect.intent" => ("intent", "inflight", 1),
                    "execution.effect.receipt" => ("receipt", "completed", 2),
                    _ => return Err("runtime retirement Native effect kind mismatch".into()),
                };
                if record.schema_version != 1
                    || record.sequence != sequence
                    || record.actor.as_deref() != Some(ticket.executor_kind.as_str())
                    || record.status.as_deref() != Some(status)
                    || record.idempotency_key.as_deref()
                        != Some(format!("{}:{suffix}", ticket.idempotency_key).as_str())
                {
                    return Err("runtime retirement Native effect identity mismatch".into());
                }
                if suffix == "intent" {
                    let original: NodeExecutionTicket = serde_json::from_value(record.payload)
                        .map_err(|error| error.to_string())?;
                    if original != *ticket
                        || ExecutionNodeBinding::effect_stream_id(&original) != record.stream_id
                    {
                        return Err("runtime retirement Native effect ticket mismatch".into());
                    }
                }
                observed += 1;
                if observed > max_records {
                    return Err(
                        "runtime retirement Native effect history exceeds original protocol".into(),
                    );
                }
            }
        }
        if observed != max_records {
            return Err("runtime retirement Native effect history is incomplete".into());
        }
        Ok(())
    }

    fn runtime_retirement_admission(
        &self,
        projection: &AgenticProgramProjection,
        envelope: &AgentActionEnvelope,
    ) -> Result<Option<RuntimeRetirementAdmission>, String> {
        use harness_contract::agent::{AgentTaskPacket, AgenticExecutionFocus};
        use harness_contract::agent_action::AgentAttemptMode;
        use harness_contract::execution_graph::ExecutionNodeKind;
        let AgentAction::TaskAttemptFail(input) = &envelope.action else {
            return Ok(None);
        };
        let Some(task) = projection.tasks.get(&input.task_ref) else {
            return Ok(None);
        };
        if task.status != AgenticTaskStatus::CancelRequested
            || task.pending_retirement != Some(super::program::AgenticTaskRetirement::Withdrawn)
            || envelope.actor.kind != AgentActorKind::Supervisor
        {
            return Ok(None);
        }
        if envelope.actor.actor_id != "runtime.program-supervisor"
            || envelope.actor.execution_id.as_deref() != Some(input.execution_id.as_str())
            || envelope.actor.agent_id.is_some()
            || envelope.actor.team_id.is_some()
            || task
                .cancel_requested_by
                .as_ref()
                .is_none_or(|value| value.is_empty())
            || task
                .cancel_reason_ref
                .as_ref()
                .is_none_or(|value| value.is_empty())
            || task.cancel_evidence_refs.is_empty()
        {
            return Err("runtime retirement actor or withdrawal binding mismatch".into());
        }
        let attempt = task
            .active_attempts
            .get(&input.execution_id)
            .ok_or("runtime retirement has no registered attempt")?;
        if attempt.mode != input.mode || attempt.execution_id != input.execution_id {
            return Err("runtime retirement registered mode mismatch".into());
        }
        let graphs = crate::ExecutionGraphStateStore::new(Arc::clone(&self.store));
        let graph = graphs
            .load(&input.execution_id)
            .map_err(|error| error.to_string())?;
        let mut nodes = graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask);
        let node = nodes
            .next()
            .ok_or("runtime retirement Agent node missing")?;
        if nodes.next().is_some() {
            return Err("runtime retirement Agent node ambiguous".into());
        }
        let packet: AgentTaskPacket =
            serde_json::from_str(&node.payload_ref).map_err(|error| error.to_string())?;
        let binding = packet
            .agentic_binding
            .as_ref()
            .ok_or("runtime retirement Program binding missing")?;
        let focus_matches = match (&binding.focus, input.mode) {
            (AgenticExecutionFocus::TaskExecute { task_ref }, AgentAttemptMode::Execute) => {
                task_ref == &input.task_ref
                    && retirement_execute_claim_matches(task, attempt)
            }
            (AgenticExecutionFocus::TaskReview { task_ref }, AgentAttemptMode::Review) => {
                task_ref == &input.task_ref && task.review_generation == attempt.generation
            }
            _ => false,
        };
        if !focus_matches
            || packet.graph_id() != graph.id
            || graph.id != input.execution_id
            || packet.node_id() != node.id
            || packet.session_id() != projection.session_id
            || packet.attempt == 0
            || u64::from(packet.attempt) != attempt.generation.saturating_add(1)
            || binding.program_id != projection.program_id
            || binding.agent_id != attempt.agent_id
            || binding.membership_id != attempt.membership_id
            || binding.task_team_id != task.team_id
            || !projection
                .membership_for(&binding.agent_id, &binding.team_id)
                .is_some_and(|member| member.membership_id == binding.membership_id)
            || packet.binding.as_ref().is_none_or(|compiled| {
                !crate::agent::binding::recompute_binding_digest(compiled)
                    .is_ok_and(|digest| digest == compiled.binding_digest)
            })
            || graph.lineage.as_ref().is_none_or(|lineage| {
                lineage.session_id != projection.session_id
                    || lineage.turn_id != projection.turn_id
                    || lineage.task_id != input.task_ref
            })
            || graph.recovery_cursor.node_attempts.get(&node.id) != Some(&packet.attempt)
        {
            return Err("runtime retirement immutable attempt binding mismatch".into());
        }
        let (_, exit_sources) = crate::agent_runtime::confirmed_run_exit(&self.store, &packet)?
            .ok_or("runtime retirement has no authenticated exact local backend exit")?;
        let mut sources = std::collections::BTreeMap::new();
        for source in exit_sources {
            if sources
                .insert(source.stream_id, source.expected_revision)
                .is_some_and(|previous| previous != source.expected_revision)
            {
                return Err("runtime retirement local exit source changed".into());
            }
        }
        if matches!(
            packet.binding.as_ref().map(|binding| &binding.executor),
            Some(harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. })
        ) {
            let lifecycle =
                crate::agent_runtime::authenticated_run_lifecycle(&self.store, &packet)?;
            if !lifecycle.no_direct_effects() {
                return Err(
                    "effect_review_pending: reaped Process retains opaque past effects".into(),
                );
            }
            for source in lifecycle.sources {
                if sources
                    .insert(source.stream_id, source.expected_revision)
                    .is_some_and(|previous| previous != source.expected_revision)
                {
                    return Err("runtime retirement physical envelope source changed".into());
                }
            }
        }
        let mut process_sources = std::collections::BTreeMap::new();
        let mut pin = |source: &str| -> Result<u64, String> {
            let revision = self
                .store
                .stream_revision(source)
                .map_err(|error| error.to_string())?;
            if sources
                .insert(source.to_string(), revision)
                .is_some_and(|previous| previous != revision)
            {
                return Err("runtime retirement source changed during admission".into());
            }
            Ok(revision)
        };
        let mut allowed_programs =
            std::collections::BTreeSet::from([projection.program_id.clone()]);
        let mut roots = projection
            .root_execution_id
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut continuation = projection.continuation.clone();
        while let Some(binding) = continuation {
            if !allowed_programs.insert(binding.source_program_id.clone()) {
                return Err("runtime retirement has cyclic Program provenance".into());
            }
            let origin = self
                .project(&binding.source_program_id)
                .map_err(|error| error.to_string())?;
            if origin.session_id != projection.session_id
                || origin.objective_id != binding.source_objective_id
                || origin.root_execution_id.as_deref() != Some(binding.source_root_id.as_str())
                || pin(&format!("agentic-program:{}", origin.program_id))? != origin.revision
            {
                return Err("runtime retirement historical Program binding changed".into());
            }
            roots.push(binding.source_root_id);
            continuation = origin.continuation;
        }
        if roots.is_empty() {
            return Err("runtime retirement physical history has no authenticated root".into());
        }
        let discovery_cursor = self
            .store
            .durable_commit_cursor()
            .map_err(|error| error.to_string())?;
        let mut pending = roots
            .into_iter()
            .map(|root| (root, None, false))
            .collect::<Vec<_>>();
        let mut parents = std::collections::BTreeMap::new();
        while let Some((graph_id, expected_parent, inherited_relevance)) = pending.pop() {
            if let Some(previous) = parents.insert(graph_id.clone(), expected_parent.clone()) {
                if previous != expected_parent {
                    return Err(
                        "runtime retirement physical lineage has ambiguous ownership".into(),
                    );
                }
                continue;
            }
            let historical = graphs.load(&graph_id).map_err(|error| error.to_string())?;
            let genesis = self
                .store
                .list_stream_after(&graph_id, 0, 1, 1, 4 * 1024 * 1024)?;
            if genesis
                .first()
                .is_none_or(|event| event.commit_cursor > discovery_cursor)
                || historical.parent_execution != expected_parent
                || historical
                    .lineage
                    .as_ref()
                    .is_none_or(|lineage| lineage.session_id != projection.session_id)
                || pin(&graph_id)? != historical.revision
            {
                return Err("runtime retirement physical history changed during discovery".into());
            }
            pin(&format!("execution-lineage:{graph_id}"))?;
            let mut relevant_graph = inherited_relevance;
            for historical_node in &historical.nodes {
                if historical_node.kind != ExecutionNodeKind::AgentTask {
                    continue;
                }
                let original: AgentTaskPacket = serde_json::from_str(&historical_node.payload_ref)
                    .map_err(|error| error.to_string())?;
                let relevant_packet = inherited_relevance
                    || original.agentic_binding.as_ref().is_none_or(|binding| {
                        !allowed_programs.contains(&binding.program_id)
                            || (binding.program_id == projection.program_id
                                && match &binding.focus {
                                    AgenticExecutionFocus::TaskExecute { task_ref }
                                    | AgenticExecutionFocus::TaskReview { task_ref } => {
                                        task_ref == &input.task_ref
                                    }
                                    AgenticExecutionFocus::Coordination { wake_ref } => projection
                                        .coordination_wake_ref(wake_ref)
                                        .and_then(|(_, wake)| wake.intent.as_ref())
                                        .is_none_or(|intent| intent.task_ref == input.task_ref),
                                    _ => true,
                                })
                    });
                relevant_graph |= relevant_packet;
                if original.binding.as_ref().is_some_and(|binding| {
                    binding.executor == harness_contract::agent::AgentExecutorPolicy::CowdNative
                }) {
                    continue;
                }
                let lifecycle =
                    crate::agent_runtime::authenticated_run_lifecycle(&self.store, &original)?;
                for source in &lifecycle.sources {
                    if pin(&source.stream_id)? != source.expected_revision {
                        return Err("runtime retirement historical Agent cut changed".into());
                    }
                }
                if original.graph_id() != graph_id
                    || original.node_id() != historical_node.id
                    || original
                        .agentic_binding
                        .as_ref()
                        .is_none_or(|binding| !allowed_programs.contains(&binding.program_id))
                    || (relevant_packet
                        && (historical
                            .recovery_cursor
                            .node_attempts
                            .get(&historical_node.id)
                            .copied()
                            .unwrap_or(original.attempt)
                            > original.attempt
                            || !lifecycle.no_direct_effects()))
                {
                    return Err(
                        "effect_review_pending: opaque Process history remains after physical reap"
                            .into(),
                    );
                }
            }
            for link in graphs
                .child_links(&graph_id)
                .map_err(|error| error.to_string())?
            {
                if link.parent_execution_id != graph_id
                    || link.child_execution_id == graph_id
                    || !historical
                        .nodes
                        .iter()
                        .any(|node| node.id == link.parent_node_id)
                {
                    return Err("runtime retirement historical child binding invalid".into());
                }
                pending.push((
                    link.child_execution_id,
                    Some(harness_contract::execution_graph::ExecutionParentBinding {
                        execution_id: graph_id.clone(),
                        node_id: link.parent_node_id,
                    }),
                    relevant_graph,
                ));
            }
        }
        if pin(&graph.id)? != graph.revision {
            return Err("runtime retirement graph changed during admission".into());
        }
        if graph.nodes.is_empty()
            || graph.nodes.iter().any(|node| {
                graph
                    .node_statuses
                    .get(&node.id)
                    .is_none_or(|status| !status.is_terminal())
            })
        {
            return Err("runtime retirement physical graph is not terminal".into());
        }
        pin(&format!("execution-lineage:{}", graph.id))?;
        let children = graphs
            .child_links(&graph.id)
            .map_err(|error| error.to_string())?;
        for link in children {
            if link.parent_execution_id != graph.id || link.parent_node_id != node.id {
                return Err("runtime retirement child has a foreign parent".into());
            }
            let child = graphs
                .load(&link.child_execution_id)
                .map_err(|error| error.to_string())?;
            if pin(&child.id)? != child.revision
                || child.lineage != graph.lineage
                || child.parent_execution.as_ref().is_none_or(|parent| {
                    parent.execution_id != graph.id || parent.node_id != node.id
                })
                || child.nodes.iter().any(|node| {
                    child
                        .node_statuses
                        .get(&node.id)
                        .is_none_or(|status| !status.is_terminal())
                })
            {
                return Err(
                    "runtime retirement child is not an authenticated terminal graph".into(),
                );
            }
            pin(&format!("execution-lineage:{}", child.id))?;
            if !graphs
                .child_links(&child.id)
                .map_err(|error| error.to_string())?
                .is_empty()
            {
                return Err("runtime retirement tool child has unmodelled descendants".into());
            }
            if !crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(
                &child,
            ) {
                self.validate_retired_native_child(&child, &packet, &mut pin)?;
                continue;
            }
            if child.service_class != graph.service_class {
                return Err("runtime retirement tool child service class mismatch".into());
            }
            let bound =
                crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_graph(
                    &child,
                )
                .map_err(|error| error.to_string())?;
            let request = bound.request;
            if bound.agent_id != packet.agent_id()
                || bound.run_id != packet.run_id()
                || request.parent_execution.as_ref().is_none_or(|parent| {
                    parent.execution_id != graph.id || parent.node_id != node.id
                })
                || request.parent_execution_attempt != Some(packet.attempt)
                || request.session_id.as_deref() != Some(projection.session_id.as_str())
                || request.governed_plan_id != graph.id
                || !packet.allowed_tools.contains(&request.tool_name)
                || !packet.binding.as_ref().is_some_and(|compiled| {
                    compiled.tool_contract_refs.contains(&request.tool_name)
                })
            {
                return Err("runtime retirement tool child request mismatch".into());
            }
            let tool_node = &child.nodes[0];
            let attempt = child
                .recovery_cursor
                .node_attempts
                .get(&tool_node.id)
                .copied()
                .ok_or("runtime retirement NoDispatch tool original attempt missing")?;
            let binding = graphs
                .current_binding(&child, &tool_node.id, attempt)
                .map_err(|error| error.to_string())?;
            let proof_commits =
                crate::execution_core::graph::ExecutionCommitService::new(self.store.clone());
            if let Some(sources) = proof_commits
                .authenticated_no_dispatch_sources(&child, &tool_node.id, &binding)
                .map_err(|error| error.to_string())?
            {
                for source in sources {
                    if pin(&source.stream_id)? != source.expected_revision {
                        return Err("runtime retirement bound NoDispatch source changed".into());
                    }
                }
                continue;
            }
            let primary_source = format!("execution-effect:{}", request.idempotency_key);
            let index_source = format!(
                "execution-agent-receipts:{}:{}:{}",
                graph.id, node.id, packet.attempt
            );
            pin(&primary_source)?;
            pin(&index_source)?;
            let intent = self
                .store
                .event_by_idempotency_key(
                    &primary_source,
                    &format!("{}:intent", request.idempotency_key),
                )
                .map_err(|error| error.to_string())?;
            let read = intent.is_none();
            let primary = if let Some(intent) = intent {
                intent
            } else {
                self.store
                    .event_by_idempotency_key(
                        &primary_source,
                        &format!("{}:read-receipt", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("runtime retirement tool read receipt missing")?
            };
            let index = self
                .store
                .event_by_idempotency_key(
                    &index_source,
                    &format!(
                        "agent-tool-{}:{}",
                        if read { "receipt" } else { "intent" },
                        request.idempotency_key
                    ),
                )
                .map_err(|error| error.to_string())?
                .ok_or("runtime retirement original tool index missing")?;
            crate::execution_core::graph::ExecutionCommitService::validate_proof_bound_tool_request(&request, &primary, &index)
                .map_err(|error| error.to_string())?;
            let descriptor_value = if read {
                &primary.payload["replay"]["invocation"]["effect"]
            } else {
                &primary.payload["effect"]
            };
            let descriptor: harness_contract::tool::ToolEffectDescriptor =
                serde_json::from_value(descriptor_value.clone())
                    .map_err(|_| "runtime retirement original typed descriptor missing")?;
            if primary.payload["resolved_effect_scope"] != index.payload["resolved_effect_scope"] {
                return Err("runtime retirement original primary/index scope conflict".into());
            }
            if read && primary.payload["resolved_effect_scope"].is_null() {
                let seal = crate::context_evidence::decode_read_acquisition(
                    &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                )
                .map_err(str::to_string)?;
                if !matches!(
                    seal.body.extent,
                    crate::context_evidence::ReadExtent::ArtifactChunks { .. }
                ) || descriptor.effect_kind != harness_contract::tool::ToolEffectKind::Read
                    || descriptor.scopes.iter().any(|scope| {
                        scope.resource == harness_contract::policy::PermissionResource::File
                    })
                    || descriptor.uses_network
                    || descriptor.spawns_process
                    || descriptor.mutates_packages
                    || descriptor.mutates_system
                {
                    return Err("runtime retirement read lacks original physical scope v2".into());
                }
            } else {
                super::review_evidence::validate_original_effect_scope(
                    &primary.payload["resolved_effect_scope"],
                    &descriptor,
                )?;
            }
            if primary.commit_cursor != index.commit_cursor {
                return Err(
                    "runtime retirement tool provenance was not atomically admitted".into(),
                );
            }
            if !read {
                let receipt = self
                    .store
                    .event_by_idempotency_key(
                        &primary_source,
                        &format!("{}:receipt", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("runtime retirement physical effect is still pending")?;
                let index_receipt = self
                    .store
                    .event_by_idempotency_key(
                        &index_source,
                        &format!("agent-tool-receipt:{}", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("runtime retirement effect index is still pending")?;
                if receipt.kind != "execution.effect.receipt"
                    || receipt.actor.as_deref() != Some("governed_tool")
                    || index_receipt.kind != "execution.agent_tool.receipt"
                    || index_receipt.actor.as_deref() != Some("governed_tool")
                    || receipt.commit_cursor != index_receipt.commit_cursor
                    || receipt.payload["idempotency_key"] != request.idempotency_key
                    || index_receipt.payload["idempotency_key"] != request.idempotency_key
                    || receipt.payload["tool_name"] != request.tool_name
                    || crate::ExecutionCommitService::receipt_observation_sequence(&index_receipt)
                        != Some(request.observation_wave_sequence)
                    || index_receipt.payload["effect_kind"] != index.payload["effect_kind"]
                    || index_receipt.payload["effect_scope"] != index.payload["effect_scope"]
                    || index_receipt.payload["outcome"]["tool_use_id"] != request.tool_use_id
                    || index_receipt.payload["outcome"]["tool_name"] != request.tool_name
                    || receipt.payload["descriptor_hash"]
                        != primary.payload["effect"]["descriptor_hash"]
                    || receipt.payload["replay"] != primary.payload["replay"]
                    || receipt.payload["resolved_effect_scope"]
                        != primary.payload["resolved_effect_scope"]
                    || receipt.payload["resolved_effect_scope"]
                        != index_receipt.payload["resolved_effect_scope"]
                    || receipt.payload["outcome"] != index_receipt.payload["outcome"]
                    || !matches!(
                        receipt.payload["outcome"]["status"].as_str(),
                        Some("executed" | "blocked_permission")
                    )
                {
                    return Err("runtime retirement effect receipt is not known settled".into());
                }
                if descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Process {
                    for cut in super::review_evidence::validate_bounded_process_retirement(
                        &self.store,
                        &receipt,
                        self.artifacts.as_deref(),
                    )? {
                        if process_sources
                            .insert(cut.stream_id, cut.expected_revision)
                            .is_some_and(|previous| previous != cut.expected_revision)
                        {
                            return Err("runtime retirement process source moved".into());
                        }
                    }
                }
            } else if !matches!(
                primary.payload["outcome"]["status"].as_str(),
                Some("executed" | "blocked_permission")
            ) {
                return Err("runtime retirement tool read is not known settled".into());
            }
        }
        for attempt_number in 1..=packet.attempt {
            let source = format!(
                "execution-agent-receipts:{}:{}:{attempt_number}",
                graph.id, node.id
            );
            let revision = pin(&source)?;
            let mut after = 0;
            let mut pending = std::collections::BTreeSet::new();
            let mut settled = std::collections::BTreeSet::new();
            while after < revision {
                let events =
                    self.store
                        .list_stream_after(&source, after, revision, 64, 4 * 1024 * 1024)?;
                if events.is_empty() {
                    return Err("runtime retirement receipt history is incomplete".into());
                }
                for event in events {
                    validate_receipt_index_record(&event, &source, after + 1, revision)?;
                    after = event.sequence;
                    if event.actor.as_deref() != Some("governed_tool") {
                        return Err("runtime retirement receipt owner mismatch".into());
                    }
                    let identity = event.payload["idempotency_key"]
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .ok_or("runtime retirement receipt identity missing")?;
                    match event.kind.as_str() {
                        "execution.agent_tool.intent" => {
                            pending.insert(identity.to_string());
                        }
                        "execution.agent_tool.receipt"
                            if matches!(
                                event.payload["outcome"]["status"].as_str(),
                                Some("executed" | "blocked_permission")
                            ) =>
                        {
                            if event.payload["effect_kind"] == "process" {
                                let stream = format!("execution-effect:{identity}");
                                let primary = self
                                    .store
                                    .event_by_idempotency_key(
                                        &stream,
                                        &format!("{identity}:receipt"),
                                    )
                                    .map_err(|error| error.to_string())?
                                    .ok_or(
                                        "runtime retirement original process terminal missing",
                                    )?;
                                for cut in
                                    super::review_evidence::validate_bounded_process_retirement(
                                        &self.store,
                                        &primary,
                                        self.artifacts.as_deref(),
                                    )?
                                {
                                    if process_sources
                                        .insert(cut.stream_id, cut.expected_revision)
                                        .is_some_and(|previous| previous != cut.expected_revision)
                                    {
                                        return Err(
                                            "runtime retirement process source moved".into()
                                        );
                                    }
                                }
                            }
                            settled.insert(identity.to_string());
                        }
                        _ => {
                            return Err("runtime retirement receipt is unknown or unsettled".into());
                        }
                    }
                }
            }
            if !pending.is_subset(&settled) {
                return Err("runtime retirement still has a pending physical effect".into());
            }
        }
        for (source, revision) in process_sources {
            if sources
                .insert(source, revision)
                .is_some_and(|previous| previous != revision)
            {
                return Err("runtime retirement process source changed before settlement".into());
            }
        }
        for (source, revision) in &sources {
            if self
                .store
                .stream_revision(source)
                .map_err(|error| error.to_string())?
                != *revision
            {
                return Err("runtime retirement source changed before settlement".into());
            }
        }
        Ok(Some(RuntimeRetirementAdmission {
            request_digest: super::review_evidence::proof_value_digest(envelope),
            program_id: projection.program_id.clone(),
            program_revision: projection.revision,
            sources: sources
                .into_iter()
                .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                })
                .collect(),
        }))
    }

    #[must_use]
    pub fn new(store: Arc<RuntimeEventStore>) -> Self {
        Self {
            store,
            artifacts: None,
            process_input_verifier: None,
            proof_session_queries: None,
            graphs: None,
            read_model: Arc::new(super::AgenticReadModel::default()),
        }
    }

    #[must_use]
    pub(crate) fn program_recovery_is_excluded(&self, program_id: &str) -> Result<bool, String> {
        if !self.store.has_session_recovery_exclusions() {
            return Ok(false);
        }
        let stream = program_stream(program_id);
        let opened = self
            .store
            .event_by_idempotency_key(&stream, "agentic-program-opened")
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "session_recovery_program_opening_missing".to_string())?;
        if opened.stream_id != stream
            || opened.scope != RuntimeEventScope::Program
            || opened.kind != PROGRAM_OPENED_EVENT_KIND
            || opened.schema_version != 1
            || opened.sequence != 1
        {
            return Err("session_recovery_program_opening_invalid".into());
        }
        let session_id = opened
            .payload
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .filter(|session| !session.trim().is_empty())
            .ok_or_else(|| "session_recovery_program_session_missing".to_string())?;
        Ok(self.store.is_session_recovery_excluded(session_id))
    }

    pub(crate) fn recovery_session_is_excluded(&self, session_id: &str) -> bool {
        self.store.is_session_recovery_excluded(session_id)
    }

    fn ensure_recovery_session_admitted(
        &self,
        session_id: &str,
    ) -> Result<(), AgentActionServiceError> {
        self.store
            .ensure_session_recovery_admitted(session_id)
            .map_err(|reason| {
                AgentActionServiceError::Store(RuntimeEventStoreError::InvalidTransaction(reason))
            })
    }

    pub(crate) fn with_graph_store(mut self, graphs: crate::ExecutionGraphStateStore) -> Self {
        self.graphs = Some(graphs);
        self
    }

    #[must_use]
    pub(crate) fn with_read_model(mut self, read_model: Arc<super::AgenticReadModel>) -> Self {
        self.read_model = read_model;
        self
    }

    /// Bind the physical artifact authority used by production Runtime
    /// services. Unit projections may omit it; an active Runtime never does.
    #[must_use]
    pub(crate) fn with_process_input_verifier(
        mut self,
        verifier: crate::agentic::review_evidence::ProcessInputVerifier,
    ) -> Self {
        self.process_input_verifier = Some(verifier);
        self
    }

    pub fn with_artifact_store(mut self, artifacts: Arc<crate::ArtifactStore>) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    #[must_use]
    pub(crate) fn with_session_query_slot(
        mut self,
        queries: Arc<OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>,
    ) -> Self {
        self.proof_session_queries = Some(queries);
        self
    }

    pub(crate) fn apply(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![stream_id.clone()];
        if matches!(&envelope.action, AgentAction::TaskPublish(input) if !input.obligation_refs.is_empty())
        {
            if let Some(root) = &envelope.actor.root_execution_id {
                streams.push(format!("goal:goal:{root}"));
            }
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(envelope, &stream_id, None, None)
        })
    }

    /// Commit one Program action and its prepared Goal revision atomically.
    /// The Goal mutation is prepared from durable state before acquiring the
    /// ordered two-stream lock; expected revisions make a stale preparation a
    /// whole-transaction conflict rather than a partial Objective update.
    pub(crate) fn apply_with_goal_event(
        &self,
        envelope: &AgentActionEnvelope,
        goal_stream_id: String,
        expected_goal_stream_revision: u64,
        goal_event: RuntimeTransactionEventInput,
        verification: Option<&ObjectiveReviewVerification>,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let program_stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![program_stream_id.clone(), goal_stream_id.clone()];
        streams.extend(
            verification
                .and_then(|proof| proof.input_set.as_ref())
                .into_iter()
                .flat_map(|inputs| inputs.source_revisions.keys().cloned()),
        );
        streams.extend(
            verification
                .into_iter()
                .flat_map(|proof| proof.result_source_revisions.keys().cloned()),
        );
        if verification.is_some_and(|proof| {
            !proof.independence_required || proof.effect_manifest_digest.is_some()
        }) {
            streams.push(format!("session:{}", envelope.actor.session_id));
            streams.extend(
                verification
                    .into_iter()
                    .flat_map(|proof| proof.effect_source_refs.iter().cloned()),
            );
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(
                envelope,
                &program_stream_id,
                Some((goal_stream_id, expected_goal_stream_revision, goal_event)),
                verification,
            )
        })
    }

    pub(crate) fn replay_if_applied(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<Option<AgentActionObservation>, AgentActionServiceError> {
        let stream_id = program_stream(&envelope.actor.program_id);
        if let Some(existing) = self
            .store
            .event_by_idempotency_key(&stream_id, &action_key(&envelope.action_id))?
        {
            let projection = self.project_snapshot(&envelope.actor.program_id)?;
            let original: AgentActionEnvelope = serde_json::from_value(
                existing.payload.get("envelope").cloned().ok_or_else(|| {
                    AgentActionServiceError::Corrupt(
                        "applied action has no durable envelope".into(),
                    )
                })?,
            )?;
            if original.actor != envelope.actor || original.action != envelope.action {
                return Ok(Some(rejected(
                    envelope,
                    projection.revision,
                    "action_id_conflict",
                    "action_id already identifies a different actor or payload; use the original request to retry",
                )));
            }
            let entity_ref = existing
                .payload
                .get("entity_ref")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            return applied_observation(envelope, &projection, entity_ref, true).map(Some);
        }

        Ok(None)
    }

    pub(crate) fn apply_verified_review(
        &self,
        envelope: &AgentActionEnvelope,
        verification: &ObjectiveReviewVerification,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        let explicit = match &envelope.action {
            AgentAction::TaskReview(input) => input.evidence_refs.as_slice(),
            AgentAction::ObjectiveReview(input) => input.evidence_refs.as_slice(),
            _ => &[],
        };
        super::review_evidence::validate_explicit_process_evidence(
            &self.store,
            verification,
            explicit,
        )
        .map_err(AgentActionServiceError::Corrupt)?;
        let stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![stream_id.clone()];
        streams.extend(
            verification
                .input_set
                .iter()
                .flat_map(|inputs| inputs.source_revisions.keys().cloned()),
        );
        streams.extend(verification.result_source_revisions.keys().cloned());
        if verification.effect_manifest_digest.is_some() {
            streams.push(format!("session:{}", envelope.actor.session_id));
            streams.extend(verification.effect_source_refs.iter().cloned());
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(envelope, &stream_id, None, Some(verification))
        })
    }

    fn apply_locked(
        &self,
        envelope: &AgentActionEnvelope,
        stream_id: &str,
        goal_event: Option<(String, u64, RuntimeTransactionEventInput)>,
        verification: Option<&ObjectiveReviewVerification>,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        self.ensure_recovery_session_admitted(&envelope.actor.session_id)?;
        if let Some(replay) = self.replay_if_applied(envelope)? {
            return Ok(replay);
        }

        let history = self
            .proof_session_queries
            .as_ref()
            .and_then(|queries| queries.get())
            .and_then(|queries| queries.history_reader());
        let process_frame = self
            .process_input_verifier
            .as_ref()
            .map(super::review_evidence::ProcessInputVerifier::frame);
        let readers = super::review_evidence::ResultContentReaders {
            artifacts: self.artifacts.as_deref(),
            history: history.as_deref(),
            process_inputs: process_frame.as_ref(),
        };
        let mut projection = match self.project_snapshot(&envelope.actor.program_id) {
            Ok(projection) => projection,
            Err(AgentActionServiceError::Corrupt(message)) if message == "program_not_found" => {
                let mut projection = AgenticProgramProjection::empty(
                    &envelope.actor.program_id,
                    &envelope.actor.objective_id,
                );
                projection.required_team_count = envelope.actor.required_team_count;
                projection.exact_team_count = envelope.actor.exact_team_count;
                projection
                    .objective_summary
                    .clone_from(&envelope.actor.objective_summary);
                if !envelope.actor.model_lease.trim().is_empty() {
                    projection
                        .model_lease
                        .clone_from(&envelope.actor.model_lease);
                }
                projection.permission_ceiling = envelope
                    .actor
                    .permission_ceiling
                    .unwrap_or(harness_contract::policy::PermissionMode::ReadOnly);
                projection
                    .resource_scopes
                    .clone_from(&envelope.actor.resource_scopes);
                projection.session_id.clone_from(&envelope.actor.session_id);
                projection.turn_id.clone_from(&envelope.actor.turn_id);
                projection
                    .root_execution_id
                    .clone_from(&envelope.actor.root_execution_id);
                Arc::new(projection)
            }
            Err(error) => return Err(error),
        };
        self.ensure_recovery_session_admitted(&projection.session_id)?;
        if projection.objective_id != envelope.actor.objective_id {
            return Ok(rejected(
                envelope,
                projection.revision,
                "objective_binding_mismatch",
                "the Program is bound to a different Objective",
            ));
        }
        if projection.session_id != envelope.actor.session_id
            || projection.turn_id != envelope.actor.turn_id
            || projection.root_execution_id != envelope.actor.root_execution_id
            || projection.required_team_count != envelope.actor.required_team_count
            || projection.objective_summary != envelope.actor.objective_summary
            || projection.model_lease != envelope.actor.model_lease
            || Some(projection.permission_ceiling) != envelope.actor.permission_ceiling
            || projection.resource_scopes != envelope.actor.resource_scopes
        {
            return Ok(rejected(
                envelope,
                projection.revision,
                "program_binding_mismatch",
                "session, turn, Objective, execution policy, or completion obligation differs from the frozen Program binding",
            ));
        }

        // ── Idempotency gate ────────────────────────────────────────────────
        // An already-Accepted Task is closed. Re-reviewing or re-submitting it
        // burns a provider round and an event without changing any fact. A live
        // 1-team run spent 169 of its 239 tool calls here and never reached the
        // terminal action. To change an accepted conclusion the model publishes a
        // superseding Task, which keeps the audit trail; supersede/withdraw/
        // release are lifecycle actions and are deliberately NOT gated.
        if matches!(
            envelope.action,
            AgentAction::TaskReview(_) | AgentAction::TaskSubmit(_)
        ) {
            let gated_task_ref = match &envelope.action {
                AgentAction::TaskReview(input) => Some(input.task_ref.as_str()),
                AgentAction::TaskSubmit(input) => Some(input.task_ref.as_str()),
                _ => None,
            };
            if let Some(task_ref) = gated_task_ref {
                if let Some(task) = projection.tasks.get(task_ref) {
                    if task.status == AgenticTaskStatus::Accepted {
                        return Ok(rejected(
                            envelope,
                            projection.revision,
                            "task_already_accepted",
                            "this Task is already Accepted; publish a superseding Task instead of re-reviewing or re-submitting it",
                        ));
                    }
                }
            }
        }

        if matches!(envelope.action, AgentAction::StateInspect(_)) {
            let goal = projection
                .root_execution_id
                .as_ref()
                .map(|root| {
                    crate::execution_core::goal::GoalStore::new(Arc::clone(&self.store))
                        .get(&format!("goal:{root}"))
                })
                .transpose()
                .map_err(AgentActionServiceError::Corrupt)?
                .flatten();
            if matches!(&envelope.action, AgentAction::StateInspect(input)
                if input.scope_ref.as_deref() == Some("collaboration_patterns"))
            {
                let patterns =
                    match crate::evolution::collaboration_experience::read_patterns(&self.store, 8)
                    {
                        Ok(patterns) => patterns,
                        Err(error) => {
                            return Ok(rejected(
                                envelope,
                                projection.revision,
                                "advisory_evidence_unavailable",
                                &error,
                            ));
                        }
                    };
                let mut observation = inspected_observation(&self.store, envelope, &projection, goal.as_ref());
                observation.projection = Some(json!({
                    "advisory_only": true,
                    "instruction": "These are structural observations from independent completed Turns, not prescribed plans, quality guarantees, executable definitions, or capability grants. Adopt, adapt, or ignore them. Inspect Runtime capabilities before using any suggested tool or skill.",
                    "patterns": patterns.iter().filter(|pattern| pattern.is_actionable()).map(|pattern| json!({
                        "pattern_ref": pattern.pattern_id,
                        "revision": pattern.pattern_revision,
                        "suggestion": pattern.semantic_suggestion,
                        "evidence_summary": pattern.evidence_summary,
                    })).collect::<Vec<_>>(),
                }));
                return Ok(observation);
            }
            return Ok(inspected_observation(&self.store, envelope, &projection, goal.as_ref()));
        }
        let mut review_source = None;
        if let AgentAction::TaskReview(input) = &envelope.action {
            if input.decision != harness_contract::agent_action::TaskReviewDecision::Accept {
                let Some(requirement) = input
                    .operator_requirement
                    .as_deref()
                    .map(str::trim)
                    .filter(|quote| !quote.is_empty())
                else {
                    return Ok(rejected(
                        envelope, projection.revision, "review_requirement_missing",
                        "Rework or challenge requires operator_requirement: quote the exact relevant original operator wording from state_inspect's Goal objective, or the frozen Program objective if no Goal exists. Explain the missing deliverable in reason; do not accept incomplete work or invent a stricter requirement.",
                    ));
                };
                let goal = projection
                    .root_execution_id
                    .as_ref()
                    .map(|root| {
                        crate::execution_core::goal::GoalStore::new(Arc::clone(&self.store))
                            .projection(&format!("goal:{root}"))
                    })
                    .transpose()
                    .map_err(AgentActionServiceError::Corrupt)?
                    .flatten();
                let objective = if let Some(goal) = goal.as_ref() {
                    if !goal.goal.execution_binding.as_ref().is_some_and(|binding| {
                        binding.agentic_program_id == projection.program_id
                            && binding.objective_id == projection.objective_id
                            && binding.session_id == projection.session_id
                            && binding.turn_id == projection.turn_id
                            && Some(&binding.root_execution_id)
                                == projection.root_execution_id.as_ref()
                    }) {
                        return Ok(rejected(envelope, projection.revision,
                            "review_goal_binding_mismatch",
                            "The durable Goal is not bound to this Program, root execution, Session, and Turn; review cannot borrow another objective."));
                    }
                    review_source = Some(ExpectedStreamRevision {
                        stream_id: format!("goal:{}", goal.goal.id),
                        expected_revision: goal.stream_revision,
                    });
                    goal.goal.objective.as_str()
                } else {
                    review_source =
                        projection
                            .root_execution_id
                            .as_ref()
                            .map(|root| ExpectedStreamRevision {
                                stream_id: format!("goal:goal:{root}"),
                                expected_revision: 0,
                            });
                    projection.objective_summary.as_str()
                };
                if !objective.contains(requirement) {
                    return Ok(rejected(envelope, projection.revision,
                        "review_requirement_not_in_objective",
                        &format!("operator_requirement does not occur in the bound original operator objective. Copy an exact relevant substring of the original objective below, not a translation or added Task acceptance. Explain the actual missing deliverable separately in reason. Bound original operator objective: {objective:?}. Rejected operator_requirement: {requirement:?}")));
                }
            }
        }
        let review_requires_proof = matches!(&envelope.action, AgentAction::TaskReview(_))
            || matches!(&envelope.action, AgentAction::ObjectiveReview(input)
                if input.decision == harness_contract::agent_action::ObjectiveReviewDecision::Satisfied);
        if (review_requires_proof && verification.is_none())
            || verification.is_some_and(|proof| {
                !proof
                    .input_set
                    .as_ref()
                    .is_some_and(|inputs| inputs.version == 2)
            })
        {
            return Ok(rejected(
                envelope,
                projection.revision,
                "unsupported_review_inputs",
                "new reviews require complete supported v2 verification",
            ));
        }
        let mut policy_sources = Vec::new();
        if let Some(proof) = verification {
            let goal = envelope
                .actor
                .root_execution_id
                .as_deref()
                .map(|root| {
                    crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                        .get(&format!("goal:{root}"))
                })
                .transpose()
                .map_err(AgentActionServiceError::EventStore)?
                .flatten();
            // The proof is re-verified against the action it is applying, so
            // the review's own in-flight call is resolved again from the
            // durable evidence - never from a value the proof carries.
            match super::review_evidence::validate_proof_inputs_for_action(
                &self.store,
                &projection,
                goal.as_ref(),
                proof,
                readers,
                Some(&super::review_evidence::CurrentReviewContext {
                    action_id: &envelope.action_id,
                    action: &envelope.action,
                    actor: &envelope.actor,
                }),
            ) {
                Ok(snapshot) => policy_sources.extend(snapshot.sources),
                Err(error) => {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "stale_review_evidence",
                        &error,
                    ));
                }
            }
        }
        if let Some(proof) = verification.filter(|proof| !proof.independence_required) {
            if envelope.actor.kind != AgentActorKind::Root
                || !matches!(envelope.action, AgentAction::ObjectiveReview(_))
                || goal_event.is_none()
            {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "invalid_review_policy",
                    "self review is confined to the bound Root Objective",
                ));
            }
            let root = envelope
                .actor
                .root_execution_id
                .as_deref()
                .ok_or_else(|| AgentActionServiceError::Corrupt("review has no root".into()))?;
            let goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                .get(&format!("goal:{root}"))
                .map_err(AgentActionServiceError::EventStore)?
                .ok_or_else(|| AgentActionServiceError::Corrupt("review has no Goal".into()))?;
            let policy =
                super::review_evidence::root_self_review_policy(&self.store, &projection, &goal)
                    .map_err(AgentActionServiceError::EventStore)?;
            let Some(policy) = policy else {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "stale_review_policy",
                    "Runtime risk or tool effects no longer permit this review",
                ));
            };
            policy_sources.push(policy.source);
        }
        if let Some(expected) = envelope.expected_revision {
            if expected != projection.revision {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "stale_revision",
                    &format!(
                        "expected Program revision {expected}, actual {}",
                        projection.revision
                    ),
                ));
            }
        }
        // ── Exception exit (blocked) ────────────────────────────────────────
        // The DECISION that a goal is unreachable belongs to the model. Runtime
        // only verifies that progress really has stopped, and never masks a real
        // gap: an accepted blocked exit ends as an honest Partial whose
        // `unresolved` stays visible.
        if let AgentAction::ObjectiveCompleteRequest(input) = &envelope.action {
            if input.blocked {
                if input
                    .blocked_reason
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                    || !input.human_review_required
                    || input.recommended_next_actions.is_empty()
                {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "blocked_exit_incomplete",
                        "a blocked exit requires a non-empty blocked_reason, human_review_required=true and at least one recommended_next_action",
                    ));
                }
                if !agentic_program_is_quiescent(&projection) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "blocked_exit_unearned",
                        "the Program still has work in flight; a blocked exit is not permitted until the remaining work is exhausted",
                    ));
                }
            }
        }
        let completion_context = if matches!(&envelope.action, AgentAction::ObjectiveCompleteRequest(input) if !input.blocked)
            && matches!(
                envelope.actor.kind,
                AgentActorKind::Root | AgentActorKind::Supervisor
            )
            && matches!(
                projection.status,
                super::program::AgenticProgramStatus::Open
                    | super::program::AgenticProgramStatus::Waiting
                    | super::program::AgenticProgramStatus::Draining
            ) {
            match super::review_evidence::completion_admission_context(
                &self.store,
                &projection,
                envelope,
                readers,
            ) {
                Ok(context) => context,
                Err(message) => {
                    self.record_completion_rejection(&projection, envelope, &message);
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "objective_not_verifiable",
                        &message,
                    ));
                }
            }
        } else {
            None
        };
        let retirement_context = match self.runtime_retirement_admission(&projection, envelope) {
            Ok(context) => context,
            Err(message) => {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "runtime_retirement_not_settled",
                    &message,
                ));
            }
        };
        let transition = match (completion_context.as_ref(), retirement_context.as_ref()) {
            (None, None) => {
                validate_transition(&projection, envelope, now_ms(), self.graphs.as_ref())
            }
            (completion, retirement) => validate_transition_with_admission_contexts(
                &projection,
                envelope,
                now_ms(),
                self.graphs.as_ref(),
                completion,
                retirement,
            ),
        };
        if let Some((code, message)) = transition {
            if matches!(
                code,
                "objective_not_verifiable" | "objective_blocked_for_human_review"
            ) && matches!(envelope.action, AgentAction::ObjectiveCompleteRequest(_))
            {
                // F2b: an authorization-time completion rejection never became a
                // durable reopen, so record it durably to keep the completion
                // loop bounded by the reopen count.
                self.record_completion_rejection(&projection, envelope, &message);
            }
            // T3.0 diagnostic: surface the exact rejection code so a failing
            // tool call (e.g. task_review accept) is readable, not a bare
            // "execution_error". Logs only; no behaviour change.
            tracing::warn!(
                code,
                action = %envelope.action.kind(),
                program_id = %envelope.actor.program_id,
                agent_id = envelope.actor.agent_id.as_deref().unwrap_or("-"),
                message = %message,
                "agent action rejected at transition validation"
            );
            return Ok(rejected(envelope, projection.revision, code, &message));
        }
        if let Some(context) = completion_context {
            policy_sources.extend(context.sources().iter().cloned());
        }
        // A Task may support several original Goal conditions, but it cannot
        // manufacture them. Keep the read revision in this same transaction so
        // a concurrent Goal change cannot admit a dangling reference.
        let mut obligation_source = None;
        if let AgentAction::TaskPublish(input) = &envelope.action {
            if !input.obligation_refs.is_empty() {
                let goal = projection
                    .root_execution_id
                    .as_ref()
                    .map(|root| {
                        crate::execution_core::goal::GoalStore::new(Arc::clone(&self.store))
                            .projection(&format!("goal:{root}"))
                    })
                    .transpose()
                    .map_err(AgentActionServiceError::Corrupt)?
                    .flatten();
                let Some(goal) = goal else {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_goal_not_found",
                        "Task obligation references require the bound durable Goal",
                    ));
                };
                if !goal.goal.execution_binding.as_ref().is_some_and(|binding| {
                    binding.agentic_program_id == projection.program_id
                        && binding.objective_id == projection.objective_id
                        && binding.session_id == projection.session_id
                        && binding.turn_id == projection.turn_id
                        && Some(&binding.root_execution_id) == projection.root_execution_id.as_ref()
                }) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_goal_binding_mismatch",
                        "Task references must belong to this Program's immutable Goal binding",
                    ));
                }
                if let Some(missing) = input.obligation_refs.iter().find(|reference| {
                    !goal
                        .goal
                        .criteria
                        .iter()
                        .any(|criterion| &criterion.id == *reference)
                        && !goal
                            .goal
                            .obligations
                            .iter()
                            .any(|obligation| &obligation.obligation_id == *reference)
                }) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_not_found",
                        &format!(
                            "Goal condition {missing} does not exist; inspect the current Goal"
                        ),
                    ));
                }
                obligation_source = Some(ExpectedStreamRevision {
                    stream_id: format!("goal:{}", goal.goal.id),
                    expected_revision: goal.stream_revision,
                });
            }
        }
        if let AgentAction::ArtifactCommit(input) = &envelope.action {
            if let Some(artifacts) = &self.artifacts {
                if let Err(error) = artifacts.resolve(&input.content_ref) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "artifact_content_not_durable",
                        &format!("content_ref must resolve in Runtime ArtifactStore: {error}"),
                    ));
                }
            }
        }
        if let Some(artifacts) = &self.artifacts {
            let evidence_refs: &[String] = match &envelope.action {
                AgentAction::TaskSubmit(input) => &input.evidence_refs,
                AgentAction::TaskReview(input) => &input.evidence_refs,
                AgentAction::TaskSupersede(input) => &input.evidence_refs,
                AgentAction::TaskWithdraw(input) => &input.evidence_refs,
                AgentAction::ObjectiveReview(input) => &input.evidence_refs,
                AgentAction::ObjectiveCompleteRequest(input) => &input.evidence_refs,
                _ => &[],
            };
            // Opaque artifact selectors can be verified locally. Stable
            // logical tool:// evidence is resolved and authenticated against
            // the Session journal by the async Gateway ingress before this
            // synchronous Program transition is applied.
            if let Some(reference) = evidence_refs.iter().find(|reference| {
                reference.starts_with("artifact://")
                    && artifacts.resolve(reference).is_err()
                    && !projection
                        .artifact_content_selector(reference)
                        .is_some_and(|selector| artifacts.resolve(selector).is_ok())
            }) {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "evidence_not_durable",
                    &format!(
                        "evidence reference does not resolve in Runtime ArtifactStore: {reference}"
                    ),
                ));
            }
        }

        let entity_ref = entity_ref(envelope).or_else(|| {
            goal_event.as_ref().and_then(|(_, _, event)| {
                event
                    .event
                    .payload
                    .get("action_entity_ref")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
        });
        let mut events = Vec::with_capacity(if projection.revision == 0 { 2 } else { 1 });
        if projection.revision == 0 {
            events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.to_string(),
                    scope: RuntimeEventScope::Program,
                    kind: PROGRAM_OPENED_EVENT_KIND.to_string(),
                    status: Some("open".to_string()),
                    actor: Some(envelope.actor.actor_id.clone()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "objective".to_string(),
                            id: envelope.actor.objective_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "session".to_string(),
                            id: envelope.actor.session_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: envelope.actor.turn_id.clone(),
                        },
                    ]
                    .into_iter()
                    .chain(
                        envelope
                            .actor
                            .root_execution_id
                            .iter()
                            .map(|id| RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: id.clone(),
                            }),
                    )
                    .collect(),
                    payload: json!({
                        "program_id": envelope.actor.program_id,
                        "objective_id": envelope.actor.objective_id,
                        "session_id": envelope.actor.session_id,
                        "turn_id": envelope.actor.turn_id,
                        "root_execution_id": envelope.actor.root_execution_id,
                        "required_team_count": envelope.actor.required_team_count,
                        "exact_team_count": envelope.actor.exact_team_count,
                        "objective_summary": envelope.actor.objective_summary,
                        "model_lease": envelope.actor.model_lease,
                        "permission_ceiling": envelope.actor.permission_ceiling,
                        "resource_scopes": envelope.actor.resource_scopes,
                    }),
                },
                idempotency_key: Some("agentic-program-opened".to_string()),
                schema_version: 1,
            });
        }
        events.push(RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: stream_id.to_string(),
                scope: RuntimeEventScope::Program,
                kind: ACTION_EVENT_KIND.to_string(),
                status: Some("applied".to_string()),
                actor: Some(envelope.actor.actor_id.clone()),
                refs: action_refs(envelope, entity_ref.as_deref()),
                payload: json!({
                    "envelope": envelope,
                    "entity_ref": entity_ref,
                    "review_verification": verification,
                }),
            },
            idempotency_key: Some(action_key(&envelope.action_id)),
            schema_version: 1,
        });
        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: stream_id.to_string(),
            expected_revision: projection.revision,
        }];
        expected_streams.extend(policy_sources);
        expected_streams.extend(
            retirement_context
                .iter()
                .flat_map(|context| context.sources.iter().cloned()),
        );
        expected_streams.extend(
            verification
                .into_iter()
                .flat_map(|proof| &proof.result_source_revisions)
                .map(|(stream_id, revision)| ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: *revision,
                }),
        );
        if let Some(source) = review_source {
            expected_streams.push(source);
        }
        if let Some(source) = obligation_source {
            expected_streams.push(source);
        }
        if let Some((goal_stream_id, expected_goal_stream_revision, goal_event)) = goal_event {
            expected_streams.push(ExpectedStreamRevision {
                stream_id: goal_stream_id,
                expected_revision: expected_goal_stream_revision,
            });
            events.push(goal_event);
        }
        let mut unique_sources = std::collections::BTreeMap::new();
        for source in expected_streams {
            if unique_sources
                .insert(source.stream_id, source.expected_revision)
                .is_some_and(|previous| previous != source.expected_revision)
            {
                return Err(AgentActionServiceError::Corrupt(
                    "review source changed during preparation".into(),
                ));
            }
        }
        let expected_streams = unique_sources
            .into_iter()
            .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                stream_id,
                expected_revision,
            })
            .collect();
        self.store
            .append_transaction_locked(AppendTransactionRequest {
                transaction_id: format!(
                    "agent-action:{}:{}",
                    envelope.actor.program_id, envelope.action_id
                ),
                expected_streams,
                events,
            })?;
        // S1: task success/rework is measured at the commit boundary (after the
        // replay guard and the durable append), never in the replayable
        // projection fold, so replays and rebuilds cannot inflate the counts.
        if let AgentAction::TaskReview(input) = &envelope.action {
            match input.decision {
                harness_contract::agent_action::TaskReviewDecision::Accept => {
                    crate::repetition_guard::clear_failure_breakers(
                        &self.store.repetition_scope(&envelope.actor.session_id, &envelope.actor.turn_id),
                    );
                    crate::execution_core::performance::observe_count("agentic_task_accepted", 1);
                }
                harness_contract::agent_action::TaskReviewDecision::Challenge
                | harness_contract::agent_action::TaskReviewDecision::Rework => {
                    crate::execution_core::performance::observe_count("agentic_task_reworked", 1);
                }
            }
        }
        projection = self.project_snapshot(&envelope.actor.program_id)?;
        applied_observation(envelope, &projection, entity_ref, false)
    }

    /// Durable head revision of one Program's journal, without folding it.
    ///
    /// Used by the barrier resolver to notice that the Program moved on since the
    /// projection it just read, so it can re-read once instead of waiting for a
    /// wake that is not guaranteed to arrive.
    pub fn program_revision(&self, program_id: &str) -> Result<u64, AgentActionServiceError> {
        Ok(self.store.stream_revision(&program_stream(program_id))?)
    }

    pub fn project(
        &self,
        program_id: &str,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        // Callers that need an independently mutable projection explicitly own a copy.
        // Bounded read consumers use the shared immutable snapshot below.
        self.project_snapshot(program_id)
            .map(|snapshot| (*snapshot).clone())
    }

    /// Immutable causal view for read consumers. Unlike `project`, this does
    /// not copy the complete Program on an unchanged durable-head cache hit.
    pub fn project_snapshot(
        &self,
        program_id: &str,
    ) -> Result<Arc<AgenticProgramProjection>, AgentActionServiceError> {
        let stream_id = program_stream(program_id);
        let durable_revision = self.store.stream_revision(&stream_id)?;
        if let Some(mut cached) = self.read_model.get(program_id) {
            if cached.revision == durable_revision {
                // The Program stream did not move, but the Goal stream may have:
                // a satisfied ObjectiveReview lives on the Goal, and it is
                // exactly what retires a recorded completion gap. Refresh the
                // derived field before serving the cached projection.
                let gaps = self.current_completion_gaps(&cached);
                if gaps != cached.unresolved {
                    Arc::make_mut(&mut cached).unresolved = gaps;
                    self.remember_projection(&cached);
                }
                return Ok(cached);
            }
            if cached.revision < durable_revision {
                self.catch_up_projection(Arc::make_mut(&mut cached), &stream_id, durable_revision)?;
                self.refresh_completion_gaps(Arc::make_mut(&mut cached));
                self.remember_projection(&cached);
                return Ok(cached);
            }
        }
        if let Some(mut persisted) = self.persisted_projection(program_id, durable_revision)? {
            self.catch_up_projection(&mut persisted, &stream_id, durable_revision)?;
            self.refresh_completion_gaps(&mut persisted);
            let persisted = Arc::new(persisted);
            self.remember_projection(&persisted);
            return Ok(persisted);
        }
        let events = self
            .store
            .list_stream_after(&stream_id, 0, durable_revision, 100, 1024 * 1024)
            .map_err(AgentActionServiceError::EventStore)?;
        let opened = events
            .iter()
            .find(|event| event.kind == PROGRAM_OPENED_EVENT_KIND)
            .ok_or_else(|| AgentActionServiceError::Corrupt("program_not_found".to_string()))?;
        let mut projection = projection_from_opened(program_id, opened)?;
        apply_projection_events(&mut projection, events)?;
        self.catch_up_projection(&mut projection, &stream_id, durable_revision)?;
        self.refresh_completion_gaps(&mut projection);
        let projection = Arc::new(projection);
        self.remember_projection(&projection);
        Ok(projection)
    }

    pub(crate) fn project_snapshot_bounded<Failure>(
        &self,
        program_id: &str,
        durable_revision: u64,
        opened: &crate::DurableRuntimeEvent,
        goal: Option<&harness_contract::goal::GoalContract>,
        mut read_page: impl FnMut(u64, u64) -> Result<Vec<crate::DurableRuntimeEvent>, Failure>,
        map_error: impl Fn(AgentActionServiceError) -> Failure,
    ) -> Result<Arc<AgenticProgramProjection>, Failure> {
        let stream = program_stream(program_id);
        if durable_revision == 0
            || opened.stream_id != stream
            || opened.sequence != 1
            || opened.scope != RuntimeEventScope::Program
            || opened.schema_version != 1
            || opened.kind != PROGRAM_OPENED_EVENT_KIND
        {
            return Err(map_error(AgentActionServiceError::Corrupt(
                "invalid bounded Program opening".into(),
            )));
        }
        let cached = self.read_model.get(program_id);
        let cached_revision = cached.as_ref().map(|snapshot| snapshot.revision);
        let mut snapshot = match cached {
            Some(cached) if cached.revision <= durable_revision => cached,
            _ => {
                let mut projection =
                    projection_from_opened(program_id, opened).map_err(&map_error)?;
                apply_projection_events(&mut projection, vec![opened.clone()])
                    .map_err(&map_error)?;
                Arc::new(projection)
            }
        };
        while snapshot.revision < durable_revision {
            let page = read_page(snapshot.revision, durable_revision)?;
            if page.is_empty() {
                return Err(map_error(AgentActionServiceError::Corrupt(
                    "bounded Program sequence gap".into(),
                )));
            }
            let mut expected = snapshot.revision;
            for event in &page {
                expected = expected.checked_add(1).ok_or_else(|| {
                    map_error(AgentActionServiceError::Corrupt(
                        "Program sequence overflow".into(),
                    ))
                })?;
                if event.stream_id != stream
                    || event.sequence != expected
                    || expected > durable_revision
                    || event.scope != RuntimeEventScope::Program
                    || event.schema_version != 1
                {
                    return Err(map_error(AgentActionServiceError::Corrupt(
                        "invalid bounded Program tail".into(),
                    )));
                }
            }
            apply_projection_events(Arc::make_mut(&mut snapshot), page).map_err(&map_error)?;
            if snapshot.revision != expected {
                return Err(map_error(AgentActionServiceError::Corrupt(
                    "bounded Program reducer coverage".into(),
                )));
            }
        }
        if snapshot.program_id != program_id
            || snapshot.objective_id != opened.payload["objective_id"].as_str().unwrap_or_default()
            || snapshot.root_execution_id.as_deref() != opened.payload["root_execution_id"].as_str()
            || snapshot.session_id != opened.payload["session_id"].as_str().unwrap_or_default()
            || snapshot.turn_id != opened.payload["turn_id"].as_str().unwrap_or_default()
        {
            return Err(map_error(AgentActionServiceError::Corrupt(
                "bounded Program identity mismatch".into(),
            )));
        }
        if let Some(goal) = goal {
            if goal.session_id != snapshot.session_id
                || goal.execution_binding.as_ref().is_some_and(|binding| {
                    Some(binding.root_execution_id.as_str())
                        != snapshot.root_execution_id.as_deref()
                        || binding.agentic_program_id != snapshot.program_id
                        || binding.objective_id != snapshot.objective_id
                        || binding.session_id != snapshot.session_id
                        || binding.turn_id != snapshot.turn_id
                })
            {
                return Err(map_error(AgentActionServiceError::Corrupt(
                    "bounded Program Goal binding mismatch".into(),
                )));
            }
        }
        let gaps = completion_gaps_from_goal(&snapshot, goal);
        if snapshot.unresolved != gaps {
            Arc::make_mut(&mut snapshot).unresolved = gaps;
        }
        if cached_revision.is_none_or(|revision| revision < snapshot.revision) {
            self.read_model.put(Arc::clone(&snapshot));
        }
        Ok(snapshot)
    }

    fn catch_up_projection(
        &self,
        projection: &mut AgenticProgramProjection,
        stream: &str,
        head: u64,
    ) -> Result<(), AgentActionServiceError> {
        while projection.revision < head {
            let page = self
                .store
                .list_stream_after(stream, projection.revision, head, 100, 1024 * 1024)
                .map_err(AgentActionServiceError::EventStore)?;
            if page.is_empty() {
                return Err(AgentActionServiceError::Corrupt(
                    "Program tail has a sequence gap".into(),
                ));
            }
            let mut expected = projection.revision;
            for event in &page {
                expected = expected.checked_add(1).ok_or_else(|| {
                    AgentActionServiceError::Corrupt("Program sequence overflow".into())
                })?;
                if event.stream_id != stream
                    || event.sequence != expected
                    || event.schema_version != 1
                {
                    return Err(AgentActionServiceError::Corrupt(
                        "unsupported or discontinuous Program tail".into(),
                    ));
                }
            }
            apply_projection_events(projection, page)?;
            if projection.revision != expected {
                return Err(AgentActionServiceError::Corrupt(
                    "Program reducer did not consume tail".into(),
                ));
            }
        }
        Ok(())
    }

    fn persisted_projection(
        &self,
        program_id: &str,
        durable_revision: u64,
    ) -> Result<Option<AgenticProgramProjection>, AgentActionServiceError> {
        let Some(checkpoint) = self
            .store
            .projection_checkpoint(&read_model_projection_id(program_id))?
        else {
            return Ok(None);
        };
        if checkpoint.source_cursor > durable_revision {
            tracing::warn!(
                program_id,
                checkpoint_revision = checkpoint.source_cursor,
                durable_revision,
                "discarding Agentic read snapshot ahead of its durable Program"
            );
            return Ok(None);
        }
        Ok(decode_read_snapshot(&checkpoint, program_id, &self.store))
    }

    fn remember_projection(&self, projection: &Arc<AgenticProgramProjection>) {
        self.read_model.put(Arc::clone(projection));
        let projection_id = read_model_projection_id(&projection.program_id);
        let payload = match serde_json::to_value(projection.as_ref()) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(program_id = %projection.program_id, %error, "Agentic read snapshot serialization failed");
                return;
            }
        };
        let body = match self.store.prepare_content(
            &format!("session:{}", projection.session_id),
            &format!("{projection_id}:{}", projection.revision),
            &payload,
            &crate::PreparedArtifactOrigin::CheckpointBody {
                projection_id: projection_id.clone(),
                source_cursor: projection.revision,
            },
        ) {
            Ok(body) => body,
            Err(error) => {
                tracing::warn!(program_id=%projection.program_id,%error,"current Program checkpoint preparation failed");
                return;
            }
        };
        let payload = json!({"schema_version":2,"session_id":projection.session_id,"body":body});
        // Concurrent Program readers race the same snapshot row. Re-read the
        // authoritative row revision and retry the CAS instead of dropping the
        // snapshot on the first stale revision.
        for attempt in 0..3 {
            let checkpoint = self
                .store
                .projection_checkpoint(&projection_id)
                .ok()
                .flatten();
            if checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.source_cursor >= projection.revision
                    && self
                        .store
                        .stream_revision(&program_stream(&projection.program_id))
                        .is_ok_and(|head| checkpoint.source_cursor <= head)
                    && decode_read_snapshot(checkpoint, &projection.program_id, &self.store)
                        .is_some()
            }) {
                return;
            }
            let repair_ahead = checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.source_cursor > projection.revision
                    && self
                        .store
                        .stream_revision(&program_stream(&projection.program_id))
                        .is_ok_and(|head| checkpoint.source_cursor > head)
            });
            let write = if repair_ahead {
                RuntimeEventStore::compare_and_repair_projection_checkpoint
            } else {
                RuntimeEventStore::compare_and_put_projection_checkpoint
            };
            match write(
                &self.store,
                &projection_id,
                projection.revision,
                checkpoint
                    .as_ref()
                    .map_or(0, |checkpoint| checkpoint.revision),
                &payload,
                now_ms(),
            ) {
                Ok(_) => return,
                Err(RuntimeEventStoreError::StaleRevision { .. }) if attempt < 2 => {
                    continue;
                }
                Err(error) => {
                    tracing::warn!(program_id = %projection.program_id, %error, "Agentic read snapshot persistence failed");
                    return;
                }
            }
        }
    }

    /// Return the bounded, durable Program/own-Team topic delta for one
    /// physical Agent execution. Program revisions order both topics, while a
    /// separate execution cursor makes delivery restart-safe without mutating
    /// Program truth or repeatedly injecting the full projection.
    pub(crate) fn topic_observations(
        &self,
        program_id: &str,
        agent_id: &str,
        team_id: &str,
        execution_id: &str,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Option<AgenticTopicObservationPage>, AgentActionServiceError> {
        if program_id.trim().is_empty()
            || agent_id.trim().is_empty()
            || execution_id.trim().is_empty()
            || max_entries == 0
            || max_bytes == 0
        {
            return Ok(None);
        }
        let projection = self.project_snapshot(program_id)?;
        let _member = projection.agents.get(agent_id).ok_or_else(|| {
            AgentActionServiceError::Corrupt(format!(
                "topic_observer_not_in_program_roster:{agent_id}"
            ))
        })?;
        let team_topics = readable_topic_refs(&projection, agent_id, team_id);
        let cursor_stream = topic_cursor_stream(program_id, execution_id);
        let (from_revision, cursor_revision) = self.topic_cursor(&cursor_stream)?;
        let mut candidates = projection
            .topics
            .iter()
            .filter(|(topic_ref, _)| team_topics.contains(topic_ref.as_str()))
            .flat_map(|(topic_ref, entries)| {
                let start = entries.partition_point(|entry| entry.revision <= from_revision);
                entries[start..]
                    .iter()
                    .filter(|entry| {
                        entry.actor_id != agent_id && topic_entry_visible(entry, agent_id)
                    })
                    .take(max_entries)
                    .map(move |entry| (topic_ref, entry))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.1
                .revision
                .cmp(&right.1.revision)
                .then_with(|| left.1.entry_id.cmp(&right.1.entry_id))
        });

        let mut entries = Vec::new();
        let mut observed_bytes = 0usize;
        for (topic_ref, source) in candidates {
            if entries.len() >= max_entries {
                break;
            }
            let mut entry = AgenticTopicObservation {
                topic_ref: topic_ref.clone(),
                entry: source.clone(),
            };
            let mut entry_bytes = serde_json::to_vec(&entry)?.len();
            if !entries.is_empty()
                && (entries.len() >= max_entries
                    || observed_bytes.saturating_add(entry_bytes) > max_bytes)
            {
                break;
            }
            if entry_bytes > max_bytes {
                // Never inject an unbounded model payload. Preserve routing,
                // identity and revision, but require an explicit scoped read
                // for a producer that violated the compact-message contract.
                entry.entry.summary = Some(
                    "Message metadata exceeded the observation page; inspect this topic/entry explicitly"
                        .to_string(),
                );
                entry.entry.content_ref = None;
                entry.entry.refs.clear();
                entry_bytes = serde_json::to_vec(&entry)?.len();
                if entry_bytes > max_bytes {
                    return Err(AgentActionServiceError::Corrupt(
                        "topic_observation_identity_exceeds_page_budget".to_string(),
                    ));
                }
            }
            observed_bytes = observed_bytes.saturating_add(entry_bytes);
            entries.push(entry);
        }
        let Some(to_revision) = entries.last().map(|entry| entry.entry.revision) else {
            return Ok(None);
        };
        Ok(Some(AgenticTopicObservationPage {
            entries,
            from_revision,
            to_revision,
            cursor_revision,
        }))
    }

    /// Monotonic acknowledgement for a topic page. The cursor is a projection
    /// offset owned by Runtime, never a second writer of Program/Topic state.
    pub(crate) fn acknowledge_topic_observations(
        &self,
        request: AgenticTopicObservationAck,
    ) -> Result<(), AgentActionServiceError> {
        if self
            .program_recovery_is_excluded(&request.program_id)
            .map_err(AgentActionServiceError::EventStore)?
        {
            return Err(AgentActionServiceError::Store(
                RuntimeEventStoreError::InvalidTransaction("session_recovery_quarantined".into()),
            ));
        }
        let stream_id = topic_cursor_stream(&request.program_id, &request.execution_id);
        self.store.with_stream_lock(&stream_id, || {
            let (through_revision, cursor_revision) = self.topic_cursor(&stream_id)?;
            if request.through_revision <= through_revision {
                return Ok(());
            }
            if cursor_revision != request.expected_cursor_revision {
                return Err(AgentActionServiceError::Store(
                    RuntimeEventStoreError::StaleRevision {
                        stream_id: stream_id.clone(),
                        expected: request.expected_cursor_revision,
                        actual: cursor_revision,
                    },
                ));
            }
            self.store
                .append_transaction_locked(AppendTransactionRequest {
                    transaction_id: format!(
                        "agentic-topic-ack:{}:{}:{}",
                        request.program_id, request.execution_id, request.through_revision
                    ),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: stream_id.clone(),
                        expected_revision: cursor_revision,
                    }],
                    events: vec![RuntimeTransactionEventInput {
                        event: RuntimeEventInput {
                            stream_id: stream_id.clone(),
                            scope: RuntimeEventScope::Agent,
                            kind: TOPIC_CURSOR_EVENT_KIND.to_string(),
                            status: Some("acknowledged".to_string()),
                            actor: Some("runtime.agent_topic_observer".to_string()),
                            refs: vec![RuntimeEventRef {
                                kind: "program".to_string(),
                                id: request.program_id.clone(),
                            }],
                            payload: json!({
                                "program_id": request.program_id,
                                "execution_id": request.execution_id,
                                "through_revision": request.through_revision,
                                "observation_kind": request.observation_kind,
                            }),
                        },
                        idempotency_key: Some(format!(
                            "agentic-topic-ack:{}",
                            request.through_revision
                        )),
                        schema_version: 1,
                    }],
                })?;
            Ok(())
        })
    }

    fn topic_cursor(&self, stream_id: &str) -> Result<(u64, u64), AgentActionServiceError> {
        let events = self
            .store
            .list_stream(stream_id)
            .map_err(AgentActionServiceError::EventStore)?;
        let cursor_revision = events.last().map_or(0, |event| event.sequence);
        let through_revision = events
            .iter()
            .rev()
            .find(|event| event.kind == TOPIC_CURSOR_EVENT_KIND)
            .and_then(|event| event.payload.get("through_revision"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        Ok((through_revision, cursor_revision))
    }

    /// Record a durable marker for a completion request rejected at
    /// authorization. Best-effort: a failed append must not change the
    /// rejection disposition the caller already computed.
    fn record_completion_rejection(
        &self,
        projection: &AgenticProgramProjection,
        envelope: &AgentActionEnvelope,
        gap: &str,
    ) {
        if self
            .store
            .is_session_recovery_excluded(&projection.session_id)
        {
            return;
        }
        let stream_id = program_stream(&projection.program_id);
        let event = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id,
                scope: RuntimeEventScope::Program,
                kind: COMPLETION_REJECTED_EVENT_KIND.to_string(),
                status: Some("rejected".to_string()),
                actor: Some(envelope.actor.actor_id.clone()),
                refs: vec![RuntimeEventRef {
                    kind: "objective".to_string(),
                    id: envelope.actor.objective_id.clone(),
                }],
                payload: json!({
                    "program_id": projection.program_id,
                    "request_action_id": envelope.action_id,
                    "gap": gap,
                }),
            },
            idempotency_key: Some(format!("completion-rejected:{}", envelope.action_id)),
            schema_version: 1,
        };
        // Same two corrections as the discipline-conflict marker: append while
        // already holding the stream lock, and carry the expected revision. This
        // marker was written with `append_transaction` and an empty
        // `expected_streams`, which the store rejects - so the bounded-convergence
        // count it exists to feed never saw a single one, behind a `let _ =` that
        // swallowed the error for the life of the feature.
        if let Err(error) = self
            .store
            .append_transaction_locked(AppendTransactionRequest {
                transaction_id: format!(
                    "completion-rejected:{}:{}",
                    projection.program_id, envelope.action_id
                ),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: program_stream(&projection.program_id),
                    expected_revision: projection.revision,
                }],
                events: vec![event],
            })
        {
            tracing::warn!(
                target: "cowd::agentic",
                program_id = %projection.program_id,
                %error,
                "completion rejection marker not recorded; the reopen bound is measuring fewer rejections than occurred"
            );
        }
    }

    /// Commit the Goal decision and the exact Program request disposition in
    /// one transaction. Neither domain can become terminal on its own here.
    pub(crate) fn commit_program_conclusion(
        &self,
        projection: &AgenticProgramProjection,
        mut prepared: crate::execution_core::goal::PreparedProgramConclusion,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        self.ensure_recovery_session_admitted(&projection.session_id)?;
        self.ensure_recovery_session_admitted(&prepared.goal.session_id)?;
        let original_revision = projection.revision;
        let own_stream = program_stream(&projection.program_id);
        if prepared.goal.completion == harness_contract::goal::GoalCompletion::Satisfied
            && !prepared.policy_sources.iter().any(|source| {
                source.stream_id == own_stream && source.expected_revision == original_revision
            })
        {
            return Err(AgentActionServiceError::Corrupt(
                "prepared own Program carrier cut is missing".into(),
            ));
        }
        let current = self.project(&projection.program_id)?;
        let lease_rebased = current.revision != original_revision;
        if lease_rebased {
            let delta = self
                .store
                .list_stream_after(
                    &own_stream,
                    original_revision,
                    current.revision,
                    1_024,
                    4 * 1024 * 1024,
                )
                .map_err(AgentActionServiceError::EventStore)?;
            if current.revision <= original_revision
                || current.revision - original_revision != delta.len() as u64
                || !delta.iter().enumerate().all(|(offset, event)| {
                    event.sequence == original_revision + offset as u64 + 1
                        && event.scope == RuntimeEventScope::Program
                        && event.kind == TASK_LEASE_RENEWED_EVENT_KIND
                })
                || super::review_evidence::proof_value_digest(&current.completion_request)
                    != super::review_evidence::proof_value_digest(&projection.completion_request)
            {
                return Err(RuntimeEventStoreError::StaleRevision {
                    stream_id: own_stream,
                    expected: original_revision,
                    actual: current.revision,
                }
                .into());
            }
        }
        let projection = &current;
        let history = self
            .proof_session_queries
            .as_ref()
            .and_then(|queries| queries.get())
            .and_then(|queries| queries.history_reader());
        let process_frame = self
            .process_input_verifier
            .as_ref()
            .map(super::review_evidence::ProcessInputVerifier::frame);
        let readers = super::review_evidence::ResultContentReaders {
            artifacts: self.artifacts.as_deref(),
            history: history.as_deref(),
            process_inputs: process_frame.as_ref(),
        };
        if prepared.goal.completion == harness_contract::goal::GoalCompletion::Satisfied {
            let mut current_sources = std::collections::BTreeMap::new();
            let request = projection.completion_request.as_ref().ok_or_else(|| {
                AgentActionServiceError::Corrupt("missing completion request before A".into())
            })?;
            let event = self
                .store
                .event_by_idempotency_key(
                    &program_stream(&projection.program_id),
                    &action_key(&request.action_id),
                )?
                .ok_or_else(|| {
                    AgentActionServiceError::Corrupt(
                        "completion request action disappeared before A".into(),
                    )
                })?;
            let envelope: AgentActionEnvelope =
                serde_json::from_value(event.payload["envelope"].clone())?;
            let AgentAction::ObjectiveCompleteRequest(input) = &envelope.action else {
                return Err(AgentActionServiceError::Corrupt(
                    "completion request action changed before A".into(),
                ));
            };
            if event.kind != ACTION_EVENT_KIND
                || event.sequence != request.program_revision
                || envelope.action_id != request.action_id
                || envelope.actor.actor_id != request.requested_by
                || envelope.actor.program_id != projection.program_id
                || envelope.actor.objective_id != projection.objective_id
                || envelope.actor.session_id != projection.session_id
                || envelope.actor.turn_id != projection.turn_id
                || envelope.actor.root_execution_id != projection.root_execution_id
                || request.result_refs != input.result_refs
                || request.evidence_refs != input.evidence_refs
                || request.unresolved != input.unresolved
                || input.blocked
            {
                return Err(AgentActionServiceError::Corrupt(
                    "completion request provenance changed before A".into(),
                ));
            }
            let context = super::review_evidence::completion_admission_context(
                &self.store,
                projection,
                &envelope,
                readers,
            )
            .map_err(AgentActionServiceError::EventStore)?;
            if context.as_ref().is_some_and(|context| {
                !context.matches_goal(&prepared.goal, prepared.expected_stream_revision)
            }) {
                return Err(AgentActionServiceError::EventStore(
                    "completion context Goal cut changed before A".into(),
                ));
            }
            if let Some(gap) =
                super::supervision::completion_gap_with_context(projection, input, context.as_ref())
            {
                return Err(AgentActionServiceError::EventStore(format!(
                    "completion admission changed before A:{gap}"
                )));
            }
            if let Some(context) = context {
                for source in context.sources() {
                    current_sources.insert(source.stream_id.clone(), source.expected_revision);
                }
            }
            if prepared.goal.criteria.iter().any(|criterion| {
                use harness_contract::goal::AcceptanceStatus;
                !matches!(
                    criterion.status,
                    AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
                ) || (criterion.status == AcceptanceStatus::Waived
                    && !criterion.waiver.as_ref().is_some_and(|waiver| {
                        !waiver.actor.trim().is_empty()
                            && !waiver.reason.trim().is_empty()
                            && !waiver.permission_receipt.trim().is_empty()
                    }))
            }) {
                return Err(AgentActionServiceError::Corrupt(
                    "prepared acceptance status cannot bypass required review or waiver".into(),
                ));
            }
            let conditions = super::review_evidence::required_review_conditions(&prepared.goal);
            for condition in conditions {
                let review = prepared
                    .goal
                    .reviews
                    .iter()
                    .rev()
                    .find(|review| review.criterion_ref == condition)
                    .ok_or_else(|| {
                        AgentActionServiceError::Corrupt(
                            "required review disappeared before A".into(),
                        )
                    })?;
                let proof = review.verification.as_ref().ok_or_else(|| {
                    AgentActionServiceError::Corrupt(format!(
                        "required review {condition} has no verification"
                    ))
                })?;
                let inputs = proof
                    .input_set
                    .as_ref()
                    .filter(|inputs| inputs.version == 2)
                    .ok_or_else(|| {
                        AgentActionServiceError::Corrupt(format!(
                            "unsupported required review {condition}"
                        ))
                    })?;
                if review.decision != "satisfied"
                    || review.spec_revision != prepared.goal.spec_revision
                    || review.input_manifest_digest != prepared.goal.spec_digest
                    || inputs.condition_ref.as_deref() != Some(condition.as_str())
                {
                    return Err(AgentActionServiceError::Corrupt(format!(
                        "invalid required review {condition}"
                    )));
                }
                let snapshot = super::review_evidence::validate_proof_inputs(
                    &self.store,
                    projection,
                    Some(&prepared.goal),
                    proof,
                    readers,
                )
                .map_err(AgentActionServiceError::EventStore)?;
                if !snapshot.reviewer_matches(projection, review)
                    || proof.goal_spec_revision != Some(prepared.goal.spec_revision)
                    || proof.goal_spec_digest.as_deref() != Some(prepared.goal.spec_digest.as_str())
                    || inputs.material_events.is_empty()
                    || review.evidence_refs.is_empty()
                    || review.producer_refs.is_empty()
                    || review.result_refs.is_empty()
                    || proof.producer_execution_ids.is_empty()
                    || proof.reviewer_execution_id.is_empty()
                    || proof.producer_execution_ids.iter().any(|producer| {
                        producer.is_empty()
                            || (proof.independence_required
                                && producer == &proof.reviewer_execution_id)
                    })
                    || (proof.independence_required
                        && review.producer_refs.contains(&review.reviewer_actor))
                {
                    return Err(AgentActionServiceError::Corrupt(format!(
                        "invalid required review identity {condition}"
                    )));
                }
                let mut required_results = review.result_refs.clone();
                if prepared.goal.user_intent_criterion_id.as_ref() == Some(&condition) {
                    required_results.extend(crate::execution_core::goal::delivered_result_refs(
                        projection,
                        &prepared.goal,
                        &request.result_refs,
                    ));
                }
                for obligation in prepared.goal.obligations.iter().filter(|obligation| {
                    obligation.required && obligation.obligation_id == condition
                }) {
                    required_results.extend(obligation.artifact_refs.iter().cloned());
                }
                for reference in required_results {
                    let identity =
                        super::review_evidence::review_result_identity(projection, &reference)
                            .map_err(AgentActionServiceError::Corrupt)?;
                    let covers = |refs: &[String]| {
                        refs.iter().any(|reference| {
                            super::review_evidence::review_result_identity(projection, reference)
                                .is_ok_and(|current| current == identity)
                        })
                    };
                    if !covers(&review.result_refs)
                        || !covers(&inputs.result_refs)
                        || !proof.reads.iter().any(|read| {
                            super::review_evidence::review_result_identity(
                                projection,
                                &read.result_ref,
                            )
                            .is_ok_and(|current| current == identity)
                                && !read.sha256.is_empty()
                                && !read.receipt_refs.is_empty()
                                && (read.result_kind
                                    != harness_contract::goal::GoalResultKind::ToolEffect
                                    || !read.effect_observation_refs.is_empty())
                        })
                    {
                        return Err(AgentActionServiceError::Corrupt(format!(
                            "required result coverage missing for {condition}"
                        )));
                    }
                }
                for source in snapshot.sources {
                    if current_sources
                        .insert(source.stream_id, source.expected_revision)
                        .is_some_and(|previous| previous != source.expected_revision)
                    {
                        return Err(AgentActionServiceError::EventStore(
                            "proof sources changed between required reviews before A".into(),
                        ));
                    }
                }
            }
            let stored_goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                .projection(&prepared.goal.id)
                .map_err(AgentActionServiceError::EventStore)?
                .ok_or_else(|| {
                    AgentActionServiceError::Corrupt("prepared Goal disappeared before A".into())
                })?;
            let terminal = prepared.goal.terminal.as_ref().ok_or_else(|| {
                AgentActionServiceError::Corrupt("Satisfied A has no terminal".into())
            })?;
            if stored_goal.stream_revision != prepared.expected_stream_revision
                || stored_goal.goal.scope != prepared.goal.scope
                || stored_goal.goal.session_id != prepared.goal.session_id
                || stored_goal.goal.objective != prepared.goal.objective
                || stored_goal.goal.spec_revision != prepared.goal.spec_revision
                || stored_goal.goal.spec_digest != prepared.goal.spec_digest
                || stored_goal.goal.user_sequence != prepared.goal.user_sequence
                || stored_goal.goal.reviews != prepared.goal.reviews
                || stored_goal.goal.obligations != prepared.goal.obligations
                || stored_goal.goal.execution_binding != prepared.goal.execution_binding
                || stored_goal.goal.user_intent_criterion_id
                    != prepared.goal.user_intent_criterion_id
                || prepared.goal.user_intent_criterion_id.is_none()
                || crate::execution_core::goal::goal_spec_digest(&stored_goal.goal)
                    != crate::execution_core::goal::goal_spec_digest(&prepared.goal)
                || prepared.goal.revision != stored_goal.goal.revision.saturating_add(1)
                || terminal.kind != harness_contract::goal::ObjectiveTerminalKind::Satisfied
                || terminal.authority_revision != request.program_revision
                || terminal.terminal_fence
                    != format!(
                        "agentic-objective:{}:request:{}",
                        projection.program_id, request.program_revision
                    )
                || prepared.event.event.stream_id != format!("goal:{}", prepared.goal.id)
                || prepared.event.event.payload["goal"] != serde_json::to_value(&prepared.goal)?
            {
                return Err(AgentActionServiceError::Corrupt(
                    "prepared Goal, review or request identity changed before A".into(),
                ));
            }
            for source in &prepared.policy_sources {
                let revision = if lease_rebased && source.stream_id == own_stream {
                    if source.expected_revision != original_revision {
                        return Err(AgentActionServiceError::EventStore(
                            "own Program prepared cut is inconsistent".into(),
                        ));
                    }
                    projection.revision
                } else {
                    source.expected_revision
                };
                if let Some(previous) = current_sources.insert(source.stream_id.clone(), revision) {
                    if previous != revision {
                        return Err(RuntimeEventStoreError::StaleRevision {
                            stream_id: source.stream_id.clone(),
                            expected: revision,
                            actual: previous,
                        }
                        .into());
                    }
                }
            }
            prepared.policy_sources = current_sources
                .into_iter()
                .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                })
                .collect();
        }
        let request = projection
            .completion_request
            .as_ref()
            .ok_or_else(|| AgentActionServiceError::Corrupt("missing completion request".into()))?;
        let program_stream_id = program_stream(&projection.program_id);
        let goal_stream_id = prepared.event.event.stream_id.clone();
        let (kind, status, payload) = if let Some(terminal) = prepared.goal.terminal.as_ref() {
            let verdict = super::program::AgenticObjectiveVerdictProjection {
                goal_id: prepared.goal.id.clone(),
                goal_revision: prepared.goal.revision,
                terminal_fence: terminal.terminal_fence.clone(),
                authority_revision: terminal.authority_revision,
                kind: terminal.kind,
            };
            (
                OBJECTIVE_VERDICT_EVENT_KIND,
                "verified",
                json!({"program_id": projection.program_id, "verdict": verdict}),
            )
        } else {
            (
                COMPLETION_REOPENED_EVENT_KIND,
                "open",
                json!({
                    "program_id": projection.program_id,
                    "request_revision": request.program_revision,
                    "gaps": prepared.gaps,
                }),
            )
        };
        let mut expected_streams = prepared.policy_sources;
        expected_streams.extend([
            ExpectedStreamRevision {
                stream_id: program_stream_id.clone(),
                expected_revision: projection.revision,
            },
            ExpectedStreamRevision {
                stream_id: goal_stream_id,
                expected_revision: prepared.expected_stream_revision,
            },
        ]);
        let mut unique_sources = std::collections::BTreeMap::new();
        for source in expected_streams {
            if unique_sources
                .insert(source.stream_id, source.expected_revision)
                .is_some_and(|previous| previous != source.expected_revision)
            {
                return Err(AgentActionServiceError::EventStore(
                    "conclusion source changed before A".into(),
                ));
            }
        }
        let expected_streams = unique_sources
            .into_iter()
            .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                stream_id,
                expected_revision,
            })
            .collect();
        let transaction = AppendTransactionRequest {
            transaction_id: format!(
                "program-conclusion:{}:{}",
                projection.program_id, request.program_revision
            ),
            expected_streams,
            events: vec![
                prepared.event,
                RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: program_stream_id,
                        scope: RuntimeEventScope::Program,
                        kind: kind.into(),
                        status: Some(status.into()),
                        actor: Some("runtime.objective_supervisor".into()),
                        refs: vec![RuntimeEventRef {
                            kind: "goal".into(),
                            id: prepared.goal.id,
                        }],
                        payload,
                    },
                    idempotency_key: Some(format!(
                        "program-conclusion:{}",
                        request.program_revision
                    )),
                    schema_version: 1,
                },
            ],
        };
        let stream_ids = transaction
            .expected_streams
            .iter()
            .map(|source| source.stream_id.clone())
            .collect::<Vec<_>>();
        #[cfg(test)]
        CONCLUSION_APPEND_HOOK.with(|slot| {
            let hook = slot.borrow_mut().take();
            if let Some(hook) = hook {
                hook(&transaction);
            }
        });
        self.store.with_stream_locks(&stream_ids, || {
            let stream_id = program_stream(&projection.program_id);
            let actual = self.store.stream_revision(&stream_id)?;
            if actual != projection.revision {
                return Err(RuntimeEventStoreError::StaleRevision {
                    stream_id,
                    expected: projection.revision,
                    actual,
                }
                .into());
            }
            self.store.append_transaction_locked(transaction)?;
            Ok::<_, AgentActionServiceError>(())
        })?;
        self.project(&projection.program_id)
    }

    /// Bind the only authoritative Objective verdict back into Program truth.
    /// The caller cannot provide a boolean: this method validates the complete
    /// GoalStore terminal, its root identity and the exact request revision.
    pub(crate) fn bind_objective_verdict(
        &self,
        program_id: &str,
        goal: &harness_contract::goal::GoalContract,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        let projection = self.project(program_id)?;
        self.ensure_recovery_session_admitted(&projection.session_id)?;
        let Some(request) = projection.completion_request.as_ref() else {
            return Err(AgentActionServiceError::Corrupt(
                "objective_verdict_without_completion_request".to_string(),
            ));
        };
        let root_execution_id = projection.root_execution_id.as_deref().ok_or_else(|| {
            AgentActionServiceError::Corrupt("objective_verdict_without_root_execution".to_string())
        })?;
        let expected_goal_id = format!("goal:{root_execution_id}");
        let terminal = goal.terminal.as_ref().ok_or_else(|| {
            AgentActionServiceError::Corrupt("objective_verdict_is_not_terminal".to_string())
        })?;
        let terminal_matches_completion = matches!(
            (terminal.kind, goal.completion),
            (
                harness_contract::goal::ObjectiveTerminalKind::Satisfied,
                harness_contract::goal::GoalCompletion::Satisfied
            ) | (
                harness_contract::goal::ObjectiveTerminalKind::PartiallySatisfied,
                harness_contract::goal::GoalCompletion::Partial
            ) | (
                harness_contract::goal::ObjectiveTerminalKind::Blocked,
                harness_contract::goal::GoalCompletion::Blocked
            ) | (
                harness_contract::goal::ObjectiveTerminalKind::Failed,
                harness_contract::goal::GoalCompletion::Failed
            ) | (
                harness_contract::goal::ObjectiveTerminalKind::Cancelled,
                harness_contract::goal::GoalCompletion::Cancelled
            )
        );
        let expected_fence = format!(
            "agentic-objective:{program_id}:request:{}",
            request.program_revision
        );
        if goal.id != expected_goal_id
            || goal.session_id != projection.session_id
            || goal.execution_binding.as_ref().is_none_or(|binding| {
                binding.objective_id != projection.objective_id
                    || binding.session_id != projection.session_id
                    || binding.turn_id != projection.turn_id
                    || binding.root_execution_id != root_execution_id
                    || binding.agentic_program_id != program_id
            })
            || terminal.terminal_fence != expected_fence
            || terminal.authority_revision != request.program_revision
            || !terminal_matches_completion
            || request
                .evidence_refs
                .iter()
                .any(|reference| !goal.evidence_refs.contains(reference))
            || !goal
                .evidence_refs
                .contains(&format!("execution_graph:{root_execution_id}"))
        {
            return Err(AgentActionServiceError::Corrupt(
                "objective_verdict_binding_mismatch".to_string(),
            ));
        }
        if let Some(existing) = projection.objective_verdict.as_ref() {
            if existing.terminal_fence == terminal.terminal_fence
                && existing.goal_revision == goal.revision
                && existing.kind == terminal.kind
            {
                return Ok(projection);
            }
            return Err(AgentActionServiceError::Corrupt(
                "objective_verdict_conflicts_with_bound_terminal".to_string(),
            ));
        }
        let verdict = super::program::AgenticObjectiveVerdictProjection {
            goal_id: goal.id.clone(),
            goal_revision: goal.revision,
            terminal_fence: terminal.terminal_fence.clone(),
            authority_revision: terminal.authority_revision,
            kind: terminal.kind,
        };
        let stream_id = program_stream(program_id);
        let actual_goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
            .projection(&goal.id)
            .map_err(AgentActionServiceError::EventStore)?
            .ok_or_else(|| {
                AgentActionServiceError::Corrupt("bound verdict Goal disappeared".into())
            })?;
        if actual_goal.goal != *goal {
            return Err(AgentActionServiceError::Corrupt(
                "bound verdict Goal cut changed".into(),
            ));
        }
        // A Program action applied after the Goal reached its terminal (a late
        // completion request is recorded but not adopted, for instance) advances
        // the Program stream without invalidating the terminal. The verdict is
        // fenced by the terminal fence and authority revision checked above, by
        // the fresh Goal cut, and by the proof source cuts below, so the carrier
        // cut is read at the current revision instead of freezing the entry-time
        // projection revision - which made any post-terminal Program event block
        // verdict binding permanently.
        let carrier_revision = self
            .store
            .stream_revision(&stream_id)
            .map_err(AgentActionServiceError::Store)?;
        let mut source_cuts = std::collections::BTreeMap::from([
            (stream_id.clone(), carrier_revision),
            (format!("goal:{}", goal.id), actual_goal.stream_revision),
        ]);
        let process_frame = self
            .process_input_verifier
            .as_ref()
            .map(super::review_evidence::ProcessInputVerifier::frame);
        if goal.completion == harness_contract::goal::GoalCompletion::Satisfied {
            let history = self
                .proof_session_queries
                .as_ref()
                .and_then(|queries| queries.get())
                .and_then(|queries| queries.history_reader());
            let readers = super::review_evidence::ResultContentReaders {
                artifacts: self.artifacts.as_deref(),
                history: history.as_deref(),
                process_inputs: process_frame.as_ref(),
            };
            for source in
                super::review_evidence::committed_proof_sources(&self.store, goal, readers)
                    .map_err(AgentActionServiceError::EventStore)?
            {
                if source_cuts
                    .insert(source.stream_id, source.expected_revision)
                    .is_some_and(|previous| previous != source.expected_revision)
                {
                    return Err(AgentActionServiceError::EventStore(
                        "bound verdict proof source changed".into(),
                    ));
                }
            }
            for condition in super::review_evidence::required_review_conditions(goal) {
                let proof = goal
                    .reviews
                    .iter()
                    .rev()
                    .find(|review| review.criterion_ref == condition)
                    .and_then(|review| review.verification.as_ref())
                    .ok_or_else(|| {
                        AgentActionServiceError::Corrupt(
                            "bound verdict requires current review".into(),
                        )
                    })?;
                let snapshot = super::review_evidence::validate_proof_inputs(
                    &self.store,
                    &projection,
                    Some(goal),
                    proof,
                    readers,
                )
                .map_err(AgentActionServiceError::EventStore)?;
                for source in snapshot.sources {
                    if source_cuts
                        .insert(source.stream_id, source.expected_revision)
                        .is_some_and(|previous| previous != source.expected_revision)
                    {
                        return Err(AgentActionServiceError::EventStore(
                            "bound verdict current proof cut changed".into(),
                        ));
                    }
                }
            }
        }
        self.store.append_transaction(AppendTransactionRequest {
            transaction_id: format!("agentic-objective-verdict:{program_id}:{}", goal.revision),
            expected_streams: source_cuts
                .into_iter()
                .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                })
                .collect(),
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Program,
                    kind: OBJECTIVE_VERDICT_EVENT_KIND.to_string(),
                    status: Some(
                        match terminal.kind {
                            harness_contract::goal::ObjectiveTerminalKind::Satisfied => "verified",
                            _ => "blocked",
                        }
                        .to_string(),
                    ),
                    actor: Some("runtime.objective_supervisor".to_string()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "goal".to_string(),
                            id: goal.id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "terminal_fence".to_string(),
                            id: terminal.terminal_fence.clone(),
                        },
                    ],
                    payload: json!({
                        "program_id": program_id,
                        "verdict": verdict,
                    }),
                },
                idempotency_key: Some(format!(
                    "agentic-objective-verdict:{}",
                    terminal.terminal_fence
                )),
                schema_version: 1,
            }],
        })?;
        self.project(program_id)
    }

    /// Resolve the already-frozen Program authority without treating a
    /// not-yet-opened Program as corruption. Root model continuations use
    /// this to inherit the first admitted binding instead of rebuilding
    /// policy and evidence scope from a later replan node.
    pub fn project_if_exists(
        &self,
        program_id: &str,
    ) -> Result<Option<AgenticProgramProjection>, AgentActionServiceError> {
        let stream_id = program_stream(program_id);
        if self.store.stream_revision(&stream_id)? == 0 {
            return Ok(None);
        }
        self.project(program_id).map(Some)
    }

    /// Enumerate every durable Agentic Program from the authoritative event
    /// store. This is the only supported cross-program carrier/projection
    /// source; callers must not reconstruct Team state from graph payloads.
    pub fn list_programs(&self) -> Result<Vec<AgenticProgramProjection>, AgentActionServiceError> {
        let mut programs = self
            .store
            .stream_ids_for_scope(RuntimeEventScope::Program)?
            .into_iter()
            .filter_map(|stream| stream.strip_prefix("agentic-program:").map(str::to_string))
            .filter_map(|program_id| self.project_if_exists(&program_id).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        programs.sort_by(|left, right| left.program_id.cmp(&right.program_id));
        Ok(programs)
    }

    /// Compact model-facing working set. Mutations return a summary rather
    /// than re-injecting the entire growing Program on every action;
    /// `state_inspect` can request one concrete Team/Agent/Task/Topic/Artifact
    /// or the whole projection when the model genuinely needs it.
    #[must_use]
    pub fn compact_projection(
        projection: &AgenticProgramProjection,
        scope_ref: Option<&str>,
    ) -> serde_json::Value {
        if let Some(scope_ref) = scope_ref {
            if let Some(team) = projection.teams.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "team": team,
                    "agents": team.member_ids.iter().take(STATE_INSPECT_PAGE_SIZE).filter_map(|id| projection.agents.get(id)).collect::<Vec<_>>(),
                    "tasks": team.task_ids.iter().take(STATE_INSPECT_PAGE_SIZE).filter_map(|id| projection.tasks.get(id)).collect::<Vec<_>>(),
                    "topics": projection.topics.get(&team.topic_ref).map(|entries| entries.iter().rev().take(STATE_INSPECT_PAGE_SIZE).collect::<Vec<_>>()),
                    "directory_request":{"query":team.team_id},
                    "coverage":{"nested_lists":"bounded_previews","complete":false,"instruction":"use directory_request for all related entries"},
                });
            }
            if let Some(agent) = projection.agents.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "agent": agent,
                });
            }
            if let Some(task) = projection.tasks.get(scope_ref) {
                let artifacts = task
                    .artifact_refs
                    .iter()
                    .filter_map(|id| projection.artifacts.get(id))
                    .collect::<Vec<_>>();
                // A reviewer must be able to read the submitted deliverable
                // without parsing nested artifact JSON or rebuilding an opaque
                // content hash. Hand back ready-to-read content references, so
                // `read("task://X")` is immediately followed by `read(<ref>)`.
                let read_requests = artifacts
                    .iter()
                    .map(|artifact| {
                        json!({
                            "artifact_ref": artifact.artifact_ref,
                            "read_request": { "evidence_ref": artifact.content_ref },
                        })
                    })
                    .collect::<Vec<_>>();
                let read_request = artifacts
                    .first()
                    .map(|artifact| json!({ "evidence_ref": artifact.content_ref }));
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "task": task,
                    "issues": super::issues::issues(projection).into_iter().filter(|issue| issue.source_ref == task.task_id).collect::<Vec<_>>(),
                    "artifacts": artifacts,
                    "read_requests": read_requests,
                    "read_request": read_request,
                });
            }
            if let Some(artifact) = projection.artifacts.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "artifact": artifact,
                    "read_request": { "evidence_ref": artifact.content_ref },
                });
            }
            if let Some(entries) = projection.topics.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "topic_ref": scope_ref,
                    "entries": entries.iter().rev().take(STATE_INSPECT_PAGE_SIZE).collect::<Vec<_>>(),
                    "directory_request":{"query":scope_ref},
                    "coverage":{"loaded":entries.len().min(STATE_INSPECT_PAGE_SIZE),"total":entries.len(),"complete":entries.len()<=STATE_INSPECT_PAGE_SIZE},
                });
            }
        }
        serde_json::to_value(projection).unwrap_or_else(
            |_| json!({"program_id": projection.program_id, "revision": projection.revision}),
        )
    }
}

impl AgentActionService {
    /// Recompute the gaps that still block completion from Goal truth.
    ///
    /// A reopen event records what one completion request was missing at one
    /// revision. Treating that record as current state strands the program: the
    /// model satisfies the criterion and records the review, and the framework
    /// keeps repeating `criterion_unsatisfied` while suppressing the exit
    /// action, so completion can never be requested again. Deriving the field
    /// from the Goal keeps the projection a projection of current facts.
    fn refresh_completion_gaps(&self, projection: &mut AgenticProgramProjection) {
        projection.unresolved = self.current_completion_gaps(projection);
    }

    fn current_completion_gaps(&self, projection: &AgenticProgramProjection) -> Vec<String> {
        let Some(root_execution_id) = projection.root_execution_id.as_deref() else {
            return Vec::new();
        };
        let goal_store = crate::execution_core::GoalStore::new(Arc::clone(&self.store));
        let Ok(Some(goal)) = goal_store.projection(&format!("goal:{root_execution_id}")) else {
            return Vec::new();
        };
        completion_gaps_from_goal(projection, Some(&goal.goal))
    }
}

fn completion_gaps_from_goal(
    projection: &AgenticProgramProjection,
    goal: Option<&harness_contract::goal::GoalContract>,
) -> Vec<String> {
    let Some(goal) = goal else {
        return Vec::new();
    };
    if goal.completion != harness_contract::goal::GoalCompletion::Open {
        // The Goal already reached a terminal completion; nothing is pending.
        return Vec::new();
    }
    let satisfied = crate::agentic::ingress::current_reviews_satisfy_the_goal(&goal);
    let uncovered = crate::agentic::ingress::uncovered_delivered_result_refs(projection, &goal);
    crate::agentic::ingress::objective_completion_waiting_gap(
        goal.waiting.as_ref(),
        satisfied,
        &uncovered,
    )
    .map(|gap| vec![gap])
    .unwrap_or_default()
}

fn projection_from_opened(
    program_id: &str,
    opened: &crate::DurableRuntimeEvent,
) -> Result<AgenticProgramProjection, AgentActionServiceError> {
    let objective_id = opened
        .payload
        .get("objective_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            AgentActionServiceError::Corrupt("program_opened_missing_objective".to_string())
        })?;
    let mut projection = AgenticProgramProjection::empty(program_id, objective_id);
    projection.session_id = opened
        .payload
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    projection.turn_id = opened
        .payload
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    projection.root_execution_id = opened
        .payload
        .get("root_execution_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    projection.required_team_count = opened
        .payload
        .get("required_team_count")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or_default();
    projection.exact_team_count = opened
        .payload
        .get("exact_team_count")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u8::try_from(value).ok());
    projection.objective_summary = opened
        .payload
        .get("objective_summary")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    projection.model_lease = opened
        .payload
        .get("model_lease")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("default")
        .to_string();
    projection.permission_ceiling = opened
        .payload
        .get("permission_ceiling")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or(harness_contract::policy::PermissionMode::ReadOnly);
    projection.resource_scopes = opened
        .payload
        .get("resource_scopes")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    Ok(projection)
}

fn apply_projection_events(
    projection: &mut AgenticProgramProjection,
    events: Vec<crate::DurableRuntimeEvent>,
) -> Result<(), AgentActionServiceError> {
    for event in events {
        if event.sequence <= projection.revision {
            continue;
        }
        if event.kind == PROGRAM_OPENED_EVENT_KIND {
            if let Some(seed) = event.payload.get("continuation_seed") {
                let inherited: AgenticProgramProjection = serde_json::from_value(seed.clone())?;
                if inherited.program_id != projection.program_id
                    || inherited.objective_id != projection.objective_id
                    || inherited.continuation.is_none()
                    || event.sequence != 1
                {
                    return Err(AgentActionServiceError::Corrupt(
                        "invalid continuation seed".into(),
                    ));
                }
                *projection = inherited;
            }
            projection.revision = event.sequence;
            continue;
        }
        if event.kind == REVIEW_DISCIPLINE_CONFLICT_EVENT_KIND {
            // Derived, never stored: `program_warnings` is a fold of the durable
            // journal, exactly like the authoring-side warning. A reviewer that
            // tried a reading the operator ruled out stays visible for the rest of
            // the Program's life, so the root can weigh later rework accordingly
            // and an operator can see why work kept going back and forth.
            let task_ref = event
                .payload
                .get("task_ref")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let discipline = event
                .payload
                .get("discipline")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if !task_ref.is_empty() && !discipline.is_empty() {
                let warning = format!(
                    "Historical review discipline refusal for Task `{task_ref}`: \
                     {discipline}. This legacy diagnostic is not a current review rule."
                );
                if !projection.program_warnings.contains(&warning) {
                    projection.program_warnings.push(warning);
                }
            }
            projection.revision = event.sequence;
            continue;
        }
        if event.kind == OBJECTIVE_VERDICT_EVENT_KIND {
            let verdict =
                serde_json::from_value::<super::program::AgenticObjectiveVerdictProjection>(
                    event.payload.get("verdict").cloned().ok_or_else(|| {
                        AgentActionServiceError::Corrupt(
                            "objective_verdict_event_missing_verdict".to_string(),
                        )
                    })?,
                )?;
            projection.apply_objective_verdict(verdict, event.sequence);
            continue;
        }
        if event.kind == COMPLETION_REOPENED_EVENT_KIND {
            let request_revision = event
                .payload
                .get("request_revision")
                .and_then(serde_json::Value::as_u64);
            if projection
                .completion_request
                .as_ref()
                .map(|request| request.program_revision)
                != request_revision
            {
                return Err(AgentActionServiceError::Corrupt(
                    "completion reopen request fence mismatch".into(),
                ));
            }
            projection.status = super::program::AgenticProgramStatus::Open;
            projection.completion_request = None;
            projection.final_artifact_ref = None;
            // The reopen event records what THIS request was missing at THIS
            // revision. That is history: the model may have closed every one of
            // those gaps a revision later. Keep the record for audit and let the
            // caller supply the gaps that still hold, so a satisfied review
            // clears the blockage instead of being contradicted forever.
            projection.last_reopen_gaps = serde_json::from_value(
                event
                    .payload
                    .get("gaps")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            )?;
            // Recomputed from Goal truth by the caller once the fold finishes.
            projection.unresolved = Vec::new();
            projection.revision = event.sequence;
            continue;
        }
        if event.kind == TASK_LEASE_RENEWED_EVENT_KIND {
            let task_ref = event
                .payload
                .get("task_ref")
                .and_then(serde_json::Value::as_str);
            let agent_id = event
                .payload
                .get("agent_id")
                .and_then(serde_json::Value::as_str);
            let execution_id = event
                .payload
                .get("execution_id")
                .and_then(serde_json::Value::as_str);
            if let Some(task) = task_ref.and_then(|task_ref| projection.tasks.get_mut(task_ref)) {
                let same_owner = task.status == AgenticTaskStatus::Claimed
                    && task.claimant.as_deref() == agent_id
                    && task.claim_execution_id.as_deref() == execution_id;
                if same_owner {
                    task.lease_expires_at_ms = Some(
                        event
                            .created_at_ms
                            .saturating_add(super::work_market::claim_lease_ms()),
                    );
                }
            }
            projection.revision = event.sequence;
            continue;
        }
        if event.kind != ACTION_EVENT_KIND {
            projection.revision = event.sequence;
            continue;
        }
        let envelope: AgentActionEnvelope =
            serde_json::from_value(event.payload.get("envelope").cloned().ok_or_else(|| {
                AgentActionServiceError::Corrupt("action_event_missing_envelope".to_string())
            })?)?;
        let entity_ref = event
            .payload
            .get("entity_ref")
            .and_then(serde_json::Value::as_str);
        projection.apply(&envelope, entity_ref, event.sequence, event.created_at_ms);
    }
    Ok(())
}

fn is_durable_evidence_ref(reference: &str) -> bool {
    let reference = reference.trim();
    ["tool://", "artifact://"]
        .iter()
        .any(|prefix| reference.starts_with(prefix) && reference.len() > prefix.len())
        && !reference.chars().any(char::is_whitespace)
}

fn actor_agent_id(envelope: &AgentActionEnvelope) -> Option<&str> {
    envelope
        .actor
        .agent_id
        .as_deref()
        .or(Some(envelope.actor.actor_id.as_str()))
}

fn action_refs(envelope: &AgentActionEnvelope, entity_ref: Option<&str>) -> Vec<RuntimeEventRef> {
    let mut refs = vec![
        RuntimeEventRef {
            kind: "objective".to_string(),
            id: envelope.actor.objective_id.clone(),
        },
        RuntimeEventRef {
            kind: "program".to_string(),
            id: envelope.actor.program_id.clone(),
        },
        RuntimeEventRef {
            kind: "actor".to_string(),
            id: envelope.actor.actor_id.clone(),
        },
        RuntimeEventRef {
            kind: "session".to_string(),
            id: envelope.actor.session_id.clone(),
        },
        RuntimeEventRef {
            kind: "turn".to_string(),
            id: envelope.actor.turn_id.clone(),
        },
    ];
    if let Some(root_execution_id) = &envelope.actor.root_execution_id {
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: root_execution_id.clone(),
        });
    }
    if let Some(entity_ref) = entity_ref {
        refs.push(RuntimeEventRef {
            kind: entity_ref.split(':').next().unwrap_or("entity").to_string(),
            id: entity_ref.to_string(),
        });
    }
    if let AgentAction::TaskSupersede(input) = &envelope.action {
        refs.extend(
            input
                .replacement_task_refs
                .iter()
                .map(|id| RuntimeEventRef {
                    kind: "replacement_task".to_string(),
                    id: id.clone(),
                }),
        );
        refs.extend(input.evidence_refs.iter().map(|id| RuntimeEventRef {
            kind: "evidence".to_string(),
            id: id.clone(),
        }));
    }
    if let AgentAction::TaskWithdraw(input) = &envelope.action {
        refs.push(RuntimeEventRef {
            kind: "task".to_string(),
            id: input.task_ref.clone(),
        });
        refs.extend(input.evidence_refs.iter().map(|id| RuntimeEventRef {
            kind: "evidence".to_string(),
            id: id.clone(),
        }));
    }
    if let AgentAction::TaskAttemptDispatch(input) = &envelope.action {
        refs.push(RuntimeEventRef {
            kind: "task".to_string(),
            id: input.task_ref.clone(),
        });
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: input.execution_id.clone(),
        });
    }
    refs
}

fn entity_ref(envelope: &AgentActionEnvelope) -> Option<String> {
    if let AgentAction::AgentInvite(input) = &envelope.action {
        if let Some(existing_agent_ref) = input.existing_agent_ref.as_ref() {
            return Some(existing_agent_ref.clone());
        }
    }
    // A claim, a submission and a review each settle one Task. They name that Task
    // in their input, so the entity they changed is the Task itself - the same
    // treatment `TaskSupersede` and `TaskWithdraw` already get. Without this, the
    // three actions that constitute the substance of a delegated Task produced an
    // empty `changed_refs`, and the progress measure that reads it could never see
    // them: in one live run 30 of 31 tool-batch observations reported no verified
    // gain while their own summaries reported new coverage, and the turn was ended
    // by the no-progress fuse after 21 replans that contradicted their observations.
    let prefix = match envelope.action {
        AgentAction::TeamCreate(_) => "team",
        AgentAction::AgentInvite(_) => "agent",
        AgentAction::TaskPublish(_) => "task",
        AgentAction::TaskClaim(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskSubmit(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskReview(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskSupersede(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskWithdraw(ref input) => return Some(input.task_ref.clone()),
        AgentAction::MessagePublish(_) => "message",
        AgentAction::ArtifactCommit(_) => "artifact",
        _ => return None,
    };
    let digest = Sha256::digest(
        format!(
            "{}|{}|{}|{}",
            envelope.actor.program_id,
            envelope.actor.actor_id,
            envelope.action_id,
            envelope.action.kind()
        )
        .as_bytes(),
    );
    let encoded = format!("{digest:x}");
    Some(format!("{prefix}:{}", &encoded[..24]))
}

fn applied_observation(
    envelope: &AgentActionEnvelope,
    projection: &AgenticProgramProjection,
    entity_ref: Option<String>,
    duplicate: bool,
) -> Result<AgentActionObservation, AgentActionServiceError> {
    let artifact_access = entity_ref
        .as_ref()
        .and_then(|reference| projection.artifacts.get(reference))
        .map(|artifact| {
            json!({
                "artifact_ref": artifact.artifact_ref,
                "content_ref": artifact.content_ref,
                "read_tool": "evidence_retrieve",
                "read_request": { "evidence_ref": artifact.content_ref },
                "submit_ref": artifact.artifact_ref,
            })
        });
    let changed_refs = entity_ref.into_iter().collect::<Vec<_>>();
    Ok(AgentActionObservation {
        receipt_id: format!(
            "receipt:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision: projection.revision,
        status: AgentActionStatus::Applied,
        duplicate,
        changed_refs,
        actionable: actionable_for_actor(projection, &envelope.actor, duplicate),
        projection: Some(json!({
            "program_id": projection.program_id,
            "objective_id": projection.objective_id,
            "revision": projection.revision,
            "status": projection.status,
            "counts": {
                "teams": projection.teams.len(),
                "agents": projection.agents.len(),
                "tasks": projection.tasks.len(),
                "artifacts": projection.artifacts.len(),
            },
            "final_artifact_ref": projection.final_artifact_ref,
            "unresolved": projection.unresolved,
            "artifact_access": artifact_access,
        })),
        error: None,
    })
}

pub(super) fn readable_topic_refs(
    projection: &AgenticProgramProjection,
    agent_id: &str,
    team_id: &str,
) -> std::collections::BTreeSet<String> {
    std::iter::once(format!("topic:{}", projection.program_id))
        .chain(
            projection
                .teams
                .get(team_id)
                .filter(|_| projection.agent_is_active_in(agent_id, team_id))
                .map(|team| team.topic_ref.clone()),
        )
        .collect()
}
pub(super) fn topic_entry_visible(entry: &AgenticTopicEntryProjection, agent_id: &str) -> bool {
    entry.actor_id == agent_id
        || entry.recipients.is_empty()
        || entry
            .recipients
            .iter()
            .any(|recipient| recipient == agent_id)
}

/// Remove the fields that describe the read, not the thing that was read.
///
/// A `state_inspect` directory page carries the program revision on the envelope,
/// on every entry, and in the program-wide `counts` block. None of those say whether
/// the entity the model asked about moved - a Task stays `published` while the
/// program advances elsewhere, and the entry's `revision` still climbs. Comparing the
/// raw page therefore never sees a stall in exactly the case a model polls one Task
/// while other work proceeds, which is the commonest loop of all.
fn strip_envelope_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("revision");
            object.remove("next_rungs");
            object.remove("counts");
            object.remove("coverage");
            // Caller identity, not entity state. The receipt echoes the action id
            // and receipt id of whoever asked, so two identical questions produce
            // different bytes and an unchanged entity reads as a change.
            object.remove("action_id");
            object.remove("receipt_id");
            object.remove("actionable");
            for nested in object.values_mut() {
                strip_envelope_fields(nested);
            }
        }
        serde_json::Value::Array(entries) => {
            for nested in entries.iter_mut() {
                strip_envelope_fields(nested);
            }
        }
        _ => {}
    }
}

fn inspected_observation(
    store: &RuntimeEventStore,
    envelope: &AgentActionEnvelope,
    projection: &AgenticProgramProjection,
    goal: Option<&harness_contract::goal::GoalContract>,
) -> AgentActionObservation {
    let input = match &envelope.action {
        AgentAction::StateInspect(input) => input,
        _ => unreachable!("only state_inspect reaches inspected_observation"),
    };
    let reader = format!(
        "{:?}:{}:{:?}:{:?}",
        envelope.actor.kind,
        envelope.actor.actor_id,
        envelope.actor.execution_id,
        envelope.actor.team_id
    );
    let topic_scope = if matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) {
        let Some(agent_id) = envelope
            .actor
            .agent_id
            .as_deref()
            .filter(|id| projection.agents.contains_key(*id))
        else {
            return rejected(
                envelope,
                projection.revision,
                "reader_not_authorized",
                "reader is not in the current Program roster",
            );
        };
        let Some(team_id) = envelope.actor.team_id.as_deref() else {
            return rejected(
                envelope,
                projection.revision,
                "reader_not_authorized",
                "reader has no bound Team",
            );
        };
        Some((agent_id, readable_topic_refs(projection, agent_id, team_id)))
    } else {
        None
    };
    let unchanged = goal.is_none()
        && input.after_revision == Some(projection.revision)
        && input.entry_ref.is_none()
        && input.scope_ref.is_none()
        && input.page_cursor.is_none()
        && input.query.is_none();
    let mut value = if unchanged {
        json!({
            "program_id": projection.program_id,
            "revision": projection.revision,
            "unchanged": true,
        })
    } else {
        match inspect_projection_page(projection, input, &reader, goal, topic_scope.as_ref()) {
            Ok(value) => value,
            Err(error) => {
                return rejected(
                    envelope,
                    projection.revision,
                    "invalid_state_cursor",
                    &error,
                );
            }
        }
    };
    // Name the outstanding rungs on a program-level read, from the same function
    // the barrier checkpoint renders them from.
    //
    // `state_inspect` used to return bounded state pages and nothing else, so the
    // one authoritative statement of what to do next existed only on the barrier
    // path. A root that polled instead of waiting was therefore told that a rung
    // was owed - the model-step invariant forces a tool call whenever one is -
    // without ever being told which. One live run, forced to call something and
    // given nothing to aim at, issued the same generic `{"query": "task"}` five
    // times while two of its Tasks sat Submitted and unreviewed, and its next rung
    // was `independent_review` the whole time.
    //
    // Not attached to the `unchanged` receipt: that path exists to stay a few
    // bytes, and an unchanged revision means unchanged rungs, which the reader
    // already has from the page that produced this revision.
    if !unchanged && input.entry_ref.is_none() && input.scope_ref.is_none() {
        if let Some(object) = value.as_object_mut() {
            let rungs =
                serde_json::Value::Array(crate::host::closure_rungs_for_program(projection));
            if matches!(
                envelope.actor.kind,
                AgentActorKind::Agent | AgentActorKind::TeamLead
            ) {
                object.insert(
                    "program_progress".to_string(),
                    json!({
                        "owner": "RootTurn",
                        "next_rungs": rungs,
                        "role_note": "Program-wide closure is read-only background, not an assignment. Continue only the Task, review, or coordination work bound to this execution.",
                    }),
                );
            } else {
                object.insert("next_rungs".to_string(), rungs);
            }
        }
    }
    // Say the cheap way to ask, at the moment asking again is pointless.
    //
    // A model waiting for a worker polls, and every poll pulls the whole state back
    // into context. Because the state carries the program revision the bytes are
    // never identical, so the duplicate-result stub never fires: one live run issued
    // the same `state_inspect` on one `task_ref` eleven times and another ten, each
    // time re-sending the full state.
    //
    // The revision is Runtime's own progress signal, so "same input, same revision"
    // is the honest observation, and the notice names the two parameters that already
    // exist to make the next ask cheap. It does not refuse, stub, or narrow anything.
    // Program-level reads only. An exact `entry_ref`/`scope_ref` asks about one
    // entity, and re-asking is legitimate: whether a topic became visible, whether a
    // Task moved, whether a ref resolved are all questions whose answer can change
    // without the program revision moving. One live test asserts exactly that - a
    // cross-team read answering `not_found` - and collapsing it into an unchanged
    // receipt would hide the answer.
    if !unchanged && input.entry_ref.is_none() && input.scope_ref.is_none() {
        let input_hash =
            crate::tool_invocation::stable_hash(&serde_json::to_string(input).unwrap_or_default());
        // The token is what was actually asked about, not the program revision.
        //
        // Keying on the revision missed the commonest loop: a model polling
        // `state_inspect {"query": "task:X"}` while the program advances elsewhere.
        // The revision moves, so the check never fires, and the same query is
        // re-issued against a Task that has not moved at all - one live run did
        // exactly that and was stopped for spinning with this shape in the evidence.
        //
        // So the token is a digest of the answer with the envelope's `revision`
        // stripped out. What remains is the entity content, and if that is identical
        // the question was answered identically, whatever the rest of the program did.
        let mut answer = value.clone();
        strip_envelope_fields(&mut answer);
        let state_token = serde_json::to_string(&answer).unwrap_or_default();
        let inspection_scope = envelope.actor.execution_id.as_deref().map_or_else(
            || store.repetition_scope(&envelope.actor.session_id, &envelope.actor.turn_id),
            |execution_id| store.repetition_execution_scope(&envelope.actor.session_id, execution_id),
        );
        if crate::repetition_guard::record_unchanged_inspection(
            &inspection_scope,
            &input_hash,
            &state_token,
        ) {
            // Answer the question that was asked, not the bytes that were asked for.
            //
            // A notice alone was tried first and measured: it reached the model
            // twenty-two times in one live run and the share of calls that repeated an
            // actor's own exact input did not move. Telling a model that polling is
            // expensive is advice; the model has already decided what it needs by the
            // time it reads the result.
            //
            // At an unchanged revision the honest answer to "what is the state" is
            // "the state you already have" - which is exactly what `after_revision`
            // would have returned. Runtime serves that instead of re-sending the whole
            // page, and names the ways to get more: a newer `after_revision`, a
            // different query, or one exact `entry_ref`.
            let revision = i64::try_from(projection.revision).unwrap_or(i64::MAX);
            value = json!({
                "program_id": projection.program_id,
                "revision": revision,
                "unchanged": true,
                "notice": "Runtime answered this with the same receipt `after_revision` \
                           would have returned: the program revision has not moved since \
                           your last identical query, so the state you already hold is \
                           current. To wait cheaply, re-issue with \
                           `after_revision: <revision>`, or with `wait_for_workers: true` \
                           to park until a worker moves. For something specific, read one \
                           exact `entry_ref` or ask a different query - a broad listing \
                           is only re-sent when the revision actually advances."
            });
        }
    }
    AgentActionObservation {
        receipt_id: format!(
            "observation:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision: projection.revision,
        status: AgentActionStatus::Observed,
        duplicate: false,
        changed_refs: Vec::new(),
        actionable: actionable_for_actor(projection, &envelope.actor, false),
        projection: Some(value),
        error: None,
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StateInspectCursor {
    reader: String,
    version: u8,
    program_id: String,
    revision: u64,
    #[serde(default)]
    goal_revision: Option<u64>,
    query: Option<String>,
    after_ref: String,
}

const STATE_INSPECT_PAGE_SIZE: usize = 32;

fn goal_summary(goal: &harness_contract::goal::GoalContract) -> serde_json::Value {
    json!({"goal_id":goal.id,"revision":goal.revision,"spec_revision":goal.spec_revision,
        "spec_digest":goal.spec_digest,"source_intent_ref":goal.source_intent_ref,"completion":goal.completion,
        "participation_requirement":goal.participation_requirement,
        "criteria_count":goal.criteria.len(),"obligation_count":goal.obligations.len(),"review_count":goal.reviews.len(),
        "inspect_request":{"entry_ref":goal.id}})
}

fn goal_directory_entries(goal: &harness_contract::goal::GoalContract) -> Vec<serde_json::Value> {
    std::iter::once(json!({"entry_ref":goal.id,"goal_id":goal.id,"kind":"goal",
        "label":goal.objective.chars().take(480).collect::<String>()}))
        .chain(goal.criteria.iter().map(|criterion| json!({"entry_ref":criterion.id,"goal_id":goal.id,
            "kind":"criterion","label":criterion.statement.chars().take(480).collect::<String>(),
            "status":criterion.status,"statement_ref":criterion.statement_ref})))
        .chain(goal.obligations.iter().map(|obligation| json!({"entry_ref":obligation.obligation_id,"goal_id":goal.id,
            "kind":"obligation","label":obligation.success_predicate.chars().take(480).collect::<String>(),
            "state":obligation.state,"required":obligation.required,
            "review_request":{"criterion_ref":obligation.obligation_id}})))
        .chain(goal.reviews.iter().map(|review| json!({"entry_ref":review.review_id,"goal_id":goal.id,
            "kind":"objective_review","criterion_ref":review.criterion_ref,"decision":review.decision,
            "spec_revision":review.spec_revision}))).collect()
}

/// Return a bounded navigation page or one exact entity.  The output never
/// contains an unbounded Program dump: models use the returned stable refs to
/// retrieve only the Task, Team, topic, or artifact relevant to their next
/// decision.  The cursor is a deterministic index over a revision snapshot,
/// so it is safe to discard after any revision change.
fn inspect_projection_page(
    projection: &AgenticProgramProjection,
    input: &harness_contract::agent_action::StateInspectInput,
    reader: &str,
    goal: Option<&harness_contract::goal::GoalContract>,
    topic_scope: Option<&(&str, std::collections::BTreeSet<String>)>,
) -> Result<serde_json::Value, String> {
    let readable_topic = |topic: &str| topic_scope.is_none_or(|(_, topics)| topics.contains(topic));
    let readable_entry = |entry: &AgenticTopicEntryProjection| {
        topic_scope.is_none_or(|(agent, _)| topic_entry_visible(entry, agent))
    };
    let exact_ref = input.entry_ref.as_deref().or(input.scope_ref.as_deref());
    if let Some(reference) = exact_ref {
        if let Some(goal) = goal {
            let value = if reference == goal.id {
                // Exact entity reads return the full objective. Directory pages keep
                // their bounded labels, but a goal read must not hide the requirement
                // behind the old 12000/4000 char thresholds.
                Some({
                    let mut summary = goal_summary(goal);
                    if let Some(object) = summary.as_object_mut() {
                        object.insert(
                            "objective".to_string(),
                            serde_json::Value::String(goal.objective.clone()),
                        );
                    }
                    summary
                })
            } else if let Some(criterion) = goal.criteria.iter().find(|item| item.id == reference) {
                Some(json!({"criterion":criterion}))
            } else if let Some(obligation) = goal
                .obligations
                .iter()
                .find(|item| item.obligation_id == reference)
            {
                Some(json!({"obligation":obligation}))
            } else {
                goal.reviews
                    .iter()
                    .find(|item| item.review_id == reference)
                    .map(|review| json!({"review":review}))
            };
            if let Some(value) = value {
                return Ok(
                    json!({"program_id":projection.program_id,"revision":projection.revision,
                    "goal_id":goal.id,"goal_revision":goal.revision,"spec_revision":goal.spec_revision,
                    "spec_digest":goal.spec_digest,"goal_entry":value}),
                );
            }
        }
        if let Some(membership) = projection.memberships.get(reference) {
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,"membership":membership}),
            );
        }
        if let Some((topic_ref, entry)) = projection
            .topics
            .iter()
            .filter(|(topic, _)| readable_topic(topic))
            .find_map(|(topic_ref, entries)| {
                entries
                    .iter()
                    .find(|entry| entry.entry_id == reference && readable_entry(entry))
                    .map(|entry| (topic_ref, entry))
            })
        {
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,"topic_ref":topic_ref,"entry":entry,
                "read_request":entry.content_ref.as_ref().map(|content| json!({"evidence_ref":content}))}),
            );
        }
        if let Some(issue) = super::issues::issues(projection)
            .into_iter()
            .find(|issue| issue.issue_ref == reference)
        {
            return Ok(
                json!({"program_id": projection.program_id, "revision": projection.revision, "issue": issue}),
            );
        }
        if let Some(entries) = projection
            .topics
            .get(reference)
            .filter(|_| readable_topic(reference))
        {
            let total = entries.iter().filter(|entry| readable_entry(entry)).count();
            let page = entries
                .iter()
                .rev()
                .filter(|entry| readable_entry(entry))
                .take(STATE_INSPECT_PAGE_SIZE)
                .collect::<Vec<_>>();
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,
                "topic_ref":reference,"entries":page,"directory_request":{"query":reference},
                "coverage":{"loaded":page.len(),"total":total,"complete":total<=STATE_INSPECT_PAGE_SIZE}}),
            );
        }
        if projection.teams.contains_key(reference)
            || projection.agents.contains_key(reference)
            || projection.tasks.contains_key(reference)
            || projection.artifacts.contains_key(reference)
        {
            return Ok(AgentActionService::compact_projection(
                projection,
                Some(reference),
            ));
        }
        return Ok(json!({
            "program_id": projection.program_id,
            "revision": projection.revision,
            "entry_ref": reference,
            "not_found": true,
        }));
    }
    let query = input
        .query
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(str::to_lowercase);
    let cursor = input
        .page_cursor
        .as_deref()
        .map(|cursor| {
            serde_json::from_str::<StateInspectCursor>(cursor).map_err(|_| {
                "invalid Program cursor; copy next_request from a fresh directory".to_string()
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.version != 1
            || cursor.reader != reader
            || cursor.program_id != projection.program_id
            || cursor.revision != projection.revision
            || cursor.goal_revision != goal.map(|goal| goal.revision)
            || cursor.query != query
    }) {
        return Err(
            "Program source, revision or query changed; restart directory discovery".into(),
        );
    }
    let directory = projection
        .teams
        .values()
        .map(|team| {
            json!({
                "entry_ref": team.team_id,
                "kind": "team",
                "label": team.name,
                "lifecycle": team.lifecycle,
            })
        })
        .chain(projection.agents.values().map(|agent| {
            json!({
                "entry_ref": agent.agent_id,
                "kind": "agent",
                "label": agent.role,
                "memberships": agent.membership_ids,
            })
        }))
        .chain(projection.tasks.values().map(|task| {
            json!({
                "entry_ref": task.task_id,
                "kind": "task",
                "label": task.title,
                "status": task.status,
                "team_ref": task.team_id,
                "active_attempt_count": task.active_attempts.len(),
            })
        }))
        .chain(projection.topics.keys().filter(|topic| readable_topic(topic)).map(|topic_ref| {
            json!({
                "entry_ref": topic_ref,
                "kind": "topic",
            })
        }))
        .chain(projection.memberships.values().map(|membership| json!({
            "entry_ref":membership.membership_id,"kind":"membership","agent_ref":membership.agent_id,"team_ref":membership.team_id,"lifecycle":membership.lifecycle
        })))
        .chain(projection.topics.iter().filter(|(topic, _)| readable_topic(topic)).flat_map(|(topic_ref,entries)| entries.iter().filter(|entry| readable_entry(entry)).map(move |entry| json!({
            "entry_ref":entry.entry_id,"kind":"topic_entry","topic_ref":topic_ref,"author":entry.actor_id,
            "label":entry.summary.as_deref().unwrap_or("").chars().take(480).collect::<String>(),"entry_revision":entry.revision
        }))))
        .chain(projection.artifacts.values().map(|artifact| {
            json!({
                "entry_ref": artifact.artifact_ref,
                "kind": "artifact",
                "label": artifact.title,
                "read_request": { "evidence_ref": artifact.content_ref },
            })
        }))
        .chain(super::issues::issues(projection).into_iter().map(|issue| json!({
            "entry_ref": issue.issue_ref, "kind": "issue", "label": issue.description,
            "source_ref": issue.source_ref, "disposition": issue.disposition,
            "unclassified": issue.disposition.is_none(),
        })))
        .chain(goal.into_iter().flat_map(goal_directory_entries));
    // Keep only the next page plus one look-ahead entry. Discovery can scan
    // metadata, but must not materialize the complete Program directory.
    let mut entries: Vec<serde_json::Value> = Vec::with_capacity(STATE_INSPECT_PAGE_SIZE + 1);
    let mut cursor_found = cursor.is_none();
    for entry in directory {
        if query
            .as_ref()
            .is_some_and(|query| !entry.to_string().to_lowercase().contains(query))
        {
            continue;
        }
        let reference = entry["entry_ref"].as_str().unwrap_or("");
        if let Some(cursor) = &cursor {
            cursor_found |= reference == cursor.after_ref;
            if reference <= cursor.after_ref.as_str() {
                continue;
            }
        }
        let position = entries.partition_point(|candidate| {
            candidate["entry_ref"].as_str().unwrap_or("") <= reference
        });
        if position <= STATE_INSPECT_PAGE_SIZE {
            entries.insert(position, entry);
            if entries.len() > STATE_INSPECT_PAGE_SIZE + 1 {
                entries.pop();
            }
        }
    }
    if !cursor_found {
        return Err("Program cursor position is not present in this directory".into());
    }
    let start: usize = 0;
    let end = start
        .saturating_add(STATE_INSPECT_PAGE_SIZE)
        .min(entries.len());
    let next_cursor = (end < entries.len())
        .then(|| {
            serde_json::to_string(&StateInspectCursor {
                version: 1,
                reader: reader.into(),
                program_id: projection.program_id.clone(),
                revision: projection.revision,
                goal_revision: goal.map(|goal| goal.revision),
                query: query.clone(),
                after_ref: entries[end - 1]["entry_ref"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            })
        })
        .transpose()
        .map_err(|error| error.to_string())?;
    for entry in &mut entries[start..end] {
        entry["source_kind"] = json!("program");
        entry["revision"] = json!(projection.revision);
        entry["scope"] = json!(projection.program_id);
        entry["inspect_request"] = json!({"entry_ref":entry["entry_ref"]});
    }
    Ok(json!({
        "program_id": projection.program_id,
        "objective_id": projection.objective_id,
        "continuation": projection.continuation,
        "revision": projection.revision,
        "status": projection.status,
        "counts": {
            "teams": projection.teams.len(),
            "agents": projection.agents.len(),
            "memberships": projection.memberships.len(),
            "tasks": projection.tasks.len(),
            "artifacts": projection.artifacts.len(),
        },
        "goal": goal.map(goal_summary),
        "entries": entries[start..end].to_vec(),
        "next_page_cursor": next_cursor,
        "next_request": next_cursor.as_ref().map(|cursor| {
            let mut request = json!({"page_cursor":cursor});
            if let Some(query) = &input.query { request["query"] = json!(query); }
            request
        }),
        "coverage": {"kind":"program_metadata_directory", "complete":next_cursor.is_none(), "revision":projection.revision},
    }))
}

fn rejected(
    envelope: &AgentActionEnvelope,
    revision: u64,
    code: &str,
    message: &str,
) -> AgentActionObservation {
    AgentActionObservation {
        receipt_id: format!(
            "rejection:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision,
        status: AgentActionStatus::Rejected,
        duplicate: false,
        changed_refs: Vec::new(),
        actionable: vec!["inspect current state and revise only this action".to_string()],
        projection: None,
        error: Some(AgentActionErrorObservation {
            code: code.to_string(),
            message: message.to_string(),
            recoverable: true,
        }),
    }
}

fn actionable_for_actor(
    projection: &AgenticProgramProjection,
    actor: &harness_contract::agent_action::AgentActorBinding,
    duplicate: bool,
) -> Vec<String> {
    if !matches!(actor.kind, AgentActorKind::Agent | AgentActorKind::TeamLead)
        || projection.status != super::program::AgenticProgramStatus::Open
    {
        return actionable(projection, duplicate);
    }
    let mut actions = Vec::new();
    if duplicate {
        actions.push("idempotent replay: no state was duplicated".to_string());
    }
    let bound_attempt = actor
        .execution_id
        .as_deref()
        .zip(actor.agent_id.as_deref())
        .and_then(|(execution_id, agent_id)| {
            projection.tasks.values().find_map(|task| {
                task.active_attempts
                    .get(execution_id)
                    .filter(|attempt| attempt.agent_id == agent_id)
                    .map(|attempt| (task, attempt))
            })
        });
    if let Some((task, attempt)) = bound_attempt {
        use harness_contract::agent_action::AgentAttemptMode;
        actions.push(match attempt.mode {
            AgentAttemptMode::Execute if task.purpose == harness_contract::agent_action::TaskPurpose::Exploration => format!(
                "Complete only your bound Task `{}`. {}",
                task.task_id,
                super::order_contract::EXPLORATION_CLOSEOUT_CONTRACT,
            ),
            AgentAttemptMode::Execute => format!(
                "Complete only your bound Task `{}`. Prefer one `task_submit` with `deliverable` selecting this response's report or the latest saved report from this same Task, Agent, claim and attempt. Use `artifact_commit` only for file-backed or multi-part deliverables. Include real durable evidence and disclose genuine gaps in `unresolved`.",
                task.task_id,
            ),
            AgentAttemptMode::Review => format!(
                "Read the submitted artifact for your bound Task `{}` and issue the independent `task_review` verdict. Do not execute or resubmit another Task.",
                task.task_id,
            ),
            AgentAttemptMode::Coordination => format!(
                "Respond only to the bound coordination work for Task `{}` using your authorized collaboration actions.",
                task.task_id,
            ),
        });
    }
    actions.push(
        "The root coordinator owns Program-level closure. Global Program progress is read-only background; follow only the Task, review, or coordination assignment in your private role brief."
            .to_string(),
    );
    actions
}

fn actionable(projection: &AgenticProgramProjection, duplicate: bool) -> Vec<String> {
    let mut actions = Vec::new();
    if duplicate {
        actions.push("idempotent replay: no state was duplicated".to_string());
    }
    match projection.status {
        super::program::AgenticProgramStatus::CompletionRequested => {
            actions.push(
                "completion request is durable; await the ObjectiveSupervisor verdict".to_string(),
            );
            return actions;
        }
        super::program::AgenticProgramStatus::Verified
        | super::program::AgenticProgramStatus::Partial
        | super::program::AgenticProgramStatus::Blocked
        | super::program::AgenticProgramStatus::Failed
        | super::program::AgenticProgramStatus::Cancelled => return actions,
        super::program::AgenticProgramStatus::Waiting => {
            actions.push("await the recorded external dependency or permission change".to_string());
            return actions;
        }
        super::program::AgenticProgramStatus::Draining => {
            actions
                .push("await cancellation and effect verification before finalizing".to_string());
            return actions;
        }
        super::program::AgenticProgramStatus::Open => {}
    }
    if !projection.unresolved.is_empty() {
        // `unresolved` is recomputed from Goal truth, so this names what is
        // still missing rather than what one reopen happened to record. The
        // reopen event's own wording is history and is shown separately:
        // presenting it as the current state is what once told a model to keep
        // resolving a criterion it had already satisfied.
        actions.push(format!("The previous completion request was reopened. Resolve the recorded gaps before requesting completion again: {}", projection.unresolved.join("; ")));
        if !projection.last_reopen_gaps.is_empty() {
            actions.push(format!(
                "For reference, the last reopen recorded: {}",
                projection.last_reopen_gaps.join("; ")
            ));
        }
        // The gap a completion request is reopened with is assembled by
        // `objective_completion_waiting_gap`, which prefixes the recorded
        // reason: `objective_completion_gap:original_intent_review_required:
        // user_intent; ...`. Matching the bare prefix therefore never fired,
        // and the one sentence that names the tool the model must call was
        // silently dropped - a live run cycled seven completion requests
        // without ever issuing a review, because the only instruction it got
        // was a gap string it could not act on. Match the marker wherever it
        // appears in the assembled gap.
        let review_required = projection.unresolved.iter().any(|gap| {
            gap.contains("original_intent_review_required:")
                || gap.contains("criterion_review_required:")
        });
        if review_required {
            // The how-to names the tool and the shape; the scope discipline is
            // shared with the terminal presentation and therefore comes from
            // the order contract, so a change cannot reach one layer and miss
            // the other.
            actions.push(format!(
                "Use objective_review for the original criterion with the current delivered result and durable evidence. {}",
                crate::agentic::order_contract::OBJECTIVE_REVIEW_SCOPE_DISCIPLINE
            ));
            // Name the exact refs that still have no review, so the model does
            // not have to parse them out of the gap string itself.
            let uncovered = projection
                .unresolved
                .iter()
                .filter_map(|gap| gap.split("uncovered_result_refs:").nth(1))
                .flat_map(|tail| tail.split(';').next().unwrap_or("").split(','))
                .map(|reference| reference.trim().to_string())
                .filter(|reference| !reference.is_empty())
                .collect::<Vec<_>>();
            if !uncovered.is_empty() {
                actions.push(format!(
                    "These delivered results have no review yet: {}. Read each one, then submit exactly one objective_review whose result_refs cover all of them.",
                    uncovered.join(", ")
                ));
            }
        }
        // A reopened Program must never also advertise the exit action in the
        // same receipt: one live run received "resolve the recorded gaps" and
        // "request Objective completion" together, re-requested completion, and
        // looped until the observation window expired. While gaps remain, the
        // gap guidance above is the entire instruction.
        return actions;
    }
    // No gap remains. The program was reopened at least once (history is
    // recorded), so the model needs to be told the exit is reachable again
    // rather than left to infer it.
    if !projection.last_reopen_gaps.is_empty() {
        actions.push(
            "Every gap recorded by the last reopen is now resolved; request Objective completion."
                .into(),
        );
    }
    // A Task no Agent will settle is a fact the root must be able to read. Runtime
    // used to keep it private in the dispatch starvation ledger, so the model had no
    // step and no explanation: in one live run a reworked Task was re-submitted and
    // never reviewed again, the root graph had no ready node, and the turn ended by
    // the no-progress fuse. Runtime states the fact; it does not choose the remedy.
    let stranded = crate::agentic::execution::dispatch::stranded_task_refs(projection);
    if !stranded.is_empty() {
        actions.push(format!(
            "These Tasks have no Agent able to progress them (Runtime could not dispatch an eligible member): {}. They remain open work in the Program. You can invite a reviewer, withdraw or supersede the Task, or record it as unresolved when requesting completion.",
            stranded.join(", ")
        ));
    }
    if projection.teams.is_empty() {
        if projection.artifacts.is_empty() {
            actions.push(super::order_contract::ARTIFACT_CONTENT_REF_CONTRACT.to_string());
        } else {
            actions.push(super::order_contract::OBJECTIVE_REVIEW_SCOPE_DISCIPLINE.to_string());
        }
    }
    // Authoring facts recorded when they were created. A model that invented an
    // acceptance standard sees it named as its own on every subsequent receipt,
    // which is the shortest correction loop available: the cost was paid at
    // authoring time, so the notice belongs there too.
    for warning in &projection.program_warnings {
        actions.push(warning.clone());
    }
    if projection
        .teams
        .values()
        .any(|team| team.member_ids.is_empty())
    {
        actions.push("invite an Agent into an unstaffed Team".to_string());
    }
    if projection.tasks.values().any(|task| {
        matches!(
            task.status,
            AgenticTaskStatus::Published | AgenticTaskStatus::Rework
        )
    }) {
        actions.push("claim an available Task".to_string());
    }
    if projection.tasks.values().any(|task| {
        task.status == AgenticTaskStatus::Published
            && (task.failed_attempts > 0 || task.failed_review_attempts > 0)
    }) {
        actions.push(
            "reassign the failed work, or publish concrete successor Task(s) and then call task_supersede with durable failure/replan evidence"
                .to_string(),
        );
    }
    // Make the execution Task's lowest-cost closeout the default. The previous text
    // listed several commit shapes without saying that one `task_submit` can bind a
    // same-attempt draft written in an earlier model step. That pushed a live Agent
    // through a separate `artifact_commit` and made a mistaken current-block index look
    // like a permanent delivery failure.
    let claimed = projection
        .tasks
        .values()
        .any(|task| task.status == AgenticTaskStatus::Claimed);
    if claimed
        || projection
            .tasks
            .values()
            .any(|t| t.status == AgenticTaskStatus::Submitted)
    {
        actions.push(format!(
            "For an Agent executing its bound claimed Task: {} Disclose genuine gaps in unresolved. An Agent bound in review mode must read the submitted artifact and issue task_review instead of resubmitting it.",
            crate::agentic::order_contract::TASK_REPORT_SUBMISSION_CONTRACT,
        ));
    }
    if !projection.artifacts.is_empty()
        && projection
            .tasks
            .values()
            .any(|task| task.status != AgenticTaskStatus::Superseded)
        && projection
            .tasks
            .values()
            .filter(|task| task.status != AgenticTaskStatus::Superseded)
            .all(|task| task.status == AgenticTaskStatus::Accepted)
    {
        actions.push("request Objective completion with the final artifact".to_string());
    }
    actions
}

fn program_stream(program_id: &str) -> String {
    format!("agentic-program:{program_id}")
}

fn topic_cursor_stream(program_id: &str, execution_id: &str) -> String {
    format!("agentic-topic-cursor:{program_id}:{execution_id}")
}

fn action_key(action_id: &str) -> String {
    format!("agent-action:{action_id}")
}

fn read_model_projection_id(program_id: &str) -> String {
    format!(
        "runtime:agentic-read-model:v1:{:x}",
        Sha256::digest(program_id.as_bytes())
    )
}

fn decode_read_snapshot(
    checkpoint: &crate::RuntimeProjectionCheckpoint,
    program_id: &str,
    store: &RuntimeEventStore,
) -> Option<AgenticProgramProjection> {
    let payload = &checkpoint.payload;
    if payload["schema_version"] != 2 {
        return None;
    }
    let session = payload["session_id"].as_str()?;
    let body = store
        .resolve_content(
            &format!("session:{session}"),
            &payload["body"],
            &crate::PreparedArtifactOrigin::CheckpointBody {
                projection_id: read_model_projection_id(program_id),
                source_cursor: checkpoint.source_cursor,
            },
        )
        .ok()?;
    let projection: AgenticProgramProjection = serde_json::from_value(body).ok()?;
    if projection.program_id != program_id
        || projection.revision != checkpoint.source_cursor
        || projection.session_id != session
    {
        return None;
    }
    Some(projection)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod native_retirement_guard_tests {
    use super::AgentActionService;
    use crate::execution_core::graph::executors::CompileTargetGuardExecutor;
    use crate::execution_core::graph::{
        ExecutionCompileRequest, ExecutionGraphCompiler, NodeExecutor,
    };
    use crate::execution_core::RuntimeCompileTarget;
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionParentBinding,
    };

    fn compiled_child(target: RuntimeCompileTarget) -> ExecutionGraph {
        let objective = "Real compiler Native retirement guards";
        let payload = serde_json::json!({
            "kind":"conversation_turn", "compile_target":target,
            "session_id":"guard-session", "objective":objective, "content":objective,
        })
        .to_string();
        let mut graph = ExecutionGraphCompiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: objective.into(),
                payload_ref: payload,
                target,
                resource_scopes: vec!["read:.".into(), "write:out/a".into()],
            })
            .unwrap();
        graph.service_class = harness_contract::execution_graph::ExecutionServiceClass::Foreground;
        graph.parent_execution = Some(ExecutionParentBinding {
            execution_id: "original-agent-graph".into(),
            node_id: "original-agent-node".into(),
        });
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "guard-session".into(),
            turn_id: "guard-turn".into(),
            root_task_id: "guard-root-task".into(),
            task_id: "guard-task".into(),
            generation: 1,
        });
        graph
    }

    #[test]
    fn r1_native_retirement_accepts_real_evidence_and_execution_compiler_guards() {
        for (target, criteria) in [
            (
                RuntimeCompileTarget::EvidenceGraph,
                vec!["evidence_read_before_synthesis"],
            ),
            (
                RuntimeCompileTarget::ExecutionGraph,
                vec![
                    "permission_and_policy_gate_required",
                    "mutation_resources_must_be_leased",
                ],
            ),
        ] {
            let original = compiled_child(target);
            let guards = original
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::Verify)
                .collect::<Vec<_>>();
            assert_eq!(guards.len(), criteria.len());
            for (guard, criterion) in guards.into_iter().zip(criteria) {
                assert_eq!(guard.executor_kind, CompileTargetGuardExecutor::KIND);
                assert_eq!(guard.acceptance.criteria, vec![criterion.to_string()]);
                CompileTargetGuardExecutor.validate(guard).unwrap();
                AgentActionService::validate_native_retirement_target_guard(
                    &original, &original, guard,
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn r1_native_retirement_rejects_foreign_compiler_guard_executor_criterion_and_owner() {
        for target in [
            RuntimeCompileTarget::EvidenceGraph,
            RuntimeCompileTarget::ExecutionGraph,
        ] {
            let original = compiled_child(target);
            let guards = original
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::Verify)
                .cloned()
                .collect::<Vec<_>>();
            for guard in guards {
                for mutation in [
                    "executor",
                    "legacy-verify",
                    "criterion",
                    "extra-criterion",
                    "parent",
                    "parent-graph",
                    "missing-owner",
                    "lineage",
                    "graph",
                    "node",
                    "payload",
                ] {
                    let mut before = original.clone();
                    let mut child = original.clone();
                    let index = child
                        .nodes
                        .iter()
                        .position(|node| node.id == guard.id)
                        .unwrap();
                    match mutation {
                        "executor" => child.nodes[index].executor_kind = "foreign-guard".into(),
                        "legacy-verify" => child.nodes[index].executor_kind = "verify".into(),
                        "criterion" => {
                            child.nodes[index].acceptance.criteria =
                                vec!["foreign-criterion".into()]
                        }
                        "extra-criterion" => child.nodes[index]
                            .acceptance
                            .criteria
                            .push("foreign-criterion".into()),
                        "parent" => child
                            .parent_execution
                            .as_mut()
                            .unwrap()
                            .node_id
                            .push_str(":foreign"),
                        "parent-graph" => child
                            .parent_execution
                            .as_mut()
                            .unwrap()
                            .execution_id
                            .push_str(":foreign"),
                        "missing-owner" => child.parent_execution = None,
                        "lineage" => child.lineage.as_mut().unwrap().generation += 1,
                        "graph" => child.id.push_str(":foreign"),
                        "node" => child.nodes[index].id.push_str(":foreign"),
                        "payload" => child.nodes[index].payload_ref = "foreign-payload".into(),
                        _ => unreachable!(),
                    }
                    if matches!(
                        mutation,
                        "executor" | "legacy-verify" | "criterion" | "extra-criterion"
                    ) {
                        before.nodes[index] = child.nodes[index].clone();
                    }
                    assert!(
                        AgentActionService::validate_native_retirement_target_guard(
                            &before,
                            &child,
                            &child.nodes[index]
                        )
                        .is_err(),
                        "{target:?}/{mutation}"
                    );
                }
            }
        }
    }
}

/// A blocked exit is only "earned" when the Program is quiescent — the model has
/// already exhausted the work it can do. While any Task can still be claimed,
/// submitted or reviewed, the model is not stuck and must keep working instead.
pub(crate) const BLOCKED_EXIT_MAX_REMAINING_WORK: usize = 0;

/// True when every Task is terminal, i.e. the Program can make no further
/// progress on its own and only the model's judgement remains.
fn agentic_program_is_quiescent(projection: &AgenticProgramProjection) -> bool {
    projection
        .tasks
        .values()
        .filter(|task| {
            !matches!(
                task.status,
                AgenticTaskStatus::Accepted
                    | AgenticTaskStatus::Withdrawn
                    | AgenticTaskStatus::Superseded
            )
        })
        .count()
        <= BLOCKED_EXIT_MAX_REMAINING_WORK
}
