use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{
    apply_node_transition, validate_execution_graph, ExecutionEdge, ExecutionGraph,
    ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionTransitionError,
};
use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind, ToolIdempotency};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::execution_core::hot_state::{
    DerivedMaterialization, HotExecutionGraphRegistry, RuntimeHotStatePlane,
};
use crate::runtime_event_store::{
    AppendTransactionReceipt, AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError,
    RuntimeTransactionEventInput, SessionTerminalInput,
};

use super::events::{ExecutionGraphDelta, ExecutionGraphEvent, ExecutionNodeBinding};
use super::state_store::ExecutionGraphStateStore;

#[path = "commit_pipeline.rs"]
mod commit_pipeline;
#[path = "local_network.rs"]
mod local_network;
#[path = "effect_authority.rs"]
mod effect_authority;
#[path = "tool_receipt_query.rs"]
mod tool_receipt_query;
pub(crate) use commit_pipeline::execution_lineage_stream_id;
pub(crate) use commit_pipeline::receipt_replay_contract;
use commit_pipeline::*;
pub(crate) use tool_receipt_query::ToolReceiptQuery;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CallbackDisposition {
    Pending,
    Commit,
    Abort,
}

impl CallbackDisposition {
    pub(super) fn from_result(node_id: &str, result: &ExecutionNodeResult) -> Self {
        if !result.evidence_refs.is_empty() {
            return Self::Commit;
        }
        let Some(failure) = result.failure.as_ref().filter(|failure| !failure.retryable) else {
            return Self::Commit;
        };
        if failure.kind == "terminal_presentation_cancelled" {
            return if result.status == ExecutionNodeStatus::Cancelled
                && result.result_ref.is_none()
                && failure.evidence_refs.is_empty()
            {
                Self::Abort
            } else {
                Self::Commit
            };
        }
        if failure.kind == "execution_deadline_exceeded" {
            return if result.status == ExecutionNodeStatus::Cancelled
                && result
                    .result_ref
                    .as_ref()
                    .and_then(|reference| {
                        reference.strip_prefix(&format!("execution-deadline:{node_id}:"))
                    })
                    .and_then(|deadline| deadline.parse::<u64>().ok())
                    .is_some()
            {
                Self::Abort
            } else {
                Self::Commit
            };
        }
        if matches!(
            result.status,
            ExecutionNodeStatus::Failed | ExecutionNodeStatus::Blocked
        ) && result.result_ref.is_none()
            && matches!(
                failure.kind.as_str(),
                "executor_error"
                    | "effect_completion_uncertain"
                    | "node_execution_isolated_failure"
            )
        {
            Self::Abort
        } else {
            Self::Commit
        }
    }
}

/// Which authority published the process terminal that a receipt carries.
///
/// A process terminal is a physical claim about a process tree, so exactly one
/// authority may publish it:
///
/// - `BoundedProcess` — the private bounded process published its own closure
///   envelope while it still holds the live authority for that process.
/// - `HostProcess` — the Runtime kernel minted the fact itself from the
///   trusted host executor's quiescence report. A host shell is not a sandbox
///   process, so it carries no private capability and no closure publication.
/// - `Absent` — the receipt carries no process terminal at all.
#[derive(Clone, Copy)]
pub(crate) enum ProcessTerminalAuthority<'a> {
    Absent,
    BoundedProcess(&'a serde_json::Value),
    HostProcess(&'a serde_json::Value),
}


#[cfg(any(test, feature = "test-fixtures"))]
type ReceiptCommitCallback = Box<dyn FnOnce() + Send + 'static>;

#[cfg(any(test, feature = "test-fixtures"))]
type ReceiptCommitProbes = Arc<std::sync::Mutex<BTreeMap<
    (String, String),
    (Arc<std::sync::atomic::AtomicBool>, ReceiptCommitCallback),
>>>;

#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) struct ReceiptCommitProbe {
    key: (String, String),
    probes: ReceiptCommitProbes,
    fired: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(any(test, feature = "test-fixtures"))]
impl ReceiptCommitProbe {
    fn install(
        probes: &ReceiptCommitProbes,
        request: &crate::RuntimeToolExecutionRequest,
        callback: impl FnOnce() + Send + 'static,
    ) -> Self {
        let key = (delegated_agent_receipt_stream_id(request).unwrap(),
            request.idempotency_key.clone());
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed = fired.clone();
        let mut pending = probes.lock().unwrap();
        assert!(!pending.contains_key(&key), "duplicate original receipt probe");
        pending.insert(key.clone(), (fired.clone(), Box::new(move || {
            callback();
            completed.store(true, std::sync::atomic::Ordering::Release);
        })));
        Self { key, probes: probes.clone(), fired }
    }

    pub(crate) fn require_fired(&self) {
        assert!(self.fired.load(std::sync::atomic::Ordering::Acquire),
            "original receipt commit must execute its synchronous probe");
    }
}

#[cfg(any(test, feature = "test-fixtures"))]
impl Drop for ReceiptCommitProbe {
    fn drop(&mut self) {
        let mut pending = self.probes.lock().unwrap();
        if pending.get(&self.key).is_some_and(|(token, _)| Arc::ptr_eq(token, &self.fired)) {
            pending.remove(&self.key);
        }
    }
}

#[cfg(any(test, feature = "test-fixtures"))]
fn probe_committed_tool_effect(
    probes: &ReceiptCommitProbes,
    request: &crate::RuntimeToolExecutionRequest,
) {
    let Some(stream) = delegated_agent_receipt_stream_id(request) else { return };
    let key = (stream, request.idempotency_key.clone());
    let callback = probes.lock().unwrap().remove(&key);
    if let Some((_, callback)) = callback {
        callback();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreEffectRefusal {
    call_id: String,
    tool_name: String,
    input_sha256: String,
    effect_key: String,
    binding: crate::step_snapshot::StepCallBinding,
    reason: String,
}

impl PreEffectRefusal {
    pub(crate) fn reason_for(
        &self,
        call_id: &str,
        tool_name: &str,
        input: &str,
        binding: &crate::step_snapshot::StepCallBinding,
    ) -> Option<&str> {
        (self.call_id == call_id
            && self.tool_name == tool_name
            && self.input_sha256 == crate::context_evidence::read_sha256(input.as_bytes())
            && self.binding == *binding
            && self.effect_key == binding.effect_idempotency_key())
        .then_some(self.reason.as_str())
    }
}

fn validate_node_ticket_start(
    store: &RuntimeEventStore,
    ticket: &super::registry::NodeExecutionTicket,
    intent: Option<&crate::DurableRuntimeEvent>,
) -> Result<ExecutionGraph, ExecutionCommitError> {
    let invalid = ExecutionCommitError::InvalidCommand;
    let through = intent.map_or(store.durable_commit_cursor()?, |intent| {
        intent.commit_cursor
    });
    let transaction = if let Some(intent) = intent {
        if intent.transaction_id.trim().is_empty() || intent.commit_cursor == 0 {
            return Err(invalid(
                "execution intent original transaction identity missing".into(),
            ));
        }
        let source = store
            .transaction_stream_revision_exact(&intent.transaction_id, &ticket.graph_id)?
            .ok_or_else(|| {
                invalid("execution intent transaction has no exact Graph start source".into())
            })?;
        if source.stream_id != ticket.graph_id
            || source.expected_revision.checked_add(1) != Some(source.committed_revision)
        {
            return Err(invalid(
                "execution intent Graph start source is not one original write".into(),
            ));
        }
        Some(source)
    } else {
        None
    };
    let mut sequence = transaction.as_ref().map_or_else(
        || store.stream_revision(&ticket.graph_id),
        |stream| Ok(stream.committed_revision),
    )?;
    let mut target = None;
    let mut tail = Vec::new();
    let mut bytes = 0usize;
    let mut base = None;
    while sequence > 0 && base.is_none() {
        let page = store
            .list_stream_before(&ticket.graph_id, sequence, 100, 1024 * 1024)
            .map_err(invalid)?;
        if page.is_empty() {
            return Err(invalid("execution start history sequence gap".into()));
        }
        for record in page {
            if record.sequence != sequence
                || record.stream_id != ticket.graph_id
                || record.scope != RuntimeEventScope::ExecutionGraph
            {
                return Err(invalid(
                    "execution start history identity/sequence mismatch".into(),
                ));
            }
            sequence = sequence
                .checked_sub(1)
                .ok_or_else(|| invalid("execution start sequence underflow".into()))?;
            let body_bytes = record.payload["bytes"]
                .as_u64()
                .ok_or_else(|| invalid("execution start content size missing".into()))?;
            if body_bytes > 1024 * 1024 {
                return Err(invalid(
                    "HistoryBudgetExceeded: execution start body".into(),
                ));
            }
            bytes = bytes
                .saturating_add(serde_json::to_vec(&record)?.len())
                .saturating_add(body_bytes as usize);
            if bytes > 4 * 1024 * 1024 {
                return Err(invalid(
                    "HistoryBudgetExceeded: execution start provenance".into(),
                ));
            }
            if record.commit_cursor > through {
                continue;
            }
            let value: ExecutionGraphEvent =
                serde_json::from_value(store.resolve_graph_record(&record).map_err(invalid)?)?;
            if value.kind() != record.kind {
                return Err(invalid("execution start event kind mismatch".into()));
            }
            let binding = match &value {
                ExecutionGraphEvent::Checkpoint { bindings, .. }
                | ExecutionGraphEvent::NodesStarted { bindings, .. } => {
                    bindings.get(&ticket.node_id)
                }
                ExecutionGraphEvent::NodeTransitioned {
                    node_id, binding, ..
                } if node_id == &ticket.node_id => binding.as_ref(),
                _ => None,
            };
            let matches = binding.is_some_and(|binding| {
                binding.attempt == ticket.attempt
                    && binding.ticket_idempotency_key == ticket.idempotency_key
                    && binding.executor_kind == ticket.executor_kind
            });
            if target.is_none() {
                if !matches {
                    if transaction.is_some() {
                        return Err(invalid(
                            "effect intent transaction has no matching start".into(),
                        ));
                    }
                    continue;
                }
                if intent.is_some_and(|intent| {
                    record.transaction_id != intent.transaction_id
                        || record.commit_cursor != intent.commit_cursor
                        || record.transaction_index >= intent.transaction_index
                        || transaction
                            .as_ref()
                            .is_none_or(|source| source.committed_revision != record.sequence)
                }) {
                    return Err(invalid(
                        "execution intent and original Graph start are not the exact atomic source"
                            .into(),
                    ));
                }
                target = Some((record.clone(), value.clone()));
            } else if matches!(
                &value,
                ExecutionGraphEvent::Planned { .. } | ExecutionGraphEvent::Checkpoint { .. }
            ) {
                base = Some(value.project(None).map_err(invalid)?);
                break;
            }
            tail.push(value);
        }
    }
    let mut graph =
        base.ok_or_else(|| invalid("execution effect has no preceding start checkpoint".into()))?;
    let (record, start) =
        target.ok_or_else(|| invalid("execution effect has no original start".into()))?;
    for value in tail.into_iter().rev() {
        if value == start {
            break;
        }
        graph = value.project(Some(graph)).map_err(invalid)?;
    }
    let binding = match &start {
        ExecutionGraphEvent::Checkpoint { bindings, .. }
        | ExecutionGraphEvent::NodesStarted { bindings, .. } => bindings.get(&ticket.node_id),
        ExecutionGraphEvent::NodeTransitioned { binding, .. } => binding.as_ref(),
        _ => None,
    }
    .ok_or_else(|| invalid("execution effect start binding missing".into()))?;
    binding
        .validate_start(&graph, &ticket.node_id)
        .map_err(invalid)?;
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == ticket.node_id)
        .ok_or_else(|| invalid("execution effect start spec missing".into()))?;
    ExecutionNodeBinding::validate_ticket(ticket, &graph, node, binding.attempt)
        .map_err(invalid)?;
    let next = start.project(Some(graph)).map_err(invalid)?;
    let next_node = next
        .nodes
        .iter()
        .find(|node| node.id == ticket.node_id)
        .ok_or_else(|| invalid("execution start result spec missing".into()))?;
    ExecutionNodeBinding::validate_ticket(ticket, &next, next_node, ticket.attempt)
        .map_err(invalid)?;
    if next.node_statuses.get(&ticket.node_id) != Some(&ExecutionNodeStatus::Running)
        || next.recovery_cursor.node_attempts.get(&ticket.node_id) != Some(&ticket.attempt)
        || next.revision != record.sequence
    {
        return Err(invalid(
            "execution effect original start state mismatch".into(),
        ));
    }
    Ok(next)
}

pub(crate) struct BindingRecoveryQuiescence {
    key: String,
    sources: Vec<ExpectedStreamRevision>,
}

impl BindingRecoveryQuiescence {
    pub(crate) fn sources(&self) -> &[ExpectedStreamRevision] {
        &self.sources
    }
    pub(crate) fn matches(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> bool {
        self.key == super::events::ExecutionBindingLeaseClosure::key(graph, node, binding)
    }
}

pub(crate) const HOST_TRANSCRIPT_KIND: &str = "execution.host.transcript";
const HOST_TRANSCRIPT_MEDIA: &str = "application/vnd.cowd.host-transcript+json";
const HOST_TRANSCRIPT_MAX_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostTranscriptPublication {
    pub(crate) schema_version: u32,
    pub(crate) ticket: super::registry::NodeExecutionTicket,
    pub(crate) session_id: String,
    pub(crate) turn_id: String,
    pub(crate) prefix_count: u64,
    pub(crate) prefix_sha256: String,
    pub(crate) messages: Vec<serde_json::Value>,
    pub(crate) root_control_team_count: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_completion: Option<harness_contract::goal::GoalCompletion>,
}

fn host_transcript_key(
    ticket: &super::registry::NodeExecutionTicket,
) -> Result<String, ExecutionCommitError> {
    Ok(format!(
        "host-transcript:{:x}",
        Sha256::digest(serde_json::to_vec(ticket)?)
    ))
}

fn host_transcript_reference(
    event: &RuntimeTransactionEventInput,
) -> Result<harness_contract::context::EvidenceAccessRef, ExecutionCommitError> {
    let key = event.idempotency_key.as_deref().ok_or_else(|| {
        ExecutionCommitError::InvalidCommand("Host transcript key missing".into())
    })?;
    Ok(harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::reality::EvidenceRef::new("host_transcript", key),
        event.event.payload["body"]["sha256"]
            .as_str()
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("Host transcript digest missing".into())
            })?,
        event.event.payload["body"]["bytes"]
            .as_u64()
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("Host transcript bytes missing".into())
            })?,
        HOST_TRANSCRIPT_MEDIA,
        key,
        format!(
            "session:{}",
            event.event.payload["session_id"].as_str().ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("Host transcript Session missing".into())
            })?
        ),
    ))
}

fn validate_host_transcript_document(document: &HostTranscriptPublication) -> Result<(), String> {
    if document.schema_version != 1
        || document.ticket.attempt == 0
        || !matches!(
            document.ticket.executor_kind.as_str(),
            "inline_model" | "tool_batch" | "synthesize"
        )
        || document.session_id.is_empty()
        || document.turn_id.is_empty()
        || document.prefix_count > 16384
        || document.messages.len() > 256
        || document.prefix_sha256.len() != 71
        || !document.prefix_sha256.starts_with("sha256:")
        || !document.prefix_sha256[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || serde_json::to_vec(document)
            .map_err(|error| error.to_string())?
            .len()
            > HOST_TRANSCRIPT_MAX_BYTES
    {
        return Err("Host transcript document identity/size invalid".into());
    }
    if document.ticket.executor_kind == "synthesize" {
        if document.root_control_team_count.is_some() {
            return Err("Synthesize transcript cannot carry root-control obligation".into());
        }
        match document.terminal_completion {
            Some(harness_contract::goal::GoalCompletion::Open) => {
                return Err("Synthesize terminal completion is Open".into())
            }
            Some(_) => {
                if document.messages.len() != 1
                    || document.messages[0]["role"] != "assistant"
                    || !document.messages[0]["usage"].is_null()
                {
                    return Err(
                        "Synthesize terminal requires one assistant message without usage".into(),
                    );
                }
                let blocks: Vec<crate::ContentBlock> =
                    serde_json::from_value(document.messages[0]["blocks"].clone())
                        .map_err(|error| error.to_string())?;
                if !matches!(blocks.as_slice(),[crate::ContentBlock::Text { text }] if !text.trim().is_empty())
                {
                    return Err("Synthesize terminal requires exactly one nonempty Text".into());
                }
            }
            None if !document.messages.is_empty() => {
                return Err("Synthesize replan publication must be explicitly empty".into())
            }
            None => {}
        }
    } else if document.terminal_completion.is_some() {
        return Err("Host non-Synthesize document cannot carry terminal completion".into());
    }
    for message in &document.messages {
        let fields = message
            .as_object()
            .ok_or("Host transcript message is not an object")?;
        if fields.len() != 3
            || !fields.contains_key("role")
            || !fields.contains_key("blocks")
            || !fields.contains_key("usage")
            || !matches!(
                message["role"].as_str(),
                Some("system" | "user" | "assistant" | "tool")
            )
        {
            return Err("Host transcript message schema invalid".into());
        }
        let blocks: Vec<crate::ContentBlock> =
            serde_json::from_value(message["blocks"].clone()).map_err(|error| error.to_string())?;
        let usage: Option<model_protocol::usage::TokenUsage> =
            serde_json::from_value(message["usage"].clone()).map_err(|error| error.to_string())?;
        if serde_json::to_value(blocks).map_err(|error| error.to_string())? != message["blocks"]
            || serde_json::to_value(usage).map_err(|error| error.to_string())? != message["usage"]
        {
            return Err("Host transcript message has unsupported nested fields".into());
        }
    }
    Ok(())
}

const MAX_TOOL_EFFECT_RECEIPT_CHARS: usize = 16 * 1024;

pub struct ProcessClosurePublicationAuthority {
    origin: crate::PreparedArtifactOrigin,
    sha256: String,
    bytes: u64,
    scope: String,
}

impl ProcessClosurePublicationAuthority {
    pub fn authorizes(&self, record: &crate::ArtifactRecord) -> bool {
        record.record_class == crate::ArtifactRecordClass::PrivatePrepared
            && record.prepared_origin.as_ref() == Some(&self.origin)
            && record.sha256 == self.sha256
            && record.bytes == self.bytes
            && record.visibility_scope == self.scope
            && record.bytes <= 32 * 1024
            && record.media_type == "application/vnd.cowd.process-closure+json"
    }
}

pub(crate) fn process_closure_document(
    intent: &crate::DurableRuntimeEvent,
    outcome: &crate::RuntimeToolExecutionOutcome,
) -> Result<serde_json::Value, ExecutionCommitError> {
    use harness_contract::context::EvidenceTargetIdentity;
    let facts = outcome
        .observed_evidence
        .iter()
        .filter_map(|evidence| match &evidence.target {
            EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
            _ => None,
        })
        .collect::<Vec<_>>();
    if facts.len() != 1 {
        return Err(ExecutionCommitError::InvalidCommand(
            "process closure requires one original execution fact".into(),
        ));
    }
    let fact = facts[0];
    let digest = |value: &serde_json::Value| {
        harness_contract::step::canonical_digest("cowd.process_closure.identity/v1", value)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
    };
    Ok(json!({
        "version":1,
        "call_key":fact.admission.binding.call_key,
        "tool_use_id":fact.admission.binding.tool_use_id,
        "binding":fact.admission.binding,
        "intent_sha256":digest(&serde_json::to_value(intent)?)?,
        "outcome_sha256":digest(&serde_json::to_value(bounded_tool_effect_outcome(outcome))?)?,
        "execution_fact_sha256":digest(&serde_json::to_value(fact)?)?,
        "admission_sha256":digest(&serde_json::to_value(&fact.admission)?)?,
        "source_bundle":fact.admission.source_bundle,
        "body":fact.body,
        "terminal":fact.terminal,
    }))
}

pub(crate) trait TerminalScopeReader {
    fn event(
        &mut self,
        stream: &str,
        key: &str,
    ) -> Result<Option<Arc<crate::DurableRuntimeEvent>>, ExecutionCommitError>;
    fn fence(
        &mut self,
        transaction: &str,
        stream: &str,
    ) -> Result<Option<crate::CommittedStreamRevision>, ExecutionCommitError>;
}

struct StoredTerminalScopeReader<'store>(&'store RuntimeEventStore);

impl TerminalScopeReader for StoredTerminalScopeReader<'_> {
    fn event(
        &mut self,
        stream: &str,
        key: &str,
    ) -> Result<Option<Arc<crate::DurableRuntimeEvent>>, ExecutionCommitError> {
        Ok(self.0.event_by_idempotency_key(stream, key)?.map(Arc::new))
    }

    fn fence(
        &mut self,
        transaction: &str,
        stream: &str,
    ) -> Result<Option<crate::CommittedStreamRevision>, ExecutionCommitError> {
        Ok(self
            .0
            .transaction_stream_revision_exact(transaction, stream)?)
    }
}

#[cfg(test)]
pub(crate) mod bounded_process_tests {
    use super::*;
    use harness_contract::context::{
        ArtifactRef, ArtifactWriteDescriptor, EvidenceTargetIdentity, ToolExecutionTerminal,
    };

    pub(crate) fn host(workspace: &std::path::Path) -> tools::ToolHost {
        tools::ToolHost::builtin("bounded-runtime-283", workspace)
            .with_authorization_lease_verifier(Arc::new(|lease| {
                lease.principal_id == "bounded-runtime-283"
                    && lease.signature == "bounded-fixture-authority"
            }))
    }

    struct LeaseRecoveryPublicationBackend423b;
    #[async_trait::async_trait]
    impl super::super::executors::ScopedNodeBackend for LeaseRecoveryPublicationBackend423b {
        async fn execute(
            &self,
            _ticket: &super::super::NodeExecutionTicket,
        ) -> Result<super::super::NodeExecutionOutcome, super::super::NodeExecutorError> {
            panic!("lease recovery must not execute the original Process again");
        }
    }
    struct LeaseRecoveryPublicationResolver423b;
    impl super::super::executors::ScopedNodeBackendResolver for LeaseRecoveryPublicationResolver423b {
        fn resolve(
            &self,
            _ticket: &super::super::NodeExecutionTicket,
        ) -> Option<Arc<dyn super::super::executors::ScopedNodeBackend>> {
            Some(Arc::new(LeaseRecoveryPublicationBackend423b))
        }
    }

    #[tokio::test]
    async fn r1_423b_builtin_toolbatch_real_process_leaf_recovers_without_node_intent() {
        for (exit_code, incomplete) in [(0, false), (7, false), (0, true)] {
            let workspace = tempfile::tempdir().unwrap();
            let storage = tempfile::tempdir().unwrap();
            let artifacts = Arc::new(crate::ArtifactStore::for_test_default(storage.path()));
            let host = host(workspace.path());
            let input = json!({"language":"python","input_files":[],"timeout_ms":5000,
                "code":format!("import sys\nprint(423)\nsys.exit({exit_code})")});
            let request = request(&host, input.clone());
            let call = json!({"id":request.tool_use_id,"name":request.tool_name,"input":request.input,"depends_on":[]});
            let mut planned = vec![call.clone()];
            if incomplete {
                let mut missing = call;
                missing["id"] = json!("missing-original-call");
                planned.push(missing);
            }
            let payload = crate::step_snapshot::encode_persisted_tool_batch(&json!({
                "session_id":"scope2-session","step_snapshots":{},
                "calls":planned,"continue_with_tool_batch":false}))
            .unwrap();
            let (store, service, graph, root, owner) =
                scope_commit_tests::root_fixture_with_payload(&payload);
            let services = services(workspace.path(), store.clone(), artifacts.clone());
            let service = service.with_artifact_store(artifacts.clone());
            let effect = &request.compiled_invocation.as_ref().unwrap().effect;
            let state = ExecutionGraphStateStore::new(store.clone());
            let (_, starts) = state
                .lease_starts_page(&graph.id, 0, graph.revision)
                .unwrap();
            let binding = state.current_binding(&graph, "scope2-node", 1).unwrap();
            assert_eq!(starts.len(), 1);
            assert_eq!(starts[0].0, "scope2-node");
            assert_eq!(starts[0].1, binding);
            assert!(super::super::events::ExecutionBindingLeaseClosure::required(&binding));
            let (original_ticket, _) = state
                .binding_start_ticket(&graph, "scope2-node", &binding)
                .unwrap();
            assert_eq!(original_ticket.executor_kind, "tool_batch");
            assert_eq!(original_ticket.attempt, 1);
            let registry = super::super::NodeExecutorRegistry::new();
            let executor = Arc::new(super::super::ScopedNodeExecutor::new("tool_batch"));
            executor.install_resolver(Arc::new(LeaseRecoveryPublicationResolver423b));
            registry.register(executor).unwrap();
            let resolver =
                crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                    .unwrap();
            let scope = crate::bound_tool_batch::admitted_effect_scope(
                &request,
                effect,
                Some(host.pin_snapshot()),
                &resolver,
            )
            .unwrap();
            service
                .begin_root_tool_effect(&request, effect, &root, &scope, &owner, 1)
                .unwrap();
            assert!(service
                .authenticated_lease_recovery(&graph, "scope2-node", &binding, &registry)
                .await
                .unwrap()
                .is_none());
            let prepared = host
                .pin_snapshot()
                .prepare_bounded_process(
                    request.authorization.as_ref().unwrap(),
                    &input,
                    service.bounded_process_binding(&request, effect).unwrap(),
                )
                .unwrap();
            let source = archive(
                &artifacts,
                prepared.source_bundle_bytes(),
                "application/octet-stream",
            )
            .await;
            let admission = service
                .admit_bounded_process(&request, effect, &prepared, &source)
                .unwrap();
            let completed = tokio::task::spawn_blocking(move || {
                prepared.run(admission, &tools::ToolExecutionCancellation::new())
            })
            .await
            .unwrap();
            let authority =
                crate::tool_host::BoundedProcessAuthority::bind(services, request.clone()).unwrap();
            let outcome = authority
                .with_commit_context(
                    service.commit_bounded_process(&request, effect, completed, &artifacts),
                )
                .await
                .unwrap();
            assert!(ExecutionCommitService::lease_recovery_process_closed(
                &outcome, effect
            ));
            let terminal = service
                .transition_node(
                    &graph,
                    "scope2-node",
                    ExecutionNodeStatus::Completed,
                    Some(harness_contract::execution_graph::ExecutionNodeResult {
                        status: ExecutionNodeStatus::Completed,
                        result_ref: None,
                        wait_class: None,
                        summary: None,
                        evidence_refs: Vec::new(),
                        failure: None,
                        usage: Default::default(),
                        finished_at_ms: 1,
                    }),
                    Vec::new(),
                )
                .unwrap()
                .graph;
            assert_eq!(
                service
                    .authenticated_lease_recovery(&terminal, "scope2-node", &binding, &registry)
                    .await
                    .unwrap()
                    .is_some(),
                !incomplete
            );
            let (ticket, _) = ExecutionGraphStateStore::new(store.clone())
                .binding_start_ticket(&terminal, "scope2-node", &binding)
                .unwrap();
            let node_effect_stream = ExecutionNodeBinding::effect_stream_id(&ticket);
            assert!(store
                .event_by_idempotency_key(&node_effect_stream, "scope2-ticket:intent")
                .unwrap()
                .is_none());
            assert!(store
                .event_by_idempotency_key(&node_effect_stream, "scope2-ticket:receipt")
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn canonical_process_domains_preserve_original_raw_and_descriptor_hashes_291() {
        let workspace = tempfile::tempdir().unwrap();
        let host = host(workspace.path());
        let input =
            json!({"language":"python","code":"print(291)","input_files":[],"timeout_ms":5000});
        let request = request(&host, input.clone());
        let effect = &request.compiled_invocation.as_ref().unwrap().effect;
        let original_descriptor = effect.descriptor_hash.clone();
        assert_eq!(original_descriptor.len(), 16);
        let descriptor = ExecutionCommitService::bounded_process_descriptor_sha256(effect).unwrap();
        assert_eq!(descriptor.len(), 64);
        assert_eq!(
            descriptor,
            harness_contract::step::canonical_digest(
                "cowd.bounded_process.descriptor/v1",
                &serde_json::to_value(effect).unwrap()
            )
            .unwrap()
            .strip_prefix("sha256:")
            .unwrap()
        );
        assert_eq!(effect.descriptor_hash, original_descriptor);
        let differently_formatted = "{ \"timeout_ms\":5000, \"input_files\":[], \"code\":\"print(291)\", \"language\":\"python\" }";
        let parsed = harness_contract::step::parse_strict_json(differently_formatted).unwrap();
        assert_eq!(parsed, input);
        assert_ne!(
            format!("{:x}", Sha256::digest(request.input.as_bytes())),
            format!("{:x}", Sha256::digest(differently_formatted.as_bytes()))
        );
        let normalized = ExecutionCommitService::bounded_process_input_sha256(&input).unwrap();
        assert_eq!(normalized.len(), 64);
        assert_eq!(
            normalized,
            ExecutionCommitService::bounded_process_input_sha256(&parsed).unwrap()
        );
        assert_ne!(
            normalized,
            format!("{:x}", Sha256::digest(request.input.as_bytes()))
        );
        let mut changed = input;
        changed["timeout_ms"] = json!(4000);
        assert_ne!(
            normalized,
            ExecutionCommitService::bounded_process_input_sha256(&changed).unwrap()
        );
        assert!(harness_contract::step::parse_strict_json("{\"code\":1,\"code\":2}").is_err());
        let mut changed_effect = effect.clone();
        changed_effect.uses_network = !changed_effect.uses_network;
        assert_ne!(
            descriptor,
            ExecutionCommitService::bounded_process_descriptor_sha256(&changed_effect).unwrap()
        );
    }

    pub(crate) fn request(
        host: &tools::ToolHost,
        input: serde_json::Value,
    ) -> crate::RuntimeToolExecutionRequest {
        let mut request = crate::bound_tool_batch::scope_admission_tests::bound_request(
            host,
            "execute_code",
            input,
        );
        request.sandbox_posture = harness_contract::policy::SessionExecutionPolicy::from_defaults(
            harness_contract::policy::PermissionMode::ReadOnly,
            harness_contract::policy::ApprovalProfile::Balanced,
        )
        .sandbox_posture;
        let effect = &request.compiled_invocation.as_ref().unwrap().effect;
        request.authorization = Some(harness_contract::tool::ToolExecutionAuthorization {
            request_id: request.tool_use_id.clone(),
            tool_id: "execute_code".into(),
            descriptor_hash: effect.descriptor_hash.clone(),
            policy_revision: 1,
            scope: effect.scopes[0].clone(),
            timeout_lease: "timeout:5".into(),
            idempotency_key: Some(request.idempotency_key.clone()),
            authorization_lease: harness_contract::policy::AuthorizationLease {
                lease_id: "bounded-runtime-lease".into(),
                principal_id: "bounded-runtime-283".into(),
                parent_lease_id: None,
                capability: "execute_code".into(),
                scopes: effect.scopes.clone(),
                ceiling: effect.required_permission,
                issued_at_ms: 0,
                expires_at_ms: u64::MAX,
                max_uses: 1,
                remaining_uses: 1,
                idempotency_key: request.idempotency_key.clone(),
                policy_revision: 1,
                effect_descriptor_hash: effect.descriptor_hash.clone(),
                signature: "bounded-fixture-authority".into(),
                status: harness_contract::policy::AuthorizationLeaseStatus::Active,
            },
        });
        request
    }

    pub(super) fn services(
        workspace: &std::path::Path,
        store: Arc<RuntimeEventStore>,
        artifacts: Arc<crate::ArtifactStore>,
    ) -> Arc<crate::RuntimeServices> {
        let services = crate::RuntimeServices::builder(workspace.join("runtime-home"), workspace)
            .runtime_event_store(store)
            .artifact_store(artifacts)
            .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
            .resource_quotas([
                (
                    super::super::ExecutionResourceKind::Tool,
                    super::super::ResourceQuota::new(1, 1, 1).unwrap(),
                ),
                (
                    super::super::ExecutionResourceKind::Custom("tool.process".into()),
                    super::super::ResourceQuota::new(1, 1, 1).unwrap(),
                ),
                (
                    super::super::ExecutionResourceKind::Custom("tool.cpu".into()),
                    super::super::ResourceQuota::new(1, 2, 2).unwrap(),
                ),
            ])
            .build()
            .unwrap();
        services.publish_session_execution_policy(
            "scope2-session",
            crate::permissions::SessionExecutionPolicyControl::new(
                harness_contract::policy::PermissionMode::ReadOnly,
            ),
        );
        services
    }

    async fn archive(artifacts: &crate::ArtifactStore, bytes: &[u8], media: &str) -> ArtifactRef {
        let artifact = artifacts
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: media.into(),
                    visibility_scope: "session:scope2-session".into(),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: None,
                },
                bytes,
            )
            .await
            .unwrap();
        artifacts
            .pin(&artifact, "bounded-runtime-test", i64::MAX as u64)
            .unwrap();
        artifact
    }

    pub(crate) struct OwnedFixture {
        workspace: tempfile::TempDir,
        _storage: tempfile::TempDir,
        store: Arc<RuntimeEventStore>,
        pub(crate) services: Arc<crate::RuntimeServices>,
        pub(crate) host: tools::ToolHost,
        pub(crate) root: crate::CowdExecutionContext,
        pub(crate) owner: harness_contract::projection::RuntimeActivityBinding,
    }

    pub(crate) fn owned_fixture() -> OwnedFixture {
        let (store, _service, _graph, root, owner) = scope_commit_tests::root_fixture();
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(storage.path()));
        let services = services(workspace.path(), store.clone(), artifacts);
        let host = host(workspace.path());
        OwnedFixture {
            workspace,
            _storage: storage,
            store,
            services,
            host,
            root,
            owner,
        }
    }

    fn revoke_policy(services: &crate::RuntimeServices) {
        let control = services
            .session_execution_policy_control("scope2-session")
            .unwrap();
        let mut policy = control.snapshot();
        policy.revision += 1;
        control.replace(policy).unwrap();
    }

    pub(crate) fn marker_processes(marker: &str) -> Vec<u32> {
        use std::io::Read;
        std::fs::read_dir("/proc")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let process = entry.file_name().to_str()?.parse::<u32>().ok()?;
                let mut bytes = Vec::new();
                std::fs::File::open(entry.path().join("cmdline"))
                    .ok()?
                    .take(65536)
                    .read_to_end(&mut bytes)
                    .ok()?;
                bytes
                    .windows(marker.len())
                    .any(|window| window == marker.as_bytes())
                    .then_some(process)
            })
            .collect()
    }

    #[tokio::test]
    async fn live_policy_revoked_in_resource_queue_prevents_source_reads_287() {
        let fixture = owned_fixture();
        let plane = fixture.services.tool_execution_plane().clone();
        let (started, began) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let holding = {
            let plane = plane.clone();
            tokio::spawn(async move {
                plane
                    .execute(
                        &harness_contract::tool::ResourceDemand::default(),
                        None,
                        move || {
                            started.send(()).unwrap();
                            released.recv().unwrap();
                        },
                    )
                    .await
            })
        };
        began.await.unwrap();
        let request = request(
            &fixture.host,
            json!({"language":"python","code":"print(287)","input_files":["must-not-be-opened.py"]}),
        );
        let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
        let authority = crate::tool_host::BoundedProcessAuthority::bind(
            fixture.services.clone(),
            request.clone(),
        )
        .unwrap();
        let call = request.idempotency_key.clone();
        let execution = {
            let plane = plane.clone();
            let service = fixture.services.commit_service().clone();
            let lease = fixture.host.pin_snapshot();
            let root = fixture.root.clone();
            let owner = fixture.owner.clone();
            tokio::spawn(async move {
                crate::bound_tool_batch::execute_bounded_process_owned(
                    lease,
                    plane,
                    authority,
                    service,
                    request,
                    effect,
                    Some(root),
                    Some((owner, 1)),
                )
                .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while plane.stats().submitted < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        revoke_policy(&fixture.services);
        release.send(()).unwrap();
        holding.await.unwrap().unwrap();
        let error = execution.await.unwrap().unwrap_err().to_string();
        assert!(error.contains("live Session authority"), "{error}");
        assert!(fixture
            .store
            .event_by_idempotency_key(
                &format!("execution-effect:{call}"),
                &format!("{call}:intent")
            )
            .unwrap()
            .is_none());
        assert_eq!(plane.stats().active, 0);
    }

    #[tokio::test]
    async fn live_policy_revoked_after_actual_copy_prevents_admission_and_spawn_287() {
        let fixture = owned_fixture();
        std::fs::write(fixture.workspace.path().join("source.py"), "VALUE=287\n").unwrap();
        let request = request(
            &fixture.host,
            json!({"language":"python","code":"from source import VALUE\nprint(VALUE)","input_files":["source.py"]}),
        );
        let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
        let mut authority = crate::tool_host::BoundedProcessAuthority::bind(
            fixture.services.clone(),
            request.clone(),
        )
        .unwrap();
        let (copied, release) = authority.observe_checkpoint_for_test("after_copy");
        let call = request.idempotency_key.clone();
        let execution = {
            let plane = fixture.services.tool_execution_plane().clone();
            let service = fixture.services.commit_service().clone();
            let lease = fixture.host.pin_snapshot();
            let root = fixture.root.clone();
            let owner = fixture.owner.clone();
            tokio::spawn(async move {
                crate::bound_tool_batch::execute_bounded_process_owned(
                    lease,
                    plane,
                    authority,
                    service,
                    request,
                    effect,
                    Some(root),
                    Some((owner, 1)),
                )
                .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), copied)
            .await
            .unwrap()
            .unwrap();
        revoke_policy(&fixture.services);
        release.send(()).unwrap();
        assert!(execution.await.unwrap().is_err());
        let stream = format!("execution-effect:{call}");
        assert!(fixture
            .store
            .event_by_idempotency_key(&stream, &format!("{call}:intent"))
            .unwrap()
            .is_some());
        assert!(fixture
            .store
            .event_by_idempotency_key(&stream, &format!("{call}:process-admission"))
            .unwrap()
            .is_none());
        assert!(fixture
            .store
            .event_by_idempotency_key(&stream, &format!("{call}:receipt"))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn live_authority_revocation_after_start_reaps_descendants_before_unlock_287() {
        use crate::execution_core::graph::{ScopeLockMode, ScopeLockRequest, ScopedResource};
        let fixture = owned_fixture();
        let marker = format!("bounded-revoked-287-{}", uuid::Uuid::new_v4());
        let code=format!("import os\nif os.fork()==0:\n os.setsid()\n os.execl('/bin/sleep','{marker}-child','60')\nos.execl('/bin/sleep','{marker}-leader','60')");
        let request = request(
            &fixture.host,
            json!({"language":"python","code":code,"input_files":[],"timeout_ms":5000}),
        );
        let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
        let authority = crate::tool_host::BoundedProcessAuthority::bind(
            fixture.services.clone(),
            request.clone(),
        )
        .unwrap();
        let call = request.idempotency_key.clone();
        let execution = {
            let plane = fixture.services.tool_execution_plane().clone();
            let service = fixture.services.commit_service().clone();
            let lease = fixture.host.pin_snapshot();
            let root = fixture.root.clone();
            let owner = fixture.owner.clone();
            tokio::spawn(async move {
                crate::bound_tool_batch::execute_bounded_process_owned(
                    lease,
                    plane,
                    authority,
                    service,
                    request,
                    effect,
                    Some(root),
                    Some((owner, 1)),
                )
                .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while marker_processes(&marker).len() < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(fixture.workspace.path())
                .unwrap();
        let writer = {
            let locks = fixture.services.scope_locks().clone();
            let marker = marker.clone();
            let scope = ScopedResource::workspace(resolver.workspace_id()).unwrap();
            tokio::spawn(async move {
                let lease = locks
                    .acquire(
                        vec![ScopeLockRequest {
                            scope,
                            mode: ScopeLockMode::Write,
                        }],
                        None,
                    )
                    .await
                    .unwrap();
                assert!(
                    marker_processes(&marker).is_empty(),
                    "canonical lease released before actual descendants disappeared"
                );
                drop(lease);
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!writer.is_finished());
        revoke_policy(&fixture.services);
        let error = tokio::time::timeout(std::time::Duration::from_secs(10), execution)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("actual process terminal archived"),
            "{error}"
        );
        writer.await.unwrap();
        assert!(marker_processes(&marker).is_empty());
        let stream = format!("execution-effect:{call}");
        assert!(fixture
            .store
            .event_by_idempotency_key(&stream, &format!("{call}:process-admission"))
            .unwrap()
            .is_some());
        assert!(fixture
            .store
            .event_by_idempotency_key(&stream, &format!("{call}:receipt"))
            .unwrap()
            .is_none());
        assert_eq!(fixture.services.tool_execution_plane().stats().active, 0);
    }

    #[tokio::test]
    async fn owned_managed_process_uses_original_permit_completion_or_reconcile_once_287() {
        use harness_contract::agent::{AgentDefinitionId, DefinitionScope, RevisionSelector};
        use harness_contract::managed_agent::{
            ManagedAgentDefinition, ManagedAgentHealthPolicy, ManagedAgentInvocationFence,
            ManagedAgentOverlapPolicy, ManagedAgentRetryPolicy, ManagedAgentTarget,
            ManagedAgentTrigger,
        };
        for revoke in [false, true] {
            let fixture = owned_fixture();
            let dispatcher = fixture.services.managed_agents();
            let definition = dispatcher
                .register_definition(
                    ManagedAgentDefinition {
                        managed_agent_id: "workspace/cowd/bounded".into(),
                        revision: 1,
                        target: ManagedAgentTarget::Agent {
                            definition_id: AgentDefinitionId::new(
                                DefinitionScope::Workspace,
                                "cowd/bounded",
                            )
                            .unwrap(),
                            selector: RevisionSelector::LatestApprovedStable,
                        },
                        trigger: ManagedAgentTrigger::Manual,
                        session_id: "scope2-session".into(),
                        objective: "bounded execution lifecycle".into(),
                        acceptance: vec![],
                        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                        model_lease: "default".into(),
                        granted_capabilities: vec![harness_contract::agent::AgentCapability::Read],
                        allowed_tool_contract_refs: vec!["execute_code".into()],
                        allowed_skill_refs: vec![],
                        resource_scopes: vec!["workspace".into()],
                        overlap_policy: ManagedAgentOverlapPolicy::Forbid,
                        retry_policy: ManagedAgentRetryPolicy::default(),
                        health_policy: ManagedAgentHealthPolicy::default(),
                        enabled: true,
                    },
                    1,
                )
                .unwrap();
            dispatcher
                .trigger_manual(&definition.managed_agent_id, "bounded-managed-287", 2)
                .unwrap();
            let claim = dispatcher
                .claim_ready("bounded-dispatcher", 3, 120000, 1)
                .unwrap()
                .pop()
                .unwrap();
            let token = claim.claim_token.as_deref().unwrap();
            dispatcher
                .begin_graph_registration(
                    &claim.invocation_id,
                    "bounded-dispatcher",
                    claim.fence_generation,
                    token,
                    fixture.root.execution_id.clone(),
                )
                .unwrap();
            dispatcher
                .materialize_invocation(
                    &claim.invocation_id,
                    "bounded-dispatcher",
                    claim.fence_generation,
                    token,
                    fixture.root.execution_id.clone(),
                    format!("registered:{}", fixture.root.execution_id),
                )
                .unwrap();
            dispatcher
                .start_invocation(
                    &claim.invocation_id,
                    "bounded-dispatcher",
                    claim.fence_generation,
                    token,
                    fixture.root.execution_id.clone(),
                    4,
                )
                .unwrap();
            let mut request = request(
                &fixture.host,
                json!({"language":"python","code":"print('managed-287')","input_files":[],"timeout_ms":5000}),
            );
            request.managed_invocation = Some(ManagedAgentInvocationFence {
                managed_agent_id: definition.managed_agent_id,
                definition_revision: definition.revision,
                invocation_id: claim.invocation_id.clone(),
                attempt_no: claim.attempt_no,
                fence_generation: claim.fence_generation,
                dispatcher_id: "bounded-dispatcher".into(),
            });
            let mut forged = request.clone();
            forged.managed_invocation.as_mut().unwrap().fence_generation += 1;
            assert!(crate::tool_host::BoundedProcessAuthority::bind(
                fixture.services.clone(),
                forged
            )
            .is_err());
            let mut authority = crate::tool_host::BoundedProcessAuthority::bind(
                fixture.services.clone(),
                request.clone(),
            )
            .unwrap();
            let checkpoint = if revoke {
                Some(authority.observe_checkpoint_for_test("after_copy"))
            } else {
                None
            };
            let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
            let execution = {
                let plane = fixture.services.tool_execution_plane().clone();
                let service = fixture.services.commit_service().clone();
                let lease = fixture.host.pin_snapshot();
                let root = fixture.root.clone();
                let owner = fixture.owner.clone();
                let request = request.clone();
                tokio::spawn(async move {
                    crate::bound_tool_batch::execute_bounded_process_owned(
                        lease,
                        plane,
                        authority,
                        service,
                        request,
                        effect,
                        Some(root),
                        Some((owner, 1)),
                    )
                    .await
                })
            };
            if let Some((copied, release)) = checkpoint {
                tokio::time::timeout(std::time::Duration::from_secs(5), copied)
                    .await
                    .unwrap()
                    .unwrap();
                revoke_policy(&fixture.services);
                release.send(()).unwrap();
            }
            let result = execution.await.unwrap();
            let records = dispatcher.outbox().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].invocation_id, claim.invocation_id);
            if revoke {
                assert!(result.is_err());
                assert_eq!(
                    records[0].status,
                    crate::FencedEffectStatus::ReconciliationRequired
                );
            } else {
                let outcome = result.unwrap();
                let EvidenceTargetIdentity::ToolExecution { execution } =
                    &outcome.observed_evidence[0].target
                else {
                    panic!("actual managed terminal missing")
                };
                assert!(
                    matches!(&execution.terminal,ToolExecutionTerminal::Settled {exit_code:Some(0),closure,..}
                    if closure.all_children_reaped && closure.init_reaped && closure.kernel_hardening_applied)
                );
                assert_eq!(records[0].status, crate::FencedEffectStatus::Completed);
                assert_eq!(
                    records[0].receipt_ref.as_deref(),
                    Some(outcome.evidence_ref.as_str())
                );
                let effect = &request.compiled_invocation.as_ref().unwrap().effect;
                assert!(fixture
                    .services
                    .commit_service()
                    .completed_tool_effect(&request, effect)
                    .unwrap()
                    .is_some());
                assert_eq!(dispatcher.outbox().unwrap().len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn actual_private_process_terminal_primary_index_and_historical_queries_283() {
        for host_parent in [false, true] {
            for exit_code in [0, 7] {
                let (store, service, graph, root, owner) = scope_commit_tests::root_fixture();
                let workspace = tempfile::tempdir().unwrap();
                let storage = tempfile::tempdir().unwrap();
                let artifacts = Arc::new(crate::ArtifactStore::for_test_default(storage.path()));
                let services = services(workspace.path(), store.clone(), artifacts.clone());
                let service = service.with_artifact_store(artifacts.clone());
                let host = host(workspace.path());
                std::fs::write(workspace.path().join("source.py"), "VALUE=283\n").unwrap();
                let capture_bytes = if !host_parent && exit_code == 0 {
                    900_000
                } else {
                    60_000
                };
                let input = json!({"language":"python","input_files":["source.py"],"timeout_ms":5000,
                    "code":format!("import sys\nfrom source import VALUE\nassert VALUE==283\nsys.stdout.buffer.write(bytes([255])*{capture_bytes})\nsys.exit({exit_code})")});
                let mut request = request(&host, input.clone());
                if host_parent {
                    request.parent_execution =
                        Some(harness_contract::execution_graph::ExecutionParentBinding {
                            execution_id: graph.id.clone(),
                            node_id: "scope2-node".into(),
                        });
                    request.parent_execution_attempt = Some(1);
                }
                let effect = &request.compiled_invocation.as_ref().unwrap().effect;
                let resolver =
                    crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                        .unwrap();
                let scope = crate::bound_tool_batch::admitted_effect_scope(
                    &request,
                    effect,
                    Some(host.pin_snapshot()),
                    &resolver,
                )
                .unwrap();
                assert!(!scope.complete);
                if host_parent {
                    service
                        .begin_bound_tool_effect(&request, effect, &scope)
                        .unwrap();
                } else {
                    service
                        .begin_root_tool_effect(&request, effect, &root, &scope, &owner, 1)
                        .unwrap();
                }
                let prepared = host
                    .pin_snapshot()
                    .prepare_bounded_process(
                        request.authorization.as_ref().unwrap(),
                        &input,
                        service.bounded_process_binding(&request, effect).unwrap(),
                    )
                    .unwrap();
                let source = archive(
                    &artifacts,
                    prepared.source_bundle_bytes(),
                    "application/octet-stream",
                )
                .await;
                let admission = service
                    .admit_bounded_process(&request, effect, &prepared, &source)
                    .unwrap();
                let stored_admission = store
                    .event_by_idempotency_key(
                        &format!("execution-effect:{}", request.idempotency_key),
                        &format!("{}:process-admission", request.idempotency_key),
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    stored_admission.payload["source_workspace_root"],
                    serde_json::to_value(prepared.workspace_root()).unwrap()
                );
                assert_eq!(stored_admission.payload["original_input"], request.input);
                assert_eq!(
                    admission.binding.descriptor_sha256,
                    ExecutionCommitService::bounded_process_descriptor_sha256(effect).unwrap()
                );
                assert_eq!(
                    admission.binding.input_sha256,
                    ExecutionCommitService::bounded_process_input_sha256(&input).unwrap()
                );
                assert!(service
                    .admit_bounded_process(&request, effect, &prepared, &source)
                    .is_err());
                let completed = tokio::task::spawn_blocking(move || {
                    prepared.run(admission, &tools::ToolExecutionCancellation::new())
                })
                .await
                .unwrap();
                match completed.terminal() {
                    ToolExecutionTerminal::Settled {
                        exit_code: actual,
                        closure,
                        ..
                    } => {
                        assert_eq!(*actual, Some(exit_code));
                        assert!(
                            closure.all_children_reaped
                                && closure.init_reaped
                                && closure.kernel_hardening_applied
                        );
                    }
                    terminal => panic!("real bounded launcher did not settle: {terminal:?}"),
                }
                let authority = crate::tool_host::BoundedProcessAuthority::bind(
                    services.clone(),
                    request.clone(),
                )
                .unwrap();
                let outcome = authority
                    .with_commit_context(
                        service.commit_bounded_process(&request, effect, completed, &artifacts),
                    )
                    .await
                    .unwrap();
                assert!(crate::execution_projection::settlement::process_closed(
                    &outcome, effect
                ));
                // Physical closure is quiescence: the reaped facts are the
                // criterion. The hardening flag is an audit fact, and it is
                // enforced where the bounded process publishes its terminal.
                for closure_flag in ["all_children_reaped", "init_reaped"] {
                    let mut raw = serde_json::to_value(&outcome).unwrap();
                    raw["observed_evidence"][0]["target"]["execution"]["terminal"]["closure"]
                        [closure_flag] = json!(false);
                    let changed: crate::RuntimeToolExecutionOutcome =
                        serde_json::from_value(raw).unwrap();
                    assert!(
                        !crate::execution_projection::settlement::process_closed(&changed, effect),
                        "{closure_flag}"
                    );
                }
                let primary = store
                    .event_by_idempotency_key(
                        &format!("execution-effect:{}", request.idempotency_key),
                        &format!("{}:receipt", request.idempotency_key),
                    )
                    .unwrap()
                    .unwrap();
                let index = store
                    .event_by_idempotency_key(
                        "execution-agent-receipts:scope2-graph:scope2-node:1",
                        &format!("agent-tool-receipt:{}", request.idempotency_key),
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(primary.transaction_id, index.transaction_id);
                assert_eq!(primary.payload["outcome"], index.payload["outcome"]);
                assert_eq!(
                    primary.payload["root_effect_owner"],
                    index.payload["root_effect_owner"]
                );
                assert_eq!(
                    primary.payload["process_closure"],
                    index.payload["process_closure"]
                );
                assert!(
                    crate::execution_projection::settlement::inspect_physical_closure_for_test(
                        &services, &primary, &outcome, effect
                    )
                    .unwrap()
                );
                if host_parent {
                    assert!(
                        crate::execution_projection::settlement::inspect_bound_leaf_for_test(
                            &services, &request
                        )
                        .unwrap()
                    );
                    for corruption in 0..9 {
                        let mut changed = request.clone();
                        match corruption {
                            0 => {
                                changed.parent_execution.as_mut().unwrap().execution_id =
                                    "foreign-parent".into()
                            }
                            1 => {
                                changed.parent_execution.as_mut().unwrap().node_id =
                                    "foreign-node".into()
                            }
                            2 => changed.parent_execution_attempt = Some(2),
                            3 => changed.idempotency_key = "missing-original-receipt".into(),
                            4 => changed.tool_use_id = "foreign-call".into(),
                            5 => changed.tool_name = "foreign-tool".into(),
                            6 => changed.input.push(' '),
                            7 => changed.session_id = Some("foreign-session".into()),
                            _ => changed.step_binding = None,
                        }
                        assert!(!matches!(
                            crate::execution_projection::settlement::inspect_bound_leaf_for_test(&services, &changed),
                            Ok(true)
                        ), "bound leaf corruption {corruption} must not close");
                    }
                }
                if capture_bytes == 900_000 {
                    let EvidenceTargetIdentity::ToolExecution { execution } =
                        &outcome.observed_evidence[0].target
                    else {
                        panic!("original execution missing")
                    };
                    assert!(execution.body.bytes > 1024 * 1024);
                    assert!(
                        primary.payload["process_closure"]["bytes"]
                            .as_u64()
                            .unwrap()
                            <= 32 * 1024
                    );
                }
                let mut missing = primary.clone();
                missing.payload["process_closure"] = serde_json::Value::Null;
                assert!(
                    crate::execution_projection::settlement::inspect_physical_closure_for_test(
                        &services, &missing, &outcome, effect
                    )
                    .is_err()
                );
                let proof: ArtifactRef = serde_json::from_value(
                    primary.payload["process_closure"]["body"]["artifact"].clone(),
                )
                .unwrap();
                let origin = crate::PreparedArtifactOrigin::ToolEffectClosure {
                    stream_id: primary.stream_id.clone(),
                    idempotency_key: primary.idempotency_key.clone().unwrap(),
                };
                let proof_body = artifacts
                    .read_process_closure_blocking(&proof, &proof.visibility_scope, &origin)
                    .unwrap()
                    .body;
                let live = archive(
                    &artifacts,
                    &proof_body,
                    "application/vnd.cowd.process-closure+json",
                )
                .await;
                let mut live_forgery = primary.clone();
                live_forgery.payload["process_closure"] = json!({"content_schema":2,"bytes":live.bytes,
                    "sha256":live.sha256,"body":{"kind":"artifact_ref","artifact":live}});
                assert!(
                    crate::execution_projection::settlement::inspect_physical_closure_for_test(
                        &services,
                        &live_forgery,
                        &outcome,
                        effect
                    )
                    .is_err()
                );
                let mut forged = primary.clone();
                forged.payload["process_closure"]["sha256"] = json!("sha256:forged");
                assert!(
                    crate::execution_projection::settlement::inspect_physical_closure_for_test(
                        &services, &forged, &outcome, effect
                    )
                    .is_err()
                );
                let mut changed = outcome.clone();
                changed.output = Some("not the originally persisted bounded result".into());
                assert!(
                    crate::execution_projection::settlement::inspect_physical_closure_for_test(
                        &services, &primary, &changed, effect
                    )
                    .is_err()
                );
                assert_eq!(
                    primary.payload["replay"]["parent_execution"].is_null(),
                    !host_parent
                );
                for corruption in 0..8 {
                    let mut corrupted = primary.clone();
                    match corruption {
                        0 => {
                            corrupted.payload["outcome"]["observed_evidence"][0]["target"]
                                ["execution"]["body"]["sha256"] = json!("0".repeat(64))
                        }
                        1 => {
                            corrupted.payload["outcome"]["observed_evidence"][0]["target"]
                                ["execution"]["admission"]["binding"]["call_key"] =
                                json!("other-call")
                        }
                        2 => {
                            corrupted.payload["outcome"]["observed_evidence"][0]["target"]
                                ["execution"]["version"] = json!(2)
                        }
                        3 => corrupted.payload["outcome"]["observed_evidence"] = json!([]),
                        4 => corrupted.payload["process_admission_ref"] = serde_json::Value::Null,
                        5 => {
                            corrupted.payload["root_effect_owner"] =
                                json!({"binding":owner,"attempt":2,"graph_revision":graph.revision})
                        }
                        6 => {
                            corrupted.payload["outcome"]["observed_evidence"][0]["target"]
                                ["execution"]["stdout_bytes"] = json!(1)
                        }
                        _ => {
                            corrupted.payload["outcome"]["observed_evidence"][0]["target"]
                                ["execution"]["terminal"] =
                                json!({"kind":"unknown","reason_code":"forged-terminal"})
                        }
                    }
                    assert!(
                        service
                            .validate_stored_terminal_scope(effect, &corrupted, false)
                            .is_err(),
                        "corruption {corruption} must not recover"
                    );
                }
                let EvidenceTargetIdentity::ToolExecution { execution } =
                    &outcome.observed_evidence[0].target
                else {
                    panic!("private fact missing")
                };
                assert_eq!(execution.stdout_bytes, capture_bytes);
                let body = artifacts
                    .read(&execution.body, &execution.body.visibility_scope, None)
                    .await
                    .unwrap();
                assert!(body.len() > 16 * 1024);
                let mut forged_body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert!(forged_body.get("stdout_utf8").is_none());
                forged_body["stdout_utf8"] = json!("lying truncated prefix");
                let forged_ref = archive(
                    &artifacts,
                    &serde_json::to_vec(&forged_body).unwrap(),
                    "application/json",
                )
                .await;
                let mut forged_primary = primary.clone();
                forged_primary.payload["outcome"]["observed_evidence"][0]["target"]["execution"]
                    ["body"] = serde_json::to_value(forged_ref).unwrap();
                assert!(service
                    .validate_stored_bounded_process(&forged_primary)
                    .unwrap_err()
                    .to_string()
                    .contains("optional UTF8"));
                service
                    .transition_node(
                        &graph,
                        "scope2-node",
                        ExecutionNodeStatus::Cancelled,
                        None,
                        vec![],
                    )
                    .unwrap();
                let parent = harness_contract::execution_graph::ExecutionParentBinding {
                    execution_id: graph.id,
                    node_id: "scope2-node".into(),
                };
                let query = ToolReceiptQuery {
                    step_binding: request.step_binding.as_ref().unwrap(),
                    parent_execution: &parent,
                    session_id: "scope2-session",
                    policy_revision: 1,
                    idempotency_key: &request.idempotency_key,
                    tool_use_id: &request.tool_use_id,
                    tool_name: &request.tool_name,
                    input: &request.input,
                };
                assert_eq!(
                    service
                        .completed_tool_effect(&request, effect)
                        .unwrap()
                        .unwrap()
                        .observed_evidence,
                    outcome.observed_evidence
                );
                assert!(service
                    .query_completed_tool_effect(&query)
                    .unwrap()
                    .is_some());
                assert!(service
                    .query_completed_agent_tool_effect(&query)
                    .unwrap()
                    .is_some());
            }
        }
    }

    #[tokio::test]
    async fn public_process_vec_and_cross_call_private_terminal_cannot_commit_283() {
        let (_store, service, _graph, root, owner) = scope_commit_tests::root_fixture();
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(storage.path()));
        let service = service.with_artifact_store(artifacts.clone());
        let host = host(workspace.path());
        let input = json!({"language":"python","code":"print(283)","input_files":[]});
        let request = request(&host, input.clone());
        let effect = &request.compiled_invocation.as_ref().unwrap().effect;
        assert!(service.bounded_process_binding(&request, effect).is_err());
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let scope = crate::bound_tool_batch::admitted_effect_scope(
            &request,
            effect,
            Some(host.pin_snapshot()),
            &resolver,
        )
        .unwrap();
        service
            .begin_root_tool_effect(&request, effect, &root, &scope, &owner, 1)
            .unwrap();
        for corruption in 0..6 {
            let mut corrupt = request.clone();
            match corruption {
                0 => corrupt.authorization = None,
                1 => corrupt.step_binding = None,
                2 => {
                    corrupt
                        .authorization
                        .as_mut()
                        .unwrap()
                        .authorization_lease
                        .signature = "forged".into()
                }
                3 => {
                    corrupt
                        .authorization
                        .as_mut()
                        .unwrap()
                        .authorization_lease
                        .expires_at_ms = 1
                }
                4 => corrupt
                    .authorization
                    .as_mut()
                    .unwrap()
                    .authorization_lease
                    .scopes
                    .clear(),
                _ => corrupt.idempotency_key.push_str("other-owner"),
            }
            assert!(
                service.bounded_process_binding(&corrupt, effect).is_err(),
                "corrupt authority {corruption}"
            );
        }
        let prepared = host
            .pin_snapshot()
            .prepare_bounded_process(
                request.authorization.as_ref().unwrap(),
                &input,
                service.bounded_process_binding(&request, effect).unwrap(),
            )
            .unwrap();
        let source = archive(
            &artifacts,
            prepared.source_bundle_bytes(),
            "application/octet-stream",
        )
        .await;
        let mut corrupt_source = source.clone();
        corrupt_source.sha256 = "0".repeat(64);
        assert!(service
            .admit_bounded_process(&request, effect, &prepared, &corrupt_source)
            .is_err());
        let mut admission = service
            .admit_bounded_process(&request, effect, &prepared, &source)
            .unwrap();
        admission.binding.call_key.push_str("-forged");
        let completed = prepared.run(admission, &tools::ToolExecutionCancellation::new());
        assert!(service
            .commit_bounded_process(&request, effect, completed, &artifacts)
            .await
            .is_err());
        let prepared = host
            .pin_snapshot()
            .prepare_bounded_process(
                request.authorization.as_ref().unwrap(),
                &input,
                service.bounded_process_binding(&request, effect).unwrap(),
            )
            .unwrap();
        let admission = prepared.admission(&source).unwrap();
        let cancellation = tools::ToolExecutionCancellation::new();
        cancellation.cancel();
        let completed = prepared.run(admission, &cancellation);
        assert!(matches!(
            completed.terminal(),
            ToolExecutionTerminal::NotStarted { .. }
        ));
        let body = archive(&artifacts, completed.body_bytes(), "application/json").await;
        let draft = completed.into_output(body).unwrap();
        let outcome = crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(draft.model_text().into()),
            error: None,
            evidence_ref: "forged-public".into(),
            observed_evidence: draft.observations().to_vec(),
        };
        assert!(service
            .commit_tool_effect(&request, effect, &outcome)
            .is_err());
        assert!(service
            .commit_bound_readonly_tool_receipt(&request, &outcome, effect, &scope)
            .is_err());
        assert!(matches!(
            service
                .begin_root_tool_effect(&request, effect, &root, &scope, &owner, 1)
                .unwrap(),
            ToolEffectState::Uncertain
        ));
        let parent = harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: root.execution_id,
            node_id: "scope2-node".into(),
        };
        let query = ToolReceiptQuery {
            step_binding: request.step_binding.as_ref().unwrap(),
            parent_execution: &parent,
            session_id: "scope2-session",
            policy_revision: 1,
            idempotency_key: &request.idempotency_key,
            tool_use_id: &request.tool_use_id,
            tool_name: &request.tool_name,
            input: &request.input,
        };
        assert!(service.query_completed_tool_effect(&query).is_err());
        assert!(service.query_completed_agent_tool_effect(&query).is_err());
    }

    #[tokio::test]
    async fn original_owned_process_path_archives_before_preview_with_small_actual_quota_283() {
        let (store, service, _graph, root, owner) = scope_commit_tests::root_fixture();
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::for_test(
                storage.path(),
                crate::ArtifactStoreConfig {
                    compact_threshold_bytes: 1024,
                    max_object_bytes: 1024 * 1024,
                    total_quota_bytes: 4 * 1024 * 1024,
                    gc_high_water_bytes: 3 * 1024 * 1024,
                    gc_low_water_bytes: 2 * 1024 * 1024,
                    orphan_grace_ms: 1000,
                },
            )
            .unwrap(),
        );
        let service = service.with_artifact_store(artifacts.clone());
        let services = services(workspace.path(), store.clone(), artifacts.clone());
        let host = host(workspace.path());
        let input = json!({"language":"python","code":"import sys\nsys.stdout.write('actual-owned-283'*8000)","input_files":[],"timeout_ms":5000});
        let mut request = request(&host, input);
        request.step_binding = Some(
            serde_json::from_value(
                serde_json::to_value(request.step_binding.as_ref().unwrap()).unwrap(),
            )
            .unwrap(),
        );
        let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
        let plane = Arc::clone(services.tool_execution_plane());
        let authority =
            crate::tool_host::BoundedProcessAuthority::bind(services.clone(), request.clone())
                .unwrap();
        let outcome = crate::bound_tool_batch::execute_bounded_process_owned(
            host.pin_snapshot(),
            plane.clone(),
            authority,
            service.clone(),
            request.clone(),
            effect.clone(),
            Some(root),
            Some((owner, 1)),
        )
        .await
        .unwrap();
        let EvidenceTargetIdentity::ToolExecution { execution } =
            &outcome.observed_evidence[0].target
        else {
            panic!("actual execution fact missing")
        };
        assert!(matches!(
            execution.terminal,
            ToolExecutionTerminal::Settled {
                exit_code: Some(0),
                ..
            }
        ));
        assert_eq!(execution.stdout_bytes, 128000);
        assert!(execution.body.bytes > 16 * 1024 && execution.body.bytes < 1024 * 1024);
        let body = artifacts
            .read(&execution.body, &execution.body.visibility_scope, None)
            .await
            .unwrap();
        assert!(body.len() > outcome.output.as_ref().unwrap().len());
        let readable: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            readable["stdout_utf8"].as_str().unwrap(),
            "actual-owned-283".repeat(8000)
        );
        assert_eq!(plane.stats().submitted, 1);
        assert_eq!(plane.stats().completed, 1);
        assert_eq!(plane.stats().active, 0);
        let through = store.durable_commit_cursor().unwrap();
        let authority =
            crate::tool_host::BoundedProcessAuthority::bind(services, request.clone()).unwrap();
        let replay = crate::bound_tool_batch::execute_bounded_process_owned(
            host.pin_snapshot(),
            plane.clone(),
            authority,
            service,
            request,
            effect,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            replay.observed_evidence,
            crate::bound_tool_batch::retained_replay_outcome(outcome).observed_evidence
        );
        assert_eq!(store.durable_commit_cursor().unwrap(), through);
    }
}

#[cfg(test)]
mod scope_commit_tests {
    use super::*;
    use crate::bound_tool_batch::scope_admission_tests::bound_request;

    pub(super) fn root_fixture() -> (
        Arc<RuntimeEventStore>,
        ExecutionCommitService,
        ExecutionGraph,
        crate::CowdExecutionContext,
        harness_contract::projection::RuntimeActivityBinding,
    ) {
        root_fixture_with_payload("scope2")
    }

    pub(super) fn root_fixture_with_payload(
        payload: &str,
    ) -> (
        Arc<RuntimeEventStore>,
        ExecutionCommitService,
        ExecutionGraph,
        crate::CowdExecutionContext,
        harness_contract::projection::RuntimeActivityBinding,
    ) {
        let store = Arc::new(RuntimeEventStore::for_test());
        let service = ExecutionCommitService::new(Arc::clone(&store));
        let mut graph = ExecutionGraph::new("scope2 root producer");
        graph.id = "scope2-graph".into();
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "scope2-session".into(),
            turn_id: "scope2-turn".into(),
            root_task_id: "scope2-task".into(),
            task_id: "scope2-task".into(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        node.id = "scope2-node".into();
        graph.nodes.push(node);
        graph = service.register_graph(graph).unwrap().graph;
        graph = service
            .transition_node(
                &graph,
                "scope2-node",
                ExecutionNodeStatus::Ready,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        graph = service
            .bind_and_start_node(
                &graph,
                "scope2-node",
                ExecutionNodeBinding {
                    executor_kind: "tool_batch".into(),
                    ticket_idempotency_key: "scope2-ticket".into(),
                    attempt: 1,
                    resource_lease_refs: vec![],
                    scope_lease_ref: None,
                    worktree_lease_ref: None,
                },
            )
            .unwrap()
            .graph;
        let owner = store
            .latest_for_stream("scope2-graph:node:scope2-node")
            .unwrap()
            .unwrap()
            .activity_binding()
            .unwrap();
        let root = crate::CowdExecutionContext {
            execution_id: graph.id.clone(),
            session_id: "scope2-session".into(),
            turn_id: "scope2-turn".into(),
        };
        (store, service, graph, root, owner)
    }

    #[test]
    fn root_read_and_write_fix_original_owner_scope_and_atomic_index() {
        for read in [false, true] {
            let (store, service, graph, root, owner) = root_fixture();
            let workspace = tempfile::tempdir().unwrap();
            std::fs::write(workspace.path().join("proof.txt"), "before").unwrap();
            let host = tools::ToolHost::builtin("scope2-root-writer", workspace.path());
            let resolver =
                crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                    .unwrap();
            let request = if read {
                bound_request(&host, "read_file", json!({"path":"proof.txt"}))
            } else {
                bound_request(
                    &host,
                    "edit",
                    json!({"action":"write","path":"proof.txt","content":"after"}),
                )
            };
            let effect = &request.compiled_invocation.as_ref().unwrap().effect;
            let scope = crate::bound_tool_batch::admitted_effect_scope(
                &request,
                effect,
                Some(host.pin_snapshot()),
                &resolver,
            )
            .unwrap();
            let admit = |owner: &harness_contract::projection::RuntimeActivityBinding, attempt| {
                if read {
                    service.begin_root_read_acquisition(
                        &request, effect, &root, &scope, owner, attempt,
                    )
                } else {
                    service.begin_root_tool_effect(&request, effect, &root, &scope, owner, attempt)
                }
            };
            let mut wrong_owner = owner.clone();
            wrong_owner.node_id = Some("unrelated-running-node".into());
            assert!(admit(&wrong_owner, 1).is_err());
            wrong_owner = owner.clone();
            wrong_owner.generation += 1;
            assert!(admit(&wrong_owner, 1).is_err());
            assert!(admit(&owner, 2).is_err());
            assert!(matches!(
                admit(&owner, 1).unwrap(),
                ToolEffectState::Fresh | ToolEffectState::NotRequired
            ));
            let output = if read {
                let content = std::fs::read_to_string(workspace.path().join("proof.txt")).unwrap();
                json!({"type":"text","truncated":false,"file":{
                    "filePath":"proof.txt","content":content,"startLine":1,"numLines":1,"totalLines":1,
                    "sha256":crate::context_evidence::read_sha256(content.as_bytes()),
                    "byteLength":content.len(),"endsWithNewline":false,
                }}).to_string()
            } else {
                std::fs::write(workspace.path().join("proof.txt"), "after").unwrap();
                json!({"ok":true}).to_string()
            };
            let outcome = crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(output),
                error: None,
                evidence_ref: "scope2-output".into(),
                observed_evidence: vec![],
            };
            if read {
                service
                    .commit_root_read_acquisition(&request, &outcome, effect, &root, &scope)
                    .unwrap();
            } else {
                service
                    .commit_tool_effect(&request, effect, &outcome)
                    .unwrap();
            }
            let suffix = if read { "read-receipt" } else { "receipt" };
            let primary = store
                .event_by_idempotency_key(
                    &format!("execution-effect:{}", request.idempotency_key),
                    &format!("{}:{suffix}", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            let index = store
                .event_by_idempotency_key(
                    "execution-agent-receipts:scope2-graph:scope2-node:1",
                    &format!("agent-tool-receipt:{}", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            assert_eq!(primary.transaction_id, index.transaction_id);
            assert_eq!(primary.commit_cursor, index.commit_cursor);
            assert!(primary.transaction_index < index.transaction_index);
            assert_eq!(
                primary.payload["resolved_effect_scope"],
                serde_json::to_value(&scope).unwrap()
            );
            assert_eq!(
                primary.payload["root_effect_owner"]["binding"],
                serde_json::to_value(&owner).unwrap()
            );
            assert_eq!(
                primary.payload["root_effect_owner"]["graph_revision"],
                graph.revision
            );
            assert_eq!(
                primary.payload["root_effect_owner"],
                index.payload["root_effect_owner"]
            );
            assert!(primary.payload["replay"]["parent_execution"].is_null());
            if read {
                let seal = crate::context_evidence::decode_read_acquisition(
                    &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                )
                .unwrap();
                assert_eq!(seal.node_id, "scope2-node");
                assert_eq!(seal.attempt, 1);
            }
            assert!(service
                .completed_tool_effect(&request, effect)
                .unwrap()
                .is_some());
            let parent = harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: graph.id.clone(),
                node_id: "scope2-node".into(),
            };
            let query = ToolReceiptQuery {
                step_binding: request.step_binding.as_ref().unwrap(),
                parent_execution: &parent,
                session_id: "scope2-session",
                policy_revision: 1,
                idempotency_key: &request.idempotency_key,
                tool_use_id: &request.tool_use_id,
                tool_name: &request.tool_name,
                input: &request.input,
            };
            assert!(service
                .query_completed_tool_effect(&query)
                .unwrap()
                .is_some());
            assert!(service
                .query_completed_agent_tool_effect(&query)
                .unwrap()
                .is_some());
            service
                .transition_node(
                    &graph,
                    "scope2-node",
                    ExecutionNodeStatus::Cancelled,
                    None,
                    vec![],
                )
                .unwrap();
            assert!(service
                .completed_tool_effect(&request, effect)
                .unwrap()
                .is_some());
            assert!(service
                .query_completed_tool_effect(&query)
                .unwrap()
                .is_some());
            assert!(service
                .query_completed_agent_tool_effect(&query)
                .unwrap()
                .is_some());
            assert!(matches!(
                admit(&owner, 1).unwrap(),
                ToolEffectState::Completed(_)
            ));
            if read {
                service
                    .commit_root_read_acquisition(&request, &outcome, effect, &root, &scope)
                    .unwrap();
            } else {
                service
                    .commit_tool_effect(&request, effect, &outcome)
                    .unwrap();
            }
            let mut corrupt = primary.clone();
            corrupt.payload["resolved_effect_scope"]
                .as_object_mut()
                .unwrap()
                .remove("scope_purposes");
            assert!(service
                .validate_stored_terminal_scope(effect, &corrupt, read)
                .is_err());
            corrupt = primary.clone();
            corrupt.payload["resolved_effect_scope"] = serde_json::Value::Null;
            assert!(service
                .validate_stored_terminal_scope(effect, &corrupt, read)
                .is_err());
            corrupt = primary.clone();
            corrupt.payload["root_effect_owner"]["binding"]["node_id"] = json!("other-node");
            assert!(service
                .validate_stored_terminal_scope(effect, &corrupt, read)
                .is_err());
            assert_eq!(
                std::fs::read_to_string(workspace.path().join("proof.txt")).unwrap(),
                if read { "before" } else { "after" }
            );
        }
    }

    #[test]
    fn immutable_artifact_acquisition_keeps_body_provenance_without_file_scope2() {
        for scoped in [false, true] {
            let (store, service, graph, _root, _owner) = root_fixture();
            let workspace = tempfile::tempdir().unwrap();
            let catalog = tools::ToolCatalog::builtin().with_runtime_tools(vec![tools::RuntimeToolDefinition {
                name:"evidence_retrieve".into(), description:Some("Read immutable evidence".into()),
                input_schema:json!({"type":"object","properties":{"evidence_ref":{"type":"string"}},
                    "required":["evidence_ref"],"additionalProperties":false}),
                required_permission:tools::permissions::PermissionMode::ReadOnly,
                effect_resolver:harness_contract::tool::ToolEffectResolverSpec {
                    resolver_id:"runtime.readonly".into(), resolver_version:1,
                },
            }]).unwrap();
            let host = tools::ToolHost::new(
                "scope2-artifact",
                workspace.path(),
                tools::ToolHostSnapshot::new(
                    Arc::new(catalog),
                    Arc::new(tools::lsp_client::LspRegistry::new()),
                    None,
                ),
            );
            let mut request = bound_request(
                &host,
                "evidence_retrieve",
                json!({"evidence_ref":"artifact://original"}),
            );
            request.parent_execution =
                Some(harness_contract::execution_graph::ExecutionParentBinding {
                    execution_id: graph.id,
                    node_id: "scope2-node".into(),
                });
            request.parent_execution_attempt = Some(1);
            let content = "original immutable artifact body";
            let outcome = crate::RuntimeToolExecutionOutcome {
                tool_use_id:request.tool_use_id.clone(), tool_name:request.tool_name.clone(),
                status:crate::RuntimeToolExecutionStatus::Executed, category:request.category,
                output:Some(json!({
                    "kind":"evidence_retrieve","available":true,"evidence_ref":"artifact://original",
                    "sha256":crate::context_evidence::read_sha256(content.as_bytes()),"bytes":content.len(),
                    "total_chunks":1,"chunks":[{"index":0,"content":content}],
                }).to_string()),
                error:None, evidence_ref:"artifact-original-receipt".into(), observed_evidence:vec![],
            };
            if scoped {
                let resolver =
                    crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                        .unwrap();
                let effect = &request.compiled_invocation.as_ref().unwrap().effect;
                let scope = crate::bound_tool_batch::admitted_effect_scope(
                    &request,
                    effect,
                    Some(host.pin_snapshot()),
                    &resolver,
                )
                .unwrap();
                assert!(!scope.complete);
                service
                    .commit_bound_readonly_tool_receipt(&request, &outcome, effect, &scope)
                    .unwrap();
            } else {
                service
                    .commit_readonly_tool_receipts(&[(request.clone(), outcome)])
                    .unwrap();
            }
            let primary = store
                .event_by_idempotency_key(
                    &format!("execution-effect:{}", request.idempotency_key),
                    &format!("{}:read-receipt", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            let index = store
                .event_by_idempotency_key(
                    "execution-agent-receipts:scope2-graph:scope2-node:1",
                    &format!("agent-tool-receipt:{}", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            ExecutionCommitService::validate_proof_bound_tool_request(&request, &primary, &index)
                .unwrap();
            let mut corrupt = primary.clone();
            corrupt.payload[crate::context_evidence::READ_ACQUISITION_FIELD]["body"]
                ["source_sha256"] = json!(crate::context_evidence::read_sha256(b"forged"));
            assert!(ExecutionCommitService::validate_proof_bound_tool_request(
                &request, &corrupt, &index
            )
            .is_err());
            let mut corrupt_index = index.clone();
            corrupt_index.transaction_id = "foreign-transaction".into();
            assert!(ExecutionCommitService::validate_proof_bound_tool_request(
                &request,
                &primary,
                &corrupt_index
            )
            .is_err());
        }
    }

    #[test]
    fn incomplete_scope_metadata_is_not_an_execution_denial_or_a_strong_proof() {
        for (command, expected_kind) in [
            ("printf authorized", ToolEffectKind::Read),
            ("printf authorized > proof.txt", ToolEffectKind::Write),
        ] {
            let workspace = tempfile::tempdir().unwrap();
            let host = tools::ToolHost::builtin("scope2-process-metadata", workspace.path());
            let request = bound_request(&host, "bash", json!({"command":command}));
            let effect = &request.compiled_invocation.as_ref().unwrap().effect;
            assert_eq!(effect.effect_kind, expected_kind);
            let resolver =
                crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                    .unwrap();
            let scope = crate::bound_tool_batch::admitted_effect_scope(
                &request,
                effect,
                Some(host.pin_snapshot()),
                &resolver,
            )
            .unwrap();
            assert!(!scope.complete);
            ExecutionCommitService::validate_bound_effect_scope(effect, &scope).unwrap();
            let mut overclaim = scope.clone();
            overclaim.complete = true;
            assert!(
                matches!(ExecutionCommitService::validate_bound_effect_scope(effect, &overclaim),
                Err(ExecutionCommitError::InvalidCommand(message)) if message == "bound scope does not cover its admitted effect descriptor")
            );
            let (store, service, graph, root, owner) = root_fixture();
            let attempt = graph.recovery_cursor.node_attempts[owner.node_id.as_ref().unwrap()];
            let state = service
                .begin_root_tool_effect(&request, effect, &root, &scope, &owner, attempt)
                .unwrap();
            let suffix = if expected_kind == ToolEffectKind::Read {
                assert_eq!(state, ToolEffectState::NotRequired);
                "read-intent"
            } else {
                assert_eq!(state, ToolEffectState::Fresh);
                "intent"
            };
            let stream = format!("execution-effect:{}", request.idempotency_key);
            let intent = store
                .event_by_idempotency_key(&stream, &format!("{}:{suffix}", request.idempotency_key))
                .unwrap()
                .unwrap();
            assert_eq!(
                intent.payload["resolved_effect_scope"],
                serde_json::to_value(&scope).unwrap()
            );
            assert_eq!(
                intent.payload["root_effect_owner"]["binding"],
                serde_json::to_value(&owner).unwrap()
            );
            assert!(store
                .event_by_idempotency_key(&stream, &format!("{}:receipt", request.idempotency_key))
                .unwrap()
                .is_none());
            assert!(!workspace.path().join("proof.txt").exists());
        }
    }
}

fn execution_effect_intent_event(
    ticket: &super::registry::NodeExecutionTicket,
) -> Result<RuntimeTransactionEventInput, serde_json::Error> {
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: ExecutionNodeBinding::effect_stream_id(ticket),
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution.effect.intent".to_string(),
            status: Some("inflight".to_string()),
            actor: Some(ticket.executor_kind.clone()),
            refs: vec![RuntimeEventRef {
                kind: "execution_node".to_string(),
                id: ticket.node_id.clone(),
            }],
            payload: serde_json::to_value(ticket)?,
        },
        idempotency_key: Some(format!("{}:intent", ticket.idempotency_key)),
        schema_version: 1,
    })
}

fn execution_effect_receipt_event(
    ticket: &super::registry::NodeExecutionTicket,
    outcome: &super::registry::NodeExecutionOutcome,
) -> Result<RuntimeTransactionEventInput, serde_json::Error> {
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: ExecutionNodeBinding::effect_stream_id(ticket),
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution.effect.receipt".to_string(),
            status: Some("completed".to_string()),
            actor: Some(ticket.executor_kind.clone()),
            refs: vec![RuntimeEventRef {
                kind: "execution_node".to_string(),
                id: ticket.node_id.clone(),
            }],
            payload: serde_json::to_value(outcome)?,
        },
        idempotency_key: Some(format!("{}:receipt", ticket.idempotency_key)),
        schema_version: 1,
    })
}

#[derive(Debug, Error)]
pub enum ExecutionCommitError {
    #[error(transparent)]
    EventStore(#[from] RuntimeEventStoreError),
    #[error(transparent)]
    Transition(#[from] ExecutionTransitionError),
    #[error("execution graph commit serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error(
        "execution graph `{graph_id}` revision mismatch: expected {expected}, actual {actual}"
    )]
    StaleRevision {
        graph_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("execution graph command is not valid in the current state: {0}")]
    InvalidCommand(String),
    #[error("execution graph registration was already applied for continuation root `{graph_id}`")]
    AlreadyAppliedSame { graph_id: String },
    #[error("domain event attempted to mutate canonical graph stream `{0}`")]
    GraphStreamCollision(String),
    #[error("domain event for stream `{0}` has no stable idempotency key")]
    MissingDomainIdempotency(String),
    #[error("graph executor cannot emit protected domain scope `{0}`")]
    ProtectedDomainScope(String),
    #[error("execution commit blocking task failed: {0}")]
    BlockingTask(String),
    #[error("execution graph replan is invalid: {0}")]
    InvalidReplan(String),
}

#[derive(Clone)]
struct PreparedGraphAppend {
    graph: ExecutionGraph,
    transaction_id: String,
    expected_streams: Vec<ExpectedStreamRevision>,
    events: Vec<RuntimeTransactionEventInput>,
    terminal: Option<SessionTerminalInput>,
    authored_metadata: Option<serde_json::Value>,
    refreshable_lineage: BTreeSet<String>,
    process_input_frame: Option<Arc<crate::agentic::review_evidence::ProcessInputFrame>>,
}

pub(crate) struct ExecutionTerminalCommit {
    pub(crate) node_id: String,
    pub(crate) outcome: super::registry::NodeExecutionOutcome,
    pub(crate) effect_ticket: Option<super::registry::NodeExecutionTicket>,
}

#[cfg(test)]
#[path = "../tests/commit.rs"]
mod tests;

pub struct ExecutionCommitReceipt {
    pub graph: ExecutionGraph,
    pub transaction: AppendTransactionReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionEffectState {
    Fresh,
    Completed(super::registry::NodeExecutionOutcome),
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolEffectState {
    NotRequired,
    Fresh,
    Completed(crate::RuntimeToolExecutionOutcome),
    Uncertain,
}

pub(crate) struct ToolReceiptReplay {
    pub invocation: harness_contract::tool::GovernedToolInvocation,
    pub outcome: crate::RuntimeToolExecutionOutcome,
    pub completed_at_ms: u64,
}

/// Canonical ToolHost receipt indexed by the immutable delegated Agent
/// attempt.  The effect stream remains the idempotency/fencing owner; this
/// compact index only gives recovery an exact, bounded way to reload the
/// receipt set that an acceptance verdict was based on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableAgentToolReceipt {
    pub committed_cursor: u64,
    pub effect_scope: Option<harness_contract::policy::PermissionScope>,
    pub effect_scopes: Vec<harness_contract::policy::PermissionScope>,
    pub sequence: u64,
    pub effect_kind: ToolEffectKind,
    pub authorized_scopes: Vec<String>,
    pub outcome: crate::RuntimeToolExecutionOutcome,
}

#[derive(Clone)]
pub struct ExecutionCommitService {
    effect_authority_actions: crate::agentic::AgentActionService,
    effect_authority_graphs: crate::ExecutionGraphStateStore,
    event_store: Arc<RuntimeEventStore>,
    hot_state: Arc<RuntimeHotStatePlane>,
    hot_graphs: Arc<HotExecutionGraphRegistry>,
    proof_artifacts: Option<Arc<crate::ArtifactStore>>,
    proof_session_queries:
        Option<Arc<std::sync::OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>>,
    process_input_verifier: Option<crate::agentic::review_evidence::ProcessInputVerifier>,
    #[cfg(any(test, feature = "test-fixtures"))]
    terminal_response_losses:
        Arc<std::sync::Mutex<BTreeMap<String, Arc<std::sync::atomic::AtomicUsize>>>>,
    #[cfg(test)]
    fail_next_bound_outcome_450: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(any(test, feature = "test-fixtures"))]
    terminal_append_failures:
        Arc<std::sync::Mutex<BTreeMap<String, tokio::sync::oneshot::Sender<String>>>>,
    #[cfg(any(test, feature = "test-fixtures"))]
    terminal_append_observers: Arc<std::sync::Mutex<BTreeMap<String, TerminalAppendObservation>>>,
    #[cfg(any(test, feature = "test-fixtures"))]
    receipt_commit_probes: ReceiptCommitProbes,
}

#[cfg(any(test, feature = "test-fixtures"))]
struct TerminalAppendObservation {
    expected: tokio::sync::oneshot::Sender<Vec<ExpectedStreamRevision>>,
    release: std::sync::mpsc::Receiver<()>,
}

fn merge_outcome_source_revisions(
    target: &mut BTreeMap<String, u64>,
    sources: &BTreeMap<String, u64>,
) -> Result<(), ExecutionCommitError> {
    for (stream_id, revision) in sources {
        if target
            .get(stream_id)
            .is_some_and(|previous| previous != revision)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "outcome source conflicts with causal read-set".into(),
            ));
        }
    }
    target.extend(
        sources
            .iter()
            .map(|(stream_id, revision)| (stream_id.clone(), *revision)),
    );
    Ok(())
}

impl ExecutionCommitService {
    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) fn receipt_commit_probe_for_test(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        callback: impl FnOnce() + Send + 'static,
    ) -> ReceiptCommitProbe {
        ReceiptCommitProbe::install(&self.receipt_commit_probes, request, callback)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn observe_terminal_append_for_test(
        &self,
        terminal_id: &str,
    ) -> Result<
        (
            tokio::sync::oneshot::Receiver<Vec<ExpectedStreamRevision>>,
            std::sync::mpsc::Sender<()>,
        ),
        String,
    > {
        if terminal_id.is_empty() {
            return Err("append observer requires exact terminal".into());
        }
        let mut observers = self
            .terminal_append_observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if observers.contains_key(terminal_id) {
            return Err("duplicate append observer".into());
        }
        let (sender, expected) = tokio::sync::oneshot::channel();
        let (release, receiver) = std::sync::mpsc::channel();
        observers.insert(
            terminal_id.into(),
            TerminalAppendObservation {
                expected: sender,
                release: receiver,
            },
        );
        Ok((expected, release))
    }
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn observe_terminal_append_failure_for_test(
        &self,
        terminal_id: &str,
    ) -> Result<tokio::sync::oneshot::Receiver<String>, String> {
        if terminal_id.is_empty() {
            return Err("append observer requires exact terminal".into());
        }
        let mut observers = self
            .terminal_append_failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if observers.contains_key(terminal_id) {
            return Err("duplicate append failure observer".into());
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        observers.insert(terminal_id.into(), sender);
        Ok(receiver)
    }
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn lose_terminal_response_once_for_test(
        &self,
        terminal_id: &str,
    ) -> Result<Arc<std::sync::atomic::AtomicUsize>, String> {
        if terminal_id.is_empty() {
            return Err("terminal response loss requires exact identity".into());
        }
        let mut entries = self
            .terminal_response_losses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.contains_key(terminal_id) {
            return Err("duplicate terminal response loss".into());
        }
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        entries.insert(terminal_id.into(), hits.clone());
        Ok(hits)
    }
    /// Reload the exact ToolHost receipts for one delegated Agent attempt.
    /// The stream name is a durable parent graph/node/attempt key, so restart
    /// never scans unrelated effect streams or reconstructs facts from the
    /// live filesystem.
    /// Every delegated Agent tool receipt for one physical node, across all of
    /// its attempts.
    ///
    /// Attempts are sequential from 1, so the union is bounded by the node's own
    /// history. The per-attempt loader stays the authority for anything that
    /// needs attempt-exact facts; this one exists because a read is a fact about
    /// the Agent, not about one dispatch of it. One live reviewer was settled
    /// twice for "not having read" artifacts it had read in an earlier attempt of
    /// the same node, and each settlement forced a fresh attempt that started
    /// from zero reads.
    pub fn load_delegated_agent_tool_receipts_all_attempts(
        &self,
        graph_id: &str,
        node_id: &str,
        max_attempt: u32,
    ) -> Result<Vec<DurableAgentToolReceipt>, ExecutionCommitError> {
        let mut merged = Vec::new();
        for attempt in 1..=max_attempt.max(1) {
            let mut receipts =
                self.load_delegated_agent_tool_receipts(graph_id, node_id, attempt)?;
            merged.append(&mut receipts);
        }
        Ok(merged)
    }

    pub(crate) fn receipt_observation_sequence(event: &crate::DurableRuntimeEvent) -> Option<u64> {
        if event.kind != "execution.agent_tool.receipt" {
            return None;
        }
        let sequence = event.payload["sequence"].as_u64()?;
        match event.schema_version {
            1 => Some(sequence),
            2 => {
                let base = event.payload["sequence_base"].as_u64()?;
                let revision = event.payload["index_revision_before"].as_u64()?;
                if base.checked_add(1) != Some(sequence)
                    || revision.checked_add(1) != Some(event.sequence)
                    || base < revision
                    || event.payload["sequence_owner"].as_str() != Some("original_receipt_index")
                {
                    return None;
                }
                event.payload["source_observation_wave_sequence"].as_u64()
            }
            _ => None,
        }
    }

    pub(crate) fn receipt_payload_observation_sequence(
        schema: u32, payload: &serde_json::Value,
    ) -> Option<u64> {
        match schema {
            1 => payload["sequence"].as_u64(),
            2 => {
                let sequence = payload["sequence"].as_u64()?;
                let base = payload["sequence_base"].as_u64()?;
                let revision = payload["index_revision_before"].as_u64()?;
                if base.checked_add(1) != Some(sequence) || base < revision
                    || payload["sequence_owner"].as_str() != Some("original_receipt_index")
                {
                    return None;
                }
                payload["source_observation_wave_sequence"].as_u64()
            }
            _ => None,
        }
    }

    fn receipt_index_high_water(
        events: &[crate::DurableRuntimeEvent],
    ) -> Result<u64, ExecutionCommitError> {
        let mut high_water = 0_u64;
        let mut sequences = BTreeSet::new();
        for event in events.iter().filter(|event| event.kind == "execution.agent_tool.receipt") {
            if Self::receipt_observation_sequence(event).is_none() {
                return Err(ExecutionCommitError::InvalidCommand(
                    "delegated Agent receipt index sequence contract is invalid".into(),
                ));
            }
            let sequence = event.payload["sequence"].as_u64().unwrap();
            if !sequences.insert(sequence) {
                return Err(ExecutionCommitError::InvalidCommand(
                    "delegated Agent receipt index contains duplicate causal sequence".into(),
                ));
            }
            if event.schema_version == 2
                && event.payload["sequence_base"].as_u64()
                    != Some(high_water.max(event.sequence.saturating_sub(1)))
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "delegated Agent receipt index high-water fence is invalid".into(),
                ));
            }
            high_water = high_water.max(sequence);
        }
        Ok(high_water)
    }

    fn allocate_receipt_sequence(
        &self,
        receipt: &mut RuntimeTransactionEventInput,
        expected: &mut BTreeMap<String, u64>,
        pending: &[RuntimeTransactionEventInput],
    ) -> Result<(), ExecutionCommitError> {
        let stream = receipt.event.stream_id.clone();
        let revision = *expected.entry(stream.clone())
            .or_insert(self.event_store.stream_revision(&stream)?);
        let history = self.event_store.list_stream(&stream)
            .map_err(RuntimeEventStoreError::Corrupt)?;
        let high_water = Self::receipt_index_high_water(&history)?;
        let prior = pending.iter().filter(|event| event.event.stream_id == stream)
            .collect::<Vec<_>>();
        let pending_count = u64::try_from(prior.len()).map_err(|_| {
            ExecutionCommitError::InvalidCommand("receipt sequence overflow".into())
        })?;
        let index_revision = revision.checked_add(pending_count).ok_or_else(|| {
            ExecutionCommitError::InvalidCommand("receipt sequence overflow".into())
        })?;
        let base = prior.iter().filter_map(|event| event.event.payload["sequence"].as_u64())
            .fold(high_water.max(index_revision), u64::max);
        let sequence = base.checked_add(1).ok_or_else(|| {
            ExecutionCommitError::InvalidCommand("receipt sequence overflow".into())
        })?;
        receipt.event.payload["sequence"] = json!(sequence);
        receipt.event.payload["sequence_base"] = json!(base);
        receipt.event.payload["index_revision_before"] = json!(index_revision);
        receipt.event.payload["sequence_owner"] = json!("original_receipt_index");
        Ok(())
    }

    pub(crate) fn delegated_observation_high_water(
        &self, graph_id: &str, node_id: &str, attempt: u32,
    ) -> Result<u64, ExecutionCommitError> {
        let stream = format!("execution-agent-receipts:{graph_id}:{node_id}:{attempt}");
        let events = self.event_store.list_stream(&stream)
            .map_err(RuntimeEventStoreError::Corrupt)?;
        Self::receipt_index_high_water(&events)?;
        Ok(events.iter().filter_map(Self::receipt_observation_sequence).max().unwrap_or(0))
    }

    pub(crate) fn exact_delegated_receipt_sequence(
        &self, request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<u64, ExecutionCommitError> {
        let invalid = || ExecutionCommitError::InvalidCommand(
            "original effect/Step receipt identity mismatch".into());
        let parent = request.parent_execution.as_ref().ok_or_else(invalid)?;
        let attempt = request.parent_execution_attempt.ok_or_else(invalid)?;
        let stream = delegated_agent_receipt_stream_id(request).ok_or_else(invalid)?;
        let index = self.event_store.event_by_idempotency_key(
            &stream, &delegated_agent_receipt_key(request),
        )?.ok_or_else(invalid)?;
        self.load_delegated_agent_tool_receipts(&parent.execution_id, &parent.node_id, attempt)?;
        if request.step_binding.as_ref().is_some_and(|binding|
            binding.effect_idempotency_key() != request.idempotency_key)
            || Self::receipt_observation_sequence(&index) != Some(request.observation_wave_sequence)
            || index.payload["idempotency_key"] != request.idempotency_key
            || index.payload["outcome"]["tool_use_id"] != request.tool_use_id
            || index.payload["outcome"]["tool_name"] != request.tool_name
            || index.scope != RuntimeEventScope::ExecutionNode
            || index.actor.as_deref() != Some("governed_tool")
        {
            return Err(invalid());
        }
        let read = index.payload["effect_kind"] == "read";
        let suffix = if read { "read-receipt" } else { "receipt" };
        let primary = self.event_store.event_by_idempotency_key(
            &format!("execution-effect:{}", request.idempotency_key),
            &format!("{}:{suffix}", request.idempotency_key),
        )?.ok_or_else(invalid)?;
        if primary.kind != if read { "execution.read.receipt" } else { "execution.effect.receipt" }
            || primary.schema_version != 1
            || primary.actor != index.actor || primary.scope != index.scope
            || primary.transaction_id != index.transaction_id
            || primary.commit_cursor != index.commit_cursor
            || primary.transaction_index >= index.transaction_index
            || primary.refs != index.refs
            || primary.payload["replay"] != receipt_replay_contract(request)?
            || primary.payload["outcome"] != index.payload["outcome"]
            || index.payload["authorized_scopes"] != serde_json::to_value(&request.authorized_scopes)?
            || index.payload["effect_scope"] != serde_json::to_value(
                request.authorization.as_ref().map(|authorization| &authorization.scope))?
        {
            return Err(invalid());
        }
        if read {
            validate_readonly_tool_receipt(request, &primary.payload)?;
        } else {
            let effect: ToolEffectDescriptor = serde_json::from_value(primary.payload["effect"].clone())?;
            validate_mutation_tool_fingerprint(request, &effect, &primary.payload, "receipt")?;
        }
        if let Some(invocation) = &request.compiled_invocation {
            self.validate_terminal_scope(request, &invocation.effect, &primary, read)?;
        }
        index.payload["sequence"].as_u64().ok_or_else(invalid)
    }

    pub fn load_delegated_agent_tool_receipts(
        &self,
        graph_id: &str,
        node_id: &str,
        attempt: u32,
    ) -> Result<Vec<DurableAgentToolReceipt>, ExecutionCommitError> {
        let stream_id = format!("execution-agent-receipts:{graph_id}:{node_id}:{attempt}");
        let history = self.event_store.list_stream(&stream_id)
            .map_err(RuntimeEventStoreError::Corrupt)?;
        Self::receipt_index_high_water(&history)?;
        let mut receipts = history.into_iter()
            .filter(|event| event.kind == "execution.agent_tool.receipt")
            .map(|event| {
                let sequence = event.payload["sequence"].as_u64().ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "delegated Agent tool receipt has no sequence".to_string(),
                    )
                })?;
                let effect_kind = serde_json::from_value(event.payload["effect_kind"].clone())?;
                let authorized_scopes =
                    serde_json::from_value(event.payload["authorized_scopes"].clone())?;
                let outcome = serde_json::from_value(event.payload["outcome"].clone())?;
                Ok(DurableAgentToolReceipt {
                    committed_cursor: event.commit_cursor,
                    effect_scope: serde_json::from_value(event.payload["effect_scope"].clone())?,
                    effect_scopes: event
                        .payload
                        .get("effect_scopes")
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()?
                        .unwrap_or_default(),
                    sequence,
                    effect_kind,
                    authorized_scopes,
                    outcome,
                })
            })
            .collect::<Result<Vec<_>, ExecutionCommitError>>()?;
        receipts.sort_by_key(|receipt| receipt.sequence);
        if receipts
            .windows(2)
            .any(|window| window[0].sequence == window[1].sequence)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "delegated Agent receipt index contains duplicate causal sequence".to_string(),
            ));
        }
        Ok(receipts)
    }

    pub fn commit_readonly_tool_receipts(
        &self,
        receipts: &[(
            crate::RuntimeToolExecutionRequest,
            crate::RuntimeToolExecutionOutcome,
        )],
    ) -> Result<(), ExecutionCommitError> {
        self.commit_readonly_tool_receipts_scoped(receipts, None, None, false)
    }

    pub(crate) fn commit_bound_readonly_tool_receipt(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        outcome: &crate::RuntimeToolExecutionOutcome,
        effect: &ToolEffectDescriptor,
        scope: &harness_contract::goal::ProofEffectScope,
    ) -> Result<(), ExecutionCommitError> {
        Self::validate_bound_effect_scope(effect, scope)?;
        self.commit_readonly_tool_receipts_scoped(
            &[(request.clone(), outcome.clone())],
            Some(scope),
            None,
            false,
        )
    }

    /// Commit a read-class receipt that carries the Runtime-minted host
    /// process closure fact. The host shell proves quiescence only; it never
    /// carries the private bounded-process capability.
    pub(crate) fn commit_bound_readonly_tool_receipt_with_host_process(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        outcome: &crate::RuntimeToolExecutionOutcome,
        effect: &ToolEffectDescriptor,
        scope: &harness_contract::goal::ProofEffectScope,
    ) -> Result<(), ExecutionCommitError> {
        Self::validate_bound_effect_scope(effect, scope)?;
        Self::validate_host_process_outcome(outcome)?;
        self.commit_readonly_tool_receipts_scoped(
            &[(request.clone(), outcome.clone())],
            Some(scope),
            None,
            true,
        )
    }

    fn commit_readonly_tool_receipts_scoped(
        &self,
        receipts: &[(
            crate::RuntimeToolExecutionRequest,
            crate::RuntimeToolExecutionOutcome,
        )],
        scope: Option<&harness_contract::goal::ProofEffectScope>,
        root_intent: Option<&crate::DurableRuntimeEvent>,
        host_process: bool,
    ) -> Result<(), ExecutionCommitError> {
        self.commit_readonly_tool_receipts_with_closure(
            receipts,
            scope,
            root_intent,
            host_process,
            None,
        )
    }

    /// The read receipt of a process-class read carries the published closure
    /// envelope and the admission reference, so the production consumer can
    /// settle it against the read key family exactly like a mutation receipt
    /// is settled against the mutation key family.
    fn commit_readonly_tool_receipts_with_closure(
        &self,
        receipts: &[(
            crate::RuntimeToolExecutionRequest,
            crate::RuntimeToolExecutionOutcome,
        )],
        scope: Option<&harness_contract::goal::ProofEffectScope>,
        root_intent: Option<&crate::DurableRuntimeEvent>,
        host_process: bool,
        closure: Option<&serde_json::Value>,
    ) -> Result<(), ExecutionCommitError> {
        for (_, outcome) in receipts {
            Self::reject_public_process_facts(outcome, host_process)?;
        }
        if receipts.is_empty() {
            return Ok(());
        }
        // Optimistic concurrency: this wave snapshots the current stream
        // revisions and appends with compare-and-set. Concurrent read receipts
        // from the same Agent share one `execution-agent-receipts:*` stream, so
        // a wave can lose a race that has nothing to do with the tool result
        // (observed as `revision mismatch` on `agent:N`). The write is
        // idempotent (stable keys plus the pre-check inside), so retry the whole
        // wave on a stale revision instead of failing an otherwise-good call.
        const MAX_ATTEMPTS: u8 = 8;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.commit_readonly_tool_receipts_once(receipts, scope, root_intent, closure) {
                Ok(()) => return Ok(()),
                Err(ExecutionCommitError::EventStore(RuntimeEventStoreError::StaleRevision {
                    ..
                })) if attempt < MAX_ATTEMPTS => continue,
                Err(error) => return Err(error),
            }
        }
    }

    fn commit_readonly_tool_receipts_once(
        &self,
        receipts: &[(
            crate::RuntimeToolExecutionRequest,
            crate::RuntimeToolExecutionOutcome,
        )],
        scope: Option<&harness_contract::goal::ProofEffectScope>,
        root_intent: Option<&crate::DurableRuntimeEvent>,
        closure: Option<&serde_json::Value>,
    ) -> Result<(), ExecutionCommitError> {
        if receipts.is_empty() {
            return Ok(());
        }
        let mut expected_streams = BTreeMap::<String, u64>::new();
        let mut events = Vec::with_capacity(receipts.len().saturating_mul(2));
        let mut receipt_keys = Vec::with_capacity(receipts.len());
        let mut staged = BTreeMap::new();
        for (request, outcome) in receipts {
            let identity = json!([receipt_replay_contract(request)?, request.tool_name,
                request.input, request.observation_wave_sequence, request.authorized_scopes,
                request.authorization, bounded_tool_effect_outcome(outcome)]);
            if let Some(previous) = staged.insert(request.idempotency_key.clone(), identity.clone()) {
                if previous != identity {
                    return Err(ExecutionCommitError::InvalidCommand("read wave original effect collision".into()));
                }
                continue;
            }
            let effect = request
                .compiled_invocation
                .as_ref()
                .map(|invocation| &invocation.effect);
            if let Some(effect) = effect {
                local_network::validate_outcome(request, effect, outcome)?;
            }
            let stream_id = format!("execution-effect:{}", request.idempotency_key);
            let receipt_key = format!("{}:read-receipt", request.idempotency_key);
            if let Some(existing) = self
                .event_store
                .event_by_idempotency_key(&stream_id, &receipt_key)?
            {
                validate_readonly_tool_receipt(request, &existing.payload)?;
                if let Some(effect) = effect {
                    self.validate_terminal_scope(request, effect, &existing, true)?;
                } else if existing.payload["resolved_effect_scope"]["version"] == 2 {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "scope2 read replay requires its original invocation".into(),
                    ));
                }
                continue;
            }
            let root_owner = if let Some(intent) = root_intent {
                let root: crate::CowdExecutionContext =
                    serde_json::from_value(intent.payload["root_effect_context"].clone())?;
                let effect: ToolEffectDescriptor =
                    serde_json::from_value(intent.payload["effect"].clone())?;
                let scope = scope.ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand("root read receipt has no scope".into())
                })?;
                let sources = self.root_effect_authority(&root)?;
                let (owner, attempt) =
                    self.root_intent_owner(request, &effect, &root, scope, intent)?;
                let revision = self.authenticated_root_owner(&root, &owner, attempt)?;
                if !sources.iter().any(|source| {
                    source.stream_id == root.execution_id && source.expected_revision == revision
                }) {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "root read owner changed during receipt snapshot".into(),
                    ));
                }
                for source in sources {
                    if expected_streams
                        .insert(source.stream_id, source.expected_revision)
                        .is_some_and(|previous| previous != source.expected_revision)
                    {
                        return Err(ExecutionCommitError::InvalidCommand(
                            "root read authority snapshot conflict".into(),
                        ));
                    }
                }
                Some((owner, attempt))
            } else {
                None
            };
            if let Some(scope) = scope {
                Self::validate_bound_effect_scope(
                    effect.ok_or_else(|| {
                        ExecutionCommitError::InvalidCommand(
                            "scoped read has no compiled descriptor".into(),
                        )
                    })?,
                    scope,
                )?;
            }
            expected_streams.insert(
                stream_id.clone(),
                self.event_store.stream_revision(&stream_id)?,
            );
            let acquisition = (outcome.status == crate::RuntimeToolExecutionStatus::Executed
                && outcome.error.is_none())
            .then(|| {
                outcome.output.as_deref().and_then(|output| {
                    if let Some((owner, attempt)) = &root_owner {
                        let parent = harness_contract::execution_graph::ExecutionParentBinding {
                            execution_id: owner.root_execution_id.clone(),
                            node_id: owner.node_id.clone().unwrap_or_default(),
                        };
                        crate::context_evidence::read_acquisition_seal_at(
                            request, output, &parent, *attempt,
                        )
                    } else {
                        crate::context_evidence::read_acquisition_seal(request, output)
                    }
                })
            })
            .flatten();
            let mut read_payload = local_network::receipt_payload(&self.event_store, request, json!({
                "effect":effect,
                "root_effect_context":root_intent.map(|intent| &intent.payload["root_effect_context"]),
                "root_effect_owner":root_intent.map(|intent| &intent.payload["root_effect_owner"]),
                "idempotency_key": request.idempotency_key,
                "tool_use_id": request.tool_use_id,
                "tool_name": request.tool_name,
                "replay": receipt_replay_contract(request)?,
                "input_sha256": format!("sha256:{:x}", Sha256::digest(request.input.as_bytes())),
                "outcome_truncated": tool_effect_outcome_requires_truncation(outcome),
                "outcome": bounded_tool_effect_outcome(outcome),
                "resolved_effect_scope":scope,
                "read_acquisition_v1": acquisition,
                "process_admission_ref": closure.map(|_| format!("{}:process-admission", request.idempotency_key)),
                "process_closure": closure.cloned(),
            }))?;
            if acquisition.is_some() {
                read_payload["original_input"] = json!(request.input);
            }
            events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.read.receipt".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("governed_tool".to_string()),
                    refs: root_owner.as_ref().map_or_else(|| tool_effect_refs(request),
                        |(owner, attempt)| root_effect_receipt_refs(request, owner, *attempt)),
                    payload: read_payload,
                },
                idempotency_key: Some(receipt_key.clone()),
                schema_version: 1,
            });
            if let Some(mut agent_receipt) =
                delegated_agent_receipt_event(request, ToolEffectKind::Read, outcome).or_else(
                    || {
                        root_owner.as_ref().map(|(owner, attempt)| {
                            root_index_receipt_event(
                                request,
                                ToolEffectKind::Read,
                                outcome,
                                owner,
                                *attempt,
                            )
                        })
                    },
                )
            {
                agent_receipt.event.payload["root_effect_context"] = root_intent
                    .map_or(serde_json::Value::Null, |intent| {
                        intent.payload["root_effect_context"].clone()
                    });
                agent_receipt.event.payload["root_effect_owner"] = root_intent
                    .map_or(serde_json::Value::Null, |intent| {
                        intent.payload["root_effect_owner"].clone()
                    });
                // The per-attempt index carries the same closure envelope and
                // admission reference as the primary receipt, so a consumer
                // that reads either one sees the same proven closure.
                if let Some(closure) = closure {
                    agent_receipt.event.payload["process_closure"] = closure.clone();
                    agent_receipt.event.payload["process_admission_ref"] = json!(format!(
                        "{}:process-admission",
                        request.idempotency_key
                    ));
                }
                agent_receipt.event.payload["effect"] = serde_json::to_value(effect)?;
                agent_receipt.event.payload["resolved_effect_scope"] = serde_json::to_value(scope)?;
                agent_receipt.event.payload[crate::context_evidence::READ_ACQUISITION_FIELD] =
                    serde_json::to_value(&acquisition)?;
                self.allocate_receipt_sequence(&mut agent_receipt, &mut expected_streams, &events)?;
                events.push(agent_receipt);
            }
            receipt_keys.push(receipt_key);
        }
        if events.is_empty() {
            return Ok(());
        }
        receipt_keys.sort();
        let digest = format!("{:x}", Sha256::digest(receipt_keys.join("\n").as_bytes()));
        let appended = self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("readonly-tool-wave:{digest}"),
                expected_streams: expected_streams
                    .into_iter()
                    .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                        stream_id,
                        expected_revision,
                    })
                    .collect(),
                events,
            });
        if let Err(error) = appended {
            if receipts.iter().all(|(request, _)| self.event_store.event_by_idempotency_key(
                &format!("execution-effect:{}", request.idempotency_key),
                &format!("{}:read-receipt", request.idempotency_key),
            ).is_ok_and(|receipt| receipt.is_some())) {
                return self.commit_readonly_tool_receipts_once(receipts, scope, root_intent, None);
            }
            return Err(error.into());
        }
        Ok(())
    }

    pub fn begin_tool_effect(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
    ) -> Result<ToolEffectState, ExecutionCommitError> {
        self.begin_tool_effect_scoped(request, effect, None, None, None)
    }

    pub(crate) fn bounded_process_artifacts(
        &self,
    ) -> Result<Arc<crate::ArtifactStore>, ExecutionCommitError> {
        self.proof_artifacts.clone().ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "bounded process requires the original ArtifactStore".into(),
            )
        })
    }

    pub(crate) fn bounded_process_uses_store(&self, store: &Arc<RuntimeEventStore>) -> bool {
        Arc::ptr_eq(&self.event_store, store)
    }

    pub(crate) fn bounded_process_service_class(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        root: Option<&crate::CowdExecutionContext>,
    ) -> Result<super::ExecutionServiceClass, ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        let graph_id = request
            .parent_execution
            .as_ref()
            .map(|parent| parent.execution_id.as_str())
            .or_else(|| root.map(|root| root.execution_id.as_str()))
            .ok_or_else(|| {
                invalid("bounded process original service-class owner missing".into())
            })?;
        let graph = self
            .effect_authority_graphs
            .load(graph_id)
            .map_err(|error| invalid(error.to_string()))?;
        if graph.lineage.as_ref().is_none_or(|lineage| {
            request.session_id.as_deref() != Some(lineage.session_id.as_str())
        }) {
            return Err(invalid(
                "bounded process original service-class lineage mismatch".into(),
            ));
        }
        Ok(graph.service_class)
    }

    fn bounded_process_sources(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        intent: &crate::DurableRuntimeEvent,
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        validate_mutation_tool_fingerprint(request, effect, &intent.payload, "intent")?;
        validate_replay_effect_scope(&intent.payload, effect)?;
        if !intent.payload["root_effect_owner"].is_null() {
            let root = serde_json::from_value(intent.payload["root_effect_context"].clone())?;
            let scope = serde_json::from_value(intent.payload["resolved_effect_scope"].clone())?;
            let (owner, attempt) =
                self.root_intent_owner(request, effect, &root, &scope, intent)?;
            let sources = self.root_effect_authority(&root)?;
            let revision = self.authenticated_root_owner(&root, &owner, attempt)?;
            if !sources.iter().any(|source| {
                source.stream_id == root.execution_id && source.expected_revision == revision
            }) {
                return Err(ExecutionCommitError::InvalidCommand(
                    "bounded process root authority changed".into(),
                ));
            }
            self.validate_system_tool_parent_scoped(request, Some(&root))?;
            Ok(sources)
        } else {
            self.validate_system_tool_parent_scoped(request, None)?;
            let parent = request.parent_execution.as_ref().ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(
                    "bounded process has neither original root owner nor parent".into(),
                )
            })?;
            let graph = self
                .effect_authority_graphs
                .load(&parent.execution_id)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            if graph.node_statuses.get(&parent.node_id) != Some(&ExecutionNodeStatus::Running)
                || request.parent_execution_attempt.is_none_or(|attempt| {
                    attempt == 0
                        || graph
                            .recovery_cursor
                            .node_attempts
                            .get(&parent.node_id)
                            .copied()
                            != Some(attempt)
                })
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "bounded process original parent attempt is no longer running".into(),
                ));
            }
            self.tool_effect_authority(request)
        }
    }

    pub(crate) fn bounded_process_descriptor_sha256(
        effect: &ToolEffectDescriptor,
    ) -> Result<String, ExecutionCommitError> {
        let digest = harness_contract::step::canonical_digest(
            harness_contract::context::TOOL_EXECUTION_DESCRIPTOR_DOMAIN,
            &serde_json::to_value(effect)?,
        )
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        digest
            .strip_prefix("sha256:")
            .map(str::to_owned)
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(
                    "canonical descriptor digest encoding invalid".into(),
                )
            })
    }

    pub(crate) fn bounded_process_input_sha256(
        input: &serde_json::Value,
    ) -> Result<String, ExecutionCommitError> {
        let digest = harness_contract::step::canonical_digest(
            harness_contract::context::TOOL_EXECUTION_INPUT_DOMAIN,
            input,
        )
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        digest
            .strip_prefix("sha256:")
            .map(str::to_owned)
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(
                    "canonical input digest encoding invalid".into(),
                )
            })
    }

    pub(crate) fn bounded_process_binding(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
    ) -> Result<harness_contract::context::ToolExecutionBinding, ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        if request.tool_name != "execute_code"
            || !effect.spawns_process
            || request.authorization.is_none()
            || request.step_binding.is_none()
        {
            return Err(invalid(
                "bounded process requires canonical execute_code, grant and Step".into(),
            ));
        }
        if request.policy_revision == 0
            || request.authorization.as_ref().unwrap().policy_revision != request.policy_revision
        {
            return Err(invalid(
                "bounded process grant and request policy revision differ".into(),
            ));
        }
        let step_binding = request.step_binding.as_ref().unwrap();
        let step = step_binding
            .resolve(None)
            .map_err(|error| invalid(error.to_string()))?;
        let input = step
            .validate_call(
                &step_binding.origin,
                request
                    .session_id
                    .as_deref()
                    .ok_or_else(|| invalid("bounded process Session missing".into()))?,
                &request.tool_use_id,
                &request.tool_name,
                &request.input,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|error| invalid(error.to_string()))?
                    .as_millis() as i64,
            )
            .map_err(|error| invalid(error.to_string()))?;
        step.lease()
            .validate_authorization(
                request.authorization.as_ref().unwrap(),
                "execute_code",
                &input,
            )
            .map_err(|error| invalid(error.to_string()))?;
        if request
            .compiled_invocation
            .as_ref()
            .is_none_or(|invocation| invocation.effect != *effect)
        {
            return Err(invalid(
                "bounded process compiled descriptor mismatch".into(),
            ));
        }
        let stream = format!("execution-effect:{}", request.idempotency_key);
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream, &format!("{}:intent", request.idempotency_key))?
            .ok_or_else(|| invalid("bounded process has no original intent".into()))?;
        self.bounded_process_sources(request, effect, &intent)?;
        let packet_digest = if let Some(parent) = &request.parent_execution {
            let graph = self
                .effect_authority_graphs
                .load(&parent.execution_id)
                .map_err(|error| invalid(error.to_string()))?;
            let node = graph
                .nodes
                .iter()
                .find(|node| node.id == parent.node_id)
                .ok_or_else(|| invalid("bounded process parent missing".into()))?;
            if node.kind == ExecutionNodeKind::AgentTask {
                let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)?;
                json!(format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&packet)?)
                ))
            } else {
                serde_json::Value::Null
            }
        } else {
            serde_json::Value::Null
        };
        Ok(harness_contract::context::ToolExecutionBinding {
            call_key: request.idempotency_key.clone(),
            tool_use_id: request.tool_use_id.clone(),
            input_sha256: Self::bounded_process_input_sha256(&input)?,
            descriptor_sha256: Self::bounded_process_descriptor_sha256(effect)?,
            authority_sha256: format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&json!({
                    "version":1,"intent":intent.payload,"request":request,"packet_digest":packet_digest,
                }))?)
            ),
        })
    }

    pub(crate) fn admit_bounded_process(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        prepared: &tools::PreparedToolExecution,
        source_bundle: &harness_contract::context::ArtifactRef,
    ) -> Result<harness_contract::context::ToolExecutionAdmission, ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        let binding = self.bounded_process_binding(request, effect)?;
        if prepared.binding() != &binding {
            return Err(invalid("prepared process authority mismatch".into()));
        }
        let artifacts = self.bounded_process_artifacts()?;
        if source_bundle.bytes > 17 * 1024 * 1024
            || source_bundle.media_type != "application/octet-stream"
            || artifacts
                .read_blocking(source_bundle, &source_bundle.visibility_scope, None)
                .map_err(|error| invalid(error.to_string()))?
                != prepared.source_bundle_bytes()
        {
            return Err(invalid("bounded source archive mismatch".into()));
        }
        let admission = prepared
            .admission(source_bundle)
            .map_err(|error| invalid(error.to_string()))?;
        let stream = format!("execution-effect:{}", request.idempotency_key);
        let key = format!("{}:process-admission", request.idempotency_key);
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream, &format!("{}:intent", request.idempotency_key))?
            .ok_or_else(|| invalid("process admission requires original intent".into()))?;
        let mut sources = self.bounded_process_sources(request, effect, &intent)?;
        let revision = self.event_store.stream_revision(&stream)?;
        if self
            .event_store
            .event_by_idempotency_key(&stream, &key)?
            .is_some()
        {
            return Err(invalid(
                "process already admitted; private handle cannot be recovered or rerun".into(),
            ));
        }
        sources.push(ExpectedStreamRevision {
            stream_id: stream.clone(),
            expected_revision: revision,
        });
        self.event_store.append_transaction(AppendTransactionRequest {
            transaction_id:key.clone(), expected_streams:sources,
            events:vec![RuntimeTransactionEventInput {
                event:RuntimeEventInput { stream_id:stream, scope:intent.scope, kind:"execution.process.admitted".into(),
                    status:Some("inflight".into()), actor:intent.actor.clone(), refs:intent.refs.clone(),
                    payload:json!({"version":1,"admission":admission,"admission_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&admission)?)),
                        "source_workspace_root":prepared.workspace_root(),"original_input":request.input}),
                }, idempotency_key:Some(key), schema_version:1,
            }],
        })?;
        Ok(admission)
    }

    pub(crate) async fn archive_bounded_process_body(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        completed: &tools::CompletedToolExecution,
        artifacts: &crate::ArtifactStore,
    ) -> Result<harness_contract::context::ArtifactRef, ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        let bytes = completed.body_bytes();
        if bytes.len() > 6 * 1024 * 1024 || bytes.len() as u64 > artifacts.config().max_object_bytes
        {
            return Err(invalid(
                "bounded process body exceeds actual archive budget".into(),
            ));
        }
        let body = artifacts
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/json".into(),
                    visibility_scope: format!(
                        "session:{}",
                        request
                            .session_id
                            .as_deref()
                            .ok_or_else(|| invalid("bounded process Session missing".into()))?
                    ),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: None,
                },
                bytes,
            )
            .await
            .map_err(|error| invalid(error.to_string()))?;
        artifacts
            .pin(
                &body,
                &format!("execution-effect:{}", request.idempotency_key),
                i64::MAX as u64,
            )
            .map_err(|error| invalid(error.to_string()))?;
        Ok(body)
    }

    pub(crate) async fn commit_bounded_process(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        completed: tools::CompletedToolExecution,
        artifacts: &crate::ArtifactStore,
    ) -> Result<crate::RuntimeToolExecutionOutcome, ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        let stream = format!("execution-effect:{}", request.idempotency_key);
        let admission = self
            .event_store
            .event_by_idempotency_key(
                &stream,
                &format!("{}:process-admission", request.idempotency_key),
            )?
            .ok_or_else(|| invalid("terminal process has no original admission".into()))?;
        if admission.payload["admission"] != serde_json::to_value(completed.admission())?
            || completed.admission().binding != self.bounded_process_binding(request, effect)?
        {
            return Err(invalid(
                "private terminal/admission authority mismatch".into(),
            ));
        }
        let bytes = completed.body_bytes();
        if bytes.len() > 6 * 1024 * 1024 || bytes.len() as u64 > artifacts.config().max_object_bytes
        {
            return Err(invalid(
                "bounded process body exceeds actual archive budget".into(),
            ));
        }
        let body = artifacts
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/json".into(),
                    visibility_scope: format!(
                        "session:{}",
                        request
                            .session_id
                            .as_deref()
                            .ok_or_else(|| invalid("bounded process session missing".into()))?
                    ),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: None,
                },
                bytes,
            )
            .await
            .map_err(|error| invalid(error.to_string()))?;
        artifacts
            .pin(
                &body,
                &format!("execution-effect:{}", request.idempotency_key),
                i64::MAX as u64,
            )
            .map_err(|error| invalid(error.to_string()))?;
        crate::tool_host::check_bounded_process_commit_authority(request).map_err(|error| {
            invalid(format!(
                "actual terminal archived at {}; live commit authority unavailable: {error}",
                body.selector
            ))
        })?;
        let draft = completed
            .into_output(body)
            .map_err(|error| invalid(error.to_string()))?;
        let mut observations = draft.observations().to_vec();
        if observations.len() > 8
            || observations
                .iter()
                .filter(|observed| {
                    matches!(
                        observed.target,
                        harness_contract::context::EvidenceTargetIdentity::ToolExecution { .. }
                    )
                })
                .count()
                != 1
            || serde_json::to_vec(&observations)?.len() > 128 * 1024
        {
            return Err(invalid("private process metadata exceeds bounds".into()));
        }
        for observation in &mut observations {
            observation.observed_at_sequence = request.observation_wave_sequence;
            observation.evidence_ref =
                Some(harness_contract::context::EvidenceAccessRef::unavailable(
                    harness_contract::reality::EvidenceRef::observed(
                        "tool_execution",
                        format!("tool-effect:{}", request.idempotency_key),
                    ),
                    "application/vnd.cowd.tool-receipt+json",
                    format!("session:{}", request.session_id.as_deref().unwrap()),
                ));
        }
        let outcome = crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(draft.model_text().to_string()),
            error: None,
            evidence_ref: format!("tool-effect:{}", request.idempotency_key),
            observed_evidence: observations,
        };
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream, &format!("{}:intent", request.idempotency_key))?
            .ok_or_else(|| invalid("process closure original intent missing".into()))?;
        let document = process_closure_document(&intent, &outcome)?;
        let encoded = serde_json::to_vec(&document)?;
        if encoded.len() > 32 * 1024 {
            return Err(invalid(
                "process closure metadata exceeds bounded extent".into(),
            ));
        }
        let scope = format!(
            "session:{}",
            request
                .session_id
                .as_deref()
                .ok_or_else(|| invalid("process closure Session missing".into()))?
        );
        let origin = crate::PreparedArtifactOrigin::ToolEffectClosure {
            stream_id: stream.clone(),
            idempotency_key: format!("{}:receipt", request.idempotency_key),
        };
        let authority = ProcessClosurePublicationAuthority {
            origin: origin.clone(),
            sha256: format!("sha256:{:x}", Sha256::digest(&encoded)),
            bytes: encoded.len() as u64,
            scope: scope.clone(),
        };
        let proof = artifacts
            .write_process_closure_blocking(
                &scope,
                &format!("process-closure:{}", request.idempotency_key),
                &encoded,
                &origin,
                &authority,
            )
            .map_err(|error| invalid(error.to_string()))?;
        let closure = json!({"content_schema":2,"sha256":proof.sha256,"bytes":proof.bytes,
            "body":{"kind":"artifact_ref","artifact":proof}});
        self.commit_tool_effect_trusted(
            request,
            effect,
            &outcome,
            ProcessTerminalAuthority::BoundedProcess(&closure),
        )?;
        Ok(outcome)
    }

    /// Write the read intent of a read-class effect that owns a process tree.
    ///
    /// The read family keeps its own identity: the intent is the read intent
    /// and the receipt is the read receipt, so a process-class read is never
    /// recorded as a mutation. A read that owns no process keeps the cheap
    /// path and writes nothing.
    fn begin_read_process_intent(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        scope: Option<&harness_contract::goal::ProofEffectScope>,
    ) -> Result<(), ExecutionCommitError> {
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        let key = format!("{}:read-intent", request.idempotency_key);
        if self
            .event_store
            .event_by_idempotency_key(&stream_id, &key)?
            .is_some()
        {
            return Ok(());
        }
        if let Some(scope) = scope {
            Self::validate_bound_effect_scope(effect, scope)?;
        }
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_transaction(AppendTransactionRequest {
            transaction_id: key.clone(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: stream_id.clone(),
                expected_revision: revision,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.read.intent".into(),
                    status: Some("inflight".into()),
                    actor: Some("governed_tool".into()),
                    refs: tool_effect_refs(request),
                    payload: json!({
                        "idempotency_key": request.idempotency_key,
                        "tool_use_id": request.tool_use_id,
                        "tool_name": request.tool_name,
                        "effect": effect,
                        "resolved_effect_scope": scope,
                        "replay": receipt_replay_contract(request)?,
                        "process_class": true,
                    }),
                },
                idempotency_key: Some(key),
                schema_version: 1,
            }],
        })?;
        Ok(())
    }

    /// Admit a host-shell process effect before its command runs.
    ///
    /// The host executor can only report quiescence *after* the fact, so the
    /// admission has to be durable before the command runs: it binds the
    /// authorized program text, the effect descriptor and the root/delegated
    /// authority snapshot to the effect key, and the receipt may only be
    /// settled against the admission that was published first. The archive of
    /// the authorized program text is written here as well, so the fact that
    /// later claims a terminal can be checked against the exact program that
    /// was admitted.
    ///
    /// Returns `None` for an effect that owns no process tree.
    pub(crate) async fn admit_host_process(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
    ) -> Result<Option<harness_contract::context::ToolExecutionAdmission>, ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        if !effect.spawns_process {
            return Ok(None);
        }
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        let admission_key = format!("{}:process-admission", request.idempotency_key);
        let input = harness_contract::step::parse_strict_json(&request.input)
            .map_err(|error| invalid(error.to_string()))?;
        let digests = json!({
            "call_key": request.idempotency_key,
            "tool_use_id": request.tool_use_id,
            "input_sha256": Self::bounded_process_input_sha256(&input)?,
            "descriptor_sha256": Self::bounded_process_descriptor_sha256(effect)?,
            "authority_sha256": crate::host_process_closure::authority_digest(request)
                .map_err(|error| invalid(error))?,
            "code_sha256": crate::host_process_closure::code_digest(request),
        });
        if let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &admission_key)?
        {
            let published = existing
                .payload
                .get("admission")
                .and_then(|value| serde_json::from_value::<
                    harness_contract::context::ToolExecutionAdmission,
                >(value.clone()).ok())
                .ok_or_else(|| invalid("published host process admission is unreadable".into()))?;
            let admitted = json!({
                "call_key": published.binding.call_key,
                "tool_use_id": published.binding.tool_use_id,
                "input_sha256": published.binding.input_sha256,
                "descriptor_sha256": published.binding.descriptor_sha256,
                "authority_sha256": published.binding.authority_sha256,
                "code_sha256": published.code_sha256,
            });
            if admitted != digests {
                return Err(invalid(
                    "host process admission does not match the original effect".into(),
                ));
            }
            return Ok(Some(published));
        }
        // A read-class effect owns its own key family: its intent is the read
        // intent and its receipt is the read receipt. A process-class read
        // must not borrow the mutation identity.
        let intent_key = if effect.effect_kind == ToolEffectKind::Read {
            format!("{}:read-intent", request.idempotency_key)
        } else {
            format!("{}:intent", request.idempotency_key)
        };
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream_id, &intent_key)?
            .ok_or_else(|| {
                let revision = self.event_store.stream_revision(&stream_id).unwrap_or(0);
                invalid(format!(
                    "host process admission requires original intent {intent_key} in {stream_id} (revision {revision})"
                ))
            })?;
        let mut sources = self.bounded_process_sources(request, effect, &intent)?;
        sources.push(ExpectedStreamRevision {
            stream_id: stream_id.clone(),
            expected_revision: self.event_store.stream_revision(&stream_id)?,
        });
        let scope = format!(
            "session:{}",
            request
                .session_id
                .as_deref()
                .ok_or_else(|| invalid("host process admission Session missing".into()))?
        );
        let source_bytes = crate::host_process_closure::source_bundle_bytes(request)
            .map_err(|error| invalid(error))?;
        let artifacts = self.bounded_process_artifacts()?;
        let source_bundle = artifacts
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/octet-stream".into(),
                    visibility_scope: scope,
                    expected_bytes: Some(source_bytes.len() as u64),
                    original_name: None,
                },
                &source_bytes,
            )
            .await
            .map_err(|error| invalid(format!("host process source archive failed: {error}")))?;
        let admission = harness_contract::context::ToolExecutionAdmission {
            version: 1,
            binding: harness_contract::context::ToolExecutionBinding {
                call_key: request.idempotency_key.clone(),
                tool_use_id: request.tool_use_id.clone(),
                input_sha256: Self::bounded_process_input_sha256(&input)?,
                descriptor_sha256: Self::bounded_process_descriptor_sha256(effect)?,
                authority_sha256: crate::host_process_closure::authority_digest(request)
                    .map_err(|error| invalid(error))?,
            },
            code_sha256: crate::host_process_closure::code_digest(request),
            inputs: Vec::new(),
            source_bundle,
        };
        crate::host_process_closure::validate_host_process_admission(&admission)
            .map_err(|error| invalid(error))?;
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: admission_key.clone(),
                expected_streams: sources,
                events: vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
                        scope: intent.scope,
                        kind: "execution.process.admitted".into(),
                        status: Some("inflight".into()),
                        actor: intent.actor.clone(),
                        refs: intent.refs.clone(),
                        payload: json!({
                            "version": 1,
                            "admission": admission,
                            "admission_sha256": format!(
                                "{:x}",
                                Sha256::digest(serde_json::to_vec(&admission)?)
                            ),
                            "original_input": request.input,
                        }),
                    },
                    idempotency_key: Some(admission_key),
                    schema_version: 1,
                }],
            })?;
        Ok(Some(admission))
    }

    /// Publish the physical closure document of a host-shell process effect and
    /// commit its durable receipt against the admission that was published
    /// before the command ran.
    ///
    /// The closure document binds the admitted program, the effect descriptor,
    /// the authenticated intent, the executor's own outcome and the terminal it
    /// reported into one artifact that only this commit service can publish.
    /// The receipt and its index are written in the same transaction, so a
    /// consumer that reads either one can settle the physical closure.
    /// Publish the physical closure document of a read-class process effect and
    /// commit its read receipt against it.
    ///
    /// The read family keeps its own identity: the receipt is the read
    /// receipt, the admission is the read family's admission, and the closure
    /// envelope travels with the receipt in the same transaction.
    pub(crate) async fn commit_host_process_read_closure(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        outcome: &crate::RuntimeToolExecutionOutcome,
        scope: &harness_contract::goal::ProofEffectScope,
        root: Option<&crate::CowdExecutionContext>,
    ) -> Result<(), ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        Self::validate_host_process_outcome(outcome)?;
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        let intent_key = format!("{}:read-intent", request.idempotency_key);
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream_id, &intent_key)?
            .ok_or_else(|| invalid("host process read closure requires original read intent".into()))?;
        // A root read keeps the original root owner: the receipt is settled
        // against the same owner that admitted the read.
        let root_intent = match root {
            Some(root) => {
                self.root_intent_owner(request, effect, root, scope, &intent)?;
                Some(&intent)
            }
            None => None,
        };
        let document = process_closure_document(&intent, outcome)?;
        let encoded = serde_json::to_vec(&document)?;
        if encoded.len() > 32 * 1024 {
            return Err(invalid(
                "host process read closure metadata exceeds bounded extent".into(),
            ));
        }
        let scope_name = format!(
            "session:{}",
            request
                .session_id
                .as_deref()
                .ok_or_else(|| invalid("host process read closure Session missing".into()))?
        );
        let origin = crate::PreparedArtifactOrigin::ToolEffectClosure {
            stream_id: stream_id.clone(),
            idempotency_key: format!("{}:read-receipt", request.idempotency_key),
        };
        let authority = ProcessClosurePublicationAuthority {
            origin: origin.clone(),
            sha256: format!("sha256:{:x}", Sha256::digest(&encoded)),
            bytes: encoded.len() as u64,
            scope: scope_name.clone(),
        };
        let artifacts = self.bounded_process_artifacts()?;
        let proof = artifacts
            .write_process_closure_blocking(
                &scope_name,
                &format!("process-closure:{}", request.idempotency_key),
                &encoded,
                &origin,
                &authority,
            )
            .map_err(|error| invalid(error.to_string()))?;
        let closure = json!({"content_schema":2,"sha256":proof.sha256,"bytes":proof.bytes,
            "body":{"kind":"artifact_ref","artifact":proof}});
        self.commit_readonly_tool_receipts_with_closure(
            &[(request.clone(), outcome.clone())],
            Some(scope),
            root_intent,
            true,
            Some(&closure),
        )
    }

    pub(crate) async fn commit_host_process_closure(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        outcome: &crate::RuntimeToolExecutionOutcome,
    ) -> Result<(), ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        Self::validate_host_process_outcome(outcome)?;
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:intent", request.idempotency_key))?
            .ok_or_else(|| invalid("host process closure original intent missing".into()))?;
        let document = process_closure_document(&intent, outcome)?;
        let encoded = serde_json::to_vec(&document)?;
        if encoded.len() > 32 * 1024 {
            return Err(invalid(
                "host process closure metadata exceeds bounded extent".into(),
            ));
        }
        let scope = format!(
            "session:{}",
            request
                .session_id
                .as_deref()
                .ok_or_else(|| invalid("host process closure Session missing".into()))?
        );
        let origin = crate::PreparedArtifactOrigin::ToolEffectClosure {
            stream_id: stream_id.clone(),
            idempotency_key: format!("{}:receipt", request.idempotency_key),
        };
        let authority = ProcessClosurePublicationAuthority {
            origin: origin.clone(),
            sha256: format!("sha256:{:x}", Sha256::digest(&encoded)),
            bytes: encoded.len() as u64,
            scope: scope.clone(),
        };
        let artifacts = self.bounded_process_artifacts()?;
        let proof = artifacts
            .write_process_closure_blocking(
                &scope,
                &format!("process-closure:{}", request.idempotency_key),
                &encoded,
                &origin,
                &authority,
            )
            .map_err(|error| invalid(error.to_string()))?;
        let closure = json!({"content_schema":2,"sha256":proof.sha256,"bytes":proof.bytes,
            "body":{"kind":"artifact_ref","artifact":proof}});
        self.commit_tool_effect_trusted(
            request,
            effect,
            outcome,
            ProcessTerminalAuthority::HostProcess(&closure),
        )
    }

    pub(crate) fn begin_bound_tool_effect(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        scope: &harness_contract::goal::ProofEffectScope,
    ) -> Result<ToolEffectState, ExecutionCommitError> {
        Self::validate_bound_effect_scope(effect, scope)?;
        self.begin_tool_effect_scoped(request, effect, None, Some(scope), None)
    }

    pub(crate) fn completed_tool_effect(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
    ) -> Result<Option<crate::RuntimeToolExecutionOutcome>, ExecutionCommitError> {
        let read = effect.effect_kind == ToolEffectKind::Read;
        let suffix = if read { "read-receipt" } else { "receipt" };
        let Some(receipt) = self.event_store.event_by_idempotency_key(
            &format!("execution-effect:{}", request.idempotency_key),
            &format!("{}:{suffix}", request.idempotency_key),
        )?
        else {
            return Ok(None);
        };
        if read {
            validate_readonly_tool_receipt(request, &receipt.payload)?;
        } else {
            validate_mutation_tool_fingerprint(request, effect, &receipt.payload, "receipt")?;
        }
        self.validate_terminal_scope(request, effect, &receipt, read)?;
        Ok(Some(serde_json::from_value(
            receipt.payload["outcome"].clone(),
        )?))
    }

    fn validate_bound_effect_scope(
        effect: &ToolEffectDescriptor,
        scope: &harness_contract::goal::ProofEffectScope,
    ) -> Result<(), ExecutionCommitError> {
        use harness_contract::context::{
            EvidenceCoverageKind, WorkspaceAccessMode, WorkspaceObjectKind,
        };
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        let mapped = scope.targets.iter().all(|target| {
            effect.scopes.contains(&target.declared_scope)
                && target.declared_scope.resource == PermissionResource::File
                && !target.path.workspace_id.is_empty()
                && !target.path.repository_id.is_empty()
                && !target.path.workspace_relative_path.is_empty()
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
        });
        let complete = !effect.scopes.is_empty()
            && effect.scopes.len() == scope.targets.len()
            && effect.scopes.iter().all(|declared| {
                scope
                    .targets
                    .iter()
                    .filter(|target| &target.declared_scope == declared)
                    .count()
                    == 1
            })
            && !effect.uses_network
            && !effect.spawns_process
            && !effect.mutates_packages
            && !effect.mutates_system
            && matches!(
                effect.effect_kind,
                ToolEffectKind::Read | ToolEffectKind::Write
            );
        scope
            .validate_purpose_mapping(effect)
            .map_err(ExecutionCommitError::InvalidCommand)?;
        if scope.version != 2
            || scope.descriptor_hash != effect.descriptor_hash
            || !mapped
            || (scope.complete && !complete)
            || scope
                .targets
                .iter()
                .any(|target| target.exact && target.path.object_kind != WorkspaceObjectKind::File)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "bound scope does not cover its admitted effect descriptor".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_proof_bound_tool_request(
        request: &crate::RuntimeToolExecutionRequest,
        primary: &crate::DurableRuntimeEvent,
        index: &crate::DurableRuntimeEvent,
    ) -> Result<(), ExecutionCommitError> {
        let denied = || {
            ExecutionCommitError::InvalidCommand(
                "proof child does not match its original admitted tool request".into(),
            )
        };
        let source = delegated_agent_receipt_stream_id(request).ok_or_else(denied)?;
        let read = primary.kind == "execution.read.receipt";
        if primary.stream_id != format!("execution-effect:{}", request.idempotency_key)
            || primary.actor.as_deref() != Some("governed_tool")
            || index.stream_id != source
            || index.actor.as_deref() != Some("governed_tool")
            || primary.payload["idempotency_key"] != request.idempotency_key
            || index.payload["idempotency_key"] != request.idempotency_key
            || primary.payload["tool_use_id"] != request.tool_use_id
            || primary.payload["tool_name"] != request.tool_name
            || Self::receipt_observation_sequence(&index) != Some(request.observation_wave_sequence)
            || index.payload["effect_scope"]
                != serde_json::to_value(
                    request
                        .authorization
                        .as_ref()
                        .map(|authorization| &authorization.scope),
                )?
            || tool_effect_refs(request).iter().any(|expected| {
                !primary
                    .refs
                    .iter()
                    .any(|actual| actual.kind == expected.kind && actual.id == expected.id)
                    || !index
                        .refs
                        .iter()
                        .any(|actual| actual.kind == expected.kind && actual.id == expected.id)
            })
        {
            return Err(denied());
        }
        if read {
            if index.kind != "execution.agent_tool.receipt"
                || index.payload["effect_kind"] != "read"
                || primary.payload["resolved_effect_scope"]
                    != index.payload["resolved_effect_scope"]
                || index.payload["authorized_scopes"]
                    != serde_json::to_value(&request.authorized_scopes)?
                || index.payload["outcome"]["tool_use_id"] != request.tool_use_id
                || index.payload["outcome"]["tool_name"] != request.tool_name
            {
                return Err(denied());
            }
            validate_readonly_tool_receipt(request, &primary.payload)?;
            let invocation = request.compiled_invocation.as_ref().ok_or_else(denied)?;
            let immutable_artifact =
                matches!(request.tool_name.as_str(), "evidence_retrieve" | "read_ref")
                    && !invocation.effect.scopes.iter().any(|scope| {
                        scope.resource == harness_contract::policy::PermissionResource::File
                    });
            if immutable_artifact {
                validate_replay_effect_scope(&primary.payload, &invocation.effect)?;
                let seal = crate::context_evidence::decode_read_acquisition(
                    &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                )
                .map_err(|_| denied())?;
                let parent = request.parent_execution.as_ref().ok_or_else(denied)?;
                let binding = request.step_binding.as_ref().ok_or_else(denied)?;
                if !primary.payload["outcome_truncated"]
                    .as_bool()
                    .unwrap_or(false)
                {
                    let output = primary.payload["outcome"]["output"]
                        .as_str()
                        .ok_or_else(denied)?;
                    if crate::context_evidence::read_body_seal(&request.tool_name, output).as_ref()
                        != Some(&seal.body)
                    {
                        return Err(denied());
                    }
                }
                if !matches!(
                    seal.body.extent,
                    crate::context_evidence::ReadExtent::ArtifactChunks { .. }
                ) || seal.graph_id != parent.execution_id
                    || seal.node_id != parent.node_id
                    || Some(seal.attempt) != request.parent_execution_attempt
                    || seal.step != binding.snapshot.identity
                    || seal.origin != serde_json::to_value(&binding.origin)?
                    || seal.session_id != binding.snapshot.identity.session_id
                    || seal.invocation_id != request.tool_use_id
                    || seal.tool_name != request.tool_name
                    || seal.primary_stream != primary.stream_id
                    || seal.index_stream != index.stream_id
                    || seal.primary_key != format!("{}:read-receipt", request.idempotency_key)
                    || seal.index_key != delegated_agent_receipt_key(request)
                    || primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
                        != index.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
                    || primary.transaction_id != index.transaction_id
                    || primary.commit_cursor != index.commit_cursor
                {
                    return Err(denied());
                }
            } else {
                let effect: ToolEffectDescriptor =
                    serde_json::from_value(primary.payload["effect"].clone())?;
                let scope: harness_contract::goal::ProofEffectScope =
                    serde_json::from_value(primary.payload["resolved_effect_scope"].clone())?;
                Self::validate_bound_effect_scope(&effect, &scope)?;
                if invocation.effect != effect {
                    return Err(denied());
                }
            }
            if invocation.effect.effect_kind != ToolEffectKind::Read {
                return Err(denied());
            }
        } else {
            if primary.kind != "execution.effect.intent"
                || index.kind != "execution.agent_tool.intent"
            {
                return Err(denied());
            }
            let effect: ToolEffectDescriptor =
                serde_json::from_value(primary.payload["effect"].clone())?;
            let scope: harness_contract::goal::ProofEffectScope =
                serde_json::from_value(primary.payload["resolved_effect_scope"].clone())?;
            let authorization = request.authorization.as_ref().ok_or_else(denied)?;
            if effect.effect_kind == ToolEffectKind::Read
                || index.payload["effect_kind"] != serde_json::to_value(effect.effect_kind)?
                || index.payload["effect_scopes"] != serde_json::to_value(&effect.scopes)?
                || index.payload["descriptor_hash"] != effect.descriptor_hash
                || index.payload["tool_name"] != request.tool_name
                || index.payload["resolved_effect_scope"]
                    != primary.payload["resolved_effect_scope"]
                || index.payload["effect_scope"] != serde_json::to_value(&authorization.scope)?
                || authorization.tool_id != request.tool_name
                || authorization.descriptor_hash != effect.descriptor_hash
                || authorization.policy_revision != request.policy_revision
                || authorization.authorization_lease.policy_revision != request.policy_revision
                || authorization.authorization_lease.effect_descriptor_hash
                    != effect.descriptor_hash
                || !authorization
                    .authorization_lease
                    .permits(&effect.tool_id, effect.required_permission)
                || !effect
                    .scopes
                    .iter()
                    .all(|scope| authorization.authorization_lease.scopes.contains(scope))
                || !effect.scopes.contains(&authorization.scope)
                || request
                    .compiled_invocation
                    .as_ref()
                    .is_some_and(|invocation| invocation.effect != effect)
            {
                return Err(denied());
            }
            Self::validate_bound_effect_scope(&effect, &scope)?;
            validate_mutation_tool_fingerprint(request, &effect, &primary.payload, "proof intent")?;
        }
        if primary.payload["replay"] != receipt_replay_contract(request)? {
            return Err(denied());
        }
        Ok(())
    }

    fn validate_terminal_scope(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        receipt: &crate::DurableRuntimeEvent,
        read: bool,
    ) -> Result<(), ExecutionCommitError> {
        self.validate_stored_terminal_scope(effect, receipt, read)?;
        self.validate_terminal_request_identity(request, effect, receipt)
    }

    pub(crate) fn validate_terminal_request_identity(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        receipt: &crate::DurableRuntimeEvent,
    ) -> Result<(), ExecutionCommitError> {
        if receipt.payload["resolved_effect_scope"]["version"] == 2 {
            if receipt.payload["replay"]["session_id"] != serde_json::to_value(&request.session_id)?
                || receipt.payload["replay"]["invocation"]["effect"]
                    != serde_json::to_value(effect)?
                || request
                    .compiled_invocation
                    .as_ref()
                    .is_none_or(|invocation| invocation.effect != *effect)
                || request.step_binding.as_ref().is_none_or(|binding| {
                    serde_json::to_value(&binding.origin)
                        .map_or(true, |origin| receipt.payload["replay"]["origin"] != origin)
                })
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "terminal receipt original invocation identity conflict".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_stored_terminal_scope(
        &self,
        effect: &ToolEffectDescriptor,
        receipt: &crate::DurableRuntimeEvent,
        read: bool,
    ) -> Result<(), ExecutionCommitError> {
        self.validate_stored_bounded_process(receipt)?;
        self.validate_terminal_scope_from_records(
            effect,
            receipt,
            read,
            &mut StoredTerminalScopeReader(&self.event_store),
        )
    }

    pub(crate) fn validate_terminal_scope_from_records(
        &self,
        effect: &ToolEffectDescriptor,
        receipt: &crate::DurableRuntimeEvent,
        read: bool,
        reader: &mut impl TerminalScopeReader,
    ) -> Result<(), ExecutionCommitError> {
        Self::validate_local_network_receipt(receipt, effect, reader)?;
        validate_replay_effect_scope(&receipt.payload, effect)?;
        let key = receipt.payload["idempotency_key"].as_str().ok_or_else(|| {
            ExecutionCommitError::InvalidCommand("terminal receipt identity missing".into())
        })?;
        let suffix = if read { "read-intent" } else { "intent" };
        let intent = reader.event(&receipt.stream_id, &format!("{key}:{suffix}"))?;
        let version2 = receipt.payload["resolved_effect_scope"]["version"] == 2;
        if intent
            .as_ref()
            .is_some_and(|intent| intent.payload["resolved_effect_scope"]["version"] == 2)
            && !version2
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "scope2 intent cannot recover through a legacy terminal".into(),
            ));
        }
        if !version2 {
            return Ok(());
        }
        if receipt.kind
            != if read {
                "execution.read.receipt"
            } else {
                "execution.effect.receipt"
            }
            || receipt.actor.as_deref() != Some("governed_tool")
            || receipt.payload["effect"] != serde_json::to_value(effect)?
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "scope2 receipt descriptor or source conflict".into(),
            ));
        }
        let root_record = &receipt.payload["root_effect_owner"];
        let root = !root_record.is_null();
        if let Some(intent) = &intent {
            if intent.kind
                != if read {
                    "execution.read.intent"
                } else {
                    "execution.effect.intent"
                }
                || intent.actor.as_deref() != Some("governed_tool")
                || intent.payload["resolved_effect_scope"]
                    != receipt.payload["resolved_effect_scope"]
                || intent.payload["root_effect_context"] != receipt.payload["root_effect_context"]
                || intent.payload["root_effect_owner"] != *root_record
                || intent.payload["effect"] != serde_json::to_value(effect)?
                || intent.payload["replay"] != receipt.payload["replay"]
                || (intent.commit_cursor, intent.transaction_index)
                    >= (receipt.commit_cursor, receipt.transaction_index)
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "terminal scope differs from original intent".into(),
                ));
            }
        } else if root || !read {
            return Err(ExecutionCommitError::InvalidCommand(
                "terminal scope has no original intent".into(),
            ));
        }
        let replay = &receipt.payload["replay"];
        let graph_id = if root {
            root_record["binding"]["root_execution_id"].as_str()
        } else {
            replay["parent_execution"]["execution_id"].as_str()
        };
        let node_id = if root {
            root_record["binding"]["node_id"].as_str()
        } else {
            replay["parent_execution"]["node_id"].as_str()
        };
        let (Some(graph_id), Some(node_id)) = (graph_id, node_id) else {
            return Err(ExecutionCommitError::InvalidCommand(
                "scope2 terminal has no original graph/node owner".into(),
            ));
        };
        let attempt = if root {
            root_record["attempt"].as_u64()
        } else {
            receipt
                .refs
                .iter()
                .find(|reference| reference.kind == "agent_attempt")
                .and_then(|reference| reference.id.parse::<u64>().ok())
        };
        let attempt = attempt.filter(|attempt| *attempt > 0).ok_or_else(|| {
            ExecutionCommitError::InvalidCommand("scope2 terminal attempt missing".into())
        })?;
        if root {
            let owner: harness_contract::projection::RuntimeActivityBinding =
                serde_json::from_value(root_record["binding"].clone())?;
            owner
                .validate()
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.into()))?;
            let context: crate::CowdExecutionContext =
                serde_json::from_value(receipt.payload["root_effect_context"].clone())?;
            let revision = root_record["graph_revision"]
                .as_u64()
                .filter(|revision| *revision > 0)
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "root receipt graph revision missing".into(),
                    )
                })?;
            let intent = intent.as_ref().ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("root receipt has no intent".into())
            })?;
            let source = reader
                .event(
                    &node_stream_id(graph_id, node_id),
                    &format!("{graph_id}:{node_id}:{}", owner.revision),
                )?
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "root receipt original node activity missing".into(),
                    )
                })?;
            let graph_fence = reader.fence(&intent.transaction_id, graph_id)?;
            if root_record
                .as_object()
                .is_none_or(|record| record.len() != 3)
                || !replay["parent_execution"].is_null()
                || owner.agent_run_id.is_some()
                || context.execution_id != graph_id
                || context.session_id != owner.session_id
                || context.turn_id != owner.turn_id
                || replay["session_id"] != owner.session_id
                || source.activity_binding().as_ref() != Some(&owner)
                || source.kind != "execution_node.transitioned"
                || source.actor.as_deref() != Some("execution_commit_service")
                || source.scope != RuntimeEventScope::ExecutionNode
                || source.schema_version != 1
                || source.status.as_deref() != Some("running")
                || source.payload["to"] != "running"
                || source.commit_cursor > intent.commit_cursor
                || graph_fence.is_none_or(|fence| fence.expected_revision != revision)
                || owner.revision > revision
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "root receipt original owner or authority provenance conflict".into(),
                ));
            }
        }
        let index_stream = format!("execution-agent-receipts:{graph_id}:{node_id}:{attempt}");
        let index = reader
            .event(&index_stream, &format!("agent-tool-receipt:{key}"))?
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("scope2 terminal index missing".into())
            })?;
        if index.kind != "execution.agent_tool.receipt"
            || index.actor.as_deref() != Some("governed_tool")
            || index.transaction_id != receipt.transaction_id
            || index.commit_cursor != receipt.commit_cursor
            || index.transaction_index <= receipt.transaction_index
            || index.payload["resolved_effect_scope"] != receipt.payload["resolved_effect_scope"]
            || index.payload["root_effect_owner"] != *root_record
            || index.payload["root_effect_context"] != receipt.payload["root_effect_context"]
            || index.payload["effect"] != receipt.payload["effect"]
            || index.payload["outcome"] != receipt.payload["outcome"]
            || index.payload["process_admission_ref"] != receipt.payload["process_admission_ref"]
            || index.payload["process_closure"] != receipt.payload["process_closure"]
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "scope2 terminal primary/index provenance conflict".into(),
            ));
        }
        Ok(())
    }

    fn authenticated_root_owner(
        &self,
        root: &crate::CowdExecutionContext,
        owner: &harness_contract::projection::RuntimeActivityBinding,
        attempt: u32,
    ) -> Result<u64, ExecutionCommitError> {
        let denied = || {
            ExecutionCommitError::InvalidCommand(
                "root effect original caller owner mismatch".into(),
            )
        };
        owner.validate().map_err(|_| denied())?;
        let node_id = owner.node_id.as_ref().ok_or_else(denied)?;
        let graph = self
            .effect_authority_graphs
            .load(&root.execution_id)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let original = self
            .event_store
            .latest_for_stream(&node_stream_id(&graph.id, node_id))
            .map_err(ExecutionCommitError::InvalidCommand)?
            .ok_or_else(denied)?;
        if owner.root_execution_id != root.execution_id
            || owner.session_id != root.session_id
            || owner.turn_id != root.turn_id
            || owner.agent_run_id.is_some()
            || graph.parent_execution.is_some()
            || !graph.lineage.as_ref().is_some_and(|lineage| {
                lineage.session_id == owner.session_id
                    && lineage.turn_id == owner.turn_id
                    && lineage.root_task_id == owner.root_task_id
                    && lineage.task_id == owner.task_id
                    && lineage.generation == owner.generation
            })
            || owner.revision > graph.revision
            || graph.node_statuses.get(node_id) != Some(&ExecutionNodeStatus::Running)
            || graph.recovery_cursor.node_attempts.get(node_id).copied() != Some(attempt)
            || attempt == 0
            || original.activity_binding().as_ref() != Some(owner)
            || original.kind != "execution_node.transitioned"
            || original.actor.as_deref() != Some("execution_commit_service")
            || original.scope != RuntimeEventScope::ExecutionNode
            || original.schema_version != 1
            || original.status.as_deref() != Some("running")
            || original.payload["to"] != "running"
        {
            return Err(denied());
        }
        Ok(graph.revision)
    }

    fn root_intent_owner(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        root: &crate::CowdExecutionContext,
        scope: &harness_contract::goal::ProofEffectScope,
        intent: &crate::DurableRuntimeEvent,
    ) -> Result<(harness_contract::projection::RuntimeActivityBinding, u32), ExecutionCommitError>
    {
        Self::validate_bound_effect_scope(effect, scope)?;
        let record = &intent.payload["root_effect_owner"];
        let owner: harness_contract::projection::RuntimeActivityBinding =
            serde_json::from_value(record["binding"].clone())?;
        let attempt: u32 = serde_json::from_value(record["attempt"].clone())?;
        let revision = record["graph_revision"].as_u64().ok_or_else(|| {
            ExecutionCommitError::InvalidCommand("root intent graph revision missing".into())
        })?;
        let graph_fence = self
            .event_store
            .transaction_stream_revision_exact(&intent.transaction_id, &root.execution_id)?;
        if record.as_object().is_none_or(|record| record.len() != 3)
            || revision == 0
            || request.parent_execution.is_some()
            || graph_fence.is_none_or(|fence| fence.expected_revision != revision)
            || intent.stream_id != format!("execution-effect:{}", request.idempotency_key)
            || intent.payload["idempotency_key"] != request.idempotency_key
            || intent.payload["tool_use_id"] != request.tool_use_id
            || intent.payload["tool_name"] != request.tool_name
            || request.session_id.as_deref() != Some(root.session_id.as_str())
            || intent.actor.as_deref() != Some("governed_tool")
            || intent.payload["root_effect_context"] != serde_json::to_value(root)?
            || intent.payload["resolved_effect_scope"] != serde_json::to_value(scope)?
            || intent.payload["effect"] != serde_json::to_value(effect)?
            || intent.payload["replay"] != receipt_replay_contract(request)?
            || receipt_payload_matches(&request.input, &intent.payload)? != Some(true)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "root original intent identity conflict".into(),
            ));
        }
        self.authenticated_root_owner(root, &owner, attempt)?;
        Ok((owner, attempt))
    }

    pub(crate) fn begin_root_tool_effect(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        root: &crate::CowdExecutionContext,
        scope: &harness_contract::goal::ProofEffectScope,
        owner: &harness_contract::projection::RuntimeActivityBinding,
        attempt: u32,
    ) -> Result<ToolEffectState, ExecutionCommitError> {
        if effect.effect_kind == ToolEffectKind::Read {
            return self.begin_root_read_acquisition(request, effect, root, scope, owner, attempt);
        }
        Self::validate_bound_effect_scope(effect, scope)?;
        self.begin_tool_effect_scoped(
            request,
            effect,
            Some(root),
            Some(scope),
            Some((owner, attempt)),
        )
    }

    pub(crate) fn begin_root_read_acquisition(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        root: &crate::CowdExecutionContext,
        scope: &harness_contract::goal::ProofEffectScope,
        owner: &harness_contract::projection::RuntimeActivityBinding,
        attempt: u32,
    ) -> Result<ToolEffectState, ExecutionCommitError> {
        Self::validate_bound_effect_scope(effect, scope)?;
        if effect.effect_kind != ToolEffectKind::Read {
            return Err(ExecutionCommitError::InvalidCommand(
                "root read admission requires a Read descriptor".into(),
            ));
        }
        if let Some(outcome) = self.completed_tool_effect(request, effect)? {
            return Ok(ToolEffectState::Completed(outcome));
        }
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        let key = format!("{}:read-intent", request.idempotency_key);
        let mut streams = self
            .root_effect_authority(root)?
            .into_iter()
            .map(|source| source.stream_id)
            .collect::<Vec<_>>();
        streams.push(stream_id.clone());
        self.event_store.with_stream_locks(&streams, || {
            if let Some(outcome) = self.completed_tool_effect(request, effect)? {
                return Ok(ToolEffectState::Completed(outcome));
            }
            self.validate_system_tool_parent_scoped(request, Some(root))?;
            if request.parent_execution.is_some() || request.session_id.as_deref() != Some(root.session_id.as_str()) {
                return Err(ExecutionCommitError::InvalidCommand("root read request lineage conflict".into()));
            }
            let graph_revision = self.authenticated_root_owner(root, owner, attempt)?;
            let mut expected_streams = self.root_effect_authority(root)?;
            if expected_streams.iter().any(|source| !streams.contains(&source.stream_id)) {
                return Err(ExecutionCommitError::InvalidCommand("root read authority sources changed".into()));
            }
            if let Some(intent) = self.event_store.event_by_idempotency_key(&stream_id, &key)? {
                let (original_owner, original_attempt) = self.root_intent_owner(request, effect, root, scope, &intent)?;
                if original_owner != *owner || original_attempt != attempt {
                    return Err(ExecutionCommitError::InvalidCommand("root read retry cannot replace its original owner".into()));
                }
                return Ok(ToolEffectState::NotRequired);
            }
            expected_streams.push(ExpectedStreamRevision {
                stream_id: stream_id.clone(), expected_revision: self.event_store.stream_revision(&stream_id)?,
            });
            self.event_store.append_transaction_locked(AppendTransactionRequest {
                transaction_id: key.clone(), expected_streams,
                events: vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: stream_id.clone(), scope: RuntimeEventScope::ExecutionNode,
                        kind: "execution.read.intent".into(), status: Some("inflight".into()),
                        actor: Some("governed_tool".into()),
                        refs: root_effect_receipt_refs(request, owner, attempt),
                        payload: json!({
                            "idempotency_key":request.idempotency_key, "tool_use_id":request.tool_use_id,
                            "tool_name":request.tool_name, "effect":effect, "resolved_effect_scope":scope,
                            "replay":receipt_replay_contract(request)?,
                            "input_sha256":format!("sha256:{:x}", Sha256::digest(request.input.as_bytes())),
                            "root_effect_context":root,
                            "root_effect_owner":{"binding":owner,"attempt":attempt,"graph_revision":graph_revision},
                        }),
                    }, idempotency_key: Some(key.clone()), schema_version: 1,
                }],
            })?;
            Ok(ToolEffectState::NotRequired)
        })
    }

    pub(crate) fn commit_root_read_acquisition(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        outcome: &crate::RuntimeToolExecutionOutcome,
        effect: &ToolEffectDescriptor,
        root: &crate::CowdExecutionContext,
        scope: &harness_contract::goal::ProofEffectScope,
    ) -> Result<(), ExecutionCommitError> {
        Self::validate_bound_effect_scope(effect, scope)?;
        if self.completed_tool_effect(request, effect)?.is_some() {
            return Ok(());
        }
        let intent = self
            .event_store
            .event_by_idempotency_key(
                &format!("execution-effect:{}", request.idempotency_key),
                &format!("{}:read-intent", request.idempotency_key),
            )?
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(
                    "root read receipt requires its original intent".into(),
                )
            })?;
        self.root_intent_owner(request, effect, root, scope, &intent)?;
        self.commit_readonly_tool_receipts_scoped(
            &[(request.clone(), outcome.clone())],
            Some(scope),
            Some(&intent),
            false,
        )
    }

    fn begin_tool_effect_scoped(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        root: Option<&crate::CowdExecutionContext>,
        scope: Option<&harness_contract::goal::ProofEffectScope>,
        root_owner: Option<(&harness_contract::projection::RuntimeActivityBinding, u32)>,
    ) -> Result<ToolEffectState, ExecutionCommitError> {
        self.begin_tool_effect_scoped_observed(request, effect, root, scope, root_owner, None)
    }

    // This call-bound token is evidence from a pre-dispatch refusal, never a
    // durable NoDispatch fact. Graph publication and closure re-read the exact
    // physical streams and carry their zero revisions in the original CAS.
    fn pre_effect_refusal_error(
        request: &crate::RuntimeToolExecutionRequest,
        error: ExecutionCommitError,
        known_refusal_without_history: bool,
    ) -> crate::ToolError {
        if known_refusal_without_history {
            if let Some(binding) = request.step_binding.as_ref().filter(|binding| {
                request.tool_use_id == binding.origin.call_id
                    && request.idempotency_key == binding.effect_idempotency_key()
            }) {
                return crate::ToolError::from_pre_effect_refusal(
                    PreEffectRefusal {
                        call_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        input_sha256: crate::context_evidence::read_sha256(
                            request.input.as_bytes(),
                        ),
                        effect_key: request.idempotency_key.clone(),
                        binding: binding.clone(),
                        reason: error.to_string(),
                    },
                    error.to_string(),
                );
            }
        }
        crate::ToolError::new(error.to_string())
    }

    pub(crate) fn preflight_fresh_root_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        root: &crate::CowdExecutionContext,
    ) -> Result<(), crate::ToolError> {
        if self
            .completed_tool_effect(request, effect)
            .map_err(|error| crate::ToolError::new(error.to_string()))?
            .is_some()
        {
            return Ok(());
        }
        let head = self
            .event_store
            .stream_revision(&format!("execution-effect:{}", request.idempotency_key))
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        let refusal = std::cell::Cell::new(false);
        self.root_effect_authority_with_refusal(root, &|_| refusal.set(true))
            .map(|_| ())
            .map_err(|error| {
                Self::pre_effect_refusal_error(
                    request,
                    error,
                    head == 0
                        && refusal.get()
                        && request.session_id.as_deref() == Some(root.session_id.as_str())
                        && request.parent_execution.is_none(),
                )
            })
    }

    pub(crate) fn preflight_fresh_bound_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
    ) -> Result<(), crate::ToolError> {
        if self
            .completed_tool_effect(request, effect)
            .map_err(|error| crate::ToolError::new(error.to_string()))?
            .is_some()
        {
            return Ok(());
        }
        if effect.effect_kind == ToolEffectKind::Read {
            self.validate_system_tool_parent_scoped(request, None)
                .map_err(|error| crate::ToolError::new(error.to_string()))
        } else {
            let head = self
                .event_store
                .stream_revision(&format!("execution-effect:{}", request.idempotency_key))
                .map_err(|error| crate::ToolError::new(error.to_string()))?;
            let refusal = std::cell::Cell::new(false);
            self.tool_effect_authority_with_refusal(request, &|_| refusal.set(true))
                .map(|_| ())
                .map_err(|error| {
                    Self::pre_effect_refusal_error(request, error, head == 0 && refusal.get())
                })
        }
    }

    pub(crate) fn begin_bound_tool_effect_observed(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        scope: &harness_contract::goal::ProofEffectScope,
        observation: Option<&super::executors::agent_tool::NoDispatchObservation>,
    ) -> Result<ToolEffectState, ExecutionCommitError> {
        Self::validate_bound_effect_scope(effect, scope).map_err(|error| {
            if let Some(observation) = observation {
                observation.refuse(&request.tool_use_id, error.to_string());
            }
            error
        })?;
        self.begin_tool_effect_scoped_observed(
            request,
            effect,
            None,
            Some(scope),
            None,
            observation,
        )
    }

    fn begin_tool_effect_scoped_observed(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        root: Option<&crate::CowdExecutionContext>,
        scope: Option<&harness_contract::goal::ProofEffectScope>,
        root_owner: Option<(&harness_contract::projection::RuntimeActivityBinding, u32)>,
        observation: Option<&super::executors::agent_tool::NoDispatchObservation>,
    ) -> Result<ToolEffectState, ExecutionCommitError> {
        if let Some(outcome) = self.completed_tool_effect(request, effect)? {
            return Ok(ToolEffectState::Completed(outcome));
        }
        if effect.effect_kind == ToolEffectKind::Read {
            self.validate_system_tool_parent_scoped(request, root)?;
            if effect.spawns_process {
                // A read that owns a process tree still needs a durable
                // identity before the command runs: the read intent is the
                // anchor of the read key family, and the admission of the
                // process is bound to it.
                self.begin_read_process_intent(request, effect, scope)?;
            }
            return Ok(ToolEffectState::NotRequired);
        }
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        // A completed receipt above is replay, not a new effect admission.
        // Non-idempotent uncertainty likewise never reaches ToolHost again.
        if let Some(intent) = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:intent", request.idempotency_key))?
        {
            if let Some(observation) = observation {
                observation.uncertain(request);
            }
            validate_mutation_tool_fingerprint(request, effect, &intent.payload, "intent")?;
            validate_replay_effect_scope(&intent.payload, effect)?;
            if let Some(scope) = scope {
                if intent.payload["resolved_effect_scope"] != serde_json::to_value(scope)? {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "effect target mapping drift; retry cannot replace original provenance"
                            .into(),
                    ));
                }
            }
            if matches!(
                effect.idempotency,
                ToolIdempotency::NonIdempotent | ToolIdempotency::Unknown
            ) {
                return Ok(ToolEffectState::Uncertain);
            }
        }
        let authority = || {
            self.validate_system_tool_parent_scoped(request, root)?;
            if let Some(root) = root {
                if request.session_id.as_deref() != Some(root.session_id.as_str())
                    || request.parent_execution.is_some()
                {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "root_effect_authority: invocation scope mismatch".into(),
                    ));
                }
                let (owner, attempt) = root_owner.ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "root admission requires its original caller owner".into(),
                    )
                })?;
                let sources = self.root_effect_authority(root)?;
                let revision = self.authenticated_root_owner(root, owner, attempt)?;
                if !sources.iter().any(|source| {
                    source.stream_id == root.execution_id && source.expected_revision == revision
                }) {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "root owner graph changed during authority snapshot".into(),
                    ));
                }
                Ok(sources)
            } else {
                self.tool_effect_authority_observed(request, observation)
            }
        };
        let sources = authority()?;
        let mut streams = sources
            .iter()
            .map(|source| source.stream_id.clone())
            .collect::<Vec<_>>();
        streams.push(stream_id.clone());
        if let Some(index) = delegated_agent_receipt_stream_id(request).or_else(|| {
            root_owner.map(|(owner, attempt)| {
                format!(
                    "execution-agent-receipts:{}:{}:{attempt}",
                    owner.root_execution_id,
                    owner.node_id.as_deref().unwrap_or_default()
                )
            })
        }) {
            streams.push(index);
        }
        self.event_store.with_stream_locks(&streams, || {
            if let Some(receipt) = self.event_store.event_by_idempotency_key(
                &stream_id, &format!("{}:receipt", request.idempotency_key))? {
                validate_mutation_tool_fingerprint(request, effect, &receipt.payload, "receipt")?;
                self.validate_terminal_scope(request, effect, &receipt, false)?;
                return Ok(ToolEffectState::Completed(serde_json::from_value(receipt.payload["outcome"].clone())?));
            }
            let mut expected_streams = authority()?;
            if expected_streams.iter().any(|source| !streams.contains(&source.stream_id)) {
                return Err(ExecutionCommitError::InvalidCommand("tool effect authority scope changed during admission".into()));
            }
            let revision = self.event_store.stream_revision(&stream_id)?;
            let intent = self.event_store.event_by_idempotency_key(
                &stream_id, &format!("{}:intent", request.idempotency_key))?;
            if let Some(intent) = &intent {
                validate_mutation_tool_fingerprint(request, effect, &intent.payload, "intent")?;
                validate_replay_effect_scope(&intent.payload, effect)?;
                if matches!(effect.idempotency, ToolIdempotency::NonIdempotent | ToolIdempotency::Unknown) {
                    return Ok(ToolEffectState::Uncertain);
                }
            }
            let root_record = if let Some((owner, attempt)) = root_owner {
                let root = root.ok_or_else(|| ExecutionCommitError::InvalidCommand("root owner has no context".into()))?;
                let revision = self.authenticated_root_owner(root, owner, attempt)?;
                if !expected_streams.iter().any(|source| source.stream_id == root.execution_id && source.expected_revision == revision) {
                    return Err(ExecutionCommitError::InvalidCommand("root caller changed under intent lock".into()));
                }
                if let Some(intent) = &intent {
                    let scope = scope.ok_or_else(|| ExecutionCommitError::InvalidCommand("root intent has no scope".into()))?;
                    let (original_owner, original_attempt) = self.root_intent_owner(request, effect, root, scope, intent)?;
                    if original_owner != *owner || original_attempt != attempt {
                        return Err(ExecutionCommitError::InvalidCommand("root retry owner conflict".into()));
                    }
                    intent.payload["root_effect_owner"].clone()
                } else {
                    json!({"binding":owner,"attempt":attempt,"graph_revision":revision})
                }
            } else { serde_json::Value::Null };
            let refs = root_owner.map_or_else(|| tool_effect_refs(request),
                |(owner, attempt)| root_effect_receipt_refs(request, owner, attempt));
            let retry = intent.is_some();
            if let (Some(intent), Some(scope)) = (&intent, scope) {
                if intent.payload["resolved_effect_scope"] != serde_json::to_value(scope)? {
                    return Err(ExecutionCommitError::InvalidCommand("effect target mapping drift under intent lock".into()));
                }
            }
            let key = if retry { format!("{}:retry:{revision}", request.idempotency_key) }
                else { format!("{}:intent", request.idempotency_key) };
            expected_streams.push(ExpectedStreamRevision { stream_id: stream_id.clone(), expected_revision: revision });
            let mut events = vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: if retry { "execution.effect.retry_admitted" } else { "execution.effect.intent" }.into(),
                    status: Some("inflight".into()), actor: Some("governed_tool".into()),
                    refs: refs.clone(),
                    payload: json!({
                        "root_effect_context":root,
                        "root_effect_owner":root_record,
                        "idempotency_key": request.idempotency_key,
                        "tool_use_id": request.tool_use_id, "tool_name": request.tool_name,
                        "replay": receipt_replay_contract(request)?,
                        "input_sha256": format!("sha256:{:x}", Sha256::digest(request.input.as_bytes())),
                        "effect": effect,
                        "resolved_effect_scope":scope,
                    }),
                },
                idempotency_key: Some(key.clone()), schema_version: 1,
            }];
            if let Some(index) = delegated_agent_receipt_stream_id(request).or_else(|| root_owner.map(|(owner, attempt)|
                format!("execution-agent-receipts:{}:{}:{attempt}", owner.root_execution_id, owner.node_id.as_deref().unwrap_or_default()))) {
                let index_key = format!("agent-tool-intent:{}", request.idempotency_key);
                if self.event_store.event_by_idempotency_key(&index, &index_key)?.is_none() {
                    expected_streams.push(ExpectedStreamRevision {
                        stream_id: index.clone(), expected_revision: self.event_store.stream_revision(&index)?,
                    });
                    events.push(RuntimeTransactionEventInput {
                        event: RuntimeEventInput {
                            stream_id: index, scope: RuntimeEventScope::ExecutionNode,
                            kind: "execution.agent_tool.intent".into(), status: Some("inflight".into()),
                            actor: Some("governed_tool".into()), refs: refs.clone(),
                            payload: json!({
                                "root_effect_context":root,
                                "root_effect_owner":root_record,
                                "idempotency_key": request.idempotency_key,
                                "tool_name": request.tool_name,
                                "sequence": request.observation_wave_sequence,
                                "effect_kind": effect.effect_kind,
                                "descriptor_hash":effect.descriptor_hash,
                                "effect_scopes": effect.scopes,
                                "effect_scope": request.authorization.as_ref().map(|authorization| &authorization.scope),
                                "resolved_effect_scope":scope,
                            }),
                        }, idempotency_key: Some(index_key), schema_version: 1,
                    });
                }
            }
            if let Some(observation) = observation { observation.uncertain(request); }
            self.event_store.append_transaction_locked(AppendTransactionRequest {
                transaction_id: key, expected_streams, events,
            })?;
            Ok(ToolEffectState::Fresh)
        })
    }

    pub fn commit_tool_effect(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        outcome: &crate::RuntimeToolExecutionOutcome,
    ) -> Result<(), ExecutionCommitError> {
        Self::reject_public_process_facts(outcome, false)?;
        local_network::validate_outcome(request, effect, outcome)?;
        self.commit_tool_effect_trusted(
            request,
            effect,
            outcome,
            ProcessTerminalAuthority::Absent,
        )
    }

    /// The honest shape of a Runtime-minted host process terminal.
    fn validate_host_process_outcome(
        outcome: &crate::RuntimeToolExecutionOutcome,
    ) -> Result<(), ExecutionCommitError> {
        let facts = outcome
            .observed_evidence
            .iter()
            .filter_map(|observed| match &observed.target {
                harness_contract::context::EvidenceTargetIdentity::ToolExecution { execution } => {
                    Some(execution)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if facts.len() != 1 {
            return Err(ExecutionCommitError::InvalidCommand(
                "host process receipt requires exactly one host process terminal fact".into(),
            ));
        }
        crate::host_process_closure::validate_host_process_fact(facts[0])
            .map_err(ExecutionCommitError::InvalidCommand)
    }

    fn reject_public_process_facts(
        outcome: &crate::RuntimeToolExecutionOutcome,
        allow_host_process: bool,
    ) -> Result<(), ExecutionCommitError> {
        if outcome.observed_evidence.iter().any(|observed| {
            matches!(
                observed.target,
                harness_contract::context::EvidenceTargetIdentity::ToolExecution { .. }
            )
        }) {
            if allow_host_process {
                return Self::validate_host_process_outcome(outcome);
            }
            return Err(ExecutionCommitError::InvalidCommand(
                "public ToolExecution observations are not an execution capability".into(),
            ));
        }
        Ok(())
    }

    fn commit_tool_effect_trusted(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        outcome: &crate::RuntimeToolExecutionOutcome,
        authority: ProcessTerminalAuthority<'_>,
    ) -> Result<(), ExecutionCommitError> {
        if effect.effect_kind == ToolEffectKind::Read {
            return Ok(());
        }
        // Same optimistic-concurrency retry as the read wave: concurrent effect
        // receipts from one Agent share its `execution-agent-receipts:*` stream,
        // so a stale-revision CAS is retryable (the receipt keys are stable).
        const MAX_ATTEMPTS: u8 = 8;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.commit_tool_effect_once(request, effect, outcome, authority) {
                Ok(()) => return Ok(()),
                Err(ExecutionCommitError::EventStore(RuntimeEventStoreError::StaleRevision {
                    ..
                })) if attempt < MAX_ATTEMPTS => continue,
                Err(error) => return Err(error),
            }
        }
    }

    fn commit_tool_effect_once(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        effect: &ToolEffectDescriptor,
        outcome: &crate::RuntimeToolExecutionOutcome,
        authority: ProcessTerminalAuthority<'_>,
    ) -> Result<(), ExecutionCommitError> {
        if effect.effect_kind == ToolEffectKind::Read {
            return Ok(());
        }
        if let Some(original) = self.completed_tool_effect(request, effect)? {
            if serde_json::to_value(&original)?
                != serde_json::to_value(bounded_tool_effect_outcome(outcome))?
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "mutation terminal outcome collision".into(),
                ));
            }
            return Ok(());
        }
        let stream_id = format!("execution-effect:{}", request.idempotency_key);
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:intent", request.idempotency_key))?
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(format!(
                    "mutation receipt has no durable intent for idempotency key `{}`",
                    request.idempotency_key
                ))
            })?;
        validate_mutation_tool_fingerprint(request, effect, &intent.payload, "intent")?;
        validate_replay_effect_scope(&intent.payload, effect)?;
        let root_owner = if !intent.payload["root_effect_owner"].is_null() {
            let root: crate::CowdExecutionContext =
                serde_json::from_value(intent.payload["root_effect_context"].clone())?;
            let scope: harness_contract::goal::ProofEffectScope =
                serde_json::from_value(intent.payload["resolved_effect_scope"].clone())?;
            Some((root, scope))
        } else {
            None
        };
        let root_sources = root_owner
            .as_ref()
            .map(|(root, scope)| {
                let sources = self.root_effect_authority(root)?;
                let (owner, attempt) =
                    self.root_intent_owner(request, effect, root, scope, &intent)?;
                let revision = self.authenticated_root_owner(root, &owner, attempt)?;
                if !sources.iter().any(|source| {
                    source.stream_id == root.execution_id && source.expected_revision == revision
                }) {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "root write owner changed during receipt snapshot".into(),
                    ));
                }
                Ok((sources, owner, attempt))
            })
            .transpose()?;
        let mut expected_streams = BTreeMap::<String, u64>::new();
        if let Some((sources, _, _)) = &root_sources {
            for source in sources {
                expected_streams.insert(source.stream_id.clone(), source.expected_revision);
            }
        }
        if outcome.observed_evidence.iter().any(|observed| {
            matches!(
                observed.target,
                harness_contract::context::EvidenceTargetIdentity::ToolExecution { .. }
            )
        }) {
            match authority {
                ProcessTerminalAuthority::BoundedProcess(_) => {
                    crate::tool_host::check_bounded_process_commit_authority(request)
                        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
                    for source in self.bounded_process_sources(request, effect, &intent)? {
                        expected_streams.insert(source.stream_id, source.expected_revision);
                    }
                }
                ProcessTerminalAuthority::HostProcess(_) => {
                    // The Runtime kernel minted this fact itself from the
                    // trusted host executor's quiescence report, and the
                    // admission of this effect was published before the command
                    // ran. The host shell carries no private sandbox
                    // capability, so it needs no live process authority, but it
                    // is bound to exactly the same root/delegated authority
                    // snapshot and the same durable admission as the bounded
                    // process: without them the receipt can never be settled.
                    Self::validate_host_process_outcome(outcome)?;
                    for source in self.bounded_process_sources(request, effect, &intent)? {
                        expected_streams.insert(source.stream_id, source.expected_revision);
                    }
                    let admission_key = format!("{}:process-admission", request.idempotency_key);
                    let published = self
                        .event_store
                        .event_by_idempotency_key(&stream_id, &admission_key)?
                        .ok_or_else(|| {
                            ExecutionCommitError::InvalidCommand(
                                "host process receipt requires its original admission".into(),
                            )
                        })?;
                    let fact = outcome
                        .observed_evidence
                        .iter()
                        .find_map(|observed| match &observed.target {
                            harness_contract::context::EvidenceTargetIdentity::ToolExecution {
                                execution,
                            } => Some(execution),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            ExecutionCommitError::InvalidCommand(
                                "host process receipt requires its execution fact".into(),
                            )
                        })?;
                    if published.payload["admission"] != serde_json::to_value(&fact.admission)? {
                        return Err(ExecutionCommitError::InvalidCommand(
                            "host process receipt admission does not match the published one".into(),
                        ));
                    }
                }
                ProcessTerminalAuthority::Absent => {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "process terminal requires original owner closure publication".into(),
                    ));
                }
            }
        }
        expected_streams.insert(
            stream_id.clone(),
            self.event_store.stream_revision(&stream_id)?,
        );
        let mut events = vec![RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: stream_id.clone(),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.effect.receipt".to_string(),
                status: Some("completed".to_string()),
                actor: Some("governed_tool".to_string()),
                refs: root_sources.as_ref().map_or_else(
                    || tool_effect_refs(request),
                    |(_, owner, attempt)| root_effect_receipt_refs(request, owner, *attempt),
                ),
                payload: local_network::receipt_payload(&self.event_store, request, json!({
                    "root_effect_context":intent.payload["root_effect_context"],
                    "root_effect_owner":intent.payload["root_effect_owner"],
                    "effect":effect,
                    "idempotency_key": request.idempotency_key,
                    "tool_name": request.tool_name,
                    "replay": receipt_replay_contract(request)?,
                    "input_sha256": format!(
                        "sha256:{:x}",
                        Sha256::digest(request.input.as_bytes())
                    ),
                    "descriptor_hash": effect.descriptor_hash,
                    "outcome": bounded_tool_effect_outcome(outcome),
                    "process_admission_ref": matches!(authority,
                        ProcessTerminalAuthority::BoundedProcess(_) | ProcessTerminalAuthority::HostProcess(_))
                        .then(|| format!("{}:process-admission", request.idempotency_key)),
                    "resolved_effect_scope":intent.payload["resolved_effect_scope"],
                }))?,
            },
            idempotency_key: Some(format!("{}:receipt", request.idempotency_key)),
            schema_version: 1,
        }];
        if let ProcessTerminalAuthority::BoundedProcess(closure)
        | ProcessTerminalAuthority::HostProcess(closure) = authority
        {
            events[0].event.payload["process_closure"] = closure.clone();
        }
        if let Some(mut agent_receipt) =
            delegated_agent_receipt_event(request, effect.effect_kind, outcome).or_else(|| {
                root_sources.as_ref().map(|(_, owner, attempt)| {
                    root_index_receipt_event(request, effect.effect_kind, outcome, owner, *attempt)
                })
            })
        {
            agent_receipt.event.payload["root_effect_context"] =
                intent.payload["root_effect_context"].clone();
            agent_receipt.event.payload["process_admission_ref"] =
                events[0].event.payload["process_admission_ref"].clone();
            if let ProcessTerminalAuthority::BoundedProcess(closure)
            | ProcessTerminalAuthority::HostProcess(closure) = authority
            {
                agent_receipt.event.payload["process_closure"] = closure.clone();
            }
            agent_receipt.event.payload["root_effect_owner"] =
                intent.payload["root_effect_owner"].clone();
            agent_receipt.event.payload["effect"] = serde_json::to_value(effect)?;
            agent_receipt.event.payload["effect_scopes"] = serde_json::to_value(&effect.scopes)?;
            agent_receipt.event.payload["resolved_effect_scope"] =
                intent.payload["resolved_effect_scope"].clone();
            self.allocate_receipt_sequence(&mut agent_receipt, &mut expected_streams, &events)?;
            events.push(agent_receipt);
        }
        let appended = self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("tool-effect-receipt:{}", request.idempotency_key),
                expected_streams: expected_streams
                    .into_iter()
                    .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                        stream_id,
                        expected_revision,
                    })
                    .collect(),
                events,
            });
        if let Err(error) = appended {
            if self.completed_tool_effect(request, effect)?.is_some() {
                return self.commit_tool_effect_once(request, effect, outcome, authority);
            }
            return Err(error.into());
        }
        #[cfg(any(test, feature = "test-fixtures"))]
        probe_committed_tool_effect(&self.receipt_commit_probes, request);
        Ok(())
    }

    pub(crate) fn inspect_execution_effect(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
    ) -> Result<ExecutionEffectState, ExecutionCommitError> {
        if ticket.attempt == 0 || ticket.idempotency_key.trim().is_empty() {
            return Err(ExecutionCommitError::InvalidCommand(
                "invalid execution effect ticket".into(),
            ));
        }
        let stream_id = ExecutionNodeBinding::effect_stream_id(ticket);
        let source_head = self.event_store.stream_revision(&stream_id)?;
        let intent = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:intent", ticket.idempotency_key))?;
        let receipt = self
            .event_store
            .event_by_idempotency_key(&stream_id, &format!("{}:receipt", ticket.idempotency_key))?;
        let through = self.event_store.durable_commit_cursor()?;
        let outcome = ExecutionNodeBinding::validate_effect_records(
            ticket,
            intent.as_ref(),
            receipt.as_ref(),
            through,
        )
        .map_err(ExecutionCommitError::InvalidCommand)?;
        let expected_head = u64::from(intent.is_some()) + u64::from(receipt.is_some());
        if source_head != expected_head
            || intent.as_ref().is_some_and(|event| event.sequence != 1)
            || receipt.as_ref().is_some_and(|event| event.sequence != 2)
            || self.event_store.stream_revision(&stream_id)? != source_head
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "NodeEffect stream has an unmodelled or changing original source".into(),
            ));
        }
        if let Some(intent) = intent.as_ref() {
            let original = serde_json::from_value(intent.payload.clone())?;
            self.validate_execution_effect_start(&original, Some(intent))?;
            Ok(outcome.map_or(
                ExecutionEffectState::Uncertain,
                ExecutionEffectState::Completed,
            ))
        } else {
            Ok(ExecutionEffectState::Fresh)
        }
    }

    fn validate_execution_effect_start(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
        intent: Option<&crate::DurableRuntimeEvent>,
    ) -> Result<(), ExecutionCommitError> {
        validate_node_ticket_start(&self.event_store, ticket, intent).map(|_| ())
    }

    fn validate_execution_effect_receipt(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
    ) -> Result<(), ExecutionCommitError> {
        if matches!(
            self.inspect_execution_effect(ticket)?,
            ExecutionEffectState::Fresh
        ) {
            return Err(ExecutionCommitError::InvalidCommand(
                "execution receipt requires original intent".into(),
            ));
        }
        self.validate_execution_effect_start(ticket, None)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn commit_execution_effect(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
        outcome: &super::registry::NodeExecutionOutcome,
    ) -> Result<(), ExecutionCommitError> {
        self.validate_execution_effect_receipt(ticket)?;
        let stream_id = ExecutionNodeBinding::effect_stream_id(ticket);
        let revision = self.event_store.stream_revision(&stream_id)?;
        self.event_store.append_batch_if_revision(
            stream_id,
            revision,
            format!("{}:receipt", ticket.idempotency_key),
            vec![execution_effect_receipt_event(ticket, outcome)?],
        )?;
        Ok(())
    }

    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self::with_hot_state(event_store, Arc::new(RuntimeHotStatePlane::default()))
    }

    #[must_use]
    pub fn with_hot_state(
        event_store: Arc<RuntimeEventStore>,
        hot_state: Arc<RuntimeHotStatePlane>,
    ) -> Self {
        Self {
            effect_authority_actions: crate::agentic::AgentActionService::new(Arc::clone(
                &event_store,
            )),
            effect_authority_graphs: crate::ExecutionGraphStateStore::new(Arc::clone(&event_store)),
            event_store,
            hot_graphs: Arc::clone(hot_state.graphs()),
            hot_state,
            proof_artifacts: None,
            proof_session_queries: None,
            process_input_verifier: None,
            #[cfg(any(test, feature = "test-fixtures"))]
            terminal_response_losses: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            #[cfg(test)]
            fail_next_bound_outcome_450: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(any(test, feature = "test-fixtures"))]
            terminal_append_failures: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            #[cfg(any(test, feature = "test-fixtures"))]
            terminal_append_observers: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            #[cfg(any(test, feature = "test-fixtures"))]
            receipt_commit_probes: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        }
    }

    #[must_use]
    pub(crate) fn with_artifact_store(mut self, artifacts: Arc<crate::ArtifactStore>) -> Self {
        self.proof_artifacts = Some(artifacts);
        self
    }

    #[must_use]
    pub(crate) fn with_session_query_slot(
        mut self,
        queries: Arc<std::sync::OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>,
    ) -> Self {
        self.proof_session_queries = Some(queries);
        self
    }

    #[must_use]
    pub(crate) fn with_process_input_verifier(
        mut self,
        verifier: crate::agentic::review_evidence::ProcessInputVerifier,
    ) -> Self {
        self.process_input_verifier = Some(verifier);
        self
    }

    pub fn register_graph(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        self.register_graph_with_continuation(graph, None)
    }

    pub(crate) fn register_graph_with_continuation(
        &self,
        mut graph: ExecutionGraph,
        continuation_actor: Option<harness_contract::agent_action::AgentActorBinding>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        graph.revision = 1;
        graph.node_statuses.clear();
        graph.node_results.clear();
        graph.recovery_cursor = Default::default();
        for node in &graph.nodes {
            graph
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        let transaction_id = format!("{}:planned", graph.id);
        let lineage_event =
            graph
                .parent_execution
                .as_ref()
                .map(|parent| RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: execution_lineage_stream_id(&parent.execution_id),
                        scope: RuntimeEventScope::Relation,
                        kind: "execution.lineage.child_registered.v1".to_string(),
                        status: Some("registered".to_string()),
                        actor: Some("execution_commit_service".to_string()),
                        refs: vec![
                            RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: parent.execution_id.clone(),
                            },
                            RuntimeEventRef {
                                kind: "execution_node".to_string(),
                                id: parent.node_id.clone(),
                            },
                            RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: graph.id.clone(),
                            },
                        ],
                        payload: json!({
                            "parent_execution_id": parent.execution_id,
                            "parent_node_id": parent.node_id,
                            "child_execution_id": graph.id,
                            "child_objective": graph.objective,
                        }),
                    },
                    idempotency_key: Some(format!("{}:child:{}", parent.execution_id, graph.id)),
                    schema_version: 1,
                });
        // Several child graphs may be compiled concurrently for the same
        // parent tool batch. Their graph streams are independent, but their
        // durable lineage registrations share one parent stream. Retry only
        // that optimistic-concurrency collision: the transaction is atomic
        // and uses stable graph/child idempotency keys, so no child can be
        // double-registered. A collision on the graph's own stream remains a
        // real duplicate-graph error and is returned unchanged.
        let lineage_stream = graph
            .parent_execution
            .as_ref()
            .map(|parent| execution_lineage_stream_id(&parent.execution_id));
        let continuation_event = graph
            .continuation_binding
            .as_ref()
            .map(|binding| {
                crate::session_continuation::graph_continuation_claim_event(binding, &graph.id)
                    .map_err(ExecutionCommitError::InvalidCommand)
            })
            .transpose()?;
        if let Some(event) = continuation_event.as_ref() {
            if let Some(graph_id) = self.existing_continuation_root(event)? {
                return Err(ExecutionCommitError::AlreadyAppliedSame { graph_id });
            }
        }
        let (seed_event, continuation_revisions) = match continuation_actor.as_ref() {
            Some(actor) => {
                let (event, revisions) =
                    crate::agentic::continuation::prepare(&self.event_store, &graph, actor)
                        .map_err(ExecutionCommitError::InvalidCommand)?;
                (event, revisions)
            }
            None => {
                if let Some(source_id) = graph
                    .continuation_binding
                    .as_ref()
                    .and_then(|binding| binding.team_set_ref.strip_prefix("agentic_program:"))
                {
                    if crate::AgentActionService::new(Arc::clone(&self.event_store))
                        .project_if_exists(source_id)
                        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
                        .is_some_and(|source| {
                            source.status != crate::AgenticProgramStatus::Verified
                        })
                    {
                        return Err(ExecutionCommitError::InvalidCommand(
                            "unfinished continuation requires an atomic Program seed".into(),
                        ));
                    }
                }
                (Vec::new(), BTreeMap::new())
            }
        };
        let domain_events = lineage_event
            .into_iter()
            .chain(continuation_event.clone())
            .chain(seed_event)
            .collect::<Vec<_>>();
        let fenced_parent = graph.parent_execution.as_ref().filter(|_| {
            graph.nodes.iter().any(|node| {
                node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                    && node
                        .resource_scopes
                        .iter()
                        .any(|scope| scope.starts_with("program:"))
            })
        });
        let mut last_lineage_conflict = None;
        for _ in 0..8 {
            let mut expected_revisions = continuation_revisions.clone();
            if let Some(parent) = fenced_parent {
                let source = super::ExecutionGraphStateStore::new(Arc::clone(&self.event_store))
                    .load(&parent.execution_id)
                    .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
                if !source.node_statuses.is_empty()
                    && source
                        .node_statuses
                        .values()
                        .all(|status| status.is_terminal())
                {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "Agentic parent is terminal; dispatch authority has ended".into(),
                    ));
                }
                expected_revisions.insert(source.id, source.revision);
            }
            match self.append_graph_event_with_expected_domain_revisions(
                &graph,
                0,
                transaction_id.clone(),
                ExecutionGraphEvent::Planned {
                    graph: graph.clone(),
                },
                domain_events.clone(),
                &expected_revisions,
            ) {
                Ok(receipt) => return Ok(receipt),
                Err(error)
                    if is_lineage_registration_conflict(&error, lineage_stream.as_deref())
                        || is_lineage_registration_conflict(
                            &error,
                            fenced_parent.map(|parent| parent.execution_id.as_str()),
                        ) =>
                {
                    last_lineage_conflict = Some(error);
                }
                Err(error) => {
                    // A racing retry can lose after the preflight lookup.
                    // Re-read the immutable continuation claim before
                    // exposing a failure, so callers resume the durable
                    // winner rather than creating or reporting a second root.
                    if let Some(event) = continuation_event.as_ref() {
                        if let Some(graph_id) = self.existing_continuation_root(event)? {
                            return Err(ExecutionCommitError::AlreadyAppliedSame { graph_id });
                        }
                    }
                    return Err(error);
                }
            }
        }
        Err(last_lineage_conflict.unwrap_or_else(|| {
            ExecutionCommitError::InvalidReplan(
                "lineage registration retry exhausted without a conflict receipt".to_string(),
            )
        }))
    }

    /// Return the root selected by an immutable continuation CAS claim. The
    /// idempotency key already includes ingress and binding digest, so this is
    /// only an `AlreadyAppliedSame` result, never a fuzzy duplicate match.
    fn existing_continuation_root(
        &self,
        event: &RuntimeTransactionEventInput,
    ) -> Result<Option<String>, ExecutionCommitError> {
        let key = event.idempotency_key.as_deref().ok_or_else(|| {
            ExecutionCommitError::MissingDomainIdempotency(event.event.stream_id.clone())
        })?;
        let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&event.event.stream_id, key)?
        else {
            return Ok(None);
        };
        let graph_id = existing
            .payload
            .pointer("/root_graph_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(
                    "existing continuation claim has no durable root graph id".to_string(),
                )
            })?;
        Ok(Some(graph_id.to_string()))
    }

    pub async fn register_graph_async(
        &self,
        graph: ExecutionGraph,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.register_graph(graph))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub(crate) async fn register_continuation_graph_async(
        &self,
        graph: ExecutionGraph,
        actor: Option<harness_contract::agent_action::AgentActorBinding>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.register_graph_with_continuation(graph, actor))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    #[cfg(test)]
    pub(super) fn inject_node_outcome_response_loss_for_test(
        &self,
        node_id: &str,
        hits: Arc<std::sync::atomic::AtomicUsize>,
    ) {
        self.terminal_response_losses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(format!("node-outcome:{node_id}"), hits);
    }

    #[cfg(test)]
    pub(super) fn fail_next_bound_outcome_before_append_for_test(&self) {
        self.fail_next_bound_outcome_450
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(super) fn inject_binding_start_before_append_failure_for_test(
        &self,
        graph_id: &str,
        hits: Arc<std::sync::atomic::AtomicUsize>,
    ) {
        self.terminal_response_losses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(format!("binding-start-before-append:{graph_id}"), hits);
    }

    #[cfg(test)]
    pub(super) fn inject_binding_start_response_loss_for_test(
        &self,
        graph_id: &str,
        hits: Arc<std::sync::atomic::AtomicUsize>,
    ) {
        self.terminal_response_losses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(format!("binding-start:{graph_id}"), hits);
    }

    pub(crate) fn prepare_host_transcript(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
        session_id: &str,
        turn_id: &str,
        prefix_count: u64,
        prefix_sha256: String,
        messages: Vec<serde_json::Value>,
        root_control_team_count: Option<u8>,
    ) -> Result<RuntimeTransactionEventInput, ExecutionCommitError> {
        if !matches!(ticket.executor_kind.as_str(), "inline_model" | "tool_batch") {
            return Err(ExecutionCommitError::InvalidCommand(
                "Host transcript requires original Model/Tool owner".into(),
            ));
        }
        self.prepare_host_publication(
            ticket,
            session_id,
            turn_id,
            prefix_count,
            prefix_sha256,
            messages,
            root_control_team_count,
            None,
        )
    }

    pub(crate) fn prepare_synthesize_transcript(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
        session_id: &str,
        turn_id: &str,
        prefix_count: u64,
        prefix_sha256: String,
        messages: Vec<serde_json::Value>,
        terminal_completion: Option<harness_contract::goal::GoalCompletion>,
    ) -> Result<RuntimeTransactionEventInput, ExecutionCommitError> {
        if ticket.executor_kind != "synthesize" {
            return Err(ExecutionCommitError::InvalidCommand(
                "Synthesize transcript requires original Synthesize owner".into(),
            ));
        }
        self.prepare_host_publication(
            ticket,
            session_id,
            turn_id,
            prefix_count,
            prefix_sha256,
            messages,
            None,
            terminal_completion,
        )
    }

    fn prepare_host_publication(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
        session_id: &str,
        turn_id: &str,
        prefix_count: u64,
        prefix_sha256: String,
        messages: Vec<serde_json::Value>,
        root_control_team_count: Option<u8>,
        terminal_completion: Option<harness_contract::goal::GoalCompletion>,
    ) -> Result<RuntimeTransactionEventInput, ExecutionCommitError> {
        let graph = ExecutionGraphStateStore::new(self.event_store.clone())
            .load(&ticket.graph_id)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let lineage = validated_graph_lineage(&graph)?;
        if lineage.session_id != session_id
            || lineage.turn_id != turn_id
            || graph.node_statuses.get(&ticket.node_id) != Some(&ExecutionNodeStatus::Running)
            || graph.recovery_cursor.node_attempts.get(&ticket.node_id) != Some(&ticket.attempt)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "Host transcript original Running owner mismatch".into(),
            ));
        }
        let original_start = validate_node_ticket_start(&self.event_store, ticket, None)?;
        let document = HostTranscriptPublication {
            schema_version: 1,
            ticket: ticket.clone(),
            session_id: session_id.into(),
            turn_id: turn_id.into(),
            prefix_count,
            prefix_sha256,
            messages,
            root_control_team_count,
            terminal_completion,
        };
        validate_host_transcript_document(&document)
            .map_err(ExecutionCommitError::InvalidCommand)?;
        let key = host_transcript_key(ticket)?;
        let stream = node_stream_id(&ticket.graph_id, &ticket.node_id);
        let origin = crate::PreparedArtifactOrigin::EventBody {
            stream_id: stream.clone(),
            idempotency_key: key.clone(),
            kind: HOST_TRANSCRIPT_KIND.into(),
        };
        let bytes = serde_json::to_vec(&document)?;
        let artifact = self
            .proof_artifacts
            .as_ref()
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("Host transcript content store missing".into())
            })?
            .write_pre_pinned_with_media_blocking(
                &format!("session:{session_id}"),
                &key,
                HOST_TRANSCRIPT_MEDIA,
                &bytes,
                &origin,
            )
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        Ok(RuntimeTransactionEventInput {
            event:RuntimeEventInput {
                stream_id:stream,scope:RuntimeEventScope::ExecutionNode,kind:HOST_TRANSCRIPT_KIND.into(),
                status:Some("prepared".into()),actor:Some(ticket.executor_kind.clone()),
                refs:vec![RuntimeEventRef { kind:"execution_graph".into(),id:ticket.graph_id.clone() },
                    RuntimeEventRef { kind:"execution_node".into(),id:ticket.node_id.clone() }],
                payload:json!({
                    "host_transcript_schema":1,"graph_id":ticket.graph_id,"node_id":ticket.node_id,"attempt":ticket.attempt,
                    "session_id":session_id,"turn_id":turn_id,"ticket_digest":key.strip_prefix("host-transcript:").unwrap(),
                    "body":{"content_schema":2,"sha256":artifact.sha256,"bytes":artifact.bytes,
                        "body":{"kind":"artifact_ref","artifact":artifact}},
                }),
            }.with_activity_binding(node_activity_binding(&original_start,&ticket.node_id)?)?,
            idempotency_key:Some(key),schema_version:2,
        })
    }

    pub(crate) fn attach_host_transcript(
        outcome: &mut super::registry::NodeExecutionOutcome,
        event: RuntimeTransactionEventInput,
    ) -> Result<(), ExecutionCommitError> {
        if outcome
            .domain_events
            .iter()
            .any(|event| event.event.kind == HOST_TRANSCRIPT_KIND)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "duplicate Host transcript publication".into(),
            ));
        }
        outcome
            .result
            .evidence_refs
            .push(host_transcript_reference(&event)?);
        outcome.domain_events.push(event);
        Ok(())
    }

    pub(crate) fn validate_host_transcript_publication(
        store: &RuntimeEventStore,
        request: &AppendTransactionRequest,
        position: usize,
    ) -> Result<harness_contract::context::ArtifactRef, RuntimeEventStoreError> {
        let invalid = |reason: &str| RuntimeEventStoreError::InvalidTransaction(reason.into());
        let input = request
            .events
            .get(position)
            .ok_or_else(|| invalid("Host transcript event position missing"))?;
        let event = &input.event;
        let payload = event
            .payload
            .as_object()
            .ok_or_else(|| invalid("Host transcript payload missing"))?;
        if event.kind != HOST_TRANSCRIPT_KIND
            || event.scope != RuntimeEventScope::ExecutionNode
            || input.schema_version != 2
            || event.status.as_deref() != Some("prepared")
            || payload.len() != 9
            || ![
                "host_transcript_schema",
                "graph_id",
                "node_id",
                "attempt",
                "session_id",
                "turn_id",
                "ticket_digest",
                "body",
                "_runtime_activity_binding",
            ]
            .iter()
            .all(|field| payload.contains_key(*field))
            || event.payload["host_transcript_schema"] != 1
        {
            return Err(invalid("Host transcript event schema invalid"));
        }
        let artifact: harness_contract::context::ArtifactRef =
            serde_json::from_value(event.payload["body"]["body"]["artifact"].clone())?;
        if event.payload["body"]["body"]["kind"] != "artifact_ref"
            || artifact.media_type != HOST_TRANSCRIPT_MEDIA
            || artifact.bytes > HOST_TRANSCRIPT_MAX_BYTES as u64
            || !artifact.is_durable()
        {
            return Err(invalid("Host transcript private body media/size invalid"));
        }
        let key = input
            .idempotency_key
            .as_ref()
            .ok_or_else(|| invalid("Host transcript publication key missing"))?;
        let origin = crate::PreparedArtifactOrigin::EventBody {
            stream_id: event.stream_id.clone(),
            idempotency_key: key.clone(),
            kind: HOST_TRANSCRIPT_KIND.into(),
        };
        let session_id = event.payload["session_id"]
            .as_str()
            .ok_or_else(|| invalid("Host transcript Session missing"))?;
        let value = store
            .resolve_host_transcript_content(
                &format!("session:{session_id}"),
                &event.payload["body"],
                &origin,
            )
            .map_err(RuntimeEventStoreError::InvalidTransaction)?;
        let document: HostTranscriptPublication = serde_json::from_value(value.clone())?;
        validate_host_transcript_document(&document)
            .map_err(RuntimeEventStoreError::InvalidTransaction)?;
        if serde_json::to_value(&document)? != value {
            return Err(invalid(
                "Host transcript body is not the exact typed producer document",
            ));
        }
        let ticket = &document.ticket;
        let expected_key =
            host_transcript_key(ticket).map_err(|error| invalid(&error.to_string()))?;
        let activity = event
            .activity_binding()
            .ok_or_else(|| invalid("Host transcript activity missing"))?;
        if key != &expected_key
            || event.stream_id != node_stream_id(&ticket.graph_id, &ticket.node_id)
            || event.actor.as_deref() != Some(ticket.executor_kind.as_str())
            || event.payload["graph_id"] != ticket.graph_id
            || event.payload["node_id"] != ticket.node_id
            || event.payload["attempt"] != ticket.attempt
            || event.payload["ticket_digest"] != key.strip_prefix("host-transcript:").unwrap_or("")
            || document.session_id != session_id
            || event.payload["turn_id"] != document.turn_id
            || activity.session_id != document.session_id
            || activity.turn_id != document.turn_id
            || activity.root_execution_id != ticket.graph_id
            || activity.node_id.as_deref() != Some(ticket.node_id.as_str())
            || artifact.visibility_scope != format!("session:{session_id}")
        {
            return Err(invalid("Host transcript original identity mismatch"));
        }
        let original_start = validate_node_ticket_start(store, ticket, None)
            .map_err(|error| invalid(&error.to_string()))?;
        let original_activity = node_activity_binding(&original_start, &ticket.node_id)
            .map_err(|error| invalid(&error.to_string()))?;
        if activity != original_activity {
            return Err(invalid(
                "Host transcript activity does not match authenticated original start",
            ));
        }
        let graphs = request
            .events
            .iter()
            .filter(|candidate| {
                candidate.event.stream_id == ticket.graph_id
                    && candidate.event.scope == RuntimeEventScope::ExecutionGraph
            })
            .collect::<Vec<_>>();
        if graphs.len() != 1 {
            return Err(invalid(
                "Host transcript requires one atomic original Graph result",
            ));
        }
        let graph_input = graphs[0];
        if request
            .events
            .first()
            .is_none_or(|first| !std::ptr::eq(first, graph_input))
        {
            return Err(invalid("Host transcript atomic Graph source must be first"));
        }
        let graph_origin = crate::PreparedArtifactOrigin::EventBody {
            stream_id: graph_input.event.stream_id.clone(),
            idempotency_key: graph_input
                .idempotency_key
                .clone()
                .ok_or_else(|| invalid("Host transcript Graph key missing"))?,
            kind: graph_input.event.kind.clone(),
        };
        let graph_event: ExecutionGraphEvent = serde_json::from_value(
            store
                .resolve_content(
                    &format!("session:{session_id}"),
                    &graph_input.event.payload,
                    &graph_origin,
                )
                .map_err(RuntimeEventStoreError::InvalidTransaction)?,
        )?;
        if graph_event.kind() != graph_input.event.kind
            || graph_input.schema_version != 2
            || graph_input.event.actor.as_deref() != Some("execution_commit_service")
        {
            return Err(invalid("Host transcript Graph source invalid"));
        }
        let result = match &graph_event {
            ExecutionGraphEvent::NodeTransitioned {
                node_id,
                from: ExecutionNodeStatus::Running,
                result: Some(result),
                ..
            }
            | ExecutionGraphEvent::NodeTransitionedAndReplanned {
                node_id,
                from: ExecutionNodeStatus::Running,
                result,
                ..
            } if node_id == &ticket.node_id => Some(result),
            ExecutionGraphEvent::NodesTransitioned { node_ids, delta }
                if node_ids.contains(&ticket.node_id) =>
            {
                delta.node_result_updates.get(&ticket.node_id)
            }
            ExecutionGraphEvent::Checkpoint { graph, cause, .. }
                if matches!(
                    cause.as_str(),
                    "execution_graph.node_transitioned"
                        | "execution_graph.node_transitioned_and_replanned"
                        | "execution_graph.nodes_transitioned"
                ) && graph.recovery_cursor.node_attempts.get(&ticket.node_id)
                    == Some(&ticket.attempt) =>
            {
                graph.node_results.get(&ticket.node_id)
            }
            _ => None,
        }
        .ok_or_else(|| invalid("Host transcript has no original Graph result transition"))?;
        super::runner::validate_outcome(
            &ticket.node_id,
            &super::registry::NodeExecutionOutcome::new(result.clone()),
        )
        .map_err(|error| invalid(&error.to_string()))?;
        let (revision, attempt) = match &graph_event {
            ExecutionGraphEvent::Checkpoint { graph, .. } => (
                graph.revision,
                graph
                    .recovery_cursor
                    .node_attempts
                    .get(&ticket.node_id)
                    .copied(),
            ),
            ExecutionGraphEvent::NodeTransitioned { delta, .. }
            | ExecutionGraphEvent::NodeTransitionedAndReplanned { delta, .. }
            | ExecutionGraphEvent::NodesTransitioned { delta, .. } => (
                delta.revision,
                delta
                    .recovery_cursor
                    .node_attempts
                    .get(&ticket.node_id)
                    .copied(),
            ),
            _ => return Err(invalid("Host transcript Graph result variant invalid")),
        };
        if revision == 0
            || attempt != Some(ticket.attempt)
            || graph_input.idempotency_key.as_deref()
                != Some(format!("{}:revision:{revision}", ticket.graph_id).as_str())
            || !request.expected_streams.iter().any(|source| {
                source.stream_id == ticket.graph_id && source.expected_revision == revision - 1
            })
            || !request
                .expected_streams
                .iter()
                .any(|source| source.stream_id == event.stream_id && source.expected_revision > 0)
        {
            return Err(invalid(
                "Host transcript original Graph/node CAS source missing or foreign",
            ));
        }
        let expected_ref =
            host_transcript_reference(input).map_err(|error| invalid(&error.to_string()))?;
        if result
            .evidence_refs
            .iter()
            .filter(|reference| reference.retrieval_selector == *key)
            .collect::<Vec<_>>()
            != vec![&expected_ref]
        {
            return Err(invalid("Host transcript Graph result reference mismatch"));
        }
        let nodes = request
            .events
            .iter()
            .filter(|candidate| {
                candidate.event.stream_id == event.stream_id
                    && candidate.event.kind == "execution_node.transitioned"
            })
            .collect::<Vec<_>>();
        if nodes.len() != 1
            || nodes[0].event.scope != RuntimeEventScope::ExecutionNode
            || nodes[0].schema_version != 1
            || nodes[0].event.actor.as_deref() != Some("execution_commit_service")
            || nodes[0].event.payload["from"] != serde_json::to_value(ExecutionNodeStatus::Running)?
            || nodes[0].event.payload["to"] != serde_json::to_value(result.status)?
            || nodes[0].event.status.as_deref() != Some(status_name(result.status))
            || nodes[0].event.payload["graph_revision"] != revision
            || nodes[0].idempotency_key.as_deref()
                != Some(format!("{}:{}:{revision}", ticket.graph_id, ticket.node_id).as_str())
            || nodes[0].event.payload["graph_id"] != ticket.graph_id
            || nodes[0].event.payload["node_id"] != ticket.node_id
            || nodes[0].event.payload["result"] != serde_json::to_value(result)?
        {
            return Err(invalid("Host transcript atomic node transition mismatch"));
        }
        let node_activity = nodes[0]
            .event
            .activity_binding()
            .ok_or_else(|| invalid("Host transcript original node activity missing"))?;
        let mut expected_node_activity = original_activity;
        expected_node_activity.revision = revision;
        expected_node_activity.fence = revision;
        let mut expected_root_activity =
            root_activity_binding(&original_start).map_err(|error| invalid(&error.to_string()))?;
        expected_root_activity.revision = revision;
        expected_root_activity.fence = revision;
        if node_activity != expected_node_activity
            || node_activity.validate().is_err()
            || activity.revision >= revision
            || activity.fence >= revision
            || graph_input.event.activity_binding().as_ref() != Some(&expected_root_activity)
        {
            return Err(invalid(
                "Host transcript original generation/parent/Task activity mismatch",
            ));
        }
        let preserves_start_owner = match &graph_event {
            ExecutionGraphEvent::Checkpoint { graph, .. } => {
                graph.id == original_start.id
                    && graph.lineage == original_start.lineage
                    && graph.parent_execution == original_start.parent_execution
            }
            ExecutionGraphEvent::NodeTransitioned { delta, .. }
            | ExecutionGraphEvent::NodeTransitionedAndReplanned { delta, .. }
            | ExecutionGraphEvent::NodesTransitioned { delta, .. } => delta
                .parent_execution
                .as_ref()
                .is_none_or(|parent| parent == &original_start.parent_execution),
            _ => false,
        };
        if !preserves_start_owner {
            return Err(invalid(
                "Host transcript Graph changed original lineage/parent",
            ));
        }
        if ticket.executor_kind != "tool_batch" {
            let receipts = request
                .events
                .iter()
                .filter(|candidate| {
                    candidate.event.stream_id == ExecutionNodeBinding::effect_stream_id(ticket)
                        && candidate.event.kind == "execution.effect.receipt"
                })
                .collect::<Vec<_>>();
            if receipts.len() != 1
                || receipts[0].event.payload["result"] != serde_json::to_value(result)?
                || receipts[0].event.scope != RuntimeEventScope::ExecutionNode
                || receipts[0].event.status.as_deref() != Some("completed")
                || receipts[0].event.refs
                    != vec![RuntimeEventRef {
                        kind: "execution_node".into(),
                        id: ticket.node_id.clone(),
                    }]
                || receipts[0].event.actor.as_deref() != Some(ticket.executor_kind.as_str())
                || receipts[0].schema_version != 1
                || receipts[0].idempotency_key.as_deref()
                    != Some(format!("{}:receipt", ticket.idempotency_key).as_str())
            {
                return Err(invalid(
                    "Host transcript atomic NodeEffect receipt mismatch",
                ));
            }
            if ticket.executor_kind == "synthesize" {
                let outcome: super::registry::NodeExecutionOutcome =
                    serde_json::from_value(receipts[0].event.payload.clone())?;
                match document.terminal_completion {
                    Some(_) => {
                        let blocks: Vec<crate::ContentBlock> =
                            serde_json::from_value(document.messages[0]["blocks"].clone())?;
                        let [crate::ContentBlock::Text { text }] = blocks.as_slice() else {
                            return Err(invalid("Synthesize original terminal Text missing"));
                        };
                        let expected = format!(
                            "turn-result:{}:{:x}",
                            ticket.graph_id,
                            Sha256::digest(text.as_bytes())
                        );
                        if result.status != ExecutionNodeStatus::Completed
                            || result.result_ref.as_deref() != Some(expected.as_str())
                            || outcome.replan.is_some()
                        {
                            return Err(invalid(
                                "Synthesize wording does not match original terminal result digest",
                            ));
                        }
                    }
                    None => {
                        let replan = outcome.replan.as_ref().ok_or_else(|| {
                            invalid("Synthesize empty publication has no original replan")
                        })?;
                        let original_replan = match &graph_event {
                            ExecutionGraphEvent::NodeTransitionedAndReplanned {
                                reason,
                                added_node_ids,
                                delta,
                                ..
                            } => {
                                reason == &replan.reason
                                    && added_node_ids
                                        == &replan
                                            .nodes
                                            .iter()
                                            .map(|node| node.id.clone())
                                            .collect::<Vec<_>>()
                                    && replan
                                        .nodes
                                        .iter()
                                        .all(|node| delta.added_nodes.contains(node))
                                    && replan
                                        .edges
                                        .iter()
                                        .all(|edge| delta.added_edges.contains(edge))
                            }
                            ExecutionGraphEvent::Checkpoint { graph, cause, .. } => {
                                cause == "execution_graph.node_transitioned_and_replanned"
                                    && replan.nodes.iter().all(|node| graph.nodes.contains(node))
                                    && replan.edges.iter().all(|edge| graph.edges.contains(edge))
                            }
                            _ => false,
                        };
                        if !original_replan {
                            return Err(invalid("Synthesize empty publication is not paired with original Graph replan"));
                        }
                    }
                }
            }
        }
        Ok(artifact)
    }

    pub(crate) fn committed_host_transcript(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
    ) -> Result<HostTranscriptPublication, ExecutionCommitError> {
        let invalid = |reason: &str| ExecutionCommitError::InvalidCommand(reason.into());
        let state = ExecutionGraphStateStore::new(self.event_store.clone());
        let graph = state
            .load(&ticket.graph_id)
            .map_err(|error| invalid(&error.to_string()))?;
        let binding = state
            .current_binding(&graph, &ticket.node_id, ticket.attempt)
            .map_err(|error| invalid(&error.to_string()))?;
        let (original, _) = state
            .binding_start_ticket(&graph, &ticket.node_id, &binding)
            .map_err(|error| invalid(&error.to_string()))?;
        if original != *ticket {
            return Err(invalid("Host transcript original ticket mismatch"));
        }
        let Some((_, completion)) =
            self.committed_node_attempt(&graph, &ticket.node_id, &binding)?
        else {
            return Err(invalid("Host transcript original committed result missing"));
        };
        let key = host_transcript_key(ticket)?;
        let stream = node_stream_id(&ticket.graph_id, &ticket.node_id);
        let publication = self
            .event_store
            .event_by_idempotency_key(&stream, &key)?
            .ok_or_else(|| invalid("Host transcript canonical publication missing"))?;
        let node = self
            .event_store
            .event_by_idempotency_key(
                &stream,
                &format!(
                    "{}:{}:{}",
                    ticket.graph_id, ticket.node_id, completion.sequence
                ),
            )?
            .ok_or_else(|| invalid("Host transcript canonical node result missing"))?;
        let mut records = vec![completion.clone(), node, publication.clone()];
        if ticket.executor_kind != "tool_batch" {
            records.push(
                self.event_store
                    .event_by_idempotency_key(
                        &ExecutionNodeBinding::effect_stream_id(ticket),
                        &format!("{}:receipt", ticket.idempotency_key),
                    )?
                    .ok_or_else(|| {
                        invalid("Host transcript canonical NodeEffect receipt missing")
                    })?,
            );
        }
        if records.iter().any(|record| {
            record.transaction_id != completion.transaction_id
                || record.commit_cursor != completion.commit_cursor
        }) {
            return Err(invalid(
                "Host transcript publication not in original result transaction",
            ));
        }
        records.sort_by_key(|record| record.transaction_index);
        let position = records
            .iter()
            .position(|record| record.kind == HOST_TRANSCRIPT_KIND)
            .ok_or_else(|| invalid("Host transcript record missing"))?;
        let mut expected_streams = Vec::new();
        let mut streams = BTreeSet::new();
        for record in &records {
            if streams.insert(record.stream_id.clone()) {
                let source = self
                    .event_store
                    .transaction_stream_revision_exact(
                        &completion.transaction_id,
                        &record.stream_id,
                    )?
                    .ok_or_else(|| {
                        invalid("Host transcript canonical transaction source missing")
                    })?;
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: record.stream_id.clone(),
                    expected_revision: source.expected_revision,
                });
            }
        }
        let request = AppendTransactionRequest {
            transaction_id: completion.transaction_id.clone(),
            expected_streams,
            events: records
                .into_iter()
                .map(|record| RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: record.stream_id,
                        scope: record.scope,
                        kind: record.kind,
                        status: record.status,
                        actor: record.actor,
                        refs: record.refs,
                        payload: record.payload,
                    },
                    idempotency_key: record.idempotency_key,
                    schema_version: record.schema_version,
                })
                .collect(),
        };
        Self::validate_host_transcript_publication(&self.event_store, &request, position)?;
        let origin = crate::PreparedArtifactOrigin::EventBody {
            stream_id: stream,
            idempotency_key: key,
            kind: HOST_TRANSCRIPT_KIND.into(),
        };
        Ok(serde_json::from_value(
            self.event_store
                .resolve_host_transcript_content(
                    &format!(
                        "session:{}",
                        publication.payload["session_id"]
                            .as_str()
                            .ok_or_else(|| invalid("Host transcript Session missing"))?
                    ),
                    &publication.payload["body"],
                    &origin,
                )
                .map_err(ExecutionCommitError::InvalidCommand)?,
        )?)
    }

    pub(crate) fn validate_unadmitted_binding_start(
        &self,
        graph: &ExecutionGraph,
        node: &ExecutionNodeSpec,
        ticket: &super::registry::NodeExecutionTicket,
    ) -> Result<(), ExecutionCommitError> {
        let invalid = |reason: &str| ExecutionCommitError::InvalidCommand(reason.into());
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        let actual = self.event_store.stream_revision(&graph.id)?;
        if actual != graph.revision {
            return Err(crate::RuntimeEventStoreError::StaleRevision {
                stream_id: graph.id.clone(),
                expected: graph.revision,
                actual,
            }
            .into());
        }
        let current = store
            .load(&graph.id)
            .map_err(|error| invalid(&error.to_string()))?;
        if current.revision != graph.revision {
            return Err(crate::RuntimeEventStoreError::StaleRevision {
                stream_id: graph.id.clone(),
                expected: graph.revision,
                actual: current.revision,
            }
            .into());
        }
        if current != *graph
            || current
                .nodes
                .iter()
                .find(|candidate| candidate.id == node.id)
                != Some(node)
            || current.node_statuses.get(&node.id) != Some(&ExecutionNodeStatus::Ready)
        {
            return Err(invalid("unadmitted original Ready/spec mismatch"));
        }
        let attempt = ExecutionNodeBinding::next_attempt(&current, &node.id)
            .map_err(ExecutionCommitError::InvalidCommand)?;
        ExecutionNodeBinding::validate_ticket(ticket, &current, node, attempt)
            .map_err(ExecutionCommitError::InvalidCommand)?;
        let mut after = 0;
        while after < current.revision {
            let (next, starts) = store
                .binding_starts_page(&current.id, after, current.revision)
                .map_err(|error| invalid(&error.to_string()))?;
            if next <= after {
                return Err(invalid("unadmitted starts page did not advance"));
            }
            if starts.iter().any(|(node_id, binding, _)| {
                node_id == &node.id && binding.attempt == ticket.attempt
            }) {
                return Err(invalid(
                    "binding already admitted; owner marker cannot authorize replay",
                ));
            }
            after = next;
        }
        self.validate_fresh_node_admission(&current, node, ticket)?;
        let actual = self.event_store.stream_revision(&graph.id)?;
        if actual != graph.revision {
            return Err(crate::RuntimeEventStoreError::StaleRevision {
                stream_id: graph.id.clone(),
                expected: graph.revision,
                actual,
            }
            .into());
        }
        Ok(())
    }

    pub(super) fn validate_unadmitted_binding_rollback(
        &self,
        ticket: &super::registry::NodeExecutionTicket,
    ) -> Result<(), ExecutionCommitError> {
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        let graph = store
            .load(&ticket.graph_id)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let mut after = 0;
        while after < graph.revision {
            let (next, starts) = store
                .binding_starts_page(&graph.id, after, graph.revision)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            if next <= after {
                return Err(ExecutionCommitError::InvalidCommand(
                    "unadmitted rollback source did not advance".into(),
                ));
            }
            if starts.iter().any(|(node, binding, _)| {
                node == &ticket.node_id && binding.attempt == ticket.attempt
            }) {
                return Err(ExecutionCommitError::InvalidCommand(
                    "unadmitted rollback cannot clear a canonical admitted binding".into(),
                ));
            }
            after = next;
        }
        let actual = self.event_store.stream_revision(&graph.id)?;
        if actual != graph.revision {
            return Err(crate::RuntimeEventStoreError::StaleRevision {
                stream_id: graph.id,
                expected: graph.revision,
                actual,
            }
            .into());
        }
        Ok(())
    }

    pub(super) fn binding_start_was_admitted(
        &self,
        graph_id: &str,
        ticket: &super::registry::NodeExecutionTicket,
        binding: &ExecutionNodeBinding,
    ) -> Result<bool, ExecutionCommitError> {
        let invalid = |reason: &str| ExecutionCommitError::InvalidCommand(reason.into());
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        let graph = store
            .load(graph_id)
            .map_err(|error| invalid(&error.to_string()))?;
        let Some(_) = store
            .binding_start_position(&graph, &ticket.node_id, binding)
            .map_err(|error| invalid(&error.to_string()))?
        else {
            return Ok(false);
        };
        let (original, spec) = store
            .binding_start_ticket(&graph, &ticket.node_id, binding)
            .map_err(|error| invalid(&error.to_string()))?;
        if original != *ticket {
            return Err(invalid("admitted ticket mismatch"));
        }
        if spec.kind == ExecutionNodeKind::ToolBatch && spec.executor_kind == "tool_batch" {
            validate_node_ticket_start(&self.event_store, ticket, None)?;
        } else if !matches!(
            self.inspect_execution_effect(ticket)?,
            ExecutionEffectState::Uncertain
        ) {
            return Err(invalid("unpolled admission has no exact atomic intent"));
        }
        Ok(true)
    }

    pub(super) fn binding_callback_disposition(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<CallbackDisposition, ExecutionCommitError> {
        if let Some((result, _)) = self.committed_node_attempt(graph, node, binding)? {
            return Ok(CallbackDisposition::from_result(node, &result));
        }
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        Ok(
            if store
                .binding_retirement_record(graph, node, binding)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
                .is_some()
            {
                CallbackDisposition::Abort
            } else {
                CallbackDisposition::Pending
            },
        )
    }

    pub(super) fn authenticate_closed_binding(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<(), ExecutionCommitError> {
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        let (ticket, spec) = store
            .binding_start_ticket(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        if self.committed_node_attempt(graph, node, binding)?.is_some() {
            return Ok(());
        }
        if store
            .binding_retirement_record(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
            .is_none()
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "closed binding has no original result or canonical retirement".into(),
            ));
        }
        if spec.kind != ExecutionNodeKind::ToolBatch || spec.executor_kind != "tool_batch" {
            if !matches!(
                self.inspect_execution_effect(&ticket)?,
                ExecutionEffectState::Uncertain
            ) {
                return Err(ExecutionCommitError::InvalidCommand(
                    "retired binding missing exact original intent".into(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_fresh_node_admission(
        &self,
        graph: &ExecutionGraph,
        node: &ExecutionNodeSpec,
        ticket: &super::registry::NodeExecutionTicket,
    ) -> Result<(), ExecutionCommitError> {
        if self.inspect_execution_effect(ticket)? != ExecutionEffectState::Fresh {
            return Err(ExecutionCommitError::InvalidCommand(
                "new attempt cannot borrow Completed or Uncertain effect".into(),
            ));
        }
        let previous = graph
            .recovery_cursor
            .node_attempts
            .get(&node.id)
            .copied()
            .unwrap_or(0);
        if previous == 0 {
            return Ok(());
        }
        if node.kind == ExecutionNodeKind::AgentTask {
            return Err(ExecutionCommitError::InvalidCommand(
                "AgentTask fresh run requires its original dispatch owner, not resumable pause"
                    .into(),
            ));
        }
        let store = ExecutionGraphStateStore::new(Arc::clone(&self.event_store));
        let binding = store
            .current_binding(graph, &node.id, previous)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        if !store
            .binding_ready_command(graph, &node.id, &binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "new attempt has no original Resume/approval/advance authority".into(),
            ));
        }
        if binding.has_lease_obligations() {
            let position = store
                .binding_start_position(graph, &node.id, &binding)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand("prior attempt start missing".into())
                })?;
            if !store
                .binding_lease_closure(graph, &node.id, &binding, position)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "new attempt prior binding not durably retired".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn binding_requires_host_publication(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<bool, ExecutionCommitError> {
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        let (ticket, _) = store
            .binding_start_ticket(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let key = host_transcript_key(&ticket)?;
        let primary = self
            .event_store
            .event_by_idempotency_key(&node_stream_id(&graph.id, node), &key)?;
        let completion = self.committed_node_attempt(graph, node, binding)?;
        let referenced = completion.as_ref().is_some_and(|(result, _)| {
            result
                .evidence_refs
                .iter()
                .any(|reference| reference.retrieval_selector == key)
        });
        if primary.is_none() && !referenced {
            return Ok(false);
        }
        let document = self.committed_host_transcript(&ticket)?;
        Ok(!document.messages.is_empty()
            || document.root_control_team_count.is_some()
            || document.terminal_completion.is_some())
    }

    pub(super) async fn recover_closed_binding_publication(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
        registry: &super::registry::NodeExecutorRegistry,
    ) -> Result<(), ExecutionCommitError> {
        self.authenticate_closed_binding(graph, node, binding)?;
        if !self.binding_requires_host_publication(graph, node, binding)? {
            return Ok(());
        }
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        let (ticket, _) = store
            .binding_start_ticket(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let executor = registry.get(&ticket.executor_kind).ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "closed Host publication requires original rebound Session owner".into(),
            )
        })?;
        executor
            .after_commit(&ticket)
            .await
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        Ok(())
    }

    fn committed_node_attempt(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<Option<(ExecutionNodeResult, crate::DurableRuntimeEvent)>, ExecutionCommitError>
    {
        let store = ExecutionGraphStateStore::new(Arc::clone(&self.event_store));
        let (ticket, spec) = store
            .binding_start_ticket(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let Some((result, record)) = store
            .committed_binding_result(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
        else {
            return Ok(None);
        };
        if spec.kind != ExecutionNodeKind::ToolBatch || spec.executor_kind != "tool_batch" {
            let ExecutionEffectState::Completed(outcome) =
                self.inspect_execution_effect(&ticket)?
            else {
                return Ok(None);
            };
            let receipt = self
                .event_store
                .event_by_idempotency_key(
                    &ExecutionNodeBinding::effect_stream_id(&ticket),
                    &format!("{}:receipt", ticket.idempotency_key),
                )?
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand("committed attempt receipt missing".into())
                })?;
            if outcome.result != result
                || record.transaction_id != receipt.transaction_id
                || record.commit_cursor != receipt.commit_cursor
                || record.transaction_index >= receipt.transaction_index
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "Node effect and original Graph result are not one transaction".into(),
                ));
            }
        }
        Ok(Some((result, record)))
    }

    fn prepare_execution_outcome(
        &self,
        graph: &ExecutionGraph,
        ticket: &super::registry::NodeExecutionTicket,
        outcome: &super::registry::NodeExecutionOutcome,
        expected: &BTreeMap<String, u64>,
    ) -> Result<PreparedGraphAppend, ExecutionCommitError> {
        validate_executor_domain_events(&outcome.domain_events)?;
        let mut sources = expected.clone();
        merge_outcome_source_revisions(&mut sources, &outcome.source_revisions)?;
        super::runner::validate_outcome(&ticket.node_id, outcome)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        if graph.node_statuses.get(&ticket.node_id) != Some(&ExecutionNodeStatus::Running)
            || graph.recovery_cursor.node_attempts.get(&ticket.node_id) != Some(&ticket.attempt)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "atomic outcome no longer owns current Running attempt".into(),
            ));
        }
        let store = ExecutionGraphStateStore::new(Arc::clone(&self.event_store));
        let binding = store
            .current_binding(graph, &ticket.node_id, ticket.attempt)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let (original, spec) = store
            .binding_start_ticket(graph, &ticket.node_id, &binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        if original != *ticket {
            return Err(ExecutionCommitError::InvalidCommand(
                "atomic outcome original ticket mismatch".into(),
            ));
        }
        let leaf = spec.kind == ExecutionNodeKind::ToolBatch && spec.executor_kind == "tool_batch";
        let effect_stream = ExecutionNodeBinding::effect_stream_id(ticket);
        let effect_head = self.event_store.stream_revision(&effect_stream)?;
        if !leaf {
            self.validate_execution_effect_receipt(ticket)?;
            if !matches!(
                self.inspect_execution_effect(ticket)?,
                ExecutionEffectState::Uncertain
            ) {
                return Err(ExecutionCommitError::InvalidCommand(
                    "atomic outcome requires exact pending original intent".into(),
                ));
            }
        }
        let no_dispatch = outcome
            .no_dispatch
            .as_ref()
            .map(|witness| {
                self.materialize_no_dispatch_fact(
                    graph,
                    ticket,
                    &binding,
                    witness,
                    Some(&outcome.result),
                )
            })
            .transpose()?;
        if let Some(fact) = &no_dispatch {
            merge_outcome_source_revisions(&mut sources, &fact.historical_sources)?;
        }
        let expected = &sources;
        let mut prepared = if let Some(replan) = &outcome.replan {
            self.prepare_transition_node_with_replan(
                graph,
                &ticket.node_id,
                outcome.result.clone(),
                outcome.domain_events.clone(),
                replan.nodes.clone(),
                replan.edges.clone(),
                replan.reason.clone(),
                replan.framework_authored,
                expected,
                Some(outcome),
            )?
        } else {
            self.prepare_transition_node(
                graph,
                &ticket.node_id,
                outcome.result.status,
                Some(outcome.result.clone()),
                outcome.domain_events.clone(),
                expected,
                Some(outcome),
            )?
        };
        if let Some(fact) = no_dispatch {
            let key = format!(
                "{}:{}:{}",
                graph.id, ticket.node_id, prepared.graph.revision
            );
            let node_event = prepared
                .events
                .iter_mut()
                .find(|event| {
                    event.event.kind == "execution_node.transitioned"
                        && event.event.stream_id == node_stream_id(&graph.id, &ticket.node_id)
                        && event.idempotency_key.as_deref() == Some(key.as_str())
                })
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "NoDispatch generated Node result missing".into(),
                    )
                })?;
            node_event.event.payload["no_dispatch"] = serde_json::to_value(fact)?;
        }
        if !leaf {
            if let Some(source) = prepared
                .expected_streams
                .iter()
                .find(|source| source.stream_id == effect_stream)
            {
                if source.expected_revision != effect_head {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "atomic effect source fence mismatch".into(),
                    ));
                }
            } else {
                prepared.expected_streams.push(ExpectedStreamRevision {
                    stream_id: effect_stream,
                    expected_revision: effect_head,
                });
            }
            prepared
                .events
                .push(execution_effect_receipt_event(ticket, outcome)?);
        }
        Ok(prepared)
    }

    #[cfg(test)]
    pub(super) fn execution_outcome_request_for_test(
        &self,
        graph: &ExecutionGraph,
        ticket: &super::registry::NodeExecutionTicket,
        outcome: &super::registry::NodeExecutionOutcome,
    ) -> Result<AppendTransactionRequest, ExecutionCommitError> {
        let prepared = self.prepare_execution_outcome(graph, ticket, outcome, &BTreeMap::new())?;
        Ok(AppendTransactionRequest {
            transaction_id: prepared.transaction_id,
            expected_streams: prepared.expected_streams,
            events: prepared.events,
        })
    }

    pub(crate) fn execution_outcome_sources(
        &self,
        graph: &ExecutionGraph,
        ticket: &super::registry::NodeExecutionTicket,
        outcome: &super::registry::NodeExecutionOutcome,
    ) -> Result<BTreeMap<String, u64>, ExecutionCommitError> {
        let prepared = self.prepare_execution_outcome(graph, ticket, outcome, &BTreeMap::new())?;
        Ok(prepared
            .expected_streams
            .iter()
            .filter(|source| {
                source.stream_id != graph.id
                    && !prepared.refreshable_lineage.contains(&source.stream_id)
            })
            .map(|source| (source.stream_id.clone(), source.expected_revision))
            .collect())
    }

    pub(crate) fn transition_execution_outcome(
        &self,
        graph: &ExecutionGraph,
        ticket: &super::registry::NodeExecutionTicket,
        outcome: &super::registry::NodeExecutionOutcome,
        expected_domain_revisions: &BTreeMap<String, u64>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let prepared =
            self.prepare_execution_outcome(graph, ticket, outcome, expected_domain_revisions)?;
        let mut replay = prepared.clone();
        match self.commit_prepared_graph_append(prepared) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                let stored = self
                    .event_store
                    .transaction_stream_revision_exact(&replay.transaction_id, &graph.id)?;
                if stored.is_some_and(|stored| {
                    stored.expected_revision == graph.revision
                        && stored.committed_revision == replay.graph.revision
                }) {
                    for source in &mut replay.expected_streams {
                        if replay.refreshable_lineage.contains(&source.stream_id) {
                            let exact = self
                                .event_store
                                .transaction_stream_revision_exact(
                                    &replay.transaction_id,
                                    &source.stream_id,
                                )?
                                .ok_or_else(|| {
                                    ExecutionCommitError::InvalidCommand(
                                        "committed generated lineage source missing".into(),
                                    )
                                })?;
                            source.expected_revision = exact.expected_revision;
                        }
                    }
                    self.commit_prepared_graph_append(replay)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(crate) async fn transition_execution_outcome_async(
        &self,
        graph: ExecutionGraph,
        ticket: super::registry::NodeExecutionTicket,
        outcome: super::registry::NodeExecutionOutcome,
        expected_domain_revisions: BTreeMap<String, u64>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.transition_execution_outcome(
                &graph,
                &ticket,
                &outcome,
                &expected_domain_revisions,
            )
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    fn apply_outcome_presentation(
        next: &mut ExecutionGraph,
        outcome: Option<&super::registry::NodeExecutionOutcome>,
    ) {
        if let Some(outcome) = outcome {
            if let Some(envelope) = &outcome.delivery_envelope {
                next.delivery_envelope = Some(envelope.clone());
            }
            if let Some(presentation) = &outcome.terminal_presentation {
                next.terminal_presentation = Some(presentation.clone());
            }
        }
    }

    pub fn transition_node(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        to: ExecutionNodeStatus,
        result: Option<ExecutionNodeResult>,
        domain_events: Vec<RuntimeTransactionEventInput>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let prepared = self.prepare_transition_node(
            graph,
            node_id,
            to,
            result,
            domain_events,
            &BTreeMap::new(),
            None,
        )?;
        self.commit_prepared_graph_append(prepared)
    }

    fn prepare_transition_node(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        to: ExecutionNodeStatus,
        result: Option<ExecutionNodeResult>,
        domain_events: Vec<RuntimeTransactionEventInput>,
        expected_sources: &BTreeMap<String, u64>,
        presentation: Option<&super::registry::NodeExecutionOutcome>,
    ) -> Result<PreparedGraphAppend, ExecutionCommitError> {
        validate_executor_domain_events(&domain_events)?;
        let from = *graph
            .node_statuses
            .get(node_id)
            .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.to_string()))?;
        let mut next = apply_node_transition(graph, graph.revision, node_id, to, result.clone())?;
        Self::apply_outcome_presentation(&mut next, presentation);
        let node_attempt = next
            .recovery_cursor
            .node_attempts
            .entry(node_id.to_string())
            .or_default();
        if to == ExecutionNodeStatus::Running {
            *node_attempt = node_attempt.saturating_add(1);
        }
        let transaction_id = format!("{}:{}:{}:{}", graph.id, node_id, next.revision, to as u8);
        let graph_event = ExecutionGraphEvent::NodeTransitioned {
            node_id: node_id.to_string(),
            from,
            to,
            result: result.clone(),
            binding: None,
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        let node_event = node_transition_event(&next, node_id, from, to, result)?;
        let mut events = vec![node_event];
        events.extend(domain_events);
        if let Some(candidate) = crate::knowledge_candidate_projector::team_terminal_candidate(
            &next,
            node_id,
            to,
            next.node_results.get(node_id),
        ) {
            events.push(
                crate::knowledge_candidate_projector::candidate_proposal_event(candidate)
                    .map_err(ExecutionCommitError::InvalidCommand)?,
            );
        }
        self.prepare_graph_append(
            &next,
            graph.revision,
            transaction_id,
            graph_event,
            events,
            expected_sources,
        )
    }

    pub fn bind_and_start_node(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        binding: ExecutionNodeBinding,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        binding
            .validate_start(graph, node_id)
            .map_err(ExecutionCommitError::InvalidCommand)?;
        let from = *graph
            .node_statuses
            .get(node_id)
            .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.to_string()))?;
        let mut next = apply_node_transition(
            graph,
            graph.revision,
            node_id,
            ExecutionNodeStatus::Running,
            None,
        )?;
        *next
            .recovery_cursor
            .node_attempts
            .entry(node_id.to_string())
            .or_default() = binding.attempt;
        let graph_event = ExecutionGraphEvent::NodeTransitioned {
            node_id: node_id.to_string(),
            from,
            to: ExecutionNodeStatus::Running,
            result: None,
            binding: Some(binding),
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        let node_event =
            node_transition_event(&next, node_id, from, ExecutionNodeStatus::Running, None)?;
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:{}:{}:bound", graph.id, node_id, next.revision),
            graph_event,
            vec![node_event],
        )
    }

    pub async fn bind_and_start_node_async(
        &self,
        graph: ExecutionGraph,
        node_id: String,
        binding: ExecutionNodeBinding,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.bind_and_start_node(&graph, &node_id, binding))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    /// Durably starts one scheduler wave in a single graph revision.
    ///
    /// Resource leases and executor tickets are acquired before this boundary.
    /// Committing the whole independent wave atomically avoids turning the
    /// graph event stream into a per-node dispatch lock while preserving an
    /// exact recovery binding for every Running node.
    pub fn bind_and_start_nodes(
        &self,
        graph: &ExecutionGraph,
        bindings: BTreeMap<String, ExecutionNodeBinding>,
        effect_tickets: Vec<super::registry::NodeExecutionTicket>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        if bindings.is_empty() {
            return Err(ExecutionCommitError::InvalidCommand(
                "cannot start an empty execution wave".to_string(),
            ));
        }
        for (node_id, binding) in &bindings {
            binding
                .validate_start(graph, node_id)
                .map_err(ExecutionCommitError::InvalidCommand)?;
        }
        let mut effect_nodes = BTreeSet::new();
        let mut effect_keys = BTreeSet::new();
        for ticket in &effect_tickets {
            let binding = bindings.get(&ticket.node_id).ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("foreign execution wave effect ticket".into())
            })?;
            let node = graph
                .nodes
                .iter()
                .find(|node| node.id == ticket.node_id)
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "execution wave effect node missing".into(),
                    )
                })?;
            ExecutionNodeBinding::validate_ticket(ticket, graph, node, binding.attempt)
                .map_err(ExecutionCommitError::InvalidCommand)?;
            if binding.ticket_idempotency_key != ticket.idempotency_key
                || !effect_nodes.insert(ticket.node_id.clone())
                || !effect_keys.insert(ticket.idempotency_key.clone())
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "duplicate or mismatched execution wave effect ticket".into(),
                ));
            }
            if node.kind == ExecutionNodeKind::ToolBatch && node.executor_kind == "tool_batch" {
                return Err(ExecutionCommitError::InvalidCommand(
                    "ToolBatch leaf owner must not acquire a NodeEffect intent".into(),
                ));
            }
            self.validate_fresh_node_admission(graph, node, ticket)?;
        }
        for (node_id, binding) in &bindings {
            if effect_nodes.contains(node_id) {
                continue;
            }
            let node = graph
                .nodes
                .iter()
                .find(|node| &node.id == node_id)
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand("execution wave node missing".into())
                })?;
            let ticket = super::registry::NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: node_id.clone(),
                executor_kind: binding.executor_kind.clone(),
                service_class: graph.service_class,
                attempt: binding.attempt,
                idempotency_key: binding.ticket_idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            if node.kind == ExecutionNodeKind::ToolBatch && node.executor_kind == "tool_batch" {
                self.validate_fresh_node_admission(graph, node, &ticket)?;
                continue;
            }
            return Err(ExecutionCommitError::InvalidCommand(
                "execution wave requires atomic original intent admission".into(),
            ));
        }
        let mut next = graph.clone();
        let mut node_events = Vec::with_capacity(bindings.len());
        for (node_id, binding) in &bindings {
            let from = *next
                .node_statuses
                .get(node_id)
                .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.clone()))?;
            next = apply_node_transition(
                &next,
                next.revision,
                node_id,
                ExecutionNodeStatus::Running,
                None,
            )?;
            *next
                .recovery_cursor
                .node_attempts
                .entry(node_id.clone())
                .or_default() = binding.attempt;
            node_events.push((node_id.clone(), from));
        }
        // A wave is one atomic graph mutation even though the contract helper
        // validates each node transition independently above.
        next.revision = graph.revision.saturating_add(1);
        let mut domain_events = node_events
            .into_iter()
            .map(|(node_id, from)| {
                node_transition_event(&next, &node_id, from, ExecutionNodeStatus::Running, None)
            })
            .collect::<Result<Vec<_>, _>>()?;
        domain_events.extend(
            effect_tickets
                .iter()
                .map(execution_effect_intent_event)
                .collect::<Result<Vec<_>, _>>()?,
        );
        let graph_event = ExecutionGraphEvent::NodesStarted {
            bindings,
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:wave:{}:started", graph.id, next.revision),
            graph_event,
            domain_events,
        )
    }

    pub async fn bind_and_start_nodes_async(
        &self,
        graph: ExecutionGraph,
        bindings: BTreeMap<String, ExecutionNodeBinding>,
        effect_tickets: Vec<super::registry::NodeExecutionTicket>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.bind_and_start_nodes(&graph, bindings, effect_tickets)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    /// Commits a set of independently completed Running nodes in one graph
    /// revision together with their effect receipts and domain events.
    pub(crate) fn transition_nodes(
        &self,
        graph: &ExecutionGraph,
        transitions: Vec<ExecutionTerminalCommit>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        if transitions.is_empty() {
            return Err(ExecutionCommitError::InvalidCommand(
                "cannot commit an empty terminal wave".to_string(),
            ));
        }
        let mut source_revisions = BTreeMap::new();
        let mut no_dispatch_facts = BTreeMap::new();
        for transition in &transitions {
            validate_executor_domain_events(&transition.outcome.domain_events)?;
            merge_outcome_source_revisions(
                &mut source_revisions,
                &transition.outcome.source_revisions,
            )?;
            super::runner::validate_outcome(&transition.node_id, &transition.outcome)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            let spec = graph
                .nodes
                .iter()
                .find(|node| node.id == transition.node_id)
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "terminal wave original node missing".into(),
                    )
                })?;
            let binding = ExecutionGraphStateStore::new(self.event_store.clone())
                .current_binding(
                    graph,
                    &transition.node_id,
                    *graph
                        .recovery_cursor
                        .node_attempts
                        .get(&transition.node_id)
                        .ok_or_else(|| {
                            ExecutionCommitError::InvalidCommand(
                                "terminal wave original attempt missing".into(),
                            )
                        })?,
                )
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            let original = super::registry::NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: spec.id.clone(),
                executor_kind: binding.executor_kind.clone(),
                service_class: graph.service_class,
                attempt: binding.attempt,
                idempotency_key: binding.ticket_idempotency_key.clone(),
                payload_ref: spec.payload_ref.clone(),
            };
            validate_node_ticket_start(&self.event_store, &original, None)?;
            if let Some(witness) = &transition.outcome.no_dispatch {
                let fact = self.materialize_no_dispatch_fact(
                    graph,
                    &original,
                    &binding,
                    witness,
                    Some(&transition.outcome.result),
                )?;
                merge_outcome_source_revisions(&mut source_revisions, &fact.historical_sources)?;
                no_dispatch_facts.insert(transition.node_id.clone(), fact);
            }
            let leaf =
                spec.kind == ExecutionNodeKind::ToolBatch && spec.executor_kind == "tool_batch";
            if (!leaf && transition.effect_ticket.as_ref() != Some(&original))
                || (leaf && transition.effect_ticket.is_some())
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "terminal wave requires exact original NodeEffect owner".into(),
                ));
            }
            if let Some(ticket) = transition.effect_ticket.as_ref() {
                if ticket.graph_id != graph.id
                    || ticket.node_id != transition.node_id
                    || graph.recovery_cursor.node_attempts.get(&transition.node_id)
                        != Some(&ticket.attempt)
                {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "terminal wave ticket node mismatch".into(),
                    ));
                }
                self.validate_execution_effect_receipt(ticket)?;
            }
        }
        let mut next = graph.clone();
        let mut transition_facts = Vec::with_capacity(transitions.len());
        for transition in &transitions {
            if transition.outcome.replan.is_some()
                || transition.outcome.delivery_envelope.is_some()
                || transition.outcome.terminal_presentation.is_some()
            {
                return Err(ExecutionCommitError::InvalidCommand(format!(
                    "node `{}` carries a non-batchable terminal mutation",
                    transition.node_id
                )));
            }
            let from = *next.node_statuses.get(&transition.node_id).ok_or_else(|| {
                ExecutionTransitionError::NodeNotFound(transition.node_id.clone())
            })?;
            next = apply_node_transition(
                &next,
                next.revision,
                &transition.node_id,
                transition.outcome.result.status,
                Some(transition.outcome.result.clone()),
            )?;
            transition_facts.push((
                transition.node_id.clone(),
                from,
                transition.outcome.result.status,
            ));
        }
        next.revision = graph.revision.saturating_add(1);
        let mut domain_events = Vec::new();
        for (transition, (node_id, from, to)) in transitions.iter().zip(transition_facts.iter()) {
            let mut node_event = node_transition_event(
                &next,
                node_id,
                *from,
                *to,
                Some(transition.outcome.result.clone()),
            )?;
            if let Some(fact) = no_dispatch_facts.get(node_id) {
                node_event.event.payload["no_dispatch"] = serde_json::to_value(fact)?;
            }
            domain_events.push(node_event);
            domain_events.extend(transition.outcome.domain_events.clone());
            if let Some(candidate) = crate::knowledge_candidate_projector::team_terminal_candidate(
                &next,
                node_id,
                *to,
                next.node_results.get(node_id),
            ) {
                domain_events.push(
                    crate::knowledge_candidate_projector::candidate_proposal_event(candidate)
                        .map_err(ExecutionCommitError::InvalidCommand)?,
                );
            }
            if let Some(ticket) = &transition.effect_ticket {
                domain_events.push(execution_effect_receipt_event(ticket, &transition.outcome)?);
            }
        }
        let node_ids = transitions
            .iter()
            .map(|transition| transition.node_id.clone())
            .collect::<Vec<_>>();
        let graph_event = ExecutionGraphEvent::NodesTransitioned {
            node_ids,
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        self.append_graph_event_with_expected_domain_revisions(
            &next,
            graph.revision,
            format!("{}:wave:{}:terminal", graph.id, next.revision),
            graph_event,
            domain_events,
            &source_revisions,
        )
    }

    pub(crate) async fn transition_nodes_async(
        &self,
        graph: ExecutionGraph,
        transitions: Vec<ExecutionTerminalCommit>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.transition_nodes(&graph, transitions))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub async fn transition_node_async(
        &self,
        graph: ExecutionGraph,
        node_id: String,
        to: ExecutionNodeStatus,
        result: Option<ExecutionNodeResult>,
        domain_events: Vec<RuntimeTransactionEventInput>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.transition_node(&graph, &node_id, to, result, domain_events)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub fn transition_node_with_replan(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        result: ExecutionNodeResult,
        domain_events: Vec<RuntimeTransactionEventInput>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
        framework_authored: bool,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let prepared = self.prepare_transition_node_with_replan(
            graph,
            node_id,
            result,
            domain_events,
            nodes,
            edges,
            reason,
            framework_authored,
            &BTreeMap::new(),
            None,
        )?;
        self.commit_prepared_graph_append(prepared)
    }

    fn prepare_transition_node_with_replan(
        &self,
        graph: &ExecutionGraph,
        node_id: &str,
        result: ExecutionNodeResult,
        domain_events: Vec<RuntimeTransactionEventInput>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
        framework_authored: bool,
        expected_sources: &BTreeMap<String, u64>,
        presentation: Option<&super::registry::NodeExecutionOutcome>,
    ) -> Result<PreparedGraphAppend, ExecutionCommitError> {
        validate_executor_domain_events(&domain_events)?;
        validate_replan(graph, &nodes, framework_authored)?;
        let from = *graph
            .node_statuses
            .get(node_id)
            .ok_or_else(|| ExecutionTransitionError::NodeNotFound(node_id.to_string()))?;
        let to = result.status;
        let mut next =
            apply_node_transition(graph, graph.revision, node_id, to, Some(result.clone()))?;
        Self::apply_outcome_presentation(&mut next, presentation);
        let added_node_ids = nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>();
        for node in &nodes {
            next.node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        next.nodes.extend(nodes);
        next.edges.extend(edges);
        validate_execution_graph(&next)
            .map_err(|error| ExecutionCommitError::InvalidReplan(error.to_string()))?;
        let mut events = vec![node_transition_event(
            &next,
            node_id,
            from,
            to,
            Some(result.clone()),
        )?];
        events.extend(domain_events);
        if let Some(candidate) = crate::knowledge_candidate_projector::team_terminal_candidate(
            &next,
            node_id,
            to,
            next.node_results.get(node_id),
        ) {
            events.push(
                crate::knowledge_candidate_projector::candidate_proposal_event(candidate)
                    .map_err(ExecutionCommitError::InvalidCommand)?,
            );
        }
        self.prepare_graph_append(
            &next,
            graph.revision,
            format!("{}:{}:{}:terminal-replan", graph.id, node_id, next.revision),
            ExecutionGraphEvent::NodeTransitionedAndReplanned {
                node_id: node_id.to_string(),
                from,
                to,
                result,
                reason,
                added_node_ids,
                delta: ExecutionGraphDelta::between(graph, &next),
            },
            events,
            expected_sources,
        )
    }

    pub async fn transition_node_with_replan_async(
        &self,
        graph: ExecutionGraph,
        node_id: String,
        result: ExecutionNodeResult,
        domain_events: Vec<RuntimeTransactionEventInput>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
        framework_authored: bool,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.transition_node_with_replan(
                &graph,
                &node_id,
                result,
                domain_events,
                nodes,
                edges,
                reason,
                framework_authored,
            )
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub fn replan(
        &self,
        graph: &ExecutionGraph,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        validate_replan(graph, &nodes, false)?;
        let added_node_ids = nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>();
        let mut next = graph.clone();
        for node in &nodes {
            next.node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        next.nodes.extend(nodes);
        next.edges.extend(edges);
        next.revision = next.revision.saturating_add(1);
        validate_execution_graph(&next)
            .map_err(|error| ExecutionCommitError::InvalidReplan(error.to_string()))?;
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:replan:{}", graph.id, next.revision),
            ExecutionGraphEvent::Replanned {
                reason,
                added_node_ids,
                delta: ExecutionGraphDelta::between(graph, &next),
            },
            Vec::new(),
        )
    }

    pub async fn replan_async(
        &self,
        graph: ExecutionGraph,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.replan(&graph, nodes, edges, reason))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    /// Replace an admitted graph topology before any node starts.
    ///
    /// Strategy downgrade uses this boundary when the initially selected Team
    /// cannot start. Replacing the still-planned snapshot keeps the durable
    /// graph ID and Surface subscription stable while ensuring the executed
    /// topology matches the revised strategy compile target.
    pub fn retarget_planned_graph(
        &self,
        graph: &ExecutionGraph,
        mut replacement: ExecutionGraph,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        if graph.nodes.is_empty()
            || graph
                .node_statuses
                .values()
                .any(|status| *status != ExecutionNodeStatus::Planned)
            || !graph.node_results.is_empty()
            || replacement.nodes.is_empty()
            || replacement.id != graph.id
        {
            return Err(ExecutionCommitError::InvalidReplan(
                "strategy retarget requires the same non-empty graph id before any node starts"
                    .to_string(),
            ));
        }
        replacement.revision = graph.revision.saturating_add(1);
        replacement.recovery_cursor = graph.recovery_cursor.clone();
        replacement.node_results.clear();
        replacement.node_statuses.clear();
        for node in &replacement.nodes {
            replacement
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        }
        validate_execution_graph(&replacement)
            .map_err(|error| ExecutionCommitError::InvalidReplan(error.to_string()))?;
        let added_node_ids = replacement
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect();
        let replacement_snapshot = replacement.clone();
        self.append_graph_event(
            &replacement_snapshot,
            graph.revision,
            format!("{}:strategy-retarget:{}", graph.id, replacement.revision),
            ExecutionGraphEvent::Replanned {
                reason,
                added_node_ids,
                delta: ExecutionGraphDelta::between(graph, &replacement),
            },
            Vec::new(),
        )
    }

    pub async fn retarget_planned_graph_async(
        &self,
        graph: ExecutionGraph,
        replacement: ExecutionGraph,
        reason: String,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.retarget_planned_graph(&graph, replacement, reason)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    pub fn apply_command(
        &self,
        graph: &ExecutionGraph,
        command: &ExecutionGraphCommand,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let expected = command_revision(command);
        if expected != graph.revision {
            return Err(ExecutionCommitError::StaleRevision {
                graph_id: graph.id.clone(),
                expected,
                actual: graph.revision,
            });
        }
        let (command_name, reason) = command_metadata(command);
        let mut next = graph.clone();
        let mut external_resolution: Option<(String, String, String)> = None;
        match command {
            ExecutionGraphCommand::Start { .. } => {}
            ExecutionGraphCommand::Pause { .. } => {
                for status in next.node_statuses.values_mut() {
                    if matches!(
                        status,
                        ExecutionNodeStatus::Ready | ExecutionNodeStatus::Running
                    ) {
                        *status = ExecutionNodeStatus::Paused;
                    }
                }
            }
            ExecutionGraphCommand::Resume { .. } | ExecutionGraphCommand::Advance { .. } => {
                for status in next.node_statuses.values_mut() {
                    if *status == ExecutionNodeStatus::Paused {
                        *status = ExecutionNodeStatus::Ready;
                    }
                }
            }
            ExecutionGraphCommand::Cancel { .. } => {
                for status in next.node_statuses.values_mut() {
                    if !status.is_terminal() {
                        *status = ExecutionNodeStatus::Cancelled;
                    }
                }
            }
            ExecutionGraphCommand::CancelNode { node_id, .. } => {
                let status = next.node_statuses.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "cancel target node `{node_id}` does not exist"
                    ))
                })?;
                if !status.is_terminal() {
                    *status = ExecutionNodeStatus::Cancelled;
                }
            }
            ExecutionGraphCommand::SubmitApproval {
                node_id, decision, ..
            } => {
                let status = next.node_statuses.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "approval node `{node_id}` does not exist"
                    ))
                })?;
                if *status != ExecutionNodeStatus::WaitingApproval {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "node `{node_id}` is not waiting for approval"
                    )));
                }
                *status = if decision.approved {
                    ExecutionNodeStatus::Ready
                } else {
                    ExecutionNodeStatus::Cancelled
                };
            }
            ExecutionGraphCommand::ResolveExternal {
                node_id,
                result_ref,
                correlation_id,
                ..
            } => {
                let status = next.node_statuses.get_mut(node_id).ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "external result node `{node_id}` does not exist"
                    ))
                })?;
                if *status != ExecutionNodeStatus::WaitingExternal {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "node `{node_id}` is not waiting for an external result"
                    )));
                }
                // C4 hardening: the node must be owned by an authorized
                // external-wait executor and must carry the durable parking
                // result recorded when it entered WaitingExternal. This rejects
                // a malformed or forged resolve that targets a node which was
                // never properly parked (PLAN W1 / C4).
                let node_kind = next
                    .nodes
                    .iter()
                    .find(|node| node.id == *node_id)
                    .map(|node| node.executor_kind.as_str())
                    .unwrap_or("");
                let parked = next
                    .node_results
                    .get(node_id)
                    .and_then(|result| result.result_ref.clone());
                // Class A (barrier) is structurally authorised; anything else
                // must have declared an authoritative wait class (PLAN W1 / C4).
                let wait_class = next
                    .node_results
                    .get(node_id)
                    .and_then(|result| result.wait_class);
                if !harness_contract::execution_graph::WAITING_EXTERNAL_EXECUTOR_KINDS
                    .contains(&node_kind)
                    || correlation_id.trim().is_empty()
                    || parked.is_none()
                    || wait_class.is_none_or(|class| {
                        class != harness_contract::execution_graph::ExternalWaitClass::Barrier
                        && class
                            != harness_contract::execution_graph::ExternalWaitClass::Authoritative
                    })
                {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "node `{node_id}` is not a resolvable external wait"
                    )));
                }
                // Bind the resolution to the durable subject the wait parked on,
                // so a forged correlation cannot complete the node (PLAN W1 / C4).
                //
                // * Class A barrier: parks on
                //   `agentic-program-wait:{program}:revision:{rev}` and resolves
                //   with `agentic-program-wait:{program}:{node}:{rev}` — same
                //   program and revision.
                // * Class B authoritative: parks on
                //   `session_handoff_accepted:{subject}:{receipt}` and must be
                //   resolved with exactly that subject.
                // A Program id contains a colon, so the parked reference has five
                // parts; requiring four refused every release as unanchored.
                let anchored = match parked.as_deref() {
                    Some(parked_ref) if parked_ref.starts_with("agentic-program-wait:") => {
                        parked_ref
                            .rsplit_once(":revision:")
                            .is_some_and(|(subject, rev)| {
                                correlation_id.starts_with(&format!("{subject}:"))
                                    && correlation_id.ends_with(&format!(":{rev}"))
                            })
                    }
                    Some(parked_ref) => parked_ref
                        .starts_with(&format!("session_handoff_accepted:{correlation_id}:")),
                    None => false,
                };
                if !anchored {
                    return Err(ExecutionCommitError::InvalidCommand(format!(
                        "external-wait resolution correlation does not match the parked subject"
                    )));
                }
                *status = ExecutionNodeStatus::Completed;
                next.node_results.insert(
                    node_id.clone(),
                    ExecutionNodeResult {
                        status: ExecutionNodeStatus::Completed,
                        result_ref: Some(result_ref.clone()),
                        wait_class: None,
                        summary: None,
                        evidence_refs: Vec::new(),
                        failure: None,
                        usage: Default::default(),
                        finished_at_ms: now_ms(),
                    },
                );
                external_resolution =
                    Some((node_id.clone(), result_ref.clone(), correlation_id.clone()));
            }
            ExecutionGraphCommand::Replan { .. } => {
                return Err(ExecutionCommitError::InvalidCommand(
                    "replan requires the graph compiler and cannot be applied as a status mutation"
                        .to_string(),
                ));
            }
        }
        next.revision = next.revision.saturating_add(1);
        validate_execution_graph(&next).map_err(|error| {
            ExecutionCommitError::InvalidCommand(format!(
                "graph command violates execution invariants: {error}"
            ))
        })?;
        let mut expected_domain_revisions = BTreeMap::new();
        let mut node_events: Vec<RuntimeTransactionEventInput> = next
            .node_statuses
            .iter()
            .filter_map(|(node_id, to)| {
                let from = graph.node_statuses[node_id];
                (from != *to).then(|| node_transition_event(&next, node_id, from, *to, None))
            })
            .collect::<Result<_, _>>()?;
        if let Some((node_id, result_ref, correlation_id)) = external_resolution {
            node_events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("session-handoff-correlation:{correlation_id}"),
                    scope: RuntimeEventScope::SessionInput,
                    kind: "session.handoff.source_resolved.v1".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("ExecutionGraphRunner".to_string()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: graph.id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "execution_node".to_string(),
                            id: node_id.clone(),
                        },
                    ],
                    payload: json!({
                        "result_ref": result_ref,
                        "correlation_id": correlation_id,
                    }),
                },
                idempotency_key: Some(format!(
                    "session-handoff-source-resolved:{}:{node_id}:{correlation_id}",
                    graph.id,
                )),
                schema_version: 1,
            });
        }
        if let ExecutionGraphCommand::SubmitApproval {
            node_id, decision, ..
        } = command
        {
            let expected_approval_id = super::executors::graph_approval_id(&graph.id, node_id);
            if decision.approval_id != expected_approval_id {
                return Err(ExecutionCommitError::InvalidCommand(format!(
                    "approval decision `{}` does not own graph node `{}`",
                    decision.approval_id, node_id
                )));
            }
            let prepared = crate::approval_queue::canonical_graph_decision_events(
                &self.event_store,
                decision,
                now_ms(),
            )
            .map_err(ExecutionCommitError::InvalidCommand)?;
            expected_domain_revisions.insert(
                prepared.stream_id.clone(),
                prepared.expected_stream_revision,
            );
            node_events.extend(prepared.events);
        }
        let event = ExecutionGraphEvent::CommandApplied {
            command: command_name.to_string(),
            reason: reason.map(str::to_string),
            delta: ExecutionGraphDelta::between(graph, &next),
        };
        self.append_graph_event_with_expected_domain_revisions(
            &next,
            graph.revision,
            format!("{}:command:{}:{}", graph.id, command_name, next.revision),
            event,
            node_events,
            &expected_domain_revisions,
        )
    }

    pub(crate) fn validate_command_revision(
        &self,
        graph: &ExecutionGraph,
        command: &ExecutionGraphCommand,
    ) -> Result<(), ExecutionCommitError> {
        let expected = command_revision(command);
        if expected != graph.revision {
            return Err(ExecutionCommitError::StaleRevision {
                graph_id: graph.id.clone(),
                expected,
                actual: graph.revision,
            });
        }
        Ok(())
    }

    pub async fn apply_command_async(
        &self,
        graph: ExecutionGraph,
        command: ExecutionGraphCommand,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.apply_command(&graph, &command))
            .await
            .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    fn lease_recovery_process_closed(
        outcome: &crate::RuntimeToolExecutionOutcome,
        effect: &ToolEffectDescriptor,
    ) -> bool {
        use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionTerminal};
        if outcome.observed_evidence.iter().any(|observed| matches!(
            observed.target, EvidenceTargetIdentity::LocalNetwork { .. }
        )) {
            return Self::local_network_closed(outcome, effect);
        }
        let facts = outcome
            .observed_evidence
            .iter()
            .filter_map(|evidence| match &evidence.target {
                EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
                _ => None,
            })
            .collect::<Vec<_>>();
        if facts.is_empty() {
            return Self::local_network_closed(outcome, effect);
        }
        facts.len() == 1
            && match &facts[0].terminal {
                ToolExecutionTerminal::NotStarted { reason_code } => !reason_code.is_empty(),
                ToolExecutionTerminal::Settled {
                    exit_code,
                    signal,
                    closure,
                    ..
                } => {
                    // Physical closure is *quiescence*: the process tree this
                    // node owns was reaped and nothing of it is still alive.
                    // It is not a security posture. The hardening, network
                    // isolation and status-channel flags stay in the evidence
                    // for audit, but they are enforced where the evidence is
                    // produced (an unhardened process never reaches a
                    // `Settled` terminal in the first place), so requiring
                    // them here would only forbid honest host-shell work
                    // from ever closing.
                    exit_code.is_some() != signal.is_some()
                        && closure.all_children_reaped
                        && closure.init_reaped
                }
                _ => false,
            }
    }

    fn no_dispatch_physical_sources(
        &self,
        fact: &super::executors::agent_tool::NoDispatchFact,
    ) -> Result<Option<BTreeMap<String, u64>>, ExecutionCommitError> {
        let mut sources = BTreeMap::new();
        for call in &fact.calls {
            if let Some(key) = &call.effect_key {
                let stream = format!("execution-effect:{key}");
                let head = self.event_store.stream_revision(&stream)?;
                if head != 0 {
                    return Ok(None);
                }
                sources.insert(stream, head);
            }
        }
        let ticket = &fact.ticket;
        let index = if super::executors::agent_tool::is_bound_ticket(ticket) {
            let bound = super::executors::agent_tool::decode_bound_agent_tool(&ticket.payload_ref)?;
            let parent = bound.request.parent_execution.as_ref().ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("NoDispatch parent missing".into())
            })?;
            format!(
                "execution-agent-receipts:{}:{}:{}",
                parent.execution_id,
                parent.node_id,
                bound.request.parent_execution_attempt.ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand("NoDispatch parent attempt missing".into())
                })?
            )
        } else {
            format!(
                "execution-agent-receipts:{}:{}:{}",
                ticket.graph_id, ticket.node_id, ticket.attempt
            )
        };
        sources.insert(index.clone(), self.event_store.stream_revision(&index)?);
        Ok(Some(sources))
    }

    fn materialize_no_dispatch_fact(
        &self,
        graph: &ExecutionGraph,
        ticket: &super::registry::NodeExecutionTicket,
        binding: &ExecutionNodeBinding,
        witness: &super::executors::agent_tool::NoDispatchWitness,
        result: Option<&ExecutionNodeResult>,
    ) -> Result<super::executors::agent_tool::NoDispatchFact, ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        witness.authenticate(ticket, result).map_err(invalid)?;
        validate_node_ticket_start(&self.event_store, ticket, None)?;
        let state = ExecutionGraphStateStore::new(self.event_store.clone());
        let (original, spec) = state
            .binding_start_ticket(graph, &ticket.node_id, binding)
            .map_err(|error| invalid(error.to_string()))?;
        if original != *ticket
            || spec.kind != ExecutionNodeKind::ToolBatch
            || spec.executor_kind != "tool_batch"
        {
            return Err(invalid("NoDispatch original start mismatch".into()));
        }
        let mut fact = witness.fact().clone();
        fact.start = Some(
            state
                .binding_start_position(graph, &ticket.node_id, binding)
                .map_err(|error| invalid(error.to_string()))?
                .ok_or_else(|| invalid("NoDispatch exact start missing".into()))?,
        );
        fact.historical_sources = self
            .no_dispatch_physical_sources(&fact)?
            .ok_or_else(|| invalid("NoDispatch contradicts original primary history".into()))?;
        let leaf = self
            .lease_recovery_leaf_sources_with_fact(
                graph,
                &ticket.node_id,
                binding,
                &spec,
                false,
                Some(&fact),
            )?
            .ok_or_else(|| {
                invalid("NoDispatch has incomplete or contradictory original leaf history".into())
            })?;
        let current = self
            .no_dispatch_physical_sources(&fact)?
            .ok_or_else(|| invalid("NoDispatch original physical history changed".into()))?;
        if current != fact.historical_sources
            || leaf.iter().any(|source| {
                self.event_store.stream_revision(&source.stream_id).ok()
                    != Some(source.expected_revision)
            })
        {
            return Err(invalid(
                "NoDispatch physical sources changed during materialization".into(),
            ));
        }
        Ok(fact)
    }

    pub(crate) fn validate_no_dispatch_source(
        graph: &ExecutionGraph,
        ticket: &super::registry::NodeExecutionTicket,
        binding: &ExecutionNodeBinding,
        start: (u64, u64, u32),
        source: &crate::DurableRuntimeEvent,
        reader: &mut impl TerminalScopeReader,
    ) -> Result<Option<super::executors::agent_tool::NoDispatchFact>, ExecutionCommitError> {
        let invalid = ExecutionCommitError::InvalidCommand;
        let fact = if source.stream_id == graph.id {
            let Some(closure) = super::events::ExecutionBindingLeaseClosure::from_record(
                source,
                graph,
                &ticket.node_id,
                binding,
            )
            .map_err(invalid)?
            else {
                return Ok(None);
            };
            if !closure.matches(graph, &ticket.node_id, binding, start) {
                return Err(invalid("NoDispatch closure start mismatch".into()));
            }
            closure.no_dispatch
        } else {
            source
                .payload
                .get("no_dispatch")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?
        };
        let Some(fact): Option<super::executors::agent_tool::NoDispatchFact> = fact else {
            return Ok(None);
        };
        fact.validate_identity(ticket).map_err(invalid)?;
        if fact.start != Some(start)
            || ticket.attempt != binding.attempt
            || ticket.idempotency_key != binding.ticket_idempotency_key
            || ticket.executor_kind != binding.executor_kind
        {
            return Err(invalid(
                "NoDispatch binding or historical start mismatch".into(),
            ));
        }
        let node_stream = node_stream_id(&graph.id, &ticket.node_id);
        let original = reader
            .event(
                &node_stream,
                &format!("{}:{}:{}", graph.id, ticket.node_id, start.0),
            )?
            .ok_or_else(|| invalid("NoDispatch original start node missing".into()))?;
        let root = reader
            .event(&graph.id, &format!("{}:revision:{}", graph.id, start.0))?
            .ok_or_else(|| invalid("NoDispatch original start Graph missing".into()))?;
        if original.kind != "execution_node.transitioned"
            || original.actor.as_deref() != Some("execution_commit_service")
            || original.scope != RuntimeEventScope::ExecutionNode
            || original.schema_version != 1
            || original.payload["to"] != "running"
            || original.payload["graph_id"] != graph.id
            || original.payload["node_id"] != ticket.node_id
            || original.payload["graph_revision"] != start.0
            || original.commit_cursor != start.1
            || root.transaction_index != start.2
            || root.sequence != start.0
            || root.commit_cursor != start.1
            || root.transaction_id != original.transaction_id
            || root.transaction_index >= original.transaction_index
            || root.actor.as_deref() != Some("execution_commit_service")
            || root.scope != RuntimeEventScope::ExecutionGraph
            || root.schema_version != 2
        {
            return Err(invalid(
                "NoDispatch original start provenance mismatch".into(),
            ));
        }
        let root_binding = root
            .activity_binding()
            .ok_or_else(|| invalid("NoDispatch start activity missing".into()))?;
        let original_binding = original
            .activity_binding()
            .ok_or_else(|| invalid("NoDispatch start node activity missing".into()))?;
        if root_binding.root_execution_id != graph.id
            || root_binding.session_id != original_binding.session_id
            || root_binding.turn_id != original_binding.turn_id
            || root_binding.task_id != original_binding.task_id
            || root_binding.root_task_id != original_binding.root_task_id
            || root_binding.generation != original_binding.generation
            || original_binding.node_id.as_deref() != Some(ticket.node_id.as_str())
            || original_binding.root_execution_id != graph.id
        {
            return Err(invalid("NoDispatch original start lineage mismatch".into()));
        }
        let source_binding = source
            .activity_binding()
            .ok_or_else(|| invalid("NoDispatch source activity missing".into()))?;
        let mut expected_binding = if source.stream_id == graph.id {
            root_binding.clone()
        } else {
            original_binding.clone()
        };
        expected_binding.revision = source_binding.revision;
        expected_binding.fence = source_binding.fence;
        if expected_binding != source_binding
            || (source.commit_cursor, source.transaction_index) <= (start.1, start.2)
        {
            return Err(invalid(
                "NoDispatch source crossed original activity lineage".into(),
            ));
        }
        if source.stream_id == node_stream {
            let revision = source.payload["graph_revision"]
                .as_u64()
                .ok_or_else(|| invalid("NoDispatch result Graph revision missing".into()))?;
            let graph_source = reader
                .event(&graph.id, &format!("{}:revision:{revision}", graph.id))?
                .ok_or_else(|| invalid("NoDispatch result Graph source missing".into()))?;
            if source.kind != "execution_node.transitioned"
                || source.actor.as_deref() != Some("execution_commit_service")
                || source.schema_version != 1
                || source.scope != RuntimeEventScope::ExecutionNode
                || source.idempotency_key.as_deref()
                    != Some(format!("{}:{}:{revision}", graph.id, ticket.node_id).as_str())
                || source.payload["graph_id"] != graph.id
                || source.payload["node_id"] != ticket.node_id
                || graph_source.transaction_id != source.transaction_id
                || graph_source.commit_cursor != source.commit_cursor
                || graph_source.sequence != revision
                || graph_source.transaction_index >= source.transaction_index
                || graph_source.actor.as_deref() != Some("execution_commit_service")
                || graph_source.scope != RuntimeEventScope::ExecutionGraph
                || graph_source.schema_version != 2
                || fact.never_polled
            {
                return Err(invalid(
                    "NoDispatch result is not original atomic Graph result".into(),
                ));
            }
            let result: ExecutionNodeResult =
                serde_json::from_value(source.payload["result"].clone())?;
            let digest = format!("sha256:{:x}", Sha256::digest(serde_json::to_vec(&result)?));
            if fact.result_sha256.as_deref() != Some(digest.as_str()) {
                return Err(invalid("NoDispatch result digest mismatch".into()));
            }
            let mut expected_root = root_binding.clone();
            let current_root = graph_source
                .activity_binding()
                .ok_or_else(|| invalid("NoDispatch result root activity missing".into()))?;
            expected_root.revision = current_root.revision;
            expected_root.fence = current_root.fence;
            if expected_root != current_root {
                return Err(invalid(
                    "NoDispatch result root crossed original lineage".into(),
                ));
            }
        }
        let index_stream = fact.index_stream().map_err(invalid)?;
        let mut physical_streams = BTreeSet::from([index_stream]);
        physical_streams.extend(
            fact.calls
                .iter()
                .filter_map(|call| call.effect_key.as_ref())
                .map(|key| format!("execution-effect:{key}")),
        );
        if fact
            .historical_sources
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != physical_streams
            || fact
                .calls
                .iter()
                .filter_map(|call| call.effect_key.as_ref())
                .any(|key| {
                    fact.historical_sources
                        .get(&format!("execution-effect:{key}"))
                        != Some(&0)
                })
        {
            return Err(invalid(
                "NoDispatch historical physical cut set mismatch".into(),
            ));
        }
        for (stream, revision) in &fact.historical_sources {
            let fence = reader
                .fence(&source.transaction_id, stream)?
                .ok_or_else(|| {
                    invalid("NoDispatch historical source transaction fence missing".into())
                })?;
            if fence.stream_id != *stream
                || fence.expected_revision != *revision
                || fence.committed_revision != *revision
            {
                return Err(invalid(
                    "NoDispatch historical source transaction fence mismatch".into(),
                ));
            }
        }
        Ok(Some(fact))
    }

    pub(crate) fn validate_no_dispatch_closure_source(
        graph: &ExecutionGraph,
        ticket: &super::registry::NodeExecutionTicket,
        binding: &ExecutionNodeBinding,
        start: (u64, u32),
        source: &crate::DurableRuntimeEvent,
        reader: &mut impl TerminalScopeReader,
    ) -> Result<Option<super::executors::agent_tool::NoDispatchFact>, ExecutionCommitError> {
        let closure = super::events::ExecutionBindingLeaseClosure::from_record(
            source,
            graph,
            &ticket.node_id,
            binding,
        )
        .map_err(ExecutionCommitError::InvalidCommand)?;
        let Some(closure) = closure else {
            return Ok(None);
        };
        Self::validate_no_dispatch_source(
            graph,
            ticket,
            binding,
            (closure.start_sequence, start.0, start.1),
            source,
            reader,
        )
    }

    pub(crate) fn authenticated_no_dispatch_sources(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<Option<Vec<ExpectedStreamRevision>>, ExecutionCommitError> {
        if self
            .canonical_no_dispatch_fact(graph, node, binding)?
            .is_none()
        {
            return Ok(None);
        }
        let state = ExecutionGraphStateStore::new(self.event_store.clone());
        let position = state
            .binding_start_position(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("NoDispatch exact start missing".into())
            })?;
        if !state
            .binding_lease_closure(graph, node, binding, position)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "NoDispatch callback or original lease closure missing".into(),
            ));
        }
        self.binding_physical_sources(graph, node, binding)?
            .map(Some)
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(
                    "NoDispatch current physical sources are incomplete".into(),
                )
            })
    }

    fn canonical_no_dispatch_fact(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<Option<super::executors::agent_tool::NoDispatchFact>, ExecutionCommitError> {
        let state = ExecutionGraphStateStore::new(self.event_store.clone());
        let (ticket, _) = state
            .binding_start_ticket(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let position = state
            .binding_start_position(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand("NoDispatch binding start missing".into())
            })?;
        let mut reader = StoredTerminalScopeReader(&self.event_store);
        if let Some((result, graph_source)) = self.committed_node_attempt(graph, node, binding)? {
            let source = reader
                .event(
                    &node_stream_id(&graph.id, node),
                    &format!("{}:{}:{}", graph.id, node, graph_source.sequence),
                )?
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "NoDispatch original result node missing".into(),
                    )
                })?;
            if source.transaction_id != graph_source.transaction_id
                || source.commit_cursor != graph_source.commit_cursor
                || source.transaction_index <= graph_source.transaction_index
                || source.scope != RuntimeEventScope::ExecutionNode
                || source.schema_version != 1
                || source.kind != "execution_node.transitioned"
                || source.actor.as_deref() != Some("execution_commit_service")
                || source.payload["graph_id"] != graph.id
                || source.payload["node_id"] != node
                || source.payload["graph_revision"] != graph_source.sequence
                || source.payload["result"] != serde_json::to_value(&result)?
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "NoDispatch original node and Graph result provenance mismatch".into(),
                ));
            }
            if let Some(fact) = Self::validate_no_dispatch_source(
                graph,
                &ticket,
                binding,
                position,
                &source,
                &mut reader,
            )? {
                return Ok(Some(fact));
            }
        }
        let key = binding.lease_closure_key(graph, node);
        if let Some(source) = self.event_store.event_by_idempotency_key(&graph.id, &key)? {
            let source = self
                .event_store
                .resolve_read_record(source)
                .map_err(ExecutionCommitError::InvalidCommand)?;
            return Self::validate_no_dispatch_source(
                graph,
                &ticket,
                binding,
                position,
                &source,
                &mut reader,
            );
        }
        Ok(None)
    }

    fn delegated_host_leaf_owner(
        &self,
        graph: &ExecutionGraph,
        spec: &ExecutionNodeSpec,
    ) -> Result<
        Option<(
            String,
            String,
            u32,
            BTreeSet<String>,
            Vec<ExpectedStreamRevision>,
        )>,
        ExecutionCommitError,
    > {
        let invalid = |reason: &str| ExecutionCommitError::InvalidCommand(reason.into());
        if spec.kind != ExecutionNodeKind::ToolBatch
            || spec.executor_kind != "tool_batch"
            || super::executors::agent_tool::is_bound_agent_tool_graph(graph)
        {
            return Ok(None);
        }
        let Some(parent) = graph.parent_execution.as_ref() else {
            return Ok(None);
        };
        let (calls, _) = crate::host::decode_tool_batch(&spec.payload_ref)?;
        if calls.is_empty() {
            return Ok(None);
        }
        let state = ExecutionGraphStateStore::new(self.event_store.clone());
        let graph_head = self.event_store.stream_revision(&parent.execution_id)?;
        let parent_graph = state
            .load(&parent.execution_id)
            .map_err(|error| invalid(&error.to_string()))?;
        let parent_node = parent_graph
            .nodes
            .iter()
            .find(|node| node.id == parent.node_id)
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .ok_or_else(|| invalid("delegated Host original AgentTask parent missing"))?;
        let packet: AgentTaskPacket = serde_json::from_str(&parent_node.payload_ref)?;
        let parent_stream = node_stream_id(&parent.execution_id, &parent.node_id);
        let node_head = self.event_store.stream_revision(&parent_stream)?;
        let binding = state
            .current_binding(&parent_graph, &parent.node_id, packet.attempt)
            .map_err(|error| invalid(&error.to_string()))?;
        let (ticket, original) = state
            .binding_start_ticket(&parent_graph, &parent.node_id, &binding)
            .map_err(|error| invalid(&error.to_string()))?;
        let original_packet: AgentTaskPacket = serde_json::from_str(&original.payload_ref)?;
        let parent_start = state
            .binding_start_position(&parent_graph, &parent.node_id, &binding)
            .map_err(|error| invalid(&error.to_string()))?
            .ok_or_else(|| invalid("delegated Host parent original start missing"))?;
        let creation = self
            .event_store
            .list_stream_after(&graph.id, 0, 1, 1, 1024 * 1024)
            .map_err(ExecutionCommitError::InvalidCommand)?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("delegated Host original Graph registration missing"))?;
        if creation.stream_id != graph.id
            || creation.sequence != 1
            || creation.commit_cursor <= parent_start.1
        {
            return Err(invalid("delegated Host cannot borrow a later parent attempt"));
        }
        if packet != original_packet
            || ticket.payload_ref != original.payload_ref
            || ticket.graph_id != parent.execution_id
            || ticket.node_id != parent.node_id
            || ticket.attempt != packet.attempt
            || original.kind != ExecutionNodeKind::AgentTask
            || ticket.executor_kind != "agent_task"
            || packet.graph_id() != parent.execution_id
            || packet.node_id() != parent.node_id
            || packet.attempt == 0
            || parent_graph.lineage.is_none()
            || parent_graph.lineage != graph.lineage
        {
            return Err(invalid("delegated Host original AgentTask start mismatch"));
        }
        validate_node_ticket_start(&self.event_store, &ticket, None)?;
        let lineage = graph.lineage.as_ref().unwrap();
        let payload = crate::step_snapshot::decode_persisted_tool_batch_value(&spec.payload_ref)?;
        if payload["session_id"] != packet.session_id() {
            return Err(invalid("delegated Host batch Session mismatch"));
        }
        let mut keys = BTreeSet::new();
        for call in &calls {
            let step = call
                .binding
                .as_ref()
                .ok_or_else(|| invalid("delegated Host original Step missing"))?;
            let identity = &step.snapshot.identity;
            let expected_actor = match &step.origin.source {
                harness_contract::step::CallSource::Provider { attempt_id }
                    if !attempt_id.is_empty()
                        && attempt_id == &step.snapshot.provider_attempt.attempt_id =>
                {
                    packet.assignment.execution_identity.principal_id()
                }
                harness_contract::step::CallSource::Process {
                    worker_id,
                    sequence,
                } if worker_id == packet.run_id()
                    && *sequence > 0
                    && step.snapshot.related_revisions.get("worker_run") == Some(worker_id) =>
                {
                    packet.agent_id()
                }
                _ => {
                    return Err(invalid(
                        "delegated Host original Step producer source mismatch",
                    ))
                }
            };
            if identity.session_id != packet.session_id()
                || lineage.session_id != packet.session_id()
                || identity.turn_id != lineage.turn_id
                || !matches!(identity.execution_id.as_deref(), Some(execution) if execution == packet.run_id() || execution == packet.graph_id())
                || identity.run_ref.as_deref() != Some(packet.run_id())
                || identity.actor_ref.as_deref() != Some(expected_actor)
                || identity
                    .attempt
                    .is_some_and(|attempt| attempt != packet.attempt)
                || step.origin.call_id != call.id
                || step.origin.origin_step_id != identity.step_id
                || step.origin.catalog_digest != step.snapshot.catalog_digest
                || (call.name != "tool_search" && !packet.allowed_tools.contains(&call.name))
                || !keys.insert(step.effect_idempotency_key())
            {
                return Err(invalid(
                    "delegated Host Step crosses original AgentTask packet",
                ));
            }
        }
        let sources = vec![
            ExpectedStreamRevision {
                stream_id: parent.execution_id.clone(),
                expected_revision: graph_head,
            },
            ExpectedStreamRevision {
                stream_id: parent_stream,
                expected_revision: node_head,
            },
        ];
        if sources.iter().any(|source| {
            self.event_store.stream_revision(&source.stream_id).ok()
                != Some(source.expected_revision)
        }) {
            return Err(invalid(
                "delegated Host parent sources changed during authentication",
            ));
        }
        Ok(Some((
            parent.execution_id.clone(),
            parent.node_id.clone(),
            packet.attempt,
            keys,
            sources,
        )))
    }

    fn lease_recovery_leaf_sources(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
        spec: &ExecutionNodeSpec,
        retired: bool,
    ) -> Result<Option<Vec<ExpectedStreamRevision>>, ExecutionCommitError> {
        let fact = self.canonical_no_dispatch_fact(graph, node, binding)?;
        self.lease_recovery_leaf_sources_with_fact(
            graph,
            node,
            binding,
            spec,
            retired,
            fact.as_ref(),
        )
    }

    fn lease_recovery_leaf_sources_with_fact(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
        spec: &ExecutionNodeSpec,
        retired: bool,
        no_dispatch: Option<&super::executors::agent_tool::NoDispatchFact>,
    ) -> Result<Option<Vec<ExpectedStreamRevision>>, ExecutionCommitError> {
        let invalid = |reason: &str| ExecutionCommitError::InvalidCommand(reason.into());
        let bound: Option<crate::RuntimeToolExecutionRequest> = if spec.kind
            == ExecutionNodeKind::ToolBatch
            && super::executors::agent_tool::is_bound_agent_tool_graph(graph)
        {
            Some(
                super::executors::agent_tool::decode_bound_agent_tool_node(&graph.id, spec)?
                    .request,
            )
        } else {
            None
        };
        let delegated = self.delegated_host_leaf_owner(graph, spec)?;
        let (index_graph, index_node, index_attempt) = if let Some(request) = &bound {
            let parent = request
                .parent_execution
                .as_ref()
                .ok_or_else(|| invalid("bound ToolBatch parent missing"))?;
            (
                parent.execution_id.as_str(),
                parent.node_id.as_str(),
                request
                    .parent_execution_attempt
                    .ok_or_else(|| invalid("bound ToolBatch parent attempt missing"))?,
            )
        } else if let Some((owner_graph, owner_node, attempt, _, _)) = &delegated {
            (owner_graph.as_str(), owner_node.as_str(), *attempt)
        } else {
            (graph.id.as_str(), node, binding.attempt)
        };
        let stream = format!("execution-agent-receipts:{index_graph}:{index_node}:{index_attempt}");
        let head = self.event_store.stream_revision(&stream)?;
        let mut after = 0;
        let mut intents = BTreeSet::new();
        let mut receipts = BTreeSet::new();
        let mut calls = BTreeMap::new();
        let mut primary_sources = BTreeMap::new();
        while after < head {
            let page = self
                .event_store
                .list_stream_after(&stream, after, head, 100, 1024 * 1024)
                .map_err(ExecutionCommitError::InvalidCommand)?;
            if page.is_empty() {
                return Err(invalid("lease recovery effect-index gap"));
            }
            for index in page {
                if index.stream_id != stream
                    || index.sequence != after + 1
                    || index.sequence > head
                    || (index.kind == "execution.agent_tool.receipt"
                        && Self::receipt_observation_sequence(&index).is_none())
                    || (index.kind != "execution.agent_tool.receipt" && index.schema_version != 1)
                    || index.scope != RuntimeEventScope::ExecutionNode
                    || index.actor.as_deref() != Some("governed_tool")
                {
                    return Err(invalid("lease recovery effect-index identity mismatch"));
                }
                after = index.sequence;
                let key = index.payload["idempotency_key"]
                    .as_str()
                    .ok_or_else(|| invalid("lease recovery effect key missing"))?;
                if bound
                    .as_ref()
                    .is_some_and(|request| request.idempotency_key != key)
                    || delegated
                        .as_ref()
                        .is_some_and(|(_, _, _, keys, _)| !keys.contains(key))
                {
                    continue;
                }
                if index.kind == "execution.agent_tool.intent" {
                    if index.idempotency_key.as_deref()
                        != Some(format!("agent-tool-intent:{key}").as_str())
                    {
                        return Err(invalid("lease recovery effect intent key mismatch"));
                    }
                    intents.insert(key.to_string());
                    continue;
                }
                if index.kind != "execution.agent_tool.receipt"
                    || index.idempotency_key.as_deref()
                        != Some(format!("agent-tool-receipt:{key}").as_str())
                {
                    return Err(invalid("lease recovery effect terminal key mismatch"));
                }
                let primary_stream = format!("execution-effect:{key}");
                let primary_head = self.event_store.stream_revision(&primary_stream)?;
                let primary = self
                    .event_store
                    .event_by_idempotency_key(&primary_stream, &format!("{key}:receipt"))?
                    .or(self.event_store.event_by_idempotency_key(
                        &primary_stream,
                        &format!("{key}:read-receipt"),
                    )?);
                let Some(primary) = primary else {
                    return Ok(None);
                };
                let read = primary.kind == "execution.read.receipt";
                if (!read && primary.kind != "execution.effect.receipt")
                    || primary.stream_id != primary_stream
                    || primary.scope != RuntimeEventScope::ExecutionNode
                    || primary.schema_version != 1
                    || primary.payload["idempotency_key"] != key
                    || primary.transaction_id != index.transaction_id
                    || primary.commit_cursor != index.commit_cursor
                    || primary.transaction_index >= index.transaction_index
                    || primary.payload["outcome"] != index.payload["outcome"]
                {
                    return Err(invalid("lease recovery original primary/index conflict"));
                }
                let replay = &primary.payload["replay"];
                let root = &primary.payload["root_effect_owner"];
                let (owner_graph, owner_node, attempt) = if root.is_null() {
                    (
                        replay["parent_execution"]["execution_id"].as_str(),
                        replay["parent_execution"]["node_id"].as_str(),
                        primary
                            .refs
                            .iter()
                            .find(|reference| reference.kind == "agent_attempt")
                            .and_then(|reference| reference.id.parse::<u32>().ok()),
                    )
                } else {
                    (
                        root["binding"]["root_execution_id"].as_str(),
                        root["binding"]["node_id"].as_str(),
                        root["attempt"]
                            .as_u64()
                            .and_then(|attempt| u32::try_from(attempt).ok()),
                    )
                };
                if owner_graph != Some(index_graph)
                    || owner_node != Some(index_node)
                    || attempt != Some(index_attempt)
                {
                    return Err(invalid("lease recovery original primary owner mismatch"));
                }
                let effect: ToolEffectDescriptor =
                    serde_json::from_value(primary.payload["effect"].clone())?;
                let outcome: crate::RuntimeToolExecutionOutcome =
                    serde_json::from_value(primary.payload["outcome"].clone())?;
                if let Some(issuer_source) = self.validate_stored_local_network_receipt(&primary, &effect)? {
                    merge_outcome_source_revisions(&mut primary_sources, &BTreeMap::from([(issuer_source.stream_id, issuer_source.expected_revision)]))?;
                }
                if let Some(request) = bound.as_ref().filter(|request| {
                    read && request.idempotency_key == key
                        && primary.payload["resolved_effect_scope"].is_null()
                }) {
                    if super::executors::agent_tool::agent_read_terminal_from_store(
                        &self.event_store,
                        self,
                        request,
                        &effect,
                    )
                    .map_err(|error| invalid(&error.to_string()))?
                    .is_none()
                    {
                        return Ok(None);
                    }
                } else {
                    self.validate_stored_terminal_scope(&effect, &primary, read)?;
                    if let Some(request) = bound
                        .as_ref()
                        .filter(|request| request.idempotency_key == key)
                    {
                        self.validate_terminal_scope(request, &effect, &primary, read)?;
                    }
                }
                if delegated.is_some() {
                    primary_sources.insert(primary_stream, primary_head);
                }
                if !Self::lease_recovery_process_closed(&outcome, &effect) {
                    return Ok(None);
                }
                if !receipts.insert(key.to_string()) {
                    return Err(invalid("duplicate lease recovery primary"));
                }
                let call = replay["call_id"]
                    .as_str()
                    .ok_or_else(|| invalid("lease recovery original call missing"))?;
                calls.insert(
                    call.to_string(),
                    (
                        primary.payload["tool_name"].clone(),
                        primary.payload["input_sha256"].clone(),
                        key.to_string(),
                        replay["origin"].clone(),
                    ),
                );
            }
        }
        if !intents.is_subset(&receipts) {
            return Ok(None);
        }
        if let Some(fact) = no_dispatch {
            fact.validate_identity(&fact.ticket)
                .map_err(ExecutionCommitError::InvalidCommand)?;
            if self.no_dispatch_physical_sources(fact)?.is_none() {
                return Ok(None);
            }
            if fact
                .calls
                .iter()
                .any(|call| calls.contains_key(&call.call_id))
            {
                return Err(invalid("NoDispatch conflicts with an actual leaf receipt"));
            }
        }
        if spec.kind == ExecutionNodeKind::ToolBatch && spec.executor_kind == "tool_batch" {
            if let Some(request) = &bound {
                let expected = json!(format!(
                    "sha256:{:x}",
                    Sha256::digest(request.input.as_bytes())
                ));
                if !no_dispatch.is_some_and(|fact| {
                    fact.calls.iter().any(|call| {
                        call.effect_key.as_deref() == Some(request.idempotency_key.as_str())
                    })
                }) && !calls.values().any(|(name, input, key, _)| {
                    *name == request.tool_name
                        && *input == expected
                        && key == &request.idempotency_key
                }) {
                    return Ok(None);
                }
            } else {
                let payload =
                    crate::step_snapshot::decode_persisted_tool_batch_value(&spec.payload_ref)?;
                let Some(planned) = payload["calls"].as_array() else {
                    return Ok(None);
                };
                if !retired
                    && calls.len() + no_dispatch.map_or(0, |fact| fact.calls.len()) != planned.len()
                {
                    return Ok(None);
                }
                if calls.keys().any(|id| {
                    !planned
                        .iter()
                        .any(|call| call["id"].as_str() == Some(id.as_str()))
                }) {
                    return Ok(None);
                }
                let mut seen = BTreeSet::new();
                for call in planned {
                    let Some(id) = call["id"].as_str() else {
                        return Ok(None);
                    };
                    let Some(input) = call["input"].as_str() else {
                        return Ok(None);
                    };
                    if no_dispatch
                        .is_some_and(|fact| fact.calls.iter().any(|call| call.call_id == id))
                    {
                        continue;
                    }
                    if retired && !calls.contains_key(id) {
                        continue;
                    }
                    if !seen.insert(id) {
                        return Ok(None);
                    }
                    let digest = json!(format!("sha256:{:x}", Sha256::digest(input.as_bytes())));
                    if !calls.get(id).is_some_and(|(name, actual, _, origin)| {
                        *name == call["name"]
                            && *actual == digest
                            && call.get("origin").is_none_or(|expected| expected == origin)
                    }) {
                        return Ok(None);
                    }
                }
            }
        }
        let mut sources = vec![ExpectedStreamRevision {
            stream_id: stream,
            expected_revision: head,
        }];
        sources.extend(primary_sources.iter().map(|(stream_id, expected_revision)| ExpectedStreamRevision {
            stream_id:stream_id.clone(),expected_revision:*expected_revision,
        }));
        if let Some((_, _, _, _, parent_sources)) = delegated {
            sources.extend(parent_sources);
        }
        let mut merged_sources = BTreeMap::new();
        for source in &sources {
            merge_outcome_source_revisions(&mut merged_sources, &BTreeMap::from([(source.stream_id.clone(), source.expected_revision)]))?;
        }
        let sources = merged_sources.into_iter().map(|(stream_id,expected_revision)| ExpectedStreamRevision {stream_id,expected_revision}).collect::<Vec<_>>();
        Ok(Some(sources))
    }

    pub(super) async fn recover_running_bound_tool_batch(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
        registry: &super::registry::NodeExecutorRegistry,
    ) -> Result<bool, ExecutionCommitError> {
        use super::executors::agent_tool;
        let invalid = |reason: &str| ExecutionCommitError::InvalidCommand(reason.into());
        if !agent_tool::is_bound_agent_tool_graph(graph) {
            return Ok(false);
        }
        let store = ExecutionGraphStateStore::new(Arc::clone(&self.event_store));
        let (ticket, spec) = store
            .binding_start_ticket(graph, node, binding)
            .map_err(|error| invalid(&error.to_string()))?;
        if spec.kind != ExecutionNodeKind::ToolBatch
            || ticket.executor_kind != "tool_batch"
            || graph.nodes.first() != Some(&spec)
        {
            return Ok(false);
        }
        let bound = agent_tool::decode_bound_agent_tool_graph(graph)?;
        let request = &bound.request;
        let parent = request
            .parent_execution
            .as_ref()
            .ok_or_else(|| invalid("bound original parent missing"))?;
        let parent_graph = store
            .load(&parent.execution_id)
            .map_err(|error| invalid(&error.to_string()))?;
        let parent_attempt = request
            .parent_execution_attempt
            .ok_or_else(|| invalid("bound original parent attempt missing"))?;
        let parent_binding = store
            .current_binding(&parent_graph, &parent.node_id, parent_attempt)
            .map_err(|error| invalid(&error.to_string()))?;
        let (_, parent_spec) = store
            .binding_start_ticket(&parent_graph, &parent.node_id, &parent_binding)
            .map_err(|error| invalid(&error.to_string()))?;
        let agent: AgentTaskPacket = serde_json::from_str(&parent_spec.payload_ref)?;
        if parent_spec.kind != ExecutionNodeKind::AgentTask
            || agent.run_id() != bound.run_id
            || agent.agent_id() != bound.agent_id
            || agent.graph_id() != parent.execution_id
            || agent.node_id() != parent.node_id
            || agent.attempt != parent_attempt
            || request.session_id.as_deref() != Some(agent.session_id())
            || request.governed_plan_id != parent.execution_id
            || graph.parent_execution != request.parent_execution
            || graph.lineage != parent_graph.lineage
            || graph.service_class != parent_graph.service_class
            || (!agent.allowed_tools.contains(&request.tool_name)
                && request.tool_name != "tool_search")
        {
            return Err(invalid(
                "bound recovery cannot borrow another Agent/parent/attempt/session",
            ));
        }
        let invocation = request
            .compiled_invocation
            .as_ref()
            .ok_or_else(|| invalid("bound compiled invocation missing"))?;
        let step = request
            .step_binding
            .as_ref()
            .ok_or_else(|| invalid("bound original Step missing"))?;
        if invocation.origin.as_ref() != Some(&step.origin)
            || invocation.invocation_id != request.tool_use_id
            || invocation.intent.tool_name != request.tool_name
            || invocation.intent.normalized_input
                != harness_contract::step::parse_strict_json(&request.input)
                    .map_err(|error| invalid(&error.to_string()))?
            || step.origin.call_id != request.tool_use_id
            || step.effect_idempotency_key() != request.idempotency_key
            || step.snapshot.identity.session_id != agent.session_id()
            || step.snapshot.identity.turn_id
                != parent_graph
                    .lineage
                    .as_ref()
                    .ok_or_else(|| invalid("parent lineage missing"))?
                    .turn_id
        {
            return Err(invalid(
                "bound recovery original Step/invocation/input mismatch",
            ));
        }
        let lifecycle =
            crate::agent_runtime::authenticated_run_lifecycle(&self.event_store, &agent)
                .map_err(ExecutionCommitError::InvalidCommand)?;
        let primary_stream = format!("execution-effect:{}", request.idempotency_key);
        let index_stream = format!(
            "execution-agent-receipts:{}:{}:{}",
            parent.execution_id, parent.node_id, parent_attempt
        );
        let mut sources = vec![
            ExpectedStreamRevision {
                stream_id: primary_stream.clone(),
                expected_revision: self.event_store.stream_revision(&primary_stream)?,
            },
            ExpectedStreamRevision {
                stream_id: index_stream.clone(),
                expected_revision: self.event_store.stream_revision(&index_stream)?,
            },
            ExpectedStreamRevision {
                stream_id: parent_graph.id.clone(),
                expected_revision: parent_graph.revision,
            },
        ];
        sources.extend(lifecycle.sources);
        let primary = self
            .event_store
            .event_by_idempotency_key(
                &primary_stream,
                &format!("{}:receipt", request.idempotency_key),
            )?
            .or(self.event_store.event_by_idempotency_key(
                &primary_stream,
                &format!("{}:read-receipt", request.idempotency_key),
            )?);
        let index = self.event_store.event_by_idempotency_key(
            &index_stream,
            &format!("agent-tool-receipt:{}", request.idempotency_key),
        )?;
        let (Some(primary), Some(index)) = (primary, index) else {
            return Ok(false);
        };
        let read = primary.kind == "execution.read.receipt";
        let stored: crate::RuntimeToolExecutionOutcome =
            serde_json::from_value(primary.payload["outcome"].clone())?;
        if (read && primary.payload["outcome_truncated"] != false)
            || (!primary.payload["outcome_truncated"].is_null()
                && primary.payload["outcome_truncated"] != false)
            || (!index.payload["outcome_truncated"].is_null()
                && index.payload["outcome_truncated"] != false)
            || tool_effect_outcome_requires_truncation(&stored)
            || stored
                .error
                .as_ref()
                .is_some_and(|error| error.chars().count() >= MAX_TOOL_EFFECT_RECEIPT_CHARS)
        {
            return Ok(false);
        }
        if primary.scope != RuntimeEventScope::ExecutionNode
            || index.scope != RuntimeEventScope::ExecutionNode
            || primary.schema_version != 1
            || Self::receipt_observation_sequence(&index).is_none()
            || primary.actor.as_deref() != Some("governed_tool")
            || index.actor.as_deref() != Some("governed_tool")
            || primary.status.as_deref() != Some("completed")
            || index.status.as_deref() != Some("completed")
            || primary.transaction_id != index.transaction_id
            || primary.commit_cursor != index.commit_cursor
            || primary.transaction_index >= index.transaction_index
            || primary.payload["outcome"] != index.payload["outcome"]
            || stored.tool_use_id != request.tool_use_id
            || stored.tool_name != request.tool_name
            || primary.payload["tool_name"] != request.tool_name
            || primary.payload["input_sha256"]
                != format!("sha256:{:x}", Sha256::digest(request.input.as_bytes()))
            || primary.refs != index.refs
            || [
                "effect",
                "resolved_effect_scope",
                "root_effect_owner",
                "root_effect_context",
                "process_admission_ref",
                "process_closure",
                crate::context_evidence::READ_ACQUISITION_FIELD,
            ]
            .iter()
            .any(|field| primary.payload[*field] != index.payload[*field])
            || Self::receipt_observation_sequence(&index) != Some(request.observation_wave_sequence)
            || index.payload["authorized_scopes"]
                != serde_json::to_value(&request.authorized_scopes)?
            || index.payload["effect_scope"]
                != serde_json::to_value(
                    request
                        .authorization
                        .as_ref()
                        .map(|authorization| &authorization.scope),
                )?
        {
            return Err(invalid(
                "bound recovery exact original primary/index mismatch",
            ));
        }
        let descriptor = &invocation.effect;
        let outcome = if read {
            let Some(outcome) = agent_tool::agent_read_terminal_from_store(
                &self.event_store,
                self,
                request,
                descriptor,
            )
            .map_err(|error| invalid(&error.to_string()))?
            else {
                return Ok(false);
            };
            outcome
        } else {
            let intent = self
                .event_store
                .event_by_idempotency_key(
                    &primary_stream,
                    &format!("{}:intent", request.idempotency_key),
                )?
                .ok_or_else(|| invalid("bound mutation original intent missing"))?;
            let index_intent = self
                .event_store
                .event_by_idempotency_key(
                    &index_stream,
                    &format!("agent-tool-intent:{}", request.idempotency_key),
                )?
                .ok_or_else(|| invalid("bound mutation original index intent missing"))?;
            Self::validate_proof_bound_tool_request(request, &intent, &index_intent)?;
            self.validate_terminal_scope(request, descriptor, &primary, false)?;
            serde_json::from_value(primary.payload["outcome"].clone())?
        };
        if let Some(issuer_source) = self.validate_stored_local_network_receipt(&primary, descriptor)? {
            sources.push(issuer_source);
        }
        if !Self::lease_recovery_process_closed(&outcome, descriptor) {
            return Ok(false);
        }
        let Some(leaf_source) =
            self.lease_recovery_leaf_sources(graph, node, binding, &spec, false)?
        else {
            return Ok(false);
        };
        sources.extend(leaf_source);
        let recovered = agent_tool::agent_tool_outcome_at(&outcome, primary.created_at_ms)?;
        let mut merged_sources = BTreeMap::new();
        for source in &sources {
            merge_outcome_source_revisions(&mut merged_sources, &BTreeMap::from([(source.stream_id.clone(), source.expected_revision)]))?;
        }
        let sources = merged_sources.into_iter().map(|(stream_id,expected_revision)| ExpectedStreamRevision {stream_id,expected_revision}).collect::<Vec<_>>();
        let expected = sources
            .iter()
            .map(|source| (source.stream_id.clone(), source.expected_revision))
            .collect();
        self.transition_execution_outcome(graph, &ticket, &recovered, &expected)?;
        let committed = store
            .load(&graph.id)
            .map_err(|error| invalid(&error.to_string()))?;
        if self
            .authenticated_lease_recovery(&committed, node, binding, registry)
            .await?
            .is_none()
        {
            return Err(invalid(
                "bound recovered result publication/physical source did not close",
            ));
        }
        Ok(true)
    }

    /// Bounded, one-shot snapshot of why a physical closure could not be
    /// proven. It is production-reachable on purpose: an unproven closure
    /// must name its missing proof in the log of the run that failed, not
    /// only inside the test harness.
    pub(crate) fn physical_closure_diagnostic(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> serde_json::Value {
        fn bounded(
            value: serde_json::Value,
            remaining: &mut usize,
        ) -> serde_json::Value {
            let bytes = serde_json::to_vec(&value).unwrap_or_default();
            if bytes.len() > *remaining {
                return json!({
                    "omitted": "diagnostic_byte_budget",
                    "bytes": bytes.len(),
                    "sha256": format!("{:x}", Sha256::digest(&bytes)),
                });
            }
            *remaining -= bytes.len();
            value
        }
        fn lifecycle_snapshot(
            store: &RuntimeEventStore,
            packet: &AgentTaskPacket,
        ) -> serde_json::Value {
            let stream = format!("agent:{}", packet.agent_id());
            let before = store.stream_revision(&stream);
            let records = ["agent.run_admitted", "agent.backend_admitted", "agent.local_exit"]
                .into_iter()
                .map(|kind| {
                    let key = format!("{kind}:{}", packet.run_id());
                    let record = store.event_by_idempotency_key(&stream, &key);
                    json!({
                        "key":key, "record":record.as_ref().ok(),
                        "error":record.as_ref().err().map(ToString::to_string),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "packet":packet, "stream":stream, "records":records,
                "head_before":before.as_ref().ok(),
                "head_after":store.stream_revision(&stream).ok(),
                "confirmed_exit":format!("{:?}", crate::agent_runtime::confirmed_run_exit(store, packet)),
                "authenticated":crate::agent_runtime::authenticated_run_lifecycle(store, packet)
                    .map(|lifecycle| lifecycle.semantic_fact()),
            })
        }
        let mut remaining = 192 * 1024;
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        let original = store.binding_start_ticket(graph, node, binding);
        let mut streams = BTreeSet::new();
        let mut lifecycle = serde_json::Value::Null;
        let mut identity = json!({"error": format!("{:?}", original.as_ref().err())});
        if let Ok((ticket, spec)) = &original {
            streams.insert(ExecutionNodeBinding::effect_stream_id(ticket));
            let bound = super::executors::agent_tool::decode_bound_agent_tool(&ticket.payload_ref);
            if let Ok(bound) = &bound {
                streams.insert(format!("execution-effect:{}", bound.request.idempotency_key));
                if let Some(grant) = &bound.request.authorization {
                    streams.insert(format!(
                        "authorization-lease:{}", grant.authorization_lease.lease_id,
                    ));
                }
                if let (Some(parent), Some(attempt)) = (
                    &bound.request.parent_execution,
                    bound.request.parent_execution_attempt,
                ) {
                    streams.insert(format!(
                        "execution-agent-receipts:{}:{}:{}",
                        parent.execution_id, parent.node_id, attempt,
                    ));
                    if let Ok(parent_graph) = store.load(&parent.execution_id) {
                        if let Some(parent_spec) = parent_graph.nodes.iter().find(|spec| {
                            spec.id == parent.node_id && spec.kind == ExecutionNodeKind::AgentTask
                        }) {
                            if let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(&parent_spec.payload_ref) {
                                streams.insert(format!("agent:{}", packet.agent_id()));
                                lifecycle = lifecycle_snapshot(&self.event_store, &packet);
                            }
                        }
                    }
                }
            } else {
                streams.insert(format!(
                    "execution-agent-receipts:{}:{}:{}",
                    graph.id, node, binding.attempt,
                ));
            }
            identity = bounded(json!({
                "ticket": ticket,
                "spec": spec,
                "bound_decode_error": bound.as_ref().err().map(ToString::to_string),
            }), &mut remaining);
            if spec.kind == ExecutionNodeKind::AgentTask {
                if let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(&ticket.payload_ref) {
                    streams.insert(format!("agent:{}", packet.agent_id()));
                    lifecycle = lifecycle_snapshot(&self.event_store, &packet);
                }
            }
        }
        let lifecycle = bounded(lifecycle, &mut remaining);
        let mut snapshots = Vec::new();
        for stream in streams.into_iter().take(8) {
            let head = self.event_store.stream_revision(&stream);
            let records = match head.as_ref() {
                Ok(head) => self.event_store.list_stream_after(
                    &stream, 0, *head, 32, 64 * 1024,
                ),
                Err(error) => {
                    snapshots.push(json!({"stream": stream, "error": error.to_string()}));
                    continue;
                }
            };
            let records = match records {
                Ok(records) => {
                    let keys = records.iter().filter_map(|record| {
                        record.payload["idempotency_key"].as_str().map(str::to_owned)
                    }).collect::<BTreeSet<_>>();
                    for key in keys.into_iter().take(8) {
                        let primary_stream = format!("execution-effect:{key}");
                        for suffix in ["receipt", "read-receipt"] {
                            let primary = self.event_store.event_by_idempotency_key(
                                &primary_stream, &format!("{key}:{suffix}"),
                            );
                            snapshots.push(bounded(json!({
                                "stream": primary_stream,
                                "key": format!("{key}:{suffix}"),
                                "record": primary.as_ref().ok(),
                                "error": primary.as_ref().err().map(ToString::to_string),
                            }), &mut remaining));
                        }
                    }
                    json!({
                        "records": records,
                        "head": head.as_ref().ok(),
                        "bounded_page": {"max_records":32,"max_bytes":65536},
                    })
                }
                Err(error) => json!({"error": error.to_string()}),
            };
            snapshots.push(bounded(json!({
                "stream": stream,
                "page": records,
                "head_after": self.event_store.stream_revision(&stream).ok(),
            }), &mut remaining));
        }
        let snapshot = json!({
            "schema": "Own834.physical_closure.v1",
            "unproved": true,
            "graph": bounded(serde_json::to_value(graph).unwrap_or_default(), &mut remaining),
            "binding": binding,
            "identity": identity,
            "lifecycle": lifecycle,
            "sources": snapshots,
            "scope": "one failure snapshot; first 8 streams, first 32 records and 8 effect keys per page; omissions and moving cuts are not evidence",
        });
        bounded(snapshot, &mut (256 * 1024))
    }

    pub(super) fn binding_physical_sources(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
    ) -> Result<Option<Vec<ExpectedStreamRevision>>, ExecutionCommitError> {
        self.binding_physical_sources_with_witness(graph, node, binding, None)
    }

    pub(super) fn binding_physical_sources_with_witness(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
        witness: Option<&super::executors::agent_tool::NoDispatchWitness>,
    ) -> Result<Option<Vec<ExpectedStreamRevision>>, ExecutionCommitError> {
        let store = ExecutionGraphStateStore::new(self.event_store.clone());
        let (ticket, spec) = store
            .binding_start_ticket(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let completion = self.committed_node_attempt(graph, node, binding)?;
        let retired = store
            .binding_retirement_record(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
            .is_some();
        let fact = if let Some(witness) = witness {
            Some(self.materialize_no_dispatch_fact(
                graph,
                &ticket,
                binding,
                witness,
                if retired {
                    None
                } else {
                    completion.as_ref().map(|(result, _)| result)
                },
            )?)
        } else {
            self.canonical_no_dispatch_fact(graph, node, binding)?
        };
        let mut sources = Vec::new();
        if let Some(fact) = &fact {
            let Some(current) = self.no_dispatch_physical_sources(fact)? else {
                return Ok(None);
            };
            sources.extend(current.into_iter().map(|(stream_id, expected_revision)| {
                ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                }
            }));
        }
        if spec.kind == ExecutionNodeKind::AgentTask {
            let packet: AgentTaskPacket = serde_json::from_str(&ticket.payload_ref)?;
            let lifecycle =
                crate::agent_runtime::authenticated_run_lifecycle(&self.event_store, &packet)
                    .map_err(ExecutionCommitError::InvalidCommand)?;
            if !lifecycle.closed_for_governed_effects() {
                return Ok(None);
            }
            let Some((_, exit_sources)) =
                crate::agent_runtime::confirmed_run_exit(&self.event_store, &packet)
                    .map_err(ExecutionCommitError::InvalidCommand)?
            else {
                return Ok(None);
            };
            if lifecycle.sources != exit_sources {
                return Ok(None);
            }
            sources.extend(exit_sources);
        }
        let precommitted = spec.kind == ExecutionNodeKind::ToolBatch
            && spec.executor_kind == "tool_batch"
            && !super::executors::agent_tool::is_bound_agent_tool_graph(graph)
            && completion.as_ref().is_some_and(|(result, _)| {
                result.status == ExecutionNodeStatus::Completed
                    && result.result_ref.as_deref()
                        == Some(format!("{}:precommitted-terminal", graph.id).as_str())
            });
        let leaf = if precommitted {
            if !self.committed_host_transcript(&ticket)?.messages.is_empty() {
                return Ok(None);
            }
            let stream_id = format!(
                "execution-agent-receipts:{}:{}:{}",
                graph.id, node, binding.attempt
            );
            let expected_revision = self.event_store.stream_revision(&stream_id)?;
            if expected_revision != 0 {
                return Ok(None);
            }
            vec![ExpectedStreamRevision {
                stream_id,
                expected_revision,
            }]
        } else {
            let partial = retired
                || completion.as_ref().is_some_and(|(result, _)| {
                    result.status != ExecutionNodeStatus::Completed
                        || CallbackDisposition::from_result(node, result)
                            == CallbackDisposition::Abort
                });
            let Some(leaf) = self.lease_recovery_leaf_sources_with_fact(
                graph,
                node,
                binding,
                &spec,
                partial,
                fact.as_ref(),
            )?
            else {
                return Ok(None);
            };
            leaf
        };
        sources.extend(leaf);
        for source in &sources {
            if self.event_store.stream_revision(&source.stream_id)? != source.expected_revision {
                return Ok(None);
            }
        }
        Ok(Some(sources))
    }

    pub(super) async fn authenticated_lease_recovery(
        &self,
        graph: &ExecutionGraph,
        node: &str,
        binding: &ExecutionNodeBinding,
        registry: &super::registry::NodeExecutorRegistry,
    ) -> Result<Option<BindingRecoveryQuiescence>, ExecutionCommitError> {
        let store = ExecutionGraphStateStore::new(Arc::clone(&self.event_store));
        let (ticket, spec) = store
            .binding_start_ticket(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let node_stream = node_stream_id(&graph.id, node);
        let mut sources = vec![ExpectedStreamRevision {
            expected_revision: self.event_store.stream_revision(&node_stream)?,
            stream_id: node_stream,
        }];
        if spec.kind != ExecutionNodeKind::ToolBatch {
            let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
            sources.push(ExpectedStreamRevision {
                expected_revision: self.event_store.stream_revision(&stream)?,
                stream_id: stream,
            });
        }
        if let Some(fact) = self.canonical_no_dispatch_fact(graph, node, binding)? {
            let Some(current) = self.no_dispatch_physical_sources(&fact)? else {
                return Ok(None);
            };
            sources.extend(current.into_iter().map(|(stream_id, expected_revision)| {
                ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                }
            }));
        }
        let completion = self.committed_node_attempt(graph, node, binding)?;
        let retirement = store
            .binding_retirement_record(graph, node, binding)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        let retired = retirement.is_some();
        let disposition = completion.as_ref().map_or(
            if retired {
                CallbackDisposition::Abort
            } else {
                CallbackDisposition::Pending
            },
            |(result, _)| CallbackDisposition::from_result(node, result),
        );
        let Some(completion_source) = completion
            .as_ref()
            .map(|(_, record)| record)
            .or(retirement.as_ref())
        else {
            return Ok(None);
        };
        if completion_source.commit_cursor == 0 {
            return Ok(None);
        }
        match (&spec.kind, ticket.executor_kind.as_str()) {
            (ExecutionNodeKind::ToolBatch, "tool_batch") => {}
            (ExecutionNodeKind::AgentTask, "agent_task") => {
                match self.inspect_execution_effect(&ticket)? {
                    ExecutionEffectState::Completed(_) => {}
                    ExecutionEffectState::Uncertain if retired => {}
                    _ => return Ok(None),
                }
                let packet: AgentTaskPacket = serde_json::from_str(&ticket.payload_ref)?;
                if packet.graph_id() != graph.id.as_str()
                    || packet.node_id() != node
                    || packet.attempt != binding.attempt
                {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "lease recovery exact Agent packet mismatch".into(),
                    ));
                }
                let lifecycle =
                    crate::agent_runtime::authenticated_run_lifecycle(&self.event_store, &packet)
                        .map_err(ExecutionCommitError::InvalidCommand)?;
                if !lifecycle.closed_for_governed_effects() {
                    return Ok(None);
                }
                let Some((_, original_sources)) =
                    crate::agent_runtime::confirmed_run_exit(&self.event_store, &packet)
                        .map_err(ExecutionCommitError::InvalidCommand)?
                else {
                    return Ok(None);
                };
                if lifecycle.sources != original_sources {
                    return Ok(None);
                }
                sources.extend(original_sources);
            }
            (ExecutionNodeKind::InlineModel, "inline_model")
            | (ExecutionNodeKind::Materialize, "materialize")
            | (
                ExecutionNodeKind::Verify,
                "verify" | "compile_target_guard" | "agentic_program_wait",
            )
            | (ExecutionNodeKind::Synthesize, "synthesize" | "agentic_program_wait")
            | (ExecutionNodeKind::Approval, "approval")
            | (ExecutionNodeKind::SessionDispatch, "session_dispatch")
            | (ExecutionNodeKind::Timer, "timer") => {
                if !retired
                    && !matches!(
                        self.inspect_execution_effect(&ticket)?,
                        ExecutionEffectState::Completed(_)
                    )
                {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        }
        let precommitted = spec.kind == ExecutionNodeKind::ToolBatch
            && !super::executors::agent_tool::is_bound_agent_tool_graph(graph)
            && completion.as_ref().is_some_and(|(result, _)| {
                result.status == ExecutionNodeStatus::Completed
                    && result.result_ref.as_deref()
                        == Some(format!("{}:precommitted-terminal", graph.id).as_str())
            });
        let leaves = if precommitted {
            let transcript = self.committed_host_transcript(&ticket)?;
            let stream = format!(
                "execution-agent-receipts:{}:{}:{}",
                graph.id, node, binding.attempt
            );
            let head = self.event_store.stream_revision(&stream)?;
            if !transcript.messages.is_empty() || head != 0 {
                return Ok(None);
            }
            vec![ExpectedStreamRevision {
                stream_id: stream,
                expected_revision: head,
            }]
        } else {
            let Some(leaves) = self.lease_recovery_leaf_sources(
                graph,
                node,
                binding,
                &spec,
                disposition == CallbackDisposition::Abort
                    || retired
                    || completion
                        .as_ref()
                        .is_some_and(|(result, _)| result.status != ExecutionNodeStatus::Completed),
            )?
            else {
                return Ok(None);
            };
            leaves
        };
        sources.extend(leaves);
        if disposition == CallbackDisposition::Commit
            && spec.kind == ExecutionNodeKind::Synthesize
            && ticket.executor_kind == "synthesize"
        {
            self.committed_host_transcript(&ticket)?;
        }
        let Some(executor) = registry.get(&ticket.executor_kind) else {
            return Ok(None);
        };
        executor
            .validate(&spec)
            .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        if disposition == CallbackDisposition::Abort {
            executor
                .cancel_bounded(&ticket, std::time::Duration::from_secs(5))
                .await
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            if executor.cancellation_requires_quiescence(&ticket) {
                use futures::FutureExt;
                match executor.cancel(&ticket).now_or_never() {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        return Err(ExecutionCommitError::InvalidCommand(error.to_string()))
                    }
                    None => return Ok(None),
                }
            }
            executor
                .after_abort(&ticket, "canonical_command_retirement_recovery")
                .await
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        } else {
            executor
                .after_commit(&ticket)
                .await
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
        }
        for source in &sources {
            if self.event_store.stream_revision(&source.stream_id)? != source.expected_revision {
                return Ok(None);
            }
        }
        Ok(Some(BindingRecoveryQuiescence {
            key: super::events::ExecutionBindingLeaseClosure::key(graph, node, binding),
            sources,
        }))
    }

    #[cfg(test)]
    pub(crate) fn commit_observed_binding_closure_for_test(
        &self,
        graph: &ExecutionGraph,
        released: &super::resources::ReleasedBindingLeases<'_>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        self.commit_binding_lease_closure(graph, released)
    }

    pub(super) fn commit_binding_lease_closure(
        &self,
        graph: &ExecutionGraph,
        released: &super::resources::ReleasedBindingLeases<'_>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        self.commit_binding_lease_closure_with_witness(graph, released, None)
    }

    pub(super) fn commit_binding_lease_closure_with_witness(
        &self,
        graph: &ExecutionGraph,
        released: &super::resources::ReleasedBindingLeases<'_>,
        witness: Option<&super::executors::agent_tool::NoDispatchWitness>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let invalid = |reason: &str| ExecutionCommitError::InvalidCommand(reason.into());
        if !released.authentic_for(graph) {
            return Err(invalid("lease release capability mismatch"));
        }
        let mut closure = released.closure().clone();
        let mut release_sources = released.sources().to_vec();
        if closure.source == "normal_release" {
            release_sources.extend(
                self.binding_physical_sources_with_witness(
                    graph,
                    &closure.node_id,
                    &closure.binding,
                    witness,
                )?
                .ok_or_else(|| {
                    invalid("normal binding physical closure is not positively proven")
                })?,
            );
        }
        let (ticket, _) = ExecutionGraphStateStore::new(self.event_store.clone())
            .binding_start_ticket(graph, &closure.node_id, &closure.binding)
            .map_err(|error| invalid(&error.to_string()))?;
        closure.no_dispatch = if let Some(witness) = witness {
            Some(self.materialize_no_dispatch_fact(
                graph,
                &ticket,
                &closure.binding,
                witness,
                None,
            )?)
        } else {
            self.canonical_no_dispatch_fact(graph, &closure.node_id, &closure.binding)?
        };
        if let Some(fact) = &mut closure.no_dispatch {
            let current = self
                .no_dispatch_physical_sources(fact)?
                .ok_or_else(|| invalid("NoDispatch closure contradicts current primary"))?;
            fact.historical_sources = current.clone();
            release_sources.extend(current.into_iter().map(|(stream_id, expected_revision)| {
                ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                }
            }));
        }
        let mut expected_release_sources = BTreeMap::new();
        for source in release_sources {
            if expected_release_sources
                .insert(source.stream_id, source.expected_revision)
                .is_some_and(|previous| previous != source.expected_revision)
            {
                return Err(invalid("lease closure physical sources conflict"));
            }
        }
        let store = ExecutionGraphStateStore::new(Arc::clone(&self.event_store));
        let position = store
            .binding_start_position(graph, &closure.node_id, &closure.binding)
            .map_err(|error| invalid(&error.to_string()))?
            .ok_or_else(|| invalid("lease closure has no canonical exact binding start"))?;
        if !closure.matches(graph, &closure.node_id, &closure.binding, position) {
            return Err(invalid("lease closure identity/start mismatch"));
        }
        let mut next = graph.clone();
        next.revision = graph
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("lease closure revision overflow"))?;
        self.append_graph_event_with_expected_domain_revisions(
            &next,
            graph.revision,
            super::events::ExecutionBindingLeaseClosure::key(
                graph,
                &closure.node_id,
                &closure.binding,
            ),
            ExecutionGraphEvent::Recovered {
                recovered_nodes: Vec::new(),
                blocked_nodes: Vec::new(),
                lease_closures: vec![closure.clone()],
                delta: ExecutionGraphDelta::between(graph, &next),
            },
            Vec::new(),
            &expected_release_sources,
        )
    }

    pub(crate) fn commit_recovery(
        &self,
        graph: &ExecutionGraph,
        mut next: ExecutionGraph,
        recovered_nodes: Vec<String>,
        blocked_nodes: Vec<String>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        next.revision = graph.revision.saturating_add(1);
        let node_events = next
            .node_statuses
            .iter()
            .filter_map(|(node_id, to)| {
                let from = graph.node_statuses[node_id];
                (from != *to).then(|| node_transition_event(&next, node_id, from, *to, None))
            })
            .collect::<Result<_, _>>()?;
        self.append_graph_event(
            &next,
            graph.revision,
            format!("{}:recovery:{}", graph.id, next.revision),
            ExecutionGraphEvent::Recovered {
                recovered_nodes,
                blocked_nodes,
                lease_closures: Vec::new(),
                delta: ExecutionGraphDelta::between(graph, &next),
            },
            node_events,
        )
    }

    pub(crate) async fn commit_recovery_async(
        &self,
        graph: ExecutionGraph,
        next: ExecutionGraph,
        recovered_nodes: Vec<String>,
        blocked_nodes: Vec<String>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            service.commit_recovery(&graph, next, recovered_nodes, blocked_nodes)
        })
        .await
        .map_err(|error| ExecutionCommitError::BlockingTask(error.to_string()))?
    }

    fn append_graph_event(
        &self,
        graph: &ExecutionGraph,
        expected_graph_revision: u64,
        transaction_id: String,
        graph_event: ExecutionGraphEvent,
        domain_events: Vec<RuntimeTransactionEventInput>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        self.append_graph_event_with_expected_domain_revisions(
            graph,
            expected_graph_revision,
            transaction_id,
            graph_event,
            domain_events,
            &BTreeMap::new(),
        )
    }

    fn prepare_graph_append(
        &self,
        graph: &ExecutionGraph,
        expected_graph_revision: u64,
        transaction_id: String,
        graph_event: ExecutionGraphEvent,
        domain_events: Vec<RuntimeTransactionEventInput>,
        expected_domain_revisions: &BTreeMap<String, u64>,
    ) -> Result<PreparedGraphAppend, ExecutionCommitError> {
        let process_input_frame = self
            .process_input_verifier
            .as_ref()
            .map(|verifier| verifier.frame());
        let mut domain_events = domain_events;
        let mut observation_sources = expected_domain_revisions.clone();
        if observation_sources
            .get(&graph.id)
            .is_some_and(|revision| *revision != expected_graph_revision)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "outcome Graph source conflicts with original Graph CAS".into(),
            ));
        }
        for event in &domain_events {
            if event.event.kind == "goal.observation" {
                let source = &event.event.payload["goal_source"];
                if event.schema_version != 2
                    || source["stream_id"] != event.event.stream_id
                    || event.event.payload.get("goal").is_some()
                {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "unsupported observation source schema".into(),
                    ));
                }
                let revision = source["sequence"].as_u64().ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "observation source revision missing".into(),
                    )
                })?;
                if observation_sources
                    .insert(event.event.stream_id.clone(), revision)
                    .is_some_and(|previous| previous != revision)
                {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "observation source conflicts with causal read-set".into(),
                    ));
                }
            }
        }
        let expected_domain_revisions = &observation_sources;
        validated_graph_lineage(graph)?;
        if domain_events
            .iter()
            .any(|event| event.event.stream_id == graph.id)
        {
            return Err(ExecutionCommitError::GraphStreamCollision(graph.id.clone()));
        }
        if let Some(event) = domain_events
            .iter()
            .find(|event| event.idempotency_key.is_none())
        {
            return Err(ExecutionCommitError::MissingDomainIdempotency(
                event.event.stream_id.clone(),
            ));
        }
        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: graph.id.clone(),
            expected_revision: expected_graph_revision,
        }];
        let mut seen = BTreeSet::from([graph.id.clone()]);
        for event in &domain_events {
            if seen.insert(event.event.stream_id.clone()) {
                let idempotency_key = event.idempotency_key.as_deref().ok_or_else(|| {
                    ExecutionCommitError::MissingDomainIdempotency(event.event.stream_id.clone())
                })?;
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: event.event.stream_id.clone(),
                    expected_revision: match expected_domain_revisions.get(&event.event.stream_id) {
                        Some(revision) => *revision,
                        None => expected_domain_revision(
                            &self.event_store,
                            &event.event.stream_id,
                            &transaction_id,
                            idempotency_key,
                        )?,
                    },
                });
            }
        }
        // Include read-only causal dependencies (source Program/graphs) in
        // the same transaction fence even when this command appends no event
        // to their streams.
        for (stream_id, revision) in expected_domain_revisions {
            if seen.insert(stream_id.clone()) {
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: *revision,
                });
            }
        }
        let graph_key = match &graph_event {
            ExecutionGraphEvent::Recovered { lease_closures, .. } if lease_closures.len() == 1 => {
                let closure = &lease_closures[0];
                super::events::ExecutionBindingLeaseClosure::key(
                    graph,
                    &closure.node_id,
                    &closure.binding,
                )
            }
            _ => format!("{}:revision:{}", graph.id, graph.revision),
        };
        let graph_event = maybe_checkpoint(&self.event_store, graph, graph_event)?;
        let graph_input = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: graph.id.clone(),
                scope: RuntimeEventScope::ExecutionGraph,
                kind: graph_event.kind().to_string(),
                status: graph_status(graph).map(str::to_string),
                actor: Some("execution_commit_service".to_string()),
                refs: graph_identity_refs(graph),
                payload: self
                    .event_store
                    .prepare_content(
                        &format!("session:{}", validated_graph_lineage(graph)?.session_id),
                        &format!("graph:{}:revision:{}", graph.id, graph.revision),
                        &serde_json::to_value(&graph_event)?,
                        &crate::PreparedArtifactOrigin::EventBody {
                            stream_id: graph.id.clone(),
                            idempotency_key: graph_key.clone(),
                            kind: graph_event.kind().to_string(),
                        },
                    )
                    .map_err(ExecutionCommitError::InvalidCommand)?,
            }
            .with_activity_binding(root_activity_binding(graph)?)?,
            idempotency_key: Some(graph_key),
            schema_version: 2,
        };
        let mut terminal_events = domain_events
            .iter()
            .filter(|event| event.event.kind == "runtime.session.terminal_requested");
        let terminal = terminal_events
            .next()
            .map(|event| {
                serde_json::from_value::<SessionTerminalInput>(event.event.payload.clone()).map_err(
                    |error| {
                        ExecutionCommitError::InvalidCommand(format!(
                            "invalid Session terminal input: {error}"
                        ))
                    },
                )
            })
            .transpose()?;
        if terminal_events.next().is_some() {
            return Err(ExecutionCommitError::InvalidCommand(
                "graph transition contains multiple Session terminal inputs".to_string(),
            ));
        }
        let mut proof_source_streams = BTreeSet::new();
        let mut authored_metadata = None;
        if let Some(terminal) = terminal.as_ref() {
            let terminal_event = domain_events
                .iter()
                .find(|event| event.event.kind == "runtime.session.terminal_requested")
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidCommand(
                        "Session terminal event disappeared".into(),
                    )
                })?;
            if terminal_event
                .event
                .payload
                .get("objective_verdict")
                .is_some()
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "Session terminal cannot supply its own Objective verdict".into(),
                ));
            }
            let lineage = validated_graph_lineage(graph)?;
            if terminal.session_id != lineage.session_id
                || terminal.turn_id.as_deref() != Some(lineage.turn_id.as_str())
                || terminal.execution_id.as_deref() != Some(graph.id.as_str())
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "Session terminal does not match its Graph, Session and Turn".into(),
                ));
            }
            let objective_id = harness_contract::agent_action::root_objective_id(
                &lineage.session_id,
                &lineage.turn_id,
            );
            let program_id =
                harness_contract::agent_action::program_id_for_objective(&objective_id);
            let actions = crate::AgentActionService::new(Arc::clone(&self.event_store));
            if let Some(program) = actions
                .project_if_exists(&program_id)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?
            {
                let invalid = |reason: &str| {
                    ExecutionCommitError::InvalidCommand(format!(
                        "Session terminal Objective source is invalid: {reason}"
                    ))
                };
                if program.objective_id != objective_id
                    || program.session_id != lineage.session_id
                    || program.turn_id != lineage.turn_id
                    || program.root_execution_id.as_deref() != Some(graph.id.as_str())
                {
                    return Err(invalid("root, Session or Turn binding mismatch"));
                }
                let verdict = program
                    .objective_verdict
                    .as_ref()
                    .ok_or_else(|| invalid("Program has no committed Objective verdict"))?;
                let goals = crate::execution_core::GoalStore::new(Arc::clone(&self.event_store));
                let goal = goals
                    .projection(&verdict.goal_id)
                    .map_err(ExecutionCommitError::InvalidCommand)?
                    .ok_or_else(|| invalid("verdict Goal does not exist"))?;
                let bound = goal.goal.execution_binding.as_ref().is_some_and(|binding| {
                    binding.objective_id == objective_id
                        && binding.agentic_program_id == program_id
                        && binding.session_id == lineage.session_id
                        && binding.turn_id == lineage.turn_id
                        && binding.root_execution_id == graph.id
                });
                let matching_terminal = goal.goal.terminal.as_ref().is_some_and(|source| {
                    source.terminal_fence == verdict.terminal_fence
                        && source.authority_revision == verdict.authority_revision
                        && source.kind == verdict.kind
                });
                let matching_completion = matches!(
                    (verdict.kind, goal.goal.completion),
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
                if !bound
                    || !matching_terminal
                    || !matching_completion
                    || goal.goal.revision != verdict.goal_revision
                {
                    return Err(invalid("Goal terminal disagrees with Program verdict"));
                }
                let program_stream_id = format!("agentic-program:{program_id}");
                let conclusion = self.event_store.event_by_idempotency_key(
                    &program_stream_id,
                    &format!("program-conclusion:{}", verdict.authority_revision),
                )?;
                let bound_verdict = self.event_store.event_by_idempotency_key(
                    &program_stream_id,
                    &format!("agentic-objective-verdict:{}", verdict.terminal_fence),
                )?;
                let verdict_event = match (conclusion, bound_verdict) {
                    (None, None) => return Err(invalid("committed A verdict identity is missing")),
                    (Some(event), None) | (None, Some(event)) => event,
                    (Some(first), Some(second)) => {
                        if first.stream_id != second.stream_id
                            || first.event_id != second.event_id
                            || first.sequence != second.sequence
                            || first.commit_cursor != second.commit_cursor
                            || first.payload != second.payload
                        {
                            return Err(invalid("conflicting committed A verdict identities"));
                        }
                        first
                    }
                };
                if verdict_event.stream_id != program_stream_id
                    || verdict_event.kind != "agentic.objective_verdict_bound"
                    || verdict_event.payload["program_id"] != program_id
                    || verdict_event.payload["verdict"]
                        != serde_json::to_value(verdict)
                            .map_err(ExecutionCommitError::Serialization)?
                {
                    return Err(invalid("exact Program event is not this Objective verdict"));
                }
                for source in [
                    ExpectedStreamRevision {
                        stream_id: program_stream_id,
                        expected_revision: program.revision,
                    },
                    ExpectedStreamRevision {
                        stream_id: format!("goal:{}", verdict.goal_id),
                        expected_revision: goal.stream_revision,
                    },
                ] {
                    if expected_domain_revisions
                        .get(&source.stream_id)
                        .is_some_and(|revision| *revision != source.expected_revision)
                    {
                        return Err(invalid(
                            "caller expected a different Objective source revision",
                        ));
                    }
                    if domain_events
                        .iter()
                        .any(|event| event.event.stream_id == source.stream_id)
                    {
                        return Err(invalid("Objective source stream collides with B writes"));
                    }
                    if seen.insert(source.stream_id.clone()) {
                        expected_streams.push(source);
                    }
                }
                let history = self
                    .proof_session_queries
                    .as_ref()
                    .and_then(|queries| queries.get())
                    .and_then(|queries| queries.history_reader());
                let readers = crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: process_input_frame.as_ref(),
                    artifacts: self.proof_artifacts.as_deref(),
                    history: history.as_deref(),
                };
                let satisfied =
                    goal.goal.completion == harness_contract::goal::GoalCompletion::Satisfied;
                let proof_sources = if satisfied {
                    crate::agentic::review_evidence::committed_proof_sources(
                        &self.event_store,
                        &goal.goal,
                        readers,
                    )
                    .map_err(ExecutionCommitError::InvalidCommand)?
                } else {
                    Vec::new()
                };
                for source in proof_sources {
                    if domain_events
                        .iter()
                        .any(|event| event.event.stream_id == source.stream_id)
                    {
                        return Err(invalid(
                            "committed A proof source stream collides with B writes",
                        ));
                    }
                    if expected_domain_revisions
                        .get(&source.stream_id)
                        .is_some_and(|revision| *revision != source.expected_revision)
                    {
                        return Err(invalid(
                            "caller expected a different committed A material source revision",
                        ));
                    }
                    proof_source_streams.insert(source.stream_id.clone());
                    if let Some(existing) = expected_streams
                        .iter()
                        .find(|existing| existing.stream_id == source.stream_id)
                    {
                        if existing.expected_revision != source.expected_revision {
                            return Err(invalid("committed A material source revision changed"));
                        }
                    } else {
                        seen.insert(source.stream_id.clone());
                        expected_streams.push(source);
                    }
                }
                let terminal_event = domain_events
                    .iter_mut()
                    .find(|event| event.event.kind == "runtime.session.terminal_requested")
                    .ok_or_else(|| invalid("Session terminal event disappeared"))?;
                let metadata = serde_json::json!({
                    "program_id": program_id,
                    "program_stream_revision": program.revision,
                    "goal_stream_revision": goal.stream_revision,
                    "goal_spec_revision": goal.goal.spec_revision,
                    "goal_spec_digest": goal.goal.spec_digest,
                    "a_commit_cursor": verdict_event.commit_cursor,
                    "proof_input_digests": if satisfied {
                        crate::agentic::review_evidence::required_review_conditions(&goal.goal).iter()
                            .filter_map(|condition| goal.goal.reviews.iter().rev()
                                .find(|review| &review.criterion_ref == condition))
                            .filter_map(|review| review.verification.as_ref()
                                .and_then(|proof| proof.input_set.as_ref())
                                .map(|inputs| inputs.semantic_digest.clone()))
                            .collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>()
                    } else { Vec::new() },
                    "verdict": verdict,
                });
                terminal_event.event.payload["objective_verdict"] = metadata.clone();
                authored_metadata = Some(metadata);
                terminal_event.event.refs.extend([
                    RuntimeEventRef {
                        kind: "program".into(),
                        id: program_id,
                    },
                    RuntimeEventRef {
                        kind: "goal".into(),
                        id: verdict.goal_id.clone(),
                    },
                    RuntimeEventRef {
                        kind: "terminal_fence".into(),
                        id: verdict.terminal_fence.clone(),
                    },
                ]);
            }
        }
        let mut events = vec![graph_input];
        events.extend(domain_events);
        let refreshable_lineage = expected_streams
            .iter()
            .filter(|source| {
                source.stream_id.starts_with("execution-lineage:")
                    && !proof_source_streams.contains(&source.stream_id)
                    && !expected_domain_revisions.contains_key(&source.stream_id)
            })
            .map(|source| source.stream_id.clone())
            .collect();
        Ok(PreparedGraphAppend {
            graph: graph.clone(),
            transaction_id,
            expected_streams,
            events,
            terminal,
            authored_metadata,
            refreshable_lineage,
            process_input_frame: process_input_frame.map(Arc::new),
        })
    }

    fn append_graph_event_with_expected_domain_revisions(
        &self,
        graph: &ExecutionGraph,
        expected_graph_revision: u64,
        transaction_id: String,
        graph_event: ExecutionGraphEvent,
        domain_events: Vec<RuntimeTransactionEventInput>,
        expected_domain_revisions: &BTreeMap<String, u64>,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let prepared = self.prepare_graph_append(
            graph,
            expected_graph_revision,
            transaction_id,
            graph_event,
            domain_events,
            expected_domain_revisions,
        )?;
        self.commit_prepared_graph_append(prepared)
    }

    fn commit_prepared_graph_append(
        &self,
        prepared: PreparedGraphAppend,
    ) -> Result<ExecutionCommitReceipt, ExecutionCommitError> {
        let PreparedGraphAppend {
            graph,
            transaction_id,
            mut expected_streams,
            events,
            terminal,
            authored_metadata,
            refreshable_lineage: lineage_streams,
            process_input_frame: _process_input_frame,
        } = prepared;
        #[cfg(test)]
        if super::executors::agent_tool::is_bound_agent_tool_graph(&graph)
            && graph
                .node_statuses
                .values()
                .any(|status| *status == ExecutionNodeStatus::Completed)
            && self
                .fail_next_bound_outcome_450
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ExecutionCommitError::InvalidCommand(
                "450 original leaf completed before Graph result append fault".into(),
            ));
        }
        const MAX_LINEAGE_RETRIES: usize = 8;
        let mut attempt = 0;
        let transaction = loop {
            attempt += 1;
            let request = AppendTransactionRequest {
                transaction_id: transaction_id.clone(),
                expected_streams: expected_streams.clone(),
                events: events.clone(),
            };
            #[cfg(any(test, feature = "test-fixtures"))]
            if let Some(terminal) = terminal.as_ref() {
                let observer = self
                    .terminal_append_observers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&terminal.terminal_id);
                if let Some(observer) = observer {
                    if observer.expected.send(expected_streams.clone()).is_ok() {
                        observer
                            .release
                            .recv_timeout(std::time::Duration::from_secs(30))
                            .map_err(|error| {
                                ExecutionCommitError::InvalidCommand(format!(
                                    "terminal append observer lost: {error}"
                                ))
                            })?;
                    }
                }
            }
            let duplicate_request = request.clone();
            #[cfg(test)]
            if events.iter().any(|event| {
                event.event.kind == "execution_node.transitioned"
                    && event.event.payload["from"] == serde_json::json!("ready")
                    && event.event.payload["to"] == serde_json::json!("running")
            }) {
                let failure = self
                    .terminal_response_losses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&format!("binding-start-before-append:{}", graph.id));
                if let Some(hits) = failure {
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    return Err(ExecutionCommitError::InvalidCommand(
                        "534 injected binding-start failure before atomic append".into(),
                    ));
                }
            }
            let result = match terminal.as_ref() {
                Some(terminal) => self
                    .event_store
                    .append_transaction_with_authored_terminal_metadata(
                        request,
                        terminal.clone(),
                        authored_metadata.as_ref(),
                    ),
                None => self.event_store.append_transaction(request),
            };
            #[cfg(test)]
            let result = if result.is_ok() {
                let loss = {
                    let mut pending = self
                        .terminal_response_losses
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let start = events.iter().any(|event| {
                        event.event.kind == "execution_node.transitioned"
                            && event.event.payload["from"] == serde_json::json!("ready")
                            && event.event.payload["to"] == serde_json::json!("running")
                    });
                    let start_loss = if start {
                        pending.remove(&format!("binding-start:{}", graph.id))
                    } else {
                        None
                    };
                    start_loss.or_else(|| {
                        events
                            .iter()
                            .filter(|event| {
                                event.event.kind == "execution_node.transitioned"
                                    && event.event.payload["from"] == serde_json::json!("running")
                                    && event.event.payload["result"].is_object()
                            })
                            .find_map(|event| {
                                event.event.payload["node_id"].as_str().and_then(|node| {
                                    pending.remove(&format!("node-outcome:{node}"))
                                })
                            })
                    })
                };
                if let Some(hits) = loss {
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(RuntimeEventStoreError::Corrupt(
                        "450 injected loss after original atomic append".into(),
                    ))
                } else {
                    result
                }
            } else {
                result
            };
            let result = if result.is_err() {
                let stored = self
                    .event_store
                    .transaction_stream_revision_exact(&transaction_id, &graph.id)?;
                let expected = duplicate_request
                    .expected_streams
                    .iter()
                    .find(|source| source.stream_id == graph.id);
                if stored.is_some_and(|stored| {
                    expected
                        .is_some_and(|source| stored.expected_revision == source.expected_revision)
                        && stored.committed_revision == graph.revision
                }) {
                    match terminal.as_ref() {
                        Some(terminal) => self
                            .event_store
                            .append_transaction_with_authored_terminal_metadata(
                                duplicate_request,
                                terminal.clone(),
                                authored_metadata.as_ref(),
                            ),
                        None => self.event_store.append_transaction(duplicate_request),
                    }
                } else {
                    result
                }
            } else {
                result
            };
            #[cfg(any(test, feature = "test-fixtures"))]
            if let (Some(terminal), Err(error)) = (terminal.as_ref(), result.as_ref()) {
                if let Some(observer) = self
                    .terminal_append_failures
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&terminal.terminal_id)
                {
                    let _ = observer.send(error.to_string());
                }
            }
            match result {
                Ok(transaction) => break transaction,
                Err(RuntimeEventStoreError::StaleRevision { ref stream_id, .. })
                    if attempt < MAX_LINEAGE_RETRIES && lineage_streams.contains(stream_id) =>
                {
                    for source in expected_streams.iter_mut() {
                        if lineage_streams.contains(&source.stream_id) {
                            source.expected_revision =
                                self.event_store.stream_revision(&source.stream_id)?;
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
        };
        let mut committed_graph = graph;
        committed_graph.recovery_cursor.commit_cursor = transaction.commit_cursor;
        self.hot_graphs.publish(committed_graph.clone());
        let _ = self
            .hot_state
            .materializer()
            .enqueue(DerivedMaterialization {
                key: format!("execution-graph:{}", committed_graph.id),
                revision: committed_graph.revision,
                commit_cursor: committed_graph.recovery_cursor.commit_cursor,
            });
        #[cfg(any(test, feature = "test-fixtures"))]
        if let Some(terminal) = terminal.as_ref() {
            let hit = self
                .terminal_response_losses
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&terminal.terminal_id);
            if let Some(hit) = hit {
                hit.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(ExecutionCommitError::InvalidCommand(format!(
                    "injected response loss after durable terminal {}",
                    terminal.terminal_id
                )));
            }
        }
        Ok(ExecutionCommitReceipt {
            graph: committed_graph,
            transaction,
        })
    }
}

#[cfg(test)]
mod recovery_physical_450_tests {
    use super::*;

    #[tokio::test]
    async fn r1_618_delegated_host_read_uses_original_parent_index_and_frozen_sources() {
        use super::super::executors::agent_tool::decode_bound_agent_tool_graph;
        use crate::agentic::execution::reviewed_goal_fixture::PhysicalProcessFixture;
        eprintln!("r1_618 stage=fixture_start");
        let fixture = PhysicalProcessFixture::new(
            crate::config::AgentExecutorSandboxProfile::WorkspaceReadOnly,
            "618-real-parent-read",
        )
        .await;
        eprintln!("r1_618 stage=fixture_ready");
        let worker = fixture
            .dispatch("618-reader", vec!["read:.".into()], false, false, true)
            .await;
        eprintln!("r1_618 stage=worker_dispatched");
        let directory = worker.directory();
        let token = uuid::Uuid::new_v4().to_string();
        let staging = directory.join(format!("{token}.staging"));
        let command = directory.join(format!("{token}.request"));
        let response = directory.join(format!("{token}.response"));
        std::fs::write(
            &staging,
            serde_json::to_vec(&json!({
                "request_id":token,"tool_name":"read_file",
                "input":{"path":"out/a"}
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::rename(staging, command).unwrap();
        eprintln!("r1_618 stage=read_requested");
        let reply = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Ok(bytes) = std::fs::read(&response) {
                    break serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("r1_618 unproven: read reply exceeded original 30s probe budget");
        eprintln!("r1_618 stage=read_replied");
        assert!(reply.get("error").is_none_or(serde_json::Value::is_null));
        let state = fixture.services.graph_state_store();
        let bound = state
            .graph_ids()
            .unwrap()
            .into_iter()
            .filter_map(|id| {
                let graph = state.load(&id).unwrap();
                decode_bound_agent_tool_graph(&graph).ok()
            })
            .find(|bound| bound.request.tool_use_id == token)
            .expect("original real Process command must retain its exact BoundAgentTool");
        let parent = state.load(worker.packet.graph_id()).unwrap();
        let step = bound.request.step_binding.as_ref().unwrap();
        assert!(matches!(
            &step.origin.source,
            harness_contract::step::CallSource::Process { worker_id, sequence }
                if worker_id == worker.packet.run_id() && *sequence > 0
        ));
        assert_ne!(
            worker.packet.assignment.execution_identity.principal_id(),
            worker.packet.agent_id()
        );
        assert_eq!(
            step.snapshot.identity.actor_ref.as_deref(),
            Some(worker.packet.agent_id())
        );
        let encode = |input: &str, snapshot: &harness_contract::step::StepSnapshot| {
            crate::step_snapshot::encode_persisted_tool_batch(&json!({
                "session_id":worker.packet.session_id(),
                "step_snapshots":{snapshot.identity.step_id.clone():snapshot},
                "calls":[{
                    "id":bound.request.tool_use_id,"name":bound.request.tool_name,
                    "input":input,"depends_on":[],"origin":step.origin
                }]
            }))
            .unwrap()
        };
        let mut graph = ExecutionGraph::new("618 original delegated Host consumer");
        graph.parent_execution = bound.request.parent_execution.clone();
        graph.lineage = parent.lineage.clone();
        let spec = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            encode(&bound.request.input, &step.snapshot),
        );
        let binding = ExecutionNodeBinding {
            executor_kind: "tool_batch".into(),
            ticket_idempotency_key: spec.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: vec![],
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let commits = fixture.services.commit_service();
        graph.nodes.push(spec.clone());
        graph = commits.register_graph(graph).unwrap().graph;
        let events = fixture.services.event_store();
        let parent_index = format!(
            "execution-agent-receipts:{}:{}:{}",
            worker.packet.graph_id(),
            worker.packet.node_id(),
            worker.packet.attempt
        );
        let own_index = format!("execution-agent-receipts:{}:{}:1", graph.id, spec.id);
        assert_eq!(events.stream_revision(&own_index).unwrap(), 0);
        let primary = format!("execution-effect:{}", bound.request.idempotency_key);
        let before = [
            (
                parent_index.clone(),
                events.stream_revision(&parent_index).unwrap(),
            ),
            (primary.clone(), events.stream_revision(&primary).unwrap()),
        ];
        assert!(before.iter().all(|(_, head)| *head > 0));
        let sources = commits
            .lease_recovery_leaf_sources_with_fact(&graph, &spec.id, &binding, &spec, false, None)
            .unwrap()
            .expect("outer Host must consume its original real delegated receipt");
        assert!(sources
            .iter()
            .any(|source| source.stream_id == parent_index));
        assert!(sources.iter().any(|source| source.stream_id == parent.id));
        assert!(sources.iter().any(|source| source.stream_id
            == node_stream_id(worker.packet.graph_id(), worker.packet.node_id())));
        assert!(!sources.iter().any(|source| source.stream_id == own_index));
        let repeated = commits
            .lease_recovery_leaf_sources_with_fact(&graph, &spec.id, &binding, &spec, false, None)
            .unwrap()
            .unwrap();
        assert_eq!(sources, repeated);
        let mut missing =
            crate::step_snapshot::decode_persisted_tool_batch_value(&spec.payload_ref).unwrap();
        missing["calls"][0]["id"] = json!("618-missing-original-primary");
        missing["calls"][0]["origin"]["call_id"] = json!("618-missing-original-primary");
        let mut absent = spec.clone();
        absent.payload_ref = crate::step_snapshot::encode_persisted_tool_batch(&missing).unwrap();
        assert!(commits
            .lease_recovery_leaf_sources_with_fact(
                &graph, &absent.id, &binding, &absent, false, None,
            )
            .unwrap()
            .is_none());
        let mut wrong = spec.clone();
        wrong.payload_ref = encode(
            &json!({"path":"unrelated-review.txt"}).to_string(),
            &step.snapshot,
        );
        assert!(
            commits
                .lease_recovery_leaf_sources_with_fact(
                    &graph, &wrong.id, &binding, &wrong, false, None,
                )
                .unwrap()
                .is_none()
        );
        for change in [
            "run",
            "actor",
            "session",
            "attempt",
            "principal",
            "definition-agent",
        ] {
            let mut snapshot = (*step.snapshot).clone();
            match change {
                "run" => snapshot.identity.run_ref = Some("foreign-run".into()),
                "actor" => snapshot.identity.actor_ref = Some("foreign-agent".into()),
                "session" => snapshot.identity.session_id = "foreign-session".into(),
                "principal" => {
                    snapshot.identity.actor_ref = Some(
                        worker
                            .packet
                            .assignment
                            .execution_identity
                            .principal_id()
                            .into(),
                    )
                }
                "definition-agent" => {
                    snapshot.identity.actor_ref = Some(
                        worker
                            .packet
                            .agentic_binding
                            .as_ref()
                            .unwrap()
                            .agent_id
                            .clone(),
                    )
                }
                _ => snapshot.identity.attempt = Some(worker.packet.attempt + 1),
            }
            wrong.payload_ref = encode(&bound.request.input, &snapshot);
            assert!(
                commits
                    .lease_recovery_leaf_sources_with_fact(
                        &graph, &wrong.id, &binding, &wrong, false, None,
                    )
                    .is_err(),
                "{change}"
            );
        }
        for (stream, head) in before {
            assert_eq!(events.stream_revision(&stream).unwrap(), head);
        }
        assert_eq!(events.stream_revision(&own_index).unwrap(), 0);
        eprintln!("r1_618 stage=source_checks_complete");
        assert!(!crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(&graph));
        eprintln!("r1_618 stage=withdraw_reap_start");
        worker.withdraw_and_require_rejected_child_709(&graph.id).await;
        eprintln!("r1_618 stage=physically_reaped_retirement_rejected");
    }

    #[tokio::test]
    async fn r1_763_selected_source_port_reads_grow_with_unrelated_history() {
        use super::super::executors::agent_tool::decode_bound_agent_tool_graph;
        use crate::agentic::execution::reviewed_goal_fixture::PhysicalProcessFixture;
        eprintln!("r1_618 stage=fixture_start");
        let (journal, counter) = crate::content_publication::tests::JournalFault::new_journal();
        let fixture = PhysicalProcessFixture::new_with_journal_763(
            crate::config::AgentExecutorSandboxProfile::WorkspaceReadOnly,
            "763-real-parent-read",
            journal,
        )
        .await;
        eprintln!("r1_618 stage=fixture_ready");
        let worker = fixture
            .dispatch("618-reader", vec!["read:.".into()], false, false, true)
            .await;
        eprintln!("r1_618 stage=worker_dispatched");
        let directory = worker.directory();
        let token = uuid::Uuid::new_v4().to_string();
        let staging = directory.join(format!("{token}.staging"));
        let command = directory.join(format!("{token}.request"));
        let response = directory.join(format!("{token}.response"));
        std::fs::write(
            &staging,
            serde_json::to_vec(&json!({
                "request_id":token,"tool_name":"read_file",
                "input":{"path":"out/a"}
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::rename(staging, command).unwrap();
        eprintln!("r1_618 stage=read_requested");
        let reply = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Ok(bytes) = std::fs::read(&response) {
                    break serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("r1_618 unproven: read reply exceeded original 30s probe budget");
        eprintln!("r1_618 stage=read_replied");
        assert!(reply.get("error").is_none_or(serde_json::Value::is_null));
        let state = fixture.services.graph_state_store();
        let bound = state
            .graph_ids()
            .unwrap()
            .into_iter()
            .filter_map(|id| {
                let graph = state.load(&id).unwrap();
                decode_bound_agent_tool_graph(&graph).ok()
            })
            .find(|bound| bound.request.tool_use_id == token)
            .expect("original real Process command must retain its exact BoundAgentTool");
        let parent = state.load(worker.packet.graph_id()).unwrap();
        let step = bound.request.step_binding.as_ref().unwrap();
        assert!(matches!(
            &step.origin.source,
            harness_contract::step::CallSource::Process { worker_id, sequence }
                if worker_id == worker.packet.run_id() && *sequence > 0
        ));
        assert_ne!(
            worker.packet.assignment.execution_identity.principal_id(),
            worker.packet.agent_id()
        );
        assert_eq!(
            step.snapshot.identity.actor_ref.as_deref(),
            Some(worker.packet.agent_id())
        );
        let encode = |input: &str, snapshot: &harness_contract::step::StepSnapshot| {
            crate::step_snapshot::encode_persisted_tool_batch(&json!({
                "session_id":worker.packet.session_id(),
                "step_snapshots":{snapshot.identity.step_id.clone():snapshot},
                "calls":[{
                    "id":bound.request.tool_use_id,"name":bound.request.tool_name,
                    "input":input,"depends_on":[],"origin":step.origin
                }]
            }))
            .unwrap()
        };
        let mut graph = ExecutionGraph::new("618 original delegated Host consumer");
        graph.parent_execution = bound.request.parent_execution.clone();
        graph.lineage = parent.lineage.clone();
        let spec = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            encode(&bound.request.input, &step.snapshot),
        );
        let binding = ExecutionNodeBinding {
            executor_kind: "tool_batch".into(),
            ticket_idempotency_key: spec.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: vec![],
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let commits = fixture.services.commit_service();
        graph.nodes.push(spec.clone());
        graph = commits.register_graph(graph).unwrap().graph;
        let events = fixture.services.event_store();
        let parent_index = format!(
            "execution-agent-receipts:{}:{}:{}",
            worker.packet.graph_id(),
            worker.packet.node_id(),
            worker.packet.attempt
        );
        let own_index = format!("execution-agent-receipts:{}:{}:1", graph.id, spec.id);
        assert_eq!(events.stream_revision(&own_index).unwrap(), 0);
        let primary = format!("execution-effect:{}", bound.request.idempotency_key);
        let before = [
            (
                parent_index.clone(),
                events.stream_revision(&parent_index).unwrap(),
            ),
            (primary.clone(), events.stream_revision(&primary).unwrap()),
        ];
        assert!(before.iter().all(|(_, head)| *head > 0));
        let sources = commits
            .lease_recovery_leaf_sources_with_fact(&graph, &spec.id, &binding, &spec, false, None)
            .unwrap()
            .expect("outer Host must consume its original real delegated receipt");
        assert!(sources
            .iter()
            .any(|source| source.stream_id == parent_index));
        assert!(sources.iter().any(|source| source.stream_id == parent.id));
        assert!(sources.iter().any(|source| source.stream_id
            == node_stream_id(worker.packet.graph_id(), worker.packet.node_id())));
        assert!(!sources.iter().any(|source| source.stream_id == own_index));
        let repeated = commits
            .lease_recovery_leaf_sources_with_fact(&graph, &spec.id, &binding, &spec, false, None)
            .unwrap()
            .unwrap();
        assert_eq!(sources, repeated);
        let mut missing =
            crate::step_snapshot::decode_persisted_tool_batch_value(&spec.payload_ref).unwrap();
        missing["calls"][0]["id"] = json!("618-missing-original-primary");
        missing["calls"][0]["origin"]["call_id"] = json!("618-missing-original-primary");
        let mut absent = spec.clone();
        absent.payload_ref = crate::step_snapshot::encode_persisted_tool_batch(&missing).unwrap();
        assert!(commits
            .lease_recovery_leaf_sources_with_fact(
                &graph, &absent.id, &binding, &absent, false, None,
            )
            .unwrap()
            .is_none());
        let mut wrong = spec.clone();
        wrong.payload_ref = encode(
            &json!({"path":"unrelated-review.txt"}).to_string(),
            &step.snapshot,
        );
        assert!(
            commits
                .lease_recovery_leaf_sources_with_fact(
                    &graph, &wrong.id, &binding, &wrong, false, None,
                )
                .unwrap()
                .is_none()
        );
        for change in [
            "run",
            "actor",
            "session",
            "attempt",
            "principal",
            "definition-agent",
        ] {
            let mut snapshot = (*step.snapshot).clone();
            match change {
                "run" => snapshot.identity.run_ref = Some("foreign-run".into()),
                "actor" => snapshot.identity.actor_ref = Some("foreign-agent".into()),
                "session" => snapshot.identity.session_id = "foreign-session".into(),
                "principal" => {
                    snapshot.identity.actor_ref = Some(
                        worker
                            .packet
                            .assignment
                            .execution_identity
                            .principal_id()
                            .into(),
                    )
                }
                "definition-agent" => {
                    snapshot.identity.actor_ref = Some(
                        worker
                            .packet
                            .agentic_binding
                            .as_ref()
                            .unwrap()
                            .agent_id
                            .clone(),
                    )
                }
                _ => snapshot.identity.attempt = Some(worker.packet.attempt + 1),
            }
            wrong.payload_ref = encode(&bound.request.input, &snapshot);
            assert!(
                commits
                    .lease_recovery_leaf_sources_with_fact(
                        &graph, &wrong.id, &binding, &wrong, false, None,
                    )
                    .is_err(),
                "{change}"
            );
        }
        for (stream, head) in before {
            assert_eq!(events.stream_revision(&stream).unwrap(), head);
        }
        assert_eq!(events.stream_revision(&own_index).unwrap(), 0);
        let measurement_start = std::time::Instant::now();
        let summarize = |reads: &[(String, String, usize, usize)]| {
            let mut totals = BTreeMap::<String, (usize, usize, usize)>::new();
            for (port, stream, records, bytes) in reads {
                let total = totals.entry(format!("{port}:{stream}")).or_default();
                total.0 += 1;
                total.1 += records;
                total.2 += bytes;
            }
            totals
        };
        let mut inserted = 0usize;
        let mut cumulative = 0usize;
        for unrelated in [0usize, 100, 1000] {
            if unrelated > inserted {
                let head = events.stream_revision(&parent_index).unwrap();
                let noise = (inserted..unrelated).map(|position| {
                    let key = format!("763-unselected-{position}");
                    let mut event: crate::runtime_event_store::RuntimeTransactionEventInput =
                        RuntimeEventInput {
                            stream_id: parent_index.clone(),
                            scope: RuntimeEventScope::ExecutionNode,
                            kind: "execution.agent_tool.intent".into(),
                            status: None, actor: Some("governed_tool".into()), refs: vec![],
                            payload: json!({"idempotency_key":key}),
                        }.into();
                    event.idempotency_key = Some(format!("agent-tool-intent:{key}"));
                    event
                }).collect();
                events.append_transaction(AppendTransactionRequest {
                    transaction_id: format!("763-unrelated-through-{unrelated}"),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: parent_index.clone(), expected_revision: head,
                    }], events: noise,
                }).unwrap();
                inserted = unrelated;
            }
            let head = events.stream_revision(&parent_index).unwrap();
            counter.begin_source_reads_763();
            let started = std::time::Instant::now();
            let selected = commits.lease_recovery_leaf_sources_with_fact(
                &graph, &spec.id, &binding, &spec, false, None,
            ).unwrap().expect("original selected real receipt remains recoverable");
            let elapsed_ns = started.elapsed().as_nanos();
            let reads = counter.take_source_reads_763();
            let index_records: usize = reads.iter()
                .filter(|(port, stream, _, _)| port == "list_stream_after" && stream == &parent_index)
                .map(|(_, _, records, _)| records).sum();
            assert_eq!(index_records as u64, head);
            cumulative += index_records;
            assert!(selected.iter().any(|source|
                source.stream_id == parent_index && source.expected_revision == head));
            eprintln!("Own763 {}", json!({"arm":"production_scan", "unrelated":unrelated,
                "selected_distinct_keys":1, "recovery_calls":1, "head":head,
                "elapsed_ns":elapsed_ns, "cumulative_index_records":cumulative,
                "counts_records_bytes_by_port_stream":summarize(&reads),
                "bytes_definition":"sum JSON-encoded returned records, not physical IO"}));
            for lookups in [100usize, 1000] {
                counter.begin_source_reads_763();
                let started = std::time::Instant::now();
                for _ in 0..lookups {
                    let intent = events.event_by_idempotency_key(&parent_index,
                        &format!("agent-tool-intent:{}", bound.request.idempotency_key)).unwrap();
                    assert!(intent.is_none(), "a read must not fabricate a mutation intent");
                    let receipt = events.event_by_idempotency_key(&parent_index,
                        &format!("agent-tool-receipt:{}", bound.request.idempotency_key))
                        .unwrap().expect("actual selected original read receipt");
                    assert!(receipt.sequence <= head);
                    assert_eq!(receipt.stream_id, parent_index);
                    assert_eq!(receipt.kind, "execution.agent_tool.receipt");
                    assert_eq!(receipt.status.as_deref(), Some("completed"));
                    assert_eq!(receipt.payload["idempotency_key"], bound.request.idempotency_key);
                    assert!(measurement_start.elapsed() < std::time::Duration::from_secs(30),
                        "Own763 measurement exceeds bounded 30-second runtime gate");
                }
                let elapsed_ns = started.elapsed().as_nanos();
                let reads = counter.take_source_reads_763();
                assert_eq!(reads.len(), 2 * lookups);
                assert!(reads.iter().all(|(port, stream, records, bytes)|
                    port == "event_by_idempotency_key" && stream == &parent_index
                        && (*records == 1 || (*records == 0 && *bytes == 0))));
                assert_eq!(reads.iter().filter(|(_, _, records, _)| *records == 0).count(), lookups);
                assert_eq!(reads.iter().filter(|(_, _, records, _)| *records == 1).count(), lookups);
                eprintln!("Own763 {}", json!({"arm":"raw_exact_port_not_recovery",
                    "unrelated":unrelated, "selected_effect_keys":1, "queried_index_keys":2,
                    "lookup_pairs":lookups, "receipt_hits":lookups, "absent_intent_misses":lookups,
                    "recovery_calls":0, "elapsed_ns":elapsed_ns,
                    "counts_records_bytes_by_port_stream":summarize(&reads)}));
            }
            let before_cursor = events.durable_commit_cursor().unwrap();
            events.append(RuntimeEventInput {
                stream_id: parent_index.clone(), scope: RuntimeEventScope::ExecutionNode,
                kind: "test.763.index_tail".into(), status: None,
                actor: Some("governed_tool".into()), refs: vec![],
                payload: json!({"idempotency_key":"763-unselected-tail","negative_only":true}),
            }).unwrap();
            assert!(events.durable_commit_cursor().unwrap() > before_cursor);
            let closure = events.append_transaction(AppendTransactionRequest {
                transaction_id: format!("763-stale-closure-{unrelated}"),
                expected_streams: selected.into_iter().chain(std::iter::once(ExpectedStreamRevision {
                    stream_id: "763-must-not-close".into(),
                    expected_revision: 0,
                })).collect(),
                events: vec![RuntimeEventInput {
                    stream_id: "763-must-not-close".into(), scope: RuntimeEventScope::ExecutionNode,
                    kind: "test.763.must_not_close".into(), status: None, actor: None,
                    refs: vec![], payload: json!({}),
                }.into()],
            });
            assert!(matches!(closure, Err(RuntimeEventStoreError::StaleRevision { stream_id, .. })
                if stream_id == parent_index));
            assert_eq!(events.stream_revision("763-must-not-close").unwrap(), 0);
            let mut tail = events.list_stream(&parent_index).unwrap();
            tail.pop().unwrap();
            counter.replace_read(&parent_index, Some(tail));
            assert!(commits.lease_recovery_leaf_sources_with_fact(
                &graph, &spec.id, &binding, &spec, false, None,
            ).is_err(), "a frozen head with a missing tail must not close");
            counter.replace_read(&parent_index, None);
        }
        counter.replace_read(&parent_index, None);
        assert!(measurement_start.elapsed() < std::time::Duration::from_secs(30));
        eprintln!("r1_618 stage=source_checks_complete");
        assert!(!crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(&graph));
        eprintln!("r1_618 stage=withdraw_reap_start");
        worker.withdraw_and_require_rejected_child_709(&graph.id).await;
        eprintln!("r1_618 stage=physically_reaped_retirement_rejected");
    }

    #[tokio::test]
    async fn r1_643_native_claim_uses_original_principal_and_rejects_foreign_step_actors() {
        use harness_contract::step::CallSource;

        let (_workspace, services, owner) =
            crate::agentic::execution::reviewed_goal_fixture::native_raw_owner_fixture_for_test()
                .await;
        let native_run = owner.activity_binding().unwrap().agent_run_id.unwrap();
        let state = services.graph_state_store();
        let graphs = state
            .graph_ids()
            .unwrap()
            .into_iter()
            .map(|graph_id| state.load(&graph_id).unwrap())
            .collect::<Vec<_>>();
        let (graph, spec, packet, payload) = graphs
            .iter()
            .find_map(|graph| {
                let parent = graph.parent_execution.as_ref()?;
                let parent_graph = graphs
                    .iter()
                    .find(|graph| graph.id == parent.execution_id)?;
                let parent_node = parent_graph.nodes.iter().find(|node| {
                    node.id == parent.node_id && node.kind == ExecutionNodeKind::AgentTask
                })?;
                let packet: AgentTaskPacket =
                    serde_json::from_str(&parent_node.payload_ref).ok()?;
                if packet.run_id() != native_run {
                    return None;
                }
                for spec in &graph.nodes {
                    if spec.kind != ExecutionNodeKind::ToolBatch
                        || spec.executor_kind != "tool_batch"
                        || super::super::executors::agent_tool::is_bound_agent_tool_graph(graph)
                    {
                        continue;
                    }
                    let payload =
                        crate::step_snapshot::decode_persisted_tool_batch_value(&spec.payload_ref)
                            .ok()?;
                    if payload["calls"][0]["id"] == "native-claim"
                        && payload["calls"][0]["name"] == "task_claim"
                    {
                        return Some((graph.clone(), spec.clone(), packet, payload));
                    }
                }
                None
            })
            .expect("actual Native first claim must retain its registered Host and packet");
        let principal = packet.assignment.execution_identity.principal_id();
        assert_ne!(principal, packet.agent_id());
        assert_ne!(
            principal,
            packet.agentic_binding.as_ref().unwrap().agent_id.as_str()
        );
        assert!(packet.allowed_tools.iter().any(|tool| tool == "task_claim"));
        let (calls, _) = crate::host::decode_tool_batch(&spec.payload_ref).unwrap();
        let step = calls[0].binding.as_ref().unwrap();
        assert!(
            matches!(&step.origin.source, CallSource::Provider { attempt_id }
            if attempt_id == &step.snapshot.provider_attempt.attempt_id)
        );
        assert_eq!(step.snapshot.identity.actor_ref.as_deref(), Some(principal));
        assert_eq!(
            step.snapshot.identity.run_ref.as_deref(),
            Some(packet.run_id())
        );
        let parent = state.load(packet.graph_id()).unwrap();
        assert!(parent.node_statuses[packet.node_id()].is_terminal());
        let binding = state
            .current_binding(
                &graph,
                &spec.id,
                graph.recovery_cursor.node_attempts[&spec.id],
            )
            .unwrap();
        let commits = services.commit_service();
        let sources = commits
            .lease_recovery_leaf_sources_with_fact(&graph, &spec.id, &binding, &spec, false, None)
            .unwrap()
            .expect("real Native claim must close using its original principal after Task exit");
        let index = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let primary = format!("execution-effect:{}", step.effect_idempotency_key());
        for stream in [
            index,
            primary,
            packet.graph_id().to_string(),
            node_stream_id(packet.graph_id(), packet.node_id()),
        ] {
            assert!(sources.iter().any(|source| source.stream_id == stream));
        }
        let before = services.event_store().durable_commit_cursor().unwrap();
        for mutation in [
            "principal",
            "instance",
            "definition-agent",
            "run",
            "execution",
            "session",
            "turn",
            "attempt",
            "catalog",
            "allowlist",
            "provider-attempt",
            "process-source",
            "system-source",
            "offline-source",
        ] {
            let mut changed = payload.clone();
            let step_id = step.snapshot.identity.step_id.as_str();
            match mutation {
                "principal" => {
                    changed["step_snapshots"][step_id]["identity"]["actor_ref"] =
                        json!("foreign-principal")
                }
                "instance" => {
                    changed["step_snapshots"][step_id]["identity"]["actor_ref"] =
                        json!(packet.agent_id())
                }
                "definition-agent" => {
                    changed["step_snapshots"][step_id]["identity"]["actor_ref"] =
                        json!(packet.agentic_binding.as_ref().unwrap().agent_id)
                }
                "run" => {
                    changed["step_snapshots"][step_id]["identity"]["run_ref"] = json!("foreign-run")
                }
                "execution" => {
                    changed["step_snapshots"][step_id]["identity"]["execution_id"] =
                        json!("foreign-execution")
                }
                "session" => {
                    changed["step_snapshots"][step_id]["identity"]["session_id"] =
                        json!("foreign-session")
                }
                "turn" => {
                    changed["step_snapshots"][step_id]["identity"]["turn_id"] =
                        json!("foreign-turn")
                }
                "attempt" => {
                    changed["step_snapshots"][step_id]["identity"]["attempt"] =
                        json!(packet.attempt + 1)
                }
                "catalog" => changed["calls"][0]["origin"]["catalog_digest"] = json!("foreign"),
                "allowlist" => changed["calls"][0]["name"] = json!("not-in-original-packet"),
                "provider-attempt" => {
                    changed["calls"][0]["origin"]["source"]["attempt_id"] = json!("foreign")
                }
                "process-source" => {
                    changed["calls"][0]["origin"]["source"] =
                        json!({"kind":"process","worker_id":packet.run_id(),"sequence":1})
                }
                "system-source" => {
                    changed["calls"][0]["origin"]["source"] =
                        json!({"kind":"system_derived","parent_step_id":step_id});
                    changed["step_snapshots"][step_id]["context_refs"] =
                        json!([format!("step:{step_id}")]);
                }
                _ => changed["calls"][0]["origin"]["source"] = json!({"kind":"offline_test"}),
            }
            let mut rejected = spec.clone();
            rejected.payload_ref =
                crate::step_snapshot::encode_persisted_tool_batch(&changed).unwrap();
            assert!(
                commits
                    .delegated_host_leaf_owner(&graph, &rejected)
                    .is_err(),
                "{mutation}"
            );
        }
        assert_eq!(
            services.event_store().durable_commit_cursor().unwrap(),
            before
        );
        services.shutdown_maintenance().await;
    }

    #[test]
    fn r1_618_root_batch_keeps_own_index_without_borrowing_a_parent() {
        let events = Arc::new(RuntimeEventStore::for_test());
        let commits = ExecutionCommitService::new(events.clone());
        let graph = ExecutionGraph::new("618 original root batch");
        let payload = crate::step_snapshot::encode_persisted_tool_batch(&json!({
            "session_id":"618-root-session","step_snapshots":{},"calls":[]
        }))
        .unwrap();
        let spec = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        let binding = ExecutionNodeBinding {
            executor_kind: "tool_batch".into(),
            ticket_idempotency_key: spec.idempotency_key.clone(),
            attempt: 3,
            resource_lease_refs: vec![],
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let before = events.durable_commit_cursor().unwrap();
        let sources = commits
            .lease_recovery_leaf_sources_with_fact(&graph, &spec.id, &binding, &spec, false, None)
            .unwrap()
            .unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].stream_id,
            format!("execution-agent-receipts:{}:{}:3", graph.id, spec.id)
        );
        assert_eq!(sources[0].expected_revision, 0);
        assert_eq!(events.durable_commit_cursor().unwrap(), before);
    }

    #[test]
    fn r1_591_legacy_original_step_fences_primary_history_across_owner_indexes() {
        original_step_primary_race(false);
    }

    #[test]
    fn root_refusal_empty_cut_cannot_close_after_a_competing_original_call_intent() {
        original_step_primary_race(true);
    }

    fn original_step_primary_race(concurrent: bool) {
        use crate::execution_core::graph::executors::agent_tool::{
            no_dispatch_calls, NoDispatchFact,
        };
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("591-primary-race", workspace.path());
        let input = json!({"action":"write","path":"proof.txt","content":"original"});
        let effect = host.pin_snapshot().describe_effect("edit", &input);
        let snapshot = Arc::new(harness_contract::step::StepSnapshot {
            contract_version: harness_contract::step::STEP_CONTRACT_VERSION,
            identity: serde_json::from_value(json!({
                "step_id":"step-591","session_id":"session-591","turn_id":"turn-591",
                "execution_id":"legacy-591","build_ref":"test-591"
            }))
            .unwrap(),
            provider_attempt: harness_contract::step::ProviderAttemptIdentity {
                attempt_id: "attempt-591".into(),
                origin_step_id: "step-591".into(),
                model_profile_digest: "profile-591".into(),
                request_digest: "request-591".into(),
            },
            catalog_digest: "catalog-591".into(),
            tools: vec![],
            related_revisions: BTreeMap::new(),
            context_refs: vec![],
            expires_at_ms: chrono::Utc::now()
                .timestamp_millis()
                .saturating_add(3_600_000),
        });
        let binding = crate::step_snapshot::StepCallBinding::persisted(
            harness_contract::step::CallOrigin {
                origin_step_id: snapshot.identity.step_id.clone(),
                call_id: "call-591".into(),
                catalog_digest: snapshot.catalog_digest.clone(),
                source: harness_contract::step::CallSource::OfflineTest,
            },
            snapshot.clone(),
        );
        let payload = crate::step_snapshot::encode_persisted_tool_batch(&json!({
            "session_id":"session-591", "step_snapshots":{"step-591":snapshot},
            "calls":[{"id":"call-591","name":"edit","input":input.to_string(),
                "depends_on":[],"origin":binding.origin}]
        }))
        .unwrap();
        let ticket = super::super::registry::NodeExecutionTicket {
            graph_id: "legacy-591".into(),
            node_id: "batch-591".into(),
            executor_kind: "tool_batch".into(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: "batch-591".into(),
            payload_ref: payload,
        };
        let mut calls = no_dispatch_calls(&ticket).unwrap();
        assert_eq!(calls[0].effect_key, Some(binding.effect_idempotency_key()));
        calls[0].reason = "known preflight refusal".into();
        let fact = NoDispatchFact {
            ticket: ticket.clone(),
            calls,
            never_polled: false,
            result_sha256: None,
            historical_sources: BTreeMap::new(),
            start: None,
        };
        fact.validate_identity(&ticket).unwrap();
        for index in [
            fact.index_stream().unwrap(),
            "execution-agent-receipts:foreign:owner:7".into(),
        ] {
            let events = Arc::new(RuntimeEventStore::for_test());
            let commits = ExecutionCommitService::new(events.clone());
            let sources = commits
                .no_dispatch_physical_sources(&fact)
                .unwrap()
                .unwrap();
            let primary = format!("execution-effect:{}", binding.effect_idempotency_key());
            assert_eq!(sources.get(&primary), Some(&0));
            let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
                &crate::tool_dispatch::ToolRequest {
                    tool_use_id: "call-591".into(),
                    tool_name: "edit".into(),
                    input: input.to_string(),
                    depends_on: vec![],
                },
            );
            request.session_id = Some("session-591".into());
            request.idempotency_key = binding.effect_idempotency_key();
            request.step_binding = Some(binding.clone());
            let admission = if concurrent {
                // Deterministically insert the same canonical call's intent
                // after the consumer captured an empty physical cut, before
                // its publication CAS. No sleep or tool-name oracle.
                let gate = std::sync::Barrier::new(2);
                std::thread::scope(|scope| {
                    let producer = scope.spawn(|| {
                        gate.wait();
                        commits.begin_tool_effect(&request, &effect)
                    });
                    gate.wait();
                    producer.join().unwrap().unwrap()
                })
            } else {
                commits.begin_tool_effect(&request, &effect).unwrap()
            };
            assert!(matches!(admission, ToolEffectState::Fresh));
            let intent = events
                .event_by_idempotency_key(&primary, &format!("{}:intent", request.idempotency_key))
                .unwrap()
                .unwrap();
            assert_eq!(intent.kind, "execution.effect.intent");
            events
                .append(RuntimeEventInput {
                    stream_id: index.clone(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "test.591.owner_index_race".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: json!({"original_primary":primary}),
                })
                .unwrap();
            assert!(commits
                .no_dispatch_physical_sources(&fact)
                .unwrap()
                .is_none());
            let before = events.durable_commit_cursor().unwrap();
            let conflict = if index == fact.index_stream().unwrap() {
                index.clone()
            } else {
                primary.clone()
            };
            let mut closure_sources = sources;
            closure_sources.insert("test-591-no-false-closure".into(), 0);
            let closure = AppendTransactionRequest {
                transaction_id: "test-591-stale-closure".into(),
                expected_streams: closure_sources
                    .into_iter()
                    .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                        stream_id,
                        expected_revision,
                    })
                    .collect(),
                events: vec![RuntimeEventInput {
                    stream_id: "test-591-no-false-closure".into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "test.591.must_not_commit".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: json!({}),
                }
                .into()],
            };
            crate::validate_runtime_event_transaction(&closure).unwrap();
            let rejected = events.append_transaction(closure).unwrap_err();
            assert!(
                matches!(rejected,
                RuntimeEventStoreError::StaleRevision { stream_id, expected: 0, actual: 1 }
                    if stream_id == conflict),
                "{index}"
            );
            assert_eq!(events.durable_commit_cursor().unwrap(), before);
            assert_eq!(
                events.stream_revision("test-591-no-false-closure").unwrap(),
                0
            );
        }
    }

    #[tokio::test]
    async fn r1_450b_intent_requires_original_atomic_start_transaction_and_position() {
        let (events, running, ticket, _, calls) =
            super::super::tests::running_inline_attempt_450("450b atomic start identity").await;
        let commits = ExecutionCommitService::new(events.clone());
        let intent = events
            .event_by_idempotency_key(
                &ExecutionNodeBinding::effect_stream_id(&ticket),
                &format!("{}:intent", ticket.idempotency_key),
            )
            .unwrap()
            .unwrap();
        let cursor = events.durable_commit_cursor().unwrap();
        let start = validate_node_ticket_start(&events, &ticket, Some(&intent)).unwrap();
        assert_eq!(start.revision, running.revision);
        for fault in [
            "absent_transaction",
            "foreign_graph_transaction",
            "cursor",
            "index",
        ] {
            let mut altered = intent.clone();
            match fault {
                "absent_transaction" => altered.transaction_id = "450b-no-Graph-source".into(),
                "foreign_graph_transaction" => {
                    let mut other = ExecutionGraph::new("450b foreign Graph");
                    crate::test_support::attach_execution_graph_lineage(&mut other);
                    let receipt = commits.register_graph(other).unwrap();
                    altered.transaction_id = receipt.transaction.transaction_id;
                    altered.commit_cursor = receipt.transaction.commit_cursor;
                }
                "cursor" => altered.commit_cursor += 1,
                "index" => altered.transaction_index = 0,
                _ => unreachable!(),
            }
            let before = events.durable_commit_cursor().unwrap();
            assert!(
                validate_node_ticket_start(&events, &ticket, Some(&altered)).is_err(),
                "{fault}"
            );
            assert_eq!(events.durable_commit_cursor().unwrap(), before);
        }
        assert!(events.durable_commit_cursor().unwrap() >= cursor);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let mut graph = ExecutionGraph::new("450b damaged split-intent storage");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut spec = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "450b-corrupt",
        );
        spec.id = "450b-corrupt".into();
        graph.nodes.push(spec.clone());
        let graph = commits.register_graph(graph).unwrap().graph;
        let graph = commits
            .transition_node(
                &graph,
                &spec.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let corrupt_ticket = super::super::registry::NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: spec.id.clone(),
            executor_kind: spec.executor_kind.clone(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: spec.idempotency_key.clone(),
            payload_ref: spec.payload_ref.clone(),
        };
        let binding = ExecutionNodeBinding {
            executor_kind: corrupt_ticket.executor_kind.clone(),
            ticket_idempotency_key: corrupt_ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let damaged = commits
            .bind_and_start_node(&graph, &spec.id, binding)
            .unwrap()
            .graph;
        events
            .inject_content_transaction_fault(AppendTransactionRequest {
                transaction_id: "450b-stored-intent-without-Graph-source".into(),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: ExecutionNodeBinding::effect_stream_id(&corrupt_ticket),
                    expected_revision: 0,
                }],
                events: vec![execution_effect_intent_event(&corrupt_ticket).unwrap()],
            })
            .unwrap();
        let before = events.durable_commit_cursor().unwrap();
        let error = commits
            .inspect_execution_effect(&corrupt_ticket)
            .unwrap_err();
        assert!(
            error.to_string().contains("no exact Graph start source"),
            "{error}"
        );
        assert_eq!(events.durable_commit_cursor().unwrap(), before);
        assert_eq!(
            ExecutionGraphStateStore::new(events.clone())
                .load(&damaged.id)
                .unwrap(),
            damaged
        );
        assert_eq!(
            events
                .stream_revision(&ExecutionNodeBinding::effect_stream_id(&corrupt_ticket))
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn r1_450_original_process_fact_unknown_or_unreaped_cannot_close_binding() {
        use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionTerminal};
        let (original, effect) = super::super::tests::recover_real_bound_leaf_450(true).await;
        assert!(ExecutionCommitService::lease_recovery_process_closed(
            &original, &effect
        ));
        // Physical closure is quiescence. The hardening / network isolation /
        // status-channel flags are audit facts that the evidence producer
        // already enforces, so they are deliberately *not* closure criteria.
        for fault in [
            "unknown",
            "missing_exit",
            "both_exit",
            "children",
            "init",
            "missing_fact",
        ] {
            let mut altered = original.clone();
            if fault == "missing_fact" {
                altered.observed_evidence.clear();
            } else {
                let fact = altered
                    .observed_evidence
                    .iter_mut()
                    .find_map(|observed| match &mut observed.target {
                        EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
                        _ => None,
                    })
                    .unwrap();
                if fault == "unknown" {
                    fact.terminal = ToolExecutionTerminal::Unknown {
                        reason_code: "original_exit_unknown".into(),
                    };
                } else if let ToolExecutionTerminal::Settled {
                    exit_code,
                    signal,
                    closure,
                    ..
                } = &mut fact.terminal
                {
                    match fault {
                        "missing_exit" => {
                            *exit_code = None;
                            *signal = None;
                        }
                        "both_exit" => {
                            *exit_code = Some(0);
                            *signal = Some(9);
                        }
                        "children" => closure.all_children_reaped = false,
                        _ => closure.init_reaped = false,
                    }
                }
            }
            assert!(
                !ExecutionCommitService::lease_recovery_process_closed(&altered, &effect),
                "{fault}"
            );
        }
    }
}

/// F2: a host-shell process effect closes physical closure on quiescence
/// alone, and stays open when the quiescence fact is missing.
#[cfg(test)]
mod host_process_quiescence_tests {
    use super::*;
    use crate::host_process_closure::{
        build_host_process_fact, host_process_observation, read_host_process_report,
        HOST_PROCESS_BACKEND,
    };
    use harness_contract::context::{
        ArtifactRef, EvidenceTargetIdentity, ToolExecutionStopReason, ToolExecutionTerminal,
    };

    fn bash_effect() -> ToolEffectDescriptor {
        let effect = tools::ToolHost::builtin("r1-890-host-process", std::env::temp_dir())
            .pin_snapshot()
            .describe_effect("bash", &json!({"command": "printf r1-890"}));
        assert!(effect.spawns_process && !effect.uses_network);
        effect
    }

    fn request() -> crate::RuntimeToolExecutionRequest {
        crate::RuntimeToolExecutionRequest {
            compiled_invocation: None,
            step_binding: None,
            governed_plan_id: "plan".to_string(),
            governed_plan_revision: 1,
            observation_wave_sequence: 7,
            idempotency_key: "r1-890-host-process-call".to_string(),
            tool_use_id: "r1-890-host-process-call".to_string(),
            tool_name: "bash".to_string(),
            input: json!({"command": "printf r1-890"}).to_string(),
            category: crate::ToolSafetyCategory::Destructive,
            authorization: None,
            session_id: Some("r1-890-session".to_string()),
            sandbox_posture: harness_contract::policy::SandboxPosture::HostFullAccess,
            policy_revision: 1,
            authorized_scopes: Vec::new(),
            memory_context: None,
            reality_context: None,
            model_lease: None,
            parent_execution: None,
            parent_execution_attempt: None,
            execution_decision: None,
            evaluation_isolated: false,
            managed_invocation: None,
            tool_progress: crate::ToolProgressSink::default(),
        }
    }

    fn artifact(selector: &str) -> ArtifactRef {
        ArtifactRef::durable(
            selector,
            format!("sha256:{}", "a".repeat(64)),
            12,
            "application/json",
            "session:r1-890-session",
        )
    }

    /// A host executor result that reports a fully reaped foreground command.
    fn bash_output(closure: serde_json::Value) -> String {
        json!({
            "stdout": "r1-890",
            "stderr": "",
            "returnTruncated": false,
            "processClosure": closure,
        })
        .to_string()
    }

    fn host_outcome(closure: serde_json::Value) -> crate::RuntimeToolExecutionOutcome {
        host_outcome_from(bash_output(closure))
    }

    /// A host executor result that reports no closure slot at all.
    fn host_outcome_without_closure() -> crate::RuntimeToolExecutionOutcome {
        host_outcome_from(
            json!({"stdout": "r1-890", "stderr": "", "returnTruncated": false}).to_string(),
        )
    }

    fn host_outcome_from(output: String) -> crate::RuntimeToolExecutionOutcome {
        let request = request();
        let effect = bash_effect();
        let report = read_host_process_report(Some(&output));
        let mut admission = crate::host_process_closure::test_admission(&request);
        admission.source_bundle = artifact("artifact://r1-890-source");
        let fact = build_host_process_fact(
            &request,
            &effect,
            &report,
            artifact("artifact://r1-890-body"),
            admission,
        )
        .expect("host process fact");
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(output),
            error: None,
            evidence_ref: "tool-effect:r1-890-host-process-call".to_string(),
            observed_evidence: vec![host_process_observation(&request, fact)],
        }
    }

    #[test]
    fn r1_890_host_process_quiescence_closes_physical_closure_without_hardening() {
        let effect = bash_effect();
        let outcome = host_outcome(json!({
            "exitCode": 0, "signal": null, "initReaped": true, "allChildrenReaped": true,
        }));
        let EvidenceTargetIdentity::ToolExecution { execution } = &outcome.observed_evidence[0].target
        else {
            panic!("host process evidence missing")
        };
        assert!(matches!(
            &execution.terminal,
            ToolExecutionTerminal::Settled {
                exit_code: Some(0),
                signal: None,
                stop_reason: ToolExecutionStopReason::CommandExit,
                closure,
            } if closure.backend == HOST_PROCESS_BACKEND
                && closure.all_children_reaped
                && closure.init_reaped
                // The host shell proves no isolation of any kind.
                && !closure.kernel_hardening_applied
                && !closure.network_disabled
                && !closure.project_read_only
                && !closure.status_channel_protected
        ));
        assert!(execution.capture_complete);
        assert!(
            ExecutionCommitService::lease_recovery_process_closed(&outcome, &effect),
            "a reaped host shell with quiescence facts must close"
        );
        assert!(crate::execution_projection::settlement::process_closed(
            &outcome, &effect
        ));
        // A non-zero exit is still quiescent: the process tree is still gone.
        let failed = host_outcome(json!({
            "exitCode": 7, "signal": null, "initReaped": true, "allChildrenReaped": true,
        }));
        assert!(ExecutionCommitService::lease_recovery_process_closed(
            &failed, &effect
        ));
        // A signal-terminated command is quiescent as well.
        let signalled = host_outcome(json!({
            "exitCode": null, "signal": 9, "initReaped": true, "allChildrenReaped": true,
        }));
        assert!(matches!(
            &signalled.observed_evidence[0].target,
            EvidenceTargetIdentity::ToolExecution { execution }
                if matches!(&execution.terminal, ToolExecutionTerminal::Settled {
                    exit_code: None,
                    signal: Some(9),
                    ..
                })
        ));
        assert!(ExecutionCommitService::lease_recovery_process_closed(
            &signalled, &effect
        ));
    }

    #[test]
    fn r1_890_host_process_without_quiescence_facts_never_closes() {
        let effect = bash_effect();
        for (case, outcome) in [
            ("absent", host_outcome_without_closure()),
            ("null", host_outcome(serde_json::Value::Null)),
        ] {
            let EvidenceTargetIdentity::ToolExecution { execution } =
                &outcome.observed_evidence[0].target
            else {
                panic!("host process evidence missing")
            };
            assert!(
                matches!(&execution.terminal, ToolExecutionTerminal::Unknown { reason_code } if !reason_code.is_empty()),
                "{case}"
            );
            assert!(
                !ExecutionCommitService::lease_recovery_process_closed(&outcome, &effect),
                "{case}"
            );
            assert!(
                !crate::execution_projection::settlement::process_closed(&outcome, &effect),
                "{case}"
            );
        }
        // A report that cannot prove quiescence stays open.
        for (case, closure) in [
            (
                "children_not_reaped",
                json!({"exitCode": 0, "signal": null, "initReaped": true, "allChildrenReaped": false}),
            ),
            (
                "init_not_reaped",
                json!({"exitCode": 0, "signal": null, "initReaped": false, "allChildrenReaped": true}),
            ),
            (
                "ambiguous_exit",
                json!({"exitCode": 0, "signal": 9, "initReaped": true, "allChildrenReaped": true}),
            ),
            (
                "no_exit_at_all",
                json!({"exitCode": null, "signal": null, "initReaped": true, "allChildrenReaped": true}),
            ),
        ] {
            let outcome = host_outcome(closure);
            assert!(
                !ExecutionCommitService::lease_recovery_process_closed(&outcome, &effect),
                "{case}"
            );
            assert!(
                !crate::execution_projection::settlement::process_closed(&outcome, &effect),
                "{case}"
            );
        }
        // No process terminal at all is not a closure either.
        let mut missing = host_outcome(json!({
            "exitCode": 0, "signal": null, "initReaped": true, "allChildrenReaped": true,
        }));
        missing.observed_evidence.clear();
        assert!(!ExecutionCommitService::lease_recovery_process_closed(
            &missing, &effect
        ));
        assert!(!crate::execution_projection::settlement::process_closed(
            &missing, &effect
        ));
    }

    #[test]
    fn r1_890_host_process_security_flags_are_audit_only_for_closure() {
        let effect = bash_effect();
        let mut outcome = host_outcome(json!({
            "exitCode": 0, "signal": null, "initReaped": true, "allChildrenReaped": true,
        }));
        for observed in &mut outcome.observed_evidence {
            let EvidenceTargetIdentity::ToolExecution { execution } = &mut observed.target else {
                panic!("host process evidence missing")
            };
            let ToolExecutionTerminal::Settled { closure, .. } = &mut execution.terminal else {
                panic!("host process terminal missing")
            };
            // The audit flags may change freely: they are enforced where the
            // evidence is produced, never by the closure criterion.
            closure.kernel_hardening_applied = true;
            closure.network_disabled = true;
            closure.project_read_only = true;
            closure.status_channel_protected = true;
        }
        assert!(ExecutionCommitService::lease_recovery_process_closed(
            &outcome, &effect
        ));
        assert!(crate::execution_projection::settlement::process_closed(
            &outcome, &effect
        ));
    }

    #[test]
    fn r1_890_host_process_fact_may_not_claim_the_private_bounded_capability() {
        let effect = bash_effect();
        let request = request();
        let report = read_host_process_report(Some(&bash_output(json!({
            "exitCode": 0, "signal": null, "initReaped": true, "allChildrenReaped": true,
        }))));
        let mut admission = crate::host_process_closure::test_admission(&request);
        admission.source_bundle = artifact("artifact://r1-890-source");
        let fact = build_host_process_fact(
            &request,
            &effect,
            &report,
            artifact("artifact://r1-890-body"),
            admission,
        )
        .expect("host process fact");
        let mut forged = fact.clone();
        let ToolExecutionTerminal::Settled { closure, .. } = &mut forged.terminal else {
            panic!("host process terminal missing")
        };
        closure.kernel_hardening_applied = true;
        assert!(crate::host_process_closure::validate_host_process_fact(&forged).is_err());
        let mut foreign = fact;
        let ToolExecutionTerminal::Settled { closure, .. } = &mut foreign.terminal else {
            panic!("host process terminal missing")
        };
        closure.backend = "bubblewrap+landlock+seccomp+pid1".into();
        closure.namespace_inode = 7;
        assert!(crate::host_process_closure::validate_host_process_fact(&foreign).is_err());
    }
}
