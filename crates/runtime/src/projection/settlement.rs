use super::*;
use crate::DurableRuntimeEvent;
use harness_contract::execution_graph::ExecutionNodeKind;

fn terminal_index_observation(
    index: &DurableRuntimeEvent,
    stream: &str,
    next: u64,
    head: u64,
) -> Result<Option<u64>, RuntimeServicesError> {
    crate::agentic::validate_receipt_index_record(index, stream, next, head)
        .map_err(|_| RuntimeServicesError::Invariant("terminal effect index source mismatch".into()))?;
    Ok(crate::ExecutionCommitService::receipt_observation_sequence(index))
}

#[cfg(test)]
mod terminal_continuation_tests {
    use super::*;

    fn append(services: &RuntimeServices, stream: &str) {
        services.event_store().append(crate::RuntimeEventInput {
            stream_id: stream.into(), scope: RuntimeEventScope::ExecutionNode,
            kind: "terminal.fence.probe".into(), status: None, actor: None,
            refs: Vec::new(), payload: serde_json::json!({}),
        }).unwrap();
    }

    #[tokio::test]
    async fn own865_terminal_scoped_refresh_survives_unrelated_append_but_rejects_source_append() {
        let services = RuntimeServices::in_memory().unwrap();
        let cut = services.event_store().durable_commit_cursor().unwrap();
        let proof = history::TerminalBindingProof {
            identity: "original-test-binding".into(), through: cut,
            read: ClosureRead { complete: true, physical_sources: vec![("terminal-owned".into(), 0)],
                verified_cursor: Some(cut), ..Default::default() },
        };
        append(&services, "terminal-unrelated");
        let next = services.event_store().durable_commit_cursor().unwrap();
        let mut budget = history::RequestBudget::default();
        let refreshed = refresh_binding(&services, &proof, next, &mut budget).unwrap().unwrap();
        assert_eq!(refreshed.verified_cursor, Some(next));
        append(&services, "terminal-owned");
        assert!(refresh_binding(&services, &proof, next, &mut budget).unwrap().is_none());
        services.shutdown_execution().await;
        services.shutdown_maintenance().await;
    }

    #[tokio::test]
    async fn own865_terminal_final_cut_and_mutable_lease_are_really_rechecked_after_prepayment() {
        let services = RuntimeServices::in_memory().unwrap();
        let cut = services.event_store().durable_commit_cursor().unwrap();
        let mut read = ClosureRead {
            complete: true, verified_cursor: Some(cut), ..Default::default()
        };
        let mut budget = history::RequestBudget::default();
        assert!(budget.prepay_terminal_recheck(&read).unwrap());
        let before = budget.consumed_work();
        append(&services, "terminal-race");
        assert!(!unchanged(&services, &read, &mut budget).unwrap());
        assert_eq!(budget.consumed_work(), before);
        read.verified_cursor = Some(services.event_store().durable_commit_cursor().unwrap());
        read.leases.push((format!("resource-lease:{}", uuid::Uuid::new_v4()), Some(false)));
        assert!(budget.prepay_terminal_recheck(&read).unwrap());
        let before = budget.consumed_work();
        assert!(!unchanged(&services, &read, &mut budget).unwrap());
        assert_eq!(budget.consumed_work(), before);
        services.shutdown_execution().await;
        services.shutdown_maintenance().await;
    }

    #[tokio::test]
    async fn own865_terminal_multiple_roots_prepay_mixed_cut_and_scoped_heads_cumulatively() {
        let services = RuntimeServices::in_memory().unwrap();
        let cut = services.event_store().durable_commit_cursor().unwrap();
        let first = ClosureRead { complete: true, verified_cursor: Some(cut), ..Default::default() };
        let second = ClosureRead {
            complete: true, physical_sources: vec![("terminal-second-root".into(), 0)],
            ..Default::default()
        };
        let mut budget = history::RequestBudget::default();
        assert!(budget.prepay_terminal_recheck(&first).unwrap());
        let first_cost = budget.consumed_work();
        assert!(budget.prepay_terminal_recheck(&second).unwrap());
        assert!(budget.consumed_work().0 > first_cost.0);
        let mut aggregate = ClosureRead { complete: true, ..Default::default() };
        aggregate.merge(first);
        aggregate.merge(second);
        let before = budget.consumed_work();
        assert!(unchanged(&services, &aggregate, &mut budget).unwrap());
        assert_eq!(budget.consumed_work(), before);
        services.shutdown_execution().await;
        services.shutdown_maintenance().await;
    }
}

#[cfg(test)]
mod terminal_index_schema_tests {
    use super::*;

    fn original_index(sequence: u64, base: u64, wave: u64, effect: &str) -> DurableRuntimeEvent {
        DurableRuntimeEvent {
            event_id: "terminal-index-original".into(),
            stream_id: "execution-agent-receipts:graph:node:1".into(),
            sequence,
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution.agent_tool.receipt".into(),
            status: Some("completed".into()),
            actor: Some("governed_tool".into()),
            payload: serde_json::json!({
                "idempotency_key":"terminal-index-call",
                "sequence":base + 1, "sequence_base":base,
                "index_revision_before":sequence - 1,
                "sequence_owner":"original_receipt_index",
                "source_observation_wave_sequence":wave,
                "effect_kind":effect
            }),
            refs: Vec::new(), created_at_ms: 1, commit_cursor: 1,
            transaction_id: "terminal-index-transaction".into(), transaction_index: 1,
            schema_version: 2,
            idempotency_key: Some("agent-tool-receipt:terminal-index-call".into()),
        }
    }

    #[test]
    fn own865_terminal_index_schema2_read_keeps_source_wave_distinct_from_canonical_sequence() {
        let index = original_index(1, 0, 2, "read");
        assert_eq!(terminal_index_observation(&index, &index.stream_id, 1, 1).unwrap(), Some(2));
        assert_eq!(index.payload["sequence"], 1);
    }

    #[test]
    fn own865_terminal_index_schema2_process_and_legacy_intent_keep_their_own_versions() {
        let index = original_index(3, 5, 2, "process");
        assert_eq!(terminal_index_observation(&index, &index.stream_id, 3, 3).unwrap(), Some(2));
        let mut legacy = index.clone();
        legacy.schema_version = 1;
        legacy.payload["sequence"] = serde_json::json!(7);
        assert_eq!(terminal_index_observation(&legacy, &legacy.stream_id, 3, 3).unwrap(), Some(7));
        legacy.kind = "execution.agent_tool.intent".into();
        legacy.idempotency_key = Some("agent-tool-intent:terminal-index-call".into());
        assert_eq!(terminal_index_observation(&legacy, &legacy.stream_id, 3, 3).unwrap(), None);
        legacy.schema_version = 2;
        assert!(terminal_index_observation(&legacy, &legacy.stream_id, 3, 3).is_err());
    }

    #[test]
    fn own865_terminal_index_schema2_foreign_owner_and_wrong_source_still_reject() {
        let original = original_index(1, 0, 2, "read");
        let mut forged = original.clone();
        forged.payload["sequence_owner"] = serde_json::json!("foreign_owner");
        assert!(terminal_index_observation(&forged, &original.stream_id, 1, 1).is_err());
        forged = original.clone();
        forged.actor = Some("foreign_actor".into());
        assert!(terminal_index_observation(&forged, &original.stream_id, 1, 1).is_err());
        assert!(terminal_index_observation(&original, "foreign-index", 1, 1).is_err());
    }

    #[test]
    fn own865_terminal_index_schema2_bad_causal_revision_missing_wave_and_unknown_version_reject() {
        let original = original_index(3, 5, 2, "process");
        for fault in ["revision", "causal", "wave", "version"] {
            let mut forged = original.clone();
            match fault {
                "revision" => forged.payload["index_revision_before"] = serde_json::json!(3),
                "causal" => forged.payload["sequence"] = serde_json::json!(5),
                "wave" => { forged.payload.as_object_mut().unwrap().remove("source_observation_wave_sequence"); }
                _ => forged.schema_version = 3,
            }
            assert!(terminal_index_observation(&forged, &original.stream_id, 3, 3).is_err(), "{fault}");
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClosureRead {
    pub complete: bool,
    pub busy: bool,
    pub refs: Vec<String>,
    pub leases: Vec<(String, Option<bool>)>,
    physical_sources: Vec<(String, u64)>,
    #[serde(default)]
    verified_cursor: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    verified_cuts: BTreeSet<u64>,
}

impl ClosureRead {
    pub(super) fn valid_binding_cut(&self, through: u64) -> bool {
        self.verified_cuts.is_empty() && self.verified_cursor.is_none_or(|cursor| cursor == through)
    }
    pub(super) fn merge(&mut self, other: Self) {
        self.complete &= other.complete;
        self.busy |= other.busy;
        if let Some(cursor) = self.verified_cursor.take() {
            self.verified_cuts.insert(cursor);
        }
        self.verified_cuts.extend(other.verified_cuts);
        if let Some(cursor) = other.verified_cursor {
            self.verified_cuts.insert(cursor);
        } else {
            self.physical_sources.extend(other.physical_sources);
        }
        self.refs.extend(other.refs);
        self.leases.extend(other.leases);
    }

    pub(super) fn recheck_work(&self) -> Result<Vec<(String, usize, usize)>, RuntimeServicesError> {
        let mut work = BTreeMap::new();
        if let Some(cursor) = self.verified_cursor {
            work.insert(format!("terminal-cut:{cursor}"), (1, 20));
        } else {
            for (stream, revision) in &self.physical_sources {
                work.insert(format!("terminal-head:{stream}:{revision}"), (1, stream.len().saturating_add(20)));
            }
        }
        for cursor in &self.verified_cuts {
            work.insert(format!("terminal-cut:{cursor}"), (1, 20));
        }
        for (reference, previous) in &self.leases {
            let identity = serde_json::to_string(&(reference, previous))
                .map_err(|_| RuntimeServicesError::Invariant("terminal recheck identity".into()))?;
            work.insert(format!("terminal-lease:{identity}"), (1, 0));
        }
        Ok(work.into_iter().map(|(identity, (records, bytes))| (identity, records, bytes)).collect())
    }
}

#[cfg(test)]
mod all_binding_closure_tests {
    use super::*;
    use crate::execution_core::graph::{ExecutionNodeBinding, NodeExecutionTicket};
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionNodeSpec, ExecutionNodeStatus,
    };

    #[tokio::test]
    async fn r1_478_empty_binding_without_durable_callback_closure_is_incomplete() {
        let services = RuntimeServices::in_memory().unwrap();
        let mut graph = ExecutionGraph::new("empty handles do not prove callback closure");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let node =
            ExecutionNodeSpec::new(ExecutionNodeKind::Materialize, "materialize", "fixture:478");
        graph.nodes.push(node.clone());
        let graph = services
            .commit_service()
            .register_graph(graph)
            .unwrap()
            .graph;
        let graph = services
            .commit_service()
            .transition_node(
                &graph,
                &node.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id.clone(),
            executor_kind: node.executor_kind.clone(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let graph = services
            .commit_service()
            .bind_and_start_nodes(
                &graph,
                BTreeMap::from([(node.id.clone(), binding.clone())]),
                vec![ticket.clone()],
            )
            .unwrap()
            .graph;
        let position = services
            .graph_state_store()
            .binding_start_position(&graph, &node.id, &binding)
            .unwrap()
            .unwrap();
        let through = services.event_store().durable_commit_cursor().unwrap();
        let slot = format!("{}:1:{}", node.id, ticket.idempotency_key);
        let mut state = history::HistoryState {
            through,
            graph_state: Some(graph.clone()),
            ..Default::default()
        };
        state
            .bindings
            .insert(slot.clone(), (node.id.clone(), binding.clone()));
        state
            .start_positions
            .insert(slot.clone(), (position.1, position.2));
        state.start_specs.insert(slot.clone(), node.clone());
        state.start_tickets.insert(slot.clone(), ticket);
        assert!(starts_covered(&state));
        assert!(known_executor(&node));
        let mut budget = history::RequestBudget::default();
        assert!(!durable_binding_leases_closed(
            &services,
            &state,
            &slot,
            &node.id,
            &binding,
            through,
            &mut budget,
            &mut Vec::new(),
            &mut None,
        )
        .unwrap());
        let observed = read(
            &services,
            &BTreeMap::from([(graph.id, state)]),
            through,
            &mut budget,
        )
        .unwrap();
        assert!(!observed.complete);
        assert!(!observed.busy);
        assert!(observed.leases.is_empty());
        assert!(observed.refs.is_empty());
        services.shutdown_execution().await;
        services.shutdown_maintenance().await;
    }

    #[test]
    fn a_moved_closure_source_keeps_its_retryable_class_through_the_scope_mapping() {
        use crate::execution_core::graph::{ExecutionCommitError, ExecutionRunnerError};

        let moved = ExecutionCommitError::PhysicalClosureSourceMoved {
            stream_id: "execution-effect:shape-moved".into(),
        };
        let mapped = scope_read_error(moved);
        assert!(
            !matches!(&mapped, RuntimeServicesError::Invariant(_)),
            "a source that moved under the proof is a wait, not an invariant violation: {mapped}"
        );
        assert!(
            !matches!(
                &mapped,
                RuntimeServicesError::GraphRunner(ExecutionRunnerError::Commit(
                    ExecutionCommitError::InvalidCommand(_)
                ))
            ),
            "the wait is not flattened into an invalid command: {mapped}"
        );
        let kept = match &mapped {
            RuntimeServicesError::GraphRunner(ExecutionRunnerError::Commit(kept)) => kept,
            other => panic!("the moved source crosses with its type intact: {other}"),
        };
        assert!(
            matches!(
                kept,
                ExecutionCommitError::PhysicalClosureSourceMoved { stream_id }
                    if stream_id == "execution-effect:shape-moved"
            ),
            "the stream that moved travels with the verdict: {kept}"
        );
        assert!(
            kept.keeps_the_closure_waiting(),
            "the wait still classifies as retryable after the mapping: {kept}"
        );
    }
}

pub(super) fn local_reference(reference: &str) -> bool {
    reference.starts_with("resource-lease:")
        || reference.starts_with("scope-lease:")
        || reference.starts_with("worktree-lease:")
}

fn read_lease(
    services: &RuntimeServices,
    reference: &str,
) -> Result<Option<bool>, RuntimeServicesError> {
    if reference.starts_with("resource-lease:") {
        services
            .resource_manager()
            .lease_is_active(reference)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    } else if reference.starts_with("scope-lease:") {
        services
            .scope_locks()
            .lease_is_active(reference)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
    } else if let Some(identity) = reference.strip_prefix("worktree-lease:") {
        let Ok(identity) = uuid::Uuid::parse_str(identity) else {
            return Ok(None);
        };
        let record = services
            .worktree_leases()
            .record(identity)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        Ok(record
            .filter(|record| record.workspace_id == services.workspace_key())
            .map(|record| {
                record.status == crate::execution_core::graph::WorktreeLeaseStatus::Active
            }))
    } else {
        Ok(None)
    }
}

pub(super) fn unchanged(
    services: &RuntimeServices,
    read: &ClosureRead,
    budget: &mut history::RequestBudget,
) -> Result<bool, RuntimeServicesError> {
    let mut cuts = read.verified_cuts.clone();
    cuts.extend(read.verified_cursor);
    for cursor in cuts {
        if !budget.take_terminal_recheck(&format!("terminal-cut:{cursor}")) {
            budget.terminal_preflight(1, 20)?;
            budget.charge(1, 20)?;
        }
        if services.event_store().durable_commit_cursor()? != cursor {
            return Ok(false);
        }
    }
    if read.verified_cursor.is_none() {
        let mut seen = BTreeSet::new();
        for (stream, revision) in &read.physical_sources {
            if !seen.insert((stream, revision)) {
                continue;
            }
            let actual = if budget.take_terminal_recheck(&format!("terminal-head:{stream}:{revision}")) {
                services.event_store().stream_revision(stream)?
            } else {
                budget.uncached_stream_head(services, stream)?
            };
            if actual != *revision {
                return Ok(false);
            }
        }
    }
    let mut seen = BTreeSet::new();
    for (reference, previous) in &read.leases {
        if !seen.insert((reference, previous)) {
            continue;
        }
        let identity = serde_json::to_string(&(reference, previous))
            .map_err(|_| RuntimeServicesError::Invariant("terminal recheck identity".into()))?;
        if !budget.take_terminal_recheck(&format!("terminal-lease:{identity}")) {
            budget.terminal_preflight(1, 0)?;
            budget.charge(1, 0)?;
        }
        if read_lease(services, reference)? != *previous {
            return Ok(false);
        }
    }
    Ok(true)
}

fn known_executor(spec: &harness_contract::execution_graph::ExecutionNodeSpec) -> bool {
    match spec.kind {
        ExecutionNodeKind::InlineModel => spec.executor_kind == "inline_model",
        ExecutionNodeKind::ToolBatch => spec.executor_kind == "tool_batch",
        ExecutionNodeKind::AgentTask => spec.executor_kind == "agent_task",
        ExecutionNodeKind::Verify => matches!(
            spec.executor_kind.as_str(),
            "verify" | "compile_target_guard" | "agentic_program_wait"
        ),
        ExecutionNodeKind::Synthesize => matches!(
            spec.executor_kind.as_str(),
            "synthesize" | "agentic_program_wait"
        ),
        ExecutionNodeKind::Materialize => spec.executor_kind == "materialize",
        ExecutionNodeKind::Approval => spec.executor_kind == "approval",
        ExecutionNodeKind::SessionDispatch => spec.executor_kind == "session_dispatch",
        ExecutionNodeKind::Timer => spec.executor_kind == "timer",
    }
}

pub(crate) fn process_closed(
    outcome: &crate::RuntimeToolExecutionOutcome,
    effect: &harness_contract::tool::ToolEffectDescriptor,
) -> bool {
    use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionTerminal};
    if outcome.observed_evidence.iter().any(|observed| matches!(
        observed.target, EvidenceTargetIdentity::LocalNetwork { .. }
    )) {
        return crate::ExecutionCommitService::local_network_closed(outcome, effect);
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
        return crate::ExecutionCommitService::local_network_closed(outcome, effect);
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
                // Same criterion as the physical closure witness: physical
                // closure is quiescence only. Security posture is enforced at
                // the evidence producer, not here.
                exit_code.is_some() != signal.is_some()
                    && closure.all_children_reaped
                    && closure.init_reaped
            }
            _ => false,
        }
}

fn primary(
    services: &RuntimeServices,
    stream: &str,
    key: &str,
    through: u64,
    budget: &mut history::RequestBudget,
) -> Result<Option<std::sync::Arc<DurableRuntimeEvent>>, RuntimeServicesError> {
    budget.canonical_record(services, stream, key, through)
}

fn commit_read_error(
    error: RuntimeServicesError,
) -> crate::execution_core::graph::ExecutionCommitError {
    match error {
        RuntimeServicesError::EventStore(error) => {
            crate::execution_core::graph::ExecutionCommitError::EventStore(error)
        }
        error => {
            crate::execution_core::graph::ExecutionCommitError::InvalidCommand(error.to_string())
        }
    }
}

fn scope_read_error(
    error: crate::execution_core::graph::ExecutionCommitError,
) -> RuntimeServicesError {
    match error {
        crate::execution_core::graph::ExecutionCommitError::EventStore(error) => {
            RuntimeServicesError::EventStore(error)
        }
        // A source that moved under the closure proof is the owner still
        // writing: it is a wait, not an invariant violation. It crosses with
        // its type and the stream that moved intact, so the layer above can
        // still tell that it may be waited on.
        error @ crate::execution_core::graph::ExecutionCommitError::PhysicalClosureSourceMoved { .. } => {
            RuntimeServicesError::GraphRunner(
                crate::execution_core::graph::ExecutionRunnerError::Commit(error),
            )
        }
        error => RuntimeServicesError::Invariant(error.to_string()),
    }
}

struct ProjectionScopeReader<'request> {
    services: &'request RuntimeServices,
    budget: &'request mut history::RequestBudget,
    through: u64,
}

impl crate::execution_core::graph::TerminalScopeReader for ProjectionScopeReader<'_> {
    fn validate_external_outcome_binding(&mut self, session: &str, graph_id: &str, node_id: &str,
        value: &str, source: &DurableRuntimeEvent,
    ) -> Result<(), crate::execution_core::graph::ExecutionCommitError> {
        use crate::execution_core::graph::{executors::agent_tool, ExecutionCommitError};
        let invalid = || ExecutionCommitError::InvalidCommand("original ToolBatch content identity mismatch".into());
        let encoded = value.strip_prefix(agent_tool::AGENT_TOOL_OUTCOME_REFERENCE).ok_or_else(invalid)?;
        let reference: agent_tool::AgentToolOutcomeReference = serde_json::from_str(encoded).map_err(|_| invalid())?;
        let selected = self.budget.canonical_record(self.services, &reference.stream_id,
            &reference.idempotency_key, self.through).map_err(commit_read_error)?.ok_or_else(invalid)?;
        if selected.as_ref() != source { return Err(invalid()); }
        crate::execution_core::graph::validate_external_outcome_binding_from_source(
            self.services.event_store(), session, graph_id, node_id, value, source,
        )
    }
    fn event(
        &mut self,
        stream: &str,
        key: &str,
    ) -> Result<
        Option<std::sync::Arc<DurableRuntimeEvent>>,
        crate::execution_core::graph::ExecutionCommitError,
    > {
        self.budget
            .canonical_record(self.services, stream, key, self.through)
            .map_err(commit_read_error)
    }

    fn fence(
        &mut self,
        transaction: &str,
        stream: &str,
    ) -> Result<
        Option<crate::CommittedStreamRevision>,
        crate::execution_core::graph::ExecutionCommitError,
    > {
        self.budget.terminal_fence(self.services, transaction, stream).map_err(commit_read_error)
    }
}

fn physical_receipt_closed(
    services: &RuntimeServices,
    receipt: &DurableRuntimeEvent,
    outcome: &crate::RuntimeToolExecutionOutcome,
    effect: &harness_contract::tool::ToolEffectDescriptor,
    through: u64,
    budget: &mut history::RequestBudget,
) -> Result<bool, RuntimeServicesError> {
    crate::ExecutionCommitService::validate_local_network_receipt(receipt, effect, &mut ProjectionScopeReader {services,budget,through})
        .map_err(scope_read_error)?;
    use harness_contract::context::EvidenceTargetIdentity;
    let facts = outcome
        .observed_evidence
        .iter()
        .filter_map(|evidence| match &evidence.target {
            EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
            _ => None,
        })
        .collect::<Vec<_>>();
    if facts.is_empty() {
        return Ok(process_closed(outcome, effect));
    }
    if facts.len() != 1 {
        return Ok(false);
    }
    let fact = facts[0];
    let Some(envelope) = receipt
        .payload
        .get("process_closure")
        .filter(|value| !value.is_null())
    else {
        return Ok(false);
    };
    let invalid =
        || RuntimeServicesError::Invariant("invalid original Process closure publication".into());
    if envelope.as_object().is_none_or(|object| object.len() != 4)
        || envelope["content_schema"] != 2
        || envelope["body"]["kind"] != "artifact_ref"
        || envelope["body"]
            .as_object()
            .is_none_or(|object| object.len() != 2)
    {
        return Err(invalid());
    }
    let artifact: harness_contract::context::ArtifactRef =
        serde_json::from_value(envelope["body"]["artifact"].clone()).map_err(|_| invalid())?;
    if artifact.bytes > 32 * 1024
        || envelope["bytes"].as_u64() != Some(artifact.bytes)
        || envelope["sha256"].as_str() != Some(artifact.sha256.as_str())
        || artifact.media_type != "application/vnd.cowd.process-closure+json"
    {
        return Err(invalid());
    }
    let key = receipt.payload["idempotency_key"]
        .as_str()
        .ok_or_else(invalid)?;
    // The receipt is settled against the intent of its own key family: a read
    // receipt is settled against the read intent, a mutation receipt against
    // the mutation intent.
    let intent_key = if receipt.kind == "execution.read.receipt" {
        format!("{key}:read-intent")
    } else {
        format!("{key}:intent")
    };
    let intent = primary(services, &receipt.stream_id, &intent_key, through, budget)?
        .ok_or_else(invalid)?;
    let admission = primary(
        services,
        &receipt.stream_id,
        &format!("{key}:process-admission"),
        through,
        budget,
    )?
    .ok_or_else(invalid)?;
    if admission.kind != "execution.process.admitted"
        || admission.scope != RuntimeEventScope::ExecutionNode
        || admission.schema_version != 1
        || admission.actor != intent.actor
        || admission.refs != intent.refs
        || admission.payload["admission"]
            != serde_json::to_value(&fact.admission).map_err(|_| invalid())?
        || (intent.commit_cursor, intent.transaction_index)
            >= (admission.commit_cursor, admission.transaction_index)
        || (admission.commit_cursor, admission.transaction_index)
            >= (receipt.commit_cursor, receipt.transaction_index)
        || receipt.payload["process_admission_ref"] != format!("{key}:process-admission")
        || fact.admission.binding.call_key != key
        || fact.admission.binding.tool_use_id != outcome.tool_use_id
    {
        return Err(invalid());
    }
    let session = receipt.payload["replay"]["session_id"]
        .as_str()
        .ok_or_else(invalid)?;
    let scope = format!("session:{session}");
    if artifact.visibility_scope != scope {
        return Err(invalid());
    }
    let origin = crate::PreparedArtifactOrigin::ToolEffectClosure {
        stream_id: receipt.stream_id.clone(),
        idempotency_key: receipt.idempotency_key.clone().ok_or_else(invalid)?,
    };
    budget.terminal_preflight(4, 128 * 1024)?;
    budget.charge(0, usize::try_from(artifact.bytes).map_err(|_| invalid())?)?;
    let artifacts = services
        .commit_service()
        .bounded_process_artifacts()
        .map_err(scope_read_error)?;
    let observed = artifacts.read_process_closure_blocking(&artifact, &scope, &origin)?;
    budget.charge(observed.reads, observed.metadata_bytes)?;
    let document = harness_contract::step::parse_strict_json(
        std::str::from_utf8(&observed.body).map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())?;
    let expected = crate::execution_core::graph::process_closure_document(&intent, outcome)
        .map_err(scope_read_error)?;
    if document != expected {
        return Err(invalid());
    }
    Ok(process_closed(outcome, effect))
}

#[cfg(test)]
pub(crate) fn inspect_physical_closure_for_test(
    services: &RuntimeServices,
    receipt: &DurableRuntimeEvent,
    outcome: &crate::RuntimeToolExecutionOutcome,
    effect: &harness_contract::tool::ToolEffectDescriptor,
) -> Result<bool, RuntimeServicesError> {
    let through = services.event_store().durable_commit_cursor()?;
    let mut budget = history::RequestBudget::default();
    let closed = physical_receipt_closed(services, receipt, outcome, effect, through, &mut budget)?;
    services
        .commit_service()
        .validate_terminal_scope_from_records(
            effect,
            receipt,
            // The receipt is settled against the scope of its own key family.
            receipt.kind == "execution.read.receipt",
            &mut ProjectionScopeReader {
                services,
                budget: &mut budget,
                through,
            },
        )
        .map_err(scope_read_error)?;
    Ok(closed)
}

fn leaf_receipts(
    services: &RuntimeServices,
    graph_id: &str,
    node_id: &str,
    attempt: u32,
    spec: &harness_contract::execution_graph::ExecutionNodeSpec,
    bound_request: Option<&crate::RuntimeToolExecutionRequest>,
    no_dispatch: Option<&crate::execution_core::graph::executors::agent_tool::NoDispatchFact>,
    through: u64,
    budget: &mut history::RequestBudget,
    read: &mut ClosureRead,
) -> Result<bool, RuntimeServicesError> {
    let (expected_graph, expected_node, expected_attempt) = if let Some(request) = bound_request {
        let parent = request.parent_execution.as_ref().ok_or_else(|| {
            RuntimeServicesError::Invariant("original bound ToolBatch parent missing".into())
        })?;
        let parent_attempt = request
            .parent_execution_attempt
            .filter(|attempt| *attempt > 0)
            .ok_or_else(|| {
                RuntimeServicesError::Invariant(
                    "original bound ToolBatch parent attempt missing".into(),
                )
            })?;
        (
            parent.execution_id.as_str(),
            parent.node_id.as_str(),
            parent_attempt,
        )
    } else {
        (graph_id, node_id, attempt)
    };
    let stream =
        format!("execution-agent-receipts:{expected_graph}:{expected_node}:{expected_attempt}");
    let head = budget.stream_head(services, &stream, through)?;
    read.physical_sources.push((stream.clone(), head));
    if let Some(fact) = no_dispatch {
        for call in &fact.calls {
            if let Some(key) = &call.effect_key {
                let primary_stream = format!("execution-effect:{key}");
                let revision = budget.stream_head(services, &primary_stream, through)?;
                read.physical_sources.push((primary_stream, revision));
                if revision != 0 {
                    return Ok(false);
                }
            }
        }
    }
    let mut after = 0;
    let mut intents = BTreeSet::new();
    let mut receipts = BTreeSet::new();
    let mut calls = BTreeMap::new();
    let mut closed = true;
    while after < head {
        let page = budget.canonical_stream_page(services, &stream, after, head, through)?;
        if page.is_empty() {
            return Err(RuntimeServicesError::Invariant(
                "terminal effect index has a sequence gap".into(),
            ));
        }
        for index in page {
            let observation = terminal_index_observation(&index, &stream, after + 1, head)?;
            after = index.sequence;
            if index.commit_cursor > through {
                closed = false;
                continue;
            }
            let key = index.payload["idempotency_key"].as_str().ok_or_else(|| {
                RuntimeServicesError::Invariant("terminal effect index key missing".into())
            })?;
            if bound_request.is_some_and(|request| request.idempotency_key != key) {
                continue;
            }
            let suffix = if index.kind == "execution.agent_tool.intent" {
                "intent"
            } else {
                "receipt"
            };
            if index.idempotency_key.as_deref()
                != Some(format!("agent-tool-{suffix}:{key}").as_str())
            {
                return Err(RuntimeServicesError::Invariant(
                    "terminal effect index idempotency mismatch".into(),
                ));
            }
            if index.kind == "execution.agent_tool.intent" {
                intents.insert(key.to_string());
                continue;
            }
            if index.kind != "execution.agent_tool.receipt" {
                return Err(RuntimeServicesError::Invariant(
                    "unsupported terminal effect index fact".into(),
                ));
            }
            if bound_request.is_some_and(|request| observation != Some(request.observation_wave_sequence)) {
                return Err(RuntimeServicesError::Invariant(
                    "terminal effect index observation wave mismatch".into(),
                ));
            }
            let effect_stream = format!("execution-effect:{key}");
            let record = if let Some(record) = primary(
                services,
                &effect_stream,
                &format!("{key}:receipt"),
                through,
                budget,
            )? {
                Some(record)
            } else {
                primary(
                    services,
                    &effect_stream,
                    &format!("{key}:read-receipt"),
                    through,
                    budget,
                )?
            };
            let Some(record) = record else {
                closed = false;
                continue;
            };
            let is_read = record.kind == "execution.read.receipt";
            if record.scope != RuntimeEventScope::ExecutionNode
                || record.schema_version != 1
                || record.stream_id != effect_stream
                || record.payload["idempotency_key"] != key
                || record.payload["resolved_effect_scope"]["version"] != 2
                || record.transaction_id != index.transaction_id
                || record.commit_cursor != index.commit_cursor
                || record.transaction_index >= index.transaction_index
                || record.payload["outcome"] != index.payload["outcome"]
            {
                return Err(RuntimeServicesError::Invariant(
                    "terminal primary/index coverage conflict".into(),
                ));
            }
            let effect: harness_contract::tool::ToolEffectDescriptor =
                serde_json::from_value(record.payload["effect"].clone())
                    .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
            let outcome: crate::RuntimeToolExecutionOutcome =
                serde_json::from_value(record.payload["outcome"].clone())
                    .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
            let physical_closed = if let Some(closed) = budget.terminal_physical(&record.event_id) {
                closed
            } else {
                let closed = physical_receipt_closed(services, &record, &outcome, &effect, through, budget)?;
                budget.terminal_physical_verified(services, &record.event_id, closed)?;
                closed
            };
            services
                .commit_service()
                .validate_terminal_scope_from_records(
                    &effect,
                    &record,
                    is_read,
                    &mut ProjectionScopeReader {
                        services,
                        budget,
                        through,
                    },
                )
                .map_err(scope_read_error)?;
            if let Some(request) = bound_request {
                services
                    .commit_service()
                    .validate_terminal_request_identity(request, &effect, &record)
                    .map_err(scope_read_error)?;
            }
            let replay = &record.payload["replay"];
            let root = &record.payload["root_effect_owner"];
            let (owner_graph, owner_node, owner_attempt) = if root.is_null() {
                (
                    replay["parent_execution"]["execution_id"].as_str(),
                    replay["parent_execution"]["node_id"].as_str(),
                    record
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
            if owner_graph != Some(expected_graph)
                || owner_node != Some(expected_node)
                || owner_attempt != Some(expected_attempt)
            {
                return Err(RuntimeServicesError::Invariant(
                    "terminal effect owner attempt mismatch".into(),
                ));
            }
            if !receipts.insert(key.to_string()) {
                return Err(RuntimeServicesError::Invariant(
                    "duplicate terminal effect primary".into(),
                ));
            }
            let call = replay["call_id"].as_str().ok_or_else(|| {
                RuntimeServicesError::Invariant("terminal call identity missing".into())
            })?;
            calls.insert(
                call.to_string(),
                (
                    record.payload["tool_name"].clone(),
                    record.payload["input_sha256"].clone(),
                ),
            );
            read.refs.push(format!("runtime-event:{}", record.event_id));
            read.refs.push(format!("runtime-event:{}", index.event_id));
            closed &= physical_closed;
        }
    }
    closed &= intents.is_subset(&receipts);
    if no_dispatch.is_some_and(|fact| {
        fact.calls
            .iter()
            .any(|call| calls.contains_key(&call.call_id))
    }) {
        return Err(RuntimeServicesError::Invariant(
            "NoDispatch conflicts with actual leaf receipt".into(),
        ));
    }
    if let Some(request) = bound_request {
        use sha2::{Digest, Sha256};
        let digest = serde_json::json!(format!(
            "sha256:{:x}",
            Sha256::digest(request.input.as_bytes())
        ));
        closed &= no_dispatch.is_some_and(|fact| {
            fact.calls
                .iter()
                .any(|call| call.effect_key.as_deref() == Some(request.idempotency_key.as_str()))
        }) || (receipts.contains(&request.idempotency_key)
            && calls
                .get(&request.tool_use_id)
                .is_some_and(|(name, input)| *name == request.tool_name && *input == digest));
    } else if spec.kind == ExecutionNodeKind::ToolBatch {
        use sha2::{Digest, Sha256};
        let batch = crate::step_snapshot::decode_persisted_tool_batch_value(&spec.payload_ref)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        let planned = batch["calls"].as_array().ok_or_else(|| {
            RuntimeServicesError::Invariant("original ToolBatch calls missing".into())
        })?;
        for call in planned {
            let Some(identity) = call["id"].as_str() else {
                return Err(RuntimeServicesError::Invariant(
                    "original call identity missing".into(),
                ));
            };
            let input = call["input"].as_str().ok_or_else(|| {
                RuntimeServicesError::Invariant("original call input missing".into())
            })?;
            let digest =
                serde_json::json!(format!("sha256:{:x}", Sha256::digest(input.as_bytes())));
            if no_dispatch
                .is_some_and(|fact| fact.calls.iter().any(|call| call.call_id == identity))
            {
                continue;
            }
            closed &= calls
                .get(identity)
                .is_some_and(|(name, actual)| *name == call["name"] && *actual == digest);
        }
    }
    Ok(closed)
}

#[cfg(test)]
pub(crate) fn inspect_bound_leaf_for_test(
    services: &RuntimeServices,
    request: &crate::RuntimeToolExecutionRequest,
) -> Result<bool, RuntimeServicesError> {
    let through = services.event_store().durable_commit_cursor()?;
    let mut budget = history::RequestBudget::default();
    let mut read = ClosureRead::default();
    let spec = harness_contract::execution_graph::ExecutionNodeSpec::new(
        ExecutionNodeKind::ToolBatch,
        "tool_batch",
        "unused-bound-carrier-test-bridge",
    );
    leaf_receipts(
        services,
        "child-graph",
        "child-node",
        1,
        &spec,
        Some(request),
        None,
        through,
        &mut budget,
        &mut read,
    )
}

fn starts_covered(state: &history::HistoryState) -> bool {
    let Some(graph) = &state.graph_state else {
        return false;
    };
    if state.bindings.len() != state.start_specs.len()
        || state.bindings.len() != state.start_tickets.len()
        || state.bindings.len() != state.start_positions.len()
    {
        return false;
    }
    let mut attempts: BTreeMap<&str, BTreeSet<u32>> = BTreeMap::new();
    for (slot, (node_id, binding)) in &state.bindings {
        let Some(ticket) = state.start_tickets.get(slot) else {
            return false;
        };
        let Some(spec) = state.start_specs.get(slot) else {
            return false;
        };
        let Some(position) = state.start_positions.get(slot) else {
            return false;
        };
        if binding.attempt == 0
            || !graph.nodes.iter().any(|node| node.id == *node_id)
            || binding.validate_identity(spec).is_err()
            || ticket.graph_id != graph.id
            || ticket.node_id != *node_id
            || spec.id != *node_id
            || ticket.attempt != binding.attempt
            || ticket.executor_kind != spec.executor_kind
            || ticket.payload_ref != spec.payload_ref
            || ticket.idempotency_key != binding.ticket_idempotency_key
            || slot
                != &format!(
                    "{node_id}:{}:{}",
                    binding.attempt, binding.ticket_idempotency_key
                )
            || position.0 > state.through
            || !attempts
                .entry(node_id.as_str())
                .or_default()
                .insert(binding.attempt)
        {
            return false;
        }
    }
    graph.nodes.iter().all(|node| {
        let expected = graph
            .recovery_cursor
            .node_attempts
            .get(&node.id)
            .copied()
            .unwrap_or(0);
        if graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::Completed)
            && expected == 0
        {
            return false;
        }
        let observed = attempts.get(node.id.as_str());
        observed.map_or(expected == 0, |observed| {
            u32::try_from(observed.len()).ok() == Some(expected)
                && observed.last().copied() == Some(expected)
        })
    })
}

fn durable_binding_leases_closed(
    services: &RuntimeServices,
    state: &history::HistoryState,
    slot: &str,
    node_id: &str,
    binding: &crate::execution_core::graph::ExecutionNodeBinding,
    through: u64,
    budget: &mut history::RequestBudget,
    refs: &mut Vec<String>,
    no_dispatch: &mut Option<crate::execution_core::graph::executors::agent_tool::NoDispatchFact>,
) -> Result<bool, RuntimeServicesError> {
    let Some(graph) = state.graph_state.as_ref() else {
        return Ok(false);
    };
    let key = binding.lease_closure_key(graph, node_id);
    let Some(record) = budget.canonical_record(services, &graph.id, &key, through)? else {
        return Ok(false);
    };
    let prepaid = serde_json::to_vec(record.as_ref())
        .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?
        .len()
        .saturating_add(
            if record.payload["body"]["kind"].as_str() != Some("inline") {
                usize::try_from(record.payload["bytes"].as_u64().unwrap_or(0))
                    .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?
            } else {
                0
            },
        );
    let resolved = budget.resolve_prepaid_record(services, &record, prepaid)?;
    let start = state.start_positions.get(slot).copied().ok_or_else(|| {
        RuntimeServicesError::Invariant("lease closure original start position missing".into())
    })?;
    let closed = binding
        .validate_lease_closure(graph, node_id, start, &resolved)
        .map_err(RuntimeServicesError::Invariant)?;
    if closed {
        let ticket = state.start_tickets.get(slot).ok_or_else(|| {
            RuntimeServicesError::Invariant("NoDispatch original ticket missing".into())
        })?;
        *no_dispatch = crate::execution_core::graph::ExecutionCommitService::validate_no_dispatch_closure_source(
            graph, ticket, binding, start, &resolved,
            &mut ProjectionScopeReader { services, budget, through },
        ).map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        refs.push(format!("runtime-event:{}", record.event_id));
    }
    Ok(closed)
}

pub(super) fn read_binding(
    services: &RuntimeServices,
    state: &history::HistoryState,
    slot: &str,
    through: u64,
    budget: &mut history::RequestBudget,
) -> Result<ClosureRead, RuntimeServicesError> {
    let (node_id, binding) = state.bindings.get(slot).ok_or_else(|| {
        RuntimeServicesError::Invariant("terminal binding missing".into())
    })?;
    let mut read = ClosureRead {
        complete: true,
        ..Default::default()
    };
    let ticket = state.start_tickets.get(slot).ok_or_else(|| {
        RuntimeServicesError::Invariant("terminal start ticket missing".into())
    })?;
    let spec = state.start_specs.get(slot).ok_or_else(|| {
        RuntimeServicesError::Invariant("terminal start spec missing".into())
    })?;
    read.complete &= known_executor(spec);
    let mut no_dispatch = None;
    if !durable_binding_leases_closed(
        services,
        state,
        slot,
        node_id,
        binding,
        through,
        budget,
        &mut read.refs,
        &mut no_dispatch,
    )? {
        read.complete = false;
        for reference in binding
            .resource_lease_refs
            .iter()
            .chain(binding.scope_lease_ref.iter())
        {
            budget.terminal_preflight(1, 0)?;
            budget.charge(1, 0)?;
            let active = read_lease(services, reference)?;
            read.complete &= active.is_some();
            read.busy |= active == Some(true);
            read.refs.push(reference.clone());
            read.leases.push((reference.clone(), active));
        }
        if let Some(identity) = &binding.worktree_lease_ref {
            let reference = format!("worktree-lease:{identity}");
            budget.terminal_preflight(2, 0)?;
            budget.charge(2, 0)?;
            let active = read_lease(services, &reference)?;
            let record = uuid::Uuid::parse_str(identity)
                .ok()
                .map(|identity| services.worktree_leases().record(identity))
                .transpose()
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?
                .flatten();
            read.complete &= active.is_some()
                && record.as_ref().is_some_and(|record| {
                    record.task_id == ticket.graph_id && record.owner_id == *node_id
                });
            read.busy |= active == Some(true);
            read.refs.push(reference.clone());
            read.leases.push((reference, active));
        }
    }
    if spec.kind == ExecutionNodeKind::AgentTask {
        let packet: harness_contract::agent::AgentTaskPacket =
            serde_json::from_str(&spec.payload_ref)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?;
        read.complete &= packet.graph_id() == ticket.graph_id.as_str()
            && packet.node_id() == node_id.as_str();
        let stream = format!("agent:{}", packet.agent_id());
        let head = budget.uncached_stream_head(services, &stream)?;
        read.physical_sources.push((stream.clone(), head));
        let mut records = Vec::new();
        for kind in [
            "agent.run_admitted",
            "agent.backend_admitted",
            "agent.local_exit",
        ] {
            if let Some(record) = primary(
                services,
                &stream,
                &format!("{kind}:{}", packet.run_id()),
                through,
                budget,
            )? {
                read.refs.push(format!("runtime-event:{}", record.event_id));
                records.push(record);
            }
        }
        read.complete &= crate::agent_runtime::projection_run_closed_from_records(
            &packet, head, &records,
        )
        .map_err(RuntimeServicesError::Invariant)?;
        if budget.uncached_stream_head(services, &stream)? != head {
            read.complete = false;
        }
    }
    let bound_request = if state.graph_state.as_ref().is_some_and(
        crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph,
    ) {
        Some(crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_node(&ticket.graph_id, spec)
            .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?.request)
    } else {
        None
    };
    let closed = leaf_receipts(
        services,
        &ticket.graph_id,
        node_id,
        binding.attempt,
        spec,
        bound_request.as_ref(),
        no_dispatch.as_ref(),
        through,
        budget,
        &mut read,
    )?;
    read.complete &= closed;
    if budget.terminal_active() {
        budget.terminal_preflight(1, 20)?;
        budget.charge(1, 20)?;
        read.verified_cursor = (services.event_store().durable_commit_cursor()? == through).then_some(through);
    }
    Ok(read)
}

pub(super) fn refresh_binding(
    services: &RuntimeServices,
    proof: &history::TerminalBindingProof,
    through: u64,
    budget: &mut history::RequestBudget,
) -> Result<Option<ClosureRead>, RuntimeServicesError> {
    if !proof.read.complete || proof.read.busy || !proof.read.leases.is_empty() {
        return Ok(None);
    }
    let mut read = proof.read.clone();
    read.verified_cursor = None;
    if !unchanged(services, &read, budget)? {
        return Ok(None);
    }
    budget.terminal_preflight(1, 20)?;
    budget.charge(1, 20)?;
    read.verified_cursor = (services.event_store().durable_commit_cursor()? == through).then_some(through);
    Ok(Some(read))
}

pub(super) fn verified_read(
    state: &history::HistoryState,
    through: u64,
) -> Result<Option<ClosureRead>, RuntimeServicesError> {
    let mut read = ClosureRead {
        complete: starts_covered(state),
        ..Default::default()
    };
    for slot in state.bindings.keys() {
        let Some(proof) = state.terminal_proofs.get(slot) else { return Ok(None); };
        if proof.identity != history::effect_proof_identity(state, slot)? || proof.through != through {
            return Ok(None);
        }
        if !proof.read.valid_binding_cut(through) {
            return Err(RuntimeServicesError::Invariant("terminal proof cut mismatch".into()));
        }
        read.merge(proof.read.clone());
    }
    Ok(Some(read))
}

pub(super) fn read(
    services: &RuntimeServices,
    facts: &BTreeMap<String, history::HistoryState>,
    through: u64,
    budget: &mut history::RequestBudget,
) -> Result<ClosureRead, RuntimeServicesError> {
    let mut read = ClosureRead {
        complete: true,
        ..Default::default()
    };
    for state in facts.values() {
        if state.source_fence.is_some() && state.source_fence == budget.request_source {
            if let Some(verified) = verified_read(state, through)? {
                read.merge(verified);
                continue;
            }
        }
        read.complete &= starts_covered(state);
        for slot in state.bindings.keys() {
            let binding = read_binding(services, state, slot, through, budget)?;
            read.merge(binding);
        }
    }
    Ok(read)
}

#[cfg(test)]
mod coverage_426_tests {
    use super::*;
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeSpec};

    #[test]
    fn completed_positive_attempt_without_original_start_never_has_coverage() {
        let mut graph = ExecutionGraph::new("coverage-426");
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Completed);
        graph
            .recovery_cursor
            .node_attempts
            .insert(node.id.clone(), 1);
        graph.nodes.push(node);
        let state = history::HistoryState {
            graph_state: Some(graph),
            ..Default::default()
        };
        assert!(!starts_covered(&state));
    }

    #[test]
    fn never_started_cancelled_node_does_not_require_a_fabricated_start() {
        let mut graph = ExecutionGraph::new("never-started-426");
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Cancelled);
        graph.nodes.push(node);
        let mut state = history::HistoryState {
            graph_state: Some(graph),
            ..Default::default()
        };
        assert!(starts_covered(&state));
        let graph = state.graph_state.as_mut().unwrap();
        graph
            .recovery_cursor
            .node_attempts
            .insert(graph.nodes[0].id.clone(), u32::MAX);
        assert!(!starts_covered(&state));
    }

    #[test]
    fn every_historical_attempt_requires_its_exact_start_tables() {
        use crate::execution_core::graph::{ExecutionNodeBinding, NodeExecutionTicket};
        let mut graph = ExecutionGraph::new("historical-starts-426");
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Completed);
        graph
            .recovery_cursor
            .node_attempts
            .insert(node.id.clone(), 2);
        graph.nodes.push(node.clone());
        let mut state = history::HistoryState {
            graph_state: Some(graph.clone()),
            through: 20,
            ..Default::default()
        };
        for attempt in [2, 1] {
            let key = format!("original-attempt-{attempt}");
            let slot = format!("{}:{attempt}:{key}", node.id);
            state.bindings.insert(
                slot.clone(),
                (
                    node.id.clone(),
                    ExecutionNodeBinding {
                        executor_kind: node.executor_kind.clone(),
                        ticket_idempotency_key: key.clone(),
                        attempt,
                        resource_lease_refs: Vec::new(),
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                ),
            );
            state.start_specs.insert(slot.clone(), node.clone());
            state.start_tickets.insert(
                slot.clone(),
                NodeExecutionTicket {
                    graph_id: graph.id.clone(),
                    node_id: node.id.clone(),
                    executor_kind: node.executor_kind.clone(),
                    service_class: graph.service_class,
                    attempt,
                    idempotency_key: key,
                    payload_ref: node.payload_ref.clone(),
                },
            );
            state
                .start_positions
                .insert(slot.clone(), (u64::from(attempt), 0));
            assert_eq!(starts_covered(&state), attempt == 1);
        }
        let slot = state.start_positions.keys().next().unwrap().clone();
        state.start_positions.insert(slot, (21, 0));
        assert!(!starts_covered(&state));
    }
}
