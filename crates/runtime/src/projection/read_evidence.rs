//! On-demand authenticated delivery pages. No projection cache/checkpoint writer.
use super::{history::RequestBudget, reducer_support::*, *};
use crate::agentic::review_evidence::{self, read_source::ReadAuthSource};
use crate::context_evidence::{self, ReadDeliveryRecipient};
use crate::DurableRuntimeEvent;
use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeKind};
use harness_contract::projection::*;

struct BudgetReader<'a> {
    services: &'a RuntimeServices,
    budget: &'a mut RequestBudget,
    through: u64,
    failure: Option<ReadEvidenceIncompleteReason>,
}
fn read_error_reason(error: &RuntimeServicesError) -> ReadEvidenceIncompleteReason {
    match error {
        RuntimeServicesError::EventStore(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
            _,
        )) => ReadEvidenceIncompleteReason::Budget,
        RuntimeServicesError::Invariant(message) if message == history::INCOMPLETE => {
            ReadEvidenceIncompleteReason::Budget
        }
        RuntimeServicesError::Invariant(message)
            if message == "history source mutation fence changed" =>
        {
            ReadEvidenceIncompleteReason::SourceChanged
        }
        _ => ReadEvidenceIncompleteReason::AuthenticationFailed,
    }
}
impl BudgetReader<'_> {
    fn result<T>(&mut self, result: Result<T, RuntimeServicesError>) -> Result<T, String> {
        result.map_err(|error| {
            self.failure = Some(read_error_reason(&error));
            error.to_string()
        })
    }
}
impl ReadAuthSource for BudgetReader<'_> {
    fn stream_revision(&mut self, stream: &str) -> Result<u64, String> {
        let value = self.budget.uncached_stream_head(self.services, stream);
        self.result(value)
    }
    fn event_by_idempotency_key(
        &mut self,
        stream: &str,
        key: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        let value = self
            .budget
            .canonical_record(self.services, stream, key, self.through);
        self.result(value)
            .map(|record| record.map(|record| record.as_ref().clone()))
    }
    fn list_stream_after(
        &mut self,
        stream: &str,
        after: u64,
        head: u64,
        limit: usize,
        bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let value = self.budget.limited_source_page(
            self.services,
            stream,
            after,
            head,
            limit,
            bytes,
            self.through,
        );
        let records = self.result(value)?;
        if records
            .iter()
            .any(|record| record.commit_cursor > self.through)
        {
            return Err("read original is ahead of request cut".into());
        }
        Ok(records
            .into_iter()
            .map(|record| record.as_ref().clone())
            .collect())
    }
    fn transaction_stream_revision_exact(
        &mut self,
        transaction: &str,
        stream: &str,
    ) -> Result<Option<crate::CommittedStreamRevision>, String> {
        let value = self
            .budget
            .read_auth_fence(self.services, transaction, stream, self.through);
        self.result(value)
    }
    fn resolve_graph_record(
        &mut self,
        record: &DurableRuntimeEvent,
    ) -> Result<serde_json::Value, String> {
        if record.commit_cursor > self.through {
            return Err("read graph is ahead of request cut".into());
        }
        let value = self.budget.resolve_raw_graph_record(self.services, record);
        self.result(value)
    }
}

fn event_ref(event: &DurableRuntimeEvent) -> ReadEvidenceEventRef {
    ReadEvidenceEventRef {
        stream_id: event.stream_id.clone(),
        sequence: event.sequence,
        event_id: event.event_id.clone(),
        commit_cursor: event.commit_cursor,
        payload_digest: context_evidence::read_payload_digest(&event.payload),
    }
}
fn extent(value: &context_evidence::ReadExtent) -> ReadEvidenceExtent {
    match value {
        context_evidence::ReadExtent::FileLines {
            start_line,
            line_count,
            total_lines,
            ends_with_newline,
            body_sha256,
            body_bytes,
            source_omitted,
        } => ReadEvidenceExtent::FileLines {
            start_line: *start_line,
            line_count: *line_count,
            total_lines: *total_lines,
            ends_with_newline: *ends_with_newline,
            body_sha256: body_sha256.clone(),
            body_bytes: *body_bytes,
            source_omitted: *source_omitted,
        },
        context_evidence::ReadExtent::ArtifactChunks {
            total_chunks,
            chunks,
        } => ReadEvidenceExtent::ArtifactChunks {
            total_chunks: *total_chunks,
            chunks: chunks
                .iter()
                .map(|chunk| ReadEvidenceChunk {
                    index: chunk.index,
                    body_sha256: chunk.body_sha256.clone(),
                    body_bytes: chunk.body_bytes,
                })
                .collect(),
        },
    }
}

/// Reconstruct the original spec at T, never borrow a newer materialized packet.
fn graph_at_cut(
    reader: &mut BudgetReader<'_>,
    graph_id: &str,
    sources: &mut BTreeMap<String, u64>,
) -> Result<ExecutionGraph, String> {
    let head = reader.stream_revision(graph_id)?;
    if sources
        .insert(graph_id.into(), head)
        .is_some_and(|old| old != head)
    {
        return Err("read graph source changed".into());
    }
    let mut sequence = head;
    let mut tail = Vec::new();
    let mut graph = loop {
        if sequence == 0 {
            return Err("read graph has no same-cut checkpoint".into());
        }
        let records = reader.budget.limited_source_page(
            reader.services,
            graph_id,
            sequence - 1,
            sequence,
            1,
            1024 * 1024,
            reader.through,
        );
        let records = reader.result(records)?;
        let record = records.first().ok_or("read graph record missing")?;
        if record.commit_cursor > reader.through {
            sequence -= 1;
            continue;
        }
        if record.actor.as_deref() != Some("execution_commit_service")
            || record.scope != RuntimeEventScope::ExecutionGraph
            || record.schema_version != 2
        {
            return Err("read graph record identity mismatch".into());
        }
        let body = reader.resolve_graph_record(record)?;
        let event: crate::execution_core::graph::ExecutionGraphEvent =
            serde_json::from_value(body).map_err(|_| "read graph body invalid")?;
        if event.kind() != record.kind {
            return Err("read graph kind mismatch".into());
        }
        if matches!(
            event,
            crate::execution_core::graph::ExecutionGraphEvent::Planned { .. }
                | crate::execution_core::graph::ExecutionGraphEvent::Checkpoint { .. }
        ) {
            break event.project(None)?;
        }
        tail.push(event);
        sequence -= 1;
    };
    for event in tail.into_iter().rev() {
        graph = event.project(Some(graph))?;
    }
    if graph.id != graph_id {
        return Err("read graph escaped scope".into());
    }
    Ok(graph)
}
fn select_owner<'a>(
    reader: &mut BudgetReader<'_>,
    scope: &'a ExecutionProjectionScope,
    material: &context_evidence::ReadDeliveryMaterial,
    sources: &mut BTreeMap<String, u64>,
) -> Result<(&'a crate::AgenticProgramProjection, Option<AgentTaskPacket>), String> {
    match &material.recipient {
        ReadDeliveryRecipient::Root { owner, .. } => {
            let mut matches = scope.programs.iter().filter(|program| {
                program.session_id == material.session_id
                    && program.turn_id == material.turn_id
                    && program.root_execution_id.as_deref()
                        == Some(owner.binding.root_execution_id.as_str())
                    && scope
                        .execution_ids
                        .contains(&owner.binding.root_execution_id)
            });
            let program = matches.next().ok_or("read Root Program missing")?;
            if matches.next().is_some() {
                return Err("read Root Program ambiguous".into());
            }
            Ok((program, None))
        }
        ReadDeliveryRecipient::AgentTask { identity } => {
            if !scope.execution_ids.contains(&material.acquisition.graph_id) {
                return Err("read Agent graph foreign".into());
            }
            let graph = graph_at_cut(reader, &material.acquisition.graph_id, sources)?;
            let node = graph
                .nodes
                .iter()
                .find(|node| {
                    node.id == material.acquisition.node_id
                        && node.kind == ExecutionNodeKind::AgentTask
                })
                .ok_or("read original Agent node missing")?;
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|_| "read original Agent packet unsupported")?;
            let binding = packet
                .binding
                .as_ref()
                .ok_or("read original Agent binding missing")?;
            if packet.assignment.validate().is_err()
                || packet.graph_id() != graph.id
                || packet.node_id() != node.id
                || packet.assignment.execution_identity != *identity
                || packet.attempt != material.acquisition.attempt
                || graph.recovery_cursor.node_attempts.get(&node.id) != Some(&packet.attempt)
                || packet.run_id() != material.acquisition.step.run_ref.as_deref().unwrap_or("")
                || !crate::agent::binding::recompute_binding_digest(binding)
                    .is_ok_and(|digest| digest == binding.binding_digest)
                || binding.definition_ref != packet.assignment.definition_ref
                || binding.instance.instance_id != packet.agent_id()
            {
                return Err("read original Agent packet identity mismatch".into());
            }
            let program_id = &packet
                .agentic_binding
                .as_ref()
                .ok_or("read Agent Program binding missing")?
                .program_id;
            let mut matches = scope.programs.iter().filter(|program| {
                &program.program_id == program_id
                    && program.session_id == packet.session_id()
                    && program.turn_id == identity.turn_id().unwrap_or("")
                    && program.session_id == material.session_id
                    && program.turn_id == material.turn_id
                    && program
                        .root_execution_id
                        .as_ref()
                        .is_some_and(|root| scope.execution_ids.contains(root))
            });
            let program = matches
                .next()
                .ok_or("read original Agent Program foreign")?;
            if matches.next().is_some()
                || graph.lineage.as_ref().is_none_or(|lineage| {
                    lineage.session_id != program.session_id || lineage.turn_id != program.turn_id
                })
            {
                return Err("read Agent lineage ambiguous".into());
            }
            Ok((program, Some(packet)))
        }
    }
}
fn fail(page: &mut ReadEvidencePage, reason: ReadEvidenceIncompleteReason) {
    page.incomplete = true;
    page.reason = Some(reason);
    page.exhausted = false;
    page.reads.clear();
}

pub async fn read_evidence(
    services: &RuntimeServices,
    execution_id: &str,
    context: &ProjectionQueryContext,
    query: ReadEvidenceQuery,
) -> Result<ReadEvidencePage, RuntimeServicesError> {
    validate_context(services, context)?;
    if context.detail_scope != ProjectionDetailScope::Full
        || query.limit == 0
        || query.limit > READ_EVIDENCE_MAX_LIMIT
    {
        return Err(RuntimeServicesError::Invariant(
            "invalid read-evidence query".into(),
        ));
    }
    let mut budget = RequestBudget::default();
    let fence = budget.capture_source_fence(services)?;
    let current = services.event_store().durable_commit_cursor()?;
    let through = query.through.unwrap_or(current);
    if through > current {
        return Err(RuntimeServicesError::Invariant(
            "future read-evidence cut".into(),
        ));
    }
    let graph = services
        .execution_supervisor()
        .graph_projection(execution_id)
        .await?;
    // Authorize the known graph Session before any Session event is read.
    validate_session_scope(
        graph.lineage.as_ref().map(|l| l.session_id.as_str()),
        context,
    )?;
    let scope = ExecutionProjectionScope::load_budgeted(
        services,
        execution_id,
        &graph,
        false,
        through,
        &mut budget,
    )?;
    validate_projection_scope(&scope, context)?;
    let session = scope
        .session_id
        .as_deref()
        .ok_or_else(|| RuntimeServicesError::Invariant("read-evidence Session missing".into()))?;
    let turn = scope
        .turn_id
        .as_deref()
        .ok_or_else(|| RuntimeServicesError::Invariant("read-evidence Turn missing".into()))?;
    let mut page = ReadEvidencePage {
        schema_version: READ_EVIDENCE_SCHEMA_VERSION,
        execution_id: execution_id.into(),
        session_id: session.into(),
        turn_id: turn.into(),
        workspace_id: context.workspace_id.clone(),
        authorization_revision: context.authorization_revision,
        redaction_revision: redaction_revision(context),
        graph_revision: graph.revision,
        through,
        next_after_sequence: query.after_sequence,
        exhausted: false,
        incomplete: false,
        reason: None,
        source_incarnation: fence.incarnation,
        source_mutation_epoch: fence.mutation_epoch,
        sources: vec![],
        reads: vec![],
    };
    if graph.commit_cursor > through {
        fail(&mut page, ReadEvidenceIncompleteReason::UnsupportedIdentity);
        return Ok(page);
    }
    let stream = format!("session:{session}");
    let head = budget.uncached_stream_head(services, &stream)?;
    if query.after_sequence > head {
        return Err(RuntimeServicesError::Invariant(
            "read-evidence sequence exceeds source".into(),
        ));
    }
    let mut sources = BTreeMap::from([(stream.clone(), head)]);
    for program in &scope.programs {
        let source = format!("agentic-program:{}", program.program_id);
        let revision = budget.uncached_stream_head(services, &source)?;
        sources.insert(source, revision);
    }
    sources.insert(
        execution_id.to_string(),
        budget.uncached_stream_head(services, execution_id)?,
    );

    let mut candidates = 0;
    while page.next_after_sequence < head && candidates < query.limit {
        if !budget.read_auth_can_scan() {
            if page.next_after_sequence == query.after_sequence {
                fail(&mut page, ReadEvidenceIncompleteReason::Budget);
            }
            break;
        }
        let previous = page.next_after_sequence;
        let records = match budget.limited_source_page(
            services,
            &stream,
            previous,
            head,
            1,
            1024 * 1024,
            through,
        ) {
            Ok(records) => records,
            Err(error) => {
                fail(&mut page, read_error_reason(&error));
                break;
            }
        };
        let Some(record) = records.first() else {
            fail(&mut page, ReadEvidenceIncompleteReason::ZeroProgress);
            break;
        };
        if record.commit_cursor > through
            || !matches!(
                record.kind.as_str(),
                context_evidence::NATIVE_DELIVERY_KIND | context_evidence::PROCESS_DELIVERY_KIND
            )
        {
            page.next_after_sequence = record.sequence;
            continue;
        }
        let material = match context_evidence::decode_read_delivery(&record.payload) {
            Ok(material) => material,
            Err(_) => {
                fail(
                    &mut page,
                    ReadEvidenceIncompleteReason::AuthenticationFailed,
                );
                break;
            }
        };
        if material.session_id != session
            || material.turn_id != turn
            || !scope.execution_ids.contains(&material.acquisition.graph_id)
        {
            page.next_after_sequence = record.sequence;
            continue;
        }
        let mut reader = BudgetReader {
            services,
            budget: &mut budget,
            through,
            failure: None,
        };
        let mut cuts = sources.clone();
        let (program, packet) = match select_owner(&mut reader, &scope, &material, &mut cuts) {
            Ok(owner) => owner,
            Err(_) => {
                fail(
                    &mut page,
                    reader
                        .failure
                        .unwrap_or(ReadEvidenceIncompleteReason::UnsupportedIdentity),
                );
                break;
            }
        };
        let snapshot = match review_evidence::review_delivery_snapshot_from_events(
            &mut reader,
            program,
            packet.as_ref(),
            vec![record.as_ref().clone()],
            cuts,
        ) {
            Ok(snapshot) => snapshot,
            Err(message) => {
                #[cfg(test)]
                eprintln!("read-evidence original core refused: {message}");
                let reason = reader.failure.unwrap_or_else(|| {
                    if message.contains("source")
                        && (message.contains("changed") || message.contains("moved"))
                    {
                        ReadEvidenceIncompleteReason::SourceChanged
                    } else {
                        ReadEvidenceIncompleteReason::AuthenticationFailed
                    }
                });
                fail(&mut page, reason);
                break;
            }
        };
        if snapshot.reads.len() != 1 {
            fail(
                &mut page,
                ReadEvidenceIncompleteReason::AuthenticationFailed,
            );
            break;
        }
        let read = &snapshot.reads[0];
        let primary = match reader.event_by_idempotency_key(
            &material.acquisition.primary_stream,
            &material.acquisition.primary_key,
        ) {
            Ok(Some(record)) => record,
            _ => {
                fail(
                    &mut page,
                    ReadEvidenceIncompleteReason::AuthenticationFailed,
                );
                break;
            }
        };
        let recipient = match &material.recipient {
            ReadDeliveryRecipient::Root { owner, .. } => ReadEvidenceRecipient::Root {
                binding: owner.binding.clone(),
            },
            ReadDeliveryRecipient::AgentTask { identity } => ReadEvidenceRecipient::Agent {
                identity: identity.clone(),
                graph_id: material.acquisition.graph_id.clone(),
                node_id: material.acquisition.node_id.clone(),
                run_id: packet
                    .as_ref()
                    .expect("authenticated Agent packet")
                    .run_id()
                    .into(),
                attempt: material.acquisition.attempt,
            },
        };
        let mut verified = Vec::new();
        if let Some(scope) = &read.original_scope {
            for target in &scope.targets {
                if target.path.workspace_id != context.workspace_id
                    || !target.exact
                    || !target.alias_checked
                    || target.path.object_kind
                        != harness_contract::context::WorkspaceObjectKind::File
                    || target.declared_scope.resource
                        != harness_contract::policy::PermissionResource::File
                    || target.declared_scope.operation
                        != harness_contract::policy::PermissionOperation::Read
                    || !super::snapshot::strategy_ref_visible(
                        &target.path.workspace_relative_path,
                        context,
                        Some("read"),
                    )
                {
                    continue;
                }
                verified.push(VerifiedReadEvidence {
                    path: target.path.clone(),
                    // Public view narrows the authenticated declared scope to the
                    // visible canonical object; absolute host aliases stay private.
                    scope: harness_contract::policy::PermissionScope {
                        resource: target.declared_scope.resource.clone(),
                        operation: target.declared_scope.operation.clone(),
                        target: Some(target.path.workspace_relative_path.clone()),
                    },
                    recipient: recipient.clone(),
                    step: material.acquisition.step.clone(),
                    call_id: material.tool_invocation_id.clone(),
                    tool_name: material.acquisition.tool_name.clone(),
                    invocation_id: material.acquisition.invocation_id.clone(),
                    delivery: event_ref(record),
                    acquisition: event_ref(&primary),
                    index: event_ref(&read.receipt),
                    source_sha256: material.acquisition.body.source_sha256.clone(),
                    source_bytes: material.acquisition.body.source_bytes,
                    delivered_extent: material.delivered_extent.as_ref().map(extent),
                    grade: match material.grade {
                        context_evidence::ReadDeliveryGrade::ProviderModelObserved => {
                            ReadEvidenceGrade::ProviderModelObserved
                        }
                        context_evidence::ReadDeliveryGrade::AgentChannelSend => {
                            ReadEvidenceGrade::AgentChannelSend
                        }
                    },
                    body_delivered: read.body_delivered(),
                });
            }
        }
        if page.reads.len() + verified.len() > query.limit {
            if previous == query.after_sequence {
                fail(&mut page, ReadEvidenceIncompleteReason::Budget);
            }
            break;
        }
        if !verified.is_empty() {
            sources = snapshot.sources;
            page.reads.extend(verified);
        }
        candidates += 1;
        page.next_after_sequence = record.sequence;
    }
    if !page.incomplete {
        page.exhausted = page.next_after_sequence == head;
    }
    finalize_page(services, &mut budget, &sources, &mut page);
    Ok(page)
}

fn finalize_page(
    services: &RuntimeServices,
    budget: &mut RequestBudget,
    sources: &BTreeMap<String, u64>,
    page: &mut ReadEvidencePage,
) {
    // Recheck fresh heads, never their memoized copies. No greens survive a race.
    for (stream, revision) in sources {
        match budget.uncached_stream_head(services, stream) {
            Ok(current) if current == *revision => {}
            Ok(_) => {
                fail(page, ReadEvidenceIncompleteReason::SourceChanged);
                break;
            }
            Err(error) => {
                fail(page, read_error_reason(&error));
                break;
            }
        }
    }
    if let Err(error) = budget.require_captured_source(services) {
        fail(page, read_error_reason(&error));
    }
    page.sources = sources
        .iter()
        .map(|(stream_id, revision)| ReadEvidenceSource {
            stream_id: stream_id.clone(),
            revision: *revision,
        })
        .collect();
}

/// Fixed original transactions for the API/core integration test. No Provider,
/// Process or database. Expectations in callers are independent literal paths.
#[cfg(any(test, feature = "test-fixtures"))]
pub fn read_evidence_fixture(
    agent: bool,
    unrelated: usize,
) -> (
    std::sync::Arc<RuntimeServices>,
    String,
    ProjectionQueryContext,
) {
    read_evidence_fixture_fault(agent, unrelated, None)
}

#[cfg(any(test, feature = "test-fixtures"))]
fn read_evidence_fixture_fault(
    agent: bool,
    unrelated: usize,
    fault: Option<&str>,
) -> (
    std::sync::Arc<RuntimeServices>,
    String,
    ProjectionQueryContext,
) {
    use crate::{
        AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
        RuntimeTransactionEventInput,
    };
    use harness_contract::execution_graph::*;
    use serde_json::json;
    fn append(
        store: &crate::RuntimeEventStore,
        stream: &str,
        key: &str,
        kind: &str,
        scope: RuntimeEventScope,
        actor: &str,
        payload: serde_json::Value,
        mut expected: Vec<ExpectedStreamRevision>,
    ) -> DurableRuntimeEvent {
        if !expected.iter().any(|source| source.stream_id == stream) {
            expected.push(ExpectedStreamRevision {
                stream_id: stream.into(),
                expected_revision: store.stream_revision(stream).unwrap(),
            });
        }
        store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("fixture-{key}"),
                expected_streams: expected,
                events: vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: stream.into(),
                        scope,
                        kind: kind.into(),
                        status: Some("completed".into()),
                        actor: Some(actor.into()),
                        refs: vec![],
                        payload,
                    },
                    idempotency_key: Some(key.into()),
                    schema_version: 1,
                }],
            })
            .unwrap();
        store
            .event_by_idempotency_key(stream, key)
            .unwrap()
            .unwrap()
    }
    let services = RuntimeServices::in_memory().unwrap();
    let store = services.event_store();
    let commits = services.commit_service();
    let id = "read-graph";
    let node_id = if agent { "agent-node" } else { "root-node" };
    let mut graph = ExecutionGraph::new("read evidence fixed originals");
    graph.id = id.into();
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: "read-session".into(),
        turn_id: "read-turn".into(),
        root_task_id: "task".into(),
        task_id: "task".into(),
        generation: 1,
    });
    let packet = if agent {
        use harness_contract::agent::*;
        let identity = harness_contract::execution::ExecutionIdentity::for_task_graph(
            "read-principal",
            services.workspace_key(),
            "mission",
            "task",
            "read-session",
            "read-turn",
            id,
        )
        .unwrap();
        let assignment = AgentAssignment {
            execution_identity: harness_contract::execution::ExecutionIdentity::for_agent_node(
                &identity, "read-run", node_id,
            )
            .unwrap(),
            definition_ref: AgentDefinitionRevisionRef::new(
                AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/read-test").unwrap(),
                1,
            )
            .unwrap(),
            instance_id: "read-agent".into(),
            run_id: "read-run".into(),
            role_id: "reader".into(),
            task_id: "task".into(),
            root_task_id: "task".into(),
            session_id: "read-session".into(),
            mission_id: "mission".into(),
            team_run_id: None,
            graph_id: id.into(),
            node_id: node_id.into(),
            scope_refs: vec![],
            capability_policy: vec![],
        };
        let mut binding = AgentBindingSnapshot {
            binding_id: "read-binding".into(),
            definition_ref: assignment.definition_ref.clone(),
            definition_digest: "sha256:fixed-definition".into(),
            instructions: "read".into(),
            instance: AgentInstanceRef {
                instance_id: "read-agent".into(),
                role_slot_id: None,
            },
            executor: if matches!(fault, Some("process" | "transport")) {
                AgentExecutorPolicy::ProcessJsonl {
                    command_ref: "fixture-process".into(),
                    command_digest: "sha256:fixed-process".into(),
                }
            } else {
                AgentExecutorPolicy::CowdNative
            },
            model_policy: AgentModelPolicy {
                profile: "test".into(),
                allowed_models: vec![],
                fallback_allowed: false,
            },
            effective_capabilities: vec![],
            skill_refs: vec![],
            tool_contract_refs: vec!["read_file".into()],
            skill_tool_pins: None,
            skill_tool_refs: None,
            data_lease: AgentDataLease {
                session_id: "read-session".into(),
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
            binding_digest: String::new(),
        };
        binding.binding_digest = crate::agent::binding::recompute_binding_digest(&binding).unwrap();
        Some(AgentTaskPacket {
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
            agentic_binding: Some(AgenticExecutionBinding {
                program_id: if fault == Some("program") {
                    "foreign-program"
                } else {
                    "read-program"
                }
                .into(),
                agent_id: "read-agent".into(),
                membership_id: "membership".into(),
                team_id: "team".into(),
                task_team_id: "team".into(),
                source_spec_revision: 1,
                task_purpose: None,
                focus: AgenticExecutionFocus::Coordination {
                    wake_ref: "read".into(),
                },
            }),
            evidence_refs: vec![],
            resource_scopes: vec![],
            allowed_tools: vec!["read_file".into()],
            allowed_skills: vec![],
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            policy_revision: 1,
            model_lease: "test".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "read-budget",
                "read-agent",
                "read",
                10,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: Some(binding),
            managed_invocation: None,
            idempotency_key: "read-packet".into(),
        })
    } else {
        None
    };
    let mut node = ExecutionNodeSpec::new(
        if agent {
            ExecutionNodeKind::AgentTask
        } else {
            ExecutionNodeKind::ToolBatch
        },
        if agent { "agent_task" } else { "tool_batch" },
        packet
            .as_ref()
            .map(|p| serde_json::to_string(p).unwrap())
            .unwrap_or_else(|| "{}".into()),
    );
    node.id = node_id.into();
    graph.nodes.push(node);
    graph = commits.register_graph(graph).unwrap().graph;
    graph = commits
        .transition_node(&graph, node_id, ExecutionNodeStatus::Ready, None, vec![])
        .unwrap()
        .graph;
    graph = commits
        .bind_and_start_node(
            &graph,
            node_id,
            crate::execution_core::graph::ExecutionNodeBinding {
                executor_kind: if agent { "agent_task" } else { "tool_batch" }.into(),
                ticket_idempotency_key: "read-ticket".into(),
                attempt: 1,
                resource_lease_refs: vec![],
                scope_lease_ref: None,
                worktree_lease_ref: None,
            },
        )
        .unwrap()
        .graph;
    let owner = store
        .latest_for_stream(&format!("{id}:node:{node_id}"))
        .unwrap()
        .unwrap()
        .activity_binding()
        .unwrap();
    let mut opened = RuntimeEventInput {
        stream_id: "agentic-program:read-program".into(),
        scope: RuntimeEventScope::Program,
        kind: "agentic.program_opened".into(),
        status: Some("open".into()),
        actor: Some("agent_action_service".into()),
        refs: vec![RuntimeEventRef {
            kind: "execution_graph".into(),
            id: id.into(),
        }],
        payload: json!({"objective_id":"read-objective","session_id":"read-session","turn_id":"read-turn","root_execution_id":id}),
    };
    // Explicit original Program opening, not a projection mutation.
    store.append(opened.clone()).unwrap();
    opened.payload = serde_json::Value::Null;
    drop(opened);
    for n in 0..unrelated {
        store
            .append(RuntimeEventInput {
                stream_id: "session:read-session".into(),
                scope: RuntimeEventScope::Session,
                kind: "fixture.unrelated".into(),
                status: None,
                actor: Some("fixture".into()),
                refs: vec![],
                payload: if fault == Some("oversized") {
                    json!({"data":"x".repeat(1024*1024-1024)})
                } else {
                    json!({"n":n})
                },
            })
            .unwrap();
    }
    let declared = json!({"resource":"file","operation":"read","target":if fault==Some("absolute") {"/private/fixture-workspace/src/a.rs"}else{"src/a.rs"}});
    let effect = json!({"tool_id":"read_file","descriptor_hash":"fixed-read-descriptor","effect_kind":"read","idempotency":"idempotent","scopes":[declared],"required_permission":"read-only","approval_class":"none","uses_network":false,"spawns_process":false,"mutates_packages":false,"mutates_system":false});
    let scope = json!({"version":2,"descriptor_hash":"fixed-read-descriptor","complete":true,"targets":[{"path":{"workspace_id":services.workspace_key(),"repository_id":"read-repository","workspace_relative_path":"src/a.rs","repository_relative_path":"src/a.rs","object_kind":"file"},"declared_scope":declared,"access_mode":"read","coverage":"scoped_content","exact":true,"physical_identity":"fixed-object","alias_checked":true}],"scope_purposes":[{"declared_scope":declared,"purpose":"input_read"}]});
    let root_owner = json!({"binding":owner,"attempt":1,"graph_revision":graph.revision});
    let root_context = json!({"execution_id":id,"session_id":"read-session","turn_id":"read-turn"});
    let replay = json!({"call_id":"read-call","parent_execution":if agent {json!({"graph_id":id})}else{serde_json::Value::Null},"invocation":{"effect":effect}});
    let intent_payload = json!({"effect":effect,"replay":replay,"root_effect_owner":root_owner,"root_effect_context":root_context,"resolved_effect_scope":scope,"tool_use_id":"read-call","tool_name":"read_file","idempotency_key":"read-key","input_sha256":"read-input"});
    append(
        store,
        "read-primary",
        "read-key:read-intent",
        "execution.read.intent",
        RuntimeEventScope::ExecutionNode,
        "governed_tool",
        intent_payload.clone(),
        vec![ExpectedStreamRevision {
            stream_id: id.into(),
            expected_revision: graph.revision,
        }],
    );
    let step:harness_contract::step::StepIdentity=serde_json::from_value(json!({"step_id":"read-step","session_id":"read-session","turn_id":"read-turn","execution_id":id,"actor_ref":if agent {"read-principal"}else{"root"},"run_ref":if agent {Some("read-run")}else{None},"attempt":1,"build_ref":"read-fixture"})).unwrap();
    let mut seal = context_evidence::ReadAcquisitionSeal {
        schema_version: 1,
        primary_stream: "read-primary".into(),
        primary_key: "read-key:read-receipt".into(),
        index_stream: format!("execution-agent-receipts:{id}:{node_id}:1"),
        index_key: "agent-tool-receipt:read-key".into(),
        session_id: "read-session".into(),
        invocation_id: "read-call".into(),
        tool_name: "read_file".into(),
        graph_id: id.into(),
        node_id: node_id.into(),
        attempt: 1,
        step,
        origin: json!({"origin_step_id":"read-step","call_id":"read-call","catalog_digest":"fixed","source":{"kind":"provider","attempt_id":"read-attempt"}}),
        input_sha256: "read-input".into(),
        body: context_evidence::ReadBodySeal {
            schema_version: 1,
            source_ref: "src/a.rs".into(),
            source_sha256:
                "sha256:cf4771a548f1f5000749e3287eb205ad8005b26fc82a3115ee8c76f5247bce4f".into(),
            source_bytes: 20,
            output_sha256:
                "sha256:81cc5b17018674b401b42f35ba07bb79e211239c23bffe658da1577e3e646877".into(),
            output_bytes: 4,
            extent: context_evidence::ReadExtent::FileLines {
                start_line: 2,
                line_count: 1,
                total_lines: 4,
                ends_with_newline: true,
                body_sha256:
                    "sha256:81cc5b17018674b401b42f35ba07bb79e211239c23bffe658da1577e3e646877".into(),
                body_bytes: 4,
                source_omitted: true,
            },
        },
    };
    if matches!(fault, Some("process" | "transport")) {
        seal.origin = serde_json::json!({"origin_step_id":"read-step","call_id":"read-call","catalog_digest":"fixed","source":{"kind":"process","worker_id":"read-run","sequence":1}});
        seal.step.actor_ref = Some("read-agent".into());
    }
    let mut payload = intent_payload;
    payload["read_acquisition_v1"] = serde_json::to_value(&seal).unwrap();
    payload["outcome"] =
        json!({"status":"executed","tool_use_id":"read-call","tool_name":"read_file"});
    let input = |stream: &str, key: &str, kind: &str, index: u32| {
        let mut p = payload.clone();
        p["sequence"] = json!(1);
        if index == 1 {
            p["effect_kind"] = json!("read");
        }
        RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: stream.into(),
                scope: RuntimeEventScope::ExecutionNode,
                kind: kind.into(),
                status: Some("completed".into()),
                actor: Some("governed_tool".into()),
                refs: vec![],
                payload: p,
            },
            idempotency_key: Some(key.into()),
            schema_version: 1,
        }
    };
    store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "read-receipts".into(),
            expected_streams: vec![
                ExpectedStreamRevision {
                    stream_id: "read-primary".into(),
                    expected_revision: 1,
                },
                ExpectedStreamRevision {
                    stream_id: seal.index_stream.clone(),
                    expected_revision: 0,
                },
            ],
            events: vec![
                input(
                    "read-primary",
                    &seal.primary_key,
                    "execution.read.receipt",
                    0,
                ),
                input(
                    &seal.index_stream,
                    &seal.index_key,
                    "execution.agent_tool.receipt",
                    1,
                ),
            ],
        })
        .unwrap();
    let primary = store
        .event_by_idempotency_key("read-primary", &seal.primary_key)
        .unwrap()
        .unwrap();
    let index = store
        .event_by_idempotency_key(&seal.index_stream, &seal.index_key)
        .unwrap()
        .unwrap();
    let recipient = if let Some(packet) = &packet {
        ReadDeliveryRecipient::AgentTask {
            identity: packet.assignment.execution_identity.clone(),
        }
    } else {
        let mut binding = owner.clone();
        binding.activity_id = format!("activity:execution:{id}:owner:read-call");
        binding.tool_call_id = Some("read-call".into());
        binding.tool_contract_id = Some("read_file".into());
        let mut completed = RuntimeEventInput {
            stream_id: "session:read-session".into(),
            scope: RuntimeEventScope::Tool,
            kind: "tool.invocation.completed".into(),
            status: Some("completed".into()),
            actor: Some("conversation_runtime".into()),
            refs: vec![
                RuntimeEventRef {
                    kind: "tool_call".into(),
                    id: "read-call".into(),
                },
                RuntimeEventRef {
                    kind: "tool_invocation".into(),
                    id: "read-invocation".into(),
                },
                RuntimeEventRef {
                    kind: "execution".into(),
                    id: id.into(),
                },
                RuntimeEventRef {
                    kind: "session".into(),
                    id: "read-session".into(),
                },
                RuntimeEventRef {
                    kind: "turn".into(),
                    id: "read-turn".into(),
                },
            ],
            payload: json!({"status":"completed","session_id":"read-session","tool_call_id":"read-call","tool_name":"read_file","invocation_id":"read-invocation","is_error":false}),
        };
        completed = completed.with_activity_binding(binding.clone()).unwrap();
        let completed = store.append(completed).unwrap();
        ReadDeliveryRecipient::Root {
            owner: context_evidence::RootReadOwner {
                stream_id: completed.stream_id,
                sequence: completed.sequence,
                event_id: completed.event_id,
                payload_digest: context_evidence::read_payload_digest(&completed.payload),
                binding,
            },
            identity: None,
        }
    };
    let transport = context_evidence::ReadDeliveryTransport::NativeProviderContinuation {
        request_id: "read-wire-request".into(),
        request_sequence: 1,
        provider_attempt: 1,
        step_render: harness_contract::step::StepRenderRef {
            step_id: "read-step".into(),
            attempt_id: "read-attempt".into(),
            catalog_digest: "fixed".into(),
            render_digest: "fixed-render".into(),
        },
        wire_body_sha256: "sha256:fixed-wire".into(),
        wire_artifact: harness_contract::context::ArtifactRef {
            selector: "artifact://fixed-wire".into(),
            sha256: "sha256:fixed-wire".into(),
            bytes: 2,
            media_type: "application/json".into(),
            durability: harness_contract::context::EvidenceDurability::Durable,
            visibility_scope: "session:read-session".into(),
        },
        packed_sequence: 1,
        outcome_sequence: 2,
        model: "fixed-no-provider".into(),
        raw_ref: harness_contract::reality::EvidenceRef::new("wire", "fixed-wire"),
    };
    let mut material = context_evidence::ReadDeliveryMaterial {
        schema_version: 1,
        grade: context_evidence::ReadDeliveryGrade::ProviderModelObserved,
        acquisition: seal.clone(),
        acquisition_event_id: primary.event_id,
        acquisition_cursor: primary.commit_cursor,
        index_event_id: index.event_id,
        session_id: "read-session".into(),
        execution_id: id.into(),
        turn_id: "read-turn".into(),
        tool_invocation_id: "read-call".into(),
        recipient,
        presentation_sha256:
            "sha256:81cc5b17018674b401b42f35ba07bb79e211239c23bffe658da1577e3e646877".into(),
        presentation_bytes: 4,
        body_omitted: false,
        delivered_extent: Some(seal.body.extent),
        transport,
    };
    if matches!(fault, Some("process" | "transport")) {
        material.grade = context_evidence::ReadDeliveryGrade::AgentChannelSend;
        material.presentation_sha256 = context_evidence::read_sha256(b"bbbb");
        material.presentation_bytes = 4;
        material.transport = context_evidence::ReadDeliveryTransport::ProcessChannelSend {
            worker_id: "read-run".into(),
            request_id: "read-call".into(),
            sequence: 1,
            step_id: "read-step".into(),
            request_fingerprint: "fixed-process-response".into(),
            response_sha256: context_evidence::read_sha256(b"bbbb"),
            response_bytes: 4,
        };
        let packet = packet.as_ref().unwrap();
        let identity = json!([
            packet.session_id(),
            packet.graph_id(),
            packet.node_id(),
            packet.run_id(),
            packet.agent_id(),
            packet.attempt,
            packet
                .binding
                .as_ref()
                .map(|binding| binding.binding_digest.as_str())
        ]);
        let stream = format!(
            "agent-process-transport:{:x}",
            Sha256::digest(identity.to_string().as_bytes())
        );
        let key = format!("completed:{:x}", Sha256::digest(b"read-call"));
        if fault == Some("process") {
            append(
                store,
                &stream,
                &key,
                "agent.process_request_completed",
                RuntimeEventScope::ExecutionNode,
                "runtime.process_jsonl",
                json!({"fingerprint":"fixed-process-response","response":{"output":"bbbb"}}),
                vec![],
            );
        }
    }
    if fault == Some("attempt") {
        material.acquisition.attempt = 2;
    }
    if fault == Some("primary") {
        material.acquisition.primary_key = "missing-primary".into();
    }
    if fault == Some("index") {
        material.acquisition.index_key = "missing-index".into();
    }
    if fault == Some("grade") {
        material.grade = context_evidence::ReadDeliveryGrade::AgentChannelSend;
    }
    if fault == Some("omitted") {
        material.body_omitted = true;
        material.delivered_extent = None;
    }
    if matches!(fault, Some("parent-only" | "parent-leaf")) {
        store
            .append(RuntimeEventInput {
                stream_id: "session:read-session".into(),
                scope: RuntimeEventScope::Session,
                kind: "tool.batch.completed".into(),
                status: Some("completed".into()),
                actor: Some("conversation_runtime".into()),
                refs: vec![],
                payload: json!({"tool_name":"batch","path":"src/a.rs","success":true}),
            })
            .unwrap();
    }
    if fault != Some("parent-only") {
        store
            .append(RuntimeEventInput {
                stream_id: "session:read-session".into(),
                scope: RuntimeEventScope::Session,
                kind: if matches!(fault, Some("process" | "transport")) {
                    context_evidence::PROCESS_DELIVERY_KIND
                } else {
                    context_evidence::NATIVE_DELIVERY_KIND
                }
                .into(),
                status: Some(
                    if matches!(fault, Some("process" | "transport")) {
                        "sent"
                    } else {
                        "confirmed"
                    }
                    .into(),
                ),
                actor: Some(
                    if matches!(fault, Some("process" | "transport")) {
                        "runtime.process_jsonl"
                    } else {
                        "conversation_runtime"
                    }
                    .into(),
                ),
                refs: vec![],
                payload: serde_json::to_value(material).unwrap(),
            })
            .unwrap();
    }
    let context = ProjectionQueryContext {
        principal: "read-principal".into(),
        workspace_id: services.workspace_key().into(),
        session_scopes: vec!["read-session".into()],
        mission_scopes: vec!["mission".into()],
        visibility_grants: vec!["read:src".into()],
        detail_scope: ProjectionDetailScope::Full,
        authorization_revision: 1,
    };
    (services, id.into(), context)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn original_root_and_agent_partial_delivery_are_authenticated() {
        for agent in [false, true] {
            let (services, id, context) = read_evidence_fixture(agent, 0);
            let page = read_evidence(&services, &id, &context, Default::default())
                .await
                .unwrap();
            assert!(!page.incomplete, "agent={agent}: {page:?}");
            assert!(page.exhausted);
            assert_eq!(page.reads.len(), 1);
            let read = &page.reads[0];
            assert_eq!(read.path.workspace_relative_path, "src/a.rs");
            assert_eq!(read.source_bytes, 20);
            assert!(read.body_delivered);
            assert_eq!(read.grade, ReadEvidenceGrade::ProviderModelObserved);
            assert!(matches!(
                read.delivered_extent,
                Some(ReadEvidenceExtent::FileLines {
                    start_line: 2,
                    line_count: 1,
                    total_lines: 4,
                    source_omitted: true,
                    ..
                })
            ));
            assert_eq!(
                matches!(
                    read.recipient,
                    ReadEvidenceRecipient::Agent { attempt: 1, .. }
                ),
                agent
            );
            let store = services.event_store();
            let original = store
                .event_by_idempotency_key("read-primary", "read-key:read-receipt")
                .unwrap()
                .unwrap();
            assert_eq!(read.acquisition.event_id, original.event_id);
            assert_eq!(read.acquisition.commit_cursor, read.index.commit_cursor);
            assert!(read.delivery.commit_cursor > read.acquisition.commit_cursor);
            services.shutdown_execution().await;
        }
    }
    #[tokio::test]
    async fn irrelevant_history_continues_and_invalid_cursor_has_no_success() {
        let (services, id, context) = read_evidence_fixture(false, 380);
        let mut query = ReadEvidenceQuery::default();
        let mut pages = 0;
        let mut found = 0;
        let mut through = None;
        loop {
            let page = read_evidence(&services, &id, &context, query.clone())
                .await
                .unwrap();
            pages += 1;
            assert!(!page.incomplete, "{page:?}");
            if let Some(cut) = through {
                assert_eq!(page.through, cut);
            } else {
                through = Some(page.through);
            }
            found += page.reads.len();
            assert!(page.next_after_sequence > query.after_sequence || page.exhausted);
            if page.exhausted {
                break;
            }
            query.after_sequence = page.next_after_sequence;
            query.through = through;
            assert!(pages < 10);
        }
        assert!(pages > 1);
        assert_eq!(found, 1);
        assert!(read_evidence(
            &services,
            &id,
            &context,
            ReadEvidenceQuery {
                through: Some(u64::MAX),
                ..Default::default()
            }
        )
        .await
        .is_err());
        assert!(read_evidence(
            &services,
            &id,
            &context,
            ReadEvidenceQuery {
                after_sequence: u64::MAX,
                ..Default::default()
            }
        )
        .await
        .is_err());
        services.shutdown_execution().await;
    }
    #[tokio::test]
    async fn bounded_originals_do_not_borrow_future_keys_or_transactions() {
        let (services, id, context) = read_evidence_fixture(false, 0);
        let mut budget = RequestBudget::default();
        budget.capture_source_fence(&services).unwrap();
        let mut reader = BudgetReader {
            services: &services,
            budget: &mut budget,
            through: 0,
            failure: None,
        };
        assert!(reader
            .event_by_idempotency_key("read-primary", "read-key:read-receipt")
            .unwrap()
            .is_none());
        assert!(reader
            .transaction_stream_revision_exact("fixture-read-key:read-intent", &id)
            .unwrap_err()
            .contains("bounded original intent"));
        let original = services
            .event_store()
            .latest_for_stream(&id)
            .unwrap()
            .unwrap();
        assert!(reader.resolve_graph_record(&original).is_err());
        let page = read_evidence(&services, &id, &context, Default::default())
            .await
            .unwrap();
        assert!(!page.incomplete);
        services.shutdown_execution().await;
    }
    #[tokio::test]
    async fn raw_hydration_reuses_charges_and_rejects_identity_poison() {
        let (services, _, _) = read_evidence_fixture(false, 0);
        let objective = "q".repeat(700 * 1024);
        let mut graph = ExecutionGraph::new(&objective);
        graph.id = "large-original".into();
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "read-session".into(),
            turn_id: "read-turn".into(),
            root_task_id: "task".into(),
            task_id: "task".into(),
            generation: 1,
        });
        services.commit_service().register_graph(graph).unwrap();
        let original = services
            .event_store()
            .latest_for_stream("large-original")
            .unwrap()
            .unwrap();
        assert_eq!(original.payload["body"]["kind"], "artifact_ref");
        let mut budget = RequestBudget::default();
        let cut = services.event_store().durable_commit_cursor().unwrap();
        let lookup = budget
            .canonical_record(
                &services,
                &original.stream_id,
                original.idempotency_key.as_deref().unwrap(),
                cut,
            )
            .unwrap()
            .unwrap();
        assert_eq!(lookup.as_ref(), &original);
        let before = budget.consumed_work();
        let raw = budget
            .resolve_raw_graph_record(&services, &original)
            .unwrap();
        assert_eq!(budget.consumed_work(), before);
        assert_eq!(raw["graph"]["objective"], objective);
        assert_eq!(raw["graph"]["id"], "large-original");
        assert!(raw.get("_runtime_activity_binding").is_none());
        let hydrated = budget.resolve_record(&services, original.clone()).unwrap();
        assert_eq!(hydrated.payload["graph"]["objective"], objective);
        assert_eq!(
            hydrated.payload["_runtime_activity_binding"],
            original.payload["_runtime_activity_binding"]
        );
        assert_eq!(hydrated.event_id, original.event_id);
        assert_eq!(hydrated.commit_cursor, original.commit_cursor);
        let paid = budget.consumed_work();
        assert_eq!(
            budget.resolve_record(&services, original.clone()).unwrap(),
            hydrated
        );
        assert_eq!(budget.consumed_work(), paid);
        let mut foreign = original.clone();
        foreign.commit_cursor += 1;
        assert!(budget
            .resolve_raw_graph_record(&services, &foreign)
            .is_err());
        let mut oversized = original.clone();
        oversized.event_id.push_str("oversized");
        oversized.payload["bytes"] = serde_json::json!(1024 * 1024 + 1);
        let mut empty = RequestBudget::default();
        assert!(empty.charge_record(&oversized).is_err());
        let mut unknown = original;
        unknown.event_id.push_str("unknown");
        unknown.payload.as_object_mut().unwrap().remove("bytes");
        let mut empty = RequestBudget::default();
        assert!(empty.charge_record(&unknown).is_err());
        services.shutdown_execution().await;
    }
}

#[cfg(test)]
mod counterexamples {
    use super::*;
    #[tokio::test]
    async fn single_fault_delivery_cannot_skip_a_candidate_or_borrow_program() {
        for (agent, fault) in [
            (false, "primary"),
            (false, "index"),
            (true, "attempt"),
            (true, "program"),
            (false, "grade"),
        ] {
            let (services, id, context) = read_evidence_fixture_fault(agent, 0, Some(fault));
            let source = services
                .event_store()
                .stream_revision("session:read-session")
                .unwrap();
            let page = read_evidence(&services, &id, &context, Default::default())
                .await
                .unwrap();
            assert!(page.incomplete, "{fault}: {page:?}");
            assert_eq!(
                page.reason,
                Some(if matches!(fault, "attempt" | "program") {
                    ReadEvidenceIncompleteReason::UnsupportedIdentity
                } else {
                    ReadEvidenceIncompleteReason::AuthenticationFailed
                }),
                "{fault}"
            );
            assert!(!page.exhausted);
            assert!(page.reads.is_empty());
            assert!(
                page.next_after_sequence < source,
                "{fault}: failed candidate skipped"
            );
            services.shutdown_execution().await;
        }
    }
    #[tokio::test]
    async fn parent_success_is_not_a_leaf_and_omission_is_not_content() {
        for (fault, expected) in [("parent-only", 0), ("parent-leaf", 1), ("omitted", 1)] {
            let (services, id, context) = read_evidence_fixture_fault(false, 0, Some(fault));
            let page = read_evidence(&services, &id, &context, Default::default())
                .await
                .unwrap();
            assert!(!page.incomplete, "{fault}: {page:?}");
            assert!(page.exhausted);
            assert_eq!(page.reads.len(), expected, "{fault}");
            if fault == "omitted" {
                assert!(!page.reads[0].body_delivered);
                assert!(page.reads[0].delivered_extent.is_none());
            }
            if fault == "parent-leaf" {
                assert!(page.reads[0].body_delivered);
                assert_eq!(page.reads[0].tool_name, "read_file");
            }
            services.shutdown_execution().await;
        }
    }
    #[tokio::test]
    async fn no_resource_grant_reveals_no_path_digest_or_receipt() {
        let (services, id, mut context) = read_evidence_fixture(false, 0);
        let legitimate = read_evidence(&services, &id, &context, Default::default())
            .await
            .unwrap();
        assert_eq!(legitimate.reads.len(), 1);
        context.visibility_grants.clear();
        let observer = read_evidence(&services, &id, &context, Default::default())
            .await
            .unwrap();
        assert!(!observer.incomplete);
        assert!(observer.reads.is_empty());
        let wire = serde_json::to_string(&observer).unwrap();
        for secret in [
            "src/a.rs",
            "read-primary",
            legitimate.reads[0].source_sha256.as_str(),
        ] {
            assert!(!wire.contains(secret));
        }
        // Same authorized Session and resource grant, different execution.
        // Empty proof here must not be attributable to the observer's missing grant.
        context.visibility_grants = vec!["read:src".into()];
        let mut other = ExecutionGraph::new("unrelated authorized execution");
        other.id = "other-read-graph".into();
        other.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "read-session".into(),
            turn_id: "read-turn".into(),
            root_task_id: "task".into(),
            task_id: "task".into(),
            generation: 1,
        });
        services.commit_service().register_graph(other).unwrap();
        let foreign_execution =
            read_evidence(&services, "other-read-graph", &context, Default::default())
                .await
                .unwrap();
        assert!(!foreign_execution.incomplete);
        assert!(foreign_execution.reads.is_empty());
        context.session_scopes = vec!["other-session".into()];
        assert!(matches!(
            read_evidence(&services, &id, &context, Default::default()).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        services.shutdown_execution().await;
    }
    #[tokio::test]
    async fn unchanged_head_and_cut_cannot_hide_mutation_fence_mismatch() {
        let (services, id, context) = read_evidence_fixture(false, 0);
        let original = read_evidence(&services, &id, &context, Default::default())
            .await
            .unwrap();
        assert_eq!(original.reads.len(), 1);
        let sources = original
            .sources
            .iter()
            .map(|s| (s.stream_id.clone(), s.revision))
            .collect();
        let mut page = original.clone();
        let mut budget = RequestBudget::default();
        budget.capture_source_fence(&services).unwrap();
        budget.request_source.as_mut().unwrap().mutation_epoch += 1; // old captured identity differs; every real stream head stays equal
        finalize_page(&services, &mut budget, &sources, &mut page);
        assert!(page.incomplete);
        assert_eq!(
            page.reason,
            Some(ReadEvidenceIncompleteReason::SourceChanged)
        );
        assert!(page.reads.is_empty());
        assert_eq!(page.through, original.through);
        let mut page = original;
        let mut budget = RequestBudget::default();
        budget.capture_source_fence(&services).unwrap();
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: "session:read-session".into(),
                scope: RuntimeEventScope::Session,
                kind: "fixture.late".into(),
                actor: None,
                status: None,
                refs: vec![],
                payload: serde_json::json!({}),
            })
            .unwrap();
        finalize_page(&services, &mut budget, &sources, &mut page);
        assert!(page.incomplete);
        assert!(page.reads.is_empty());
        assert_eq!(
            page.reason,
            Some(ReadEvidenceIncompleteReason::SourceChanged)
        );
        services.shutdown_execution().await;
    }
    #[tokio::test]
    async fn limits_and_zero_progress_budget_are_explicit_not_complete() {
        let (services, id, context) = read_evidence_fixture(false, 0);
        let mut budget = RequestBudget::default();
        let source = "session:read-session";
        let head = services.event_store().stream_revision(source).unwrap();
        let cut = services.event_store().durable_commit_cursor().unwrap();
        let page = budget
            .limited_source_page(&services, source, 0, head, 1, 1024 * 1024, cut)
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].sequence, 1);
        let mut tiny = RequestBudget::default();
        assert!(tiny
            .limited_source_page(&services, source, 0, head, 1, 1, cut)
            .is_err());
        assert!(read_evidence(
            &services,
            &id,
            &context,
            ReadEvidenceQuery {
                limit: 17,
                ..Default::default()
            }
        )
        .await
        .is_err());
        services.shutdown_execution().await;
        let (services, id, context) = read_evidence_fixture_fault(false, 1, Some("oversized"));
        let page = read_evidence(&services, &id, &context, Default::default())
            .await
            .unwrap();
        assert!(page.incomplete);
        assert!(!page.exhausted);
        assert_eq!(page.next_after_sequence, 0);
        assert!(page.reads.is_empty());
        services.shutdown_execution().await;
    }
}

#[cfg(test)]
mod process_counterexample {
    use super::*;
    #[tokio::test]
    async fn process_needs_its_actual_same_cut_response_not_only_a_delivery_claim() {
        for fault in ["process", "transport"] {
            let (services, id, context) = read_evidence_fixture_fault(true, 0, Some(fault));
            let page = read_evidence(&services, &id, &context, Default::default())
                .await
                .unwrap();
            if fault == "process" {
                assert!(!page.incomplete, "{page:?}");
                assert_eq!(page.reads.len(), 1);
                assert_eq!(page.reads[0].grade, ReadEvidenceGrade::AgentChannelSend);
                assert!(page.reads[0].body_delivered);
            } else {
                assert!(page.incomplete);
                assert!(page.reads.is_empty());
                assert!(!page.exhausted);
            }
            services.shutdown_execution().await;
        }
    }
}

#[cfg(test)]
mod canonical_scope_tests {
    use super::*;
    #[tokio::test]
    async fn relative_and_absolute_original_scopes_have_one_visible_canonical_object() {
        for fault in [None, Some("absolute")] {
            let (services, id, context) = read_evidence_fixture_fault(false, 0, fault);
            let page = read_evidence(&services, &id, &context, Default::default())
                .await
                .unwrap();
            assert!(!page.incomplete, "{page:?}");
            assert_eq!(page.reads.len(), 1);
            assert_eq!(page.reads[0].path.workspace_relative_path, "src/a.rs");
            assert_eq!(page.reads[0].scope.target.as_deref(), Some("src/a.rs"));
            assert_eq!(
                page.reads[0].scope.operation,
                harness_contract::policy::PermissionOperation::Read
            );
            assert!(!serde_json::to_string(&page)
                .unwrap()
                .contains("/private/fixture-workspace"));
            services.shutdown_execution().await;
        }
    }
}
