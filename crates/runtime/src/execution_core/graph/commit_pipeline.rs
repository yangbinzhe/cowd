//! Commit validation, graph projection, and cross-team delivery pipeline.

use super::*;

pub(super) fn maybe_checkpoint(
    graph: &ExecutionGraph,
    event: ExecutionGraphEvent,
) -> Result<ExecutionGraphEvent, ExecutionCommitError> {
    if matches!(
        event,
        ExecutionGraphEvent::Planned { .. } | ExecutionGraphEvent::Checkpoint { .. }
    ) {
        return Ok(event);
    }
    let delta_bytes = event.estimated_delta_bytes();
    let snapshot_bytes = crate::execution_core::hot_state::estimate_graph_bytes(graph).max(1);
    let topology_interval = (256 / graph.nodes.len().max(1)).clamp(8, 64) as u64;
    if delta_bytes.saturating_mul(4) >= snapshot_bytes.saturating_mul(3)
        || graph.revision % topology_interval == 0
    {
        return Ok(ExecutionGraphEvent::Checkpoint {
            cause: event.kind().to_string(),
            graph: graph.clone(),
        });
    }
    Ok(event)
}

pub(super) fn validate_executor_domain_events(
    domain_events: &[RuntimeTransactionEventInput],
) -> Result<(), ExecutionCommitError> {
    if let Some(event) = domain_events.iter().find(|event| {
        !matches!(
            event.event.scope,
            RuntimeEventScope::ExecutionNode
                | RuntimeEventScope::Goal
                | RuntimeEventScope::SessionInput
                | RuntimeEventScope::Relation
                | RuntimeEventScope::Team
                // Approval decisions are generated from a canonical
                // ExecutionGraph command and must commit atomically with the
                // node transition.
                | RuntimeEventScope::Approval
        ) && !crate::authorization_negotiator::is_controlled_recovery_terminal_event(&event.event)
    }) {
        return Err(ExecutionCommitError::ProtectedDomainScope(
            event.event.scope.as_str().to_string(),
        ));
    }
    Ok(())
}

pub(super) fn graph_identity_refs(graph: &ExecutionGraph) -> Vec<RuntimeEventRef> {
    let mut refs = vec![RuntimeEventRef {
        kind: "execution_graph".to_string(),
        id: graph.id.clone(),
    }];
    for packet in graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .filter_map(|node| serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok())
    {
        let identity = &packet.assignment.execution_identity;
        refs.extend([
            RuntimeEventRef {
                kind: "principal".to_string(),
                id: identity.principal_id().to_string(),
            },
            RuntimeEventRef {
                kind: "workspace".to_string(),
                id: identity.workspace_id().to_string(),
            },
            RuntimeEventRef {
                kind: "mission".to_string(),
                id: packet.mission_id().to_string(),
            },
            RuntimeEventRef {
                kind: "task".to_string(),
                id: packet.task_id().to_string(),
            },
            RuntimeEventRef {
                kind: "session".to_string(),
                id: packet.session_id().to_string(),
            },
            RuntimeEventRef {
                kind: "agent_run".to_string(),
                id: packet.run_id().to_string(),
            },
            RuntimeEventRef {
                kind: "execution_node".to_string(),
                id: packet.node_id().to_string(),
            },
        ]);
        if let Some(turn_id) = identity.turn_id() {
            refs.push(RuntimeEventRef {
                kind: "turn".to_string(),
                id: turn_id.to_string(),
            });
        }
        if let Some(team_run_id) = packet.team_id() {
            refs.push(RuntimeEventRef {
                kind: "team_run".to_string(),
                id: team_run_id.to_string(),
            });
        }
    }
    refs.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.id.cmp(&right.id))
    });
    refs.dedup_by(|left, right| left.kind == right.kind && left.id == right.id);
    refs
}

pub(super) fn tool_effect_refs(
    request: &crate::RuntimeToolExecutionRequest,
) -> Vec<RuntimeEventRef> {
    let mut refs = vec![RuntimeEventRef {
        kind: "tool_invocation".to_string(),
        id: request.tool_use_id.clone(),
    }];
    if let Some(parent) = &request.parent_execution {
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: parent.execution_id.clone(),
        });
        refs.push(RuntimeEventRef {
            kind: "execution_node".to_string(),
            id: parent.node_id.clone(),
        });
    }
    refs
}

pub(super) fn delegated_agent_receipt_stream_id(
    request: &crate::RuntimeToolExecutionRequest,
) -> Option<String> {
    let parent = request.parent_execution.as_ref()?;
    let attempt = request.parent_execution_attempt?;
    Some(format!(
        "execution-agent-receipts:{}:{}:{attempt}",
        parent.execution_id, parent.node_id
    ))
}

pub(super) fn delegated_agent_receipt_key(request: &crate::RuntimeToolExecutionRequest) -> String {
    format!("agent-tool-receipt:{}", request.idempotency_key)
}

pub(super) fn delegated_agent_receipt_event(
    request: &crate::RuntimeToolExecutionRequest,
    effect_kind: ToolEffectKind,
    outcome: &crate::RuntimeToolExecutionOutcome,
) -> Option<RuntimeTransactionEventInput> {
    let stream_id = delegated_agent_receipt_stream_id(request)?;
    let mut refs = tool_effect_refs(request);
    if let Some(attempt) = request.parent_execution_attempt {
        refs.push(RuntimeEventRef {
            kind: "agent_attempt".to_string(),
            id: attempt.to_string(),
        });
    }
    Some(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id,
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution.agent_tool.receipt".to_string(),
            status: Some("completed".to_string()),
            actor: Some("governed_tool".to_string()),
            refs,
            payload: json!({
                "sequence": request.observation_wave_sequence,
                "effect_kind": effect_kind,
                "authorized_scopes": request.authorized_scopes,
                "outcome": bounded_tool_effect_outcome(outcome),
            }),
        },
        idempotency_key: Some(delegated_agent_receipt_key(request)),
        schema_version: 1,
    })
}

pub(super) fn bounded_tool_effect_outcome(
    outcome: &crate::RuntimeToolExecutionOutcome,
) -> crate::RuntimeToolExecutionOutcome {
    let mut bounded = outcome.clone();
    bounded.output = bounded.output.map(|output| {
        if output.chars().count() <= MAX_TOOL_EFFECT_RECEIPT_CHARS {
            output
        } else {
            let prefix = output
                .chars()
                .take(MAX_TOOL_EFFECT_RECEIPT_CHARS)
                .collect::<String>();
            format!(
                "{prefix}\n[effect receipt truncated; full output requires its artifact receipt]"
            )
        }
    });
    bounded.error = bounded
        .error
        .map(|error| error.chars().take(MAX_TOOL_EFFECT_RECEIPT_CHARS).collect());
    bounded
}

pub(super) fn tool_effect_outcome_requires_truncation(
    outcome: &crate::RuntimeToolExecutionOutcome,
) -> bool {
    outcome
        .output
        .as_deref()
        .is_some_and(|output| output.chars().count() > MAX_TOOL_EFFECT_RECEIPT_CHARS)
        || outcome
            .error
            .as_deref()
            .is_some_and(|error| error.chars().count() > MAX_TOOL_EFFECT_RECEIPT_CHARS)
}

pub(super) fn validate_readonly_tool_receipt(
    request: &crate::RuntimeToolExecutionRequest,
    payload: &serde_json::Value,
) -> Result<(), ExecutionCommitError> {
    let expected_hash = format!("sha256:{:x}", Sha256::digest(request.input.as_bytes()));
    let actual_hash = payload
        .get("input_sha256")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let actual_tool = payload
        .get("tool_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if actual_hash != expected_hash || actual_tool != request.tool_name {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "read-only receipt collision for idempotency key `{}`",
            request.idempotency_key
        )));
    }
    Ok(())
}

pub(super) fn validate_mutation_tool_fingerprint(
    request: &crate::RuntimeToolExecutionRequest,
    effect: &ToolEffectDescriptor,
    payload: &serde_json::Value,
    phase: &str,
) -> Result<(), ExecutionCommitError> {
    let expected_input = format!("sha256:{:x}", Sha256::digest(request.input.as_bytes()));
    let actual_input = payload
        .get("input_sha256")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let actual_tool = payload
        .get("tool_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let actual_descriptor = payload
        .get("descriptor_hash")
        .or_else(|| payload.pointer("/effect/descriptor_hash"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if actual_input != expected_input
        || actual_tool != request.tool_name
        || actual_tool != effect.tool_id
        || actual_descriptor != effect.descriptor_hash
    {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "mutation {phase} collision for idempotency key `{}`",
            request.idempotency_key
        )));
    }
    Ok(())
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(super) fn is_lineage_registration_conflict(
    error: &ExecutionCommitError,
    lineage_stream: Option<&str>,
) -> bool {
    matches!(
        (error, lineage_stream),
        (
            ExecutionCommitError::EventStore(RuntimeEventStoreError::StaleRevision {
                stream_id,
                ..
            }),
            Some(expected_stream),
        ) if stream_id == expected_stream
    )
}

pub(super) fn is_lineage_stream_conflict(
    error: &ExecutionCommitError,
    lineage_streams: &BTreeSet<String>,
) -> bool {
    matches!(
        error,
        ExecutionCommitError::EventStore(RuntimeEventStoreError::StaleRevision { stream_id, .. })
            if lineage_streams.contains(stream_id)
    )
}

pub(super) fn validate_replan(
    graph: &ExecutionGraph,
    nodes: &[ExecutionNodeSpec],
) -> Result<(), ExecutionCommitError> {
    let existing = graph
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    let unique = nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    if nodes.is_empty()
        || unique.len() != nodes.len()
        || nodes.iter().any(|node| existing.contains(node.id.as_str()))
    {
        return Err(ExecutionCommitError::InvalidReplan(
            "replan must add at least one uniquely identified node".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn expected_domain_revision(
    event_store: &RuntimeEventStore,
    stream_id: &str,
    transaction_id: &str,
    idempotency_key: &str,
) -> Result<u64, RuntimeEventStoreError> {
    let Some(existing_event) = event_store.event_by_idempotency_key(stream_id, idempotency_key)?
    else {
        return event_store.stream_revision(stream_id);
    };
    if existing_event.transaction_id != transaction_id {
        return event_store.stream_revision(stream_id);
    }
    let existing = event_store
        .list_stream(stream_id)
        .map_err(RuntimeEventStoreError::Corrupt)?;
    existing
        .iter()
        .filter(|event| event.transaction_id == transaction_id)
        .map(|event| event.sequence)
        .min()
        .map_or_else(
            || event_store.stream_revision(stream_id),
            |first_sequence| Ok(first_sequence.saturating_sub(1)),
        )
}

pub(super) fn node_stream_id(graph_id: &str, node_id: &str) -> String {
    format!("{graph_id}:node:{node_id}")
}

pub(crate) fn execution_lineage_stream_id(parent_execution_id: &str) -> String {
    format!("execution-lineage:{parent_execution_id}")
}

pub(super) fn node_transition_event(
    graph: &ExecutionGraph,
    node_id: &str,
    from: ExecutionNodeStatus,
    to: ExecutionNodeStatus,
    result: Option<ExecutionNodeResult>,
) -> Result<RuntimeTransactionEventInput, ExecutionCommitError> {
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: node_stream_id(&graph.id, node_id),
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution_node.transitioned".to_string(),
            status: Some(status_name(to).to_string()),
            actor: Some("execution_commit_service".to_string()),
            refs: vec![RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: graph.id.clone(),
            }],
            payload: json!({
                "graph_id": graph.id,
                "node_id": node_id,
                "from": from,
                "to": to,
                "result": result,
                "graph_revision": graph.revision,
            }),
        }
        .with_activity_binding(node_activity_binding(graph, node_id)?)?,
        idempotency_key: Some(format!("{}:{}:{}", graph.id, node_id, graph.revision)),
        schema_version: 1,
    })
}

pub(super) fn root_activity_binding(
    graph: &ExecutionGraph,
) -> Result<harness_contract::projection::RuntimeActivityBinding, ExecutionCommitError> {
    let lineage = validated_graph_lineage(graph)?;
    let parent_activity_id = graph.parent_execution.as_ref().map(|parent| {
        format!(
            "activity:execution:{}:node:{}",
            parent.execution_id, parent.node_id
        )
    });
    Ok(harness_contract::projection::RuntimeActivityBinding {
        root_execution_id: graph.id.clone(),
        session_id: lineage.session_id.clone(),
        turn_id: lineage.turn_id.clone(),
        root_task_id: lineage.root_task_id.clone(),
        task_id: lineage.task_id.clone(),
        activity_id: format!("activity:execution:{}", graph.id),
        node_id: None,
        parent_activity_id: parent_activity_id.clone(),
        initiator_activity_id: parent_activity_id,
        team_run_id: None,
        agent_instance_id: None,
        agent_run_id: None,
        skill_id: None,
        skill_revision: None,
        skill_activation_id: None,
        tool_contract_id: None,
        tool_call_id: None,
        approval_id: None,
        parallel_group_id: None,
        revision: graph.revision.max(1),
        fence: graph.revision.max(1),
        generation: lineage.generation,
    })
}

pub(super) fn node_activity_binding(
    graph: &ExecutionGraph,
    node_id: &str,
) -> Result<harness_contract::projection::RuntimeActivityBinding, ExecutionCommitError> {
    let lineage = validated_graph_lineage(graph)?;
    let root_activity_id = format!("activity:execution:{}", graph.id);
    Ok(harness_contract::projection::RuntimeActivityBinding {
        root_execution_id: graph.id.clone(),
        session_id: lineage.session_id.clone(),
        turn_id: lineage.turn_id.clone(),
        root_task_id: lineage.root_task_id.clone(),
        task_id: lineage.task_id.clone(),
        activity_id: format!("activity:execution:{}:node:{node_id}", graph.id),
        node_id: Some(node_id.to_string()),
        parent_activity_id: Some(root_activity_id.clone()),
        initiator_activity_id: Some(root_activity_id),
        team_run_id: None,
        agent_instance_id: None,
        agent_run_id: None,
        skill_id: None,
        skill_revision: None,
        skill_activation_id: None,
        tool_contract_id: None,
        tool_call_id: None,
        approval_id: None,
        parallel_group_id: None,
        revision: graph.revision.max(1),
        fence: graph.revision.max(1),
        generation: lineage.generation,
    })
}

pub(super) fn validated_graph_lineage(
    graph: &ExecutionGraph,
) -> Result<&harness_contract::execution_graph::ExecutionGraphLineage, ExecutionCommitError> {
    let lineage = graph.lineage.as_ref().ok_or_else(|| {
        ExecutionCommitError::InvalidCommand(format!(
            "execution graph `{}` is missing canonical business lineage",
            graph.id
        ))
    })?;
    lineage.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!(
            "execution graph `{}` has invalid canonical business lineage: {error}",
            graph.id
        ))
    })?;
    Ok(lineage)
}

pub(super) fn command_revision(command: &ExecutionGraphCommand) -> u64 {
    match command {
        ExecutionGraphCommand::Start { expected_revision }
        | ExecutionGraphCommand::Advance { expected_revision }
        | ExecutionGraphCommand::Pause {
            expected_revision, ..
        }
        | ExecutionGraphCommand::Resume { expected_revision }
        | ExecutionGraphCommand::Cancel {
            expected_revision, ..
        }
        | ExecutionGraphCommand::CancelNode {
            expected_revision, ..
        }
        | ExecutionGraphCommand::SubmitApproval {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ResolveExternal {
            expected_revision, ..
        }
        | ExecutionGraphCommand::Replan {
            expected_revision, ..
        } => *expected_revision,
    }
}

pub(super) fn command_metadata(command: &ExecutionGraphCommand) -> (&'static str, Option<&str>) {
    match command {
        ExecutionGraphCommand::Start { .. } => ("start", None),
        ExecutionGraphCommand::Advance { .. } => ("advance", None),
        ExecutionGraphCommand::Pause { reason, .. } => ("pause", Some(reason)),
        ExecutionGraphCommand::Resume { .. } => ("resume", None),
        ExecutionGraphCommand::Cancel { reason, .. } => ("cancel", Some(reason)),
        ExecutionGraphCommand::CancelNode { reason, .. } => ("cancel_node", Some(reason)),
        ExecutionGraphCommand::SubmitApproval { .. } => ("submit_approval", None),
        ExecutionGraphCommand::ResolveExternal { .. } => ("resolve_external", None),
        ExecutionGraphCommand::Replan { reason, .. } => ("replan", Some(reason)),
    }
}

pub(super) fn status_name(status: ExecutionNodeStatus) -> &'static str {
    match status {
        ExecutionNodeStatus::Planned => "planned",
        ExecutionNodeStatus::Ready => "ready",
        ExecutionNodeStatus::Running => "running",
        ExecutionNodeStatus::WaitingInput => "waiting_input",
        ExecutionNodeStatus::WaitingApproval => "waiting_approval",
        ExecutionNodeStatus::WaitingExternal => "waiting_external",
        ExecutionNodeStatus::Paused => "paused",
        ExecutionNodeStatus::Completed => "completed",
        ExecutionNodeStatus::Blocked => "blocked",
        ExecutionNodeStatus::Failed => "failed",
        ExecutionNodeStatus::Cancelled => "cancelled",
    }
}

pub(super) fn graph_status(graph: &ExecutionGraph) -> Option<&'static str> {
    let statuses = graph.node_statuses.values().copied().collect::<Vec<_>>();
    if statuses.is_empty() {
        return Some("planned");
    }
    if statuses.iter().all(|status| status.is_terminal()) {
        if statuses.contains(&ExecutionNodeStatus::Failed) {
            Some("failed")
        } else if statuses.contains(&ExecutionNodeStatus::Blocked) {
            Some("blocked")
        } else if statuses.contains(&ExecutionNodeStatus::Cancelled) {
            Some("cancelled")
        } else {
            Some("completed")
        }
    } else if statuses.contains(&ExecutionNodeStatus::Paused) {
        Some("paused")
    } else {
        Some("running")
    }
}
