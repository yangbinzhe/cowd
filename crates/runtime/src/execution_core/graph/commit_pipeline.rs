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

pub(super) fn retire_program_instances_for_semantic_replan(
    graph: &mut ExecutionGraph,
    instance_ids: &[String],
) -> Result<(), ExecutionCommitError> {
    if instance_ids.is_empty() {
        return Ok(());
    }
    let unique = instance_ids.iter().collect::<BTreeSet<_>>();
    if unique.len() != instance_ids.len() || instance_ids.iter().any(|id| id.trim().is_empty()) {
        return Err(ExecutionCommitError::InvalidReplan(
            "semantic topology replacement has invalid retired Team instances".to_string(),
        ));
    }
    let program = graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidReplan(
                "semantic topology replacement requires a collaboration Program".to_string(),
            )
        })?;
    if program.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidReplan(
            "semantic topology replacement targets a terminal Program".to_string(),
        ));
    }

    let mut retired_nodes = Vec::with_capacity(instance_ids.len());
    let mut released_context = 0u64;
    let mut released_output = 0u64;
    let mut released_parallel = 0u16;
    for instance_id in instance_ids {
        let instance = program
            .team_instances
            .iter()
            .find(|instance| &instance.instance_id == instance_id)
            .ok_or_else(|| {
                ExecutionCommitError::InvalidReplan(format!(
                    "semantic topology replacement references unknown Team instance `{instance_id}`"
                ))
            })?;
        let node_id = physical_node_for_team_instance(program, instance_id)?;
        if graph.node_statuses.get(&node_id).copied() != Some(ExecutionNodeStatus::Planned) {
            return Err(ExecutionCommitError::InvalidReplan(format!(
                "semantic topology replacement requires planned Team `{instance_id}`"
            )));
        }
        if program.control.lifecycle
            != harness_contract::execution_graph::CollaborationProgramLifecycle::Planning
        {
            let reservation = program
                .control
                .obligations
                .iter()
                .find(|obligation| obligation.instance_id == *instance_id)
                .map(|obligation| obligation.reservation.clone())
                .ok_or_else(|| {
                    ExecutionCommitError::InvalidReplan(format!(
                        "semantic topology replacement has no obligation for `{instance_id}`"
                    ))
                })?;
            if reservation == Default::default()
                && (program.control.resource_ledger.context_reservation_tokens > 0
                    || program.control.resource_ledger.output_reservation_tokens > 0
                    || program.control.resource_ledger.parallel_demand > 0)
            {
                return Err(ExecutionCommitError::InvalidReplan(
                    "semantic topology replacement requires exact durable resource reservations"
                        .to_string(),
                ));
            }
            released_context =
                released_context.saturating_add(reservation.context_reservation_tokens);
            released_output = released_output.saturating_add(reservation.output_reservation_tokens);
            released_parallel = released_parallel.saturating_add(reservation.parallel_demand);
        }
        retired_nodes.push((instance.semantic_node_id.clone(), node_id));
    }

    let retired_set = instance_ids.iter().collect::<BTreeSet<_>>();
    program
        .team_instances
        .retain(|instance| !retired_set.contains(&instance.instance_id));
    program
        .edges
        .retain(|edge| !retired_set.contains(&edge.from) && !retired_set.contains(&edge.to));
    program
        .control
        .obligations
        .retain(|obligation| !retired_set.contains(&obligation.instance_id));
    program.control.resource_ledger.context_reservation_tokens = program
        .control
        .resource_ledger
        .context_reservation_tokens
        .saturating_sub(released_context);
    program.control.resource_ledger.output_reservation_tokens = program
        .control
        .resource_ledger
        .output_reservation_tokens
        .saturating_sub(released_output);
    program.control.resource_ledger.parallel_demand = program
        .control
        .resource_ledger
        .parallel_demand
        .saturating_sub(released_parallel);
    for (semantic_id, node_id) in &retired_nodes {
        let remove_mapping = program
            .semantic_node_instances
            .get_mut(semantic_id)
            .is_some_and(|nodes| {
                nodes.retain(|candidate| candidate != node_id);
                nodes.is_empty()
            });
        if remove_mapping {
            program.semantic_node_instances.remove(semantic_id);
        }
    }
    program.required_team_count = u16::try_from(
        program
            .team_instances
            .iter()
            .filter(|instance| instance.required)
            .count(),
    )
    .unwrap_or(u16::MAX);
    for (_, node_id) in &retired_nodes {
        graph
            .node_statuses
            .insert(node_id.clone(), ExecutionNodeStatus::Cancelled);
    }
    let retired_node_set = retired_nodes
        .iter()
        .map(|(_, node_id)| node_id.as_str())
        .collect::<BTreeSet<_>>();
    graph.edges.retain(|edge| {
        !(matches!(
            edge.kind,
            harness_contract::execution_graph::ExecutionEdgeKind::CrossTeamHandoff
                | harness_contract::execution_graph::ExecutionEdgeKind::ArtifactRequires
        ) && (retired_node_set.contains(edge.from.as_str())
            || retired_node_set.contains(edge.to.as_str())))
    });
    if let Some(metadata) = graph.orchestration.as_mut() {
        metadata
            .completion
            .required_node_ids
            .retain(|node_id| !retired_node_set.contains(node_id.as_str()));
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
        | ExecutionGraphCommand::OfferWork {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ClaimWork {
            expected_revision, ..
        }
        | ExecutionGraphCommand::HeartbeatWork {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ReleaseWork {
            expected_revision, ..
        }
        | ExecutionGraphCommand::SubmitWork {
            expected_revision, ..
        }
        | ExecutionGraphCommand::AcceptWork {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ChallengeWork {
            expected_revision, ..
        }
        | ExecutionGraphCommand::SubmitApproval {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ResolveExternal {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ResolveChildExecution {
            expected_revision, ..
        }
        | ExecutionGraphCommand::UpdateCollaborationProgramControl {
            expected_revision, ..
        }
        | ExecutionGraphCommand::RecordCrossTeamEdgeDelivery {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ClaimCrossTeamEdgeDelivery {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ApplyCrossTeamEdgePatch {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing {
            expected_revision, ..
        }
        | ExecutionGraphCommand::ApplyCollaborationParallelismHint {
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
        ExecutionGraphCommand::OfferWork { .. } => ("offer_work", None),
        ExecutionGraphCommand::ClaimWork { .. } => ("claim_work", None),
        ExecutionGraphCommand::HeartbeatWork { .. } => ("heartbeat_work", None),
        ExecutionGraphCommand::ReleaseWork { reason, .. } => ("release_work", Some(reason)),
        ExecutionGraphCommand::SubmitWork { .. } => ("submit_work", None),
        ExecutionGraphCommand::AcceptWork { .. } => ("accept_work", None),
        ExecutionGraphCommand::ChallengeWork { finding, .. } => ("challenge_work", Some(finding)),
        ExecutionGraphCommand::SubmitApproval { .. } => ("submit_approval", None),
        ExecutionGraphCommand::ResolveExternal { .. } => ("resolve_external", None),
        ExecutionGraphCommand::ResolveChildExecution { .. } => ("resolve_child_execution", None),
        ExecutionGraphCommand::UpdateCollaborationProgramControl { .. } => {
            ("update_collaboration_program_control", None)
        }
        ExecutionGraphCommand::RecordCrossTeamEdgeDelivery { .. } => {
            ("record_cross_team_edge_delivery", None)
        }
        ExecutionGraphCommand::ClaimCrossTeamEdgeDelivery { .. } => {
            ("claim_cross_team_edge_delivery", None)
        }
        ExecutionGraphCommand::ApplyCrossTeamEdgePatch { .. } => {
            ("apply_cross_team_edge_patch", None)
        }
        ExecutionGraphCommand::ApplyCollaborationTeamRetirement { .. } => {
            ("apply_collaboration_team_retirement", None)
        }
        ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing { .. } => {
            ("apply_collaboration_objective_narrowing", None)
        }
        ExecutionGraphCommand::ApplyCollaborationParallelismHint { .. } => {
            ("apply_collaboration_parallelism_hint", None)
        }
        ExecutionGraphCommand::Replan { reason, .. } => ("replan", Some(reason)),
    }
}

pub(super) fn physical_node_for_team_instance(
    program: &harness_contract::execution_graph::CollaborationProgram,
    instance_id: &str,
) -> Result<String, ExecutionCommitError> {
    let instance = program
        .team_instances
        .iter()
        .find(|instance| instance.instance_id == instance_id)
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge references unknown Team instance `{instance_id}`"
            ))
        })?;
    let sibling_index = program
        .team_instances
        .iter()
        .filter(|candidate| candidate.semantic_node_id == instance.semantic_node_id)
        .position(|candidate| candidate.instance_id == instance.instance_id)
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge cannot derive physical Team mapping for `{instance_id}`"
            ))
        })?;
    program
        .semantic_node_instances
        .get(&instance.semantic_node_id)
        .and_then(|nodes| nodes.get(sibling_index))
        .cloned()
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge has no physical Team node for `{instance_id}`"
            ))
        })
}

/// Apply the only currently-safe in-place Program mutation: replacing a
/// pending cross-Team relation before either endpoint has started. The Program
/// contract and its physical `CrossTeamHandoff` edge change together, so a
/// recovery projection can never observe one without the other.
pub(super) fn apply_cross_team_edge_patch(
    graph: &mut ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), ExecutionCommitError> {
    use harness_contract::execution_graph::{
        CollaborationIntentPatchOperation, CrossTeamEdgeState, ExecutionEdgeKind,
    };

    patch
        .validate()
        .map_err(ExecutionCommitError::InvalidCommand)?;
    let CollaborationIntentPatchOperation::ChangeEdge {
        edge_id,
        from_instance_id,
        to_instance_id,
        edge_kind,
        input_contract,
    } = &patch.operation
    else {
        return Err(ExecutionCommitError::InvalidCommand(
            "cross-Team edge command requires a change_edge patch".to_string(),
        ));
    };
    let current = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "graph has no collaboration program control plane".to_string(),
            )
        })?;
    if patch.program_id != current.program_id || patch.base_revision != current.revision {
        return Err(ExecutionCommitError::InvalidCommand(
            "cross-Team edge patch program revision conflict".to_string(),
        ));
    }
    if current.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidCommand(
            "cross-Team edge patch targets a terminal Program".to_string(),
        ));
    }
    let edge_index = current
        .edges
        .iter()
        .position(|edge| edge.edge_id == *edge_id)
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge patch references unknown edge `{edge_id}`"
            ))
        })?;
    let previous = &current.edges[edge_index];
    if !matches!(
        previous.state,
        CrossTeamEdgeState::Pending | CrossTeamEdgeState::AwaitingProducer
    ) {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "cross-Team edge `{edge_id}` is already {:?} and cannot be changed",
            previous.state
        )));
    }
    let old_from_node = physical_node_for_team_instance(current, &previous.from)?;
    let old_to_node = physical_node_for_team_instance(current, &previous.to)?;
    let new_from_node = physical_node_for_team_instance(current, from_instance_id)?;
    let new_to_node = physical_node_for_team_instance(current, to_instance_id)?;
    for node_id in [&old_from_node, &old_to_node, &new_from_node, &new_to_node] {
        let status = graph.node_statuses.get(node_id).copied().ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge patch node `{node_id}` is absent"
            ))
        })?;
        if status != ExecutionNodeStatus::Planned {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "cross-Team edge patch requires planned endpoints; `{node_id}` is {status:?}"
            )));
        }
    }

    let mut candidate = current.clone();
    candidate.edges[edge_index].from = from_instance_id.clone();
    candidate.edges[edge_index].to = to_instance_id.clone();
    candidate.edges[edge_index].kind = *edge_kind;
    candidate.edges[edge_index].input_contract = input_contract.clone();
    candidate.edges[edge_index].state = CrossTeamEdgeState::Pending;
    candidate.edges[edge_index].delivery_receipt = None;
    candidate.edges[edge_index].claim_receipt = None;
    candidate.revision = candidate.revision.saturating_add(1);
    if candidate.control.lifecycle
        != harness_contract::execution_graph::CollaborationProgramLifecycle::Planning
    {
        candidate.control.resource_ledger.revision = candidate.revision;
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
    }
    candidate.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!(
            "invalid cross-Team edge patch candidate: {error}"
        ))
    })?;

    let old_pair_still_exists = candidate.edges.iter().enumerate().any(|(index, edge)| {
        index != edge_index
            && physical_node_for_team_instance(&candidate, &edge.from)
                .is_ok_and(|from| from == old_from_node)
            && physical_node_for_team_instance(&candidate, &edge.to)
                .is_ok_and(|to| to == old_to_node)
    });
    let old_pair_was_hard_artifact_dependency = graph.edges.iter().any(|edge| {
        edge.kind == ExecutionEdgeKind::ArtifactRequires
            && edge.from == old_from_node
            && edge.to == old_to_node
    });
    if !old_pair_still_exists {
        graph.edges.retain(|edge| {
            !(matches!(
                edge.kind,
                ExecutionEdgeKind::CrossTeamHandoff | ExecutionEdgeKind::ArtifactRequires
            ) && edge.from == old_from_node
                && edge.to == old_to_node)
        });
    }
    if !graph.edges.iter().any(|edge| {
        edge.kind == ExecutionEdgeKind::CrossTeamHandoff
            && edge.from == new_from_node
            && edge.to == new_to_node
    }) {
        graph.edges.push(ExecutionEdge {
            from: new_from_node.clone(),
            to: new_to_node.clone(),
            kind: ExecutionEdgeKind::CrossTeamHandoff,
        });
    }
    if old_pair_was_hard_artifact_dependency
        && !graph.edges.iter().any(|edge| {
            edge.kind == ExecutionEdgeKind::ArtifactRequires
                && edge.from == new_from_node
                && edge.to == new_to_node
        })
    {
        graph.edges.push(ExecutionEdge {
            from: new_from_node,
            to: new_to_node,
            kind: ExecutionEdgeKind::ArtifactRequires,
        });
    }
    graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .expect("current Program exists")
        .clone_from(&candidate);
    validate_execution_graph(graph)
        .map(|_| ())
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
}

/// Retire exactly one Team before it has acquired any execution or admission
/// effect.  This is intentionally narrower than a generic graph deletion:
/// the Team node remains durably visible as cancelled, while the Program's
/// semantic contract, resource obligations, physical handoffs and completion
/// gate are revised in the same graph transaction.
pub(super) fn apply_collaboration_team_retirement(
    graph: &mut ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), ExecutionCommitError> {
    use harness_contract::execution_graph::{
        CollaborationIntentPatchOperation, CollaborationProgramLifecycle, ExecutionEdgeKind,
    };

    patch
        .validate()
        .map_err(ExecutionCommitError::InvalidCommand)?;
    let CollaborationIntentPatchOperation::RetireTeam { instance_id } = &patch.operation else {
        return Err(ExecutionCommitError::InvalidCommand(
            "Team retirement command requires a retire_team patch".to_string(),
        ));
    };
    let current = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "graph has no collaboration program control plane".to_string(),
            )
        })?;
    if patch.program_id != current.program_id || patch.base_revision != current.revision {
        return Err(ExecutionCommitError::InvalidCommand(
            "Team retirement patch program revision conflict".to_string(),
        ));
    }
    if current.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidCommand(
            "Team retirement patch targets a terminal Program".to_string(),
        ));
    }
    let instance = current
        .team_instances
        .iter()
        .find(|candidate| candidate.instance_id == *instance_id)
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "Team retirement patch references unknown Team instance `{instance_id}`"
            ))
        })?;
    if instance.required
        && patch
            .user_confirmation_ref
            .as_deref()
            .is_none_or(|reference| reference.trim().is_empty())
    {
        return Err(ExecutionCommitError::InvalidCommand(
            "retiring a required Team requires an explicit user confirmation reference".to_string(),
        ));
    }
    let retired_node_id = physical_node_for_team_instance(current, instance_id)?;
    let status = graph
        .node_statuses
        .get(&retired_node_id)
        .copied()
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "Team retirement node `{retired_node_id}` is absent"
            ))
        })?;
    if status != ExecutionNodeStatus::Planned {
        return Err(ExecutionCommitError::InvalidCommand(format!(
            "Team retirement requires a planned Team; `{retired_node_id}` is {status:?}"
        )));
    }
    let reservation = current
        .control
        .obligations
        .iter()
        .find(|obligation| obligation.instance_id == *instance_id)
        .map(|obligation| obligation.reservation.clone())
        .unwrap_or_default();
    if current.control.lifecycle != CollaborationProgramLifecycle::Planning
        && reservation == Default::default()
        && (current.control.resource_ledger.context_reservation_tokens > 0
            || current.control.resource_ledger.output_reservation_tokens > 0
            || current.control.resource_ledger.parallel_demand > 0)
    {
        return Err(ExecutionCommitError::InvalidCommand(
            "Team retirement requires an exact durable resource reservation; legacy aggregate-only Programs are read-only"
                .to_string(),
        ));
    }

    let mut candidate = current.clone();
    candidate
        .team_instances
        .retain(|team| team.instance_id != *instance_id);
    candidate
        .edges
        .retain(|edge| edge.from != *instance_id && edge.to != *instance_id);
    candidate
        .control
        .obligations
        .retain(|obligation| obligation.instance_id != *instance_id);
    candidate.control.resource_ledger.context_reservation_tokens = candidate
        .control
        .resource_ledger
        .context_reservation_tokens
        .saturating_sub(reservation.context_reservation_tokens);
    candidate.control.resource_ledger.output_reservation_tokens = candidate
        .control
        .resource_ledger
        .output_reservation_tokens
        .saturating_sub(reservation.output_reservation_tokens);
    candidate.control.resource_ledger.parallel_demand = candidate
        .control
        .resource_ledger
        .parallel_demand
        .saturating_sub(reservation.parallel_demand);
    let semantic_id = instance.semantic_node_id.clone();
    let remove_mapping = candidate
        .semantic_node_instances
        .get_mut(&semantic_id)
        .is_some_and(|nodes| {
            nodes.retain(|node_id| node_id != &retired_node_id);
            nodes.is_empty()
        });
    if remove_mapping {
        candidate.semantic_node_instances.remove(&semantic_id);
    }
    candidate.required_team_count = u16::try_from(
        candidate
            .team_instances
            .iter()
            .filter(|team| team.required)
            .count(),
    )
    .unwrap_or(u16::MAX);
    candidate.revision = candidate.revision.saturating_add(1);
    if candidate.control.lifecycle != CollaborationProgramLifecycle::Planning {
        candidate.control.resource_ledger.revision = candidate.revision;
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
    }
    candidate.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!("invalid Team retirement candidate: {error}"))
    })?;

    graph
        .node_statuses
        .insert(retired_node_id.clone(), ExecutionNodeStatus::Cancelled);
    graph.edges.retain(|edge| {
        !(matches!(
            edge.kind,
            ExecutionEdgeKind::CrossTeamHandoff | ExecutionEdgeKind::ArtifactRequires
        ) && (edge.from == retired_node_id || edge.to == retired_node_id))
    });
    if let Some(orchestration) = graph.orchestration.as_mut() {
        orchestration
            .completion
            .required_node_ids
            .retain(|node_id| node_id != &retired_node_id);
        orchestration
            .collaboration_program
            .as_mut()
            .expect("current Program exists")
            .clone_from(&candidate);
    }
    validate_execution_graph(graph)
        .map(|_| ())
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
}

/// Change a semantic Team objective before any mapped physical instance has
/// started. Team identity, template, scope, acceptance, effects and graph
/// topology are intentionally untouched: only the serialized immutable Team
/// request that admission will consume is replaced atomically with the
/// Program revision. A broadening requires a durable user confirmation at
/// contract validation; no objective-only patch can acquire new authority.
pub(super) fn apply_collaboration_objective_narrowing(
    graph: &mut ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), ExecutionCommitError> {
    use harness_contract::execution_graph::{
        CollaborationIntentPatchOperation, CollaborationProgramLifecycle, ExecutionNodeKind,
    };

    patch
        .validate()
        .map_err(ExecutionCommitError::InvalidCommand)?;
    let (semantic_node_id, objective) = match &patch.operation {
        CollaborationIntentPatchOperation::NarrowObjective {
            semantic_node_id,
            objective,
        }
        | CollaborationIntentPatchOperation::ExpandObjective {
            semantic_node_id,
            objective,
        } => (semantic_node_id, objective),
        _ => {
            return Err(ExecutionCommitError::InvalidCommand(
                "objective mutation command requires an objective patch".to_string(),
            ));
        }
    };
    let current = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "graph has no collaboration program control plane".to_string(),
            )
        })?;
    if patch.program_id != current.program_id || patch.base_revision != current.revision {
        return Err(ExecutionCommitError::InvalidCommand(
            "objective narrowing patch program revision conflict".to_string(),
        ));
    }
    if current.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidCommand(
            "objective narrowing patch targets a terminal Program".to_string(),
        ));
    }
    let node_ids = current
        .semantic_node_instances
        .get(semantic_node_id)
        .filter(|nodes| !nodes.is_empty())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing patch references unknown Team semantic `{semantic_node_id}`"
            ))
        })?
        .clone();
    for node_id in &node_ids {
        let status = graph.node_statuses.get(node_id).copied().ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing node `{node_id}` is absent"
            ))
        })?;
        if status != ExecutionNodeStatus::Planned {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing requires planned Team nodes; `{node_id}` is {status:?}"
            )));
        }
    }

    let mut updated_payloads = BTreeMap::new();
    for node_id in &node_ids {
        let node = graph
            .nodes
            .iter()
            .find(|candidate| candidate.id == *node_id)
            .ok_or_else(|| {
                ExecutionCommitError::InvalidCommand(format!(
                    "objective narrowing node `{node_id}` is absent from node specs"
                ))
            })?;
        if node.kind != ExecutionNodeKind::Subgraph {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing node `{node_id}` is not a Team subgraph"
            )));
        }
        let mut request = serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
            &node.payload_ref,
        )
        .map_err(|error| {
            ExecutionCommitError::InvalidCommand(format!(
                "objective narrowing node `{node_id}` has invalid Team payload: {error}"
            ))
        })?;
        request.objective = objective.clone();
        let payload_ref =
            serde_json::to_string(&request).map_err(ExecutionCommitError::Serialization)?;
        updated_payloads.insert(node_id.clone(), payload_ref);
    }

    let mut candidate = current.clone();
    candidate.revision = candidate.revision.saturating_add(1);
    if candidate.control.lifecycle != CollaborationProgramLifecycle::Planning {
        candidate.control.resource_ledger.revision = candidate.revision;
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
    }
    candidate.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!(
            "invalid objective narrowing candidate: {error}"
        ))
    })?;
    for node in &mut graph.nodes {
        if let Some(payload_ref) = updated_payloads.remove(&node.id) {
            node.payload_ref = payload_ref;
        }
    }
    graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .expect("current Program exists")
        .clone_from(&candidate);
    validate_execution_graph(graph)
        .map(|_| ())
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
}

/// Apply a model-proposed soft parallelism hint or priority only to unstarted
/// Team work. The value becomes a durable work scheduling priority; it
/// deliberately does not change Team multiplicity, resource demands, permits,
/// or the Program resource ledger.
pub(super) fn apply_collaboration_parallelism_hint(
    graph: &mut ExecutionGraph,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), ExecutionCommitError> {
    use harness_contract::execution_graph::{
        CollaborationIntentPatchOperation, CollaborationProgramLifecycle,
    };

    patch
        .validate()
        .map_err(ExecutionCommitError::InvalidCommand)?;
    let (semantic_node_id, priority) = match &patch.operation {
        CollaborationIntentPatchOperation::SetParallelismHint {
            semantic_node_id,
            parallelism_hint,
        } => (
            semantic_node_id,
            u8::try_from(*parallelism_hint).unwrap_or(u8::MAX),
        ),
        CollaborationIntentPatchOperation::Reprioritize {
            semantic_node_id,
            priority,
        } => (semantic_node_id, *priority),
        _ => {
            return Err(ExecutionCommitError::InvalidCommand(
                "parallelism hint command requires a soft scheduling patch".to_string(),
            ));
        }
    };
    let current = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(
                "graph has no collaboration program control plane".to_string(),
            )
        })?;
    if patch.program_id != current.program_id || patch.base_revision != current.revision {
        return Err(ExecutionCommitError::InvalidCommand(
            "parallelism hint patch program revision conflict".to_string(),
        ));
    }
    if current.control.lifecycle.is_terminal() {
        return Err(ExecutionCommitError::InvalidCommand(
            "parallelism hint patch targets a terminal Program".to_string(),
        ));
    }
    let node_ids = current
        .semantic_node_instances
        .get(semantic_node_id)
        .filter(|nodes| !nodes.is_empty())
        .ok_or_else(|| {
            ExecutionCommitError::InvalidCommand(format!(
                "parallelism hint patch references unknown Team semantic `{semantic_node_id}`"
            ))
        })?
        .clone();
    for node_id in &node_ids {
        if graph.node_statuses.get(node_id) != Some(&ExecutionNodeStatus::Planned) {
            return Err(ExecutionCommitError::InvalidCommand(format!(
                "parallelism hint requires planned Team nodes; `{node_id}` is not planned"
            )));
        }
    }
    let mut candidate = current.clone();
    candidate.revision = candidate.revision.saturating_add(1);
    if candidate.control.lifecycle != CollaborationProgramLifecycle::Planning {
        candidate.control.resource_ledger.revision = candidate.revision;
        for obligation in &mut candidate.control.obligations {
            obligation.revision = candidate.revision;
        }
    }
    candidate.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!("invalid parallelism hint candidate: {error}"))
    })?;
    for node in &mut graph.nodes {
        if node_ids.contains(&node.id) {
            if node.kind != ExecutionNodeKind::Subgraph {
                return Err(ExecutionCommitError::InvalidCommand(format!(
                    "parallelism hint node `{}` is not a Team subgraph",
                    node.id
                )));
            }
            let work = node.work.get_or_insert_with(|| {
                harness_contract::execution_graph::ExecutionWorkContract::new(
                    harness_contract::execution_graph::ExecutionWorkRole::EvidenceAnalyze,
                )
            });
            work.scheduling_priority = priority;
        }
    }
    graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .expect("current Program exists")
        .clone_from(&candidate);
    validate_execution_graph(graph)
        .map(|_| ())
        .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))
}

/// Derive the immutable producer receipt as part of its terminal graph
/// transition. A terminal result that fails the typed input contract records a
/// `Blocked` edge instead of leaving an apparently-pending handoff that a
/// consumer could mistake for recoverable work.
pub(super) fn record_terminal_cross_team_edge_deliveries(
    graph: &mut ExecutionGraph,
    producer_node_id: &str,
) -> Result<(), ExecutionCommitError> {
    let producer_attempt = graph
        .recovery_cursor
        .node_attempts
        .get(producer_node_id)
        .copied()
        .unwrap_or_default();
    let result = graph.node_results.get(producer_node_id).cloned();
    let Some(program) = graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
    else {
        return Ok(());
    };
    let mut affected = Vec::new();
    for (index, edge) in program.edges.iter().enumerate() {
        if !matches!(
            edge.state,
            harness_contract::execution_graph::CrossTeamEdgeState::Pending
                | harness_contract::execution_graph::CrossTeamEdgeState::AwaitingProducer
        ) {
            continue;
        }
        if physical_node_for_team_instance(program, &edge.from)? == producer_node_id {
            affected.push(index);
        }
    }

    for index in affected {
        let edge = &mut program.edges[index];
        edge.claim_receipt = None;
        if producer_attempt > 0
            && result.as_ref().is_some_and(|result| {
                cross_team_input_contract_is_satisfied(&edge.input_contract, result)
            })
        {
            let result = result.as_ref().expect("checked above");
            edge.delivery_receipt = Some(
                harness_contract::execution_graph::CrossTeamEdgeDeliveryReceipt {
                    receipt_ref: format!(
                        "cross-team-edge:{}:{}:producer:{}:attempt:{}",
                        graph.id, edge.edge_id, producer_node_id, producer_attempt
                    ),
                    producer_node_id: producer_node_id.to_string(),
                    producer_attempt,
                    producer_result_ref: result
                        .result_ref
                        .clone()
                        .unwrap_or_else(|| format!("execution-node:{producer_node_id}")),
                    evidence_refs: result.evidence_refs.clone(),
                },
            );
            edge.state = harness_contract::execution_graph::CrossTeamEdgeState::Delivered;
        } else {
            edge.delivery_receipt = None;
            edge.state = harness_contract::execution_graph::CrossTeamEdgeState::Blocked;
        }
    }
    program.validate().map_err(|error| {
        ExecutionCommitError::InvalidCommand(format!(
            "invalid automatic cross-Team edge delivery update: {error}"
        ))
    })
}

pub(super) fn cross_team_input_contract_is_satisfied(
    contract: &harness_contract::execution_graph::CrossTeamInputContract,
    result: &ExecutionNodeResult,
) -> bool {
    use harness_contract::acceptance::{AcceptanceVerdict, TerminalFactKind};

    let has_fact = |kind| match kind {
        TerminalFactKind::CommittedEffect => {
            result
                .usage
                .observed_acceptance
                .observed_evidence
                .iter()
                .any(|evidence| evidence.workspace_prior_state.is_some())
                || result
                    .evidence_refs
                    .iter()
                    .any(|reference| reference.evidence_ref.ref_type == "runtime_change")
        }
        TerminalFactKind::ObservedEvidence => {
            !result
                .usage
                .observed_acceptance
                .observed_evidence
                .is_empty()
                || result
                    .evidence_refs
                    .iter()
                    .any(harness_contract::context::EvidenceAccessRef::is_durable)
        }
        TerminalFactKind::Artifact => result.evidence_refs.iter().any(|reference| {
            reference.evidence_ref.ref_type == "artifact" || reference.is_durable()
        }),
        TerminalFactKind::AcceptanceVerdict => result
            .usage
            .acceptance_evaluation
            .as_ref()
            .is_some_and(|evaluation| evaluation.verdict != AcceptanceVerdict::Unresolved),
    };
    contract
        .required_fact_kinds
        .iter()
        .all(|kind| has_fact(*kind))
        && contract.required_artifact_kinds.iter().all(|kind| {
            result
                .evidence_refs
                .iter()
                .any(|reference| reference.evidence_ref.ref_type == *kind)
        })
        && (!contract.require_committed_effect || has_fact(TerminalFactKind::CommittedEffect))
        && (!contract.require_satisfied_acceptance
            || result
                .usage
                .acceptance_evaluation
                .as_ref()
                .is_some_and(|evaluation| evaluation.verdict == AcceptanceVerdict::Satisfied))
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
