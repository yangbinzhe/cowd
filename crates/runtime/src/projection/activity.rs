use std::collections::BTreeMap;

use harness_contract::execution_graph::{
    ExecutionEdgeKind, ExecutionGraphProjection, ExecutionNodeKind, ExecutionNodeStatus,
};
use harness_contract::projection::{
    ActivityRelationKind, ActivityVisibility, ExecutionActivityKind, ExecutionActivityProjection,
    ExecutionActivityRelation, ExecutionScopeProjection, EXECUTION_ACTIVITY_SCHEMA_VERSION,
};

use super::reducer_support::ExecutionProjectionScope;
use super::snapshot::{safe_public_ref, safe_public_text};
use crate::{DurableRuntimeEvent, RuntimeEventScope, RuntimeServices};

pub(super) fn project_execution_activities(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    graph: &ExecutionGraphProjection,
    include_audit_only: bool,
) -> (
    Vec<ExecutionActivityProjection>,
    Vec<ExecutionActivityRelation>,
) {
    let events = scope
        .session_id
        .as_deref()
        .map(|session_id| execution_events(services, session_id))
        .unwrap_or_default()
        .into_iter()
        .filter(|event| scope.contains_activity_event(event))
        .collect::<Vec<_>>();
    let root_id = execution_activity_id(&graph.graph_id);
    let root_scope = execution_scope(services, scope, graph);
    let (root_started, root_completed) = execution_bounds(&events, graph);
    let mut activities = BTreeMap::<String, ExecutionActivityProjection>::new();
    activities.insert(
        root_id.clone(),
        ExecutionActivityProjection {
            schema_version: EXECUTION_ACTIVITY_SCHEMA_VERSION,
            activity_id: root_id.clone(),
            scope: root_scope.clone(),
            kind: ExecutionActivityKind::Execution,
            visibility: vec![
                ActivityVisibility::Narrative,
                ActivityVisibility::Operational,
                ActivityVisibility::Audit,
            ],
            parent_activity_id: graph
                .parent_execution
                .as_ref()
                .map(|binding| node_activity_id(&binding.execution_id, &binding.node_id)),
            initiator_activity_id: graph
                .parent_execution
                .as_ref()
                .map(|binding| node_activity_id(&binding.execution_id, &binding.node_id)),
            causal_parent_ids: graph
                .parent_execution
                .as_ref()
                .map(|binding| vec![node_activity_id(&binding.execution_id, &binding.node_id)])
                .unwrap_or_default(),
            dependency_ids: Vec::new(),
            parallel_group_id: None,
            team_id: None,
            agent_id: None,
            tool_call_id: None,
            approval_id: None,
            status: graph_status(graph),
            started_at_ms: root_started,
            completed_at_ms: root_completed,
            duration_ms: duration(root_started, root_completed),
            sequence: 0,
            commit_cursor: graph.commit_cursor,
            public_summary: non_empty(graph.objective.as_str()),
            artifact_refs: graph.terminal_result_ref.clone().into_iter().collect(),
            evidence_refs: Vec::new(),
            detail_capability: Some(activity_detail_capability(&graph.graph_id)),
        },
    );

    let dependency_map = graph
        .edges
        .iter()
        .filter(|edge| edge.kind == ExecutionEdgeKind::DependsOn)
        .fold(BTreeMap::<String, Vec<String>>::new(), |mut map, edge| {
            map.entry(edge.to.clone())
                .or_default()
                .push(node_activity_id(&graph.graph_id, &edge.from));
            map
        });
    for (index, node) in graph.nodes.iter().enumerate() {
        let activity_id = node_activity_id(&graph.graph_id, &node.node_id);
        let node_events = events
            .iter()
            .filter(|event| event_refers_to_node(event, &node.node_id))
            .collect::<Vec<_>>();
        let started = node_events
            .iter()
            .map(|event| event.created_at_ms)
            .min()
            .or(root_started);
        let completed = node
            .status
            .is_terminal()
            .then(|| {
                node_events
                    .iter()
                    .map(|event| event.created_at_ms)
                    .max()
                    .or_else(|| started.map(|value| value.saturating_add(node.usage.duration_ms)))
            })
            .flatten();
        let evidence_refs = node
            .evidence_refs
            .iter()
            .map(|reference| reference.evidence_ref.id.clone())
            .collect::<Vec<_>>();
        let artifact_refs = node.result_ref.clone().into_iter().collect::<Vec<_>>();
        activities.insert(
            activity_id.clone(),
            ExecutionActivityProjection {
                schema_version: EXECUTION_ACTIVITY_SCHEMA_VERSION,
                activity_id: activity_id.clone(),
                scope: root_scope.clone(),
                kind: activity_kind(node.kind),
                visibility: visibility(node.kind),
                parent_activity_id: Some(root_id.clone()),
                initiator_activity_id: Some(root_id.clone()),
                causal_parent_ids: dependency_map
                    .get(&node.node_id)
                    .cloned()
                    .unwrap_or_default(),
                dependency_ids: dependency_map
                    .get(&node.node_id)
                    .cloned()
                    .unwrap_or_default(),
                // Cancellation groups express stop propagation, not observed
                // concurrency. Actual overlap is assigned from lifecycle spans
                // after all activities have been reduced.
                parallel_group_id: None,
                team_id: None,
                agent_id: None,
                tool_call_id: None,
                approval_id: None,
                status: status_name(node.status),
                started_at_ms: started,
                completed_at_ms: completed,
                duration_ms: node
                    .usage
                    .duration_ms
                    .gt(&0)
                    .then_some(node.usage.duration_ms)
                    .or_else(|| duration(started, completed)),
                sequence: index as u64 + 1,
                commit_cursor: node_events
                    .iter()
                    .map(|event| event.commit_cursor)
                    .max()
                    .unwrap_or(graph.commit_cursor),
                public_summary: node
                    .summary
                    .as_deref()
                    .and_then(non_empty)
                    .or_else(|| non_empty(node.executor_kind.as_str())),
                artifact_refs,
                evidence_refs,
                detail_capability: Some(activity_detail_capability(&graph.graph_id)),
            },
        );
    }

    for event in events.iter().filter(|event| {
        matches!(
            event.scope,
            RuntimeEventScope::ExecutionGraph
                | RuntimeEventScope::Tool
                | RuntimeEventScope::Agent
                | RuntimeEventScope::Team
                | RuntimeEventScope::Approval
                | RuntimeEventScope::Recovery
        )
    }) {
        let activity_id = event_activity_id(event, &graph.graph_id);
        let (kind, visibility) = event_kind(event);
        let team_id = event_team_run_id(event);
        let agent_run_id = event_agent_run_id(event, &graph.graph_id);
        let agent_id = event_agent_instance_id(event);
        let tool_call_id = event_tool_call_id(event);
        let approval_id = ref_id(event, "approval");
        let parent_activity_id = event_parent_activity_id(
            event,
            &graph.graph_id,
            &root_id,
            kind,
            team_id.as_deref(),
            agent_run_id.as_deref(),
        );
        let status = event_activity_status(event);
        let terminal = is_terminal_status(&status)
            || event.kind.contains("completed")
            || event.kind.contains("failed")
            || event.kind.contains("rejected");
        let started_at_ms = event_started_at(event).or(Some(event.created_at_ms));
        let completed_at_ms =
            terminal.then(|| event_completed_at(event).unwrap_or(event.created_at_ms));
        let candidate = ExecutionActivityProjection {
            schema_version: EXECUTION_ACTIVITY_SCHEMA_VERSION,
            activity_id: activity_id.clone(),
            scope: root_scope.clone(),
            kind,
            visibility,
            parent_activity_id: Some(parent_activity_id.clone()),
            initiator_activity_id: Some(parent_activity_id),
            causal_parent_ids: Vec::new(),
            dependency_ids: Vec::new(),
            parallel_group_id: value_string(&event.payload, "parallel_group_id"),
            team_id,
            agent_id,
            tool_call_id,
            approval_id,
            status,
            started_at_ms,
            completed_at_ms,
            duration_ms: event_duration(event).or_else(|| duration(started_at_ms, completed_at_ms)),
            sequence: event.sequence,
            commit_cursor: event.commit_cursor,
            public_summary: event_public_summary(event),
            artifact_refs: event_artifact_refs(event),
            evidence_refs: event_evidence_refs(event),
            detail_capability: Some(activity_detail_capability(&graph.graph_id)),
        };
        if let Some(existing) = activities.get_mut(&activity_id) {
            merge_activity(existing, candidate);
        } else {
            activities.insert(activity_id, candidate);
        }
    }

    if !include_audit_only {
        activities
            .retain(|_, activity| activity.visibility.contains(&ActivityVisibility::Narrative));
    }
    for activity in activities.values_mut() {
        activity.public_summary = activity
            .public_summary
            .as_deref()
            .map(|summary| safe_public_text(summary, 320));
        activity.artifact_refs = activity
            .artifact_refs
            .iter()
            .filter_map(|reference| safe_public_ref(reference))
            .collect();
        activity.evidence_refs = activity
            .evidence_refs
            .iter()
            .filter_map(|reference| safe_public_ref(reference))
            .collect();
    }
    materialize_artifact_activities(&mut activities, &root_scope);
    assign_observed_parallel_groups(&mut activities, &graph.graph_id);

    let mut relations = BTreeMap::<String, ExecutionActivityRelation>::new();
    for activity in activities.values() {
        if let Some(parent) = activity.parent_activity_id.as_ref() {
            insert_relation(
                &mut relations,
                relation_kind_for(activity.kind),
                parent,
                &activity.activity_id,
                None,
            );
        }
    }
    for edge in &graph.edges {
        let kind = match edge.kind {
            ExecutionEdgeKind::DependsOn => ActivityRelationKind::DependsOn,
            ExecutionEdgeKind::Verifies => ActivityRelationKind::ContributesTo,
            ExecutionEdgeKind::Produces => ActivityRelationKind::Produced,
        };
        insert_relation(
            &mut relations,
            kind,
            &node_activity_id(&graph.graph_id, &edge.from),
            &node_activity_id(&graph.graph_id, &edge.to),
            None,
        );
        if edge.kind == ExecutionEdgeKind::DependsOn {
            insert_consumed_relations(
                &mut relations,
                &activities,
                &graph.graph_id,
                &edge.from,
                &edge.to,
            );
        }
    }
    let mut activities = activities.into_values().collect::<Vec<_>>();
    activities.sort_by_key(|activity| {
        (
            activity.commit_cursor,
            activity.sequence,
            activity.activity_id.clone(),
        )
    });
    (activities, relations.into_values().collect())
}

fn insert_consumed_relations(
    relations: &mut BTreeMap<String, ExecutionActivityRelation>,
    activities: &BTreeMap<String, ExecutionActivityProjection>,
    execution_id: &str,
    source_node_id: &str,
    target_node_id: &str,
) {
    let source_id = node_activity_id(execution_id, source_node_id);
    let target_id = node_activity_id(execution_id, target_node_id);
    let Some(source) = activities.get(&source_id) else {
        return;
    };
    for reference in &source.artifact_refs {
        let artifact_id = artifact_activity_id(execution_id, reference);
        if activities.contains_key(&artifact_id) {
            insert_relation(
                relations,
                ActivityRelationKind::Consumed,
                &artifact_id,
                &target_id,
                source.evidence_refs.first().cloned(),
            );
        }
    }
}

fn execution_scope(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    graph: &ExecutionGraphProjection,
) -> ExecutionScopeProjection {
    ExecutionScopeProjection {
        workspace_id: services.workspace_key().to_string(),
        mission_id: scope.mission_id.clone(),
        task_id: scope.task_id.clone(),
        goal_id: scope.goals.first().map(|goal| goal.id.clone()),
        session_id: scope.session_id.clone(),
        turn_id: scope.turn_id.clone(),
        execution_id: graph.graph_id.clone(),
        parent_execution_id: graph
            .parent_execution
            .as_ref()
            .map(|binding| binding.execution_id.clone()),
        parent_node_id: graph
            .parent_execution
            .as_ref()
            .map(|binding| binding.node_id.clone()),
    }
}

fn execution_bounds(
    events: &[DurableRuntimeEvent],
    graph: &ExecutionGraphProjection,
) -> (Option<u64>, Option<u64>) {
    let started = events.iter().map(|event| event.created_at_ms).min();
    let completed = graph
        .nodes
        .iter()
        .all(|node| node.status.is_terminal())
        .then(|| events.iter().map(|event| event.created_at_ms).max())
        .flatten();
    (started, completed)
}

fn execution_activity_id(execution_id: &str) -> String {
    format!("activity:execution:{execution_id}")
}

fn activity_detail_capability(execution_id: &str) -> String {
    format!("/api/runtime/executions/{execution_id}/activity")
}

fn node_activity_id(execution_id: &str, node_id: &str) -> String {
    format!("activity:execution:{execution_id}:node:{node_id}")
}

fn event_activity_id(event: &DurableRuntimeEvent, execution_id: &str) -> String {
    let (kind, id) = match event.scope {
        RuntimeEventScope::Tool => ("tool", event_tool_call_id(event)),
        RuntimeEventScope::Agent => ("agent", event_agent_run_id(event, execution_id)),
        RuntimeEventScope::Team => ("team", event_team_run_id(event)),
        RuntimeEventScope::Approval => (
            "approval",
            ref_id(event, "approval").or_else(|| value_string(&event.payload, "approval_id")),
        ),
        RuntimeEventScope::Recovery => (
            "recovery",
            value_string(&event.payload, "recovery_id").or_else(|| ref_id(event, "recovery")),
        ),
        _ => ("event", None),
    };
    id.map_or_else(
        || format!("activity:event:{}", event.event_id),
        |id| format!("activity:execution:{execution_id}:{kind}:{id}"),
    )
}

fn activity_kind(kind: ExecutionNodeKind) -> ExecutionActivityKind {
    match kind {
        ExecutionNodeKind::InlineModel | ExecutionNodeKind::Synthesize => {
            ExecutionActivityKind::Model
        }
        ExecutionNodeKind::ToolBatch => ExecutionActivityKind::ToolBatch,
        ExecutionNodeKind::AgentTask => ExecutionActivityKind::Agent,
        ExecutionNodeKind::Subgraph => ExecutionActivityKind::Execution,
        ExecutionNodeKind::Verify => ExecutionActivityKind::Verify,
        ExecutionNodeKind::Approval => ExecutionActivityKind::Approval,
        ExecutionNodeKind::SessionDispatch | ExecutionNodeKind::Timer => {
            ExecutionActivityKind::Runtime
        }
    }
}

fn visibility(kind: ExecutionNodeKind) -> Vec<ActivityVisibility> {
    if matches!(
        kind,
        ExecutionNodeKind::SessionDispatch | ExecutionNodeKind::Timer
    ) {
        vec![ActivityVisibility::Operational, ActivityVisibility::Audit]
    } else {
        vec![
            ActivityVisibility::Narrative,
            ActivityVisibility::Operational,
            ActivityVisibility::Audit,
        ]
    }
}

fn event_kind(event: &DurableRuntimeEvent) -> (ExecutionActivityKind, Vec<ActivityVisibility>) {
    let kind = match event.scope {
        RuntimeEventScope::ExecutionGraph if event.kind.contains("replan") => {
            ExecutionActivityKind::Replan
        }
        RuntimeEventScope::ExecutionGraph if event.kind.contains("recover") => {
            ExecutionActivityKind::Recovery
        }
        RuntimeEventScope::Tool => ExecutionActivityKind::Tool,
        RuntimeEventScope::Agent => ExecutionActivityKind::Agent,
        RuntimeEventScope::Team => ExecutionActivityKind::Team,
        RuntimeEventScope::Approval => ExecutionActivityKind::Approval,
        RuntimeEventScope::Recovery => ExecutionActivityKind::Recovery,
        _ => ExecutionActivityKind::Runtime,
    };
    let narrative = match event.scope {
        RuntimeEventScope::ExecutionGraph => {
            event.kind.contains("replan") || event.kind.contains("recover")
        }
        RuntimeEventScope::Tool => event.kind.starts_with("tool.invocation."),
        RuntimeEventScope::Agent => {
            event.kind.starts_with("agent.")
                && !event.kind.contains("legacy")
                && !event.kind.contains("projection")
        }
        RuntimeEventScope::Team => {
            event.kind.starts_with("team.lifecycle.") || event.kind.starts_with("team.execution.")
        }
        RuntimeEventScope::Approval => true,
        RuntimeEventScope::Recovery => event
            .refs
            .iter()
            .any(|reference| reference.kind == "execution"),
        _ => false,
    };
    let visibility = if narrative {
        vec![
            ActivityVisibility::Narrative,
            ActivityVisibility::Operational,
            ActivityVisibility::Audit,
        ]
    } else {
        vec![ActivityVisibility::Operational, ActivityVisibility::Audit]
    };
    (kind, visibility)
}

fn relation_kind_for(kind: ExecutionActivityKind) -> ActivityRelationKind {
    match kind {
        ExecutionActivityKind::Team | ExecutionActivityKind::Agent => {
            ActivityRelationKind::DelegatedTo
        }
        ExecutionActivityKind::Tool | ExecutionActivityKind::ToolBatch => {
            ActivityRelationKind::Invoked
        }
        ExecutionActivityKind::Artifact | ExecutionActivityKind::Outcome => {
            ActivityRelationKind::Produced
        }
        ExecutionActivityKind::Approval => ActivityRelationKind::ApprovedBy,
        ExecutionActivityKind::Replan => ActivityRelationKind::ReplannedTo,
        ExecutionActivityKind::Recovery => ActivityRelationKind::RecoveredFrom,
        _ => ActivityRelationKind::Contains,
    }
}

fn insert_relation(
    relations: &mut BTreeMap<String, ExecutionActivityRelation>,
    kind: ActivityRelationKind,
    from: &str,
    to: &str,
    evidence_ref: Option<String>,
) {
    let relation_id = format!("relation:{kind:?}:{from}:{to}");
    relations.insert(
        relation_id.clone(),
        ExecutionActivityRelation {
            relation_id,
            kind,
            from_activity_id: from.to_string(),
            to_activity_id: to.to_string(),
            evidence_ref,
        },
    );
}

fn graph_status(graph: &ExecutionGraphProjection) -> String {
    if graph.nodes.iter().all(|node| node.status.is_terminal()) {
        if graph.nodes.iter().any(|node| {
            matches!(
                node.status,
                ExecutionNodeStatus::Failed | ExecutionNodeStatus::Blocked
            )
        }) {
            "failed".to_string()
        } else if graph
            .nodes
            .iter()
            .any(|node| node.status == ExecutionNodeStatus::Cancelled)
        {
            "cancelled".to_string()
        } else {
            "completed".to_string()
        }
    } else if graph.nodes.iter().any(|node| {
        matches!(
            node.status,
            ExecutionNodeStatus::Running
                | ExecutionNodeStatus::WaitingInput
                | ExecutionNodeStatus::WaitingApproval
                | ExecutionNodeStatus::WaitingExternal
        )
    }) {
        "running".to_string()
    } else {
        "planned".to_string()
    }
}

fn status_name(status: ExecutionNodeStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

fn event_refers_to_node(event: &DurableRuntimeEvent, node_id: &str) -> bool {
    event
        .refs
        .iter()
        .any(|reference| reference.kind == "execution_node" && reference.id == node_id)
        || event.stream_id.ends_with(&format!(":node:{node_id}"))
}

fn ref_id(event: &DurableRuntimeEvent, kind: &str) -> Option<String> {
    event
        .refs
        .iter()
        .find(|reference| reference.kind == kind)
        .map(|reference| reference.id.clone())
}

fn event_public_summary(event: &DurableRuntimeEvent) -> Option<String> {
    [
        "public_summary",
        "summary",
        "tool_name",
        "message",
        "reason",
    ]
    .iter()
    .find_map(|key| value_string(&event.payload, key))
    .or_else(|| pointer_string(&event.payload, "/returned/failure"))
    .or_else(|| pointer_string(&event.payload, "/returned/outcome"))
    .or_else(|| pointer_string(&event.payload, "/snapshot/failure"))
    .or_else(|| pointer_string(&event.payload, "/snapshot/binding/instance/role_slot_id"))
    .and_then(|value| non_empty(&value))
    .map(|value| crop(&value, 320))
    .or_else(|| Some(event.kind.clone()))
}

fn payload_refs(payload: &serde_json::Value, key: &str) -> Vec<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| {
            value.as_str().map(str::to_owned).or_else(|| {
                value
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
        })
        .collect()
}

fn value_string(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn value_u64(payload: &serde_json::Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(serde_json::Value::as_u64)
}

fn pointer_string(payload: &serde_json::Value, pointer: &str) -> Option<String> {
    payload
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn pointer_u64(payload: &serde_json::Value, pointer: &str) -> Option<u64> {
    payload.pointer(pointer).and_then(serde_json::Value::as_u64)
}

fn event_tool_call_id(event: &DurableRuntimeEvent) -> Option<String> {
    ref_id(event, "tool_call")
        .or_else(|| value_string(&event.payload, "tool_call_id"))
        .or_else(|| ref_id(event, "tool"))
}

fn event_team_run_id(event: &DurableRuntimeEvent) -> Option<String> {
    ref_id(event, "team_run")
        .or_else(|| ref_id(event, "team"))
        .or_else(|| value_string(&event.payload, "team_id"))
        .or_else(|| pointer_string(&event.payload, "/snapshot/execution_identity/team_run_id"))
        .or_else(|| pointer_string(&event.payload, "/returned/team_id"))
}

fn event_agent_instance_id(event: &DurableRuntimeEvent) -> Option<String> {
    ref_id(event, "agent_instance")
        .or_else(|| ref_id(event, "agent"))
        .or_else(|| value_string(&event.payload, "agent_id"))
        .or_else(|| pointer_string(&event.payload, "/snapshot/agent_id"))
        .or_else(|| pointer_string(&event.payload, "/returned/agent_id"))
}

fn event_agent_run_id(event: &DurableRuntimeEvent, root_execution_id: &str) -> Option<String> {
    ref_id(event, "agent_run")
        .or_else(|| ref_id(event, "run"))
        .or_else(|| value_string(&event.payload, "run_id"))
        .or_else(|| pointer_string(&event.payload, "/snapshot/run_id"))
        .or_else(|| pointer_string(&event.payload, "/returned/run_id"))
        .or_else(|| {
            (event.scope == RuntimeEventScope::Tool)
                .then(|| ref_id(event, "execution"))
                .flatten()
                .filter(|execution_id| execution_id != root_execution_id)
        })
}

fn event_activity_status(event: &DurableRuntimeEvent) -> String {
    [
        pointer_string(&event.payload, "/status"),
        pointer_string(&event.payload, "/snapshot/status"),
        pointer_string(&event.payload, "/returned/status"),
        event.status.clone(),
    ]
    .into_iter()
    .flatten()
    .map(|status| status.trim().to_ascii_lowercase())
    .find(|status| {
        matches!(
            status.as_str(),
            "planned"
                | "queued"
                | "starting"
                | "started"
                | "running"
                | "waiting"
                | "waiting_input"
                | "waiting_approval"
                | "waiting_external"
                | "complete"
                | "completed"
                | "succeeded"
                | "failed"
                | "blocked"
                | "cancelled"
                | "rejected"
                | "denied"
        )
    })
    .unwrap_or_else(|| event_status_from_kind(&event.kind))
}

fn event_started_at(event: &DurableRuntimeEvent) -> Option<u64> {
    value_u64(&event.payload, "started_at_ms")
        .or_else(|| pointer_u64(&event.payload, "/snapshot/started_at_ms"))
}

fn event_completed_at(event: &DurableRuntimeEvent) -> Option<u64> {
    value_u64(&event.payload, "ended_at_ms")
        .or_else(|| value_u64(&event.payload, "completed_at_ms"))
        .or_else(|| pointer_u64(&event.payload, "/snapshot/updated_at_ms"))
}

fn event_duration(event: &DurableRuntimeEvent) -> Option<u64> {
    value_u64(&event.payload, "duration_ms")
}

fn event_artifact_refs(event: &DurableRuntimeEvent) -> Vec<String> {
    let mut refs = payload_refs(&event.payload, "artifact_refs");
    refs.extend(
        [
            pointer_string(&event.payload, "/output_ref/ref_id"),
            pointer_string(&event.payload, "/full_output_ref"),
        ]
        .into_iter()
        .flatten(),
    );
    if let Some(changes) = event
        .payload
        .pointer("/returned/runtime_change_receipts")
        .and_then(serde_json::Value::as_array)
    {
        refs.extend(changes.iter().filter_map(|change| {
            change
                .get("path")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        }));
    }
    refs.sort();
    refs.dedup();
    refs
}

fn event_evidence_refs(event: &DurableRuntimeEvent) -> Vec<String> {
    let mut refs = event
        .refs
        .iter()
        .filter(|reference| reference.kind.contains("evidence"))
        .map(|reference| reference.id.clone())
        .chain(payload_refs(&event.payload, "evidence_refs"))
        .collect::<Vec<_>>();
    if let Some(evidence) = event
        .payload
        .pointer("/returned/evidence_refs")
        .and_then(serde_json::Value::as_array)
    {
        refs.extend(evidence.iter().filter_map(|reference| {
            reference
                .pointer("/evidence_ref/id")
                .or_else(|| reference.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        }));
    }
    refs.sort();
    refs.dedup();
    refs
}

fn event_status_from_kind(kind: &str) -> String {
    if kind.contains("failed") || kind.contains("blocked") || kind.contains("rejected") {
        "failed".to_string()
    } else if kind.contains("completed") || kind.contains("approved") || kind.contains("returned") {
        "completed".to_string()
    } else {
        "running".to_string()
    }
}

fn is_terminal_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "complete" | "succeeded" | "failed" | "blocked" | "cancelled" | "rejected"
    )
}

fn duration(started: Option<u64>, completed: Option<u64>) -> Option<u64> {
    Some(completed?.saturating_sub(started?))
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn crop(value: &str, limit: usize) -> String {
    let mut output = value.chars().take(limit).collect::<String>();
    if value.chars().count() > limit {
        output.push_str("...");
    }
    output
}

fn execution_events(services: &RuntimeServices, session_id: &str) -> Vec<DurableRuntimeEvent> {
    const PAGE_SIZE: usize = 512;
    let mut events = Vec::new();
    let mut after = None;
    loop {
        let Ok(page) = services
            .event_store()
            .execution_events_for_session(session_id, after, PAGE_SIZE)
        else {
            break;
        };
        if page.is_empty() {
            break;
        }
        let next = page
            .last()
            .map(|event| (event.commit_cursor, event.transaction_index));
        let page_len = page.len();
        events.extend(page);
        if page_len < PAGE_SIZE || next == after {
            break;
        }
        after = next;
    }
    events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
    events.dedup_by(|left, right| left.event_id == right.event_id);
    events
}

fn event_parent_activity_id(
    event: &DurableRuntimeEvent,
    execution_id: &str,
    root_id: &str,
    kind: ExecutionActivityKind,
    team_id: Option<&str>,
    agent_run_id: Option<&str>,
) -> String {
    if let Some(node_id) = ref_id(event, "execution_node") {
        return node_activity_id(execution_id, &node_id);
    }
    match kind {
        ExecutionActivityKind::Tool => agent_run_id
            .map(|id| format!("activity:execution:{execution_id}:agent:{id}"))
            .or_else(|| team_id.map(|id| format!("activity:execution:{execution_id}:team:{id}")))
            .unwrap_or_else(|| root_id.to_string()),
        ExecutionActivityKind::Agent => team_id
            .map(|id| format!("activity:execution:{execution_id}:team:{id}"))
            .unwrap_or_else(|| root_id.to_string()),
        _ => root_id.to_string(),
    }
}

fn materialize_artifact_activities(
    activities: &mut BTreeMap<String, ExecutionActivityProjection>,
    scope: &ExecutionScopeProjection,
) {
    let producers = activities
        .values()
        .flat_map(|activity| {
            activity
                .artifact_refs
                .iter()
                .cloned()
                .map(move |reference| (activity.clone(), reference))
        })
        .collect::<Vec<_>>();
    for (producer, reference) in producers {
        let kind = if producer.kind == ExecutionActivityKind::Execution {
            ExecutionActivityKind::Outcome
        } else {
            ExecutionActivityKind::Artifact
        };
        let activity_id = if kind == ExecutionActivityKind::Outcome {
            outcome_activity_id(&scope.execution_id, &reference)
        } else {
            artifact_activity_id(&scope.execution_id, &reference)
        };
        activities
            .entry(activity_id.clone())
            .or_insert_with(|| ExecutionActivityProjection {
                schema_version: EXECUTION_ACTIVITY_SCHEMA_VERSION,
                activity_id,
                scope: scope.clone(),
                kind,
                visibility: vec![
                    ActivityVisibility::Narrative,
                    ActivityVisibility::Operational,
                    ActivityVisibility::Audit,
                ],
                parent_activity_id: Some(producer.activity_id.clone()),
                initiator_activity_id: Some(producer.activity_id.clone()),
                causal_parent_ids: vec![producer.activity_id.clone()],
                dependency_ids: Vec::new(),
                parallel_group_id: None,
                team_id: producer.team_id.clone(),
                agent_id: producer.agent_id.clone(),
                tool_call_id: producer.tool_call_id.clone(),
                approval_id: None,
                status: "completed".to_string(),
                started_at_ms: producer.completed_at_ms.or(producer.started_at_ms),
                completed_at_ms: producer.completed_at_ms.or(producer.started_at_ms),
                duration_ms: Some(0),
                sequence: producer.sequence,
                commit_cursor: producer.commit_cursor,
                public_summary: Some(crop(&reference, 160)),
                artifact_refs: vec![reference],
                evidence_refs: producer.evidence_refs.clone(),
                detail_capability: producer.detail_capability.clone(),
            });
    }
}

fn artifact_activity_id(execution_id: &str, reference: &str) -> String {
    format!("activity:execution:{execution_id}:artifact:{reference}")
}

fn outcome_activity_id(execution_id: &str, reference: &str) -> String {
    format!("activity:execution:{execution_id}:outcome:{reference}")
}

fn assign_observed_parallel_groups(
    activities: &mut BTreeMap<String, ExecutionActivityProjection>,
    execution_id: &str,
) {
    let mut siblings = BTreeMap::<String, Vec<String>>::new();
    for activity in activities.values() {
        if !matches!(
            activity.kind,
            ExecutionActivityKind::Team
                | ExecutionActivityKind::Agent
                | ExecutionActivityKind::Tool
        ) || activity.started_at_ms.is_none()
        {
            continue;
        }
        if let Some(parent) = activity.parent_activity_id.as_ref() {
            siblings
                .entry(parent.clone())
                .or_default()
                .push(activity.activity_id.clone());
        }
    }
    for ids in siblings.values_mut() {
        ids.sort_by_key(|id| {
            activities
                .get(id)
                .map_or((u64::MAX, id.clone()), |activity| {
                    (activity.started_at_ms.unwrap_or(u64::MAX), id.clone())
                })
        });
        let mut component = Vec::<String>::new();
        let mut component_end = 0_u64;
        let flush =
            |component: &mut Vec<String>,
             activities: &mut BTreeMap<String, ExecutionActivityProjection>| {
                if component.len() > 1 {
                    let group_id = format!(
                        "parallel:execution:{execution_id}:{}",
                        component.first().cloned().unwrap_or_default()
                    );
                    for id in component.iter() {
                        if let Some(activity) = activities.get_mut(id) {
                            activity.parallel_group_id = Some(group_id.clone());
                        }
                    }
                }
                component.clear();
            };
        for id in ids.iter() {
            let Some(activity) = activities.get(id) else {
                continue;
            };
            let start = activity.started_at_ms.unwrap_or(u64::MAX);
            let end = activity.completed_at_ms.unwrap_or(u64::MAX);
            if !component.is_empty() && start > component_end {
                flush(&mut component, activities);
            }
            component.push(id.clone());
            component_end = component_end.max(end);
        }
        flush(&mut component, activities);
    }
}

fn merge_activity(
    existing: &mut ExecutionActivityProjection,
    mut update: ExecutionActivityProjection,
) {
    existing.started_at_ms = match (existing.started_at_ms, update.started_at_ms) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    };
    existing.completed_at_ms = match (existing.completed_at_ms, update.completed_at_ms) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
    existing.duration_ms = update
        .duration_ms
        .or_else(|| duration(existing.started_at_ms, existing.completed_at_ms))
        .or(existing.duration_ms);
    existing.sequence = existing.sequence.min(update.sequence);
    existing.commit_cursor = existing.commit_cursor.max(update.commit_cursor);
    existing.status = update.status;
    existing.parallel_group_id = update
        .parallel_group_id
        .take()
        .or_else(|| existing.parallel_group_id.clone());
    existing.team_id = update.team_id.take().or_else(|| existing.team_id.clone());
    existing.agent_id = update.agent_id.take().or_else(|| existing.agent_id.clone());
    existing.tool_call_id = update
        .tool_call_id
        .take()
        .or_else(|| existing.tool_call_id.clone());
    existing.approval_id = update
        .approval_id
        .take()
        .or_else(|| existing.approval_id.clone());
    if update.public_summary.is_some() {
        existing.public_summary = update.public_summary.take();
    }
    existing.artifact_refs.extend(update.artifact_refs);
    existing.artifact_refs.sort();
    existing.artifact_refs.dedup();
    existing.evidence_refs.extend(update.evidence_refs);
    existing.evidence_refs.sort();
    existing.evidence_refs.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(event_id: &str, kind: &str, status: &str, cursor: u64) -> DurableRuntimeEvent {
        scoped_event(
            RuntimeEventScope::Tool,
            event_id,
            kind,
            status,
            cursor,
            vec![crate::RuntimeEventRef {
                kind: "tool_call".to_string(),
                id: "call-1".to_string(),
            }],
            serde_json::json!({"tool_call_id": "call-1"}),
        )
    }

    fn scoped_event(
        scope: RuntimeEventScope,
        event_id: &str,
        kind: &str,
        status: &str,
        cursor: u64,
        refs: Vec<crate::RuntimeEventRef>,
        payload: serde_json::Value,
    ) -> DurableRuntimeEvent {
        DurableRuntimeEvent {
            event_id: event_id.to_string(),
            stream_id: "session:activity-test".to_string(),
            sequence: cursor,
            scope,
            kind: kind.to_string(),
            status: Some(status.to_string()),
            actor: Some("test".to_string()),
            refs,
            payload,
            created_at_ms: cursor * 10,
            commit_cursor: cursor,
            transaction_id: format!("tx-{cursor}"),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        }
    }

    #[test]
    fn tool_lifecycle_events_share_one_stable_activity_identity() {
        let started = event("event-started", "tool.started", "running", 1);
        let completed = event("event-completed", "tool.completed", "completed", 2);
        assert_eq!(
            event_activity_id(&started, "execution-1"),
            event_activity_id(&completed, "execution-1")
        );
    }

    #[test]
    fn lifecycle_merge_preserves_start_and_applies_terminal_state() {
        let scope = ExecutionScopeProjection {
            workspace_id: "workspace".to_string(),
            mission_id: None,
            task_id: None,
            goal_id: None,
            session_id: Some("session".to_string()),
            turn_id: Some("turn".to_string()),
            execution_id: "execution".to_string(),
            parent_execution_id: None,
            parent_node_id: None,
        };
        let mut started = ExecutionActivityProjection {
            schema_version: EXECUTION_ACTIVITY_SCHEMA_VERSION,
            activity_id: "activity".to_string(),
            scope: scope.clone(),
            kind: ExecutionActivityKind::Tool,
            visibility: vec![ActivityVisibility::Narrative],
            parent_activity_id: Some("parent".to_string()),
            initiator_activity_id: Some("parent".to_string()),
            causal_parent_ids: Vec::new(),
            dependency_ids: Vec::new(),
            parallel_group_id: Some("batch".to_string()),
            team_id: None,
            agent_id: Some("agent".to_string()),
            tool_call_id: Some("call".to_string()),
            approval_id: None,
            status: "running".to_string(),
            started_at_ms: Some(10),
            completed_at_ms: None,
            duration_ms: None,
            sequence: 1,
            commit_cursor: 1,
            public_summary: Some("search".to_string()),
            artifact_refs: Vec::new(),
            evidence_refs: vec!["evidence-start".to_string()],
            detail_capability: None,
        };
        let completed = ExecutionActivityProjection {
            completed_at_ms: Some(35),
            duration_ms: Some(25),
            status: "completed".to_string(),
            sequence: 2,
            commit_cursor: 2,
            artifact_refs: vec!["artifact".to_string()],
            evidence_refs: vec!["evidence-end".to_string()],
            ..started.clone()
        };
        merge_activity(&mut started, completed);
        assert_eq!(started.started_at_ms, Some(10));
        assert_eq!(started.completed_at_ms, Some(35));
        assert_eq!(started.duration_ms, Some(25));
        assert_eq!(started.status, "completed");
        assert_eq!(started.sequence, 1);
        assert_eq!(started.commit_cursor, 2);
        assert_eq!(started.artifact_refs, vec!["artifact"]);
        assert_eq!(
            started.evidence_refs,
            vec!["evidence-end", "evidence-start"]
        );
    }

    #[test]
    fn production_agent_refs_use_run_identity_and_team_parent() {
        let event = scoped_event(
            RuntimeEventScope::Agent,
            "agent-started",
            "agent.execution.started",
            "provider-backed child execution admitted",
            1,
            vec![
                crate::RuntimeEventRef {
                    kind: "agent_instance".to_string(),
                    id: "researcher".to_string(),
                },
                crate::RuntimeEventRef {
                    kind: "agent_run".to_string(),
                    id: "agent-run-1".to_string(),
                },
                crate::RuntimeEventRef {
                    kind: "team_run".to_string(),
                    id: "team-run-1".to_string(),
                },
            ],
            serde_json::json!({
                "snapshot": {
                    "status": "running",
                    "started_at_ms": 10,
                    "agent_id": "researcher",
                    "run_id": "agent-run-1"
                }
            }),
        );
        assert_eq!(
            event_activity_id(&event, "execution-1"),
            "activity:execution:execution-1:agent:agent-run-1"
        );
        assert_eq!(
            event_agent_instance_id(&event).as_deref(),
            Some("researcher")
        );
        assert_eq!(event_activity_status(&event), "running");
        assert_eq!(
            event_parent_activity_id(
                &event,
                "execution-1",
                "activity:execution:execution-1",
                ExecutionActivityKind::Agent,
                event_team_run_id(&event).as_deref(),
                event_agent_run_id(&event, "execution-1").as_deref(),
            ),
            "activity:execution:execution-1:team:team-run-1"
        );
    }

    #[test]
    fn child_tool_execution_ref_links_to_agent_run() {
        let event = scoped_event(
            RuntimeEventScope::Tool,
            "tool-started",
            "tool.invocation.started",
            "running",
            1,
            vec![
                crate::RuntimeEventRef {
                    kind: "tool_call".to_string(),
                    id: "call-1".to_string(),
                },
                crate::RuntimeEventRef {
                    kind: "execution".to_string(),
                    id: "agent-run-1".to_string(),
                },
            ],
            serde_json::json!({
                "tool_call_id": "call-1",
                "tool_name": "web_search",
                "status": "running",
                "started_at_ms": 10
            }),
        );
        let agent_run_id = event_agent_run_id(&event, "execution-1");
        assert_eq!(agent_run_id.as_deref(), Some("agent-run-1"));
        assert_eq!(
            event_parent_activity_id(
                &event,
                "execution-1",
                "activity:execution:execution-1",
                ExecutionActivityKind::Tool,
                None,
                agent_run_id.as_deref(),
            ),
            "activity:execution:execution-1:agent:agent-run-1"
        );
    }

    #[test]
    fn technical_tool_events_are_audit_only() {
        let event = scoped_event(
            RuntimeEventScope::Tool,
            "authorization",
            "authorization.capability_assessed",
            "allowed",
            1,
            Vec::new(),
            serde_json::json!({"summary": "low-risk read admitted"}),
        );
        let (_, visibility) = event_kind(&event);
        assert!(!visibility.contains(&ActivityVisibility::Narrative));
        assert!(visibility.contains(&ActivityVisibility::Audit));
    }

    #[test]
    fn observed_overlap_assigns_parallel_group_without_using_cancellation_groups() {
        let scope = ExecutionScopeProjection {
            workspace_id: "workspace".to_string(),
            mission_id: None,
            task_id: None,
            goal_id: None,
            session_id: Some("session".to_string()),
            turn_id: Some("turn".to_string()),
            execution_id: "execution".to_string(),
            parent_execution_id: None,
            parent_node_id: None,
        };
        let mut activities = BTreeMap::new();
        for (id, start, end) in [("left", 10, 30), ("right", 20, 40), ("later", 50, 60)] {
            activities.insert(
                id.to_string(),
                ExecutionActivityProjection {
                    schema_version: EXECUTION_ACTIVITY_SCHEMA_VERSION,
                    activity_id: id.to_string(),
                    scope: scope.clone(),
                    kind: ExecutionActivityKind::Tool,
                    visibility: vec![ActivityVisibility::Narrative],
                    parent_activity_id: Some("parent".to_string()),
                    initiator_activity_id: Some("parent".to_string()),
                    causal_parent_ids: Vec::new(),
                    dependency_ids: Vec::new(),
                    parallel_group_id: None,
                    team_id: None,
                    agent_id: None,
                    tool_call_id: Some(id.to_string()),
                    approval_id: None,
                    status: "completed".to_string(),
                    started_at_ms: Some(start),
                    completed_at_ms: Some(end),
                    duration_ms: Some(end - start),
                    sequence: start,
                    commit_cursor: start,
                    public_summary: Some(id.to_string()),
                    artifact_refs: Vec::new(),
                    evidence_refs: Vec::new(),
                    detail_capability: None,
                },
            );
        }
        assign_observed_parallel_groups(&mut activities, "execution");
        assert_eq!(
            activities["left"].parallel_group_id,
            activities["right"].parallel_group_id
        );
        assert!(activities["left"].parallel_group_id.is_some());
        assert!(activities["later"].parallel_group_id.is_none());
    }

    #[test]
    fn materialized_results_distinguish_outcomes_and_consumed_artifacts() {
        let scope = ExecutionScopeProjection {
            workspace_id: "workspace".to_string(),
            mission_id: Some("mission".to_string()),
            task_id: Some("task".to_string()),
            goal_id: Some("goal".to_string()),
            session_id: Some("session".to_string()),
            turn_id: Some("turn".to_string()),
            execution_id: "execution".to_string(),
            parent_execution_id: None,
            parent_node_id: None,
        };
        let mut activities = BTreeMap::new();
        for (activity_id, kind, artifact_refs) in [
            (
                execution_activity_id("execution"),
                ExecutionActivityKind::Execution,
                vec!["result:terminal".to_string()],
            ),
            (
                node_activity_id("execution", "source"),
                ExecutionActivityKind::Agent,
                vec!["artifact:source".to_string()],
            ),
            (
                node_activity_id("execution", "consumer"),
                ExecutionActivityKind::Agent,
                Vec::new(),
            ),
        ] {
            activities.insert(
                activity_id.clone(),
                ExecutionActivityProjection {
                    schema_version: EXECUTION_ACTIVITY_SCHEMA_VERSION,
                    activity_id,
                    scope: scope.clone(),
                    kind,
                    visibility: vec![ActivityVisibility::Narrative],
                    parent_activity_id: None,
                    initiator_activity_id: None,
                    causal_parent_ids: Vec::new(),
                    dependency_ids: Vec::new(),
                    parallel_group_id: None,
                    team_id: None,
                    agent_id: None,
                    tool_call_id: None,
                    approval_id: None,
                    status: "completed".to_string(),
                    started_at_ms: Some(10),
                    completed_at_ms: Some(20),
                    duration_ms: Some(10),
                    sequence: 1,
                    commit_cursor: 1,
                    public_summary: None,
                    artifact_refs,
                    evidence_refs: vec!["evidence:source".to_string()],
                    detail_capability: None,
                },
            );
        }
        materialize_artifact_activities(&mut activities, &scope);
        assert_eq!(
            activities[&outcome_activity_id("execution", "result:terminal")].kind,
            ExecutionActivityKind::Outcome
        );
        assert_eq!(
            activities[&artifact_activity_id("execution", "artifact:source")].kind,
            ExecutionActivityKind::Artifact
        );

        let mut relations = BTreeMap::new();
        insert_consumed_relations(
            &mut relations,
            &activities,
            "execution",
            "source",
            "consumer",
        );
        let relation = relations.values().next().expect("consumed relation");
        assert_eq!(relation.kind, ActivityRelationKind::Consumed);
        assert_eq!(
            relation.from_activity_id,
            artifact_activity_id("execution", "artifact:source")
        );
        assert_eq!(
            relation.to_activity_id,
            node_activity_id("execution", "consumer")
        );
    }
}
