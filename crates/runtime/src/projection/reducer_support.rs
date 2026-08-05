use super::*;

pub(super) fn execution_health_entity(
    execution_id: &str,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    full: bool,
) -> ProjectionEntity {
    ProjectionEntity {
        id: format!("execution-health:{execution_id}"),
        kind: "execution_health".to_string(),
        revision: graph.revision,
        status: Some(graph_status(&graph.nodes)),
        summary: Some("derived from canonical execution graph state".to_string()),
        evidence_refs: Vec::new(),
        payload: None,
        detail: full.then(|| {
            serde_json::json!({
                "commit_cursor": graph.commit_cursor,
                "terminal_result_ref": graph.terminal_result_ref,
            })
        }),
    }
}

pub(super) fn activity_binding_health_entities(
    events: &[crate::DurableRuntimeEvent],
    full: bool,
) -> Vec<ProjectionEntity> {
    events
        .iter()
        .filter(|event| {
            super::activity::requires_activity_binding(event) && event.activity_binding().is_none()
        })
        .map(|event| ProjectionEntity {
            id: format!("activity-binding-health:{}", event.event_id),
            kind: "activity_binding_health".to_string(),
            revision: event.sequence,
            status: Some("error".to_string()),
            summary: Some(format!(
                "business lifecycle event `{}` has no Runtime activity binding",
                event.kind
            )),
            evidence_refs: vec![format!("runtime-event:{}", event.event_id)],
            payload: None,
            detail: full.then(|| {
                serde_json::json!({
                    "event_id": event.event_id,
                    "stream_id": event.stream_id,
                    "scope": event.scope,
                    "kind": event.kind,
                    "commit_cursor": event.commit_cursor,
                })
            }),
        })
        .collect()
}

pub(super) fn redaction_revision(context: &ProjectionQueryContext) -> String {
    let mut grants = context.visibility_grants.clone();
    grants.sort();
    grants.dedup();
    let payload = serde_json::to_vec(&(
        context.principal.as_str(),
        context.workspace_id.as_str(),
        &context.session_scopes,
        &context.mission_scopes,
        grants,
        context.detail_scope,
        context.authorization_revision,
    ))
    .unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(payload))
}

pub(super) fn validate_context(
    services: &RuntimeServices,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if context.principal.trim().is_empty()
        || context.authorization_revision == 0
        || context.workspace_id != services.workspace_key()
    {
        return Err(RuntimeServicesError::ProjectionAccessDenied);
    }
    Ok(())
}

pub(super) fn validate_projection_scope(
    scope: &ExecutionProjectionScope,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    validate_session_scope(scope.session_id.as_deref(), context)?;
    validate_mission_scope(scope.mission_id.as_deref(), context)
}

pub(super) fn has_workspace_visibility(context: &ProjectionQueryContext) -> bool {
    context
        .visibility_grants
        .iter()
        .any(|grant| grant == &format!("workspace:{}", context.workspace_id))
}

pub(super) fn validate_session_scope(
    session_id: Option<&str>,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if let Some(session_id) = session_id {
        if !has_workspace_visibility(context)
            && !context
                .session_scopes
                .iter()
                .any(|scope| scope == session_id)
        {
            return Err(RuntimeServicesError::ProjectionAccessDenied);
        }
    }
    Ok(())
}

pub(super) fn validate_mission_scope(
    mission_id: Option<&str>,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if let Some(mission_id) = mission_id {
        if !has_workspace_visibility(context)
            && !context
                .mission_scopes
                .iter()
                .any(|scope| scope == mission_id)
        {
            return Err(RuntimeServicesError::ProjectionAccessDenied);
        }
    }
    Ok(())
}

/// The read scope is derived from durable graph bindings before any domain
/// projection is assembled. This prevents a workspace-wide query from
/// accidentally becoming an execution-wide response.
pub(super) struct ExecutionProjectionScope {
    pub(super) session_id: Option<String>,
    pub(super) mission_id: Option<String>,
    pub(super) task_id: Option<String>,
    pub(super) turn_id: Option<String>,
    pub(super) execution_ids: BTreeSet<String>,
    pub(super) node_ids: BTreeSet<String>,
    pub(super) entity_ids: BTreeSet<String>,
    pub(super) goals: Vec<ProjectionEntity>,
    pub(super) agents: Vec<ProjectionEntity>,
    pub(super) teams: Vec<ProjectionEntity>,
    pub(super) relations: Vec<ProjectionEntity>,
    pub(super) approvals: Vec<ProjectionEntity>,
    pub(super) interventions: Vec<ProjectionEntity>,
    pub(super) child_executions: Vec<ChildExecutionProjection>,
}

impl ExecutionProjectionScope {
    pub(super) fn load(
        services: &RuntimeServices,
        execution_id: &str,
        graph: &harness_contract::execution_graph::ExecutionGraphProjection,
        full: bool,
    ) -> Result<Self, RuntimeServicesError> {
        let (execution_ids, child_executions, node_ids) =
            execution_lineage(services, execution_id, graph)?;

        let agent_snapshots = services.agent_runtime().list_for_graphs(&execution_ids);
        let execution_id_list = execution_ids.iter().cloned().collect::<Vec<_>>();
        let tasks = services
            .task_aggregate_service()
            .for_graphs(&execution_id_list)
            .map_err(RuntimeServicesError::Invariant)?;
        let mut matching_tasks = tasks;
        matching_tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        matching_tasks.dedup_by(|left, right| left.task_id == right.task_id);
        let matching_task_ids = matching_tasks
            .iter()
            .map(|task| task.task_id.clone())
            .collect::<BTreeSet<_>>();
        if matching_tasks.len() > 1 {
            let anchor = &matching_tasks[0];
            let shares_one_scope = matching_tasks.iter().all(|task| {
                task.mission_id == anchor.mission_id
                    && task.source_session_id == anchor.source_session_id
                    && task.source_turn_id == anchor.source_turn_id
            });
            if !shares_one_scope {
                return Err(RuntimeServicesError::Invariant(format!(
                    "execution lineage `{execution_id}` crosses task scopes: {}",
                    matching_tasks
                        .iter()
                        .map(|task| task.task_id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
        // A Team graph is intentionally shared by all role tasks in the same
        // Mission/Session/Turn. Only a graph with one owner can expose a
        // singular task_id at the root.
        let task = (matching_tasks.len() == 1)
            .then(|| matching_tasks.first())
            .flatten();
        let identity = agent_snapshots
            .iter()
            .map(|agent| &agent.execution_identity)
            .find(|identity| {
                identity
                    .graph_id()
                    .is_some_and(|id| execution_ids.contains(id))
            });
        let identity_fallback = matching_tasks.is_empty().then_some(identity).flatten();
        let task_id = task.map(|task| task.task_id.clone()).or_else(|| {
            identity_fallback.and_then(|identity| identity.task_id().map(str::to_owned))
        });
        let mission_id = matching_tasks
            .first()
            .map(|task| task.mission_id.clone())
            .or_else(|| identity.and_then(|identity| identity.mission_id().map(str::to_owned)));
        let session_id = matching_tasks
            .first()
            .map(|task| task.source_session_id.clone())
            .or_else(|| identity.and_then(|identity| identity.session_id().map(str::to_owned)))
            .or_else(|| session_id_from_graph(services, execution_id));
        let turn_id = matching_tasks
            .first()
            .map(|task| task.source_turn_id.clone())
            .or_else(|| identity.and_then(|identity| identity.turn_id().map(str::to_owned)));
        for agent in &agent_snapshots {
            let identity = &agent.execution_identity;
            let task_matches = if matching_task_ids.is_empty() {
                identity.task_id() == task_id.as_deref()
            } else {
                identity
                    .task_id()
                    .is_some_and(|id| matching_task_ids.contains(id))
            };
            if !task_matches
                || identity.mission_id() != mission_id.as_deref()
                || identity.session_id() != session_id.as_deref()
            {
                return Err(RuntimeServicesError::Invariant(format!(
                    "agent `{}` has lineage inconsistent with execution `{execution_id}`",
                    agent.agent_id
                )));
            }
        }
        let agent_ids = agent_snapshots
            .iter()
            .flat_map(|agent| [agent.agent_id.clone(), agent.run_id.clone()])
            .collect::<BTreeSet<_>>();
        let agents = entities_from_details(
            "agent",
            agent_snapshots
                .into_iter()
                .filter_map(|agent| serde_json::to_value(agent).ok()),
            full,
        );

        let team_snapshots = execution_ids
            .iter()
            .filter_map(|graph_id| services.team_runtime().project(graph_id).ok())
            .collect::<Vec<_>>();
        let team_ids = team_snapshots
            .iter()
            .map(|team| team.team_id.clone())
            .collect::<BTreeSet<_>>();
        let teams = entities_from_details(
            "team",
            team_snapshots
                .into_iter()
                .filter_map(|team| serde_json::to_value(team).ok()),
            full,
        );

        let goal_projections = goals_for_executions(services, &execution_ids);
        let goal_ids = goal_projections
            .iter()
            .map(|projection| projection.goal.id.clone())
            .collect::<BTreeSet<_>>();
        let goals = goal_projections
            .iter()
            .map(|projection| ProjectionEntity {
                id: projection.goal.id.clone(),
                kind: "goal".to_string(),
                revision: projection.stream_revision,
                status: Some(projection.goal.phase.clone()),
                summary: Some(projection.goal.objective.clone()),
                evidence_refs: projection.goal.evidence_refs.clone(),
                payload: None,
                detail: full.then(|| serde_json::to_value(projection).unwrap_or_default()),
            })
            .collect();
        let interventions = goal_projections
            .into_iter()
            .flat_map(|projection| projection.interventions.into_iter())
            .enumerate()
            .map(|(index, intervention)| {
                entity_from_value(
                    "intervention",
                    serde_json::to_value(intervention).unwrap_or_default(),
                    index as u64,
                    full,
                )
            })
            .collect();

        let relation_snapshots = session_id
            .as_deref()
            .map(|id| services.session_relations().relations_for(id))
            .unwrap_or_default();
        let relation_ids = relation_snapshots
            .iter()
            .map(|relation| relation.relation_id.clone())
            .collect::<BTreeSet<_>>();
        let relations = entities_from_details(
            "session_relation",
            relation_snapshots
                .into_iter()
                .filter_map(|relation| serde_json::to_value(relation).ok()),
            full,
        );

        let approvals = services.approval_queue().list_for_execution_scope(
            session_id.as_deref(),
            &agent_ids,
            &team_ids,
        );
        let approval_ids = approvals
            .iter()
            .map(|approval| approval.approval_id.clone())
            .collect::<BTreeSet<_>>();
        let approvals = entities_from_details(
            "approval",
            approvals
                .into_iter()
                .filter_map(|approval| serde_json::to_value(approval).ok()),
            full,
        );

        let mut entity_ids = agent_ids;
        entity_ids.extend(matching_task_ids);
        entity_ids.extend(team_ids);
        entity_ids.extend(goal_ids);
        entity_ids.extend(relation_ids);
        entity_ids.extend(approval_ids);
        Ok(Self {
            session_id,
            mission_id,
            task_id,
            turn_id,
            execution_ids,
            node_ids,
            entity_ids,
            goals,
            agents,
            teams,
            relations,
            approvals,
            interventions,
            child_executions,
        })
    }

    pub(super) fn contains_event(&self, event: &crate::DurableRuntimeEvent) -> bool {
        self.contains_activity_event(event)
            || event.refs.iter().any(|reference| {
                reference.kind == "turn" && self.turn_id.as_deref() == Some(reference.id.as_str())
            })
    }

    /// Execution projection is execution/turn-scoped. A Session reference by
    /// itself is deliberately insufficient because one Session can own many
    /// current and historical Executions, possibly across different Missions.
    pub(super) fn contains_activity_event(&self, event: &crate::DurableRuntimeEvent) -> bool {
        self.execution_ids.contains(&event.stream_id)
            || self.execution_ids.iter().any(|execution_id| {
                event
                    .stream_id
                    .starts_with(&format!("{execution_id}:node:"))
            })
            || event.refs.iter().any(|reference| {
                (reference.kind == "execution_graph" && self.execution_ids.contains(&reference.id))
                    || (reference.kind == "execution" && self.execution_ids.contains(&reference.id))
                    || (reference.kind == "execution_node" && self.node_ids.contains(&reference.id))
                    || self.entity_ids.contains(&reference.id)
            })
            || ["goal:", "approval:", "agent:", "task:"]
                .iter()
                .filter_map(|prefix| event.stream_id.strip_prefix(prefix))
                .any(|id| self.entity_ids.contains(id))
    }
}

pub(super) fn session_id_from_graph(
    services: &RuntimeServices,
    execution_id: &str,
) -> Option<String> {
    let graph = services.graph_state_store().load(execution_id).ok()?;
    graph.nodes.iter().find_map(|node| {
        serde_json::from_str::<serde_json::Value>(&node.payload_ref)
            .ok()
            .and_then(|payload| {
                payload
                    .get("session_id")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                node.payload_ref
                    .strip_prefix("session_handoff:")
                    .and_then(|payload| {
                        serde_json::from_str::<harness_contract::turn::SessionDispatchCommand>(
                            payload,
                        )
                        .ok()
                        .map(|command| command.handoff.source_session_id)
                    })
            })
    })
}

pub(super) fn entities_from_details(
    kind: &str,
    details: impl IntoIterator<Item = serde_json::Value>,
    full: bool,
) -> Vec<ProjectionEntity> {
    details
        .into_iter()
        .enumerate()
        .map(|(index, detail)| entity_from_value(kind, detail, index as u64, full))
        .collect()
}

pub(super) fn entity_from_value(
    kind: &str,
    detail: serde_json::Value,
    revision: u64,
    full: bool,
) -> ProjectionEntity {
    let id = ["id", "agent_id", "team_id", "relation_id", "approval_id"]
        .iter()
        .find_map(|key| detail.get(*key).and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{kind}:{revision}"));
    let status = detail
        .get("status")
        .or_else(|| detail.get("state"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let summary = detail
        .get("summary")
        .or_else(|| detail.get("objective"))
        .or_else(|| detail.get("title"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    ProjectionEntity {
        id,
        kind: kind.to_string(),
        revision,
        status,
        summary,
        evidence_refs: detail
            .get("evidence_refs")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
        payload: None,
        detail: full.then_some(detail),
    }
}

pub(super) fn goals_for_executions(
    services: &RuntimeServices,
    execution_ids: &BTreeSet<String>,
) -> Vec<crate::execution_core::GoalProjection> {
    services
        .event_store()
        .stream_ids_for_scope(RuntimeEventScope::Goal)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|stream| stream.strip_prefix("goal:").map(ToOwned::to_owned))
        .filter_map(|goal_id| services.goal_store().projection(&goal_id).ok().flatten())
        .filter(|projection| {
            projection
                .goal
                .id
                .strip_prefix("goal:")
                .is_some_and(|execution_id| execution_ids.contains(execution_id))
        })
        .collect()
}

/// Resolves the durable execution lineage rooted at `execution_id`. Child
/// graphs retain an immutable parent binding in their canonical graph state;
/// registration atomically writes a reverse relation event for the same fact.
/// Therefore a root projection walks only its durable descendant index and
/// never scans every graph in the runtime or infers containment from prose.
pub(super) fn execution_lineage(
    services: &RuntimeServices,
    execution_id: &str,
    root: &harness_contract::execution_graph::ExecutionGraphProjection,
) -> Result<
    (
        BTreeSet<String>,
        Vec<ChildExecutionProjection>,
        BTreeSet<String>,
    ),
    RuntimeServicesError,
> {
    let mut execution_ids = BTreeSet::from([execution_id.to_string()]);
    let mut child_executions = Vec::new();
    let mut discovered = vec![execution_id.to_string()];
    let mut lineage_graphs = Vec::new();
    while let Some(parent_execution_id) = discovered.pop() {
        for link in services
            .graph_state_store()
            .child_links(&parent_execution_id)?
        {
            if !execution_ids.insert(link.child_execution_id.clone()) {
                continue;
            }
            let graph = services
                .graph_state_store()
                .projection(&link.child_execution_id)?;
            let parent = graph.parent_execution.as_ref().ok_or_else(|| {
                RuntimeServicesError::Invariant(format!(
                    "lineage index references child graph `{}` without a parent binding",
                    graph.graph_id
                ))
            })?;
            if parent.execution_id != link.parent_execution_id
                || parent.node_id != link.parent_node_id
            {
                return Err(RuntimeServicesError::Invariant(format!(
                    "lineage index disagrees with child graph `{}` parent binding",
                    graph.graph_id
                )));
            }
            child_executions.push(ChildExecutionProjection {
                execution_id: graph.graph_id.clone(),
                parent_execution_id: parent.execution_id.clone(),
                parent_node_id: parent.node_id.clone(),
                revision: graph.revision,
                cursor: graph.commit_cursor,
                status: graph_status(&graph.nodes),
                objective: graph.objective.clone(),
            });
            discovered.push(graph.graph_id.clone());
            lineage_graphs.push(graph);
        }
    }
    child_executions.sort_by(|left, right| left.execution_id.cmp(&right.execution_id));
    let mut node_ids = root
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    for graph in lineage_graphs {
        node_ids.extend(graph.nodes.into_iter().map(|node| node.node_id));
    }
    Ok((execution_ids, child_executions, node_ids))
}

pub(super) fn graph_status(
    nodes: &[harness_contract::execution_graph::ExecutionNodeProjection],
) -> String {
    if nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::Failed)
    {
        "failed".to_string()
    } else if nodes.iter().all(|node| node.status.is_terminal()) {
        "terminal".to_string()
    } else if nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::WaitingExternal)
    {
        "waiting_external".to_string()
    } else {
        "running".to_string()
    }
}

pub(super) fn string_payload(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> ExecutionProjectionScope {
        ExecutionProjectionScope {
            session_id: Some("session-1".to_string()),
            mission_id: Some("mission-1".to_string()),
            task_id: Some("task-1".to_string()),
            turn_id: Some("turn-1".to_string()),
            execution_ids: BTreeSet::from(["execution-1".to_string()]),
            node_ids: BTreeSet::new(),
            entity_ids: BTreeSet::new(),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
            interventions: Vec::new(),
            child_executions: Vec::new(),
        }
    }

    fn event(refs: Vec<crate::RuntimeEventRef>) -> crate::DurableRuntimeEvent {
        crate::DurableRuntimeEvent {
            event_id: "event-1".to_string(),
            stream_id: "session:session-1".to_string(),
            sequence: 1,
            scope: RuntimeEventScope::Session,
            kind: "session.event".to_string(),
            status: None,
            actor: None,
            refs,
            payload: serde_json::json!({}),
            created_at_ms: 1,
            commit_cursor: 1,
            transaction_id: "tx-1".to_string(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        }
    }

    #[test]
    fn session_reference_alone_does_not_enter_an_execution_projection() {
        assert!(!scope().contains_event(&event(vec![crate::RuntimeEventRef {
            kind: "session".to_string(),
            id: "session-1".to_string(),
        }])));
    }

    #[test]
    fn exact_turn_reference_enters_the_execution_projection() {
        assert!(scope().contains_event(&event(vec![crate::RuntimeEventRef {
            kind: "turn".to_string(),
            id: "turn-1".to_string(),
        }])));
    }
}
