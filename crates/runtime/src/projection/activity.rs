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
    project_execution_activities_from_events(
        services,
        scope,
        graph,
        execution_events(services, scope),
        include_audit_only,
    )
}

pub(super) fn project_execution_activities_from_events(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    graph: &ExecutionGraphProjection,
    events: Vec<DurableRuntimeEvent>,
    include_audit_only: bool,
) -> (
    Vec<ExecutionActivityProjection>,
    Vec<ExecutionActivityRelation>,
) {
    let mut graphs = vec![graph.clone()];
    graphs.extend(
        scope
            .execution_ids
            .iter()
            .filter(|execution_id| execution_id.as_str() != graph.graph_id)
            .filter_map(|execution_id| services.graph_state_store().projection(execution_id).ok()),
    );
    graphs.sort_by_key(|candidate| {
        (
            usize::from(candidate.parent_execution.is_some()),
            candidate.graph_id.clone(),
        )
    });

    let mut activities = BTreeMap::<String, ExecutionActivityProjection>::new();
    let mut relations = BTreeMap::<String, ExecutionActivityRelation>::new();
    for lineage_graph in &graphs {
        let graph_events = events
            .iter()
            .filter(|event| event_belongs_to_graph(event, lineage_graph))
            .cloned()
            .collect::<Vec<_>>();
        let (graph_activities, graph_relations) = project_single_execution_activities_from_events(
            services,
            scope,
            lineage_graph,
            graph_events,
            include_audit_only,
        );
        for activity in graph_activities {
            let replace = activities
                .get(&activity.activity_id)
                .is_none_or(|existing| {
                    (activity.commit_cursor, activity.sequence)
                        >= (existing.commit_cursor, existing.sequence)
                });
            if replace {
                activities.insert(activity.activity_id.clone(), activity);
            }
        }
        for relation in graph_relations {
            relations.insert(relation.relation_id.clone(), relation);
        }
    }

    let team_activity_by_run = activities
        .values()
        .filter(|activity| activity.kind == ExecutionActivityKind::Team)
        .filter_map(|activity| {
            activity
                .team_run_id
                .as_ref()
                .map(|team_run_id| (team_run_id.clone(), activity.activity_id.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    for activity in activities
        .values_mut()
        .filter(|activity| activity.kind == ExecutionActivityKind::Agent)
    {
        let Some(team_activity_id) = activity
            .team_run_id
            .as_ref()
            .and_then(|team_run_id| team_activity_by_run.get(team_run_id))
        else {
            continue;
        };
        relations.retain(|_, relation| {
            relation.to_activity_id != activity.activity_id
                || relation.kind != ActivityRelationKind::DelegatedTo
        });
        activity.parent_activity_id = Some(team_activity_id.clone());
        activity.initiator_activity_id = Some(team_activity_id.clone());
        insert_relation(
            &mut relations,
            ActivityRelationKind::DelegatedTo,
            team_activity_id,
            &activity.activity_id,
            None,
        );
    }
    insert_explicit_tool_consumed_relations(&mut relations, &activities, &events);

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

fn project_single_execution_activities_from_events(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    graph: &ExecutionGraphProjection,
    events: Vec<DurableRuntimeEvent>,
    include_audit_only: bool,
) -> (
    Vec<ExecutionActivityProjection>,
    Vec<ExecutionActivityRelation>,
) {
    // Membership was already validated by `event_belongs_to_graph`, including
    // the root execution, turn and generation carried by activity bindings.
    // Applying the legacy stream/ref predicate again drops valid child Agent
    // activities because their durable stream belongs to the child execution.
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
            node_id: None,
            display_label: non_empty(graph.objective.as_str()),
            phase: Some("execution".to_string()),
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
            team_run_id: None,
            agent_instance_id: None,
            agent_run_id: None,
            skill_id: None,
            skill_revision: None,
            skill_activation_id: None,
            tool_contract_id: None,
            tool_call_id: None,
            approval_id: None,
            status: graph_status(graph),
            status_reason: graph_status_reason(graph),
            required: true,
            started_at_ms: root_started,
            completed_at_ms: root_completed,
            duration_ms: duration(root_started, root_completed),
            sequence: 0,
            commit_cursor: graph.commit_cursor,
            public_summary: non_empty(graph.objective.as_str()),
            result_summary: None,
            artifact_refs: graph.terminal_result_ref.clone().into_iter().collect(),
            evidence_refs: Vec::new(),
            definition_refs: Vec::new(),
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
        let identity = graph_node_identity(node);
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
                kind: activity_kind(node.kind, &node.executor_kind),
                node_id: Some(node.node_id.clone()),
                display_label: node_display_label(node.kind, &node.executor_kind),
                phase: Some(node_phase(node.kind).to_string()),
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
                team_run_id: identity.team_run_id,
                agent_instance_id: identity.agent_instance_id,
                agent_run_id: identity.agent_run_id,
                skill_id: None,
                skill_revision: None,
                skill_activation_id: None,
                tool_contract_id: None,
                tool_call_id: None,
                approval_id: None,
                status: status_name(node.status),
                status_reason: node
                    .failure
                    .as_ref()
                    .and_then(|failure| non_empty(&failure.message))
                    .map(|reason| crop(&reason, 320)),
                required: node.work.as_ref().is_none_or(|work| work.required),
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
                result_summary: node
                    .status
                    .is_terminal()
                    .then(|| node.summary.as_deref().and_then(non_empty))
                    .flatten(),
                artifact_refs,
                evidence_refs,
                definition_refs: identity.definition_refs,
                detail_capability: Some(activity_detail_capability(&graph.graph_id)),
            },
        );
    }

    let team_activity_by_run = activities
        .values()
        .filter(|activity| activity.kind == ExecutionActivityKind::Team)
        .filter_map(|activity| {
            activity
                .team_run_id
                .as_ref()
                .map(|team_run_id| (team_run_id.clone(), activity.activity_id.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    for activity in activities
        .values_mut()
        .filter(|activity| activity.kind == ExecutionActivityKind::Agent)
    {
        if let Some(team_activity_id) = activity
            .team_run_id
            .as_ref()
            .and_then(|team_run_id| team_activity_by_run.get(team_run_id))
        {
            activity.parent_activity_id = Some(team_activity_id.clone());
            activity.initiator_activity_id = Some(team_activity_id.clone());
        }
    }

    for event in events.iter().filter(|event| {
        matches!(
            event.scope,
            RuntimeEventScope::ExecutionGraph
                | RuntimeEventScope::Tool
                | RuntimeEventScope::Skill
                | RuntimeEventScope::Agent
                | RuntimeEventScope::Team
                | RuntimeEventScope::Approval
                | RuntimeEventScope::Recovery
                | RuntimeEventScope::Session
        ) && (event.scope != RuntimeEventScope::Session || is_public_reasoning_event(event))
    }) {
        let binding = event.activity_binding();
        if binding.is_none() && requires_activity_binding(event) {
            continue;
        }
        let activity_id = binding.as_ref().map_or_else(
            || format!("activity:event:{}", event.event_id),
            |binding| binding.activity_id.clone(),
        );
        let (kind, mut visibility) = event_kind(event);
        if binding.is_none() {
            visibility.retain(|item| *item != ActivityVisibility::Narrative);
        }
        let team_run_id = binding
            .as_ref()
            .and_then(|binding| binding.team_run_id.clone());
        let agent_run_id = binding
            .as_ref()
            .and_then(|binding| binding.agent_run_id.clone());
        let agent_instance_id = binding
            .as_ref()
            .and_then(|binding| binding.agent_instance_id.clone());
        let tool_call_id = binding
            .as_ref()
            .and_then(|binding| binding.tool_call_id.clone());
        let approval_id = binding
            .as_ref()
            .and_then(|binding| binding.approval_id.clone());
        let bound_parent_activity_id = binding
            .as_ref()
            .and_then(|binding| binding.parent_activity_id.clone())
            .unwrap_or_else(|| root_id.clone());
        let parent_activity_id = if kind == ExecutionActivityKind::Agent {
            team_run_id
                .as_ref()
                .and_then(|team_run_id| team_activity_by_run.get(team_run_id))
                .cloned()
                .unwrap_or(bound_parent_activity_id)
        } else {
            bound_parent_activity_id
        };
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
            node_id: binding.as_ref().and_then(|binding| binding.node_id.clone()),
            display_label: event_display_label(event, kind),
            phase: event_phase(event),
            visibility,
            parent_activity_id: Some(parent_activity_id.clone()),
            initiator_activity_id: binding
                .as_ref()
                .and_then(|binding| binding.initiator_activity_id.clone())
                .or_else(|| Some(parent_activity_id)),
            causal_parent_ids: Vec::new(),
            dependency_ids: Vec::new(),
            parallel_group_id: binding
                .as_ref()
                .and_then(|binding| binding.parallel_group_id.clone()),
            team_run_id,
            agent_instance_id,
            agent_run_id,
            skill_id: binding
                .as_ref()
                .and_then(|binding| binding.skill_id.clone()),
            skill_revision: binding
                .as_ref()
                .and_then(|binding| binding.skill_revision.clone()),
            skill_activation_id: binding
                .as_ref()
                .and_then(|binding| binding.skill_activation_id.clone()),
            tool_contract_id: binding
                .as_ref()
                .and_then(|binding| binding.tool_contract_id.clone()),
            tool_call_id,
            approval_id,
            status,
            status_reason: event_status_reason(event),
            required: value_bool(&event.payload, "required").unwrap_or(true),
            started_at_ms,
            completed_at_ms,
            duration_ms: event_duration(event).or_else(|| duration(started_at_ms, completed_at_ms)),
            sequence: event.sequence,
            commit_cursor: event.commit_cursor,
            public_summary: event_public_summary(event),
            result_summary: terminal.then(|| event_result_summary(event)).flatten(),
            artifact_refs: event_artifact_refs(event),
            evidence_refs: event_evidence_refs(event),
            definition_refs: Vec::new(),
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
        activity.display_label = activity
            .display_label
            .as_deref()
            .map(|label| safe_public_text(label, 120));
        activity.phase = activity
            .phase
            .as_deref()
            .map(|phase| safe_public_text(phase, 80));
        activity.result_summary = activity
            .result_summary
            .as_deref()
            .map(|summary| safe_public_text(summary, 320));
        activity.status_reason = activity
            .status_reason
            .as_deref()
            .map(|reason| safe_public_text(reason, 320));
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
            insert_committed_predecessor_consumed_relations(
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

fn insert_committed_predecessor_consumed_relations(
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
    let Some(target) = activities.get(&target_id) else {
        return;
    };
    if !matches!(
        (source.kind, target.kind),
        (
            ExecutionActivityKind::Agent | ExecutionActivityKind::Team,
            ExecutionActivityKind::Agent | ExecutionActivityKind::Team
        )
    ) {
        return;
    }
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

fn insert_explicit_tool_consumed_relations(
    relations: &mut BTreeMap<String, ExecutionActivityRelation>,
    activities: &BTreeMap<String, ExecutionActivityProjection>,
    events: &[DurableRuntimeEvent],
) {
    for event in events
        .iter()
        .filter(|event| event.kind.starts_with("tool.invocation."))
    {
        let Some(binding) = event.activity_binding() else {
            continue;
        };
        let Some(input_refs) = event
            .payload
            .get("input_refs")
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };
        for reference in input_refs
            .iter()
            .filter_map(serde_json::Value::as_str)
            .filter(|reference| !reference.trim().is_empty())
        {
            let producer = activities.values().find(|activity| {
                activity.activity_id != binding.activity_id
                    && activity
                        .artifact_refs
                        .iter()
                        .any(|candidate| candidate == reference)
            });
            let Some(producer) = producer else {
                continue;
            };
            let artifact_id = artifact_activity_id(&producer.scope.execution_id, reference);
            if !activities.contains_key(&artifact_id)
                || !activities.contains_key(&binding.activity_id)
            {
                continue;
            }
            insert_relation(
                relations,
                ActivityRelationKind::Consumed,
                &artifact_id,
                &binding.activity_id,
                Some(reference.to_string()),
            );
        }
    }
}

fn event_belongs_to_graph(event: &DurableRuntimeEvent, graph: &ExecutionGraphProjection) -> bool {
    if let Some(binding) = event.activity_binding() {
        if binding.root_execution_id != graph.graph_id || binding.validate().is_err() {
            return false;
        }
        return graph.lineage.as_ref().is_none_or(|lineage| {
            binding.session_id == lineage.session_id
                && binding.turn_id == lineage.turn_id
                && binding.root_task_id == lineage.root_task_id
                && binding.generation == lineage.generation
        });
    }
    event.stream_id == graph.graph_id
        || event
            .stream_id
            .starts_with(&format!("{}:node:", graph.graph_id))
        || event.refs.iter().any(|reference| {
            matches!(reference.kind.as_str(), "execution" | "execution_graph")
                && reference.id == graph.graph_id
        })
        || event.refs.iter().any(|reference| {
            reference.kind == "execution_node"
                && graph.nodes.iter().any(|node| node.node_id == reference.id)
        })
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
        root_task_id: graph
            .lineage
            .as_ref()
            .map(|lineage| lineage.root_task_id.clone()),
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

fn activity_kind(kind: ExecutionNodeKind, executor_kind: &str) -> ExecutionActivityKind {
    match kind {
        ExecutionNodeKind::InlineModel | ExecutionNodeKind::Synthesize => {
            ExecutionActivityKind::Model
        }
        ExecutionNodeKind::ToolBatch => ExecutionActivityKind::ToolBatch,
        ExecutionNodeKind::AgentTask => ExecutionActivityKind::Agent,
        ExecutionNodeKind::Subgraph
            if executor_kind == crate::orchestration::compiler::TEAM_SUBGRAPH_EXECUTOR =>
        {
            ExecutionActivityKind::Team
        }
        ExecutionNodeKind::Subgraph => ExecutionActivityKind::Execution,
        ExecutionNodeKind::Verify => ExecutionActivityKind::Verify,
        ExecutionNodeKind::Approval => ExecutionActivityKind::Approval,
        ExecutionNodeKind::SessionDispatch | ExecutionNodeKind::Timer => {
            ExecutionActivityKind::Runtime
        }
    }
}

#[derive(Default)]
struct GraphNodeIdentity {
    team_run_id: Option<String>,
    agent_instance_id: Option<String>,
    agent_run_id: Option<String>,
    definition_refs: Vec<String>,
}

fn graph_node_identity(
    node: &harness_contract::execution_graph::ExecutionNodeProjection,
) -> GraphNodeIdentity {
    if node.kind == ExecutionNodeKind::AgentTask {
        if let Ok(intent) =
            serde_json::from_str::<harness_contract::agent::AgentTaskIntent>(&node.payload_ref)
        {
            let mut definition_refs = intent
                .selected_agent_id
                .clone()
                .into_iter()
                .map(|id| format!("agent-definition:{id}"))
                .collect::<Vec<_>>();
            if let Some(reference) = intent.definition_ref {
                if let Ok(value) = serde_json::to_string(&reference) {
                    definition_refs.push(format!("agent-definition-ref:{value}"));
                }
            }
            return GraphNodeIdentity {
                team_run_id: intent.team_id,
                agent_run_id: non_empty(&intent.run_id),
                definition_refs,
                ..GraphNodeIdentity::default()
            };
        }
    }
    if node.kind == ExecutionNodeKind::Subgraph
        && node.executor_kind == crate::orchestration::compiler::TEAM_SUBGRAPH_EXECUTOR
    {
        if let Ok(request) = serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
            &node.payload_ref,
        ) {
            let definition_refs = serde_json::to_string(&request.template_selector)
                .ok()
                .map(|value| vec![format!("team-template:{value}")])
                .unwrap_or_default();
            return GraphNodeIdentity {
                team_run_id: non_empty(&request.team_id),
                definition_refs,
                ..GraphNodeIdentity::default()
            };
        }
    }
    GraphNodeIdentity::default()
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
        RuntimeEventScope::Skill => ExecutionActivityKind::Skill,
        RuntimeEventScope::Agent if is_agent_activity_event(&event.kind) => {
            ExecutionActivityKind::Agent
        }
        RuntimeEventScope::Agent => ExecutionActivityKind::Runtime,
        RuntimeEventScope::Team => ExecutionActivityKind::Team,
        RuntimeEventScope::Approval => ExecutionActivityKind::Approval,
        RuntimeEventScope::Recovery => ExecutionActivityKind::Recovery,
        RuntimeEventScope::Session if is_public_reasoning_event(event) => {
            ExecutionActivityKind::Reasoning
        }
        _ => ExecutionActivityKind::Runtime,
    };
    let narrative = match event.scope {
        RuntimeEventScope::ExecutionGraph => {
            event.kind.contains("replan") || event.kind.contains("recover")
        }
        RuntimeEventScope::Tool => event.kind.starts_with("tool.invocation."),
        RuntimeEventScope::Skill => event.kind == "skill.activation.selected",
        RuntimeEventScope::Agent => is_agent_activity_event(&event.kind),
        RuntimeEventScope::Team => {
            event.kind.starts_with("team.lifecycle.") || event.kind.starts_with("team.execution.")
        }
        RuntimeEventScope::Approval => true,
        RuntimeEventScope::Recovery => event
            .refs
            .iter()
            .any(|reference| reference.kind == "execution"),
        RuntimeEventScope::Session => is_public_reasoning_event(event),
        _ => false,
    };
    let visibility = if kind == ExecutionActivityKind::Reasoning {
        vec![ActivityVisibility::Narrative, ActivityVisibility::Audit]
    } else if narrative {
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

pub(super) fn requires_activity_binding(event: &DurableRuntimeEvent) -> bool {
    match event.scope {
        RuntimeEventScope::Tool => event.kind.starts_with("tool.invocation."),
        RuntimeEventScope::Skill => event.kind == "skill.activation.selected",
        RuntimeEventScope::Agent => is_agent_activity_event(&event.kind),
        RuntimeEventScope::Team => {
            event.kind.starts_with("team.lifecycle.") || event.kind.starts_with("team.execution.")
        }
        RuntimeEventScope::Session => is_public_reasoning_event(event),
        _ => false,
    }
}

fn is_public_reasoning_event(event: &DurableRuntimeEvent) -> bool {
    event.kind == "model.item_completed"
        && event.payload.get("kind").is_some_and(|kind| {
            kind.as_str().is_some_and(|kind| {
                matches!(
                    kind,
                    "public_reasoning" | "reasoning-summary" | "reasoning_summary"
                )
            })
        })
}

fn is_agent_activity_event(kind: &str) -> bool {
    matches!(
        kind,
        "agent.prepared"
            | "agent.running"
            | "agent.terminal"
            | "agent.cancelled"
            | "agent.blocked"
            | "agent.blocked_recovery"
            | "agent.command"
            | "agent.command_rejected"
            | "agent.recovered"
            | "agent.execution.started"
            | "agent.provider.first_output"
            | "agent.acceptance.evaluated"
    )
}

fn relation_kind_for(kind: ExecutionActivityKind) -> ActivityRelationKind {
    match kind {
        ExecutionActivityKind::Team | ExecutionActivityKind::Agent => {
            ActivityRelationKind::DelegatedTo
        }
        ExecutionActivityKind::Skill
        | ExecutionActivityKind::Reasoning
        | ExecutionActivityKind::Tool
        | ExecutionActivityKind::ToolBatch => ActivityRelationKind::Invoked,
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
    let evidence_identity = evidence_ref.as_deref().unwrap_or("-");
    let relation_id = format!("relation:{kind:?}:{from}:{to}:{evidence_identity}");
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
            node.work.as_ref().is_none_or(|work| work.required)
                && matches!(
                    node.status,
                    ExecutionNodeStatus::Failed | ExecutionNodeStatus::Blocked
                )
        }) {
            "failed".to_string()
        } else if graph.nodes.iter().any(|node| {
            node.work.as_ref().is_none_or(|work| work.required)
                && node.status == ExecutionNodeStatus::Cancelled
        }) {
            "cancelled".to_string()
        } else if graph.nodes.iter().any(|node| {
            node.work.as_ref().is_some_and(|work| !work.required)
                && matches!(
                    node.status,
                    ExecutionNodeStatus::Failed
                        | ExecutionNodeStatus::Blocked
                        | ExecutionNodeStatus::Cancelled
                )
        }) {
            "completed_with_warnings".to_string()
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

fn graph_status_reason(graph: &ExecutionGraphProjection) -> Option<String> {
    let required_failures = graph
        .nodes
        .iter()
        .filter(|node| {
            node.work.as_ref().is_none_or(|work| work.required)
                && matches!(
                    node.status,
                    ExecutionNodeStatus::Failed | ExecutionNodeStatus::Blocked
                )
        })
        .count();
    if required_failures > 0 {
        return Some(format!(
            "{required_failures} required activities failed or blocked"
        ));
    }
    let optional_warnings = graph
        .nodes
        .iter()
        .filter(|node| {
            node.work.as_ref().is_some_and(|work| !work.required)
                && matches!(
                    node.status,
                    ExecutionNodeStatus::Failed
                        | ExecutionNodeStatus::Blocked
                        | ExecutionNodeStatus::Cancelled
                )
        })
        .count();
    (optional_warnings > 0)
        .then(|| format!("{optional_warnings} optional activities did not complete"))
}

fn node_phase(kind: ExecutionNodeKind) -> &'static str {
    match kind {
        ExecutionNodeKind::InlineModel => "model",
        ExecutionNodeKind::ToolBatch => "tools",
        ExecutionNodeKind::AgentTask => "agent",
        ExecutionNodeKind::Subgraph => "delegation",
        ExecutionNodeKind::Verify => "verification",
        ExecutionNodeKind::Synthesize => "synthesis",
        ExecutionNodeKind::Approval => "approval",
        ExecutionNodeKind::SessionDispatch => "dispatch",
        ExecutionNodeKind::Timer => "timer",
    }
}

fn node_display_label(kind: ExecutionNodeKind, executor_kind: &str) -> Option<String> {
    let executor = executor_kind.trim();
    if !executor.is_empty()
        && !matches!(
            executor,
            "inline_model"
                | "tool_batch"
                | "agent_task"
                | "subgraph"
                | "verify"
                | "synthesize"
                | "approval"
                | "session_dispatch"
                | "timer"
        )
    {
        return Some(crop(executor, 120));
    }
    Some(node_phase(kind).to_string())
}

fn event_display_label(event: &DurableRuntimeEvent, kind: ExecutionActivityKind) -> Option<String> {
    [
        "display_label",
        "label",
        "name",
        "tool_name",
        "team_name",
        "agent_name",
        "role",
        "action",
    ]
    .iter()
    .find_map(|key| value_string(&event.payload, key))
    .or_else(|| pointer_string(&event.payload, "/snapshot/display_name"))
    .or_else(|| pointer_string(&event.payload, "/snapshot/binding/instance/role_slot_id"))
    .or_else(|| match kind {
        ExecutionActivityKind::Team => event_team_run_id(event),
        ExecutionActivityKind::Agent => event_agent_instance_id(event),
        ExecutionActivityKind::Skill => ref_id(event, "skill"),
        ExecutionActivityKind::Tool => event_tool_call_id(event),
        ExecutionActivityKind::Approval => ref_id(event, "approval"),
        ExecutionActivityKind::Reasoning => Some("思考".to_string()),
        _ => None,
    })
    .and_then(|value| non_empty(&value))
    .map(|value| crop(&value, 120))
}

fn event_phase(event: &DurableRuntimeEvent) -> Option<String> {
    value_string(&event.payload, "phase")
        .or_else(|| pointer_string(&event.payload, "/snapshot/phase"))
        .or_else(|| {
            event
                .kind
                .rsplit('.')
                .find(|segment| !segment.is_empty() && *segment != "v1")
                .map(str::to_owned)
        })
        .and_then(|value| non_empty(&value))
        .map(|value| crop(&value, 80))
}

fn event_status_reason(event: &DurableRuntimeEvent) -> Option<String> {
    [
        pointer_string(&event.payload, "/failure/message"),
        pointer_string(&event.payload, "/returned/failure"),
        pointer_string(&event.payload, "/snapshot/failure"),
        value_string(&event.payload, "error"),
        value_string(&event.payload, "reason"),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| non_empty(&value))
    .map(|value| crop(&value, 320))
}

fn event_result_summary(event: &DurableRuntimeEvent) -> Option<String> {
    let reasoning_content = is_public_reasoning_event(event)
        .then(|| value_string(&event.payload, "content"))
        .flatten();
    [
        reasoning_content,
        value_string(&event.payload, "result_summary"),
        value_string(&event.payload, "outcome"),
        pointer_string(&event.payload, "/returned/outcome"),
        pointer_string(&event.payload, "/snapshot/outcome"),
        value_string(&event.payload, "summary"),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| non_empty(&value))
    .map(|value| crop(&value, 320))
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
    let reasoning_content = is_public_reasoning_event(event)
        .then(|| value_string(&event.payload, "content"))
        .flatten();
    reasoning_content.or_else(|| {
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
    })
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

fn value_bool(payload: &serde_json::Value, key: &str) -> Option<bool> {
    payload.get(key).and_then(serde_json::Value::as_bool)
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

fn event_activity_status(event: &DurableRuntimeEvent) -> String {
    let status = [
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
    .unwrap_or_else(|| event_status_from_kind(&event.kind));
    if matches!(status.as_str(), "running" | "started" | "starting")
        && event_kind_is_point_fact(&event.kind)
    {
        "completed".to_string()
    } else {
        status
    }
}

fn event_kind_is_point_fact(kind: &str) -> bool {
    [
        "capability_assessed",
        "lease_transition",
        "recorded",
        "replanned",
        "intervention",
    ]
    .iter()
    .any(|marker| kind.contains(marker))
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

pub(super) fn execution_events(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
) -> Vec<DurableRuntimeEvent> {
    let mut events = Vec::new();
    for execution_id in &scope.execution_ids {
        events.extend(events_for_root_execution(services, execution_id));
    }
    events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
    events.dedup_by(|left, right| left.event_id == right.event_id);
    events
}

pub(super) fn events_for_root_execution(
    services: &RuntimeServices,
    execution_id: &str,
) -> Vec<DurableRuntimeEvent> {
    const PAGE_SIZE: usize = 512;
    let mut events = Vec::new();
    let mut after = None;
    loop {
        let Ok(page) =
            services
                .event_store()
                .events_for_root_execution(execution_id, after, PAGE_SIZE)
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
    events
}

pub(super) fn events_for_root_execution_kind(
    services: &RuntimeServices,
    execution_id: &str,
    kind: &str,
) -> Vec<DurableRuntimeEvent> {
    const PAGE_SIZE: usize = 128;
    let mut events = Vec::new();
    let mut after = None;
    loop {
        let Ok(page) = services.event_store().events_for_root_execution_kind(
            execution_id,
            kind,
            after,
            PAGE_SIZE,
        ) else {
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
    events
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
                node_id: producer.node_id.clone(),
                display_label: Some(if kind == ExecutionActivityKind::Outcome {
                    "outcome".to_string()
                } else {
                    "artifact".to_string()
                }),
                phase: Some("completed".to_string()),
                visibility: artifact_visibility(&reference),
                parent_activity_id: Some(producer.activity_id.clone()),
                initiator_activity_id: Some(producer.activity_id.clone()),
                causal_parent_ids: vec![producer.activity_id.clone()],
                dependency_ids: Vec::new(),
                parallel_group_id: None,
                team_run_id: producer.team_run_id.clone(),
                agent_instance_id: producer.agent_instance_id.clone(),
                agent_run_id: producer.agent_run_id.clone(),
                skill_id: producer.skill_id.clone(),
                skill_revision: producer.skill_revision.clone(),
                skill_activation_id: producer.skill_activation_id.clone(),
                tool_contract_id: producer.tool_contract_id.clone(),
                tool_call_id: producer.tool_call_id.clone(),
                approval_id: None,
                status: "completed".to_string(),
                status_reason: None,
                required: producer.required,
                started_at_ms: producer.completed_at_ms.or(producer.started_at_ms),
                completed_at_ms: producer.completed_at_ms.or(producer.started_at_ms),
                duration_ms: Some(0),
                sequence: producer.sequence,
                commit_cursor: producer.commit_cursor,
                public_summary: Some(crop(&reference, 160)),
                result_summary: producer.result_summary.clone(),
                artifact_refs: vec![reference],
                evidence_refs: producer.evidence_refs.clone(),
                definition_refs: producer.definition_refs.clone(),
                detail_capability: producer.detail_capability.clone(),
            });
    }
}

fn artifact_visibility(reference: &str) -> Vec<ActivityVisibility> {
    let normalized = reference.to_ascii_lowercase();
    let internal = [
        "session-ingress-graph:",
        "session-ingress-confirmed:",
        ":tool-results:",
        ":model-result",
        "turn-result:",
        "compile-target-guard:",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    if internal {
        vec![ActivityVisibility::Operational, ActivityVisibility::Audit]
    } else {
        vec![
            ActivityVisibility::Narrative,
            ActivityVisibility::Operational,
            ActivityVisibility::Audit,
        ]
    }
}

fn artifact_activity_id(execution_id: &str, reference: &str) -> String {
    format!("activity:execution:{execution_id}:artifact:{reference}")
}

fn outcome_activity_id(execution_id: &str, reference: &str) -> String {
    format!("activity:execution:{execution_id}:outcome:{reference}")
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
    existing.required = existing.required && update.required;
    existing.display_label = update
        .display_label
        .take()
        .or_else(|| existing.display_label.clone());
    existing.phase = update.phase.take().or_else(|| existing.phase.clone());
    existing.status_reason = update
        .status_reason
        .take()
        .or_else(|| existing.status_reason.clone());
    existing.parallel_group_id = update
        .parallel_group_id
        .take()
        .or_else(|| existing.parallel_group_id.clone());
    existing.node_id = update.node_id.take().or_else(|| existing.node_id.clone());
    existing.team_run_id = update
        .team_run_id
        .take()
        .or_else(|| existing.team_run_id.clone());
    existing.agent_instance_id = update
        .agent_instance_id
        .take()
        .or_else(|| existing.agent_instance_id.clone());
    existing.agent_run_id = update
        .agent_run_id
        .take()
        .or_else(|| existing.agent_run_id.clone());
    existing.skill_id = update.skill_id.take().or_else(|| existing.skill_id.clone());
    existing.skill_revision = update
        .skill_revision
        .take()
        .or_else(|| existing.skill_revision.clone());
    existing.skill_activation_id = update
        .skill_activation_id
        .take()
        .or_else(|| existing.skill_activation_id.clone());
    existing.tool_contract_id = update
        .tool_contract_id
        .take()
        .or_else(|| existing.tool_contract_id.clone());
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
    if update.result_summary.is_some() {
        existing.result_summary = update.result_summary.take();
    }
    existing.artifact_refs.extend(update.artifact_refs);
    existing.artifact_refs.sort();
    existing.artifact_refs.dedup();
    existing.evidence_refs.extend(update.evidence_refs);
    existing.evidence_refs.sort();
    existing.evidence_refs.dedup();
    existing.definition_refs.extend(update.definition_refs);
    existing.definition_refs.sort();
    existing.definition_refs.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn graph_node(
        id: &str,
        status: ExecutionNodeStatus,
        required: bool,
    ) -> harness_contract::execution_graph::ExecutionNodeProjection {
        harness_contract::execution_graph::ExecutionNodeProjection {
            node_id: id.to_string(),
            kind: ExecutionNodeKind::Verify,
            status,
            executor_kind: "verify".to_string(),
            payload_ref: String::new(),
            acceptance: Default::default(),
            resource_scopes: Vec::new(),
            result_ref: None,
            summary: None,
            failure: None,
            evidence_refs: Vec::new(),
            usage: Default::default(),
            work: Some(harness_contract::execution_graph::ExecutionWorkProjection {
                role: harness_contract::execution_graph::ExecutionWorkRole::Verify,
                required,
                dependency: Default::default(),
                cancellation_group: None,
                expected_input_tokens: 0,
                expected_output_tokens: 0,
                expected_duration_ms: 0,
            }),
        }
    }

    fn graph_with_nodes(
        nodes: Vec<harness_contract::execution_graph::ExecutionNodeProjection>,
    ) -> ExecutionGraphProjection {
        ExecutionGraphProjection {
            graph_id: "execution".to_string(),
            revision: 1,
            objective: "test".to_string(),
            service_class: Default::default(),
            parent_execution: None,
            lineage: None,
            orchestration: None,
            nodes,
            edges: Vec::new(),
            commit_cursor: 1,
            terminal_result_ref: None,
            work: None,
        }
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

    fn bound_event(
        scope: RuntimeEventScope,
        event_id: &str,
        kind: &str,
        status: &str,
        cursor: u64,
        binding: harness_contract::projection::RuntimeActivityBinding,
        mut payload: serde_json::Value,
    ) -> DurableRuntimeEvent {
        payload
            .as_object_mut()
            .expect("test payload object")
            .insert(
                "_runtime_activity_binding".to_string(),
                serde_json::to_value(binding).expect("serialize activity binding"),
            );
        scoped_event(scope, event_id, kind, status, cursor, Vec::new(), payload)
    }

    fn activity_binding(
        activity_id: &str,
        parent_activity_id: &str,
    ) -> harness_contract::projection::RuntimeActivityBinding {
        harness_contract::projection::RuntimeActivityBinding {
            root_execution_id: "execution-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_task_id: "task-root-1".to_string(),
            task_id: "task-root-1".to_string(),
            activity_id: activity_id.to_string(),
            node_id: None,
            parent_activity_id: Some(parent_activity_id.to_string()),
            initiator_activity_id: Some(parent_activity_id.to_string()),
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
            revision: 1,
            fence: 1,
            generation: 1,
        }
    }

    #[test]
    fn activity_membership_uses_current_turn_generation_fence() {
        let mut graph = graph_with_nodes(Vec::new());
        graph.graph_id = "execution-1".to_string();
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_task_id: "task-root-1".to_string(),
            task_id: "task-root-1".to_string(),
            generation: 7,
        });
        let mut binding = activity_binding(
            "activity:execution:execution-1:agent:agent-1",
            "activity:execution:execution-1",
        );
        binding.task_id = "task-child-1".to_string();
        binding.generation = 7;
        let current = bound_event(
            RuntimeEventScope::Agent,
            "agent-current",
            "agent.progress",
            "running",
            1,
            binding.clone(),
            serde_json::json!({}),
        );
        assert!(event_belongs_to_graph(&current, &graph));

        let mut wrong_root_task = binding.clone();
        wrong_root_task.root_task_id = "task-other-root".to_string();
        let unrelated = bound_event(
            RuntimeEventScope::Agent,
            "agent-unrelated-root",
            "agent.progress",
            "running",
            2,
            wrong_root_task,
            serde_json::json!({}),
        );
        assert!(!event_belongs_to_graph(&unrelated, &graph));

        binding.generation = 6;
        let stale = bound_event(
            RuntimeEventScope::Agent,
            "agent-stale",
            "agent.progress",
            "running",
            2,
            binding,
            serde_json::json!({}),
        );
        assert!(!event_belongs_to_graph(&stale, &graph));
    }

    #[test]
    fn tool_lifecycle_events_share_one_stable_activity_identity() {
        let binding = activity_binding(
            "activity:execution:execution-1:tool:call-1",
            "activity:execution:execution-1",
        );
        let started = bound_event(
            RuntimeEventScope::Tool,
            "event-started",
            "tool.invocation.started",
            "running",
            1,
            binding.clone(),
            serde_json::json!({"tool_call_id": "call-1"}),
        );
        let completed = bound_event(
            RuntimeEventScope::Tool,
            "event-completed",
            "tool.invocation.completed",
            "completed",
            2,
            binding,
            serde_json::json!({"tool_call_id": "call-1"}),
        );
        assert_eq!(
            started
                .activity_binding()
                .map(|binding| binding.activity_id),
            completed
                .activity_binding()
                .map(|binding| binding.activity_id)
        );
    }

    #[test]
    fn optional_failure_completes_root_with_warnings() {
        let graph = graph_with_nodes(vec![
            graph_node("required", ExecutionNodeStatus::Completed, true),
            graph_node("optional", ExecutionNodeStatus::Failed, false),
        ]);

        assert_eq!(graph_status(&graph), "completed_with_warnings");
        assert_eq!(
            graph_status_reason(&graph).as_deref(),
            Some("1 optional activities did not complete")
        );
    }

    #[test]
    fn required_failure_fails_root() {
        let graph = graph_with_nodes(vec![
            graph_node("required", ExecutionNodeStatus::Failed, true),
            graph_node("optional", ExecutionNodeStatus::Completed, false),
        ]);

        assert_eq!(graph_status(&graph), "failed");
        assert_eq!(
            graph_status_reason(&graph).as_deref(),
            Some("1 required activities failed or blocked")
        );
    }

    #[test]
    fn protocol_event_kind_is_not_used_as_public_summary() {
        let event = scoped_event(
            RuntimeEventScope::Recovery,
            "event",
            "runtime.internal.phase.changed",
            "completed",
            1,
            Vec::new(),
            serde_json::json!({}),
        );

        assert_eq!(event_public_summary(&event), None);
    }

    #[test]
    fn point_in_time_policy_events_do_not_remain_running() {
        let capability = scoped_event(
            RuntimeEventScope::Tool,
            "capability",
            "tool.authorization.capability_assessed",
            "running",
            1,
            Vec::new(),
            serde_json::json!({}),
        );
        let lifecycle = scoped_event(
            RuntimeEventScope::Tool,
            "started",
            "tool.invocation.started",
            "running",
            2,
            Vec::new(),
            serde_json::json!({}),
        );

        assert_eq!(event_activity_status(&capability), "completed");
        assert_eq!(event_activity_status(&lifecycle), "running");
    }

    #[test]
    fn lifecycle_merge_preserves_start_and_applies_terminal_state() {
        let scope = ExecutionScopeProjection {
            workspace_id: "workspace".to_string(),
            mission_id: None,
            task_id: None,
            root_task_id: None,
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
            node_id: Some("node".to_string()),
            display_label: Some("search".to_string()),
            phase: Some("tool".to_string()),
            visibility: vec![ActivityVisibility::Narrative],
            parent_activity_id: Some("parent".to_string()),
            initiator_activity_id: Some("parent".to_string()),
            causal_parent_ids: Vec::new(),
            dependency_ids: Vec::new(),
            parallel_group_id: Some("batch".to_string()),
            team_run_id: None,
            agent_instance_id: Some("agent".to_string()),
            agent_run_id: Some("agent-run".to_string()),
            skill_id: None,
            skill_revision: None,
            skill_activation_id: None,
            tool_contract_id: Some("search".to_string()),
            tool_call_id: Some("call".to_string()),
            approval_id: None,
            status: "running".to_string(),
            status_reason: None,
            required: true,
            started_at_ms: Some(10),
            completed_at_ms: None,
            duration_ms: None,
            sequence: 1,
            commit_cursor: 1,
            public_summary: Some("search".to_string()),
            result_summary: None,
            artifact_refs: Vec::new(),
            evidence_refs: vec!["evidence-start".to_string()],
            definition_refs: Vec::new(),
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
        let mut binding = activity_binding(
            "activity:execution:execution-1:node:research",
            "activity:execution:execution-1:node:team",
        );
        binding.node_id = Some("research".to_string());
        binding.team_run_id = Some("team-run-1".to_string());
        binding.agent_instance_id = Some("researcher".to_string());
        binding.agent_run_id = Some("agent-run-1".to_string());
        let event = bound_event(
            RuntimeEventScope::Agent,
            "agent-started",
            "agent.execution.started",
            "provider-backed child execution admitted",
            1,
            binding,
            serde_json::json!({
                "snapshot": {
                    "status": "running",
                    "started_at_ms": 10,
                    "agent_id": "researcher",
                    "run_id": "agent-run-1"
                }
            }),
        );
        let binding = event.activity_binding().expect("bound activity");
        assert_eq!(
            binding.activity_id,
            "activity:execution:execution-1:node:research"
        );
        assert_eq!(binding.agent_instance_id.as_deref(), Some("researcher"));
        assert_eq!(binding.agent_run_id.as_deref(), Some("agent-run-1"));
        assert_eq!(event_activity_status(&event), "running");
        assert_eq!(
            binding.parent_activity_id.as_deref(),
            Some("activity:execution:execution-1:node:team")
        );
    }

    #[test]
    fn child_tool_binding_links_to_exact_agent_activity() {
        let mut binding = activity_binding(
            "activity:execution:execution-1:tool:call-1",
            "activity:execution:execution-1:node:research",
        );
        binding.agent_run_id = Some("agent-run-1".to_string());
        binding.tool_call_id = Some("call-1".to_string());
        binding.tool_contract_id = Some("web_search".to_string());
        let event = bound_event(
            RuntimeEventScope::Tool,
            "tool-started",
            "tool.invocation.started",
            "running",
            1,
            binding,
            serde_json::json!({
                "tool_call_id": "call-1",
                "tool_name": "web_search",
                "status": "running",
                "started_at_ms": 10
            }),
        );
        let binding = event.activity_binding().expect("bound activity");
        assert_eq!(binding.agent_run_id.as_deref(), Some("agent-run-1"));
        assert_eq!(
            binding.parent_activity_id.as_deref(),
            Some("activity:execution:execution-1:node:research")
        );
    }

    #[test]
    fn root_projection_keeps_bound_tools_from_child_agent_streams() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut graph = graph_with_nodes(Vec::new());
        graph.graph_id = "execution-1".to_string();
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_task_id: "task-root-1".to_string(),
            task_id: "task-root-1".to_string(),
            generation: 1,
        });
        let scope = ExecutionProjectionScope {
            session_id: Some("session-1".to_string()),
            mission_id: None,
            task_id: Some("task-root-1".to_string()),
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
        };
        let root_id = execution_activity_id("execution-1");
        let mut events = Vec::new();
        let mut cursor = 1;
        for (agent, tool_count) in [("researcher-1", 3usize), ("researcher-2", 4usize)] {
            let agent_activity_id = format!("activity:execution:execution-1:agent:{agent}");
            let child_task_id = format!("task:{agent}");
            let mut agent_binding = activity_binding(&agent_activity_id, &root_id);
            agent_binding.task_id.clone_from(&child_task_id);
            agent_binding.agent_instance_id = Some(agent.to_string());
            agent_binding.agent_run_id = Some(format!("run:{agent}"));
            events.push(bound_event(
                RuntimeEventScope::Agent,
                &format!("agent:{agent}"),
                "agent.execution.started",
                "running",
                cursor,
                agent_binding,
                serde_json::json!({"agent_id": agent}),
            ));
            cursor += 1;

            let mut skill_binding = activity_binding(
                &format!("activity:execution:execution-1:skill:{agent}"),
                &agent_activity_id,
            );
            skill_binding.task_id.clone_from(&child_task_id);
            skill_binding.agent_instance_id = Some(agent.to_string());
            skill_binding.agent_run_id = Some(format!("run:{agent}"));
            skill_binding.skill_id = Some("workspace-research".to_string());
            skill_binding.skill_revision = Some("1".to_string());
            skill_binding.skill_activation_id = Some(format!("activation:{agent}"));
            events.push(bound_event(
                RuntimeEventScope::Skill,
                &format!("skill:{agent}"),
                "skill.activation.selected",
                "completed",
                cursor,
                skill_binding,
                serde_json::json!({
                    "skill_id": "workspace-research",
                    "activation_id": format!("activation:{agent}"),
                }),
            ));
            cursor += 1;

            let mut reasoning_binding = activity_binding(
                &format!("activity:execution:execution-1:reasoning:{agent}"),
                &agent_activity_id,
            );
            reasoning_binding.task_id.clone_from(&child_task_id);
            reasoning_binding.agent_instance_id = Some(agent.to_string());
            reasoning_binding.agent_run_id = Some(format!("run:{agent}"));
            events.push(bound_event(
                RuntimeEventScope::Session,
                &format!("reasoning:{agent}"),
                "model.item_completed",
                "completed",
                cursor,
                reasoning_binding,
                serde_json::json!({
                    "kind": "public_reasoning",
                    "content": format!("{agent} 正在核查工作区"),
                }),
            ));
            cursor += 1;

            for index in 0..tool_count {
                let call_id = format!("{agent}-tool-{index}");
                let mut tool_binding = activity_binding(
                    &format!("activity:execution:execution-1:tool:{call_id}"),
                    &agent_activity_id,
                );
                tool_binding.task_id.clone_from(&child_task_id);
                tool_binding.agent_instance_id = Some(agent.to_string());
                tool_binding.agent_run_id = Some(format!("run:{agent}"));
                tool_binding.tool_call_id = Some(call_id.clone());
                tool_binding.tool_contract_id = Some("read_file".to_string());
                let mut event = bound_event(
                    RuntimeEventScope::Tool,
                    &format!("event:{call_id}"),
                    "tool.invocation.completed",
                    "completed",
                    cursor,
                    tool_binding,
                    serde_json::json!({
                        "tool_call_id": call_id,
                        "tool_name": "read_file",
                    }),
                );
                event.stream_id = format!("agent-child:{agent}:tool");
                events.push(event);
                cursor += 1;
            }
        }
        let events = events
            .into_iter()
            .filter(|event| event_belongs_to_graph(event, &graph))
            .collect::<Vec<_>>();
        let (activities, _) = project_single_execution_activities_from_events(
            &services, &scope, &graph, events, false,
        );
        let tools = activities
            .iter()
            .filter(|activity| activity.kind == ExecutionActivityKind::Tool)
            .collect::<Vec<_>>();
        assert_eq!(tools.len(), 7);
        assert_eq!(
            activities
                .iter()
                .filter(|activity| activity.kind == ExecutionActivityKind::Skill)
                .count(),
            2
        );
        assert_eq!(
            activities
                .iter()
                .filter(|activity| activity.kind == ExecutionActivityKind::Reasoning)
                .count(),
            2
        );
        for tool in tools {
            let owner = tool.agent_instance_id.as_deref().expect("agent owner");
            let expected_parent = format!("activity:execution:execution-1:agent:{owner}");
            assert_eq!(
                tool.parent_activity_id.as_deref(),
                Some(expected_parent.as_str())
            );
        }
    }

    #[test]
    fn public_reasoning_is_narrative_and_audit_only() {
        let event = bound_event(
            RuntimeEventScope::Session,
            "reasoning",
            "model.item_completed",
            "completed",
            1,
            activity_binding(
                "activity:execution:execution-1:reasoning:item-1",
                "activity:execution:execution-1",
            ),
            serde_json::json!({
                "kind": "public_reasoning",
                "content": "核对真实调用链",
            }),
        );
        let (kind, visibility) = event_kind(&event);
        assert_eq!(kind, ExecutionActivityKind::Reasoning);
        assert_eq!(
            visibility,
            vec![ActivityVisibility::Narrative, ActivityVisibility::Audit]
        );
        assert_eq!(
            event_public_summary(&event).as_deref(),
            Some("核对真实调用链")
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
    fn materialized_results_distinguish_outcomes_and_consumed_artifacts() {
        let scope = ExecutionScopeProjection {
            workspace_id: "workspace".to_string(),
            mission_id: Some("mission".to_string()),
            task_id: Some("task".to_string()),
            root_task_id: Some("task".to_string()),
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
                    node_id: None,
                    display_label: None,
                    phase: None,
                    visibility: vec![ActivityVisibility::Narrative],
                    parent_activity_id: None,
                    initiator_activity_id: None,
                    causal_parent_ids: Vec::new(),
                    dependency_ids: Vec::new(),
                    parallel_group_id: None,
                    team_run_id: None,
                    agent_instance_id: None,
                    agent_run_id: None,
                    skill_id: None,
                    skill_revision: None,
                    skill_activation_id: None,
                    tool_contract_id: None,
                    tool_call_id: None,
                    approval_id: None,
                    status: "completed".to_string(),
                    status_reason: None,
                    required: true,
                    started_at_ms: Some(10),
                    completed_at_ms: Some(20),
                    duration_ms: Some(10),
                    sequence: 1,
                    commit_cursor: 1,
                    public_summary: None,
                    result_summary: None,
                    artifact_refs,
                    evidence_refs: vec!["evidence:source".to_string()],
                    definition_refs: Vec::new(),
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
        insert_committed_predecessor_consumed_relations(
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

        let tool_activity_id = "activity:execution:execution:tool:consumer".to_string();
        let mut tool_activity = activities[&node_activity_id("execution", "consumer")].clone();
        tool_activity.activity_id = tool_activity_id.clone();
        tool_activity.kind = ExecutionActivityKind::Tool;
        tool_activity.tool_call_id = Some("consumer".to_string());
        tool_activity.tool_contract_id = Some("render_report".to_string());
        activities.insert(tool_activity_id.clone(), tool_activity);
        let input_event = bound_event(
            RuntimeEventScope::Tool,
            "tool-consumer-started",
            "tool.invocation.started",
            "running",
            3,
            activity_binding(
                &tool_activity_id,
                &node_activity_id("execution", "consumer"),
            ),
            serde_json::json!({
                "tool_call_id": "consumer",
                "input_refs": ["artifact:source"]
            }),
        );
        insert_explicit_tool_consumed_relations(&mut relations, &activities, &[input_event]);
        assert!(relations.values().any(|relation| {
            relation.kind == ActivityRelationKind::Consumed
                && relation.from_activity_id == artifact_activity_id("execution", "artifact:source")
                && relation.to_activity_id == tool_activity_id
                && relation.evidence_ref.as_deref() == Some("artifact:source")
        }));
    }

    #[test]
    fn internal_transport_artifacts_are_not_narrative() {
        assert!(
            !artifact_visibility("session-ingress-confirmed:webui:session:request")
                .contains(&ActivityVisibility::Narrative)
        );
        assert!(
            !artifact_visibility("session-ingress-graph:execution:tool-results:2")
                .contains(&ActivityVisibility::Narrative)
        );
        assert!(artifact_visibility("workspace://reports/result.md")
            .contains(&ActivityVisibility::Narrative));
    }
}
