use super::reducer_support::*;
use super::snapshot::{entity_from_runtime_event, is_strategy_event, strategy_entity};
use super::*;

pub fn delta(
    services: &RuntimeServices,
    execution_id: &str,
    base_revision: u64,
    base_cursor: u64,
    context: &ProjectionQueryContext,
) -> Result<ProjectionDelta, RuntimeServicesError> {
    validate_context(services, context)?;
    let upper_bound = *services.event_store().subscribe_commits().borrow();
    let batches = services
        .event_store()
        .events_after_cursor(base_cursor, MAX_DELTA_BATCHES.saturating_add(1))?
        .into_iter()
        .take_while(|batch| batch.commit_cursor <= upper_bound)
        .collect::<Vec<_>>();
    let target_cursor = batches
        .last()
        .map_or(base_cursor, |batch| batch.commit_cursor);
    if upper_bound > base_cursor
        && (batches.is_empty() || batches.len() > MAX_DELTA_BATCHES || target_cursor < upper_bound)
    {
        return Ok(resync_delta(
            execution_id,
            base_revision,
            base_cursor,
            upper_bound,
            context,
            ProjectionResyncReason::RetentionGap,
        ));
    }
    let graph = services.graph_state_store().projection(execution_id)?;
    if graph.revision < base_revision {
        return Ok(resync_delta(
            execution_id,
            base_revision,
            base_cursor,
            upper_bound,
            context,
            ProjectionResyncReason::UnsafeMaterialization,
        ));
    }
    let scope = ExecutionProjectionScope::load(
        services,
        execution_id,
        &graph,
        context.detail_scope == ProjectionDetailScope::Full,
    )?;
    validate_projection_scope(&scope, context)?;
    if graph.commit_cursor > target_cursor {
        return Ok(resync_delta(
            execution_id,
            base_revision,
            base_cursor,
            upper_bound,
            context,
            ProjectionResyncReason::UnsafeMaterialization,
        ));
    }
    let mut visible_events = Vec::new();
    for batch in batches {
        for event in batch.events {
            if scope.contains_event(&event) {
                visible_events.push(event);
            }
        }
    }
    let operations = materialize_delta_operations(
        services,
        execution_id,
        &graph,
        &scope,
        &visible_events,
        base_revision,
        base_cursor,
        target_cursor,
        context,
    )?;
    Ok(ProjectionDelta {
        schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
        reducer_version: EXECUTION_PROJECTION_REDUCER_VERSION,
        execution_id: execution_id.to_string(),
        from_revision: base_revision,
        target_revision: graph.revision,
        base_cursor,
        target_cursor,
        detail_scope: context.detail_scope,
        authorization_revision: context.authorization_revision,
        redaction_revision: redaction_revision(context),
        source_health: ProjectionSourceHealth::Fresh,
        operations,
        resync_reason: None,
    })
}

fn resync_delta(
    execution_id: &str,
    base_revision: u64,
    base_cursor: u64,
    target_cursor: u64,
    context: &ProjectionQueryContext,
    reason: ProjectionResyncReason,
) -> ProjectionDelta {
    ProjectionDelta {
        schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
        reducer_version: EXECUTION_PROJECTION_REDUCER_VERSION,
        execution_id: execution_id.to_string(),
        from_revision: base_revision,
        target_revision: base_revision,
        base_cursor,
        target_cursor,
        detail_scope: context.detail_scope,
        authorization_revision: context.authorization_revision,
        redaction_revision: redaction_revision(context),
        source_health: ProjectionSourceHealth::Lagged,
        operations: Vec::new(),
        resync_reason: Some(reason),
    }
}

fn materialize_delta_operations(
    services: &RuntimeServices,
    execution_id: &str,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    scope: &ExecutionProjectionScope,
    events: &[crate::DurableRuntimeEvent],
    base_revision: u64,
    _base_cursor: u64,
    target_cursor: u64,
    context: &ProjectionQueryContext,
) -> Result<Vec<ProjectionOperation>, RuntimeServicesError> {
    let full = context.detail_scope == ProjectionDetailScope::Full;
    let mut operations = Vec::new();
    let graph_changed = graph.revision != base_revision;
    let topology_changed = events.iter().any(|event| {
        event.kind.contains("register")
            || event.kind.contains("replan")
            || event.kind.contains("lineage")
    });
    // Root snapshots include the complete durable descendant lineage. Child
    // graph commits do not increment the root graph revision, so a child
    // change must replace the inclusive activities instead of being
    // misaddressed as a root-node upsert.
    let descendant_changed = events.iter().any(|event| {
        scope
            .descendant_graphs
            .iter()
            .any(|descendant| super::activity::event_belongs_to_graph(event, descendant))
    });
    let affected_node_ids = events
        .iter()
        .filter(|event| event.scope == RuntimeEventScope::ExecutionNode)
        .filter_map(|event| {
            event
                .payload
                .get("node_id")
                .and_then(serde_json::Value::as_str)
                .or_else(|| {
                    event
                        .stream_id
                        .strip_prefix(&format!("{execution_id}:node:"))
                })
        })
        .collect::<BTreeSet<_>>();

    if graph.revision != base_revision || !events.is_empty() {
        operations.push(ProjectionOperation::SetProjectionHeader {
            revision: graph.revision,
            session_id: scope.session_id.clone(),
            mission_id: scope.mission_id.clone(),
            task_id: scope.task_id.clone(),
            turn_id: scope.turn_id.clone(),
        });
    }
    if graph_changed {
        operations.push(ProjectionOperation::SetGraphMetadata {
            revision: graph.revision,
            commit_cursor: graph.commit_cursor,
            objective: graph.objective.clone(),
            service_class: graph.service_class,
            parent_execution: graph.parent_execution.clone(),
        });
        operations.push(ProjectionOperation::ReplaceGraphOrchestration {
            orchestration: graph.orchestration.clone(),
        });
        if topology_changed {
            operations.push(ProjectionOperation::ReplaceGraphTopology {
                node_ids: graph
                    .nodes
                    .iter()
                    .map(|node| node.node_id.clone())
                    .collect(),
                edges: graph.edges.clone(),
            });
        }
        let replace_all_nodes = topology_changed || affected_node_ids.is_empty();
        operations.extend(
            graph
                .nodes
                .iter()
                .filter(|node| {
                    replace_all_nodes || affected_node_ids.contains(node.node_id.as_str())
                })
                .cloned()
                .map(|node| ProjectionOperation::UpsertGraphNode { node }),
        );
        operations.push(ProjectionOperation::ReplaceAvailableCommands {
            commands: available_commands_for_graph(graph),
        });
    }
    if graph_changed || !events.is_empty() {
        operations.push(ProjectionOperation::SetDeliveryTruth {
            delivery_envelope: graph.delivery_envelope.clone(),
            terminal_presentation: graph.terminal_presentation.clone(),
            cancellation_receipt: services
                .latest_cancellation_receipt_for_execution(
                    scope.session_id.as_deref().unwrap_or_default(),
                    execution_id,
                    scope.turn_id.as_deref().unwrap_or_default(),
                )
                .ok()
                .flatten(),
        });
    }

    if events.iter().any(|event| is_strategy_event(&event.kind)) {
        operations.push(ProjectionOperation::ReplaceStrategy {
            strategy: strategy_entity(services, scope, execution_id, full, context),
        });
    }
    if events
        .iter()
        .any(|event| event.kind == "execution.lineage.child_registered.v1")
    {
        operations.extend(
            scope
                .child_executions
                .iter()
                .cloned()
                .map(|child| ProjectionOperation::UpsertChildExecution { child }),
        );
    }

    if graph_changed || !events.is_empty() {
        operations.push(ProjectionOperation::ReplaceConcurrency {
            concurrency: super::snapshot::execution_concurrency(services, graph, scope),
        });
        if topology_changed || descendant_changed {
            let (activities, relations) =
                super::activity::project_execution_activities(services, scope, graph, full);
            operations.push(ProjectionOperation::ReplaceActivities {
                activities,
                relations,
            });
        } else {
            let mut changed_activity_ids = affected_node_ids
                .iter()
                .map(|node_id| format!("activity:execution:{execution_id}:node:{node_id}"))
                .collect::<BTreeSet<_>>();
            changed_activity_ids.extend(
                events.iter().filter_map(|event| {
                    event.activity_binding().map(|binding| binding.activity_id)
                }),
            );
            if graph_changed && changed_activity_ids.is_empty() {
                changed_activity_ids.insert(format!("activity:execution:{execution_id}"));
            }
            let (activities, relations) =
                project_activity_changes(services, scope, graph, &changed_activity_ids, full)?;
            let produced_activity_ids = activities
                .iter()
                .filter(|activity| {
                    activity
                        .parent_activity_id
                        .as_ref()
                        .is_some_and(|parent| changed_activity_ids.contains(parent))
                })
                .map(|activity| activity.activity_id.clone())
                .collect::<BTreeSet<_>>();
            changed_activity_ids.extend(produced_activity_ids);
            operations.extend(
                activities
                    .into_iter()
                    .filter(|activity| changed_activity_ids.contains(&activity.activity_id))
                    .map(|activity| ProjectionOperation::UpsertActivity { activity }),
            );
            operations.extend(
                relations
                    .into_iter()
                    .filter(|relation| {
                        changed_activity_ids.contains(&relation.from_activity_id)
                            || changed_activity_ids.contains(&relation.to_activity_id)
                    })
                    .map(|relation| ProjectionOperation::UpsertActivityRelation { relation }),
            );
        }
    }

    materialize_canonical_collection(
        &mut operations,
        events,
        RuntimeEventScope::Goal,
        ProjectionEntityCollection::Goals,
        &scope.goals,
    );
    materialize_canonical_collection(
        &mut operations,
        events,
        RuntimeEventScope::Agent,
        ProjectionEntityCollection::Agents,
        &scope.agents,
    );
    materialize_canonical_collection(
        &mut operations,
        events,
        RuntimeEventScope::Team,
        ProjectionEntityCollection::Teams,
        &scope.teams,
    );
    materialize_canonical_collection(
        &mut operations,
        events,
        RuntimeEventScope::Relation,
        ProjectionEntityCollection::Relations,
        &scope.relations,
    );
    materialize_canonical_collection(
        &mut operations,
        events,
        RuntimeEventScope::Approval,
        ProjectionEntityCollection::Approvals,
        &scope.approvals,
    );

    for event in events {
        if super::activity::requires_activity_binding(event) && event.activity_binding().is_none() {
            for entity in activity_binding_health_entities(std::slice::from_ref(event), full) {
                operations.push(ProjectionOperation::UpsertEntity {
                    collection: ProjectionEntityCollection::Health,
                    entity,
                });
            }
        }
        if event.kind.starts_with("resource.admission.") {
            operations.push(event_entity_operation(
                ProjectionEntityCollection::Admissions,
                "admission",
                event,
                full,
            ));
            continue;
        }
        if event.kind == crate::execution_core::OUTCOME_EVENT_KIND {
            operations.push(event_entity_operation(
                ProjectionEntityCollection::Outcomes,
                "outcome",
                event,
                full,
            ));
            continue;
        }
        if event.scope == RuntimeEventScope::Tool || event.kind.contains("usage") {
            operations.push(event_entity_operation(
                ProjectionEntityCollection::Usage,
                "usage",
                event,
                full,
            ));
        }
        if event.kind.contains("context") || event.kind.contains("memory") {
            operations.push(event_entity_operation(
                ProjectionEntityCollection::Context,
                "context",
                event,
                full,
            ));
        }
        if !is_strategy_event(&event.kind)
            && (!event.refs.is_empty() || event.kind.contains("evidence"))
        {
            operations.push(event_entity_operation(
                ProjectionEntityCollection::Evidence,
                "evidence",
                event,
                full,
            ));
        }
        if event.scope == RuntimeEventScope::Recovery || event.kind.contains("recovery") {
            operations.push(event_entity_operation(
                ProjectionEntityCollection::Recovery,
                "recovery",
                event,
                full,
            ));
        }
    }

    if !events.is_empty() {
        operations.push(ProjectionOperation::UpsertEntity {
            collection: ProjectionEntityCollection::Health,
            entity: execution_health_entity(execution_id, graph, full),
        });
    }
    if graph.terminal_result_ref.is_some()
        || graph.nodes.iter().all(|node| node.status.is_terminal())
    {
        operations.push(ProjectionOperation::SetTerminal {
            terminal_result_ref: graph.terminal_result_ref.clone(),
            live: services.execution_live(execution_id),
        });
    }
    operations.push(ProjectionOperation::AdvanceCursor {
        cursor: target_cursor,
    });
    Ok(operations)
}

fn project_activity_changes(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    activity_ids: &BTreeSet<String>,
    full: bool,
) -> Result<
    (
        Vec<harness_contract::projection::ExecutionActivityProjection>,
        Vec<harness_contract::projection::ExecutionActivityRelation>,
    ),
    RuntimeServicesError,
> {
    const PAGE_SIZE: usize = 256;
    let mut events = Vec::new();
    for activity_id in activity_ids {
        let mut after = None;
        loop {
            let page = services
                .event_store()
                .events_for_activity(activity_id, after, PAGE_SIZE)
                .map_err(RuntimeServicesError::Invariant)?;
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
    }
    events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
    events.dedup_by(|left, right| left.event_id == right.event_id);
    Ok(super::activity::project_execution_activities_from_events(
        services, scope, graph, events, full,
    ))
}

fn materialize_canonical_collection(
    operations: &mut Vec<ProjectionOperation>,
    events: &[crate::DurableRuntimeEvent],
    scope: RuntimeEventScope,
    collection: ProjectionEntityCollection,
    entities: &[ProjectionEntity],
) {
    if events.iter().any(|event| event.scope == scope) {
        operations.extend(
            entities
                .iter()
                .cloned()
                .map(|entity| ProjectionOperation::UpsertEntity { collection, entity }),
        );
    }
}

fn event_entity_operation(
    collection: ProjectionEntityCollection,
    kind: &str,
    event: &crate::DurableRuntimeEvent,
    full: bool,
) -> ProjectionOperation {
    ProjectionOperation::UpsertEntity {
        collection,
        entity: entity_from_runtime_event(kind, event.clone(), full),
    }
}
