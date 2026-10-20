use super::reducer_support::*;
use super::*;

pub fn delta(
    services: &RuntimeServices,
    execution_id: &str,
    base_revision: u64,
    base_cursor: u64,
    context: &ProjectionQueryContext,
) -> Result<ProjectionDelta, RuntimeServicesError> {
    validate_context(services, context)?;
    let mut budget = history::RequestBudget::default();
    budget.capture_source_fence(services)?;
    let upper_bound = services.event_store().durable_commit_cursor()?;
    if base_cursor > upper_bound {
        return Ok(resync_delta(
            execution_id,
            base_revision,
            base_cursor,
            upper_bound,
            context,
            ProjectionResyncReason::UnsafeMaterialization,
        ));
    }
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
    if services.event_store().projection_checkpoint(&format!("projection-history:{execution_id}"))?
        .is_some_and(|saved| saved.payload["schema"] == 6 && saved.payload["incomplete"] == true)
    {
        return Ok(resync_delta(execution_id, base_revision, base_cursor, upper_bound,
            context, ProjectionResyncReason::UnsafeMaterialization));
    }
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
    let scope = ExecutionProjectionScope::load_budgeted(
        services,
        execution_id,
        &graph,
        context.detail_scope == ProjectionDetailScope::Full,
        target_cursor,
        &mut budget,
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
    let mut result = ProjectionDelta {
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
        operations: Vec::new(),
        resync_reason: None,
    };
    if !budget.cached_source_current(services, execution_id, target_cursor)? {
        return Ok(resync_delta(execution_id, base_revision, base_cursor, upper_bound,
            context, ProjectionResyncReason::UnsafeMaterialization));
    }
    if target_cursor == base_cursor && graph.revision == base_revision {
        let key =
            ExecutionProjectionCacheKey::new(execution_id, base_revision, base_cursor, context);
        if services
            .cached_execution_projection(&key)
            .is_some_and(|projection| {
                projection
                    .terminal_axes
                    .execution
                    .evidence_refs
                    .iter()
                    .any(|reference| super::settlement::local_reference(reference))
            })
        {
            return Ok(resync_delta(
                execution_id,
                base_revision,
                base_cursor,
                upper_bound,
                context,
                ProjectionResyncReason::UnsafeMaterialization,
            ));
        }
        validate_context(services, context)?;
        budget.require_captured_source(services)?;
    history::check_bytes(&result)?;
        return Ok(result);
    }
    let base_key =
        ExecutionProjectionCacheKey::new(execution_id, base_revision, base_cursor, context);
    let Some(base) = services
        .cached_execution_projection(&base_key)
        .filter(|base| {
            base.schema_version == EXECUTION_PROJECTION_SCHEMA_VERSION
                && base.execution_id == execution_id
                && base.revision == base_revision
                && base.cursor == base_cursor
                && base.detail_scope == context.detail_scope
                && base.authorization_revision == context.authorization_revision
                && base.redaction_revision == redaction_revision(context)
                && !base.terminal_axes.source_race
                && !base.health.iter().any(|entity| entity.kind == "history_verification" && entity.status.as_deref() == Some("incomplete"))
        })
    else {
        return Ok(resync_delta(
            execution_id,
            base_revision,
            base_cursor,
            upper_bound,
            context,
            ProjectionResyncReason::UnsafeMaterialization,
        ));
    };
    for batch in batches {
        for record in batch.events {
            budget.charge_record(&record)?;
        }
    }
    let facts = match history::load_scope_budgeted(services, &scope, target_cursor, false, &mut budget) {
        Ok(facts) => facts,
        Err(RuntimeServicesError::Invariant(message)) if message == history::INCOMPLETE => {
            return Ok(resync_delta(execution_id, base_revision, base_cursor, upper_bound,
                context, ProjectionResyncReason::UnsafeMaterialization));
        }
        Err(error) => return Err(error),
    };
    if facts.values().any(|state| state.incomplete) {
        return Ok(resync_delta(execution_id, base_revision, base_cursor,
            upper_bound, context, ProjectionResyncReason::UnsafeMaterialization));
    }
    let commands = available_commands_for_graph(&graph);
    let target = super::snapshot::materialize_snapshot(
        services,
        execution_id,
        context,
        graph,
        scope,
        facts,
        target_cursor,
        commands,
        &mut budget,
    )?;
    if target.terminal_axes.source_cursor != target_cursor || target.terminal_axes.source_race {
        return Ok(resync_delta(
            execution_id,
            base_revision,
            base_cursor,
            upper_bound,
            context,
            ProjectionResyncReason::UnsafeMaterialization,
        ));
    }
    result.operations = materialize_delta_operations(&base, &target);
    let reduced = harness_contract::projection::reduce_projection_delta(&base, &result);
    if !matches!(reduced, Ok(ref projection) if projection == &target) {
        return Ok(resync_delta(
            execution_id,
            base_revision,
            base_cursor,
            upper_bound,
            context,
            ProjectionResyncReason::UnsafeMaterialization,
        ));
    }
    validate_context(services, context)?;
    budget.require_captured_source(services)?;
    history::check_bytes(&result)?;
    if services.event_store().durable_commit_cursor()? != target_cursor {
        return Ok(resync_delta(
            execution_id,
            base_revision,
            base_cursor,
            upper_bound,
            context,
            ProjectionResyncReason::UnsafeMaterialization,
        ));
    }
    budget.publish(services)?;
    let target_key =
        ExecutionProjectionCacheKey::new(execution_id, target.revision, target.cursor, context);
    services.cache_execution_projection(target_key, target);
    Ok(result)
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
    base: &ExecutionProjection,
    target: &ExecutionProjection,
) -> Vec<ProjectionOperation> {
    let mut operations = Vec::new();
    if base.revision != target.revision
        || base.session_id != target.session_id
        || base.mission_id != target.mission_id
        || base.task_id != target.task_id
        || base.turn_id != target.turn_id
    {
        operations.push(ProjectionOperation::SetProjectionHeader {
            revision: target.revision,
            session_id: target.session_id.clone(),
            mission_id: target.mission_id.clone(),
            task_id: target.task_id.clone(),
            turn_id: target.turn_id.clone(),
        });
    }
    if base.graph.revision != target.graph.revision
        || base.graph.commit_cursor != target.graph.commit_cursor
        || base.graph.objective != target.graph.objective
        || base.graph.service_class != target.graph.service_class
        || base.graph.parent_execution != target.graph.parent_execution
    {
        operations.push(ProjectionOperation::SetGraphMetadata {
            revision: target.graph.revision,
            commit_cursor: target.graph.commit_cursor,
            objective: target.graph.objective.clone(),
            service_class: target.graph.service_class,
            parent_execution: target.graph.parent_execution.clone(),
        });
    }
    let base_nodes = base
        .graph
        .nodes
        .iter()
        .map(|node| node.node_id.as_str())
        .collect::<BTreeSet<_>>();
    let target_nodes = target
        .graph
        .nodes
        .iter()
        .map(|node| node.node_id.as_str())
        .collect::<BTreeSet<_>>();
    if base_nodes != target_nodes || base.graph.edges != target.graph.edges {
        operations.push(ProjectionOperation::ReplaceGraphTopology {
            node_ids: target
                .graph
                .nodes
                .iter()
                .map(|node| node.node_id.clone())
                .collect(),
            edges: target.graph.edges.clone(),
        });
    }
    for node_id in base_nodes.difference(&target_nodes) {
        operations.push(ProjectionOperation::RemoveGraphNode {
            node_id: (*node_id).to_string(),
        });
    }
    for node in &target.graph.nodes {
        if base
            .graph
            .nodes
            .iter()
            .find(|previous| previous.node_id == node.node_id)
            != Some(node)
        {
            operations.push(ProjectionOperation::UpsertGraphNode { node: node.clone() });
        }
    }
    for child in &base.child_executions {
        if !target
            .child_executions
            .iter()
            .any(|current| current.execution_id == child.execution_id)
        {
            operations.push(ProjectionOperation::RemoveChildExecution {
                execution_id: child.execution_id.clone(),
            });
        }
    }
    for child in &target.child_executions {
        if base
            .child_executions
            .iter()
            .find(|previous| previous.execution_id == child.execution_id)
            != Some(child)
        {
            operations.push(ProjectionOperation::UpsertChildExecution {
                child: child.clone(),
            });
        }
    }
    if base.activities != target.activities || base.activity_relations != target.activity_relations
    {
        operations.push(ProjectionOperation::ReplaceActivities {
            activities: target.activities.clone(),
            relations: target.activity_relations.clone(),
        });
    }
    if base.concurrency != target.concurrency {
        operations.push(ProjectionOperation::ReplaceConcurrency {
            concurrency: target.concurrency.clone(),
        });
    }
    if base.agentic_collaboration != target.agentic_collaboration {
        operations.push(ProjectionOperation::ReplaceAgenticCollaboration {
            collaboration: target.agentic_collaboration.clone(),
        });
    }
    if base.strategy != target.strategy {
        operations.push(ProjectionOperation::ReplaceStrategy {
            strategy: target.strategy.clone(),
        });
    }
    for (collection, previous, current) in [
        (
            ProjectionEntityCollection::Goals,
            &base.goals,
            &target.goals,
        ),
        (
            ProjectionEntityCollection::Agents,
            &base.agents,
            &target.agents,
        ),
        (
            ProjectionEntityCollection::Teams,
            &base.teams,
            &target.teams,
        ),
        (
            ProjectionEntityCollection::Relations,
            &base.relations,
            &target.relations,
        ),
        (
            ProjectionEntityCollection::Approvals,
            &base.approvals,
            &target.approvals,
        ),
        (
            ProjectionEntityCollection::Admissions,
            &base.admissions,
            &target.admissions,
        ),
        (
            ProjectionEntityCollection::Outcomes,
            &base.outcomes,
            &target.outcomes,
        ),
        (
            ProjectionEntityCollection::Interventions,
            &base.interventions,
            &target.interventions,
        ),
        (
            ProjectionEntityCollection::Usage,
            &base.usage,
            &target.usage,
        ),
        (
            ProjectionEntityCollection::Context,
            &base.context,
            &target.context,
        ),
        (
            ProjectionEntityCollection::Evidence,
            &base.evidence,
            &target.evidence,
        ),
        (
            ProjectionEntityCollection::Health,
            &base.health,
            &target.health,
        ),
        (
            ProjectionEntityCollection::Recovery,
            &base.recovery,
            &target.recovery,
        ),
    ] {
        materialize_canonical_collection(&mut operations, collection, previous, current);
    }
    if base.available_commands != target.available_commands {
        operations.push(ProjectionOperation::ReplaceAvailableCommands {
            commands: target.available_commands.clone(),
        });
    }
    if base.graph.terminal_result_ref != target.graph.terminal_result_ref
        || base.live != target.live
    {
        operations.push(ProjectionOperation::SetTerminal {
            terminal_result_ref: target.graph.terminal_result_ref.clone(),
            live: target.live.clone(),
        });
    }
    if base.delivery_envelope != target.delivery_envelope
        || base.terminal_presentation != target.terminal_presentation
        || base.cancellation_receipt != target.cancellation_receipt
    {
        operations.push(ProjectionOperation::SetDeliveryTruth {
            delivery_envelope: target.delivery_envelope.clone(),
            terminal_presentation: target.terminal_presentation.clone(),
            cancellation_receipt: target.cancellation_receipt.clone(),
        });
    }
    if base.terminal_axes != target.terminal_axes {
        operations.push(ProjectionOperation::ReplaceTerminalAxes {
            terminal_axes: target.terminal_axes.clone(),
        });
    }
    if base.cursor != target.cursor {
        operations.push(ProjectionOperation::AdvanceCursor {
            cursor: target.cursor,
        });
    }
    operations
}

fn materialize_canonical_collection(
    operations: &mut Vec<ProjectionOperation>,
    collection: ProjectionEntityCollection,
    previous: &[ProjectionEntity],
    current: &[ProjectionEntity],
) {
    let previous = previous
        .iter()
        .map(|entity| (entity.id.as_str(), entity))
        .collect::<BTreeMap<_, _>>();
    let current = current
        .iter()
        .map(|entity| (entity.id.as_str(), entity))
        .collect::<BTreeMap<_, _>>();
    for entity_key in previous.keys().filter(|key| !current.contains_key(*key)) {
        operations.push(ProjectionOperation::RemoveEntity {
            collection,
            entity_key: (*entity_key).to_string(),
        });
    }
    for (entity_key, entity) in current {
        if previous.get(entity_key).copied() != Some(entity) {
            operations.push(ProjectionOperation::UpsertEntity {
                collection,
                entity: entity.clone(),
            });
        }
    }
}
