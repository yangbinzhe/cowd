use super::reducer_support::*;
use super::*;

pub async fn snapshot(
    services: &RuntimeServices,
    execution_id: &str,
    context: &ProjectionQueryContext,
) -> Result<ExecutionProjection, RuntimeServicesError> {
    // Freeze the durable consumption boundary before reading materialized
    // stores. Concurrent commits may be replayed once, but cannot be skipped
    // by advertising a cursor newer than the state observed below.
    let cursor = *services.event_store().subscribe_commits().borrow();
    validate_context(services, context)?;
    let graph = services
        .execution_supervisor()
        .graph_projection(execution_id)
        .await?;
    let cache_key = ExecutionProjectionCacheKey::new(execution_id, graph.revision, cursor, context);
    if let Some(mut projection) = services.cached_execution_projection(&cache_key) {
        projection.live = services.execution_live(execution_id);
        return Ok(projection);
    }
    let mut projection = snapshot_with_graph(services, execution_id, context, graph).await?;
    projection.cursor = cursor;
    // A concurrent graph commit can be observed after the frozen source
    // cursor. Return the coherent snapshot, but do not cache it under an older
    // cursor; the next read/delta will reconcile against the newer revision.
    if projection.graph.commit_cursor <= cursor {
        services.cache_execution_projection(cache_key, projection.clone());
    }
    Ok(projection)
}

async fn snapshot_with_graph(
    services: &RuntimeServices,
    execution_id: &str,
    context: &ProjectionQueryContext,
    graph: harness_contract::execution_graph::ExecutionGraphProjection,
) -> Result<ExecutionProjection, RuntimeServicesError> {
    let full = context.detail_scope == ProjectionDetailScope::Full;
    let scope = ExecutionProjectionScope::load(services, execution_id, &graph, full)?;
    let session_id = scope.session_id.clone();
    validate_projection_scope(&scope, context)?;
    let activity_events = activity::execution_events(services, &scope);
    let mut health = vec![execution_health_entity(execution_id, &graph, full)];
    health.extend(activity_binding_health_entities(&activity_events, full));
    let strategy = strategy_entity(services, &scope, execution_id, full, context);
    let usage = related_event_entities(services, &scope, "usage", full, |event| {
        // Model, tool and agent node outcomes all carry canonical
        // `ExecutionUsage` in their committed node result. Exposing the
        // execution-node events here lets consumers aggregate a root graph
        // and its durable lineage without scraping session prose timelines.
        event.scope == RuntimeEventScope::Tool
            || event.scope == RuntimeEventScope::ExecutionNode
            || event.kind.contains("usage")
    });
    let context_entities = related_event_entities(services, &scope, "context", full, |event| {
        event.kind.contains("context") || event.kind.contains("memory")
    });
    let evidence = related_event_entities(services, &scope, "evidence", full, |event| {
        event.kind != crate::execution_core::OUTCOME_EVENT_KIND
            && !event.kind.starts_with("resource.admission.")
            && !is_strategy_event(&event.kind)
            && (!event.refs.is_empty() || event.kind.contains("evidence"))
    });
    let admissions = related_event_entities(services, &scope, "admission", full, |event| {
        event.kind.starts_with("resource.admission.")
    });
    let outcomes = related_event_entities(services, &scope, "outcome", full, |event| {
        event.kind == crate::execution_core::OUTCOME_EVENT_KIND
    });
    let recovery = related_event_entities(services, &scope, "recovery", full, |event| {
        event.scope == RuntimeEventScope::Recovery || event.kind.contains("recovery")
    });
    let graph = if full {
        graph
    } else {
        summary_graph_projection(graph)
    };
    let (activities, activity_relations) = activity::project_execution_activities_from_events(
        services,
        &scope,
        &graph,
        activity_events,
        full,
    );
    let delivery_envelope = graph.delivery_envelope.clone();
    let terminal_presentation = graph.terminal_presentation.clone();
    let cancellation_receipt =
        latest_cancellation_receipt(services, session_id.as_deref(), execution_id);
    let concurrency = execution_concurrency(services, &graph, &scope);
    let mut projection = ExecutionProjection {
        schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id: execution_id.to_string(),
        revision: graph.revision,
        cursor: graph.commit_cursor,
        detail_scope: context.detail_scope,
        authorization_revision: context.authorization_revision,
        redaction_revision: redaction_revision(context),
        session_id,
        mission_id: scope.mission_id.clone(),
        task_id: scope.task_id.clone(),
        turn_id: scope.turn_id.clone(),
        activities,
        activity_relations,
        strategy,
        graph,
        concurrency,
        child_executions: scope.child_executions,
        goals: scope.goals,
        agents: scope.agents,
        teams: scope.teams,
        relations: scope.relations,
        approvals: scope.approvals,
        admissions,
        outcomes,
        interventions: scope.interventions,
        usage,
        context: context_entities,
        evidence,
        health,
        recovery,
        live: services.execution_live(execution_id),
        delivery_envelope,
        terminal_presentation,
        cancellation_receipt,
        available_commands: available_commands(services, execution_id, context).await?,
    };
    if !full {
        bound_summary_projection_collections(&mut projection);
    }
    Ok(projection)
}

const SUMMARY_OBJECTIVE_CHARS: usize = 1_024;
const SUMMARY_NODE_CHARS: usize = 1_024;
const SUMMARY_WORK_CHARS: usize = 512;
const SUMMARY_FAILURE_CHARS: usize = 512;
const SUMMARY_STATE_ENTRIES: usize = 64;
pub(super) const SUMMARY_EVIDENCE_REFS: usize = 32;
const SUMMARY_ENTITY_LIMIT: usize = 128;

/// Turn an execution graph into a bounded status/topology projection.
///
/// `ProjectionDetailScope::Summary` is a transport contract, not just a hint
/// for related entities. Keeping lossless node results and acceptance
/// observations here made a single lineage poll return megabytes while the
/// same response still omitted the public Agent identities an operator needs.
/// Full semantic outputs remain addressable through their durable result and
/// evidence references; Summary retains identities, topology, lifecycle,
/// numeric usage and the bounded collaboration-market receipts used by live
/// control surfaces.
pub(super) fn summary_graph_projection(
    mut graph: harness_contract::execution_graph::ExecutionGraphProjection,
) -> harness_contract::execution_graph::ExecutionGraphProjection {
    graph.objective = bounded_summary_text(&graph.objective, SUMMARY_OBJECTIVE_CHARS);
    for node in &mut graph.nodes {
        node.acceptance = Default::default();
        node.resource_scopes.truncate(SUMMARY_EVIDENCE_REFS);
        node.evidence_refs.truncate(SUMMARY_EVIDENCE_REFS);
        node.summary = node
            .summary
            .as_deref()
            .map(|value| bounded_summary_text(value, SUMMARY_NODE_CHARS));
        if let Some(failure) = node.failure.as_mut() {
            failure.message = bounded_summary_text(&failure.message, SUMMARY_FAILURE_CHARS);
            failure.evidence_refs.truncate(SUMMARY_EVIDENCE_REFS);
        }
        node.usage.required_acceptance = Default::default();
        node.usage.observed_acceptance = Default::default();
        node.usage.acceptance_evaluation = None;
        node.usage.runtime_write_attempt_paths.clear();
        node.usage.runtime_observed_resource_scopes.clear();
        if let Some(work) = node.work.as_mut() {
            bound_work_projection(work);
        }
        if let Some(state) = node.work_state.as_mut() {
            bound_work_state(state);
        }
    }
    for item in &mut graph.autonomous_work {
        bound_work_projection(&mut item.work);
        bound_work_state(&mut item.state);
    }
    graph
}

fn bound_work_projection(work: &mut harness_contract::execution_graph::ExecutionWorkProjection) {
    work.objective = work
        .objective
        .as_deref()
        .map(|value| bounded_summary_text(value, SUMMARY_WORK_CHARS));
    work.proposal_evidence_refs.truncate(SUMMARY_EVIDENCE_REFS);
    work.input_artifact_refs.truncate(SUMMARY_EVIDENCE_REFS);
    work.output_artifact_kinds.truncate(SUMMARY_EVIDENCE_REFS);
    work.eligibility
        .allowed_agent_instance_ids
        .truncate(SUMMARY_STATE_ENTRIES);
    work.eligibility
        .allowed_role_ids
        .truncate(SUMMARY_STATE_ENTRIES);
    work.eligibility
        .required_capabilities
        .truncate(SUMMARY_STATE_ENTRIES);
    if let harness_contract::execution_graph::ExecutionWorkReviewPolicy::Peer {
        eligible_role_ids,
        ..
    } = &mut work.review_policy
    {
        eligible_role_ids.truncate(SUMMARY_STATE_ENTRIES);
    }
}

fn bound_work_state(state: &mut harness_contract::execution_graph::ExecutionWorkRuntimeState) {
    state.review_findings.truncate(SUMMARY_STATE_ENTRIES);
    for finding in &mut state.review_findings {
        *finding = bounded_summary_text(finding, SUMMARY_WORK_CHARS);
    }
    state.reviews.truncate(SUMMARY_STATE_ENTRIES);
    for review in &mut state.reviews {
        review.finding = review
            .finding
            .as_deref()
            .map(|value| bounded_summary_text(value, SUMMARY_WORK_CHARS));
    }
    state.bids.truncate(SUMMARY_STATE_ENTRIES);
    for bid in &mut state.bids {
        bid.rationale = bounded_summary_text(&bid.rationale, SUMMARY_WORK_CHARS);
    }
}

fn bounded_summary_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_string()
    } else {
        let mut bounded = value.chars().take(max_chars).collect::<String>();
        bounded.push('…');
        bounded
    }
}

fn bound_summary_projection_collections(projection: &mut ExecutionProjection) {
    for entities in [
        &mut projection.goals,
        &mut projection.approvals,
        &mut projection.admissions,
        &mut projection.outcomes,
        &mut projection.interventions,
        &mut projection.usage,
        &mut projection.context,
        &mut projection.evidence,
        &mut projection.health,
        &mut projection.recovery,
    ] {
        bound_summary_entities(entities);
    }
    // Agent, Team and relation identities are the control-plane purpose of a
    // Summary projection and therefore remain complete. Their human text and
    // public refs are still bounded independently of population size.
    for entities in [
        &mut projection.agents,
        &mut projection.teams,
        &mut projection.relations,
    ] {
        for entity in entities.iter_mut() {
            bound_summary_entity(entity);
        }
    }
}

fn bound_summary_entities(entities: &mut Vec<ProjectionEntity>) {
    for entity in entities.iter_mut() {
        bound_summary_entity(entity);
    }
    if entities.len() > SUMMARY_ENTITY_LIMIT {
        entities.sort_by(|left, right| {
            left.revision
                .cmp(&right.revision)
                .then_with(|| left.id.cmp(&right.id))
        });
        entities.drain(..entities.len() - SUMMARY_ENTITY_LIMIT);
    }
}

fn bound_summary_entity(entity: &mut ProjectionEntity) {
    entity.id = bounded_summary_text(&entity.id, 512);
    entity.kind = bounded_summary_text(&entity.kind, 128);
    entity.status = entity
        .status
        .as_deref()
        .map(|value| bounded_summary_text(value, 128));
    entity.summary = entity
        .summary
        .as_deref()
        .map(|value| bounded_summary_text(value, SUMMARY_WORK_CHARS));
    entity.evidence_refs.truncate(SUMMARY_EVIDENCE_REFS);
    for reference in &mut entity.evidence_refs {
        *reference = bounded_summary_text(reference, 512);
    }
    entity.detail = None;
}

pub(super) fn execution_concurrency(
    services: &RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    scope: &ExecutionProjectionScope,
) -> ExecutionConcurrencyProjection {
    let root = concurrency_counts(&graph.nodes);
    let mut inclusive = root.clone();
    for descendant in &scope.descendant_graphs {
        accumulate_concurrency_counts(&mut inclusive, &descendant.nodes);
    }
    let mut resources = services
        .resource_manager()
        .snapshots()
        .unwrap_or_default()
        .into_iter()
        .map(|snapshot| {
            let utilization_basis_points = if snapshot.effective_limit == 0 {
                0
            } else {
                u16::try_from(
                    snapshot
                        .active_leases
                        .saturating_mul(10_000)
                        .saturating_div(snapshot.effective_limit),
                )
                .unwrap_or(10_000)
                .min(10_000)
            };
            ExecutionResourceCapacityProjection {
                kind: format!("{:?}", snapshot.kind).to_ascii_lowercase(),
                effective_limit: snapshot.effective_limit as u64,
                active_leases: snapshot.active_leases as u64,
                queued_waiters: snapshot.queued_waiters as u64,
                utilization_basis_points,
                scope: "process_global".to_string(),
            }
        })
        .collect::<Vec<_>>();
    resources.sort_by(|left, right| left.kind.cmp(&right.kind));
    ExecutionConcurrencyProjection {
        root,
        inclusive,
        resources,
    }
}

fn concurrency_counts(
    nodes: &[harness_contract::execution_graph::ExecutionNodeProjection],
) -> ExecutionConcurrencyCountsProjection {
    let mut counts = ExecutionConcurrencyCountsProjection::default();
    accumulate_concurrency_counts(&mut counts, nodes);
    counts
}

fn accumulate_concurrency_counts(
    counts: &mut ExecutionConcurrencyCountsProjection,
    nodes: &[harness_contract::execution_graph::ExecutionNodeProjection],
) {
    counts.total = counts.total.saturating_add(nodes.len() as u64);
    for node in nodes {
        match node.status {
            ExecutionNodeStatus::Planned => counts.planned = counts.planned.saturating_add(1),
            ExecutionNodeStatus::Ready => counts.ready = counts.ready.saturating_add(1),
            ExecutionNodeStatus::Running => counts.running = counts.running.saturating_add(1),
            ExecutionNodeStatus::WaitingInput => {
                counts.waiting_input = counts.waiting_input.saturating_add(1);
            }
            ExecutionNodeStatus::WaitingApproval => {
                counts.waiting_approval = counts.waiting_approval.saturating_add(1);
            }
            ExecutionNodeStatus::WaitingExternal => {
                counts.waiting_external = counts.waiting_external.saturating_add(1);
            }
            ExecutionNodeStatus::Paused => counts.paused = counts.paused.saturating_add(1),
            ExecutionNodeStatus::Blocked => counts.blocked = counts.blocked.saturating_add(1),
            status if status.is_terminal() => {
                counts.terminal = counts.terminal.saturating_add(1);
            }
            ExecutionNodeStatus::Failed
            | ExecutionNodeStatus::Completed
            | ExecutionNodeStatus::Cancelled => unreachable!("terminal status matched above"),
        }
    }
}

fn latest_cancellation_receipt(
    services: &RuntimeServices,
    session_id: Option<&str>,
    execution_id: &str,
) -> Option<harness_contract::turn::CancellationReceipt> {
    let session_id = session_id?.trim();
    if session_id.is_empty() || execution_id.trim().is_empty() {
        return None;
    }
    const PAGE_SIZE: usize = 256;
    let reader = services.event_reader();
    let mut after = None;
    let mut latest = None;
    loop {
        let page = reader
            .session_timeline_events(session_id, after, PAGE_SIZE)
            .ok()?;
        if page.is_empty() {
            break;
        }
        for event in &page {
            if event.kind != "session.cancellation_committed"
                || event
                    .payload
                    .get("execution_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(execution_id)
            {
                continue;
            }
            let mut receipt =
                serde_json::from_value::<harness_contract::turn::CancellationReceipt>(
                    event.payload.clone(),
                )
                .ok()?;
            receipt.journal_sequence = event.commit_cursor;
            receipt.projection_revision = event.sequence;
            latest = Some(receipt);
        }
        after = page
            .last()
            .map(|event| (event.commit_cursor, event.transaction_index));
        if page.len() < PAGE_SIZE {
            break;
        }
    }
    latest
}

pub(super) fn strategy_entity(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    root_execution_id: &str,
    full: bool,
    context: &ProjectionQueryContext,
) -> Option<StrategyDecisionProjection> {
    let session_id = scope.session_id.as_deref()?;
    let mut events = [
        "runtime.strategy.selected",
        "runtime.strategy.downgraded",
        "runtime.strategy.early_stopped",
        "runtime.strategy.outcome",
    ]
    .into_iter()
    .flat_map(|kind| activity::events_for_root_execution_kind(services, root_execution_id, kind))
    .collect::<Vec<_>>();
    events.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
    let selected = events.iter().rev().find(|event| {
        event.kind == "runtime.strategy.selected"
            && strategy_scope(event).is_some_and(|candidate| {
                candidate.execution_id == root_execution_id && candidate.session_id == session_id
            })
    })?;
    let key = strategy_scope(selected)?;
    let mut decision_events = events
        .iter()
        .filter(|event| {
            is_strategy_event(&event.kind) && strategy_scope(event).as_ref() == Some(&key)
        })
        .collect::<Vec<_>>();
    decision_events.sort_by_key(|event| event.sequence);
    let mut accepted_event: Option<&crate::DurableRuntimeEvent> = None;
    decision_events.retain(|event| {
        let revision = strategy_revision(event);
        if let Some(previous) = accepted_event {
            let accepted_revision = strategy_revision(previous);
            if revision < accepted_revision {
                return false;
            }
            if revision == accepted_revision {
                if !strategy_events_semantically_identical(previous, event) {
                    tracing::warn!(
                        decision_id = %key.decision_id,
                        revision,
                        accepted_event_id = %previous.event_id,
                        conflicting_event_id = %event.event_id,
                        "ignoring conflicting equal-revision strategy event"
                    );
                }
                return false;
            }
        }
        accepted_event = Some(event);
        true
    });
    let latest = decision_events.last().copied()?;
    let revision = strategy_revision(latest);

    let selected_candidate = payload_value::<ExecutionCandidateKind>(latest, "selected_candidate")
        .or_else(|| payload_value(selected, "selected_candidate"));
    if selected_candidate.is_none() {
        return None;
    }
    let policy_version = latest
        .payload
        .get("policy_version")
        .or_else(|| selected.payload.get("policy_version"))
        .and_then(serde_json::Value::as_str)
        .map(|value| safe_public_text(value, 96))
        .filter(|value| !value.is_empty())?;
    let pattern = latest
        .payload
        .get("selected_pattern")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_execution_pattern)
        .or_else(|| {
            selected
                .payload
                .get("selected_pattern")
                .and_then(serde_json::Value::as_str)
                .and_then(parse_execution_pattern)
        });
    let mut candidate_estimates =
        payload_value::<Vec<ExecutionCandidateEstimate>>(latest, "candidate_estimates")
            .or_else(|| payload_value(selected, "candidate_estimates"))
            .unwrap_or_default();
    sanitize_candidate_estimates(&mut candidate_estimates);
    let estimated = selected_candidate.and_then(|candidate| {
        candidate_estimates
            .iter()
            .find(|estimate| estimate.candidate == candidate)
            .cloned()
    });
    let resource_snapshot = payload_value::<StrategyResourceSnapshot>(latest, "resource_snapshot")
        .or_else(|| payload_value(selected, "resource_snapshot"))
        .map(sanitize_resource_snapshot);
    let selection_reasons = latest
        .payload
        .get("selection_reasons")
        .or_else(|| selected.payload.get("selection_reasons"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(|reason| safe_public_text(reason, 240))
        .filter(|reason| !reason.is_empty())
        .collect::<Vec<_>>();
    let (benefit_reasons, cost_reasons) = strategy_public_reasons(
        &selection_reasons,
        selected_candidate,
        estimated.as_ref(),
        &candidate_estimates,
    );
    let evidence_scopes = decision_events
        .iter()
        .rev()
        .find_map(|event| {
            payload_value::<Vec<FocusPartitionPlan>>(event, "evidence_scopes")
                .filter(|plans| !plans.is_empty())
        })
        .map(|plans| crop_strategy_evidence_scopes(plans, context))
        .unwrap_or_default();
    let mut evidence_refs = evidence_scopes
        .iter()
        .flat_map(|scope| scope.capability_cropped_refs.iter().cloned())
        .collect::<Vec<_>>();
    evidence_refs.sort();
    evidence_refs.dedup();
    let downgrades = strategy_transitions(&decision_events, "runtime.strategy.downgraded");
    let early_stops = strategy_transitions(&decision_events, "runtime.strategy.early_stopped");
    let actual = decision_events
        .iter()
        .rev()
        .find(|event| event.kind == "runtime.strategy.outcome")
        .and_then(|event| event.payload.get("outcome"))
        .filter(|outcome| !outcome.is_null())
        .cloned()
        .and_then(|value| serde_json::from_value::<crate::TurnStrategyActualOutcome>(value).ok())
        .map(|outcome| strategy_actual_projection(outcome, context));
    let latest_receipt = decision_events.iter().rev().find_map(|event| {
        event
            .payload
            .get("collaboration_receipt")
            .filter(|receipt| !receipt.is_null())
    });
    let receipt_team_id = latest_receipt
        .and_then(|receipt| {
            receipt
                .get("team_id")
                .or_else(|| receipt.pointer("/evidence/team_id"))
        })
        .and_then(serde_json::Value::as_str)
        .and_then(safe_public_ref);
    let receipt_team_execution_id = latest_receipt
        .and_then(|receipt| {
            receipt
                .pointer("/execution/graph_id")
                .or_else(|| receipt.pointer("/evidence/graph_id"))
        })
        .and_then(serde_json::Value::as_str)
        .and_then(safe_public_ref);
    // A Team graph becomes durable before its terminal collaboration receipt
    // is available and remains evidence after a downgrade. Preserve that
    // authoritative topology independently of the latest fallback candidate.
    let live_team = live_team_topology(scope);
    let team_id =
        receipt_team_id.or_else(|| live_team.as_ref().map(|(team_id, _)| team_id.clone()));
    let team_execution_id =
        receipt_team_execution_id.or_else(|| live_team.map(|(_, execution_id)| execution_id));
    let source = payload_value::<StrategyDecisionSource>(latest, "decision_source")
        .or_else(|| payload_value(selected, "decision_source"));
    let confidence = latest
        .payload
        .get("confidence")
        .or_else(|| selected.payload.get("confidence"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .map(|value| value.min(100));
    let proof_status = Some(
        if estimated.as_ref().is_some_and(|estimate| {
            estimate.duration_provenance == MeasureProvenance::Calibrated
                && estimate.duration_sample_count >= 3
                && estimate.quality_provenance == MeasureProvenance::Calibrated
                && estimate.quality_sample_count >= 3
                && estimate
                    .duration_calibration_source
                    .starts_with("strategy-experience-store:paired-and-absolute-cost")
                && estimate
                    .quality_calibration_source
                    .starts_with("strategy-experience-store:paired-quality-lift")
        }) {
            StrategyProofStatus::Calibrated
        } else {
            StrategyProofStatus::NotProven
        },
    );
    let status = if actual.is_none() && latest.kind == "runtime.strategy.selected" {
        "running".to_string()
    } else {
        latest
            .status
            .clone()
            .unwrap_or_else(|| "running".to_string())
    };
    let safe_detail = full.then(|| {
        serde_json::json!({
            "decision_id": key.decision_id.clone(),
            "execution_id": key.execution_id.clone(),
            "session_id": key.session_id.clone(),
            "turn_id": key.turn_id.clone(),
            "selected_candidate": selected_candidate,
            "pattern": pattern,
            "policy_version": policy_version.clone(),
            "proof_status": proof_status,
            "actual_status": if actual.is_some() {
                StrategyActualStatus::Observed
            } else {
                StrategyActualStatus::Unknown
            },
        })
    });
    Some(StrategyDecisionProjection {
        schema_version: STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION,
        id: key.decision_id.clone(),
        kind: "strategy_decision".to_string(),
        revision,
        status: Some(status),
        summary: Some(latest.kind.clone()),
        evidence_refs,
        detail: safe_detail,
        decision_id: Some(key.decision_id),
        execution_id: Some(key.execution_id),
        session_id: Some(key.session_id),
        turn_id: Some(key.turn_id),
        selected_candidate,
        pattern,
        candidate_estimates,
        benefit_reasons,
        cost_reasons,
        evidence_scopes,
        downgrades,
        early_stops,
        estimated,
        actual_status: Some(if actual.is_some() {
            StrategyActualStatus::Observed
        } else {
            StrategyActualStatus::Unknown
        }),
        actual,
        resource_snapshot,
        policy_version: Some(policy_version),
        source,
        confidence,
        proof_status,
        team_id,
        team_execution_id,
    })
}

pub(super) fn live_team_topology(scope: &ExecutionProjectionScope) -> Option<(String, String)> {
    scope.teams.iter().find_map(|team| {
        let graph_id = team
            .detail
            .as_ref()
            .and_then(|detail| detail.get("graph_id"))
            .and_then(serde_json::Value::as_str)
            .and_then(safe_public_ref)?;
        scope
            .child_executions
            .iter()
            .any(|child| child.execution_id == graph_id)
            .then(|| (team.id.clone(), graph_id))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StrategyProjectionScope {
    execution_id: String,
    session_id: String,
    turn_id: String,
    decision_id: String,
}

pub(super) fn is_strategy_event(kind: &str) -> bool {
    matches!(
        kind,
        "runtime.strategy.selected"
            | "runtime.strategy.downgraded"
            | "runtime.strategy.early_stopped"
            | "runtime.strategy.outcome"
    )
}

pub(super) fn strategy_scope(
    event: &crate::DurableRuntimeEvent,
) -> Option<StrategyProjectionScope> {
    Some(StrategyProjectionScope {
        execution_id: event
            .payload
            .get("execution_graph_ref")?
            .as_str()
            .and_then(safe_public_ref)?,
        session_id: event
            .payload
            .get("session_ref")?
            .as_str()
            .and_then(safe_public_ref)?,
        turn_id: event
            .payload
            .get("turn_ref")?
            .as_str()
            .and_then(safe_public_ref)?,
        decision_id: event
            .payload
            .get("decision_id")?
            .as_str()
            .and_then(safe_public_ref)?,
    })
}

pub(super) fn strategy_revision(event: &crate::DurableRuntimeEvent) -> u64 {
    event
        .payload
        .get("decision_revision")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

pub(super) fn strategy_events_semantically_identical(
    left: &crate::DurableRuntimeEvent,
    right: &crate::DurableRuntimeEvent,
) -> bool {
    left.scope == right.scope
        && left.kind == right.kind
        && left.status == right.status
        && left.actor == right.actor
        && left.refs == right.refs
        && left.payload == right.payload
}

pub(super) fn payload_value<T: serde::de::DeserializeOwned>(
    event: &crate::DurableRuntimeEvent,
    key: &str,
) -> Option<T> {
    event
        .payload
        .get(key)
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(super) fn sanitize_candidate_estimates(estimates: &mut [ExecutionCandidateEstimate]) {
    for estimate in estimates {
        estimate.duration_calibration_source =
            safe_public_text(&estimate.duration_calibration_source, 160);
        estimate.quality_calibration_source =
            safe_public_text(&estimate.quality_calibration_source, 160);
        estimate.evidence_overlap_penalty_bp = estimate.evidence_overlap_penalty_bp.min(10_000);
        estimate.provider_concurrency_penalty_bp =
            estimate.provider_concurrency_penalty_bp.min(10_000);
        estimate.risk_approval_penalty_bp = estimate.risk_approval_penalty_bp.min(10_000);
        estimate.reasons = estimate
            .reasons
            .iter()
            .map(|reason| safe_public_text(reason, 240))
            .filter(|reason| !reason.is_empty())
            .collect();
    }
}

pub(super) fn sanitize_resource_snapshot(
    mut snapshot: StrategyResourceSnapshot,
) -> StrategyResourceSnapshot {
    snapshot.version = safe_public_text(&snapshot.version, 96);
    snapshot.sample_source = safe_public_text(&snapshot.sample_source, 160);
    snapshot.provider_concurrency_penalty_bp = snapshot.provider_concurrency_penalty_bp.min(10_000);
    snapshot.provider_failure_timeout_upper_bound_bp =
        snapshot.provider_failure_timeout_upper_bound_bp.min(10_000);
    snapshot.provider_profile_fingerprint.clear();
    snapshot
}

pub(super) fn parse_execution_pattern(
    value: &str,
) -> Option<harness_contract::core::ExecutionPattern> {
    use harness_contract::core::ExecutionPattern;
    match value {
        "direct" => Some(ExecutionPattern::Direct),
        "explore" => Some(ExecutionPattern::Explore),
        "execute" => Some(ExecutionPattern::Execute),
        "deliberate" => Some(ExecutionPattern::Deliberate),
        "collaborate" => Some(ExecutionPattern::Collaborate),
        "supervise" => Some(ExecutionPattern::Supervise),
        _ => None,
    }
}

pub(super) fn strategy_public_reasons(
    selection_reasons: &[String],
    selected_candidate: Option<ExecutionCandidateKind>,
    estimated: Option<&ExecutionCandidateEstimate>,
    candidate_estimates: &[ExecutionCandidateEstimate],
) -> (Vec<String>, Vec<String>) {
    let (selection_cost_warnings, selection_benefits) = selection_reasons
        .iter()
        .map(|reason| safe_public_text(reason, 240))
        .filter(|reason| !reason.is_empty())
        .partition::<Vec<_>, _>(|reason| strategy_reason_is_cost_warning(reason));
    let mut benefit = selection_benefits;
    let mut cost = selection_cost_warnings;
    if let Some(estimate) = estimated {
        if let Some((selected_candidate, alternative)) = selected_candidate.zip(
            candidate_estimates
                .iter()
                .filter(|candidate| {
                    candidate.eligible && Some(candidate.candidate) != selected_candidate
                })
                .min_by_key(|candidate| {
                    (
                        candidate.effective_duration_ms(),
                        candidate.context_duplication_tokens,
                        candidate.candidate,
                    )
                }),
        ) {
            benefit.push(format!(
                "selected {} effective duration {} ms versus eligible alternative {} at {} ms",
                selected_candidate.as_str(),
                estimate.effective_duration_ms(),
                alternative.candidate.as_str(),
                alternative.effective_duration_ms()
            ));
        }
        if estimate.estimated_serial_ms > estimate.estimated_critical_path_ms {
            benefit.push(format!(
                "estimated critical path is {} ms below the {} ms serial baseline",
                estimate
                    .estimated_serial_ms
                    .saturating_sub(estimate.estimated_critical_path_ms),
                estimate.estimated_serial_ms
            ));
        }
        if estimate.expected_quality_lift_bp > 0 {
            benefit.push(format!(
                "expected quality lift is {} basis points",
                estimate.expected_quality_lift_bp
            ));
        }
        for (label, value, unit) in [
            ("startup overhead", estimate.startup_overhead_ms, "ms"),
            (
                "context duplication",
                estimate.context_duplication_tokens,
                "tokens",
            ),
            ("merge cost", estimate.merge_cost_ms, "ms"),
            (
                "evidence overlap penalty",
                u64::from(estimate.evidence_overlap_penalty_bp),
                "bp",
            ),
            (
                "provider concurrency penalty",
                u64::from(estimate.provider_concurrency_penalty_bp),
                "bp",
            ),
            (
                "risk approval penalty",
                u64::from(estimate.risk_approval_penalty_bp),
                "bp",
            ),
        ] {
            if value > 0 {
                cost.push(format!("{label} is {value} {unit}"));
            }
        }
    }
    benefit.sort();
    benefit.dedup();
    cost.sort();
    cost.dedup();
    (benefit, cost)
}

/// A strategy can be selected because it was explicitly requested even when
/// its predicted incremental value is negative.  That is not a benefit: it is
/// a required, user-visible cost warning and must reach the dedicated
/// `cost_reasons` surface field rather than being hidden among rationale.
pub(super) fn strategy_reason_is_cost_warning(reason: &str) -> bool {
    let normalized = reason.to_ascii_lowercase();
    normalized.contains("negative estimated lift")
        || normalized.contains("no measured duration advantage or paired quality proof")
        || normalized.contains("surface must show the cost warning")
}

pub(super) fn crop_strategy_evidence_scopes(
    plans: Vec<FocusPartitionPlan>,
    context: &ProjectionQueryContext,
) -> Vec<StrategyEvidenceScopeProjection> {
    let mut scopes = plans
        .into_iter()
        .flat_map(|plan| {
            plan.slots.into_iter().map(move |slot| {
                let mut refs = slot
                    .capability_cropped_refs
                    .iter()
                    .filter_map(|reference| {
                        safe_public_ref(reference)
                            .filter(|reference| strategy_ref_visible(reference, context, None))
                    })
                    .collect::<Vec<_>>();
                refs.sort();
                refs.dedup();
                StrategyEvidenceScopeProjection {
                    role_id: safe_public_text(&plan.role_id, 96),
                    focus_id: safe_public_text(&slot.focus_id, 96),
                    responsibility_summary: safe_public_text(&slot.evidence_responsibility, 200),
                    capability_cropped_refs: refs,
                    scope_hash: safe_public_text(&slot.scope_hash, 96),
                    overlap_budget_bp: slot.overlap_budget_bp.min(10_000),
                    novelty_target_bp: slot.novelty_target_bp.min(10_000),
                }
            })
        })
        .collect::<Vec<_>>();
    scopes.sort_by(|left, right| {
        left.role_id
            .cmp(&right.role_id)
            .then_with(|| left.focus_id.cmp(&right.focus_id))
    });
    scopes
}

pub(super) fn strategy_transitions(
    events: &[&crate::DurableRuntimeEvent],
    kind: &str,
) -> Vec<StrategyTransitionProjection> {
    events
        .iter()
        .filter(|event| event.kind == kind)
        .map(|event| StrategyTransitionProjection {
            revision: strategy_revision(event),
            kind: event.kind.clone(),
            status: event
                .status
                .clone()
                .unwrap_or_else(|| "recorded".to_string()),
            summary: event
                .payload
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(|reason| safe_public_text(reason, 240))
                .unwrap_or_else(|| "Runtime strategy policy updated".to_string()),
        })
        .collect()
}

pub(super) fn strategy_actual_projection(
    outcome: crate::TurnStrategyActualOutcome,
    context: &ProjectionQueryContext,
) -> StrategyActualProjection {
    StrategyActualProjection {
        duration_ms: outcome.duration_ms,
        input_tokens: outcome.input_tokens,
        output_tokens: outcome.output_tokens,
        cached_tokens: outcome.cached_tokens,
        tool_calls: outcome.tool_calls,
        duplicate_tool_calls: outcome.duplicate_tool_calls,
        max_tool_concurrency_observed: outcome.max_tool_concurrency_observed,
        parallel_tool_batches: outcome.parallel_tool_batches,
        write_attempt_refs: outcome
            .write_attempt_paths
            .iter()
            .filter_map(|path| {
                safe_public_ref(path)
                    .filter(|reference| strategy_ref_visible(reference, context, Some("write")))
            })
            .collect(),
        evidence_overlap_bp: outcome.evidence_overlap_bp.min(10_000),
        evidence_overlap_observed: outcome.evidence_overlap_observed,
        working_state_verified: outcome.working_state_verified,
        merge_cost_ms: outcome.merge_cost_ms,
        parent_merge_count: outcome.parent_merge_count,
        evaluation_token_limit: outcome.evaluation_token_limit,
        evaluation_tokens_consumed: outcome.evaluation_tokens_consumed,
        evaluation_budget_observed: outcome.evaluation_budget_observed,
        evaluation_budget_breached: outcome.evaluation_budget_breached,
        quality_score_bp: outcome.quality_score_bp.map(|value| value.min(10_000)),
        actual_speedup_ratio_bp: outcome.actual_speedup_ratio_bp,
        terminal_reason: safe_public_text(&outcome.terminal_reason, 240),
    }
}

pub(super) fn safe_public_ref(value: &str) -> Option<String> {
    let value = value.trim();
    let lower = value.to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 256
        || value.chars().any(char::is_whitespace)
        || value.starts_with('/')
        || value.starts_with('\\')
        || value.contains("../")
        || value.contains("..\\")
        || lower.contains("/home/")
        || lower.contains("/media/")
        || lower.contains("/tmp/")
        || lower.starts_with("file:")
        || contains_absolute_path(value)
        || value
            .as_bytes()
            .get(1..3)
            .is_some_and(|bytes| bytes == b":\\" || bytes == b":/")
    {
        return None;
    }
    Some(value.to_string())
}

pub(super) fn strategy_ref_visible(
    reference: &str,
    context: &ProjectionQueryContext,
    required_mode: Option<&str>,
) -> bool {
    let (_, raw_path) = reference
        .split_once(':')
        .filter(|(mode, _)| matches!(*mode, "read" | "write" | "worktree"))
        .map_or((required_mode, reference), |(mode, path)| {
            (Some(mode), path)
        });
    if !safe_workspace_relative_path(raw_path) {
        return false;
    }
    if context
        .visibility_grants
        .iter()
        .any(|grant| grant == "resource:*")
    {
        return true;
    }
    let (reference_mode, reference_path) = reference
        .split_once(':')
        .filter(|(mode, _)| matches!(*mode, "read" | "write" | "worktree"))
        .map_or((required_mode, reference), |(mode, path)| {
            (Some(mode), path)
        });
    let Some(reference_mode) = reference_mode else {
        // Opaque execution-local evidence identifiers are safe once the
        // execution/session/mission authorization predicate has passed.
        return true;
    };
    context.visibility_grants.iter().any(|grant| {
        let Some((grant_mode, grant_path)) = grant.split_once(':') else {
            return false;
        };
        if grant_mode != reference_mode {
            return false;
        }
        grant_path == "."
            || grant_path == reference_path
            || reference_path
                .strip_prefix(grant_path.trim_end_matches('/'))
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

pub(super) fn safe_workspace_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.starts_with('\\')
        && !path.contains("../")
        && !path.contains("..\\")
        && !contains_absolute_path(path)
        && !path
            .as_bytes()
            .get(1..3)
            .is_some_and(|bytes| bytes == b":\\" || bytes == b":/")
}

pub(super) fn safe_public_text(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = normalized.to_ascii_lowercase();
    if lower.contains("/home/")
        || lower.contains("/media/")
        || lower.contains("/tmp/")
        || lower.contains("../")
        || lower.contains("..\\")
        || lower.contains("prompt")
        || lower.contains("chain of thought")
        || lower.contains("reasoning")
        || lower.contains("hidden")
        || contains_absolute_path(&normalized)
    {
        return "redacted by strategy projection policy".to_string();
    }
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized.chars().take(max_chars).collect::<String>() + "…"
    }
}

pub(super) fn contains_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    if value.to_ascii_lowercase().contains("file:") {
        return true;
    }
    bytes.iter().enumerate().any(|(index, byte)| {
        let previous = index.checked_sub(1).and_then(|offset| bytes.get(offset));
        let boundary = previous.is_none_or(|value| {
            value.is_ascii_whitespace()
                || matches!(
                    *value,
                    b'(' | b'['
                        | b'{'
                        | b':'
                        | b'='
                        | b','
                        | b'\''
                        | b'"'
                        | b'`'
                        | b'>'
                        | b'<'
                        | b';'
                        | b'|'
                        | b'&'
                        | b'-'
                        | b'_'
                )
        });
        if *byte == b'/' {
            return boundary && bytes.get(index + 1).is_some_and(|next| *next != b'/');
        }
        byte.is_ascii_alphabetic()
            && bytes.get(index + 1) == Some(&b':')
            && bytes
                .get(index + 2)
                .is_some_and(|next| matches!(*next, b'/' | b'\\'))
            && boundary
    })
}

pub(super) fn related_event_entities(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    kind: &str,
    full: bool,
    predicate: impl Fn(&crate::DurableRuntimeEvent) -> bool,
) -> Vec<ProjectionEntity> {
    let mut entities = BTreeMap::<String, ProjectionEntity>::new();
    for entity in super::activity::execution_events(services, scope)
        .into_iter()
        .filter(|event| scope.contains_event(event) && predicate(event))
        .map(|event| entity_from_runtime_event(kind, event, full))
    {
        let replace = entities
            .get(&entity.id)
            .is_none_or(|current| current.revision <= entity.revision);
        if replace {
            entities.insert(entity.id.clone(), entity);
        }
    }
    entities.into_values().collect()
}

pub(super) fn entity_from_runtime_event(
    kind: &str,
    event: crate::DurableRuntimeEvent,
    full: bool,
) -> ProjectionEntity {
    let strategy_event = is_strategy_event(&event.kind);
    // Unstructured event payloads can contain paths, prompts, tool output, or
    // provider details. Public projections expose typed payloads and cropped
    // references; raw evidence remains behind its dedicated authority.
    let detail = if full && strategy_event {
        Some(safe_strategy_event_detail(&event))
    } else if full && event.kind == "model.item_completed" {
        // This event contains only provider-visible text, public reasoning
        // summaries, or a governed tool call. Private reasoning is never
        // persisted by ModelStreamReducer, so an authorized full projection
        // may replay the causal activity without exposing hidden CoT.
        Some(event.payload.clone())
    } else {
        None
    };
    let payload = projection_entity_payload(&event);
    let id = stable_event_entity_id(&event, payload.as_ref());
    let public_kind = match payload.as_ref() {
        Some(ProjectionEntityPayload::Admission(_)) => "admission",
        Some(ProjectionEntityPayload::Outcome(_)) => "outcome",
        Some(ProjectionEntityPayload::Evidence(_)) => "evidence",
        None => kind,
    };
    ProjectionEntity {
        id,
        kind: public_kind.to_string(),
        revision: event.sequence,
        status: event.status,
        summary: Some(event.kind),
        evidence_refs: event
            .refs
            .into_iter()
            .filter_map(|reference| safe_public_ref(&reference.id))
            .collect(),
        payload,
        detail,
    }
}

pub(super) fn stable_event_entity_id(
    event: &crate::DurableRuntimeEvent,
    payload: Option<&ProjectionEntityPayload>,
) -> String {
    match payload {
        Some(ProjectionEntityPayload::Admission(admission)) => {
            format!("admission:{}", admission.request_id)
        }
        Some(ProjectionEntityPayload::Outcome(outcome)) => {
            format!("outcome:{}", outcome.execution_id)
        }
        Some(ProjectionEntityPayload::Evidence(evidence)) => {
            format!("evidence:{}", evidence.evidence_ref.id)
        }
        None => event.event_id.clone(),
    }
}

pub(super) fn projection_entity_payload(
    event: &crate::DurableRuntimeEvent,
) -> Option<ProjectionEntityPayload> {
    if event.kind.starts_with("resource.admission.") {
        let observation =
            serde_json::from_value::<ResourceAdmissionObservation>(event.payload.clone()).ok()?;
        let status = match observation.status {
            ResourceAdmissionObservationStatus::Queued => AdmissionProjectionStatus::Queued,
            ResourceAdmissionObservationStatus::Waiting => {
                if observation.wait_reason == Some(ResourceWaitReason::ScopeInfeasible) {
                    AdmissionProjectionStatus::WaitingScope
                } else {
                    AdmissionProjectionStatus::WaitingResource
                }
            }
            ResourceAdmissionObservationStatus::Granted => AdmissionProjectionStatus::Materialized,
            ResourceAdmissionObservationStatus::Deferred
            | ResourceAdmissionObservationStatus::Overloaded => AdmissionProjectionStatus::Blocked,
        };
        return Some(ProjectionEntityPayload::Admission(AdmissionProjection {
            request_id: observation.request_id.to_string(),
            status,
            requested_service_class: serialized_label(&observation.requested_service_class),
            resolved_service_class: serialized_label(&observation.resolved_service_class),
            requested_priority: observation.requested_priority,
            deadline_at_ms: observation.deadline_at_ms,
            queue_age_ms: observation.queue_age_ms,
            wait_reason: observation
                .wait_reason
                .map(|reason| serialized_label(&reason)),
            blocker: observation.blocker.map(|value| value.to_string()),
            resource_demands: observation
                .demands
                .into_iter()
                .map(|(kind, amount)| format!("{}:{amount}", serialized_label(&kind)))
                .collect(),
            normalized_scope: observation.normalized_scope,
            accepted_at_ms: observation
                .enqueued_at_ms
                .unwrap_or(observation.observed_at_ms),
            policy_revision: observation.policy_revision,
            refs: event
                .refs
                .iter()
                .map(|reference| format!("{}:{}", reference.kind, reference.id))
                .collect(),
        }));
    }
    if event.kind == crate::execution_core::OUTCOME_EVENT_KIND {
        let outcome = serde_json::from_value::<harness_contract::outcome::ExecutionOutcome>(
            event.payload.clone(),
        )
        .ok()?;
        let provider = outcome.provider.as_ref();
        let quality = match outcome.quality {
            harness_contract::outcome::OutcomeQuality::Unknown => OutcomeQualityProjection::Unknown,
            harness_contract::outcome::OutcomeQuality::Estimate { .. } => {
                OutcomeQualityProjection::Estimated
            }
        };
        return Some(ProjectionEntityPayload::Outcome(OutcomeProjection {
            execution_id: outcome.identity.execution_id,
            session_id: outcome.identity.session_id,
            turn_id: outcome.identity.turn_id,
            task_id: outcome.identity.task_id,
            mission_id: outcome.identity.mission_id,
            agent_id: outcome.identity.agent_id,
            team_id: outcome.identity.team_id,
            execution_graph_ref: outcome.identity.execution_graph_ref,
            provider: provider.map(|value| value.provider_name.clone()),
            model: provider.map(|value| value.model.clone()),
            profile: provider.and_then(|value| value.profile.clone()),
            protocol: provider.and_then(|value| value.protocol.clone()),
            config_revision: outcome.runtime.config_revision,
            strategy_revision: outcome.strategy.policy_revision,
            terminal_class: outcome.terminal.class_name().to_string(),
            duration_ms: outcome.timing.duration_ms,
            input_tokens: outcome.usage.input_tokens,
            output_tokens: outcome.usage.output_tokens,
            cached_tokens: outcome.usage.cached_tokens,
            tool_calls: outcome.usage.tool_calls,
            duplicate_tool_calls: outcome.usage.duplicate_tool_calls,
            retries: outcome.usage.retries,
            quality,
            evidence_completeness: outcome.evidence_completeness,
            freshness_ms: outcome.observation.freshness_ms,
            evidence_refs: outcome.evidence_refs,
        }));
    }
    evidence_ref_from_event(event).map(|evidence_ref| {
        ProjectionEntityPayload::Evidence(EvidenceProjection {
            evidence_ref,
            support: event
                .payload
                .get("support")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            completeness: event
                .payload
                .get("completeness")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or(EvidenceCompleteness::None),
            freshness_ms: event
                .payload
                .get("freshness_ms")
                .and_then(serde_json::Value::as_u64),
            projector_lag_commits: event
                .payload
                .get("projector_lag_commits")
                .and_then(serde_json::Value::as_u64),
        })
    })
}

fn serialized_label<T>(value: &T) -> String
where
    T: serde::Serialize + std::fmt::Debug,
{
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(label)) => label,
        Ok(value) => value.to_string(),
        Err(_) => format!("{value:?}").to_ascii_lowercase(),
    }
}

pub(super) fn evidence_ref_from_event(event: &crate::DurableRuntimeEvent) -> Option<EvidenceRef> {
    for key in ["evidence_ref", "evidence"] {
        if let Some(reference) = event
            .payload
            .get(key)
            .cloned()
            .and_then(|value| serde_json::from_value::<EvidenceRef>(value).ok())
        {
            return Some(reference);
        }
    }
    event.refs.first().map(|reference| EvidenceRef {
        ref_type: reference.kind.clone(),
        id: reference.id.clone(),
        source: event.actor.clone(),
        boundary: RealityBoundary::Unknown,
        confidence_bp: None,
    })
}

pub(super) fn safe_strategy_event_detail(event: &crate::DurableRuntimeEvent) -> serde_json::Value {
    serde_json::json!({
        "decision_id": safe_event_payload_ref(event, "decision_id"),
        "decision_revision": strategy_revision(event),
        "execution_id": safe_event_payload_ref(event, "execution_graph_ref"),
        "session_id": safe_event_payload_ref(event, "session_ref"),
        "turn_id": safe_event_payload_ref(event, "turn_ref"),
        "selected_candidate": event.payload.get("selected_candidate"),
        "selected_pattern": event.payload.get("selected_pattern"),
        "status": event.status.as_deref(),
        "policy_version": event
            .payload
            .get("policy_version")
            .and_then(serde_json::Value::as_str)
            .map(|value| safe_public_text(value, 96)),
    })
}

pub(super) fn safe_event_payload_ref(
    event: &crate::DurableRuntimeEvent,
    key: &str,
) -> Option<String> {
    event
        .payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .and_then(safe_public_ref)
}
