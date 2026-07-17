//! Canonical read and command model for live execution state.
//!
//! This module owns no durable state. It translates the existing graph, goal,
//! agent, team, relation, approval, context and V3 event stores into the one
//! public contract exposed by `harness-contract::projection`.

use std::collections::BTreeSet;

use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeStatus};
use harness_contract::projection::{
    ChildExecutionProjection, ExecutionCommandKind, ExecutionCommandReceipt,
    ExecutionCommandRequest, ExecutionProjection, ProjectionCommandAvailability, ProjectionDelta,
    ProjectionDetailScope, ProjectionEntity, ProjectionEvent, ProjectionEventKind,
    ProjectionQueryContext, EXECUTION_PROJECTION_SCHEMA_VERSION,
};

use crate::{ExecutionGraphHost, RuntimeEventScope, RuntimeServices, RuntimeServicesError};

const MAX_DELTA_BATCHES: usize = 256;

pub async fn snapshot(
    services: &RuntimeServices,
    execution_id: &str,
    context: &ProjectionQueryContext,
) -> Result<ExecutionProjection, RuntimeServicesError> {
    validate_context(services, context)?;
    let graph = services
        .graph_runner()
        .graph_projection(execution_id)
        .await?;
    let full = context.detail_scope == ProjectionDetailScope::Full;
    let scope = ExecutionProjectionScope::load(services, execution_id, &graph, full)?;
    let session_id = scope.session_id.clone();
    validate_session_scope(session_id.as_deref(), context)?;
    validate_mission_scope(scope.mission_id.as_deref(), context)?;
    let health = vec![ProjectionEntity {
        id: format!("execution-health:{execution_id}"),
        kind: "execution_health".to_string(),
        revision: graph.revision,
        status: Some(graph_status(&graph.nodes)),
        summary: Some("derived from canonical execution graph state".to_string()),
        evidence_refs: Vec::new(),
        detail: full.then(|| {
            serde_json::json!({
                "commit_cursor": graph.commit_cursor,
                "terminal_result_ref": graph.terminal_result_ref,
            })
        }),
    }];
    let strategy = strategy_entity(services, &scope, execution_id, full);
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
        !event.refs.is_empty() || event.kind.contains("evidence")
    });
    let recovery = related_event_entities(services, &scope, "recovery", full, |event| {
        event.scope == RuntimeEventScope::Recovery || event.kind.contains("recovery")
    });

    Ok(ExecutionProjection {
        schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id: execution_id.to_string(),
        revision: graph.revision,
        cursor: graph.commit_cursor,
        session_id,
        mission_id: scope.mission_id,
        strategy,
        graph,
        child_executions: scope.child_executions,
        goals: scope.goals,
        agents: scope.agents,
        teams: scope.teams,
        relations: scope.relations,
        approvals: scope.approvals,
        interventions: scope.interventions,
        usage,
        context: context_entities,
        evidence,
        health,
        recovery,
        live: services.execution_live(execution_id),
        available_commands: available_commands(services, execution_id, context).await?,
    })
}

fn strategy_entity(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    root_execution_id: &str,
    full: bool,
) -> Option<ProjectionEntity> {
    let session_id = scope.session_id.as_deref()?;
    let events = services
        .event_store()
        .list_stream(&format!("session:{session_id}"))
        .ok()?;
    let selected = events.iter().find(|event| {
        event.kind == "runtime.strategy.selected"
            && event
                .payload
                .get("execution_graph_ref")
                .and_then(serde_json::Value::as_str)
                == Some(root_execution_id)
    })?;
    let decision_id = selected
        .payload
        .get("decision_id")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    events
        .into_iter()
        .filter(|event| {
            matches!(
                event.kind.as_str(),
                "runtime.strategy.selected"
                    | "runtime.strategy.retargeted"
                    | "runtime.strategy.downgraded"
                    | "runtime.strategy.early_stopped"
                    | "runtime.strategy.outcome"
            ) && event
                .payload
                .get("execution_graph_ref")
                .and_then(serde_json::Value::as_str)
                == Some(root_execution_id)
                && event
                    .payload
                    .get("decision_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(decision_id.as_str())
        })
        .max_by_key(|event| {
            (
                event
                    .payload
                    .get("decision_revision")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                event.sequence,
            )
        })
        .map(|event| entity_from_runtime_event("strategy", event, full))
}

fn related_event_entities(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    kind: &str,
    full: bool,
    predicate: impl Fn(&crate::DurableRuntimeEvent) -> bool,
) -> Vec<ProjectionEntity> {
    services
        .event_store()
        .all_events(512)
        .unwrap_or_default()
        .into_iter()
        .filter(|event| scope.contains_event(event) && predicate(event))
        .map(|event| entity_from_runtime_event(kind, event, full))
        .collect()
}

fn entity_from_runtime_event(
    kind: &str,
    event: crate::DurableRuntimeEvent,
    full: bool,
) -> ProjectionEntity {
    ProjectionEntity {
        id: event.event_id,
        kind: kind.to_string(),
        revision: event.sequence,
        status: event.status,
        summary: Some(event.kind),
        evidence_refs: event
            .refs
            .into_iter()
            .map(|reference| reference.id)
            .collect(),
        detail: full.then_some(event.payload),
    }
}

pub fn delta(
    services: &RuntimeServices,
    execution_id: &str,
    base_cursor: u64,
    context: &ProjectionQueryContext,
) -> Result<ProjectionDelta, RuntimeServicesError> {
    validate_context(services, context)?;
    let graph = services.graph_state_store().projection(execution_id)?;
    let scope = ExecutionProjectionScope::load(
        services,
        execution_id,
        &graph,
        context.detail_scope == ProjectionDetailScope::Full,
    )?;
    validate_session_scope(scope.session_id.as_deref(), context)?;
    let mut events = Vec::new();
    let mut target_cursor = base_cursor;
    for batch in services
        .event_store()
        .events_after_cursor(base_cursor, MAX_DELTA_BATCHES)?
    {
        target_cursor = batch.commit_cursor;
        let mut visible = false;
        for event in batch.events {
            if scope.contains_event(&event) {
                visible = true;
                events.push(event_from_runtime(event, context.detail_scope));
            }
        }
        if !visible {
            events.push(ProjectionEvent {
                commit_cursor: batch.commit_cursor,
                transaction_index: 0,
                event_id: format!("cursor:{}", batch.commit_cursor),
                kind: ProjectionEventKind::CursorAdvanced,
                entity: None,
            });
        }
    }
    Ok(ProjectionDelta {
        schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id: execution_id.to_string(),
        base_cursor,
        target_cursor,
        events,
    })
}

pub async fn command(
    services: &RuntimeServices,
    execution_id: &str,
    context: &ProjectionQueryContext,
    request: ExecutionCommandRequest,
) -> Result<ExecutionCommandReceipt, RuntimeServicesError> {
    validate_context(services, context)?;
    let graph = services
        .graph_runner()
        .graph_projection(execution_id)
        .await?;
    validate_session_scope(
        session_id_from_graph(services, execution_id).as_deref(),
        context,
    )?;
    let command = match request.command {
        ExecutionCommandKind::Pause => ExecutionGraphCommand::Pause {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "paused by projection command".to_string()),
        },
        ExecutionCommandKind::Resume => ExecutionGraphCommand::Resume {
            expected_revision: request.expected_revision,
        },
        ExecutionCommandKind::Cancel => ExecutionGraphCommand::Cancel {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "cancelled by projection command".to_string()),
        },
        ExecutionCommandKind::Replan => ExecutionGraphCommand::Replan {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "replan requested by projection command".to_string()),
            replacement_payload_ref: string_payload(&request.payload, "replacement_payload_ref")
                .unwrap_or_else(|| "projection-command:replan".to_string()),
        },
    };
    if graph.revision != request.expected_revision {
        return Ok(ExecutionCommandReceipt {
            command_id: request.command_id,
            accepted_revision: graph.revision,
            status: "rejected_stale_revision".to_string(),
            reason: Some(
                "projection revision changed; refresh snapshot before retrying".to_string(),
            ),
        });
    }
    let receipt = services
        .graph_runner()
        .command_graph(execution_id, command)
        .await?;
    Ok(ExecutionCommandReceipt {
        command_id: request.command_id,
        accepted_revision: receipt.graph.revision,
        status: "accepted".to_string(),
        reason: None,
    })
}

async fn available_commands(
    services: &RuntimeServices,
    execution_id: &str,
    _context: &ProjectionQueryContext,
) -> Result<Vec<ProjectionCommandAvailability>, RuntimeServicesError> {
    let graph = services
        .graph_runner()
        .graph_projection(execution_id)
        .await?;
    let terminal = graph.nodes.iter().all(|node| node.status.is_terminal());
    let paused = graph
        .nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::Paused);
    Ok([
        ExecutionCommandKind::Pause,
        ExecutionCommandKind::Resume,
        ExecutionCommandKind::Cancel,
        ExecutionCommandKind::Replan,
    ]
    .into_iter()
    .map(|command| {
        let available = match command {
            ExecutionCommandKind::Pause => !terminal && !paused,
            ExecutionCommandKind::Resume => !terminal && paused,
            ExecutionCommandKind::Cancel | ExecutionCommandKind::Replan => !terminal,
        };
        ProjectionCommandAvailability {
            command,
            available,
            reason: (!available)
                .then(|| "execution state does not permit this command".to_string()),
        }
    })
    .collect())
}

fn validate_context(
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

fn validate_session_scope(
    session_id: Option<&str>,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if let Some(session_id) = session_id {
        if !context.session_scopes.is_empty()
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

fn validate_mission_scope(
    mission_id: Option<&str>,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if let Some(mission_id) = mission_id {
        if !context.mission_scopes.is_empty()
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
struct ExecutionProjectionScope {
    session_id: Option<String>,
    mission_id: Option<String>,
    execution_ids: BTreeSet<String>,
    node_ids: BTreeSet<String>,
    entity_ids: BTreeSet<String>,
    goals: Vec<ProjectionEntity>,
    agents: Vec<ProjectionEntity>,
    teams: Vec<ProjectionEntity>,
    relations: Vec<ProjectionEntity>,
    approvals: Vec<ProjectionEntity>,
    interventions: Vec<ProjectionEntity>,
    child_executions: Vec<ChildExecutionProjection>,
}

impl ExecutionProjectionScope {
    fn load(
        services: &RuntimeServices,
        execution_id: &str,
        graph: &harness_contract::execution_graph::ExecutionGraphProjection,
        full: bool,
    ) -> Result<Self, RuntimeServicesError> {
        let session_id = session_id_from_graph(services, execution_id);
        let mission_id = session_id.as_deref().and_then(|session_id| {
            services
                .mission_runtime()
                .mission_id_for_session(session_id)
        });
        let (execution_ids, child_executions, node_ids) =
            execution_lineage(services, execution_id, graph)?;

        let agent_snapshots = services
            .agent_runtime()
            .list()
            .into_iter()
            .filter(|agent| execution_ids.contains(&agent.graph_id))
            .collect::<Vec<_>>();
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

        let team_snapshots = services
            .team_runtime()
            .list()
            .unwrap_or_default()
            .into_iter()
            .filter(|team| execution_ids.contains(&team.graph_id))
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

        let approvals = services
            .approval_queue()
            .list()
            .into_iter()
            .filter(|approval| {
                approval
                    .source
                    .session_id
                    .as_deref()
                    .is_some_and(|id| session_id.as_deref() == Some(id))
                    || approval
                        .source
                        .agent_id
                        .as_ref()
                        .is_some_and(|id| agent_ids.contains(id))
                    || approval
                        .source
                        .team_id
                        .as_ref()
                        .is_some_and(|id| team_ids.contains(id))
            })
            .collect::<Vec<_>>();
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
        entity_ids.extend(team_ids);
        entity_ids.extend(goal_ids);
        entity_ids.extend(relation_ids);
        entity_ids.extend(approval_ids);
        Ok(Self {
            session_id,
            mission_id,
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

    fn contains_event(&self, event: &crate::DurableRuntimeEvent) -> bool {
        self.execution_ids.contains(&event.stream_id)
            || self.execution_ids.iter().any(|execution_id| {
                event
                    .stream_id
                    .starts_with(&format!("{execution_id}:node:"))
            })
            || event.refs.iter().any(|reference| {
                (reference.kind == "execution_graph" && self.execution_ids.contains(&reference.id))
                    || (reference.kind == "execution_node" && self.node_ids.contains(&reference.id))
                    || (reference.kind == "session"
                        && self.session_id.as_deref() == Some(reference.id.as_str()))
                    || self.entity_ids.contains(&reference.id)
            })
            || ["goal:", "approval:", "agent:"]
                .iter()
                .filter_map(|prefix| event.stream_id.strip_prefix(prefix))
                .any(|id| self.entity_ids.contains(id))
    }
}

fn session_id_from_graph(services: &RuntimeServices, execution_id: &str) -> Option<String> {
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

fn entities_from_details(
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

fn entity_from_value(
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
        detail: full.then_some(detail),
    }
}

fn goals_for_executions(
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
fn execution_lineage(
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

fn event_from_runtime(
    event: crate::DurableRuntimeEvent,
    detail_scope: ProjectionDetailScope,
) -> ProjectionEvent {
    let kind = if event.kind == "execution.lineage.child_registered.v1" {
        ProjectionEventKind::UpsertChildExecution
    } else if event.kind.contains("terminal") {
        ProjectionEventKind::TerminalCommitted
    } else if event.scope == RuntimeEventScope::Goal {
        ProjectionEventKind::GoalChanged
    } else if event.scope == RuntimeEventScope::Agent {
        ProjectionEventKind::UpsertAgent
    } else if event.scope == RuntimeEventScope::Team {
        ProjectionEventKind::UpsertTeam
    } else if event.scope == RuntimeEventScope::Approval {
        ProjectionEventKind::ApprovalChanged
    } else if event.scope == RuntimeEventScope::Relation {
        ProjectionEventKind::UpsertSessionRelation
    } else if event.scope == RuntimeEventScope::Tool {
        ProjectionEventKind::UsageChanged
    } else if event.scope == RuntimeEventScope::ExecutionNode {
        ProjectionEventKind::UpsertNode
    } else {
        ProjectionEventKind::HealthChanged
    };
    ProjectionEvent {
        commit_cursor: event.commit_cursor,
        transaction_index: event.transaction_index,
        event_id: event.event_id.clone(),
        kind,
        entity: Some(ProjectionEntity {
            id: event.event_id,
            kind: event.kind,
            revision: event.sequence,
            status: event.status,
            summary: Some(event.stream_id),
            evidence_refs: event
                .refs
                .into_iter()
                .map(|reference| reference.id)
                .collect(),
            detail: (detail_scope == ProjectionDetailScope::Full).then_some(event.payload),
        }),
    }
}

fn graph_status(nodes: &[harness_contract::execution_graph::ExecutionNodeProjection]) -> String {
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

fn string_payload(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{
        execution_graph::{
            ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
            ExecutionParentBinding,
        },
        goal::{AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract},
    };

    fn context(services: &RuntimeServices) -> ProjectionQueryContext {
        ProjectionQueryContext {
            principal: "test".to_string(),
            workspace_id: services.workspace_key().to_string(),
            session_scopes: Vec::new(),
            mission_scopes: Vec::new(),
            visibility_grants: vec!["test".to_string()],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 1,
        }
    }

    #[tokio::test]
    async fn projection_snapshot_delta_and_command_share_one_graph_revision() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("projection contract graph");
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial_snapshot = snapshot(&services, &graph_id, &query)
            .await
            .expect("snapshot");
        assert_eq!(initial_snapshot.execution_id, graph_id);
        assert_eq!(
            initial_snapshot.schema_version,
            EXECUTION_PROJECTION_SCHEMA_VERSION
        );
        let delta = delta(&services, &initial_snapshot.execution_id, 0, &query).expect("delta");
        assert!(delta.target_cursor >= initial_snapshot.cursor);
        assert!(delta.events.iter().all(|event| event.commit_cursor > 0));
        let receipt = command(
            &services,
            &initial_snapshot.execution_id,
            &query,
            ExecutionCommandRequest {
                command_id: "projection-pause".to_string(),
                expected_revision: initial_snapshot.revision,
                command: ExecutionCommandKind::Pause,
                payload: serde_json::json!({ "reason": "test" }),
            },
        )
        .await
        .expect("command receipt");
        assert_eq!(receipt.status, "accepted");
        assert!(receipt.accepted_revision > initial_snapshot.revision);
    }

    #[tokio::test]
    async fn projection_exposes_only_durable_child_execution_lineage() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let parent = ExecutionGraph::new("root execution");
        let parent_id = parent.id.clone();
        services
            .graph_runner()
            .start(parent)
            .await
            .expect("parent graph starts");

        let mut child = ExecutionGraph::new("nested team protocol");
        child.parent_execution = Some(ExecutionParentBinding {
            execution_id: parent_id.clone(),
            node_id: "root-tool-batch".to_string(),
        });
        let child_id = child.id.clone();
        services
            .graph_runner()
            .start(child)
            .await
            .expect("child graph starts");

        let sibling = ExecutionGraph::new("unrelated same-runtime execution");
        let sibling_id = sibling.id.clone();
        services
            .graph_runner()
            .start(sibling)
            .await
            .expect("sibling graph starts");

        let links = services
            .graph_state_store()
            .child_links(&parent_id)
            .expect("durable child index");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].child_execution_id, child_id);
        assert_eq!(links[0].parent_node_id, "root-tool-batch");

        let projection = snapshot(&services, &parent_id, &context(&services))
            .await
            .expect("parent projection");
        assert_eq!(projection.child_executions.len(), 1);
        assert_eq!(projection.child_executions[0].execution_id, child_id);
        assert_eq!(
            projection.child_executions[0].parent_node_id,
            "root-tool-batch"
        );
        assert!(projection
            .child_executions
            .iter()
            .all(|child| child.execution_id != sibling_id));

        let delta = delta(&services, &parent_id, 0, &context(&services)).expect("lineage delta");
        assert!(delta.events.iter().any(|event| {
            event
                .entity
                .as_ref()
                .is_some_and(|entity| entity.summary.as_deref() == Some(child_id.as_str()))
        }));
    }

    #[tokio::test]
    async fn projection_uses_latest_exact_strategy_revision_not_generic_orchestration() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let session_graph = |objective: &str| {
            let mut graph = ExecutionGraph::new(objective);
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "projection-test",
                serde_json::json!({
                    "session_id": "strategy-projection",
                    "kind": "projection_test",
                })
                .to_string(),
            );
            node.id = format!("{}:node", graph.id);
            graph
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
            graph.nodes.push(node);
            graph
        };
        let graph = session_graph("strategy projection");
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");
        let sibling = session_graph("same-session sibling strategy");
        let sibling_id = sibling.id.clone();
        services
            .graph_runner()
            .start(sibling)
            .await
            .expect("sibling graph starts");
        let child = session_graph("same-session child strategy");
        let child_id = child.id.clone();
        services
            .graph_runner()
            .start(child)
            .await
            .expect("child graph starts");
        let strategy_event = |execution_id: &str, decision_id: &str, kind: &str, revision: u64| {
            crate::RuntimeEventInput {
                stream_id: "session:strategy-projection".to_string(),
                scope: crate::RuntimeEventScope::ExecutionGraph,
                kind: kind.to_string(),
                status: Some("completed".to_string()),
                actor: Some("test".to_string()),
                refs: vec![crate::RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: execution_id.to_string(),
                }],
                payload: serde_json::json!({
                    "decision_id": decision_id,
                    "decision_revision": revision,
                    "execution_graph_ref": execution_id,
                    "selected_candidate": if revision == 1 { "team" } else { "direct" },
                }),
            }
        };
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.selected",
                1,
            ))
            .expect("selected event");
        for index in 0..600 {
            services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: "session:strategy-projection".to_string(),
                    scope: crate::RuntimeEventScope::ExecutionGraph,
                    kind: "runtime.noise".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("test".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({"index": index}),
                })
                .expect("noise event");
        }
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                kind: "runtime.orchestration.completed".to_string(),
                ..strategy_event(
                    &graph_id,
                    "decision-1",
                    "runtime.orchestration.completed",
                    99,
                )
            })
            .expect("generic orchestration event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.outcome",
                2,
            ))
            .expect("outcome event");
        for (execution_id, decision_id, revision) in [
            (&sibling_id, "decision-sibling", 30),
            (&child_id, "decision-child", 40),
        ] {
            services
                .event_store()
                .append(strategy_event(
                    execution_id,
                    decision_id,
                    "runtime.strategy.selected",
                    1,
                ))
                .expect("other selected event");
            services
                .event_store()
                .append(strategy_event(
                    execution_id,
                    decision_id,
                    "runtime.strategy.outcome",
                    revision,
                ))
                .expect("other outcome event");
        }

        let projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("projection");
        let strategy = projection.strategy.expect("exact strategy projection");
        assert_eq!(
            strategy.summary.as_deref(),
            Some("runtime.strategy.outcome")
        );
        assert_eq!(
            strategy
                .detail
                .as_ref()
                .and_then(|detail| detail.get("decision_revision"))
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
    }

    #[tokio::test]
    async fn projection_scope_never_leaks_other_session_goals() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mission = services
            .mission_runtime()
            .register_session(crate::StartMissionSessionRequest {
                title: "projection scope session".to_string(),
                session_id: Some("session-a".to_string()),
            })
            .expect("mission membership registers");
        assert_eq!(mission.session_id, "session-a");
        let mut graph = ExecutionGraph::new("session-scoped projection");
        let dispatch = harness_contract::turn::SessionDispatchCommand {
            command_id: "scope-dispatch".to_string(),
            action: harness_contract::turn::SessionDispatchAction::Enqueue,
            handoff: harness_contract::turn::SessionHandoff {
                handoff_id: "scope-handoff".to_string(),
                source_session_id: "session-a".to_string(),
                target_session_id: "session-target".to_string(),
                objective: "scope test".to_string(),
                acceptance: Vec::new(),
                scope: Vec::new(),
                context_lens: Vec::new(),
                evidence_refs: Vec::new(),
                context_budget_lease: None,
                permission_lease: "test".to_string(),
                deadline_at_ms: None,
                priority: 1,
                correlation_id: "scope-correlation".to_string(),
                result_contract: "return result".to_string(),
            },
            expected_target_revision: 0,
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::SessionDispatch,
            crate::SESSION_DISPATCH_EXECUTOR,
            format!(
                "session_handoff:{}",
                serde_json::to_string(&dispatch).expect("handoff serializes")
            ),
        );
        node.id = "dispatch-a".to_string();
        node.idempotency_key = "dispatch-a-key".to_string();
        graph.nodes.push(node);
        graph
            .node_statuses
            .insert("dispatch-a".to_string(), ExecutionNodeStatus::Planned);
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");

        for (id, session_id) in [
            (format!("goal:{graph_id}"), "session-a"),
            ("goal-b".to_string(), "session-b"),
        ] {
            services
                .goal_store()
                .create(GoalContract {
                    id: id.clone(),
                    session_id: session_id.to_string(),
                    objective: format!("objective for {session_id}"),
                    criteria: vec![AcceptanceCriterion {
                        id: format!("criterion-{id}"),
                        statement: "produce evidence".to_string(),
                        required_evidence: Vec::new(),
                        status: AcceptanceStatus::Open,
                        waiver: None,
                    }],
                    constraints: Vec::new(),
                    phase: "execution".to_string(),
                    evidence_refs: Vec::new(),
                    unresolved: Vec::new(),
                    blockers: Vec::new(),
                    completion: GoalCompletion::Open,
                    revision: 1,
                    user_sequence: 1,
                })
                .expect("goal creates");
        }

        let projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("snapshot");
        assert_eq!(projection.session_id.as_deref(), Some("session-a"));
        assert_eq!(
            projection.mission_id.as_deref(),
            Some(services.mission_runtime().mission_id())
        );
        assert_eq!(projection.goals.len(), 1);
        assert_eq!(projection.goals[0].id, format!("goal:{graph_id}"));
        assert!(projection.goals.iter().all(|goal| goal.id != "goal-b"));

        let mut denied = context(&services);
        denied.mission_scopes = vec!["mission-runtime:other-workspace".to_string()];
        assert!(matches!(
            snapshot(&services, &graph_id, &denied).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
    }

    #[tokio::test]
    async fn projection_command_rejects_stale_revision_without_mutating_graph() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("stale projection command");
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial_snapshot = snapshot(&services, &graph_id, &query)
            .await
            .expect("snapshot");
        let receipt = command(
            &services,
            &graph_id,
            &query,
            ExecutionCommandRequest {
                command_id: "stale-command".to_string(),
                expected_revision: initial_snapshot.revision.saturating_add(1),
                command: ExecutionCommandKind::Pause,
                payload: serde_json::Value::Null,
            },
        )
        .await
        .expect("stale receipt");
        assert_eq!(receipt.status, "rejected_stale_revision");
        let after = snapshot(&services, &graph_id, &query).await.expect("after");
        assert_eq!(after.revision, initial_snapshot.revision);
    }

    #[tokio::test]
    async fn projection_rejects_a_context_from_another_workspace() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("workspace scope");
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");
        let mut query = context(&services);
        query.workspace_id = "other-workspace".to_string();
        assert!(matches!(
            snapshot(&services, &graph_id, &query).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
    }
}
