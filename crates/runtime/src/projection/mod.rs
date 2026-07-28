//! Canonical read and command model for live execution state.
//!
//! This module owns no durable state. It translates the existing graph, goal,
//! agent, team, relation, approval, context and V3 event stores into the one
//! public contract exposed by `harness-contract::projection`.

use std::collections::{BTreeMap, BTreeSet};

use harness_contract::core::MeasureProvenance;
use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeStatus};
use harness_contract::projection::{
    AdmissionProjection, AdmissionProjectionStatus, ChildExecutionProjection, EvidenceProjection,
    ExecutionCommandKind, ExecutionCommandReceipt, ExecutionCommandRequest, ExecutionProjection,
    OutcomeProjection, OutcomeQualityProjection, ProjectionCommandAvailability, ProjectionDelta,
    ProjectionDetailScope, ProjectionEntity, ProjectionEntityCollection, ProjectionEntityPayload,
    ProjectionOperation, ProjectionQueryContext, ProjectionResyncReason, ProjectionSourceHealth,
    StrategyActualProjection, StrategyActualStatus, StrategyDecisionProjection,
    StrategyEvidenceScopeProjection, StrategyProofStatus, StrategyTransitionProjection,
    EXECUTION_PROJECTION_REDUCER_VERSION, EXECUTION_PROJECTION_SCHEMA_VERSION,
    STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION,
};
use harness_contract::reality::{EvidenceCompleteness, EvidenceRef, RealityBoundary};
use harness_contract::strategy::{
    ExecutionCandidateEstimate, ExecutionCandidateKind, StrategyDecisionSource,
    StrategyResourceSnapshot,
};
use harness_contract::team::FocusPartitionPlan;
use sha2::{Digest, Sha256};

use crate::execution_core::graph::{
    ResourceAdmissionObservation, ResourceAdmissionObservationStatus, ResourceWaitReason,
};
use crate::{ExecutionGraphHost, RuntimeEventScope, RuntimeServices, RuntimeServicesError};

mod delta;
mod reducer_support;
mod snapshot;

pub use delta::delta;
pub use snapshot::snapshot;

use reducer_support::*;
use snapshot::safe_public_ref;

const MAX_DELTA_BATCHES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionAuthorizationScope {
    pub session_id: Option<String>,
    pub mission_id: Option<String>,
    pub resource_grants: Vec<String>,
}

pub fn authorization_scope(
    services: &RuntimeServices,
    execution_id: &str,
) -> Result<ProjectionAuthorizationScope, RuntimeServicesError> {
    let graph = services.graph_state_store().projection(execution_id)?;
    let scope = ExecutionProjectionScope::load(services, execution_id, &graph, false)?;
    let mut resource_grants = scope
        .execution_ids
        .iter()
        .filter_map(|execution_id| services.graph_state_store().load(execution_id).ok())
        .flat_map(|graph| graph.nodes.into_iter())
        .flat_map(|node| node.resource_scopes.into_iter())
        .filter_map(|scope| safe_public_ref(&scope))
        .collect::<Vec<_>>();
    resource_grants.sort();
    resource_grants.dedup();
    Ok(ProjectionAuthorizationScope {
        session_id: scope.session_id,
        mission_id: scope.mission_id,
        resource_grants,
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
        .execution_supervisor()
        .graph_projection(execution_id)
        .await?;
    let session_id = session_id_from_graph(services, execution_id);
    validate_session_scope(session_id.as_deref(), context)?;
    let mission_id = session_id.as_deref().and_then(|session_id| {
        services
            .mission_runtime()
            .mission_id_for_session(session_id)
    });
    validate_mission_scope(mission_id.as_deref(), context)?;
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
        .execution_supervisor()
        .command_graph(execution_id, command)
        .await?;
    Ok(ExecutionCommandReceipt {
        command_id: request.command_id,
        accepted_revision: receipt.accepted_revision,
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
        .execution_supervisor()
        .graph_projection(execution_id)
        .await?;
    Ok(available_commands_for_graph(&graph))
}

fn available_commands_for_graph(
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
) -> Vec<ProjectionCommandAvailability> {
    let terminal = graph.nodes.iter().all(|node| node.status.is_terminal());
    let paused = graph
        .nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::Paused);
    [
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
    .collect()
}

#[cfg(test)]
mod tests {
    use super::snapshot::*;
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
            visibility_grants: vec![
                format!("workspace:{}", services.workspace_key()),
                "resource:*".to_string(),
            ],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 1,
        }
    }

    #[test]
    fn explicit_negative_strategy_warning_projects_as_cost_not_benefit() {
        let warning =
            "explicit Team request has negative estimated lift; surface must show the cost warning";
        let (benefit, cost) = strategy_public_reasons(
            &[
                warning.to_string(),
                "explicit Team topology was requested".to_string(),
            ],
            None,
            None,
            &[],
        );

        assert!(benefit.contains(&"explicit Team topology was requested".to_string()));
        assert!(!benefit.contains(&warning.to_string()));
        assert_eq!(cost, vec![warning.to_string()]);
    }

    #[test]
    fn admission_observation_projects_typed_wait_state_without_prose_inference() {
        let request_id = uuid::Uuid::new_v4();
        let event = crate::DurableRuntimeEvent {
            event_id: "admission-waiting".to_string(),
            stream_id: format!("resource-admission:{request_id}"),
            sequence: 3,
            scope: RuntimeEventScope::Schedule,
            kind: "resource.admission.waiting".to_string(),
            status: Some("waiting".to_string()),
            actor: Some("execution_resource_manager".to_string()),
            refs: vec![crate::RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: "graph-admission".to_string(),
            }],
            payload: serde_json::to_value(ResourceAdmissionObservation {
                request_id,
                status: ResourceAdmissionObservationStatus::Waiting,
                requested_priority: Some(90),
                deadline_at_ms: Some(42_000),
                requested_service_class:
                    harness_contract::execution_graph::ExecutionServiceClass::Interactive,
                resolved_service_class:
                    harness_contract::execution_graph::ExecutionServiceClass::Foreground,
                parent_class_ceiling: Some(
                    harness_contract::execution_graph::ExecutionServiceClass::Foreground,
                ),
                demands: vec![(crate::execution_core::graph::ExecutionResourceKind::Tool, 2)],
                normalized_scope: Some("workspace:/project".to_string()),
                fairness_key: "graph:graph-admission".to_string(),
                enqueue_sequence: Some(4),
                enqueued_at_ms: Some(1_000),
                observed_at_ms: 1_125,
                queue_age_ms: 125,
                wait_reason: Some(ResourceWaitReason::ScopeInfeasible),
                blocker: None,
                policy_revision: 7,
                pending: 2,
            })
            .expect("serialize observation"),
            created_at_ms: 1_125,
            commit_cursor: 8,
            transaction_id: "tx-admission".to_string(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        };

        let payload = projection_entity_payload(&event).expect("typed admission payload");
        let ProjectionEntityPayload::Admission(admission) = payload else {
            panic!("expected admission payload");
        };
        assert_eq!(admission.request_id, request_id.to_string());
        assert_eq!(admission.status, AdmissionProjectionStatus::WaitingScope);
        assert_eq!(admission.queue_age_ms, 125);
        assert_eq!(admission.wait_reason.as_deref(), Some("scope_infeasible"));
        assert_eq!(admission.resource_demands, vec!["tool:2"]);
    }

    #[tokio::test]
    async fn projection_snapshot_delta_and_command_share_one_graph_revision() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("projection contract graph");
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
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
        let delta = delta(&services, &initial_snapshot.execution_id, 0, 0, &query).expect("delta");
        assert!(delta.target_cursor >= initial_snapshot.cursor);
        assert!(delta.operations.iter().any(|operation| {
            matches!(
                operation,
                ProjectionOperation::AdvanceCursor { cursor }
                    if *cursor == delta.target_cursor
            )
        }));
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
    async fn projection_delta_materializes_the_same_state_as_a_fresh_snapshot() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("projection equivalence");
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial = snapshot(&services, &graph_id, &query)
            .await
            .expect("initial snapshot");
        let receipt = command(
            &services,
            &graph_id,
            &query,
            ExecutionCommandRequest {
                command_id: "projection-equivalence-pause".to_string(),
                expected_revision: initial.revision,
                command: ExecutionCommandKind::Pause,
                payload: serde_json::json!({"reason": "projection equivalence"}),
            },
        )
        .await
        .expect("pause command");
        assert_eq!(receipt.status, "accepted");

        let delta = delta(
            &services,
            &graph_id,
            initial.revision,
            initial.cursor,
            &query,
        )
        .expect("materialized delta");
        let reduced = harness_contract::projection::reduce_projection_delta(&initial, &delta)
            .expect("delta applies");
        let canonical = snapshot(&services, &graph_id, &query)
            .await
            .expect("canonical snapshot");
        assert_eq!(reduced, canonical);
    }

    #[tokio::test]
    async fn unrelated_commits_advance_only_the_projection_consumption_cursor() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("projection cursor isolation");
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial = snapshot(&services, &graph_id, &query)
            .await
            .expect("initial snapshot");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: "evolution:unrelated".to_string(),
                scope: RuntimeEventScope::Evolution,
                kind: "evolution.unrelated.recorded".to_string(),
                status: Some("recorded".to_string()),
                actor: Some("projection-test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"unrelated": true}),
            })
            .expect("unrelated event commits");

        let delta = delta(
            &services,
            &graph_id,
            initial.revision,
            initial.cursor,
            &query,
        )
        .expect("cursor-only delta");
        let reduced = harness_contract::projection::reduce_projection_delta(&initial, &delta)
            .expect("cursor-only delta applies");
        assert!(reduced.cursor > initial.cursor);
        assert_eq!(reduced.revision, initial.revision);
        assert_eq!(
            reduced.graph.commit_cursor, initial.graph.commit_cursor,
            "global projection consumption must not rewrite the graph commit cursor"
        );
    }

    #[tokio::test]
    async fn projection_exposes_only_durable_child_execution_lineage() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let parent = ExecutionGraph::new("root execution");
        let parent_id = parent.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                parent,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("parent graph starts");

        let mut child = ExecutionGraph::new("nested team protocol");
        child.parent_execution = Some(ExecutionParentBinding {
            execution_id: parent_id.clone(),
            node_id: "root-tool-batch".to_string(),
        });
        let child_id = child.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                child,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("child graph starts");

        let sibling = ExecutionGraph::new("unrelated same-runtime execution");
        let sibling_id = sibling.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                sibling,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
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

        let delta = delta(&services, &parent_id, 0, 0, &context(&services)).expect("lineage delta");
        assert!(delta.operations.iter().any(|operation| {
            matches!(
                operation,
                ProjectionOperation::UpsertChildExecution { child }
                    if child.execution_id == child_id
            )
        }));
    }

    #[test]
    fn linked_team_topology_supplies_strategy_identity_without_terminal_receipt() {
        let team_id = "runtime-team:live".to_string();
        let team_graph_id = "team-graph:runtime-team:live".to_string();
        let scope = ExecutionProjectionScope {
            session_id: Some("session-live-team".to_string()),
            mission_id: None,
            execution_ids: BTreeSet::from(["parent-execution".to_string(), team_graph_id.clone()]),
            node_ids: BTreeSet::new(),
            entity_ids: BTreeSet::from([team_id.clone()]),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: vec![ProjectionEntity {
                id: team_id.clone(),
                kind: "team".to_string(),
                revision: 1,
                status: Some("running".to_string()),
                summary: Some("live team".to_string()),
                evidence_refs: Vec::new(),
                payload: None,
                detail: Some(serde_json::json!({"graph_id": team_graph_id})),
            }],
            relations: Vec::new(),
            approvals: Vec::new(),
            interventions: Vec::new(),
            child_executions: vec![ChildExecutionProjection {
                execution_id: team_graph_id.clone(),
                parent_execution_id: "parent-execution".to_string(),
                parent_node_id: "parent-node".to_string(),
                revision: 1,
                cursor: 3,
                status: "running".to_string(),
                objective: "live delegated work".to_string(),
            }],
        };

        assert_eq!(live_team_topology(&scope), Some((team_id, team_graph_id)));
    }

    #[tokio::test]
    async fn projection_uses_latest_exact_strategy_revision_not_generic_orchestration() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let session_graph = |objective: &str| {
            let mut graph = ExecutionGraph::new(objective);
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
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
            .execution_supervisor()
            .register_graph(graph)
            .await
            .expect("graph registers");
        let sibling = session_graph("same-session sibling strategy");
        let sibling_id = sibling.id.clone();
        services
            .execution_supervisor()
            .register_graph(sibling)
            .await
            .expect("sibling graph registers");
        let child = session_graph("same-session child strategy");
        let child_id = child.id.clone();
        services
            .execution_supervisor()
            .register_graph(child)
            .await
            .expect("child graph registers");
        let strategy_event = |execution_id: &str, decision_id: &str, kind: &str, revision: u64| {
            crate::RuntimeEventInput {
                stream_id: "session:strategy-projection".to_string(),
                scope: crate::RuntimeEventScope::Session,
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
                    "session_ref": "strategy-projection",
                    "turn_ref": format!("turn-{decision_id}"),
                    "policy_version": "strategy-decision-v4",
                    "decision_source": "deterministic",
                    "confidence": 90,
                    "selected_candidate": if revision == 1 { "team" } else { "direct" },
                    "selected_pattern": if revision == 1 { "collaborate" } else { "direct" },
                    "candidate_estimates": [],
                    "selection_reasons": ["integer cost model selected the candidate"],
                    "resource_snapshot": harness_contract::strategy::StrategyResourceSnapshot::default(),
                    "evidence_scopes": [],
                    "outcome": if kind == "runtime.strategy.outcome" {
                        serde_json::json!({
                            "duration_ms": 42,
                            "input_tokens": 10,
                            "output_tokens": 5,
                            "cached_tokens": 0,
                            "tool_calls": 1,
                            "duplicate_tool_calls": 0,
                            "max_tool_concurrency_observed": 1,
                            "parallel_tool_batches": 0,
                            "write_attempt_paths": ["/home/private/secret.txt"],
                            "evidence_overlap_bp": 0,
                            "evidence_overlap_observed": true,
                            "working_state_verified": true,
                            "merge_cost_ms": 0,
                            "parent_merge_count": 1,
                            "quality_score_bp": 9000,
                            "actual_speedup_ratio_bp": null,
                            "terminal_reason": "completed"
                        })
                    } else {
                        serde_json::Value::Null
                    },
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
        let selected_projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("selected-only projection")
            .strategy
            .expect("selected-only strategy");
        assert_eq!(selected_projection.status.as_deref(), Some("running"));
        assert_eq!(
            selected_projection.actual_status,
            Some(StrategyActualStatus::Unknown)
        );
        assert!(selected_projection.actual.is_none());
        for index in 0..600 {
            services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: "session:strategy-projection".to_string(),
                    scope: crate::RuntimeEventScope::Session,
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
                "runtime.strategy.downgraded",
                2,
            ))
            .expect("downgrade event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.early_stopped",
                3,
            ))
            .expect("early stop event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.outcome",
                4,
            ))
            .expect("outcome event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.outcome",
                4,
            ))
            .expect("exact equal-revision replay");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                payload: serde_json::json!({
                    "decision_id": "decision-1",
                    "decision_revision": 4,
                    "execution_graph_ref": graph_id.clone(),
                    "session_ref": "strategy-projection",
                    "turn_ref": "turn-decision-1",
                    "policy_version": "strategy-decision-v4",
                    "decision_source": "deterministic",
                    "confidence": 1,
                    "selected_candidate": "team",
                    "selected_pattern": "collaborate",
                    "candidate_estimates": [],
                    "selection_reasons": ["conflicting replay must not replace truth"],
                    "resource_snapshot": harness_contract::strategy::StrategyResourceSnapshot::default(),
                    "evidence_scopes": [],
                    "outcome": serde_json::Value::Null,
                }),
                ..strategy_event(
                    &graph_id,
                    "decision-1",
                    "runtime.strategy.outcome",
                    4,
                )
            })
            .expect("conflicting equal-revision event");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                payload: serde_json::json!({
                    "decision_id": "decision-1",
                    "decision_revision": 99,
                    "execution_graph_ref": graph_id.clone(),
                    "session_ref": "strategy-projection",
                    "turn_ref": "turn-other",
                    "selected_candidate": "team",
                    "selected_pattern": "collaborate",
                }),
                ..strategy_event(&graph_id, "decision-1", "runtime.strategy.downgraded", 99)
            })
            .expect("wrong-turn event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.downgraded",
                1,
            ))
            .expect("stale revision event");
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
            strategy.schema_version,
            STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION
        );
        assert_eq!(strategy.decision_id.as_deref(), Some("decision-1"));
        assert_eq!(
            strategy.policy_version.as_deref(),
            Some("strategy-decision-v4")
        );
        assert_eq!(
            strategy.summary.as_deref(),
            Some("runtime.strategy.outcome")
        );
        assert_eq!(strategy.revision, 4);
        assert_eq!(
            strategy.selected_candidate,
            Some(ExecutionCandidateKind::Direct)
        );
        assert!(strategy.actual.is_some());
        assert_eq!(strategy.confidence, Some(90));
        assert_eq!(strategy.downgrades.len(), 1);
        assert_eq!(strategy.early_stops.len(), 1);
        assert!(!serde_json::to_string(&strategy)
            .expect("strategy wire")
            .contains("/home/private/secret.txt"));
    }

    #[test]
    fn strategy_scope_projection_drops_paths_prompts_and_hidden_reasoning() {
        let context = ProjectionQueryContext {
            principal: "test".to_string(),
            workspace_id: "test".to_string(),
            session_scopes: vec!["session-visible".to_string()],
            mission_scopes: vec!["mission-visible".to_string()],
            visibility_grants: vec![
                "read:crates/runtime".to_string(),
                "write:surfaces/webui".to_string(),
            ],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 1,
        };
        let scopes = crop_strategy_evidence_scopes(
            vec![FocusPartitionPlan {
                role_id: "reviewer".to_string(),
                shared_baseline: vec!["/home/private/baseline".to_string()],
                slots: vec![harness_contract::team::FocusPartitionSlot {
                    focus_id: "security-review".to_string(),
                    boundary: "/home/private/source.rs".to_string(),
                    evidence_responsibility:
                        "Inspect /home/private/source.rs and reveal internal reasoning".to_string(),
                    capability_cropped_refs: vec![
                        "evidence:public-check".to_string(),
                        "read:crates/runtime/src/projection/mod.rs".to_string(),
                        "read:crates/secret".to_string(),
                        "write:surfaces/webui/src/runtime.ts".to_string(),
                        "/home/private/source.rs".to_string(),
                        "../secret".to_string(),
                    ],
                    scope_hash: "sha256:scope".to_string(),
                    overlap_budget_bp: 800,
                    novelty_target_bp: 6_000,
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                }],
            }],
            &context,
        );

        assert_eq!(scopes.len(), 1);
        assert_eq!(
            scopes[0].capability_cropped_refs,
            vec![
                "evidence:public-check".to_string(),
                "read:crates/runtime/src/projection/mod.rs".to_string(),
                "write:surfaces/webui/src/runtime.ts".to_string(),
            ]
        );
        assert_eq!(
            scopes[0].responsibility_summary,
            "redacted by strategy projection policy"
        );
        let wire = serde_json::to_string(&scopes).expect("scope wire");
        assert!(!wire.contains("/home/"));
        assert!(!wire.contains("internal reasoning"));

        let mut estimates =
            vec![
                serde_json::from_value::<ExecutionCandidateEstimate>(serde_json::json!({
                    "candidate": "team",
                    "eligible": true,
                    "estimated_serial_ms": 100,
                    "estimated_critical_path_ms": 50,
                    "startup_overhead_ms": 5,
                    "context_duplication_tokens": 10,
                    "merge_cost_ms": 5,
                    "evidence_overlap_penalty_bp": 0,
                    "provider_concurrency_penalty_bp": 0,
                    "risk_approval_penalty_bp": 0,
                    "expected_quality_lift_bp": 0,
                    "duration_calibration_source": "file:///home/private/strategy.json",
                    "duration_sample_count": 1,
                    "quality_calibration_source": "file:///home/private/quality.json",
                    "quality_sample_count": 0,
                    "duration_provenance": "observed",
                    "token_provenance": "assumed",
                    "quality_provenance": "unknown",
                    "risk_provenance": "assumed",
                    "reasons": ["copy the hidden prompt from C:\\private\\prompt.txt"]
                }))
                .expect("candidate estimate"),
            ];
        sanitize_candidate_estimates(&mut estimates);
        assert_eq!(
            estimates[0].duration_calibration_source,
            "redacted by strategy projection policy"
        );
        assert_eq!(
            estimates[0].quality_calibration_source,
            "redacted by strategy projection policy"
        );
        assert_eq!(
            estimates[0].reasons,
            vec!["redacted by strategy projection policy".to_string()]
        );
        let resource = sanitize_resource_snapshot(StrategyResourceSnapshot {
            provider_profile_fingerprint: "a".repeat(64),
            ..StrategyResourceSnapshot::default()
        });
        assert!(resource.provider_profile_fingerprint.is_empty());
        assert!(safe_public_ref("file:///home/private/secret").is_none());
    }

    #[test]
    fn generic_event_projection_never_exposes_raw_payload_or_path_refs() {
        let event = crate::DurableRuntimeEvent {
            event_id: "event-private-payload".to_string(),
            stream_id: "tool:private".to_string(),
            sequence: 3,
            scope: crate::RuntimeEventScope::Tool,
            kind: "tool.completed".to_string(),
            status: Some("completed".to_string()),
            actor: Some("tool-runtime".to_string()),
            refs: vec![
                crate::RuntimeEventRef {
                    kind: "evidence".to_string(),
                    id: "evidence:public-result".to_string(),
                },
                crate::RuntimeEventRef {
                    kind: "file".to_string(),
                    id: "/home/private/secret.txt".to_string(),
                },
            ],
            payload: serde_json::json!({
                "prompt": "hidden system instruction",
                "path": "/home/private/secret.txt",
                "result": "private tool output"
            }),
            created_at_ms: 1,
            commit_cursor: 2,
            transaction_id: "transaction-private".to_string(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        };

        let projection = entity_from_runtime_event("usage", event, true);
        let wire = serde_json::to_string(&projection).expect("projection wire");
        assert!(projection.detail.is_none());
        assert_eq!(
            projection.evidence_refs,
            vec!["evidence:public-result".to_string()]
        );
        assert!(!wire.contains("hidden system instruction"));
        assert!(!wire.contains("/home/private"));
        assert!(!wire.contains("private tool output"));
    }

    #[tokio::test]
    async fn projection_scope_never_leaks_other_session_goals() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mission = services
            .mission_runtime()
            .ensure_default_mission()
            .expect("default Mission");
        services
            .mission_runtime()
            .link_session(
                &mission.mission_id,
                mission.revision,
                "session-a",
                Vec::new(),
            )
            .expect("mission membership registers");
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
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
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
            Some(services.mission_runtime().default_mission_id())
        );
        assert_eq!(projection.goals.len(), 1);
        assert_eq!(projection.goals[0].id, format!("goal:{graph_id}"));
        assert!(projection.goals.iter().all(|goal| goal.id != "goal-b"));

        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let denied = ProjectionQueryContext {
            principal: "scoped-reader".to_string(),
            workspace_id: services.workspace_key().to_string(),
            session_scopes: vec!["session-b".to_string()],
            mission_scopes: vec![mission_id],
            visibility_grants: vec!["read:crates/runtime".to_string()],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 7,
        };
        assert!(matches!(
            snapshot(&services, &graph_id, &denied).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            delta(&services, &graph_id, 0, 0, &denied),
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            command(
                &services,
                &graph_id,
                &denied,
                ExecutionCommandRequest {
                    command_id: "denied-session".to_string(),
                    expected_revision: projection.revision,
                    command: ExecutionCommandKind::Pause,
                    payload: serde_json::json!({"reason": "must not execute"}),
                },
            )
            .await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));

        let denied_mission = ProjectionQueryContext {
            session_scopes: vec!["session-a".to_string()],
            mission_scopes: vec!["mission-other".to_string()],
            ..denied
        };
        assert!(matches!(
            snapshot(&services, &graph_id, &denied_mission).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            delta(&services, &graph_id, 0, 0, &denied_mission),
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            command(
                &services,
                &graph_id,
                &denied_mission,
                ExecutionCommandRequest {
                    command_id: "denied-mission".to_string(),
                    expected_revision: projection.revision,
                    command: ExecutionCommandKind::Pause,
                    payload: serde_json::json!({"reason": "must not execute"}),
                },
            )
            .await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
    }

    #[tokio::test]
    async fn projection_command_rejects_stale_revision_without_mutating_graph() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("stale projection command");
        let graph_id = graph.id.clone();
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
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
            .execution_supervisor()
            .submit_and_wait(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph starts");
        let mut query = context(&services);
        query.workspace_id = "other-workspace".to_string();
        assert!(matches!(
            snapshot(&services, &graph_id, &query).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
    }

    #[test]
    fn public_projection_text_rejects_every_shared_path_syntax() {
        let corpus: Vec<String> = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-public-redaction-corpus.json"
        ))
        .expect("shared redaction corpus");
        for secret in corpus {
            let rendered = safe_public_text(&format!("strategy detail {secret}"), 512);
            assert_eq!(
                rendered, "redacted by strategy projection policy",
                "{secret}"
            );
            assert!(
                safe_public_ref(&secret).is_none(),
                "unsafe reference {secret}"
            );
        }
    }
}
