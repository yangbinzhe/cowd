use std::sync::Weak;

use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionFailure, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionUsage,
};
use harness_contract::team::TeamInstantiationRequest;

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};
use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope};

pub struct TeamSubgraphExecutor {
    teams: Weak<crate::TeamRuntime>,
    supervisor: Weak<crate::RuntimeExecutionSupervisor>,
}

impl TeamSubgraphExecutor {
    pub const KIND: &'static str = crate::orchestration::compiler::TEAM_SUBGRAPH_EXECUTOR;

    #[must_use]
    pub fn new(
        teams: Weak<crate::TeamRuntime>,
        supervisor: Weak<crate::RuntimeExecutionSupervisor>,
    ) -> Self {
        Self { teams, supervisor }
    }

    fn request(
        &self,
        node_id: &str,
        payload_ref: &str,
    ) -> Result<TeamInstantiationRequest, NodeExecutorError> {
        let request =
            serde_json::from_str::<TeamInstantiationRequest>(payload_ref).map_err(|error| {
                NodeExecutorError::Invalid {
                    node_id: node_id.to_string(),
                    reason: format!("invalid Team subgraph request: {error}"),
                }
            })?;
        request
            .validate()
            .map_err(|error| NodeExecutorError::Invalid {
                node_id: node_id.to_string(),
                reason: error.to_string(),
            })?;
        Ok(request)
    }
}

#[async_trait]
impl NodeExecutor for TeamSubgraphExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn supports_resumable_pause(&self) -> bool {
        false
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind != Self::KIND {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Team subgraph node must use the canonical executor".to_string(),
            });
        }
        let request = self.request(&node.id, &node.payload_ref)?;
        if request
            .parent_execution
            .as_ref()
            .is_none_or(|parent| parent.node_id != node.id || parent.execution_id.trim().is_empty())
        {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Team subgraph parent binding does not match its root node".to_string(),
            });
        }
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        let mut request = self.request(&context.node.id, &context.node.payload_ref)?;
        let parent =
            request
                .parent_execution
                .as_ref()
                .ok_or_else(|| NodeExecutorError::Invalid {
                    node_id: context.node.id.clone(),
                    reason: "Team subgraph has no parent execution binding".to_string(),
                })?;
        if parent.execution_id != context.graph.id || parent.node_id != context.node.id {
            return Err(NodeExecutorError::Invalid {
                node_id: context.node.id.clone(),
                reason: "Team subgraph cannot redirect its Runtime-owned parent binding"
                    .to_string(),
            });
        }
        let mut summaries = Vec::new();
        for edge in context.graph.edges.iter().filter(|edge| {
            edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::DependsOn
                && edge.to == context.node.id
        }) {
            let Some(result) = context.graph.node_results.get(&edge.from) else {
                continue;
            };
            request
                .upstream_evidence_refs
                .extend(result.evidence_refs.iter().cloned());
            if let Some(result_ref) = result
                .result_ref
                .as_deref()
                .filter(|reference| is_durable_artifact_locator(reference))
            {
                request.upstream_artifact_refs.push(result_ref.to_string());
            }
            if let Some(summary) = result.summary.as_deref() {
                summaries.push(format!(
                    "{}: {}",
                    edge.from,
                    summary.chars().take(4_000).collect::<String>()
                ));
            }
        }
        request.upstream_evidence_refs.sort_by(|left, right| {
            left.evidence_ref
                .ref_type
                .cmp(&right.evidence_ref.ref_type)
                .then_with(|| left.evidence_ref.id.cmp(&right.evidence_ref.id))
        });
        request
            .upstream_evidence_refs
            .dedup_by(|left, right| left.evidence_ref == right.evidence_ref);
        request.upstream_artifact_refs.sort();
        request.upstream_artifact_refs.dedup();
        if !summaries.is_empty() {
            request.objective.push_str(
                "\n\n## Verified predecessor artifacts\nUse these committed root-graph results as upstream evidence. Preserve unresolved conflicts explicitly.\n",
            );
            request.objective.push_str(&summaries.join("\n"));
        }
        let payload_ref =
            serde_json::to_string(&request).map_err(|error| NodeExecutorError::Invalid {
                node_id: context.node.id.clone(),
                reason: format!("encode Team subgraph dependency context: {error}"),
            })?;
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: Self::KIND.to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let request = self.request(&ticket.node_id, &ticket.payload_ref)?;
        let child_graph_id = format!("team-graph:{}", request.team_id);
        let teams = self
            .teams
            .upgrade()
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        let projection = teams
            .instantiate_or_resume(request)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?;
        let completed = projection.status == "completed" && projection.terminal_result.is_some();
        let evidence_refs = projection
            .terminal_result
            .as_ref()
            .map(|result| result.evidence_refs.clone())
            .unwrap_or_default();
        let status = if completed {
            ExecutionNodeStatus::Completed
        } else {
            match projection.status.as_str() {
                "failed" => ExecutionNodeStatus::Failed,
                "cancelled" => ExecutionNodeStatus::Cancelled,
                _ => ExecutionNodeStatus::Blocked,
            }
        };
        let failure = (!completed).then(|| ExecutionFailure {
            kind: "child_graph_terminal_without_verified_result".to_string(),
            message: format!(
                "Team child graph `{child_graph_id}` settled as {}",
                projection.status
            ),
            retryable: false,
            evidence_refs: evidence_refs.clone(),
        });
        let result_ref = projection
            .terminal_result
            .as_ref()
            .map(|result| result.result_ref.clone())
            .or_else(|| Some(format!("execution-graph:{child_graph_id}")));
        let usage = if completed {
            let supervisor =
                self.supervisor
                    .upgrade()
                    .ok_or_else(|| NodeExecutorError::Unavailable {
                        executor_kind: Self::KIND.to_string(),
                        node_id: ticket.node_id.clone(),
                    })?;
            supervisor
                .projection(&child_graph_id)
                .await
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("load completed Team child usage: {reason}"),
                })?
                .nodes
                .into_iter()
                .find(|node| node.kind == ExecutionNodeKind::Synthesize)
                .map(|node| node.usage)
                .unwrap_or_default()
        } else {
            ExecutionUsage::default()
        };
        let summary = result_ref
            .as_deref()
            .and_then(decode_team_terminal_summary)
            .map(|terminal| format!("Team `{}` result:\n{terminal}", projection.team_id))
            .unwrap_or_else(|| {
                format!(
                    "Team `{}` completed child graph revision {} with {} role tasks",
                    projection.team_id,
                    projection.graph_revision,
                    projection.tasks.len()
                )
            });
        let domain_event = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: format!("execution-lineage:{}", ticket.graph_id),
                scope: RuntimeEventScope::Relation,
                kind: "execution.lineage.child_terminal.v1".to_string(),
                status: Some(projection.status.clone()),
                actor: Some(Self::KIND.to_string()),
                refs: vec![
                    RuntimeEventRef {
                        kind: "execution_graph".to_string(),
                        id: ticket.graph_id.clone(),
                    },
                    RuntimeEventRef {
                        kind: "execution_node".to_string(),
                        id: ticket.node_id.clone(),
                    },
                    RuntimeEventRef {
                        kind: "execution_graph".to_string(),
                        id: child_graph_id.clone(),
                    },
                ],
                payload: serde_json::json!({
                    "parent_execution_id": ticket.graph_id,
                    "parent_node_id": ticket.node_id,
                    "child_execution_id": child_graph_id,
                    "child_revision": projection.graph_revision,
                    "child_status": projection.status,
                    "terminal_result_ref": result_ref.clone(),
                }),
            },
            idempotency_key: Some(format!(
                "child-terminal:{}:{}:{}",
                ticket.graph_id, ticket.node_id, projection.graph_revision
            )),
            schema_version: 1,
        };
        Ok(NodeExecutionOutcome {
            result: ExecutionNodeResult {
                status,
                result_ref,
                summary: Some(summary),
                evidence_refs,
                failure,
                usage,
                finished_at_ms: now_ms(),
            },
            delivery_envelope: None,
            terminal_presentation: None,
            domain_events: vec![domain_event],
            replan: None,
        })
    }

    async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let request = self.request(&ticket.node_id, &ticket.payload_ref)?;
        let graph_id = format!("team-graph:{}", request.team_id);
        let supervisor =
            self.supervisor
                .upgrade()
                .ok_or_else(|| NodeExecutorError::Unavailable {
                    executor_kind: Self::KIND.to_string(),
                    node_id: ticket.node_id.clone(),
                })?;
        let projection = match supervisor.projection(&graph_id).await {
            Ok(projection) => projection,
            Err(_) => return Ok(()),
        };
        if projection
            .nodes
            .iter()
            .all(|node| node.status.is_terminal())
        {
            return Ok(());
        }
        use crate::ExecutionGraphHost;
        supervisor
            .command_graph(
                &graph_id,
                harness_contract::execution_graph::ExecutionGraphCommand::Cancel {
                    expected_revision: projection.revision,
                    reason: format!("parent subgraph node {} cancelled", ticket.node_id),
                },
            )
            .await
            .map_err(|error| NodeExecutorError::Cancel {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        Ok(())
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_durable_artifact_locator(reference: &str) -> bool {
    reference
        .strip_prefix("artifact:")
        .is_some_and(|locator| !locator.trim().is_empty())
}

fn decode_team_terminal_summary(reference: &str) -> Option<String> {
    const MAX_CHARS: usize = 4_000;
    let encoded = reference.strip_prefix("assistant_json:")?;
    let value = serde_json::from_str::<String>(encoded).ok()?;
    let mut chars = value.trim().chars();
    let summary = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        Some(format!(
            "{summary}\n...[Team terminal result truncated; inspect durable evidence for full content]"
        ))
    } else {
        Some(summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent::DefinitionScope;
    use harness_contract::context::{EvidenceAccessRef, EvidenceRef};
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionUsage,
    };
    use harness_contract::team::{
        TeamSelectionMode, TeamTemplateDefinitionId, TeamTemplateSelector,
    };
    use std::sync::{Arc, Weak};

    #[test]
    fn team_terminal_result_becomes_bounded_dependency_summary() {
        let encoded = serde_json::to_string(&format!("verified:{}", "x".repeat(5_000)))
            .expect("terminal result");
        let summary = decode_team_terminal_summary(&format!("assistant_json:{encoded}"))
            .expect("decoded summary");

        assert!(summary.starts_with("verified:"));
        assert!(summary.contains("Team terminal result truncated"));
        assert!(summary.chars().count() < 4_200);
        assert!(decode_team_terminal_summary("artifact:report").is_none());
    }

    #[tokio::test]
    async fn start_materializes_verified_predecessor_context_for_child_team() {
        let graph_id = "mission-graph:root";
        let node_id = "mission-graph:root:semantic:team-b:1";
        let request = TeamInstantiationRequest {
            request_id: "team-b-request".to_string(),
            team_id: "team-b".to_string(),
            mission_id: "mission-1".to_string(),
            lineage: harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                root_task_id: "task-root-1".to_string(),
                task_id: "task-root-1".to_string(),
                generation: 1,
            },
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: graph_id.to_string(),
                node_id: node_id.to_string(),
            }),
            selection_mode: TeamSelectionMode::Explicit,
            strategy_binding: None,
            template_selector: TeamTemplateSelector::LatestStable {
                template_id: TeamTemplateDefinitionId::new(
                    DefinitionScope::Builtin,
                    "cowd/direct-executor",
                )
                .expect("template"),
            },
            objective: "consume Team A findings".to_string(),
            acceptance: vec!["checked synthesis".to_string()],
            risk: None,
            role_binding_overrides: Vec::new(),
            cardinality_overrides: Vec::new(),
            focus_partition_plans: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "test-model".to_string(),
            budget_lease: None,
            managed_invocation: None,
            resource_scopes: vec!["read:crates/runtime".to_string()],
            upstream_evidence_refs: Vec::new(),
            upstream_artifact_refs: Vec::new(),
        };
        let mut predecessor =
            ExecutionNodeSpec::new(ExecutionNodeKind::Subgraph, "fixture", "fixture");
        predecessor.id = "team-a".to_string();
        let mut inline_predecessor =
            ExecutionNodeSpec::new(ExecutionNodeKind::Subgraph, "fixture", "fixture");
        inline_predecessor.id = "team-inline".to_string();
        let mut target = ExecutionNodeSpec::new(
            ExecutionNodeKind::Subgraph,
            TeamSubgraphExecutor::KIND,
            serde_json::to_string(&request).expect("request"),
        );
        target.id = node_id.to_string();
        target.idempotency_key = "team-b-request".to_string();
        let evidence = EvidenceAccessRef::durable(
            EvidenceRef::observed("team_result", "team-a-result"),
            "a".repeat(64),
            1,
            "application/json",
            "artifact://team-a-result",
            "mission:mission-1",
        );
        let mut graph = ExecutionGraph::new("root mission");
        graph.id = graph_id.to_string();
        graph.nodes = vec![predecessor, inline_predecessor, target.clone()];
        graph.edges = vec![
            ExecutionEdge {
                from: "team-a".to_string(),
                to: node_id.to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
            ExecutionEdge {
                from: "team-inline".to_string(),
                to: node_id.to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
        ];
        graph.node_statuses.insert(
            "team-a".to_string(),
            harness_contract::execution_graph::ExecutionNodeStatus::Completed,
        );
        graph.node_results.insert(
            "team-a".to_string(),
            ExecutionNodeResult {
                status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                result_ref: Some("artifact:team-a:report".to_string()),
                summary: Some("Team A verified the runtime boundary.".to_string()),
                evidence_refs: vec![evidence],
                failure: None,
                usage: ExecutionUsage::default(),
                finished_at_ms: 1,
            },
        );
        graph.node_statuses.insert(
            "team-inline".to_string(),
            harness_contract::execution_graph::ExecutionNodeStatus::Completed,
        );
        graph.node_results.insert(
            "team-inline".to_string(),
            ExecutionNodeResult {
                status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                result_ref: Some(format!("assistant_json:{}", "x".repeat(8_000))),
                summary: Some("Team inline result remains available as a bounded summary.".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: ExecutionUsage::default(),
                finished_at_ms: 1,
            },
        );

        let executor = TeamSubgraphExecutor::new(Weak::new(), Weak::new());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node: target,
                attempt: 1,
            })
            .await
            .expect("ticket");
        let materialized: TeamInstantiationRequest =
            serde_json::from_str(&ticket.payload_ref).expect("materialized request");
        assert_eq!(materialized.upstream_evidence_refs.len(), 1);
        assert_eq!(
            materialized.upstream_artifact_refs,
            vec!["artifact:team-a:report"]
        );
        materialized
            .validate()
            .expect("inline assistant output is not misclassified as an artifact locator");
        assert!(materialized
            .objective
            .contains("Team A verified the runtime boundary."));
        assert!(materialized
            .objective
            .contains("Team inline result remains available as a bounded summary."));
    }
}
