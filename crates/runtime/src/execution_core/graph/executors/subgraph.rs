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
        for edge in context
            .graph
            .edges
            .iter()
            .filter(|edge| edge.kind.is_dependency() && edge.to == context.node.id)
        {
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
                summaries.push(format!("{}: {}", edge.from, summary));
            }
        }
        if let Some(binding) = context.graph.continuation_binding.as_ref() {
            if binding.source_session_id != request.lineage.session_id
                && binding.handoff_id.as_deref().is_none_or(str::is_empty)
            {
                return Err(NodeExecutorError::Invalid {
                    node_id: context.node.id.clone(),
                    reason: "cross-session continuation lacks a durable accepted handoff"
                        .to_string(),
                });
            }
            // The binding is immutable graph truth.  Give the child Team its
            // exact durable result locators, never an old user transcript or
            // a free-form reconstruction of the earlier Team prompt.
            request
                .upstream_artifact_refs
                .extend(binding.result_refs.iter().cloned());
            summaries.push(format!(
                "continuation {} from {} ({})",
                binding.team_set_ref, binding.source_root_id, binding.binding_digest
            ));
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
            let predecessor_context = format!(
                "## Verified predecessor artifacts\nUse these committed root-graph results as upstream evidence. Preserve unresolved conflicts explicitly.\n{}",
                summaries.join("\n")
            );
            request.objective.push_str("\n\n");
            request.objective.push_str(&predecessor_context);
            // Focus plans are compiled before this child node becomes ready.
            // Agent prompts consume their shared baselines rather than the
            // Team graph's top-level objective, so attaching handoff text
            // only above would make a downstream Team claim its Runtime
            // inputs were absent even though the immutable request carried
            // their evidence refs.  Copy this Runtime-derived context into
            // every role baseline; it is a governed handoff, never model text.
            for focus_plan in &mut request.focus_partition_plans {
                if !focus_plan
                    .shared_baseline
                    .iter()
                    .any(|item| item == &predecessor_context)
                {
                    focus_plan.shared_baseline.push(predecessor_context.clone());
                }
            }
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
            // Each durable join observation is independently replayable. A
            // fixed key would replay the first WaitingExternal receipt after
            // the child settles and the parent node is re-queued.
            idempotency_key: format!(
                "{}:attempt:{}",
                context.node.idempotency_key, context.attempt
            ),
            payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let mut request = self.request(&ticket.node_id, &ticket.payload_ref)?;
        let child_graph_id = format!("team-graph:{}", request.team_id);
        let teams = self
            .teams
            .upgrade()
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        let supervisor =
            self.supervisor
                .upgrade()
                .ok_or_else(|| NodeExecutorError::Unavailable {
                    executor_kind: Self::KIND.to_string(),
                    node_id: ticket.node_id.clone(),
                })?;
        // `start` may be called before every dependency has a terminal
        // parent result. Re-materialize immediately before admission so a
        // ready cross-Team consumer receives the actual completed handoff,
        // rather than the empty snapshot from its first scheduling attempt.
        let parent_graph = teams
            .graph_state_store()
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("load current cross-Team predecessor context: {error}"),
            })?;
        attach_current_predecessor_context(&mut request, &parent_graph, &ticket.node_id);
        crate::orchestration::collaboration_coordinator::record_incoming_cross_team_deliveries(
            &ticket.graph_id,
            &ticket.node_id,
            supervisor.as_ref(),
            teams.graph_state_store(),
        )
        .await
        .map_err(|reason| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason,
        })?;
        // Claim the exact delivered receipt before the child Team is admitted.
        // `admit_or_resume` may start its child graph, so claiming afterwards
        // would allow the consumer to execute before its authorized inputs
        // were durably fenced.
        crate::orchestration::collaboration_coordinator::claim_incoming_cross_team_deliveries(
            &ticket.graph_id,
            &ticket.node_id,
            ticket.attempt,
            supervisor.as_ref(),
            teams.graph_state_store(),
        )
        .await
        .map_err(|reason| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason,
        })?;
        let projection = match teams.admit_or_resume(request).await {
            Ok(projection) => projection,
            Err(reason) => {
                crate::orchestration::collaboration_coordinator::mark_team_admission_rejected(
                    &ticket.graph_id,
                    &ticket.node_id,
                    supervisor.as_ref(),
                    teams.graph_state_store(),
                )
                .await
                .map_err(|mark_error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!(
                        "team admission failed ({reason}); persist Program rejection failed: {mark_error}"
                    ),
                })?;
                return Err(NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                });
            }
        };
        crate::orchestration::collaboration_coordinator::mark_team_admitted(
            &ticket.graph_id,
            &ticket.node_id,
            &child_graph_id,
            supervisor.as_ref(),
            teams.graph_state_store(),
        )
        .await
        .map_err(|reason| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason,
        })?;
        // `TeamProjection.status` is a delivery string.  It deliberately has
        // richer non-terminal values such as `preparing`, `waiting_approval`
        // and `waiting_external`; treating every value other than `running`
        // as a child terminal races the Team's own durable scheduler and
        // turns a healthy second Team into a false parent Blocked result.
        // Lifecycle is the typed graph-derived authority for this decision.
        if projection.lifecycle != harness_contract::team::TeamLifecycleState::Terminal {
            return Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::WaitingExternal,
                result_ref: Some(format!("execution-graph:{child_graph_id}")),
                summary: Some(format!(
                    "Team `{}` was durably admitted and is {} under the Runtime supervisor",
                    projection.team_id, projection.status
                )),
                evidence_refs: Vec::new(),
                failure: None,
                usage: ExecutionUsage::default(),
                finished_at_ms: now_ms(),
            }));
        }
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
        let child_projection = if completed {
            let supervisor =
                self.supervisor
                    .upgrade()
                    .ok_or_else(|| NodeExecutorError::Unavailable {
                        executor_kind: Self::KIND.to_string(),
                        node_id: ticket.node_id.clone(),
                    })?;
            Some(
                supervisor
                    .projection(&child_graph_id)
                    .await
                    .map_err(|reason| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: format!("load completed Team child usage: {reason}"),
                    })?,
            )
        } else {
            None
        };
        let usage = child_projection
            .as_ref()
            .and_then(|child| {
                child
                    .nodes
                    .iter()
                    .find(|node| node.kind == ExecutionNodeKind::Synthesize)
                    .map(|node| node.usage.clone())
            })
            .unwrap_or_default();
        let summary = child_projection
            .as_ref()
            .and_then(|child| {
                child
                    .nodes
                    .iter()
                    .find(|node| node.kind == ExecutionNodeKind::Synthesize)
                    .and_then(|node| node.summary.clone())
            })
            .or_else(|| result_ref.as_deref().and_then(decode_team_terminal_summary))
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

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        // Close the fast-child race: the child may settle after admission but
        // before the parent WaitingExternal transition commits. Rechecking
        // after that commit guarantees the durable join is re-queued.
        let request = self.request(&ticket.node_id, &ticket.payload_ref)?;
        let child_graph_id = format!("team-graph:{}", request.team_id);
        let supervisor =
            self.supervisor
                .upgrade()
                .ok_or_else(|| NodeExecutorError::Unavailable {
                    executor_kind: Self::KIND.to_string(),
                    node_id: ticket.node_id.clone(),
                })?;
        if let Err(error) = supervisor
            .wake_parent_for_settled_child(&child_graph_id)
            .await
        {
            // WaitingExternal is already durable. A post-commit wake failure
            // must never rewrite that truth as a failed node; the supervisor's
            // settled-child observer and startup recovery can safely retry.
            tracing::warn!(
                parent_graph_id = ticket.graph_id,
                parent_node_id = ticket.node_id,
                child_graph_id,
                %error,
                "settled Team child could not wake its durable parent join"
            );
        }
        Ok(())
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

/// Attach only the results that are already committed on dependency edges of
/// the parent graph. This is called again at admission time because scheduler
/// ticket creation can precede the producer's terminal commit.
fn attach_current_predecessor_context(
    request: &mut TeamInstantiationRequest,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    consumer_node_id: &str,
) {
    let mut summaries = Vec::new();
    for edge in graph
        .edges
        .iter()
        .filter(|edge| edge.kind.is_dependency() && edge.to == consumer_node_id)
    {
        let Some(result) = graph.node_results.get(&edge.from) else {
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
            summaries.push(format!("{}: {}", edge.from, summary));
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
    if summaries.is_empty() {
        return;
    }
    let predecessor_context = format!(
        "## Verified predecessor artifacts\nUse these committed root-graph results as upstream evidence. Preserve unresolved conflicts explicitly.\n{}",
        summaries.join("\n")
    );
    if !request.objective.contains(&predecessor_context) {
        request.objective.push_str("\n\n");
        request.objective.push_str(&predecessor_context);
    }
    for focus_plan in &mut request.focus_partition_plans {
        if !focus_plan
            .shared_baseline
            .iter()
            .any(|item| item == &predecessor_context)
        {
            focus_plan.shared_baseline.push(predecessor_context.clone());
        }
    }
}

fn is_durable_artifact_locator(reference: &str) -> bool {
    reference
        .strip_prefix("artifact:")
        .is_some_and(|locator| !locator.trim().is_empty())
}

fn decode_team_terminal_summary(reference: &str) -> Option<String> {
    let encoded = reference.strip_prefix("assistant_json:")?;
    let value = serde_json::from_str::<String>(encoded).ok()?;
    Some(value.trim().to_string())
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
        FocusPartitionPlan, FocusPartitionSlot, TeamSelectionMode, TeamTemplateDefinitionId,
        TeamTemplateSelector,
    };
    use std::sync::{Arc, Weak};

    #[test]
    fn team_terminal_result_remains_complete_dependency_evidence() {
        let encoded = serde_json::to_string(&format!("verified:{}", "x".repeat(5_000)))
            .expect("terminal result");
        let summary = decode_team_terminal_summary(&format!("assistant_json:{encoded}"))
            .expect("decoded summary");

        assert!(summary.starts_with("verified:"));
        assert_eq!(summary.chars().count(), "verified:".chars().count() + 5_000);
        assert!(!summary.contains("truncated"));
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
            display_name: None,
            role_display_overrides: Vec::new(),
            cardinality_overrides: Vec::new(),
            focus_partition_plans: vec![FocusPartitionPlan {
                role_id: "consumer".to_string(),
                shared_baseline: vec!["bounded consumer task".to_string()],
                slots: vec![FocusPartitionSlot {
                    focus_id: "consume-handoff".to_string(),
                    boundary: "upstream terminal only".to_string(),
                    evidence_responsibility: "synthesize supplied evidence".to_string(),
                    capability_cropped_refs: vec!["read:crates/runtime".to_string()],
                    scope_hash: harness_contract::team::focus_scope_hash(
                        "consumer",
                        "upstream terminal only",
                        &["read:crates/runtime".to_string()],
                    ),
                    overlap_budget_bp: 0,
                    novelty_target_bp: 1,
                    output_contract: vec!["summary".to_string()],
                    output_acceptance: vec!["summary".to_string()],
                }],
            }],
            requires_managed_collaboration_escalation: false,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "test-model".to_string(),
            execution_budget: harness_contract::context::ParentExecutionBudget::new(
                "subgraph-team-budget",
                65_536,
                u64::MAX,
                32,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            resource_scopes: vec!["read:crates/runtime".to_string()],
            allow_whole_workspace_scope: false,
            upstream_evidence_refs: Vec::new(),
            upstream_artifact_refs: Vec::new(),
            execution_capacity: None,
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
                summary: Some(format!(
                    "Team A verified the runtime boundary. {} COMPLETE_TEAM_A_TAIL",
                    "evidence ".repeat(1_000)
                )),
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
        assert!(materialized.objective.contains("COMPLETE_TEAM_A_TAIL"));
        assert!(!materialized.objective.contains("truncated"));
        assert!(materialized
            .objective
            .contains("Team inline result remains available as a bounded summary."));
        assert!(materialized.focus_partition_plans[0]
            .shared_baseline
            .iter()
            .any(|item| item.contains("COMPLETE_TEAM_A_TAIL")));
    }
}
