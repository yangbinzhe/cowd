use std::collections::BTreeSet;

use async_trait::async_trait;
use harness_contract::context::EvidenceAccessRef;
use harness_contract::execution_graph::{
    ExecutionEdgeKind, ExecutionFailure, ExecutionNodeResult, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionUsage,
};

use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
    NodeExecutor, NodeExecutorError,
};

/// Verifies one node from the durable results of its graph predecessors.
///
/// Verification has no caller-managed side channel: recovery and a live run
/// therefore evaluate exactly the same committed evidence.
pub struct VerifyNodeExecutor {
    state_store: ExecutionGraphStateStore,
}

impl VerifyNodeExecutor {
    pub const KIND: &'static str = "verify";

    #[must_use]
    pub fn new(state_store: ExecutionGraphStateStore) -> Self {
        Self { state_store }
    }
}

#[async_trait]
impl NodeExecutor for VerifyNodeExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind == Self::KIND {
            Ok(())
        } else {
            Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Verify must use canonical verify executor".into(),
            })
        }
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: Self::KIND.into(),
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let graph = self
            .state_store
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == ticket.node_id)
            .ok_or_else(|| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "verify node is absent from its execution graph".into(),
            })?;
        let predecessor_ids = graph
            .edges
            .iter()
            .filter(|edge| {
                edge.to == ticket.node_id
                    && matches!(
                        edge.kind,
                        ExecutionEdgeKind::DependsOn
                            | ExecutionEdgeKind::Produces
                            | ExecutionEdgeKind::Verifies
                    )
            })
            .map(|edge| edge.from.as_str())
            .collect::<Vec<_>>();

        let mut evidence = Vec::<EvidenceAccessRef>::new();
        let mut available = BTreeSet::new();
        let mut incomplete = Vec::new();
        let mut invalid_team_slots = Vec::new();
        let mut satisfied_team_criteria = BTreeSet::new();
        let team_verification = ticket.payload_ref.starts_with("team:");
        for predecessor_id in predecessor_ids {
            if graph.node_statuses.get(predecessor_id) != Some(&ExecutionNodeStatus::Completed) {
                incomplete.push(predecessor_id.to_string());
                continue;
            }
            if let Some(result) = graph.node_results.get(predecessor_id) {
                if let Some(result_ref) = &result.result_ref {
                    available.insert(result_ref.clone());
                }
                for item in &result.evidence_refs {
                    available.insert(item.evidence_ref.id.clone());
                    available.insert(item.retrieval_selector.clone());
                    available.insert(item.sha256.clone());
                    evidence.push(item.clone());
                }
                if team_verification {
                    let Some(predecessor_node) = graph
                        .nodes
                        .iter()
                        .find(|candidate| candidate.id == predecessor_id)
                    else {
                        invalid_team_slots.push(format!("{predecessor_id}:missing_node_contract"));
                        continue;
                    };
                    let Ok(packet) = serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(
                        &predecessor_node.payload_ref,
                    ) else {
                        invalid_team_slots.push(format!("{predecessor_id}:invalid_agent_packet"));
                        continue;
                    };
                    let upstream_evidence = graph
                        .edges
                        .iter()
                        .filter(|edge| {
                            edge.to == predecessor_id && edge.kind == ExecutionEdgeKind::DependsOn
                        })
                        .filter_map(|edge| graph.node_results.get(&edge.from))
                        .flat_map(|result| result.evidence_refs.iter())
                        .filter(|reference| {
                            crate::agent_result_validator::is_materialized_durable_evidence(
                                reference,
                            )
                        })
                        .map(|reference| {
                            (
                                reference.evidence_ref.ref_type.clone(),
                                reference.evidence_ref.id.clone(),
                            )
                        })
                        .collect::<BTreeSet<_>>();
                    let produced_evidence =
                        produced_team_evidence(result, &packet.evidence_refs, &upstream_evidence);
                    let completed_acceptance = packet.acceptance.iter().all(|criterion| {
                        result.evidence_refs.iter().any(|reference| {
                            reference.evidence_ref.ref_type == "runtime_acceptance"
                                && reference.evidence_ref.id
                                    == crate::execution_core::graph::executors::agent::acceptance_marker_id(
                                        predecessor_id,
                                        criterion,
                                    )
                        })
                    });
                    let typed_requirements = packet
                        .constraints
                        .iter()
                        .find_map(|constraint| constraint.strip_prefix("team_acceptance_contract:"))
                        .and_then(|value| {
                            serde_json::from_str::<
                                Vec<harness_contract::team::TeamAcceptanceRequirement>,
                            >(value)
                            .ok()
                        })
                        .filter(|requirements| {
                            requirements.len() == packet.acceptance.len()
                                && requirements.iter().all(|requirement| {
                                    packet.acceptance.contains(&requirement.criterion)
                                })
                        });
                    let typed_acceptance = typed_requirements.is_some();
                    let requires_new_tool_evidence = typed_requirements
                        .as_ref()
                        .is_some_and(|requirements| {
                            requirements.iter().any(|requirement| {
                                matches!(
                                    &requirement.check,
                                    harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                                        ..
                                    } | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange {
                                        ..
                                    } | harness_contract::team::TeamAcceptanceCheck::SourceVerification {
                                        ..
                                    } | harness_contract::team::TeamAcceptanceCheck::UpstreamReview
                                        | harness_contract::team::TeamAcceptanceCheck::LegacyEvidenceBound {
                                            ..
                                        }
                                )
                            })
                        });
                    let consumes_upstream =
                        typed_requirements.as_ref().is_some_and(|requirements| {
                            requirements.iter().any(|requirement| {
                                requirement.check
                                    == harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence
                            })
                        });
                    let retained_upstream = consumes_upstream
                        && !upstream_evidence.is_empty()
                        && upstream_evidence.iter().any(|(ref_type, id)| {
                            result.evidence_refs.iter().any(|reference| {
                                reference.evidence_ref.ref_type == ref_type.as_str()
                                    && reference.evidence_ref.id == id.as_str()
                            })
                        });
                    let evidence_satisfied =
                        (requires_new_tool_evidence && produced_evidence) || retained_upstream;
                    let role = packet_constraint(&packet, "team_role:");
                    let focus = packet_constraint(&packet, "focus_partition:");
                    let focus_hash = packet_constraint(&packet, "focus_scope_hash:");
                    let evidence_responsibility =
                        packet_constraint(&packet, "focus_evidence_responsibility:");
                    if requires_new_tool_evidence && result.usage.tool_calls == 0 {
                        invalid_team_slots.push(format!("{predecessor_id}:zero_tool_calls"));
                    }
                    if !evidence_satisfied {
                        invalid_team_slots.push(format!(
                            "{predecessor_id}:missing_required_durable_evidence"
                        ));
                    }
                    if !completed_acceptance {
                        invalid_team_slots
                            .push(format!("{predecessor_id}:acceptance_not_runtime_satisfied"));
                    }
                    if !typed_acceptance {
                        invalid_team_slots.push(format!(
                            "{predecessor_id}:missing_typed_acceptance_contract"
                        ));
                    }
                    if role.is_none()
                        || focus.is_none()
                        || focus_hash.is_none()
                        || evidence_responsibility.is_none()
                    {
                        invalid_team_slots
                            .push(format!("{predecessor_id}:incomplete_role_focus_contract"));
                    }
                    if result
                        .summary
                        .as_deref()
                        .map(str::trim)
                        .is_none_or(str::is_empty)
                    {
                        invalid_team_slots.push(format!("{predecessor_id}:missing_summary"));
                    } else {
                        satisfied_team_criteria.insert("summary".to_string());
                    }
                    if evidence_satisfied {
                        satisfied_team_criteria.insert("evidence".to_string());
                    }
                    for criterion in &packet.acceptance {
                        if result.evidence_refs.iter().any(|reference| {
                            reference.evidence_ref.ref_type == "runtime_acceptance"
                                && reference.evidence_ref.id
                                    == crate::execution_core::graph::executors::agent::acceptance_marker_id(
                                        predecessor_id,
                                        criterion,
                                    )
                        }) {
                            satisfied_team_criteria.insert(criterion.to_ascii_lowercase());
                        }
                    }
                }
            }
        }

        let mut missing = node
            .acceptance
            .required_evidence
            .iter()
            .filter(|required| !available.contains(required.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if team_verification {
            missing.extend(node.acceptance.criteria.iter().filter_map(|required| {
                let required_normalized = required.to_ascii_lowercase();
                (!satisfied_team_criteria.contains(&required_normalized))
                    .then(|| format!("team_contract:{required}"))
            }));
        }
        let (status, failure) =
            if !incomplete.is_empty() || !missing.is_empty() || !invalid_team_slots.is_empty() {
                let mut detail = Vec::new();
                if !incomplete.is_empty() {
                    detail.push(format!(
                        "incomplete predecessors: {}",
                        incomplete.join(", ")
                    ));
                }
                if !missing.is_empty() {
                    detail.push(format!("missing required evidence: {}", missing.join(", ")));
                }
                if !invalid_team_slots.is_empty() {
                    detail.push(format!(
                        "invalid Team role slots: {}",
                        invalid_team_slots.join(", ")
                    ));
                }
                (
                    ExecutionNodeStatus::Blocked,
                    Some(ExecutionFailure {
                        kind: "missing_evidence".into(),
                        message: detail.join("; "),
                        retryable: true,
                        evidence_refs: evidence.clone(),
                    }),
                )
            } else {
                (ExecutionNodeStatus::Completed, None)
            };

        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref: (status == ExecutionNodeStatus::Completed).then(|| {
                format!(
                    "verification:{}:{}:satisfied",
                    ticket.graph_id, ticket.node_id
                )
            }),
            summary: failure
                .as_ref()
                .map(|failure| failure.message.clone())
                .or_else(|| Some("Required evidence was verified".to_string())),
            evidence_refs: evidence,
            failure,
            usage: ExecutionUsage::default(),
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
}

fn packet_constraint(
    packet: &harness_contract::agent::AgentTaskPacket,
    prefix: &str,
) -> Option<String> {
    packet
        .constraints
        .iter()
        .find_map(|constraint| constraint.strip_prefix(prefix))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn produced_team_evidence(
    result: &ExecutionNodeResult,
    input_evidence: &[EvidenceAccessRef],
    upstream_evidence: &BTreeSet<(String, String)>,
) -> bool {
    // Content-addressed verification reads can legitimately retain the same
    // EvidenceRef as their upstream input. The Agent executor carries the
    // Runtime-derived tool/scoped usage into the committed node result, so a
    // fresh scoped tool observation proves reacquisition without weakening
    // the durable evidence requirement.
    let fresh_runtime_tool_observed =
        result.usage.tool_calls > 0 && !result.usage.runtime_observed_resource_scopes.is_empty();
    result.evidence_refs.iter().any(|reference| {
        crate::agent_result_validator::is_materialized_durable_evidence(reference)
            && (fresh_runtime_tool_observed
                || (!input_evidence
                    .iter()
                    .any(|input| input.evidence_ref == reference.evidence_ref)
                    && !upstream_evidence.contains(&(
                        reference.evidence_ref.ref_type.clone(),
                        reference.evidence_ref.id.clone(),
                    ))))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_core::graph::ExecutionCommitService;
    use crate::RuntimeEventStore;
    use harness_contract::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionNodeKind};
    use harness_contract::reality::EvidenceRef;
    use std::sync::Arc;

    fn evidence(id: &str) -> EvidenceAccessRef {
        EvidenceAccessRef::durable(
            EvidenceRef::new("evidence", id),
            "sha",
            1,
            "text/plain",
            format!("evidence://{id}"),
            "workspace",
        )
    }

    #[tokio::test]
    async fn verification_reads_committed_predecessor_evidence() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let state = ExecutionGraphStateStore::new(Arc::clone(&store));
        let commit = ExecutionCommitService::new(store);
        let executor = VerifyNodeExecutor::new(state.clone());
        let mut graph = ExecutionGraph::new("verify");
        let source = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool", "tool");
        let mut verify = ExecutionNodeSpec::new(
            ExecutionNodeKind::Verify,
            VerifyNodeExecutor::KIND,
            "verify",
        );
        verify.acceptance.required_evidence = vec!["proof".into()];
        graph.edges.push(ExecutionEdge {
            from: source.id.clone(),
            to: verify.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.nodes = vec![source.clone(), verify.clone()];
        let graph = commit.register_graph(graph).unwrap().graph;
        let graph = commit
            .transition_node(
                &graph,
                &source.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let graph = commit
            .transition_node(
                &graph,
                &source.id,
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        commit
            .transition_node(
                &graph,
                &source.id,
                ExecutionNodeStatus::Completed,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: None,
                    summary: Some("Verification fixture completed".to_string()),
                    evidence_refs: vec![evidence("proof")],
                    failure: None,
                    usage: ExecutionUsage::default(),
                    finished_at_ms: 1,
                }),
                Vec::new(),
            )
            .unwrap();
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(state.load(&graph.id).unwrap()),
                node: verify,
                attempt: 1,
            })
            .await
            .unwrap();
        let result = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(result.status, ExecutionNodeStatus::Completed);
        assert_eq!(result.evidence_refs.len(), 1);
    }

    #[test]
    fn team_verification_accepts_fresh_scoped_read_with_same_evidence_ref() {
        let shared = evidence("same-content");
        let upstream = BTreeSet::from([(
            shared.evidence_ref.ref_type.clone(),
            shared.evidence_ref.id.clone(),
        )]);
        let mut result = ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some("agent-return:reviewer".to_string()),
            summary: Some("reviewed".to_string()),
            evidence_refs: vec![shared.clone()],
            failure: None,
            usage: ExecutionUsage {
                tool_calls: 1,
                runtime_observed_resource_scopes: vec!["read:src".to_string()],
                ..ExecutionUsage::default()
            },
            finished_at_ms: 1,
        };

        assert!(produced_team_evidence(
            &result,
            std::slice::from_ref(&shared),
            &upstream,
        ));

        result.usage.runtime_observed_resource_scopes.clear();
        assert!(!produced_team_evidence(
            &result,
            std::slice::from_ref(&shared),
            &upstream,
        ));
    }
}
