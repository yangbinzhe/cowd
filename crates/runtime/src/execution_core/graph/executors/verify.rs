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
            service_class: context.graph.service_class,
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
                            | ExecutionEdgeKind::CrossTeamHandoff
                            | ExecutionEdgeKind::Produces
                            | ExecutionEdgeKind::Verifies
                    )
            })
            .map(|edge| edge.from.as_str())
            .collect::<Vec<_>>();

        let mut evidence = Vec::<EvidenceAccessRef>::new();
        let mut available = BTreeSet::new();
        let mut incomplete = Vec::new();
        let mut terminal_unsatisfied = Vec::new();
        let mut invalid_team_slots = Vec::new();
        let mut satisfied_team_criteria = BTreeSet::new();
        // A Team-level custom acceptance label is compiled by
        // `team_acceptance_contract` as scoped Runtime evidence. It is not a
        // request for a model to echo that label as a JSON key. Keep this
        // verifier aligned with that frozen contract.
        let mut has_durable_team_evidence = false;
        let team_verification = ticket.payload_ref.starts_with("team:");
        for predecessor_id in predecessor_ids {
            let predecessor_status = graph.node_statuses.get(predecessor_id).copied();
            if let Some(result) = graph.node_results.get(predecessor_id) {
                if let Some(result_ref) = &result.result_ref {
                    available.insert(result_ref.clone());
                }
                for item in &result.evidence_refs {
                    available.insert(item.evidence_ref.id.clone());
                    available.insert(item.retrieval_selector.clone());
                    available.insert(item.sha256.clone());
                    has_durable_team_evidence |=
                        crate::agent_result_validator::is_materialized_durable_evidence(item);
                    evidence.push(item.clone());
                }
            }
            if predecessor_status != Some(ExecutionNodeStatus::Completed) {
                if team_verification
                    && predecessor_status.is_some_and(|status| status.is_terminal())
                {
                    terminal_unsatisfied.push(format!(
                        "{predecessor_id}:{:?}",
                        predecessor_status.unwrap_or(ExecutionNodeStatus::Blocked)
                    ));
                } else {
                    incomplete.push(predecessor_id.to_string());
                }
                continue;
            }
            if let Some(result) = graph.node_results.get(predecessor_id) {
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
                        .filter(|edge| edge.to == predecessor_id && edge.kind.is_dependency())
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
                    let completed_acceptance = result
                        .usage
                        .acceptance_evaluation
                        .as_ref()
                        .is_some_and(|evaluation| {
                            evaluation.evaluator_revision
                                == crate::acceptance_evaluator::AcceptanceEvaluator::REVISION
                                && evaluation.verdict
                                    == harness_contract::acceptance::AcceptanceVerdict::Satisfied
                        });
                    // The frozen packet is the contract. Reading a serialized
                    // constraint here would let recovery and a live execution
                    // disagree about the Team evidence obligation.
                    let typed_requirements = (!packet.output_acceptance.is_empty())
                        .then(|| packet.output_acceptance.clone())
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
                    let role = packet.team_role_assignment();
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
                    if role.is_none() {
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
                    if completed_acceptance {
                        for criterion in &packet.acceptance {
                            satisfied_team_criteria.insert(criterion.to_ascii_lowercase());
                        }
                    }
                    // Team result fields (e.g. `key_decisions` /
                    // `unresolved_or_risks`) are owned by the Team contract,
                    // not by any single role's acceptance list. A completed
                    // role that actually materialized those fields in its
                    // durable terminal JSON satisfies the corresponding Team
                    // delivery criterion even when its own acceptance vector
                    // does not enumerate them (the convergence role carries
                    // the write obligation, not the full result contract).
                    for required in &node.acceptance.criteria {
                        let required_normalized = required.to_ascii_lowercase();
                        if satisfied_team_criteria.contains(&required_normalized) {
                            continue;
                        }
                        let summary = result.summary.as_deref().unwrap_or_default();
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(summary) {
                            let Some(object) = value.as_object() else {
                                continue;
                            };
                            let Some(field) = object.get(required.as_str()) else {
                                continue;
                            };
                            let materialized = match field {
                                serde_json::Value::Null => false,
                                serde_json::Value::String(value) => !value.trim().is_empty(),
                                serde_json::Value::Array(values) => !values.is_empty(),
                                serde_json::Value::Object(values) => !values.is_empty(),
                                serde_json::Value::Bool(_) | serde_json::Value::Number(_) => true,
                            };
                            if materialized {
                                satisfied_team_criteria.insert(required_normalized);
                            }
                        }
                    }
                }
            } else if team_verification {
                invalid_team_slots.push(format!("{predecessor_id}:missing_committed_result"));
            } else {
                incomplete.push(predecessor_id.to_string());
            }
        }

        // Unknown user-defined Team labels are evidence-backed by the typed
        // contract compiler. Once each role's own acceptance is satisfied,
        // use durable receipts rather than an optional free-form JSON field.
        if team_verification && has_durable_team_evidence && invalid_team_slots.is_empty() {
            for criterion in &node.acceptance.criteria {
                if is_runtime_evidence_backed_team_criterion(criterion) {
                    satisfied_team_criteria.insert(criterion.to_ascii_lowercase());
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
        let (status, failure) = if !incomplete.is_empty()
            || !terminal_unsatisfied.is_empty()
            || !missing.is_empty()
            || !invalid_team_slots.is_empty()
        {
            let mut detail = Vec::new();
            if !incomplete.is_empty() {
                detail.push(format!(
                    "incomplete predecessors: {}",
                    incomplete.join(", ")
                ));
            }
            if !terminal_unsatisfied.is_empty() {
                detail.push(format!(
                    "terminal unsatisfied predecessors: {}",
                    terminal_unsatisfied.join(", ")
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
                    kind: if team_verification && incomplete.is_empty() {
                        "team_delivery_unsatisfied".into()
                    } else {
                        "missing_evidence".into()
                    },
                    message: detail.join("; "),
                    // A Finally verifier runs only after every predecessor
                    // is terminal. Re-running cannot turn a failed branch
                    // into success; the envelope must preserve the partial
                    // verdict instead of creating an endless retry loop.
                    retryable: !incomplete.is_empty(),
                    evidence_refs: evidence.clone(),
                }),
            )
        } else {
            (ExecutionNodeStatus::Completed, None)
        };

        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref: Some(format!(
                "verification:{}:{}:{}",
                ticket.graph_id,
                ticket.node_id,
                if status == ExecutionNodeStatus::Completed {
                    "satisfied"
                } else {
                    "not_satisfied"
                }
            )),
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

/// Return whether a Team delivery criterion is compiled as Runtime-backed
/// scoped evidence rather than a structured presentation field.
///
/// This mirrors the explicit structured field set in
/// `team::instantiation::team_acceptance_contract`. Unknown user-defined
/// labels are intentionally evidence-backed there; requiring a matching JSON
/// property here would create a second, model-fragile acceptance language.
fn is_runtime_evidence_backed_team_criterion(criterion: &str) -> bool {
    !matches!(
        criterion.trim().to_ascii_lowercase().as_str(),
        "summary"
            | "findings"
            | "plan"
            | "risks"
            | "unresolved"
            | "key_decisions"
            | "unresolved_or_risks"
            | "proposal"
            | "critique"
            | "checkpoint"
            | "implementation"
            | "mitigation"
            | "source_verification"
            | "review"
            | "evidence"
    ) && !criterion.trim().starts_with("evidence_scope:")
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
    let fresh_runtime_tool_observed = result.usage.tool_calls > 0
        && !result
            .usage
            .observed_acceptance
            .observed_evidence
            .is_empty();
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
            EvidenceRef::observed("evidence", id),
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
        crate::test_support::attach_execution_graph_lineage(&mut graph);
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
        let graph = state.load(&graph.id).unwrap();
        let graph = commit
            .transition_node(
                &graph,
                &verify.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let graph = commit
            .transition_node(
                &graph,
                &verify.id,
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph.clone()),
                node: verify.clone(),
                attempt: 1,
            })
            .await
            .unwrap();
        let result = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(result.status, ExecutionNodeStatus::Completed);
        assert_eq!(result.evidence_refs.len(), 1);
    }

    #[tokio::test]
    async fn finally_verifier_preserves_terminal_failure_as_non_retryable_verdict() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let state = ExecutionGraphStateStore::new(Arc::clone(&store));
        let commit = ExecutionCommitService::new(store);
        let executor = VerifyNodeExecutor::new(state.clone());
        let mut graph = ExecutionGraph::new("verify terminal failure");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let source = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        let verify = ExecutionNodeSpec::new(
            ExecutionNodeKind::Verify,
            VerifyNodeExecutor::KIND,
            "team:fixture",
        );
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
                ExecutionNodeStatus::Failed,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::Failed,
                    result_ref: None,
                    summary: None,
                    evidence_refs: vec![evidence("failure-proof")],
                    failure: Some(ExecutionFailure {
                        kind: "fixture".to_string(),
                        message: "branch failed".to_string(),
                        retryable: false,
                        evidence_refs: Vec::new(),
                    }),
                    usage: ExecutionUsage::default(),
                    finished_at_ms: 1,
                }),
                Vec::new(),
            )
            .unwrap();
        let graph = state.load(&graph.id).unwrap();
        let graph = commit
            .transition_node(
                &graph,
                &verify.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let graph = commit
            .transition_node(
                &graph,
                &verify.id,
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph.clone()),
                node: verify.clone(),
                attempt: 1,
            })
            .await
            .unwrap();
        let result = executor.poll_or_await(&ticket).await.unwrap().result;

        assert_eq!(result.status, ExecutionNodeStatus::Blocked);
        assert!(result
            .result_ref
            .as_deref()
            .is_some_and(|reference| reference.ends_with(":not_satisfied")));
        assert_eq!(result.evidence_refs.len(), 1);
        assert!(!result.failure.as_ref().unwrap().retryable);
        let receipt = commit
            .transition_node(&graph, &verify.id, result.status, Some(result), Vec::new())
            .unwrap();
        assert!(receipt
            .graph
            .node_results
            .get(&verify.id)
            .and_then(|result| result.result_ref.as_deref())
            .is_some_and(|reference| reference.ends_with(":not_satisfied")));
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
                observed_acceptance: harness_contract::context::ObservedAcceptance {
                    satisfied_criteria: Vec::new(),
                    observed_evidence: vec![harness_contract::context::ObservedEvidence {
                        obligation_id: "fresh-read".to_string(),
                        target: harness_contract::context::EvidenceTargetIdentity::Network {
                            endpoint: "fixture".to_string(),
                        },
                        observed_at_sequence: 1,
                        tool_name: "read_file".to_string(),
                        provenance:
                            harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
                        evidence_ref: None,
                        workspace_prior_state: None,
                    }],
                    unresolved_obligation_ids: Vec::new(),
                },
                ..ExecutionUsage::default()
            },
            finished_at_ms: 1,
        };

        assert!(produced_team_evidence(
            &result,
            std::slice::from_ref(&shared),
            &upstream,
        ));

        result.usage.observed_acceptance.observed_evidence.clear();
        assert!(!produced_team_evidence(
            &result,
            std::slice::from_ref(&shared),
            &upstream,
        ));
    }

    #[test]
    fn custom_team_delivery_labels_are_evidence_backed_not_model_json_fields() {
        assert!(is_runtime_evidence_backed_team_criterion("evidence_paths"));
        assert!(is_runtime_evidence_backed_team_criterion(
            "findings_summary"
        ));
        assert!(is_runtime_evidence_backed_team_criterion(
            "user_defined_delivery"
        ));
        assert!(!is_runtime_evidence_backed_team_criterion("summary"));
        assert!(!is_runtime_evidence_backed_team_criterion("evidence"));
        assert!(!is_runtime_evidence_backed_team_criterion(
            "evidence_scope:read:src"
        ));
    }
}
