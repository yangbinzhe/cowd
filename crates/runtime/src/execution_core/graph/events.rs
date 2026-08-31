use std::collections::BTreeMap;

use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionGraph, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
    ExecutionOrchestrationMetadata, ExecutionParentBinding, ExecutionRecoveryCursor,
    ExecutionServiceClass, ExecutionWorkContract, ExecutionWorkRuntimeState,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionNodeBinding {
    pub executor_kind: String,
    pub ticket_idempotency_key: String,
    pub attempt: u32,
    pub resource_lease_refs: Vec<String>,
    pub scope_lease_ref: Option<String>,
    pub worktree_lease_ref: Option<String>,
}

/// Minimal reconstruction payload between durable graph checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphDelta {
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_class: Option<ExecutionServiceClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<Option<ExecutionParentBinding>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestration: Option<Option<ExecutionOrchestrationMetadata>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_binding:
        Option<Option<harness_contract::turn::CollaborationContinuationBinding>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_envelope: Option<Option<harness_contract::outcome::DeliveryEnvelope>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_presentation: Option<Option<harness_contract::outcome::TerminalPresentation>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_nodes: Vec<ExecutionNodeSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_node_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_edges: Vec<ExecutionEdge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_edges: Vec<ExecutionEdge>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_status_updates: BTreeMap<String, ExecutionNodeStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_node_statuses: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_result_updates: BTreeMap<String, ExecutionNodeResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_node_results: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub work_state_updates: BTreeMap<String, ExecutionWorkRuntimeState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_work_states: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub autonomous_work_updates: BTreeMap<String, ExecutionWorkContract>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_autonomous_work: Vec<String>,
    pub recovery_cursor: ExecutionRecoveryCursor,
}

impl ExecutionGraphDelta {
    #[must_use]
    pub fn between(previous: &ExecutionGraph, next: &ExecutionGraph) -> Self {
        Self {
            revision: next.revision,
            objective: (previous.objective != next.objective).then(|| next.objective.clone()),
            service_class: (previous.service_class != next.service_class)
                .then_some(next.service_class),
            parent_execution: (previous.parent_execution != next.parent_execution)
                .then(|| next.parent_execution.clone()),
            orchestration: (previous.orchestration != next.orchestration)
                .then(|| next.orchestration.clone()),
            continuation_binding: (previous.continuation_binding != next.continuation_binding)
                .then(|| next.continuation_binding.clone()),
            delivery_envelope: (previous.delivery_envelope != next.delivery_envelope)
                .then(|| next.delivery_envelope.clone()),
            terminal_presentation: (previous.terminal_presentation != next.terminal_presentation)
                .then(|| next.terminal_presentation.clone()),
            added_nodes: next
                .nodes
                .iter()
                .filter(|node| {
                    !previous
                        .nodes
                        .iter()
                        .any(|old| old.id == node.id && old == *node)
                })
                .cloned()
                .collect(),
            removed_node_ids: previous
                .nodes
                .iter()
                .filter(|node| !next.nodes.iter().any(|new| new.id == node.id))
                .map(|node| node.id.clone())
                .collect(),
            added_edges: next
                .edges
                .iter()
                .filter(|edge| !previous.edges.contains(edge))
                .cloned()
                .collect(),
            removed_edges: previous
                .edges
                .iter()
                .filter(|edge| !next.edges.contains(edge))
                .cloned()
                .collect(),
            node_status_updates: next
                .node_statuses
                .iter()
                .filter(|(id, status)| previous.node_statuses.get(*id) != Some(*status))
                .map(|(id, status)| (id.clone(), *status))
                .collect(),
            removed_node_statuses: previous
                .node_statuses
                .keys()
                .filter(|id| !next.node_statuses.contains_key(*id))
                .cloned()
                .collect(),
            node_result_updates: next
                .node_results
                .iter()
                .filter(|(id, result)| previous.node_results.get(*id) != Some(*result))
                .map(|(id, result)| (id.clone(), result.clone()))
                .collect(),
            removed_node_results: previous
                .node_results
                .keys()
                .filter(|id| !next.node_results.contains_key(*id))
                .cloned()
                .collect(),
            work_state_updates: next
                .work_states
                .iter()
                .filter(|(id, state)| previous.work_states.get(*id) != Some(*state))
                .map(|(id, state)| (id.clone(), state.clone()))
                .collect(),
            removed_work_states: previous
                .work_states
                .keys()
                .filter(|id| !next.work_states.contains_key(*id))
                .cloned()
                .collect(),
            autonomous_work_updates: next
                .autonomous_work
                .iter()
                .filter(|(id, work)| previous.autonomous_work.get(*id) != Some(*work))
                .map(|(id, work)| (id.clone(), work.clone()))
                .collect(),
            removed_autonomous_work: previous
                .autonomous_work
                .keys()
                .filter(|id| !next.autonomous_work.contains_key(*id))
                .cloned()
                .collect(),
            recovery_cursor: next.recovery_cursor.clone(),
        }
    }

    pub fn apply(&self, graph: &mut ExecutionGraph) -> Result<(), String> {
        let expected = graph.revision.saturating_add(1);
        if self.revision != expected {
            return Err(format!(
                "delta revision {} does not follow graph revision {}",
                self.revision, graph.revision
            ));
        }
        if let Some(objective) = &self.objective {
            graph.objective.clone_from(objective);
        }
        if let Some(service_class) = self.service_class {
            graph.service_class = service_class;
        }
        if let Some(parent_execution) = &self.parent_execution {
            graph.parent_execution.clone_from(parent_execution);
        }
        if let Some(orchestration) = &self.orchestration {
            graph.orchestration.clone_from(orchestration);
        }
        if let Some(binding) = &self.continuation_binding {
            graph.continuation_binding.clone_from(binding);
        }
        if let Some(envelope) = &self.delivery_envelope {
            graph.delivery_envelope.clone_from(envelope);
        }
        if let Some(presentation) = &self.terminal_presentation {
            graph.terminal_presentation.clone_from(presentation);
        }
        for id in &self.removed_node_ids {
            graph.nodes.retain(|node| node.id != *id);
        }
        for node in &self.added_nodes {
            graph.nodes.retain(|current| current.id != node.id);
            graph.nodes.push(node.clone());
        }
        graph
            .edges
            .retain(|edge| !self.removed_edges.contains(edge));
        for edge in &self.added_edges {
            if !graph.edges.contains(edge) {
                graph.edges.push(edge.clone());
            }
        }
        for id in &self.removed_node_statuses {
            graph.node_statuses.remove(id);
        }
        graph.node_statuses.extend(self.node_status_updates.clone());
        for id in &self.removed_node_results {
            graph.node_results.remove(id);
        }
        graph.node_results.extend(self.node_result_updates.clone());
        for id in &self.removed_work_states {
            graph.work_states.remove(id);
        }
        graph.work_states.extend(self.work_state_updates.clone());
        for id in &self.removed_autonomous_work {
            graph.autonomous_work.remove(id);
        }
        graph
            .autonomous_work
            .extend(self.autonomous_work_updates.clone());
        graph.recovery_cursor = self.recovery_cursor.clone();
        graph.revision = self.revision;
        Ok(())
    }

    /// Allocation-aware size estimate used only for checkpoint admission.
    ///
    /// This deliberately walks owned fields instead of serializing the delta
    /// on every graph commit. Durable encoding still happens exactly once
    /// when the selected event is appended.
    #[must_use]
    pub fn estimated_bytes(&self) -> u64 {
        let mut bytes = std::mem::size_of::<Self>()
            .saturating_add(self.objective.as_ref().map_or(0, String::len))
            .saturating_add(self.continuation_binding.as_ref().map_or(0, |binding| {
                binding.as_ref().map_or(0, |binding| {
                    binding.source_session_id.len()
                        + binding.source_turn_id.len()
                        + binding.source_root_id.len()
                        + binding.team_set_ref.len()
                        + binding.binding_digest.len()
                        + binding.current_ingress.len()
                        + binding.result_refs.iter().map(String::len).sum::<usize>()
                })
            }))
            .saturating_add(self.delivery_envelope.as_ref().map_or(0, |value| {
                value.as_ref().map_or(0, |envelope| {
                        envelope.envelope_id.len()
                            + envelope.objective_id.len()
                            + envelope.branch_terminals.len()
                                * std::mem::size_of::<
                                    harness_contract::outcome::DeliveryBranchTerminal,
                                >()
                            + envelope
                                .unresolved
                                .iter()
                                .map(|item| item.summary.len())
                                .sum::<usize>()
                    })
            }))
            .saturating_add(self.removed_node_ids.iter().map(String::len).sum::<usize>())
            .saturating_add(
                self.removed_node_statuses
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.removed_node_results
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.removed_work_states
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.removed_autonomous_work
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            );
        for node in &self.added_nodes {
            bytes = bytes
                .saturating_add(std::mem::size_of_val(node))
                .saturating_add(node.id.len())
                .saturating_add(node.payload_ref.len())
                .saturating_add(node.executor_kind.len())
                .saturating_add(node.idempotency_key.len())
                .saturating_add(node.resource_scopes.iter().map(String::len).sum::<usize>());
        }
        bytes = bytes.saturating_add(
            self.added_edges
                .iter()
                .chain(self.removed_edges.iter())
                .map(|edge| {
                    std::mem::size_of_val(edge)
                        .saturating_add(edge.from.len())
                        .saturating_add(edge.to.len())
                })
                .sum::<usize>(),
        );
        bytes = bytes.saturating_add(
            self.node_status_updates
                .keys()
                .map(String::len)
                .sum::<usize>(),
        );
        for (node_id, result) in &self.node_result_updates {
            bytes = bytes
                .saturating_add(node_id.len())
                .saturating_add(std::mem::size_of_val(result))
                .saturating_add(result.result_ref.as_ref().map_or(0, String::len))
                .saturating_add(result.summary.as_ref().map_or(0, String::len));
            if let Some(failure) = &result.failure {
                bytes = bytes
                    .saturating_add(failure.kind.len())
                    .saturating_add(failure.message.len());
            }
        }
        for (node_id, state) in &self.work_state_updates {
            bytes = bytes
                .saturating_add(node_id.len())
                .saturating_add(std::mem::size_of_val(state))
                .saturating_add(state.submission_ref.as_ref().map_or(0, String::len))
                .saturating_add(state.review_findings.iter().map(String::len).sum::<usize>())
                .saturating_add(
                    state
                        .reviews
                        .iter()
                        .map(|review| {
                            review.reviewer_instance_id.len()
                                + review.reviewer_role_id.as_ref().map_or(0, String::len)
                                + review.submission_ref.len()
                                + review.finding.as_ref().map_or(0, String::len)
                        })
                        .sum::<usize>(),
                )
                .saturating_add(
                    state
                        .bids
                        .iter()
                        .map(|bid| {
                            bid.bidder_instance_id.len()
                                + bid.bidder_role_id.as_ref().map_or(0, String::len)
                                + bid.rationale.len()
                        })
                        .sum::<usize>(),
                );
            if let Some(claim) = &state.claim {
                bytes = bytes
                    .saturating_add(claim.claimant_instance_id.len())
                    .saturating_add(claim.claimant_role_id.as_ref().map_or(0, String::len))
                    .saturating_add(claim.claim_token.len());
            }
        }
        for (work_id, work) in &self.autonomous_work_updates {
            bytes = bytes
                .saturating_add(work_id.len())
                .saturating_add(std::mem::size_of_val(work))
                .saturating_add(work.objective.as_ref().map_or(0, String::len))
                .saturating_add(work.proposed_by.as_ref().map_or(0, String::len))
                .saturating_add(
                    work.proposal_evidence_refs
                        .iter()
                        .map(String::len)
                        .sum::<usize>(),
                );
        }
        u64::try_from(bytes).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ExecutionGraphEvent {
    Planned {
        graph: ExecutionGraph,
    },
    Checkpoint {
        cause: String,
        graph: ExecutionGraph,
    },
    NodeTransitioned {
        node_id: String,
        from: ExecutionNodeStatus,
        to: ExecutionNodeStatus,
        result: Option<ExecutionNodeResult>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binding: Option<ExecutionNodeBinding>,
        delta: ExecutionGraphDelta,
    },
    NodesStarted {
        bindings: BTreeMap<String, ExecutionNodeBinding>,
        delta: ExecutionGraphDelta,
    },
    NodesTransitioned {
        node_ids: Vec<String>,
        delta: ExecutionGraphDelta,
    },
    NodeTransitionedAndReplanned {
        node_id: String,
        from: ExecutionNodeStatus,
        to: ExecutionNodeStatus,
        result: ExecutionNodeResult,
        reason: String,
        added_node_ids: Vec<String>,
        delta: ExecutionGraphDelta,
    },
    CommandApplied {
        command: String,
        reason: Option<String>,
        delta: ExecutionGraphDelta,
    },
    Replanned {
        reason: String,
        added_node_ids: Vec<String>,
        delta: ExecutionGraphDelta,
    },
    Recovered {
        recovered_nodes: Vec<String>,
        blocked_nodes: Vec<String>,
        delta: ExecutionGraphDelta,
    },
}

impl ExecutionGraphEvent {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Planned { .. } => "execution_graph.planned",
            Self::Checkpoint { .. } => "execution_graph.checkpoint",
            Self::NodeTransitioned { .. } => "execution_graph.node_transitioned",
            Self::NodesStarted { .. } => "execution_graph.nodes_started",
            Self::NodesTransitioned { .. } => "execution_graph.nodes_transitioned",
            Self::NodeTransitionedAndReplanned { .. } => {
                "execution_graph.node_transitioned_and_replanned"
            }
            Self::CommandApplied { .. } => "execution_graph.command_applied",
            Self::Replanned { .. } => "execution_graph.replanned",
            Self::Recovered { .. } => "execution_graph.recovered",
        }
    }

    pub fn project(&self, current: Option<ExecutionGraph>) -> Result<ExecutionGraph, String> {
        match self {
            Self::Planned { graph } | Self::Checkpoint { graph, .. } => Ok(graph.clone()),
            Self::NodeTransitioned { delta, .. }
            | Self::NodesStarted { delta, .. }
            | Self::NodesTransitioned { delta, .. }
            | Self::NodeTransitionedAndReplanned { delta, .. }
            | Self::CommandApplied { delta, .. }
            | Self::Replanned { delta, .. }
            | Self::Recovered { delta, .. } => {
                let mut graph =
                    current.ok_or_else(|| "graph delta has no preceding snapshot".to_string())?;
                delta.apply(&mut graph)?;
                Ok(graph)
            }
        }
    }

    #[must_use]
    pub fn estimated_delta_bytes(&self) -> u64 {
        let bytes = match self {
            Self::Planned { graph } | Self::Checkpoint { graph, .. } => {
                return crate::execution_core::hot_state::estimate_graph_bytes(graph);
            }
            Self::NodeTransitioned {
                node_id,
                result,
                binding,
                delta,
                ..
            } => std::mem::size_of::<Self>()
                .saturating_add(node_id.len())
                .saturating_add(result.as_ref().map_or(0, |value| {
                    value
                        .summary
                        .as_ref()
                        .map_or(0, String::len)
                        .saturating_add(value.result_ref.as_ref().map_or(0, String::len))
                }))
                .saturating_add(binding.as_ref().map_or(0, |value| {
                    value
                        .executor_kind
                        .len()
                        .saturating_add(value.ticket_idempotency_key.len())
                }))
                .saturating_add(usize::try_from(delta.estimated_bytes()).unwrap_or(usize::MAX)),
            Self::NodesStarted { bindings, delta } => std::mem::size_of::<Self>()
                .saturating_add(
                    bindings
                        .iter()
                        .map(|(node_id, binding)| {
                            node_id.len()
                                + binding.executor_kind.len()
                                + binding.ticket_idempotency_key.len()
                                + binding
                                    .resource_lease_refs
                                    .iter()
                                    .map(String::len)
                                    .sum::<usize>()
                        })
                        .sum::<usize>(),
                )
                .saturating_add(usize::try_from(delta.estimated_bytes()).unwrap_or(usize::MAX)),
            Self::NodesTransitioned { node_ids, delta } => std::mem::size_of::<Self>()
                .saturating_add(node_ids.iter().map(String::len).sum::<usize>())
                .saturating_add(usize::try_from(delta.estimated_bytes()).unwrap_or(usize::MAX)),
            Self::NodeTransitionedAndReplanned {
                node_id,
                reason,
                added_node_ids,
                delta,
                ..
            } => std::mem::size_of::<Self>()
                .saturating_add(node_id.len())
                .saturating_add(reason.len())
                .saturating_add(added_node_ids.iter().map(String::len).sum::<usize>())
                .saturating_add(usize::try_from(delta.estimated_bytes()).unwrap_or(usize::MAX)),
            Self::CommandApplied {
                command,
                reason,
                delta,
            } => std::mem::size_of::<Self>()
                .saturating_add(command.len())
                .saturating_add(reason.as_ref().map_or(0, String::len))
                .saturating_add(usize::try_from(delta.estimated_bytes()).unwrap_or(usize::MAX)),
            Self::Replanned {
                reason,
                added_node_ids,
                delta,
            } => std::mem::size_of::<Self>()
                .saturating_add(reason.len())
                .saturating_add(added_node_ids.iter().map(String::len).sum::<usize>())
                .saturating_add(usize::try_from(delta.estimated_bytes()).unwrap_or(usize::MAX)),
            Self::Recovered {
                recovered_nodes,
                blocked_nodes,
                delta,
            } => std::mem::size_of::<Self>()
                .saturating_add(recovered_nodes.iter().map(String::len).sum::<usize>())
                .saturating_add(blocked_nodes.iter().map(String::len).sum::<usize>())
                .saturating_add(usize::try_from(delta.estimated_bytes()).unwrap_or(usize::MAX)),
        };
        u64::try_from(bytes).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{
        ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
    };

    use super::*;

    #[test]
    fn delivery_and_presentation_survive_delta_replay() {
        let mut previous = ExecutionGraph::new("terminal replay");
        previous.revision = 3;
        let mut next = previous.clone();
        next.revision = 4;
        next.delivery_envelope = Some(harness_contract::outcome::DeliveryEnvelope {
            envelope_id: "envelope-4".to_string(),
            revision: 4,
            objective_id: "goal-1".to_string(),
            pipeline_status: harness_contract::outcome::PipelineStatus::Completed,
            delivery_status: harness_contract::outcome::DeliveryStatus::Partial,
            branch_terminals: Vec::new(),
            verified_receipts: Vec::new(),
            verified_artifacts: Vec::new(),
            verified_effects: Vec::new(),
            workspace_materializations: Vec::new(),
            coverage: Default::default(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
            cancellation: None,
            user_answer_contract: Default::default(),
            created_at_ms: 10,
        });
        next.terminal_presentation = Some(harness_contract::outcome::TerminalPresentation {
            presentation_id: "presentation-4".to_string(),
            attempt_id: "attempt-1".to_string(),
            envelope_id: "envelope-4".to_string(),
            envelope_revision: 4,
            state: harness_contract::outcome::TerminalPresentationState::Committed,
            answer_origin: harness_contract::outcome::AnswerOrigin::TerminalNarrator,
            source_execution_id: Some(next.id.clone()),
            narrator_model: Some("model".to_string()),
            narrator_provider: Some("provider".to_string()),
            models_attempted: Vec::new(),
            validation: Default::default(),
            fallback_reason: None,
            generated_at_ms: 11,
            committed_at_ms: Some(12),
        });

        let delta = ExecutionGraphDelta::between(&previous, &next);
        delta.apply(&mut previous).unwrap();
        assert_eq!(previous.delivery_envelope, next.delivery_envelope);
        assert_eq!(previous.terminal_presentation, next.terminal_presentation);
    }

    #[test]
    fn delta_round_trip_reconstructs_next_graph() {
        let mut previous = ExecutionGraph::new("test");
        previous.revision = 1;
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "model", "{}");
        previous.nodes.push(node.clone());
        previous
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        let mut next = previous.clone();
        next.revision = 2;
        next.node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Running);
        next.recovery_cursor.node_attempts.insert(node.id, 1);
        let delta = ExecutionGraphDelta::between(&previous, &next);
        let mut projected = previous;
        delta.apply(&mut projected).unwrap();
        assert_eq!(projected, next);
        assert!(serde_json::to_value(delta).unwrap().get("nodes").is_none());
    }
}
