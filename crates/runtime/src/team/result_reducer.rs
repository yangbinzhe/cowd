use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::agent::{AgentTaskPacket, AgentTerminalStatus};
use harness_contract::execution_graph::{
    ExecutionFailure, ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
};

use crate::execution_core::graph::executors::{SynthesizeBackend, SynthesizeBackendResolver};
use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionOutcome, NodeExecutionTicket,
};
use crate::AgentRuntime;

/// Reduces only durable AgentRuntime terminal packets already bound to the
/// graph. It returns an outcome to ExecutionCommitService and never mutates a
/// graph itself.
pub struct TeamResultReducer {
    state_store: ExecutionGraphStateStore,
    agents: Arc<AgentRuntime>,
}

impl TeamResultReducer {
    #[must_use]
    pub fn new(state_store: ExecutionGraphStateStore, agents: Arc<AgentRuntime>) -> Self {
        Self {
            state_store,
            agents,
        }
    }
}

impl SynthesizeBackendResolver for TeamResultReducer {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
        ticket.payload_ref.starts_with("team:").then(|| {
            Arc::new(Self::new(
                self.state_store.clone(),
                Arc::clone(&self.agents),
            )) as Arc<dyn SynthesizeBackend>
        })
    }
}

#[async_trait]
impl SynthesizeBackend for TeamResultReducer {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let graph = self
            .state_store
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| error.to_string())?;
        let mut summaries = Vec::new();
        let mut supporting_outputs = Vec::new();
        let mut evidence = Vec::new();
        let mut usage = ExecutionUsage::default();
        let mut blockers = Vec::new();
        let mut allows_unresolved = false;
        let terminal_agent_nodes = terminal_agent_node_ids(&graph);

        for node in graph.nodes.iter().filter(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
        }) {
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|_| format!("team node {} is not an AgentTask packet", node.id))?;
            let returned = self
                .agents
                .terminal_return(&packet.agent_id)
                .ok_or_else(|| {
                    format!(
                        "team binding missing terminal AgentRuntime result for {}",
                        packet.agent_id
                    )
                })?;
            if returned.run_id != packet.run_id
                || returned.graph_id != graph.id
                || returned.node_id != node.id
                || returned.attempt != packet.attempt
                || returned.expected_graph_revision != packet.expected_graph_revision
            {
                return Err(format!(
                    "team result binding mismatch for {}",
                    packet.agent_id
                ));
            }
            usage.input_tokens = usage.input_tokens.saturating_add(returned.input_tokens);
            usage.output_tokens = usage.output_tokens.saturating_add(returned.output_tokens);
            usage.tool_calls = usage.tool_calls.saturating_add(returned.tool_calls);
            evidence.extend(returned.evidence_refs.clone());
            allows_unresolved |= packet
                .constraints
                .iter()
                .any(|constraint| constraint == "protocol_allows_unresolved:true");
            match returned.status {
                AgentTerminalStatus::Completed => {
                    if terminal_agent_nodes.contains(&node.id) {
                        summaries.push(format!("## {}\n{}", packet.agent_id, returned.outcome));
                    } else {
                        supporting_outputs
                            .push(format!("## {}\n{}", packet.agent_id, returned.outcome));
                    }
                }
                AgentTerminalStatus::Failed
                | AgentTerminalStatus::Cancelled
                | AgentTerminalStatus::Blocked => blockers.push(format!(
                    "{}: {}",
                    packet.agent_id,
                    returned
                        .failure
                        .unwrap_or_else(|| "no terminal outcome".into())
                )),
            }
        }

        if summaries.is_empty() {
            return Err("team synthesis has no completed AgentRuntime results".into());
        }
        let (status, result_ref, failure) = if !blockers.is_empty() && !allows_unresolved {
            (
                ExecutionNodeStatus::Blocked,
                None,
                Some(ExecutionFailure {
                    kind: "team_agent_terminal_failure".into(),
                    message: blockers.join("; "),
                    retryable: false,
                    evidence_refs: evidence.clone(),
                }),
            )
        } else {
            // Protocol teams explicitly permit incomplete lanes. Preserve the
            // lifecycle failure on those agents, but publish the independent
            // completed evidence and name the gap for the parent turn. This
            // keeps a single unavailable synthesis worker from erasing a
            // real, auditable team result.
            let mut final_answer = String::new();
            if !supporting_outputs.is_empty() {
                final_answer.push_str("# Committed supporting role outputs\n\n");
                final_answer.push_str(&supporting_outputs.join("\n\n"));
                final_answer.push_str("\n\n# Terminal review/synthesis\n\n");
            }
            final_answer.push_str(&summaries.join("\n\n"));
            if !blockers.is_empty() {
                final_answer.push_str("\n\n## Unresolved team role outcomes\n");
                for blocker in blockers {
                    final_answer.push_str("- ");
                    final_answer.push_str(&blocker);
                    final_answer.push('\n');
                }
            }
            (
                ExecutionNodeStatus::Completed,
                Some(format!(
                    "assistant_json:{}",
                    serde_json::to_string(&final_answer).map_err(|error| error.to_string())?
                )),
                None,
            )
        };
        let summary = failure
            .as_ref()
            .map(|failure| failure.message.clone())
            .or_else(|| {
                result_ref
                    .as_deref()
                    .and_then(|value| value.strip_prefix("assistant_json:"))
                    .and_then(|value| serde_json::from_str::<String>(value).ok())
            });
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref,
            summary,
            evidence_refs: evidence,
            failure,
            usage,
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
}

fn terminal_agent_node_ids(
    graph: &harness_contract::execution_graph::ExecutionGraph,
) -> BTreeSet<String> {
    let agent_nodes = graph
        .nodes
        .iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    agent_nodes
        .iter()
        .filter(|node_id| {
            !graph.edges.iter().any(|edge| {
                edge.from == **node_id
                    && edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::DependsOn
                    && agent_nodes.contains(edge.to.as_str())
            })
        })
        .map(|node_id| (*node_id).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
    };

    use super::terminal_agent_node_ids;

    #[test]
    fn only_topology_terminal_agent_publishes_the_team_answer() {
        let mut graph = ExecutionGraph::new("research");
        let mut researcher =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        researcher.id = "researcher".into();
        let mut synthesizer =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        synthesizer.id = "synthesizer".into();
        graph.nodes.extend([researcher, synthesizer]);
        graph.edges.push(ExecutionEdge {
            from: "researcher".into(),
            to: "synthesizer".into(),
            kind: ExecutionEdgeKind::DependsOn,
        });

        assert_eq!(
            terminal_agent_node_ids(&graph),
            std::collections::BTreeSet::from(["synthesizer".to_string()])
        );
    }
}
