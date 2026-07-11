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
        let mut evidence = Vec::new();
        let mut usage = ExecutionUsage::default();
        let mut blockers = Vec::new();

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
            match returned.status {
                AgentTerminalStatus::Completed => {
                    summaries.push(format!("## {}\n{}", packet.agent_id, returned.outcome))
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

        let (status, result_ref, failure) = if blockers.is_empty() {
            if summaries.is_empty() {
                return Err("team synthesis has no completed AgentRuntime results".into());
            }
            let final_answer = summaries.join("\n\n");
            (
                ExecutionNodeStatus::Completed,
                Some(format!(
                    "assistant_json:{}",
                    serde_json::to_string(&final_answer).map_err(|error| error.to_string())?
                )),
                None,
            )
        } else {
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
        };
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref,
            evidence_refs: evidence,
            failure,
            usage,
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
}
