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

/// Canonically reduces already-persisted protocol AgentRuntime returns.
///
/// The reducer has no scheduling or graph mutation privilege. It validates
/// each agent-to-graph binding and returns a single terminal candidate to the
/// V3 commit path, exactly like every other node executor.
pub struct ProtocolResultReducer {
    state_store: ExecutionGraphStateStore,
    agents: Arc<AgentRuntime>,
}

impl ProtocolResultReducer {
    #[must_use]
    pub fn new(state_store: ExecutionGraphStateStore, agents: Arc<AgentRuntime>) -> Self {
        Self {
            state_store,
            agents,
        }
    }
}

impl SynthesizeBackendResolver for ProtocolResultReducer {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
        ticket.payload_ref.starts_with("protocol:").then(|| {
            Arc::new(Self::new(
                self.state_store.clone(),
                Arc::clone(&self.agents),
            )) as Arc<dyn SynthesizeBackend>
        })
    }
}

#[async_trait]
impl SynthesizeBackend for ProtocolResultReducer {
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
                .map_err(|_| format!("protocol node {} is not an AgentTask packet", node.id))?;
            let returned = self
                .agents
                .terminal_return(&packet.agent_id)
                .ok_or_else(|| {
                    format!(
                        "protocol binding missing terminal AgentRuntime result for {}",
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
                    "protocol result binding mismatch for {}",
                    packet.agent_id
                ));
            }
            usage.input_tokens = usage.input_tokens.saturating_add(returned.input_tokens);
            usage.output_tokens = usage.output_tokens.saturating_add(returned.output_tokens);
            usage.tool_calls = usage.tool_calls.saturating_add(returned.tool_calls);
            evidence.extend(returned.evidence_refs.clone());
            let role = packet
                .constraints
                .iter()
                .find_map(|constraint| constraint.strip_prefix("protocol_role:"))
                .unwrap_or("agent");
            match returned.status {
                AgentTerminalStatus::Completed => {
                    summaries.push(format!("## {role}\n{}", returned.outcome))
                }
                AgentTerminalStatus::Failed
                | AgentTerminalStatus::Cancelled
                | AgentTerminalStatus::Blocked => blockers.push(format!(
                    "{role}: {}",
                    returned
                        .failure
                        .unwrap_or_else(|| "no terminal outcome".to_string())
                )),
            }
        }

        let (status, result_ref, failure) = if blockers.is_empty() {
            if summaries.is_empty() {
                return Err("protocol synthesis has no completed AgentRuntime results".to_string());
            }
            let answer = summaries.join("\n\n");
            (
                ExecutionNodeStatus::Completed,
                Some(format!(
                    "assistant_json:{}",
                    serde_json::to_string(&answer).map_err(|error| error.to_string())?
                )),
                None,
            )
        } else {
            (
                ExecutionNodeStatus::Blocked,
                None,
                Some(ExecutionFailure {
                    kind: "protocol_agent_terminal_failure".to_string(),
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
