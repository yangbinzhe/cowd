use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::agent::{AgentTaskPacket, AgentTerminalStatus};
use harness_contract::execution_graph::{ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage};

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
        let mut allows_unresolved = false;

        for node in graph.nodes.iter().filter(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
        }) {
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|_| format!("protocol node {} is not an AgentTask packet", node.id))?;
            let returned = self
                .agents
                .terminal_return(packet.agent_id())
                .ok_or_else(|| {
                    format!(
                        "protocol binding missing terminal AgentRuntime result for {}",
                        packet.agent_id()
                    )
                })?;
            if returned.run_id != packet.run_id()
                || returned.graph_id != graph.id
                || returned.node_id != node.id
                || returned.attempt != packet.attempt
                || returned.expected_graph_revision != packet.expected_graph_revision
            {
                return Err(format!(
                    "protocol result binding mismatch for {}",
                    packet.agent_id()
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
            allows_unresolved |= packet
                .constraints
                .iter()
                .any(|constraint| constraint == "protocol_allows_unresolved:true");
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

        if summaries.is_empty() {
            return Err("protocol synthesis has no completed AgentRuntime results".to_string());
        }
        if !blockers.is_empty() && !allows_unresolved {
            return Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Blocked,
                result_ref: None,
                summary: Some(blockers.join("; ")),
                evidence_refs: evidence.clone(),
                failure: Some(harness_contract::execution_graph::ExecutionFailure {
                    kind: "protocol_agent_terminal_failure".to_string(),
                    message: blockers.join("; "),
                    retryable: false,
                    evidence_refs: evidence,
                }),
                usage,
                finished_at_ms: crate::tool_invocation::now_ms(),
            }));
        }
        // A protocol that explicitly allows unresolved findings must converge
        // with the completed lanes and name failed lanes in the durable final
        // result. Agent lifecycle records remain terminal failures; this is
        // not a success rewrite, it prevents a single unavailable worker from
        // destroying independently verified team evidence.
        let mut answer = summaries.join("\n\n");
        if !blockers.is_empty() {
            answer.push_str("\n\n## Unresolved role outcomes\n");
            for blocker in blockers {
                answer.push_str("- ");
                answer.push_str(&blocker);
                answer.push('\n');
            }
        }
        let (status, result_ref, failure) = (
            ExecutionNodeStatus::Completed,
            Some(format!(
                "assistant_json:{}",
                serde_json::to_string(&answer).map_err(|error| error.to_string())?
            )),
            None,
        );
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref,
            summary: Some(answer),
            evidence_refs: evidence,
            failure,
            usage,
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
}
