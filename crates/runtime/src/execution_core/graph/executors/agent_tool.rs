//! Durable Agent ToolBatch tickets reconstructed by the original scoped executor.
use super::{ScopedNodeBackend, ScopedNodeBackendResolver};
use crate::execution_core::graph::{NodeExecutionOutcome, NodeExecutionTicket, NodeExecutorError};
use crate::{RuntimeServices, RuntimeToolExecutionOutcome, RuntimeToolExecutionRequest, ToolError};
use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Weak};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundAgentTool {
    kind: String,
    agent_id: String,
    run_id: String,
    request: RuntimeToolExecutionRequest,
}

pub(crate) fn is_bound_agent_tool_graph(graph: &ExecutionGraph) -> bool {
    if graph.nodes.len() != 1 {
        return false;
    }
    let node = &graph.nodes[0];
    let Ok(bound) = serde_json::from_str::<BoundAgentTool>(&node.payload_ref) else {
        return false;
    };
    let digest = format!("{:x}", Sha256::digest(node.payload_ref.as_bytes()));
    bound.kind == "agent_tool_batch.v1"
        && node.kind == ExecutionNodeKind::ToolBatch
        && node.executor_kind == "tool_batch"
        && graph.parent_execution == bound.request.parent_execution
        && graph.parent_execution.is_some()
        && graph.id == format!("agent-tool-batch:{digest}")
        && node.id == format!("agent-tool:{digest}")
}

pub(crate) struct AgentToolBatchDispatcher {
    services: Weak<RuntimeServices>,
    agent_id: String,
    run_id: String,
}
impl AgentToolBatchDispatcher {
    pub(crate) fn new(
        services: &Arc<RuntimeServices>,
        packet: &harness_contract::agent::AgentTaskPacket,
    ) -> Self {
        Self {
            services: Arc::downgrade(services),
            agent_id: packet.agent_id().into(),
            run_id: packet.run_id().into(),
        }
    }
    pub(crate) fn execute(
        &self,
        request: RuntimeToolExecutionRequest,
    ) -> futures::future::BoxFuture<'_, Result<RuntimeToolExecutionOutcome, ToolError>> {
        // Tool dispatch is nested inside model/graph futures. Keep its durable
        // registration state off callers' stacks, including concurrent replay.
        Box::pin(async move {
            let services = self
                .services
                .upgrade()
                .ok_or_else(|| ToolError::new("Agent ToolBatch Runtime is unavailable"))?;
            let parent = request
                .parent_execution
                .clone()
                .ok_or_else(|| ToolError::new("Agent ToolBatch omitted parent execution"))?;
            let parent_graph = services
                .graph_state_store()
                .load_async(&parent.execution_id)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let payload = serde_json::to_string(&BoundAgentTool {
                kind: "agent_tool_batch.v1".into(),
                agent_id: self.agent_id.clone(),
                run_id: self.run_id.clone(),
                request,
            })
            .map_err(|e| ToolError::new(e.to_string()))?;
            let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
            let mut graph = ExecutionGraph::new("Runtime-bound Agent tool invocation");
            graph.id = format!("agent-tool-batch:{digest}");
            graph.parent_execution = Some(parent);
            graph.lineage = parent_graph.lineage;
            graph.service_class = parent_graph.service_class;
            let mut node =
                ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
            node.id = format!("agent-tool:{digest}");
            node.idempotency_key = node.id.clone();
            let node_id = node.id.clone();
            graph.nodes.push(node);
            let graph_id = graph.id.clone();
            let supervisor = services.execution_supervisor();
            if let Err(error) = supervisor.register_graph(graph.clone()).await {
                use crate::execution_core::graph::{ExecutionCommitError, ExecutionRunnerError};
                if !matches!(
                    &error,
                    ExecutionRunnerError::Commit(
                        ExecutionCommitError::EventStore(_)
                            | ExecutionCommitError::StaleRevision { .. }
                    )
                ) {
                    return Err(ToolError::new(error.to_string()));
                }
                // Registration races/retries may only attach to the exact durable
                // winner. Never hide policy, validation, or identity conflicts.
                let existing = services
                    .graph_state_store()
                    .load_async(&graph_id)
                    .await
                    .map_err(|_| ToolError::new(error.to_string()))?;
                if existing.nodes != graph.nodes
                    || existing.parent_execution != graph.parent_execution
                    || existing.lineage != graph.lineage
                    || existing.service_class != graph.service_class
                {
                    return Err(ToolError::new("Agent ToolBatch durable identity conflict"));
                }
            }
            supervisor
                .drive_registered(&graph_id)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            supervisor
                .wait_for_terminal(&graph_id)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let graph = services
                .graph_state_store()
                .load_async(&graph_id)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let result = graph
                .node_results
                .get(&node_id)
                .and_then(|result| result.result_ref.as_deref())
                .and_then(|value| value.strip_prefix("agent-tool-outcome:"))
                .ok_or_else(|| {
                    ToolError::new(
                        graph
                            .node_results
                            .get(&node_id)
                            .and_then(|result| result.failure.as_ref())
                            .map(|failure| failure.message.clone())
                            .unwrap_or_else(|| {
                                "Agent ToolBatch has no committed tool outcome".into()
                            }),
                    )
                })?;
            serde_json::from_str(result).map_err(|e| ToolError::new(e.to_string()))
        })
    }
}

pub(crate) struct AgentToolBatchResolver {
    services: Weak<RuntimeServices>,
}
impl AgentToolBatchResolver {
    pub(crate) fn install(services: &Arc<RuntimeServices>) {
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(Self {
                services: Arc::downgrade(services),
            }));
    }
}
impl ScopedNodeBackendResolver for AgentToolBatchResolver {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        if ticket.executor_kind != "tool_batch" || !ticket.graph_id.starts_with("agent-tool-batch:")
        {
            return None;
        }
        let bound: BoundAgentTool = serde_json::from_str(&ticket.payload_ref).ok()?;
        if bound.kind != "agent_tool_batch.v1" {
            return None;
        }
        let digest = format!("{:x}", Sha256::digest(ticket.payload_ref.as_bytes()));
        if ticket.graph_id != format!("agent-tool-batch:{digest}")
            || ticket.node_id != format!("agent-tool:{digest}")
        {
            return None;
        }
        Some(Arc::new(AgentToolBatchBackend {
            services: self.services.upgrade()?,
            bound,
        }))
    }
}
struct AgentToolBatchBackend {
    services: Arc<RuntimeServices>,
    bound: BoundAgentTool,
}
#[async_trait]
impl ScopedNodeBackend for AgentToolBatchBackend {
    fn retains_physical_execution(&self) -> bool {
        true
    }
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let fail = |reason: String| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason,
        };
        let request = &self.bound.request;
        let graph = self
            .services
            .graph_state_store()
            .load_async(&ticket.graph_id)
            .await
            .map_err(|e| fail(e.to_string()))?;
        if !is_bound_agent_tool_graph(&graph) || graph.service_class != ticket.service_class {
            return Err(fail(
                "Agent ToolBatch ticket does not match its durable graph".into(),
            ));
        }
        let parent = request
            .parent_execution
            .as_ref()
            .ok_or_else(|| fail("missing Agent parent".into()))?;
        let parent_graph = self
            .services
            .graph_state_store()
            .load_async(&parent.execution_id)
            .await
            .map_err(|e| fail(e.to_string()))?;
        if graph.lineage != parent_graph.lineage
            || graph.service_class != parent_graph.service_class
        {
            return Err(fail(
                "Agent ToolBatch cannot change its parent's lineage or service class".into(),
            ));
        }
        let parent_node = parent_graph
            .nodes
            .iter()
            .find(|node| node.id == parent.node_id && node.kind == ExecutionNodeKind::AgentTask)
            .ok_or_else(|| fail("Agent ToolBatch parent is not a durable AgentTask".into()))?;
        let agent: harness_contract::agent::AgentTaskPacket =
            serde_json::from_str(&parent_node.payload_ref)
                .map_err(|e| fail(format!("invalid durable Agent packet: {e}")))?;
        if agent.run_id() != self.bound.run_id
            || agent.agent_id() != self.bound.agent_id
            || agent.graph_id() != parent.execution_id
            || agent.node_id() != parent.node_id
            || Some(agent.attempt) != request.parent_execution_attempt
            || request.session_id.as_deref() != Some(agent.session_id())
            || request.governed_plan_id != parent.execution_id
            || !agent.allowed_tools.contains(&request.tool_name)
        {
            return Err(fail(
                "tool_effect_authority: Agent ToolBatch focus does not match its canonical run"
                    .into(),
            ));
        }
        let host = self
            .services
            .tool_execution_host()
            .ok_or_else(|| fail("Agent ToolBatch ToolHost unavailable".into()))?;
        let input = serde_json::from_str(&request.input)
            .map_err(|e| fail(format!("invalid tool input: {e}")))?;
        let descriptor = host
            .delegated_tool_effect_descriptor(&request.tool_name, &input)
            .ok_or_else(|| fail("missing enforceable tool descriptor".into()))?;
        let demand = crate::governed_tool_plan::resource_demand_from_effect(&descriptor);
        let (outcome, admission) = self
            .services
            .tool_execution_plane()
            .execute_async_classified_retained(
                &demand,
                None,
                ticket.service_class,
                Some(ticket.service_class),
                request.session_id.as_deref(),
                crate::bound_tool_batch::execute_bound_agent_tool(
                    host.as_ref(),
                    Some(self.services.commit_service()),
                    self.services.path_identity_resolver(),
                    self.services.scope_locks(),
                    request,
                    &descriptor,
                ),
            )
            .await;
        let outcome = outcome
            .map_err(|e| fail(e.to_string()))?
            .map_err(|e| fail(e.to_string()))?;
        // Leaf effect/receipt has committed under its admission. The graph
        // outcome below is the delivery fence; callers only read it after commit.
        let encoded = serde_json::to_string(&outcome).map_err(|e| fail(e.to_string()))?;
        drop(admission);
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(format!("agent-tool-outcome:{encoded}")),
            summary: None,
            evidence_refs: vec![],
            failure: None,
            usage: Default::default(),
            finished_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_tool_graph_cannot_borrow_a_foreign_parent_or_payload_identity() {
        let mut request =
            RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
                tool_use_id: "invocation".into(),
                tool_name: "read_file".into(),
                input: "{}".into(),
                depends_on: vec![],
            });
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "parent".into(),
                node_id: "agent".into(),
            });
        let parent = request.parent_execution.clone();
        let payload = serde_json::to_string(&BoundAgentTool {
            kind: "agent_tool_batch.v1".into(),
            agent_id: "agent".into(),
            run_id: "run".into(),
            request,
        })
        .unwrap();
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let mut graph = ExecutionGraph::new("typed fixture");
        graph.id = format!("agent-tool-batch:{digest}");
        graph.parent_execution = parent;
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        node.id = format!("agent-tool:{digest}");
        graph.nodes.push(node);
        assert!(is_bound_agent_tool_graph(&graph));
        let mut foreign = graph.clone();
        foreign.parent_execution.as_mut().unwrap().execution_id = "foreign-parent".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes[0].payload_ref = "{\"kind\":\"turn_tool_batch\"}".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes[0].id = "foreign-node".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes.push(graph.nodes[0].clone());
        assert!(!is_bound_agent_tool_graph(&foreign));
    }
}
