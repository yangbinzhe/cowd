use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeSpec,
    ExecutionNodeStatus,
};
use harness_contract::turn::{SessionDispatchAction, SessionDispatchCommand, SessionHandoff};
use thiserror::Error;

use crate::execution_core::{
    ExecutionCompileError, ExecutionCompileRequest, ExecutionGraphCompiler, ProtocolCompileRequest,
    ProtocolId, ProtocolRef, ProtocolRegistry,
};

use super::{RuntimeOrchestrationAction, RuntimeOrchestrationPlan, RuntimeOrchestrationRequest};

#[derive(Debug, Error)]
pub enum OrchestrationCompileError {
    #[error(transparent)]
    Graph(#[from] ExecutionCompileError),
    #[error("protocol compilation failed: {0}")]
    Protocol(String),
    #[error("runtime action `{0}` has no executable protocol mapping")]
    ProtocolUnavailable(&'static str),
}

#[derive(Debug, Clone)]
pub struct CompiledOrchestration {
    pub graph: ExecutionGraph,
    pub command: ExecutionGraphCommand,
    pub protocol: Option<ProtocolRef>,
    pub execute_without_protocol: bool,
}

/// Pure orchestration mapper. It may describe work, but it never executes it.
pub fn compile_orchestration(
    request_id: &str,
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
) -> Result<CompiledOrchestration, OrchestrationCompileError> {
    if request.action == RuntimeOrchestrationAction::DispatchSession {
        return compile_session_dispatch(request_id, request, parent_execution);
    }
    let protocol = select_protocol(request, plan)?;
    let mut graph = if let Some(protocol) = &protocol {
        let mut protocol_request = ProtocolCompileRequest::new(
            protocol.clone(),
            format!("protocol-graph:{request_id}"),
            request.session_id.clone().unwrap_or_default(),
            request.intent.clone(),
        );
        protocol_request.parent_execution = parent_execution.clone();
        protocol_request.team_id = Some(format!("protocol-team:{request_id}"));
        protocol_request.context_refs = request.capabilities.clone();
        protocol_request.allowed_tools = request
            .capabilities
            .iter()
            .filter_map(|capability| capability.strip_prefix("tool:").map(str::to_string))
            .collect();
        if parent_execution.is_some() {
            protocol_request.allowed_tools.retain(|tool| {
                !matches!(
                    tool.as_str(),
                    "runtime_orchestrate" | "runtime_capabilities"
                )
            });
        }
        protocol_request.allowed_skills = request
            .capabilities
            .iter()
            .filter_map(|capability| capability.strip_prefix("skill:").map(str::to_string))
            .collect();
        protocol_request.permission_lease = if request.constraints.requires_write == Some(true) {
            "workspace_write".to_string()
        } else {
            "read_only".to_string()
        };
        // Team protocol packets must inherit the model bound to the parent
        // session. `default` is retained only for direct Runtime callers that
        // intentionally have no gateway/session binding.
        protocol_request.model_lease = request
            .model_lease
            .as_deref()
            .filter(|model| !model.trim().is_empty())
            .unwrap_or("default")
            .to_string();
        protocol_request.backend_constraint = request
            .capabilities
            .iter()
            .find(|capability| capability.as_str() == "backend:process_jsonl")
            .cloned();
        protocol_request.budget_lease_id = format!("runtime-orchestration:{request_id}");
        protocol_request.budget_tokens = 0;
        protocol_request.budget_revision = 1;
        protocol_request.resource_scopes = orchestration_resource_scopes(request);
        protocol_request.fanout = request
            .constraints
            .max_parallel_agents
            .unwrap_or(2)
            .clamp(2, 4);
        protocol_request.enable_repair = matches!(
            request.action,
            RuntimeOrchestrationAction::RequestReflexionRetry
        );
        ProtocolRegistry::resolve(protocol)
            .map_err(|error| OrchestrationCompileError::Protocol(error.to_string()))?;
        ProtocolRegistry::compile(&protocol_request)
            .map_err(|error| OrchestrationCompileError::Protocol(error.to_string()))?
    } else {
        ExecutionGraphCompiler.compile(ExecutionCompileRequest {
            objective: request.intent.clone(),
            payload_ref: format!("runtime-orchestration:{request_id}"),
            target: plan.execution_decision.compile_target,
            resource_scopes: orchestration_resource_scopes(request),
        })?
    };
    graph.parent_execution = parent_execution;
    let expected_revision = graph.revision;
    Ok(CompiledOrchestration {
        graph,
        command: ExecutionGraphCommand::Start { expected_revision },
        protocol,
        execute_without_protocol: false,
    })
}

fn compile_session_dispatch(
    request_id: &str,
    request: &RuntimeOrchestrationRequest,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
) -> Result<CompiledOrchestration, OrchestrationCompileError> {
    let source_session_id = request
        .session_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or(OrchestrationCompileError::ProtocolUnavailable(
            "dispatch_session",
        ))?;
    let target_session_id = request
        .target_session_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or(OrchestrationCompileError::ProtocolUnavailable(
            "dispatch_session",
        ))?;
    let handoff = SessionHandoff {
        handoff_id: format!("runtime-handoff:{request_id}"),
        source_session_id: source_session_id.to_string(),
        target_session_id: target_session_id.to_string(),
        objective: request.intent.clone(),
        acceptance: Vec::new(),
        scope: orchestration_resource_scopes(request),
        context_lens: request.capabilities.clone(),
        evidence_refs: request.evidence_refs.clone(),
        permission_lease: if request.constraints.requires_write == Some(true) {
            "workspace_write".to_string()
        } else {
            "read_only".to_string()
        },
        deadline_at_ms: None,
        priority: 128,
        correlation_id: format!("runtime-handoff-correlation:{request_id}"),
        result_contract: "return checked result to source graph".to_string(),
    };
    let command = SessionDispatchCommand {
        command_id: format!("runtime-dispatch:{request_id}"),
        action: SessionDispatchAction::Enqueue,
        handoff,
        expected_target_revision: 0,
    };
    let mut graph = ExecutionGraph::new(request.intent.clone());
    graph.id = format!("runtime-session-dispatch:{request_id}");
    graph.parent_execution = parent_execution;
    let node = ExecutionNodeSpec::new(
        ExecutionNodeKind::SessionDispatch,
        crate::SESSION_DISPATCH_EXECUTOR,
        format!(
            "session_handoff:{}",
            serde_json::to_string(&command)
                .map_err(|error| { OrchestrationCompileError::Protocol(error.to_string()) })?
        ),
    );
    graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(node);
    let expected_revision = graph.revision;
    Ok(CompiledOrchestration {
        graph,
        command: ExecutionGraphCommand::Start { expected_revision },
        protocol: None,
        execute_without_protocol: true,
    })
}

fn select_protocol(
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
) -> Result<Option<ProtocolRef>, OrchestrationCompileError> {
    if let Some(protocol) = request.protocol.clone() {
        return protocol_is_compatible(request.action, protocol.id)
            .then_some(Some(protocol))
            .ok_or(OrchestrationCompileError::ProtocolUnavailable(
                request.action.as_str(),
            ));
    }

    let protocol = match request.action {
        RuntimeOrchestrationAction::RequestDeliberation => ProtocolId::Debate,
        RuntimeOrchestrationAction::RequestReflexionRetry => ProtocolId::ReviewFix,
        RuntimeOrchestrationAction::RequestTeam => {
            match plan.collaboration_decision.protocol_id.as_deref() {
                Some("debate@1") => ProtocolId::Debate,
                Some("review_fix@1") => ProtocolId::ReviewFix,
                Some("incident@1") => ProtocolId::Incident,
                // A generic multi-agent request has no caller-supplied role
                // topology. JPS is the canonical bounded team protocol rather
                // than pretending a V5 custom-role graph was started.
                _ => ProtocolId::Jps,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(ProtocolRef::new(protocol, 1)))
}

fn protocol_is_compatible(action: RuntimeOrchestrationAction, protocol: ProtocolId) -> bool {
    matches!(
        (action, protocol),
        (
            RuntimeOrchestrationAction::RequestDeliberation,
            ProtocolId::Debate | ProtocolId::Jps
        ) | (
            RuntimeOrchestrationAction::RequestReflexionRetry,
            ProtocolId::ReviewFix
        ) | (
            RuntimeOrchestrationAction::RequestTeam,
            ProtocolId::Debate | ProtocolId::Jps | ProtocolId::ReviewFix | ProtocolId::Incident
        )
    )
}

fn orchestration_resource_scopes(request: &RuntimeOrchestrationRequest) -> Vec<String> {
    let mut scopes = request
        .capabilities
        .iter()
        .filter_map(|capability| capability.strip_prefix("resource:").map(str::to_owned))
        .collect::<Vec<_>>();
    if request.constraints.requires_write == Some(true) && scopes.is_empty() {
        scopes.extend(["write:.".to_string(), "worktree:.".to_string()]);
    }
    if let Some(session_id) = request.session_id.as_deref().filter(|id| !id.is_empty()) {
        scopes.push(format!("session:{session_id}"));
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

#[must_use]
pub fn guidance_for_compile_result(compiled: bool) -> String {
    if compiled {
        "The request was compiled into the canonical execution graph. Use the graph projection and committed evidence as the only execution truth."
            .to_string()
    } else {
        "The requested execution capability is unavailable in this runtime version. Choose an exposed capability or continue directly; no side effect was started."
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::planner::plan_runtime_orchestration;
    use harness_contract::agent::AgentTaskPacket;
    use harness_contract::execution_graph::ExecutionParentBinding;

    #[test]
    fn nested_protocol_agents_do_not_receive_orchestration_tools() {
        let request = RuntimeOrchestrationRequest {
            intent: "team architecture review".to_string(),
            model_lease: Some("test-model".to_string()),
            session_id: Some("session-root".to_string()),
            target_session_id: None,
            action: RuntimeOrchestrationAction::RequestTeam,
            reason: Some("test nested delegation boundary".to_string()),
            template_hint: None,
            protocol: None,
            capabilities: vec![
                "tool:runtime_orchestrate".to_string(),
                "tool:runtime_capabilities".to_string(),
                "tool:read_file".to_string(),
            ],
            evidence_refs: Vec::new(),
            constraints: Default::default(),
            surface: None,
        };
        let plan = plan_runtime_orchestration(&request);
        let compiled = compile_orchestration(
            "nested-boundary",
            &request,
            &plan,
            Some(ExecutionParentBinding {
                execution_id: "parent-graph".to_string(),
                node_id: "parent-graph:tool-batch".to_string(),
            }),
        )
        .expect("nested protocol compiles");

        let packets = compiled
            .graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .map(|node| serde_json::from_str::<AgentTaskPacket>(&node.payload_ref))
            .collect::<Result<Vec<_>, _>>()
            .expect("canonical agent packets");
        assert!(!packets.is_empty());
        for packet in &packets {
            assert!(!packet.allowed_tools.iter().any(|tool| matches!(
                tool.as_str(),
                "runtime_orchestrate" | "runtime_capabilities"
            )));
        }
        assert!(packets
            .iter()
            .any(|packet| { packet.allowed_tools.contains(&"read_file".to_string()) }));
        assert!(packets.iter().any(|packet| packet.allowed_tools.is_empty()));
    }
}
