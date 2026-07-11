use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphCommand};
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
}

/// Pure orchestration mapper. It may describe work, but it never executes it.
pub fn compile_orchestration(
    request_id: &str,
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
) -> Result<CompiledOrchestration, OrchestrationCompileError> {
    let protocol = select_protocol(request, plan)?;
    let graph = if let Some(protocol) = &protocol {
        let mut protocol_request = ProtocolCompileRequest::new(
            protocol.clone(),
            format!("protocol-graph:{request_id}"),
            request.session_id.clone().unwrap_or_default(),
            request.intent.clone(),
        );
        protocol_request.team_id = Some(format!("protocol-team:{request_id}"));
        protocol_request.context_refs = request.capabilities.clone();
        protocol_request.allowed_tools = request
            .capabilities
            .iter()
            .filter_map(|capability| capability.strip_prefix("tool:").map(str::to_string))
            .collect();
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
        protocol_request.model_lease = "default".to_string();
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
    let expected_revision = graph.revision;
    Ok(CompiledOrchestration {
        graph,
        command: ExecutionGraphCommand::Start { expected_revision },
        protocol,
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
