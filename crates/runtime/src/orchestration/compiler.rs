use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphCommand};

use crate::execution_core::graph::ExecutionCompileError;
use crate::execution_core::{ExecutionCompileRequest, ExecutionGraphCompiler};

use super::{RuntimeOrchestrationPlan, RuntimeOrchestrationRequest};

#[derive(Debug, Clone)]
pub struct CompiledOrchestration {
    pub graph: ExecutionGraph,
    pub command: ExecutionGraphCommand,
}

/// Pure orchestration mapper. It may describe work, but it never executes it.
pub fn compile_orchestration(
    request_id: &str,
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
) -> Result<CompiledOrchestration, ExecutionCompileError> {
    let graph = ExecutionGraphCompiler.compile(ExecutionCompileRequest {
        objective: request.intent.clone(),
        payload_ref: format!("runtime-orchestration:{request_id}"),
        target: plan.execution_decision.compile_target,
        resource_scopes: orchestration_resource_scopes(request),
    })?;
    let expected_revision = graph.revision;
    Ok(CompiledOrchestration {
        graph,
        command: ExecutionGraphCommand::Start { expected_revision },
    })
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
