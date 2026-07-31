use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeSpec,
    ExecutionNodeStatus,
};
use harness_contract::turn::{SessionDispatchAction, SessionDispatchCommand, SessionHandoff};
use thiserror::Error;

use crate::execution_core::{
    ExecutionCompileError, ExecutionCompileRequest, ExecutionGraphCompiler,
};
use crate::TeamRuntime;

use super::{RuntimeOrchestrationAction, RuntimeOrchestrationPlan, RuntimeOrchestrationRequest};

#[derive(Debug, Error)]
pub enum OrchestrationCompileError {
    #[error(transparent)]
    Graph(#[from] ExecutionCompileError),
    #[error("protocol compilation failed: {0}")]
    Protocol(String),
    #[error("runtime action `{0}` has no executable protocol mapping")]
    ProtocolUnavailable(&'static str),
    #[error("Team instantiation requires an active Runtime service")]
    TeamRuntimeRequired,
    #[error("Team template instantiation failed: {0}")]
    TeamInstantiation(String),
}

#[derive(Debug, Clone)]
pub struct CompiledOrchestration {
    pub graph: ExecutionGraph,
    pub command: ExecutionGraphCommand,
    pub execute_without_protocol: bool,
    pub team_request: Option<harness_contract::team::TeamInstantiationRequest>,
}

/// Pure orchestration mapper. It may describe work, but it never executes it.
pub fn compile_orchestration(
    request_id: &str,
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
    team_runtime: Option<&TeamRuntime>,
) -> Result<CompiledOrchestration, OrchestrationCompileError> {
    if request.action == RuntimeOrchestrationAction::DispatchSession {
        return compile_session_dispatch(request_id, request, parent_execution);
    }
    if let Some(fallback_template_path) = team_template_path(request, plan) {
        let team_runtime = team_runtime.ok_or(OrchestrationCompileError::TeamRuntimeRequired)?;
        let selection_mode = request
            .selection_mode
            .unwrap_or(harness_contract::team::TeamSelectionMode::ModelAssisted);
        // `template_hint` is part of the model/human request and wins over a
        // strategy fallback.  Every role-scoped override must therefore use
        // that same resolved template path.  Using the fallback here used to
        // attach (for example) a `workstream` override to an explicitly
        // selected research template, which makes a valid Team request fail
        // before Runtime can create a graph.
        let selected_template_path = if selection_mode
            == harness_contract::team::TeamSelectionMode::Automatic
            && request.template_hint.as_deref() == Some("cowd/external-research-synthesis")
        {
            "cowd/external-research-synthesis"
        } else if selection_mode == harness_contract::team::TeamSelectionMode::Automatic {
            if request.constraints.requires_write == Some(true) {
                "cowd/execute-review"
            } else {
                "cowd/parallel-research-synthesis"
            }
        } else if request.action == RuntimeOrchestrationAction::RequestTeam
            && request
                .template_hint
                .as_deref()
                .is_none_or(|template| template.trim().is_empty())
            && request.constraints.requires_write != Some(true)
        {
            // A model-assisted read-only Team request must not inherit the
            // generic planner/executor/verifier fallback: that template
            // has an implementation role and correctly requires a write
            // lease.  Select the published read-only research protocol
            // unless the caller explicitly chose another template.
            "cowd/parallel-research-synthesis"
        } else {
            requested_template_path(request, fallback_template_path)
        };
        let template_selector =
            if selection_mode == harness_contract::team::TeamSelectionMode::Automatic {
                harness_contract::team::TeamTemplateSelector::Automatic
            } else {
                requested_template_selector(selected_template_path)?
            };
        let team_id = format!("runtime-team:{request_id}");
        let agent_budget_tokens =
            orchestration_agent_budget_tokens(plan.execution_decision.complexity());
        let team_request = harness_contract::team::TeamInstantiationRequest {
            request_id: request_id.to_string(),
            team_id: team_id.clone(),
            session_id: request.session_id.clone().unwrap_or_default(),
            mission_id: team_runtime.mission_id_for_session_or_default(
                request.session_id.as_deref().unwrap_or_default(),
            ),
            parent_execution: parent_execution.clone(),
            selection_mode,
            strategy_binding: request.strategy_binding.clone(),
            template_selector,
            objective: request.intent.clone(),
            acceptance: Vec::new(),
            risk: None,
            role_binding_overrides: Vec::new(),
            cardinality_overrides: team_cardinality_overrides(request, selected_template_path)?,
            focus_partition_plans: request.focus_partition_plans.clone(),
            permission_ceiling: if request.constraints.requires_write == Some(true) {
                harness_contract::policy::PermissionMode::WorkspaceWrite
            } else {
                harness_contract::policy::PermissionMode::ReadOnly
            },
            model_lease: request
                .model_lease
                .as_deref()
                .filter(|model| !model.trim().is_empty())
                .unwrap_or("default")
                .to_string(),
            budget_lease: Some(harness_contract::context::ContextBudgetLeaseRef::new(
                format!("runtime-team-budget:{request_id}"),
                team_id,
                "runtime_team_agent",
                agent_budget_tokens,
                1,
            )),
            managed_invocation: None,
            resource_scopes: orchestration_resource_scopes(request),
        };
        let instantiated = team_runtime
            .plan(team_request.clone())
            .map_err(OrchestrationCompileError::TeamInstantiation)?;
        let expected_revision = instantiated.graph.revision;
        return Ok(CompiledOrchestration {
            graph: instantiated.graph,
            command: ExecutionGraphCommand::Start { expected_revision },
            execute_without_protocol: true,
            team_request: Some(team_request),
        });
    }
    let mut graph = ExecutionGraphCompiler.compile(ExecutionCompileRequest {
        objective: request.intent.clone(),
        payload_ref: format!("runtime-orchestration:{request_id}"),
        target: plan.execution_decision.compile_target,
        resource_scopes: orchestration_resource_scopes(request),
    })?;
    graph.parent_execution = parent_execution;
    if graph.parent_execution.is_some() {
        graph.service_class = harness_contract::execution_graph::ExecutionServiceClass::Foreground;
    }
    let expected_revision = graph.revision;
    Ok(CompiledOrchestration {
        graph,
        command: ExecutionGraphCommand::Start { expected_revision },
        execute_without_protocol: false,
        team_request: None,
    })
}

const fn orchestration_agent_budget_tokens(
    complexity: harness_contract::core::TaskComplexity,
) -> u64 {
    match complexity {
        harness_contract::core::TaskComplexity::Trivial => 8_000,
        harness_contract::core::TaskComplexity::Simple => 12_000,
        harness_contract::core::TaskComplexity::Moderate => 16_000,
        harness_contract::core::TaskComplexity::Complex
        | harness_contract::core::TaskComplexity::Strategic => 24_000,
    }
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
        evidence_refs: request
            .evidence_refs
            .iter()
            .cloned()
            .map(|reference| {
                harness_contract::turn::opaque_session_evidence_ref(source_session_id, reference)
            })
            .collect(),
        context_budget_lease: None,
        permission_ceiling: if request.constraints.requires_write == Some(true) {
            harness_contract::policy::PermissionMode::WorkspaceWrite
        } else {
            harness_contract::policy::PermissionMode::ReadOnly
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
    graph.service_class = harness_contract::execution_graph::ExecutionServiceClass::Foreground;
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
        execute_without_protocol: true,
        team_request: None,
    })
}

fn requested_template_path<'a>(
    request: &'a RuntimeOrchestrationRequest,
    fallback_path: &'a str,
) -> &'a str {
    request
        .template_hint
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(fallback_path)
        .trim()
        .strip_prefix("builtin/")
        .unwrap_or_else(|| {
            request
                .template_hint
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(fallback_path)
                .trim()
        })
}

fn requested_template_selector(
    requested: &str,
) -> Result<harness_contract::team::TeamTemplateSelector, OrchestrationCompileError> {
    let template_id = harness_contract::team::TeamTemplateDefinitionId::new(
        harness_contract::agent::DefinitionScope::Builtin,
        requested,
    )
    .map_err(|error| {
        OrchestrationCompileError::TeamInstantiation(format!(
            "template_hint must name a builtin versioned Team template, got `{requested}`: {error}"
        ))
    })?;
    Ok(harness_contract::team::TeamTemplateSelector::LatestStable { template_id })
}

fn team_template_path(
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
) -> Option<&'static str> {
    use RuntimeOrchestrationAction as Action;

    match request.action {
        Action::RequestDeliberation => Some("cowd/debate-critic-arbiter"),
        Action::RequestReflexionRetry | Action::RequestVerification => {
            Some("cowd/implementation-review-fix")
        }
        Action::RequestTeam => Some(plan.collaboration_decision.template_id.template_path()),
        _ => None,
    }
}

fn team_cardinality_overrides(
    request: &RuntimeOrchestrationRequest,
    template_path: &str,
) -> Result<Vec<harness_contract::team::TeamRoleCardinalityOverride>, OrchestrationCompileError> {
    let Some(count) = request.constraints.max_parallel_agents else {
        return Ok(Vec::new());
    };
    let role_id = match template_path {
        "cowd/parallel-research-synthesis" | "cowd/external-research-synthesis" => "researcher",
        "cowd/debate-critic-arbiter" => "proposer",
        "cowd/matrix-scenario-ensemble" => "scenario",
        "cowd/long-running-workstreams" => "workstream",
        _ => return Ok(Vec::new()),
    };
    let count = u16::try_from(count).map_err(|_| {
        OrchestrationCompileError::TeamInstantiation(
            "requested maximum parallel Agent count exceeds the Team contract representation"
                .to_string(),
        )
    })?;
    Ok(vec![harness_contract::team::TeamRoleCardinalityOverride {
        role_id: role_id.to_string(),
        cardinality: harness_contract::team::RoleCardinalityPolicy::Fixed { count },
    }])
}

fn orchestration_resource_scopes(request: &RuntimeOrchestrationRequest) -> Vec<String> {
    let mut scopes = request
        .capabilities
        .iter()
        .filter_map(|capability| capability.strip_prefix("resource:").map(str::to_owned))
        .collect::<Vec<_>>();
    if request.constraints.requires_write == Some(true)
        && request.action != RuntimeOrchestrationAction::RequestTeam
        && scopes.is_empty()
    {
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
    use harness_contract::execution_graph::ExecutionParentBinding;

    #[test]
    fn nested_protocol_agents_do_not_receive_orchestration_tools() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let request = RuntimeOrchestrationRequest {
            intent: "team architecture review".to_string(),
            model_lease: Some("test-model".to_string()),
            session_id: Some("session-root".to_string()),
            target_session_id: None,
            action: RuntimeOrchestrationAction::RequestTeam,
            selection_mode: None,
            strategy_binding: None,
            reason: Some("test nested delegation boundary".to_string()),
            template_hint: None,
            focus_partition_plans: Vec::new(),
            capabilities: vec![
                "tool:runtime_orchestrate".to_string(),
                "tool:runtime_capabilities".to_string(),
                "tool:read_file".to_string(),
                "resource:read:crates/runtime".to_string(),
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
            Some(services.team_runtime().as_ref()),
        )
        .expect("nested Team template compiles");

        let packets = compiled
            .graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .map(|node| {
                serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(&node.payload_ref)
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("canonical bound agent packets");
        assert!(!packets.is_empty());
        for packet in &packets {
            assert!(!packet.allowed_tools.iter().any(|tool| matches!(
                tool.as_str(),
                "runtime_orchestrate" | "runtime_capabilities"
            )));
            assert!(
                packet.budget_lease.max_tokens > 0,
                "every model-originated Team agent must receive an explicit context budget"
            );
        }
        assert!(packets.iter().all(|packet| packet.binding.is_some()));
    }

    #[test]
    fn explicit_template_owns_its_parallel_role_override() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let request = RuntimeOrchestrationRequest {
            // This wording deliberately lets strategy prefer durable
            // workstreams. The explicit published template must still be the
            // source of truth for role-scoped constraints.
            intent: "run several durable workstreams with a synthesis".to_string(),
            model_lease: Some("test-model".to_string()),
            session_id: Some("session-root".to_string()),
            target_session_id: None,
            action: RuntimeOrchestrationAction::RequestTeam,
            selection_mode: Some(harness_contract::team::TeamSelectionMode::Explicit),
            strategy_binding: None,
            reason: Some("test explicit template constraint ownership".to_string()),
            template_hint: Some("cowd/parallel-research-synthesis".to_string()),
            focus_partition_plans: Vec::new(),
            capabilities: vec![
                "tool:read_file".to_string(),
                "resource:read:crates/runtime".to_string(),
            ],
            evidence_refs: Vec::new(),
            constraints: crate::RuntimeOrchestrationConstraints {
                max_parallel_agents: Some(3),
                ..Default::default()
            },
            surface: None,
        };
        let plan = plan_runtime_orchestration(&request);
        let compiled = compile_orchestration(
            "explicit-template-role-override",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("selected Team template compiles with its own role override");

        let role_ids = compiled
            .graph
            .nodes
            .iter()
            .filter_map(|node| {
                serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(&node.payload_ref)
                    .ok()
                    .and_then(|packet| {
                        packet.constraints.iter().find_map(|constraint| {
                            constraint.strip_prefix("team_role:").map(str::to_string)
                        })
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            role_ids
                .iter()
                .filter(|role| role.as_str() == "researcher")
                .count(),
            3
        );
        assert!(role_ids.iter().any(|role| role == "synthesizer"));
        assert!(!role_ids.iter().any(|role| role == "workstream"));
    }
}
