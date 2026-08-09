//! Deterministic foreground Task routing.

use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::{
    reality::EvidenceRef,
    task::{
        TaskAggregate, TaskCreateCommand, TaskKind, TaskMissionAssignment, TaskOrigin,
        TaskRouteDecision, TaskRouteHint, TaskRouteReceipt, TaskSpec, TaskStatus, TaskTurnBinding,
        TaskTurnRole,
    },
};
use serde::Deserialize;

use crate::TaskAggregateService;

#[derive(Debug, Clone)]
pub struct TaskRouteContext {
    pub session_id: String,
    pub turn_id: String,
    pub objective: String,
    pub workspace_default_mission_id: String,
    pub hint: Option<TaskRouteHint>,
    pub candidates: Vec<TaskAggregate>,
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskRouter;

#[derive(Debug, Clone)]
pub struct TaskRouteMaterialization {
    pub receipt: TaskRouteReceipt,
    pub primary_task: TaskAggregate,
    pub root_task: TaskAggregate,
    pub bindings: Vec<TaskTurnBinding>,
}

impl TaskRouter {
    pub fn route(&self, context: TaskRouteContext) -> Result<TaskRouteReceipt, String> {
        let started = now_ms();
        if context.session_id.trim().is_empty()
            || context.turn_id.trim().is_empty()
            || context.objective.trim().is_empty()
            || context.workspace_default_mission_id.trim().is_empty()
        {
            return Err(
                "task route context requires session, turn, objective and default mission"
                    .to_string(),
            );
        }
        let hint = context.hint.unwrap_or_default();
        let open_candidates = context
            .candidates
            .iter()
            .filter(|task| !task.status.is_terminal())
            .collect::<Vec<_>>();
        let candidate_task_ids = open_candidates
            .iter()
            .map(|task| task.task_id.clone())
            .collect::<Vec<_>>();

        let (decision, source, reason) = if let Some(task_id) = hint.task_id.as_deref() {
            match context
                .candidates
                .iter()
                .find(|task| task.task_id == task_id)
            {
                Some(task) if !task.status.is_terminal() => (
                    TaskRouteDecision::Continue {
                        task_id: task.task_id.clone(),
                        role: TaskTurnRole::Primary,
                    },
                    "explicit_hint",
                    "explicit task focus matched an open task".to_string(),
                ),
                Some(task) => (
                    TaskRouteDecision::CreateSuccessor {
                        predecessor_task_id: task.task_id.clone(),
                        spec: TaskSpec::new(context.objective.clone()),
                        mission_id: hint
                            .mission_id
                            .clone()
                            .unwrap_or_else(|| task.mission_id.clone()),
                    },
                    "explicit_hint",
                    "explicit task is terminal; create a successor".to_string(),
                ),
                None => return Err(format!("explicit task `{task_id}` does not exist")),
            }
        } else if !hint.compound_objectives.is_empty() {
            let mission_id = hint
                .mission_id
                .clone()
                .unwrap_or_else(|| context.workspace_default_mission_id.clone());
            (
                TaskRouteDecision::CreateCompound {
                    primary: TaskSpec::new(context.objective.clone()),
                    additional: hint
                        .compound_objectives
                        .iter()
                        .filter(|objective| !objective.trim().is_empty())
                        .cloned()
                        .map(TaskSpec::new)
                        .collect(),
                    mission_id,
                    assignment: if hint.mission_id.is_some() {
                        TaskMissionAssignment::ExplicitLocked
                    } else {
                        TaskMissionAssignment::Default
                    },
                },
                "explicit_hint",
                "compound objectives were supplied by ingress".to_string(),
            )
        } else if open_candidates.len() == 1 {
            (
                TaskRouteDecision::Continue {
                    task_id: open_candidates[0].task_id.clone(),
                    role: TaskTurnRole::Primary,
                },
                "deterministic_rule",
                "the session has exactly one open root task".to_string(),
            )
        } else {
            let mission_id = hint
                .mission_id
                .clone()
                .unwrap_or_else(|| context.workspace_default_mission_id.clone());
            (
                TaskRouteDecision::CreateRoot {
                    spec: TaskSpec::new(context.objective),
                    mission_id,
                    assignment: if hint.mission_id.is_some() {
                        TaskMissionAssignment::ExplicitLocked
                    } else {
                        TaskMissionAssignment::Default
                    },
                },
                "deterministic_fallback",
                if open_candidates.is_empty() {
                    "no open task candidate; create a root task"
                } else {
                    "multiple candidates are ambiguous; create an isolated root task"
                }
                .to_string(),
            )
        };

        let created_at_ms = now_ms();
        Ok(TaskRouteReceipt {
            route_id: format!("task-route-{}", uuid::Uuid::new_v4()),
            session_id: context.session_id,
            turn_id: context.turn_id,
            decision,
            candidate_task_ids,
            source: source.to_string(),
            reason,
            evidence_refs: context.evidence_refs,
            elapsed_ms: created_at_ms.saturating_sub(started),
            created_at_ms,
        })
    }

    pub async fn route_with_provider(
        &self,
        services: &crate::RuntimeServices,
        preferred_model: Option<&str>,
        context: TaskRouteContext,
    ) -> Result<TaskRouteReceipt, String> {
        let hint = context.hint.clone().unwrap_or_default();
        let open_candidates = context
            .candidates
            .iter()
            .filter(|task| !task.status.is_terminal())
            .collect::<Vec<_>>();
        let needs_semantic_route = hint.task_id.is_none()
            && hint.compound_objectives.is_empty()
            && (open_candidates.len() > 1 || objective_may_be_compound(&context.objective));
        if !needs_semantic_route {
            return self.route(context);
        }

        let started = std::time::Instant::now();
        let registry = services.provider_registry();
        let snapshot = registry.pin();
        let model = preferred_model
            .filter(|model| snapshot.resolve(model).is_some())
            .map(str::to_string)
            .or_else(|| snapshot.all_models().into_iter().next());
        let Some(model) = model else {
            return Ok(provider_fallback_receipt(
                self.route(context)?,
                "no Provider model is configured for ambiguous Task routing",
            ));
        };
        let candidate_payload = open_candidates
            .iter()
            .take(16)
            .map(|task| {
                serde_json::json!({
                    "task_id": task.task_id,
                    "objective": task.objective,
                    "status": task.status.as_str(),
                    "mission_id": task.mission_id,
                    "updated_at_ms": task.updated_at_ms,
                })
            })
            .collect::<Vec<_>>();
        let client = match crate::ProviderRuntimeClient::new_with_transport_and_template_cache(
            std::sync::Arc::clone(registry),
            std::sync::Arc::clone(services.provider_transport_pool()),
            std::sync::Arc::clone(services.provider_template_cache()),
            model.clone(),
            Vec::new(),
        ) {
            Ok(client) => client.with_emit_output(false),
            Err(error) => {
                return Ok(provider_fallback_receipt(
                    self.route(context)?,
                    &format!("Provider routing client failed: {error}"),
                ));
            }
        };
        let completion = match client
            .complete_control_analysis(
                &model,
                "Route one user input to durable Tasks. Return one strict JSON object only. Continue only a clearly matching open Task. Split only genuinely independent objectives. Never invent candidate Task IDs.",
                serde_json::json!({
                    "input": context.objective,
                    "mission_hint": hint.mission_id,
                    "candidates": candidate_payload,
                    "schema": {
                        "action": "continue | create_root | create_compound",
                        "task_id": "required only for continue",
                        "additional_objectives": ["required independent objectives after the primary"],
                        "reason": "short routing reason"
                    }
                })
                .to_string(),
                640,
            )
            .await
        {
            Ok(completion) => completion,
            Err(error) => {
                return Ok(provider_fallback_receipt(
                    self.route(context)?,
                    &format!("Provider routing failed: {error}"),
                ));
            }
        };
        let proposal = match parse_route_proposal(&completion.text) {
            Ok(proposal) => proposal,
            Err(error) => {
                return Ok(provider_fallback_receipt(
                    self.route(context)?,
                    &format!("Provider routing contract failed: {error}"),
                ));
            }
        };
        let mission_id = hint
            .mission_id
            .clone()
            .unwrap_or_else(|| context.workspace_default_mission_id.clone());
        let assignment = if hint.mission_id.is_some() {
            TaskMissionAssignment::ExplicitLocked
        } else {
            TaskMissionAssignment::Default
        };
        let decision = match proposal.action {
            ProviderRouteAction::Continue => {
                let task_id = proposal.task_id.filter(|task_id| {
                    open_candidates
                        .iter()
                        .any(|task| task.task_id.as_str() == task_id.as_str())
                });
                let Some(task_id) = task_id else {
                    return Ok(provider_fallback_receipt(
                        self.route(context)?,
                        "Provider continue proposal did not select a supplied open Task",
                    ));
                };
                TaskRouteDecision::Continue {
                    task_id,
                    role: TaskTurnRole::Primary,
                }
            }
            ProviderRouteAction::CreateRoot => TaskRouteDecision::CreateRoot {
                spec: TaskSpec::new(context.objective.clone()),
                mission_id,
                assignment,
            },
            ProviderRouteAction::CreateCompound => {
                let additional = proposal
                    .additional_objectives
                    .into_iter()
                    .map(|objective| objective.trim().to_string())
                    .filter(|objective| !objective.is_empty() && objective != &context.objective)
                    .take(4)
                    .map(TaskSpec::new)
                    .collect::<Vec<_>>();
                if additional.is_empty() {
                    TaskRouteDecision::CreateRoot {
                        spec: TaskSpec::new(context.objective.clone()),
                        mission_id,
                        assignment,
                    }
                } else {
                    TaskRouteDecision::CreateCompound {
                        primary: TaskSpec::new(context.objective.clone()),
                        additional,
                        mission_id,
                        assignment,
                    }
                }
            }
        };
        Ok(TaskRouteReceipt {
            route_id: format!("task-route-{}", uuid::Uuid::new_v4()),
            session_id: context.session_id,
            turn_id: context.turn_id,
            decision,
            candidate_task_ids: open_candidates
                .iter()
                .map(|task| task.task_id.clone())
                .collect(),
            source: "provider".to_string(),
            reason: proposal.reason,
            evidence_refs: completion
                .request_id
                .map(|request_id| EvidenceRef::observed("provider_request", request_id))
                .into_iter()
                .chain(context.evidence_refs)
                .collect(),
            elapsed_ms: started.elapsed().as_millis() as u64,
            created_at_ms: now_ms(),
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn materialize_session_task_route(
    services: &crate::RuntimeServices,
    router: &TaskRouter,
    request_id: &str,
    input_id: &str,
    session_id: &str,
    turn_id: &str,
    objective: &str,
    workspace_default_mission_id: &str,
    hint: Option<TaskRouteHint>,
    origin: TaskOrigin,
    preferred_model: Option<&str>,
) -> Result<TaskRouteMaterialization, String> {
    let tasks = services.task_aggregate_service();
    if let Some(bindings) = nonempty_turn_bindings(tasks, session_id, turn_id)? {
        let primary = bindings
            .iter()
            .find(|binding| binding.role == TaskTurnRole::Primary)
            .ok_or_else(|| format!("turn `{turn_id}` has no primary Task binding"))?;
        let primary_task = tasks
            .get(&primary.task_id)?
            .ok_or_else(|| format!("bound task `{}` does not exist", primary.task_id))?;
        let root_task = tasks
            .get(&primary_task.root_task_id)?
            .ok_or_else(|| format!("root task `{}` does not exist", primary_task.root_task_id))?;
        return Ok(TaskRouteMaterialization {
            receipt: TaskRouteReceipt {
                route_id: format!("task-route:{request_id}"),
                session_id: session_id.to_string(),
                turn_id: turn_id.to_string(),
                decision: TaskRouteDecision::Continue {
                    task_id: primary_task.task_id.clone(),
                    role: TaskTurnRole::Primary,
                },
                candidate_task_ids: vec![primary_task.task_id.clone()],
                source: "durable_replay".to_string(),
                reason: "the Turn already has a durable primary Task binding".to_string(),
                evidence_refs: primary.evidence_refs.clone(),
                elapsed_ms: 0,
                created_at_ms: primary.bound_at_ms,
            },
            primary_task,
            root_task,
            bindings,
        });
    }

    let mut hint = hint;
    let mut candidates = tasks.open_root_candidates(session_id, 16)?;
    if let Some(task_id) = hint.as_ref().and_then(|hint| hint.task_id.as_deref()) {
        if let Some(explicit) = tasks.get(task_id)? {
            let root = tasks
                .get(&explicit.root_task_id)?
                .ok_or_else(|| format!("root task `{}` does not exist", explicit.root_task_id))?;
            if explicit.task_id != root.task_id {
                if let Some(route_hint) = hint.as_mut() {
                    route_hint.task_id = Some(root.task_id.clone());
                }
            }
            if !candidates.iter().any(|task| task.task_id == root.task_id) {
                candidates.push(root);
            }
        }
    }
    let mut receipt = router
        .route_with_provider(
            services,
            preferred_model,
            TaskRouteContext {
                session_id: session_id.to_string(),
                turn_id: turn_id.to_string(),
                objective: objective.to_string(),
                workspace_default_mission_id: workspace_default_mission_id.to_string(),
                hint,
                candidates,
                evidence_refs: Vec::new(),
            },
        )
        .await?;
    receipt.route_id = format!("task-route:{request_id}");
    let mut bindings = Vec::new();
    let primary_task = match &receipt.decision {
        TaskRouteDecision::NoTask { reason } => {
            return Err(format!("task router produced no Task: {reason}"));
        }
        TaskRouteDecision::Continue { task_id, role } => {
            let task = tasks
                .get(task_id)?
                .ok_or_else(|| format!("routed task `{task_id}` does not exist"))?;
            crate::task::require_continuable(&task)?;
            bindings.push(bind_task(
                tasks,
                request_id,
                input_id,
                session_id,
                turn_id,
                task_id,
                *role,
                receipt.created_at_ms,
            )?);
            task
        }
        TaskRouteDecision::CreateRoot {
            spec,
            mission_id,
            assignment,
        } => {
            let task_id = format!("task:root:{request_id}");
            let (task, binding) = create_root_with_binding(
                tasks,
                request_id,
                input_id,
                &task_id,
                mission_id,
                *assignment,
                session_id,
                turn_id,
                spec.clone(),
                None,
                origin,
                TaskTurnRole::Primary,
                receipt.created_at_ms,
            )?;
            bindings.push(binding);
            task
        }
        TaskRouteDecision::CreateSuccessor {
            predecessor_task_id,
            spec,
            mission_id,
        } => {
            let task_id = format!("task:successor:{request_id}");
            let (task, binding) = create_root_with_binding(
                tasks,
                request_id,
                input_id,
                &task_id,
                mission_id,
                TaskMissionAssignment::Automatic,
                session_id,
                turn_id,
                spec.clone(),
                Some(predecessor_task_id.clone()),
                origin,
                TaskTurnRole::Primary,
                receipt.created_at_ms,
            )?;
            bindings.push(binding);
            task
        }
        TaskRouteDecision::CreateCompound {
            primary,
            additional,
            mission_id,
            assignment,
        } => {
            let task_id = format!("task:root:{request_id}");
            let (task, binding) = create_root_with_binding(
                tasks,
                request_id,
                input_id,
                &task_id,
                mission_id,
                *assignment,
                session_id,
                turn_id,
                primary.clone(),
                None,
                origin,
                TaskTurnRole::Primary,
                receipt.created_at_ms,
            )?;
            bindings.push(binding);
            for (index, spec) in additional.iter().enumerate() {
                let additional_id = format!("task:root:{request_id}:{}", index + 1);
                let (additional_task, binding) = create_root_with_binding(
                    tasks,
                    request_id,
                    input_id,
                    &additional_id,
                    mission_id,
                    *assignment,
                    session_id,
                    turn_id,
                    spec.clone(),
                    None,
                    origin,
                    TaskTurnRole::Additional,
                    receipt.created_at_ms,
                )?;
                debug_assert_eq!(additional_task.task_id, additional_id);
                bindings.push(binding);
            }
            task
        }
    };
    let root_task = tasks
        .get(&primary_task.root_task_id)?
        .ok_or_else(|| format!("root task `{}` does not exist", primary_task.root_task_id))?;
    Ok(TaskRouteMaterialization {
        receipt,
        primary_task,
        root_task,
        bindings,
    })
}

/// Materialize model-classified work that arrives after a Turn already owns
/// its primary Task. Identity derives only from the durable disposition, so a
/// recovery attempt returns the same Root Task and binding.
#[allow(clippy::too_many_arguments)]
pub fn materialize_additional_session_task(
    services: &crate::RuntimeServices,
    disposition_id: &str,
    leader_input_id: &str,
    session_id: &str,
    turn_id: &str,
    objective: &str,
    mission_id: &str,
    predecessor_task_id: Option<String>,
    origin: TaskOrigin,
) -> Result<TaskRouteMaterialization, String> {
    for value in [
        disposition_id,
        leader_input_id,
        session_id,
        turn_id,
        objective,
        mission_id,
    ] {
        if value.trim().is_empty() {
            return Err(
                "additional Task materialization requires complete disposition scope".to_string(),
            );
        }
    }
    let tasks = services.task_aggregate_service();
    let task_id = format!("task:input-disposition:{disposition_id}");
    let binding_id = format!("task-binding:{disposition_id}:{task_id}");
    let existing_task = tasks.get(&task_id)?;
    let existing_binding = tasks
        .bindings_for_turn(session_id, turn_id)?
        .into_iter()
        .find(|binding| binding.binding_id == binding_id);
    match (existing_task, existing_binding) {
        (Some(task), Some(binding)) => {
            let exact_replay = task.mission_id == mission_id
                && task.kind == TaskKind::Root
                && task.origin == origin
                && task.origin_session_id == session_id
                && task.origin_turn_id == turn_id
                && task.root_task_id == task_id
                && task.predecessor_task_id == predecessor_task_id
                && task.mission_assignment == TaskMissionAssignment::Automatic
                && task.objective == objective
                && binding.task_id == task_id
                && binding.role == TaskTurnRole::Additional
                && binding.input_id.as_deref() == Some(leader_input_id);
            if !exact_replay {
                return Err(format!(
                    "additional Task `{task_id}` conflicts with the durable disposition replay"
                ));
            }
            return Ok(additional_task_materialization(
                disposition_id,
                leader_input_id,
                session_id,
                turn_id,
                objective,
                mission_id,
                task,
                binding,
                "durable_replay",
            ));
        }
        (None, None) => {}
        _ => {
            return Err(format!(
                "additional Task `{task_id}` has an incomplete durable disposition binding"
            ));
        }
    }
    let created_at_ms = now_ms();
    let (task, binding) = create_root_with_binding(
        tasks,
        disposition_id,
        leader_input_id,
        &task_id,
        mission_id,
        TaskMissionAssignment::Automatic,
        session_id,
        turn_id,
        TaskSpec::new(objective),
        predecessor_task_id,
        origin,
        TaskTurnRole::Additional,
        created_at_ms,
    )?;
    Ok(additional_task_materialization(
        disposition_id,
        leader_input_id,
        session_id,
        turn_id,
        objective,
        mission_id,
        task,
        binding,
        "runtime_input_disposition",
    ))
}

#[allow(clippy::too_many_arguments)]
fn additional_task_materialization(
    disposition_id: &str,
    leader_input_id: &str,
    session_id: &str,
    turn_id: &str,
    objective: &str,
    mission_id: &str,
    task: TaskAggregate,
    binding: TaskTurnBinding,
    source: &str,
) -> TaskRouteMaterialization {
    let created_at_ms = binding.bound_at_ms;
    TaskRouteMaterialization {
        receipt: TaskRouteReceipt {
            route_id: format!("task-route:{disposition_id}"),
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            decision: TaskRouteDecision::CreateRoot {
                spec: TaskSpec::new(objective),
                mission_id: mission_id.to_string(),
                assignment: TaskMissionAssignment::Automatic,
            },
            candidate_task_ids: Vec::new(),
            source: source.to_string(),
            reason: "a typed running-Turn disposition created additional governed work".to_string(),
            evidence_refs: vec![EvidenceRef::observed("session_input", leader_input_id)],
            elapsed_ms: now_ms().saturating_sub(created_at_ms),
            created_at_ms,
        },
        root_task: task.clone(),
        primary_task: task,
        bindings: vec![binding],
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProviderRouteAction {
    Continue,
    CreateRoot,
    CreateCompound,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderRouteProposal {
    action: ProviderRouteAction,
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    additional_objectives: Vec<String>,
    reason: String,
}

fn parse_route_proposal(value: &str) -> Result<ProviderRouteProposal, String> {
    let trimmed = value.trim();
    let json = if trimmed.starts_with('{') && trimmed.ends_with('}') {
        trimmed
    } else {
        let start = trimmed
            .find('{')
            .ok_or_else(|| "Task Router Provider returned no JSON object".to_string())?;
        let end = trimmed
            .rfind('}')
            .ok_or_else(|| "Task Router Provider returned incomplete JSON".to_string())?;
        &trimmed[start..=end]
    };
    let proposal: ProviderRouteProposal =
        serde_json::from_str(json).map_err(|error| format!("invalid route JSON: {error}"))?;
    if proposal.reason.trim().is_empty() {
        return Err("Task Router Provider reason must not be empty".to_string());
    }
    Ok(proposal)
}

fn objective_may_be_compound(value: &str) -> bool {
    let normalized = value.to_lowercase();
    [" and ", " also ", "；", ";", "并且", "同时", "另外"]
        .iter()
        .any(|marker| normalized.contains(marker))
}

fn provider_fallback_receipt(mut receipt: TaskRouteReceipt, reason: &str) -> TaskRouteReceipt {
    receipt.source = "provider_fallback".to_string();
    receipt.reason = format!("{}; {reason}", receipt.reason);
    receipt
}

fn nonempty_turn_bindings(
    tasks: &TaskAggregateService,
    session_id: &str,
    turn_id: &str,
) -> Result<Option<Vec<TaskTurnBinding>>, String> {
    let bindings = tasks.bindings_for_turn(session_id, turn_id)?;
    Ok((!bindings.is_empty()).then_some(bindings))
}

#[allow(clippy::too_many_arguments)]
fn create_root_with_binding(
    tasks: &TaskAggregateService,
    request_id: &str,
    input_id: &str,
    task_id: &str,
    mission_id: &str,
    assignment: TaskMissionAssignment,
    session_id: &str,
    turn_id: &str,
    spec: TaskSpec,
    predecessor_task_id: Option<String>,
    origin: TaskOrigin,
    role: TaskTurnRole,
    bound_at_ms: u64,
) -> Result<(TaskAggregate, TaskTurnBinding), String> {
    let binding = TaskTurnBinding {
        binding_id: format!("task-binding:{request_id}:{task_id}"),
        task_id: task_id.to_string(),
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        role,
        input_id: Some(input_id.to_string()),
        evidence_refs: Vec::new(),
        bound_at_ms,
    };
    tasks
        .create_with_origin_binding(
            TaskCreateCommand {
                task_id: task_id.to_string(),
                mission_id: mission_id.to_string(),
                kind: TaskKind::Root,
                origin,
                origin_session_id: session_id.to_string(),
                origin_turn_id: turn_id.to_string(),
                root_task_id: task_id.to_string(),
                parent_task_id: None,
                predecessor_task_id,
                mission_assignment: assignment,
                mission_assigned_by: format!("runtime.task_router:{}", task_origin_name(origin)),
                spec,
                evidence_refs: Vec::new(),
            },
            &binding,
        )
        .map(|(result, binding)| (result.aggregate, binding))
}

const fn task_origin_name(origin: TaskOrigin) -> &'static str {
    match origin {
        TaskOrigin::User => "user",
        TaskOrigin::Schedule => "schedule",
        TaskOrigin::Mission => "mission",
        TaskOrigin::Delegated => "delegated",
        TaskOrigin::System => "system",
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_task(
    tasks: &TaskAggregateService,
    request_id: &str,
    input_id: &str,
    session_id: &str,
    turn_id: &str,
    task_id: &str,
    role: TaskTurnRole,
    bound_at_ms: u64,
) -> Result<TaskTurnBinding, String> {
    tasks.bind_turn(&TaskTurnBinding {
        binding_id: format!("task-binding:{request_id}:{task_id}"),
        task_id: task_id.to_string(),
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        role,
        input_id: Some(input_id.to_string()),
        evidence_refs: Vec::new(),
        bound_at_ms,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[must_use]
pub fn is_open_root(task: &TaskAggregate) -> bool {
    task.kind == harness_contract::task::TaskKind::Root
        && matches!(
            task.status,
            TaskStatus::Pending | TaskStatus::Running | TaskStatus::Reviewing | TaskStatus::Blocked
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_route_contract_is_strict_and_supports_fenced_json() {
        let parsed = parse_route_proposal(
            "```json\n{\"action\":\"create_compound\",\"additional_objectives\":[\"B\"],\"reason\":\"two independent outcomes\"}\n```",
        )
        .expect("parse provider route");
        assert!(matches!(parsed.action, ProviderRouteAction::CreateCompound));
        assert_eq!(parsed.additional_objectives, vec!["B"]);
        assert!(parse_route_proposal(
            "{\"action\":\"create_root\",\"reason\":\"ok\",\"unknown\":true}"
        )
        .is_err());
    }

    #[tokio::test]
    async fn explicit_focus_can_bind_an_open_root_from_another_session() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let task_id = "task-cross-session-focus";
        services
            .task_runtime_port()
            .create(TaskCreateCommand {
                task_id: task_id.to_string(),
                mission_id: mission_id.clone(),
                kind: TaskKind::Root,
                origin: TaskOrigin::User,
                origin_session_id: "session-origin".to_string(),
                origin_turn_id: "turn-origin".to_string(),
                root_task_id: task_id.to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: TaskMissionAssignment::Default,
                mission_assigned_by: "test".to_string(),
                spec: TaskSpec::new("continue across sessions"),
                evidence_refs: vec![EvidenceRef::observed("test", "cross-session")],
            })
            .expect("create root task");

        let routed = materialize_session_task_route(
            &services,
            &TaskRouter,
            "request-cross-session",
            "input-cross-session",
            "session-target",
            "turn-target",
            "continue this task",
            &mission_id,
            Some(TaskRouteHint {
                task_id: Some(task_id.to_string()),
                ..TaskRouteHint::default()
            }),
            TaskOrigin::User,
            None,
        )
        .await
        .expect("materialize cross-session route");

        assert_eq!(routed.primary_task.task_id, task_id);
        assert_eq!(routed.root_task.task_id, task_id);
        assert_eq!(routed.bindings[0].session_id, "session-target");
        assert_eq!(routed.bindings[0].role, TaskTurnRole::Primary);
    }

    #[tokio::test]
    async fn replaying_the_same_turn_reuses_its_durable_primary_binding() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let first = materialize_session_task_route(
            &services,
            &TaskRouter,
            "request-replay-first",
            "input-replay",
            "session-replay",
            "turn-replay",
            "analyze the durable route",
            &mission_id,
            None,
            TaskOrigin::User,
            None,
        )
        .await
        .expect("first route");
        let replay = materialize_session_task_route(
            &services,
            &TaskRouter,
            "request-replay-second",
            "input-replay",
            "session-replay",
            "turn-replay",
            "different payload must not reroute an admitted Turn",
            &mission_id,
            None,
            TaskOrigin::User,
            None,
        )
        .await
        .expect("durable replay");

        assert_eq!(replay.receipt.source, "durable_replay");
        assert_eq!(replay.primary_task.task_id, first.primary_task.task_id);
        assert_eq!(replay.bindings, first.bindings);
        assert_eq!(
            services
                .task_runtime_port()
                .bindings_for_turn("session-replay", "turn-replay")
                .expect("bindings")
                .len(),
            1
        );
    }

    #[test]
    fn replaying_input_disposition_reuses_additional_task_and_binding() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let first = materialize_additional_session_task(
            &services,
            "disposition-replay",
            "input-replay",
            "session-replay",
            "turn-replay",
            "perform independent follow-up work",
            &mission_id,
            None,
            TaskOrigin::User,
        )
        .expect("materialize first additional Task");
        let replay = materialize_additional_session_task(
            &services,
            "disposition-replay",
            "input-replay",
            "session-replay",
            "turn-replay",
            "perform independent follow-up work",
            &mission_id,
            None,
            TaskOrigin::User,
        )
        .expect("replay additional Task materialization");

        assert_eq!(first.root_task.task_id, replay.root_task.task_id);
        assert_eq!(first.primary_task.revision, replay.primary_task.revision);
        assert_eq!(first.bindings[0].binding_id, replay.bindings[0].binding_id);
        let conflict = materialize_additional_session_task(
            &services,
            "disposition-replay",
            "input-replay",
            "session-replay",
            "turn-replay",
            "silently replace the original objective",
            &mission_id,
            None,
            TaskOrigin::User,
        )
        .expect_err("semantic drift must not be accepted as an idempotent replay");
        assert!(conflict.contains("conflicts with the durable disposition replay"));
        let tasks = services
            .task_runtime_port()
            .list()
            .expect("list materialized Tasks");
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.task_id == first.primary_task.task_id)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn compound_route_materializes_one_primary_and_bounded_additional_tasks() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let routed = materialize_session_task_route(
            &services,
            &TaskRouter,
            "request-compound",
            "input-compound",
            "session-compound",
            "turn-compound",
            "analyze the architecture",
            &mission_id,
            Some(TaskRouteHint {
                compound_objectives: vec![
                    "audit the implementation".to_string(),
                    "write the validation report".to_string(),
                ],
                ..TaskRouteHint::default()
            }),
            TaskOrigin::User,
            None,
        )
        .await
        .expect("compound route");

        assert_eq!(routed.bindings.len(), 3);
        assert_eq!(
            routed
                .bindings
                .iter()
                .filter(|binding| binding.role == TaskTurnRole::Primary)
                .count(),
            1
        );
        assert_eq!(
            routed
                .bindings
                .iter()
                .filter(|binding| binding.role == TaskTurnRole::Additional)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn explicit_terminal_focus_creates_a_successor_in_the_same_mission() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let task_id = "task-terminal-focus";
        let created = services
            .task_runtime_port()
            .create(TaskCreateCommand {
                task_id: task_id.to_string(),
                mission_id: mission_id.clone(),
                kind: TaskKind::Root,
                origin: TaskOrigin::User,
                origin_session_id: "session-terminal".to_string(),
                origin_turn_id: "turn-terminal".to_string(),
                root_task_id: task_id.to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: TaskMissionAssignment::Default,
                mission_assigned_by: "test".to_string(),
                spec: TaskSpec::new("completed work"),
                evidence_refs: Vec::new(),
            })
            .expect("create terminal candidate");
        let reviewing = services
            .task_runtime_port()
            .transition(
                task_id,
                created.aggregate.revision,
                TaskStatus::Reviewing,
                Vec::new(),
                "review",
            )
            .expect("review task");
        services
            .task_runtime_port()
            .transition(
                task_id,
                reviewing.aggregate.revision,
                TaskStatus::Completed,
                vec![EvidenceRef::observed("test", "task-terminal")],
                "complete",
            )
            .expect("complete task");

        let routed = materialize_session_task_route(
            &services,
            &TaskRouter,
            "request-successor",
            "input-successor",
            "session-successor",
            "turn-successor",
            "continue with the next outcome",
            &mission_id,
            Some(TaskRouteHint {
                task_id: Some(task_id.to_string()),
                ..TaskRouteHint::default()
            }),
            TaskOrigin::User,
            None,
        )
        .await
        .expect("successor route");

        assert_ne!(routed.primary_task.task_id, task_id);
        assert_eq!(
            routed.primary_task.predecessor_task_id.as_deref(),
            Some(task_id)
        );
        assert_eq!(routed.primary_task.mission_id, mission_id);
        assert_eq!(routed.bindings[0].role, TaskTurnRole::Primary);
    }
}
