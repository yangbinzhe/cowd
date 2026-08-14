use std::sync::Arc;

use crate::command::slash::{
    command_projection, normalize_command_name, unified_command_registry, CommandActionTarget,
    CommandDefinition, CommandProjection, CommandRegistry, CommandSurface,
};

use crate::runtime_service::RuntimeService;
use harness_contract::{reality::EvidenceRef, task::TaskStatus};

use super::{ServiceEnvelope, TaskService};

const GATEWAY_SLASH_OPERATIONS: [&str; 3] =
    ["runtime.status", "task.manage", "session.permissions"];

#[derive(Clone)]
pub(crate) struct SlashController {
    runtime: Option<Arc<RuntimeService>>,
    task: TaskService,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SlashResolution {
    pub(crate) input: String,
    pub(crate) surface: CommandSurface,
    pub(crate) command: CommandDefinition,
    pub(crate) action_request: serde_json::Value,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SlashDispatchReceipt {
    pub(crate) ok: bool,
    pub(crate) slash: String,
    pub(crate) id: String,
    pub(crate) action: CommandActionTarget,
    pub(crate) status: String,
    pub(crate) data: serde_json::Value,
    pub(crate) executed_at_ms: i64,
}

impl SlashController {
    pub(crate) fn new(runtime: Option<Arc<RuntimeService>>, task: TaskService) -> Self {
        Self { runtime, task }
    }

    pub(crate) fn label(&self) -> &'static str {
        "slash"
    }

    pub(crate) fn contracts(&self) -> Vec<ServiceEnvelope> {
        ["catalog", "projection", "detail", "resolve", "dispatch"]
            .into_iter()
            .map(|operation| ServiceEnvelope {
                service: self.label(),
                operation,
                status: "service_boundary_ready",
                owner: "0.9.380 Slash controller boundary",
                boundary_status: "0620_final_boundary",
            })
            .collect()
    }

    pub(crate) fn registry(&self) -> CommandRegistry {
        unified_command_registry()
    }

    pub(crate) fn projection(&self, surface: CommandSurface) -> CommandProjection {
        command_projection(surface)
    }

    pub(crate) fn detail(&self, id: &str) -> Option<CommandDefinition> {
        let normalized = normalize_command_name(id);
        self.registry()
            .definitions()
            .iter()
            .find(|definition| {
                definition.id == id
                    || definition.name == normalized
                    || definition.name.trim_start_matches('/') == id
            })
            .cloned()
    }

    pub(crate) fn resolve(
        &self,
        input: &str,
        surface: CommandSurface,
        context: serde_json::Value,
    ) -> Result<SlashResolution, String> {
        let normalized = normalize_command_name(input);
        let registry = self.registry();
        let definition = registry
            .find(&normalized)
            .cloned()
            .ok_or_else(|| format!("unknown command `{input}`"))?;
        if !definition.surfaces.contains(&surface) {
            return Err(format!(
                "command `{}` is not available on {surface:?}",
                definition.name
            ));
        }
        let action_request = serde_json::json!({
            "command_id": definition.id,
            "command": definition.name,
            "surface": surface,
            "action": definition.action,
            "context": context,
        });
        Ok(SlashResolution {
            input: input.to_string(),
            surface,
            command: definition,
            action_request,
        })
    }

    pub(crate) async fn dispatch(
        &self,
        command: &str,
        args: serde_json::Value,
    ) -> Result<SlashDispatchReceipt, String> {
        let definition = self
            .registry()
            .find(command)
            .cloned()
            .ok_or_else(|| format!("unknown command `{command}`"))?;
        if !definition.dispatchable {
            return Err(format!(
                "command `{}` is owned by the requesting Surface and cannot be dispatched by Gateway",
                definition.name
            ));
        }
        let (ok, status, data) = self.execute_target(&definition.action, args).await;
        Ok(SlashDispatchReceipt {
            ok,
            slash: definition.name,
            id: definition.id,
            action: definition.action,
            status: status.to_string(),
            data,
            executed_at_ms: chrono::Utc::now().timestamp_millis(),
        })
    }

    async fn execute_target(
        &self,
        action: &CommandActionTarget,
        args: serde_json::Value,
    ) -> (bool, &'static str, serde_json::Value) {
        match action {
            CommandActionTarget::Runtime { operation }
                if operation == GATEWAY_SLASH_OPERATIONS[0] =>
            {
                match &self.runtime {
                    Some(runtime) => (true, "complete", runtime.status_value()),
                    None => (
                        true,
                        "degraded",
                        serde_json::json!({
                            "ok": true,
                            "runtime_host": "gateway-baseline",
                            "active_sessions": 0,
                            "warning": "runtime service is unavailable in this gateway state",
                        }),
                    ),
                }
            }
            CommandActionTarget::Runtime { operation }
                if operation == GATEWAY_SLASH_OPERATIONS[1] =>
            {
                match self.execute_task_command(&args) {
                    Ok(data) => (true, "complete", data),
                    Err(error) => (
                        false,
                        "rejected",
                        serde_json::json!({
                            "dispatch": "task_service",
                            "operation": operation,
                            "error": error,
                        }),
                    ),
                }
            }
            CommandActionTarget::Runtime { operation }
                if operation == GATEWAY_SLASH_OPERATIONS[2] =>
            {
                match self.execute_permission_command(&args).await {
                    Ok(data) => (true, "complete", data),
                    Err(error) => (
                        false,
                        "rejected",
                        serde_json::json!({
                            "dispatch": "runtime_service",
                            "operation": operation,
                            "error": error,
                        }),
                    ),
                }
            }
            unsupported => (
                false,
                "unsupported",
                serde_json::json!({
                    "error": "slash action has no Gateway handler",
                    "action": unsupported,
                }),
            ),
        }
    }

    async fn execute_permission_command(
        &self,
        args: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| "runtime service is unavailable".to_string())?;
        let session_id = required_session_id(args)?;
        let explicit_mode = args
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let input_mode = args
            .get("input")
            .and_then(serde_json::Value::as_str)
            .and_then(|input| {
                let mut parts = input.split_whitespace();
                let command = parts.next()?;
                command
                    .trim_start_matches('/')
                    .eq_ignore_ascii_case("permissions")
                    .then(|| parts.next().map(str::to_string))
                    .flatten()
            });
        let Some(label) = explicit_mode.or(input_mode) else {
            return runtime
                .session_execution_policy_value(session_id)
                .await
                .and_then(|value| serde_json::to_value(value).map_err(|error| error.to_string()));
        };
        let profile = match label.trim().to_ascii_lowercase().as_str() {
            "read-only" | "cautious" => runtime::AutonomyProfileId::Cautious,
            "workspace-write" | "supervised" => runtime::AutonomyProfileId::Supervised,
            "danger-full-access" | "solo" | "autonomous" => runtime::AutonomyProfileId::Autonomous,
            "yolo" => runtime::AutonomyProfileId::Yolo,
            "stewarded" => runtime::AutonomyProfileId::Stewarded,
            other => return Err(format!("unsupported execution mode `{other}`")),
        };
        let current = runtime.session_execution_policy_value(session_id).await?;
        let expected_revision = current.policy.revision;
        runtime
            .set_session_execution_policy(
                session_id,
                profile,
                expected_revision,
                runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
            )
            .await
            .and_then(|value| serde_json::to_value(value).map_err(|error| error.to_string()))
    }

    fn execute_task_command(&self, args: &serde_json::Value) -> Result<serde_json::Value, String> {
        let input = args
            .get("input")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("/tasks");
        match TaskCommand::parse(input)? {
            TaskCommand::List => Ok(serde_json::json!({
                "dispatch": "task_service",
                "operation": "list",
                "tasks": self.task.list_records()?,
            })),
            TaskCommand::Start { objective } => {
                let session_id = required_session_id(args)?;
                let turn_id = command_turn_id(args);
                let task_id = format!("task-{}", uuid::Uuid::new_v4());
                let task = self.task.create(
                    task_id,
                    self.task.workspace_default_mission_id()?,
                    session_id.to_string(),
                    turn_id.clone(),
                    objective,
                    command_evidence(session_id, &turn_id, "start"),
                )?;
                Ok(serde_json::json!({
                    "dispatch": "task_service",
                    "operation": "start",
                    "task": task,
                }))
            }
            TaskCommand::Cancel { id } => {
                let session_id = required_session_id(args)?;
                let turn_id = command_turn_id(args);
                let current = self
                    .task
                    .get(&id)?
                    .ok_or_else(|| format!("task not found: {id}"))?;
                let task = self.task.transition(
                    &id,
                    current.revision,
                    TaskStatus::Cancelled,
                    command_evidence(session_id, &turn_id, "cancel"),
                    "cancelled by slash command".to_string(),
                )?;
                Ok(serde_json::json!({
                    "dispatch": "task_service",
                    "operation": "cancel",
                    "task": task,
                }))
            }
            TaskCommand::Complete { id } => {
                let session_id = required_session_id(args)?;
                let turn_id = command_turn_id(args);
                let current = self
                    .task
                    .get(&id)?
                    .ok_or_else(|| format!("task not found: {id}"))?;
                let task = self.task.transition(
                    &id,
                    current.revision,
                    TaskStatus::Completed,
                    command_evidence(session_id, &turn_id, "complete"),
                    "completed by slash command".to_string(),
                )?;
                Ok(serde_json::json!({
                    "dispatch": "task_service",
                    "operation": "complete",
                    "task": task,
                }))
            }
        }
    }
}

#[cfg(test)]
mod contract_tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn dispatchable_registry_is_exactly_the_gateway_handler_set() {
        let registry = unified_command_registry();
        let registered = registry
            .definitions()
            .iter()
            .filter(|definition| definition.dispatchable)
            .map(|definition| match &definition.action {
                CommandActionTarget::Runtime { operation } => operation.as_str(),
                other => panic!("dispatchable command has no Runtime handler: {other:?}"),
            })
            .collect::<BTreeSet<_>>();
        let handled = GATEWAY_SLASH_OPERATIONS
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(registered, handled);
    }
}

fn required_session_id(args: &serde_json::Value) -> Result<&str, String> {
    args.get("session_id")
        .and_then(serde_json::Value::as_str)
        .filter(|session_id| !session_id.trim().is_empty())
        .ok_or_else(|| "task mutation requires an authoritative session_id".to_string())
}

fn command_turn_id(args: &serde_json::Value) -> String {
    args.get("turn_id")
        .and_then(serde_json::Value::as_str)
        .filter(|turn_id| !turn_id.trim().is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("slash-turn-{}", uuid::Uuid::new_v4()))
}

fn command_evidence(session_id: &str, turn_id: &str, operation: &str) -> Vec<EvidenceRef> {
    vec![EvidenceRef::observed(
        "slash_command",
        format!("slash://sessions/{session_id}/turns/{turn_id}?operation={operation}"),
    )]
}

enum TaskCommand {
    List,
    Start { objective: String },
    Cancel { id: String },
    Complete { id: String },
}

impl TaskCommand {
    fn parse(input: &str) -> Result<Self, String> {
        let input = input.trim();
        let remainder = input
            .strip_prefix("/tasks")
            .or_else(|| input.strip_prefix("tasks"))
            .unwrap_or(input)
            .trim();
        if remainder.is_empty() || matches!(remainder, "list" | "ls") {
            return Ok(Self::List);
        }

        let mut parts = remainder.split_whitespace();
        match parts.next() {
            Some("start") => {
                let tail = parts.collect::<Vec<_>>().join(" ");
                let objective = tail.trim();
                if objective.is_empty() {
                    return Err("usage: /tasks start <objective>".to_string());
                }
                Ok(Self::Start {
                    objective: objective.to_string(),
                })
            }
            Some("cancel") => {
                required_task_id("cancel", parts.next()).map(|id| Self::Cancel { id })
            }
            Some("complete") => {
                required_task_id("complete", parts.next()).map(|id| Self::Complete { id })
            }
            Some(other) => Err(format!(
                "unknown /tasks action `{other}`; use list, start, cancel, or complete"
            )),
            None => Ok(Self::List),
        }
    }
}

fn required_task_id(action: &str, id: Option<&str>) -> Result<String, String> {
    id.map(str::trim)
        .filter(|id| !id.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("usage: /tasks {action} <task-id>"))
}
