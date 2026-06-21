use std::sync::Arc;

use slash_contract::{
    command_projection, normalize_command_name, unified_command_registry, CommandActionTarget,
    CommandDefinition, CommandProjection, CommandRegistry, CommandSurface,
};

use crate::runtime_service::RuntimeService;

use super::ServiceEnvelope;

#[derive(Clone)]
pub(crate) struct SlashController {
    runtime: Option<Arc<RuntimeService>>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct SlashResolution {
    pub(crate) input: String,
    pub(crate) surface: CommandSurface,
    pub(crate) slash: CommandDefinition,
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
    pub(crate) fn new(runtime: Option<Arc<RuntimeService>>) -> Self {
        Self { runtime }
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
                owner: "0.9.353 Slash controller boundary",
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
            slash: definition,
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
            CommandActionTarget::Runtime { operation } if operation == "runtime.status" => {
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
            CommandActionTarget::Client { action } => (
                true,
                "dispatch_required",
                serde_json::json!({
                    "dispatch": "client",
                    "message": "client action must be handled by the requesting surface",
                    "action": action,
                    "args": args,
                }),
            ),
            CommandActionTarget::Route { path } => (
                true,
                "dispatch_required",
                serde_json::json!({
                    "dispatch": "route",
                    "message": "route-backed command resolved; dispatch through the owning API/service",
                    "path": path,
                    "args": args,
                }),
            ),
            CommandActionTarget::Config { operation } => (
                true,
                "dispatch_required",
                serde_json::json!({
                    "dispatch": "system_service",
                    "operation": operation,
                    "args": args,
                }),
            ),
            CommandActionTarget::Registry { operation } => (
                true,
                "dispatch_required",
                serde_json::json!({
                    "dispatch": if operation.starts_with("skills.") {
                        "skill_service"
                    } else if operation.starts_with("agents.") {
                        "agent_service"
                    } else {
                        "registry_service"
                    },
                    "operation": operation,
                    "args": args,
                }),
            ),
            CommandActionTarget::Runtime { operation } => (
                true,
                "dispatch_required",
                serde_json::json!({
                    "dispatch": "runtime_service",
                    "operation": operation,
                    "args": args,
                }),
            ),
        }
    }
}
