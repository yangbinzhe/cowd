use std::sync::Arc;

use commands::{
    command_projection, normalize_command_name, unified_command_registry, CommandActionTarget,
    CommandDefinition, CommandProjection, CommandRegistry, CommandSurface,
};

use crate::runtime_service::RuntimeService;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServiceEnvelope {
    pub(crate) service: &'static str,
    pub(crate) operation: &'static str,
    pub(crate) status: &'static str,
    pub(crate) owner: &'static str,
    pub(crate) route_transition_delete_by: &'static str,
}

macro_rules! define_gateway_service {
    ($name:ident, $label:literal) => {
        #[derive(Clone)]
        pub(crate) struct $name {
            pub(crate) label: &'static str,
            pub(crate) owner: &'static str,
        }

        impl $name {
            pub(crate) fn new() -> Self {
                Self {
                    label: $label,
                    owner: "0.9.292 Gateway RuntimeHost",
                }
            }

            pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
                ServiceEnvelope {
                    service: self.label,
                    operation,
                    status: "service_boundary_ready",
                    owner: self.owner,
                    route_transition_delete_by: "0.9.293-0.9.298",
                }
            }
        }
    };
}

define_gateway_service!(SessionService, "session");
define_gateway_service!(TaskService, "task");
define_gateway_service!(ApprovalService, "approval");
define_gateway_service!(MemoryService, "memory");
define_gateway_service!(ContextService, "context");
define_gateway_service!(ConnectorService, "connector");
define_gateway_service!(ToolService, "tool");
define_gateway_service!(SystemService, "system");
define_gateway_service!(AuditService, "audit");
define_gateway_service!(SkillService, "skill");
define_gateway_service!(AgentService, "agent");
define_gateway_service!(MatrixService, "matrix");
define_gateway_service!(MfgService, "mfg");

#[derive(Clone)]
pub(crate) struct CommandService {
    runtime: Option<Arc<RuntimeService>>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct CommandResolution {
    pub(crate) input: String,
    pub(crate) surface: CommandSurface,
    pub(crate) command: CommandDefinition,
    pub(crate) action_request: serde_json::Value,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct CommandExecutionReceipt {
    pub(crate) ok: bool,
    pub(crate) command: String,
    pub(crate) id: String,
    pub(crate) action: CommandActionTarget,
    pub(crate) status: String,
    pub(crate) data: serde_json::Value,
    pub(crate) executed_at_ms: i64,
}

impl CommandService {
    pub(crate) fn new(runtime: Option<Arc<RuntimeService>>) -> Self {
        Self { runtime }
    }

    pub(crate) fn label(&self) -> &'static str {
        "command"
    }

    pub(crate) fn contracts(&self) -> Vec<ServiceEnvelope> {
        ["registry", "projection", "detail", "resolve", "execute"]
            .into_iter()
            .map(|operation| ServiceEnvelope {
                service: self.label(),
                operation,
                status: "service_boundary_ready",
                owner: "0.9.294 Commands unified registry",
                route_transition_delete_by: "0.9.295-0.9.298",
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
    ) -> Result<CommandResolution, String> {
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
        Ok(CommandResolution {
            input: input.to_string(),
            surface,
            command: definition,
            action_request,
        })
    }

    pub(crate) async fn execute(
        &self,
        command: &str,
        args: serde_json::Value,
    ) -> Result<CommandExecutionReceipt, String> {
        let definition = self
            .registry()
            .find(command)
            .cloned()
            .ok_or_else(|| format!("unknown command `{command}`"))?;
        let (ok, status, data) = self.execute_target(&definition.action, args).await;
        Ok(CommandExecutionReceipt {
            ok,
            command: definition.name,
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
                            "runtime_host": "transition-only",
                            "active_sessions": 0,
                            "warning": "runtime service is unavailable in this gateway state",
                        }),
                    ),
                }
            }
            CommandActionTarget::Client { action } => (
                false,
                "client-action",
                serde_json::json!({
                    "error": "client action must be handled by the requesting surface",
                    "action": action,
                    "args": args,
                }),
            ),
            CommandActionTarget::Route { path } => (
                false,
                "unsupported",
                serde_json::json!({
                    "error": "route-backed command execution is not enabled; call resolve and dispatch through the owning service",
                    "path": path,
                    "args": args,
                }),
            ),
            CommandActionTarget::Runtime { operation }
            | CommandActionTarget::Config { operation }
            | CommandActionTarget::Registry { operation } => (
                false,
                "unsupported",
                serde_json::json!({
                    "error": "command target is declared but not yet executable through CommandService",
                    "operation": operation,
                    "args": args,
                }),
            ),
        }
    }
}

#[derive(Clone)]
pub(crate) struct GatewayServices {
    pub(crate) runtime: Option<Arc<RuntimeService>>,
    pub(crate) command: CommandService,
    pub(crate) session: SessionService,
    pub(crate) task: TaskService,
    pub(crate) approval: ApprovalService,
    pub(crate) memory: MemoryService,
    pub(crate) context: ContextService,
    pub(crate) connector: ConnectorService,
    pub(crate) tool: ToolService,
    pub(crate) system: SystemService,
    pub(crate) audit: AuditService,
    pub(crate) skill: SkillService,
    pub(crate) agent: AgentService,
    pub(crate) matrix: MatrixService,
    pub(crate) mfg: MfgService,
    pub(crate) owner: &'static str,
    pub(crate) route_transition_delete_by: &'static str,
}

impl GatewayServices {
    pub(crate) fn new(runtime: Arc<RuntimeService>) -> Self {
        let command_runtime = Arc::clone(&runtime);
        Self {
            runtime: Some(runtime),
            command: CommandService::new(Some(command_runtime)),
            ..Self::transition_only()
        }
    }

    pub(crate) fn transition_only() -> Self {
        Self {
            runtime: None,
            command: CommandService::new(None),
            session: SessionService::new(),
            task: TaskService::new(),
            approval: ApprovalService::new(),
            memory: MemoryService::new(),
            context: ContextService::new(),
            connector: ConnectorService::new(),
            tool: ToolService::new(),
            system: SystemService::new(),
            audit: AuditService::new(),
            skill: SkillService::new(),
            agent: AgentService::new(),
            matrix: MatrixService::new(),
            mfg: MfgService::new(),
            owner: "0.9.292 Gateway RuntimeHost",
            route_transition_delete_by: "0.9.293-0.9.298",
        }
    }

    pub(crate) fn service_labels(&self) -> Vec<&'static str> {
        vec![
            "runtime",
            self.command.label(),
            self.session.label,
            self.task.label,
            self.approval.label,
            self.memory.label,
            self.context.label,
            self.connector.label,
            self.tool.label,
            self.system.label,
            self.audit.label,
            self.skill.label,
            self.agent.label,
            self.matrix.label,
            self.mfg.label,
        ]
    }

    pub(crate) fn service_contracts(&self) -> Vec<ServiceEnvelope> {
        let mut contracts = Vec::new();
        contracts.extend(self.command.contracts());
        contracts.extend(self.session.contracts());
        contracts.extend(self.task.contracts());
        contracts.extend(self.approval.contracts());
        contracts.extend(self.memory.contracts());
        contracts.extend(self.context.contracts());
        contracts.extend(self.connector.contracts());
        contracts.extend(self.tool.contracts());
        contracts.extend(self.system.contracts());
        contracts.extend(self.audit.contracts());
        contracts.extend(self.skill.contracts());
        contracts.extend(self.agent.contracts());
        contracts.extend(self.matrix.contracts());
        contracts.extend(self.mfg.contracts());
        contracts
    }

    pub(crate) fn has_minimum_service_contract(&self) -> bool {
        let contracts = self.service_contracts();
        let has = |service: &str, operation: &str| {
            contracts
                .iter()
                .any(|item| item.service == service && item.operation == operation)
        };

        [
            ("session", "chat"),
            ("command", "registry"),
            ("command", "projection"),
            ("command", "resolve"),
            ("command", "execute"),
            ("session", "create"),
            ("session", "list"),
            ("session", "replay"),
            ("task", "list"),
            ("task", "start"),
            ("task", "cancel"),
            ("task", "complete"),
            ("approval", "pending"),
            ("approval", "respond"),
            ("memory", "status"),
            ("memory", "list"),
            ("memory", "query"),
            ("context", "snapshot"),
            ("context", "status"),
            ("connector", "resource_list"),
            ("connector", "resource_revalidate"),
            ("connector", "resource_promote_memory"),
            ("tool", "approve"),
            ("tool", "deny"),
            ("system", "health"),
            ("system", "config_summary"),
            ("system", "storage_summary"),
            ("system", "runtime_summary"),
            ("audit", "approval_projection"),
            ("audit", "audit_projection"),
            ("skill", "list"),
            ("skill", "view"),
            ("skill", "validate"),
            ("agent", "list"),
            ("agent", "task_projection"),
            ("matrix", "placeholder"),
            ("mfg", "placeholder"),
        ]
        .into_iter()
        .all(|(service, operation)| has(service, operation))
    }
}

impl SessionService {
    pub(crate) fn chat(&self) -> ServiceEnvelope {
        self.envelope("chat")
    }

    pub(crate) fn create_session(&self) -> ServiceEnvelope {
        self.envelope("create")
    }

    pub(crate) fn list_sessions(&self) -> ServiceEnvelope {
        self.envelope("list")
    }

    pub(crate) fn replay_session(&self) -> ServiceEnvelope {
        self.envelope("replay")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.chat(),
            self.create_session(),
            self.list_sessions(),
            self.replay_session(),
        ]
    }
}

impl TaskService {
    pub(crate) fn list(&self) -> ServiceEnvelope {
        self.envelope("list")
    }

    pub(crate) fn start(&self) -> ServiceEnvelope {
        self.envelope("start")
    }

    pub(crate) fn cancel(&self) -> ServiceEnvelope {
        self.envelope("cancel")
    }

    pub(crate) fn complete(&self) -> ServiceEnvelope {
        self.envelope("complete")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.list(), self.start(), self.cancel(), self.complete()]
    }
}

impl ApprovalService {
    pub(crate) fn pending(&self) -> ServiceEnvelope {
        self.envelope("pending")
    }

    pub(crate) fn respond(&self) -> ServiceEnvelope {
        self.envelope("respond")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.pending(), self.respond()]
    }
}

impl MemoryService {
    pub(crate) fn status(&self) -> ServiceEnvelope {
        self.envelope("status")
    }

    pub(crate) fn list(&self) -> ServiceEnvelope {
        self.envelope("list")
    }

    pub(crate) fn query(&self) -> ServiceEnvelope {
        self.envelope("query")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.status(), self.list(), self.query()]
    }
}

impl ContextService {
    pub(crate) fn snapshot(&self) -> ServiceEnvelope {
        self.envelope("snapshot")
    }

    pub(crate) fn status(&self) -> ServiceEnvelope {
        self.envelope("status")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.snapshot(), self.status()]
    }
}

impl ConnectorService {
    pub(crate) fn resource_list(&self) -> ServiceEnvelope {
        self.envelope("resource_list")
    }

    pub(crate) fn resource_revalidate(&self) -> ServiceEnvelope {
        self.envelope("resource_revalidate")
    }

    pub(crate) fn resource_promote_memory(&self) -> ServiceEnvelope {
        self.envelope("resource_promote_memory")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.resource_list(),
            self.resource_revalidate(),
            self.resource_promote_memory(),
        ]
    }
}

impl ToolService {
    pub(crate) fn approve(&self) -> ServiceEnvelope {
        self.envelope("approve")
    }

    pub(crate) fn deny(&self) -> ServiceEnvelope {
        self.envelope("deny")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.approve(), self.deny()]
    }
}

impl SystemService {
    pub(crate) fn health(&self) -> ServiceEnvelope {
        self.envelope("health")
    }

    pub(crate) fn config_summary(&self) -> ServiceEnvelope {
        self.envelope("config_summary")
    }

    pub(crate) fn storage_summary(&self) -> ServiceEnvelope {
        self.envelope("storage_summary")
    }

    pub(crate) fn runtime_summary(&self) -> ServiceEnvelope {
        self.envelope("runtime_summary")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.health(),
            self.config_summary(),
            self.storage_summary(),
            self.runtime_summary(),
        ]
    }
}

impl AuditService {
    pub(crate) fn approval_projection(&self) -> ServiceEnvelope {
        self.envelope("approval_projection")
    }

    pub(crate) fn audit_projection(&self) -> ServiceEnvelope {
        self.envelope("audit_projection")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.approval_projection(), self.audit_projection()]
    }
}

impl SkillService {
    pub(crate) fn list(&self) -> ServiceEnvelope {
        self.envelope("list")
    }

    pub(crate) fn view(&self) -> ServiceEnvelope {
        self.envelope("view")
    }

    pub(crate) fn validate(&self) -> ServiceEnvelope {
        self.envelope("validate")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.list(), self.view(), self.validate()]
    }
}

impl AgentService {
    pub(crate) fn list(&self) -> ServiceEnvelope {
        self.envelope("list")
    }

    pub(crate) fn task_projection(&self) -> ServiceEnvelope {
        self.envelope("task_projection")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.list(), self.task_projection()]
    }
}

impl MatrixService {
    pub(crate) fn placeholder(&self) -> ServiceEnvelope {
        self.envelope("placeholder")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.placeholder()]
    }
}

impl MfgService {
    pub(crate) fn placeholder(&self) -> ServiceEnvelope {
        self.envelope("placeholder")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.placeholder()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_services_declares_transition_owner() {
        let services = GatewayServices::transition_only();
        assert_eq!(services.owner, "0.9.292 Gateway RuntimeHost");
        assert_eq!(services.route_transition_delete_by, "0.9.293-0.9.298");
        assert!(services.runtime.is_none());
        assert_eq!(
            services.service_labels(),
            vec![
                "runtime",
                "session",
                "task",
                "approval",
                "memory",
                "context",
                "connector",
                "tool",
                "system",
                "audit",
                "skill",
                "agent",
                "matrix",
                "mfg",
            ]
        );
        assert!(services.has_minimum_service_contract());
        assert_eq!(services.session.create_session().operation, "create");
        assert_eq!(services.session.chat().status, "service_boundary_ready");
        assert_eq!(services.task.complete().service, "task");
        assert_eq!(services.approval.respond().operation, "respond");
        assert_eq!(services.memory.status().operation, "status");
        assert_eq!(services.context.snapshot().operation, "snapshot");
        assert_eq!(
            services.connector.resource_promote_memory().operation,
            "resource_promote_memory"
        );
        assert_eq!(services.tool.approve().operation, "approve");
        assert_eq!(
            services.system.storage_summary().operation,
            "storage_summary"
        );
        assert_eq!(
            services.audit.approval_projection().operation,
            "approval_projection"
        );
        assert_eq!(services.skill.validate().operation, "validate");
        assert_eq!(
            services.agent.task_projection().operation,
            "task_projection"
        );
        assert_eq!(services.matrix.placeholder().operation, "placeholder");
        assert_eq!(services.mfg.placeholder().operation, "placeholder");
    }
}
