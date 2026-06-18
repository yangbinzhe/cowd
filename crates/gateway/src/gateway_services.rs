use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use app_mfg::{MfgMatrixAdapterError, MfgStore};
use approval::{ApprovalRepository, FileApprovalRepository};
use commands::{
    command_projection, normalize_command_name, unified_command_registry, CommandActionTarget,
    CommandDefinition, CommandProjection, CommandRegistry, CommandSurface,
};
use matrix_store::MatrixRepository;
use memory::store::session::{
    SessionEvent, SessionListOptions, SessionListPage, SessionMessage, SessionRecord,
};
use memory::{
    CognitiveContextManager, MemoryError, RuntimeEventPage, RuntimeEventScope, UnifiedSessionStore,
};
use runtime::{
    approval_gate::SmartApprovalGate,
    permission_enforcer::{ApprovalPersistence, ApprovalVerdict},
    AgentWorkGraph, ApprovalConfig, CollaborationReviewPacket, ExternalResourceRef,
    SqliteResourceDirectory,
};

use crate::runtime_service::RuntimeService;
use crate::session_kernel::SessionKernel;

pub(crate) type GatewayMemoryManager = CognitiveContextManager;
pub(crate) type RuntimeContextBoundary = runtime::ContextRuntimeKernel;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServiceEnvelope {
    pub(crate) service: &'static str,
    pub(crate) operation: &'static str,
    pub(crate) status: &'static str,
    pub(crate) owner: &'static str,
    pub(crate) boundary_status: &'static str,
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
                    boundary_status: "reviewed_0.9.305",
                }
            }
        }
    };
}

define_gateway_service!(TaskService, "task");
define_gateway_service!(ContextService, "context");
define_gateway_service!(ConnectorService, "connector");
define_gateway_service!(ToolService, "tool");
define_gateway_service!(SystemService, "system");
define_gateway_service!(AuditService, "audit");
define_gateway_service!(SkillService, "skill");
define_gateway_service!(AgentService, "agent");
define_gateway_service!(MfgService, "mfg");

#[derive(Clone)]
pub(crate) struct MemoryService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    manager: Option<Arc<GatewayMemoryManager>>,
}

impl MemoryService {
    pub(crate) fn new() -> Self {
        Self {
            label: "memory",
            owner: "0.9.292 Gateway RuntimeHost",
            manager: None,
        }
    }

    pub(crate) fn with_manager(manager: Option<Arc<GatewayMemoryManager>>) -> Self {
        Self {
            manager,
            ..Self::new()
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.manager.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "reviewed_0.9.307",
        }
    }

    pub(crate) fn manager(&self) -> Option<Arc<GatewayMemoryManager>> {
        self.manager.clone()
    }

    pub(crate) fn is_available(&self) -> bool {
        self.manager.is_some()
    }
}

#[derive(Clone)]
pub(crate) struct MatrixService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl MatrixService {
    pub(crate) fn new() -> Self {
        Self {
            label: "matrix",
            owner: "0.9.297 Matrix core boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: "service_ready",
            owner: self.owner,
            boundary_status: "reviewed_0.9.305",
        }
    }

    pub(crate) fn repository_handle(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<::matrix_store::MatrixRepositoryHandle, ::matrix_store::MatrixRepositoryError> {
        ::matrix_store::MatrixRepositoryHandle::from_config_home(config_home)
    }

    pub(crate) fn store_path(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<PathBuf, ::matrix_store::MatrixRepositoryError> {
        Ok(self.repository_handle(config_home)?.db_path().to_path_buf())
    }
}

#[derive(Clone)]
pub(crate) struct SessionService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    kernel: Option<Arc<SessionKernel>>,
}

impl SessionService {
    pub(crate) fn new() -> Self {
        Self {
            label: "session",
            owner: "0.9.296 Session service boundary",
            kernel: None,
        }
    }

    pub(crate) fn with_kernel(kernel: Arc<SessionKernel>) -> Self {
        Self {
            kernel: Some(kernel),
            ..Self::new()
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.kernel.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "reviewed_0.9.305",
        }
    }

    fn kernel(&self) -> Option<&Arc<SessionKernel>> {
        self.kernel.as_ref()
    }

    pub(crate) fn unified_store(&self) -> Option<Arc<UnifiedSessionStore>> {
        self.kernel().and_then(|kernel| kernel.unified_store())
    }

    pub(crate) fn has_unified_store(&self) -> bool {
        self.kernel()
            .is_some_and(|kernel| kernel.has_unified_store())
    }

    pub(crate) fn list_active_session_ids(&self) -> Vec<String> {
        self.kernel()
            .map_or_else(Vec::new, |kernel| kernel.list_active_session_ids())
    }

    pub(crate) fn active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::BuiltRuntime>>> {
        self.kernel()
            .and_then(|kernel| kernel.active_runtime(session_id))
    }

    pub(crate) fn register_runtime(
        &self,
        session_id: String,
        runtime: crate::BuiltRuntime,
    ) -> Result<Option<Arc<tokio::sync::Mutex<crate::BuiltRuntime>>>, String> {
        self.kernel()
            .ok_or_else(|| "session service not configured".to_string())?
            .register_runtime(session_id, runtime)
    }

    pub(crate) fn remove_active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::BuiltRuntime>>> {
        self.kernel()
            .and_then(|kernel| kernel.remove_active_runtime(session_id))
    }

    pub(crate) async fn list_stored_sessions_page(
        &self,
        options: &SessionListOptions<'_>,
    ) -> Result<Option<SessionListPage>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.list_stored_sessions_page(options).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecord>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.stored_session(session_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn upsert_stored_session(
        &self,
        record: &SessionRecord,
    ) -> Result<bool, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.upsert_stored_session(record).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn update_stored_session(
        &self,
        record: &SessionRecord,
    ) -> Result<bool, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.update_stored_session(record).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn delete_stored_session(
        &self,
        session_id: &str,
    ) -> Result<bool, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.delete_stored_session(session_id).await,
            None => Ok(false),
        }
    }

    pub(crate) async fn stored_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_events_page(session_id, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_events_by_type_page(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<(usize, Vec<SessionEvent>)>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_events_by_type_page(session_id, event_type, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn search_stored_messages(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.search_stored_messages(query, limit).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_message_count(
        &self,
        session_id: &str,
    ) -> Result<Option<usize>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.stored_message_count(session_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.stored_messages(session_id, offset, limit).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<Vec<SessionMessage>>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_messages_from_sequence(session_id, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn append_timeline_event(
        &self,
        session_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<bool, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .append_timeline_event(session_id, event_type, payload)
                    .await
            }
            None => Ok(false),
        }
    }

    pub(crate) async fn append_runtime_event(
        &self,
        session_id: &str,
        scope: RuntimeEventScope,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Result<Option<usize>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .append_runtime_event(session_id, scope, kind, payload)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn persist_workgraph_review(
        &self,
        graph: &AgentWorkGraph,
        packet: &CollaborationReviewPacket,
        memory_manager: Option<&Arc<CognitiveContextManager>>,
    ) -> Result<crate::session_kernel::RuntimeClosedLoopResult, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .persist_workgraph_review(graph, packet, memory_manager)
                    .await
            }
            None => Ok(crate::session_kernel::RuntimeClosedLoopResult {
                session_id: graph.session_id.clone(),
                persisted: false,
                runtime_event_sequence: None,
                memory_pulse: None,
                degraded_reason: Some("session service not configured".to_string()),
            }),
        }
    }

    pub(crate) async fn context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>, MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel.context_event_by_envelope_id(envelope_id).await,
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_runtime_events_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<RuntimeEventPage>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_runtime_events_page(session_id, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn stored_timeline_runtime_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Option<RuntimeEventPage>, MemoryError> {
        match self.kernel() {
            Some(kernel) => {
                kernel
                    .stored_timeline_runtime_page(session_id, from_sequence, limit)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn sync_runtime_session_snapshot(
        &self,
        session_id: &str,
        session: &runtime::Session,
    ) -> Result<(), MemoryError> {
        match self.kernel() {
            Some(kernel) => kernel
                .sync_runtime_session_snapshot(session_id, session)
                .await
                .map(|_| ()),
            None => Ok(()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ApprovalService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    gate: Option<Arc<SmartApprovalGate>>,
    repository: Option<FileApprovalRepository>,
}

impl ApprovalService {
    pub(crate) fn new() -> Self {
        Self {
            label: "approval",
            owner: "0.9.296 Approval service boundary",
            gate: None,
            repository: None,
        }
    }

    pub(crate) fn with_gate_and_repository(
        gate: Arc<SmartApprovalGate>,
        repository: FileApprovalRepository,
    ) -> Self {
        Self {
            gate: Some(gate),
            repository: Some(repository),
            ..Self::new()
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.gate.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "reviewed_0.9.305",
        }
    }

    pub(crate) fn is_configured(&self) -> bool {
        self.gate.is_some()
    }

    pub(crate) async fn pending(&self) -> serde_json::Value {
        let pending = match &self.gate {
            Some(gate) => gate.get_pending_requests().await,
            None => Vec::new(),
        };
        serde_json::json!(pending)
    }

    pub(crate) async fn config(&self) -> ApprovalConfig {
        match &self.gate {
            Some(gate) => gate.config().read().await.clone(),
            None => ApprovalConfig::default(),
        }
    }

    pub(crate) async fn update_config(&self, config: ApprovalConfig) -> ApprovalConfig {
        if let Some(gate) = &self.gate {
            gate.update_config(config.clone()).await;
        }
        config
    }

    pub(crate) async fn toggle_solo(&self) -> ApprovalConfig {
        let mut cfg = self.config().await;
        cfg.solo_mode = !cfg.solo_mode;
        self.update_config(cfg).await
    }

    pub(crate) async fn history(&self, limit: usize, offset: usize) -> serde_json::Value {
        if let Some(repository) = &self.repository {
            if let Ok((history, _total)) = repository.list_history(limit, offset) {
                if !history.is_empty() {
                    return serde_json::json!(history);
                }
            }
        }
        let history = match &self.gate {
            Some(gate) => gate.history().list_history(limit, offset).await.0,
            None => Vec::new(),
        };
        serde_json::json!(history)
    }

    pub(crate) async fn respond(
        &self,
        id: &str,
        approved: bool,
        persistence: ApprovalPersistence,
        reason: Option<String>,
    ) -> Result<serde_json::Value, String> {
        let gate = self
            .gate
            .as_ref()
            .ok_or_else(|| "approval gate not configured".to_string())?;
        let deny_reason = reason
            .clone()
            .unwrap_or_else(|| "denied by user".to_string());
        let verdict = if approved {
            ApprovalVerdict::Approved
        } else {
            ApprovalVerdict::Denied {
                reason: deny_reason.clone(),
            }
        };
        let request = gate
            .resolve_approval(id, verdict, persistence)
            .await
            .ok_or_else(|| "approval request not found".to_string())?;
        Ok(serde_json::json!({
            "id": id,
            "resolved": true,
            "approved": approved,
            "tool": "bash",
            "action": request.command,
        }))
    }
}

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
                boundary_status: "reviewed_0.9.305",
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
    pub(crate) boundary_status: &'static str,
}

impl GatewayServices {
    pub(crate) fn new(
        runtime: Arc<RuntimeService>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        approval_gate: Arc<SmartApprovalGate>,
        approval_repository: FileApprovalRepository,
    ) -> Self {
        let command_runtime = Arc::clone(&runtime);
        let session_kernel = runtime.session_kernel();
        Self {
            runtime: Some(runtime),
            command: CommandService::new(Some(command_runtime)),
            session: SessionService::with_kernel(session_kernel),
            memory: MemoryService::with_manager(memory_manager),
            approval: ApprovalService::with_gate_and_repository(approval_gate, approval_repository),
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
            boundary_status: "reviewed_0.9.305",
        }
    }

    #[cfg(test)]
    pub(crate) fn transition_with_approval_for_tests(
        approval_gate: Arc<SmartApprovalGate>,
    ) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "cowd-approval-service-test-{}",
            uuid::Uuid::new_v4()
        ));
        let repository = FileApprovalRepository::new(
            dir.join("approval_history.json"),
            dir.join("always_approved.json"),
        );
        Self {
            approval: ApprovalService::with_gate_and_repository(approval_gate, repository),
            ..Self::transition_only()
        }
    }

    #[cfg(test)]
    pub(crate) fn transition_with_memory_for_tests(
        memory_manager: Arc<GatewayMemoryManager>,
    ) -> Self {
        Self {
            memory: MemoryService::with_manager(Some(memory_manager)),
            ..Self::transition_only()
        }
    }

    #[cfg(test)]
    pub(crate) fn transition_with_session_kernel_for_tests(
        session_kernel: Arc<SessionKernel>,
    ) -> Self {
        Self {
            session: SessionService::with_kernel(session_kernel),
            ..Self::transition_only()
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
            ("matrix", "health"),
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
    pub(crate) fn pending_contract(&self) -> ServiceEnvelope {
        self.envelope("pending")
    }

    pub(crate) fn respond_contract(&self) -> ServiceEnvelope {
        self.envelope("respond")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.pending_contract(), self.respond_contract()]
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

    pub(crate) fn resource_directory(
        &self,
        workspace_root: impl AsRef<Path>,
    ) -> rusqlite::Result<SqliteResourceDirectory> {
        let path = self.resource_directory_path(workspace_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                    error.kind(),
                    format!("failed to create resource directory parent: {error}"),
                )))
            })?;
        }
        SqliteResourceDirectory::open(path)
    }

    pub(crate) fn resource_directory_path(&self, workspace_root: impl AsRef<Path>) -> PathBuf {
        workspace_root
            .as_ref()
            .join(".cowd")
            .join("resource-directory.sqlite")
    }

    pub(crate) fn list_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        limit: usize,
        offset: usize,
        query: Option<&str>,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        let directory = self.resource_directory(workspace_root)?;
        query
            .map(|value| directory.search(value, limit))
            .unwrap_or_else(|| directory.list_page(limit, offset))
    }

    pub(crate) fn recent_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        limit: usize,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?.list_recent(limit)
    }

    pub(crate) fn search_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        query: &str,
        limit: usize,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?
            .search(query, limit)
    }

    pub(crate) fn get_resource(
        &self,
        workspace_root: impl AsRef<Path>,
        reference: &str,
    ) -> rusqlite::Result<Option<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?.get(reference)
    }

    pub(crate) fn upsert_resource(
        &self,
        workspace_root: impl AsRef<Path>,
        resource: &ExternalResourceRef,
    ) -> rusqlite::Result<()> {
        self.resource_directory(workspace_root)?
            .upsert(resource)
            .map(|_| ())
    }

    pub(crate) fn mark_resource_state(
        &self,
        workspace_root: impl AsRef<Path>,
        reference: &str,
        desired_state: &str,
    ) -> rusqlite::Result<(bool, Option<ExternalResourceRef>, Option<String>)> {
        let directory = self.resource_directory(workspace_root)?;
        let changed = match desired_state {
            "indexed" => directory.mark_indexed(reference)?,
            "stale" => directory.mark_stale(reference)?,
            other => return Ok((false, None, Some(format!("unsupported state: {other}")))),
        };
        let resource = directory.get(reference)?;
        Ok((changed, resource, None))
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
    pub(crate) fn health(&self) -> ServiceEnvelope {
        self.envelope("health")
    }

    pub(crate) fn structured_projection(&self) -> ServiceEnvelope {
        self.envelope("structured_projection")
    }

    pub(crate) fn repository(&self) -> ServiceEnvelope {
        self.envelope("repository")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.health(),
            self.structured_projection(),
            self.repository(),
        ]
    }
}

impl MfgService {
    pub(crate) fn placeholder(&self) -> ServiceEnvelope {
        self.envelope("placeholder")
    }

    pub(crate) fn open_store(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MfgStore, MfgMatrixAdapterError> {
        let path = ::matrix_store::MatrixRepositoryHandle::from_config_home(config_home)
            .map_err(to_mfg_sqlite_error)?
            .db_path()
            .to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(to_mfg_sqlite_error)?;
        }
        MfgStore::open(path)
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.placeholder()]
    }
}

fn to_mfg_sqlite_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> MfgMatrixAdapterError {
    MfgMatrixAdapterError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_services_declares_transition_owner() {
        let services = GatewayServices::transition_only();
        assert_eq!(services.owner, "0.9.292 Gateway RuntimeHost");
        assert_eq!(services.boundary_status, "reviewed_0.9.305");
        assert!(services.runtime.is_none());
        assert_eq!(
            services.service_labels(),
            vec![
                "runtime",
                "command",
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
        assert_eq!(services.approval.respond_contract().operation, "respond");
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
        assert_eq!(services.matrix.health().operation, "health");
        assert_eq!(services.mfg.placeholder().operation, "placeholder");
    }
}
