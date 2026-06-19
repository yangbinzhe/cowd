use std::sync::Arc;

use approval::{ApprovalRepository, FileApprovalRepository};
use memory::store::session::{
    SessionEvent, SessionListOptions, SessionListPage, SessionMessage, SessionRecord,
};
use memory::{
    CognitiveContextManager, MemoryError, RuntimeEventPage, RuntimeEventScope, UnifiedSessionStore,
};
use runtime::{
    approval_gate::SmartApprovalGate,
    permission_enforcer::{ApprovalPersistence, ApprovalVerdict},
    AgentRunGraph, AgentWorkGraph, ApprovalConfig, CollaborationReviewPacket,
};

use crate::runtime_service::RuntimeService;
use crate::session_kernel::SessionKernel;
use crate::task_kernel::{TaskKernel, TaskRecord, TaskStatus};

mod agent_service;
mod command_service;
mod connector_service;
mod context;
mod context_service;
mod cross_plane_service;
mod error;
mod matrix_service;
mod memory_service;
mod mfg_service;
mod policy;
mod receipt;
mod registry;
mod skill_service;
mod system_service;
mod workspace_service;

pub(crate) use agent_service::UpsertAgentTeamProfileRequest;
pub(crate) use command_service::CommandService;
pub(crate) use context_service::ContextServiceError;
pub(crate) use cross_plane_service::CrossPlaneExecutionRecord;
pub(crate) use matrix_service::MatrixService;
pub(crate) use memory_service::MemoryService;
pub(crate) use mfg_service::{
    MfgCockpitReportDeliveryOutcome, MfgCockpitReportDeliveryRequest, MfgCrossPlaneBridgeRequest,
    MfgService,
};
pub(crate) use receipt::{service_envelope, ServiceEnvelope};
pub(crate) use skill_service::{
    SkillActionRequest, SkillCatalogQuery, SkillFileQuery, SkillProjectionQuery, SkillServiceError,
};

pub(crate) type GatewayMemoryManager = CognitiveContextManager;
pub(crate) type GatewayMatrixRepositoryError = ::matrix_repository::MatrixSqliteRepositoryError;
pub(crate) type RuntimeContextBoundary = runtime::ContextRuntimeKernel;

#[derive(Clone)]
pub(crate) struct ContextService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl ContextService {
    pub(crate) fn new() -> Self {
        Self {
            label: "context",
            owner: "0.9.315 Context service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct ConnectorService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl ConnectorService {
    pub(crate) fn new() -> Self {
        Self {
            label: "connector",
            owner: "0.9.315 Connector service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct CrossPlaneService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl CrossPlaneService {
    pub(crate) fn new() -> Self {
        Self {
            label: "cross_plane",
            owner: "0.9.315 Cross-plane service boundary",
        }
    }
}

#[derive(Clone)]
pub(crate) struct ToolService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl ToolService {
    pub(crate) fn new() -> Self {
        Self {
            label: "tool",
            owner: "0.9.315 Tool service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct SystemService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl SystemService {
    pub(crate) fn new() -> Self {
        Self {
            label: "system",
            owner: "0.9.315 System service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct AuditService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl AuditService {
    pub(crate) fn new() -> Self {
        Self {
            label: "audit",
            owner: "0.9.315 Audit service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct WorkspaceService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl WorkspaceService {
    pub(crate) fn new() -> Self {
        Self {
            label: "workspace",
            owner: "0.9.315 Workspace service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct SkillService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl SkillService {
    pub(crate) fn new() -> Self {
        Self {
            label: "skill",
            owner: "0.9.315 Skill service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct AgentService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl AgentService {
    pub(crate) fn new() -> Self {
        Self {
            label: "agent",
            owner: "0.9.315 Agent service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct TaskService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    kernel: Option<Arc<TaskKernel>>,
}

impl TaskService {
    pub(crate) fn new() -> Self {
        Self {
            label: "task",
            owner: "0.9.296 Task service boundary",
            kernel: None,
        }
    }

    pub(crate) fn with_kernel(kernel: Arc<TaskKernel>) -> Self {
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
            boundary_status: "0618_final_boundary",
        }
    }

    fn kernel(&self) -> Result<&Arc<TaskKernel>, String> {
        self.kernel
            .as_ref()
            .ok_or_else(|| "task service not configured".to_string())
    }

    pub(crate) fn list_records(&self) -> Result<Vec<TaskRecord>, String> {
        Ok(self.kernel()?.list())
    }

    pub(crate) fn current(&self) -> Result<Option<TaskRecord>, String> {
        Ok(self.kernel()?.current())
    }

    pub(crate) fn list_agent_graphs(&self) -> Result<Vec<runtime::AgentRunGraph>, String> {
        Ok(self.kernel()?.list_agent_graphs())
    }

    pub(crate) fn agent_graph(&self, task_id: &str) -> Result<Option<AgentRunGraph>, String> {
        Ok(self.kernel()?.agent_graph(task_id))
    }

    pub(crate) fn start_goal(
        &self,
        objective: impl Into<String>,
        yolo_mode: bool,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.start_goal(objective, yolo_mode)
    }

    pub(crate) fn start_phase(
        &self,
        id: &str,
        name: String,
        objective: String,
        plan: Vec<String>,
        acceptance: Vec<String>,
        test_commands: Vec<String>,
    ) -> Result<TaskRecord, String> {
        self.kernel()?
            .start_phase(id, name, objective, plan, acceptance, test_commands)
    }

    pub(crate) fn record_phase_artifact(
        &self,
        id: &str,
        phase_id: &str,
        kind: String,
        label: String,
        value: String,
    ) -> Result<TaskRecord, String> {
        self.kernel()?
            .record_phase_artifact(id, phase_id, kind, label, value)
    }

    pub(crate) fn review_phase(
        &self,
        id: &str,
        phase_id: &str,
        result: String,
        completed: bool,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.review_phase(id, phase_id, result, completed)
    }

    pub(crate) fn transition(
        &self,
        id: &str,
        status: TaskStatus,
        current_phase: Option<String>,
        note: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.transition(id, status, current_phase, note)
    }

    pub(crate) fn record_failure(
        &self,
        id: &str,
        reason: impl Into<String>,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.record_failure(id, reason)
    }

    pub(crate) fn upsert_agent_graph(
        &self,
        task_id: &str,
        graph: AgentRunGraph,
    ) -> Result<TaskRecord, String> {
        self.kernel()?.upsert_agent_graph(task_id, graph)
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
            boundary_status: "0618_final_boundary",
        }
    }

    fn kernel(&self) -> Option<&Arc<SessionKernel>> {
        self.kernel.as_ref()
    }

    pub(crate) fn unified_store(&self) -> Option<Arc<UnifiedSessionStore>> {
        self.kernel().and_then(|kernel| kernel.unified_store())
    }

    pub(crate) fn event_bus(&self) -> Option<Arc<crate::event_bus::SessionEventBus>> {
        self.kernel().map(|kernel| kernel.event_bus())
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
            boundary_status: "0618_final_boundary",
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
pub(crate) struct GatewayServices {
    pub(crate) runtime: Option<Arc<RuntimeService>>,
    pub(crate) command: CommandService,
    pub(crate) session: SessionService,
    pub(crate) task: TaskService,
    pub(crate) approval: ApprovalService,
    pub(crate) memory: MemoryService,
    pub(crate) context: ContextService,
    pub(crate) connector: ConnectorService,
    pub(crate) cross_plane: CrossPlaneService,
    pub(crate) tool: ToolService,
    pub(crate) system: SystemService,
    pub(crate) audit: AuditService,
    pub(crate) workspace: WorkspaceService,
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
        task_kernel: Arc<TaskKernel>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        approval_gate: Arc<SmartApprovalGate>,
        approval_repository: FileApprovalRepository,
    ) -> Self {
        let command_host_runtime = Arc::clone(&runtime);
        let session_kernel = runtime.session_kernel();
        Self {
            runtime: Some(runtime),
            command: CommandService::new(Some(command_host_runtime)),
            session: SessionService::with_kernel(session_kernel),
            task: TaskService::with_kernel(task_kernel),
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
            cross_plane: CrossPlaneService::new(),
            tool: ToolService::new(),
            system: SystemService::new(),
            audit: AuditService::new(),
            workspace: WorkspaceService::new(),
            skill: SkillService::new(),
            agent: AgentService::new(),
            matrix: MatrixService::new(),
            mfg: MfgService::new(),
            owner: "0.9.292 Gateway RuntimeHost",
            boundary_status: "0618_final_boundary",
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

    #[cfg(test)]
    pub(crate) fn transition_with_kernels_for_tests(
        session_kernel: Arc<SessionKernel>,
        task_kernel: Arc<TaskKernel>,
    ) -> Self {
        Self {
            session: SessionService::with_kernel(session_kernel),
            task: TaskService::with_kernel(task_kernel),
            ..Self::transition_only()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_task_kernel_for_tests(mut self, task_kernel: Arc<TaskKernel>) -> Self {
        self.task = TaskService::with_kernel(task_kernel);
        self
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
            ("skill", "catalog"),
            ("skill", "projection"),
            ("agent", "list"),
            ("agent", "task_projection"),
            ("matrix", "health"),
            ("mfg", "health"),
            ("mfg", "incident"),
            ("mfg", "analysis"),
            ("mfg", "skill_run"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{
        context::ServiceContext, error::ServiceError, policy::ServicePolicy,
        receipt::ServiceReceipt, registry::ServiceRegistry,
    };

    #[test]
    fn services_declares_transition_owner() {
        let services = GatewayServices::transition_only();
        assert_eq!(services.owner, "0.9.292 Gateway RuntimeHost");
        assert_eq!(services.boundary_status, "0618_final_boundary");
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
        let skill_contracts = services.skill.contracts();
        assert!(skill_contracts
            .iter()
            .any(|contract| contract.operation == "catalog"));
        assert!(skill_contracts
            .iter()
            .any(|contract| contract.operation == "projection"));
        assert_eq!(
            services.agent.task_projection().operation,
            "task_projection"
        );
        assert_eq!(services.matrix.health().operation, "health");
        assert!(services
            .mfg
            .contracts()
            .iter()
            .any(|contract| contract.operation == "incident"));
        let _registry: ServiceRegistry = services.clone();
        let ctx = ServiceContext::transition_only()
            .with_workspace(std::env::temp_dir())
            .with_session("session-1");
        assert_eq!(ctx.session_id.as_deref(), Some("session-1"));
        let error = ServiceError::InvalidInput("bad".to_string());
        assert_eq!(error.kind(), "invalid_input");
        let policy = ServicePolicy::final_boundary("service-test-owner");
        assert_eq!(policy.boundary_status, "0618_final_boundary");
        let receipt = ServiceReceipt::completed("service", "operation", Some("trace".to_string()));
        assert_eq!(receipt.outcome, "completed");
    }
}
