use std::sync::Arc;

use approval::FileApprovalRepository;
use runtime::approval_gate::SmartApprovalGate;

use super::*;
use crate::runtime_service::RuntimeService;
#[cfg(test)]
use crate::session_kernel::SessionKernel;
use crate::task_kernel::TaskKernel;

pub(crate) type ServiceRegistry = GatewayServices;

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
