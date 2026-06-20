use std::sync::Arc;

use crate::runtime_service::RuntimeService;
use memory::CognitiveContextManager;

mod agent_service;
mod approval_service;
mod channel_service;
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
mod session_service;
mod skill_service;
mod slash_controller;
mod system_service;
mod task_service;
mod workspace_service;

pub(crate) use agent_service::UpsertAgentTeamProfileRequest;
pub(crate) use approval_service::ApprovalService;
pub(crate) use channel_service::ChannelService;
pub(crate) use context_service::ContextServiceError;
pub(crate) use cross_plane_service::CrossPlaneExecutionRecord;
pub(crate) use matrix_service::MatrixService;
pub(crate) use memory_service::MemoryService;
pub(crate) use mfg_service::{
    MfgCockpitReportDeliveryOutcome, MfgCockpitReportDeliveryRequest, MfgCrossPlaneBridgeRequest,
    MfgService,
};
pub(crate) use receipt::{service_envelope, ServiceEnvelope};
pub(crate) use session_service::{SessionService, SessionUpdateRequest};
pub(crate) use skill_service::{
    SkillActionRequest, SkillCatalogQuery, SkillFileQuery, SkillProjectionQuery, SkillServiceError,
};
pub(crate) use slash_controller::SlashController;
pub(crate) use task_service::TaskService;

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
pub(crate) struct GatewayServices {
    pub(crate) runtime: Option<Arc<RuntimeService>>,
    pub(crate) channel: ChannelService,
    pub(crate) slash: SlashController,
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
    fn services_declares_gateway_boundary_owner() {
        let services = GatewayServices::baseline();
        assert_eq!(services.owner, "0.9.346 GatewayServices");
        assert_eq!(services.boundary_status, "0620_final_boundary");
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
        let ctx = ServiceContext::new()
            .with_workspace(std::path::PathBuf::from("/tmp/cowd-service-context-test"))
            .with_session("session-1");
        assert_eq!(ctx.session_id.as_deref(), Some("session-1"));
        let error = ServiceError::InvalidInput("bad".to_string());
        assert_eq!(error.kind(), "invalid_input");
        let policy = ServicePolicy::final_boundary("service-test-owner");
        assert_eq!(policy.boundary_status, "0620_final_boundary");
        let receipt = ServiceReceipt::completed("service", "operation", Some("trace".to_string()));
        assert_eq!(receipt.outcome, "completed");
    }
}
