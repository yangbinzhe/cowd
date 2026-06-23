use std::sync::{Arc, Mutex};

use ai_kernel::{
    core::{ExecutionMode, TaskComplexity, TaskRisk},
    growth::{GrowthEvent, GrowthEventInput, GrowthEvidenceRef, GrowthInput, LearningRecord},
    policy::{PolicyDecisionKind, RiskGateReceipt},
};

use crate::runtime_service::RuntimeService;
use memory::CognitiveContextManager;

mod agent_service;
mod approval_service;
mod connector_service;
mod context;
mod context_service;
mod cross_plane_service;
mod error;
mod growth_service;
mod matrix_service;
mod memory_service;
mod mfg_service;
mod mission_service;
mod policy;
pub(crate) mod reality_service;
mod receipt;
mod registry;
mod session_service;
mod skill_service;
mod slash_controller;
mod surface_service;
mod system_service;
mod task_service;
mod workspace_service;

pub(crate) use agent_service::UpsertAgentTeamProfileRequest;
pub(crate) use approval_service::ApprovalService;
pub(crate) use context_service::ContextServiceError;
pub(crate) use cross_plane_service::CrossPlaneExecutionRecord;
pub(crate) use growth_service::growth_storage_migrations;
pub(crate) use matrix_service::MatrixService;
pub(crate) use memory_service::MemoryService;
pub(crate) use mfg_service::{
    MfgCockpitReportDeliveryOutcome, MfgCockpitReportDeliveryRequest, MfgCrossPlaneBridgeRequest,
    MfgService,
};
pub(crate) use mission_service::{
    AttachMissionAgentHttpRequest, AttachMissionTeamHttpRequest, StartMissionSessionHttpRequest,
};
pub(crate) use reality_service::RealityService;
pub(crate) use receipt::{service_envelope, ServiceEnvelope};
pub(crate) use session_service::{SessionService, SessionUpdateRequest};
pub(crate) use skill_service::{
    SkillActionRequest, SkillCatalogQuery, SkillFileQuery, SkillProjectionQuery, SkillServiceError,
};
pub(crate) use slash_controller::SlashController;
pub(crate) use surface_service::SurfaceService;
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

impl CrossPlaneService {
    pub(crate) fn summary(&self) -> ServiceEnvelope {
        self.envelope("summary")
    }

    pub(crate) fn grants(&self) -> ServiceEnvelope {
        self.envelope("grants")
    }

    pub(crate) fn identities(&self) -> ServiceEnvelope {
        self.envelope("identities")
    }

    pub(crate) fn audit(&self) -> ServiceEnvelope {
        self.envelope("audit")
    }

    pub(crate) fn execute(&self) -> ServiceEnvelope {
        self.envelope("execute")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.summary(),
            self.grants(),
            self.identities(),
            self.audit(),
            self.execute(),
        ]
    }
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

    pub(crate) fn risk_gate_projection(&self, receipt: &RiskGateReceipt) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.envelope("risk_gate_projection"),
            "source": "approval.risk_receipt",
            "issued_at": receipt.issued_at,
            "decision": receipt.decision,
            "approval_required": receipt.approval_required,
            "risk": receipt.risk,
            "scope": receipt.scope,
        })
    }
}

#[derive(Clone)]
pub(crate) struct ProviderService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

impl ProviderService {
    pub(crate) fn new() -> Self {
        Self {
            label: "provider",
            owner: "0.9.370 Provider service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn config_projection(
        &self,
        runtime_config: &runtime::RuntimeConfig,
    ) -> serde_json::Value {
        let providers = runtime_config.providers();
        let configured_model = runtime_config.model().map(str::to_string);
        let configured_model_provider = configured_model
            .as_deref()
            .and_then(|model| providers.resolve_full(model))
            .map(|provider| provider.name.clone());
        let mut provider_rows = providers
            .providers
            .values()
            .map(|provider| {
                serde_json::json!({
                    "name": provider.name,
                    "base_url": provider.base_url,
                    "protocol": provider.protocol,
                    "models": provider.models,
                    "model_count": provider.models.len(),
                    "credential_present": !provider.api_key.trim().is_empty(),
                })
            })
            .collect::<Vec<_>>();
        provider_rows.sort_by(|left, right| {
            left["name"]
                .as_str()
                .unwrap_or("")
                .cmp(right["name"].as_str().unwrap_or(""))
        });
        let selected_model = configured_model.clone();
        let models = provider_rows
            .iter()
            .flat_map(|provider| {
                let provider_name = provider["name"].as_str().unwrap_or("").to_string();
                let selected_model = selected_model.clone();
                provider["models"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(move |model| {
                        model.as_str().map(|id| {
                            serde_json::json!({
                                "id": id,
                                "name": id,
                                "provider": provider_name,
                                "selected": selected_model.as_deref() == Some(id),
                            })
                        })
                    })
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "envelope": self.envelope("config_projection"),
            "providers": provider_rows,
            "models": models,
            "provider_count": providers.providers.len(),
            "provider_model_count": models.len(),
            "configured_model": configured_model,
            "configured_model_provider": configured_model_provider,
            "configured_model_resolved": configured_model.is_none() || configured_model_provider.is_some(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct GrowthService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    events: Arc<Mutex<Vec<GrowthEvent>>>,
    fact_kernel: Arc<Mutex<fact_kernel::FactKernelService>>,
}

impl GrowthService {
    pub(crate) fn new() -> Self {
        Self {
            label: "growth",
            owner: "0.9.370 Growth service boundary",
            events: Arc::new(Mutex::new(Vec::new())),
            fact_kernel: Arc::new(Mutex::new(fact_kernel::FactKernelService::new())),
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn risk_gate_event(
        &self,
        session_id: impl Into<String>,
        receipt: &RiskGateReceipt,
    ) -> serde_json::Value {
        let record = LearningRecord::from_input(GrowthInput {
            selected_mode: if receipt.approval_required {
                ExecutionMode::HumanConfirm
            } else {
                ExecutionMode::RiskGate
            },
            complexity: TaskComplexity::Moderate,
            risk: if receipt.approval_required {
                TaskRisk::High
            } else {
                TaskRisk::Medium
            },
            context_omitted: 0,
            tool_requires_checkpoint: !matches!(receipt.decision, PolicyDecisionKind::Allow),
            tool_requires_human_confirm: receipt.approval_required,
            verification_can_finalize: !receipt.approval_required,
            bench_passed: true,
        });
        let event = GrowthEvent::from_input(GrowthEventInput {
            session_id: session_id.into(),
            source_event_kind: "approval.risk_receipt".to_string(),
            strategy_mode: if receipt.approval_required {
                ExecutionMode::HumanConfirm
            } else {
                ExecutionMode::RiskGate
            },
            learning_record: record,
            evidence_refs: vec![GrowthEvidenceRef::new(
                "risk_gate_receipt",
                format!("risk:{}", receipt.issued_at.timestamp_millis()),
                format!(
                    "decision={:?} approval_required={}",
                    receipt.decision, receipt.approval_required
                ),
            )],
        });
        self.record_event(event.clone());

        serde_json::json!({
            "envelope": self.envelope("risk_gate_event"),
            "event": event,
        })
    }

    pub(crate) fn record_event(&self, event: GrowthEvent) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
    }

    pub(crate) fn event_log(&self) -> Vec<GrowthEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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

#[derive(Clone)]
pub(crate) struct MissionService {
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
    pub(crate) surface: SurfaceService,
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
    pub(crate) provider: ProviderService,
    pub(crate) reality: RealityService,
    pub(crate) growth: GrowthService,
    pub(crate) workspace: WorkspaceService,
    pub(crate) skill: SkillService,
    pub(crate) agent: AgentService,
    pub(crate) matrix: MatrixService,
    pub(crate) mfg: MfgService,
    pub(crate) mission: MissionService,
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

    pub(crate) fn risk_gate_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("risk_gate_projection")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.approval_projection(),
            self.audit_projection(),
            self.risk_gate_projection_contract(),
        ]
    }
}

impl ProviderService {
    pub(crate) fn config_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("config_projection")
    }

    pub(crate) fn model_routing(&self) -> ServiceEnvelope {
        self.envelope("model_routing")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.config_projection_contract(), self.model_routing()]
    }
}

impl GrowthService {
    pub(crate) fn risk_gate_event_contract(&self) -> ServiceEnvelope {
        self.envelope("risk_gate_event")
    }

    pub(crate) fn memory_candidates(&self) -> ServiceEnvelope {
        self.envelope("memory_candidates")
    }

    pub(crate) fn matrix_signals(&self) -> ServiceEnvelope {
        self.envelope("matrix_signals")
    }

    pub(crate) fn event_log_contract(&self) -> ServiceEnvelope {
        self.envelope("event_log")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.risk_gate_event_contract(),
            self.memory_candidates(),
            self.matrix_signals(),
            self.event_log_contract(),
        ]
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
        assert_eq!(services.owner, "0.9.370 GatewayServices");
        assert_eq!(services.boundary_status, "0620_final_boundary");
        assert!(services.runtime.is_none());
        assert_eq!(
            services.service_labels(),
            vec![
                "runtime",
                "surface",
                "slash",
                "session",
                "task",
                "approval",
                "memory",
                "context",
                "connector",
                "cross_plane",
                "tool",
                "system",
                "audit",
                "provider",
                "reality",
                "growth",
                "workspace",
                "skill",
                "agent",
                "matrix",
                "mfg",
                "mission",
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
        assert_eq!(services.cross_plane.summary().operation, "summary");
        assert_eq!(services.tool.approve().operation, "approve");
        assert_eq!(
            services.system.storage_summary().operation,
            "storage_summary"
        );
        assert_eq!(
            services.audit.approval_projection().operation,
            "approval_projection"
        );
        assert_eq!(
            services.audit.risk_gate_projection_contract().operation,
            "risk_gate_projection"
        );
        assert_eq!(
            services.provider.config_projection_contract().operation,
            "config_projection"
        );
        assert_eq!(services.reality.status_contract().operation, "status");
        assert_eq!(services.reality.flow_contract().operation, "flow");
        assert_eq!(
            services.growth.risk_gate_event_contract().operation,
            "risk_gate_event"
        );
        assert_eq!(services.growth.event_log_contract().operation, "event_log");
        assert!(services
            .workspace
            .contracts()
            .iter()
            .any(|contract| contract.operation == "overview"));
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
        assert_eq!(
            services.mission.projection_contract().operation,
            "projection"
        );
        assert_eq!(
            services.mission.session_control_contract().operation,
            "session_control"
        );
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

    #[tokio::test]
    async fn growth_service_persists_and_projects_events_to_fact_and_matrix() {
        let services = GatewayServices::baseline();
        let config_home = std::env::temp_dir().join(format!(
            "cowd-growth-pipeline-test-{}",
            uuid::Uuid::new_v4()
        ));
        let record = LearningRecord::from_input(GrowthInput {
            selected_mode: ExecutionMode::PlanExecute,
            complexity: TaskComplexity::Complex,
            risk: TaskRisk::Medium,
            context_omitted: 0,
            tool_requires_checkpoint: false,
            tool_requires_human_confirm: false,
            verification_can_finalize: false,
            bench_passed: false,
        });
        let event = GrowthEvent::from_input(GrowthEventInput {
            session_id: "growth-session-1".to_string(),
            source_event_kind: "runtime.ai_kernel.trace".to_string(),
            strategy_mode: ExecutionMode::PlanExecute,
            learning_record: record,
            evidence_refs: vec![GrowthEvidenceRef::new(
                "runtime_trace",
                "trace-1",
                "blocked verification",
            )],
        });

        let receipt = services
            .growth
            .ingest_growth_event(
                &config_home,
                &services.memory,
                &services.matrix,
                event.clone(),
            )
            .await;

        assert!(receipt.durable, "{receipt:#?}");
        assert!(receipt.errors.is_empty(), "{receipt:#?}");
        assert!(receipt
            .promotions
            .iter()
            .any(|item| item.target == "fact.memory" && item.status == "promote"));
        assert!(receipt
            .promotions
            .iter()
            .any(|item| item.target == "fact.matrix" && item.status == "promote"));
        assert!(receipt
            .promotions
            .iter()
            .any(|item| item.target == "matrix.fact" && item.status == "promoted"));
        assert!(receipt
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "held"));
        assert_eq!(
            services
                .growth
                .durable_event_log(&config_home)
                .expect("durable events")
                .len(),
            1
        );
        assert!(!services
            .growth
            .durable_promotion_log(&config_home)
            .expect("durable promotions")
            .is_empty());
        assert!(!services
            .matrix
            .list_facts(&config_home, 10)
            .expect("matrix facts")
            .is_empty());

        let _ = std::fs::remove_dir_all(config_home);
    }
}
