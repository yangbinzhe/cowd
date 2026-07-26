use std::{
    collections::HashMap,
    path::Path,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

use harness_contract::policy::RiskGateReceipt;

use crate::runtime_service::RuntimeService;
use memory::CognitiveContextManager;

mod agent_service;
mod app_host_ports;
mod approval_service;
mod connector_service;
mod context;
mod context_service;
mod cross_plane_executor;
mod cross_plane_service;
mod error;
mod evolution_service;
mod growth_service;
pub(crate) mod harness_eval_service;
mod matrix_app_reality;
mod matrix_service;
mod memory_service;
mod mission_service;
mod policy;
pub(crate) mod reality_service;
mod receipt;
mod registry;
mod runtime_event_service;
pub(crate) mod session_service;
mod skill_service;
mod slash_controller;
mod surface_service;
mod system_service;
mod task_service;
mod workspace_service;

pub(crate) use app_host_ports::GatewayAppHostBinding;
pub(crate) use approval_service::ApprovalService;
pub(crate) use context_service::ContextServiceError;
pub(crate) use cross_plane_executor::{GatewayConnectorServiceExecutor, GatewayCrossPlaneExecutor};
pub(crate) use cross_plane_service::CrossPlaneExecutionRecord;
pub(crate) use evolution_service::{
    EvolutionProposalCreateRequest, EvolutionProposalDecisionRequest, EvolutionServiceError,
    EvolutionSignalCreateRequest,
};
pub(crate) use growth_service::GrowthPromotionReceipt;
pub(crate) use harness_eval_service::HarnessEvalServiceError;
pub(crate) use matrix_service::MatrixService;
pub(crate) use memory_service::MemoryService;
pub(crate) use mission_service::{
    AddMissionRelationHttpRequest, CreateMissionScheduleHttpRequest,
    DecideMissionApprovalHttpRequest, InterpretMissionCommandHttpRequest,
    StartMissionSessionHttpRequest, SubmitMissionApprovalHttpRequest,
    UpdateMissionScheduleHttpRequest, UpsertMissionProxyHttpRequest,
};
pub(crate) use reality_service::RealityService;
pub(crate) use receipt::{service_envelope, ServiceEnvelope};
pub(crate) use registry::{
    broker_backed_app_registry_with_storage, embedded_app_registry, enabled_app_descriptors,
};
pub(crate) use runtime_event_service::RuntimeEventService;
pub(crate) use session_service::{
    ActiveMessagesPage, EnsureSessionRequest, SessionCompactResult, SessionMessageCounts,
    SessionService, SessionSource, SessionStatsSnapshot, SessionTokenCounts, SessionUpdateRequest,
};
pub(crate) use skill_service::profile_provider::runtime_skill_assets_for_workspace;
pub(crate) use skill_service::{
    SkillActionRequest, SkillCatalogQuery, SkillFileQuery, SkillMaintenanceEvaluateRequest,
    SkillProjectionQuery, SkillServiceError,
};
pub(crate) use slash_controller::SlashController;
pub(crate) use surface_service::SurfaceService;
pub(crate) use task_service::TaskService;

pub(crate) type GatewayMemoryManager = CognitiveContextManager;
pub(crate) type GatewayMatrixRepositoryError = ::matrix_repository::MatrixStoreError;
pub(crate) type RuntimeContextBoundary = runtime::ContextRuntimeKernel;

#[allow(dead_code)]
pub(crate) fn process_cwd_lock() -> &'static Mutex<()> {
    system_service::process_cwd_lock()
}

#[derive(Clone)]
pub(crate) struct ContextService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    artifact_store: Option<Arc<runtime::ArtifactStore>>,
}

impl ContextService {
    pub(crate) fn new() -> Self {
        Self {
            label: "context",
            owner: "0.9.315 Context service boundary",
            artifact_store: None,
        }
    }

    pub(crate) fn with_artifact_store(mut self, store: Arc<runtime::ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
        self
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }
}

#[derive(Clone)]
pub(crate) struct ConnectorService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    resource_directory_factory: Arc<dyn connector::ResourceDirectoryFactory>,
    resource_directory_handle: Option<storage::StorageHandle>,
}

impl ConnectorService {
    pub(crate) fn new() -> Self {
        Self {
            label: "connector",
            owner: "0.9.315 Connector service boundary",
            resource_directory_factory: Arc::new(connector::SqliteResourceDirectoryFactory),
            resource_directory_handle: None,
        }
    }

    pub(crate) fn with_resource_directory_factory(
        factory: Arc<dyn connector::ResourceDirectoryFactory>,
        handle: storage::StorageHandle,
    ) -> Self {
        Self {
            label: "connector",
            owner: "0.9.567 Connector durable port",
            resource_directory_factory: factory,
            resource_directory_handle: Some(handle),
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
    runtime_services: Arc<runtime::RuntimeServices>,
}

impl CrossPlaneService {
    pub(crate) fn new(runtime_services: Arc<runtime::RuntimeServices>) -> Self {
        Self {
            label: "cross_plane",
            owner: "0.9.315 Cross-plane service boundary",
            runtime_services,
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

#[derive(Clone)]
pub(crate) struct HarnessEvalService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    pub(crate) active_jobs: HarnessEvalJobRegistry,
    pub(crate) gateway_tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
}

/// Process-local worker registry owned by the Gateway service instance.
/// Durable run state remains in `HarnessEvalReportStore`; this only tracks
/// cancellable workers currently owned by this process.
#[derive(Clone)]
pub(crate) struct ActiveHarnessEvalJob {
    pub(crate) run_id: String,
    pub(crate) level: String,
    pub(crate) requested_at_ms: u128,
    pub(crate) cancel_requested: Arc<AtomicBool>,
    pub(crate) cancellation: runtime::CancellationToken,
}

pub(crate) type HarnessEvalJobRegistry = Arc<Mutex<HashMap<String, ActiveHarnessEvalJob>>>;

#[derive(Clone)]
pub(crate) struct EvolutionService {
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
            owner: "0.9.380 Provider service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn config_projection(
        &self,
        runtime_config: &runtime::RuntimeConfig,
        active_snapshot: Option<&runtime::ProviderRegistrySnapshot>,
    ) -> serde_json::Value {
        let configured_providers = runtime_config.providers();
        let registry = model_protocol::model_registry::global_registry();
        let configured_model = runtime_config.model().map(str::to_string);
        let config_source = if runtime_config.loaded_entries().is_empty() {
            "default"
        } else {
            "config"
        };
        let configured_catalog =
            provider::ProviderCatalog::from_input(provider::ProviderCatalogInput {
                providers: configured_providers,
                registry,
                model_context_windows: runtime_config.model_context_windows(),
                max_output_tokens_override: runtime_config.plugins().max_output_tokens(),
                configured_model: configured_model.as_deref(),
                aliases: runtime_config.aliases(),
                config_source,
                extra_sources: Vec::new(),
                transforms: Vec::new(),
                warnings: Vec::new(),
            });
        let active_provider_revision =
            active_snapshot.map(runtime::ProviderRegistrySnapshot::revision);
        let active_matches_configured =
            active_snapshot.is_none_or(|snapshot| snapshot.config() == configured_providers);
        let catalog = active_snapshot.map_or_else(
            || configured_catalog.clone(),
            |snapshot| {
                provider::ProviderCatalog::from_input(provider::ProviderCatalogInput {
                    providers: snapshot.config(),
                    registry,
                    model_context_windows: runtime_config.model_context_windows(),
                    max_output_tokens_override: runtime_config.plugins().max_output_tokens(),
                    configured_model: configured_model.as_deref(),
                    aliases: runtime_config.aliases(),
                    config_source: "active_runtime",
                    extra_sources: Vec::new(),
                    transforms: Vec::new(),
                    warnings: Vec::new(),
                })
            },
        );
        let configured_catalog_generation = configured_catalog.generation.clone();
        let configured_model_provider = catalog
            .profiles
            .iter()
            .find(|profile| profile.id == "default")
            .and_then(|profile| profile.provider.clone());
        let catalog_generation = catalog.generation.clone();
        let provider_count = catalog.providers.len();
        let provider_model_count = catalog.models.len();
        let catalog_profiles = catalog.profiles.clone();
        let catalog_warnings = catalog.warnings.clone();
        let provider_rows = catalog
            .providers
            .iter()
            .map(|provider| {
                let provider_models = catalog
                    .models
                    .iter()
                    .filter(|model| model.provider == provider.id)
                    .map(|model| model.id.clone())
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "name": provider.name,
                    "base_url": provider.base_url,
                    "protocol": provider.configured_protocol,
                    "effective_protocol": provider.effective_protocol,
                    "protocol_configured": provider.protocol_configured,
                    "models": provider_models,
                    "model_count": provider.model_count,
                    "credential_present": provider.credential_present,
                    "catalog_generation": catalog_generation.clone(),
                })
            })
            .collect::<Vec<_>>();
        let models = catalog
            .models
            .iter()
            .map(|model| {
                serde_json::json!({
                    "id": model.id,
                    "name": model.name,
                    "display_name": model.display_name,
                    "provider": model.provider,
                    "effective_protocol": model.effective_protocol,
                    "protocol_configured": model.protocol_configured,
                    "selected": model.selected,
                    "context_window_tokens": model.context_window_tokens,
                    "context_window_source": model.context_window_source,
                    "max_output_tokens": model.max_output_tokens,
                    "max_output_source": model.max_output_source,
                    "capabilities": model.capabilities,
                    "catalog_generation": catalog_generation.clone(),
                })
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "envelope": self.envelope("config_projection"),
            "catalog": catalog,
            "catalog_generation": catalog_generation,
            "configured_catalog_generation": configured_catalog_generation,
            "active_provider_revision": active_provider_revision,
            "active_matches_configured": active_matches_configured,
            "providers": provider_rows,
            "models": models,
            "profiles": catalog_profiles,
            "warnings": catalog_warnings,
            "provider_count": provider_count,
            "provider_model_count": provider_model_count,
            "configured_model": configured_model,
            "configured_model_provider": configured_model_provider,
            "configured_model_resolved": configured_model.is_none() || configured_model_provider.is_some(),
            "activation_scope": {
                "provider_credentials_and_protocol": "subsequent_provider_checkout",
                "model_capacity_overrides": "new_or_restored_session_runtime",
                "models_yaml": "gateway_restart",
            },
        })
    }
}

#[derive(Clone)]
pub(crate) struct GrowthService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    ledger: Arc<dyn fact_kernel::FactLedger>,
}

impl GrowthService {
    pub(crate) fn new() -> Self {
        Self::new_for_config_home(runtime::cowd_dirs::config_home_dir())
    }

    pub(crate) fn new_for_config_home(config_home: impl AsRef<Path>) -> Self {
        let registry = storage::StorageRegistry::default_for_config_home(config_home);
        let ledger = registry
            .endpoint(&storage::StorageDomainId::Fact)
            .map_err(|error| error.to_string())
            .and_then(|fact_endpoint| {
                let growth_endpoint = registry
                    .endpoint(&storage::StorageDomainId::Growth)
                    .map_err(|error| error.to_string())?;
                fact_sqlite::SqliteFactLedger::open_with_legacy_growth(fact_endpoint, growth_endpoint)
                    .map_err(|error| error.to_string())
            })
            .map(|ledger| Arc::new(ledger) as Arc<dyn fact_kernel::FactLedger>)
            .unwrap_or_else(|error| {
                tracing::error!(%error, "fact/growth ledger unavailable; growth operations will fail closed");
                Arc::new(fact_kernel::UnavailableFactLedger::new(error))
            });
        Self {
            label: "growth",
            owner: "0.9.380 Growth service boundary",
            ledger,
        }
    }

    pub(crate) fn with_ledger(ledger: Arc<dyn fact_kernel::FactLedger>) -> Self {
        Self {
            label: "growth",
            owner: "0.9.573 Growth service boundary",
            ledger,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_ledger_for_tests(ledger: Arc<dyn fact_kernel::FactLedger>) -> Self {
        Self::with_ledger(ledger)
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

#[derive(Clone)]
pub(crate) struct MissionService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    runtime_port: Option<runtime::MissionRuntimePort>,
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
    /// Immutable process-wide durable backend composition retained for health
    /// and APP provisioning. Business services consume only its typed ports.
    pub(crate) selected_storage: Option<Arc<crate::selected_storage::SelectedStorageTopology>>,
    /// Product-composed APP catalogue. The core host only consumes its generic
    /// descriptors and routers; it never imports an APP implementation.
    pub(crate) app_registry: Arc<cowd_app_host::AppRegistry>,
    /// Generic APP-to-host effect binding. This is deliberately separate from
    /// the immutable registry so product startup can compose descriptors
    /// before the final `AppState` exists.
    pub(crate) app_host_binding: GatewayAppHostBinding,
    pub(crate) runtime: Option<Arc<RuntimeService>>,
    pub(crate) runtime_events: RuntimeEventService,
    pub(crate) surface: SurfaceService,
    pub(crate) slash: SlashController,
    pub(crate) session: Arc<SessionService>,
    pub(crate) task: TaskService,
    pub(crate) approval: ApprovalService,
    pub(crate) memory: MemoryService,
    pub(crate) context: ContextService,
    pub(crate) connector: ConnectorService,
    pub(crate) cross_plane: CrossPlaneService,
    pub(crate) tool: ToolService,
    pub(crate) system: SystemService,
    pub(crate) audit: AuditService,
    pub(crate) harness_eval: HarnessEvalService,
    pub(crate) evolution: EvolutionService,
    pub(crate) provider: ProviderService,
    pub(crate) reality: RealityService,
    pub(crate) growth: GrowthService,
    pub(crate) workspace: WorkspaceService,
    pub(crate) skill: SkillService,
    pub(crate) agent: AgentService,
    pub(crate) matrix: MatrixService,
    pub(crate) mission: MissionService,
    pub(crate) capacity: crate::gateway_capacity::GatewayCapacityController,
    pub(crate) owner: &'static str,
    pub(crate) boundary_status: &'static str,
}

impl GatewayServices {
    pub(crate) fn artifact_store(&self) -> Option<Arc<runtime::ArtifactStore>> {
        self.selected_storage
            .as_ref()
            .map(|topology| Arc::clone(&topology.artifact_store))
            .or_else(|| {
                self.runtime
                    .as_ref()
                    .map(|runtime| Arc::clone(runtime.runtime_services().artifact_store()))
            })
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

    pub(crate) fn knowledge(&self) -> ServiceEnvelope {
        self.envelope("knowledge")
    }

    fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.status(), self.list(), self.query(), self.knowledge()]
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
    use harness_contract::{
        core::{ExecutionPattern, TaskComplexity, TaskRisk},
        growth::{GrowthEvent, GrowthEventInput, GrowthEvidenceRef, GrowthInput, LearningRecord},
    };

    #[test]
    fn baseline_registers_the_complete_gateway_service_contract() {
        let services = GatewayServices::baseline();
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
                "harness_eval",
                "evolution",
                "provider",
                "reality",
                "growth",
                "workspace",
                "skill",
                "agent",
                "matrix",
                "mission",
            ]
        );
        assert!(services.has_minimum_service_contract());
        assert_eq!(services.session.create_session().operation, "create");
        assert_eq!(services.session.chat().status, "service_ready");
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
            services.harness_eval.envelope("reports").operation,
            "reports"
        );
        let evolution_contracts = services.evolution.contracts();
        assert!(evolution_contracts
            .iter()
            .any(|contract| contract.operation == "signals"));
        assert!(evolution_contracts
            .iter()
            .any(|contract| contract.operation == "proposals"));
        assert!(evolution_contracts.iter().all(|contract| {
            !contract.operation.contains("candidate")
                && !contract.operation.contains("sandbox")
                && !contract.operation.contains("rollback")
        }));
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
        let config_home = std::env::temp_dir().join(format!(
            "cowd-growth-pipeline-test-{}",
            uuid::Uuid::new_v4()
        ));
        let services = GatewayServices::baseline_with_config_home(&config_home);
        let record = LearningRecord::from_input(GrowthInput {
            selected_pattern: ExecutionPattern::Execute,
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
            source_event_kind: "runtime.harness_contract.trace".to_string(),
            strategy_pattern: ExecutionPattern::Execute,
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
                .durable_event_log()
                .expect("durable events")
                .len(),
            1
        );
        assert!(!services
            .growth
            .durable_promotion_log()
            .expect("durable promotions")
            .is_empty());
        assert!(!services
            .matrix
            .list_facts(&config_home, 10)
            .expect("matrix facts")
            .is_empty());

        let fact_count_before_replay = services
            .growth
            .list_fact_records()
            .expect("durable facts")
            .len();
        let replay = services
            .growth
            .ingest_growth_event(&config_home, &services.memory, &services.matrix, event)
            .await;
        assert!(replay.durable, "{replay:#?}");
        assert!(replay.errors.is_empty(), "{replay:#?}");
        assert_eq!(
            services
                .growth
                .durable_event_log()
                .expect("replayed durable events")
                .len(),
            1,
            "same event id must not create a second Growth event"
        );
        assert_eq!(
            services
                .growth
                .list_fact_records()
                .expect("replayed durable facts")
                .len(),
            fact_count_before_replay,
            "deterministic fact ids must make event replay idempotent"
        );

        let _ = std::fs::remove_dir_all(config_home);
    }
}
