use std::sync::Arc;

use approval::SharedApprovalHistoryLedger;
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
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        approval_gate: Arc<SmartApprovalGate>,
        approval_ledger: SharedApprovalHistoryLedger,
    ) -> Self {
        let resource_lifecycle =
            Arc::new(runtime::session_lifecycle::SessionLifecycleManager::new(
                runtime::session_lifecycle::SessionLifecycleConfig::default(),
            ));
        Self::new_with_config_home(
            runtime,
            task_kernel,
            surface_host,
            memory_manager,
            approval_gate,
            approval_ledger,
            resource_lifecycle,
            ::runtime::cowd_dirs::config_home_dir(),
        )
    }

    pub(crate) fn new_with_config_home(
        runtime: Arc<RuntimeService>,
        task_kernel: Arc<TaskKernel>,
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        approval_gate: Arc<SmartApprovalGate>,
        approval_ledger: SharedApprovalHistoryLedger,
        resource_lifecycle: Arc<runtime::session_lifecycle::SessionLifecycleManager>,
        config_home: impl AsRef<std::path::Path>,
    ) -> Self {
        let session_manager = Arc::new(crate::unified_session_manager::UnifiedSessionManager::new(
            Arc::clone(&runtime),
            resource_lifecycle,
            100,
        ));
        Self::new_with_session_manager(
            runtime,
            task_kernel,
            surface_host,
            memory_manager,
            approval_gate,
            approval_ledger,
            session_manager,
            config_home,
            runtime::GatewayCapacityConfig::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_session_manager(
        runtime: Arc<RuntimeService>,
        task_kernel: Arc<TaskKernel>,
        surface_host: Arc<crate::surface_host::SurfaceHost>,
        memory_manager: Option<Arc<GatewayMemoryManager>>,
        approval_gate: Arc<SmartApprovalGate>,
        approval_ledger: SharedApprovalHistoryLedger,
        session_manager: Arc<crate::unified_session_manager::UnifiedSessionManager>,
        config_home: impl AsRef<std::path::Path>,
        capacity_config: runtime::GatewayCapacityConfig,
    ) -> Self {
        let config_home = config_home.as_ref().to_path_buf();
        let app_host_binding = GatewayAppHostBinding::new();
        let command_host_runtime = Arc::clone(&runtime);
        let runtime_services = runtime.runtime_services();
        let runtime_events = RuntimeEventService::from_runtime_services(runtime_services.as_ref());
        let session_kernel = runtime.session_kernel();
        let lifecycle_kernel = runtime.lifecycle_kernel();
        let task = TaskService::with_kernel_and_runtime(task_kernel, Arc::clone(&runtime_services));
        let capacity = crate::gateway_capacity::GatewayCapacityController::new(
            crate::gateway_capacity::GatewayCapacityConfig::resolve(&capacity_config),
            Arc::clone(runtime_services.resource_manager()),
        );
        Self {
            app_registry: Arc::new(embedded_app_registry(
                &config_home,
                app_host_binding.context(),
            )),
            app_host_binding,
            runtime: Some(runtime),
            session_manager: Some(session_manager),
            runtime_events,
            surface: SurfaceService::with_host(surface_host),
            slash: SlashController::new(Some(command_host_runtime), task.clone()),
            session: SessionService::with_runtime_boundaries(session_kernel, lifecycle_kernel),
            task,
            memory: MemoryService::with_manager(memory_manager),
            approval: ApprovalService::with_gate_and_ledger(approval_gate, approval_ledger)
                .with_runtime_services(Arc::clone(&runtime_services)),
            cross_plane: CrossPlaneService::new(Arc::clone(&runtime_services)),
            mission: MissionService::new()
                .with_runtime_port(runtime::MissionRuntimePort::new(runtime_services)),
            capacity,
            ..Self::baseline_with_config_home(&config_home)
        }
    }

    pub(crate) fn baseline() -> Self {
        Self::baseline_with_config_home(::runtime::cowd_dirs::config_home_dir())
    }

    #[allow(
        clippy::expect_used,
        reason = "the in-memory cross-plane baseline is a deterministic local dependency for static command projections"
    )]
    pub(crate) fn baseline_with_config_home(config_home: impl AsRef<std::path::Path>) -> Self {
        let config_home = config_home.as_ref();
        let app_host_binding = GatewayAppHostBinding::new();
        let baseline_runtime =
            runtime::RuntimeServices::in_memory().expect("baseline runtime event projection");
        let capacity = crate::gateway_capacity::GatewayCapacityController::defaults(Arc::clone(
            baseline_runtime.resource_manager(),
        ));
        let task = TaskService::new();
        Self {
            app_registry: Arc::new(embedded_app_registry(
                config_home,
                app_host_binding.context(),
            )),
            app_host_binding,
            runtime: None,
            session_manager: None,
            runtime_events: RuntimeEventService::from_runtime_services(baseline_runtime.as_ref()),
            surface: SurfaceService::new(),
            slash: SlashController::new(None, task.clone()),
            session: SessionService::new(),
            task,
            approval: ApprovalService::new(),
            memory: MemoryService::new(),
            context: ContextService::new(),
            connector: ConnectorService::new(),
            cross_plane: CrossPlaneService::new(baseline_runtime),
            tool: ToolService::new(),
            system: SystemService::new(),
            audit: AuditService::new(),
            harness_eval: HarnessEvalService::new(),
            evolution: EvolutionService::new(),
            provider: ProviderService::new(),
            reality: RealityService::new(),
            growth: GrowthService::new_for_config_home(config_home),
            workspace: WorkspaceService::new(),
            skill: SkillService::new(),
            agent: AgentService::new(),
            matrix: MatrixService::new(),
            mission: MissionService::new(),
            capacity,
            owner: "0.9.380 GatewayServices",
            boundary_status: "0620_final_boundary",
        }
    }

    /// Product assembly injects a completely validated, immutable registry
    /// before Gateway starts serving requests. The method deliberately takes
    /// ownership so a running Gateway cannot hot-load source or mutate routes.
    #[must_use]
    pub(crate) fn with_app_registry(mut self, app_registry: cowd_app_host::AppRegistry) -> Self {
        self.app_registry = Arc::new(app_registry);
        self
    }

    #[must_use]
    pub(crate) fn app_host_context(&self) -> cowd_app_sdk::CowdAppContext {
        self.app_host_binding.context()
    }

    /// Complete the composition root after an immutable APP registry and the
    /// concrete application state both exist. APP handlers still see only the
    /// stable SDK ports supplied by this binding.
    pub(crate) fn bind_app_host_ports(&self, state: &Arc<crate::api_routes::AppState>) {
        self.app_host_binding.bind(state);
    }

    /// Associate an APP request correlation id with the Gateway-verified
    /// principal before an external APP can submit a host effect.
    pub(crate) fn bind_app_request_principal(
        &self,
        principal: &runtime::VerifiedPrincipal,
        context: &cowd_app_sdk::InvocationContext,
    ) {
        self.app_host_binding
            .bind_request_principal(principal, context);
    }

    /// Gateway only projects the Runtime-owned capability snapshot. Baseline
    /// services used by static command/tests intentionally expose core-only
    /// capabilities and never perform environment discovery.
    pub(crate) fn resource_capability_index(&self) -> runtime::ResourceCapabilityIndex {
        self.runtime
            .as_ref()
            .map_or_else(runtime::ResourceCapabilityIndex::default, |runtime| {
                runtime.resource_capability_index()
            })
    }

    pub(crate) fn refresh_resource_capabilities(&self) -> runtime::ResourceCapabilitySnapshot {
        self.runtime
            .as_ref()
            .map_or_else(runtime::ResourceCapabilitySnapshot::default, |runtime| {
                runtime.refresh_resource_capabilities()
            })
    }

    #[cfg(test)]
    pub(crate) fn with_approval_for_tests(approval_gate: Arc<SmartApprovalGate>) -> Self {
        let ledger = approval_gate.history().clone();
        Self {
            approval: ApprovalService::with_gate_and_ledger(approval_gate, ledger),
            ..Self::baseline()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_memory_for_tests(memory_manager: Arc<GatewayMemoryManager>) -> Self {
        Self {
            memory: MemoryService::with_manager(Some(memory_manager)),
            ..Self::baseline()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_session_kernel_for_tests(session_kernel: Arc<SessionKernel>) -> Self {
        Self {
            session: SessionService::with_kernel(session_kernel),
            ..Self::baseline()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_kernels_for_tests(
        session_kernel: Arc<SessionKernel>,
        task_kernel: Arc<TaskKernel>,
    ) -> Self {
        Self {
            session: SessionService::with_kernel(session_kernel),
            task: TaskService::with_kernel(task_kernel),
            ..Self::baseline()
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
            self.surface.label(),
            self.slash.label(),
            self.session.label,
            self.task.label,
            self.approval.label,
            self.memory.label,
            self.context.label,
            self.connector.label,
            self.cross_plane.label,
            self.tool.label,
            self.system.label,
            self.audit.label,
            self.harness_eval.label,
            self.evolution.label,
            self.provider.label,
            self.reality.label,
            self.growth.label,
            self.workspace.label,
            self.skill.label,
            self.agent.label,
            self.matrix.label,
            self.mission.label,
        ]
    }

    pub(crate) fn service_contracts(&self) -> Vec<ServiceEnvelope> {
        let mut contracts = Vec::new();
        contracts.extend(self.slash.contracts());
        contracts.extend(self.session.contracts());
        contracts.extend(self.task.contracts());
        contracts.extend(self.approval.contracts());
        contracts.extend(self.memory.contracts());
        contracts.extend(self.context.contracts());
        contracts.extend(self.connector.contracts());
        contracts.extend(self.cross_plane.contracts());
        contracts.extend(self.tool.contracts());
        contracts.extend(self.system.contracts());
        contracts.extend(self.audit.contracts());
        contracts.extend(self.harness_eval.contracts());
        contracts.extend(self.evolution.contracts());
        contracts.extend(self.provider.contracts());
        contracts.extend(self.reality.contracts());
        contracts.extend(self.growth.contracts());
        contracts.extend(self.workspace.contracts());
        contracts.extend(self.skill.contracts());
        contracts.extend(self.agent.contracts());
        contracts.extend(self.matrix.contracts());
        contracts.extend(self.mission.contracts());
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
            ("slash", "catalog"),
            ("slash", "projection"),
            ("slash", "resolve"),
            ("slash", "dispatch"),
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
            ("cross_plane", "summary"),
            ("cross_plane", "grants"),
            ("cross_plane", "identities"),
            ("cross_plane", "audit"),
            ("cross_plane", "execute"),
            ("tool", "approve"),
            ("tool", "deny"),
            ("system", "health"),
            ("system", "config_summary"),
            ("system", "storage_summary"),
            ("system", "runtime_summary"),
            ("audit", "approval_projection"),
            ("audit", "audit_projection"),
            ("audit", "risk_gate_projection"),
            ("harness_eval", "reports"),
            ("harness_eval", "latest_report"),
            ("harness_eval", "runs"),
            ("harness_eval", "run_start"),
            ("evolution", "signals"),
            ("evolution", "proposals"),
            ("evolution", "signal_create"),
            ("evolution", "diagnoses"),
            ("evolution", "diagnosis_create"),
            ("evolution", "missions_summary"),
            ("evolution", "mission_detail"),
            ("evolution", "proposal_create"),
            ("evolution", "proposal_detail"),
            ("evolution", "chain"),
            ("evolution", "proposal_decision"),
            ("evolution", "skill_draft"),
            ("provider", "config_projection"),
            ("provider", "model_routing"),
            ("reality", "status"),
            ("reality", "static"),
            ("reality", "flow"),
            ("reality", "promotions"),
            ("reality", "boundaries"),
            ("growth", "risk_gate_event"),
            ("growth", "memory_candidates"),
            ("growth", "matrix_signals"),
            ("growth", "event_log"),
            ("workspace", "overview"),
            ("skill", "catalog"),
            ("skill", "projection"),
            ("agent", "list"),
            ("agent", "task_projection"),
            ("matrix", "health"),
            ("mfg", "health"),
            ("mfg", "incident"),
            ("mfg", "analysis"),
            ("mfg", "skill_run"),
            ("mission", "projection"),
            ("mission", "session_control"),
            ("mission", "approval_projection"),
            ("mission", "relation_projection"),
        ]
        .into_iter()
        .all(|(service, operation)| has(service, operation))
    }
}

/// Build the deterministic first-party application registry for local/test
/// Gateway instances. Production startup replaces this with the same static
/// registry using broker-backed live credential revalidation before accepting
/// traffic.  The APP source itself remains external; this helper owns only
/// product assembly policy.
fn embedded_app_registry(
    config_home: &std::path::Path,
    host_context: cowd_app_sdk::CowdAppContext,
) -> cowd_app_host::AppRegistry {
    embedded_app_registry_with_policy(config_home, host_context, &runtime::AppsConfig::default())
}

fn embedded_app_registry_with_policy(
    config_home: &std::path::Path,
    host_context: cowd_app_sdk::CowdAppContext,
    apps: &runtime::AppsConfig,
) -> cowd_app_host::AppRegistry {
    let mut registry = cowd_app_host::AppRegistry::default();
    cowd_product_apps::register_enabled(&mut registry, config_home, host_context, &|app_id| {
        apps.is_enabled(app_id)
    })
    .expect("static APP product contributions must have valid descriptors");
    registry
}

/// The set of application descriptors admitted to this Gateway process.
///
/// Source selection is deliberately already complete at this point: this
/// function only filters compile-time linked product contributions according
/// to the unified startup policy.  The same result is used for broker
/// capabilities and the AppRegistry so an APP cannot be authorised without
/// being mounted, or mounted without being authorised.
pub(crate) fn enabled_app_descriptors(
    apps: &runtime::AppsConfig,
) -> Vec<cowd_app_sdk::AppDescriptor> {
    cowd_product_apps::enabled_descriptors(&|app_id| apps.is_enabled(app_id))
}

pub(crate) fn broker_backed_app_registry(
    config_home: impl AsRef<std::path::Path>,
    host_context: cowd_app_sdk::CowdAppContext,
    apps: &runtime::AppsConfig,
) -> cowd_app_host::AppRegistry {
    let mut registry = cowd_app_host::AppRegistry::default();
    cowd_product_apps::register_enabled(
        &mut registry,
        config_home.as_ref(),
        host_context,
        &|app_id| apps.is_enabled(app_id),
    )
    .expect("static APP product contributions must have valid descriptors");
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_app_is_absent_from_descriptor_and_router_composition() {
        let apps = runtime::AppsConfig::default().with_app_enabled("mfg", false);
        let binding = GatewayAppHostBinding::new();
        let registry = broker_backed_app_registry(std::env::temp_dir(), binding.context(), &apps);

        assert!(enabled_app_descriptors(&apps).is_empty());
        assert!(registry.apps().is_empty());
        assert!(registry.skills().is_empty());
        assert!(registry.storage_endpoints().is_empty());
    }

    #[test]
    fn enabled_app_registry_owns_source_storage_and_surface_truth_together() {
        let apps = runtime::AppsConfig::default();
        let binding = GatewayAppHostBinding::new();
        let config_home =
            std::env::temp_dir().join(format!("cowd-v579-registry-{}", std::process::id()));
        let registry = broker_backed_app_registry(&config_home, binding.context(), &apps);
        let registered = registry.apps();
        let mfg = registered
            .iter()
            .find(|app| app.descriptor.id.as_str() == "mfg")
            .expect("enabled MFG descriptor");
        assert!(mfg.source_lock.is_some());
        let storage = mfg.storage.as_ref().expect("MFG storage contract");
        assert_eq!(storage.contract.migration_owner.as_str(), "mfg");
        assert_eq!(storage.provisions.len(), 1);
        assert_eq!(
            storage.provisions[0].backend,
            cowd_app_sdk::AppStorageBackend::Sqlite
        );
        assert!(registry.storage_endpoints().iter().any(|endpoint| {
            endpoint.domain == storage::StorageDomainId::app("mfg", "primary")
        }));
        let projection = serde_json::to_string(&registered).expect("registry projection");
        assert!(projection.contains("db13d02e611840b95160c8a4effb383c0835e399"));
        assert!(!projection.contains(".sqlite"));
        assert!(!projection.contains(config_home.to_string_lossy().as_ref()));
        let _ = std::fs::remove_dir_all(config_home);
    }
}
