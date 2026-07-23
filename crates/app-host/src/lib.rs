//! Generic host-side application composition.
//!
//! This crate has no product application imports. Product bundles construct
//! contributions; Gateway, TUI and WebUI consume the registry projection.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use axum::Router;
use cowd_app_sdk::{
    AppContractError, AppDescriptor, AppHealth, AppHttpContract, AppId, AppRouteMetadata,
    AppSkillDescriptor, AppSourceLock, AppStorageContract, AppStorageProvision,
    AppStorageRequirement, CapabilityApp, CowdAppContext,
};
use crossterm::event::KeyEvent;
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    Frame,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Clone)]
pub struct HttpAppContribution {
    pub router: Router,
}

impl std::fmt::Debug for HttpAppContribution {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpAppContribution")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiAppContribution {
    pub panel_id: String,
    pub title: String,
    pub actions: Vec<String>,
}

/// Stable, product-neutral visual tokens for an APP-owned terminal panel.
///
/// The Cowd TUI owns theme selection and converts its skin to this compact
/// palette before rendering an external APP.  APP code deliberately never
/// receives `SkinConfig`, `ThemeEngine`, or another private TUI type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TuiAppTheme {
    pub foreground: Color,
    pub muted: Color,
    pub accent: Color,
    pub success: Color,
    pub warning: Color,
    pub danger: Color,
    pub border: Color,
}

impl TuiAppTheme {
    #[must_use]
    pub const fn dark() -> Self {
        Self {
            foreground: Color::White,
            muted: Color::DarkGray,
            accent: Color::Cyan,
            success: Color::Green,
            warning: Color::Yellow,
            danger: Color::Red,
            border: Color::DarkGray,
        }
    }

    #[must_use]
    pub const fn foreground_style(self) -> Style {
        Style::new().fg(self.foreground)
    }

    #[must_use]
    pub const fn muted_style(self) -> Style {
        Style::new().fg(self.muted)
    }

    #[must_use]
    pub const fn accent_style(self) -> Style {
        Style::new().fg(self.accent)
    }
}

/// Read-only context supplied to an APP panel while Cowd renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TuiAppRenderContext {
    pub theme: TuiAppTheme,
    pub focused: bool,
}

/// A generic, app-owned action displayed by the terminal command palette.
/// The host does not interpret its risk policy: it asks the owning panel to
/// dispatch the action and only executes the resulting generic effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiAppAction {
    pub id: String,
    pub label: String,
    pub description: String,
    pub domain: String,
    pub risk: String,
    pub requires_confirmation: bool,
    pub enabled: bool,
    pub unavailable_reason: Option<String>,
}

/// Host-to-APP envelope. Payloads are intentionally JSON values: the APP
/// owns its canonical typed contract and deserializes only its own messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TuiAppEvent {
    Response {
        request_id: String,
        status: u16,
        body: Value,
    },
    RequestFailed {
        request_id: String,
        #[serde(default)]
        status: Option<u16>,
        #[serde(default)]
        body: Option<Value>,
        error: String,
    },
    LiveEnvelope {
        subscription_id: String,
        body: Value,
    },
    LiveFailed {
        subscription_id: String,
        #[serde(default)]
        status: Option<u16>,
        #[serde(default)]
        body: Option<Value>,
        error: String,
    },
    LiveStopped {
        subscription_id: String,
    },
}

/// APP-to-host command. Cowd performs network authentication, task lifetime,
/// cross-panel navigation and composer insertion; the APP owns endpoint
/// selection, request body, live semantics and state reduction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TuiAppEffect {
    Request {
        request_id: String,
        method: String,
        path: String,
        body: Option<Value>,
        /// APP-provided request metadata. The host must reject attempts to
        /// override authentication, surface identity or any reserved Cowd
        /// transport header before the request is sent.
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    Subscribe {
        subscription_id: String,
        path: String,
        /// Same reserved-header policy as [`Self::Request`]. This carries
        /// APP-owned cursors or epochs, never credentials.
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    Unsubscribe {
        subscription_id: String,
    },
    Navigate {
        route: String,
        /// Optional application-owned navigation context. Cowd recognizes
        /// only its generic backlink envelope and validates the canonical
        /// identity before a core panel can display the resolved object.
        #[serde(default)]
        context: Option<Value>,
    },
    Composer {
        text: String,
    },
    Notice {
        level: TuiAppNoticeLevel,
        title: Option<String>,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TuiAppNoticeLevel {
    Info,
    Warning,
    Error,
}

/// Mutable effect sink passed to an APP panel. It has no network client,
/// credential, Gateway service, Tokio handle or Cowd state access.
#[derive(Debug, Default)]
pub struct TuiAppEffects {
    effects: Vec<TuiAppEffect>,
}

impl TuiAppEffects {
    pub fn push(&mut self, effect: TuiAppEffect) {
        self.effects.push(effect);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }

    pub fn take(&mut self) -> Vec<TuiAppEffect> {
        std::mem::take(&mut self.effects)
    }
}

/// Complete APP-owned TUI behavior. The trait intentionally contains the
/// whole user-facing seam (rendering, input, app event reduction, palette and
/// slash dispatch), so the Cowd TUI host cannot grow an APP-specific branch.
pub trait TuiAppPanel: Send {
    fn panel_id(&self) -> &str;

    /// Called once after the host has mounted a fresh APP panel. The APP can
    /// request its contract/snapshot without receiving a runtime handle.
    fn on_mount(&mut self, _effects: &mut TuiAppEffects) {}

    fn render(&mut self, frame: &mut Frame<'_>, area: Rect, context: TuiAppRenderContext);

    fn handle_key(&mut self, key: KeyEvent, effects: &mut TuiAppEffects) -> bool;

    fn apply_event(&mut self, event: TuiAppEvent, effects: &mut TuiAppEffects);

    fn actions(&self) -> Vec<TuiAppAction>;

    fn dispatch_action(&mut self, action_id: &str, effects: &mut TuiAppEffects) -> bool;

    fn handle_command(&mut self, command: &str, effects: &mut TuiAppEffects) -> bool;
}

/// A factory is registered at product startup; each TUI process creates its
/// own panel and state.  It prevents any APP state from leaking through the
/// global Gateway registry.
pub trait TuiAppPanelFactory: Send + Sync {
    fn create(&self) -> Box<dyn TuiAppPanel>;
}

impl<F> TuiAppPanelFactory for F
where
    F: Fn() -> Box<dyn TuiAppPanel> + Send + Sync,
{
    fn create(&self) -> Box<dyn TuiAppPanel> {
        self()
    }
}

/// Product composition of a descriptor already visible to `AppRegistry` and
/// the concrete external panel factory consumed by the generic TUI host.
#[derive(Clone)]
pub struct TuiAppSurfaceContribution {
    pub app_id: AppId,
    pub descriptor: TuiAppContribution,
    factory: Arc<dyn TuiAppPanelFactory>,
}

impl std::fmt::Debug for TuiAppSurfaceContribution {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TuiAppSurfaceContribution")
            .field("app_id", &self.app_id)
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl TuiAppSurfaceContribution {
    #[must_use]
    pub fn new(
        app_id: AppId,
        descriptor: TuiAppContribution,
        factory: Arc<dyn TuiAppPanelFactory>,
    ) -> Self {
        Self {
            app_id,
            descriptor,
            factory,
        }
    }

    #[must_use]
    pub fn create_panel(&self) -> Box<dyn TuiAppPanel> {
        self.factory.create()
    }

    pub fn validate(&self) -> Result<(), AppRegistryError> {
        if self.descriptor.panel_id.trim().is_empty() {
            return Err(AppRegistryError::InvalidTuiContribution {
                app_id: self.app_id.clone(),
                reason: "panel_id is empty".to_string(),
            });
        }
        if self.descriptor.title.trim().is_empty() {
            return Err(AppRegistryError::InvalidTuiContribution {
                app_id: self.app_id.clone(),
                reason: "title is empty".to_string(),
            });
        }
        let mut actions = BTreeSet::new();
        if self
            .descriptor
            .actions
            .iter()
            .any(|action| action.trim().is_empty() || !actions.insert(action.as_str()))
        {
            return Err(AppRegistryError::InvalidTuiContribution {
                app_id: self.app_id.clone(),
                reason: "actions contain an empty or duplicate id".to_string(),
            });
        }
        Ok(())
    }
}

pub struct AppContribution {
    pub app: Box<dyn CapabilityApp>,
    pub http: Option<HttpAppContribution>,
    pub tui: Option<TuiAppContribution>,
}

/// Product-neutral registration record for a route semantic projection. It
/// keeps the owning App visible in public inventories without exposing any
/// domain DTO, route enum or policy table to Gateway.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RegisteredAppRouteMetadata {
    pub app_id: AppId,
    pub route: AppRouteMetadata,
    pub auth_error_schema: Option<String>,
}

/// The host supplies only a common auth status and message. The App owns its
/// public error JSON envelope.
pub type AppErrorEnvelopeMapper = fn(u16, String) -> Value;

#[derive(Clone)]
struct RegisteredAppHttpContract {
    contract: AppHttpContract,
    error_mapper: AppErrorEnvelopeMapper,
}

/// A deterministic App-owned release predicate. It receives no Gateway
/// service, token or mutable runtime state.
#[derive(Debug, Clone)]
pub struct AppQualityCheck {
    pub id: String,
    pub verify: fn() -> bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AppQualityCheckResult {
    pub app_id: AppId,
    pub id: String,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredApp {
    pub descriptor: AppDescriptor,
    pub health: AppHealth,
    pub http_registered: bool,
    pub tui_registered: bool,
    pub source_lock: Option<AppSourceLock>,
    pub storage: Option<AppStorageRegistration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppStorageRegistration {
    pub contract: AppStorageContract,
    pub provisions: Vec<AppStorageProvision>,
}

#[derive(Clone)]
enum AppStorageResource {
    Sqlite(storage::SqliteExecutor),
    #[cfg(feature = "app-postgres")]
    Postgres(storage::PostgresExecutor),
    Artifact(AppArtifactLease),
}

/// A host-owned artifact root.  APP code can perform bounded relative I/O but
/// cannot inspect or replace the physical root chosen by the deployment.
#[derive(Clone)]
pub struct AppArtifactLease {
    endpoint: storage::StorageEndpoint,
}

impl AppArtifactLease {
    fn new(endpoint: storage::StorageEndpoint) -> Self {
        Self { endpoint }
    }

    fn resolved_path(&self, relative: &std::path::Path) -> Result<PathBuf, AppRegistryError> {
        use std::path::Component;
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
        {
            return Err(AppRegistryError::InvalidArtifactPath(
                self.endpoint.logical_id(),
            ));
        }
        match self.endpoint.backend {
            storage::StorageBackendKind::FileJson => {
                if relative.as_os_str().is_empty() || relative == std::path::Path::new(".") {
                    Ok(self.endpoint.path.clone())
                } else {
                    Err(AppRegistryError::InvalidArtifactPath(
                        self.endpoint.logical_id(),
                    ))
                }
            }
            storage::StorageBackendKind::Directory | storage::StorageBackendKind::BlobDirectory => {
                Ok(self.endpoint.path.join(relative))
            }
            _ => Err(AppRegistryError::InvalidArtifactPath(
                self.endpoint.logical_id(),
            )),
        }
    }

    pub fn read(&self, relative: impl AsRef<std::path::Path>) -> Result<Vec<u8>, AppRegistryError> {
        Ok(std::fs::read(self.resolved_path(relative.as_ref())?)?)
    }

    pub fn write(
        &self,
        relative: impl AsRef<std::path::Path>,
        value: &[u8],
    ) -> Result<(), AppRegistryError> {
        let target = self.resolved_path(relative.as_ref())?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        static ARTIFACT_WRITE_NONCE: AtomicU64 = AtomicU64::new(1);
        let temporary = target.with_extension(format!(
            "cowd-tmp-{}-{}",
            std::process::id(),
            ARTIFACT_WRITE_NONCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&temporary, value)?;
        std::fs::rename(temporary, target)?;
        Ok(())
    }
}

/// One ready, host-provisioned storage capability.  Its public metadata is
/// path-free; engine access is limited to the selected bounded executor.
#[derive(Clone)]
pub struct AppStorageLease {
    endpoint: storage::StorageEndpoint,
    provision: AppStorageProvision,
    resource: AppStorageResource,
}

impl AppStorageLease {
    #[must_use]
    pub fn sqlite(
        endpoint: storage::StorageEndpoint,
        provision: AppStorageProvision,
        executor: storage::SqliteExecutor,
    ) -> Self {
        Self {
            endpoint,
            provision,
            resource: AppStorageResource::Sqlite(executor),
        }
    }

    #[must_use]
    #[cfg(feature = "app-postgres")]
    pub fn postgres(
        endpoint: storage::StorageEndpoint,
        provision: AppStorageProvision,
        executor: storage::PostgresExecutor,
    ) -> Self {
        Self {
            endpoint,
            provision,
            resource: AppStorageResource::Postgres(executor),
        }
    }

    #[must_use]
    pub fn artifact(endpoint: storage::StorageEndpoint, provision: AppStorageProvision) -> Self {
        Self {
            resource: AppStorageResource::Artifact(AppArtifactLease::new(endpoint.clone())),
            endpoint,
            provision,
        }
    }

    #[must_use]
    pub fn provision(&self) -> &AppStorageProvision {
        &self.provision
    }

    #[must_use]
    pub fn sqlite_executor(&self) -> Option<&storage::SqliteExecutor> {
        match &self.resource {
            AppStorageResource::Sqlite(executor) => Some(executor),
            _ => None,
        }
    }

    #[must_use]
    #[cfg(feature = "app-postgres")]
    pub fn postgres_executor(&self) -> Option<&storage::PostgresExecutor> {
        match &self.resource {
            AppStorageResource::Postgres(executor) => Some(executor),
            _ => None,
        }
    }

    #[must_use]
    pub fn artifact_lease(&self) -> Option<&AppArtifactLease> {
        match &self.resource {
            AppStorageResource::Artifact(lease) => Some(lease),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct AppStorageLeases {
    app_id: AppId,
    leases: BTreeMap<(String, cowd_app_sdk::AppStorageScope), AppStorageLease>,
}

/// Canonical proof returned by one APP-owned storage migration.  Cowd owns
/// orchestration and activation, while the APP remains the only component
/// that understands its schema and record counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppStorageDomainMigrationEvidence {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    #[serde(default)]
    pub record_counts: BTreeMap<String, u64>,
}

impl AppStorageDomainMigrationEvidence {
    pub fn validate(&self) -> Result<(), AppRegistryError> {
        if self.domain.trim().is_empty()
            || self.source_digest.trim().is_empty()
            || self.source_digest != self.target_digest
        {
            return Err(AppRegistryError::InvalidStorageMigrationEvidence(
                self.domain.clone(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppStorageMigrationEvidence {
    pub app_id: AppId,
    pub contract_version: u32,
    pub domains: Vec<AppStorageDomainMigrationEvidence>,
}

impl AppStorageMigrationEvidence {
    pub fn validate_for(&self, app_id: &AppId) -> Result<(), AppRegistryError> {
        if &self.app_id != app_id || self.contract_version == 0 || self.domains.is_empty() {
            return Err(AppRegistryError::InvalidStorageMigrationEvidence(
                app_id.to_string(),
            ));
        }
        let mut domains = BTreeMap::new();
        for evidence in &self.domains {
            evidence.validate()?;
            if domains.insert(evidence.domain.clone(), ()).is_some() {
                return Err(AppRegistryError::InvalidStorageMigrationEvidence(
                    evidence.domain.clone(),
                ));
            }
        }
        Ok(())
    }
}

impl AppStorageLeases {
    pub fn new(app_id: AppId, leases: Vec<AppStorageLease>) -> Result<Self, AppRegistryError> {
        let mut indexed = BTreeMap::new();
        for lease in leases {
            let key = (
                lease.provision.domain.clone(),
                lease.provision.scope.clone(),
            );
            if indexed.insert(key, lease).is_some() {
                return Err(AppRegistryError::DuplicateStorageLease(app_id));
            }
        }
        Ok(Self {
            app_id,
            leases: indexed,
        })
    }

    #[must_use]
    pub fn app_id(&self) -> &AppId {
        &self.app_id
    }

    #[must_use]
    pub fn get(
        &self,
        domain: &str,
        scope: &cowd_app_sdk::AppStorageScope,
    ) -> Option<&AppStorageLease> {
        self.leases.get(&(domain.to_string(), scope.clone()))
    }

    #[must_use]
    pub fn provisions(&self) -> Vec<AppStorageProvision> {
        self.leases
            .values()
            .map(|lease| lease.provision.clone())
            .collect()
    }

    fn endpoints(&self) -> Vec<storage::StorageEndpoint> {
        self.leases
            .values()
            .map(|lease| lease.endpoint.clone())
            .collect()
    }
}

/// Complete, app-scoped product registration context.  New APP code has no
/// config-home accessor and therefore cannot derive an undeclared DB path.
#[derive(Clone)]
pub struct AppProductContext {
    host: CowdAppContext,
    storage: AppStorageLeases,
}

impl AppProductContext {
    #[must_use]
    pub fn new(host: CowdAppContext, storage: AppStorageLeases) -> Self {
        Self { host, storage }
    }

    #[must_use]
    pub fn host(&self) -> &CowdAppContext {
        &self.host
    }

    #[must_use]
    pub fn storage(&self) -> &AppStorageLeases {
        &self.storage
    }
}

/// Startup-only registry. It is intentionally immutable after construction:
/// source changes require a product build/release, never a runtime code load.
#[derive(Default)]
pub struct AppRegistry {
    apps: BTreeMap<AppId, RegisteredApp>,
    http: Vec<HttpAppContribution>,
    http_contracts: BTreeMap<AppId, RegisteredAppHttpContract>,
    tui: BTreeMap<AppId, TuiAppContribution>,
    skills: BTreeMap<AppId, Vec<AppSkillDescriptor>>,
    quality_checks: BTreeMap<AppId, Vec<AppQualityCheck>>,
    storage_leases: BTreeMap<AppId, AppStorageLeases>,
}

impl AppRegistry {
    pub fn register(&mut self, contribution: AppContribution) -> Result<(), AppRegistryError> {
        let descriptor = contribution.app.descriptor();
        descriptor.validate()?;
        let app_id = descriptor.id.clone();
        if self.apps.contains_key(&app_id) {
            return Err(AppRegistryError::DuplicateApp(app_id));
        }

        let existing_routes: BTreeSet<(String, String)> = self
            .apps
            .values()
            .flat_map(|app| app.descriptor.routes.iter())
            .map(|route| (route.method.clone(), route.path.clone()))
            .collect();
        for route in &descriptor.routes {
            if existing_routes.contains(&(route.method.clone(), route.path.clone())) {
                return Err(AppRegistryError::RouteCollision {
                    app_id,
                    method: route.method.clone(),
                    path: route.path.clone(),
                });
            }
        }

        let existing_capabilities: BTreeSet<String> = self
            .apps
            .values()
            .flat_map(|app| app.descriptor.capabilities.iter().cloned())
            .collect();
        if let Some(capability) = descriptor
            .capabilities
            .iter()
            .find(|capability| existing_capabilities.contains(*capability))
        {
            return Err(AppRegistryError::CapabilityCollision {
                app_id,
                capability: capability.clone(),
            });
        }

        let health = contribution.app.health();
        let http_registered = contribution.http.is_some();
        let tui_registered = contribution.tui.is_some();
        if let Some(http) = contribution.http {
            self.http.push(http);
        }
        if let Some(tui) = contribution.tui {
            self.tui.insert(app_id.clone(), tui);
        }
        self.apps.insert(
            app_id,
            RegisteredApp {
                descriptor,
                health,
                http_registered,
                tui_registered,
                source_lock: None,
                storage: None,
            },
        );
        Ok(())
    }

    pub fn attach_product_contract(
        &mut self,
        app_id: &AppId,
        source_lock: Option<AppSourceLock>,
        contract: AppStorageContract,
        leases: AppStorageLeases,
    ) -> Result<(), AppRegistryError> {
        let app = self
            .apps
            .get_mut(app_id)
            .ok_or_else(|| AppRegistryError::UnknownApp(app_id.clone()))?;
        if app.storage.is_some() || self.storage_leases.contains_key(app_id) {
            return Err(AppRegistryError::DuplicateStorageContract(app_id.clone()));
        }
        contract.validate_for(app_id)?;
        let provisions = leases.provisions();
        if leases.app_id() != app_id || provisions.len() != contract.requirements.len() {
            return Err(AppRegistryError::IncompleteStorageProvision(app_id.clone()));
        }
        for requirement in &contract.requirements {
            let Some(provision) = provisions.iter().find(|provision| {
                provision.domain == requirement.domain && provision.scope == requirement.scope
            }) else {
                return Err(AppRegistryError::IncompleteStorageProvision(app_id.clone()));
            };
            let backend_matches = match requirement.backend {
                cowd_app_sdk::AppStorageBackend::Relational => matches!(
                    provision.backend,
                    cowd_app_sdk::AppStorageBackend::Sqlite
                        | cowd_app_sdk::AppStorageBackend::Postgres
                ),
                _ => provision.backend == requirement.backend,
            };
            let capabilities_match = requirement
                .required_capabilities()
                .iter()
                .all(|required| provision.capabilities.contains(required));
            if !backend_matches
                || !capabilities_match
                || provision.logical_id.trim().is_empty()
                || provision.namespace.trim().is_empty()
                || provision.migration != requirement.migration
                || provision.migration_owner != *app_id
                || provision.readiness != cowd_app_sdk::AppStorageReadiness::Ready
            {
                return Err(AppRegistryError::IncompleteStorageProvision(app_id.clone()));
            }
        }
        if let Some(source_lock) = &source_lock {
            source_lock.validate(app_id)?;
        }
        app.source_lock = source_lock;
        app.storage = Some(AppStorageRegistration {
            contract,
            provisions,
        });
        self.storage_leases.insert(app_id.clone(), leases);
        Ok(())
    }

    #[must_use]
    pub fn storage_endpoints(&self) -> Vec<storage::StorageEndpoint> {
        self.storage_leases
            .values()
            .flat_map(AppStorageLeases::endpoints)
            .collect()
    }

    /// Host-side lookup for an already provisioned APP lease set. This never
    /// crosses the APP ABI; it lets composition tests and host diagnostics use
    /// the exact attached executor instead of reconstructing a physical path.
    #[must_use]
    pub fn storage_leases(&self, app_id: &AppId) -> Option<&AppStorageLeases> {
        self.storage_leases.get(app_id)
    }

    #[must_use]
    pub fn apps(&self) -> Vec<RegisteredApp> {
        self.apps.values().cloned().collect()
    }

    #[must_use]
    pub fn app(&self, id: &AppId) -> Option<&RegisteredApp> {
        self.apps.get(id)
    }

    #[must_use]
    pub fn tui(&self) -> Vec<(AppId, TuiAppContribution)> {
        self.tui
            .iter()
            .map(|(id, item)| (id.clone(), item.clone()))
            .collect()
    }

    /// Attach the full HTTP projection after the App router itself has been
    /// accepted. This mirrors `register_app_skills`: product composition is
    /// the only writer, while Gateway reads a complete, immutable projection.
    pub fn register_app_http_contract(
        &mut self,
        app_id: &AppId,
        contract: AppHttpContract,
        error_mapper: AppErrorEnvelopeMapper,
    ) -> Result<(), AppRegistryError> {
        let app = self
            .apps
            .get(app_id)
            .ok_or_else(|| AppRegistryError::UnknownApp(app_id.clone()))?;
        if !app.http_registered {
            return Err(AppRegistryError::AppHasNoHttpContribution(app_id.clone()));
        }
        contract.validate_against(&app.descriptor)?;
        if self.http_contracts.contains_key(app_id) {
            return Err(AppRegistryError::DuplicateHttpContract(app_id.clone()));
        }
        let existing_components = self
            .http_contracts
            .values()
            .flat_map(|registered| registered.contract.openapi_components.keys())
            .collect::<BTreeSet<_>>();
        if let Some(component) = contract
            .openapi_components
            .keys()
            .find(|component| existing_components.contains(component))
        {
            return Err(AppRegistryError::OpenApiComponentCollision {
                app_id: app_id.clone(),
                component: component.clone(),
            });
        }
        self.http_contracts.insert(
            app_id.clone(),
            RegisteredAppHttpContract {
                contract,
                error_mapper,
            },
        );
        Ok(())
    }

    #[must_use]
    pub fn route_metadata(&self) -> Vec<RegisteredAppRouteMetadata> {
        self.http_contracts
            .iter()
            .flat_map(|(app_id, registered)| {
                registered
                    .contract
                    .routes
                    .iter()
                    .cloned()
                    .map(|route| RegisteredAppRouteMetadata {
                        app_id: app_id.clone(),
                        route,
                        auth_error_schema: registered.contract.auth_error_schema.clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[must_use]
    pub fn openapi_components(&self) -> BTreeMap<String, Value> {
        self.http_contracts
            .values()
            .flat_map(|registered| registered.contract.openapi_components.clone())
            .collect()
    }

    #[must_use]
    pub fn error_envelope_for_path(
        &self,
        path: &str,
        status: u16,
        message: String,
    ) -> Option<Value> {
        self.http_contracts.values().find_map(|registered| {
            registered
                .contract
                .routes
                .iter()
                .any(|route| route.active && route.path == path)
                .then(|| (registered.error_mapper)(status, message.clone()))
        })
    }

    pub fn register_app_quality_checks(
        &mut self,
        app_id: &AppId,
        checks: Vec<AppQualityCheck>,
    ) -> Result<(), AppRegistryError> {
        if !self.apps.contains_key(app_id) {
            return Err(AppRegistryError::UnknownApp(app_id.clone()));
        }
        if self.quality_checks.contains_key(app_id) {
            return Err(AppRegistryError::DuplicateQualityChecks(app_id.clone()));
        }
        let mut ids = BTreeSet::new();
        if checks
            .iter()
            .any(|check| check.id.trim().is_empty() || !ids.insert(check.id.as_str()))
        {
            return Err(AppRegistryError::InvalidQualityCheck(app_id.clone()));
        }
        self.quality_checks.insert(app_id.clone(), checks);
        Ok(())
    }

    #[must_use]
    pub fn verify_quality_checks(&self) -> Vec<AppQualityCheckResult> {
        self.quality_checks
            .iter()
            .flat_map(|(app_id, checks)| {
                checks
                    .iter()
                    .map(|check| AppQualityCheckResult {
                        app_id: app_id.clone(),
                        id: check.id.clone(),
                        passed: (check.verify)(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Attach APP-owned skill descriptors after the APP's transport
    /// contribution has been validated. Product composition is the sole
    /// caller: Gateway, CLI and TUI only read the flattened generic view.
    pub fn register_app_skills(
        &mut self,
        app_id: &AppId,
        skills: Vec<AppSkillDescriptor>,
    ) -> Result<(), AppRegistryError> {
        if !self.apps.contains_key(app_id) {
            return Err(AppRegistryError::UnknownApp(app_id.clone()));
        }
        let existing_ids = self
            .skills
            .values()
            .flat_map(|items| items.iter().map(|skill| skill.id.as_str()))
            .collect::<BTreeSet<_>>();
        let mut local_ids = BTreeSet::new();
        for skill in &skills {
            skill.validate(app_id)?;
            if !local_ids.insert(skill.id.as_str()) || existing_ids.contains(skill.id.as_str()) {
                return Err(AppRegistryError::DuplicateSkill {
                    app_id: app_id.clone(),
                    skill_id: skill.id.clone(),
                });
            }
        }
        self.skills.insert(app_id.clone(), skills);
        Ok(())
    }

    /// Deterministic, app-agnostic application skill catalogue.
    #[must_use]
    pub fn skills(&self) -> Vec<AppSkillDescriptor> {
        self.skills
            .values()
            .flat_map(|items| items.iter().cloned())
            .collect()
    }

    #[must_use]
    pub fn into_http_router(self) -> Router {
        self.http
            .into_iter()
            .fold(Router::new(), |router, app| router.merge(app.router))
    }

    /// Build the product's application router from already-registered
    /// contributions. This is read-only and may be called by Gateway while it
    /// assembles its outer authentication/capacity middleware.
    #[must_use]
    pub fn http_router(&self) -> Router {
        self.http.iter().fold(Router::new(), |router, app| {
            router.merge(app.router.clone())
        })
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AppRegistryError {
    #[error(transparent)]
    Contract(#[from] AppContractError),
    #[error("duplicate app id: {0}")]
    DuplicateApp(AppId),
    #[error("route collision for app {app_id}: {method} {path}")]
    RouteCollision {
        app_id: AppId,
        method: String,
        path: String,
    },
    #[error("capability collision for app {app_id}: {capability}")]
    CapabilityCollision { app_id: AppId, capability: String },
    #[error("application {0} has no HTTP contribution")]
    AppHasNoHttpContribution(AppId),
    #[error("application {0} registered more than one HTTP contract")]
    DuplicateHttpContract(AppId),
    #[error("OpenAPI component collision for app {app_id}: {component}")]
    OpenApiComponentCollision { app_id: AppId, component: String },
    #[error("application {0} registered more than one quality-check set")]
    DuplicateQualityChecks(AppId),
    #[error("application {0} contains an invalid quality check")]
    InvalidQualityCheck(AppId),
    #[error("application not registered: {0}")]
    UnknownApp(AppId),
    #[error("duplicate skill for app {app_id}: {skill_id}")]
    DuplicateSkill { app_id: AppId, skill_id: String },
    #[error("invalid TUI contribution for app {app_id}: {reason}")]
    InvalidTuiContribution { app_id: AppId, reason: String },
    #[error("duplicate storage contract for application {0}")]
    DuplicateStorageContract(AppId),
    #[error("duplicate storage lease for application {0}")]
    DuplicateStorageLease(AppId),
    #[error("incomplete storage provision for application {0}")]
    IncompleteStorageProvision(AppId),
    #[error("invalid artifact path for storage endpoint {0}")]
    InvalidArtifactPath(String),
    #[error("application artifact I/O failed: {0}")]
    StorageIo(String),
    #[error("application storage migration failed: {0}")]
    StorageMigration(String),
    #[error("application storage migration evidence is invalid for {0}")]
    InvalidStorageMigrationEvidence(String),
}

impl From<std::io::Error> for AppRegistryError {
    fn from(error: std::io::Error) -> Self {
        Self::StorageIo(error.to_string())
    }
}

/// A reviewed, compile-time linked application product contribution.
///
/// This is deliberately a small static factory contract rather than a dynamic
/// plug-in ABI.  An APP repository owns its domain adapters and returns one
/// value; the product composer can then aggregate any number of such values
/// without importing an APP's DTOs, router functions, or TUI implementation.
#[derive(Clone, Copy)]
pub struct StaticAppProduct {
    descriptor: fn() -> AppDescriptor,
    register: fn(&mut AppRegistry, AppProductContext) -> Result<(), AppRegistryError>,
    tui_surface: Option<fn() -> TuiAppSurfaceContribution>,
    storage_requirements: fn() -> Vec<AppStorageRequirement>,
    storage_migrator: Option<
        fn(
            &AppStorageLeases,
            &AppStorageLeases,
        ) -> Result<AppStorageMigrationEvidence, AppRegistryError>,
    >,
    source_lock: Option<StaticAppSourceLock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticAppSourceLock {
    pub git: &'static str,
    pub revision: &'static str,
}

impl StaticAppSourceLock {
    #[must_use]
    pub const fn new(git: &'static str, revision: &'static str) -> Self {
        Self { git, revision }
    }

    fn owned(self) -> AppSourceLock {
        AppSourceLock {
            git: self.git.to_string(),
            revision: self.revision.to_string(),
        }
    }
}

impl StaticAppProduct {
    /// Constructor for applications that consume only host-provisioned
    /// storage leases.  This is the required entry point for new APPs.
    #[must_use]
    pub const fn new_provisioned(
        descriptor: fn() -> AppDescriptor,
        register: fn(&mut AppRegistry, AppProductContext) -> Result<(), AppRegistryError>,
        tui_surface: Option<fn() -> TuiAppSurfaceContribution>,
        storage_requirements: fn() -> Vec<AppStorageRequirement>,
    ) -> Self {
        Self {
            descriptor,
            register,
            tui_surface,
            storage_requirements,
            storage_migrator: None,
            source_lock: None,
        }
    }

    #[must_use]
    pub const fn with_storage_migrator(
        mut self,
        migrator: fn(
            &AppStorageLeases,
            &AppStorageLeases,
        ) -> Result<AppStorageMigrationEvidence, AppRegistryError>,
    ) -> Self {
        self.storage_migrator = Some(migrator);
        self
    }

    #[must_use]
    pub const fn with_source_lock(mut self, source_lock: StaticAppSourceLock) -> Self {
        self.source_lock = Some(source_lock);
        self
    }

    #[must_use]
    pub fn descriptor(self) -> AppDescriptor {
        (self.descriptor)()
    }

    #[must_use]
    pub fn app_id(self) -> AppId {
        self.descriptor().id
    }

    pub fn register(
        self,
        registry: &mut AppRegistry,
        context: AppProductContext,
    ) -> Result<(), AppRegistryError> {
        (self.register)(registry, context)
    }

    #[must_use]
    pub fn tui_surface(self) -> Option<TuiAppSurfaceContribution> {
        self.tui_surface.map(|factory| factory())
    }

    #[must_use]
    pub fn storage_requirements(self) -> Vec<AppStorageRequirement> {
        (self.storage_requirements)()
    }

    #[must_use]
    pub fn storage_contract(self) -> AppStorageContract {
        AppStorageContract::new(self.app_id(), self.storage_requirements())
    }

    pub fn migrate_storage(
        self,
        source: &AppStorageLeases,
        target: &AppStorageLeases,
    ) -> Result<AppStorageMigrationEvidence, AppRegistryError> {
        let app_id = self.app_id();
        let migrator = self.storage_migrator.ok_or_else(|| {
            AppRegistryError::StorageMigration(format!(
                "application {app_id} has durable storage but no migration hook"
            ))
        })?;
        let evidence = migrator(source, target)?;
        evidence.validate_for(&app_id)?;
        Ok(evidence)
    }

    #[must_use]
    pub const fn has_storage_migrator(self) -> bool {
        self.storage_migrator.is_some()
    }

    #[must_use]
    pub fn source_lock(self) -> Option<AppSourceLock> {
        self.source_lock.map(StaticAppSourceLock::owned)
    }
}

#[cfg(test)]
mod tests {
    use axum::{routing::get, Router};
    use cowd_app_sdk::{
        AppActionDescriptor, AppProfileDescriptor, AppProfileVariant, SDK_API_VERSION,
    };
    use serde_json::json;

    use super::*;

    fn static_descriptor() -> AppDescriptor {
        FixtureApp {
            id: "fixture",
            path: "/api/apps/fixture",
            capability: "fixture.read",
        }
        .descriptor()
    }

    fn static_register(
        _registry: &mut AppRegistry,
        _context: AppProductContext,
    ) -> Result<(), AppRegistryError> {
        Ok(())
    }

    fn static_requirements() -> Vec<AppStorageRequirement> {
        Vec::new()
    }

    fn static_migrate(
        source: &AppStorageLeases,
        target: &AppStorageLeases,
    ) -> Result<AppStorageMigrationEvidence, AppRegistryError> {
        assert_eq!(source.app_id(), target.app_id());
        Ok(AppStorageMigrationEvidence {
            app_id: source.app_id().clone(),
            contract_version: 1,
            domains: vec![AppStorageDomainMigrationEvidence {
                domain: "primary".to_string(),
                source_digest: "same-digest".to_string(),
                target_digest: "same-digest".to_string(),
                record_counts: BTreeMap::new(),
            }],
        })
    }

    struct FixtureApp {
        id: &'static str,
        path: &'static str,
        capability: &'static str,
    }

    impl CapabilityApp for FixtureApp {
        fn descriptor(&self) -> AppDescriptor {
            AppDescriptor {
                id: AppId::parse(self.id).expect("fixture id"),
                display_name: self.id.to_string(),
                sdk_api: SDK_API_VERSION,
                version: "0.1.0".to_string(),
                capabilities: vec![self.capability.to_string()],
                routes: vec![cowd_app_sdk::AppRouteDescriptor {
                    method: "GET".to_string(),
                    path: self.path.to_string(),
                    operation_id: format!("{}.ping", self.id),
                    streaming: false,
                }],
                actions: vec![AppActionDescriptor {
                    id: "read".to_string(),
                    title: "Read".to_string(),
                    requires_confirmation: false,
                }],
                profile: Some(AppProfileDescriptor {
                    catalog_revision: 1,
                    capability_digest: "fixture".to_string(),
                    default_profile_id: "operator".to_string(),
                    profiles: vec![AppProfileVariant {
                        id: "operator".to_string(),
                        capabilities: vec!["fixture.read".to_string()],
                    }],
                    surface_capabilities: std::collections::BTreeMap::from([(
                        "tui".to_string(),
                        vec!["fixture.read".to_string()],
                    )]),
                }),
            }
        }
        fn health(&self) -> AppHealth {
            AppHealth::Ready
        }
    }

    fn fixture(id: &'static str, path: &'static str, capability: &'static str) -> AppContribution {
        AppContribution {
            app: Box::new(FixtureApp {
                id,
                path,
                capability,
            }),
            http: Some(HttpAppContribution {
                router: Router::new().route(path, get(|| async { "ok" })),
            }),
            tui: Some(TuiAppContribution {
                panel_id: format!("{id}.panel"),
                title: id.to_string(),
                actions: vec!["read".to_string()],
            }),
        }
    }

    fn fixture_error(status: u16, message: String) -> Value {
        json!({"status": status, "app_error": message})
    }

    fn fixture_quality() -> bool {
        true
    }

    #[test]
    fn registry_has_a_real_generic_http_and_tui_consumer() {
        let mut registry = AppRegistry::default();
        registry
            .register(fixture("fixture", "/api/apps/fixture/ping", "fixture.read"))
            .expect("register fixture");
        assert_eq!(registry.apps().len(), 1);
        assert_eq!(registry.tui().len(), 1);
        let _router = registry.into_http_router();
    }

    #[test]
    fn registry_accepts_only_complete_ready_path_free_storage_contracts() {
        let mut registry = AppRegistry::default();
        registry
            .register(fixture("fixture", "/api/apps/fixture/ping", "fixture.read"))
            .expect("register fixture");
        let app_id = AppId::parse("fixture").expect("fixture id");
        let scope = cowd_app_sdk::AppStorageScope::App;
        let endpoint = storage::StorageEndpoint::sqlite(
            storage::StorageDomainId::app("fixture", "primary"),
            storage::StorageScope::App {
                app_id: "fixture".to_string(),
            },
            std::env::temp_dir().join(format!(
                "cowd-app-host-fixture-{}.sqlite",
                std::process::id()
            )),
            "fixture",
            "fixture_primary_v1",
        );
        let executor = storage::SqliteExecutor::for_endpoint(&endpoint).expect("SQLite lease");
        let provision = cowd_app_sdk::AppStorageProvision {
            domain: "primary".to_string(),
            scope: scope.clone(),
            backend: cowd_app_sdk::AppStorageBackend::Sqlite,
            logical_id: endpoint.logical_id(),
            namespace: endpoint.logical_id(),
            migration: "fixture_primary_v1".to_string(),
            migration_owner: app_id.clone(),
            capabilities: cowd_app_sdk::AppStorageRequirement {
                domain: "primary".to_string(),
                backend: cowd_app_sdk::AppStorageBackend::Sqlite,
                scope: scope.clone(),
                migration: "fixture_primary_v1".to_string(),
            }
            .required_capabilities(),
            readiness: cowd_app_sdk::AppStorageReadiness::Ready,
        };
        let leases = AppStorageLeases::new(
            app_id.clone(),
            vec![AppStorageLease::sqlite(endpoint, provision, executor)],
        )
        .expect("indexed lease");
        let requirement = cowd_app_sdk::AppStorageRequirement {
            domain: "primary".to_string(),
            backend: cowd_app_sdk::AppStorageBackend::Sqlite,
            scope,
            migration: "fixture_primary_v1".to_string(),
        };
        registry
            .attach_product_contract(
                &app_id,
                Some(cowd_app_sdk::AppSourceLock {
                    git: "https://example.invalid/fixture".to_string(),
                    revision: "0123456789abcdef0123456789abcdef01234567".to_string(),
                }),
                cowd_app_sdk::AppStorageContract::new(app_id.clone(), vec![requirement]),
                leases,
            )
            .expect("complete contract");
        let projection = serde_json::to_string(&registry.apps()).expect("registry JSON");
        assert!(!projection.contains(".sqlite"));
        assert!(!projection.contains(std::env::temp_dir().to_string_lossy().as_ref()));
    }

    #[test]
    fn registry_rejects_route_and_capability_collisions_before_startup() {
        let mut registry = AppRegistry::default();
        registry
            .register(fixture("fixture", "/api/apps/fixture/ping", "fixture.read"))
            .expect("first app");
        assert!(matches!(
            registry.register(fixture("second", "/api/apps/fixture/ping", "second.read")),
            Err(AppRegistryError::RouteCollision { .. })
        ));
        assert!(matches!(
            registry.register(fixture("third", "/api/apps/third/ping", "fixture.read")),
            Err(AppRegistryError::CapabilityCollision { .. })
        ));
    }

    #[test]
    fn registry_projects_app_http_contract_errors_and_quality_without_domain_types() {
        let mut registry = AppRegistry::default();
        registry
            .register(fixture("fixture", "/api/apps/fixture/ping", "fixture.read"))
            .expect("fixture contribution");
        let app_id = AppId::parse("fixture").expect("fixture id");
        registry
            .register_app_http_contract(
                &app_id,
                AppHttpContract {
                    routes: vec![AppRouteMetadata {
                        method: "GET".to_string(),
                        path: "/api/apps/fixture/ping".to_string(),
                        route_id: "fixture.ping".to_string(),
                        operation_id: "fixture.ping".to_string(),
                        request_schema: "FixtureNoBody".to_string(),
                        response_schema: "FixtureResponse".to_string(),
                        class: "read".to_string(),
                        capability: "fixture.read".to_string(),
                        risk: "low".to_string(),
                        confirmation: "none".to_string(),
                        streaming: false,
                        active: true,
                    }],
                    openapi_components: BTreeMap::from([(
                        "FixtureResponse".to_string(),
                        json!({"type": "object"}),
                    )]),
                    auth_error_schema: None,
                },
                fixture_error,
            )
            .expect("HTTP contract");
        registry
            .register_app_quality_checks(
                &app_id,
                vec![AppQualityCheck {
                    id: "fixture.quality".to_string(),
                    verify: fixture_quality,
                }],
            )
            .expect("quality check");

        assert_eq!(registry.route_metadata().len(), 1);
        assert!(registry
            .openapi_components()
            .contains_key("FixtureResponse"));
        assert_eq!(
            registry
                .error_envelope_for_path("/api/apps/fixture/ping", 401, "unauthorized".to_string(),)
                .expect("typed error")["app_error"],
            "unauthorized"
        );
        assert_eq!(
            registry.verify_quality_checks(),
            vec![AppQualityCheckResult {
                app_id,
                id: "fixture.quality".to_string(),
                passed: true,
            }]
        );
    }

    struct FixturePanel;

    impl TuiAppPanel for FixturePanel {
        fn panel_id(&self) -> &str {
            "fixture.panel"
        }

        fn render(&mut self, frame: &mut Frame<'_>, area: Rect, context: TuiAppRenderContext) {
            use ratatui::widgets::Paragraph;

            frame.render_widget(
                Paragraph::new(if context.focused { "focused" } else { "idle" })
                    .style(context.theme.accent_style()),
                area,
            );
        }

        fn handle_key(&mut self, key: KeyEvent, effects: &mut TuiAppEffects) -> bool {
            if key.code == crossterm::event::KeyCode::Char('r') {
                effects.push(TuiAppEffect::Request {
                    request_id: "fixture.read".to_string(),
                    method: "GET".to_string(),
                    path: "/api/apps/fixture/ping".to_string(),
                    body: None,
                    headers: BTreeMap::new(),
                });
                return true;
            }
            false
        }

        fn apply_event(&mut self, event: TuiAppEvent, effects: &mut TuiAppEffects) {
            if let TuiAppEvent::RequestFailed { error, .. } = event {
                effects.push(TuiAppEffect::Notice {
                    level: TuiAppNoticeLevel::Error,
                    title: Some("Fixture".to_string()),
                    message: error,
                });
            }
        }

        fn actions(&self) -> Vec<TuiAppAction> {
            vec![TuiAppAction {
                id: "fixture.read".to_string(),
                label: "Read fixture".to_string(),
                description: "Issue a generic app request".to_string(),
                domain: "fixture".to_string(),
                risk: "low".to_string(),
                requires_confirmation: false,
                enabled: true,
                unavailable_reason: None,
            }]
        }

        fn dispatch_action(&mut self, action_id: &str, effects: &mut TuiAppEffects) -> bool {
            if action_id == "fixture.read" {
                effects.push(TuiAppEffect::Request {
                    request_id: action_id.to_string(),
                    method: "GET".to_string(),
                    path: "/api/apps/fixture/ping".to_string(),
                    body: None,
                    headers: BTreeMap::new(),
                });
                return true;
            }
            false
        }

        fn handle_command(&mut self, command: &str, effects: &mut TuiAppEffects) -> bool {
            if command == "/fixture read" {
                return self.dispatch_action("fixture.read", effects);
            }
            false
        }
    }

    #[test]
    fn external_tui_panel_factory_exercises_the_complete_generic_host_seam() {
        let contribution = TuiAppSurfaceContribution::new(
            AppId::parse("fixture").expect("fixture app id"),
            TuiAppContribution {
                panel_id: "fixture.panel".to_string(),
                title: "Fixture".to_string(),
                actions: vec!["fixture.read".to_string()],
            },
            Arc::new(|| Box::new(FixturePanel) as Box<dyn TuiAppPanel>),
        );
        contribution.validate().expect("valid contribution");

        let mut panel = contribution.create_panel();
        assert_eq!(panel.panel_id(), "fixture.panel");
        assert_eq!(panel.actions().len(), 1);
        let mut effects = TuiAppEffects::default();
        panel.on_mount(&mut effects);
        assert!(effects.is_empty());
        assert!(panel.dispatch_action("fixture.read", &mut effects));
        assert!(matches!(
            effects.take().as_slice(),
            [TuiAppEffect::Request { .. }]
        ));
        assert!(panel.handle_command("/fixture read", &mut effects));
        assert!(matches!(
            effects.take().as_slice(),
            [TuiAppEffect::Request { .. }]
        ));
        panel.apply_event(
            TuiAppEvent::RequestFailed {
                request_id: "fixture.read".to_string(),
                status: Some(503),
                body: None,
                error: "offline".to_string(),
            },
            &mut effects,
        );
        assert!(matches!(
            effects.take().as_slice(),
            [TuiAppEffect::Notice { .. }]
        ));

        use ratatui::{backend::TestBackend, Terminal};
        let backend = TestBackend::new(24, 4);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| {
                let area = frame.area();
                panel.render(
                    frame,
                    area,
                    TuiAppRenderContext {
                        theme: TuiAppTheme::dark(),
                        focused: true,
                    },
                );
            })
            .expect("render external panel");
    }

    #[test]
    fn app_storage_migration_hook_is_explicit_and_evidence_is_validated() {
        let app_id = AppId::parse("fixture").unwrap();
        let source = AppStorageLeases::new(app_id.clone(), Vec::new()).unwrap();
        let target = AppStorageLeases::new(app_id, Vec::new()).unwrap();
        let product = StaticAppProduct::new_provisioned(
            static_descriptor,
            static_register,
            None,
            static_requirements,
        );
        assert!(!product.has_storage_migrator());
        assert!(matches!(
            product.migrate_storage(&source, &target),
            Err(AppRegistryError::StorageMigration(_))
        ));

        let product = product.with_storage_migrator(static_migrate);
        let evidence = product.migrate_storage(&source, &target).unwrap();
        assert_eq!(evidence.app_id.as_str(), "fixture");
        assert_eq!(evidence.domains[0].source_digest, "same-digest");
    }
}
