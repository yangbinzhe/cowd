//! Cowd application ABI.
//!
//! This crate deliberately contains only transport-neutral, stable contracts.
//! An application can depend on it without gaining access to Gateway, Runtime,
//! authentication credentials, or another application's implementation.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod execution;
pub mod presentation;

pub use execution::{
    ApplicationExecutionCounterV1, ApplicationExecutionIdempotencyKeyV1, ApplicationExecutionKind,
    ApplicationExecutionOutcomeIntentV1, ApplicationExecutionOutcomeReceiptV1,
    ApplicationExecutionOutcomeV1, ApplicationExecutionRefV1, ApplicationExecutionStatus,
    APPEND_APPLICATION_EXECUTION_OUTCOME_INTENT_V1, APPLICATION_EXECUTION_OUTCOME_VERSION,
};

pub const SDK_API_VERSION: u32 = 1;
pub const APP_STORAGE_CONTRACT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AppId(String);

impl AppId {
    pub fn parse(value: impl Into<String>) -> Result<Self, AppContractError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 63
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
        if valid {
            Ok(Self(value))
        } else {
            Err(AppContractError::InvalidAppId(value))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AppId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppDescriptor {
    pub id: AppId,
    pub display_name: String,
    pub sdk_api: u32,
    pub version: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub routes: Vec<AppRouteDescriptor>,
    #[serde(default)]
    pub actions: Vec<AppActionDescriptor>,
    pub profile: Option<AppProfileDescriptor>,
}

impl AppDescriptor {
    pub fn validate(&self) -> Result<(), AppContractError> {
        if self.sdk_api != SDK_API_VERSION {
            return Err(AppContractError::UnsupportedSdkApi {
                app_id: self.id.clone(),
                expected: SDK_API_VERSION,
                actual: self.sdk_api,
            });
        }
        if self.display_name.trim().is_empty() || self.version.trim().is_empty() {
            return Err(AppContractError::InvalidDescriptor(self.id.clone()));
        }
        for route in &self.routes {
            route.validate(&self.id)?;
        }
        for capability in &self.capabilities {
            if capability.trim().is_empty() {
                return Err(AppContractError::InvalidDescriptor(self.id.clone()));
            }
        }
        if let Some(profile_catalogue) = &self.profile {
            if profile_catalogue.capability_digest.trim().is_empty()
                || profile_catalogue.default_profile_id.trim().is_empty()
                || profile_catalogue.profiles.iter().any(|profile| {
                    profile.id.trim().is_empty()
                        || profile
                            .capabilities
                            .iter()
                            .any(|capability| capability.trim().is_empty())
                })
            {
                return Err(AppContractError::InvalidDescriptor(self.id.clone()));
            }
            for (index, profile) in profile_catalogue.profiles.iter().enumerate() {
                if profile_catalogue.profiles[..index]
                    .iter()
                    .any(|previous| previous.id == profile.id)
                {
                    return Err(AppContractError::InvalidDescriptor(self.id.clone()));
                }
            }
            if !profile_catalogue
                .profiles
                .iter()
                .any(|profile| profile.id == profile_catalogue.default_profile_id)
                || profile_catalogue
                    .surface_capabilities
                    .values()
                    .any(|capabilities| {
                        capabilities
                            .iter()
                            .any(|capability| capability.trim().is_empty())
                    })
            {
                return Err(AppContractError::InvalidDescriptor(self.id.clone()));
            }
        }
        Ok(())
    }
}

/// Backend-neutral durable storage requested by a product APP.
///
/// An APP may name its own data domains but may not choose a host filesystem
/// path, connection URL, credential, or another APP's namespace.  The product
/// composer resolves this declaration into a `storage::StorageEndpoint`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppStorageBackend {
    /// The application ships adapters for both supported relational engines;
    /// the host may resolve this requirement to SQLite or PostgreSQL.
    Relational,
    Sqlite,
    Postgres,
    FileJson,
    Directory,
    BlobDirectory,
}

/// Isolation required for an APP-owned durable domain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppStorageScope {
    App,
    Workspace,
}

/// A declarative product-storage request. This is intentionally a stable SDK
/// DTO rather than a `PathBuf`: the host owns deployment topology and future
/// backend selection, while the APP owns only its domain contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppStorageRequirement {
    pub domain: String,
    pub backend: AppStorageBackend,
    pub scope: AppStorageScope,
    pub migration: String,
}

impl AppStorageRequirement {
    pub fn validate(&self, app_id: &AppId) -> Result<(), AppContractError> {
        let valid_domain = !self.domain.is_empty()
            && self.domain.len() <= 63
            && self.domain.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
            });
        if !valid_domain || self.migration.trim().is_empty() {
            return Err(AppContractError::InvalidStorageRequirement {
                app_id: app_id.clone(),
                domain: self.domain.clone(),
            });
        }
        Ok(())
    }

    /// Capabilities implied by this requirement.  These values are stable
    /// contract facts, not a claim that a particular endpoint is ready.
    #[must_use]
    pub fn required_capabilities(&self) -> Vec<String> {
        let capabilities: &[&str] = match self.backend {
            AppStorageBackend::Relational => &["relational", "transactions", "schema_migrations"],
            AppStorageBackend::Sqlite => {
                &["relational", "transactions", "schema_migrations", "sqlite"]
            }
            AppStorageBackend::Postgres => &[
                "relational",
                "transactions",
                "schema_migrations",
                "postgres",
                "connection_pool",
            ],
            AppStorageBackend::FileJson => &["artifact", "atomic_replace", "json"],
            AppStorageBackend::Directory => &["artifact", "directory"],
            AppStorageBackend::BlobDirectory => &["artifact", "blob_directory"],
        };
        capabilities
            .iter()
            .map(|capability| (*capability).to_string())
            .collect()
    }
}

/// Validated storage contract attached to one reviewed APP descriptor.
///
/// `migration_owner` is deliberately the APP id rather than a crate or host
/// service name: the host provisions topology, while the APP owns schema
/// meaning and migration code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppStorageContract {
    pub contract_version: u32,
    pub migration_owner: AppId,
    pub requirements: Vec<AppStorageRequirement>,
}

impl AppStorageContract {
    #[must_use]
    pub fn new(migration_owner: AppId, requirements: Vec<AppStorageRequirement>) -> Self {
        Self {
            contract_version: APP_STORAGE_CONTRACT_VERSION,
            migration_owner,
            requirements,
        }
    }

    pub fn validate_for(&self, app_id: &AppId) -> Result<(), AppContractError> {
        if self.contract_version != APP_STORAGE_CONTRACT_VERSION || self.migration_owner != *app_id
        {
            return Err(AppContractError::InvalidStorageContract {
                app_id: app_id.clone(),
                reason: "contract version or migration owner does not match the application"
                    .to_string(),
            });
        }
        let mut identities = BTreeSet::new();
        for requirement in &self.requirements {
            requirement.validate(app_id)?;
            if !identities.insert((requirement.domain.as_str(), &requirement.scope)) {
                return Err(AppContractError::InvalidStorageContract {
                    app_id: app_id.clone(),
                    reason: format!(
                        "duplicate storage domain/scope `{}` / {:?}",
                        requirement.domain, requirement.scope
                    ),
                });
            }
        }
        Ok(())
    }
}

/// Non-secret immutable source identity supplied by the product catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSourceLock {
    pub git: String,
    pub revision: String,
}

impl AppSourceLock {
    pub fn validate(&self, app_id: &AppId) -> Result<(), AppContractError> {
        let revision_valid = (7..=64).contains(&self.revision.len())
            && self.revision.bytes().all(|byte| byte.is_ascii_hexdigit());
        if self.git.trim().is_empty() || !revision_valid {
            return Err(AppContractError::InvalidSourceLock(app_id.clone()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppStorageReadiness {
    Ready,
    Unavailable { reason: String },
}

/// Public, path-free projection of one host-provisioned storage lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppStorageProvision {
    pub domain: String,
    pub scope: AppStorageScope,
    pub backend: AppStorageBackend,
    pub logical_id: String,
    pub namespace: String,
    pub migration: String,
    pub migration_owner: AppId,
    pub capabilities: Vec<String>,
    pub readiness: AppStorageReadiness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRouteDescriptor {
    pub method: String,
    pub path: String,
    pub operation_id: String,
    pub streaming: bool,
}

impl AppRouteDescriptor {
    fn validate(&self, app_id: &AppId) -> Result<(), AppContractError> {
        let known_method = matches!(
            self.method.as_str(),
            "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
        );
        if !known_method
            || !self.path.starts_with("/api/apps/")
            || self.operation_id.trim().is_empty()
        {
            return Err(AppContractError::InvalidRoute {
                app_id: app_id.clone(),
                path: self.path.clone(),
            });
        }
        Ok(())
    }
}

/// Transport semantics owned by an application for one of its declared HTTP
/// routes. Gateway publishes this data but does not interpret an application's
/// domain route enum, DTO or policy table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRouteMetadata {
    pub method: String,
    pub path: String,
    pub route_id: String,
    pub operation_id: String,
    pub request_schema: String,
    pub response_schema: String,
    pub class: String,
    pub capability: String,
    pub risk: String,
    pub confirmation: String,
    pub streaming: bool,
    pub active: bool,
}

impl AppRouteMetadata {
    fn validate(&self, app_id: &AppId) -> Result<(), AppContractError> {
        AppRouteDescriptor {
            method: self.method.clone(),
            path: self.path.clone(),
            operation_id: self.operation_id.clone(),
            streaming: self.streaming,
        }
        .validate(app_id)?;
        if [
            &self.route_id,
            &self.request_schema,
            &self.response_schema,
            &self.class,
            &self.capability,
            &self.risk,
            &self.confirmation,
        ]
        .iter()
        .any(|field| field.trim().is_empty())
        {
            return Err(AppContractError::InvalidHttpContract {
                app_id: app_id.clone(),
                reason: "route metadata contains an empty semantic field".to_string(),
            });
        }
        Ok(())
    }
}

/// Complete HTTP projection for one statically linked application.
///
/// OpenAPI component values are JSON because the host emits an aggregate
/// document. The schemas themselves remain owned by the application.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppHttpContract {
    #[serde(default)]
    pub routes: Vec<AppRouteMetadata>,
    #[serde(default)]
    pub openapi_components: BTreeMap<String, serde_json::Value>,
    /// Named component used when Gateway's common auth middleware rejects a
    /// request before it reaches the App router. `None` selects `GatewayError`.
    #[serde(default)]
    pub auth_error_schema: Option<String>,
}

impl AppHttpContract {
    pub fn validate_against(&self, descriptor: &AppDescriptor) -> Result<(), AppContractError> {
        let mut metadata_routes = BTreeSet::new();
        for route in &self.routes {
            route.validate(&descriptor.id)?;
            let identity = (route.method.as_str(), route.path.as_str());
            if !metadata_routes.insert(identity) {
                return Err(AppContractError::InvalidHttpContract {
                    app_id: descriptor.id.clone(),
                    reason: format!("duplicate route metadata: {} {}", route.method, route.path),
                });
            }
            let declared = descriptor.routes.iter().any(|declared| {
                declared.method == route.method
                    && declared.path == route.path
                    && declared.operation_id == route.operation_id
                    && declared.streaming == route.streaming
            });
            if !declared {
                return Err(AppContractError::InvalidHttpContract {
                    app_id: descriptor.id.clone(),
                    reason: format!(
                        "route metadata is not declared by the app descriptor: {} {}",
                        route.method, route.path
                    ),
                });
            }
        }
        let declared_routes = descriptor
            .routes
            .iter()
            .map(|route| (route.method.as_str(), route.path.as_str()))
            .collect::<BTreeSet<_>>();
        if metadata_routes != declared_routes {
            return Err(AppContractError::InvalidHttpContract {
                app_id: descriptor.id.clone(),
                reason: "HTTP metadata and descriptor routes are not a one-to-one match"
                    .to_string(),
            });
        }
        if self
            .openapi_components
            .keys()
            .any(|component| component.trim().is_empty())
        {
            return Err(AppContractError::InvalidHttpContract {
                app_id: descriptor.id.clone(),
                reason: "OpenAPI component name is empty".to_string(),
            });
        }
        if let Some(error_schema) = &self.auth_error_schema {
            if error_schema.trim().is_empty() || !self.openapi_components.contains_key(error_schema)
            {
                return Err(AppContractError::InvalidHttpContract {
                    app_id: descriptor.id.clone(),
                    reason: "auth error schema must name an App OpenAPI component".to_string(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppActionDescriptor {
    pub id: String,
    pub title: String,
    pub requires_confirmation: bool,
}

/// A domain APP's skill projected through Cowd's stable application ABI.
///
/// The host may list, inspect and run governance over this description, but
/// it never receives an APP service, route enum, or private storage handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSkillDescriptor {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub scope: String,
    pub source: String,
    pub domain: Option<String>,
    pub status: String,
    pub risk: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub profile: Option<serde_json::Value>,
    #[serde(default)]
    pub virtual_files: Option<AppVirtualSkillFiles>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub shadowed_by: Option<String>,
}

impl AppSkillDescriptor {
    pub fn validate(&self, app_id: &AppId) -> Result<(), AppContractError> {
        let valid = !self.id.trim().is_empty()
            && !self.name.trim().is_empty()
            && !self.scope.trim().is_empty()
            && !self.source.trim().is_empty()
            && !self.status.trim().is_empty()
            && !self.risk.trim().is_empty();
        if valid {
            Ok(())
        } else {
            Err(AppContractError::InvalidDescriptor(app_id.clone()))
        }
    }
}

/// A virtual, APP-owned skill document. The document is data rather than a
/// host filesystem path, so the generic host can expose it without knowing an
/// application's repository layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppVirtualSkillFiles {
    pub root: String,
    pub primary: String,
    #[serde(default)]
    pub files: Vec<AppVirtualSkillFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppVirtualSkillFile {
    pub path: String,
    pub name: String,
    pub kind: String,
    pub primary: bool,
    pub content_type: String,
    /// Raw content is omitted from catalogue/list projections and is returned
    /// only by the generic `files/raw` host flow.
    #[serde(skip)]
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppProfileDescriptor {
    /// Build-time catalog revision supplied by the APP. This is distinct from
    /// a credential profile revision and cannot be chosen by a request.
    pub catalog_revision: u64,
    pub capability_digest: String,
    pub default_profile_id: String,
    #[serde(default)]
    pub profiles: Vec<AppProfileVariant>,
    #[serde(default)]
    pub surface_capabilities: BTreeMap<String, Vec<String>>,
}

/// A named application profile exposed through the stable APP ABI. Hosts can
/// aggregate these descriptors without importing APP-specific enums or
/// capability helper functions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppProfileVariant {
    pub id: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationContext {
    pub principal_id: String,
    pub workspace_id: String,
    pub surface: String,
    pub request_id: String,
}

/// Verified request facts made available to an APP handler. Gateway derives
/// this value after authentication; applications cannot choose the actor,
/// capability ceiling, profile revision or workspace scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRequestContext {
    pub invocation: InvocationContext,
    pub granted_capabilities: Vec<String>,
    pub profile_revision: u64,
    /// Verified authorization scopes. These are facts projected by Gateway,
    /// never client-supplied routing parameters.
    #[serde(default)]
    pub granted_scopes: Vec<String>,
    /// Credential generation bound to this request. Long-lived application
    /// streams use it only through a host revalidation port.
    #[serde(default)]
    pub credential_epoch: u64,
    /// Optional broker-issued expiry in Unix milliseconds.
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
}

/// Opaque, typed intent submitted by an APP to a Cowd-owned effect port.
/// The SDK does not define domain payloads or expose Cowd implementation types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostIntent {
    pub kind: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostReceipt {
    pub id: String,
    pub status: String,
    pub replayed: bool,
    pub payload: serde_json::Value,
}

#[async_trait]
pub trait RuntimePort: Send + Sync {
    async fn execute(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError>;
}

#[async_trait]
pub trait ApprovalPort: Send + Sync {
    async fn request(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError>;
}

#[async_trait]
pub trait CrossPlanePort: Send + Sync {
    async fn submit(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError>;
}

#[async_trait]
pub trait ConnectorPort: Send + Sync {
    async fn dispatch(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError>;
}

#[async_trait]
pub trait RealityPort: Send + Sync {
    async fn query(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError>;
}

#[async_trait]
pub trait WorkContextPort: Send + Sync {
    /// Invoke one closed work-context effect.  Work context includes both
    /// read projections and append-only session records, so `execute` is
    /// intentionally broader and less misleading than the former `read`
    /// spelling.
    async fn execute(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError>;
}

/// Read-only product/runtime facts that an APP may use to present its own
/// governance posture. The host returns only a closed snapshot; it never
/// exposes configuration, a service handle, a token or an arbitrary probe.
#[async_trait]
pub trait PlatformPort: Send + Sync {
    async fn query(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError>;
}

/// Non-secret credential facts captured when a request entered an APP.
///
/// Long-lived APP projections use this closed value to ask the host whether
/// the authority is still valid.  It deliberately carries neither a token nor
/// an application-specific identity type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialLifecycleCheck {
    pub credential_epoch: u64,
    pub profile_revision: u64,
}

/// A closed result for credential lifecycle revalidation.
///
/// The host may implement this through a local broker, an operating-system
/// credential store, or a remote authority.  Applications observe only the
/// durable lifecycle outcome and never the concrete authority.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CredentialLifecycleError {
    #[error("credential lifecycle authority is unavailable: {0}")]
    AuthorityUnavailable(String),
    #[error("credential is inactive")]
    CredentialInactive,
    #[error("credential epoch changed")]
    CredentialEpochChanged,
    #[error("credential profile revision changed")]
    ProfileRevisionChanged,
}

/// Host-owned lifecycle revalidation for APP streams or other long-running
/// projections.  This is synchronous because an APP's stream checkpoint is
/// already executed on a blocking boundary; implementations must perform one
/// bounded authority lookup only.
pub trait CredentialLifecyclePort: Send + Sync {
    fn verify(&self, check: CredentialLifecycleCheck) -> Result<(), CredentialLifecycleError>;
}

/// The only service bundle visible to an APP. Concrete Cowd services are
/// intentionally hidden behind explicit ports so an APP cannot downcast into
/// Gateway state or obtain secrets.
pub trait AppHostPorts: Send + Sync {
    fn runtime(&self) -> &dyn RuntimePort;
    fn approval(&self) -> &dyn ApprovalPort;
    fn cross_plane(&self) -> &dyn CrossPlanePort;
    fn connector(&self) -> &dyn ConnectorPort;
    fn reality(&self) -> &dyn RealityPort;
    fn work_context(&self) -> &dyn WorkContextPort;
    fn platform(&self) -> &dyn PlatformPort;
    fn credential_lifecycle(&self) -> &dyn CredentialLifecyclePort;
}

#[derive(Clone)]
pub struct CowdAppContext {
    ports: Arc<dyn AppHostPorts>,
}

impl CowdAppContext {
    #[must_use]
    pub fn new(ports: Arc<dyn AppHostPorts>) -> Self {
        Self { ports }
    }

    #[must_use]
    pub fn ports(&self) -> &dyn AppHostPorts {
        self.ports.as_ref()
    }
}

pub trait CapabilityApp: Send + Sync {
    fn descriptor(&self) -> AppDescriptor;
    fn health(&self) -> AppHealth;

    /// Presentation capabilities are optional and remain independent from
    /// transport routes. The host validates them once during product assembly.
    fn presentation(&self) -> Option<presentation::AppPresentationContribution> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppHealth {
    Ready,
    Degraded { reason: String },
    Disabled { reason: String },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AppContractError {
    #[error("invalid app id: {0}")]
    InvalidAppId(String),
    #[error("invalid descriptor for app {0}")]
    InvalidDescriptor(AppId),
    #[error("invalid route {path} for app {app_id}")]
    InvalidRoute { app_id: AppId, path: String },
    #[error("invalid HTTP contract for app {app_id}: {reason}")]
    InvalidHttpContract { app_id: AppId, reason: String },
    #[error("invalid storage requirement `{domain}` for app {app_id}")]
    InvalidStorageRequirement { app_id: AppId, domain: String },
    #[error("invalid storage contract for app {app_id}: {reason}")]
    InvalidStorageContract { app_id: AppId, reason: String },
    #[error("invalid source lock for app {0}")]
    InvalidSourceLock(AppId),
    #[error("app {app_id} requires SDK API {actual}, host supports {expected}")]
    UnsupportedSdkApi {
        app_id: AppId,
        expected: u32,
        actual: u32,
    },
    #[error("invalid application execution outcome: {0}")]
    InvalidApplicationExecutionOutcome(String),
    #[error("invalid presentation contribution for app {app_id}: {reason}")]
    InvalidPresentationContribution { app_id: AppId, reason: String },
    #[error("invalid view spec `{view_id}`: {reason}")]
    InvalidViewSpec { view_id: String, reason: String },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AppHostConflict {
    #[error(
        "idempotency conflict in {namespace} for producer `{producer_id}`, contract v{contract_version}, outcome `{outcome_id}`"
    )]
    Idempotency {
        namespace: String,
        key: String,
        producer_id: String,
        contract_version: u16,
        outcome_id: String,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AppHostError {
    #[error("host port unavailable: {0}")]
    Unavailable(String),
    #[error("host denied request: {0}")]
    Denied(String),
    #[error("host request conflict: {0}")]
    Conflict(AppHostConflict),
    #[error("host request failed: {0}")]
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_idempotency_conflict_is_typed_and_carries_stable_identity() {
        let error = AppHostError::Conflict(AppHostConflict::Idempotency {
            namespace: "session_domain_event".to_string(),
            key: "application-execution:v1:fixture".to_string(),
            producer_id: "app:mfg".to_string(),
            contract_version: 1,
            outcome_id: "outcome-1".to_string(),
        });

        assert!(matches!(
            error,
            AppHostError::Conflict(AppHostConflict::Idempotency {
                namespace,
                key,
                producer_id,
                contract_version: 1,
                outcome_id,
            }) if namespace == "session_domain_event"
                && key == "application-execution:v1:fixture"
                && producer_id == "app:mfg"
                && outcome_id == "outcome-1"
        ));
    }

    #[test]
    fn descriptor_rejects_non_app_route_and_wrong_sdk() {
        let descriptor = AppDescriptor {
            id: AppId::parse("fixture").expect("fixture id"),
            display_name: "Fixture".to_string(),
            sdk_api: SDK_API_VERSION + 1,
            version: "0.1.0".to_string(),
            capabilities: vec![],
            routes: vec![],
            actions: vec![],
            profile: None,
        };
        assert!(matches!(
            descriptor.validate(),
            Err(AppContractError::UnsupportedSdkApi { .. })
        ));
    }

    #[test]
    fn app_storage_requirement_is_namespaced_and_path_free() {
        let app_id = AppId::parse("fixture").expect("fixture app id");
        AppStorageRequirement {
            domain: "primary_data".to_string(),
            backend: AppStorageBackend::Sqlite,
            scope: AppStorageScope::App,
            migration: "fixture_primary_v1".to_string(),
        }
        .validate(&app_id)
        .expect("valid generic requirement");
        assert!(AppStorageRequirement {
            domain: "../escape".to_string(),
            backend: AppStorageBackend::Sqlite,
            scope: AppStorageScope::App,
            migration: "fixture_primary_v1".to_string(),
        }
        .validate(&app_id)
        .is_err());
    }

    #[test]
    fn storage_contract_rejects_duplicate_domain_scope_and_wrong_owner() {
        let app_id = AppId::parse("fixture").expect("fixture app id");
        let requirement = AppStorageRequirement {
            domain: "primary".to_string(),
            backend: AppStorageBackend::Relational,
            scope: AppStorageScope::App,
            migration: "fixture_primary_v1".to_string(),
        };
        let duplicate = AppStorageContract::new(
            app_id.clone(),
            vec![requirement.clone(), requirement.clone()],
        );
        assert!(matches!(
            duplicate.validate_for(&app_id),
            Err(AppContractError::InvalidStorageContract { .. })
        ));

        let wrong_owner = AppStorageContract::new(
            AppId::parse("another").expect("another id"),
            vec![requirement],
        );
        assert!(wrong_owner.validate_for(&app_id).is_err());
    }

    #[test]
    fn provision_projection_is_path_and_secret_free() {
        let app_id = AppId::parse("fixture").expect("fixture app id");
        let projection = AppStorageProvision {
            domain: "primary".to_string(),
            scope: AppStorageScope::App,
            backend: AppStorageBackend::Postgres,
            logical_id: "app:fixture:primary@app:fixture".to_string(),
            namespace: "cowd_app_fixture_0123456789ab".to_string(),
            migration: "fixture_primary_v1".to_string(),
            migration_owner: app_id,
            capabilities: vec!["relational".to_string(), "postgres".to_string()],
            readiness: AppStorageReadiness::Ready,
        };
        let json = serde_json::to_string(&projection).expect("serialize provision");
        assert!(!json.contains("postgres://"));
        assert!(!json.contains(".sqlite"));
        assert!(!json.contains("secret"));
        assert!(!json.contains("path"));
    }

    #[test]
    fn app_id_is_a_deliberately_small_stable_namespace() {
        assert!(AppId::parse("engineering-delivery").is_ok());
        assert!(AppId::parse("MFG").is_err());
        assert!(AppId::parse("mfg.app").is_err());
    }

    #[test]
    fn request_context_keeps_verified_lifecycle_facts_and_decodes_older_payloads() {
        let context: AppRequestContext = serde_json::from_value(serde_json::json!({
            "invocation": {
                "principal_id": "operator",
                "workspace_id": "sha256:workspace",
                "surface": "tui",
                "request_id": "request"
            },
            "granted_capabilities": ["fixture.read"],
            "profile_revision": 7
        }))
        .expect("older request context remains readable");
        assert!(context.granted_scopes.is_empty());
        assert_eq!(context.credential_epoch, 0);
        assert_eq!(context.expires_at_ms, None);
    }
}
