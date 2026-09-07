#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
//! Connector contracts, external resources, and connector-local stores.
//!
//! This module describes provider accounts, external data/resource
//! capabilities, and resource references without binding them to message
//! surfaces or service SDKs. The cross-plane policy engine remains the
//! execution governance layer.

pub mod source;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
pub use harness_contract::policy::{CrossPlaneRisk, DataClassification};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub use source::{
    builtin_source_adapter_manifests, canonical_source_resource_ref, read_local_source_batch,
    source_adapter_manifest, SourceAdapterManifest, SourceBatchCursor, SourceConnectorState,
    SourceEventBatch, SourceFieldSchema, SourceIncrementalRunRequest, SourceIncrementalRunResult,
    SourceIngestionReceipt, SourceReadPlan, SourceRecordBatch, SourceTableSchema, SourceWatermark,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorPlane {
    Service,
    Mcp,
    Tool,
    Agent,
    Governance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorHealthStatus {
    Ready,
    Disabled,
    Degraded,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorHealth {
    pub status: ConnectorHealthStatus,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub checked_at: Option<DateTime<Utc>>,
}

impl ConnectorHealth {
    #[must_use]
    pub fn ready() -> Self {
        Self {
            status: ConnectorHealthStatus::Ready,
            reason: None,
            checked_at: Some(Utc::now()),
        }
    }

    #[must_use]
    pub fn disabled(reason: impl Into<String>) -> Self {
        Self {
            status: ConnectorHealthStatus::Disabled,
            reason: Some(reason.into()),
            checked_at: Some(Utc::now()),
        }
    }

    #[must_use]
    pub fn degraded(reason: impl Into<String>) -> Self {
        Self {
            status: ConnectorHealthStatus::Degraded,
            reason: Some(reason.into()),
            checked_at: Some(Utc::now()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAccount {
    pub provider: String,
    pub account_id: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    pub auth_mode: String,
    #[serde(default)]
    pub secret_refs: Vec<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub enabled_bindings: Vec<String>,
    #[serde(default)]
    pub default_for_agent: bool,
    pub health: ConnectorHealth,
    #[serde(default)]
    pub last_used_at: Option<DateTime<Utc>>,
}

impl ProviderAccount {
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        account_id: impl Into<String>,
        auth_mode: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            account_id: account_id.into(),
            tenant_id: None,
            auth_mode: auth_mode.into(),
            secret_refs: Vec::new(),
            scopes: Vec::new(),
            enabled_bindings: Vec::new(),
            default_for_agent: false,
            health: ConnectorHealth::disabled("account is declared but not configured"),
            last_used_at: None,
        }
    }

    #[must_use]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|item| item == scope)
    }

    #[must_use]
    pub fn missing_scopes<'a>(&self, required: &'a [String]) -> Vec<&'a str> {
        required
            .iter()
            .filter(|scope| !self.has_scope(scope))
            .map(String::as_str)
            .collect()
    }

    #[must_use]
    pub fn mcp_server(
        server_name: impl Into<String>,
        transport: impl Into<String>,
        health: ConnectorHealth,
    ) -> Self {
        let server_name = server_name.into();
        let mut account = Self::new("mcp", server_name.clone(), transport);
        account.secret_refs = vec![format!("config://mcpServers/{server_name}")];
        account.enabled_bindings = vec![CapabilityManifest::mcp_server(&server_name).capability_id];
        account.health = health;
        account
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    pub capability_id: String,
    pub family: String,
    pub provider: String,
    pub plane: ConnectorPlane,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    pub risk: CrossPlaneRisk,
    pub data_classification: DataClassification,
    #[serde(default)]
    pub supports_dry_run: bool,
    #[serde(default)]
    pub supports_commit: bool,
    #[serde(default)]
    pub requires_approval: bool,
    #[serde(default)]
    pub input_schema_ref: Option<String>,
    #[serde(default)]
    pub output_schema_ref: Option<String>,
}

impl CapabilityManifest {
    #[must_use]
    pub fn service(provider: impl Into<String>, operation: impl Into<String>) -> Self {
        let provider = provider.into();
        let operation = operation.into();
        Self {
            capability_id: format!("service.{provider}.{operation}"),
            family: format!("service.{provider}"),
            provider,
            plane: ConnectorPlane::Service,
            required_scopes: Vec::new(),
            risk: CrossPlaneRisk::Medium,
            data_classification: DataClassification::Internal,
            supports_dry_run: true,
            supports_commit: false,
            requires_approval: true,
            input_schema_ref: Some(format!("schema://connector/service/{operation}/input")),
            output_schema_ref: Some("schema://connector/service/resource/output".to_string()),
        }
    }

    #[must_use]
    pub fn service_readonly(provider: impl Into<String>, operation: impl Into<String>) -> Self {
        let provider = provider.into();
        let operation = operation.into();
        Self {
            capability_id: format!("service.{provider}.{operation}"),
            family: format!("service.{provider}"),
            required_scopes: readonly_required_scopes(&provider, &operation),
            provider,
            plane: ConnectorPlane::Service,
            risk: CrossPlaneRisk::Low,
            data_classification: DataClassification::Internal,
            supports_dry_run: true,
            supports_commit: true,
            requires_approval: false,
            input_schema_ref: Some(format!(
                "schema://connector/service/{operation}/readonly/input"
            )),
            output_schema_ref: Some("schema://connector/service/resource/output".to_string()),
        }
    }

    #[must_use]
    pub fn governance(capability_id: impl Into<String>) -> Self {
        let capability_id = capability_id.into();
        Self {
            family: "governance.cross_plane".to_string(),
            provider: "runtime".to_string(),
            plane: ConnectorPlane::Governance,
            required_scopes: Vec::new(),
            risk: CrossPlaneRisk::Medium,
            data_classification: DataClassification::Internal,
            supports_dry_run: true,
            supports_commit: true,
            requires_approval: false,
            input_schema_ref: Some("schema://connector/governance/action/input".to_string()),
            output_schema_ref: Some("schema://connector/governance/receipt/output".to_string()),
            capability_id,
        }
    }

    #[must_use]
    pub fn mcp_server(server_name: impl AsRef<str>) -> Self {
        let normalized_server = normalize_name_for_mcp(server_name.as_ref());
        Self {
            capability_id: format!("mcp.{normalized_server}.server"),
            family: format!("mcp.{normalized_server}"),
            provider: "mcp".to_string(),
            plane: ConnectorPlane::Mcp,
            required_scopes: Vec::new(),
            risk: CrossPlaneRisk::Low,
            data_classification: DataClassification::Internal,
            supports_dry_run: true,
            supports_commit: false,
            requires_approval: false,
            input_schema_ref: Some("schema://connector/mcp/server/input".to_string()),
            output_schema_ref: Some("schema://connector/mcp/server/output".to_string()),
        }
    }

    #[must_use]
    pub fn mcp_tool(server_name: impl AsRef<str>, tool_name: impl AsRef<str>) -> Self {
        let normalized_server = normalize_name_for_mcp(server_name.as_ref());
        let normalized_tool = normalize_name_for_mcp(tool_name.as_ref());
        Self {
            capability_id: format!("mcp.{normalized_server}.tool.{normalized_tool}"),
            family: format!("mcp.{normalized_server}"),
            provider: "mcp".to_string(),
            plane: ConnectorPlane::Mcp,
            required_scopes: Vec::new(),
            risk: CrossPlaneRisk::Medium,
            data_classification: DataClassification::Internal,
            supports_dry_run: false,
            supports_commit: true,
            requires_approval: true,
            input_schema_ref: Some(format!(
                "schema://connector/mcp/{normalized_server}/tool/{normalized_tool}/input"
            )),
            output_schema_ref: Some("schema://connector/mcp/tool/output".to_string()),
        }
    }

    #[must_use]
    pub fn mcp_resource(server_name: impl AsRef<str>, resource_kind: impl AsRef<str>) -> Self {
        let normalized_server = normalize_name_for_mcp(server_name.as_ref());
        let normalized_kind = normalize_name_for_mcp(resource_kind.as_ref());
        Self {
            capability_id: format!("mcp.{normalized_server}.resource.{normalized_kind}"),
            family: format!("mcp.{normalized_server}"),
            provider: "mcp".to_string(),
            plane: ConnectorPlane::Mcp,
            required_scopes: Vec::new(),
            risk: CrossPlaneRisk::Low,
            data_classification: DataClassification::Internal,
            supports_dry_run: true,
            supports_commit: false,
            requires_approval: false,
            input_schema_ref: Some("schema://connector/mcp/resource/input".to_string()),
            output_schema_ref: Some("schema://connector/mcp/resource/output".to_string()),
        }
    }

    #[must_use]
    pub fn missing_scopes<'a>(&'a self, account: &'a ProviderAccount) -> Vec<&'a str> {
        account.missing_scopes(&self.required_scopes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalResourceRef {
    pub reference: String,
    pub provider: String,
    #[serde(default)]
    pub account_id: Option<String>,
    pub resource_type: String,
    pub title: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub permissions_summary: Option<String>,
    #[serde(default)]
    pub digest: Option<String>,
    pub indexed_state: String,
}

impl ExternalResourceRef {
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        resource_type: impl Into<String>,
        resource_id: impl AsRef<str>,
        title: impl Into<String>,
    ) -> Self {
        let provider = provider.into();
        let resource_type = resource_type.into();
        let resource_id = resource_id.as_ref().trim().trim_matches('/');
        Self {
            reference: format!("service://{provider}/{resource_type}/{resource_id}"),
            provider,
            account_id: None,
            resource_type,
            title: title.into(),
            source: None,
            permissions_summary: None,
            digest: None,
            indexed_state: "unknown".to_string(),
        }
    }

    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.reference.starts_with("service://") || self.reference.starts_with("mcp://")
    }

    #[must_use]
    pub fn mcp_resource(
        server_name: impl AsRef<str>,
        uri: impl AsRef<str>,
        title: impl Into<String>,
    ) -> Self {
        let server_name = server_name.as_ref();
        let normalized_server = normalize_name_for_mcp(server_name);
        let uri = uri.as_ref();
        let digest = stable_hex_hash(uri);
        Self {
            reference: format!("mcp://{normalized_server}/{digest}"),
            provider: "mcp".to_string(),
            account_id: Some(server_name.to_string()),
            resource_type: "resource".to_string(),
            title: title.into(),
            source: Some(uri.to_string()),
            permissions_summary: Some(
                "MCP resource access is governed by server configuration".to_string(),
            ),
            digest: Some(digest),
            indexed_state: "unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorRegistrySnapshot {
    pub generated_at: DateTime<Utc>,
    #[serde(default)]
    pub accounts: Vec<ProviderAccount>,
    #[serde(default)]
    pub capabilities: Vec<CapabilityManifest>,
    #[serde(default)]
    pub resources: Vec<ExternalResourceRef>,
    #[serde(default)]
    pub degraded: bool,
    #[serde(default)]
    pub degraded_reasons: Vec<String>,
}

impl ConnectorRegistrySnapshot {
    #[must_use]
    pub fn new(
        accounts: Vec<ProviderAccount>,
        capabilities: Vec<CapabilityManifest>,
        resources: Vec<ExternalResourceRef>,
    ) -> Self {
        let degraded_reasons = accounts
            .iter()
            .filter(|account| {
                matches!(
                    account.health.status,
                    ConnectorHealthStatus::Degraded | ConnectorHealthStatus::Unknown
                )
            })
            .filter_map(|account| {
                account
                    .health
                    .reason
                    .as_ref()
                    .map(|reason| format!("account:{}:{reason}", account.account_id))
            })
            .collect::<Vec<_>>();
        Self {
            generated_at: Utc::now(),
            accounts,
            capabilities,
            resources,
            degraded: !degraded_reasons.is_empty(),
            degraded_reasons,
        }
    }

    #[must_use]
    pub fn empty_with_default_capabilities() -> Self {
        Self::new(Vec::new(), default_capabilities(), Vec::new())
    }

    #[must_use]
    pub fn summary(&self) -> ConnectorSummary {
        let service_capabilities = self
            .capabilities
            .iter()
            .filter(|capability| capability.plane == ConnectorPlane::Service)
            .count();
        let governance_capabilities = self
            .capabilities
            .iter()
            .filter(|capability| capability.plane == ConnectorPlane::Governance)
            .count();
        let mcp_capabilities = self
            .capabilities
            .iter()
            .filter(|capability| capability.plane == ConnectorPlane::Mcp)
            .count();
        ConnectorSummary {
            account_count: self.accounts.len(),
            capability_count: self.capabilities.len(),
            resource_count: self.resources.len(),
            service_capabilities,
            governance_capabilities,
            mcp_capabilities,
            degraded: self.degraded,
            degraded_reasons: self.degraded_reasons.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorSummary {
    pub account_count: usize,
    pub capability_count: usize,
    pub resource_count: usize,
    pub service_capabilities: usize,
    pub governance_capabilities: usize,
    pub mcp_capabilities: usize,
    pub degraded: bool,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ResourceDirectory {
    resources: RwLock<HashMap<String, ExternalResourceRef>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("resource directory error: {message}")]
pub struct ResourceDirectoryError {
    message: String,
}

impl ResourceDirectoryError {
    #[must_use]
    pub fn backend(error: impl std::fmt::Display) -> Self {
        Self {
            message: error.to_string(),
        }
    }
}

pub type ResourceDirectoryResult<T> = Result<T, ResourceDirectoryError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDirectorySourceBinding {
    pub reference: String,
    pub source_kind: String,
    pub source_id: String,
    pub attached_at: String,
}

pub trait ResourceDirectoryRepository: Send + Sync {
    fn upsert(
        &self,
        resource: &ExternalResourceRef,
    ) -> ResourceDirectoryResult<ExternalResourceRef>;
    fn get(&self, reference: &str) -> ResourceDirectoryResult<Option<ExternalResourceRef>>;
    fn list_recent(&self, limit: usize) -> ResourceDirectoryResult<Vec<ExternalResourceRef>>;
    fn list_page(
        &self,
        limit: usize,
        offset: usize,
    ) -> ResourceDirectoryResult<Vec<ExternalResourceRef>>;
    fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> ResourceDirectoryResult<Vec<ExternalResourceRef>>;
    fn mark_indexed(&self, reference: &str) -> ResourceDirectoryResult<bool>;
    fn mark_stale(&self, reference: &str) -> ResourceDirectoryResult<bool>;
    fn attach_source(
        &self,
        reference: &str,
        source_kind: &str,
        source_id: &str,
    ) -> ResourceDirectoryResult<()>;
    fn list_sources(
        &self,
        reference: &str,
    ) -> ResourceDirectoryResult<Vec<ResourceDirectorySourceBinding>>;
}

pub trait ResourceDirectoryFactory: Send + Sync {
    fn open(
        &self,
        handle: &storage::StorageHandle,
    ) -> ResourceDirectoryResult<Arc<dyn ResourceDirectoryRepository>>;

    fn is_initialized(&self, handle: &storage::StorageHandle) -> bool;
}

#[derive(Debug, Default)]
pub struct UnavailableResourceDirectoryFactory;

impl ResourceDirectoryFactory for UnavailableResourceDirectoryFactory {
    fn open(
        &self,
        _handle: &storage::StorageHandle,
    ) -> ResourceDirectoryResult<Arc<dyn ResourceDirectoryRepository>> {
        Err(ResourceDirectoryError::backend(
            "resource directory is unavailable until the PostgreSQL owner is composed",
        ))
    }

    fn is_initialized(&self, _handle: &storage::StorageHandle) -> bool {
        false
    }
}

impl ResourceDirectory {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, resource: ExternalResourceRef) -> ExternalResourceRef {
        let mut resources = self
            .resources
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        resources.insert(resource.reference.clone(), resource.clone());
        resource
    }

    #[must_use]
    pub fn get(&self, reference: &str) -> Option<ExternalResourceRef> {
        let resources = self
            .resources
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        resources.get(reference).cloned()
    }

    #[must_use]
    pub fn list_recent(&self, limit: usize) -> Vec<ExternalResourceRef> {
        let resources = self
            .resources
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut items = resources.values().cloned().collect::<Vec<_>>();
        items.sort_by(|left, right| left.reference.cmp(&right.reference));
        items.truncate(limit);
        items
    }

    #[must_use]
    pub fn search(&self, query: &str, limit: usize) -> Vec<ExternalResourceRef> {
        let query = query.trim().to_ascii_lowercase();
        if query.is_empty() {
            return self.list_recent(limit);
        }
        let resources = self
            .resources
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut items = resources
            .values()
            .filter(|resource| {
                resource.reference.to_ascii_lowercase().contains(&query)
                    || resource.title.to_ascii_lowercase().contains(&query)
                    || resource.resource_type.to_ascii_lowercase().contains(&query)
            })
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by(|left, right| left.reference.cmp(&right.reference));
        items.truncate(limit);
        items
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceConnectorMetadata {
    pub id: String,
    pub provider: String,
    pub family: String,
    pub display_name: String,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceToolRequest {
    pub tool_id: String,
    pub resource_id: String,
    pub title: String,
    #[serde(default)]
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceToolResult {
    pub status: String,
    pub tool_id: String,
    #[serde(default)]
    pub resource: Option<ExternalResourceRef>,
    #[serde(default)]
    pub output: Value,
}

pub trait ServiceConnector: Send + Sync {
    fn metadata(&self) -> ServiceConnectorMetadata;
    fn capabilities(&self) -> Vec<CapabilityManifest>;
    fn probe(&self, accounts: &[ProviderAccount]) -> ConnectorHealth {
        if accounts.iter().any(|account| {
            account.provider == self.metadata().provider
                && matches!(account.health.status, ConnectorHealthStatus::Ready)
        }) {
            ConnectorHealth::ready()
        } else {
            ConnectorHealth::degraded(format!(
                "no ready provider account for {}",
                self.metadata().provider
            ))
        }
    }
    fn execute_tool(&self, request: ServiceToolRequest) -> ServiceToolResult;
}

#[derive(Debug)]
pub struct ConnectorBulkhead {
    max_in_flight_per_provider: usize,
    failure_threshold: u32,
    cooldown: Duration,
    providers: Mutex<HashMap<String, ConnectorProviderBulkheadState>>,
}

#[derive(Debug, Clone, Default)]
struct ConnectorProviderBulkheadState {
    in_flight: usize,
    consecutive_failures: u32,
    cooldown_until: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorBulkheadRejection {
    Busy {
        provider: String,
        in_flight: usize,
        max_in_flight: usize,
    },
    CoolingDown {
        provider: String,
    },
}

#[derive(Debug)]
pub struct ConnectorBulkheadGuard<'a> {
    provider: String,
    bulkhead: &'a ConnectorBulkhead,
}

impl ConnectorBulkhead {
    #[must_use]
    pub fn new(
        max_in_flight_per_provider: usize,
        failure_threshold: u32,
        cooldown: Duration,
    ) -> Self {
        Self {
            max_in_flight_per_provider: max_in_flight_per_provider.max(1),
            failure_threshold: failure_threshold.max(1),
            cooldown,
            providers: Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn default_service_gate() -> Self {
        Self::new(4, 3, Duration::from_secs(30))
    }

    pub fn try_acquire(
        &self,
        provider: impl Into<String>,
    ) -> Result<ConnectorBulkheadGuard<'_>, ConnectorBulkheadRejection> {
        let provider = provider.into();
        let now = Instant::now();
        let mut providers = self
            .providers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = providers.entry(provider.clone()).or_default();
        if state.cooldown_until.is_some_and(|until| until > now) {
            return Err(ConnectorBulkheadRejection::CoolingDown { provider });
        }
        if state.in_flight >= self.max_in_flight_per_provider {
            return Err(ConnectorBulkheadRejection::Busy {
                provider,
                in_flight: state.in_flight,
                max_in_flight: self.max_in_flight_per_provider,
            });
        }
        state.in_flight += 1;
        Ok(ConnectorBulkheadGuard {
            provider,
            bulkhead: self,
        })
    }

    pub fn record_success(&self, provider: &str) {
        let mut providers = self
            .providers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = providers.entry(provider.to_string()).or_default();
        state.consecutive_failures = 0;
        state.cooldown_until = None;
    }

    pub fn record_failure(&self, provider: &str) {
        let mut providers = self
            .providers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = providers.entry(provider.to_string()).or_default();
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.consecutive_failures >= self.failure_threshold {
            state.cooldown_until = Some(Instant::now() + self.cooldown);
        }
    }

    #[must_use]
    pub fn in_flight(&self, provider: &str) -> usize {
        self.providers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(provider)
            .map(|state| state.in_flight)
            .unwrap_or(0)
    }

    fn release(&self, provider: &str) {
        let mut providers = self
            .providers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(state) = providers.get_mut(provider) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }
}

impl Default for ConnectorBulkhead {
    fn default() -> Self {
        Self::default_service_gate()
    }
}

impl Drop for ConnectorBulkheadGuard<'_> {
    fn drop(&mut self) {
        self.bulkhead.release(&self.provider);
    }
}

#[derive(Default)]
pub struct ServiceConnectorRegistry {
    connectors: Vec<Box<dyn ServiceConnector>>,
}

impl ServiceConnectorRegistry {
    #[must_use]
    pub fn new(connectors: Vec<Box<dyn ServiceConnector>>) -> Self {
        Self { connectors }
    }

    #[must_use]
    pub fn builtin() -> Self {
        Self::new(vec![Box::new(LocalDocsServiceConnector::new())])
    }

    #[must_use]
    pub fn services(&self) -> Vec<ServiceConnectorMetadata> {
        self.connectors
            .iter()
            .map(|connector| connector.metadata())
            .collect()
    }

    #[must_use]
    pub fn connector_refs(&self) -> Vec<&dyn ServiceConnector> {
        self.connectors.iter().map(Box::as_ref).collect()
    }

    #[must_use]
    pub fn capabilities(&self) -> Vec<CapabilityManifest> {
        self.connectors
            .iter()
            .flat_map(|connector| connector.capabilities())
            .collect()
    }

    #[must_use]
    pub fn connector(&self, service_id: &str) -> Option<&dyn ServiceConnector> {
        self.connectors
            .iter()
            .map(Box::as_ref)
            .find(|connector| connector.metadata().id == service_id)
    }
}

#[must_use]
pub fn builtin_service_connector_registry() -> ServiceConnectorRegistry {
    ServiceConnectorRegistry::builtin()
}

#[derive(Debug, Clone, Default)]
pub struct LocalDocsServiceConnector;

impl LocalDocsServiceConnector {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl ServiceConnector for LocalDocsServiceConnector {
    fn metadata(&self) -> ServiceConnectorMetadata {
        ServiceConnectorMetadata {
            id: "local.docs".to_string(),
            provider: "local".to_string(),
            family: "service.local.docs".to_string(),
            display_name: "Local Docs".to_string(),
            read_only: true,
        }
    }

    fn capabilities(&self) -> Vec<CapabilityManifest> {
        ["read", "export", "summarize_ref"]
            .into_iter()
            .map(|operation| CapabilityManifest::service_readonly("local.docs", operation))
            .collect()
    }

    fn execute_tool(&self, request: ServiceToolRequest) -> ServiceToolResult {
        let ServiceToolRequest {
            tool_id,
            resource_id,
            title,
            input: _,
        } = request;
        let operation = tool_id
            .rsplit('.')
            .next()
            .filter(|value| !value.is_empty())
            .unwrap_or("read")
            .to_string();
        let resource = ExternalResourceRef::new("local.docs", "document", &resource_id, &title);
        ServiceToolResult {
            status: "ok".to_string(),
            tool_id,
            resource: Some(resource.clone()),
            output: serde_json::json!({
                "operation": operation,
                "summary": format!("Local document `{title}` resolved as {}", resource.reference),
                "read_only": true,
            }),
        }
    }
}

#[must_use]
pub fn default_capabilities() -> Vec<CapabilityManifest> {
    let mut capabilities = vec![
        CapabilityManifest::governance("governance.cross_plane.identity_binding"),
        CapabilityManifest::governance("governance.cross_plane.grant"),
        CapabilityManifest::governance("governance.cross_plane.audit"),
        CapabilityManifest::governance("governance.cross_plane.policy_simulation"),
    ];
    capabilities.extend(builtin_service_connector_registry().capabilities());
    capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    capabilities
}

fn readonly_required_scopes(_provider: &str, _operation: &str) -> Vec<String> {
    Vec::new()
}

fn stable_hex_hash(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn normalize_name_for_mcp(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            normalized.push(character.to_ascii_lowercase());
        } else if !normalized.ends_with('_') {
            normalized.push('_');
        }
    }
    normalized.trim_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_account_reports_missing_scopes() {
        let mut account = ProviderAccount::new("local.docs", "local-docs-main", "api_key");
        account.scopes = vec!["document:read".to_string()];
        let required = vec!["document:read".to_string(), "document:write".to_string()];

        assert_eq!(account.missing_scopes(&required), vec!["document:write"]);
    }

    #[test]
    fn external_resource_ref_uses_canonical_service_scheme() {
        let resource = ExternalResourceRef::new("feishu", "docx", "doccn123", "Design");

        assert_eq!(resource.reference, "service://feishu/docx/doccn123");
        assert!(resource.is_canonical());
        let mut message_ref = resource;
        message_ref.reference = "channel://feishu/chat/open-id".to_string();
        assert!(!message_ref.is_canonical());
    }

    #[test]
    fn mcp_capability_contracts_are_stable() {
        let server = CapabilityManifest::mcp_server("github.com");
        assert_eq!(server.capability_id, "mcp.github_com.server");
        assert_eq!(server.plane, ConnectorPlane::Mcp);
        assert!(!server.requires_approval);

        let tool = CapabilityManifest::mcp_tool("github.com", "create issue");
        assert_eq!(tool.capability_id, "mcp.github_com.tool.create_issue");
        assert_eq!(tool.family, "mcp.github_com");
        assert!(tool.supports_commit);
        assert!(tool.requires_approval);

        let account = ProviderAccount::mcp_server("github.com", "stdio", ConnectorHealth::ready());
        assert_eq!(account.provider, "mcp");
        assert_eq!(account.account_id, "github.com");
        assert_eq!(account.enabled_bindings, vec!["mcp.github_com.server"]);
    }

    #[test]
    fn mcp_resource_ref_uses_canonical_scheme_without_exposing_uri_as_id() {
        let resource = ExternalResourceRef::mcp_resource(
            "github.com",
            "repo://owner/project/issues/1",
            "Issue 1",
        );

        assert!(resource.reference.starts_with("mcp://github_com/"));
        assert_eq!(resource.provider, "mcp");
        assert_eq!(resource.account_id.as_deref(), Some("github.com"));
        assert_eq!(
            resource.source.as_deref(),
            Some("repo://owner/project/issues/1")
        );
        assert!(resource.is_canonical());
    }

    #[test]
    fn default_snapshot_exposes_connector_capabilities_without_accounts() {
        let snapshot = ConnectorRegistrySnapshot::empty_with_default_capabilities();
        let summary = snapshot.summary();

        assert_eq!(summary.account_count, 0);
        assert!(summary.capability_count >= 4);
        assert!(snapshot
            .capabilities
            .iter()
            .any(|capability| capability.capability_id == "governance.cross_plane.audit"));
    }

    #[test]
    fn local_docs_service_connector_returns_resource_ref() {
        let connector = LocalDocsServiceConnector::new();
        let capabilities = connector.capabilities();
        assert!(capabilities
            .iter()
            .any(
                |capability| capability.capability_id == "service.local.docs.read"
                    && capability.plane == ConnectorPlane::Service
                    && capability.supports_commit
                    && !capability.requires_approval
            ));

        let result = connector.execute_tool(ServiceToolRequest {
            tool_id: "service.local.docs.read".to_string(),
            resource_id: "doc-1".to_string(),
            title: "Architecture".to_string(),
            input: serde_json::json!({}),
        });

        assert_eq!(result.status, "ok");
        assert_eq!(
            result.resource.unwrap().reference,
            "service://local.docs/document/doc-1"
        );
    }

    #[test]
    fn connector_bulkhead_rejects_when_provider_is_at_capacity() {
        let bulkhead = ConnectorBulkhead::new(1, 3, Duration::from_secs(30));
        let guard = bulkhead.try_acquire("feishu").unwrap();

        let rejected = bulkhead.try_acquire("feishu").unwrap_err();

        assert_eq!(
            rejected,
            ConnectorBulkheadRejection::Busy {
                provider: "feishu".to_string(),
                in_flight: 1,
                max_in_flight: 1,
            }
        );
        assert_eq!(bulkhead.in_flight("feishu"), 1);
        drop(guard);
        assert_eq!(bulkhead.in_flight("feishu"), 0);
        assert!(bulkhead.try_acquire("feishu").is_ok());
    }

    #[test]
    fn connector_bulkhead_enters_cooldown_after_repeated_failures() {
        let bulkhead = ConnectorBulkhead::new(2, 2, Duration::from_secs(30));

        bulkhead.record_failure("feishu");
        assert!(bulkhead.try_acquire("feishu").is_ok());
        bulkhead.record_failure("feishu");

        let rejected = bulkhead.try_acquire("feishu").unwrap_err();

        assert_eq!(
            rejected,
            ConnectorBulkheadRejection::CoolingDown {
                provider: "feishu".to_string(),
            }
        );
        bulkhead.record_success("feishu");
        assert!(bulkhead.try_acquire("feishu").is_ok());
    }

    #[test]
    fn resource_directory_upserts_and_searches_refs() {
        let directory = ResourceDirectory::new();
        let resource = ExternalResourceRef::new("local.docs", "document", "doc-2", "Runtime Plan");
        directory.upsert(resource.clone());

        assert_eq!(directory.get(&resource.reference), Some(resource.clone()));
        assert_eq!(directory.search("runtime", 10), vec![resource.clone()]);
        assert_eq!(directory.list_recent(1), vec![resource]);
    }
}
