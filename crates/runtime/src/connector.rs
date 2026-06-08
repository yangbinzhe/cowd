//! Connector runtime contracts.
//!
//! This module describes provider accounts, external capabilities, and resource
//! references without binding them to channel adapters or service SDKs. The
//! cross-plane policy engine remains the execution governance layer.

use std::collections::HashMap;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CrossPlaneRisk, DataClassification};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorPlane {
    Channel,
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
    pub fn channel(provider: impl Into<String>, operation: impl Into<String>) -> Self {
        let provider = provider.into();
        let operation = operation.into();
        Self {
            capability_id: format!("channel.{provider}.{operation}"),
            family: format!("channel.{provider}"),
            provider,
            plane: ConnectorPlane::Channel,
            required_scopes: Vec::new(),
            risk: risk_for_channel_operation(&operation),
            data_classification: DataClassification::Internal,
            supports_dry_run: true,
            supports_commit: true,
            requires_approval: matches!(operation.as_str(), "send_image" | "send_file"),
            input_schema_ref: Some(format!("schema://connector/channel/{operation}/input")),
            output_schema_ref: Some("schema://connector/channel/dispatch/output".to_string()),
        }
    }

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
        self.reference.starts_with("service://") || self.reference.starts_with("channel://")
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
        let channel_capabilities = self
            .capabilities
            .iter()
            .filter(|capability| capability.plane == ConnectorPlane::Channel)
            .count();
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
        ConnectorSummary {
            account_count: self.accounts.len(),
            capability_count: self.capabilities.len(),
            resource_count: self.resources.len(),
            channel_capabilities,
            service_capabilities,
            governance_capabilities,
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
    pub channel_capabilities: usize,
    pub service_capabilities: usize,
    pub governance_capabilities: usize,
    pub degraded: bool,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ResourceDirectory {
    resources: RwLock<HashMap<String, ExternalResourceRef>>,
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

pub trait ServiceConnector {
    fn metadata(&self) -> ServiceConnectorMetadata;
    fn capabilities(&self) -> Vec<CapabilityManifest>;
    fn execute_tool(&self, request: ServiceToolRequest) -> ServiceToolResult;
}

#[derive(Debug, Clone, Default)]
pub struct MockDocsServiceConnector;

impl MockDocsServiceConnector {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl ServiceConnector for MockDocsServiceConnector {
    fn metadata(&self) -> ServiceConnectorMetadata {
        ServiceConnectorMetadata {
            id: "mock.docs".to_string(),
            provider: "mock".to_string(),
            family: "service.mock.docs".to_string(),
            display_name: "Mock Docs".to_string(),
            read_only: true,
        }
    }

    fn capabilities(&self) -> Vec<CapabilityManifest> {
        ["read", "export", "summarize_ref"]
            .into_iter()
            .map(|operation| CapabilityManifest::service("mock.docs", operation))
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
        let resource = ExternalResourceRef::new("mock.docs", "document", &resource_id, &title);
        ServiceToolResult {
            status: "ok".to_string(),
            tool_id,
            resource: Some(resource.clone()),
            output: serde_json::json!({
                "operation": operation,
                "summary": format!("Mock document `{title}` resolved as {}", resource.reference),
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
    capabilities.extend(
        [
            ("feishu", "send_text"),
            ("feishu", "send_image"),
            ("feishu", "send_file"),
            ("wechat-ilink", "qr_login"),
            ("wechat-ilink", "send_text"),
            ("wecom", "send_text"),
            ("wecom", "callback"),
            ("email", "send_email"),
        ]
        .into_iter()
        .map(|(provider, operation)| CapabilityManifest::channel(provider, operation)),
    );
    capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    capabilities
}

fn risk_for_channel_operation(operation: &str) -> CrossPlaneRisk {
    match operation {
        "send_file" | "send_image" | "send_email" => CrossPlaneRisk::Medium,
        "callback" => CrossPlaneRisk::High,
        _ => CrossPlaneRisk::Low,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_account_reports_missing_scopes() {
        let mut account = ProviderAccount::new("feishu", "feishu-main", "app_secret");
        account.scopes = vec!["docx:read".to_string()];
        let required = vec!["docx:read".to_string(), "drive:read".to_string()];

        assert_eq!(account.missing_scopes(&required), vec!["drive:read"]);
    }

    #[test]
    fn capability_manifest_channel_contract_is_stable() {
        let manifest = CapabilityManifest::channel("feishu", "send_file");

        assert_eq!(manifest.capability_id, "channel.feishu.send_file");
        assert_eq!(manifest.family, "channel.feishu");
        assert_eq!(manifest.plane, ConnectorPlane::Channel);
        assert!(manifest.supports_dry_run);
        assert!(manifest.supports_commit);
        assert!(manifest.requires_approval);
    }

    #[test]
    fn external_resource_ref_uses_canonical_service_scheme() {
        let resource = ExternalResourceRef::new("feishu", "docx", "doccn123", "Design");

        assert_eq!(resource.reference, "service://feishu/docx/doccn123");
        assert!(resource.is_canonical());
    }

    #[test]
    fn default_snapshot_exposes_connector_capabilities_without_accounts() {
        let snapshot = ConnectorRegistrySnapshot::empty_with_default_capabilities();
        let summary = snapshot.summary();

        assert_eq!(summary.account_count, 0);
        assert!(summary.capability_count >= 8);
        assert!(snapshot
            .capabilities
            .iter()
            .any(|capability| capability.capability_id == "channel.feishu.send_text"));
        assert!(snapshot
            .capabilities
            .iter()
            .any(|capability| capability.capability_id == "governance.cross_plane.audit"));
    }

    #[test]
    fn mock_docs_service_connector_returns_resource_ref() {
        let connector = MockDocsServiceConnector::new();
        let capabilities = connector.capabilities();
        assert!(capabilities
            .iter()
            .any(
                |capability| capability.capability_id == "service.mock.docs.read"
                    && capability.plane == ConnectorPlane::Service
            ));

        let result = connector.execute_tool(ServiceToolRequest {
            tool_id: "service.mock.docs.read".to_string(),
            resource_id: "doc-1".to_string(),
            title: "Architecture".to_string(),
            input: serde_json::json!({}),
        });

        assert_eq!(result.status, "ok");
        assert_eq!(
            result.resource.unwrap().reference,
            "service://mock.docs/document/doc-1"
        );
    }

    #[test]
    fn resource_directory_upserts_and_searches_refs() {
        let directory = ResourceDirectory::new();
        let resource = ExternalResourceRef::new("mock.docs", "document", "doc-2", "Runtime Plan");
        directory.upsert(resource.clone());

        assert_eq!(directory.get(&resource.reference), Some(resource.clone()));
        assert_eq!(directory.search("runtime", 10), vec![resource.clone()]);
        assert_eq!(directory.list_recent(1), vec![resource]);
    }
}
