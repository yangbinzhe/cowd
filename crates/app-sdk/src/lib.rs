//! Cowd application ABI.
//!
//! This crate deliberately contains only transport-neutral, stable contracts.
//! An application can depend on it without gaining access to Gateway, Runtime,
//! authentication credentials, or another application's implementation.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SDK_API_VERSION: u32 = 1;

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
        Ok(())
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppActionDescriptor {
    pub id: String,
    pub title: String,
    pub requires_confirmation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppProfileDescriptor {
    pub id: String,
    pub revision: u64,
    pub capability_digest: String,
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
    async fn read(
        &self,
        context: &InvocationContext,
        intent: HostIntent,
    ) -> Result<HostReceipt, AppHostError>;
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
    #[error("app {app_id} requires SDK API {actual}, host supports {expected}")]
    UnsupportedSdkApi {
        app_id: AppId,
        expected: u32,
        actual: u32,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AppHostError {
    #[error("host port unavailable: {0}")]
    Unavailable(String),
    #[error("host denied request: {0}")]
    Denied(String),
    #[error("host request failed: {0}")]
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

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
