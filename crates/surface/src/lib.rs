// Test assertions intentionally use unwrap/expect/panic; normal library builds remain strict.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

pub mod message;

include!("edge_v2_generated.rs");

/// Current wire protocol for Cowd edge sidecars.
///
/// The crate is still named `surface` because WebUI/TUI are real surfaces and
/// TUI stays in the core repository for now. The manifest, runtime, and JSONL
/// primitives are intentionally edge-compatible: non-UI message/source
/// connectors use the same lifecycle contract while exposing a different
/// business domain.
pub const SURFACE_PROTOCOL: &str = "cowd.surface.v1";
pub const SURFACE_MANIFEST_FILE: &str = "surface.json";

#[derive(Debug, Error)]
pub enum SurfaceError {
    #[error("surface manifest io error at `{path}`: {source}")]
    ManifestIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("surface manifest json error at `{path}`: {source}")]
    ManifestJson {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid surface manifest `{surface}`: {reason}")]
    InvalidManifest { surface: String, reason: String },
    #[error("surface `{0}` is not installed or not running")]
    Unavailable(String),
    #[error("surface invocation failed for `{surface}`: {reason}")]
    Invocation { surface: String, reason: String },
    #[error("surface jsonl frame parse error: {0}")]
    FrameParse(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceKind {
    InteractiveSurface,
    WebSurface,
    ExternalIntegration,
    MessageConnector,
    SourceConnector,
    AutomationEndpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeDomain {
    Surface,
    MessageConnector,
    SourceConnector,
    AutomationConnector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceTransport {
    StdioJsonl,
    UdsHttp2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceLifecycle {
    Builtin,
    OneShot,
    Managed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum SurfaceRuntimeSpec {
    OneShot {
        entry: String,
        #[serde(default = "default_transport")]
        transport: SurfaceTransport,
    },
    Managed {
        artifact: String,
        driver_profile: String,
        #[serde(default = "default_managed_transport")]
        transport: SurfaceTransport,
    },
}

impl SurfaceRuntimeSpec {
    #[must_use]
    pub fn lifecycle(&self) -> SurfaceLifecycle {
        match self {
            Self::OneShot { .. } => SurfaceLifecycle::OneShot,
            Self::Managed { .. } => SurfaceLifecycle::Managed,
        }
    }

    #[must_use]
    pub fn transport(&self) -> SurfaceTransport {
        match self {
            Self::OneShot { transport, .. } | Self::Managed { transport, .. } => *transport,
        }
    }

    #[must_use]
    pub fn entry(&self) -> Option<&str> {
        match self {
            Self::OneShot { entry, .. } => Some(entry),
            Self::Managed { .. } => None,
        }
    }

    #[must_use]
    pub fn managed_artifact(&self) -> Option<(&str, &str)> {
        match self {
            Self::Managed {
                artifact,
                driver_profile,
                ..
            } => Some((artifact, driver_profile)),
            Self::OneShot { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceRouteKind {
    Callback,
    Webhook,
    OAuthRedirect,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceRoute {
    pub kind: SurfaceRouteKind,
    pub path: String,
    #[serde(default = "default_route_method")]
    pub method: String,
    #[serde(default)]
    pub public: bool,
}

fn default_route_method() -> String {
    "POST".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceResourceKind {
    Static,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceResource {
    pub kind: SurfaceResourceKind,
    pub mount: String,
    pub dir: String,
    #[serde(default)]
    pub spa: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceHealthMode {
    Registry,
    Jsonl,
    Http2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceHealthSpec {
    #[serde(default = "default_health_mode")]
    pub mode: SurfaceHealthMode,
    #[serde(default = "default_health_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_health_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub repair: SurfaceRepairPolicy,
}

fn default_health_mode() -> SurfaceHealthMode {
    SurfaceHealthMode::Registry
}

fn default_health_interval_ms() -> u64 {
    30_000
}

fn default_health_timeout_ms() -> u64 {
    5_000
}

impl Default for SurfaceHealthSpec {
    fn default() -> Self {
        Self {
            mode: default_health_mode(),
            interval_ms: default_health_interval_ms(),
            timeout_ms: default_health_timeout_ms(),
            repair: SurfaceRepairPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceRuntimeStatus {
    Builtin,
    Discovered,
    Starting,
    Ready,
    Degraded,
    Restarting,
    Unavailable,
    Disabled,
    Failed,
    CircuitOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceFailureKind {
    ManifestInvalid,
    EntryMissing,
    SpawnFailed,
    ProcessExited,
    HealthTimeout,
    ProtocolError,
    AuthError,
    NetworkError,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceSupervisorAction {
    Start,
    Stop,
    Restart,
    Repair,
    HealthCheck,
    ArchiveDeadLetters,
    PurgeArchivedEvents,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceRepairPolicy {
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_restart_limit")]
    pub restart_limit: u32,
    #[serde(default = "default_restart_window_ms")]
    pub restart_window_ms: u64,
    #[serde(default = "default_backoff_initial_ms")]
    pub backoff_initial_ms: u64,
    #[serde(default = "default_backoff_max_ms")]
    pub backoff_max_ms: u64,
    #[serde(default = "default_circuit_half_open_after_ms")]
    pub circuit_half_open_after_ms: u64,
}

fn default_failure_threshold() -> u32 {
    3
}

fn default_restart_limit() -> u32 {
    3
}

fn default_restart_window_ms() -> u64 {
    600_000
}

fn default_backoff_initial_ms() -> u64 {
    1_000
}

fn default_backoff_max_ms() -> u64 {
    60_000
}

fn default_circuit_half_open_after_ms() -> u64 {
    300_000
}

impl Default for SurfaceRepairPolicy {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            restart_limit: default_restart_limit(),
            restart_window_ms: default_restart_window_ms(),
            backoff_initial_ms: default_backoff_initial_ms(),
            backoff_max_ms: default_backoff_max_ms(),
            circuit_half_open_after_ms: default_circuit_half_open_after_ms(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceRuntimeError {
    pub kind: SurfaceFailureKind,
    pub message: String,
    pub occurred_at: DateTime<Utc>,
}

impl SurfaceRuntimeError {
    #[must_use]
    pub fn new(kind: SurfaceFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            occurred_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceRuntimeSnapshot {
    pub surface: String,
    pub status: SurfaceRuntimeStatus,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_health_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub consecutive_failures: u32,
    pub restart_count: u32,
    pub circuit_open: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<SurfaceRuntimeError>,
    #[serde(default)]
    pub available_actions: Vec<SurfaceSupervisorAction>,
}

impl SurfaceRuntimeSnapshot {
    #[must_use]
    pub fn builtin(surface: impl AsRef<str>) -> Self {
        Self {
            surface: normalize_surface_id(surface.as_ref()),
            status: SurfaceRuntimeStatus::Builtin,
            active: true,
            pid: None,
            started_at: None,
            last_seen_at: Some(Utc::now()),
            last_health_at: Some(Utc::now()),
            latency_ms: Some(0),
            consecutive_failures: 0,
            restart_count: 0,
            circuit_open: false,
            next_retry_at: None,
            last_error: None,
            available_actions: vec![SurfaceSupervisorAction::HealthCheck],
        }
    }

    #[must_use]
    pub fn discovered(surface: impl AsRef<str>, lifecycle: SurfaceLifecycle) -> Self {
        let actions = match lifecycle {
            SurfaceLifecycle::Builtin => vec![SurfaceSupervisorAction::HealthCheck],
            SurfaceLifecycle::OneShot => vec![SurfaceSupervisorAction::HealthCheck],
            SurfaceLifecycle::Managed => vec![
                SurfaceSupervisorAction::Start,
                SurfaceSupervisorAction::HealthCheck,
                SurfaceSupervisorAction::Repair,
            ],
        };
        Self {
            surface: normalize_surface_id(surface.as_ref()),
            status: SurfaceRuntimeStatus::Discovered,
            active: false,
            pid: None,
            started_at: None,
            last_seen_at: None,
            last_health_at: None,
            latency_ms: None,
            consecutive_failures: 0,
            restart_count: 0,
            circuit_open: false,
            next_retry_at: None,
            last_error: None,
            available_actions: actions,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceSupervisorEvent {
    pub surface: String,
    pub status: SurfaceRuntimeStatus,
    pub message: String,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SurfaceRuntimeError>,
}

impl SurfaceSupervisorEvent {
    #[must_use]
    pub fn new(
        surface: impl AsRef<str>,
        status: SurfaceRuntimeStatus,
        message: impl Into<String>,
    ) -> Self {
        Self {
            surface: normalize_surface_id(surface.as_ref()),
            status,
            message: message.into(),
            timestamp: Utc::now(),
            error: None,
        }
    }

    #[must_use]
    pub fn error(
        surface: impl AsRef<str>,
        status: SurfaceRuntimeStatus,
        error: SurfaceRuntimeError,
    ) -> Self {
        Self {
            surface: normalize_surface_id(surface.as_ref()),
            status,
            message: error.message.clone(),
            timestamp: Utc::now(),
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceCapability {
    pub id: String,
    pub capability: String,
}

impl SurfaceCapability {
    #[must_use]
    pub fn new(surface: &str, capability: impl Into<String>) -> Self {
        let capability = capability.into();
        Self {
            id: format!("surface.{surface}.{capability}"),
            capability,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceManifest {
    pub schema: String,
    pub id: String,
    pub name: String,
    pub version: String,
    pub kind: SurfaceKind,
    #[serde(default)]
    pub runtime: Option<SurfaceRuntimeSpec>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub routes: Vec<SurfaceRoute>,
    #[serde(default)]
    pub resources: Vec<SurfaceResource>,
    #[serde(default)]
    pub health: SurfaceHealthSpec,
    #[serde(default)]
    pub config_schema: Value,
    #[serde(default)]
    pub default_enabled: bool,
}

fn default_transport() -> SurfaceTransport {
    SurfaceTransport::StdioJsonl
}

fn default_managed_transport() -> SurfaceTransport {
    SurfaceTransport::UdsHttp2
}

impl SurfaceManifest {
    #[must_use]
    pub fn builtin(id: &str, name: &str, kind: SurfaceKind, capabilities: &[&str]) -> Self {
        Self {
            schema: SURFACE_PROTOCOL.to_string(),
            id: normalize_surface_id(id),
            name: name.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            kind,
            runtime: None,
            capabilities: capabilities
                .iter()
                .map(|item| (*item).to_string())
                .collect(),
            routes: Vec::new(),
            resources: Vec::new(),
            health: SurfaceHealthSpec::default(),
            config_schema: Value::Null,
            default_enabled: true,
        }
    }

    pub fn load(path: &Path) -> Result<Self, SurfaceError> {
        let raw = std::fs::read_to_string(path).map_err(|source| SurfaceError::ManifestIo {
            path: path.to_path_buf(),
            source,
        })?;
        let manifest =
            serde_json::from_str::<Self>(&raw).map_err(|source| SurfaceError::ManifestJson {
                path: path.to_path_buf(),
                source,
            })?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), SurfaceError> {
        if self.schema != SURFACE_PROTOCOL {
            return Err(SurfaceError::InvalidManifest {
                surface: self.id.clone(),
                reason: format!("schema must be `{SURFACE_PROTOCOL}`"),
            });
        }
        if self.id.trim().is_empty() {
            return Err(SurfaceError::InvalidManifest {
                surface: self.id.clone(),
                reason: "id is required".to_string(),
            });
        }
        if self.runtime.is_none()
            && !matches!(
                self.kind,
                SurfaceKind::InteractiveSurface | SurfaceKind::WebSurface
            )
        {
            return Err(SurfaceError::InvalidManifest {
                surface: self.id.clone(),
                reason: "external edge connector requires runtime".to_string(),
            });
        }
        if let Some(runtime) = &self.runtime {
            match runtime {
                SurfaceRuntimeSpec::OneShot { entry, transport } => {
                    validate_one_shot_entry(&self.id, entry)?;
                    if *transport != SurfaceTransport::StdioJsonl {
                        return Err(SurfaceError::InvalidManifest {
                            surface: self.id.clone(),
                            reason: "one-shot runtime requires `stdio-jsonl` transport".to_string(),
                        });
                    }
                }
                SurfaceRuntimeSpec::Managed {
                    artifact,
                    driver_profile,
                    transport,
                } => {
                    validate_runtime_name(&self.id, "runtime.artifact", artifact)?;
                    validate_runtime_name(&self.id, "runtime.driver_profile", driver_profile)?;
                    if *transport != SurfaceTransport::UdsHttp2 {
                        return Err(SurfaceError::InvalidManifest {
                            surface: self.id.clone(),
                            reason: "managed runtime requires `uds-http2` transport".to_string(),
                        });
                    }
                }
            }
        }
        for route in &self.routes {
            validate_surface_path(&self.id, "route.path", &route.path)?;
            if route.method.trim().is_empty() {
                return Err(SurfaceError::InvalidManifest {
                    surface: self.id.clone(),
                    reason: "route.method is required".to_string(),
                });
            }
        }
        for resource in &self.resources {
            validate_surface_path(&self.id, "resource.mount", &resource.mount)?;
            if resource.dir.trim().is_empty() {
                return Err(SurfaceError::InvalidManifest {
                    surface: self.id.clone(),
                    reason: "resource.dir is required".to_string(),
                });
            }
            if resource.dir.split('/').any(|part| part == "..")
                || resource.dir.split('\\').any(|part| part == "..")
            {
                return Err(SurfaceError::InvalidManifest {
                    surface: self.id.clone(),
                    reason: "resource.dir must not contain path traversal".to_string(),
                });
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn capability_rows(&self) -> Vec<SurfaceCapability> {
        self.capabilities
            .iter()
            .map(|capability| SurfaceCapability::new(&self.id, capability.clone()))
            .collect()
    }

    #[must_use]
    pub fn edge_domain(&self) -> EdgeDomain {
        classify_edge_domain(self.kind, &self.capabilities, &self.id)
    }
}

fn validate_runtime_name(surface: &str, field: &str, value: &str) -> Result<(), SurfaceError> {
    if value.trim().is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value == "."
        || value == ".."
    {
        return Err(SurfaceError::InvalidManifest {
            surface: surface.to_string(),
            reason: format!("{field} must be a non-empty trusted name without path separators"),
        });
    }
    Ok(())
}

fn validate_one_shot_entry(surface: &str, value: &str) -> Result<(), SurfaceError> {
    let path = Path::new(value);
    if value.trim().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(SurfaceError::InvalidManifest {
            surface: surface.to_string(),
            reason: "runtime.entry must be a relative path without parent traversal".to_string(),
        });
    }
    Ok(())
}

fn validate_surface_path(surface: &str, field: &str, path: &str) -> Result<(), SurfaceError> {
    if !path.starts_with('/') {
        return Err(SurfaceError::InvalidManifest {
            surface: surface.to_string(),
            reason: format!("{field} must start with `/`"),
        });
    }
    if path.split('/').any(|part| part == "..") || path.split('\\').any(|part| part == "..") {
        return Err(SurfaceError::InvalidManifest {
            surface: surface.to_string(),
            reason: format!("{field} must not contain path traversal"),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceStatus {
    Builtin,
    Discovered,
    Ready,
    Unavailable,
    Disabled,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceDescriptor {
    pub id: String,
    pub name: String,
    pub version: String,
    pub kind: SurfaceKind,
    pub status: SurfaceStatus,
    pub source: String,
    pub runtime: Option<SurfaceRuntimeSpec>,
    pub entry: Option<String>,
    pub transport: Option<SurfaceTransport>,
    pub lifecycle: SurfaceLifecycle,
    pub capabilities: Vec<SurfaceCapability>,
    pub routes: Vec<SurfaceRoute>,
    pub resources: Vec<SurfaceResource>,
    pub health: SurfaceHealthSpec,
    pub edge_domain: EdgeDomain,
    pub diagnostics: Vec<String>,
}

impl SurfaceDescriptor {
    #[must_use]
    pub fn from_manifest(manifest: &SurfaceManifest, source: impl Into<String>) -> Self {
        Self {
            id: normalize_surface_id(&manifest.id),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            kind: manifest.kind,
            status: if manifest.runtime.is_some() {
                SurfaceStatus::Discovered
            } else {
                SurfaceStatus::Builtin
            },
            source: source.into(),
            runtime: manifest.runtime.clone(),
            entry: manifest
                .runtime
                .as_ref()
                .and_then(SurfaceRuntimeSpec::entry)
                .map(ToString::to_string),
            transport: manifest.runtime.as_ref().map(SurfaceRuntimeSpec::transport),
            lifecycle: manifest
                .runtime
                .as_ref()
                .map_or(SurfaceLifecycle::Builtin, SurfaceRuntimeSpec::lifecycle),
            capabilities: manifest.capability_rows(),
            routes: manifest.routes.clone(),
            resources: manifest.resources.clone(),
            health: manifest.health.clone(),
            edge_domain: manifest.edge_domain(),
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn edge_domain(&self) -> EdgeDomain {
        self.edge_domain
    }

    #[must_use]
    pub fn is_executable(&self) -> bool {
        self.runtime.is_some()
    }

    #[must_use]
    pub fn managed_artifact(&self) -> Option<(&str, &str)> {
        self.runtime
            .as_ref()
            .and_then(SurfaceRuntimeSpec::managed_artifact)
    }
}

fn classify_edge_domain(kind: SurfaceKind, capabilities: &[String], id: &str) -> EdgeDomain {
    match kind {
        SurfaceKind::InteractiveSurface | SurfaceKind::WebSurface => EdgeDomain::Surface,
        SurfaceKind::MessageConnector => EdgeDomain::MessageConnector,
        SurfaceKind::SourceConnector => EdgeDomain::SourceConnector,
        SurfaceKind::AutomationEndpoint => EdgeDomain::AutomationConnector,
        SurfaceKind::ExternalIntegration => classify_external_integration(capabilities, id),
    }
}

fn classify_external_integration(capabilities: &[String], id: &str) -> EdgeDomain {
    if capabilities
        .iter()
        .any(|capability| capability.starts_with("source."))
        || id.contains("bitable")
        || id.contains("base")
    {
        return EdgeDomain::SourceConnector;
    }
    if capabilities.iter().any(|capability| {
        capability.starts_with("message.")
            || matches!(
                capability.as_str(),
                "ingress" | "egress" | "delivery" | "callback" | "processing_lifecycle"
            )
    }) {
        return EdgeDomain::MessageConnector;
    }
    EdgeDomain::AutomationConnector
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceRegistrySnapshot {
    pub protocol: String,
    pub generated_at: DateTime<Utc>,
    pub surfaces: Vec<SurfaceDescriptor>,
}

impl SurfaceRegistrySnapshot {
    #[must_use]
    pub fn new(surfaces: Vec<SurfaceDescriptor>) -> Self {
        Self {
            protocol: SURFACE_PROTOCOL.to_string(),
            generated_at: Utc::now(),
            surfaces,
        }
    }

    #[must_use]
    pub fn has_surface(&self, id: &str) -> bool {
        let id = normalize_surface_id(id);
        self.surfaces.iter().any(|surface| surface.id == id)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SurfaceFrame {
    Handshake {
        id: String,
        protocol: String,
        gateway_version: String,
    },
    HandshakeOk {
        id: String,
        surface_id: String,
        capabilities: Vec<String>,
    },
    Configure {
        id: String,
        surface: String,
        config: Value,
    },
    Connect {
        id: String,
        surface: String,
    },
    Disconnect {
        id: String,
        surface: String,
    },
    Send {
        id: String,
        surface: String,
        recipient: String,
        thread: Option<String>,
        text: String,
        metadata: Value,
    },
    Action {
        id: String,
        surface: String,
        action: String,
        payload: Value,
    },
    Health {
        id: String,
        surface: Option<String>,
    },
    Ok {
        id: String,
        payload: Value,
    },
    Error {
        id: Option<String>,
        code: String,
        message: String,
    },
    Event {
        surface: String,
        event: String,
        payload: Value,
    },
}

impl SurfaceFrame {
    #[must_use]
    pub fn new_id() -> String {
        format!("surface-frame-{}", Uuid::new_v4())
    }

    pub fn encode_jsonl(&self) -> Result<String, SurfaceError> {
        Ok(format!("{}\n", serde_json::to_string(self)?))
    }

    pub fn decode_jsonl(line: &str) -> Result<Self, SurfaceError> {
        Ok(serde_json::from_str(line.trim_end())?)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceSendRequest {
    pub surface: String,
    pub recipient: String,
    pub thread: Option<String>,
    pub text: String,
    /// Stable caller-owned key. A sidecar that performs external effects must
    /// return the original provider receipt when this key is replayed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceActionRequest {
    pub surface: String,
    pub action: String,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SurfaceOperationResult {
    pub kind: String,
    pub surface: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SurfaceOperationError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceOperationError {
    pub code: String,
    pub message: String,
}

impl SurfaceOperationResult {
    #[must_use]
    pub fn unavailable(surface: &str) -> Self {
        let surface = normalize_surface_id(surface);
        Self {
            kind: "surface.result".to_string(),
            surface: surface.clone(),
            status: "unavailable".to_string(),
            message_id: None,
            payload: None,
            error: Some(SurfaceOperationError {
                code: "surface_unavailable".to_string(),
                message: format!("surface `{surface}` is not installed or not running"),
            }),
        }
    }

    #[must_use]
    pub fn ok(surface: &str, payload: Value) -> Self {
        let surface = normalize_surface_id(surface);
        let status = payload
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("ok")
            .to_string();
        let message_id = payload
            .get("message_id")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        Self {
            kind: "surface.result".to_string(),
            surface,
            status,
            message_id,
            payload: Some(payload),
            error: None,
        }
    }

    #[must_use]
    pub fn error(surface: &str, code: impl Into<String>, message: impl Into<String>) -> Self {
        let surface = normalize_surface_id(surface);
        Self {
            kind: "surface.result".to_string(),
            surface,
            status: "error".to_string(),
            message_id: None,
            payload: None,
            error: Some(SurfaceOperationError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

#[must_use]
pub fn normalize_surface_id(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "lark" => "feishu".to_string(),
        "wechat" | "wechat_ilink" => "wechat-ilink".to_string(),
        other => other.replace('_', "-"),
    }
}

#[must_use]
pub fn builtin_surfaces() -> BTreeMap<String, SurfaceManifest> {
    [
        SurfaceManifest::builtin(
            "tui",
            "Terminal UI",
            SurfaceKind::InteractiveSurface,
            &["ingress", "delivery", "subscribe", "health"],
        ),
        SurfaceManifest::builtin(
            "webui",
            "Web UI",
            SurfaceKind::WebSurface,
            &["ingress", "delivery", "subscribe", "health"],
        ),
    ]
    .into_iter()
    .map(|manifest| (manifest.id.clone(), manifest))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonl_frame_round_trips() {
        let frame = SurfaceFrame::Send {
            id: "1".to_string(),
            surface: "feishu".to_string(),
            recipient: "oc_xxx".to_string(),
            thread: None,
            text: "hello".to_string(),
            metadata: Value::Null,
        };
        let encoded = frame.encode_jsonl().unwrap();
        assert!(encoded.ends_with('\n'));
        assert_eq!(SurfaceFrame::decode_jsonl(&encoded).unwrap(), frame);
    }

    #[test]
    fn edge_v2_contract_round_trips_bootstrap_and_event_ack() {
        let request = EdgeBootstrapRequest {
            protocol: EDGE_PROTOCOL_V2.to_string(),
            gateway_version: "0.9.550".to_string(),
            surface_id: "feishu".to_string(),
            driver_profile: "feishu-message".to_string(),
            capabilities: vec!["message.health".to_string()],
        };
        let encoded = serde_json::to_vec(&request).unwrap();
        assert_eq!(
            serde_json::from_slice::<EdgeBootstrapRequest>(&encoded).unwrap(),
            request
        );
        let ack = EdgeEventAck { sequence: 42 };
        assert_eq!(
            serde_json::from_str::<EdgeEventAck>(&serde_json::to_string(&ack).unwrap()).unwrap(),
            ack
        );
    }

    #[test]
    fn builtin_surfaces_include_tui_and_webui() {
        let surfaces = builtin_surfaces();
        assert!(surfaces.contains_key("tui"));
        assert!(surfaces.contains_key("webui"));
        assert_eq!(surfaces["tui"].edge_domain(), EdgeDomain::Surface);
        assert_eq!(surfaces["webui"].edge_domain(), EdgeDomain::Surface);
    }

    #[test]
    fn normalizes_legacy_wechat_ids_without_channel_runtime() {
        assert_eq!(normalize_surface_id("wechat_ilink"), "wechat-ilink");
        assert_eq!(normalize_surface_id("WeChat"), "wechat-ilink");
        assert_eq!(normalize_surface_id("lark"), "feishu");
    }

    #[test]
    fn manifest_v2_accepts_routes_resources_and_health() {
        let manifest = serde_json::from_str::<SurfaceManifest>(
            r#"{
                "schema": "cowd.surface.v1",
                "id": "feishu",
                "name": "Feishu Message Connector",
                "version": "1.0.0",
                "kind": "message-connector",
                "runtime": {
                    "kind": "managed",
                    "artifact": "cowd-edge-open-platform-message",
                    "driver_profile": "feishu-message",
                    "transport": "uds-http2"
                },
                "capabilities": ["message.send_text", "message.callback"],
                "routes": [
                    {"kind": "callback", "path": "/webhook", "method": "POST", "public": true}
                ],
                "resources": [
                    {"kind": "static", "mount": "/", "dir": "./public", "spa": true}
                ],
                "health": {"mode": "http2", "interval_ms": 1000}
            }"#,
        )
        .unwrap();

        manifest.validate().unwrap();
        let descriptor = SurfaceDescriptor::from_manifest(&manifest, "/tmp/surface.json");
        assert_eq!(descriptor.lifecycle, SurfaceLifecycle::Managed);
        assert_eq!(descriptor.edge_domain(), EdgeDomain::MessageConnector);
        assert_eq!(descriptor.routes.len(), 1);
        assert_eq!(descriptor.resources.len(), 1);
        assert_eq!(descriptor.health.mode, SurfaceHealthMode::Http2);
    }

    #[test]
    fn edge_domain_distinguishes_source_connectors() {
        let manifest = serde_json::from_str::<SurfaceManifest>(
            r#"{
                "schema": "cowd.surface.v1",
                "id": "feishu-bitable",
                "name": "Feishu Bitable Source Connector",
                "version": "1.0.0",
                "kind": "source-connector",
                "runtime": {
                    "kind": "managed",
                    "artifact": "cowd-edge-bitable-source",
                    "driver_profile": "feishu-bitable"
                },
                "capabilities": ["source.schema_discovery", "source.snapshot", "source.health"]
            }"#,
        )
        .unwrap();

        assert_eq!(manifest.edge_domain(), EdgeDomain::SourceConnector);
        let descriptor = SurfaceDescriptor::from_manifest(&manifest, "/tmp/source.json");
        assert_eq!(descriptor.edge_domain(), EdgeDomain::SourceConnector);
    }

    #[test]
    fn health_spec_defaults_bound_repair_retries() {
        let spec = SurfaceHealthSpec::default();
        assert_eq!(spec.timeout_ms, 5_000);
        assert_eq!(spec.repair.failure_threshold, 3);
        assert_eq!(spec.repair.restart_limit, 3);
        assert_eq!(spec.repair.backoff_max_ms, 60_000);
    }

    #[test]
    fn runtime_snapshot_round_trips_with_circuit_state() {
        let mut snapshot = SurfaceRuntimeSnapshot::discovered("feishu", SurfaceLifecycle::Managed);
        snapshot.status = SurfaceRuntimeStatus::CircuitOpen;
        snapshot.circuit_open = true;
        snapshot.last_error = Some(SurfaceRuntimeError::new(
            SurfaceFailureKind::HealthTimeout,
            "health timeout",
        ));

        let encoded = serde_json::to_string(&snapshot).unwrap();
        let decoded: SurfaceRuntimeSnapshot = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.surface, "feishu");
        assert_eq!(decoded.status, SurfaceRuntimeStatus::CircuitOpen);
        assert!(decoded.circuit_open);
        assert_eq!(
            decoded.last_error.unwrap().kind,
            SurfaceFailureKind::HealthTimeout
        );
    }

    #[test]
    fn manifest_v2_rejects_unsafe_resource_paths() {
        let manifest = serde_json::from_str::<SurfaceManifest>(
            r#"{
                "schema": "cowd.surface.v1",
                "id": "bad",
                "name": "Bad Surface",
                "version": "1.0.0",
                "kind": "external-integration",
                "runtime": {
                    "kind": "one-shot",
                    "entry": "bad",
                    "transport": "stdio-jsonl"
                },
                "resources": [
                    {"kind": "static", "mount": "/assets", "dir": "../secret", "spa": false}
                ]
            }"#,
        )
        .unwrap();

        assert!(manifest.validate().is_err());
    }

    #[test]
    fn managed_manifest_rejects_legacy_runtime_fields_and_transport_mismatch() {
        let legacy = r#"{
            "schema":"cowd.surface.v1","id":"legacy","name":"Legacy","version":"1.0.0",
            "kind":"message-connector","entry":"old-sidecar","lifecycle":"managed",
            "transport":"stdio-jsonl","capabilities":[]
        }"#;
        assert!(serde_json::from_str::<SurfaceManifest>(legacy).is_err());

        let mismatched = serde_json::from_str::<SurfaceManifest>(
            r#"{
                "schema":"cowd.surface.v1","id":"bad","name":"Bad","version":"1.0.0",
                "kind":"message-connector","runtime":{"kind":"managed",
                "artifact":"cowd-edge-bad","driver_profile":"bad","transport":"stdio-jsonl"},
                "capabilities":[]
            }"#,
        )
        .unwrap();
        assert!(mismatched.validate().is_err());
    }

    #[test]
    fn managed_manifest_rejects_untrusted_artifact_and_profile_names() {
        for (artifact, profile) in [("../edge", "safe"), ("cowd-edge-safe", "../profile")] {
            let manifest = SurfaceManifest {
                schema: SURFACE_PROTOCOL.to_string(),
                id: "bad".to_string(),
                name: "Bad".to_string(),
                version: "1.0.0".to_string(),
                kind: SurfaceKind::MessageConnector,
                runtime: Some(SurfaceRuntimeSpec::Managed {
                    artifact: artifact.to_string(),
                    driver_profile: profile.to_string(),
                    transport: SurfaceTransport::UdsHttp2,
                }),
                capabilities: Vec::new(),
                routes: Vec::new(),
                resources: Vec::new(),
                health: SurfaceHealthSpec::default(),
                config_schema: Value::Null,
                default_enabled: false,
            };
            assert!(manifest.validate().is_err());
        }
    }
}
