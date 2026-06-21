use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

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
    AutomationEndpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceTransport {
    StdioJsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SurfaceLifecycle {
    Builtin,
    OneShot,
    Managed,
}

fn default_lifecycle() -> SurfaceLifecycle {
    SurfaceLifecycle::OneShot
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceHealthSpec {
    #[serde(default = "default_health_mode")]
    pub mode: SurfaceHealthMode,
    #[serde(default = "default_health_interval_ms")]
    pub interval_ms: u64,
}

fn default_health_mode() -> SurfaceHealthMode {
    SurfaceHealthMode::Registry
}

fn default_health_interval_ms() -> u64 {
    30_000
}

impl Default for SurfaceHealthSpec {
    fn default() -> Self {
        Self {
            mode: default_health_mode(),
            interval_ms: default_health_interval_ms(),
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
pub struct SurfaceManifest {
    pub schema: String,
    pub id: String,
    pub name: String,
    pub version: String,
    pub kind: SurfaceKind,
    pub entry: Option<String>,
    #[serde(default = "default_transport")]
    pub transport: SurfaceTransport,
    #[serde(default = "default_lifecycle")]
    pub lifecycle: SurfaceLifecycle,
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

impl SurfaceManifest {
    #[must_use]
    pub fn builtin(id: &str, name: &str, kind: SurfaceKind, capabilities: &[&str]) -> Self {
        Self {
            schema: SURFACE_PROTOCOL.to_string(),
            id: normalize_surface_id(id),
            name: name.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            kind,
            entry: None,
            transport: SurfaceTransport::StdioJsonl,
            lifecycle: SurfaceLifecycle::Builtin,
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
        if self.entry.is_none()
            && !matches!(
                self.kind,
                SurfaceKind::InteractiveSurface | SurfaceKind::WebSurface
            )
        {
            return Err(SurfaceError::InvalidManifest {
                surface: self.id.clone(),
                reason: "external surface requires entry".to_string(),
            });
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
    pub entry: Option<String>,
    pub lifecycle: SurfaceLifecycle,
    pub capabilities: Vec<SurfaceCapability>,
    pub routes: Vec<SurfaceRoute>,
    pub resources: Vec<SurfaceResource>,
    pub health: SurfaceHealthSpec,
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
            status: if manifest.entry.is_some() {
                SurfaceStatus::Discovered
            } else {
                SurfaceStatus::Builtin
            },
            source: source.into(),
            entry: manifest.entry.clone(),
            lifecycle: manifest.lifecycle,
            capabilities: manifest.capability_rows(),
            routes: manifest.routes.clone(),
            resources: manifest.resources.clone(),
            health: manifest.health.clone(),
            diagnostics: Vec::new(),
        }
    }
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
    fn builtin_surfaces_include_tui_and_webui() {
        let surfaces = builtin_surfaces();
        assert!(surfaces.contains_key("tui"));
        assert!(surfaces.contains_key("webui"));
    }

    #[test]
    fn normalizes_legacy_wechat_ids_without_channel_runtime() {
        assert_eq!(normalize_surface_id("wechat_ilink"), "wechat-ilink");
        assert_eq!(normalize_surface_id("WeChat"), "wechat-ilink");
    }

    #[test]
    fn manifest_v2_accepts_routes_resources_and_health() {
        let manifest = serde_json::from_str::<SurfaceManifest>(
            r#"{
                "schema": "cowd.surface.v1",
                "id": "feishu",
                "name": "Feishu Surface",
                "version": "1.0.0",
                "kind": "external-integration",
                "entry": "./cowd-surface-feishu",
                "transport": "stdio-jsonl",
                "lifecycle": "managed",
                "capabilities": ["send_text", "callback"],
                "routes": [
                    {"kind": "callback", "path": "/webhook", "method": "POST", "public": true}
                ],
                "resources": [
                    {"kind": "static", "mount": "/", "dir": "./public", "spa": true}
                ],
                "health": {"mode": "jsonl", "interval_ms": 1000}
            }"#,
        )
        .unwrap();

        manifest.validate().unwrap();
        let descriptor = SurfaceDescriptor::from_manifest(&manifest, "/tmp/surface.json");
        assert_eq!(descriptor.lifecycle, SurfaceLifecycle::Managed);
        assert_eq!(descriptor.routes.len(), 1);
        assert_eq!(descriptor.resources.len(), 1);
        assert_eq!(descriptor.health.mode, SurfaceHealthMode::Jsonl);
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
                "entry": "./bad",
                "resources": [
                    {"kind": "static", "mount": "/assets", "dir": "../secret", "spa": false}
                ]
            }"#,
        )
        .unwrap();

        assert!(manifest.validate().is_err());
    }
}
