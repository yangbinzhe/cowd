use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use surface::{SurfaceDescriptor, SurfaceFrame, SurfaceRuntimeSnapshot, SurfaceSupervisorEvent};
use tokio::sync::{broadcast, Mutex as AsyncMutex};

mod ingress;
mod invocation;
mod ledger;
mod monitor;
mod registry;
mod repair;
mod static_assets;
mod supervisor;
mod types;

pub(crate) use ingress::spawn_surface_ingress_dispatcher;
pub(crate) use types::{
    SurfaceDiscoveryFailure, SurfaceDiscoveryReport, SurfaceHostHealth, SurfaceResourceSummary,
    SurfaceRouteSummary, SurfaceStaticFile,
};

use invocation::frame_id;
use ledger::{mark_surface_seen, push_supervisor_event};
use repair::{classify_surface_error, managed_actions};
use static_assets::normalize_request_path;
use types::ManagedSurfaceProcess;

#[derive(Debug, Clone)]
pub(crate) struct SurfaceHost {
    registry: Arc<RwLock<BTreeMap<String, SurfaceDescriptor>>>,
    runtime: Arc<RwLock<BTreeMap<String, SurfaceRuntimeSnapshot>>>,
    configs: Arc<RwLock<BTreeMap<String, serde_json::Value>>>,
    roots: Vec<PathBuf>,
    managed: Arc<AsyncMutex<HashMap<String, Arc<ManagedSurfaceProcess>>>>,
    ledger: Arc<AsyncMutex<HashMap<String, VecDeque<SurfaceSupervisorEvent>>>>,
    event_tx: broadcast::Sender<SurfaceFrame>,
    monitor_started: Arc<RwLock<bool>>,
}

impl SurfaceHost {
    pub(crate) fn new(roots: Vec<PathBuf>) -> Self {
        Self::with_configs(roots, BTreeMap::new())
    }

    pub(crate) fn with_configs(
        roots: Vec<PathBuf>,
        configs: BTreeMap<String, serde_json::Value>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        let host = Self {
            registry: Arc::new(RwLock::new(BTreeMap::new())),
            runtime: Arc::new(RwLock::new(BTreeMap::new())),
            configs: Arc::new(RwLock::new(configs)),
            roots,
            managed: Arc::new(AsyncMutex::new(HashMap::new())),
            ledger: Arc::new(AsyncMutex::new(HashMap::new())),
            event_tx,
            monitor_started: Arc::new(RwLock::new(false)),
        };
        host.register_builtin_surfaces();
        host
    }

    pub(crate) fn default_for(config_home: &Path) -> Self {
        Self::default_for_with_configs(config_home, BTreeMap::new())
    }

    pub(crate) fn default_for_with_configs(
        config_home: &Path,
        configs: BTreeMap<String, serde_json::Value>,
    ) -> Self {
        let install_root = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(Path::to_path_buf))
            .map(|root| root.join("surfaces"));
        let mut roots = Vec::new();
        if let Some(root) = install_root {
            roots.push(root);
        }
        roots.push(config_home.join("surfaces"));
        Self::with_configs(roots, configs)
    }

    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<SurfaceFrame> {
        self.event_tx.subscribe()
    }
}

impl Default for SurfaceHost {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use surface::{
        SurfaceFailureKind, SurfaceFrame, SurfaceLifecycle, SurfaceManifest, SurfaceRuntimeStatus,
        SurfaceSendRequest, SURFACE_MANIFEST_FILE,
    };

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    #[tokio::test]
    async fn discovers_and_invokes_stdio_jsonl_sidecar() {
        let root =
            std::env::temp_dir().join(format!("cowd-surface-host-test-{}", uuid::Uuid::new_v4()));
        let surface_dir = root.join("echo");
        fs::create_dir_all(&surface_dir).unwrap();
        let sidecar = surface_dir.join("cowd-surface-echo");
        fs::write(
            &sidecar,
            "#!/usr/bin/env sh\nread _line\nprintf '%s\\n' '{\"type\":\"ok\",\"id\":\"reply\",\"payload\":{\"status\":\"sent\",\"message_id\":\"m-1\"}}'\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&sidecar).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&sidecar, permissions).unwrap();
        fs::write(
            surface_dir.join(SURFACE_MANIFEST_FILE),
            r#"{
                "schema": "cowd.surface.v1",
                "id": "echo",
                "name": "Echo Surface",
                "version": "1.0.0",
                "kind": "external-integration",
                "entry": "./cowd-surface-echo",
                "transport": "stdio-jsonl",
                "capabilities": ["send_text"],
                "default_enabled": true
            }"#,
        )
        .unwrap();

        let host = SurfaceHost::new(vec![root.clone()]);
        let report = host.discover();
        assert_eq!(report.discovered, 1);
        assert!(host.has_external_surface("echo"));

        let result = host
            .send(SurfaceSendRequest {
                surface: "echo".to_string(),
                recipient: "room-1".to_string(),
                thread: None,
                text: "hello".to_string(),
                metadata: serde_json::Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(result.status, "sent");
        assert_eq!(result.message_id.as_deref(), Some("m-1"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolves_static_resources_and_rejects_traversal() {
        let root =
            std::env::temp_dir().join(format!("cowd-surface-static-test-{}", uuid::Uuid::new_v4()));
        let surface_dir = root.join("panel");
        let public_dir = surface_dir.join("public");
        fs::create_dir_all(&public_dir).unwrap();
        fs::write(public_dir.join("index.html"), "<!doctype html>panel").unwrap();
        fs::write(public_dir.join("app.js"), "console.log('ok');").unwrap();
        let sidecar = surface_dir.join("cowd-surface-panel");
        fs::write(
            &sidecar,
            "#!/usr/bin/env sh\nread _line\nprintf '%s\\n' '{\"type\":\"ok\",\"id\":\"reply\",\"payload\":{\"status\":\"ok\"}}'\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&sidecar).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&sidecar, permissions).unwrap();
        fs::write(
            surface_dir.join(SURFACE_MANIFEST_FILE),
            r#"{
                "schema": "cowd.surface.v1",
                "id": "panel",
                "name": "Panel Surface",
                "version": "1.0.0",
                "kind": "external-integration",
                "entry": "./cowd-surface-panel",
                "resources": [
                    {"kind": "static", "mount": "/", "dir": "./public", "spa": true}
                ]
            }"#,
        )
        .unwrap();

        let host = SurfaceHost::new(vec![root.clone()]);
        assert_eq!(host.discover().discovered, 1);
        let app = host.resolve_static("panel", "/app.js").unwrap().unwrap();
        assert_eq!(
            app.file_path.file_name().and_then(|name| name.to_str()),
            Some("app.js")
        );
        let fallback = host
            .resolve_static("panel", "/missing/route")
            .unwrap()
            .unwrap();
        assert!(fallback.spa_fallback);
        assert_eq!(
            fallback
                .file_path
                .file_name()
                .and_then(|name| name.to_str()),
            Some("index.html")
        );
        assert!(host
            .resolve_static("panel", "/../secret")
            .unwrap()
            .is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn callback_routes_dispatch_to_surface_action() {
        let root = std::env::temp_dir().join(format!(
            "cowd-surface-callback-test-{}",
            uuid::Uuid::new_v4()
        ));
        let surface_dir = root.join("hook");
        fs::create_dir_all(&surface_dir).unwrap();
        let sidecar = surface_dir.join("cowd-surface-hook");
        fs::write(
            &sidecar,
            "#!/usr/bin/env sh\nread _line\nprintf '%s\\n' '{\"type\":\"ok\",\"id\":\"reply\",\"payload\":{\"status\":\"ok\",\"message_id\":\"cb-1\"}}'\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&sidecar).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&sidecar, permissions).unwrap();
        fs::write(
            surface_dir.join(SURFACE_MANIFEST_FILE),
            r#"{
                "schema": "cowd.surface.v1",
                "id": "hook",
                "name": "Hook Surface",
                "version": "1.0.0",
                "kind": "external-integration",
                "entry": "./cowd-surface-hook",
                "routes": [
                    {"kind": "callback", "path": "/webhook", "method": "POST", "public": true}
                ]
            }"#,
        )
        .unwrap();

        let host = SurfaceHost::new(vec![root.clone()]);
        assert_eq!(host.discover().discovered, 1);
        let result = host
            .callback(
                "hook",
                "/webhook",
                "POST",
                serde_json::json!({"hello": "world"}),
            )
            .await
            .unwrap();
        assert_eq!(result.status, "ok");
        assert_eq!(result.message_id.as_deref(), Some("cb-1"));
        let missing = host
            .callback("hook", "/unknown", "POST", serde_json::Value::Null)
            .await
            .unwrap();
        assert_eq!(missing.status, "error");

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_sidecar_reuses_process_and_collects_events() {
        let root = std::env::temp_dir().join(format!(
            "cowd-surface-managed-test-{}",
            uuid::Uuid::new_v4()
        ));
        let surface_dir = root.join("managed");
        fs::create_dir_all(&surface_dir).unwrap();
        let sidecar = surface_dir.join("cowd-surface-managed");
        fs::write(
            &sidecar,
            r#"#!/usr/bin/env sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '%s\n' '{"type":"event","surface":"managed","event":"tick","payload":{"status":"seen"}}'
  printf '%s\n' "{\"type\":\"ok\",\"id\":\"$id\",\"payload\":{\"status\":\"sent\",\"message_id\":\"managed-1\"}}"
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&sidecar).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&sidecar, permissions).unwrap();
        fs::write(
            surface_dir.join(SURFACE_MANIFEST_FILE),
            r#"{
                "schema": "cowd.surface.v1",
                "id": "managed",
                "name": "Managed Surface",
                "version": "1.0.0",
                "kind": "external-integration",
                "entry": "./cowd-surface-managed",
                "lifecycle": "managed",
                "capabilities": ["send_text", "inbound"]
            }"#,
        )
        .unwrap();

        let host = SurfaceHost::new(vec![root.clone()]);
        assert_eq!(host.discover().discovered, 1);
        let result = host
            .send(SurfaceSendRequest {
                surface: "managed".to_string(),
                recipient: "room-1".to_string(),
                thread: None,
                text: "hello".to_string(),
                metadata: serde_json::Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(result.status, "sent");
        assert_eq!(result.message_id.as_deref(), Some("managed-1"));
        let events = host.events("managed").await;
        assert!(events
            .iter()
            .any(|event| matches!(event, SurfaceFrame::Event { event, .. } if event == "tick")));

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn repair_policy_opens_circuit_after_bounded_restarts() {
        let host = SurfaceHost::new(Vec::new());
        let mut descriptor = SurfaceDescriptor::from_manifest(
            &SurfaceManifest {
                schema: surface::SURFACE_PROTOCOL.to_string(),
                id: "bounded".to_string(),
                name: "Bounded Surface".to_string(),
                version: "1.0.0".to_string(),
                kind: surface::SurfaceKind::ExternalIntegration,
                entry: Some("./missing".to_string()),
                transport: surface::SurfaceTransport::StdioJsonl,
                lifecycle: SurfaceLifecycle::Managed,
                capabilities: vec!["health".to_string()],
                routes: Vec::new(),
                resources: Vec::new(),
                health: surface::SurfaceHealthSpec {
                    mode: surface::SurfaceHealthMode::Jsonl,
                    interval_ms: 10,
                    timeout_ms: 10,
                    repair: surface::SurfaceRepairPolicy {
                        failure_threshold: 1,
                        restart_limit: 1,
                        restart_window_ms: 10_000,
                        backoff_initial_ms: 1,
                        backoff_max_ms: 1,
                        circuit_half_open_after_ms: 10_000,
                    },
                },
                config_schema: serde_json::Value::Null,
                default_enabled: true,
            },
            "bounded/surface.json",
        );
        descriptor.id = "bounded".to_string();

        let first = host
            .record_surface_failure(
                descriptor.clone(),
                SurfaceFailureKind::HealthTimeout,
                "first timeout",
            )
            .await;
        assert_eq!(first.status, SurfaceRuntimeStatus::Restarting);
        assert_eq!(first.restart_count, 1);
        assert!(!first.circuit_open);

        let second = host
            .record_surface_failure(
                descriptor,
                SurfaceFailureKind::HealthTimeout,
                "second timeout",
            )
            .await;
        assert_eq!(second.status, SurfaceRuntimeStatus::CircuitOpen);
        assert!(second.circuit_open);
        assert!(second.next_retry_at.is_some());
    }
}
