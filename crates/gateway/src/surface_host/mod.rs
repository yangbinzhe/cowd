use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use surface::{
    SurfaceDescriptor, SurfaceFrame, SurfaceMessageLedger, SurfaceRuntimeSnapshot,
    SurfaceSupervisorEvent,
};
use tokio::sync::{broadcast, Mutex as AsyncMutex};

mod edge_h2;
mod ingress;
mod invocation;
mod ledger;
mod message_store;
mod monitor;
mod registry;
mod repair;
mod static_assets;
mod supervisor;
mod types;

pub(crate) use ingress::spawn_surface_ingress_dispatcher;
pub(crate) use message_store::SqliteSurfaceMessageStore;
pub(crate) use message_store::{
    SurfaceDeliveryEvent, SurfaceInboxReceipt, SurfaceInboxRecord, SurfaceIngressClaim,
    SurfaceMessageSnapshot, SurfaceOutboxRecord, SurfaceTriggerEventReceipt,
    SurfaceTriggerEventRecord, SurfaceTurnCorrelation,
};
pub(crate) use types::{
    SurfaceDiscoveryFailure, SurfaceDiscoveryReport, SurfaceHostHealth, SurfaceResourceSummary,
    SurfaceRouteSummary, SurfaceStaticFile,
};

use ledger::push_supervisor_event;
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
    messages: Arc<dyn SurfaceMessageLedger>,
    event_tx: broadcast::Sender<SurfaceFrame>,
    monitor_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl SurfaceHost {
    #[cfg(test)]
    pub(crate) fn new(roots: Vec<PathBuf>) -> Self {
        Self::with_configs(roots, BTreeMap::new())
    }

    #[cfg(test)]
    #[allow(
        clippy::expect_used,
        reason = "isolated Surface test fixtures must fail at their local construction boundary"
    )]
    pub(crate) fn with_configs(
        roots: Vec<PathBuf>,
        configs: BTreeMap<String, serde_json::Value>,
    ) -> Self {
        let message_root = roots
            .first()
            .map(|root| root.join(".cowd-edge-messages"))
            .unwrap_or_else(isolated_message_root);
        Self::with_configs_and_message_root(roots, configs, message_root)
    }

    #[cfg(test)]
    #[allow(
        clippy::expect_used,
        reason = "isolated Surface test fixtures must fail at their local construction boundary"
    )]
    pub(crate) fn with_configs_and_message_root(
        roots: Vec<PathBuf>,
        configs: BTreeMap<String, serde_json::Value>,
        message_root: PathBuf,
    ) -> Self {
        Self::with_configs_and_message_store(
            roots,
            configs,
            Arc::new(
                SqliteSurfaceMessageStore::try_new(message_root)
                    .expect("isolated Surface test store"),
            ),
        )
    }

    pub(crate) fn with_configs_and_message_store(
        roots: Vec<PathBuf>,
        configs: BTreeMap<String, serde_json::Value>,
        messages: Arc<dyn SurfaceMessageLedger>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        let host = Self {
            registry: Arc::new(RwLock::new(BTreeMap::new())),
            runtime: Arc::new(RwLock::new(BTreeMap::new())),
            configs: Arc::new(RwLock::new(configs)),
            roots,
            managed: Arc::new(AsyncMutex::new(HashMap::new())),
            ledger: Arc::new(AsyncMutex::new(HashMap::new())),
            messages,
            event_tx,
            monitor_task: Arc::new(Mutex::new(None)),
        };
        host.register_builtin_surfaces();
        host
    }

    pub(crate) fn baseline() -> Result<Self, String> {
        Ok(Self::with_configs_and_message_store(
            Vec::new(),
            BTreeMap::new(),
            Arc::new(SqliteSurfaceMessageStore::in_memory(
                "gateway-surface-baseline",
            )?),
        ))
    }

    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<SurfaceFrame> {
        self.event_tx.subscribe()
    }

    pub(crate) fn message_store_root(&self) -> PathBuf {
        self.messages.diagnostic_root()
    }
}

pub(crate) fn default_surface_roots(config_home: &Path) -> Vec<PathBuf> {
    let install_root = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .map(|root| edge_manifest_roots(&root));
    let mut roots = Vec::new();
    if let Some(mut install_roots) = install_root {
        roots.append(&mut install_roots);
    }
    roots.extend(edge_manifest_roots(config_home));
    roots
}

fn edge_manifest_roots(root: &Path) -> Vec<PathBuf> {
    [
        root.join("surfaces"),
        root.join("connectors").join("message"),
        root.join("connectors").join("source"),
        root.join("connectors").join("automation"),
    ]
    .into_iter()
    .collect()
}

fn cowd_config_home() -> PathBuf {
    std::env::var_os("COWD_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cowd")))
        .unwrap_or_else(|| PathBuf::from(".cowd"))
}

fn isolated_message_root() -> PathBuf {
    std::env::temp_dir()
        .join("cowd-surface-messages")
        .join(uuid::Uuid::new_v4().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use surface::{
        SurfaceFailureKind, SurfaceManifest, SurfaceRuntimeStatus, SurfaceSendRequest,
        SURFACE_MANIFEST_FILE,
    };

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn edge_manifest_roots_include_surfaces_and_connectors() {
        let root = PathBuf::from("/tmp/cowd-edge-root");
        let roots = edge_manifest_roots(&root);
        assert!(roots.contains(&root.join("surfaces")));
        assert!(roots.contains(&root.join("connectors").join("message")));
        assert!(roots.contains(&root.join("connectors").join("source")));
        assert!(roots.contains(&root.join("connectors").join("automation")));
    }

    #[test]
    fn surface_host_empty_roots_message_store_does_not_use_current_directory() {
        let host = SurfaceHost::with_configs(Vec::new(), BTreeMap::new());
        assert_ne!(
            host.message_store_root(),
            Path::new("."),
            "surface message store must not default to the source/current directory"
        );
        assert!(host
            .message_store_root()
            .components()
            .any(|component| component.as_os_str() == "cowd-surface-messages"));
    }

    #[test]
    fn surface_host_composes_message_ledger_through_storage_neutral_contract() {
        let root = std::env::temp_dir().join(format!(
            "cowd-surface-ledger-contract-{}",
            uuid::Uuid::new_v4()
        ));
        let messages: Arc<dyn SurfaceMessageLedger> =
            Arc::new(SqliteSurfaceMessageStore::new(&root));
        let host = SurfaceHost::with_configs_and_message_store(
            Vec::new(),
            BTreeMap::new(),
            messages.clone(),
        );

        assert_eq!(host.message_store_root(), root);
        assert!(messages.list_inbox("fixture").unwrap().is_empty());
        let _ = fs::remove_dir_all(host.message_store_root());
    }

    #[tokio::test]
    async fn reload_manifests_upserts_and_prunes_removed_edges() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-reload-test-{}", uuid::Uuid::new_v4()));
        let surface_dir = root.join("echo");
        fs::create_dir_all(&surface_dir).unwrap();
        let manifest_path = surface_dir.join(SURFACE_MANIFEST_FILE);
        fs::write(
            &manifest_path,
            r#"{
                "schema": "cowd.surface.v1",
                "id": "echo",
                "name": "Echo Surface",
                "version": "1.0.0",
                "kind": "external-integration",
                "runtime": {
                    "kind": "one-shot",
                    "entry": "cowd-edge-echo",
                    "transport": "stdio-jsonl"
                },
                "capabilities": ["send_text"],
                "default_enabled": true
            }"#,
        )
        .unwrap();

        let host = SurfaceHost::new(vec![root.clone()]);
        let first = host.reload_manifests().await;
        assert_eq!(first.discovered, 1);
        assert!(first.removed.is_empty());
        assert_eq!(host.get("echo").unwrap().version, "1.0.0");

        fs::write(
            &manifest_path,
            r#"{
                "schema": "cowd.surface.v1",
                "id": "echo",
                "name": "Echo Surface",
                "version": "2.0.0",
                "kind": "external-integration",
                "runtime": {
                    "kind": "one-shot",
                    "entry": "cowd-edge-echo",
                    "transport": "stdio-jsonl"
                },
                "capabilities": ["send_text", "receive_text"],
                "default_enabled": true
            }"#,
        )
        .unwrap();
        let second = host.reload_manifests().await;
        assert_eq!(second.discovered, 1);
        assert_eq!(host.get("echo").unwrap().version, "2.0.0");

        fs::remove_dir_all(&surface_dir).unwrap();
        let third = host.reload_manifests().await;
        assert_eq!(third.discovered, 0);
        assert_eq!(third.removed, vec!["echo".to_string()]);
        assert!(host.get("echo").is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn discovers_and_invokes_stdio_jsonl_sidecar() {
        let root =
            std::env::temp_dir().join(format!("cowd-edge-host-test-{}", uuid::Uuid::new_v4()));
        let surface_dir = root.join("echo");
        fs::create_dir_all(&surface_dir).unwrap();
        let sidecar = surface_dir.join("cowd-edge-echo");
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
                "runtime": {
                    "kind": "one-shot",
                    "entry": "cowd-edge-echo",
                    "transport": "stdio-jsonl"
                },
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
                idempotency_key: None,
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
            std::env::temp_dir().join(format!("cowd-edge-static-test-{}", uuid::Uuid::new_v4()));
        let surface_dir = root.join("panel");
        let public_dir = surface_dir.join("public");
        fs::create_dir_all(&public_dir).unwrap();
        fs::write(public_dir.join("index.html"), "<!doctype html>panel").unwrap();
        fs::write(public_dir.join("app.js"), "console.log('ok');").unwrap();
        let sidecar = surface_dir.join("cowd-edge-panel");
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
                "runtime": {
                    "kind": "one-shot",
                    "entry": "cowd-edge-panel",
                    "transport": "stdio-jsonl"
                },
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
        let root =
            std::env::temp_dir().join(format!("cowd-edge-callback-test-{}", uuid::Uuid::new_v4()));
        let surface_dir = root.join("hook");
        fs::create_dir_all(&surface_dir).unwrap();
        let sidecar = surface_dir.join("cowd-edge-hook");
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
                "runtime": {
                    "kind": "one-shot",
                    "entry": "cowd-edge-hook",
                    "transport": "stdio-jsonl"
                },
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
                runtime: Some(surface::SurfaceRuntimeSpec::Managed {
                    artifact: "missing".to_string(),
                    driver_profile: "bounded".to_string(),
                    transport: surface::SurfaceTransport::UdsHttp2,
                }),
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
