use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{BufRead, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;
use surface::{
    builtin_surfaces, normalize_surface_id, SurfaceActionRequest, SurfaceDescriptor, SurfaceError,
    SurfaceFailureKind, SurfaceFrame, SurfaceLifecycle, SurfaceManifest, SurfaceOperationResult,
    SurfaceRegistrySnapshot, SurfaceRepairPolicy, SurfaceResource, SurfaceResourceKind,
    SurfaceRoute, SurfaceRuntimeError, SurfaceRuntimeSnapshot, SurfaceRuntimeStatus,
    SurfaceSendRequest, SurfaceSupervisorAction, SurfaceSupervisorEvent, SURFACE_MANIFEST_FILE,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command as TokioCommand};
use tokio::sync::{oneshot, Mutex as AsyncMutex};

#[derive(Debug, Clone)]
pub(crate) struct SurfaceHost {
    registry: Arc<RwLock<BTreeMap<String, SurfaceDescriptor>>>,
    runtime: Arc<RwLock<BTreeMap<String, SurfaceRuntimeSnapshot>>>,
    roots: Vec<PathBuf>,
    managed: Arc<AsyncMutex<HashMap<String, Arc<ManagedSurfaceProcess>>>>,
    ledger: Arc<AsyncMutex<HashMap<String, VecDeque<SurfaceSupervisorEvent>>>>,
    monitor_started: Arc<RwLock<bool>>,
}

#[derive(Debug)]
struct ManagedSurfaceProcess {
    pid: Option<u32>,
    started_at: DateTime<Utc>,
    stdin: AsyncMutex<ChildStdin>,
    pending: Arc<AsyncMutex<HashMap<String, oneshot::Sender<SurfaceFrame>>>>,
    events: Arc<AsyncMutex<VecDeque<SurfaceFrame>>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceDiscoveryReport {
    pub(crate) roots: Vec<String>,
    pub(crate) discovered: usize,
    pub(crate) failures: Vec<SurfaceDiscoveryFailure>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceDiscoveryFailure {
    pub(crate) path: String,
    pub(crate) error: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceHostHealth {
    pub(crate) status: String,
    pub(crate) surface_count: usize,
    pub(crate) external_surface_count: usize,
    pub(crate) route_count: usize,
    pub(crate) resource_count: usize,
    pub(crate) ready_count: usize,
    pub(crate) degraded_count: usize,
    pub(crate) failed_count: usize,
    pub(crate) circuit_open_count: usize,
    pub(crate) roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceStaticFile {
    pub(crate) surface: String,
    pub(crate) mount: String,
    pub(crate) requested_path: String,
    pub(crate) file_path: PathBuf,
    pub(crate) spa_fallback: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceRouteSummary {
    pub(crate) surface: String,
    pub(crate) routes: Vec<SurfaceRoute>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceResourceSummary {
    pub(crate) surface: String,
    pub(crate) resources: Vec<SurfaceResource>,
}

impl SurfaceHost {
    pub(crate) fn new(roots: Vec<PathBuf>) -> Self {
        let host = Self {
            registry: Arc::new(RwLock::new(BTreeMap::new())),
            runtime: Arc::new(RwLock::new(BTreeMap::new())),
            roots,
            managed: Arc::new(AsyncMutex::new(HashMap::new())),
            ledger: Arc::new(AsyncMutex::new(HashMap::new())),
            monitor_started: Arc::new(RwLock::new(false)),
        };
        host.register_builtin_surfaces();
        host
    }

    pub(crate) fn default_for(config_home: &Path) -> Self {
        let install_root = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(Path::to_path_buf))
            .map(|root| root.join("surfaces"));
        let mut roots = Vec::new();
        if let Some(root) = install_root {
            roots.push(root);
        }
        roots.push(config_home.join("surfaces"));
        Self::new(roots)
    }

    pub(crate) fn register_builtin_surfaces(&self) {
        let mut registry = self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut runtime = self
            .runtime
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for manifest in builtin_surfaces().into_values() {
            let descriptor = SurfaceDescriptor::from_manifest(&manifest, "builtin");
            runtime.insert(
                descriptor.id.clone(),
                SurfaceRuntimeSnapshot::builtin(&descriptor.id),
            );
            registry.insert(descriptor.id.clone(), descriptor);
        }
    }

    pub(crate) fn register_webui_static_resource(&self, dir: &Path) {
        let mut registry = self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(webui) = registry.get_mut("webui") {
            webui.resources.retain(|resource| resource.mount != "/");
            webui.resources.push(SurfaceResource {
                kind: SurfaceResourceKind::Static,
                mount: "/".to_string(),
                dir: dir.display().to_string(),
                spa: true,
            });
            webui
                .diagnostics
                .push("webui static assets registered as builtin surface resource".to_string());
        }
    }

    pub(crate) fn discover(&self) -> SurfaceDiscoveryReport {
        let mut failures = Vec::new();
        let mut discovered = 0usize;
        for root in &self.roots {
            if !root.is_dir() {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(root) else {
                failures.push(SurfaceDiscoveryFailure {
                    path: root.display().to_string(),
                    error: "failed to read surface root".to_string(),
                });
                continue;
            };
            for entry in entries.flatten() {
                let manifest_path = entry.path().join(SURFACE_MANIFEST_FILE);
                if !manifest_path.is_file() {
                    continue;
                }
                match SurfaceManifest::load(&manifest_path) {
                    Ok(manifest) => {
                        let mut descriptor = SurfaceDescriptor::from_manifest(
                            &manifest,
                            manifest_path.display().to_string(),
                        );
                        descriptor.diagnostics.push(
                            "surface discovered; sidecar launch is controlled by gateway"
                                .to_string(),
                        );
                        self.runtime
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .entry(descriptor.id.clone())
                            .or_insert_with(|| {
                                SurfaceRuntimeSnapshot::discovered(
                                    &descriptor.id,
                                    descriptor.lifecycle,
                                )
                            });
                        self.registry
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(descriptor.id.clone(), descriptor);
                        discovered += 1;
                    }
                    Err(error) => failures.push(SurfaceDiscoveryFailure {
                        path: manifest_path.display().to_string(),
                        error: error.to_string(),
                    }),
                }
            }
        }
        SurfaceDiscoveryReport {
            roots: self
                .roots
                .iter()
                .map(|root| root.display().to_string())
                .collect(),
            discovered,
            failures,
        }
    }

    pub(crate) fn snapshot(&self) -> SurfaceRegistrySnapshot {
        let mut surfaces = self
            .registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        surfaces.sort_by(|left, right| left.id.cmp(&right.id));
        SurfaceRegistrySnapshot::new(surfaces)
    }

    pub(crate) fn runtime_snapshot(&self, id: &str) -> Option<SurfaceRuntimeSnapshot> {
        self.runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&normalize_surface_id(id))
            .cloned()
    }

    pub(crate) fn runtime_snapshots(&self) -> Vec<SurfaceRuntimeSnapshot> {
        let mut snapshots = self
            .runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.surface.cmp(&right.surface));
        snapshots
    }

    pub(crate) fn health(&self) -> SurfaceHostHealth {
        let snapshot = self.snapshot();
        let runtime = self.runtime_snapshots();
        let external_surface_count = snapshot
            .surfaces
            .iter()
            .filter(|surface| surface.entry.is_some())
            .count();
        let route_count = snapshot
            .surfaces
            .iter()
            .map(|surface| surface.routes.len())
            .sum();
        let resource_count = snapshot
            .surfaces
            .iter()
            .map(|surface| surface.resources.len())
            .sum();
        let ready_count = runtime
            .iter()
            .filter(|surface| {
                matches!(
                    surface.status,
                    SurfaceRuntimeStatus::Ready | SurfaceRuntimeStatus::Builtin
                )
            })
            .count();
        let degraded_count = runtime
            .iter()
            .filter(|surface| surface.status == SurfaceRuntimeStatus::Degraded)
            .count();
        let failed_count = runtime
            .iter()
            .filter(|surface| {
                matches!(
                    surface.status,
                    SurfaceRuntimeStatus::Failed | SurfaceRuntimeStatus::Unavailable
                )
            })
            .count();
        let circuit_open_count = runtime
            .iter()
            .filter(|surface| surface.status == SurfaceRuntimeStatus::CircuitOpen)
            .count();
        let status = if failed_count > 0 || circuit_open_count > 0 {
            "degraded"
        } else if degraded_count > 0 {
            "warning"
        } else {
            "ready"
        }
        .to_string();
        SurfaceHostHealth {
            status,
            surface_count: snapshot.surfaces.len(),
            external_surface_count,
            route_count,
            resource_count,
            ready_count,
            degraded_count,
            failed_count,
            circuit_open_count,
            roots: self
                .roots
                .iter()
                .map(|root| root.display().to_string())
                .collect(),
        }
    }

    pub(crate) fn get(&self, id: &str) -> Option<SurfaceDescriptor> {
        self.registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&normalize_surface_id(id))
            .cloned()
    }

    pub(crate) fn has_external_surface(&self, id: &str) -> bool {
        self.get(id).is_some_and(|surface| surface.entry.is_some())
    }

    pub(crate) fn start_monitor(&self) {
        let mut started = self
            .monitor_started
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *started {
            return;
        }
        *started = true;
        drop(started);

        let host = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                host.monitor_tick().await;
            }
        });
    }

    async fn monitor_tick(&self) {
        let surfaces = self.snapshot().surfaces;
        for surface in surfaces {
            if surface.lifecycle != SurfaceLifecycle::Managed {
                continue;
            }
            let Some(runtime) = self.runtime_snapshot(&surface.id) else {
                continue;
            };
            if matches!(
                runtime.status,
                SurfaceRuntimeStatus::Disabled | SurfaceRuntimeStatus::CircuitOpen
            ) {
                continue;
            }
            if let Some(next_retry_at) = runtime.next_retry_at {
                if next_retry_at > Utc::now() {
                    continue;
                }
            }
            let due = runtime
                .last_health_at
                .map(|last| {
                    let elapsed = Utc::now().signed_duration_since(last);
                    elapsed.num_milliseconds() >= surface.health.interval_ms as i64
                })
                .unwrap_or(surface.entry.is_some());
            if due {
                let _ = self.check_surface_health(&surface.id).await;
            }
        }
    }

    pub(crate) async fn start_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        let descriptor = self
            .get(surface)
            .ok_or_else(|| SurfaceError::Unavailable(normalize_surface_id(surface)))?;
        if descriptor.lifecycle == SurfaceLifecycle::Builtin {
            let snapshot = SurfaceRuntimeSnapshot::builtin(&descriptor.id);
            self.set_runtime(snapshot.clone()).await;
            return Ok(snapshot);
        }
        if descriptor.lifecycle != SurfaceLifecycle::Managed {
            let snapshot = self
                .mark_runtime_error(
                    &descriptor.id,
                    SurfaceRuntimeStatus::Unavailable,
                    SurfaceFailureKind::Unsupported,
                    "one-shot surface cannot be started as a managed process",
                )
                .await;
            return Ok(snapshot);
        }
        let process = self.managed_process(descriptor.clone()).await?;
        let mut snapshot = self.runtime_snapshot(&descriptor.id).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&descriptor.id, descriptor.lifecycle)
        });
        snapshot.status = SurfaceRuntimeStatus::Ready;
        snapshot.active = true;
        snapshot.pid = process.pid;
        snapshot.started_at = Some(process.started_at);
        snapshot.last_seen_at = Some(Utc::now());
        snapshot.consecutive_failures = 0;
        snapshot.circuit_open = false;
        snapshot.last_error = None;
        snapshot.available_actions = managed_actions(false);
        self.set_runtime(snapshot.clone()).await;
        self.push_ledger(SurfaceSupervisorEvent::new(
            &descriptor.id,
            SurfaceRuntimeStatus::Ready,
            "managed surface started",
        ))
        .await;
        Ok(snapshot)
    }

    pub(crate) async fn stop_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        let surface = normalize_surface_id(surface);
        if let Some(process) = self.managed.lock().await.remove(&surface) {
            if let Some(pid) = process.pid {
                #[cfg(unix)]
                {
                    let _ = Command::new("kill").arg(pid.to_string()).status();
                }
            }
            process.pending.lock().await.clear();
        }
        let mut snapshot = self.runtime_snapshot(&surface).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&surface, SurfaceLifecycle::Managed)
        });
        snapshot.status = SurfaceRuntimeStatus::Disabled;
        snapshot.active = false;
        snapshot.pid = None;
        snapshot.available_actions = vec![
            SurfaceSupervisorAction::Start,
            SurfaceSupervisorAction::Repair,
            SurfaceSupervisorAction::HealthCheck,
        ];
        self.set_runtime(snapshot.clone()).await;
        self.push_ledger(SurfaceSupervisorEvent::new(
            &surface,
            SurfaceRuntimeStatus::Disabled,
            "managed surface stopped by operator",
        ))
        .await;
        Ok(snapshot)
    }

    pub(crate) async fn restart_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        let surface = normalize_surface_id(surface);
        let _ = self.stop_surface(&surface).await?;
        let mut snapshot = self.runtime_snapshot(&surface).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&surface, SurfaceLifecycle::Managed)
        });
        snapshot.status = SurfaceRuntimeStatus::Restarting;
        snapshot.restart_count = snapshot.restart_count.saturating_add(1);
        snapshot.active = false;
        snapshot.available_actions = managed_actions(false);
        self.set_runtime(snapshot).await;
        self.start_surface(&surface).await
    }

    pub(crate) async fn repair_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        let surface = normalize_surface_id(surface);
        let mut snapshot = self.runtime_snapshot(&surface).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&surface, SurfaceLifecycle::Managed)
        });
        snapshot.circuit_open = false;
        snapshot.next_retry_at = None;
        snapshot.consecutive_failures = 0;
        snapshot.restart_count = 0;
        snapshot.status = SurfaceRuntimeStatus::Starting;
        snapshot.available_actions = managed_actions(false);
        self.set_runtime(snapshot).await;
        self.push_ledger(SurfaceSupervisorEvent::new(
            &surface,
            SurfaceRuntimeStatus::Starting,
            "manual surface repair requested",
        ))
        .await;
        self.restart_surface(&surface).await
    }

    pub(crate) fn routes(&self, id: &str) -> Option<SurfaceRouteSummary> {
        self.get(id).map(|surface| SurfaceRouteSummary {
            surface: surface.id,
            routes: surface.routes,
        })
    }

    pub(crate) fn resources(&self, id: &str) -> Option<SurfaceResourceSummary> {
        self.get(id).map(|surface| SurfaceResourceSummary {
            surface: surface.id,
            resources: surface.resources,
        })
    }

    pub(crate) fn resolve_static(
        &self,
        id: &str,
        requested_path: &str,
    ) -> Result<Option<SurfaceStaticFile>, SurfaceError> {
        if !request_path_is_safe(requested_path) {
            return Ok(None);
        }
        let Some(surface) = self.get(id) else {
            return Ok(None);
        };
        let requested_path = normalize_request_path(requested_path);
        for resource in &surface.resources {
            let mount = normalize_mount(&resource.mount);
            if !path_matches_mount(&requested_path, &mount) {
                continue;
            }
            let relative = strip_mount(&requested_path, &mount);
            let base = resource_base_dir(&surface, resource);
            if let Some(file_path) =
                resolve_resource_file(&base, &relative).filter(|file_path| file_path.is_file())
            {
                return Ok(Some(SurfaceStaticFile {
                    surface: surface.id,
                    mount,
                    requested_path,
                    file_path,
                    spa_fallback: false,
                }));
            }
            if resource.spa {
                let index = resolve_resource_file(&base, "index.html");
                if let Some(index) = index.filter(|index| index.is_file()) {
                    return Ok(Some(SurfaceStaticFile {
                        surface: surface.id,
                        mount,
                        requested_path,
                        file_path: index,
                        spa_fallback: true,
                    }));
                }
            }
        }
        Ok(None)
    }

    pub(crate) async fn send(
        &self,
        request: SurfaceSendRequest,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(surface) = self.get(&request.surface) else {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        };
        if surface.entry.is_none() {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        }
        let surface_id = normalize_surface_id(&request.surface);
        let frame = SurfaceFrame::Send {
            id: SurfaceFrame::new_id(),
            surface: surface_id.clone(),
            recipient: request.recipient,
            thread: request.thread,
            text: request.text,
            metadata: request.metadata,
        };
        self.invoke(surface, frame).await
    }

    pub(crate) async fn action(
        &self,
        request: SurfaceActionRequest,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(surface) = self.get(&request.surface) else {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        };
        if surface.entry.is_none() {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        }
        let surface_id = normalize_surface_id(&request.surface);
        let frame = SurfaceFrame::Action {
            id: SurfaceFrame::new_id(),
            surface: surface_id,
            action: request.action,
            payload: request.payload,
        };
        self.invoke(surface, frame).await
    }

    pub(crate) async fn callback(
        &self,
        surface: &str,
        path: &str,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(descriptor) = self.get(surface) else {
            return Ok(SurfaceOperationResult::unavailable(surface));
        };
        if !descriptor
            .routes
            .iter()
            .any(|route| route_matches(route, path, method))
        {
            return Ok(SurfaceOperationResult::error(
                surface,
                "surface_route_not_found",
                format!(
                    "surface `{}` has no route for {method} {path}",
                    descriptor.id
                ),
            ));
        }
        self.action(SurfaceActionRequest {
            surface: descriptor.id,
            action: "callback.dispatch".to_string(),
            payload: serde_json::json!({
                "path": normalize_request_path(path),
                "method": method.to_ascii_uppercase(),
                "payload": payload,
            }),
        })
        .await
    }

    pub(crate) async fn check_surface_health(
        &self,
        surface: &str,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(descriptor) = self.get(surface) else {
            return Ok(SurfaceOperationResult::unavailable(surface));
        };
        if descriptor.entry.is_none() {
            self.set_runtime(SurfaceRuntimeSnapshot::builtin(&descriptor.id))
                .await;
            return Ok(SurfaceOperationResult::ok(
                &descriptor.id,
                serde_json::json!({
                    "status": "ready",
                    "kind": "builtin",
                    "route_count": descriptor.routes.len(),
                    "resource_count": descriptor.resources.len(),
                }),
            ));
        }
        let frame = SurfaceFrame::Health {
            id: SurfaceFrame::new_id(),
            surface: Some(descriptor.id.clone()),
        };
        let started = Instant::now();
        let timeout = Duration::from_millis(descriptor.health.timeout_ms.max(1));
        let result = tokio::time::timeout(timeout, self.invoke(descriptor.clone(), frame)).await;
        match result {
            Ok(Ok(result)) if result.error.is_none() => {
                let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                let mut snapshot = self.runtime_snapshot(&descriptor.id).unwrap_or_else(|| {
                    SurfaceRuntimeSnapshot::discovered(&descriptor.id, descriptor.lifecycle)
                });
                snapshot.status = SurfaceRuntimeStatus::Ready;
                snapshot.active = true;
                snapshot.last_seen_at = Some(Utc::now());
                snapshot.last_health_at = Some(Utc::now());
                snapshot.latency_ms = Some(latency_ms);
                snapshot.consecutive_failures = 0;
                snapshot.circuit_open = false;
                snapshot.next_retry_at = None;
                snapshot.last_error = None;
                snapshot.available_actions = managed_actions(false);
                self.set_runtime(snapshot).await;
                Ok(result)
            }
            Ok(Ok(result)) => {
                let message = result
                    .error
                    .as_ref()
                    .map(|error| error.message.clone())
                    .unwrap_or_else(|| "surface health returned error".to_string());
                self.record_surface_failure(
                    descriptor.clone(),
                    SurfaceFailureKind::ProtocolError,
                    message.clone(),
                )
                .await;
                Ok(result)
            }
            Ok(Err(error)) => {
                self.record_surface_failure(
                    descriptor.clone(),
                    classify_surface_error(&error),
                    error.to_string(),
                )
                .await;
                Err(error)
            }
            Err(_) => {
                let snapshot = self
                    .record_surface_failure(
                        descriptor.clone(),
                        SurfaceFailureKind::HealthTimeout,
                        format!("surface health timed out after {}ms", timeout.as_millis()),
                    )
                    .await;
                Ok(SurfaceOperationResult::error(
                    &snapshot.surface,
                    "surface_health_timeout",
                    "surface health check timed out",
                ))
            }
        }
    }

    async fn invoke(
        &self,
        surface: SurfaceDescriptor,
        frame: SurfaceFrame,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        if surface.lifecycle == SurfaceLifecycle::Managed {
            let surface_id = surface.id.clone();
            let response = match self.invoke_managed(surface.clone(), frame).await {
                Ok(response) => response,
                Err(error) => {
                    self.record_surface_failure(
                        surface,
                        classify_surface_error(&error),
                        error.to_string(),
                    )
                    .await;
                    return Err(error);
                }
            };
            return Ok(operation_result_from_frame(&surface_id, response));
        }
        tokio::task::spawn_blocking(move || invoke_sidecar(surface, frame))
            .await
            .map_err(|error| SurfaceError::Invocation {
                surface: "unknown".to_string(),
                reason: format!("surface task join failed: {error}"),
            })?
    }

    async fn invoke_managed(
        &self,
        surface: SurfaceDescriptor,
        frame: SurfaceFrame,
    ) -> Result<SurfaceFrame, SurfaceError> {
        let request_id = frame_id(&frame).ok_or_else(|| SurfaceError::Invocation {
            surface: surface.id.clone(),
            reason: "managed surface request frame missing id".to_string(),
        })?;
        let process = self.managed_process(surface.clone()).await?;
        let (sender, receiver) = oneshot::channel();
        process
            .pending
            .lock()
            .await
            .insert(request_id.clone(), sender);
        let encoded = frame.encode_jsonl()?;
        let write_result: Result<(), std::io::Error> = {
            let mut stdin = process.stdin.lock().await;
            if let Err(error) = stdin.write_all(encoded.as_bytes()).await {
                Err(error)
            } else {
                stdin.flush().await
            }
        };
        if let Err(error) = write_result {
            process.pending.lock().await.remove(&request_id);
            return Err(SurfaceError::Invocation {
                surface: surface.id,
                reason: format!("failed to write managed jsonl request: {error}"),
            });
        }
        tokio::time::timeout(Duration::from_secs(30), receiver)
            .await
            .map_err(|_| SurfaceError::Invocation {
                surface: surface.id.clone(),
                reason: "managed surface request timed out".to_string(),
            })?
            .map_err(|_| SurfaceError::Invocation {
                surface: surface.id,
                reason: "managed surface response channel closed".to_string(),
            })
    }

    async fn managed_process(
        &self,
        surface: SurfaceDescriptor,
    ) -> Result<Arc<ManagedSurfaceProcess>, SurfaceError> {
        if let Some(process) = self.managed.lock().await.get(&surface.id).cloned() {
            return Ok(process);
        }
        let mut snapshot = self
            .runtime_snapshot(&surface.id)
            .unwrap_or_else(|| SurfaceRuntimeSnapshot::discovered(&surface.id, surface.lifecycle));
        if snapshot.circuit_open {
            return Err(SurfaceError::Invocation {
                surface: surface.id,
                reason: "surface circuit is open; manual repair is required".to_string(),
            });
        }
        snapshot.status = SurfaceRuntimeStatus::Starting;
        snapshot.available_actions = managed_actions(false);
        self.set_runtime(snapshot).await;
        let process = match start_managed_process(
            surface.clone(),
            self.runtime.clone(),
            self.ledger.clone(),
            self.managed.clone(),
        )
        .await
        {
            Ok(process) => Arc::new(process),
            Err(error) => {
                self.record_surface_failure(
                    surface.clone(),
                    SurfaceFailureKind::SpawnFailed,
                    error.to_string(),
                )
                .await;
                return Err(error);
            }
        };
        let mut processes = self.managed.lock().await;
        if let Some(existing) = processes.get(&surface.id) {
            return Ok(existing.clone());
        }
        processes.insert(surface.id, process.clone());
        Ok(process)
    }

    pub(crate) async fn events(&self, surface: &str) -> Vec<SurfaceFrame> {
        let surface = normalize_surface_id(surface);
        let process = self.managed.lock().await.get(&surface).cloned();
        let Some(process) = process else {
            return Vec::new();
        };
        let events = process.events.lock().await;
        events.iter().cloned().collect()
    }

    pub(crate) async fn supervisor_events(&self, surface: &str) -> Vec<SurfaceSupervisorEvent> {
        let surface = normalize_surface_id(surface);
        let ledger = self.ledger.lock().await;
        ledger
            .get(&surface)
            .map(|events| events.iter().cloned().collect())
            .unwrap_or_default()
    }

    async fn set_runtime(&self, snapshot: SurfaceRuntimeSnapshot) {
        self.runtime
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(snapshot.surface.clone(), snapshot);
    }

    async fn push_ledger(&self, event: SurfaceSupervisorEvent) {
        let mut ledger = self.ledger.lock().await;
        let events = ledger.entry(event.surface.clone()).or_default();
        events.push_back(event);
        while events.len() > 500 {
            events.pop_front();
        }
    }

    async fn mark_runtime_error(
        &self,
        surface: &str,
        status: SurfaceRuntimeStatus,
        kind: SurfaceFailureKind,
        message: impl Into<String>,
    ) -> SurfaceRuntimeSnapshot {
        let surface = normalize_surface_id(surface);
        let error = SurfaceRuntimeError::new(kind, message);
        let mut snapshot = self.runtime_snapshot(&surface).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&surface, SurfaceLifecycle::Managed)
        });
        snapshot.status = status;
        snapshot.active = false;
        snapshot.last_error = Some(error.clone());
        snapshot.available_actions = managed_actions(snapshot.circuit_open);
        self.set_runtime(snapshot.clone()).await;
        self.push_ledger(SurfaceSupervisorEvent::error(&surface, status, error))
            .await;
        snapshot
    }

    async fn record_surface_failure(
        &self,
        surface: SurfaceDescriptor,
        kind: SurfaceFailureKind,
        message: impl Into<String>,
    ) -> SurfaceRuntimeSnapshot {
        let message = message.into();
        let policy = surface.health.repair.clone();
        let mut snapshot = self
            .runtime_snapshot(&surface.id)
            .unwrap_or_else(|| SurfaceRuntimeSnapshot::discovered(&surface.id, surface.lifecycle));
        snapshot.consecutive_failures = snapshot.consecutive_failures.saturating_add(1);
        snapshot.last_health_at = Some(Utc::now());
        snapshot.last_error = Some(SurfaceRuntimeError::new(kind, message.clone()));

        if surface.lifecycle != SurfaceLifecycle::Managed {
            snapshot.status = SurfaceRuntimeStatus::Unavailable;
            snapshot.active = false;
            snapshot.available_actions = vec![SurfaceSupervisorAction::HealthCheck];
            self.set_runtime(snapshot.clone()).await;
            return snapshot;
        }

        if snapshot.restart_count >= policy.restart_limit {
            snapshot.status = SurfaceRuntimeStatus::CircuitOpen;
            snapshot.active = false;
            snapshot.circuit_open = true;
            snapshot.next_retry_at = Some(
                Utc::now()
                    + chrono::Duration::milliseconds(policy.circuit_half_open_after_ms as i64),
            );
            snapshot.available_actions = managed_actions(true);
            self.managed.lock().await.remove(&surface.id);
            self.set_runtime(snapshot.clone()).await;
            self.push_ledger(SurfaceSupervisorEvent::error(
                &surface.id,
                SurfaceRuntimeStatus::CircuitOpen,
                SurfaceRuntimeError::new(kind, message),
            ))
            .await;
            return snapshot;
        }

        if snapshot.consecutive_failures >= policy.failure_threshold {
            snapshot.status = SurfaceRuntimeStatus::Restarting;
            snapshot.active = false;
            snapshot.restart_count = snapshot.restart_count.saturating_add(1);
            snapshot.next_retry_at =
                Some(Utc::now() + backoff_duration(&policy, snapshot.restart_count));
            snapshot.available_actions = managed_actions(false);
            self.managed.lock().await.remove(&surface.id);
        } else {
            snapshot.status = SurfaceRuntimeStatus::Degraded;
            snapshot.available_actions = managed_actions(false);
        }
        self.set_runtime(snapshot.clone()).await;
        self.push_ledger(SurfaceSupervisorEvent::error(
            &surface.id,
            snapshot.status,
            SurfaceRuntimeError::new(kind, message),
        ))
        .await;
        snapshot
    }
}

impl Default for SurfaceHost {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

fn invoke_sidecar(
    surface: SurfaceDescriptor,
    frame: SurfaceFrame,
) -> Result<SurfaceOperationResult, SurfaceError> {
    let surface_id = surface.id.clone();
    let entry = surface
        .entry
        .clone()
        .ok_or_else(|| SurfaceError::Unavailable(surface_id.clone()))?;
    let manifest_path = PathBuf::from(&surface.source);
    let working_dir = manifest_path.parent().map(Path::to_path_buf);
    let mut command_path = PathBuf::from(entry);
    if command_path.is_relative() {
        if let Some(root) = &working_dir {
            command_path = root.join(command_path);
        }
    }

    let mut child = Command::new(&command_path)
        .current_dir(working_dir.as_deref().unwrap_or_else(|| Path::new(".")))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to launch `{}`: {error}", command_path.display()),
        })?;

    let mut stdin = child.stdin.take().ok_or_else(|| SurfaceError::Invocation {
        surface: surface_id.clone(),
        reason: "sidecar stdin is not available".to_string(),
    })?;
    let encoded = frame.encode_jsonl()?;
    stdin
        .write_all(encoded.as_bytes())
        .and_then(|_| stdin.flush())
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to write jsonl request: {error}"),
        })?;
    drop(stdin);

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "sidecar stdout is not available".to_string(),
        })?;
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to read jsonl response: {error}"),
        })?;
    if line.trim().is_empty() {
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: "sidecar returned no jsonl response".to_string(),
        });
    }

    let response = SurfaceFrame::decode_jsonl(&line)?;
    let _ = child.wait();
    Ok(operation_result_from_frame(&surface_id, response))
}

async fn start_managed_process(
    surface: SurfaceDescriptor,
    runtime: Arc<RwLock<BTreeMap<String, SurfaceRuntimeSnapshot>>>,
    ledger: Arc<AsyncMutex<HashMap<String, VecDeque<SurfaceSupervisorEvent>>>>,
    managed: Arc<AsyncMutex<HashMap<String, Arc<ManagedSurfaceProcess>>>>,
) -> Result<ManagedSurfaceProcess, SurfaceError> {
    let surface_id = surface.id.clone();
    let entry = surface
        .entry
        .clone()
        .ok_or_else(|| SurfaceError::Unavailable(surface_id.clone()))?;
    let manifest_path = PathBuf::from(&surface.source);
    let working_dir = manifest_path.parent().map(Path::to_path_buf);
    let mut command_path = PathBuf::from(entry);
    if command_path.is_relative() {
        if let Some(root) = &working_dir {
            command_path = root.join(command_path);
        }
    }

    let mut child = TokioCommand::new(&command_path)
        .current_dir(working_dir.as_deref().unwrap_or_else(|| Path::new(".")))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!(
                "failed to launch managed `{}`: {error}",
                command_path.display()
            ),
        })?;
    let pid = child.id();
    let started_at = Utc::now();
    let stdin = child.stdin.take().ok_or_else(|| SurfaceError::Invocation {
        surface: surface_id.clone(),
        reason: "managed sidecar stdin is not available".to_string(),
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "managed sidecar stdout is not available".to_string(),
        })?;
    let pending: Arc<AsyncMutex<HashMap<String, oneshot::Sender<SurfaceFrame>>>> =
        Arc::new(AsyncMutex::new(HashMap::new()));
    let events = Arc::new(AsyncMutex::new(VecDeque::new()));
    let reader_pending = pending.clone();
    let reader_events = events.clone();
    let reader_runtime = runtime.clone();
    let reader_ledger = ledger.clone();
    let reader_surface = surface_id.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(frame) = SurfaceFrame::decode_jsonl(&line) else {
                continue;
            };
            mark_surface_seen(&reader_runtime, &reader_surface, pid);
            if let Some(id) = frame_id(&frame) {
                if let Some(sender) = reader_pending.lock().await.remove(&id) {
                    let _ = sender.send(frame);
                    continue;
                }
            }
            if matches!(frame, SurfaceFrame::Event { .. }) {
                let mut events = reader_events.lock().await;
                events.push_back(frame);
                while events.len() > 200 {
                    events.pop_front();
                }
            }
        }
        push_supervisor_event(
            &reader_ledger,
            SurfaceSupervisorEvent::new(
                &reader_surface,
                SurfaceRuntimeStatus::Unavailable,
                "managed surface stdout closed",
            ),
        )
        .await;
    });
    let wait_runtime = runtime.clone();
    let wait_ledger = ledger.clone();
    let wait_managed = managed.clone();
    let wait_surface = surface_id.clone();
    tokio::spawn(async move {
        let status = child.wait().await;
        {
            let mut runtime = wait_runtime
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let snapshot = runtime.entry(wait_surface.clone()).or_insert_with(|| {
                SurfaceRuntimeSnapshot::discovered(&wait_surface, SurfaceLifecycle::Managed)
            });
            if !matches!(snapshot.status, SurfaceRuntimeStatus::Disabled) {
                snapshot.status = SurfaceRuntimeStatus::Unavailable;
                snapshot.active = false;
                snapshot.pid = None;
                snapshot.last_error = Some(SurfaceRuntimeError::new(
                    SurfaceFailureKind::ProcessExited,
                    match status {
                        Ok(status) => format!("managed surface exited with status {status}"),
                        Err(error) => format!("managed surface wait failed: {error}"),
                    },
                ));
                snapshot.available_actions = managed_actions(false);
            }
        }
        wait_managed.lock().await.remove(&wait_surface);
        push_supervisor_event(
            &wait_ledger,
            SurfaceSupervisorEvent::new(
                &wait_surface,
                SurfaceRuntimeStatus::Unavailable,
                "managed surface process exited",
            ),
        )
        .await;
    });
    Ok(ManagedSurfaceProcess {
        pid,
        started_at,
        stdin: AsyncMutex::new(stdin),
        pending,
        events,
    })
}

fn operation_result_from_frame(surface: &str, frame: SurfaceFrame) -> SurfaceOperationResult {
    match frame {
        SurfaceFrame::Ok { payload, .. } => SurfaceOperationResult::ok(surface, payload),
        SurfaceFrame::Error { code, message, .. } => {
            SurfaceOperationResult::error(surface, code, message)
        }
        SurfaceFrame::HandshakeOk { capabilities, .. } => SurfaceOperationResult::ok(
            surface,
            serde_json::json!({
                "status": "ok",
                "capabilities": capabilities,
            }),
        ),
        other => SurfaceOperationResult::error(
            surface,
            "surface_unexpected_frame",
            format!("unexpected surface response frame: {other:?}"),
        ),
    }
}

fn frame_id(frame: &SurfaceFrame) -> Option<String> {
    match frame {
        SurfaceFrame::Handshake { id, .. }
        | SurfaceFrame::HandshakeOk { id, .. }
        | SurfaceFrame::Configure { id, .. }
        | SurfaceFrame::Connect { id, .. }
        | SurfaceFrame::Disconnect { id, .. }
        | SurfaceFrame::Send { id, .. }
        | SurfaceFrame::Action { id, .. }
        | SurfaceFrame::Health { id, .. }
        | SurfaceFrame::Ok { id, .. } => Some(id.clone()),
        SurfaceFrame::Error { id, .. } => id.clone(),
        SurfaceFrame::Event { .. } => None,
    }
}

fn managed_actions(circuit_open: bool) -> Vec<SurfaceSupervisorAction> {
    if circuit_open {
        return vec![
            SurfaceSupervisorAction::Repair,
            SurfaceSupervisorAction::HealthCheck,
        ];
    }
    vec![
        SurfaceSupervisorAction::Start,
        SurfaceSupervisorAction::Stop,
        SurfaceSupervisorAction::Restart,
        SurfaceSupervisorAction::Repair,
        SurfaceSupervisorAction::HealthCheck,
    ]
}

fn backoff_duration(policy: &SurfaceRepairPolicy, restart_count: u32) -> chrono::Duration {
    let exponent = restart_count.saturating_sub(1).min(10);
    let multiplier = 2u64.saturating_pow(exponent);
    let millis = policy
        .backoff_initial_ms
        .saturating_mul(multiplier)
        .min(policy.backoff_max_ms);
    chrono::Duration::milliseconds(millis as i64)
}

fn classify_surface_error(error: &SurfaceError) -> SurfaceFailureKind {
    match error {
        SurfaceError::InvalidManifest { .. } => SurfaceFailureKind::ManifestInvalid,
        SurfaceError::Unavailable(_) => SurfaceFailureKind::EntryMissing,
        SurfaceError::Invocation { reason, .. } if reason.contains("timed out") => {
            SurfaceFailureKind::HealthTimeout
        }
        SurfaceError::Invocation { reason, .. } if reason.contains("launch") => {
            SurfaceFailureKind::SpawnFailed
        }
        SurfaceError::FrameParse(_) => SurfaceFailureKind::ProtocolError,
        SurfaceError::ManifestIo { .. } | SurfaceError::ManifestJson { .. } => {
            SurfaceFailureKind::ManifestInvalid
        }
        SurfaceError::Invocation { .. } => SurfaceFailureKind::Unknown,
    }
}

fn mark_surface_seen(
    runtime: &Arc<RwLock<BTreeMap<String, SurfaceRuntimeSnapshot>>>,
    surface: &str,
    pid: Option<u32>,
) {
    let mut runtime = runtime
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let snapshot = runtime
        .entry(surface.to_string())
        .or_insert_with(|| SurfaceRuntimeSnapshot::discovered(surface, SurfaceLifecycle::Managed));
    snapshot.active = true;
    snapshot.pid = pid;
    snapshot.last_seen_at = Some(Utc::now());
    if matches!(
        snapshot.status,
        SurfaceRuntimeStatus::Starting
            | SurfaceRuntimeStatus::Restarting
            | SurfaceRuntimeStatus::Discovered
            | SurfaceRuntimeStatus::Unavailable
    ) {
        snapshot.status = SurfaceRuntimeStatus::Ready;
    }
}

async fn push_supervisor_event(
    ledger: &Arc<AsyncMutex<HashMap<String, VecDeque<SurfaceSupervisorEvent>>>>,
    event: SurfaceSupervisorEvent,
) {
    let mut ledger = ledger.lock().await;
    let events = ledger.entry(event.surface.clone()).or_default();
    events.push_back(event);
    while events.len() > 500 {
        events.pop_front();
    }
}

fn normalize_request_path(path: &str) -> String {
    let mut cleaned = PathBuf::new();
    for component in Path::new(path.trim_start_matches('/')).components() {
        match component {
            Component::Normal(part) => cleaned.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => {}
        }
    }
    let normalized = cleaned.to_string_lossy().replace('\\', "/");
    if normalized.is_empty() {
        "/".to_string()
    } else {
        format!("/{normalized}")
    }
}

fn request_path_is_safe(path: &str) -> bool {
    Path::new(path.trim_start_matches('/'))
        .components()
        .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn normalize_mount(mount: &str) -> String {
    let normalized = normalize_request_path(mount);
    if normalized == "/" {
        normalized
    } else {
        normalized.trim_end_matches('/').to_string()
    }
}

fn path_matches_mount(path: &str, mount: &str) -> bool {
    if mount == "/" {
        return true;
    }
    path == mount
        || path
            .strip_prefix(mount)
            .is_some_and(|tail| tail.starts_with('/'))
}

fn strip_mount(path: &str, mount: &str) -> String {
    if mount == "/" {
        return path.trim_start_matches('/').to_string();
    }
    path.strip_prefix(mount)
        .unwrap_or(path)
        .trim_start_matches('/')
        .to_string()
}

fn resource_base_dir(surface: &SurfaceDescriptor, resource: &SurfaceResource) -> PathBuf {
    let declared = PathBuf::from(&resource.dir);
    if declared.is_absolute() {
        return declared;
    }
    let source_path = PathBuf::from(&surface.source);
    let root = source_path.parent().unwrap_or_else(|| Path::new("."));
    root.join(declared)
}

fn resolve_resource_file(base: &Path, relative: &str) -> Option<PathBuf> {
    let base = base.canonicalize().ok()?;
    let mut candidate = base.clone();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(part) => candidate.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    let canonical = candidate.canonicalize().ok()?;
    if canonical.starts_with(&base) {
        Some(canonical)
    } else {
        None
    }
}

fn route_matches(route: &SurfaceRoute, path: &str, method: &str) -> bool {
    let route_path = normalize_request_path(&route.path);
    let request_path = normalize_request_path(path);
    route_path == request_path && route.method.eq_ignore_ascii_case(method)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
