use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{BufRead, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::Serialize;
use surface::{
    builtin_surfaces, normalize_surface_id, SurfaceActionRequest, SurfaceDescriptor, SurfaceError,
    SurfaceFrame, SurfaceLifecycle, SurfaceManifest, SurfaceOperationResult,
    SurfaceRegistrySnapshot, SurfaceResource, SurfaceResourceKind, SurfaceRoute,
    SurfaceSendRequest, SURFACE_MANIFEST_FILE,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command as TokioCommand};
use tokio::sync::{oneshot, Mutex as AsyncMutex};

#[derive(Debug, Clone)]
pub(crate) struct SurfaceHost {
    registry: Arc<RwLock<BTreeMap<String, SurfaceDescriptor>>>,
    roots: Vec<PathBuf>,
    managed: Arc<AsyncMutex<HashMap<String, Arc<ManagedSurfaceProcess>>>>,
}

#[derive(Debug)]
struct ManagedSurfaceProcess {
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
    pub(crate) status: &'static str,
    pub(crate) surface_count: usize,
    pub(crate) external_surface_count: usize,
    pub(crate) route_count: usize,
    pub(crate) resource_count: usize,
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
            roots,
            managed: Arc::new(AsyncMutex::new(HashMap::new())),
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
        for manifest in builtin_surfaces().into_values() {
            let descriptor = SurfaceDescriptor::from_manifest(&manifest, "builtin");
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

    pub(crate) fn health(&self) -> SurfaceHostHealth {
        let snapshot = self.snapshot();
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
        SurfaceHostHealth {
            status: "ready",
            surface_count: snapshot.surfaces.len(),
            external_surface_count,
            route_count,
            resource_count,
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
        self.invoke(descriptor, frame).await
    }

    async fn invoke(
        &self,
        surface: SurfaceDescriptor,
        frame: SurfaceFrame,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        if surface.lifecycle == SurfaceLifecycle::Managed {
            let surface_id = surface.id.clone();
            let response = self.invoke_managed(surface, frame).await?;
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
        let mut processes = self.managed.lock().await;
        if let Some(process) = processes.get(&surface.id) {
            return Ok(process.clone());
        }
        let process = Arc::new(start_managed_process(surface.clone()).await?);
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
        .spawn()
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!(
                "failed to launch managed `{}`: {error}",
                command_path.display()
            ),
        })?;
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
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(frame) = SurfaceFrame::decode_jsonl(&line) else {
                continue;
            };
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
    });
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
    Ok(ManagedSurfaceProcess {
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
}
