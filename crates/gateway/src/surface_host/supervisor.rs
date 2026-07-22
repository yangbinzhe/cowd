use std::collections::{BTreeMap, HashMap, VecDeque};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};

use chrono::Utc;
use sandbox_launcher::{program_command_with_args, SandboxLaunchSpec};
use surface::{
    normalize_surface_id, SurfaceDescriptor, SurfaceError, SurfaceFailureKind, SurfaceFrame,
    SurfaceLifecycle, SurfaceRuntimeError, SurfaceRuntimeSnapshot, SurfaceRuntimeStatus,
    SurfaceSupervisorAction, SurfaceSupervisorEvent,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::sync::{broadcast, Mutex as AsyncMutex};

use super::edge_h2::{bootstrap_request, EdgeH2Client};
use super::types::ManagedSurfaceProcess;
use super::{managed_actions, push_supervisor_event, SurfaceHost};

impl SurfaceHost {
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
        let mut snapshot = self.runtime_for_discovered(&descriptor.id, descriptor.lifecycle);
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
            let _ = std::fs::remove_dir_all(&process.runtime_dir);
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

    pub(super) async fn managed_process(
        &self,
        surface: SurfaceDescriptor,
    ) -> Result<Arc<ManagedSurfaceProcess>, SurfaceError> {
        if let Some(process) = self.managed.lock().await.get(&surface.id).cloned() {
            return Ok(process);
        }
        let mut snapshot = self.runtime_for_discovered(&surface.id, surface.lifecycle);
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
            self.event_tx.clone(),
            self.messages.clone(),
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
        processes.insert(surface.id.clone(), process.clone());
        drop(processes);
        if let Err(error) = self
            .configure_managed_surface(surface.clone(), process.clone())
            .await
        {
            self.managed.lock().await.remove(&surface.id);
            return Err(error);
        }
        Ok(process)
    }

    async fn configure_managed_surface(
        &self,
        surface: SurfaceDescriptor,
        process: Arc<ManagedSurfaceProcess>,
    ) -> Result<(), SurfaceError> {
        let config = self
            .config_for(&surface.id)
            .or_else(|| default_source_surface_config(&surface.capabilities));
        let Some(config) = config else {
            return Ok(());
        };
        let frame = SurfaceFrame::Configure {
            id: SurfaceFrame::new_id(),
            surface: surface.id.clone(),
            config,
        };
        let response = process.client.invoke(&frame).await?;
        if matches!(response, SurfaceFrame::Ok { .. }) {
            return Ok(());
        }
        Err(SurfaceError::Invocation {
            surface: surface.id,
            reason: format!("surface configure failed: {response:?}"),
        })
    }
}

fn default_source_surface_config(
    capabilities: &[surface::SurfaceCapability],
) -> Option<serde_json::Value> {
    capabilities
        .iter()
        .any(|capability| capability.capability.starts_with("source."))
        .then(|| serde_json::json!({}))
}

async fn start_managed_process(
    surface: SurfaceDescriptor,
    runtime: Arc<RwLock<BTreeMap<String, SurfaceRuntimeSnapshot>>>,
    ledger: Arc<AsyncMutex<HashMap<String, VecDeque<SurfaceSupervisorEvent>>>>,
    managed: Arc<AsyncMutex<HashMap<String, Arc<ManagedSurfaceProcess>>>>,
    event_tx: broadcast::Sender<SurfaceFrame>,
    messages: Arc<dyn surface::SurfaceMessageLedger>,
) -> Result<ManagedSurfaceProcess, SurfaceError> {
    let surface_id = surface.id.clone();
    let (artifact, driver_profile) =
        surface
            .managed_artifact()
            .ok_or_else(|| SurfaceError::Invocation {
                surface: surface_id.clone(),
                reason: "managed surface is missing managed runtime spec".to_string(),
            })?;
    let manifest_path = PathBuf::from(&surface.source);
    let working_dir = manifest_path.parent().map(Path::to_path_buf);
    let manifest_dir = working_dir
        .as_deref()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "managed surface manifest has no parent directory".to_string(),
        })?;
    let command_path = resolve_managed_artifact(&manifest_path, artifact).map_err(|reason| {
        SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason,
        }
    })?;
    let runtime_dir =
        create_runtime_dir(&surface_id).map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to create managed edge runtime directory: {error}"),
        })?;
    let staged_command = stage_managed_artifact(&command_path, &runtime_dir).map_err(|error| {
        let _ = std::fs::remove_dir_all(&runtime_dir);
        SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to stage managed edge artifact: {error}"),
        }
    })?;
    let socket_path = runtime_dir.join("edge.sock");
    let credential_path = runtime_dir.join("credential");
    let token = format!("{}{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    std::fs::write(&credential_path, &token).map_err(|error| {
        let _ = std::fs::remove_dir_all(&runtime_dir);
        SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to write managed edge credential: {error}"),
        }
    })?;
    std::fs::set_permissions(&credential_path, std::fs::Permissions::from_mode(0o600)).map_err(
        |error| {
            let _ = std::fs::remove_dir_all(&runtime_dir);
            SurfaceError::Invocation {
                surface: surface_id.clone(),
                reason: format!("failed to secure managed edge credential: {error}"),
            }
        },
    )?;
    // 受信 artifact 先复制到本次 0700 runtime
    // 目录，再以该目录作为最小 sandbox workspace。这样安装包中的
    // `edge/` 二进制无需位于单个 connector 清单目录内，也不会为了
    // 找到程序而把整个安装父目录暴露给 sidecar。
    let mut sandbox = SandboxLaunchSpec::workspace(&runtime_dir);
    sandbox.working_directory = Some(runtime_dir.clone());
    sandbox.readable_roots.push(manifest_dir.to_path_buf());
    sandbox.writable_roots.push(runtime_dir.clone());
    let program_args = vec![
        "--socket".to_string(),
        socket_path.display().to_string(),
        "--credential-file".to_string(),
        credential_path.display().to_string(),
    ];
    let prepared =
        program_command_with_args(&staged_command, &program_args, &sandbox).map_err(|error| {
            let _ = std::fs::remove_dir_all(&runtime_dir);
            SurfaceError::Invocation {
                surface: surface_id.clone(),
                reason: format!("managed surface sandbox unavailable: {error}"),
            }
        })?;
    let mut child = TokioCommand::new(prepared.program)
        .args(prepared.args)
        .env_clear()
        .envs(prepared.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            let _ = std::fs::remove_dir_all(&runtime_dir);
            SurfaceError::Invocation {
                surface: surface_id.clone(),
                reason: format!(
                    "failed to launch managed `{}`: {error}",
                    staged_command.display()
                ),
            }
        })?;
    let pid = child.id();
    let started_at = Utc::now();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "managed sidecar stdout is not available".to_string(),
        })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "managed sidecar stderr is not available".to_string(),
        })?;
    spawn_child_log_drain(surface_id.clone(), "stdout", stdout);
    spawn_child_log_drain(surface_id.clone(), "stderr", stderr);

    let client = match EdgeH2Client::connect(&socket_path, &surface_id, &token).await {
        Ok(client) => client,
        Err(error) => {
            let _ = child.kill().await;
            let _ = std::fs::remove_dir_all(&runtime_dir);
            return Err(error);
        }
    };
    let capabilities = surface
        .capabilities
        .iter()
        .map(|capability| capability.capability.clone())
        .collect::<Vec<_>>();
    let bootstrap = bootstrap_request(&surface_id, driver_profile, capabilities);
    let bootstrap_response = match client.bootstrap(&bootstrap).await {
        Ok(response) => response,
        Err(error) => {
            let _ = child.kill().await;
            let _ = std::fs::remove_dir_all(&runtime_dir);
            return Err(error);
        }
    };
    if bootstrap_response.surface_id != surface_id
        || bootstrap_response.driver_profile != driver_profile
    {
        let _ = child.kill().await;
        let _ = std::fs::remove_dir_all(&runtime_dir);
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: "managed edge bootstrap identity mismatch".to_string(),
        });
    }
    let events = Arc::new(AsyncMutex::new(VecDeque::new()));
    client.spawn_event_stream(events.clone(), event_tx, messages);
    let wait_runtime = runtime.clone();
    let wait_ledger = ledger.clone();
    let wait_managed = managed.clone();
    let wait_surface = surface_id.clone();
    let wait_runtime_dir = runtime_dir.clone();
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
        let _ = std::fs::remove_dir_all(wait_runtime_dir);
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
        client,
        events,
        runtime_dir,
    })
}

fn resolve_managed_artifact(manifest: &Path, artifact: &str) -> Result<PathBuf, String> {
    if artifact.is_empty()
        || artifact.contains('/')
        || artifact.contains('\\')
        || artifact == "."
        || artifact == ".."
    {
        return Err("managed artifact must be a trusted file name".to_string());
    }
    let mut candidates = Vec::new();
    if let Some(parent) = manifest.parent() {
        for ancestor in parent.ancestors().take(5) {
            candidates.push(ancestor.join("bin").join(artifact));
        }
    }
    if let Some(parent) = std::env::current_exe()
        .ok()
        .and_then(|executable| executable.parent().map(Path::to_path_buf))
    {
        candidates.push(parent.join("edge").join(artifact));
        candidates.push(parent.join(artifact));
    }
    for candidate in candidates {
        if candidate.is_file() {
            return candidate.canonicalize().map_err(|error| {
                format!(
                    "failed to canonicalize managed artifact `{}`: {error}",
                    candidate.display()
                )
            });
        }
    }
    Err(format!(
        "managed artifact `{artifact}` was not found in the trusted Edge bundle"
    ))
}

fn stage_managed_artifact(command: &Path, runtime_dir: &Path) -> std::io::Result<PathBuf> {
    let file_name = command.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "managed artifact has no file name",
        )
    })?;
    let metadata = std::fs::metadata(command)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "managed artifact is not an executable regular file",
        ));
    }
    let staged = runtime_dir.join(file_name);
    std::fs::copy(command, &staged)?;
    Ok(staged)
}

fn create_runtime_dir(surface: &str) -> std::io::Result<PathBuf> {
    let root = std::env::temp_dir().join("cowd-edge-runtime").join(format!(
        "{}-{}",
        normalize_surface_id(surface),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    Ok(root)
}

fn spawn_child_log_drain<R>(surface: String, stream: &'static str, reader: R)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = if line.len() > 16 * 1024 {
                format!("{}…", line.chars().take(16 * 1024).collect::<String>())
            } else {
                line
            };
            if stream == "stderr" {
                tracing::warn!(surface = %surface, child_stream = stream, message = %line);
            } else {
                tracing::debug!(surface = %surface, child_stream = stream, message = %line);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{default_source_surface_config, stage_managed_artifact};
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn source_surface_without_explicit_config_receives_empty_config() {
        let source = vec![surface::SurfaceCapability::new(
            "postgres",
            "source.incremental",
        )];
        let message = vec![surface::SurfaceCapability::new("lark", "message.send")];

        assert_eq!(
            default_source_surface_config(&source),
            Some(serde_json::json!({}))
        );
        assert_eq!(default_source_surface_config(&message), None);
    }

    #[test]
    fn managed_artifact_is_staged_inside_private_runtime_root() {
        let root = tempfile::tempdir().expect("temporary bundle");
        let bundle = root.path().join("bundle");
        let runtime = root.path().join("runtime");
        std::fs::create_dir_all(&bundle).expect("bundle directory");
        std::fs::create_dir_all(&runtime).expect("runtime directory");
        let command = bundle.join("cowd-edge-fixture");
        std::fs::write(&command, b"fixture").expect("fixture artifact");
        std::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o755))
            .expect("executable fixture");

        let staged = stage_managed_artifact(&command, &runtime).expect("stage artifact");

        assert_eq!(staged, runtime.join("cowd-edge-fixture"));
        assert!(staged.starts_with(&runtime));
        assert_eq!(std::fs::read(staged).unwrap(), b"fixture");
    }
}
