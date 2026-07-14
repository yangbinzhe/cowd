use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};

use chrono::Utc;
use sandbox_launcher::{program_command, SandboxLaunchSpec};
use surface::{
    normalize_surface_id, SurfaceDescriptor, SurfaceError, SurfaceFailureKind, SurfaceFrame,
    SurfaceLifecycle, SurfaceRuntimeError, SurfaceRuntimeSnapshot, SurfaceRuntimeStatus,
    SurfaceSupervisorAction, SurfaceSupervisorEvent,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::sync::{broadcast, oneshot, Mutex as AsyncMutex};

use super::types::ManagedSurfaceProcess;
use super::{frame_id, managed_actions, mark_surface_seen, push_supervisor_event, SurfaceHost};

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
        let Some(config) = self.config_for(&surface.id) else {
            return Ok(());
        };
        let frame = SurfaceFrame::Configure {
            id: SurfaceFrame::new_id(),
            surface: surface.id.clone(),
            config,
        };
        let request_id = frame_id(&frame).ok_or_else(|| SurfaceError::Invocation {
            surface: surface.id.clone(),
            reason: "managed surface configure frame missing id".to_string(),
        })?;
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
                reason: format!("failed to write managed configure request: {error}"),
            });
        }
        let response = tokio::time::timeout(std::time::Duration::from_secs(30), receiver)
            .await
            .map_err(|_| SurfaceError::Invocation {
                surface: surface.id.clone(),
                reason: "managed surface configure timed out".to_string(),
            })?
            .map_err(|_| SurfaceError::Invocation {
                surface: surface.id.clone(),
                reason: "managed surface configure channel closed".to_string(),
            })?;
        if matches!(response, SurfaceFrame::Ok { .. }) {
            return Ok(());
        }
        Err(SurfaceError::Invocation {
            surface: surface.id,
            reason: format!("surface configure failed: {response:?}"),
        })
    }
}

async fn start_managed_process(
    surface: SurfaceDescriptor,
    runtime: Arc<RwLock<BTreeMap<String, SurfaceRuntimeSnapshot>>>,
    ledger: Arc<AsyncMutex<HashMap<String, VecDeque<SurfaceSupervisorEvent>>>>,
    managed: Arc<AsyncMutex<HashMap<String, Arc<ManagedSurfaceProcess>>>>,
    event_tx: broadcast::Sender<SurfaceFrame>,
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

    let workspace_root = working_dir
        .as_deref()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "managed surface manifest has no parent directory".to_string(),
        })?;
    let mut sandbox = SandboxLaunchSpec::workspace(workspace_root);
    sandbox.working_directory = Some(workspace_root.to_path_buf());
    let prepared =
        program_command(&command_path, &sandbox).map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("managed surface sandbox unavailable: {error}"),
        })?;
    let mut child = TokioCommand::new(prepared.program)
        .args(prepared.args)
        .env_clear()
        .envs(prepared.environment)
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
    let pending: Arc<AsyncMutex<HashMap<String, tokio::sync::oneshot::Sender<SurfaceFrame>>>> =
        Arc::new(AsyncMutex::new(HashMap::new()));
    let events = Arc::new(AsyncMutex::new(VecDeque::new()));
    let reader_pending = pending.clone();
    let reader_events = events.clone();
    let reader_runtime = runtime.clone();
    let reader_ledger = ledger.clone();
    let reader_surface = surface_id.clone();
    let reader_event_tx = event_tx.clone();
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
                events.push_back(frame.clone());
                while events.len() > 200 {
                    events.pop_front();
                }
                let _ = reader_event_tx.send(frame);
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
