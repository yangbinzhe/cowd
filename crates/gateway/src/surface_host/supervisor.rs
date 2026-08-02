use std::collections::VecDeque;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sandbox_launcher::{program_command_with_args, SandboxLaunchSpec};
use surface::{
    normalize_surface_id, SurfaceDescriptor, SurfaceError, SurfaceFailureKind, SurfaceFrame,
    SurfaceLifecycle, SurfaceRepairPolicy, SurfaceRuntimeError, SurfaceRuntimeSnapshot,
    SurfaceRuntimeStatus, SurfaceStateMode, SurfaceSupervisorAction, SurfaceSupervisorEvent,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::sync::{broadcast, Mutex as AsyncMutex};

use super::edge_h2::{bootstrap_request, EdgeH2Client};
use super::types::ManagedSurfaceProcess;
use super::{managed_actions, push_supervisor_event, repair::backoff_duration, SurfaceHost};

#[derive(Debug)]
enum ManagedWorkerStart {
    Created(Arc<ManagedSurfaceProcess>),
    Existing(Arc<ManagedSurfaceProcess>),
}

#[derive(Clone)]
struct ManagedWorkerFactory {
    host: SurfaceHost,
    descriptor: SurfaceDescriptor,
}

impl ManagedWorkerFactory {
    fn new(host: SurfaceHost, descriptor: SurfaceDescriptor) -> Self {
        Self { host, descriptor }
    }

    async fn start(&self) -> Result<ManagedWorkerStart, SurfaceError> {
        let lifecycle_lock = self.host.lifecycle_lock_for(&self.descriptor.id).await;
        let _lifecycle = lifecycle_lock.lock().await;
        self.start_locked().await
    }

    async fn start_locked(&self) -> Result<ManagedWorkerStart, SurfaceError> {
        self.host
            .gateway_tasks()
            .open_owner(crate::runtime_host::task_set::GatewayTaskOwner::Surface(
                self.descriptor.id.clone(),
            ))
            .await
            .map_err(|error| SurfaceError::Invocation {
                surface: self.descriptor.id.clone(),
                reason: format!("managed Surface owner admission failed: {error}"),
            })?;
        if let Some(existing) = self
            .host
            .managed
            .lock()
            .await
            .get(&self.descriptor.id)
            .cloned()
        {
            return Ok(ManagedWorkerStart::Existing(existing));
        }

        self.host
            .publish_managed_worker_starting(&self.descriptor)
            .await;
        let process = match start_managed_process(
            self.descriptor.clone(),
            self.host.event_tx.clone(),
            self.host.messages.clone(),
            self.host.gateway_tasks(),
        )
        .await
        {
            Ok(process) => Arc::new(process),
            Err(error) => {
                self.host
                    .gateway_tasks()
                    .close_owner_and_drain(
                        crate::runtime_host::task_set::GatewayTaskOwner::Surface(
                            self.descriptor.id.clone(),
                        ),
                        Duration::from_secs(5),
                    )
                    .await;
                return Err(error);
            }
        };
        if let Err(error) = self
            .host
            .configure_managed_surface(self.descriptor.clone(), process.clone())
            .await
        {
            rollback_managed_worker_start(&self.descriptor.id, &process).await;
            self.host
                .gateway_tasks()
                .close_owner_and_drain(
                    crate::runtime_host::task_set::GatewayTaskOwner::Surface(
                        self.descriptor.id.clone(),
                    ),
                    Duration::from_secs(5),
                )
                .await;
            return Err(error);
        }

        self.host
            .managed
            .lock()
            .await
            .insert(self.descriptor.id.clone(), process.clone());
        self.host
            .publish_managed_worker_ready(&self.descriptor, &process)
            .await;
        Ok(ManagedWorkerStart::Created(process))
    }
}

#[derive(Debug)]
struct ManagedRestartDecision {
    delay: Duration,
    next_retry_at: chrono::DateTime<Utc>,
}

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
        let lifecycle_lock = self.lifecycle_lock_for(&descriptor.id).await;
        let _lifecycle = lifecycle_lock.lock().await;
        self.start_surface_locked(descriptor).await
    }

    async fn start_surface_locked(
        &self,
        descriptor: SurfaceDescriptor,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        let process = self.managed_process_locked(descriptor.clone()).await?;
        let mut snapshot = self.runtime_for_discovered(&descriptor.id, descriptor.lifecycle);
        snapshot.status = SurfaceRuntimeStatus::Ready;
        snapshot.active = true;
        snapshot.pid = process.pid;
        snapshot.started_at = Some(process.started_at);
        snapshot.last_seen_at = Some(Utc::now());
        snapshot.consecutive_failures = 0;
        snapshot.circuit_open = false;
        snapshot.next_retry_at = None;
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
        let lifecycle_lock = self.lifecycle_lock_for(&surface).await;
        let _lifecycle = lifecycle_lock.lock().await;
        self.stop_surface_locked(surface).await
    }

    async fn stop_surface_locked(
        &self,
        surface: String,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        // Publish the stop gate before sending termination. The process remains
        // owned until termination succeeds, so a failed stop cannot leave a
        // live but untracked child.
        let process = self.managed.lock().await.get(&surface).cloned();
        let mut snapshot = self.runtime_snapshot(&surface).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&surface, SurfaceLifecycle::Managed)
        });
        snapshot.status = SurfaceRuntimeStatus::Disabled;
        snapshot.active = false;
        snapshot.pid = None;
        snapshot.next_retry_at = None;
        snapshot.available_actions = vec![
            SurfaceSupervisorAction::Start,
            SurfaceSupervisorAction::Repair,
            SurfaceSupervisorAction::HealthCheck,
        ];
        self.set_runtime(snapshot.clone()).await;

        let mut cleanup_error = None;
        if let Some(process) = process.as_ref() {
            if let Err(error) = terminate_managed_child(&process.child).await {
                snapshot.active = true;
                snapshot.pid = process.pid;
                snapshot.last_error = Some(SurfaceRuntimeError::new(
                    SurfaceFailureKind::ProcessExited,
                    error.clone(),
                ));
                self.set_runtime(snapshot.clone()).await;
                self.push_ledger(SurfaceSupervisorEvent::error(
                    &surface,
                    SurfaceRuntimeStatus::Disabled,
                    SurfaceRuntimeError::new(SurfaceFailureKind::ProcessExited, error.clone()),
                ))
                .await;
                return Err(SurfaceError::Invocation {
                    surface,
                    reason: error,
                });
            }
            let mut processes = self.managed.lock().await;
            if processes
                .get(&surface)
                .is_some_and(|current| Arc::ptr_eq(current, process))
            {
                processes.remove(&surface);
            }
            drop(processes);
            if let Err(error) = std::fs::remove_dir_all(&process.runtime_dir) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    cleanup_error = Some(format!(
                        "managed surface stopped but runtime cleanup failed: {error}"
                    ));
                }
            }
        }
        let task_report = self
            .gateway_tasks
            .close_owner_and_drain(
                crate::runtime_host::task_set::GatewayTaskOwner::Surface(surface.clone()),
                Duration::from_secs(5),
            )
            .await;
        if task_report.forced_aborts > 0 {
            cleanup_error = Some(format!(
                "{} Surface tasks required forced abort",
                task_report.forced_aborts
            ));
        }
        snapshot.last_error = cleanup_error
            .clone()
            .map(|message| SurfaceRuntimeError::new(SurfaceFailureKind::Unknown, message));
        self.set_runtime(snapshot.clone()).await;
        self.push_ledger(SurfaceSupervisorEvent::new(
            &surface,
            SurfaceRuntimeStatus::Disabled,
            cleanup_error
                .as_deref()
                .unwrap_or("managed surface stopped by operator"),
        ))
        .await;
        Ok(snapshot)
    }

    pub(crate) async fn restart_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        let surface = normalize_surface_id(surface);
        let lifecycle_lock = self.lifecycle_lock_for(&surface).await;
        let _lifecycle = lifecycle_lock.lock().await;
        self.restart_surface_locked(surface).await
    }

    async fn restart_surface_locked(
        &self,
        surface: String,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        let _ = self.stop_surface_locked(surface.clone()).await?;
        let mut snapshot = self.runtime_snapshot(&surface).unwrap_or_else(|| {
            SurfaceRuntimeSnapshot::discovered(&surface, SurfaceLifecycle::Managed)
        });
        snapshot.status = SurfaceRuntimeStatus::Restarting;
        snapshot.restart_count = snapshot.restart_count.saturating_add(1);
        snapshot.active = false;
        snapshot.available_actions = managed_actions(false);
        self.set_runtime(snapshot).await;
        let descriptor = self
            .get(&surface)
            .ok_or_else(|| SurfaceError::Unavailable(surface.clone()))?;
        self.start_surface_locked(descriptor).await
    }

    pub(crate) async fn repair_surface(
        &self,
        surface: &str,
    ) -> Result<SurfaceRuntimeSnapshot, SurfaceError> {
        let surface = normalize_surface_id(surface);
        let lifecycle_lock = self.lifecycle_lock_for(&surface).await;
        let _lifecycle = lifecycle_lock.lock().await;
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
        self.restart_surface_locked(surface).await
    }

    pub(super) async fn managed_process(
        &self,
        surface: SurfaceDescriptor,
    ) -> Result<Arc<ManagedSurfaceProcess>, SurfaceError> {
        let lifecycle_lock = self.lifecycle_lock_for(&surface.id).await;
        let _lifecycle = lifecycle_lock.lock().await;
        self.managed_process_locked(surface).await
    }

    async fn managed_process_locked(
        &self,
        surface: SurfaceDescriptor,
    ) -> Result<Arc<ManagedSurfaceProcess>, SurfaceError> {
        if let Some(process) = self.managed.lock().await.get(&surface.id).cloned() {
            return Ok(process);
        }
        let snapshot = self.runtime_for_discovered(&surface.id, surface.lifecycle);
        if snapshot.circuit_open {
            return Err(SurfaceError::Invocation {
                surface: surface.id,
                reason: "surface circuit is open; manual repair is required".to_string(),
            });
        }
        if snapshot
            .next_retry_at
            .is_some_and(|next_retry_at| next_retry_at > Utc::now())
        {
            return Err(SurfaceError::Invocation {
                surface: surface.id,
                reason: "surface restart is already scheduled".to_string(),
            });
        }

        let factory = ManagedWorkerFactory::new(self.clone(), surface.clone());
        match factory.start_locked().await {
            Ok(ManagedWorkerStart::Existing(process)) => Ok(process),
            Ok(ManagedWorkerStart::Created(process)) => {
                if let Err(error) = spawn_managed_worker_supervisor(factory, process.clone()) {
                    self.rollback_managed_supervisor_admission(&surface.id, &process)
                        .await;
                    return Err(error);
                }
                Ok(process)
            }
            Err(error) => {
                let failure_kind = classify_managed_start_error(&error);
                if let Some(decision) = self
                    .record_managed_worker_failure(&surface, failure_kind, error.to_string())
                    .await
                {
                    if self.managed_supervisor_running() {
                        self.gateway_tasks
                            .open_owner(crate::runtime_host::task_set::GatewayTaskOwner::Surface(
                                surface.id.clone(),
                            ))
                            .await
                            .map_err(|open_error| SurfaceError::Invocation {
                                surface: surface.id.clone(),
                                reason: format!(
                                    "managed Surface restart owner admission failed: {open_error}"
                                ),
                            })?;
                        if let Err(spawn_error) =
                            spawn_managed_worker_supervisor_after_failure(factory, decision)
                        {
                            let report = self
                                .gateway_tasks
                                .close_owner_and_drain(
                                    crate::runtime_host::task_set::GatewayTaskOwner::Surface(
                                        surface.id.clone(),
                                    ),
                                    Duration::from_secs(5),
                                )
                                .await;
                            tracing::warn!(
                                surface = %surface.id,
                                error = %spawn_error,
                                forced_aborts = report.forced_aborts,
                                "managed Surface restart supervisor admission failed"
                            );
                        }
                    }
                }
                Err(error)
            }
        }
    }

    async fn rollback_managed_supervisor_admission(
        &self,
        surface: &str,
        process: &Arc<ManagedSurfaceProcess>,
    ) {
        {
            let mut processes = self.managed.lock().await;
            if processes
                .get(surface)
                .is_some_and(|current| Arc::ptr_eq(current, process))
            {
                processes.remove(surface);
            }
        }
        rollback_managed_worker_start(surface, process).await;
        let report = self
            .gateway_tasks
            .close_owner_and_drain(
                crate::runtime_host::task_set::GatewayTaskOwner::Surface(surface.to_string()),
                Duration::from_secs(5),
            )
            .await;
        if report.forced_aborts > 0 || report.panicked > 0 {
            tracing::error!(
                %surface,
                joined = report.joined,
                panicked = report.panicked,
                forced_aborts = report.forced_aborts,
                "managed Surface supervisor admission rollback required forced task cleanup"
            );
        }
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

    async fn publish_managed_worker_starting(&self, surface: &SurfaceDescriptor) {
        let mut snapshot = self.runtime_for_discovered(&surface.id, surface.lifecycle);
        snapshot.status = SurfaceRuntimeStatus::Starting;
        snapshot.active = false;
        snapshot.pid = None;
        snapshot.available_actions = managed_actions(false);
        self.set_runtime(snapshot).await;
    }

    async fn publish_managed_worker_ready(
        &self,
        surface: &SurfaceDescriptor,
        process: &ManagedSurfaceProcess,
    ) {
        let mut snapshot = self.runtime_for_discovered(&surface.id, surface.lifecycle);
        snapshot.status = SurfaceRuntimeStatus::Ready;
        snapshot.active = true;
        snapshot.pid = process.pid;
        snapshot.started_at = Some(process.started_at);
        snapshot.last_seen_at = Some(Utc::now());
        snapshot.consecutive_failures = 0;
        snapshot.circuit_open = false;
        snapshot.next_retry_at = None;
        snapshot.available_actions = managed_actions(false);
        self.set_runtime(snapshot).await;
    }

    async fn publish_managed_worker_shutdown(&self, surface: &SurfaceDescriptor) {
        let mut snapshot = self.runtime_for_discovered(&surface.id, surface.lifecycle);
        snapshot.status = SurfaceRuntimeStatus::Disabled;
        snapshot.active = false;
        snapshot.pid = None;
        snapshot.next_retry_at = None;
        snapshot.available_actions = vec![
            SurfaceSupervisorAction::Start,
            SurfaceSupervisorAction::Repair,
            SurfaceSupervisorAction::HealthCheck,
        ];
        self.set_runtime(snapshot).await;
    }

    fn managed_supervisor_running(&self) -> bool {
        self.monitor_is_running()
    }

    async fn detach_managed_worker(
        &self,
        surface: &str,
        process: &Arc<ManagedSurfaceProcess>,
    ) -> bool {
        let mut processes = self.managed.lock().await;
        if !processes
            .get(surface)
            .is_some_and(|current| Arc::ptr_eq(current, process))
        {
            return false;
        }
        processes.remove(surface);
        true
    }

    async fn restart_allowed(&self, surface: &str) -> bool {
        if !self.managed_supervisor_running() {
            return false;
        }
        if self.managed.lock().await.contains_key(surface) {
            return false;
        }
        self.runtime_snapshot(surface).is_some_and(|snapshot| {
            !matches!(
                snapshot.status,
                SurfaceRuntimeStatus::Disabled | SurfaceRuntimeStatus::CircuitOpen
            ) && !snapshot.circuit_open
                && snapshot
                    .next_retry_at
                    .is_none_or(|next_retry_at| next_retry_at <= Utc::now())
        })
    }

    async fn record_managed_worker_failure(
        &self,
        surface: &SurfaceDescriptor,
        kind: SurfaceFailureKind,
        message: impl Into<String>,
    ) -> Option<ManagedRestartDecision> {
        let message = message.into();
        let policy = &surface.health.repair;
        let now = Utc::now();
        let mut snapshot = self.runtime_for_discovered(&surface.id, surface.lifecycle);
        if snapshot
            .last_error
            .as_ref()
            .is_some_and(|last| restart_window_elapsed(policy, last.occurred_at, now))
        {
            snapshot.restart_count = 0;
            snapshot.consecutive_failures = 0;
        }
        snapshot.active = false;
        snapshot.pid = None;
        snapshot.consecutive_failures = snapshot.consecutive_failures.saturating_add(1);
        snapshot.last_health_at = Some(now);
        snapshot.last_error = Some(SurfaceRuntimeError::new(kind, message.clone()));

        if snapshot.restart_count >= policy.restart_limit {
            snapshot.status = SurfaceRuntimeStatus::CircuitOpen;
            snapshot.circuit_open = true;
            snapshot.next_retry_at = Some(
                now + chrono::Duration::milliseconds(
                    policy.circuit_half_open_after_ms.min(i64::MAX as u64) as i64,
                ),
            );
            snapshot.available_actions = managed_actions(true);
            self.set_runtime(snapshot).await;
            self.push_ledger(SurfaceSupervisorEvent::error(
                &surface.id,
                SurfaceRuntimeStatus::CircuitOpen,
                SurfaceRuntimeError::new(kind, message),
            ))
            .await;
            return None;
        }

        snapshot.restart_count = snapshot.restart_count.saturating_add(1);
        let delay = backoff_duration(policy, snapshot.restart_count);
        let delay = delay.to_std().unwrap_or(Duration::ZERO);
        let next_retry_at =
            now + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::zero());
        snapshot.status = SurfaceRuntimeStatus::Restarting;
        snapshot.circuit_open = false;
        snapshot.next_retry_at = Some(next_retry_at);
        snapshot.available_actions = managed_actions(false);
        self.set_runtime(snapshot).await;
        self.push_ledger(SurfaceSupervisorEvent::error(
            &surface.id,
            SurfaceRuntimeStatus::Restarting,
            SurfaceRuntimeError::new(kind, message),
        ))
        .await;
        Some(ManagedRestartDecision {
            delay,
            next_retry_at,
        })
    }
}

fn spawn_managed_worker_supervisor(
    factory: ManagedWorkerFactory,
    process: Arc<ManagedSurfaceProcess>,
) -> Result<(), SurfaceError> {
    let surface_id = factory.descriptor.id.clone();
    factory
        .host
        .gateway_tasks()
        .spawn_owned(
            crate::runtime_host::task_set::GatewayTaskKind::SurfaceSupervisor,
            crate::runtime_host::task_set::GatewayTaskOwner::Surface(surface_id.clone()),
            move |cancellation| async move {
                tokio::select! {
                    _ = cancellation.cancelled() => {}
                    _ = run_managed_worker_supervisor(factory, Some(process), None) => {}
                }
            },
        )
        .map(|_| ())
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id,
            reason: format!("managed Surface supervisor admission failed: {error}"),
        })
}

fn spawn_managed_worker_supervisor_after_failure(
    factory: ManagedWorkerFactory,
    decision: ManagedRestartDecision,
) -> Result<(), SurfaceError> {
    let surface_id = factory.descriptor.id.clone();
    factory
        .host
        .gateway_tasks()
        .spawn_owned(
            crate::runtime_host::task_set::GatewayTaskKind::SurfaceSupervisor,
            crate::runtime_host::task_set::GatewayTaskOwner::Surface(surface_id.clone()),
            move |cancellation| async move {
                tokio::select! {
                    _ = cancellation.cancelled() => {}
                    _ = run_managed_worker_supervisor(factory, None, Some(decision)) => {}
                }
            },
        )
        .map(|_| ())
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id,
            reason: format!("managed Surface restart supervisor admission failed: {error}"),
        })
}

async fn run_managed_worker_supervisor(
    factory: ManagedWorkerFactory,
    initial_process: Option<Arc<ManagedSurfaceProcess>>,
    initial_decision: Option<ManagedRestartDecision>,
) {
    let mut process = initial_process;
    let mut pending_decision = initial_decision;
    loop {
        let decision = if let Some(current) = process.take() {
            let status = wait_for_managed_child(&current.child).await;
            if !factory
                .host
                .detach_managed_worker(&factory.descriptor.id, &current)
                .await
            {
                return;
            }
            cleanup_runtime_dir(&factory.descriptor.id, &current.runtime_dir);
            let restart_intended = factory
                .host
                .runtime_snapshot(&factory.descriptor.id)
                .is_some_and(|snapshot| snapshot.status != SurfaceRuntimeStatus::Disabled);
            if !factory.host.managed_supervisor_running() || !restart_intended {
                factory
                    .host
                    .publish_managed_worker_shutdown(&factory.descriptor)
                    .await;
                return;
            }
            let message = match status {
                Ok(status) => format!("managed surface exited with status {status}"),
                Err(error) => format!("managed surface wait failed: {error}"),
            };
            factory
                .host
                .record_managed_worker_failure(
                    &factory.descriptor,
                    SurfaceFailureKind::ProcessExited,
                    message,
                )
                .await
        } else {
            pending_decision
                .take()
                .or_else(|| pending_restart_decision(&factory.host, &factory.descriptor.id))
        };

        let Some(decision) = decision else {
            return;
        };
        tracing::warn!(
            surface = %factory.descriptor.id,
            retry_at = %decision.next_retry_at,
            delay_ms = decision.delay.as_millis(),
            "managed surface worker failed; scheduling supervised restart"
        );

        let gate_host = factory.host.clone();
        let gate_surface = factory.descriptor.id.clone();
        let restarted = restart_with_factory(
            decision.delay,
            move || {
                let gate_host = gate_host.clone();
                let gate_surface = gate_surface.clone();
                async move { gate_host.restart_allowed(&gate_surface).await }
            },
            || factory.start(),
        )
        .await;
        match restarted {
            None => {
                let stopped = !factory.host.managed_supervisor_running()
                    || factory
                        .host
                        .runtime_snapshot(&factory.descriptor.id)
                        .is_some_and(|snapshot| snapshot.status == SurfaceRuntimeStatus::Disabled);
                if stopped {
                    factory
                        .host
                        .publish_managed_worker_shutdown(&factory.descriptor)
                        .await;
                }
                return;
            }
            Some(Ok(ManagedWorkerStart::Existing(_))) => return,
            Some(Ok(ManagedWorkerStart::Created(next))) => {
                push_supervisor_event(
                    &factory.host.ledger,
                    SurfaceSupervisorEvent::new(
                        &factory.descriptor.id,
                        SurfaceRuntimeStatus::Ready,
                        "managed surface restarted by supervisor",
                    ),
                )
                .await;
                process = Some(next);
            }
            Some(Err(error)) => {
                let failure_kind = classify_managed_start_error(&error);
                if factory
                    .host
                    .record_managed_worker_failure(
                        &factory.descriptor,
                        failure_kind,
                        error.to_string(),
                    )
                    .await
                    .is_none()
                {
                    return;
                }
            }
        }
    }
}

fn pending_restart_decision(host: &SurfaceHost, surface: &str) -> Option<ManagedRestartDecision> {
    let snapshot = host.runtime_snapshot(surface)?;
    if snapshot.status != SurfaceRuntimeStatus::Restarting || snapshot.circuit_open {
        return None;
    }
    let now = Utc::now();
    let next_retry_at = snapshot.next_retry_at.unwrap_or(now);
    let delay = next_retry_at
        .signed_duration_since(now)
        .to_std()
        .unwrap_or(Duration::ZERO);
    Some(ManagedRestartDecision {
        delay,
        next_retry_at,
    })
}

async fn restart_with_factory<T, E, Allowed, AllowedFuture, Factory, FactoryFuture>(
    delay: Duration,
    mut allowed: Allowed,
    factory: Factory,
) -> Option<Result<T, E>>
where
    Allowed: FnMut() -> AllowedFuture,
    AllowedFuture: std::future::Future<Output = bool>,
    Factory: FnOnce() -> FactoryFuture,
    FactoryFuture: std::future::Future<Output = Result<T, E>>,
{
    if !allowed().await {
        return None;
    }
    let mut remaining = delay;
    while !remaining.is_zero() {
        let slice = remaining.min(Duration::from_millis(50));
        tokio::time::sleep(slice).await;
        remaining = remaining.saturating_sub(slice);
        if !allowed().await {
            return None;
        }
    }
    Some(factory().await)
}

fn restart_window_elapsed(
    policy: &SurfaceRepairPolicy,
    previous_failure: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> bool {
    now.signed_duration_since(previous_failure)
        .num_milliseconds()
        > policy.restart_window_ms.min(i64::MAX as u64) as i64
}

fn classify_managed_start_error(error: &SurfaceError) -> SurfaceFailureKind {
    super::classify_surface_error(error)
}

async fn rollback_managed_worker_start(surface: &str, process: &ManagedSurfaceProcess) {
    rollback_managed_worker_parts(surface, &process.child, &process.runtime_dir).await;
}

async fn rollback_managed_worker_parts(
    surface: &str,
    child: &Arc<AsyncMutex<tokio::process::Child>>,
    runtime_dir: &Path,
) {
    if let Err(error) = terminate_managed_child(child).await {
        tracing::error!(
            %surface,
            %error,
            "failed to terminate managed surface during startup rollback"
        );
    }
    cleanup_runtime_dir(surface, runtime_dir);
}

fn cleanup_runtime_dir(surface: &str, runtime_dir: &Path) {
    if let Err(error) = std::fs::remove_dir_all(runtime_dir) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                %surface,
                path = %runtime_dir.display(),
                %error,
                "managed surface runtime directory cleanup failed"
            );
        }
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
    event_tx: broadcast::Sender<SurfaceFrame>,
    messages: Arc<dyn surface::SurfaceMessageLedger>,
    gateway_tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
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
    let state_mode = surface
        .runtime
        .as_ref()
        .map(surface::SurfaceRuntimeSpec::state_mode)
        .unwrap_or_default();
    let state_dir = match state_mode {
        SurfaceStateMode::Ephemeral => runtime_dir.join("state"),
        SurfaceStateMode::Persistent => persistent_surface_state_dir(&surface_id),
    };
    create_private_dir(&state_dir).map_err(|error| {
        let _ = std::fs::remove_dir_all(&runtime_dir);
        SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to create managed edge state directory: {error}"),
        }
    })?;
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
    if state_mode == SurfaceStateMode::Persistent {
        sandbox.writable_roots.push(state_dir.clone());
    }
    let program_args = vec![
        "--socket".to_string(),
        socket_path.display().to_string(),
        "--credential-file".to_string(),
        credential_path.display().to_string(),
        "--state-dir".to_string(),
        state_dir.display().to_string(),
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
    let Some(stdout) = child.stdout.take() else {
        cleanup_failed_managed_start(&surface_id, &mut child, &runtime_dir).await;
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: "managed sidecar stdout is not available".to_string(),
        });
    };
    let Some(stderr) = child.stderr.take() else {
        cleanup_failed_managed_start(&surface_id, &mut child, &runtime_dir).await;
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: "managed sidecar stderr is not available".to_string(),
        });
    };
    if let Err(error) = spawn_child_log_drain(
        Arc::clone(&gateway_tasks),
        surface_id.clone(),
        "stdout",
        stdout,
    ) {
        cleanup_failed_managed_start(&surface_id, &mut child, &runtime_dir).await;
        return Err(error);
    }
    if let Err(error) = spawn_child_log_drain(
        Arc::clone(&gateway_tasks),
        surface_id.clone(),
        "stderr",
        stderr,
    ) {
        cleanup_failed_managed_start(&surface_id, &mut child, &runtime_dir).await;
        return Err(error);
    }

    let client = match EdgeH2Client::connect(
        &socket_path,
        &surface_id,
        &token,
        Arc::clone(&gateway_tasks),
    )
    .await
    {
        Ok(client) => client,
        Err(error) => {
            cleanup_failed_managed_start(&surface_id, &mut child, &runtime_dir).await;
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
            cleanup_failed_managed_start(&surface_id, &mut child, &runtime_dir).await;
            return Err(error);
        }
    };
    if bootstrap_response.surface_id != surface_id
        || bootstrap_response.driver_profile != driver_profile
    {
        cleanup_failed_managed_start(&surface_id, &mut child, &runtime_dir).await;
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: "managed edge bootstrap identity mismatch".to_string(),
        });
    }
    let child = Arc::new(AsyncMutex::new(child));
    let events = Arc::new(AsyncMutex::new(VecDeque::new()));
    if let Err(error) = client.spawn_event_stream(events.clone(), event_tx, messages) {
        if let Err(cleanup_error) = terminate_managed_child(&child).await {
            tracing::error!(
                surface = %surface_id,
                error = %cleanup_error,
                "failed to terminate managed Surface after event stream admission failure"
            );
        }
        cleanup_runtime_dir(&surface_id, &runtime_dir);
        return Err(error);
    }
    Ok(ManagedSurfaceProcess {
        pid,
        started_at,
        client,
        child,
        events,
        runtime_dir,
    })
}

async fn wait_for_managed_child(
    child: &Arc<AsyncMutex<tokio::process::Child>>,
) -> std::io::Result<std::process::ExitStatus> {
    loop {
        if let Some(status) = child.lock().await.try_wait()? {
            return Ok(status);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn terminate_managed_child(
    child: &Arc<AsyncMutex<tokio::process::Child>>,
) -> Result<(), String> {
    let mut child = child.lock().await;
    terminate_child_process(&mut child).await
}

async fn terminate_child_process(child: &mut tokio::process::Child) -> Result<(), String> {
    const GRACEFUL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
    const FORCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
    if child
        .try_wait()
        .map_err(|error| format!("failed to inspect managed surface process: {error}"))?
        .is_some()
    {
        return Ok(());
    }
    let pid = child
        .id()
        .ok_or_else(|| "managed surface process has no process id".to_string())?;
    let term_status = TokioCommand::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await
        .map_err(|error| format!("failed to send SIGTERM to managed surface process: {error}"))?;
    if !term_status.success() {
        return Err(format!(
            "SIGTERM command failed for managed surface process {pid}: {term_status}"
        ));
    }

    let graceful_exit = tokio::time::timeout(GRACEFUL_TIMEOUT, async {
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    match graceful_exit {
        Ok(Ok(_)) => return Ok(()),
        Ok(Err(error)) => {
            return Err(format!(
                "failed while waiting for managed surface process termination: {error}"
            ));
        }
        Err(_) => {}
    }

    child
        .start_kill()
        .map_err(|error| format!("failed to force-kill managed surface process: {error}"))?;
    match tokio::time::timeout(FORCE_TIMEOUT, child.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(format!(
            "failed while waiting for forced managed surface process termination: {error}"
        )),
        Err(_) => Err(format!(
            "managed surface process did not terminate after SIGTERM and SIGKILL within {} ms",
            (GRACEFUL_TIMEOUT + FORCE_TIMEOUT).as_millis()
        )),
    }
}

async fn cleanup_failed_managed_start(
    surface: &str,
    child: &mut tokio::process::Child,
    runtime_dir: &Path,
) {
    if let Err(error) = terminate_child_process(child).await {
        tracing::error!(
            %surface,
            %error,
            "failed to terminate managed surface after startup failure"
        );
    }
    if let Err(error) = std::fs::remove_dir_all(runtime_dir) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                %surface,
                path = %runtime_dir.display(),
                %error,
                "failed to clean managed surface runtime directory after startup failure"
            );
        }
    }
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

fn persistent_surface_state_dir(surface: &str) -> PathBuf {
    let root = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".local").join("state"))
        })
        .unwrap_or_else(|| std::env::temp_dir().join("cowd-state"));
    root.join("cowd")
        .join("edge")
        .join(normalize_surface_id(surface))
}

fn create_private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

fn spawn_child_log_drain<R>(
    gateway_tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
    surface: String,
    stream: &'static str,
    reader: R,
) -> Result<(), SurfaceError>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let owner = surface.clone();
    gateway_tasks
        .spawn_owned(
            crate::runtime_host::task_set::GatewayTaskKind::SurfaceTransport,
            crate::runtime_host::task_set::GatewayTaskOwner::Surface(owner.clone()),
            move |cancellation| async move {
                let mut lines = BufReader::new(reader).lines();
                loop {
                    let line = tokio::select! {
                        _ = cancellation.cancelled() => break,
                        line = lines.next_line() => line,
                    };
                    let Ok(Some(line)) = line else {
                        break;
                    };
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
            },
        )
        .map(|_| ())
        .map_err(|error| SurfaceError::Invocation {
            surface: owner,
            reason: format!("managed Surface log drain admission failed: {error}"),
        })
}

#[cfg(test)]
mod tests {
    use super::{
        default_source_surface_config, restart_with_factory, rollback_managed_worker_parts,
        stage_managed_artifact, terminate_managed_child,
    };
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;
    use surface::{
        SurfaceDescriptor, SurfaceFailureKind, SurfaceHealthMode, SurfaceHealthSpec, SurfaceKind,
        SurfaceManifest, SurfaceRepairPolicy, SurfaceRuntimeSnapshot, SurfaceRuntimeSpec,
        SurfaceRuntimeStatus, SurfaceStateMode, SurfaceTransport,
    };
    use tokio::sync::Mutex;

    fn managed_fixture(restart_limit: u32) -> SurfaceDescriptor {
        SurfaceDescriptor::from_manifest(
            &SurfaceManifest {
                schema: surface::SURFACE_PROTOCOL.to_string(),
                id: "supervised-fixture".to_string(),
                name: "Supervised Fixture".to_string(),
                version: "1.0.0".to_string(),
                kind: SurfaceKind::ExternalIntegration,
                runtime: Some(SurfaceRuntimeSpec::Managed {
                    artifact: "fixture".to_string(),
                    driver_profile: "fixture".to_string(),
                    transport: SurfaceTransport::UdsHttp2,
                    state: SurfaceStateMode::Ephemeral,
                }),
                capabilities: Vec::new(),
                routes: Vec::new(),
                resources: Vec::new(),
                health: SurfaceHealthSpec {
                    mode: SurfaceHealthMode::Jsonl,
                    interval_ms: 1,
                    timeout_ms: 1,
                    repair: SurfaceRepairPolicy {
                        failure_threshold: 1,
                        restart_limit,
                        restart_window_ms: 60_000,
                        backoff_initial_ms: 0,
                        backoff_max_ms: 0,
                        circuit_half_open_after_ms: 60_000,
                    },
                },
                config_schema: serde_json::Value::Null,
                default_enabled: true,
            },
            "fixture/surface.json",
        )
    }

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

    #[tokio::test]
    async fn managed_child_termination_waits_for_the_process_to_exit() {
        let child = tokio::process::Command::new("sh")
            .args(["-c", "sleep 30"])
            .kill_on_drop(true)
            .spawn()
            .expect("spawn managed child fixture");
        let child = Arc::new(Mutex::new(child));

        terminate_managed_child(&child)
            .await
            .expect("managed child should terminate");

        assert!(
            child
                .lock()
                .await
                .try_wait()
                .expect("inspect terminated child")
                .is_some(),
            "stop must not return before the managed process exits"
        );
    }

    #[tokio::test]
    async fn managed_child_termination_escalates_when_sigterm_is_ignored() {
        let child = tokio::process::Command::new("sh")
            .args(["-c", "trap '' TERM; while :; do sleep 1; done"])
            .kill_on_drop(true)
            .spawn()
            .expect("spawn TERM-resistant managed child fixture");
        let child = Arc::new(Mutex::new(child));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        terminate_managed_child(&child)
            .await
            .expect("managed child should be force-terminated");

        assert!(
            child
                .lock()
                .await
                .try_wait()
                .expect("inspect force-terminated child")
                .is_some(),
            "stop must escalate to SIGKILL when SIGTERM is ignored"
        );
    }

    #[tokio::test]
    async fn managed_supervisor_admission_rollback_terminates_child_and_removes_runtime_dir() {
        let root = tempfile::tempdir().expect("temporary rollback fixture");
        let runtime_dir = root.path().join("managed-runtime");
        std::fs::create_dir_all(&runtime_dir).expect("managed runtime directory");
        std::fs::write(runtime_dir.join("credential"), b"secret")
            .expect("managed runtime credential");
        let child = tokio::process::Command::new("sh")
            .args(["-c", "trap '' TERM; while :; do sleep 1; done"])
            .kill_on_drop(true)
            .spawn()
            .expect("spawn managed child fixture");
        let child = Arc::new(Mutex::new(child));
        tokio::time::sleep(Duration::from_millis(100)).await;

        rollback_managed_worker_parts("rollback-fixture", &child, &runtime_dir).await;

        assert!(
            child
                .lock()
                .await
                .try_wait()
                .expect("inspect rolled back child")
                .is_some(),
            "supervisor admission rollback must reap the already-started child"
        );
        assert!(
            !runtime_dir.exists(),
            "supervisor admission rollback must remove the private runtime directory"
        );
    }

    #[tokio::test]
    async fn supervised_restart_rebuilds_worker_through_factory() {
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = factory_calls.clone();

        let restarted = restart_with_factory(
            Duration::ZERO,
            || std::future::ready(true),
            move || {
                let observed_calls = observed_calls.clone();
                async move {
                    let epoch = observed_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok::<_, &'static str>(epoch)
                }
            },
        )
        .await;

        assert_eq!(restarted, Some(Ok(1)));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn managed_failure_records_bounded_restarts_and_last_failure() {
        let host = super::SurfaceHost::new(Vec::new());
        let descriptor = managed_fixture(2);

        let first = host
            .record_managed_worker_failure(
                &descriptor,
                SurfaceFailureKind::ProcessExited,
                "epoch one failed",
            )
            .await
            .expect("first restart");
        assert_eq!(first.delay, Duration::ZERO);
        let first_snapshot = host
            .runtime_snapshot(&descriptor.id)
            .expect("first observation");
        assert_eq!(first_snapshot.status, SurfaceRuntimeStatus::Restarting);
        assert_eq!(first_snapshot.restart_count, 1);
        assert_eq!(
            first_snapshot
                .last_error
                .as_ref()
                .map(|failure| failure.message.as_str()),
            Some("epoch one failed")
        );
        assert!(first_snapshot.next_retry_at.is_some());

        host.record_managed_worker_failure(
            &descriptor,
            SurfaceFailureKind::SpawnFailed,
            "epoch two failed",
        )
        .await
        .expect("second restart");
        let exhausted = host
            .record_managed_worker_failure(
                &descriptor,
                SurfaceFailureKind::SpawnFailed,
                "restart budget exhausted",
            )
            .await;
        assert!(exhausted.is_none());
        let final_snapshot = host
            .runtime_snapshot(&descriptor.id)
            .expect("final observation");
        assert_eq!(final_snapshot.status, SurfaceRuntimeStatus::CircuitOpen);
        assert_eq!(final_snapshot.restart_count, 2);
        assert_eq!(
            final_snapshot
                .last_error
                .as_ref()
                .map(|failure| failure.message.as_str()),
            Some("restart budget exhausted")
        );
        assert!(final_snapshot.next_retry_at.is_some());
    }

    #[tokio::test]
    async fn managed_restart_backoff_is_exponential_and_bounded() {
        let host = super::SurfaceHost::new(Vec::new());
        let mut descriptor = managed_fixture(4);
        descriptor.health.repair.backoff_initial_ms = 5;
        descriptor.health.repair.backoff_max_ms = 12;

        let first = host
            .record_managed_worker_failure(&descriptor, SurfaceFailureKind::ProcessExited, "first")
            .await
            .expect("first retry");
        let second = host
            .record_managed_worker_failure(&descriptor, SurfaceFailureKind::ProcessExited, "second")
            .await
            .expect("second retry");
        let third = host
            .record_managed_worker_failure(&descriptor, SurfaceFailureKind::ProcessExited, "third")
            .await
            .expect("third retry");

        assert_eq!(first.delay, Duration::from_millis(5));
        assert_eq!(second.delay, Duration::from_millis(10));
        assert_eq!(third.delay, Duration::from_millis(12));
    }

    #[tokio::test]
    async fn supervised_stop_gate_never_invokes_worker_factory() {
        let host = super::SurfaceHost::new(Vec::new());
        let descriptor = managed_fixture(1);
        let mut stopped = SurfaceRuntimeSnapshot::discovered(&descriptor.id, descriptor.lifecycle);
        stopped.status = SurfaceRuntimeStatus::Disabled;
        stopped.next_retry_at = Some(chrono::Utc::now());
        host.set_runtime(stopped).await;
        host.start_monitor().expect("start Surface monitor");
        host.shutdown().await.expect("surface host shutdown");

        let factory_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = factory_calls.clone();
        let gate_host = host.clone();
        let surface_id = descriptor.id.clone();

        let restarted = restart_with_factory(
            Duration::ZERO,
            move || {
                let gate_host = gate_host.clone();
                let surface_id = surface_id.clone();
                async move { gate_host.restart_allowed(&surface_id).await }
            },
            move || {
                let observed_calls = observed_calls.clone();
                async move {
                    observed_calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, &'static str>(())
                }
            },
        )
        .await;

        assert_eq!(restarted, None);
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    }
}
