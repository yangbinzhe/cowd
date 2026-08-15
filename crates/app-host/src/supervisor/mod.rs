mod connector;
mod types;

#[cfg(test)]
mod tests;

pub use connector::{AppWorkerConnector, ConnectorFuture};
pub use types::{AppRuntimeLogs, AppRuntimeStatus, AppRuntimeSupervisorConfig, SupervisorError};

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use cowd_app_protocol::{AppActivationPolicyV1, AppId, AppLifecycleStateV1, GenerationId};
use managed_worker_runtime::{CancellationToken, ManagedWorkerHandle, ManagedWorkerSpec};
use tokio::{
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

use crate::catalog::{AdmittedApp, AppCatalogSnapshot};

const MONITOR_INTERVAL: Duration = Duration::from_millis(20);

pub struct AppRuntimeSupervisor<C: AppWorkerConnector> {
    inner: Arc<SupervisorInner<C>>,
}

struct SupervisorInner<C: AppWorkerConnector> {
    catalog: Arc<AppCatalogSnapshot>,
    connector: Arc<C>,
    config: AppRuntimeSupervisorConfig,
    apps: BTreeMap<AppId, Arc<AppSlot<C::Connection>>>,
    starting: Arc<Semaphore>,
    active: Arc<Semaphore>,
    shutting_down: AtomicBool,
    shutdown_complete: AtomicBool,
    shutdown_changed: Notify,
}

struct AppSlot<T> {
    admitted: AdmittedApp,
    state: Mutex<SlotState<T>>,
    changed: Notify,
}

struct SlotState<T> {
    lifecycle: AppLifecycleStateV1,
    reason: Option<String>,
    worker: Option<ManagedWorkerHandle>,
    connection: Option<Arc<T>>,
    active_permit: Option<OwnedSemaphorePermit>,
    last_logs: Option<AppRuntimeLogs>,
    active_leases: usize,
    waiters: usize,
    idle_since: Option<Instant>,
    failures: VecDeque<Instant>,
    retry_at: Option<Instant>,
    restart_count: u32,
    startup_cancel: Option<CancellationToken>,
}

impl<T> SlotState<T> {
    fn mounted() -> Self {
        Self {
            lifecycle: AppLifecycleStateV1::Mounted,
            reason: None,
            worker: None,
            connection: None,
            active_permit: None,
            last_logs: None,
            active_leases: 0,
            waiters: 0,
            idle_since: None,
            failures: VecDeque::new(),
            retry_at: None,
            restart_count: 0,
            startup_cancel: None,
        }
    }
}

impl<C: AppWorkerConnector> Clone for AppRuntimeSupervisor<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct AppRuntimeLease<C: AppWorkerConnector> {
    supervisor: Weak<SupervisorInner<C>>,
    app_id: AppId,
    generation: GenerationId,
    connection: Arc<C::Connection>,
    released: bool,
}

impl<C: AppWorkerConnector> AppRuntimeLease<C> {
    #[must_use]
    pub fn app_id(&self) -> &AppId {
        &self.app_id
    }

    #[must_use]
    pub fn generation(&self) -> &GenerationId {
        &self.generation
    }

    #[must_use]
    pub fn connection(&self) -> &C::Connection {
        &self.connection
    }

    pub async fn release(mut self) {
        self.release_inner().await;
    }

    async fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        if let Some(supervisor) = self.supervisor.upgrade() {
            supervisor.release(&self.app_id, &self.generation).await;
        }
    }
}

impl<C: AppWorkerConnector> Drop for AppRuntimeLease<C> {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let supervisor = self.supervisor.clone();
        let app_id = self.app_id.clone();
        let generation = self.generation.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(supervisor) = supervisor.upgrade() {
                    supervisor.release(&app_id, &generation).await;
                }
            });
        }
    }
}

impl<C: AppWorkerConnector> AppRuntimeSupervisor<C> {
    pub fn new(
        catalog: Arc<AppCatalogSnapshot>,
        connector: Arc<C>,
        config: AppRuntimeSupervisorConfig,
    ) -> Result<Self, SupervisorError> {
        config.validate()?;
        let apps = catalog
            .apps()
            .map(|app| {
                (
                    app.manifest.app_id.clone(),
                    Arc::new(AppSlot {
                        admitted: app.clone(),
                        state: Mutex::new(SlotState::mounted()),
                        changed: Notify::new(),
                    }),
                )
            })
            .collect();
        let inner = Arc::new(SupervisorInner {
            catalog,
            connector,
            starting: Arc::new(Semaphore::new(config.max_starting_workers)),
            active: Arc::new(Semaphore::new(config.max_active_workers)),
            config,
            apps,
            shutting_down: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            shutdown_changed: Notify::new(),
        });
        if inner.config.idle_ttl.is_some() {
            let weak = Arc::downgrade(&inner);
            tokio::runtime::Handle::try_current()
                .map_err(|_| {
                    SupervisorError::InvalidConfiguration(
                        "idle TTL requires an active Tokio runtime".to_owned(),
                    )
                })?
                .spawn(async move { SupervisorInner::idle_loop(weak).await });
        }
        Ok(Self { inner })
    }

    #[must_use]
    pub fn catalog(&self) -> &Arc<AppCatalogSnapshot> {
        &self.inner.catalog
    }

    pub async fn start_resident(&self) -> Result<(), SupervisorError> {
        let mut required_failures = Vec::new();
        for app in self
            .inner
            .catalog
            .apps()
            .filter(|app| matches!(app.policy.activation, AppActivationPolicyV1::Resident))
        {
            let result = self
                .activate(
                    &app.manifest.app_id,
                    &app.generation,
                    self.inner
                        .config
                        .activation_timeout
                        .saturating_add(Duration::from_millis(100)),
                    &CancellationToken::default(),
                    false,
                )
                .await;
            if result.is_err() && app.policy.required {
                required_failures.push(app.manifest.app_id.clone());
            }
        }
        if required_failures.is_empty() {
            Ok(())
        } else {
            Err(SupervisorError::RequiredResidentsFailed(required_failures))
        }
    }

    pub async fn acquire(
        &self,
        app_id: &AppId,
        generation: &GenerationId,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<AppRuntimeLease<C>, SupervisorError> {
        let connection = self
            .activate(app_id, generation, timeout, cancellation, true)
            .await?
            .ok_or_else(|| SupervisorError::Worker {
                app_id: app_id.clone(),
                detail: "activation completed without a connection".to_owned(),
            })?;
        Ok(AppRuntimeLease {
            supervisor: Arc::downgrade(&self.inner),
            app_id: app_id.clone(),
            generation: generation.clone(),
            connection,
            released: false,
        })
    }

    async fn activate(
        &self,
        app_id: &AppId,
        generation: &GenerationId,
        timeout: Duration,
        cancellation: &CancellationToken,
        lease: bool,
    ) -> Result<Option<Arc<C::Connection>>, SupervisorError> {
        self.inner
            .activate(app_id, generation, timeout, cancellation, lease)
            .await
    }

    pub async fn status(&self, app_id: &AppId) -> Result<AppRuntimeStatus, SupervisorError> {
        self.inner.status(app_id).await
    }

    pub async fn statuses(&self) -> Vec<AppRuntimeStatus> {
        let mut statuses = Vec::with_capacity(self.inner.apps.len());
        for app_id in self.inner.apps.keys() {
            if let Ok(status) = self.inner.status(app_id).await {
                statuses.push(status);
            }
        }
        statuses
    }

    pub async fn logs(&self, app_id: &AppId) -> Result<AppRuntimeLogs, SupervisorError> {
        let slot = self.inner.slot(app_id)?;
        let (worker, last_logs) = {
            let state = slot.state.lock().await;
            (state.worker.clone(), state.last_logs.clone())
        };
        if let Some(worker) = worker {
            Ok(AppRuntimeLogs {
                stdout: worker.stdout().await,
                stderr: worker.stderr().await,
            })
        } else {
            last_logs.ok_or_else(|| SupervisorError::Worker {
                app_id: app_id.clone(),
                detail: "worker has not produced logs".to_owned(),
            })
        }
    }

    pub async fn health(
        &self,
        app_id: &AppId,
        generation: &GenerationId,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<(), SupervisorError> {
        self.inner.ensure_generation(app_id, generation)?;
        let slot = self.inner.slot(app_id)?;
        let (worker, connection) = {
            let state = slot.state.lock().await;
            (
                state
                    .worker
                    .clone()
                    .ok_or_else(|| SupervisorError::Worker {
                        app_id: app_id.clone(),
                        detail: "worker is not active".to_owned(),
                    })?,
                state
                    .connection
                    .clone()
                    .ok_or_else(|| SupervisorError::Worker {
                        app_id: app_id.clone(),
                        detail: "worker connection is not active".to_owned(),
                    })?,
            )
        };
        let check = self
            .inner
            .connector
            .health(&slot.admitted, &worker, &connection, cancellation);
        tokio::select! {
            () = cancellation.cancelled() => Err(SupervisorError::Cancelled),
            result = tokio::time::timeout(timeout, check) => {
                result.map_err(|_| SupervisorError::DeadlineExceeded(timeout))?
            }
        }
    }

    pub async fn restart(
        &self,
        app_id: &AppId,
        generation: &GenerationId,
        cancellation: &CancellationToken,
    ) -> Result<(), SupervisorError> {
        self.inner.ensure_generation(app_id, generation)?;
        {
            let slot = self.inner.slot(app_id)?;
            let mut state = slot.state.lock().await;
            state.failures.clear();
            state.retry_at = None;
            state.restart_count = 0;
        }
        self.inner.stop_one(app_id).await?;
        self.activate(
            app_id,
            generation,
            self.inner.config.activation_timeout,
            cancellation,
            false,
        )
        .await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        self.inner.shutdown().await
    }
}

impl<C: AppWorkerConnector> SupervisorInner<C> {
    fn slot(&self, app_id: &AppId) -> Result<&Arc<AppSlot<C::Connection>>, SupervisorError> {
        self.apps
            .get(app_id)
            .ok_or_else(|| SupervisorError::UnknownApp(app_id.clone()))
    }

    fn ensure_generation(
        &self,
        app_id: &AppId,
        observed: &GenerationId,
    ) -> Result<(), SupervisorError> {
        let expected = &self.slot(app_id)?.admitted.generation;
        if expected == observed {
            Ok(())
        } else {
            Err(SupervisorError::StaleGeneration {
                app_id: app_id.clone(),
                expected: expected.clone(),
                observed: observed.clone(),
            })
        }
    }

    async fn activate(
        self: &Arc<Self>,
        app_id: &AppId,
        generation: &GenerationId,
        timeout: Duration,
        cancellation: &CancellationToken,
        lease: bool,
    ) -> Result<Option<Arc<C::Connection>>, SupervisorError> {
        self.ensure_generation(app_id, generation)?;
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(SupervisorError::ShuttingDown);
        }
        let slot = Arc::clone(self.slot(app_id)?);
        let deadline = Instant::now() + timeout;
        loop {
            if self.shutting_down.load(Ordering::Acquire) {
                return Err(SupervisorError::ShuttingDown);
            }
            let mut state = slot.state.lock().await;
            match state.lifecycle {
                AppLifecycleStateV1::Ready | AppLifecycleStateV1::Idle => {
                    let connection =
                        state
                            .connection
                            .clone()
                            .ok_or_else(|| SupervisorError::Worker {
                                app_id: app_id.clone(),
                                detail: "ready state has no connection".to_owned(),
                            })?;
                    if lease {
                        state.active_leases = state.active_leases.saturating_add(1);
                        state.lifecycle = AppLifecycleStateV1::Ready;
                        state.idle_since = None;
                    }
                    return Ok(Some(connection));
                }
                AppLifecycleStateV1::Starting | AppLifecycleStateV1::Stopping => {
                    if state.waiters >= self.config.max_waiters_per_app {
                        return Err(SupervisorError::WaiterOverloaded(app_id.clone()));
                    }
                    state.waiters += 1;
                    let wait = slot.changed.notified();
                    drop(state);
                    let result = tokio::select! {
                        () = cancellation.cancelled() => Err(SupervisorError::Cancelled),
                        result = tokio::time::timeout_at(deadline, wait) => {
                            result.map_err(|_| SupervisorError::DeadlineExceeded(timeout))
                        }
                    };
                    let mut state = slot.state.lock().await;
                    state.waiters = state.waiters.saturating_sub(1);
                    if result.is_err()
                        && state.waiters == 0
                        && state.lifecycle == AppLifecycleStateV1::Starting
                        && matches!(slot.admitted.policy.activation, AppActivationPolicyV1::Lazy)
                    {
                        if let Some(startup_cancel) = state.startup_cancel.take() {
                            startup_cancel.cancel();
                        }
                        state.lifecycle = AppLifecycleStateV1::Mounted;
                        state.reason = None;
                    }
                    result?;
                    continue;
                }
                AppLifecycleStateV1::CircuitOpen => {
                    return Err(SupervisorError::CircuitOpen(app_id.clone()));
                }
                AppLifecycleStateV1::Failed => {
                    if let Some(retry_at) = state.retry_at {
                        if retry_at > Instant::now() {
                            return Err(SupervisorError::BackingOff {
                                app_id: app_id.clone(),
                                retry_after: retry_at.saturating_duration_since(Instant::now()),
                            });
                        }
                    }
                    state.lifecycle = AppLifecycleStateV1::Starting;
                    state.reason = None;
                    let startup_cancel = CancellationToken::default();
                    state.startup_cancel = Some(startup_cancel.clone());
                    drop(state);
                    self.spawn_start(
                        Arc::clone(&slot),
                        app_id.clone(),
                        generation.clone(),
                        startup_cancel,
                    );
                    continue;
                }
                AppLifecycleStateV1::Mounted | AppLifecycleStateV1::Stopped => {
                    state.lifecycle = AppLifecycleStateV1::Starting;
                    state.reason = None;
                    let startup_cancel = CancellationToken::default();
                    state.startup_cancel = Some(startup_cancel.clone());
                    drop(state);
                    self.spawn_start(
                        Arc::clone(&slot),
                        app_id.clone(),
                        generation.clone(),
                        startup_cancel,
                    );
                    continue;
                }
                _ => {
                    return Err(SupervisorError::Worker {
                        app_id: app_id.clone(),
                        detail: format!("application cannot activate from {:?}", state.lifecycle),
                    });
                }
            }
        }
    }

    fn spawn_start(
        self: &Arc<Self>,
        slot: Arc<AppSlot<C::Connection>>,
        app_id: AppId,
        generation: GenerationId,
        startup_cancel: CancellationToken,
    ) {
        let supervisor = Arc::clone(self);
        tokio::spawn(async move {
            let mut resident_retry = None;
            let timeout = supervisor.config.activation_timeout;
            let deadline = Instant::now() + timeout;
            let result = supervisor
                .start_worker(
                    &slot,
                    &app_id,
                    &generation,
                    deadline,
                    timeout,
                    &startup_cancel,
                )
                .await;
            match result {
                Ok((worker, connection, active_permit)) => {
                    let connection = Arc::new(connection);
                    let accepted = {
                        let mut state = slot.state.lock().await;
                        state.startup_cancel.take();
                        if state.lifecycle != AppLifecycleStateV1::Starting
                            || supervisor.shutting_down.load(Ordering::Acquire)
                        {
                            false
                        } else {
                            state.worker = Some(worker.clone());
                            state.connection = Some(Arc::clone(&connection));
                            state.active_permit = Some(active_permit);
                            state.lifecycle = if matches!(
                                slot.admitted.policy.activation,
                                AppActivationPolicyV1::Lazy
                            ) {
                                state.idle_since = Some(Instant::now());
                                AppLifecycleStateV1::Idle
                            } else {
                                AppLifecycleStateV1::Ready
                            };
                            state.reason = None;
                            state.retry_at = None;
                            true
                        }
                    };
                    if accepted {
                        supervisor.spawn_monitor(Arc::clone(&slot), worker, generation.clone());
                    } else {
                        let _ = worker.shutdown().await;
                    }
                }
                Err(error) => {
                    let should_record = {
                        let mut state = slot.state.lock().await;
                        state.startup_cancel.take();
                        state.lifecycle == AppLifecycleStateV1::Starting
                            && !supervisor.shutting_down.load(Ordering::Acquire)
                    };
                    if should_record {
                        supervisor.record_failure(&slot, error.to_string()).await;
                        if matches!(
                            slot.admitted.policy.activation,
                            AppActivationPolicyV1::Resident
                        ) {
                            resident_retry = slot.state.lock().await.retry_at;
                        }
                    }
                }
            }
            slot.changed.notify_waiters();
            if let Some(retry_at) = resident_retry {
                tokio::time::sleep_until(retry_at).await;
                if !supervisor.shutting_down.load(Ordering::Acquire) {
                    let cancellation = CancellationToken::default();
                    let _ = supervisor
                        .activate(
                            &app_id,
                            &generation,
                            supervisor.config.activation_timeout,
                            &cancellation,
                            false,
                        )
                        .await;
                }
            }
        });
    }

    async fn start_worker(
        &self,
        slot: &AppSlot<C::Connection>,
        app_id: &AppId,
        generation: &GenerationId,
        deadline: Instant,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<(ManagedWorkerHandle, C::Connection, OwnedSemaphorePermit), SupervisorError> {
        let starting = self
            .acquire_permit(Arc::clone(&self.starting), deadline, timeout, cancellation)
            .await?;
        let active = self
            .acquire_permit(Arc::clone(&self.active), deadline, timeout, cancellation)
            .await?;
        let runtime_dir = self.config.runtime_root.join(&app_id.0).join(&generation.0);
        let mut spec =
            ManagedWorkerSpec::new(&slot.admitted.executable, runtime_dir, generation.0.clone());
        spec.startup_timeout = deadline.saturating_duration_since(Instant::now());
        spec.graceful_shutdown_timeout = self.config.shutdown_timeout;
        let spec = self.connector.configure(&slot.admitted, spec);
        let worker = tokio::select! {
            () = cancellation.cancelled() => return Err(SupervisorError::Cancelled),
            result = tokio::time::timeout_at(deadline, ManagedWorkerHandle::spawn(spec)) => {
                result.map_err(|_| SupervisorError::DeadlineExceeded(timeout))?
                    .map_err(|error| SupervisorError::Worker {
                        app_id: app_id.clone(),
                        detail: error.to_string(),
                    })?
            }
        };
        let connected = tokio::select! {
            () = cancellation.cancelled() => Err(SupervisorError::Cancelled),
            result = tokio::time::timeout_at(
                deadline,
                self.connector.connect(&slot.admitted, &worker, cancellation),
            ) => result.map_err(|_| SupervisorError::DeadlineExceeded(timeout))?,
        };
        drop(starting);
        match connected {
            Ok(connection) => Ok((worker, connection, active)),
            Err(error) => {
                let _ = worker.shutdown().await;
                Err(error)
            }
        }
    }

    async fn acquire_permit(
        &self,
        semaphore: Arc<Semaphore>,
        deadline: Instant,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<OwnedSemaphorePermit, SupervisorError> {
        tokio::select! {
            () = cancellation.cancelled() => Err(SupervisorError::Cancelled),
            result = tokio::time::timeout_at(deadline, semaphore.acquire_owned()) => {
                result.map_err(|_| SupervisorError::DeadlineExceeded(timeout))?
                    .map_err(|_| SupervisorError::ShuttingDown)
            }
        }
    }

    fn spawn_monitor(
        self: &Arc<Self>,
        slot: Arc<AppSlot<C::Connection>>,
        worker: ManagedWorkerHandle,
        generation: GenerationId,
    ) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                match worker.try_wait().await {
                    Ok(Some(exit)) => {
                        let _ = worker.shutdown().await;
                        if let Some(supervisor) = weak.upgrade() {
                            supervisor
                                .worker_exited(
                                    slot,
                                    &worker,
                                    generation,
                                    format!("worker exited code={:?}", exit.code),
                                )
                                .await;
                        }
                        return;
                    }
                    Ok(None) => tokio::time::sleep(MONITOR_INTERVAL).await,
                    Err(error) => {
                        let _ = worker.shutdown().await;
                        if let Some(supervisor) = weak.upgrade() {
                            supervisor
                                .worker_exited(slot, &worker, generation, error.to_string())
                                .await;
                        }
                        return;
                    }
                }
            }
        });
    }

    async fn worker_exited(
        self: Arc<Self>,
        slot: Arc<AppSlot<C::Connection>>,
        observed_worker: &ManagedWorkerHandle,
        generation: GenerationId,
        reason: String,
    ) {
        let logs = AppRuntimeLogs {
            stdout: observed_worker.stdout().await,
            stderr: observed_worker.stderr().await,
        };
        let should_restart = {
            let mut state = slot.state.lock().await;
            if state.worker.as_ref().map(ManagedWorkerHandle::pid) != Some(observed_worker.pid())
                || slot.admitted.generation != generation
                || matches!(
                    state.lifecycle,
                    AppLifecycleStateV1::Stopping | AppLifecycleStateV1::Stopped
                )
            {
                return;
            }
            state.worker.take();
            state.connection.take();
            state.active_permit.take();
            state.active_leases = 0;
            state.startup_cancel.take();
            state.last_logs = Some(logs);
            matches!(
                slot.admitted.policy.activation,
                AppActivationPolicyV1::Resident
            ) && !self.shutting_down.load(Ordering::Acquire)
        };
        self.record_failure(&slot, reason).await;
        slot.changed.notify_waiters();
        if should_restart {
            let retry_at = slot.state.lock().await.retry_at;
            if let Some(retry_at) = retry_at {
                tokio::time::sleep_until(retry_at).await;
            }
            if !self.shutting_down.load(Ordering::Acquire) {
                let cancellation = CancellationToken::default();
                let _ = self
                    .activate(
                        &slot.admitted.manifest.app_id,
                        &slot.admitted.generation,
                        self.config.activation_timeout,
                        &cancellation,
                        false,
                    )
                    .await;
            }
        }
    }

    async fn record_failure(&self, slot: &AppSlot<C::Connection>, reason: String) {
        let now = Instant::now();
        let mut state = slot.state.lock().await;
        while state
            .failures
            .front()
            .is_some_and(|failure| now.duration_since(*failure) > self.config.crash_window)
        {
            state.failures.pop_front();
        }
        state.failures.push_back(now);
        state.restart_count = state.restart_count.saturating_add(1);
        state.reason = Some(reason);
        state.worker.take();
        state.connection.take();
        state.active_permit.take();
        state.idle_since = None;
        state.startup_cancel.take();
        if state.failures.len() >= self.config.crash_budget {
            state.lifecycle = AppLifecycleStateV1::CircuitOpen;
            state.retry_at = None;
        } else {
            let shift = state.restart_count.saturating_sub(1).min(31);
            let factor = 1_u32 << shift;
            let backoff = self
                .config
                .restart_backoff_initial
                .saturating_mul(factor)
                .min(self.config.restart_backoff_maximum);
            state.lifecycle = AppLifecycleStateV1::Failed;
            state.retry_at = Some(now + backoff);
        }
    }

    async fn release(&self, app_id: &AppId, generation: &GenerationId) {
        let Ok(slot) = self.slot(app_id) else {
            return;
        };
        if &slot.admitted.generation != generation {
            return;
        }
        let mut state = slot.state.lock().await;
        state.active_leases = state.active_leases.saturating_sub(1);
        if state.active_leases == 0
            && state.worker.is_some()
            && matches!(slot.admitted.policy.activation, AppActivationPolicyV1::Lazy)
        {
            state.lifecycle = AppLifecycleStateV1::Idle;
            state.idle_since = Some(Instant::now());
        }
    }

    async fn idle_loop(weak: Weak<Self>) {
        loop {
            let Some(supervisor) = weak.upgrade() else {
                return;
            };
            tokio::time::sleep(supervisor.config.idle_scan_interval).await;
            if supervisor.shutting_down.load(Ordering::Acquire) {
                return;
            }
            let Some(ttl) = supervisor.config.idle_ttl else {
                return;
            };
            let app_ids = supervisor.apps.keys().cloned().collect::<Vec<_>>();
            for app_id in app_ids {
                let _ = supervisor.stop_idle(&app_id, ttl).await;
            }
        }
    }

    async fn stop_one(&self, app_id: &AppId) -> Result<(), SupervisorError> {
        let slot = self.slot(app_id)?;
        let worker = {
            let mut state = slot.state.lock().await;
            state.lifecycle = AppLifecycleStateV1::Stopping;
            state.reason = None;
            state.active_leases = 0;
            state.connection.take();
            if let Some(cancellation) = state.startup_cancel.take() {
                cancellation.cancel();
            }
            state.worker.take()
        };
        let (logs, shutdown_error) = if let Some(worker) = worker {
            let shutdown_error = worker.shutdown().await.err().map(|error| error.to_string());
            (
                Some(AppRuntimeLogs {
                    stdout: worker.stdout().await,
                    stderr: worker.stderr().await,
                }),
                shutdown_error,
            )
        } else {
            (None, None)
        };
        let mut state = slot.state.lock().await;
        if let Some(logs) = logs {
            state.last_logs = Some(logs);
        }
        state.active_permit.take();
        state.idle_since = None;
        state.lifecycle = AppLifecycleStateV1::Stopped;
        slot.changed.notify_waiters();
        if let Some(detail) = shutdown_error {
            Err(SupervisorError::Worker {
                app_id: app_id.clone(),
                detail,
            })
        } else {
            Ok(())
        }
    }

    async fn stop_idle(&self, app_id: &AppId, ttl: Duration) -> Result<(), SupervisorError> {
        let slot = self.slot(app_id)?;
        let worker = {
            let mut state = slot.state.lock().await;
            let expired = matches!(slot.admitted.policy.activation, AppActivationPolicyV1::Lazy)
                && state.active_leases == 0
                && state.lifecycle == AppLifecycleStateV1::Idle
                && state.idle_since.is_some_and(|since| since.elapsed() >= ttl);
            if !expired {
                return Ok(());
            }
            state.lifecycle = AppLifecycleStateV1::Stopping;
            state.connection.take();
            state.worker.take()
        };
        let (logs, shutdown_error) = if let Some(worker) = worker {
            let shutdown_error = worker.shutdown().await.err().map(|error| error.to_string());
            (
                Some(AppRuntimeLogs {
                    stdout: worker.stdout().await,
                    stderr: worker.stderr().await,
                }),
                shutdown_error,
            )
        } else {
            (None, None)
        };
        {
            let mut state = slot.state.lock().await;
            if let Some(logs) = logs {
                state.last_logs = Some(logs);
            }
            state.active_permit.take();
            state.idle_since = None;
            state.lifecycle = AppLifecycleStateV1::Stopped;
        }
        slot.changed.notify_waiters();
        if let Some(detail) = shutdown_error {
            Err(SupervisorError::Worker {
                app_id: app_id.clone(),
                detail,
            })
        } else {
            Ok(())
        }
    }

    async fn status(&self, app_id: &AppId) -> Result<AppRuntimeStatus, SupervisorError> {
        let slot = self.slot(app_id)?;
        let state = slot.state.lock().await;
        Ok(AppRuntimeStatus {
            app_id: app_id.clone(),
            generation: slot.admitted.generation.clone(),
            state: state.lifecycle,
            reason: state.reason.clone(),
            active_leases: state.active_leases,
            waiters: state.waiters,
            pid: state.worker.as_ref().map(ManagedWorkerHandle::pid),
            restart_count: state.restart_count,
        })
    }

    async fn shutdown(&self) -> Result<(), SupervisorError> {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            loop {
                let changed = self.shutdown_changed.notified();
                if self.shutdown_complete.load(Ordering::Acquire) {
                    return Ok(());
                }
                changed.await;
            }
        }
        self.starting.close();
        self.active.close();
        for slot in self.apps.values() {
            slot.changed.notify_waiters();
        }
        let drain_deadline = Instant::now() + self.config.shutdown_timeout;
        loop {
            let mut leases = 0_usize;
            for slot in self.apps.values() {
                leases = leases.saturating_add(slot.state.lock().await.active_leases);
            }
            if leases == 0 || Instant::now() >= drain_deadline {
                break;
            }
            tokio::time::sleep(MONITOR_INTERVAL).await;
        }
        let mut first_error = None;
        for app_id in self.apps.keys() {
            if let Err(error) = self.stop_one(app_id).await {
                first_error.get_or_insert(error);
            }
        }
        self.shutdown_complete.store(true, Ordering::Release);
        self.shutdown_changed.notify_waiters();
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }
}
