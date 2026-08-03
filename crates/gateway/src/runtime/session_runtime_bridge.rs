use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::FutureExt;
use harness_contract::turn::{InputRoutingDecision, SessionInputStatus};
use runtime::execution_core::graph::{
    ExecutionResourceKind, ExecutionResourceLease, ExecutionResourceManager, ResourceObservation,
    ResourceResultClass,
};
#[cfg(test)]
use session::UnifiedSessionStore;
use session::{
    OutboxFailureClass, SessionMissionOutboxOperation, SessionRuntimeInputStatus,
    SessionRuntimeOutboxRecord,
};
use tokio::{
    sync::{oneshot, watch, Notify},
    task::{JoinHandle, JoinSet},
};

use crate::{
    event_bus::{SessionProjectionEvent, SessionProjectionHub},
    runtime_service::{RuntimeService, SESSION_RUNTIME_BUSY_ERROR},
    services::SessionService,
};

const WORKER_BATCH: usize = 32;
const INGRESS_CONCURRENCY: usize = 32;
const LEASE_MS: u64 = 30_000;
const MAX_ATTEMPTS: u32 = 8;
const SUPERVISOR_RESTART_BASE: Duration = Duration::from_millis(100);
const SUPERVISOR_RESTART_MAX: Duration = Duration::from_secs(5);
const WORKER_STARTUP_TIMEOUT: Duration = Duration::from_secs(8);
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);
const SUPERVISOR_JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const CROSS_PROCESS_RECONCILIATION_FALLBACK: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct GatewaySessionIngressExecutor {
    runtime: Arc<RuntimeService>,
    session: Arc<SessionService>,
    resources: Arc<ExecutionResourceManager>,
    lease_lost: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait]
impl runtime::SessionIngressExecutor for GatewaySessionIngressExecutor {
    async fn execute_ingress(
        &self,
        record: &runtime::RuntimeSessionInputRecord,
        content: &str,
    ) -> Result<runtime::SessionIngressExecutionReceipt, String> {
        let mut record = session_input_record_from_runtime(record)?;
        let durable = self
            .session
            .runtime_input(&record.request_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                format!(
                    "Session input `{}` disappeared before Runtime execution",
                    record.request_id
                )
            })?;
        if durable.input_id != record.input_id
            || durable.session_id != record.session_id
            || durable.sequence != record.sequence
            || durable.session_generation != record.session_generation
            || durable.claim_owner != record.claim_owner
            || durable.claim_token != record.claim_token
            || durable.claim_fence_epoch.is_none()
            || !matches!(
                durable.status,
                session::SessionRuntimeInputStatus::Claimed
                    | session::SessionRuntimeInputStatus::Running
            )
        {
            return Err(format!(
                "Session input `{}` claim identity changed before Runtime execution",
                record.request_id
            ));
        }
        record.claim_fence_epoch = durable.claim_fence_epoch;
        let lease = self
            .resources
            .acquire(ExecutionResourceKind::SessionTurn, None)
            .await
            .map_err(|error| format!("SessionTurn admission failed: {error}"))?;
        self.execute_ingress_with_lease(&record, content, lease)
            .await
    }
}

fn session_input_record_from_runtime(
    record: &runtime::RuntimeSessionInputRecord,
) -> Result<session::SessionRuntimeOutboxRecord, String> {
    let failure_class = match record.failure_class.as_deref() {
        None => None,
        Some("retryable") => Some(session::OutboxFailureClass::Retryable),
        Some("permanent") => Some(session::OutboxFailureClass::Permanent),
        Some("authorization_blocked") => Some(session::OutboxFailureClass::AuthorizationBlocked),
        Some("corrupt_payload") => Some(session::OutboxFailureClass::CorruptPayload),
        Some(other) => {
            return Err(format!(
                "Runtime Session input contains unknown failure class `{other}`"
            ))
        }
    };
    Ok(session::SessionRuntimeOutboxRecord {
        input_id: record.input_id.clone(),
        request_id: record.request_id.clone(),
        turn_id: record.turn_id.clone(),
        message_id: record.message_id.clone(),
        session_id: record.session_id.clone(),
        sequence: record.sequence,
        session_generation: record.session_generation,
        decision: record.decision,
        target_turn_id: record.target_turn_id.clone(),
        classification_json: record.classification_json.clone(),
        status: match record.status {
            runtime::RuntimeSessionInputStatus::Accepted => {
                session::SessionRuntimeInputStatus::Accepted
            }
            runtime::RuntimeSessionInputStatus::Classified => {
                session::SessionRuntimeInputStatus::Classified
            }
            runtime::RuntimeSessionInputStatus::Queued => {
                session::SessionRuntimeInputStatus::Queued
            }
            runtime::RuntimeSessionInputStatus::RejectedDuplicate => {
                session::SessionRuntimeInputStatus::RejectedDuplicate
            }
            runtime::RuntimeSessionInputStatus::RejectedPolicy => {
                session::SessionRuntimeInputStatus::RejectedPolicy
            }
            runtime::RuntimeSessionInputStatus::Claimed => {
                session::SessionRuntimeInputStatus::Claimed
            }
            runtime::RuntimeSessionInputStatus::Running => {
                session::SessionRuntimeInputStatus::Running
            }
            runtime::RuntimeSessionInputStatus::Reclassified => {
                session::SessionRuntimeInputStatus::Reclassified
            }
            runtime::RuntimeSessionInputStatus::Completed => {
                session::SessionRuntimeInputStatus::Completed
            }
            runtime::RuntimeSessionInputStatus::Supplemented => {
                session::SessionRuntimeInputStatus::Supplemented
            }
            runtime::RuntimeSessionInputStatus::Failed => {
                session::SessionRuntimeInputStatus::Failed
            }
            runtime::RuntimeSessionInputStatus::Blocked => {
                session::SessionRuntimeInputStatus::Blocked
            }
            runtime::RuntimeSessionInputStatus::Cancelled => {
                session::SessionRuntimeInputStatus::Cancelled
            }
            runtime::RuntimeSessionInputStatus::Expired => {
                session::SessionRuntimeInputStatus::Expired
            }
        },
        runtime_commit_cursor: record.runtime_commit_cursor,
        attempts: record.attempts,
        next_attempt_at_ms: record.next_attempt_at_ms,
        claim_owner: record.claim_owner.clone(),
        claim_token: record.claim_token.clone(),
        claim_fence_epoch: record.claim_fence_epoch,
        claim_expires_at_ms: record.claim_expires_at_ms,
        failure_class,
        last_error: record.last_error.clone(),
        revision: record.revision,
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
        terminal_at_ms: record.terminal_at_ms,
        runtime_options_json: record.runtime_options_json.clone(),
    })
}

impl GatewaySessionIngressExecutor {
    async fn execute_ingress_with_lease(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        content: &str,
        lease: ExecutionResourceLease,
    ) -> Result<runtime::SessionIngressExecutionReceipt, String> {
        let service_started = Instant::now();
        let queue_wait = lease.queue_wait();
        self.session
            .activate_worker_session(&record.session_id)
            .await?;
        let outcome = self.runtime.execute_ingress_record(record, content).await;
        let result_class = if outcome.is_ok() {
            ResourceResultClass::Completed
        } else {
            ResourceResultClass::Failed
        };
        drop(lease);
        if let Err(error) = self.resources.record_observation(
            &ExecutionResourceKind::SessionTurn,
            ResourceObservation {
                observed_at_ms: now_ms(),
                queue_wait,
                service_time: service_started.elapsed(),
                result_class,
            },
        ) {
            tracing::warn!(%error, "failed to record SessionTurn resource observation");
        }
        outcome
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionWorkerState {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SessionWorkerObservation {
    pub(crate) state: SessionWorkerState,
    pub(crate) restart_count: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) next_retry_at_ms: Option<u64>,
    pub(crate) last_backend_success_at_ms: Option<u64>,
    pub(crate) last_backend_error_at_ms: Option<u64>,
    pub(crate) last_backend_error: Option<String>,
    pub(crate) consecutive_backend_failures: u64,
    pub(crate) oldest_queue_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct SessionReconciliationProgress {
    pub(crate) scan_count: u64,
    pub(crate) scanned_count: u64,
    pub(crate) attempted_count: u64,
    pub(crate) processed_count: u64,
    pub(crate) pending_count: u64,
    pub(crate) pending_count_truncated: bool,
    pub(crate) oldest_pending_age_ms: Option<u64>,
    pub(crate) last_operation_id: Option<String>,
    pub(crate) last_scan_at_ms: Option<u64>,
    pub(crate) last_success_at_ms: Option<u64>,
    pub(crate) last_error_at_ms: Option<u64>,
    pub(crate) last_error: Option<String>,
    pub(crate) consecutive_failures: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SessionWorkerHealth {
    pub(crate) accepting: bool,
    pub(crate) forced_aborts: u64,
    pub(crate) claim_lease_lost: u64,
    pub(crate) workers: BTreeMap<String, SessionWorkerObservation>,
    pub(crate) recovery: crate::services::session_service::activation::SessionRecoverySummary,
    pub(crate) recovery_completed_at_ms: u64,
    pub(crate) reconciliation: BTreeMap<String, SessionReconciliationProgress>,
}

pub(crate) const REQUIRED_SESSION_WORKERS: [&str; 6] = [
    "ingress",
    "terminal_delivery",
    "mission_membership",
    "working_set_cleanup",
    "lifecycle_reconciliation",
    "branch_activation_reconciliation",
];

struct SupervisedWorker {
    name: &'static str,
    handle: JoinHandle<()>,
    initial_ready: Option<oneshot::Receiver<Result<(), String>>>,
}

type WorkerFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
type WorkerAttemptResult = Result<Result<(), String>, Box<dyn std::any::Any + Send>>;
type WorkerAttemptFuture = Pin<Box<dyn Future<Output = WorkerAttemptResult> + Send>>;
type WorkerFactory = Arc<
    dyn Fn(watch::Receiver<bool>, oneshot::Sender<Result<(), String>>) -> WorkerFuture
        + Send
        + Sync,
>;

const BACKEND_FAILURE_RESTART_THRESHOLD: u64 = 3;

#[derive(Clone)]
struct WorkerBackendReporter {
    name: &'static str,
    states: Arc<Mutex<BTreeMap<String, SessionWorkerObservation>>>,
}

impl WorkerBackendReporter {
    fn success(&self, oldest_queue_age_ms: Option<u64>) {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let observation = states
            .get_mut(self.name)
            .expect("supervised worker observation exists");
        observation.last_backend_success_at_ms = Some(now_ms());
        observation.last_backend_error_at_ms = None;
        observation.last_backend_error = None;
        observation.consecutive_backend_failures = 0;
        observation.oldest_queue_age_ms = oldest_queue_age_ms;
    }

    fn failure(&self, error: impl Into<String>) -> bool {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let observation = states
            .get_mut(self.name)
            .expect("supervised worker observation exists");
        let error = error.into();
        observation.last_backend_error_at_ms = Some(now_ms());
        observation.last_backend_error = Some(error);
        observation.consecutive_backend_failures =
            observation.consecutive_backend_failures.saturating_add(1);
        observation.consecutive_backend_failures >= BACKEND_FAILURE_RESTART_THRESHOLD
    }
}

#[derive(Clone, Copy)]
struct WorkerSupervisorConfig {
    restart_base: Duration,
    restart_max: Duration,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
}

impl Default for WorkerSupervisorConfig {
    fn default() -> Self {
        Self {
            restart_base: SUPERVISOR_RESTART_BASE,
            restart_max: SUPERVISOR_RESTART_MAX,
            startup_timeout: WORKER_STARTUP_TIMEOUT,
            shutdown_timeout: WORKER_SHUTDOWN_TIMEOUT,
        }
    }
}

/// Sole owner of every long-lived Session worker and its shutdown lifecycle.
///
/// A timed-out worker is explicitly aborted and awaited; no JoinHandle is
/// ever dropped while the task can continue detached from Gateway ownership.
pub(crate) struct SessionWorkerSupervisor {
    accepting: std::sync::atomic::AtomicBool,
    shutdown: watch::Sender<bool>,
    workers: Mutex<Option<Vec<SupervisedWorker>>>,
    states: Arc<Mutex<BTreeMap<String, SessionWorkerObservation>>>,
    reconciliation: Arc<Mutex<BTreeMap<String, SessionReconciliationProgress>>>,
    forced_aborts: Arc<std::sync::atomic::AtomicU64>,
    claim_lease_lost: Arc<std::sync::atomic::AtomicU64>,
    recovery: Mutex<crate::services::session_service::activation::SessionRecoverySummary>,
    recovery_completed_at_ms: std::sync::atomic::AtomicU64,
}

impl SessionWorkerSupervisor {
    #[cfg(test)]
    pub(crate) fn for_tests() -> Arc<Self> {
        let (shutdown, _) = watch::channel(false);
        let states = REQUIRED_SESSION_WORKERS
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    SessionWorkerObservation {
                        state: SessionWorkerState::Running,
                        restart_count: 0,
                        last_error: None,
                        next_retry_at_ms: None,
                        last_backend_success_at_ms: Some(now_ms()),
                        last_backend_error_at_ms: None,
                        last_backend_error: None,
                        consecutive_backend_failures: 0,
                        oldest_queue_age_ms: None,
                    },
                )
            })
            .collect();
        let reconciliation = reconciliation_progress_map();
        Arc::new(Self {
            accepting: std::sync::atomic::AtomicBool::new(true),
            shutdown,
            workers: Mutex::new(Some(Vec::new())),
            states: Arc::new(Mutex::new(states)),
            reconciliation: Arc::new(Mutex::new(reconciliation)),
            forced_aborts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            claim_lease_lost: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            recovery: Mutex::new(Default::default()),
            recovery_completed_at_ms: std::sync::atomic::AtomicU64::new(now_ms()),
        })
    }

    pub(crate) async fn start(
        runtime_service: Arc<RuntimeService>,
        session_service: Arc<SessionService>,
        event_bus: Arc<SessionProjectionHub>,
    ) -> Result<Arc<Self>, String> {
        let (shutdown, _) = watch::channel(false);
        let claim_lease_lost = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ingress_runtime = GatewaySessionIngressExecutor {
            runtime: Arc::clone(&runtime_service),
            session: Arc::clone(&session_service),
            resources: Arc::clone(runtime_service.runtime_services().resource_manager()),
            lease_lost: Arc::clone(&claim_lease_lost),
        };
        let ingress_service = Arc::clone(&session_service);
        let ingress_wake = runtime_service.session_input_router().wake_signal();
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let reconciliation = Arc::new(Mutex::new(reconciliation_progress_map()));
        let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ingress_states = Arc::clone(&states);
        let ingress_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&ingress_service);
            let executor = ingress_runtime.clone();
            let wake = Arc::clone(&ingress_wake);
            let reporter = WorkerBackendReporter {
                name: "ingress",
                states: Arc::clone(&ingress_states),
            };
            Box::pin(async move {
                run_ingress_worker(session_service, executor, wake, reporter, shutdown, ready)
                    .await?;
                Ok(())
            })
        });
        let ingress = spawn_supervised(
            "ingress",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            ingress_factory,
            WorkerSupervisorConfig::default(),
        );
        let delivery_runtime = Arc::clone(&runtime_service);
        let delivery_store = delivery_runtime
            .runtime_services()
            .session_terminal_delivery();
        let mission_service = Arc::clone(&session_service);
        let delivery_session_service = Arc::clone(&session_service);
        let delivery_states = Arc::clone(&states);
        let delivery_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let delivery_store = delivery_store.clone();
            let session_service = Arc::clone(&delivery_session_service);
            let event_bus = Arc::clone(&event_bus);
            let reporter = WorkerBackendReporter {
                name: "terminal_delivery",
                states: Arc::clone(&delivery_states),
            };
            Box::pin(async move {
                run_delivery_worker(
                    delivery_store,
                    session_service,
                    event_bus,
                    reporter,
                    shutdown,
                    ready,
                )
                .await?;
                Ok(())
            })
        });
        let delivery = spawn_supervised(
            "terminal_delivery",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            delivery_factory,
            WorkerSupervisorConfig::default(),
        );
        let mission_runtime = runtime::MissionRuntimePort::new(runtime_service.runtime_services());
        let workspace_key = runtime_service
            .runtime_services()
            .workspace_key()
            .to_string();
        let mission_states = Arc::clone(&states);
        let mission_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&mission_service);
            let mission_runtime = mission_runtime.clone();
            let workspace_key = workspace_key.clone();
            let reporter = WorkerBackendReporter {
                name: "mission_membership",
                states: Arc::clone(&mission_states),
            };
            Box::pin(async move {
                run_mission_membership_worker(
                    session_service,
                    mission_runtime,
                    workspace_key,
                    reporter,
                    shutdown,
                    ready,
                )
                .await?;
                Ok(())
            })
        });
        let mission = spawn_supervised(
            "mission_membership",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            mission_factory,
            WorkerSupervisorConfig::default(),
        );
        let cleanup_service = Arc::clone(&session_service);
        let cleanup_states = Arc::clone(&states);
        let cleanup_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&cleanup_service);
            let reporter = WorkerBackendReporter {
                name: "working_set_cleanup",
                states: Arc::clone(&cleanup_states),
            };
            Box::pin(async move {
                run_session_cleanup_worker(session_service, reporter, shutdown, ready).await?;
                Ok(())
            })
        });
        let cleanup = spawn_supervised(
            "working_set_cleanup",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            cleanup_factory,
            WorkerSupervisorConfig::default(),
        );
        let lifecycle_service = Arc::clone(&session_service);
        let lifecycle_progress = Arc::clone(&reconciliation);
        let lifecycle_states = Arc::clone(&states);
        let lifecycle_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&lifecycle_service);
            let progress = Arc::clone(&lifecycle_progress);
            let reporter = WorkerBackendReporter {
                name: "lifecycle_reconciliation",
                states: Arc::clone(&lifecycle_states),
            };
            Box::pin(run_lifecycle_reconciliation_worker(
                session_service,
                progress,
                reporter,
                shutdown,
                ready,
            ))
        });
        let lifecycle = spawn_supervised(
            "lifecycle_reconciliation",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            lifecycle_factory,
            WorkerSupervisorConfig::default(),
        );
        let branch_service = Arc::clone(&session_service);
        let branch_progress = Arc::clone(&reconciliation);
        let branch_states = Arc::clone(&states);
        let branch_factory: WorkerFactory = Arc::new(move |shutdown, ready| {
            let session_service = Arc::clone(&branch_service);
            let progress = Arc::clone(&branch_progress);
            let reporter = WorkerBackendReporter {
                name: "branch_activation_reconciliation",
                states: Arc::clone(&branch_states),
            };
            Box::pin(run_branch_activation_reconciliation_worker(
                session_service,
                progress,
                reporter,
                shutdown,
                ready,
            ))
        });
        let branch = spawn_supervised(
            "branch_activation_reconciliation",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            shutdown.subscribe(),
            branch_factory,
            WorkerSupervisorConfig::default(),
        );
        let mut workers = vec![ingress, delivery, mission, cleanup, lifecycle, branch];
        if let Err(error) =
            await_initial_worker_readiness(&mut workers, WorkerSupervisorConfig::default()).await
        {
            rollback_started_workers(
                &shutdown,
                &mut workers,
                &states,
                &forced_aborts,
                WorkerSupervisorConfig::default(),
            )
            .await;
            return Err(error);
        }
        Ok(Arc::new(Self {
            accepting: std::sync::atomic::AtomicBool::new(true),
            shutdown,
            workers: Mutex::new(Some(workers)),
            states,
            reconciliation,
            forced_aborts,
            claim_lease_lost,
            recovery: Mutex::new(Default::default()),
            recovery_completed_at_ms: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    pub(crate) fn record_recovery(
        &self,
        recovery: crate::services::session_service::activation::SessionRecoverySummary,
    ) {
        tracing::info!(
            discovered = recovery.discovered,
            metadata_loaded = recovery.metadata_loaded,
            required = recovery.required,
            attached = recovery.attached,
            recent = recovery.recent,
            recovered = recovery.recovered,
            already_active = recovery.already_active,
            metadata_only = recovery.metadata_only,
            model_rebind_required = recovery.model_rebind_required,
            failed = recovery.failed,
            hot_bytes = recovery.hot_bytes,
            "Session supervisor startup recovery completed"
        );
        for failure in &recovery.failures {
            tracing::warn!(error = %failure, "Session startup recovery item failed");
        }
        *self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = recovery;
        self.recovery_completed_at_ms
            .store(now_ms(), std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn health(&self) -> SessionWorkerHealth {
        if let Some(workers) = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            for worker in workers {
                if worker.handle.is_finished() {
                    let exited_while_running = self
                        .states
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(worker.name)
                        .is_some_and(|worker| worker.state == SessionWorkerState::Running);
                    if exited_while_running {
                        set_worker_failure(
                            &self.states,
                            worker.name,
                            "worker exited before supervised shutdown",
                        );
                    }
                }
            }
        }
        SessionWorkerHealth {
            accepting: self.accepting.load(std::sync::atomic::Ordering::Acquire),
            forced_aborts: self
                .forced_aborts
                .load(std::sync::atomic::Ordering::Relaxed),
            claim_lease_lost: self
                .claim_lease_lost
                .load(std::sync::atomic::Ordering::Relaxed),
            workers: self
                .states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            recovery: self
                .recovery
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            recovery_completed_at_ms: self
                .recovery_completed_at_ms
                .load(std::sync::atomic::Ordering::Acquire),
            reconciliation: self
                .reconciliation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }

    #[must_use]
    pub(crate) fn is_accepting(&self) -> bool {
        self.accepting.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn stop_accepting(&self) {
        self.accepting
            .store(false, std::sync::atomic::Ordering::Release);
    }

    pub(crate) async fn shutdown(&self) {
        self.stop_accepting();
        self.shutdown.send_replace(true);
        let workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_default();
        for mut worker in workers {
            match tokio::time::timeout(SUPERVISOR_JOIN_TIMEOUT, &mut worker.handle).await {
                Ok(Ok(())) => {
                    if worker_observation(&self.states, worker.name)
                        .map_or(true, |state| state.state != SessionWorkerState::Aborted)
                    {
                        set_worker_state(&self.states, worker.name, SessionWorkerState::Stopped);
                    }
                }
                Ok(Err(error)) => {
                    tracing::error!(worker = worker.name, %error, "Session worker failed");
                    set_worker_failure(&self.states, worker.name, error.to_string());
                }
                Err(_) => {
                    worker.handle.abort();
                    let _ = worker.handle.await;
                    self.forced_aborts
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    set_worker_state(&self.states, worker.name, SessionWorkerState::Aborted);
                }
            }
        }
    }
}

fn spawn_supervised(
    name: &'static str,
    states: Arc<Mutex<BTreeMap<String, SessionWorkerObservation>>>,
    forced_aborts: Arc<std::sync::atomic::AtomicU64>,
    shutdown: watch::Receiver<bool>,
    factory: WorkerFactory,
    config: WorkerSupervisorConfig,
) -> SupervisedWorker {
    set_worker_state(&states, name, SessionWorkerState::Starting);
    let worker_states = Arc::clone(&states);
    let (initial_ready, initial_ready_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        supervise_worker(
            name,
            worker_states,
            forced_aborts,
            shutdown,
            factory,
            config,
            initial_ready,
        )
        .await;
    });
    SupervisedWorker {
        name,
        handle,
        initial_ready: Some(initial_ready_rx),
    }
}

async fn supervise_worker(
    name: &'static str,
    states: Arc<Mutex<BTreeMap<String, SessionWorkerObservation>>>,
    forced_aborts: Arc<std::sync::atomic::AtomicU64>,
    mut shutdown: watch::Receiver<bool>,
    factory: WorkerFactory,
    config: WorkerSupervisorConfig,
    initial_ready: oneshot::Sender<Result<(), String>>,
) {
    let mut initial_ready = Some(initial_ready);
    loop {
        if *shutdown.borrow() {
            set_worker_state(&states, name, SessionWorkerState::Stopped);
            notify_initial_readiness(
                &mut initial_ready,
                Err(format!("Session worker `{name}` stopped before readiness")),
            );
            return;
        }

        set_worker_state(&states, name, SessionWorkerState::Starting);
        let (attempt_ready, mut attempt_ready_rx) = oneshot::channel();
        let mut worker: WorkerAttemptFuture = Box::pin(
            std::panic::AssertUnwindSafe(factory(shutdown.clone(), attempt_ready)).catch_unwind(),
        );
        let (readiness, worker_completed) = tokio::select! {
            ready = &mut attempt_ready_rx => {
                match ready {
                    Ok(result) => (result, false),
                    Err(_) => {
                        match tokio::time::timeout(Duration::from_millis(10), &mut worker).await {
                            Ok(result) => (Err(worker_exit_failure(result)), true),
                            Err(_) => (Err(format!(
                                "Session worker `{name}` closed its readiness channel before signalling"
                            )), false),
                        }
                    }
                }
            }
            result = &mut worker => {
                (Err(worker_exit_failure(result)), true)
            }
            _ = tokio::time::sleep(config.startup_timeout) => {
                (Err(format!(
                    "Session worker `{name}` readiness timed out after {} ms",
                    config.startup_timeout.as_millis()
                )), false)
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    stop_worker(
                        &states,
                        &forced_aborts,
                        name,
                        &mut worker,
                        config.shutdown_timeout,
                    )
                    .await;
                    notify_initial_readiness(
                        &mut initial_ready,
                        Err(format!("Session worker `{name}` stopped before readiness")),
                    );
                    return;
                }
                continue;
            }
        };
        if let Err(failure) = readiness {
            if !worker_completed {
                forced_aborts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            notify_initial_readiness(&mut initial_ready, Err(failure.clone()));
            let delay = record_worker_restart(&states, name, failure, config);
            if wait_for_restart_or_shutdown(&states, name, delay, &mut shutdown).await {
                return;
            }
            continue;
        }
        set_worker_state(&states, name, SessionWorkerState::Running);
        notify_initial_readiness(&mut initial_ready, Ok(()));

        let failure = tokio::select! {
            result = &mut worker => {
                if *shutdown.borrow() {
                    record_shutdown_result(&states, name, result);
                    return;
                }
                match result {
                    Ok(Ok(())) => "worker exited unexpectedly".to_string(),
                    Ok(Err(error)) => error,
                    Err(payload) => {
                        format!("worker panicked: {}", panic_payload_message(payload))
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    stop_worker(
                        &states,
                        &forced_aborts,
                        name,
                        &mut worker,
                        config.shutdown_timeout,
                    )
                    .await;
                    return;
                }
                continue;
            }
        };

        let delay = record_worker_restart(&states, name, failure, config);
        tracing::warn!(
            worker = name,
            restart_in_ms = delay.as_millis() as u64,
            "Session worker failed; scheduling supervised restart"
        );
        if wait_for_restart_or_shutdown(&states, name, delay, &mut shutdown).await {
            return;
        }
    }
}

fn signal_worker_ready(ready: oneshot::Sender<Result<(), String>>) -> Result<(), String> {
    ready
        .send(Ok(()))
        .map_err(|_| "Session worker supervisor dropped readiness receiver".to_string())
}

fn notify_initial_readiness(
    initial_ready: &mut Option<oneshot::Sender<Result<(), String>>>,
    result: Result<(), String>,
) {
    if let Some(sender) = initial_ready.take() {
        let _ = sender.send(result);
    }
}

fn worker_exit_failure(result: WorkerAttemptResult) -> String {
    match result {
        Ok(Ok(())) => "worker exited unexpectedly".to_string(),
        Ok(Err(error)) => error,
        Err(payload) => format!("worker panicked: {}", panic_payload_message(payload)),
    }
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload")
        .to_string()
}

async fn wait_for_restart_or_shutdown(
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    name: &str,
    delay: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                set_worker_state(states, name, SessionWorkerState::Stopped);
                true
            } else {
                false
            }
        }
        _ = tokio::time::sleep(delay) => false,
    }
}

async fn await_initial_worker_readiness(
    workers: &mut [SupervisedWorker],
    config: WorkerSupervisorConfig,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + config.startup_timeout;
    for worker in workers {
        let receiver = worker.initial_ready.take().ok_or_else(|| {
            format!(
                "Session worker `{}` has no initial readiness receiver",
                worker.name
            )
        })?;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "Session worker startup readiness timed out after {} ms",
                config.startup_timeout.as_millis()
            ));
        }
        match tokio::time::timeout(remaining, receiver).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                return Err(format!(
                    "Session worker `{}` failed startup readiness: {error}",
                    worker.name
                ))
            }
            Ok(Err(_)) => {
                return Err(format!(
                    "Session worker `{}` dropped startup readiness",
                    worker.name
                ))
            }
            Err(_) => {
                return Err(format!(
                    "Session worker `{}` startup readiness timed out after {} ms",
                    worker.name,
                    config.startup_timeout.as_millis()
                ))
            }
        }
    }
    Ok(())
}

async fn rollback_started_workers(
    shutdown: &watch::Sender<bool>,
    workers: &mut Vec<SupervisedWorker>,
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    forced_aborts: &std::sync::atomic::AtomicU64,
    config: WorkerSupervisorConfig,
) {
    shutdown.send_replace(true);
    for worker in workers {
        let timeout = config
            .shutdown_timeout
            .saturating_add(Duration::from_millis(250));
        match tokio::time::timeout(timeout, &mut worker.handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                set_worker_failure(states, worker.name, error.to_string());
            }
            Err(_) => {
                worker.handle.abort();
                let _ = (&mut worker.handle).await;
                forced_aborts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                set_worker_state(states, worker.name, SessionWorkerState::Aborted);
            }
        }
    }
}

async fn stop_worker(
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    forced_aborts: &std::sync::atomic::AtomicU64,
    name: &str,
    worker: &mut WorkerAttemptFuture,
    timeout: Duration,
) {
    set_worker_state(states, name, SessionWorkerState::Stopping);
    match tokio::time::timeout(timeout, &mut *worker).await {
        Ok(result) => record_shutdown_result(states, name, result),
        Err(_) => {
            forced_aborts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            set_worker_state(states, name, SessionWorkerState::Aborted);
        }
    }
}

fn record_shutdown_result(
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    name: &str,
    result: WorkerAttemptResult,
) {
    match result {
        Ok(Ok(())) => set_worker_state(states, name, SessionWorkerState::Stopped),
        Ok(Err(error)) => set_worker_failure(states, name, error),
        Err(payload) => set_worker_failure(
            states,
            name,
            format!("worker panicked: {}", panic_payload_message(payload)),
        ),
    }
}

fn record_worker_restart(
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    name: &str,
    error: impl Into<String>,
    config: WorkerSupervisorConfig,
) -> Duration {
    let mut states = states
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = states
        .entry(name.to_string())
        .or_insert_with(|| SessionWorkerObservation {
            state: SessionWorkerState::Failed,
            restart_count: 0,
            last_error: None,
            next_retry_at_ms: None,
            last_backend_success_at_ms: None,
            last_backend_error_at_ms: None,
            last_backend_error: None,
            consecutive_backend_failures: 0,
            oldest_queue_age_ms: None,
        });
    observation.state = SessionWorkerState::Failed;
    observation.restart_count = observation.restart_count.saturating_add(1);
    observation.last_error = Some(error.into());
    let delay = supervisor_restart_delay(observation.restart_count, config);
    observation.next_retry_at_ms =
        Some(now_ms().saturating_add(delay.as_millis().try_into().unwrap_or(u64::MAX)));
    delay
}

fn supervisor_restart_delay(restart_count: u64, config: WorkerSupervisorConfig) -> Duration {
    let shift = restart_count.saturating_sub(1).min(31) as u32;
    config
        .restart_base
        .saturating_mul(1_u32 << shift)
        .min(config.restart_max)
}

fn set_worker_state(
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    name: &str,
    state: SessionWorkerState,
) {
    let mut states = states
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = states
        .entry(name.to_string())
        .or_insert_with(|| SessionWorkerObservation {
            state,
            restart_count: 0,
            last_error: None,
            next_retry_at_ms: None,
            last_backend_success_at_ms: None,
            last_backend_error_at_ms: None,
            last_backend_error: None,
            consecutive_backend_failures: 0,
            oldest_queue_age_ms: None,
        });
    observation.state = state;
    observation.next_retry_at_ms = None;
}

fn set_worker_failure(
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    name: &str,
    error: impl Into<String>,
) {
    let mut states = states
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = states
        .entry(name.to_string())
        .or_insert_with(|| SessionWorkerObservation {
            state: SessionWorkerState::Failed,
            restart_count: 0,
            last_error: None,
            next_retry_at_ms: None,
            last_backend_success_at_ms: None,
            last_backend_error_at_ms: None,
            last_backend_error: None,
            consecutive_backend_failures: 0,
            oldest_queue_age_ms: None,
        });
    observation.state = SessionWorkerState::Failed;
    observation.last_error = Some(error.into());
    observation.next_retry_at_ms = None;
}

fn worker_observation(
    states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
    name: &str,
) -> Option<SessionWorkerObservation> {
    states
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(name)
        .cloned()
}

fn reconciliation_progress_map() -> BTreeMap<String, SessionReconciliationProgress> {
    [
        "lifecycle_reconciliation",
        "branch_activation_reconciliation",
    ]
    .into_iter()
    .map(|name| (name.to_string(), SessionReconciliationProgress::default()))
    .collect()
}

fn begin_reconciliation_scan(
    progress: &Mutex<BTreeMap<String, SessionReconciliationProgress>>,
    name: &str,
    pending_count: usize,
    pending_count_truncated: bool,
    oldest_pending_at_ms: Option<u64>,
    observed_at_ms: u64,
) {
    let mut progress = progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = progress.entry(name.to_string()).or_default();
    observation.scan_count = observation.scan_count.saturating_add(1);
    observation.scanned_count = observation
        .scanned_count
        .saturating_add(pending_count as u64);
    observation.pending_count = pending_count as u64;
    observation.pending_count_truncated = pending_count_truncated;
    observation.oldest_pending_age_ms =
        oldest_pending_at_ms.map(|pending_at| observed_at_ms.saturating_sub(pending_at));
    observation.last_scan_at_ms = Some(observed_at_ms);
}

fn record_reconciliation_outcome(
    progress: &Mutex<BTreeMap<String, SessionReconciliationProgress>>,
    name: &str,
    operation_id: &str,
    outcome: &Result<bool, String>,
    observed_at_ms: u64,
) {
    let mut progress = progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = progress.entry(name.to_string()).or_default();
    observation.attempted_count = observation.attempted_count.saturating_add(1);
    observation.last_operation_id = Some(operation_id.to_string());
    match outcome {
        Ok(processed) => {
            if *processed {
                observation.processed_count = observation.processed_count.saturating_add(1);
            }
            observation.last_success_at_ms = Some(observed_at_ms);
        }
        Err(error) => {
            observation.last_error_at_ms = Some(observed_at_ms);
            observation.last_error = Some(error.clone());
            observation.consecutive_failures = observation.consecutive_failures.saturating_add(1);
        }
    }
}

fn finish_reconciliation_scan(
    progress: &Mutex<BTreeMap<String, SessionReconciliationProgress>>,
    name: &str,
    successful: bool,
    observed_at_ms: u64,
) {
    let mut progress = progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = progress.entry(name.to_string()).or_default();
    if successful {
        observation.last_success_at_ms = Some(observed_at_ms);
        observation.consecutive_failures = 0;
    }
}

fn record_reconciliation_scan_failure(
    progress: &Mutex<BTreeMap<String, SessionReconciliationProgress>>,
    name: &str,
    error: &str,
    observed_at_ms: u64,
) {
    let mut progress = progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let observation = progress.entry(name.to_string()).or_default();
    observation.scan_count = observation.scan_count.saturating_add(1);
    observation.last_scan_at_ms = Some(observed_at_ms);
    observation.last_error_at_ms = Some(observed_at_ms);
    observation.last_error = Some(error.to_string());
    observation.consecutive_failures = observation.consecutive_failures.saturating_add(1);
}

async fn run_session_cleanup_worker(
    session_service: Arc<SessionService>,
    reporter: WorkerBackendReporter,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), String> {
    let mut ticker = tokio::time::interval(Duration::from_secs(300));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let health = match session_service.runtime_outbox_health().await {
        Ok(health) => health,
        Err(error) => {
            let message = error.to_string();
            reporter.failure(message.clone());
            let _ = ready.send(Err(message.clone()));
            return Err(message);
        }
    };
    reporter.success(
        health
            .oldest_runnable_created_at_ms
            .map(|created_at| now_ms().saturating_sub(created_at)),
    );
    let initial_unloaded = match session_service.run_resource_cleanup().await {
        Ok(unloaded) => {
            reporter.success(None);
            unloaded
        }
        Err(error) => {
            reporter.failure(error.clone());
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    if initial_unloaded > 0 {
        tracing::info!(
            unloaded = initial_unloaded,
            "initial session resource cleanup completed"
        );
    }
    signal_worker_ready(ready)?;
    ticker.reset();
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = ticker.tick() => {
                match session_service.run_resource_cleanup().await {
                    Ok(unloaded) if unloaded > 0 => {
                        reporter.success(None);
                        tracing::info!(unloaded, "session resource cleanup completed");
                    }
                    Ok(_) => reporter.success(None),
                    Err(error) => {
                        tracing::error!(%error, "session resource cleanup failed");
                        if reporter.failure(error.clone()) {
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

async fn run_lifecycle_reconciliation_worker(
    session_service: Arc<SessionService>,
    progress: Arc<Mutex<BTreeMap<String, SessionReconciliationProgress>>>,
    reporter: WorkerBackendReporter,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), String> {
    const WORKER_NAME: &str = "lifecycle_reconciliation";
    let wake = session_service.lifecycle_work_wake();
    let mut ready = Some(ready);
    loop {
        let scanned_at_ms = now_ms();
        let mut pending = match session_service
            .list_pending_lifecycle_operations(WORKER_BATCH.saturating_add(1))
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                reporter.failure(error.clone());
                record_reconciliation_scan_failure(&progress, WORKER_NAME, &error, scanned_at_ms);
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error.clone()));
                }
                return Err(error);
            }
        };
        let pending_count_truncated = pending.len() > WORKER_BATCH;
        let oldest_pending_at_ms = pending.iter().map(|intent| intent.updated_at_ms).min();
        begin_reconciliation_scan(
            &progress,
            WORKER_NAME,
            pending.len(),
            pending_count_truncated,
            oldest_pending_at_ms,
            scanned_at_ms,
        );
        if let Some(ready) = ready.take() {
            signal_worker_ready(ready)?;
        }
        pending.truncate(WORKER_BATCH);
        let had_work = !pending.is_empty();
        let mut scan_succeeded = true;
        let mut scan_error = None;
        for intent in pending {
            if *shutdown.borrow() {
                return Ok(());
            }
            let outcome = session_service
                .reconcile_lifecycle_once(&intent.operation_id)
                .await;
            record_reconciliation_outcome(
                &progress,
                WORKER_NAME,
                &intent.operation_id,
                &outcome,
                now_ms(),
            );
            if let Err(error) = outcome {
                scan_succeeded = false;
                scan_error.get_or_insert_with(|| error.clone());
                tracing::warn!(
                    operation_id = %intent.operation_id,
                    session_id = %intent.session_id,
                    %error,
                    "Session lifecycle reconciliation deferred"
                );
            }
        }
        finish_reconciliation_scan(&progress, WORKER_NAME, scan_succeeded, now_ms());
        finish_reconciliation_backend_round(
            &reporter,
            oldest_pending_at_ms.map(|pending_at| scanned_at_ms.saturating_sub(pending_at)),
            scan_error,
        )?;
        if had_work {
            continue;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            () = wake.notified() => {}
            () = tokio::time::sleep(CROSS_PROCESS_RECONCILIATION_FALLBACK) => {}
        }
    }
}

async fn run_branch_activation_reconciliation_worker(
    session_service: Arc<SessionService>,
    progress: Arc<Mutex<BTreeMap<String, SessionReconciliationProgress>>>,
    reporter: WorkerBackendReporter,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), String> {
    const WORKER_NAME: &str = "branch_activation_reconciliation";
    let wake = session_service.branch_work_wake();
    let mut ready = Some(ready);
    loop {
        let scanned_at_ms = now_ms();
        let mut pending = match session_service
            .list_pending_branch_activations(WORKER_BATCH.saturating_add(1))
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                reporter.failure(error.clone());
                record_reconciliation_scan_failure(&progress, WORKER_NAME, &error, scanned_at_ms);
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error.clone()));
                }
                return Err(error);
            }
        };
        let pending_count_truncated = pending.len() > WORKER_BATCH;
        let oldest_pending_at_ms = pending
            .iter()
            .map(|activation| activation.updated_at_ms)
            .min();
        begin_reconciliation_scan(
            &progress,
            WORKER_NAME,
            pending.len(),
            pending_count_truncated,
            oldest_pending_at_ms,
            scanned_at_ms,
        );
        if let Some(ready) = ready.take() {
            signal_worker_ready(ready)?;
        }
        pending.truncate(WORKER_BATCH);
        let had_work = !pending.is_empty();
        let mut scan_succeeded = true;
        let mut scan_error = None;
        for activation in pending {
            if *shutdown.borrow() {
                return Ok(());
            }
            let outcome = session_service
                .reconcile_branch_once(&activation.operation_id)
                .await;
            record_reconciliation_outcome(
                &progress,
                WORKER_NAME,
                &activation.operation_id,
                &outcome,
                now_ms(),
            );
            if let Err(error) = outcome {
                scan_succeeded = false;
                scan_error.get_or_insert_with(|| error.clone());
                tracing::warn!(
                    operation_id = %activation.operation_id,
                    source_session_id = %activation.source_session_id,
                    target_session_id = %activation.target_session_id,
                    %error,
                    "Session branch activation reconciliation deferred"
                );
            }
        }
        finish_reconciliation_scan(&progress, WORKER_NAME, scan_succeeded, now_ms());
        finish_reconciliation_backend_round(
            &reporter,
            oldest_pending_at_ms.map(|pending_at| scanned_at_ms.saturating_sub(pending_at)),
            scan_error,
        )?;
        if had_work {
            continue;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            () = wake.notified() => {}
            () = tokio::time::sleep(CROSS_PROCESS_RECONCILIATION_FALLBACK) => {}
        }
    }
}

fn finish_reconciliation_backend_round(
    reporter: &WorkerBackendReporter,
    oldest_queue_age_ms: Option<u64>,
    error: Option<String>,
) -> Result<(), String> {
    if let Some(error) = error {
        if reporter.failure(error.clone()) {
            return Err(error);
        }
    } else {
        reporter.success(oldest_queue_age_ms);
    }
    Ok(())
}

async fn run_mission_membership_worker(
    session_service: Arc<SessionService>,
    mission: runtime::MissionRuntimePort,
    workspace_key: String,
    reporter: WorkerBackendReporter,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), String> {
    let worker_id = format!("gateway-mission-membership:{}", uuid::Uuid::new_v4());
    let wake = session_service.mission_work_wake();
    let mut ready = Some(ready);
    loop {
        if *shutdown.borrow() {
            break;
        }
        let claimed = session_service
            .claim_mission_work(&workspace_key, &worker_id, now_ms(), LEASE_MS, WORKER_BATCH)
            .await;
        let had_work = match claimed {
            Ok(records) => {
                let had_work = !records.is_empty();
                reporter.success(
                    records
                        .iter()
                        .map(|record| now_ms().saturating_sub(record.created_at_ms))
                        .max(),
                );
                if let Some(ready) = ready.take() {
                    signal_worker_ready(ready)?;
                }
                for record in records {
                    materialize_mission_membership(&session_service, &mission, &worker_id, record)
                        .await;
                }
                had_work
            }
            Err(error) => {
                if let Some(ready) = ready.take() {
                    let message = error.to_string();
                    reporter.failure(message.clone());
                    let _ = ready.send(Err(message.clone()));
                    return Err(message);
                }
                let message = error.to_string();
                tracing::error!(%error, workspace_key, "mission membership outbox claim failed");
                if reporter.failure(message.clone()) {
                    return Err(message);
                }
                false
            }
        };
        if had_work {
            continue;
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            () = wake.notified() => {},
            () = tokio::time::sleep(CROSS_PROCESS_RECONCILIATION_FALLBACK) => {},
        }
    }
    Ok(())
}

async fn materialize_mission_membership(
    session_service: &SessionService,
    mission: &runtime::MissionRuntimePort,
    worker_id: &str,
    record: session::SessionMissionOutboxRecord,
) {
    let outcome = match record.operation {
        SessionMissionOutboxOperation::Register | SessionMissionOutboxOperation::Start => mission
            .ensure_session_membership(&record.session_id)
            .map(|_| ()),
        SessionMissionOutboxOperation::Close => mission
            .remove_session_membership(&record.session_id)
            .map(|_| ()),
    };
    match outcome {
        Ok(()) => {
            if let Err(error) = session_service
                .complete_mission_work(&record, worker_id, now_ms())
                .await
            {
                tracing::error!(request_id = %record.request_id, %error, "mission lifecycle applied but outbox acknowledgement failed");
            }
        }
        Err(error) => {
            let retry_at = now_ms().saturating_add(retry_delay_ms(record.attempts));
            if let Err(failure) = session_service
                .fail_mission_work(
                    &record,
                    worker_id,
                    OutboxFailureClass::Retryable,
                    &error,
                    retry_at,
                    MAX_ATTEMPTS,
                    now_ms(),
                )
                .await
            {
                tracing::error!(request_id = %record.request_id, error = %failure, "mission lifecycle failure state could not be recorded");
            }
        }
    }
}

async fn run_ingress_worker(
    session_service: Arc<SessionService>,
    executor: GatewaySessionIngressExecutor,
    wake: Arc<Notify>,
    reporter: WorkerBackendReporter,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), String> {
    let worker_id = format!("gateway-session-input:{}", uuid::Uuid::new_v4());
    let mut running = JoinSet::new();
    let health = match session_service.runtime_outbox_health().await {
        Ok(health) => health,
        Err(error) => {
            let message = error.to_string();
            reporter.failure(message.clone());
            let _ = ready.send(Err(message.clone()));
            return Err(message);
        }
    };
    reporter.success(
        health
            .oldest_runnable_created_at_ms
            .map(|created_at| now_ms().saturating_sub(created_at)),
    );
    signal_worker_ready(ready)?;
    loop {
        if *shutdown.borrow() {
            break;
        }
        let mut claimed_any = false;
        while running.len() < INGRESS_CONCURRENCY {
            let record = match session_service
                .claim_ingress_work(&worker_id, now_ms(), LEASE_MS, 1)
                .await
            {
                Ok(mut records) => {
                    reporter.success(
                        records
                            .iter()
                            .map(|record| now_ms().saturating_sub(record.created_at_ms))
                            .max(),
                    );
                    let Some(record) = records.pop() else {
                        break;
                    };
                    tracing::debug!(
                        request_id = %record.request_id,
                        worker_id,
                        status = ?record.status,
                        revision = record.revision,
                        attempts = record.attempts,
                        claim_expires_at_ms = ?record.claim_expires_at_ms,
                        "Session ingress worker claimed durable input"
                    );
                    record
                }
                Err(error) => {
                    tracing::error!(%error, "session ingress claim failed");
                    if reporter.failure(error.to_string()) {
                        return Err(error.to_string());
                    }
                    break;
                }
            };
            let lease = tokio::select! {
                changed = shutdown.changed() => {
                    let reason = if changed.is_err() {
                        "Session ingress worker shutdown channel closed before resource admission"
                    } else {
                        "Session ingress worker is shutting down before resource admission"
                    };
                    requeue_claimed_without_execution(
                        &session_service,
                        &worker_id,
                        &record,
                        reason,
                    ).await;
                    break;
                }
                lease = executor.resources.acquire(ExecutionResourceKind::SessionTurn, None) => {
                    match lease {
                        Ok(lease) => lease,
                        Err(error) => {
                            let reason = format!(
                                "SessionTurn capacity admission failed before execution: {error}"
                            );
                            requeue_claimed_without_execution(
                                &session_service,
                                &worker_id,
                                &record,
                                &reason,
                            ).await;
                            tracing::error!(%error, "SessionTurn capacity admission failed");
                            break;
                        }
                    }
                }
            };
            claimed_any = true;
            let session_service = Arc::clone(&session_service);
            let executor = executor.clone();
            let worker_id = worker_id.clone();
            running.spawn(async move {
                process_claimed_session_input(session_service, executor, worker_id, record, lease)
                    .await;
            });
        }
        if claimed_any {
            continue;
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = wake.notified() => {},
            result = running.join_next(), if !running.is_empty() => {
                if let Some(Err(error)) = result {
                    tracing::error!(%error, "session ingress task panicked");
                }
            }
            _ = tokio::time::sleep(CROSS_PROCESS_RECONCILIATION_FALLBACK) => {},
        }
    }
    let drained = tokio::time::timeout(Duration::from_secs(8), async {
        while let Some(result) = running.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error, "session ingress task failed during shutdown");
            }
        }
    })
    .await;
    if drained.is_err() {
        tracing::warn!("Session ingress drain timed out; aborting remaining supervised turns");
        running.abort_all();
        while let Some(result) = running.join_next().await {
            if let Err(error) = result {
                tracing::debug!(%error, "aborted Session ingress turn joined");
            }
        }
    }
    Ok(())
}

async fn requeue_claimed_without_execution(
    session_service: &SessionService,
    worker_id: &str,
    record: &SessionRuntimeOutboxRecord,
    reason: &str,
) {
    let Some(claim_token) = record.claim_token.as_deref() else {
        tracing::error!(
            request_id = %record.request_id,
            "claimed Session input has no claim token and cannot be requeued"
        );
        return;
    };
    if let Err(error) = session_service
        .requeue_ingress_work(
            record,
            worker_id,
            claim_token,
            record.revision,
            record.decision,
            record.target_turn_id.as_deref(),
            reason,
            now_ms(),
        )
        .await
    {
        tracing::error!(
            request_id = %record.request_id,
            %error,
            "claimed Session input could not be requeued before execution"
        );
    }
}

async fn process_claimed_session_input(
    session_service: Arc<SessionService>,
    executor: GatewaySessionIngressExecutor,
    worker_id: String,
    record: SessionRuntimeOutboxRecord,
    lease: ExecutionResourceLease,
) {
    let Some(claim_token) = record.claim_token.clone() else {
        tracing::error!(request_id = %record.request_id, "claimed Session input has no claim token");
        return;
    };
    let content = match session_service.load_ingress_content(&record).await {
        Ok(content) => content,
        Err(error) => {
            record_ingress_failure(
                &session_service,
                &worker_id,
                &record,
                &claim_token,
                record.revision,
                OutboxFailureClass::CorruptPayload,
                &error.to_string(),
            )
            .await;
            return;
        }
    };

    if matches!(
        record.decision,
        InputRoutingDecision::SupplementCurrentTurn
            | InputRoutingDecision::ControlOrApproval
            | InputRoutingDecision::InterruptAndReplan
    ) {
        if record.decision == InputRoutingDecision::SupplementCurrentTurn
            && executor.runtime.session_input_checkpoint_consumed(
                &record.session_id,
                &record.input_id,
                record.target_turn_id.as_deref(),
            )
        {
            acknowledge_checkpoint_consumed_ingress(
                &session_service,
                &worker_id,
                &record,
                &claim_token,
            )
            .await;
            return;
        }
        let target_active = record.target_turn_id.as_deref().is_some_and(|turn_id| {
            executor
                .runtime
                .is_session_turn_active(&record.session_id, turn_id)
        });
        if !target_active {
            let target_is_terminal = match session_service
                .durable_target_turn_is_terminal(&record)
                .await
            {
                Ok(terminal) => terminal,
                Err(error) => {
                    record_ingress_failure(
                        &session_service,
                        &worker_id,
                        &record,
                        &claim_token,
                        record.revision,
                        OutboxFailureClass::Retryable,
                        &error.to_string(),
                    )
                    .await;
                    return;
                }
            };
            let (decision, target_turn_id, reason) = if target_is_terminal {
                (
                    InputRoutingDecision::StartNewTurn,
                    None,
                    "durable target turn is terminal; input starts a new turn",
                )
            } else {
                (
                    record.decision,
                    record.target_turn_id.as_deref(),
                    "durable target turn is recovering; relation retained",
                )
            };
            match session_service
                .requeue_ingress_work(
                    &record,
                    &worker_id,
                    &claim_token,
                    record.revision,
                    decision,
                    target_turn_id,
                    reason,
                    now_ms(),
                )
                .await
            {
                Ok(_) => tracing::debug!(
                    request_id = %record.request_id,
                    target_terminal = target_is_terminal,
                    "requeued turn-targeted input from durable target state"
                ),
                Err(error) => tracing::warn!(
                    request_id = %record.request_id,
                    %error,
                    "failed to reclassify stale turn-targeted input"
                ),
            }
            return;
        }
    }

    if record.decision == InputRoutingDecision::InterruptAndReplan {
        executor
            .runtime
            .cancel_active_session(&record.session_id, "durable interrupt-and-replan input");
        match session_service
            .requeue_ingress_work(
                &record,
                &worker_id,
                &claim_token,
                record.revision,
                InputRoutingDecision::StartNewTurn,
                None,
                "active turn interrupted; input promoted to replacement turn",
                now_ms(),
            )
            .await
        {
            Ok(_) => {}
            Err(error) => tracing::warn!(
                request_id = %record.request_id,
                %error,
                "failed to promote interrupt input to replacement turn"
            ),
        }
        return;
    }

    let running = match session_service
        .mark_ingress_running(&record, &worker_id, &claim_token, record.revision, now_ms())
        .await
    {
        Ok(running) => running,
        Err(error) => {
            tracing::warn!(request_id = %record.request_id, %error, "Session input claim became stale before execution");
            return;
        }
    };
    tracing::debug!(
        request_id = %running.request_id,
        worker_id,
        revision = running.revision,
        claim_expires_at_ms = ?running.claim_expires_at_ms,
        "Session ingress claim entered running state"
    );

    if matches!(
        record.decision,
        InputRoutingDecision::SupplementCurrentTurn | InputRoutingDecision::ControlOrApproval
    ) {
        if record.decision == InputRoutingDecision::ControlOrApproval {
            let normalized = content.trim().to_ascii_lowercase();
            let approval = executor.runtime.resolve_session_approval_control(
                &record.session_id,
                &content,
                record.classification_json.as_deref(),
            );
            if let Err(error) = approval {
                record_ingress_failure(
                    &session_service,
                    &worker_id,
                    &record,
                    &claim_token,
                    running.revision,
                    OutboxFailureClass::Permanent,
                    &error,
                )
                .await;
                return;
            }
            if normalized.starts_with("/stop")
                || normalized.starts_with("/cancel")
                || (normalized.starts_with("/deny")
                    && executor
                        .runtime
                        .runtime_services()
                        .approval_queue()
                        .pending()
                        .iter()
                        .all(|request| {
                            request.source.session_id.as_deref() != Some(&record.session_id)
                        }))
            {
                executor
                    .runtime
                    .cancel_active_session(&record.session_id, "durable Session control input");
            }
        }
        let status = if record.decision == InputRoutingDecision::ControlOrApproval {
            SessionInputStatus::ControlResolved
        } else {
            SessionInputStatus::AttachedToTurn
        };
        let delivered = executor
            .runtime
            .deliver_durable_session_input_view(&record, content, status)
            .await;
        match delivered {
            Ok(()) => {
                if let Err(error) = session_service
                    .complete_ingress_work(
                        &record,
                        &worker_id,
                        &claim_token,
                        running.revision,
                        SessionRuntimeInputStatus::Supplemented,
                        0,
                        now_ms(),
                    )
                    .await
                {
                    tracing::warn!(request_id = %record.request_id, %error, "supplement delivery acknowledgement was fenced");
                }
            }
            Err(error) => {
                record_ingress_failure(
                    &session_service,
                    &worker_id,
                    &record,
                    &claim_token,
                    running.revision,
                    OutboxFailureClass::Retryable,
                    &error.message(),
                )
                .await;
            }
        }
        return;
    }

    if matches!(
        record.decision,
        InputRoutingDecision::RejectDuplicate | InputRoutingDecision::RejectPolicy
    ) {
        record_ingress_failure(
            &session_service,
            &worker_id,
            &record,
            &claim_token,
            running.revision,
            OutboxFailureClass::Permanent,
            "Session input was rejected by durable classification policy",
        )
        .await;
        return;
    }

    execute_primary_ingress_with_lease(
        &session_service,
        &executor,
        &worker_id,
        &running,
        &claim_token,
        running.revision,
        &content,
        lease,
    )
    .await;
}

async fn acknowledge_checkpoint_consumed_ingress(
    session_service: &SessionService,
    worker_id: &str,
    record: &SessionRuntimeOutboxRecord,
    claim_token: &str,
) {
    let running = match session_service
        .mark_ingress_running(record, worker_id, claim_token, record.revision, now_ms())
        .await
    {
        Ok(running) => running,
        Err(error) => {
            tracing::warn!(
                request_id = %record.request_id,
                %error,
                "checkpoint-consumed Session input claim became stale before acknowledgement"
            );
            return;
        }
    };
    if let Err(error) = session_service
        .complete_ingress_work(
            record,
            worker_id,
            claim_token,
            running.revision,
            SessionRuntimeInputStatus::Supplemented,
            0,
            now_ms(),
        )
        .await
    {
        tracing::warn!(
            request_id = %record.request_id,
            %error,
            "checkpoint-consumed Session input acknowledgement was fenced"
        );
    }
}

async fn execute_primary_ingress_with_lease(
    session_service: &SessionService,
    executor: &GatewaySessionIngressExecutor,
    worker_id: &str,
    record: &SessionRuntimeOutboxRecord,
    claim_token: &str,
    mut revision: u64,
    content: &str,
    lease: ExecutionResourceLease,
) {
    let execution = executor.execute_ingress_with_lease(record, content, lease);
    tokio::pin!(execution);
    let heartbeat_ms = (LEASE_MS / 3).max(1);
    let outcome = loop {
        tokio::select! {
            outcome = &mut execution => {
                break Some(outcome)
            },
            _ = tokio::time::sleep(Duration::from_millis(heartbeat_ms)) => {
                match session_service.renew_ingress_lease(
                    record,
                    worker_id,
                    claim_token,
                    revision,
                    now_ms(),
                    LEASE_MS,
                ).await {
                    Ok(renewed) => revision = renewed.revision,
                    Err(error) => {
                        executor
                            .lease_lost
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::error!(
                            request_id = %record.request_id,
                            %error,
                            "Session input lease lost; cancelling Runtime and fencing terminal writes"
                        );
                        executor.runtime.cancel_active_session(
                            &record.session_id,
                            "Session input lease ownership lost",
                        );
                        if tokio::time::timeout(Duration::from_secs(5), &mut execution)
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                request_id = %record.request_id,
                                "Session Runtime did not stop inside the lease-loss grace period; dropping the owned execution future"
                            );
                        }
                        break None;
                    }
                }
            }
        }
    };
    match outcome {
        Some(Ok(executed)) => {
            tracing::debug!(
                request_id = %record.request_id,
                worker_id,
                revision,
                commit_cursor = executed.commit_cursor,
                "Session Runtime execution returned to ingress worker"
            );
            let already_settled = session_service
                .ingress_completed_at(&record.request_id, executed.commit_cursor)
                .await
                .unwrap_or(false);
            if already_settled {
                return;
            }
            if let Err(error) = session_service
                .complete_ingress_work(
                    record,
                    worker_id,
                    claim_token,
                    revision,
                    SessionRuntimeInputStatus::Completed,
                    executed.commit_cursor,
                    now_ms(),
                )
                .await
            {
                tracing::error!(
                    request_id = %record.request_id,
                    %error,
                    "Runtime completed but durable Session acknowledgement was fenced"
                );
            }
        }
        Some(Err(error)) => {
            tracing::warn!(
                request_id = %record.request_id,
                worker_id,
                revision,
                %error,
                "Session Runtime execution failed before durable ingress acknowledgement"
            );
            if error.contains(SESSION_RUNTIME_BUSY_ERROR) {
                match session_service
                    .requeue_ingress_work(
                        record,
                        worker_id,
                        claim_token,
                        revision,
                        record.decision,
                        record.target_turn_id.as_deref(),
                        "session runtime owner is busy; preserve the input and retry after the active turn",
                        now_ms(),
                    )
                    .await
                {
                    Ok(_) => tracing::debug!(
                        request_id = %record.request_id,
                        session_id = %record.session_id,
                        "requeued concurrent primary input without consuming its retry budget"
                    ),
                    Err(requeue_error) => tracing::error!(
                        request_id = %record.request_id,
                        session_id = %record.session_id,
                        %requeue_error,
                        "failed to requeue concurrent primary input"
                    ),
                }
            } else {
                record_ingress_failure(
                    session_service,
                    worker_id,
                    record,
                    claim_token,
                    revision,
                    classify_ingress_failure(&error),
                    &error,
                )
                .await;
            }
        }
        None => {}
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_ingress_failure(
    session_service: &SessionService,
    worker_id: &str,
    record: &SessionRuntimeOutboxRecord,
    claim_token: &str,
    revision: u64,
    class: OutboxFailureClass,
    error: &str,
) {
    if let Err(persist_error) = session_service
        .fail_ingress_work(
            record,
            worker_id,
            claim_token,
            revision,
            class,
            error,
            now_ms().saturating_add(retry_delay_ms(record.attempts)),
            MAX_ATTEMPTS,
            now_ms(),
        )
        .await
    {
        tracing::error!(
            request_id = %record.request_id,
            error = %persist_error,
            work_error = error,
            "Session input failure state could not be persisted"
        );
    }
}

fn classify_ingress_failure(error: &str) -> OutboxFailureClass {
    if error.contains("authorization") || error.contains("approval") {
        OutboxFailureClass::AuthorizationBlocked
    } else if error.contains("payload") || error.contains("JSON") {
        OutboxFailureClass::CorruptPayload
    } else if error.contains("invalid") || error.contains("unavailable until") {
        OutboxFailureClass::Permanent
    } else {
        OutboxFailureClass::Retryable
    }
}

async fn run_delivery_worker(
    event_store: runtime::SessionTerminalDeliveryPort,
    session_service: Arc<SessionService>,
    event_bus: Arc<SessionProjectionHub>,
    reporter: WorkerBackendReporter,
    mut shutdown: watch::Receiver<bool>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), String> {
    let worker_id = format!("gateway-delivery:{}", uuid::Uuid::new_v4());
    if let Err(error) = event_store.health() {
        let message = error.to_string();
        reporter.failure(message.clone());
        let _ = ready.send(Err(message.clone()));
        return Err(message);
    }
    let mut commits = event_store.subscribe_commits();
    reporter.success(None);
    signal_worker_ready(ready)?;
    loop {
        if *shutdown.borrow() {
            break;
        }
        let claim_store = event_store.clone();
        let claim_worker = worker_id.clone();
        let claimed = tokio::task::spawn_blocking(move || {
            claim_store.claim(&claim_worker, now_ms(), LEASE_MS, WORKER_BATCH)
        })
        .await;
        let had_work = match claimed {
            Ok(Ok(records)) => {
                let had_work = !records.is_empty();
                reporter.success(None);
                for record in records {
                    let _ = deliver_terminal(
                        &event_store,
                        &session_service,
                        &event_bus,
                        &worker_id,
                        record,
                    )
                    .await;
                }
                had_work
            }
            Ok(Err(error)) => {
                let message = error.to_string();
                tracing::error!(%error, "terminal outbox claim failed");
                if reporter.failure(message.clone()) {
                    return Err(message);
                }
                false
            }
            Err(error) => {
                let message = error.to_string();
                tracing::error!(%error, "terminal outbox worker join failed");
                if reporter.failure(message.clone()) {
                    return Err(message);
                }
                false
            }
        };
        if had_work {
            continue;
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = commits.changed() => {},
            () = tokio::time::sleep(CROSS_PROCESS_RECONCILIATION_FALLBACK) => {},
        }
    }
    Ok(())
}

async fn deliver_terminal(
    event_store: &runtime::SessionTerminalDeliveryPort,
    session_service: &SessionService,
    event_bus: &SessionProjectionHub,
    worker_id: &str,
    record: runtime::RuntimeSessionOutboxRecord,
) -> Result<bool, String> {
    let outcome = match decode_terminal_payload(&record.payload_ref) {
        Ok(payload) => {
            let mut transcript = payload.transcript.unwrap_or_else(|| {
                vec![DecodedTerminalTranscriptMessage {
                    role: "assistant".to_string(),
                    content_json: serde_json::json!([
                        { "type": "text", "text": payload.text.clone() }
                    ])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: payload.token_usage_json.clone(),
                }]
            });
            annotate_terminal_tool_instances(
                &mut transcript,
                record.execution_id.as_deref(),
                record.turn_id.as_deref(),
                payload.ingress_message_id.as_deref(),
            );
            let transcript_len = transcript.len();
            let messages = transcript
                .into_iter()
                .enumerate()
                .map(|(index, message)| session::SessionMessage {
                    stable_message_id: if index + 1 == transcript_len {
                        record.message_id.clone()
                    } else {
                        format!("{}:transcript:{index}", record.message_id)
                    },
                    session_id: record.session_id.clone(),
                    sequence: index,
                    role: message.role,
                    content_json: message.content_json,
                    blocks_count: message.blocks_count,
                    tool_use_id: message.tool_use_id,
                    tool_name: message.tool_name,
                    token_usage_json: message.token_usage_json,
                    created_at_ms: 0,
                })
                .collect::<Vec<_>>();
            let durable_input = match record.request_id.as_deref() {
                Some(request_id) => session_service.runtime_input(request_id).await,
                None => Ok(None),
            };
            let terminal_commit = match (
                durable_input,
                record.request_id.as_ref(),
                record.session_generation,
                record.input_sequence,
                record.input_claim_owner.as_ref(),
                record.input_claim_token.as_ref(),
                record.input_claim_revision,
                record.turn_id.as_ref(),
                payload.ingress_message_id.as_ref(),
            ) {
                (
                    Ok(Some(input)),
                    Some(request_id),
                    Some(generation),
                    Some(input_sequence),
                    Some(claim_owner),
                    Some(claim_token),
                    Some(claim_revision),
                    Some(turn_id),
                    Some(ingress_message_id),
                ) if input.session_id == record.session_id
                    && input.turn_id == *turn_id
                    && input.message_id == *ingress_message_id
                    && input.sequence == input_sequence as usize
                    && input.session_generation == generation
                    && input.claim_owner.as_deref() == Some(claim_owner.as_str())
                    && input.claim_token.as_deref() == Some(claim_token.as_str())
                    && input.claim_fence_epoch == Some(claim_revision)
                    && matches!(
                        input.status,
                        session::SessionRuntimeInputStatus::Running
                            | session::SessionRuntimeInputStatus::Completed
                    ) =>
                {
                    Ok(session::SessionTerminalTranscriptCommit {
                        terminal_message_id: record.message_id.clone(),
                        ingress_message_id: ingress_message_id.clone(),
                        session_id: record.session_id.clone(),
                        turn_id: turn_id.clone(),
                        messages,
                        runtime_commit_cursor: record.commit_cursor,
                        consumed_input_sequence: payload
                            .consumed_input_sequence
                            .unwrap_or(input_sequence as usize)
                            .max(input_sequence as usize),
                        created_at_ms: now_ms(),
                        fence: session::SessionTerminalExecutionFence {
                            request_id: request_id.clone(),
                            input_sequence: input_sequence as usize,
                            session_generation: generation,
                            claim_owner: claim_owner.clone(),
                            claim_token: claim_token.clone(),
                            claim_fence_epoch: claim_revision,
                        },
                    })
                }
                (Err(error), ..) => Err(error),
                _ => Err(session::SessionError::StaleExecutionFence(format!(
                    "terminal `{}` has no complete, identity-matched Session execution fence",
                    record.terminal_id
                ))),
            };
            let write = match terminal_commit {
                Ok(commit) => session_service
                    .commit_terminal_transcript(&commit)
                    .await
                    .map(|receipt| (receipt.messages, receipt.inserted)),
                Err(error) => Err(error),
            };
            match write {
                Ok((messages, inserted)) => {
                    if inserted {
                        session_service.schedule_context_index_reconciliation(&record.session_id);
                    }
                    let terminal = messages.last().cloned().ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript committed no terminal row".to_string(),
                        )
                    });
                    terminal.map(|terminal| {
                        (payload.text, payload.token_usage_json, terminal, inserted)
                    })
                }
                Err(session::SessionError::StaleExecutionFence(error)) => Err((
                    runtime::RuntimeSessionOutboxFailureClass::Permanent,
                    format!("stale terminal fence: {error}"),
                )),
                Err(error) => Err((
                    runtime::RuntimeSessionOutboxFailureClass::Permanent,
                    error.to_string(),
                )),
            }
        }
        Err(error) => Err(error),
    };
    match outcome {
        Ok((text, token_usage_json, message, inserted)) => {
            // The message write is exactly-once; delivery notification is
            // intentionally at-least-once. A process can die after commit but
            // before broadcast, so suppressing a duplicate notification would
            // leave live Surfaces permanently waiting. Stable terminal/message
            // identities make replay harmless and let each Surface dedupe.
            let token_usage = token_usage_json
                .as_deref()
                .and_then(|usage| serde_json::from_str(usage).ok());
            let event = SessionProjectionEvent::TerminalCommitted {
                session_id: record.session_id.clone(),
                terminal_id: record.terminal_id.clone(),
                message_id: record.message_id.clone(),
                sequence: message.sequence,
                response: text,
                runtime_commit_cursor: record.commit_cursor,
                replayed: !inserted,
                token_usage,
                execution_id: record.execution_id.clone(),
                turn_id: record.turn_id.clone(),
            };
            event_bus.publish(&record.session_id, event).await;
            let event_store = event_store.clone();
            let terminal_id = record.terminal_id.clone();
            let worker = worker_id.to_string();
            let revision = record.revision;
            let acknowledgement = tokio::task::spawn_blocking(move || {
                event_store.acknowledge(&terminal_id, &worker, revision, now_ms())
            })
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
            if let Err(error) = acknowledgement {
                // The durable message ID makes replay safe. Leaving the lease
                // unacked intentionally lets the next worker take it over.
                tracing::error!(terminal_id = %record.terminal_id, %error, "terminal append committed but ack failed");
            }
            Ok(inserted)
        }
        Err((class, error)) => {
            let delivery_error = error.clone();
            let event_store = event_store.clone();
            let terminal_id = record.terminal_id.clone();
            let worker = worker_id.to_string();
            let revision = record.revision;
            let retry_at = now_ms().saturating_add(retry_delay_ms(record.attempts));
            let failure_record = tokio::task::spawn_blocking(move || {
                event_store.fail(
                    &terminal_id,
                    &worker,
                    revision,
                    class,
                    &error,
                    retry_at,
                    MAX_ATTEMPTS,
                    now_ms(),
                )
            })
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
            if let Err(failure) = failure_record {
                tracing::error!(terminal_id = %record.terminal_id, error = %failure, "terminal failure state could not be recorded");
            }
            Err(delivery_error)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedTerminalPayload {
    pub(crate) text: String,
    pub(crate) token_usage_json: Option<String>,
    pub(crate) ingress_message_id: Option<String>,
    pub(crate) transcript: Option<Vec<DecodedTerminalTranscriptMessage>>,
    pub(crate) consumed_input_sequence: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedTerminalTranscriptMessage {
    pub(crate) role: String,
    pub(crate) content_json: String,
    pub(crate) blocks_count: usize,
    pub(crate) tool_use_id: Option<String>,
    pub(crate) tool_name: Option<String>,
    pub(crate) token_usage_json: Option<String>,
}

fn annotate_terminal_tool_instances(
    transcript: &mut [DecodedTerminalTranscriptMessage],
    execution_id: Option<&str>,
    turn_id: Option<&str>,
    ingress_message_id: Option<&str>,
) {
    let mut ordinals = HashMap::<String, u64>::new();
    let mut pending = HashMap::<String, VecDeque<String>>::new();
    for message in transcript {
        let Ok(mut blocks) = serde_json::from_str::<Vec<serde_json::Value>>(&message.content_json)
        else {
            continue;
        };
        for block in &mut blocks {
            let Some(object) = block.as_object_mut() else {
                continue;
            };
            if let Some(turn_id) = turn_id {
                object.insert(
                    "cowd_turn_id".to_string(),
                    serde_json::Value::String(turn_id.to_string()),
                );
            }
            if let Some(ingress_message_id) = ingress_message_id {
                object.insert(
                    "cowd_turn_ingress_message_id".to_string(),
                    serde_json::Value::String(ingress_message_id.to_string()),
                );
            }
            if let Some(execution_id) = execution_id {
                object.insert(
                    "cowd_execution_id".to_string(),
                    serde_json::Value::String(execution_id.to_string()),
                );
            }
            let (provider_id, is_use) = match object.get("type").and_then(serde_json::Value::as_str)
            {
                Some("tool_use") => (
                    object
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    true,
                ),
                Some("tool_result") => (
                    object
                        .get("tool_use_id")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    false,
                ),
                _ => (None, false),
            };
            let Some(provider_id) = provider_id else {
                continue;
            };
            let instance_id = if is_use {
                let ordinal = ordinals.entry(provider_id.clone()).or_default();
                let instance_id = format!("{provider_id}#cowd-{ordinal}");
                *ordinal = ordinal.saturating_add(1);
                pending
                    .entry(provider_id)
                    .or_default()
                    .push_back(instance_id.clone());
                instance_id
            } else {
                pending
                    .entry(provider_id.clone())
                    .or_default()
                    .pop_front()
                    .unwrap_or_else(|| {
                        let ordinal = ordinals.entry(provider_id.clone()).or_default();
                        let instance_id = format!("{provider_id}#cowd-{ordinal}");
                        *ordinal = ordinal.saturating_add(1);
                        instance_id
                    })
            };
            object.insert(
                "cowd_tool_instance_id".to_string(),
                serde_json::Value::String(instance_id),
            );
        }
        if let Ok(content_json) = serde_json::to_string(&blocks) {
            message.content_json = content_json;
        }
    }
}

pub(crate) fn decode_terminal_payload(
    payload_ref: &str,
) -> Result<DecodedTerminalPayload, (runtime::RuntimeSessionOutboxFailureClass, String)> {
    if let Some(encoded) = payload_ref
        .strip_prefix("assistant_terminal_v2:")
        .or_else(|| payload_ref.strip_prefix("assistant_terminal_v1:"))
    {
        let is_v2 = payload_ref.starts_with("assistant_terminal_v2:");
        let payload = serde_json::from_str::<serde_json::Value>(encoded).map_err(|error| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                error.to_string(),
            )
        })?;
        let text = payload
            .get("text")
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.is_empty())
            .ok_or_else(|| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal payload has no visible text".to_string(),
                )
            })?
            .to_string();
        let token_usage_json = decode_terminal_usage(payload.get("token_usage"), is_v2)?;
        let ingress_message_id = if is_v2 {
            Some(
                payload
                    .get("ingress_message_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|message_id| !message_id.trim().is_empty())
                    .ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript requires ingress_message_id".to_string(),
                        )
                    })?
                    .to_string(),
            )
        } else {
            None
        };
        let consumed_input_sequence = if is_v2 {
            Some(
                payload
                    .get("consumed_input_sequence")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript requires consumed_input_sequence".to_string(),
                        )
                    })?,
            )
        } else {
            None
        };
        let transcript = if is_v2 {
            let messages = payload
                .get("transcript")
                .and_then(serde_json::Value::as_array)
                .filter(|messages| !messages.is_empty() && messages.len() <= 10_000)
                .ok_or_else(|| {
                    (
                        runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                        "terminal transcript must contain 1..=10000 messages".to_string(),
                    )
                })?;
            let mut decoded = Vec::with_capacity(messages.len());
            for message in messages {
                let object = message.as_object().ok_or_else(|| {
                    (
                        runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                        "terminal transcript message must be an object".to_string(),
                    )
                })?;
                let role = object
                    .get("role")
                    .and_then(serde_json::Value::as_str)
                    .filter(|role| matches!(*role, "system" | "user" | "assistant" | "tool"))
                    .ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript message has an invalid role".to_string(),
                        )
                    })?
                    .to_string();
                let blocks = object
                    .get("blocks")
                    .and_then(serde_json::Value::as_array)
                    .filter(|blocks| !blocks.is_empty())
                    .ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript message must contain blocks".to_string(),
                        )
                    })?;
                let (tool_use_id, tool_name) = blocks
                    .iter()
                    .find_map(|block| {
                        let block = block.as_object()?;
                        match block.get("type").and_then(serde_json::Value::as_str)? {
                            "tool_use" => Some((
                                block
                                    .get("id")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                                block
                                    .get("name")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                            )),
                            "tool_result" => Some((
                                block
                                    .get("tool_use_id")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                                block
                                    .get("tool_name")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                            )),
                            _ => None,
                        }
                    })
                    .unwrap_or((None, None));
                decoded.push(DecodedTerminalTranscriptMessage {
                    role,
                    content_json: serde_json::to_string(blocks).map_err(|error| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            error.to_string(),
                        )
                    })?,
                    blocks_count: blocks.len(),
                    tool_use_id,
                    tool_name,
                    token_usage_json: decode_terminal_usage(object.get("usage"), false)?,
                });
            }
            let terminal = decoded.last_mut().ok_or_else(|| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal transcript has no final message".to_string(),
                )
            })?;
            let terminal_has_text = terminal.role == "assistant"
                && serde_json::from_str::<serde_json::Value>(&terminal.content_json)
                    .ok()
                    .and_then(|value| value.as_array().cloned())
                    .is_some_and(|blocks| {
                        blocks.iter().any(|block| {
                            block.get("type").and_then(serde_json::Value::as_str) == Some("text")
                                && block.get("text").and_then(serde_json::Value::as_str)
                                    == Some(text.as_str())
                        })
                    });
            if !terminal_has_text {
                return Err((
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal transcript final assistant row does not contain terminal text"
                        .to_string(),
                ));
            }
            if terminal.token_usage_json.is_none() {
                terminal.token_usage_json = token_usage_json.clone();
            }
            Some(decoded)
        } else {
            None
        };
        return Ok(DecodedTerminalPayload {
            text,
            token_usage_json,
            ingress_message_id,
            transcript,
            consumed_input_sequence,
        });
    }
    let encoded = payload_ref.strip_prefix("assistant_json:").ok_or_else(|| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal payload does not use a supported typed schema".to_string(),
        )
    })?;
    serde_json::from_str::<String>(encoded)
        .map(|text| DecodedTerminalPayload {
            text,
            token_usage_json: None,
            ingress_message_id: None,
            transcript: None,
            consumed_input_sequence: None,
        })
        .map_err(|error| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                error.to_string(),
            )
        })
}

fn decode_terminal_usage(
    usage: Option<&serde_json::Value>,
    required_core_fields: bool,
) -> Result<Option<String>, (runtime::RuntimeSessionOutboxFailureClass, String)> {
    let Some(usage) = usage else {
        return if required_core_fields {
            Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "terminal token_usage is required".to_string(),
            ))
        } else {
            Ok(None)
        };
    };
    let usage = usage.as_object().ok_or_else(|| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal token_usage must be an object".to_string(),
        )
    })?;
    for field in [
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ] {
        let value = usage.get(field);
        if required_core_fields
            && matches!(field, "input_tokens" | "output_tokens")
            && value.is_none()
        {
            return Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                format!("terminal token_usage.{field} is required"),
            ));
        }
        if value.is_some_and(|value| value.as_u64().is_none_or(|value| value > i64::MAX as u64)) {
            return Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                format!(
                    "terminal token_usage.{field} must be a non-negative 64-bit database integer"
                ),
            ));
        }
    }
    serde_json::to_string(usage).map(Some).map_err(|error| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            error.to_string(),
        )
    })
}

fn retry_delay_ms(attempt: u32) -> u64 {
    250_u64.saturating_mul(1_u64 << attempt.min(8))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        gateway::HotSessionPool,
        services::session_service::{
            presence::SessionPresenceLedger, repository::SessionRepository,
        },
    };
    use session::{SessionMissionOutboxOperation, SessionMissionOutboxRequest, SessionRecord};

    fn test_backend_reporter(name: &'static str) -> WorkerBackendReporter {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        set_worker_state(&states, name, SessionWorkerState::Starting);
        WorkerBackendReporter { name, states }
    }

    #[test]
    fn concurrent_session_owner_conflict_remains_retryable() {
        assert_eq!(
            classify_ingress_failure(SESSION_RUNTIME_BUSY_ERROR),
            OutboxFailureClass::Retryable
        );
    }

    #[tokio::test]
    async fn checkpoint_consumed_supplement_is_acknowledged_without_reclassification() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&SessionRecord {
                session_id: "supplement-session".to_string(),
                platform: "test".to_string(),
                chat_id: "supplement-session".to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        store
            .append_ingress_with_runtime_outbox(
                "supplement-session",
                "user",
                Some(r#"[{"type":"text","text":"late supplement"}]"#),
                1,
                &session::SessionRuntimeOutboxRequest {
                    input_id: "supplement-input".to_string(),
                    request_id: "supplement-request".to_string(),
                    turn_id: "supplement-message-turn".to_string(),
                    message_id: "supplement-message".to_string(),
                    session_generation: 1,
                    decision: InputRoutingDecision::SupplementCurrentTurn,
                    target_turn_id: Some("turn-active".to_string()),
                    classification_json: None,
                    created_at_ms: 1,
                    runtime_options_json: None,
                },
            )
            .await
            .unwrap();
        let session_service = test_session_service(Arc::clone(&store), SessionProjectionHub::new());
        let record = session_service
            .claim_ingress_work("checkpoint-worker", now_ms(), LEASE_MS, 1)
            .await
            .unwrap()
            .pop()
            .expect("claimed supplement");
        let claim_token = record.claim_token.clone().expect("claim token");

        acknowledge_checkpoint_consumed_ingress(
            &session_service,
            "checkpoint-worker",
            &record,
            &claim_token,
        )
        .await;

        let persisted = session_service
            .runtime_input("supplement-request")
            .await
            .unwrap()
            .expect("persisted input");
        assert_eq!(persisted.status, SessionRuntimeInputStatus::Supplemented);
        assert_eq!(
            persisted.decision,
            InputRoutingDecision::SupplementCurrentTurn
        );
        assert_eq!(persisted.target_turn_id.as_deref(), Some("turn-active"));
        assert_eq!(persisted.runtime_commit_cursor, Some(0));
    }

    fn test_session_service(
        store: Arc<UnifiedSessionStore>,
        event_bus: Arc<SessionProjectionHub>,
    ) -> Arc<SessionService> {
        let repository = Arc::new(SessionRepository::new(
            Arc::new(HotSessionPool::new()),
            Some(Arc::clone(&store)),
            event_bus,
        ));
        Arc::new(SessionService::for_tests(
            repository,
            Arc::new(SessionPresenceLedger::with_store(store)),
        ))
    }

    async fn delivery_fixture() -> (
        Arc<runtime::RuntimeEventStore>,
        runtime::SessionTerminalDeliveryPort,
        Arc<SessionService>,
        Arc<UnifiedSessionStore>,
        Arc<SessionProjectionHub>,
        crate::event_bus::SessionProjectionSubscription,
    ) {
        let runtime_event_store =
            Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap());
        let fixture_root = std::env::temp_dir()
            .join("cowd-terminal-delivery-fixtures")
            .join(uuid::Uuid::new_v4().to_string());
        let home = fixture_root.join("home");
        let workspace = fixture_root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let event_store = runtime::RuntimeServices::builder(&home, &workspace)
            .runtime_event_store(Arc::clone(&runtime_event_store))
            .build()
            .unwrap()
            .session_terminal_delivery();
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "chat".to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let event_bus = SessionProjectionHub::new();
        let rx = event_bus.subscribe("s1", 8).await;
        let session_service = test_session_service(Arc::clone(&store), Arc::clone(&event_bus));
        (
            runtime_event_store,
            event_store,
            session_service,
            store,
            event_bus,
            rx,
        )
    }

    async fn enqueue_fenced_terminal(
        runtime_event_store: &runtime::RuntimeEventStore,
        store: &UnifiedSessionStore,
        terminal_id: &str,
        message_id: &str,
        request_id: &str,
        turn_id: &str,
        ingress_message_id: &str,
        payload_ref: &str,
    ) -> u64 {
        store
            .append_ingress_with_runtime_outbox(
                "s1",
                "user",
                Some(r#"[{"type":"text","text":"fixture ingress"}]"#),
                1,
                &session::SessionRuntimeOutboxRequest {
                    input_id: request_id.to_string(),
                    request_id: request_id.to_string(),
                    turn_id: turn_id.to_string(),
                    message_id: ingress_message_id.to_string(),
                    session_generation: 1,
                    decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                    target_turn_id: None,
                    classification_json: None,
                    created_at_ms: 1,
                    runtime_options_json: None,
                },
            )
            .await
            .unwrap();
        let now = now_ms();
        let claimed = store
            .claim_session_runtime_outbox("session-worker", now, 30_000, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let claim_token = claimed.claim_token.clone().unwrap();
        let running = store
            .mark_session_runtime_outbox_running(
                request_id,
                "session-worker",
                claimed.session_generation,
                &claim_token,
                claimed.revision,
                now,
            )
            .await
            .unwrap();
        runtime_event_store
            .append_transaction_with_terminal(
                runtime::AppendTransactionRequest {
                    transaction_id: format!("terminal-fixture:{terminal_id}"),
                    expected_streams: vec![runtime::ExpectedStreamRevision {
                        stream_id: format!("turn:{turn_id}"),
                        expected_revision: 0,
                    }],
                    events: vec![runtime::RuntimeTransactionEventInput {
                        event: runtime::RuntimeEventInput {
                            stream_id: format!("turn:{turn_id}"),
                            scope: runtime::RuntimeEventScope::SessionInput,
                            kind: "turn.terminal_committed".to_string(),
                            status: Some("completed".to_string()),
                            actor: Some("terminal-delivery-fixture".to_string()),
                            refs: Vec::new(),
                            payload: serde_json::json!({"terminal_id": terminal_id}),
                        },
                        idempotency_key: Some(format!("terminal-event:{terminal_id}")),
                        schema_version: 1,
                    }],
                },
                runtime::SessionTerminalInput {
                    terminal_id: terminal_id.to_string(),
                    message_id: message_id.to_string(),
                    session_id: "s1".to_string(),
                    execution_id: Some(format!("execution:{request_id}")),
                    turn_id: Some(turn_id.to_string()),
                    request_id: Some(request_id.to_string()),
                    session_generation: Some(running.session_generation),
                    input_sequence: Some(running.sequence as u64),
                    input_claim_owner: running.claim_owner,
                    input_claim_token: running.claim_token,
                    input_claim_revision: running.claim_fence_epoch,
                    payload_ref: payload_ref.to_string(),
                },
            )
            .unwrap()
            .commit_cursor
    }

    #[test]
    fn terminal_payload_requires_typed_prefix() {
        assert_eq!(
            decode_terminal_payload("assistant_json:\"done\"")
                .unwrap()
                .text,
            "done"
        );
        let payload = decode_terminal_payload(
            r#"assistant_terminal_v1:{"text":"done","token_usage":{"input_tokens":12,"output_tokens":3}}"#,
        )
        .unwrap();
        assert_eq!(payload.text, "done");
        assert!(payload
            .token_usage_json
            .as_deref()
            .is_some_and(|usage| usage.contains("\"input_tokens\":12")));
        assert!(decode_terminal_payload(
            r#"assistant_terminal_v1:{"text":"done","token_usage":{"input_tokens":"12","output_tokens":3}}"#
        )
        .is_err());
        assert!(decode_terminal_payload("evidence:1").is_err());
    }

    #[tokio::test]
    async fn stop_accepting_does_not_stop_supervised_workers() {
        let supervisor = SessionWorkerSupervisor::for_tests();

        supervisor.stop_accepting();

        assert!(!supervisor.is_accepting());
        let health = supervisor.health();
        assert!(!health.accepting);
        assert!(REQUIRED_SESSION_WORKERS.iter().all(|name| {
            health
                .workers
                .get(*name)
                .is_some_and(|worker| worker.state == SessionWorkerState::Running)
        }));

        supervisor.shutdown().await;
        assert!(*supervisor.shutdown.borrow());
    }

    #[test]
    fn recovery_health_is_updated_after_background_restoration() {
        let supervisor = SessionWorkerSupervisor::for_tests();
        let recovery = crate::services::session_service::activation::SessionRecoverySummary {
            discovered: 7,
            required: 2,
            recovered: 2,
            ..Default::default()
        };

        supervisor.record_recovery(recovery);

        let health = supervisor.health();
        assert_eq!(health.recovery.discovered, 7);
        assert_eq!(health.recovery.required, 2);
        assert_eq!(health.recovery.recovered, 2);
        assert!(health.recovery_completed_at_ms > 0);
    }

    #[test]
    fn terminal_annotation_preserves_causality_on_non_tool_blocks() {
        let mut transcript = vec![DecodedTerminalTranscriptMessage {
            role: "assistant".to_string(),
            content_json: serde_json::json!([
                {"type": "thinking", "thinking": "reason"},
                {"type": "text", "text": "done"}
            ])
            .to_string(),
            blocks_count: 2,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
        }];

        annotate_terminal_tool_instances(
            &mut transcript,
            Some("execution-1"),
            Some("turn-1"),
            Some("ingress-1"),
        );

        let blocks =
            serde_json::from_str::<Vec<serde_json::Value>>(transcript[0].content_json.as_str())
                .unwrap();
        assert_eq!(blocks.len(), 2);
        for block in blocks {
            assert_eq!(
                block
                    .get("cowd_execution_id")
                    .and_then(serde_json::Value::as_str),
                Some("execution-1")
            );
            assert_eq!(
                block
                    .get("cowd_turn_id")
                    .and_then(serde_json::Value::as_str),
                Some("turn-1")
            );
            assert_eq!(
                block
                    .get("cowd_turn_ingress_message_id")
                    .and_then(serde_json::Value::as_str),
                Some("ingress-1")
            );
        }
    }

    #[tokio::test]
    async fn mission_membership_bridge_replays_registration_once() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        let record = SessionRecord {
            session_id: "mission-session".to_string(),
            platform: "test".to_string(),
            chat_id: "mission-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        let request = SessionMissionOutboxRequest {
            request_id: "mission-register-1".to_string(),
            session_id: record.session_id.clone(),
            title: "Mission session".to_string(),
            workspace_key: "workspace-a".to_string(),
            operation: SessionMissionOutboxOperation::Register,
            created_at_ms: 100,
        };
        store
            .upsert_session_with_mission_outbox(&record, &request)
            .await
            .unwrap();
        let session_service = test_session_service(Arc::clone(&store), SessionProjectionHub::new());
        let claimed = session_service
            .claim_mission_work("workspace-a", "worker", 100, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mission =
            runtime::MissionRuntimePort::new(runtime::RuntimeServices::in_memory().unwrap());

        materialize_mission_membership(&session_service, &mission, "worker", claimed).await;

        assert_eq!(
            mission.mission_id_for_session("mission-session"),
            Some(mission.default_mission_id().to_string())
        );
        assert_eq!(
            store
                .get_session_mission_outbox("mission-register-1")
                .await
                .unwrap()
                .unwrap()
                .status,
            session::OutboxStatus::Materialized
        );
    }

    #[tokio::test]
    async fn mission_membership_replay_after_lost_ack_is_idempotent() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        let record = SessionRecord {
            session_id: "mission-replay".to_string(),
            platform: "test".to_string(),
            chat_id: "mission-replay".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        let request = SessionMissionOutboxRequest {
            request_id: "mission-replay-1".to_string(),
            session_id: record.session_id.clone(),
            title: "Replay session".to_string(),
            workspace_key: "workspace-a".to_string(),
            operation: SessionMissionOutboxOperation::Register,
            created_at_ms: 100,
        };
        store
            .upsert_session_with_mission_outbox(&record, &request)
            .await
            .unwrap();
        let session_service = test_session_service(Arc::clone(&store), SessionProjectionHub::new());
        let mission =
            runtime::MissionRuntimePort::new(runtime::RuntimeServices::in_memory().unwrap());
        let first = session_service
            .claim_mission_work("workspace-a", "worker-a", 100, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();

        // Runtime applied the event, but the bridge process lost ownership
        // before the acknowledgement. A restarted worker must replay safely.
        materialize_mission_membership(&session_service, &mission, "wrong-worker", first).await;
        let replay = session_service
            .claim_mission_work("workspace-a", "worker-b", 150, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        materialize_mission_membership(&session_service, &mission, "worker-b", replay).await;

        let projection = mission.projection();
        assert_eq!(
            projection
                .aggregate
                .expect("default Mission aggregate")
                .session_refs
                .len(),
            1
        );
        assert_eq!(
            store
                .get_session_mission_outbox("mission-replay-1")
                .await
                .unwrap()
                .unwrap()
                .status,
            session::OutboxStatus::Materialized
        );
    }

    #[tokio::test]
    async fn append_success_ack_failure_replays_notification_without_duplicate_message() {
        let (runtime_event_store, event_store, session_service, store, event_bus, mut rx) =
            delivery_fixture().await;
        let private_reasoning = "private-provider-reasoning";
        let provider_signature = "provider-signature";
        let sealed_reasoning =
            runtime::provider_transcript::seal_provider_transcript(private_reasoning).unwrap();
        let sealed_signature =
            runtime::provider_transcript::seal_provider_transcript(provider_signature).unwrap();
        let payload_ref = format!(
            "assistant_terminal_v2:{}",
            serde_json::json!({
                "text": "done",
                "ingress_message_id": "ingress-1",
                "consumed_input_sequence": 0,
                "token_usage": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0
                },
                "transcript": [{
                    "role": "assistant",
                    "blocks": [
                        {"type": "reasoning_summary", "text": "public summary"},
                        {
                            "type": "thinking",
                            "thinking": sealed_reasoning,
                            "signature": sealed_signature
                        },
                        {"type": "text", "text": "done"}
                    ]
                }]
            })
        );
        let commit_cursor = enqueue_fenced_terminal(
            &runtime_event_store,
            &store,
            "t1",
            "m1",
            "request-1",
            "turn-1",
            "ingress-1",
            &payload_ref,
        )
        .await;
        let claim_at = now_ms();
        let record = event_store
            .claim("owner-a", claim_at, 10, 1)
            .unwrap()
            .pop()
            .unwrap();

        deliver_terminal(
            &event_store,
            &session_service,
            &event_bus,
            "wrong-owner",
            record,
        )
        .await
        .unwrap();
        let persisted = store.get_messages("s1", 0, 10).await.unwrap();
        let terminal_state = event_store.get("t1").unwrap().unwrap();
        let terminal_content = persisted
            .iter()
            .find(|message| message.stable_message_id == "m1")
            .map(|message| message.content_json.as_str())
            .unwrap();
        assert!(terminal_content.contains("public summary"));
        assert!(terminal_content.contains("cowd-provider-transcript:v1:"));
        assert!(!terminal_content.contains(private_reasoning));
        assert!(!terminal_content.contains(provider_signature));
        assert_eq!(
            persisted
                .iter()
                .filter(|message| message.stable_message_id == "m1")
                .count(),
            1,
            "terminal_state={terminal_state:?}, persisted={persisted:?}"
        );
        let terminal_event = rx.try_recv().unwrap().to_transport_value();
        assert_eq!(terminal_event["type"], "TerminalCommitted");
        assert_eq!(terminal_event["terminal_id"], "t1");
        assert_eq!(terminal_event["message_id"], "m1");
        assert_eq!(terminal_event["runtime_commit_cursor"], commit_cursor);
        assert_eq!(terminal_event["replayed"], false);
        assert_eq!(event_store.get("t1").unwrap().unwrap().status, "claimed");

        let reclaimed = event_store
            .claim("owner-b", claim_at + 11, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        deliver_terminal(
            &event_store,
            &session_service,
            &event_bus,
            "owner-b",
            reclaimed,
        )
        .await
        .unwrap();
        assert_eq!(
            store
                .get_messages("s1", 0, 10)
                .await
                .unwrap()
                .iter()
                .filter(|message| message.stable_message_id == "m1")
                .count(),
            1
        );
        let replayed = rx
            .try_recv()
            .expect("retry must rebroadcast")
            .to_transport_value();
        assert_eq!(replayed["terminal_id"], "t1");
        assert_eq!(replayed["message_id"], "m1");
        assert_eq!(replayed["replayed"], true);
        assert!(rx.try_recv().is_err(), "one retry emits one notification");
        assert_eq!(
            event_store.get("t1").unwrap().unwrap().status,
            "materialized"
        );
    }

    #[tokio::test]
    async fn generation_change_after_delivery_claim_rejects_terminal_without_projection() {
        let (runtime_event_store, event_store, session_service, store, event_bus, mut rx) =
            delivery_fixture().await;
        enqueue_fenced_terminal(
            &runtime_event_store,
            &store,
            "terminal-stale-generation",
            "message-stale-generation",
            "request-stale-generation",
            "turn-stale-generation",
            "ingress-stale-generation",
            r#"assistant_terminal_v2:{"text":"must not commit","ingress_message_id":"ingress-stale-generation","consumed_input_sequence":0,"token_usage":{"input_tokens":0,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0},"transcript":[{"role":"assistant","blocks":[{"type":"text","text":"must not commit"}]}]}"#,
        )
        .await;
        let claim_at = now_ms();
        let record = event_store
            .claim("delivery-stale-generation", claim_at, 30_000, 1)
            .unwrap()
            .pop()
            .unwrap();
        store
            .advance_session_input_generation(
                "s1",
                1,
                true,
                "test",
                "invalidate terminal after delivery claim",
                claim_at + 1,
            )
            .await
            .unwrap();

        let result = deliver_terminal(
            &event_store,
            &session_service,
            &event_bus,
            "delivery-stale-generation",
            record,
        )
        .await;
        assert!(result.is_err());
        assert!(store
            .get_all_messages("s1")
            .await
            .unwrap()
            .iter()
            .all(|message| message.stable_message_id != "message-stale-generation"));
        assert!(
            rx.try_recv().is_err(),
            "a rejected stale terminal must not reach Surface projections"
        );
        let terminal = event_store
            .get("terminal-stale-generation")
            .unwrap()
            .unwrap();
        assert_eq!(terminal.status, "blocked");
        assert!(
            terminal
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("stale terminal fence")),
            "blocked terminal: {terminal:?}"
        );
    }

    #[tokio::test]
    async fn corrupt_terminal_is_poisoned_and_visible_to_operations() {
        let (_runtime_event_store, event_store, session_service, _store, event_bus, _rx) =
            delivery_fixture().await;
        event_store
            .enqueue("poison", "m2", "s1", 8, "not-typed")
            .unwrap();
        let record = event_store
            .claim("worker", 100, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        assert!(
            deliver_terminal(&event_store, &session_service, &event_bus, "worker", record)
                .await
                .is_err()
        );
        let poison = event_store.blocked(10).unwrap();
        assert_eq!(poison.len(), 1);
        assert_eq!(poison[0].terminal_id, "poison");
        assert_eq!(poison[0].failure_class.as_deref(), Some("corrupt_payload"));
    }

    #[tokio::test]
    async fn typed_terminal_atomically_materializes_usage_and_session_counters_before_ack() {
        let (runtime_event_store, event_store, session_service, store, event_bus, _rx) =
            delivery_fixture().await;
        enqueue_fenced_terminal(
            &runtime_event_store,
            &store,
            "usage-terminal",
            "usage-message",
            "usage-request",
            "usage-turn",
            "usage-ingress",
            r#"assistant_terminal_v2:{"text":"done","ingress_message_id":"usage-ingress","consumed_input_sequence":0,"token_usage":{"input_tokens":12,"output_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":0},"transcript":[{"role":"assistant","blocks":[{"type":"text","text":"done"}]}]}"#,
        )
        .await;
        let record = event_store
            .claim("worker", now_ms(), 30_000, 1)
            .unwrap()
            .pop()
            .unwrap();

        deliver_terminal(&event_store, &session_service, &event_bus, "worker", record)
            .await
            .unwrap();

        let session = store.get_session("s1").await.unwrap().unwrap();
        let messages = store.get_messages("s1", 0, 10).await.unwrap();
        assert_eq!(session.message_count, 2);
        assert_eq!(session.input_tokens, 12);
        assert_eq!(session.output_tokens, 3);
        let terminal = messages
            .iter()
            .find(|message| message.stable_message_id == "usage-message")
            .unwrap();
        assert_eq!(
            terminal
                .token_usage_json
                .as_deref()
                .and_then(|usage| serde_json::from_str::<serde_json::Value>(usage).ok())
                .and_then(|usage| usage["output_tokens"].as_u64()),
            Some(3)
        );
    }

    #[tokio::test]
    async fn delivery_worker_wakes_on_commit_and_shuts_down_gracefully() {
        let (runtime_event_store, event_store, session_service, store, event_bus, _rx) =
            delivery_fixture().await;
        let (shutdown, receiver) = watch::channel(false);
        let (ready, ready_rx) = oneshot::channel();
        let mut commit_observer = event_store.subscribe_commits();
        let handle = tokio::spawn(run_delivery_worker(
            event_store.clone(),
            session_service,
            event_bus,
            test_backend_reporter("terminal_delivery"),
            receiver,
            ready,
        ));
        ready_rx.await.unwrap().unwrap();
        enqueue_fenced_terminal(
            &runtime_event_store,
            &store,
            "wake-terminal",
            "wake-message",
            "wake-request",
            "wake-turn",
            "wake-ingress",
            r#"assistant_terminal_v2:{"text":"awake","ingress_message_id":"wake-ingress","consumed_input_sequence":0,"token_usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0},"transcript":[{"role":"assistant","blocks":[{"type":"text","text":"awake"}]}]}"#,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(1), commit_observer.changed())
            .await
            .expect("terminal transaction must publish a commit notification")
            .expect("terminal commit signal remains open");
        let delivered = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if store.get_message_count("s1").await.unwrap() >= 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            delivered.is_ok(),
            "commit notification must wake terminal delivery before fallback polling; terminal={:?}",
            event_store.get("wake-terminal").unwrap()
        );
        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("worker must observe graceful shutdown")
            .unwrap()
            .unwrap();
    }

    fn fast_supervisor_config() -> WorkerSupervisorConfig {
        WorkerSupervisorConfig {
            restart_base: Duration::from_millis(2),
            restart_max: Duration::from_millis(8),
            startup_timeout: Duration::from_millis(100),
            shutdown_timeout: Duration::from_millis(25),
        }
    }

    #[test]
    fn backend_reporter_exposes_failure_threshold_and_resets_on_success() {
        let reporter = test_backend_reporter("ingress");
        assert!(!reporter.failure("failure-1"));
        assert!(!reporter.failure("failure-2"));
        assert!(reporter.failure("failure-3"));
        let failed = worker_observation(&reporter.states, "ingress").unwrap();
        assert_eq!(failed.consecutive_backend_failures, 3);
        assert_eq!(failed.last_backend_error.as_deref(), Some("failure-3"));

        reporter.success(Some(42));
        let recovered = worker_observation(&reporter.states, "ingress").unwrap();
        assert_eq!(recovered.consecutive_backend_failures, 0);
        assert_eq!(recovered.oldest_queue_age_ms, Some(42));
        assert!(recovered.last_backend_error.is_none());
        assert!(recovered.last_backend_success_at_ms.is_some());
    }

    #[test]
    fn permanent_reconciliation_failure_restarts_after_three_failed_rounds() {
        let reporter = test_backend_reporter("lifecycle_reconciliation");
        for round in 1..BACKEND_FAILURE_RESTART_THRESHOLD {
            finish_reconciliation_backend_round(
                &reporter,
                Some(100),
                Some("permanent operation failure".to_string()),
            )
            .unwrap_or_else(|error| panic!("round {round} restarted too early: {error}"));
            assert_eq!(
                worker_observation(&reporter.states, "lifecycle_reconciliation")
                    .unwrap()
                    .consecutive_backend_failures,
                round
            );
        }

        let error = finish_reconciliation_backend_round(
            &reporter,
            Some(100),
            Some("permanent operation failure".to_string()),
        )
        .expect_err("the third failed reconciliation round must restart the worker");
        assert_eq!(error, "permanent operation failure");
        assert_eq!(
            worker_observation(&reporter.states, "lifecycle_reconciliation")
                .unwrap()
                .consecutive_backend_failures,
            BACKEND_FAILURE_RESTART_THRESHOLD
        );
    }

    async fn wait_for_worker(
        states: &Mutex<BTreeMap<String, SessionWorkerObservation>>,
        name: &str,
        predicate: impl Fn(&SessionWorkerObservation) -> bool,
    ) -> SessionWorkerObservation {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(observation) = worker_observation(states, name) {
                    if predicate(&observation) {
                        break observation;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("supervised worker did not reach the expected state")
    }

    #[tokio::test]
    async fn supervisor_restarts_panics_and_error_returns_with_bounded_backoff() {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let release_error = Arc::new(Notify::new());
        let release_restart_readiness = Arc::new(Notify::new());
        let factory: WorkerFactory = Arc::new({
            let attempts = Arc::clone(&attempts);
            let release_error = Arc::clone(&release_error);
            let release_restart_readiness = Arc::clone(&release_restart_readiness);
            move |mut shutdown, ready| {
                let attempts = Arc::clone(&attempts);
                let release_error = Arc::clone(&release_error);
                let release_restart_readiness = Arc::clone(&release_restart_readiness);
                Box::pin(async move {
                    let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt == 0 {
                        panic!("deterministic supervised worker panic");
                    }
                    if attempt >= 2 {
                        release_restart_readiness.notified().await;
                    }
                    signal_worker_ready(ready)?;
                    match attempt {
                        1 => {
                            release_error.notified().await;
                            Err("deterministic worker error".to_string())
                        }
                        _ => {
                            let _ = shutdown.changed().await;
                            Ok(())
                        }
                    }
                })
            }
        });
        let (shutdown, receiver) = watch::channel(false);
        let mut supervised = spawn_supervised(
            "deterministic",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            receiver,
            factory,
            fast_supervisor_config(),
        );

        let after_panic = wait_for_worker(&states, "deterministic", |observation| {
            observation.state == SessionWorkerState::Running
                && observation.restart_count == 1
                && attempts.load(std::sync::atomic::Ordering::SeqCst) == 2
        })
        .await;
        assert!(after_panic
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("panicked")));

        release_error.notify_one();
        let restarting = wait_for_worker(&states, "deterministic", |observation| {
            observation.state == SessionWorkerState::Starting
                && observation.restart_count == 2
                && attempts.load(std::sync::atomic::Ordering::SeqCst) == 3
        })
        .await;
        assert_eq!(
            restarting.last_error.as_deref(),
            Some("deterministic worker error")
        );
        release_restart_readiness.notify_one();
        let after_error = wait_for_worker(&states, "deterministic", |observation| {
            observation.state == SessionWorkerState::Running
                && observation.restart_count == 2
                && attempts.load(std::sync::atomic::Ordering::SeqCst) == 3
        })
        .await;
        assert_eq!(
            after_error.last_error.as_deref(),
            Some("deterministic worker error")
        );
        assert_eq!(forced_aborts.load(std::sync::atomic::Ordering::SeqCst), 0);

        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut supervised.handle)
            .await
            .expect("supervisor must join after shutdown")
            .unwrap();
        let stopped = worker_observation(&states, "deterministic").unwrap();
        assert_eq!(stopped.state, SessionWorkerState::Stopped);
        assert_eq!(stopped.restart_count, 2);
    }

    #[tokio::test]
    async fn graceful_shutdown_does_not_restart_worker() {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let factory: WorkerFactory = Arc::new({
            let attempts = Arc::clone(&attempts);
            move |mut shutdown, ready| {
                let attempts = Arc::clone(&attempts);
                Box::pin(async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    signal_worker_ready(ready)?;
                    let _ = shutdown.changed().await;
                    Ok(())
                })
            }
        });
        let (shutdown, receiver) = watch::channel(false);
        let mut supervised = spawn_supervised(
            "graceful",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            receiver,
            factory,
            fast_supervisor_config(),
        );
        wait_for_worker(&states, "graceful", |observation| {
            observation.state == SessionWorkerState::Running
        })
        .await;

        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut supervised.handle)
            .await
            .expect("graceful supervisor must join")
            .unwrap();

        let observation = worker_observation(&states, "graceful").unwrap();
        assert_eq!(observation.state, SessionWorkerState::Stopped);
        assert_eq!(observation.restart_count, 0);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(forced_aborts.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shutdown_aborts_and_joins_worker_that_refuses_to_drain() {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let factory: WorkerFactory = Arc::new(|_, ready| {
            Box::pin(async move {
                signal_worker_ready(ready)?;
                std::future::pending::<()>().await;
                Ok(())
            })
        });
        let (shutdown, receiver) = watch::channel(false);
        let mut supervised = spawn_supervised(
            "hung",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            receiver,
            factory,
            fast_supervisor_config(),
        );
        wait_for_worker(&states, "hung", |observation| {
            observation.state == SessionWorkerState::Running
        })
        .await;

        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut supervised.handle)
            .await
            .expect("hung child must be aborted and supervisor joined")
            .unwrap();

        assert_eq!(
            worker_observation(&states, "hung").unwrap().state,
            SessionWorkerState::Aborted
        );
        assert_eq!(forced_aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn restart_delay_is_exponential_and_bounded() {
        let config = WorkerSupervisorConfig {
            restart_base: Duration::from_millis(10),
            restart_max: Duration::from_millis(25),
            startup_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
        };
        assert_eq!(
            supervisor_restart_delay(1, config),
            Duration::from_millis(10)
        );
        assert_eq!(
            supervisor_restart_delay(2, config),
            Duration::from_millis(20)
        );
        assert_eq!(
            supervisor_restart_delay(3, config),
            Duration::from_millis(25)
        );
        assert_eq!(
            supervisor_restart_delay(64, config),
            Duration::from_millis(25)
        );

        let states = Mutex::new(BTreeMap::new());
        let recorded_at_ms = now_ms();
        let delay = record_worker_restart(&states, "observed", "deterministic failure", config);
        let observation = worker_observation(&states, "observed").unwrap();
        assert_eq!(delay, Duration::from_millis(10));
        assert_eq!(observation.state, SessionWorkerState::Failed);
        assert_eq!(observation.restart_count, 1);
        assert_eq!(
            observation.last_error.as_deref(),
            Some("deterministic failure")
        );
        assert!(observation
            .next_retry_at_ms
            .is_some_and(|retry_at| retry_at >= recorded_at_ms.saturating_add(10)));
    }

    #[tokio::test]
    async fn worker_remains_starting_until_child_signals_readiness() {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let release_readiness = Arc::new(Notify::new());
        let factory: WorkerFactory = Arc::new({
            let release_readiness = Arc::clone(&release_readiness);
            move |mut shutdown, ready| {
                let release_readiness = Arc::clone(&release_readiness);
                Box::pin(async move {
                    release_readiness.notified().await;
                    signal_worker_ready(ready)?;
                    let _ = shutdown.changed().await;
                    Ok(())
                })
            }
        });
        let (shutdown, receiver) = watch::channel(false);
        let mut supervised = spawn_supervised(
            "readiness-gated",
            Arc::clone(&states),
            Arc::clone(&forced_aborts),
            receiver,
            factory,
            fast_supervisor_config(),
        );

        tokio::task::yield_now().await;
        assert_eq!(
            worker_observation(&states, "readiness-gated")
                .unwrap()
                .state,
            SessionWorkerState::Starting
        );
        release_readiness.notify_one();
        let ready = supervised.initial_ready.take().unwrap();
        tokio::time::timeout(Duration::from_secs(1), ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            worker_observation(&states, "readiness-gated")
                .unwrap()
                .state,
            SessionWorkerState::Running
        );

        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut supervised.handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn startup_failure_rolls_back_all_six_started_workers() {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (shutdown, receiver) = watch::channel(false);
        let mut workers = Vec::new();
        for name in REQUIRED_SESSION_WORKERS {
            let should_fail = name == "mission_membership";
            let factory: WorkerFactory = Arc::new(move |mut shutdown, ready| {
                Box::pin(async move {
                    if should_fail {
                        let _ = ready.send(Err("deterministic startup failure".to_string()));
                        return Err("deterministic startup failure".to_string());
                    }
                    signal_worker_ready(ready)?;
                    let _ = shutdown.changed().await;
                    Ok(())
                })
            });
            workers.push(spawn_supervised(
                name,
                Arc::clone(&states),
                Arc::clone(&forced_aborts),
                receiver.clone(),
                factory,
                fast_supervisor_config(),
            ));
        }

        let error = await_initial_worker_readiness(&mut workers, fast_supervisor_config())
            .await
            .expect_err("one failed worker must fail Session supervisor startup");
        assert!(error.contains("mission_membership"));
        rollback_started_workers(
            &shutdown,
            &mut workers,
            &states,
            &forced_aborts,
            fast_supervisor_config(),
        )
        .await;

        assert!(*shutdown.borrow());
        assert!(workers.iter().all(|worker| worker.handle.is_finished()));
        assert!(REQUIRED_SESSION_WORKERS.iter().all(|name| {
            worker_observation(&states, name).is_some_and(|observation| {
                observation.state != SessionWorkerState::Running
                    && observation.state != SessionWorkerState::Starting
            })
        }));
    }

    #[tokio::test]
    async fn startup_timeout_rolls_back_all_six_started_workers() {
        let states = Arc::new(Mutex::new(BTreeMap::new()));
        let forced_aborts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (shutdown, receiver) = watch::channel(false);
        let mut workers = Vec::new();
        let config = WorkerSupervisorConfig {
            startup_timeout: Duration::from_millis(20),
            ..fast_supervisor_config()
        };
        for name in REQUIRED_SESSION_WORKERS {
            let should_timeout = name == "terminal_delivery";
            let factory: WorkerFactory = Arc::new(move |mut shutdown, ready| {
                Box::pin(async move {
                    if should_timeout {
                        let readiness_sender = ready;
                        std::future::pending::<()>().await;
                        drop(readiness_sender);
                        return Ok(());
                    }
                    signal_worker_ready(ready)?;
                    let _ = shutdown.changed().await;
                    Ok(())
                })
            });
            workers.push(spawn_supervised(
                name,
                Arc::clone(&states),
                Arc::clone(&forced_aborts),
                receiver.clone(),
                factory,
                config,
            ));
        }

        let error = await_initial_worker_readiness(&mut workers, config)
            .await
            .expect_err("one readiness timeout must fail Session supervisor startup");
        assert!(error.contains("terminal_delivery"));
        assert!(error.contains("timed out"));
        rollback_started_workers(&shutdown, &mut workers, &states, &forced_aborts, config).await;

        assert!(*shutdown.borrow());
        assert!(workers.iter().all(|worker| worker.handle.is_finished()));
        assert!(REQUIRED_SESSION_WORKERS.iter().all(|name| {
            worker_observation(&states, name).is_some_and(|observation| {
                observation.state != SessionWorkerState::Running
                    && observation.state != SessionWorkerState::Starting
            })
        }));
    }

    #[tokio::test]
    async fn reconciliation_workers_publish_continuous_runtime_progress() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let service = test_session_service(store, SessionProjectionHub::new());
        let progress = Arc::new(Mutex::new(reconciliation_progress_map()));
        let (shutdown, receiver) = watch::channel(false);
        let (lifecycle_ready, lifecycle_ready_rx) = oneshot::channel();
        let lifecycle = tokio::spawn(run_lifecycle_reconciliation_worker(
            Arc::clone(&service),
            Arc::clone(&progress),
            test_backend_reporter("lifecycle_reconciliation"),
            receiver.clone(),
            lifecycle_ready,
        ));
        let (branch_ready, branch_ready_rx) = oneshot::channel();
        let branch = tokio::spawn(run_branch_activation_reconciliation_worker(
            Arc::clone(&service),
            Arc::clone(&progress),
            test_backend_reporter("branch_activation_reconciliation"),
            receiver,
            branch_ready,
        ));
        lifecycle_ready_rx.await.unwrap().unwrap();
        branch_ready_rx.await.unwrap().unwrap();
        service.lifecycle_work_wake().notify_one();
        service.branch_work_wake().notify_one();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                if snapshot
                    .values()
                    .all(|observation| observation.scan_count >= 2)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both reconciliation workers must update progress after an explicit wake");

        shutdown.send(true).unwrap();
        lifecycle.await.unwrap().unwrap();
        branch.await.unwrap().unwrap();
        let snapshot = progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for name in [
            "lifecycle_reconciliation",
            "branch_activation_reconciliation",
        ] {
            let observation = snapshot.get(name).unwrap();
            assert!(observation.scan_count >= 2);
            assert_eq!(observation.pending_count, 0);
            assert_eq!(observation.oldest_pending_age_ms, None);
            assert!(observation.last_scan_at_ms.is_some());
            assert!(observation.last_success_at_ms.is_some());
            assert!(observation.last_error.is_none());
        }
    }

    #[test]
    fn reconciliation_progress_preserves_pending_age_cursor_and_failure() {
        let progress = Mutex::new(reconciliation_progress_map());
        begin_reconciliation_scan(
            &progress,
            "lifecycle_reconciliation",
            WORKER_BATCH + 1,
            true,
            Some(250),
            1_000,
        );
        let failure = Err("deterministic reconcile failure".to_string());
        record_reconciliation_outcome(
            &progress,
            "lifecycle_reconciliation",
            "operation-7",
            &failure,
            1_010,
        );
        finish_reconciliation_scan(&progress, "lifecycle_reconciliation", false, 1_020);

        let snapshot = progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let observation = snapshot.get("lifecycle_reconciliation").unwrap();
        assert_eq!(observation.scan_count, 1);
        assert_eq!(observation.pending_count, (WORKER_BATCH + 1) as u64);
        assert!(observation.pending_count_truncated);
        assert_eq!(observation.oldest_pending_age_ms, Some(750));
        assert_eq!(
            observation.last_operation_id.as_deref(),
            Some("operation-7")
        );
        assert_eq!(
            observation.last_error.as_deref(),
            Some("deterministic reconcile failure")
        );
        assert_eq!(observation.last_error_at_ms, Some(1_010));
        assert_eq!(observation.consecutive_failures, 1);
        drop(snapshot);

        finish_reconciliation_scan(&progress, "lifecycle_reconciliation", true, 1_030);
        let snapshot = progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let observation = snapshot.get("lifecycle_reconciliation").unwrap();
        assert_eq!(observation.consecutive_failures, 0);
        assert_eq!(
            observation.last_error.as_deref(),
            Some("deterministic reconcile failure"),
            "recovery must retain the most recent error as historical evidence"
        );
    }
}
