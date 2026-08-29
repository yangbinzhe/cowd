use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::FutureExt;
use harness_contract::turn::{InputRoutingDecision, SessionInputId, SessionInputStatus};
use runtime::{SessionTurnAdmissionLease, SessionTurnAdmissionPort, SessionTurnOutcome};
#[cfg(test)]
use session::UnifiedSessionStore;
use session::{OutboxFailureClass, SessionRuntimeInputStatus, SessionRuntimeOutboxRecord};
use tokio::{
    sync::{oneshot, watch, Notify},
    task::{JoinHandle, JoinSet},
};

use crate::{
    event_bus::{SessionProjectionEvent, SessionProjectionHub},
    runtime_service::{RuntimeService, SESSION_RUNTIME_BUSY_ERROR},
    services::SessionService,
};

#[path = "terminal_codec.rs"]
mod terminal_codec;
use terminal_codec::annotate_terminal_tool_instances;
#[cfg(test)]
use terminal_codec::decode_terminal_payload;
pub(crate) use terminal_codec::{
    load_terminal_payload, DecodedTerminalPayload, DecodedTerminalTranscriptMessage,
};

#[path = "session_worker_supervisor.rs"]
mod session_worker_supervisor;
pub(crate) use session_worker_supervisor::SessionWorkerSupervisor;

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
    admission: SessionTurnAdmissionPort,
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
        let lease = self.admission.acquire().await?;
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
        task_route_hint: record.task_route_hint.clone(),
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
            runtime::RuntimeSessionInputStatus::Attached => {
                session::SessionRuntimeInputStatus::Attached
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
        application_receipt: record.application_receipt.clone(),
    })
}

impl GatewaySessionIngressExecutor {
    async fn execute_ingress_with_lease(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        content: &str,
        mut lease: SessionTurnAdmissionLease,
    ) -> Result<runtime::SessionIngressExecutionReceipt, String> {
        lease.begin_service();
        self.session
            .activate_worker_session(&record.session_id)
            .await?;
        self.restore_attached_inputs(record).await?;
        let outcome = self.runtime.execute_ingress_record(record, content).await;
        let result_class = if outcome.is_ok() {
            SessionTurnOutcome::Completed
        } else {
            SessionTurnOutcome::Failed
        };
        if let Err(error) = lease.finish(result_class) {
            tracing::warn!(%error, "failed to record SessionTurn resource observation");
        }
        outcome
    }

    async fn restore_attached_inputs(
        &self,
        primary: &session::SessionRuntimeOutboxRecord,
    ) -> Result<(), String> {
        let attached = self
            .session
            .runtime_inputs_for_turn_relation(
                &primary.session_id,
                primary.session_generation,
                &primary.turn_id,
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|record| {
                record.status == session::SessionRuntimeInputStatus::Attached
                    && record.target_turn_id.as_deref() == Some(primary.turn_id.as_str())
            })
            .collect::<Vec<_>>();
        for record in attached {
            if self.runtime.session_input_checkpoint_consumed(
                &record.session_id,
                &record.input_id,
                record.target_turn_id.as_deref(),
            ) {
                continue;
            }
            let content = self
                .session
                .load_ingress_content(&record)
                .await
                .map_err(|error| error.to_string())?;
            self.runtime
                .deliver_durable_session_input_view(
                    &record,
                    content,
                    SessionInputStatus::AttachedToTurn,
                )
                .await
                .map_err(|error| error.message())?;
        }
        Ok(())
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

pub(crate) const REQUIRED_SESSION_WORKERS: [&str; 5] = [
    "ingress",
    "terminal_delivery",
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
    runtime_service: Option<Arc<RuntimeService>>,
    runtime_services: Option<Arc<runtime::RuntimeServices>>,
    event_bus: Option<Arc<SessionProjectionHub>>,
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
        if let Some(services) = runtime_services.as_ref() {
            if let Some(runtime_service) = runtime_service.as_ref() {
                for receipt in services
                    .pending_cancellation_receipts(WORKER_BATCH)
                    .map_err(|error| error.to_string())?
                {
                    let _ = runtime_service.cancel_active_execution(
                        &receipt.session_id,
                        &receipt.turn_id,
                        &receipt.execution_id,
                        receipt.reason.as_deref().unwrap_or("user_requested"),
                    );
                }
            }
            let receipts = services
                .reconcile_requested_cancellations(WORKER_BATCH)
                .map_err(|error| error.to_string())?;
            if let Some(event_bus) = event_bus.as_ref() {
                for receipt in receipts {
                    let session_id = receipt.session_id.clone();
                    event_bus
                        .publish(
                            &session_id,
                            SessionProjectionEvent::TerminalDelivery {
                                session_id: Some(receipt.session_id.clone()),
                                execution_id: (!receipt.execution_id.is_empty())
                                    .then(|| receipt.execution_id.clone()),
                                turn_id: (!receipt.turn_id.is_empty())
                                    .then(|| receipt.turn_id.clone()),
                                delivery: harness_contract::live::TerminalDeliveryEvent::CancellationCommitted {
                                    receipt,
                                },
                            },
                        )
                        .await;
                }
            }
        }
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
                lease = executor.admission.acquire() => {
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
    lease: SessionTurnAdmissionLease,
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
                let acknowledged_status =
                    if record.decision == InputRoutingDecision::SupplementCurrentTurn {
                        SessionRuntimeInputStatus::Attached
                    } else {
                        // Control and approval inputs apply an immediate side
                        // effect and do not wait for the target answer commit.
                        SessionRuntimeInputStatus::Supplemented
                    };
                if let Err(error) = session_service
                    .complete_ingress_work(
                        &record,
                        &worker_id,
                        &claim_token,
                        running.revision,
                        acknowledged_status,
                        0,
                        now_ms(),
                    )
                    .await
                {
                    if acknowledged_status == SessionRuntimeInputStatus::Attached {
                        resolve_attachment_fence(
                            &session_service,
                            &worker_id,
                            &running,
                            &claim_token,
                            &error.to_string(),
                        )
                        .await;
                    } else {
                        tracing::warn!(
                            request_id = %record.request_id,
                            %error,
                            "control input acknowledgement was fenced"
                        );
                    }
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
            SessionRuntimeInputStatus::Attached,
            0,
            now_ms(),
        )
        .await
    {
        resolve_attachment_fence(
            session_service,
            worker_id,
            &running,
            claim_token,
            &error.to_string(),
        )
        .await;
    }
}

async fn resolve_attachment_fence(
    session_service: &SessionService,
    worker_id: &str,
    running: &SessionRuntimeOutboxRecord,
    claim_token: &str,
    attachment_error: &str,
) {
    let relation = match session_service
        .runtime_inputs_for_turn_relation(
            &running.session_id,
            running.session_generation,
            running.target_turn_id.as_deref().unwrap_or_default(),
        )
        .await
    {
        Ok(relation) => relation,
        Err(error) => {
            tracing::error!(
                request_id = %running.request_id,
                %error,
                attachment_error,
                "could not resolve a fenced Session input attachment"
            );
            return;
        }
    };
    let Some(current) = relation
        .iter()
        .find(|candidate| candidate.input_id == running.input_id)
    else {
        tracing::error!(
            request_id = %running.request_id,
            attachment_error,
            "fenced Session input disappeared while resolving attachment"
        );
        return;
    };
    if current.status == SessionRuntimeInputStatus::Supplemented {
        return;
    }
    let target_terminal = relation.iter().any(|candidate| {
        candidate.turn_id == running.target_turn_id.as_deref().unwrap_or_default()
            && candidate.status.is_terminal()
    });
    if current.status == SessionRuntimeInputStatus::Running && target_terminal {
        match session_service
            .requeue_ingress_work(
                current,
                worker_id,
                claim_token,
                current.revision,
                InputRoutingDecision::StartNewTurn,
                None,
                "target turn became terminal before attached acknowledgement; promote input to a new turn",
                now_ms(),
            )
            .await
        {
            Ok(_) => tracing::info!(
                request_id = %current.request_id,
                "promoted a fenced attached input to a new turn"
            ),
            Err(error) => tracing::error!(
                request_id = %current.request_id,
                %error,
                attachment_error,
                "failed to promote a fenced attached input"
            ),
        }
        return;
    }
    tracing::warn!(
        request_id = %current.request_id,
        status = ?current.status,
        target_terminal,
        attachment_error,
        "Session input attachment was fenced without a terminal resolution"
    );
}

async fn execute_primary_ingress_with_lease(
    session_service: &SessionService,
    executor: &GatewaySessionIngressExecutor,
    worker_id: &str,
    record: &SessionRuntimeOutboxRecord,
    claim_token: &str,
    mut revision: u64,
    content: &str,
    lease: SessionTurnAdmissionLease,
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
                    match executed.status {
                        runtime::SessionIngressExecutionStatus::Completed => {
                            SessionRuntimeInputStatus::Completed
                        }
                        runtime::SessionIngressExecutionStatus::Cancelled => {
                            SessionRuntimeInputStatus::Cancelled
                        }
                    },
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
    match session_service
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
        Ok(failed) if failed.status == SessionRuntimeInputStatus::Failed => {
            roll_forward_unapplied_inputs(session_service, record, error).await;
        }
        Ok(_) => {}
        Err(persist_error) => {
            tracing::error!(
                request_id = %record.request_id,
                error = %persist_error,
                work_error = error,
                "Session input failure state could not be persisted"
            );
        }
    }
}

async fn roll_forward_unapplied_inputs(
    session_service: &SessionService,
    failed: &SessionRuntimeOutboxRecord,
    failure: &str,
) {
    let records = match session_service
        .runtime_inputs_for_turn_relation(
            &failed.session_id,
            failed.session_generation,
            &failed.turn_id,
        )
        .await
    {
        Ok(records) => records,
        Err(error) => {
            tracing::error!(
                session_id = %failed.session_id,
                turn_id = %failed.turn_id,
                %error,
                "failed to inspect attached inputs after terminal turn failure"
            );
            return;
        }
    };
    for attached in records.into_iter().filter(|candidate| {
        matches!(
            candidate.status,
            SessionRuntimeInputStatus::Accepted
                | SessionRuntimeInputStatus::Classified
                | SessionRuntimeInputStatus::Queued
                | SessionRuntimeInputStatus::Reclassified
                | SessionRuntimeInputStatus::Attached
        ) && candidate.decision == InputRoutingDecision::SupplementCurrentTurn
            && candidate.target_turn_id.as_deref() == Some(failed.turn_id.as_str())
    }) {
        let reason = format!(
            "target turn {} failed before applying the attached input: {}",
            failed.turn_id, failure
        );
        match session_service
            .reclassify_input(
                &failed.session_id,
                SessionInputId::from_string(attached.input_id.clone()),
                InputRoutingDecision::StartNewTurn,
                &reason,
            )
            .await
        {
            Ok(_) => tracing::info!(
                session_id = %failed.session_id,
                failed_turn_id = %failed.turn_id,
                input_id = %attached.input_id,
                "rolled an unapplied attached input forward as a new turn"
            ),
            Err(error) => tracing::error!(
                session_id = %failed.session_id,
                failed_turn_id = %failed.turn_id,
                input_id = %attached.input_id,
                %error,
                "failed to roll an unapplied attached input forward"
            ),
        }
    }
}

fn classify_ingress_failure(error: &str) -> OutboxFailureClass {
    if error.contains("authorization") || error.contains("approval") {
        OutboxFailureClass::AuthorizationBlocked
    } else if error.contains("payload") || error.contains("JSON") {
        OutboxFailureClass::CorruptPayload
    } else if error.contains("invalid") || error.contains("unavailable until") {
        OutboxFailureClass::Permanent
    } else if error.contains("no terminal turn result")
        || error.contains("terminal without its durable session receipt")
    {
        // The execution graph is already terminal/cancelled; replaying the
        // same request cannot produce a new result. Retrying only floods the
        // ingress worker every retry interval, so the input must become
        // Failed immediately instead of consuming the retry budget.
        OutboxFailureClass::Permanent
    } else {
        OutboxFailureClass::Retryable
    }
}

async fn run_delivery_worker(
    event_store: runtime::SessionTerminalDeliveryPort,
    artifacts: Arc<runtime::ArtifactStore>,
    session_service: Arc<SessionService>,
    event_bus: Arc<SessionProjectionHub>,
    runtime_services: Option<Arc<runtime::RuntimeServices>>,
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
                        &artifacts,
                        &session_service,
                        &event_bus,
                        runtime_services.as_deref(),
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
    artifacts: &runtime::ArtifactStore,
    session_service: &SessionService,
    event_bus: &SessionProjectionHub,
    runtime_services: Option<&runtime::RuntimeServices>,
    worker_id: &str,
    record: runtime::RuntimeSessionOutboxRecord,
) -> Result<bool, String> {
    let outcome = match load_terminal_payload(artifacts, &record).await {
        Ok(payload) => {
            if let (Some(services), Some(execution_id)) =
                (runtime_services, record.execution_id.as_deref())
            {
                let terminal_status = match payload.goal_completion {
                    harness_contract::goal::GoalCompletion::Satisfied => {
                        harness_contract::projection::ExecutionLiveStatus::Complete
                    }
                    harness_contract::goal::GoalCompletion::Cancelled => {
                        harness_contract::projection::ExecutionLiveStatus::Cancelled
                    }
                    harness_contract::goal::GoalCompletion::Partial
                    | harness_contract::goal::GoalCompletion::Open
                    | harness_contract::goal::GoalCompletion::WaitingExternalDecision => {
                        harness_contract::projection::ExecutionLiveStatus::Error
                    }
                };
                match services.claim_live_terminal_fence(
                    execution_id,
                    record.terminal_id.clone(),
                    terminal_status,
                ) {
                    Ok(
                        runtime::execution_live::TerminalFenceClaim::Claimed
                        | runtime::execution_live::TerminalFenceClaim::SameWinner,
                    ) => {}
                    Ok(runtime::execution_live::TerminalFenceClaim::ConflictingWinner) => {
                        let event_store = event_store.clone();
                        let terminal_id = record.terminal_id.clone();
                        let worker = worker_id.to_string();
                        let revision = record.revision;
                        tokio::task::spawn_blocking(move || {
                            event_store.suppress(
                                &terminal_id,
                                &worker,
                                revision,
                                "durable execution terminal fence was won by cancellation or another terminal",
                                now_ms(),
                            )
                        })
                        .await
                        .map_err(|error| error.to_string())?
                        .map_err(|error| error.to_string())?;
                        return Ok(false);
                    }
                    Ok(runtime::execution_live::TerminalFenceClaim::MissingExecution) => {
                        return Err(format!(
                            "terminal fence execution `{execution_id}` is not yet recoverable"
                        ));
                    }
                    Err(error) => {
                        return Err(format!(
                            "terminal fence persistence failed for `{execution_id}`: {error}"
                        ));
                    }
                }
            }
            let terminal_presentation = payload.terminal_presentation.clone();
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
                        (
                            payload.text,
                            payload.token_usage_json,
                            terminal_presentation,
                            terminal,
                            inserted,
                        )
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
        Ok((text, token_usage_json, terminal_presentation, message, inserted)) => {
            // The message write is exactly-once; delivery notification is
            // intentionally at-least-once. A process can die after commit but
            // before broadcast, so suppressing a duplicate notification would
            // leave live Surfaces permanently waiting. Stable terminal/message
            // identities make replay harmless and let each Surface dedupe.
            let token_usage = token_usage_json
                .as_deref()
                .and_then(|usage| serde_json::from_str(usage).ok());
            if let Some(presentation) = terminal_presentation {
                event_bus
                    .publish(
                        &record.session_id,
                        SessionProjectionEvent::TerminalDelivery {
                            delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationCommitted {
                                presentation_id: presentation.presentation_id,
                                attempt_id: presentation.attempt_id,
                                answer_origin: presentation.answer_origin,
                                terminal_id: record.terminal_id.clone(),
                            },
                            session_id: Some(record.session_id.clone()),
                            execution_id: record.execution_id.clone(),
                            turn_id: record.turn_id.clone(),
                        },
                    )
                    .await;
            }
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
                // The durable message ID makes replay safe. The store treats a
                // retried ack after materialization as idempotent success
                // (P4); any error reaching here is a genuine lease gap.
                tracing::warn!(
                    terminal_id = %record.terminal_id,
                    %error,
                    "terminal committed but ack lease conflict; delivery is at-least-once"
                );
            } else if let (Some(services), Some(execution_id)) =
                (runtime_services, record.execution_id.as_deref())
            {
                services.release_live_terminal_fence(execution_id);
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
#[path = "tests/session_activation.rs"]
mod tests;
