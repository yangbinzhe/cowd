use std::{path::PathBuf, time::Duration};

use cowd_app_protocol::{AppId, AppLifecycleStateV1, GenerationId};
use managed_worker_runtime::LogSnapshot;

#[derive(Debug, Clone)]
pub struct AppRuntimeSupervisorConfig {
    pub runtime_root: PathBuf,
    pub max_starting_workers: usize,
    pub max_active_workers: usize,
    pub max_waiters_per_app: usize,
    pub activation_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub idle_ttl: Option<Duration>,
    pub idle_scan_interval: Duration,
    pub crash_window: Duration,
    pub crash_budget: usize,
    pub restart_backoff_initial: Duration,
    pub restart_backoff_maximum: Duration,
}

impl Default for AppRuntimeSupervisorConfig {
    fn default() -> Self {
        Self {
            runtime_root: std::env::temp_dir().join("cowd-workers"),
            max_starting_workers: 8,
            max_active_workers: 64,
            max_waiters_per_app: 512,
            activation_timeout: Duration::from_secs(15),
            shutdown_timeout: Duration::from_secs(10),
            idle_ttl: None,
            idle_scan_interval: Duration::from_secs(1),
            crash_window: Duration::from_secs(60),
            crash_budget: 5,
            restart_backoff_initial: Duration::from_millis(250),
            restart_backoff_maximum: Duration::from_secs(30),
        }
    }
}

impl AppRuntimeSupervisorConfig {
    pub(crate) fn validate(&self) -> Result<(), SupervisorError> {
        if self.max_starting_workers == 0
            || self.max_active_workers == 0
            || self.max_starting_workers > self.max_active_workers
            || self.max_waiters_per_app == 0
            || self.crash_budget == 0
        {
            return Err(SupervisorError::InvalidConfiguration(
                "worker and waiter limits must be positive and starting cannot exceed active"
                    .to_owned(),
            ));
        }
        if self.activation_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
            || self.idle_scan_interval.is_zero()
            || self.crash_window.is_zero()
            || self.restart_backoff_initial.is_zero()
            || self.restart_backoff_maximum < self.restart_backoff_initial
        {
            return Err(SupervisorError::InvalidConfiguration(
                "timeouts must be positive and restart backoff must be ordered".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppRuntimeStatus {
    pub app_id: AppId,
    pub generation: GenerationId,
    pub state: AppLifecycleStateV1,
    pub reason: Option<String>,
    pub active_leases: usize,
    pub waiters: usize,
    pub pid: Option<u32>,
    pub restart_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppRuntimeLogs {
    pub stdout: LogSnapshot,
    pub stderr: LogSnapshot,
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("invalid supervisor configuration: {0}")]
    InvalidConfiguration(String),
    #[error("unknown application `{0}`")]
    UnknownApp(AppId),
    #[error("stale generation for `{app_id}`: expected {}, observed {}", expected.0, observed.0)]
    StaleGeneration {
        app_id: AppId,
        expected: GenerationId,
        observed: GenerationId,
    },
    #[error("application `{0}` activation waiter limit is exhausted")]
    WaiterOverloaded(AppId),
    #[error("application `{0}` is circuit-open")]
    CircuitOpen(AppId),
    #[error("application `{app_id}` is backing off for {retry_after:?}")]
    BackingOff {
        app_id: AppId,
        retry_after: Duration,
    },
    #[error("application `{app_id}` worker failed: {detail}")]
    Worker { app_id: AppId, detail: String },
    #[error("required resident applications failed: {0:?}")]
    RequiredResidentsFailed(Vec<AppId>),
    #[error("supervisor is shutting down")]
    ShuttingDown,
    #[error("operation exceeded its {0:?} deadline")]
    DeadlineExceeded(Duration),
    #[error("operation was cancelled")]
    Cancelled,
}
