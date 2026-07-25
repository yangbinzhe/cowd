//! Bounded blocking boundary for the synchronous Session backend.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::MemoryError;
use crate::store::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageExecutionPlaneConfig {
    pub workers: usize,
    pub queue_capacity: usize,
}

impl Default for StorageExecutionPlaneConfig {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map_or(4, usize::from)
            .clamp(2, 16);
        let queue_capacity = workers.saturating_mul(8);
        Self {
            workers,
            queue_capacity,
        }
    }
}

impl StorageExecutionPlaneConfig {
    pub fn validate(self) -> Result<Self> {
        if self.workers == 0 {
            return Err(MemoryError::InvalidArgument(
                "session storage workers must be greater than zero".to_string(),
            ));
        }
        if self.queue_capacity == 0 {
            return Err(MemoryError::InvalidArgument(
                "session storage queue capacity must be greater than zero".to_string(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct StorageExecutionPlaneStats {
    pub accepting: bool,
    pub workers: usize,
    pub queue_capacity: usize,
    pub active: usize,
    pub queued: usize,
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub panicked: u64,
    pub queue_rejected: u64,
    pub average_queue_wait_micros: u64,
    pub average_service_micros: u64,
}

#[derive(Debug, Default)]
struct StorageCounters {
    active: AtomicUsize,
    queued: AtomicUsize,
    submitted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    panicked: AtomicU64,
    queue_rejected: AtomicU64,
    queue_wait_micros: AtomicU64,
    service_micros: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct StorageExecutionPlane {
    config: StorageExecutionPlaneConfig,
    workers: Arc<Semaphore>,
    admission: Arc<Semaphore>,
    accepting: Arc<AtomicBool>,
    counters: Arc<StorageCounters>,
}

impl StorageExecutionPlane {
    pub(crate) fn default_plane() -> Self {
        Self::from_validated(StorageExecutionPlaneConfig::default())
    }

    pub(crate) fn new(config: StorageExecutionPlaneConfig) -> Result<Self> {
        let config = config.validate()?;
        Ok(Self::from_validated(config))
    }

    fn from_validated(config: StorageExecutionPlaneConfig) -> Self {
        Self {
            config,
            workers: Arc::new(Semaphore::new(config.workers)),
            admission: Arc::new(Semaphore::new(
                config.workers.saturating_add(config.queue_capacity),
            )),
            accepting: Arc::new(AtomicBool::new(true)),
            counters: Arc::new(StorageCounters::default()),
        }
    }

    pub(crate) async fn execute<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(MemoryError::StoragePlaneShutdown);
        }
        let admission = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| {
                self.counters.queue_rejected.fetch_add(1, Ordering::Relaxed);
                MemoryError::StorageQueueFull {
                    workers: self.config.workers,
                    queue_capacity: self.config.queue_capacity,
                }
            })?;
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        self.counters.queued.fetch_add(1, Ordering::AcqRel);
        let queued = QueuedGuard::new(Arc::clone(&self.counters), admission);
        let queued_at = Instant::now();
        let worker = Arc::clone(&self.workers)
            .acquire_owned()
            .await
            .map_err(|_| MemoryError::Store("session storage worker pool is closed".to_string()))?;
        self.counters
            .queue_wait_micros
            .fetch_add(elapsed_micros(queued_at), Ordering::Relaxed);
        let admission = queued.into_admission()?;
        self.counters.active.fetch_add(1, Ordering::AcqRel);
        let counters = Arc::clone(&self.counters);

        tokio::task::spawn_blocking(move || {
            execute_started_job(operation, worker, admission, counters)
        })
        .await
        .map_err(|error| MemoryError::StorageWorkerJoin(error.to_string()))?
    }

    pub(crate) fn stats(&self) -> StorageExecutionPlaneStats {
        let completed = self.counters.completed.load(Ordering::Relaxed);
        let failed = self.counters.failed.load(Ordering::Relaxed);
        let finished = completed.saturating_add(failed).max(1);
        let submitted = self.counters.submitted.load(Ordering::Relaxed).max(1);
        StorageExecutionPlaneStats {
            accepting: self.accepting.load(Ordering::Acquire),
            workers: self.config.workers,
            queue_capacity: self.config.queue_capacity,
            active: self.counters.active.load(Ordering::Acquire),
            queued: self.counters.queued.load(Ordering::Acquire),
            submitted: self.counters.submitted.load(Ordering::Relaxed),
            completed,
            failed,
            panicked: self.counters.panicked.load(Ordering::Relaxed),
            queue_rejected: self.counters.queue_rejected.load(Ordering::Relaxed),
            average_queue_wait_micros: self.counters.queue_wait_micros.load(Ordering::Relaxed)
                / submitted,
            average_service_micros: self.counters.service_micros.load(Ordering::Relaxed) / finished,
        }
    }

    #[cfg(test)]
    pub(crate) fn shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        self.admission.close();
        self.workers.close();
    }
}

fn execute_started_job<T, F>(
    operation: F,
    _worker: OwnedSemaphorePermit,
    _admission: OwnedSemaphorePermit,
    counters: Arc<StorageCounters>,
) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let started = Instant::now();
    let result = std::panic::catch_unwind(AssertUnwindSafe(operation));
    counters
        .service_micros
        .fetch_add(elapsed_micros(started), Ordering::Relaxed);
    counters.active.fetch_sub(1, Ordering::AcqRel);
    match result {
        Ok(Ok(value)) => {
            counters.completed.fetch_add(1, Ordering::Relaxed);
            Ok(value)
        }
        Ok(Err(error)) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            Err(error)
        }
        Err(_) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            counters.panicked.fetch_add(1, Ordering::Relaxed);
            Err(MemoryError::StorageWorkerPanic)
        }
    }
}

#[derive(Debug)]
struct QueuedGuard {
    counters: Arc<StorageCounters>,
    admission: Option<OwnedSemaphorePermit>,
}

impl QueuedGuard {
    fn new(counters: Arc<StorageCounters>, admission: OwnedSemaphorePermit) -> Self {
        Self {
            counters,
            admission: Some(admission),
        }
    }

    fn into_admission(mut self) -> Result<OwnedSemaphorePermit> {
        self.counters.queued.fetch_sub(1, Ordering::AcqRel);
        self.admission
            .take()
            .ok_or_else(|| MemoryError::Store("queued admission permit is missing".to_string()))
    }
}

impl Drop for QueuedGuard {
    fn drop(&mut self) {
        if self.admission.is_some() {
            self.counters.queued.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn plane(workers: usize, queue_capacity: usize) -> StorageExecutionPlane {
        StorageExecutionPlane::new(StorageExecutionPlaneConfig {
            workers,
            queue_capacity,
        })
        .expect("plane")
    }

    #[tokio::test]
    async fn blocking_job_does_not_block_tokio_heartbeat() {
        let plane = Arc::new(plane(1, 2));
        let task = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move {
                plane
                    .execute(|| {
                        std::thread::sleep(Duration::from_millis(40));
                        Ok(())
                    })
                    .await
            })
        };
        tokio::time::timeout(
            Duration::from_millis(15),
            tokio::time::sleep(Duration::from_millis(2)),
        )
        .await
        .expect("Tokio timer must progress");
        task.await.expect("task").expect("job");
    }

    #[tokio::test]
    async fn queue_is_bounded_and_reports_overload() {
        let plane = Arc::new(plane(1, 1));
        let first = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move {
                plane
                    .execute(|| {
                        std::thread::sleep(Duration::from_millis(50));
                        Ok(())
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        let second = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move {
                plane
                    .execute(|| {
                        std::thread::sleep(Duration::from_millis(10));
                        Ok(())
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        let error = plane.execute(|| Ok(())).await.expect_err("queue full");
        assert!(error.to_string().contains("queue is full"));
        first.await.expect("first task").expect("first job");
        second.await.expect("second task").expect("second job");
        assert_eq!(plane.stats().queue_rejected, 1);
    }

    #[tokio::test]
    async fn cancelled_waiter_leaves_queue_while_started_job_finishes() {
        let plane = Arc::new(plane(1, 2));
        let first = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move {
                plane
                    .execute(|| {
                        std::thread::sleep(Duration::from_millis(40));
                        Ok(7)
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        let waiting = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move { plane.execute(|| Ok(9)).await })
        };
        tokio::task::yield_now().await;
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(first.await.expect("first").expect("started job"), 7);
        assert_eq!(plane.stats().queued, 0);
        assert_eq!(plane.execute(|| Ok(11)).await.expect("next job"), 11);
    }

    #[tokio::test]
    async fn dropping_started_waiter_does_not_cancel_the_transaction() {
        let plane = Arc::new(plane(1, 1));
        let completed = Arc::new(AtomicBool::new(false));
        let waiter = {
            let plane = Arc::clone(&plane);
            let completed = Arc::clone(&completed);
            tokio::spawn(async move {
                plane
                    .execute(move || {
                        std::thread::sleep(Duration::from_millis(30));
                        completed.store(true, Ordering::Release);
                        Ok(())
                    })
                    .await
            })
        };
        while plane.stats().active == 0 {
            tokio::task::yield_now().await;
        }
        waiter.abort();
        let _ = waiter.await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(completed.load(Ordering::Acquire));
        assert_eq!(plane.stats().completed, 1);
        assert_eq!(plane.stats().active, 0);
    }

    #[tokio::test]
    async fn panic_is_typed_and_capacity_is_released() {
        let plane = plane(1, 1);
        let error = plane
            .execute(|| -> Result<()> { panic!("boom") })
            .await
            .expect_err("panic");
        assert!(matches!(error, MemoryError::StorageWorkerPanic));
        assert_eq!(plane.execute(|| Ok(3)).await.expect("next job"), 3);
        assert_eq!(plane.stats().panicked, 1);
    }

    #[tokio::test]
    async fn backend_error_is_preserved_without_retry_reclassification() {
        let plane = plane(1, 1);
        let error = plane
            .execute(|| Err::<(), _>(MemoryError::Store("commit outcome unknown".to_string())))
            .await
            .expect_err("backend error");
        assert!(matches!(
            error,
            MemoryError::Store(message) if message == "commit outcome unknown"
        ));
        assert_eq!(plane.stats().failed, 1);
    }

    #[tokio::test]
    async fn shutdown_rejects_new_work() {
        let plane = plane(1, 1);
        plane.shutdown();
        let error = plane.execute(|| Ok(())).await.expect_err("shutdown");
        assert!(matches!(error, MemoryError::StoragePlaneShutdown));
    }
}
