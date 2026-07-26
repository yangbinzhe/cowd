//! Bounded, lane-aware blocking boundary for the synchronous Session backend.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::oneshot;

use crate::error::{Result, SessionError};

const INTERACTIVE_BURST_LIMIT: usize = 8;
const JOB_QUEUED: u8 = 0;
const JOB_GRANTED: u8 = 1;
const JOB_STARTED: u8 = 2;
const JOB_RELEASED: u8 = 3;

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
            return Err(SessionError::InvalidArgument(
                "session storage workers must be greater than zero".to_string(),
            ));
        }
        if self.queue_capacity == 0 {
            return Err(SessionError::InvalidArgument(
                "session storage queue capacity must be greater than zero".to_string(),
            ));
        }
        Ok(self)
    }
}

/// The three workload classes admitted by the Session repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageExecutionLane {
    InteractiveRead,
    InteractiveWrite,
    Background,
}

impl StorageExecutionLane {
    const ALL: [Self; 3] = [
        Self::InteractiveRead,
        Self::InteractiveWrite,
        Self::Background,
    ];

    const fn index(self) -> usize {
        match self {
            Self::InteractiveRead => 0,
            Self::InteractiveWrite => 1,
            Self::Background => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct StorageExecutionLaneStats {
    pub active: usize,
    pub queued: usize,
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub panicked: u64,
    pub queue_rejected: u64,
    pub total_queue_wait_micros: u64,
    pub total_service_micros: u64,
    pub average_queue_wait_micros: u64,
    pub average_service_micros: u64,
    /// Age of the oldest currently queued operation in this lane.
    pub oldest_queue_age_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct StorageExecutionPlaneStats {
    pub accepting: bool,
    /// True only after admission has stopped and no active or queued work remains.
    pub drained: bool,
    pub workers: usize,
    pub queue_capacity: usize,
    pub active: usize,
    pub queued: usize,
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub panicked: u64,
    pub queue_rejected: u64,
    pub total_queue_wait_micros: u64,
    pub total_service_micros: u64,
    pub average_queue_wait_micros: u64,
    pub average_service_micros: u64,
    pub oldest_queue_age_micros: u64,
    pub interactive_read: StorageExecutionLaneStats,
    pub interactive_write: StorageExecutionLaneStats,
    pub background: StorageExecutionLaneStats,
}

impl StorageExecutionPlaneStats {
    #[must_use]
    pub const fn lane(self, lane: StorageExecutionLane) -> StorageExecutionLaneStats {
        match lane {
            StorageExecutionLane::InteractiveRead => self.interactive_read,
            StorageExecutionLane::InteractiveWrite => self.interactive_write,
            StorageExecutionLane::Background => self.background,
        }
    }
}

#[derive(Debug, Default)]
struct LaneCounters {
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

impl LaneCounters {
    fn snapshot(&self, oldest_queue_age_micros: u64) -> StorageExecutionLaneStats {
        let submitted = self.submitted.load(Ordering::Relaxed);
        let completed = self.completed.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let finished = completed.saturating_add(failed);
        StorageExecutionLaneStats {
            active: self.active.load(Ordering::Acquire),
            queued: self.queued.load(Ordering::Acquire),
            submitted,
            completed,
            failed,
            panicked: self.panicked.load(Ordering::Relaxed),
            queue_rejected: self.queue_rejected.load(Ordering::Relaxed),
            total_queue_wait_micros: self.queue_wait_micros.load(Ordering::Relaxed),
            total_service_micros: self.service_micros.load(Ordering::Relaxed),
            average_queue_wait_micros: self.counters_average(&self.queue_wait_micros, submitted),
            average_service_micros: self.counters_average(&self.service_micros, finished),
            oldest_queue_age_micros,
        }
    }

    fn counters_average(&self, counter: &AtomicU64, denominator: u64) -> u64 {
        if denominator == 0 {
            0
        } else {
            counter.load(Ordering::Relaxed) / denominator
        }
    }
}

#[derive(Debug)]
struct PlaneCounters {
    lanes: [LaneCounters; 3],
}

impl Default for PlaneCounters {
    fn default() -> Self {
        Self {
            lanes: std::array::from_fn(|_| LaneCounters::default()),
        }
    }
}

impl PlaneCounters {
    fn lane(&self, lane: StorageExecutionLane) -> &LaneCounters {
        &self.lanes[lane.index()]
    }
}

#[derive(Debug, Clone, Copy)]
struct BackendConcurrency {
    single_writer: bool,
}

impl BackendConcurrency {
    const SQLITE: Self = Self {
        single_writer: true,
    };
    const CONCURRENT: Self = Self {
        single_writer: false,
    };
}

#[derive(Debug)]
struct QueuedJob {
    id: u64,
    lane: StorageExecutionLane,
    write: bool,
    queued_at: Instant,
    state: Arc<AtomicU8>,
    grant: oneshot::Sender<()>,
}

#[derive(Debug)]
struct SchedulerState {
    active: usize,
    active_writes: usize,
    active_background: usize,
    interactive_burst: usize,
    accepting: bool,
    queues: [VecDeque<QueuedJob>; 3],
}

impl Default for SchedulerState {
    fn default() -> Self {
        Self {
            active: 0,
            active_writes: 0,
            active_background: 0,
            interactive_burst: 0,
            accepting: true,
            queues: std::array::from_fn(|_| VecDeque::new()),
        }
    }
}

#[derive(Debug)]
struct Scheduler {
    workers: usize,
    queue_capacity: usize,
    max_background: usize,
    backend: BackendConcurrency,
    counters: Arc<PlaneCounters>,
    state: Mutex<SchedulerState>,
}

impl Scheduler {
    fn new(
        config: StorageExecutionPlaneConfig,
        backend: BackendConcurrency,
        counters: Arc<PlaneCounters>,
    ) -> Self {
        Self {
            workers: config.workers,
            queue_capacity: config.queue_capacity,
            max_background: config.workers.saturating_sub(1).max(1),
            backend,
            counters,
            state: Mutex::new(SchedulerState::default()),
        }
    }

    fn enqueue(&self, job: QueuedJob) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| {
            SessionError::Store("session storage scheduler is poisoned".to_string())
        })?;
        if !state.accepting {
            return Err(SessionError::StoragePlaneShutdown);
        }
        let lane = job.lane;
        let id = job.id;
        self.counters
            .lane(lane)
            .submitted
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .lane(lane)
            .queued
            .fetch_add(1, Ordering::AcqRel);
        state.queues[lane.index()].push_back(job);
        self.schedule_locked(&mut state);
        let queued = state.queues.iter().map(VecDeque::len).sum::<usize>();
        if queued > self.queue_capacity {
            if let Some(position) = state.queues[lane.index()]
                .iter()
                .position(|job| job.id == id)
            {
                if let Some(rejected) = state.queues[lane.index()].remove(position) {
                    rejected.state.store(JOB_RELEASED, Ordering::Release);
                    self.counters
                        .lane(lane)
                        .submitted
                        .fetch_sub(1, Ordering::Relaxed);
                    self.counters
                        .lane(lane)
                        .queued
                        .fetch_sub(1, Ordering::AcqRel);
                    self.counters
                        .lane(lane)
                        .queue_rejected
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(SessionError::StorageQueueFull {
                        workers: self.workers,
                        queue_capacity: self.queue_capacity,
                    });
                }
            }
        }
        Ok(())
    }

    fn cancel(&self, id: u64, lane: StorageExecutionLane, write: bool, status: &AtomicU8) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        match status.load(Ordering::Acquire) {
            JOB_QUEUED => {
                let queue = &mut state.queues[lane.index()];
                if let Some(position) = queue.iter().position(|job| job.id == id) {
                    queue.remove(position);
                    status.store(JOB_RELEASED, Ordering::Release);
                    self.counters
                        .lane(lane)
                        .queued
                        .fetch_sub(1, Ordering::AcqRel);
                }
            }
            JOB_GRANTED => {
                if status
                    .compare_exchange(
                        JOB_GRANTED,
                        JOB_RELEASED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.release_active_locked(&mut state, lane, write);
                    self.schedule_locked(&mut state);
                }
            }
            JOB_STARTED | JOB_RELEASED => {}
            _ => {}
        }
    }

    fn finish(&self, lane: StorageExecutionLane, write: bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        self.release_active_locked(&mut state, lane, write);
        self.schedule_locked(&mut state);
    }

    fn stop_accepting(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.accepting = false;
    }

    fn is_drained(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.active == 0 && state.queues.iter().all(VecDeque::is_empty))
    }

    fn queue_observation(&self) -> ([u64; 3], bool) {
        let Ok(state) = self.state.lock() else {
            return ([u64::MAX; 3], false);
        };
        let now = Instant::now();
        let oldest = std::array::from_fn(|index| {
            state.queues[index]
                .iter()
                .map(|job| elapsed_micros_at(job.queued_at, now))
                .max()
                .unwrap_or_default()
        });
        let drained =
            !state.accepting && state.active == 0 && state.queues.iter().all(VecDeque::is_empty);
        (oldest, drained)
    }

    fn schedule_locked(&self, state: &mut SchedulerState) {
        while state.active < self.workers {
            let Some((lane, position)) = self.next_runnable(state) else {
                break;
            };
            let Some(job) = state.queues[lane.index()].remove(position) else {
                break;
            };
            self.counters
                .lane(lane)
                .queued
                .fetch_sub(1, Ordering::AcqRel);
            if job
                .state
                .compare_exchange(JOB_QUEUED, JOB_GRANTED, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            state.active = state.active.saturating_add(1);
            if job.write {
                state.active_writes = state.active_writes.saturating_add(1);
            }
            if lane == StorageExecutionLane::Background {
                state.active_background = state.active_background.saturating_add(1);
                state.interactive_burst = 0;
            } else {
                state.interactive_burst = state.interactive_burst.saturating_add(1);
            }
            self.counters
                .lane(lane)
                .active
                .fetch_add(1, Ordering::AcqRel);

            if job.grant.send(()).is_err()
                && job
                    .state
                    .compare_exchange(
                        JOB_GRANTED,
                        JOB_RELEASED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                self.release_active_locked(state, lane, job.write);
            }
        }
    }

    fn next_runnable(&self, state: &SchedulerState) -> Option<(StorageExecutionLane, usize)> {
        let read = self.front_if_runnable(state, StorageExecutionLane::InteractiveRead);
        let write = self.front_if_runnable(state, StorageExecutionLane::InteractiveWrite);
        let interactive = match (read, write) {
            (Some(read_id), Some(write_id)) if read_id <= write_id => {
                Some(StorageExecutionLane::InteractiveRead)
            }
            (Some(_), Some(_)) => Some(StorageExecutionLane::InteractiveWrite),
            (Some(_), None) => Some(StorageExecutionLane::InteractiveRead),
            (None, Some(_)) => Some(StorageExecutionLane::InteractiveWrite),
            (None, None) => None,
        };
        let background = self.front_if_runnable(state, StorageExecutionLane::Background);

        if background.is_some()
            && (interactive.is_none() || state.interactive_burst >= INTERACTIVE_BURST_LIMIT)
        {
            return Some((StorageExecutionLane::Background, 0));
        }
        interactive
            .map(|lane| (lane, 0))
            .or_else(|| background.map(|_| (StorageExecutionLane::Background, 0)))
    }

    fn front_if_runnable(&self, state: &SchedulerState, lane: StorageExecutionLane) -> Option<u64> {
        let job = state.queues[lane.index()].front()?;
        if self.backend.single_writer && job.write && state.active_writes > 0 {
            return None;
        }
        if lane == StorageExecutionLane::Background
            && state.active_background >= self.max_background
        {
            return None;
        }
        Some(job.id)
    }

    fn release_active_locked(
        &self,
        state: &mut SchedulerState,
        lane: StorageExecutionLane,
        write: bool,
    ) {
        state.active = state.active.saturating_sub(1);
        if write {
            state.active_writes = state.active_writes.saturating_sub(1);
        }
        if lane == StorageExecutionLane::Background {
            state.active_background = state.active_background.saturating_sub(1);
        }
        self.counters
            .lane(lane)
            .active
            .fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub(crate) struct StorageExecutionPlane {
    config: StorageExecutionPlaneConfig,
    accepting: Arc<AtomicBool>,
    next_job_id: AtomicU64,
    counters: Arc<PlaneCounters>,
    scheduler: Arc<Scheduler>,
}

impl StorageExecutionPlane {
    pub(crate) fn default_plane() -> Self {
        Self::from_validated(
            StorageExecutionPlaneConfig::default(),
            BackendConcurrency::CONCURRENT,
        )
    }

    pub(crate) fn sqlite_default_plane() -> Self {
        Self::from_validated(
            StorageExecutionPlaneConfig::default(),
            BackendConcurrency::SQLITE,
        )
    }

    pub(crate) fn new(config: StorageExecutionPlaneConfig) -> Result<Self> {
        let config = config.validate()?;
        Ok(Self::from_validated(config, BackendConcurrency::CONCURRENT))
    }

    pub(crate) fn new_sqlite(config: StorageExecutionPlaneConfig) -> Result<Self> {
        let config = config.validate()?;
        Ok(Self::from_validated(config, BackendConcurrency::SQLITE))
    }

    fn from_validated(config: StorageExecutionPlaneConfig, backend: BackendConcurrency) -> Self {
        let counters = Arc::new(PlaneCounters::default());
        Self {
            config,
            accepting: Arc::new(AtomicBool::new(true)),
            next_job_id: AtomicU64::new(1),
            scheduler: Arc::new(Scheduler::new(config, backend, Arc::clone(&counters))),
            counters,
        }
    }

    pub(crate) async fn execute<T, F>(
        &self,
        lane: StorageExecutionLane,
        write: bool,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(SessionError::StoragePlaneShutdown);
        }
        let id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let queued_at = Instant::now();
        let status = Arc::new(AtomicU8::new(JOB_QUEUED));
        let (grant, granted) = oneshot::channel();
        let mut ticket = QueuedTicket {
            id,
            lane,
            write,
            queued_at,
            status: Arc::clone(&status),
            scheduler: Arc::clone(&self.scheduler),
        };
        if let Err(error) = self.scheduler.enqueue(QueuedJob {
            id,
            lane,
            write,
            queued_at,
            state: status,
            grant,
        }) {
            ticket.disarm();
            return Err(error);
        }
        granted
            .await
            .map_err(|_| SessionError::StoragePlaneShutdown)?;
        let started = ticket.start()?;
        let counters = Arc::clone(&self.counters);

        tokio::task::spawn_blocking(move || execute_started_job(operation, started, counters, lane))
            .await
            .map_err(|error| SessionError::StorageWorkerJoin(error.to_string()))?
    }

    pub(crate) fn stats(&self) -> StorageExecutionPlaneStats {
        let (oldest_queue_age_micros, drained) = self.scheduler.queue_observation();
        let interactive_read = self
            .counters
            .lane(StorageExecutionLane::InteractiveRead)
            .snapshot(oldest_queue_age_micros[StorageExecutionLane::InteractiveRead.index()]);
        let interactive_write = self
            .counters
            .lane(StorageExecutionLane::InteractiveWrite)
            .snapshot(oldest_queue_age_micros[StorageExecutionLane::InteractiveWrite.index()]);
        let background = self
            .counters
            .lane(StorageExecutionLane::Background)
            .snapshot(oldest_queue_age_micros[StorageExecutionLane::Background.index()]);
        let lanes = [interactive_read, interactive_write, background];
        let sum_usize =
            |field: fn(&StorageExecutionLaneStats) -> usize| lanes.iter().map(field).sum::<usize>();
        let sum_u64 =
            |field: fn(&StorageExecutionLaneStats) -> u64| lanes.iter().map(field).sum::<u64>();
        let submitted = sum_u64(|lane| lane.submitted);
        let completed = sum_u64(|lane| lane.completed);
        let failed = sum_u64(|lane| lane.failed);
        let queue_wait_micros = StorageExecutionLane::ALL
            .iter()
            .map(|lane| {
                self.counters
                    .lane(*lane)
                    .queue_wait_micros
                    .load(Ordering::Relaxed)
            })
            .sum::<u64>();
        let service_micros = StorageExecutionLane::ALL
            .iter()
            .map(|lane| {
                self.counters
                    .lane(*lane)
                    .service_micros
                    .load(Ordering::Relaxed)
            })
            .sum::<u64>();
        StorageExecutionPlaneStats {
            accepting: self.accepting.load(Ordering::Acquire),
            drained,
            workers: self.config.workers,
            queue_capacity: self.config.queue_capacity,
            active: sum_usize(|lane| lane.active),
            queued: sum_usize(|lane| lane.queued),
            submitted,
            completed,
            failed,
            panicked: sum_u64(|lane| lane.panicked),
            queue_rejected: sum_u64(|lane| lane.queue_rejected),
            total_queue_wait_micros: queue_wait_micros,
            total_service_micros: service_micros,
            average_queue_wait_micros: average(queue_wait_micros, submitted),
            average_service_micros: average(service_micros, completed.saturating_add(failed)),
            oldest_queue_age_micros: oldest_queue_age_micros
                .into_iter()
                .max()
                .unwrap_or_default(),
            interactive_read,
            interactive_write,
            background,
        }
    }

    pub(crate) async fn shutdown_and_drain(
        &self,
        timeout: std::time::Duration,
    ) -> Result<StorageExecutionPlaneStats> {
        self.accepting.store(false, Ordering::Release);
        self.scheduler.stop_accepting();
        if tokio::time::timeout(timeout, async {
            while !self.scheduler.is_drained() {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await
        .is_err()
        {
            let stats = self.stats();
            return Err(SessionError::StorageDrainTimeout {
                active: stats.active,
                queued: stats.queued,
            });
        }
        Ok(self.stats())
    }
}

fn execute_started_job<T, F>(
    operation: F,
    mut capacity: StartedCapacity,
    counters: Arc<PlaneCounters>,
    lane: StorageExecutionLane,
) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let started = Instant::now();
    let result = std::panic::catch_unwind(AssertUnwindSafe(operation));
    counters
        .lane(lane)
        .service_micros
        .fetch_add(elapsed_micros(started), Ordering::Relaxed);
    match result {
        Ok(Ok(value)) => {
            counters
                .lane(lane)
                .completed
                .fetch_add(1, Ordering::Relaxed);
            capacity.release();
            Ok(value)
        }
        Ok(Err(error)) => {
            counters.lane(lane).failed.fetch_add(1, Ordering::Relaxed);
            capacity.release();
            Err(error)
        }
        Err(_) => {
            counters.lane(lane).failed.fetch_add(1, Ordering::Relaxed);
            counters.lane(lane).panicked.fetch_add(1, Ordering::Relaxed);
            capacity.release();
            Err(SessionError::StorageWorkerPanic)
        }
    }
}

#[derive(Debug)]
struct QueuedTicket {
    id: u64,
    lane: StorageExecutionLane,
    write: bool,
    queued_at: Instant,
    status: Arc<AtomicU8>,
    scheduler: Arc<Scheduler>,
}

impl QueuedTicket {
    fn start(&mut self) -> Result<StartedCapacity> {
        self.status
            .compare_exchange(
                JOB_GRANTED,
                JOB_STARTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| SessionError::StoragePlaneShutdown)?;
        self.scheduler
            .counters
            .lane(self.lane)
            .queue_wait_micros
            .fetch_add(elapsed_micros(self.queued_at), Ordering::Relaxed);
        Ok(StartedCapacity {
            lane: self.lane,
            write: self.write,
            status: Arc::clone(&self.status),
            scheduler: Arc::clone(&self.scheduler),
            released: false,
        })
    }

    fn disarm(&mut self) {
        self.status.store(JOB_RELEASED, Ordering::Release);
    }
}

impl Drop for QueuedTicket {
    fn drop(&mut self) {
        self.scheduler
            .cancel(self.id, self.lane, self.write, &self.status);
    }
}

#[derive(Debug)]
struct StartedCapacity {
    lane: StorageExecutionLane,
    write: bool,
    status: Arc<AtomicU8>,
    scheduler: Arc<Scheduler>,
    released: bool,
}

impl StartedCapacity {
    fn release(&mut self) {
        if !self.released {
            self.released = true;
            self.status.store(JOB_RELEASED, Ordering::Release);
            self.scheduler.finish(self.lane, self.write);
        }
    }
}

impl Drop for StartedCapacity {
    fn drop(&mut self) {
        self.release();
    }
}

fn average(total: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        0
    } else {
        total / denominator
    }
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn elapsed_micros_at(started: Instant, now: Instant) -> u64 {
    u64::try_from(now.saturating_duration_since(started).as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn plane(workers: usize, queue_capacity: usize) -> StorageExecutionPlane {
        StorageExecutionPlane::new(StorageExecutionPlaneConfig {
            workers,
            queue_capacity,
        })
        .expect("plane")
    }

    fn sqlite_plane(workers: usize, queue_capacity: usize) -> StorageExecutionPlane {
        StorageExecutionPlane::new_sqlite(StorageExecutionPlaneConfig {
            workers,
            queue_capacity,
        })
        .expect("sqlite plane")
    }

    async fn wait_until(predicate: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("condition");
    }

    fn update_max(max: &AtomicUsize, value: usize) {
        let mut current = max.load(Ordering::Acquire);
        while value > current {
            match max.compare_exchange(current, value, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    #[tokio::test]
    async fn blocking_job_does_not_block_tokio_heartbeat() {
        let plane = Arc::new(plane(1, 2));
        let task = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveRead, false, || {
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
    async fn queue_is_globally_bounded_and_rejection_is_attributed_to_lane() {
        let plane = Arc::new(plane(1, 1));
        let release = Arc::new(AtomicBool::new(false));
        let first = {
            let plane = Arc::clone(&plane);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveRead, false, move || {
                        while !release.load(Ordering::Acquire) {
                            std::thread::yield_now();
                        }
                        Ok(())
                    })
                    .await
            })
        };
        wait_until(|| plane.stats().active == 1).await;
        let second = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::Background, false, || Ok(()))
                    .await
            })
        };
        wait_until(|| plane.stats().queued == 1).await;
        let error = plane
            .execute(StorageExecutionLane::InteractiveWrite, true, || Ok(()))
            .await
            .expect_err("queue full");
        assert!(error.to_string().contains("queue is full"));
        assert_eq!(plane.stats().interactive_write.queue_rejected, 1);
        release.store(true, Ordering::Release);
        first.await.expect("first task").expect("first job");
        second.await.expect("second task").expect("second job");
    }

    #[tokio::test]
    async fn sqlite_writes_are_serial_while_reads_can_run_concurrently() {
        let plane = Arc::new(sqlite_plane(4, 4));
        let write_active = Arc::new(AtomicUsize::new(0));
        let write_max = Arc::new(AtomicUsize::new(0));
        let read_active = Arc::new(AtomicUsize::new(0));
        let read_max = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let plane = Arc::clone(&plane);
            let active = Arc::clone(&write_active);
            let max = Arc::clone(&write_max);
            tasks.push(tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveWrite, true, move || {
                        let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                        update_max(&max, now);
                        std::thread::sleep(Duration::from_millis(30));
                        active.fetch_sub(1, Ordering::AcqRel);
                        Ok(())
                    })
                    .await
            }));
        }
        for _ in 0..2 {
            let plane = Arc::clone(&plane);
            let active = Arc::clone(&read_active);
            let max = Arc::clone(&read_max);
            tasks.push(tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveRead, false, move || {
                        let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                        update_max(&max, now);
                        std::thread::sleep(Duration::from_millis(30));
                        active.fetch_sub(1, Ordering::AcqRel);
                        Ok(())
                    })
                    .await
            }));
        }
        for task in tasks {
            task.await.expect("task").expect("job");
        }
        assert_eq!(write_max.load(Ordering::Acquire), 1);
        assert!(read_max.load(Ordering::Acquire) >= 2);
    }

    #[tokio::test]
    async fn concurrent_backend_does_not_serialize_writes() {
        let plane = Arc::new(plane(2, 2));
        let active = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let plane = Arc::clone(&plane);
            let active = Arc::clone(&active);
            let max = Arc::clone(&max);
            tasks.push(tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveWrite, true, move || {
                        let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                        update_max(&max, now);
                        std::thread::sleep(Duration::from_millis(30));
                        active.fetch_sub(1, Ordering::AcqRel);
                        Ok(())
                    })
                    .await
            }));
        }
        for task in tasks {
            task.await.expect("task").expect("job");
        }
        assert_eq!(max.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn queued_interactive_work_precedes_older_background_work() {
        let plane = Arc::new(plane(1, 3));
        let release = Arc::new(AtomicBool::new(false));
        let order = Arc::new(Mutex::new(Vec::new()));
        let blocker = {
            let plane = Arc::clone(&plane);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveRead, false, move || {
                        while !release.load(Ordering::Acquire) {
                            std::thread::yield_now();
                        }
                        Ok(())
                    })
                    .await
            })
        };
        wait_until(|| plane.stats().active == 1).await;
        let background = {
            let plane = Arc::clone(&plane);
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::Background, false, move || {
                        order.lock().expect("order").push("background");
                        Ok(())
                    })
                    .await
            })
        };
        wait_until(|| plane.stats().background.queued == 1).await;
        let interactive = {
            let plane = Arc::clone(&plane);
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveWrite, true, move || {
                        order.lock().expect("order").push("interactive");
                        Ok(())
                    })
                    .await
            })
        };
        wait_until(|| plane.stats().interactive_write.queued == 1).await;
        release.store(true, Ordering::Release);
        blocker.await.expect("blocker").expect("blocker job");
        interactive
            .await
            .expect("interactive")
            .expect("interactive job");
        background
            .await
            .expect("background")
            .expect("background job");
        assert_eq!(
            order.lock().expect("order").as_slice(),
            ["interactive", "background"]
        );
    }

    #[tokio::test]
    async fn background_concurrency_is_bounded_below_total_workers() {
        let plane = Arc::new(plane(3, 3));
        let release = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for _ in 0..3 {
            let plane = Arc::clone(&plane);
            let release = Arc::clone(&release);
            tasks.push(tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::Background, false, move || {
                        while !release.load(Ordering::Acquire) {
                            std::thread::yield_now();
                        }
                        Ok(())
                    })
                    .await
            }));
        }
        wait_until(|| {
            let stats = plane.stats().background;
            stats.active == 2 && stats.queued == 1
        })
        .await;
        assert_eq!(plane.stats().active, 2);
        release.store(true, Ordering::Release);
        for task in tasks {
            task.await.expect("task").expect("job");
        }
    }

    #[tokio::test]
    async fn reserved_interactive_worker_does_not_expand_queue_capacity() {
        let plane = Arc::new(plane(3, 1));
        let release = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for _ in 0..3 {
            let plane = Arc::clone(&plane);
            let release = Arc::clone(&release);
            tasks.push(tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::Background, false, move || {
                        while !release.load(Ordering::Acquire) {
                            std::thread::yield_now();
                        }
                        Ok(())
                    })
                    .await
            }));
        }
        wait_until(|| {
            let stats = plane.stats().background;
            stats.active == 2 && stats.queued == 1
        })
        .await;

        let error = plane
            .execute(StorageExecutionLane::Background, false, || Ok(()))
            .await
            .expect_err("background queue must remain exactly bounded");
        assert!(matches!(error, SessionError::StorageQueueFull { .. }));
        assert_eq!(plane.stats().background.queued, 1);
        assert_eq!(plane.stats().background.queue_rejected, 1);

        release.store(true, Ordering::Release);
        for task in tasks {
            task.await.expect("task").expect("job");
        }
    }

    #[tokio::test]
    async fn cancelled_waiter_releases_lane_and_global_capacity() {
        let plane = Arc::new(plane(1, 1));
        let release = Arc::new(AtomicBool::new(false));
        let first = {
            let plane = Arc::clone(&plane);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveRead, false, move || {
                        while !release.load(Ordering::Acquire) {
                            std::thread::yield_now();
                        }
                        Ok(7)
                    })
                    .await
            })
        };
        wait_until(|| plane.stats().active == 1).await;
        let waiting = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::Background, false, || Ok(9))
                    .await
            })
        };
        wait_until(|| plane.stats().queued == 1).await;
        waiting.abort();
        let _ = waiting.await;
        wait_until(|| plane.stats().queued == 0).await;
        release.store(true, Ordering::Release);
        assert_eq!(first.await.expect("first").expect("started job"), 7);
        assert_eq!(
            plane
                .execute(StorageExecutionLane::InteractiveWrite, true, || Ok(11))
                .await
                .expect("next job"),
            11
        );
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
                    .execute(StorageExecutionLane::InteractiveWrite, true, move || {
                        std::thread::sleep(Duration::from_millis(30));
                        completed.store(true, Ordering::Release);
                        Ok(())
                    })
                    .await
            })
        };
        wait_until(|| plane.stats().active == 1).await;
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
            .execute(
                StorageExecutionLane::InteractiveWrite,
                true,
                || -> Result<()> { panic!("boom") },
            )
            .await
            .expect_err("panic");
        assert!(matches!(error, SessionError::StorageWorkerPanic));
        assert_eq!(
            plane
                .execute(StorageExecutionLane::InteractiveRead, false, || Ok(3))
                .await
                .expect("next job"),
            3
        );
        assert_eq!(plane.stats().interactive_write.panicked, 1);
    }

    #[tokio::test]
    async fn backend_error_is_preserved_without_retry_reclassification() {
        let plane = plane(1, 1);
        let error = plane
            .execute(StorageExecutionLane::Background, false, || {
                Err::<(), _>(SessionError::Store("commit outcome unknown".to_string()))
            })
            .await
            .expect_err("backend error");
        assert!(matches!(
            error,
            SessionError::Store(message) if message == "commit outcome unknown"
        ));
        assert_eq!(plane.stats().background.failed, 1);
    }

    #[tokio::test]
    async fn shutdown_rejects_new_work() {
        let plane = plane(1, 1);
        let stats = plane
            .shutdown_and_drain(Duration::from_secs(1))
            .await
            .expect("idle plane drains");
        assert!(!stats.accepting);
        let error = plane
            .execute(StorageExecutionLane::InteractiveRead, false, || Ok(()))
            .await
            .expect_err("shutdown");
        assert!(matches!(error, SessionError::StoragePlaneShutdown));
    }

    #[tokio::test]
    async fn shutdown_drains_admitted_active_and_queued_work() {
        let plane = Arc::new(plane(1, 2));
        let release = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicUsize::new(0));
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let plane = Arc::clone(&plane);
            let release = Arc::clone(&release);
            let completed = Arc::clone(&completed);
            jobs.push(tokio::spawn(async move {
                plane
                    .execute(StorageExecutionLane::InteractiveWrite, true, move || {
                        while !release.load(Ordering::Acquire) {
                            std::thread::yield_now();
                        }
                        completed.fetch_add(1, Ordering::AcqRel);
                        Ok(())
                    })
                    .await
            }));
        }
        wait_until(|| {
            let stats = plane.stats();
            stats.active == 1 && stats.queued == 1
        })
        .await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        let queued_stats = plane.stats();
        assert!(queued_stats.oldest_queue_age_micros > 0);
        assert!(queued_stats.interactive_write.oldest_queue_age_micros > 0);
        assert!(!queued_stats.drained);

        let draining = {
            let plane = Arc::clone(&plane);
            tokio::spawn(async move { plane.shutdown_and_drain(Duration::from_secs(2)).await })
        };
        wait_until(|| !plane.stats().accepting).await;
        assert!(matches!(
            plane
                .execute(StorageExecutionLane::InteractiveRead, false, || Ok(()))
                .await,
            Err(SessionError::StoragePlaneShutdown)
        ));

        release.store(true, Ordering::Release);
        for job in jobs {
            job.await.expect("join").expect("admitted work drains");
        }
        let stats = draining
            .await
            .expect("drain join")
            .expect("drain completes");
        assert_eq!(completed.load(Ordering::Acquire), 2);
        assert_eq!(stats.active, 0);
        assert_eq!(stats.queued, 0);
        assert!(!stats.accepting);
        assert!(stats.drained);
    }
}
