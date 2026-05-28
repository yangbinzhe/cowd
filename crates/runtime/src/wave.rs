//! Wave Orchestration for Parallel Task Execution.
//!
//! Implements a wave-based execution model where tasks are organized into waves
//! based on dependencies, allowing parallel execution within each wave.
//!
//! # Architecture
//!
//! - `WaveTask`: Individual task with dependencies and payload
//! - `Wave`: Collection of tasks that can execute in parallel
//! - `WaveOrchestrator`: Manages task registration, wave building, and execution
//! - `WaveExecutor`: Trait for executing tasks (implement by application)

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::RwLock;

/// Wave execution error.
#[derive(Error, Debug, Clone)]
pub enum WaveError {
    #[error("dependency cycle detected: {0}")]
    DependencyCycle(String),

    #[error("task not found: {0}")]
    TaskNotFound(String),

    #[error("wave execution failed: {0}")]
    ExecutionFailed(String),

    #[error("invalid dependency: {0}")]
    InvalidDependency(String),

    #[error("execution timeout: {0}")]
    Timeout(String),

    #[error("cancelled")]
    Cancelled,
}

/// Task identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub String);

impl TaskId {
    /// Create a new task ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Task status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Task is pending.
    Pending,
    /// Task is running.
    Running,
    /// Task completed successfully.
    Completed,
    /// Task failed.
    Failed,
    /// Task was skipped.
    Skipped,
    /// Task timed out.
    Timeout,
    /// Task was cancelled.
    Cancelled,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::Pending
    }
}

/// Task result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Task ID.
    pub task_id: TaskId,
    /// Whether the task succeeded.
    pub success: bool,
    /// Output from the task.
    pub output: Option<String>,
    /// Error message if failed.
    pub error: Option<String>,
    /// Execution duration in milliseconds.
    pub duration_ms: u64,
}

/// A task to be executed.
#[derive(Debug, Clone)]
pub struct WaveTask {
    /// Task identifier.
    pub id: TaskId,
    /// Task name for display.
    pub name: String,
    /// Task description.
    pub description: Option<String>,
    /// Dependencies on other tasks (by task ID).
    pub dependencies: Vec<TaskId>,
    /// Task priority (higher = earlier execution within wave).
    pub priority: i32,
    /// Whether this task can run in parallel with others.
    pub parallelizable: bool,
    /// Task payload/data.
    pub payload: serde_json::Value,
}

impl WaveTask {
    /// Create a new task.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: TaskId::new(id),
            name: name.into(),
            description: None,
            dependencies: Vec::new(),
            priority: 0,
            parallelizable: true,
            payload: serde_json::Value::Null,
        }
    }

    /// Add a dependency.
    pub fn with_dependency(mut self, dep: TaskId) -> Self {
        self.dependencies.push(dep);
        self
    }

    /// Set priority.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set description.
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set payload.
    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }
}

/// A wave of tasks that can be executed in parallel.
#[derive(Debug, Clone)]
pub struct Wave {
    /// Wave number.
    pub number: u32,
    /// Tasks in this wave.
    pub tasks: Vec<TaskId>,
    /// Status of the wave.
    pub status: WaveStatus,
}

impl Wave {
    /// Create a new wave.
    pub fn new(number: u32) -> Self {
        Self {
            number,
            tasks: Vec::new(),
            status: WaveStatus::Waiting,
        }
    }
}

/// Wave status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaveStatus {
    /// Wave is waiting for dependencies.
    Waiting,
    /// Wave is ready to execute.
    Ready,
    /// Wave is executing.
    Executing,
    /// Wave completed.
    Completed,
    /// Wave failed.
    Failed,
    /// Wave was cancelled.
    Cancelled,
}

impl Default for WaveStatus {
    fn default() -> Self {
        Self::Waiting
    }
}

/// Wave execution result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaveResult {
    /// Wave number.
    pub wave_number: u32,
    /// Whether all tasks in the wave succeeded.
    pub success: bool,
    /// Task results.
    pub task_results: Vec<TaskResult>,
    /// Total duration in milliseconds.
    pub duration_ms: u64,
    /// Error message if wave failed.
    pub error: Option<String>,
}

/// Error recovery policy for wave execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorPolicy {
    /// Retry failed tasks up to a limit before giving up.
    Retry { max_retries: usize },
    /// Skip failed tasks and continue with remaining waves.
    Skip,
    /// Abort the entire plan on first failure.
    Abort,
}

impl Default for ErrorPolicy {
    fn default() -> Self {
        ErrorPolicy::Skip
    }
}

/// Wave execution configuration.
#[derive(Debug, Clone)]
pub struct WaveConfig {
    /// Maximum parallel tasks per wave.
    pub max_parallel: usize,
    /// Continue on failure (execute remaining tasks in wave).
    pub continue_on_failure: bool,
    /// Stop execution on first wave failure.
    pub stop_on_wave_failure: bool,
    /// Timeout per task in milliseconds.
    pub task_timeout_ms: u64,
    /// Timeout per wave in milliseconds.
    pub wave_timeout_ms: u64,
    /// Error recovery policy.
    pub error_policy: ErrorPolicy,
}

impl Default for WaveConfig {
    fn default() -> Self {
        Self {
            max_parallel: 4,
            continue_on_failure: true,
            stop_on_wave_failure: false,
            task_timeout_ms: 300000, // 5 minutes
            wave_timeout_ms: 1800000, // 30 minutes
            error_policy: ErrorPolicy::default(),
        }
    }
}

impl WaveConfig {
    /// Set maximum parallel tasks.
    pub fn with_max_parallel(mut self, max: usize) -> Self {
        self.max_parallel = max;
        self
    }

    /// Set continue on failure.
    pub fn with_continue_on_failure(mut self, continue_on_failure: bool) -> Self {
        self.continue_on_failure = continue_on_failure;
        self
    }

    /// Set stop on wave failure.
    pub fn with_stop_on_wave_failure(mut self, stop: bool) -> Self {
        self.stop_on_wave_failure = stop;
        self
    }

    /// Set task timeout.
    pub fn with_task_timeout(mut self, timeout_ms: u64) -> Self {
        self.task_timeout_ms = timeout_ms;
        self
    }

    /// Set wave timeout.
    pub fn with_wave_timeout(mut self, timeout_ms: u64) -> Self {
        self.wave_timeout_ms = timeout_ms;
        self
    }

    /// Set error recovery policy.
    pub fn with_error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }
}

/// Trait for executing tasks asynchronously.
///
/// Implement this trait to define how tasks should be executed.
/// The executor receives the task payload and returns a result.
pub trait WaveExecutor: Send + Sync + 'static {
    /// Execute a single task and return the result.
    fn execute(
        self: Arc<Self>,
        task: WaveTask,
        context: TaskContext,
    ) -> Pin<Box<dyn Future<Output = Result<TaskResult, WaveError>> + Send>>;

    /// Called before a wave starts executing.
    fn on_wave_start(self: Arc<Self>, wave: Wave) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move { let _ = wave; })
    }

    /// Called after a wave completes.
    fn on_wave_complete(self: Arc<Self>, wave: Wave, result: WaveResult) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move { let _ = (wave, result); })
    }
}

/// Context passed to task executors.
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// Wave number being executed.
    pub wave_number: u32,
    /// Total number of waves.
    pub total_waves: usize,
    /// Results from previous waves.
    pub previous_results: HashMap<TaskId, TaskResult>,
    /// Cancellation flag.
    pub cancelled: Arc<RwLock<bool>>,
}

impl TaskContext {
    /// Check if execution should be cancelled.
    pub async fn is_cancelled(&self) -> bool {
        *self.cancelled.read().await
    }

    /// Get result of a completed task.
    pub fn get_result(&self, task_id: &TaskId) -> Option<&TaskResult> {
        self.previous_results.get(task_id)
    }
}

/// Execution state shared across waves.
#[derive(Debug)]
pub struct WaveExecutionState {
    /// Task statuses.
    pub task_statuses: HashMap<TaskId, TaskStatus>,
    /// Task results.
    pub task_results: HashMap<TaskId, TaskResult>,
    /// Wave statuses.
    pub wave_statuses: HashMap<u32, WaveStatus>,
    /// Cancellation flag.
    pub cancelled: Arc<RwLock<bool>>,
}

impl WaveExecutionState {
    /// Create new execution state.
    pub fn new() -> Self {
        Self {
            task_statuses: HashMap::new(),
            task_results: HashMap::new(),
            wave_statuses: HashMap::new(),
            cancelled: Arc::new(RwLock::new(false)),
        }
    }

    /// Mark a task as running.
    pub fn set_running(&mut self, task_id: &TaskId) {
        self.task_statuses.insert(task_id.clone(), TaskStatus::Running);
    }

    /// Record a task result.
    pub fn record_result(&mut self, result: TaskResult) {
        let status = if result.success {
            TaskStatus::Completed
        } else if result.error.as_deref() == Some("Cancelled") {
            TaskStatus::Cancelled
        } else {
            TaskStatus::Failed
        };
        self.task_statuses.insert(result.task_id.clone(), status);
        self.task_results.insert(result.task_id.clone(), result);
    }

    /// Check if cancelled.
    pub async fn is_cancelled(&self) -> bool {
        *self.cancelled.read().await
    }

    /// Cancel execution.
    pub async fn cancel(&self) {
        *self.cancelled.write().await = true;
    }
}

impl Default for WaveExecutionState {
    fn default() -> Self {
        Self::new()
    }
}

/// Wave orchestrator for managing task execution.
pub struct WaveOrchestrator {
    config: WaveConfig,
    tasks: HashMap<TaskId, WaveTask>,
    waves: Vec<Wave>,
}

impl WaveOrchestrator {
    /// Create a new orchestrator.
    pub fn new() -> Self {
        Self {
            config: WaveConfig::default(),
            tasks: HashMap::new(),
            waves: Vec::new(),
        }
    }

    /// Set configuration.
    pub fn with_config(mut self, config: WaveConfig) -> Self {
        self.config = config;
        self
    }

    /// Add a task.
    pub fn add_task(&mut self, task: WaveTask) -> &mut Self {
        self.tasks.insert(task.id.clone(), task);
        self
    }

    /// Add multiple tasks.
    pub fn add_tasks(&mut self, tasks: impl IntoIterator<Item = WaveTask>) -> &mut Self {
        for task in tasks {
            self.add_task(task);
        }
        self
    }

    /// Build waves from task dependencies.
    pub fn build_waves(&mut self) -> Result<&mut Self, WaveError> {
        // Detect cycles using DFS
        self.detect_cycles()?;

        // Calculate in-degree for each task
        // For "task B depends on A", the edge is A -> B, so B's in-degree should increase
        let mut in_degree: HashMap<TaskId, usize> = self
            .tasks
            .keys()
            .map(|id| (id.clone(), 0))
            .collect();

        for task in self.tasks.values() {
            // Each task's in-degree = number of dependencies (how many tasks it waits on)
            // If task B depends on [A, C], then B has in-degree 2 (waits for 2 tasks)
            let dep_count = task.dependencies.len();
            if let Some(degree) = in_degree.get_mut(&task.id) {
                *degree = dep_count;
            }
        }

        // Kahn's algorithm for topological sort with wave assignment
        let mut waves: Vec<Vec<TaskId>> = Vec::new();

        loop {
            // Find tasks with no remaining dependencies
            let ready: Vec<TaskId> = in_degree
                .iter()
                .filter(|(_, &degree)| degree == 0)
                .map(|(id, _)| id.clone())
                .collect();

            if ready.is_empty() {
                break;
            }

            // Sort by priority within wave
            let mut ready_sorted: Vec<&TaskId> = ready.iter().collect();
            ready_sorted.sort_by(|a, b| {
                let task_a = self.tasks.get(*a).unwrap();
                let task_b = self.tasks.get(*b).unwrap();
                task_b.priority.cmp(&task_a.priority)
            });

            let current_wave: Vec<TaskId> = ready_sorted.into_iter().cloned().collect();

            // Remove from dependency graph
            for id in &current_wave {
                in_degree.remove(id);
                for task in self.tasks.values() {
                    if task.dependencies.contains(id) {
                        if let Some(degree) = in_degree.get_mut(&task.id) {
                            *degree = degree.saturating_sub(1);
                        }
                    }
                }
            }

            waves.push(current_wave);
        }

        // Check if all tasks were assigned
        if !in_degree.is_empty() {
            let unassigned: Vec<String> = in_degree.keys().map(|id| id.0.clone()).collect();
            return Err(WaveError::DependencyCycle(format!(
                "unassigned tasks: {}",
                unassigned.join(", ")
            )));
        }

        // Build Wave objects
        self.waves = waves
            .into_iter()
            .enumerate()
            .map(|(i, tasks)| {
                let mut wave = Wave::new(i as u32 + 1);
                wave.tasks = tasks;
                wave.status = WaveStatus::Waiting;
                wave
            })
            .collect();

        Ok(self)
    }

    /// Detect circular dependencies.
    fn detect_cycles(&self) -> Result<(), WaveError> {
        let mut visited: HashSet<TaskId> = HashSet::new();
        let mut rec_stack: HashSet<TaskId> = HashSet::new();
        let mut path: Vec<TaskId> = Vec::new();

        fn dfs(
            tasks: &HashMap<TaskId, WaveTask>,
            visited: &mut HashSet<TaskId>,
            rec_stack: &mut HashSet<TaskId>,
            path: &mut Vec<TaskId>,
            node: &TaskId,
        ) -> Option<Vec<TaskId>> {
            visited.insert(node.clone());
            rec_stack.insert(node.clone());
            path.push(node.clone());

            if let Some(task) = tasks.get(node) {
                for dep in &task.dependencies {
                    if !visited.contains(dep) {
                        if let Some(cycle) = dfs(tasks, visited, rec_stack, path, dep) {
                            return Some(cycle);
                        }
                    } else if rec_stack.contains(dep) {
                        // Found cycle
                        if let Some(cycle_start) = path.iter().position(|id| id == dep) {
                            return Some(path[cycle_start..].to_vec());
                        }
                    }
                }
            }

            path.pop();
            rec_stack.remove(node);
            None
        }

        for id in self.tasks.keys() {
            if !visited.contains(id) {
                if let Some(cycle) = dfs(&self.tasks, &mut visited, &mut rec_stack, &mut path, id) {
                    let cycle_str: Vec<String> = cycle.iter().map(|id| id.0.clone()).collect();
                    return Err(WaveError::DependencyCycle(cycle_str.join(" -> ")));
                }
            }
        }

        Ok(())
    }

    /// Get all waves.
    pub fn get_waves(&self) -> &[Wave] {
        &self.waves
    }

    /// Get wave by number.
    pub fn get_wave(&self, number: u32) -> Option<&Wave> {
        self.waves.iter().find(|w| w.number == number)
    }

    /// Get task by ID.
    pub fn get_task(&self, id: &TaskId) -> Option<&WaveTask> {
        self.tasks.get(id)
    }

    /// Get all tasks.
    pub fn get_all_tasks(&self) -> &HashMap<TaskId, WaveTask> {
        &self.tasks
    }

    /// Get the number of waves.
    pub fn wave_count(&self) -> usize {
        self.waves.len()
    }

    /// Get tasks in a specific wave.
    pub fn get_wave_tasks(&self, wave_number: u32) -> Option<Vec<&WaveTask>> {
        let wave = self.get_wave(wave_number)?;
        Some(
            wave
                .tasks
                .iter()
                .filter_map(|id| self.tasks.get(id))
                .collect(),
        )
    }

    /// Execute all waves with the given executor.
    ///
    /// Returns a vector of wave results, one for each wave.
    pub async fn execute<E: WaveExecutor>(
        &self,
        executor: E,
    ) -> Result<Vec<WaveResult>, WaveError> {
        use std::sync::Arc;
        let executor = Arc::new(executor);

        let mut state = WaveExecutionState::new();
        let mut previous_results: HashMap<TaskId, TaskResult> = HashMap::new();
        let mut wave_results: Vec<WaveResult> = Vec::new();

        // Initialize wave statuses
        for wave in &self.waves {
            state.wave_statuses.insert(wave.number, WaveStatus::Ready);
        }

        for wave in &self.waves {
            // Check if cancelled
            if state.is_cancelled().await {
                return Err(WaveError::Cancelled);
            }

            let wave = wave.clone();
            state.wave_statuses.insert(wave.number, WaveStatus::Executing);

            // Notify wave start
            executor.clone().on_wave_start(wave.clone()).await;

            // Execute wave
            let wave_result = self
                .execute_wave(executor.clone(), &wave, &mut state, &previous_results)
                .await;

            // Update wave status
            let success = wave_result.success;
            state.wave_statuses.insert(
                wave.number,
                if success {
                    WaveStatus::Completed
                } else {
                    WaveStatus::Failed
                },
            );

            // Store task results for next wave
            for result in &wave_result.task_results {
                previous_results.insert(result.task_id.clone(), result.clone());
            }

            // Notify wave complete
            executor.clone().on_wave_complete(wave.clone(), wave_result.clone()).await;

            wave_results.push(wave_result);

            // Check if we should stop on failure
            if !success && self.config.stop_on_wave_failure {
                // Mark remaining waves as cancelled
                for remaining_wave in &self.waves {
                    if remaining_wave.number > wave.number {
                        state.wave_statuses.insert(remaining_wave.number, WaveStatus::Cancelled);
                    }
                }
                break;
            }
        }

        Ok(wave_results)
    }

    /// Execute a single wave with parallel task execution and error recovery.
    async fn execute_wave(
        &self,
        executor: std::sync::Arc<dyn WaveExecutor>,
        wave: &Wave,
        state: &mut WaveExecutionState,
        previous_results: &HashMap<TaskId, TaskResult>,
    ) -> WaveResult {
        let start_time = Instant::now();
        let max_parallel = self.config.max_parallel;
        let task_timeout = Duration::from_millis(self.config.task_timeout_ms);
        let cancelled = state.cancelled.clone();

        // Create context for this wave
        let context = TaskContext {
            wave_number: wave.number,
            total_waves: self.waves.len(),
            previous_results: previous_results.clone(),
            cancelled,
        };

        // Execute tasks in parallel batches
        let mut task_results: Vec<TaskResult> = Vec::new();
        let tasks: Vec<_> = wave.tasks.iter().cloned().collect();
        let mut failed = false;

        // Process in batches of max_parallel
        for chunk in tasks.chunks(max_parallel) {
            // Check cancellation
            if state.is_cancelled().await {
                for task_id in chunk {
                    let result = TaskResult {
                        task_id: task_id.clone(),
                        success: false,
                        output: None,
                        error: Some("Cancelled".to_string()),
                        duration_ms: 0,
                    };
                    task_results.push(result.clone());
                    state.record_result(result);
                }
                continue;
            }

            let chunk_results = self
                .execute_task_chunk(executor.clone(), chunk, &context, task_timeout)
                .await;

            for result in chunk_results {
                if !result.success && !self.config.continue_on_failure {
                    failed = true;
                }
                task_results.push(result.clone());
                state.record_result(result);
            }

            if failed && self.config.stop_on_wave_failure {
                break;
            }
        }

        // Apply error recovery policy for failed tasks
        let failed_tasks: Vec<(TaskId, Option<String>)> = task_results
            .iter()
            .filter(|r| !r.success)
            .map(|r| (r.task_id.clone(), r.error.clone()))
            .collect();

        if !failed_tasks.is_empty() {
            match &self.config.error_policy {
                ErrorPolicy::Retry { max_retries } => {
                    tracing::info!(
                        wave = wave.number,
                        failed = failed_tasks.len(),
                        max_retries = max_retries,
                        "applying retry policy for failed tasks"
                    );
                    let retry_results = self
                        .retry_failed_tasks(
                            executor.clone(),
                            &failed_tasks,
                            &context,
                            task_timeout,
                            *max_retries,
                        )
                        .await;

                    // Replace failed results with retry results
                    let failed_ids: std::collections::HashSet<TaskId> =
                        failed_tasks.iter().map(|(id, _)| id.clone()).collect();
                    task_results.retain(|r| !failed_ids.contains(&r.task_id));
                    for result in retry_results {
                        state.record_result(result.clone());
                        task_results.push(result);
                    }
                }
                ErrorPolicy::Skip => {
                    tracing::info!(
                        wave = wave.number,
                        skipped = failed_tasks.len(),
                        "skipping failed tasks and continuing"
                    );
                }
                ErrorPolicy::Abort => {
                    tracing::warn!(
                        wave = wave.number,
                        failed = failed_tasks.len(),
                        "aborting execution due to failed tasks"
                    );
                    let duration_ms = start_time.elapsed().as_millis() as u64;
                    return WaveResult {
                        wave_number: wave.number,
                        success: false,
                        task_results,
                        duration_ms,
                        error: Some(format!("{} tasks failed, aborting", failed_tasks.len())),
                    };
                }
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;
        let success = task_results.iter().all(|r| r.success);

        WaveResult {
            wave_number: wave.number,
            success,
            task_results,
            duration_ms,
            error: None,
        }
    }

    /// Execute a chunk of tasks in parallel.
    async fn execute_task_chunk(
        &self,
        executor: std::sync::Arc<dyn WaveExecutor>,
        task_ids: &[TaskId],
        context: &TaskContext,
        timeout: Duration,
    ) -> Vec<TaskResult> {
        
        let mut handles: Vec<tokio::task::JoinHandle<TaskResult>> = Vec::new();

        for task_id in task_ids {
            if let Some(task) = self.tasks.get(task_id) {
                let task = task.clone();
                let context = context.clone();
                let timeout = timeout;
                let exec = executor.clone();

                let handle: tokio::task::JoinHandle<TaskResult> = tokio::spawn(async move {
                    let start = Instant::now();
                    let task_id = task.id.clone();

                    // Execute with timeout
                    let result = tokio::time::timeout(timeout, async {
                        if context.is_cancelled().await {
                            return Err(WaveError::Cancelled);
                        }
                        exec.execute(task.clone(), context.clone()).await
                    })
                    .await;

                    let duration_ms = start.elapsed().as_millis() as u64;

                    match result {
                        Ok(Ok(mut task_result)) => {
                            task_result.duration_ms = duration_ms;
                            task_result
                        }
                        Ok(Err(e)) => TaskResult {
                            task_id,
                            success: false,
                            output: None,
                            error: Some(e.to_string()),
                            duration_ms,
                        },
                        Err(_) => TaskResult {
                            task_id,
                            success: false,
                            output: None,
                            error: Some("Task timeout".to_string()),
                            duration_ms,
                        },
                    }
                });

                handles.push(handle);
            }
        }

        // Wait for all tasks in chunk
        let mut results: Vec<TaskResult> = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(_) => {} // Task panicked, skip
            }
        }

        results
    }

    /// Cancel all execution.
    pub async fn cancel(&self, state: &WaveExecutionState) {
        state.cancel().await;
    }

    /// Get current task status.
    pub fn get_task_status(state: &WaveExecutionState, id: &TaskId) -> Option<TaskStatus> {
        state.task_statuses.get(id).copied()
    }

    /// Get wave status.
    pub fn get_wave_status(state: &WaveExecutionState, wave_number: u32) -> Option<WaveStatus> {
        state.wave_statuses.get(&wave_number).copied()
    }

    /// Retry failed tasks within a wave according to ErrorPolicy::Retry.
    async fn retry_failed_tasks(
        &self,
        executor: std::sync::Arc<dyn WaveExecutor>,
        failed_tasks: &[(TaskId, Option<String>)],
        context: &TaskContext,
        task_timeout: Duration,
        max_retries: usize,
    ) -> Vec<TaskResult> {
        let mut retry_results = Vec::new();
        // T32: Track still-failing tasks across attempts instead of re-using
        // the immutable `failed_tasks` parameter on every iteration.
        let mut still_failing: Vec<(TaskId, Option<String>)> = failed_tasks.to_vec();

        for attempt in 1..=max_retries {
            if still_failing.is_empty() {
                break;
            }

            tracing::info!(
                attempt = attempt,
                failed_count = still_failing.len(),
                "retrying failed tasks"
            );

            let retry_ids: Vec<TaskId> =
                still_failing.iter().map(|(id, _)| id.clone()).collect();
            let chunk_results = self
                .execute_task_chunk(executor.clone(), &retry_ids, context, task_timeout)
                .await;

            still_failing.clear();
            for result in chunk_results {
                if result.success {
                    retry_results.push(result);
                } else {
                    // T32: Keep in still_failing for the next retry pass;
                    // do NOT push to retry_results yet to avoid duplicates.
                    still_failing.push((result.task_id.clone(), result.error.clone()));
                }
            }

            if still_failing.is_empty() {
                break;
            }

            if attempt >= max_retries {
                tracing::warn!(
                    attempts = max_retries,
                    failed_count = still_failing.len(),
                    "retry limit exhausted, some tasks remain failed"
                );
                // T32: Only now push final still-failing results (once, no duplicates).
                for (id, error) in &still_failing {
                    retry_results.push(TaskResult {
                        task_id: id.clone(),
                        success: false,
                        output: None,
                        error: error.clone(),
                        duration_ms: 0,
                    });
                }
            }
        }

        retry_results
    }
}

impl Default for WaveOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

/// Dependency graph for visualization.
pub struct DependencyGraph<'a> {
    orchestrator: &'a WaveOrchestrator,
}

impl<'a> DependencyGraph<'a> {
    /// Create from orchestrator.
    pub fn new(orchestrator: &'a WaveOrchestrator) -> Self {
        Self { orchestrator }
    }

    /// Generate mermaid flowchart representation.
    pub fn to_mermaid(&self) -> String {
        let mut output = String::from("flowchart TD\n");

        // Add nodes
        for task in self.orchestrator.tasks.values() {
            let label = task.name.replace('"', "'");
            output.push_str(&format!("    {}[\"{}\"]\n", task.id.0, label));
        }

        output.push('\n');

        // Add edges
        for task in self.orchestrator.tasks.values() {
            for dep in &task.dependencies {
                output.push_str(&format!("    {} --> {}\n", dep.0, task.id.0));
            }
        }

        output.push('\n');

        // Add subgraph for waves
        for (i, wave) in self.orchestrator.waves.iter().enumerate() {
            output.push_str(&format!("    subgraph wave{} [Wave {}]\n", i + 1, i + 1));
            for task_id in &wave.tasks {
                output.push_str(&format!("        {}\n", task_id.0));
            }
            output.push_str("    end\n");
        }

        output
    }

    /// Generate DOT representation.
    pub fn to_dot(&self) -> String {
        let mut output = String::from("digraph Waves {\n");
        output.push_str("    rankdir=TB;\n");
        output.push_str("    node [shape=box];\n\n");

        // Add nodes
        for task in self.orchestrator.tasks.values() {
            let label = task.name.replace('"', "\\\"");
            output.push_str(&format!("    \"{}\" [label=\"{}\"];\n", task.id.0, label));
        }

        output.push('\n');

        // Add edges
        for task in self.orchestrator.tasks.values() {
            for dep in &task.dependencies {
                output.push_str(&format!("    \"{}\" -> \"{}\";\n", dep.0, task.id.0));
            }
        }

        output.push_str("}\n");
        output
    }

    /// Generate ASCII tree representation.
    pub fn to_ascii_tree(&self) -> String {
        let mut output = String::new();
        let mut visited: HashSet<TaskId> = HashSet::new();

        // Find root tasks (no dependencies)
        let roots: Vec<&TaskId> = self
            .orchestrator
            .tasks
            .values()
            .filter(|t| t.dependencies.is_empty())
            .map(|t| &t.id)
            .collect();

        for root_id in roots {
            self.print_task_tree(root_id, "", true, &mut output, &mut visited);
        }

        // Print orphaned tasks
        for task in self.orchestrator.tasks.values() {
            if !visited.contains(&task.id) {
                output.push_str(&format!("{}\n", task.name));
            }
        }

        output
    }

    fn print_task_tree(
        &self,
        task_id: &TaskId,
        prefix: &str,
        is_last: bool,
        output: &mut String,
        visited: &mut HashSet<TaskId>,
    ) {
        visited.insert(task_id.clone());

        if let Some(task) = self.orchestrator.tasks.get(task_id) {
            let connector = if is_last { "`-- " } else { "|-- " };
            output.push_str(&format!("{}{}{}\n", prefix, connector, task.name));

            let new_prefix = if is_last {
                format!("{}    ", prefix)
            } else {
                format!("{}|   ", prefix)
            };

            // Find dependents
            let dependents: Vec<&TaskId> = self
                .orchestrator
                .tasks
                .values()
                .filter(|t| t.dependencies.contains(task_id))
                .map(|t| &t.id)
                .collect();

            for (i, dep_id) in dependents.iter().enumerate() {
                let is_last_dep = i == dependents.len() - 1;
                self.print_task_tree(dep_id, &new_prefix, is_last_dep, output, visited);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wave_orchestrator() {
        let mut orchestrator = WaveOrchestrator::new();

        // Create tasks: A -> B -> C, D parallel with B
        orchestrator.add_task(WaveTask::new("a", "Task A"));
        orchestrator.add_task(
            WaveTask::new("b", "Task B").with_dependency(TaskId::new("a")),
        );
        orchestrator.add_task(
            WaveTask::new("c", "Task C").with_dependency(TaskId::new("b")),
        );
        orchestrator.add_task(
            WaveTask::new("d", "Task D").with_dependency(TaskId::new("a")),
        );

        orchestrator.build_waves().unwrap();

        let waves = orchestrator.get_waves();
        assert_eq!(waves.len(), 3);

        // Wave 1: A
        assert_eq!(waves[0].number, 1);
        assert!(waves[0].tasks.contains(&TaskId::new("a")));

        // Wave 2: B, D (parallel)
        assert_eq!(waves[1].number, 2);
        assert!(waves[1].tasks.contains(&TaskId::new("b")));
        assert!(waves[1].tasks.contains(&TaskId::new("d")));

        // Wave 3: C
        assert_eq!(waves[2].number, 3);
        assert!(waves[2].tasks.contains(&TaskId::new("c")));
    }

    #[test]
    fn test_dependency_cycle_detection() {
        let mut orchestrator = WaveOrchestrator::new();

        // Create circular dependency: A -> B -> C -> A
        orchestrator.add_task(WaveTask::new("a", "Task A").with_dependency(TaskId::new("c")));
        orchestrator.add_task(WaveTask::new("b", "Task B").with_dependency(TaskId::new("a")));
        orchestrator.add_task(WaveTask::new("c", "Task C").with_dependency(TaskId::new("b")));

        let result = orchestrator.build_waves();
        assert!(result.is_err());
    }

    #[test]
    fn test_dependency_graph_mermaid() {
        let mut orchestrator = WaveOrchestrator::new();

        orchestrator.add_task(WaveTask::new("a", "Task A"));
        orchestrator.add_task(
            WaveTask::new("b", "Task B").with_dependency(TaskId::new("a")),
        );

        orchestrator.build_waves().unwrap();

        let graph = DependencyGraph::new(&orchestrator);
        let mermaid = graph.to_mermaid();

        assert!(mermaid.contains("Task A"));
        assert!(mermaid.contains("Task B"));
        assert!(mermaid.contains("-->"));
    }

    #[test]
    fn test_get_wave_tasks() {
        let mut orchestrator = WaveOrchestrator::new();

        orchestrator.add_task(WaveTask::new("a", "Task A"));
        orchestrator.add_task(WaveTask::new("b", "Task B").with_dependency(TaskId::new("a")));

        orchestrator.build_waves().unwrap();

        let wave1_tasks = orchestrator.get_wave_tasks(1).unwrap();
        assert_eq!(wave1_tasks.len(), 1);
        assert_eq!(wave1_tasks[0].name, "Task A");
    }

    #[test]
    fn test_task_priority() {
        let mut orchestrator = WaveOrchestrator::new();

        orchestrator.add_task(WaveTask::new("a", "Task A").with_priority(1));
        orchestrator.add_task(WaveTask::new("b", "Task B").with_priority(10));

        orchestrator.build_waves().unwrap();

        let wave1_tasks = orchestrator.get_wave_tasks(1).unwrap();
        // Higher priority should come first
        assert_eq!(wave1_tasks.len(), 2);
        assert_eq!(wave1_tasks[0].name, "Task B"); // priority 10
        assert_eq!(wave1_tasks[1].name, "Task A"); // priority 1
    }

    #[tokio::test]
    async fn test_async_execution() {
        use std::sync::Arc;

        let mut orchestrator = WaveOrchestrator::new();

        orchestrator.add_task(WaveTask::new("a", "Task A"));
        orchestrator.add_task(WaveTask::new("b", "Task B").with_dependency(TaskId::new("a")));

        orchestrator.build_waves().unwrap();

        // Create a simple executor
        struct SimpleExecutor;
        impl WaveExecutor for SimpleExecutor {
            fn execute(
                self: Arc<Self>,
                task: WaveTask,
                _context: TaskContext,
            ) -> Pin<Box<dyn Future<Output = Result<TaskResult, WaveError>> + Send>> {
                let name = task.name.clone();
                let task_id = task.id.clone();
                Box::pin(async move {
                    // Simulate some async work
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(TaskResult {
                        task_id,
                        success: true,
                        output: Some(format!("Executed: {}", name)),
                        error: None,
                        duration_ms: 10,
                    })
                })
            }
        }

        let executor = SimpleExecutor;
        let results = orchestrator.execute(executor).await.unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].success);
        assert!(results[1].success);
    }
}
