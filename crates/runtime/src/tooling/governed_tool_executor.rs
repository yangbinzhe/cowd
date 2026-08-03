//! Completion-driven executor for validated governed tool DAGs.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::governed_tool_plan::{GovernedToolPlanTask, ValidatedGovernedToolDag};

pub type GovernedToolFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug)]
pub enum GovernedToolAdmission<A> {
    Granted(A),
    Deferred,
    Refused(String),
}

/// Runtime adapter for hard gates, resource admission, effects, and durable
/// terminal commits. The executor owns ordering only; it does not become a
/// second policy, resource, or receipt owner.
pub trait GovernedToolExecutionContext: Send + Sync {
    type Output: Send;
    type Admission: Send;
    type Receipt: Send;

    fn local_ceiling(&self) -> usize;

    fn is_cancelled(&self) -> bool {
        false
    }

    fn try_admit<'a>(
        &'a self,
        task: &'a GovernedToolPlanTask,
    ) -> GovernedToolFuture<'a, GovernedToolAdmission<Self::Admission>>;

    fn wait_for_admission_change(&self) -> GovernedToolFuture<'_, ()> {
        Box::pin(std::future::pending())
    }

    fn wait_for_cancellation(&self) -> GovernedToolFuture<'_, ()> {
        Box::pin(std::future::pending())
    }

    fn execute<'a>(
        &'a self,
        task: &'a GovernedToolPlanTask,
        admission: &'a mut Self::Admission,
    ) -> GovernedToolFuture<'a, Result<Self::Output, String>>;

    fn classify_output(&self, _output: &Self::Output) -> Result<(), String> {
        Ok(())
    }

    /// Return a durable terminal receipt produced by the governed early lane.
    /// The finalized DAG consumes it as an already-completed node, preserving
    /// dependencies and protocol ordering without executing the effect twice.
    fn precompleted(
        &self,
        _task: &GovernedToolPlanTask,
    ) -> Option<(GovernedToolTaskTerminal<Self::Output>, Self::Receipt)> {
        None
    }

    fn commit_terminal<'a>(
        &'a self,
        task: &'a GovernedToolPlanTask,
        terminal: &'a GovernedToolTaskTerminal<Self::Output>,
    ) -> GovernedToolFuture<'a, Result<Self::Receipt, String>>;

    /// Publish the canonical start of one governed invocation. The executor
    /// guarantees this hook runs at most once, including tasks that become
    /// refused or blocked before their effect body starts.
    fn on_task_started(&self, _task: &GovernedToolPlanTask) {}

    /// Publish the canonical terminal state after the durable commit attempt.
    /// `receipt` is absent only when the durability barrier itself failed.
    fn on_task_terminal(
        &self,
        _task: &GovernedToolPlanTask,
        _terminal: &GovernedToolTaskTerminal<Self::Output>,
        _receipt: Option<&Self::Receipt>,
    ) {
    }

    fn cancel_active(&self, _task_id: &str) {}
}

#[derive(Debug, PartialEq, Eq)]
pub enum GovernedToolTaskTerminal<O> {
    Succeeded(O),
    FailedOutput {
        output: O,
        error: String,
    },
    Failed {
        error: String,
    },
    Refused {
        reason: String,
    },
    Blocked {
        predecessor_id: String,
        reason: String,
    },
    Cancelled {
        reason: String,
    },
    Panicked {
        reason: String,
    },
}

impl<O> GovernedToolTaskTerminal<O> {
    fn succeeded(&self) -> bool {
        matches!(self, Self::Succeeded(_))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct GovernedToolTaskOutcome<O, R> {
    pub original_call_index: usize,
    pub task_id: String,
    pub terminal: GovernedToolTaskTerminal<O>,
    pub receipt: Option<R>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GovernedToolExecutionReport<O, R> {
    /// Outcomes are always in original model call order.
    pub outcomes: Vec<GovernedToolTaskOutcome<O, R>>,
    /// Terminal order is retained separately for latency and scheduling audit.
    pub completion_order: Vec<String>,
    pub terminal_task_ids: Vec<String>,
    pub blocked_task_ids: Vec<String>,
    pub max_active: usize,
    /// Observed active ceiling per governed safety category.
    pub max_active_by_category: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GovernedToolExecutor;

struct WorkerCompletion<O, A> {
    task_index: usize,
    terminal: GovernedToolTaskTerminal<O>,
    admission: A,
}

type WorkerFuture<'a, O, A> = Pin<Box<dyn Future<Output = WorkerCompletion<O, A>> + Send + 'a>>;

struct GovernedToolExecutorState<O, R> {
    remaining_indegree: Vec<usize>,
    ready: BTreeSet<(usize, usize)>,
    active: BTreeSet<usize>,
    lifecycle_started: BTreeSet<usize>,
    terminal: BTreeSet<usize>,
    blocked: BTreeSet<usize>,
    outcomes: Vec<Option<GovernedToolTaskOutcome<O, R>>>,
    completion_order: Vec<String>,
    max_active: usize,
    max_active_by_category: BTreeMap<String, usize>,
}

impl<O, R> GovernedToolExecutorState<O, R> {
    fn new(dag: &ValidatedGovernedToolDag) -> Self {
        let remaining_indegree = dag
            .tasks
            .iter()
            .map(|task| task.indegree)
            .collect::<Vec<_>>();
        let ready = dag
            .tasks
            .iter()
            .enumerate()
            .filter_map(|(index, task)| {
                (task.indegree == 0).then_some((task.original_call_index, index))
            })
            .collect();
        Self {
            remaining_indegree,
            ready,
            active: BTreeSet::new(),
            lifecycle_started: BTreeSet::new(),
            terminal: BTreeSet::new(),
            blocked: BTreeSet::new(),
            outcomes: (0..dag.tasks.len()).map(|_| None).collect(),
            completion_order: Vec::with_capacity(dag.tasks.len()),
            max_active: 0,
            max_active_by_category: BTreeMap::new(),
        }
    }

    fn is_complete(&self) -> bool {
        self.terminal.len() == self.outcomes.len()
    }

    fn record(
        &mut self,
        task_index: usize,
        task: &GovernedToolPlanTask,
        outcome: GovernedToolTaskOutcome<O, R>,
    ) {
        if matches!(outcome.terminal, GovernedToolTaskTerminal::Blocked { .. }) {
            self.blocked.insert(task_index);
        }
        self.terminal.insert(task_index);
        self.completion_order.push(task.tool_call_id.clone());
        self.outcomes[task.original_call_index] = Some(outcome);
    }
}

impl GovernedToolExecutor {
    pub async fn execute<C>(
        &self,
        dag: &ValidatedGovernedToolDag,
        context: &C,
    ) -> GovernedToolExecutionReport<C::Output, C::Receipt>
    where
        C: GovernedToolExecutionContext,
    {
        let ceiling = context.local_ceiling().max(1);
        let mut state = GovernedToolExecutorState::new(dag);
        let mut workers = FuturesUnordered::<WorkerFuture<'_, C::Output, C::Admission>>::new();
        seed_precompleted(dag, context, &mut state).await;

        while !state.is_complete() {
            if context.is_cancelled() {
                cancel_remaining(dag, context, &mut state, &mut workers).await;
                break;
            }

            let admitted = refill(dag, context, ceiling, &mut state, &mut workers).await;
            if state.is_complete() {
                break;
            }

            if workers.is_empty() {
                if !state.ready.is_empty() && !admitted {
                    tokio::select! {
                        _ = context.wait_for_admission_change() => {}
                        _ = context.wait_for_cancellation() => {
                            cancel_remaining(dag, context, &mut state, &mut workers).await;
                        }
                    }
                    continue;
                }
                mark_unreachable_tasks(dag, context, &mut state).await;
                break;
            }

            tokio::select! {
                joined = workers.next() => {
                    if let Some(joined) = joined {
                        handle_joined(dag, context, &mut state, joined).await;
                    }
                }
                _ = context.wait_for_admission_change(), if !state.ready.is_empty() => {}
                _ = context.wait_for_cancellation() => {
                    cancel_remaining(dag, context, &mut state, &mut workers).await;
                }
            }
        }

        let terminal_task_ids = state
            .terminal
            .iter()
            .map(|index| dag.tasks[*index].tool_call_id.clone())
            .collect();
        let blocked_task_ids = state
            .blocked
            .iter()
            .map(|index| dag.tasks[*index].tool_call_id.clone())
            .collect();
        GovernedToolExecutionReport {
            outcomes: state
                .outcomes
                .into_iter()
                .map(|outcome| {
                    outcome.expect("validated DAG executor produces one outcome per task")
                })
                .collect(),
            completion_order: state.completion_order,
            terminal_task_ids,
            blocked_task_ids,
            max_active: state.max_active,
            max_active_by_category: state.max_active_by_category,
        }
    }
}

async fn seed_precompleted<C>(
    dag: &ValidatedGovernedToolDag,
    context: &C,
    state: &mut GovernedToolExecutorState<C::Output, C::Receipt>,
) where
    C: GovernedToolExecutionContext,
{
    for task_index in &dag.topological_order {
        if state.terminal.contains(task_index) || state.remaining_indegree[*task_index] != 0 {
            continue;
        }
        let task = &dag.tasks[*task_index];
        let Some((terminal, receipt)) = context.precompleted(task) else {
            continue;
        };
        state.ready.remove(&(task.original_call_index, *task_index));
        state.lifecycle_started.insert(*task_index);
        let succeeded = terminal.succeeded();
        let failure_reason = (!succeeded).then(|| terminal_reason(&terminal));
        state.record(
            *task_index,
            task,
            GovernedToolTaskOutcome {
                original_call_index: task.original_call_index,
                task_id: task.tool_call_id.clone(),
                terminal,
                receipt: Some(receipt),
            },
        );
        if succeeded {
            for successor in &task.successors {
                state.remaining_indegree[*successor] =
                    state.remaining_indegree[*successor].saturating_sub(1);
                if state.remaining_indegree[*successor] == 0 {
                    let successor_task = &dag.tasks[*successor];
                    state
                        .ready
                        .insert((successor_task.original_call_index, *successor));
                }
            }
        } else {
            block_descendants(
                dag,
                context,
                state,
                *task_index,
                failure_reason.unwrap_or_else(|| "precompleted task did not succeed".to_string()),
            )
            .await;
        }
    }
}

async fn refill<'a, C>(
    dag: &ValidatedGovernedToolDag,
    context: &'a C,
    ceiling: usize,
    state: &mut GovernedToolExecutorState<C::Output, C::Receipt>,
    workers: &mut FuturesUnordered<WorkerFuture<'a, C::Output, C::Admission>>,
) -> bool
where
    C: GovernedToolExecutionContext,
{
    let candidates = state.ready.len();
    let mut admitted_any = false;
    for _ in 0..candidates {
        if state.active.len() >= ceiling {
            break;
        }
        let Some((original_index, task_index)) = state.ready.pop_first() else {
            break;
        };
        let task = &dag.tasks[task_index];
        if !task_can_start(dag, state, task_index) {
            state.ready.insert((original_index, task_index));
            continue;
        }
        match context.try_admit(task).await {
            GovernedToolAdmission::Granted(admission) => {
                admitted_any = true;
                notify_task_started(context, state, task_index, task);
                let worker_task = task.clone();
                workers.push(Box::pin(async move {
                    let mut admission = admission;
                    let terminal =
                        match AssertUnwindSafe(context.execute(&worker_task, &mut admission))
                            .catch_unwind()
                            .await
                        {
                            Ok(Ok(output)) => match context.classify_output(&output) {
                                Ok(()) => GovernedToolTaskTerminal::Succeeded(output),
                                Err(error) => {
                                    GovernedToolTaskTerminal::FailedOutput { output, error }
                                }
                            },
                            Ok(Err(error)) if context.is_cancelled() => {
                                GovernedToolTaskTerminal::Cancelled { reason: error }
                            }
                            Ok(Err(error)) => GovernedToolTaskTerminal::Failed { error },
                            Err(payload) => GovernedToolTaskTerminal::Panicked {
                                reason: panic_message(payload),
                            },
                        };
                    WorkerCompletion {
                        task_index,
                        terminal,
                        admission,
                    }
                }));
                state.active.insert(task_index);
                state.max_active = state.max_active.max(state.active.len());
                let category = safety_category_name(task);
                let active_in_category = state
                    .active
                    .iter()
                    .filter(|index| dag.tasks[**index].safety_category == task.safety_category)
                    .count();
                state
                    .max_active_by_category
                    .entry(category.to_string())
                    .and_modify(|observed| *observed = (*observed).max(active_in_category))
                    .or_insert(active_in_category);
            }
            GovernedToolAdmission::Deferred => {
                state.ready.insert((original_index, task_index));
            }
            GovernedToolAdmission::Refused(reason) => {
                admitted_any = true;
                finish_task(
                    dag,
                    context,
                    state,
                    task_index,
                    GovernedToolTaskTerminal::Refused { reason },
                )
                .await;
            }
        }
    }
    admitted_any
}

fn task_can_start<O, R>(
    dag: &ValidatedGovernedToolDag,
    state: &GovernedToolExecutorState<O, R>,
    task_index: usize,
) -> bool {
    let task = &dag.tasks[task_index];
    if !task.can_parallelize && !state.active.is_empty() {
        return false;
    }
    if state
        .active
        .iter()
        .any(|index| !dag.tasks[*index].can_parallelize)
    {
        return false;
    }
    if state.active.iter().any(|index| {
        let active = &dag.tasks[*index];
        task.conflicts
            .iter()
            .any(|conflict| conflict.tool_call_id == active.tool_call_id)
            || active
                .conflicts
                .iter()
                .any(|conflict| conflict.tool_call_id == task.tool_call_id)
    }) {
        return false;
    }
    let active_in_category = state
        .active
        .iter()
        .filter(|index| dag.tasks[**index].safety_category == task.safety_category)
        .count();
    active_in_category < task.max_concurrency.max(1)
}

fn safety_category_name(task: &GovernedToolPlanTask) -> &'static str {
    use crate::tool_orchestrator::ToolSafetyCategory;
    match task.safety_category {
        ToolSafetyCategory::ReadOnly => "read_only",
        ToolSafetyCategory::WriteLocal => "write_local",
        ToolSafetyCategory::Network => "network",
        ToolSafetyCategory::Destructive => "destructive",
    }
}

async fn handle_joined<C>(
    dag: &ValidatedGovernedToolDag,
    context: &C,
    state: &mut GovernedToolExecutorState<C::Output, C::Receipt>,
    completion: WorkerCompletion<C::Output, C::Admission>,
) where
    C: GovernedToolExecutionContext,
{
    let WorkerCompletion {
        task_index,
        terminal,
        admission,
    } = completion;
    state.active.remove(&task_index);
    finish_task(dag, context, state, task_index, terminal).await;
    drop(admission);
}

async fn finish_task<C>(
    dag: &ValidatedGovernedToolDag,
    context: &C,
    state: &mut GovernedToolExecutorState<C::Output, C::Receipt>,
    task_index: usize,
    mut terminal: GovernedToolTaskTerminal<C::Output>,
) where
    C: GovernedToolExecutionContext,
{
    if state.terminal.contains(&task_index) {
        return;
    }
    let task = &dag.tasks[task_index];
    notify_task_started(context, state, task_index, task);
    let receipt = match context.commit_terminal(task, &terminal).await {
        Ok(receipt) => Some(receipt),
        Err(error) => {
            terminal = GovernedToolTaskTerminal::Failed {
                error: format!("durable terminal commit failed: {error}"),
            };
            None
        }
    };
    let succeeded = terminal.succeeded() && receipt.is_some();
    let failure_reason = (!succeeded).then(|| terminal_reason(&terminal));
    context.on_task_terminal(task, &terminal, receipt.as_ref());
    state.record(
        task_index,
        task,
        GovernedToolTaskOutcome {
            original_call_index: task.original_call_index,
            task_id: task.tool_call_id.clone(),
            terminal,
            receipt,
        },
    );

    if succeeded {
        for successor in &task.successors {
            if state.terminal.contains(successor) {
                continue;
            }
            state.remaining_indegree[*successor] =
                state.remaining_indegree[*successor].saturating_sub(1);
            if state.remaining_indegree[*successor] == 0 {
                let successor_task = &dag.tasks[*successor];
                state
                    .ready
                    .insert((successor_task.original_call_index, *successor));
            }
        }
    } else {
        block_descendants(
            dag,
            context,
            state,
            task_index,
            failure_reason.unwrap_or_else(|| "predecessor did not succeed".to_string()),
        )
        .await;
    }
}

async fn block_descendants<C>(
    dag: &ValidatedGovernedToolDag,
    context: &C,
    state: &mut GovernedToolExecutorState<C::Output, C::Receipt>,
    failed_index: usize,
    reason: String,
) where
    C: GovernedToolExecutionContext,
{
    let failed_id = dag.tasks[failed_index].tool_call_id.clone();
    let mut pending = dag.tasks[failed_index]
        .successors
        .iter()
        .map(|index| (*index, failed_id.clone(), reason.clone()))
        .collect::<Vec<_>>();
    while let Some((task_index, predecessor_id, block_reason)) = pending.pop() {
        if state.terminal.contains(&task_index) {
            continue;
        }
        let task = &dag.tasks[task_index];
        state.ready.remove(&(task.original_call_index, task_index));
        notify_task_started(context, state, task_index, task);
        let terminal = GovernedToolTaskTerminal::Blocked {
            predecessor_id,
            reason: block_reason,
        };
        let receipt = context.commit_terminal(task, &terminal).await.ok();
        context.on_task_terminal(task, &terminal, receipt.as_ref());
        state.record(
            task_index,
            task,
            GovernedToolTaskOutcome {
                original_call_index: task.original_call_index,
                task_id: task.tool_call_id.clone(),
                terminal,
                receipt,
            },
        );
        for successor in task.successors.iter().rev() {
            pending.push((
                *successor,
                task.tool_call_id.clone(),
                format!("predecessor `{}` was blocked", task.tool_call_id),
            ));
        }
    }
}

async fn cancel_remaining<C>(
    dag: &ValidatedGovernedToolDag,
    context: &C,
    state: &mut GovernedToolExecutorState<C::Output, C::Receipt>,
    workers: &mut FuturesUnordered<WorkerFuture<'_, C::Output, C::Admission>>,
) where
    C: GovernedToolExecutionContext,
{
    for task_index in &state.active {
        context.cancel_active(&dag.tasks[*task_index].tool_call_id);
    }

    for task_index in 0..dag.tasks.len() {
        if state.terminal.contains(&task_index) || state.active.contains(&task_index) {
            continue;
        }
        let task = &dag.tasks[task_index];
        notify_task_started(context, state, task_index, task);
        let terminal = GovernedToolTaskTerminal::Cancelled {
            reason: "execution context cancelled before task start".to_string(),
        };
        let receipt = context.commit_terminal(task, &terminal).await.ok();
        context.on_task_terminal(task, &terminal, receipt.as_ref());
        state.record(
            task_index,
            task,
            GovernedToolTaskOutcome {
                original_call_index: task.original_call_index,
                task_id: task.tool_call_id.clone(),
                terminal,
                receipt,
            },
        );
    }

    // Started effects retain their admission/receipt lifecycle. Dropping the
    // futures here would let synchronous work continue detached while the DAG
    // falsely published Cancelled. Drain each started task to its real
    // terminal commit after forwarding cancellation to the execution host.
    while let Some(completion) = workers.next().await {
        handle_joined(dag, context, state, completion).await;
    }
}

async fn mark_unreachable_tasks<C>(
    dag: &ValidatedGovernedToolDag,
    context: &C,
    state: &mut GovernedToolExecutorState<C::Output, C::Receipt>,
) where
    C: GovernedToolExecutionContext,
{
    for task_index in 0..dag.tasks.len() {
        if state.terminal.contains(&task_index) {
            continue;
        }
        let task = &dag.tasks[task_index];
        notify_task_started(context, state, task_index, task);
        let terminal = GovernedToolTaskTerminal::Blocked {
            predecessor_id: "executor".to_string(),
            reason: "validated DAG became unreachable during execution".to_string(),
        };
        let receipt = context.commit_terminal(task, &terminal).await.ok();
        context.on_task_terminal(task, &terminal, receipt.as_ref());
        state.record(
            task_index,
            task,
            GovernedToolTaskOutcome {
                original_call_index: task.original_call_index,
                task_id: task.tool_call_id.clone(),
                terminal,
                receipt,
            },
        );
    }
}

fn notify_task_started<C>(
    context: &C,
    state: &mut GovernedToolExecutorState<C::Output, C::Receipt>,
    task_index: usize,
    task: &GovernedToolPlanTask,
) where
    C: GovernedToolExecutionContext,
{
    if state.lifecycle_started.insert(task_index) {
        context.on_task_started(task);
    }
}

fn terminal_reason<O>(terminal: &GovernedToolTaskTerminal<O>) -> String {
    match terminal {
        GovernedToolTaskTerminal::Succeeded(_) => "task succeeded".to_string(),
        GovernedToolTaskTerminal::FailedOutput { error, .. } => error.clone(),
        GovernedToolTaskTerminal::Failed { error } => error.clone(),
        GovernedToolTaskTerminal::Refused { reason }
        | GovernedToolTaskTerminal::Cancelled { reason }
        | GovernedToolTaskTerminal::Panicked { reason } => reason.clone(),
        GovernedToolTaskTerminal::Blocked {
            predecessor_id,
            reason,
        } => format!("blocked by `{predecessor_id}`: {reason}"),
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "tool worker panicked".to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::governed_tool_plan::GovernedToolCompiler;
    use crate::tool_dispatch::ToolRequest;

    struct TestContext {
        ceiling: usize,
        delays_ms: BTreeMap<String, u64>,
        failures: BTreeSet<String>,
        panics: BTreeSet<String>,
        refusals: BTreeSet<String>,
        commit_failures: BTreeSet<String>,
        precompleted: BTreeSet<String>,
        deferred_once: Mutex<BTreeSet<String>>,
        admission_notify: Arc<tokio::sync::Notify>,
        started_notify: tokio::sync::Notify,
        cancelled: AtomicBool,
        cancellation_notify: tokio::sync::Notify,
        active_cancellation_notify: tokio::sync::Notify,
        active: AtomicUsize,
        max_active: AtomicUsize,
        starts: Mutex<Vec<String>>,
        lifecycle_starts: Mutex<Vec<String>>,
        lifecycle_terminals: Mutex<Vec<String>>,
        commits: Mutex<Vec<String>>,
        cancelled_active: Mutex<Vec<String>>,
    }

    impl TestContext {
        fn new(ceiling: usize) -> Self {
            Self {
                ceiling,
                delays_ms: BTreeMap::new(),
                failures: BTreeSet::new(),
                panics: BTreeSet::new(),
                refusals: BTreeSet::new(),
                commit_failures: BTreeSet::new(),
                precompleted: BTreeSet::new(),
                deferred_once: Mutex::new(BTreeSet::new()),
                admission_notify: Arc::new(tokio::sync::Notify::new()),
                started_notify: tokio::sync::Notify::new(),
                cancelled: AtomicBool::new(false),
                cancellation_notify: tokio::sync::Notify::new(),
                active_cancellation_notify: tokio::sync::Notify::new(),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                starts: Mutex::new(Vec::new()),
                lifecycle_starts: Mutex::new(Vec::new()),
                lifecycle_terminals: Mutex::new(Vec::new()),
                commits: Mutex::new(Vec::new()),
                cancelled_active: Mutex::new(Vec::new()),
            }
        }

        fn delay(mut self, task_id: &str, delay_ms: u64) -> Self {
            self.delays_ms.insert(task_id.to_string(), delay_ms);
            self
        }

        fn fail(mut self, task_id: &str) -> Self {
            self.failures.insert(task_id.to_string());
            self
        }

        fn panic(mut self, task_id: &str) -> Self {
            self.panics.insert(task_id.to_string());
            self
        }

        fn refuse(mut self, task_id: &str) -> Self {
            self.refusals.insert(task_id.to_string());
            self
        }

        fn fail_commit(mut self, task_id: &str) -> Self {
            self.commit_failures.insert(task_id.to_string());
            self
        }

        fn precomplete(mut self, task_id: &str) -> Self {
            self.precompleted.insert(task_id.to_string());
            self
        }

        fn defer_once(self, task_id: &str) -> Self {
            self.deferred_once
                .lock()
                .expect("deferred lock")
                .insert(task_id.to_string());
            self
        }

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::SeqCst);
            self.cancellation_notify.notify_waiters();
        }
    }

    impl GovernedToolExecutionContext for TestContext {
        type Output = String;
        type Admission = ();
        type Receipt = String;

        fn local_ceiling(&self) -> usize {
            self.ceiling
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::SeqCst)
        }

        fn wait_for_cancellation(&self) -> GovernedToolFuture<'_, ()> {
            Box::pin(async move {
                if !self.is_cancelled() {
                    self.cancellation_notify.notified().await;
                }
            })
        }

        fn wait_for_admission_change(&self) -> GovernedToolFuture<'_, ()> {
            Box::pin(self.admission_notify.notified())
        }

        fn try_admit<'a>(
            &'a self,
            task: &'a GovernedToolPlanTask,
        ) -> GovernedToolFuture<'a, GovernedToolAdmission<Self::Admission>> {
            Box::pin(async move {
                if self.refusals.contains(&task.tool_call_id) {
                    GovernedToolAdmission::Refused(format!("{} refused", task.tool_call_id))
                } else if self
                    .deferred_once
                    .lock()
                    .expect("deferred lock")
                    .remove(&task.tool_call_id)
                {
                    let notify = Arc::clone(&self.admission_notify);
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        notify.notify_waiters();
                    });
                    GovernedToolAdmission::Deferred
                } else {
                    GovernedToolAdmission::Granted(())
                }
            })
        }

        fn execute<'a>(
            &'a self,
            task: &'a GovernedToolPlanTask,
            _admission: &'a mut Self::Admission,
        ) -> GovernedToolFuture<'a, Result<Self::Output, String>> {
            Box::pin(async move {
                self.starts
                    .lock()
                    .expect("starts lock")
                    .push(task.tool_call_id.clone());
                self.started_notify.notify_one();
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active.fetch_max(active, Ordering::SeqCst);
                let delay = self.delays_ms.get(&task.tool_call_id).copied().unwrap_or(1);
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(delay)) => {}
                    () = self.active_cancellation_notify.notified() => {
                        self.active.fetch_sub(1, Ordering::SeqCst);
                        return Err(format!("{} cancelled", task.tool_call_id));
                    }
                }
                if self.panics.contains(&task.tool_call_id) {
                    panic!("panic requested for {}", task.tool_call_id);
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
                if self.failures.contains(&task.tool_call_id) {
                    Err(format!("{} failed", task.tool_call_id))
                } else {
                    Ok(format!("{} output", task.tool_call_id))
                }
            })
        }

        fn commit_terminal<'a>(
            &'a self,
            task: &'a GovernedToolPlanTask,
            _terminal: &'a GovernedToolTaskTerminal<Self::Output>,
        ) -> GovernedToolFuture<'a, Result<Self::Receipt, String>> {
            Box::pin(async move {
                if self.commit_failures.contains(&task.tool_call_id) {
                    return Err(format!("{} commit failed", task.tool_call_id));
                }
                self.commits
                    .lock()
                    .expect("commits lock")
                    .push(task.tool_call_id.clone());
                Ok(format!("receipt:{}", task.tool_call_id))
            })
        }

        fn precompleted(
            &self,
            task: &GovernedToolPlanTask,
        ) -> Option<(GovernedToolTaskTerminal<Self::Output>, Self::Receipt)> {
            self.precompleted.contains(&task.tool_call_id).then(|| {
                (
                    GovernedToolTaskTerminal::Succeeded(format!(
                        "{} early output",
                        task.tool_call_id
                    )),
                    format!("early-receipt:{}", task.tool_call_id),
                )
            })
        }

        fn on_task_started(&self, task: &GovernedToolPlanTask) {
            self.lifecycle_starts
                .lock()
                .expect("lifecycle starts lock")
                .push(task.tool_call_id.clone());
        }

        fn on_task_terminal(
            &self,
            task: &GovernedToolPlanTask,
            _terminal: &GovernedToolTaskTerminal<Self::Output>,
            _receipt: Option<&Self::Receipt>,
        ) {
            self.lifecycle_terminals
                .lock()
                .expect("lifecycle terminals lock")
                .push(task.tool_call_id.clone());
        }

        fn cancel_active(&self, task_id: &str) {
            self.cancelled_active
                .lock()
                .expect("cancelled active lock")
                .push(task_id.to_string());
            self.active_cancellation_notify.notify_waiters();
        }
    }

    fn request(id: &str, dependencies: &[&str]) -> ToolRequest {
        ToolRequest {
            tool_use_id: id.to_string(),
            tool_name: "read_file".to_string(),
            input: format!(r#"{{"path":"{id}.txt"}}"#),
            depends_on: dependencies.iter().map(|id| (*id).to_string()).collect(),
        }
    }

    fn dag(requests: &[ToolRequest]) -> ValidatedGovernedToolDag {
        let workspace = std::env::current_dir().expect("test workspace");
        GovernedToolCompiler
            .compile(&workspace, requests, |name, input| {
                Some((
                    crate::governed_tool_plan::fixture_effect(name, input),
                    1,
                    "executor-test".to_string(),
                ))
            })
            .expect("valid executor test DAG")
    }

    #[tokio::test]
    async fn completion_refills_successors_without_waiting_for_unrelated_slow_tasks() {
        let dag = dag(&[
            request("slow", &[]),
            request("fast", &[]),
            request("fast-child", &["fast"]),
        ]);
        let context = Arc::new(
            TestContext::new(2)
                .delay("slow", 80)
                .delay("fast", 5)
                .delay("fast-child", 5),
        );

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert_eq!(report.completion_order, vec!["fast", "fast-child", "slow"]);
        assert_eq!(report.max_active, 2);
        assert_eq!(
            context.starts.lock().expect("starts lock").as_slice(),
            ["slow", "fast", "fast-child"]
        );
    }

    #[tokio::test]
    async fn failure_recursively_blocks_descendants_but_not_independent_tasks() {
        let dag = dag(&[
            request("root", &[]),
            request("child", &["root"]),
            request("grandchild", &["child"]),
            request("independent", &[]),
        ]);
        let context = Arc::new(TestContext::new(2).fail("root"));

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert_eq!(
            report.blocked_task_ids,
            vec!["child".to_string(), "grandchild".to_string()]
        );
        assert!(matches!(
            report.outcomes[0].terminal,
            GovernedToolTaskTerminal::Failed { .. }
        ));
        assert!(matches!(
            report.outcomes[1].terminal,
            GovernedToolTaskTerminal::Blocked { .. }
        ));
        assert!(matches!(
            report.outcomes[2].terminal,
            GovernedToolTaskTerminal::Blocked { .. }
        ));
        assert!(matches!(
            report.outcomes[3].terminal,
            GovernedToolTaskTerminal::Succeeded(_)
        ));
    }

    #[tokio::test]
    async fn report_preserves_original_order_despite_reverse_completion() {
        let dag = dag(&[
            request("first", &[]),
            request("second", &[]),
            request("third", &[]),
        ]);
        let context = Arc::new(
            TestContext::new(3)
                .delay("first", 30)
                .delay("second", 20)
                .delay("third", 5),
        );

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert_eq!(report.completion_order, vec!["third", "second", "first"]);
        assert_eq!(
            report
                .outcomes
                .iter()
                .map(|outcome| outcome.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second", "third"]
        );
    }

    #[tokio::test]
    async fn external_admission_wakeup_refills_while_an_unrelated_task_is_active() {
        let dag = dag(&[request("slow", &[]), request("deferred", &[])]);
        let context = Arc::new(
            TestContext::new(2)
                .delay("slow", 80)
                .delay("deferred", 5)
                .defer_once("deferred"),
        );

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert_eq!(report.completion_order, vec!["deferred", "slow"]);
        assert_eq!(report.max_active, 2);
    }

    #[tokio::test]
    async fn category_ceiling_is_an_execution_constraint() {
        let mut dag = dag(&[
            request("first", &[]),
            request("second", &[]),
            request("third", &[]),
        ]);
        for task in &mut dag.tasks {
            task.max_concurrency = 1;
        }
        let context = Arc::new(
            TestContext::new(8)
                .delay("first", 10)
                .delay("second", 10)
                .delay("third", 10),
        );

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert_eq!(report.max_active, 1);
        assert_eq!(report.max_active_by_category.get("read_only"), Some(&1));
    }

    #[tokio::test]
    async fn finalized_dag_consumes_precompleted_receipt_without_reexecution() {
        let dag = dag(&[request("early", &[]), request("dependent", &["early"])]);
        let context = Arc::new(TestContext::new(2).precomplete("early"));

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert_eq!(
            context.starts.lock().expect("starts lock").as_slice(),
            ["dependent"]
        );
        assert_eq!(
            report.outcomes[0].receipt.as_deref(),
            Some("early-receipt:early")
        );
        assert!(matches!(
            report.outcomes[1].terminal,
            GovernedToolTaskTerminal::Succeeded(_)
        ));
    }

    #[tokio::test]
    async fn panic_becomes_terminal_failure_and_blocks_dependents() {
        let dag = dag(&[request("panic", &[]), request("child", &["panic"])]);
        let context = Arc::new(TestContext::new(2).panic("panic"));

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert!(matches!(
            report.outcomes[0].terminal,
            GovernedToolTaskTerminal::Panicked { .. }
        ));
        assert!(matches!(
            report.outcomes[1].terminal,
            GovernedToolTaskTerminal::Blocked { .. }
        ));
    }

    #[tokio::test]
    async fn refusal_and_commit_failure_both_block_dependents() {
        let refusal_dag = dag(&[
            request("refused", &[]),
            request("refused-child", &["refused"]),
        ]);
        let refusal_report = GovernedToolExecutor
            .execute(&refusal_dag, &TestContext::new(1).refuse("refused"))
            .await;
        assert!(matches!(
            refusal_report.outcomes[0].terminal,
            GovernedToolTaskTerminal::Refused { .. }
        ));
        assert!(matches!(
            refusal_report.outcomes[1].terminal,
            GovernedToolTaskTerminal::Blocked { .. }
        ));

        let commit_dag = dag(&[
            request("commit-fails", &[]),
            request("commit-child", &["commit-fails"]),
        ]);
        let commit_report = GovernedToolExecutor
            .execute(
                &commit_dag,
                &TestContext::new(1).fail_commit("commit-fails"),
            )
            .await;
        assert!(matches!(
            commit_report.outcomes[0].terminal,
            GovernedToolTaskTerminal::Failed { .. }
        ));
        assert!(commit_report.outcomes[0].receipt.is_none());
        assert!(matches!(
            commit_report.outcomes[1].terminal,
            GovernedToolTaskTerminal::Blocked { .. }
        ));
    }

    #[tokio::test]
    async fn lifecycle_hooks_publish_exactly_once_for_every_terminal_path() {
        let dag = dag(&[
            request("fails", &[]),
            request("blocked", &["fails"]),
            request("refused", &[]),
            request("succeeds", &[]),
        ]);
        let context = Arc::new(
            TestContext::new(3)
                .fail("fails")
                .refuse("refused")
                .defer_once("succeeds"),
        );

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert_eq!(report.outcomes.len(), 4);
        let mut starts = context
            .lifecycle_starts
            .lock()
            .expect("lifecycle starts lock")
            .clone();
        let mut terminals = context
            .lifecycle_terminals
            .lock()
            .expect("lifecycle terminals lock")
            .clone();
        starts.sort();
        terminals.sort();
        let expected = vec![
            "blocked".to_string(),
            "fails".to_string(),
            "refused".to_string(),
            "succeeds".to_string(),
        ];
        assert_eq!(starts, expected);
        assert_eq!(terminals, expected);
    }

    #[tokio::test]
    async fn pre_cancelled_execution_never_starts_an_effect() {
        let dag = dag(&[request("first", &[]), request("second", &[])]);
        let context = Arc::new(TestContext::new(2));
        context.cancelled.store(true, Ordering::SeqCst);

        let report = GovernedToolExecutor.execute(&dag, context.as_ref()).await;

        assert!(context.starts.lock().expect("starts lock").is_empty());
        assert!(report
            .outcomes
            .iter()
            .all(|outcome| matches!(outcome.terminal, GovernedToolTaskTerminal::Cancelled { .. })));
    }

    #[tokio::test]
    async fn cancellation_propagates_to_active_work_and_terminates_the_dag() {
        let dag = dag(&[request("active", &[]), request("dependent", &["active"])]);
        let context = Arc::new(TestContext::new(1).delay("active", 5_000));
        let execution = tokio::spawn({
            let context = Arc::clone(&context);
            async move { GovernedToolExecutor.execute(&dag, context.as_ref()).await }
        });
        context.started_notify.notified().await;
        context.cancel();

        let report = tokio::time::timeout(Duration::from_secs(1), execution)
            .await
            .expect("executor responds to cancellation")
            .expect("executor task joins");

        assert_eq!(
            context
                .cancelled_active
                .lock()
                .expect("cancelled active lock")
                .as_slice(),
            ["active"]
        );
        assert_eq!(report.outcomes.len(), 2);
        assert!(report.outcomes.iter().all(|outcome| matches!(
            outcome.terminal,
            GovernedToolTaskTerminal::Cancelled { .. } | GovernedToolTaskTerminal::Blocked { .. }
        )));
    }
}
