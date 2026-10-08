use std::sync::Weak;

use async_trait::async_trait;
use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeResult,
    ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
};
use serde::{Deserialize, Serialize};

use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
    NodeExecutor, NodeExecutorError,
};
use crate::{
    AgentActionService, AgenticProgramProjection, AgenticProgramStatus, AgenticTaskStatus,
};

/// Durable payload for a root graph barrier that waits for autonomous Agent
/// work without spending another provider round. The Program journal remains
/// semantic truth; this payload only binds the exact root that may resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgenticProgramWaitRequest {
    pub program_id: String,
    pub root_execution_id: String,
    /// Durable wall-clock deadline for the collaboration budget. The Program
    /// journal remains semantic truth; this payload only binds the exact root
    /// that may resume and the time by which it must. `None` on legacy
    /// barriers and executor-created waits, which stay event-driven only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at_ms: Option<u64>,
}

impl AgenticProgramWaitRequest {
    /// Resolve an explicit root yield against durable execution truth. Child
    /// activity alone must never suspend unfinished root planning.
    pub(crate) async fn for_active_root(
        program: &crate::AgenticProgramProjection,
        root_execution_id: &str,
        state_store: &ExecutionGraphStateStore,
        wait_requested: bool,
        deadline_at_ms: Option<u64>,
    ) -> Result<Option<Self>, String> {
        if !wait_requested
            || program.status != crate::AgenticProgramStatus::Open
            || program.root_execution_id.as_deref() != Some(root_execution_id)
        {
            return Ok(None);
        }
        let dispatched =
            !active_agentic_child_graphs(root_execution_id, &program.program_id, state_store)
                .await?
                .is_empty();
        Ok(dispatched.then(|| Self {
            program_id: program.program_id.clone(),
            root_execution_id: root_execution_id.to_string(),
            deadline_at_ms,
        }))
    }
}

impl AgenticProgramWaitRequest {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.program_id.trim().is_empty() {
            return Err("Agentic Program wait requires program_id");
        }
        if self.root_execution_id.trim().is_empty() {
            return Err("Agentic Program wait requires root_execution_id");
        }
        Ok(())
    }
}

#[derive(Debug)]
enum WaitDisposition {
    Wait {
        revision: u64,
        active_graphs: Vec<String>,
    },
    Resume {
        revision: u64,
        reason: &'static str,
    },
}

/// Deterministic control-plane barrier for Agent-first Programs.
///
/// It deliberately returns `WaitingExternal` instead of awaiting in-process:
/// the graph driver releases its permit and a durable Program event wakes the
/// same node. No model, timer poll, graph lock, or scheduler slot is retained.
pub struct AgenticProgramWaitExecutor {
    actions: AgentActionService,
    state_store: ExecutionGraphStateStore,
    supervisor: Weak<crate::RuntimeExecutionSupervisor>,
}

impl AgenticProgramWaitExecutor {
    pub const KIND: &'static str = "agentic_program_wait";

    #[must_use]
    pub fn new(
        actions: AgentActionService,
        state_store: ExecutionGraphStateStore,
        supervisor: Weak<crate::RuntimeExecutionSupervisor>,
    ) -> Self {
        Self {
            actions,
            state_store,
            supervisor,
        }
    }

    fn request(
        &self,
        node_id: &str,
        payload_ref: &str,
    ) -> Result<AgenticProgramWaitRequest, NodeExecutorError> {
        let request =
            serde_json::from_str::<AgenticProgramWaitRequest>(payload_ref).map_err(|error| {
                NodeExecutorError::Invalid {
                    node_id: node_id.to_string(),
                    reason: format!("invalid Agentic Program wait request: {error}"),
                }
            })?;
        request
            .validate()
            .map_err(|reason| NodeExecutorError::Invalid {
                node_id: node_id.to_string(),
                reason: reason.to_string(),
            })?;
        Ok(request)
    }
}

#[async_trait]
impl NodeExecutor for AgenticProgramWaitExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn supports_resumable_pause(&self) -> bool {
        false
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.kind != ExecutionNodeKind::Verify || node.executor_kind != Self::KIND {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Agentic Program wait must be a Verify node using its canonical executor"
                    .to_string(),
            });
        }
        self.request(&node.id, &node.payload_ref).map(|_| ())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        let request = self.request(&context.node.id, &context.node.payload_ref)?;
        if request.root_execution_id != context.graph.id {
            return Err(NodeExecutorError::Invalid {
                node_id: context.node.id.clone(),
                reason: "Agentic Program wait cannot redirect its Runtime-owned root binding"
                    .to_string(),
            });
        }
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: Self::KIND.to_string(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let request = self.request(&ticket.node_id, &ticket.payload_ref)?;
        let projection =
            self.actions
                .project(&request.program_id)
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("load Agentic Program wait projection: {error}"),
                })?;
        let disposition = wait_disposition(&projection, &request, &self.state_store)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?;
        let (status, result_ref, summary) = match disposition {
            WaitDisposition::Wait {
                revision,
                active_graphs,
            } => (
                ExecutionNodeStatus::WaitingExternal,
                format!(
                    "agentic-program-wait:{}:revision:{revision}",
                    request.program_id
                ),
                format!(
                    "Agentic Program `{}` revision {revision} is driven by {} active Agent execution graph(s)",
                    request.program_id,
                    active_graphs.len()
                ),
            ),
            WaitDisposition::Resume { revision, reason } => (
                ExecutionNodeStatus::Completed,
                format!(
                    "agentic-program-checkpoint:{}:revision:{revision}",
                    request.program_id
                ),
                format!(
                    "Agentic Program `{}` reached root checkpoint `{reason}` at revision {revision}",
                    request.program_id
                ),
            ),
        };
        let wait_class = (status == ExecutionNodeStatus::WaitingExternal)
            .then_some(harness_contract::execution_graph::ExternalWaitClass::Barrier);
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref: Some(result_ref),
            wait_class,
            summary: Some(summary),
            evidence_refs: Vec::new(),
            failure: None,
            usage: ExecutionUsage::default(),
            finished_at_ms: now_ms(),
        }))
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        // Close the Program-event-before-WaitingExternal race. The resolver
        // rechecks durable Program and child truth; a still-active Program is
        // a no-op, while a fast terminal/checkpoint atomically releases the
        // barrier after its wait transition has committed.
        let request = self.request(&ticket.node_id, &ticket.payload_ref)?;
        let supervisor =
            self.supervisor
                .upgrade()
                .ok_or_else(|| NodeExecutorError::Unavailable {
                    executor_kind: Self::KIND.to_string(),
                    node_id: ticket.node_id.clone(),
                })?;
        resolve_agentic_program_wait(
            &request.program_id,
            &self.actions,
            &self.state_store,
            supervisor.as_ref(),
            true,
        )
        .await
        .map(|_| ())
        .map_err(|reason| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason,
        })?;
        // A parked barrier must not depend on somebody else waking it.
        //
        // It has three external invocation paths - this hook, the graph-settled
        // observer, and a projection lane - and in paid runs all three missed the
        // moment that mattered. One run recorded `wait` at Program revision 38 with
        // two Tasks accepted and one submitted; the final accept and its child graph's
        // `running -> completed` landed in the same second 23 seconds later; and not
        // one resolver pass ran in the following ten minutes, so the evaluator
        // cancelled the barrier and the turn never reached a terminal state. The lane
        // that was supposed to back the others up ran twice in a fourteen-minute run.
        //
        // So the barrier re-arms itself: a bounded number of re-evaluations on a fixed
        // interval, each of which stops the moment the node is no longer parked. The
        // bound is what keeps this from becoming a poller - the no-progress fuse still
        // owns the turn's terminal, and a Program that genuinely cannot settle is
        // still ended honestly rather than spun on.
        let actions = self.actions.clone();
        let state_store = self.state_store.clone();
        let root_execution_id = request.root_execution_id.clone();
        let program_id = request.program_id.clone();
        let node_id = ticket.node_id.clone();
        let supervisor = std::sync::Arc::downgrade(&supervisor);
        tokio::spawn(async move {
            for _ in 0..MAX_BARRIER_SELF_REARMS {
                tokio::time::sleep(BARRIER_SELF_REARM_INTERVAL).await;
                let Some(supervisor) = supervisor.upgrade() else {
                    return;
                };
                let parked = match state_store
                    .load_current_async(root_execution_id.clone())
                    .await
                {
                    Ok(graph) => {
                        graph.node_statuses.get(&node_id)
                            == Some(&ExecutionNodeStatus::WaitingExternal)
                    }
                    Err(_) => return,
                };
                if !parked {
                    return;
                }
                let released = resolve_agentic_program_wait(
                    &program_id,
                    &actions,
                    &state_store,
                    supervisor.as_ref(),
                    true,
                )
                .await
                .unwrap_or(0);
                if released > 0 {
                    return;
                }
            }
        });
        Ok(())
    }
}

/// Bounded self-rearm count for one parked barrier. Three paid runs showed the
/// external wake paths missing the decisive moment, so the barrier re-evaluates
/// itself; the bound keeps that from becoming an unbounded poller.
pub(crate) const MAX_BARRIER_SELF_REARMS: u32 = 120;

/// Interval between a parked barrier's own re-evaluations. Long enough that a
/// settling Program is picked up within a couple of rounds, short enough that the
/// whole budget stays inside one turn.
pub(crate) const BARRIER_SELF_REARM_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(2);

/// Resolve every waiting barrier for one Program when canonical Program or
/// child-graph truth says the root model has a semantic decision to make.
/// `allow_same_revision_quiescence` is used by child-settle/startup recovery;
/// Program-event passes keep it false so replaying the event that originally
/// created the wait cannot spuriously release an active barrier.
/// Release the root's Agentic Program barrier, with one bounded re-read.
///
/// A caller that settles a child graph reads the Program projection and decides; the
/// Program event that settles the *last* Task can be committed after that read, so the
/// pass sees an unsettled Task, correctly waits, and - because nothing else is
/// guaranteed to run again - never revisits it. In three paid runs the barrier was
/// never released for exactly this reason: one run recorded `wait` at revision 38 with
/// two Tasks accepted and one submitted, the final accept landed 23 seconds later, and
/// the next resolver pass was 10 minutes after that - one second after the evaluator
/// had already cancelled the barrier.
///
/// The re-read compares against the durable Program head, so it introduces no new
/// signal source, and it runs at most once per call.
pub async fn resolve_agentic_program_wait(
    program_id: &str,
    actions: &AgentActionService,
    state_store: &ExecutionGraphStateStore,
    supervisor: &crate::RuntimeExecutionSupervisor,
    allow_same_revision_quiescence: bool,
) -> Result<usize, String> {
    let first = resolve_wait_once(
        program_id,
        actions,
        state_store,
        supervisor,
        allow_same_revision_quiescence,
    )
    .await?;
    if first > 0 {
        return Ok(first);
    }
    let head = actions.program_revision(program_id).unwrap_or(0);
    let read = actions.project(program_id).map_err(|error| error.to_string())?;
    if head <= read.revision {
        return Ok(0);
    }
    resolve_wait_once(
        program_id,
        actions,
        state_store,
        supervisor,
        allow_same_revision_quiescence,
    )
    .await
}

/// One durable line saying what the barrier resolver decided and what it read. See
/// `AgentActionService::record_wait_disposition` for why this exists.
fn record(
    actions: &crate::AgentActionService,
    projection: &crate::AgenticProgramProjection,
    program_id: &str,
    root_execution_id: &str,
    disposition: &'static str,
    reason: Option<&'static str>,
    barrier_candidates: usize,
    observed_revision: u64,
    released: usize,
    barrier_rejections: &serde_json::Value,
) {
    if projection.program_id != program_id
        || projection.root_execution_id.as_deref() != Some(root_execution_id)
    {
        return;
    }
    actions.record_wait_disposition(crate::agentic::AgenticWaitDisposition {
        program_id,
        root_execution_id,
        revision: projection.revision,
        disposition,
        reason,
        all_tasks_accepted: all_tasks_accepted(projection),
        task_statuses: projection
            .tasks
            .iter()
            .map(|(task_ref, task)| (task_ref.clone(), format!("{:?}", task.status)))
            .collect(),
        barrier_candidates,
        released,
        observed_revision,
        barrier_rejections: barrier_rejections.clone(),
    });
}

async fn resolve_wait_once(
    program_id: &str,
    actions: &AgentActionService,
    state_store: &ExecutionGraphStateStore,
    supervisor: &crate::RuntimeExecutionSupervisor,
    allow_same_revision_quiescence: bool,
) -> Result<usize, String> {
    let projection = actions
        .project(program_id)
        .map_err(|error| error.to_string())?;
    let Some(root_execution_id) = projection.root_execution_id.as_deref() else {
        return Ok(0);
    };
    let deadline_at_ms =
        stored_barrier_deadline(state_store, root_execution_id, program_id).await?;
    let request = AgenticProgramWaitRequest {
        program_id: program_id.to_string(),
        root_execution_id: root_execution_id.to_string(),
        deadline_at_ms,
    };
    let disposition = wait_disposition(&projection, &request, state_store).await?;
    let (revision, reason, disposition) = match disposition {
        WaitDisposition::Wait { .. } => (projection.revision, None, "wait"),
        WaitDisposition::Resume { revision, reason } => (revision, Some(reason), "resume"),
    };
    // A strand resume hands the root one chance to replan, withdraw, or retire the
    // unreachable Task. The root graph's own barrier nodes are the durable record
    // of how many chances it has already had: each released barrier keeps its
    // `agentic-program-checkpoint:` result, so no new state is needed to bound it.
    let strand_resumes = if reason == Some("task_stranded") {
        strand_resume_count(state_store, root_execution_id, program_id).await?
    } else {
        0
    };
    let strand_exhausted = strand_resumes >= MAX_TASK_STRAND_RESUMES;
    // One bounded re-read. A caller that settles a child graph reads the Program
    // projection and decides; the Program event that settles the *last* Task can be
    // committed after that read, so the pass sees an unsettled Task, correctly waits,
    // and - because nothing else is guaranteed to run again - never revisits it. In
    // three paid runs the barrier was never released for exactly this reason: one run
    // recorded `wait` at revision 38 with two Tasks accepted and one submitted, the
    // final accept landed 23 seconds later, and the next resolver pass was 10 minutes
    // after that - one second after the evaluator had cancelled the barrier.
    //
    // The re-read compares against the durable Program head, so it needs no new
    // signal source, and it runs at most once per call.
    let mut resolved = 0usize;
    let mut barrier_candidates = 0usize;
    let mut barrier_rejections = serde_json::Value::Null;
    let mut observed_revision = 0u64;
    let mut barrier_rejections = serde_json::Value::Null;
    const MAX_GRAPH_CAS_ATTEMPTS: usize = 8;
    if strand_exhausted {
        record(actions, &projection, program_id, root_execution_id, disposition, reason, 0, 0, 0, &serde_json::Value::Null);
        return Ok(0);
    }
    if disposition == "wait" {
        record(actions, &projection, program_id, root_execution_id, disposition, reason, 0, 0, 0, &serde_json::Value::Null);
        return Ok(0);
    }
    for attempt in 0..MAX_GRAPH_CAS_ATTEMPTS {
        let graph = match state_store
            .load_current_async(root_execution_id.to_string())
            .await
        {
            Ok(graph) => graph,
            Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. }) => {
                return Ok(resolved);
            }
            Err(error) => return Err(error.to_string()),
        };
        let candidate = graph.nodes.iter().find(|node| {
            node.kind == ExecutionNodeKind::Verify
                && node.executor_kind == AgenticProgramWaitExecutor::KIND
                && graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::WaitingExternal)
                && serde_json::from_str::<AgenticProgramWaitRequest>(&node.payload_ref).is_ok_and(
                    |candidate| {
                        candidate.program_id == request.program_id
                            && candidate.root_execution_id == request.root_execution_id
                    },
                )
        });
        let Some(node) = candidate else {
            // Name which condition rejected the barrier, instead of leaving a zero
            // count to be inferred. In one live run this count was zero in every pass
            // of every run while the durable stream showed the node reaching
            // `waiting_external`, so exactly one of the four conditions below was
            // false and nothing said which.
            barrier_rejections = serde_json::json!({
                "loaded_graph_revision": graph.revision,
                "loaded_graph_nodes": graph.nodes.len(),
                "loaded_graph_kinds": graph
                    .nodes
                    .iter()
                    .map(|node| format!("{:?}:{}", node.kind, node.executor_kind))
                    .collect::<Vec<_>>(),
                "rejections": graph
                    .nodes
                    .iter()
                    .filter(|node| node.executor_kind == AgenticProgramWaitExecutor::KIND)
                .map(|node| {
                    let status = graph
                        .node_statuses
                        .get(&node.id)
                        .map(|status| format!("{status:?}"))
                        .unwrap_or_else(|| "absent".to_string());
                    let payload_matches =
                        serde_json::from_str::<AgenticProgramWaitRequest>(&node.payload_ref)
                            .is_ok_and(|candidate| {
                                candidate.program_id == request.program_id
                                    && candidate.root_execution_id == request.root_execution_id
                            });
                    serde_json::json!({
                        "node_id": node.id,
                        "kind": format!("{:?}", node.kind),
                        "status": status,
                        "payload_matches": payload_matches,
                        "is_verify": node.kind == ExecutionNodeKind::Verify,
                    })
                })
                .collect::<Vec<_>>(),
            });
            break;
        };
        barrier_candidates = barrier_candidates.saturating_add(1);
        observed_revision = graph
            .node_results
            .get(&node.id)
            .and_then(|result| result.result_ref.as_deref())
            .and_then(wait_result_revision)
            .unwrap_or_default();
        // The same-revision guard exists to stop a replayed Program event from
        // re-releasing an already-resolved barrier. It must not defeat a
        // `worker_quiescent` resume: when no child graph is active the barrier
        // is genuinely releasable even if the Program revision never advanced,
        // otherwise the root stays parked forever (G56/G57 root non-finalization).
        let quiescent_resume = reason == Some("worker_quiescent");
        // A budget resume is equally real even when the Program revision never
        // advanced: the durable deadline is the release condition.
        let budget_resume = reason == Some("collaboration_budget");
        if !allow_same_revision_quiescence
            && !quiescent_resume
            && !budget_resume
            && projection.status == AgenticProgramStatus::Open
            && !all_tasks_accepted(&projection)
            && revision <= observed_revision
        {
            break;
        }
        // The correlation must reproduce the durable subject the node parked on, which is
        // the parked revision, not the revision the resolver is resolving at. The commit
        // gate (PLAN W1 / C4) anchors a resolution on the parked subject, so a correlation
        // carrying a later revision is refused as unanchored - meaning a barrier could only
        // ever be released at the exact revision it parked on. In one live run the barrier
        // parked at Program revision 17 and the Program later reached 30, so every genuine
        // release was rejected and the root stayed parked until the fuse ended the turn.
        // The resolution's own revision is still recorded in `result_ref` below.
        let correlation_id = format!(
            "agentic-program-wait:{}:{}:{}",
            program_id, node.id, observed_revision
        );
        match supervisor
            .command(
                root_execution_id,
                ExecutionGraphCommand::ResolveExternal {
                    expected_revision: graph.revision,
                    node_id: node.id.clone(),
                    result_ref: format!(
                        "agentic-program-checkpoint:{program_id}:revision:{revision}:{}",
                        reason.unwrap_or("unspecified")
                    ),
                    correlation_id,
                },
            )
            .await
        {
            Ok(_) => resolved = resolved.saturating_add(1),
            Err(crate::execution_core::graph::ExecutionRunnerError::Commit(
                crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
            )) if attempt + 1 < MAX_GRAPH_CAS_ATTEMPTS => continue,
            Err(crate::execution_core::graph::ExecutionRunnerError::Commit(
                crate::execution_core::graph::ExecutionCommitError::StaleRevision { .. },
            )) => {
                return Err(format!(
                    "agentic_program_wait_conflict_exhausted:{root_execution_id}"
                ));
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    if resolved > 0 {
        supervisor
            .notify_graph(root_execution_id)
            .await
            .map_err(|error| error.to_string())?;
    }
    record(
        actions,
        &projection,
        program_id,
        root_execution_id,
        disposition,
        reason,
        barrier_candidates,
        observed_revision,
        resolved,
        &barrier_rejections,
    );
    Ok(resolved)
}

/// Child-settle hook. It derives Program identity from the immutable
/// AgentTask packet rather than graph labels, then applies the same root
/// checkpoint predicate used by Program events and startup recovery.
pub async fn reconcile_agentic_program_wait_for_settled_graph(
    graph_id: &str,
    actions: &AgentActionService,
    state_store: &ExecutionGraphStateStore,
    supervisor: &crate::RuntimeExecutionSupervisor,
) -> Result<usize, String> {
    let graph = match state_store.load_current_async(graph_id.to_string()).await {
        Ok(graph) => graph,
        Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. }) => return Ok(0),
        Err(error) => return Err(error.to_string()),
    };
    let mut program_ids = agentic_program_ids(&graph);
    program_ids.sort();
    program_ids.dedup();
    // Record what this settle produced before deciding anything. In one paid run the
    // final Task accept and its child graph's `running -> completed` landed in the
    // same second, yet not one resolver pass ran afterwards - so either the observer
    // never fired, or it fired and this function found no Program to reconcile. Both
    // look identical from the resolver's side; this event separates them.
    let mut resolved = 0usize;
    for program_id in program_ids {
        resolved = resolved.saturating_add(
            resolve_agentic_program_wait(&program_id, actions, state_store, supervisor, true)
                .await?,
        );
    }
    Ok(resolved)
}

async fn stored_barrier_deadline(
    state_store: &ExecutionGraphStateStore,
    root_execution_id: &str,
    program_id: &str,
) -> Result<Option<u64>, String> {
    // The parked barrier payload is the durable carrier of the collaboration
    // deadline; a freshly built request has no deadline of its own.
    let graph = state_store
        .load_current_async(root_execution_id.to_string())
        .await;
    let Ok(graph) = graph else {
        return Ok(None);
    };
    let deadline = graph
        .nodes
        .iter()
        .find(|node| {
            node.kind == ExecutionNodeKind::Verify
                && node.executor_kind == AgenticProgramWaitExecutor::KIND
                && graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::WaitingExternal)
        })
        .and_then(|node| {
            serde_json::from_str::<AgenticProgramWaitRequest>(&node.payload_ref).ok()
        })
        .filter(|request| {
            request.program_id == program_id && request.root_execution_id == root_execution_id
        })
        .and_then(|request| request.deadline_at_ms);
    Ok(deadline)
}

async fn wait_disposition(
    projection: &AgenticProgramProjection,
    request: &AgenticProgramWaitRequest,
    state_store: &ExecutionGraphStateStore,
) -> Result<WaitDisposition, String> {
    if projection.program_id != request.program_id
        || projection.root_execution_id.as_deref() != Some(request.root_execution_id.as_str())
    {
        return Err(
            "Agentic Program wait binding does not match canonical Program truth".to_string(),
        );
    }
    if projection.status == AgenticProgramStatus::Verified {
        return Ok(WaitDisposition::Resume {
            revision: projection.revision,
            reason: "verified",
        });
    }
    if projection.status == AgenticProgramStatus::Blocked {
        return Ok(WaitDisposition::Resume {
            revision: projection.revision,
            reason: "blocked",
        });
    }
    if projection.status == AgenticProgramStatus::CompletionRequested {
        return Ok(WaitDisposition::Wait {
            revision: projection.revision,
            active_graphs: Vec::new(),
        });
    }
    // The collaboration budget is a durable Program-bound deadline, not an
    // actor-local limit. When it elapses the parked root must wake so its own
    // honest terminal path can close the turn in-bound.
    if request
        .deadline_at_ms
        .is_some_and(|deadline| now_ms() >= deadline)
    {
        return Ok(WaitDisposition::Resume {
            revision: projection.revision,
            reason: "collaboration_budget",
        });
    }
    if all_tasks_accepted(projection) {
        return Ok(WaitDisposition::Resume {
            revision: projection.revision,
            reason: "all_tasks_accepted",
        });
    }
    let active_graphs =
        active_agentic_child_graphs(&request.root_execution_id, &request.program_id, state_store)
            .await?;
    if active_graphs.is_empty() {
        // "No active child graph" is not the same as "every dispatched task
        // settled": a child link may not be durable yet. Never read a missing
        // link as completion while a task is still live (PLAN C5-6).
        if has_unsettled_tasks(projection) {
            // One bounded exception, and only for a Task Runtime has already
            // declared undispatched past its own bound. Parking on it forever is
            // the starvation the fuse later reports, and the root can still
            // replan, withdraw, or retire it.
            return if crate::agentic::execution::dispatch::stranded_task_refs(projection)
                .is_empty()
            {
                Ok(WaitDisposition::Wait {
                    revision: projection.revision,
                    active_graphs,
                })
            } else {
                Ok(WaitDisposition::Resume {
                    revision: projection.revision,
                    reason: "task_stranded",
                })
            };
        }
        return Ok(WaitDisposition::Resume {
            revision: projection.revision,
            reason: "worker_quiescent",
        });
    }
    Ok(WaitDisposition::Wait {
        revision: projection.revision,
        active_graphs,
    })
}

/// Bounded strand resumes per (root, Program). Past the bound the barrier goes back
/// to waiting and the no-progress fuse produces the honest partial, which is the
/// correct answer once the root has already been offered the chance and did not
/// take it.
pub(crate) const MAX_TASK_STRAND_RESUMES: usize = 2;

/// How many times this root's barriers have already been released for a stranded
/// Task. Every released barrier keeps its durable `agentic-program-checkpoint`
/// result, so the count is read from the root graph itself.
async fn strand_resume_count(
    state_store: &ExecutionGraphStateStore,
    root_execution_id: &str,
    program_id: &str,
) -> Result<usize, String> {
    let graph = state_store
        .load_current_async(root_execution_id.to_string())
        .await
        .map_err(|error| error.to_string())?;
    Ok(graph
        .nodes
        .iter()
        .filter(|node| node.executor_kind == AgenticProgramWaitExecutor::KIND)
        .filter_map(|node| graph.node_results.get(&node.id))
        .filter_map(|result| result.result_ref.as_deref())
        .filter(|result_ref| {
            result_ref.starts_with(&format!("agentic-program-checkpoint:{program_id}:revision:"))
                && result_ref.ends_with(":task_stranded")
        })
        .count())
}

fn all_tasks_accepted(projection: &AgenticProgramProjection) -> bool {
    !projection.tasks.is_empty()
        && projection
            .tasks
            .values()
            .all(|task| task.status == AgenticTaskStatus::Accepted)
}



/// Tasks that were dispatched into the Program but have not settled yet.
///
/// `Published`/`Claimed`/`Submitted`/`Rework`/`CancelRequested` are all live:
/// the owning Agent may still be working, or its child graph link may simply
/// not be durable yet. Treating "no active child graph" as completion while any
/// of these exist is exactly the missing-link-reads-as-done bug (PLAN C5-6).
fn has_unsettled_tasks(projection: &AgenticProgramProjection) -> bool {
    projection.tasks.values().any(|task| {
        matches!(
            task.status,
            AgenticTaskStatus::Published
                | AgenticTaskStatus::Claimed
                | AgenticTaskStatus::Submitted
                | AgenticTaskStatus::Rework
                | AgenticTaskStatus::CancelRequested
        )
    })
}

async fn active_agentic_child_graphs(
    root_execution_id: &str,
    program_id: &str,
    state_store: &ExecutionGraphStateStore,
) -> Result<Vec<String>, String> {
    let links = state_store
        .child_links_async(root_execution_id.to_string())
        .await
        .map_err(|error| error.to_string())?;
    let mut active = Vec::new();
    for link in links {
        let graph = match state_store
            .load_current_async(link.child_execution_id.clone())
            .await
        {
            Ok(graph) => graph,
            Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        };
        if graph_belongs_to_program(&graph, program_id)
            && graph
                .node_statuses
                .values()
                .any(|status| !status.is_terminal())
        {
            active.push(graph.id);
        }
    }
    active.sort();
    active.dedup();
    Ok(active)
}

fn graph_belongs_to_program(graph: &ExecutionGraph, program_id: &str) -> bool {
    agentic_program_ids(graph)
        .iter()
        .any(|candidate| candidate == program_id)
}

fn agentic_program_ids(graph: &ExecutionGraph) -> Vec<String> {
    graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .flat_map(|node| {
            let mut ids = node
                .resource_scopes
                .iter()
                .filter_map(|scope| scope.strip_prefix("program:").map(str::to_string))
                .collect::<Vec<_>>();
            if let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref) {
                ids.extend(packet.context_refs.into_iter().filter_map(|reference| {
                    reference
                        .strip_prefix("agentic_program:")
                        .map(str::to_string)
                }));
            }
            ids
        })
        .collect()
}

fn wait_result_revision(result_ref: &str) -> Option<u64> {
    result_ref
        .rsplit_once(":revision:")
        .and_then(|(_, revision)| revision.parse().ok())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, AgentInviteInput,
        TaskClaimInput, TaskPublishInput, TeamCreateInput,
    };
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage, ExecutionNodeKind,
        ExecutionNodeSpec, ExecutionNodeStatus, ExecutionParentBinding,
    };
    use harness_contract::policy::PermissionMode;
    use serde_json::Value;

    use super::{
        reconcile_agentic_program_wait_for_settled_graph, resolve_agentic_program_wait,
        AgenticProgramWaitExecutor, AgenticProgramWaitRequest,
    };
    use crate::execution_core::graph::executors::AgentTaskExecutor;
    use crate::RuntimeServices;

    fn root_actor(program_id: &str, root_execution_id: &str) -> AgentActorBinding {
        AgentActorBinding {
            objective_id: format!("objective:{program_id}"),
            program_id: program_id.to_string(),
            session_id: format!("session:{program_id}"),
            turn_id: format!("turn:{program_id}"),
            root_execution_id: Some(root_execution_id.to_string()),
            required_team_count: 1,
            exact_team_count: None,
            objective_summary: "wait for autonomous Agent work".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: format!("root:{program_id}"),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        }
    }

    fn open_program(services: &RuntimeServices, program_id: &str, root_execution_id: &str) {
        let observation = services
            .agent_action_service()
            .apply(&AgentActionEnvelope {
                action_id: format!("open:{program_id}"),
                actor: root_actor(program_id, root_execution_id),
                expected_revision: None,
                action: AgentAction::TeamCreate(TeamCreateInput {
                    name: "Evidence Team".to_string(),
                    mission: "perform autonomous work".to_string(),
                    objective: None,
                }),
            })
            .expect("open Program");
        assert_eq!(
            observation.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
    }

    #[tokio::test]
    async fn root_can_publish_dependencies_while_child_runs_and_yield_explicitly() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-publish-before-invite";
        let root_execution_id = "root-publish-before-invite";
        let root_parent_node = register_root_wait(&services, root_execution_id, program_id);
        let root = root_actor(program_id, root_execution_id);
        let apply_root = |action_id: &str, action: AgentAction| {
            services
                .agent_action_service()
                .apply(&AgentActionEnvelope {
                    action_id: action_id.to_string(),
                    actor: root.clone(),
                    expected_revision: None,
                    action,
                })
                .expect("apply root Agent action")
        };
        let team_ref = apply_root(
            "empty-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Late staffed Team".to_string(),
                mission: "execute work published before staffing".to_string(),
                objective: None,
            }),
        )
        .changed_refs[0]
            .clone();
        let first_task_ref = apply_root(
            "publish-before-staffing",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team_ref.clone(),
                title: "Delayed dispatch".to_string(),
                objective: "prove post-action wait eligibility".to_string(),
                acceptance: "the claimed child eventually submits evidence".to_string(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        )
        .changed_refs[0]
            .clone();
        let published = services
            .agent_action_service()
            .project(program_id)
            .expect("published projection");
        assert!(AgenticProgramWaitRequest::for_active_root(
            &published,
            root_execution_id,
            services.graph_state_store(),
            true,
            None,
        )
        .await
        .expect("derive pre-dispatch wait")
        .is_none());

        apply_root(
            "staff-after-publish",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_ref.clone(),
                role: "Executor".to_string(),
                mission: "claim the already-published work".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        );
        register_agent_child(
            &services,
            root_execution_id,
            &root_parent_node,
            program_id,
            "agent-child-after-invite",
        );

        let dispatched = services
            .agent_action_service()
            .project(program_id)
            .expect("dispatched projection");
        assert!(AgenticProgramWaitRequest::for_active_root(
            &dispatched,
            root_execution_id,
            services.graph_state_store(),
            false,
            None,
        )
        .await
        .expect("continue publishing while a child is active")
        .is_none());
        let downstream = apply_root(
            "publish-dependent-work-while-child-runs",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team_ref.clone(),
                title: "Consume the first result".into(),
                objective: "plan downstream without waiting for the first execution".into(),
                acceptance: "consume predecessor evidence".into(),
                acceptance_checks: Vec::new(),
                required_capabilities: Vec::new(),
                depends_on: vec![first_task_ref.clone()],
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        );
        assert_eq!(
            downstream.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let planned = services
            .agent_action_service()
            .project(program_id)
            .expect("expanded plan");
        assert_eq!(
            planned.tasks[&downstream.changed_refs[0]].depends_on,
            vec![first_task_ref]
        );
        assert_eq!(
            AgenticProgramWaitRequest::for_active_root(
                &dispatched,
                root_execution_id,
                services.graph_state_store(),
                true,
                None,
            )
            .await
            .expect("derive dispatched wait"),
            Some(AgenticProgramWaitRequest {
                program_id: program_id.to_string(),
                root_execution_id: root_execution_id.to_string(),
                deadline_at_ms: None,
            })
        );
    }

    fn register_root_wait(
        services: &RuntimeServices,
        root_execution_id: &str,
        program_id: &str,
    ) -> String {
        let mut graph = ExecutionGraph::new("root Program wait");
        graph.id = root_execution_id.to_string();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: format!("session:{program_id}"),
            turn_id: format!("turn:{program_id}"),
            root_task_id: format!("task:{program_id}"),
            task_id: format!("task:{program_id}"),
            generation: 1,
        });
        let mut wait = ExecutionNodeSpec::new(
            ExecutionNodeKind::Verify,
            AgenticProgramWaitExecutor::KIND,
            serde_json::to_string(&AgenticProgramWaitRequest {
                program_id: program_id.to_string(),
                root_execution_id: root_execution_id.to_string(),
                deadline_at_ms: None,
            })
            .expect("wait request"),
        );
        wait.id = format!("{root_execution_id}:program-wait");
        wait.idempotency_key = format!("{root_execution_id}:program-wait:attempt");
        let node_id = wait.id.clone();
        graph.nodes.push(wait);
        services
            .commit_service()
            .register_graph(graph)
            .expect("register root wait");
        node_id
    }

    fn register_agent_child(
        services: &RuntimeServices,
        root_execution_id: &str,
        parent_node_id: &str,
        program_id: &str,
        child_id: &str,
    ) {
        let mut graph = ExecutionGraph::new("Agentic child");
        graph.id = child_id.to_string();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: format!("session:{program_id}"),
            turn_id: format!("turn:{program_id}"),
            root_task_id: format!("task:{program_id}"),
            task_id: format!("task:{program_id}:{child_id}"),
            generation: 1,
        });
        graph.parent_execution = Some(ExecutionParentBinding {
            execution_id: root_execution_id.to_string(),
            node_id: parent_node_id.to_string(),
        });
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            AgentTaskExecutor::KIND,
            "test-only-unbound-packet",
        );
        node.id = format!("{child_id}:agent");
        node.idempotency_key = format!("{child_id}:agent:attempt");
        node.resource_scopes.push(format!("program:{program_id}"));
        graph.nodes.push(node);
        services
            .commit_service()
            .register_graph(graph)
            .expect("register Agent child");
    }

    async fn cancel_graph(services: &RuntimeServices, graph_id: &str) {
        let graph = services
            .graph_state_store()
            .load_async(graph_id.to_string())
            .await
            .expect("load child");
        services
            .commit_service()
            .apply_command_async(
                graph.clone(),
                ExecutionGraphCommand::Cancel {
                    expected_revision: graph.revision,
                    reason: "fixture settled".to_string(),
                },
            )
            .await
            .expect("settle child");
    }

    #[tokio::test]
    async fn root_wait_quiesces_until_every_agentic_child_settles() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-wait-fan-in";
        let root_id = "root-wait-fan-in";
        let wait_node = register_root_wait(&services, root_id, program_id);
        open_program(&services, program_id, root_id);
        register_agent_child(
            &services,
            root_id,
            &wait_node,
            program_id,
            "agent-child-left",
        );
        register_agent_child(
            &services,
            root_id,
            &wait_node,
            program_id,
            "agent-child-right",
        );

        let (_, report) = services
            .execution_supervisor()
            .drive_registered(root_id)
            .await
            .expect("drive wait root");
        assert_eq!(report.waiting, 1);
        assert_eq!(
            services
                .graph_state_store()
                .load(root_id)
                .expect("root")
                .node_statuses[&wait_node],
            ExecutionNodeStatus::WaitingExternal
        );

        cancel_graph(&services, "agent-child-left").await;
        assert_eq!(
            reconcile_agentic_program_wait_for_settled_graph(
                "agent-child-left",
                &services.agent_action_service(),
                services.graph_state_store(),
                services.execution_supervisor().as_ref(),
            )
            .await
            .expect("first settle"),
            0,
            "one terminal child must not release a fan-in barrier"
        );
        assert_eq!(
            services
                .graph_state_store()
                .load(root_id)
                .expect("root")
                .node_statuses[&wait_node],
            ExecutionNodeStatus::WaitingExternal
        );

        cancel_graph(&services, "agent-child-right").await;
        // The settled observer and this explicit recovery pass may race. The
        // CAS resolver permits exactly one of them to release the barrier;
        // both outcomes must converge on the same terminal root.
        let resolved = reconcile_agentic_program_wait_for_settled_graph(
            "agent-child-right",
            &services.agent_action_service(),
            services.graph_state_store(),
            services.execution_supervisor().as_ref(),
        )
        .await
        .expect("last settle");
        assert!(
            resolved <= 1,
            "one root barrier may be released at most once"
        );
        let terminal = services
            .execution_supervisor()
            .wait_for_terminal(root_id)
            .await
            .expect("root terminal");
        assert_eq!(terminal.completed, 1);
        assert_eq!(terminal.waiting, 0);
    }

    #[tokio::test]
    async fn startup_reconciliation_releases_a_durable_quiescent_program_wait() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-wait-startup";
        let root_id = "root-wait-startup";
        let wait_node = register_root_wait(&services, root_id, program_id);
        open_program(&services, program_id, root_id);
        register_agent_child(
            &services,
            root_id,
            &wait_node,
            program_id,
            "agent-child-startup",
        );
        services
            .execution_supervisor()
            .drive_registered(root_id)
            .await
            .expect("drive wait root");
        cancel_graph(&services, "agent-child-startup").await;

        // The live settled-event observer and the explicit startup scan share
        // the same CAS release path. In an in-process test the observer may
        // win before the recovery call; a real restart has no live observer.
        // Either owner may perform the one release, but they must converge on
        // the same terminal root without duplicate completion.
        let recovered = services
            .recover_agentic_program_waits_on_startup()
            .await
            .expect("startup Program wait recovery");
        assert!(
            recovered <= 1,
            "one Program wait may be released at most once"
        );
        assert_eq!(
            services
                .execution_supervisor()
                .wait_for_terminal(root_id)
                .await
                .expect("root terminal")
                .completed,
            1
        );
    }

    #[tokio::test]
    async fn durable_child_transition_event_wakes_a_quiescent_root_without_polling() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-wait-event";
        let root_id = "root-wait-event";
        let wait_node = register_root_wait(&services, root_id, program_id);
        open_program(&services, program_id, root_id);
        register_agent_child(
            &services,
            root_id,
            &wait_node,
            program_id,
            "agent-child-event",
        );
        services
            .execution_supervisor()
            .drive_registered(root_id)
            .await
            .expect("drive wait root");

        // Settle without invoking the graph-settled observer. The durable
        // execution-node transition must wake the root through the projection
        // lane; no Program poll or process-local callback is allowed here.
        cancel_graph(&services, "agent-child-event").await;

        let terminal = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            services
                .execution_supervisor()
                .wait_for_terminal(root_id),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "child transition must wake the root without a safety-tick poll: {error:?}; root={:?}; reactor={:?}",
                services.graph_state_store().load(root_id),
                services.event_reactor_health(),
            )
        })
        .expect("root terminal");
        assert_eq!(terminal.completed, 1);
        assert_eq!(terminal.waiting, 0);
    }

    /// Run #5 parked the root barrier at Program revision 17 with three active child
    /// graphs. Every later Program event settled one more Task, and the last event
    /// accepted the final one - yet no `ResolveExternal` command was ever recorded for
    /// the barrier, and the turn was ended by the no-progress fuse 30 checks later.
    /// The barrier must be releasable against the Program truth the settling event
    /// produces, not only against the state that existed before it.
    #[tokio::test]
    async fn the_event_that_settles_the_last_task_releases_the_parked_barrier() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-wait-last-settle";
        let root_id = "root-wait-last-settle";
        let wait_node = register_root_wait(&services, root_id, program_id);
        open_program(&services, program_id, root_id);
        let actions = services.agent_action_service();
        let root = root_actor(program_id, root_id);
        let apply = |action_id: &str,
                     actor: &AgentActorBinding,
                     action: AgentAction| {
            actions
                .apply(&AgentActionEnvelope {
                    action_id: action_id.to_string(),
                    actor: actor.clone(),
                    expected_revision: None,
                    action,
                })
                .expect("apply Program action")
        };
        let team = apply("settle-team", &root, AgentAction::TeamCreate(TeamCreateInput {
            name: "Evidence".into(),
            mission: "produce and verify one fact".into(),
            objective: None,
        }));
        let team = apply("settle-team", &root, AgentAction::TeamCreate(TeamCreateInput {
            name: "Evidence".into(),
            mission: "produce and verify one fact".into(),
            objective: None,
        }))
        .changed_refs[0]
        .clone();
        let invite = |role: &str, mission: &str| {
            apply(
                &format!("settle-invite-{role}"),
                &root,
                AgentAction::AgentInvite(
                    serde_json::from_value(serde_json::json!({
                        "team_ref": team,
                        "role": role,
                        "mission": mission,
                        "required_capabilities": ["read"]
                    }))
                    .unwrap(),
                ),
            )
            .changed_refs[0]
            .clone()
        };
        let executor = invite("Executor", "produce one verifiable fact");
        let reviewer = invite("Reviewer", "verify the fact independently");
        let agent_actor = {
            let mut actor = root.clone();
            actor.kind = AgentActorKind::Agent;
            actor.actor_id = executor.clone();
            actor.team_id = Some(team.clone());
            actor.agent_id = Some(executor.clone());
            actor.execution_id = Some("agent-child-last-settle".to_string());
            actor
        };
        let reviewer_actor = {
            let mut actor = root.clone();
            actor.kind = AgentActorKind::Agent;
            actor.actor_id = reviewer.clone();
            actor.team_id = Some(team.clone());
            actor.agent_id = Some(reviewer.clone());
            actor.execution_id = Some("agent-child-last-settle-review".to_string());
            actor
        };
        let task = apply("settle-task", &root, AgentAction::TaskPublish(
            serde_json::from_value(serde_json::json!({
                "team_ref": team,
                "title": "One fact",
                "objective": "extract one verifiable fact",
                "acceptance": "artifact plus evidence",
                "required_capabilities": ["read"]
            }))
            .unwrap(),
        ))
        .changed_refs[0]
        .clone();
        register_agent_child(&services, root_id, &wait_node, program_id, "agent-child-last-settle");
        services
            .execution_supervisor()
            .drive_registered(root_id)
            .await
            .expect("drive wait root");
        assert_eq!(
            services
                .graph_state_store()
                .load(root_id)
                .expect("root")
                .node_statuses[&wait_node],
            ExecutionNodeStatus::WaitingExternal,
            "the barrier parks while the Task is unsettled"
        );

        apply("settle-claim", &agent_actor, AgentAction::TaskClaim(
            serde_json::from_value(serde_json::json!({"task_ref": task})).unwrap(),
        ));
        let content = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/markdown".to_string(),
                    visibility_scope: format!("session:{program_id}"),
                    expected_bytes: None,
                    original_name: Some("fact.md".to_string()),
                },
                b"one verifiable fact",
            )
            .await
            .expect("artifact content");
        let artifact = apply("settle-artifact", &agent_actor, AgentAction::ArtifactCommit(
            serde_json::from_value(serde_json::json!({
                "content_ref": content.selector,
                "kind": "report",
                "title": "One fact",
                "relates_to": [task]
            }))
            .unwrap(),
        ))
        .changed_refs[0]
        .clone();
        apply("settle-submit", &agent_actor, AgentAction::TaskSubmit(
            serde_json::from_value(serde_json::json!({
                "task_ref": task,
                "artifact_refs": [artifact],
                "evidence_refs": ["tool://settle-observation"]
            }))
            .unwrap(),
        ));
        // The last Program event is the one that settles the Task. It must be read as
        // the state it produces, otherwise the barrier is evaluated against a Program
        // that still has live work and the root stays parked forever.
        let review = apply("settle-review", &reviewer_actor, AgentAction::TaskReview(
            serde_json::from_value(serde_json::json!({
                "task_ref": task,
                "decision": "accept",
                "reason": "verified",
                "evidence_refs": ["tool://settle-review"]
            }))
            .unwrap(),
        ));
        assert_eq!(review.status, harness_contract::agent_action::AgentActionStatus::Applied);

        let resolved = resolve_agentic_program_wait(
            program_id,
            &services.agent_action_service(),
            services.graph_state_store(),
            services.execution_supervisor().as_ref(),
            true,
        )
        .await
        .expect("resolve barrier");
        assert_eq!(
            resolved, 1,
            "the event that settles the last Task must release the parked barrier"
        );
    }

    /// PLAN C5-6 must keep refusing to read a missing child link as completion, so a
    /// Task with no live child graph still parks the root. But there is a second,
    /// distinguishable state: Runtime's own dispatch machinery has already exhausted
    /// its bound for the Task, which only happens after repeated "no free eligible
    /// member" deferrals. Such a Task is not late, it is unreachable, and parking on
    /// it forever is exactly the starvation run #6 hit - the root had no ready node,
    /// no Agent would ever review the re-submitted Task, and the no-progress fuse had
    /// to end the turn.
    #[tokio::test]
    async fn a_task_runtime_cannot_dispatch_anymore_is_offered_to_the_root_once_per_revision() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-wait-stranded";
        let root_id = "root-wait-stranded";
        let wait_node = register_root_wait(&services, root_id, program_id);
        open_program(&services, program_id, root_id);
        let actions = services.agent_action_service();
        let root = root_actor(program_id, root_id);
        let team_ref = actions
            .project(program_id)
            .expect("opened projection")
            .teams
            .keys()
            .next()
            .cloned()
            .expect("opened Program has a Team");
        // Exactly one Agent, so the roster is never empty but this one member is
        // permanently busy: it is the claimant of the other Task.
        let worker = actions
            .apply(&AgentActionEnvelope {
                action_id: "stranded-invite".to_string(),
                actor: root.clone(),
                expected_revision: None,
                action: AgentAction::AgentInvite(
                    serde_json::from_value(serde_json::json!({
                        "team_ref": team_ref,
                        "role": "Worker",
                        "mission": "do the work",
                        "required_capabilities": ["read"]
                    }))
                    .unwrap(),
                ),
            })
            .expect("invite Agent")
            .changed_refs[0]
            .clone();
        let publish = |action_id: &str, title: &str| {
            actions
                .apply(&AgentActionEnvelope {
                    action_id: action_id.to_string(),
                    actor: root.clone(),
                    expected_revision: None,
                    action: AgentAction::TaskPublish(TaskPublishInput {
                        team_ref: team_ref.clone(),
                        title: title.to_string(),
                        objective: "prove post-action wait eligibility".to_string(),
                        acceptance: "the claimed child eventually submits evidence".to_string(),
                        acceptance_checks: Vec::new(),
                        required_capabilities: vec!["read".to_string()],
                        depends_on: Vec::new(),

                        obligation_refs: Vec::new(),
                        purpose: Default::default(),
                        execution_requirements: Vec::new(),
                        expertise_hints: Vec::new(),
                    }),
                })
                .expect("publish Task")
                .changed_refs[0]
                .clone()
        };
        let unreachable = publish("stranded-publish", "Unreachable work");
        let busy = publish("stranded-busy", "Work the Agent is busy with");
        let worker_actor = {
            let mut actor = root.clone();
            actor.kind = AgentActorKind::Agent;
            actor.actor_id = worker.clone();
            actor.team_id = Some(team_ref.clone());
            actor.agent_id = Some(worker.clone());
            actor.execution_id = Some("stranded-worker".to_string());
            actor
        };
        actions
            .apply(&AgentActionEnvelope {
                action_id: "stranded-claim".to_string(),
                actor: worker_actor,
                expected_revision: None,
                action: AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: busy.clone(),
                    reason: None,
                }),
            })
            .expect("claim the other Task");

        let context = crate::agentic::execution::AgenticDispatchContext {
            session_id: format!("session:{program_id}"),
            turn_id: format!("turn:{program_id}"),
            model_lease: "test".to_string(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            resource_scopes: Vec::new(),
        };
        services
            .execution_supervisor()
            .drive_registered(root_id)
            .await
            .expect("drive wait root");
        assert_eq!(
            services
                .graph_state_store()
                .load(root_id)
                .expect("root")
                .node_statuses[&wait_node],
            ExecutionNodeStatus::WaitingExternal,
            "a Task with no live child graph still parks the root (PLAN C5-6)"
        );

        // Re-dispatch the unreachable Task: Runtime tried and could not place it.
        for attempt in 0..4 {
            let receipt = services
                .dispatch_agentic_followups(
                &AgentActionEnvelope {
                    action_id: format!("stranded-release-{attempt}"),
                    actor: root.clone(),
                    expected_revision: None,
                    action: AgentAction::TaskRelease(
                        harness_contract::agent_action::TaskReleaseInput {
                            task_ref: unreachable.clone(),
                            reason: String::new(),
                        },
                    ),
                },
                context.clone(),
            )
            .await;
        }

        let resolved = resolve_agentic_program_wait(
            program_id,
            &services.agent_action_service(),
            services.graph_state_store(),
            services.execution_supervisor().as_ref(),
            true,
        )
        .await
        .expect("resolve stranded barrier");
        assert_eq!(
            resolved, 1,
            "a Task Runtime cannot dispatch anymore must be offered to the root"
        );

        // The same Program revision must not release it twice: a root that cannot
        // act would otherwise be resumed forever and the fuse would never fire.
        let re_resolved = resolve_agentic_program_wait(
            program_id,
            &services.agent_action_service(),
            services.graph_state_store(),
            services.execution_supervisor().as_ref(),
            true,
        )
        .await
        .expect("re-resolve stranded barrier");
        assert_eq!(re_resolved, 0, "one strand resume per Program revision");

        // The same fact must reach the model. Runtime used to keep it private in the
        // dispatch starvation ledger, so the root had neither a step nor an
        // explanation; `actionable` is read on every observation.
        let inspect = services
            .agent_action_service()
            .apply(&AgentActionEnvelope {
                action_id: "stranded-inspect".to_string(),
                actor: root.clone(),
                expected_revision: None,
                action: AgentAction::StateInspect(
                    serde_json::from_value(serde_json::json!({})).unwrap(),
                ),
            })
            .expect("inspect");
        assert!(
            inspect
                .actionable
                .iter()
                .any(|hint| hint.contains("no Agent able to progress them")),
            "the model must be told a Task is unreachable, not left without a step: {:?}",
            inspect.actionable
        );
    }

    /// D6 reproduction: the barrier is parked while a Task is unsettled, the Task is
    /// then settled to `accepted`, and the resolver is asked to release the root.
    ///
    /// In paired run 3 every precondition was visibly satisfied and the resolver still
    /// recorded `resolved_waits: 0`: the barrier node was `WaitingExternal`, its payload
    /// matched the projection's program_id and root_execution_id word for word, the
    /// Program read model showed all three Tasks `accepted`, and the resolver's own
    /// cursor was level with the event stream's head. `wait_disposition` should have
    /// returned `all_tasks_accepted`. This test pins that expectation so the live
    /// divergence cannot pass silently again.
    #[tokio::test]
    async fn every_task_accepted_releases_the_parked_barrier() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-wait-all-accepted";
        let root_id = "root-wait-all-accepted";
        let wait_node = register_root_wait(&services, root_id, program_id);
        open_program(&services, program_id, root_id);
        let actions = services.agent_action_service();
        let root = root_actor(program_id, root_id);
        let team_ref = actions
            .project(program_id)
            .expect("opened projection")
            .teams
            .keys()
            .next()
            .cloned()
            .expect("opened Program has a Team");
        let invite = |action_id: &str, role: &str| {
            actions
                .apply(&AgentActionEnvelope {
                    action_id: action_id.to_string(),
                    actor: root.clone(),
                    expected_revision: None,
                    action: AgentAction::AgentInvite(
                        serde_json::from_value(serde_json::json!({
                            "team_ref": team_ref,
                            "role": role,
                            "mission": "do and verify the work",
                            "required_capabilities": ["read"]
                        }))
                        .unwrap(),
                    ),
                })
                .expect("invite Agent")
                .changed_refs[0]
                .clone()
        };
        let worker = invite("all-accepted-worker", "Worker");
        let reviewer = invite("all-accepted-reviewer", "Reviewer");
        let task = actions
            .apply(&AgentActionEnvelope {
                action_id: "all-accepted-task".to_string(),
                actor: root.clone(),
                expected_revision: None,
                action: AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: team_ref.clone(),
                    title: "One verifiable fact".to_string(),
                    objective: "extract one verifiable fact".to_string(),
                    acceptance: "artifact plus evidence".to_string(),
                    acceptance_checks: Vec::new(),
                    required_capabilities: vec!["read".to_string()],
                    depends_on: Vec::new(),

                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                }),
            })
            .expect("publish Task")
            .changed_refs[0]
            .clone();
        register_agent_child(&services, root_id, &wait_node, program_id, "child-all-accepted");
        services
            .execution_supervisor()
            .drive_registered(root_id)
            .await
            .expect("drive wait root");
        assert_eq!(
            services
                .graph_state_store()
                .load(root_id)
                .expect("root")
                .node_statuses[&wait_node],
            ExecutionNodeStatus::WaitingExternal,
            "the barrier parks while the Task is unsettled"
        );

        let agent_actor = |agent_id: &str| {
            let mut actor = root.clone();
            actor.kind = AgentActorKind::Agent;
            actor.actor_id = agent_id.to_string();
            actor.team_id = Some(team_ref.clone());
            actor.agent_id = Some(agent_id.to_string());
            actor.execution_id = Some("child-all-accepted".to_string());
            actor
        };
        actions
            .apply(&AgentActionEnvelope {
                action_id: "all-accepted-claim".to_string(),
                actor: agent_actor(&worker),
                expected_revision: None,
                action: AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: task.clone(),
                    reason: None,
                }),
            })
            .expect("claim");
        let content = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/markdown".to_string(),
                    visibility_scope: format!("session:{program_id}"),
                    expected_bytes: None,
                    original_name: Some("fact.md".to_string()),
                },
                b"one verifiable fact",
            )
            .await
            .expect("artifact content");
        let artifact = actions
            .apply(&AgentActionEnvelope {
                action_id: "all-accepted-artifact".to_string(),
                actor: agent_actor(&worker),
                expected_revision: None,
                action: AgentAction::ArtifactCommit(
                    serde_json::from_value(serde_json::json!({
                        "content_ref": content.selector,
                        "kind": "report",
                        "title": "One fact",
                        "relates_to": [task]
                    }))
                    .unwrap(),
                ),
            })
            .expect("commit artifact")
            .changed_refs[0]
            .clone();
        actions
            .apply(&AgentActionEnvelope {
                action_id: "all-accepted-submit".to_string(),
                actor: agent_actor(&worker),
                expected_revision: None,
                action: AgentAction::TaskSubmit(
                    serde_json::from_value(serde_json::json!({
                        "task_ref": task,
                        "artifact_refs": [artifact],
                        "evidence_refs": ["tool://settle-observation"]
                    }))
                    .unwrap(),
                ),
            })
            .expect("submit");

        let projection = actions.project(program_id).expect("projection before review");
        assert_eq!(
            projection.tasks.values().map(|task| task.status.clone()).collect::<Vec<_>>(),
            vec![crate::AgenticTaskStatus::Submitted],
            "the Task is Submitted, so the barrier is legitimately parked"
        );

        actions
            .apply(&AgentActionEnvelope {
                action_id: "all-accepted-review".to_string(),
                actor: {
                    let mut actor = agent_actor(&reviewer);
                    actor.execution_id = Some("child-all-accepted-review".to_string());
                    actor
                },
                expected_revision: None,
                action: AgentAction::TaskReview(
                    serde_json::from_value(serde_json::json!({
                        "task_ref": task,
                        "decision": "accept",
                        "reason": "verified",
                        "evidence_refs": ["tool://settle-review"]
                    }))
                    .unwrap(),
                ),
            })
            .expect("review accept");

        let resolved = resolve_agentic_program_wait(
            program_id,
            &actions,
            services.graph_state_store(),
            services.execution_supervisor().as_ref(),
            true,
        )
        .await
        .expect("resolve accepted barrier");
        assert_eq!(
            resolved, 1,
            "every Task accepted must release the parked barrier (D6)"
        );

        // The decision itself must be observable, which is the whole point of the
        // diagnostic record: run #6 style stalls were only inferable from a zero count.
        let dispositions = wait_dispositions(&services, program_id);
        assert!(
            dispositions.iter().any(|event| {
                event.get("disposition").and_then(Value::as_str) == Some("resume")
                    && event.get("reason").and_then(Value::as_str) == Some("all_tasks_accepted")
                    && event.get("released").and_then(Value::as_u64) == Some(1)
                    && event.get("barrier_candidates").and_then(Value::as_u64) == Some(1)
                    && event
                        .get("all_tasks_accepted")
                        .and_then(Value::as_bool)
                        == Some(true)
            }),
            "a releasing decision must be durable and must name its reason: {dispositions:?}"
        );
    }

    /// The racy sequence, end to end. A caller that settles a child graph reads the
    /// Program and waits; the event that settles the *last* Task lands afterwards, so
    /// nothing revisits the barrier unless the resolver itself does. In three paid
    /// runs nothing did: one run recorded `wait` at revision 38 with two Tasks
    /// accepted and one submitted, the final accept landed 23 seconds later, and the
    /// next resolver pass was 10 minutes after that - one second after the evaluator
    /// had cancelled the barrier.
    #[tokio::test]
    async fn one_call_after_the_final_accept_releases_the_barrier() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-wait-racy";
        let root_id = "root-wait-racy";
        let wait_node = register_root_wait(&services, root_id, program_id);
        open_program(&services, program_id, root_id);
        let actions = services.agent_action_service();
        let root = root_actor(program_id, root_id);
        let team_ref = actions
            .project(program_id)
            .expect("opened projection")
            .teams
            .keys()
            .next()
            .cloned()
            .expect("opened Program has a Team");
        let invite = |action_id: &str, role: &str| {
            actions
                .apply(&AgentActionEnvelope {
                    action_id: action_id.to_string(),
                    actor: root.clone(),
                    expected_revision: None,
                    action: AgentAction::AgentInvite(
                        serde_json::from_value(serde_json::json!({
                            "team_ref": team_ref,
                            "role": role,
                            "mission": "do and verify the work",
                            "required_capabilities": ["read"]
                        }))
                        .unwrap(),
                    ),
                })
                .expect("invite Agent")
                .changed_refs[0]
                .clone()
        };
        let worker = invite("racy-worker", "Worker");
        let reviewer = invite("racy-reviewer", "Reviewer");
        let task = actions
            .apply(&AgentActionEnvelope {
                action_id: "racy-task".to_string(),
                actor: root.clone(),
                expected_revision: None,
                action: AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: team_ref.clone(),
                    title: "One verifiable fact".to_string(),
                    objective: "extract one verifiable fact".to_string(),
                    acceptance: "artifact plus evidence".to_string(),
                    acceptance_checks: Vec::new(),
                    required_capabilities: vec!["read".to_string()],
                    depends_on: Vec::new(),

                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                }),
            })
            .expect("publish Task")
            .changed_refs[0]
            .clone();
        register_agent_child(&services, root_id, &wait_node, program_id, "child-racy");
        services
            .execution_supervisor()
            .drive_registered(root_id)
            .await
            .expect("drive wait root");
        assert_eq!(
            services
                .graph_state_store()
                .load(root_id)
                .expect("root")
                .node_statuses[&wait_node],
            ExecutionNodeStatus::WaitingExternal
        );

        let agent_actor = |agent_id: &str, execution: &str| {
            let mut actor = root.clone();
            actor.kind = AgentActorKind::Agent;
            actor.actor_id = agent_id.to_string();
            actor.team_id = Some(team_ref.clone());
            actor.agent_id = Some(agent_id.to_string());
            actor.execution_id = Some(execution.to_string());
            actor
        };
        actions
            .apply(&AgentActionEnvelope {
                action_id: "racy-claim".to_string(),
                actor: agent_actor(&worker, "child-racy"),
                expected_revision: None,
                action: AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: task.clone(),
                    reason: None,
                }),
            })
            .expect("claim");
        let content = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/markdown".to_string(),
                    visibility_scope: format!("session:{program_id}"),
                    expected_bytes: None,
                    original_name: Some("fact.md".to_string()),
                },
                b"one verifiable fact",
            )
            .await
            .expect("artifact content");
        let artifact = actions
            .apply(&AgentActionEnvelope {
                action_id: "racy-artifact".to_string(),
                actor: agent_actor(&worker, "child-racy"),
                expected_revision: None,
                action: AgentAction::ArtifactCommit(
                    serde_json::from_value(serde_json::json!({
                        "content_ref": content.selector,
                        "kind": "report",
                        "title": "One fact",
                        "relates_to": [task]
                    }))
                    .unwrap(),
                ),
            })
            .expect("commit artifact")
            .changed_refs[0]
            .clone();
        actions
            .apply(&AgentActionEnvelope {
                action_id: "racy-submit".to_string(),
                actor: agent_actor(&worker, "child-racy"),
                expected_revision: None,
                action: AgentAction::TaskSubmit(
                    serde_json::from_value(serde_json::json!({
                        "task_ref": task,
                        "artifact_refs": [artifact],
                        "evidence_refs": ["tool://racy-observation"]
                    }))
                    .unwrap(),
                ),
            })
            .expect("submit");

        // The caller's own pass: the Task is still Submitted, so waiting is correct.
        let waited = resolve_agentic_program_wait(
            program_id,
            &actions,
            services.graph_state_store(),
            services.execution_supervisor().as_ref(),
            true,
        )
        .await
        .expect("first resolve");
        assert_eq!(waited, 0, "an unsettled Task must keep the root parked");

        // The Program event that settles the last Task lands afterwards.
        actions
            .apply(&AgentActionEnvelope {
                action_id: "racy-review".to_string(),
                actor: agent_actor(&reviewer, "child-racy-review"),
                expected_revision: None,
                action: AgentAction::TaskReview(
                    serde_json::from_value(serde_json::json!({
                        "task_ref": task,
                        "decision": "accept",
                        "reason": "verified",
                        "evidence_refs": ["tool://racy-review"]
                    }))
                    .unwrap(),
                ),
            })
            .expect("review accept");

        // The very next call must release it, without waiting for any other wake.
        let released = resolve_agentic_program_wait(
            program_id,
            &actions,
            services.graph_state_store(),
            services.execution_supervisor().as_ref(),
            true,
        )
        .await
        .expect("second resolve");
        assert_eq!(
            released, 1,
            "one call after the final accept must release the barrier (D6)"
        );
    }

    /// A parked barrier must release itself without anybody waking it. Three paid
    /// runs had all three external invocation paths miss the decisive moment: the
    /// final accept and its child graph's `running -> completed` landed in the same
    /// second, and not one resolver pass ran in the following ten minutes. The
    /// barrier therefore re-arms itself on a bounded interval and stops the moment it
    /// is no longer parked.
    #[tokio::test]
    async fn a_parked_barrier_releases_itself_without_any_external_wake() {
        let services = RuntimeServices::in_memory().expect("runtime");
        let program_id = "program-wait-self-rearm";
        let root_id = "root-wait-self-rearm";
        let wait_node = register_root_wait(&services, root_id, program_id);
        open_program(&services, program_id, root_id);
        let actions = services.agent_action_service();
        let root = root_actor(program_id, root_id);
        let team_ref = actions
            .project(program_id)
            .expect("opened projection")
            .teams
            .keys()
            .next()
            .cloned()
            .expect("opened Program has a Team");
        let invite = |action_id: &str, role: &str| {
            actions
                .apply(&AgentActionEnvelope {
                    action_id: action_id.to_string(),
                    actor: root.clone(),
                    expected_revision: None,
                    action: AgentAction::AgentInvite(
                        serde_json::from_value(serde_json::json!({
                            "team_ref": team_ref,
                            "role": role,
                            "mission": "do and verify the work",
                            "required_capabilities": ["read"]
                        }))
                        .unwrap(),
                    ),
                })
                .expect("invite Agent")
                .changed_refs[0]
                .clone()
        };
        let worker = invite("rearm-worker", "Worker");
        let reviewer = invite("rearm-reviewer", "Reviewer");
        let task = actions
            .apply(&AgentActionEnvelope {
                action_id: "rearm-task".to_string(),
                actor: root.clone(),
                expected_revision: None,
                action: AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: team_ref.clone(),
                    title: "One verifiable fact".to_string(),
                    objective: "extract one verifiable fact".to_string(),
                    acceptance: "artifact plus evidence".to_string(),
                    acceptance_checks: Vec::new(),
                    required_capabilities: vec!["read".to_string()],
                    depends_on: Vec::new(),

                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                }),
            })
            .expect("publish Task")
            .changed_refs[0]
            .clone();
        register_agent_child(&services, root_id, &wait_node, program_id, "child-rearm");
        services
            .execution_supervisor()
            .drive_registered(root_id)
            .await
            .expect("drive wait root");
        assert_eq!(
            services
                .graph_state_store()
                .load(root_id)
                .expect("root")
                .node_statuses[&wait_node],
            ExecutionNodeStatus::WaitingExternal
        );

        let agent_actor = |agent_id: &str, execution: &str| {
            let mut actor = root.clone();
            actor.kind = AgentActorKind::Agent;
            actor.actor_id = agent_id.to_string();
            actor.team_id = Some(team_ref.clone());
            actor.agent_id = Some(agent_id.to_string());
            actor.execution_id = Some(execution.to_string());
            actor
        };
        actions
            .apply(&AgentActionEnvelope {
                action_id: "rearm-claim".to_string(),
                actor: agent_actor(&worker, "child-rearm"),
                expected_revision: None,
                action: AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: task.clone(),
                    reason: None,
                }),
            })
            .expect("claim");
        let content = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/markdown".to_string(),
                    visibility_scope: format!("session:{program_id}"),
                    expected_bytes: None,
                    original_name: Some("fact.md".to_string()),
                },
                b"one verifiable fact",
            )
            .await
            .expect("artifact content");
        let artifact = actions
            .apply(&AgentActionEnvelope {
                action_id: "rearm-artifact".to_string(),
                actor: agent_actor(&worker, "child-rearm"),
                expected_revision: None,
                action: AgentAction::ArtifactCommit(
                    serde_json::from_value(serde_json::json!({
                        "content_ref": content.selector,
                        "kind": "report",
                        "title": "One fact",
                        "relates_to": [task]
                    }))
                    .unwrap(),
                ),
            })
            .expect("commit artifact")
            .changed_refs[0]
            .clone();
        actions
            .apply(&AgentActionEnvelope {
                action_id: "rearm-submit".to_string(),
                actor: agent_actor(&worker, "child-rearm"),
                expected_revision: None,
                action: AgentAction::TaskSubmit(
                    serde_json::from_value(serde_json::json!({
                        "task_ref": task,
                        "artifact_refs": [artifact],
                        "evidence_refs": ["tool://rearm-observation"]
                    }))
                    .unwrap(),
                ),
            })
            .expect("submit");

        // Settle the Task, and from here on invoke nothing: no lane, no observer, no
        // second resolver call. The only thing that can release the barrier is the
        // barrier itself.
        actions
            .apply(&AgentActionEnvelope {
                action_id: "rearm-review".to_string(),
                actor: {
                    let mut actor = agent_actor(&reviewer, "child-rearm-review");
                    actor
                },
                expected_revision: None,
                action: AgentAction::TaskReview(
                    serde_json::from_value(serde_json::json!({
                        "task_ref": task,
                        "decision": "accept",
                        "reason": "verified",
                        "evidence_refs": ["tool://rearm-review"]
                    }))
                    .unwrap(),
                ),
            })
            .expect("review accept");

        // Nothing else is invoked from here on: no lane, no observer, no second call.
        // The only thing that can release the barrier is the barrier itself.
        let released_itself = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            async {
                loop {
                    let still_parked = services
                        .graph_state_store()
                        .load_current_async(root_id.to_string())
                        .await
                        .ok()
                        .and_then(|graph| graph.node_statuses.get(&wait_node).copied())
                        == Some(ExecutionNodeStatus::WaitingExternal);
                    if !still_parked {
                        return true;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            },
        )
        .await
        .unwrap_or(false);
        assert!(
            released_itself,
            "a parked barrier must release itself without any external wake (D6)"
        );
    }

    fn wait_dispositions(services: &RuntimeServices, program_id: &str) -> Vec<Value> {
        let store = services.event_store();
        let stream_id = format!("agentic-program-wait:{program_id}");
        let revision = store.stream_revision(&stream_id).unwrap_or_default();
        store
            .list_stream_after(&stream_id, 0, revision, 1_000, 32 * 1024 * 1024)
            .unwrap_or_default()
            .into_iter()
            .filter(|event| event.kind == "agentic.program_wait_disposition")
            .map(|event| event.payload)
            .collect()
    }
}
