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
}

impl AgenticProgramWaitRequest {
    /// Resolve an explicit root yield against durable execution truth. Child
    /// activity alone must never suspend unfinished root planning.
    pub(crate) async fn for_active_root(
        program: &crate::AgenticProgramProjection,
        root_execution_id: &str,
        state_store: &ExecutionGraphStateStore,
        wait_requested: bool,
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
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref: Some(result_ref),
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
        })
    }
}

/// Resolve every waiting barrier for one Program when canonical Program or
/// child-graph truth says the root model has a semantic decision to make.
/// `allow_same_revision_quiescence` is used by child-settle/startup recovery;
/// Program-event passes keep it false so replaying the event that originally
/// created the wait cannot spuriously release an active barrier.
pub async fn resolve_agentic_program_wait(
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
    let request = AgenticProgramWaitRequest {
        program_id: program_id.to_string(),
        root_execution_id: root_execution_id.to_string(),
    };
    let disposition = wait_disposition(&projection, &request, state_store).await?;
    let WaitDisposition::Resume { revision, reason } = disposition else {
        return Ok(0);
    };
    let mut resolved = 0usize;
    const MAX_GRAPH_CAS_ATTEMPTS: usize = 8;
    for attempt in 0..MAX_GRAPH_CAS_ATTEMPTS {
        let graph = match state_store
            .load_current_async(root_execution_id.to_string())
            .await
        {
            Ok(graph) => graph,
            Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound(_)) => {
                return Ok(resolved);
            }
            Err(error) => return Err(error.to_string()),
        };
        let candidate = graph.nodes.iter().find(|node| {
            node.kind == ExecutionNodeKind::Verify
                && node.executor_kind == AgenticProgramWaitExecutor::KIND
                && graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::WaitingExternal)
                && serde_json::from_str::<AgenticProgramWaitRequest>(&node.payload_ref)
                    .is_ok_and(|candidate| candidate == request)
        });
        let Some(node) = candidate else {
            break;
        };
        let observed_revision = graph
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
        let quiescent_resume = reason == "worker_quiescent";
        if !allow_same_revision_quiescence
            && !quiescent_resume
            && projection.status == AgenticProgramStatus::Open
            && !all_tasks_accepted(&projection)
            && revision <= observed_revision
        {
            break;
        }
        let correlation_id = format!(
            "agentic-program-wait:{}:{}:{}",
            program_id, node.id, revision
        );
        match supervisor
            .command(
                root_execution_id,
                ExecutionGraphCommand::ResolveExternal {
                    expected_revision: graph.revision,
                    node_id: node.id.clone(),
                    result_ref: format!(
                        "agentic-program-checkpoint:{program_id}:revision:{revision}:{reason}"
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
        Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound(_)) => return Ok(0),
        Err(error) => return Err(error.to_string()),
    };
    let mut program_ids = agentic_program_ids(&graph);
    program_ids.sort();
    program_ids.dedup();
    let mut resolved = 0usize;
    for program_id in program_ids {
        resolved = resolved.saturating_add(
            resolve_agentic_program_wait(&program_id, actions, state_store, supervisor, true)
                .await?,
        );
    }
    Ok(resolved)
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

fn all_tasks_accepted(projection: &AgenticProgramProjection) -> bool {
    !projection.tasks.is_empty()
        && projection
            .tasks
            .values()
            .all(|task| task.status == AgenticTaskStatus::Accepted)
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
            Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound(_)) => continue,
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
        TaskPublishInput, TeamCreateInput,
    };
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage, ExecutionNodeKind,
        ExecutionNodeSpec, ExecutionNodeStatus, ExecutionParentBinding,
    };
    use harness_contract::policy::PermissionMode;

    use super::{
        reconcile_agentic_program_wait_for_settled_graph, AgenticProgramWaitExecutor,
        AgenticProgramWaitRequest,
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
            )
            .await
            .expect("derive dispatched wait"),
            Some(AgenticProgramWaitRequest {
                program_id: program_id.to_string(),
                root_execution_id: root_execution_id.to_string(),
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
}
