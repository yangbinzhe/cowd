//! Graph-owned Team facade.
//!
//! TeamRuntime compiles collaboration semantics and reads projections. It owns
//! no scheduler, worker, state file, or process-global registry.

use std::collections::BTreeSet;
use std::sync::Arc;

use harness_contract::{
    agent::{AgentCommand, AgentCommandRequest, AgentInput, AgentStatus, AgentTaskPacket},
    reality::EvidenceRef,
    team::{TeamInstantiationRequest, TeamTemplateRevisionRef},
};
use sha2::{Digest, Sha256};

use crate::execution_core::ExecutionStateStoreError;
use crate::{
    AgentRuntime, AppendTransactionRequest, CollaborationControlOperation,
    CollaborationControlRequest, EvolutionGovernanceService, ExecutionGraphHost,
    ExecutionGraphHostReceipt, ExecutionGraphStateStore, ExpectedStreamRevision,
    LegacyTeamImportReport, LegacyTeamProfileMigrationReport, MissionRuntime,
    RuntimeDefinitionRegistry, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope,
    RuntimeEventStore, RuntimeExecutionSupervisor, RuntimeTransactionEventInput, TaskRuntimePort,
    TeamInstantiation, TeamProjection, TeamProjectionCursor, TeamProjectionPage,
    TeamProjectionReader, TeamWorkingState, TeamWorkingStateAcknowledgeRequest,
    TeamWorkingStateCursor, TeamWorkingStateEntry, TeamWorkingStateKind,
    TeamWorkingStatePublishRequest, TeamWorkingStateReadRequest, TeamWorkingStateVisibility,
};

pub struct TeamRuntime {
    execution: Arc<RuntimeExecutionSupervisor>,
    instantiation: crate::TeamInstantiationService,
    projection: TeamProjectionReader,
    graphs: ExecutionGraphStateStore,
    agents: Arc<AgentRuntime>,
    event_store: Arc<RuntimeEventStore>,
    tasks: TaskRuntimePort,
    missions: Arc<MissionRuntime>,
}

const MIN_COLLABORATION_LEASE_MS: u64 = 5_000;
const DEFAULT_COLLABORATION_LEASE_MS: u64 = 900_000;
const MAX_COLLABORATION_LEASE_MS: u64 = 3_600_000;

fn agent_node_is_actionable(
    graph: &harness_contract::execution_graph::ExecutionGraph,
    node_id: &str,
) -> bool {
    use harness_contract::execution_graph::ExecutionNodeStatus;

    match graph.node_statuses.get(node_id) {
        Some(ExecutionNodeStatus::Ready | ExecutionNodeStatus::Running) => true,
        Some(ExecutionNodeStatus::Planned) => graph
            .edges
            .iter()
            .filter(|edge| edge.to == node_id && edge.kind.is_dependency())
            .all(|edge| {
                graph.node_statuses.get(&edge.from) == Some(&ExecutionNodeStatus::Completed)
            }),
        _ => false,
    }
}

/// True when an Agent is either runnable now or remains reachable later in
/// the admitted Team topology. Proposal admission must use this broader
/// predicate: requiring a peer to be concurrently Ready made valid serial
/// Teams incapable of creating work for their next role. Failed/cancelled
/// dependency ancestry still fails closed, so a proposal cannot target an
/// orphan that the scheduler can never start.
fn agent_node_is_future_schedulable(
    graph: &harness_contract::execution_graph::ExecutionGraph,
    node_id: &str,
) -> bool {
    use harness_contract::execution_graph::ExecutionNodeStatus;

    if !matches!(
        graph.node_statuses.get(node_id),
        Some(
            ExecutionNodeStatus::Planned
                | ExecutionNodeStatus::Ready
                | ExecutionNodeStatus::Running
        )
    ) {
        return false;
    }
    let mut pending = vec![node_id];
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop() {
        if !visited.insert(current) {
            continue;
        }
        for dependency in graph
            .edges
            .iter()
            .filter(|edge| edge.to == current && edge.kind.is_dependency())
        {
            if matches!(
                graph.node_statuses.get(&dependency.from),
                Some(ExecutionNodeStatus::Failed | ExecutionNodeStatus::Cancelled)
            ) {
                return false;
            }
            pending.push(dependency.from.as_str());
        }
    }
    true
}

fn effective_collaboration_lease_ms(
    requested: Option<u64>,
    expected_duration_ms: u64,
) -> Result<u64, String> {
    let requested = requested.unwrap_or(DEFAULT_COLLABORATION_LEASE_MS);
    if !(MIN_COLLABORATION_LEASE_MS..=MAX_COLLABORATION_LEASE_MS).contains(&requested) {
        return Err(format!(
            "collaboration lease_duration_ms must be within {MIN_COLLABORATION_LEASE_MS}..{MAX_COLLABORATION_LEASE_MS}"
        ));
    }
    Ok(requested
        .max(expected_duration_ms)
        .clamp(MIN_COLLABORATION_LEASE_MS, MAX_COLLABORATION_LEASE_MS))
}

/// Return the collaboration market as a bounded, action-first control view.
///
/// The public graph projection is intentionally rich enough for operators and
/// Surfaces, but returning that entire projection to every model-side inspect
/// buried the mutation fences and eligible work behind tens of kilobytes of
/// topology. This view contains only the state an Agent can act on. Full graph
/// observability remains available through the graph and Mission APIs.
fn collaboration_control_view(
    operation: CollaborationControlOperation,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    caller_node_id: &str,
    caller_instance_id: &str,
    caller_role_id: Option<&str>,
    accepted_revision: Option<u64>,
    claim_token: Option<String>,
    idempotent_replay: bool,
) -> serde_json::Value {
    use harness_contract::execution_graph::ExecutionWorkRuntimeStatus;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let eligible_peers = graph
        .nodes
        .iter()
        .filter(|node| {
            node.id != caller_node_id
                && node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                && agent_node_is_future_schedulable(graph, &node.id)
        })
        .filter_map(|node| {
            serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                .ok()
                .map(|packet| {
                    serde_json::json!({
                        "node_id": node.id,
                        "instance_id": packet.assignment.instance_id,
                        "role_id": packet.assignment.role_id,
                        "capabilities": packet.allowed_tools,
                        "availability": if agent_node_is_actionable(graph, &node.id) { "active" } else { "future" },
                    })
                })
        })
        .collect::<Vec<_>>();

    let assigned_work = graph
        .nodes
        .iter()
        .filter_map(|node| {
            let work = node.work.as_ref()?;
            let state = graph.work_states.get(&node.id).cloned().unwrap_or_default();
            let identity = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok();
            Some(serde_json::json!({
                "work_node_id": node.id,
                "role_id": identity.as_ref().map(|packet| packet.assignment.role_id.as_str()),
                "status": state.status,
                "work_revision": state.revision,
                "work_role": work.role,
                "required": work.required,
            }))
        })
        .collect::<Vec<_>>();
    let active_peers = eligible_peers
        .iter()
        .filter(|peer| {
            peer.get("availability").and_then(serde_json::Value::as_str) == Some("active")
        })
        .cloned()
        .collect::<Vec<_>>();

    let autonomous_work = graph
        .autonomous_work
        .iter()
        .map(|(work_id, work)| {
            let mut state = graph.work_states.get(work_id).cloned().unwrap_or_default();
            if let Some(claim) = state.claim.as_mut() {
                claim.claim_token = "<redacted>".to_string();
            }
            let already_bid = state
                .bids
                .iter()
                .any(|bid| bid.bidder_instance_id == caller_instance_id);
            let owner = state
                .claim
                .as_ref()
                .is_some_and(|claim| claim.claimant_instance_id == caller_instance_id);
            let proposed_by_caller = work.proposed_by.as_deref() == Some(caller_instance_id);
            let claim_expired = state
                .claim
                .as_ref()
                .is_some_and(|claim| claim.lease_expires_at_ms <= now_ms);
            let mut next_actions = Vec::new();
            match state.status {
                ExecutionWorkRuntimeStatus::Offered | ExecutionWorkRuntimeStatus::Challenged => {
                    if !proposed_by_caller && !already_bid {
                        next_actions.push("bid");
                    }
                    if !proposed_by_caller && already_bid {
                        next_actions.push("claim");
                    }
                }
                ExecutionWorkRuntimeStatus::Claimed if claim_expired => {
                    if !proposed_by_caller && already_bid {
                        next_actions.push("claim");
                    }
                }
                ExecutionWorkRuntimeStatus::Claimed if owner => {
                    next_actions.extend(["heartbeat", "submit"]);
                }
                ExecutionWorkRuntimeStatus::Submitted if !owner && !proposed_by_caller => {
                    next_actions.extend(["accept", "challenge"]);
                }
                ExecutionWorkRuntimeStatus::Claimed
                | ExecutionWorkRuntimeStatus::Submitted
                | ExecutionWorkRuntimeStatus::Accepted => {}
            }
            serde_json::json!({
                "work_node_id": work_id,
                "objective": work.objective,
                "proposed_by": work.proposed_by,
                "required_capabilities": work.eligibility.required_capabilities,
                "output_artifact_kinds": work.output_artifact_kinds,
                "status": state.status,
                "work_revision": state.revision,
                "bids": state.bids,
                "claim": state.claim,
                "submission_ref": state.submission_ref,
                "reviews": state.reviews,
                "review_findings": state.review_findings,
                "next_actions_for_caller": next_actions,
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "action_guide": {
            "rule": "Use each work item's work_revision as expected_work_revision. Agent-proposed work requires bid before claim; only the claim owner receives claim_token; a different peer must accept or challenge a submission.",
            "propose_work": {"operation": "propose_work", "expected_work_revision": 0, "proposal": {"idempotency_key": "stable-key", "objective": "bounded objective", "role": "cross_check", "output_artifact_kinds": ["cross_check"]}},
            "bid": {"operation": "bid", "work_node_id": "<id>", "expected_work_revision": "<revision>", "rationale": "bounded rationale", "estimated_cost": 0},
            "claim": {"operation": "claim", "work_node_id": "<id>", "expected_work_revision": "<revision>", "lease_duration_ms": DEFAULT_COLLABORATION_LEASE_MS},
            "submit": {"operation": "submit", "work_node_id": "<id>", "expected_work_revision": "<revision>", "claim_token": "<owner-token>", "submission_ref": "team-board:<entry_id>"},
            "review": {"operation": "accept|challenge", "work_node_id": "<id>", "expected_work_revision": "<revision>"}
        },
        "accepted_revision": accepted_revision,
        "attested_agent_instance_id": caller_instance_id,
        "caller": {"node_id": caller_node_id, "role_id": caller_role_id},
        "claim_token": claim_token,
        "graph_id": graph.id,
        "graph_revision": graph.revision,
        "idempotent_replay": idempotent_replay,
        "marketplace": {
            // Backward-compatible now-runnable view plus the broader future
            // schedulable view used for proposal admission.
            "active_peer_count": active_peers.len(),
            "active_peers": active_peers,
            "eligible_peer_count": eligible_peers.len(),
            "eligible_peers": eligible_peers,
            "assigned_work": assigned_work,
            "autonomous_work": autonomous_work,
            "can_propose_work": !eligible_peers.is_empty(),
        },
        "operation": operation,
    })
}

impl TeamRuntime {
    #[must_use]
    pub fn new(
        execution: Arc<RuntimeExecutionSupervisor>,
        graphs: ExecutionGraphStateStore,
        agents: Arc<AgentRuntime>,
        event_store: Arc<RuntimeEventStore>,
        definition_registry: Arc<RuntimeDefinitionRegistry>,
        evolution_governance: Arc<EvolutionGovernanceService>,
        workspace_id: impl Into<String>,
        path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
        tasks: TaskRuntimePort,
        missions: Arc<MissionRuntime>,
    ) -> Self {
        Self {
            execution,
            instantiation: crate::TeamInstantiationService::new(
                definition_registry,
                evolution_governance,
                workspace_id,
                path_identity_resolver,
            ),
            projection: TeamProjectionReader::new(
                graphs.clone(),
                Arc::clone(&agents),
                Some(Arc::clone(&event_store)),
            ),
            graphs,
            agents,
            event_store,
            tasks,
            missions,
        }
    }

    /// Resolve a durable Team request into its immutable graph without
    /// scheduling it. Evaluation and dry-run callers use this same canonical
    /// compiler; no second graph builder exists outside Runtime.
    pub fn plan(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<crate::TeamInstantiation, String> {
        if request.mission_id == self.missions.default_mission_id() {
            self.missions.ensure_default_mission()?;
        }
        if self.missions.aggregate(&request.mission_id).is_none() {
            return Err(format!("Team mission not found: {}", request.mission_id));
        }
        self.instantiation.instantiate(request)
    }

    pub(crate) fn graph_state_store(&self) -> &ExecutionGraphStateStore {
        &self.graphs
    }

    pub(crate) fn ensure_root_task(
        &self,
        request: &TeamInstantiationRequest,
    ) -> Result<(), String> {
        if self.tasks.get(&request.lineage.root_task_id)?.is_some() {
            return Ok(());
        }
        let spec = self.tasks.bind_task_spec(
            &request.lineage.session_id,
            Some(request.permission_ceiling),
            harness_contract::task::TaskSpec::new(request.objective.clone()),
        )?;
        self.tasks
            .create(harness_contract::task::TaskCreateCommand {
                task_id: request.lineage.root_task_id.clone(),
                mission_id: request.mission_id.clone(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::System,
                origin_session_id: request.lineage.session_id.clone(),
                origin_turn_id: request.lineage.turn_id.clone(),
                root_task_id: request.lineage.root_task_id.clone(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::Automatic,
                mission_assigned_by: "runtime.team".to_string(),
                spec,
                evidence_refs: vec![EvidenceRef::observed(
                    "team_request",
                    request.request_id.clone(),
                )],
            })
            .map(|_| ())
    }

    /// Instantiate a durable Team Template through the canonical Runtime
    /// request. The returned graph already contains exact Agent Bindings, so
    /// Runner never selects a profile or template while executing.
    pub async fn instantiate(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<TeamProjection, String> {
        self.ensure_ephemeral_template_parent_is_live(&request)?;
        self.claim_team_root(&request)?;
        self.ensure_root_task(&request)?;
        let mut instantiated = self.plan(request)?;
        self.bind_instantiated_task_policies(&mut instantiated)?;
        self.instantiation.validate_release(&instantiated)?;
        let graph_id = instantiated.graph.id.clone();
        let TeamInstantiation {
            graph,
            task_commands,
            binding,
            ..
        } = instantiated;
        let registered = self
            .execution
            .register_graph(graph)
            .await
            .map_err(|error| error.to_string())?;
        let binding = binding
            .ok_or_else(|| format!("Team graph `{graph_id}` has no compiled Team Binding"))?;
        self.persist_binding_markers(
            &registered.id,
            &binding,
            &task_commands,
            registered.revision,
        )?;
        self.execution
            .drive_registered(&registered.id)
            .await
            .map_err(|error| error.to_string())?;
        self.projection.project(&graph_id)
    }

    fn ensure_ephemeral_template_parent_is_live(
        &self,
        request: &TeamInstantiationRequest,
    ) -> Result<(), String> {
        let harness_contract::team::TeamTemplateSelector::Ephemeral { snapshot } =
            &request.template_selector
        else {
            return Ok(());
        };
        snapshot.validate()?;
        let parent = request
            .parent_execution
            .as_ref()
            .ok_or_else(|| "ephemeral_template_requires_program_parent".to_string())?;
        let graph = self
            .graphs
            .load(&parent.execution_id)
            .map_err(|error| format!("ephemeral_template_parent_load_failed:{error}"))?;
        let program = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .ok_or_else(|| "ephemeral_template_parent_has_no_program".to_string())?;
        if program.control.lifecycle.is_terminal() {
            return Err("ephemeral_template_parent_program_is_terminal".to_string());
        }
        let expected_fence = format!(
            "task:{}:turn:{}",
            request.lineage.root_task_id, request.lineage.turn_id
        );
        if snapshot.terminal_fence != expected_fence {
            return Err("ephemeral_template_parent_terminal_fence_mismatch".to_string());
        }
        Ok(())
    }

    /// Idempotent child-graph entry used by a root Mission graph. Recovery
    /// resumes the exact deterministic Team graph instead of registering a
    /// second child or rebuilding topology from model prose.
    pub async fn instantiate_or_resume(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<TeamProjection, String> {
        let graph_id = format!("team-graph:{}", request.team_id);
        match self.graphs.load(&graph_id) {
            Ok(_) => {
                self.ensure_root_task(&request)?;
                self.reconcile_binding_markers(&graph_id, &request)?;
                self.execution
                    .drive_registered(&graph_id)
                    .await
                    .map_err(|error| error.to_string())?;
                return self.projection.project(&graph_id);
            }
            Err(ExecutionStateStoreError::NotFound(_)) => {}
            Err(error) => return Err(error.to_string()),
        }
        self.instantiate(request).await
    }

    /// Idempotently admit a child Team graph without awaiting its execution.
    ///
    /// A root graph owns the durable `WaitingExternal` join. The supervisor
    /// must be allowed to release the root graph slot before scheduling this
    /// child, otherwise `max_parallel_graphs = 1` deadlocks recursively.
    pub(crate) async fn admit_or_resume(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<TeamProjection, String> {
        let graph_id = format!("team-graph:{}", request.team_id);
        match self.graphs.load(&graph_id) {
            Ok(existing) => {
                if existing.parent_execution != request.parent_execution
                    || existing.lineage.as_ref() != Some(&request.lineage)
                    || existing.id != graph_id
                {
                    return Err(format!(
                        "existing Team graph `{graph_id}` does not match the requested parent/lineage binding"
                    ));
                }
                let projection = self.projection.project(&graph_id)?;
                if projection.team_id != request.team_id
                    || projection.session_id != request.lineage.session_id
                {
                    return Err(format!(
                        "existing Team graph `{graph_id}` projects a different Team or Session identity"
                    ));
                }
                self.ensure_root_task(&request)?;
                self.reconcile_binding_markers(&graph_id, &request)?;
                // Lifecycle is the graph-derived execution authority. A
                // recovered Team may legitimately be `preparing` or waiting
                // when this process starts; delivery text must not suppress
                // its one durable admission.
                if projection.lifecycle != harness_contract::team::TeamLifecycleState::Terminal {
                    self.execution
                        .admit_registered(&graph_id)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                return Ok(projection);
            }
            Err(ExecutionStateStoreError::NotFound(_)) => {}
            Err(error) => return Err(error.to_string()),
        }
        self.ensure_root_task(&request)?;
        let mut instantiated = self.plan(request)?;
        self.bind_instantiated_task_policies(&mut instantiated)?;
        self.instantiation.validate_release(&instantiated)?;
        let TeamInstantiation {
            graph,
            task_commands,
            binding,
            ..
        } = instantiated;
        let registered = self
            .execution
            .register_graph(graph)
            .await
            .map_err(|error| error.to_string())?;
        let binding = binding.ok_or_else(|| {
            format!(
                "Team graph `{}` has no compiled Team Binding",
                registered.id
            )
        })?;
        self.persist_binding_markers(
            &registered.id,
            &binding,
            &task_commands,
            registered.revision,
        )?;
        self.execution
            .admit_registered(&registered.id)
            .await
            .map_err(|error| error.to_string())?;
        self.projection.project(&registered.id)
    }

    /// Admit a Team graph and return after durable submission. The supervisor
    /// owns all Agent/model/tool execution after this boundary.
    pub async fn admit(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<ExecutionGraphHostReceipt, String> {
        self.claim_team_root(&request)?;
        self.ensure_root_task(&request)?;
        let mission_id = request.mission_id.clone();
        let team_id = request.team_id.clone();
        let instantiated = self.plan(request)?;
        self.admit_planned(&mission_id, &team_id, instantiated)
            .await
    }

    pub(crate) async fn admit_planned(
        &self,
        mission_id: &str,
        team_id: &str,
        instantiated: crate::TeamInstantiation,
    ) -> Result<ExecutionGraphHostReceipt, String> {
        let graph_id = self
            .prepare_planned(mission_id, team_id, instantiated)
            .await?;
        self.execution
            .admit_registered(&graph_id)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn prepare_planned(
        &self,
        _mission_id: &str,
        _team_id: &str,
        mut instantiated: crate::TeamInstantiation,
    ) -> Result<String, String> {
        self.instantiation.validate_release(&instantiated)?;
        self.bind_instantiated_task_policies(&mut instantiated)?;
        let TeamInstantiation {
            graph,
            task_commands,
            binding,
            ..
        } = instantiated;
        let registered = self
            .execution
            .register_graph(graph)
            .await
            .map_err(|error| error.to_string())?;
        let binding = binding.ok_or_else(|| {
            format!(
                "Team graph `{}` has no compiled Team Binding",
                registered.id
            )
        })?;
        self.persist_binding_markers(
            &registered.id,
            &binding,
            &task_commands,
            registered.revision,
        )?;
        Ok(registered.id)
    }

    /// Run one evaluation-only Team graph. The candidate template is selected
    /// by a crate-private Runtime path and is never materialized as a normal
    /// release assignment; the baseline continues through normal exact
    /// approved resolution. Both sides still use the canonical Team compiler
    /// and graph runner.
    pub(crate) async fn instantiate_evaluation(
        &self,
        request: TeamInstantiationRequest,
        candidate_revision: Option<&TeamTemplateRevisionRef>,
        allowed_tools: &[String],
    ) -> Result<TeamProjection, String> {
        self.ensure_root_task(&request)?;
        let mut instantiated = match candidate_revision {
            Some(revision) => {
                self.instantiation
                    .instantiate_evaluation(request, revision, allowed_tools)?
            }
            None => self
                .instantiation
                .instantiate_evaluation_baseline(request, allowed_tools)?,
        };
        self.bind_instantiated_task_policies(&mut instantiated)?;
        let graph_id = instantiated.graph.id.clone();
        let TeamInstantiation {
            graph,
            task_commands,
            binding,
            ..
        } = instantiated;
        let registered = self
            .execution
            .register_graph(graph)
            .await
            .map_err(|error| error.to_string())?;
        let binding = binding
            .ok_or_else(|| format!("Team graph `{graph_id}` has no compiled Team Binding"))?;
        self.persist_binding_markers(
            &registered.id,
            &binding,
            &task_commands,
            registered.revision,
        )?;
        self.execution
            .drive_registered(&registered.id)
            .await
            .map_err(|error| error.to_string())?;
        self.projection.project(&graph_id)
    }

    fn admit_tasks(
        &self,
        commands: &[harness_contract::task::TaskCreateCommand],
        graph_id: &str,
        graph_revision: u64,
    ) -> Result<(), String> {
        for command in commands {
            let task = self.tasks.create_inherited(command.clone())?;
            self.tasks.link_existing_graph(
                &task.task_id,
                graph_id,
                graph_revision,
                vec![EvidenceRef::observed(
                    "execution_graph",
                    format!("execution-graph://{graph_id}?revision={graph_revision}"),
                )],
            )?;
        }
        Ok(())
    }

    /// Persist the frozen Team Binding and close the durable link set exactly
    /// once. Recovery retries are idempotent through stable event keys and
    /// task create/link idempotency.
    fn persist_binding_markers(
        &self,
        graph_id: &str,
        binding: &harness_contract::team::TeamBindingSnapshot,
        task_commands: &[harness_contract::task::TaskCreateCommand],
        graph_revision: u64,
    ) -> Result<(), String> {
        if crate::team_binding::has_ready_marker(&self.event_store, graph_id)? {
            return Ok(());
        }
        crate::team_binding::persist_preparing_with_task_commands(
            &self.event_store,
            graph_id,
            binding,
            task_commands,
        )?;
        self.admit_tasks(task_commands, graph_id, graph_revision)?;
        crate::team_binding::persist_ready(&self.event_store, graph_id, &binding.binding_digest)
    }

    /// CAS gate for one Team root per request/team tuple. A concurrent
    /// continuation or retry can never create a second root for the same
    /// continuation digest+ingress.
    fn claim_team_root(&self, request: &TeamInstantiationRequest) -> Result<(), String> {
        if !crate::claim_continuation_root(
            &self.event_store,
            &request.request_id,
            &format!("team:{}", request.team_id),
        )? {
            return Err(format!(
                "concurrent continuation already claimed Team root `{}` for ingress `{}`",
                request.team_id, request.request_id
            ));
        }
        Ok(())
    }

    /// Reconciliation facade for an existing graph whose durable link set may
    /// be incomplete (crash between registration and Ready). It never drives
    /// an orphan graph and never re-registers the graph.
    fn reconcile_binding_markers(
        &self,
        graph_id: &str,
        request: &TeamInstantiationRequest,
    ) -> Result<(), String> {
        if crate::team_binding::has_ready_marker(&self.event_store, graph_id)? {
            return Ok(());
        }
        // A Preparing marker produced by the current admission protocol is
        // already the immutable source of the Task link plan.  Never
        // recompile it from a retried request: policy/template evolution in
        // between a crash and recovery must not turn the same graph into a
        // different Team.
        if let Some(task_commands) =
            crate::team_binding::load_prepared_task_commands(&self.event_store, graph_id)?
        {
            if task_commands.is_empty() {
                return Err(format!(
                    "Team graph `{graph_id}` Preparing Task link plan is empty"
                ));
            }
            let binding = crate::team_binding::load_binding(&self.event_store, graph_id)?
                .ok_or_else(|| format!("Team graph `{graph_id}` has no compiled Team Binding"))?;
            let revision = self
                .graphs
                .load(graph_id)
                .map_err(|error| error.to_string())?
                .revision;
            return self.persist_binding_markers(graph_id, &binding, &task_commands, revision);
        }

        // Old marker compatibility is deliberately confined to this live
        // retry path. Startup reconciliation rejects markers without an
        // immutable task plan rather than guessing from mutable state.
        let mut instantiated = self.plan(request.clone())?;
        self.bind_instantiated_task_policies(&mut instantiated)?;
        let TeamInstantiation {
            binding,
            task_commands,
            ..
        } = instantiated;
        let revision = self
            .graphs
            .load(graph_id)
            .map_err(|error| error.to_string())?
            .revision;
        let binding = binding
            .ok_or_else(|| format!("Team graph `{graph_id}` has no compiled Team Binding"))?;
        self.persist_binding_markers(graph_id, &binding, &task_commands, revision)
    }

    /// Complete durable Team admission markers left in `Preparing` by a
    /// process crash.  The stored task plan was frozen with the Team Binding,
    /// so this never re-selects a template, re-parses a prompt, or invents a
    /// second Team.  The normal supervisor startup recovery owns execution
    /// after this method has closed the Task link set.
    pub(crate) fn reconcile_preparing_bindings_on_startup(
        &self,
        limit: usize,
    ) -> Result<usize, String> {
        let mut repaired = 0usize;
        let mut cursor = None;
        while repaired < limit {
            let page = self
                .graphs
                .graph_ids_page(cursor.take(), limit.saturating_sub(repaired).max(1))
                .map_err(|error| error.to_string())?;
            if page.is_empty() {
                break;
            }
            for (graph_id, _) in &page {
                if repaired >= limit {
                    break;
                }
                if crate::team_binding::has_ready_marker(&self.event_store, graph_id)? {
                    continue;
                }
                let Some(binding) = crate::team_binding::load_binding(&self.event_store, graph_id)?
                else {
                    continue;
                };
                let graph = self
                    .graphs
                    .load(graph_id)
                    .map_err(|error| error.to_string())?;
                if !graph.nodes.iter().any(|node| {
                    node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                }) {
                    continue;
                }
                let task_commands = crate::team_binding::load_prepared_task_commands(
                    &self.event_store,
                    graph_id,
                )?
                .ok_or_else(|| {
                    format!(
                        "Team graph `{graph_id}` has a Preparing binding without its durable Task link plan"
                    )
                })?;
                if task_commands.is_empty() {
                    return Err(format!(
                        "Team graph `{graph_id}` Preparing Task link plan is empty"
                    ));
                }
                self.admit_tasks(&task_commands, graph_id, graph.revision)?;
                crate::team_binding::persist_ready(
                    &self.event_store,
                    graph_id,
                    &binding.binding_digest,
                )?;
                repaired = repaired.saturating_add(1);
            }
            if page.len() < limit.saturating_sub(repaired).max(1) {
                break;
            }
            cursor = page
                .last()
                .map(|(graph_id, commit_cursor)| (*commit_cursor, graph_id.clone()));
        }
        Ok(repaired)
    }

    pub(crate) fn bind_instantiated_task_policies(
        &self,
        instantiated: &mut crate::TeamInstantiation,
    ) -> Result<(), String> {
        for command in &mut instantiated.task_commands {
            let parent_task_id = command
                .parent_task_id
                .as_deref()
                .unwrap_or(instantiated.task_policy_parent_id.as_str());
            command.spec = self.tasks.bind_inherited_task_spec(
                parent_task_id,
                instantiated.task_permission_ceiling,
                command.spec.clone(),
            )?;
        }
        Ok(())
    }

    pub fn project(&self, graph_id: &str) -> Result<TeamProjection, String> {
        self.projection.project(graph_id)
    }

    /// Binding-attested Agent autonomy facade. The model supplies only a
    /// semantic operation and revision/token fence; Runtime derives identity,
    /// role, capabilities and current time from the canonical Team packet.
    pub async fn apply_collaboration_control(
        &self,
        request: CollaborationControlRequest,
    ) -> Result<serde_json::Value, String> {
        let (graph, packet) = self.bound_team_packet(&request.graph_id, &request.node_id)?;
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "collaboration control requires an immutable Agent Binding".to_string()
        })?;
        let role_id = packet
            .team_role_assignment()
            .map(|role| role.identity.role_id.clone());
        if request.operation == CollaborationControlOperation::Inspect {
            return Ok(collaboration_control_view(
                request.operation,
                &graph,
                &request.node_id,
                &binding.instance.instance_id,
                role_id.as_deref(),
                None,
                None,
                false,
            ));
        }
        let proposed_contract = if request.operation == CollaborationControlOperation::ProposeWork {
            let proposal = request.proposal.as_ref().ok_or_else(|| {
                "collaboration propose_work requires a bounded proposal".to_string()
            })?;
            let idempotency_key = proposal.idempotency_key.trim();
            if idempotency_key.is_empty() || idempotency_key.chars().count() > 160 {
                return Err(
                    "collaboration proposal idempotency_key must contain 1..160 characters"
                        .to_string(),
                );
            }
            let mut known_refs = packet
                .evidence_refs
                .iter()
                .flat_map(|reference| {
                    [
                        reference.evidence_ref.id.clone(),
                        reference.retrieval_selector.clone(),
                    ]
                })
                .filter(|value| !value.trim().is_empty())
                .collect::<BTreeSet<_>>();
            for result in graph.node_results.values() {
                known_refs.extend(result.result_ref.iter().cloned());
                known_refs.extend(
                    result
                        .evidence_refs
                        .iter()
                        .map(|reference| reference.evidence_ref.id.clone()),
                );
            }
            if let Some(team_id) = packet.team_id() {
                if let Ok(board) = self.working_state_for_graph(team_id, &graph.id) {
                    known_refs.extend(board.entries.iter().flat_map(|entry| {
                        entry.refs.iter().chain(entry.artifact_refs.iter()).cloned()
                    }));
                }
            }
            if proposal
                .input_artifact_refs
                .iter()
                .chain(proposal.evidence_refs.iter())
                .any(|reference| !known_refs.contains(reference))
            {
                return Err(
                    "collaboration proposal references evidence or artifacts not held by the Team"
                        .to_string(),
                );
            }
            let eligible_peer_exists = graph.nodes.iter().any(|node| {
                node.id != request.node_id
                    && node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                    && agent_node_is_future_schedulable(&graph, &node.id)
                    && serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).is_ok_and(
                        |candidate| {
                            proposal.required_capabilities.iter().all(|required| {
                                candidate.allowed_tools.iter().any(|tool| tool == required)
                            })
                        },
                    )
            });
            if !eligible_peer_exists {
                return Err(
                    "collaboration proposal has no other schedulable eligible Agent in this Team"
                        .to_string(),
                );
            }
            let work_id = format!(
                "agent-work:{:x}",
                Sha256::digest(
                    format!(
                        "{}:{}:{idempotency_key}",
                        graph.id, binding.instance.instance_id
                    )
                    .as_bytes()
                )
            );
            let mut contract =
                harness_contract::execution_graph::ExecutionWorkContract::new(proposal.role);
            contract.collaboration_work_id = Some(work_id.clone());
            contract.objective = Some(proposal.objective.trim().to_string());
            contract.proposed_by = Some(binding.instance.instance_id.clone());
            contract.proposal_evidence_refs = proposal.evidence_refs.clone();
            contract.required_evidence_refs = proposal.evidence_refs.clone();
            contract.eligibility.required_capabilities = proposal.required_capabilities.clone();
            contract.input_artifact_refs = proposal.input_artifact_refs.clone();
            contract.output_artifact_kinds = proposal.output_artifact_kinds.clone();
            contract.review_policy =
                harness_contract::execution_graph::ExecutionWorkReviewPolicy::Peer {
                    minimum_reviewers: 1,
                    eligible_role_ids: Vec::new(),
                };
            contract.expected_input_tokens = proposal.expected_input_tokens;
            contract.expected_output_tokens = proposal.expected_output_tokens;
            contract.expected_duration_ms = proposal.expected_duration_ms;
            contract.scheduling_priority = proposal.scheduling_priority;
            Some((work_id, contract))
        } else {
            None
        };
        if let Some((work_id, contract)) = proposed_contract.as_ref() {
            if let Some(existing) = graph.autonomous_work.get(work_id) {
                if existing != contract {
                    return Err(format!(
                        "collaboration proposal idempotency key collides with different work `{work_id}`"
                    ));
                }
                return Ok(collaboration_control_view(
                    request.operation,
                    &graph,
                    &request.node_id,
                    &binding.instance.instance_id,
                    role_id.as_deref(),
                    Some(graph.revision),
                    None,
                    true,
                ));
            }
        }
        if request.operation == CollaborationControlOperation::Submit {
            let submission_ref = request
                .submission_ref
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "collaboration submit requires submission_ref".to_string())?;
            let durable_tool_ref = self
                .execution
                .load_delegated_agent_tool_receipts(&graph.id, &request.node_id, packet.attempt)?
                .into_iter()
                .any(|receipt| {
                    receipt.outcome.evidence_ref == submission_ref
                        || receipt.outcome.observed_evidence.iter().any(|evidence| {
                            evidence.evidence_ref.as_ref().is_some_and(|reference| {
                                reference.evidence_ref.id == submission_ref
                                    || reference.retrieval_selector == submission_ref
                            })
                        })
                });
            let durable_board_ref = submission_ref
                .strip_prefix("team-board:")
                .and_then(|entry_id| {
                    packet.team_id().and_then(|team_id| {
                        self.working_state_for_graph(team_id, &graph.id)
                            .ok()
                            .and_then(|board| {
                                board.entries.into_iter().find(|entry| {
                                    entry.entry_id == entry_id
                                        && entry.producer_instance_id
                                            == binding.instance.instance_id
                                })
                            })
                    })
                })
                .is_some();
            if !durable_tool_ref && !durable_board_ref {
                return Err(
                    "collaboration submission_ref must name this Agent's durable tool receipt or team-board entry"
                        .to_string(),
                );
            }
        }
        let expected_work_revision = if proposed_contract.is_some() {
            request.expected_work_revision.unwrap_or(0)
        } else {
            request.expected_work_revision.ok_or_else(|| {
                "collaboration control mutation requires expected_work_revision".to_string()
            })?
        };
        let work_node_id = proposed_contract
            .as_ref()
            .map(|(work_id, _)| work_id.clone())
            .or(request.work_node_id.clone())
            .unwrap_or_else(|| request.node_id.clone());
        if proposed_contract.is_none()
            && !graph.nodes.iter().any(|node| node.id == work_node_id)
            && !graph.autonomous_work.contains_key(&work_node_id)
        {
            return Err(format!(
                "collaboration control work node `{work_node_id}` is outside Team graph `{}`",
                graph.id
            ));
        }
        let mut receipt = None;
        // Unrelated work items share one durable graph stream. Retry only when
        // that stream advanced while this exact work item's semantic revision
        // remained unchanged. This preserves stale-work rejection and makes
        // independent claims work-conserving instead of globally conflicting.
        for _ in 0..16 {
            let current = self
                .graphs
                .load_async(graph.id.clone())
                .await
                .map_err(|error| error.to_string())?;
            if request
                .expected_revision
                .is_some_and(|hint| hint > current.revision)
            {
                return Err(format!(
                    "collaboration control graph revision {} is ahead of current {}",
                    request.expected_revision.unwrap_or_default(),
                    current.revision
                ));
            }
            let observed_work_revision = current
                .work_states
                .get(&work_node_id)
                .map_or(0, |state| state.revision);
            if observed_work_revision != expected_work_revision {
                return Err(format!(
                    "collaboration work revision mismatch for `{work_node_id}`: expected {expected_work_revision}, actual {observed_work_revision}"
                ));
            }
            if matches!(
                request.operation,
                CollaborationControlOperation::Heartbeat
                    | CollaborationControlOperation::Release
                    | CollaborationControlOperation::Submit
            ) && current
                .work_states
                .get(&work_node_id)
                .and_then(|state| state.claim.as_ref())
                .is_none_or(|claim| claim.claimant_instance_id != binding.instance.instance_id)
            {
                return Err(format!(
                    "collaboration {} requires the attested Agent to own work `{work_node_id}`",
                    serde_json::to_value(request.operation)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_string))
                        .unwrap_or_else(|| "mutation".to_string())
                ));
            }
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let command = match request.operation {
                CollaborationControlOperation::Inspect => unreachable!(),
                CollaborationControlOperation::ProposeWork => {
                    let (_, contract) = proposed_contract.as_ref().expect("proposal was validated");
                    harness_contract::execution_graph::ExecutionGraphCommand::ProposeWork {
                        expected_revision: current.revision,
                        work_id: work_node_id.clone(),
                        contract: Box::new(contract.clone()),
                    }
                }
                CollaborationControlOperation::Bid => {
                    harness_contract::execution_graph::ExecutionGraphCommand::BidWork {
                        expected_revision: current.revision,
                        work_id: work_node_id.clone(),
                        bidder_instance_id: binding.instance.instance_id.clone(),
                        bidder_role_id: role_id.clone(),
                        bidder_capabilities: packet.allowed_tools.clone(),
                        rationale: request
                            .rationale
                            .clone()
                            .ok_or_else(|| "collaboration bid requires rationale".to_string())?,
                        estimated_cost: request.estimated_cost.unwrap_or_default(),
                        bid_at_ms: now_ms,
                    }
                }
                CollaborationControlOperation::Claim => {
                    let expected_duration_ms = current
                        .autonomous_work
                        .get(&work_node_id)
                        .or_else(|| {
                            current
                                .nodes
                                .iter()
                                .find(|node| node.id == work_node_id)
                                .and_then(|node| node.work.as_ref())
                        })
                        .map_or(0, |work| work.expected_duration_ms);
                    let lease_duration_ms = effective_collaboration_lease_ms(
                        request.lease_duration_ms,
                        expected_duration_ms,
                    )?;
                    harness_contract::execution_graph::ExecutionGraphCommand::ClaimWork {
                        expected_revision: current.revision,
                        node_id: work_node_id.clone(),
                        claimant_instance_id: binding.instance.instance_id.clone(),
                        claimant_role_id: role_id.clone(),
                        claimant_capabilities: packet.allowed_tools.clone(),
                        claimed_at_ms: now_ms,
                        lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
                    }
                }
                CollaborationControlOperation::Heartbeat => {
                    let expected_duration_ms = current
                        .autonomous_work
                        .get(&work_node_id)
                        .or_else(|| {
                            current
                                .nodes
                                .iter()
                                .find(|node| node.id == work_node_id)
                                .and_then(|node| node.work.as_ref())
                        })
                        .map_or(0, |work| work.expected_duration_ms);
                    let lease_duration_ms = effective_collaboration_lease_ms(
                        request.lease_duration_ms,
                        expected_duration_ms,
                    )?;
                    harness_contract::execution_graph::ExecutionGraphCommand::HeartbeatWork {
                        expected_revision: current.revision,
                        node_id: work_node_id.clone(),
                        claim_token: request.claim_token.clone().ok_or_else(|| {
                            "collaboration heartbeat requires claim_token".to_string()
                        })?,
                        heartbeat_at_ms: now_ms,
                        lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
                    }
                }
                CollaborationControlOperation::Release => {
                    harness_contract::execution_graph::ExecutionGraphCommand::ReleaseWork {
                        expected_revision: current.revision,
                        node_id: work_node_id.clone(),
                        claim_token: request.claim_token.clone().ok_or_else(|| {
                            "collaboration release requires claim_token".to_string()
                        })?,
                        reason: request
                            .finding
                            .clone()
                            .unwrap_or_else(|| "released by Agent".to_string()),
                    }
                }
                CollaborationControlOperation::Submit => {
                    harness_contract::execution_graph::ExecutionGraphCommand::SubmitWork {
                        expected_revision: current.revision,
                        node_id: work_node_id.clone(),
                        claim_token: request.claim_token.clone().ok_or_else(|| {
                            "collaboration submit requires claim_token".to_string()
                        })?,
                        submitted_at_ms: now_ms,
                        submission_ref: request.submission_ref.clone().ok_or_else(|| {
                            "collaboration submit requires submission_ref".to_string()
                        })?,
                    }
                }
                CollaborationControlOperation::Accept => {
                    harness_contract::execution_graph::ExecutionGraphCommand::AcceptWork {
                        expected_revision: current.revision,
                        node_id: work_node_id.clone(),
                        reviewer_instance_id: binding.instance.instance_id.clone(),
                        reviewer_role_id: role_id.clone(),
                        reviewed_at_ms: now_ms,
                    }
                }
                CollaborationControlOperation::Challenge => {
                    harness_contract::execution_graph::ExecutionGraphCommand::ChallengeWork {
                        expected_revision: current.revision,
                        node_id: work_node_id.clone(),
                        reviewer_instance_id: binding.instance.instance_id.clone(),
                        reviewer_role_id: role_id.clone(),
                        finding: request.finding.clone().ok_or_else(|| {
                            "collaboration challenge requires finding".to_string()
                        })?,
                        reviewed_at_ms: now_ms,
                    }
                }
            };
            match self.execution.command_graph(&graph.id, command).await {
                Ok(committed) => {
                    receipt = Some(committed);
                    break;
                }
                Err(error) => {
                    let fresh = self
                        .graphs
                        .load_async(graph.id.clone())
                        .await
                        .map_err(|load_error| load_error.to_string())?;
                    let fresh_work_revision = fresh
                        .work_states
                        .get(&work_node_id)
                        .map_or(0, |state| state.revision);
                    if fresh.revision <= current.revision
                        || fresh_work_revision != expected_work_revision
                    {
                        return Err(error.to_string());
                    }
                }
            }
        }
        let receipt = receipt.ok_or_else(|| {
            format!("collaboration control contention budget exhausted for `{work_node_id}`")
        })?;
        let committed_graph = self
            .graphs
            .load_async(graph.id.clone())
            .await
            .map_err(|error| error.to_string())?;
        let caller_claim_token = committed_graph
            .work_states
            .get(&work_node_id)
            .and_then(|state| state.claim.as_ref())
            .filter(|claim| claim.claimant_instance_id == binding.instance.instance_id)
            .map(|claim| claim.claim_token.clone());
        Ok(collaboration_control_view(
            request.operation,
            &committed_graph,
            &request.node_id,
            &binding.instance.instance_id,
            role_id.as_deref(),
            Some(receipt.accepted_revision),
            caller_claim_token,
            false,
        ))
    }

    /// Rebuild the team-local collaboration projection from events committed
    /// atomically with graph transitions. The graph itself still owns node
    /// status and topology; this view contains only shareable semantics.
    pub fn working_state(&self, team_id: &str) -> Result<crate::TeamWorkingState, String> {
        let team = self
            .list()?
            .into_iter()
            .find(|team| team.team_id == team_id)
            .ok_or_else(|| format!("team not found: {team_id}"))?;
        self.working_state_for_graph(team_id, &team.graph_id)
    }

    /// Rebuild collaboration state for the exact Team run that a caller is
    /// validating. This graph-qualified path prevents an unrelated corrupt or
    /// historical Team projection from poisoning a newly completed run.
    pub fn working_state_for_graph(
        &self,
        team_id: &str,
        graph_id: &str,
    ) -> Result<crate::TeamWorkingState, String> {
        let team = self.projection.project(graph_id)?;
        if team.team_id != team_id {
            return Err(format!(
                "team identity mismatch for graph {graph_id}: expected {team_id}, found {}",
                team.team_id
            ));
        }
        let events = self
            .event_store
            .list_stream(&format!("team-working-state:{team_id}"))?;
        Ok(crate::TeamWorkingState::from_events(
            team_id, graph_id, events,
        ))
    }

    /// Append a bounded semantic update from an immutable Team Agent binding.
    /// The caller cannot choose its team, role, producer or authority scope.
    pub async fn publish_working_state(
        &self,
        request: TeamWorkingStatePublishRequest,
    ) -> Result<TeamWorkingState, String> {
        let (graph, packet) = self.bound_team_packet(&request.graph_id, &request.node_id)?;
        let team_id = packet
            .team_id()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "team board publish requires a Team-bound Agent".to_string())?;
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| "team board publish requires an immutable Agent Binding".to_string())?;
        let role = packet.team_role_assignment().ok_or_else(|| {
            "team board publish requires the frozen typed Team role assignment".to_string()
        })?;
        let summary = request.summary.trim();
        if summary.is_empty() || summary.chars().count() > 16_000 {
            return Err("team board summary must contain 1..16000 characters".to_string());
        }
        let normalized = summary.to_ascii_lowercase();
        if normalized.contains("<thinking")
            || normalized.contains("chain-of-thought")
            || normalized.contains("internal reasoning trace")
        {
            return Err(
                "team board accepts semantic findings, not private reasoning traces".to_string(),
            );
        }
        let stream_id = format!("team-working-state:{team_id}");
        let entry_id = format!(
            "{}:{}:{:016x}",
            request.graph_id,
            request.node_id,
            model_protocol::fingerprint::stable_hash_bytes(
                serde_json::to_string(&request)
                    .map_err(|error| error.to_string())?
                    .as_bytes(),
            )
        );
        let current_revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        if current_revision != request.expected_revision {
            let existing = self.working_state_for_graph(team_id, &request.graph_id)?;
            if existing
                .entries
                .iter()
                .any(|entry| entry.entry_id == entry_id)
            {
                return Ok(existing);
            }
            return Err(format!(
                "team board revision mismatch: expected {}, actual {current_revision}",
                request.expected_revision
            ));
        }
        if let Some(thread) = &request.thread {
            if thread.thread_id.trim().is_empty() {
                return Err("team discussion thread id is empty".to_string());
            }
            let current = self.working_state_for_graph(team_id, &request.graph_id)?;
            if thread.reply_to_entry_id.as_ref().is_some_and(|entry_id| {
                !current.entries.iter().any(|entry| {
                    entry.entry_id == *entry_id
                        && entry
                            .thread
                            .as_ref()
                            .is_some_and(|parent| parent.thread_id == thread.thread_id)
                })
            }) {
                return Err(
                    "team discussion reply target is missing or belongs to another thread"
                        .to_string(),
                );
            }
            if thread.resolves_entry_ids.iter().any(|entry_id| {
                !current
                    .entries
                    .iter()
                    .any(|entry| entry.entry_id == *entry_id)
            }) {
                return Err("team discussion resolution references a missing entry".to_string());
            }
            if request.kind == TeamWorkingStateKind::Resolution
                && thread.resolves_entry_ids.is_empty()
            {
                return Err(
                    "team discussion resolution must resolve at least one entry".to_string()
                );
            }
        }
        let mut refs = request
            .refs
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .take(64)
            .collect::<Vec<_>>();
        refs.sort();
        refs.dedup();
        let mut artifact_refs = request
            .artifact_refs
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .take(64)
            .collect::<Vec<_>>();
        artifact_refs.sort();
        artifact_refs.dedup();
        let entry = TeamWorkingStateEntry {
            entry_id: entry_id.clone(),
            team_id: team_id.to_string(),
            graph_id: graph.id.clone(),
            node_id: request.node_id.clone(),
            producer_instance_id: binding.instance.instance_id.clone(),
            role_id: Some(role.identity.role_id.clone()),
            focus_id: Some(role.identity.focus_id.clone()),
            focus_scope_hash: Some(role.identity.focus_scope_hash.clone()),
            overlap_budget_bp: Some(role.identity.overlap_budget_bp),
            novelty_target_bp: Some(role.identity.novelty_target_bp),
            focus_resource_scopes: packet.resource_scopes.clone(),
            observed_resource_scopes: Vec::new(),
            kind: request.kind,
            summary: summary.to_string(),
            refs,
            artifact_refs,
            boundary: "runtime semantic checkpoint; no raw chain-of-thought or raw tool output"
                .to_string(),
            confidence_milli: 1_000,
            graph_revision: graph.revision,
            revision: current_revision.saturating_add(1),
            source_generation: graph
                .orchestration
                .as_ref()
                .map(|metadata| metadata.source_generation)
                .or_else(|| graph.lineage.as_ref().map(|lineage| lineage.generation))
                .unwrap_or(1),
            visibility: request.visibility,
            thread: request.thread,
        };
        let agent_activity_id = format!("activity:execution:{}:node:{}", graph.id, request.node_id);
        let discussion_event = RuntimeEventInput {
            stream_id: stream_id.clone(),
            scope: RuntimeEventScope::Team,
            kind: "team.working_state.appended.v1".to_string(),
            status: Some("committed".to_string()),
            actor: Some(binding.instance.instance_id.clone()),
            refs: vec![
                RuntimeEventRef {
                    kind: "team_run".to_string(),
                    id: team_id.to_string(),
                },
                RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: graph.id.clone(),
                },
                RuntimeEventRef {
                    kind: "execution_node".to_string(),
                    id: request.node_id.clone(),
                },
            ],
            payload: serde_json::to_value(entry).map_err(|error| error.to_string())?,
        }
        .with_activity_binding(harness_contract::projection::RuntimeActivityBinding {
            root_execution_id: graph.id.clone(),
            session_id: packet.assignment.session_id.clone(),
            turn_id: graph
                .lineage
                .as_ref()
                .map(|lineage| lineage.turn_id.clone())
                .unwrap_or_else(|| packet.assignment.run_id.clone()),
            root_task_id: packet.assignment.root_task_id.clone(),
            task_id: packet.assignment.task_id.clone(),
            activity_id: format!("activity:execution:{}:discussion:{entry_id}", graph.id),
            node_id: Some(request.node_id.clone()),
            parent_activity_id: Some(agent_activity_id.clone()),
            initiator_activity_id: Some(agent_activity_id),
            team_run_id: Some(team_id.to_string()),
            agent_instance_id: Some(binding.instance.instance_id.clone()),
            agent_run_id: Some(packet.assignment.run_id.clone()),
            skill_id: None,
            skill_revision: None,
            skill_activation_id: None,
            tool_contract_id: None,
            tool_call_id: None,
            approval_id: None,
            parallel_group_id: None,
            revision: current_revision.saturating_add(1),
            fence: graph.revision.max(1),
            generation: graph
                .lineage
                .as_ref()
                .map_or(1, |lineage| lineage.generation),
        })
        .map_err(|error| error.to_string())?;
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("team-board:{entry_id}"),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: current_revision,
                }],
                events: vec![RuntimeTransactionEventInput {
                    event: discussion_event,
                    idempotency_key: Some(entry_id),
                    schema_version: 1,
                }],
            })
            .map_err(|error| error.to_string())?;
        let committed = self.working_state_for_graph(team_id, &graph.id)?;
        self.notify_team_board_revision(
            &graph,
            &packet,
            team_id,
            committed.board_revision,
            summary,
        )
        .await;
        Ok(committed)
    }

    /// Read only entries visible to the immutable caller binding.
    pub fn read_working_state(
        &self,
        request: TeamWorkingStateReadRequest,
    ) -> Result<TeamWorkingState, String> {
        if request.after_revision.is_some() && request.exact_revision.is_some() {
            return Err(
                "team board read accepts after_revision or exact_revision, not both".to_string(),
            );
        }
        let (_, packet) = self.bound_team_packet(&request.graph_id, &request.node_id)?;
        let team_id = packet
            .team_id()
            .ok_or_else(|| "team board read requires a Team-bound Agent".to_string())?;
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| "team board read requires an immutable Agent Binding".to_string())?;
        let role_id = packet
            .team_role_assignment()
            .ok_or_else(|| {
                "team board read requires the frozen typed Team role assignment".to_string()
            })?
            .identity
            .role_id
            .clone();
        let mut state = self.working_state_for_graph(team_id, &request.graph_id)?;
        state.entries.retain(|entry| {
            let visible = match entry.visibility {
                TeamWorkingStateVisibility::Team => true,
                TeamWorkingStateVisibility::Role => entry.role_id.as_deref() == Some(&role_id),
                TeamWorkingStateVisibility::Private => {
                    entry.producer_instance_id == binding.instance.instance_id
                }
            };
            visible
                && request
                    .after_revision
                    .is_none_or(|revision| entry.revision > revision)
                && request
                    .exact_revision
                    .is_none_or(|revision| entry.revision == revision)
        });
        Ok(state)
    }

    /// Read from the caller's durable inbox cursor. Offline/non-running peers
    /// replay the same committed semantic messages when they next reach a
    /// safe Runtime checkpoint; no process-local mailbox is involved.
    pub fn read_working_state_from_cursor(
        &self,
        graph_id: String,
        node_id: String,
    ) -> Result<TeamWorkingState, String> {
        let cursor = self.working_state_cursor(&graph_id, &node_id)?;
        self.read_working_state(TeamWorkingStateReadRequest {
            graph_id,
            node_id,
            after_revision: Some(cursor.through_revision),
            exact_revision: None,
        })
    }

    pub fn working_state_cursor(
        &self,
        graph_id: &str,
        node_id: &str,
    ) -> Result<TeamWorkingStateCursor, String> {
        let (_, packet) = self.bound_team_packet(graph_id, node_id)?;
        let team_id = packet
            .team_id()
            .ok_or_else(|| "team cursor requires a Team-bound Agent".to_string())?;
        let instance_id = packet
            .binding
            .as_ref()
            .ok_or_else(|| "team cursor requires an immutable Agent Binding".to_string())?
            .instance
            .instance_id
            .clone();
        let stream_id = format!("team-working-state-cursor:{team_id}:{instance_id}");
        let events = self
            .event_store
            .list_stream(&stream_id)
            .map_err(|error| error.to_string())?;
        let mut cursor = events
            .last()
            .and_then(|event| {
                serde_json::from_value::<TeamWorkingStateCursor>(event.payload.clone()).ok()
            })
            .unwrap_or(TeamWorkingStateCursor {
                team_id: team_id.to_string(),
                graph_id: graph_id.to_string(),
                agent_instance_id: instance_id,
                through_revision: 0,
                cursor_revision: 0,
            });
        cursor.cursor_revision = events.last().map_or(0, |event| event.sequence);
        Ok(cursor)
    }

    pub fn acknowledge_working_state(
        &self,
        request: TeamWorkingStateAcknowledgeRequest,
    ) -> Result<TeamWorkingStateCursor, String> {
        let (_, packet) = self.bound_team_packet(&request.graph_id, &request.node_id)?;
        let team_id = packet
            .team_id()
            .ok_or_else(|| "team cursor requires a Team-bound Agent".to_string())?;
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| "team cursor requires an immutable Agent Binding".to_string())?;
        let current = self.working_state_cursor(&request.graph_id, &request.node_id)?;
        let board = self.working_state_for_graph(team_id, &request.graph_id)?;
        if current.cursor_revision != request.expected_cursor_revision {
            return Err(format!(
                "team cursor revision mismatch: expected {}, actual {}",
                request.expected_cursor_revision, current.cursor_revision
            ));
        }
        if request.through_revision < current.through_revision
            || request.through_revision > board.board_revision
        {
            return Err(format!(
                "team cursor must advance monotonically within board revision {}",
                board.board_revision
            ));
        }
        let stream_id = format!(
            "team-working-state-cursor:{team_id}:{}",
            binding.instance.instance_id
        );
        let cursor = TeamWorkingStateCursor {
            team_id: team_id.to_string(),
            graph_id: request.graph_id.clone(),
            agent_instance_id: binding.instance.instance_id.clone(),
            through_revision: request.through_revision,
            cursor_revision: current.cursor_revision.saturating_add(1),
        };
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!(
                    "team-cursor:{}:{}:{}",
                    team_id, binding.instance.instance_id, request.through_revision
                ),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: current.cursor_revision,
                }],
                events: vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
                        scope: RuntimeEventScope::Team,
                        kind: "team.working_state.cursor_advanced.v1".to_string(),
                        status: Some("committed".to_string()),
                        actor: Some(binding.instance.instance_id.clone()),
                        refs: vec![RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: request.graph_id,
                        }],
                        payload: serde_json::to_value(&cursor)
                            .map_err(|error| error.to_string())?,
                    },
                    idempotency_key: Some(format!(
                        "team-cursor:{}:{}:{}",
                        team_id, binding.instance.instance_id, request.through_revision
                    )),
                    schema_version: 1,
                }],
            })
            .map_err(|error| error.to_string())?;
        Ok(cursor)
    }

    fn bound_team_packet(
        &self,
        graph_id: &str,
        node_id: &str,
    ) -> Result<
        (
            harness_contract::execution_graph::ExecutionGraph,
            AgentTaskPacket,
        ),
        String,
    > {
        let graph = self
            .graphs
            .load(graph_id)
            .map_err(|error| error.to_string())?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or_else(|| format!("team graph node not found: {node_id}"))?;
        if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
            return Err("team board caller must be an AgentTask node".to_string());
        }
        let packet = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
            .map_err(|error| format!("team board caller packet is invalid: {error}"))?;
        Ok((graph, packet))
    }

    async fn notify_team_board_revision(
        &self,
        graph: &harness_contract::execution_graph::ExecutionGraph,
        source: &AgentTaskPacket,
        team_id: &str,
        revision: u64,
        summary: &str,
    ) {
        let source_agent_id = source.agent_id();
        for node in &graph.nodes {
            if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
                continue;
            }
            let Ok(peer) = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref) else {
                continue;
            };
            if peer.agent_id() == source_agent_id || peer.team_id() != Some(team_id) {
                continue;
            }
            let Some(snapshot) = self.agents.get(peer.agent_id()) else {
                continue;
            };
            if snapshot.status != AgentStatus::Running {
                continue;
            }
            let receipt = self
                .agents
                .command(AgentCommandRequest {
                    command_id: format!(
                        "team-board-notify:{team_id}:{revision}:{}",
                        peer.agent_id()
                    ),
                    agent_id: peer.agent_id().to_string(),
                    expected_revision: snapshot.revision,
                    command: AgentCommand::SendInput,
                    input: Some(AgentInput::PeerMessage {
                        from_agent_id: source_agent_id.to_string(),
                        message: format!(
                            "Team board revision {revision} is available. At the next safe checkpoint call `team_board` with `read_after` using your last consumed revision. Semantic preview: {}",
                            summary.chars().take(512).collect::<String>()
                        ),
                    }),
                })
                .await;
            if !receipt.accepted {
                tracing::debug!(
                    team_id,
                    revision,
                    peer_agent_id = %peer.agent_id(),
                    reason = ?receipt.reject_reason,
                    "Team board revision notification was not accepted by the peer Agent"
                );
            }
        }
    }

    /// Import and retire the removed pre-V5 Team state file. Unbound active
    /// records are kept as durable blocked audit events rather than resumed.
    pub fn import_legacy_state_file(
        &self,
        path: &std::path::Path,
    ) -> Result<Option<LegacyTeamImportReport>, String> {
        crate::team_legacy_import::import_legacy_team_state_file(
            Arc::clone(&self.event_store),
            path,
        )
    }

    /// Retire legacy browser-owned Team Profiles without pretending that their
    /// mutable role names are valid TeamTemplate revisions.
    pub fn archive_legacy_profile_file(
        &self,
        source_path: &std::path::Path,
        archive_root: &std::path::Path,
    ) -> Result<Option<LegacyTeamProfileMigrationReport>, String> {
        crate::team_profile_migration::archive_legacy_team_profile_file(
            Arc::clone(&self.event_store),
            source_path,
            archive_root,
        )
    }

    /// Enumerate canonical team graphs. A team is recognized strictly by the
    /// typed AgentTask payload, so unrelated ExecutionGraphs never leak into
    /// Team projections.
    pub fn list(&self) -> Result<Vec<TeamProjection>, String> {
        self.projection.list()
    }

    pub fn list_page(
        &self,
        after: Option<TeamProjectionCursor>,
        limit: usize,
    ) -> Result<TeamProjectionPage, String> {
        self.projection.list_page(after, limit)
    }

    #[must_use]
    pub fn projection_json(&self) -> serde_json::Value {
        let teams = self.list().unwrap_or_default();
        let quarantined = self.projection.quarantined().unwrap_or_default();
        serde_json::json!({
            "kind": "runtime.teams",
            "teams": teams.into_iter().map(|team| serde_json::json!({
                "team_id": team.team_id,
                "session_id": team.session_id,
                "graph_id": team.graph_id,
                "graph_revision": team.graph_revision,
                "status": team.status,
                "display_label": team.display_label,
                "agents": team.tasks,
                "agent_displays": team.agent_displays,
                "terminal_result": team.terminal_result,
                "working_state": self
                    .working_state_for_graph(&team.team_id, &team.graph_id)
                    .ok(),
            })).collect::<Vec<_>>(),
            "quarantined": quarantined,
        })
    }
}

#[cfg(test)]
mod collaboration_market_tests {
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionNodeStatus,
    };

    use super::{
        agent_node_is_actionable, agent_node_is_future_schedulable,
        effective_collaboration_lease_ms,
    };

    #[test]
    fn planned_successors_are_not_advertised_as_actionable_peers() {
        let mut graph = ExecutionGraph::new("serial Team");
        let mut planned = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        planned.id = "planned-peer".to_string();
        graph.nodes.push(planned);
        let mut predecessor =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        predecessor.id = "active-predecessor".to_string();
        graph.nodes.push(predecessor);
        graph.edges.push(ExecutionEdge {
            from: "active-predecessor".to_string(),
            to: "planned-peer".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph
            .node_statuses
            .insert("planned-peer".to_string(), ExecutionNodeStatus::Planned);
        graph.node_statuses.insert(
            "active-predecessor".to_string(),
            ExecutionNodeStatus::Running,
        );

        assert!(!agent_node_is_actionable(&graph, "planned-peer"));
        assert!(agent_node_is_future_schedulable(&graph, "planned-peer"));
        graph
            .node_statuses
            .insert("planned-peer".to_string(), ExecutionNodeStatus::Ready);
        assert!(agent_node_is_actionable(&graph, "planned-peer"));
    }

    #[test]
    fn future_peer_rejects_irrecoverable_dependency_ancestry() {
        let mut graph = ExecutionGraph::new("failed serial Team");
        for id in ["failed-root", "planned-middle", "planned-peer"] {
            let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
            node.id = id.to_string();
            graph.nodes.push(node);
        }
        graph.edges.push(ExecutionEdge {
            from: "failed-root".to_string(),
            to: "planned-middle".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.edges.push(ExecutionEdge {
            from: "planned-middle".to_string(),
            to: "planned-peer".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph
            .node_statuses
            .insert("failed-root".to_string(), ExecutionNodeStatus::Failed);
        graph
            .node_statuses
            .insert("planned-middle".to_string(), ExecutionNodeStatus::Planned);
        graph
            .node_statuses
            .insert("planned-peer".to_string(), ExecutionNodeStatus::Planned);

        assert!(!agent_node_is_future_schedulable(&graph, "planned-peer"));
    }

    #[test]
    fn collaboration_lease_covers_the_declared_work_duration() {
        assert_eq!(
            effective_collaboration_lease_ms(Some(60_000), 900_000).expect("lease"),
            900_000
        );
        assert_eq!(
            effective_collaboration_lease_ms(None, 30_000).expect("default lease"),
            900_000
        );
        assert!(effective_collaboration_lease_ms(Some(3_600_001), 1).is_err());
    }
}
