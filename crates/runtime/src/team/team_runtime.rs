//! Graph-owned Team facade.
//!
//! TeamRuntime compiles collaboration semantics and reads projections. It owns
//! no scheduler, worker, state file, or process-global registry.

use std::sync::Arc;

use harness_contract::{
    agent::{AgentCommand, AgentCommandRequest, AgentInput, AgentStatus, AgentTaskPacket},
    reality::EvidenceRef,
    team::{TeamInstantiationRequest, TeamTemplateRevisionRef},
};

use crate::execution_core::ExecutionStateStoreError;
use crate::{
    AgentRuntime, AppendTransactionRequest, EvolutionGovernanceService, ExecutionGraphHostReceipt,
    ExecutionGraphStateStore, ExpectedStreamRevision, LegacyTeamImportReport,
    LegacyTeamProfileMigrationReport, MissionRuntime, RuntimeDefinitionRegistry, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, RuntimeEventStore, RuntimeExecutionSupervisor,
    RuntimeTransactionEventInput, TaskRuntimePort, TeamInstantiation, TeamProjection,
    TeamProjectionCursor, TeamProjectionPage, TeamProjectionReader, TeamWorkingState,
    TeamWorkingStateEntry, TeamWorkingStatePublishRequest, TeamWorkingStateReadRequest,
    TeamWorkingStateVisibility,
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
                .map_or(graph.revision, |metadata| metadata.source_generation),
            visibility: request.visibility,
        };
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("team-board:{entry_id}"),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: current_revision,
                }],
                events: vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
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
                                id: request.node_id,
                            },
                        ],
                        payload: serde_json::to_value(entry).map_err(|error| error.to_string())?,
                    },
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
