//! Graph-owned Team facade.
//!
//! TeamRuntime compiles collaboration semantics and reads projections. It owns
//! no scheduler, worker, state file, or process-global registry.

use std::sync::Arc;

use harness_contract::{
    reality::EvidenceRef,
    team::{TeamInstantiationRequest, TeamTemplateRevisionRef},
};

use crate::execution_core::graph::ExecutionResourceManager;

use crate::{
    AgentRuntime, EvolutionGovernanceService, ExecutionGraphHostReceipt, ExecutionGraphStateStore,
    LegacyTeamImportReport, LegacyTeamProfileMigrationReport, MissionRuntime,
    RuntimeDefinitionRegistry, RuntimeEventStore, RuntimeExecutionSupervisor, TaskRuntimePort,
    TeamProjection, TeamProjectionReader,
};

pub struct TeamRuntime {
    execution: Arc<RuntimeExecutionSupervisor>,
    instantiation: crate::TeamInstantiationService,
    projection: TeamProjectionReader,
    event_store: Arc<RuntimeEventStore>,
    tasks: TaskRuntimePort,
    missions: Arc<MissionRuntime>,
}

impl TeamRuntime {
    #[must_use]
    pub fn mission_id_for_session_or_default(&self, session_id: &str) -> String {
        self.missions
            .mission_id_for_session(session_id)
            .unwrap_or_else(|| self.missions.default_mission_id().to_string())
    }

    #[must_use]
    pub fn new(
        execution: Arc<RuntimeExecutionSupervisor>,
        graphs: ExecutionGraphStateStore,
        agents: Arc<AgentRuntime>,
        event_store: Arc<RuntimeEventStore>,
        definition_registry: Arc<RuntimeDefinitionRegistry>,
        resources: Arc<ExecutionResourceManager>,
        evolution_governance: Arc<EvolutionGovernanceService>,
        workspace_id: impl Into<String>,
        tasks: TaskRuntimePort,
        missions: Arc<MissionRuntime>,
    ) -> Self {
        Self {
            execution,
            instantiation: crate::TeamInstantiationService::new(
                definition_registry,
                resources,
                evolution_governance,
                workspace_id,
            ),
            projection: TeamProjectionReader::new(graphs, agents),
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

    /// Instantiate a durable Team Template through the canonical Runtime
    /// request. The returned graph already contains exact Agent Bindings, so
    /// Runner never selects a profile or template while executing.
    pub async fn instantiate(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<TeamProjection, String> {
        let mission_id = request.mission_id.clone();
        let team_id = request.team_id.clone();
        let instantiated = self.plan(request)?;
        self.instantiation.validate_release(&instantiated)?;
        let graph_id = instantiated.graph.id.clone();
        let registered = self
            .execution
            .register_graph(instantiated.graph)
            .await
            .map_err(|error| error.to_string())?;
        self.admit_tasks(
            &instantiated.task_commands,
            &registered.id,
            registered.revision,
        )?;
        self.tasks.link_mission_team_run(
            &mission_id,
            &team_id,
            vec![EvidenceRef::new(
                "team_run",
                format!("team-run://{team_id}?graph={}", registered.id),
            )],
        )?;
        self.execution
            .drive_registered(&registered.id)
            .await
            .map_err(|error| error.to_string())?;
        self.projection.project(&graph_id)
    }

    /// Admit a Team graph and return after durable submission. The supervisor
    /// owns all Agent/model/tool execution after this boundary.
    pub async fn admit(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<ExecutionGraphHostReceipt, String> {
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
        mission_id: &str,
        team_id: &str,
        instantiated: crate::TeamInstantiation,
    ) -> Result<String, String> {
        self.instantiation.validate_release(&instantiated)?;
        let registered = self
            .execution
            .register_graph(instantiated.graph)
            .await
            .map_err(|error| error.to_string())?;
        self.admit_tasks(
            &instantiated.task_commands,
            &registered.id,
            registered.revision,
        )?;
        self.tasks.link_mission_team_run(
            mission_id,
            team_id,
            vec![EvidenceRef::new(
                "team_run",
                format!("team-run://{team_id}?graph={}", registered.id),
            )],
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
        let instantiated = match candidate_revision {
            Some(revision) => {
                self.instantiation
                    .instantiate_evaluation(request, revision, allowed_tools)?
            }
            None => self
                .instantiation
                .instantiate_evaluation_baseline(request, allowed_tools)?,
        };
        let graph_id = instantiated.graph.id.clone();
        let registered = self
            .execution
            .register_graph(instantiated.graph)
            .await
            .map_err(|error| error.to_string())?;
        self.admit_tasks(
            &instantiated.task_commands,
            &registered.id,
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
            let task = self.tasks.create(command.clone())?;
            self.tasks.link_existing_graph(
                &task.task_id,
                graph_id,
                graph_revision,
                vec![EvidenceRef::new(
                    "execution_graph",
                    format!("execution-graph://{graph_id}?revision={graph_revision}"),
                )],
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
        let events = self
            .event_store
            .list_stream(&format!("team-working-state:{team_id}"))?;
        Ok(crate::TeamWorkingState::from_events(
            team_id,
            team.graph_id,
            events,
        ))
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

    #[must_use]
    pub fn projection_json(&self) -> serde_json::Value {
        let teams = self.list().unwrap_or_default();
        serde_json::json!({
            "kind": "runtime.teams",
            "teams": teams.into_iter().map(|team| serde_json::json!({
                "team_id": team.team_id,
                "session_id": team.session_id,
                "graph_id": team.graph_id,
                "graph_revision": team.graph_revision,
                "status": team.status,
                "agents": team.tasks,
                "terminal_result": team.terminal_result,
                "working_state": self.working_state(&team.team_id).ok(),
            })).collect::<Vec<_>>(),
        })
    }
}
