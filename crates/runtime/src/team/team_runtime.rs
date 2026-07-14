//! Graph-owned Team facade.
//!
//! TeamRuntime compiles collaboration semantics and reads projections. It owns
//! no scheduler, worker, state file, or process-global registry.

use std::sync::Arc;

use harness_contract::team::{TeamInstantiationRequest, TeamTemplateRevisionRef};

use crate::execution_core::graph::ExecutionResourceManager;

use crate::{
    AgentRuntime, EvolutionGovernanceService, ExecutionGraphRunner, ExecutionGraphStateStore,
    LegacyTeamImportReport, LegacyTeamProfileMigrationReport, RuntimeDefinitionRegistry,
    RuntimeEventStore, TeamProjection, TeamProjectionReader,
};

pub struct TeamRuntime {
    runner: Arc<ExecutionGraphRunner>,
    instantiation: crate::TeamInstantiationService,
    projection: TeamProjectionReader,
    event_store: Arc<RuntimeEventStore>,
}

impl TeamRuntime {
    #[must_use]
    pub fn new(
        runner: Arc<ExecutionGraphRunner>,
        graphs: ExecutionGraphStateStore,
        agents: Arc<AgentRuntime>,
        event_store: Arc<RuntimeEventStore>,
        definition_registry: Arc<RuntimeDefinitionRegistry>,
        resources: Arc<ExecutionResourceManager>,
        evolution_governance: Arc<EvolutionGovernanceService>,
    ) -> Self {
        Self {
            runner,
            instantiation: crate::TeamInstantiationService::new(
                definition_registry,
                resources,
                evolution_governance,
            ),
            projection: TeamProjectionReader::new(graphs, agents),
            event_store,
        }
    }

    /// Resolve a durable Team request into its immutable graph without
    /// scheduling it. Evaluation and dry-run callers use this same canonical
    /// compiler; no second graph builder exists outside Runtime.
    pub fn plan(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<crate::TeamInstantiation, String> {
        self.instantiation.instantiate(request)
    }

    /// Instantiate a durable Team Template through the canonical Runtime
    /// request. The returned graph already contains exact Agent Bindings, so
    /// Runner never selects a profile or template while executing.
    pub async fn instantiate(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<TeamProjection, String> {
        let instantiated = self.plan(request)?;
        self.instantiation.validate_release(&instantiated)?;
        let graph_id = instantiated.graph.id.clone();
        self.runner
            .start(instantiated.graph)
            .await
            .map_err(|error| error.to_string())?;
        self.projection.project(&graph_id)
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
        self.runner
            .start(instantiated.graph)
            .await
            .map_err(|error| error.to_string())?;
        self.projection.project(&graph_id)
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
