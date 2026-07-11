//! Graph-owned Team facade.
//!
//! TeamRuntime compiles collaboration semantics and reads projections. It owns
//! no scheduler, worker, state file, or process-global registry.

use std::collections::BTreeMap;
use std::sync::Arc;

use harness_contract::team::{TeamRoleSpec, TeamTemplateId};

use crate::{
    AgentRuntime, AgentSelector, CollaborationLiftGate, CollaborationLiftInput,
    ExecutionGraphRunner, ExecutionGraphStateStore, LegacyTeamImportReport, RuntimeEventStore,
    TeamBuildRequest, TeamBuilder, TeamProjection, TeamProjectionReader, TeamRoleDependency,
};

#[derive(Debug, Clone)]
pub struct StartTeamRequest {
    pub team_id: String,
    pub session_id: String,
    pub objective: String,
    pub template_id: TeamTemplateId,
    pub roles: Vec<TeamRoleSpec>,
    pub role_dependencies: Vec<TeamRoleDependency>,
    pub lift_input: CollaborationLiftInput,
    pub permission_lease: String,
    pub model_lease: String,
    pub backend_constraint: Option<String>,
}

pub struct TeamRuntime {
    runner: Arc<ExecutionGraphRunner>,
    builder: TeamBuilder,
    selector: AgentSelector,
    lift_gate: CollaborationLiftGate,
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
    ) -> Self {
        Self {
            runner,
            builder: TeamBuilder,
            selector: AgentSelector::new(Arc::clone(agents.catalog()), Arc::clone(&agents)),
            lift_gate: CollaborationLiftGate,
            projection: TeamProjectionReader::new(graphs, agents),
            event_store,
        }
    }

    pub async fn start(&self, request: StartTeamRequest) -> Result<TeamProjection, String> {
        let verdict = self.lift_gate.decide(&request.lift_input);
        if !verdict.accepted {
            return Err(format!(
                "team lift rejected: {}",
                verdict.reasons.join("; ")
            ));
        }
        let selected_agent_profiles = request
            .roles
            .iter()
            .filter_map(|role| {
                self.selector
                    .select(&role.required_capabilities)
                    .map(|agent| (role.role_id.clone(), agent.agent_id))
            })
            .collect::<BTreeMap<_, _>>();
        let build = self.builder.build(TeamBuildRequest {
            team_id: request.team_id,
            session_id: request.session_id,
            objective: request.objective,
            template_id: request.template_id,
            roles: request.roles,
            role_dependencies: request.role_dependencies,
            selected_agent_profiles,
            verdict,
            permission_lease: request.permission_lease,
            model_lease: request.model_lease,
            backend_constraint: request.backend_constraint,
        })?;
        let graph_id = build.graph.id.clone();
        self.runner
            .start(build.graph)
            .await
            .map_err(|error| error.to_string())?;
        self.projection.project(&graph_id)
    }

    pub fn project(&self, graph_id: &str) -> Result<TeamProjection, String> {
        self.projection.project(graph_id)
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
            })).collect::<Vec<_>>(),
        })
    }
}
