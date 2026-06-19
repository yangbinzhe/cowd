use std::{fs, path::Path};

use app_mfg::{
    server_manufacturing_skill_pack, skill_agent_node_id, MfgIncident, MfgSkillPlan, MfgSkillRun,
};
use matrix_core::MatrixEvidencePacket;
use memory::store::session::SessionRecord;
use memory::RuntimeEventScope;
use runtime::{AgentNodeStatus, AgentRole, AgentRunGraph, AgentTaskNode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::task_kernel::TaskRecord;

use super::{AgentService, GatewayServices, ServiceEnvelope};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct AgentTeamProfile {
    pub(crate) id: String,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) objective: String,
    #[serde(default)]
    pub(crate) leader: Option<String>,
    #[serde(default)]
    pub(crate) members: Vec<String>,
    #[serde(default)]
    pub(crate) policy: Value,
    #[serde(default)]
    pub(crate) evaluation: Value,
    #[serde(default)]
    pub(crate) reputation: Value,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Deserialize)]
pub(crate) struct UpsertAgentTeamProfileRequest {
    #[serde(default)]
    pub(crate) id: Option<String>,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) objective: String,
    #[serde(default)]
    pub(crate) leader: Option<String>,
    #[serde(default)]
    pub(crate) members: Vec<String>,
    #[serde(default)]
    pub(crate) policy: Value,
    #[serde(default)]
    pub(crate) evaluation: Value,
}

impl AgentService {
    pub(crate) fn list(&self) -> ServiceEnvelope {
        self.envelope("list")
    }

    pub(crate) fn task_projection(&self) -> ServiceEnvelope {
        self.envelope("task_projection")
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.list(), self.task_projection()]
    }

    pub(crate) fn catalog(&self, workspace_root: &Path) -> std::io::Result<Value> {
        crate::slash_catalog::agent_catalog_json(workspace_root)
    }

    pub(crate) fn directory(&self, workspace_root: &Path) -> std::io::Result<Value> {
        let catalog = self.catalog(workspace_root)?;
        let agents = catalog
            .get("agents")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(serde_json::json!({
            "kind": "agents.directory",
            "agents": agents,
            "summary": catalog.get("summary").cloned().unwrap_or_else(|| serde_json::json!({})),
            "source": "agents.catalog",
        }))
    }

    pub(crate) fn discover(&self, workspace_root: &Path, task: &str) -> std::io::Result<Value> {
        crate::slash_catalog::agent_discovery_json(workspace_root, task.trim())
    }

    pub(crate) fn command_json(
        &self,
        workspace_root: &Path,
        args: Option<&str>,
    ) -> std::io::Result<Value> {
        match normalize_agent_command_args(args) {
            None | Some("list") => self.catalog(workspace_root),
            Some(args) if args.starts_with("discover") => {
                let task = args.strip_prefix("discover").unwrap_or("").trim();
                if task.is_empty() {
                    return Ok(agent_usage_json(Some("discover")));
                }
                self.discover(workspace_root, task)
            }
            Some("help") | Some("-h") | Some("--help") => Ok(agent_usage_json(None)),
            Some(args) => Ok(agent_usage_json(Some(args))),
        }
    }

    pub(crate) fn command_text(
        &self,
        workspace_root: &Path,
        args: Option<&str>,
    ) -> std::io::Result<String> {
        let value = self.command_json(workspace_root, args)?;
        Ok(render_agent_command_text(&value))
    }

    pub(crate) fn assemble(&self, workspace_root: &Path, task: &str) -> std::io::Result<Value> {
        let task = task.trim();
        let discovery = self.discover(workspace_root, task)?;
        Ok(serde_json::json!({
            "kind": "agents.assemble",
            "task": task,
            "agents": discovery.get("agents").cloned().unwrap_or_else(|| serde_json::json!([])),
            "team": discovery.get("team").cloned().unwrap_or_else(|| serde_json::json!(null)),
            "source": "agents.discover",
        }))
    }

    pub(crate) fn reputation(&self, workspace_root: &Path) -> std::io::Result<Value> {
        let catalog = self.catalog(workspace_root)?;
        let agents = catalog
            .get("agents")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let reputation: Vec<Value> = agents
            .iter()
            .map(|agent| {
                serde_json::json!({
                    "agent_id": agent.get("id").or_else(|| agent.get("name")).cloned().unwrap_or_else(|| serde_json::json!("unknown")),
                    "name": agent.get("name").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
                    "reputation": agent.get("reputation").cloned().unwrap_or_else(|| serde_json::json!(null)),
                    "status": agent.get("status").or_else(|| agent.get("active")).cloned().unwrap_or_else(|| serde_json::json!("unknown")),
                })
            })
            .collect();
        Ok(serde_json::json!({
            "kind": "agents.reputation",
            "items": reputation,
            "summary": {
                "total": agents.len(),
                "scored": reputation.iter().filter(|item| !item.get("reputation").unwrap_or(&Value::Null).is_null()).count(),
            },
        }))
    }

    pub(crate) fn team_profiles_path(&self, workspace_root: &Path) -> std::path::PathBuf {
        workspace_root
            .join(".cowd")
            .join("agents")
            .join("team-profiles.json")
    }

    pub(crate) fn list_team_profiles(
        &self,
        workspace_root: &Path,
    ) -> Result<Vec<AgentTeamProfile>, String> {
        let path = self.team_profiles_path(workspace_root);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let text = fs::read_to_string(&path)
            .map_err(|error| format!("failed to read team profiles: {error}"))?;
        serde_json::from_str(&text)
            .map_err(|error| format!("failed to parse team profiles: {error}"))
    }

    pub(crate) fn get_team_profile(
        &self,
        workspace_root: &Path,
        id: &str,
    ) -> Result<Option<AgentTeamProfile>, String> {
        Ok(self
            .list_team_profiles(workspace_root)?
            .into_iter()
            .find(|profile| profile.id == id))
    }

    pub(crate) fn create_team_profile(
        &self,
        workspace_root: &Path,
        body: UpsertAgentTeamProfileRequest,
    ) -> Result<AgentTeamProfile, String> {
        let mut profiles = self.list_team_profiles(workspace_root)?;
        let profile = build_team_profile(body, None)?;
        if profiles.iter().any(|existing| existing.id == profile.id) {
            return Err("team profile id already exists".to_string());
        }
        profiles.push(profile.clone());
        self.save_team_profiles(workspace_root, &profiles)?;
        Ok(profile)
    }

    pub(crate) fn update_team_profile(
        &self,
        workspace_root: &Path,
        id: &str,
        body: UpsertAgentTeamProfileRequest,
    ) -> Result<Option<AgentTeamProfile>, String> {
        let mut profiles = self.list_team_profiles(workspace_root)?;
        let Some(index) = profiles.iter().position(|profile| profile.id == id) else {
            return Ok(None);
        };
        let profile = build_team_profile(body, Some(&profiles[index]))?;
        profiles[index] = profile.clone();
        self.save_team_profiles(workspace_root, &profiles)?;
        Ok(Some(profile))
    }

    pub(crate) fn delete_team_profile(
        &self,
        workspace_root: &Path,
        id: &str,
    ) -> Result<bool, String> {
        let mut profiles = self.list_team_profiles(workspace_root)?;
        let before = profiles.len();
        profiles.retain(|profile| profile.id != id);
        if profiles.len() == before {
            return Ok(false);
        }
        self.save_team_profiles(workspace_root, &profiles)?;
        Ok(true)
    }

    fn save_team_profiles(
        &self,
        workspace_root: &Path,
        profiles: &[AgentTeamProfile],
    ) -> Result<(), String> {
        let path = self.team_profiles_path(workspace_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create team profile directory: {error}"))?;
        }
        let text = serde_json::to_string_pretty(profiles)
            .map_err(|error| format!("failed to serialize team profiles: {error}"))?;
        fs::write(&path, text).map_err(|error| format!("failed to write team profiles: {error}"))
    }
}

fn normalize_agent_command_args(args: Option<&str>) -> Option<&str> {
    args.map(str::trim).filter(|value| !value.is_empty())
}

fn agent_usage_json(unexpected: Option<&str>) -> Value {
    serde_json::json!({
        "kind": "agents",
        "action": "help",
        "usage": {
            "slash_command": "/agents [list|discover <task>|help]",
            "sources": [".cowd/agents", "~/.cowd/agents", "$CC_CONFIG_HOME/agents"],
        },
        "unexpected": unexpected,
    })
}

fn render_agent_command_text(value: &Value) -> String {
    match value.get("action").and_then(Value::as_str) {
        Some("list") => render_agent_catalog_text(value),
        Some("discover") => render_agent_discovery_text(value),
        _ => render_agent_usage_text(value),
    }
}

fn render_agent_catalog_text(value: &Value) -> String {
    let agents = value
        .get("agents")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if agents.is_empty() {
        return "No agents found.".to_string();
    }
    let active = value
        .get("summary")
        .and_then(|summary| summary.get("active"))
        .and_then(Value::as_u64)
        .unwrap_or_else(|| agents.iter().filter(|agent| is_active_agent(agent)).count() as u64);
    let mut lines = vec![
        "Agents".to_string(),
        format!("  {active} active agents"),
        String::new(),
    ];
    for scope in ["Project roots", "User config roots", "User home roots"] {
        let group = agents
            .iter()
            .filter(|agent| source_label(agent) == Some(scope))
            .collect::<Vec<_>>();
        if group.is_empty() {
            continue;
        }
        lines.push(format!("{scope}:"));
        for agent in group {
            let detail = agent_detail_text(agent);
            if let Some(winner) = agent
                .get("shadowed_by")
                .and_then(|source| source.get("label"))
                .and_then(Value::as_str)
            {
                lines.push(format!("  (shadowed by {winner}) {detail}"));
            } else {
                lines.push(format!("  {detail}"));
            }
        }
        lines.push(String::new());
    }
    lines.join("\n").trim_end().to_string()
}

fn render_agent_discovery_text(value: &Value) -> String {
    let task = value
        .get("task")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let agents = value
        .get("agents")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if agents.is_empty() {
        return format!(
            "No agents matched the task: \"{task}\"\n\nRegister agents with relevant capabilities first."
        );
    }
    let mut lines = vec![format!(
        "Discovered {} agent(s) for \"{task}\"",
        agents.len()
    )];
    lines.push(String::new());
    for (index, agent) in agents.iter().enumerate() {
        let name = agent
            .get("agent_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let terms = agent
            .get("capabilities")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let source = source_label(agent).unwrap_or("unknown");
        lines.push(format!("  {}. {name} ({source}) - [{terms}]", index + 1));
    }
    if let Some(team) = value.get("team").filter(|team| !team.is_null()) {
        if let Some(leader) = team
            .get("leader")
            .and_then(|leader| leader.get("agent_id"))
            .and_then(Value::as_str)
        {
            lines.push(String::new());
            lines.push("Auto-assembled team:".to_string());
            lines.push(format!("  Leader: {leader}"));
            let workers = team
                .get("workers")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if workers.is_empty() {
                lines.push("  Workers: none".to_string());
            } else {
                lines.push("  Workers:".to_string());
                for worker in workers {
                    if let Some(worker_id) = worker.get("agent_id").and_then(Value::as_str) {
                        lines.push(format!("    - {worker_id}"));
                    }
                }
            }
        }
    }
    lines.join("\n")
}

fn render_agent_usage_text(value: &Value) -> String {
    let mut lines = vec![
        "Agents".to_string(),
        "  Usage            /agents [list|discover <task>|help]".to_string(),
        "  Sources          .cowd/agents, ~/.cowd/agents, $CC_CONFIG_HOME/agents".to_string(),
    ];
    if let Some(unexpected) = value.get("unexpected").and_then(Value::as_str) {
        lines.push(format!("  Unexpected       {unexpected}"));
    }
    lines.join("\n")
}

fn source_label(agent: &Value) -> Option<&str> {
    agent.get("source")?.get("label")?.as_str()
}

fn is_active_agent(agent: &Value) -> bool {
    agent
        .get("active")
        .and_then(Value::as_bool)
        .or_else(|| {
            agent
                .get("status")
                .and_then(Value::as_str)
                .map(|status| status == "active")
        })
        .unwrap_or(false)
}

fn agent_detail_text(agent: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(name) = agent.get("name").and_then(Value::as_str) {
        parts.push(name.to_string());
    }
    if let Some(description) = agent.get("description").and_then(Value::as_str) {
        parts.push(description.to_string());
    }
    if let Some(model) = agent.get("model").and_then(Value::as_str) {
        parts.push(model.to_string());
    }
    if let Some(reasoning) = agent.get("reasoning_effort").and_then(Value::as_str) {
        parts.push(reasoning.to_string());
    }
    if parts.is_empty() {
        "unknown".to_string()
    } else {
        parts.join(" · ")
    }
}

fn normalize_team_profile_id(value: &str) -> String {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if normalized.is_empty() {
        format!("team-{}", now_ms())
    } else {
        normalized
    }
}

fn build_team_profile(
    body: UpsertAgentTeamProfileRequest,
    existing: Option<&AgentTeamProfile>,
) -> Result<AgentTeamProfile, String> {
    if body.name.trim().is_empty() {
        return Err("team profile name is required".to_string());
    }
    let created_at_ms = existing
        .map(|profile| profile.created_at_ms)
        .unwrap_or_else(now_ms);
    let id = existing
        .map(|profile| profile.id.clone())
        .or_else(|| body.id.clone())
        .unwrap_or_else(|| body.name.clone());
    let mut reputation = existing
        .map(|profile| profile.reputation.clone())
        .unwrap_or_else(|| serde_json::json!({}));
    if reputation.is_null() {
        reputation = serde_json::json!({});
    }
    Ok(AgentTeamProfile {
        id: normalize_team_profile_id(&id),
        name: body.name.trim().to_string(),
        objective: body.objective.trim().to_string(),
        leader: body.leader.filter(|leader| !leader.trim().is_empty()),
        members: body
            .members
            .into_iter()
            .map(|member| member.trim().to_string())
            .filter(|member| !member.is_empty())
            .collect(),
        policy: body.policy,
        evaluation: body.evaluation,
        reputation,
        created_at_ms,
        updated_at_ms: now_ms(),
    })
}

impl GatewayServices {
    pub(crate) async fn enrich_mfg_evidence_agent_graph(
        &self,
        task: &TaskRecord,
        packet: &MatrixEvidencePacket,
    ) -> Result<(TaskRecord, AgentRunGraph), String> {
        let mut graph = task.agent_graph.clone().unwrap_or_else(|| {
            AgentRunGraph::from_objective(task.id.clone(), task.objective.clone())
        });
        enrich_mfg_evidence_agent_graph(&mut graph, packet).map_err(|error| error.to_string())?;
        let task = self.task.upsert_agent_graph(&task.id, graph.clone())?;
        self.append_mfg_agent_runtime_event(&task, &graph).await?;
        Ok((task, graph))
    }

    pub(crate) async fn plan_mfg_skill_agent_nodes(
        &self,
        incident: &MfgIncident,
        plan: &MfgSkillPlan,
    ) -> Result<Option<AgentRunGraph>, String> {
        let Some(task_id) = incident.task_id.as_deref() else {
            return Ok(None);
        };
        let Some(mut graph) = self.task.agent_graph(task_id)? else {
            return Ok(None);
        };
        let now = now_ms();
        let dependency = if graph.nodes.iter().any(|node| node.id == "mfg_reviewer") {
            "mfg_reviewer"
        } else {
            "planner"
        };
        for skill in &plan.selected_skills {
            let node_id = skill_agent_node_id(&skill.skill_id);
            ensure_agent_node(
                &mut graph,
                AgentTaskNode {
                    id: node_id.clone(),
                    role: AgentRole::Researcher,
                    title: skill.role.clone(),
                    objective: skill.analysis_method.clone(),
                    depends_on: vec![dependency.to_string()],
                    status: AgentNodeStatus::Pending,
                    assigned_agent: Some(skill.skill_id.clone()),
                    result: None,
                    error: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                },
            )
            .map_err(|error| error.to_string())?;
            graph
                .add_evidence(
                    &node_id,
                    "mfg_skill_manifest",
                    format!("mfg:skill:{}", skill.skill_id),
                    format!(
                        "inputs={}, metrics={}, evidence={}",
                        skill.input_fact_types.join(","),
                        skill.input_metric_keys.join(","),
                        skill.required_evidence.join(",")
                    ),
                )
                .map_err(|error| error.to_string())?;
        }
        let task = self.task.upsert_agent_graph(task_id, graph.clone())?;
        self.append_mfg_agent_runtime_event(&task, &graph).await?;
        Ok(Some(graph))
    }

    pub(crate) async fn complete_mfg_skill_agent_node(
        &self,
        incident: &MfgIncident,
        run: &MfgSkillRun,
    ) -> Result<Option<AgentRunGraph>, String> {
        let Some(task_id) = incident.task_id.as_deref() else {
            return Ok(None);
        };
        let Some(mut graph) = self.task.agent_graph(task_id)? else {
            return Ok(None);
        };
        let node_id = run
            .agent_node_id
            .clone()
            .unwrap_or_else(|| skill_agent_node_id(&run.skill_id));
        if !graph.nodes.iter().any(|node| node.id == node_id) {
            let skill = server_manufacturing_skill_pack()
                .into_iter()
                .find(|skill| skill.skill_id == run.skill_id)
                .ok_or_else(|| format!("MFG skill {} not found", run.skill_id))?;
            let now = now_ms();
            ensure_agent_node(
                &mut graph,
                AgentTaskNode {
                    id: node_id.clone(),
                    role: AgentRole::Researcher,
                    title: skill.role,
                    objective: skill.analysis_method,
                    depends_on: vec!["planner".to_string()],
                    status: AgentNodeStatus::Pending,
                    assigned_agent: Some(run.skill_id.clone()),
                    result: None,
                    error: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                },
            )
            .map_err(|error| error.to_string())?;
        }
        if let Some(node) = graph.nodes.iter_mut().find(|node| node.id == node_id) {
            node.status = AgentNodeStatus::Completed;
            node.result = Some(run.summary.clone());
            node.updated_at_ms = now_ms();
        }
        graph
            .add_evidence(
                &node_id,
                "mfg_skill_run",
                format!("mfg:skill-run:{}:{}", incident.incident_id, run.skill_id),
                run.summary.clone(),
            )
            .map_err(|error| error.to_string())?;
        let task = self.task.upsert_agent_graph(task_id, graph.clone())?;
        self.append_mfg_agent_runtime_event(&task, &graph).await?;
        Ok(Some(graph))
    }

    async fn append_mfg_agent_runtime_event(
        &self,
        task: &TaskRecord,
        graph: &AgentRunGraph,
    ) -> Result<(), String> {
        self.ensure_mfg_task_session_record(task).await?;
        self.session
            .append_runtime_event(
                &task.id,
                RuntimeEventScope::Workgraph,
                "mfg.agent_graph.updated",
                serde_json::json!({ "graph": graph }),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn ensure_mfg_task_session_record(&self, task: &TaskRecord) -> Result<(), String> {
        let now = chrono::Utc::now().to_rfc3339();
        let metadata_json = serde_json::json!({
            "kind": "mfg.incident.task",
            "task_id": task.id,
            "objective": task.objective,
            "yolo_mode": task.yolo_mode,
            "current_phase": task.current_phase,
        })
        .to_string();
        let record = SessionRecord {
            session_id: task.id.clone(),
            platform: "mfg".to_string(),
            chat_id: task.id.clone(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: task.audit.len() as i64,
            reset_policy: "none".to_string(),
            metadata_json: Some(metadata_json),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: task.status.as_str().to_string(),
        };
        self.session
            .upsert_stored_session(&record)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn list_agent_graphs(
        &self,
        _state: &crate::api_routes::AppState,
    ) -> serde_json::Value {
        let runs = self.task.list_agent_graphs().unwrap_or_default();
        serde_json::json!({
            "kind": "agent_run_graphs",
            "runs": runs,
        })
    }

    pub(crate) fn agent_graph(
        &self,
        _state: &crate::api_routes::AppState,
        task_id: &str,
    ) -> Option<AgentRunGraph> {
        self.task.agent_graph(task_id).ok().flatten()
    }

    pub(crate) async fn upsert_agent_graph(
        &self,
        _state: &crate::api_routes::AppState,
        task_id: &str,
        objective: Option<String>,
        nodes: Vec<serde_json::Value>,
    ) -> Result<AgentRunGraph, String> {
        let objective = objective
            .or_else(|| {
                self.task
                    .agent_graph(task_id)
                    .ok()
                    .flatten()
                    .map(|graph| graph.objective)
            })
            .unwrap_or_else(|| "agent run".to_string());
        let mut graph = AgentRunGraph::new(task_id.to_string(), objective);
        for node in nodes {
            let node: AgentTaskNode = serde_json::from_value(node)
                .map_err(|error| format!("invalid agent graph node: {error}"))?;
            graph.add_node(node).map_err(|error| error.to_string())?;
        }
        graph
            .validate_acyclic()
            .map_err(|error| error.to_string())?;
        let task = self
            .task
            .upsert_agent_graph(task_id, graph.clone())
            .map_err(|error| error.to_string())?;
        self.session
            .append_runtime_event(
                &task.id,
                RuntimeEventScope::Workgraph,
                "agent.run_graph.updated",
                serde_json::json!({ "graph": graph }),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(graph)
    }
}

fn enrich_mfg_evidence_agent_graph(
    graph: &mut AgentRunGraph,
    packet: &MatrixEvidencePacket,
) -> Result<(), runtime::AgentGraphError> {
    let now = now_ms();
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "mfg_researcher".to_string(),
            role: AgentRole::Researcher,
            title: "MFG Evidence Research".to_string(),
            objective: "Validate MFG evidence packet and identify missing evidence".to_string(),
            depends_on: vec!["planner".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("mfg_researcher".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "mfg_reviewer".to_string(),
            role: AgentRole::Reviewer,
            title: "MFG Insight Review".to_string(),
            objective: "Review confidence, conflicts, and governance readiness".to_string(),
            depends_on: vec!["mfg_researcher".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("mfg_reviewer".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    ensure_agent_node(
        graph,
        AgentTaskNode {
            id: "mfg_merger".to_string(),
            role: AgentRole::Merger,
            title: "MFG Decision Merge".to_string(),
            objective: "Merge agent findings into one governed operating decision".to_string(),
            depends_on: vec!["mfg_reviewer".to_string()],
            status: AgentNodeStatus::Pending,
            assigned_agent: Some("mfg_merger".to_string()),
            result: None,
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        },
    )?;
    let reference = format!("mfg:evidence:{}", packet.packet_id);
    graph.add_evidence(
        "planner",
        "structured_evidence_packet",
        reference.clone(),
        packet.problem_statement.clone(),
    )?;
    graph.add_evidence(
        "mfg_researcher",
        "structured_evidence_packet",
        reference,
        format!(
            "metric_evidence={}, change_evidence={}, missing_evidence={}",
            packet.metric_evidence.len(),
            packet.change_evidence.len(),
            packet.missing_evidence.len()
        ),
    )?;
    Ok(())
}

fn ensure_agent_node(
    graph: &mut AgentRunGraph,
    node: AgentTaskNode,
) -> Result<(), runtime::AgentGraphError> {
    if graph.nodes.iter().any(|existing| existing.id == node.id) {
        return Ok(());
    }
    graph.add_node(node)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
