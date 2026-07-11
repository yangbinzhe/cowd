use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{AgentService, ServiceEnvelope, TaskService};

mod command;
mod graph;

use command::*;
use graph::*;

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
        crate::agent_static::agent_catalog_json(workspace_root)
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
        crate::agent_static::agent_discovery_json(workspace_root, task.trim())
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
