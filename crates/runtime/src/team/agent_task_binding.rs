//! Runtime-owned binding between lifecycle agents and team task/workgraph nodes.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::{AgentTask, AgentTaskStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTaskBindingStatus {
    Bound,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskBinding {
    pub agent_id: String,
    pub team_id: String,
    pub session_id: String,
    pub role_id: String,
    pub task_id: String,
    pub workgraph_node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_mode: Option<String>,
    pub status: AgentTaskBindingStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Default)]
pub struct AgentTaskBindingRegistry {
    bindings_by_agent: Mutex<BTreeMap<String, AgentTaskBinding>>,
}

impl AgentTaskBindingRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind_task(
        &self,
        agent_id: impl Into<String>,
        task: &AgentTask,
        workgraph_node_id: impl Into<String>,
        backend_mode: Option<String>,
    ) -> AgentTaskBinding {
        let now = now_ms();
        let binding = AgentTaskBinding {
            agent_id: agent_id.into(),
            team_id: task.team_id.clone(),
            session_id: task.session_id.clone(),
            role_id: task.role_id.clone(),
            task_id: task.task_id.clone(),
            workgraph_node_id: workgraph_node_id.into(),
            backend_mode,
            status: status_from_task(task.status),
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.bind(binding)
    }

    pub fn bind(&self, mut binding: AgentTaskBinding) -> AgentTaskBinding {
        let mut bindings = self
            .bindings_by_agent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = bindings.get(&binding.agent_id) {
            binding.created_at_ms = existing.created_at_ms;
        }
        binding.updated_at_ms = now_ms();
        bindings.insert(binding.agent_id.clone(), binding.clone());
        binding
    }

    #[must_use]
    pub fn get_by_agent(&self, agent_id: &str) -> Option<AgentTaskBinding> {
        self.bindings_by_agent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .cloned()
    }

    #[must_use]
    pub fn get_by_task(&self, task_id: &str) -> Option<AgentTaskBinding> {
        self.bindings_by_agent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .find(|binding| binding.task_id == task_id)
            .cloned()
    }

    #[must_use]
    pub fn list_for_team(&self, team_id: &str) -> Vec<AgentTaskBinding> {
        self.bindings_by_agent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|binding| binding.team_id == team_id)
            .cloned()
            .collect()
    }

    pub fn mark_task_status(
        &self,
        agent_id: &str,
        task_status: AgentTaskStatus,
    ) -> Option<AgentTaskBinding> {
        self.mark_status(agent_id, status_from_task(task_status))
    }

    pub fn mark_status(
        &self,
        agent_id: &str,
        status: AgentTaskBindingStatus,
    ) -> Option<AgentTaskBinding> {
        let mut bindings = self
            .bindings_by_agent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let binding = bindings.get_mut(agent_id)?;
        binding.status = status;
        binding.updated_at_ms = now_ms();
        Some(binding.clone())
    }
}

pub fn global_agent_task_binding_registry() -> &'static AgentTaskBindingRegistry {
    static REGISTRY: OnceLock<AgentTaskBindingRegistry> = OnceLock::new();
    REGISTRY.get_or_init(AgentTaskBindingRegistry::new)
}

fn status_from_task(status: AgentTaskStatus) -> AgentTaskBindingStatus {
    match status {
        AgentTaskStatus::Pending | AgentTaskStatus::Claimed => AgentTaskBindingStatus::Bound,
        AgentTaskStatus::Running => AgentTaskBindingStatus::Running,
        AgentTaskStatus::Completed => AgentTaskBindingStatus::Completed,
        AgentTaskStatus::Failed => AgentTaskBindingStatus::Failed,
        AgentTaskStatus::Cancelled => AgentTaskBindingStatus::Cancelled,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
