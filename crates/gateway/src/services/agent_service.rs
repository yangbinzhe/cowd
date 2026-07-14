use serde_json::Value;

use super::{AgentService, ServiceEnvelope, TaskService};

mod graph;

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

    /// Render the Runtime-owned Definition projection. Gateway must not
    /// discover agent files or apply name-shadowing rules of its own.
    pub(crate) fn catalog(&self, runtime: &runtime::RuntimeServices) -> Value {
        let agents = runtime.agent_runtime().catalog().all();
        serde_json::json!({
            "kind": "agents",
            "action": "list",
            "count": agents.len(),
            "summary": {
                "total": agents.len(),
                "runnable": agents.len(),
            },
            "agents": agents,
            "source": "runtime.definition_catalog",
        })
    }

    pub(crate) fn directory(&self, runtime: &runtime::RuntimeServices) -> Value {
        let catalog = self.catalog(runtime);
        serde_json::json!({
            "kind": "agents.directory",
            "agents": catalog.get("agents").cloned().unwrap_or_else(|| serde_json::json!([])),
            "summary": catalog.get("summary").cloned().unwrap_or_else(|| serde_json::json!({})),
            "source": "runtime.definition_catalog",
        })
    }

    pub(crate) fn discover(&self, runtime: &runtime::RuntimeServices, task: &str) -> Value {
        let agents = runtime.agent_runtime().catalog().search(task.trim());
        serde_json::json!({
            "kind": "agents",
            "action": "discover",
            "task": task.trim(),
            "count": agents.len(),
            "agents": agents,
            "source": "runtime.definition_catalog",
        })
    }

    pub(crate) fn assemble(&self, runtime: &runtime::RuntimeServices, task: &str) -> Value {
        let task = task.trim();
        let discovery = self.discover(runtime, task);
        let agents = discovery
            .get("agents")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let team = agents.first().map(|leader| {
            serde_json::json!({
                "leader": leader,
                "workers": agents.iter().skip(1).take(4).collect::<Vec<_>>(),
                "selection": "candidate_only",
            })
        });
        serde_json::json!({
            "kind": "agents.assemble",
            "task": task,
            "agents": agents,
            "team": team,
            "source": "runtime.definition_catalog",
        })
    }

    /// Immutable, environment-bucketed self-models derive exclusively from
    /// terminal Agent run evidence. They replace mutable instance reputation
    /// as the operational feedback projection.
    pub(crate) fn self_models(&self, runtime: &runtime::RuntimeServices) -> Value {
        let models = runtime.agent_self_models();
        serde_json::json!({
            "kind": "agents.self_models",
            "items": models,
            "summary": {
                "total": models.len(),
                "runs": models.iter().map(|model| model.run_count).sum::<u64>(),
                "successful_runs": models.iter().map(|model| model.success_count).sum::<u64>(),
            },
            "source": "runtime.agent_run_evaluations",
        })
    }
}
