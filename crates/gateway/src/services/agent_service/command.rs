use super::*;

pub(super) fn normalize_agent_command_args(args: Option<&str>) -> Option<&str> {
    args.map(str::trim).filter(|value| !value.is_empty())
}

pub(super) fn agent_usage_json(unexpected: Option<&str>) -> Value {
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

pub(super) fn render_agent_command_text(value: &Value) -> String {
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
