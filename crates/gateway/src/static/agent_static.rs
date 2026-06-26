use std::collections::BTreeMap;
use std::{env, fs, path::Path, path::PathBuf};

use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DefinitionSource {
    ProjectCowd,
    ProjectCodex,
    ProjectClaude,
    UserCowdConfigHome,
    UserCodexHome,
    UserCowd,
    UserCodex,
    UserClaude,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentSummary {
    name: String,
    description: Option<String>,
    model: Option<String>,
    reasoning_effort: Option<String>,
    source: DefinitionSource,
    shadowed_by: Option<DefinitionSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticAgentMatch {
    name: String,
    description: Option<String>,
    source: DefinitionSource,
    shadowed_by: Option<DefinitionSource>,
    match_terms: Vec<String>,
    score: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticAgentTeam {
    leader: StaticAgentMatch,
    workers: Vec<StaticAgentMatch>,
}

pub(crate) fn agent_catalog_json(cwd: &Path) -> std::io::Result<Value> {
    let roots = discover_definition_roots(cwd, "agents");
    let agents = load_agents_from_roots(&roots)?;
    Ok(render_agents_report_json(cwd, &agents))
}

pub(crate) fn agent_discovery_json(cwd: &Path, task: &str) -> std::io::Result<Value> {
    let roots = discover_definition_roots(cwd, "agents");
    let agents = load_agents_from_roots(&roots)?;
    let ranked = discover_agents_for_task(&agents, task);
    let agents_json = ranked
        .iter()
        .map(|agent| {
            json!({
                "agent_id": agent.name,
                "role": agent.description,
                "capabilities": agent.match_terms,
                "reputation": null,
                "status": if agent.shadowed_by.is_some() { "shadowed" } else { "active" },
                "source": definition_source_json(agent.source),
            })
        })
        .collect::<Vec<_>>();
    let team_json = assemble_static_agent_team(&ranked).map(|team| {
        json!({
            "leader": { "agent_id": team.leader.name, "role": team.leader.description },
            "workers": team.workers.iter().map(|worker| json!({
                "agent_id": worker.name,
                "role": worker.description,
            })).collect::<Vec<_>>(),
        })
    });
    Ok(json!({
        "kind": "agents",
        "action": "discover",
        "task": task,
        "count": ranked.len(),
        "agents": agents_json,
        "team": team_json,
    }))
}

fn discover_definition_roots(cwd: &Path, leaf: &str) -> Vec<(DefinitionSource, PathBuf)> {
    let mut roots = Vec::new();
    for ancestor in cwd.ancestors() {
        push_unique_root(
            &mut roots,
            DefinitionSource::ProjectCowd,
            ancestor.join(".cowd").join(leaf),
        );
        push_unique_root(
            &mut roots,
            DefinitionSource::ProjectCodex,
            ancestor.join(".codex").join(leaf),
        );
        push_unique_root(
            &mut roots,
            DefinitionSource::ProjectClaude,
            ancestor.join(".claude").join(leaf),
        );
    }
    if let Ok(config_home) = env::var("COWD_CONFIG_HOME") {
        push_unique_root(
            &mut roots,
            DefinitionSource::UserCowdConfigHome,
            PathBuf::from(config_home).join(leaf),
        );
    }
    if let Ok(codex_home) = env::var("CODEX_HOME") {
        push_unique_root(
            &mut roots,
            DefinitionSource::UserCodexHome,
            PathBuf::from(codex_home).join(leaf),
        );
    }
    if let Some(home) = env::var_os("HOME") {
        let home = PathBuf::from(home);
        push_unique_root(
            &mut roots,
            DefinitionSource::UserCowd,
            home.join(".cowd").join(leaf),
        );
        push_unique_root(
            &mut roots,
            DefinitionSource::UserCodex,
            home.join(".codex").join(leaf),
        );
        push_unique_root(
            &mut roots,
            DefinitionSource::UserClaude,
            home.join(".claude").join(leaf),
        );
    }
    roots
}

fn push_unique_root(
    roots: &mut Vec<(DefinitionSource, PathBuf)>,
    source: DefinitionSource,
    path: PathBuf,
) {
    if path.is_dir() && !roots.iter().any(|(_, existing)| existing == &path) {
        roots.push((source, path));
    }
}

fn load_agents_from_roots(
    roots: &[(DefinitionSource, PathBuf)],
) -> std::io::Result<Vec<AgentSummary>> {
    let mut agents = Vec::new();
    let mut active_sources = BTreeMap::<String, DefinitionSource>::new();
    for (source, root) in roots {
        let mut root_agents = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if entry.path().extension().is_none_or(|ext| ext != "toml") {
                continue;
            }
            let contents = fs::read_to_string(entry.path())?;
            let fallback_name = entry.path().file_stem().map_or_else(
                || entry.file_name().to_string_lossy().to_string(),
                |stem| stem.to_string_lossy().to_string(),
            );
            root_agents.push(AgentSummary {
                name: parse_toml_string(&contents, "name").unwrap_or(fallback_name),
                description: parse_toml_string(&contents, "description"),
                model: parse_toml_string(&contents, "model"),
                reasoning_effort: parse_toml_string(&contents, "model_reasoning_effort"),
                source: *source,
                shadowed_by: None,
            });
        }
        root_agents.sort_by(|left, right| left.name.cmp(&right.name));
        for mut agent in root_agents {
            let key = agent.name.to_ascii_lowercase();
            if let Some(existing) = active_sources.get(&key) {
                agent.shadowed_by = Some(*existing);
            } else {
                active_sources.insert(key, agent.source);
            }
            agents.push(agent);
        }
    }
    Ok(agents)
}

fn render_agents_report_json(cwd: &Path, agents: &[AgentSummary]) -> Value {
    let active = agents
        .iter()
        .filter(|agent| agent.shadowed_by.is_none())
        .count();
    json!({
        "kind": "agents",
        "action": "list",
        "working_directory": cwd.display().to_string(),
        "count": agents.len(),
        "summary": {
            "total": agents.len(),
            "active": active,
            "shadowed": agents.len().saturating_sub(active),
        },
        "agents": agents.iter().map(agent_summary_json).collect::<Vec<_>>(),
    })
}

fn discover_agents_for_task(agents: &[AgentSummary], task: &str) -> Vec<StaticAgentMatch> {
    let task_terms = normalized_terms(task);
    let mut matches = agents
        .iter()
        .filter(|agent| agent.shadowed_by.is_none())
        .filter_map(|agent| {
            let haystack = [
                agent.name.as_str(),
                agent.description.as_deref().unwrap_or_default(),
                agent.model.as_deref().unwrap_or_default(),
                agent.reasoning_effort.as_deref().unwrap_or_default(),
            ]
            .join(" ")
            .to_ascii_lowercase();
            let mut match_terms = task_terms
                .iter()
                .filter(|term| haystack.contains(term.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if match_terms.is_empty() {
                let name = agent.name.to_ascii_lowercase();
                match_terms = normalized_terms(&name)
                    .into_iter()
                    .filter(|term| task_terms.iter().any(|task_term| term.contains(task_term)))
                    .collect();
            }
            (!match_terms.is_empty()).then(|| StaticAgentMatch {
                name: agent.name.clone(),
                description: agent.description.clone(),
                source: agent.source,
                shadowed_by: agent.shadowed_by,
                score: match_terms.len(),
                match_terms,
            })
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.name.cmp(&right.name))
    });
    matches
}

fn assemble_static_agent_team(matches: &[StaticAgentMatch]) -> Option<StaticAgentTeam> {
    let leader = matches.first()?.clone();
    let workers = matches.iter().skip(1).take(4).cloned().collect();
    Some(StaticAgentTeam { leader, workers })
}

fn normalized_terms(value: &str) -> Vec<String> {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(str::trim)
        .filter(|term| term.len() >= 3)
        .map(str::to_ascii_lowercase)
        .collect()
}

fn parse_toml_string(contents: &str, key: &str) -> Option<String> {
    let prefix = format!("{key} =");
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            continue;
        }
        let Some(value) = trimmed.strip_prefix(&prefix) else {
            continue;
        };
        let value = value.trim();
        let Some(value) = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
        else {
            continue;
        };
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    None
}

fn definition_source_json(source: DefinitionSource) -> Value {
    let id = match source {
        DefinitionSource::ProjectCowd => "project_cowd",
        DefinitionSource::ProjectCodex => "project_codex",
        DefinitionSource::ProjectClaude => "project_claude",
        DefinitionSource::UserCowdConfigHome => "user_cowd_config_home",
        DefinitionSource::UserCodexHome => "user_codex_home",
        DefinitionSource::UserCowd => "user_cowd",
        DefinitionSource::UserCodex => "user_codex",
        DefinitionSource::UserClaude => "user_claude",
    };
    json!({ "id": id })
}

fn agent_summary_json(agent: &AgentSummary) -> Value {
    json!({
        "name": agent.name,
        "description": agent.description,
        "model": agent.model,
        "reasoning_effort": agent.reasoning_effort,
        "source": definition_source_json(agent.source),
        "active": agent.shadowed_by.is_none(),
        "shadowed_by": agent.shadowed_by.map(definition_source_json),
    })
}
