use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use app_mfg::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_skill_pack, skill_agent_node_id, MfgSkillManifest,
};
use command_service::SkillSlashDispatch;
use serde::{Deserialize, Serialize};
use skill_service::{
    SkillInfo, SkillManager, SkillRegistry, SkillRegistryRootKind, SkillRouter, SkillViewInput,
    SkillViewOutput,
};

use super::{ServiceEnvelope, SkillService};

#[derive(Debug, Deserialize)]
pub(crate) struct SkillCatalogQuery {
    #[serde(default)]
    pub(crate) scope: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SkillProjectionQuery {
    #[serde(default)]
    pub(crate) surface: Option<String>,
    #[serde(default)]
    pub(crate) query: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SkillActionRequest {
    #[serde(default)]
    pub(crate) request_id: Option<String>,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) incident_id: Option<String>,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SkillFileQuery {
    #[serde(default)]
    pub(crate) path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SkillCatalogItem {
    id: String,
    name: String,
    description: Option<String>,
    scope: String,
    source: String,
    domain: Option<String>,
    status: String,
    risk: String,
    tags: Vec<String>,
    tools: Vec<String>,
    required_evidence: Vec<String>,
    capabilities: Vec<String>,
    path: Option<String>,
    shadowed_by: Option<String>,
}

#[derive(Debug, Serialize)]
struct SkillFileEntry {
    path: String,
    name: String,
    kind: &'static str,
    size: Option<u64>,
    primary: bool,
}

#[derive(Debug, Serialize)]
struct SkillAction {
    id: &'static str,
    label: &'static str,
    surface: &'static str,
    mutation: bool,
}

#[derive(Debug, Clone)]
struct LocalSkillSummary {
    name: String,
    description: Option<String>,
    source: String,
    shadowed_by: Option<String>,
    origin: &'static str,
    path: String,
}

#[derive(Debug, Clone)]
struct InstalledSkill {
    invocation_name: String,
    display_name: Option<String>,
    source: PathBuf,
    registry_root: PathBuf,
    installed_path: PathBuf,
}

#[derive(Debug, Clone)]
enum SkillInstallSource {
    Directory { root: PathBuf, prompt_path: PathBuf },
    MarkdownFile { path: PathBuf },
}

impl SkillInstallSource {
    fn prompt_path(&self) -> &Path {
        match self {
            Self::Directory { prompt_path, .. } => prompt_path,
            Self::MarkdownFile { path } => path,
        }
    }

    fn report_path(&self) -> &Path {
        match self {
            Self::Directory { root, .. } => root,
            Self::MarkdownFile { path } => path,
        }
    }

    fn fallback_name(&self) -> Option<String> {
        let path = self.report_path();
        if path.is_file() {
            path.file_stem()
        } else {
            path.file_name()
        }
        .map(|name| name.to_string_lossy().into_owned())
    }
}

#[derive(Debug, Serialize)]
struct SkillProjection {
    kind: &'static str,
    surface: String,
    catalog_count: usize,
    capabilities: Vec<&'static str>,
    actions: Vec<SkillAction>,
    facets: SkillProjectionFacets,
    queue: SkillProjectionQueue,
    governance: SkillProjectionGovernance,
    diagnostics: Vec<String>,
    activation: Option<serde_json::Value>,
    items: Vec<SkillCatalogItem>,
}

#[derive(Debug, Serialize)]
struct SkillProjectionFacets {
    scopes: Vec<String>,
    domains: Vec<String>,
    tags: Vec<String>,
    risks: Vec<String>,
    statuses: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SkillProjectionQueue {
    source: &'static str,
    run_list_endpoint: &'static str,
    supports_watch: bool,
}

#[derive(Debug, Serialize)]
struct SkillProjectionGovernance {
    evidence_model: &'static str,
    tool_fact_model: &'static str,
    approval_model: &'static str,
}

#[derive(Debug)]
pub(crate) enum SkillServiceError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl SkillServiceError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::BadRequest(message) | Self::NotFound(message) | Self::Internal(message) => {
                message.clone()
            }
        }
    }
}

impl SkillService {
    pub(crate) fn catalog_envelope(&self) -> ServiceEnvelope {
        self.envelope("catalog")
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![self.catalog_envelope(), self.envelope("projection")]
    }

    pub(crate) fn command_text(
        &self,
        workspace_root: &Path,
        args: Option<&str>,
    ) -> std::io::Result<String> {
        if let Some(args) = normalize_optional_args(args) {
            if let Some(help_path) = help_path_from_args(args) {
                return Ok(match help_path.as_slice() {
                    [] => render_skills_usage(None),
                    ["install", ..] => render_skills_usage(Some("install")),
                    ["view", ..] => render_skills_usage(Some("view")),
                    ["create" | "edit" | "delete" | "generate", ..] => {
                        render_skills_usage(Some("managed"))
                    }
                    _ => render_skills_usage(Some(&help_path.join(" "))),
                });
            }
        }

        match normalize_optional_args(args) {
            None | Some("list") => {
                let skills = local_skill_summaries(workspace_root)?;
                Ok(render_skills_report(&skills))
            }
            Some("install") => Ok(render_skills_usage(Some("install"))),
            Some(args) if args.starts_with("install ") => {
                let target = args["install ".len()..].trim();
                if target.is_empty() {
                    return Ok(render_skills_usage(Some("install")));
                }
                let install = install_skill(target, workspace_root)?;
                Ok(render_skill_install_report(&install))
            }
            Some("view") => Ok(render_skills_usage(Some("view"))),
            Some(args) if args.starts_with("view ") => {
                let name = args["view ".len()..].trim();
                if name.is_empty() {
                    return Ok(render_skills_usage(Some("view")));
                }
                let paths = discover_skill_root_paths(workspace_root);
                let manager = SkillManager::new(paths);
                let result = manager.view_skill(SkillViewInput {
                    name: name.to_string(),
                    file_path: None,
                    include_files: true,
                });
                Ok(render_skill_view_report(&result))
            }
            Some("create" | "edit" | "delete" | "generate") => {
                Ok(render_skills_usage(Some("managed")))
            }
            Some(args)
                if args.starts_with("create ")
                    || args.starts_with("edit ")
                    || args.starts_with("delete ")
                    || args.starts_with("generate ") =>
            {
                Ok(render_skills_usage(Some("managed")))
            }
            Some(args) if is_help_arg(args) => Ok(render_skills_usage(None)),
            Some(args) => Ok(render_skills_usage(Some(args))),
        }
    }

    pub(crate) fn command_json(
        &self,
        workspace_root: &Path,
        args: Option<&str>,
    ) -> std::io::Result<serde_json::Value> {
        if let Some(args) = normalize_optional_args(args) {
            if let Some(help_path) = help_path_from_args(args) {
                return Ok(match help_path.as_slice() {
                    [] => render_skills_usage_json(None),
                    ["install", ..] => render_skills_usage_json(Some("install")),
                    ["view", ..] => render_skills_usage_json(Some("view")),
                    ["create" | "edit" | "delete" | "generate", ..] => {
                        render_skills_usage_json(Some("managed"))
                    }
                    _ => render_skills_usage_json(Some(&help_path.join(" "))),
                });
            }
        }

        match normalize_optional_args(args) {
            None | Some("list") => {
                let skills = local_skill_summaries(workspace_root)?;
                Ok(render_skills_report_json(&skills))
            }
            Some("install") => Ok(render_skills_usage_json(Some("install"))),
            Some(args) if args.starts_with("install ") => {
                let target = args["install ".len()..].trim();
                if target.is_empty() {
                    return Ok(render_skills_usage_json(Some("install")));
                }
                let install = install_skill(target, workspace_root)?;
                Ok(render_skill_install_report_json(&install))
            }
            Some("view") => Ok(render_skills_usage_json(Some("view"))),
            Some(args) if args.starts_with("view ") => {
                let name = args["view ".len()..].trim();
                if name.is_empty() {
                    return Ok(render_skills_usage_json(Some("view")));
                }
                let paths = discover_skill_root_paths(workspace_root);
                let manager = SkillManager::new(paths);
                let result = manager.view_skill(SkillViewInput {
                    name: name.to_string(),
                    file_path: None,
                    include_files: true,
                });
                Ok(serde_json::json!({
                    "kind": "skills",
                    "action": "view",
                    "success": result.success,
                    "name": result.name,
                    "description": result.description,
                    "tags": result.tags,
                    "content": result.content,
                    "setup_needed": result.setup_needed,
                    "readiness_status": result.readiness_status,
                    "linked_files": {
                        "references": result.linked_files.references,
                        "templates": result.linked_files.templates,
                        "scripts": result.linked_files.scripts,
                    },
                    "config_vars": result.config_vars,
                    "path": result.path,
                }))
            }
            Some("create" | "edit" | "delete" | "generate") => {
                Ok(render_skills_usage_json(Some("managed")))
            }
            Some(args)
                if args.starts_with("create ")
                    || args.starts_with("edit ")
                    || args.starts_with("delete ")
                    || args.starts_with("generate ") =>
            {
                Ok(render_skills_usage_json(Some("managed")))
            }
            Some(args) if is_help_arg(args) => Ok(render_skills_usage_json(None)),
            Some(args) => Ok(render_skills_usage_json(Some(args))),
        }
    }

    pub(crate) fn resolve_invocation(
        &self,
        workspace_root: &Path,
        args: Option<&str>,
    ) -> Result<SkillSlashDispatch, String> {
        let dispatch = classify_static_skill_command(args);
        if let SkillSlashDispatch::Invoke(ref prompt) = dispatch {
            let skill_token = prompt
                .trim_start_matches('$')
                .split_whitespace()
                .next()
                .unwrap_or_default();
            if !skill_token.is_empty() && find_catalog_item(workspace_root, skill_token).is_err() {
                let mut message = format!("Unknown skill: {skill_token}");
                if let Ok(available) = collect_skill_catalog(workspace_root) {
                    let names = available
                        .iter()
                        .filter(|skill| skill.status == "ready")
                        .map(|skill| skill.name.clone())
                        .collect::<Vec<_>>();
                    if !names.is_empty() {
                        message.push_str("\n  Available skills: ");
                        message.push_str(&names.join(", "));
                    }
                }
                message.push_str(
                    "\n  Usage: /skills [list|view <name>|install <path>|help|<skill> [args]]",
                );
                return Err(message);
            }
        }
        Ok(dispatch)
    }

    pub(crate) fn catalog(
        &self,
        workspace_root: &Path,
        query: SkillCatalogQuery,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let items = filter_scope(
            collect_skill_catalog(workspace_root)?,
            query.scope.as_deref(),
        );
        Ok(serde_json::json!({
            "kind": "skills.catalog",
            "schema_version": 1,
            "items": items,
        }))
    }

    pub(crate) fn projection(
        &self,
        workspace_root: &Path,
        query: SkillProjectionQuery,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let surface = normalize_surface(query.surface.as_deref());
        let items = collect_skill_catalog(workspace_root)?;
        let activation = activation_projection(workspace_root, query.query.as_deref())?;
        Ok(serde_json::to_value(SkillProjection {
            kind: "skills.projection",
            surface: surface.clone(),
            catalog_count: items.len(),
            capabilities: projection_capabilities(&surface),
            actions: projection_actions(&surface),
            facets: projection_facets(&items),
            queue: SkillProjectionQueue {
                source: "mfg.skill_runs",
                run_list_endpoint: "/api/apps/mfg/incidents/:incident_id/skills",
                supports_watch: surface != "cli",
            },
            governance: SkillProjectionGovernance {
                evidence_model: "matrix.evidence.packet + agent_evidence + tool_invocation",
                tool_fact_model: "tool.execution_plan + tool.invocation.runtime_event",
                approval_model: "quality_gate + cross_plane_policy",
            },
            diagnostics: projection_diagnostics(&items),
            activation,
            items,
        })
        .map_err(|error| SkillServiceError::Internal(error.to_string()))?)
    }

    pub(crate) fn detail(
        &self,
        workspace_root: &Path,
        id: &str,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, id)?;
        Ok(serde_json::json!({
            "kind": "skills.detail",
            "schema_version": 1,
            "skill": item,
        }))
    }

    pub(crate) fn files(
        &self,
        workspace_root: &Path,
        id: &str,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, id)?;
        if item.scope == "mfg" {
            return Ok(mfg_virtual_files(&item));
        }
        let root = local_skill_root(&item)?;
        let files = list_skill_files(&root)
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
        let primary = files
            .iter()
            .find(|file| file.primary)
            .map(|file| file.path.clone());
        Ok(serde_json::json!({
            "kind": "skills.files",
            "schema_version": 1,
            "skill": item,
            "root": root.display().to_string(),
            "primary": primary,
            "files": files,
        }))
    }

    pub(crate) fn raw_file(
        &self,
        workspace_root: &Path,
        id: &str,
        query: SkillFileQuery,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, id)?;
        let requested = query.path.unwrap_or_else(|| "SKILL.md".to_string());
        if item.scope == "mfg" {
            if requested != "SKILL.md" {
                return Err(SkillServiceError::NotFound(
                    "skill file not found".to_string(),
                ));
            }
            return Ok(serde_json::json!({
                "kind": "skills.file.raw",
                "schema_version": 1,
                "skill": item,
                "path": "SKILL.md",
                "content_type": "text/markdown",
                "content": mfg_virtual_skill_markdown(&item),
            }));
        }
        let root = local_skill_root(&item)?;
        let file_path = safe_skill_file_path(&root, &requested)?;
        let content = fs::read_to_string(&file_path)
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
        Ok(serde_json::json!({
            "kind": "skills.file.raw",
            "schema_version": 1,
            "skill": item,
            "path": requested,
            "content_type": "text/markdown",
            "content": content,
        }))
    }

    pub(crate) fn validate(
        &self,
        workspace_root: &Path,
        id: &str,
        request: SkillActionRequest,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, id)?;
        let validation = match item.scope.as_str() {
            "mfg" => {
                let skill = find_mfg_skill(&item.name).ok_or_else(|| {
                    SkillServiceError::NotFound("MFG skill not found".to_string())
                })?;
                serde_json::json!({
                    "status": "pass",
                    "scope": "mfg",
                    "skill_id": skill.skill_id,
                    "checks": [
                        {"id":"manifest.present","status":"pass"},
                        {"id":"evidence.required","status": if skill.required_evidence.is_empty() {"warn"} else {"pass"}},
                        {"id":"tools.declared","status": if skill.tools.is_empty() {"warn"} else {"pass"}},
                        {"id":"quality_gate.present","status": if skill.quality_gate.is_empty() {"warn"} else {"pass"}}
                    ],
                    "required_evidence": skill.required_evidence,
                    "tools": skill.tools,
                    "quality_gate": skill.quality_gate,
                })
            }
            _ => serde_json::json!({
                "status": "unsupported",
                "scope": item.scope,
                "reason": "unsupported_for_local_skill",
                "path": item.path,
            }),
        };
        Ok(serde_json::json!({
            "kind": "skills.action.validate",
            "schema_version": 1,
            "request_id": request.request_id,
            "session_id": request.session_id,
            "skill": item,
            "validation": validation,
        }))
    }

    pub(crate) fn plan(
        &self,
        workspace_root: &Path,
        config_home: &Path,
        mfg: &super::MfgService,
        id: &str,
        request: SkillActionRequest,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, id)?;
        if item.scope != "mfg" {
            return Ok(serde_json::json!({
                "kind": "skills.action.plan",
                "schema_version": 1,
                "request_id": request.request_id,
                "session_id": request.session_id,
                "skill": item,
                "status": "unsupported",
                "reason": "unsupported_for_local_skill",
            }));
        }
        let incident_id = required_incident_id(&request)?;
        let context = mfg
            .incident_context(config_home, &incident_id)
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?
            .ok_or_else(|| SkillServiceError::NotFound("MFG incident not found".to_string()))?;
        let mut plan = plan_server_manufacturing_skills(
            &context.incident,
            context.analysis.as_ref(),
            context.packet.as_ref(),
            request.limit.unwrap_or(3).clamp(1, 8),
        );
        if let Some(skill) = find_mfg_skill(&item.name) {
            if !plan
                .selected_skills
                .iter()
                .any(|selected| selected.skill_id == skill.skill_id)
            {
                plan.selected_skills.insert(0, skill);
                plan.selected_skills
                    .truncate(request.limit.unwrap_or(3).clamp(1, 8));
                plan.evidence_requirements = plan
                    .selected_skills
                    .iter()
                    .flat_map(|skill| skill.required_evidence.iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                plan.planned_agent_nodes = plan
                    .selected_skills
                    .iter()
                    .map(|skill| skill_agent_node_id(&skill.skill_id))
                    .collect();
            }
        }
        Ok(serde_json::json!({
            "kind": "skills.action.plan",
            "schema_version": 1,
            "request_id": request.request_id,
            "session_id": request.session_id,
            "skill": item,
            "incident_id": context.incident.incident_id,
            "plan": plan,
        }))
    }

    pub(crate) fn run(
        &self,
        workspace_root: &Path,
        config_home: &Path,
        mfg: &super::MfgService,
        id: &str,
        request: SkillActionRequest,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, id)?;
        if item.scope != "mfg" {
            return Ok(serde_json::json!({
                "kind": "skills.action.run",
                "schema_version": 1,
                "request_id": request.request_id,
                "session_id": request.session_id,
                "skill": item,
                "status": "unsupported",
                "reason": "unsupported_for_local_skill",
            }));
        }
        let incident_id = required_incident_id(&request)?;
        let context = mfg
            .incident_context(config_home, &incident_id)
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?
            .ok_or_else(|| SkillServiceError::NotFound("MFG incident not found".to_string()))?;
        let skill = find_mfg_skill(&item.name)
            .ok_or_else(|| SkillServiceError::NotFound("MFG skill not found".to_string()))?;
        let run = run_server_manufacturing_skill(
            &context.incident,
            &skill,
            context.analysis.as_ref(),
            context.packet.as_ref(),
        );
        let run = mfg
            .record_skill_run(config_home, &run)
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
        Ok(serde_json::json!({
            "kind": "skills.action.run",
            "schema_version": 1,
            "request_id": request.request_id,
            "session_id": request.session_id,
            "skill": item,
            "incident_id": context.incident.incident_id,
            "skill_run": run,
        }))
    }
}

#[must_use]
fn classify_static_skill_command(args: Option<&str>) -> SkillSlashDispatch {
    match normalize_optional_args(args) {
        None | Some("list" | "help" | "-h" | "--help") => SkillSlashDispatch::Local,
        Some(args) if args == "install" || args.starts_with("install ") => {
            SkillSlashDispatch::Local
        }
        Some("view") => SkillSlashDispatch::Local,
        Some(args) if args.starts_with("view ") => SkillSlashDispatch::Local,
        Some("create" | "edit" | "delete" | "generate") => SkillSlashDispatch::Local,
        Some(args)
            if args.starts_with("create ")
                || args.starts_with("edit ")
                || args.starts_with("delete ")
                || args.starts_with("generate ") =>
        {
            SkillSlashDispatch::Local
        }
        Some(args) => SkillSlashDispatch::Invoke(format!("${}", args.trim_start_matches('/'))),
    }
}

fn normalize_optional_args(args: Option<&str>) -> Option<&str> {
    args.map(str::trim).filter(|value| !value.is_empty())
}

fn is_help_arg(arg: &str) -> bool {
    matches!(arg.trim(), "help" | "-h" | "--help")
}

fn help_path_from_args(args: &str) -> Option<Vec<&str>> {
    let parts = args.split_whitespace().collect::<Vec<_>>();
    let help_index = parts.iter().position(|part| is_help_arg(part))?;
    Some(parts[..help_index].to_vec())
}

fn discover_skill_root_paths(cwd: &Path) -> Vec<PathBuf> {
    SkillRegistry::discover(cwd)
        .roots()
        .iter()
        .filter(|root| root.kind == SkillRegistryRootKind::SkillsDir)
        .map(|root| root.path.clone())
        .collect()
}

fn local_skill_summaries(cwd: &Path) -> std::io::Result<Vec<LocalSkillSummary>> {
    SkillRegistry::discover(cwd).list().map(|skills| {
        skills
            .into_iter()
            .map(|skill| LocalSkillSummary {
                name: skill.name,
                description: skill.description,
                source: format!("{:?}", skill.source),
                shadowed_by: skill.shadowed_by.map(|source| format!("{source:?}")),
                origin: match skill.kind {
                    SkillRegistryRootKind::SkillsDir => "skills",
                    SkillRegistryRootKind::LegacyCommandsDir => "legacy /commands",
                },
                path: skill.path.display().to_string(),
            })
            .collect()
    })
}

fn render_skills_report(skills: &[LocalSkillSummary]) -> String {
    if skills.is_empty() {
        return "No skills found.".to_string();
    }
    let active = skills
        .iter()
        .filter(|skill| skill.shadowed_by.is_none())
        .count();
    let mut lines = vec![
        "Skills".to_string(),
        format!("  {active} available skills"),
        String::new(),
    ];
    for skill in skills {
        let mut detail = vec![skill.name.clone()];
        if let Some(description) = &skill.description {
            detail.push(description.clone());
        }
        if skill.origin != "skills" {
            detail.push(skill.origin.to_string());
        }
        match &skill.shadowed_by {
            Some(winner) => lines.push(format!("  (shadowed by {winner}) {}", detail.join(" · "))),
            None => lines.push(format!("  {}", detail.join(" · "))),
        }
    }
    lines.join("\n")
}

fn render_skills_report_json(skills: &[LocalSkillSummary]) -> serde_json::Value {
    let active = skills
        .iter()
        .filter(|skill| skill.shadowed_by.is_none())
        .count();
    serde_json::json!({
        "kind": "skills",
        "action": "list",
        "summary": {
            "total": skills.len(),
            "active": active,
            "shadowed": skills.len().saturating_sub(active),
        },
        "skills": skills.iter().map(|skill| serde_json::json!({
            "name": skill.name,
            "description": skill.description,
            "source": skill.source,
            "shadowed_by": skill.shadowed_by,
            "origin": skill.origin,
            "path": skill.path,
        })).collect::<Vec<_>>(),
    })
}

fn render_skill_view_report(result: &SkillViewOutput) -> String {
    let mut lines = vec!["Skills".to_string()];
    if result.success {
        lines.push(format!("  Name             {}", result.name));
        lines.push(format!("  Description      {}", result.description));
        if !result.tags.is_empty() {
            lines.push(format!("  Tags             {}", result.tags.join(", ")));
        }
        lines.push(format!(
            "  Status           {}",
            if result.setup_needed {
                "setup_needed"
            } else {
                "ready"
            }
        ));
        lines.push(String::new());
        lines.push("---".to_string());
        lines.push(String::new());
        let preview = if result.content.len() > 500 {
            format!(
                "{}...\n\n[Truncated - use /skill view {} --file <path> for full content]",
                &result.content[..500],
                result.name
            )
        } else {
            result.content.clone()
        };
        lines.push(preview);
    } else {
        lines.push("  Result           not found".to_string());
    }
    lines.join("\n")
}

fn render_skills_usage(topic: Option<&str>) -> String {
    match topic {
        Some("create" | "edit" | "delete" | "generate" | "managed") => {
            "Skills - Managed In WebUI/TUI\n\nThe CLI intentionally exposes only list, view, install, and invocation.\nUse WebUI or TUI for create, edit, delete, generate, validation, run queues,\ngovernance review, and stateful skill management.".to_string()
        }
        Some("view") => "Skills - View\n\nUsage: /skill view <name>".to_string(),
        Some("install") => "Skills - Install\n\nUsage: /skill install <source>".to_string(),
        _ => [
            "Skills",
            "  Usage            /skills [list|view <name>|install <path>|help|<skill> [args]]",
            "  Alias            /skill",
            "  Direct CLI       cowd skill [list|view <name>|install <path>|help]",
            "  Local controls   list, view <name>, install <path>",
            "  Managed in UI    create, edit, delete, generate, validate, run, governance",
        ]
        .join("\n"),
    }
}

fn render_skills_usage_json(topic: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "kind": "skills",
        "action": "help",
        "topic": topic.unwrap_or("overview"),
        "usage": render_skills_usage(topic),
    })
}

fn install_skill(source: &str, cwd: &Path) -> std::io::Result<InstalledSkill> {
    let registry_root = crate::skill_static::default_skill_install_root()?;
    install_skill_into(source, cwd, &registry_root)
}

fn install_skill_into(
    source: &str,
    cwd: &Path,
    registry_root: &Path,
) -> std::io::Result<InstalledSkill> {
    let source = resolve_skill_install_source(source, cwd)?;
    let contents = fs::read_to_string(source.prompt_path())?;
    let display_name = parse_skill_frontmatter(&contents).0;
    let invocation_name = derive_skill_install_name(&source, display_name.as_deref())?;
    let installed_path = registry_root.join(&invocation_name);
    fs::create_dir_all(&registry_root)?;
    if installed_path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "skill '{invocation_name}' is already installed at {}",
                installed_path.display()
            ),
        ));
    }

    fs::create_dir_all(&installed_path)?;
    let install_result = match &source {
        SkillInstallSource::Directory { root, .. } => {
            copy_directory_contents(root, &installed_path)
        }
        SkillInstallSource::MarkdownFile { path } => {
            fs::copy(path, installed_path.join("SKILL.md")).map(|_| ())
        }
    };
    if let Err(error) = install_result {
        let _ = fs::remove_dir_all(&installed_path);
        return Err(error);
    }

    Ok(InstalledSkill {
        display_name,
        invocation_name,
        source: source.report_path().to_path_buf(),
        registry_root: registry_root.to_path_buf(),
        installed_path,
    })
}

fn resolve_skill_install_source(source: &str, cwd: &Path) -> std::io::Result<SkillInstallSource> {
    let candidate = PathBuf::from(source);
    let path = if candidate.is_absolute() {
        candidate
    } else {
        cwd.join(candidate)
    };
    let canonical = path.canonicalize()?;

    if canonical.is_dir() {
        let prompt_path = canonical.join("SKILL.md");
        if !prompt_path.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "skill directory '{}' must contain SKILL.md",
                    canonical.display()
                ),
            ));
        }
        return Ok(SkillInstallSource::Directory {
            root: canonical,
            prompt_path,
        });
    }

    if canonical
        .extension()
        .is_some_and(|ext| ext.to_string_lossy().eq_ignore_ascii_case("md"))
    {
        return Ok(SkillInstallSource::MarkdownFile { path: canonical });
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "skill source '{}' must be a directory with SKILL.md or a markdown file",
            canonical.display()
        ),
    ))
}

fn derive_skill_install_name(
    source: &SkillInstallSource,
    declared_name: Option<&str>,
) -> std::io::Result<String> {
    for candidate in [declared_name, source.fallback_name().as_deref()] {
        if let Some(candidate) = candidate.and_then(sanitize_skill_invocation_name) {
            return Ok(candidate);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "unable to derive an installable invocation name from '{}'",
            source.report_path().display()
        ),
    ))
}

fn parse_skill_frontmatter(contents: &str) -> (Option<String>, Option<String>) {
    let mut lines = contents.lines();
    if lines.next().map(str::trim) != Some("---") {
        return (None, None);
    }

    let mut name = None;
    let mut description = None;
    for line in lines {
        let trimmed = line.trim();
        if trimmed == "---" {
            break;
        }
        if let Some(value) = trimmed.strip_prefix("name:") {
            let value = unquote_frontmatter_value(value.trim());
            if !value.is_empty() {
                name = Some(value);
            }
            continue;
        }
        if let Some(value) = trimmed.strip_prefix("description:") {
            let value = unquote_frontmatter_value(value.trim());
            if !value.is_empty() {
                description = Some(value);
            }
        }
    }

    (name, description)
}

fn unquote_frontmatter_value(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|trimmed| trimmed.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|trimmed| trimmed.strip_suffix('\''))
        })
        .unwrap_or(value)
        .to_string()
}

fn sanitize_skill_invocation_name(candidate: &str) -> Option<String> {
    let trimmed = candidate
        .trim()
        .trim_start_matches('/')
        .trim_start_matches('$');
    if trimmed.is_empty() {
        return None;
    }

    let mut name = String::new();
    let mut previous_dash = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            previous_dash = false;
            name.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '-' | '_' | ' ' | '.') {
            if !previous_dash {
                name.push('-');
                previous_dash = true;
            }
        } else {
            return None;
        }
    }

    let name = name.trim_matches('-').to_string();
    (!name.is_empty()).then_some(name)
}

fn copy_directory_contents(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_directory_contents(&source_path, &destination_path)?;
        } else if source_path.is_file() {
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

fn render_skill_install_report(skill: &InstalledSkill) -> String {
    let mut lines = vec![
        "Skills".to_string(),
        format!("  Result           installed {}", skill.invocation_name),
    ];
    if let Some(display_name) = &skill.display_name {
        lines.push(format!("  Display name     {display_name}"));
    }
    lines.push(format!("  Source           {}", skill.source.display()));
    lines.push(format!(
        "  Registry         {}",
        skill.registry_root.display()
    ));
    lines.push(format!(
        "  Installed path   {}",
        skill.installed_path.display()
    ));
    lines.join("\n")
}

fn render_skill_install_report_json(skill: &InstalledSkill) -> serde_json::Value {
    serde_json::json!({
        "kind": "skills",
        "action": "install",
        "status": "installed",
        "invocation_name": skill.invocation_name,
        "display_name": skill.display_name,
        "source": skill.source.display().to_string(),
        "registry_root": skill.registry_root.display().to_string(),
        "installed_path": skill.installed_path.display().to_string(),
    })
}

fn collect_skill_catalog(
    workspace_root: &Path,
) -> Result<Vec<SkillCatalogItem>, SkillServiceError> {
    let mut items = server_manufacturing_skill_pack()
        .into_iter()
        .map(mfg_skill_catalog_item)
        .collect::<Vec<_>>();
    let registry = SkillRegistry::discover(workspace_root);
    for skill in registry
        .list()
        .map_err(|error| SkillServiceError::Internal(error.to_string()))?
    {
        items.push(local_skill_catalog_item(skill));
    }
    items.sort_by(|left, right| {
        left.scope
            .cmp(&right.scope)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(items)
}

fn find_catalog_item(
    workspace_root: &Path,
    id: &str,
) -> Result<SkillCatalogItem, SkillServiceError> {
    collect_skill_catalog(workspace_root)?
        .into_iter()
        .find(|item| item.id == id || item.name.eq_ignore_ascii_case(id))
        .ok_or_else(|| SkillServiceError::NotFound("skill not found".to_string()))
}

fn find_mfg_skill(skill_id: &str) -> Option<MfgSkillManifest> {
    server_manufacturing_skill_pack()
        .into_iter()
        .find(|skill| skill.skill_id.eq_ignore_ascii_case(skill_id))
}

fn required_incident_id(request: &SkillActionRequest) -> Result<String, SkillServiceError> {
    request
        .incident_id
        .as_ref()
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| SkillServiceError::BadRequest("incident_id is required".to_string()))
}

fn mfg_skill_catalog_item(skill: MfgSkillManifest) -> SkillCatalogItem {
    let risk = mfg_risk(&skill).to_string();
    let tags = vec![skill.domain.clone(), "mfg".to_string()];
    let capabilities = skill
        .output_actions
        .iter()
        .chain(skill.input_fact_types.iter())
        .cloned()
        .collect();
    SkillCatalogItem {
        id: format!("mfg:{}", skill.skill_id),
        name: skill.skill_id,
        description: Some(skill.role),
        scope: "mfg".to_string(),
        source: "app_mfg.server_manufacturing".to_string(),
        domain: Some(skill.domain),
        status: "ready".to_string(),
        risk,
        tags,
        tools: skill.tools,
        required_evidence: skill.required_evidence,
        capabilities,
        path: None,
        shadowed_by: None,
    }
}

fn local_skill_catalog_item(skill: SkillInfo) -> SkillCatalogItem {
    SkillCatalogItem {
        id: format!("local:{}", skill.name),
        name: skill.name,
        description: skill.description,
        scope: "local".to_string(),
        source: format!("{:?}", skill.source),
        domain: None,
        status: if skill.shadowed_by.is_some() {
            "shadowed".to_string()
        } else {
            "ready".to_string()
        },
        risk: "operator_review".to_string(),
        tags: skill.tags,
        tools: Vec::new(),
        required_evidence: Vec::new(),
        capabilities: skill.related_skills,
        path: Some(skill.path.display().to_string()),
        shadowed_by: skill.shadowed_by.map(|source| format!("{source:?}")),
    }
}

fn local_skill_root(item: &SkillCatalogItem) -> Result<PathBuf, SkillServiceError> {
    let Some(path) = item.path.as_ref() else {
        return Err(SkillServiceError::NotFound(
            "skill path unavailable".to_string(),
        ));
    };
    let path = PathBuf::from(path);
    let root = if path.is_file() {
        path.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else {
        path
    };
    root.canonicalize()
        .map_err(|error| SkillServiceError::Internal(format!("skill root unavailable: {error}")))
}

fn safe_skill_file_path(root: &Path, requested: &str) -> Result<PathBuf, SkillServiceError> {
    if requested.trim().is_empty() || requested.starts_with('/') || requested.contains('\\') {
        return Err(SkillServiceError::BadRequest(
            "invalid skill file path".to_string(),
        ));
    }
    let candidate = root.join(requested);
    let canonical = candidate
        .canonicalize()
        .map_err(|_| SkillServiceError::NotFound("skill file not found".to_string()))?;
    if !canonical.starts_with(root) || !canonical.is_file() {
        return Err(SkillServiceError::BadRequest(
            "skill file path escapes skill root".to_string(),
        ));
    }
    Ok(canonical)
}

fn list_skill_files(root: &Path) -> std::io::Result<Vec<SkillFileEntry>> {
    let mut files = Vec::new();
    collect_skill_files(root, root, &mut files, 0)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn collect_skill_files(
    root: &Path,
    dir: &Path,
    files: &mut Vec<SkillFileEntry>,
    depth: usize,
) -> std::io::Result<()> {
    if depth > 3 || files.len() >= 240 {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name == "node_modules" || name == "target" {
            continue;
        }
        let metadata = entry.metadata()?;
        let relative = path
            .strip_prefix(root)
            .unwrap_or(path.as_path())
            .to_string_lossy()
            .replace('\\', "/");
        if metadata.is_dir() {
            files.push(SkillFileEntry {
                path: relative.clone(),
                name,
                kind: "directory",
                size: None,
                primary: false,
            });
            collect_skill_files(root, &path, files, depth + 1)?;
        } else if metadata.is_file() {
            files.push(SkillFileEntry {
                primary: relative == "SKILL.md",
                path: relative,
                name,
                kind: "file",
                size: Some(metadata.len()),
            });
        }
    }
    Ok(())
}

fn mfg_virtual_files(item: &SkillCatalogItem) -> serde_json::Value {
    serde_json::json!({
        "kind": "skills.files",
        "schema_version": 1,
        "skill": item,
        "root": "virtual://mfg/server-manufacturing",
        "primary": "SKILL.md",
        "files": [{
            "path": "SKILL.md",
            "name": "SKILL.md",
            "kind": "file",
            "size": mfg_virtual_skill_markdown(item).len(),
            "primary": true
        }],
    })
}

fn mfg_virtual_skill_markdown(item: &SkillCatalogItem) -> String {
    format!(
        "# {}\n\n{}\n\n- Scope: {}\n- Source: {}\n- Domain: {}\n- Status: {}\n- Risk: {}\n\n## Tools\n{}\n\n## Required Evidence\n{}\n\n## Capabilities\n{}\n",
        item.name,
        item.description.as_deref().unwrap_or("MFG manufacturing skill."),
        item.scope,
        item.source,
        item.domain.as_deref().unwrap_or("manufacturing"),
        item.status,
        item.risk,
        markdown_list(&item.tools),
        markdown_list(&item.required_evidence),
        markdown_list(&item.capabilities),
    )
}

fn markdown_list(values: &[String]) -> String {
    if values.is_empty() {
        return "- none".to_string();
    }
    values
        .iter()
        .map(|value| format!("- {value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn mfg_risk(skill: &MfgSkillManifest) -> &'static str {
    if skill
        .output_actions
        .iter()
        .any(|action| action.contains("dispatch") || action.contains("escalation"))
    {
        "controlled"
    } else if skill.tools.iter().any(|tool| tool.contains("cross_plane")) {
        "governed"
    } else {
        "review"
    }
}

fn filter_scope(items: Vec<SkillCatalogItem>, scope: Option<&str>) -> Vec<SkillCatalogItem> {
    match scope.unwrap_or("all") {
        "all" => items,
        scope => items
            .into_iter()
            .filter(|item| item.scope == scope || item.source.contains(scope))
            .collect(),
    }
}

fn normalize_surface(surface: Option<&str>) -> String {
    match surface.unwrap_or("webui").to_ascii_lowercase().as_str() {
        "tui" => "tui".to_string(),
        "cli" => "cli".to_string(),
        _ => "webui".to_string(),
    }
}

fn projection_capabilities(surface: &str) -> Vec<&'static str> {
    match surface {
        "cli" => vec![
            "catalog.read",
            "skill.view",
            "skill.import",
            "diagnostics.read",
        ],
        "tui" => vec![
            "catalog.read",
            "skill.view",
            "skill.validate",
            "skill.plan",
            "skill.run",
            "run.watch",
            "evidence.summary",
            "governance.queue",
        ],
        _ => vec![
            "catalog.read",
            "skill.view",
            "skill.validate",
            "skill.plan",
            "skill.run",
            "run.watch",
            "evidence.timeline",
            "evidence.diff",
            "governance.bulk",
            "imports.guided",
            "telemetry.dashboard",
        ],
    }
}

fn projection_actions(surface: &str) -> Vec<SkillAction> {
    let mut actions = vec![
        SkillAction {
            id: "view",
            label: "View",
            surface: "all",
            mutation: false,
        },
        SkillAction {
            id: "validate",
            label: "Validate",
            surface: "webui,tui",
            mutation: false,
        },
    ];
    if surface != "cli" {
        actions.extend([
            SkillAction {
                id: "plan",
                label: "Plan",
                surface: "webui,tui",
                mutation: false,
            },
            SkillAction {
                id: "run",
                label: "Run",
                surface: "webui,tui",
                mutation: true,
            },
            SkillAction {
                id: "watch",
                label: "Watch",
                surface: "webui,tui",
                mutation: false,
            },
        ]);
    }
    if surface == "webui" {
        actions.push(SkillAction {
            id: "bulk_govern",
            label: "Bulk Govern",
            surface: "webui",
            mutation: true,
        });
    }
    actions
}

fn projection_facets(items: &[SkillCatalogItem]) -> SkillProjectionFacets {
    let mut scopes = Vec::new();
    let mut domains = Vec::new();
    let mut tags = Vec::new();
    let mut risks = Vec::new();
    let mut statuses = Vec::new();
    for item in items {
        push_unique(&mut scopes, item.scope.clone());
        if let Some(domain) = &item.domain {
            push_unique(&mut domains, domain.clone());
        }
        for tag in &item.tags {
            push_unique(&mut tags, tag.clone());
        }
        push_unique(&mut risks, item.risk.clone());
        push_unique(&mut statuses, item.status.clone());
    }
    SkillProjectionFacets {
        scopes,
        domains,
        tags,
        risks,
        statuses,
    }
}

fn projection_diagnostics(items: &[SkillCatalogItem]) -> Vec<String> {
    let mut diagnostics = Vec::new();
    if !items.iter().any(|item| item.scope == "mfg") {
        diagnostics.push("mfg_skill_pack_unavailable".to_string());
    }
    if !items.iter().any(|item| item.scope == "local") {
        diagnostics.push("local_skill_registry_empty".to_string());
    }
    diagnostics
}

fn activation_projection(
    workspace_root: &Path,
    query: Option<&str>,
) -> Result<Option<serde_json::Value>, SkillServiceError> {
    let Some(query) = query.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let registry = SkillRegistry::discover(workspace_root);
    let result = SkillRouter::new(registry)
        .suggest(query)
        .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
    Ok(Some(serde_json::json!({
        "kind": "skills.activation",
        "query": result.query,
        "selected": result.selected,
        "candidates": result.candidates,
    })))
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempTree {
        root: PathBuf,
    }

    impl TempTree {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos();
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/tmp")
                .join(format!(
                    "gateway-skill-service-{name}-{}-{nonce}",
                    std::process::id()
                ));
            fs::create_dir_all(&root).expect("temp tree should be created");
            Self { root }
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn install_skill_uses_frontmatter_name_and_rejects_overwrite() {
        let temp = TempTree::new("install");
        let source = temp.root.join("source-skill");
        fs::create_dir_all(&source).expect("source skill dir should be created");
        fs::write(
            source.join("SKILL.md"),
            "---\nname: \"Display Skill\"\ndescription: demo\n---\n\nRun it.\n",
        )
        .expect("skill prompt should be written");

        let registry = temp.root.join("registry");
        let installed =
            install_skill_into(source.to_str().unwrap(), &temp.root, &registry).expect("install");

        assert_eq!(installed.invocation_name, "display-skill");
        assert_eq!(installed.display_name.as_deref(), Some("Display Skill"));
        assert!(registry.join("display-skill").join("SKILL.md").is_file());

        let error = install_skill_into(source.to_str().unwrap(), &temp.root, &registry)
            .expect_err("second install must not overwrite existing skill");
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn install_skill_rejects_non_skill_sources() {
        let temp = TempTree::new("invalid-source");
        let plain_dir = temp.root.join("plain-dir");
        fs::create_dir_all(&plain_dir).expect("plain dir should be created");
        let plain_file = temp.root.join("notes.txt");
        fs::write(&plain_file, "not a skill").expect("plain file should be written");

        let dir_error = resolve_skill_install_source(plain_dir.to_str().unwrap(), &temp.root)
            .expect_err("directories without SKILL.md are not installable");
        assert_eq!(dir_error.kind(), std::io::ErrorKind::InvalidInput);

        let file_error = resolve_skill_install_source(plain_file.to_str().unwrap(), &temp.root)
            .expect_err("non-markdown files are not installable");
        assert_eq!(file_error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn help_path_accepts_help_after_subcommand() {
        assert_eq!(help_path_from_args("install help"), Some(vec!["install"]));
        assert_eq!(help_path_from_args("view --help"), Some(vec!["view"]));
        assert_eq!(help_path_from_args("help install"), Some(Vec::new()));
    }
}
