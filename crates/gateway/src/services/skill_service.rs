use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use app_mfg::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_skill_pack, skill_agent_node_id, MfgSkillManifest,
};
use serde::{Deserialize, Serialize};
use skill_service::{SkillInfo, SkillRegistry, SkillRouter};

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
