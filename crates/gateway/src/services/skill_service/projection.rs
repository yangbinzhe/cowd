use std::{
    fs,
    path::{Path, PathBuf},
};

use cowd_app_host::AppRegistry;
use cowd_app_sdk::AppSkillDescriptor;
use serde::Serialize;
use skill::{profile_skill_package, SkillInfo, SkillRouter, SkillRouterConfig};

use super::{profile_provider::workspace_skill_snapshot, SkillServiceError};

pub(super) type SkillCatalogItem = AppSkillDescriptor;

#[derive(Debug, Serialize)]
pub(super) struct SkillFileEntry {
    pub(super) path: String,
    pub(super) name: String,
    pub(super) kind: &'static str,
    pub(super) size: Option<u64>,
    pub(super) primary: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct SkillAction {
    pub(super) id: &'static str,
    pub(super) label: &'static str,
    pub(super) surface: &'static str,
    pub(super) mutation: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct SkillProjection {
    pub(super) kind: &'static str,
    pub(super) surface: String,
    pub(super) catalog_count: usize,
    pub(super) capabilities: Vec<&'static str>,
    pub(super) actions: Vec<SkillAction>,
    pub(super) facets: SkillProjectionFacets,
    pub(super) queue: SkillProjectionQueue,
    pub(super) governance: SkillProjectionGovernance,
    pub(super) cache: super::profile_provider::SkillCacheHealth,
    pub(super) diagnostics: Vec<String>,
    pub(super) activation: Option<serde_json::Value>,
    pub(super) items: Vec<SkillCatalogItem>,
}

#[derive(Debug, Serialize)]
pub(super) struct SkillProjectionFacets {
    pub(super) scopes: Vec<String>,
    pub(super) domains: Vec<String>,
    pub(super) tags: Vec<String>,
    pub(super) risks: Vec<String>,
    pub(super) statuses: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct SkillProjectionQueue {
    pub(super) source: &'static str,
    pub(super) run_list_endpoint: &'static str,
    pub(super) supports_watch: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct SkillProjectionGovernance {
    pub(super) evidence_model: &'static str,
    pub(super) tool_fact_model: &'static str,
    pub(super) approval_model: &'static str,
}
pub(super) fn collect_skill_catalog(
    workspace_root: &Path,
    app_registry: &AppRegistry,
) -> Result<Vec<SkillCatalogItem>, SkillServiceError> {
    let mut items = app_registry.skills();
    for skill in workspace_skill_snapshot(workspace_root).skills.clone() {
        items.push(local_skill_catalog_item(skill));
    }
    items.sort_by(|left, right| {
        left.scope
            .cmp(&right.scope)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(items)
}

pub(super) fn find_catalog_item(
    workspace_root: &Path,
    app_registry: &AppRegistry,
    id: &str,
) -> Result<SkillCatalogItem, SkillServiceError> {
    collect_skill_catalog(workspace_root, app_registry)?
        .into_iter()
        .find(|item| item.id == id || item.name.eq_ignore_ascii_case(id))
        .ok_or_else(|| SkillServiceError::NotFound("skill not found".to_string()))
}

fn local_skill_catalog_item(skill: SkillInfo) -> SkillCatalogItem {
    let root = if skill.path.is_file() {
        skill.path.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else {
        skill.path.clone()
    };
    let profile = profile_skill_package(&root, &skill.name, None)
        .ok()
        .and_then(|profile| serde_json::to_value(profile).ok());
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
        profile,
        virtual_files: None,
        path: Some(skill.path.display().to_string()),
        shadowed_by: skill.shadowed_by.map(|source| format!("{source:?}")),
    }
}

pub(super) fn local_skill_root(item: &SkillCatalogItem) -> Result<PathBuf, SkillServiceError> {
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

pub(super) fn safe_skill_file_path(
    root: &Path,
    requested: &str,
) -> Result<PathBuf, SkillServiceError> {
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

pub(super) fn list_skill_files(root: &Path) -> std::io::Result<Vec<SkillFileEntry>> {
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

pub(super) fn app_virtual_files(item: &SkillCatalogItem) -> Option<serde_json::Value> {
    let virtual_files = item.virtual_files.as_ref()?;
    Some(serde_json::json!({
        "kind": "skills.files",
        "schema_version": 1,
        "skill": item,
        "root": virtual_files.root,
        "primary": virtual_files.primary,
        "files": virtual_files.files.iter().map(|file| serde_json::json!({
            "path": file.path,
            "name": file.name,
            "kind": file.kind,
            "size": file.content.len(),
            "primary": file.primary,
        })).collect::<Vec<_>>(),
    }))
}

pub(super) fn app_virtual_skill_file(
    item: &SkillCatalogItem,
    requested: &str,
) -> Option<(String, String)> {
    let files = item.virtual_files.as_ref()?;
    files
        .files
        .iter()
        .find(|file| file.path == requested)
        .map(|file| (file.content_type.clone(), file.content.clone()))
}

pub(super) fn filter_scope(
    items: Vec<SkillCatalogItem>,
    scope: Option<&str>,
) -> Vec<SkillCatalogItem> {
    match scope.unwrap_or("all") {
        "all" => items,
        scope => items
            .into_iter()
            .filter(|item| item.scope == scope || item.source.contains(scope))
            .collect(),
    }
}

pub(super) fn normalize_surface(surface: Option<&str>) -> String {
    match surface.unwrap_or("webui").to_ascii_lowercase().as_str() {
        "tui" => "tui".to_string(),
        "cli" => "cli".to_string(),
        _ => "webui".to_string(),
    }
}

pub(super) fn projection_capabilities(surface: &str) -> Vec<&'static str> {
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
            "skill.profile",
            "skill.validate",
            "skill.plan",
            "skill.run",
            "skill.import",
            "skill.maintenance.review",
            "run.watch",
            "evidence.summary",
            "governance.queue",
        ],
        _ => vec![
            "catalog.read",
            "skill.view",
            "skill.profile",
            "skill.validate",
            "skill.plan",
            "skill.run",
            "skill.import",
            "skill.maintenance.review",
            "run.watch",
            "evidence.timeline",
            "evidence.diff",
            "governance.bulk",
            "imports.guided",
            "telemetry.dashboard",
        ],
    }
}

pub(super) fn projection_actions(surface: &str) -> Vec<SkillAction> {
    let mut actions = vec![
        SkillAction {
            id: "view",
            label: "View",
            surface: "all",
            mutation: false,
        },
        SkillAction {
            id: "profile",
            label: "Profile",
            surface: "webui,tui",
            mutation: false,
        },
        SkillAction {
            id: "inspect",
            label: "Inspect",
            surface: "webui,tui",
            mutation: false,
        },
    ];
    if surface != "cli" {
        actions.extend([
            SkillAction {
                id: "validate",
                label: "Validate",
                surface: "webui,tui",
                mutation: false,
            },
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
                id: "import",
                label: "Import",
                surface: "webui,tui",
                mutation: true,
            },
            SkillAction {
                id: "maintenance_review",
                label: "Maintenance evidence",
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

pub(super) fn projection_facets(items: &[SkillCatalogItem]) -> SkillProjectionFacets {
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

pub(super) fn projection_diagnostics(items: &[SkillCatalogItem]) -> Vec<String> {
    let mut diagnostics = Vec::new();
    if !items.iter().any(|item| item.virtual_files.is_some()) {
        diagnostics.push("application_skill_catalog_empty".to_string());
    }
    if !items.iter().any(|item| item.scope == "local") {
        diagnostics.push("local_skill_registry_empty".to_string());
    }
    diagnostics
}

pub(super) fn activation_projection(
    workspace_root: &Path,
    query: Option<&str>,
) -> Result<Option<serde_json::Value>, SkillServiceError> {
    let Some(query) = query.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let snapshot = workspace_skill_snapshot(workspace_root);
    let result =
        SkillRouter::suggest_snapshot(&snapshot.skills, query, SkillRouterConfig::default())
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

    fn temp_workspace(name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let root = std::env::temp_dir().join(format!(
            "cowd-gateway-skill-projection-{name}-{millis}-{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create temp workspace");
        root
    }

    #[test]
    fn local_skill_catalog_item_projects_inspection_profile() {
        let workspace = temp_workspace("profile");
        let skill_root = workspace.join(".cowd").join("skills").join("profile-demo");
        fs::create_dir_all(&skill_root).expect("create skill root");
        fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: profile-demo\ndescription: Profile demo\n---\n# Profile Demo\n",
        )
        .expect("write skill");
        fs::write(
            skill_root.join("pyproject.toml"),
            "[project]\nname='profile-demo'\n",
        )
        .expect("write pyproject");

        let items = collect_skill_catalog(&workspace, &AppRegistry::default()).expect("catalog");
        let item = items
            .iter()
            .find(|item| item.id == "local:profile-demo")
            .expect("local profile skill");
        let profile = item.profile.as_ref().expect("profile is projected");

        assert_eq!(profile["skill_id"], "profile-demo");
        assert_eq!(profile["lifecycle_status"], "usable_runtime");
        assert!(profile["adapters"]
            .as_array()
            .expect("adapters")
            .iter()
            .any(|adapter| adapter == "sandbox_exec"));
        assert!(profile["entrypoints"]
            .as_array()
            .expect("entrypoints")
            .iter()
            .any(|entry| entry["path"] == "SKILL.md"));

        fs::remove_dir_all(workspace).expect("cleanup");
    }

    #[test]
    fn generic_projection_exposes_readiness_backed_skill_actions() {
        let webui_capabilities = projection_capabilities("webui");
        assert!(webui_capabilities.contains(&"skill.profile"));
        assert!(webui_capabilities.contains(&"skill.maintenance.review"));
        assert!(webui_capabilities.contains(&"skill.validate"));
        assert!(webui_capabilities.contains(&"skill.plan"));
        assert!(webui_capabilities.contains(&"skill.run"));
        assert!(webui_capabilities.contains(&"run.watch"));

        let action_ids = projection_actions("webui")
            .iter()
            .map(|action| action.id)
            .collect::<Vec<_>>();
        assert!(action_ids.contains(&"view"));
        assert!(action_ids.contains(&"profile"));
        assert!(action_ids.contains(&"maintenance_review"));
        assert!(action_ids.contains(&"validate"));
        assert!(action_ids.contains(&"plan"));
        assert!(action_ids.contains(&"run"));
        assert!(!action_ids.contains(&"watch"));
    }
}
