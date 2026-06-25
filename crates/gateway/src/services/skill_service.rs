use std::{collections::BTreeSet, fs, path::Path};

use crate::command::slash::SkillSlashDispatch;
use app_mfg::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill, skill_agent_node_id,
};
use serde::Deserialize;
use skill_service::{SkillManager, SkillViewInput};

use super::{ServiceEnvelope, SkillService};

mod local_command;
mod projection;
use local_command::{
    classify_static_skill_command, discover_skill_root_paths, help_path_from_args, install_skill,
    is_help_arg, local_skill_summaries, normalize_optional_args, render_skill_install_report,
    render_skill_install_report_json, render_skill_view_report, render_skills_report,
    render_skills_report_json, render_skills_usage, render_skills_usage_json,
};
use projection::{
    activation_projection, collect_skill_catalog, filter_scope, find_catalog_item, find_mfg_skill,
    list_skill_files, local_skill_root, mfg_virtual_files, mfg_virtual_skill_markdown,
    normalize_surface, projection_actions, projection_capabilities, projection_diagnostics,
    projection_facets, required_incident_id, safe_skill_file_path, SkillProjection,
    SkillProjectionGovernance, SkillProjectionQueue,
};
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

#[cfg(test)]
mod tests {
    use super::local_command::{install_skill_into, resolve_skill_install_source};
    use super::*;
    use std::path::PathBuf;
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
