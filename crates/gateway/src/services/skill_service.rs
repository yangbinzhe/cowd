use std::{
    fs,
    hash::{Hash, Hasher},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::command::slash::SkillSlashDispatch;
use chrono::Utc;
use serde::Deserialize;
use skill::{
    evaluate_skill_maintenance, inspect_skill_package, SkillActionKind, SkillCreateInput,
    SkillDeleteInput, SkillMaintenanceAction, SkillManager, SkillRunEvidence, SkillRunPlan,
    SkillRunReceipt, SkillRunRecord, SkillRunStatus, SkillUsageSignal, SkillViewInput,
};

use super::{ServiceEnvelope, SkillService};

mod local_command;
pub(crate) mod profile_provider;
mod projection;
mod run_store;
use local_command::{
    classify_static_skill_command, discover_skill_root_paths, help_path_from_args, install_skill,
    install_skill_into, is_help_arg, local_skill_summaries, normalize_optional_args,
    render_skill_install_report, render_skill_install_report_json, render_skill_view_report,
    render_skills_report, render_skills_report_json, render_skills_usage, render_skills_usage_json,
};
use projection::{
    activation_projection, app_virtual_files, app_virtual_skill_file, collect_skill_catalog,
    filter_scope, find_catalog_item, list_skill_files, local_skill_root, normalize_surface,
    projection_actions, projection_capabilities, projection_diagnostics, projection_facets,
    safe_skill_file_path, SkillProjection, SkillProjectionGovernance, SkillProjectionQueue,
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
pub(crate) struct SkillMaintenanceEvaluateRequest {
    #[serde(default)]
    pub(crate) request_id: Option<String>,
    pub(crate) skill_id: String,
    #[serde(default)]
    pub(crate) selected_count: u32,
    #[serde(default)]
    pub(crate) success_count: u32,
    #[serde(default)]
    pub(crate) failure_count: u32,
    #[serde(default)]
    pub(crate) correction_count: u32,
    #[serde(default)]
    pub(crate) activation_gap_count: u32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SkillFileQuery {
    #[serde(default)]
    pub(crate) path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SkillActionRequest {
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) objective: Option<String>,
    #[serde(default)]
    pub(crate) reason: Option<String>,
    #[serde(default)]
    pub(crate) payload: Option<serde_json::Value>,
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
    pub(crate) fn install_uploaded_tar(
        &self,
        archive_name: &str,
        bytes: &[u8],
    ) -> Result<serde_json::Value, SkillServiceError> {
        let install_root = crate::skill_static::default_skill_install_root()
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
        self.install_uploaded_tar_into(archive_name, bytes, &install_root)
    }

    fn install_uploaded_tar_into(
        &self,
        archive_name: &str,
        bytes: &[u8],
        install_root: &Path,
    ) -> Result<serde_json::Value, SkillServiceError> {
        const MAX_ARCHIVE_BYTES: usize = 32 * 1024 * 1024;
        const MAX_EXTRACTED_BYTES: u64 = 64 * 1024 * 1024;
        const MAX_FILES: usize = 512;
        if bytes.is_empty() || bytes.len() > MAX_ARCHIVE_BYTES {
            return Err(SkillServiceError::BadRequest(format!(
                "skill archive must contain 1..={MAX_ARCHIVE_BYTES} bytes"
            )));
        }
        if !archive_name.to_ascii_lowercase().ends_with(".tar") {
            return Err(SkillServiceError::BadRequest(
                "skill package must be an uncompressed .tar archive".to_string(),
            ));
        }
        let staging =
            std::env::temp_dir().join(format!("cowd-skill-upload-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&staging)
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
        let result = (|| {
            let mut archive = tar::Archive::new(std::io::Cursor::new(bytes));
            let entries = archive
                .entries()
                .map_err(|error| SkillServiceError::BadRequest(error.to_string()))?;
            let mut file_count = 0usize;
            let mut extracted_bytes = 0u64;
            for entry in entries {
                let mut entry =
                    entry.map_err(|error| SkillServiceError::BadRequest(error.to_string()))?;
                let entry_type = entry.header().entry_type();
                if !entry_type.is_file() && !entry_type.is_dir() {
                    return Err(SkillServiceError::BadRequest(
                        "skill archives may contain regular files and directories only".to_string(),
                    ));
                }
                let path = entry
                    .path()
                    .map_err(|error| SkillServiceError::BadRequest(error.to_string()))?;
                if path.is_absolute()
                    || path.components().any(|component| {
                        matches!(
                            component,
                            std::path::Component::ParentDir
                                | std::path::Component::RootDir
                                | std::path::Component::Prefix(_)
                        )
                    })
                {
                    return Err(SkillServiceError::BadRequest(
                        "skill archive contains an unsafe path".to_string(),
                    ));
                }
                if entry_type.is_file() {
                    file_count = file_count.saturating_add(1);
                }
                extracted_bytes =
                    extracted_bytes.saturating_add(entry.header().size().unwrap_or_default());
                if file_count > MAX_FILES || extracted_bytes > MAX_EXTRACTED_BYTES {
                    return Err(SkillServiceError::BadRequest(
                        "skill archive exceeds the extracted file or byte limit".to_string(),
                    ));
                }
                entry
                    .unpack_in(&staging)
                    .map_err(|error| SkillServiceError::BadRequest(error.to_string()))?;
            }
            let package_root = if staging.join("SKILL.md").is_file() {
                staging.clone()
            } else {
                let roots = fs::read_dir(&staging)
                    .map_err(|error| SkillServiceError::Internal(error.to_string()))?
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .filter(|path| path.is_dir() && path.join("SKILL.md").is_file())
                    .collect::<Vec<_>>();
                if roots.len() != 1 {
                    return Err(SkillServiceError::BadRequest(
                        "skill archive must contain one package root with SKILL.md".to_string(),
                    ));
                }
                roots[0].clone()
            };
            let inspection = inspect_skill_package(&package_root)
                .map_err(|error| SkillServiceError::BadRequest(error.to_string()))?;
            if !inspection.blocked_reasons.is_empty() {
                return Err(SkillServiceError::BadRequest(format!(
                    "skill package inspection blocked installation: {}",
                    inspection.blocked_reasons.join("; ")
                )));
            }
            let installed = install_skill_into(
                package_root.to_string_lossy().as_ref(),
                &staging,
                install_root,
            )
            .map_err(|error| SkillServiceError::BadRequest(error.to_string()))?;
            Ok(serde_json::json!({
                "kind": "skills.management.installed",
                "schema_version": 1,
                "receipt": render_skill_install_report_json(&installed),
                "inspection": inspection,
                "changed_refs": [installed.installed_path],
            }))
        })();
        let _ = fs::remove_dir_all(&staging);
        result
    }

    pub(crate) fn cached_translation(&self, key: &str) -> Option<serde_json::Value> {
        let mut cache = self
            .translation_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = cache.entries.get(key).cloned()?;
        cache.order.retain(|candidate| candidate != key);
        cache.order.push_back(key.to_string());
        Some(value)
    }

    pub(crate) fn cache_translation(&self, key: String, value: serde_json::Value, capacity: usize) {
        if capacity == 0 {
            return;
        }
        let mut cache = self
            .translation_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.order.retain(|candidate| candidate != &key);
        cache.entries.insert(key.clone(), value);
        cache.order.push_back(key);
        while cache.entries.len() > capacity {
            let Some(expired) = cache.order.pop_front() else {
                break;
            };
            cache.entries.remove(&expired);
        }
    }

    pub(crate) fn catalog_envelope(&self) -> ServiceEnvelope {
        self.envelope("catalog")
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.catalog_envelope(),
            self.envelope("projection"),
            self.envelope("evolution_skill_draft"),
        ]
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
        app_registry: &cowd_app_host::AppRegistry,
        args: Option<&str>,
    ) -> Result<SkillSlashDispatch, String> {
        let dispatch = classify_static_skill_command(args);
        if let SkillSlashDispatch::Invoke(ref prompt) = dispatch {
            let skill_token = prompt
                .trim_start_matches('$')
                .split_whitespace()
                .next()
                .unwrap_or_default();
            if !skill_token.is_empty()
                && find_catalog_item(workspace_root, app_registry, skill_token).is_err()
            {
                let mut message = format!("Unknown skill: {skill_token}");
                if let Ok(available) = collect_skill_catalog(workspace_root, app_registry) {
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
        app_registry: &cowd_app_host::AppRegistry,
        query: SkillCatalogQuery,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let items = filter_scope(
            collect_skill_catalog(workspace_root, app_registry)?,
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
        app_registry: &cowd_app_host::AppRegistry,
        query: SkillProjectionQuery,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let surface = normalize_surface(query.surface.as_deref());
        let items = collect_skill_catalog(workspace_root, app_registry)?;
        let activation = activation_projection(workspace_root, query.query.as_deref())?;
        serde_json::to_value(SkillProjection {
            kind: "skills.projection",
            surface: surface.clone(),
            catalog_count: items.len(),
            capabilities: projection_capabilities(&surface),
            actions: projection_actions(&surface),
            facets: projection_facets(&items),
            queue: SkillProjectionQueue {
                source: "gateway.skill_runs",
                run_list_endpoint: "/api/skills/runs",
                supports_watch: surface != "cli",
            },
            governance: SkillProjectionGovernance {
                evidence_model: "matrix.evidence.packet + agent_evidence + tool_invocation",
                tool_fact_model: "tool.execution_plan + tool.invocation.runtime_event",
                approval_model: "quality_gate + cross_plane_policy",
            },
            cache: profile_provider::workspace_skill_cache_health(workspace_root),
            diagnostics: projection_diagnostics(&items),
            activation,
            items,
        })
        .map_err(|error| SkillServiceError::Internal(error.to_string()))
    }

    pub(crate) fn maintenance_evaluate(
        &self,
        request: SkillMaintenanceEvaluateRequest,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let skill_id = request.skill_id.trim();
        if skill_id.is_empty() {
            return Err(SkillServiceError::BadRequest(
                "skill_id is required".to_string(),
            ));
        }
        let signal = SkillUsageSignal {
            skill_id: skill_id.to_string(),
            selected_count: request.selected_count,
            success_count: request.success_count,
            failure_count: request.failure_count,
            correction_count: request.correction_count,
            activation_gap_count: request.activation_gap_count,
        };
        let action = evaluate_skill_maintenance(&signal);
        Ok(serde_json::json!({
            "kind": "skills.maintenance.evaluation",
            "schema_version": 1,
            "request_id": request.request_id,
            "skill_id": signal.skill_id,
            "usage_signal": signal,
            "action": maintenance_action_wire(&action),
            "reason": maintenance_action_reason(&action),
        }))
    }

    pub(crate) fn evolution_skill_draft(
        &self,
        proposal: &runtime::EvolutionProposal,
    ) -> serde_json::Value {
        serde_json::json!({
            "kind": "skills.evolution_draft",
            "schema_version": 1,
            "envelope": self.envelope("evolution_skill_draft"),
            "proposal_id": proposal.proposal_id,
            "draft": proposal.to_skill_draft(),
            "owner": "gateway.skill_service",
            "mainline_modified": false,
        })
    }

    pub(crate) fn runs(&self, config_home: &Path) -> Result<serde_json::Value, SkillServiceError> {
        let items = run_store::load_runs(config_home)?;
        Ok(serde_json::json!({
            "kind": "skills.runs",
            "schema_version": 1,
            "store": "gateway.skill.runs.jsonl",
            "count": items.len(),
            "items": items,
        }))
    }

    pub(crate) fn run_detail(
        &self,
        config_home: &Path,
        run_id: &str,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let run = run_store::find_run(config_home, run_id)?;
        Ok(serde_json::json!({
            "kind": "skills.run.detail",
            "schema_version": 1,
            "run": run,
        }))
    }

    pub(crate) fn run_action(
        &self,
        workspace_root: &Path,
        app_registry: &cowd_app_host::AppRegistry,
        config_home: &Path,
        id: &str,
        action: SkillActionKind,
        request: SkillActionRequest,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, app_registry, id)?;
        let now = Utc::now().to_rfc3339();
        let run_id = skill_run_id(&item.id, action);
        let inspection = inspect_skill_item(&item);
        let (inspection, inspection_error) = match inspection {
            Ok(inspection) => (inspection, None),
            Err(error) => (None, Some(error.message())),
        };
        let plan = skill_run_plan(&item, action, inspection.as_ref(), &request);
        let blocked_reasons = skill_action_blockers(
            &item,
            action,
            inspection.as_ref(),
            inspection_error.as_deref(),
        );
        let status = if inspection_error.is_some() {
            SkillRunStatus::Failed
        } else if blocked_reasons.is_empty() {
            SkillRunStatus::Succeeded
        } else {
            SkillRunStatus::Rejected
        };
        let receipt = SkillRunReceipt {
            run_id: run_id.clone(),
            skill_id: item.id.clone(),
            action,
            status,
            reason: skill_action_reason(action, status, inspection_error.as_deref()),
            risk_level: item.risk.clone(),
            blocked_reasons,
            tool_permission_summary: tool_permission_summary(&item, action, status),
            evidence: skill_run_evidence(&item, inspection.as_ref(), request.payload.as_ref()),
        };
        let record = SkillRunRecord {
            run_id: run_id.clone(),
            skill_id: item.id.clone(),
            action,
            status,
            created_at: now.clone(),
            updated_at: now,
            session_id: request.session_id,
            inspection,
            plan: Some(plan),
            receipt: Some(receipt.clone()),
            error: inspection_error,
        };
        run_store::append_run(config_home, &record)?;
        Ok(serde_json::json!({
            "kind": "skills.action.receipt",
            "schema_version": 1,
            "run": record,
            "receipt": receipt,
        }))
    }

    pub(crate) fn detail(
        &self,
        workspace_root: &Path,
        app_registry: &cowd_app_host::AppRegistry,
        id: &str,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, app_registry, id)?;
        let management = managed_skill_root(&item).map_or_else(
            |_| {
                serde_json::json!({
                    "managed": false,
                    "can_delete": false,
                    "reason": "only user-installed skills can be modified",
                })
            },
            |root| {
                serde_json::json!({
                    "managed": true,
                    "can_delete": true,
                    "root": root,
                })
            },
        );
        Ok(serde_json::json!({
            "kind": "skills.detail",
            "schema_version": 1,
            "skill": item,
            "management": management,
        }))
    }

    pub(crate) fn create_managed(
        &self,
        input: SkillCreateInput,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let root = crate::skill_static::default_skill_install_root()
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
        fs::create_dir_all(&root)
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
        let output = SkillManager::new(vec![root]).create_skill(input);
        if !output.success {
            return Err(SkillServiceError::BadRequest(output.message));
        }
        Ok(serde_json::json!({
            "kind": "skills.management.created",
            "schema_version": 1,
            "receipt": output,
            "changed_refs": [output.path],
        }))
    }

    pub(crate) fn delete_managed(
        &self,
        workspace_root: &Path,
        app_registry: &cowd_app_host::AppRegistry,
        id: &str,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, app_registry, id)?;
        let managed_root = managed_skill_root(&item)?;
        let output = SkillManager::new(vec![managed_root]).delete_skill(SkillDeleteInput {
            name: item.name,
            force: true,
        });
        if !output.success {
            return Err(SkillServiceError::BadRequest(output.message));
        }
        Ok(serde_json::json!({
            "kind": "skills.management.deleted",
            "schema_version": 1,
            "receipt": output,
            "changed_refs": [id],
        }))
    }

    pub(crate) fn files(
        &self,
        workspace_root: &Path,
        app_registry: &cowd_app_host::AppRegistry,
        id: &str,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, app_registry, id)?;
        if let Some(files) = app_virtual_files(&item) {
            return Ok(files);
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
        app_registry: &cowd_app_host::AppRegistry,
        id: &str,
        query: SkillFileQuery,
    ) -> Result<serde_json::Value, SkillServiceError> {
        let item = find_catalog_item(workspace_root, app_registry, id)?;
        let requested = query.path.unwrap_or_else(|| "SKILL.md".to_string());
        if item.virtual_files.is_some() {
            let Some((content_type, content)) = app_virtual_skill_file(&item, &requested) else {
                return Err(SkillServiceError::NotFound(
                    "skill file not found".to_string(),
                ));
            };
            return Ok(serde_json::json!({
                "kind": "skills.file.raw",
                "schema_version": 1,
                "skill": item,
                "path": requested,
                "content_type": content_type,
                "content": content,
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
}

fn managed_skill_root(
    item: &projection::SkillCatalogItem,
) -> Result<std::path::PathBuf, SkillServiceError> {
    if item.virtual_files.is_some() || item.scope != "local" {
        return Err(SkillServiceError::BadRequest(
            "only user-installed skills can be modified".to_string(),
        ));
    }
    let configured_root = crate::skill_static::default_skill_install_root()
        .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
    let configured_root = configured_root
        .canonicalize()
        .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
    let skill_root = local_skill_root(item)?;
    if skill_root.parent() != Some(configured_root.as_path()) {
        return Err(SkillServiceError::BadRequest(
            "workspace, bundled, and application skills are read-only".to_string(),
        ));
    }
    Ok(configured_root)
}

fn skill_run_id(skill_id: &str, action: SkillActionKind) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    skill_id.hash(&mut hasher);
    action.as_str().hash(&mut hasher);
    millis.hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    format!("skillrun-{millis}-{:08x}", hasher.finish() as u32)
}

fn inspect_skill_item(
    item: &projection::SkillCatalogItem,
) -> Result<Option<harness_contract::skill::SkillInspectionReport>, SkillServiceError> {
    if item.scope == "mfg" {
        return Ok(None);
    }
    let root = local_skill_root(item)?;
    inspect_skill_package(&root)
        .map(Some)
        .map_err(|error| SkillServiceError::Internal(error.to_string()))
}

fn skill_run_plan(
    item: &projection::SkillCatalogItem,
    action: SkillActionKind,
    inspection: Option<&harness_contract::skill::SkillInspectionReport>,
    request: &SkillActionRequest,
) -> SkillRunPlan {
    let mut steps = vec![
        format!("resolve skill `{}` from gateway catalog", item.id),
        "inspect package structure and risk signals".to_string(),
        "summarize required tool permissions before side effects".to_string(),
    ];
    if matches!(action, SkillActionKind::Run) {
        steps.push(
            "hand execution intent to tools permission gate before any script/runtime action"
                .to_string(),
        );
    }
    if let Some(objective) = request
        .objective
        .as_deref()
        .filter(|item| !item.trim().is_empty())
    {
        steps.push(format!("apply caller objective: {}", objective.trim()));
    }
    if let Some(reason) = request
        .reason
        .as_deref()
        .filter(|item| !item.trim().is_empty())
    {
        steps.push(format!("record operator reason: {}", reason.trim()));
    }

    let mut required_tools = item.tools.clone();
    if required_tools.is_empty()
        && inspection.is_some_and(|report| !report.entrypoints.is_empty())
        && matches!(action, SkillActionKind::Run)
    {
        required_tools.push("tools.permission_gate".to_string());
    }

    SkillRunPlan {
        summary: format!(
            "{} skill `{}` through Gateway skill lifecycle",
            action.as_str(),
            item.id
        ),
        steps,
        required_tools,
        expected_side_effects: if matches!(action, SkillActionKind::Run) {
            vec!["no direct side effect before tools permission approval".to_string()]
        } else {
            vec!["none".to_string()]
        },
    }
}

fn skill_action_blockers(
    item: &projection::SkillCatalogItem,
    action: SkillActionKind,
    inspection: Option<&harness_contract::skill::SkillInspectionReport>,
    inspection_error: Option<&str>,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if let Some(error) = inspection_error {
        blockers.push(format!("inspection_failed: {error}"));
    }
    if let Some(report) = inspection {
        blockers.extend(report.blocked_reasons.clone());
        if matches!(action, SkillActionKind::Run)
            && report.recommended_adapters.iter().any(|adapter| {
                !matches!(
                    adapter,
                    harness_contract::skill::SkillAdapterKind::PromptOnly
                        | harness_contract::skill::SkillAdapterKind::ToolGuided
                )
            })
        {
            blockers.push(
                "runtime execution requires an installed tools adapter and explicit permission"
                    .to_string(),
            );
        }
    }
    if matches!(action, SkillActionKind::Run) && matches!(item.risk.as_str(), "high" | "critical") {
        blockers.push("high risk skill run requires explicit approval/tool gate".to_string());
    }
    blockers.sort();
    blockers.dedup();
    blockers
}

fn skill_action_reason(
    action: SkillActionKind,
    status: SkillRunStatus,
    inspection_error: Option<&str>,
) -> String {
    if let Some(error) = inspection_error {
        return format!("inspection failed before {}: {error}", action.as_str());
    }
    match (action, status) {
        (SkillActionKind::Validate, SkillRunStatus::Succeeded) => {
            "skill package validated and governance receipt recorded".to_string()
        }
        (SkillActionKind::Plan, SkillRunStatus::Succeeded) => {
            "skill execution plan generated without side effects".to_string()
        }
        (SkillActionKind::Run, SkillRunStatus::Succeeded) => {
            "skill run intent accepted without direct script execution".to_string()
        }
        (_, SkillRunStatus::Rejected) => {
            "skill action rejected until required tool permission or adapter is available"
                .to_string()
        }
        (_, SkillRunStatus::Failed) => "skill action failed".to_string(),
        (_, SkillRunStatus::Queued | SkillRunStatus::Running) => "skill action pending".to_string(),
    }
}

fn tool_permission_summary(
    item: &projection::SkillCatalogItem,
    action: SkillActionKind,
    status: SkillRunStatus,
) -> String {
    if !matches!(action, SkillActionKind::Run) {
        return "no tool execution requested".to_string();
    }
    if status == SkillRunStatus::Rejected {
        return "direct execution blocked; tools permission gate must approve runtime adapter"
            .to_string();
    }
    if item.tools.is_empty() {
        "prompt-only skill activation recorded; no tool invocation was executed".to_string()
    } else {
        format!(
            "tool intent recorded for permission gate: {}",
            item.tools.join(", ")
        )
    }
}

fn skill_run_evidence(
    item: &projection::SkillCatalogItem,
    inspection: Option<&harness_contract::skill::SkillInspectionReport>,
    payload: Option<&serde_json::Value>,
) -> Vec<SkillRunEvidence> {
    let mut evidence = vec![SkillRunEvidence {
        kind: "skill.catalog".to_string(),
        summary: format!("resolved `{}` from {} scope", item.id, item.scope),
        refs: item.path.clone().into_iter().collect(),
    }];
    if let Some(report) = inspection {
        evidence.push(SkillRunEvidence {
            kind: "skill.inspection".to_string(),
            summary: format!(
                "{} files, {} entrypoints, {} risk signals",
                report.detected_files.len(),
                report.entrypoints.len(),
                report.risk_signals.len()
            ),
            refs: report.detected_files.iter().take(12).cloned().collect(),
        });
    }
    if payload.is_some() {
        evidence.push(SkillRunEvidence {
            kind: "skill.request.payload".to_string(),
            summary: "caller supplied action payload".to_string(),
            refs: Vec::new(),
        });
    }
    evidence
}

fn maintenance_action_wire(action: &SkillMaintenanceAction) -> &'static str {
    match action {
        SkillMaintenanceAction::KeepActive => "keep_active",
        SkillMaintenanceAction::GenerateRevisionCandidate => "generate_revision_candidate",
        SkillMaintenanceAction::Deprecate => "deprecate",
        SkillMaintenanceAction::Archive => "archive",
    }
}

fn maintenance_action_reason(action: &SkillMaintenanceAction) -> &'static str {
    match action {
        SkillMaintenanceAction::KeepActive => "usage_signal_healthy",
        SkillMaintenanceAction::GenerateRevisionCandidate => {
            "usage_signal_needs_revision_candidate"
        }
        SkillMaintenanceAction::Deprecate => "usage_signal_failed_without_success",
        SkillMaintenanceAction::Archive => "usage_signal_unused",
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
                    "gateway-skill-{name}-{}-{nonce}",
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
    fn uploaded_skill_tar_is_inspected_and_installed_as_one_package() {
        let temp = TempTree::new("uploaded-tar");
        let body = b"---\nname: Uploaded Skill\ndescription: Installed from WebUI\n---\n\nUse verified evidence.\n";
        let mut archive = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_size(body.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(&mut header, "uploaded/SKILL.md", &body[..])
            .expect("append skill prompt");
        let bytes = archive.into_inner().expect("finish archive");

        let response = SkillService::new()
            .install_uploaded_tar_into("uploaded.tar", &bytes, &temp.root.join("registry"))
            .expect("uploaded package installs");

        assert_eq!(response["kind"], "skills.management.installed");
        assert!(temp.root.join("registry/uploaded-skill/SKILL.md").is_file());
        assert!(response["inspection"]["blocked_reasons"]
            .as_array()
            .is_some_and(Vec::is_empty));
    }

    #[test]
    fn skill_maintenance_evaluate_recommends_revision_for_repeated_corrections() {
        let service = SkillService::new();

        let response = service
            .maintenance_evaluate(SkillMaintenanceEvaluateRequest {
                request_id: Some("req-1".to_string()),
                skill_id: "plan-review".to_string(),
                selected_count: 5,
                success_count: 3,
                failure_count: 1,
                correction_count: 2,
                activation_gap_count: 0,
            })
            .expect("maintenance evaluation should succeed");

        assert_eq!(response["kind"], "skills.maintenance.evaluation");
        assert_eq!(response["request_id"], "req-1");
        assert_eq!(response["skill_id"], "plan-review");
        assert_eq!(response["action"], "generate_revision_candidate");
        assert_eq!(response["reason"], "usage_signal_needs_revision_candidate");
    }

    #[test]
    fn app_owned_skills_are_served_through_the_generic_registry_and_virtual_file_flow() {
        let temp = TempTree::new("app-owned-catalogue");
        let services = crate::services::GatewayServices::baseline();
        let service = SkillService::new();

        let catalogue = service
            .catalog(
                &temp.root,
                services.app_registry.as_ref(),
                SkillCatalogQuery {
                    scope: Some("mfg".to_string()),
                },
            )
            .expect("generic application skill catalogue");
        let skills = catalogue["items"]
            .as_array()
            .expect("catalogue skill array");
        assert_eq!(skills.len(), 7);
        assert!(skills
            .iter()
            .any(|skill| skill["id"] == "mfg:supply-risk-analyst"));

        let raw = service
            .raw_file(
                &temp.root,
                services.app_registry.as_ref(),
                "mfg:supply-risk-analyst",
                SkillFileQuery {
                    path: Some("SKILL.md".to_string()),
                },
            )
            .expect("generic virtual skill file");
        assert_eq!(raw["content_type"], "text/markdown");
        assert!(raw["content"]
            .as_str()
            .is_some_and(|content| content.contains("Required Evidence")));
    }

    #[test]
    fn help_path_accepts_help_after_subcommand() {
        assert_eq!(help_path_from_args("install help"), Some(vec!["install"]));
        assert_eq!(help_path_from_args("view --help"), Some(vec!["view"]));
        assert_eq!(help_path_from_args("help install"), Some(Vec::new()));
    }

    #[test]
    fn translation_cache_is_bounded_and_refreshes_recent_entries() {
        let service = SkillService::new();
        service.cache_translation("first".to_string(), serde_json::json!({"value": 1}), 2);
        service.cache_translation("second".to_string(), serde_json::json!({"value": 2}), 2);
        assert_eq!(service.cached_translation("first").unwrap()["value"], 1);

        service.cache_translation("third".to_string(), serde_json::json!({"value": 3}), 2);

        assert!(service.cached_translation("second").is_none());
        assert_eq!(service.cached_translation("first").unwrap()["value"], 1);
        assert_eq!(service.cached_translation("third").unwrap()["value"], 3);
    }
}
