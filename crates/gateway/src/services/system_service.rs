use std::{
    fs,
    path::{Component, Path},
    sync::{Mutex, OnceLock},
};

use provider::ToolDefinition;
use runtime::{
    classify_intent, plan_context_fanout, tool_execution_profile, ConfigLoader, JsonValue,
    RuntimeConfig, ToolSafetyCategory,
};
use serde::Serialize;
use tools::GlobalToolRegistry;

use super::SystemService;

static TOOL_CWD_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ToolOperationReceipt {
    pub(crate) request_id: String,
    pub(crate) tool_name: String,
    pub(crate) mode: String,
    pub(crate) risk: String,
    pub(crate) status: String,
    pub(crate) changed_refs: Vec<String>,
    pub(crate) audit_ref: String,
    pub(crate) data: serde_json::Value,
    pub(crate) warnings: Vec<String>,
    pub(crate) next_actions: Vec<String>,
}

impl SystemService {
    pub(crate) fn tool_catalog(&self, tools: Vec<ToolDefinition>) -> serde_json::Value {
        let rows: Vec<serde_json::Value> = tools
            .iter()
            .map(|tool| {
                let profile = tool_execution_profile(&tool.name);
                serde_json::json!({
                    "name": tool.name,
                    "description": tool.description,
                    "enabled": true,
                    "safety_category": profile.safety_category,
                    "cache_policy": profile.cache_policy,
                    "prepared_readonly_supported": profile.prepared_readonly_supported,
                    "max_concurrency": profile.max_concurrency,
                    "timeout_secs": profile.timeout_secs,
                    "managed_tags": managed_tool_tags(&tool.name),
                })
            })
            .collect();
        serde_json::json!({ "tools": rows, "count": rows.len() })
    }

    pub(crate) fn is_prepared_readonly_tool(&self, tool_name: &str) -> bool {
        let profile = tool_execution_profile(tool_name);
        profile.safety_category == ToolSafetyCategory::ReadOnly
            && profile.prepared_readonly_supported
    }

    pub(crate) fn execute_tool_receipt(
        &self,
        registry: &GlobalToolRegistry,
        workspace_root: &Path,
        tool_name: &str,
        input: serde_json::Value,
        mode: impl Into<String>,
        risk: impl Into<String>,
    ) -> Result<ToolOperationReceipt, String> {
        let mode = mode.into();
        let risk = risk.into();
        let output = self.with_workspace_root(workspace_root, || {
            registry
                .execute(tool_name, &input)
                .map_err(|error| error.to_string())
        })?;
        let data = serde_json::from_str::<serde_json::Value>(&output)
            .unwrap_or_else(|_| serde_json::json!({ "text": output }));
        let changed_refs = changed_refs_for_tool(tool_name, &data);
        let status = if data.get("error").is_some() {
            "failed"
        } else {
            "ok"
        };
        Ok(ToolOperationReceipt {
            request_id: format!("tool-op-{}", chrono::Utc::now().timestamp_millis()),
            tool_name: tool_name.to_string(),
            mode,
            risk,
            status: status.to_string(),
            changed_refs,
            audit_ref: format!(
                "tool://{tool_name}/{}",
                chrono::Utc::now().timestamp_millis()
            ),
            warnings: warnings_for_tool(tool_name, &data),
            next_actions: next_actions_for_tool(tool_name, &data),
            data,
        })
    }

    pub(crate) fn intent_plan(
        &self,
        prompt: &str,
        selected_tools: Vec<String>,
    ) -> serde_json::Value {
        let plan = classify_intent(prompt);
        serde_json::json!({
            "kind": "tool.intent_plan",
            "status": "ok",
            "intent": plan.intent,
            "recommended_tools": plan.recommended_tools,
            "selected_tools": selected_tools,
            "reason": plan.reason,
            "batch_ready": plan.recommended_tools.iter().any(|tool| tool == "tool_batch_readonly"),
        })
    }

    pub(crate) fn context_fanout_plan(&self, prompt: &str) -> serde_json::Value {
        let plan = plan_context_fanout(prompt);
        serde_json::json!({
            "kind": "tool.context_fanout_plan",
            "status": "ok",
            "intent": plan.intent,
            "calls": plan.calls,
            "reason": plan.reason,
            "batch_ready": plan.calls.iter().any(|call| call.name == "tool_batch_readonly"),
        })
    }

    pub(crate) fn runtime_config(
        &self,
        workspace_root: &Path,
        config_home: &Path,
    ) -> Result<RuntimeConfig, String> {
        ConfigLoader::new(workspace_root, config_home)
            .load()
            .map_err(|error| error.to_string())
    }

    pub(crate) fn runtime_config_json(
        &self,
        workspace_root: &Path,
        config_home: &Path,
    ) -> Result<serde_json::Value, String> {
        let runtime_config = self.runtime_config(workspace_root, config_home)?;
        Ok(json_value_to_serde(&runtime_config.as_json()))
    }

    pub(crate) fn redacted_runtime_config_json(
        &self,
        workspace_root: &Path,
        config_home: &Path,
    ) -> Result<serde_json::Value, String> {
        let mut value = self.runtime_config_json(workspace_root, config_home)?;
        redact_config_secrets(&mut value);
        Ok(value)
    }

    pub(crate) fn redact_config_json(&self, mut value: serde_json::Value) -> serde_json::Value {
        redact_config_secrets(&mut value);
        value
    }

    pub(crate) fn validate_tool_input_paths(
        &self,
        workspace_root: &Path,
        value: &serde_json::Value,
    ) -> Result<(), String> {
        validate_value_paths(workspace_root, value)
    }

    pub(crate) fn validate_tool_edits(
        &self,
        workspace_root: &Path,
        edits: &[serde_json::Value],
    ) -> Result<(), String> {
        for edit in edits {
            let Some(path) = edit.get("path").and_then(serde_json::Value::as_str) else {
                return Err("edit path is required".to_string());
            };
            validate_workspace_relative_path(workspace_root, path)?;
        }
        Ok(())
    }

    pub(crate) fn with_workspace_root<T>(
        &self,
        workspace_root: &Path,
        action: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let lock = TOOL_CWD_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock
            .lock()
            .map_err(|error| format!("failed to lock tool workspace root guard: {error}"))?;
        let previous = std::env::current_dir().map_err(|error| error.to_string())?;
        std::env::set_current_dir(workspace_root).map_err(|error| error.to_string())?;
        let result = action();
        let restore = std::env::set_current_dir(previous);
        if let Err(error) = restore {
            return Err(format!(
                "failed to restore process cwd after tool operation: {error}"
            ));
        }
        result
    }

    pub(crate) fn update_config_model(
        &self,
        config_home: &Path,
        model: &str,
    ) -> Result<String, String> {
        let path = config_home.join("config.yaml");
        fs::create_dir_all(config_home).map_err(|error| error.to_string())?;
        let mut value = if path.exists() {
            let raw = fs::read_to_string(&path).map_err(|error| error.to_string())?;
            serde_yaml::from_str::<serde_yaml::Value>(&raw).map_err(|error| error.to_string())?
        } else {
            serde_yaml::Value::Mapping(Default::default())
        };
        let mapping = value
            .as_mapping_mut()
            .ok_or_else(|| "config root must be a mapping before it can be updated".to_string())?;
        mapping.insert(
            serde_yaml::Value::String("model".to_string()),
            serde_yaml::Value::String(model.to_string()),
        );
        let rendered = serde_yaml::to_string(&value).map_err(|error| error.to_string())?;
        fs::write(&path, rendered).map_err(|error| error.to_string())?;
        Ok(path.display().to_string())
    }

    pub(crate) fn command_history(&self, config_home: &Path) -> Vec<serde_json::Value> {
        let path = config_home.join("command_history.jsonl");
        fs::read_to_string(path)
            .ok()
            .map(|raw| {
                raw.lines()
                    .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    pub(crate) fn append_command_history(&self, config_home: &Path, receipt: &serde_json::Value) {
        if fs::create_dir_all(config_home).is_err() {
            return;
        }
        let path = config_home.join("command_history.jsonl");
        let Ok(line) = serde_json::to_string(receipt) else {
            return;
        };
        let mut options = fs::OpenOptions::new();
        let _ = options
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut file| {
                use std::io::Write;
                writeln!(file, "{line}")
            });
    }
}

fn managed_tool_tags(tool_name: &str) -> Vec<&'static str> {
    let normalized = normalize_tool_name(tool_name);
    let mut tags = Vec::new();
    if matches!(
        normalized.as_str(),
        "mutation_preview" | "edit_many_preview" | "patch_plan" | "apply_patch_transaction"
    ) {
        tags.push("mutation");
    }
    if normalized.starts_with("checkpoint_") {
        tags.push("checkpoint");
    }
    if normalized == "tool_batch_readonly" {
        tags.push("batch");
    }
    if normalized == "tool_cache_stats" {
        tags.push("cache");
    }
    if tool_execution_profile(&normalized).prepared_readonly_supported {
        tags.push("prepared");
    }
    tags
}

fn normalize_tool_name(name: &str) -> String {
    name.trim().replace('-', "_").to_ascii_lowercase()
}

fn changed_refs_for_tool(tool_name: &str, data: &serde_json::Value) -> Vec<String> {
    match tool_name {
        "apply_patch_transaction" => data
            .get("applied")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| item.get("path").and_then(serde_json::Value::as_str))
            .map(|path| format!("file:{path}"))
            .collect(),
        "checkpoint_create" | "checkpoint_restore" => data
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(|id| vec![format!("checkpoint:{id}")])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn warnings_for_tool(tool_name: &str, data: &serde_json::Value) -> Vec<String> {
    let mut warnings = Vec::new();
    if tool_name == "checkpoint_restore" {
        warnings.push("restore replaces workspace files from a checkpoint".to_string());
    }
    if data
        .get("conflictCount")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default()
        > 0
    {
        warnings.push("mutation preview contains conflicts".to_string());
    }
    warnings
}

fn next_actions_for_tool(tool_name: &str, data: &serde_json::Value) -> Vec<String> {
    match tool_name {
        "mutation_preview" | "patch_plan" | "edit_many_preview" => {
            if data
                .get("conflictCount")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
                == 0
            {
                vec!["Apply transaction with expected hashes".to_string()]
            } else {
                vec!["Resolve conflicts before applying".to_string()]
            }
        }
        "checkpoint_create" => vec!["Use checkpoint diff before restore".to_string()],
        "checkpoint_restore" => vec!["Refresh workspace and inspect diff".to_string()],
        _ => Vec::new(),
    }
}

fn redact_config_secrets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                let normalized = key.to_ascii_lowercase();
                if normalized.contains("api_key")
                    || normalized == "token"
                    || normalized.ends_with("_token")
                    || normalized == "secret"
                    || normalized.ends_with("_secret")
                    || normalized == "password"
                {
                    *item = serde_json::Value::String("[redacted]".to_string());
                } else {
                    redact_config_secrets(item);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_config_secrets(item);
            }
        }
        _ => {}
    }
}

fn json_value_to_serde(value: &JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(value) => serde_json::Value::Bool(*value),
        JsonValue::Number(value) => serde_json::Value::Number((*value).into()),
        JsonValue::String(value) => serde_json::Value::String(value.clone()),
        JsonValue::Array(values) => {
            serde_json::Value::Array(values.iter().map(json_value_to_serde).collect())
        }
        JsonValue::Object(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), json_value_to_serde(value)))
                .collect(),
        ),
    }
}

fn validate_value_paths(workspace_root: &Path, value: &serde_json::Value) -> Result<(), String> {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                if matches!(key.as_str(), "path" | "root" | "dir") {
                    if let Some(path) = child.as_str() {
                        validate_workspace_relative_path(workspace_root, path)?;
                    }
                }
                validate_value_paths(workspace_root, child)?;
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                validate_value_paths(workspace_root, child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_workspace_relative_path(workspace_root: &Path, raw_path: &str) -> Result<(), String> {
    let path = Path::new(raw_path);
    if path.is_absolute() {
        return Err("absolute paths are not allowed for tool operations".to_string());
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::Prefix(_) | Component::RootDir
        )
    }) {
        return Err("tool path escapes workspace root".to_string());
    }
    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|error| format!("workspace root is unavailable: {error}"))?;
    let candidate = canonical_root.join(path);
    let resolved = if candidate.exists() {
        candidate
            .canonicalize()
            .map_err(|error| format!("tool path is unavailable: {error}"))?
    } else {
        let parent = candidate
            .parent()
            .ok_or_else(|| "tool path parent is unavailable".to_string())?;
        let resolved_parent = parent
            .canonicalize()
            .map_err(|error| format!("tool path parent is unavailable: {error}"))?;
        if let Some(file_name) = candidate.file_name() {
            resolved_parent.join(file_name)
        } else {
            resolved_parent
        }
    };
    if !resolved.starts_with(&canonical_root) {
        return Err("tool path escapes workspace root".to_string());
    }
    Ok(())
}
