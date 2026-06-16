use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};

use axum::{
    extract::{Path as AxumPath, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::{
    classify_intent, plan_context_fanout, tool_execution_profile, ConfigLoader, JsonValue,
    ToolSafetyCategory,
};
use serde::{Deserialize, Serialize};

use super::{api_error, AppState, ErrorResponse};

static TOOL_CWD_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/tools", get(tools_handler))
        .route("/api/tools/execute", post(tool_execute_handler))
        .route("/api/tools/cache", get(tool_cache_handler))
        .route(
            "/api/tools/batch-readonly",
            post(tool_batch_readonly_handler),
        )
        .route(
            "/api/tools/mutations/preview",
            post(tool_mutation_preview_handler),
        )
        .route(
            "/api/tools/mutations/apply",
            post(tool_mutation_apply_handler),
        )
        .route(
            "/api/tools/checkpoints",
            get(tool_checkpoints_handler).post(tool_checkpoint_create_handler),
        )
        .route(
            "/api/tools/checkpoints/:id/diff",
            get(tool_checkpoint_diff_handler),
        )
        .route(
            "/api/tools/checkpoints/:id/restore",
            post(tool_checkpoint_restore_handler),
        )
        .route("/api/tools/intent-plan", post(tool_intent_plan_handler))
        .route(
            "/api/tools/context-fanout/plan",
            post(tool_context_fanout_plan_handler),
        )
        .route(
            "/api/config",
            get(config_handler).put(update_config_handler),
        )
        .route("/api/config/providers", get(config_providers_handler))
        .route("/api/usage", get(usage_handler))
        .route("/api/commands", get(commands_handler))
        .route("/api/commands/history", get(commands_history_handler))
        .route("/api/commands/execute", post(commands_execute_handler))
}

#[derive(Deserialize)]
struct UpdateConfigRequest {
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
struct ExecuteCommandRequest {
    command: String,
    #[serde(default)]
    args: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ToolExecuteRequest {
    name: String,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    mode: Option<String>,
}

#[derive(Deserialize)]
struct ToolBatchReadonlyRequest {
    calls: Vec<serde_json::Value>,
    #[serde(default)]
    max_concurrency: Option<usize>,
}

#[derive(Deserialize)]
struct ToolMutationRequest {
    edits: Vec<serde_json::Value>,
    #[serde(default)]
    expected_hashes: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct ToolCheckpointCreateRequest {
    #[serde(default)]
    label: Option<String>,
}

#[derive(Deserialize)]
struct ToolIntentPlanRequest {
    prompt: String,
    #[serde(default)]
    selected_tools: Vec<String>,
}

#[derive(Deserialize)]
struct ToolContextFanoutPlanRequest {
    prompt: String,
}

#[derive(Debug, Clone, Serialize)]
struct ToolOperationReceipt {
    request_id: String,
    tool_name: String,
    mode: String,
    risk: String,
    status: String,
    changed_refs: Vec<String>,
    audit_ref: String,
    data: serde_json::Value,
    warnings: Vec<String>,
    next_actions: Vec<String>,
}

async fn usage_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let active_session_count = state.list_active_session_ids().len();
    let Some(store) = state.unified_store() else {
        return Json(serde_json::json!({
            "kind": "usage.summary",
            "status": "degraded",
            "reason": "unified_session_store_unavailable",
            "active_session_count": active_session_count,
            "session_count": 0,
            "message_count": 0,
            "tokens": {
                "input": 0,
                "output": 0,
                "total": 0,
            },
            "estimated_cost_usd": 0.0,
            "by_platform": {},
            "by_model": {},
            "sessions": [],
        }));
    };

    match store.list_sessions().await {
        Ok(sessions) => {
            let mut input_tokens = 0_i64;
            let mut output_tokens = 0_i64;
            let mut message_count = 0_i64;
            let mut estimated_cost_usd = 0.0_f64;
            let mut by_platform: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            let mut by_model: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            let mut session_rows = Vec::new();

            for session in sessions {
                input_tokens += session.input_tokens;
                output_tokens += session.output_tokens;
                message_count += session.message_count;
                estimated_cost_usd += session.estimated_cost_usd;

                accumulate_usage_bucket(
                    &mut by_platform,
                    if session.platform.trim().is_empty() {
                        "unknown"
                    } else {
                        session.platform.as_str()
                    },
                    session.message_count,
                    session.input_tokens,
                    session.output_tokens,
                    session.estimated_cost_usd,
                );
                accumulate_usage_bucket(
                    &mut by_model,
                    session.model.as_deref().unwrap_or("unknown"),
                    session.message_count,
                    session.input_tokens,
                    session.output_tokens,
                    session.estimated_cost_usd,
                );

                session_rows.push(serde_json::json!({
                    "session_id": session.session_id,
                    "platform": session.platform,
                    "model": session.model,
                    "message_count": session.message_count,
                    "input_tokens": session.input_tokens,
                    "output_tokens": session.output_tokens,
                    "total_tokens": session.input_tokens + session.output_tokens,
                    "estimated_cost_usd": session.estimated_cost_usd,
                    "status": session.status,
                    "last_activity": session.last_activity,
                }));
            }

            session_rows.sort_by(|left, right| {
                right["last_activity"]
                    .as_str()
                    .cmp(&left["last_activity"].as_str())
            });

            Json(serde_json::json!({
                "kind": "usage.summary",
                "status": "ready",
                "active_session_count": active_session_count,
                "session_count": session_rows.len(),
                "message_count": message_count,
                "tokens": {
                    "input": input_tokens,
                    "output": output_tokens,
                    "total": input_tokens + output_tokens,
                },
                "estimated_cost_usd": estimated_cost_usd,
                "by_platform": by_platform,
                "by_model": by_model,
                "sessions": session_rows,
            }))
        }
        Err(error) => Json(serde_json::json!({
            "kind": "usage.summary",
            "status": "error",
            "error": error.to_string(),
            "active_session_count": active_session_count,
            "session_count": 0,
            "message_count": 0,
            "tokens": {
                "input": 0,
                "output": 0,
                "total": 0,
            },
            "estimated_cost_usd": 0.0,
            "by_platform": {},
            "by_model": {},
            "sessions": [],
        })),
    }
}

fn accumulate_usage_bucket(
    buckets: &mut BTreeMap<String, serde_json::Value>,
    key: &str,
    message_count: i64,
    input_tokens: i64,
    output_tokens: i64,
    estimated_cost_usd: f64,
) {
    let current = buckets.entry(key.to_string()).or_insert_with(|| {
        serde_json::json!({
            "session_count": 0,
            "message_count": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "estimated_cost_usd": 0.0,
        })
    });
    current["session_count"] =
        serde_json::json!(current["session_count"].as_i64().unwrap_or(0) + 1);
    current["message_count"] =
        serde_json::json!(current["message_count"].as_i64().unwrap_or(0) + message_count);
    current["input_tokens"] =
        serde_json::json!(current["input_tokens"].as_i64().unwrap_or(0) + input_tokens);
    current["output_tokens"] =
        serde_json::json!(current["output_tokens"].as_i64().unwrap_or(0) + output_tokens);
    current["total_tokens"] = serde_json::json!(
        current["total_tokens"].as_i64().unwrap_or(0) + input_tokens + output_tokens
    );
    current["estimated_cost_usd"] = serde_json::json!(
        current["estimated_cost_usd"].as_f64().unwrap_or(0.0) + estimated_cost_usd
    );
}

async fn tools_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let tools: Vec<serde_json::Value> = state
        .tool_registry
        .definitions(None)
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
    Json(serde_json::json!({ "tools": tools, "count": tools.len() }))
}

async fn tool_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolExecuteRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let tool_name = normalize_tool_name(&body.name);
    if !is_webui_generic_tool_allowed(&tool_name) {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            format!(
                "tool `{}` is not allowed through /api/tools/execute",
                body.name
            ),
        ));
    }
    let input = ensure_tool_input_within_workspace(&state.workspace_root, body.input)?;
    let receipt = execute_tool_receipt(
        &state,
        &tool_name,
        input,
        body.mode.unwrap_or_else(|| "read_only".to_string()),
        "low",
    )?;
    Ok(Json(receipt))
}

async fn tool_cache_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let receipt = execute_tool_receipt(
        &state,
        "tool_cache_stats",
        serde_json::json!({}),
        "stats",
        "low",
    )?;
    Ok(Json(receipt))
}

async fn tool_batch_readonly_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolBatchReadonlyRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    for call in &body.calls {
        let name = call
            .get("name")
            .and_then(serde_json::Value::as_str)
            .map(normalize_tool_name)
            .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "batch call name is required"))?;
        if !is_prepared_readonly_tool(&name) {
            return Err(api_error(
                StatusCode::FORBIDDEN,
                format!("tool_batch_readonly rejects non read-only tool `{name}`"),
            ));
        }
        let input = call
            .get("input")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        ensure_tool_input_within_workspace(&state.workspace_root, input)?;
    }
    let receipt = execute_tool_receipt(
        &state,
        "tool_batch_readonly",
        serde_json::json!({
            "calls": body.calls,
            "max_concurrency": body.max_concurrency,
        }),
        "read_only_batch",
        "low",
    )?;
    Ok(Json(receipt))
}

async fn tool_mutation_preview_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolMutationRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let edits = ensure_edits_within_workspace(&state.workspace_root, body.edits)?;
    let receipt = execute_tool_receipt(
        &state,
        "mutation_preview",
        serde_json::json!({ "edits": edits }),
        "preview",
        "medium",
    )?;
    Ok(Json(receipt))
}

async fn tool_mutation_apply_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolMutationRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let edits = ensure_edits_within_workspace(&state.workspace_root, body.edits)?;
    let receipt = execute_tool_receipt(
        &state,
        "apply_patch_transaction",
        serde_json::json!({
            "edits": edits,
            "expected_hashes": body.expected_hashes,
        }),
        "transaction",
        "high",
    )?;
    Ok(Json(receipt))
}

async fn tool_checkpoints_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let receipt = execute_tool_receipt(
        &state,
        "checkpoint_list",
        serde_json::json!({}),
        "read_only",
        "low",
    )?;
    Ok(Json(receipt))
}

async fn tool_checkpoint_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolCheckpointCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let receipt = execute_tool_receipt(
        &state,
        "checkpoint_create",
        serde_json::json!({ "label": body.label }),
        "checkpoint",
        "medium",
    )?;
    Ok(Json(receipt))
}

async fn tool_checkpoint_diff_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let receipt = execute_tool_receipt(
        &state,
        "checkpoint_diff",
        serde_json::json!({ "id": id }),
        "read_only",
        "low",
    )?;
    Ok(Json(receipt))
}

async fn tool_checkpoint_restore_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let receipt = execute_tool_receipt(
        &state,
        "checkpoint_restore",
        serde_json::json!({ "id": id }),
        "restore",
        "high",
    )?;
    Ok(Json(receipt))
}

async fn tool_intent_plan_handler(
    Json(body): Json<ToolIntentPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let plan = classify_intent(&body.prompt);
    Ok(Json(serde_json::json!({
        "kind": "tool.intent_plan",
        "status": "ok",
        "intent": plan.intent,
        "recommended_tools": plan.recommended_tools,
        "selected_tools": body.selected_tools,
        "reason": plan.reason,
        "batch_ready": plan.recommended_tools.iter().any(|tool| tool == "tool_batch_readonly"),
    })))
}

async fn tool_context_fanout_plan_handler(
    Json(body): Json<ToolContextFanoutPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let plan = plan_context_fanout(&body.prompt);
    Ok(Json(serde_json::json!({
        "kind": "tool.context_fanout_plan",
        "status": "ok",
        "intent": plan.intent,
        "calls": plan.calls,
        "reason": plan.reason,
        "batch_ready": plan.calls.iter().any(|call| call.name == "tool_batch_readonly"),
    })))
}

fn normalize_tool_name(name: &str) -> String {
    name.trim().replace('-', "_").to_ascii_lowercase()
}

fn is_webui_generic_tool_allowed(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "read_file"
            | "glob_search"
            | "grep_search"
            | "workspace_snapshot"
            | "mutation_preview"
            | "edit_many_preview"
            | "patch_plan"
            | "checkpoint_list"
            | "checkpoint_diff"
            | "tool_batch_readonly"
            | "tool_cache_stats"
    )
}

fn is_prepared_readonly_tool(tool_name: &str) -> bool {
    let profile = tool_execution_profile(tool_name);
    profile.safety_category == ToolSafetyCategory::ReadOnly && profile.prepared_readonly_supported
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

fn ensure_tool_input_within_workspace(
    workspace_root: &Path,
    input: serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, Json<ErrorResponse>)> {
    validate_value_paths(workspace_root, &input)?;
    Ok(input)
}

fn ensure_edits_within_workspace(
    workspace_root: &Path,
    edits: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    for edit in &edits {
        let Some(path) = edit.get("path").and_then(serde_json::Value::as_str) else {
            return Err(api_error(StatusCode::BAD_REQUEST, "edit path is required"));
        };
        validate_workspace_relative_path(workspace_root, path)?;
    }
    Ok(edits)
}

fn validate_value_paths(
    workspace_root: &Path,
    value: &serde_json::Value,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
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

fn validate_workspace_relative_path(
    workspace_root: &Path,
    raw_path: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let path = Path::new(raw_path);
    if path.is_absolute() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "absolute paths are not allowed for tool operations",
        ));
    }
    let candidate = workspace_root.join(path);
    if !candidate.starts_with(workspace_root) || raw_path.split('/').any(|part| part == "..") {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "tool path escapes workspace root",
        ));
    }
    Ok(())
}

fn execute_tool_receipt(
    state: &AppState,
    tool_name: &str,
    input: serde_json::Value,
    mode: impl Into<String>,
    risk: impl Into<String>,
) -> Result<ToolOperationReceipt, (StatusCode, Json<ErrorResponse>)> {
    let mode = mode.into();
    let risk = risk.into();
    let output = with_workspace_root(&state.workspace_root, || {
        state
            .tool_registry
            .execute(tool_name, &input)
            .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))
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

fn with_workspace_root<T>(
    workspace_root: &Path,
    action: impl FnOnce() -> Result<T, (StatusCode, Json<ErrorResponse>)>,
) -> Result<T, (StatusCode, Json<ErrorResponse>)> {
    let lock = TOOL_CWD_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().map_err(|error| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to lock tool workspace root guard: {error}"),
        )
    })?;
    let previous = std::env::current_dir()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    std::env::set_current_dir(workspace_root)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let result = action();
    let restore = std::env::set_current_dir(previous);
    if let Err(error) = restore {
        return Err(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to restore process cwd after tool operation: {error}"),
        ));
    }
    result
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

async fn config_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let mut config = load_runtime_config_json(&state).unwrap_or_else(|error| {
        serde_json::json!({
            "error": error,
            "model": "unknown",
            "version": env!("CARGO_PKG_VERSION"),
        })
    });
    redact_config_secrets(&mut config);
    Json(config)
}

async fn update_config_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<UpdateConfigRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(model) = body
        .model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "model is required for config update",
        ));
    };
    let providers = load_runtime_config(&state)?.providers().clone();
    if !providers.is_empty() && providers.resolve_full(model).is_none() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("model `{model}` is not declared by any configured provider"),
        ));
    }

    let path = state.config_home.join("config.yaml");
    fs::create_dir_all(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let mut value = if path.exists() {
        let raw = fs::read_to_string(&path)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        serde_yaml::from_str::<serde_yaml::Value>(&raw)
            .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?
    } else {
        serde_yaml::Value::Mapping(Default::default())
    };
    let mapping = value.as_mapping_mut().ok_or_else(|| {
        api_error(
            StatusCode::BAD_REQUEST,
            "config root must be a mapping before it can be updated",
        )
    })?;
    mapping.insert(
        serde_yaml::Value::String("model".to_string()),
        serde_yaml::Value::String(model.to_string()),
    );
    let rendered = serde_yaml::to_string(&value)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    fs::write(&path, rendered)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "changed": true,
        "config_path": path.display().to_string(),
        "config": redacted_config_json(load_runtime_config_json(&state).unwrap_or_else(|_| serde_json::json!({"model": model}))),
    })))
}

async fn config_providers_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_config = load_runtime_config(&state)?;
    let providers = runtime_config.providers();
    let configured_model = runtime_config.model().map(str::to_string);
    let configured_model_provider = configured_model
        .as_deref()
        .and_then(|model| providers.resolve_full(model))
        .map(|provider| provider.name.clone());
    let mut provider_rows = providers
        .providers
        .values()
        .map(|provider| {
            serde_json::json!({
                "name": provider.name,
                "base_url": provider.base_url,
                "protocol": provider.protocol,
                "models": provider.models,
                "model_count": provider.models.len(),
                "credential_present": !provider.api_key.trim().is_empty(),
            })
        })
        .collect::<Vec<_>>();
    provider_rows.sort_by(|left, right| {
        left["name"]
            .as_str()
            .unwrap_or("")
            .cmp(right["name"].as_str().unwrap_or(""))
    });
    let selected_model = configured_model.clone();
    let models = provider_rows
        .iter()
        .flat_map(|provider| {
            let provider_name = provider["name"].as_str().unwrap_or("").to_string();
            let selected_model = selected_model.clone();
            provider["models"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(move |model| {
                    model.as_str().map(|id| {
                        serde_json::json!({
                            "id": id,
                            "name": id,
                            "provider": provider_name,
                            "selected": selected_model.as_deref() == Some(id),
                        })
                    })
                })
        })
        .collect::<Vec<_>>();

    Ok(Json(serde_json::json!({
        "providers": provider_rows,
        "models": models,
        "provider_count": providers.providers.len(),
        "provider_model_count": models.len(),
        "configured_model": configured_model,
        "configured_model_provider": configured_model_provider,
        "configured_model_resolved": configured_model.is_none() || configured_model_provider.is_some(),
    })))
}

async fn commands_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "commands": command_registry(),
    }))
}

async fn commands_history_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let path = state.config_home.join("command_history.jsonl");
    let entries = fs::read_to_string(path)
        .ok()
        .map(|raw| {
            raw.lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Json(serde_json::json!({ "history": entries, "total": entries.len() }))
}

async fn commands_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ExecuteCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let command = normalize_command(&body.command);
    let Some(definition) = command_registry()
        .into_iter()
        .find(|item| item["name"].as_str() == Some(command.as_str()))
    else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("unknown command `{}`", body.command),
        ));
    };
    let receipt = serde_json::json!({
        "ok": true,
        "command": command,
        "args": body.args.unwrap_or_else(|| serde_json::json!({})),
        "action": definition["action"].clone(),
        "target": definition["target"].clone(),
        "executed_at_ms": chrono::Utc::now().timestamp_millis(),
    });
    append_command_history(&state, &receipt);
    Ok(Json(receipt))
}

fn command_registry() -> Vec<serde_json::Value> {
    vec![
        command(
            "/status",
            "Show runtime, session, memory, and gateway status.",
            "open",
            "/runtime",
        ),
        command(
            "/model",
            "Open model selector or switch the current session model.",
            "open",
            "model-modal",
        ),
        command(
            "/workspace",
            "Open workspace browser and file controls.",
            "open",
            "workspace-panel",
        ),
        command(
            "/memory",
            "Open memory search, facts, and packet tools.",
            "open",
            "/memory",
        ),
        command(
            "/context",
            "Open context packet and evidence tools.",
            "open",
            "/context",
        ),
        command(
            "/skills",
            "Open skills catalog and run console.",
            "open",
            "/skills",
        ),
        command(
            "/agents",
            "Open tasks and agent work graph.",
            "open",
            "/agents",
        ),
        command(
            "/gateway",
            "Open channel, connector, and cross-plane controls.",
            "open",
            "/gateway",
        ),
        command(
            "/settings",
            "Open settings and provider/profile controls.",
            "open",
            "/settings",
        ),
        command(
            "/clear",
            "Clear the local composer input.",
            "client",
            "composer",
        ),
        command(
            "/compact",
            "Compact the current session.",
            "api",
            "/api/sessions/:id/compact",
        ),
    ]
}

fn command(name: &str, description: &str, action: &str, target: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "description": description,
        "action": action,
        "target": target,
        "surface": ["webui", "tui"],
    })
}

fn normalize_command(command: &str) -> String {
    let trimmed = command.trim();
    let first = trimmed.split_whitespace().next().unwrap_or(trimmed);
    if first.starts_with('/') {
        first.to_string()
    } else {
        format!("/{first}")
    }
}

fn append_command_history(state: &AppState, receipt: &serde_json::Value) {
    if fs::create_dir_all(&state.config_home).is_err() {
        return;
    }
    let path = state.config_home.join("command_history.jsonl");
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

fn load_runtime_config(
    state: &AppState,
) -> Result<runtime::RuntimeConfig, (StatusCode, Json<ErrorResponse>)> {
    ConfigLoader::new(&state.workspace_root, &state.config_home)
        .load()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

fn load_runtime_config_json(state: &AppState) -> Result<serde_json::Value, String> {
    let runtime_config = ConfigLoader::new(&state.workspace_root, &state.config_home)
        .load()
        .map_err(|error| error.to_string())?;
    Ok(json_value_to_serde(&runtime_config.as_json()))
}

fn redacted_config_json(mut value: serde_json::Value) -> serde_json::Value {
    redact_config_secrets(&mut value);
    value
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
