use std::{collections::BTreeMap, path::Path, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;

use super::{AppState, ErrorResponse, api_error};

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
        .route(
            "/api/config/provider-catalog",
            get(config_provider_catalog_handler),
        )
        .route("/api/usage", get(usage_handler))
}

#[derive(Deserialize)]
struct UpdateConfigRequest {
    #[serde(default)]
    model: Option<String>,
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

async fn usage_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let active_session_count = state.list_active_session_ids().len();
    let Some(store) = state.services.session.unified_store() else {
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
    let definitions = state
        .services
        .runtime
        .as_ref()
        .map(|runtime| {
            runtime
                .tool_host()
                .pin_snapshot()
                .snapshot()
                .catalog
                .definitions(None)
        })
        .unwrap_or_else(|| state.tool_registry.definitions(None));
    Json(state.services.system.tool_catalog(definitions))
}

fn runtime_tool_host(
    state: &AppState,
) -> Result<Arc<tools::ToolHost>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.tool_host())
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "runtime tool host unavailable",
            )
        })
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
    let input = ensure_tool_input_within_workspace(&state, &state.workspace_root, body.input)?;
    let tool_host = runtime_tool_host(&state)?;
    let receipt = state
        .services
        .system
        .execute_tool_receipt(
            &tool_host,
            &state.workspace_root,
            &tool_name,
            input,
            body.mode.unwrap_or_else(|| "read_only".to_string()),
            "low",
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(receipt))
}

async fn tool_cache_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let tool_host = runtime_tool_host(&state)?;
    Ok(Json(state.services.system.tool_cache_receipt(&tool_host)))
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
        if !state.services.system.is_prepared_readonly_tool(&name) {
            return Err(api_error(
                StatusCode::FORBIDDEN,
                format!("tool_batch_readonly rejects non read-only tool `{name}`"),
            ));
        }
        let input = call
            .get("input")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        ensure_tool_input_within_workspace(&state, &state.workspace_root, input)?;
    }
    let tool_host = runtime_tool_host(&state)?;
    let receipt = state
        .services
        .system
        .execute_tool_receipt(
            &tool_host,
            &state.workspace_root,
            "tool_batch_readonly",
            serde_json::json!({
                "calls": body.calls,
                "max_concurrency": body.max_concurrency,
            }),
            "read_only_batch",
            "low",
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(receipt))
}

async fn tool_mutation_preview_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolMutationRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let edits = ensure_edits_within_workspace(&state, &state.workspace_root, body.edits)?;
    let tool_host = runtime_tool_host(&state)?;
    let receipt = state
        .services
        .system
        .execute_tool_receipt(
            &tool_host,
            &state.workspace_root,
            "mutation_preview",
            serde_json::json!({ "edits": edits }),
            "preview",
            "medium",
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(receipt))
}

async fn tool_mutation_apply_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolMutationRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let edits = ensure_edits_within_workspace(&state, &state.workspace_root, body.edits)?;
    let tool_host = runtime_tool_host(&state)?;
    let receipt = state
        .services
        .system
        .execute_tool_receipt(
            &tool_host,
            &state.workspace_root,
            "apply_patch_transaction",
            serde_json::json!({
                "edits": edits,
                "expected_hashes": body.expected_hashes,
            }),
            "transaction",
            "high",
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(receipt))
}

async fn tool_checkpoints_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let tool_host = runtime_tool_host(&state)?;
    let receipt = state
        .services
        .system
        .execute_tool_receipt(
            &tool_host,
            &state.workspace_root,
            "checkpoint_list",
            serde_json::json!({}),
            "read_only",
            "low",
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(receipt))
}

async fn tool_checkpoint_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolCheckpointCreateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let tool_host = runtime_tool_host(&state)?;
    let receipt = state
        .services
        .system
        .execute_tool_receipt(
            &tool_host,
            &state.workspace_root,
            "checkpoint_create",
            serde_json::json!({ "label": body.label }),
            "checkpoint",
            "medium",
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(receipt))
}

async fn tool_checkpoint_diff_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let tool_host = runtime_tool_host(&state)?;
    let receipt = state
        .services
        .system
        .execute_tool_receipt(
            &tool_host,
            &state.workspace_root,
            "checkpoint_diff",
            serde_json::json!({ "id": id }),
            "read_only",
            "low",
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(receipt))
}

async fn tool_checkpoint_restore_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let tool_host = runtime_tool_host(&state)?;
    let receipt = state
        .services
        .system
        .execute_tool_receipt(
            &tool_host,
            &state.workspace_root,
            "checkpoint_restore",
            serde_json::json!({ "id": id }),
            "restore",
            "high",
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(Json(receipt))
}

async fn tool_intent_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolIntentPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(
        state
            .services
            .system
            .intent_plan(&body.prompt, body.selected_tools),
    ))
}

async fn tool_context_fanout_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ToolContextFanoutPlanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(
        state.services.system.context_fanout_plan(&body.prompt),
    ))
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

fn ensure_tool_input_within_workspace(
    state: &AppState,
    workspace_root: &Path,
    input: serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .system
        .validate_tool_input_paths(workspace_root, &input)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(input)
}

fn ensure_edits_within_workspace(
    state: &AppState,
    workspace_root: &Path,
    edits: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .system
        .validate_tool_edits(workspace_root, &edits)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;
    Ok(edits)
}

async fn config_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let config = state
        .services
        .system
        .redacted_runtime_config_json(&state.workspace_root, &state.config_home)
        .unwrap_or_else(|error| {
            serde_json::json!({
                "error": error,
                "model": "unknown",
                "version": env!("CARGO_PKG_VERSION"),
            })
        });
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
    let providers = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?
        .providers()
        .clone();
    if !providers.is_empty() && providers.resolve_full(model).is_none() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("model `{model}` is not declared by any configured provider"),
        ));
    }

    let config_path = state
        .services
        .system
        .update_config_model(&state.config_home, model)
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "changed": true,
        "config_path": config_path,
        "config": state.services.system.redact_config_json(
            state
                .services
                .system
                .runtime_config_json(&state.workspace_root, &state.config_home)
                .unwrap_or_else(|_| serde_json::json!({"model": model}))
        ),
    })))
}

async fn config_providers_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_config = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(
        state.services.provider.config_projection(&runtime_config),
    ))
}

async fn config_provider_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_config = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let projection = state.services.provider.config_projection(&runtime_config);
    Ok(Json(serde_json::json!({
        "envelope": state.services.provider.envelope("provider_catalog"),
        "catalog": projection.get("catalog").cloned().unwrap_or_else(|| serde_json::json!({})),
        "catalog_generation": projection.get("catalog_generation").cloned().unwrap_or(serde_json::Value::Null),
        "warnings": projection.get("warnings").cloned().unwrap_or_else(|| serde_json::json!([])),
    })))
}
