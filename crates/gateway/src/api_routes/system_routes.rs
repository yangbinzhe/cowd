use std::{collections::BTreeMap, path::Path, sync::Arc};

use axum::{
    extract::{Path as AxumPath, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            surface::gateway_api::paths::API_TOOLS.template(),
            get(tools_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_EXECUTE.template(),
            post(tool_execute_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_CACHE.template(),
            get(tool_cache_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_BATCH_READONLY.template(),
            post(tool_batch_readonly_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_MUTATIONS_PREVIEW.template(),
            post(tool_mutation_preview_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_MUTATIONS_APPLY.template(),
            post(tool_mutation_apply_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_CHECKPOINTS.template(),
            get(tool_checkpoints_handler).post(tool_checkpoint_create_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_CHECKPOINTS_BY_ID_DIFF.template(),
            get(tool_checkpoint_diff_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_CHECKPOINTS_BY_ID_RESTORE.template(),
            post(tool_checkpoint_restore_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_INTENT_PLAN.template(),
            post(tool_intent_plan_handler),
        )
        .route(
            surface::gateway_api::paths::API_TOOLS_CONTEXT_FANOUT_PLAN.template(),
            post(tool_context_fanout_plan_handler),
        )
        .route(
            surface::gateway_api::paths::API_CONFIG.template(),
            get(config_handler).put(update_config_handler),
        )
        .route(
            surface::gateway_api::paths::API_CONFIG_PROVIDERS.template(),
            get(config_providers_handler),
        )
        .route(
            surface::gateway_api::paths::API_CONFIG_PROVIDER_CATALOG.template(),
            get(config_provider_catalog_handler),
        )
        .route(
            surface::gateway_api::paths::API_USAGE.template(),
            get(usage_handler),
        )
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
    let usage = match state.services.session.session_usage_summary(100).await {
        Ok(Some(usage)) => usage,
        Ok(None) => {
            return Json(serde_json::json!({
                "kind": "usage.summary",
                "status": "degraded",
                "reason": "durable_session_store_unavailable",
                "active_session_count": active_session_count,
                "session_count": 0,
                "message_count": 0,
                "tokens": { "input": 0, "output": 0, "total": 0 },
                "by_platform": {},
                "by_model": {},
                "sessions": [],
            }));
        }
        Err(error) => {
            return Json(serde_json::json!({
                "kind": "usage.summary",
                "status": "error",
                "error": error.to_string(),
                "active_session_count": active_session_count,
                "session_count": 0,
                "message_count": 0,
                "tokens": { "input": 0, "output": 0, "total": 0 },
                "by_platform": {},
                "by_model": {},
                "sessions": [],
            }));
        }
    };
    let session_rows = usage
        .recent_sessions
        .into_iter()
        .map(|session| {
            serde_json::json!({
                "session_id": session.session_id,
                "platform": session.platform,
                "model": session.model,
                "message_count": session.message_count,
                "input_tokens": session.input_tokens,
                "output_tokens": session.output_tokens,
                "total_tokens": session.input_tokens + session.output_tokens,
                "status": session.status,
                "last_activity": session.last_activity,
            })
        })
        .collect::<Vec<_>>();
    let sessions_truncated = usage.session_count > session_rows.len();
    Json(serde_json::json!({
        "kind": "usage.summary",
        "status": "ready",
        "active_session_count": active_session_count,
        "session_count": usage.session_count,
        "message_count": usage.message_count,
        "tokens": {
            "input": usage.input_tokens,
            "output": usage.output_tokens,
            "total": usage.input_tokens + usage.output_tokens,
        },
        "by_platform": usage.by_platform,
        "by_model": usage.by_model,
        "sessions": session_rows,
        "sessions_truncated": sessions_truncated,
    }))
}

async fn tools_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let tool_host = state
        .services
        .runtime
        .as_ref()
        .map(|runtime| runtime.tool_host());
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
    let tool_host = tool_host.unwrap_or_else(|| {
        Arc::new(tools::ToolHost::builtin(
            "gateway-catalog",
            &state.workspace_root,
        ))
    });
    Json(state.services.system.tool_catalog(&tool_host, definitions))
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
    let tool_host = runtime_tool_host(&state)?;
    for call in &body.calls {
        let name = call
            .get("name")
            .and_then(serde_json::Value::as_str)
            .map(normalize_tool_name)
            .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "batch call name is required"))?;
        if !state
            .services
            .system
            .is_prepared_readonly_tool(&tool_host, &name)
        {
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
    let active_snapshot = state
        .services
        .runtime
        .as_ref()
        .map(|service| service.provider_registry().pin());
    Ok(Json(state.services.provider.config_projection(
        &runtime_config,
        active_snapshot.as_ref(),
    )))
}

async fn config_provider_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_config = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let active_snapshot = state
        .services
        .runtime
        .as_ref()
        .map(|service| service.provider_registry().pin());
    let projection = state
        .services
        .provider
        .config_projection(&runtime_config, active_snapshot.as_ref());
    Ok(Json(serde_json::json!({
        "envelope": state.services.provider.envelope("provider_catalog"),
        "catalog": projection.get("catalog").cloned().unwrap_or_else(|| serde_json::json!({})),
        "catalog_generation": projection.get("catalog_generation").cloned().unwrap_or(serde_json::Value::Null),
        "configured_catalog_generation": projection.get("configured_catalog_generation").cloned().unwrap_or(serde_json::Value::Null),
        "active_provider_revision": projection.get("active_provider_revision").cloned().unwrap_or(serde_json::Value::Null),
        "active_matches_configured": projection.get("active_matches_configured").cloned().unwrap_or(serde_json::Value::Bool(false)),
        "warnings": projection.get("warnings").cloned().unwrap_or_else(|| serde_json::json!([])),
    })))
}
