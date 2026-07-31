use std::sync::Arc;

use axum::{
    extract::{Extension, Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use provider::{InputMessage, MessageRequest, OutputContentBlock, ProviderClient};
use serde::Deserialize;

use crate::services::{
    SkillActionRequest, SkillCatalogQuery, SkillFileQuery, SkillMaintenanceEvaluateRequest,
    SkillProjectionQuery, SkillServiceError,
};
use skill::SkillActionKind;

use super::AuthenticatedPrincipal;
use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/skills", post(skill_create_handler))
        .route("/api/skills/catalog", get(skills_catalog_handler))
        .route("/api/skills/projection", get(skills_projection_handler))
        .route("/api/skills/runs", get(skill_runs_handler))
        .route("/api/skills/runs/:id", get(skill_run_detail_handler))
        .route(
            "/api/skills/maintenance/evaluate",
            post(skill_maintenance_evaluate_handler),
        )
        .route(
            "/api/skills/:id/actions/validate",
            post(skill_action_validate_handler),
        )
        .route(
            "/api/skills/:id/actions/plan",
            post(skill_action_plan_handler),
        )
        .route(
            "/api/skills/:id/actions/run",
            post(skill_action_run_handler),
        )
        .route("/api/skills/:id/translate", post(skill_translate_handler))
        .route("/api/skills/:id/files", get(skill_files_handler))
        .route("/api/skills/:id/files/raw", get(skill_file_raw_handler))
        .route(
            "/api/skills/:id",
            get(skill_get_handler).delete(skill_delete_handler),
        )
}

fn require_skill_manager(
    principal: &AuthenticatedPrincipal,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if principal.0.is_human_interactive() && principal.0.has_capability("definition.manage") {
        Ok(())
    } else {
        Err(api_error(
            StatusCode::FORBIDDEN,
            "skill_human_definition_manage_capability_required",
        ))
    }
}

async fn skill_create_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(input): Json<skill::SkillCreateInput>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_manager(&principal)?;
    state
        .services
        .skill
        .create_managed(input)
        .map(|value| (StatusCode::CREATED, Json(value)))
        .map_err(skill_error)
}

async fn skill_delete_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_skill_manager(&principal)?;
    state
        .services
        .skill
        .delete_managed(
            &state.workspace_root,
            state.services.app_registry.as_ref(),
            &id,
        )
        .map(Json)
        .map_err(skill_error)
}

async fn skills_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<SkillCatalogQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .catalog(
            &state.workspace_root,
            state.services.app_registry.as_ref(),
            query,
        )
        .map(Json)
        .map_err(skill_error)
}

async fn skills_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<SkillProjectionQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .projection(
            &state.workspace_root,
            state.services.app_registry.as_ref(),
            query,
        )
        .map(Json)
        .map_err(skill_error)
}

async fn skill_runs_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .runs(&state.config_home)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_run_detail_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .run_detail(&state.config_home, &id)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_maintenance_evaluate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<SkillMaintenanceEvaluateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .maintenance_evaluate(request)
        .map(Json)
        .map_err(skill_error)
}

async fn skill_action_validate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SkillActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    skill_action_handler(state, id, SkillActionKind::Validate, request)
}

async fn skill_action_plan_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SkillActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    skill_action_handler(state, id, SkillActionKind::Plan, request)
}

async fn skill_action_run_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SkillActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    skill_action_handler(state, id, SkillActionKind::Run, request)
}

fn skill_action_handler(
    state: Arc<AppState>,
    id: String,
    action: SkillActionKind,
    request: SkillActionRequest,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .run_action(
            &state.workspace_root,
            state.services.app_registry.as_ref(),
            &state.config_home,
            &id,
            action,
            request,
        )
        .map(Json)
        .map_err(skill_error)
}

#[derive(Deserialize)]
struct SkillTranslateRequest {
    content: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    locale: Option<String>,
}

async fn skill_translate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<SkillTranslateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let content = request.content.trim();
    if content.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "content is required".to_string(),
        ));
    }
    state
        .services
        .skill
        .detail(
            &state.workspace_root,
            state.services.app_registry.as_ref(),
            &id,
        )
        .map_err(skill_error)?;

    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime services are unavailable".to_string(),
        )
    })?;
    let runtime_config = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map_err(|error| api_error(StatusCode::SERVICE_UNAVAILABLE, error))?;
    let model = runtime_config
        .resolved_gateway_translation_model()
        .ok_or_else(|| {
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "no translation or default model is configured for skill translation".to_string(),
            )
        })?;
    let cache_entries = runtime_config.gateway().translation.cache_entries;
    let runtime_services = runtime_service.runtime_services();
    let provider_registry = runtime_service.provider_registry();
    let provider_snapshot = provider_registry.pin();
    let provider = provider_snapshot.resolve(&model).ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("configured model '{model}' is not declared by any active runtime provider"),
        )
    })?;
    let (_, http) = runtime_services
        .provider_transport_pool()
        .checkout_default()
        .map_err(|error| api_error(StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
    let client = ProviderClient::from_config_with_http(provider, http).map_err(|error| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("failed to create configured provider client: {error}"),
        )
    })?;

    let char_limit = 24_000usize;
    let truncated = content.chars().count() > char_limit;
    let source = content.chars().take(char_limit).collect::<String>();
    let locale = request
        .locale
        .as_deref()
        .map(str::trim)
        .filter(|locale| !locale.is_empty())
        .unwrap_or("zh-CN")
        .to_string();
    let path = request.path.as_deref().unwrap_or("SKILL.md");
    let cache_material = format!("skill-translation-v2\0{id}\0{path}\0{locale}\0{model}\0{source}");
    let source_digest = format!(
        "{:016x}",
        model_protocol::fingerprint::stable_hash_bytes(source.as_bytes())
    );
    let cache_key = format!(
        "{:016x}",
        model_protocol::fingerprint::stable_hash_bytes(cache_material.as_bytes())
    );
    if cache_entries > 0 {
        if let Some(mut cached) = state.services.skill.cached_translation(&cache_key) {
            if let Some(object) = cached.as_object_mut() {
                object.insert("cached".to_string(), serde_json::Value::Bool(true));
            }
            return Ok(Json(cached));
        }
    }
    let prompt = format!(
        "请把下面的 Skill Markdown 翻译为 {locale}。\n\
         要求：保留 Markdown 结构、代码块、YAML front matter、命令和路径；只翻译自然语言说明；不要添加额外解释。\n\n\
         ## Source metadata\n\
         - Skill: `{id}`\n\
         - Path: `{path}`\n\n\
         ## Markdown to translate\n\n\
         {source}"
    );
    let response = client
        .send_message(&MessageRequest {
            model: model.clone(),
            max_tokens: 4096,
            messages: vec![InputMessage::user_text(prompt)],
            system: Some("你是 Skill 文档翻译器，输出必须是可直接预览的 Markdown。".to_string()),
            temperature: Some(0.2),
            ..Default::default()
        })
        .await
        .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error.to_string()))?;
    let translated_markdown = response
        .content
        .iter()
        .filter_map(|block| match block {
            OutputContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
        .trim()
        .to_string();

    let result = serde_json::json!({
        "ok": true,
        "kind": "skills.translation",
        "skill_id": id,
        "path": request.path,
        "locale": locale,
        "model": response.model,
        "translated_markdown": translated_markdown,
        "truncated": truncated,
        "cached": false,
        "source_digest": source_digest,
        "usage": response.usage,
    });
    state
        .services
        .skill
        .cache_translation(cache_key, result.clone(), cache_entries);
    Ok(Json(result))
}

async fn skill_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .detail(
            &state.workspace_root,
            state.services.app_registry.as_ref(),
            &id,
        )
        .map(Json)
        .map_err(skill_error)
}

async fn skill_files_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .files(
            &state.workspace_root,
            state.services.app_registry.as_ref(),
            &id,
        )
        .map(Json)
        .map_err(skill_error)
}

async fn skill_file_raw_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<SkillFileQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .raw_file(
            &state.workspace_root,
            state.services.app_registry.as_ref(),
            &id,
            query,
        )
        .map(Json)
        .map_err(skill_error)
}

fn skill_error(error: SkillServiceError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        SkillServiceError::BadRequest(message) => api_error(StatusCode::BAD_REQUEST, message),
        SkillServiceError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        SkillServiceError::Internal(message) => {
            api_error(StatusCode::INTERNAL_SERVER_ERROR, message)
        }
    }
}
