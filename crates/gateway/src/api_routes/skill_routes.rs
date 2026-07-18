use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use provider::{InputMessage, MessageRequest, OutputContentBlock, ProviderClient};
use serde::Deserialize;

use crate::services::{
    SkillActionRequest, SkillCatalogQuery, SkillFileQuery, SkillMaintenanceEvaluateRequest,
    SkillProjectionQuery, SkillServiceError,
};
use skill::SkillActionKind;

use super::{AppState, ErrorResponse, api_error};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
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
        .route("/api/skills/:id", get(skill_get_handler))
}

async fn skills_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<SkillCatalogQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .catalog(&state.workspace_root, query)
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
        .projection(&state.workspace_root, query)
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
        .detail(&state.workspace_root, &id)
        .map_err(skill_error)?;

    let runtime_config = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map_err(|error| api_error(StatusCode::SERVICE_UNAVAILABLE, error))?;
    let model = runtime_config
        .model()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(crate::DEFAULT_MODEL)
        .to_string();
    let client = match runtime_config.providers().resolve_full(&model) {
        Some(provider) => ProviderClient::from_config(provider),
        None => ProviderClient::from_model(&model),
    }
    .map_err(|error| api_error(StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;

    let char_limit = 24_000usize;
    let truncated = content.chars().count() > char_limit;
    let source = content.chars().take(char_limit).collect::<String>();
    let locale = request.locale.unwrap_or_else(|| "zh-CN".to_string());
    let prompt = format!(
        "请把下面的 Skill Markdown 翻译为 {locale}。\n\
         要求：保留 Markdown 结构、代码块、YAML front matter、命令和路径；只翻译自然语言说明；不要添加额外解释。\n\n\
         <skill id=\"{id}\" path=\"{}\">\n{}\n</skill>",
        request.path.as_deref().unwrap_or("SKILL.md"),
        source
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

    Ok(Json(serde_json::json!({
        "ok": true,
        "kind": "skills.translation",
        "skill_id": id,
        "path": request.path,
        "locale": locale,
        "model": response.model,
        "translated_markdown": translated_markdown,
        "truncated": truncated,
        "usage": response.usage,
    })))
}

async fn skill_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .skill
        .detail(&state.workspace_root, &id)
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
        .files(&state.workspace_root, &id)
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
        .raw_file(&state.workspace_root, &id, query)
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
