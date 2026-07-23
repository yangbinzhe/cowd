use std::sync::Arc;

use axum::{
    extract::{Multipart, Path, Query, State as AxumState},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/workspace", get(workspace_handler))
        .route("/api/workspaces", get(workspaces_handler))
        .route(
            "/api/workspace/files",
            get(workspace_files_handler)
                .post(create_workspace_file_handler)
                .delete(delete_workspace_path_handler),
        )
        .route("/api/workspace/dirs", post(create_workspace_dir_handler))
        .route("/api/workspace/meta", get(workspace_meta_handler))
        .route("/api/workspace/rename", post(rename_workspace_path_handler))
        .route(
            "/api/workspace/download",
            get(download_workspace_path_handler),
        )
        .route("/api/upload", post(upload_workspace_file_handler))
        .route("/api/file/raw", get(raw_workspace_file_handler))
        .route(
            "/api/sessions/:id/attachments",
            get(list_session_attachments_handler).post(add_session_attachment_handler),
        )
        .route(
            "/api/sessions/:id/attachments/:ref_id",
            delete(delete_session_attachment_handler),
        )
}

#[derive(Deserialize)]
struct WorkspaceFilesParams {
    #[serde(default)]
    dir: Option<String>,
    #[serde(default)]
    recursive: bool,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct RawFileParams {
    path: String,
}

#[derive(Deserialize)]
struct PathParams {
    path: String,
}

#[derive(Deserialize)]
struct CreateWorkspaceFileRequest {
    path: String,
    #[serde(default)]
    content: String,
}

#[derive(Deserialize)]
struct CreateWorkspaceDirRequest {
    path: String,
}

#[derive(Deserialize)]
struct RenameWorkspacePathRequest {
    path: String,
    to: String,
}

#[derive(Deserialize)]
struct AddAttachmentRequest {
    path: String,
    #[serde(default = "default_attachment_kind")]
    kind: String,
    #[serde(default)]
    label: Option<String>,
}

fn default_attachment_kind() -> String {
    "workspace_file".to_string()
}

async fn workspace_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state.services.workspace.overview(
            &state.workspace_root,
            &state.config_home,
            Some(state.profile_id.as_str()),
            state
                .services
                .selected_storage
                .as_ref()
                .and_then(|selected| {
                    selected
                        .registry
                        .endpoint(&storage::StorageDomainId::Session)
                        .ok()
                }),
        ),
    )
}

async fn workspaces_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state
            .services
            .workspace
            .workspaces(&state.workspace_root, Some(state.profile_id.as_str())),
    )
}

async fn workspace_files_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<WorkspaceFilesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .workspace
        .list_files(
            &state.workspace_root,
            params.dir.as_deref(),
            params.recursive,
            params.limit.unwrap_or(500),
        )
        .map(Json)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))
}

async fn create_workspace_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateWorkspaceFileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok((
        StatusCode::CREATED,
        Json(
            state
                .services
                .workspace
                .create_file(&state.workspace_root, &body.path, &body.content)
                .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?,
        ),
    ))
}

async fn create_workspace_dir_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateWorkspaceDirRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok((
        StatusCode::CREATED,
        Json(
            state
                .services
                .workspace
                .create_dir(&state.workspace_root, &body.path)
                .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?,
        ),
    ))
}

async fn delete_workspace_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<PathParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .workspace
        .delete_path(&state.workspace_root, &params.path)
        .map(Json)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))
}

async fn workspace_meta_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<PathParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .workspace
        .metadata(&state.workspace_root, &params.path)
        .map(Json)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))
}

async fn rename_workspace_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<RenameWorkspacePathRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .workspace
        .rename_path(&state.workspace_root, &body.path, &body.to)
        .map(Json)
        .map_err(|e| {
            if e == "target already exists" {
                api_error(StatusCode::CONFLICT, e)
            } else {
                api_error(StatusCode::BAD_REQUEST, e)
            }
        })
}

async fn upload_workspace_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut dir = String::new();
    let mut overwrite = false;
    let mut uploaded: Option<(String, Vec<u8>)> = None;
    const MAX_UPLOAD_BYTES: usize = 25 * 1024 * 1024;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name == "dir" {
            dir = field
                .text()
                .await
                .map_err(|e| api_error(StatusCode::BAD_REQUEST, e.to_string()))?;
        } else if name == "overwrite" {
            overwrite = field
                .text()
                .await
                .map(|value| value == "true" || value == "1")
                .unwrap_or(false);
        } else if name == "file" {
            let file_name = field.file_name().unwrap_or("upload.bin").to_string();
            let bytes = field
                .bytes()
                .await
                .map_err(|e| api_error(StatusCode::BAD_REQUEST, e.to_string()))?;
            if bytes.len() > MAX_UPLOAD_BYTES {
                return Err(api_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "upload is too large",
                ));
            }
            uploaded = Some((file_name, bytes.to_vec()));
        }
    }

    let Some((file_name, bytes)) = uploaded else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "multipart field `file` is required",
        ));
    };
    Ok((
        StatusCode::CREATED,
        Json(
            state
                .services
                .workspace
                .upload_file(&state.workspace_root, &dir, &file_name, &bytes, overwrite)
                .map_err(|e| {
                    if e == "target already exists" {
                        api_error(StatusCode::CONFLICT, e)
                    } else {
                        api_error(StatusCode::BAD_REQUEST, e)
                    }
                })?,
        ),
    ))
}

async fn raw_workspace_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RawFileParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let bytes = state
        .services
        .workspace
        .raw_file(&state.workspace_root, &params.path)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        bytes,
    ))
}

async fn download_workspace_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<PathParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let download = state
        .services
        .workspace
        .download_path(&state.workspace_root, &params.path)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, download.content_type.to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!(
                    "attachment; filename=\"{}\"",
                    sanitize_download_name(&download.file_name)
                ),
            ),
        ],
        download.bytes,
    ))
}

fn sanitize_download_name(name: &str) -> String {
    name.chars()
        .map(|ch| match ch {
            '"' | '\\' | '/' | '\0' => '_',
            _ => ch,
        })
        .collect()
}

async fn list_session_attachments_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    let attachments = state
        .services
        .workspace
        .list_attachments(&state.config_home, &session_id);
    Json(serde_json::json!({
        "session_id": session_id,
        "attachments": attachments,
        "count": attachments.len(),
    }))
}

async fn add_session_attachment_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(body): Json<AddAttachmentRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let attachment = state
        .services
        .workspace
        .add_attachment(
            &state.workspace_root,
            &state.config_home,
            &session_id,
            &body.path,
            body.kind,
            body.label,
        )
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    let count = state
        .services
        .workspace
        .list_attachments(&state.config_home, &session_id)
        .len();
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "session_id": session_id,
            "attachment": attachment,
            "count": count,
        })),
    ))
}

async fn delete_session_attachment_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((session_id, ref_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let deleted = state
        .services
        .workspace
        .delete_attachment(&state.config_home, &session_id, &ref_id)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    if !deleted {
        return Err(api_error(StatusCode::NOT_FOUND, "attachment not found"));
    }
    let count = state
        .services
        .workspace
        .list_attachments(&state.config_home, &session_id)
        .len();
    Ok(Json(serde_json::json!({
        "deleted": true,
        "session_id": session_id,
        "ref_id": ref_id,
        "count": count,
    })))
}
