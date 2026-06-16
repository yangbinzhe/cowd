use std::{
    fs,
    path::{Component, Path as FsPath, PathBuf},
    sync::Arc,
};

use axum::{
    extract::{Multipart, Path, Query, State as AxumState},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

#[derive(Serialize)]
struct WorkspaceFileItem {
    name: String,
    path: String,
    is_dir: bool,
    #[serde(rename = "type")]
    kind: String,
    size: u64,
    modified_ms: Option<u128>,
}

#[derive(Serialize, Deserialize, Clone)]
struct SessionAttachment {
    ref_id: String,
    kind: String,
    path: String,
    label: String,
    size: u64,
    sha256: String,
    added_at_ms: i64,
}

fn default_attachment_kind() -> String {
    "workspace_file".to_string()
}

fn path_has_safe_relative_components(path: &FsPath) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn workspace_root_canonical(workspace_root: &FsPath) -> Result<PathBuf, String> {
    workspace_root
        .canonicalize()
        .map_err(|e| format!("workspace root is unavailable: {e}"))
}

fn resolve_existing_workspace_path(
    workspace_root: &FsPath,
    relative: Option<&str>,
) -> Result<PathBuf, String> {
    let root = workspace_root_canonical(workspace_root)?;
    let rel = relative.map(str::trim).unwrap_or("");
    let rel_path = FsPath::new(rel);
    if !rel.is_empty() && !path_has_safe_relative_components(rel_path) {
        return Err("path must stay inside the workspace".to_string());
    }
    let candidate = if rel.is_empty() {
        root.clone()
    } else {
        root.join(rel_path)
    };
    let resolved = candidate
        .canonicalize()
        .map_err(|e| format!("path not found: {e}"))?;
    if !resolved.starts_with(&root) {
        return Err("path must stay inside the workspace".to_string());
    }
    Ok(resolved)
}

fn resolve_new_workspace_file_path(
    workspace_root: &FsPath,
    relative: &str,
) -> Result<PathBuf, String> {
    let root = workspace_root_canonical(workspace_root)?;
    let rel = relative.trim();
    if rel.is_empty() {
        return Err("path is required".to_string());
    }
    let rel_path = FsPath::new(rel);
    if !path_has_safe_relative_components(rel_path) {
        return Err("path must stay inside the workspace".to_string());
    }
    let target = root.join(rel_path);
    let parent = target
        .parent()
        .ok_or_else(|| "file parent is unavailable".to_string())?;
    fs::create_dir_all(parent).map_err(|e| format!("failed to create parent directory: {e}"))?;
    let parent_resolved = parent
        .canonicalize()
        .map_err(|e| format!("file parent is unavailable: {e}"))?;
    if !parent_resolved.starts_with(&root) {
        return Err("path must stay inside the workspace".to_string());
    }
    Ok(target)
}

fn workspace_relative_path(root: &FsPath, path: &FsPath) -> String {
    path.strip_prefix(root)
        .ok()
        .and_then(|path| path.to_str())
        .unwrap_or("")
        .replace('\\', "/")
}

fn workspace_file_item(root: &FsPath, path: PathBuf) -> Option<WorkspaceFileItem> {
    let metadata = fs::metadata(&path).ok()?;
    let name = path.file_name()?.to_string_lossy().to_string();
    let is_dir = metadata.is_dir();
    let modified_ms = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis());
    Some(WorkspaceFileItem {
        name,
        path: workspace_relative_path(root, &path),
        is_dir,
        kind: if is_dir { "dir" } else { "file" }.to_string(),
        size: if is_dir { 0 } else { metadata.len() },
        modified_ms,
    })
}

fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn safe_upload_name(name: &str) -> Result<String, String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err("file name is required".to_string());
    }
    let path = FsPath::new(trimmed);
    if !path_has_safe_relative_components(path) || path.components().count() != 1 {
        return Err("uploaded file name must be a safe file name".to_string());
    }
    Ok(trimmed.to_string())
}

fn attachment_store_path(state: &AppState, session_id: &str) -> PathBuf {
    state
        .config_home
        .join("session_attachments")
        .join(format!("{session_id}.json"))
}

fn load_session_attachments(state: &AppState, session_id: &str) -> Vec<SessionAttachment> {
    let path = attachment_store_path(state, session_id);
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Vec<SessionAttachment>>(&raw).ok())
        .unwrap_or_default()
}

fn save_session_attachments(
    state: &AppState,
    session_id: &str,
    attachments: &[SessionAttachment],
) -> Result<(), String> {
    let path = attachment_store_path(state, session_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let rendered = serde_json::to_string_pretty(attachments).map_err(|error| error.to_string())?;
    fs::write(path, rendered).map_err(|error| error.to_string())
}

async fn workspace_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let workspace_root = state.workspace_root.clone();
    let workspace_canonical = workspace_root.canonicalize().ok();
    Json(serde_json::json!({
        "workspace_root": workspace_root.display().to_string(),
        "workspace_canonical": workspace_canonical.map(|path| path.display().to_string()),
        "profile_id": state.profile_id,
        "config_home": state.config_home.display().to_string(),
        "sessions_db": state.config_home.join("sessions.db").display().to_string(),
        "memory_dir": state.config_home.join("memory").display().to_string(),
    }))
}

async fn workspaces_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "workspaces": [{
            "id": "current",
            "name": state.workspace_root.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("workspace"),
            "path": state.workspace_root.display().to_string(),
            "active": true,
            "profile_id": state.profile_id,
        }]
    }))
}

async fn workspace_files_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<WorkspaceFilesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let dir = resolve_existing_workspace_path(&state.workspace_root, params.dir.as_deref())
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if !dir.is_dir() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "path is not a directory",
        ));
    }

    let mut files = fs::read_dir(&dir)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .flatten()
        .filter_map(|entry| workspace_file_item(&root, entry.path()))
        .collect::<Vec<_>>();
    files.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
    });
    files.truncate(500);

    Ok(Json(serde_json::json!({
        "workspace_root": state.workspace_root.display().to_string(),
        "dir": workspace_relative_path(&root, &dir),
        "files": files,
    })))
}

async fn create_workspace_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateWorkspaceFileRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let target = resolve_new_workspace_file_path(&state.workspace_root, &body.path)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if target.exists() && target.is_dir() {
        return Err(api_error(StatusCode::BAD_REQUEST, "path is a directory"));
    }
    fs::write(&target, body.content)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "path": workspace_relative_path(&root, &target),
            "created": true,
        })),
    ))
}

async fn create_workspace_dir_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateWorkspaceDirRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let target = resolve_new_workspace_file_path(&state.workspace_root, &body.path)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    fs::create_dir_all(&target)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "path": workspace_relative_path(&root, &target),
            "created": true,
            "type": "dir",
        })),
    ))
}

async fn delete_workspace_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<PathParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let target = resolve_existing_workspace_path(&state.workspace_root, Some(&params.path))
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if target.is_dir() {
        fs::remove_dir_all(&target)
    } else {
        fs::remove_file(&target)
    }
    .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "deleted": true,
        "path": params.path,
    })))
}

async fn workspace_meta_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<PathParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let target = resolve_existing_workspace_path(&state.workspace_root, Some(&params.path))
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    let item = workspace_file_item(&root, target.clone())
        .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "path not found"))?;
    let sha256 = if target.is_file() {
        fs::read(&target).ok().map(|bytes| hash_bytes(&bytes))
    } else {
        None
    };
    Ok(Json(serde_json::json!({
        "item": item,
        "sha256": sha256,
    })))
}

async fn rename_workspace_path_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<RenameWorkspacePathRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let source = resolve_existing_workspace_path(&state.workspace_root, Some(&body.path))
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    let target = resolve_new_workspace_file_path(&state.workspace_root, &body.to)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if target.exists() {
        return Err(api_error(StatusCode::CONFLICT, "target already exists"));
    }
    fs::rename(&source, &target)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({
        "renamed": true,
        "from": body.path,
        "to": workspace_relative_path(&root, &target),
    })))
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
            let file_name = safe_upload_name(field.file_name().unwrap_or("upload.bin"))
                .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
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
    let path = if dir.trim().is_empty() {
        file_name
    } else {
        format!("{}/{}", dir.trim().trim_end_matches('/'), file_name)
    };
    let target = resolve_new_workspace_file_path(&state.workspace_root, &path)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if target.exists() && !overwrite {
        return Err(api_error(StatusCode::CONFLICT, "target already exists"));
    }
    fs::write(&target, &bytes)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "uploaded": true,
            "path": workspace_relative_path(&root, &target),
            "size": bytes.len(),
            "sha256": hash_bytes(&bytes),
        })),
    ))
}

async fn raw_workspace_file_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RawFileParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let file = resolve_existing_workspace_path(&state.workspace_root, Some(&params.path))
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if !file.is_file() {
        return Err(api_error(StatusCode::BAD_REQUEST, "path is not a file"));
    }
    let bytes =
        fs::read(&file).map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        bytes,
    ))
}

async fn list_session_attachments_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    let attachments = load_session_attachments(&state, &session_id);
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
    let file = resolve_existing_workspace_path(&state.workspace_root, Some(&body.path))
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    if !file.is_file() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "attachment path is not a file",
        ));
    }
    let bytes =
        fs::read(&file).map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let root = workspace_root_canonical(&state.workspace_root)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let relative = workspace_relative_path(&root, &file);
    let hash = hash_bytes(&bytes);
    let ref_id = format!(
        "att-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        hash.trim_start_matches("sha256:")
            .chars()
            .take(12)
            .collect::<String>()
    );
    let attachment = SessionAttachment {
        ref_id: ref_id.clone(),
        kind: body.kind,
        label: body.label.unwrap_or_else(|| relative.clone()),
        path: relative,
        size: bytes.len() as u64,
        sha256: hash,
        added_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    let mut attachments = load_session_attachments(&state, &session_id);
    attachments.retain(|item| item.path != attachment.path);
    attachments.push(attachment.clone());
    save_session_attachments(&state, &session_id, &attachments)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "session_id": session_id,
            "attachment": attachment,
            "count": attachments.len(),
        })),
    ))
}

async fn delete_session_attachment_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((session_id, ref_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut attachments = load_session_attachments(&state, &session_id);
    let before = attachments.len();
    attachments.retain(|item| item.ref_id != ref_id);
    if attachments.len() == before {
        return Err(api_error(StatusCode::NOT_FOUND, "attachment not found"));
    }
    save_session_attachments(&state, &session_id, &attachments)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({
        "deleted": true,
        "session_id": session_id,
        "ref_id": ref_id,
        "count": attachments.len(),
    })))
}
