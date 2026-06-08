use std::{
    fs,
    path::{Component, Path as FsPath, PathBuf},
    sync::Arc,
};

use axum::{
    extract::{Query, State as AxumState},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/workspace", get(workspace_handler))
        .route("/api/workspaces", get(workspaces_handler))
        .route(
            "/api/workspace/files",
            get(workspace_files_handler).post(create_workspace_file_handler),
        )
        .route("/api/file/raw", get(raw_workspace_file_handler))
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
struct CreateWorkspaceFileRequest {
    path: String,
    #[serde(default)]
    content: String,
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
