use std::{fs, path::PathBuf, sync::Arc};

use axum::{
    extract::{Multipart, Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use super::{api_error, AppState, ErrorResponse};

const MAX_RESOURCE_UPLOAD_BYTES: usize = runtime::MAX_RESOURCE_BYTES as usize;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/resources", post(upload_resource_handler))
        .route("/api/resources/:id", get(get_resource_handler))
        .route(
            "/api/resources/:id/evidence",
            get(get_resource_evidence_handler),
        )
}

async fn upload_resource_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut session_id = None::<String>;
    let mut source = "webui".to_string();
    let mut source_message_id = None::<String>;
    let mut declared_mime = None::<String>;
    let mut uploaded = None::<(String, Vec<u8>)>;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "session_id" => {
                session_id = field
                    .text()
                    .await
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
            }
            "source" => {
                source = field
                    .text()
                    .await
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(source);
            }
            "source_message_id" => {
                source_message_id = field
                    .text()
                    .await
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
            }
            "mime" | "declared_mime" => {
                declared_mime = field
                    .text()
                    .await
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
            }
            "file" => {
                let file_name =
                    safe_resource_file_name(field.file_name().unwrap_or("resource.bin"));
                let content_type = field.content_type().map(str::to_string);
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| api_error(StatusCode::BAD_REQUEST, e.to_string()))?;
                if bytes.len() > MAX_RESOURCE_UPLOAD_BYTES {
                    return Err(api_error(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "resource upload is too large",
                    ));
                }
                declared_mime = declared_mime.or(content_type);
                uploaded = Some((file_name, bytes.to_vec()));
            }
            _ => {}
        }
    }

    let Some((file_name, bytes)) = uploaded else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "multipart field `file` is required",
        ));
    };

    let temp_path = write_resource_upload_temp(&state.config_home, &file_name, &bytes)
        .map_err(|e| api_error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let (resource, hint) = runtime::register_resource_from_path(
        &state.config_home,
        &temp_path,
        source,
        source_message_id,
        session_id,
        declared_mime,
    )
    .map_err(|e| api_error(StatusCode::BAD_REQUEST, e))?;
    let _ = fs::remove_file(&temp_path);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "resource": resource,
            "hint": hint,
        })),
    ))
}

async fn get_resource_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = runtime::ResourceStore::default_for_config_home(&state.config_home);
    let resource = store
        .get(&id)
        .map_err(|e| api_error(StatusCode::NOT_FOUND, e))?;
    let hint = runtime::resource_hint(&resource, &runtime::ResourceCapabilitySnapshot::default());
    Ok(Json(serde_json::json!({
        "resource": resource,
        "hint": hint,
    })))
}

async fn get_resource_evidence_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = runtime::ResourceStore::default_for_config_home(&state.config_home);
    Json(serde_json::json!({
        "resource_id": id,
        "evidence": store.evidence(&id),
    }))
}

fn write_resource_upload_temp(
    config_home: &std::path::Path,
    file_name: &str,
    bytes: &[u8],
) -> Result<PathBuf, String> {
    let dir = config_home.join("storage").join("resources").join("temp");
    fs::create_dir_all(&dir).map_err(|e| format!("create resource temp dir: {e}"))?;
    let path = dir.join(format!(
        "{}-{}",
        uuid::Uuid::new_v4().simple(),
        safe_resource_file_name(file_name)
    ));
    fs::write(&path, bytes).map_err(|e| format!("write resource temp file: {e}"))?;
    Ok(path)
}

fn safe_resource_file_name(name: &str) -> String {
    let name = std::path::Path::new(name)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("resource.bin")
        .trim();
    if name.is_empty() {
        "resource.bin".to_string()
    } else {
        name.replace(['/', '\\', '\0'], "_")
    }
}
