use std::{ops::Range, sync::Arc};

use axum::{
    body::Body,
    extract::{Extension, Multipart, Path, State as AxumState},
    http::{header, HeaderMap, HeaderValue, Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use harness_contract::context::ArtifactWriteDescriptor;

use super::{
    api_error,
    session_routes::{authorize_session_access, SessionAccess},
    AppState, AuthenticatedPrincipal, ErrorResponse,
};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/resources", post(upload_resource_handler))
        .route("/api/resources/:id", get(get_resource_handler))
        .route(
            "/api/resources/:id/content",
            get(get_resource_content_handler),
        )
        .route(
            "/api/resources/:id/evidence",
            get(get_resource_evidence_handler),
        )
}

async fn upload_resource_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut session_id = None::<String>;
    let mut source = "webui".to_string();
    let mut source_message_id = None::<String>;
    let mut declared_mime = None::<String>;
    let mut uploaded = None;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "session_id" => {
                ensure_metadata_precedes_file(uploaded.is_some(), "session_id")?;
                session_id = field
                    .text()
                    .await
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
            }
            "source" => {
                ensure_metadata_precedes_file(uploaded.is_some(), "source")?;
                source = field
                    .text()
                    .await
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(source);
            }
            "source_message_id" => {
                ensure_metadata_precedes_file(uploaded.is_some(), "source_message_id")?;
                source_message_id = field
                    .text()
                    .await
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
            }
            "mime" | "declared_mime" => {
                ensure_metadata_precedes_file(uploaded.is_some(), "declared_mime")?;
                declared_mime = field
                    .text()
                    .await
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty());
            }
            "file" => {
                if uploaded.is_some() {
                    return Err(api_error(
                        StatusCode::BAD_REQUEST,
                        "exactly one multipart file is supported",
                    ));
                }
                let file_name =
                    safe_resource_file_name(field.file_name().unwrap_or("resource.bin"));
                let content_type = field.content_type().map(str::to_string);
                let media_type = declared_mime
                    .clone()
                    .or_else(|| content_type.clone())
                    .unwrap_or_else(|| "application/octet-stream".to_string());
                if let Some(session_id) = session_id.as_deref() {
                    authorize_session_access(&state, &principal, session_id, SessionAccess::Write)
                        .await?;
                }
                let visibility_scope = session_id
                    .as_ref()
                    .map_or_else(|| "public".to_string(), |id| format!("session:{id}"));
                let artifacts = state.services.artifact_store().ok_or_else(|| {
                    api_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "runtime artifact store is unavailable",
                    )
                })?;
                let mut sink = artifacts
                    .begin(ArtifactWriteDescriptor {
                        media_type,
                        visibility_scope,
                        expected_bytes: None,
                        original_name: Some(file_name.clone()),
                    })
                    .await
                    .map_err(artifact_api_error)?;
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?
                {
                    sink.write_chunk(&chunk).await.map_err(artifact_api_error)?;
                }
                let artifact = sink.finish().await.map_err(artifact_api_error)?;
                declared_mime = declared_mime.or(content_type);
                uploaded = Some((file_name, artifact));
            }
            _ => {}
        }
    }

    let Some((file_name, artifact)) = uploaded else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "multipart field `file` is required",
        ));
    };
    let artifacts = state.services.artifact_store().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime artifact store is unavailable",
        )
    })?;
    let store = runtime::ResourceStore::from_artifact_store(
        &state.config_home,
        artifacts,
        state.services.resource_capability_index(),
    );
    let (resource, hint) = store
        .register_uploaded_artifact(
            artifact,
            source,
            source_message_id,
            session_id,
            file_name,
            declared_mime,
        )
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, error))?;

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
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = resource_store(&state)?;
    let resource = store
        .get(&id)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    authorize_resource_read(&state, &principal, &resource).await?;
    let hint = runtime::resource_hint(
        &resource,
        &state.services.resource_capability_index().snapshot(),
    );
    Ok(Json(serde_json::json!({
        "resource": resource,
        "hint": hint,
    })))
}

async fn get_resource_content_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response<Body>, (StatusCode, Json<ErrorResponse>)> {
    let store = resource_store(&state)?;
    let resource = store
        .get(&id)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    authorize_resource_read(&state, &principal, &resource).await?;
    let range = parse_range(headers.get(header::RANGE), resource.size_bytes)?;
    let body = store
        .artifact_store()
        .read(
            &resource.artifact,
            &resource.artifact.visibility_scope,
            range.clone(),
        )
        .await
        .map_err(artifact_api_error)?;
    let mut response = Response::new(Body::from(body));
    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(
            resource
                .detected_mime
                .as_deref()
                .or(resource.declared_mime.as_deref())
                .unwrap_or("application/octet-stream"),
        )
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(range) = range {
        let value = format!(
            "bytes {}-{}/{}",
            range.start,
            range.end.saturating_sub(1),
            resource.size_bytes
        );
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&value)
                .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?,
        );
    }
    Ok(response)
}

async fn get_resource_evidence_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let store = resource_store(&state)?;
    let resource = store
        .get(&id)
        .map_err(|error| api_error(StatusCode::NOT_FOUND, error))?;
    authorize_resource_read(&state, &principal, &resource).await?;
    Ok(Json(serde_json::json!({
        "resource_id": id,
        "evidence": store.evidence(&id),
    })))
}

async fn authorize_resource_read(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    resource: &runtime::ResourceProjection,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if let Some(session_id) = resource.session_id.as_deref() {
        authorize_session_access(state, principal, session_id, SessionAccess::Read).await?;
    }
    Ok(())
}

fn resource_store(
    state: &AppState,
) -> Result<runtime::ResourceStore, (StatusCode, Json<ErrorResponse>)> {
    let artifacts = state.services.artifact_store().ok_or_else(|| {
        api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime artifact store is unavailable",
        )
    })?;
    Ok(runtime::ResourceStore::from_artifact_store(
        &state.config_home,
        artifacts,
        state.services.resource_capability_index(),
    ))
}

fn ensure_metadata_precedes_file(
    file_seen: bool,
    field: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if file_seen {
        Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("multipart metadata field `{field}` must precede `file`"),
        ))
    } else {
        Ok(())
    }
}

fn artifact_api_error(error: runtime::ArtifactError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match error {
        runtime::ArtifactError::ObjectTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        runtime::ArtifactError::QuotaExceeded => StatusCode::INSUFFICIENT_STORAGE,
        runtime::ArtifactError::NotFound => StatusCode::NOT_FOUND,
        runtime::ArtifactError::Unauthorized => StatusCode::FORBIDDEN,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    api_error(status, error.to_string())
}

fn parse_range(
    value: Option<&HeaderValue>,
    bytes: u64,
) -> Result<Option<Range<u64>>, (StatusCode, Json<ErrorResponse>)> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| api_error(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range header"))?;
    let Some(value) = value.strip_prefix("bytes=") else {
        return Err(api_error(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "only byte ranges are supported",
        ));
    };
    let Some((start, end)) = value.split_once('-') else {
        return Err(api_error(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "invalid byte range",
        ));
    };
    let start = start
        .parse::<u64>()
        .map_err(|_| api_error(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range start"))?;
    let end = if end.is_empty() {
        bytes
    } else {
        end.parse::<u64>()
            .map_err(|_| api_error(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range end"))?
            .saturating_add(1)
    };
    if start >= end || end > bytes {
        return Err(api_error(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "requested range is outside the artifact",
        ));
    }
    Ok(Some(start..end))
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
