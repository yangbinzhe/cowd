//! Immutable dynamic APP catalogue and signed static bundle delivery.

use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use axum::{
    body::Body,
    extract::Path as AxumPath,
    http::{header, HeaderValue, Response, StatusCode},
    routing::get,
    Extension, Json, Router,
};
use cowd_app_protocol::{
    AppCatalogV1, AppId, AppLifecycleV1, ProtocolValidate, Sha256Digest, PROTOCOL_REVISION_V1,
};

use crate::app_platform::{GatewayAppPlatform, PROTOCOL_DIGEST_V1};

pub(super) fn router(platform: Arc<GatewayAppPlatform>) -> Router<Arc<super::AppState>> {
    Router::new()
        .route("/api/apps", get(list_apps))
        .route("/api/apps/:app_id", get(get_app))
        .route("/apps/:app_id", get(serve_index))
        .route("/apps/:app_id/*path", get(serve_asset))
        .layer(Extension(platform))
}

async fn list_apps(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
) -> Result<Json<AppCatalogV1>, (StatusCode, Json<serde_json::Value>)> {
    Ok(Json(project_catalog(&platform, &principal).await?))
}

async fn get_app(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumPath(app_id): AxumPath<String>,
) -> Result<Json<cowd_app_protocol::AppCatalogEntryV1>, (StatusCode, Json<serde_json::Value>)> {
    let app_id = AppId(app_id);
    let catalog = project_catalog(&platform, &principal).await?;
    catalog
        .apps
        .into_iter()
        .find(|app| app.app_id == app_id)
        .map(Json)
        .ok_or_else(|| typed_error(StatusCode::NOT_FOUND, "app_not_found", "APP is not mounted"))
}

async fn project_catalog(
    platform: &GatewayAppPlatform,
    principal: &super::AuthenticatedPrincipal,
) -> Result<AppCatalogV1, (StatusCode, Json<serde_json::Value>)> {
    let granted = &principal.0.claims().capabilities;
    let statuses = platform
        .supervisor()
        .statuses()
        .await
        .into_iter()
        .map(|status| (status.app_id.clone(), status))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut apps = platform
        .catalog()
        .apps()
        .map(|app| {
            let mut entry = app.catalog_entry();
            entry
                .effective_capabilities
                .retain(|capability| granted.binary_search(capability).is_ok());
            if let Some(status) = statuses.get(&entry.app_id) {
                entry.lifecycle = AppLifecycleV1 {
                    state: status.state,
                    reason_code: status.reason.as_ref().map(|_| "runtime_failure".to_owned()),
                    retryable: matches!(
                        status.state,
                        cowd_app_protocol::AppLifecycleStateV1::Failed
                            | cowd_app_protocol::AppLifecycleStateV1::CircuitOpen
                    ),
                    retry_after_ms: None,
                };
            }
            entry
        })
        .collect::<Vec<_>>();
    apps.sort_by(|left, right| left.app_id.cmp(&right.app_id));
    let catalog = AppCatalogV1 {
        schema_version: 1,
        protocol_revision: PROTOCOL_REVISION_V1,
        protocol_digest: Sha256Digest(PROTOCOL_DIGEST_V1.to_owned()),
        catalog_generation: platform.catalog().generation().clone(),
        apps,
    };
    catalog.validate().map_err(|error| {
        typed_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "catalog_projection_invalid",
            &error.to_string(),
        )
    })?;
    Ok(catalog)
}

async fn serve_index(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    AxumPath(app_id): AxumPath<String>,
) -> Response<Body> {
    serve(&platform, &app_id, "index.html").await
}

async fn serve_asset(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    AxumPath((app_id, path)): AxumPath<(String, String)>,
) -> Response<Body> {
    serve(&platform, &app_id, &path).await
}

async fn serve(platform: &GatewayAppPlatform, app_id: &str, requested: &str) -> Response<Body> {
    let Some(app) = platform.catalog().get(&AppId(app_id.to_owned())) else {
        return response(StatusCode::NOT_FOUND, Body::empty());
    };
    let Some(web_root) = app.web_root.as_ref().filter(|_| app.manifest.surfaces.web) else {
        return response(StatusCode::NOT_FOUND, Body::empty());
    };
    let requested = Path::new(requested);
    if requested.is_absolute()
        || requested.components().any(|part| match part {
            Component::Normal(value) => {
                value.to_string_lossy().starts_with('.')
                    || value.to_string_lossy().ends_with(".map")
            }
            _ => true,
        })
    {
        return response(StatusCode::NOT_FOUND, Body::empty());
    }
    let candidate = web_root.join(requested);
    let path = match canonical_signed_asset(app, web_root, &candidate) {
        Ok(path) if path.is_file() => path,
        _ => {
            let index = web_root.join("index.html");
            match canonical_signed_asset(app, web_root, &index) {
                Ok(path) => path,
                Err(_) => return response(StatusCode::NOT_FOUND, Body::empty()),
            }
        }
    };
    let body = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(_) => return response(StatusCode::NOT_FOUND, Body::empty()),
    };
    let mut response = response(StatusCode::OK, Body::from(body));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type(&path)),
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", app.generation.0))
            .unwrap_or_else(|_| HeaderValue::from_static("\"invalid\"")),
    );
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(
            "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn canonical_signed_asset(
    app: &cowd_app_host::catalog::AdmittedApp,
    web_root: &Path,
    candidate: &Path,
) -> Result<PathBuf, ()> {
    let canonical = std::fs::canonicalize(candidate).map_err(|_| ())?;
    if !canonical.starts_with(web_root) {
        return Err(());
    }
    let metadata = std::fs::symlink_metadata(candidate).map_err(|_| ())?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(());
    }
    let relative = canonical
        .strip_prefix(&app.bundle_root)
        .map_err(|_| ())?
        .to_string_lossy()
        .replace('\\', "/");
    if !app.manifest.integrity.files.contains_key(&relative) {
        return Err(());
    }
    Ok(canonical)
}

fn content_type(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
    {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}
fn response(status: StatusCode, body: Body) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(body)
        .expect("static response")
}
fn typed_error(
    status: StatusCode,
    code: &str,
    detail: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    (
        status,
        Json(serde_json::json!({"error":{"code":code,"detail":detail}})),
    )
}
