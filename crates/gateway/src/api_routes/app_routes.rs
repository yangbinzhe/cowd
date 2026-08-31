//! Immutable dynamic APP catalogue and signed static bundle delivery.

use std::{
    convert::Infallible,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use axum::{
    body::Body,
    extract::{Path as AxumPath, State as AxumState},
    http::{header, HeaderMap, HeaderValue, Method, Request, Response, StatusCode},
    routing::{delete, get, post},
    Extension, Json, Router,
};
use bytes::{Bytes, BytesMut};
use cowd_app_host::supervisor::AppRuntimeLease;
use cowd_app_protocol::{
    manifest_authorization_profile_digest_v1, manifest_capability_digest_v1, AppActionV1,
    AppCatalogEntryV1, AppCatalogV1, AppHandshakeV1, AppId, AppInvocationEnvelopeV1,
    AppLifecycleV1, AppManifestV1, AppProviderResponseV1, AppStreamAckV1, AppStreamFrameV1,
    AppTuiViewActionResponseV1, AppTuiViewOpenRequestV1, AppTuiViewOpenResponseV1,
    AppTuiViewStreamRequestV1, DelegationKindV1, DurableReceiptV1, ExecutionContextV1,
    OperationDescriptorV1, OperationKindV1, PrincipalContextV1, ProtocolValidate, Sha256Digest,
    APP_RECEIPT_PATH_V1, APP_SUBSCRIPTION_ACK_PATH_V1, APP_SUBSCRIPTION_PATH_V1,
    HEADER_APP_GENERATION_V1, HEADER_APP_ID_V1, HEADER_AUTHORIZATION_V1, HEADER_CONTENT_TYPE_V1,
    HEADER_CORRELATION_ID_V1, HEADER_DEADLINE_UNIX_MS_V1, HEADER_PROTOCOL_VERSION_V1,
    HEADER_REQUEST_ID_V1, PROTOCOL_REVISION_V1, STREAM_CONTENT_TYPE_V1, UNARY_CONTENT_TYPE_V1,
};
use harness_contract::security::PrincipalKind;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use managed_worker_runtime::CancellationToken;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::app_platform::{
    GatewayAppConnection, GatewayAppConnector, GatewayAppPlatform, PROTOCOL_DIGEST_V1,
};

const APP_STATIC_CONTENT_SECURITY_POLICY: &str = "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'none'; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'self'";

pub(super) fn router(platform: Arc<GatewayAppPlatform>) -> Router<Arc<super::AppState>> {
    Router::new()
        .route(
            surface::gateway_api::paths::API_APPS.template(),
            get(list_apps),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID.template(),
            get(get_app),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_LOGS.template(),
            get(get_app_logs),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_RESTART.template(),
            post(restart_app),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_OPERATIONS_BY_OPERATION_ID_INVOKE
                .template(),
            post(invoke_operation),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_OPERATIONS_BY_OPERATION_ID_STREAM
                .template(),
            post(stream_operation),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_RECEIPTS_BY_RECEIPT_ID.template(),
            get(get_receipt),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_SUBSCRIPTIONS_BY_SUBSCRIPTION_ID_ACK
                .template(),
            post(ack_subscription),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_SUBSCRIPTIONS_BY_SUBSCRIPTION_ID
                .template(),
            delete(cancel_subscription),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_OPEN.template(),
            post(tui_open),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_ACTIONS.template(),
            post(tui_action),
        )
        .route(
            surface::gateway_api::paths::API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_STREAM.template(),
            post(tui_stream),
        )
        .layer(Extension(platform))
}

/// Serves immutable, integrity-checked APP web assets without a browser session.
///
/// The iframe is sandboxed to an opaque origin and its CSP denies network
/// access, so it can communicate with Cowd only through the authenticated host
/// MessageChannel. Keeping signed static bytes public also avoids sending the
/// `/api` session cookie into an untrusted APP document.
pub(super) fn static_router(platform: Arc<GatewayAppPlatform>) -> Router<Arc<super::AppState>> {
    Router::new()
        .route(
            surface::gateway_api::paths::APPS_BY_APP_ID.template(),
            get(serve_index),
        )
        .route(
            surface::gateway_api::paths::APPS_BY_APP_ID_WILDCARD_PATH.template(),
            get(serve_asset),
        )
        .layer(Extension(platform))
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayAppLogStreamV1 {
    text: String,
    retained_bytes: usize,
    dropped_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayAppLogsResponseV1 {
    schema_version: u16,
    app_id: AppId,
    generation: cowd_app_protocol::GenerationId,
    stdout: GatewayAppLogStreamV1,
    stderr: GatewayAppLogStreamV1,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayAppRestartResponseV1 {
    schema_version: u16,
    app_id: AppId,
    generation: cowd_app_protocol::GenerationId,
    lifecycle: AppLifecycleV1,
}

async fn get_app_logs(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumPath(app_id): AxumPath<String>,
) -> Result<Json<GatewayAppLogsResponseV1>, (StatusCode, Json<serde_json::Value>)> {
    require_app_management_access(&platform, &principal, &app_id, "runtime.task.read")?;
    let app_id = AppId(app_id);
    let admitted = platform
        .catalog()
        .get(&app_id)
        .ok_or_else(|| typed_error(StatusCode::NOT_FOUND, "app_not_found", "APP is not mounted"))?;
    let logs = platform
        .supervisor()
        .logs(&app_id)
        .await
        .map_err(supervisor_management_error)?;
    Ok(Json(GatewayAppLogsResponseV1 {
        schema_version: 1,
        app_id,
        generation: admitted.generation.clone(),
        stdout: GatewayAppLogStreamV1 {
            text: String::from_utf8_lossy(&logs.stdout.bytes).into_owned(),
            retained_bytes: logs.stdout.bytes.len(),
            dropped_bytes: logs.stdout.dropped_bytes,
        },
        stderr: GatewayAppLogStreamV1 {
            text: String::from_utf8_lossy(&logs.stderr.bytes).into_owned(),
            retained_bytes: logs.stderr.bytes.len(),
            dropped_bytes: logs.stderr.dropped_bytes,
        },
    }))
}

async fn restart_app(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumPath(app_id): AxumPath<String>,
) -> Result<Json<GatewayAppRestartResponseV1>, (StatusCode, Json<serde_json::Value>)> {
    require_app_management_access(&platform, &principal, &app_id, "runtime.maintenance.manage")?;
    let app_id = AppId(app_id);
    let admitted = platform
        .catalog()
        .get(&app_id)
        .ok_or_else(|| typed_error(StatusCode::NOT_FOUND, "app_not_found", "APP is not mounted"))?;
    let generation = admitted.generation.clone();
    let cancellation = CancellationToken::default();
    platform
        .supervisor()
        .restart(&app_id, &generation, &cancellation)
        .await
        .map_err(supervisor_management_error)?;
    let status = platform
        .supervisor()
        .status(&app_id)
        .await
        .map_err(supervisor_management_error)?;
    Ok(Json(GatewayAppRestartResponseV1 {
        schema_version: 1,
        app_id,
        generation,
        lifecycle: AppLifecycleV1 {
            state: status.state,
            reason_code: status.reason.map(|_| "runtime_failure".to_owned()),
            retryable: matches!(
                status.state,
                cowd_app_protocol::AppLifecycleStateV1::Failed
                    | cowd_app_protocol::AppLifecycleStateV1::CircuitOpen
            ),
            retry_after_ms: None,
        },
    }))
}

fn require_app_management_access(
    platform: &GatewayAppPlatform,
    principal: &super::AuthenticatedPrincipal,
    app_id: &str,
    core_capability: &str,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let app_id = AppId(app_id.to_owned());
    if platform.catalog().get(&app_id).is_none() {
        return Err(typed_error(
            StatusCode::NOT_FOUND,
            "app_not_found",
            "APP is not mounted",
        ));
    }
    let claims = principal.0.claims();
    if claims.app_profile(&app_id.0).is_none() || !claims.has_capability(core_capability) {
        return Err(typed_error(
            StatusCode::FORBIDDEN,
            "app_management_forbidden",
            "the principal lacks the signed APP profile or Core management capability",
        ));
    }
    Ok(())
}

fn supervisor_management_error(
    error: cowd_app_host::supervisor::SupervisorError,
) -> (StatusCode, Json<serde_json::Value>) {
    use cowd_app_host::supervisor::SupervisorError;
    let (status, code) = match &error {
        SupervisorError::UnknownApp(_) => (StatusCode::NOT_FOUND, "app_not_found"),
        SupervisorError::CircuitOpen(_) => (StatusCode::SERVICE_UNAVAILABLE, "app_circuit_open"),
        SupervisorError::BackingOff { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "app_restart_backoff")
        }
        SupervisorError::DeadlineExceeded(_) => {
            (StatusCode::GATEWAY_TIMEOUT, "app_management_deadline")
        }
        SupervisorError::Cancelled | SupervisorError::ShuttingDown => (
            StatusCode::SERVICE_UNAVAILABLE,
            "app_management_unavailable",
        ),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "app_management_failed"),
    };
    typed_error(status, code, &error.to_string())
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
) -> Result<Json<GatewayAppDetailResponseV1>, (StatusCode, Json<serde_json::Value>)> {
    let app_id = AppId(app_id);
    let catalog = project_catalog(&platform, &principal).await?;
    let entry = catalog
        .apps
        .into_iter()
        .find(|app| app.app_id == app_id)
        .ok_or_else(|| typed_error(StatusCode::NOT_FOUND, "app_not_found", "APP is not mounted"))?;
    let admitted = platform
        .catalog()
        .get(&app_id)
        .ok_or_else(|| typed_error(StatusCode::NOT_FOUND, "app_not_found", "APP is not mounted"))?;
    let cancellation = CancellationToken::default();
    let lease = platform
        .supervisor()
        .acquire(
            &app_id,
            &admitted.generation,
            Duration::from_secs(15),
            &cancellation,
        )
        .await
        .map_err(platform_error)?;
    let detail = GatewayAppDetailResponseV1 {
        schema_version: 1,
        entry,
        manifest: admitted.manifest.clone(),
        operations: lease.connection().operations().to_vec(),
    };
    validate_detail(&detail)
        .map_err(|detail| typed_error(StatusCode::BAD_GATEWAY, "app_contract_invalid", &detail))?;
    Ok(Json(detail))
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct GatewayAppDetailResponseV1 {
    schema_version: u16,
    entry: AppCatalogEntryV1,
    manifest: AppManifestV1,
    operations: Vec<OperationDescriptorV1>,
}

fn validate_detail(detail: &GatewayAppDetailResponseV1) -> Result<(), String> {
    detail.entry.validate().map_err(|error| error.to_string())?;
    detail
        .manifest
        .validate()
        .map_err(|error| error.to_string())?;
    let digest = cowd_app_protocol::app_operation_catalog_digest_v1(
        &detail.manifest.app_id,
        &detail.operations,
    )
    .map_err(|error| error.to_string())?;
    if detail.schema_version != 1
        || detail.entry.app_id != detail.manifest.app_id
        || detail.entry.artifact_version != detail.manifest.artifact_version
        || digest != detail.manifest.operation_catalog_digest
    {
        return Err("APP detail identity or signed operation catalog mismatch".to_owned());
    }
    AppHandshakeV1 {
        schema_version: 1,
        protocol_revision: PROTOCOL_REVISION_V1,
        app_id: detail.manifest.app_id.clone(),
        generation: detail.entry.generation.clone(),
        artifact_version: detail.manifest.artifact_version.clone(),
        worker_pid: 1,
        worker_nonce: "sanitized-detail-validation".to_owned(),
        operations: detail.operations.clone(),
        operation_catalog_digest: digest,
        capability_digest: manifest_capability_digest_v1(&detail.manifest)
            .map_err(|error| error.to_string())?,
        authorization_profile_digest: manifest_authorization_profile_digest_v1(&detail.manifest)
            .map_err(|error| error.to_string())?,
    }
    .validate_against_manifest(&detail.manifest)
    .map_err(|error| error.to_string())?;
    Ok(())
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
    let mut apps = Vec::new();
    for app in platform.catalog().apps() {
        let Some(profile_id) = principal.0.claims().app_profile(&app.manifest.app_id.0) else {
            continue;
        };
        let profile = app
            .manifest
            .authorization_profiles
            .iter()
            .find(|profile| profile.profile_id == profile_id)
            .ok_or_else(|| {
                typed_error(
                    StatusCode::UNAUTHORIZED,
                    "signed_app_profile_invalid",
                    "the signed principal APP profile is not admitted by this catalog generation",
                )
            })?;
        let mut entry = app.catalog_entry();
        entry.effective_authorization_profile = profile.profile_id.clone();
        entry.effective_capabilities = profile.capabilities.clone();
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
        apps.push(entry);
    }
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvokeInput {
    #[serde(default)]
    payload: serde_json::Value,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    expected_revision: Option<String>,
    #[serde(default)]
    deadline_ms: Option<u64>,
}

async fn invoke_operation(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumState(state): AxumState<Arc<super::AppState>>,
    headers: HeaderMap,
    AxumPath((app_id, operation_id)): AxumPath<(String, String)>,
    Json(input): Json<InvokeInput>,
) -> Response<Body> {
    proxy_unary(
        &platform,
        &state.workspace_root,
        &principal,
        &headers,
        &app_id,
        &operation_id,
        input,
        None,
        None,
    )
    .await
}

async fn stream_operation(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumState(state): AxumState<Arc<super::AppState>>,
    headers: HeaderMap,
    AxumPath((app_id, operation_id)): AxumPath<(String, String)>,
    Json(input): Json<InvokeInput>,
) -> Response<Body> {
    proxy_stream(
        &platform,
        &state.workspace_root,
        &principal,
        &headers,
        &app_id,
        &operation_id,
        input,
        None,
        false,
    )
    .await
}

async fn tui_open(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumState(state): AxumState<Arc<super::AppState>>,
    headers: HeaderMap,
    AxumPath((app_id, view_id)): AxumPath<(String, String)>,
    Json(payload): Json<AppTuiViewOpenRequestV1>,
) -> Response<Body> {
    if payload.validate().is_err() || payload.view_id != view_id {
        return proxy_error(
            StatusCode::BAD_REQUEST,
            "invalid_tui_open",
            "view id mismatch",
        );
    }
    let Some(operation_id) = tui_operation(&platform, &app_id, &view_id, "open") else {
        return proxy_error(
            StatusCode::NOT_FOUND,
            "tui_view_not_found",
            "signed TUI view not found",
        );
    };
    let payload = match serde_json::to_value(payload) {
        Ok(payload) => payload,
        Err(error) => {
            return proxy_error(
                StatusCode::BAD_REQUEST,
                "invalid_tui_open",
                &error.to_string(),
            )
        }
    };
    proxy_unary(
        &platform,
        &state.workspace_root,
        &principal,
        &headers,
        &app_id,
        &operation_id,
        InvokeInput {
            payload,
            idempotency_key: None,
            expected_revision: None,
            deadline_ms: None,
        },
        Some(format!("/_cowd/v1/tui/views/{}/open", encode(&view_id))),
        Some(TuiUnaryRole::Open),
    )
    .await
}

async fn tui_action(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumState(state): AxumState<Arc<super::AppState>>,
    headers: HeaderMap,
    AxumPath((app_id, view_id)): AxumPath<(String, String)>,
    Json(payload): Json<AppActionV1>,
) -> Response<Body> {
    if payload.validate().is_err() || payload.app_id.0 != app_id || payload.view_id != view_id {
        return proxy_error(
            StatusCode::BAD_REQUEST,
            "invalid_tui_action",
            "TUI action route mismatch",
        );
    }
    let Some(operation_id) = tui_operation(&platform, &app_id, &view_id, "action") else {
        return proxy_error(
            StatusCode::NOT_FOUND,
            "tui_view_not_found",
            "signed TUI view not found",
        );
    };
    let action_id = payload.action_id.clone();
    let payload = match serde_json::to_value(payload) {
        Ok(payload) => payload,
        Err(error) => {
            return proxy_error(
                StatusCode::BAD_REQUEST,
                "invalid_tui_action",
                &error.to_string(),
            )
        }
    };
    let claims = principal.0.claims();
    let idempotency_key = stable_tui_action_idempotency(
        &app_id,
        &view_id,
        &payload,
        &claims.principal_id,
        &claims.tenant_id,
        &state.workspace_root,
    );
    proxy_unary(
        &platform,
        &state.workspace_root,
        &principal,
        &headers,
        &app_id,
        &operation_id,
        InvokeInput {
            payload,
            idempotency_key: Some(idempotency_key),
            expected_revision: None,
            deadline_ms: None,
        },
        Some(format!(
            "/_cowd/v1/tui/views/{}/actions/{}",
            encode(&view_id),
            encode(&action_id)
        )),
        Some(TuiUnaryRole::Action),
    )
    .await
}

async fn tui_stream(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumState(state): AxumState<Arc<super::AppState>>,
    headers: HeaderMap,
    AxumPath((app_id, view_id)): AxumPath<(String, String)>,
    Json(payload): Json<AppTuiViewStreamRequestV1>,
) -> Response<Body> {
    if payload.validate().is_err() || payload.view_id != view_id {
        return proxy_error(
            StatusCode::BAD_REQUEST,
            "invalid_tui_stream",
            "view id mismatch",
        );
    }
    let Some(operation_id) = tui_operation(&platform, &app_id, &view_id, "stream") else {
        return proxy_error(
            StatusCode::NOT_FOUND,
            "tui_view_not_found",
            "signed TUI view not found",
        );
    };
    let payload = match serde_json::to_value(payload) {
        Ok(payload) => payload,
        Err(error) => {
            return proxy_error(
                StatusCode::BAD_REQUEST,
                "invalid_tui_stream",
                &error.to_string(),
            )
        }
    };
    proxy_stream(
        &platform,
        &state.workspace_root,
        &principal,
        &headers,
        &app_id,
        &operation_id,
        InvokeInput {
            payload,
            idempotency_key: None,
            expected_revision: None,
            deadline_ms: None,
        },
        Some(format!("/_cowd/v1/tui/views/{}/stream", encode(&view_id))),
        true,
    )
    .await
}

fn tui_operation(
    platform: &GatewayAppPlatform,
    app_id: &str,
    view_id: &str,
    role: &str,
) -> Option<String> {
    let app = platform.catalog().get(&AppId(app_id.to_owned()))?;
    let view = app
        .manifest
        .presentation
        .as_ref()?
        .tui_views
        .iter()
        .find(|view| view.view_id == view_id)?;
    Some(
        match role {
            "open" => &view.open_operation_id,
            "action" => &view.action_operation_id,
            "stream" => &view.stream_operation_id,
            _ => return None,
        }
        .clone(),
    )
}

// The explicit arguments are the complete trust boundary; keeping them visible
// prevents a partially initialized proxy context from acquiring a worker.
#[allow(clippy::too_many_arguments)]
async fn proxy_unary(
    platform: &GatewayAppPlatform,
    workspace_root: &Path,
    principal: &super::AuthenticatedPrincipal,
    headers: &HeaderMap,
    app_id: &str,
    operation_id: &str,
    input: InvokeInput,
    worker_path: Option<String>,
    tui_role: Option<TuiUnaryRole>,
) -> Response<Body> {
    let (lease, descriptor) =
        match acquire_operation(platform, principal, app_id, operation_id).await {
            Ok(acquired) => acquired,
            Err(response) => return response,
        };
    if descriptor.streaming {
        return proxy_error(
            StatusCode::BAD_REQUEST,
            "stream_required",
            "operation requires stream route",
        );
    }
    let envelope = match invocation_envelope(
        platform,
        workspace_root,
        principal,
        headers,
        app_id,
        &descriptor,
        input,
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let path = worker_path
        .unwrap_or_else(|| format!("/_cowd/v1/operations/{}/invoke", encode(operation_id)));
    let effective_deadline = envelope.effective_deadline_unix_ms();
    let timeout = match deadline_timeout(effective_deadline) {
        Ok(timeout) => timeout,
        Err(response) => return response,
    };
    let response = match send_worker(
        lease.connection(),
        lease.app_id(),
        lease.generation().0.as_str(),
        &envelope.request_id,
        effective_deadline,
        Method::POST,
        &path,
        Some(&envelope),
        timeout,
    )
    .await
    {
        Ok(response) => response,
        Err(response) => return response,
    };
    let status = response.status();
    let bytes = match bounded_body(response.into_body(), descriptor.maximum_response_bytes).await {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };
    if !status.is_success() {
        return response_bytes(status, bytes, UNARY_CONTENT_TYPE_V1);
    }
    if descriptor.kind == OperationKindV1::Command && tui_role.is_none() {
        let receipt: DurableReceiptV1 = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return proxy_error(
                    StatusCode::BAD_GATEWAY,
                    "invalid_app_response",
                    "APP returned an invalid receipt",
                )
            }
        };
        if receipt.validate().is_err()
            || receipt.request_id != envelope.request_id
            || envelope.idempotency_key.as_deref() != Some(receipt.idempotency_key.as_str())
        {
            return proxy_error(
                StatusCode::BAD_GATEWAY,
                "invalid_app_response",
                "APP receipt binding mismatch",
            );
        }
        return match serde_json::to_vec(&receipt) {
            Ok(bytes) => response_bytes(status, bytes, UNARY_CONTENT_TYPE_V1),
            Err(error) => proxy_error(
                StatusCode::BAD_GATEWAY,
                "invalid_app_response",
                &error.to_string(),
            ),
        };
    }
    let provider: AppProviderResponseV1 = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return proxy_error(
                StatusCode::BAD_GATEWAY,
                "invalid_app_response",
                "APP returned an invalid provider response",
            )
        }
    };
    if provider.validate().is_err()
        || provider.request_id != envelope.request_id
        || provider.output_schema_digest != descriptor.output_schema_digest
    {
        return proxy_error(
            StatusCode::BAD_GATEWAY,
            "invalid_app_response",
            "APP response binding mismatch",
        );
    }
    let value = match tui_role {
        Some(TuiUnaryRole::Open) => {
            match serde_json::from_value::<AppTuiViewOpenResponseV1>(provider.payload) {
                Ok(value) if value.validate().is_ok() => serde_json::to_value(value),
                _ => {
                    return proxy_error(
                        StatusCode::BAD_GATEWAY,
                        "invalid_app_response",
                        "APP returned an invalid TUI open response",
                    )
                }
            }
        }
        Some(TuiUnaryRole::Action) => {
            match serde_json::from_value::<AppTuiViewActionResponseV1>(provider.payload) {
                Ok(value) if value.validate().is_ok() => serde_json::to_value(value),
                _ => {
                    return proxy_error(
                        StatusCode::BAD_GATEWAY,
                        "invalid_app_response",
                        "APP returned an invalid TUI action response",
                    )
                }
            }
        }
        None => serde_json::to_value(provider),
    };
    match value.and_then(|value| serde_json::to_vec(&value)) {
        Ok(body) => response_bytes(status, body, UNARY_CONTENT_TYPE_V1),
        Err(error) => proxy_error(
            StatusCode::BAD_GATEWAY,
            "invalid_app_response",
            &error.to_string(),
        ),
    }
}

#[derive(Debug, Clone, Copy)]
enum TuiUnaryRole {
    Open,
    Action,
}

fn stable_tui_action_idempotency(
    app_id: &str,
    view_id: &str,
    payload: &serde_json::Value,
    principal_id: &str,
    tenant_id: &str,
    workspace_root: &Path,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"cowd.gateway.tui-action-idempotency/v1\0");
    digest.update(app_id.as_bytes());
    digest.update(b"\0");
    digest.update(view_id.as_bytes());
    digest.update(b"\0");
    digest.update(principal_id.as_bytes());
    digest.update(b"\0");
    digest.update(tenant_id.as_bytes());
    digest.update(b"\0");
    digest.update(workspace_root.to_string_lossy().as_bytes());
    digest.update(b"\0");
    digest.update(payload.to_string().as_bytes());
    format!("tui-action:sha256:{:x}", digest.finalize())
}

struct ProxyStreamState {
    body: Incoming,
    buffer: BytesMut,
    lease: AppRuntimeLease<GatewayAppConnector>,
    descriptor: OperationDescriptorV1,
    subscription_id: Option<String>,
    expected_sequence: u64,
    deadline_unix_ms: u64,
    tui_sse: bool,
    finished: bool,
}

#[allow(clippy::too_many_arguments)]
async fn proxy_stream(
    platform: &GatewayAppPlatform,
    workspace_root: &Path,
    principal: &super::AuthenticatedPrincipal,
    headers: &HeaderMap,
    app_id: &str,
    operation_id: &str,
    input: InvokeInput,
    worker_path: Option<String>,
    tui_sse: bool,
) -> Response<Body> {
    let (lease, descriptor) =
        match acquire_operation(platform, principal, app_id, operation_id).await {
            Ok(value) => value,
            Err(response) => return response,
        };
    if !descriptor.streaming {
        return proxy_error(
            StatusCode::BAD_REQUEST,
            "unary_required",
            "operation requires invoke route",
        );
    }
    let envelope = match invocation_envelope(
        platform,
        workspace_root,
        principal,
        headers,
        app_id,
        &descriptor,
        input,
    ) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let path = worker_path
        .unwrap_or_else(|| format!("/_cowd/v1/operations/{}/stream", encode(operation_id)));
    let effective_deadline = envelope.effective_deadline_unix_ms();
    let timeout = match deadline_timeout(effective_deadline) {
        Ok(timeout) => timeout,
        Err(response) => return response,
    };
    let response = match send_worker(
        lease.connection(),
        lease.app_id(),
        lease.generation().0.as_str(),
        &envelope.request_id,
        effective_deadline,
        Method::POST,
        &path,
        Some(&envelope),
        timeout,
    )
    .await
    {
        Ok(response) => response,
        Err(response) => return response,
    };
    if !response.status().is_success() {
        let status = response.status();
        return match bounded_body(response.into_body(), descriptor.maximum_response_bytes).await {
            Ok(bytes) => response_bytes(status, bytes, UNARY_CONTENT_TYPE_V1),
            Err(response) => response,
        };
    }
    let stream_state = ProxyStreamState {
        body: response.into_body(),
        buffer: BytesMut::new(),
        lease,
        descriptor,
        subscription_id: None,
        expected_sequence: 0,
        deadline_unix_ms: effective_deadline,
        tui_sse,
        finished: false,
    };
    let stream = futures::stream::unfold(stream_state, |mut state| async move {
        match next_proxy_stream_chunk(&mut state).await {
            Ok(Some(bytes)) => Some((Ok::<Bytes, Infallible>(bytes), state)),
            Ok(None) => None,
            Err(detail) => {
                state.finished = true;
                let payload =
                    serde_json::json!({"error":{"code":"invalid_app_stream","detail":detail}});
                let encoded = serde_json::to_vec(&payload).unwrap_or_else(|_| {
                    br#"{"error":{"code":"invalid_app_stream","detail":"APP stream failed"}}"#
                        .to_vec()
                });
                let bytes = if state.tui_sse {
                    Bytes::from(format!("data: {}\n\n", String::from_utf8_lossy(&encoded)))
                } else {
                    let mut encoded = encoded;
                    encoded.push(b'\n');
                    Bytes::from(encoded)
                };
                Some((Ok(bytes), state))
            }
        }
    });
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(if tui_sse {
            "text/event-stream; charset=utf-8"
        } else {
            STREAM_CONTENT_TYPE_V1
        }),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn next_proxy_stream_chunk(state: &mut ProxyStreamState) -> Result<Option<Bytes>, String> {
    if state.finished {
        return Ok(None);
    }
    loop {
        if let Some(newline) = state.buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = state.buffer.split_to(newline + 1);
            line.truncate(newline);
            if line.last() == Some(&b'\r') {
                line.truncate(line.len() - 1);
            }
            if line.is_empty() {
                continue;
            }
            return validate_and_encode_stream_line(state, &line)
                .await
                .map(Some);
        }
        if state.buffer.len() as u64 > state.descriptor.maximum_frame_bytes {
            return Err("APP stream frame exceeds the signed byte limit".to_owned());
        }
        let now = now_unix_ms()?;
        let remaining = state.deadline_unix_ms.saturating_sub(now);
        if remaining == 0 {
            return Err("APP stream deadline exceeded".to_owned());
        }
        let next = tokio::time::timeout(Duration::from_millis(remaining), state.body.frame())
            .await
            .map_err(|_| "APP stream deadline exceeded".to_owned())?;
        match next {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    state.buffer.extend_from_slice(&data);
                }
            }
            Some(Err(error)) => return Err(format!("APP stream transport failed: {error}")),
            None if state.buffer.is_empty() => {
                if state.subscription_id.is_some() && !state.finished {
                    return Err("APP stream ended without a terminal frame".to_owned());
                }
                return Ok(None);
            }
            None => {
                let line = state.buffer.split().freeze();
                return validate_and_encode_stream_line(state, &line)
                    .await
                    .map(Some);
            }
        }
    }
}

async fn validate_and_encode_stream_line(
    state: &mut ProxyStreamState,
    line: &[u8],
) -> Result<Bytes, String> {
    if line.len() as u64 > state.descriptor.maximum_frame_bytes {
        return Err("APP stream frame exceeds the signed byte limit".to_owned());
    }
    let frame: AppStreamFrameV1 =
        serde_json::from_slice(line).map_err(|_| "APP returned invalid NDJSON".to_owned())?;
    frame.validate().map_err(|error| error.to_string())?;
    if frame.sequence() != state.expected_sequence {
        return Err("APP stream sequence is not contiguous".to_owned());
    }
    match (&state.subscription_id, &frame) {
        (None, AppStreamFrameV1::Open { schema_digest, .. })
            if schema_digest == &state.descriptor.output_schema_digest =>
        {
            state.subscription_id = Some(frame.subscription_id().to_owned());
        }
        (None, _) => return Err("APP stream must begin with a schema-bound open frame".to_owned()),
        (Some(expected), _) if expected != frame.subscription_id() => {
            return Err("APP stream changed subscription identity".to_owned())
        }
        (Some(_), AppStreamFrameV1::Open { .. }) => {
            return Err("APP stream emitted more than one open frame".to_owned())
        }
        _ => {}
    }
    state.expected_sequence = state.expected_sequence.saturating_add(1);
    if let AppStreamFrameV1::Checkpoint {
        subscription_id,
        sequence,
        cursor,
        ..
    } = &frame
    {
        let ack = AppStreamAckV1 {
            schema_version: 1,
            subscription_id: subscription_id.clone(),
            maximum_contiguous_sequence: *sequence,
            cursor: cursor.clone(),
        };
        let path =
            APP_SUBSCRIPTION_ACK_PATH_V1.replace("{subscription_id}", &encode(subscription_id));
        let ack_request_id = format!("stream-ack-{}", uuid::Uuid::new_v4());
        let ack_now = now_unix_ms()?;
        let ack_deadline = ack_now.saturating_add(5_000).min(state.deadline_unix_ms);
        let ack_timeout = Duration::from_millis(ack_deadline.saturating_sub(ack_now));
        if ack_timeout.is_zero() {
            return Err("APP stream acknowledgement deadline exceeded".to_owned());
        }
        let response = send_worker(
            state.lease.connection(),
            state.lease.app_id(),
            state.lease.generation().0.as_str(),
            &ack_request_id,
            ack_deadline,
            Method::POST,
            &path,
            Some(&ack),
            ack_timeout,
        )
        .await
        .map_err(|_| "APP stream acknowledgement failed".to_owned())?;
        if !response.status().is_success() {
            return Err("APP rejected stream acknowledgement".to_owned());
        }
        bounded_body(response.into_body(), 64 * 1024)
            .await
            .map_err(|_| "APP stream acknowledgement response was invalid".to_owned())?;
    }
    if matches!(
        frame,
        AppStreamFrameV1::End { .. } | AppStreamFrameV1::Error { .. }
    ) {
        state.finished = true;
    }
    let encoded = serde_json::to_vec(&frame).map_err(|error| error.to_string())?;
    if state.tui_sse {
        Ok(Bytes::from(format!(
            "data: {}\n\n",
            String::from_utf8_lossy(&encoded)
        )))
    } else {
        let mut encoded = encoded;
        encoded.push(b'\n');
        Ok(Bytes::from(encoded))
    }
}

async fn acquire_app(
    platform: &GatewayAppPlatform,
    principal: &super::AuthenticatedPrincipal,
    app_id: &str,
) -> Result<AppRuntimeLease<GatewayAppConnector>, Response<Body>> {
    let app_id = AppId(app_id.to_owned());
    if principal.0.claims().app_profile(&app_id.0).is_none() {
        return Err(proxy_error(
            StatusCode::FORBIDDEN,
            "app_profile_required",
            "signed APP authorization profile is missing",
        ));
    }
    let admitted = platform
        .catalog()
        .get(&app_id)
        .ok_or_else(|| proxy_error(StatusCode::NOT_FOUND, "app_not_found", "APP is not mounted"))?;
    platform
        .supervisor()
        .acquire(
            &app_id,
            &admitted.generation,
            Duration::from_secs(15),
            &CancellationToken::default(),
        )
        .await
        .map_err(|error| {
            proxy_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "app_activation_failed",
                &error.to_string(),
            )
        })
}

async fn get_receipt(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumPath((app_id, receipt_id)): AxumPath<(String, String)>,
) -> Response<Body> {
    let lease = match acquire_app(&platform, &principal, &app_id).await {
        Ok(lease) => lease,
        Err(response) => return response,
    };
    let path = APP_RECEIPT_PATH_V1.replace("{receipt_id}", &encode(&receipt_id));
    let request_id = format!("receipt-{}", uuid::Uuid::new_v4());
    let deadline = match now_unix_ms() {
        Ok(now) => now.saturating_add(10_000),
        Err(detail) => {
            return proxy_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "clock_unavailable",
                &detail,
            )
        }
    };
    let response = match send_worker::<serde_json::Value>(
        lease.connection(),
        lease.app_id(),
        lease.generation().0.as_str(),
        &request_id,
        deadline,
        Method::GET,
        &path,
        None,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(response) => response,
        Err(response) => return response,
    };
    let status = response.status();
    let bytes = match bounded_body(response.into_body(), 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };
    if status.is_success() {
        let receipt: DurableReceiptV1 = match serde_json::from_slice(&bytes) {
            Ok(receipt) => receipt,
            Err(_) => {
                return proxy_error(
                    StatusCode::BAD_GATEWAY,
                    "invalid_app_response",
                    "APP returned an invalid receipt",
                )
            }
        };
        if receipt.validate().is_err() || receipt.receipt_id != receipt_id {
            return proxy_error(
                StatusCode::BAD_GATEWAY,
                "invalid_app_response",
                "APP receipt route binding mismatch",
            );
        }
    }
    response_bytes(status, bytes, UNARY_CONTENT_TYPE_V1)
}

async fn ack_subscription(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumPath((app_id, subscription_id)): AxumPath<(String, String)>,
    Json(ack): Json<AppStreamAckV1>,
) -> Response<Body> {
    if ack.validate().is_err() || ack.subscription_id != subscription_id {
        return proxy_error(
            StatusCode::BAD_REQUEST,
            "invalid_stream_ack",
            "subscription ACK route binding mismatch",
        );
    }
    let lease = match acquire_app(&platform, &principal, &app_id).await {
        Ok(lease) => lease,
        Err(response) => return response,
    };
    let path = APP_SUBSCRIPTION_ACK_PATH_V1.replace("{subscription_id}", &encode(&subscription_id));
    relay_control(lease, Method::POST, &path, Some(&ack)).await
}

async fn cancel_subscription(
    Extension(platform): Extension<Arc<GatewayAppPlatform>>,
    Extension(principal): Extension<super::AuthenticatedPrincipal>,
    AxumPath((app_id, subscription_id)): AxumPath<(String, String)>,
) -> Response<Body> {
    let lease = match acquire_app(&platform, &principal, &app_id).await {
        Ok(lease) => lease,
        Err(response) => return response,
    };
    let path = APP_SUBSCRIPTION_PATH_V1.replace("{subscription_id}", &encode(&subscription_id));
    relay_control::<serde_json::Value>(lease, Method::DELETE, &path, None).await
}

async fn relay_control<T: Serialize>(
    lease: AppRuntimeLease<GatewayAppConnector>,
    method: Method,
    path: &str,
    payload: Option<&T>,
) -> Response<Body> {
    let request_id = format!("control-{}", uuid::Uuid::new_v4());
    let deadline = match now_unix_ms() {
        Ok(now) => now.saturating_add(10_000),
        Err(detail) => {
            return proxy_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "clock_unavailable",
                &detail,
            )
        }
    };
    let response = match send_worker(
        lease.connection(),
        lease.app_id(),
        lease.generation().0.as_str(),
        &request_id,
        deadline,
        method,
        path,
        payload,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(response) => response,
        Err(response) => return response,
    };
    let status = response.status();
    match bounded_body(response.into_body(), 1024 * 1024).await {
        Ok(bytes) => response_bytes(status, bytes, UNARY_CONTENT_TYPE_V1),
        Err(response) => response,
    }
}

async fn acquire_operation(
    platform: &GatewayAppPlatform,
    principal: &super::AuthenticatedPrincipal,
    app_id: &str,
    operation_id: &str,
) -> Result<(AppRuntimeLease<GatewayAppConnector>, OperationDescriptorV1), Response<Body>> {
    let app_id = AppId(app_id.to_owned());
    if principal.0.claims().app_profile(&app_id.0).is_none() {
        return Err(proxy_error(
            StatusCode::FORBIDDEN,
            "app_profile_required",
            "signed APP authorization profile is missing",
        ));
    }
    let admitted = platform
        .catalog()
        .get(&app_id)
        .ok_or_else(|| proxy_error(StatusCode::NOT_FOUND, "app_not_found", "APP is not mounted"))?;
    let cancellation = CancellationToken::default();
    let lease = platform
        .supervisor()
        .acquire(
            &app_id,
            &admitted.generation,
            Duration::from_secs(15),
            &cancellation,
        )
        .await
        .map_err(|error| {
            proxy_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "app_activation_failed",
                &error.to_string(),
            )
        })?;
    let descriptor = lease
        .connection()
        .operations()
        .iter()
        .find(|descriptor| descriptor.operation_id == operation_id)
        .cloned()
        .ok_or_else(|| {
            proxy_error(
                StatusCode::NOT_FOUND,
                "operation_not_found",
                "operation is not present in the admitted handshake catalog",
            )
        })?;
    if !operation_id.starts_with(&format!("{}.", app_id.0)) || descriptor.validate().is_err() {
        return Err(proxy_error(
            StatusCode::BAD_GATEWAY,
            "app_contract_invalid",
            "operation descriptor is outside the admitted APP namespace",
        ));
    }
    Ok((lease, descriptor))
}

// Callers return the terminal Axum response immediately, so boxing this local
// error would add allocation without reducing any long-lived state.
#[allow(clippy::result_large_err)]
fn invocation_envelope(
    platform: &GatewayAppPlatform,
    workspace_root: &Path,
    principal: &super::AuthenticatedPrincipal,
    headers: &HeaderMap,
    app_id: &str,
    descriptor: &OperationDescriptorV1,
    input: InvokeInput,
) -> Result<AppInvocationEnvelopeV1, Response<Body>> {
    if !descriptor.operation_id.starts_with(&format!("{app_id}.")) {
        return Err(proxy_error(
            StatusCode::BAD_GATEWAY,
            "app_contract_invalid",
            "operation namespace is invalid",
        ));
    }
    let claims = principal.0.claims();
    let profile_id = claims.app_profile(app_id).ok_or_else(|| {
        proxy_error(
            StatusCode::FORBIDDEN,
            "app_profile_required",
            "signed APP authorization profile is missing",
        )
    })?;
    let admitted = platform
        .catalog()
        .get(&AppId(app_id.to_owned()))
        .ok_or_else(|| proxy_error(StatusCode::NOT_FOUND, "app_not_found", "APP is not mounted"))?;
    let profile = admitted
        .manifest
        .authorization_profiles
        .iter()
        .find(|profile| profile.profile_id == profile_id)
        .ok_or_else(|| {
            proxy_error(
                StatusCode::FORBIDDEN,
                "app_profile_invalid",
                "signed APP profile is not admitted",
            )
        })?;
    let mut granted_capabilities = profile
        .capabilities
        .iter()
        .filter(|capability| claims.capabilities.binary_search(capability).is_ok())
        .cloned()
        .collect::<Vec<_>>();
    granted_capabilities.sort();
    granted_capabilities.dedup();

    let now = now_unix_ms().map_err(|detail| {
        proxy_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "clock_unavailable",
            &detail,
        )
    })?;
    let deadline_ms = input.deadline_ms.unwrap_or(descriptor.default_deadline_ms);
    if deadline_ms == 0 || deadline_ms > descriptor.maximum_deadline_ms {
        return Err(proxy_error(
            StatusCode::BAD_REQUEST,
            "invalid_deadline",
            "deadline exceeds the signed operation limit",
        ));
    }
    let surface = match headers.get("x-cowd-surface-id") {
        Some(value) => value.to_str().map_err(|_| {
            proxy_error(
                StatusCode::BAD_REQUEST,
                "invalid_surface",
                "surface identity must be valid UTF-8",
            )
        })?,
        None => "gateway",
    }
    .trim();
    if surface.is_empty()
        || surface.len() > 128
        || !surface
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'))
    {
        return Err(proxy_error(
            StatusCode::BAD_REQUEST,
            "invalid_surface",
            "surface identity is invalid",
        ));
    }
    let workspace_id = format!(
        "sha256:{:x}",
        Sha256::digest(workspace_root.to_string_lossy().as_bytes())
    );
    let request_id = uuid::Uuid::new_v4().to_string();
    let envelope = AppInvocationEnvelopeV1 {
        schema_version: 1,
        operation_id: descriptor.operation_id.clone(),
        request_id: request_id.clone(),
        correlation_id: request_id,
        causation_id: None,
        deadline_unix_ms: now.saturating_add(deadline_ms),
        idempotency_key: input.idempotency_key,
        expected_revision: input.expected_revision,
        call_chain: vec![format!("surface:{surface}"), "core:gateway".to_owned()],
        max_hops: 4,
        input_schema_digest: descriptor.input_schema_digest.clone(),
        principal: PrincipalContextV1 {
            subject: claims.principal_id.clone(),
            tenant_id: claims.tenant_id.clone(),
            workspace_id,
            delegation: if claims.kind == PrincipalKind::Human {
                DelegationKindV1::User
            } else {
                DelegationKindV1::Service
            },
            grant_id: claims.grant_id.clone(),
            authorization_profile_id: profile_id.to_owned(),
            authorization_revision: claims.profile_revision,
            granted_capabilities,
            granted_scopes: claims.scopes.clone(),
            credential_epoch: claims.credential_epoch,
            expires_at_unix_ms: claims.expires_at_ms,
        },
        execution: ExecutionContextV1 {
            surface: surface.to_owned(),
            session_id: None,
            turn_id: None,
            task_id: None,
        },
        payload: input.payload,
    };
    envelope.validate_at(now, descriptor).map_err(|error| {
        proxy_error(
            StatusCode::FORBIDDEN,
            "operation_not_granted",
            &error.to_string(),
        )
    })?;
    let size = serde_json::to_vec(&envelope)
        .map_err(|_| {
            proxy_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "invocation cannot be encoded",
            )
        })?
        .len() as u64;
    if size > descriptor.maximum_request_bytes {
        return Err(proxy_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "invocation exceeds the signed operation byte limit",
        ));
    }
    Ok(envelope)
}

#[allow(clippy::too_many_arguments)]
async fn send_worker<T: Serialize>(
    connection: &GatewayAppConnection,
    app_id: &AppId,
    generation: &str,
    request_id: &str,
    deadline_unix_ms: u64,
    method: Method,
    path: &str,
    payload: Option<&T>,
    timeout: Duration,
) -> Result<Response<Incoming>, Response<Body>> {
    let bytes = match payload {
        Some(payload) => serde_json::to_vec(payload).map_err(|_| {
            proxy_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "request cannot be encoded",
            )
        })?,
        None => Vec::new(),
    };
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(HEADER_AUTHORIZATION_V1, connection.authorization())
        .header(HEADER_CONTENT_TYPE_V1, UNARY_CONTENT_TYPE_V1)
        .header(HEADER_PROTOCOL_VERSION_V1, PROTOCOL_REVISION_V1)
        .header(HEADER_APP_ID_V1, &app_id.0)
        .header(HEADER_APP_GENERATION_V1, generation)
        .header(HEADER_REQUEST_ID_V1, request_id)
        .header(HEADER_CORRELATION_ID_V1, request_id)
        .header(HEADER_DEADLINE_UNIX_MS_V1, deadline_unix_ms)
        .body(Full::new(Bytes::from(bytes)))
        .map_err(|_| {
            proxy_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "proxy_request_invalid",
                "worker request cannot be built",
            )
        })?;
    let cancellation = CancellationToken::default();
    connection
        .send(generation, request, timeout, &cancellation)
        .await
        .map_err(|error| {
            proxy_error(
                StatusCode::BAD_GATEWAY,
                "app_transport_failed",
                &error.to_string(),
            )
        })
}

async fn bounded_body(mut body: Incoming, maximum_bytes: u64) -> Result<Vec<u8>, Response<Body>> {
    let maximum = usize::try_from(maximum_bytes).unwrap_or(usize::MAX);
    let mut bytes = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            proxy_error(
                StatusCode::BAD_GATEWAY,
                "app_transport_failed",
                &error.to_string(),
            )
        })?;
        if let Ok(data) = frame.into_data() {
            if bytes.len().saturating_add(data.len()) > maximum {
                return Err(proxy_error(
                    StatusCode::BAD_GATEWAY,
                    "app_response_too_large",
                    "APP response exceeds the signed byte limit",
                ));
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes.to_vec())
}

fn now_unix_ms() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .map_err(|error| error.to_string())
}

#[allow(clippy::result_large_err)]
fn deadline_timeout(deadline_unix_ms: u64) -> Result<Duration, Response<Body>> {
    let now = now_unix_ms().map_err(|detail| {
        proxy_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "clock_unavailable",
            &detail,
        )
    })?;
    let remaining = deadline_unix_ms.saturating_sub(now);
    if remaining == 0 {
        return Err(proxy_error(
            StatusCode::REQUEST_TIMEOUT,
            "deadline_exceeded",
            "operation deadline has expired",
        ));
    }
    Ok(Duration::from_millis(remaining))
}

fn encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn platform_error(
    error: cowd_app_host::supervisor::SupervisorError,
) -> (StatusCode, Json<serde_json::Value>) {
    typed_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "app_activation_failed",
        &error.to_string(),
    )
}

fn proxy_error(status: StatusCode, code: &str, detail: &str) -> Response<Body> {
    let bytes = serde_json::to_vec(&serde_json::json!({"error":{"code":code,"detail":detail}}))
        .unwrap_or_else(|_| {
            br#"{"error":{"code":"gateway_error","detail":"response encoding failed"}}"#.to_vec()
        });
    response_bytes(status, bytes, UNARY_CONTENT_TYPE_V1)
}

fn response_bytes(
    status: StatusCode,
    bytes: Vec<u8>,
    content_type: &'static str,
) -> Response<Body> {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
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
    apply_static_security_headers(headers);
    response
}

fn apply_static_security_headers(headers: &mut HeaderMap) {
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(APP_STATIC_CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.remove("x-frame-options");
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

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use cowd_app_host::catalog::{
        AppCatalogBuilder, AppCatalogPolicy, AppTrustStore, TrustedSigningKey,
    };
    use cowd_app_protocol::{AppErrorCodeV1, AppErrorResponseV1};
    use harness_contract::security::{PrincipalAssurance, PrincipalClaims};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;
    use tower::ServiceExt;

    #[test]
    fn app_static_headers_allow_only_same_origin_embedding() {
        let mut headers = HeaderMap::new();
        headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
        apply_static_security_headers(&mut headers);
        let csp = headers["content-security-policy"]
            .to_str()
            .expect("static CSP");
        assert!(csp.contains("frame-ancestors 'self'"));
        assert!(csp.contains("script-src 'self'"));
        assert!(csp.contains("style-src 'self' 'unsafe-inline'"));
        assert!(csp.contains("connect-src 'none'"));
        assert!(!csp.contains("frame-ancestors *"));
        assert!(!csp.contains("frame-ancestors 'none'"));
        assert!(csp.contains("object-src 'none'"));
        assert!(csp.contains("base-uri 'none'"));
        assert!(!headers.contains_key("x-frame-options"));
        assert_eq!(headers["x-content-type-options"], "nosniff");
    }

    #[test]
    fn chromium_loads_same_origin_app_iframe() {
        let chromium = [
            "/snap/bin/chromium",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
        ]
        .into_iter()
        .find(|path| Path::new(path).is_file());
        let Some(chromium) = chromium else {
            eprintln!("skipping real iframe regression: Chromium is unavailable");
            return;
        };
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind iframe regression server");
        listener
            .set_nonblocking(true)
            .expect("nonblocking iframe regression server");
        let address = listener.local_addr().expect("iframe server address");
        let stopped = Arc::new(AtomicBool::new(false));
        let server_stopped = Arc::clone(&stopped);
        let server = std::thread::spawn(move || {
            while !server_stopped.load(Ordering::Acquire) {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("iframe request timeout");
                let mut request = [0_u8; 4096];
                let read = match stream.read(&mut request) {
                    Ok(0) => continue,
                    Ok(read) => read,
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::TimedOut
                                | std::io::ErrorKind::WouldBlock
                        ) =>
                    {
                        continue;
                    }
                    Err(error) => panic!("read iframe request: {error}"),
                };
                let request = String::from_utf8_lossy(&request[..read]);
                let child = request.starts_with("GET /child ");
                let body = if child {
                    "<!doctype html><body data-loaded=\"true\">child-loaded</body>"
                } else {
                    "<!doctype html><body><iframe id=\"app\" src=\"/child\"></iframe><output id=\"result\">pending</output><script>const app=document.getElementById('app');app.onload=()=>{document.getElementById('result').textContent=app.contentDocument.body.dataset.loaded==='true'?'same-origin-frame-loaded':'blocked'}</script></body>"
                };
                let csp = if child {
                    format!("Content-Security-Policy: {APP_STATIC_CONTENT_SECURITY_POLICY}\r\n")
                } else {
                    String::new()
                };
                if let Err(error) = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n{csp}X-Content-Type-Options: nosniff\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ) {
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::BrokenPipe
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::ConnectionReset
                    ) {
                        continue;
                    }
                    panic!("write iframe response: {error}");
                }
            }
        });
        let output = std::process::Command::new(chromium)
            .args([
                "--headless",
                "--no-sandbox",
                "--disable-gpu",
                "--virtual-time-budget=1500",
                "--dump-dom",
                &format!("http://{address}/"),
            ])
            .output()
            .expect("run Chromium iframe regression");
        stopped.store(true, Ordering::Release);
        server.join().expect("join iframe regression server");
        assert!(
            output.status.success(),
            "Chromium failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let rendered = String::from_utf8(output.stdout).expect("Chromium UTF-8 DOM");
        assert!(
            rendered.contains(">same-origin-frame-loaded<"),
            "same-origin APP iframe did not load: {rendered}"
        );
    }

    #[test]
    fn generic_invoke_input_rejects_gateway_owned_identity_fields() {
        for forged in [
            "principal",
            "delegation",
            "tenant_id",
            "workspace_id",
            "session_id",
            "turn_id",
            "task_id",
            "call_chain",
            "input_schema_digest",
        ] {
            let mut request = serde_json::Map::from_iter([("payload".to_owned(), json!({}))]);
            request.insert(forged.to_owned(), json!("forged"));
            assert!(serde_json::from_value::<InvokeInput>(request.into()).is_err());
        }
    }

    #[test]
    fn tui_action_idempotency_is_stable_and_content_bound() {
        let first = json!({
            "schema_version": 1,
            "app_id": "reference-app",
            "view_id": "reference.main",
            "document_revision": "7",
            "component_id": "root",
            "action_id": "refresh",
            "selection": null,
            "form": null,
            "confirmed": true
        });
        let second = first.clone();
        let changed = json!({
            "schema_version": 1,
            "app_id": "reference-app",
            "view_id": "reference.main",
            "document_revision": "8",
            "component_id": "root",
            "action_id": "refresh",
            "selection": null,
            "form": null,
            "confirmed": true
        });
        assert_eq!(
            stable_tui_action_idempotency(
                "reference-app",
                "reference.main",
                &first,
                "principal-a",
                "tenant-a",
                Path::new("/workspace-a"),
            ),
            stable_tui_action_idempotency(
                "reference-app",
                "reference.main",
                &second,
                "principal-a",
                "tenant-a",
                Path::new("/workspace-a"),
            )
        );
        assert_ne!(
            stable_tui_action_idempotency(
                "reference-app",
                "reference.main",
                &first,
                "principal-a",
                "tenant-a",
                Path::new("/workspace-a"),
            ),
            stable_tui_action_idempotency(
                "reference-app",
                "reference.main",
                &changed,
                "principal-a",
                "tenant-a",
                Path::new("/workspace-a"),
            )
        );
        assert_ne!(
            stable_tui_action_idempotency(
                "reference-app",
                "reference.main",
                &first,
                "principal-a",
                "tenant-a",
                Path::new("/workspace-a"),
            ),
            stable_tui_action_idempotency(
                "reference-app",
                "reference.main",
                &first,
                "principal-b",
                "tenant-a",
                Path::new("/workspace-a"),
            )
        );
    }

    #[test]
    fn worker_path_segments_are_percent_encoded() {
        assert_eq!(encode("reference.main"), "reference.main");
        assert_eq!(encode("../other?x=1"), "..%2Fother%3Fx%3D1");
    }

    #[tokio::test]
    #[ignore = "run via scripts/test/reference-app.sh"]
    async fn reference_bundle_gateway_proxy_e2e() {
        let bundle = std::env::var("COWD_REFERENCE_APP_BUNDLE")
            .expect("COWD_REFERENCE_APP_BUNDLE must name a packaged reference Bundle");
        let public_key = std::env::var("COWD_REFERENCE_APP_PUBLIC_KEY_BASE64URL")
            .expect("reference Bundle public key");
        let public_key = URL_SAFE_NO_PAD
            .decode(public_key)
            .expect("reference Bundle public key encoding");
        let bundle = PathBuf::from(bundle);
        let apps_root = bundle
            .parent()
            .expect("reference Bundle parent")
            .to_path_buf();
        let now = now_unix_ms().expect("trusted test clock");
        let discovered = AppCatalogBuilder::new(
            vec![apps_root],
            AppCatalogPolicy::default(),
            AppTrustStore::new([TrustedSigningKey {
                key_id: "reference-app-fixture-ed25519-v1".to_owned(),
                public_key,
                revoked: false,
            }]),
            unsafe { libc::geteuid() },
            now,
        )
        .build()
        .expect("admitted reference Bundle");
        let admitted = discovered
            .get(&AppId("reference-app".to_owned()))
            .expect("reference APP admitted");
        let profile = admitted
            .manifest
            .authorization_profiles
            .iter()
            .find(|profile| profile.is_default)
            .expect("reference default profile");
        let auth_catalog =
            auth_broker::AuthorizationCatalog::from_app_manifests([&admitted.manifest])
                .expect("reference authorization catalog");
        for surface_id in ["webui", "tui"] {
            let projected = auth_catalog.surface_capabilities(surface_id);
            for capability in &profile.capabilities {
                assert!(
                    projected.contains(capability),
                    "{surface_id} did not project signed capability {capability}"
                );
            }
        }
        let unknown_surface = auth_catalog.surface_capabilities("unknown-surface");
        assert!(
            profile
                .capabilities
                .iter()
                .all(|capability| !unknown_surface.contains(capability)),
            "APP capabilities must be projected only to signed surface identifiers"
        );
        let admitted_generation = admitted.generation.clone();
        let mut effective_capabilities = profile.capabilities.clone();
        effective_capabilities.extend([
            "runtime.maintenance.manage".to_owned(),
            "runtime.task.read".to_owned(),
        ]);
        effective_capabilities.sort();
        effective_capabilities.dedup();
        let principal = super::super::AuthenticatedPrincipal(
            runtime::VerifiedPrincipal::from_test_claims(PrincipalClaims {
                principal_id: "reference-e2e".to_owned(),
                tenant_id: "tenant-reference".to_owned(),
                grant_id: "grant-reference".to_owned(),
                kind: PrincipalKind::Human,
                scopes: Vec::new(),
                capabilities: effective_capabilities,
                assurance: PrincipalAssurance::HumanInteractive,
                issuer: "cowd.gateway".to_owned(),
                issued_at_ms: now,
                expires_at_ms: Some(now + 120_000),
                credential_fingerprint: "reference-e2e".to_owned(),
                credential_epoch: 1,
                profile_revision: 1,
                app_profiles: BTreeMap::from([(
                    "reference-app".to_owned(),
                    profile.profile_id.clone(),
                )]),
            }),
        );
        let root = tempfile::tempdir().expect("reference Gateway runtime root");
        let platform = GatewayAppPlatform::for_test_direct_catalog(
            discovered,
            root.path().join("runtime"),
            root.path().join("data"),
            root.path().join("core-bridge.sock"),
        );

        let app_id = AppId("reference-app".to_owned());
        assert_eq!(
            platform
                .supervisor()
                .status(&app_id)
                .await
                .expect("mounted status")
                .state,
            cowd_app_protocol::AppLifecycleStateV1::Mounted
        );
        let catalog = project_catalog(&platform, &principal)
            .await
            .expect("static catalog projection");
        assert_eq!(catalog.apps.len(), 1);
        assert_eq!(
            platform
                .supervisor()
                .status(&app_id)
                .await
                .expect("catalog status")
                .state,
            cowd_app_protocol::AppLifecycleStateV1::Mounted
        );
        let static_response = serve(&platform, "reference-app", "index.html").await;
        assert_eq!(static_response.status(), StatusCode::OK);
        assert_eq!(
            platform
                .supervisor()
                .status(&app_id)
                .await
                .expect("static status")
                .state,
            cowd_app_protocol::AppLifecycleStateV1::Mounted
        );
        let state = super::super::tests::test_state_with_app_platform(Arc::clone(&platform));
        let mut state = Arc::try_unwrap(state)
            .unwrap_or_else(|_| panic!("fresh reference state must be unique"));
        state.auth_token = Some("reference-browser-secret".to_owned());
        let gateway = super::super::api_router(Arc::new(state));
        let static_http_response = gateway
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/apps/reference-app/index.html")
                    .body(Body::empty())
                    .expect("static APP request"),
            )
            .await
            .expect("static APP response");
        assert_eq!(static_http_response.status(), StatusCode::OK);
        let protected_catalog_response = gateway
            .oneshot(
                Request::builder()
                    .uri("/api/apps")
                    .body(Body::empty())
                    .expect("protected APP catalog request"),
            )
            .await
            .expect("protected APP catalog response");
        assert_eq!(
            protected_catalog_response.status(),
            StatusCode::UNAUTHORIZED
        );
        let unauthorized = super::super::AuthenticatedPrincipal(
            runtime::VerifiedPrincipal::from_test_claims(PrincipalClaims {
                principal_id: "reference-unauthorized".to_owned(),
                tenant_id: "tenant-reference".to_owned(),
                grant_id: "grant-reference-unauthorized".to_owned(),
                kind: PrincipalKind::Human,
                scopes: Vec::new(),
                capabilities: Vec::new(),
                assurance: PrincipalAssurance::HumanInteractive,
                issuer: "cowd.gateway".to_owned(),
                issued_at_ms: now,
                expires_at_ms: Some(now + 120_000),
                credential_fingerprint: "reference-unauthorized".to_owned(),
                credential_epoch: 1,
                profile_revision: 1,
                app_profiles: BTreeMap::new(),
            }),
        );
        let unauthorized_invoke = proxy_unary(
            &platform,
            root.path(),
            &unauthorized,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.echo",
            InvokeInput {
                payload: json!({"message":"must not activate"}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            None,
        )
        .await;
        assert_eq!(unauthorized_invoke.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            platform
                .supervisor()
                .status(&app_id)
                .await
                .expect("unauthorized status")
                .state,
            cowd_app_protocol::AppLifecycleStateV1::Mounted
        );
        let detail_result = get_app(
            Extension(Arc::clone(&platform)),
            Extension(principal.clone()),
            AxumPath("reference-app".to_owned()),
        )
        .await;
        let detail = match detail_result {
            Ok(detail) => detail.0,
            Err(error) => {
                let status = platform.supervisor().status(&app_id).await;
                let logs = platform.supervisor().logs(&app_id).await;
                panic!(
                    "reference APP detail failed: {error:?}; runtime status: {status:?}; logs: {logs:?}"
                )
            }
        };
        assert_eq!(detail.manifest.app_id, app_id);
        assert!(!detail.operations.is_empty());

        let workspace = root.path().join("workspace");
        let query = proxy_unary(
            &platform,
            &workspace,
            &principal,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.echo",
            InvokeInput {
                payload: json!({"message":"through-gateway"}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            None,
        )
        .await;
        assert_eq!(query.status(), StatusCode::OK);
        let query: AppProviderResponseV1 = decode_response(query).await;
        assert_eq!(query.payload["echo"]["message"], "through-gateway");

        let malformed_business_payload = proxy_unary(
            &platform,
            &workspace,
            &principal,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.echo",
            InvokeInput {
                payload: json!({"message":7,"unsigned_extra":true}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            None,
        )
        .await;
        assert_eq!(malformed_business_payload.status(), StatusCode::BAD_REQUEST);
        let malformed: AppErrorResponseV1 = decode_response(malformed_business_payload).await;
        assert_eq!(malformed.error.code, AppErrorCodeV1::InvalidRequest);

        let invalid_deadline = proxy_unary(
            &platform,
            &workspace,
            &principal,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.echo",
            InvokeInput {
                payload: json!({}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: Some(30_001),
            },
            None,
            None,
        )
        .await;
        assert_eq!(invalid_deadline.status(), StatusCode::BAD_REQUEST);
        let oversized = proxy_unary(
            &platform,
            &workspace,
            &principal,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.echo",
            InvokeInput {
                payload: json!({"value":"x".repeat(70 * 1024)}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            None,
        )
        .await;
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let invalid_query_idempotency = proxy_unary(
            &platform,
            &workspace,
            &principal,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.echo",
            InvokeInput {
                payload: json!({}),
                idempotency_key: Some("not-valid-for-query".to_owned()),
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            None,
        )
        .await;
        assert_eq!(invalid_query_idempotency.status(), StatusCode::FORBIDDEN);

        let command = proxy_unary(
            &platform,
            &workspace,
            &principal,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.counter.increment",
            InvokeInput {
                payload: json!({"delta":1}),
                idempotency_key: Some("reference-gateway-increment-1".to_owned()),
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            None,
        )
        .await;
        assert_eq!(command.status(), StatusCode::OK);
        let receipt: DurableReceiptV1 = decode_response(command).await;
        let missing_command_idempotency = proxy_unary(
            &platform,
            &workspace,
            &principal,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.counter.increment",
            InvokeInput {
                payload: json!({"delta":1}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            None,
        )
        .await;
        assert_eq!(missing_command_idempotency.status(), StatusCode::FORBIDDEN);
        let receipt_response = get_receipt(
            Extension(Arc::clone(&platform)),
            Extension(principal.clone()),
            AxumPath(("reference-app".to_owned(), receipt.receipt_id.clone())),
        )
        .await;
        assert_eq!(receipt_response.status(), StatusCode::OK);

        let stream = proxy_stream(
            &platform,
            &workspace,
            &principal,
            &HeaderMap::new(),
            "reference-app",
            "reference-app.events",
            InvokeInput {
                payload: json!({}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            false,
        )
        .await;
        assert_eq!(stream.status(), StatusCode::OK);
        let bytes = stream
            .into_body()
            .collect()
            .await
            .expect("Gateway stream body")
            .to_bytes();
        let frames = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<AppStreamFrameV1>(line).expect("typed frame"))
            .collect::<Vec<_>>();
        assert!(matches!(
            frames.first(),
            Some(AppStreamFrameV1::Open { .. })
        ));
        assert!(matches!(frames.last(), Some(AppStreamFrameV1::End { .. })));
        let subscription_id = frames[0].subscription_id().to_owned();
        let ack = ack_subscription(
            Extension(Arc::clone(&platform)),
            Extension(principal.clone()),
            AxumPath(("reference-app".to_owned(), subscription_id.clone())),
            Json(AppStreamAckV1 {
                schema_version: 1,
                subscription_id: subscription_id.clone(),
                maximum_contiguous_sequence: frames.last().expect("terminal frame").sequence(),
                cursor: "cursor-3".to_owned(),
            }),
        )
        .await;
        assert_eq!(ack.status(), StatusCode::NO_CONTENT);
        let cancel = cancel_subscription(
            Extension(Arc::clone(&platform)),
            Extension(principal.clone()),
            AxumPath(("reference-app".to_owned(), subscription_id)),
        )
        .await;
        assert_eq!(cancel.status(), StatusCode::NO_CONTENT);

        let logs = get_app_logs(
            Extension(Arc::clone(&platform)),
            Extension(principal.clone()),
            AxumPath("reference-app".to_owned()),
        )
        .await
        .expect("reference APP logs")
        .0;
        assert_eq!(logs.app_id, app_id);
        assert_eq!(logs.generation, admitted_generation);
        let denied_logs = get_app_logs(
            Extension(Arc::clone(&platform)),
            Extension(unauthorized.clone()),
            AxumPath("reference-app".to_owned()),
        )
        .await
        .expect_err("unsigned management access denied");
        assert_eq!(denied_logs.0, StatusCode::FORBIDDEN);
        let first_restart = restart_app(
            Extension(Arc::clone(&platform)),
            Extension(principal.clone()),
            AxumPath("reference-app".to_owned()),
        )
        .await;
        let restarted = match first_restart {
            Ok(response) => response.0,
            Err((StatusCode::SERVICE_UNAVAILABLE, _)) => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                restart_app(
                    Extension(Arc::clone(&platform)),
                    Extension(principal.clone()),
                    AxumPath("reference-app".to_owned()),
                )
                .await
                .expect("reference APP restart after bounded backoff")
                .0
            }
            Err(error) => panic!("reference APP restart: {error:?}"),
        };
        assert_eq!(restarted.app_id, app_id);
        assert_eq!(
            restarted.lifecycle.state,
            cowd_app_protocol::AppLifecycleStateV1::Idle
        );

        let rejected = tui_action(
            Extension(Arc::clone(&platform)),
            Extension(principal),
            AxumState(Arc::new(super::super::AppState {
                workspace_root: workspace,
                ..Arc::try_unwrap(super::super::tests::test_state())
                    .unwrap_or_else(|_| panic!("unique test state"))
            })),
            HeaderMap::new(),
            AxumPath(("reference-app".to_owned(), "reference.main".to_owned())),
            Json(AppActionV1 {
                schema_version: 1,
                app_id: AppId("reference-app".to_owned()),
                view_id: "reference.main".to_owned(),
                document_revision: "1".to_owned(),
                component_id: "root".to_owned(),
                action_id: "unsupported".to_owned(),
                selection: serde_json::Value::Null,
                form: serde_json::Value::Null,
                confirmed: true,
            }),
        )
        .await;
        assert!(!rejected.status().is_success());
        platform.shutdown().await.expect("reference APP shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "run via scripts/test/reference-app-performance.sh"]
    async fn reference_bundle_performance_contract() {
        const COLD_SAMPLES: usize = 100;
        const HOT_ROUNDS: usize = 6;
        const HOT_SAMPLES_PER_ROUND: usize = 250;
        const HOT_SAMPLES: usize = HOT_ROUNDS * HOT_SAMPLES_PER_ROUND;

        let bundle = PathBuf::from(
            std::env::var("COWD_REFERENCE_APP_BUNDLE")
                .expect("COWD_REFERENCE_APP_BUNDLE must name a packaged reference Bundle"),
        );
        let public_key = URL_SAFE_NO_PAD
            .decode(
                std::env::var("COWD_REFERENCE_APP_PUBLIC_KEY_BASE64URL")
                    .expect("reference Bundle public key"),
            )
            .expect("reference Bundle public key encoding");
        let now = now_unix_ms().expect("trusted test clock");
        let discovered = AppCatalogBuilder::new(
            vec![bundle.parent().expect("Bundle parent").to_path_buf()],
            AppCatalogPolicy::default(),
            AppTrustStore::new([TrustedSigningKey {
                key_id: "reference-app-fixture-ed25519-v1".to_owned(),
                public_key,
                revoked: false,
            }]),
            unsafe { libc::geteuid() },
            now,
        )
        .build()
        .expect("admitted reference Bundle");
        let manifest = &discovered
            .get(&AppId("reference-app".to_owned()))
            .expect("reference APP admitted")
            .manifest;
        let profile = manifest
            .authorization_profiles
            .iter()
            .find(|profile| profile.is_default)
            .expect("reference default profile");
        let mut capabilities = profile.capabilities.clone();
        capabilities.extend([
            "runtime.maintenance.manage".to_owned(),
            "runtime.task.read".to_owned(),
        ]);
        capabilities.sort();
        capabilities.dedup();
        let principal = super::super::AuthenticatedPrincipal(
            runtime::VerifiedPrincipal::from_test_claims(PrincipalClaims {
                principal_id: "reference-performance".to_owned(),
                tenant_id: "tenant-reference".to_owned(),
                grant_id: "grant-reference-performance".to_owned(),
                kind: PrincipalKind::Human,
                scopes: Vec::new(),
                capabilities,
                assurance: PrincipalAssurance::HumanInteractive,
                issuer: "cowd.gateway".to_owned(),
                issued_at_ms: now,
                expires_at_ms: Some(now + 300_000),
                credential_fingerprint: "reference-performance".to_owned(),
                credential_epoch: 1,
                profile_revision: 1,
                app_profiles: BTreeMap::from([(
                    "reference-app".to_owned(),
                    profile.profile_id.clone(),
                )]),
            }),
        );
        let root = tempfile::tempdir().expect("reference performance root");
        let workspace = root.path().join("workspace");
        let app_id = AppId("reference-app".to_owned());
        let headers = HeaderMap::new();

        let mut cold_us = Vec::with_capacity(COLD_SAMPLES);
        for index in 0..COLD_SAMPLES {
            let case_root = root.path().join(format!("cold-{index:03}"));
            let platform = GatewayAppPlatform::for_test_direct_catalog(
                discovered.clone(),
                case_root.join("runtime"),
                case_root.join("data"),
                case_root.join("core-bridge.sock"),
            );
            let started = Instant::now();
            let response = proxy_unary(
                &platform,
                &workspace,
                &principal,
                &headers,
                "reference-app",
                "reference-app.echo",
                echo_input(),
                None,
                None,
            )
            .await;
            let status = response.status();
            response
                .into_body()
                .collect()
                .await
                .expect("cold response body");
            cold_us.push(started.elapsed().as_micros());
            assert_eq!(status, StatusCode::OK);
            platform.shutdown().await.expect("cold platform shutdown");
        }
        cold_us.sort_unstable();
        let cold_p95_us = nearest_rank_u128(&cold_us, 95);
        let cold_p99_us = nearest_rank_u128(&cold_us, 99);
        assert!(cold_p95_us <= 1_000_000, "reference cold p95 exceeded 1s");
        assert!(cold_p99_us <= 2_000_000, "reference cold p99 exceeded 2s");

        let platform = GatewayAppPlatform::for_test_direct_catalog(
            discovered,
            root.path().join("hot-runtime"),
            root.path().join("hot-data"),
            root.path().join("hot-core-bridge.sock"),
        );
        let warm = proxy_unary(
            &platform,
            &workspace,
            &principal,
            &headers,
            "reference-app",
            "reference-app.echo",
            echo_input(),
            None,
            None,
        )
        .await;
        assert_eq!(warm.status(), StatusCode::OK);
        warm.into_body().collect().await.expect("warm response");
        let (lease, descriptor) =
            acquire_operation(&platform, &principal, "reference-app", "reference-app.echo")
                .await
                .expect("hot direct lease");
        let worker_pid = platform
            .supervisor()
            .status(&app_id)
            .await
            .expect("hot worker status")
            .pid
            .expect("hot worker pid");

        let mut direct_us = Vec::with_capacity(HOT_SAMPLES);
        let mut gateway_us = Vec::with_capacity(HOT_SAMPLES);
        let mut direct_wall = Duration::ZERO;
        let mut gateway_wall = Duration::ZERO;
        let mut direct_cpu_ticks = 0_u64;
        let mut gateway_cpu_ticks = 0_u64;
        for round in 0..HOT_ROUNDS {
            if round % 2 == 0 {
                let cpu_before = process_and_worker_cpu_ticks(worker_pid);
                let (wall, latencies) = measure_direct_hot_samples(
                    &platform,
                    &workspace,
                    &principal,
                    &headers,
                    &lease,
                    &descriptor,
                    HOT_SAMPLES_PER_ROUND,
                )
                .await;
                direct_wall += wall;
                direct_us.extend(latencies);
                direct_cpu_ticks = direct_cpu_ticks.saturating_add(
                    process_and_worker_cpu_ticks(worker_pid).saturating_sub(cpu_before),
                );

                let cpu_before = process_and_worker_cpu_ticks(worker_pid);
                let (wall, latencies) = measure_gateway_hot_samples(
                    &platform,
                    &workspace,
                    &principal,
                    &headers,
                    HOT_SAMPLES_PER_ROUND,
                )
                .await;
                gateway_wall += wall;
                gateway_us.extend(latencies);
                gateway_cpu_ticks = gateway_cpu_ticks.saturating_add(
                    process_and_worker_cpu_ticks(worker_pid).saturating_sub(cpu_before),
                );
            } else {
                let cpu_before = process_and_worker_cpu_ticks(worker_pid);
                let (wall, latencies) = measure_gateway_hot_samples(
                    &platform,
                    &workspace,
                    &principal,
                    &headers,
                    HOT_SAMPLES_PER_ROUND,
                )
                .await;
                gateway_wall += wall;
                gateway_us.extend(latencies);
                gateway_cpu_ticks = gateway_cpu_ticks.saturating_add(
                    process_and_worker_cpu_ticks(worker_pid).saturating_sub(cpu_before),
                );

                let cpu_before = process_and_worker_cpu_ticks(worker_pid);
                let (wall, latencies) = measure_direct_hot_samples(
                    &platform,
                    &workspace,
                    &principal,
                    &headers,
                    &lease,
                    &descriptor,
                    HOT_SAMPLES_PER_ROUND,
                )
                .await;
                direct_wall += wall;
                direct_us.extend(latencies);
                direct_cpu_ticks = direct_cpu_ticks.saturating_add(
                    process_and_worker_cpu_ticks(worker_pid).saturating_sub(cpu_before),
                );
            }
        }
        direct_us.sort_unstable();
        gateway_us.sort_unstable();
        let direct_p95_us = nearest_rank_u128(&direct_us, 95);
        let gateway_p95_us = nearest_rank_u128(&gateway_us, 95);
        let allowed_gateway_p95_us = direct_p95_us
            .saturating_mul(115)
            .div_ceil(100)
            .max(direct_p95_us.saturating_add(2_000));
        assert!(
            gateway_p95_us <= allowed_gateway_p95_us,
            "Gateway p95 overhead exceeded max(2ms, 15%)"
        );
        let direct_rps = HOT_SAMPLES as f64 / direct_wall.as_secs_f64();
        let gateway_rps = HOT_SAMPLES as f64 / gateway_wall.as_secs_f64();
        assert!(
            gateway_rps >= direct_rps * 0.85,
            "Gateway throughput fell below 85%: direct_rps={direct_rps:.2}, gateway_rps={gateway_rps:.2}, direct_wall_ms={}, gateway_wall_ms={}",
            direct_wall.as_millis(),
            gateway_wall.as_millis(),
        );
        let direct_cpu_per_request = direct_cpu_ticks as f64 / HOT_SAMPLES as f64;
        let gateway_cpu_per_request = gateway_cpu_ticks as f64 / HOT_SAMPLES as f64;
        assert!(
            gateway_cpu_per_request <= direct_cpu_per_request * 1.20 + 0.01,
            "Gateway CPU/request increment exceeded 20%"
        );

        let (_, stream_descriptor) = acquire_operation(
            &platform,
            &principal,
            "reference-app",
            "reference-app.events",
        )
        .await
        .expect("stream descriptor");
        let direct_stream_envelope = invocation_envelope(
            &platform,
            &workspace,
            &principal,
            &headers,
            "reference-app",
            &stream_descriptor,
            InvokeInput {
                payload: json!({}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: None,
            },
        )
        .expect("direct stream envelope");
        let direct_stream_started = Instant::now();
        let direct_stream = send_worker(
            lease.connection(),
            lease.app_id(),
            lease.generation().0.as_str(),
            &direct_stream_envelope.request_id,
            direct_stream_envelope.effective_deadline_unix_ms(),
            Method::POST,
            "/_cowd/v1/operations/reference-app.events/stream",
            Some(&direct_stream_envelope),
            Duration::from_secs(30),
        )
        .await
        .expect("direct stream");
        let mut direct_stream_body = direct_stream.into_body();
        direct_stream_body
            .frame()
            .await
            .expect("direct stream first frame")
            .expect("direct stream frame");
        let direct_ttfb_us = direct_stream_started.elapsed().as_micros();

        let gateway_stream_started = Instant::now();
        let gateway_stream = proxy_stream(
            &platform,
            &workspace,
            &principal,
            &headers,
            "reference-app",
            "reference-app.events",
            InvokeInput {
                payload: json!({}),
                idempotency_key: None,
                expected_revision: None,
                deadline_ms: None,
            },
            None,
            false,
        )
        .await;
        assert_eq!(gateway_stream.status(), StatusCode::OK);
        let mut gateway_stream_body = gateway_stream.into_body();
        let first_gateway_frame = gateway_stream_body
            .frame()
            .await
            .expect("Gateway stream first frame")
            .expect("Gateway stream frame")
            .into_data()
            .expect("Gateway stream data");
        let gateway_ttfb_us = gateway_stream_started.elapsed().as_micros();
        assert!(
            gateway_ttfb_us <= direct_ttfb_us.saturating_add(10_000),
            "Gateway stream TTFB overhead exceeded 10ms"
        );
        let open_line = first_gateway_frame
            .split(|byte| *byte == b'\n')
            .find(|line| !line.is_empty())
            .expect("Gateway stream open line");
        let open: AppStreamFrameV1 =
            serde_json::from_slice(open_line).expect("Gateway stream open frame");
        let cancel_started = Instant::now();
        let cancel = cancel_subscription(
            Extension(Arc::clone(&platform)),
            Extension(principal.clone()),
            AxumPath((
                "reference-app".to_owned(),
                open.subscription_id().to_owned(),
            )),
        )
        .await;
        let cancel_us = cancel_started.elapsed().as_micros();
        assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
        assert!(cancel_us <= 1_000_000, "stream cancellation exceeded 1s");

        let report = serde_json::json!({
            "schema_version": 1,
            "case": "reference_transport",
            "cold": {"samples": COLD_SAMPLES, "p95_us": cold_p95_us, "p99_us": cold_p99_us},
            "hot": {
                "samples": HOT_SAMPLES,
                "paired_rounds": HOT_ROUNDS,
                "direct_p95_us": direct_p95_us,
                "gateway_p95_us": gateway_p95_us,
                "direct_rps": direct_rps,
                "gateway_rps": gateway_rps,
                "direct_cpu_ticks_per_request": direct_cpu_per_request,
                "gateway_cpu_ticks_per_request": gateway_cpu_per_request,
            },
            "stream": {
                "direct_ttfb_us": direct_ttfb_us,
                "gateway_ttfb_us": gateway_ttfb_us,
                "cancel_us": cancel_us,
            },
        });
        let report_path = std::env::var("COWD_PERFORMANCE_REPORT")
            .expect("COWD_PERFORMANCE_REPORT must name a /tmp JSON report");
        assert!(Path::new(&report_path).starts_with("/tmp"));
        std::fs::write(
            report_path,
            serde_json::to_vec_pretty(&report).expect("performance report JSON"),
        )
        .expect("performance report");
        eprintln!("COWD_PERF_JSON {report}");
        lease.release().await;
        platform.shutdown().await.expect("performance shutdown");
    }

    async fn measure_direct_hot_samples(
        platform: &GatewayAppPlatform,
        workspace: &Path,
        principal: &super::super::AuthenticatedPrincipal,
        headers: &HeaderMap,
        lease: &AppRuntimeLease<GatewayAppConnector>,
        descriptor: &OperationDescriptorV1,
        samples: usize,
    ) -> (Duration, Vec<u128>) {
        let wall_started = Instant::now();
        let mut latencies = Vec::with_capacity(samples);
        for _ in 0..samples {
            let envelope = invocation_envelope(
                platform,
                workspace,
                principal,
                headers,
                "reference-app",
                descriptor,
                echo_input(),
            )
            .expect("direct envelope");
            let started = Instant::now();
            let response = send_worker(
                lease.connection(),
                lease.app_id(),
                lease.generation().0.as_str(),
                &envelope.request_id,
                envelope.effective_deadline_unix_ms(),
                Method::POST,
                "/_cowd/v1/operations/reference-app.echo/invoke",
                Some(&envelope),
                Duration::from_secs(30),
            )
            .await
            .expect("direct UDS response");
            assert_eq!(response.status(), StatusCode::OK);
            let body = response
                .into_body()
                .collect()
                .await
                .expect("direct body")
                .to_bytes();
            let provider: AppProviderResponseV1 =
                serde_json::from_slice(&body).expect("direct typed response");
            provider.validate().expect("direct response contract");
            latencies.push(started.elapsed().as_micros());
        }
        (wall_started.elapsed(), latencies)
    }

    async fn measure_gateway_hot_samples(
        platform: &GatewayAppPlatform,
        workspace: &Path,
        principal: &super::super::AuthenticatedPrincipal,
        headers: &HeaderMap,
        samples: usize,
    ) -> (Duration, Vec<u128>) {
        let wall_started = Instant::now();
        let mut latencies = Vec::with_capacity(samples);
        for _ in 0..samples {
            let started = Instant::now();
            let response = proxy_unary(
                platform,
                workspace,
                principal,
                headers,
                "reference-app",
                "reference-app.echo",
                echo_input(),
                None,
                None,
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            response
                .into_body()
                .collect()
                .await
                .expect("Gateway hot body");
            latencies.push(started.elapsed().as_micros());
        }
        (wall_started.elapsed(), latencies)
    }

    fn echo_input() -> InvokeInput {
        InvokeInput {
            payload: json!({"message":"performance"}),
            idempotency_key: None,
            expected_revision: None,
            deadline_ms: None,
        }
    }

    fn nearest_rank_u128(samples: &[u128], percentile: usize) -> u128 {
        let rank = samples.len().saturating_mul(percentile).saturating_add(99) / 100;
        samples[rank.saturating_sub(1).min(samples.len().saturating_sub(1))]
    }

    fn process_and_worker_cpu_ticks(worker_pid: u32) -> u64 {
        process_cpu_ticks("/proc/self/stat")
            .saturating_add(process_cpu_ticks(&format!("/proc/{worker_pid}/stat")))
    }

    fn process_cpu_ticks(path: &str) -> u64 {
        let stat = std::fs::read_to_string(path).expect("process stat");
        let fields = stat
            .rsplit_once(')')
            .expect("process stat comm")
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        fields[11]
            .parse::<u64>()
            .expect("process user ticks")
            .saturating_add(fields[12].parse::<u64>().expect("process system ticks"))
    }

    async fn decode_response<T: serde::de::DeserializeOwned>(response: Response<Body>) -> T {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("Gateway response body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("typed Gateway response")
    }
}
