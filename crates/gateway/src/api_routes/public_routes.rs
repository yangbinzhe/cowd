use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State as AxumState,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;

use super::capability_contract::{
    gateway_capability_contract, gateway_openai_tools, gateway_openapi_document,
};
use super::route_manifest::gateway_route_manifest;
use super::{
    AppState, ErrorResponse, WEB_SESSION_COOKIE, authenticated_human_principal_for_surface,
    cookie_value, issue_web_session, surface_capability_inventory,
    validate_surface_capability_request, web_session_principal,
};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(health_handler))
        .route("/healthz", get(gateway_health_handler))
        .route("/readyz", get(gateway_ready_handler))
        .route("/api/webui/manifest", get(webui_manifest_handler))
        // Schema and route inventory are static documentation, not runtime
        // control data. Keeping them public allows typed clients to generate
        // against a Gateway before a local human credential exists; all
        // operational endpoints remain behind the authenticated router.
        .route("/api/gateway/route-manifest", get(route_manifest_handler))
        .route(
            "/api/gateway/capability-contract",
            get(capability_contract_handler),
        )
        .route("/api/gateway/openapi.json", get(openapi_handler))
        .route("/api/gateway/openai-tools", get(openai_tools_handler))
        .route("/api/auth/login", post(login_handler))
        .route("/api/auth/verify", get(verify_handler))
        .route("/api/auth/logout", post(logout_handler))
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn gateway_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    Json(
        serde_json::to_value(crate::gateway_health::gateway_health_snapshot(&state))
            .unwrap_or_else(|_| serde_json::json!({"status":"error"})),
    )
}

async fn gateway_ready_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let snapshot = crate::gateway_health::gateway_readiness_snapshot(&state);
    let status = if snapshot.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(
            serde_json::to_value(snapshot)
                .unwrap_or_else(|_| serde_json::json!({"ready":false,"status":"error"})),
        ),
    )
}

async fn webui_manifest_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let health = crate::gateway_health::gateway_health_snapshot(&state);
    Json(
        serde_json::to_value(crate::gateway_service::webui_manifest(health))
            .unwrap_or_else(|_| serde_json::json!({"kind":"cowd.webui.manifest","status":"error"})),
    )
}

async fn route_manifest_handler() -> Json<serde_json::Value> {
    let routes = gateway_route_manifest();
    Json(serde_json::json!({
        "kind": "gateway.route_manifest",
        "schema_version": 1,
        "route_count": routes.len(),
        "routes": routes,
    }))
}

async fn capability_contract_handler() -> Json<serde_json::Value> {
    Json(
        serde_json::to_value(gateway_capability_contract()).unwrap_or_else(
            |_| serde_json::json!({"kind":"gateway.capability_contract","status":"error"}),
        ),
    )
}

async fn openapi_handler() -> Json<serde_json::Value> {
    Json(gateway_openapi_document())
}

async fn openai_tools_handler() -> Json<serde_json::Value> {
    Json(gateway_openai_tools())
}

#[derive(Deserialize)]
struct LoginRequest {
    token: String,
    #[serde(default)]
    surface_id: Option<String>,
    #[serde(default)]
    requested_capabilities: Vec<String>,
}

async fn login_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<LoginRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match &state.auth_token {
        None => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "auth not configured".to_string(),
            }),
        )),
        Some(expected) if expected == &body.token => {
            let surface_id = body
                .surface_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("legacy_gateway");
            let allowed = surface_capability_inventory(surface_id);
            let requested_capabilities = if body.requested_capabilities.is_empty() {
                allowed
            } else {
                let mut requested =
                    validate_surface_capability_request(surface_id, body.requested_capabilities)
                        .map_err(|error| {
                            (StatusCode::BAD_REQUEST, Json(ErrorResponse { error }))
                        })?;
                requested.sort();
                requested.dedup();
                requested
            };
            let (session, entitlement) = issue_web_session(
                &state.config_home,
                &body.token,
                surface_id,
                requested_capabilities,
            )
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("authentication_authority_error:{error}"),
                    }),
                )
            })?;
            let cookie = HeaderValue::from_str(&format!(
                "{WEB_SESSION_COOKIE}={session}; HttpOnly; SameSite=Strict; Path=/api; Max-Age=28800"
            ))
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("browser_session_cookie_error:{error}"),
                    }),
                )
            })?;
            tracing::info!("API login successful");
            Ok((
                [(header::SET_COOKIE, cookie)],
                Json(serde_json::json!({
                    "success": true,
                    "auth_required": true,
                    "session_kind": "broker_signed_http_only_cookie",
                    "surface_id": surface_id,
                    "entitlement": entitlement,
                })),
            ))
        }
        Some(_) => Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid token".to_string(),
            }),
        )),
    }
}

async fn verify_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let auth_token = match &state.auth_token {
        None => {
            return Ok(Json(serde_json::json!({
                "valid": true,
                "auth_required": false,
            })));
        }
        Some(token) => token,
    };

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    match auth_header {
        Some(value) if value == format!("Bearer {auth_token}") => {
            let (_, entitlement) = authenticated_human_principal_for_surface(
                &state.config_home,
                auth_token,
                "legacy_gateway",
                surface_capability_inventory("legacy_gateway"),
            )
            .map_err(|error| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse {
                        error: format!("invalid_bearer_credential:{error}"),
                    }),
                )
            })?;
            Ok(Json(serde_json::json!({
                "valid": true,
                "auth_required": true,
                "transport": "bearer",
                "entitlement": entitlement,
            })))
        }
        _ if cookie_value(&headers, WEB_SESSION_COOKIE).is_some() => {
            let principal =
                web_session_principal(&state.config_home, &headers, state.auth_token.as_deref())
                    .map_err(|error| {
                        (
                            StatusCode::UNAUTHORIZED,
                            Json(ErrorResponse {
                                error: format!("invalid_browser_session:{error}"),
                            }),
                        )
                    })?;
            let (_, entitlement) = authenticated_human_principal_for_surface(
                &state.config_home,
                auth_token,
                "legacy_gateway",
                principal.claims().capabilities.clone(),
            )
            .map_err(|error| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse {
                        error: format!("browser_session_entitlement_error:{error}"),
                    }),
                )
            })?;
            Ok(Json(serde_json::json!({
                "valid": true,
                "auth_required": true,
                "transport": "browser_session",
                "entitlement": entitlement,
            })))
        }
        _ => Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid or missing credential".to_string(),
            }),
        )),
    }
}

async fn logout_handler() -> (
    [(axum::http::header::HeaderName, HeaderValue); 1],
    Json<serde_json::Value>,
) {
    (
        [(
            header::SET_COOKIE,
            HeaderValue::from_static(
                "cowd_web_session=; HttpOnly; SameSite=Strict; Path=/api; Max-Age=0",
            ),
        )],
        Json(serde_json::json!({
            "success": true,
            "browser_session_cleared": true,
        })),
    )
}
