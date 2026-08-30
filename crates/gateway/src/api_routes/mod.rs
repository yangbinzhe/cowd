// ── API Routes ─────────────────────────────────────────────────
// Core gateway routes shared between TUI and HTTP API.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::State as AxumState,
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
#[cfg(test)]
use runtime::{
    ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextRole, ContextRuntimeKernel,
    ContextSourceKind,
};
use serde::Serialize;

use runtime::ProfileManager;
use tools::ToolCatalog;

#[cfg(test)]
use crate::active_session::ActiveSessionDirectory;
use crate::event_bus::SessionProjectionHub;
#[cfg(test)]
use crate::services::session_service::repository::SessionRepository;
use crate::services::GatewayServices;
#[cfg(test)]
use memory::cognitive::CognitiveContextManager;
#[cfg(test)]
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
#[cfg(test)]
use memory::MemoryScope;
#[cfg(test)]
use session::{SessionRecord, UnifiedSessionStore};

mod agent_routes;
mod app_routes;
mod approval_routes;
mod audit_routes;
mod binding;
mod capability_contract;
pub(crate) use capability_contract::benchmark_openapi_document;
pub(crate) mod connector_routes;
mod context_routes;
mod core_routes;
pub(crate) mod cross_plane_routes;
mod edge_routes;
mod evolution_routes;
mod growth_routes;
mod harness_eval_routes;
pub(crate) mod live_routes;
mod managed_agent_routes;
mod matrix_outcomes;
mod matrix_routes;
pub(crate) mod memory_routes;
mod message_connector_routes;
mod message_routes;
mod mission_routes;
mod profile_routes;
mod public_routes;
mod reality_routes;
mod resource_routes;
pub(crate) mod route_manifest;
mod route_registry;
mod runtime_routes;
mod session_routes;
mod skill_routes;
mod slash_routes;
mod surface_routes;
mod system_routes;
mod task_routes;
mod workspace_routes;

// ── Shared application state ───────────────────────────────────

pub struct AppState {
    pub tool_registry: Arc<ToolCatalog>,
    pub config: Option<serde_json::Value>,
    pub static_webui: crate::gateway_static::StaticWebUiSource,
    pub auth_token: Option<String>,
    pub workspace_root: PathBuf,
    pub config_home: PathBuf,
    pub profile_id: String,
    pub profile_manager: Arc<ProfileManager>,
    pub services: Arc<GatewayServices>,
    pub session_lease_registry: Option<Arc<session::SessionLeaseRegistry>>,
    pub(crate) live_registry: Arc<live_routes::LiveRegistry>,
}

fn validated_session_observer_id(observer_id: Option<&str>) -> Option<&str> {
    observer_id.map(str::trim).filter(|value| {
        !value.is_empty()
            && value.len() <= 128
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ':' | '-' | '_' | '.'))
    })
}

pub(super) fn session_lease_owner(principal: &AuthenticatedPrincipal, observer_id: &str) -> String {
    let principal_owner = format!("principal:{}", principal.0.claims().principal_id);
    format!("{principal_owner}:observer:{observer_id}")
}

/// Admit one session mutation from a concrete attached Surface.
///
/// Authentication grants access to the account; it does not silently upgrade
/// a reader attachment into a writer. Mutation routes therefore require a
/// valid observer identity, an existing writer attachment for that exact
/// principal/surface pair, and a compatible writer lease.
pub(super) async fn require_session_writer_admission(
    state: &AppState,
    principal: &AuthenticatedPrincipal,
    headers: &HeaderMap,
    session_id: &str,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    let raw_observer = match headers.get("x-cowd-observer-id") {
        Some(value) => Some(value.to_str().map_err(|_| {
            (
                StatusCode::FORBIDDEN,
                Json(ErrorResponse {
                    error: "x-cowd-observer-id is not valid UTF-8".to_string(),
                }),
            )
        })?),
        None => None,
    };
    let observer_id = raw_observer
        .and_then(|value| validated_session_observer_id(Some(value)))
        .ok_or_else(|| {
            (
                StatusCode::FORBIDDEN,
                Json(ErrorResponse {
                    error: if raw_observer.is_some() {
                        "x-cowd-observer-id is invalid".to_string()
                    } else {
                        "session mutation requires x-cowd-observer-id".to_string()
                    },
                }),
            )
        })?;
    let actor_id = surface_actor_id(principal, observer_id);
    let role = state
        .services
        .session
        .lifecycle_attachment_role(session_id, &actor_id)
        .await;
    if role.as_deref() != Some("writer") {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: match role.as_deref() {
                    Some("reader") => {
                        "reader session attachment cannot execute mutations".to_string()
                    }
                    _ => "session mutation requires an attached writer Surface".to_string(),
                },
            }),
        ));
    }
    let owner = session_lease_owner(principal, observer_id);
    let registry = state.session_lease_registry.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session lease registry unavailable; mutation is fail-closed".to_string(),
            }),
        )
    })?;
    let admission = registry.ensure_writer(session_id, &owner).await;
    if admission.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: admission
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("session writer lease rejected")
                    .to_string(),
            }),
        ));
    }
    Ok(owner)
}

/// Request identity derived exclusively by Gateway authentication middleware.
/// Route payloads must never carry actor or capability fields.
#[derive(Debug, Clone)]
pub(crate) struct AuthenticatedPrincipal(pub(crate) runtime::VerifiedPrincipal);

pub(super) const WEB_SESSION_COOKIE: &str = "cowd_web_session";
pub(super) const WEB_SESSION_TTL_SECONDS: u64 = 24 * 60 * 60;
const WEB_SESSION_TTL_MS: u64 = WEB_SESSION_TTL_SECONDS * 1_000;

/// Stable audit principal derived only from the verified Gateway credential.
///
/// HTTP payloads may describe an external identity reference, but never choose
/// the principal on whose behalf an effect is authorized or recorded.
pub(crate) fn principal_actor_id(principal: &AuthenticatedPrincipal) -> String {
    format!("principal:{}", principal.0.claims().principal_id)
}

/// Lifecycle attachments identify a verified principal at a concrete surface.
///
/// A person may observe the same session in TUI and WebUI at once.  The
/// lifecycle kernel keys attachments by actor id, so using only the principal
/// would make the later surface silently replace the earlier one.  The
/// surface remains descriptive input, while the authenticated principal is
/// always supplied by the Gateway.
pub(crate) fn surface_actor_id(principal: &AuthenticatedPrincipal, surface: &str) -> String {
    format!(
        "{}:surface:{}",
        principal_actor_id(principal),
        surface.trim()
    )
}

/// Revalidate a long-lived projection stream against the same broker
/// lifecycle authority used when its request was authenticated. Axum's
/// middleware can authenticate only the opening HTTP request; without this
/// check an already-open SSE response would retain revoked or re-profiled
/// access indefinitely.
pub(super) fn projection_stream_principal_current(
    config_home: &std::path::Path,
    principal: &AuthenticatedPrincipal,
) -> Result<(), String> {
    let claims = principal.0.claims();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(u64::MAX, |duration| duration.as_millis() as u64);
    if claims
        .expires_at_ms
        .is_some_and(|expires_at| expires_at <= now_ms)
    {
        return Err("projection stream principal expired".to_string());
    }

    #[cfg(not(any(test, feature = "test-support")))]
    {
        let client = auth_broker::BrokerClient::new(auth_broker::BrokerClient::default_socket(
            config_home.join("auth-broker"),
        ));
        let lifecycle = client
            .credential_lifecycle()
            .map_err(|error| format!("projection authorization authority unavailable: {error}"))?;
        if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
            return Err("projection stream credential is no longer active".to_string());
        }
        if lifecycle.credential_epoch != claims.credential_epoch {
            return Err("projection stream credential epoch changed".to_string());
        }
        if lifecycle.profile_revision != claims.profile_revision {
            return Err("projection stream authorization profile changed".to_string());
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    {
        let _ = config_home;
    }

    Ok(())
}

impl AppState {
    pub(crate) fn startup_config_snapshot(&self) -> Option<&serde_json::Value> {
        self.config.as_ref()
    }

    pub(crate) fn runtime_config_json_snapshot(&self) -> Option<serde_json::Value> {
        self.services
            .system
            .runtime_config_json(&self.workspace_root, &self.config_home)
            .ok()
            .or_else(|| self.config.clone())
    }

    pub(crate) fn redacted_runtime_config_json_snapshot(&self) -> Option<serde_json::Value> {
        self.services
            .system
            .redacted_runtime_config_json(&self.workspace_root, &self.config_home)
            .ok()
            .or_else(|| {
                self.config
                    .clone()
                    .map(|value| self.services.system.redact_config_json(value))
            })
    }
}

impl AppState {
    pub(crate) fn has_unified_store(&self) -> bool {
        self.services.session.has_unified_store()
    }

    fn event_bus(&self) -> Arc<SessionProjectionHub> {
        self.services.session.event_bus()
    }

    pub(crate) fn list_active_session_ids(&self) -> Vec<String> {
        self.services.session.list_active_session_ids()
    }
}

// ── Auth middleware ────────────────────────────────────────────

/// Attach a server-derived principal to every protected request.  Same-origin
/// headers are browser metadata, not proof of identity, and are deliberately
/// never accepted as an authentication bypass.
async fn auth_middleware(
    AxumState(state): AxumState<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, Response> {
    let request_path = request.uri().path().to_string();
    let claims = if let Some(token) = &state.auth_token {
        let auth_header = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v: &axum::http::HeaderValue| v.to_str().ok());

        match auth_header {
            Some(h) if h == format!("Bearer {token}") => {
                let (surface_id, requested_capabilities) =
                    requested_capabilities_for_headers(request.headers()).map_err(|error| {
                        auth_error_response(&state, &request_path, StatusCode::BAD_REQUEST, error)
                    })?;
                authenticated_human_principal_for_surface(
                    &state.config_home,
                    token,
                    &surface_id,
                    requested_capabilities,
                )
                .map(|(principal, _)| principal)
                .map_err(|error| {
                    auth_error_response(
                        &state,
                        &request_path,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("authentication_authority_error:{error}"),
                    )
                })?
            }
            _ => web_session_principal(
                &state.config_home,
                request.headers(),
                state.auth_token.as_deref(),
            )
            .map_err(|error| {
                auth_error_response(
                    &state,
                    &request_path,
                    StatusCode::UNAUTHORIZED,
                    format!("unauthorized:{error}"),
                )
            })?,
        }
    } else {
        #[cfg(any(test, feature = "test-support"))]
        {
            test_human_principal()
        }
        #[cfg(not(any(test, feature = "test-support")))]
        return Err(auth_error_response(
            &state,
            &request_path,
            StatusCode::UNAUTHORIZED,
            "authentication_not_configured".to_string(),
        ));
    };
    request
        .extensions_mut()
        .insert(AuthenticatedPrincipal(claims));
    Ok(next.run(request).await)
}

fn auth_error_response(
    state: &AppState,
    path: &str,
    status: StatusCode,
    message: String,
) -> Response {
    let _ = (state, path);
    (status, Json(ErrorResponse { error: message })).into_response()
}

pub(super) fn authenticated_human_principal(
    config_home: &std::path::Path,
    token: &str,
) -> Result<runtime::VerifiedPrincipal, String> {
    authenticated_human_principal_for_surface(config_home, token, "legacy_gateway", Vec::new())
        .map(|(principal, _)| principal)
}

pub(super) fn authenticated_human_principal_for_surface(
    config_home: &std::path::Path,
    token: &str,
    surface_id: &str,
    requested_capabilities: Vec<String>,
) -> Result<
    (
        runtime::VerifiedPrincipal,
        auth_broker::HumanEntitlementProjection,
    ),
    String,
> {
    let requested_capabilities =
        validate_surface_capability_request(surface_id, requested_capabilities)?;
    #[cfg(not(any(test, feature = "test-support")))]
    {
        let client = auth_broker::BrokerClient::new(auth_broker::BrokerClient::default_socket(
            config_home.join("auth-broker"),
        ));
        let result = client
            .authenticate_human_for_surface(
                token,
                surface_id,
                requested_capabilities,
                Some(5 * 60 * 1_000),
            )
            .map_err(|error| error.to_string())?;
        let lifecycle = client
            .credential_lifecycle()
            .map_err(|error| error.to_string())?;
        if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
            return Err("local human credential is revoked".to_string());
        }
        let principal = runtime::PrincipalVerifier::from_base64(
            &result.envelope.key_id,
            &result.public_key_base64,
        )
        .map_err(|error| error.to_string())?
        .requiring_credential_epoch(lifecycle.credential_epoch)
        .verify(&result.envelope)
        .map_err(|error| error.to_string())?;
        Ok((principal, result.entitlement))
    }

    #[cfg(any(test, feature = "test-support"))]
    {
        // Product integration tests may start a real local broker.  Use it
        // whenever its socket exists so request principals and one-time
        // decision leases share the same credential epoch.  The direct helper
        // below remains only for isolated route tests that intentionally do
        // not assemble a broker process.
        let socket = auth_broker::BrokerClient::default_socket(config_home.join("auth-broker"));
        if socket.exists() {
            let client = auth_broker::BrokerClient::new(socket);
            let result = client
                .authenticate_human_for_surface(
                    token,
                    surface_id,
                    requested_capabilities.clone(),
                    Some(5 * 60 * 1_000),
                )
                .map_err(|error| error.to_string())?;
            let lifecycle = client
                .credential_lifecycle()
                .map_err(|error| error.to_string())?;
            if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
                return Err("local human credential is revoked".to_string());
            }
            let principal = runtime::PrincipalVerifier::from_base64(
                &result.envelope.key_id,
                &result.public_key_base64,
            )
            .map_err(|error| error.to_string())?
            .requiring_credential_epoch(lifecycle.credential_epoch)
            .verify(&result.envelope)
            .map_err(|error| error.to_string())?;
            return Ok((principal, result.entitlement));
        }
        let requested_capabilities = if requested_capabilities.is_empty() {
            test_human_capabilities()
        } else {
            requested_capabilities
        };
        let (envelope, public_key) = auth_broker::test_support::issue_human_principal(
            config_home.join("auth-broker"),
            token,
            requested_capabilities.clone(),
            Some(5 * 60 * 1_000),
        )
        .map_err(|error| error.to_string())?;
        let principal = runtime::PrincipalVerifier::from_base64(&envelope.key_id, &public_key)
            .map_err(|error| error.to_string())?
            .verify(&envelope)
            .map_err(|error| error.to_string())?;
        let app_profiles = envelope.claims.app_profiles.clone();
        Ok((
            principal,
            auth_broker::HumanEntitlementProjection {
                core_profile_id: "core_manager".to_string(),
                app_profiles,
                profile_revision: 1,
                credential_epoch: 1,
                ceiling: test_human_capabilities(),
                granted: requested_capabilities,
                denied: Vec::new(),
            },
        ))
    }
}

/// Mint a bounded browser session from a local human credential. The
/// browser receives only a broker-signed principal envelope, never the raw
/// credential. Gateway has no signing key and verifies this envelope again on
/// every protected request.
pub(super) fn issue_web_session(
    config_home: &std::path::Path,
    credential: &str,
    surface_id: &str,
    requested_capabilities: Vec<String>,
) -> Result<(String, auth_broker::HumanEntitlementProjection), String> {
    let requested_capabilities =
        validate_surface_capability_request(surface_id, requested_capabilities)?;
    #[cfg(not(any(test, feature = "test-support")))]
    {
        let client = auth_broker::BrokerClient::new(auth_broker::BrokerClient::default_socket(
            config_home.join("auth-broker"),
        ));
        let result = client
            .authenticate_human_for_surface(
                credential,
                surface_id,
                requested_capabilities,
                Some(WEB_SESSION_TTL_MS),
            )
            .map_err(|error| error.to_string())?;
        let lifecycle = client
            .credential_lifecycle()
            .map_err(|error| error.to_string())?;
        if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
            return Err("local human credential is revoked".to_string());
        }
        verify_human_envelope(
            &result.envelope,
            &result.public_key_base64,
            lifecycle.credential_epoch,
        )?;
        encode_web_session(&result.envelope).map(|session| (session, result.entitlement))
    }

    #[cfg(any(test, feature = "test-support"))]
    {
        // Keep a real-broker integration test on the same issuance path as a
        // production browser session. The direct fixture below remains only
        // for isolated route tests without a broker process.
        let socket = auth_broker::BrokerClient::default_socket(config_home.join("auth-broker"));
        if socket.exists() {
            let client = auth_broker::BrokerClient::new(socket);
            let result = client
                .authenticate_human_for_surface(
                    credential,
                    surface_id,
                    requested_capabilities.clone(),
                    Some(WEB_SESSION_TTL_MS),
                )
                .map_err(|error| error.to_string())?;
            let lifecycle = client
                .credential_lifecycle()
                .map_err(|error| error.to_string())?;
            if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
                return Err("local human credential is revoked".to_string());
            }
            verify_human_envelope(
                &result.envelope,
                &result.public_key_base64,
                lifecycle.credential_epoch,
            )?;
            return encode_web_session(&result.envelope)
                .map(|session| (session, result.entitlement));
        }
        let requested_capabilities = if requested_capabilities.is_empty() {
            test_human_capabilities()
        } else {
            requested_capabilities
        };
        let (envelope, public_key) = auth_broker::test_support::issue_human_principal(
            config_home.join("auth-broker"),
            credential,
            requested_capabilities.clone(),
            Some(WEB_SESSION_TTL_MS),
        )
        .map_err(|error| error.to_string())?;
        verify_human_envelope(&envelope, &public_key, 0)?;
        let app_profiles = envelope.claims.app_profiles.clone();
        encode_web_session(&envelope).map(|session| {
            (
                session,
                auth_broker::HumanEntitlementProjection {
                    core_profile_id: "core_manager".to_string(),
                    app_profiles,
                    profile_revision: 1,
                    credential_epoch: 1,
                    ceiling: test_human_capabilities(),
                    granted: requested_capabilities,
                    denied: Vec::new(),
                },
            )
        })
    }
}

pub(super) fn web_session_principal(
    config_home: &std::path::Path,
    headers: &axum::http::HeaderMap,
    test_credential: Option<&str>,
) -> Result<runtime::VerifiedPrincipal, String> {
    let encoded = cookie_value(headers, WEB_SESSION_COOKIE)
        .ok_or_else(|| "missing_browser_session".to_string())?;
    if encoded.len() > 8_192 {
        return Err("browser_session_too_large".to_string());
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|error| format!("invalid_browser_session:{error}"))?;
    let envelope =
        serde_json::from_slice::<harness_contract::security::SignedPrincipalEnvelope>(&bytes)
            .map_err(|error| format!("invalid_browser_session:{error}"))?;

    #[cfg(not(any(test, feature = "test-support")))]
    {
        let _ = test_credential;
        let client = auth_broker::BrokerClient::new(auth_broker::BrokerClient::default_socket(
            config_home.join("auth-broker"),
        ));
        let (key_id, public_key) = client.trust_metadata().map_err(|error| error.to_string())?;
        if envelope.key_id != key_id {
            return Err("browser_session_authority_mismatch".to_string());
        }
        let lifecycle = client
            .credential_lifecycle()
            .map_err(|error| error.to_string())?;
        if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
            return Err("local_human_credential_revoked".to_string());
        }
        verify_human_envelope(&envelope, &public_key, lifecycle.credential_epoch)
    }

    #[cfg(any(test, feature = "test-support"))]
    {
        // Product integration tests may run a real broker. When it is
        // present, a browser session must use the same authority and epoch as
        // every other protected request; otherwise a long-lived APP stream
        // would correctly observe a stale lifecycle immediately after login.
        let socket = auth_broker::BrokerClient::default_socket(config_home.join("auth-broker"));
        if socket.exists() {
            let client = auth_broker::BrokerClient::new(socket);
            let (key_id, public_key) =
                client.trust_metadata().map_err(|error| error.to_string())?;
            if envelope.key_id != key_id {
                return Err("browser_session_authority_mismatch".to_string());
            }
            let lifecycle = client
                .credential_lifecycle()
                .map_err(|error| error.to_string())?;
            if lifecycle.status != auth_broker::CredentialLifecycleStatus::Active {
                return Err("local_human_credential_revoked".to_string());
            }
            return verify_human_envelope(&envelope, &public_key, lifecycle.credential_epoch);
        }
        let (_, public_key) = auth_broker::test_support::issue_human_principal(
            config_home.join("auth-broker"),
            test_credential.ok_or_else(|| "test_browser_session_credential_missing".to_string())?,
            test_human_capabilities(),
            Some(WEB_SESSION_TTL_MS),
        )
        .map_err(|error| error.to_string())?;
        verify_human_envelope(&envelope, &public_key, 0)
    }
}

fn verify_human_envelope(
    envelope: &harness_contract::security::SignedPrincipalEnvelope,
    public_key: &str,
    credential_epoch: u64,
) -> Result<runtime::VerifiedPrincipal, String> {
    let verifier = runtime::PrincipalVerifier::from_base64(&envelope.key_id, public_key)
        .map_err(|error| error.to_string())?;
    let verifier = if credential_epoch == 0 {
        verifier
    } else {
        verifier.requiring_credential_epoch(credential_epoch)
    };
    verifier.verify(envelope).map_err(|error| error.to_string())
}

fn encode_web_session(
    envelope: &harness_contract::security::SignedPrincipalEnvelope,
) -> Result<String, String> {
    serde_json::to_vec(envelope)
        .map(|value| URL_SAFE_NO_PAD.encode(value))
        .map_err(|error| error.to_string())
}

pub(super) fn cookie_value<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())?
        .split(';')
        .map(str::trim)
        .find_map(|pair| pair.split_once('='))
        .and_then(|(key, value)| (key == name).then_some(value))
}

/// Ask the isolated local authority for a short-lived, action-bound human
/// decision lease. The Gateway receives only a signed envelope and the
/// authority public key; signing material never enters the process.
pub(super) fn issue_human_decision_lease(
    config_home: &std::path::Path,
    credential: &str,
    review_id: impl Into<String>,
    action: impl Into<String>,
    scope: impl Into<String>,
    evidence_digest: impl Into<String>,
    expires_at_ms: u64,
) -> Result<(harness_contract::security::SignedDecisionLease, String), String> {
    let review_id = review_id.into();
    let action = action.into();
    let scope = scope.into();
    let evidence_digest = evidence_digest.into();
    #[cfg(not(any(test, feature = "test-support")))]
    {
        let client = auth_broker::BrokerClient::new(auth_broker::BrokerClient::default_socket(
            config_home.join("auth-broker"),
        ));
        client
            .issue_decision_lease(
                credential,
                review_id,
                action,
                scope,
                evidence_digest,
                expires_at_ms,
            )
            .map_err(|error| error.to_string())
    }

    #[cfg(any(test, feature = "test-support"))]
    {
        let socket = auth_broker::BrokerClient::default_socket(config_home.join("auth-broker"));
        if socket.exists() {
            return auth_broker::BrokerClient::new(socket)
                .issue_decision_lease(
                    credential,
                    review_id,
                    action,
                    scope,
                    evidence_digest,
                    expires_at_ms,
                )
                .map_err(|error| error.to_string());
        }
        auth_broker::test_support::issue_decision_lease(
            config_home.join("auth-broker"),
            credential,
            review_id,
            action,
            scope,
            evidence_digest,
            expires_at_ms,
        )
        .map_err(|error| error.to_string())
    }
}

#[cfg(any(test, feature = "test-support"))]
fn test_human_capabilities() -> Vec<String> {
    let mut capabilities = harness_contract::security::CORE_HUMAN_CAPABILITIES
        .iter()
        .map(|capability| (*capability).to_string())
        .collect::<Vec<_>>();
    capabilities.sort();
    capabilities.dedup();
    capabilities
}

fn requested_capabilities_for_headers(
    headers: &axum::http::HeaderMap,
) -> Result<(String, Vec<String>), String> {
    let surface_id = headers
        .get("x-cowd-surface-id")
        .map(|value| {
            value
                .to_str()
                .map_err(|_| "x-cowd-surface-id must be valid UTF-8".to_string())
        })
        .transpose()?
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("legacy_gateway")
        .trim()
        .to_string();
    let requested = headers
        .get("x-cowd-requested-capabilities")
        .map(|value| {
            value
                .to_str()
                .map_err(|_| "x-cowd-requested-capabilities must be valid UTF-8".to_string())
        })
        .transpose()?
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut requested = validate_surface_capability_request(&surface_id, requested)?;
    requested.sort();
    requested.dedup();
    Ok((surface_id, requested))
}

pub(super) fn validate_surface_capability_request(
    surface_id: &str,
    requested: Vec<String>,
) -> Result<Vec<String>, String> {
    if surface_id.trim().is_empty()
        || requested
            .iter()
            .any(|capability| capability.trim().is_empty())
    {
        return Err(format!(
            "surface {surface_id} has an invalid requested capability declaration"
        ));
    }
    let mut requested = requested;
    requested.sort();
    requested.dedup();
    Ok(requested)
}

#[cfg(any(test, feature = "test-support"))]
fn test_human_principal() -> runtime::VerifiedPrincipal {
    static PRINCIPAL: std::sync::OnceLock<runtime::VerifiedPrincipal> = std::sync::OnceLock::new();
    PRINCIPAL
        .get_or_init(|| {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |duration| duration.as_nanos());
            let (envelope, public_key) = auth_broker::test_support::issue_human_principal(
                std::env::temp_dir().join(format!(
                    "cowd-gateway-test-auth-{}-{nonce}",
                    std::process::id()
                )),
                "test-only-credential",
                test_human_capabilities(),
                None,
            )
            .expect("test principal envelope");
            runtime::PrincipalVerifier::from_base64(&envelope.key_id, &public_key)
                .expect("test verifier")
                .verify(&envelope)
                .expect("test verified principal")
        })
        .clone()
}

/// Minimal, feature-gated fixture for external black-box route tests.
///
/// The harness uses the same `api_router` and Runtime-backed `GatewayServices`
/// construction as the in-crate route tests. It deliberately exposes no
/// handlers, no raw stores, and no authentication bypass outside the explicit
/// `test-support` feature.
#[cfg(feature = "test-support")]
pub mod test_support {
    use std::{
        collections::HashMap,
        path::PathBuf,
        sync::{mpsc, Arc},
        thread::JoinHandle,
        time::Instant,
    };

    use axum::Router;
    use model_protocol::provider_config::{ProviderConfig, ProvidersConfig};
    use runtime::ProfileManager;
    use tools::ToolCatalog;

    use super::{api_router, live_routes, AppState};
    use crate::{
        active_session::ActiveSessionDirectory,
        event_bus::SessionProjectionHub,
        runtime_service::RuntimeService,
        services::session_service::{
            activation::SessionActivationCoordinator, presence::SessionPresenceLedger,
            repository::SessionRepository,
        },
        services::{GatewayServices, SessionService},
    };

    pub struct GatewayTestHarness {
        state: Arc<AppState>,
        session_workers: Arc<TestSessionWorkerDriver>,
    }

    /// Owns the Tokio runtime that drives the production Session workers used
    /// by the synchronous black-box harness API.
    ///
    /// Integration tests call `GatewayTestHarness::in_memory()` from inside a
    /// Tokio test, so attempting to synchronously block that test runtime would
    /// panic. A dedicated thread preserves the existing synchronous fixture
    /// API while still exercising the production supervisor and shutdown path.
    struct TestSessionWorkerDriver {
        stop: Option<mpsc::Sender<()>>,
        thread: Option<JoinHandle<()>>,
    }

    impl TestSessionWorkerDriver {
        fn start(
            runtime: Arc<RuntimeService>,
            session: Arc<SessionService>,
            event_bus: Arc<SessionProjectionHub>,
        ) -> Result<
            (
                Self,
                Arc<crate::session_runtime_bridge::SessionWorkerSupervisor>,
            ),
            String,
        > {
            let (ready_tx, ready_rx) = mpsc::sync_channel(1);
            let (stop_tx, stop_rx) = mpsc::channel();
            let thread = std::thread::Builder::new()
                .name("cowd-gateway-test-session-workers".to_string())
                .spawn(move || {
                    let tokio_runtime = match tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let _ = ready_tx.send(Err(format!(
                                "failed to create Gateway test worker runtime: {error}"
                            )));
                            return;
                        }
                    };
                    match tokio_runtime.block_on(
                        crate::session_runtime_bridge::SessionWorkerSupervisor::start(
                            runtime, session, event_bus,
                        ),
                    ) {
                        Ok(supervisor) => {
                            if ready_tx.send(Ok(Arc::clone(&supervisor))).is_err() {
                                tokio_runtime.block_on(supervisor.shutdown());
                                return;
                            }
                            let _ = stop_rx.recv();
                            tokio_runtime.block_on(supervisor.shutdown());
                        }
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                        }
                    }
                })
                .map_err(|error| format!("failed to spawn Gateway test worker thread: {error}"))?;
            let supervisor = ready_rx
                .recv()
                .map_err(|error| format!("Gateway test worker startup disconnected: {error}"))??;
            Ok((
                Self {
                    stop: Some(stop_tx),
                    thread: Some(thread),
                },
                supervisor,
            ))
        }
    }

    impl Drop for TestSessionWorkerDriver {
        fn drop(&mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    impl GatewayTestHarness {
        pub fn in_memory() -> Result<Self, String> {
            Self::in_memory_with_optional_auth_token(None)
        }

        /// Construct the same in-memory production router with bearer
        /// authentication enabled. This is intentionally test-support only so
        /// black-box tests can prove missing and invalid credentials are
        /// rejected without exposing a production authentication bypass.
        pub fn in_memory_with_auth_token(token: impl Into<String>) -> Result<Self, String> {
            let token = token.into();
            if token.trim().is_empty() {
                return Err("test auth token must not be empty".to_string());
            }
            Self::in_memory_with_optional_auth_token(Some(token))
        }

        fn in_memory_with_optional_auth_token(auth_token: Option<String>) -> Result<Self, String> {
            let root = unique_test_root("gateway-api-harness");
            let config_home = root.join("config");
            let workspace_root = root.join("workspace");
            std::fs::create_dir_all(&config_home).map_err(|error| error.to_string())?;
            std::fs::create_dir_all(&workspace_root).map_err(|error| error.to_string())?;

            let selected_storage = Arc::new(
                crate::selected_storage::SelectedStorageTopology::compose_for_runtime(
                    &runtime::StorageTopologyConfig::default(),
                    &config_home,
                    &workspace_root,
                )?,
            );
            let sessions = Arc::new(ActiveSessionDirectory::new());
            let event_bus = SessionProjectionHub::new();
            let session_store = Arc::clone(&selected_storage.session_store);
            let session_repository = Arc::new(SessionRepository::new(
                Arc::clone(&sessions),
                Some(Arc::clone(&session_store)),
                Arc::clone(&event_bus),
            ));
            let presence_ledger = Arc::new(SessionPresenceLedger::with_store(Arc::clone(
                &session_store,
            )));
            let session_runtime_port =
                crate::session_runtime_data_port::GatewaySessionRuntimePort::new();
            let provider_registry = Arc::new(
                runtime::ProviderRegistry::new(ProvidersConfig {
                    providers: HashMap::from([(
                        "test".to_string(),
                        ProviderConfig {
                            name: "test".to_string(),
                            // The black-box harness validates model ownership
                            // but never submits requests to this closed endpoint.
                            base_url: "http://127.0.0.1:9/v1".to_string(),
                            api_key: "test".to_string(),
                            models: vec![
                                crate::DEFAULT_MODEL_ALIAS.to_string(),
                                "test-model".to_string(),
                            ],
                            protocol: Some("completions".to_string()),
                            parallel_tool_calls: Default::default(),
                            early_tool_start: Default::default(),
                        },
                    )]),
                })
                .map_err(|error| format!("{error:?}"))?,
            );
            let runtime_services = runtime::RuntimeServices::builder(&config_home, &workspace_root)
                .provider_registry(Arc::clone(&provider_registry))
                .runtime_event_store(Arc::clone(&selected_storage.runtime_event_store))
                .artifact_store(Arc::clone(&selected_storage.artifact_store))
                .task_aggregate_service(Arc::clone(&selected_storage.task_service))
                .session_ports(
                    session_runtime_port.clone(),
                    session_runtime_port.clone(),
                    session_runtime_port.clone(),
                    session_runtime_port.clone(),
                )
                .build()
                .map_err(|error| error.to_string())?;
            let runtime = Arc::new(
                RuntimeService::new(
                    Arc::clone(&sessions),
                    Arc::new(session::SessionLeaseRegistry::default()),
                    session_runtime_port.clone(),
                    Arc::clone(&event_bus),
                    Instant::now(),
                    Some("test-model".to_string()),
                    provider_registry,
                    Arc::new(runtime::UpgradeCoordinator::new()),
                    runtime_services,
                )
                .map_err(|error| error.to_string())?
                .with_tool_host(Arc::new(
                    tools::ToolHost::builtin("gateway-black-box-test", workspace_root.clone())
                        .with_authorization_lease_verifier(Arc::new(
                            runtime::AuthorizationNegotiator::verify_lease_signature,
                        )),
                )),
            );
            let session_activation = Arc::new(SessionActivationCoordinator::new(
                Arc::clone(&runtime),
                Arc::clone(&session_repository),
                Arc::clone(&presence_ledger),
                Arc::new(runtime::session_lifecycle::SessionWorkingSetManager::new(
                    runtime::session_lifecycle::SessionLifecycleConfig::default(),
                )),
                None,
                runtime::SessionRecoveryConfig::default(),
            ));
            let session_service = Arc::new(SessionService::new_unbound(
                Arc::clone(&runtime),
                Arc::clone(&session_activation),
            ));
            session_runtime_port.bind(&session_service)?;
            let (session_workers, session_supervisor) = TestSessionWorkerDriver::start(
                Arc::clone(&runtime),
                Arc::clone(&session_service),
                Arc::clone(&event_bus),
            )?;
            session_service.install_supervisor(Arc::clone(&session_supervisor))?;
            let growth_projection_services = crate::services::GrowthProjectionServices::selected(
                None,
                selected_storage.as_ref(),
            )?;
            let services = Arc::new(GatewayServices::new_with_bound_session_and_storage(
                runtime,
                session_service,
                Arc::new(crate::surface_host::SurfaceHost::baseline()?),
                None,
                session_activation,
                session_supervisor,
                &config_home,
                runtime::GatewayCapacityConfig::default(),
                selected_storage,
                growth_projection_services,
            ));
            let profiles = Arc::new(ProfileManager::new_with_profiles_dir(
                config_home.join("profiles"),
            ));
            profiles.initialize().map_err(|error| error.to_string())?;

            Ok(Self {
                state: Arc::new(AppState {
                    tool_registry: Arc::new(ToolCatalog::builtin()),
                    config: None,
                    static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
                    auth_token,
                    workspace_root,
                    config_home,
                    profile_id: "default".to_string(),
                    profile_manager: profiles,
                    services,
                    session_lease_registry: Some(
                        Arc::new(session::SessionLeaseRegistry::default()),
                    ),
                    live_registry: Arc::new(live_routes::LiveRegistry::new()),
                }),
                session_workers: Arc::new(session_workers),
            })
        }

        pub fn router(&self) -> Router {
            api_router(Arc::clone(&self.state))
                .layer(axum::Extension(Arc::clone(&self.session_workers)))
        }

        /// Seed one terminal Surface-to-Runtime handoff through the production
        /// ledger APIs. Black-box tests use this only to establish a durable
        /// dead-letter precondition that no public ingress route can create
        /// deterministically; their assertions still traverse `api_router`.
        pub fn seed_dead_letter_trigger_event(
            &self,
            surface: &str,
            event_id: &str,
        ) -> Result<String, String> {
            let normalized_surface = surface::normalize_surface_id(surface);
            let payload = serde_json::json!({
                "event_id": event_id,
                "surface": normalized_surface,
                "fixture": "gateway-test-support",
            });
            let trigger = harness_contract::managed_agent::ManagedAgentTriggerEvent {
                event_id: event_id.to_string(),
                source_id: normalized_surface.clone(),
                source_kind: "surface".to_string(),
                event_type: "fixture.trigger".to_string(),
                subject: format!("fixture:{event_id}"),
                payload_ref: format!("surface://{normalized_surface}/events/{event_id}"),
                payload_digest: format!("sha256:{event_id}"),
                occurred_at_ms: 1,
                source_sequence: Some(1),
                idempotency_key: format!("surface:{normalized_surface}:{event_id}"),
                source_capabilities: vec!["surface.event.receive".to_string()],
                attributes: std::collections::BTreeMap::new(),
                trace_refs: vec!["test-support".to_string()],
            };
            let receipt = self.state.services.surface.record_trigger_event_received(
                &normalized_surface,
                "fixture.trigger",
                &trigger,
                &payload,
            )?;
            let key = receipt.record.idempotency_key;
            for _ in 0..receipt.record.max_attempts {
                self.state
                    .services
                    .surface
                    .mark_trigger_event_dispatching(&key)?
                    .ok_or_else(|| "fixture trigger event was not dispatchable".to_string())?;
                let updated = self
                    .state
                    .services
                    .surface
                    .mark_trigger_event_failed(&key, "fixture delivery failure")?;
                if updated.status == "dead_letter" {
                    return Ok(key);
                }
            }
            Err("fixture trigger event did not reach dead_letter".to_string())
        }
    }

    fn unique_test_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("cowd-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("gateway test root should be writable");
        root
    }
}

// ── Router ─────────────────────────────────────────────────────

pub fn api_router(state: Arc<AppState>) -> Router {
    let public_routes = public_routes::router();

    let (dynamic_app_routes, static_app_routes) = state
        .services
        .app_platform
        .as_ref()
        .map(|platform| {
            (
                app_routes::router(Arc::clone(platform)),
                app_routes::static_router(Arc::clone(platform)),
            )
        })
        .unwrap_or_default();
    let protected_routes = Router::new()
        .merge(dynamic_app_routes)
        .merge(approval_routes::router())
        .merge(agent_routes::router())
        .merge(audit_routes::router())
        .merge(message_connector_routes::router())
        .merge(connector_routes::router())
        .merge(context_routes::router())
        .merge(core_routes::router())
        .merge(cross_plane_routes::router())
        .merge(edge_routes::router())
        .merge(evolution_routes::router())
        .merge(growth_routes::router())
        .merge(harness_eval_routes::router())
        .merge(managed_agent_routes::router())
        .merge(matrix_routes::router())
        .merge(mission_routes::router())
        .merge(memory_routes::router())
        .merge(live_routes::router())
        .merge(message_routes::router())
        .merge(profile_routes::router())
        .merge(reality_routes::router())
        .merge(resource_routes::router())
        .merge(runtime_routes::router())
        .merge(session_routes::router())
        .merge(skill_routes::router())
        .merge(slash_routes::router())
        .merge(surface_routes::router())
        .merge(system_routes::router())
        .merge(task_routes::router())
        .merge(workspace_routes::router())
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    public_routes
        .merge(static_app_routes)
        .merge(protected_routes)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            capacity_middleware,
        ))
        .with_state(state)
}

#[cfg(feature = "test-support")]
pub(crate) fn route_contract_snapshots() -> (
    std::collections::BTreeSet<(String, String)>,
    std::collections::BTreeSet<(String, String)>,
) {
    let bindings = binding::GATEWAY_ROUTE_BINDINGS
        .iter()
        .map(|binding| {
            (
                binding.route.method().as_str().to_owned(),
                binding.route.path().template().to_owned(),
            )
        })
        .collect();
    let document = capability_contract::gateway_openapi_document();
    let methods = ["delete", "get", "patch", "post", "put"];
    let openapi = document["paths"]
        .as_object()
        .into_iter()
        .flat_map(|paths| paths.iter())
        .flat_map(|(path, item)| {
            methods.into_iter().filter_map(move |method| {
                item.get(method).is_some().then(|| {
                    (
                        method.to_ascii_uppercase(),
                        path.replace('{', ":").replace('}', ""),
                    )
                })
            })
        })
        .collect();
    (bindings, openapi)
}

async fn capacity_middleware(
    AxumState(state): AxumState<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    use crate::gateway_capacity::HttpCapacityLane;

    let path = request.uri().path();
    let lane = if is_stream_capacity_path(path) {
        HttpCapacityLane::Stream
    } else if is_control_capacity_path(path) {
        HttpCapacityLane::Control
    } else {
        HttpCapacityLane::Data
    };
    let lease = match state.services.capacity.admit_http(lane).await {
        Ok(lease) => lease,
        Err(overload) => {
            let retry_after_seconds = overload.retry_after_ms.div_ceil(1_000).max(1);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [
                    (header::RETRY_AFTER, retry_after_seconds.to_string()),
                    (
                        header::HeaderName::from_static("x-cowd-capacity-lane"),
                        format!("{:?}", overload.lane).to_ascii_lowercase(),
                    ),
                ],
                Json(serde_json::json!({
                    "error": "gateway_capacity_exhausted",
                    "lane": format!("{:?}", overload.lane).to_ascii_lowercase(),
                    "retry_after_ms": overload.retry_after_ms,
                })),
            )
                .into_response();
        }
    };
    let response = next.run(request).await;
    if lane == HttpCapacityLane::Stream {
        hold_capacity_lease_for_body(response, lease)
    } else {
        drop(lease);
        response
    }
}

fn hold_capacity_lease_for_body(
    response: Response,
    lease: crate::gateway_capacity::GatewayCapacityLease,
) -> Response {
    use futures::StreamExt;

    let (parts, body) = response.into_parts();
    let stream = Box::pin(body.into_data_stream());
    let guarded = futures::stream::unfold((stream, lease), |(mut stream, lease)| async move {
        stream.next().await.map(|item| (item, (stream, lease)))
    });
    Response::from_parts(parts, Body::from_stream(guarded))
}

fn is_stream_capacity_path(path: &str) -> bool {
    path.ends_with("/stream") || path.starts_with("/api/runtime/live/")
}

fn is_control_capacity_path(path: &str) -> bool {
    path == "/health"
        || path == "/healthz"
        || path == "/readyz"
        || path.contains("/cancel")
        || path.ends_with("/status")
        || path.starts_with("/api/approvals")
        || path.starts_with("/api/runtime/config")
}

#[cfg(test)]
mod capacity_middleware_tests {
    use super::*;
    use crate::gateway_capacity::HttpCapacityLane;

    #[tokio::test]
    async fn stream_body_owns_capacity_until_response_is_dropped() {
        let services = GatewayServices::baseline();
        let lease = services
            .capacity
            .admit_http(HttpCapacityLane::Stream)
            .await
            .unwrap();
        let response = hold_capacity_lease_for_body(Response::new(Body::from("event")), lease);
        assert_eq!(services.capacity.snapshot().stream.active, 1);
        drop(response);
        assert_eq!(services.capacity.snapshot().stream.active, 0);
    }

    #[test]
    fn routes_are_partitioned_without_treating_long_stream_as_data() {
        assert!(is_control_capacity_path("/healthz"));
        assert!(is_control_capacity_path("/api/runtime/turns/t-1/cancel"));
        assert!(is_stream_capacity_path("/api/runtime/live/subscription-1"));
        assert!(is_stream_capacity_path(
            "/api/apps/reference/operations/reference.events/stream"
        ));
        assert!(!is_stream_capacity_path("/api/runtime/events"));
    }
}

// ── Response types ─────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub(super) struct ErrorResponse {
    error: String,
}

fn default_config_home() -> PathBuf {
    std::env::var_os("COWD_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".cowd"))
        })
        .unwrap_or_else(|| PathBuf::from(".cowd"))
}

#[cfg(test)]
pub(crate) fn new_api_session_record(session_id: &str, model: Option<String>) -> SessionRecord {
    let now = chrono::Utc::now().to_rfc3339();
    let title = format!("Session {}", session_id.chars().take(8).collect::<String>());
    SessionRecord {
        session_id: session_id.to_string(),
        platform: "api_server".to_string(),
        chat_id: session_id.to_string(),
        user_id: None,
        model,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(serde_json::json!({ "title": title }).to_string()),
        input_tokens: 0,
        output_tokens: 0,
        status: "active".to_string(),
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn api_error(status: StatusCode, error: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: error.into(),
        }),
    )
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/mod.rs"]
pub(crate) mod tests;
