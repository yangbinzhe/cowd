use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use axum::{
    extract::{Path, State as AxumState},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use runtime::{
    CrossPlaneAction, CrossPlaneControlPlane, CrossPlaneGrant, CrossPlaneIdentityBinding,
};

use super::AppState;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/cross-plane/summary", get(cross_plane_summary_handler))
        .route(
            "/api/cross-plane/identities",
            get(cross_plane_identities_handler).post(cross_plane_create_identity_handler),
        )
        .route(
            "/api/cross-plane/identities/:id",
            delete(cross_plane_revoke_identity_handler),
        )
        .route(
            "/api/cross-plane/grants",
            get(cross_plane_grants_handler).post(cross_plane_create_grant_handler),
        )
        .route(
            "/api/cross-plane/grants/:id",
            delete(cross_plane_revoke_grant_handler),
        )
        .route("/api/cross-plane/audit", get(cross_plane_audit_handler))
        .route(
            "/api/cross-plane/policy/simulate",
            post(cross_plane_policy_simulate_handler),
        )
}

static CROSS_PLANE_CONTROL: OnceLock<CrossPlaneControlPlane> = OnceLock::new();

fn cross_plane_control() -> &'static CrossPlaneControlPlane {
    CROSS_PLANE_CONTROL.get_or_init(CrossPlaneControlPlane::new)
}

fn cross_plane_state_path(state: &AppState) -> PathBuf {
    state
        .config_home
        .join("cross-plane")
        .join("control-state.json")
}

fn ensure_cross_plane_loaded(state: &AppState) {
    static CROSS_PLANE_LOADED: OnceLock<()> = OnceLock::new();
    let _ = CROSS_PLANE_LOADED.get_or_init(|| {
        let _ = cross_plane_control().load_from_path(&cross_plane_state_path(state));
    });
}

fn save_cross_plane_state(state: &AppState) {
    let _ = cross_plane_control().save_to_path(&cross_plane_state_path(state));
}

async fn cross_plane_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let summary = cross_plane_control().summary(chrono::Utc::now());
    Json(serde_json::json!({
        "kind": "cross_plane_summary",
        "providers": [],
        "channels": [],
        "services": [],
        "identity_bindings": {
            "verified": summary.verified_identities,
            "claimed": summary.claimed_identities,
            "observed": summary.observed_identities,
            "unknown": 0
        },
        "grants": {
            "active": summary.active_grants,
            "expiring": 0,
            "expired": 0
        },
        "approvals": {
            "pending": 0
        },
        "interop": {
            "actions_24h": summary.audit_records,
            "allowed_24h": summary.allowed_actions,
            "denied_24h": summary.denied_actions,
            "approval_required_24h": summary.approval_required_actions
        }
    }))
}

async fn cross_plane_grants_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let grants = cross_plane_control().list_grants();
    Json(serde_json::json!({
        "kind": "cross_plane_grants",
        "grants": grants
    }))
}

async fn cross_plane_identities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let identities = cross_plane_control().list_identities();
    Json(serde_json::json!({
        "kind": "cross_plane_identities",
        "identities": identities
    }))
}

async fn cross_plane_create_identity_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(binding): Json<CrossPlaneIdentityBinding>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let binding = cross_plane_control().upsert_identity(binding);
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_identity",
        "identity": binding
    }))
}

async fn cross_plane_revoke_identity_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let revoked = cross_plane_control().revoke_identity(&id);
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_identity_revoked",
        "id": id,
        "revoked": revoked
    }))
}

async fn cross_plane_create_grant_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(grant): Json<CrossPlaneGrant>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let grant = cross_plane_control().upsert_grant(grant);
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_grant",
        "grant": grant
    }))
}

async fn cross_plane_revoke_grant_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let revoked = cross_plane_control().revoke_grant(&id);
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_grant_revoked",
        "id": id,
        "revoked": revoked
    }))
}

async fn cross_plane_audit_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let records = cross_plane_control().list_audit(100, 0);
    let total = records.len();
    Json(serde_json::json!({
        "kind": "cross_plane_audit",
        "records": records,
        "total": total
    }))
}

async fn cross_plane_policy_simulate_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(action): Json<CrossPlaneAction>,
) -> impl IntoResponse {
    ensure_cross_plane_loaded(&state);
    let decision = cross_plane_control().decide_and_audit(action.clone(), chrono::Utc::now());
    save_cross_plane_state(&state);
    Json(serde_json::json!({
        "kind": "cross_plane_policy_simulation",
        "action": action,
        "decision": decision,
    }))
}
