use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use super::{AppState, ErrorResponse};
use crate::services::ContextServiceError;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/context/current", get(context_current_handler))
        .route(
            "/api/context/:envelope_id",
            get(get_context_envelope_handler),
        )
        .route(
            "/api/sessions/:id/context",
            get(get_session_context_history),
        )
        .route(
            "/api/sessions/:id/context/recommendations",
            get(get_context_recommendation_stats).post(record_context_recommendation_action),
        )
        .route("/api/evidence/resolve", get(resolve_evidence_ref_handler))
        .route(
            "/api/evidence/resolve/batch",
            post(resolve_evidence_refs_batch_handler),
        )
        .route(
            "/api/evidence/projections",
            get(list_evidence_audit_projections),
        )
}

#[derive(Deserialize)]
struct ContextEventsParams {
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_envelopes: Option<bool>,
}

#[derive(Deserialize)]
struct GetRecommendationStatsParams {
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct ContextRecommendationActionRequest {
    envelope_id: String,
    recommendation: String,
    #[serde(default = "default_context_recommendation_action")]
    action: String,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Deserialize)]
struct EvidenceBatchResolveRequest {
    refs: Vec<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    snippet_bytes: Option<usize>,
}

fn default_context_recommendation_action() -> String {
    "acknowledged".to_string()
}

async fn context_current_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let requested_session_id = params
        .get("session_id")
        .cloned()
        .or_else(|| state.list_active_session_ids().into_iter().next());
    let active_envelope = requested_session_id.as_deref().and_then(|session_id| {
        state.services.runtime.as_ref().and_then(|runtime_service| {
            runtime_service.last_context_envelope_nonblocking(session_id)
        })
    });
    let query = params
        .get("q")
        .or_else(|| params.get("query"))
        .cloned()
        .unwrap_or_default();
    let reality_items = state
        .services
        .reality
        .recall_augmentation(
            &state.config_home,
            &state.services.matrix,
            &state.services.growth,
            &query,
            8,
        )
        .context_items;
    Json(
        state
            .services
            .context
            .current_context_projection(
                &state.services.memory,
                &state.services.connector,
                &state.workspace_root,
                active_envelope,
                requested_session_id,
                params,
                reality_items,
            )
            .await,
    )
}

async fn get_session_context_history(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<ContextEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(50).min(200);
    let include_envelopes = params.include_envelopes.unwrap_or(true);
    let value = state
        .services
        .context
        .context_history(
            &state.services.session,
            &id,
            from_seq,
            limit,
            include_envelopes,
        )
        .await
        .map_err(context_service_error)?;
    tracing::info!(
        session_id = id.as_str(),
        include_envelopes = include_envelopes,
        total = value["total"].as_u64().unwrap_or(0),
        from_seq = from_seq,
        limit = limit,
        "context history loaded"
    );
    Ok(Json(value))
}

async fn get_context_envelope_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(envelope_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let value = state
        .services
        .context
        .context_envelope(&state.services.session, &envelope_id)
        .await
        .map_err(context_service_error)?;
    tracing::info!(
        envelope_id = envelope_id.as_str(),
        "context envelope loaded"
    );
    Ok(Json(value))
}

async fn get_context_recommendation_stats(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetRecommendationStatsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(200).min(500);
    state
        .services
        .context
        .context_recommendation_stats(&state.services.session, &id, from_seq, limit)
        .await
        .map(Json)
        .map_err(context_service_error)
}

async fn resolve_evidence_ref_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(reference) = params
        .get("ref")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "ref query parameter is required".to_string(),
            }),
        ));
    };
    let session_id = params
        .get("session_id")
        .cloned()
        .or_else(|| state.list_active_session_ids().into_iter().next());
    let snippet_bytes = params
        .get("snippet_bytes")
        .and_then(|value| value.parse::<usize>().ok());

    state
        .services
        .context
        .resolve_evidence_ref_with_snippet(
            &state.services.session,
            &state.services.connector,
            &state.workspace_root,
            reference,
            session_id.as_deref(),
            snippet_bytes,
        )
        .await
        .map(Json)
        .map_err(context_service_error)
}

async fn resolve_evidence_refs_batch_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<EvidenceBatchResolveRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let refs = request
        .refs
        .into_iter()
        .map(|reference| reference.trim().to_string())
        .filter(|reference| !reference.is_empty())
        .collect::<Vec<_>>();
    if refs.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "at least one evidence ref is required".to_string(),
            }),
        ));
    }
    if refs.len() > 100 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "evidence batch is limited to 100 refs".to_string(),
            }),
        ));
    }
    let session_id = request
        .session_id
        .or_else(|| state.list_active_session_ids().into_iter().next());
    let mut items = Vec::with_capacity(refs.len());
    for reference in refs {
        let resolved = state
            .services
            .context
            .resolve_evidence_ref_with_snippet(
                &state.services.session,
                &state.services.connector,
                &state.workspace_root,
                &reference,
                session_id.as_deref(),
                request.snippet_bytes,
            )
            .await;
        match resolved {
            Ok(evidence) => items.push(serde_json::json!({
                "ref": reference,
                "status": if evidence.get("available").and_then(serde_json::Value::as_bool) == Some(false) { "unavailable" } else { "resolved" },
                "evidence": evidence,
            })),
            Err(error) => items.push(serde_json::json!({
                "ref": reference,
                "status": "unavailable",
                "error": error.to_string(),
            })),
        }
    }
    Ok(Json(serde_json::json!({
        "kind": "evidence_batch_projection",
        "count": items.len(),
        "items": items,
    })))
}

async fn list_evidence_audit_projections(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(session_id) = params
        .get("session_id")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "session_id query parameter is required".to_string(),
            }),
        ));
    };
    let from_sequence = params
        .get("from_seq")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        .min(500);
    state
        .services
        .context
        .evidence_audit_projections(&state.services.session, session_id, from_sequence, limit)
        .await
        .map(Json)
        .map_err(context_service_error)
}

async fn record_context_recommendation_action(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<ContextRecommendationActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    state
        .services
        .context
        .record_context_recommendation_action(
            &state.services.session,
            &id,
            body.envelope_id,
            body.recommendation,
            body.action,
            body.note,
        )
        .await
        .map(Json)
        .map_err(context_service_error)
}

fn context_service_error(error: ContextServiceError) -> (StatusCode, Json<ErrorResponse>) {
    let status = match &error {
        ContextServiceError::BadRequest(_) => StatusCode::BAD_REQUEST,
        ContextServiceError::NotFound(_) => StatusCode::NOT_FOUND,
        ContextServiceError::StoreUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        ContextServiceError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ErrorResponse {
            error: error.message(),
        }),
    )
}
