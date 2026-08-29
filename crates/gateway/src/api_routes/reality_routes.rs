use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use memory::RecallReport;

use super::{AppState, ErrorResponse};
use crate::services::reality_service::RealityFlowQuery;

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            surface::gateway_api::paths::API_REALITY_STATUS.template(),
            get(reality_status_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_CAPABILITIES.template(),
            get(reality_capabilities_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_STATIC.template(),
            get(reality_static_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_FLOW.template(),
            get(reality_flow_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_RECALL_REPORT.template(),
            get(reality_recall_report_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_CONTEXT_ENVELOPE.template(),
            get(reality_context_envelope_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_EVIDENCE_BY_ID.template(),
            get(reality_evidence_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_PROMOTIONS.template(),
            get(reality_promotions_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_GOVERNANCE.template(),
            get(reality_governance_handler),
        )
        .route(
            surface::gateway_api::paths::API_REALITY_BOUNDARIES.template(),
            get(reality_boundaries_handler),
        )
}

async fn reality_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state
            .services
            .reality
            .status_projection(
                &state.config_home,
                &state.services.memory,
                &state.services.matrix,
                &state.services.growth,
                &state.services.context,
                &state.services.session,
                &state.services.audit,
            )
            .await,
    )
}

async fn reality_capabilities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let status = state
        .services
        .reality
        .status_projection(
            &state.config_home,
            &state.services.memory,
            &state.services.matrix,
            &state.services.growth,
            &state.services.context,
            &state.services.session,
            &state.services.audit,
        )
        .await;
    Json(serde_json::json!({
        "kind": "reality.capabilities",
        "ok": status.get("ok").and_then(serde_json::Value::as_bool).unwrap_or(false),
        "generated_at": status.get("generated_at").cloned(),
        "envelope": state.services.reality.envelope("capabilities"),
        "capabilities": status.get("capabilities").cloned().unwrap_or_else(|| serde_json::json!({})),
        "engines": status.get("engines").cloned().unwrap_or_else(|| serde_json::json!({})),
    }))
}

async fn reality_static_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state
            .services
            .reality
            .static_projection(
                &state.config_home,
                &state.services.memory,
                &state.services.matrix,
                &state.services.growth,
                &state.services.context,
                &state.services.audit,
            )
            .await,
    )
}

async fn reality_flow_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let session_id = params
        .get("session_id")
        .filter(|value| !value.trim().is_empty())
        .cloned();
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(25)
        .min(200);
    Json(
        state
            .services
            .reality
            .flow_projection(
                &state.config_home,
                &state.services.growth,
                RealityFlowQuery { session_id, limit },
            )
            .await,
    )
}

async fn reality_recall_report_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let session_id = params
        .get("session_id")
        .cloned()
        .or_else(|| state.list_active_session_ids().into_iter().next())
        .unwrap_or_else(|| "api-reality-recall".to_string());
    let query = params
        .get("q")
        .or_else(|| params.get("query"))
        .cloned()
        .unwrap_or_else(|| "reality recall report".to_string());
    let max_items = params
        .get("max_items")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20)
        .clamp(1, 100);
    let max_tokens = params
        .get("max_tokens")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(4_000)
        .clamp(256, 64_000);

    let packet = state
        .services
        .memory
        .context_packet_preview(
            session_id.clone(),
            "api-reality-recall",
            query.clone(),
            max_items,
            max_tokens,
        )
        .await;

    Json(match packet {
        Ok(mut packet) => {
            state.services.reality.augment_recall_report(
                &state.config_home,
                &state.services.matrix,
                &state.services.growth,
                &query,
                max_items,
                &mut packet.recall_report,
            );
            serde_json::json!({
                "kind": "reality.recall_report",
                "ok": true,
                "envelope": state.services.reality.envelope("recall_report"),
                "session_id": session_id,
                "query": query,
                "selected_count": packet.recall_report.selected.len(),
                "omitted_count": packet.recall_report.omitted.len(),
                "recall_report": packet.recall_report,
                "packet_summary": {
                    "selected": packet.selected,
                    "omitted": packet.omitted,
                    "token_estimate": packet.token_estimate,
                    "truncated": packet.truncated,
                },
            })
        }
        Err(error) => {
            let mut recall_report = RecallReport::default();
            state.services.reality.augment_recall_report(
                &state.config_home,
                &state.services.matrix,
                &state.services.growth,
                &query,
                max_items,
                &mut recall_report,
            );
            serde_json::json!({
                "kind": "reality.recall_report",
                "ok": !recall_report.selected.is_empty(),
                "degraded": true,
                "degraded_sources": ["memory"],
                "envelope": state.services.reality.envelope("recall_report"),
                "session_id": session_id,
                "query": query,
                "memory_error": error,
                "selected_count": recall_report.selected.len(),
                "omitted_count": recall_report.omitted.len(),
                "recall_report": recall_report,
                "packet_summary": null,
            })
        }
    })
}

async fn reality_context_envelope_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let requested_session_id = params
        .get("session_id")
        .cloned()
        .or_else(|| state.list_active_session_ids().into_iter().next());
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
    let active_envelope = requested_session_id.as_deref().and_then(|session_id| {
        state.services.runtime.as_ref().and_then(|runtime_service| {
            runtime_service.last_context_envelope_nonblocking(session_id)
        })
    });
    let projection = state
        .services
        .context
        .current_context_projection(
            &state.services.memory,
            &state.services.connector,
            &state.workspace_root,
            active_envelope,
            requested_session_id.clone(),
            params,
            reality_items,
        )
        .await;
    Json(serde_json::json!({
        "kind": "reality.context_envelope",
        "ok": projection.get("enabled").and_then(serde_json::Value::as_bool).unwrap_or(false),
        "envelope": state.services.reality.envelope("context_envelope"),
        "session_id": requested_session_id,
        "projection": projection,
    }))
}

async fn reality_evidence_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let reference = params
        .get("ref")
        .map(String::as_str)
        .unwrap_or(id.as_str())
        .trim();
    if reference.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "evidence id or ref is required".to_string(),
            }),
        ));
    }

    if let Ok(Some(packet)) = state
        .services
        .matrix
        .get_evidence_packet(&state.config_home, reference)
    {
        return Ok(Json(serde_json::json!({
            "kind": "reality.evidence",
            "ok": true,
            "envelope": state.services.reality.envelope("evidence"),
            "ref": reference,
            "source": "matrix",
            "evidence": packet,
        })));
    }

    match state.services.growth.fact_evidence(reference) {
        Ok(Some(packet)) => {
            return Ok(Json(serde_json::json!({
                "kind": "reality.evidence",
                "ok": true,
                "envelope": state.services.reality.envelope("evidence"),
                "ref": reference,
                "source": "fact-ledger",
                "evidence": packet,
            })));
        }
        Ok(None) => {}
        Err(error) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("fact evidence ledger unavailable: {error}"),
                }),
            ));
        }
    }

    let session_id = params
        .get("session_id")
        .cloned()
        .or_else(|| state.list_active_session_ids().into_iter().next());
    let resolved = state
        .services
        .context
        .resolve_evidence_ref(
            &state.services.session,
            &state.services.connector,
            &state.workspace_root,
            reference,
            session_id.as_deref(),
        )
        .await
        .unwrap_or_else(|error| {
            serde_json::json!({
                "ref": reference,
                "available": false,
                "reason": error.message(),
            })
        });

    Ok(Json(serde_json::json!({
        "kind": "reality.evidence",
        "ok": resolved.get("available").and_then(serde_json::Value::as_bool).unwrap_or(false),
        "envelope": state.services.reality.envelope("evidence"),
        "ref": reference,
        "source": "context",
        "evidence": resolved,
    })))
}

async fn reality_promotions_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let session_id = params.get("session_id").map(String::as_str);
    let target = params.get("target").map(String::as_str);
    let status = params.get("status").map(String::as_str);
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        .min(500);
    Json(state.services.reality.promotions_projection(
        &state.config_home,
        &state.services.growth,
        session_id,
        target,
        status,
        limit,
    ))
}

async fn reality_governance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let status = state
        .services
        .reality
        .status_projection(
            &state.config_home,
            &state.services.memory,
            &state.services.matrix,
            &state.services.growth,
            &state.services.context,
            &state.services.session,
            &state.services.audit,
        )
        .await;
    let boundaries = state
        .services
        .reality
        .boundaries_projection(&state.config_home, &state.services.growth);
    Json(serde_json::json!({
        "kind": "reality.governance",
        "ok": status.get("ok").and_then(serde_json::Value::as_bool).unwrap_or(false),
        "envelope": state.services.reality.envelope("governance"),
        "capabilities": status.get("capabilities").cloned().unwrap_or_else(|| serde_json::json!({})),
        "boundaries": boundaries,
        "knowledge": status.pointer("/engines/knowledge_fabric/projection").cloned().unwrap_or_else(|| serde_json::json!({})),
        "latest": status.get("latest").cloned().unwrap_or_else(|| serde_json::json!({})),
    }))
}

async fn reality_boundaries_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    Json(
        state
            .services
            .reality
            .boundaries_projection(&state.config_home, &state.services.growth),
    )
}
