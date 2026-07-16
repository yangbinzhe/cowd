use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, patch},
    Json, Router,
};
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
use memory::{
    MaintenanceCandidateFilter, MaintenanceCandidateKind, MaintenanceCandidateStatus,
    MaintenanceScanConfig, MemoryScope, SearchMemoriesRequest,
};
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/memory", get(memory_handler))
        .route("/api/memory/status", get(memory_status_handler))
        .route("/api/memory/search", get(memory_search_handler))
        .route(
            "/api/memory/recall/explain",
            get(memory_recall_explain_handler),
        )
        .route("/api/memory/packet", get(memory_packet_handler))
        .route("/api/memory/links", get(memory_links_handler))
        .route("/api/memory/runtime", get(memory_runtime_handler))
        .route(
            "/api/memory/context-envelope",
            get(memory_context_envelope_handler),
        )
        .route(
            "/api/memory/context-envelope/:session_id",
            get(memory_context_envelope_session_handler),
        )
        .route("/api/memory/knowledge", get(memory_knowledge_handler))
        .route(
            "/api/memory/knowledge/health",
            get(memory_knowledge_health_handler),
        )
        .route(
            "/api/memory/knowledge/namespaces",
            get(memory_knowledge_namespaces_handler),
        )
        .route(
            "/api/memory/knowledge/conflicts",
            get(memory_knowledge_conflicts_handler),
        )
        .route(
            "/api/memory/knowledge/maintenance",
            get(memory_knowledge_maintenance_handler),
        )
        .route("/api/memory/clusters", get(memory_clusters_handler))
        .route("/api/memory/lifecycle/:id", get(memory_lifecycle_handler))
        .route("/api/memory/stats", get(memory_stats_handler))
        .route("/api/memory/layers", get(memory_layers_handler))
        .route(
            "/api/memory/maintenance",
            get(memory_maintenance_handler).post(scan_memory_maintenance_handler),
        )
        .route(
            "/api/memory/maintenance/:id",
            patch(update_memory_maintenance_handler),
        )
        .route("/api/memory/entities", get(memory_entities_handler))
        .route("/api/memory/triples", get(memory_triples_handler))
        .route("/api/memory/graph", get(memory_graph_handler))
        .route(
            "/api/memory/symbol-links",
            get(memory_symbol_links_handler).post(create_memory_symbol_link_handler),
        )
        .route("/api/memory/performance", get(performance_handler))
        .route(
            "/api/memory/:layer",
            get(memory_layer_handler).post(create_memory_entry_handler),
        )
        .route(
            "/api/memory/:layer/:id",
            delete(delete_memory_entry_handler),
        )
        .route("/api/memory/entry/:id", patch(update_memory_entry_handler))
}

pub(crate) async fn memory_status_value(state: &AppState) -> serde_json::Value {
    let mut status = state.services.memory.status_projection().await;
    let projection = memory_context_envelope_projection_value(state, None, 20).await;
    if let Some(object) = status.as_object_mut() {
        object.insert(
            "context_envelope_projection".to_string(),
            projection.clone(),
        );
        if let Some(capabilities) = object
            .get_mut("capabilities")
            .and_then(serde_json::Value::as_object_mut)
        {
            capabilities.insert(
                "context_envelope".to_string(),
                context_envelope_capability_from_projection(&projection),
            );
        }
    }
    status
}

async fn memory_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(memory_status_value(&state).await)
}

async fn memory_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(memory_status_value(&state).await)
}

#[derive(Debug, Deserialize)]
struct ContextEnvelopeQuery {
    #[serde(default)]
    limit: Option<usize>,
}

async fn memory_context_envelope_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<ContextEnvelopeQuery>,
) -> impl IntoResponse {
    Json(memory_context_envelope_projection_value(&state, None, query.limit.unwrap_or(20)).await)
}

async fn memory_context_envelope_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
    Query(query): Query<ContextEnvelopeQuery>,
) -> impl IntoResponse {
    Json(
        memory_context_envelope_projection_value(
            &state,
            Some(session_id.as_str()),
            query.limit.unwrap_or(20),
        )
        .await,
    )
}

pub(crate) async fn memory_context_envelope_projection_value(
    state: &AppState,
    session_id: Option<&str>,
    limit: usize,
) -> serde_json::Value {
    if !state.services.memory.is_available() {
        return serde_json::json!({
            "kind": "memory.context_envelope_projection",
            "status": "disabled",
            "enabled": false,
            "latest_envelope_id": null,
            "latest_session_id": null,
            "latest_event_id": null,
            "latest_checkpoint_id": null,
            "last_written_at": null,
            "last_restored_at": null,
            "token_budget": 0,
            "used_tokens": 0,
            "used_ratio": 0.0,
            "pressure_bp": 0,
            "compression_threshold": 0.70,
            "compression_status": "degraded",
            "recall_quality_status": "disabled",
            "selected_count": 0,
            "omitted_count": 0,
            "protected_count": 0,
            "omission_reasons": [],
            "restore_pointer": null,
            "degraded_reason": "memory not configured",
            "summaries": [],
            "events": [],
            "total": 0,
            "limit": limit.clamp(1, 100),
        });
    }
    state
        .services
        .context
        .context_envelope_projection(
            &state.services.session,
            session_id,
            &state.services.session.list_active_session_ids(),
            limit,
        )
        .await
}

pub(crate) fn context_envelope_capability_from_projection(
    projection: &serde_json::Value,
) -> serde_json::Value {
    let status = projection
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("degraded");
    let capability_status = match status {
        "ready" => "enabled_and_wired",
        "disabled" => "disabled",
        "degraded" => "degraded",
        _ => "configured_but_unwired",
    };
    serde_json::json!({
        "status": capability_status,
        "reason": projection
            .get("degraded_reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("ContextEnvelope projection is backed by persisted session events and Reality status"),
        "latest_envelope_id": projection.get("latest_envelope_id").cloned().unwrap_or(serde_json::Value::Null),
        "compression_status": projection.get("compression_status").cloned().unwrap_or(serde_json::Value::Null),
        "recall_quality_status": projection.get("recall_quality_status").cloned().unwrap_or(serde_json::Value::Null),
    })
}

async fn memory_stats_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(mgr) = state.services.memory.manager() {
        let layers = mgr.list_layers().await;
        let total_entries: usize = layers
            .iter()
            .filter_map(|layer| layer.get("entry_count").and_then(|value| value.as_u64()))
            .map(|count| count as usize)
            .sum();
        let entity_count = mgr
            .list_entities()
            .await
            .map(|value| value.len())
            .unwrap_or_default();
        let triple_count = mgr
            .list_triples()
            .await
            .map(|value| value.len())
            .unwrap_or_default();
        Json(serde_json::json!({
            "enabled": true,
            "total_entries": total_entries,
            "layers": layers,
            "entity_count": entity_count,
            "triple_count": triple_count,
            "vector_count": mgr.vector_index_count(),
            "performance": mgr.performance_report(),
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "total_entries": 0,
            "layers": empty_memory_layers(),
            "entity_count": 0,
            "triple_count": 0,
            "vector_count": 0,
        }))
    }
}

async fn memory_layers_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(mgr) = state.services.memory.manager() {
        Json(serde_json::json!({
            "enabled": true,
            "layers": mgr.list_layers().await,
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "layers": empty_memory_layers(),
        }))
    }
}

async fn memory_knowledge_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(
        state
            .services
            .memory
            .knowledge_projection(&state.config_home)
            .await,
    )
}

async fn memory_knowledge_health_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let projection = state
        .services
        .memory
        .knowledge_projection(&state.config_home)
        .await;
    Json(serde_json::json!({
        "enabled": projection.get("enabled").cloned().unwrap_or(serde_json::Value::Bool(false)),
        "degraded": projection.get("degraded").cloned().unwrap_or(serde_json::Value::Bool(true)),
        "degraded_reason": projection.get("degraded_reason").cloned().unwrap_or(serde_json::Value::Null),
        "health": projection
            .get("projection")
            .and_then(|value| value.get("health"))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    }))
}

async fn memory_knowledge_namespaces_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let projection = state
        .services
        .memory
        .knowledge_projection(&state.config_home)
        .await;
    Json(serde_json::json!({
        "enabled": projection.get("enabled").cloned().unwrap_or(serde_json::Value::Bool(false)),
        "namespace_tree": projection
            .pointer("/projection/namespace_tree")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
        "activation_policy_distribution": projection
            .pointer("/projection/activation_policy_distribution")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
        "governance_distribution": projection
            .pointer("/projection/governance_distribution")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    }))
}

async fn memory_knowledge_conflicts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let projection = state
        .services
        .memory
        .knowledge_projection(&state.config_home)
        .await;
    Json(serde_json::json!({
        "enabled": projection.get("enabled").cloned().unwrap_or(serde_json::Value::Bool(false)),
        "conflict_projection": projection
            .pointer("/projection/conflict_projection")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({
                "total": 0,
                "unresolved": 0,
                "conflicts": [],
            })),
    }))
}

async fn memory_knowledge_maintenance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let projection = state
        .services
        .memory
        .knowledge_projection(&state.config_home)
        .await;
    Json(serde_json::json!({
        "enabled": projection.get("enabled").cloned().unwrap_or(serde_json::Value::Bool(false)),
        "maintenance_candidates": projection
            .pointer("/projection/maintenance_candidates")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
        "recall_quality": projection
            .pointer("/projection/recall_quality")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
    }))
}

async fn performance_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(mgr) = state.services.memory.manager() {
        let report = mgr.performance_report();
        Json(serde_json::json!(report))
    } else {
        Json(serde_json::json!({
            "error": "memory not configured",
        }))
    }
}

fn empty_memory_layers() -> Vec<serde_json::Value> {
    ["L0", "L1", "L2", "L3", "L4"]
        .into_iter()
        .map(|layer| serde_json::json!({ "layer": layer, "entry_count": 0 }))
        .collect()
}

#[derive(Deserialize)]
struct CreateMemoryEntryRequest {
    content: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Deserialize)]
struct UpdateMemoryEntryRequest {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    priority: Option<String>,
}

#[derive(Deserialize)]
struct CreateSymbolLinkRequest {
    symbol_id: String,
    memory_id: String,
    #[serde(default)]
    turn_index: Option<i32>,
    #[serde(default)]
    reference_type: Option<String>,
}

#[derive(Deserialize)]
struct MemoryMaintenanceQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct MemoryMaintenanceScanRequest {
    #[serde(default)]
    stale_threshold: Option<f32>,
    #[serde(default)]
    low_confidence_threshold: Option<f32>,
    #[serde(default)]
    authority_confidence_threshold: Option<f32>,
    #[serde(default)]
    max_candidates: Option<usize>,
}

#[derive(Deserialize)]
struct UpdateMemoryMaintenanceRequest {
    status: String,
}

async fn memory_maintenance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MemoryMaintenanceQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(mgr) = state.services.memory.manager() else {
        return Ok(Json(serde_json::json!({
            "enabled": false,
            "candidates": [],
            "degraded_reason": "memory not configured",
        })));
    };
    let status = match query.status.as_deref() {
        Some(value) => Some(
            parse_maintenance_status(value)
                .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "invalid maintenance status"))?,
        ),
        None => None,
    };
    let kind = match query.kind.as_deref() {
        Some(value) => Some(
            parse_maintenance_kind(value)
                .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "invalid maintenance kind"))?,
        ),
        None => None,
    };
    let candidates = mgr
        .list_memory_maintenance(MaintenanceCandidateFilter {
            status,
            kind,
            source: query.source.filter(|source| !source.trim().is_empty()),
            limit: query.limit.map(|limit| limit.min(500)),
        })
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "enabled": true,
        "candidates": candidates,
    })))
}

async fn scan_memory_maintenance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<MemoryMaintenanceScanRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(mgr) = state.services.memory.manager() else {
        return Ok(Json(serde_json::json!({
            "enabled": false,
            "candidates": [],
            "degraded_reason": "memory not configured",
        })));
    };
    let defaults = MaintenanceScanConfig::default();
    let config = MaintenanceScanConfig {
        stale_threshold: body.stale_threshold.unwrap_or(defaults.stale_threshold),
        low_confidence_threshold: body
            .low_confidence_threshold
            .unwrap_or(defaults.low_confidence_threshold),
        authority_confidence_threshold: body
            .authority_confidence_threshold
            .unwrap_or(defaults.authority_confidence_threshold),
        max_candidates: body
            .max_candidates
            .unwrap_or(defaults.max_candidates)
            .min(500),
    };
    let candidates = mgr
        .scan_memory_maintenance(config)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(serde_json::json!({
        "enabled": true,
        "candidates": candidates,
    })))
}

async fn update_memory_maintenance_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateMemoryMaintenanceRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(mgr) = state.services.memory.manager() else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let status = parse_maintenance_status(&body.status)
        .ok_or_else(|| api_error(StatusCode::BAD_REQUEST, "invalid maintenance status"))?;
    match mgr.transition_memory_maintenance(&id, status) {
        Ok(Some(candidate)) => Ok(Json(serde_json::json!({
            "enabled": true,
            "candidate": candidate,
        }))),
        Ok(None) => Err(api_error(
            StatusCode::NOT_FOUND,
            "maintenance candidate not found",
        )),
        Err(error) => Err(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )),
    }
}

fn parse_maintenance_kind(kind: &str) -> Option<MaintenanceCandidateKind> {
    match kind.to_ascii_lowercase().as_str() {
        "conflict" => Some(MaintenanceCandidateKind::Conflict),
        "stale" => Some(MaintenanceCandidateKind::Stale),
        "duplicate" => Some(MaintenanceCandidateKind::Duplicate),
        "authoritypromotion" | "authority_promotion" => {
            Some(MaintenanceCandidateKind::AuthorityPromotion)
        }
        "relationshiprefresh" | "relationship_refresh" => {
            Some(MaintenanceCandidateKind::RelationshipRefresh)
        }
        _ => None,
    }
}

fn parse_maintenance_status(status: &str) -> Option<MaintenanceCandidateStatus> {
    match status.to_ascii_lowercase().as_str() {
        "open" => Some(MaintenanceCandidateStatus::Open),
        "acknowledged" | "ack" => Some(MaintenanceCandidateStatus::Acknowledged),
        "applied" => Some(MaintenanceCandidateStatus::Applied),
        "dismissed" | "dismiss" => Some(MaintenanceCandidateStatus::Dismissed),
        _ => None,
    }
}

async fn memory_layer_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(layer): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(layer) = parse_memory_layer(&layer) else {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid memory layer"));
    };
    if layer == MemoryLayer::L4 {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "L4 is promoted Runtime knowledge and cannot be created through the memory API",
        ));
    }

    if let Some(mgr) = state.services.memory.manager() {
        match mgr.list_layer_full_entries(layer).await {
            Ok(entries) => Ok(Json(serde_json::json!({
                "enabled": true,
                "layer": format!("{layer:?}"),
                "entries": entries,
            }))),
            Err(error) => Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
            )),
        }
    } else {
        Ok(Json(serde_json::json!({
            "enabled": false,
            "layer": format!("{layer:?}"),
            "entries": [],
        })))
    }
}

async fn create_memory_entry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(layer): Path<String>,
    Json(body): Json<CreateMemoryEntryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(layer) = parse_memory_layer(&layer) else {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid memory layer"));
    };
    if !state.services.memory.is_available() {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    }
    let content = body.content.trim();
    if content.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "memory content is required",
        ));
    }
    let category = body
        .category
        .as_deref()
        .and_then(parse_memory_category)
        .unwrap_or(MemoryCategory::Reference);
    let priority = body
        .priority
        .as_deref()
        .and_then(parse_memory_priority)
        .unwrap_or(Priority::Normal);
    let title = body
        .title
        .as_deref()
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(String::from)
        .unwrap_or_else(|| content.chars().take(64).collect());
    let scope = body
        .scope
        .as_deref()
        .and_then(|scope| scope.parse::<MemoryScope>().ok())
        .unwrap_or_default();

    let id = MemoryId::new_v4();
    let entry = MemoryEntry {
        id,
        layer,
        category,
        priority,
        source: MemorySource::UserExplicit,
        title: title.clone(),
        content: content.to_string(),
        embedding: None,
        tags: body.tags,
        relations: vec![],
        confidence: 1.0,
        access_count: 0,
        staleness: 0.0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        last_accessed_at: None,
        scope,
        session_id: None,
        source_agent: None,
        visibility: AgentVisibility::Shared,
    };
    match state.services.memory.remember_entry(entry).await {
        Ok(()) => Ok((
            StatusCode::CREATED,
            Json(serde_json::json!({
                "id": id,
                "layer": format!("{layer:?}"),
                "title": title,
            })),
        )),
        Err(error) => Err(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )),
    }
}

async fn delete_memory_entry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path((_layer, id)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.memory.is_available() {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    }
    let memory_id = MemoryId::try_parse(&id)
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "invalid memory id"))?;
    state
        .services
        .memory
        .archive_entry(memory_id)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))
}

async fn update_memory_entry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateMemoryEntryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(mgr) = state.services.memory.manager() else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };

    let content = body
        .content
        .map(|content| content.trim().to_string())
        .filter(|content| !content.is_empty());
    let priority = body.priority.as_deref().and_then(parse_memory_priority);

    if content.is_none() && body.tags.is_none() && priority.is_none() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "content, tags, or priority is required",
        ));
    }

    mgr.update_entry(&id, content, body.tags, priority)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(Json(serde_json::json!({
        "id": id,
        "updated": true,
    })))
}

async fn memory_entities_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(mgr) = state.services.memory.manager() {
        let entities = mgr.list_entities().await.unwrap_or_default();
        Json(serde_json::json!({
            "enabled": true,
            "entities": entities,
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "entities": [],
        }))
    }
}

async fn memory_triples_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(mgr) = state.services.memory.manager() {
        let triples = mgr.list_triples().await.unwrap_or_default();
        Json(serde_json::json!({
            "enabled": true,
            "triples": triples,
        }))
    } else {
        Json(serde_json::json!({
            "enabled": false,
            "triples": [],
        }))
    }
}

#[derive(Debug, Deserialize)]
struct MemoryGraphQuery {
    #[serde(default)]
    focus: Option<String>,
    #[serde(default)]
    depth: Option<usize>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<usize>,
}

async fn memory_graph_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MemoryGraphQuery>,
) -> impl IntoResponse {
    let Some(mgr) = state.services.memory.manager() else {
        return Json(serde_json::json!({
            "kind": "memory.knowledge_subgraph",
            "schema_version": "memory.knowledge_subgraph.v1",
            "enabled": false,
            "entities": [],
            "triples": [],
            "truncated": false,
            "next_cursor": null,
            "degraded_reason": "memory not configured",
        }));
    };
    let mut entities = mgr.list_entities().await.unwrap_or_default();
    let mut triples = mgr.list_triples().await.unwrap_or_default();
    entities.sort_by(|left, right| left.id.cmp(&right.id));
    triples.sort_by(|left, right| left.id.cmp(&right.id));

    let focus = query.focus.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let depth = query.depth.unwrap_or(2).clamp(1, 6);
    let filter = query
        .filter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);
    let mut included: HashSet<String> = if let Some(focus) = focus {
        let normalized = focus.to_ascii_lowercase();
        entities
            .iter()
            .filter(|entity| {
                entity.id.eq_ignore_ascii_case(focus)
                    || entity.name.to_ascii_lowercase().contains(&normalized)
            })
            .map(|entity| entity.id.clone())
            .collect()
    } else {
        entities.iter().map(|entity| entity.id.clone()).collect()
    };

    if focus.is_some() {
        let mut frontier = included.clone();
        for _ in 0..depth {
            let mut next = HashSet::new();
            for triple in &triples {
                if frontier.contains(&triple.subject_id) && !included.contains(&triple.object_id) {
                    next.insert(triple.object_id.clone());
                }
                if frontier.contains(&triple.object_id) && !included.contains(&triple.subject_id) {
                    next.insert(triple.subject_id.clone());
                }
            }
            if next.is_empty() {
                break;
            }
            included.extend(next.iter().cloned());
            frontier = next;
        }
    }

    entities.retain(|entity| {
        included.contains(&entity.id)
            && filter.as_ref().map_or(true, |filter| {
                entity.id.to_ascii_lowercase().contains(filter)
                    || entity.name.to_ascii_lowercase().contains(filter)
                    || entity.entity_type.to_string().to_ascii_lowercase().contains(filter)
            })
    });
    let total_entities = entities.len();
    let cursor = query.cursor.unwrap_or(0).min(total_entities);
    let limit = query.limit.unwrap_or(80).clamp(1, 200);
    let end = cursor.saturating_add(limit).min(total_entities);
    let page_entities = &entities[cursor..end];
    let page_ids: HashSet<&str> = page_entities.iter().map(|entity| entity.id.as_str()).collect();
    let edge_limit = limit.saturating_mul(4);
    let mut page_triples = triples
        .into_iter()
        .filter(|triple| {
            page_ids.contains(triple.subject_id.as_str()) && page_ids.contains(triple.object_id.as_str())
        })
        .collect::<Vec<_>>();
    let edge_truncated = page_triples.len() > edge_limit;
    page_triples.truncate(edge_limit);
    let truncated = end < total_entities || edge_truncated;

    Json(serde_json::json!({
        "kind": "memory.knowledge_subgraph",
        "schema_version": "memory.knowledge_subgraph.v1",
        "enabled": true,
        "focus": focus,
        "depth": depth,
        "filter": filter,
        "limit": limit,
        "cursor": cursor,
        "total_entities": total_entities,
        "entities": page_entities,
        "triples": page_triples,
        "truncated": truncated,
        "edge_truncated": edge_truncated,
        "next_cursor": (end < total_entities).then_some(end),
        "degraded_reason": null,
    }))
}

async fn create_memory_symbol_link_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateSymbolLinkRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(mgr) = state.services.memory.manager() else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let symbol_id = body.symbol_id.trim();
    if symbol_id.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "symbol_id is required"));
    }
    let memory_id = body
        .memory_id
        .parse::<uuid::Uuid>()
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "memory_id must be a valid UUID"))?;
    let reference_type = body
        .reference_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("reference");

    mgr.link_symbol_to_memory(symbol_id, memory_id, body.turn_index, reference_type)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "symbol_id": symbol_id,
            "memory_id": memory_id,
            "reference_type": reference_type,
        })),
    ))
}

async fn memory_symbol_links_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(mgr) = state.services.memory.manager() else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let symbol = params
        .get("symbol")
        .or_else(|| params.get("q"))
        .map(String::as_str)
        .unwrap_or("")
        .trim();
    if symbol.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "symbol query is required",
        ));
    }

    let entries = mgr
        .find_memories_by_symbol(symbol)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let total = entries.len();
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(80)
        .clamp(1, 200);
    let cursor = params
        .get("cursor")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
        .min(total);
    let end = cursor.saturating_add(limit).min(total);
    let truncated = end < total;
    let entries = entries.into_iter().skip(cursor).take(limit).collect::<Vec<_>>();
    Ok(Json(serde_json::json!({
        "enabled": true,
        "symbol": symbol,
        "entries": entries,
        "total": total,
        "limit": limit,
        "cursor": cursor,
        "truncated": truncated,
        "next_cursor": truncated.then_some(end),
    })))
}

async fn memory_search_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query = params.get("q").cloned().unwrap_or_default();
    if let Some(mgr) = state.services.memory.manager() {
        match mgr.search(&query).await {
            Ok(results) => Json(serde_json::json!({ "results": results })),
            Err(error) => Json(serde_json::json!({ "error": error.to_string() })),
        }
    } else {
        Json(serde_json::json!({ "results": [] }))
    }
}

async fn memory_recall_explain_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query = params.get("q").cloned().unwrap_or_default();
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10)
        .clamp(1, 50);

    let Some(mgr) = state.services.memory.manager() else {
        return Json(serde_json::json!({
            "enabled": false,
            "query": query,
            "mode": "disabled",
            "degraded": true,
            "degraded_reason": "memory not configured",
            "total": 0,
            "results": [],
            "keywords": [],
            "categories": [],
        }));
    };

    let request = SearchMemoriesRequest {
        query: query.clone(),
        limit,
        with_snippets: true,
        with_keywords: true,
        ..Default::default()
    };

    match mgr.search_memories(request).await {
        Ok(result) => {
            let mode = result.search_mode.clone();
            let results: Vec<_> = result
                .entries
                .into_iter()
                .enumerate()
                .map(|(index, entry)| {
                    let snippet = result
                        .snippets
                        .get(index)
                        .and_then(|snippet| snippet.as_ref())
                        .map(|snippet| snippet.text.clone());
                    serde_json::json!({
                        "id": entry.id,
                        "title": entry.title,
                        "content": entry.content,
                        "source_layer": format!("{:?}", entry.layer),
                        "category": format!("{:?}", entry.category),
                        "priority": format!("{:?}", entry.priority),
                        "scope": entry.scope.to_string(),
                        "score": entry.confidence,
                        "mode": mode,
                        "snippet": snippet,
                        "tags": entry.tags,
                    })
                })
                .collect();
            Json(serde_json::json!({
                "enabled": true,
                "query": result.query,
                "mode": mode,
                "degraded": false,
                "degraded_reason": null,
                "total": result.total_matches,
                "results": results,
                "keywords": result.keywords,
                "categories": result.categories_found,
            }))
        }
        Err(error) => Json(serde_json::json!({
            "enabled": true,
            "query": query,
            "mode": mgr.search_mode_label(),
            "degraded": true,
            "degraded_reason": error.to_string(),
            "total": 0,
            "results": [],
            "keywords": [],
            "categories": [],
        })),
    }
}

async fn memory_packet_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let query = params.get("q").cloned().unwrap_or_default();
    let max_items = params
        .get("max_items")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(12)
        .clamp(1, 64);
    let max_tokens = params
        .get("max_tokens")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(2_000)
        .clamp(64, 32_000);

    Json(
        state
            .services
            .memory
            .packet_projection(query, max_items, max_tokens)
            .await,
    )
}

async fn memory_links_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    Json(state.services.memory.links_projection().await)
}

async fn memory_runtime_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    Json(state.services.memory.runtime_projection().await)
}

#[derive(Deserialize)]
struct MemoryClusterQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<usize>,
    #[serde(default)]
    focus: Option<String>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    depth: Option<usize>,
}

async fn memory_clusters_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MemoryClusterQuery>,
) -> Json<serde_json::Value> {
    let limit = query.limit.unwrap_or(24).clamp(1, 100);
    let cursor = query.cursor.unwrap_or(0).min(500);
    let fetch_limit = cursor.saturating_add(limit).saturating_add(1).min(501);
    let mut projection = state
        .services
        .memory
        .clusters_projection(fetch_limit)
        .await;
    let mut clusters = projection
        .get_mut("clusters")
        .and_then(serde_json::Value::as_array_mut)
        .map(std::mem::take)
        .unwrap_or_default();
    let needle = query
        .filter
        .as_deref()
        .or(query.focus.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);
    if let Some(needle) = &needle {
        clusters.retain(|cluster| cluster.to_string().to_ascii_lowercase().contains(needle));
    }
    let available = clusters.len();
    let start = cursor.min(available);
    let end = start.saturating_add(limit).min(available);
    let truncated = end < available;
    let page = clusters.into_iter().skip(start).take(limit).collect::<Vec<_>>();
    if let Some(object) = projection.as_object_mut() {
        object.insert("clusters".to_string(), serde_json::json!(page));
        object.insert("focus".to_string(), serde_json::json!(query.focus));
        object.insert("filter".to_string(), serde_json::json!(query.filter));
        object.insert("depth".to_string(), serde_json::json!(query.depth.unwrap_or(1)));
        object.insert("limit".to_string(), serde_json::json!(limit));
        object.insert("cursor".to_string(), serde_json::json!(start));
        object.insert("total".to_string(), serde_json::json!(available));
        object.insert("truncated".to_string(), serde_json::json!(truncated));
        object.insert(
            "next_cursor".to_string(),
            serde_json::json!(truncated.then_some(end)),
        );
    }
    Json(projection)
}

async fn memory_lifecycle_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.services.memory.is_available() {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    }
    let memory_id = MemoryId::try_parse(&id)
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "invalid memory id"))?;
    let projection = state
        .services
        .memory
        .lifecycle_projection(memory_id, id)
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(Json(projection))
}

fn parse_memory_layer(layer: &str) -> Option<MemoryLayer> {
    match layer.to_ascii_uppercase().as_str() {
        "L0" => Some(MemoryLayer::L0),
        "L1" => Some(MemoryLayer::L1),
        "L2" => Some(MemoryLayer::L2),
        "L3" => Some(MemoryLayer::L3),
        "L4" => Some(MemoryLayer::L4),
        _ => None,
    }
}

fn parse_memory_category(category: &str) -> Option<MemoryCategory> {
    match category.to_ascii_lowercase().as_str() {
        "userpreference" | "user_preference" => Some(MemoryCategory::UserPreference),
        "projectconvention" | "project_convention" => Some(MemoryCategory::ProjectConvention),
        "decision" => Some(MemoryCategory::Decision),
        "reference" => Some(MemoryCategory::Reference),
        "shared" => Some(MemoryCategory::Shared),
        "compressedsummary" | "compressed_summary" => Some(MemoryCategory::CompressedSummary),
        "projectknowledge" | "project_knowledge" => Some(MemoryCategory::ProjectKnowledge),
        _ => None,
    }
}

fn parse_memory_priority(priority: &str) -> Option<Priority> {
    match priority.to_ascii_lowercase().as_str() {
        "critical" => Some(Priority::Critical),
        "high" => Some(Priority::High),
        "normal" => Some(Priority::Normal),
        "low" => Some(Priority::Low),
        _ => None,
    }
}
