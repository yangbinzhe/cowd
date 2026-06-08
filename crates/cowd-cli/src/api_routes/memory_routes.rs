use std::{collections::HashMap, sync::Arc};

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
    MaintenanceScanConfig, MemoryKernel, MemoryScope, MemoryTurnContext, RotAlert,
    SearchMemoriesRequest,
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

fn context_health_json(alert: RotAlert) -> serde_json::Value {
    match alert {
        RotAlert::None => serde_json::json!({
            "level": "healthy",
            "message": null,
        }),
        RotAlert::Warning(message) => serde_json::json!({
            "level": "warning",
            "message": message,
        }),
        RotAlert::Critical(message) => serde_json::json!({
            "level": "critical",
            "message": message,
        }),
    }
}

fn memory_kernel_health_json(health: memory::MemoryHealth) -> serde_json::Value {
    let degraded_reasons: Vec<String> = health
        .degraded
        .iter()
        .map(|reason| format!("{reason:?}"))
        .collect();
    serde_json::json!({
        "degraded": health.is_degraded(),
        "degraded_reasons": degraded_reasons,
        "orientation_pressure": health.orientation_pressure,
        "conflict_pressure": health.conflict_pressure,
        "stale_pressure": health.stale_pressure,
        "evidence_coverage": health.evidence_coverage,
        "link_coverage": health.link_coverage,
        "background_lag_ms": health.background_lag_ms,
    })
}

async fn memory_status_value(state: &AppState) -> serde_json::Value {
    if let Some(ref mgr) = state.memory_manager {
        let layers = mgr.list_layers().await;
        let kernel = MemoryKernel::new(Arc::clone(mgr));
        let kernel_ctx = MemoryTurnContext::new("api-memory-status", "api");
        let kernel_health = kernel
            .health(&kernel_ctx)
            .await
            .map(memory_kernel_health_json)
            .unwrap_or_else(|error| {
                serde_json::json!({
                    "degraded": true,
                    "degraded_reasons": [format!("health failed: {error}")],
                    "orientation_pressure": 0.0,
                    "conflict_pressure": 0.0,
                    "stale_pressure": 0.0,
                    "evidence_coverage": 0.0,
                    "link_coverage": 0.0,
                    "background_lag_ms": null,
                })
            });
        let vector_count = mgr.vector_index_count();
        let total_entries: usize = layers
            .iter()
            .filter_map(|layer| layer.get("entry_count").and_then(|value| value.as_u64()))
            .map(|count| count as usize)
            .sum();
        serde_json::json!({
            "enabled": true,
            "status": "ready",
            "degraded": false,
            "degraded_reason": null,
            "layers": layers,
            "total_entries": total_entries,
            "vector_count": vector_count,
            "session_store": true,
            "context_health": context_health_json(mgr.ctx_health()),
            "kernel_health": kernel_health,
            "performance": mgr.performance_report(),
        })
    } else {
        serde_json::json!({
            "enabled": false,
            "status": "disabled",
            "degraded": false,
            "degraded_reason": "memory not configured",
            "layers": empty_memory_layers(),
            "total_entries": 0,
            "vector_count": 0,
            "session_store": false,
            "context_health": {
                "level": "unavailable",
                "message": "memory not configured",
            },
            "kernel_health": {
                "degraded": true,
                "degraded_reasons": ["memory not configured"],
                "orientation_pressure": 0.0,
                "conflict_pressure": 0.0,
                "stale_pressure": 0.0,
                "evidence_coverage": 0.0,
                "link_coverage": 0.0,
                "background_lag_ms": null,
            },
            "message": "memory not configured"
        })
    }
}

async fn memory_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(memory_status_value(&state).await)
}

async fn memory_status_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    Json(memory_status_value(&state).await)
}

async fn memory_stats_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
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
    if let Some(ref mgr) = state.memory_manager {
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

async fn performance_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    if let Some(ref mgr) = state.memory_manager {
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
    let Some(ref mgr) = state.memory_manager else {
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
    let Some(ref mgr) = state.memory_manager else {
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
    let Some(ref mgr) = state.memory_manager else {
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

    if let Some(ref mgr) = state.memory_manager {
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
    let Some(ref mgr) = state.memory_manager else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
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
        .unwrap_or_else(|| {
            if layer == MemoryLayer::L4 {
                MemoryScope::Global
            } else {
                MemoryScope::default()
            }
        });

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
    let kernel = MemoryKernel::new(Arc::clone(mgr));
    let memory_ctx = MemoryTurnContext::new("api-memory-create", "api");

    match kernel.remember(&memory_ctx, entry).await {
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
    let Some(ref mgr) = state.memory_manager else {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory not configured",
        ));
    };
    let memory_id = MemoryId::try_parse(&id)
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "invalid memory id"))?;
    let kernel = MemoryKernel::new(Arc::clone(mgr));
    let memory_ctx = MemoryTurnContext::new("api-memory-delete", "api");
    kernel
        .archive(&memory_ctx, memory_id, "archived by API delete request")
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn update_memory_entry_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateMemoryEntryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
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
    if let Some(ref mgr) = state.memory_manager {
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
    if let Some(ref mgr) = state.memory_manager {
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

async fn create_memory_symbol_link_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateSymbolLinkRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(ref mgr) = state.memory_manager else {
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
    let Some(ref mgr) = state.memory_manager else {
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
    Ok(Json(serde_json::json!({
        "enabled": true,
        "symbol": symbol,
        "entries": entries,
        "total": total,
    })))
}

async fn memory_search_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query = params.get("q").cloned().unwrap_or_default();
    if let Some(ref mgr) = state.memory_manager {
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

    let Some(ref mgr) = state.memory_manager else {
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

    let Some(ref mgr) = state.memory_manager else {
        return Json(serde_json::json!({
            "enabled": false,
            "query": query,
            "packet": null,
            "degraded": true,
            "degraded_reason": "memory not configured",
        }));
    };

    let mgr = Arc::clone(mgr);
    let query_for_packet = query.clone();
    let packet_result = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        rt.block_on(async move {
            let kernel = MemoryKernel::new(mgr);
            let ctx = MemoryTurnContext::new("api-memory-packet", "api");
            kernel
                .context_packet(&ctx, &query_for_packet, &[], max_items, max_tokens)
                .await
                .map_err(|error| error.to_string())
        })
    })
    .await
    .map_err(|error| error.to_string())
    .and_then(|result| result);

    match packet_result {
        Ok(packet) => Json(serde_json::json!({
            "enabled": true,
            "query": query,
            "packet": packet,
            "degraded": false,
            "degraded_reason": null,
        })),
        Err(error) => Json(serde_json::json!({
            "enabled": true,
            "query": query,
            "packet": null,
            "degraded": true,
            "degraded_reason": error,
        })),
    }
}

async fn memory_links_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let Some(ref mgr) = state.memory_manager else {
        return Json(serde_json::json!({
            "enabled": false,
            "links": [],
            "degraded": true,
            "degraded_reason": "memory not configured",
        }));
    };
    let kernel = MemoryKernel::new(Arc::clone(mgr));
    match kernel.links().await {
        Ok(links) => Json(serde_json::json!({
            "enabled": true,
            "links": links,
            "total": links.len(),
            "degraded": false,
            "degraded_reason": null,
        })),
        Err(error) => Json(serde_json::json!({
            "enabled": true,
            "links": [],
            "total": 0,
            "degraded": true,
            "degraded_reason": error.to_string(),
        })),
    }
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
