use std::{collections::HashMap, path::Path as FsPath, sync::Arc};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use memory::store::session::SessionEvent;
use memory::{MemoryKernel, MemoryTurnContext};
use runtime::{
    ContextAuthority, ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextOmission,
    ContextProfile, ContextRole, ContextRuntimeKernel, ContextSourceKind, ContextVisibility,
    ExternalResourceRef, SqliteResourceDirectory,
};
use serde::Deserialize;

use super::{AppState, ErrorResponse};

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

fn default_context_recommendation_action() -> String {
    "acknowledged".to_string()
}

async fn context_current_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let session_id = params
        .get("session_id")
        .cloned()
        .or_else(|| state.list_active_session_ids().into_iter().next())
        .unwrap_or_else(|| "api-context".to_string());
    let query = params.get("q").cloned().unwrap_or_default();
    let profile = params
        .get("profile")
        .and_then(|value| parse_context_profile(value))
        .unwrap_or(ContextProfile::MainTurn);

    if let Some(runtime_entry) = state.active_runtime(&session_id) {
        let runtime = runtime_entry.lock().await;
        if let Some(envelope) = runtime.last_context_envelope() {
            let lean_probe = ContextRuntimeKernel::lean_probe(&envelope);
            let policy_decision = ContextRuntimeKernel::policy_decision(&lean_probe);
            let mode_coverage = ContextRuntimeKernel::mode_coverage_report(
                envelope.identity.session_id.clone(),
                envelope.intent.clone(),
                envelope.assembled.stable_head.clone(),
                envelope.selected.clone(),
                envelope.budget.total_tokens,
            );
            let cache_stability =
                ContextRuntimeKernel::cache_stability_report(&envelope, &envelope);
            return Json(serde_json::json!({
                "enabled": true,
                "source": "runtime",
                "envelope": envelope,
                "lean_probe": lean_probe,
                "policy_decision": policy_decision,
                "cache_stability": cache_stability,
                "mode_coverage": mode_coverage,
            }));
        }
    }

    let mut identity = ContextIdentity::main(session_id.clone());
    identity.mode = ContextRuntimeKernel::mode_for_profile(profile);
    let mut dynamic_items = Vec::new();
    let mut omitted_items = Vec::new();
    let mut degraded = Vec::new();

    if let Some(ref mgr) = state.memory_manager {
        let mgr = Arc::clone(mgr);
        let session_for_packet = session_id.clone();
        let query_for_packet = query.clone();
        let packet_result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            rt.block_on(async move {
                let kernel = MemoryKernel::new(mgr);
                let memory_ctx = MemoryTurnContext::new(session_for_packet, "api");
                kernel
                    .context_packet(&memory_ctx, &query_for_packet, &[], 12, 2_000)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result);

        match packet_result {
            Ok(packet) => {
                for item in packet.selected {
                    let mut context_item = ContextItem::new(
                        item.atom.id.to_string(),
                        ContextSourceKind::Memory,
                        match item.role {
                            memory::MemoryPacketRole::Orientation => ContextRole::Orientation,
                            memory::MemoryPacketRole::Supporting => ContextRole::Evidence,
                            memory::MemoryPacketRole::Warning
                            | memory::MemoryPacketRole::Conflict => ContextRole::Warning,
                        },
                        format!(
                            "{}\nreason: {}\nevidence: {}",
                            item.atom.title,
                            item.reason,
                            item.atom.evidence_pointer.as_deref().unwrap_or("")
                        ),
                    );
                    context_item.authority = ContextAuthority::Session;
                    context_item.visibility = ContextVisibility::Private;
                    context_item.score = item.atom.confidence;
                    dynamic_items.push(context_item);
                }
                for omitted in packet.omitted {
                    omitted_items.push(ContextOmission {
                        source: ContextSourceKind::Memory,
                        reason: format!("{}: {}", omitted.reason, omitted.title),
                        token_estimate: 0,
                    });
                }
            }
            Err(_) => degraded.push(ContextSourceKind::Memory),
        }
    } else {
        degraded.push(ContextSourceKind::Memory);
    }

    dynamic_items.extend(resource_context_items(&state, &query));

    let mut envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
        profile,
        runtime_header: ContextRuntimeKernel::runtime_header(&identity, profile),
        identity,
        intent: query,
        stable_head: vec!["cowd-context-runtime:v0.8.13".to_string()],
        dynamic_items,
        omitted: omitted_items,
        total_budget_tokens: 8_000,
    });
    envelope.diagnostics.degraded_sources = degraded;
    let lean_probe = ContextRuntimeKernel::lean_probe(&envelope);
    let policy_decision = ContextRuntimeKernel::policy_decision(&lean_probe);
    let mode_coverage = ContextRuntimeKernel::mode_coverage_report(
        session_id,
        envelope.intent.clone(),
        envelope.assembled.stable_head.clone(),
        envelope.selected.clone(),
        envelope.budget.total_tokens,
    );
    let cache_stability = ContextRuntimeKernel::cache_stability_report(&envelope, &envelope);

    Json(serde_json::json!({
        "enabled": true,
        "source": "synthetic",
        "lean_probe": lean_probe,
        "policy_decision": policy_decision,
        "cache_stability": cache_stability,
        "mode_coverage": mode_coverage,
        "envelope": envelope,
    }))
}

fn parse_context_profile(value: &str) -> Option<ContextProfile> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mainturn" | "main" => Some(ContextProfile::MainTurn),
        "sologoal" | "solo" => Some(ContextProfile::SoloGoal),
        "yologoal" | "yolo" => Some(ContextProfile::YoloGoal),
        "subagent" | "sub_agent" => Some(ContextProfile::SubAgent),
        "collaboration" => Some(ContextProfile::Collaboration),
        "review" => Some(ContextProfile::Review),
        "resume" => Some(ContextProfile::Resume),
        "cron" => Some(ContextProfile::Cron),
        _ => None,
    }
}

fn context_envelope_event_json(event: SessionEvent) -> serde_json::Value {
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    let envelope = payload
        .get("envelope")
        .cloned()
        .unwrap_or_else(|| payload.clone());
    let envelope_id = payload
        .get("envelope_id")
        .cloned()
        .or_else(|| envelope.get("id").cloned())
        .unwrap_or(serde_json::Value::Null);
    let run_id = payload
        .get("run_id")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "session_id": event.session_id,
        "type": event.event_type,
        "sequence": event.sequence,
        "created_at_ms": event.created_at_ms,
        "envelope_id": envelope_id,
        "run_id": run_id,
        "envelope": envelope,
    })
}

fn context_envelope_summary_json(event: &serde_json::Value) -> serde_json::Value {
    let envelope = event
        .get("envelope")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let diagnostics = envelope
        .get("diagnostics")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    serde_json::json!({
        "session_id": event.get("session_id").cloned().unwrap_or(serde_json::Value::Null),
        "sequence": event.get("sequence").cloned().unwrap_or(serde_json::Value::Null),
        "created_at_ms": event.get("created_at_ms").cloned().unwrap_or(serde_json::Value::Null),
        "envelope_id": event.get("envelope_id").cloned().unwrap_or_else(|| envelope.get("id").cloned().unwrap_or(serde_json::Value::Null)),
        "run_id": event.get("run_id").cloned().unwrap_or(serde_json::Value::Null),
        "profile": envelope.get("profile").cloned().unwrap_or(serde_json::Value::Null),
        "intent": envelope.get("intent").cloned().unwrap_or(serde_json::Value::Null),
        "pressure_bp": diagnostics.get("pressure_bp").cloned().unwrap_or(serde_json::Value::Null),
        "selected_count": envelope.get("selected").and_then(|value| value.as_array()).map(|items| items.len()).unwrap_or(0),
        "omitted_count": envelope.get("omitted").and_then(|value| value.as_array()).map(|items| items.len()).unwrap_or(0),
    })
}

async fn get_session_context_history(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<ContextEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(50).min(200);
    let include_envelopes = params.include_envelopes.unwrap_or(true);
    let Some((total, stored_events)) = state
        .session_kernel
        .stored_events_by_type_page(&id, "ContextEnvelope", from_seq, limit)
        .await
        .map_err(|error| {
            tracing::warn!(
                session_id = %id,
                from_seq,
                limit,
                error = %error,
                "context history load failed"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load context timeline: {error}"),
                }),
            )
        })?
    else {
        tracing::warn!(
            session_id = %id,
            from_seq,
            limit,
            "context history store unavailable"
        );
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let envelope_events: Vec<serde_json::Value> = stored_events
        .into_iter()
        .map(context_envelope_event_json)
        .collect();
    let summaries: Vec<serde_json::Value> = envelope_events
        .iter()
        .map(context_envelope_summary_json)
        .collect();
    let next_seq = envelope_events
        .last()
        .and_then(|event| event["sequence"].as_u64())
        .map(|sequence| sequence as usize + 1);
    let has_more = envelope_events.len() < total;
    let returned_events = envelope_events.len();
    let summary_count = summaries.len();
    tracing::info!(
        session_id = %id,
        from_seq,
        limit,
        total,
        returned_events,
        summary_count,
        include_envelopes,
        ?next_seq,
        has_more,
        "context history loaded"
    );
    let envelopes = if include_envelopes {
        envelope_events
    } else {
        Vec::new()
    };

    Ok(Json(serde_json::json!({
        "session_id": id,
        "envelopes": envelopes,
        "summaries": summaries,
        "include_envelopes": include_envelopes,
        "total": total,
        "from_seq": from_seq,
        "next_seq": next_seq,
        "limit": limit,
        "has_more": has_more,
    })))
}

async fn get_context_envelope_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(envelope_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if !state.has_unified_store() {
        tracing::warn!(
            envelope_id = %envelope_id,
            "context envelope store unavailable"
        );
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    }

    let Some(event) = state
        .session_kernel
        .context_event_by_envelope_id(&envelope_id)
        .await
        .map_err(|error| {
            tracing::warn!(
                envelope_id = %envelope_id,
                error = %error,
                "context envelope load failed"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load context envelope: {error}"),
                }),
            )
        })?
    else {
        tracing::warn!(
            envelope_id = %envelope_id,
            "context envelope not found"
        );
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("context envelope {envelope_id} not found"),
            }),
        ));
    };

    tracing::info!(
        session_id = %event.session_id,
        envelope_id = %envelope_id,
        sequence = event.sequence,
        created_at_ms = event.created_at_ms,
        "context envelope loaded"
    );
    Ok(Json(serde_json::json!({
        "enabled": true,
        "source": "history",
        "context": context_envelope_event_json(event),
    })))
}

async fn get_context_recommendation_stats(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetRecommendationStatsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(200).min(500);
    let Some((total, stored_events)) = state
        .session_kernel
        .stored_events_by_type_page(&id, "ContextRecommendationAction", from_seq, limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load context recommendation stats: {error}"),
                }),
            )
        })?
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session store not available".to_string(),
            }),
        ));
    };

    let event_count = stored_events.len();
    let mut grouped: HashMap<String, serde_json::Value> = HashMap::new();
    for event in stored_events {
        let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        let Some(recommendation) = payload
            .get("recommendation")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let action = payload
            .get("action")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("acknowledged");
        let entry = grouped
            .entry(recommendation.to_string())
            .or_insert_with(|| {
                serde_json::json!({
                    "recommendation": recommendation,
                    "count": 0_u64,
                    "actions": {},
                    "latest_envelope_id": null,
                    "latest_created_at_ms": 0_u64,
                })
            });
        let count = entry["count"].as_u64().unwrap_or(0) + 1;
        entry["count"] = serde_json::json!(count);
        let action_count = entry["actions"][action].as_u64().unwrap_or(0) + 1;
        entry["actions"][action] = serde_json::json!(action_count);
        if event.created_at_ms >= entry["latest_created_at_ms"].as_u64().unwrap_or(0) {
            entry["latest_created_at_ms"] = serde_json::json!(event.created_at_ms);
            entry["latest_envelope_id"] = payload
                .get("envelope_id")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
        }
    }

    let mut recommendations: Vec<serde_json::Value> = grouped.into_values().collect();
    recommendations.sort_by(|left, right| {
        right["count"]
            .as_u64()
            .cmp(&left["count"].as_u64())
            .then_with(|| {
                left["recommendation"]
                    .as_str()
                    .cmp(&right["recommendation"].as_str())
            })
    });

    Ok(Json(serde_json::json!({
        "session_id": id,
        "recommendations": recommendations,
        "total": total,
        "from_seq": from_seq,
        "limit": limit,
        "has_more": event_count < total,
    })))
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

    let resolved = if let Some(path) = reference.strip_prefix("workspace://changed-file/") {
        resolve_workspace_evidence(&state.workspace_root, reference, path)
    } else if let Some(symbol) = reference.strip_prefix("workspace://symbol/") {
        serde_json::json!({
            "ref": reference,
            "kind": "workspace_symbol",
            "available": true,
            "symbol": symbol,
        })
    } else if let Some(session_ref) = reference.strip_prefix("session://") {
        resolve_session_evidence(&state, reference, session_ref).await
    } else if reference.starts_with("tool://") {
        resolve_tool_evidence(&state, reference, session_id.as_deref()).await
    } else if reference.starts_with("service://") || reference.starts_with("mcp://") {
        resolve_resource_evidence(&state, reference)
    } else if reference.starts_with("agent://") {
        serde_json::json!({
            "ref": reference,
            "kind": "agent",
            "available": false,
            "reason": "agent evidence payload drilldown is not persisted yet",
        })
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("unsupported evidence ref: {reference}"),
            }),
        ));
    };

    Ok(Json(resolved))
}

fn resource_context_items(state: &AppState, query: &str) -> Vec<ContextItem> {
    let path = super::connector_routes::resource_directory_path(&state.workspace_root);
    if !path.exists() {
        return Vec::new();
    }
    let Ok(directory) = SqliteResourceDirectory::open(path) else {
        return Vec::new();
    };
    let resources = if query.trim().is_empty() {
        directory.list_recent(5)
    } else {
        directory.search(query, 5)
    }
    .unwrap_or_default();

    resources.into_iter().map(resource_context_item).collect()
}

fn resource_context_item(resource: ExternalResourceRef) -> ContextItem {
    let mut content = format!(
        "resource: {}\nref: {}\nprovider: {}\ntype: {}\nindexed_state: {}",
        resource.title,
        resource.reference,
        resource.provider,
        resource.resource_type,
        resource.indexed_state
    );
    if matches!(resource.indexed_state.as_str(), "stale" | "degraded") {
        content.push_str("\nwarning: resource metadata may be stale or degraded; resolve evidence before relying on details");
    }
    let mut item = ContextItem::new(
        resource.reference.clone(),
        ContextSourceKind::Workspace,
        ContextRole::Evidence,
        content,
    );
    item.authority = ContextAuthority::Derived;
    item.visibility = ContextVisibility::Shared;
    item.score = if resource.indexed_state == "stale" {
        0.45
    } else {
        0.7
    };
    item.evidence = vec![resource.reference];
    item
}

fn resolve_resource_evidence(state: &AppState, reference: &str) -> serde_json::Value {
    let path = super::connector_routes::resource_directory_path(&state.workspace_root);
    if !path.exists() {
        return serde_json::json!({
            "ref": reference,
            "kind": "resource",
            "available": false,
            "reason": "resource directory is not initialized",
        });
    }
    match SqliteResourceDirectory::open(path).and_then(|directory| directory.get(reference)) {
        Ok(Some(resource)) => serde_json::json!({
            "ref": reference,
            "kind": "resource",
            "available": true,
            "resource": resource,
            "body": null,
            "reason": "resource evidence resolves metadata only; fetch/read must go through connector capability",
        }),
        Ok(None) => serde_json::json!({
            "ref": reference,
            "kind": "resource",
            "available": false,
            "reason": "resource ref not found",
        }),
        Err(error) => serde_json::json!({
            "ref": reference,
            "kind": "resource",
            "available": false,
            "reason": format!("resource lookup failed: {error}"),
        }),
    }
}

fn resolve_workspace_evidence(root: &FsPath, reference: &str, relative: &str) -> serde_json::Value {
    const MAX_BYTES: u64 = 256 * 1024;
    const PREVIEW_BYTES: usize = 4096;

    let path = root.join(relative);
    let Ok(canonical_root) = root.canonicalize() else {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "workspace root unavailable",
        });
    };
    let Ok(canonical_path) = path.canonicalize() else {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "file unavailable",
        });
    };
    if !canonical_path.starts_with(&canonical_root) {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "file is outside workspace",
        });
    }
    let Ok(metadata) = std::fs::metadata(&canonical_path) else {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "file metadata unavailable",
        });
    };
    if !metadata.is_file() {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": false,
            "reason": "path is not a file",
        });
    }
    if metadata.len() > MAX_BYTES {
        return serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": true,
            "truncated": true,
            "size_bytes": metadata.len(),
            "reason": "file exceeds preview limit",
        });
    }
    let preview = std::fs::read_to_string(&canonical_path)
        .map(|content| content.chars().take(PREVIEW_BYTES).collect::<String>())
        .unwrap_or_default();
    serde_json::json!({
        "ref": reference,
        "kind": "workspace_file",
        "available": true,
        "path": relative,
        "size_bytes": metadata.len(),
        "preview": preview,
        "truncated": metadata.len() as usize > PREVIEW_BYTES,
    })
}

async fn resolve_session_evidence(
    state: &AppState,
    reference: &str,
    session_ref: &str,
) -> serde_json::Value {
    let session_id = session_ref.split('/').next().unwrap_or_default();
    if session_id.is_empty() {
        return serde_json::json!({
            "ref": reference,
            "kind": "session",
            "available": false,
            "reason": "missing session id",
        });
    }
    match state.session_kernel.stored_session(session_id).await {
        Ok(Some(session)) => serde_json::json!({
            "ref": reference,
            "kind": "session",
            "available": true,
            "session": {
                "session_id": session.session_id,
                "platform": session.platform,
                "model": session.model,
                "created_at": session.created_at,
                "last_activity": session.last_activity,
                "message_count": session.message_count,
                "status": session.status,
            },
        }),
        Ok(None) => serde_json::json!({
            "ref": reference,
            "kind": "session",
            "available": false,
            "reason": "session not found",
        }),
        Err(error) => serde_json::json!({
            "ref": reference,
            "kind": "session",
            "available": false,
            "reason": format!("session lookup failed: {error}"),
        }),
    }
}

async fn resolve_tool_evidence(
    state: &AppState,
    reference: &str,
    session_id: Option<&str>,
) -> serde_json::Value {
    let Some(session_id) = session_id else {
        return serde_json::json!({
            "ref": reference,
            "kind": "tool",
            "available": false,
            "reason": "session_id is required for tool evidence",
        });
    };
    let tool_id = reference
        .strip_prefix("tool://")
        .and_then(|tail| tail.split('/').next())
        .unwrap_or_default();
    let Some((_, events)) = state
        .session_kernel
        .stored_events_page(session_id, 0, 500)
        .await
        .ok()
        .flatten()
    else {
        return serde_json::json!({
            "ref": reference,
            "kind": "tool",
            "available": false,
            "reason": "session events unavailable",
        });
    };
    let matches = events
        .into_iter()
        .filter_map(|event| {
            let payload = serde_json::from_str::<serde_json::Value>(&event.event_json).ok()?;
            let id_matches = payload
                .get("id")
                .and_then(|value| value.as_str())
                .is_some_and(|id| id == tool_id)
                || payload
                    .get("tool_use_id")
                    .and_then(|value| value.as_str())
                    .is_some_and(|id| id == tool_id);
            id_matches.then(|| {
                serde_json::json!({
                    "type": event.event_type,
                    "sequence": event.sequence,
                    "created_at_ms": event.created_at_ms,
                    "payload": payload,
                })
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "ref": reference,
        "kind": "tool",
        "available": !matches.is_empty(),
        "session_id": session_id,
        "events": matches,
    })
}

async fn record_context_recommendation_action(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<ContextRecommendationActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if body.envelope_id.trim().is_empty() || body.recommendation.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "envelope_id and recommendation are required".to_string(),
            }),
        ));
    }
    let action = if body.action.trim().is_empty() {
        "acknowledged".to_string()
    } else {
        body.action
    };
    let payload = serde_json::json!({
        "type": "ContextRecommendationAction",
        "session_id": id.clone(),
        "envelope_id": body.envelope_id,
        "recommendation": body.recommendation,
        "action": action,
        "note": body.note,
    });
    state
        .session_kernel
        .append_timeline_event(&id, "ContextRecommendationAction", payload.clone())
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to record context recommendation action: {error}"),
                }),
            )
        })?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "session_id": id,
        "event": payload,
    })))
}
