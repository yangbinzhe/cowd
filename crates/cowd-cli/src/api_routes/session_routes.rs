use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use memory::store::session::{SessionEvent, SessionListOptions, SessionRecord};
use serde::{Deserialize, Serialize};

use super::{new_api_session_record, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/sessions", get(list_sessions).post(create_session))
        .route("/api/sessions/search", get(search_messages_handler))
        .route("/api/sessions/:id/ensure", post(ensure_session_handler))
        .route(
            "/api/sessions/:id",
            get(get_session)
                .patch(update_session_handler)
                .delete(delete_session),
        )
        .route("/api/sessions/:id/events", get(get_session_events))
        .route("/api/sessions/:id/runs", get(get_session_runs))
        .route("/api/sessions/:id/compact", post(compact_session_handler))
        .route("/api/sessions/:id/stats", get(get_session_stats_handler))
}

#[derive(Serialize)]
struct SessionInfo {
    id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_tokens: Option<i64>,
}

#[derive(Deserialize)]
struct CreateSessionRequest {
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
struct ListSessionsParams {
    #[serde(default = "default_sort")]
    sort: String,
    #[serde(default = "default_order")]
    order: String,
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

fn default_sort() -> String {
    "updated_at".to_string()
}

fn default_order() -> String {
    "desc".to_string()
}

fn default_session_model(state: &AppState) -> String {
    runtime::ConfigLoader::new(&state.workspace_root, &state.config_home)
        .load()
        .ok()
        .and_then(|config| config.model().map(str::to_string))
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string())
}

#[derive(Deserialize)]
struct GetEventsParams {
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct SearchMessagesParams {
    q: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

fn default_search_limit() -> usize {
    20
}

#[derive(Serialize)]
struct SearchMessagesItem {
    session_id: String,
    sequence: usize,
    role: String,
    blocks: Vec<serde_json::Value>,
    content_preview: String,
    tool_use_id: Option<String>,
    tool_name: Option<String>,
    created_at_ms: u64,
}

#[derive(Serialize)]
struct SearchMessagesResponse {
    query: String,
    results: Vec<SearchMessagesItem>,
    total: usize,
}

#[derive(Deserialize)]
struct UpdateSessionRequest {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

fn session_title_from_metadata(metadata_json: Option<&str>) -> Option<String> {
    metadata_json
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|value| {
            value
                .get("title")
                .and_then(|title| title.as_str())
                .map(ToString::to_string)
        })
}

fn session_info_from_record(record: SessionRecord) -> SessionInfo {
    SessionInfo {
        id: record.session_id,
        status: record.status,
        title: session_title_from_metadata(record.metadata_json.as_deref()),
        model: record.model,
        created_at: Some(record.created_at),
        updated_at: Some(record.last_activity),
        input_tokens: Some(record.input_tokens),
        output_tokens: Some(record.output_tokens),
    }
}

fn active_session_info(id: String) -> SessionInfo {
    SessionInfo {
        id,
        status: "active".to_string(),
        title: None,
        model: None,
        created_at: None,
        updated_at: None,
        input_tokens: None,
        output_tokens: None,
    }
}

async fn list_sessions(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<ListSessionsParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(20).min(200);
    let offset = params.offset.unwrap_or(0);

    if let Ok(Some(page)) = state
        .session_kernel
        .list_stored_sessions_page(&SessionListOptions {
            query: params.q.as_deref(),
            model: params.model.as_deref(),
            status: params.status.as_deref(),
            sort: &params.sort,
            order: &params.order,
            limit,
            offset,
        })
        .await
    {
        let total = page.total;
        let sessions: Vec<SessionInfo> = page
            .records
            .into_iter()
            .map(session_info_from_record)
            .collect();
        return Json(serde_json::json!({
            "sessions": sessions,
            "total": total,
            "offset": offset,
            "limit": limit,
            "sort": params.sort,
            "order": params.order,
        }));
    }

    let mut sessions: Vec<SessionInfo> = state
        .list_active_session_ids()
        .into_iter()
        .map(active_session_info)
        .collect();
    if let Some(status) = params.status.as_ref().filter(|value| !value.is_empty()) {
        sessions.retain(|session| session.status.eq_ignore_ascii_case(status));
    }
    if let Some(model) = params.model.as_ref().filter(|value| !value.is_empty()) {
        sessions.retain(|session| {
            session
                .model
                .as_deref()
                .is_some_and(|session_model| session_model.eq_ignore_ascii_case(model))
        });
    }
    if let Some(query) = params
        .q
        .as_ref()
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty())
    {
        sessions.retain(|session| {
            session.id.to_lowercase().contains(&query)
                || session
                    .title
                    .as_deref()
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&query)
        });
    }
    if params.sort == "created_at" {
        sessions.sort_by(|left, right| left.created_at.cmp(&right.created_at));
    } else {
        sessions.sort_by(|left, right| left.updated_at.cmp(&right.updated_at));
    }
    if !params.order.eq_ignore_ascii_case("asc") {
        sessions.reverse();
    }
    let total = sessions.len();
    let sessions: Vec<SessionInfo> = sessions.into_iter().skip(offset).take(limit).collect();
    Json(serde_json::json!({
        "sessions": sessions,
        "total": total,
        "offset": offset,
        "limit": limit,
        "sort": params.sort,
        "order": params.order,
    }))
}

async fn create_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let session_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(%session_id, "API session create requested");

    let session = runtime::Session::new();
    let model = body
        .model
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| default_session_model(&state));
    let runtime = if let Some(store) = state.unified_store() {
        crate::build_runtime_with_session_store(
            store.clone(),
            session,
            &session_id,
            model.clone(),
            vec![],
            true,
            true,
            None,
            runtime::PermissionMode::WorkspaceWrite,
            None,
            None,
        )
    } else {
        crate::build_runtime(
            session,
            &session_id,
            model.clone(),
            vec![],
            true,
            true,
            None,
            runtime::PermissionMode::WorkspaceWrite,
            None,
            None,
        )
    }
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("failed to build runtime: {error}"),
            }),
        )
    })?;

    if let Err(error) = state.register_runtime(session_id.clone(), runtime) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("failed to register session: {error}"),
            }),
        ));
    }

    let mut info = active_session_info(session_id.clone());
    if state.has_unified_store() {
        let record = new_api_session_record(&session_id, Some(model));
        state
            .session_kernel
            .upsert_stored_session(&record)
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to persist session: {error}"),
                    }),
                )
            })?;
        info = session_info_from_record(record);
    }

    Ok((StatusCode::CREATED, Json(info)))
}

async fn ensure_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "session id is required".to_string(),
            }),
        ));
    }

    let mut created = false;
    if state.active_runtime(&id).is_none() {
        let session = runtime::Session::new();
        let model = body
            .model
            .filter(|model| !model.trim().is_empty())
            .unwrap_or_else(|| default_session_model(&state));
        let runtime = if let Some(store) = state.unified_store() {
            crate::build_runtime_with_session_store(
                store.clone(),
                session,
                &id,
                model.clone(),
                vec![],
                true,
                true,
                None,
                runtime::PermissionMode::WorkspaceWrite,
                None,
                None,
            )
        } else {
            crate::build_runtime(
                session,
                &id,
                model.clone(),
                vec![],
                true,
                true,
                None,
                runtime::PermissionMode::WorkspaceWrite,
                None,
                None,
            )
        }
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to build runtime: {error}"),
                }),
            )
        })?;

        if let Err(error) = state.register_runtime(id.clone(), runtime) {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("failed to register session: {error}"),
                }),
            ));
        }
        if state.has_unified_store()
            && state
                .session_kernel
                .stored_session(&id)
                .await
                .ok()
                .flatten()
                .is_none()
        {
            let record = new_api_session_record(&id, Some(model));
            state
                .session_kernel
                .upsert_stored_session(&record)
                .await
                .map_err(|error| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: format!("failed to persist session: {error}"),
                        }),
                    )
                })?;
        }
        created = true;
    }

    Ok(Json(serde_json::json!({
        "ok": true,
        "session_id": id,
        "created": created,
        "active_sessions": state.list_active_session_ids().len(),
    })))
}

async fn get_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if state.has_unified_store() {
        match state.session_kernel.stored_session(&id).await {
            Ok(Some(record)) => return Ok(Json(session_info_from_record(record))),
            Ok(None) => {}
            Err(error) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to load session: {error}"),
                    }),
                ));
            }
        }
    }

    if state.active_runtime(&id).is_some() {
        Ok(Json(active_session_info(id)))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ))
    }
}

async fn delete_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let removed_active = state.remove_active_runtime(&id).is_some();
    let removed_stored = state
        .session_kernel
        .delete_stored_session(&id)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to delete session: {error}"),
                }),
            )
        })?;

    if removed_active || removed_stored {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ))
    }
}

async fn get_session_events(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(500);
    let Some((total, stored_events)) = state
        .session_kernel
        .stored_events_page(&id, from_seq, limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load session events: {error}"),
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
    let events: Vec<serde_json::Value> = stored_events
        .into_iter()
        .map(|event| {
            let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
                .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
            serde_json::json!({
                "session_id": event.session_id,
                "type": event.event_type,
                "sequence": event.sequence,
                "created_at_ms": event.created_at_ms,
                "payload": payload,
            })
        })
        .collect();
    let has_more = events.len() < total;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "events": events,
        "total": total,
        "from_seq": from_seq,
        "limit": limit,
        "has_more": has_more,
    })))
}

fn runtime_run_event_json(event: SessionEvent) -> serde_json::Value {
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    serde_json::json!({
        "session_id": event.session_id,
        "type": event.event_type,
        "sequence": event.sequence,
        "created_at_ms": event.created_at_ms,
        "run": payload,
    })
}

fn runtime_run_tree_summary(runs: &[serde_json::Value]) -> serde_json::Value {
    use std::collections::{BTreeMap, BTreeSet};

    let mut parents: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut children: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut latest_status: BTreeMap<String, String> = BTreeMap::new();
    let mut failed_count = 0usize;
    let mut completed_count = 0usize;
    let mut running_count = 0usize;

    for event in runs {
        let run = &event["run"];
        let Some(run_id) = run.get("run_id").and_then(|value| value.as_str()) else {
            continue;
        };
        let parent = run
            .get("parent_run_id")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        parents.entry(run_id.to_string()).or_insert(parent.clone());
        if let Some(parent_id) = parent {
            children
                .entry(parent_id)
                .or_default()
                .insert(run_id.to_string());
        }
        if let Some(status) = run.get("status").and_then(|value| value.as_str()) {
            latest_status.insert(run_id.to_string(), status.to_string());
        }
    }

    for status in latest_status.values() {
        match status.as_str() {
            "completed" => completed_count += 1,
            "failed" | "timeout" | "cancelled" => failed_count += 1,
            "running" => running_count += 1,
            _ => {}
        }
    }

    let roots = parents
        .iter()
        .filter_map(|(run_id, parent)| {
            let is_root = match parent.as_ref() {
                Some(parent_id) => !parents.contains_key(parent_id),
                None => true,
            };
            is_root.then(|| run_id.clone())
        })
        .collect::<Vec<_>>();
    let root_count = roots.len();
    let child_map = children
        .into_iter()
        .map(|(parent, child_ids)| (parent, child_ids.into_iter().collect::<Vec<_>>()))
        .collect::<BTreeMap<_, _>>();

    serde_json::json!({
        "roots": roots,
        "children": child_map,
        "summary": {
            "event_count": runs.len(),
            "span_count": parents.len(),
            "root_count": root_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "running_count": running_count,
        }
    })
}

async fn get_session_runs(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetEventsParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(50).min(200);
    let Some((total, stored_events)) = state
        .session_kernel
        .stored_events_by_type_page(&id, "RuntimeRun", from_seq, limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load runtime runs: {error}"),
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

    let runs: Vec<serde_json::Value> = stored_events
        .into_iter()
        .map(runtime_run_event_json)
        .collect();
    let tree = runtime_run_tree_summary(&runs);
    let next_seq = runs
        .last()
        .and_then(|event| event["sequence"].as_u64())
        .map(|sequence| sequence as usize + 1);
    let has_more = runs.len() < total;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "runs": runs,
        "tree": tree,
        "total": total,
        "from_seq": from_seq,
        "next_seq": next_seq,
        "limit": limit,
        "has_more": has_more,
    })))
}

async fn search_messages_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<SearchMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(db_messages) = state
        .session_kernel
        .search_stored_messages(&params.q, params.limit)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("search failed: {error}"),
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

    let results: Vec<SearchMessagesItem> = db_messages
        .into_iter()
        .map(|message| {
            let blocks: Vec<serde_json::Value> =
                serde_json::from_str(&message.content_json).unwrap_or_default();
            let content_preview = blocks
                .iter()
                .filter_map(|block| block.get("text").and_then(|text| text.as_str()))
                .collect::<Vec<_>>()
                .join(" ");
            let preview = if content_preview.len() > 200 {
                format!("{}...", &content_preview[..200])
            } else {
                content_preview
            };
            SearchMessagesItem {
                session_id: message.session_id,
                sequence: message.sequence,
                role: message.role,
                blocks,
                content_preview: preview,
                tool_use_id: message.tool_use_id,
                tool_name: message.tool_name,
                created_at_ms: message.created_at_ms,
            }
        })
        .collect();

    let total = results.len();
    Ok(Json(SearchMessagesResponse {
        query: params.q,
        results,
        total,
    }))
}

async fn compact_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_entry = state.active_runtime(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

    let mut runtime_guard = runtime_entry.lock().await;
    let result = runtime_guard.compact(runtime::CompactionConfig::default());
    if result.removed_message_count > 0 {
        *runtime_guard.session_mut() = result.compacted_session.clone();
    }
    let session_snapshot = runtime_guard.session().clone();
    drop(runtime_guard);

    state
        .session_kernel
        .sync_runtime_session_snapshot(&id, &session_snapshot)
        .await
        .map_err(|error| {
            tracing::error!(session_id = %id, error = %error, "failed to sync compacted session to unified store");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to sync compacted session: {error}"),
                }),
            )
        })?;

    tracing::info!(%id, removed = result.removed_message_count, "API session compacted");

    Ok(Json(serde_json::json!({
        "session_id": id,
        "compacted": result.removed_message_count > 0,
        "removed_message_count": result.removed_message_count,
        "summary": result.formatted_summary,
    })))
}

async fn get_session_stats_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_entry = state.active_runtime(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

    let runtime_guard = runtime_entry.lock().await;
    let session = runtime_guard.session();
    let messages = &session.messages;

    let user_count = messages
        .iter()
        .filter(|message| message.role == runtime::MessageRole::User)
        .count();
    let assistant_count = messages
        .iter()
        .filter(|message| message.role == runtime::MessageRole::Assistant)
        .count();
    let tool_count = messages
        .iter()
        .filter(|message| message.role == runtime::MessageRole::Tool)
        .count();

    let total_input_tokens: u32 = messages
        .iter()
        .filter_map(|message| message.usage.as_ref())
        .map(|usage| usage.input_tokens)
        .sum();
    let total_output_tokens: u32 = messages
        .iter()
        .filter_map(|message| message.usage.as_ref())
        .map(|usage| usage.output_tokens)
        .sum();

    let mut tool_usage: HashMap<String, usize> = HashMap::new();
    for message in messages {
        if message.role == runtime::MessageRole::Assistant {
            for block in &message.blocks {
                if let runtime::ContentBlock::ToolUse { name, .. } = block {
                    *tool_usage.entry(name.clone()).or_insert(0) += 1;
                }
            }
        }
    }

    let duration_ms = session.updated_at_ms.saturating_sub(session.created_at_ms);

    Ok(Json(serde_json::json!({
        "session_id": id,
        "message_count": messages.len(),
        "message_counts": {
            "user": user_count,
            "assistant": assistant_count,
            "tool": tool_count,
        },
        "tokens": {
            "input": total_input_tokens,
            "output": total_output_tokens,
            "total": total_input_tokens + total_output_tokens,
        },
        "tool_usage": tool_usage,
        "duration_ms": duration_ms,
    })))
}

async fn update_session_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<UpdateSessionRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut found = false;

    if let Some(runtime_entry) = state.active_runtime(&id) {
        found = true;
        let mut runtime_guard = runtime_entry.lock().await;
        let mut session = runtime_guard.session_mut_async().await;
        if let Some(ref model) = body.model {
            session.model = Some(model.clone());
        }
    }

    if state.has_unified_store() {
        match state.session_kernel.stored_session(&id).await {
            Ok(Some(mut record)) => {
                found = true;
                if let Some(ref model) = body.model {
                    record.model = Some(model.clone());
                }
                if let Some(ref title) = body.title {
                    let mut meta: serde_json::Value = record
                        .metadata_json
                        .as_deref()
                        .and_then(|value| serde_json::from_str(value).ok())
                        .unwrap_or(serde_json::json!({}));
                    meta["title"] = serde_json::Value::String(title.clone());
                    record.metadata_json = Some(serde_json::to_string(&meta).unwrap_or_default());
                }
                if let Some(ref metadata) = body.metadata {
                    let mut meta: serde_json::Value = record
                        .metadata_json
                        .as_deref()
                        .and_then(|value| serde_json::from_str(value).ok())
                        .unwrap_or(serde_json::json!({}));
                    if let Some(obj) = meta.as_object_mut() {
                        if let Some(new_obj) = metadata.as_object() {
                            for (key, value) in new_obj {
                                obj.insert(key.clone(), value.clone());
                            }
                        }
                    }
                    record.metadata_json = Some(serde_json::to_string(&meta).unwrap_or_default());
                }
                state
                    .session_kernel
                    .update_stored_session(&record)
                    .await
                    .map_err(|error| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: format!("failed to update session: {error}"),
                            }),
                        )
                    })?;
            }
            Ok(None) => {}
            Err(error) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("failed to load session: {error}"),
                    }),
                ));
            }
        }
    }

    if !found {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ));
    }

    Ok(Json(serde_json::json!({
        "session_id": id,
        "updated": true,
    })))
}
