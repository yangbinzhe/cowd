use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use serde_json::Value;

use super::{AppState, ErrorResponse};
use memory::RuntimeEvent;
use runtime::RuntimeControlPolicy;

#[derive(Deserialize)]
pub(super) struct RuntimeTimelineParams {
    session_id: String,
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

pub(super) async fn get_runtime_timeline(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RuntimeTimelineParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(500);
    let page = state
        .session_kernel
        .stored_timeline_runtime_page(&params.session_id, from_seq, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load runtime timeline: {e}"),
                }),
            )
        })?;

    let Some(page) = page else {
        return Ok(Json(serde_json::json!({
            "session_id": params.session_id,
            "events": [],
            "total": 0,
            "from_seq": from_seq,
            "next_seq": null,
            "limit": limit,
            "has_more": false,
            "degraded": true,
            "degraded_reason": "session store not available",
            "workgraph_summary": empty_workgraph_summary(),
        })));
    };

    let workgraph_summary = workgraph_summary(&page.events);

    Ok(Json(serde_json::json!({
        "session_id": params.session_id,
        "events": page.events,
        "total": page.total,
        "from_seq": from_seq,
        "next_seq": page.next_seq,
        "limit": limit,
        "has_more": page.has_more,
        "degraded": false,
        "degraded_reason": null,
        "workgraph_summary": workgraph_summary,
    })))
}

pub(super) async fn get_runtime_effective_config(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    Json(serde_json::json!({
        "source": "default",
        "workspace_root": state.workspace_root,
        "profile_id": state.profile_id,
        "control_policy": RuntimeControlPolicy::default(),
        "warnings": [],
    }))
}

fn workgraph_summary(events: &[RuntimeEvent]) -> Value {
    let graph_events: Vec<&RuntimeEvent> = events
        .iter()
        .filter(|event| {
            event.kind == "agent.workgraph.reviewed" || event.kind == "agent.workgraph.planned"
        })
        .collect();
    let Some(latest) = graph_events.last() else {
        return empty_workgraph_summary();
    };

    let payload = &latest.payload;
    let graph = payload.get("graph").unwrap_or(&Value::Null);
    let scorecard = payload.get("scorecard").unwrap_or(&Value::Null);
    let agent_tasks = graph
        .get("nodes")
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes
                .iter()
                .filter(|node| {
                    matches!(
                        node.get("kind").and_then(Value::as_str),
                        Some("AgentTask") | Some("agent_task")
                    )
                })
                .count()
        })
        .unwrap_or(0);
    let memory_candidates = payload
        .get("maintenance_candidates")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let graph_id = graph
        .get("graph_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            latest
                .refs
                .iter()
                .find(|reference| reference.ref_type == "workgraph")
                .map(|reference| reference.id.clone())
        });
    let board_id = payload
        .get("board_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            graph
                .get("board_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            latest
                .refs
                .iter()
                .find(|reference| reference.ref_type == "collaboration_board")
                .map(|reference| reference.id.clone())
        });

    serde_json::json!({
        "count": graph_events.len(),
        "latest": {
            "sequence": latest.sequence,
            "kind": latest.kind,
            "status": graph
                .get("status")
                .and_then(Value::as_str)
                .or(latest.status.as_deref())
                .unwrap_or("n/a"),
            "graph_id": graph_id,
            "board_id": board_id,
            "completion_rate": scorecard.get("completion_rate").and_then(Value::as_f64),
            "synthesis_lift": scorecard.get("synthesis_lift").and_then(Value::as_f64),
            "complementarity_score": scorecard
                .get("complementarity_score")
                .and_then(Value::as_f64),
        },
        "agent_tasks": agent_tasks,
        "memory_candidates": memory_candidates,
        "conflicts": scorecard
            .get("conflict_count")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    })
}

fn empty_workgraph_summary() -> Value {
    serde_json::json!({
        "count": 0,
        "latest": null,
        "agent_tasks": 0,
        "memory_candidates": 0,
        "conflicts": 0,
    })
}
