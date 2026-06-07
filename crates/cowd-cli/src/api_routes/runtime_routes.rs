use std::sync::Arc;

use axum::{
    extract::{Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::Value;

use super::{AppState, ErrorResponse};
use memory::RuntimeEvent;
use runtime::{ConfigLoader, RuntimeConfig};

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
            "health_summary": degraded_health_summary("session store not available"),
        })));
    };

    let workgraph_summary = workgraph_summary(&page.events);
    let health_summary = health_summary(&page.events, false, None);

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
        "health_summary": health_summary,
    })))
}

pub(super) async fn get_runtime_effective_config(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    let (source, runtime_config, warnings) =
        match ConfigLoader::new(&state.workspace_root, &state.config_home).load() {
            Ok(config) => {
                let source = if config.loaded_entries().is_empty() {
                    "default"
                } else {
                    "config"
                };
                (source, config, Vec::<String>::new())
            }
            Err(error) => (
                "default",
                RuntimeConfig::empty(),
                vec![format!("failed to load runtime config: {error}")],
            ),
        };
    let control = runtime_config.runtime_control();
    Json(serde_json::json!({
        "source": source,
        "workspace_root": state.workspace_root,
        "profile_id": state.profile_id,
        "scenario": control.scenario.as_str(),
        "control_policy": control.policy,
        "warnings": warnings,
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
    let value_verdict = payload.get("value_verdict").unwrap_or(&Value::Null);
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
            "value_verdict": value_verdict,
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

fn health_summary(events: &[RuntimeEvent], degraded: bool, degraded_reason: Option<&str>) -> Value {
    let mut score: i64 = if degraded { 35 } else { 100 };
    let mut failed_events = 0usize;
    let mut degraded_events = 0usize;
    let mut open_tasks = 0i64;
    let mut positive_agent_lift = false;
    let mut latest_policy = Value::Null;
    let mut latest_value_score: Option<u64> = None;
    let mut reasons: Vec<String> = Vec::new();
    let mut scope_counts = serde_json::Map::new();

    if let Some(reason) = degraded_reason {
        reasons.push(reason.to_string());
    }

    for event in events {
        let scope = serde_json::to_value(event.scope)
            .ok()
            .and_then(|value| value.as_str().map(ToString::to_string))
            .unwrap_or_else(|| "unknown".to_string());
        let next = scope_counts
            .get(&scope)
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        scope_counts.insert(scope, Value::from(next));

        if matches!(event.status.as_deref(), Some("failed") | Some("error")) {
            failed_events += 1;
        }
        if matches!(event.status.as_deref(), Some("degraded"))
            || event.payload.get("parse_error").is_some()
        {
            degraded_events += 1;
        }

        match event.kind.as_str() {
            "task.started" => open_tasks += 1,
            "task.completed" | "task.cancelled" | "task.blocked" => {
                open_tasks = open_tasks.saturating_sub(1);
            }
            "runtime.policy.decided" => {
                latest_policy = serde_json::json!({
                    "sequence": event.sequence,
                    "agent_mode": event.payload.get("agent_mode").cloned().unwrap_or(Value::Null),
                    "requires_review": event
                        .payload
                        .get("requires_review")
                        .cloned()
                        .unwrap_or(Value::Null),
                    "complexity": event.payload.get("complexity").cloned().unwrap_or(Value::Null),
                });
            }
            "agent.workgraph.reviewed" => {
                if let Some(verdict) = event.payload.get("value_verdict") {
                    positive_agent_lift |= verdict
                        .get("positive_lift")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    latest_value_score = verdict.get("value_score").and_then(Value::as_u64);
                }
            }
            _ => {}
        }
    }

    if failed_events > 0 {
        score -= (failed_events as i64 * 18).min(45);
        reasons.push(format!("{failed_events} failed runtime event(s)"));
    }
    if degraded_events > 0 {
        score -= (degraded_events as i64 * 12).min(36);
        reasons.push(format!("{degraded_events} degraded runtime event(s)"));
    }
    if open_tasks > 0 {
        score -= (open_tasks * 4).min(16);
        reasons.push(format!("{open_tasks} open task(s)"));
    }
    if let Some(value_score) = latest_value_score {
        if value_score < 50 {
            score -= 10;
            reasons.push("latest agent collaboration value below threshold".to_string());
        } else if positive_agent_lift {
            score = (score + 3).min(100);
        }
    }
    if events.is_empty() && !degraded {
        score = 80;
        reasons.push("no runtime events in selected page".to_string());
    }

    let score = score.clamp(0, 100) as u64;
    let status = if degraded || degraded_events > 0 {
        "degraded"
    } else if failed_events > 0 || open_tasks > 0 || score < 85 {
        "attention"
    } else {
        "healthy"
    };

    if reasons.is_empty() {
        reasons.push("runtime event spine is coherent".to_string());
    }

    serde_json::json!({
        "status": status,
        "score": score,
        "event_count": events.len(),
        "failed_events": failed_events,
        "degraded_events": degraded_events,
        "open_tasks": open_tasks,
        "positive_agent_lift": positive_agent_lift,
        "latest_policy": latest_policy,
        "latest_value_score": latest_value_score,
        "reasons": reasons,
        "scope_counts": scope_counts,
    })
}

fn degraded_health_summary(reason: &str) -> Value {
    health_summary(&[], true, Some(reason))
}
