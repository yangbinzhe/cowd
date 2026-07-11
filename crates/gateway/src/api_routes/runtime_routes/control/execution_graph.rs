use super::*;

pub(in crate::api_routes) fn execution_graph_summary(events: &[RuntimeEvent]) -> Value {
    let graph_events: Vec<&RuntimeEvent> = events
        .iter()
        .filter(|event| {
            event.kind == "agent.execution_graph.reviewed"
                || event.kind == "agent.execution_graph.planned"
        })
        .collect();
    let Some(latest) = graph_events.last() else {
        return empty_execution_graph_summary();
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
                .find(|reference| reference.ref_type == "execution_graph")
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

pub(in crate::api_routes) fn empty_execution_graph_summary() -> Value {
    serde_json::json!({
        "count": 0,
        "latest": null,
        "agent_tasks": 0,
        "memory_candidates": 0,
        "conflicts": 0,
    })
}
