use super::*;

pub(in crate::api_routes) fn execution_graph_summary(events: &[RuntimeEvent]) -> Value {
    let mut graph_events = std::collections::BTreeMap::<String, &RuntimeEvent>::new();
    let mut latest = None;
    for event in events.iter().filter(|event| agent_graph(event).is_some()) {
        let graph = agent_graph(event).expect("filtered agent graph");
        let graph_id = graph_identity(graph, event);
        graph_events.insert(graph_id, event);
        latest = Some(event);
    }
    let Some(latest) = latest else {
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
    let graph_id = Some(graph_identity(graph, latest));
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
                .or_else(|| graph_runtime_status(graph))
                .or(latest.status.as_deref())
                .unwrap_or("n/a"),
            "graph_id": graph_id,
            "board_id": board_id,
            "completion_rate": scorecard
                .get("completion_rate")
                .and_then(Value::as_f64)
                .or_else(|| graph_completion_rate(graph)),
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

fn agent_graph(event: &RuntimeEvent) -> Option<&Value> {
    let supported = matches!(
        event.kind.as_str(),
        "agent.execution_graph.reviewed"
            | "agent.execution_graph.planned"
            | "execution_graph.planned"
            | "execution_graph.node_transitioned"
            | "execution_graph.node_transitioned_and_replanned"
            | "execution_graph.command_applied"
            | "execution_graph.replanned"
            | "execution_graph.recovered"
    );
    let graph = supported.then(|| event.payload.get("graph")).flatten()?;
    graph
        .get("nodes")
        .and_then(Value::as_array)
        .is_some_and(|nodes| {
            nodes.iter().any(|node| {
                matches!(
                    node.get("kind").and_then(Value::as_str),
                    Some("AgentTask") | Some("agent_task")
                )
            })
        })
        .then_some(graph)
}

fn graph_identity(graph: &Value, event: &RuntimeEvent) -> String {
    graph
        .get("graph_id")
        .or_else(|| graph.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            event
                .refs
                .iter()
                .find(|reference| reference.ref_type == "execution_graph")
                .map(|reference| reference.id.clone())
        })
        .unwrap_or_else(|| format!("runtime-graph-event:{}", event.sequence))
}

fn graph_runtime_status(graph: &Value) -> Option<&str> {
    let statuses = graph.get("node_statuses")?.as_object()?;
    if statuses.is_empty() {
        return None;
    }
    if statuses.values().any(|status| {
        matches!(
            status.as_str(),
            Some("failed") | Some("blocked") | Some("cancelled")
        )
    }) {
        return Some("degraded");
    }
    statuses
        .values()
        .all(|status| matches!(status.as_str(), Some("completed") | Some("skipped")))
        .then_some("completed")
        .or(Some("running"))
}

fn graph_completion_rate(graph: &Value) -> Option<f64> {
    let statuses = graph.get("node_statuses")?.as_object()?;
    (!statuses.is_empty()).then(|| {
        let completed = statuses
            .values()
            .filter(|status| {
                matches!(
                    status.as_str(),
                    Some("completed")
                        | Some("failed")
                        | Some("blocked")
                        | Some("cancelled")
                        | Some("skipped")
                )
            })
            .count();
        completed as f64 / statuses.len() as f64
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
