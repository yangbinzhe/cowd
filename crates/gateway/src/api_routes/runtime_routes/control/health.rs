use super::*;

pub(in crate::api_routes) fn health_summary(
    events: &[RuntimeEvent],
    degraded: bool,
    degraded_reason: Option<&str>,
) -> Value {
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
        let scope = event.scope.clone();
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
            "agent.execution_graph.reviewed" => {
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

pub(in crate::api_routes) fn degraded_health_summary(reason: &str) -> Value {
    health_summary(&[], true, Some(reason))
}
