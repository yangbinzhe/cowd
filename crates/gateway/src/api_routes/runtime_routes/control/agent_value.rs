use super::*;

pub(in crate::api_routes) fn agent_value_summary(
    events: &[RuntimeEvent],
    policy: &AgentControlPolicy,
    degraded: bool,
    degraded_reason: Option<&str>,
) -> Value {
    let latest = events.iter().rev().find(|event| {
        event.kind == "agent.workgraph.reviewed" || event.kind == "agent.workgraph.planned"
    });
    let mut reasons: Vec<String> = degraded_reason.map(str::to_string).into_iter().collect();

    if !policy.enabled {
        reasons.push("agent policy is disabled".to_string());
        return serde_json::json!({
            "status": "disabled",
            "recommendation": "single_agent_or_manual_review",
            "policy": agent_policy_json(policy),
            "latest": null,
            "policy_passed": false,
            "reasons": reasons,
        });
    }

    let Some(event) = latest else {
        reasons.push("no agent workgraph evidence in selected timeline".to_string());
        return serde_json::json!({
            "status": if degraded { "degraded" } else { "unproven" },
            "recommendation": "collect_workgraph_review",
            "policy": agent_policy_json(policy),
            "latest": null,
            "policy_passed": false,
            "reasons": reasons,
        });
    };

    let payload = &event.payload;
    let scorecard = payload.get("scorecard").unwrap_or(&Value::Null);
    let verdict = payload.get("value_verdict").unwrap_or(&Value::Null);
    let graph = payload.get("graph").unwrap_or(&Value::Null);
    let value_score = verdict
        .get("value_score")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u16;
    let positive_lift = verdict
        .get("positive_lift")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let continue_multi_agent = verdict
        .get("continue_multi_agent")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let completion_rate = scorecard
        .get("completion_rate")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let synthesis_lift = scorecard
        .get("synthesis_lift")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let complementarity_score = scorecard
        .get("complementarity_score")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let conflict_count = scorecard
        .get("conflict_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
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
    let policy_score_passed = value_score >= policy.min_collaboration_score;
    let lift_passed = !policy.require_positive_lift || positive_lift;
    let conflict_review_required = policy.review_on_conflict && conflict_count > 0;
    let event_failed = runtime_event_failed(event);
    let event_degraded = runtime_event_degraded(event) || degraded;
    let policy_passed = policy_score_passed
        && lift_passed
        && !event_failed
        && !event_degraded
        && !conflict_review_required;

    if !policy_score_passed {
        reasons.push(format!(
            "value score {value_score} is below policy threshold {}",
            policy.min_collaboration_score
        ));
    }
    if !lift_passed {
        reasons.push("positive lift is required by policy but was not proven".to_string());
    }
    if conflict_review_required {
        reasons.push(format!("{conflict_count} conflict(s) require review"));
    }
    if event_failed {
        reasons.push("latest workgraph event failed".to_string());
    }
    if event_degraded {
        reasons.push("latest workgraph evidence is degraded".to_string());
    }
    for reason in verdict
        .get("reasons")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        if !reasons.iter().any(|existing| existing == reason) {
            reasons.push(reason.to_string());
        }
    }
    if reasons.is_empty() {
        reasons.push("multi-agent collaboration clears policy threshold".to_string());
    }

    let status = if event_failed || event_degraded {
        "degraded"
    } else if conflict_review_required {
        "review_required"
    } else if policy_passed {
        "proven"
    } else {
        "insufficient"
    };
    let recommendation = if status == "proven" && continue_multi_agent {
        "continue_multi_agent"
    } else if conflict_review_required {
        "review_conflicts"
    } else if status == "insufficient" {
        "prefer_single_agent_or_review_only"
    } else if status == "degraded" {
        "repair_workgraph_evidence"
    } else {
        "collect_more_collaboration_evidence"
    };

    serde_json::json!({
        "status": status,
        "recommendation": recommendation,
        "policy": agent_policy_json(policy),
        "policy_passed": policy_passed,
        "latest": {
            "sequence": event.sequence,
            "kind": event.kind,
            "status": event.status,
            "value_score": value_score,
            "positive_lift": positive_lift,
            "continue_multi_agent": continue_multi_agent,
            "completion_rate": completion_rate,
            "synthesis_lift": synthesis_lift,
            "complementarity_score": complementarity_score,
            "conflict_count": conflict_count,
            "agent_tasks": agent_tasks,
        },
        "reasons": reasons,
    })
}

pub(in crate::api_routes) fn degraded_agent_value_summary(
    policy: &AgentControlPolicy,
    reason: &str,
) -> Value {
    agent_value_summary(&[], policy, true, Some(reason))
}

fn agent_policy_json(policy: &AgentControlPolicy) -> Value {
    serde_json::json!({
        "enabled": policy.enabled,
        "max_parallel_agents": policy.max_parallel_agents,
        "review_on_conflict": policy.review_on_conflict,
        "require_positive_lift": policy.require_positive_lift,
        "min_collaboration_score": policy.min_collaboration_score,
    })
}
