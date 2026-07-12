use super::*;

#[derive(Debug, Clone, Copy)]
struct ValueLoopStageSpec {
    id: &'static str,
    label: &'static str,
    required: bool,
}

#[derive(Debug, Clone)]
struct ValueLoopStageState {
    spec: ValueLoopStageSpec,
    event_count: usize,
    failed_count: usize,
    degraded_count: usize,
    latest_sequence: Option<usize>,
    latest_kind: Option<String>,
}

const VALUE_LOOP_STAGES: [ValueLoopStageSpec; 8] = [
    ValueLoopStageSpec {
        id: "intake",
        label: "Intake",
        required: true,
    },
    ValueLoopStageSpec {
        id: "context",
        label: "Context",
        required: true,
    },
    ValueLoopStageSpec {
        id: "memory",
        label: "Memory",
        required: true,
    },
    ValueLoopStageSpec {
        id: "governance",
        label: "Governance",
        required: true,
    },
    ValueLoopStageSpec {
        id: "task",
        label: "Task",
        required: true,
    },
    ValueLoopStageSpec {
        id: "execution",
        label: "Execution",
        required: true,
    },
    ValueLoopStageSpec {
        id: "agent",
        label: "Agent",
        required: true,
    },
    ValueLoopStageSpec {
        id: "channel",
        label: "Channel",
        required: false,
    },
];

pub(in crate::api_routes) fn value_loop_summary(
    events: &[RuntimeEvent],
    degraded: bool,
    degraded_reason: Option<&str>,
) -> Value {
    let mut stages: Vec<ValueLoopStageState> = VALUE_LOOP_STAGES
        .iter()
        .copied()
        .map(|spec| ValueLoopStageState {
            spec,
            event_count: 0,
            failed_count: 0,
            degraded_count: 0,
            latest_sequence: None,
            latest_kind: None,
        })
        .collect();
    let mut failed_events = 0usize;
    let mut degraded_events = 0usize;
    let mut open_tasks = 0i64;
    let mut positive_agent_lift = false;
    let mut latest_value_score: Option<u64> = None;
    let mut reasons: Vec<String> = Vec::new();

    if let Some(reason) = degraded_reason {
        reasons.push(reason.to_string());
    }

    for event in events {
        let stage_id = value_loop_stage_id(event);
        if let Some(stage) = stages.iter_mut().find(|stage| stage.spec.id == stage_id) {
            stage.event_count += 1;
            stage.latest_sequence = usize::try_from(event.sequence).ok();
            stage.latest_kind = Some(event.kind.clone());
            if runtime_event_failed(event) {
                stage.failed_count += 1;
            }
            if runtime_event_degraded(event) {
                stage.degraded_count += 1;
            }
        }

        if runtime_event_failed(event) {
            failed_events += 1;
        }
        if runtime_event_degraded(event) {
            degraded_events += 1;
        }
        match event.kind.as_str() {
            "task.started" => open_tasks += 1,
            "task.completed" | "task.cancelled" | "task.blocked" => {
                open_tasks = open_tasks.saturating_sub(1);
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

    let required_total = stages.iter().filter(|stage| stage.spec.required).count();
    let required_observed = stages
        .iter()
        .filter(|stage| stage.spec.required && stage.event_count > 0)
        .count();
    let missing_required: Vec<Value> = stages
        .iter()
        .filter(|stage| stage.spec.required && stage.event_count == 0)
        .map(|stage| {
            serde_json::json!({
                "id": stage.spec.id,
                "label": stage.spec.label,
                "next_action": value_loop_next_action(stage.spec.id),
            })
        })
        .collect();
    let missing_required_count = missing_required.len();
    let mut score = if required_total == 0 {
        100i64
    } else {
        ((required_observed * 100) / required_total) as i64
    };

    if degraded {
        score -= 35;
    }
    if failed_events > 0 {
        score -= (failed_events as i64 * 15).min(45);
        reasons.push(format!("{failed_events} failed event(s) in value loop"));
    }
    if degraded_events > 0 {
        score -= (degraded_events as i64 * 10).min(30);
        reasons.push(format!("{degraded_events} degraded event(s) in value loop"));
    }
    if open_tasks > 0 {
        score -= (open_tasks * 5).min(20);
        reasons.push(format!(
            "{open_tasks} open task(s) still need review or completion"
        ));
    }
    if missing_required_count > 0 {
        reasons.push(format!(
            "{missing_required_count} required stage(s) missing from selected timeline"
        ));
    }
    if let Some(value_score) = latest_value_score {
        if value_score < 50 {
            score -= 10;
            reasons.push("latest multi-agent value score is below threshold".to_string());
        } else if positive_agent_lift {
            score = (score + 3).min(100);
        }
    }
    if events.is_empty() && !degraded {
        score = 0;
        reasons.push("no runtime events available for value-loop assessment".to_string());
    }

    let score = score.clamp(0, 100) as u64;
    let status = if degraded || failed_events > 0 || degraded_events > 0 {
        "degraded"
    } else if missing_required_count > 0 || open_tasks > 0 || score < 90 {
        "incomplete"
    } else {
        "complete"
    };
    if reasons.is_empty() {
        reasons
            .push("runtime value loop has all required stages and no blocking defects".to_string());
    }

    let stages_json: Vec<Value> = stages
        .into_iter()
        .map(|stage| {
            let status = if stage.failed_count > 0 {
                "failed"
            } else if stage.degraded_count > 0 {
                "degraded"
            } else if stage.event_count > 0 {
                "observed"
            } else if stage.spec.required {
                "missing"
            } else {
                "optional"
            };
            serde_json::json!({
                "id": stage.spec.id,
                "label": stage.spec.label,
                "required": stage.spec.required,
                "status": status,
                "event_count": stage.event_count,
                "failed_count": stage.failed_count,
                "degraded_count": stage.degraded_count,
                "latest_sequence": stage.latest_sequence,
                "latest_kind": stage.latest_kind,
            })
        })
        .collect();

    serde_json::json!({
        "status": status,
        "score": score,
        "event_count": events.len(),
        "required_total": required_total,
        "required_observed": required_observed,
        "missing_required_count": missing_required_count,
        "missing_required": missing_required,
        "failed_events": failed_events,
        "degraded_events": degraded_events,
        "open_tasks": open_tasks,
        "positive_agent_lift": positive_agent_lift,
        "latest_value_score": latest_value_score,
        "stages": stages_json,
        "reasons": reasons,
        "next_actions": value_loop_next_actions(&missing_required_count, open_tasks, failed_events, degraded_events),
    })
}

pub(in crate::api_routes) fn degraded_value_loop_summary(reason: &str) -> Value {
    value_loop_summary(&[], true, Some(reason))
}

fn value_loop_stage_id(event: &RuntimeEvent) -> &'static str {
    if is_channel_event(event) {
        return "channel";
    }
    match event.scope.as_str() {
        "session" | "message" | "turn" | "session_input" => "intake",
        "context" => "context",
        "memory" | "mfg" => "memory",
        "policy" | "approval" => "governance",
        "application_task" | "task" | "goal" => "task",
        "tool" | "schedule" | "worker" | "execution_node" => "execution",
        "agent" | "team" | "execution_graph" => "agent",
        "mission" | "relation" | "steward" | "recovery" => "governance",
        _ => "execution",
    }
}

fn is_channel_event(event: &RuntimeEvent) -> bool {
    event.kind.starts_with("channel.")
        || event.kind.starts_with("platform.")
        || event.kind.starts_with("cross_plane.")
        || event.refs.iter().any(|reference| {
            matches!(
                reference.ref_type.as_str(),
                "channel" | "platform" | "feishu" | "wechat" | "wecom" | "email"
            )
        })
}

fn value_loop_next_action(stage_id: &str) -> &'static str {
    match stage_id {
        "intake" => "persist at least one session, turn, or message event",
        "context" => "build and persist a context envelope for this run",
        "memory" => "record memory recall, write, pulse, or maintenance evidence",
        "governance" => "record runtime policy, approval, or permission decision",
        "task" => "bind execution to a task lifecycle event",
        "execution" => "record tool, scheduler, or channel execution evidence",
        "agent" => "record agent collaboration, execution graph, or single-agent decision evidence",
        _ => "record runtime evidence for this stage",
    }
}

fn value_loop_next_actions(
    missing_required_count: &usize,
    open_tasks: i64,
    failed_events: usize,
    degraded_events: usize,
) -> Vec<String> {
    let mut actions = Vec::new();
    if *missing_required_count > 0 {
        actions.push(
            "complete missing required runtime stages before claiming closed-loop execution"
                .to_string(),
        );
    }
    if open_tasks > 0 {
        actions
            .push("complete, cancel, or explicitly block open task lifecycle records".to_string());
    }
    if failed_events > 0 {
        actions.push("inspect failed events and append recovery or rollback evidence".to_string());
    }
    if degraded_events > 0 {
        actions.push(
            "resolve degraded runtime evidence before promoting the session as healthy".to_string(),
        );
    }
    if actions.is_empty() {
        actions.push("no blocking action required for the selected runtime timeline".to_string());
    }
    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_routes::runtime_routes::control::agent_value::agent_value_summary;

    fn event(sequence: usize, scope: &str, kind: &str) -> RuntimeEvent {
        RuntimeEvent {
            sequence: sequence as u64,
            commit_cursor: None,
            scope: scope.to_string(),
            kind: kind.to_string(),
            status: None,
            refs: Vec::new(),
            payload: serde_json::json!({}),
            created_at_ms: sequence as u64,
            source: "test",
        }
    }

    fn reviewed_execution_graph_event(
        sequence: usize,
        value_score: u16,
        positive_lift: bool,
    ) -> RuntimeEvent {
        let mut event = event(
            sequence,
            "execution_graph",
            "agent.execution_graph.reviewed",
        );
        event.payload = serde_json::json!({
            "graph": {
                "graph_id": "graph-agent-value",
                "nodes": [
                    {"kind": "AgentTask", "node_id": "worker-1"},
                    {"kind": "AgentTask", "node_id": "worker-2"},
                    {"kind": "Synthesis", "node_id": "synthesis"}
                ]
            },
            "scorecard": {
                "completion_rate": 1.0,
                "synthesis_lift": if positive_lift { 1.25 } else { 1.0 },
                "complementarity_score": if positive_lift { 0.75 } else { 0.0 },
                "conflict_count": 0
            },
            "value_verdict": {
                "positive_lift": positive_lift,
                "continue_multi_agent": positive_lift,
                "value_score": value_score,
                "reasons": if positive_lift {
                    vec!["positive_multi_agent_lift"]
                } else {
                    vec!["no_synthesis_lift", "no_complementarity"]
                }
            }
        });
        event.status = Some("completed".to_string());
        event
    }

    #[test]
    fn value_loop_summary_marks_complete_closed_loop() {
        let mut execution_graph = event(6, "execution_graph", "agent.execution_graph.reviewed");
        execution_graph.payload = serde_json::json!({
            "value_verdict": {
                "positive_lift": true,
                "value_score": 76
            }
        });
        let events = vec![
            event(0, "message", "message.received"),
            event(1, "context", "context.envelope.built"),
            event(2, "memory", "memory.recall.completed"),
            event(3, "policy", "runtime.policy.decided"),
            event(4, "task", "task.started"),
            event(5, "tool", "tool.completed"),
            execution_graph,
            event(7, "task", "task.completed"),
        ];

        let summary = value_loop_summary(&events, false, None);

        assert_eq!(summary["status"], "complete");
        assert_eq!(summary["score"], 100);
        assert_eq!(summary["required_total"], 7);
        assert_eq!(summary["required_observed"], 7);
        assert_eq!(summary["missing_required_count"], 0);
        assert_eq!(summary["open_tasks"], 0);
        assert_eq!(summary["positive_agent_lift"], true);
        assert_eq!(
            summary["next_actions"][0],
            "no blocking action required for the selected runtime timeline"
        );
    }

    #[test]
    fn value_loop_summary_surfaces_missing_and_degraded_stages() {
        let mut failed_tool = event(2, "tool", "tool.failed");
        failed_tool.status = Some("failed".to_string());
        let mut degraded_memory = event(1, "memory", "memory.recall.completed");
        degraded_memory.status = Some("degraded".to_string());
        let events = vec![
            event(0, "message", "message.received"),
            degraded_memory,
            failed_tool,
        ];

        let summary = value_loop_summary(&events, false, None);

        assert_eq!(summary["status"], "degraded");
        assert_eq!(summary["failed_events"], 1);
        assert_eq!(summary["degraded_events"], 1);
        assert_eq!(summary["missing_required_count"], 4);
        assert!(summary["score"].as_u64().unwrap() < 50);
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action
                .as_str()
                .unwrap()
                .contains("complete missing required runtime stages")));
    }

    #[test]
    fn value_loop_summary_tracks_optional_channel_stage() {
        let mut channel_event = event(0, "tool", "channel.feishu.message.sent");
        channel_event.refs = vec![RuntimeTimelineRef {
            ref_type: "feishu".to_string(),
            id: "chat-1".to_string(),
            label: Some("Feishu".to_string()),
        }];

        let summary = value_loop_summary(&[channel_event], false, None);
        let channel = summary["stages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|stage| stage["id"] == "channel")
            .unwrap();

        assert_eq!(channel["required"], false);
        assert_eq!(channel["status"], "observed");
        assert_eq!(channel["event_count"], 1);
    }

    #[test]
    fn agent_value_summary_proves_multi_agent_lift_against_policy() {
        let policy = AgentControlPolicy {
            min_collaboration_score: 70,
            ..AgentControlPolicy::default()
        };
        let event = reviewed_execution_graph_event(4, 76, true);

        let summary = agent_value_summary(&[event], &policy, false, None);

        assert_eq!(summary["status"], "proven");
        assert_eq!(summary["recommendation"], "continue_multi_agent");
        assert_eq!(summary["policy_passed"], true);
        assert_eq!(summary["latest"]["agent_tasks"], 2);
        assert_eq!(summary["latest"]["value_score"], 76);
        assert_eq!(summary["latest"]["positive_lift"], true);
    }

    #[test]
    fn agent_value_summary_rejects_low_value_or_missing_lift() {
        let policy = AgentControlPolicy {
            min_collaboration_score: 70,
            require_positive_lift: true,
            ..AgentControlPolicy::default()
        };
        let event = reviewed_execution_graph_event(4, 48, false);

        let summary = agent_value_summary(&[event], &policy, false, None);

        assert_eq!(summary["status"], "insufficient");
        assert_eq!(
            summary["recommendation"],
            "prefer_single_agent_or_review_only"
        );
        assert_eq!(summary["policy_passed"], false);
        assert!(summary["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason.as_str().unwrap().contains("below policy threshold")));
        assert!(summary["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason.as_str().unwrap() == "no_synthesis_lift"));
    }

    #[test]
    fn agent_value_summary_requires_review_for_unresolved_conflict() {
        let policy = AgentControlPolicy::default();
        let mut event = reviewed_execution_graph_event(4, 82, false);
        event.payload["scorecard"]["conflict_count"] = serde_json::json!(2);
        event.payload["value_verdict"]["reasons"] = serde_json::json!(["excessive_conflict"]);

        let summary = agent_value_summary(&[event], &policy, false, None);

        assert_eq!(summary["status"], "review_required");
        assert_eq!(summary["recommendation"], "review_conflicts");
        assert_eq!(summary["policy_passed"], false);
        assert_eq!(summary["latest"]["conflict_count"], 2);
    }
}
