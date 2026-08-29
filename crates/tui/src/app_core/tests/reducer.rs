use super::*;

fn make_msg(content: &str) -> TimelineEntry {
    TimelineEntry::Message {
        role: "user".into(),
        content: content.into(),
        timestamp: "12:00".into(),
        identity: None,
    }
}

#[test]
fn timeline_no_trim_at_3000() {
    let mut app = App::new("test", "sess");
    for i in 0..3500 {
        app.add_message("user", &format!("msg {i}"));
    }
    assert_eq!(app.timeline_len(), 3500);
    let first = app.timeline_get(0).unwrap();
    assert!(first.full_text().contains("msg 0"));
    let last = app.timeline_get(3499).unwrap();
    assert!(last.full_text().contains("msg 3499"));
}

#[test]
fn scroll_up_loads_page() {
    let mut app = App::new("test", "sess");
    for i in 0..600 {
        app.add_message("user", &format!("msg {i}"));
    }
    assert_eq!(app.timeline_len(), 600);
    assert_eq!(app.timeline.timeline_pages.len(), 2);
    let at_500 = app.timeline_get(500).unwrap();
    assert!(at_500.full_text().contains("msg 500"));
    let at_0 = app.timeline_get(0).unwrap();
    assert!(at_0.full_text().contains("msg 0"));
}

#[test]
fn context_envelope_event_updates_app_state() {
    let envelope = crate::test_utils::context_envelope_fixture();
    let expected_id = envelope
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap()
        .to_string();
    let mut app = App::new("test", "sess");

    app.apply_event(CowdEvent::ContextEnvelope { envelope });

    assert_eq!(
        app.execution
            .latest_context_envelope
            .as_ref()
            .and_then(|env| env.get("id"))
            .and_then(serde_json::Value::as_str),
        Some(expected_id.as_str())
    );
}

#[test]
fn turn_started_clears_previous_turn_runtime_evidence() {
    let mut app = App::new("test", "sess");
    app.execution.latest_context_envelope = Some(serde_json::json!({"selected": [{"id": "old"}]}));
    app.execution.latest_runtime_policy = Some(crate::RuntimePolicyDecisionSummary {
        level: "complex".into(),
        score: 80,
        recommended_profile: "deep".into(),
        agent_mode: "team".into(),
        requires_review: true,
        signal_count: 3,
    });
    app.execution.latest_execution_graph_summary = Some(crate::RuntimeExecutionGraphSummary {
        graph_id: Some("g".into()),
        board_id: Some("b".into()),
        status: "done".into(),
        agent_tasks: 1,
        child_executions: 0,
        memory_candidates: 2,
        conflicts: 0,
        completion_rate: Some(1.0),
        synthesis_lift: None,
        complementarity_score: None,
    });
    app.execution.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::Complete);
    app.shell.effective_model = Some("old-effective-model".to_string());
    app.execution.context_used_tokens = Some(8_000);
    app.execution.context_window_tokens = Some(128_000);
    app.execution.current_run_metrics = Some(Default::default());

    app.apply_event(CowdEvent::TurnStarted);

    assert!(app.execution.latest_context_envelope.is_none());
    assert!(app.execution.latest_runtime_policy.is_none());
    assert!(app.execution.latest_execution_graph_summary.is_none());
    assert!(app.execution.current_execution_status.is_none());
    assert!(app.shell.effective_model.is_none());
    assert!(app.execution.context_used_tokens.is_none());
    assert!(app.execution.context_window_tokens.is_none());
    assert!(app.execution.current_run_metrics.is_none());
    assert!(app.turn_is_active());
}

#[test]
fn app_applies_gateway_session_stats() {
    let mut app = App::new("test", "sess");
    app.apply_session_stats(serde_json::json!({
        "session_id": "sess",
        "tokens": {
            "input": 500,
            "output": 12,
            "total": 512
        }
    }));

    assert_eq!(app.shell.token_count, 512);
    assert_eq!(app.history.authoritative_session_input_tokens, Some(500));
    assert_eq!(app.history.authoritative_session_output_tokens, Some(12));
}

#[test]
fn session_stats_own_full_session_tokens_separately_from_the_visible_window() {
    let mut app = App::new("test", "sess");
    app.apply_session_stats(serde_json::json!({
        "session_id": "sess",
        "tokens": {
            "input": 45_000,
            "output": 5_000,
            "total": 50_000
        }
    }));
    app.record_durable_message_usage(
        "visible-message",
        &serde_json::json!({"input_tokens": 40, "output_tokens": 5}),
    );

    assert_eq!(app.history.authoritative_session_input_tokens, Some(45_000));
    assert_eq!(app.history.authoritative_session_output_tokens, Some(5_000));
    assert_eq!(app.history.durable_session_input_tokens, 40);
    assert_eq!(app.history.durable_session_output_tokens, 5);
}

#[test]
fn execution_projection_owner_rejects_lower_revision_for_same_execution() {
    use harness_contract::execution_graph::ExecutionGraph;
    use harness_contract::projection::{ExecutionProjection, ProjectionCommandAvailability};

    let projection = |revision: u64, objective: &str| ExecutionProjection {
        schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id: "execution-monotonic".to_string(),
        revision,
        cursor: revision,
        detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
        authorization_revision: 1,
        redaction_revision: "redaction-1".to_string(),
        session_id: Some("session-monotonic".to_string()),
        mission_id: None,
        task_id: None,
        turn_id: None,
        strategy: None,
        graph: harness_contract::execution_graph::project_execution_graph(&ExecutionGraph::new(
            objective,
        )),
        child_executions: Vec::new(),
        activities: Vec::new(),
        activity_relations: Vec::new(),
        goals: Vec::new(),
        agents: Vec::new(),
        teams: Vec::new(),
        relations: Vec::new(),
        approvals: Vec::new(),
        admissions: Vec::new(),
        outcomes: Vec::new(),
        interventions: Vec::new(),
        usage: Vec::new(),
        context: Vec::new(),
        evidence: Vec::new(),
        health: Vec::new(),
        recovery: Vec::new(),
        live: None,
        delivery_envelope: None,
        terminal_presentation: None,
        cancellation_receipt: None,
        available_commands: Vec::<ProjectionCommandAvailability>::new(),
    };

    let mut app = App::new("test", "session-monotonic");
    assert!(app.apply_execution_projection(projection(5, "revision five")));
    let graph_summary_id = app
        .execution
        .latest_execution_graph_summary
        .as_ref()
        .and_then(|summary| summary.graph_id.clone());
    assert!(!app.apply_execution_projection(projection(4, "stale revision four")));

    assert_eq!(
        app.execution
            .latest_execution_projection
            .as_ref()
            .map(|current| current.revision),
        Some(5)
    );
    assert_eq!(
        app.execution
            .latest_execution_graph_summary
            .as_ref()
            .and_then(|summary| summary.graph_id.clone()),
        graph_summary_id
    );
    assert!(app
        .execution
        .latest_execution_projection
        .as_ref()
        .is_some_and(|current| current.graph.objective == "revision five"));
}

#[test]
fn execution_projection_without_live_facts_cannot_reuse_previous_execution_values() {
    use harness_contract::execution_graph::ExecutionGraph;
    use harness_contract::projection::{ExecutionProjection, ProjectionCommandAvailability};

    let mut app = App::new("requested-model", "session-live-missing");
    app.execution.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::Complete);
    app.execution.current_execution_id = Some("execution-old".to_string());
    app.execution.current_turn_id = Some("turn-old".to_string());
    app.shell.effective_model = Some("old-effective-model".to_string());
    app.execution.context_used_tokens = Some(64_000);
    app.execution.context_window_tokens = Some(128_000);
    app.execution.context_remaining_tokens = Some(64_000);
    app.execution.context_usage_percent_bp = Some(5_000);
    app.execution.current_run_metrics = Some(Default::default());
    app.history.input_tokens = 64_000;
    app.history.output_tokens = 2_000;
    app.shell.token_count = 66_000;

    assert!(app.apply_execution_projection(ExecutionProjection {
        schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id: "execution-new".to_string(),
        revision: 1,
        cursor: 1,
        detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
        authorization_revision: 1,
        redaction_revision: "redaction-1".to_string(),
        session_id: Some("session-live-missing".to_string()),
        mission_id: None,
        task_id: None,
        turn_id: None,
        strategy: None,
        graph: harness_contract::execution_graph::project_execution_graph(&ExecutionGraph::new(
            "new execution"
        ),),
        child_executions: Vec::new(),
        activities: Vec::new(),
        activity_relations: Vec::new(),
        goals: Vec::new(),
        agents: Vec::new(),
        teams: Vec::new(),
        relations: Vec::new(),
        approvals: Vec::new(),
        admissions: Vec::new(),
        outcomes: Vec::new(),
        interventions: Vec::new(),
        usage: Vec::new(),
        context: Vec::new(),
        evidence: Vec::new(),
        health: Vec::new(),
        recovery: Vec::new(),
        live: None,
        delivery_envelope: None,
        terminal_presentation: None,
        cancellation_receipt: None,
        available_commands: Vec::<ProjectionCommandAvailability>::new(),
    }));

    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-new")
    );
    assert!(app.execution.current_execution_status.is_none());
    assert!(app.execution.current_turn_id.is_none());
    assert!(app.shell.effective_model.is_none());
    assert!(app.execution.context_used_tokens.is_none());
    assert!(app.execution.context_window_tokens.is_none());
    assert!(app.execution.current_run_metrics.is_none());
    assert_eq!(app.shell.token_count, 0);
}

#[test]
fn delayed_provider_attempt_cannot_replace_observed_projection_context_usage() {
    use harness_contract::projection::{
        ContextUsageProjection, ExecutionLiveState, ExecutionLiveStatus,
    };

    let mut app = App::new("requested-model", "session-context-authority");
    app.install_execution_live_facts(
        "execution-context-authority",
        &ExecutionLiveState {
            revision: 8,
            status: ExecutionLiveStatus::Complete,
            status_detail: None,
            turn_id: Some("turn-context-authority".to_string()),
            started_at_ms: 1,
            updated_at_ms: 2,
            last_progress_at_ms: 2,
            context_usage: Some(ContextUsageProjection {
                model: Some("observed-model".to_string()),
                window_tokens: Some(16_384),
                window_source: Some("configured".to_string()),
                input_tokens: Some(188),
                input_source: Some("provider_actual".to_string()),
                remaining_tokens: Some(16_196),
                usage_percent_bp: Some(114),
                request_sequence: Some(5),
                components: Vec::new(),
            }),
            metrics: harness_contract::projection::RunMetricsProjection {
                input_tokens: 188,
                output_tokens: 19,
                total_tokens: 207,
                ..Default::default()
            },
            latency: Default::default(),
            output_preview: None,
            output_preview_start_bytes: 0,
            output_bytes: 0,
            output_parts: Vec::new(),
            terminal_ref: Some("terminal-context-authority".to_string()),
            error: None,
        },
        None,
    );

    app.apply_event(CowdEvent::ProviderAttempt {
        model: "observed-model".to_string(),
        models_tried: vec!["observed-model".to_string()],
        context_window_tokens: 16_384,
        context_window_source: "configured".to_string(),
        packed_input_tokens: 5_536,
    });

    assert_eq!(app.execution.context_used_tokens, Some(188));
    assert_eq!(app.execution.context_remaining_tokens, Some(16_196));
    assert_eq!(app.execution.context_usage_percent_bp, Some(114));
    assert_eq!(
        app.execution.context_usage_source.as_deref(),
        Some("provider_actual")
    );
    assert_eq!(
        app.execution
            .current_run_metrics
            .as_ref()
            .unwrap()
            .total_tokens,
        207
    );
}

#[test]
fn invalidating_selected_execution_clears_identity_without_materialized_projection() {
    let mut app = App::new("requested-model", "session-selection");
    app.execution.current_execution_id = Some("execution-old".to_string());
    app.execution.current_turn_id = Some("turn-old".to_string());
    app.execution.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::Finalizing);
    app.shell.effective_model = Some("stale-model".to_string());
    assert!(app.execution.latest_execution_projection.is_none());

    assert!(app.invalidate_execution_projection("execution-old"));
    assert!(app.execution.current_execution_id.is_none());
    assert!(app.execution.current_turn_id.is_none());
    assert!(app.execution.current_execution_status.is_none());
    assert!(app.shell.effective_model.is_none());
}

#[test]
fn page_boundary_seamless() {
    let mut app = App::new("test", "sess");
    for i in 0..PAGE_SIZE {
        app.add_message("user", &format!("msg {i}"));
    }
    assert_eq!(app.timeline_len(), PAGE_SIZE);
    assert_eq!(app.timeline.timeline_pages.len(), 1);

    app.add_message("user", "overflow");
    assert_eq!(app.timeline_len(), PAGE_SIZE + 1);
    assert_eq!(app.timeline.timeline_pages.len(), 2);

    assert!(app.timeline_get(0).unwrap().full_text().contains("msg 0"));
    assert!(app
        .timeline_get(PAGE_SIZE - 1)
        .unwrap()
        .full_text()
        .contains(&format!("msg {}", PAGE_SIZE - 1)));
    assert!(app
        .timeline_get(PAGE_SIZE)
        .unwrap()
        .full_text()
        .contains("overflow"));

    let count = app.timeline_iter().count();
    assert_eq!(count, PAGE_SIZE + 1);
}

#[test]
fn memory_soft_cap() {
    let mut app = App::new("test", "sess");
    for i in 0..(SOFT_CAP + 500) {
        app.add_message("user", &format!("msg {i}"));
    }
    assert!(app.timeline_len() <= SOFT_CAP);
    let first_entry = app.timeline_get(0).unwrap();
    assert!(!first_entry.full_text().contains("msg 0"));
}

#[test]
fn empty_timeline_handled() {
    let app = App::new("test", "sess");
    assert!(app.timeline_is_empty());
    assert_eq!(app.timeline_len(), 0);
    assert!(app.timeline_get(0).is_none());
    assert_eq!(app.timeline_iter().count(), 0);
}

#[test]
fn unresolved_startup_model_is_not_claimed_as_requested_model() {
    let app = App::new("unresolved", "sess");
    assert_eq!(app.shell.model, "unresolved");
    assert_eq!(app.shell.requested_model, None);
    assert_eq!(app.shell.effective_model, None);
}

#[test]
fn oversized_durable_history_exposes_the_visible_window_limit() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: Vec::new(),
            total: SOFT_CAP + 1,
            offset: 0,
            from_seq: Some(0),
            next_seq: None,
            limit: PAGE_SIZE,
            has_more: false,
        },
    });

    assert!(app.history.history_window_truncated);
    assert!(app.workbench.system_notices.iter().any(|notice| {
        notice.kind == SystemNoticeKind::Warning
            && notice.content.contains("Compact or checkpoint")
            && notice.content.contains(&SOFT_CAP.to_string())
    }));
    assert!(app
        .shell
        .notification
        .as_deref()
        .is_some_and(|notice| notice.contains("Durable history")));
}

#[test]
fn body_free_history_index_drives_session_coverage_without_materializing_messages() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::SessionHistoryIndexLoaded {
        projection: crate::protocol::SessionHistoryIndexProjection {
            schema_version: 1,
            session_id: "sess".to_string(),
            projection_generation: 9,
            durable_cursor: 42,
            event_cursor: 41,
            history_revision: 7,
            total_messages: 100_000,
            total_bytes: 8_000_000,
            latest_checkpoint_sequence: Some(90_000),
            latest_checkpoint_event_id: Some("checkpoint-1".to_string()),
            index_generation: 4,
            indexed_through_sequence: Some(99_999),
            index_card_count: 250,
            index_complete: true,
            recovery_state: crate::protocol::SessionHistoryRecoveryState::Ready,
            recent_metadata: Vec::new(),
            cards: Vec::new(),
        },
    });

    assert_eq!(app.history.history_total_messages, 100_000);
    assert!(app.history.history_has_older);
    assert_eq!(
        app.history
            .session_history_index
            .as_ref()
            .map(|index| (index.projection_generation, index.durable_cursor)),
        Some((9, 42))
    );
    assert!(app.timeline_is_empty());
}

#[test]
fn session_input_projection_is_a_bounded_runtime_owned_queue_view() {
    let mut app = App::new("test", "sess");
    app.apply_session_input_projection(serde_json::json!({
        "inputs": [
            {
                "input_id": "queued-a",
                "status": "queued_next",
                "decision": "enqueue_next_step",
                "content_preview": "follow up with tests"
            },
            {
                "input_id": "done-b",
                "status": "consumed",
                "decision": "start_new_turn",
                "content_preview": "already consumed"
            }
        ]
    }));

    assert_eq!(app.queued_follow_up_count(), 1);
    let preview = app.queued_follow_up_preview().expect("queued preview");
    assert_eq!(preview.input_id, "queued-a");
    assert_eq!(preview.content_preview, "follow up with tests");
    assert!(app.workbench.system_notices.iter().any(|notice| {
        notice.content.contains("/queue edit queued-a")
            && notice.content.contains("/queue cancel queued-a")
    }));
    assert!(app
        .workbench
        .pending_inputs
        .iter()
        .all(|input| input.input_id != "done-b"));

    app.apply_session_input_projection(serde_json::json!({
        "pending_count": 0,
        "inputs": [
            {
                "input_id": "queued-a",
                "status": "consumed",
                "decision": "start_new_turn",
                "content_preview": "follow up with tests"
            }
        ]
    }));
    assert_eq!(
        app.queued_follow_up_count(),
        0,
        "a consumed canonical projection must clear the composer queue"
    );
    assert!(app.queued_follow_up_preview().is_none());
}

#[test]
fn incremental_history_hydration_preserves_and_advances_the_existing_window() {
    use crate::protocol::SessionHistoryHydrationKind;

    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::SessionHistoryHydrated {
        session_id: "sess".to_string(),
        kind: SessionHistoryHydrationKind::InitialWindow,
        duration_ms: 4,
        message_count: 26,
        page_count: 1,
        oldest_offset: 0,
        total_messages: 26,
        next_sequence: 26,
        has_older: false,
    });
    app.apply_event(CowdEvent::SessionHistoryHydrated {
        session_id: "sess".to_string(),
        kind: SessionHistoryHydrationKind::IncrementalCatchup,
        duration_ms: 2,
        message_count: 2,
        page_count: 1,
        oldest_offset: 0,
        total_messages: 28,
        next_sequence: 28,
        has_older: false,
    });

    assert_eq!(app.history.history_oldest_offset, 0);
    assert_eq!(app.history.history_window_end_offset, 28);
    assert_eq!(app.history.history_total_messages, 28);
    assert!(!app.history.history_has_older);

    app.history.history_oldest_offset = 5;
    app.history.history_window_end_offset = 15;
    app.history.history_total_messages = 28;
    app.apply_event(CowdEvent::SessionHistoryHydrated {
        session_id: "sess".to_string(),
        kind: SessionHistoryHydrationKind::IncrementalCatchup,
        duration_ms: 1,
        message_count: 2,
        page_count: 1,
        oldest_offset: 0,
        total_messages: 30,
        next_sequence: 30,
        has_older: false,
    });

    assert_eq!(
        (
            app.history.history_oldest_offset,
            app.history.history_window_end_offset
        ),
        (5, 15),
        "catch-up while browsing a middle window must not invent a new pagination offset"
    );
    assert_eq!(app.history.history_total_messages, 30);

    app.history.history_oldest_offset = 10;
    app.history.history_window_end_offset = SOFT_CAP + 10;
    app.history.history_total_messages = SOFT_CAP + 10;
    app.history.history_has_older = true;
    app.apply_event(CowdEvent::SessionHistoryHydrated {
        session_id: "sess".to_string(),
        kind: SessionHistoryHydrationKind::IncrementalCatchup,
        duration_ms: 1,
        message_count: 2,
        page_count: 1,
        oldest_offset: 0,
        total_messages: SOFT_CAP + 12,
        next_sequence: SOFT_CAP + 12,
        has_older: false,
    });

    assert_eq!(app.history.history_oldest_offset, 12);
    assert_eq!(app.history.history_window_end_offset, SOFT_CAP + 12);
    assert!(app.history.history_has_older);
}

#[test]
fn fifty_thousand_message_catchup_does_not_contaminate_a_middle_history_window() {
    let page = crate::protocol::SessionMessagesPage {
        session_id: "sess".to_string(),
        messages: vec![crate::protocol::SessionMessageProjection {
            id: "new-message-50000".to_string(),
            session_id: "sess".to_string(),
            sequence: 50_000,
            role: "assistant".to_string(),
            blocks: vec![serde_json::json!({
                "type": "text",
                "text": "new tail answer"
            })],
            created_at_ms: 50_000,
            token_usage: None,
            tool_use_id: None,
            tool_name: None,
        }],
        total: 50_001,
        offset: 50_000,
        from_seq: Some(50_000),
        next_seq: Some(50_001),
        limit: 500,
        has_more: false,
    };
    let mut app = App::new("test", "sess");
    app.history.history_oldest_offset = 24_000;
    app.history.history_window_end_offset = 25_000;
    app.history.history_total_messages = 50_000;

    app.apply_event(CowdEvent::SessionHistoryCatchupPage { page: page.clone() });
    app.apply_event(CowdEvent::SessionHistoryHydrated {
        session_id: "sess".to_string(),
        kind: crate::protocol::SessionHistoryHydrationKind::IncrementalCatchup,
        duration_ms: 1,
        message_count: 1,
        page_count: 1,
        oldest_offset: 50_000,
        total_messages: 50_001,
        next_sequence: 50_001,
        has_older: true,
    });

    assert!(
        app.timeline_iter()
            .all(|(_, entry)| !entry.full_text().contains("new tail answer")),
        "a reconnect catch-up must not splice the newest message into a browsed middle window"
    );
    assert_eq!(app.history.history_oldest_offset, 24_000);
    assert_eq!(app.history.history_window_end_offset, 25_000);
    assert_eq!(app.history.history_total_messages, 50_001);

    app.history.history_oldest_offset = 49_000;
    app.history.history_window_end_offset = 50_000;
    app.history.history_total_messages = 50_000;
    app.apply_event(CowdEvent::SessionHistoryCatchupPage { page });
    assert!(app
        .timeline_iter()
        .any(|(_, entry)| entry.full_text().contains("new tail answer")));
}

#[test]
fn terminal_without_complete_causal_identity_is_visible_and_fail_closed() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation("execution-live", "turn-live"),
            text: "partial".to_string(),
            start_bytes: 0,
            end_bytes: 7,
            stream_revision: 7,
        },
    });
    let mut incomplete = correlation("execution-live", "turn-live");
    incomplete.message_id = Some("assistant-live".to_string());
    incomplete.terminal_id = None;
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
            correlation: incomplete,
            assistant_text: "must not commit".to_string(),
            sequence: Some(1),
            iterations: 1,
            token_usage: None,
        },
    });

    assert_eq!(app.execution.telemetry.orphan_event_count, 1);
    assert!(app
        .shell
        .notification
        .as_deref()
        .is_some_and(|value| value.contains("Rejected terminal")));
    assert!(app
        .timeline_iter()
        .all(|(_, entry)| !entry.full_text().contains("must not commit")));
}

#[test]
fn e10_history_failure_is_visible_without_polluting_the_transcript() {
    let mut app = App::new("test", "sess");
    app.add_message("assistant", "durable answer");
    let timeline_before = app.timeline_clone_vec();

    app.apply_event(CowdEvent::SessionHistoryHydrationFailed {
        session_id: "sess".to_string(),
        error: "HTTP 500 malformed stored message".to_string(),
    });

    assert!(!app.history.history_hydrated);
    assert_eq!(
        app.history.history_hydration_error.as_deref(),
        Some("HTTP 500 malformed stored message")
    );
    assert_eq!(app.timeline_clone_vec(), timeline_before);
    assert!(app.workbench.system_notices.iter().any(|notice| {
        notice.kind == SystemNoticeKind::Error
            && notice.content.contains("Session history unavailable")
            && notice.content.contains("malformed stored message")
    }));
}

#[test]
fn e10_closed_session_admission_restores_the_draft_without_ghost_messages() {
    let mut app = App::new("test", "sess");
    let message_id = "tui:e10-message".to_string();
    app.begin_message_admission("must remain editable", message_id.clone(), 11, true);
    assert!(app
        .timeline_iter()
        .any(|(_, entry)| entry.full_text().contains("must remain editable")));

    app.apply_event(CowdEvent::MessageAdmissionFailed {
        session_id: "sess".to_string(),
        client_message_id: message_id,
        submission_generation: 11,
        original_text: "must remain editable".to_string(),
        started_new_turn: true,
        error: "session is closed".to_string(),
    });

    assert_eq!(app.shell.input.text(), "must remain editable");
    assert!(app.execution.pending_message_admissions.is_empty());
    assert!(app
        .timeline_iter()
        .all(|(_, entry)| !entry.full_text().contains("must remain editable")));
    assert!(app.workbench.system_notices.iter().any(|notice| {
        notice.kind == SystemNoticeKind::Error
            && notice.content.contains("draft was restored")
            && notice.content.contains("session is closed")
    }));
}

#[test]
fn e10_session_authorization_revocation_clears_all_session_derived_state() {
    let mut app = App::new("private-model", "sess");
    app.add_message("assistant", "private transcript");
    app.shell.input.set_text("private draft");
    app.shell.effective_model = Some("private-effective-model".to_string());
    app.execution.latest_context_envelope = Some(serde_json::json!({"secret": true}));

    app.apply_event(CowdEvent::SessionAuthorizationRevoked {
        session_id: "sess".to_string(),
        reason: "credential epoch changed".to_string(),
    });

    assert!(app.timeline_is_empty());
    assert!(app.shell.input.text().is_empty());
    assert!(app.shell.effective_model.is_none());
    assert!(app.execution.latest_context_envelope.is_none());
    assert_eq!(app.shell.model, "unavailable");
    assert!(app.workbench.system_notices.iter().any(|notice| {
        notice.kind == SystemNoticeKind::Error
            && notice.content.contains("Session authorization revoked")
    }));
}

#[test]
fn execution_policy_is_unavailable_until_gateway_truth_is_loaded() {
    let app = App::new("test", "sess");

    assert_eq!(app.shell.execution_policy_preset, "unavailable");
}

#[test]
fn session_activity_stats_cover_current_conversation() {
    let mut app = App::new("test", "sess");
    app.add_message("user", "hi");
    app.add_message("system", "memory update");
    app.timeline_push(TimelineEntry::Thinking {
        id: 1,
        causal_item_id: None,
        causality: None,
        content: "reasoning".to_string(),
        complete: true,
        expanded: false,
    });
    app.timeline_push(TimelineEntry::ToolCall {
        id: "tool-1".to_string(),
        name: "bash".to_string(),
        preview: "echo ok".to_string(),
        output: "ok".to_string(),
        done: true,
        expanded: false,
        exit_code: Some(0),
        causality: None,
    });
    app.add_message("assistant", "done");

    let stats = app.session_activity_stats();
    assert_eq!(stats.thinking_count, 1);
    assert_eq!(stats.tool_count, 1);
    assert_eq!(stats.message_count, 2);
    assert_eq!(stats.event_count, 4);
}

#[test]
fn add_entry_appends_to_last_page() {
    let mut app = App::new("test", "sess");
    for i in 0..300 {
        app.timeline_push(make_msg(&format!("entry {i}")));
    }
    assert_eq!(app.timeline_len(), 300);
    assert_eq!(app.timeline.timeline_pages.len(), 1);
    assert_eq!(app.timeline.timeline_pages[0].entries.len(), 300);
    assert_eq!(app.timeline.timeline_pages[0].start_index, 0);
}

#[test]
fn get_entry_cross_page() {
    let mut app = App::new("test", "sess");
    for i in 0..(PAGE_SIZE * 3 + 200) {
        app.timeline_push(make_msg(&format!("entry {i}")));
    }
    assert_eq!(app.timeline_len(), PAGE_SIZE * 3 + 200);
    assert!(app.timeline_get(0).unwrap().full_text().contains("entry 0"));
    assert!(app
        .timeline_get(PAGE_SIZE)
        .unwrap()
        .full_text()
        .contains(&format!("entry {}", PAGE_SIZE)));
    assert!(app
        .timeline_get(PAGE_SIZE * 2 + 50)
        .unwrap()
        .full_text()
        .contains(&format!("entry {}", PAGE_SIZE * 2 + 50)));
}

#[test]
fn cursor_up_down_works_across_pages() {
    let mut app = App::new("test", "sess");
    for i in 0..600 {
        app.timeline_push(TimelineEntry::Thinking {
            id: i,
            causal_item_id: None,
            causality: None,
            content: format!("think {i}"),
            complete: true,
            expanded: false,
        });
    }
    app.timeline.timeline_cursor = 599;
    let moved = app.cursor_up();
    assert!(moved);
    assert!(app.timeline.timeline_cursor < 599);
}

fn correlation(execution_id: &str, turn_id: &str) -> crate::protocol::GatewayEventCorrelation {
    crate::protocol::GatewayEventCorrelation {
        session_id: "sess".to_string(),
        execution_id: Some(execution_id.to_string()),
        turn_id: Some(turn_id.to_string()),
        ..Default::default()
    }
}

#[test]
fn root_presentation_has_one_preview_owner_and_durable_commit_replaces_it() {
    use harness_contract::live::TerminalDeliveryEvent;

    let mut app = App::new("test", "sess");
    app.execution.current_execution_id = Some("execution-root".to_string());
    app.execution.current_turn_id = Some("turn-root".to_string());
    let correlation = correlation("execution-root", "turn-root");
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
        correlation: correlation.clone(),
        delivery: TerminalDeliveryEvent::TerminalPresentationStarted {
            presentation_id: "presentation-root".to_string(),
            attempt_id: "attempt-1".to_string(),
            envelope_id: "envelope-1".to_string(),
            envelope_revision: 1,
            objective_scope: harness_contract::outcome::AnswerObjectiveScope::Root,
        },
    });
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
        correlation: correlation.clone(),
        delivery: TerminalDeliveryEvent::TextDelta {
            presentation_id: "presentation-root".to_string(),
            attempt_id: "attempt-1".to_string(),
            byte_start: 0,
            byte_end: 7,
            delta: "preview".to_string(),
        },
    });
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
        correlation: correlation.clone(),
        delivery: TerminalDeliveryEvent::TerminalPresentationCommitted {
            presentation_id: "presentation-root".to_string(),
            attempt_id: "attempt-1".to_string(),
            answer_origin: harness_contract::outcome::AnswerOrigin::TerminalNarrator,
            terminal_id: "terminal-root".to_string(),
        },
    });
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
        correlation: correlation.clone(),
        delivery: TerminalDeliveryEvent::TextDelta {
            presentation_id: "presentation-root".to_string(),
            attempt_id: "attempt-1".to_string(),
            byte_start: 7,
            byte_end: 12,
            delta: " late".to_string(),
        },
    });

    let mut committed = correlation;
    committed.message_id = Some("assistant-root".to_string());
    committed.terminal_id = Some("terminal-root".to_string());
    committed.part_id = Some("terminal-message:assistant-root".to_string());
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalCommitted {
        correlation: committed,
        assistant_text: "authoritative final".to_string(),
        sequence: Some(2),
        iterations: 1,
        token_usage: None,
    });

    let assistant = app
        .timeline_iter()
        .filter_map(|(_, entry)| match entry {
            TimelineEntry::Message { role, content, .. } if role == "assistant" => {
                Some(content.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(assistant, vec!["authoritative final"]);
    assert!(app
        .execution
        .turn_interaction
        .presentation
        .active_root
        .is_none());
}

#[test]
fn dropped_abort_then_projection_resync_clears_orphaned_root_preview() {
    use harness_contract::execution_graph::ExecutionGraph;
    use harness_contract::live::TerminalDeliveryEvent;
    use harness_contract::projection::{ExecutionProjection, ProjectionCommandAvailability};

    let mut app = App::new("test", "sess");
    app.execution.current_execution_id = Some("execution-root".to_string());
    app.execution.current_turn_id = Some("turn-root".to_string());
    let correlation = correlation("execution-root", "turn-root");
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
        correlation: correlation.clone(),
        delivery: TerminalDeliveryEvent::TerminalPresentationStarted {
            presentation_id: "presentation-root".to_string(),
            attempt_id: "attempt-1".to_string(),
            envelope_id: "envelope-1".to_string(),
            envelope_revision: 1,
            objective_scope: harness_contract::outcome::AnswerObjectiveScope::Root,
        },
    });
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
        correlation: correlation.clone(),
        delivery: TerminalDeliveryEvent::TextDelta {
            presentation_id: "presentation-root".to_string(),
            attempt_id: "attempt-1".to_string(),
            byte_start: 0,
            byte_end: 7,
            delta: "preview".to_string(),
        },
    });
    assert!(app
        .execution
        .turn_interaction
        .presentation
        .active_root
        .is_some());

    // The reconstructible Abort is intentionally absent. The canonical
    // snapshot carries neither an active presentation nor a durable
    // terminal/cancellation winner and must therefore close the orphan.
    assert!(app.apply_execution_projection(ExecutionProjection {
        schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id: "execution-root".to_string(),
        revision: 1,
        cursor: 1,
        detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
        authorization_revision: 1,
        redaction_revision: "redaction-1".to_string(),
        session_id: Some("sess".to_string()),
        mission_id: None,
        task_id: None,
        turn_id: Some("turn-root".to_string()),
        strategy: None,
        graph: harness_contract::execution_graph::project_execution_graph(&ExecutionGraph::new(
            "recover dropped abort"
        ),),
        child_executions: Vec::new(),
        activities: Vec::new(),
        activity_relations: Vec::new(),
        goals: Vec::new(),
        agents: Vec::new(),
        teams: Vec::new(),
        relations: Vec::new(),
        approvals: Vec::new(),
        admissions: Vec::new(),
        outcomes: Vec::new(),
        interventions: Vec::new(),
        usage: Vec::new(),
        context: Vec::new(),
        evidence: Vec::new(),
        health: Vec::new(),
        recovery: Vec::new(),
        live: None,
        delivery_envelope: None,
        terminal_presentation: None,
        cancellation_receipt: None,
        available_commands: Vec::<ProjectionCommandAvailability>::new(),
    }));
    assert!(app
        .execution
        .turn_interaction
        .presentation
        .active_root
        .is_none());
    assert!(app.execution.turn_interaction.root_preview_closed());

    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
        correlation,
        delivery: TerminalDeliveryEvent::TextDelta {
            presentation_id: "presentation-root".to_string(),
            attempt_id: "attempt-1".to_string(),
            byte_start: 7,
            byte_end: 12,
            delta: " late".to_string(),
        },
    });
    assert!(app.timeline_iter().all(|(_, entry)| {
        !matches!(
            entry,
            TimelineEntry::Message { role, .. } if role == "assistant"
        )
    }));
}

#[test]
fn http_and_sse_cancellation_receipts_dedupe_to_activity_without_assistant_text() {
    let mut app = App::new("test", "sess");
    app.execution.current_execution_id = Some("execution-cancel".to_string());
    app.execution.current_turn_id = Some("turn-cancel".to_string());
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TextDelta {
        correlation: correlation("execution-cancel", "turn-cancel"),
        text: "transient".to_string(),
        start_bytes: 0,
        end_bytes: 9,
        stream_revision: 9,
    });
    let mut receipt = harness_contract::turn::CancellationReceipt {
        cancellation_id: "cancel-once".to_string(),
        session_id: "sess".to_string(),
        turn_id: "turn-cancel".to_string(),
        execution_id: "execution-cancel".to_string(),
        actor_id: "principal:user".to_string(),
        cause: harness_contract::turn::CancellationCause::UserRequested,
        reason: Some("user requested".to_string()),
        requested_at_ms: 100,
        effective_at_ms: None,
        status: harness_contract::turn::CancellationStatus::Requested,
        journal_sequence: 4,
        projection_revision: 1,
    };
    app.apply_cancellation_receipt(receipt.clone());
    assert_ne!(
        app.execution.current_execution_status,
        Some(harness_contract::projection::ExecutionLiveStatus::Cancelled)
    );
    receipt.effective_at_ms = Some(120);
    receipt.status = harness_contract::turn::CancellationStatus::Cancelled;
    for _ in 0..2 {
        app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
            correlation: correlation("execution-cancel", "turn-cancel"),
            delivery: harness_contract::live::TerminalDeliveryEvent::CancellationCommitted {
                receipt: receipt.clone(),
            },
        });
    }

    assert!(!app.timeline_iter().any(|(_, entry)| matches!(
        entry,
        TimelineEntry::Message { role, .. } if role == "assistant"
    )));
    assert_eq!(
        app.workbench
            .system_notices
            .iter()
            .filter(|notice| notice.content.contains("cancel-once"))
            .count(),
        1
    );
    assert_eq!(
        app.execution.current_execution_status,
        Some(harness_contract::projection::ExecutionLiveStatus::Cancelled)
    );

    app.install_execution_live_facts(
        "execution-cancel",
        &harness_contract::projection::ExecutionLiveState {
            revision: 10,
            status: harness_contract::projection::ExecutionLiveStatus::Cancelled,
            status_detail: Some("user requested".to_string()),
            turn_id: Some("turn-cancel".to_string()),
            started_at_ms: 1,
            updated_at_ms: 120,
            last_progress_at_ms: 120,
            context_usage: None,
            metrics: Default::default(),
            latency: Default::default(),
            output_preview: Some("must stay hidden".to_string()),
            output_preview_start_bytes: 0,
            output_bytes: 16,
            output_parts: vec![harness_contract::projection::ExecutionLiveOutputPart {
                model_step_id: "step-cancelled".to_string(),
                item_id: "item-cancelled".to_string(),
                part_id: "part-cancelled".to_string(),
                causal_sequence: 1,
                completed: false,
                preview: Some("must stay hidden".to_string()),
                preview_start_bytes: 0,
                bytes: 16,
            }],
            terminal_ref: None,
            error: None,
        },
        None,
    );
    assert!(!app.timeline_iter().any(|(_, entry)| matches!(
        entry,
        TimelineEntry::Message { role, content, .. }
            if role == "assistant" && content.contains("stay hidden")
    )));

    let mut late_terminal = correlation("execution-cancel", "turn-cancel");
    late_terminal.message_id = Some("assistant-late".to_string());
    late_terminal.terminal_id = Some("terminal-late".to_string());
    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalCommitted {
        correlation: late_terminal,
        assistant_text: "must not resurrect cancelled output".to_string(),
        sequence: Some(5),
        iterations: 1,
        token_usage: None,
    });
    assert_eq!(
        app.execution.current_execution_status,
        Some(harness_contract::projection::ExecutionLiveStatus::Cancelled)
    );
    assert!(!app.timeline_iter().any(|(_, entry)| matches!(
        entry,
        TimelineEntry::Message { role, content, .. }
            if role == "assistant" && content.contains("resurrect")
    )));

    app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::TerminalDelivery {
        correlation: correlation("execution-cancel", "turn-cancel"),
        delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationStarted {
            presentation_id: "late-presentation".to_string(),
            attempt_id: "late-attempt".to_string(),
            envelope_id: "late-envelope".to_string(),
            envelope_revision: 1,
            objective_scope: harness_contract::outcome::AnswerObjectiveScope::Root,
        },
    });
    assert!(app
        .execution
        .turn_interaction
        .presentation
        .active_root
        .is_none());
}

#[test]
fn causal_reasoning_items_remain_distinct_in_the_tui_timeline() {
    let mut app = App::new("test", "sess");
    app.execution.current_execution_id = Some("execution-causal".to_string());
    app.execution.current_turn_id = Some("turn-causal".to_string());
    for (item_id, text) in [("reasoning-a", "inspect"), ("reasoning-b", "decide")] {
        let mut item = correlation("execution-causal", "turn-causal");
        item.model_step_id = Some("step-causal".to_string());
        item.item_id = Some(item_id.to_string());
        item.segment_id = Some(format!("{item_id}:reasoning-summary:0"));
        app.apply_gateway_session_event(
            crate::protocol::GatewaySessionEvent::ReasoningSummaryDelta {
                correlation: item.clone(),
                summary: text.to_string(),
            },
        );
        app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::ItemCompleted {
            correlation: item,
            kind: "public_reasoning".to_string(),
        });
    }
    let items = app
        .timeline_clone_vec()
        .into_iter()
        .filter_map(|entry| match entry {
            TimelineEntry::Thinking {
                causal_item_id,
                content,
                complete,
                ..
            } => Some((causal_item_id, content, complete)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0],
        (
            Some("reasoning-a:reasoning-summary:0".to_string()),
            "inspect".to_string(),
            true
        )
    );
    assert_eq!(
        items[1],
        (
            Some("reasoning-b:reasoning-summary:0".to_string()),
            "decide".to_string(),
            true
        )
    );
}

#[test]
fn canonical_cross_surface_fixture_keeps_causal_order_and_parallel_tool_waves() {
    let fixture: serde_json::Value =
        serde_json::from_str(harness_contract::live::CAUSAL_SURFACE_TIMELINE_V1_FIXTURE_JSON)
            .expect("canonical causal fixture");
    let session_id = fixture["session_id"].as_str().expect("fixture session");
    let mut app = App::new("fixture-model", session_id);

    for payload in fixture["events"].as_array().expect("fixture events") {
        let event = crate::gateway_client::gateway_sse_json_to_cowd_event_for_session(
            payload,
            Some(session_id),
        )
        .expect("fixture event must map to the TUI protocol");
        app.apply_event(event);
    }

    let rows = app
        .timeline_iter()
        .filter_map(|(_, entry)| match entry {
            TimelineEntry::Thinking {
                causality: Some(causality),
                ..
            } => causality.item_id.clone(),
            TimelineEntry::ToolCall {
                causality: Some(causality),
                ..
            } => causality.tool_call_id.clone(),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected = fixture["expected_activity"]
        .as_array()
        .expect("expected activity")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);

    let tools = app
        .timeline_iter()
        .filter_map(|(_, entry)| match entry {
            TimelineEntry::ToolCall {
                causality: Some(causality),
                ..
            } => Some((
                causality.tool_call_id.clone().unwrap_or_default(),
                causality.wave,
                causality.lane,
                causality.lane_count,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tools,
        vec![
            ("tool-a".to_string(), 0, 0, 2),
            ("tool-b".to_string(), 0, 1, 2),
            ("tool-c".to_string(), 1, 0, 1),
        ]
    );
    assert!(app.timeline_iter().any(|(_, entry)| matches!(
        entry,
        TimelineEntry::Message {
            role,
            content,
            ..
        } if role == "assistant" && content == "完成"
    )));
}

#[test]
fn durable_history_hydrates_full_transcript_and_deduplicates_replayed_terminal() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![
                crate::protocol::SessionMessageProjection {
                    id: "user-1".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 0,
                    role: "user".to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "text",
                        "text": "historical question"
                    })],
                    created_at_ms: 1_000,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                },
                crate::protocol::SessionMessageProjection {
                    id: "assistant-1".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 1,
                    role: "assistant".to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "text",
                        "text": "historical answer"
                    })],
                    created_at_ms: 2_000,
                    token_usage: Some(serde_json::json!({
                        "input_tokens": 12,
                        "output_tokens": 3
                    })),
                    tool_use_id: None,
                    tool_name: None,
                },
            ],
            total: 2,
            offset: 0,
            from_seq: Some(0),
            next_seq: Some(2),
            limit: 500,
            has_more: false,
        },
    });

    let mut terminal = correlation("execution-old", "turn-old");
    terminal.message_id = Some("assistant-1".to_string());
    terminal.terminal_id = Some("terminal-old".to_string());
    terminal.replayed = true;
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
            correlation: terminal,
            assistant_text: "historical answer".to_string(),
            sequence: Some(1),
            iterations: 1,
            token_usage: None,
        },
    });

    assert!(app.history.history_hydrated);
    assert_eq!(app.timeline_len(), 2);
    assert_eq!(app.history.durable_session_input_tokens, 12);
    assert_eq!(app.history.durable_session_output_tokens, 3);
    assert_eq!(
        app.timeline_get(0).unwrap().full_text(),
        "historical question"
    );
    assert_eq!(
        app.timeline_get(1).unwrap().full_text(),
        "historical answer"
    );
    assert_eq!(
        app.shell.input_history,
        vec!["historical question".to_string()]
    );
}

#[test]
fn durable_history_keeps_provider_transcript_evidence_without_duplicate_answer() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![
                crate::protocol::SessionMessageProjection {
                    id: "assistant:turn-1:transcript:0".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 1,
                    role: "assistant".to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "text",
                        "text": "premature answer"
                    })],
                    created_at_ms: 1_000,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                },
                crate::protocol::SessionMessageProjection {
                    id: "assistant:turn-1".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 2,
                    role: "assistant".to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "text",
                        "text": "verified final answer"
                    })],
                    created_at_ms: 2_000,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                },
            ],
            total: 2,
            offset: 0,
            from_seq: Some(1),
            next_seq: Some(3),
            limit: 500,
            has_more: false,
        },
    });

    let messages = app
        .timeline_iter()
        .filter_map(|(_, entry)| match entry {
            TimelineEntry::Message { content, .. } => Some(content.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(messages, vec!["verified final answer"]);
}

#[test]
fn durable_history_restores_tool_use_and_result_as_one_deduplicated_card() {
    let page = crate::protocol::SessionMessagesPage {
        session_id: "sess".to_string(),
        messages: vec![
            crate::protocol::SessionMessageProjection {
                id: "assistant-tool-use".to_string(),
                session_id: "sess".to_string(),
                sequence: 0,
                role: "assistant".to_string(),
                blocks: vec![serde_json::json!({
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "read_file",
                    "input": "{\"path\":\"Cargo.toml\"}"
                })],
                created_at_ms: 1_000,
                token_usage: None,
                tool_use_id: Some("tool-1".to_string()),
                tool_name: Some("read_file".to_string()),
            },
            crate::protocol::SessionMessageProjection {
                id: "tool-result".to_string(),
                session_id: "sess".to_string(),
                sequence: 1,
                role: "tool".to_string(),
                blocks: vec![serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "tool_name": "read_file",
                    "output": "workspace manifest",
                    "is_error": false
                })],
                created_at_ms: 2_000,
                token_usage: None,
                tool_use_id: Some("tool-1".to_string()),
                tool_name: Some("read_file".to_string()),
            },
        ],
        total: 2,
        offset: 0,
        from_seq: Some(0),
        next_seq: Some(2),
        limit: 500,
        has_more: false,
    };
    let mut app = App::new("test", "sess");

    app.apply_event(CowdEvent::SessionHistoryPage { page: page.clone() });
    app.apply_event(CowdEvent::SessionHistoryPage { page });

    let tools = app
        .timeline_iter()
        .filter_map(|(_, entry)| match entry {
            TimelineEntry::ToolCall {
                id,
                name,
                output,
                done,
                exit_code,
                ..
            } => Some((id, name, output, done, exit_code)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tools.len(), 1);
    assert!(
        tools[0].0.starts_with("tool-instance|"),
        "history tool cards must use their collision-safe canonical instance identity"
    );
    assert_eq!(tools[0].1, "read_file");
    assert_eq!(tools[0].2, "workspace manifest");
    assert!(*tools[0].3);
    assert_eq!(*tools[0].4, Some(0));
    assert!(
            app.timeline_iter()
                .all(|(_, entry)| !matches!(entry, TimelineEntry::Message { content, .. } if content.is_empty())),
            "tool-only assistant messages must not become empty chat bubbles"
        );
}

#[test]
fn stable_turn_identity_prevents_cross_turn_delta_and_terminal_corruption() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation("execution-1", "turn-1"),
            text: "same prefix first".to_string(),
            start_bytes: 0,
            end_bytes: 17,
            stream_revision: 17,
        },
    });
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
            correlation: {
                let mut correlation = correlation("execution-2", "turn-2");
                correlation.message_id = Some("user-second".to_string());
                correlation
            },
            content: "second question".to_string(),
            sequence: 1,
            created_at_ms: 2_000,
        },
    });
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation("execution-2", "turn-2"),
            text: "same prefix second".to_string(),
            start_bytes: 0,
            end_bytes: 18,
            stream_revision: 18,
        },
    });
    let mut first_terminal = correlation("execution-1", "turn-1");
    first_terminal.message_id = Some("assistant-first".to_string());
    first_terminal.terminal_id = Some("terminal-first".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
            correlation: first_terminal,
            assistant_text: "first terminal".to_string(),
            sequence: Some(2),
            iterations: 1,
            token_usage: None,
        },
    });

    let messages = app
        .timeline_iter()
        .filter_map(|(_, entry)| match entry {
            TimelineEntry::Message {
                content, identity, ..
            } => Some((content.as_str(), identity.as_ref())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(messages.len(), 3);
    assert!(
        messages
            .iter()
            .all(|(content, _)| *content != "first terminal"),
        "a stale terminal must not materialize prose into the active turn"
    );
    assert!(messages
        .iter()
        .any(|(content, _)| *content == "same prefix second"));
    assert_eq!(
        app.execution.current_execution_status,
        Some(harness_contract::projection::ExecutionLiveStatus::Queued)
    );
    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-2")
    );
    assert_eq!(app.execution.telemetry.orphan_event_count, 1);
}

#[test]
fn durable_history_race_reconciles_live_reply_before_terminal_commit() {
    let mut app = App::new("test", "sess");
    let mut live = correlation("execution-race", "turn-race");
    live.part_id = Some("item-text-1:text:0".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: live,
            text: "streamed answer".to_string(),
            start_bytes: 0,
            end_bytes: 15,
            stream_revision: 15,
        },
    });
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![crate::protocol::SessionMessageProjection {
                id: "assistant-race".to_string(),
                session_id: "sess".to_string(),
                sequence: 1,
                role: "assistant".to_string(),
                blocks: vec![serde_json::json!({
                    "type": "text",
                    "text": "streamed answer",
                    "cowd_turn_id": "turn-race"
                })],
                created_at_ms: 2_000,
                token_usage: None,
                tool_use_id: None,
                tool_name: None,
            }],
            total: 2,
            offset: 0,
            from_seq: Some(0),
            next_seq: Some(2),
            limit: 500,
            has_more: false,
        },
    });

    let mut terminal = correlation("execution-race", "turn-race");
    terminal.part_id = Some("item-text-1:text:0".to_string());
    terminal.message_id = Some("assistant-race".to_string());
    terminal.terminal_id = Some("terminal-race".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
            correlation: terminal,
            assistant_text: "streamed answer".to_string(),
            sequence: Some(1),
            iterations: 1,
            token_usage: None,
        },
    });

    let assistant = app
            .timeline_iter()
            .filter(|(_, entry)| {
                matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant")
            })
            .collect::<Vec<_>>();
    assert_eq!(
        assistant.len(),
        1,
        "history hydration and terminal delivery must reconcile the live bubble"
    );
    assert!(matches!(
        assistant[0].1,
        TimelineEntry::Message {
            content,
            identity: Some(MessageIdentity {
                message_id: Some(message_id),
                source: MessageSource::DurableHistory,
                ..
            }),
            ..
        } if content == "streamed answer" && message_id == "assistant-race"
    ));
}

#[test]
fn late_live_snapshot_cannot_recreate_a_committed_assistant_bubble() {
    let mut app = App::new("test", "sess");
    let mut live = correlation("execution-late", "turn-late");
    live.part_id = Some("item-text-1:text:0".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: live,
            text: "one answer".to_string(),
            start_bytes: 0,
            end_bytes: 10,
            stream_revision: 10,
        },
    });

    let mut terminal = correlation("execution-late", "turn-late");
    terminal.part_id = Some("item-text-1:text:0".to_string());
    terminal.message_id = Some("assistant-late".to_string());
    terminal.terminal_id = Some("terminal-late".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
            correlation: terminal,
            assistant_text: "one answer".to_string(),
            sequence: Some(1),
            iterations: 1,
            token_usage: None,
        },
    });

    app.reconcile_live_output_parts(
        "execution-late",
        Some("turn-late"),
        &[harness_contract::projection::ExecutionLiveOutputPart {
            model_step_id: "step-late".to_string(),
            item_id: "item-late".to_string(),
            part_id: "item-text-1:text:0".to_string(),
            causal_sequence: 1,
            completed: true,
            preview: Some("one answer".to_string()),
            preview_start_bytes: 0,
            bytes: 10,
        }],
        10,
    );

    let assistant_count = app
            .timeline_iter()
            .filter(|(_, entry)| {
                matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant")
            })
            .count();
    assert_eq!(
        assistant_count, 1,
        "a delayed canonical preview must not duplicate its committed terminal"
    );
}

#[test]
fn identical_typed_text_deltas_are_appended_without_snapshot_guessing() {
    let mut app = App::new("test", "sess");
    for (start_bytes, text) in [(0, "ha"), (2, "ha")] {
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-repeat", "turn-repeat"),
                text: text.to_string(),
                start_bytes,
                end_bytes: start_bytes + text.len(),
                stream_revision: (start_bytes + text.len()) as u64,
            },
        });
    }

    assert!(matches!(
        app.timeline_get(0),
        Some(TimelineEntry::Message { content, .. }) if content == "haha"
    ));
}

#[test]
fn text_delta_revision_is_monotonic_within_one_causal_part() {
    let mut app = App::new("test", "sess");
    let apply = |app: &mut App, text: &str, start_bytes, end_bytes, stream_revision| {
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-revision", "turn-revision"),
                text: text.to_string(),
                start_bytes,
                end_bytes,
                stream_revision,
            },
        });
    };

    apply(&mut app, "first", 0, 5, 10);
    apply(&mut app, "stale-conflict", 0, 14, 9);
    apply(&mut app, "replayed-conflict", 0, 17, 10);
    apply(&mut app, " tail", 5, 10, 11);

    assert!(matches!(
        app.timeline_get(0),
        Some(TimelineEntry::Message { content, .. }) if content == "first tail"
    ));
    assert_eq!(
        app.execution.telemetry.text_delta_dedupe_count, 2,
        "older and equal revisions must not mutate visible text"
    );
}

#[test]
fn durable_terminal_rejects_late_non_terminal_phase_for_the_same_execution() {
    let mut app = App::new("test", "sess");
    app.execution.current_execution_id = Some("execution-terminal".to_string());
    app.execution.current_turn_id = Some("turn-terminal".to_string());
    app.execution.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::Error);
    app.execution.current_execution_status_detail = Some("partial result".to_string());
    let mut terminal = correlation("execution-terminal", "turn-terminal");
    terminal.message_id = Some("assistant-terminal".to_string());
    terminal.terminal_id = Some("terminal-1".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
            correlation: terminal.clone(),
            assistant_text: "done".to_string(),
            sequence: Some(1),
            iterations: 1,
            token_usage: None,
        },
    });
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::ExecutionPhase {
            correlation: terminal,
            status: harness_contract::projection::ExecutionLiveStatus::Finalizing,
            detail: Some("late projection".to_string()),
        },
    });

    assert_eq!(
        app.execution.current_execution_status,
        Some(harness_contract::projection::ExecutionLiveStatus::Error)
    );
    assert_eq!(
        app.execution.current_execution_status_detail.as_deref(),
        Some("partial result")
    );
    assert!(app.execution_is_terminalized("execution-terminal"));
    assert_eq!(
        app.execution.telemetry.orphan_event_count, 0,
        "a known late phase is discarded as an ordering fact, not misreported as a causal orphan"
    );
}

#[test]
fn queued_followup_does_not_replace_the_running_execution_status() {
    let mut app = App::new("test", "sess");
    app.execution
        .turn_interaction
        .ingress_accepted("execution-running");
    app.execution.current_execution_id = Some("execution-running".to_string());
    app.execution.current_turn_id = Some("turn-running".to_string());
    app.execution.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::CallingModel);

    let mut queued = correlation("execution-queued", "turn-queued");
    queued.message_id = Some("message-queued".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
            correlation: queued,
            content: "follow up".to_string(),
            sequence: 4,
            created_at_ms: 5,
        },
    });

    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-running")
    );
    assert_eq!(
        app.execution.current_execution_status,
        Some(harness_contract::projection::ExecutionLiveStatus::CallingModel)
    );
    assert!(app.timeline_iter().any(|(_, entry)| matches!(
        entry,
        TimelineEntry::Message {
            identity: Some(MessageIdentity {
                message_id: Some(message_id),
                ..
            }),
            ..
        } if message_id == "message-queued"
    )));
}

#[test]
fn started_followup_replaces_stale_finalizing_correlation_for_observers() {
    let mut app = App::new("test", "sess");
    app.execution
        .turn_interaction
        .ingress_accepted("execution-old");
    app.execution.current_execution_id = Some("execution-old".to_string());
    app.execution.current_turn_id = Some("turn-old".to_string());
    app.execution.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::Finalizing);

    let mut admitted = correlation("execution-new", "turn-new");
    admitted.message_id = Some("message-new".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
            correlation: admitted,
            content: "new observer turn".to_string(),
            sequence: 8,
            created_at_ms: 9,
        },
    });
    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-old"),
        "durable admission alone may still be queued and cannot steal an active turn"
    );

    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::ExecutionPhase {
            correlation: correlation("execution-new", "turn-new"),
            status: harness_contract::projection::ExecutionLiveStatus::PreparingContext,
            detail: Some("started by Runtime".to_string()),
        },
    });
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation("execution-new", "turn-new"),
            text: "first live delta".to_string(),
            start_bytes: 0,
            end_bytes: 16,
            stream_revision: 16,
        },
    });

    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-new")
    );
    assert_eq!(app.execution.current_turn_id.as_deref(), Some("turn-new"));
    assert_eq!(
        app.execution.current_execution_status,
        Some(harness_contract::projection::ExecutionLiveStatus::PreparingContext)
    );
    assert_eq!(app.execution.telemetry.orphan_event_count, 0);
    assert!(app.timeline_iter().any(|(_, entry)| matches!(
        entry,
        TimelineEntry::Message {
            role,
            content,
            identity: Some(MessageIdentity {
                source: MessageSource::Live,
                execution_id: Some(execution_id),
                turn_id: Some(turn_id),
                ..
            }),
            ..
        } if role == "assistant"
            && content == "first live delta"
            && execution_id == "execution-new"
            && turn_id == "turn-new"
    )));

    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::ExecutionPhase {
            correlation: correlation("execution-old", "turn-old"),
            status: harness_contract::projection::ExecutionLiveStatus::Finalizing,
            detail: Some("delayed old phase".to_string()),
        },
    });
    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-new"),
        "a superseded execution cannot reclaim the observer after the new Runtime phase"
    );
}

#[test]
fn first_live_delta_activates_a_committed_followup_when_phase_was_coalesced() {
    let mut app = App::new("test", "sess");
    app.execution
        .turn_interaction
        .ingress_accepted("execution-old");
    app.execution.current_execution_id = Some("execution-old".to_string());
    app.execution.current_turn_id = Some("turn-old".to_string());
    app.execution.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::Finalizing);

    let mut admitted = correlation("execution-new", "turn-new");
    admitted.message_id = Some("message-new".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
            correlation: admitted,
            content: "queued until Runtime starts it".to_string(),
            sequence: 8,
            created_at_ms: 9,
        },
    });
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation("execution-new", "turn-new"),
            text: "visible before terminal".to_string(),
            start_bytes: 0,
            end_bytes: 23,
            stream_revision: 23,
        },
    });

    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-new")
    );
    assert_eq!(app.execution.current_turn_id.as_deref(), Some("turn-new"));
    assert_eq!(app.execution.telemetry.orphan_event_count, 0);
    assert!(app.timeline_iter().any(|(_, entry)| matches!(
        entry,
        TimelineEntry::Message {
            role,
            content,
            identity: Some(MessageIdentity {
                source: MessageSource::Live,
                execution_id: Some(execution_id),
                ..
            }),
            ..
        } if role == "assistant"
            && content == "visible before terminal"
            && execution_id == "execution-new"
    )));

    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation("execution-old", "turn-old"),
            text: "late old output".to_string(),
            start_bytes: 0,
            end_bytes: 15,
            stream_revision: 15,
        },
    });
    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-new")
    );
    assert_eq!(
        app.execution.telemetry.orphan_event_count, 1,
        "the causal tombstone must reject delayed output from the superseded execution"
    );
}

#[test]
fn first_live_delta_activates_new_turn_after_terminal_when_admission_was_missed() {
    let mut app = App::new("test", "sess");
    app.execution.current_execution_id = Some("execution-old".to_string());
    app.execution.current_turn_id = Some("turn-old".to_string());
    app.execution.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::Complete);
    app.execution
        .terminal_correlations
        .push_back(("execution-old".to_string(), "turn-old".to_string()));
    app.execution.turn_interaction.terminal_observed();

    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation("execution-new", "turn-new"),
            text: "visible before terminal".to_string(),
            start_bytes: 0,
            end_bytes: 23,
            stream_revision: 23,
        },
    });

    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-new")
    );
    assert_eq!(app.execution.current_turn_id.as_deref(), Some("turn-new"));
    assert_eq!(app.execution.telemetry.orphan_event_count, 0);
    assert_eq!(
        app.execution.telemetry.text_delta_dedupe_count, 0,
        "the new execution's first delta must not inherit the terminal preview tombstone"
    );
    assert_eq!(
        app.execution
            .turn_interaction
            .execution
            .execution_id
            .as_deref(),
        Some("execution-new"),
        "presentation state must be rebound to the new causal execution"
    );
    assert!(app.timeline_iter().any(|(_, entry)| matches!(
        entry,
        TimelineEntry::Message {
            role,
            content,
            identity: Some(MessageIdentity {
                source: MessageSource::Live,
                execution_id: Some(execution_id),
                ..
            }),
            ..
        } if role == "assistant"
            && content == "visible before terminal"
            && execution_id == "execution-new"
    )));

    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation("execution-old", "turn-old"),
            text: "late old output".to_string(),
            start_bytes: 0,
            end_bytes: 15,
            stream_revision: 15,
        },
    });
    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-new")
    );
    assert_eq!(app.execution.telemetry.orphan_event_count, 1);
}

#[test]
fn committed_cross_surface_user_message_reconciles_optimistic_identity() {
    let mut app = App::new("test", "sess");
    app.add_message_with_id(
        "user",
        "cross-surface prompt",
        Some("client-message-1".to_string()),
    );
    let mut committed = correlation("execution-1", "turn-1");
    committed.message_id = Some("client-message-1".to_string());
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
            correlation: committed,
            content: "cross-surface prompt".to_string(),
            sequence: 7,
            created_at_ms: 9_000,
        },
    });

    assert_eq!(app.timeline_len(), 1);
    assert!(matches!(
        app.timeline_get(0),
        Some(TimelineEntry::Message {
            identity: Some(MessageIdentity {
                sequence: Some(7),
                source: MessageSource::DurableIngress,
                ..
            }),
            ..
        })
    ));
}

#[test]
fn reconnect_history_repairs_cross_surface_message_order_by_durable_sequence() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![
                crate::protocol::SessionMessageProjection {
                    id: "user-0".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 0,
                    role: "user".to_string(),
                    blocks: vec![serde_json::json!({"type": "text", "text": "zero"})],
                    created_at_ms: 1,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                },
                crate::protocol::SessionMessageProjection {
                    id: "assistant-2".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 2,
                    role: "assistant".to_string(),
                    blocks: vec![serde_json::json!({"type": "text", "text": "two"})],
                    created_at_ms: 3,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                },
            ],
            total: 2,
            offset: 0,
            from_seq: Some(0),
            next_seq: Some(3),
            limit: 500,
            has_more: false,
        },
    });
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![
                crate::protocol::SessionMessageProjection {
                    id: "user-0".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 0,
                    role: "user".to_string(),
                    blocks: vec![serde_json::json!({"type": "text", "text": "zero"})],
                    created_at_ms: 1,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                },
                crate::protocol::SessionMessageProjection {
                    id: "user-1".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 1,
                    role: "user".to_string(),
                    blocks: vec![serde_json::json!({"type": "text", "text": "one"})],
                    created_at_ms: 2,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                },
                crate::protocol::SessionMessageProjection {
                    id: "assistant-2".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 2,
                    role: "assistant".to_string(),
                    blocks: vec![serde_json::json!({"type": "text", "text": "two"})],
                    created_at_ms: 3,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                },
            ],
            total: 3,
            offset: 0,
            from_seq: Some(0),
            next_seq: Some(3),
            limit: 500,
            has_more: false,
        },
    });

    assert_eq!(
        app.timeline_iter()
            .map(|(_, entry)| entry.full_text())
            .collect::<Vec<_>>(),
        vec!["zero", "one", "two"]
    );
}

#[test]
fn correlated_turn_error_stops_activity_and_exposes_terminal_status() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::TurnStarted);
    app.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TurnError {
            correlation: correlation("execution-failed", "turn-failed"),
            error: "provider unavailable".to_string(),
        },
    });

    assert!(!app.turn_is_active());
    assert_eq!(
        app.execution.current_execution_status,
        Some(harness_contract::projection::ExecutionLiveStatus::Error)
    );
    assert_eq!(
        app.execution.current_execution_status_detail.as_deref(),
        Some("provider unavailable")
    );
    assert_eq!(
        app.execution.current_execution_id.as_deref(),
        Some("execution-failed")
    );
}

#[test]
fn causal_history_places_late_terminal_before_the_next_ingress() {
    let mut app = App::new("test", "sess");
    let projection =
        |id: &str, sequence: usize, role: &str, text: &str, turn_id: &str, ingress_id: &str| {
            crate::protocol::SessionMessageProjection {
                id: id.to_string(),
                session_id: "sess".to_string(),
                sequence,
                role: role.to_string(),
                blocks: vec![serde_json::json!({
                    "type": "text",
                    "text": text,
                    "cowd_turn_id": turn_id,
                    "cowd_turn_ingress_message_id": ingress_id,
                })],
                created_at_ms: sequence as u64 + 1,
                token_usage: None,
                tool_use_id: None,
                tool_name: None,
            }
        };
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![
                projection("user-1", 0, "user", "first", "turn-1", "user-1"),
                projection("user-2", 1, "user", "second", "turn-2", "user-2"),
                projection(
                    "assistant-1",
                    2,
                    "assistant",
                    "first answer",
                    "turn-1",
                    "user-1",
                ),
            ],
            total: 3,
            offset: 0,
            from_seq: Some(0),
            next_seq: Some(3),
            limit: 500,
            has_more: false,
        },
    });

    assert_eq!(
        app.timeline_iter()
            .map(|(_, entry)| entry.full_text())
            .collect::<Vec<_>>(),
        vec!["first", "first answer", "second"]
    );
    assert_eq!(
        app.timeline_iter()
            .filter_map(|(_, entry)| match entry {
                TimelineEntry::Message {
                    identity: Some(identity),
                    ..
                } => identity.sequence,
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![0, 2, 1],
        "logical presentation order must not rewrite the immutable physical cursor"
    );
}

#[test]
fn repeated_provider_tool_ids_pair_history_results_fifo() {
    let tool_use =
        |message_id: &str, sequence: usize, name: &str| crate::protocol::SessionMessageProjection {
            id: message_id.to_string(),
            session_id: "sess".to_string(),
            sequence,
            role: "assistant".to_string(),
            blocks: vec![serde_json::json!({
                "type": "tool_use",
                "id": "provider-reused-id",
                "name": name,
                "input": "{}"
            })],
            created_at_ms: sequence as u64,
            token_usage: None,
            tool_use_id: Some("provider-reused-id".to_string()),
            tool_name: Some(name.to_string()),
        };
    let tool_result = |message_id: &str, sequence: usize, output: &str| {
        crate::protocol::SessionMessageProjection {
            id: message_id.to_string(),
            session_id: "sess".to_string(),
            sequence,
            role: "tool".to_string(),
            blocks: vec![serde_json::json!({
                "type": "tool_result",
                "tool_use_id": "provider-reused-id",
                "tool_name": "tool",
                "output": output,
                "is_error": false
            })],
            created_at_ms: sequence as u64,
            token_usage: None,
            tool_use_id: Some("provider-reused-id".to_string()),
            tool_name: Some("tool".to_string()),
        }
    };
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![
                tool_use("use-1", 0, "first-tool"),
                tool_use("use-2", 1, "second-tool"),
                tool_result("result-1", 2, "first-output"),
                tool_result("result-2", 3, "second-output"),
            ],
            total: 4,
            offset: 0,
            from_seq: Some(0),
            next_seq: Some(4),
            limit: 500,
            has_more: false,
        },
    });
    assert_eq!(
        app.timeline_iter()
            .filter_map(|(_, entry)| match entry {
                TimelineEntry::ToolCall { name, output, .. } =>
                    Some((name.as_str(), output.as_str())),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![
            ("first-tool", "first-output"),
            ("second-tool", "second-output")
        ]
    );
}

#[test]
fn current_turn_thinking_counter_ignores_history_and_counts_one_live_stream() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::SessionHistoryPage {
        page: crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![crate::protocol::SessionMessageProjection {
                id: "historical-thinking".to_string(),
                session_id: "sess".to_string(),
                sequence: 0,
                role: "assistant".to_string(),
                blocks: vec![serde_json::json!({
                    "type": "thinking",
                    "thinking": "old reasoning"
                })],
                created_at_ms: 1,
                token_usage: None,
                tool_use_id: None,
                tool_name: None,
            }],
            total: 1,
            offset: 0,
            from_seq: Some(0),
            next_seq: Some(1),
            limit: 500,
            has_more: false,
        },
    });
    assert_eq!(app.execution.current_turn_thinking_count, 0);
    app.apply_event(CowdEvent::TurnStarted);
    app.apply_event(CowdEvent::ReasoningSummaryDelta {
        summary: "new".to_string(),
    });
    app.apply_event(CowdEvent::ReasoningSummaryDelta {
        summary: " reasoning".to_string(),
    });
    assert_eq!(app.execution.current_turn_thinking_count, 1);
}

#[test]
fn unicode_tool_progress_is_bounded_without_splitting_utf8() {
    let mut app = App::new("test", "sess");
    app.apply_event(CowdEvent::ToolStart {
        id: "tool-unicode".to_string(),
        name: "logger".to_string(),
        preview: String::new(),
    });
    app.apply_event(CowdEvent::ToolProgress {
        id: "tool-unicode".to_string(),
        name: "logger".to_string(),
        progress: "你好🙂".repeat(1200),
    });
    let output = app
        .timeline_iter()
        .find_map(|(_, entry)| match entry {
            TimelineEntry::ToolCall { output, .. } => Some(output),
            _ => None,
        })
        .expect("tool output");
    assert!(output.len() <= 4096);
    assert!(std::str::from_utf8(output.as_bytes()).is_ok());
}
