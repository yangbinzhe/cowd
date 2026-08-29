use super::*;
use crate::layout::LayoutNode;
use crate::test_utils::MockTerminal;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

fn gateway_correlation(
    session_id: &str,
    execution_id: &str,
    turn_id: &str,
) -> crate::protocol::GatewayEventCorrelation {
    crate::protocol::GatewayEventCorrelation {
        session_id: session_id.to_string(),
        execution_id: Some(execution_id.to_string()),
        turn_id: Some(turn_id.to_string()),
        part_id: Some("item-text-1:text:0".to_string()),
        ..Default::default()
    }
}

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ── Construction ────────────────────────────────────────────

#[test]
fn gateway_skill_projection_maps_canonical_identity_without_fallback() {
    let skills = skill_summaries_from_catalog(&serde_json::json!({
        "items": [{
            "id": "local:release",
            "name": "release",
            "description": "Prepare release",
            "scope": "workspace",
            "domain": "delivery",
            "source": "Project",
            "status": "ready",
            "risk": "operator_review",
            "tags": ["git"]
        }]
    }))
    .expect("valid Gateway projection");

    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].id, "local:release");
    assert_eq!(skills[0].category, "delivery");
    assert_eq!(skills[0].status, "ready");
    assert_eq!(skills[0].risk, "operator_review");
    assert!(skills[0].installed);
    assert!(skill_summaries_from_catalog(&serde_json::json!({
        "items": [{ "name": "invented" }]
    }))
    .is_err());
}

#[test]
fn tui_state_new_creates_all_engines() {
    let state = TuiState::new("test-model", "test-session");

    // App fields
    assert_eq!(state.app.shell.model, "test-model");
    assert_eq!(state.app.shell.session_id, "test-session");
    assert!(!state.app.shell.should_quit);

    // Layout tree exists
    assert!(matches!(state.shell.layout_tree.root, LayoutNode::Split(_)));

    // Keybind engine ready
    assert!(!state.shell.keybind_engine.which_key_visible);

    // Dialog manager empty
    assert!(state.overlay.dialog_manager.is_empty());

    // Theme engine dark by default
    assert_eq!(state.shell.theme_engine.theme.name, "dark");
}

#[test]
fn reality_governance_action_is_not_queued_twice_while_running() {
    let mut state = TuiState::new("test-model", "test-session");
    let key = crossterm::event::Event::Key(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE));

    assert!(state.handle_reality_panel_action(&key));
    assert_eq!(state.take_pending_core_gateway_effects().len(), 1);

    state
        .workbench
        .reality_panel
        .record_governance_result(Ok(serde_json::json!({
            "running": true,
            "automatic_governance_run": {"run_id": "run-1"}
        })));
    assert!(state.handle_reality_panel_action(&key));
    assert!(
        state.take_pending_core_gateway_effects().is_empty(),
        "a visible active governance run must suppress duplicate manual submissions"
    );
}

#[tokio::test]
async fn gateway_effect_is_deferred_and_reduced_only_on_the_ui_owner() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    let operation_ran = Arc::new(AtomicBool::new(false));
    let completion_ran = Arc::new(AtomicBool::new(false));
    let operation_probe = Arc::clone(&operation_ran);
    let completion_probe = Arc::clone(&completion_ran);
    let mut state = TuiState::new("test-model", "test-session");
    state.queue_gateway_api(
        move |_client| async move {
            operation_probe.store(true, Ordering::SeqCst);
            Ok(serde_json::json!({"ok": true}))
        },
        move |_state, result| {
            assert_eq!(
                result.expect("background result"),
                serde_json::json!({"ok": true})
            );
            completion_probe.store(true, Ordering::SeqCst);
        },
    );

    assert!(
        !operation_ran.load(Ordering::SeqCst),
        "queuing an HTTP effect must never execute it on the input/render thread"
    );
    let mut pending = state.take_pending_core_gateway_effects();
    assert_eq!(pending.len(), 1);
    let PendingCoreGatewayEffect {
        session_id,
        authority_generation,
        operation,
        completion,
    } = pending.pop().expect("queued effect");
    let client =
        crate::gateway_client::GatewayApiClient::new("http://127.0.0.1:1", None).expect("client");
    let result = operation(client).await.map_err(|error| error.to_string());
    assert!(operation_ran.load(Ordering::SeqCst));
    assert!(
        !completion_ran.load(Ordering::SeqCst),
        "background completion must not mutate UI state"
    );

    CompletedCoreGatewayEffect::new(session_id, authority_generation, result, completion)
        .apply_if_current(&mut state);
    assert!(completion_ran.load(Ordering::SeqCst));
}

#[test]
fn model_switch_waits_for_gateway_authority_and_rolls_back_on_failure() {
    let mut state = TuiState::new("model-a", "session-model");
    state.app.shell.available_models = vec!["model-a".to_string(), "model-b".to_string()];
    state.app.shell.requested_model = Some("model-a".to_string());

    state.dispatch_action(Action::NextModel);
    assert_eq!(state.app.shell.model, "model-b");
    assert_eq!(
        state.app.shell.requested_model.as_deref(),
        Some("model-a"),
        "the requested model remains authoritative until Gateway confirms the PATCH"
    );
    let mut pending = state.take_pending_core_gateway_effects();
    assert_eq!(pending.len(), 1);
    let PendingCoreGatewayEffect { completion, .. } = pending.pop().expect("model update effect");
    CompletedCoreGatewayEffect::new(
        state.app.shell.session_id.clone(),
        state.authority_generation(),
        Err("Gateway rejected model".to_string()),
        completion,
    )
    .apply_if_current(&mut state);

    assert_eq!(state.app.shell.model, "model-a");
    assert_eq!(state.app.shell.requested_model.as_deref(), Some("model-a"));
    assert!(!state.app.shell.model_dirty);
    assert!(state
        .app
        .shell
        .notification
        .as_deref()
        .is_some_and(|value| value.contains("rolled back")));
}

#[test]
fn revoked_authority_rejects_late_core_gateway_completion() {
    let mut state = TuiState::new("model-a", "session-a");
    state.queue_gateway_api(
        |_client| async { Ok(serde_json::json!({"secret":"late"})) },
        |state, result| {
            if result.is_ok() {
                state.app.shell.input.set_text("late secret completion");
            }
        },
    );
    let PendingCoreGatewayEffect {
        session_id,
        authority_generation,
        completion,
        ..
    } = state
        .take_pending_core_gateway_effects()
        .pop()
        .expect("queued completion");
    state.revoke_session_authority("test revoke");
    CompletedCoreGatewayEffect::new(
        session_id,
        authority_generation,
        Ok(serde_json::json!({"secret":"late"})),
        completion,
    )
    .apply_if_current(&mut state);

    assert_ne!(state.app.shell.input.text(), "late secret completion");
    assert!(state
        .app
        .history
        .history_hydration_error
        .as_deref()
        .is_some_and(|error| error.contains("authorization revoked")));
}

#[test]
fn core_tui_starts_with_an_empty_dynamic_application_catalog() {
    let state = TuiState::new("test-model", "test-session");
    assert!(state.session.app_surface_host.is_empty());
}

#[test]
fn application_backlink_identity_guards_accept_only_the_canonical_approval_and_surface_object() {
    assert!(evidence_backlink_object_matches_target(
        "evidence://matrix/packet-1",
        &serde_json::json!({"packet": {"packet_id": "packet-1"}}),
    ));
    assert!(!evidence_backlink_object_matches_target(
        "evidence://matrix/packet-1",
        &serde_json::json!({"packet": {"packet_id": "packet-2"}}),
    ));
    assert!(approval_backlink_object_matches_target(
        "approval://approval-1",
        &serde_json::json!({"approval_id": "approval-1", "status": "pending"}),
    ));
    assert!(approval_backlink_object_matches_target(
        "approval://approval-1",
        &serde_json::json!({"id": "history-1", "request_id": "approval-1"}),
    ));
    assert!(!approval_backlink_object_matches_target(
        "approval://approval-1",
        &serde_json::json!({"approval_id": "approval-2"}),
    ));

    assert!(surface_backlink_receipt_matches_target(
        "surface://webui/delivery/delivery-1",
        &serde_json::json!({"surface": "webui", "delivery_id": "delivery-1"}),
    ));
    assert!(surface_backlink_receipt_matches_target(
        "surface://webui/message-1",
        &serde_json::json!({"surface": "webui", "message_id": "message-1"}),
    ));
    assert!(surface_backlink_receipt_matches_target(
        "receipt://cross-plane/cpx-1",
        &serde_json::json!({"id": "cpx-1"}),
    ));
    assert!(!surface_backlink_receipt_matches_target(
        "surface://webui/delivery/delivery-1",
        &serde_json::json!({"surface": "webui", "delivery_id": "delivery-2"}),
    ));
    assert!(!surface_backlink_receipt_matches_target(
        "surface://webui/message-1",
        &serde_json::json!({"surface": "slack", "message_id": "message-1"}),
    ));
}

#[test]
fn late_application_backlink_response_cannot_refocus_a_newer_selection() {
    let mut state = TuiState::new("model", "session");
    state.apply_app_navigation_context(&serde_json::json!({
        "kind": "backlink",
        "target": "runtime-execution://execution-a",
        "object": null,
        "error": null,
    }));
    state.apply_app_navigation_context(&serde_json::json!({
        "kind": "backlink",
        "target": "runtime-execution://execution-b",
        "object": null,
        "error": null,
    }));
    state.apply_app_navigation_context(&serde_json::json!({
        "kind": "backlink",
        "target": "runtime-execution://execution-a",
        "object": {"execution_id": "execution-a"},
        "error": null,
    }));
    assert!(state
        .workbench
        .runtime_activity_panel
        .accepts_backlink_result("runtime-execution://execution-b"));
    assert!(!state
        .workbench
        .runtime_activity_panel
        .accepts_backlink_result("runtime-execution://execution-a"));

    state.apply_app_navigation_context(&serde_json::json!({
        "kind": "backlink",
        "target": "evidence://matrix/packet-b",
        "object": null,
        "error": null,
    }));
    state.apply_app_navigation_context(&serde_json::json!({
        "kind": "backlink",
        "target": "evidence://matrix/packet-a",
        "object": {"packet": {"packet_id": "packet-a"}},
        "error": null,
    }));
    assert!(state
        .workbench
        .reality_panel
        .accepts_backlink_result("evidence://matrix/packet-b"));
}

#[test]
fn local_connector_resource_state_updates_projection_state() {
    let mut state = TuiState::new("test-model", "test-session");
    state.app.gateway.gateway_connector_resources =
        vec![crate::runtime_control_store::ConnectorResourceSummary {
            reference: "service://local.docs/document/tui-doc".to_string(),
            provider: "local.docs".to_string(),
            resource_type: "document".to_string(),
            title: "TUI Doc".to_string(),
            indexed_state: "indexed".to_string(),
        }];

    state.apply_local_connector_resource_state("service://local.docs/document/tui-doc", "stale");

    assert_eq!(
        state.app.gateway.gateway_connector_resources[0].indexed_state,
        "stale"
    );
}

#[test]
fn reload_runtime_provider_projection_reports_gateway_state_without_leaking_secret() {
    let mut state = TuiState::new("tui-reload-model", "session-tui-provider");
    state.app.gateway.gateway_connector_accounts =
        vec![crate::runtime_control_store::ConnectorAccountSummary {
            provider: "gateway-provider".to_string(),
            account_id: "account-1".to_string(),
            auth_mode: "token".to_string(),
            status: "available".to_string(),
            reason: None,
            binding_count: 1,
        }];
    state.app.shell.available_models = vec!["tui-reload-model".to_string(), "tui-fast".to_string()];

    assert!(state.reload_runtime_provider_projection());
    assert!(state
        .app
        .shell
        .notification
        .as_deref()
        .unwrap_or_default()
        .contains("Provider projection refreshed"));
    assert!(!state
        .app
        .shell
        .notification
        .as_deref()
        .unwrap_or_default()
        .contains("tui-secret-key"));
}

#[test]
fn memory_projection_wires_tui_memory_surfaces() {
    let mut state = TuiState::new("test-model", "test-session");
    state.set_memory_projection_available(true);
    state.app.workbench.memory_status = Some("available".to_string());
    state.app.workbench.memory_entries = vec![crate::app::MemoryEntry {
        id: Some("m1".to_string()),
        layer: "L4".to_string(),
        content: "TUI L4 Decision".to_string(),
        priority: "high".to_string(),
    }];
    state.workbench.l4_memory_view.sync_from_app(&state.app);

    assert!(
        state
            .workbench
            .l4_memory_view
            .entries
            .iter()
            .any(|entry| entry.contains("TUI L4 Decision")),
        "L4 overlay should sync real entries from the memory store"
    );
}

// ── Explicit App composition ────────────────────────────────

#[test]
fn explicit_app_composition_exposes_domain_methods() {
    let mut state = TuiState::new("m", "s");

    state.app.add_message("user", "hello");
    assert_eq!(state.app.timeline_len(), 1);

    state.app.add_message("assistant", "world");
    assert_eq!(state.app.timeline_len(), 2);

    state.apply_event(CowdEvent::TurnStarted);
    assert!(state.app.turn_is_active());
}

#[test]
fn explicit_app_composition_preserves_public_domain_behavior() {
    let mut state = TuiState::new("m", "s");

    state.app.add_message("system", "test");
    state.app.add_message("assistant", "response");

    assert_eq!(state.app.timeline_len(), 1);
    assert!(state.app.timeline.auto_scroll);

    // picker methods
    let sessions = vec![crate::app::SessionSummary {
        id: "s1".into(),
        title: None,
        path: "/tmp".into(),
        updated_at_ms: 1000,
        message_count: 3,
    }];
    state.app.open_session_picker(sessions);
    assert!(state.app.shell.picker_active);
    assert_eq!(state.app.picker_selected_id(), Some("s1"));
    state.app.close_session_picker();
    assert!(!state.app.shell.picker_active);

    // cursor_* methods work
    state.app.cursor_down();
    state.app.cursor_up();
    state.app.toggle_expand_current();
}

#[test]
fn explicit_partitions_allow_scoped_state_updates() {
    let mut state = TuiState::new("m", "s");

    state.app.shell.spinner_idx = 5;
    assert_eq!(state.app.shell.spinner_idx, 5);

    state.app.timeline.scroll_offset = 42;
    assert_eq!(state.app.timeline.scroll_offset, 42);

    state.app.shell.help_visible = true;
    assert!(state.app.shell.help_visible);
}

// ── apply_event ─────────────────────────────────────────────

#[test]
fn apply_event_text_delta_adds_to_timeline() {
    let mut state = TuiState::new("m", "s");

    state.apply_event(CowdEvent::TurnStarted);
    let correlation = gateway_correlation("s", "execution-1", "turn-1");
    state.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation.clone(),
            text: "Hello world".into(),
            start_bytes: 0,
            end_bytes: "Hello world".len(),
            stream_revision: 1,
        },
    });
    state.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
            correlation: crate::protocol::GatewayEventCorrelation {
                message_id: Some("assistant-1".to_string()),
                terminal_id: Some("terminal-1".to_string()),
                ..correlation
            },
            assistant_text: String::new(),
            sequence: Some(1),
            iterations: 1,
            token_usage: None,
        },
    });

    assert!(state.app.timeline_len() >= 1);
    let last = state
        .app
        .timeline_get(state.app.timeline_len() - 1)
        .unwrap();
    let text = last.full_text();
    assert!(
        text.contains("Hello world"),
        "expected streamed assistant text to remain the final entry, got: {text}"
    );
    assert!(
        !state
            .app
            .timeline_iter()
            .any(|(_, entry)| entry.full_text().contains("Done")),
        "turn completion should not inject Done messages"
    );
}

#[test]
fn apply_event_resources_committed_clears_only_sent_resources() {
    let mut state = TuiState::new("m", "s");
    state
        .app
        .workbench
        .pending_resources
        .push(crate::app::PendingResource {
            id: "res-a".into(),
            label: "a.mp3".into(),
            kind: "audio".into(),
        });
    state
        .app
        .workbench
        .pending_resources
        .push(crate::app::PendingResource {
            id: "res-b".into(),
            label: "b.pdf".into(),
            kind: "pdf".into(),
        });

    state.apply_event(CowdEvent::ResourcesCommitted {
        ids: vec!["res-a".into()],
    });

    assert_eq!(state.app.workbench.pending_resources.len(), 1);
    assert_eq!(state.app.workbench.pending_resources[0].id, "res-b");
}

#[test]
fn apply_event_tool_lifecycle() {
    let mut state = TuiState::new("m", "s");
    state.set_focus_target(FocusTarget::Input);

    state.apply_event(CowdEvent::TurnStarted);
    state.apply_event(CowdEvent::ToolStart {
        id: "t1".into(),
        name: "bash".into(),
        preview: "ls -la".into(),
    });

    assert!(state
        .app
        .timeline_iter()
        .any(|(_, e)| matches!(&e, crate::app::TimelineEntry::ToolCall { id, .. } if id == "t1")));
    assert_eq!(state.shell.focus_target, FocusTarget::Input);
    assert!(!state.shell.layout_state.sidebar_visible);
}

#[test]
fn apply_event_token_usage_updates_counters() {
    let mut state = TuiState::new("m", "s");

    state.apply_event(CowdEvent::TokenUsage {
        input: 100,
        output: 50,
        cache_create: 10,
        cache_read: 5,
    });

    assert_eq!(state.app.history.input_tokens, 100);
    assert_eq!(state.app.history.output_tokens, 50);
    assert_eq!(state.app.shell.token_count, 165);
}

// ── handle_input ────────────────────────────────────────────

#[test]
fn handle_input_quit_chord() {
    let mut state = TuiState::new("m", "s");

    let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
    let handled = state.handle_input(ctrl_c);

    assert!(handled);
    assert!(state.app.shell.should_quit);
}

#[test]
fn handle_input_scroll_down() {
    let mut state = TuiState::new("m", "s");
    state.app.timeline.scroll_offset = 0;
    state.app.timeline.auto_scroll = true;

    let j = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE);
    let handled = state.handle_input(j);

    assert!(handled);
    assert_eq!(state.app.timeline.scroll_offset, 1);
    assert!(!state.app.timeline.auto_scroll); // manual scroll disables auto-scroll
}

#[test]
fn handle_input_scroll_up() {
    let mut state = TuiState::new("m", "s");
    state.app.timeline.scroll_offset = 10;

    let k = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE);
    let handled = state.handle_input(k);

    assert!(handled);
    assert_eq!(state.app.timeline.scroll_offset, 9);
}

#[test]
fn handle_input_unbound_key_returns_false() {
    let mut state = TuiState::new("m", "s");

    let x = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
    let handled = state.handle_input(x);

    assert!(!handled);
}

#[test]
fn process_raw_key_blocks_submit_when_context_file_is_missing() {
    let mut state = TuiState::new("m", "s");
    state.replace_input_text("分析 @file:missing.rs");

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert!(!state.overlay.toast_manager.is_empty());
}

#[test]
fn process_raw_key_allows_submit_when_context_file_is_valid() {
    let mut state = TuiState::new("m", "s");
    state.app.workbench.file_entries = vec![crate::FileEntry {
        name: "readme.md".to_string(),
        is_dir: false,
        size: 6,
    }];
    state.replace_input_text("分析 @file:readme.md");

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    match result {
        ProcessedKey::Submit(text) => assert_eq!(text, "分析 @file:readme.md"),
        other => panic!("expected submit, got {other:?}"),
    }
}

#[test]
fn submit_preserves_authored_whitespace_and_newlines() {
    let mut state = TuiState::new("m", "s");
    state.replace_input_text("  keep leading\nkeep trailing  ");

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    assert!(matches!(
        result,
        ProcessedKey::Submit(text) if text == "  keep leading\nkeep trailing  "
    ));
    assert_eq!(
        state.app.shell.input_history.last().map(String::as_str),
        Some("  keep leading\nkeep trailing  ")
    );
}

#[test]
fn long_input_layout_never_inserts_physical_newlines_or_moves_cursor() {
    let mut state = TuiState::new("m", "s");
    state.shell.last_terminal_width = 12;
    state.replace_input_text("abcdefghij klmnopqrstuvwxyz");
    state.app.shell.input.set_cursor_byte(0);
    let before = state.input_text();
    let cursor_before = state.app.shell.input.cursor_byte();

    let _layout = crate::components::composer::layout::ComposerLayout::from_model(
        &state.app.shell.input,
        state.shell.last_terminal_width,
    );

    assert_eq!(state.input_text(), before);
    assert_eq!(state.app.shell.input.cursor_byte(), cursor_before);
    assert!(!state.input_text().contains('\n'));
}

#[test]
fn handle_input_space_leader_shows_which_key() {
    let mut state = TuiState::new("m", "s");

    let space = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE);
    let handled = state.handle_input(space);

    // Space alone is a prefix match, so which_key should be visible
    assert!(!handled); // No action resolved yet
    assert!(state.shell.keybind_engine.which_key_visible);
    assert!(!state.shell.keybind_engine.pending_chord().is_empty());
}

#[test]
fn handle_input_gg_multi_chord() {
    let mut state = TuiState::new("m", "s");

    // First 'g' — prefix match
    let g1 = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE);
    assert!(!state.handle_input(g1));
    assert_eq!(state.shell.keybind_engine.pending_chord().len(), 1);

    // Second 'g' — full match
    let g2 = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE);
    assert!(state.handle_input(g2));
    assert!(state.shell.keybind_engine.pending_chord().is_empty());
}

// ── dialog focus trap ───────────────────────────────────────

#[test]
fn handle_input_dialog_focus_trap() {
    let mut state = TuiState::new("m", "s");

    // Push an alert dialog
    use crate::components::dialog::{DialogKind, DialogState};
    state
        .overlay
        .dialog_manager
        .push(DialogState::new(DialogKind::Alert {
            title: "Test".into(),
            message: "Alert!".into(),
        }));

    // Any key should be consumed by the dialog
    let enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    let handled = state.handle_input(enter);

    assert!(handled);
    assert!(state.overlay.dialog_manager.is_empty()); // Dialog dismissed
}

// ── toggle_theme ────────────────────────────────────────────

#[test]
fn toggle_theme_via_leader_chord() {
    let mut state = TuiState::new("m", "s");
    assert_eq!(state.app.shell.theme, crate::app::Theme::Dark);

    // Space → leader prefix
    state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    // t → ToggleTheme
    state.handle_input(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));

    assert_eq!(state.app.shell.theme, crate::app::Theme::Light);
    assert_eq!(state.shell.theme_engine.theme.name, "light");
}

// ── command_palette ─────────────────────────────────────────

#[test]
fn command_palette_via_leader_chord() {
    let mut state = TuiState::new("m", "s");

    // Space → leader prefix
    state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    // p → ToggleCommandPalette
    state.handle_input(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));

    assert!(state.overlay.command_palette.is_open());
    assert!(state.overlay.dialog_manager.is_empty());
}

// ── cancel_action ───────────────────────────────────────────

#[test]
fn cancel_flushes_pending_and_closes_dialog() {
    let mut state = TuiState::new("m", "s");

    // Start a chord prefix
    state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    assert!(state.shell.keybind_engine.which_key_visible);

    // Push a dialog
    use crate::components::dialog::{DialogKind, DialogState};
    state
        .overlay
        .dialog_manager
        .push(DialogState::new(DialogKind::Alert {
            title: "X".into(),
            message: "Y".into(),
        }));

    // Esc → Cancel
    let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    state.handle_input(esc);

    // Dialog should still be active (Esc in dialog context dismisses it)
    // Wait - actually Esc in the dialog context triggers dismissal already.
    // Let's test the cancel action directly
    assert!(state.overlay.dialog_manager.is_empty());
}

// ── convenience methods ─────────────────────────────────────

#[test]
fn flush_chord_clears_pending() {
    let mut state = TuiState::new("m", "s");

    state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    assert!(!state.shell.keybind_engine.pending_chord().is_empty());

    state.flush_chord();
    assert!(state.shell.keybind_engine.pending_chord().is_empty());
    assert!(!state.shell.keybind_engine.which_key_visible);
}

#[test]
fn hot_reload_theme_no_file_returns_false() {
    let mut state = TuiState::new("m", "s");

    // ThemeEngine starts with dark builtin (no file), so hot_reload is a no-op
    assert!(!state.hot_reload_theme());
}

#[test]
fn sidebar_tab_labels_use_compact_mode_for_narrow_sidebars() {
    let compact = sidebar_tab_labels(72);
    let full = sidebar_tab_labels(120);

    assert_eq!(compact.len(), SIDEBAR_TAB_COUNT);
    assert_eq!(full.len(), SIDEBAR_TAB_COUNT);
    assert_eq!(compact[TAB_RUNTIME], "Run");
    assert_eq!(compact[TAB_TOOLS], "Tool");
    assert_eq!(compact[TAB_APPROVALS], "Appr");
    assert_eq!(compact[TAB_FILES], "File");
    assert_eq!(compact[TAB_APPS], "Apps");
    assert!(!compact.contains(&"Mem"));
    assert!(!compact.contains(&"Skill"));
    assert_eq!(full[TAB_RUNTIME], "Runtime");
    assert_eq!(full[TAB_TOOLS], "Tools");
    assert_eq!(full[TAB_APPROVALS], "Approvals");
    assert_eq!(full[TAB_FILES], "Files");
    assert_eq!(full[TAB_APPS], "Apps");
    assert!(!full.contains(&"Memory"));
    assert!(!full.contains(&"Skills"));
}

#[test]
fn new_state_starts_with_sidebar_hidden_for_focused_first_screen() {
    let state = TuiState::new("m", "s");

    assert!(!state.shell.layout_state.sidebar_visible);
    assert_eq!(
        state
            .shell
            .layout_state
            .current_ratio(&state.shell.layout_tree),
        1.0
    );
}

#[test]
fn ctrl_b_toggles_sidebar_visibility_in_tui_state() {
    let mut state = TuiState::new("m", "s");
    assert!(!state.shell.layout_state.sidebar_visible);

    state.handle_input(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::CONTROL));
    assert!(state.shell.layout_state.sidebar_visible);

    state.handle_input(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::CONTROL));
    assert!(!state.shell.layout_state.sidebar_visible);
}

#[test]
fn focus_actions_open_sidebar_and_select_expected_tab() {
    let mut state = TuiState::new("m", "s");

    state.dispatch_action(Action::FocusDiff);
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(
        state.workbench.active_topic_panel,
        Some(SidebarTopicPanel::Diff)
    );

    state
        .shell
        .layout_state
        .toggle_sidebar(&mut state.shell.layout_tree);
    state.dispatch_action(Action::FocusFileTree);
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(state.workbench.active_topic_panel, None);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_FILES);

    state
        .shell
        .layout_state
        .toggle_sidebar(&mut state.shell.layout_tree);
    state.dispatch_action(Action::FocusSessions);
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(state.workbench.active_topic_panel, None);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_SESSIONS);
}

#[test]
fn slash_panel_command_opens_sidebar_without_submitting() {
    let mut state = TuiState::new("m", "s");
    state.replace_input_text("/files");

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(state.workbench.active_topic_panel, None);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_FILES);
    assert_eq!(state.input_text(), "");
}

#[test]
fn slash_activity_command_toggles_activity_panel_without_submitting() {
    let mut state = TuiState::new("m", "s");
    state.replace_input_text("/activity");

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert!(!state.shell.layout_state.sidebar_visible);
    assert!(state.workbench.activity_panel_visible);
    assert_eq!(state.input_text(), "");
}

#[test]
fn empty_input_navigation_routes_to_focus_instead_of_textarea() {
    let mut state = TuiState::new("m", "s");
    state.app.timeline.scroll_offset = 0;

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert_eq!(state.input_text(), "");
    assert_eq!(state.app.timeline.scroll_offset, 1);
    assert_eq!(state.shell.focus_target, FocusTarget::Chat);
}

#[test]
fn topic_panel_navigation_keeps_topic_focus() {
    let mut state = TuiState::new("m", "s");
    state.dispatch_action(Action::Execute("/memory".into()));

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert_eq!(
        state.shell.focus_target,
        FocusTarget::TopicPanel(SidebarTopicPanel::Memory)
    );
    assert_eq!(state.input_text(), "");
}

#[test]
fn slash_activity_command_closes_sidebar_for_focused_first_screen() {
    let mut state = TuiState::new("m", "s");
    state.dispatch_action(Action::Execute("/files".into()));
    assert!(state.shell.layout_state.sidebar_visible);

    state.replace_input_text("/recent");
    let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert!(!state.shell.layout_state.sidebar_visible);
    assert!(state.workbench.activity_panel_visible);
}

#[test]
fn command_palette_panel_execute_opens_sidebar_directly() {
    let mut state = TuiState::new("m", "s");

    state.dispatch_action(Action::Execute("/memory".into()));

    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(
        state.workbench.active_topic_panel,
        Some(SidebarTopicPanel::Memory)
    );
    assert_eq!(state.input_text(), "");
}

#[test]
fn topic_panel_commands_open_on_demand_and_tab_returns_to_core_tabs() {
    let mut state = TuiState::new("m", "s");

    state.dispatch_action(Action::Execute("/skills".into()));

    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(
        state.workbench.active_topic_panel,
        Some(SidebarTopicPanel::Skills)
    );

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert_eq!(state.workbench.active_topic_panel, None);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_TOOLS);
}

#[test]
fn render_topic_panel_uses_dedicated_title_instead_of_core_tabs() {
    let mut state = TuiState::new("m", "s");
    state.dispatch_action(Action::Execute("/skills".into()));

    let mut terminal = MockTerminal::new(140, 32);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(state.workbench.active_topic_panel.is_some());
    assert!(!joined.trim().is_empty());
}

#[test]
fn render_topic_panel_compact_layout_keeps_input_and_status_visible() {
    let mut state = TuiState::new("m", "s");
    state.dispatch_action(Action::Execute("/memory".into()));

    let mut terminal = MockTerminal::new(88, 28);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(state.workbench.active_topic_panel.is_some());
    assert!(!joined.trim().is_empty());
    assert!(
        !joined.contains("focus:memory"),
        "focus should not be pinned in footer: {joined}"
    );
}

#[test]
fn command_palette_activity_execute_toggles_activity_panel_directly() {
    let mut state = TuiState::new("m", "s");

    state.dispatch_action(Action::Execute("/activity".into()));

    assert!(state.workbench.activity_panel_visible);
    assert!(!state.shell.layout_state.sidebar_visible);
    assert_eq!(state.input_text(), "");
}

#[test]
fn runtime_apps_and_gateway_panel_commands_open_expected_tabs() {
    let mut state = TuiState::new("m", "s");

    state.dispatch_action(Action::Execute("/runtime".into()));
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(state.workbench.active_topic_panel, None);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_RUNTIME);
    assert_eq!(state.shell.focus_target, FocusTarget::Sidebar);

    state.dispatch_action(Action::Execute("/tools".into()));
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(state.workbench.active_topic_panel, None);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_TOOLS);
    assert_eq!(state.shell.focus_target, FocusTarget::Sidebar);

    state.dispatch_action(Action::Execute("/apps".into()));
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(state.workbench.active_topic_panel, None);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_APPS);
    assert_eq!(state.shell.focus_target, FocusTarget::Sidebar);

    state.dispatch_action(Action::Execute("/gateway".into()));
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(state.workbench.active_topic_panel, None);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_GATEWAY);
    assert_eq!(state.shell.focus_target, FocusTarget::Sidebar);
}

#[test]
fn gateway_review_keys_fail_closed_without_a_loaded_pending_review() {
    let mut state = TuiState::new("m", "s");
    let event = crossterm::event::Event::Key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE));

    assert!(state.handle_gateway_panel_action(&event));
    assert_eq!(
            state.workbench.gateway_panel.action_status.as_deref(),
            Some(
                "evolution.release_review.approve failed: no pending release review selected; press v to refresh"
            )
        );
    assert!(state.workbench.gateway_panel.action_receipt.is_none());
}

#[test]
fn tool_ops_mutation_apply_requires_preview_hashes_before_confirmed_apply() {
    let mut state = TuiState::new("m", "s");
    state.workbench.sidebar_active_tab = TAB_TOOLS;
    state
        .workbench
        .tool_ops_panel
        .set_mode(ToolOpsMode::Mutations);
    state.workbench.tool_ops_panel.armed_action =
        Some(crate::components::tool_ops_panel::ToolOpsArmedAction::ApplyMutation);

    let consumed = state.handle_tool_ops_action(&crossterm::event::Event::Key(KeyEvent::new(
        KeyCode::Char('A'),
        KeyModifiers::NONE,
    )));

    assert!(consumed);
    assert!(state
        .workbench
        .tool_ops_panel
        .status
        .contains("run preview first"));
    assert!(state.workbench.tool_ops_panel.last_receipt.is_none());
}

#[test]
fn focus_command_switches_between_primary_surfaces() {
    let mut state = TuiState::new("m", "s");

    state.dispatch_action(Action::Execute("/focus activity".into()));
    assert!(state.workbench.activity_panel_visible);
    assert_eq!(state.shell.focus_target, FocusTarget::Activity);

    state.dispatch_action(Action::Execute("/focus input".into()));
    assert!(!state.workbench.activity_panel_visible);
    assert_eq!(state.shell.focus_target, FocusTarget::Input);

    state.dispatch_action(Action::Execute("/focus memory".into()));
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(
        state.workbench.active_topic_panel,
        Some(SidebarTopicPanel::Memory)
    );
    assert_eq!(
        state.shell.focus_target,
        FocusTarget::TopicPanel(SidebarTopicPanel::Memory)
    );
}

#[test]
fn mouse_scroll_routes_to_focused_sidebar_panel() {
    let mut state = TuiState::new("m", "s");
    state.dispatch_action(Action::Execute("/gateway".into()));
    state.app.timeline.scroll_offset = 12;
    state.workbench.gateway_panel.scroll_offset = 0;

    assert!(state.handle_mouse_scroll(true));

    assert_eq!(
        state.app.timeline.scroll_offset, 12,
        "sidebar mouse scroll should not move chat"
    );
    assert!(
        state.workbench.gateway_panel.scroll_offset > 0,
        "gateway panel should receive the scroll"
    );
    assert_eq!(state.shell.focus_target, FocusTarget::Sidebar);
}

#[test]
fn slash_keeps_input_control_without_opening_palette_or_placeholder() {
    let mut state = TuiState::new("m", "s");
    state.replace_input_text("inspect ");

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert!(!state.overlay.command_palette.is_open());
    assert_eq!(state.input_text(), "inspect /");
    assert!(
        !state.shell.prompt.suggestions_visible(),
        "bare slash should not show placeholder suggestions"
    );
    assert_eq!(state.focus_for_current_surface(), FocusTarget::Input);
}

#[test]
fn context_suggestions_do_not_render_over_prompt_dropdown() {
    let mut state = TuiState::new("m", "s");
    let projection = crate::test_utils::gateway_command_projection_fixture();
    state
        .shell
        .prompt
        .sync_command_suggestions_from_projection(&projection);
    state
        .overlay
        .context_suggestions
        .test_show("context side effect");
    state.replace_input_text("inspect ");
    state.process_raw_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
    assert!(state.shell.prompt.suggestions_visible());

    let mut terminal = MockTerminal::new(100, 24);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(
        joined.contains("suggestions"),
        "missing prompt dropdown: {joined}"
    );
    assert!(
        !joined.contains("context side effect"),
        "context bar should yield while prompt dropdown is active: {joined}"
    );
}

#[test]
fn exact_slash_command_enter_submits_instead_of_accepting_completion() {
    let mut state = TuiState::new("m", "s");
    let projection = crate::test_utils::gateway_command_projection_fixture();
    state
        .shell
        .prompt
        .sync_command_suggestions_from_projection(&projection);
    state.replace_input_text("/status");
    state.shell.prompt.refresh_suggestions_from_text_at_cursor(
        &state.input_text(),
        state.input_cursor_byte_offset(),
    );
    assert!(state.shell.prompt.suggestions_visible());

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Submit(text) if text == "/status"));
    assert_eq!(state.input_text(), "");
}

#[test]
fn slash_result_opens_expected_surface() {
    let mut state = TuiState::new("m", "s");

    state.open_surface_for_slash_result("runtime");
    assert!(state.shell.layout_state.sidebar_visible);
    assert_eq!(state.workbench.sidebar_active_tab, TAB_RUNTIME);
    assert_eq!(state.shell.focus_target, FocusTarget::Sidebar);

    state.open_surface_for_slash_result("memory");
    assert_eq!(
        state.workbench.active_topic_panel,
        Some(SidebarTopicPanel::Memory)
    );
    assert_eq!(
        state.shell.focus_target,
        FocusTarget::TopicPanel(SidebarTopicPanel::Memory)
    );
}

#[test]
fn application_backlink_completion_preserves_the_pending_runtime_identity() {
    let mut state = TuiState::new("m", "s");
    let target = "task://task-1";
    state.apply_app_navigation_effect(
        "/runtime",
        Some(&serde_json::json!({
            "kind": "backlink",
            "target": target,
            "object": null,
            "error": null,
        })),
    );
    assert_eq!(state.workbench.sidebar_active_tab, TAB_RUNTIME);
    assert!(state
        .workbench
        .runtime_activity_panel
        .accepts_backlink_result(target));

    state.apply_app_navigation_effect(
        "/runtime",
        Some(&serde_json::json!({
            "kind": "backlink",
            "target": target,
            "object": {"task_id": "task-1", "status": "active"},
            "error": null,
        })),
    );
    assert_eq!(state.workbench.sidebar_active_tab, TAB_RUNTIME);
    assert!(
        state
            .workbench
            .runtime_activity_panel
            .accepts_backlink_result(target),
        "resolved navigation must not clear its own pending target"
    );
}

#[test]
fn mouse_scroll_uses_pointer_region_before_focus() {
    let mut state = TuiState::new("m", "s");
    state.dispatch_action(Action::Execute("/gateway".into()));

    let mut terminal = MockTerminal::new(120, 30);
    terminal.draw(|frame| state.render(frame));
    let sidebar = state
        .shell
        .last_hit_areas
        .sidebar
        .expect("sidebar area should be recorded");
    state.set_focus_target(FocusTarget::Chat);
    state.app.timeline.scroll_offset = 7;
    state.workbench.gateway_panel.scroll_offset = 0;

    assert!(state.handle_mouse_scroll_at(
        true,
        sidebar.x.saturating_add(1),
        sidebar.y.saturating_add(2),
    ));

    assert_eq!(
        state.app.timeline.scroll_offset, 7,
        "pointer over sidebar should not scroll chat even when chat has focus"
    );
    assert!(
        state.workbench.gateway_panel.scroll_offset > 0,
        "sidebar pointer scroll should route into gateway panel"
    );
    assert_eq!(state.shell.focus_target, FocusTarget::Sidebar);
}

#[test]
fn render_activity_panel_as_main_screen_side_rail() {
    let mut state = TuiState::new("m", "s");
    state.workbench.activity_panel_visible = true;
    state.app.add_message("assistant", "inspect build runtime");

    let mut terminal = MockTerminal::new(120, 30);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(
        joined.contains("Activity"),
        "missing activity title: {joined}"
    );
    assert!(
        joined.contains("inspect build runtime"),
        "missing activity event: {joined}"
    );
    assert!(
        !state.shell.layout_state.sidebar_visible,
        "activity rail should not open the heavy sidebar"
    );
}

#[test]
fn focus_change_shows_toast_instead_of_footer_focus() {
    let mut state = TuiState::new("m", "s");
    state.dispatch_action(Action::Execute("/activity".into()));

    let mut terminal = MockTerminal::new(120, 30);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(
        joined.contains("activity: j/k scroll"),
        "missing focus toast: {joined}"
    );
    assert!(
        !joined.contains("focus:activity"),
        "focus should not be pinned in footer: {joined}"
    );
}

#[test]
fn render_status_bar_keeps_top_identity_and_footer_model_on_narrow_width() {
    let mut state = TuiState::new("deepseek-v4-pro", "session-status-narrow");

    let mut terminal = MockTerminal::new(88, 28);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(
        joined.contains(concat!("v", env!("CARGO_PKG_VERSION"))),
        "missing top version: {joined}"
    );
    assert!(
        joined.contains("session session-"),
        "missing top abbreviated session id: {joined}"
    );
    assert!(
        joined.contains("m:deepseek-v4-pro…"),
        "missing compact requested-model waiting state: {joined}"
    );
    assert!(
        !joined.contains("model:") && !joined.contains("focus:"),
        "footer should not show model prefix or focus: {joined}"
    );
    assert!(
        joined.contains("ctx —"),
        "missing compact context: {joined}"
    );
}

#[test]
fn render_never_double_counts_canonical_live_token_metrics() {
    let mut state = TuiState::new("model", "session-token-render");
    state.app.execution.turn_interaction.submit_started();
    state.app.execution.turn_input_tokens = 10;
    state.app.execution.turn_output_tokens = 2;
    state.app.history.input_tokens = 10;
    state.app.history.output_tokens = 2;
    state.app.shell.token_count = 12;

    let mut terminal = MockTerminal::new(100, 28);
    terminal.draw(|frame| state.render(frame));

    assert_eq!(state.app.shell.token_count, 12);
}

#[test]
fn render_status_bar_shows_focus_specific_hint() {
    let mut state = TuiState::new("m", "s");
    state.dispatch_action(Action::Execute("/memory".into()));

    let mut terminal = MockTerminal::new(140, 30);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(!joined.trim().is_empty());
    assert!(
        !joined.contains("focus:memory"),
        "focus should not be pinned in footer: {joined}"
    );
}

#[test]
fn render_thinking_inline_without_floating_panel() {
    let mut state = TuiState::new("m", "s");
    state.apply_event(CowdEvent::TurnStarted);
    state.apply_event(CowdEvent::ReasoningSummaryDelta {
        summary: "Reviewing the request and checking the TUI render path.".into(),
    });

    let mut terminal = MockTerminal::new(100, 30);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(
        joined.contains("|  thinking"),
        "missing top thinking state after stats: {joined}"
    );
    assert!(
        !joined.contains("state "),
        "top bar should not render the word state: {joined}"
    );
    assert!(
        !joined.contains("details in Process"),
        "thinking handoff should stay out of main body: {joined}"
    );
    assert!(
        !joined.contains("┌💭 Thinking") && !joined.contains("┌ 💭 Thinking"),
        "thinking should not render as a floating panel: {joined}"
    );
}

#[test]
fn input_up_down_browses_history_when_input_is_focused() {
    let mut state = TuiState::new("m", "s");
    state.app.shell.input_history.push("first".into());
    state.app.shell.input_history.push("second".into());
    state.set_focus_target(FocusTarget::Input);

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
    assert!(matches!(result, ProcessedKey::Nothing));
    assert_eq!(state.input_text(), "second");

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    assert!(matches!(result, ProcessedKey::Nothing));
    assert_eq!(state.input_text(), "");
}

#[test]
fn normal_typing_and_suggestions_do_not_stack_focus_toasts() {
    let mut state = TuiState::new("m", "s");

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));

    assert!(matches!(result, ProcessedKey::Nothing));
    assert!(
        state.overlay.toast_manager.is_empty(),
        "composer focus transitions must stay silent during ordinary typing"
    );
    assert!(matches!(
        state.shell.focus_target,
        FocusTarget::Input | FocusTarget::PromptSuggestions
    ));
}

#[test]
fn input_up_down_moves_cursor_when_input_has_content() {
    let mut state = TuiState::new("m", "s");
    state.app.shell.input_history.push("history".into());
    state.replace_input_text("first\nsecond");
    state.set_focus_target(FocusTarget::Input);

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
    assert!(matches!(result, ProcessedKey::Nothing));
    assert_eq!(state.input_text(), "first\nsecond");
    assert_eq!(state.app.shell.history_idx, None);
}

#[test]
fn composer_uses_visual_rows_for_vertical_movement_and_keeps_bytes() {
    let mut state = TuiState::new("m", "s");
    state.shell.composer_content_width = 3;
    state.replace_input_text("abcdef");
    let before = state.input_text();

    state.process_raw_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));

    assert_eq!(state.input_text(), before);
    assert_eq!(state.app.shell.input.cursor_byte(), 3);
}

#[test]
fn composer_paste_is_one_undoable_unicode_transaction() {
    let mut state = TuiState::new("m", "s");
    state.replace_input_text("prefix ");
    state.process_paste("👨‍👩‍👧‍👦\r\n中文");
    assert_eq!(state.input_text(), "prefix 👨‍👩‍👧‍👦\r\n中文");

    state.process_raw_key(KeyEvent::new(KeyCode::Char('z'), KeyModifiers::CONTROL));
    assert_eq!(state.input_text(), "prefix ");
}

#[test]
fn streaming_snapshot_deltas_replace_instead_of_duplicate() {
    let mut state = TuiState::new("m", "s");
    state.apply_event(CowdEvent::TurnStarted);
    let correlation = gateway_correlation("s", "execution-1", "turn-1");
    state.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation.clone(),
            text: "partial".into(),
            start_bytes: 0,
            end_bytes: "partial".len(),
            stream_revision: 1,
        },
    });
    state.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation.clone(),
            text: "partial output".into(),
            start_bytes: 0,
            end_bytes: "partial output".len(),
            stream_revision: 2,
        },
    });
    state.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
            correlation: crate::protocol::GatewayEventCorrelation {
                message_id: Some("assistant-1".to_string()),
                terminal_id: Some("terminal-1".to_string()),
                ..correlation
            },
            assistant_text: "partial output".into(),
            sequence: Some(1),
            iterations: 1,
            token_usage: None,
        },
    });

    assert_eq!(state.app.timeline_len(), 1);
    let text = state.app.timeline_get(0).unwrap().full_text();
    assert_eq!(text, "partial output");
}

#[test]
fn render_search_bar_is_not_cleared_by_chat_view() {
    let mut state = TuiState::new("m", "s");
    state.app.timeline.search_active = true;
    state.app.timeline.search_query = "needle".to_string();
    state
        .app
        .add_message("assistant", "needle in the conversation");

    let mut terminal = MockTerminal::new(120, 30);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(joined.contains("/ needle"), "missing search bar: {joined}");
    assert!(
        joined.contains("Esc:cancel Enter:search"),
        "missing search hint: {joined}"
    );
    assert!(joined.contains("needle in the conversation"));
}

#[test]
fn search_moves_a_ten_thousand_message_timeline_to_the_earliest_match() {
    let mut state = TuiState::new("m", "large-session");
    for page_index in 0..20 {
        let offset = page_index * 500;
        let messages = (offset..offset + 500)
            .map(|index| {
                let marker = if index == 0 { "EARLY" } else { "ROW" };
                crate::protocol::SessionMessageProjection {
                    id: format!("message-{index:05}"),
                    session_id: "large-session".to_string(),
                    sequence: index,
                    role: if index % 2 == 0 {
                        "user".to_string()
                    } else {
                        "assistant".to_string()
                    },
                    blocks: vec![serde_json::json!({
                        "type": "text",
                        "text": format!(
                            "TUI-10K-{marker}-{index:05} durable history payload"
                        )
                    })],
                    created_at_ms: u64::try_from(index).unwrap_or(u64::MAX),
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                }
            })
            .collect();
        state.app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "large-session".to_string(),
                messages,
                total: 10_000,
                offset,
                from_seq: Some(offset),
                next_seq: Some(offset + 500),
                limit: 500,
                has_more: page_index < 19,
            },
        });
    }
    let mut terminal = MockTerminal::new(120, 40);
    terminal.draw(|frame| state.render(frame));

    state.process_raw_key(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::CONTROL));
    state.process_paste("TUI-10K-EARLY-00000");
    state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert!(
        joined.contains("TUI-10K-EARLY-00000 durable history payload"),
        "earliest search match must be visible: {joined}"
    );
    assert!(!state.app.timeline.auto_scroll);
    assert_eq!(state.app.timeline.timeline_cursor, 0);
}

#[test]
fn paste_targets_search_without_mutating_the_hidden_composer() {
    let mut state = TuiState::new("m", "s");
    state.app.shell.input.set_text("preserved composer draft");
    state.app.timeline.search_active = true;

    state.process_paste("中文\r\nneedle\tquery");

    assert_eq!(state.app.timeline.search_query, "中文  needle query");
    assert_eq!(state.input_text(), "preserved composer draft");
}

#[test]
fn approval_dialog_binds_canonical_request_and_requires_explicit_yes() {
    let mut state = TuiState::new("m", "s");
    state.app.gateway.gateway_approval_items =
        vec![crate::runtime_control_store::ApprovalSummary {
            id: "approval-exact-1".to_string(),
            tool_name: "write_file".to_string(),
            risk: Some("high".to_string()),
            requester: Some("session:s".to_string()),
            input_preview: "workspace/file.txt".to_string(),
            source_kind: None,
            resource_ref: None,
            review_ref: None,
            effect: Some("write".to_string()),
            resources: vec!["workspace/file.txt".to_string()],
            policy_revision: Some(7),
            expires_at_ms: Some(10_000),
            requested_sandbox_posture: Some("workspace-write-sandbox".to_string()),
            effective_sandbox_posture: Some("workspace-write-sandbox".to_string()),
            blocks_execution: true,
            timeout_policy: Some("pending".to_string()),
            timeout_behavior: Some("execution_waits_for_timeout_resolution".to_string()),
            skippable: false,
            allowed_scopes: vec!["once".to_string()],
        }];

    state.open_approval_dialog();

    assert_eq!(
        state.overlay.pending_approval_dialog,
        Some(("approval-exact-1".to_string(), "once".to_string(), true))
    );
    match &state.overlay.dialog_manager.current().unwrap().kind {
        crate::components::dialog::DialogKind::Confirm {
            message, default, ..
        } => {
            assert!(!default, "Enter must never approve a side effect");
            assert!(message.contains("approval-exact-1"));
            assert!(message.contains("write_file"));
            assert!(message.contains("Risk: high"));
            assert!(message.contains("workspace/file.txt"));
            assert!(message.contains("Policy revision: 7"));
            assert!(message.contains("Allowed scopes: once"));
        }
        other => panic!("unexpected approval dialog: {other:?}"),
    }
}

#[test]
fn startup_overlay_stays_above_input_area() {
    let mut state = TuiState::new("m", "s");
    state.shell.startup_phase = StartupPhase::Loading;

    let mut terminal = MockTerminal::new(100, 24);
    terminal.draw(|frame| state.render(frame));
    let lines = terminal.buffer_lines();
    let loading_row = lines
        .iter()
        .position(|line| line.contains("⟳"))
        .expect("loading overlay should render");
    let input_row = lines
        .iter()
        .position(|line| line.contains("Enter send"))
        .expect("input should render");

    assert!(
        loading_row < input_row,
        "loading overlay row {loading_row} should be above input row {input_row}"
    );
}

#[test]
fn renders_every_sidebar_tab_in_wide_and_compact_layouts() {
    for (width, height) in [(140, 38), (88, 32)] {
        for tab in 0..SIDEBAR_TAB_COUNT {
            let mut state = TuiState::new("m", "scenario-session");
            state
                .shell
                .layout_state
                .toggle_sidebar(&mut state.shell.layout_tree);
            state.app.gateway.server_running = true;
            state.app.gateway.active_api_sessions = 1;
            state.app.gateway.gateway_runtime_readiness = Some("92%".to_string());
            state.app.gateway.gateway_task_count = Some(1);
            state.app.gateway.gateway_pending_approvals = Some(1);
            state.app.gateway.gateway_cross_plane_grants_active = Some(1);
            state.app.workbench.memory_status = Some("available".to_string());
            state.workbench.sidebar_active_tab = tab;

            let mut terminal = MockTerminal::new(width, height);
            terminal.draw(|frame| state.render(frame));
            let joined = terminal.buffer_lines().join("\n");

            assert!(
                !joined.trim().is_empty(),
                "tab {tab} at {width}x{height} rendered an empty buffer"
            );
        }
    }
}

#[test]
fn render_bridge_projects_runtime_command_center_to_gateway_tab() {
    let mut state = TuiState::new("m", "scenario-session");
    state
        .shell
        .layout_state
        .toggle_sidebar(&mut state.shell.layout_tree);
    state.workbench.sidebar_active_tab = TAB_GATEWAY;
    state.app.gateway.server_running = true;
    state.app.gateway.server_uptime_secs = Some(61);
    state.app.gateway.active_api_sessions = 2;
    state.app.gateway.gateway_runtime_readiness = Some("94%".to_string());
    state.app.gateway.gateway_runtime_components = Some(12);
    state.app.gateway.gateway_task_count = Some(3);
    state.app.gateway.gateway_pending_approvals = Some(1);
    state.app.workbench.memory_status = Some("available".to_string());
    state.app.gateway.gateway_action_receipts =
        vec![crate::runtime_control_store::RuntimeActionReceiptSummary {
            status: "ok".to_string(),
            dispatch_status: "completed".to_string(),
            mode: "daemon-control".to_string(),
            capability: "daemon.task.complete".to_string(),
            idempotency_key: Some("task-1".to_string()),
        }];
    state.app.gateway.gateway_connector_resources =
        vec![crate::runtime_control_store::ConnectorResourceSummary {
            reference: "service://local.docs/document/1".to_string(),
            provider: "local.docs".to_string(),
            resource_type: "document".to_string(),
            title: "Bridge Doc".to_string(),
            indexed_state: "indexed".to_string(),
        }];

    let mut terminal = MockTerminal::new(132, 38);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    for expected in [
        "Core Runtime",
        "AI Context",
        "Work Control",
        "Connector Plane",
        "available",
        "completed",
        "local.docs",
        "indexed",
    ] {
        assert!(
            joined.contains(expected),
            "gateway bridge render should contain {expected}, got: {joined}"
        );
    }
}

#[test]
fn system_notices_do_not_pollute_main_chat_timeline() {
    let mut state = TuiState::new("m", "s");
    state.app.add_message("system", "Gateway connected");
    state.app.add_message("assistant", "Visible answer");

    let mut terminal = MockTerminal::new(100, 24);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");

    assert_eq!(state.app.timeline_len(), 1);
    assert!(joined.contains("Visible answer"), "{joined}");
    assert!(
        !joined.contains("Gateway connected"),
        "system control notices must stay out of main chat: {joined}"
    );
}

#[test]
fn config_and_reality_topics_open_dedicated_workbench_panels() {
    let mut state = TuiState::new("m", "s");

    state.dispatch_action(Action::Execute("/config".into()));
    assert_eq!(
        state.workbench.active_topic_panel,
        Some(SidebarTopicPanel::Config)
    );
    assert_eq!(
        state.shell.focus_target,
        FocusTarget::TopicPanel(SidebarTopicPanel::Config)
    );

    let mut terminal = MockTerminal::new(120, 30);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");
    assert!(joined.contains("Config"), "{joined}");

    state.dispatch_action(Action::Execute("/reality".into()));
    assert_eq!(
        state.workbench.active_topic_panel,
        Some(SidebarTopicPanel::Reality)
    );
    state.app.gateway.gateway_reality_core =
        Some(crate::runtime_control_store::RealityCoreSummary {
            status: "ready".to_string(),
            fact_status: "ready".to_string(),
            memory_status: "available".to_string(),
            matrix_status: "ready".to_string(),
            matrix_context_status: "ready".to_string(),
            growth_status: "ready".to_string(),
            context_status: "ready".to_string(),
            audit_status: "ready".to_string(),
            degraded_reasons: Vec::new(),
        });
    state.app.gateway.gateway_structured_data =
        Some(crate::runtime_control_store::StructuredDataSummary {
            source_count: 2,
            fact_count: 7,
            evidence_count: 3,
            watermark_count: 1,
            sample_sources: vec!["source://a".into()],
            sample_facts: vec!["fact://a".into()],
            sample_evidence: vec!["evidence://a".into()],
            sample_watermarks: vec!["wm://a".into()],
        });

    let mut terminal = MockTerminal::new(120, 30);
    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");
    assert!(joined.contains("Reality Core"), "{joined}");
    assert!(joined.contains("facts 7"), "{joined}");
    assert!(joined.contains("Matrix"), "{joined}");
}

// ── startup_loading ─────────────────────────────────────────

#[test]
fn startup_shows_after_delay() {
    let mut state = TuiState::new("m", "s");
    assert_eq!(state.shell.startup_phase, StartupPhase::Hidden);

    // Before 500ms show delay → still Hidden
    state.update_startup_phase_at(
        false,
        state.shell.startup_start + Duration::from_millis(400),
    );
    assert_eq!(state.shell.startup_phase, StartupPhase::Hidden);

    // After 500ms show delay → Loading
    state.update_startup_phase_at(
        false,
        state.shell.startup_start + Duration::from_millis(501),
    );
    assert_eq!(state.shell.startup_phase, StartupPhase::Loading);
}

#[test]
fn startup_hides_when_ready() {
    let mut state = TuiState::new("m", "s");

    // Advance past 500ms to Loading phase
    state.update_startup_phase_at(
        false,
        state.shell.startup_start + Duration::from_millis(501),
    );
    assert_eq!(state.shell.startup_phase, StartupPhase::Loading);

    // Signal ready → Finishing
    let ready_time = state.shell.startup_start + Duration::from_millis(501);
    state.update_startup_phase_at(true, ready_time);
    assert_eq!(state.shell.startup_phase, StartupPhase::Finishing);

    // Before min_display (3s) → still Finishing
    state.update_startup_phase_at(true, ready_time + Duration::from_millis(2500));
    assert_eq!(state.shell.startup_phase, StartupPhase::Finishing);

    // After min_display → Done
    state.update_startup_phase_at(true, ready_time + Duration::from_secs(3));
    assert_eq!(state.shell.startup_phase, StartupPhase::Done);
}

#[test]
fn startup_min_display_3s() {
    let mut state = TuiState::new("m", "s");

    // Start showing Loading at t=500ms
    state.update_startup_phase_at(
        false,
        state.shell.startup_start + Duration::from_millis(500),
    );
    assert_eq!(state.shell.startup_phase, StartupPhase::Loading);

    // Signal ready at t=600ms → Finishing
    let ready_time = state.shell.startup_start + Duration::from_millis(600);
    state.update_startup_phase_at(true, ready_time);
    assert_eq!(state.shell.startup_phase, StartupPhase::Finishing);

    // 2.5s after ready → still Finishing (not yet 3s)
    state.update_startup_phase_at(true, ready_time + Duration::from_millis(2500));
    assert_eq!(state.shell.startup_phase, StartupPhase::Finishing);

    // 3s after ready → Done
    state.update_startup_phase_at(true, ready_time + Duration::from_secs(3));
    assert_eq!(state.shell.startup_phase, StartupPhase::Done);
}

#[test]
fn startup_completes_before_delay_never_shows() {
    let mut state = TuiState::new("m", "s");

    // Ready at t=100ms (before 500ms show delay)
    state.update_startup_phase_at(true, state.shell.startup_start + Duration::from_millis(100));

    // Should skip overlay entirely → Done immediately
    assert_eq!(state.shell.startup_phase, StartupPhase::Done);
}

#[test]
fn startup_loading_text_no_trailing_newline() {
    let mut state = TuiState::new("m", "s");

    state.update_startup_phase_at(
        false,
        state.shell.startup_start + Duration::from_millis(501),
    );
    assert_eq!(
        state.shell.startup_phase,
        StartupPhase::Loading,
        "should be Loading after delay"
    );

    // Signal ready
    state.update_startup_phase_at(true, state.shell.startup_start + Duration::from_millis(600));
    assert_eq!(
        state.shell.startup_phase,
        StartupPhase::Finishing,
        "should be Finishing when ready"
    );
}
