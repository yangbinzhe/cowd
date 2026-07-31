// ── TUI Integration Tests — 10+ End-to-End Scenarios ────────────
// Uses MockTerminal (ratatui TestBackend) + MockEventSender for
// deterministic, in-memory testing of the full TUI event loop.
//
// Scenarios covered:
//   1. Launch → type message → stream response
//   2. Launch → render check (chat view, status bar visible)
//   3. Panel switch (Chat → Gateway → Files → Memory → Skills → Delegate)
//   4. Session picker lifecycle
//   5. Approval dialog lifecycle
//   6. Search flow (Ctrl+F, type query, Enter, F3/Shift+F3 navigation)
//   7. Model switch (via action dispatch)
//   8. Theme toggle dark↔light
//   9. Command palette dialog open/dismiss
//  10. Dialog focus trap
//  11. Scroll offset updates
//  12. Input history navigation
// -------------------------------------------------------------------

use crate::app::App;
use crate::state::{ProcessedKey, TuiState, SIDEBAR_TAB_COUNT, TAB_GATEWAY, TAB_RUNTIME};
use crate::test_utils::MockTerminal;
use crate::CowdEvent;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn key_ctrl(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::CONTROL)
}

fn key_alt(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::ALT)
}

#[test]
fn integration_launch_type_stream() {
    let mut state = TuiState::new("test-model", "test-session");

    assert!(!state.should_quit);
    assert_eq!(state.model, "test-model");

    state.process_raw_key(KeyEvent::new(KeyCode::Char('H'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::NONE));

    let result = state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(matches!(result, ProcessedKey::Submit(text) if text == "Hello"));

    state.add_message("user", "Hello");
    state.apply_event(CowdEvent::TurnStarted);
    assert!(state.turn_is_active());

    let correlation = crate::protocol::GatewayEventCorrelation {
        session_id: "test-session".to_string(),
        execution_id: Some("execution-1".to_string()),
        turn_id: Some("turn-1".to_string()),
        part_id: Some("item-text-1:text:0".to_string()),
        ..Default::default()
    };
    state.apply_event(CowdEvent::GatewaySession {
        event: crate::protocol::GatewaySessionEvent::TextDelta {
            correlation: correlation.clone(),
            text: "Hi there!".into(),
            start_bytes: 0,
            end_bytes: "Hi there!".len(),
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
            assistant_text: "Hi there!".into(),
            sequence: Some(1),
            iterations: 1,
            token_usage: None,
        },
    });

    assert!(!state.turn_is_active());
    assert!(state.timeline_len() >= 2);
}

#[test]
fn integration_render_chat_view_visible() {
    let mut terminal = MockTerminal::new(80, 24);
    let mut app = App::new("test-model", "test-session");
    app.add_message("user", "Hello, world!");
    app.add_message("assistant", "Hi there!");

    terminal.draw(|f: &mut ratatui::Frame| {
        crate::render::draw(f, &mut app);
    });

    terminal.assert_line_count(24);
    let lines = terminal.buffer_lines();
    let has_content = lines.iter().any(|l| !l.is_empty());
    assert!(has_content);
}

#[test]
fn integration_panel_switch() {
    let mut state = TuiState::new("test-model", "test-session");
    assert!(!state.layout_state.sidebar_visible);

    state.handle_input(key_ctrl(KeyCode::Char('b')));
    assert!(state.layout_state.sidebar_visible);

    // Default tab is Chat (index 0)
    assert_eq!(state.sidebar_active_tab, 0);

    // Tab cycles through every registered sidebar tab and then wraps.
    for expected in 1..SIDEBAR_TAB_COUNT {
        state.handle_input(key(KeyCode::Tab));
        assert_eq!(
            state.sidebar_active_tab, expected,
            "Tab cycle step to tab {expected}"
        );
    }
    // Final Tab wraps back to 0.
    state.handle_input(key(KeyCode::Tab));
    assert_eq!(state.sidebar_active_tab, 0);
}

#[test]
fn integration_terminal_display_mode_and_control_shortcuts() {
    let mut state = TuiState::new("test-model", "test-session");

    assert!(!state.app.compact_chat);
    state.process_raw_key(key_alt(KeyCode::Char('v')));
    assert!(state.app.compact_chat);
    state.process_raw_key(key_alt(KeyCode::Char('v')));
    assert!(!state.app.compact_chat);

    state.process_raw_key(key_alt(KeyCode::Char('e')));
    assert!(state.layout_state.sidebar_visible);
    assert_eq!(state.sidebar_active_tab, TAB_RUNTIME);
    assert!(!state.app.compact_chat);

    state.process_raw_key(key_alt(KeyCode::Char('g')));
    assert!(state.layout_state.sidebar_visible);
    assert_eq!(state.sidebar_active_tab, TAB_GATEWAY);
}

#[test]
fn integration_clean_mode_renders_current_turn_live_stats() {
    let mut terminal = MockTerminal::new(120, 24);
    let mut state = TuiState::new("test-model", "test-session");
    state.app.compact_chat = true;
    state.app.add_message("user", "Run a diagnostic");
    state.app.add_message("assistant", "Diagnostic complete.");
    state.app.turn_input_tokens = 1_200;
    state.app.turn_output_tokens = 340;
    state.app.turn_usage_known = true;
    state.app.current_execution_status =
        Some(harness_contract::projection::ExecutionLiveStatus::CallingTool);
    state.app.current_run_metrics = Some(harness_contract::projection::RunMetricsProjection {
        tool_calls: 4,
        memory_recalls: 2,
        memory_evidence: 1,
        approvals: 1,
        files_touched: 3,
        input_tokens: 1_200,
        output_tokens: 340,
        total_tokens: 1_540,
        ..Default::default()
    });

    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");
    assert!(
        joined.contains("in 1.2k · out 340 · total 1.5k"),
        "{joined}"
    );
    assert!(
        joined.contains("tools 4 · memory 2/1 · approvals 1 · files 3"),
        "{joined}"
    );
    assert!(joined.contains("calling tool"), "{joined}");
}

#[test]
fn integration_session_picker_lifecycle() {
    let mut state = TuiState::new("test-model", "test-session");

    let sessions = vec![
        crate::app::SessionSummary {
            id: "sess-001".into(),
            title: None,
            path: "/tmp".into(),
            updated_at_ms: 1000,
            message_count: 5,
        },
        crate::app::SessionSummary {
            id: "sess-002".into(),
            title: None,
            path: "/tmp".into(),
            updated_at_ms: 2000,
            message_count: 10,
        },
    ];
    state.open_session_picker(sessions);
    assert!(state.picker_active);
    assert_eq!(state.picker_selected_id(), Some("sess-001"));

    state.picker_down();
    assert_eq!(state.picker_selected_id(), Some("sess-002"));

    state.picker_up();
    assert_eq!(state.picker_selected_id(), Some("sess-001"));

    state.close_session_picker();
    assert!(!state.picker_active);
}

#[test]
fn integration_approval_dialog_lifecycle() {
    let mut state = TuiState::new("test-model", "test-session");

    state.approval = Some(crate::app::ApprovalRequest {
        tool_name: "bash".into(),
        input_preview: "rm -rf /".into(),
        approved: false,
    });

    state.open_approval_dialog();
    assert!(!state.dialog_manager.is_empty());

    let enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    let handled = state.handle_input(enter);
    assert!(handled);
}

#[test]
fn integration_model_switch() {
    let mut state = TuiState::new("claude-sonnet-4-6", "test-session");
    state.available_models = vec![
        "claude-sonnet-4-6".into(),
        "claude-haiku-4-5".into(),
        "deepseek-v4-pro".into(),
    ];

    let new_model = state.next_model();
    assert_eq!(new_model, Some("claude-haiku-4-5".into()));
    assert!(state.model_dirty);

    let new_model2 = state.next_model();
    assert_eq!(new_model2, Some("deepseek-v4-pro".into()));

    let new_model3 = state.next_model();
    assert_eq!(new_model3, Some("claude-sonnet-4-6".into()));
}

#[test]
fn integration_theme_toggle() {
    let mut state = TuiState::new("test-model", "test-session");
    assert_eq!(state.theme, crate::app::Theme::Dark);
    assert_eq!(state.theme_engine.theme.name, "dark");

    state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    state.handle_input(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));

    assert_eq!(state.theme, crate::app::Theme::Light);
    assert_eq!(state.theme_engine.theme.name, "light");

    state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    state.handle_input(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));

    assert_eq!(state.theme, crate::app::Theme::Dark);
    assert_eq!(state.theme_engine.theme.name, "dark");
}

#[test]
fn integration_command_palette() {
    let mut state = TuiState::new("test-model", "test-session");

    state.handle_input(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    state.handle_input(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));

    assert!(state.command_palette.is_open());
    assert!(state.dialog_manager.is_empty());

    let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    state.handle_input(esc);
    assert!(!state.command_palette.is_open());
}

#[test]
fn integration_slash_keeps_input_control_without_opening_palette() {
    let mut state = TuiState::new("test-model", "test-session");

    state.process_raw_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
    assert!(!state.command_palette.is_open());
    assert_eq!(state.app.input.text(), "/");

    state.process_raw_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::NONE));

    assert!(!state.command_palette.is_open());
    assert_eq!(state.app.input.text(), "/statu");

    state.process_raw_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
    assert_eq!(state.app.input.text(), "/status");
}

#[test]
fn integration_mid_text_slash_completion_replaces_current_token() {
    let mut state = TuiState::new("test-model", "test-session");
    let projection = crate::test_utils::gateway_command_projection_fixture();
    state
        .prompt
        .sync_command_suggestions_from_projection(&projection);

    for c in "please run /statu now".chars() {
        state.process_raw_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
    }
    for _ in 0..4 {
        state.process_raw_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE));
    }

    state.process_raw_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));

    assert_eq!(state.app.input.text(), "please run /status now");
}

#[test]
fn integration_absolute_path_slash_does_not_open_command_palette() {
    let mut state = TuiState::new("test-model", "test-session");

    for c in "read /home/yi/project".chars() {
        state.process_raw_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
    }

    assert!(!state.command_palette.is_open());
}

#[test]
fn integration_dialog_focus_trap_multiple() {
    let mut state = TuiState::new("test-model", "test-session");

    use crate::components::dialog::{DialogKind, DialogState};
    state
        .dialog_manager
        .push(DialogState::new(DialogKind::Alert {
            title: "Error".into(),
            message: "Something went wrong".into(),
        }));

    let a_key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);
    let handled = state.handle_input(a_key);
    assert!(handled);

    assert!(state.dialog_manager.is_empty());
}

#[test]
fn integration_scroll_offset_updates() {
    let mut state = TuiState::new("test-model", "test-session");

    for i in 0..50 {
        state.add_message("user", &format!("Message number {i}"));
    }

    assert!(state.timeline_len() >= 50);

    let initial_scroll = state.scroll_offset;
    state.handle_input(key(KeyCode::Char('j')));
    assert_eq!(state.scroll_offset, initial_scroll + 1);
    assert!(!state.auto_scroll);

    state.handle_input(key(KeyCode::Char('k')));
    assert_eq!(state.scroll_offset, initial_scroll);

    state.handle_input(key(KeyCode::Char('g')));
    state.handle_input(key(KeyCode::Char('g')));
    assert_eq!(state.scroll_offset, 0);
}

#[test]
fn integration_input_history_navigation() {
    let mut state = TuiState::new("test-model", "test-session");

    state.input_history.push("first command".into());
    state.input_history.push("second command".into());
    state.input_history.push("third command".into());

    let text = state.history_prev();
    assert_eq!(text, Some("third command".into()));

    let text = state.history_prev();
    assert_eq!(text, Some("second command".into()));

    let text = state.history_next();
    assert_eq!(text, Some("third command".into()));

    let text = state.history_next();
    assert_eq!(text, Some(String::new()));

    assert!(state.history_idx.is_none());
}

#[test]
fn integration_timeline_entry_lifecycle() {
    let mut state = TuiState::new("test-model", "test-session");

    state.apply_event(CowdEvent::TurnStarted);
    state.apply_event(CowdEvent::ToolStart {
        id: "tool-1".into(),
        name: "bash".into(),
        preview: "ls -la".into(),
    });

    let has_tool = state.timeline_iter().any(
        |(_, e)| matches!(e, crate::app::TimelineEntry::ToolCall { id, .. } if id == "tool-1"),
    );
    assert!(has_tool);

    state.apply_event(CowdEvent::ToolComplete {
        id: "tool-1".into(),
        name: "bash".into(),
        summary: "file1 file2 file3".into(),
        exit_code: Some(0),
    });

    let tool = state.timeline_iter().find_map(|(_, e)| {
        if let crate::app::TimelineEntry::ToolCall {
            id, done, expanded, ..
        } = e
        {
            if id == "tool-1" {
                Some((*done, *expanded))
            } else {
                None
            }
        } else {
            None
        }
    });
    assert_eq!(tool, Some((true, false)));
}

#[test]
fn integration_notification_lifecycle() {
    let mut state = TuiState::new("test-model", "test-session");

    state.show_notification("Model switched to claude-haiku");
    assert!(state.notification.is_some());

    for _ in 0..30 {
        state.tick();
    }
    assert!(state.notification.is_none());
}

#[test]
fn integration_search_flow() {
    let mut state = TuiState::new("test-model", "test-session");
    state.add_message("user", "Hello world");
    state.add_message("assistant", "Hi there, world!");

    // Clear input to allow search activation via keybind engine
    state.input = crate::components::composer::model::ComposerModel::default();

    state.process_raw_key(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::CONTROL));
    assert!(
        state.search_active,
        "search should be activated by Ctrl+F through the production input path"
    );

    // Type query characters (search_active routes to handle_search_key)
    state.process_raw_key(key(KeyCode::Char('w')));
    state.process_raw_key(key(KeyCode::Char('o')));
    state.process_raw_key(key(KeyCode::Char('r')));
    state.process_raw_key(key(KeyCode::Char('l')));
    state.process_raw_key(key(KeyCode::Char('d')));
    assert_eq!(state.search_query, "world");

    // Press Enter to execute search
    state.process_raw_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(!state.search_active);
    assert!(!state.search_matches.is_empty());

    assert_eq!(state.search_matches.len(), 2);
    state.process_raw_key(KeyEvent::new(KeyCode::F(3), KeyModifiers::NONE));
    assert_eq!(state.search_current, 1);
    state.process_raw_key(KeyEvent::new(KeyCode::F(3), KeyModifiers::SHIFT));
    assert_eq!(state.search_current, 0);

    state.cancel_search();
    assert!(state.search_query.is_empty());
    assert!(state.search_matches.is_empty());
}
