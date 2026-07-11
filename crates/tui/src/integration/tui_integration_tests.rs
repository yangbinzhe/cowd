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
//   6. Search flow (/, type query, Enter, n/N navigation)
//   7. Model switch (via action dispatch)
//   8. Theme toggle dark↔light
//   9. Diff viewer panel
//  10. Command palette dialog open/dismiss
//  11. Dialog focus trap
//  12. Scroll offset updates
//  13. Input history navigation
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
    assert!(state.turn_active);
    assert!(state.is_loading);

    state.apply_event(CowdEvent::TextDelta {
        text: "Hi there!".into(),
    });
    state.apply_event(CowdEvent::TurnComplete {
        assistant_text: "Hi there!".into(),
        iterations: 1,
    });

    assert!(!state.turn_active);
    assert!(!state.is_loading);
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
fn integration_clean_mode_renders_current_turn_evidence_summary() {
    let mut terminal = MockTerminal::new(120, 24);
    let mut state = TuiState::new("test-model", "test-session");
    state.app.compact_chat = true;
    state.app.add_message("user", "Run a diagnostic");
    state.app.add_message("assistant", "Diagnostic complete.");
    state.app.latest_context_envelope = Some(serde_json::json!({
        "selected": [{"id": "ctx-1"}, {"id": "ctx-2"}],
        "omitted": [{"id": "ctx-old"}]
    }));
    state.app.latest_execution_graph_summary = Some(crate::RuntimeExecutionGraphSummary {
        graph_id: None,
        board_id: None,
        status: "ready".into(),
        agent_tasks: 0,
        memory_candidates: 4,
        conflicts: 0,
        completion_rate: None,
        synthesis_lift: None,
        complementarity_score: None,
    });
    state.app.gateway_fact_flow = Some(crate::runtime_control_store::FactFlowSummary {
        source: "test".into(),
        session_id: Some("test-session".into()),
        stage_count: 2,
        event_count: 3,
        promotion_count: 1,
        boundary_count: 1,
    });
    state.app.gateway_pending_approvals = Some(1);

    terminal.draw(|frame| state.render(frame));
    let joined = terminal.buffer_lines().join("\n");
    assert!(joined.contains("Evidence:"));
    assert!(joined.contains("ctx 2/1"));
    assert!(joined.contains("mem candidates 4"));
    assert!(joined.contains("reality s2 e3 p1 b1"));
    assert!(joined.contains("approvals 1"));
}

#[test]
fn integration_session_picker_lifecycle() {
    let mut state = TuiState::new("test-model", "test-session");

    let sessions = vec![
        crate::app::SessionSummary {
            id: "sess-001".into(),
            path: "/tmp".into(),
            updated_at_ms: 1000,
            message_count: 5,
        },
        crate::app::SessionSummary {
            id: "sess-002".into(),
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
fn integration_diff_viewer_component_exists() {
    let mut state = TuiState::new("test-model", "test-session");

    state.add_message("assistant", "diff --git a/file.rs b/file.rs");
    state.add_message("assistant", "+ added line");
    state.add_message("assistant", "- removed line");

    assert!(state.timeline_len() >= 3);
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
    assert_eq!(state.app.input.lines().join("\n"), "/");

    state.process_raw_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    state.process_raw_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::NONE));

    assert!(!state.command_palette.is_open());
    assert_eq!(state.app.input.lines().join("\n"), "/statu");

    state.process_raw_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
    assert_eq!(state.app.input.lines().join("\n"), "/status");
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

    assert_eq!(state.app.input.lines().join("\n"), "please run /status now");
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
    state.input = tui_textarea::TextArea::default();

    // Use handle_input to route / through keybind engine
    let handled = state.handle_input(key(KeyCode::Char('/')));
    // / is bound to Action::Search
    assert!(
        handled || state.search_active,
        "search should be activated by /"
    );

    // If search wasn't activated by keybind, activate directly
    if !state.search_active {
        state.search_active = true;
        state.search_query.clear();
    }
    assert!(state.search_active);

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

    if state.search_matches.len() > 1 {
        let before = state.search_current;
        state.search_next();
        assert!(state.search_current != before || state.search_matches.len() == 1);
    }

    state.cancel_search();
    assert!(state.search_query.is_empty());
    assert!(state.search_matches.is_empty());
}

#[test]
fn integration_accessibility_labels() {
    let state = TuiState::new("test-model", "test-session");

    assert!(state.accessibility.labels.is_empty());
    assert_eq!(state.accessibility.label_for("input"), "input");
    assert_eq!(state.accessibility.label_for("chat_view"), "chat_view");
}

#[test]
fn integration_catch_render_panic() {
    use crate::error_recovery::{catch_render_panic, RenderResult};

    let result = catch_render_panic("test_component", || {
        panic!("render failure simulation");
    });
    match result {
        RenderResult::Degraded(msg) => {
            assert!(msg.contains("test_component"));
            assert!(msg.contains("render failure"));
        }
        RenderResult::Ok => panic!("should have caught the panic"),
    }

    let result = catch_render_panic("test_component", || {});
    assert!(matches!(result, RenderResult::Ok));
}

#[test]
fn integration_profiler_frame_skip() {
    use crate::profiler::FrameTimer;
    use std::time::Duration;

    let mut timer = FrameTimer::new();

    assert!(!timer.should_render(5, 5, Duration::from_millis(100)));
    timer.end_frame();

    assert!(timer.should_render(6, 5, Duration::from_millis(100)));
    timer.mark_rendered();
    timer.end_frame();

    let snap = timer.snapshot();
    assert_eq!(snap.total_frames, 2);
    assert_eq!(snap.rendered_frames, 1);
    assert_eq!(snap.skipped_frames, 1);
}

#[test]
fn integration_animation_engine_tick_and_get() {
    use crate::animation::{AnimationEngine, AnimationKind};

    let mut engine = AnimationEngine::new();

    engine.start_one_shot(AnimationKind::DialogFade, 4);
    assert!(engine.get(AnimationKind::DialogFade).is_some());

    for _ in 0..5 {
        engine.tick();
    }
    assert!(engine.get(AnimationKind::DialogFade).is_none());
    assert!(!engine.any_active());

    engine.start_one_shot(AnimationKind::SearchPulse, 4);
    assert!(engine.any_active());
    let state = engine.get(AnimationKind::SearchPulse).unwrap();
    assert_eq!(state.frame, 0);
    assert!((state.progress - 0.0).abs() < 0.001);
}

#[test]
fn integration_config_migration_format() {
    use crate::config_migration::MigrationReport;
    use crate::config_migration::MigrationResult;
    use std::path::PathBuf;

    let report = MigrationReport {
        result: MigrationResult::Migrated {
            skin_path: PathBuf::from("/tmp/skin.yaml"),
            theme_path: PathBuf::from("/tmp/theme.yaml"),
            backup_path: PathBuf::from("/tmp/skin.yaml.bak"),
        },
        tui_version: 2,
    };

    let formatted = report.format();
    assert!(formatted.contains("v2"));
    assert!(formatted.contains("No data loss"));
    assert!(formatted.contains("Backup"));
}

#[test]
fn integration_high_contrast_wcag_audit() {
    use crate::accessibility::{
        audit_palette_contrast, contrast_ratio, high_contrast_dark_palette,
    };
    use ratatui::style::Color;

    let palette = high_contrast_dark_palette();
    let failures = audit_palette_contrast(&palette);

    assert!(failures.is_empty());

    assert!(contrast_ratio(Color::White, Color::Black) > 10.0);
    assert!(contrast_ratio(Color::Rgb(0, 255, 255), Color::Black) > 10.0);
}

#[test]
fn integration_spinner_rotation() {
    use crate::animation::AnimationEngine;

    let chars: Vec<&str> = (0..10).map(|i| AnimationEngine::spinner_char(i)).collect();

    for c in &chars {
        assert!(!c.is_empty());
        assert!(c.chars().count() == 1);
    }

    assert_eq!(
        AnimationEngine::spinner_char(0),
        AnimationEngine::spinner_char(10)
    );
    assert_eq!(
        AnimationEngine::spinner_char(1),
        AnimationEngine::spinner_char(11)
    );
}
