// ── TUI Test Infrastructure ──────────────────────────────────────────
// MockTerminal, MockEventSender, tui_test! macro, and test fixtures.
// Phase 0: harness for deterministic TUI component testing.
// ----------------------------------------------------------------------
#![allow(dead_code)]

use std::sync::mpsc;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::{backend::TestBackend, Frame, Terminal};

use crate::tui::app::App;
use runtime::CowdEvent;

// ── MockTerminal ──────────────────────────────────────────────────

/// Wraps a `ratatui::Terminal<TestBackend>` for in-memory, deterministic testing.
///
/// # Panics
/// - `draw()` panics if draw fails (should never happen with TestBackend)
/// - `assert_line_contains()` panics with buffer content if text not found
/// - `assert_line_count()` panics with buffer content if line count mismatch
pub struct MockTerminal {
    terminal: Terminal<TestBackend>,
}

impl MockTerminal {
    /// Create a new MockTerminal with the given (columns, rows) dimensions.
    #[must_use]
    pub fn new(width: u16, height: u16) -> Self {
        let backend = TestBackend::new(width, height);
        let terminal = Terminal::new(backend).expect("TestBackend::new never fails");
        Self { terminal }
    }

    /// Draw using the provided render closure.
    /// The closure receives a `&mut Frame` and can render widgets into it.
    ///
    /// # Panics
    /// Panics if the internal `terminal.draw()` call fails (unexpected for TestBackend).
    pub fn draw<F>(&mut self, render_fn: F)
    where
        F: FnOnce(&mut Frame),
    {
        self.terminal.draw(render_fn).expect("draw failed");
    }

    /// Assert that at least one line in the rendered buffer contains `text`.
    ///
    /// # Panics
    /// Panics with the full buffer content if `text` is not found in any line.
    pub fn assert_line_contains(&self, text: &str) {
        let lines = self.buffer_lines();
        if !lines.iter().any(|l| l.contains(text)) {
            panic!(
                "assert_line_contains: text '{text}' not found\nBuffer:\n{}",
                self.buffer_str()
            );
        }
    }

    /// Assert that the rendered buffer has exactly `expected` lines.
    /// The line count includes all rows (empty rows at the bottom are counted).
    ///
    /// # Panics
    /// Panics with the full buffer content if the line count doesn't match.
    pub fn assert_line_count(&self, expected: usize) {
        let lines = self.buffer_lines();
        let actual = lines.len();
        assert_eq!(
            actual,
            expected,
            "assert_line_count: expected {expected} lines, got {actual}\nBuffer:\n{}",
            self.buffer_str()
        );
    }

    /// Return all lines from the terminal buffer, trimmed of trailing whitespace.
    /// The returned `Vec` has exactly `dimensions().1` elements.
    #[must_use]
    pub fn buffer_lines(&self) -> Vec<String> {
        let buffer = self.terminal.backend().buffer();
        let area = buffer.area();
        let mut lines = Vec::with_capacity(area.height as usize);
        for y in 0..area.height {
            let mut line = String::with_capacity(area.width as usize);
            for x in 0..area.width {
                line.push_str(buffer[(x, y)].symbol());
            }
            lines.push(line.trim_end().to_string());
        }
        lines
    }

    /// Return the terminal dimensions as `(columns, rows)`.
    #[must_use]
    pub fn dimensions(&self) -> (u16, u16) {
        let size = self.terminal.size().expect("TestBackend::size never fails");
        (size.width, size.height)
    }

    fn buffer_str(&self) -> String {
        self.buffer_lines().join("\n")
    }
}

// ── MockEventSender ───────────────────────────────────────────────

/// Sends synthetic crossterm `Event`s into a channel for TUI testing.
///
/// Holds a `std::sync::mpsc::Sender<crossterm::event::Event>` internally.
/// Create via `MockEventSender::new()` which returns `(Self, Receiver<Event>)`.
pub struct MockEventSender {
    tx: mpsc::Sender<Event>,
}

impl MockEventSender {
    /// Create a new MockEventSender and the corresponding receiver.
    /// The returned `mpsc::Receiver<Event>` can be polled by the TUI event loop.
    #[must_use]
    pub fn new() -> (Self, mpsc::Receiver<Event>) {
        let (tx, rx) = mpsc::channel();
        (Self { tx }, rx)
    }

    /// Send a single key press event with no modifiers.
    ///
    /// # Arguments
    /// * `code` - The key code (e.g., `KeyCode::Char('a')`, `KeyCode::Enter`).
    ///
    /// # Panics
    /// Silently ignores send failures (channel full / disconnected).
    pub fn press_key(&self, code: KeyCode) {
        let event = Event::Key(KeyEvent::new(code, KeyModifiers::NONE));
        let _ = self.tx.send(event);
    }

    /// Send a sequence of key presses in order.
    ///
    /// # Example
    /// ```ignore
    /// sender.press_chord([KeyCode::Char('q'), KeyCode::Esc]);
    /// ```
    pub fn press_chord<I>(&self, codes: I)
    where
        I: IntoIterator<Item = KeyCode>,
    {
        for code in codes {
            self.press_key(code);
        }
    }

    /// Send `KeyCode::Char(ch)` events for each character in `text`.
    /// Does NOT send an Enter at the end — use `press_key(KeyCode::Enter)` separately.
    pub fn type_text(&self, text: &str) {
        for ch in text.chars() {
            self.press_key(KeyCode::Char(ch));
        }
    }
}

// ── Test Fixtures ─────────────────────────────────────────────────

/// Create an `App` pre-populated with `n` alternating user/assistant messages.
///
/// Messages are added via `App::add_message()`, which handles timeline
/// cursor positioning, message versioning, and auto-trim.
#[must_use]
pub fn app_with_messages(n: usize) -> App {
    let mut app = App::new("fixture-model", "fixture-session");
    for i in 0..n {
        let role = if i % 2 == 0 { "user" } else { "assistant" };
        app.add_message(role, &format!("Message {i}"));
    }
    app
}

/// Create an `App` with `n` completed tool calls in the timeline.
///
/// Each tool call has a unique id: `tool_0`, `tool_1`, ... with name `"bash"`
/// and exit_code `Some(0)`. Tool calls go through `App::apply_event` so they
/// follow the same code path as the real TUI.
#[must_use]
pub fn app_with_tool_calls(n: usize) -> App {
    let mut app = App::new("fixture-model", "fixture-session");
    for i in 0..n {
        app.apply_event(CowdEvent::ToolStart {
            id: format!("tool_{i}"),
            name: "bash".into(),
            preview: format!("echo {i}"),
        });
        app.apply_event(CowdEvent::ToolComplete {
            id: format!("tool_{i}"),
            name: "bash".into(),
            summary: format!("Output of tool {i}"),
            exit_code: Some(0),
        });
    }
    app
}

/// Create an `App` in a streaming state — `TurnStarted` received,
/// one `TextDelta` applied, `turn_active` is still true.
#[must_use]
pub fn app_streaming() -> App {
    let mut app = App::new("fixture-model", "fixture-session");
    app.apply_event(CowdEvent::TurnStarted);
    app.apply_event(CowdEvent::TextDelta {
        text: "Streaming response...".into(),
    });
    app
}

// ── tui_test! Macro and implementation ────────────────────────────

/// Internal: shared setup for `tui_test!` macro.
/// Creates terminal + app + channel, calls the user's test closure.
pub fn __tui_test_setup<F>(test_fn: F)
where
    F: FnOnce(&mut MockTerminal, &mut App, MockEventSender),
{
    let mut terminal = MockTerminal::new(80, 24);
    let mut app = App::new("test-model", "test-session");
    let (event_sender, _receiver) = MockEventSender::new();
    test_fn(&mut terminal, &mut app, event_sender);
}

/// Convenience macro to set up a TUI test environment at 80×24.
///
/// The closure receives three arguments:
/// - `terminal`: `&mut MockTerminal`  (80×24)
/// - `app`: `&mut App` (empty, "test-model" / "test-session")
/// - `event_sender`: `MockEventSender` (receiver is dropped)
///
/// # Example
/// ```ignore
/// tui_test!(my_test, |terminal, app, _sender| {
///     terminal.draw(|f: &mut Frame| {
///         f.render_widget(Paragraph::new("hello"), f.area());
///     });
///     terminal.assert_line_contains("hello");
/// });
/// ```
#[macro_export]
macro_rules! tui_test {
    ($name:ident, $body:expr) => {
        #[test]
        fn $name() {
            $crate::tui::test_utils::__tui_test_setup($body);
        }
    };
}

// ── Harness Self-Tests ────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::widgets::Paragraph;

    // ── MockTerminal tests ───────────────────────────────────────

    #[test]
    fn mock_terminal_renders_app() {
        let mut terminal = MockTerminal::new(80, 24);
        terminal.draw(|f: &mut Frame| {
            f.render_widget(Paragraph::new("Hello, world!"), f.area());
        });
        // Buffer should have 24 lines (empty ones count)
        terminal.assert_line_count(24);
        // First line should contain our text
        let lines = terminal.buffer_lines();
        assert!(lines[0].contains("Hello, world!"));
    }

    #[test]
    fn mock_terminal_assert_line_found() {
        let mut terminal = MockTerminal::new(80, 24);
        terminal.draw(|f: &mut Frame| {
            f.render_widget(Paragraph::new("Hello, world!"), f.area());
        });
        terminal.assert_line_contains("Hello");
        terminal.assert_line_contains("world");
        terminal.assert_line_count(24);
    }

    #[test]
    #[should_panic(expected = "not found")]
    fn mock_terminal_assert_line_missing_panics() {
        let mut terminal = MockTerminal::new(80, 24);
        terminal.draw(|f: &mut Frame| {
            f.render_widget(Paragraph::new("xyz"), f.area());
        });
        terminal.assert_line_contains("nonexistent");
    }

    #[test]
    fn mock_terminal_dimensions() {
        let terminal = MockTerminal::new(120, 40);
        assert_eq!(terminal.dimensions(), (120, 40));
    }

    #[test]
    fn mock_terminal_buffer_lines_count() {
        let terminal = MockTerminal::new(80, 30);
        assert_eq!(terminal.buffer_lines().len(), 30);
    }

    #[test]
    fn multi_line_paragraph() {
        let mut terminal = MockTerminal::new(40, 10);
        terminal.draw(|f: &mut Frame| {
            f.render_widget(Paragraph::new("line1\nline2\nline3"), f.area());
        });
        let lines = terminal.buffer_lines();
        assert!(lines[0].contains("line1"));
        assert!(lines[1].contains("line2"));
        assert!(lines[2].contains("line3"));
    }

    // ── MockEventSender tests ────────────────────────────────────

    #[test]
    fn mock_event_sender_press_key() {
        let (sender, rx) = MockEventSender::new();
        sender.press_key(KeyCode::Char('a'));
        let event = rx.try_recv().expect("should have received event");
        assert!(matches!(event, Event::Key(k) if k.code == KeyCode::Char('a')));
    }

    #[test]
    fn mock_event_sender_press_chord() {
        let (sender, rx) = MockEventSender::new();
        sender.press_chord([KeyCode::Char('h'), KeyCode::Char('i')]);
        let e1 = rx.try_recv().expect("should have first event");
        let e2 = rx.try_recv().expect("should have second event");
        assert!(matches!(e1, Event::Key(k) if k.code == KeyCode::Char('h')));
        assert!(matches!(e2, Event::Key(k) if k.code == KeyCode::Char('i')));
    }

    #[test]
    fn mock_event_sender_type_text() {
        let (sender, rx) = MockEventSender::new();
        sender.type_text("abc");
        let mut chars: Vec<char> = Vec::new();
        for _ in 0..3 {
            if let Ok(Event::Key(k)) = rx.try_recv() {
                if let KeyCode::Char(c) = k.code {
                    chars.push(c);
                }
            }
        }
        assert_eq!(chars, vec!['a', 'b', 'c']);
    }

    #[test]
    fn mock_event_sender_enter_key() {
        let (sender, rx) = MockEventSender::new();
        sender.press_key(KeyCode::Enter);
        let event = rx.try_recv().expect("should have event");
        assert!(matches!(event, Event::Key(k) if k.code == KeyCode::Enter));
    }

    // ── Fixture tests ────────────────────────────────────────────

    #[test]
    fn fixture_roundtrip() {
        let app = app_with_messages(5);
        assert_eq!(app.timeline_len(), 5);
        for (i, entry) in app.timeline_iter() {
            let expected_role = if i % 2 == 0 { "user" } else { "assistant" };
            match entry {
                crate::tui::app::TimelineEntry::Message { role, content, .. } => {
                    assert_eq!(role.as_str(), expected_role);
                    assert!(
                        content.contains(&format!("Message {i}")),
                        "content={content}, expected Message {i}"
                    );
                }
                _ => panic!("expected Message entry, got {entry:?}"),
            }
        }
    }

    #[test]
    fn fixture_messages_alternate_roles() {
        let app = app_with_messages(4);
        let roles: Vec<&str> = app
            .timeline_iter()
            .filter_map(|(_, e)| match e {
                crate::tui::app::TimelineEntry::Message { role, .. } => Some(role.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(roles, vec!["user", "assistant", "user", "assistant"]);
    }

    #[test]
    fn fixture_tool_calls_completed() {
        let app = app_with_tool_calls(3);
        // ToolStart pushes a ToolCall entry; ToolComplete mutates it in-place.
        // So 3 tool calls → 3 timeline entries.
        assert_eq!(app.timeline_len(), 3);
        for (_, entry) in app.timeline_iter() {
            if let crate::tui::app::TimelineEntry::ToolCall {
                done, exit_code, ..
            } = entry
            {
                assert!(*done, "tool should be done");
                assert_eq!(exit_code, &Some(0), "exit_code should be 0");
            }
        }
    }

    #[test]
    fn fixture_app_streaming_is_active() {
        let app = app_streaming();
        assert!(app.turn_active);
        // Should have 1 TextDelta message in timeline
        let msg = app
            .timeline_iter()
            .find_map(|(_, e)| match e {
                crate::tui::app::TimelineEntry::Message { content, .. } => Some(content.clone()),
                _ => None,
            })
            .expect("should have a message");
        assert!(msg.contains("Streaming response"));
    }

    #[test]
    fn app_with_zero_messages_produces_empty_timeline() {
        let app = app_with_messages(0);
        assert!(app.timeline_is_empty());
    }

    // ── tui_test! macro tests ────────────────────────────────────

    tui_test!(
        tui_test_macro_basic,
        |terminal: &mut MockTerminal, _app: &mut App, _sender: MockEventSender| {
            terminal.draw(|f: &mut Frame| {
                f.render_widget(Paragraph::new("macro test"), f.area());
            });
            terminal.assert_line_contains("macro test");
        }
    );

    tui_test!(
        tui_test_macro_dimensions,
        |terminal: &mut MockTerminal, app: &mut App, _sender: MockEventSender| {
            assert_eq!(terminal.dimensions(), (80, 24));
            assert_eq!(app.model, "test-model");
            assert_eq!(app.session_id, "test-session");
        }
    );
}
