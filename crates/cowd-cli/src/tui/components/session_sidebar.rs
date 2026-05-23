// ── Session Sidebar Component ──────────────────────────────────────────
// Session list with inline rename, delete flag, and new session actions.
// Pure UI state management — no file operations, no dialog rendering.
//
// The parent app is responsible for inspecting the `pending_*` fields
// after `handle_event()` returns `Consumed` and performing the actual
// session operations (rename persistence, deletion via DialogManager, etc.)
// -----------------------------------------------------------------

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    widgets::{Block, Borders, List, ListItem},
};

use crate::tui::app::{SessionSummary, TimelineEntry};
use crate::tui::components::base::{Component, EventResult, RenderContext};
use crate::tui::components::dialog::{DialogKind, DialogManager, DialogResult, DialogState};

/// TUI sidebar for browsing and managing sessions.
///
/// Displays a scrollable, recency-sorted list of sessions with:
///
/// | Key    | Action                               |
/// |--------|--------------------------------------|
/// | `j`/`↓` | Select next session (wraps)           |
/// | `k`/`↑` | Select previous session (wraps)        |
/// | Enter  | Switch to selected session            |
/// | `r`    | Inline rename selected session        |
/// | `d`    | Flag selected session for deletion    |
/// | `n`    | Request a new session                 |
/// | Esc    | Exit inline rename (cancel)           |
///
/// ## Action flags
///
/// After handling a key event that triggers an action, the component sets
/// one of the `pending_*` fields. The parent app reads these fields each
/// frame and performs the corresponding operation, then clears them.
pub struct SessionSidebar {
    /// Full list of available sessions (sorted by `updated_at_ms` descending).
    sessions: Vec<SessionSummary>,
    /// ID of the session currently active in the main chat panel.
    current_session_id: String,
    /// Index of the keyboard-focus cursor (0-based, always valid while
    /// `sessions` is non-empty).
    selected_idx: usize,

    // ── Inline rename state ──
    /// Whether the user is actively typing a new name.
    editing: bool,
    /// The partial (or complete) new name being typed.
    edit_buffer: String,
    /// Index of the session being renamed (meaningful only while `editing`).
    edit_idx: usize,

    // ── Action flags (consumed by parent App) ──
    /// Set when user presses `d` on a session. Reset by `load()`.
    pub pending_delete_idx: Option<usize>,
    /// Set when user presses Enter on a session (not in edit mode).
    pub pending_switch_idx: Option<usize>,
    /// Set when user commits an inline rename (Enter while editing).
    /// Contains `(index_in_sorted_list, new_name)`.
    pub pending_rename: Option<(usize, String)>,
    /// Set when user presses `n`.
    pub pending_new_session: bool,

    // ── Fork state ──
    /// Set when user presses `f` — signals parent to call `open_fork_dialog()`.
    pub pending_fork: bool,
    /// Fork target: `None` = full session, `Some(idx)` = user message index.
    /// Only valid when `pending_fork` is `true`.
    pub pending_fork_at: Option<usize>,
}

impl SessionSidebar {
    /// Create a new sidebar with no sessions.
    ///
    /// `current_session_id` is the ID of the session active in the main
    /// panel — it gets visually highlighted in the list.
    #[must_use]
    pub fn new(current_session_id: &str) -> Self {
        Self {
            sessions: Vec::new(),
            current_session_id: current_session_id.to_string(),
            selected_idx: 0,
            editing: false,
            edit_buffer: String::new(),
            edit_idx: 0,
            pending_delete_idx: None,
            pending_switch_idx: None,
            pending_rename: None,
            pending_new_session: false,
            pending_fork: false,
            pending_fork_at: None,
        }
    }

    /// Populate the sidebar with a fresh session list.
    ///
    /// Sorts sessions by `updated_at_ms` descending. Resets selection,
    /// edit state, and all pending action flags. If the current session
    /// is in the list, the selection moves to it; otherwise selection
    /// starts at index 0.
    pub fn load(&mut self, sessions: Vec<SessionSummary>) {
        // Sort by updated_at_ms descending (most recent first)
        let mut sorted = sessions;
        sorted.sort_by(|a, b| b.updated_at_ms.cmp(&a.updated_at_ms));

        self.sessions = sorted;
        self.editing = false;
        self.edit_buffer.clear();
        self.edit_idx = 0;

        // Reset pending flags
        self.pending_delete_idx = None;
        self.pending_switch_idx = None;
        self.pending_rename = None;
        self.pending_new_session = false;
        self.pending_fork = false;
        self.pending_fork_at = None;

        // If current session is in the list, select it; otherwise start at 0
        self.selected_idx = self
            .sessions
            .iter()
            .position(|s| s.id == self.current_session_id)
            .unwrap_or(0);
    }

    /// Update which session is considered "current".
    ///
    /// This does not reload the session list; it only changes the visual
    /// highlight. Call `load()` to refresh the full list.
    pub fn set_current_session(&mut self, id: &str) {
        self.current_session_id = id.to_string();
    }

    /// Return the number of sessions in the list.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Return `true` if the session list is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Return a reference to the session list (sorted by recency).
    #[must_use]
    pub fn sessions(&self) -> &[SessionSummary] {
        &self.sessions
    }

    /// Return the index of the currently selected session.
    #[must_use]
    pub fn selected_idx(&self) -> usize {
        self.selected_idx
    }

    /// Return whether the component is in inline-rename mode.
    #[must_use]
    pub fn is_editing(&self) -> bool {
        self.editing
    }

    // ── Private helpers ──────────────────────────────────────────────

    /// Move the selection one step forward (with wrap-around).
    /// No-op when the list is empty.
    fn select_next(&mut self) {
        if self.sessions.is_empty() {
            return;
        }
        self.selected_idx = (self.selected_idx + 1) % self.sessions.len();
    }

    /// Move the selection one step backward (with wrap-around).
    /// No-op when the list is empty.
    fn select_prev(&mut self) {
        if self.sessions.is_empty() {
            return;
        }
        self.selected_idx = if self.selected_idx == 0 {
            self.sessions.len().saturating_sub(1)
        } else {
            self.selected_idx - 1
        };
    }

    // ── Fork dialog ─────────────────────────────────────────────────

    /// Open a fork selection dialog listing user messages as fork points.
    ///
    /// Builds a `DialogKind::Select` with:
    /// - Item 0: "⬡ Full session" — fork the entire session
    /// - Item N+1: user message at index N, formatted as
    ///   `"{timestamp} — {preview}"` where `preview` is the first 80
    ///   characters of the message content.
    ///
    /// The dialog is pushed onto `dialog_manager` and rendered by the
    /// TUI's dialog layer. After the user makes a selection, call
    /// [`take_fork_result`](Self::take_fork_result) to consume the result.
    pub fn open_fork_dialog(
        &mut self,
        dialog_manager: &mut DialogManager,
        timeline: &[TimelineEntry],
    ) {
        let mut items: Vec<String> = Vec::new();
        items.push("⬡ Full session".to_string());

        for entry in timeline.iter() {
            if let TimelineEntry::Message { role, content, timestamp } = entry {
                if role == "user" {
                    let preview: String = content.chars().take(80).collect();
                    let preview = if content.len() > 80 {
                        format!("{}…", preview)
                    } else {
                        preview
                    };
                    items.push(format!("{} — {}", timestamp, preview));
                }
            }
        }

        let dialog = DialogState::new(DialogKind::Select {
            title: " Fork session at… ".to_string(),
            items,
            selected: 0,
        });
        dialog_manager.push(dialog);
    }

    /// Consume the result of a dismissed fork dialog.
    ///
    /// Call this after the fork dialog has been dismissed by the user.
    /// Reads the result from the dialog manager's last dismissed result.
    ///
    /// Returns:
    /// - `Some(None)` — user selected "Full session" (fork entire session)
    /// - `Some(Some(idx))` — user selected message at index `idx`
    /// - `None` — dialog was cancelled or no result available
    ///
    /// As a side effect, sets `pending_fork = true` and `pending_fork_at`
    /// to the selected target (or `None` for full session).
    pub fn take_fork_result(
        &mut self,
        dialog_manager: &mut DialogManager,
    ) -> Option<Option<usize>> {
        let result = dialog_manager.take_last_dismissed_result()?;
        match result {
            DialogResult::Selected(0) => {
                self.pending_fork = true;
                self.pending_fork_at = None;
                Some(None)
            }
            DialogResult::Selected(idx) => {
                let msg_idx = idx.saturating_sub(1);
                self.pending_fork = true;
                self.pending_fork_at = Some(msg_idx);
                Some(Some(msg_idx))
            }
            _ => None,
        }
    }
}

// ── Component trait implementation ─────────────────────────────────

impl Component for SessionSidebar {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let accent = ctx.theme().accent_color();

        let title = if self.sessions.is_empty() {
            " Sessions ".to_string()
        } else {
            format!(" Sessions ({}) ", self.sessions.len())
        };

        // Build list items from session data
        let mut items: Vec<ListItem> = Vec::new();

        for (i, session) in self.sessions.iter().enumerate() {
            let is_selected = i == self.selected_idx;
            let is_current = session.id == self.current_session_id;
            let is_editing = self.editing && self.edit_idx == i;

            let label = if is_editing {
                // Inline rename mode — show edit buffer with cursor
                if self.edit_buffer.is_empty() {
                    "  ✎ ▊".to_string()
                } else {
                    format!("  ✎ {}▊", self.edit_buffer)
                }
            } else {
                // Normal display
                let ts = chrono::DateTime::from_timestamp(
                    (session.updated_at_ms / 1000) as i64,
                    0,
                )
                .map(|d| d.format("%m-%d %H:%M").to_string())
                .unwrap_or_default();

                let marker = if is_current { "*" } else { " " };
                let prefix = if is_selected { "▸" } else { " " };
                let id_trunc = &session.id[..8.min(session.id.len())];
                format!(
                    "{} {}  {} msgs  {}  {}",
                    prefix, marker, session.message_count, ts, id_trunc
                )
            };

            let style = if is_editing || is_selected {
                if is_current {
                    Style::default()
                        .fg(Color::Black)
                        .bg(accent)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                }
            } else if is_current {
                Style::default()
                    .fg(accent)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };

            items.push(ListItem::from(label).style(style));
        }

        // Footer hint
        items.push(ListItem::from(""));
        if self.editing {
            items.push(
                ListItem::from(" Enter confirm  Esc cancel ")
                    .style(Style::default().fg(Color::DarkGray)),
            );
        } else {
            items.push(
                ListItem::from(" j/k↓↑ Enter r d n f ")
                    .style(Style::default().fg(Color::DarkGray)),
            );
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .title(title.as_str())
            .fg(accent);

        let list = List::new(items).block(block);
        ctx.frame_mut().render_widget(list, area);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => self.handle_key(key),
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "session_sidebar"
    }
}

// ── Key handling ──────────────────────────────────────────────────

impl SessionSidebar {
    fn handle_key(&mut self, key: &KeyEvent) -> EventResult {
        // ── Inline rename mode keys ──
        if self.editing {
            return match key.code {
                KeyCode::Enter => {
                    // Commit rename
                    let new_name = self.edit_buffer.clone();
                    if !new_name.is_empty() {
                        self.pending_rename = Some((self.edit_idx, new_name));
                    }
                    self.editing = false;
                    self.edit_buffer.clear();
                    EventResult::Consumed
                }
                KeyCode::Esc => {
                    // Cancel rename
                    self.editing = false;
                    self.edit_buffer.clear();
                    EventResult::Consumed
                }
                KeyCode::Char(c) => {
                    self.edit_buffer.push(c);
                    EventResult::Consumed
                }
                KeyCode::Backspace => {
                    self.edit_buffer.pop();
                    EventResult::Consumed
                }
                _ => EventResult::Consumed, // Consume all keys while editing
            };
        }

        // ── Normal mode keys ──
        match key.code {
            // Navigation
            KeyCode::Char('j') | KeyCode::Down => {
                self.select_next();
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.select_prev();
                EventResult::Consumed
            }

            // Switch session
            KeyCode::Enter => {
                if !self.sessions.is_empty() {
                    self.pending_switch_idx = Some(self.selected_idx);
                }
                EventResult::Consumed
            }

            // Inline rename
            KeyCode::Char('r') => {
                if !self.sessions.is_empty() {
                    self.editing = true;
                    self.edit_idx = self.selected_idx;
                    self.edit_buffer.clear();
                }
                EventResult::Consumed
            }

            // Delete flag
            KeyCode::Char('d') => {
                if !self.sessions.is_empty() {
                    self.pending_delete_idx = Some(self.selected_idx);
                }
                EventResult::Consumed
            }

            // New session
            KeyCode::Char('n') => {
                self.pending_new_session = true;
                EventResult::Consumed
            }

            // Fork dialog
            KeyCode::Char('f') => {
                self.pending_fork = true;
                EventResult::Consumed
            }

            _ => EventResult::NotConsumed,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::app::TimelineEntry;
    use crate::tui::components::dialog::{DialogKind, DialogManager, DialogResult, DialogState};
    use crate::tui::components::RenderContext;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;
    use ratatui::Frame;

    // ── Helpers ───────────────────────────────────────────────────

    fn test_session(id: &str, updated_at_ms: u64, message_count: usize) -> SessionSummary {
        SessionSummary {
            id: id.to_string(),
            path: format!("/sessions/{id}.json"),
            updated_at_ms,
            message_count,
        }
    }

    fn sessions_vec(ids: &[&str]) -> Vec<SessionSummary> {
        ids.iter()
            .enumerate()
            .map(|(i, id)| test_session(id, 1000 + i as u64 * 100, (i + 1) * 3))
            .collect()
    }

    fn make_sidebar(sessions: Vec<SessionSummary>, current_id: &str) -> SessionSidebar {
        let mut sidebar = SessionSidebar::new(current_id);
        sidebar.load(sessions);
        sidebar
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, crossterm::event::KeyModifiers::NONE)
    }

    fn key_event(code: KeyCode) -> KeyEvent {
        key(code)
    }

    fn key_press(code: KeyCode) -> Event {
        Event::Key(key(code))
    }

    // ── Construction & load ────────────────────────────────────────

    #[test]
    fn sidebar_new_empty() {
        let sidebar = SessionSidebar::new("sess-1");
        assert!(sidebar.is_empty());
        assert_eq!(sidebar.len(), 0);
        assert_eq!(sidebar.selected_idx(), 0);
        assert!(!sidebar.is_editing());
        assert!(sidebar.pending_delete_idx.is_none());
        assert!(sidebar.pending_switch_idx.is_none());
        assert!(sidebar.pending_rename.is_none());
        assert!(!sidebar.pending_new_session);
    }

    #[test]
    fn sidebar_load_sorts_by_date_descending() {
        // Sessions added out of order
        let sessions = vec![
            test_session("old", 100, 1),
            test_session("mid", 500, 5),
            test_session("new", 900, 10),
        ];
        let sidebar = make_sidebar(sessions, "mid");

        assert_eq!(sidebar.len(), 3);
        // Sorted: new (900), mid (500), old (100)
        assert_eq!(sidebar.sessions()[0].id, "new");
        assert_eq!(sidebar.sessions()[1].id, "mid");
        assert_eq!(sidebar.sessions()[2].id, "old");
    }

    #[test]
    fn sidebar_load_selects_current_session() {
        let sessions = sessions_vec(&["sess-a", "sess-b", "sess-c"]);
        let sidebar = make_sidebar(sessions, "sess-c");

        // "sess-c" has the highest timestamp (index 2 before sort, but
        // sort is by updated_at_ms descending, so "sess-c" is first)
        assert_eq!(sidebar.sessions()[0].id, "sess-c");
        assert_eq!(sidebar.selected_idx(), 0);
    }

    #[test]
    fn sidebar_load_resets_pending_flags() {
        let mut sidebar = SessionSidebar::new("sess-1");
        sidebar.pending_delete_idx = Some(0);
        sidebar.pending_switch_idx = Some(1);
        sidebar.pending_new_session = true;

        let sessions = sessions_vec(&["sess-1", "sess-2"]);
        sidebar.load(sessions);

        assert!(sidebar.pending_delete_idx.is_none());
        assert!(sidebar.pending_switch_idx.is_none());
        assert!(!sidebar.pending_new_session);
        assert!(sidebar.pending_rename.is_none());
    }

    // ── Navigation ────────────────────────────────────────────────

    #[test]
    fn sidebar_jk_navigation() {
        let sessions = sessions_vec(&["a", "b", "c"]);
        let mut sidebar = make_sidebar(sessions, "a");

        // sessions are sorted by updated_at_ms descending:
        //   idx 0: "c" (1200), idx 1: "b" (1100), idx 2: "a" (1000)
        // Current "a" → selected_idx = 2
        assert_eq!(sidebar.selected_idx(), 2);

        // j → 0 (wrap)
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('j')));
        assert_eq!(sidebar.selected_idx(), 0);

        // j → 1
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('j')));
        assert_eq!(sidebar.selected_idx(), 1);

        // j → 2
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('j')));
        assert_eq!(sidebar.selected_idx(), 2);

        // k → 1 (back from 2)
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('k')));
        assert_eq!(sidebar.selected_idx(), 1);
    }

    #[test]
    fn sidebar_arrow_navigation() {
        let sessions = sessions_vec(&["a", "b", "c"]);
        let mut sidebar = make_sidebar(sessions, "a");

        // Sorted: c(1200), b(1100), a(1000) — "a" is at idx 2
        assert_eq!(sidebar.selected_idx(), 2);

        // Down → 0 (wrap from end)
        let _ = sidebar.handle_key(&key_event(KeyCode::Down));
        assert_eq!(sidebar.selected_idx(), 0);

        // Up → 2 (wrap backward from 0)
        let _ = sidebar.handle_key(&key_event(KeyCode::Up));
        assert_eq!(sidebar.selected_idx(), 2);
    }

    #[test]
    fn sidebar_navigation_empty_list_noop() {
        let mut sidebar = SessionSidebar::new("none");
        assert!(sidebar.is_empty());

        // j/k should not panic or modify state
        let r1 = sidebar.handle_key(&key_event(KeyCode::Char('j')));
        assert!(r1.is_consumed());
        assert_eq!(sidebar.selected_idx(), 0);

        let r2 = sidebar.handle_key(&key_event(KeyCode::Char('k')));
        assert!(r2.is_consumed());
        assert_eq!(sidebar.selected_idx(), 0);

        let r3 = sidebar.handle_key(&key_event(KeyCode::Down));
        assert!(r3.is_consumed());
        assert_eq!(sidebar.selected_idx(), 0);
    }

    // ── Enter → switch ────────────────────────────────────────────

    #[test]
    fn sidebar_enter_sets_switch_flag() {
        let sessions = sessions_vec(&["sess-x", "sess-y"]);
        let mut sidebar = make_sidebar(sessions, "sess-x");

        // Sorted: sess-y(1100), sess-x(1000) — "sess-x" is at idx 1
        // j wraps from 1 → 0
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('j')));
        assert_eq!(sidebar.selected_idx(), 0);

        // Enter → switch flag set to idx 0 ("sess-y")
        let _ = sidebar.handle_key(&key_event(KeyCode::Enter));
        assert_eq!(sidebar.pending_switch_idx, Some(0));
    }

    #[test]
    fn sidebar_enter_empty_list_no_switch() {
        let mut sidebar = SessionSidebar::new("none");
        let _ = sidebar.handle_key(&key_event(KeyCode::Enter));
        assert!(sidebar.pending_switch_idx.is_none());
    }

    // ── Rename ────────────────────────────────────────────────────

    #[test]
    fn sidebar_rename_commit() {
        let sessions = sessions_vec(&["sess-a", "sess-b"]);
        let mut sidebar = make_sidebar(sessions, "sess-a");

        // Sorted: sess-b(1100), sess-a(1000) — "sess-a" is at idx 1
        // Press 'r' to enter edit mode at idx 1
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('r')));
        assert!(sidebar.is_editing());
        assert_eq!(sidebar.edit_idx, 1);

        // Type new name
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('m')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('y')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('-')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('s')));

        // Enter to commit — rename at idx 1
        let _ = sidebar.handle_key(&key_event(KeyCode::Enter));
        assert!(!sidebar.is_editing());
        assert_eq!(sidebar.pending_rename, Some((1, "my-s".to_string())));
    }

    #[test]
    fn sidebar_rename_esc_cancel() {
        let sessions = sessions_vec(&["sess-a", "sess-b"]);
        let mut sidebar = make_sidebar(sessions, "sess-a");

        // Enter edit mode
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('r')));
        assert!(sidebar.is_editing());

        // Type some text
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('x')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('y')));
        assert_eq!(sidebar.edit_buffer, "xy");

        // Esc to cancel
        let _ = sidebar.handle_key(&key_event(KeyCode::Esc));
        assert!(!sidebar.is_editing());
        assert!(sidebar.edit_buffer.is_empty());
        // No rename flag set
        assert!(sidebar.pending_rename.is_none());
    }

    #[test]
    fn sidebar_rename_backspace() {
        let sessions = sessions_vec(&["sess-a", "sess-b"]);
        let mut sidebar = make_sidebar(sessions, "sess-a");

        let _ = sidebar.handle_key(&key_event(KeyCode::Char('r')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('a')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('b')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('c')));
        assert_eq!(sidebar.edit_buffer, "abc");

        // Backspace
        let _ = sidebar.handle_key(&key_event(KeyCode::Backspace));
        assert_eq!(sidebar.edit_buffer, "ab");
    }

    #[test]
    fn sidebar_rename_empty_commit_noop() {
        let sessions = sessions_vec(&["sess-a"]);
        let mut sidebar = make_sidebar(sessions, "sess-a");

        // Enter edit mode and immediately press Enter (empty buffer)
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('r')));
        assert!(sidebar.is_editing());
        let _ = sidebar.handle_key(&key_event(KeyCode::Enter));
        assert!(!sidebar.is_editing());
        // No rename should be pending (empty name)
        assert!(sidebar.pending_rename.is_none());
    }

    #[test]
    fn sidebar_rename_on_empty_list_noop() {
        let mut sidebar = SessionSidebar::new("none");
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('r')));
        assert!(!sidebar.is_editing());
    }

    // ── Delete ────────────────────────────────────────────────────

    #[test]
    fn sidebar_delete_sets_flag() {
        let sessions = sessions_vec(&["sess-a", "sess-b"]);
        let mut sidebar = make_sidebar(sessions, "sess-a");

        // Sorted: sess-b(1100), sess-a(1000) — "sess-a" is at idx 1
        // j wraps from 1 → 0, pressing d at idx 0
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('j')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('d')));
        assert_eq!(sidebar.pending_delete_idx, Some(0));
    }

    #[test]
    fn sidebar_delete_empty_list_noop() {
        let mut sidebar = SessionSidebar::new("none");
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('d')));
        assert!(sidebar.pending_delete_idx.is_none());
    }

    // ── New session ───────────────────────────────────────────────

    #[test]
    fn sidebar_new_session_flag() {
        let sessions = sessions_vec(&["sess-a"]);
        let mut sidebar = make_sidebar(sessions, "sess-a");

        let _ = sidebar.handle_key(&key_event(KeyCode::Char('n')));
        assert!(sidebar.pending_new_session);
    }

    #[test]
    fn sidebar_new_session_on_empty_list() {
        let mut sidebar = SessionSidebar::new("none");
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('n')));
        assert!(sidebar.pending_new_session);
    }

    // ── set_current_session ────────────────────────────────────────

    #[test]
    fn sidebar_set_current_session_updates_highlight() {
        let sessions = sessions_vec(&["a", "b", "c"]);
        let mut sidebar = make_sidebar(sessions, "a");

        assert_eq!(sidebar.current_session_id, "a");
        // Current session is at index 0 (sorted by timestamp, "a" has
        // the lowest timestamp so it's last)

        sidebar.set_current_session("b");
        assert_eq!(sidebar.current_session_id, "b");
    }

    // ── Focusable & id ─────────────────────────────────────────────

    #[test]
    fn sidebar_focusable() {
        let sidebar = SessionSidebar::new("test");
        assert!(sidebar.focusable());
    }

    #[test]
    fn sidebar_id() {
        let sidebar = SessionSidebar::new("test");
        assert_eq!(sidebar.id(), "session_sidebar");
    }

    // ── Render tests ──────────────────────────────────────────────

    #[test]
    fn sidebar_render_shows_sessions() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();

        let sessions = sessions_vec(&["abc12345", "def67890", "ghi11111"]);
        let mut sidebar = make_sidebar(sessions, "def67890");

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            sidebar.render(&mut ctx, area);
        });

        // Should show truncated session IDs
        terminal.assert_line_contains("abc12345");
        terminal.assert_line_contains("def67890");
        terminal.assert_line_contains("ghi11111");
        // Title should show count
        terminal.assert_line_contains("Sessions (3)");
        // Footer should show hints
        terminal.assert_line_contains("j/k");
    }

    #[test]
    fn sidebar_render_empty_shows_no_sessions() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();

        let mut sidebar = SessionSidebar::new("none");

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            sidebar.render(&mut ctx, area);
        });

        // Title should show without count
        let lines = terminal.buffer_lines();
        let has_title = lines.iter().any(|l| l.contains("Sessions"));
        assert!(has_title, "Should render title even with empty list");
    }

    #[test]
    fn sidebar_render_rename_mode_shows_edit() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();

        let sessions = sessions_vec(&["session-a"]);
        let mut sidebar = make_sidebar(sessions, "session-a");

        // Enter rename mode and type
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('r')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('m')));
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('y')));

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            sidebar.render(&mut ctx, area);
        });

        // Should show edit indicator and buffer content
        terminal.assert_line_contains("✎");
        terminal.assert_line_contains("my");
        // Footer should show rename-specific hints
        terminal.assert_line_contains("Enter confirm");
    }

    #[test]
    fn sidebar_event_not_consumed_for_non_key() {
        let mut sidebar = SessionSidebar::new("test");
        let result = sidebar.handle_event(&Event::Resize(100, 50));
        assert!(result.is_not_consumed());
    }

    #[test]
    fn sidebar_unknown_key_not_consumed() {
        let mut sidebar = SessionSidebar::new("test");
        let result = sidebar.handle_key(&key(KeyCode::F(1)));
        assert!(result.is_not_consumed());
    }

    #[test]
    fn sidebar_editing_consumes_all_keys() {
        let sessions = sessions_vec(&["a"]);
        let mut sidebar = make_sidebar(sessions, "a");

        // Enter edit mode
        let _ = sidebar.handle_key(&key_event(KeyCode::Char('r')));
        assert!(sidebar.is_editing());

        // Even "d", "n" etc should be consumed while editing
        let r1 = sidebar.handle_key(&key_event(KeyCode::Char('d')));
        assert!(r1.is_consumed()); // 'd' is a regular char, should append to buffer
        assert!(!sidebar.edit_buffer.is_empty());

        // Clean up: Esc
        let _ = sidebar.handle_key(&key_event(KeyCode::Esc));
        assert!(!sidebar.is_editing());
    }

    // ── Fork tests ──────────────────────────────────────────────────

    /// Build a timeline with user and assistant messages for fork testing.
    fn fork_timeline() -> Vec<TimelineEntry> {
        vec![
            TimelineEntry::Message {
                role: "user".into(),
                content: "What is the capital of France?".into(),
                timestamp: "14:30".into(),
            },
            TimelineEntry::Message {
                role: "assistant".into(),
                content: "The capital is Paris.".into(),
                timestamp: "14:31".into(),
            },
            TimelineEntry::Message {
                role: "user".into(),
                content: "And what about Italy?".into(),
                timestamp: "14:32".into(),
            },
            TimelineEntry::Message {
                role: "assistant".into(),
                content: "Rome is the capital of Italy.".into(),
                timestamp: "14:33".into(),
            },
        ]
    }

    #[test]
    fn fork_dialog_lists_user_messages() {
        let mut sidebar = SessionSidebar::new("sess-1");
        let mut dm = DialogManager::new();
        let timeline = fork_timeline();

        sidebar.open_fork_dialog(&mut dm, &timeline);

        // Dialog should be pushed and non-empty
        assert!(!dm.is_empty(), "fork dialog should be pushed");

        let current = dm.current().unwrap();
        match &current.kind {
            DialogKind::Select { title, items, selected } => {
                assert_eq!(*selected, 0, "should start at index 0");
                assert_eq!(title, " Fork session at… ", "dialog title");

                // Item 0: "Full session"
                assert_eq!(items[0], "⬡ Full session");

                // Items 1+2: user messages only (2 users in timeline)
                assert_eq!(items.len(), 3, "should have Full session + 2 user messages");
                assert!(items[1].contains("14:30"), "first user timestamp");
                assert!(items[1].contains("capital of France"), "first user content");
                assert!(items[2].contains("14:32"), "second user timestamp");
                assert!(items[2].contains("Italy"), "second user content");
            }
            _ => panic!("expected DialogKind::Select"),
        }
    }

    #[test]
    fn fork_full_session_option() {
        let mut sidebar = SessionSidebar::new("sess-1");
        let mut dm = DialogManager::new();
        let timeline = fork_timeline();

        sidebar.open_fork_dialog(&mut dm, &timeline);

        // Select "Full session" (index 0) with Enter
        let consumed = dm.handle_key(&key(KeyCode::Enter));
        assert!(consumed, "Enter should be consumed");
        assert!(dm.is_empty(), "dialog should be dismissed");

        // Consume the fork result
        let result = sidebar.take_fork_result(&mut dm);
        assert_eq!(result, Some(None), "Full session → Some(None)");
        assert!(sidebar.pending_fork, "pending_fork should be true");
        assert_eq!(sidebar.pending_fork_at, None, "pending_fork_at = None for full session");
    }

    #[test]
    fn fork_select_message_option() {
        let mut sidebar = SessionSidebar::new("sess-1");
        let mut dm = DialogManager::new();
        let timeline = fork_timeline();

        sidebar.open_fork_dialog(&mut dm, &timeline);

        // Navigate down twice: index 0 → 1 → 2 (second user message)
        let _ = dm.handle_key(&key(KeyCode::Down));
        let _ = dm.handle_key(&key(KeyCode::Down));

        // Verify selection is at index 2
        let current = dm.current().unwrap();
        match &current.kind {
            DialogKind::Select { selected, .. } => assert_eq!(*selected, 2),
            _ => panic!("expected Select"),
        }

        // Select with Enter
        let consumed = dm.handle_key(&key(KeyCode::Enter));
        assert!(consumed);
        assert!(dm.is_empty());

        // Consume the fork result
        let result = sidebar.take_fork_result(&mut dm);
        // Index 2 → message index 1 (because 0 is "Full session")
        assert_eq!(result, Some(Some(1)), "item 2 → message idx 1");
        assert!(sidebar.pending_fork);
        assert_eq!(sidebar.pending_fork_at, Some(1));
    }

    #[test]
    fn fork_navigation_select_cancel() {
        let mut sidebar = SessionSidebar::new("sess-1");
        let mut dm = DialogManager::new();
        let timeline = fork_timeline();

        sidebar.open_fork_dialog(&mut dm, &timeline);

        // Check initial selection at index 0
        assert_eq!(
            match &dm.current().unwrap().kind {
                DialogKind::Select { selected, .. } => *selected,
                _ => unreachable!(),
            },
            0
        );

        // Down → index 1
        let _ = dm.handle_key(&key(KeyCode::Down));
        assert_eq!(
            match &dm.current().unwrap().kind {
                DialogKind::Select { selected, .. } => *selected,
                _ => unreachable!(),
            },
            1
        );

        // Up → back to index 0
        let _ = dm.handle_key(&key(KeyCode::Up));
        assert_eq!(
            match &dm.current().unwrap().kind {
                DialogKind::Select { selected, .. } => *selected,
                _ => unreachable!(),
            },
            0
        );

        // Press Esc to cancel
        let consumed = dm.handle_key(&key(KeyCode::Esc));
        assert!(consumed, "Esc should be consumed");
        assert!(dm.is_empty(), "dialog should be dismissed after Esc");

        // take_fork_result should return None (cancelled)
        let result = sidebar.take_fork_result(&mut dm);
        assert_eq!(result, None, "cancel should return None");
        // pending_fork remains false since we cancelled
        assert!(!sidebar.pending_fork, "cancelled fork should not set pending_fork");
    }

    #[test]
    fn fork_pending_flag_on_f_key() {
        let sessions = sessions_vec(&["sess-a", "sess-b"]);
        let mut sidebar = make_sidebar(sessions, "sess-a");

        // 'f' should set pending_fork
        let result = sidebar.handle_key(&key_event(KeyCode::Char('f')));
        assert!(result.is_consumed(), "'f' key should be consumed");
        assert!(sidebar.pending_fork, "pending_fork should be set by 'f'");
    }

    #[test]
    fn fork_reset_on_load() {
        let mut sidebar = SessionSidebar::new("sess-1");
        sidebar.pending_fork = true;
        sidebar.pending_fork_at = Some(3);

        let sessions = sessions_vec(&["sess-1"]);
        sidebar.load(sessions);

        assert!(!sidebar.pending_fork, "load should reset pending_fork");
        assert_eq!(sidebar.pending_fork_at, None, "load should reset pending_fork_at");
    }
}
