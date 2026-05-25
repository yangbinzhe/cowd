// ── ScrollState — Unified scroll state for TUI scrolling ──────
// Borrows pattern from ratatui-kit ScrollViewState:
//   offset, content_height, viewport_height, auto_scroll
// Provides unified handle_event() for keyboard + mouse input.
// -----------------------------------------------------------------

use crossterm::event::{Event, KeyCode, KeyEventKind, MouseEventKind};

/// Encapsulates scroll position and viewport state for a scrollable area.
///
/// # Fields
/// - `offset`: current scroll position (topmost visible line).
/// - `content_height`: total height of the content in lines.
/// - `viewport_height`: visible height of the viewport in lines.
/// - `auto_scroll`: when true, the view auto-follows new content (streaming).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScrollState {
    pub offset: u16,
    pub content_height: u16,
    pub viewport_height: u16,
    pub auto_scroll: bool,
}

impl ScrollState {
    /// Create a new ScrollState with sensible defaults.
    ///
    /// Default viewport height is 24 rows (common terminal height minus status/input).
    /// Auto-scroll is enabled by default so new content pulls the view to the bottom.
    #[must_use]
    pub fn new() -> Self {
        Self {
            offset: 0,
            content_height: 0,
            viewport_height: 24,
            auto_scroll: true,
        }
    }

    // ── Basic scroll methods ─────────────────────────────────────

    /// Scroll up by one line. Disables auto-scroll.
    pub fn scroll_up(&mut self) {
        self.offset = self.offset.saturating_sub(1);
        self.auto_scroll = false;
    }

    /// Scroll down by one line. Disables auto-scroll.
    pub fn scroll_down(&mut self) {
        self.offset = self.offset.saturating_add(1);
        self.auto_scroll = false;
    }

    /// Scroll up by one viewport height (minus 1 line for context).
    pub fn scroll_page_up(&mut self) {
        let amount = self.viewport_height.max(1).saturating_sub(1);
        self.offset = self.offset.saturating_sub(amount);
        self.auto_scroll = false;
    }

    /// Scroll down by one viewport height (minus 1 line for context).
    pub fn scroll_page_down(&mut self) {
        let amount = self.viewport_height.max(1).saturating_sub(1);
        self.offset = self.offset.saturating_add(amount);
        self.auto_scroll = false;
    }

    /// Scroll to the top of the content.
    pub fn scroll_to_top(&mut self) {
        self.offset = 0;
        self.auto_scroll = false;
    }

    /// Scroll to the bottom of the content. Enables auto-scroll.
    pub fn scroll_to_bottom(&mut self) {
        self.auto_scroll = true;
    }

    // ── Unified event handler ────────────────────────────────────

    /// Handle a crossterm Event for scrolling (keyboard + mouse).
    ///
    /// Returns `true` if the event was consumed (scroll action performed),
    /// `false` if the event was not a scroll-related event.
    ///
    /// Keyboard events are only handled on `KeyEventKind::Press` to avoid
    /// double-processing releases.
    ///
    /// Mouse `ScrollDown` and `ScrollUp` map to `scroll_down()`/`scroll_up()`.
    pub fn handle_event(&mut self, event: &Event) -> bool {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => {
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        self.scroll_up();
                        true
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        self.scroll_down();
                        true
                    }
                    KeyCode::PageUp => {
                        self.scroll_page_up();
                        true
                    }
                    KeyCode::PageDown => {
                        self.scroll_page_down();
                        true
                    }
                    KeyCode::Home => {
                        self.scroll_to_top();
                        true
                    }
                    KeyCode::End => {
                        self.scroll_to_bottom();
                        true
                    }
                    _ => false,
                }
            }
            Event::Mouse(mouse) => match mouse.kind {
                MouseEventKind::ScrollDown => {
                    self.scroll_down();
                    true
                }
                MouseEventKind::ScrollUp => {
                    self.scroll_up();
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }

    /// Set content size after rendering and clamp offsets.
    ///
    /// Called post-render to sync the actual rendered content height.
    /// This prevents ghost scroll space by ensuring the offset never
    /// exceeds the real content bounds.
    pub fn set_content_size(&mut self, lines: u16) {
        self.content_height = lines;
        self.clamp();
    }

    /// Clamp scroll offset to prevent ghost scroll space.
    ///
    /// Call after content height or viewport height changes to ensure
    /// the offset does not exceed `content_height - viewport_height`.
    /// If `auto_scroll` is true, snaps to bottom.
    pub fn clamp(&mut self) {
        if self.auto_scroll {
            let total = self.content_height as usize;
            let vh = self.viewport_height as usize;
            if total > vh {
                self.offset = (total - vh) as u16;
            } else {
                self.offset = 0;
            }
        } else {
            let max_offset = self
                .content_height
                .saturating_sub(self.viewport_height);
            if self.offset > max_offset {
                self.offset = max_offset;
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind};

    // ── test_scroll_state_up ─────────────────────────────────────
    #[test]
    fn test_scroll_state_up() {
        let mut s = ScrollState::new();
        s.offset = 5;
        s.scroll_up();
        assert_eq!(s.offset, 4);
        assert!(!s.auto_scroll, "manual scroll should disable auto_scroll");
    }

    // ── test_scroll_state_down ───────────────────────────────────
    #[test]
    fn test_scroll_state_down() {
        let mut s = ScrollState::new();
        s.offset = 0;
        s.auto_scroll = true;
        s.scroll_down();
        assert_eq!(s.offset, 1);
        assert!(!s.auto_scroll, "manual scroll should disable auto_scroll");
    }

    // ── test_scroll_state_page_down ──────────────────────────────
    #[test]
    fn test_scroll_state_page_down() {
        let mut s = ScrollState::new();
        s.viewport_height = 20;
        s.offset = 0;
        s.scroll_page_down();
        assert_eq!(s.offset, 19, "page down = viewport_height - 1");
        assert!(!s.auto_scroll);
    }

    // ── test_scroll_state_mouse ──────────────────────────────────
    #[test]
    fn test_scroll_state_mouse() {
        let mut s = ScrollState::new();
        s.offset = 5;
        s.auto_scroll = true;

        // Mouse scroll down → offset increases
        let mouse_down = Event::Mouse(MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: 10,
            row: 5,
            modifiers: KeyModifiers::NONE,
        });
        let consumed = s.handle_event(&mouse_down);
        assert!(consumed);
        assert_eq!(s.offset, 6);
        assert!(!s.auto_scroll);
    }

    // ── test_scroll_state_handle_event ───────────────────────────
    #[test]
    fn test_scroll_state_handle_event() {
        let mut s = ScrollState::new();
        s.viewport_height = 10;
        s.offset = 5;
        s.auto_scroll = true;

        // Keyboard: Up → scroll up
        let up = Event::Key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert!(s.handle_event(&up));
        assert_eq!(s.offset, 4);

        // Keyboard: PageDown → page scroll
        let pgdn = Event::Key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE));
        assert!(s.handle_event(&pgdn));
        assert_eq!(s.offset, 13); // 4 + 9

        // Keyboard: Home → scroll to top
        let home = Event::Key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE));
        assert!(s.handle_event(&home));
        assert_eq!(s.offset, 0);
        assert!(!s.auto_scroll);

        // Keyboard: End → enable auto-scroll
        let end_key = Event::Key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(s.handle_event(&end_key));
        assert!(s.auto_scroll);

        // Mouse: ScrollUp → scroll up
        let mouse_up = Event::Mouse(MouseEvent {
            kind: MouseEventKind::ScrollUp,
            column: 10,
            row: 5,
            modifiers: KeyModifiers::NONE,
        });
        // Reset offset first
        s.offset = 3;
        s.auto_scroll = true;
        assert!(s.handle_event(&mouse_up));
        assert_eq!(s.offset, 2);
        assert!(!s.auto_scroll);

        // Non-scroll event → not consumed
        let non_scroll = Event::Key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
        assert!(!s.handle_event(&non_scroll));

        // Key release → not consumed (only Press handled)
        let release_event = Event::Key(KeyEvent::new_with_kind(
            KeyCode::Down,
            KeyModifiers::NONE,
            KeyEventKind::Release,
        ));
        assert!(!s.handle_event(&release_event));
    }

    // ── test_scroll_state_clamp ──────────────────────────────────
    #[test]
    fn test_scroll_state_clamp() {
        let mut s = ScrollState::new();
        s.content_height = 50;
        s.viewport_height = 10;
        s.offset = 45; // offset > content - viewport (40)
        s.auto_scroll = false;

        s.clamp();
        assert_eq!(s.offset, 40, "offset should be clamped to max(0, content - viewport)");

        // auto_scroll = true → snap to bottom
        s.auto_scroll = true;
        s.offset = 5; // irrelevant, clamp snaps to bottom
        s.clamp();
        assert_eq!(s.offset, 40);

        // Content fits in viewport → offset should be 0
        s.content_height = 5;
        s.viewport_height = 10;
        s.auto_scroll = true;
        s.offset = 3;
        s.clamp();
        assert_eq!(s.offset, 0);
    }

    // ── test_scroll_state_home_end ───────────────────────────────
    #[test]
    fn test_scroll_state_home_end() {
        let mut s = ScrollState::new();
        s.offset = 100;
        s.auto_scroll = true;

        s.scroll_to_top();
        assert_eq!(s.offset, 0);
        assert!(!s.auto_scroll);

        s.scroll_to_bottom();
        assert!(s.auto_scroll);
    }

    // ── test_set_content_size ────────────────────────────────────
    #[test]
    fn test_set_content_size() {
        let mut s = ScrollState::new();
        s.viewport_height = 10;
        s.offset = 100;
        s.auto_scroll = false;

        s.set_content_size(50);
        assert_eq!(s.content_height, 50);
        assert_eq!(s.offset, 40); // clamped to 50 - 10 = 40

        // auto_scroll = true → snaps to bottom
        s.auto_scroll = true;
        s.set_content_size(30);
        assert_eq!(s.content_height, 30);
        assert_eq!(s.offset, 20); // 30 - 10 = 20, snapped to bottom

        // content smaller than viewport
        s.auto_scroll = false;
        s.set_content_size(5);
        assert_eq!(s.content_height, 5);
        assert_eq!(s.offset, 0); // 0 because content fits in viewport
    }

    // ── test_scroll_state_saturating ─────────────────────────────
    #[test]
    fn test_scroll_state_saturating() {
        let mut s = ScrollState::new();
        s.offset = 0;

        // Can't go below 0
        s.scroll_up();
        assert_eq!(s.offset, 0);

        // Saturates at u16::MAX
        s.offset = u16::MAX;
        s.scroll_down();
        assert_eq!(s.offset, u16::MAX);
    }
}
