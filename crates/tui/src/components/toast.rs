// Task: Toast notification system — multi-variant overlay with auto-dismiss and stacking.
// Independent of StatusBar; renders as top-right overlay. DialogManager gets priority.
#![allow(dead_code)]

use std::collections::VecDeque;

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

use crate::components::base::terminal_len;
use crate::components::RenderContext;

// ─── Toast Variant ────────────────────────────────────────────────────

/// The visual variant of a toast notification.
///
/// Each variant maps to a distinct border color for quick visual recognition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastVariant {
    /// Informational notification — cyan border.
    Info,
    /// Successful operation — green border.
    Success,
    /// Warning condition — yellow border.
    Warning,
    /// Error/failure notification — red border.
    Error,
}

impl ToastVariant {
    /// Return the border/accent color for this variant.
    #[must_use]
    pub fn border_color(&self) -> Color {
        match self {
            Self::Info => Color::Cyan,
            Self::Success => Color::Green,
            Self::Warning => Color::Yellow,
            Self::Error => Color::Red,
        }
    }
}

// ─── Toast ────────────────────────────────────────────────────────────

/// A single toast notification with a variant, optional title, message,
/// and auto-dismiss timer tracked in display ticks (~100ms per tick).
#[derive(Debug, Clone)]
pub struct Toast {
    /// Visual variant (determines border color).
    pub variant: ToastVariant,
    /// Optional bold title line shown above the message.
    pub title: Option<String>,
    /// The main body text of the notification.
    pub message: String,
    /// Original duration in milliseconds (for reference).
    pub duration_ms: u64,
    /// Remaining ticks until auto-dismiss (~100ms per tick).
    pub remaining_ticks: u64,
}

impl Toast {
    /// Create a new toast notification.
    ///
    /// Duration is converted to ticks at ~10 fps (duration_ms / 100).
    /// Minimum duration is 1 tick (100ms).
    #[must_use]
    pub fn new(
        variant: ToastVariant,
        title: Option<String>,
        message: String,
        duration_ms: u64,
    ) -> Self {
        let remaining_ticks = (duration_ms / 100).max(1);
        Self {
            variant,
            title,
            message,
            duration_ms,
            remaining_ticks,
        }
    }
}

// ─── ToastManager ─────────────────────────────────────────────────────

/// Stack-based toast notification manager.
///
/// Toasts are rendered at the **top-right** of the given area, stacked
/// vertically with a one-line gap between them. Each toast auto-dismisses
/// after its duration expires (call `tick()` from the main render loop).
///
/// # Priority
///
/// Dialogs (`DialogManager`) render ON TOP of toasts. Call `ToastManager::render`
/// before `DialogManager::render` in the render pipeline (toasts first, dialogs on top).
///
/// # Limits
///
/// Maximum simultaneous toasts defaults to 5. Older toasts are evicted
/// (FIFO) when the limit is exceeded.
#[derive(Debug, Clone)]
pub struct ToastManager {
    toasts: VecDeque<Toast>,
    max_toasts: usize,
}

impl ToastManager {
    /// Create a new empty toast manager with the default max (5 toasts).
    #[must_use]
    pub fn new() -> Self {
        Self {
            toasts: VecDeque::new(),
            max_toasts: 5,
        }
    }

    /// Create a toast manager with a custom maximum simultaneous toasts.
    #[must_use]
    pub fn with_max(max_toasts: usize) -> Self {
        Self {
            toasts: VecDeque::new(),
            max_toasts,
        }
    }

    /// Push a new toast notification.
    ///
    /// If the number of active toasts equals `max_toasts`, the oldest
    /// toast is removed (FIFO eviction).
    pub fn push(
        &mut self,
        variant: ToastVariant,
        title: Option<String>,
        message: String,
        duration_ms: u64,
    ) {
        if self.toasts.len() >= self.max_toasts {
            self.toasts.pop_front();
        }
        self.toasts
            .push_back(Toast::new(variant, title, message, duration_ms));
    }

    /// Advance time by one tick (~100ms).
    ///
    /// Decrements `remaining_ticks` for every active toast and removes
    /// any that have expired. Call this once per render loop iteration.
    pub fn tick(&mut self) {
        // Decrement remaining ticks
        for toast in &mut self.toasts {
            toast.remaining_ticks = toast.remaining_ticks.saturating_sub(1);
        }
        // Remove expired toasts
        self.toasts.retain(|t| t.remaining_ticks > 0);
    }

    /// Returns `true` if there are no active toasts.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.toasts.is_empty()
    }

    /// Returns the number of active toasts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.toasts.len()
    }

    // ─── Rendering ─────────────────────────────────────────────────

    /// Render all active toasts at the top-right of `area`.
    ///
    /// Each toast is rendered as a bordered block with the variant's
    /// border color. Toasts are stacked vertically with a one-line gap.
    /// Maximum width is `min(60, area.width - 4)`.
    ///
    /// # Arguments
    /// * `ctx`  — mutable render context providing access to the frame and theme
    /// * `area` — the area to render into (toasts are positioned at top-right)
    pub fn render(&self, ctx: &mut RenderContext, area: Rect) {
        if self.toasts.is_empty() {
            return;
        }

        let frame = ctx.frame_mut();

        // A stack of desktop-sized notices can consume an entire narrow
        // terminal and cover the newest assistant output. Compact layouts show
        // only the latest notification in a bounded banner; normal widths keep
        // the full notification stack.
        if area.width < 60 || area.height < 16 {
            let Some(toast) = self.toasts.back() else {
                return;
            };
            let width = area.width.saturating_sub(2).max(1);
            let height = area.height.min(4).max(1);
            let rect = Rect::new(
                area.x.saturating_add(1),
                area.y
                    .saturating_add(1)
                    .min(area.bottom().saturating_sub(height)),
                width,
                height,
            );
            frame.render_widget(Clear, rect);
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(toast.variant.border_color()))
                .title(toast.title.as_deref().unwrap_or(""));
            frame.render_widget(
                Paragraph::new(toast.message.as_str())
                    .block(block)
                    .wrap(Wrap { trim: true }),
                rect,
            );
            return;
        }

        // Max toast width: min(60, area.width - 4) for right-side padding
        let max_w = 60u16.min(area.width.saturating_sub(4));

        // Starting position: 1 row from top, right-aligned
        let mut y = area.y + 1;
        let x = area.x + area.width.saturating_sub(max_w + 1); // 1-cell right margin

        for toast in &self.toasts {
            // Calculate toast height:
            // border(2) + title_line(1 if title exists else 0) + message + bottom spacing(1)
            let title_h = if toast.title.is_some() { 1u16 } else { 0u16 };
            let msg_lines = terminal_len(toast.message.lines().count().max(1));
            let toast_h = 2u16
                .saturating_add(title_h)
                .saturating_add(1)
                .saturating_add(msg_lines)
                .saturating_add(1)
                .min(area.height.saturating_sub(y.saturating_sub(area.y)));

            let toast_rect = Rect::new(x, y, max_w, toast_h);

            // Clear the area before rendering (prevents visual artifacts)
            frame.render_widget(Clear, toast_rect);

            // Build the bordered block with variant-specific border color
            let border_color = toast.variant.border_color();
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border_color))
                .title(toast.title.as_deref().unwrap_or(""));

            let p = Paragraph::new(toast.message.as_str())
                .block(block)
                .wrap(Wrap { trim: true });

            frame.render_widget(p, toast_rect);

            // Advance Y for next toast: height + 1 gap line
            y = y.saturating_add(toast_h).saturating_add(1);

            // Safety: stop if we'd overflow the area
            if y >= area.y + area.height {
                break;
            }
        }
    }
}

impl Default for ToastManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::RenderContext;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;
    use ratatui::Frame;

    // ── Variant display ────────────────────────────────────────────

    #[test]
    fn toast_shows_info_variant() {
        let mut mgr = ToastManager::new();
        mgr.push(
            ToastVariant::Info,
            Some("Info Title".into()),
            "Info message content".into(),
            3000,
        );

        assert!(!mgr.is_empty());
        assert_eq!(mgr.len(), 1);

        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            mgr.render(&mut ctx, area);
        });

        terminal.assert_line_contains("Info Title");
        terminal.assert_line_contains("Info message content");
    }

    // ── Auto-dismiss ───────────────────────────────────────────────

    #[test]
    fn toast_auto_dismisses() {
        let mut mgr = ToastManager::new();
        // 100ms → 1 tick
        mgr.push(ToastVariant::Info, None, "Quick toast".into(), 100);

        assert!(!mgr.is_empty());

        // After one tick, remaining_ticks hits 0 → removed
        mgr.tick();
        assert!(
            mgr.is_empty(),
            "Toast should be dismissed after its ticks expire"
        );
    }

    // ── Stacking ───────────────────────────────────────────────────

    #[test]
    fn toast_stacks_multiple() {
        let mut mgr = ToastManager::new();
        mgr.push(
            ToastVariant::Info,
            Some("First".into()),
            "First message".into(),
            5000,
        );
        mgr.push(
            ToastVariant::Success,
            Some("Second".into()),
            "Second message".into(),
            5000,
        );
        mgr.push(
            ToastVariant::Warning,
            Some("Third".into()),
            "Third message".into(),
            5000,
        );

        assert_eq!(mgr.len(), 3);

        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            mgr.render(&mut ctx, area);
        });

        // All three toasts should be visible
        terminal.assert_line_contains("First");
        terminal.assert_line_contains("Second");
        terminal.assert_line_contains("Third");
        terminal.assert_line_contains("First message");
        terminal.assert_line_contains("Second message");
        terminal.assert_line_contains("Third message");
    }

    #[test]
    fn narrow_terminal_shows_only_latest_toast_in_bounded_banner() {
        let mut mgr = ToastManager::new();
        mgr.push(
            ToastVariant::Info,
            Some("First".into()),
            "First message that must not cover the transcript".into(),
            5000,
        );
        mgr.push(
            ToastVariant::Warning,
            Some("Latest".into()),
            "Latest compact notice".into(),
            5000,
        );

        let mut terminal = MockTerminal::new(40, 24);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            mgr.render(&mut ctx, area);
        });

        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(joined.contains("Latest"));
        assert!(joined.contains("Latest compact notice"));
        assert!(!joined.contains("First message"));
        assert!(
            lines.iter().filter(|line| !line.trim().is_empty()).count() <= 4,
            "compact toast must not consume the narrow transcript viewport: {joined}"
        );
    }

    // ── Error border ───────────────────────────────────────────────

    #[test]
    fn toast_error_red_border() {
        let mut mgr = ToastManager::new();
        mgr.push(
            ToastVariant::Error,
            Some("Error!".into()),
            "Something went wrong".into(),
            5000,
        );

        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            mgr.render(&mut ctx, area);
        });

        terminal.assert_line_contains("Error!");
        terminal.assert_line_contains("Something went wrong");

        // Verify a border is rendered (check for box-drawing characters)
        let lines = terminal.buffer_lines();
        let has_border = lines
            .iter()
            .any(|l| l.contains('┌') || l.contains('┐') || l.contains('─'));
        assert!(has_border, "Error toast should render a border");
    }

    // ── Max limit ──────────────────────────────────────────────────

    #[test]
    fn max_toasts_limits_stack() {
        let mut mgr = ToastManager::with_max(3);
        mgr.push(ToastVariant::Info, Some("1".into()), "Msg 1".into(), 5000);
        mgr.push(ToastVariant::Info, Some("2".into()), "Msg 2".into(), 5000);
        mgr.push(ToastVariant::Info, Some("3".into()), "Msg 3".into(), 5000);
        // This push should evict the oldest (1)
        mgr.push(ToastVariant::Info, Some("4".into()), "Msg 4".into(), 5000);

        assert_eq!(mgr.len(), 3);
        let titles: Vec<Option<String>> = mgr.toasts.iter().map(|t| t.title.clone()).collect();
        assert_eq!(titles[0].as_deref(), Some("2"));
        assert_eq!(titles[1].as_deref(), Some("3"));
        assert_eq!(titles[2].as_deref(), Some("4"));
    }

    // ── Empty state ────────────────────────────────────────────────

    #[test]
    fn empty_toast_manager_renders_nothing() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        let mgr = ToastManager::new();

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            mgr.render(&mut ctx, area);
        });

        let lines = terminal.buffer_lines();
        assert!(
            lines.iter().all(|l| l.is_empty()),
            "Empty toast manager should not render anything"
        );
    }

    // ── Tick progression ───────────────────────────────────────────

    #[test]
    fn tick_removes_expired_in_order() {
        let mut mgr = ToastManager::new();
        mgr.push(ToastVariant::Info, None, "Toast A".into(), 200); // 2 ticks
        mgr.push(ToastVariant::Info, None, "Toast B".into(), 100); // 1 tick

        // Tick 1: B expires (0 remaining), A stays at 1
        mgr.tick();
        assert_eq!(mgr.len(), 1);

        // Tick 2: A expires (0 remaining)
        mgr.tick();
        assert!(mgr.is_empty());
    }
}
