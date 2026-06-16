// ── Status Bar Component — Modular Section Registration ──────────
// Ported from widgets/status_bar.rs to Component trait.
// Each status section registers independently with Fixed or Fill width.
// Colors provided by ThemeEngine::compute_style (via RenderContext).

#![allow(dead_code)]

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
};

use crate::tui::app::App;
use crate::tui::components::{Component, EventResult, RenderContext};

// ── SectionWidth ─────────────────────────────────────────────────

/// How a section's width is allocated in the status bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionWidth {
    /// Fixed width in cells.
    Fixed(u16),
    /// Takes remaining space proportionally after Fixed sections.
    Fill,
}

// ── StatusSection ────────────────────────────────────────────────

/// A registered status bar section.
///
/// Each section renders as `content` with `style`.
/// Set `content` to `None` to hide the section entirely.
#[derive(Debug, Clone)]
pub struct StatusSection {
    /// Unique identifier (e.g. "brand", "token_bar").
    pub id: String,
    /// Display text content. `None` = hidden.
    pub content: Option<String>,
    /// Style applied to this section's spans.
    pub style: Style,
    /// Width allocation strategy (used for truncation).
    pub width: SectionWidth,
}

// ── WaveState ─────────────────────────────────────────────────────

/// Tracks the current agentic loop wave execution state.
#[derive(Debug, Clone, Default)]
pub struct WaveState {
    /// Current wave index (1-based).
    pub current: u32,
    /// Total number of waves planned.
    pub total: u32,
    /// Tasks waiting to be dispatched.
    pub pending: usize,
    /// Tasks currently running.
    pub running: usize,
    /// Tasks completed successfully.
    pub done: usize,
    /// Tasks that failed.
    pub failed: usize,
}

// ── StatusBar ────────────────────────────────────────────────────

/// A modular status bar rendered as a single-line bar at the top.
///
/// Sections are registered via [`add_section`](Self::add_section) or
/// [`remove_section`](Self::remove_section) and rendered left-to-right,
/// concatenated with ` │ ` separators. If the total content exceeds the
/// given area width, sections are truncated from the right.
pub struct StatusBar {
    sections: Vec<StatusSection>,
    notification: Option<(String, Style)>,
    notification_ttl: u32,
}

impl StatusBar {
    /// Create a new empty status bar with no sections.
    pub fn new() -> Self {
        Self {
            sections: Vec::new(),
            notification: None,
            notification_ttl: 0,
        }
    }

    /// Create a status bar pre-populated with all default sections
    /// matching the original `widgets/status_bar.rs` functionality.
    pub fn with_default_sections() -> Self {
        let mut sb = Self::new();
        sb.add_section(StatusSection {
            id: "version".into(),
            content: None,
            style: Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            width: SectionWidth::Fixed(10),
        });
        sb.add_section(StatusSection {
            id: "model".into(),
            content: None,
            style: Style::default().fg(Color::White),
            width: SectionWidth::Fixed(24),
        });
        sb.add_section(StatusSection {
            id: "session".into(),
            content: None,
            style: Style::default().fg(Color::Cyan),
            width: SectionWidth::Fixed(16),
        });
        sb.add_section(StatusSection {
            id: "focus".into(),
            content: None,
            style: Style::default().fg(Color::Cyan),
            width: SectionWidth::Fixed(16),
        });
        sb.add_section(StatusSection {
            id: "context".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(18),
        });
        sb.add_section(StatusSection {
            id: "approvals".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(14),
        });
        sb.add_section(Self::permission_status_section());
        sb.add_section(Self::search_section());
        sb.add_section(Self::history_section());
        sb.add_section(StatusSection {
            id: "input_hint".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fill,
        });
        sb
    }

    // ── Default section factories ────────────────────────────────

    fn brand_section() -> StatusSection {
        StatusSection {
            id: "brand".into(),
            content: None,
            style: Style::default(),
            width: SectionWidth::Fixed(6),
        }
    }

    fn panel_model_status_section() -> StatusSection {
        StatusSection {
            id: "panel_model_status".into(),
            content: None,
            style: Style::default(),
            width: SectionWidth::Fill,
        }
    }

    fn token_bar_section() -> StatusSection {
        StatusSection {
            id: "token_bar".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(32),
        }
    }

    fn token_count_section() -> StatusSection {
        StatusSection {
            id: "token_count".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(20),
        }
    }

    fn turn_token_section() -> StatusSection {
        StatusSection {
            id: "turn_token".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(22),
        }
    }

    fn compaction_section() -> StatusSection {
        StatusSection {
            id: "compaction".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(10),
        }
    }

    fn cache_section() -> StatusSection {
        StatusSection {
            id: "cache".into(),
            content: None,
            style: Style::default().fg(Color::Green),
            width: SectionWidth::Fixed(12),
        }
    }

    fn search_section() -> StatusSection {
        StatusSection {
            id: "search".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(30),
        }
    }

    fn history_section() -> StatusSection {
        StatusSection {
            id: "history".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(10),
        }
    }

    fn task_section() -> StatusSection {
        StatusSection {
            id: "task".into(),
            content: None,
            style: Style::default().fg(Color::Magenta),
            width: SectionWidth::Fixed(28),
        }
    }

    fn lease_section() -> StatusSection {
        StatusSection {
            id: "lease".into(),
            content: None,
            style: Style::default().fg(Color::Cyan),
            width: SectionWidth::Fixed(30),
        }
    }

    fn wave_section() -> StatusSection {
        StatusSection {
            id: "wave".into(),
            content: None,
            style: Style::default().fg(Color::Cyan),
            width: SectionWidth::Fixed(28),
        }
    }

    // ── Reputation section ──────────────────────────────────────

    fn reputation_section() -> StatusSection {
        StatusSection {
            id: "reputation".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(8),
        }
    }

    // ── Task 12: Footer status sections ─────────────────────────

    fn mcp_status_section() -> StatusSection {
        StatusSection {
            id: "mcp_status".into(),
            content: None,
            style: Style::default().fg(Color::Green),
            width: SectionWidth::Fixed(16),
        }
    }

    fn lsp_status_section() -> StatusSection {
        StatusSection {
            id: "lsp_status".into(),
            content: None,
            style: Style::default().fg(Color::Green),
            width: SectionWidth::Fixed(16),
        }
    }

    fn permission_status_section() -> StatusSection {
        StatusSection {
            id: "permission_status".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(12),
        }
    }

    // ── Section management ───────────────────────────────────────

    /// Register a new section (appended to the right).
    pub fn add_section(&mut self, section: StatusSection) {
        self.sections.push(section);
    }

    /// Remove a section by its `id`. Returns the removed section if found.
    pub fn remove_section(&mut self, id: &str) -> Option<StatusSection> {
        let idx = self.sections.iter().position(|s| s.id == id)?;
        Some(self.sections.remove(idx))
    }

    /// Mutable access to a section by id (for updating content or style).
    pub fn section_mut(&mut self, id: &str) -> Option<&mut StatusSection> {
        self.sections.iter_mut().find(|s| s.id == id)
    }

    /// Iterate over all sections.
    pub fn sections(&self) -> &[StatusSection] {
        &self.sections
    }

    // ── Notifications ────────────────────────────────────────────

    /// Set a notification banner that overlays the status line.
    ///
    /// The notification is shown for `ttl` ticks then auto-dismisses.
    /// Call [`tick`](Self::tick) each frame to count down.
    pub fn set_notification(&mut self, text: impl Into<String>, style: Style, ttl: u32) {
        self.notification = Some((text.into(), style));
        self.notification_ttl = ttl;
    }

    /// Clear the notification immediately.
    pub fn clear_notification(&mut self) {
        self.notification = None;
        self.notification_ttl = 0;
    }

    /// Advance one tick (for notification auto-dismiss).
    /// Call this once per frame from the main loop.
    pub fn tick(&mut self) {
        if self.notification_ttl > 0 {
            self.notification_ttl -= 1;
            if self.notification_ttl == 0 {
                self.notification = None;
            }
        }
    }

    // ── Sync from App state ──────────────────────────────────────

    /// Populate each built-in section's `content` from `App` state.
    ///
    /// Call this before `render()` to reflect current App state.
    /// Only sections with recognized built-in IDs are populated;
    /// custom sections are left unchanged.
    pub fn sync_from_app(&mut self, app: &App) {
        for section in &mut self.sections {
            section.content = match section.id.as_str() {
                "version" => Some(format!("v{}", env!("CARGO_PKG_VERSION"))),
                "model" => {
                    let mode = if app.yolo_mode { "YOLO" } else { "STD" };
                    Some(format!("model:{} {mode}", preview(&app.model, 14)))
                }
                "context" => {
                    let pct = if app.context_window > 0 {
                        (app.token_count as f64 / app.context_window as f64 * 100.0).min(100.0)
                    } else {
                        0.0
                    };
                    Some(format!(
                        "ctx:{}/{} {:.0}%",
                        fmt_tokens(app.token_count),
                        fmt_tokens(app.context_window),
                        pct
                    ))
                }
                "approvals" => {
                    let count = app
                        .daemon_pending_approvals
                        .unwrap_or_default()
                        .max(app.permission_count as u64)
                        + u64::from(app.approval.is_some());
                    Some(format!("approvals:{count}"))
                }
                "search" => {
                    if app.search_active {
                        Some(format!("/{}", app.search_query))
                    } else if !app.search_matches.is_empty() {
                        Some(format!(
                            "/{} [{}/{}]",
                            app.search_query,
                            app.search_current + 1,
                            app.search_matches.len()
                        ))
                    } else {
                        None
                    }
                }
                "history" => app.history_idx.map(|hidx| format!("hist:{}", hidx + 1)),
                "permission_status" => Some(format!("perm:{}", app.permission_count)),
                "session" => Some(format!("session:{}", short_id(&app.session_id))),
                "input_hint" => {
                    Some("Enter send · Alt+Enter/Ctrl+J newline · Ctrl+B panels".into())
                }
                _ => None,
            };
        }
    }

    /// Build a status bar from App state in one call.
    pub fn from_app(app: &App) -> Self {
        let mut sb = Self::with_default_sections();
        sb.sync_from_app(app);
        sb
    }
}

impl Default for StatusBar {
    fn default() -> Self {
        Self::with_default_sections()
    }
}

// ── Component Trait ──────────────────────────────────────────────

impl Component for StatusBar {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let available = area.width;

        // Determine brand style from theme engine or skin config
        let brand_style = if let Some(engine) = ctx.theme_engine() {
            engine.compute_style("heading1")
        } else {
            Style::default()
                .fg(ctx.theme().accent_color())
                .add_modifier(Modifier::BOLD)
        };

        // Build spans from visible sections
        let mut spans: Vec<Span<'static>> = Vec::new();
        let mut first = true;
        let sep = " │ ";

        for section in &self.sections {
            if !status_section_visible_for_width(section.id.as_str(), available) {
                continue;
            }
            let text = match &section.content {
                Some(c) if !c.is_empty() => c.clone(),
                _ => continue,
            };

            // Override style for brand section
            let style = if section.id == "brand" {
                brand_style
            } else {
                section.style
            };

            let display = if first {
                first = false;
                text
            } else {
                format!("{sep}{text}")
            };

            // Truncate from right if too wide
            if display.len() as u16 > available {
                break;
            }

            spans.push(Span::styled(display, style));
        }

        // Render the status line
        let bg = ctx.theme().bg_color();
        let par = Paragraph::new(Line::from(spans)).style(Style::default().bg(bg));
        ctx.frame_mut().render_widget(par, area);

        // ── Notification overlay ──────────────────────────────────
        if let Some((ref text, ref style)) = self.notification {
            let note_bg = style.bg.unwrap_or(Color::Cyan);
            let note_line = Line::from(Span::styled(text.clone(), style.clone()));
            ctx.frame_mut().render_widget(
                Paragraph::new(note_line).style(Style::default().bg(note_bg)),
                area,
            );
        }
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        false
    }

    fn id(&self) -> &str {
        "status_bar"
    }
}

fn status_section_visible_for_width(id: &str, available: u16) -> bool {
    match id {
        "version" | "model" | "session" | "focus" => true,
        "context" => available >= 96,
        "approvals" | "permission_status" => available >= 120,
        "input_hint" => available >= 150,
        _ => available >= 132,
    }
}

// ── Helpers (ported from widgets/status_bar.rs) ──────────────────

/// Format a token count for display: 1.2M, 6.2K, 128, etc.
pub fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 10_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else if n >= 1_000 {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

fn short_id(value: &str) -> String {
    if value.chars().count() <= 10 {
        value.to_string()
    } else {
        value.chars().take(10).collect()
    }
}

fn preview(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let mut out: String = value.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Build a character-based progress bar: "████░░░░ 6.2K/128K (39%)"
pub fn token_bar(app: &App) -> Option<String> {
    let window = app.context_window;
    if window == 0 {
        return None;
    }
    let used = app.token_count;
    let pct = (used as f64 / window as f64 * 100.0).min(100.0);
    let bar_width: i32 = 12;
    let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
    let empty = (bar_width as usize).saturating_sub(filled);

    let mut bar = String::with_capacity(bar_width as usize + 32);
    for _ in 0..filled {
        bar.push('█');
    }
    for _ in 0..empty {
        bar.push('░');
    }

    Some(format!(
        "{} {}/{} ({:.0}%)",
        bar,
        fmt_tokens(used),
        fmt_tokens(window),
        pct
    ))
}

// ── Tests ────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::RenderContext;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    // ── Section management ───────────────────────────────────────

    #[test]
    fn add_and_remove_section() {
        let mut bar = StatusBar::new();
        assert_eq!(bar.sections().len(), 0);

        bar.add_section(StatusSection {
            id: "test".into(),
            content: Some("hello".into()),
            style: Style::default(),
            width: SectionWidth::Fixed(10),
        });
        assert_eq!(bar.sections().len(), 1);

        let removed = bar.remove_section("test");
        assert!(removed.is_some());
        assert_eq!(bar.sections().len(), 0);
    }

    #[test]
    fn remove_nonexistent_section_returns_none() {
        let mut bar = StatusBar::new();
        assert!(bar.remove_section("nope").is_none());
    }

    // ── Default sections ─────────────────────────────────────────

    #[test]
    fn with_default_sections_has_all_parts() {
        let bar = StatusBar::with_default_sections();
        let ids: Vec<&str> = bar.sections().iter().map(|s| s.id.as_str()).collect();
        assert!(ids.contains(&"version"));
        assert!(ids.contains(&"model"));
        assert!(ids.contains(&"context"));
        assert!(ids.contains(&"approvals"));
        assert!(ids.contains(&"permission_status"));
        assert!(ids.contains(&"session"));
        assert!(ids.contains(&"search"));
        assert!(ids.contains(&"history"));
        assert!(ids.contains(&"input_hint"));
    }

    // ── Notification ─────────────────────────────────────────────

    #[test]
    fn notification_set_and_clear() {
        let mut bar = StatusBar::new();
        assert!(bar.notification.is_none());

        bar.set_notification(
            "test notice",
            Style::default().fg(Color::Black).bg(Color::Cyan),
            10,
        );
        assert!(bar.notification.is_some());
        assert_eq!(bar.notification.as_ref().unwrap().0, "test notice");
        assert_eq!(bar.notification_ttl, 10);

        bar.clear_notification();
        assert!(bar.notification.is_none());
        assert_eq!(bar.notification_ttl, 0);
    }

    #[test]
    fn notification_auto_dismiss_via_tick() {
        let mut bar = StatusBar::new();
        bar.set_notification("dismiss me", Style::default(), 3);

        assert!(bar.notification.is_some());
        bar.tick();
        assert!(bar.notification.is_some());
        bar.tick();
        assert!(bar.notification.is_some());
        bar.tick();
        assert!(bar.notification.is_none());
    }

    // ── sync_from_app ────────────────────────────────────────────

    #[test]
    fn sync_from_app_populates_model_section() {
        let app = App::new("test-model", "test-session");
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let version = bar.section_mut("version").unwrap();
        assert_eq!(
            version.content.as_deref(),
            Some(concat!("v", env!("CARGO_PKG_VERSION")))
        );
        let model = bar.section_mut("model").unwrap();
        assert_eq!(model.content.as_deref(), Some("model:test-model STD"));
    }

    #[test]
    fn sync_from_app_populates_context_section() {
        let app = App::new("claude-sonnet-4", "test-session");
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let section = bar.section_mut("context").unwrap();
        let content = section.content.as_deref().unwrap();
        assert!(content.contains("ctx:"));
    }

    #[test]
    fn sync_from_app_shows_yolo_mode() {
        let mut app = App::new("claude-sonnet-4", "test-session");
        app.yolo_mode = true;
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let section = bar.section_mut("model").unwrap();
        let content = section.content.as_ref().unwrap();
        assert!(content.contains("YOLO"));
    }

    #[test]
    fn sync_from_app_shows_approval_count() {
        let mut app = App::new("claude-sonnet-4", "test-session");
        app.daemon_pending_approvals = Some(2);
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let section = bar.section_mut("approvals").unwrap();
        let content = section.content.as_ref().unwrap();
        assert_eq!(content, "approvals:2");
    }

    #[test]
    fn sync_from_app_shows_session_id() {
        let app = App::new("claude-sonnet-4", "test-session-abcdef");
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let section = bar.section_mut("session").unwrap();
        let content = section.content.as_ref().unwrap();
        assert!(content.contains("test-sessi"));
    }

    // ── Render tests ─────────────────────────────────────────────

    #[test]
    fn render_shows_section_content() {
        let mut bar = StatusBar::new();
        bar.add_section(StatusSection {
            id: "left".into(),
            content: Some("Cowd".into()),
            style: Style::default(),
            width: SectionWidth::Fixed(6),
        });
        bar.add_section(StatusSection {
            id: "right".into(),
            content: Some("Ready".into()),
            style: Style::default(),
            width: SectionWidth::Fill,
        });

        let mut terminal = MockTerminal::new(40, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 40, 1));
        });

        terminal.assert_line_contains("Cowd");
        terminal.assert_line_contains("Ready");
    }

    #[test]
    fn render_default_status_keeps_version_and_model_on_narrow_width() {
        let app = App::new("deepseek-v4-pro", "session-status-narrow");
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);

        let mut terminal = MockTerminal::new(88, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 88, 1));
        });

        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains(concat!("v", env!("CARGO_PKG_VERSION"))),
            "version should stay visible on narrow status bars: {joined}"
        );
        assert!(
            joined.contains("model:deepseek"),
            "model should stay visible on narrow status bars: {joined}"
        );
    }

    #[test]
    fn render_default_status_suppresses_low_priority_sections_on_medium_width() {
        let app = App::new("deepseek-v4-pro", "session-status-medium");
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);
        if let Some(section) = bar.section_mut("focus") {
            section.content = Some("focus:chat".into());
        }

        let mut terminal = MockTerminal::new(110, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 110, 1));
        });

        let joined = terminal.buffer_lines().join("\n");
        assert!(joined.contains("focus:chat"));
        assert!(joined.contains("ctx:"));
        assert!(!joined.contains("approvals:"));
        assert!(!joined.contains("perm:"));
        assert!(!joined.contains("Enter send"));
    }

    #[test]
    fn render_hidden_sections_skipped() {
        let mut bar = StatusBar::new();
        bar.add_section(StatusSection {
            id: "visible".into(),
            content: Some("Hello".into()),
            style: Style::default(),
            width: SectionWidth::Fill,
        });
        bar.add_section(StatusSection {
            id: "hidden".into(),
            content: None,
            style: Style::default(),
            width: SectionWidth::Fixed(10),
        });

        let mut terminal = MockTerminal::new(40, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 40, 1));
        });

        terminal.assert_line_contains("Hello");
        let lines = terminal.buffer_lines();
        let any_hidden = lines.iter().any(|l| l.contains("hidden"));
        assert!(!any_hidden, "hidden sections should not render");
    }

    // ── Section management extras ────────────────────────────────

    #[test]
    fn section_mut_updates_content() {
        let mut bar = StatusBar::new();
        bar.add_section(StatusSection {
            id: "test".into(),
            content: Some("old".into()),
            style: Style::default(),
            width: SectionWidth::Fixed(5),
        });

        if let Some(s) = bar.section_mut("test") {
            s.content = Some("new".into());
        }
        assert_eq!(bar.sections()[0].content.as_deref(), Some("new"));
    }

    // ── fmt_tokens ───────────────────────────────────────────────

    #[test]
    fn fmt_tokens_values() {
        assert_eq!(fmt_tokens(0), "0");
        assert_eq!(fmt_tokens(999), "999");
        assert_eq!(fmt_tokens(1_000), "1k");
        assert_eq!(fmt_tokens(1_500), "1k");
        assert_eq!(fmt_tokens(9_999), "9k");
        assert_eq!(fmt_tokens(10_000), "10.0k");
        assert_eq!(fmt_tokens(12_345), "12.3k");
        assert_eq!(fmt_tokens(999_999), "1000.0k");
        assert_eq!(fmt_tokens(1_000_000), "1.0M");
        assert_eq!(fmt_tokens(1_234_567), "1.2M");
    }

    // ── Notification render ──────────────────────────────────────

    #[test]
    fn permission_status_shows() {
        let mut app = App::new("test-model", "test-session");
        app.permission_count = 2;
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);

        let section = bar.section_mut("permission_status").unwrap();
        let content = section.content.as_deref().unwrap();
        assert_eq!(content, "perm:2");
    }

    #[test]
    fn input_hint_shows_core_interactions() {
        let app = App::new("test-model", "test-session");
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);

        let section = bar.section_mut("input_hint").unwrap();
        let content = section.content.as_deref().unwrap();
        assert!(content.contains("Enter send"));
        assert!(content.contains("Ctrl+B"));
    }

    #[test]
    fn with_default_sections_includes_footer_sections() {
        let bar = StatusBar::with_default_sections();
        let ids: Vec<&str> = bar.sections().iter().map(|s| s.id.as_str()).collect();
        assert!(
            ids.contains(&"permission_status"),
            "Should have permission_status section"
        );
        assert!(ids.contains(&"model"), "Should have model section");
        assert!(ids.contains(&"context"), "Should have context section");
        assert!(ids.contains(&"approvals"), "Should have approvals section");
    }

    #[test]
    fn render_notification_overlay() {
        let mut bar = StatusBar::new();
        bar.add_section(StatusSection {
            id: "main".into(),
            content: Some("status content".into()),
            style: Style::default(),
            width: SectionWidth::Fill,
        });
        bar.set_notification(
            "NOTICE",
            Style::default().fg(Color::Black).bg(Color::Cyan),
            5,
        );

        let mut terminal = MockTerminal::new(40, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 40, 1));
        });

        // Notification text should be visible
        terminal.assert_line_contains("NOTICE");
    }

    #[test]
    fn token_bar_returns_some_when_window_nonzero() {
        use crate::tui::app::App;
        let mut app = App::new("test", "test-session");
        app.context_window = 200_000;
        app.token_count = 50_000;
        let bar = token_bar(&app);
        assert!(
            bar.is_some(),
            "token_bar should return Some when context_window > 0"
        );
        assert!(bar.unwrap().contains("25%"), "should show 25% usage");
    }

    #[test]
    fn token_bar_returns_none_when_window_zero() {
        use crate::tui::app::App;
        let app = App::new("test", "test-session");
        assert!(
            token_bar(&app).is_none(),
            "token_bar should return None when context_window == 0"
        );
    }
}
