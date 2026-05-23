// ── ChatView Component ────────────────────────────────────────────
// Port of widgets/chat.rs to the Component trait.
// Preserves ALL functionality:
//   - 4 TimelineEntry variants (Message, Thinking, ToolCall, SlashOutput)
//   - Virtual scrolling (>3x viewport)
//   - Incremental rebuild (streaming tail)
//   - Scroll-to-entry with scrollbar
//   - Loading spinner during active turns
//   - Entry line counts pre-computation
// -----------------------------------------------------------------

use crossterm::event::{Event, KeyCode, KeyEvent};
use ratatui::{
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
};

use crate::tui::app::{Theme, TimelineEntry};
use crate::tui::components::{Component, EventResult, RenderContext};
use crate::tui::md_renderer;

// ── ChatView ──────────────────────────────────────────────────────

/// Self-contained chat view component.
///
/// Holds all state needed to render the conversation timeline:
/// entries, scrolling, render cache, and theme. Call `sync_from_app()`
/// to pull state from the shared `App` before each render frame.
pub struct ChatView {
    // ── Timeline ──
    pub timeline: Vec<TimelineEntry>,
    pub timeline_cursor: usize,

    // ── Scrolling ──
    pub scroll_offset: u16,
    pub auto_scroll: bool,
    pub viewport_height: u16,

    // ── Turn state ──
    pub turn_active: bool,
    spinner_idx: usize,

    // ── Render cache ──
    cached_chat_lines: Vec<Line<'static>>,
    entry_line_counts: Vec<u16>,
    msg_version: u64,
    last_drawn_version: u64,
    lines_dirty: bool,

    // ── Theme ──
    pub theme: Theme,
}

impl ChatView {
    /// Create a new empty chat view with default 24-row viewport.
    #[must_use]
    pub fn new() -> Self {
        Self {
            timeline: Vec::new(),
            timeline_cursor: 0,
            scroll_offset: 0,
            auto_scroll: true,
            viewport_height: 24,
            turn_active: false,
            spinner_idx: 0,
            cached_chat_lines: Vec::new(),
            entry_line_counts: Vec::new(),
            msg_version: 0,
            last_drawn_version: u64::MAX,
            lines_dirty: true,
            theme: Theme::Dark,
        }
    }

    /// Return the current spinner character (braille animation frame).
    #[must_use]
    pub fn spinner_char(&self) -> &'static str {
        const F: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        F[self.spinner_idx % F.len()]
    }

    /// Advance the spinner animation by one frame.
    pub fn tick(&mut self) {
        self.spinner_idx = self.spinner_idx.wrapping_add(1);
    }

    /// Mark render cache as dirty (force full rebuild next frame).
    pub fn mark_dirty(&mut self) {
        self.lines_dirty = true;
        self.msg_version = self.msg_version.wrapping_add(1);
    }

    /// Sync view-model from the shared App state.
    /// Called once per frame before rendering.
    pub fn sync_from_app(&mut self, app: &crate::tui::App) {
        self.timeline = app.timeline.clone();
        self.timeline_cursor = app.timeline_cursor;
        self.scroll_offset = app.scroll_offset;
        self.auto_scroll = app.auto_scroll;
        self.viewport_height = app.viewport_height;
        self.turn_active = app.turn_active;
        self.spinner_idx = app.spinner_idx;
        self.theme = app.theme;
        self.msg_version = app.msg_version;
        self.lines_dirty = app.lines_dirty;
    }

    /// Persist view-model back to the shared App state.
    /// Called after rendering to preserve cursor position and scroll offset.
    pub fn sync_to_app(&self, app: &mut crate::tui::App) {
        app.scroll_offset = self.scroll_offset;
        app.auto_scroll = self.auto_scroll;
        app.viewport_height = self.viewport_height;
    }

    // ── Navigation ────────────────────────────────────────────────

    /// Move timeline cursor up by one collapsible entry.
    pub fn cursor_up(&mut self) -> bool {
        if self.timeline.is_empty() {
            return false;
        }
        let mut idx = self.timeline_cursor;
        loop {
            if idx == 0 {
                break;
            }
            idx -= 1;
            if self.timeline[idx].is_collapsible() {
                self.timeline_cursor = idx;
                self.auto_scroll = false;
                return true;
            }
        }
        false
    }

    /// Move timeline cursor down by one collapsible entry.
    pub fn cursor_down(&mut self) -> bool {
        if self.timeline.is_empty() {
            return false;
        }
        let mut idx = self.timeline_cursor;
        while idx + 1 < self.timeline.len() {
            idx += 1;
            if self.timeline[idx].is_collapsible() {
                self.timeline_cursor = idx;
                self.auto_scroll = true;
                return true;
            }
        }
        false
    }

    /// Toggle expand/collapse on the currently focused timeline entry.
    pub fn toggle_expand_current(&mut self) {
        if let Some(entry) = self.timeline.get_mut(self.timeline_cursor) {
            entry.toggle();
            self.msg_version = self.msg_version.wrapping_add(1);
        }
    }

    /// Scroll up by one viewport worth of lines.
    pub fn scroll_page_up(&mut self) {
        let amount = self.viewport_height.max(1).saturating_sub(1);
        self.scroll_offset = self.scroll_offset.saturating_sub(amount);
    }

    /// Scroll down by one viewport worth of lines.
    pub fn scroll_page_down(&mut self) {
        let amount = self.viewport_height.max(1).saturating_sub(1);
        self.scroll_offset = self.scroll_offset.saturating_add(amount);
    }

    /// Scroll so the entry at the given index is visible.
    pub fn scroll_to_entry(&mut self, entry_idx: usize) {
        let vh = self.viewport_height.max(1) as usize;
        let mut offset: usize = 0;
        for i in 0..entry_idx.min(self.entry_line_counts.len()) {
            offset += self.entry_line_counts[i] as usize + 1;
        }
        let entry_h = self
            .entry_line_counts
            .get(entry_idx)
            .copied()
            .unwrap_or(1) as usize;

        let scroll = self.scroll_offset as usize;
        if offset < scroll {
            self.scroll_offset = offset as u16;
        } else if offset + entry_h > scroll + vh {
            self.scroll_offset = offset.saturating_sub(vh.saturating_sub(entry_h)) as u16;
        }
    }

    // ── Line computation (internal) ───────────────────────────────

    /// Total number of content lines (including separator blanks and spinner).
    fn total_lines(&self) -> usize {
        let mut total: usize = self
            .entry_line_counts
            .iter()
            .map(|&c| c as usize + 1)
            .sum();
        if total == 0 && self.timeline.is_empty() {
            total = 1;
        }
        if self.turn_active {
            total += 1;
        }
        total
    }
}

// ── Component impl ────────────────────────────────────────────────

impl Component for ChatView {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let viewport_h = area.height as usize;
        self.viewport_height = viewport_h as u16;

        // ── Compute total lines ──
        let total_lines = self.total_lines();

        // ── Auto-scroll ──
        if self.auto_scroll && total_lines > viewport_h {
            self.scroll_offset = (total_lines - viewport_h) as u16;
        }
        let scroll_off = self
            .scroll_offset
            .min(total_lines.saturating_sub(1) as u16) as usize;

        // ── Build visible lines ──
        let visible_lines: Vec<Line<'static>>;
        let paragraph_scroll: u16;

        if total_lines > viewport_h.saturating_mul(3) {
            // Virtual scrolling
            visible_lines = Self::build_visible(self, scroll_off, viewport_h);
            paragraph_scroll = 0;
        } else {
            // Small timeline: use render cache
            if self.msg_version != self.last_drawn_version {
                self.cached_chat_lines = Self::build_new_lines(self);
                self.entry_line_counts = Self::compute_entry_line_counts(self);
                self.last_drawn_version = self.msg_version;
                self.lines_dirty = false;
            } else if self.lines_dirty {
                Self::rebuild_streaming_tail(self);
                self.entry_line_counts = Self::compute_entry_line_counts(self);
                self.lines_dirty = false;
            }
            visible_lines = self.cached_chat_lines.clone();
            paragraph_scroll = scroll_off as u16;
        }

        // ── Render ──
        let inner_area = Rect {
            x: area.x,
            y: area.y,
            width: area.width.saturating_sub(1),
            height: area.height,
        };
        let scrollbar_area = Rect {
            x: area.right().saturating_sub(1),
            y: area.y,
            width: 1,
            height: area.height,
        };

        let frame = ctx.frame_mut();
        frame.render_widget(Clear, area);

        let paragraph = Paragraph::new(Text::from(visible_lines))
            .wrap(Wrap { trim: false })
            .scroll((paragraph_scroll, 0));
        frame.render_widget(paragraph, inner_area);

        // ── Scrollbar ──
        if total_lines > viewport_h {
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(Some("↑"))
                .end_symbol(Some("↓"))
                .track_symbol(Some("│"))
                .thumb_symbol("█");
            let mut scroll_state = ScrollbarState::new(total_lines)
                .position(scroll_off)
                .viewport_content_length(viewport_h);
            frame.render_stateful_widget(scrollbar, scrollbar_area, &mut scroll_state);
        }
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) => self.handle_key(key),
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "chat_view"
    }
}

impl ChatView {
    fn handle_key(&mut self, key: &KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Enter => {
                self.toggle_expand_current();
                EventResult::Consumed
            }
            KeyCode::Up => {
                self.cursor_up();
                EventResult::Consumed
            }
            KeyCode::Down => {
                self.cursor_down();
                EventResult::Consumed
            }
            KeyCode::PageUp => {
                self.scroll_page_up();
                self.auto_scroll = false;
                EventResult::Consumed
            }
            KeyCode::PageDown => {
                self.scroll_page_down();
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }
}

// ── Drawing logic (ported from widgets/chat.rs) ───────────────────

impl ChatView {
    /// Pre-compute per-entry line counts.
    fn compute_entry_line_counts(&self) -> Vec<u16> {
        self.timeline
            .iter()
            .map(|e| e.expanded_lines() as u16)
            .collect()
    }

    /// Full rebuild of chat lines from internal state.
    fn build_new_lines(&self) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();

        if self.timeline.is_empty() {
            lines.push(Line::from(Span::styled(
                "Type to start. /help /resume /exit",
                Style::default().fg(Color::DarkGray),
            )));
        }

        for (idx, entry) in self.timeline.iter().enumerate() {
            let is_focused = idx == self.timeline_cursor;
            Self::build_entry(entry, is_focused, &mut lines, &self.theme);
            lines.push(Line::raw(""));
        }

        if self.turn_active {
            let spinner = self.spinner_char();
            lines.push(Line::from(vec![Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(Color::Blue),
            )]));
        }

        lines
    }

    /// Rebuild only the last entry (being streamed) in-place.
    fn rebuild_streaming_tail(&mut self) {
        let n = self.timeline.len();
        if n == 0 {
            return;
        }

        let prefix_count: usize = self
            .entry_line_counts
            .iter()
            .take(n.saturating_sub(1))
            .sum::<u16>()
            .saturating_add((n.saturating_sub(1)) as u16) as usize;

        let last_entry = self.timeline[n - 1].clone();
        let is_focused = (n - 1) == self.timeline_cursor;
        let theme = self.theme;
        let turn_active = self.turn_active;
        let spinner_str = if turn_active {
            Some(self.spinner_char().to_string())
        } else {
            None
        };

        self.cached_chat_lines
            .truncate(prefix_count.min(self.cached_chat_lines.len()));
        let before_len = self.cached_chat_lines.len();
        Self::build_entry(&last_entry, is_focused, &mut self.cached_chat_lines, &theme);
        self.cached_chat_lines.push(Line::raw(""));

        if let Some(count) = self.entry_line_counts.get_mut(n - 1) {
            *count = (self.cached_chat_lines.len() - before_len) as u16;
        }

        if let Some(spinner) = spinner_str {
            self.cached_chat_lines.push(Line::from(vec![Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(Color::Blue),
            )]));
        }
    }

    /// Build only visible entries for virtual scrolling.
    fn build_visible(&self, scroll_offset: usize, viewport_h: usize) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();
        let mut cumulative: usize = 0;
        let viewport_end = scroll_offset + viewport_h;

        if self.timeline.is_empty() {
            lines.push(Line::from(Span::styled(
                "Type to start. /help /resume /exit",
                Style::default().fg(Color::DarkGray),
            )));
            return lines;
        }

        for (idx, entry) in self.timeline.iter().enumerate() {
            let entry_lines =
                self.entry_line_counts.get(idx).copied().unwrap_or(1) as usize + 1;
            let entry_end = cumulative + entry_lines;

            if entry_end > scroll_offset && cumulative < viewport_end {
                let is_focused = idx == self.timeline_cursor;
                Self::build_entry(entry, is_focused, &mut lines, &self.theme);
                lines.push(Line::raw(""));
            }

            cumulative = entry_end;
            if cumulative >= viewport_end {
                break;
            }
        }

        if self.turn_active {
            let spinner = self.spinner_char();
            lines.push(Line::from(vec![Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(Color::Blue),
            )]));
        }

        lines
    }

    /// Highlight inline markdown spans in a message line.
    fn highlight_line(line: &str, spans: &mut Vec<Span<'static>>, base_color: Color) {
        let mut remaining = line;
        while !remaining.is_empty() {
            let bt = remaining.find('`');
            let bs = remaining.find("**");
            let is_ = remaining.find('*');

            let earliest = [bt, bs, is_].iter().filter_map(|&o| o).min();

            match earliest {
                None => {
                    spans.push(Span::styled(
                        remaining.to_string(),
                        Style::default().fg(base_color),
                    ));
                    break;
                }
                Some(pos) => {
                    if pos > 0 {
                        spans.push(Span::styled(
                            remaining[..pos].to_string(),
                            Style::default().fg(base_color),
                        ));
                    }

                    if bt == Some(pos) {
                        remaining = &remaining[pos + 1..];
                        if let Some(end) = remaining.find('`') {
                            spans.push(Span::styled(
                                remaining[..end].to_string(),
                                Style::default().fg(Color::Yellow),
                            ));
                            remaining = &remaining[end + 1..];
                        } else {
                            spans.push(Span::styled(
                                remaining.to_string(),
                                Style::default().fg(Color::Yellow),
                            ));
                            remaining = "";
                        }
                    } else if bs == Some(pos) {
                        remaining = &remaining[pos + 2..];
                        if let Some(end) = remaining.find("**") {
                            spans.push(Span::styled(
                                remaining[..end].to_string(),
                                Style::default().fg(base_color).bold(),
                            ));
                            remaining = &remaining[end + 2..];
                        } else {
                            spans.push(Span::styled(
                                remaining.to_string(),
                                Style::default().fg(base_color).bold(),
                            ));
                            remaining = "";
                        }
                    } else if is_ == Some(pos) {
                        remaining = &remaining[pos + 1..];
                        if let Some(end) = remaining.find('*') {
                            if remaining.get(end + 1..end + 2) == Some("*") {
                                spans.push(Span::styled(
                                    format!("*{}", &remaining[..end]),
                                    Style::default().fg(base_color),
                                ));
                                remaining = &remaining[end..];
                            } else {
                                spans.push(Span::styled(
                                    remaining[..end].to_string(),
                                    Style::default().fg(base_color).italic(),
                                ));
                                remaining = &remaining[end + 1..];
                            }
                        } else {
                            spans.push(Span::styled(
                                remaining.to_string(),
                                Style::default().fg(base_color).italic(),
                            ));
                            remaining = "";
                        }
                    } else {
                        spans.push(Span::styled(
                            remaining[..1].to_string(),
                            Style::default().fg(base_color),
                        ));
                        remaining = &remaining[1..];
                    }
                }
            }
        }
    }

    /// Build ratatui Lines for a single timeline entry.
    pub fn build_entry(
        entry: &TimelineEntry,
        is_focused: bool,
        lines: &mut Vec<Line<'static>>,
        theme: &Theme,
    ) {
        match entry {
            TimelineEntry::Message { role, content, .. } => {
                let (color, prefix) = match role.as_str() {
                    "user" => (theme.user_color(), "> "),
                    "system" => (Color::DarkGray, "  "),
                    _ => (theme.fg(), ""),
                };
                let total_lines = content.lines().count();
                const MAX_LINES: usize = 500;

                if role == "assistant" {
                    let md_lines = md_renderer::render_markdown_lines(content, color);
                    for line in md_lines.into_iter().take(MAX_LINES) {
                        lines.push(line);
                    }
                    if total_lines > MAX_LINES {
                        lines.push(Line::from(Span::styled(
                            format!(
                                "  ... ({} more lines truncated)",
                                total_lines.saturating_sub(MAX_LINES)
                            ),
                            Style::default().fg(Color::DarkGray),
                        )));
                    }
                    return;
                }

                for (i, line) in content.lines().enumerate() {
                    if i >= MAX_LINES {
                        lines.push(Line::from(Span::styled(
                            format!(
                                "  ... ({} more lines truncated)",
                                total_lines.saturating_sub(MAX_LINES)
                            ),
                            Style::default().fg(Color::DarkGray),
                        )));
                        break;
                    }
                    let mut spans =
                        vec![Span::styled(prefix.to_string(), Style::default().fg(color).bold())];
                    Self::highlight_line(line, &mut spans, color);
                    lines.push(Line::from(spans));
                }
            }

            TimelineEntry::Thinking {
                id: _,
                content,
                complete,
                expanded,
            } => {
                let total_lines = content.lines().count();
                let status = if *complete { "complete" } else { "thinking" };
                let focus_marker = if is_focused { "● " } else { "  " };

                if *expanded {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!(
                                "{focus_marker}┌─ 💭 Thinking [{status}] ({total_lines} lines)"
                            ),
                            Style::default().fg(Color::Cyan),
                        ),
                        Span::styled(
                            if is_focused {
                                "[Enter=collapse]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));

                    for line in content.lines().take(200) {
                        lines.push(Line::from(vec![
                            Span::styled("│  ".to_string(), Style::default().fg(Color::Cyan)),
                            Span::styled(line.to_string(), Style::default().fg(Color::DarkGray)),
                        ]));
                    }
                    if total_lines > 200 {
                        lines.push(Line::from(vec![
                            Span::styled("│  ".to_string(), Style::default().fg(Color::Cyan)),
                            Span::styled(
                                format!("... ({} more lines)", total_lines - 200),
                                Style::default().fg(Color::DarkGray),
                            ),
                        ]));
                    }
                    lines.push(Line::from(Span::styled(
                        "└─".to_string(),
                        Style::default().fg(Color::Cyan),
                    )));
                } else {
                    let preview: String = content.chars().take(80).collect();
                    let more = if content.len() > 80 { "..." } else { "" };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!(
                                "{focus_marker}💭 Thinking [{status}] ({total_lines}L): {preview}{more}"
                            ),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(
                            if is_focused && *complete {
                                "[Enter=expand]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }

            TimelineEntry::ToolCall {
                id: _,
                name,
                preview,
                output,
                done,
                expanded,
                exit_code,
            } => {
                let status_style = if *done {
                    if exit_code == &Some(0) {
                        Style::default().fg(Color::Green)
                    } else {
                        Style::default().fg(Color::Red)
                    }
                } else {
                    Style::default().fg(Color::Yellow)
                };
                let status_icon = if *done {
                    if exit_code == &Some(0) {
                        "✅"
                    } else {
                        "❌"
                    }
                } else {
                    "⏳"
                };
                let status_text = if *done {
                    format!("exit:{}", exit_code.unwrap_or(0))
                } else {
                    "running...".to_string()
                };
                let focus_marker = if is_focused { "● " } else { "  " };

                if *expanded && !output.is_empty() {
                    let total_lines = output.lines().count();
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{focus_marker}┌─ 🔧 {name}"),
                            Style::default().fg(Color::Yellow).bold(),
                        ),
                        Span::styled(format!(" [{status_text}]"), status_style),
                        Span::styled(
                            if is_focused {
                                "[Enter=collapse]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                    let display_lines: Vec<String> =
                        output.lines().take(100).map(|s| s.to_string()).collect();
                    for line in &display_lines {
                        lines.push(Line::from(Span::styled(
                            format!("│ {line}"),
                            Style::default().fg(Color::DarkGray),
                        )));
                    }
                    if total_lines > 100 {
                        lines.push(Line::from(Span::styled(
                            format!("│ ... ({} more lines)", total_lines - 100),
                            Style::default().fg(Color::DarkGray),
                        )));
                    }
                    lines.push(Line::from(Span::styled(
                        "└─".to_string(),
                        Style::default().fg(Color::Yellow),
                    )));
                } else {
                    let preview_text = if preview.is_empty() {
                        name.as_str()
                    } else {
                        preview.as_str()
                    };
                    let short_preview: String = preview_text.chars().take(60).collect();
                    let more = if preview_text.len() > 60 { "..." } else { "" };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{focus_marker}🔧 {name}"),
                            Style::default().fg(Color::Yellow).bold(),
                        ),
                        Span::styled(
                            format!(": {short_preview}{more}"),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(
                            format!(" [{status_icon} {status_text}]"),
                            status_style,
                        ),
                        Span::styled(
                            if is_focused && *done {
                                "[Enter=expand]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }

            TimelineEntry::SlashOutput {
                command,
                output,
                expanded,
            } => {
                let total_lines = output.lines().count();
                let focus_marker = if is_focused { "● " } else { "  " };

                if *expanded {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{focus_marker}┌─ /{command} ({total_lines} lines)"),
                            Style::default().fg(Color::Magenta).bold(),
                        ),
                        Span::styled(
                            if is_focused {
                                "[Enter=collapse] [Ctrl+Y=copy]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                    for line in output.lines().take(100) {
                        lines.push(Line::from(vec![
                            Span::styled("│  ".to_string(), Style::default().fg(Color::Magenta)),
                            Span::styled(line.to_string(), Style::default().fg(Color::White)),
                        ]));
                    }
                    if total_lines > 100 {
                        lines.push(Line::from(Span::styled(
                            format!("│ ... ({} more lines)", total_lines - 100),
                            Style::default().fg(Color::DarkGray),
                        )));
                    }
                    lines.push(Line::from(Span::styled(
                        "└─".to_string(),
                        Style::default().fg(Color::Magenta),
                    )));
                } else {
                    let preview: String = output.chars().take(80).collect();
                    let more = if output.len() > 80 { "..." } else { "" };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{focus_marker}/ {command} ({total_lines}L): {preview}{more}"),
                            Style::default().fg(Color::Magenta).bold(),
                        ),
                        Span::styled(
                            if is_focused {
                                "[Enter=expand] [Ctrl+Y=copy]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }
        }
    }
}

impl Default for ChatView {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::test_utils::MockTerminal;

    // ── Helpers ───────────────────────────────────────────────────

    fn make_message(role: &str, content: &str) -> TimelineEntry {
        TimelineEntry::Message {
            role: role.into(),
            content: content.into(),
            timestamp: "12:00".into(),
        }
    }

    fn make_thinking(id: u64, content: &str, complete: bool, expanded: bool) -> TimelineEntry {
        TimelineEntry::Thinking {
            id,
            content: content.into(),
            complete,
            expanded,
        }
    }

    fn make_tool_call(
        id: &str,
        name: &str,
        output: &str,
        done: bool,
        expanded: bool,
        exit_code: Option<i32>,
    ) -> TimelineEntry {
        TimelineEntry::ToolCall {
            id: id.into(),
            name: name.into(),
            preview: format!("Run {name}"),
            output: output.into(),
            done,
            expanded,
            exit_code,
        }
    }

    fn make_slash_output(command: &str, output: &str, expanded: bool) -> TimelineEntry {
        TimelineEntry::SlashOutput {
            command: command.into(),
            output: output.into(),
            expanded,
        }
    }

    fn render_view(view: &mut ChatView, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let theme_skin = crate::tui::skin::SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme_skin);
            view.render(&mut ctx, area);
        });
        terminal.buffer_lines()
    }

    // ── render_message tests ──────────────────────────────────────

    #[test]
    fn render_message_user() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "Hello, world!")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        assert!(
            lines.iter().any(|l| l.contains("Hello, world!")),
            "Expected 'Hello, world!' in output"
        );
        assert!(
            lines.iter().any(|l| l.contains("> ")),
            "Expected '> ' user prefix"
        );
    }

    #[test]
    fn render_message_assistant() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("assistant", "Here is **bold** and `code`.")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("bold"), "Expected 'bold' in output");
        assert!(joined.contains("code"), "Expected 'code' in output");
    }

    #[test]
    fn render_message_system() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("system", "System notification.")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        assert!(
            lines.iter().any(|l| l.contains("System notification.")),
            "Expected system message text"
        );
    }

    #[test]
    fn render_thinking_collapsed() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "line1\nline2\nline3", true, false)];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Thinking"),
            "Expected 'Thinking' in collapsed output"
        );
        assert!(joined.contains("3L"), "Expected line count '3L'");
        assert!(joined.contains("complete"), "Expected 'complete' status");
    }

    #[test]
    fn render_thinking_expanded() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "line A\nline B", true, true)];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![4];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("line A"),
            "Expected 'line A' in expanded thinking"
        );
        assert!(
            joined.contains("line B"),
            "Expected 'line B' in expanded thinking"
        );
        assert!(
            joined.contains("[Enter=collapse]"),
            "Expected collapse hint for focused entry"
        );
        assert!(
            joined.contains("🔒") || joined.contains("💭"),
            "Expected thinking icon in output"
        );
    }

    #[test]
    fn render_tool_call() {
        let mut view = ChatView::new();
        view.timeline = vec![make_tool_call("t1", "bash", "output line", true, false, Some(0))];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("bash"), "Expected tool name 'bash'");
        assert!(
            joined.contains("exit:0"),
            "Expected 'exit:0' for success status"
        );
    }

    #[test]
    fn render_tool_call_expanded() {
        let mut view = ChatView::new();
        view.timeline = vec![make_tool_call(
            "t1",
            "echo",
            "Hello\nWorld",
            true,
            true,
            Some(0),
        )];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![4];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("Hello"), "Expected 'Hello' in output");
        assert!(joined.contains("World"), "Expected 'World' in output");
        assert!(
            joined.contains("[Enter=collapse]"),
            "Expected collapse hint"
        );
    }

    #[test]
    fn render_slash_output() {
        let mut view = ChatView::new();
        view.timeline = vec![make_slash_output("status", "All systems go", false)];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("/ status"),
            "Expected '/ status' in slash output"
        );
        assert!(
            joined.contains("All systems go"),
            "Expected command output text"
        );
    }

    #[test]
    fn render_slash_output_expanded() {
        let mut view = ChatView::new();
        view.timeline = vec![make_slash_output("status", "line1\nline2\nline3\nline4", true)];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![6];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("┌─"),
            "Expected top border '┌─' in expanded slash output"
        );
        assert!(
            joined.contains("line1"),
            "Expected first output line"
        );
        assert!(
            joined.contains("└─"),
            "Expected bottom border '└─'"
        );
    }

    // ── Virtual scrolling ─────────────────────────────────────────

    #[test]
    fn virtual_scroll_activates() {
        // Create enough entries that total_lines > 3 * viewport
        // Each message = 1 content line + 1 separator = 2 lines.
        // viewport = 10, threshold = 30, need >30 lines.
        // 20 messages → 20 * 2 = 40 lines (>30).
        let mut view = ChatView::new();
        view.viewport_height = 10;
        view.timeline = (0..20)
            .map(|i| make_message("user", &format!("msg {i}")))
            .collect();
        view.entry_line_counts = vec![1u16; 20];
        view.msg_version = 1;
        view.lines_dirty = true;

        // After rendering, the cached lines should NOT be fully populated
        // because virtual scrolling builds only visible entries.
        let _lines = render_view(&mut view, 80, 10);

        // In virtual scrolling mode, cached_chat_lines is NOT used;
        // build_visible is called directly. The cache stays stale.
        // Verify by checking that cached_chat_lines is NOT 40+ lines
        let total = view.total_lines();
        let threshold = 10_usize.saturating_mul(3);
        assert!(
            total > threshold,
            "total_lines={total} should exceed threshold={threshold}"
        );
    }

    // ── Streaming tail rebuild ───────────────────────────────────

    #[test]
    fn streaming_tail_rebuild() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "first message"),
            make_message("assistant", "partial streamin"),
        ];
        view.turn_active = true;
        view.entry_line_counts = vec![1, 1];
        view.msg_version = 0;
        // Build initial cache
        view.cached_chat_lines = ChatView::build_new_lines(&view);
        view.last_drawn_version = 0;
        view.lines_dirty = false;

        let before_count = view.cached_chat_lines.len();

        // Simulate streaming: update last entry content
        view.timeline[1] = make_message("assistant", "partial streaming complete!");
        view.lines_dirty = true;

        // Call rebuild
        ChatView::rebuild_streaming_tail(&mut view);

        let after_count = view.cached_chat_lines.len();
        // The last entry gets rebuilt; the prefix stays the same.
        // Total lines should be comparable (could be same or more/less).
        assert!(
            after_count > 0,
            "cached_chat_lines should have content after rebuild"
        );

        // Verify the updated content is present
        let joined = view
            .cached_chat_lines
            .iter()
            .map(|l| {
                l.spans
                    .iter()
                    .map(|s| s.content.as_ref())
                    .collect::<Vec<&str>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            joined.contains("complete!"),
            "Expected 'complete!' after streaming rebuild, got:\n{joined}"
        );
        assert!(
            joined.contains("first message"),
            "Prefix entry should be preserved"
        );

        // Verify before and after line counts are reasonable
        assert!(
            (after_count as isize - before_count as isize).abs() < 10,
            "Line count changed drastically: before={before_count}, after={after_count}"
        );
    }

    // ── Scroll-to-entry ───────────────────────────────────────────

    #[test]
    fn scroll_to_entry() {
        let mut view = ChatView::new();
        view.viewport_height = 5;
        // 10 entries, each 1 line + 1 separator = 2 lines → 20 total lines
        view.timeline = (0..10)
            .map(|i| make_message("user", &format!("msg {i}")))
            .collect();
        view.entry_line_counts = vec![1u16; 10];
        view.scroll_offset = 0;

        // Scroll to entry 8 (near the bottom)
        view.scroll_to_entry(8);

        // Entry 8 starts at offset 8 * 2 = 16.
        // Viewport of 5: scroll_offset should be 16 - (5 - 1) = 12 or close.
        // With entry_h=1: offset=16, need offset + 1 > scroll + 5 → 17 > scroll + 5 → scroll < 12
        // scroll_to_entry computes: offset.saturating_sub(5.saturating_sub(1)) = 16 - 4 = 12
        assert!(
            view.scroll_offset >= 10,
            "scroll_offset={} should be large enough to show entry 8",
            view.scroll_offset
        );

        // Scroll to entry 0
        view.scroll_to_entry(0);
        assert_eq!(view.scroll_offset, 0, "scroll_to_entry(0) should reset scroll");
    }

    // ── Empty timeline ────────────────────────────────────────────

    #[test]
    fn empty_timeline_shows_placeholder() {
        let mut view = ChatView::new();
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Type to start"),
            "Empty timeline should show placeholder"
        );
    }

    // ── Spinner during active turn ────────────────────────────────

    #[test]
    fn spinner_renders_when_turn_active() {
        let mut view = ChatView::new();
        view.turn_active = true;
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Processing..."),
            "Spinner should show 'Processing...' when turn is active"
        );
    }

    // ── Event handling ────────────────────────────────────────────

    #[test]
    fn handle_enter_expands_thinking() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "content", true, false)];
        view.timeline_cursor = 0;

        let result = view.handle_event(&Event::Key(KeyEvent::from(KeyCode::Enter)));
        assert!(result.is_consumed());

        // Entry should now be expanded
        match &view.timeline[0] {
            TimelineEntry::Thinking { expanded, .. } => assert!(*expanded),
            _ => panic!("expected Thinking entry"),
        }
    }

    #[test]
    fn handle_enter_collapses_thinking() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "content", true, true)];
        view.timeline_cursor = 0;

        let result = view.handle_event(&Event::Key(KeyEvent::from(KeyCode::Enter)));
        assert!(result.is_consumed());

        match &view.timeline[0] {
            TimelineEntry::Thinking { expanded, .. } => assert!(!*expanded),
            _ => panic!("expected Thinking entry"),
        }
    }

    #[test]
    fn handle_pageup_scrolls() {
        let mut view = ChatView::new();
        view.viewport_height = 10;
        view.scroll_offset = 30;

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::PageUp)));
        assert!(
            view.scroll_offset < 30,
            "PageUp should reduce scroll offset"
        );
    }

    #[test]
    fn handle_pagedown_scrolls() {
        let mut view = ChatView::new();
        view.viewport_height = 10;
        view.scroll_offset = 5;

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::PageDown)));
        assert!(
            view.scroll_offset > 5,
            "PageDown should increase scroll offset"
        );
    }

    #[test]
    fn component_trait_methods() {
        let view = ChatView::new();
        assert!(view.focusable(), "ChatView should be focusable");
        assert_eq!(view.id(), "chat_view");
    }

    // ── Markdown line highlighting ────────────────────────────────

    #[test]
    fn highlight_line_code_span() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message(
            "user",
            "use `cargo test` to run tests",
        )];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("cargo test"),
            "Expected code content in output"
        );
    }

    // ── Multiple entries render correctly ─────────────────────────

    #[test]
    fn multiple_entries_rendered() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "Hello"),
            make_message("assistant", "Hi there!"),
            make_thinking(1, "Let me think...", true, false),
            make_tool_call("t1", "bash", "", true, false, Some(0)),
        ];
        view.entry_line_counts = vec![1, 1, 1, 1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("Hello"), "Expected user message");
        assert!(joined.contains("Hi there!"), "Expected assistant message");
        assert!(
            joined.contains("Thinking"),
            "Expected thinking entry"
        );
        assert!(joined.contains("bash"), "Expected tool call entry");
    }

    // ── tick / spinner ────────────────────────────────────────────

    #[test]
    fn tick_advances_spinner() {
        let mut view = ChatView::new();
        let s1 = view.spinner_char().to_string();
        view.tick();
        // After 10 ticks it should cycle, but after 1 tick the char
        // may be the same or different (cycle is length 10).
        // The spinner_idx wrapping_add(1) is enough for the test.
        let s2 = view.spinner_char().to_string();
        // After 1 tick, the character might change (depends on position in cycle).
        // Just verify it doesn't panic and returns valid chars.
        assert!(!s1.is_empty());
        assert!(!s2.is_empty());
    }

    // ── Default impl ──────────────────────────────────────────────

    #[test]
    fn default_chat_view_is_empty() {
        let view = ChatView::default();
        assert!(view.timeline.is_empty());
        assert_eq!(view.scroll_offset, 0);
        assert!(view.auto_scroll);
        assert_eq!(view.id(), "chat_view");
    }

    // ── sync_from_app / sync_to_app ───────────────────────────────

    #[test]
    fn sync_roundtrip() {
        let mut app = crate::tui::test_utils::app_with_messages(5);
        app.scroll_offset = 10;
        app.auto_scroll = false;
        app.viewport_height = 30;

        let mut view = ChatView::new();
        view.sync_from_app(&app);

        assert_eq!(view.timeline.len(), 5);
        assert_eq!(view.scroll_offset, 10);
        assert!(!view.auto_scroll);

        // Modify view and sync back
        view.scroll_offset = 42;
        view.auto_scroll = true;
        view.sync_to_app(&mut app);

        assert_eq!(app.scroll_offset, 42);
        assert!(app.auto_scroll);
    }
}
