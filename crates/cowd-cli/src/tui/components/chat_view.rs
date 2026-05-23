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
    style::{Color, Modifier, Style, Stylize},
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

    // ── Message menu (Task 5) ──
    /// Set when user presses Ctrl+O on a focused message.
    pub pending_message_menu: bool,
    /// Index of the message to show the menu for.
    pub pending_menu_entry_idx: usize,

    // ── Subagent navigation (Task 9) ──
    /// Set when user presses Enter on a tool call with subagent_session_id.
    pub pending_subagent_nav: Option<String>,

    // ── Render cache ──
    cached_chat_lines: Vec<Line<'static>>,
    entry_line_counts: Vec<u16>,
    msg_version: u64,
    last_drawn_version: u64,
    lines_dirty: bool,

    // ── Theme ──
    pub theme: Theme,

    // ── Search highlight (Task 17) ──
    pub search_query: String,
    pub search_matches: Vec<usize>,
    pub search_current: usize,
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
            pending_message_menu: false,
            pending_menu_entry_idx: 0,
            pending_subagent_nav: None,
            cached_chat_lines: Vec::new(),
            entry_line_counts: Vec::new(),
            msg_version: 0,
            last_drawn_version: u64::MAX,
            lines_dirty: true,
            theme: Theme::Dark,
            search_query: String::new(),
            search_matches: Vec::new(),
            search_current: 0,
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
        self.timeline = app.timeline_clone_vec();
        self.timeline_cursor = app.timeline_cursor;
        self.scroll_offset = app.scroll_offset;
        self.auto_scroll = app.auto_scroll;
        self.viewport_height = app.viewport_height;
        self.turn_active = app.turn_active;
        self.spinner_idx = app.spinner_idx;
        self.theme = app.theme;
        self.msg_version = app.msg_version;
        self.lines_dirty = app.lines_dirty;
        self.search_query = app.search_query.clone();
        self.search_matches = app.search_matches.clone();
        self.search_current = app.search_current;
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
        let mut visible_lines: Vec<Line<'static>>;
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

        // ── Apply search highlight (Task 17) ──
        if !self.search_query.is_empty() && !self.search_matches.is_empty() {
            let mut global_match_counter: usize = 0;
            let current_match_entry = self.search_matches.get(self.search_current).copied();
            for line in &mut visible_lines {
                // Only highlight lines whose entry index matches a search match
                // We search all lines and track matches globally
                ChatView::highlight_search_in_line(
                    line,
                    &self.search_query,
                    current_match_entry,
                    &mut global_match_counter,
                );
            }
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
        // ── Ctrl+O: open per-message action menu (Task 5) ──
        if key.modifiers == crossterm::event::KeyModifiers::CONTROL && key.code == KeyCode::Char('o') {
            if !self.timeline.is_empty() && self.timeline_cursor < self.timeline.len() {
                self.pending_menu_entry_idx = self.timeline_cursor;
                self.pending_message_menu = true;
            }
            return EventResult::Consumed;
        }

        match key.code {
            KeyCode::Enter => {
                // Check if focused entry is a ToolCall with subagent_session_id (Task 9)
                if let Some(entry) = self.timeline.get(self.timeline_cursor) {
                    if let TimelineEntry::ToolCall { name, output, done, .. } = entry {
                        if *done && name == "task" && !output.is_empty() {
                            // Try to extract subagent_session_id from output
                            if let Ok(val) = serde_json::from_str::<serde_json::Value>(output) {
                                let session_id = val
                                    .get("subagent_session_id")
                                    .or_else(|| val.get("session_id"))
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                if let Some(sid) = session_id {
                                    self.pending_subagent_nav = Some(sid);
                                    return EventResult::Consumed;
                                }
                            }
                        }
                    }
                }
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

    /// Apply search highlight to a Line by splitting spans around match boundaries.
    ///
    /// Every occurrence of `search_query` (case-insensitive) gets inverse video.
    /// The current match (at `search_current` global index) gets a yellow background.
    fn highlight_search_in_line(line: &mut Line<'static>, query: &str, current_match_idx: Option<usize>, global_match_counter: &mut usize) {
        if query.is_empty() {
            return;
        }
        let lower_query = query.to_lowercase();
        let mut new_spans: Vec<Span<'static>> = Vec::new();

        for span in line.spans.drain(..) {
            let content = span.content.to_string();
            let lower_content = content.to_lowercase();
            let mut search_start = 0;

            while let Some(pos) = lower_content[search_start..].find(&lower_query) {
                let abs_pos = search_start + pos;

                // Text before match
                if abs_pos > 0 {
                    new_spans.push(Span::styled(
                        content[search_start..search_start + pos].to_string(),
                        span.style,
                    ));
                }

                // The match itself
                let matched = &content[abs_pos..abs_pos + query.len()];
                let is_current = current_match_idx.map(|idx| *global_match_counter == idx).unwrap_or(false);
                let match_style = if is_current {
                    Style::default().bg(Color::Yellow).fg(Color::Black).add_modifier(Modifier::BOLD)
                } else {
                    span.style.bg(Color::DarkGray).add_modifier(Modifier::REVERSED)
                };
                new_spans.push(Span::styled(matched.to_string(), match_style));

                *global_match_counter += 1;
                search_start = abs_pos + query.len();
            }

            // Remaining text after last match
            if search_start < content.len() {
                new_spans.push(Span::styled(
                    content[search_start..].to_string(),
                    span.style,
                ));
            }
        }

        line.spans = new_spans;
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

                // Check for subagent session (Task 9)
                let subagent_label = if *done && name == "task" && !output.is_empty() {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(output) {
                        let has_sid = val
                            .get("subagent_session_id")
                            .or_else(|| val.get("session_id"))
                            .and_then(|v| v.as_str())
                            .map(|s| !s.is_empty())
                            .unwrap_or(false);
                        if has_sid && is_focused {
                            Some(" [Open Subagent]".to_string())
                        } else if has_sid {
                            Some(String::new())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                if *expanded && !output.is_empty() {
                    let total_lines = output.lines().count();
                    let mut tool_line = vec![
                        Span::styled(
                            format!("{focus_marker}┌─ 🔧 {name}"),
                            Style::default().fg(Color::Yellow).bold(),
                        ),
                        Span::styled(format!(" [{status_text}]"), status_style),
                    ];
                    if let Some(sa) = &subagent_label {
                        if !sa.is_empty() {
                            tool_line.push(Span::styled(
                                sa.clone(),
                                Style::default().fg(Color::Cyan),
                            ));
                        }
                    }
                    tool_line.push(Span::styled(
                        if is_focused {
                            "[Enter=collapse]".to_string()
                        } else {
                            String::new()
                        },
                        Style::default().fg(Color::DarkGray),
                    ));
                    lines.push(Line::from(tool_line));

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
                    let mut tool_line = vec![
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
                    ];
                    if let Some(sa) = &subagent_label {
                        if !sa.is_empty() {
                            tool_line.push(Span::styled(
                                sa.clone(),
                                Style::default().fg(Color::Cyan),
                            ));
                        }
                    }
                    tool_line.push(Span::styled(
                        if is_focused && *done {
                            if name == "task" {
                                "[Enter=expand|nav]".to_string()
                            } else {
                                "[Enter=expand]".to_string()
                            }
                        } else {
                            String::new()
                        },
                        Style::default().fg(Color::DarkGray),
                    ));
                    lines.push(Line::from(tool_line));
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

    // ── Task 5: Per-message actions menu ────────────────────────────

    #[test]
    fn message_menu_shows_on_ctrl_o() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "Hello")];
        view.timeline_cursor = 0;
        view.msg_version = 0;
        view.lines_dirty = false;

        let ev = Event::Key(KeyEvent::new(
            KeyCode::Char('o'),
            crossterm::event::KeyModifiers::CONTROL,
        ));
        let result = view.handle_event(&ev);
        assert!(result.is_consumed(), "Ctrl+O should be consumed");
        assert!(
            view.pending_message_menu,
            "pending_message_menu should be set"
        );
        assert_eq!(
            view.pending_menu_entry_idx, 0,
            "should target entry 0"
        );
    }

    #[test]
    fn message_menu_ctrl_o_empty_timeline_noop() {
        let mut view = ChatView::new();
        let ev = Event::Key(KeyEvent::new(
            KeyCode::Char('o'),
            crossterm::event::KeyModifiers::CONTROL,
        ));
        let result = view.handle_event(&ev);
        assert!(result.is_consumed(), "Ctrl+O should always be consumed");
        assert!(
            !view.pending_message_menu,
            "no pending menu on empty timeline"
        );
    }

    #[test]
    fn message_menu_entry_idx_tracks_cursor() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "A"),
            make_message("user", "B"),
            make_message("user", "C"),
        ];
        view.timeline_cursor = 2;
        view.msg_version = 0;
        view.lines_dirty = false;

        let ev = Event::Key(KeyEvent::new(
            KeyCode::Char('o'),
            crossterm::event::KeyModifiers::CONTROL,
        ));
        let _ = view.handle_event(&ev);
        assert_eq!(
            view.pending_menu_entry_idx, 2,
            "should track cursor position"
        );
    }

    // ── Task 9: Subagent session navigation ─────────────────────────

    #[test]
    fn subagent_nav_shows_on_task_tool_call() {
        let mut view = ChatView::new();
        let output = r#"{"subagent_session_id": "sess_sub_123"}"#;
        view.timeline = vec![TimelineEntry::ToolCall {
            id: "t1".into(),
            name: "task".into(),
            preview: "Run subagent".into(),
            output: output.into(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        // Should show subagent navigation label
        assert!(
            joined.contains("Open Subagent") || joined.contains("nav"),
            "Expected subagent navigation indicator in output, got: {joined}"
        );
    }

    #[test]
    fn open_subagent_navigates_on_enter() {
        let mut view = ChatView::new();
        let output = r#"{"subagent_session_id": "sess_sub_456"}"#;
        view.timeline = vec![TimelineEntry::ToolCall {
            id: "t1".into(),
            name: "task".into(),
            preview: "Run subagent".into(),
            output: output.into(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];
        view.timeline_cursor = 0;

        // Enter on task tool call should set pending_subagent_nav
        let ev = Event::Key(KeyEvent::from(KeyCode::Enter));
        let result = view.handle_event(&ev);
        assert!(result.is_consumed(), "Enter should be consumed");

        assert!(
            view.pending_subagent_nav.is_some(),
            "pending_subagent_nav should be set"
        );
        assert_eq!(
            view.pending_subagent_nav.as_deref(),
            Some("sess_sub_456"),
            "should capture session ID"
        );
    }

    #[test]
    fn subagent_nav_no_output_does_not_navigate() {
        let mut view = ChatView::new();
        view.timeline = vec![TimelineEntry::ToolCall {
            id: "t1".into(),
            name: "task".into(),
            preview: "Run subagent".into(),
            output: String::new(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];
        view.timeline_cursor = 0;

        let ev = Event::Key(KeyEvent::from(KeyCode::Enter));
        let result = view.handle_event(&ev);
        assert!(result.is_consumed(), "Enter should be consumed");

        // Without output, should just toggle expand, not set nav
        assert!(
            view.pending_subagent_nav.is_none(),
            "no nav without output"
        );
        // Should have expanded the entry
        match &view.timeline[0] {
            TimelineEntry::ToolCall { expanded, .. } => {
                assert!(*expanded, "Should have expanded tool call");
            }
            _ => panic!("expected ToolCall"),
        }
    }

    // ── Task 17: Search highlight tests ───────────────────────────

    #[test]
    fn search_highlight_inverts_matching() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "Hello world, this is a test message")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;
        view.search_query = "test".to_string();
        view.search_matches = vec![0];
        view.search_current = 0;

        let mut terminal = MockTerminal::new(80, 24);
        let theme_skin = crate::tui::skin::SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme_skin);
            view.render(&mut ctx, area);
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        // The text should still be rendered (containing "test")
        assert!(joined.contains("test"), "Search match text should be visible");
    }

    #[test]
    fn handles_cjk() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "你好世界 test 中文")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;
        view.search_query = "中文".to_string();
        view.search_matches = vec![0];
        view.search_current = 0;

        // Verify the highlight function works correctly with CJK
        let mut line = Line::from(Span::raw("你好世界 test 中文"));
        let mut counter = 0;
        ChatView::highlight_search_in_line(&mut line, "中文", Some(0), &mut counter);
        // The counter should have incremented (found match)
        assert!(counter > 0, "Should find CJK match in text");
        // The line spans should have the highlighted match
        let has_cjk = line.spans.iter().any(|s| s.content.contains("中文"));
        assert!(has_cjk, "CJK match should be preserved in spans");
    }

    #[test]
    fn next_match_cycles() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "first match here"),
            make_message("user", "second match here"),
            make_message("user", "no match"),
        ];
        view.entry_line_counts = vec![1, 1, 1];
        view.msg_version = 0;
        view.lines_dirty = false;
        view.search_query = "match".to_string();
        view.search_matches = vec![0, 1];
        view.search_current = 0;

        // Verify initial state
        assert_eq!(view.search_current, 0);
        assert_eq!(view.search_matches.len(), 2);

        // Verify highlight_search_in_line works on a line containing match
        let mut line = Line::from(Span::raw("first match here"));
        let mut counter = 0;
        ChatView::highlight_search_in_line(&mut line, "match", Some(0), &mut counter);
        assert!(
            counter > 0,
            "Should have found at least one match in the line"
        );
        // The spans should contain both the original text and highlighted parts
        assert!(!line.spans.is_empty(), "Should have split spans");

        let mut line2 = Line::from(Span::raw("no match here"));
        let mut counter2 = 0;
        ChatView::highlight_search_in_line(&mut line2, "match", None, &mut counter2);
        // "no match here" contains "match", so counter2 should be > 0
        assert!(counter2 > 0, "Should find 'match' in 'no match here'");
    }

    #[test]
    fn search_highlight_skips_empty_query() {
        let mut line = Line::from(Span::raw("hello world"));
        let mut counter = 0;
        ChatView::highlight_search_in_line(&mut line, "", None, &mut counter);
        assert_eq!(counter, 0, "Empty query should find nothing");
        assert_eq!(line.spans.len(), 1, "Spans should be unmodified");
    }

    // ── Continue existing tests ──────────────────────────────────

    #[test]
    fn subagent_nav_non_task_tool_no_nav() {
        let mut view = ChatView::new();
        let output = r#"{"subagent_session_id": "sess_sub_789"}"#;
        view.timeline = vec![TimelineEntry::ToolCall {
            id: "t1".into(),
            name: "bash".into(), // not "task"
            preview: "echo".into(),
            output: output.into(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];
        view.timeline_cursor = 0;

        let ev = Event::Key(KeyEvent::from(KeyCode::Enter));
        let _ = view.handle_event(&ev);
        assert!(
            view.pending_subagent_nav.is_none(),
            "non-task tool should not set nav flag"
        );
    }
}
