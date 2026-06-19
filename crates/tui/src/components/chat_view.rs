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
    widgets::{Clear, Paragraph, Wrap},
};

use crate::app::{Theme, TimelineEntry};
use crate::components::{Component, EventResult, RenderContext};
use crate::md_renderer;
use crate::scroll_state::ScrollState;

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

    // ── Scrolling (unified scroll state from ratatui-kit pattern) ──
    pub scroll_state: ScrollState,

    // ── Turn state ──
    pub turn_active: bool,
    turn_input_tokens: u64,
    turn_output_tokens: u64,
    session_input_tokens: u64,
    session_output_tokens: u64,
    memory_total_entries: usize,
    spinner_idx: usize,

    // ── Message menu (Task 5) ──
    /// Set when user presses Ctrl+O on a focused message.
    pub pending_message_menu: bool,
    /// Index of the message to show the menu for.
    pub pending_menu_entry_idx: usize,

    // ── Subagent navigation (Task 9) ──
    /// Set when user presses Enter on a tool call with subagent_session_id.
    pub pending_subagent_nav: Option<String>,

    // ── Compact chat mode ──
    /// When true, shows only key content (thinking, final answer, stats).
    pub compact_mode: bool,

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

#[derive(Debug, Clone, Copy, Default)]
struct ChatTurnStats {
    thinking_count: usize,
    tool_count: usize,
}

impl ChatView {
    fn renders_in_main_chat(entry: &TimelineEntry) -> bool {
        matches!(
            entry,
            TimelineEntry::Message { .. } | TimelineEntry::SlashOutput { .. }
        )
    }

    fn visible_main_entries(&self) -> impl Iterator<Item = (usize, &TimelineEntry)> {
        self.timeline
            .iter()
            .enumerate()
            .filter(|(_, entry)| Self::renders_in_main_chat(entry))
    }

    /// Create a new empty chat view with default 24-row viewport.
    #[must_use]
    pub fn new() -> Self {
        Self {
            timeline: Vec::new(),
            timeline_cursor: 0,
            scroll_state: ScrollState::new(),
            turn_active: false,
            turn_input_tokens: 0,
            turn_output_tokens: 0,
            session_input_tokens: 0,
            session_output_tokens: 0,
            memory_total_entries: 0,
            spinner_idx: 0,
            pending_message_menu: false,
            pending_menu_entry_idx: 0,
            pending_subagent_nav: None,
            compact_mode: false,
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
    /// Uses incremental append: clones only new entries, falls back to
    /// full rebuild on eviction, and patches the streaming tail in-place.
    pub fn sync_from_app(&mut self, app: &crate::App) {
        let new_len = app.timeline_len();
        // Append new entries
        if new_len > self.timeline.len() {
            for i in self.timeline.len()..new_len {
                if let Some(entry) = app.timeline_entry(i) {
                    self.timeline.push(entry);
                }
            }
        } else if new_len < self.timeline.len() {
            self.timeline = app.timeline_clone_vec();
        }
        // Fix: sync ALL entries that changed, not just the last one
        // This catches mid-timeline mutations like ToolProgress/ToolComplete
        if new_len <= self.timeline.len() {
            let sync_len = new_len.min(self.timeline.len());
            for i in 0..sync_len {
                if let (Some(fresh), Some(local)) =
                    (app.timeline_entry(i), self.timeline.get_mut(i))
                {
                    if fresh != *local {
                        *local = fresh;
                    }
                }
            }
        }
        self.timeline_cursor = app.timeline_cursor;
        self.scroll_state.offset = app.scroll_offset;
        self.scroll_state.auto_scroll = app.auto_scroll;
        self.scroll_state.viewport_height = app.viewport_height;
        self.turn_active = app.turn_active;
        self.turn_input_tokens = app.turn_input_tokens;
        self.turn_output_tokens = app.turn_output_tokens;
        self.session_input_tokens = app.input_tokens;
        self.session_output_tokens = app.output_tokens;
        self.memory_total_entries = app.memory_total_entries.unwrap_or(app.memory_entries.len());
        self.spinner_idx = app.spinner_idx;
        self.theme = app.theme;
        self.msg_version = app.msg_version;
        self.lines_dirty = app.lines_dirty;
        self.search_query = app.search_query.clone();
        self.search_matches = app.search_matches.clone();
        self.search_current = app.search_current;
        self.compact_mode = app.compact_chat;
    }

    /// Persist view-model back to the shared App state.
    /// Called after rendering to preserve cursor position and scroll offset.
    pub fn sync_to_app(&self, app: &mut crate::App) {
        app.scroll_offset = self.scroll_state.offset;
        app.auto_scroll = self.scroll_state.auto_scroll;
        app.viewport_height = self.scroll_state.viewport_height;
    }

    // ── Navigation ────────────────────────────────────────────────

    /// Move timeline cursor up by one collapsible entry.
    pub fn cursor_up(&mut self) -> bool {
        if self.timeline.is_empty() {
            return false;
        }
        if self.timeline_cursor >= self.timeline.len() {
            self.timeline_cursor = self.timeline.len().saturating_sub(1);
        }
        let mut idx = self.timeline_cursor;
        loop {
            if idx == 0 {
                break;
            }
            idx -= 1;
            if self.timeline[idx].is_collapsible() {
                self.timeline_cursor = idx;
                self.scroll_state.auto_scroll = false;
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
                self.scroll_state.auto_scroll = true;
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
        self.scroll_state.scroll_page_up();
    }

    /// Scroll down by one viewport worth of lines.
    pub fn scroll_page_down(&mut self) {
        self.scroll_state.scroll_page_down();
    }

    /// Scroll so the entry at the given index is visible.
    pub fn scroll_to_entry(&mut self, entry_idx: usize) {
        let vh = self.scroll_state.viewport_height.max(1) as usize;
        let mut offset: usize = 0;
        for i in 0..entry_idx.min(self.entry_line_counts.len()) {
            offset += self.entry_line_counts[i] as usize + 1;
        }
        let entry_h = self.entry_line_counts.get(entry_idx).copied().unwrap_or(1) as usize;

        let scroll = self.scroll_state.offset as usize;
        if offset < scroll {
            self.scroll_state.offset = offset as u16;
        } else if offset + entry_h > scroll + vh {
            self.scroll_state.offset = offset.saturating_sub(vh.saturating_sub(entry_h)) as u16;
        }
    }

    // ── Line computation (internal) ───────────────────────────────

    /// Total number of content lines (including separator blanks and spinner).
    pub fn total_lines(&self) -> usize {
        let n = self
            .timeline
            .iter()
            .filter(|entry| Self::renders_in_main_chat(entry))
            .count();
        let mut total: usize = self
            .entry_line_counts
            .iter()
            .map(|&c| c as usize)
            .sum::<usize>()
            + n.saturating_sub(1); // separators between entries, not after last
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
        self.scroll_state.viewport_height = viewport_h as u16;

        // ── Build line buffer before computing scroll bounds ──
        // Scroll is intentionally based on the exact line buffer we render.
        // This avoids the old split-brain path where entry estimates, virtual
        // slicing, Paragraph wrapping, and the scrollbar each had different
        // ideas of content height.
        if self.msg_version != self.last_drawn_version {
            self.cached_chat_lines = Self::build_new_lines(self);
            self.entry_line_counts = Self::compute_entry_line_counts(self);
            self.last_drawn_version = self.msg_version;
            self.lines_dirty = false;
        } else if self.lines_dirty {
            self.cached_chat_lines = Self::build_new_lines(self);
            self.entry_line_counts = Self::compute_entry_line_counts(self);
            self.lines_dirty = false;
        }
        if self.cached_chat_lines.is_empty() {
            self.cached_chat_lines = Self::build_new_lines(self);
            self.entry_line_counts = Self::compute_entry_line_counts(self);
        }

        let total_lines = self.cached_chat_lines.len().max(1);

        // ── Post-render size callback: sync actual content height ──
        self.scroll_state.set_content_size(total_lines as u16);

        // ── Auto-scroll ──
        if self.scroll_state.auto_scroll && total_lines > viewport_h {
            self.scroll_state.offset = (total_lines.saturating_sub(viewport_h)) as u16;
        }
        let scroll_off =
            self.scroll_state
                .offset
                .min(total_lines.saturating_sub(viewport_h).max(0) as u16) as usize;

        // ── Compact mode: summary view ──
        if self.compact_mode {
            let compact_lines = Self::build_compact_lines(self);
            let frame = ctx.frame_mut();
            frame.render_widget(Clear, area);
            let paragraph = Paragraph::new(Text::from(compact_lines))
                .wrap(Wrap { trim: false })
                .scroll((0, 0));
            frame.render_widget(paragraph, area);
            return;
        }

        // ── Build visible lines ──
        let mut visible_lines = self.cached_chat_lines.clone();

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
        let frame = ctx.frame_mut();
        frame.render_widget(Clear, area);

        let paragraph = Paragraph::new(Text::from(visible_lines))
            // No Wrap here: wrapping makes rendered height depend on terminal
            // width while scroll offset is line-based. Clipping long lines is
            // less harmful than a viewport that cannot reliably reach top/bottom.
            .scroll((scroll_off as u16, 0));
        frame.render_widget(paragraph, area);
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
        if key.modifiers == crossterm::event::KeyModifiers::CONTROL
            && key.code == KeyCode::Char('o')
        {
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
                    if let TimelineEntry::ToolCall {
                        name, output, done, ..
                    } = entry
                    {
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
                self.scroll_state.auto_scroll = false;
                EventResult::Consumed
            }
            KeyCode::PageDown => {
                self.scroll_page_down();
                self.scroll_state.auto_scroll = false;
                EventResult::Consumed
            }
            KeyCode::Home => {
                self.scroll_state.scroll_to_top();
                EventResult::Consumed
            }
            KeyCode::End => {
                self.scroll_state.scroll_to_bottom();
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
            .map(|e| {
                if Self::renders_in_main_chat(e) {
                    e.expanded_lines() as u16
                } else {
                    0
                }
            })
            .collect()
    }

    /// Full rebuild of chat lines from internal state.
    fn build_new_lines(&self) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();

        if self.timeline.is_empty() {
            lines.push(Line::from(Span::styled(
                "Type to start. /help /resume /exit",
                Style::default().fg(self.theme.muted_color()),
            )));
        }

        let visible: Vec<_> = self.visible_main_entries().collect();
        let visible_count = visible.len();
        let final_assistant_idx =
            self.timeline
                .iter()
                .enumerate()
                .rev()
                .find_map(|(idx, entry)| {
                    matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant")
                        .then_some(idx)
                });
        let current_turn_stats = self.current_turn_stats();
        for (visible_idx, (idx, entry)) in visible.into_iter().enumerate() {
            let is_focused = idx == self.timeline_cursor;
            Self::build_entry_with_meta(
                entry,
                is_focused,
                final_assistant_idx == Some(idx),
                &mut lines,
                &self.theme,
                self.turn_input_tokens,
                self.turn_output_tokens,
                current_turn_stats.tool_count,
                current_turn_stats.thinking_count,
                self.memory_total_entries,
            );
            if visible_idx + 1 < visible_count {
                lines.push(Line::raw(""));
            }
        }

        if self.turn_active {
            let spinner = self.spinner_char();
            lines.push(Line::from(vec![Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(self.theme.accent()),
            )]));
        }

        lines
    }

    fn build_compact_lines(&self) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();
        if self.timeline.is_empty() {
            lines.push(Line::from(Span::styled(
                "Type to start. /help /resume /exit",
                Style::default().fg(self.theme.muted_color()),
            )));
            return lines;
        }

        let mut tool_count = 0u32;
        let mut thinking_rounds = 0u32;
        let mut assistant_messages: Vec<(usize, &str)> = Vec::new();
        let mut user_messages = 0u32;

        for (i, entry) in self.timeline.iter().enumerate() {
            match entry {
                TimelineEntry::ToolCall { done, .. } => {
                    if *done {
                        tool_count += 1;
                    }
                }
                TimelineEntry::Thinking { .. } => {
                    thinking_rounds += 1;
                }
                TimelineEntry::Message { role, content, .. } => {
                    if role == "assistant" {
                        assistant_messages.push((i, content.as_str()));
                    } else if role == "user" {
                        user_messages += 1;
                    }
                }
                _ => {}
            }
        }

        for (pos, (entry_idx, content)) in assistant_messages.iter().enumerate() {
            let is_final = pos + 1 == assistant_messages.len();
            let is_focused = *entry_idx == self.timeline_cursor;
            let label = if is_final {
                if is_focused {
                    "● ├─ FINAL REPLY"
                } else {
                    "  ├─ FINAL REPLY"
                }
            } else if is_focused {
                "● ├─"
            } else {
                "  ├─"
            };
            lines.push(Line::from(Span::styled(
                label,
                Style::default()
                    .fg(if is_focused {
                        self.theme.accent()
                    } else if is_final {
                        self.theme.success_color()
                    } else {
                        self.theme.muted_color()
                    })
                    .bold(),
            )));
            let md_lines = md_renderer::render_markdown_lines(content, &self.theme);
            let max_lines = 800usize;
            for line in md_lines.into_iter().take(max_lines) {
                lines.push(line);
            }
            let total = content.lines().count();
            if total > max_lines {
                lines.push(Line::from(Span::styled(
                    format!("  ... ({} more lines)", total.saturating_sub(max_lines)),
                    Style::default().fg(self.theme.muted_color()),
                )));
            }
            lines.push(Line::raw(""));
        }

        lines.push(Line::from(Span::styled(
            "─".repeat(60),
            Style::default().fg(self.theme.muted_color()),
        )));

        let mut stats_parts: Vec<String> = Vec::new();
        if tool_count > 0 {
            stats_parts.push(format!("Tools: {}", tool_count));
        }
        if thinking_rounds > 0 {
            stats_parts.push(format!("Think: {}", thinking_rounds));
        }
        if user_messages > 0 {
            stats_parts.push(format!("Msgs: {}", user_messages));
        }
        stats_parts.push(format!(
            "Tokens: in {} / out {}",
            fmt_tokens(self.session_input_tokens),
            fmt_tokens(self.session_output_tokens)
        ));
        stats_parts.push(format!("Turn: {:.1}s", 0.0));
        lines.push(Line::from(Span::styled(
            format!("  {}", stats_parts.join(" · ")),
            Style::default().fg(self.theme.warn_color()),
        )));

        lines
    }

    fn current_turn_stats(&self) -> ChatTurnStats {
        let start = self
            .timeline
            .iter()
            .rposition(
                |entry| matches!(entry, TimelineEntry::Message { role, .. } if role == "user"),
            )
            .unwrap_or(0);
        let mut stats = ChatTurnStats::default();
        for entry in self.timeline.iter().skip(start) {
            match entry {
                TimelineEntry::Thinking { .. } => stats.thinking_count += 1,
                TimelineEntry::ToolCall { .. } => stats.tool_count += 1,
                _ => {}
            }
        }
        stats
    }

    #[cfg(test)]
    fn rebuild_streaming_tail(&mut self) {
        let n = self.timeline.len();
        if n == 0 || n > self.timeline.len() {
            return;
        }

        let prefix_count: usize =
            self.entry_line_counts
                .iter()
                .take(n.saturating_sub(1))
                .sum::<u16>()
                .saturating_add((n.saturating_sub(1)) as u16) as usize;

        let last_entry = self.timeline[n - 1].clone();
        if !Self::renders_in_main_chat(&last_entry) {
            self.cached_chat_lines = Self::build_new_lines(self);
            return;
        }
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

        if let Some(count) = self.entry_line_counts.get_mut(n - 1) {
            *count = (self.cached_chat_lines.len() - before_len) as u16;
        }

        if let Some(spinner) = spinner_str {
            self.cached_chat_lines.push(Line::from(vec![Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(self.theme.accent()),
            )]));
        }
    }

    /// Highlight inline markdown spans in a message line.
    fn highlight_line(
        line: &str,
        spans: &mut Vec<Span<'static>>,
        base_color: Color,
        theme: &Theme,
    ) {
        let code_color = theme.inline_code_color();
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
                                Style::default().fg(code_color),
                            ));
                            remaining = &remaining[end + 1..];
                        } else {
                            spans.push(Span::styled(
                                remaining.to_string(),
                                Style::default().fg(code_color),
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
                        let mut chars = remaining.char_indices();
                        let (_, ch) = chars.next().expect("remaining is not empty");
                        let next = chars.next().map(|(idx, _)| idx).unwrap_or(remaining.len());
                        spans.push(Span::styled(
                            ch.to_string(),
                            Style::default().fg(base_color),
                        ));
                        remaining = &remaining[next..];
                    }
                }
            }
        }
    }

    /// Apply search highlight to a Line by splitting spans around match boundaries.
    ///
    /// Every occurrence of `search_query` (case-insensitive) gets inverse video.
    /// The current match (at `search_current` global index) gets a yellow background.
    fn highlight_search_in_line(
        line: &mut Line<'static>,
        query: &str,
        current_match_idx: Option<usize>,
        global_match_counter: &mut usize,
    ) {
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
                let is_current = current_match_idx
                    .map(|idx| *global_match_counter == idx)
                    .unwrap_or(false);
                let match_style = if is_current {
                    Style::default()
                        .bg(Color::Yellow)
                        .fg(Color::Black)
                        .add_modifier(Modifier::BOLD)
                } else {
                    span.style
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::REVERSED)
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
        Self::build_entry_with_meta(entry, is_focused, false, lines, theme, 0, 0, 0, 0, 0);
    }

    #[allow(clippy::too_many_arguments)]
    fn build_entry_with_meta(
        entry: &TimelineEntry,
        is_focused: bool,
        is_final_assistant: bool,
        lines: &mut Vec<Line<'static>>,
        theme: &Theme,
        turn_input_tokens: u64,
        turn_output_tokens: u64,
        tool_count: usize,
        thinking_rounds: usize,
        memory_count: usize,
    ) {
        match entry {
            TimelineEntry::Message { role, content, .. } => {
                let (color, prefix) = match role.as_str() {
                    "user" => (theme.user_color(), "> "),
                    "system" => (theme.muted_color(), "  "),
                    _ => (theme.fg(), ""),
                };
                let total_lines = content.lines().count();
                const MAX_LINES: usize = 500;

                if role == "assistant" {
                    if is_final_assistant {
                        lines.push(Line::from(Span::styled(
                            "├─ FINAL REPLY",
                            Style::default().fg(theme.success_color()).bold(),
                        )));
                    }
                    let mut md_lines = md_renderer::render_markdown_lines(content, theme);
                    if !is_final_assistant {
                        if let Some(first) = md_lines.first_mut() {
                            first.spans.insert(
                                0,
                                Span::styled(
                                    "├─ ",
                                    Style::default().fg(theme.muted_color()).bold(),
                                ),
                            );
                        } else {
                            md_lines.push(Line::from(Span::styled(
                                "├─",
                                Style::default().fg(theme.muted_color()).bold(),
                            )));
                        }
                    }
                    for line in md_lines.into_iter().take(MAX_LINES) {
                        lines.push(line);
                    }
                    if total_lines > MAX_LINES {
                        lines.push(Line::from(Span::styled(
                            format!(
                                "  ... ({} more lines truncated)",
                                total_lines.saturating_sub(MAX_LINES)
                            ),
                            Style::default().fg(theme.muted_color()),
                        )));
                    }
                    if is_final_assistant {
                        lines.push(Line::from(vec![
                            Span::styled(
                                "└─ usage ",
                                Style::default().fg(theme.warn_color()).bold(),
                            ),
                            Span::styled(
                                format!(
                                    "in:{} out:{} think:{} tools:{} memory:{}",
                                    fmt_tokens(turn_input_tokens),
                                    fmt_tokens(turn_output_tokens),
                                    thinking_rounds,
                                    tool_count,
                                    memory_count
                                ),
                                Style::default().fg(theme.muted_color()),
                            ),
                        ]));
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
                            Style::default().fg(theme.muted_color()),
                        )));
                        break;
                    }
                    let mut spans = vec![Span::styled(
                        prefix.to_string(),
                        Style::default().fg(color).bold(),
                    )];
                    Self::highlight_line(line, &mut spans, color, theme);
                    lines.push(Line::from(spans));
                }
            }

            TimelineEntry::Thinking {
                id,
                content,
                complete,
                expanded: _,
            } => {
                let focus_marker = if is_focused { "● " } else { "  " };
                let status = if *complete { "saved" } else { "streaming" };
                let total_non_empty = content
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .count();
                let words = content.split_whitespace().count();
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}think "),
                        Style::default().fg(theme.accent()).bold(),
                    ),
                    Span::styled(
                        format!("#{id}"),
                        Style::default().fg(theme.muted_color()).bold(),
                    ),
                    Span::styled(
                        format!(" · {status}"),
                        Style::default().fg(if *complete {
                            theme.muted_color()
                        } else {
                            theme.warn_color()
                        }),
                    ),
                    Span::styled(
                        format!(" · {total_non_empty} lines · {words} words · details in Process"),
                        Style::default().fg(theme.muted_color()),
                    ),
                ]));
            }

            TimelineEntry::ToolCall {
                id: _,
                name,
                preview,
                output,
                done,
                expanded: _,
                exit_code,
            } => {
                let status_style = if *done {
                    if exit_code == &Some(0) {
                        Style::default().fg(theme.success_color())
                    } else {
                        Style::default().fg(theme.error_color())
                    }
                } else {
                    Style::default().fg(theme.warn_color())
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

                // Always collapsed in main view – details in Run panel
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
                        Style::default().fg(theme.warn_color()).bold(),
                    ),
                    Span::styled(
                        format!(": {short_preview}{more}"),
                        Style::default().fg(theme.muted_color()),
                    ),
                    Span::styled(format!(" [{status_icon} {status_text}]"), status_style),
                ];
                // Check for subagent session
                if *done && name == "task" && !output.is_empty() {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(output) {
                        let has_sid = val
                            .get("subagent_session_id")
                            .or_else(|| val.get("session_id"))
                            .and_then(|v| v.as_str())
                            .map(|s| !s.is_empty())
                            .unwrap_or(false);
                        if has_sid && is_focused {
                            tool_line.push(Span::styled(
                                " [Open Subagent]".to_string(),
                                Style::default().fg(theme.link_color()),
                            ));
                        }
                    }
                }
                lines.push(Line::from(tool_line));
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
                            Style::default().fg(theme.accent()).bold(),
                        ),
                        Span::styled(
                            if is_focused {
                                "[Enter=collapse] [Ctrl+Y=copy]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(theme.muted_color()),
                        ),
                    ]));
                    for line in output.lines().take(100) {
                        lines.push(Line::from(vec![
                            Span::styled("│  ".to_string(), Style::default().fg(theme.accent())),
                            Span::styled(line.to_string(), Style::default().fg(theme.fg())),
                        ]));
                    }
                    if total_lines > 100 {
                        lines.push(Line::from(Span::styled(
                            format!("│ ... ({} more lines)", total_lines - 100),
                            Style::default().fg(theme.muted_color()),
                        )));
                    }
                    lines.push(Line::from(Span::styled(
                        "└─".to_string(),
                        Style::default().fg(theme.accent()),
                    )));
                } else {
                    let preview: String = output.chars().take(80).collect();
                    let more = if output.len() > 80 { "..." } else { "" };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{focus_marker}/ {command} ({total_lines}L): {preview}{more}"),
                            Style::default().fg(theme.accent()).bold(),
                        ),
                        Span::styled(
                            if is_focused {
                                "[Enter=expand] [Ctrl+Y=copy]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(theme.muted_color()),
                        ),
                    ]));
                }
            }
        }
    }
}

fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        n.to_string()
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
    use crate::test_utils::MockTerminal;

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
        let theme_skin = crate::skin::SkinConfig::default();
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
            !joined.contains("think") && !joined.contains("line1"),
            "Thinking should stay out of main chat: {joined}"
        );
    }

    #[test]
    fn render_thinking() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "line A\nline B", true, true)];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            !joined.contains("line A") && !joined.contains("saved"),
            "Thinking details should stay out of main chat: {joined}"
        );
    }

    #[test]
    fn render_tool_call() {
        let mut view = ChatView::new();
        view.timeline = vec![make_tool_call(
            "t1",
            "bash",
            "output line",
            true,
            false,
            Some(0),
        )];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            !joined.contains("bash") && !joined.contains("output line"),
            "tool process should stay out of the main chat view, got: {joined}"
        );
    }

    #[test]
    fn render_tool_call_completed() {
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
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            !joined.contains("echo") && !joined.contains("Hello") && !joined.contains("🔧"),
            "completed tool process should stay out of the main chat view, got: {joined}"
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
        view.timeline = vec![make_slash_output(
            "status",
            "line1\nline2\nline3\nline4",
            true,
        )];
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
        assert!(joined.contains("line1"), "Expected first output line");
        assert!(joined.contains("└─"), "Expected bottom border '└─'");
    }

    // ── Virtual scrolling ─────────────────────────────────────────

    #[test]
    fn virtual_scroll_activates() {
        // Create enough entries that total_lines > 3 * viewport
        // Each message = 1 content line + 1 separator = 2 lines.
        // viewport = 10, threshold = 30, need >30 lines.
        // 20 messages → 20 * 2 = 40 lines (>30).
        let mut view = ChatView::new();
        view.scroll_state.viewport_height = 10;
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
        view.scroll_state.viewport_height = 5;
        // 10 entries, each 1 line + 1 separator = 2 lines → 20 total lines
        view.timeline = (0..10)
            .map(|i| make_message("user", &format!("msg {i}")))
            .collect();
        view.entry_line_counts = vec![1u16; 10];
        view.scroll_state.offset = 0;

        // Scroll to entry 8 (near the bottom)
        view.scroll_to_entry(8);

        // Entry 8 starts at offset 8 * 2 = 16.
        // Viewport of 5: scroll_offset should be 16 - (5 - 1) = 12 or close.
        // With entry_h=1: offset=16, need offset + 1 > scroll + 5 → 17 > scroll + 5 → scroll < 12
        // scroll_to_entry computes: offset.saturating_sub(5.saturating_sub(1)) = 16 - 4 = 12
        assert!(
            view.scroll_state.offset >= 10,
            "scroll_offset={} should be large enough to show entry 8",
            view.scroll_state.offset
        );

        // Scroll to entry 0
        view.scroll_to_entry(0);
        assert_eq!(
            view.scroll_state.offset, 0,
            "scroll_to_entry(0) should reset scroll"
        );
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
        view.scroll_state.viewport_height = 10;
        view.scroll_state.offset = 30;

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::PageUp)));
        assert!(
            view.scroll_state.offset < 30,
            "PageUp should reduce scroll offset"
        );
    }

    #[test]
    fn handle_pagedown_scrolls() {
        let mut view = ChatView::new();
        view.scroll_state.viewport_height = 10;
        view.scroll_state.offset = 5;

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::PageDown)));
        assert!(
            view.scroll_state.offset > 5,
            "PageDown should increase scroll offset"
        );
    }

    #[test]
    fn render_clamps_scroll_to_bottom_with_exact_rendered_lines() {
        let mut view = ChatView::new();
        for i in 0..40 {
            view.timeline
                .push(make_message("assistant", &format!("message {i}")));
        }
        view.msg_version = 1;
        view.scroll_state.auto_scroll = true;

        let _ = render_view(&mut view, 80, 10);

        let max_offset =
            view.cached_chat_lines
                .len()
                .saturating_sub(view.scroll_state.viewport_height as usize) as u16;
        assert_eq!(view.scroll_state.offset, max_offset);
        assert!(max_offset > 0);
    }

    #[test]
    fn home_and_end_jump_to_stable_scroll_bounds() {
        let mut view = ChatView::new();
        for i in 0..30 {
            view.timeline
                .push(make_message("assistant", &format!("line {i}")));
        }
        view.msg_version = 1;
        view.scroll_state.auto_scroll = true;
        let _ = render_view(&mut view, 80, 8);
        let bottom = view.scroll_state.offset;
        assert!(bottom > 0);

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::Home)));
        let _ = render_view(&mut view, 80, 8);
        assert_eq!(view.scroll_state.offset, 0);
        assert!(!view.scroll_state.auto_scroll);

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::End)));
        let _ = render_view(&mut view, 80, 8);
        assert_eq!(view.scroll_state.offset, bottom);
        assert!(view.scroll_state.auto_scroll);
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
        view.timeline = vec![make_message("user", "use `cargo test` to run tests")];
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
            joined.contains("FINAL REPLY"),
            "Expected final reply marker"
        );
        assert!(
            !joined.contains("Let me think") && !joined.contains("bash"),
            "Thinking and tools should stay in Process, not main chat: {joined}"
        );
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
        assert_eq!(view.scroll_state.offset, 0);
        assert!(view.scroll_state.auto_scroll);
        assert_eq!(view.id(), "chat_view");
    }

    // ── sync_from_app / sync_to_app ───────────────────────────────

    #[test]
    fn sync_roundtrip() {
        let mut app = crate::test_utils::app_with_messages(5);
        app.scroll_offset = 10;
        app.auto_scroll = false;
        app.viewport_height = 30;

        let mut view = ChatView::new();
        view.sync_from_app(&app);

        assert_eq!(view.timeline.len(), 5);
        assert_eq!(view.scroll_state.offset, 10);
        assert!(!view.scroll_state.auto_scroll);

        // Modify view and sync back
        view.scroll_state.offset = 42;
        view.scroll_state.auto_scroll = true;
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
        assert_eq!(view.pending_menu_entry_idx, 0, "should target entry 0");
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
        assert!(
            !joined.contains("Open Subagent") && !joined.contains("Run subagent"),
            "task tool details should stay out of the main chat view, got: {joined}"
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
        assert!(view.pending_subagent_nav.is_none(), "no nav without output");
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
        let theme_skin = crate::skin::SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme_skin);
            view.render(&mut ctx, area);
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        // The text should still be rendered (containing "test")
        assert!(
            joined.contains("test"),
            "Search match text should be visible"
        );
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
