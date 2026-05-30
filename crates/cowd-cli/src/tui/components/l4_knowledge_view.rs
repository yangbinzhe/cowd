// ── L4 Knowledge View ─────────────────────────────────────────────
// Displays team-shared L4 memory entries from the MemoryOrchestrator.
//
// Features:
//   - Sync from MemoryOrchestrator::team_query() (async, block_on)
//   - Priority-based status icons with color coding
//   - Agent source and tag display
//   - Filter by source agent or tag
//   - Expand/lapse entries for full content detail
//   - L4EventBus subscription for real-time updates
//   - Keyboard navigation: j↓/k↑, Enter expand, / agent filter, t tag filter

#![allow(dead_code)]

use std::sync::Arc;

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use memory::{
    L4Event, L4EventBus, MemoryEntry, MemoryOrchestrator, MemoryScope,
};

use crate::tui::components::{Component, EventResult, RenderContext};

// ── View Mode ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    /// Normal list browsing.
    List,
    /// Viewing full detail of an expanded entry.
    Detail,
    /// Typing a filter query (agent or tag).
    Search,
}

// ── Filter Kind ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterKind {
    Agent,
    Tag,
}

// ── L4KnowledgeView ─────────────────────────────────────────────────

/// Panel displaying team-shared L4 knowledge entries from the
/// MemoryOrchestrator.
///
/// Provides:
/// - Real-time sync via `MemoryOrchestrator::team_query`
/// - Priority-based status icons (Critical=red, High=yellow, Normal=green, Low=grey)
/// - Filtering by source agent or tag
/// - Expand/collapse for full entry detail
/// - Optional L4EventBus subscription for push updates
pub struct L4KnowledgeView {
    /// Raw entries from the orchestrator.
    pub entries: Vec<MemoryEntry>,
    /// Indices into `self.entries` for the currently filtered view.
    pub filtered_entries: Vec<usize>,
    /// Index into `self.filtered_entries` of the currently selected item.
    pub selected_idx: usize,
    /// Active agent filter (None = show all).
    pub filter_agent: Option<String>,
    /// Active tag filter (None = show all).
    pub filter_tag: Option<String>,
    /// Index into `self.filtered_entries` of the currently expanded entry.
    pub expanded_entry: Option<usize>,
    /// Whether the view is visible.
    pub visible: bool,
    /// Current view mode.
    view_mode: ViewMode,
    /// Scroll offset for detail view and long lists.
    pub scroll_offset: u16,
    /// Status message to display (auto-clears).
    status_message: Option<String>,
    /// Tick counter for auto-clearing status messages.
    status_ticks: u32,
    /// Reference to the memory orchestrator.
    pub orchestrator: Option<Arc<MemoryOrchestrator>>,
    /// Optional event bus receiver for real-time L4 updates.
    pub event_rx: Option<tokio::sync::broadcast::Receiver<L4Event>>,
    /// Currently active filter kind (for search mode).
    filter_kind: Option<FilterKind>,
    /// Search query text (when in Search mode).
    search_query: String,
}

impl L4KnowledgeView {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            filtered_entries: Vec::new(),
            selected_idx: 0,
            filter_agent: None,
            filter_tag: None,
            expanded_entry: None,
            visible: false,
            view_mode: ViewMode::List,
            scroll_offset: 0,
            status_message: None,
            status_ticks: 0,
            orchestrator: None,
            event_rx: None,
            filter_kind: None,
            search_query: String::new(),
        }
    }

    /// Set the memory orchestrator reference.
    pub fn set_orchestrator(&mut self, orch: Arc<MemoryOrchestrator>) {
        self.orchestrator = Some(orch);
    }

    /// Subscribe to the L4 event bus for real-time push updates.
    ///
    /// When new L4 events arrive (insert/update/delete), the view
    /// can re-sync automatically.
    pub fn subscribe_events(&mut self, bus: &L4EventBus) {
        self.event_rx = Some(bus.subscribe());
    }

    /// Drain pending L4 events from the event bus and trigger a re-sync
    /// if any events were received.
    pub fn drain_events(&mut self) -> bool {
        let Some(ref mut rx) = self.event_rx else {
            return false;
        };
        let mut had_event = false;
        while rx.try_recv().is_ok() {
            had_event = true;
        }
        if had_event {
            self.sync();
        }
        had_event
    }

    /// Sync entries from the MemoryOrchestrator via team_query.
    ///
    /// Uses `block_on` on the current tokio runtime to call the
    /// async `team_query` method.  Gracefully handles the case where
    /// no runtime is available (e.g. during testing).
    pub fn sync(&mut self) {
        let Some(ref orch) = self.orchestrator else {
            self.set_status("No orchestrator attached");
            return;
        };
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => {
                self.set_status("No tokio runtime");
                return;
            }
        };

        let scope: Option<&MemoryScope> = None; // global scope
        match handle.block_on(orch.team_query("knowledge", scope, 40)) {
            Ok(entries) => {
                let count = entries.len();
                self.entries = entries;
                self.apply_filter();
                self.set_status(&format!("Synced {count} L4 entries"));
            }
            Err(e) => {
                self.set_status(&format!("Sync error: {e}"));
            }
        }
    }

    /// Apply the current agent and tag filters, rebuilding `filtered_entries`.
    fn apply_filter(&mut self) {
        self.filtered_entries = (0..self.entries.len())
            .filter(|&i| {
                let entry = &self.entries[i];
                let agent_ok = self
                    .filter_agent
                    .as_ref()
                    .map_or(true, |a| entry.source_agent.as_deref() == Some(a));
                let tag_ok = self
                    .filter_tag
                    .as_ref()
                    .map_or(true, |t| entry.tags.iter().any(|tag| tag == t));
                agent_ok && tag_ok
            })
            .collect();

        if self.selected_idx >= self.filtered_entries.len() {
            self.selected_idx = self.filtered_entries.len().saturating_sub(1);
        }
        self.expanded_entry = None;
        self.scroll_offset = 0;
    }

    /// Enter search mode for a specific filter kind.
    fn begin_search(&mut self, kind: FilterKind) {
        self.search_query.clear();
        self.filter_kind = Some(kind);
        self.view_mode = ViewMode::Search;
    }

    /// Execute the search/filter query.
    fn execute_search(&mut self) {
        let query = std::mem::take(&mut self.search_query);
        self.view_mode = ViewMode::List;

        if query.is_empty() {
            match self.filter_kind {
                Some(FilterKind::Agent) => {
                    self.filter_agent = None;
                    self.set_status("Agent filter cleared");
                }
                Some(FilterKind::Tag) => {
                    self.filter_tag = None;
                    self.set_status("Tag filter cleared");
                }
                None => {}
            }
        } else {
            match self.filter_kind {
                Some(FilterKind::Agent) => {
                    self.filter_agent = Some(query.clone());
                    self.set_status(&format!("Agent filter: {query}"));
                }
                Some(FilterKind::Tag) => {
                    self.filter_tag = Some(query.clone());
                    self.set_status(&format!("Tag filter: {query}"));
                }
                None => {}
            }
        }
        self.apply_filter();
    }

    /// Cancel the active search/filter.
    fn cancel_search(&mut self) {
        self.search_query.clear();
        self.view_mode = ViewMode::List;
    }

    /// Reset all filters.
    fn clear_filters(&mut self) {
        self.filter_agent = None;
        self.filter_tag = None;
        self.apply_filter();
        self.set_status("Filters cleared");
    }

    /// Set a status message that auto-clears after ~60 frames.
    fn set_status(&mut self, msg: &str) {
        self.status_message = Some(msg.to_string());
        self.status_ticks = 0;
    }

    // ── Priority helpers ──────────────────────────────────────────

    fn priority_icon(entry: &MemoryEntry) -> &'static str {
        use memory::Priority;
        match entry.priority {
            Priority::Critical => "🔴",
            Priority::High => "🟠",
            Priority::Normal => "🟢",
            Priority::Low => "⚪",
        }
    }

    fn priority_color(entry: &MemoryEntry) -> Color {
        use memory::Priority;
        match entry.priority {
            Priority::Critical => Color::Red,
            Priority::High => Color::Yellow,
            Priority::Normal => Color::Green,
            Priority::Low => Color::DarkGray,
        }
    }

    fn layer_color(entry: &MemoryEntry) -> Color {
        use memory::MemoryLayer;
        match entry.layer {
            MemoryLayer::L4 => Color::Cyan,
            _ => Color::Blue,
        }
    }

    // ── Rendering helpers ──────────────────────────────────────────

    fn build_title(&self) -> String {
        let mut title = String::from(" L4 Knowledge ");
        if let Some(ref agent) = self.filter_agent {
            title.push_str(&format!("[agent:{agent}] "));
        }
        if let Some(ref tag) = self.filter_tag {
            title.push_str(&format!("[tag:{tag}] "));
        }
        if self.view_mode == ViewMode::Search {
            title.push_str("(filter) ");
        }
        if let Some(ref msg) = self.status_message {
            title.push_str(msg);
        }
        title
    }

    /// Render the list view with entries, headers, and keyboard hints.
    fn render_list(&mut self, ctx: &mut RenderContext, area: Rect, block: &Block) {
        let title = self.build_title();
        let block = block.clone().title(title);

        let inner_width = area.width.saturating_sub(2) as usize;
        let inner_height = area.height.saturating_sub(2) as usize;

        if inner_height < 3 {
            let para = Paragraph::new("Too small").block(block);
            ctx.frame_mut().render_widget(para, area);
            return;
        }

        let mut lines: Vec<Line> = Vec::new();

        // Header bar: summary + active filters
        let filter_info = match (&self.filter_agent, &self.filter_tag) {
            (Some(a), Some(t)) => format!("agent:{a}, tag:{t}"),
            (Some(a), None) => format!("agent:{a}"),
            (None, Some(t)) => format!("tag:{t}"),
            (None, None) => "all".to_string(),
        };
        let total = self.filtered_entries.len();
        let of_total = self.entries.len();
        let header = if total == of_total {
            format!("{total} entries ({filter_info}) | j↓ k↑ select | Enter expand | a filter agent | t filter tag | r clear filters | s sync")
        } else {
            format!("{total} of {of_total} entries ({filter_info}) | j↓ k↑ select | Enter expand | a/t filter | r clear | s sync")
        };
        lines.push(Line::from(Span::styled(
            header,
            Style::default().fg(Color::Yellow),
        )));
        lines.push(Line::raw(""));

        if self.filtered_entries.is_empty() {
            lines.push(Line::from(Span::styled(
                "No matching entries. Try clearing filters (r).",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "L4: Team-shared memory — conventions, decisions, peer handoff data",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            let max_visible = inner_height.saturating_sub(lines.len() + 1);
            let visible_start = self.scroll_offset as usize;
            let visible_end = (visible_start + max_visible).min(total);

            for i in visible_start..visible_end {
                let entry_idx = self.filtered_entries[i];
                let entry = &self.entries[entry_idx];
                let is_selected = self.selected_idx == i;

                let cursor = if is_selected { " ▶ " } else { "   " };
                let (cursor_style, title_style, meta_style, tag_style) = if is_selected {
                    (
                        Style::default().fg(Color::Green),
                        Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
                        Style::default().fg(Color::Gray),
                        Style::default().fg(Color::Cyan),
                    )
                } else {
                    (
                        Style::default().fg(Color::DarkGray),
                        Style::default().fg(Color::White),
                        Style::default().fg(Color::DarkGray),
                        Style::default().fg(Color::Blue),
                    )
                };

                let icon = Self::priority_icon(entry);
                let pcolor = Self::priority_color(entry);
                let lcolor = Self::layer_color(entry);

                // Source agent (truncated)
                let agent = entry.source_agent.as_deref().unwrap_or("?");
                let agent_short = if agent.len() > 12 {
                    format!("{}..", &agent[..10])
                } else {
                    agent.to_string()
                };

                // Title (truncated)
                let avail_title = inner_width.saturating_sub(30);
                let title_text = if entry.title.len() > avail_title {
                    format!("{}…", &entry.title[..avail_title.saturating_sub(1)])
                } else {
                    entry.title.clone()
                };

                // Tags (compact)
                let tag_text = if entry.tags.is_empty() {
                    String::new()
                } else {
                    let tags: Vec<String> = entry.tags.iter().take(3).map(|t| format!("#{t}")).collect();
                    let mut t = tags.join(" ");
                    if entry.tags.len() > 3 {
                        t.push_str(" …");
                    }
                    t
                };

                // Confidence badge
                let conf = entry.confidence;
                let conf_str = if conf >= 0.9 { "★★★" } else if conf >= 0.7 { "★★" } else if conf >= 0.5 { "★" } else { "·" };

                lines.push(Line::from(vec![
                    Span::styled(cursor, cursor_style),
                    Span::styled(format!("{icon} "), Style::default().fg(pcolor)),
                    Span::styled(format!("[{}]", agent_short), meta_style),
                    Span::styled(format!(" {conf_str} "), Style::default().fg(lcolor)),
                    Span::styled(title_text, title_style),
                ]));

                if !tag_text.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("       ", cursor_style),
                        Span::styled(tag_text, tag_style),
                    ]));
                }
            }

            if visible_end < total {
                lines.push(Line::styled(
                    format!("... {} more entries (j/k to scroll)", total - visible_end),
                    Style::default().fg(Color::DarkGray),
                ));
            }
        }

        // Keyboard hint bar
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "a:agent filter  t:tag filter  r:clear filters  s:sync  /:free text  Esc:close",
            Style::default().fg(Color::DarkGray),
        )));

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    /// Render the detail view for an expanded entry.
    fn render_detail(&mut self, ctx: &mut RenderContext, area: Rect, block: &Block) {
        let title = self.build_title();
        let block = block.clone().title(title);

        let inner_width = area.width.saturating_sub(4) as usize;
        let inner_height = area.height.saturating_sub(2) as usize;

        if inner_height < 5 || self.expanded_entry.is_none() {
            let para = Paragraph::new("Detail area too small or no entry selected").block(block);
            ctx.frame_mut().render_widget(para, area);
            return;
        }

        let entry_idx = self.filtered_entries[self.expanded_entry.unwrap()];
        let entry = &self.entries[entry_idx];

        let mut lines: Vec<Line> = Vec::new();

        // Header
        let icon = Self::priority_icon(entry);
        let pcolor = Self::priority_color(entry);
        lines.push(Line::from(vec![
            Span::styled(format!("{icon} "), Style::default().fg(pcolor)),
            Span::styled(
                &entry.title,
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
        ]));
        lines.push(Line::raw(""));

        // Metadata block
        lines.push(Line::from(Span::styled(
            format!(" ID: {}  |  Layer: {:?}  |  Category: {:?}  |  Confidence: {:.0}%",
                entry.id, entry.layer, entry.category, entry.confidence * 100.0),
            Style::default().fg(Color::DarkGray),
        )));
        let agent = entry.source_agent.as_deref().unwrap_or("?");
        lines.push(Line::from(Span::styled(
            format!(" Source: {:?}  |  Agent: {agent}  |  Priority: {:?}  |  Scope: {}",
                entry.source, entry.priority, entry.scope.scope_key()),
            Style::default().fg(Color::DarkGray),
        )));

        if !entry.tags.is_empty() {
            lines.push(Line::from(Span::styled(
                format!(" Tags: {}", entry.tags.join(", ")),
                Style::default().fg(Color::Cyan),
            )));
        }

        lines.push(Line::styled(
            "─".repeat(inner_width.min(80)),
            Style::default().fg(Color::DarkGray),
        ));
        lines.push(Line::raw(""));

        // Content (scrollable)
        let content_lines: Vec<&str> = entry.content.lines().collect();
        let offset = self.scroll_offset as usize;
        let max_content = inner_height.saturating_sub(lines.len() + 3);
        let visible = &content_lines[offset.min(content_lines.len())
            ..(offset + max_content).min(content_lines.len())];

        for line in visible {
            if line.len() > inner_width {
                let mut start = 0;
                while start < line.len() {
                    let end = (start + inner_width).min(line.len());
                    lines.push(Line::from(Span::styled(
                        &line[start..end],
                        Style::default().fg(Color::White),
                    )));
                    start = end;
                }
            } else {
                lines.push(Line::from(Span::styled(
                    *line,
                    Style::default().fg(Color::White),
                )));
            }
        }

        if offset + max_content < content_lines.len() {
            lines.push(Line::styled(
                format!("... {} more lines (j/k to scroll)", content_lines.len() - offset - max_content),
                Style::default().fg(Color::DarkGray),
            ));
        }

        // Footer
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            " Esc:back  j↓/k↑:scroll  a/t:filter",
            Style::default().fg(Color::Yellow),
        )));

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    /// Render search/filter input bar.
    fn render_search_bar(&mut self, ctx: &mut RenderContext, area: Rect, block: &Block) {
        let title = self.build_title();
        let block = block.clone().title(title);

        let inner_height = area.height.saturating_sub(2) as usize;

        if inner_height < 3 {
            let para = Paragraph::new("Too small").block(block);
            ctx.frame_mut().render_widget(para, area);
            return;
        }

        let mut lines: Vec<Line> = Vec::new();

        let kind_label = match self.filter_kind {
            Some(FilterKind::Agent) => "Agent",
            Some(FilterKind::Tag) => "Tag",
            None => "Free-text",
        };
        let cursor = if self.search_query.is_empty() { "▌" } else { "" };
        lines.push(Line::from(Span::styled(
            format!(" Filter by {kind_label}: {}{}", self.search_query, cursor),
            Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            " Enter:apply  Esc:cancel  Backspace:delete",
            Style::default().fg(Color::DarkGray),
        )));

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    // ── Event handlers ────────────────────────────────────────────

    fn handle_list_key(&mut self, key: &crossterm::event::KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                if !self.filtered_entries.is_empty() {
                    let next = (self.selected_idx + 1).min(self.filtered_entries.len() - 1);
                    self.selected_idx = next;
                    // Auto-scroll
                    let max_visible = 12usize;
                    if next >= self.scroll_offset as usize + max_visible {
                        self.scroll_offset = (next.saturating_sub(max_visible - 1)) as u16;
                    }
                }
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if !self.filtered_entries.is_empty() {
                    self.selected_idx = self.selected_idx.saturating_sub(1);
                    if self.selected_idx < self.scroll_offset as usize {
                        self.scroll_offset = self.selected_idx as u16;
                    }
                }
                EventResult::Consumed
            }
            KeyCode::Char('g') => {
                if !self.filtered_entries.is_empty() {
                    self.selected_idx = 0;
                    self.scroll_offset = 0;
                }
                EventResult::Consumed
            }
            KeyCode::Char('G') => {
                if !self.filtered_entries.is_empty() {
                    self.selected_idx = self.filtered_entries.len() - 1;
                }
                EventResult::Consumed
            }
            KeyCode::Enter => {
                if !self.filtered_entries.is_empty() {
                    self.expanded_entry = Some(self.selected_idx);
                    self.view_mode = ViewMode::Detail;
                    self.scroll_offset = 0;
                }
                EventResult::Consumed
            }
            KeyCode::Char('a') => {
                self.begin_search(FilterKind::Agent);
                EventResult::Consumed
            }
            KeyCode::Char('t') => {
                self.begin_search(FilterKind::Tag);
                EventResult::Consumed
            }
            KeyCode::Char('/') => {
                // Free-text search mode (filter by title/content)
                self.begin_search(FilterKind::Agent);
                EventResult::Consumed
            }
            KeyCode::Char('r') => {
                self.clear_filters();
                EventResult::Consumed
            }
            KeyCode::Char('s') => {
                self.sync();
                EventResult::Consumed
            }
            KeyCode::Esc => {
                self.visible = false;
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn handle_detail_key(&mut self, key: &crossterm::event::KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Esc => {
                self.view_mode = ViewMode::List;
                self.expanded_entry = None;
                self.scroll_offset = 0;
                EventResult::Consumed
            }
            KeyCode::Char('j') | KeyCode::Down => {
                self.scroll_offset = self.scroll_offset.saturating_add(1);
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
                EventResult::Consumed
            }
            KeyCode::Char('a') => {
                self.begin_search(FilterKind::Agent);
                EventResult::Consumed
            }
            KeyCode::Char('t') => {
                self.begin_search(FilterKind::Tag);
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn handle_search_key(&mut self, key: &crossterm::event::KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Esc => {
                self.cancel_search();
                EventResult::Consumed
            }
            KeyCode::Enter => {
                self.execute_search();
                EventResult::Consumed
            }
            KeyCode::Backspace => {
                self.search_query.pop();
                EventResult::Consumed
            }
            KeyCode::Char(c) => {
                self.search_query.push(c);
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }
}

// ── Default impl ────────────────────────────────────────────────────

impl Default for L4KnowledgeView {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component Trait ─────────────────────────────────────────────────

impl Component for L4KnowledgeView {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        // Tick status auto-clear
        if self.status_message.is_some() {
            self.status_ticks += 1;
            if self.status_ticks > 60 {
                self.status_message = None;
            }
        }

        let block = Block::default().borders(Borders::ALL);

        match self.view_mode {
            ViewMode::Detail => self.render_detail(ctx, area, &block),
            ViewMode::Search => self.render_search_bar(ctx, area, &block),
            ViewMode::List => self.render_list(ctx, area, &block),
        }
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        if key.kind != KeyEventKind::Press {
            return EventResult::NotConsumed;
        }

        match self.view_mode {
            ViewMode::Search => self.handle_search_key(key),
            ViewMode::Detail => self.handle_detail_key(key),
            ViewMode::List => self.handle_list_key(key),
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "l4_knowledge_view"
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::test_utils::MockTerminal;
    use crate::tui::skin::SkinConfig;

    fn make_entry(id: &str, title: &str, agent: &str) -> MemoryEntry {
        use memory::{MemoryCategory, MemoryLayer, MemorySource, Priority, AgentVisibility};
        use chrono::Utc;
        use uuid::Uuid;

        MemoryEntry {
            id: Uuid::parse_str(id).unwrap_or_else(|_| Uuid::nil()),
            layer: MemoryLayer::L4,
            category: MemoryCategory::Shared,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: title.to_string(),
            content: format!("Content for {title}"),
            embedding: None,
            tags: vec!["team".to_string()],
            relations: vec![],
            confidence: 0.85,
            access_count: 0,
            staleness: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Global,
            session_id: None,
            source_agent: Some(agent.to_string()),
            visibility: AgentVisibility::Shared,
        }
    }

    fn render_panel(panel: &mut L4KnowledgeView, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    #[test]
    fn empty_state_shows_placeholder() {
        let mut view = L4KnowledgeView::new();
        let lines = render_panel(&mut view, 50, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("No matching entries"),
            "Empty state should show placeholder, got: {joined}"
        );
    }

    #[test]
    fn shows_entries_with_agent_and_title() {
        let mut view = L4KnowledgeView::new();
        view.entries = vec![
            make_entry("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "Team Convention: Use Rust", "orchestrator"),
            make_entry("b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e", "Decision: SQLite for storage", "architect"),
        ];
        view.apply_filter();

        let lines = render_panel(&mut view, 100, 10);
        let joined = lines.join("\n");
        assert!(joined.contains("Team Convention"), "Should show title, got: {joined}");
        assert!(joined.contains("orchestrator") || joined.contains("orchest.."), "Should show agent, got: {joined}");
    }

    #[test]
    fn agent_filter_works() {
        let mut view = L4KnowledgeView::new();
        view.entries = vec![
            make_entry("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "Entry A", "agent1"),
            make_entry("b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e", "Entry B", "agent2"),
        ];
        view.filter_agent = Some("agent1".to_string());
        view.apply_filter();

        assert_eq!(view.filtered_entries.len(), 1);
        let idx = view.filtered_entries[0];
        assert_eq!(view.entries[idx].title, "Entry A");
    }

    #[test]
    fn tag_filter_works() {
        let mut view = L4KnowledgeView::new();
        let mut e1 = make_entry("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "Entry A", "agent1");
        e1.tags = vec!["team".to_string(), "rust".to_string()];
        let mut e2 = make_entry("b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e", "Entry B", "agent2");
        e2.tags = vec!["team".to_string(), "python".to_string()];
        view.entries = vec![e1, e2];
        view.filter_tag = Some("rust".to_string());
        view.apply_filter();

        assert_eq!(view.filtered_entries.len(), 1);
        let idx = view.filtered_entries[0];
        assert_eq!(view.entries[idx].title, "Entry A");
    }

    #[test]
    fn clear_filters_restores_all() {
        let mut view = L4KnowledgeView::new();
        view.entries = vec![
            make_entry("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "A", "a1"),
            make_entry("b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e", "B", "a2"),
        ];
        view.filter_agent = Some("a1".to_string());
        view.apply_filter();
        assert_eq!(view.filtered_entries.len(), 1);

        view.clear_filters();
        assert_eq!(view.filtered_entries.len(), 2);
        assert!(view.filter_agent.is_none());
        assert!(view.filter_tag.is_none());
    }

    #[test]
    fn component_trait_methods() {
        let view = L4KnowledgeView::new();
        assert!(view.focusable());
        assert_eq!(view.id(), "l4_knowledge_view");
    }

    #[test]
    fn keyboard_navigation_bounds() {
        let mut view = L4KnowledgeView::new();
        view.entries = vec![
            make_entry("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "A", "a1"),
            make_entry("b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e", "B", "a2"),
        ];
        view.apply_filter();

        // j moves down
        let key_j = crossterm::event::KeyEvent::new(KeyCode::Char('j'), crossterm::event::KeyModifiers::NONE);
        view.handle_list_key(&key_j);
        assert_eq!(view.selected_idx, 1);

        // j at bottom stays put
        view.handle_list_key(&key_j);
        assert_eq!(view.selected_idx, 1);

        // k moves up
        let key_k = crossterm::event::KeyEvent::new(KeyCode::Char('k'), crossterm::event::KeyModifiers::NONE);
        view.handle_list_key(&key_k);
        assert_eq!(view.selected_idx, 0);

        // k at top stays put
        view.handle_list_key(&key_k);
        assert_eq!(view.selected_idx, 0);
    }

    #[test]
    fn search_mode_toggles() {
        let mut view = L4KnowledgeView::new();
        view.begin_search(FilterKind::Agent);
        assert_eq!(view.view_mode, ViewMode::Search);
        assert!(view.search_query.is_empty());

        view.cancel_search();
        assert_eq!(view.view_mode, ViewMode::List);
    }

    #[test]
    fn detail_view_on_enter() {
        let mut view = L4KnowledgeView::new();
        view.entries = vec![make_entry("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "Test", "agent")];
        view.apply_filter();

        let key_enter = crossterm::event::KeyEvent::new(KeyCode::Enter, crossterm::event::KeyModifiers::NONE);
        let result = view.handle_list_key(&key_enter);
        assert!(result.is_consumed());
        assert_eq!(view.view_mode, ViewMode::Detail);
        assert_eq!(view.expanded_entry, Some(0));
    }

    #[test]
    fn detail_view_renders_entry_content() {
        let mut view = L4KnowledgeView::new();
        view.entries = vec![make_entry(
            "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
            "Detailed Entry",
            "orchestrator",
        )];
        view.apply_filter();
        view.expanded_entry = Some(0);
        view.view_mode = ViewMode::Detail;

        let lines = render_panel(&mut view, 60, 15);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Detailed Entry"),
            "Detail view should show entry title, got: {joined}"
        );
        assert!(
            joined.contains("Content for Detailed Entry") || joined.contains("Content for"),
            "Detail view should show entry content, got: {joined}"
        );
    }

    #[test]
    fn esc_from_detail_returns_to_list() {
        let mut view = L4KnowledgeView::new();
        view.entries = vec![make_entry("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "Test", "agent")];
        view.apply_filter();
        view.expanded_entry = Some(0);
        view.view_mode = ViewMode::Detail;

        let key_esc = crossterm::event::KeyEvent::new(KeyCode::Esc, crossterm::event::KeyModifiers::NONE);
        let result = view.handle_detail_key(&key_esc);
        assert!(result.is_consumed());
        assert_eq!(view.view_mode, ViewMode::List);
        assert!(view.expanded_entry.is_none());
    }

    #[test]
    fn search_executes_and_filters() {
        let mut view = L4KnowledgeView::new();
        view.entries = vec![
            make_entry("a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d", "Entry A", "agent1"),
            make_entry("b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e", "Entry B", "agent2"),
        ];
        view.apply_filter();
        view.begin_search(FilterKind::Agent);
        // Simulate typing "agent1" and pressing Enter
        let key_a = crossterm::event::KeyEvent::new(KeyCode::Char('a'), crossterm::event::KeyModifiers::NONE);
        let key_g = crossterm::event::KeyEvent::new(KeyCode::Char('g'), crossterm::event::KeyModifiers::NONE);
        let key_e = crossterm::event::KeyEvent::new(KeyCode::Char('e'), crossterm::event::KeyModifiers::NONE);
        let key_n = crossterm::event::KeyEvent::new(KeyCode::Char('n'), crossterm::event::KeyModifiers::NONE);
        let key_t = crossterm::event::KeyEvent::new(KeyCode::Char('t'), crossterm::event::KeyModifiers::NONE);
        let key_1 = crossterm::event::KeyEvent::new(KeyCode::Char('1'), crossterm::event::KeyModifiers::NONE);
        let key_enter = crossterm::event::KeyEvent::new(KeyCode::Enter, crossterm::event::KeyModifiers::NONE);

        view.handle_search_key(&key_a);
        view.handle_search_key(&key_g);
        view.handle_search_key(&key_e);
        view.handle_search_key(&key_n);
        view.handle_search_key(&key_t);
        view.handle_search_key(&key_1);
        view.handle_search_key(&key_enter);

        assert_eq!(view.view_mode, ViewMode::List);
        assert_eq!(view.filtered_entries.len(), 1);
        assert_eq!(view.filter_agent.as_deref(), Some("agent1"));
    }

    #[test]
    fn entry_title_truncation_long_text() {
        // Entry with a very long title should be truncated in render
        let long_title = "A".repeat(100);
        let mut view = L4KnowledgeView::new();
        view.entries = vec![make_entry(
            "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
            &long_title,
            "agent",
        )];
        view.apply_filter();

        let lines = render_panel(&mut view, 50, 10);
        let joined = lines.join("\n");
        // Should contain truncated version with ellipsis
        assert!(
            joined.contains("…"),
            "Long title should be truncated with ellipsis, got: {joined}"
        );
    }

    #[test]
    fn filter_on_empty_entries_is_noop() {
        let mut view = L4KnowledgeView::new();
        view.filter_agent = Some("nonexistent".to_string());
        view.apply_filter();
        assert!(view.filtered_entries.is_empty());
        assert_eq!(view.selected_idx, 0);
        // Should not panic
        let lines = render_panel(&mut view, 50, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("No matching entries"),
            "Should show empty filter message"
        );
    }
}
