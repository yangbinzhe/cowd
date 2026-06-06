// ── Memory Panel ──────────────────────────────────────────────────
// Displays memory entries from App state, with layer tags and previews.
//
// Enhanced with:
//   - Layer browser (L0/L1/L2/L3/L4 filter)
//   - Search via CognitiveContextManager
//   - Entry detail view with full content + metadata
//   - Archive capability
//   - Refresh from cognitive context

#![allow(dead_code)]

use std::sync::Arc;

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use memory::cognitive::CognitiveContextManager;
use memory::types::{MemoryId, MemoryLayer};
use memory::{MemoryInformationState, MemoryKernel, MemoryTurnContext};

use crate::tui::app::{App, MemoryEntry};
use crate::tui::components::{Component, EventResult, RenderContext};

// ── Layer Filter ─────────────────────────────────────────────────

/// Memory layer used for filtering entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    L0,
    L1,
    L2,
    L3,
    L4,
}

impl Layer {
    /// All layers in order.
    pub fn all() -> [Layer; 5] {
        [Layer::L0, Layer::L1, Layer::L2, Layer::L3, Layer::L4]
    }

    /// Human-readable layer label.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Layer::L0 => "L0",
            Layer::L1 => "L1",
            Layer::L2 => "L2",
            Layer::L3 => "L3",
            Layer::L4 => "L4",
        }
    }

    /// Parse layer from string.
    #[must_use]
    pub fn from_str(s: &str) -> Option<Layer> {
        match s {
            "L0" => Some(Layer::L0),
            "L1" => Some(Layer::L1),
            "L2" => Some(Layer::L2),
            "L3" => Some(Layer::L3),
            "L4" => Some(Layer::L4),
            _ => None,
        }
    }

    /// Short description of what this layer stores.
    #[must_use]
    pub fn description(&self) -> &'static str {
        match self {
            Layer::L0 => "Identity / Global Facts",
            Layer::L1 => "Essential Working Memory",
            Layer::L2 => "Project Conventions & Decisions",
            Layer::L3 => "Deep Long-term Knowledge",
            Layer::L4 => "Shared / Team Memory",
        }
    }
}

impl std::fmt::Display for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ── View Mode ────────────────────────────────────────────────────

/// Current interaction mode of the memory panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    /// Normal list browsing.
    List,
    /// Viewing full details of a selected entry.
    Detail,
    /// Typing a search query.
    Search,
}

// ── MemoryPanel ──────────────────────────────────────────────────

/// Panel showing memory entries with layer-tagged previews.
///
/// Supports:
/// - Layer filtering (cycle through L0–L4 or show all)
/// - Full-text search via CognitiveContextManager
/// - Detail view for individual entries
/// - Archive selected entries
/// - Keyboard navigation with j/k, Enter, d, /, Esc
pub struct MemoryPanel {
    pub entries: Vec<MemoryEntry>,
    /// Optional reference to the memory manager for cognitive operations.
    pub memory_manager: Option<Arc<CognitiveContextManager>>,
    /// Active layer filter (None = show all).
    pub active_layer: Option<Layer>,
    /// Currently selected entry index (0-based).
    pub selected_entry: Option<usize>,
    /// Search query text (when in Search mode).
    pub search_query: String,
    /// Whether the search bar should capture keyboard input.
    search_active: bool,
    /// Current view mode.
    view_mode: ViewMode,
    /// Scroll offset for detail view and long lists.
    pub scroll_offset: u16,
    /// Status message to display (e.g., "Archived", "Refreshed").
    status_message: Option<String>,
    /// Tick counter for auto-clearing status messages.
    status_ticks: u32,
}

impl MemoryPanel {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            memory_manager: None,
            active_layer: None,
            selected_entry: None,
            search_query: String::new(),
            search_active: false,
            view_mode: ViewMode::List,
            scroll_offset: 0,
            status_message: None,
            status_ticks: 0,
        }
    }

    /// Pull the latest memory entries from App state.
    pub fn sync_from_app(&mut self, app: &App) {
        self.entries = app.memory_entries.clone();
    }

    /// Create a MemoryPanel pre-populated from App state.
    pub fn from_app(app: &App) -> Self {
        let mut mp = Self::new();
        mp.sync_from_app(app);
        mp
    }

    /// Attach a memory manager for cognitive operations.
    pub fn with_memory_manager(mut self, mm: Arc<CognitiveContextManager>) -> Self {
        self.memory_manager = Some(mm);
        self
    }

    /// Set the memory manager reference.
    pub fn set_memory_manager(&mut self, mm: Arc<CognitiveContextManager>) {
        self.memory_manager = Some(mm);
    }

    /// Refresh entries from the cognitive context manager.
    ///
    /// If a layer filter is active, only entries from that layer are fetched.
    /// Otherwise, all entries are listed.
    pub fn sync_from_cognitive(&mut self) {
        let Some(ref mm) = self.memory_manager else {
            return;
        };

        let fresh: Vec<MemoryEntry> = if let Some(layer) = self.active_layer {
            let mlayer = layer_to_memory_layer(layer);
            match memory_layer_view_blocking(Arc::clone(mm), mlayer) {
                Ok(view) => view
                    .atoms
                    .into_iter()
                    .map(memory_atom_to_tui_entry)
                    .collect(),
                Err(err) => {
                    self.set_status(&format!("Refresh failed: {err}"));
                    return;
                }
            }
        } else {
            match memory_layer_views_blocking(Arc::clone(mm)) {
                Ok(views) => views
                    .into_iter()
                    .flat_map(|view| view.atoms.into_iter().map(memory_atom_to_tui_entry))
                    .collect(),
                Err(err) => {
                    self.set_status(&format!("Refresh failed: {err}"));
                    return;
                }
            }
        };

        self.entries = fresh;
        self.selected_entry = self.selected_entry_filtered();
        self.scroll_offset = 0;
        self.set_status("Entries refreshed");
    }

    /// Execute a search query via the cognitive context manager.
    fn execute_search(&mut self, query: &str) {
        let Some(ref mm) = self.memory_manager else {
            self.set_status("No memory manager attached");
            return;
        };
        if query.is_empty() {
            self.sync_from_cognitive();
            return;
        }

        match search_memory_blocking(Arc::clone(mm), query.to_string()) {
            Ok(results) => {
                let count = results.len();
                self.entries = results
                    .into_iter()
                    .map(|e| MemoryEntry {
                        id: Some(e.id.to_string()),
                        layer: format!("{:?}", e.layer),
                        content: e.content,
                        priority: format!("{:?}", e.priority),
                    })
                    .collect();
                self.selected_entry = self.selected_entry_filtered();
                self.scroll_offset = 0;
                self.set_status(&format!("Found {count} results for \"{query}\""));
            }
            Err(err) => {
                self.set_status(&format!("Search failed: {err}"));
            }
        }
    }

    /// Archive the currently selected entry in the kernel and remove it from active view.
    fn delete_selected(&mut self) {
        let Some(idx) = self.selected_entry else {
            return;
        };

        let entry_id = self.entries.get(idx).and_then(|e| e.id.clone());

        if let Some(ref mm) = self.memory_manager {
            if let Some(ref id) = entry_id {
                if let Err(err) = delete_memory_entry_blocking(Arc::clone(mm), id.clone()) {
                    self.set_status(&format!("Archive failed in store: {err}"));
                }
            }
        }

        self.remove_entry_at(idx);
        self.set_status("Entry archived");
    }

    fn remove_entry_at(&mut self, idx: usize) {
        if idx < self.entries.len() {
            self.entries.remove(idx);
        }

        self.selected_entry = if self.entries.is_empty() {
            None
        } else if idx >= self.entries.len() {
            Some(self.entries.len().saturating_sub(1))
        } else {
            Some(idx)
        };
    }

    /// Enter search mode.
    fn begin_search(&mut self) {
        self.search_query.clear();
        self.search_active = true;
        self.view_mode = ViewMode::Search;
    }

    /// Cancel search mode (return to list).
    fn cancel_search(&mut self) {
        self.search_query.clear();
        self.search_active = false;
        self.view_mode = ViewMode::List;
        // Reset to non-filtered view
        self.sync_from_cognitive();
    }

    /// Set a status message that auto-clears after a few frames.
    fn set_status(&mut self, msg: &str) {
        self.status_message = Some(msg.to_string());
        self.status_ticks = 0;
    }

    /// Clamp selected_entry to the current entries length.
    fn selected_entry_filtered(&self) -> Option<usize> {
        self.selected_entry.and_then(|i| {
            if self.entries.is_empty() {
                None
            } else {
                Some(i.min(self.entries.len().saturating_sub(1)))
            }
        })
    }

    // ── Rendering helpers ────────────────────────────────────────────

    /// Build the title string for the block border.
    fn build_title(&self) -> String {
        let mut title = String::from(" Memory ");
        if let Some(layer) = self.active_layer {
            title.push_str(&format!("[{}] ", layer));
        } else {
            title.push_str("[All] ");
        }
        if self.view_mode == ViewMode::Search {
            title.push_str("(searching) ");
        }
        if let Some(ref msg) = self.status_message {
            title.push_str(msg);
        }
        title
    }

    /// Render the normal list view.
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

        if self.entries.is_empty() {
            lines.push(Line::from(Span::styled(
                "No memory entries loaded.",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Memory system: 5-layer (L0 Identity → L4 Shared)",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::from(Span::styled(
                "Keys: / search  l/L layer filter  r refresh  j↓ k↑  Enter view  d archive",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            let total = self.entries.len();
            lines.push(Line::from(Span::styled(
                format!("{total} entries active | j↓ k↑ select | Enter view | d archive | / search | l/L layer | r refresh"),
                Style::default().fg(Color::Yellow),
            )));
            lines.push(Line::raw(""));

            let visible_start = self.scroll_offset as usize;
            let max_visible = inner_height.saturating_sub(4); // top header + bottom padding
            let visible_end = (visible_start + max_visible).min(total);

            for i in visible_start..visible_end {
                let entry = &self.entries[i];
                let is_selected = self.selected_entry == Some(i);

                let cursor = if is_selected { " > " } else { "   " };
                let (cursor_style, layer_style, content_style) = if is_selected {
                    (
                        Style::default().fg(Color::Green),
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    )
                } else {
                    (
                        Style::default().fg(Color::DarkGray),
                        Style::default().fg(Color::Cyan),
                        Style::default().fg(Color::Gray),
                    )
                };

                let icon = match entry.priority.as_str() {
                    "High" | "high" | "Critical" => "🔴",
                    "Medium" | "Normal" | "normal" | "medium" => "🟡",
                    _ => "⚪",
                };

                let content_preview = if entry.content.len() > inner_width.saturating_sub(12) {
                    let max_len = inner_width.saturating_sub(12);
                    format!("{}...", &entry.content[..max_len.min(entry.content.len())])
                } else {
                    entry.content.clone()
                };

                lines.push(Line::from(vec![
                    Span::styled(cursor, cursor_style),
                    Span::styled(format!(" {icon} [{}] ", entry.layer), layer_style),
                    Span::styled(content_preview, content_style),
                ]));
            }
        }

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    /// Render entry detail view.
    fn render_detail(&mut self, ctx: &mut RenderContext, area: Rect, block: &Block) {
        let title = self.build_title();
        let block = block.clone().title(title);

        let inner_width = area.width.saturating_sub(4) as usize;
        let inner_height = area.height.saturating_sub(2) as usize;

        if inner_height < 5 || self.selected_entry.is_none() {
            let para = Paragraph::new("Detail area too small").block(block);
            ctx.frame_mut().render_widget(para, area);
            return;
        }

        let idx = self.selected_entry.unwrap();
        let Some(entry) = self.entries.get(idx) else {
            let para = Paragraph::new("Entry not found").block(block);
            ctx.frame_mut().render_widget(para, area);
            return;
        };

        let mut lines: Vec<Line> = Vec::new();

        // Header info
        lines.push(Line::from(Span::styled(
            format!(" Layer: {}  |  Priority: {}", entry.layer, entry.priority),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        if let Some(ref id) = entry.id {
            lines.push(Line::from(Span::styled(
                format!(" ID: {id}"),
                Style::default().fg(Color::DarkGray),
            )));
        }
        lines.push(Line::styled(
            "─".repeat(inner_width.min(60)),
            Style::default().fg(Color::DarkGray),
        ));
        lines.push(Line::raw(""));

        // Content (scrollable)
        let content_lines: Vec<&str> = entry.content.lines().collect();
        let offset = self.scroll_offset as usize;
        let max_content_lines = inner_height.saturating_sub(lines.len() + 2);
        let visible_content = &content_lines[offset.min(content_lines.len())
            ..(offset + max_content_lines).min(content_lines.len())];

        for line in visible_content {
            // Wrap long lines
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

        if offset + max_content_lines < content_lines.len() {
            lines.push(Line::styled(
                format!(
                    "... {} more lines (scroll with j/k)",
                    content_lines.len() - offset - max_content_lines
                ),
                Style::default().fg(Color::DarkGray),
            ));
        }

        // Footer
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            " Esc:back  j↓/k↑:scroll  d:archive ",
            Style::default().fg(Color::Yellow),
        )));

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    /// Render search input overlay (rendered below the list).
    fn render_search_bar(&mut self, ctx: &mut RenderContext, area: Rect, block: &Block) {
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

        // Search input
        let cursor = if self.search_query.is_empty() {
            "▌"
        } else {
            ""
        };
        lines.push(Line::from(Span::styled(
            format!(" Search: {}{}", self.search_query, cursor),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            " Enter:execute  Esc:cancel  Backspace:delete",
            Style::default().fg(Color::DarkGray),
        )));

        // Show results list (if search executed)
        if !self.entries.is_empty() && self.search_query.is_empty() {
            // Already showing results from a prior search execution
        } else if !self.search_query.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Press Enter to search...",
                Style::default().fg(Color::Yellow),
            )));
        }

        // Show entries if available
        if !self.entries.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                "─".repeat(inner_width.min(60)),
                Style::default().fg(Color::DarkGray),
            ));
            let visible_start = self.scroll_offset as usize;
            let max_visible = inner_height.saturating_sub(lines.len() + 1);
            let visible_end = (visible_start + max_visible).min(self.entries.len());

            for i in visible_start..visible_end {
                let entry = &self.entries[i];
                let is_selected = self.selected_entry == Some(i);
                let cursor = if is_selected { ">" } else { " " };
                lines.push(Line::from(vec![
                    Span::styled(
                        format!(" {cursor} [{}] ", entry.layer),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled(
                        if entry.content.len() > inner_width.saturating_sub(8) {
                            &entry.content[..inner_width.saturating_sub(8)]
                        } else {
                            &entry.content
                        },
                        if is_selected {
                            Style::default()
                                .fg(Color::White)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::Gray)
                        },
                    ),
                ]));
            }
        }

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    // ── Event handlers ──────────────────────────────────────────────

    /// Handle key events in normal list mode.
    fn handle_list_key(&mut self, key: &crossterm::event::KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                let next = self
                    .selected_entry
                    .map_or(0, |i| (i + 1).min(self.entries.len().saturating_sub(1)));
                if !self.entries.is_empty() {
                    self.selected_entry = Some(next);
                }
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if !self.entries.is_empty() {
                    self.selected_entry =
                        Some(self.selected_entry.map_or(0, |i| i.saturating_sub(1)));
                }
                EventResult::Consumed
            }
            KeyCode::Char('g') => {
                // Go to top
                if !self.entries.is_empty() {
                    self.selected_entry = Some(0);
                    self.scroll_offset = 0;
                }
                EventResult::Consumed
            }
            KeyCode::Char('G') => {
                // Go to bottom (shift-g)
                if !self.entries.is_empty() {
                    self.selected_entry = Some(self.entries.len() - 1);
                }
                EventResult::Consumed
            }
            KeyCode::Enter => {
                if self.selected_entry.is_some() {
                    self.view_mode = ViewMode::Detail;
                    self.scroll_offset = 0;
                }
                EventResult::Consumed
            }
            KeyCode::Char('d') => {
                if self.selected_entry.is_some() {
                    self.delete_selected();
                    if self.view_mode == ViewMode::Detail {
                        self.view_mode = ViewMode::List;
                    }
                }
                EventResult::Consumed
            }
            KeyCode::Char('/') => {
                self.begin_search();
                EventResult::Consumed
            }
            KeyCode::Char('l') => {
                // Cycle layer forward
                self.cycle_layer_forward();
                self.sync_from_cognitive();
                EventResult::Consumed
            }
            KeyCode::Char('L') => {
                // Cycle layer backward
                self.cycle_layer_backward();
                self.sync_from_cognitive();
                EventResult::Consumed
            }
            KeyCode::Char('0') => {
                // Clear layer filter (show all)
                self.active_layer = None;
                self.sync_from_cognitive();
                EventResult::Consumed
            }
            KeyCode::Char('r') => {
                self.sync_from_cognitive();
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    /// Handle key events in detail view mode.
    fn handle_detail_key(&mut self, key: &crossterm::event::KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Esc => {
                self.view_mode = ViewMode::List;
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
            KeyCode::Char('d') => {
                self.delete_selected();
                self.view_mode = ViewMode::List;
                EventResult::Consumed
            }
            KeyCode::Char('r') => {
                self.sync_from_cognitive();
                if self
                    .selected_entry
                    .map_or(true, |i| i >= self.entries.len())
                {
                    self.view_mode = ViewMode::List;
                }
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    /// Handle key events in search mode.
    fn handle_search_key(&mut self, key: &crossterm::event::KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Esc => {
                self.cancel_search();
                EventResult::Consumed
            }
            KeyCode::Enter => {
                let query = self.search_query.clone();
                self.search_active = false;
                self.view_mode = ViewMode::List;
                self.execute_search(&query);
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

    /// Cycle layer filter forward (None → L0 → L1 → ... → L4 → None).
    fn cycle_layer_forward(&mut self) {
        self.active_layer = match self.active_layer {
            None => Some(Layer::L0),
            Some(Layer::L0) => Some(Layer::L1),
            Some(Layer::L1) => Some(Layer::L2),
            Some(Layer::L2) => Some(Layer::L3),
            Some(Layer::L3) => Some(Layer::L4),
            Some(Layer::L4) => None,
        };
    }

    /// Cycle layer filter backward (None → L4 → L3 → ... → L0 → None).
    fn cycle_layer_backward(&mut self) {
        self.active_layer = match self.active_layer {
            None => Some(Layer::L4),
            Some(Layer::L4) => Some(Layer::L3),
            Some(Layer::L3) => Some(Layer::L2),
            Some(Layer::L2) => Some(Layer::L1),
            Some(Layer::L1) => Some(Layer::L0),
            Some(Layer::L0) => None,
        };
    }

    // ── Legacy draw API ────────────────────────────────────────────

    /// Legacy draw API used by `render.rs` for direct frame access.
    ///
    /// This is a **static** method that renders memory entries from the
    /// app state. It does not participate in the component system and
    /// is preserved for backward compatibility.
    pub fn draw(frame: &mut ratatui::Frame, area: Rect, app: &App) {
        let mut lines: Vec<Line> = Vec::new();
        if app.memory_entries.is_empty() {
            lines.push(Line::from(Span::styled(
                "No memory entries loaded.",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Memory system: 5-layer (L0 Identity / L1 Essential / L2 Project / L3 Deep / L4 Shared)",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::from(Span::styled(
                "Auto-extraction: background async, zero token cost",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for entry in &app.memory_entries {
                let icon = match entry.priority.as_str() {
                    "high" => "🔴",
                    "medium" => "🟡",
                    _ => "⚪",
                };
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{icon} [{}] ", entry.layer),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled(&entry.content, Style::default().fg(Color::White)),
                ]));
            }
        }
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            area,
        );
    }
}

// ── Helpers ──────────────────────────────────────────────────────

/// Convert our local Layer enum to memory's MemoryLayer.
fn layer_to_memory_layer(layer: Layer) -> MemoryLayer {
    match layer {
        Layer::L0 => MemoryLayer::L0,
        Layer::L1 => MemoryLayer::L1,
        Layer::L2 => MemoryLayer::L2,
        Layer::L3 => MemoryLayer::L3,
        Layer::L4 => MemoryLayer::L4,
    }
}

fn memory_atom_to_tui_entry(atom: memory::MemoryAtomView) -> MemoryEntry {
    MemoryEntry {
        id: Some(atom.id.to_string()),
        layer: format!("{:?}", atom.layer),
        content: atom.title,
        priority: format!("{:?}", atom.state),
    }
}

fn memory_layer_view_blocking(
    manager: Arc<CognitiveContextManager>,
    layer: MemoryLayer,
) -> Result<memory::MemoryLayerView, String> {
    run_memory_operation_blocking(move || async move {
        MemoryKernel::new(manager)
            .layer_view(layer, MemoryInformationState::Orientation)
            .await
    })
}

fn memory_layer_views_blocking(
    manager: Arc<CognitiveContextManager>,
) -> Result<Vec<memory::MemoryLayerView>, String> {
    run_memory_operation_blocking(move || async move {
        MemoryKernel::new(manager)
            .layer_views(MemoryInformationState::Orientation)
            .await
    })
}

fn search_memory_blocking(
    manager: Arc<CognitiveContextManager>,
    query: String,
) -> Result<Vec<memory::MemoryEntry>, String> {
    run_memory_operation_blocking(move || async move { manager.search(&query).await })
}

fn delete_memory_entry_blocking(
    manager: Arc<CognitiveContextManager>,
    id: String,
) -> Result<(), String> {
    let memory_id = MemoryId::try_parse(&id).map_err(|_| "invalid memory id".to_string())?;
    run_memory_operation_blocking(move || async move {
        let ctx = MemoryTurnContext::new("tui-memory-archive", "tui");
        MemoryKernel::new(manager)
            .archive(&ctx, memory_id, "archived from TUI memory panel")
            .await
    })
}

fn run_memory_operation_blocking<F, Fut, T, E>(operation: F) -> Result<T, String>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let run = move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| format!("build runtime: {err}"))?;
        rt.block_on(operation()).map_err(|err| err.to_string())
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(run)
            .join()
            .map_err(|_| "memory worker panicked".to_string())?
    } else {
        run()
    }
}

// ── Default impl ─────────────────────────────────────────────────

impl Default for MemoryPanel {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component Trait ──────────────────────────────────────────────

impl Component for MemoryPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        // Tick status message auto-clear
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

        // Global hotkeys (work in any mode)
        if key.code == KeyCode::Esc && self.view_mode != ViewMode::Search {
            // In list/detail mode: deselect
            self.view_mode = ViewMode::List;
            self.selected_entry = None;
            self.scroll_offset = 0;
            self.search_active = false;
            self.search_query.clear();
            return EventResult::Consumed;
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
        "memory_panel"
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    fn test_memory_config(path: &std::path::Path) -> memory::MemoryConfig {
        let mut config = memory::MemoryConfig::default();
        config.store.sqlite_path = path.to_path_buf();
        config.store.blob_dir = path.parent().unwrap().join("blobs");
        config
    }

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn seeded_memory_manager() -> Arc<CognitiveContextManager> {
        let dir = unique_temp_dir("cowd-memory-panel");
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&dir.join("memory.db")))
                .await
                .unwrap(),
        );
        manager
            .create_entry(
                memory::MemoryLayer::L4,
                memory::MemoryCategory::Shared,
                "Runtime Safe Memory Panel",
                "TUI memory panel must read real memory while running inside Tokio.",
                memory::Priority::High,
                vec!["tui".into(), "memory".into()],
                memory::MemoryScope::Global,
            )
            .await
            .unwrap();
        manager
    }

    fn render_panel(panel: &mut MemoryPanel, width: u16, height: u16) -> Vec<String> {
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
        let mut panel = MemoryPanel::new();
        let lines = render_panel(&mut panel, 50, 5);
        let joined = lines.join("\n");
        assert!(
            joined.contains("No memory entries loaded"),
            "Empty state should show placeholder, got: {joined}"
        );
    }

    #[test]
    fn shows_memory_entries_with_layer_tag() {
        let mut panel = MemoryPanel::new();
        panel.entries = vec![
            MemoryEntry {
                id: None,
                layer: "core".into(),
                content: "User prefers TypeScript over JavaScript".into(),
                priority: "high".into(),
            },
            MemoryEntry {
                id: None,
                layer: "session".into(),
                content: "Currently working on the TUI component system".into(),
                priority: "normal".into(),
            },
        ];

        let lines = render_panel(&mut panel, 80, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("2 entries"),
            "Should show entry count, got: {joined}"
        );
        assert!(
            joined.contains("[core]"),
            "Should show core layer tag, got: {joined}"
        );
        assert!(
            joined.contains("[session]"),
            "Should show session layer tag, got: {joined}"
        );
    }

    #[test]
    fn entry_preview_in_list() {
        let mut panel = MemoryPanel::new();
        let long = "a".repeat(200);
        panel.entries = vec![MemoryEntry {
            id: None,
            layer: "test".into(),
            content: long.clone(),
            priority: "low".into(),
        }];

        let lines = render_panel(&mut panel, 100, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("..."),
            "Long entries should show ellipsis, got: {joined}"
        );
    }

    #[test]
    fn sync_from_app_populates_entries() {
        let mut app = App::new("test-model", "test-session");
        app.memory_entries = vec![MemoryEntry {
            id: None,
            layer: "core".into(),
            content: "test memory".into(),
            priority: "high".into(),
        }];

        let mut panel = MemoryPanel::new();
        panel.sync_from_app(&app);
        assert_eq!(panel.entries.len(), 1);
        assert_eq!(panel.entries[0].layer, "core");
        assert_eq!(panel.entries[0].content, "test memory");
    }

    #[tokio::test]
    async fn sync_from_cognitive_reads_entries_inside_tokio_runtime() {
        let manager = seeded_memory_manager().await;
        let mut panel = MemoryPanel::new().with_memory_manager(manager);

        panel.sync_from_cognitive();

        assert!(
            panel
                .entries
                .iter()
                .any(|entry| entry.content == "Runtime Safe Memory Panel"),
            "memory panel should refresh real memory entries inside an active Tokio runtime"
        );
    }

    #[tokio::test]
    async fn execute_search_reads_entries_inside_tokio_runtime() {
        let manager = seeded_memory_manager().await;
        let mut panel = MemoryPanel::new().with_memory_manager(manager);

        panel.execute_search("Tokio");

        assert!(
            panel
                .entries
                .iter()
                .any(|entry| entry.content.contains("Tokio")),
            "memory panel should search real memory entries inside an active Tokio runtime"
        );
    }

    #[test]
    fn component_trait_methods() {
        let panel = MemoryPanel::new();
        assert!(panel.focusable());
        assert_eq!(panel.id(), "memory_panel");
    }

    #[test]
    fn from_app_constructor() {
        let mut app = App::new("test", "s");
        app.memory_entries = vec![MemoryEntry {
            id: None,
            layer: "a".into(),
            content: "b".into(),
            priority: "c".into(),
        }];
        let panel = MemoryPanel::from_app(&app);
        assert_eq!(panel.entries.len(), 1);
    }

    #[test]
    fn layer_enum_values() {
        assert_eq!(Layer::L0.as_str(), "L0");
        assert_eq!(Layer::L1.as_str(), "L1");
        assert_eq!(Layer::L4.as_str(), "L4");
        assert_eq!(Layer::from_str("L2"), Some(Layer::L2));
        assert_eq!(Layer::from_str("INVALID"), None);
    }

    #[test]
    fn layer_cycle_forward() {
        let mut panel = MemoryPanel::new();
        assert_eq!(panel.active_layer, None);

        panel.cycle_layer_forward();
        assert_eq!(panel.active_layer, Some(Layer::L0));

        for _ in 0..4 {
            panel.cycle_layer_forward();
        }
        assert_eq!(panel.active_layer, Some(Layer::L4));

        panel.cycle_layer_forward();
        assert_eq!(panel.active_layer, None);
    }

    #[test]
    fn layer_cycle_backward() {
        let mut panel = MemoryPanel::new();
        panel.cycle_layer_backward();
        assert_eq!(panel.active_layer, Some(Layer::L4));

        panel.cycle_layer_backward();
        assert_eq!(panel.active_layer, Some(Layer::L3));

        for _ in 0..4 {
            panel.cycle_layer_backward();
        }
        assert_eq!(panel.active_layer, None);
    }

    #[test]
    fn detail_view_mode_activated_on_enter() {
        let mut panel = MemoryPanel::new();
        panel.entries = vec![MemoryEntry {
            id: Some("abc-123".into()),
            layer: "L1".into(),
            content: "Entry content".into(),
            priority: "high".into(),
        }];
        panel.selected_entry = Some(0);

        let key =
            crossterm::event::KeyEvent::new(KeyCode::Enter, crossterm::event::KeyModifiers::NONE);
        let result = panel.handle_list_key(&key);
        assert!(result.is_consumed());
        assert_eq!(panel.view_mode, ViewMode::Detail);
    }

    #[test]
    fn select_cycles_within_bounds() {
        let mut panel = MemoryPanel::new();
        panel.entries = vec![
            MemoryEntry {
                id: None,
                layer: "L1".into(),
                content: "a".into(),
                priority: "low".into(),
            },
            MemoryEntry {
                id: None,
                layer: "L1".into(),
                content: "b".into(),
                priority: "low".into(),
            },
        ];

        // Start with no selection
        let key_j = crossterm::event::KeyEvent::new(
            KeyCode::Char('j'),
            crossterm::event::KeyModifiers::NONE,
        );
        panel.handle_list_key(&key_j);
        assert_eq!(panel.selected_entry, Some(0));

        panel.handle_list_key(&key_j);
        assert_eq!(panel.selected_entry, Some(1));

        // Should not go out of bounds
        panel.handle_list_key(&key_j);
        assert_eq!(panel.selected_entry, Some(1));
    }

    #[test]
    fn delete_removes_entry_and_adjusts_selection() {
        let mut panel = MemoryPanel::new();
        panel.entries = vec![
            MemoryEntry {
                id: Some("1".into()),
                layer: "L1".into(),
                content: "a".into(),
                priority: "low".into(),
            },
            MemoryEntry {
                id: Some("2".into()),
                layer: "L1".into(),
                content: "b".into(),
                priority: "low".into(),
            },
            MemoryEntry {
                id: Some("3".into()),
                layer: "L1".into(),
                content: "c".into(),
                priority: "low".into(),
            },
        ];
        panel.selected_entry = Some(1);

        panel.delete_selected();
        assert_eq!(panel.entries.len(), 2);
        assert_eq!(panel.entries[0].content, "a");
        assert_eq!(panel.entries[1].content, "c");
        assert_eq!(panel.selected_entry, Some(1));
    }

    #[tokio::test]
    async fn delete_archives_entry_in_memory_kernel() {
        let manager = seeded_memory_manager().await;
        let views = MemoryKernel::new(Arc::clone(&manager))
            .layer_views(MemoryInformationState::Orientation)
            .await
            .unwrap();
        let id = views
            .into_iter()
            .flat_map(|view| view.atoms.into_iter())
            .next()
            .expect("seeded manager should contain one memory atom")
            .id;

        delete_memory_entry_blocking(Arc::clone(&manager), id.to_string()).unwrap();

        let events = MemoryKernel::new(Arc::clone(&manager))
            .lifecycle_events(id)
            .await
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.to == memory::MemoryState::Archived),
            "TUI delete key should archive through the memory kernel"
        );
        assert!(
            manager.get_entry(&id.to_string()).await.unwrap().is_some(),
            "archiving must preserve the underlying memory entry"
        );
    }

    #[test]
    fn search_mode_toggle() {
        let mut panel = MemoryPanel::new();
        panel.begin_search();
        assert_eq!(panel.view_mode, ViewMode::Search);
        assert!(panel.search_query.is_empty());

        panel.cancel_search();
        assert_eq!(panel.view_mode, ViewMode::List);
        assert!(!panel.search_active);
    }
}
