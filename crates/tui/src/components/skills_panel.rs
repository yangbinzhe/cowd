// ── Skills Panel ───────────────────────────────────────────────────
// Displays categorized skill/plugin browsing with descriptions and
// management. Follows the MemoryPanel pattern for consistency with
// the TUI component system.
//
// Features:
//   - Categorized skill display (Tools, Memory, Platform, System)
//   - Active category highlighting with Tab cycling
//   - Skill enabled/disabled status rendering
//   - Search capability via / key binding
//   - Keyboard navigation (j/k, ↑/↓, g/G)
//   - Gateway-backed catalog; the TUI never invents capabilities.

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::App;
use crate::components::{Component, EventResult, RenderContext};

// ── View Mode ───────────────────────────────────────────────────────

/// Current interaction mode of the skills panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    /// Normal category/list browsing.
    List,
    /// Typing a search query.
    Search,
}

// ── SkillsPanel ─────────────────────────────────────────────────────

/// Panel displaying available agent skills/capabilities.
///
/// Supports:
/// - Category-based browsing (Tools, Memory, Platform, System)
/// - Active category highlighting with Tab cycling
/// - Skill enabled/disabled status display
/// - Search with / key binding
/// - Keyboard navigation with j/k, ↑/↓, g/G, Enter
/// - Gateway-backed truthful capability state
pub struct SkillsPanel {
    /// Skill entries projected by Gateway.
    pub entries: Vec<SkillDisplayEntry>,
    /// Pre-computed category labels in display order.
    pub categories: Vec<String>,
    /// Currently active category filter (None = show all).
    pub active_category: Option<usize>,
    /// Currently selected skill index (within displayed skills).
    pub selected_index: Option<usize>,
    /// Search query text.
    pub search_query: String,
    /// Whether search input is active.
    search_active: bool,
    /// Current view mode.
    view_mode: ViewMode,
    /// Scroll offset for long lists.
    pub scroll_offset: usize,
    /// Status message to display (auto-clears).
    status_message: Option<String>,
    /// Last Gateway skill action receipt.
    last_receipt: Option<serde_json::Value>,
    /// Compact cache health from the canonical Gateway projection.
    cache_summary: String,
    /// Tick counter for auto-clearing status messages.
    status_ticks: u32,
    /// Whether category cycling has started (avoids wrapping from None back to 0).
    category_cycle_started: bool,
}

/// Unified display entry for a skill, regardless of source.
#[derive(Debug, Clone)]
pub struct SkillDisplayEntry {
    pub id: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub enabled: bool,
    pub source: String,
    pub status: String,
    pub risk: String,
    pub tags: Vec<String>,
}

impl SkillsPanel {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            categories: Vec::new(),
            active_category: None,
            selected_index: None,
            search_query: String::new(),
            search_active: false,
            view_mode: ViewMode::List,
            scroll_offset: 0,
            status_message: None,
            last_receipt: None,
            cache_summary: String::new(),
            status_ticks: 0,
            category_cycle_started: false,
        }
    }

    /// Sync skill data from the App state.
    ///
    /// An empty catalog remains empty. It means Gateway has not returned any
    /// available Skill and must never be replaced with invented capabilities.
    pub fn sync_from_app(&mut self, app: &App) {
        self.entries = app
            .workbench
            .skill_list
            .iter()
            .map(|s| SkillDisplayEntry {
                id: s.id.clone(),
                name: s.name.clone(),
                description: s.description.clone(),
                category: s.category.clone(),
                enabled: s.installed,
                source: s.source.clone(),
                status: s.status.clone(),
                risk: s.risk.clone(),
                tags: s.tags.clone(),
            })
            .collect();
        self.categories = self
            .entries
            .iter()
            .fold(Vec::new(), |mut categories, entry| {
                if !entry.category.is_empty() && !categories.contains(&entry.category) {
                    categories.push(entry.category.clone());
                }
                categories
            });
        self.active_category = None;
        if self
            .selected_index
            .is_some_and(|index| index >= self.entries.len())
        {
            self.selected_index = None;
        }
    }

    /// Create a SkillsPanel pre-populated from App state.
    pub fn from_app(app: &App) -> Self {
        let mut sp = Self::new();
        sp.sync_from_app(app);
        sp
    }

    /// Filter entries to only those in the active category.
    fn filtered_entries(&self) -> Vec<&SkillDisplayEntry> {
        let all: Vec<&SkillDisplayEntry> = self.entries.iter().collect();
        if let Some(cat_idx) = self.active_category {
            let cat_name = self.categories.get(cat_idx);
            all.into_iter()
                .filter(|e| cat_name.is_some_and(|c| e.category == *c))
                .collect()
        } else {
            all
        }
    }

    /// Count skills per category (for category header display).
    fn category_skill_counts(&self) -> Vec<(String, usize)> {
        self.categories
            .iter()
            .map(|cat| {
                let count = self.entries.iter().filter(|e| &e.category == cat).count();
                (cat.clone(), count)
            })
            .collect()
    }

    /// Cycle to the next category.
    fn next_category(&mut self) {
        if self.categories.is_empty() {
            return;
        }
        self.active_category = match self.active_category {
            None if !self.category_cycle_started => {
                self.category_cycle_started = true;
                Some(0)
            }
            None => None,
            Some(i) if i + 1 < self.categories.len() => Some(i + 1),
            Some(_) => None,
        };
        self.selected_index = None;
        self.scroll_offset = 0;
    }

    /// Cycle to the previous category.
    fn prev_category(&mut self) {
        if self.categories.is_empty() {
            return;
        }
        self.active_category = match self.active_category {
            None if !self.category_cycle_started => {
                self.category_cycle_started = true;
                Some(self.categories.len() - 1)
            }
            None => None,
            Some(0) => None,
            Some(i) => Some(i - 1),
        };
        self.selected_index = None;
        self.scroll_offset = 0;
    }

    /// Enter search mode.
    fn begin_search(&mut self) {
        self.search_query.clear();
        self.search_active = true;
        self.view_mode = ViewMode::Search;
    }

    /// Execute search and return to list mode.
    fn execute_search(&mut self, query: &str) {
        self.search_active = false;
        self.view_mode = ViewMode::List;

        if query.is_empty() {
            self.active_category = None;
            self.selected_index = None;
            self.scroll_offset = 0;
            return;
        }

        let lower = query.to_lowercase();
        self.active_category = None;
        // Re-sort entries: matching ones first
        let mut matching: Vec<SkillDisplayEntry> = Vec::new();
        let mut non_matching: Vec<SkillDisplayEntry> = Vec::new();
        for entry in self.entries.drain(..) {
            if entry.name.to_lowercase().contains(&lower)
                || entry.description.to_lowercase().contains(&lower)
                || entry.category.to_lowercase().contains(&lower)
                || entry.source.to_lowercase().contains(&lower)
                || entry.status.to_lowercase().contains(&lower)
                || entry.risk.to_lowercase().contains(&lower)
                || entry
                    .tags
                    .iter()
                    .any(|tag| tag.to_lowercase().contains(&lower))
            {
                matching.push(entry);
            } else {
                non_matching.push(entry);
            }
        }
        self.entries = matching;
        self.entries.append(&mut non_matching);

        let match_count = self
            .entries
            .iter()
            .filter(|e| {
                e.name.to_lowercase().contains(&lower)
                    || e.description.to_lowercase().contains(&lower)
                    || e.category.to_lowercase().contains(&lower)
                    || e.source.to_lowercase().contains(&lower)
                    || e.status.to_lowercase().contains(&lower)
                    || e.risk.to_lowercase().contains(&lower)
                    || e.tags.iter().any(|tag| tag.to_lowercase().contains(&lower))
            })
            .count();

        self.selected_index = if match_count > 0 { Some(0) } else { None };
        self.scroll_offset = 0;
        self.set_status(&format!("Found {match_count} matching skills"));
    }

    /// Cancel search mode (return to list).
    fn cancel_search(&mut self) {
        self.search_query.clear();
        self.search_active = false;
        self.view_mode = ViewMode::List;

        self.set_status("Search cancelled");
    }

    /// Set a status message that auto-clears after ~60 frames.
    fn set_status(&mut self, msg: &str) {
        self.status_message = Some(msg.to_string());
        self.status_ticks = 0;
    }

    pub fn selected_skill_id(&self) -> Option<String> {
        let idx = self.selected_index?;
        let filtered = self.filtered_entries();
        filtered.get(idx).map(|entry| entry.id.clone())
    }

    pub fn record_catalog_loaded(&mut self, count: usize, projection: &serde_json::Value) {
        let cache = projection.get("cache").unwrap_or(&serde_json::Value::Null);
        self.cache_summary = format!(
            "cache {} entries / {} bytes · hits {} · misses {} · loads {} · failures {}",
            cache
                .get("resident_entries")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            cache
                .get("resident_bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            cache
                .get("hits")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            cache
                .get("misses")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            cache
                .get("loads")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            cache
                .get("failures")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
        );
        self.set_status(&format!("Gateway catalog: {count} skills"));
    }

    pub fn record_catalog_failure(&mut self, error: &str) {
        self.set_status(&format!("Gateway catalog unavailable: {error}"));
    }

    pub fn record_action_result(
        &mut self,
        action: &str,
        result: Result<serde_json::Value, String>,
    ) {
        match result {
            Ok(payload) => {
                self.last_receipt = Some(payload);
                self.set_status(&format!("skill {action}: receipt recorded"));
            }
            Err(error) => {
                self.last_receipt = None;
                self.set_status(&format!("skill {action} failed: {error}"));
            }
        }
    }

    // ── Rendering ────────────────────────────────────────────────────

    fn build_title(&self) -> String {
        let mut title = String::from(" Skills ");
        if let Some(cat_idx) = self.active_category {
            if let Some(cat) = self.categories.get(cat_idx) {
                title.push_str(&format!("[{}] ", cat));
            }
        } else {
            title.push_str("[All] ");
        }
        if self.view_mode == ViewMode::Search {
            title.push_str("(search) ");
        }
        if let Some(ref msg) = self.status_message {
            title.push_str(msg);
        }
        title
    }

    /// Render the normal list view with category headers.
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

        if self.categories.len() > 1 {
            // Show category header bar
            let cat_counts = self.category_skill_counts();
            let mut header_spans: Vec<Span> = Vec::new();
            for (i, (cat, count)) in cat_counts.iter().enumerate() {
                let is_active = self.active_category == Some(i);
                let style = if is_active {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                let prefix = if is_active { "▸ " } else { "  " };
                header_spans.push(Span::styled(format!("{prefix}{cat}({count}) "), style));
            }
            lines.push(Line::from(header_spans));
            lines.push(Line::styled(
                "─".repeat(inner_width.min(80)),
                Style::default().fg(Color::DarkGray),
            ));
        }

        let filtered = self.filtered_entries();
        if filtered.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "No skills in this category.",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Skills: installable agent capabilities with safety scanning",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            let total = filtered.len();
            let status_label = format!(
                "{total} Gateway skills | j↓ k↑ select | v validate | p plan | r run | / search"
            );
            lines.push(Line::from(Span::styled(
                status_label,
                Style::default().fg(Color::Yellow),
            )));
            lines.push(Line::raw(""));

            let visible_start = self.scroll_offset;
            let max_visible = inner_height.saturating_sub(lines.len() + 1);
            let visible_end = (visible_start + max_visible).min(total);

            for i in visible_start..visible_end {
                let entry = &filtered[i];
                let is_selected = self.selected_index == Some(i);

                let cursor = if is_selected { " > " } else { "   " };
                let (cursor_style, name_style, desc_style, status_style) = if is_selected {
                    (
                        Style::default().fg(Color::Green),
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                        Style::default().fg(Color::Gray),
                        // status icon inline
                        Style::default().fg(Color::White),
                    )
                } else {
                    (
                        Style::default().fg(Color::DarkGray),
                        Style::default().fg(Color::White),
                        Style::default().fg(Color::DarkGray),
                        Style::default().fg(Color::DarkGray),
                    )
                };

                let status_icon = if entry.enabled { "✅" } else { "⬜" };

                let mut meta = vec![
                    entry.category.clone(),
                    entry.status.clone(),
                    entry.risk.clone(),
                ];
                if !entry.source.is_empty() {
                    meta.push(entry.source.clone());
                }
                let tag_suffix = if entry.tags.is_empty() {
                    String::new()
                } else {
                    format!(
                        " [{}]",
                        entry
                            .tags
                            .iter()
                            .take(4)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                let action_suffix = entry_action_hints(entry).join(" · ");

                lines.push(Line::from(vec![
                    Span::styled(cursor, cursor_style),
                    Span::styled(format!("{status_icon} "), status_style),
                    Span::styled(&entry.name, name_style),
                    Span::styled(format!(" — {}", entry.description), desc_style),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("     "),
                    Span::styled(
                        format!("{}{}", meta.join(" · "), tag_suffix),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("     "),
                    Span::styled(
                        format!("actions: {action_suffix}"),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }

            if visible_end < total {
                lines.push(Line::styled(
                    format!("... {} more skills (j/k to scroll)", total - visible_end),
                    Style::default().fg(Color::DarkGray),
                ));
            }
        }

        // Keyboard hint bar
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "/ search  j↓ k↑  Tab category  R refresh  v validate  p plan  r run",
            Style::default().fg(Color::DarkGray),
        )));
        if !self.cache_summary.is_empty() {
            lines.push(Line::from(Span::styled(
                &self.cache_summary,
                Style::default().fg(Color::DarkGray),
            )));
        }
        if let Some(receipt) = &self.last_receipt {
            lines.push(Line::from(vec![
                Span::styled("Receipt: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    skill_receipt_summary(receipt),
                    Style::default().fg(Color::Green),
                ),
            ]));
        }

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    /// Render search input overlay.
    fn render_search_bar(&mut self, ctx: &mut RenderContext, area: Rect, block: &Block) {
        let title = self.build_title();
        let block = block.clone().title(title);

        let _inner_width = area.width.saturating_sub(2) as usize;
        let inner_height = area.height.saturating_sub(2) as usize;

        if inner_height < 3 {
            let para = Paragraph::new("Too small").block(block);
            ctx.frame_mut().render_widget(para, area);
            return;
        }

        let mut lines: Vec<Line> = Vec::new();

        // Search input field
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
            " Enter:execute  Esc:cancel  Backspace:delete  (searches name, description, category)",
            Style::default().fg(Color::DarkGray),
        )));

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    // ── Event handlers ────────────────────────────────────────────────

    fn handle_list_key(&mut self, key: &crossterm::event::KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                let filtered = self.filtered_entries();
                if !filtered.is_empty() {
                    let next = self
                        .selected_index
                        .map_or(0, |i| (i + 1).min(filtered.len().saturating_sub(1)));
                    self.selected_index = Some(next);
                    // Auto-scroll: ensure selected is visible
                    let max_visible = 10; // approximate
                    if next >= self.scroll_offset + max_visible {
                        self.scroll_offset = next.saturating_sub(max_visible - 1);
                    }
                }
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                let filtered = self.filtered_entries();
                if !filtered.is_empty() {
                    let prev = self.selected_index.map_or(0, |i| i.saturating_sub(1));
                    self.selected_index = Some(prev);
                    if prev < self.scroll_offset {
                        self.scroll_offset = prev;
                    }
                }
                EventResult::Consumed
            }
            KeyCode::Char('g') => {
                if !self.filtered_entries().is_empty() {
                    self.selected_index = Some(0);
                    self.scroll_offset = 0;
                }
                EventResult::Consumed
            }
            KeyCode::Char('G') => {
                let filtered = self.filtered_entries();
                if !filtered.is_empty() {
                    self.selected_index = Some(filtered.len() - 1);
                }
                EventResult::Consumed
            }
            KeyCode::Tab => {
                if key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::SHIFT)
                {
                    self.prev_category();
                } else {
                    self.next_category();
                }
                EventResult::Consumed
            }
            KeyCode::Char('/') => {
                self.begin_search();
                EventResult::Consumed
            }
            KeyCode::Char('v') => {
                self.report_selected_action("validate");
                EventResult::Consumed
            }
            KeyCode::Char('p') => {
                self.report_selected_action("plan");
                EventResult::Consumed
            }
            KeyCode::Char('r') => {
                self.report_selected_action("run");
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn report_selected_action(&mut self, action: &str) {
        let Some(idx) = self.selected_index else {
            self.set_status("Select a skill first");
            return;
        };
        let Some((name, supported)) = ({
            let filtered = self.filtered_entries();
            filtered.get(idx).map(|target| {
                (
                    target.name.clone(),
                    entry_action_hints(target).contains(&action),
                )
            })
        }) else {
            self.set_status("Select a skill first");
            return;
        };
        if supported {
            self.set_status(&format!("{name} {action}: unified governance action ready"));
        } else {
            self.set_status(&format!("{name} does not support {action}"));
        }
    }

    fn handle_search_key(&mut self, key: &crossterm::event::KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Esc => {
                self.cancel_search();
                EventResult::Consumed
            }
            KeyCode::Enter => {
                let query = self.search_query.clone();
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
}

// ── Helpers ─────────────────────────────────────────────────────────

fn entry_action_hints(entry: &SkillDisplayEntry) -> Vec<&'static str> {
    if entry.risk.eq_ignore_ascii_case("governed")
        || entry.category.to_ascii_lowercase().starts_with("server_")
    {
        vec!["view", "validate", "plan", "run", "maintenance"]
    } else if entry.category.eq_ignore_ascii_case("local")
        || entry.risk.eq_ignore_ascii_case("operator_review")
    {
        vec!["view", "validate", "plan", "run", "inspect", "maintenance"]
    } else {
        vec!["view", "validate", "plan"]
    }
}

fn skill_receipt_summary(receipt: &serde_json::Value) -> String {
    let run_id = receipt
        .pointer("/run/run_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            receipt
                .pointer("/receipt/run_id")
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or("unknown");
    let status = receipt
        .pointer("/receipt/status")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            receipt
                .pointer("/run/status")
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or("recorded");
    format!("{run_id} status={status}")
}

// ── Default impl ────────────────────────────────────────────────────

impl Default for SkillsPanel {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component Trait ─────────────────────────────────────────────────

impl Component for SkillsPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        // Tick status message auto-clear (~60 frames = ~2 seconds at 30fps)
        if self.status_message.is_some() {
            self.status_ticks += 1;
            if self.status_ticks > 60 {
                self.status_message = None;
            }
        }

        let block = Block::default().borders(Borders::ALL);

        match self.view_mode {
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
            ViewMode::List => self.handle_list_key(key),
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "skills_panel"
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::SkillSummary;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;
    use crossterm::event::KeyCode;

    fn render_panel(panel: &mut SkillsPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    fn app_with_catalog() -> App {
        let mut app = App::new("test-model", "test-session");
        app.workbench.skill_list = vec![
            SkillSummary {
                id: "local:release".to_string(),
                name: "release".to_string(),
                description: "Prepare release".to_string(),
                installed: true,
                category: "local".to_string(),
                source: "Project".to_string(),
                status: "ready".to_string(),
                risk: "operator_review".to_string(),
                tags: vec!["git".to_string()],
            },
            SkillSummary {
                id: "app:risk".to_string(),
                name: "risk".to_string(),
                description: "Inspect risk".to_string(),
                installed: true,
                category: "application".to_string(),
                source: "application".to_string(),
                status: "ready".to_string(),
                risk: "governed".to_string(),
                tags: vec!["analysis".to_string()],
            },
        ];
        app
    }

    #[test]
    fn new_panel_does_not_invent_gateway_skills() {
        let panel = SkillsPanel::new();
        assert!(panel.categories.is_empty());
        assert!(panel.entries.is_empty());
    }

    #[test]
    fn gateway_categories_come_from_app_catalog() {
        let panel = SkillsPanel::from_app(&app_with_catalog());
        assert_eq!(panel.categories, vec!["local", "application"]);
    }

    #[test]
    fn category_cycle() {
        let mut panel = SkillsPanel::from_app(&app_with_catalog());
        assert_eq!(panel.active_category, None);

        panel.next_category();
        assert_eq!(panel.active_category, Some(0));
        assert_eq!(panel.categories[0], "local");

        panel.next_category();
        assert_eq!(panel.active_category, Some(1));

        // Cycle back to None
        for _ in 0..panel.categories.len() {
            panel.next_category();
        }
        assert_eq!(panel.active_category, None);
    }

    #[test]
    fn category_cycle_prev() {
        let mut panel = SkillsPanel::from_app(&app_with_catalog());
        panel.prev_category();
        assert_eq!(panel.active_category, Some(panel.categories.len() - 1));

        panel.prev_category();
        assert_eq!(panel.active_category, Some(panel.categories.len() - 2));
    }

    #[test]
    fn filtered_entries_respects_category() {
        let mut panel = SkillsPanel::from_app(&app_with_catalog());
        panel.active_category = Some(0);
        let filtered = panel.filtered_entries();
        assert!(!filtered.is_empty());
        for entry in &filtered {
            assert_eq!(entry.category, "local");
        }
    }

    #[test]
    fn search_mode_toggle() {
        let mut panel = SkillsPanel::new();
        panel.begin_search();
        assert_eq!(panel.view_mode, ViewMode::Search);
        assert!(panel.search_query.is_empty());

        panel.cancel_search();
        assert_eq!(panel.view_mode, ViewMode::List);
        assert!(!panel.search_active);
    }

    #[test]
    fn search_execution_finds_matches() {
        let mut panel = SkillsPanel::from_app(&app_with_catalog());
        let before = panel.entries.len();
        panel.execute_search("release");
        let after = panel.entries.len();
        assert_eq!(before, after, "search should preserve entry count");
        // First entries should match
        assert!(panel.entries[0].name.to_lowercase().contains("release"));
    }

    #[test]
    fn component_trait_methods() {
        let panel = SkillsPanel::new();
        assert!(panel.focusable());
        assert_eq!(panel.id(), "skills_panel");
    }

    #[test]
    fn from_app_constructor() {
        let mut app = App::new("test-model", "test-session");
        app.workbench.skill_list = vec![SkillSummary {
            id: "test:skill".to_string(),
            name: "TestSkill".to_string(),
            description: "A test skill".to_string(),
            installed: true,
            category: "application".to_string(),
            source: "application".to_string(),
            status: "ready".to_string(),
            risk: "governed".to_string(),
            tags: vec!["demo".to_string()],
        }];
        let panel = SkillsPanel::from_app(&app);
        assert_eq!(panel.entries.len(), 1);
        assert_eq!(panel.entries[0].id, "test:skill");
        assert_eq!(panel.entries[0].name, "TestSkill");
    }

    #[test]
    fn governed_application_entries_render_action_hints() {
        let mut app = App::new("test-model", "test-session");
        app.workbench.skill_list = vec![SkillSummary {
            id: "app:supply-risk-analyst".to_string(),
            name: "supply-risk-analyst".to_string(),
            description: "Supply Risk Analyst".to_string(),
            installed: true,
            category: "server_manufacturing".to_string(),
            source: "application".to_string(),
            status: "ready".to_string(),
            risk: "governed".to_string(),
            tags: vec!["application".to_string()],
        }];
        let mut panel = SkillsPanel::from_app(&app);
        let lines = render_panel(&mut panel, 92, 12);
        let joined = lines.join("\n");
        assert!(joined.contains("actions: view · validate · plan · run · maintenance"));
    }

    #[test]
    fn local_entries_report_run_as_supported() {
        let mut app = App::new("test-model", "test-session");
        app.workbench.skill_list = vec![SkillSummary {
            id: "local:release".to_string(),
            name: "release".to_string(),
            description: "Prepare release".to_string(),
            installed: true,
            category: "local".to_string(),
            source: "Project".to_string(),
            status: "ready".to_string(),
            risk: "operator_review".to_string(),
            tags: vec!["git".to_string()],
        }];
        let mut panel = SkillsPanel::from_app(&app);
        panel.selected_index = Some(0);
        panel.report_selected_action("run");
        assert_eq!(
            panel.status_message.as_deref(),
            Some("release run: unified governance action ready")
        );
    }

    #[test]
    fn local_entries_key_r_triggers_run() {
        let mut app = App::new("test-model", "test-session");
        app.workbench.skill_list = vec![SkillSummary {
            id: "local:release".to_string(),
            name: "release".to_string(),
            description: "Prepare release".to_string(),
            installed: true,
            category: "local".to_string(),
            source: "Project".to_string(),
            status: "ready".to_string(),
            risk: "operator_review".to_string(),
            tags: vec!["git".to_string()],
        }];
        let mut panel = SkillsPanel::from_app(&app);
        panel.selected_index = Some(0);
        let key_r = crossterm::event::KeyEvent::new(
            KeyCode::Char('r'),
            crossterm::event::KeyModifiers::NONE,
        );
        let result = panel.handle_list_key(&key_r);
        assert!(result.is_consumed());
        assert_eq!(
            panel.status_message.as_deref(),
            Some("release run: unified governance action ready")
        );
    }

    #[test]
    fn from_app_empty_stays_truthfully_empty() {
        let app = App::new("test-model", "test-session");
        let panel = SkillsPanel::from_app(&app);
        assert!(panel.entries.is_empty());
        assert!(panel.categories.is_empty());
    }

    #[test]
    fn empty_state_renders() {
        let mut panel = SkillsPanel::new();
        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        assert!(!joined.trim().is_empty(), "should render empty state");
    }

    #[test]
    fn keyboard_navigation_does_not_panic_on_empty() {
        let mut panel = SkillsPanel::new();
        // Empty out entries
        panel.entries.clear();
        panel.categories.clear();

        let key_j = crossterm::event::KeyEvent::new(
            KeyCode::Char('j'),
            crossterm::event::KeyModifiers::NONE,
        );
        let result = panel.handle_list_key(&key_j);
        // Should not panic and should consume the event
        assert!(result.is_consumed());
    }
}
