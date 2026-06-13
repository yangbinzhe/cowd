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
//   - Self-contained built-in skill definitions fallback
//
// Built-in skill categories serve as a fallback when App does not
// provide skill_list data. Each category maps to a real cowd
// capability area visible in the plugin system and slash commands.

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::App;
use crate::tui::components::{Component, EventResult, RenderContext};

// ── View Mode ───────────────────────────────────────────────────────

/// Current interaction mode of the skills panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    /// Normal category/list browsing.
    List,
    /// Typing a search query.
    Search,
}

// ── Built-in Skill Definitions ──────────────────────────────────────

/// A statically defined skill entry for fallback display.
#[derive(Debug, Clone)]
struct BuiltinSkill {
    name: &'static str,
    description: &'static str,
    category: &'static str,
    enabled: bool,
    version: Option<&'static str>,
}

/// Built-in skill categories and their skill entries.
fn builtin_skill_categories() -> Vec<(&'static str, Vec<BuiltinSkill>)> {
    vec![
        (
            "Tools",
            vec![
                BuiltinSkill {
                    name: "Bash",
                    description: "Execute shell commands with sandboxing and approval flows",
                    category: "Tools",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "FileOps",
                    description: "Read/write workspace files with permission checks",
                    category: "Tools",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "GitExpert",
                    description: "Git operations: branch, commit, diff, rebase, bisect",
                    category: "Tools",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "LSP",
                    description: "Language server diagnostics and code navigation",
                    category: "Tools",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "WebFetch",
                    description: "Fetch and parse web content for research",
                    category: "Tools",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "GrepExpert",
                    description: "Advanced codebase search with regex and AST patterns",
                    category: "Tools",
                    enabled: true,
                    version: None,
                },
            ],
        ),
        (
            "Memory",
            vec![
                BuiltinSkill {
                    name: "CognitiveContext",
                    description: "5-layer memory: identity, essential, project, deep, shared",
                    category: "Memory",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "CrossStoreVerify",
                    description: "Verify consistency across memory stores",
                    category: "Memory",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "SessionResume",
                    description: "BM25-based session context restoration",
                    category: "Memory",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "CodeIndexer",
                    description: "Index code for semantic vector search",
                    category: "Memory",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "FactChecker",
                    description: "Fact validation and conflict detection",
                    category: "Memory",
                    enabled: true,
                    version: None,
                },
            ],
        ),
        (
            "Platform",
            vec![
                BuiltinSkill {
                    name: "Wecom",
                    description: "WeChat Work (企业微信) integration",
                    category: "Platform",
                    enabled: false,
                    version: None,
                },
                BuiltinSkill {
                    name: "Email",
                    description: "Email inbox monitoring and response",
                    category: "Platform",
                    enabled: false,
                    version: None,
                },
                BuiltinSkill {
                    name: "Feishu",
                    description: "Lark/Feishu integration",
                    category: "Platform",
                    enabled: false,
                    version: None,
                },
            ],
        ),
        (
            "System",
            vec![
                BuiltinSkill {
                    name: "MCP",
                    description: "Model Context Protocol server integration",
                    category: "System",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "Plugins",
                    description: "Plugin lifecycle: install/enable/disable/uninstall",
                    category: "System",
                    enabled: true,
                    version: Some("0.1.0"),
                },
                BuiltinSkill {
                    name: "OAuth",
                    description: "Auth provider integrations",
                    category: "System",
                    enabled: true,
                    version: None,
                },
                BuiltinSkill {
                    name: "Config",
                    description: "Configuration management and validation",
                    category: "System",
                    enabled: true,
                    version: None,
                },
            ],
        ),
    ]
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
/// - Self-contained fallback skill definitions
pub struct SkillsPanel {
    /// Skill entries (either from App or built-in fallback).
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
    pub scroll_offset: u16,
    /// Status message to display (auto-clears).
    status_message: Option<String>,
    /// Tick counter for auto-clearing status messages.
    status_ticks: u32,
    /// Whether to use built-in definitions (true when App.skill_list is empty).
    using_builtins: bool,
    /// Whether category cycling has started (avoids wrapping from None back to 0).
    category_cycle_started: bool,
    /// Optional reference to GlobalToolRegistry for real enable/disable.
    pub registry: Option<std::sync::Arc<dyn crate::tui::app::ToolRegistry>>,
}

/// Unified display entry for a skill, regardless of source.
#[derive(Debug, Clone)]
pub struct SkillDisplayEntry {
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
        let builtins = builtin_skill_categories();
        let categories: Vec<String> = builtins.iter().map(|(c, _)| (*c).to_string()).collect();
        let entries = flatten_builtin_skills(&builtins);
        Self {
            entries,
            categories,
            active_category: None,
            selected_index: None,
            search_query: String::new(),
            search_active: false,
            view_mode: ViewMode::List,
            scroll_offset: 0,
            status_message: None,
            status_ticks: 0,
            using_builtins: true,
            category_cycle_started: false,
            registry: None,
        }
    }

    /// Sync skill data from the App state.
    ///
    /// If `app.skill_list` is non-empty, those entries are used and displayed
    /// without category grouping. Otherwise, built-in skill definitions are
    /// loaded as a fallback with category browsing enabled.
    pub fn sync_from_app(&mut self, app: &App) {
        if app.skill_list.is_empty() {
            // Use built-in fallback
            let builtins = builtin_skill_categories();
            self.categories = builtins.iter().map(|(c, _)| (*c).to_string()).collect();
            self.entries = flatten_builtin_skills(&builtins);
            self.using_builtins = true;
        } else {
            self.using_builtins = false;
            // Derive categories from unique category fields if present,
            // otherwise treat all as uncategorized.
            self.entries = app
                .skill_list
                .iter()
                .map(|s| SkillDisplayEntry {
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
                    if !categories.contains(&entry.category) {
                        categories.push(entry.category.clone());
                    }
                    categories
                });
            self.active_category = None;
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
                .filter(|e| cat_name.map_or(false, |c| e.category == *c))
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

    /// Toggle the enabled state of the selected skill.
    fn toggle_selected(&mut self) {
        if let Some(idx) = self.selected_index {
            let filtered = self.filtered_entries();
            if let Some(target) = filtered.get(idx) {
                let name = target.name.clone();
                // Toggle in-place in entries
                if let Some(entry) = self.entries.iter_mut().find(|e| e.name == name) {
                    entry.enabled = !entry.enabled;
                    let new_enabled = entry.enabled;
                    let status = if new_enabled { "enabled" } else { "disabled" };
                    self.set_status(&format!("{name}: {status}"));
                    if let Some(ref registry) = self.registry {
                        if new_enabled {
                            registry.enable_tool(&name);
                        } else {
                            registry.disable_tool(&name);
                        }
                    }
                }
            }
        }
    }

    /// Set the tool registry for real enable/disable operations.
    pub fn set_registry(&mut self, registry: std::sync::Arc<dyn crate::tui::app::ToolRegistry>) {
        self.registry = Some(registry);
    }

    /// Enable the selected skill.
    fn enable_selected(&mut self) {
        self.set_selected_enabled(true);
    }

    /// Disable the selected skill.
    fn disable_selected(&mut self) {
        self.set_selected_enabled(false);
    }

    fn set_selected_enabled(&mut self, value: bool) {
        if let Some(idx) = self.selected_index {
            let filtered = self.filtered_entries();
            if let Some(target) = filtered.get(idx) {
                let name = target.name.clone();
                if let Some(entry) = self.entries.iter_mut().find(|e| e.name == name) {
                    entry.enabled = value;
                    let status = if value { "enabled" } else { "disabled" };
                    self.set_status(&format!("{name}: {status}"));
                    if let Some(ref registry) = self.registry {
                        if value {
                            registry.enable_tool(&name);
                        } else {
                            registry.disable_tool(&name);
                        }
                    }
                }
            }
        }
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

        // Reload built-in entries to restore category order
        if self.using_builtins {
            let builtins = builtin_skill_categories();
            self.entries = flatten_builtin_skills(&builtins);
        }
        self.set_status("Search cancelled");
    }

    /// Set a status message that auto-clears after ~60 frames.
    fn set_status(&mut self, msg: &str) {
        self.status_message = Some(msg.to_string());
        self.status_ticks = 0;
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

        if self.using_builtins || self.categories.len() > 1 {
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
            let status_label = if self.using_builtins {
                format!("{total} skills | j↓ k↑ select | Enter toggle | e enable | d disable | / search | Tab category")
            } else {
                format!(
                    "{total} unified skills | j↓ k↑ select | v validate | p plan | r run | w watch | / search"
                )
            };
            lines.push(Line::from(Span::styled(
                status_label,
                Style::default().fg(Color::Yellow),
            )));
            lines.push(Line::raw(""));

            let visible_start = self.scroll_offset as usize;
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
            "/ search  j↓ k↑  Tab category  Enter toggle  v validate  p plan  r run  w watch",
            Style::default().fg(Color::DarkGray),
        )));

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
                    if next >= self.scroll_offset as usize + max_visible {
                        self.scroll_offset = (next.saturating_sub(max_visible - 1)) as u16;
                    }
                }
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                let filtered = self.filtered_entries();
                if !filtered.is_empty() {
                    let prev = self.selected_index.map_or(0, |i| i.saturating_sub(1));
                    self.selected_index = Some(prev);
                    if prev < self.scroll_offset as usize {
                        self.scroll_offset = prev as u16;
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
            KeyCode::Enter => {
                self.toggle_selected();
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
            KeyCode::Char('w') => {
                self.report_selected_action("watch");
                EventResult::Consumed
            }
            KeyCode::Char('e') => {
                self.enable_selected();
                EventResult::Consumed
            }
            KeyCode::Char('d') => {
                self.disable_selected();
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
            self.set_status(&format!(
                "{} {action}: unified action ready; incident id required for plan/run",
                name
            ));
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

/// Flatten built-in skill categories into a flat display entry list.
fn flatten_builtin_skills(categories: &[(&str, Vec<BuiltinSkill>)]) -> Vec<SkillDisplayEntry> {
    let mut entries = Vec::new();
    for (cat_name, skills) in categories {
        for skill in skills {
            entries.push(SkillDisplayEntry {
                name: skill.name.to_string(),
                description: skill.description.to_string(),
                category: (*cat_name).to_string(),
                enabled: skill.enabled,
                source: skill.category.to_string(),
                status: if skill.enabled { "ready" } else { "disabled" }.to_string(),
                risk: "local".to_string(),
                tags: skill
                    .version
                    .map_or_else(Vec::new, |version| vec![format!("v{version}")]),
            });
        }
    }
    entries
}

fn entry_action_hints(entry: &SkillDisplayEntry) -> Vec<&'static str> {
    if entry.source.eq_ignore_ascii_case("iacc")
        || entry
            .tags
            .iter()
            .any(|tag| tag.eq_ignore_ascii_case("iacc"))
    {
        vec!["view", "validate", "plan", "run", "watch"]
    } else if entry.category.eq_ignore_ascii_case("local")
        || entry.risk.eq_ignore_ascii_case("operator_review")
    {
        vec!["view", "import"]
    } else {
        vec!["view"]
    }
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
    use crate::tui::app::SkillSummary;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;
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

    #[test]
    fn new_panel_has_categories() {
        let panel = SkillsPanel::new();
        assert!(!panel.categories.is_empty());
        assert!(
            panel.entries.len() > 10,
            "should have built-in skill entries"
        );
    }

    #[test]
    fn builtin_categories_include_tools_and_memory() {
        let panel = SkillsPanel::new();
        assert!(panel.categories.contains(&"Tools".to_string()));
        assert!(panel.categories.contains(&"Memory".to_string()));
        assert!(panel.categories.contains(&"Platform".to_string()));
        assert!(panel.categories.contains(&"System".to_string()));
    }

    #[test]
    fn category_cycle() {
        let mut panel = SkillsPanel::new();
        assert_eq!(panel.active_category, None);

        panel.next_category();
        assert_eq!(panel.active_category, Some(0));
        assert_eq!(panel.categories[0], "Tools");

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
        let mut panel = SkillsPanel::new();
        panel.prev_category();
        assert_eq!(panel.active_category, Some(panel.categories.len() - 1));

        panel.prev_category();
        assert_eq!(panel.active_category, Some(panel.categories.len() - 2));
    }

    #[test]
    fn filtered_entries_respects_category() {
        let mut panel = SkillsPanel::new();
        panel.active_category = Some(0); // Tools
        let filtered = panel.filtered_entries();
        assert!(!filtered.is_empty());
        for entry in &filtered {
            assert_eq!(entry.category, "Tools");
        }
    }

    #[test]
    fn toggle_selected_works() {
        let mut panel = SkillsPanel::new();
        panel.active_category = Some(0); // Tools
        panel.selected_index = Some(1); // Second tool

        let before = panel.filtered_entries()[1].enabled;
        panel.toggle_selected();
        let after = panel.filtered_entries()[1].enabled;
        assert_ne!(before, after, "toggling should flip enabled state");
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
        let mut panel = SkillsPanel::new();
        let before = panel.entries.len();
        panel.execute_search("Bash");
        let after = panel.entries.len();
        assert_eq!(before, after, "search should preserve entry count");
        // First entries should match
        assert!(panel.entries[0].name.to_lowercase().contains("bash"));
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
        app.skill_list = vec![SkillSummary {
            name: "TestSkill".to_string(),
            description: "A test skill".to_string(),
            installed: true,
            category: "iacc".to_string(),
            source: "iacc".to_string(),
            status: "ready".to_string(),
            risk: "governed".to_string(),
            tags: vec!["demo".to_string()],
        }];
        let panel = SkillsPanel::from_app(&app);
        assert!(!panel.using_builtins);
        assert_eq!(panel.entries.len(), 1);
        assert_eq!(panel.entries[0].name, "TestSkill");
    }

    #[test]
    fn unified_iacc_entries_render_action_hints() {
        let mut app = App::new("test-model", "test-session");
        app.skill_list = vec![SkillSummary {
            name: "supply-risk-analyst".to_string(),
            description: "Supply Risk Analyst".to_string(),
            installed: true,
            category: "server_manufacturing".to_string(),
            source: "iacc".to_string(),
            status: "ready".to_string(),
            risk: "governed".to_string(),
            tags: vec!["iacc".to_string()],
        }];
        let mut panel = SkillsPanel::from_app(&app);
        let lines = render_panel(&mut panel, 92, 12);
        let joined = lines.join("\n");
        assert!(joined.contains("actions: view · validate · plan · run · watch"));
    }

    #[test]
    fn local_entries_report_run_as_unsupported() {
        let mut app = App::new("test-model", "test-session");
        app.skill_list = vec![SkillSummary {
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
            Some("release does not support run")
        );
    }

    #[test]
    fn from_app_empty_falls_back_to_builtins() {
        let app = App::new("test-model", "test-session");
        let panel = SkillsPanel::from_app(&app);
        assert!(panel.using_builtins);
        assert!(panel.entries.len() > 10);
    }

    #[test]
    fn empty_state_renders() {
        let mut panel = SkillsPanel::new();
        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Tools"),
            "should render category headers, got: {joined}"
        );
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
