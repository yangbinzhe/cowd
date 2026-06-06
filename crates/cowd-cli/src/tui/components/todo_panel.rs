// ── Todo Panel Component ──────────────────────────────────────────────
// Extracts ToolCall(name="TodoWrite") from the timeline, parses JSON
// todo items, and renders them with status icons.
//
// Features:
//   - Parses TodoWrite JSON: [{content, status, priority}]
//   - Renders: ☐ pending / ⏳ in_progress / ✅ completed
//   - 2+ items collapsible
//   - Hides when empty
// -----------------------------------------------------------------

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::components::base::{Component, EventResult, RenderContext};

/// A single todo item extracted from TodoWrite JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TodoItem {
    pub content: String,
    pub status: String,
    pub priority: String,
}

impl TodoItem {
    /// Parse a JSON value into a TodoItem. Graceful on missing fields.
    fn from_json_value(val: &serde_json::Value) -> Option<Self> {
        let obj = val.as_object()?;
        Some(Self {
            content: obj
                .get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            status: obj
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("pending")
                .to_string(),
            priority: obj
                .get("priority")
                .and_then(|v| v.as_str())
                .unwrap_or("medium")
                .to_string(),
        })
    }

    /// Status icon for display.
    fn icon(&self) -> &'static str {
        match self.status.as_str() {
            "completed" | "done" => "✅",
            "in_progress" | "in-progress" | "running" => "⏳",
            _ => "☐",
        }
    }

    /// Style for this item based on status.
    fn style(&self) -> Style {
        match self.status.as_str() {
            "completed" | "done" => Style::default().fg(Color::DarkGray),
            "in_progress" | "in-progress" | "running" => Style::default().fg(Color::Yellow),
            _ => Style::default().fg(Color::White),
        }
    }
}

/// Todo Panel: displays AI-generated task list from TodoWrite ToolCalls.
///
/// # Key bindings
/// | Key | Action |
/// |-----|--------|
/// | `c` | Toggle collapse/expand |
pub struct TodoPanel {
    /// Parsed todo items.
    items: Vec<TodoItem>,
    /// Whether the panel is collapsed (>2 items).
    collapsed: bool,
    /// Max items before collapsing. Default: 2.
    collapse_limit: usize,
}

impl TodoPanel {
    /// Create a new, empty todo panel.
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            collapsed: false,
            collapse_limit: 2,
        }
    }

    /// Extract todo items from the timeline (ToolCall entries with name="TodoWrite").
    /// Parses the output JSON: [{content, status, priority}]
    pub fn extract_from_timeline(&mut self, timeline: &[crate::tui::app::TimelineEntry]) {
        self.items.clear();
        for entry in timeline {
            if let crate::tui::app::TimelineEntry::ToolCall { name, output, .. } = entry {
                if name == "TodoWrite" && !output.is_empty() {
                    self.parse_todo_json(output);
                }
            }
        }
        self.collapsed = self.items.len() > self.collapse_limit;
    }

    /// Parse a JSON string containing todo items and add them.
    fn parse_todo_json(&mut self, json_str: &str) {
        // Try parsing as array first
        if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(json_str) {
            for val in &arr {
                if let Some(item) = TodoItem::from_json_value(val) {
                    self.items.push(item);
                }
            }
            return;
        }

        // Try parsing as single object
        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(json_str) {
            if let Some(item) = TodoItem::from_json_value(&obj) {
                self.items.push(item);
            }
        }
        // Silently skip if can't parse (graceful degradation)
    }

    /// Sync todo items from timeline entries by extracting ToolCall outputs
    /// where name="TodoWrite". Delegates to extract_from_timeline.
    pub fn sync_from_timeline(&mut self, timeline: &[crate::tui::app::TimelineEntry]) {
        self.extract_from_timeline(timeline);
    }

    /// Populate with pre-parsed items (for testing).
    pub fn load(&mut self, items: Vec<TodoItem>) {
        self.items = items;
        self.collapsed = self.items.len() > self.collapse_limit;
    }

    /// Return the number of items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Return true if no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Return true if currently collapsed.
    #[must_use]
    pub fn is_collapsed(&self) -> bool {
        self.collapsed
    }

    /// Toggle collapse/expand.
    pub fn toggle_collapse(&mut self) {
        self.collapsed = !self.collapsed;
    }
}

impl Default for TodoPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for TodoPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if self.items.is_empty() {
            return; // Hide when empty
        }

        if area.width < 10 || area.height < 3 {
            return;
        }

        let accent = ctx.theme().accent_color();

        let title = format!(" Todo ({}) ", self.items.len());

        let block = Block::default()
            .borders(Borders::ALL)
            .title(title.as_str())
            .fg(accent);

        let inner = block.inner(area);
        ctx.frame_mut().render_widget(block, area);

        let max_display = if self.collapsed {
            self.collapse_limit
        } else {
            self.items.len()
        };

        let mut items: Vec<Line> = Vec::new();

        for item in self.items.iter().take(max_display) {
            let icon = item.icon();
            let style = item.style();
            let label = format!(" {} {} {}", icon, item.content, item.priority);
            items.push(Line::styled(label, style));
        }

        // Collapse indicator
        if self.collapsed && self.items.len() > self.collapse_limit {
            let hidden = self.items.len() - self.collapse_limit;
            items.push(Line::styled(
                format!("  ▼ {} more items — press 'c' to expand", hidden),
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::ITALIC),
            ));
        }

        items.push(Line::raw(""));
        items.push(Line::from(Span::styled(
            "Keys: a:add  Enter:done  j↓ k↑  d:delete  p:priority",
            Style::default().fg(Color::DarkGray),
        )));

        let text = Text::from(items);
        let paragraph = Paragraph::new(text).wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, inner);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Char('c') => {
                    self.toggle_collapse();
                    EventResult::Consumed
                }
                _ => EventResult::NotConsumed,
            },
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "todo_panel"
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::RenderContext;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    fn render_panel(panel: &mut TodoPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            panel.render(&mut ctx, area);
        });
        terminal.buffer_lines()
    }

    fn make_item(content: &str, status: &str, priority: &str) -> TodoItem {
        TodoItem {
            content: content.to_string(),
            status: status.to_string(),
            priority: priority.to_string(),
        }
    }

    // ── Test: todo_panel_lists_items ─────────────────────────────────

    #[test]
    fn todo_panel_lists_items() {
        let mut panel = TodoPanel::new();
        panel.load(vec![
            make_item("Refactor auth module", "in_progress", "high"),
            make_item("Add tests for API", "pending", "medium"),
        ]);

        assert_eq!(panel.len(), 2);

        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Refactor auth module"),
            "Should show task 1"
        );
        assert!(joined.contains("Add tests for API"), "Should show task 2");
    }

    // ── Test: todo_items_show_status ─────────────────────────────────

    #[test]
    fn todo_items_show_status() {
        let mut panel = TodoPanel::new();
        panel.load(vec![
            make_item("Task pending", "pending", "low"),
            make_item("Task in progress", "in_progress", "high"),
        ]);
        panel.toggle_collapse(); // expand to show all

        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");

        // Check status icons
        assert!(joined.contains('☐'), "Should show pending icon");
        assert!(joined.contains('⏳'), "Should show in_progress icon");

        // Check priority labels
        assert!(joined.contains("high"), "Should show priority");
    }

    // ── Test: todo_panel_hides_when_empty ────────────────────────────

    #[test]
    fn todo_panel_hides_when_empty() {
        let mut panel = TodoPanel::new();
        assert!(panel.is_empty());

        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        // Should render nothing (panel hides when empty)
        assert!(
            !joined.contains("Todo"),
            "Empty panel should not show 'Todo' title"
        );
    }

    // ── Collapse tests ──────────────────────────────────────────────

    #[test]
    fn todo_panel_collapses_over_limit() {
        let mut panel = TodoPanel::new();
        panel.load(vec![
            make_item("A", "pending", "low"),
            make_item("B", "pending", "low"),
            make_item("C", "pending", "low"),
        ]);
        assert!(panel.is_collapsed(), "Should be collapsed with 3 items");

        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("more items"),
            "Should show collapse indicator"
        );
        assert!(joined.contains("A"), "Should show first item");
        // "C" might not be shown if collapsed
    }

    #[test]
    fn todo_panel_toggle_collapse() {
        let mut panel = TodoPanel::new();
        panel.load(vec![
            make_item("A", "pending", "low"),
            make_item("B", "pending", "low"),
            make_item("C", "pending", "low"),
        ]);
        assert!(panel.is_collapsed());

        panel.toggle_collapse();
        assert!(!panel.is_collapsed());

        panel.toggle_collapse();
        assert!(panel.is_collapsed());
    }

    // ── JSON parsing tests ──────────────────────────────────────────

    #[test]
    fn todo_parses_json_array() {
        let mut panel = TodoPanel::new();
        let json = r#"[
            {"content": "Fix bug", "status": "in_progress", "priority": "high"},
            {"content": "Write docs", "status": "pending", "priority": "medium"}
        ]"#;
        panel.parse_todo_json(json);
        assert_eq!(panel.len(), 2);
        assert_eq!(panel.items[0].content, "Fix bug");
        assert_eq!(panel.items[0].status, "in_progress");
        assert_eq!(panel.items[1].content, "Write docs");
    }

    #[test]
    fn todo_parses_single_object() {
        let mut panel = TodoPanel::new();
        let json = r#"{"content": "Single task", "status": "completed", "priority": "low"}"#;
        panel.parse_todo_json(json);
        assert_eq!(panel.len(), 1);
        assert_eq!(panel.items[0].content, "Single task");
        assert_eq!(panel.items[0].status, "completed");
    }

    #[test]
    fn todo_invalid_json_silently_skipped() {
        let mut panel = TodoPanel::new();
        panel.parse_todo_json("not valid json");
        assert!(panel.is_empty(), "Invalid JSON should be silently skipped");
    }

    // ── Focusable & id ──────────────────────────────────────────────

    #[test]
    fn todo_panel_focusable_and_id() {
        let panel = TodoPanel::new();
        assert!(panel.focusable());
        assert_eq!(panel.id(), "todo_panel");
    }

    // ── Test: sync_from_timeline extracts TodoWrite items ───────────

    #[test]
    fn test_todo_from_timeline() {
        let mut panel = TodoPanel::new();
        let json = r#"[
            {"content": "Fix bug", "status": "in_progress", "priority": "high"},
            {"content": "Write docs", "status": "pending", "priority": "medium"}
        ]"#;
        let timeline = vec![crate::tui::app::TimelineEntry::ToolCall {
            id: "tc1".to_string(),
            name: "TodoWrite".to_string(),
            preview: "todo".to_string(),
            output: json.to_string(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];

        panel.sync_from_timeline(&timeline);
        assert_eq!(panel.len(), 2);
        assert_eq!(panel.items[0].content, "Fix bug");
        assert_eq!(panel.items[0].status, "in_progress");
        assert_eq!(panel.items[0].priority, "high");
        assert_eq!(panel.items[1].content, "Write docs");
        assert_eq!(panel.items[1].status, "pending");
        assert_eq!(panel.items[1].priority, "medium");
    }
}
