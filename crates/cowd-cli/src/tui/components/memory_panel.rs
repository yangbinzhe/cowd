// ── Memory Panel ──────────────────────────────────────────────────
// Displays memory entries from App state, with layer tags and previews.

#![allow(dead_code)]

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

use crate::tui::app::{App, MemoryEntry};
use crate::tui::components::{Component, EventResult, RenderContext};

/// Panel showing memory entries with layer-tagged previews.
///
/// Empty state shows a friendly placeholder message.
/// Entries are displayed with `[layer]` tags in cyan and content previews
/// truncated to 80 characters.
pub struct MemoryPanel {
    pub entries: Vec<MemoryEntry>,
}

impl MemoryPanel {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
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

    /// Legacy draw API used by `render.rs`.
    pub fn draw(frame: &mut ratatui::Frame, area: Rect, app: &App) {
        use ratatui::text::Text;
        use ratatui::widgets::Wrap;

        let mut lines: Vec<Line> = Vec::new();
        if app.memory_entries.is_empty() {
            lines.push(Line::from(Span::styled(
                "No memory entries loaded.",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Memory system: 3-layer (L0 Identity / L1 Essential / L3 Deep Recall)",
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
            ratatui::widgets::Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            area,
        );
    }
}

impl Default for MemoryPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for MemoryPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let text: Vec<Line> = if self.entries.is_empty() {
            vec![
                Line::from("No memory entries yet."),
                Line::from(""),
                Line::from("Send a message to populate memory."),
            ]
        } else {
            let mut lines = vec![
                Line::from(Span::styled(
                    format!("{} memory entries:", self.entries.len()),
                    Style::default().fg(Color::Yellow),
                )),
                Line::from(""),
            ];
            for entry in self.entries.iter().take(20) {
                let layer_span = Span::styled(
                    format!("[{}]", entry.layer),
                    Style::default().fg(Color::Cyan),
                );
                let content_preview = if entry.content.len() > 80 {
                    format!("{}...", &entry.content[..77])
                } else {
                    entry.content.clone()
                };
                lines.push(Line::from(vec![
                    layer_span,
                    Span::from(" "),
                    Span::from(content_preview),
                ]));
            }
            lines
        };

        let block = Block::default()
            .title(" Memory ")
            .borders(Borders::ALL);
        let paragraph = Paragraph::new(text).block(block);
        ctx.frame_mut().render_widget(paragraph, area);
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "memory_panel"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::test_utils::MockTerminal;
    use crate::tui::skin::SkinConfig;

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
            joined.contains("No memory entries yet"),
            "Empty state should show placeholder, got: {joined}"
        );
    }

    #[test]
    fn shows_memory_entries_with_layer_tag() {
        let mut panel = MemoryPanel::new();
        panel.entries = vec![
            MemoryEntry {
                layer: "core".into(),
                content: "User prefers TypeScript over JavaScript".into(),
                priority: "high".into(),
            },
            MemoryEntry {
                layer: "session".into(),
                content: "Currently working on the TUI component system".into(),
                priority: "normal".into(),
            },
        ];

        let lines = render_panel(&mut panel, 60, 8);
        let joined = lines.join("\n");
        assert!(joined.contains("2 memory entries"), "Should show entry count");
        assert!(joined.contains("[core]"), "Should show core layer tag");
        assert!(joined.contains("[session]"), "Should show session layer tag");
    }

    #[test]
    fn entry_preview_truncated() {
        let mut panel = MemoryPanel::new();
        let long = "a".repeat(200);
        panel.entries = vec![MemoryEntry {
            layer: "test".into(),
            content: long.clone(),
            priority: "low".into(),
        }];

        // Use a wide terminal so the truncated line fits on one row
        let lines = render_panel(&mut panel, 100, 8);
        let joined = lines.join("\n");
        assert!(
            joined.contains(&long[..77]),
            "Should show first 77 chars of long content"
        );
        assert!(joined.contains("..."), "Truncated entries should show ellipsis");
    }

    #[test]
    fn sync_from_app_populates_entries() {
        let mut app = App::new("test-model", "test-session");
        app.memory_entries = vec![
            MemoryEntry {
                layer: "core".into(),
                content: "test memory".into(),
                priority: "high".into(),
            },
        ];

        let mut panel = MemoryPanel::new();
        panel.sync_from_app(&app);
        assert_eq!(panel.entries.len(), 1);
        assert_eq!(panel.entries[0].layer, "core");
        assert_eq!(panel.entries[0].content, "test memory");
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
        app.memory_entries = vec![
            MemoryEntry {
                layer: "a".into(),
                content: "b".into(),
                priority: "c".into(),
            },
        ];
        let panel = MemoryPanel::from_app(&app);
        assert_eq!(panel.entries.len(), 1);
    }

    #[test]
    fn at_most_twenty_entries_displayed() {
        let mut panel = MemoryPanel::new();
        for i in 0..30 {
            panel.entries.push(MemoryEntry {
                layer: "t".into(),
                content: format!("entry {i}"),
                priority: "low".into(),
            });
        }
        let lines = render_panel(&mut panel, 60, 25);
        let joined = lines.join("\n");
        assert!(joined.contains("30 memory entries"), "Should show total count");
        assert!(joined.contains("entry 19"), "Should show entry 19");
        assert!(
            !joined.contains("entry 20"),
            "Should NOT show entry 20 (beyond limit)"
        );
    }
}
