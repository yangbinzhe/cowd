#![allow(dead_code)]

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::{App, TimelineEntry};
use crate::tui::components::{Component, EventResult, RenderContext};

/// A single tool progress entry for the thinking panel.
#[derive(Debug, Clone)]
struct ToolProgress {
    id: String,
    name: String,
    done: bool,
    exit_code: Option<i32>,
}

/// Unified thinking panel showing reasoning + tool progress during a turn.
///
/// During a turn:
///   ┌─ Thinking ────────────────────┐
///   │ ⠋ Reasoning...                │
///   │ Let me analyze the code...    │
///   │                               │
///   │ Tools:                        │
///   │   ├── bash: running (5s)      │
///   │   └── read: ✓ done (2s)       │
///   └───────────────────────────────┘
///
/// After turn complete (collapsed summary):
///   ┌─ Thinking ────────────────────┐
///   │ ✓ 3 tools, 12s total          │
///   └───────────────────────────────┘
pub struct ThinkingPanel {
    pub visible: bool,
    pub reasoning: String,
    pub reasoning_complete: bool,
    pub collapsed: bool,
    tools: Vec<ToolProgress>,
    spinner_idx: usize,
}

impl ThinkingPanel {
    pub fn new() -> Self {
        Self {
            visible: false,
            reasoning: String::new(),
            reasoning_complete: false,
            collapsed: true,
            tools: Vec::new(),
            spinner_idx: 0,
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.visible = app.turn_active;
        if !app.turn_active {
            self.collapsed = true;
        }

        // Extract reasoning from the last Thinking entry
        self.reasoning.clear();
        let mut last_reasoning: Option<(String, bool)> = None;
        for (_, entry) in app.timeline_iter() {
            if let TimelineEntry::Thinking {
                content, complete, ..
            } = entry
            {
                last_reasoning = Some((content.clone(), *complete));
            }
        }
        if let Some((reasoning, complete)) = last_reasoning {
            self.reasoning = reasoning;
            self.reasoning_complete = complete;
        }

        // Extract tool progress
        self.tools.clear();
        for (_, entry) in app.timeline_iter() {
            if let TimelineEntry::ToolCall {
                id,
                name,
                done,
                exit_code,
                ..
            } = entry
            {
                self.tools.push(ToolProgress {
                    id: id.clone(),
                    name: name.clone(),
                    done: *done,
                    exit_code: *exit_code,
                });
            }
        }
    }

    pub fn tick(&mut self) {
        self.spinner_idx = self.spinner_idx.wrapping_add(1);
    }

    fn spinner_char(&self) -> &'static str {
        const F: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        F[self.spinner_idx % F.len()]
    }

    /// Toggle collapsed/expanded state.
    pub fn toggle(&mut self) {
        self.collapsed = !self.collapsed;
    }
}

impl Default for ThinkingPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for ThinkingPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if !self.visible {
            return;
        }

        let mut lines: Vec<Line> = Vec::new();

        // ── Reasoning section ─────────────────────────────────────
        if self.collapsed {
            let total_tools = self.tools.len();
            let done_tools = self.tools.iter().filter(|t| t.done).count();
            let summary = if self.reasoning_complete && total_tools == done_tools {
                format!("✓ {total_tools} tools completed",)
            } else if total_tools > 0 {
                format!(
                    "{} {} reasoning, {done_tools}/{total_tools} tools",
                    self.spinner_char(),
                    if self.reasoning.is_empty() {
                        "processing"
                    } else {
                        "thinking"
                    },
                )
            } else if !self.reasoning.is_empty() {
                format!(
                    "{} {}",
                    self.spinner_char(),
                    self.reasoning.chars().take(60).collect::<String>()
                )
            } else {
                format!("{} Processing...", self.spinner_char())
            };

            lines.push(Line::from(Span::styled(
                summary,
                Style::default().fg(Color::Cyan),
            )));
        } else {
            // ── Expanded: reasoning inline ────────────────────────
            if !self.reasoning.is_empty() {
                lines.push(Line::from(vec![Span::styled(
                    format!("{} Reasoning:", self.spinner_char()),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )]));
                for line in self.reasoning.lines().take(10) {
                    let trimmed = if line.len() > 80 { &line[..80] } else { line };
                    lines.push(Line::from(Span::styled(
                        format!("  {trimmed}"),
                        Style::default().fg(Color::DarkGray),
                    )));
                }
                if self.reasoning.lines().count() > 10 {
                    lines.push(Line::from(Span::styled(
                        "  ... (truncated)",
                        Style::default().fg(Color::DarkGray),
                    )));
                }
                lines.push(Line::raw(""));
            }

            // ── Tool progress list ────────────────────────────────
            if !self.tools.is_empty() {
                lines.push(Line::from(Span::styled(
                    "Tools:",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )));
                for (i, tool) in self.tools.iter().enumerate() {
                    let tree_char = if i == self.tools.len() - 1 {
                        "└── "
                    } else {
                        "├── "
                    };
                    let (icon, icon_color) = if tool.done {
                        if tool.exit_code == Some(0) {
                            ("✓", Color::Green)
                        } else {
                            ("✗", Color::Red)
                        }
                    } else {
                        (self.spinner_char(), Color::Yellow)
                    };
                    let status = if tool.done { "done" } else { "running" };

                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("  {tree_char}"),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(format!("{} ", tool.name), Style::default().fg(Color::White)),
                        Span::styled(icon, Style::default().fg(icon_color)),
                        Span::styled(format!(" {status}"), Style::default().fg(Color::DarkGray)),
                    ]));
                }
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(if self.collapsed {
                Color::Cyan
            } else {
                Color::Yellow
            }))
            .title(format!(
                "{} Thinking ",
                if self.collapsed { "💭" } else { "⟳" }
            ));

        let paragraph = Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    fn handle_event(&mut self, event: &crossterm::event::Event) -> EventResult {
        match event {
            crossterm::event::Event::Key(key)
                if key.kind == crossterm::event::KeyEventKind::Press =>
            {
                match key.code {
                    crossterm::event::KeyCode::Enter | crossterm::event::KeyCode::Char(' ') => {
                        self.toggle();
                        EventResult::Consumed
                    }
                    _ => EventResult::NotConsumed,
                }
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        self.visible
    }

    fn id(&self) -> &str {
        "thinking_panel"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    fn render_panel(panel: &mut ThinkingPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    #[test]
    fn unified_thinking_shows_spinner() {
        let mut panel = ThinkingPanel::new();
        panel.visible = true;
        panel.collapsed = true;
        panel.reasoning = String::new();
        panel.reasoning_complete = false;

        let lines = render_panel(&mut panel, 40, 8);
        let joined = lines.join("\n");
        // Should show some processing text with spinner character
        assert!(
            joined.contains("Processing"),
            "Should show Processing text, got: {joined}"
        );
    }

    #[test]
    fn reasoning_inline() {
        let mut panel = ThinkingPanel::new();
        panel.visible = true;
        panel.collapsed = false;
        panel.reasoning = "Let me think about this carefully.\nFirst, I'll analyze the code.\nThen I'll write a solution.".to_string();
        panel.reasoning_complete = false;

        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        assert!(joined.contains("Reasoning"), "Should show Reasoning header");
        assert!(
            joined.contains("analyze the code"),
            "Should show reasoning content"
        );
    }

    #[test]
    fn tool_progress_in_same_panel() {
        let mut panel = ThinkingPanel::new();
        panel.visible = true;
        panel.collapsed = false;
        panel.tools = vec![
            ToolProgress {
                id: "t1".into(),
                name: "bash".into(),
                done: false,
                exit_code: None,
            },
            ToolProgress {
                id: "t2".into(),
                name: "read".into(),
                done: true,
                exit_code: Some(0),
            },
        ];

        let lines = render_panel(&mut panel, 40, 10);
        let joined = lines.join("\n");
        assert!(joined.contains("Tools"), "Should show Tools header");
        assert!(joined.contains("bash"), "Should show bash tool");
        assert!(joined.contains("read"), "Should show read tool");
        assert!(
            joined.contains("├──") || joined.contains("└──"),
            "Should show tree characters"
        );
    }

    #[test]
    fn turn_complete_collapses_to_summary() {
        let mut panel = ThinkingPanel::new();
        panel.visible = true;
        panel.collapsed = true;
        panel.reasoning_complete = true;
        panel.tools = vec![
            ToolProgress {
                id: "t1".into(),
                name: "bash".into(),
                done: true,
                exit_code: Some(0),
            },
            ToolProgress {
                id: "t2".into(),
                name: "read".into(),
                done: true,
                exit_code: Some(0),
            },
        ];
        panel.reasoning = "Done".to_string();

        let lines = render_panel(&mut panel, 40, 5);
        let joined = lines.join("\n");
        assert!(
            joined.contains("completed"),
            "Collapsed summary should show completion status, got: {joined}"
        );
    }

    #[test]
    fn sync_from_app_turn_complete_hides() {
        let mut app = App::new("m", "s");
        app.turn_active = false;

        let mut panel = ThinkingPanel::new();
        panel.visible = true;
        panel.sync_from_app(&app);
        assert!(
            panel.collapsed,
            "Panel should collapse when turn is complete"
        );
    }
}
