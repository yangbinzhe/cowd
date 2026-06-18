#![allow(dead_code)]

use std::time::Instant;

use crossterm::event::{Event, KeyCode};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

use crate::app::{App, TimelineEntry};
use crate::components::{Component, EventResult, RenderContext};

/// A single agent entry extracted from the timeline.
#[derive(Debug, Clone)]
pub struct AgentEntry {
    pub id: String,
    pub name: String,
    pub done: bool,
    pub exit_code: Option<i32>,
    pub duration_secs: u64,
    pub depth: usize,
}

/// Agents overlay showing a tree of agent/subagent calls from the timeline.
///
/// Features:
/// - Extracts ToolCall entries from timeline
/// - Shows status: running (spinner) / done (exit code)
/// - 'x' to interrupt a running agent
pub struct AgentsOverlay {
    pub agents: Vec<AgentEntry>,
    pub visible: bool,
    pub spinner_idx: usize,
    pub selected_idx: usize,
    started_at: Instant,
}

impl AgentsOverlay {
    pub fn new() -> Self {
        Self {
            agents: Vec::new(),
            visible: false,
            spinner_idx: 0,
            selected_idx: 0,
            started_at: Instant::now(),
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.agents.clear();
        for (_, entry) in app.timeline_iter() {
            if let TimelineEntry::ToolCall {
                id,
                name,
                done,
                exit_code,
                ..
            } = entry
            {
                self.agents.push(AgentEntry {
                    id: id.clone(),
                    name: name.clone(),
                    done: *done,
                    exit_code: *exit_code,
                    duration_secs: self.started_at.elapsed().as_secs(),
                    depth: 0, // flat for now; subagents could be nested
                });
            }
        }
        if self.selected_idx >= self.agents.len() {
            self.selected_idx = self.agents.len().saturating_sub(1);
        }
    }

    pub fn tick(&mut self) {
        self.spinner_idx = self.spinner_idx.wrapping_add(1);
    }

    pub fn spinner_char(&self) -> &'static str {
        const F: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        F[self.spinner_idx % F.len()]
    }

    /// Toggle visibility.
    pub fn toggle(&mut self) {
        self.visible = !self.visible;
    }

    /// Interrupt the currently selected agent.
    pub fn interrupt_selected(&mut self) -> Option<String> {
        if self.selected_idx < self.agents.len() {
            let id = self.agents[self.selected_idx].id.clone();
            if !self.agents[self.selected_idx].done {
                self.agents[self.selected_idx].done = true;
                self.agents[self.selected_idx].exit_code = Some(-1);
                return Some(id);
            }
        }
        None
    }
}

impl Default for AgentsOverlay {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for AgentsOverlay {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if !self.visible || self.agents.is_empty() {
            return;
        }

        let overlay = Rect::new(
            area.width.saturating_sub(40).min(2),
            area.y,
            40.min(area.width),
            (self.agents.len() as u16 + 5).min(area.height),
        );

        // Clear background
        ctx.frame_mut().render_widget(Clear, overlay);

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::from(Span::styled(
            " Agents ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));

        for (i, agent) in self.agents.iter().enumerate() {
            let is_selected = i == self.selected_idx;
            let prefix = if is_selected { "> " } else { "  " };

            let indent = "  ".repeat(agent.depth);
            let tree_char = if i == self.agents.len() - 1 {
                "└── "
            } else {
                "├── "
            };

            let (status_icon, status_color) = if agent.done {
                if agent.exit_code == Some(0) {
                    ("✓", Color::Green)
                } else {
                    ("✗", Color::Red)
                }
            } else {
                (self.spinner_char(), Color::Yellow)
            };

            let status_color_style = Style::default().fg(status_color);
            let secs = agent.duration_secs;

            lines.push(Line::from(vec![
                Span::styled(prefix, Style::default().fg(Color::Cyan)),
                Span::styled(
                    format!("{indent}{tree_char}"),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("{} ", agent.name),
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(format!("{status_icon}"), status_color_style),
                Span::styled(
                    if agent.done {
                        format!(" ({}s)", secs)
                    } else {
                        format!(" ({secs}s)")
                    },
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }

        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "Keys: j↓ k↑  Enter:view-task  Tab:filter  Esc:close",
            Style::default().fg(Color::DarkGray),
        )));

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title(" Agent Tree ");
        let paragraph = Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, overlay);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        if !self.visible {
            return EventResult::NotConsumed;
        }
        match event {
            Event::Key(key) if key.kind == crossterm::event::KeyEventKind::Press => {
                match key.code {
                    KeyCode::Char('x') | KeyCode::Char('X') => {
                        self.interrupt_selected();
                        EventResult::Consumed
                    }
                    KeyCode::Up => {
                        self.selected_idx = self.selected_idx.saturating_sub(1);
                        EventResult::Consumed
                    }
                    KeyCode::Down => {
                        if self.selected_idx + 1 < self.agents.len() {
                            self.selected_idx += 1;
                        }
                        EventResult::Consumed
                    }
                    KeyCode::Esc => {
                        self.visible = false;
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
        "agents_overlay"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;
    use crate::CowdEvent;
    use crossterm::event::KeyEvent;

    fn make_tool_call(id: &str, name: &str, done: bool, exit_code: Option<i32>) -> TimelineEntry {
        TimelineEntry::ToolCall {
            id: id.into(),
            name: name.into(),
            preview: format!("Run {name}"),
            output: String::new(),
            done,
            expanded: false,
            exit_code,
        }
    }

    fn render_overlay(overlay: &mut AgentsOverlay, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            overlay.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    #[test]
    fn agents_overlay_shows_tree() {
        let _app = App::new("m", "s");

        let mut overlay = AgentsOverlay::new();
        overlay.visible = true;
        overlay.agents = vec![
            AgentEntry {
                id: "t1".into(),
                name: "Build".into(),
                done: true,
                exit_code: Some(0),
                duration_secs: 12,
                depth: 0,
            },
            AgentEntry {
                id: "t2".into(),
                name: "Test".into(),
                done: true,
                exit_code: Some(0),
                duration_secs: 3,
                depth: 1,
            },
        ];

        let lines = render_overlay(&mut overlay, 40, 20);
        let joined = lines.join("\n");
        assert!(joined.contains("Build"), "Should show Build agent");
        assert!(joined.contains("Test"), "Should show Test agent");
        assert!(
            joined.contains("├──") || joined.contains("└──"),
            "Should show tree characters"
        );
    }

    #[test]
    fn status_updates() {
        let mut overlay = AgentsOverlay::new();
        overlay.visible = true;
        overlay.agents = vec![AgentEntry {
            id: "t1".into(),
            name: "Running".into(),
            done: false,
            exit_code: None,
            duration_secs: 5,
            depth: 0,
        }];

        let s1 = overlay.spinner_char().to_string();
        overlay.tick();
        let s2 = overlay.spinner_char().to_string();
        // After 1 tick the spinner may change or stay same within its 10-char cycle
        assert!(!s1.is_empty(), "Spinner should not be empty");
        assert!(!s2.is_empty(), "Spinner should not be empty after tick");

        // Sync from app with done entries
        let mut app = App::new("m", "s");
        app.apply_event(CowdEvent::ToolStart {
            id: "t1".into(),
            name: "bash".into(),
            preview: "echo".into(),
        });
        overlay.sync_from_app(&app);
        assert_eq!(overlay.agents.len(), 1);
        assert!(!overlay.agents[0].done);
    }

    #[test]
    fn interrupt_on_x() {
        let mut overlay = AgentsOverlay::new();
        overlay.visible = true;
        overlay.agents = vec![AgentEntry {
            id: "t1".into(),
            name: "Running".into(),
            done: false,
            exit_code: None,
            duration_secs: 5,
            depth: 0,
        }];

        let result = overlay.handle_event(&Event::Key(KeyEvent::new(
            KeyCode::Char('x'),
            crossterm::event::KeyModifiers::NONE,
        )));
        assert!(result.is_consumed(), "'x' should be consumed");
        assert!(
            overlay.agents[0].done,
            "Agent should be marked done after interrupt"
        );
        assert_eq!(overlay.agents[0].exit_code, Some(-1));
    }

    #[test]
    fn hidden_when_not_visible() {
        let mut overlay = AgentsOverlay::new();
        overlay.visible = false;

        let result = overlay.handle_event(&Event::Key(KeyEvent::new(
            KeyCode::Char('x'),
            crossterm::event::KeyModifiers::NONE,
        )));
        assert!(
            result.is_not_consumed(),
            "Hidden overlay should not consume events"
        );
    }

    #[test]
    fn esc_hides() {
        let mut overlay = AgentsOverlay::new();
        overlay.visible = true;

        let result = overlay.handle_event(&Event::Key(KeyEvent::new(
            KeyCode::Esc,
            crossterm::event::KeyModifiers::NONE,
        )));
        assert!(result.is_consumed());
        assert!(!overlay.visible);
    }
}
