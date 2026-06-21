#![allow(dead_code)]

use crossterm::event::{Event, KeyCode};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::{App, MemoryEntry};
use crate::components::{Component, EventResult, RenderContext};

pub struct L4MemoryView {
    entries: Vec<MemoryEntry>,
    selected: usize,
    visible: bool,
}

impl L4MemoryView {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            selected: 0,
            visible: false,
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.entries = app
            .memory_entries
            .iter()
            .filter(|entry| entry.layer.eq_ignore_ascii_case("l4"))
            .cloned()
            .collect();
        if self.selected >= self.entries.len() {
            self.selected = self.entries.len().saturating_sub(1);
        }
    }

    pub fn toggle(&mut self) {
        self.visible = !self.visible;
    }
}

impl Default for L4MemoryView {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for L4MemoryView {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if !self.visible {
            return;
        }
        let mut lines = Vec::new();
        if self.entries.is_empty() {
            lines.push(Line::from(Span::styled(
                "No L4 memory projection entries.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for (idx, entry) in self
                .entries
                .iter()
                .take(area.height.saturating_sub(2) as usize)
                .enumerate()
            {
                let style = if idx == self.selected {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                };
                lines.push(Line::from(vec![
                    Span::styled(
                        if idx == self.selected { "> " } else { "  " },
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(
                        preview(&entry.content, area.width.saturating_sub(4) as usize),
                        style,
                    ),
                ]));
            }
        }
        let block = Block::default()
            .title(" L4 Memory ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(block)
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                self.selected = (self.selected + 1).min(self.entries.len().saturating_sub(1));
                EventResult::Consumed
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = self.selected.saturating_sub(1);
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "l4_memory_view"
    }
}

fn preview(text: &str, max: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max {
        normalized
    } else {
        format!(
            "{}...",
            normalized
                .chars()
                .take(max.saturating_sub(3))
                .collect::<String>()
        )
    }
}
