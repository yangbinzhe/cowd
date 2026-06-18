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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoryFilter {
    All,
    L1,
    L2,
    L3,
    L4,
}

pub struct MemoryPanel {
    entries: Vec<MemoryEntry>,
    filter: MemoryFilter,
    selected: usize,
    status: Option<String>,
}

impl MemoryPanel {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            filter: MemoryFilter::All,
            selected: 0,
            status: None,
        }
    }

    pub fn from_app(app: &App) -> Self {
        let mut panel = Self::new();
        panel.sync_from_app(app);
        panel
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.entries = app.memory_entries.clone();
        self.status = app.memory_status.clone();
        if self.selected >= self.filtered_entries().len() {
            self.selected = self.filtered_entries().len().saturating_sub(1);
        }
    }

    fn filtered_entries(&self) -> Vec<&MemoryEntry> {
        self.entries
            .iter()
            .filter(|entry| match self.filter {
                MemoryFilter::All => true,
                MemoryFilter::L1 => entry.layer.eq_ignore_ascii_case("l1"),
                MemoryFilter::L2 => entry.layer.eq_ignore_ascii_case("l2"),
                MemoryFilter::L3 => entry.layer.eq_ignore_ascii_case("l3"),
                MemoryFilter::L4 => entry.layer.eq_ignore_ascii_case("l4"),
            })
            .collect()
    }

    fn set_filter(&mut self, filter: MemoryFilter) {
        self.filter = filter;
        self.selected = 0;
    }

    pub fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        <Self as Component>::render(self, ctx, area);
    }

    pub fn handle_event(&mut self, event: &Event) -> EventResult {
        <Self as Component>::handle_event(self, event)
    }
}

impl Default for MemoryPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for MemoryPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let entries = self.filtered_entries();
        let mut lines = Vec::new();
        lines.push(Line::from(vec![
            Span::styled("Status: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.status.as_deref().unwrap_or("unknown"),
                Style::default().fg(Color::White),
            ),
            Span::styled(
                format!("  entries {}", entries.len()),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        lines.push(Line::raw(""));

        if entries.is_empty() {
            lines.push(Line::from(Span::styled(
                "No memory projection entries.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for (idx, entry) in entries
                .iter()
                .take(area.height.saturating_sub(4) as usize)
                .enumerate()
            {
                let selected = idx == self.selected;
                let marker = if selected { "> " } else { "  " };
                let style = if selected {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                };
                lines.push(Line::from(vec![
                    Span::styled(marker, Style::default().fg(Color::Yellow)),
                    Span::styled(
                        format!("[{}] ", entry.layer),
                        Style::default().fg(Color::Green),
                    ),
                    Span::styled(
                        preview(&entry.content, area.width.saturating_sub(8) as usize),
                        style,
                    ),
                ]));
            }
        }

        let block = Block::default()
            .title(" Memory ")
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
                self.selected =
                    (self.selected + 1).min(self.filtered_entries().len().saturating_sub(1));
                EventResult::Consumed
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = self.selected.saturating_sub(1);
                EventResult::Consumed
            }
            KeyCode::Char('0') => {
                self.set_filter(MemoryFilter::All);
                EventResult::Consumed
            }
            KeyCode::Char('1') => {
                self.set_filter(MemoryFilter::L1);
                EventResult::Consumed
            }
            KeyCode::Char('2') => {
                self.set_filter(MemoryFilter::L2);
                EventResult::Consumed
            }
            KeyCode::Char('3') => {
                self.set_filter(MemoryFilter::L3);
                EventResult::Consumed
            }
            KeyCode::Char('4') => {
                self.set_filter(MemoryFilter::L4);
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "memory_panel"
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
