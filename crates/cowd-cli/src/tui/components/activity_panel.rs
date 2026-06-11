use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::{App, TimelineEntry};
use crate::tui::components::panel_scroll::PanelScrollState;
use crate::tui::components::{Component, EventResult, RenderContext};

#[derive(Debug, Clone, Default)]
pub struct ActivityPanel {
    labels: Vec<String>,
    scroll: PanelScrollState,
}

impl ActivityPanel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.labels = app
            .timeline_clone_vec()
            .into_iter()
            .rev()
            .map(activity_label)
            .collect();
        self.scroll.sync(self.labels.len(), 10);
    }

    pub fn scroll_down(&mut self, visible_rows: usize) {
        self.scroll.sync(self.labels.len(), visible_rows);
        self.scroll.line_down();
    }

    pub fn scroll_up(&mut self) {
        self.scroll.line_up();
    }

    pub fn scroll_page_down(&mut self, visible_rows: usize) {
        self.scroll.sync(self.labels.len(), visible_rows);
        self.scroll.page_down();
    }

    pub fn scroll_page_up(&mut self, visible_rows: usize) {
        self.scroll.sync(self.labels.len(), visible_rows);
        self.scroll.page_up();
    }

    fn visible_rows(&self, area: Rect) -> usize {
        area.height.saturating_sub(3).max(1) as usize
    }
}

impl Component for ActivityPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let visible_rows = self.visible_rows(area);
        self.scroll.sync(self.labels.len(), visible_rows);

        let mut lines = vec![Line::from(vec![
            Span::styled(
                format!("{} events", self.labels.len()),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  up/down", Style::default().fg(Color::DarkGray)),
        ])];

        if self.labels.is_empty() {
            lines.push(Line::from(Span::styled(
                "No activity yet",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            let start = self.scroll.offset;
            let end = (start + visible_rows).min(self.labels.len());
            for (idx, label) in self.labels[start..end].iter().enumerate() {
                let absolute = start + idx;
                let marker = if absolute == 0 { "* " } else { "  " };
                let color = if label.contains("error") || label.contains("failed") {
                    Color::Red
                } else if label.contains("tool") {
                    Color::Yellow
                } else if label.contains("thinking") {
                    Color::Cyan
                } else {
                    Color::White
                };
                lines.push(Line::from(vec![
                    Span::styled(marker, Style::default().fg(Color::DarkGray)),
                    Span::styled(label.clone(), Style::default().fg(color)),
                ]));
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Activity ");
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(block)
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &crossterm::event::Event) -> EventResult {
        let rows = 10usize;
        match event {
            crossterm::event::Event::Key(key)
                if key.kind == crossterm::event::KeyEventKind::Press =>
            {
                match key.code {
                    crossterm::event::KeyCode::Char('j') | crossterm::event::KeyCode::Down => {
                        self.scroll_down(rows);
                        EventResult::Consumed
                    }
                    crossterm::event::KeyCode::Char('k') | crossterm::event::KeyCode::Up => {
                        self.scroll_up();
                        EventResult::Consumed
                    }
                    crossterm::event::KeyCode::Char('d')
                        if key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                    {
                        self.scroll_page_down(rows);
                        EventResult::Consumed
                    }
                    crossterm::event::KeyCode::Char('u')
                        if key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                    {
                        self.scroll_page_up(rows);
                        EventResult::Consumed
                    }
                    _ => EventResult::NotConsumed,
                }
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "activity_panel"
    }
}

fn activity_label(entry: TimelineEntry) -> String {
    match entry {
        TimelineEntry::Thinking {
            complete, content, ..
        } => {
            let state = if complete {
                "thinking done"
            } else {
                "thinking"
            };
            if content.is_empty() {
                state.to_string()
            } else {
                format!("{state}: {}", preview(&content, 48))
            }
        }
        TimelineEntry::ToolCall {
            name,
            preview,
            done,
            ..
        } => {
            let state = if done { "tool done" } else { "tool running" };
            if preview.is_empty() {
                format!("{state}: {name}")
            } else {
                format!("{state}: {name} {}", preview_text(&preview, 38))
            }
        }
        TimelineEntry::Message { role, content, .. } => {
            format!("{role}: {}", preview(&content, 56))
        }
        TimelineEntry::SlashOutput {
            command, output, ..
        } => {
            format!("/{command}: {}", preview(&output, 50))
        }
    }
}

fn preview(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(3)).collect();
    out.push_str("...");
    out
}

fn preview_text(text: &str, max: usize) -> String {
    preview(&text.replace('\n', " "), max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_panel_syncs_newest_first() {
        let mut app = App::new("m", "s");
        app.add_message("user", "first");
        app.add_message("assistant", "second");
        let mut panel = ActivityPanel::new();

        panel.sync_from_app(&app);

        assert!(panel.labels[0].contains("assistant"));
        assert!(panel.labels[1].contains("user"));
    }

    #[test]
    fn activity_panel_scrolls_with_bounds() {
        let mut panel = ActivityPanel {
            labels: (0..20).map(|idx| format!("event {idx}")).collect(),
            scroll: PanelScrollState::new(),
        };

        panel.scroll_page_down(5);
        assert_eq!(panel.scroll.offset, 4);
        panel.scroll_page_up(5);
        assert_eq!(panel.scroll.offset, 0);
    }
}
