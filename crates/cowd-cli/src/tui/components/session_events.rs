// ── Session Events Component ──────────────────────────────────────
// Receives session management events from UnifiedSessionManager
// and displays current session list in the TUI.

#![allow(dead_code)]

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

use crate::tui::app::App;
use crate::tui::components::{Component, EventResult, RenderContext};

pub struct SessionEvents {
    pub sessions: Vec<(String, String, String)>, // (id, name, created_at)
    pub active_session_name: String,
}

impl SessionEvents {
    pub fn new() -> Self {
        Self {
            sessions: Vec::new(),
            active_session_name: String::new(),
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.sessions = app.sessions.clone();
        self.active_session_name = app.active_session_name.clone();
    }
}

impl Component for SessionEvents {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Sessions ");
        let inner = block.inner(area);
        ctx.frame_mut().render_widget(&block, area);

        if self.sessions.is_empty() {
            ctx.frame_mut().render_widget(
                Paragraph::new(Line::from(Span::styled(
                    " No active sessions ",
                    Style::default().fg(Color::DarkGray),
                ))),
                inner,
            );
            return;
        }

        let mut lines: Vec<Line> = Vec::new();
        for (id, name, created) in &self.sessions {
            let is_active = *name == self.active_session_name;
            let style = if is_active {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::White)
            };
            let prefix = if is_active { "▶ " } else { "  " };
            let display_name = if name.is_empty() { id } else { name };
            lines.push(Line::from(vec![
                Span::styled(prefix, style),
                Span::styled(format!("{}  [{}]", display_name, created), style),
            ]));
        }

        ctx.frame_mut().render_widget(Paragraph::new(lines), inner);
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        false
    }

    fn id(&self) -> &str {
        "session_events"
    }
}
