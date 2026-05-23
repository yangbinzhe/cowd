#![allow(dead_code)]

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::App;
use crate::tui::components::{Component, EventResult, RenderContext};

/// Context panel showing token usage, progress bar, and cost estimate.
///
/// Features:
/// - Token count: used / context window
/// - Progress bar: ████░░ 75%
/// - Cost estimate in USD
/// - Auto-updates from App state on sync
pub struct ContextPanel {
    /// Token count used so far.
    pub token_count: u64,
    /// Total context window size.
    pub context_window: u64,
    /// Input tokens for current turn.
    pub turn_input_tokens: u64,
    /// Output tokens for current turn.
    pub turn_output_tokens: u64,
}

impl ContextPanel {
    pub fn new() -> Self {
        Self {
            token_count: 0,
            context_window: 0,
            turn_input_tokens: 0,
            turn_output_tokens: 0,
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.token_count = app.token_count;
        self.context_window = app.context_window;
        self.turn_input_tokens = app.turn_input_tokens;
        self.turn_output_tokens = app.turn_output_tokens;
    }

    pub fn from_app(app: &App) -> Self {
        let mut cp = Self::new();
        cp.sync_from_app(app);
        cp
    }

    /// Build a unicode progress bar string: "████░░░░"
    fn progress_bar(used: u64, total: u64, bar_width: usize) -> String {
        if total == 0 {
            return "░".repeat(bar_width);
        }
        let pct = (used as f64 / total as f64).min(1.0);
        let filled = (pct * bar_width as f64).round() as usize;
        let empty = bar_width.saturating_sub(filled);
        let mut bar = String::with_capacity(bar_width);
        for _ in 0..filled {
            bar.push('█');
        }
        for _ in 0..empty {
            bar.push('░');
        }
        bar
    }

    fn usage_pct(&self) -> f64 {
        if self.context_window == 0 {
            0.0
        } else {
            (self.token_count as f64 / self.context_window as f64 * 100.0).min(100.0)
        }
    }

    /// Estimate cost in USD based on token counts.
    fn cost_estimate(&self) -> f64 {
        let input_cost = self.token_count as f64 * 3.0 / 1_000_000.0;
        let output_cost = self.turn_output_tokens as f64 * 15.0 / 1_000_000.0;
        input_cost + output_cost
    }
}

impl Default for ContextPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for ContextPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let pct = self.usage_pct();
        let bar = Self::progress_bar(self.token_count, self.context_window, 12);
        let cost = self.cost_estimate();

        let mut lines = Vec::new();
        lines.push(Line::from(Span::styled(
            "Context Usage",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));

        // Progress bar line
        let bar_color = if pct > 90.0 {
            Color::Red
        } else if pct > 70.0 {
            Color::Yellow
        } else {
            Color::Green
        };
        lines.push(Line::from(vec![
            Span::styled(bar, Style::default().fg(bar_color)),
            Span::styled(
                format!(" {:.0}%", pct),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
        ]));
        lines.push(Line::raw(""));

        // Token counts
        lines.push(Line::from(vec![
            Span::styled("Used:     ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.token_count),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Window:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.context_window),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Input:    ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.turn_input_tokens),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Output:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.turn_output_tokens),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::raw(""));

        // Cost
        lines.push(Line::from(vec![
            Span::styled("Cost:     ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("${:.4}", cost),
                Style::default().fg(Color::Yellow),
            ),
        ]));

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Context ");
        let paragraph = Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "context_panel"
    }
}

fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 10_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else if n >= 1_000 {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::test_utils::MockTerminal;
    use crate::tui::skin::SkinConfig;

    fn render_panel(panel: &mut ContextPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    #[test]
    fn context_panel_shows_token_count() {
        let mut panel = ContextPanel::new();
        panel.token_count = 1500;
        panel.context_window = 128_000;

        let lines = render_panel(&mut panel, 40, 12);
        let joined = lines.join("\n");
        assert!(joined.contains("1k") || joined.contains("1500"), "Should show token count");
        assert!(joined.contains("128") || joined.contains("128k"), "Should show context window");
    }

    #[test]
    fn shows_percent() {
        let mut panel = ContextPanel::new();
        panel.token_count = 64_000;
        panel.context_window = 128_000;

        let lines = render_panel(&mut panel, 40, 12);
        let joined = lines.join("\n");
        assert!(joined.contains("50%"), "Should show 50% for half usage");
    }

    #[test]
    fn updates_on_sync() {
        let mut app = App::new("m", "s");
        app.token_count = 10_000;
        app.context_window = 100_000;
        app.turn_input_tokens = 500;
        app.turn_output_tokens = 200;

        let mut panel = ContextPanel::from_app(&app);
        assert_eq!(panel.token_count, 10_000);
        assert_eq!(panel.context_window, 100_000);
        assert_eq!(panel.turn_input_tokens, 500);
        assert_eq!(panel.turn_output_tokens, 200);

        // Re-sync with updated values
        app.token_count = 20_000;
        panel.sync_from_app(&app);
        assert_eq!(panel.token_count, 20_000);
    }

    #[test]
    fn progress_bar_zero_window() {
        let bar = ContextPanel::progress_bar(100, 0, 12);
        assert_eq!(bar.chars().count(), 12, "bar should have 12 chars");
        assert!(bar.chars().all(|c| c == '░'), "zero window = empty bar");
    }

    #[test]
    fn cost_estimate_non_zero() {
        let mut panel = ContextPanel::new();
        panel.token_count = 100_000;
        panel.turn_output_tokens = 10_000;
        let cost = panel.cost_estimate();
        assert!(cost > 0.0, "Cost should be positive");
    }

    #[test]
    fn component_trait_methods() {
        let panel = ContextPanel::new();
        assert!(panel.focusable());
        assert_eq!(panel.id(), "context_panel");
    }
}
