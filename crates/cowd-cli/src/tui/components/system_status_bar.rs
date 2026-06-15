use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
};

use runtime;

use crate::tui::app::App;
use crate::tui::components::{Component, EventResult, RenderContext};

#[derive(Debug, Clone, Default)]
pub struct SystemStatusBar {
    runtime: String,
    turn: String,
    daemon: String,
    gateway: String,
    provider: String,
    connectors: String,
    memory: String,
    issue: Option<String>,
}

impl SystemStatusBar {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.runtime = runtime_health(app).to_string();
        self.turn = if app.turn_active {
            if app.is_loading {
                "thinking".to_string()
            } else {
                "running".to_string()
            }
        } else {
            "idle".to_string()
        };
        self.daemon = app
            .daemon_runtime_readiness
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        self.gateway = if app.server_running {
            format!("up:{}s", app.server_uptime_secs.unwrap_or_default())
        } else {
            "down".to_string()
        };
        self.provider = runtime::resolve_global_provider(&app.model)
            .map(|provider| provider.name)
            .unwrap_or_else(|| "unresolved".to_string());
        self.connectors = connector_health(app);
        self.memory = app
            .memory_status
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        self.issue = app
            .daemon_degraded_reasons
            .first()
            .or_else(|| app.daemon_connector_degraded_reasons.first())
            .map(|reason| preview(reason, 34));
    }
}

impl Component for SystemStatusBar {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let runtime_color = match self.runtime.as_str() {
            "ready" => Color::Green,
            "blocked" | "degraded" => Color::Red,
            _ => Color::Yellow,
        };
        let turn_color = match self.turn.as_str() {
            "idle" => Color::DarkGray,
            "thinking" => Color::Yellow,
            _ => Color::White,
        };

        let mut spans = vec![
            Span::styled(
                "cowd ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(self.runtime.clone(), Style::default().fg(runtime_color)),
            sep(),
            Span::styled("provider ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                preview(&self.provider, 24),
                Style::default().fg(Color::White),
            ),
            sep(),
            Span::styled("turn ", Style::default().fg(Color::DarkGray)),
            Span::styled(self.turn.clone(), Style::default().fg(turn_color)),
        ];

        if let Some(issue) = &self.issue {
            spans.push(sep());
            spans.push(Span::styled("issue ", Style::default().fg(Color::DarkGray)));
            spans.push(Span::styled(
                issue.clone(),
                Style::default().fg(Color::Yellow),
            ));
        }

        let line = Line::from(fit_spans(spans, area.width as usize));
        let bg = ctx.theme().bg_color();
        ctx.frame_mut()
            .render_widget(Paragraph::new(line).style(Style::default().bg(bg)), area);
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        false
    }

    fn id(&self) -> &str {
        "system_status_bar"
    }
}

fn runtime_health(app: &App) -> &'static str {
    if !app.daemon_degraded_reasons.is_empty() || !app.daemon_connector_degraded_reasons.is_empty()
    {
        "degraded"
    } else if app.daemon_pending_approvals.unwrap_or(0) > 0 || app.approval.is_some() {
        "blocked"
    } else if app.daemon_runtime_readiness.is_some() || app.server_running {
        "ready"
    } else {
        "unknown"
    }
}

fn connector_health(app: &App) -> String {
    if !app.daemon_connector_degraded_reasons.is_empty() {
        return format!("degraded:{}", app.daemon_connector_degraded_reasons.len());
    }
    let accounts = app.daemon_connector_accounts.len();
    let capabilities = app.daemon_connector_capabilities.len();
    if accounts == 0 && capabilities == 0 {
        "none".to_string()
    } else {
        format!("{}a/{}c", accounts, capabilities)
    }
}

fn sep() -> Span<'static> {
    Span::styled("  |  ", Style::default().fg(Color::DarkGray))
}

fn preview(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn fit_spans(spans: Vec<Span<'static>>, max_width: usize) -> Vec<Span<'static>> {
    let mut used = 0usize;
    let mut fitted = Vec::new();
    for span in spans {
        let width = span.content.chars().count();
        if used + width > max_width {
            break;
        }
        used += width;
        fitted.push(span);
    }
    fitted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::RenderContext;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    #[test]
    fn runtime_health_blocks_on_pending_approvals() {
        let mut app = App::new("m", "s");
        app.daemon_pending_approvals = Some(2);

        assert_eq!(runtime_health(&app), "blocked");
    }

    #[test]
    fn connector_health_summarizes_accounts_and_capabilities() {
        let mut app = App::new("m", "s");
        app.daemon_connector_accounts.push(
            crate::tui::runtime_control_store::ConnectorAccountSummary {
                provider: "mock".into(),
                account_id: "a1".into(),
                auth_mode: "token".into(),
                status: "ready".into(),
                reason: None,
                binding_count: 1,
            },
        );
        app.daemon_connector_capabilities.push(
            crate::tui::runtime_control_store::ConnectorCapabilitySummary {
                provider: "mock".into(),
                capability_id: "read".into(),
                plane: "service".into(),
                risk: "low".into(),
                supports_commit: false,
                requires_approval: false,
            },
        );

        assert_eq!(connector_health(&app), "1a/1c");
    }

    #[test]
    fn render_system_status_bar_keeps_top_line_calm() {
        let app = App::new("deepseek-v4-pro", "s");
        let mut bar = SystemStatusBar::new();
        bar.sync_from_app(&app);

        let mut terminal = MockTerminal::new(100, 3);
        let skin = SkinConfig::default();
        terminal.draw(|frame| {
            let mut ctx = RenderContext::new(frame, &skin);
            bar.render(&mut ctx, Rect::new(0, 0, 100, 1));
        });

        let joined = terminal.buffer_lines().join("\n");
        assert!(joined.contains("cowd"));
        assert!(joined.contains("provider"));
        assert!(joined.contains("turn"));
        assert!(!joined.contains("gateway"));
        assert!(!joined.contains("connectors"));
        assert!(!joined.contains("memory"));
    }
}
