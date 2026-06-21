#![allow(dead_code)]

use crossterm::event::{Event, KeyCode};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::App;
use crate::components::{Component, EventResult, RenderContext};
use crate::runtime_control_store::{SurfaceEventSummary, SurfaceHealthSummary, SurfaceSummary};

pub struct SurfacePanel {
    surfaces: Vec<SurfaceSummary>,
    health: Option<SurfaceHealthSummary>,
    events: Vec<SurfaceEventSummary>,
    selected: usize,
}

impl SurfacePanel {
    pub fn new() -> Self {
        Self {
            surfaces: Vec::new(),
            health: None,
            events: Vec::new(),
            selected: 0,
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.surfaces = app.gateway_surfaces.clone();
        self.health = app.gateway_surface_health.clone();
        self.events = app.gateway_surface_events.clone();
        if self.selected >= self.surfaces.len() {
            self.selected = self.surfaces.len().saturating_sub(1);
        }
    }

    pub fn selected_surface_id(&self) -> Option<&str> {
        self.surfaces
            .get(self.selected)
            .map(|surface| surface.id.as_str())
    }

    pub fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        <Self as Component>::render(self, ctx, area);
    }

    pub fn handle_event(&mut self, event: &Event) -> EventResult {
        <Self as Component>::handle_event(self, event)
    }
}

impl Default for SurfacePanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for SurfacePanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let mut lines = Vec::new();
        let health = self.health.as_ref();
        lines.push(Line::from(vec![
            Span::styled("Host: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                health
                    .map(|item| item.status.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                Style::default().fg(Color::Green),
            ),
            Span::styled(
                format!(
                    "  surfaces {}  external {}",
                    health
                        .map(|item| item.surface_count)
                        .unwrap_or(self.surfaces.len() as u64),
                    health.map(|item| item.external_surface_count).unwrap_or(0)
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        lines.push(Line::from(Span::styled(
            "Keys: j/k select  refresh by running /surfaces",
            Style::default().fg(Color::DarkGray),
        )));
        lines.push(Line::raw(""));

        if self.surfaces.is_empty() {
            lines.push(Line::from(Span::styled(
                "No surfaces reported by Gateway.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            lines.push(Line::from(Span::styled(
                "Surfaces",
                Style::default().fg(Color::Cyan),
            )));
            let list_limit = area.height.saturating_sub(10).max(4) as usize;
            for (idx, surface) in self.surfaces.iter().take(list_limit).enumerate() {
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
                    Span::styled(format!("{:14}", surface.id), style),
                    Span::styled(
                        format!("{:20}", compact(&surface.kind, 20)),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(
                        format!("{:12}", surface.status),
                        status_style(&surface.status),
                    ),
                    Span::styled(
                        format!(
                            " caps {} routes {} res {}",
                            surface.capability_count, surface.route_count, surface.resource_count
                        ),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        if let Some(surface) = self.surfaces.get(self.selected) {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Selected",
                Style::default().fg(Color::Cyan),
            )));
            lines.push(Line::from(vec![
                Span::styled("Name: ", Style::default().fg(Color::DarkGray)),
                Span::styled(surface.name.clone(), Style::default().fg(Color::White)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Transport: ", Style::default().fg(Color::DarkGray)),
                Span::styled(surface.transport.clone(), Style::default().fg(Color::White)),
                Span::styled("  Lifecycle: ", Style::default().fg(Color::DarkGray)),
                Span::styled(surface.lifecycle.clone(), Style::default().fg(Color::White)),
            ]));
            let diag = surface.diagnostics.first().cloned().unwrap_or_else(|| {
                if surface.entry.is_some() {
                    "managed by Gateway sidecar contract".to_string()
                } else {
                    "builtin surface".to_string()
                }
            });
            lines.push(Line::from(vec![
                Span::styled("Note: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    compact(&diag, area.width.saturating_sub(10) as usize),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }

        if !self.events.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Recent Events",
                Style::default().fg(Color::Cyan),
            )));
            for event in self.events.iter().take(5) {
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{:10}", event.surface),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(
                        format!("{:18}", compact(&event.event_type, 18)),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(
                        compact(&event.detail, area.width.saturating_sub(32) as usize),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        let block = Block::default()
            .title(" Surfaces ")
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
                self.selected = (self.selected + 1).min(self.surfaces.len().saturating_sub(1));
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
        "surface_panel"
    }
}

fn status_style(status: &str) -> Style {
    match status {
        "builtin" | "ready" | "discovered" => Style::default().fg(Color::Green),
        "disabled" => Style::default().fg(Color::DarkGray),
        "unavailable" | "error" => Style::default().fg(Color::Red),
        _ => Style::default().fg(Color::Yellow),
    }
}

fn compact(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        format!(
            "{}...",
            value
                .chars()
                .take(max.saturating_sub(3))
                .collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_control_store::{SurfaceHealthSummary, SurfaceSummary};
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;

    #[test]
    fn renders_surface_registry_without_platform_coupling() {
        let mut app = App::new("model", "session");
        app.gateway_surface_health = Some(SurfaceHealthSummary {
            status: "ready".to_string(),
            surface_count: 2,
            external_surface_count: 1,
            route_count: 1,
            resource_count: 1,
        });
        app.gateway_surfaces = vec![
            SurfaceSummary {
                id: "tui".to_string(),
                name: "TUI".to_string(),
                kind: "interactive-surface".to_string(),
                status: "builtin".to_string(),
                lifecycle: "builtin".to_string(),
                transport: "stdio-jsonl".to_string(),
                capability_count: 2,
                route_count: 0,
                resource_count: 0,
                entry: None,
                diagnostics: Vec::new(),
            },
            SurfaceSummary {
                id: "webui".to_string(),
                name: "WebUI".to_string(),
                kind: "web-surface".to_string(),
                status: "builtin".to_string(),
                lifecycle: "builtin".to_string(),
                transport: "stdio-jsonl".to_string(),
                capability_count: 3,
                route_count: 0,
                resource_count: 1,
                entry: None,
                diagnostics: vec!["static assets registered".to_string()],
            },
        ];

        let mut panel = SurfacePanel::new();
        panel.sync_from_app(&app);
        let mut terminal = MockTerminal::new(100, 28);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 100, 28));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(joined.contains("Surfaces"), "{joined}");
        assert!(joined.contains("tui"), "{joined}");
        assert!(joined.contains("webui"), "{joined}");
        assert!(!joined.contains("channel.feishu"), "{joined}");
    }
}
