use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::components::{Component, EventResult, RenderContext};

#[derive(Debug, Clone, Default)]
pub struct ConfigPanel {
    config: Option<serde_json::Value>,
    provider_projection: Option<serde_json::Value>,
    effective_config: Option<serde_json::Value>,
    selected_model: usize,
    last_status: Option<String>,
    last_receipt: Option<serde_json::Value>,
}

impl ConfigPanel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_config(
        &mut self,
        config: serde_json::Value,
        provider_projection: serde_json::Value,
        effective_config: serde_json::Value,
    ) {
        self.config = Some(config);
        self.provider_projection = Some(provider_projection);
        self.effective_config = Some(effective_config);
        self.clamp_selection();
    }

    pub fn selected_model_id(&self) -> Option<String> {
        self.models()
            .get(self.selected_model)
            .and_then(|model| model.get("id").or_else(|| model.get("name")))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    }

    pub fn record_action_result(&mut self, label: &str, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => {
                self.last_status = Some(format!("{label} succeeded"));
                self.last_receipt = Some(payload);
            }
            Err(error) => {
                self.last_status = Some(format!("{label} failed: {error}"));
                self.last_receipt = None;
            }
        }
    }

    pub fn set_status(&mut self, status: impl Into<String>) {
        self.last_status = Some(status.into());
    }

    pub fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        <Self as Component>::render(self, ctx, area);
    }

    pub fn handle_event(&mut self, event: &Event) -> EventResult {
        <Self as Component>::handle_event(self, event)
    }

    fn models(&self) -> Vec<serde_json::Value> {
        self.provider_projection
            .as_ref()
            .and_then(|value| value.get("models"))
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    fn clamp_selection(&mut self) {
        let len = self.models().len();
        if len == 0 {
            self.selected_model = 0;
        } else if self.selected_model >= len {
            self.selected_model = len - 1;
        }
    }

    fn configured_model(&self) -> String {
        self.provider_projection
            .as_ref()
            .and_then(|value| value.get("configured_model"))
            .or_else(|| self.config.as_ref().and_then(|value| value.get("model")))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unset")
            .to_string()
    }

    fn provider_count(&self) -> u64 {
        self.provider_projection
            .as_ref()
            .and_then(|value| value.get("provider_count"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    }

    fn provider_model_count(&self) -> u64 {
        self.provider_projection
            .as_ref()
            .and_then(|value| value.get("provider_model_count"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    }

    fn source(&self) -> &str {
        self.effective_config
            .as_ref()
            .and_then(|value| value.get("source"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
    }

    fn warnings(&self) -> Vec<String> {
        self.effective_config
            .as_ref()
            .and_then(|value| value.get("warnings"))
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(str::to_string)
            .collect()
    }
}

impl Component for ConfigPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let accent = ctx.theme().accent_color();
        let mut lines = Vec::new();

        lines.push(Line::from(vec![
            Span::styled("Model: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.configured_model(),
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(
                    "  providers {}  models {}  source {}",
                    self.provider_count(),
                    self.provider_model_count(),
                    self.source()
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        lines.push(Line::from(Span::styled(
            "Keys: j/k select model  Enter set default  r reload providers  e refresh",
            Style::default().fg(Color::DarkGray),
        )));

        for warning in self.warnings().into_iter().take(3) {
            lines.push(Line::from(vec![
                Span::styled("Warning: ", Style::default().fg(Color::Yellow)),
                Span::styled(warning, Style::default().fg(Color::White)),
            ]));
        }

        if let Some(status) = &self.last_status {
            lines.push(Line::from(vec![
                Span::styled("Status: ", Style::default().fg(Color::DarkGray)),
                Span::styled(status.clone(), Style::default().fg(Color::Yellow)),
            ]));
        }
        if let Some(receipt) = &self.last_receipt {
            lines.push(Line::from(vec![
                Span::styled("Receipt: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    compact_json(receipt, area.width.saturating_sub(10) as usize),
                    Style::default().fg(Color::Cyan),
                ),
            ]));
        }

        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "Available Models",
            Style::default().fg(accent).add_modifier(Modifier::BOLD),
        )));

        let models = self.models();
        if models.is_empty() {
            lines.push(Line::from(Span::styled(
                "No configured provider models. Configure providers in ~/.cowd/config.yaml, then press r.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            let max_rows = area.height.saturating_sub(9) as usize;
            for (index, model) in models.iter().enumerate().take(max_rows.max(1)) {
                let id = model
                    .get("id")
                    .or_else(|| model.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                let provider = model
                    .get("provider")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("provider");
                let selected = model
                    .get("selected")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                let marker = if index == self.selected_model {
                    ">"
                } else {
                    " "
                };
                let current = if selected { " current" } else { "" };
                lines.push(Line::from(vec![
                    Span::styled(
                        marker,
                        Style::default().fg(if index == self.selected_model {
                            Color::Green
                        } else {
                            Color::DarkGray
                        }),
                    ),
                    Span::raw(" "),
                    Span::styled(id.to_string(), Style::default().fg(Color::White)),
                    Span::styled(
                        format!("  {provider}{current}"),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
            if models.len() > max_rows {
                lines.push(Line::from(Span::styled(
                    format!("... {} more", models.len() - max_rows),
                    Style::default().fg(Color::DarkGray),
                )));
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Config ")
            .border_style(Style::default().fg(accent));
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(block)
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Char('j') | KeyCode::Down => {
                    let len = self.models().len();
                    if len > 0 {
                        self.selected_model = (self.selected_model + 1).min(len - 1);
                    }
                    EventResult::Consumed
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    self.selected_model = self.selected_model.saturating_sub(1);
                    EventResult::Consumed
                }
                _ => EventResult::NotConsumed,
            },
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "config_panel"
    }
}

fn compact_json(value: &serde_json::Value, width: usize) -> String {
    let text = value.to_string();
    if text.chars().count() <= width {
        return text;
    }
    let mut output: String = text.chars().take(width.saturating_sub(1)).collect();
    output.push('~');
    output
}
