use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::App;
use crate::components::{Component, EventResult, RenderContext};
use crate::runtime_control_store::{FactFlowSummary, RealityCoreSummary, StructuredDataSummary};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RealityView {
    Overview,
    Samples,
}

pub struct RealityPanel {
    core: Option<RealityCoreSummary>,
    flow: Option<FactFlowSummary>,
    structured: Option<StructuredDataSummary>,
    memory_status: Option<String>,
    memory_entries: usize,
    view: RealityView,
    scroll: usize,
    focused_backlink_target: Option<String>,
    focused_backlink_resolution: Option<String>,
    focused_backlink_object: Option<serde_json::Value>,
}

impl RealityPanel {
    pub fn new() -> Self {
        Self {
            core: None,
            flow: None,
            structured: None,
            memory_status: None,
            memory_entries: 0,
            view: RealityView::Overview,
            scroll: 0,
            focused_backlink_target: None,
            focused_backlink_resolution: None,
            focused_backlink_object: None,
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.core = app.gateway_reality_core.clone();
        self.flow = app.gateway_fact_flow.clone();
        self.structured = app.gateway_structured_data.clone();
        self.memory_status = app.memory_status.clone();
        self.memory_entries = app.memory_entries.len();
    }

    pub fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        <Self as Component>::render(self, ctx, area);
    }

    pub fn handle_event(&mut self, event: &Event) -> EventResult {
        <Self as Component>::handle_event(self, event)
    }

    pub fn focus_backlink_target(&mut self, target: impl Into<String>) {
        let target = target.into();
        self.focused_backlink_target = Some(target);
        self.focused_backlink_resolution =
            Some("loading exact evidence object from its canonical owner".to_string());
        self.focused_backlink_object = None;
        self.view = RealityView::Samples;
        self.scroll = 0;
    }

    #[must_use]
    pub fn accepts_backlink_result(&self, target: &str) -> bool {
        self.focused_backlink_target.as_deref() == Some(target)
    }

    pub fn record_backlink_object(&mut self, target: &str, object: serde_json::Value) {
        if !self.accepts_backlink_result(target) {
            return;
        }
        let available = object
            .get("available")
            .and_then(serde_json::Value::as_bool)
            .map_or("resolved", |available| {
                if available {
                    "available"
                } else {
                    "unavailable"
                }
            });
        self.focused_backlink_resolution = Some(format!("{target} {available}"));
        self.focused_backlink_object = Some(object);
        self.view = RealityView::Samples;
        self.scroll = 0;
    }

    pub fn record_backlink_failure(&mut self, target: &str, message: &str) {
        if !self.accepts_backlink_result(target) {
            return;
        }
        self.focused_backlink_resolution = Some(format!("Resolution failed: {message}"));
        self.focused_backlink_object = None;
        self.scroll = 0;
    }
}

impl Default for RealityPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for RealityPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let accent = ctx.theme().accent_color();
        let mut lines = Vec::new();

        let core = self.core.clone().unwrap_or_default();
        let flow = self.flow.clone().unwrap_or_default();
        let structured = self.structured.clone().unwrap_or_default();

        lines.push(Line::from(vec![
            Span::styled("Reality: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fallback(&core.status, "unknown"),
                Style::default()
                    .fg(status_color(&core.status))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(
                    "  facts {}  evidence {}  memory {}",
                    structured.fact_count, structured.evidence_count, self.memory_entries
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        lines.push(Line::from(Span::styled(
            "Keys: 1 overview  2 samples  j/k scroll",
            Style::default().fg(Color::DarkGray),
        )));
        lines.push(Line::raw(""));
        if let Some(target) = self.focused_backlink_target.as_deref() {
            lines.push(kv("Focused evidence", target));
            lines.push(kv(
                "Resolution",
                self.focused_backlink_resolution
                    .as_deref()
                    .unwrap_or("pending"),
            ));
            if let Some(object) = self.focused_backlink_object.as_ref() {
                let summary = serde_json::to_string(object)
                    .unwrap_or_else(|_| "evidence object is not serializable".to_string());
                lines.push(kv("Object", &summary));
            }
            lines.push(Line::raw(""));
        }

        match self.view {
            RealityView::Overview => {
                lines.extend([
                    kv("Fact", &core.fact_status),
                    kv(
                        "Memory",
                        self.memory_status.as_deref().unwrap_or(&core.memory_status),
                    ),
                    kv("Matrix", &core.matrix_status),
                    kv("Matrix context", &core.matrix_context_status),
                    kv("Growth", &core.growth_status),
                    kv("Context", &core.context_status),
                    kv("Audit", &core.audit_status),
                    kv(
                        "Fact Flow",
                        &format!(
                            "source {} stages {} events {} promotions {} boundaries {}",
                            fallback(&flow.source, "unknown"),
                            flow.stage_count,
                            flow.event_count,
                            flow.promotion_count,
                            flow.boundary_count
                        ),
                    ),
                    kv(
                        "Structured",
                        &format!(
                            "sources {} facts {} evidence {} watermarks {}",
                            structured.source_count,
                            structured.fact_count,
                            structured.evidence_count,
                            structured.watermark_count
                        ),
                    ),
                ]);
                for reason in core.degraded_reasons.iter().take(4) {
                    lines.push(Line::from(vec![
                        Span::styled("Degraded: ", Style::default().fg(Color::Yellow)),
                        Span::styled(reason.clone(), Style::default().fg(Color::White)),
                    ]));
                }
            }
            RealityView::Samples => {
                sample_section(&mut lines, "Sources", &structured.sample_sources);
                sample_section(&mut lines, "Facts", &structured.sample_facts);
                sample_section(&mut lines, "Evidence", &structured.sample_evidence);
                sample_section(&mut lines, "Watermarks", &structured.sample_watermarks);
            }
        }

        let viewport = usize::from(area.height.saturating_sub(2));
        self.scroll = self.scroll.min(lines.len().saturating_sub(viewport.max(1)));
        let start = self.scroll;
        let visible = lines
            .into_iter()
            .skip(start)
            .take(viewport.max(1))
            .collect::<Vec<_>>();

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Reality Core ")
            .border_style(Style::default().fg(accent));
        ctx.frame_mut().render_widget(
            Paragraph::new(visible)
                .block(block)
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Char('1') => {
                    self.view = RealityView::Overview;
                    self.scroll = 0;
                    EventResult::Consumed
                }
                KeyCode::Char('2') => {
                    self.view = RealityView::Samples;
                    self.scroll = 0;
                    EventResult::Consumed
                }
                KeyCode::Char('j') | KeyCode::Down => {
                    self.scroll = self.scroll.saturating_add(1);
                    EventResult::Consumed
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    self.scroll = self.scroll.saturating_sub(1);
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
        "reality_panel"
    }
}

fn kv(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label}: "), Style::default().fg(Color::DarkGray)),
        Span::styled(
            fallback(value, "unknown"),
            Style::default().fg(Color::White),
        ),
    ])
}

fn sample_section(lines: &mut Vec<Line<'static>>, label: &'static str, values: &[String]) {
    lines.push(Line::from(Span::styled(
        label,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )));
    if values.is_empty() {
        lines.push(Line::from(Span::styled(
            "  none",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        for value in values.iter().take(5) {
            lines.push(Line::from(vec![
                Span::styled("  - ", Style::default().fg(Color::DarkGray)),
                Span::styled(value.clone(), Style::default().fg(Color::White)),
            ]));
        }
    }
}

fn fallback(value: &str, default: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        default.to_string()
    } else {
        value.to_string()
    }
}

fn status_color(status: &str) -> Color {
    match status {
        "ready" | "healthy" | "available" => Color::Green,
        "degraded" | "attention" => Color::Yellow,
        "failed" | "blocked" => Color::Red,
        _ => Color::White,
    }
}
