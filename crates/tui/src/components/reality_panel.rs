use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::App;
use crate::components::{Component, EventResult, RenderContext};
use crate::runtime_control_store::{
    FactFlowSummary, KnowledgeCandidateSummary, RealityCoreSummary, StructuredDataSummary,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RealityView {
    Overview,
    Samples,
    Candidates,
}

pub struct RealityPanel {
    core: Option<RealityCoreSummary>,
    flow: Option<FactFlowSummary>,
    structured: Option<StructuredDataSummary>,
    memory_status: Option<String>,
    memory_entries: usize,
    memory_governance: Option<serde_json::Value>,
    knowledge_candidates: Vec<KnowledgeCandidateSummary>,
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
            memory_governance: None,
            knowledge_candidates: Vec::new(),
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
        self.memory_governance = app.memory_governance.clone();
        self.knowledge_candidates = app.gateway_knowledge_candidates.clone();
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

    pub fn record_governance_result(&mut self, result: Result<serde_json::Value, String>) {
        self.memory_governance = Some(match result {
            Ok(value) => value,
            Err(error) => serde_json::json!({
                "running": false,
                "automatic_governance_error": error,
            }),
        });
        self.view = RealityView::Overview;
        self.scroll = 0;
    }

    #[must_use]
    pub fn governance_is_running(&self) -> bool {
        governance_status(self.memory_governance.as_ref()) == "running"
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
            "Keys: 1 overview  2 samples  3 candidates  g govern  j/k scroll",
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
                        "Governance",
                        governance_status(self.memory_governance.as_ref()),
                    ),
                    kv(
                        "Review queue",
                        governance_queue_status(self.memory_governance.as_ref()),
                    ),
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
            RealityView::Candidates => {
                lines.push(kv(
                    "Candidates",
                    &format!(
                        "{} total · {} awaiting approval · {} promoted · {} rolled back",
                        self.knowledge_candidates.len(),
                        candidate_count(&self.knowledge_candidates, "awaiting_approval"),
                        candidate_count(&self.knowledge_candidates, "promoted"),
                        candidate_count(&self.knowledge_candidates, "rolled_back"),
                    ),
                ));
                if self.knowledge_candidates.is_empty() {
                    lines.push(Line::from(Span::styled(
                        "No governed knowledge candidates.",
                        Style::default().fg(Color::DarkGray),
                    )));
                } else {
                    for candidate in &self.knowledge_candidates {
                        lines.push(Line::from(vec![
                            Span::styled(
                                format!("[{}] ", candidate.state),
                                Style::default().fg(status_color(&candidate.state)),
                            ),
                            Span::styled(
                                candidate.title.clone(),
                                Style::default().fg(Color::White),
                            ),
                        ]));
                        lines.push(kv(
                            "  Scope",
                            &format!(
                                "{} · novelty {} · approval {}",
                                candidate.scope,
                                candidate.novelty,
                                candidate.approval_id.as_deref().unwrap_or("none")
                            ),
                        ));
                        if let Some(reason) = candidate.reason.as_deref() {
                            lines.push(kv("  Reason", reason));
                        }
                    }
                }
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
                KeyCode::Char('3') => {
                    self.view = RealityView::Candidates;
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

fn governance_status(value: Option<&serde_json::Value>) -> &'static str {
    let Some(value) = value else {
        return "unknown";
    };
    if value
        .get("running")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || value
            .get("automatic_governance_run")
            .is_some_and(|run| !run.is_null())
    {
        return "running";
    }
    if value
        .get("automatic_governance_error")
        .is_some_and(|error| !error.is_null())
    {
        return "failed";
    }
    match value
        .pointer("/automatic_governance/outcome")
        .or_else(|| value.pointer("/automatic_governance/status"))
        .and_then(serde_json::Value::as_str)
    {
        Some("succeeded") => "succeeded",
        Some("completed_with_errors") => "completed with errors",
        Some("failed") => "failed",
        _ => "idle",
    }
}

fn governance_queue_status(value: Option<&serde_json::Value>) -> &'static str {
    let durable = value
        .and_then(|value| {
            value
                .pointer("/review_queue/durable")
                .or_else(|| value.pointer("/governance_review_queue/durable"))
        })
        .and_then(serde_json::Value::as_bool);
    match durable {
        Some(true) => "durable",
        Some(false) => "process-local",
        None => "unknown",
    }
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

fn candidate_count(candidates: &[KnowledgeCandidateSummary], state: &str) -> usize {
    candidates
        .iter()
        .filter(|candidate| candidate.state == state)
        .count()
}

fn status_color(status: &str) -> Color {
    match status {
        "ready" | "healthy" | "available" | "promoted" | "approved" => Color::Green,
        "degraded" | "attention" | "awaiting_approval" | "validated" => Color::Yellow,
        "failed" | "blocked" | "rejected" | "conflicts" => Color::Red,
        "rolled_back" | "superseded" => Color::DarkGray,
        _ => Color::White,
    }
}

#[cfg(test)]
mod tests {
    use super::{governance_queue_status, governance_status, RealityPanel};

    #[test]
    fn governance_status_prefers_live_and_failure_signals() {
        assert_eq!(
            governance_status(Some(&serde_json::json!({
                "running": true,
                "automatic_governance": {"outcome": "succeeded"}
            }))),
            "running"
        );
        assert_eq!(
            governance_status(Some(&serde_json::json!({
                "automatic_governance_error": "store unavailable"
            }))),
            "failed"
        );
        assert_eq!(
            governance_status(Some(&serde_json::json!({
                "automatic_governance": {"outcome": "completed_with_errors"}
            }))),
            "completed with errors"
        );
    }

    #[test]
    fn governance_queue_reports_backend_durability() {
        assert_eq!(
            governance_queue_status(Some(&serde_json::json!({
                "review_queue": {"durable": true}
            }))),
            "durable"
        );
        assert_eq!(
            governance_queue_status(Some(&serde_json::json!({
                "governance_review_queue": {"durable": false}
            }))),
            "process-local"
        );
    }

    #[test]
    fn governance_action_result_is_visible_to_the_panel() {
        let mut panel = RealityPanel::new();
        panel.record_governance_result(Err("maintenance failed".to_string()));

        assert!(!panel.governance_is_running());
        assert_eq!(
            governance_status(panel.memory_governance.as_ref()),
            "failed"
        );
    }

    #[test]
    fn running_governance_is_detected_before_manual_submission() {
        let mut panel = RealityPanel::new();
        panel.memory_governance = Some(serde_json::json!({
            "running": true,
            "automatic_governance_run": {"run_id": "run-1"}
        }));

        assert!(panel.governance_is_running());
    }
}
