use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::{App, TimelineEntry};
use crate::tui::components::{Component, EventResult, RenderContext};

#[derive(Debug, Clone, Default)]
pub struct RuntimeActivityPanel {
    profile: String,
    pressure_pct: u16,
    pressure_level: String,
    degradation_path: String,
    policy_action: String,
    policy_reason: String,
    selected_count: usize,
    omitted_count: usize,
    stable_hash: String,
    runtime_hash: String,
    dynamic_hash: String,
    recent_activity: Vec<String>,
    yolo_mode: bool,
    session_id: String,
}

impl RuntimeActivityPanel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.yolo_mode = app.yolo_mode;
        self.session_id = app.session_id.clone();
        if let Some(envelope) = &app.latest_context_envelope {
            let probe = runtime::ContextRuntimeKernel::lean_probe(envelope);
            let policy = runtime::ContextRuntimeKernel::policy_decision(&probe);
            self.profile = format!("{:?}", envelope.profile);
            self.pressure_pct = envelope.diagnostics.pressure_bp / 100;
            self.pressure_level = format!("{:?}", probe.pressure_level);
            self.degradation_path = format!("{:?}", probe.degradation_path);
            self.policy_action = format!("{:?}", policy.action);
            self.policy_reason = policy.reason;
            self.selected_count = envelope.selected.len();
            self.omitted_count = envelope.omitted.len();
            self.stable_hash = short_hash(&envelope.diagnostics.stable_head_hash);
            self.runtime_hash = short_hash(&envelope.diagnostics.runtime_header_hash);
            self.dynamic_hash = short_hash(&envelope.diagnostics.dynamic_tail_hash);
        } else {
            self.profile = if app.yolo_mode {
                "YoloGoal".to_string()
            } else {
                "MainTurn".to_string()
            };
            self.pressure_pct = 0;
            self.pressure_level = "Nominal".to_string();
            self.degradation_path = "None".to_string();
            self.policy_action = "None".to_string();
            self.policy_reason = "context pressure nominal; no policy action required".to_string();
            self.selected_count = 0;
            self.omitted_count = 0;
            self.stable_hash = "n/a".to_string();
            self.runtime_hash = "n/a".to_string();
            self.dynamic_hash = "n/a".to_string();
        }
        self.recent_activity = app
            .timeline_clone_vec()
            .into_iter()
            .rev()
            .take(8)
            .map(activity_label)
            .collect();
    }
}

impl Component for RuntimeActivityPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let mut lines = Vec::new();
        lines.push(Line::from(Span::styled(
            "Runtime Activity",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));
        lines.push(Line::from(vec![
            Span::styled("Session:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(short_id(&self.session_id), Style::default().fg(Color::White)),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Profile:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                &self.profile,
                Style::default().fg(if self.yolo_mode { Color::Yellow } else { Color::White }),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Pressure: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "{}% {} via {}",
                    self.pressure_pct, self.pressure_level, self.degradation_path
                ),
                Style::default().fg(if self.pressure_pct > 85 {
                    Color::Red
                } else if self.pressure_pct > 70 {
                    Color::Yellow
                } else {
                    Color::Green
                }),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Context:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("selected {} omitted {}", self.selected_count, self.omitted_count),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Policy:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{} - {}", self.policy_action, preview(&self.policy_reason, 72)),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Hashes:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "stable {} runtime {} dyn {}",
                    self.stable_hash, self.runtime_hash, self.dynamic_hash
                ),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "Recent",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )));
        if self.recent_activity.is_empty() {
            lines.push(Line::from(Span::styled(
                "No runtime activity yet",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for item in &self.recent_activity {
                lines.push(Line::from(vec![
                    Span::styled("- ", Style::default().fg(Color::DarkGray)),
                    Span::styled(item.clone(), Style::default().fg(Color::White)),
                ]));
            }
        }
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "Tab: panels  Context tab: evidence details",
            Style::default().fg(Color::DarkGray),
        )));

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Runtime ");
        ctx.frame_mut()
            .render_widget(Paragraph::new(lines).block(block).wrap(Wrap { trim: false }), area);
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "runtime_activity_panel"
    }
}

fn activity_label(entry: TimelineEntry) -> String {
    match entry {
        TimelineEntry::Message { role, content, .. } => {
            format!("{role}: {}", preview(&content, 64))
        }
        TimelineEntry::Thinking { content, complete, .. } => {
            format!("thinking{}: {}", if complete { "" } else { "*" }, preview(&content, 64))
        }
        TimelineEntry::ToolCall {
            name,
            preview: tool_preview,
            done,
            exit_code,
            ..
        } => {
            let status = exit_code
                .map(|code| format!("exit {code}"))
                .unwrap_or_else(|| if done { "done".to_string() } else { "running".to_string() });
            format!("tool {name} {status}: {}", preview(&tool_preview, 48))
        }
        TimelineEntry::SlashOutput { command, output, .. } => {
            format!("/{command}: {}", preview(&output, 64))
        }
    }
}

fn preview(value: &str, max: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max {
        normalized
    } else {
        normalized.chars().take(max).collect::<String>() + "..."
    }
}

fn short_hash(hash: &str) -> String {
    if hash.is_empty() {
        "n/a".to_string()
    } else {
        hash.chars().take(8).collect()
    }
}

fn short_id(id: &str) -> String {
    if id.chars().count() <= 12 {
        id.to_string()
    } else {
        id.chars().take(12).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    fn render_panel(panel: &mut RuntimeActivityPanel, width: u16, height: u16) -> String {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines().join("\n")
    }

    #[test]
    fn syncs_runtime_activity_from_app() {
        let mut app = App::new("m", "session-runtime-123456789");
        app.yolo_mode = true;
        app.add_message("user", "ship the runtime console");
        app.apply_event(runtime::CowdEvent::ToolStart {
            id: "tool-1".to_string(),
            name: "bash".to_string(),
            preview: "cargo test".to_string(),
        });
        app.apply_event(runtime::CowdEvent::ToolComplete {
            id: "tool-1".to_string(),
            name: "bash".to_string(),
            summary: "ok".to_string(),
            exit_code: Some(0),
        });

        let identity = runtime::ContextIdentity::main("session-runtime-123456789".to_string());
        app.latest_context_envelope = Some(runtime::ContextRuntimeKernel::build_envelope(
            runtime::ContextEnvelopeRequest {
                profile: runtime::ContextProfile::YoloGoal,
                identity,
                intent: "ship".to_string(),
                stable_head: vec!["stable".to_string()],
                runtime_header: vec!["runtime".to_string()],
                dynamic_items: vec![runtime::ContextItem::new(
                    "ctx-1",
                    runtime::ContextSourceKind::Memory,
                    runtime::ContextRole::Evidence,
                    "runtime evidence",
                )],
                omitted: Vec::new(),
                total_budget_tokens: 8_000,
            },
        ));

        let mut panel = RuntimeActivityPanel::new();
        panel.sync_from_app(&app);
        let rendered = render_panel(&mut panel, 88, 24);

        assert!(rendered.contains("Runtime Activity"));
        assert!(rendered.contains("YoloGoal"));
        assert!(rendered.contains("Nominal"));
        assert!(rendered.contains("None"));
        assert!(rendered.contains("selected 1 omitted 0"));
        assert!(rendered.contains("tool bash exit 0"));
        assert!(rendered.contains("user: ship the runtime console"));
    }
}
