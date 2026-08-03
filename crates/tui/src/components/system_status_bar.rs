use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
};

use crate::app::{App, TimelineEntry};
use crate::components::{Component, EventResult, RenderContext};

#[derive(Debug, Clone, Default)]
pub struct SystemStatusBar {
    runtime: String,
    turn: String,
    thinking_count: u32,
    tool_count: u32,
    reply_count: u32,
    event_count: u32,
    approval_count: u64,
    permission_count: usize,
    last_phase: String,
    session_id: String,
    daemon: String,
    gateway: String,
    provider: String,
    connectors: String,
    memory: String,
    mode: String,
    evidence: String,
    model: String,
    transport: String,
    history: String,
    issue: Option<String>,
    activity_timeline_len: usize,
    activity_full_sync_revision: u64,
}

impl SystemStatusBar {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.runtime = runtime_health(app).to_string();
        self.turn = if let Some(status) = app.current_execution_status {
            execution_status_label(status).to_string()
        } else if app.turn_is_active() {
            match app.timeline_last() {
                Some(TimelineEntry::Thinking {
                    complete: false, ..
                }) => "thinking".to_string(),
                Some(TimelineEntry::ToolCall { done: false, .. }) => "tool".to_string(),
                _ => "running".to_string(),
            }
        } else {
            "idle".to_string()
        };
        self.session_id = app.session_id.clone();
        if self.activity_timeline_len != app.timeline_len()
            || self.activity_full_sync_revision != app.timeline_full_sync_revision
        {
            let stats = app.session_activity_stats();
            self.thinking_count = stats.thinking_count as u32;
            self.tool_count = stats.tool_count as u32;
            self.reply_count = stats.message_count as u32;
            self.event_count = stats.event_count as u32;
            self.activity_timeline_len = app.timeline_len();
            self.activity_full_sync_revision = app.timeline_full_sync_revision;
        }
        self.approval_count = app
            .gateway_pending_approvals
            .unwrap_or_default()
            .max(app.permission_count as u64);
        self.permission_count = app.permission_count;
        self.last_phase = app
            .current_execution_status_detail
            .clone()
            .unwrap_or_else(|| match app.timeline_last() {
                Some(TimelineEntry::Thinking { complete, .. }) => if *complete {
                    "thought saved"
                } else {
                    "thinking"
                }
                .to_string(),
                Some(TimelineEntry::ToolCall { name, done, .. }) => {
                    if *done {
                        format!("tool {name} done")
                    } else {
                        format!("tool {name} running")
                    }
                }
                Some(TimelineEntry::Message { role, .. }) => format!("{role} message"),
                Some(TimelineEntry::SlashOutput { command, .. }) => format!("/{command} output"),
                None => "idle".to_string(),
            });
        self.daemon = app
            .gateway_runtime_readiness
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        self.gateway = if app.server_running {
            format!("up:{}s", app.server_uptime_secs.unwrap_or_default())
        } else {
            "down".to_string()
        };
        self.provider = app
            .gateway_connector_accounts
            .first()
            .map(|account| account.provider.clone())
            .unwrap_or_else(|| "unresolved".to_string());
        self.connectors = connector_health(app);
        self.memory = app
            .memory_status
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        self.mode = if app.compact_chat {
            "clean".to_string()
        } else {
            "panorama".to_string()
        };
        self.evidence = evidence_health(app);
        self.model = match (
            app.requested_model.as_deref(),
            app.effective_model.as_deref(),
        ) {
            (Some(requested), Some(effective)) if requested != effective => {
                format!("{}→{}", preview(requested, 12), preview(effective, 12))
            }
            (_, Some(effective)) => preview(effective, 26),
            (Some(requested), None) => format!("{}…", preview(requested, 24)),
            (None, None) => "unresolved".to_string(),
        };
        self.transport = match &app.stream_connection_state {
            crate::protocol::SessionStreamConnectionState::Connecting => "connecting".to_string(),
            crate::protocol::SessionStreamConnectionState::Connected if app.history_hydrated => {
                "live".to_string()
            }
            crate::protocol::SessionStreamConnectionState::Connected => "syncing".to_string(),
            crate::protocol::SessionStreamConnectionState::Reconnecting {
                attempt,
                after_cursor,
            } => after_cursor.map_or_else(
                || format!("reconnect:{attempt}"),
                |cursor| format!("reconnect:{attempt}@{cursor}"),
            ),
        };
        if matches!(
            app.stream_connection_state,
            crate::protocol::SessionStreamConnectionState::Connected
        ) {
            if let Some(projection_state) = app.projection_connection_state.as_ref() {
                self.transport = match projection_state {
                    crate::protocol::SessionStreamConnectionState::Connecting => {
                        "projection:connecting".to_string()
                    }
                    crate::protocol::SessionStreamConnectionState::Connected => {
                        self.transport.clone()
                    }
                    crate::protocol::SessionStreamConnectionState::Reconnecting {
                        attempt,
                        after_cursor,
                    } => after_cursor.map_or_else(
                        || format!("projection:reconnect:{attempt}"),
                        |cursor| format!("projection:reconnect:{attempt}@{cursor}"),
                    ),
                };
            }
        }
        self.history = app.session_history_index.as_ref().map_or_else(
            || "history:syncing".to_string(),
            |index| {
                let indexed = index
                    .indexed_through_sequence
                    .map_or(0, |sequence| sequence.saturating_add(1));
                format!(
                    "history:{indexed}/{} g{}",
                    index.total_messages, index.projection_generation
                )
            },
        );
        self.issue = app
            .gateway_degraded_reasons
            .first()
            .or_else(|| app.gateway_connector_degraded_reasons.first())
            .map(|reason| preview(reason, 34));
    }
}

impl Component for SystemStatusBar {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let turn_color = match self.turn.as_str() {
            "idle" => Color::DarkGray,
            "thinking" => Color::Yellow,
            "tool" => Color::Cyan,
            _ => Color::White,
        };

        let mut spans = vec![
            Span::styled(
                format!("cowd v{} ", env!("CARGO_PKG_VERSION")),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                self.transport.clone(),
                Style::default().fg(if self.transport == "live" {
                    Color::Green
                } else {
                    Color::Yellow
                }),
            ),
            sep(),
            Span::styled(self.turn.clone(), Style::default().fg(turn_color)),
            sep(),
            Span::styled(
                format!("model {}", self.model),
                Style::default().fg(if self.model.contains('→') {
                    Color::Yellow
                } else {
                    Color::White
                }),
            ),
            sep(),
            Span::styled(
                format!("session {}", short_session(&self.session_id)),
                Style::default().fg(Color::White),
            ),
            sep(),
            Span::styled(self.history.clone(), Style::default().fg(Color::DarkGray)),
            sep(),
            Span::styled(
                format!("approvals:{} ", self.approval_count),
                Style::default().fg(Color::Yellow),
            ),
            Span::styled(
                format!("perm:{} ", self.permission_count),
                Style::default().fg(Color::Yellow),
            ),
            Span::styled(
                format!(
                    "🧠:{} ⚙:{} 💬:{} ◇:{}",
                    self.thinking_count, self.tool_count, self.reply_count, self.event_count
                ),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                format!("  {}", preview(&self.last_phase, 28)),
                Style::default().fg(turn_color),
            ),
            sep(),
            Span::styled("mode ", Style::default().fg(Color::DarkGray)),
            Span::styled(self.mode.clone(), Style::default().fg(Color::Cyan)),
            Span::styled("  ev ", Style::default().fg(Color::DarkGray)),
            Span::styled(self.evidence.clone(), Style::default().fg(Color::DarkGray)),
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

fn execution_status_label(
    status: harness_contract::projection::ExecutionLiveStatus,
) -> &'static str {
    use harness_contract::projection::ExecutionLiveStatus;
    match status {
        ExecutionLiveStatus::Queued => "queued",
        ExecutionLiveStatus::PreparingContext => "context",
        ExecutionLiveStatus::CallingModel | ExecutionLiveStatus::Thinking => "thinking",
        ExecutionLiveStatus::CallingTool => "tool",
        ExecutionLiveStatus::WaitingApproval => "approval",
        ExecutionLiveStatus::Finalizing => "finalizing",
        ExecutionLiveStatus::Complete => "complete",
        ExecutionLiveStatus::Cancelled => "cancelled",
        ExecutionLiveStatus::Error => "error",
    }
}

fn runtime_health(app: &App) -> &'static str {
    if !app.gateway_degraded_reasons.is_empty()
        || !app.gateway_connector_degraded_reasons.is_empty()
    {
        "degraded"
    } else if app.gateway_pending_approvals.unwrap_or(0) > 0 {
        "blocked"
    } else if app.gateway_runtime_readiness.is_some() || app.server_running {
        "ready"
    } else {
        "unknown"
    }
}

fn connector_health(app: &App) -> String {
    if !app.gateway_connector_degraded_reasons.is_empty() {
        return format!("degraded:{}", app.gateway_connector_degraded_reasons.len());
    }
    let accounts = app.gateway_connector_accounts.len();
    let capabilities = app.gateway_connector_capabilities.len();
    if accounts == 0 && capabilities == 0 {
        "none".to_string()
    } else {
        format!("{}a/{}c", accounts, capabilities)
    }
}

fn evidence_health(app: &App) -> String {
    let context_count = app
        .latest_context_envelope
        .as_ref()
        .and_then(|value| value.get("selected").and_then(serde_json::Value::as_array))
        .map(Vec::len)
        .unwrap_or_default();
    let memory_count = app
        .latest_execution_graph_summary
        .as_ref()
        .map(|summary| summary.memory_candidates)
        .unwrap_or_default();
    let reality_count = app
        .gateway_fact_flow
        .as_ref()
        .map(|flow| {
            flow.stage_count + flow.event_count + flow.promotion_count + flow.boundary_count
        })
        .unwrap_or_default();
    format!("ctx:{context_count} mem:{memory_count} reality:{reality_count}")
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

fn short_session(id: &str) -> String {
    let chars = id.chars().count();
    if chars <= 14 {
        return id.to_string();
    }
    let head: String = id.chars().take(8).collect();
    let tail: String = id
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{head}…{tail}")
}

fn fit_spans(spans: Vec<Span<'static>>, max_width: usize) -> Vec<Span<'static>> {
    let mut used = 0usize;
    let mut fitted = Vec::new();
    for span in spans {
        let width = unicode_width::UnicodeWidthStr::width(span.content.as_ref());
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
    use crate::components::RenderContext;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;

    #[test]
    fn fit_spans_counts_terminal_cells_for_wide_glyphs() {
        let spans = vec![Span::raw("A"), Span::raw("🧠"), Span::raw("B")];

        let fitted = fit_spans(spans, 2);

        assert_eq!(fitted.len(), 1);
        assert_eq!(fitted[0].content.as_ref(), "A");
    }

    #[test]
    fn runtime_health_blocks_on_pending_approvals() {
        let mut app = App::new("m", "s");
        app.gateway_pending_approvals = Some(2);

        assert_eq!(runtime_health(&app), "blocked");
    }

    #[test]
    fn connector_health_summarizes_accounts_and_capabilities() {
        let mut app = App::new("m", "s");
        app.gateway_connector_accounts.push(
            crate::runtime_control_store::ConnectorAccountSummary {
                provider: "mock".into(),
                account_id: "a1".into(),
                auth_mode: "token".into(),
                status: "ready".into(),
                reason: None,
                binding_count: 1,
            },
        );
        app.gateway_connector_capabilities.push(
            crate::runtime_control_store::ConnectorCapabilitySummary {
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
    fn evidence_health_summarizes_current_turn_signals() {
        let mut app = App::new("m", "s");
        app.compact_chat = true;
        app.latest_context_envelope = Some(serde_json::json!({
            "selected": [{"id": "a"}, {"id": "b"}],
            "omitted": []
        }));
        app.latest_execution_graph_summary = Some(crate::RuntimeExecutionGraphSummary {
            graph_id: None,
            board_id: None,
            status: "ready".into(),
            agent_tasks: 0,
            child_executions: 0,
            memory_candidates: 3,
            conflicts: 0,
            completion_rate: None,
            synthesis_lift: None,
            complementarity_score: None,
        });
        app.gateway_fact_flow = Some(crate::runtime_control_store::FactFlowSummary {
            source: "test".into(),
            session_id: Some("s".into()),
            stage_count: 1,
            event_count: 2,
            promotion_count: 1,
            boundary_count: 1,
        });

        assert_eq!(evidence_health(&app), "ctx:2 mem:3 reality:5");
    }

    #[test]
    fn render_system_status_bar_keeps_top_line_calm() {
        let mut app = App::new("deepseek-v4-pro", "s");
        app.effective_model = Some("deepseek-v4-flash".into());
        app.history_hydrated = true;
        app.stream_connection_state = crate::protocol::SessionStreamConnectionState::Connected;
        let mut bar = SystemStatusBar::new();
        bar.sync_from_app(&app);

        let mut terminal = MockTerminal::new(100, 3);
        let skin = SkinConfig::default();
        terminal.draw(|frame| {
            let mut ctx = RenderContext::new(frame, &skin);
            bar.render(&mut ctx, Rect::new(0, 0, 100, 1));
        });

        let joined = terminal.buffer_lines().join("\n");
        assert!(joined.contains("session"));
        assert!(joined.contains("model"));
        assert!(joined.contains("deepseek"));
        assert!(joined.contains("live"));
        assert!(!joined.contains("state"));
        assert!(!joined.contains("provider"));
        assert!(!joined.contains("gateway"));
        assert!(!joined.contains("connectors"));
    }
}
