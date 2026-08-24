use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::App;
use crate::components::{Component, EventResult, RenderContext};
use crate::runtime_control_store::{ApprovalGrantSummary, ApprovalSummary};

#[derive(Debug, Clone, Default)]
pub struct ApprovalCockpitPanel {
    approval_items: Vec<ApprovalSummary>,
    approval_grants: Vec<ApprovalGrantSummary>,
    permission_count: usize,
    pending_approvals: Option<u64>,
    cross_plane_grants_active: Option<u64>,
    cross_plane_actions_24h: Option<u64>,
    lease_owner: Option<String>,
    lease_mode: Option<String>,
    focused_backlink_target: Option<String>,
    focused_backlink_resolution: Option<String>,
    degraded_reasons: Vec<String>,
    gateway_running: bool,
}

impl ApprovalCockpitPanel {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.approval_items = app.gateway_approval_items.clone();
        self.approval_grants = app.gateway_approval_grants.clone();
        self.permission_count = app.permission_count;
        self.pending_approvals = app.gateway_pending_approvals;
        self.cross_plane_grants_active = app.gateway_cross_plane_grants_active;
        self.cross_plane_actions_24h = app.gateway_cross_plane_actions_24h;
        self.lease_owner = app.gateway_lease_owner.clone();
        self.lease_mode = app.gateway_lease_mode.clone();
        self.degraded_reasons = app.gateway_degraded_reasons.clone();
        self.gateway_running = app.server_running;
    }

    pub fn focus_backlink_target(&mut self, target: impl Into<String>) {
        let target = target.into();
        let approval_id = target
            .strip_prefix("approval://")
            .and_then(|value| value.split(['?', '#']).next())
            .unwrap_or(target.as_str());
        if let Some(index) = self.approval_items.iter().position(|item| {
            approval_id == item.id
                || item
                    .review_ref
                    .as_deref()
                    .is_some_and(|review| approval_id == review)
        }) {
            self.approval_items.rotate_left(index);
        }
        self.focused_backlink_resolution =
            Some("loading exact approval from pending and history".to_string());
        self.focused_backlink_target = Some(target);
    }

    pub fn clear_backlink_target(&mut self) {
        self.focused_backlink_target = None;
        self.focused_backlink_resolution = None;
    }

    #[must_use]
    pub fn accepts_backlink_result(&self, target: &str) -> bool {
        self.focused_backlink_target.as_deref() == Some(target)
    }

    pub fn record_backlink_object(
        &mut self,
        target: impl Into<String>,
        object: &serde_json::Value,
    ) {
        let target = target.into();
        if !self.accepts_backlink_result(&target) {
            return;
        }
        let id = object
            .get("id")
            .or_else(|| object.get("approval_id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("canonical approval");
        let status = object
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("loaded");
        self.focused_backlink_resolution = Some(format!("{id} status {status}"));
    }

    pub fn record_backlink_failure(
        &mut self,
        target: impl Into<String>,
        message: impl Into<String>,
    ) {
        let target = target.into();
        if !self.accepts_backlink_result(&target) {
            return;
        }
        self.focused_backlink_resolution = Some(format!("Resolution failed: {}", message.into()));
    }

    fn render_lines(&self) -> Text<'_> {
        let pending = self.pending_approvals.unwrap_or_default();
        let blocking = self
            .approval_items
            .iter()
            .filter(|item| item.blocks_execution)
            .count();
        let confirmations = self
            .approval_items
            .iter()
            .filter(|item| item.is_confirmation())
            .count();
        let grants = self.cross_plane_grants_active.unwrap_or_default();
        let actions = self.cross_plane_actions_24h.unwrap_or_default();
        let mut lines = Vec::new();
        if let Some(target) = self.focused_backlink_target.as_deref() {
            lines.push(Line::from(vec![
                Span::styled(
                    "Backlink target: ",
                    Style::default().fg(Color::LightMagenta),
                ),
                Span::styled(target.to_string(), Style::default().fg(Color::White)),
            ]));
            lines.push(Line::from(vec![
                Span::styled(
                    "Resolved object: ",
                    Style::default().fg(Color::LightMagenta),
                ),
                Span::styled(
                    self.focused_backlink_resolution
                        .as_deref()
                        .unwrap_or("loading exact approval")
                        .to_string(),
                    Style::default().fg(Color::White),
                ),
            ]));
        }
        lines.extend([
            Line::from(vec![
                Span::styled("Gateway: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    if self.gateway_running {
                        "connected"
                    } else {
                        "offline"
                    },
                    Style::default().fg(if self.gateway_running {
                        Color::Green
                    } else {
                        Color::Red
                    }),
                ),
            ]),
            Line::from(format!(
                "Lease: {} / {}",
                self.lease_owner.as_deref().unwrap_or("none"),
                self.lease_mode.as_deref().unwrap_or("detached")
            )),
            Line::raw(""),
            Line::from(Span::styled(
                "Approval Queue",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(format!(
                "Pending daemon items: {pending} (blocks: {blocking}, confirmations: {confirmations})"
            )),
            Line::from(format!(
                "Local permission prompts: {}",
                self.permission_count
            )),
        ]);

        if !self.approval_items.is_empty() {
            lines.push(Line::from(Span::styled(
                "Gateway queue",
                Style::default().fg(Color::Cyan),
            )));
            for item in self.approval_items.iter().take(3) {
                let risk = item.risk.as_deref().unwrap_or("unknown");
                let requester = item.requester.as_deref().unwrap_or("unknown");
                lines.push(Line::from(vec![
                    Span::styled(short_id(&item.id), Style::default().fg(Color::Cyan)),
                    Span::raw(" "),
                    Span::styled(item.tool_name.clone(), Style::default().fg(Color::Yellow)),
                    Span::styled(
                        format!(" [{risk}] {requester}"),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
                if !item.input_preview.is_empty() {
                    lines.push(Line::from(Span::styled(
                        format!("  {}", truncate(&item.input_preview, 64)),
                        Style::default().fg(Color::DarkGray),
                    )));
                }
                lines.push(Line::from(Span::styled(
                    if item.is_confirmation() {
                        "  Confirmation — execution continues; deny only to veto"
                    } else {
                        "  Approval required — execution waits for a decision"
                    },
                    Style::default().fg(if item.is_confirmation() {
                        Color::LightCyan
                    } else {
                        Color::LightYellow
                    }),
                )));
                if let Some(policy) = item.timeout_policy.as_deref() {
                    let behavior = item.timeout_behavior.as_deref().unwrap_or(policy);
                    lines.push(Line::from(Span::styled(
                        format!("  On timeout: {}", humanize_identifier(behavior)),
                        Style::default().fg(Color::DarkGray),
                    )));
                }
                if item.has_application_review() {
                    let app_id = item.application_source_id().unwrap_or("app");
                    lines.push(Line::from(Span::styled(
                        format!(
                            "  application review {} · /{} review {}; generic boolean disabled",
                            item.review_ref.as_deref().unwrap_or("unknown"),
                            app_id,
                            item.review_ref.as_deref().unwrap_or("unknown")
                        ),
                        Style::default().fg(Color::LightMagenta),
                    )));
                } else if item.application_source_id().is_some() {
                    lines.push(Line::from(Span::styled(
                        "  invalid application approval: review reference missing; generic boolean disabled",
                        Style::default().fg(Color::LightRed),
                    )));
                }
            }
        }

        if !self.approval_grants.is_empty() {
            lines.push(Line::from(Span::styled(
                "Runtime grants",
                Style::default().fg(Color::Cyan),
            )));
            for grant in self
                .approval_grants
                .iter()
                .filter(|grant| grant.status == "active")
                .take(3)
            {
                lines.push(Line::from(format!(
                    "{} [{}] {}",
                    truncate(&grant.capability, 28),
                    grant.scope,
                    truncate(&grant.workspace_key, 24)
                )));
            }
        }

        lines.extend([
            Line::raw(""),
            Line::from(Span::styled(
                "Cross-Plane Grants",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(format!("Active grants: {grants}")),
            Line::from(format!("Actions in 24h: {actions}")),
            Line::from("Policy: explicit grant + preflight for cross-channel actions"),
            Line::raw(""),
            Line::from(Span::styled(
                "/approvals  /gateway cross-plane  /permissions",
                Style::default().fg(Color::DarkGray),
            )),
        ]);

        if !self.degraded_reasons.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Projection degraded",
                Style::default().fg(Color::Red),
            )));
            for reason in self.degraded_reasons.iter().take(3) {
                lines.push(Line::from(Span::styled(
                    format!("- {}", truncate(reason, 56)),
                    Style::default().fg(Color::DarkGray),
                )));
            }
        }

        Text::from(lines)
    }
}

impl Component for ApprovalCockpitPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if area.width < 12 || area.height < 3 {
            return;
        }
        let risk = self.pending_approvals.unwrap_or_default()
            + self.cross_plane_grants_active.unwrap_or_default();
        let title = if risk > 0 {
            format!(" Approvals ({risk}) ")
        } else {
            " Approvals ".to_string()
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(ctx.theme().accent_color()));
        let inner = block.inner(area);
        ctx.frame_mut().render_widget(block, area);
        ctx.frame_mut().render_widget(
            Paragraph::new(self.render_lines()).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "approval_cockpit_panel"
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut out = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    out.push_str("...");
    out
}

fn short_id(id: &str) -> &str {
    id.get(..id.len().min(10)).unwrap_or(id)
}

fn humanize_identifier(value: &str) -> String {
    value.replace(['_', '-'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;

    fn render_panel(panel: &mut ApprovalCockpitPanel, width: u16, height: u16) -> String {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|frame| {
            let area = frame.area();
            let mut ctx = RenderContext::new(frame, &skin);
            panel.render(&mut ctx, area);
        });
        terminal.buffer_lines().join("\n")
    }

    #[test]
    fn renders_approval_and_cross_plane_summary() {
        let mut app = App::new("model", "session");
        app.server_running = true;
        app.permission_count = 2;
        app.gateway_pending_approvals = Some(1);
        app.gateway_approval_items = vec![ApprovalSummary {
            id: "approval-123456789".to_string(),
            tool_name: "bash".to_string(),
            risk: Some("high".to_string()),
            requester: Some("session".to_string()),
            input_preview: "rm -rf /tmp/example".to_string(),
            blocks_execution: true,
            timeout_policy: Some("pending".to_string()),
            timeout_behavior: Some("execution_waits_for_timeout_resolution".to_string()),
            ..ApprovalSummary::default()
        }];
        app.gateway_cross_plane_grants_active = Some(3);
        app.gateway_cross_plane_actions_24h = Some(9);
        app.gateway_lease_owner = Some("tui:session".to_string());
        app.gateway_lease_mode = Some("attached".to_string());
        let mut panel = ApprovalCockpitPanel::new();
        panel.sync_from_app(&app);

        let rendered = render_panel(&mut panel, 82, 16);
        assert!(rendered.contains("Approvals (4)"), "{rendered}");
        assert!(
            rendered.contains("Pending daemon items: 1 (blocks: 1, confirmations: 0)"),
            "{rendered}"
        );
        assert!(rendered.contains("Active grants: 3"), "{rendered}");
        assert!(rendered.contains("bash"), "{rendered}");
        assert!(rendered.contains("approval-1"), "{rendered}");
        assert!(rendered.contains("high"), "{rendered}");
        assert!(
            rendered.contains("Approval required — execution waits for a decision"),
            "{rendered}"
        );
    }

    #[test]
    fn renders_nonblocking_confirmation_as_a_veto_window() {
        let mut app = App::new("model", "session");
        app.server_running = true;
        app.gateway_pending_approvals = Some(1);
        app.gateway_approval_items = vec![ApprovalSummary {
            id: "confirmation-123456789".to_string(),
            tool_name: "definition.template.publish".to_string(),
            risk: Some("medium".to_string()),
            input_preview: "publish shared template".to_string(),
            blocks_execution: false,
            timeout_policy: Some("continue_alternative".to_string()),
            timeout_behavior: Some("continue_alternative_after_deadline".to_string()),
            ..ApprovalSummary::default()
        }];
        let mut panel = ApprovalCockpitPanel::new();
        panel.sync_from_app(&app);

        let rendered = render_panel(&mut panel, 88, 18);
        assert!(
            rendered.contains("Pending daemon items: 1 (blocks: 0, confirmations: 1)"),
            "{rendered}"
        );
        assert!(
            rendered.contains("Confirmation — execution continues; deny only to veto"),
            "{rendered}"
        );
        assert!(
            rendered.contains("On timeout: continue alternative after deadline"),
            "{rendered}"
        );
    }

    #[test]
    fn renders_degraded_projection_reasons() {
        let mut app = App::new("model", "session");
        app.gateway_degraded_reasons = vec!["approval projection unavailable".to_string()];

        let mut panel = ApprovalCockpitPanel::new();
        panel.sync_from_app(&app);

        let rendered = render_panel(&mut panel, 72, 20);
        assert!(rendered.contains("Projection degraded"), "{rendered}");
        assert!(
            rendered.contains("approval projection unavailable"),
            "{rendered}"
        );
    }
}
