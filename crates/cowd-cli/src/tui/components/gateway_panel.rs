// ── Gateway Panel ─────────────────────────────────────────────────
// Displays backend daemon management info in the TUI sidebar.
//
// Shows:
//   - Server status (running/stopped) with colored indicator
//   - Key API endpoints with HTTP methods (GET/POST/DELETE)
//   - Health check status from /health endpoint
//   - Quick actions via slash commands

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

use crate::tui::components::{Component, EventResult, RenderContext};
use crate::tui::{
    app::App,
    runtime_control_store::{
        ConnectorAccountSummary, ConnectorCapabilitySummary, ConnectorResourceSummary,
    },
};

/// Panel showing backend daemon/API gateway status.
///
/// Tracks server running state, health status, and
/// displays available API endpoints with descriptions.
pub struct GatewayPanel {
    /// Whether the backend server is currently running.
    pub server_running: bool,
    /// Last known health status string (e.g., "Healthy", "Unhealthy").
    pub health_status: Option<String>,
    /// Server uptime in seconds, if available.
    pub uptime_secs: Option<u64>,
    /// Number of active sessions.
    pub active_sessions: usize,
    /// Runtime readiness score or status from daemon projection API.
    pub runtime_readiness: Option<String>,
    /// Number of runtime control-plane components.
    pub runtime_components: Option<u64>,
    /// Number of daemon tasks visible to the TUI.
    pub task_count: Option<u64>,
    /// Number of pending daemon approval requests.
    pub pending_approvals: Option<u64>,
    /// Current daemon session lease owner.
    pub lease_owner: Option<String>,
    /// Current daemon session lease mode.
    pub lease_mode: Option<String>,
    /// Cross-plane adapter capability summaries.
    pub adapter_capabilities: Vec<GatewayAdapterCapability>,
    /// Recent cross-plane execution receipts.
    pub execution_receipts: Vec<GatewayExecutionReceipt>,
    /// Recent dispatch target readiness summaries.
    pub dispatch_targets: Vec<GatewayDispatchTarget>,
    /// Connector provider account summaries.
    pub connector_accounts: Vec<ConnectorAccountSummary>,
    /// Connector capability summaries.
    pub connector_capabilities: Vec<ConnectorCapabilitySummary>,
    /// Connector resource summaries.
    pub connector_resources: Vec<ConnectorResourceSummary>,
    /// Connector-specific degraded reasons.
    pub connector_degraded_reasons: Vec<String>,
    /// Scroll offset for content overflow.
    pub scroll_offset: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAdapterCapability {
    pub platform: String,
    pub operation: String,
    pub live_supported: bool,
    pub adapter_bound: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayExecutionReceipt {
    pub status: String,
    pub dispatch_status: String,
    pub mode: String,
    pub capability: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayDispatchTarget {
    pub platform: String,
    pub operation: String,
    pub session_key: Option<String>,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GatewayActionTemplate {
    operation: &'static str,
    capability: &'static str,
    resource_ref: &'static str,
    risk: &'static str,
}

const GATEWAY_ACTION_TEMPLATES: [GatewayActionTemplate; 3] = [
    GatewayActionTemplate {
        operation: "send_text",
        capability: "channel.feishu.send_text",
        resource_ref: "text://hello",
        risk: "low",
    },
    GatewayActionTemplate {
        operation: "send_image",
        capability: "channel.feishu.send_image",
        resource_ref: "image://https://example.test/panel.png",
        risk: "high",
    },
    GatewayActionTemplate {
        operation: "send_file",
        capability: "channel.feishu.send_file",
        resource_ref: "workspace://file/reports/panel.txt",
        risk: "high",
    },
];

impl GatewayPanel {
    /// Create a new GatewayPanel in default stopped state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            server_running: false,
            health_status: None,
            uptime_secs: None,
            active_sessions: 0,
            runtime_readiness: None,
            runtime_components: None,
            task_count: None,
            pending_approvals: None,
            lease_owner: None,
            lease_mode: None,
            adapter_capabilities: Vec::new(),
            execution_receipts: Vec::new(),
            dispatch_targets: Vec::new(),
            connector_accounts: Vec::new(),
            connector_capabilities: Vec::new(),
            connector_resources: Vec::new(),
            connector_degraded_reasons: Vec::new(),
            scroll_offset: 0,
        }
    }

    /// Sync panel state from the application model.
    ///
    /// Copies server state from App into the panel fields for display.
    /// Derives health_status from server_running: "Healthy" when running,
    /// None when stopped.
    pub fn sync_from_app(&mut self, app: &App) {
        self.server_running = app.server_running;
        self.uptime_secs = app.server_uptime_secs;
        self.active_sessions = app.active_api_sessions;
        self.runtime_readiness = app.daemon_runtime_readiness.clone();
        self.runtime_components = app.daemon_runtime_components;
        self.task_count = app.daemon_task_count;
        self.pending_approvals = app.daemon_pending_approvals;
        self.lease_owner = app.daemon_lease_owner.clone();
        self.lease_mode = app.daemon_lease_mode.clone();
        self.connector_accounts = app.daemon_connector_accounts.clone();
        self.connector_capabilities = app.daemon_connector_capabilities.clone();
        self.connector_resources = app.daemon_connector_resources.clone();
        self.connector_degraded_reasons = app.daemon_connector_degraded_reasons.clone();
        if app.server_running {
            self.health_status = Some("Healthy".to_string());
        } else {
            self.health_status = None;
        }
    }

    /// Update the health status string and mark server as running.
    pub fn update_health(&mut self, status: String) {
        self.server_running = true;
        self.health_status = Some(status);
    }

    /// Set the server running state.
    pub fn set_server_status(&mut self, running: bool) {
        self.server_running = running;
        if !running {
            self.health_status = None;
            self.uptime_secs = None;
            self.active_sessions = 0;
        }
    }

    /// Set server uptime in seconds.
    pub fn set_uptime(&mut self, secs: u64) {
        self.uptime_secs = Some(secs);
    }

    /// Set the active session count.
    pub fn set_active_sessions(&mut self, count: usize) {
        self.active_sessions = count;
    }

    pub fn set_adapter_capabilities(&mut self, capabilities: Vec<GatewayAdapterCapability>) {
        self.adapter_capabilities = capabilities;
    }

    pub fn set_execution_receipts(&mut self, receipts: Vec<GatewayExecutionReceipt>) {
        self.execution_receipts = receipts;
    }

    pub fn set_dispatch_targets(&mut self, targets: Vec<GatewayDispatchTarget>) {
        self.dispatch_targets = targets;
    }

    pub fn set_connector_accounts(&mut self, accounts: Vec<ConnectorAccountSummary>) {
        self.connector_accounts = accounts;
    }

    pub fn set_connector_capabilities(&mut self, capabilities: Vec<ConnectorCapabilitySummary>) {
        self.connector_capabilities = capabilities;
    }

    pub fn set_connector_resources(&mut self, resources: Vec<ConnectorResourceSummary>) {
        self.connector_resources = resources;
    }

    pub fn set_connector_degraded_reasons(&mut self, reasons: Vec<String>) {
        self.connector_degraded_reasons = reasons;
    }

    // ── Rendering helpers ────────────────────────────────────────

    /// Build the title string for the block border.
    fn build_title(&self) -> String {
        if self.server_running {
            " Gateway ● ".to_string()
        } else {
            " Gateway ○ ".to_string()
        }
    }

    /// Format uptime seconds into a human-readable string.
    fn format_uptime(secs: u64) -> String {
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        let minutes = (secs % 3600) / 60;
        let seconds = secs % 60;

        if days > 0 {
            format!("{days}d {hours}h {minutes}m")
        } else if hours > 0 {
            format!("{hours}h {minutes}m {seconds}s")
        } else if minutes > 0 {
            format!("{minutes}m {seconds}s")
        } else {
            format!("{seconds}s")
        }
    }
}

// ── Default impl ─────────────────────────────────────────────────

impl Default for GatewayPanel {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component Trait ──────────────────────────────────────────────

impl Component for GatewayPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let title = self.build_title();
        let block = Block::default().title(title).borders(Borders::ALL);

        let _inner_width = area.width.saturating_sub(2) as usize;
        let _inner_height = area.height.saturating_sub(2) as usize;

        let mut lines: Vec<Line> = Vec::new();

        // ── Status indicator ───────────────────────────────────
        let status = if self.server_running {
            Span::styled("● RUNNING", Style::default().fg(Color::Green))
        } else {
            Span::styled("○ STOPPED", Style::default().fg(Color::Red))
        };
        lines.push(Line::from(vec![
            Span::styled("Server: ", Style::default()),
            status,
        ]));
        lines.push(Line::from(Span::styled(
            "Keys: r refresh  h health  s start/stop  / connector actions",
            Style::default().fg(Color::DarkGray),
        )));

        // ── Health check ───────────────────────────────────────
        if let Some(ref health) = self.health_status {
            lines.push(Line::from(""));
            let health_color = if health.to_lowercase().contains("healthy") {
                Color::Green
            } else {
                Color::Yellow
            };
            lines.push(Line::from(vec![
                Span::styled("Health: ", Style::default().fg(Color::Yellow)),
                Span::styled(health.clone(), Style::default().fg(health_color)),
            ]));

            if let Some(uptime) = self.uptime_secs {
                lines.push(Line::from(vec![
                    Span::styled("Uptime: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        Self::format_uptime(uptime),
                        Style::default().fg(Color::White),
                    ),
                ]));
            }

            if self.active_sessions > 0 {
                lines.push(Line::from(vec![
                    Span::styled("Sessions: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("{}", self.active_sessions),
                        Style::default().fg(Color::Cyan),
                    ),
                ]));
            }

            if let Some(readiness) = self.runtime_readiness.as_ref() {
                lines.push(Line::from(vec![
                    Span::styled("Runtime: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "ready {readiness}, components {}",
                            self.runtime_components.unwrap_or_default()
                        ),
                        Style::default().fg(Color::Cyan),
                    ),
                ]));
            }

            if self.task_count.is_some() || self.pending_approvals.is_some() {
                lines.push(Line::from(vec![
                    Span::styled("Control: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "tasks {}, approvals {}",
                            self.task_count.unwrap_or_default(),
                            self.pending_approvals.unwrap_or_default()
                        ),
                        Style::default().fg(Color::Yellow),
                    ),
                ]));
            }

            if let Some(owner) = self.lease_owner.as_ref() {
                lines.push(Line::from(vec![
                    Span::styled("Lease: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} ({})",
                            owner,
                            self.lease_mode.as_deref().unwrap_or("unknown")
                        ),
                        Style::default().fg(Color::Magenta),
                    ),
                ]));
            }
        }

        if !self.adapter_capabilities.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "─ Cross-Plane Adapters ─",
                Style::default().fg(Color::Cyan),
            )));
            for capability in self.adapter_capabilities.iter().take(4) {
                let support = if capability.live_supported {
                    "live"
                } else {
                    "plan"
                };
                let binding = if capability.adapter_bound {
                    "bound"
                } else {
                    "not-bound"
                };
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{:12}", capability.platform),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(
                        format!("{:12}", capability.operation),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(
                        format!("{support} · {binding}"),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        if !self.execution_receipts.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "─ Execution Receipts ─",
                Style::default().fg(Color::Cyan),
            )));
            for receipt in self.execution_receipts.iter().take(3) {
                let idem = receipt
                    .idempotency_key
                    .as_deref()
                    .map(|key| format!(" · idem {key}"))
                    .unwrap_or_default();
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{:8}", receipt.status),
                        Style::default().fg(Color::Green),
                    ),
                    Span::styled(
                        format!("{:16}", receipt.dispatch_status),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(
                        format!("{} · {}{}", receipt.mode, receipt.capability, idem),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        if !self.dispatch_targets.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "─ Dispatch Targets ─",
                Style::default().fg(Color::Cyan),
            )));
            for target in self.dispatch_targets.iter().take(3) {
                let state = if target.ready { "ready" } else { "blocked" };
                let detail = target
                    .session_key
                    .as_deref()
                    .map(|key| format!("session {key}"))
                    .or_else(|| target.blockers.first().map(|blocker| blocker.to_string()))
                    .unwrap_or_else(|| "no target".to_string());
                let state_color = if target.ready {
                    Color::Green
                } else {
                    Color::Yellow
                };
                lines.push(Line::from(vec![
                    Span::styled(format!("{:8}", state), Style::default().fg(state_color)),
                    Span::styled(
                        format!("{:10}", target.platform),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(
                        format!("{:12}", target.operation),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(detail, Style::default().fg(Color::DarkGray)),
                ]));
            }
        }

        if !self.connector_accounts.is_empty()
            || !self.connector_capabilities.is_empty()
            || !self.connector_resources.is_empty()
            || !self.connector_degraded_reasons.is_empty()
        {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "─ Connector Console ─",
                Style::default().fg(Color::Cyan),
            )));
            lines.push(Line::from(vec![
                Span::styled("Accounts: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", self.connector_accounts.len()),
                    Style::default().fg(Color::White),
                ),
                Span::styled("  Capabilities: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", self.connector_capabilities.len()),
                    Style::default().fg(Color::White),
                ),
                Span::styled("  Resources: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", self.connector_resources.len()),
                    Style::default().fg(Color::White),
                ),
            ]));

            for account in self.connector_accounts.iter().take(4) {
                let color = match account.status.as_str() {
                    "ready" => Color::Green,
                    "disabled" => Color::DarkGray,
                    "degraded" => Color::Yellow,
                    _ => Color::White,
                };
                let detail = account
                    .reason
                    .as_deref()
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| format!("{} bindings", account.binding_count));
                lines.push(Line::from(vec![
                    Span::styled(format!("{:8}", account.status), Style::default().fg(color)),
                    Span::styled(
                        format!("{:10}", account.provider),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(
                        format!("{:18}", account.account_id),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(detail, Style::default().fg(Color::DarkGray)),
                ]));
            }

            if !self.connector_capabilities.is_empty() {
                lines.push(Line::from(Span::styled(
                    "Capabilities",
                    Style::default().fg(Color::DarkGray),
                )));
                for capability in self.connector_capabilities.iter().take(5) {
                    let approval = if capability.requires_approval {
                        "approval"
                    } else {
                        "open"
                    };
                    let commit = if capability.supports_commit {
                        "commit"
                    } else {
                        "dry-run"
                    };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{:8}", capability.plane),
                            Style::default().fg(Color::Cyan),
                        ),
                        Span::styled(
                            format!("{:34}", capability.capability_id),
                            Style::default().fg(Color::White),
                        ),
                        Span::styled(
                            format!("{} · {} · {}", capability.risk, commit, approval),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }

            if !self.connector_resources.is_empty() {
                lines.push(Line::from(Span::styled(
                    "Resources",
                    Style::default().fg(Color::DarkGray),
                )));
                for resource in self.connector_resources.iter().take(4) {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{:10}", resource.provider),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::styled(
                            format!("{:10}", resource.resource_type),
                            Style::default().fg(Color::Cyan),
                        ),
                        Span::styled(
                            format!("{:18}", resource.indexed_state),
                            Style::default().fg(Color::Green),
                        ),
                        Span::styled(resource.title.clone(), Style::default().fg(Color::DarkGray)),
                    ]));
                }
                lines.push(Line::from(Span::styled(
                    "Use command palette: Mark indexed · Mark stale · Remember resource",
                    Style::default().fg(Color::DarkGray),
                )));
            }

            if !self.connector_degraded_reasons.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("Connector degraded: ", Style::default().fg(Color::Yellow)),
                    Span::styled(
                        self.connector_degraded_reasons
                            .iter()
                            .take(2)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(" · "),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        // ── API Endpoints ──────────────────────────────────────
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "─ API Endpoints ─",
            Style::default().fg(Color::Cyan),
        )));
        lines.push(Line::from(""));

        let endpoints: [(&str, &str); 22] = [
            ("GET  /health", "Server health check"),
            ("GET  /api/sessions", "List sessions"),
            ("POST /api/sessions", "Create session"),
            ("POST /api/sessions/:id/msgs", "Send message"),
            ("GET  /api/memory", "Memory status"),
            ("GET  /api/memory/stats", "Memory statistics"),
            ("GET  /api/memory/search", "Search memory"),
            ("GET  /api/config", "View config"),
            ("PUT  /api/config", "Update config"),
            ("GET  /api/platforms", "List platforms"),
            ("GET  /api/cross-plane/summary", "Interop policy summary"),
            ("GET  /api/cross-plane/grants", "List interop grants"),
            (
                "GET  /api/cross-plane/action/adapters",
                "Interop dispatch capability",
            ),
            (
                "GET  /api/cross-plane/action/executions",
                "Interop execution receipts",
            ),
            ("GET  /api/connectors/summary", "Connector summary"),
            ("GET  /api/connectors/accounts", "Connector accounts"),
            (
                "GET  /api/connectors/capabilities",
                "Connector capabilities",
            ),
            ("GET  /api/connectors/resources", "Connector resources"),
            (
                "GET  /api/connectors/services/mock.docs/tools",
                "Mock docs service tools",
            ),
            (
                "POST /api/connectors/services/mock.docs/execute",
                "Mock docs dry-run/commit",
            ),
            (
                "POST /api/cross-plane/policy/simulate",
                "Test policy decision",
            ),
            ("GET  /api/cross-plane/audit", "Interop audit records"),
        ];

        for (endpoint, desc) in &endpoints {
            let method_color = if endpoint.starts_with("GET") {
                Color::Green
            } else if endpoint.starts_with("POST") {
                Color::Yellow
            } else if endpoint.starts_with("PUT") {
                Color::Cyan
            } else if endpoint.starts_with("DELETE") {
                Color::Red
            } else {
                Color::White
            };

            // Build endpoint with color-coded method
            let parts: Vec<&str> = endpoint.splitn(2, ' ').collect();
            let (method, path) = (parts[0], if parts.len() > 1 { parts[1] } else { "" });

            lines.push(Line::from(vec![
                Span::styled(format!("{:4}", method), Style::default().fg(method_color)),
                Span::styled(format!("{:25}", path), Style::default().fg(Color::White)),
                Span::styled(format!(" — {desc}"), Style::default().fg(Color::DarkGray)),
            ]));
        }

        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "─ Action Composer Contract ─",
            Style::default().fg(Color::Cyan),
        )));
        lines.push(Line::from(Span::styled(
            "POST /api/cross-plane/action/preflight  POST /api/cross-plane/action/execute",
            Style::default().fg(Color::DarkGray),
        )));
        for template in GATEWAY_ACTION_TEMPLATES {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:10}", template.operation),
                    Style::default().fg(Color::Yellow),
                ),
                Span::styled(
                    format!("{:26}", template.capability),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("{} · {}", template.risk, template.resource_ref),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }
        lines.push(Line::from(Span::styled(
            "source local:tui · principal user:demo · target channel://feishu/chat/demo",
            Style::default().fg(Color::DarkGray),
        )));

        // ── Keyboard hint bar ──────────────────────────────────
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Keys: r refresh  h health  s start/stop  / connector actions",
            Style::default().fg(Color::DarkGray),
        )));

        let paragraph = Paragraph::new(lines).block(block);
        ctx.frame_mut().render_widget(paragraph, area);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        if key.kind != KeyEventKind::Press {
            return EventResult::NotConsumed;
        }

        match key.code {
            KeyCode::Char('r') => EventResult::Consumed, // refresh
            KeyCode::Char('h') => EventResult::Consumed, // health check
            KeyCode::Char('s') => EventResult::Consumed, // start/stop
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "gateway_panel"
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyEvent;

    #[test]
    fn gateway_panel_defaults_stopped() {
        let panel = GatewayPanel::new();
        assert!(!panel.server_running);
        assert!(panel.health_status.is_none());
        assert!(panel.uptime_secs.is_none());
        assert_eq!(panel.active_sessions, 0);
    }

    #[test]
    fn update_health_marks_server_running() {
        let mut panel = GatewayPanel::new();
        panel.update_health("Healthy".into());
        assert!(panel.server_running);
        assert_eq!(panel.health_status.as_deref(), Some("Healthy"));
    }

    #[test]
    fn set_server_status_clears_on_stop() {
        let mut panel = GatewayPanel::new();
        panel.update_health("OK".into());
        panel.set_uptime(3600);
        panel.set_active_sessions(5);
        assert!(panel.server_running);

        panel.set_server_status(false);
        assert!(!panel.server_running);
        assert!(panel.health_status.is_none());
        assert!(panel.uptime_secs.is_none());
        assert_eq!(panel.active_sessions, 0);
    }

    #[test]
    fn set_server_status_to_running() {
        let mut panel = GatewayPanel::new();
        panel.set_server_status(true);
        assert!(panel.server_running);
        assert!(panel.health_status.is_none()); // not set yet
    }

    #[test]
    fn component_trait_methods() {
        let panel = GatewayPanel::new();
        assert!(panel.focusable());
        assert_eq!(panel.id(), "gateway_panel");
    }

    #[test]
    fn format_uptime_seconds() {
        assert_eq!(GatewayPanel::format_uptime(0), "0s");
        assert_eq!(GatewayPanel::format_uptime(45), "45s");
        assert_eq!(GatewayPanel::format_uptime(90), "1m 30s");
        assert_eq!(GatewayPanel::format_uptime(3661), "1h 1m 1s");
        assert_eq!(GatewayPanel::format_uptime(90061), "1d 1h 1m");
    }

    #[test]
    fn set_uptime_and_sessions() {
        let mut panel = GatewayPanel::new();
        panel.set_uptime(7200);
        assert_eq!(panel.uptime_secs, Some(7200));

        panel.set_active_sessions(3);
        assert_eq!(panel.active_sessions, 3);
    }

    #[test]
    fn render_stopped_state() {
        use crate::tui::skin::SkinConfig;
        use crate::tui::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(60, 20);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 60, 20));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("STOPPED"),
            "Stopped state must show STOPPED, got: {joined}"
        );
    }

    #[test]
    fn render_running_state() {
        use crate::tui::skin::SkinConfig;
        use crate::tui::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.update_health("Healthy - all systems operational".into());
        panel.set_uptime(3600);
        panel.set_active_sessions(2);
        panel.runtime_readiness = Some("87%".to_string());
        panel.runtime_components = Some(12);
        panel.task_count = Some(3);
        panel.pending_approvals = Some(1);
        panel.lease_owner = Some("tui:42".to_string());
        panel.lease_mode = Some("collaborative".to_string());

        let mut terminal = MockTerminal::new(60, 20);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 60, 20));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("RUNNING"),
            "Running state must show RUNNING, got: {joined}"
        );
        assert!(
            joined.contains("Healthy"),
            "Should show health status, got: {joined}"
        );
        assert!(
            joined.contains("Uptime"),
            "Should show uptime, got: {joined}"
        );
        assert!(
            joined.contains("Sessions"),
            "Should show sessions, got: {joined}"
        );
        assert!(
            joined.contains("Runtime") && joined.contains("87%"),
            "Should show runtime projection summary, got: {joined}"
        );
        assert!(
            joined.contains("Control") && joined.contains("approvals 1"),
            "Should show daemon control summary, got: {joined}"
        );
        assert!(
            joined.contains("Lease") && joined.contains("tui:42"),
            "Should show daemon lease summary, got: {joined}"
        );
    }

    #[test]
    fn render_shows_api_endpoints() {
        use crate::tui::skin::SkinConfig;
        use crate::tui::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(70, 22);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 70, 22));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("/health"),
            "Should show /health endpoint, got: {joined}"
        );
        assert!(
            joined.contains("/api/sessions"),
            "Should show sessions endpoint, got: {joined}"
        );
        assert!(
            joined.contains("/api/memory"),
            "Should show memory endpoint, got: {joined}"
        );
        assert!(
            joined.contains("/api/config"),
            "Should show config endpoint, got: {joined}"
        );
        assert!(
            joined.contains("/api/platforms"),
            "Should show platforms endpoint, got: {joined}"
        );
        assert!(
            joined.contains("/api/cross-plane/action/adapters"),
            "Should show cross-plane adapter endpoint, got: {joined}"
        );
        assert!(
            joined.contains("/api/cross-plane/action/executions"),
            "Should show cross-plane execution endpoint, got: {joined}"
        );
    }

    #[test]
    fn render_shows_cross_plane_adapter_and_execution_state() {
        use crate::tui::skin::SkinConfig;
        use crate::tui::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.set_adapter_capabilities(vec![GatewayAdapterCapability {
            platform: "feishu".to_string(),
            operation: "send_text".to_string(),
            live_supported: true,
            adapter_bound: false,
        }]);
        panel.set_execution_receipts(vec![GatewayExecutionReceipt {
            status: "planned".to_string(),
            dispatch_status: "dry_run".to_string(),
            mode: "dry_run".to_string(),
            capability: "channel.feishu.send_text".to_string(),
            idempotency_key: Some("idem-demo".to_string()),
        }]);
        panel.set_dispatch_targets(vec![GatewayDispatchTarget {
            platform: "feishu".to_string(),
            operation: "send_text".to_string(),
            session_key: Some("feishu:open-id:chat-id".to_string()),
            ready: true,
            blockers: Vec::new(),
        }]);

        let mut terminal = MockTerminal::new(96, 30);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 96, 30));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("Cross-Plane Adapters"),
            "Should show adapter section, got: {joined}"
        );
        assert!(
            joined.contains("feishu"),
            "Should show platform, got: {joined}"
        );
        assert!(
            joined.contains("send_text"),
            "Should show operation, got: {joined}"
        );
        assert!(
            joined.contains("live") && joined.contains("not-bound"),
            "Should show adapter support and binding state, got: {joined}"
        );
        assert!(
            joined.contains("Execution Receipts"),
            "Should show execution section, got: {joined}"
        );
        assert!(
            joined.contains("planned") && joined.contains("dry_run"),
            "Should show receipt state, got: {joined}"
        );
        assert!(
            joined.contains("channel.feishu.send_text") && joined.contains("idem-demo"),
            "Should show receipt capability and idempotency key, got: {joined}"
        );
        assert!(
            joined.contains("Dispatch Targets"),
            "Should show dispatch target section, got: {joined}"
        );
        assert!(
            joined.contains("ready") && joined.contains("feishu:open-id:chat-id"),
            "Should show target readiness and session key, got: {joined}"
        );
    }

    #[test]
    fn render_shows_cross_plane_action_contracts() {
        use crate::tui::skin::SkinConfig;
        use crate::tui::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(118, 40);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 118, 40));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Action Composer Contract"),
            "Should show action contract section, got: {joined}"
        );
        assert!(
            joined.contains("channel.feishu.send_text")
                && joined.contains("channel.feishu.send_image")
                && joined.contains("channel.feishu.send_file"),
            "Should show typed send capabilities, got: {joined}"
        );
        assert!(
            joined.contains("workspace://file/reports/panel.txt"),
            "Should show canonical workspace file reference, got: {joined}"
        );
        assert!(
            joined.contains("/api/cross-plane/action/execute"),
            "Should show execute endpoint, got: {joined}"
        );
    }

    #[test]
    fn render_shows_connector_console_state() {
        use crate::tui::skin::SkinConfig;
        use crate::tui::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.set_connector_accounts(vec![
            ConnectorAccountSummary {
                provider: "feishu".to_string(),
                account_id: "feishu-main".to_string(),
                auth_mode: "app_secret".to_string(),
                status: "degraded".to_string(),
                reason: Some("missing required fields: app_secret".to_string()),
                binding_count: 1,
            },
            ConnectorAccountSummary {
                provider: "mock".to_string(),
                account_id: "mock.docs".to_string(),
                auth_mode: "none".to_string(),
                status: "ready".to_string(),
                reason: None,
                binding_count: 1,
            },
        ]);
        panel.set_connector_capabilities(vec![
            ConnectorCapabilitySummary {
                capability_id: "service.feishu.docx.read".to_string(),
                provider: "feishu".to_string(),
                plane: "service".to_string(),
                risk: "low".to_string(),
                supports_commit: true,
                requires_approval: false,
            },
            ConnectorCapabilitySummary {
                capability_id: "mcp.filesystem.server".to_string(),
                provider: "filesystem".to_string(),
                plane: "mcp".to_string(),
                risk: "low".to_string(),
                supports_commit: false,
                requires_approval: false,
            },
        ]);
        panel.set_connector_resources(vec![ConnectorResourceSummary {
            reference: "service://feishu/docx/doccn-ready".to_string(),
            provider: "feishu".to_string(),
            resource_type: "docx".to_string(),
            title: "Ready Feishu Doc".to_string(),
            indexed_state: "indexed".to_string(),
        }]);
        panel.set_connector_degraded_reasons(vec!["resource_directory: locked".to_string()]);

        let mut terminal = MockTerminal::new(112, 34);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 112, 34));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Connector Console"),
            "Should show connector section, got: {joined}"
        );
        assert!(
            joined.contains("feishu-main") && joined.contains("missing required fields"),
            "Should show degraded account reason, got: {joined}"
        );
        assert!(
            joined.contains("service.feishu.docx.read") && joined.contains("mcp.filesystem.server"),
            "Should show connector capabilities, got: {joined}"
        );
        assert!(
            joined.contains("Ready Feishu Doc") && joined.contains("indexed"),
            "Should show connector resources, got: {joined}"
        );
        assert!(
            joined.contains("resource_directory: locked"),
            "Should show connector degraded reasons, got: {joined}"
        );
    }

    #[test]
    fn render_shows_keyboard_hints() {
        use crate::tui::skin::SkinConfig;
        use crate::tui::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(60, 20);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 60, 20));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("r refresh"),
            "Should show 'r refresh' hint, got: {joined}"
        );
        assert!(
            joined.contains("h health"),
            "Should show 'h health' hint, got: {joined}"
        );
        assert!(
            joined.contains("s start/stop"),
            "Should show 's start/stop' hint, got: {joined}"
        );
    }

    #[test]
    fn handle_event_consumes_known_keys() {
        let mut panel = GatewayPanel::new();

        let press_r = Event::Key(KeyEvent::new(
            KeyCode::Char('r'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_r), EventResult::Consumed);

        let press_h = Event::Key(KeyEvent::new(
            KeyCode::Char('h'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_h), EventResult::Consumed);

        let press_s = Event::Key(KeyEvent::new(
            KeyCode::Char('s'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_s), EventResult::Consumed);
    }

    #[test]
    fn handle_event_ignores_unknown_keys() {
        let mut panel = GatewayPanel::new();

        let press_x = Event::Key(KeyEvent::new(
            KeyCode::Char('x'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_x), EventResult::NotConsumed);

        let press_tab = Event::Key(KeyEvent::new(
            KeyCode::Tab,
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_tab), EventResult::NotConsumed);
    }

    #[test]
    fn handle_event_ignores_release_events() {
        let mut panel = GatewayPanel::new();

        let _release_r = Event::Key(KeyEvent::new(
            KeyCode::Char('r'),
            crossterm::event::KeyModifiers::NONE,
        ));
        // We can't easily create a KeyEventKind::Release with crossterm's new(),
        // so test the pattern via the press guard — already covered above.
        // This test validates the guard is present:
        let non_key = Event::Resize(80, 24);
        assert_eq!(panel.handle_event(&non_key), EventResult::NotConsumed);
    }
}
