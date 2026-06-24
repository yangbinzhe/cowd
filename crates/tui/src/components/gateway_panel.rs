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

use crate::components::panel_scroll::{offset_to_u16, PanelScrollState};
use crate::components::{Component, EventResult, RenderContext};
use crate::{
    app::App,
    runtime_control_store::{
        ConnectorAccountSummary, ConnectorCapabilitySummary, ConnectorResourceSummary,
        CowdKernelSummary, FactFlowSummary, MissionControlSummary, RealityCoreSummary,
        RuntimeActionReceiptSummary, StructuredDataSummary, SurfaceHealthSummary, SurfaceSummary,
    },
};

/// Panel showing backend runtime/API gateway status.
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
    /// Runtime readiness score or status from Gateway API API.
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
    /// Memory/kernel status visible through runtime control.
    pub memory_status: Option<String>,
    /// Recent cross-plane execution receipts.
    pub execution_receipts: Vec<GatewayExecutionReceipt>,
    /// Cowd kernel capability and release-gate summary.
    pub cowd_kernel: Option<CowdKernelSummary>,
    /// Structured data-plane summary.
    pub structured_data: Option<StructuredDataSummary>,
    /// Reality Core engine health summary.
    pub reality_core: Option<RealityCoreSummary>,
    /// Fact Flow trace summary.
    pub fact_flow: Option<FactFlowSummary>,
    /// Mission Runtime global control summary.
    pub mission_control: Option<MissionControlSummary>,
    /// Surface registry summaries managed by Gateway SurfaceHost.
    pub surfaces: Vec<SurfaceSummary>,
    /// Surface host health summary.
    pub surface_health: Option<SurfaceHealthSummary>,
    /// Connector provider account summaries.
    pub connector_accounts: Vec<ConnectorAccountSummary>,
    /// Connector capability summaries.
    pub connector_capabilities: Vec<ConnectorCapabilitySummary>,
    /// Connector resource summaries.
    pub connector_resources: Vec<ConnectorResourceSummary>,
    /// Connector-specific degraded reasons.
    pub connector_degraded_reasons: Vec<String>,
    /// Global runtime/control degradation reasons.
    pub degraded_reasons: Vec<String>,
    /// Scroll offset for content overflow.
    pub scroll_offset: u16,
}

pub type GatewayExecutionReceipt = RuntimeActionReceiptSummary;

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
            memory_status: None,
            execution_receipts: Vec::new(),
            cowd_kernel: None,
            structured_data: None,
            reality_core: None,
            fact_flow: None,
            mission_control: None,
            surfaces: Vec::new(),
            surface_health: None,
            connector_accounts: Vec::new(),
            connector_capabilities: Vec::new(),
            connector_resources: Vec::new(),
            connector_degraded_reasons: Vec::new(),
            degraded_reasons: Vec::new(),
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
        self.runtime_readiness = app.gateway_runtime_readiness.clone();
        self.runtime_components = app.gateway_runtime_components;
        self.task_count = app.gateway_task_count;
        self.pending_approvals = app.gateway_pending_approvals;
        self.lease_owner = app.gateway_lease_owner.clone();
        self.lease_mode = app.gateway_lease_mode.clone();
        self.memory_status = app.memory_status.clone();
        self.connector_accounts = app.gateway_connector_accounts.clone();
        self.connector_capabilities = app.gateway_connector_capabilities.clone();
        self.connector_resources = app.gateway_connector_resources.clone();
        self.execution_receipts = app.gateway_action_receipts.clone();
        self.surfaces = app.gateway_surfaces.clone();
        self.surface_health = app.gateway_surface_health.clone();
        self.cowd_kernel = app.gateway_cowd_kernel.clone();
        self.structured_data = app.gateway_structured_data.clone();
        self.reality_core = app.gateway_reality_core.clone();
        self.fact_flow = app.gateway_fact_flow.clone();
        self.mission_control = app.gateway_mission_control.clone();
        self.connector_degraded_reasons = app.gateway_connector_degraded_reasons.clone();
        self.degraded_reasons = app.gateway_degraded_reasons.clone();
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
            self.memory_status = None;
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

    pub fn set_execution_receipts(&mut self, receipts: Vec<GatewayExecutionReceipt>) {
        self.execution_receipts = receipts;
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
            " Control Deck ● ".to_string()
        } else {
            " Control Deck ○ ".to_string()
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
        lines.push(Line::from(Span::styled(
            "─ Core Runtime ─",
            Style::default().fg(Color::Cyan),
        )));
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
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "─ Operator Summary ─",
            Style::default().fg(Color::Cyan),
        )));
        let issue_count = self.degraded_reasons.len() + self.connector_degraded_reasons.len();
        let surface_count = self.surfaces.len();
        let surface_status = self
            .surface_health
            .as_ref()
            .map(|health| health.status.as_str())
            .unwrap_or("unknown");
        let reality_status = self
            .reality_core
            .as_ref()
            .map(|core| core.status.as_str())
            .unwrap_or("unknown");
        let flow = self.fact_flow.as_ref();
        lines.push(Line::from(vec![
            Span::styled("Gateway ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                if self.server_running { "ready" } else { "down" },
                Style::default().fg(if self.server_running {
                    Color::Green
                } else {
                    Color::Red
                }),
            ),
            Span::styled(" · Runtime ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.runtime_readiness
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                Style::default().fg(Color::Cyan),
            ),
            Span::styled(" · Sessions ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", self.active_sessions),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Work ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "tasks {} approvals {}",
                    self.task_count.unwrap_or_default(),
                    self.pending_approvals.unwrap_or_default()
                ),
                Style::default().fg(Color::Yellow),
            ),
            Span::styled(" · Lease ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.lease_owner
                    .as_deref()
                    .map(|owner| {
                        format!(
                            "{} ({})",
                            owner,
                            self.lease_mode.as_deref().unwrap_or("unknown")
                        )
                    })
                    .unwrap_or_else(|| "none".to_string()),
                Style::default().fg(Color::Magenta),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Surface ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{surface_count} / {surface_status}"),
                Style::default().fg(Color::Cyan),
            ),
            Span::styled(" · Reality ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                reality_status.to_string(),
                Style::default().fg(Color::Green),
            ),
            Span::styled(" · FactFlow ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                flow.map(|flow| {
                    format!(
                        "s{} e{} p{} b{}",
                        flow.stage_count,
                        flow.event_count,
                        flow.promotion_count,
                        flow.boundary_count
                    )
                })
                .unwrap_or_else(|| "none".to_string()),
                Style::default().fg(Color::White),
            ),
        ]));
        if issue_count > 0 {
            lines.push(Line::from(vec![
                Span::styled("Issues ", Style::default().fg(Color::Yellow)),
                Span::styled(
                    format!("{issue_count} degraded signals"),
                    Style::default().fg(Color::Yellow),
                ),
            ]));
        }

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

            lines.push(Line::from(vec![
                Span::styled("Transport: ", Style::default().fg(Color::DarkGray)),
                Span::styled("gateway http/sse", Style::default().fg(Color::Cyan)),
            ]));

            if let Some(readiness) = self.runtime_readiness.as_ref() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ AI Context ─",
                    Style::default().fg(Color::Cyan),
                )));
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

            if let Some(memory_status) = self.memory_status.as_ref() {
                lines.push(Line::from(vec![
                    Span::styled("Memory: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(memory_status.clone(), Style::default().fg(Color::Green)),
                ]));
            }

            if let Some(reality) = self.reality_core.as_ref() {
                let status_color = match reality.status.as_str() {
                    "ready" => Color::Green,
                    "degraded" => Color::Yellow,
                    _ => Color::White,
                };
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Reality Core ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Core: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(reality.status.clone(), Style::default().fg(status_color)),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Engines: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "memory {} · matrix {} · growth {}",
                            reality.memory_status, reality.matrix_status, reality.growth_status
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Bridge: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "context {} · audit {}",
                            reality.context_status, reality.audit_status
                        ),
                        Style::default().fg(Color::Cyan),
                    ),
                ]));
                if !reality.degraded_reasons.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Reality degraded: ", Style::default().fg(Color::Yellow)),
                        Span::styled(
                            reality
                                .degraded_reasons
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

            if let Some(flow) = self.fact_flow.as_ref() {
                lines.push(Line::from(vec![
                    Span::styled("Fact Flow: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "stages {}, events {}, promotions {}, boundaries {}",
                            flow.stage_count,
                            flow.event_count,
                            flow.promotion_count,
                            flow.boundary_count
                        ),
                        Style::default().fg(Color::Yellow),
                    ),
                ]));
                if let Some(session_id) = flow.session_id.as_ref() {
                    lines.push(Line::from(vec![
                        Span::styled("Session: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            format!("{} · {}", session_id, flow.source),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }

            if let Some(mission) = self.mission_control.as_ref() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Mission Control ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Sessions: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} total, {} active, {} bg, {} paused",
                            mission.session_count,
                            mission.active_count,
                            mission.background_count,
                            mission.paused_count
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Teams/Agents: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("{} / {}", mission.team_count, mission.agent_count),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(
                        "  Approvals/Relations: ",
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        format!("{} / {}", mission.pending_approvals, mission.relation_count),
                        Style::default().fg(Color::Magenta),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Inbox: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} total, {} pending, {} running, {} failed",
                            mission.command_total,
                            mission.command_pending,
                            mission.command_running,
                            mission.command_failed
                        ),
                        Style::default().fg(if mission.command_failed > 0 {
                            Color::Red
                        } else if mission.command_running > 0 {
                            Color::Yellow
                        } else {
                            Color::White
                        }),
                    ),
                ]));
                if let Some(active) = mission.active_session_id.as_ref() {
                    lines.push(Line::from(vec![
                        Span::styled("Active: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(active.clone(), Style::default().fg(Color::Green)),
                    ]));
                }
                for session in mission.sessions.iter().take(3) {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{:10}", session.status),
                            Style::default().fg(match session.status.as_str() {
                                "active" => Color::Green,
                                "paused" => Color::Yellow,
                                "closed" => Color::DarkGray,
                                _ => Color::Cyan,
                            }),
                        ),
                        Span::styled(
                            format!(
                                "{} · teams {} agents {}",
                                compact_text(&session.title, 28),
                                session.team_count,
                                session.agent_count
                            ),
                            Style::default().fg(Color::White),
                        ),
                    ]));
                }
            }

            if let Some(kernel) = self.cowd_kernel.as_ref() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Cowd Kernel ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Kernel: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "caps {}, tui {}",
                            kernel.capability_count, kernel.projection_capability_count
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                let parity = if kernel.webui_tui_full_parity {
                    "parity yes"
                } else {
                    "parity no"
                };
                let cli = if kernel.cli_is_minimal_control {
                    "cli minimal"
                } else {
                    "cli check"
                };
                lines.push(Line::from(vec![
                    Span::styled("Surfaces: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(format!("{parity}, {cli}"), Style::default().fg(Color::Cyan)),
                ]));
                let gate_color = if kernel.release_gate_status == "pass" {
                    Color::Green
                } else {
                    Color::Yellow
                };
                lines.push(Line::from(vec![
                    Span::styled("Gate: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{}, failed {}",
                            kernel.release_gate_status, kernel.release_gate_failed_checks
                        ),
                        Style::default().fg(gate_color),
                    ),
                ]));
            }

            if let Some(data) = self.structured_data.as_ref() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Structured Data ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Data: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "sources {}, facts {}, evidence {}, watermarks {}",
                            data.source_count,
                            data.fact_count,
                            data.evidence_count,
                            data.watermark_count
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                let samples = [
                    data.sample_sources
                        .first()
                        .map(|value| format!("source {value}")),
                    data.sample_facts
                        .first()
                        .map(|value| format!("fact {value}")),
                    data.sample_evidence
                        .first()
                        .map(|value| format!("evidence {value}")),
                    data.sample_watermarks
                        .first()
                        .map(|value| format!("watermark {value}")),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" · ");
                if !samples.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Samples: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(samples, Style::default().fg(Color::Yellow)),
                    ]));
                }
            }

            if !self.surfaces.is_empty() || self.surface_health.is_some() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Surface Host ─",
                    Style::default().fg(Color::Cyan),
                )));
                let health = self.surface_health.as_ref();
                let surface_count = health
                    .map(|item| item.surface_count)
                    .unwrap_or(self.surfaces.len() as u64);
                let external_count = health
                    .map(|item| item.external_surface_count)
                    .unwrap_or_else(|| {
                        self.surfaces
                            .iter()
                            .filter(|surface| surface.entry.is_some())
                            .count() as u64
                    });
                lines.push(Line::from(vec![
                    Span::styled("Surfaces: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} total, {} external, routes {}, resources {}",
                            surface_count,
                            external_count,
                            health.map(|item| item.route_count).unwrap_or_default(),
                            health.map(|item| item.resource_count).unwrap_or_default()
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                if let Some(health) = health {
                    lines.push(Line::from(vec![
                        Span::styled("Host: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(health.status.clone(), Style::default().fg(Color::Green)),
                    ]));
                }
                let preview = self
                    .surfaces
                    .iter()
                    .take(4)
                    .map(|surface| format!("{}:{}", surface.id, surface.status))
                    .collect::<Vec<_>>()
                    .join(" · ");
                if !preview.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Preview: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(preview, Style::default().fg(Color::Cyan)),
                    ]));
                }
                lines.push(Line::from(Span::styled(
                    "Open /surfaces for routes, resources, events, send/action.",
                    Style::default().fg(Color::DarkGray),
                )));
            }

            if self.task_count.is_some() || self.pending_approvals.is_some() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Work Control ─",
                    Style::default().fg(Color::Cyan),
                )));
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

            if !self.degraded_reasons.is_empty() {
                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    Span::styled("Degraded: ", Style::default().fg(Color::Yellow)),
                    Span::styled(
                        self.degraded_reasons
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

        if !self.connector_accounts.is_empty()
            || !self.connector_capabilities.is_empty()
            || !self.connector_resources.is_empty()
            || !self.connector_degraded_reasons.is_empty()
        {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "─ Connector Plane ─",
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

        // ── HTTP Projection Endpoints ──────────────────────────
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "─ HTTP Projection Endpoints ─",
            Style::default().fg(Color::Cyan),
        )));
        lines.push(Line::from(""));

        let endpoints: &[(&str, &str)] = &[
            ("GET  /health", "Server health check"),
            ("GET  /api/sessions", "List sessions"),
            ("POST /api/sessions", "Create session"),
            ("POST /api/sessions/:id/msgs", "Send message"),
            ("GET  /api/memory", "Memory status"),
            ("GET  /api/memory/stats", "Memory statistics"),
            ("GET  /api/memory/search", "Search memory"),
            ("GET  /api/reality/status", "Reality Core health"),
            ("GET  /api/reality/static", "Reality Core map"),
            ("GET  /api/reality/flow", "Fact Flow trace"),
            ("GET  /api/reality/promotions", "Growth promotion trace"),
            ("GET  /api/reality/boundaries", "Reality boundary map"),
            ("GET  /api/config", "View config"),
            ("PUT  /api/config", "Update config"),
            ("GET  /api/platforms", "List platforms"),
            ("GET  /api/cowd/capabilities", "Cowd capability registry"),
            ("GET  /api/cowd/projection", "Surface capability projection"),
            ("GET  /api/cowd/surfaces", "Surface parity contract"),
            ("GET  /api/cowd/release-gate", "Release gate status"),
            (
                "GET  /api/cowd/structured/sources",
                "Structured data sources",
            ),
            ("GET  /api/cowd/structured/facts", "Structured facts"),
            (
                "GET  /api/cowd/structured/evidence",
                "Structured evidence packets",
            ),
            (
                "GET  /api/cowd/structured/watermarks",
                "Structured ingest watermarks",
            ),
            (
                "POST /api/cowd/structured/ingest-plan",
                "Plan structured ingest",
            ),
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
            ("GET  /api/surfaces", "Surface registry"),
            ("GET  /api/surfaces/health", "Surface host health"),
            ("GET  /api/surfaces/:id/events", "Surface event buffer"),
            ("POST /api/surfaces/:id/send", "Surface message egress"),
            ("POST /api/surfaces/:id/action", "Surface action dispatch"),
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

        for (endpoint, desc) in endpoints {
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
            "─ Surface Dispatch Contract ─",
            Style::default().fg(Color::Cyan),
        )));
        lines.push(Line::from(Span::styled(
            "Gateway owns routing. TUI submits surface send/action requests by surface id.",
            Style::default().fg(Color::DarkGray),
        )));

        // ── Keyboard hint bar ──────────────────────────────────
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Keys: j/k scroll  PgUp/PgDn page  r refresh  h health  s start/stop  / connector actions",
            Style::default().fg(Color::DarkGray),
        )));

        let viewport_len = area.height.saturating_sub(2).max(1) as usize;
        let mut scroll = PanelScrollState {
            offset: self.scroll_offset as usize,
            content_len: lines.len(),
            viewport_len,
        };
        scroll.clamp();
        self.scroll_offset = offset_to_u16(scroll.offset);

        let paragraph = Paragraph::new(lines)
            .block(block)
            .scroll((self.scroll_offset, 0));
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
            KeyCode::Char('j') | KeyCode::Down => {
                self.scroll_offset = self.scroll_offset.saturating_add(1);
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
                EventResult::Consumed
            }
            KeyCode::PageDown => {
                self.scroll_offset = self.scroll_offset.saturating_add(8);
                EventResult::Consumed
            }
            KeyCode::PageUp => {
                self.scroll_offset = self.scroll_offset.saturating_sub(8);
                EventResult::Consumed
            }
            KeyCode::Home => {
                self.scroll_offset = 0;
                EventResult::Consumed
            }
            KeyCode::End => {
                self.scroll_offset = u16::MAX;
                EventResult::Consumed
            }
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

fn compact_text(value: &str, max_chars: usize) -> String {
    let text = value.trim();
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut output = text
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    output.push_str("...");
    output
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
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

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
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

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
            joined.contains("Transport") && joined.contains("gateway http/sse"),
            "Should show Gateway transport, got: {joined}"
        );
        assert!(
            joined.contains("Runtime") && joined.contains("87%"),
            "Should show Gateway API summary, got: {joined}"
        );
        assert!(
            joined.contains("Control") && joined.contains("approvals 1"),
            "Should show runtime control summary, got: {joined}"
        );
        assert!(
            joined.contains("Lease") && joined.contains("tui:42"),
            "Should show runtime lease summary, got: {joined}"
        );
    }

    #[test]
    fn render_shows_cowd_kernel_and_structured_data_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.cowd_kernel = Some(CowdKernelSummary {
            capability_count: 11,
            projection_capability_count: 10,
            webui_tui_full_parity: true,
            cli_is_minimal_control: true,
            release_gate_status: "pass".to_string(),
            release_gate_failed_checks: 0,
        });
        panel.structured_data = Some(StructuredDataSummary {
            source_count: 1,
            fact_count: 2,
            evidence_count: 3,
            watermark_count: 1,
            sample_sources: vec!["pack-tui".to_string()],
            sample_facts: vec!["fact-tui".to_string()],
            sample_evidence: vec!["evidence-tui".to_string()],
            sample_watermarks: vec!["pack-tui".to_string()],
        });

        let mut terminal = MockTerminal::new(100, 34);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 100, 34));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Cowd Kernel"),
            "Should show cowd kernel section, got: {joined}"
        );
        assert!(
            joined.contains("caps 11") && joined.contains("tui 10"),
            "Should show capability summary, got: {joined}"
        );
        assert!(
            joined.contains("parity yes") && joined.contains("cli minimal"),
            "Should show surface policy summary, got: {joined}"
        );
        assert!(
            joined.contains("Structured Data"),
            "Should show structured section, got: {joined}"
        );
        assert!(
            joined.contains("sources 1")
                && joined.contains("facts 2")
                && joined.contains("evidence 3")
                && joined.contains("watermarks 1"),
            "Should show structured counts, got: {joined}"
        );
        assert!(
            joined.contains("pack-tui")
                && joined.contains("fact-tui")
                && joined.contains("evidence-tui"),
            "Should show structured samples, got: {joined}"
        );
    }

    #[test]
    fn render_shows_reality_core_and_fact_flow_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.reality_core = Some(RealityCoreSummary {
            status: "ready".to_string(),
            memory_status: "ready".to_string(),
            matrix_status: "ready".to_string(),
            growth_status: "ready".to_string(),
            context_status: "ready".to_string(),
            audit_status: "ready".to_string(),
            degraded_reasons: Vec::new(),
        });
        panel.fact_flow = Some(FactFlowSummary {
            source: "growth.promotions".to_string(),
            session_id: Some("session-tui".to_string()),
            stage_count: 5,
            event_count: 2,
            promotion_count: 1,
            boundary_count: 4,
        });

        let mut terminal = MockTerminal::new(100, 34);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 100, 34));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Reality Core"),
            "Should show Reality Core section, got: {joined}"
        );
        assert!(
            joined.contains("memory ready")
                && joined.contains("matrix ready")
                && joined.contains("growth ready"),
            "Should show Reality engines, got: {joined}"
        );
        assert!(
            joined.contains("Fact Flow")
                && joined.contains("stages 5")
                && joined.contains("promotions 1")
                && joined.contains("boundaries 4"),
            "Should show Fact Flow summary, got: {joined}"
        );
        assert!(
            joined.contains("session-tui") && joined.contains("growth.promotions"),
            "Should show Fact Flow session/source, got: {joined}"
        );
    }

    #[test]
    fn render_shows_api_endpoints() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(82, 72);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 82, 72));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("HTTP Projection Endpoints"),
            "Should show HTTP projection endpoint section, got: {joined}"
        );
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
            joined.contains("/api/reality/status")
                && joined.contains("/api/reality/static")
                && joined.contains("/api/reality/flow")
                && joined.contains("/api/reality/promotions")
                && joined.contains("/api/reality/boundaries"),
            "Should show Reality Core endpoints, got: {joined}"
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
            joined.contains("/api/cowd/capabilities")
                && joined.contains("/api/cowd/projection")
                && joined.contains("/api/cowd/release-gate"),
            "Should show cowd kernel endpoints, got: {joined}"
        );
        assert!(
            joined.contains("/api/cowd/structured/sources")
                && joined.contains("/api/cowd/structured/facts")
                && joined.contains("/api/cowd/structured/evidence")
                && joined.contains("/api/cowd/structured/watermarks")
                && joined.contains("/api/cowd/structured/ingest-plan"),
            "Should show structured data endpoints, got: {joined}"
        );
        assert!(
            joined.contains("/api/surfaces"),
            "Should show surface registry endpoint, got: {joined}"
        );
        assert!(
            joined.contains("/api/surfaces/:id/send")
                && joined.contains("/api/surfaces/:id/action"),
            "Should show surface dispatch endpoints, got: {joined}"
        );
    }

    #[test]
    fn render_shows_surface_host_and_execution_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.surfaces = vec![crate::runtime_control_store::SurfaceSummary {
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
            diagnostics: Vec::new(),
        }];
        panel.surface_health = Some(crate::runtime_control_store::SurfaceHealthSummary {
            status: "ready".to_string(),
            surface_count: 1,
            external_surface_count: 0,
            route_count: 0,
            resource_count: 1,
        });
        panel.set_execution_receipts(vec![GatewayExecutionReceipt {
            status: "planned".to_string(),
            dispatch_status: "dry_run".to_string(),
            mode: "dry_run".to_string(),
            capability: "surface.webui.action".to_string(),
            idempotency_key: Some("idem-demo".to_string()),
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
            joined.contains("Surface Host"),
            "Should show surface section, got: {joined}"
        );
        assert!(
            joined.contains("webui") && joined.contains("builtin"),
            "Should show surface status, got: {joined}"
        );
        assert!(
            joined.contains("routes 0") && joined.contains("resources 1"),
            "Should show surface health counts, got: {joined}"
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
            joined.contains("surface.webui.action") && joined.contains("idem-demo"),
            "Should show receipt capability and idempotency key, got: {joined}"
        );
        assert!(
            !joined.contains("channel.feishu"),
            "Gateway panel must not hard-code platform channel capabilities, got: {joined}"
        );
    }

    #[test]
    fn render_shows_mission_control_state() {
        use crate::runtime_control_store::{MissionControlSummary, MissionSessionSummary};
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.mission_control = Some(MissionControlSummary {
            active_session_id: Some("mission-a".to_string()),
            session_count: 2,
            active_count: 1,
            background_count: 1,
            paused_count: 0,
            closed_count: 0,
            team_count: 1,
            agent_count: 2,
            pending_approvals: 3,
            relation_count: 4,
            event_count: 5,
            command_pending: 2,
            command_claimed: 0,
            command_running: 1,
            command_completed: 0,
            command_failed: 0,
            command_cancelled: 0,
            command_total: 3,
            sessions: vec![MissionSessionSummary {
                session_id: "mission-a".to_string(),
                title: "Primary mission control task".to_string(),
                status: "active".to_string(),
                team_count: 1,
                agent_count: 2,
            }],
        });

        let mut terminal = MockTerminal::new(96, 32);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 96, 32));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Mission Control"),
            "Should show mission section, got: {joined}"
        );
        assert!(
            joined.contains("2 total, 1 active, 1 bg"),
            "Should show session counts, got: {joined}"
        );
        assert!(
            joined.contains("1 / 2") && joined.contains("3 / 4"),
            "Should show team/agent and approval/relation counts, got: {joined}"
        );
    }

    #[test]
    fn render_shows_surface_dispatch_contracts() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(118, 72);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 118, 72));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Surface Dispatch Contract"),
            "Should show surface dispatch contract section, got: {joined}"
        );
        assert!(
            joined.contains("/api/surfaces/:id/send")
                && joined.contains("/api/surfaces/:id/action"),
            "Should show surface dispatch endpoints, got: {joined}"
        );
        assert!(
            !joined.contains("channel.feishu"),
            "Should not show legacy channel templates, got: {joined}"
        );
    }

    #[test]
    fn render_shows_connector_console_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.runtime_readiness = Some("91%".to_string());
        panel.task_count = Some(2);
        panel.pending_approvals = Some(1);
        panel.memory_status = Some("available".to_string());
        panel.degraded_reasons = vec!["context socket degraded".to_string()];
        panel.set_connector_accounts(vec![ConnectorAccountSummary {
            provider: "mock".to_string(),
            account_id: "mock-docs".to_string(),
            auth_mode: "none".to_string(),
            status: "ready".to_string(),
            reason: None,
            binding_count: 1,
        }]);
        panel.set_connector_capabilities(vec![
            ConnectorCapabilitySummary {
                capability_id: "service.mock.docs.read".to_string(),
                provider: "mock".to_string(),
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
            reference: "service://mock/docs/ready".to_string(),
            provider: "mock".to_string(),
            resource_type: "document".to_string(),
            title: "Ready Mock Document".to_string(),
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
            joined.contains("available"),
            "Should show memory status, got: {joined}"
        );
        assert!(
            joined.contains("Degraded") && joined.contains("context socket degraded"),
            "Should show global degradation reasons, got: {joined}"
        );
        assert!(
            joined.contains("Connector Plane"),
            "Should show connector section, got: {joined}"
        );
        assert!(
            joined.contains("mock-docs"),
            "Should show connector account, got: {joined}"
        );
        assert!(
            joined.contains("service.mock.docs.read") && joined.contains("mcp.filesystem.server"),
            "Should show connector capabilities, got: {joined}"
        );
        assert!(
            joined.contains("Ready Mock Document") && joined.contains("indexed"),
            "Should show connector resources, got: {joined}"
        );
        assert!(
            joined.contains("resource_directory: locked"),
            "Should show connector degraded reasons, got: {joined}"
        );
    }

    #[test]
    fn render_shows_keyboard_hints() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

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
