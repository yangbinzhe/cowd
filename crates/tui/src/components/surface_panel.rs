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
use crate::runtime_control_store::{
    MessageBindingSummary, MessageConnectorSummary, MessageEndpointSummary, MessageRouteSummary,
    SurfaceEventSummary, SurfaceHealthSummary, SurfaceSummary,
};

pub struct SurfacePanel {
    surfaces: Vec<SurfaceSummary>,
    health: Option<SurfaceHealthSummary>,
    events: Vec<SurfaceEventSummary>,
    message_connectors: Vec<MessageConnectorSummary>,
    message_endpoints: Vec<MessageEndpointSummary>,
    message_routes: Vec<MessageRouteSummary>,
    message_bindings: Vec<MessageBindingSummary>,
    selected: usize,
    focused_backlink_target: Option<String>,
    focused_backlink_resolution: Option<String>,
    last_status: Option<String>,
    last_receipt: Option<serde_json::Value>,
    pending_confirm: Option<&'static str>,
}

impl SurfacePanel {
    pub fn new() -> Self {
        Self {
            surfaces: Vec::new(),
            health: None,
            events: Vec::new(),
            message_connectors: Vec::new(),
            message_endpoints: Vec::new(),
            message_routes: Vec::new(),
            message_bindings: Vec::new(),
            selected: 0,
            focused_backlink_target: None,
            focused_backlink_resolution: None,
            last_status: None,
            last_receipt: None,
            pending_confirm: None,
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.surfaces = app.gateway_surfaces.clone();
        self.health = app.gateway_surface_health.clone();
        self.events = app.gateway_surface_events.clone();
        self.message_connectors = app.gateway_message_connectors.clone();
        self.message_endpoints = app.gateway_message_endpoints.clone();
        self.message_routes = app.gateway_message_routes.clone();
        self.message_bindings = app.gateway_message_bindings.clone();
        if self.selected >= self.surfaces.len() {
            self.selected = self.surfaces.len().saturating_sub(1);
        }
    }

    pub fn selected_surface_id(&self) -> Option<&str> {
        self.surfaces
            .get(self.selected)
            .map(|surface| surface.id.as_str())
    }

    pub fn selected_surface_id_owned(&self) -> Option<String> {
        self.selected_surface_id().map(str::to_string)
    }

    pub fn require_confirmation(&mut self, action_id: &'static str, key_hint: &str) -> bool {
        if self.pending_confirm == Some(action_id) {
            self.pending_confirm = None;
            return true;
        }
        self.pending_confirm = Some(action_id);
        self.last_status = Some(format!("Press {key_hint} again to confirm {action_id}"));
        false
    }

    pub fn record_action_result(&mut self, label: &str, result: Result<serde_json::Value, String>) {
        self.pending_confirm = None;
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

    pub fn focus_backlink_target(&mut self, target: impl Into<String>) {
        let target = target.into();
        let surface_id = target
            .strip_prefix("surface://")
            .and_then(|suffix| suffix.split('/').next());
        if let Some(index) = self.surfaces.iter().position(|surface| {
            surface_id == Some(surface.id.as_str()) || target.contains(&surface.id)
        }) {
            self.selected = index;
            let surface = &self.surfaces[index];
            let event = self.events.iter().find(|event| {
                event.surface == surface.id
                    && target
                        .split('/')
                        .next_back()
                        .is_some_and(|object| event.detail.contains(object))
            });
            self.focused_backlink_resolution = Some(match event {
                Some(event) => format!(
                    "surface {} event {} {}",
                    surface.id, event.event_type, event.detail
                ),
                None => format!(
                    "surface {} status {} transport {}",
                    surface.id, surface.status, surface.transport
                ),
            });
        } else if target.starts_with("receipt://cross-plane/") {
            self.focused_backlink_resolution =
                Some("loading exact cross-plane execution receipt".to_string());
        } else {
            self.focused_backlink_resolution = None;
        }
        self.focused_backlink_target = Some(target);
    }

    pub fn clear_backlink_target(&mut self) {
        self.focused_backlink_target = None;
        self.focused_backlink_resolution = None;
    }

    pub fn record_backlink_receipt(
        &mut self,
        target: impl Into<String>,
        receipt: serde_json::Value,
    ) {
        let target = target.into();
        self.focus_backlink_target(target.clone());
        self.last_status = Some(format!("Resolved exact Surface receipt {target}"));
        let status = receipt
            .get("cross_plane_dispatch_status")
            .or_else(|| receipt.get("cross_plane_status"))
            .or_else(|| receipt.get("dispatch_status"))
            .or_else(|| receipt.get("status"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        self.focused_backlink_resolution = Some(format!("{target} status {status}"));
        self.last_receipt = Some(receipt);
    }

    pub fn record_backlink_failure(
        &mut self,
        target: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.focused_backlink_target = Some(target.into());
        self.focused_backlink_resolution = Some(format!("Resolution failed: {}", message.into()));
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
                        .unwrap_or("canonical Surface object is unavailable")
                        .to_string(),
                    Style::default().fg(Color::White),
                ),
            ]));
        }
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
        lines.push(Line::from(vec![
            Span::styled("Summary: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                surface_summary(&self.surfaces, health),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Runtime: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "ready {} degraded {} failed {} circuit {}",
                    health.map(|item| item.ready_count).unwrap_or(0),
                    health.map(|item| item.degraded_count).unwrap_or(0),
                    health.map(|item| item.failed_count).unwrap_or(0),
                    health.map(|item| item.circuit_open_count).unwrap_or(0),
                ),
                Style::default().fg(Color::White),
            ),
        ]));
        if !self.message_connectors.is_empty()
            || !self.message_endpoints.is_empty()
            || !self.message_routes.is_empty()
            || !self.message_bindings.is_empty()
        {
            let ready = self
                .message_connectors
                .iter()
                .filter(|connector| {
                    connector.enabled
                        && connector.configured
                        && !connector.circuit_open
                        && !matches!(
                            connector.runtime_status.as_str(),
                            "failed" | "error" | "unavailable" | "circuit-open"
                        )
                })
                .count();
            lines.push(Line::from(vec![
                Span::styled("Message Plane: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        "{ready}/{} connectors  endpoints {}  routes {}  bindings {}",
                        self.message_connectors.len(),
                        self.message_endpoints.len(),
                        self.message_routes.len(),
                        self.message_bindings.len()
                    ),
                    Style::default().fg(Color::Cyan),
                ),
            ]));
        }
        lines.push(Line::from(vec![
            Span::styled("Next: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                surface_next_action(&self.surfaces, health),
                Style::default().fg(Color::Yellow),
            ),
        ]));
        lines.push(Line::from(Span::styled(
            "Keys: j/k  h health  s start  x stop  r restart  R repair  m send  a action  g ledger  i inbox  o outbox  v deliveries  p replay  d retry  D dlq  A archive  P purge",
            Style::default().fg(Color::DarkGray),
        )));
        if let Some(status) = &self.last_status {
            lines.push(Line::from(vec![
                Span::styled("Status: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    compact(status, area.width.saturating_sub(10) as usize),
                    Style::default().fg(Color::Yellow),
                ),
            ]));
        }
        if let Some(receipt) = &self.last_receipt {
            lines.push(Line::from(vec![
                Span::styled("Receipt: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    compact(
                        &receipt_summary(receipt),
                        area.width.saturating_sub(11) as usize,
                    ),
                    Style::default().fg(Color::Green),
                ),
            ]));
        }
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
                            " fail {} restart {} circuit {}",
                            surface.consecutive_failures,
                            surface.restart_count,
                            if surface.circuit_open {
                                "open"
                            } else {
                                "closed"
                            }
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
            lines.push(Line::from(vec![
                Span::styled("Runtime: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        "active={} pid={} failures={} restarts={} circuit={}",
                        surface.active,
                        surface
                            .pid
                            .map(|pid| pid.to_string())
                            .unwrap_or_else(|| "-".to_string()),
                        surface.consecutive_failures,
                        surface.restart_count,
                        if surface.circuit_open {
                            "open"
                        } else {
                            "closed"
                        }
                    ),
                    status_style(&surface.status),
                ),
            ]));
            if let Some(error) = &surface.last_error {
                lines.push(Line::from(vec![
                    Span::styled("Error: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        compact(error, area.width.saturating_sub(10) as usize),
                        Style::default().fg(Color::Red),
                    ),
                ]));
            }
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
            let connector_matches = self
                .message_connectors
                .iter()
                .filter(|connector| {
                    connector.connector == surface.id || connector.name == surface.id
                })
                .collect::<Vec<_>>();
            if !connector_matches.is_empty() {
                let summary = connector_matches
                    .iter()
                    .take(2)
                    .map(|connector| {
                        format!(
                            "{}:{}:{}",
                            connector.connector,
                            connector.configuration_status,
                            connector.runtime_status
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" · ");
                lines.push(Line::from(vec![
                    Span::styled("Message: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        compact(&summary, area.width.saturating_sub(11) as usize),
                        Style::default().fg(Color::Cyan),
                    ),
                ]));
            }
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
        "unavailable" | "error" | "failed" | "circuit-open" => Style::default().fg(Color::Red),
        "degraded" | "restarting" | "starting" => Style::default().fg(Color::Yellow),
        _ => Style::default().fg(Color::Yellow),
    }
}

fn surface_summary(surfaces: &[SurfaceSummary], health: Option<&SurfaceHealthSummary>) -> String {
    let ready = surfaces
        .iter()
        .filter(|surface| matches!(surface.status.as_str(), "builtin" | "ready" | "discovered"))
        .count();
    let routes = health
        .map(|item| item.route_count)
        .unwrap_or_else(|| surfaces.iter().map(|surface| surface.route_count).sum());
    let resources = health
        .map(|item| item.resource_count)
        .unwrap_or_else(|| surfaces.iter().map(|surface| surface.resource_count).sum());
    format!(
        "{ready}/{} ready, routes {routes}, resources {resources}",
        surfaces.len()
    )
}

fn surface_next_action(
    surfaces: &[SurfaceSummary],
    health: Option<&SurfaceHealthSummary>,
) -> &'static str {
    if surfaces.is_empty() {
        return "refresh Gateway surface registry";
    }
    if health.is_some_and(|item| item.status == "error" || item.status == "offline") {
        return "inspect Gateway surface host health";
    }
    if surfaces.iter().any(|surface| surface.circuit_open) {
        return "repair circuit-open surfaces after fixing sidecar credentials";
    }
    if surfaces.iter().any(|surface| {
        matches!(
            surface.status.as_str(),
            "unavailable" | "error" | "failed" | "degraded" | "disabled"
        )
    }) {
        return "open selected surface diagnostics";
    }
    if surfaces.iter().any(|surface| surface.entry.is_some()) {
        return "monitor sidecar routes and recent events";
    }
    "all registered surfaces are builtin or ready"
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

fn receipt_summary(receipt: &serde_json::Value) -> String {
    let kind = receipt
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("receipt");
    if kind == "surface.messages" {
        let snapshot = receipt.get("snapshot").unwrap_or(receipt);
        let root = receipt
            .get("message_root")
            .or_else(|| snapshot.get("message_root"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let active_outbox = snapshot
            .get("active_outbox")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        let dead_letters = snapshot
            .get("dead_letters")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        let archived = snapshot
            .get("archived_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        return format!(
            "{kind} active_outbox={active_outbox} dead_letters={dead_letters} archived={archived} root={root}"
        );
    }
    let status = receipt
        .get("status")
        .or_else(|| receipt.get("ok"))
        .map(serde_json::Value::to_string)
        .unwrap_or_else(|| "recorded".to_string());
    format!("{kind} status={status}")
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
            ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            },
        ];
        app.gateway_message_connectors =
            vec![crate::runtime_control_store::MessageConnectorSummary {
                connector: "webui".to_string(),
                name: "webui".to_string(),
                configuration_status: "configured".to_string(),
                runtime_status: "ready".to_string(),
                enabled: true,
                configured: true,
                capability_count: 2,
                missing_required_count: 0,
                consecutive_failures: 0,
                restart_count: 0,
                circuit_open: false,
            }];
        app.gateway_message_endpoints =
            vec![crate::runtime_control_store::MessageEndpointSummary {
                endpoint_id: "message:webui:user".to_string(),
                connector: "webui".to_string(),
                kind: "User".to_string(),
                status: "configured".to_string(),
                configured: true,
                capability_count: 1,
            }];
        app.gateway_message_routes = vec![crate::runtime_control_store::MessageRouteSummary {
            route_id: "message:webui:default".to_string(),
            connector: "webui".to_string(),
            policy: "origin".to_string(),
            status: "configured".to_string(),
            configured: true,
            capability_count: 1,
            runtime_status: "ready".to_string(),
        }];
        app.gateway_message_bindings = vec![crate::runtime_control_store::MessageBindingSummary {
            binding_id: "message:webui:user:thread".to_string(),
            connector: "webui".to_string(),
            endpoint: "user".to_string(),
            direction: "inbound".to_string(),
            status: "processed".to_string(),
            runtime_session_id: Some("session".to_string()),
            resource_count: 0,
            last_seen_at_ms: Some(1),
        }];

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
        assert!(joined.contains("Summary:"), "{joined}");
        assert!(joined.contains("Next:"), "{joined}");
        assert!(joined.contains("Message Plane:"), "{joined}");
        assert!(joined.contains("1/1 connectors"), "{joined}");
        assert!(joined.contains("tui"), "{joined}");
        assert!(joined.contains("webui"), "{joined}");
        assert!(!joined.contains("channel.feishu"), "{joined}");
    }
}
