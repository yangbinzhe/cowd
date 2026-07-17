#![allow(dead_code)]

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use harness_contract::projection::{ProjectionEntity, StrategyDecisionProjection};

use crate::components::strategy_decision::{
    strategy_agent_ids, strategy_matches_target, strategy_summary_lines,
};
use crate::components::{Component, EventResult, RenderContext};
use crate::runtime_control_store::{
    MfgBacklinkKind, MfgConnectionStatus, MfgFreshness, MfgOperationsState, MfgViewFocus,
    MfgViewTab,
};

pub const MFG_PANEL_READ_ROUTE_IDS: [app_mfg_contract::MfgRouteId; 20] = [
    app_mfg_contract::MfgRouteId::ContractGet,
    app_mfg_contract::MfgRouteId::AppGet,
    app_mfg_contract::MfgRouteId::CommandCenterGet,
    app_mfg_contract::MfgRouteId::DecisionTraceGet,
    app_mfg_contract::MfgRouteId::IncidentList,
    app_mfg_contract::MfgRouteId::IncidentGet,
    app_mfg_contract::MfgRouteId::IncidentRoomGet,
    app_mfg_contract::MfgRouteId::AnalysisGet,
    app_mfg_contract::MfgRouteId::ExecutionGet,
    app_mfg_contract::MfgRouteId::ReportList,
    app_mfg_contract::MfgRouteId::ReportGet,
    app_mfg_contract::MfgRouteId::ReportDeliveryStateGet,
    app_mfg_contract::MfgRouteId::ReportReviewList,
    app_mfg_contract::MfgRouteId::ReportReviewGet,
    app_mfg_contract::MfgRouteId::AlertRuleList,
    app_mfg_contract::MfgRouteId::AlertList,
    app_mfg_contract::MfgRouteId::AssignmentList,
    app_mfg_contract::MfgRouteId::AssignmentGet,
    app_mfg_contract::MfgRouteId::LiveStream,
    app_mfg_contract::MfgRouteId::LiveSnapshot,
];

pub struct MfgOperationsPanel;

impl MfgOperationsPanel {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn render_state(
        &mut self,
        ctx: &mut RenderContext,
        area: Rect,
        state: &MfgOperationsState,
        strategy: Option<&StrategyDecisionProjection>,
        agents: &[ProjectionEntity],
    ) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        let cached = state.selected_runtime_strategy_projection();
        let strategy = cached.map(|projection| &projection.strategy).or(strategy);
        let agents = cached
            .map(|projection| projection.agents.as_slice())
            .unwrap_or(agents);
        let regions = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Length(1),
                Constraint::Min(4),
                Constraint::Length(2),
            ])
            .split(area);
        self.render_header(ctx, regions[0], state);
        self.render_tabs(ctx, regions[1], state);
        self.render_body(ctx, regions[2], state, strategy, agents);
        self.render_footer(ctx, regions[3], state);
    }

    fn render_header(&self, ctx: &mut RenderContext, area: Rect, state: &MfgOperationsState) {
        let contract = state
            .contract
            .as_ref()
            .map(|contract| contract.contract_version.0.as_str())
            .unwrap_or("contract pending");
        let connection_color = match state.connection {
            MfgConnectionStatus::ReadOnly | MfgConnectionStatus::Operational => Color::Green,
            MfgConnectionStatus::Loading => Color::Yellow,
            MfgConnectionStatus::Degraded => Color::LightYellow,
            MfgConnectionStatus::Failed => Color::Red,
            MfgConnectionStatus::Disconnected => Color::DarkGray,
        };
        let stale = if state.is_stale { " · STALE" } else { "" };
        let live = if state.live_stream_available {
            "live-route · not-connected"
        } else {
            "live-route-unavailable"
        };
        let action_count = state
            .contract
            .as_ref()
            .and_then(|contract| {
                contract
                    .surfaces
                    .iter()
                    .find(|surface| surface.surface == app_mfg_contract::MfgSurfaceKind::Tui)
            })
            .map(|surface| surface.actions.len())
            .unwrap_or_default();
        let line = Line::from(vec![
            Span::styled(
                " MFG Operations ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::styled(
                format!("{:?}{stale}", state.connection),
                Style::default()
                    .fg(connection_color)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(" · {contract} · {live}"),
                Style::default().fg(Color::DarkGray),
            ),
        ]);
        let view_state = Line::from(Span::styled(
            format!(
                " actions={} · mutations={} · generation={}",
                action_count,
                state.pending_mutation_count(),
                state.applied_generation,
            ),
            Style::default().fg(Color::DarkGray),
        ));
        let selection_state = Line::from(Span::styled(
            format!(
                " tab={} · selection-revision={} · focus={:?} · list-scroll={}",
                state.active_tab.label(),
                state.selection_revision,
                state.focus,
                state.list_scroll,
            ),
            Style::default().fg(Color::DarkGray),
        ));
        let updated = Line::from(Span::styled(
            format!(
                " refreshed={} · capabilities={}",
                state.last_updated_at.as_deref().unwrap_or("never"),
                if state.granted_capabilities.is_empty() {
                    "none".to_string()
                } else {
                    state.granted_capabilities.join(",")
                }
            ),
            Style::default().fg(Color::DarkGray),
        ));
        ctx.frame_mut().render_widget(
            Paragraph::new(vec![line, view_state, selection_state, updated]),
            area,
        );
    }

    fn render_tabs(&self, ctx: &mut RenderContext, area: Rect, state: &MfgOperationsState) {
        let spans = MfgViewTab::ALL
            .iter()
            .flat_map(|tab| {
                let active = *tab == state.active_tab;
                [
                    Span::styled(
                        format!(" {} ", tab.label()),
                        if active {
                            Style::default()
                                .fg(Color::Black)
                                .bg(Color::LightCyan)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::Gray)
                        },
                    ),
                    Span::raw(" "),
                ]
            })
            .collect::<Vec<_>>();
        ctx.frame_mut()
            .render_widget(Paragraph::new(Line::from(spans)), area);
    }

    fn render_body(
        &self,
        ctx: &mut RenderContext,
        area: Rect,
        state: &MfgOperationsState,
        strategy: Option<&StrategyDecisionProjection>,
        agents: &[ProjectionEntity],
    ) {
        if area.width < 96 {
            match state.focus {
                MfgViewFocus::Detail | MfgViewFocus::Backlinks => {
                    self.render_detail(ctx, area, state, strategy, agents);
                }
                MfgViewFocus::Actions => {
                    self.render_links_and_recovery(ctx, area, state);
                }
                MfgViewFocus::Tabs | MfgViewFocus::List => {
                    self.render_list(ctx, area, state);
                }
            }
            return;
        }
        let constraints = if area.width >= 120 {
            vec![
                Constraint::Percentage(32),
                Constraint::Percentage(43),
                Constraint::Percentage(25),
            ]
        } else {
            vec![Constraint::Percentage(40), Constraint::Percentage(60)]
        };
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(constraints)
            .split(area);
        self.render_list(ctx, columns[0], state);
        if columns.len() == 2 && state.focus == MfgViewFocus::Actions {
            self.render_links_and_recovery(ctx, columns[1], state);
        } else {
            self.render_detail(ctx, columns[1], state, strategy, agents);
        }
        if columns.len() == 3 {
            self.render_links_and_recovery(ctx, columns[2], state);
        }
    }

    fn render_list(&self, ctx: &mut RenderContext, area: Rect, state: &MfgOperationsState) {
        let title = if state.active_tab == MfgViewTab::Alerts {
            format!(
                " Alerts ({}) · Rules ({}) ",
                state.alerts.len(),
                state.alert_rules.len()
            )
        } else {
            format!(" {} ", state.active_tab.label())
        };
        let mut lines = Vec::new();
        if state.active_tab == MfgViewTab::Overview {
            lines.extend([
                metric_line("Incidents", state.incidents.len()),
                metric_line("Alerts", state.alerts.len()),
                metric_line("Alert rules", state.alert_rules.len()),
                metric_line("Assignments", state.assignments.len()),
                metric_line("Reports", state.reports.len()),
                metric_line("Reviews", state.reviews.len()),
                metric_line("Live receipts", state.live_receipts.len()),
                metric_line("Insights", state.insights.len()),
            ]);
            if state.command_center.is_none() {
                lines.push(Line::from(Span::styled(
                    "Command center has no data.",
                    Style::default().fg(Color::DarkGray),
                )));
            }
        } else if let Some((section, reason)) = state.active_tab_forbidden() {
            lines.push(Line::from(Span::styled(
                format!("FORBIDDEN · {section}"),
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(reason.clone()));
            lines.push(Line::from(
                "Access required; use an enabled recovery target when available.",
            ));
        } else if state.current_items().is_empty() {
            lines.push(Line::from(Span::styled(
                "No records in this principal scope.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for item in state
                .current_items()
                .iter()
                .skip(state.list_scroll.saturating_sub(1))
            {
                let selected = state.selected_id() == Some(item.id.as_str());
                let marker = if selected { "›" } else { " " };
                let severity = item
                    .severity
                    .as_deref()
                    .map(|value| format!(" [{value}]"))
                    .unwrap_or_default();
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{marker} "),
                        Style::default().fg(if selected {
                            Color::Cyan
                        } else {
                            Color::DarkGray
                        }),
                    ),
                    Span::styled(
                        format!("id={} · ", item.id),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        item.title.clone(),
                        if selected {
                            Style::default()
                                .fg(Color::White)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::Gray)
                        },
                    ),
                    Span::styled(severity, Style::default().fg(Color::LightYellow)),
                ]));
                lines.push(Line::from(Span::styled(
                    format!(
                        "  {} · owner={} · sla={} · rev={}",
                        item.status,
                        item.owner.as_deref().unwrap_or("unassigned"),
                        item.sla.as_deref().unwrap_or("n/a"),
                        item.revision
                            .map(|revision| revision.to_string())
                            .unwrap_or_else(|| "n/a".to_string())
                    ),
                    Style::default().fg(Color::DarkGray),
                )));
            }
            if state.active_tab == MfgViewTab::Alerts && !state.alert_rules.is_empty() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "Alert rules",
                    Style::default()
                        .fg(Color::LightCyan)
                        .add_modifier(Modifier::BOLD),
                )));
                for rule in &state.alert_rules {
                    lines.push(Line::from(format!(
                        "  RULE {} · {} · rev={}",
                        rule.title,
                        rule.status,
                        rule.revision
                            .map(|revision| revision.to_string())
                            .unwrap_or_else(|| "n/a".to_string())
                    )));
                }
            }
        }
        let pagination_key = match state.active_tab {
            MfgViewTab::Overview => None,
            MfgViewTab::Incidents => Some("incidents"),
            MfgViewTab::Alerts => Some("alerts"),
            MfgViewTab::Assignments => Some("assignments"),
            MfgViewTab::Reports => Some("reports"),
            MfgViewTab::Reviews => Some("reviews"),
            MfgViewTab::Insights => None,
        };
        if let Some(pagination) = pagination_key.and_then(|key| state.pagination.get(key)) {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!(
                    "page loaded={} limit={} total={} next={}",
                    pagination.loaded_count,
                    pagination.limit,
                    pagination
                        .total_count
                        .map(|total| total.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    pagination
                        .next_cursor
                        .as_deref()
                        .unwrap_or("unknown; use ] to load more")
                ),
                Style::default().fg(Color::DarkGray),
            )));
        }
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(title)
                        .border_style(focus_style(state.focus == MfgViewFocus::List)),
                )
                .wrap(Wrap { trim: true }),
            area,
        );
    }

    fn render_detail(
        &self,
        ctx: &mut RenderContext,
        area: Rect,
        state: &MfgOperationsState,
        strategy: Option<&StrategyDecisionProjection>,
        agents: &[ProjectionEntity],
    ) {
        let title = state
            .selected_item()
            .map(|item| format!(" Detail · {} ", item.id))
            .unwrap_or_else(|| " Detail ".to_string());
        let content = state
            .current_detail()
            .and_then(|value| serde_json::to_string_pretty(&value).ok())
            .unwrap_or_else(|| {
                if state.freshness == MfgFreshness::Refreshing {
                    "Loading canonical detail…".to_string()
                } else {
                    "Select a record to inspect its canonical detail.".to_string()
                }
            });
        let mut lines = Vec::new();
        if let Some(strategy) = strategy.filter(|strategy| {
            state.selected_item().is_some_and(|item| {
                item.backlinks.iter().any(|backlink| {
                    backlink.kind == MfgBacklinkKind::Runtime
                        && strategy_matches_target(strategy, &backlink.target)
                })
            })
        }) {
            lines.extend(strategy_summary_lines(
                strategy,
                usize::from(area.width.saturating_sub(2)),
                &strategy_agent_ids(strategy, agents),
            ));
            lines.push(Line::raw(""));
        }
        lines.extend(content.lines().map(|line| Line::from(line.to_string())));
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(title)
                        .border_style(focus_style(state.focus == MfgViewFocus::Detail)),
                )
                .scroll((u16::try_from(state.detail_scroll).unwrap_or(u16::MAX), 0))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_links_and_recovery(
        &self,
        ctx: &mut RenderContext,
        area: Rect,
        state: &MfgOperationsState,
    ) {
        let request_route_count = MFG_PANEL_READ_ROUTE_IDS
            .iter()
            .filter(|route| **route != app_mfg_contract::MfgRouteId::LiveStream)
            .count();
        let visible_route_count = MFG_PANEL_READ_ROUTE_IDS
            .iter()
            .filter(|route| state.route_projection_status(**route) == Some("visible"))
            .count();
        let p0_attempted = MFG_PANEL_READ_ROUTE_IDS
            .iter()
            .filter(|route| state.attempted_routes.contains(route))
            .count();
        let p1_attempted = state.attempted_routes.len().saturating_sub(p0_attempted);
        let mut lines = vec![Line::from(Span::styled(
            "Governed action status",
            Style::default()
                .fg(Color::LightMagenta)
                .add_modifier(Modifier::BOLD),
        ))];
        if let Some(intent) = state.latest_action_intent() {
            lines.push(Line::from(format!(
                "intent={} · {:?} · key={}",
                intent.action_id.as_str(),
                intent.status,
                intent.idempotency_key
            )));
            if let Some(error) = intent.last_error.as_ref() {
                lines.push(Line::from(format!(
                    "{:?} · retryable={} · request={}",
                    error.code,
                    error.retryable,
                    error.request_id.as_deref().unwrap_or("none")
                )));
            }
            if let Some(receipt) = intent.receipt.as_ref() {
                lines.push(Line::from(format!(
                    "receipt={} · {:?} · correlation={} · result-revision={}",
                    receipt.receipt_id,
                    receipt.status,
                    receipt.correlation_id.as_deref().unwrap_or("none"),
                    receipt
                        .result_revision
                        .map(|revision| revision.to_string())
                        .unwrap_or_else(|| "n/a".to_string())
                )));
                if receipt.status == app_mfg_contract::MfgReceiptStatus::Preview {
                    let preview = serde_json::to_string(&receipt.response)
                        .unwrap_or_else(|_| "<invalid preview payload>".to_string());
                    let preview = if preview.chars().count() > 180 {
                        format!("{}…", preview.chars().take(179).collect::<String>())
                    } else {
                        preview
                    };
                    lines.push(Line::from(format!("preview={preview}")));
                }
            }
            lines.push(Line::from(format!(
                "target={} · revision={}",
                intent.resource_ref,
                intent
                    .expected_revision
                    .map(|revision| revision.to_string())
                    .unwrap_or_else(|| "n/a".to_string())
            )));
            if intent.status == crate::runtime_control_store::MfgIntentStatus::AwaitingConfirmation
            {
                lines.push(Line::from(Span::styled(
                    format!(
                        "CONFIRM {:?}: this governed action can change canonical state{}",
                        intent.risk,
                        if intent.risk == app_mfg_contract::MfgActionRisk::High {
                            " and may trigger irreversible external effects"
                        } else {
                            ""
                        }
                    ),
                    Style::default()
                        .fg(Color::LightRed)
                        .add_modifier(Modifier::BOLD),
                )));
            }
        }
        lines.push(Line::from(Span::styled(
            "Recovery",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
        for action in state
            .recovery_actions
            .iter()
            .filter(|action| action.enabled)
        {
            lines.push(Line::from(format!(
                "• {}{}",
                action.label,
                action
                    .target
                    .as_deref()
                    .map(|target| format!(" → {target}"))
                    .unwrap_or_default()
            )));
        }
        if state.recovery_actions.is_empty() {
            lines.push(Line::from(Span::styled(
                "No recovery action pending.",
                Style::default().fg(Color::DarkGray),
            )));
        }
        lines.extend([
            Line::from("a actions · Enter prepare/confirm"),
            Line::from("c cancel · y confirm · R retry same key"),
            Line::from("Ctrl+Tab focus · Tab switches sidebar"),
            Line::from(""),
            Line::from(Span::styled(
                "Governed actions",
                Style::default()
                    .fg(Color::LightMagenta)
                    .add_modifier(Modifier::BOLD),
            )),
        ]);
        let action_contracts = state.visible_action_contracts();
        if action_contracts.is_empty() {
            lines.push(Line::from(Span::styled(
                "No operational actions in contract.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            let start = state.selected_action_index.saturating_sub(2);
            for (index, action) in action_contracts.iter().enumerate().skip(start).take(5) {
                let marker = if index == state.selected_action_index {
                    "›"
                } else {
                    " "
                };
                lines.push(Line::from(format!(
                    "{marker} {} · {:?} · {}",
                    action.action_id.as_str(),
                    action.risk,
                    state.action_usability_label(action),
                )));
            }
        }
        lines.extend([
            Line::from(""),
            Line::from(Span::styled(
                "Route projection",
                Style::default()
                    .fg(Color::LightGreen)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(format!(
                "P0 requests={}/{} · visible={}/{}",
                p0_attempted, request_route_count, visible_route_count, request_route_count,
            )),
            Line::from(format!(
                "P1 attempted={} · documents={}",
                p1_attempted,
                state.p1_documents.len()
            )),
            Line::from(if state.live_stream_available {
                "live stream: route declared · not connected"
            } else {
                "live stream: unavailable"
            }),
            Line::from(""),
            Line::from(Span::styled(
                "Backlinks",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from("e Evidence · p Approval"),
            Line::from("s Surface · x Runtime"),
            Line::from(""),
        ]);
        if let Some(item) = state.selected_item() {
            for link in &item.backlinks {
                lines.push(Line::from(format!(
                    "{} · {}",
                    link.kind.label(),
                    link.target
                )));
            }
        } else {
            lines.push(Line::from(Span::styled(
                "No selected object.",
                Style::default().fg(Color::DarkGray),
            )));
        }
        for route in MFG_PANEL_READ_ROUTE_IDS.iter().filter(|route| {
            matches!(
                state.route_projection_status(**route),
                Some("forbidden" | "error" | "unavailable")
            )
        }) {
            lines.push(Line::from(format!(
                "{} · {}",
                route.as_str(),
                state.route_projection_status(*route).unwrap_or("unmapped")
            )));
        }
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" Context & Actions ")
                        .border_style(focus_style(matches!(
                            state.focus,
                            MfgViewFocus::Backlinks | MfgViewFocus::Actions
                        ))),
                )
                .wrap(Wrap { trim: true }),
            area,
        );
    }

    fn render_footer(&self, ctx: &mut RenderContext, area: Rect, state: &MfgOperationsState) {
        let status = state
            .last_error
            .as_ref()
            .map(|error| {
                format!(
                    "{:?}: {} · request={}",
                    error.code,
                    error.message,
                    error.request_id.as_deref().unwrap_or("none")
                )
            })
            .or_else(|| {
                (!state.degraded_reasons.is_empty()).then(|| state.degraded_reasons.join(" | "))
            })
            .unwrap_or_else(|| {
                format!(
                    "{:?} · action-receipts={} · live-receipts={} · pending-mutations={} · live-cursor={}",
                    state.connection,
                    state.receipts.len(),
                    state.live_receipts.len(),
                    state.pending_mutation_count(),
                    state.live_cursor.as_deref().unwrap_or("not-started")
                )
            });
        let lines = vec![
            Line::from(Span::styled(
                status,
                Style::default().fg(if state.last_error.is_some() {
                    Color::Red
                } else if state.degraded_reasons.is_empty() {
                    Color::DarkGray
                } else {
                    Color::Yellow
                }),
            )),
            Line::from(Span::styled(
                "j/k select · a actions · Enter prepare/confirm · y confirm · c cancel · R retry · r refresh",
                Style::default().fg(Color::DarkGray),
            )),
        ];
        ctx.frame_mut().render_widget(Paragraph::new(lines), area);
    }
}

impl Default for MfgOperationsPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for MfgOperationsPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        ctx.frame_mut().render_widget(
            Paragraph::new("MFG projection is rendered from App::mfg_operations.")
                .block(Block::default().borders(Borders::ALL).title(" MFG ")),
            area,
        );
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "mfg_operations_panel"
    }
}

fn metric_line(label: &str, count: usize) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:<14}"), Style::default().fg(Color::DarkGray)),
        Span::styled(
            count.to_string(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ])
}

fn focus_style(focused: bool) -> Style {
    Style::default().fg(if focused {
        Color::Cyan
    } else {
        Color::DarkGray
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_control_store::{MfgBacklink, MfgItemSummary};
    use crate::skin::SkinConfig;
    use ratatui::{Terminal, backend::TestBackend};

    fn render(width: u16, height: u16, state: &MfgOperationsState) -> String {
        render_with_strategy(width, height, state, None)
    }

    fn render_with_strategy(
        width: u16,
        height: u16,
        state: &MfgOperationsState,
        strategy: Option<&StrategyDecisionProjection>,
    ) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut panel = MfgOperationsPanel::new();
        terminal
            .draw(|frame| {
                let skin = SkinConfig::default();
                let area = frame.area();
                let mut context = RenderContext::new(frame, &skin);
                panel.render_state(&mut context, area, state, strategy, &[]);
            })
            .expect("draw");
        let buffer = terminal.backend().buffer();
        let mut output = String::new();
        for y in 0..height {
            for x in 0..width {
                output.push_str(buffer[(x, y)].symbol());
            }
            output.push('\n');
        }
        output
    }

    fn populated_state() -> MfgOperationsState {
        let mut state = MfgOperationsState::default();
        state.active_tab = MfgViewTab::Incidents;
        state.connection = MfgConnectionStatus::ReadOnly;
        state.incidents = vec![MfgItemSummary {
            id: "incident-1".to_string(),
            kind: "incident".to_string(),
            title: "Line stop".to_string(),
            status: "open".to_string(),
            backlinks: vec![MfgBacklink {
                kind: MfgBacklinkKind::Evidence,
                target: "evidence-1".to_string(),
                label: "Evidence".to_string(),
            }],
            ..MfgItemSummary::default()
        }];
        state.selected_incident_id = Some("incident-1".to_string());
        state
    }

    fn execution_projection(
        execution_id: &str,
        strategy: StrategyDecisionProjection,
    ) -> harness_contract::projection::ExecutionProjection {
        harness_contract::projection::ExecutionProjection {
            schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
            execution_id: execution_id.to_string(),
            revision: 4,
            cursor: 4,
            session_id: Some("session-mfg-history".to_string()),
            mission_id: Some("mission-mfg-history".to_string()),
            strategy: Some(strategy),
            graph: harness_contract::execution_graph::project_execution_graph(
                &harness_contract::execution_graph::ExecutionGraph::new("MFG linked execution"),
            ),
            child_executions: Vec::new(),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
            interventions: Vec::new(),
            usage: Vec::new(),
            context: Vec::new(),
            evidence: Vec::new(),
            health: Vec::new(),
            recovery: Vec::new(),
            live: None,
            available_commands: Vec::new(),
        }
    }

    #[test]
    fn responsive_layouts_expose_single_double_and_triple_column_content() {
        let mut state = populated_state();
        let compact = render(80, 24, &state);
        assert!(compact.contains("Line stop"));
        assert!(!compact.contains("Backlinks"));

        let medium = render(96, 28, &state);
        assert!(medium.contains("Line stop"));
        assert!(medium.contains("Detail"));
        assert!(!medium.contains("Backlinks"));

        state.focus = MfgViewFocus::Backlinks;
        let wide = render(120, 40, &state);
        assert!(wide.contains("Line stop"));
        assert!(wide.contains("Detail"));
        assert!(wide.contains("Backlinks"));
        assert!(wide.contains("evidence-1"));
    }

    #[test]
    fn empty_forbidden_stale_and_pagination_states_are_explicit() {
        let mut state = MfgOperationsState::default();
        state.active_tab = MfgViewTab::Reports;
        state.is_stale = true;
        state.forbidden_sections.insert(
            "report_detail".to_string(),
            "mfg.read was recropped".to_string(),
        );
        state.pagination.insert(
            "reports".to_string(),
            crate::runtime_control_store::MfgPaginationState {
                next_cursor: Some("cursor-2".to_string()),
                ..crate::runtime_control_store::MfgPaginationState::default()
            },
        );
        let output = render(120, 40, &state);
        assert!(output.contains("STALE"));
        assert!(output.contains("FORBIDDEN"));
        assert!(output.contains("mfg.read was recropped"));
        assert!(output.contains("cursor-2"));
    }

    #[test]
    fn matching_runtime_backlink_reuses_the_shared_strategy_projection_summary() {
        let mut state = populated_state();
        state
            .incidents
            .first_mut()
            .expect("incident")
            .backlinks
            .extend([
                MfgBacklink {
                    kind: MfgBacklinkKind::Runtime,
                    target: "mfg-execution://execution-547".to_string(),
                    label: "MFG execution".to_string(),
                },
                MfgBacklink {
                    kind: MfgBacklinkKind::Runtime,
                    target: "runtime-execution://execution-547".to_string(),
                    label: "Runtime".to_string(),
                },
            ]);
        state.focus = MfgViewFocus::Detail;
        state.request_selected_runtime_strategy_projection();
        assert_eq!(
            state.take_runtime_backlink_request().as_deref(),
            Some("runtime-execution://execution-547"),
            "selection must automatically request the canonical Runtime graph, not its MFG-local execution"
        );
        assert_eq!(
            state
                .activate_backlink(MfgBacklinkKind::Runtime)
                .as_ref()
                .map(|backlink| backlink.target.as_str()),
            Some("runtime-execution://execution-547"),
            "manual runtime navigation must prefer the same canonical graph"
        );
        let strategy: StrategyDecisionProjection = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-projection-v1.json"
        ))
        .expect("shared strategy fixture");
        state.record_runtime_strategy_projection(
            &execution_projection("execution-547", strategy),
            state.generation,
            state.selection_revision,
            state.live_generation,
            state.live_epoch.clone(),
            state.live_reauthentication_count,
        );

        let output = render_with_strategy(120, 40, &state, None);

        assert!(output.contains("Strategy"));
        assert!(output.contains("team / collaborate"));
        assert!(output.contains("Estimate: 48000ms"));
        assert!(output.contains("Actual: 51000ms"));
        assert_eq!(
            state
                .selected_runtime_strategy_projection()
                .map(|projection| projection.execution_id.as_str()),
            Some("execution-547"),
            "historical MFG projection remains keyed and does not replace the active chat projection"
        );

        state.live_reauthentication_count = state.live_reauthentication_count.saturating_add(1);
        assert!(state.selected_runtime_strategy_projection().is_none());
        let recropped = render_with_strategy(120, 40, &state, None);
        assert!(!recropped.contains("team / collaborate"));
        assert!(!recropped.contains("evidence-strategy-547"));
    }

    #[test]
    fn coincident_mfg_execution_id_never_selects_a_runtime_strategy() {
        let mut state = populated_state();
        state.execution_ref = Some("execution-547".to_string());
        state
            .incidents
            .first_mut()
            .expect("incident")
            .backlinks
            .push(MfgBacklink {
                kind: MfgBacklinkKind::Runtime,
                target: "mfg-execution://execution-547".to_string(),
                label: "MFG execution".to_string(),
            });
        state.focus = MfgViewFocus::Detail;
        let strategy: StrategyDecisionProjection = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-projection-v1.json"
        ))
        .expect("shared strategy fixture");

        let output = render_with_strategy(120, 40, &state, Some(&strategy));

        assert!(!output.contains("team / collaborate"));
        assert!(!output.contains("Estimate: 48000ms"));
    }

    #[test]
    fn panel_visible_route_inventory_equals_the_derived_tui_p0_read_contract() {
        let expected = app_mfg_contract::mfg_tui_p0_read_route_contracts()
            .into_iter()
            .map(|route| route.route_id)
            .collect::<std::collections::BTreeSet<_>>();
        let visible = MFG_PANEL_READ_ROUTE_IDS
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(visible, expected);
        let state = MfgOperationsState::default();
        assert!(
            MFG_PANEL_READ_ROUTE_IDS
                .iter()
                .all(|route| state.route_projection_status(*route).is_some())
        );
        assert_eq!(
            MFG_PANEL_READ_ROUTE_IDS
                .iter()
                .filter(|route| { state.route_projection_status(**route) == Some("not-requested") })
                .count(),
            18
        );
    }
}
