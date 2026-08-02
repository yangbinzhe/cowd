use crossterm::event::{Event, KeyCode, KeyEventKind, MouseEventKind};
use harness_contract::projection::{ProjectionEntityPayload, StrategyDecisionProjection};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::{App, TimelineEntry};
use crate::components::panel_scroll::PanelScrollState;
use crate::components::strategy_decision::{
    strategy_agent_ids, strategy_runtime_backlink_targets, strategy_summary_lines,
};
use crate::components::{Component, EventResult, RenderContext};

/// Turn-level activity summary for "what's happening now" display.
#[derive(Debug, Clone, Default)]
struct TurnActivity {
    active: bool,
    thinking: bool,
    tool_count: u32,
    tool_names: Vec<String>,
    last_phase: String,
}

#[derive(Debug, Clone)]
enum ProcessKind {
    Thinking,
    Tool,
    Output,
    Slash,
}

#[derive(Debug, Clone)]
struct ProcessEvent {
    kind: ProcessKind,
    text: String,
    complete: bool,
    exit_code: Option<i32>,
    turn_index: usize,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeActivityPanel {
    // ── Context / Ctx fields ───────────────────────────────────────
    token_count: u64,
    context_window: u64,
    turn_input_tokens: u64,
    turn_output_tokens: u64,
    profile: String,
    pressure_pct: u16,
    pressure_level: String,
    degradation_path: String,
    policy_action: String,
    policy_reason: String,
    runtime_policy_level: String,
    runtime_policy_score: u16,
    runtime_policy_agent: String,
    runtime_policy_review: bool,
    runtime_policy_signals: usize,
    selected_count: usize,
    omitted_count: usize,
    stable_hash: String,
    runtime_hash: String,
    dynamic_hash: String,
    execution_graph_status: String,
    execution_graph_graph_id: String,
    execution_graph_board_id: String,
    execution_graph_agent_tasks: usize,
    execution_graph_children: usize,
    execution_graph_candidates: usize,
    execution_graph_conflicts: usize,
    execution_graph_completion_pct: String,
    execution_work_width: usize,
    execution_work_depth: usize,
    execution_work_expected_ms: u64,
    execution_work_actual_ms: u64,
    execution_work_speedup_bp: Option<u32>,
    projection_run_count: usize,
    projection_tool_count: usize,
    projection_selected_count: usize,
    projection_omitted_count: usize,
    projection_team_event_count: usize,
    projection_approval_count: usize,
    projection_model_speed: String,
    projection_admission_status: String,
    projection_admission_queue_ms: u64,
    projection_admission_wait_reason: String,
    projection_outcome_status: String,
    projection_outcome_duration_ms: u64,
    projection_outcome_tools: u64,
    projection_outcome_quality: String,
    projection_evidence_count: usize,
    projection_evidence_completeness: String,
    session_id: String,
    model: String,
    provider_status: String,
    provider_count: usize,
    provider_model_count: usize,
    provider_route: String,
    provider_names: String,
    control_plane_status: String,
    control_plane_reason: String,
    focused_backlink_target: Option<String>,
    focused_backlink_resolution: Option<String>,
    strategy: Option<StrategyDecisionProjection>,
    strategy_agent_ids: Vec<String>,
    strategy_backlink_targets: Vec<String>,
    strategy_backlink_index: usize,
    yolo_mode: bool,

    // ── Runtime counters ─────────────────────────────────────────
    event_count: usize,
    message_count: usize,
    tool_event_count: usize,
    open_tool_count: usize,
    recent_tools: Vec<String>,
    recent_process: Vec<ProcessEvent>,
    /// Current turn activity summary.
    turn_activity: TurnActivity,
    /// Scroll offset for long runtime status content.
    scroll: PanelScrollState,
}

impl RuntimeActivityPanel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync_from_app(&mut self, app: &App) {
        // ── Context / Ctx info ────────────────────────────────────
        self.token_count = app.token_count;
        self.context_window = app.context_window;
        self.turn_input_tokens = app.turn_input_tokens;
        self.turn_output_tokens = app.turn_output_tokens;
        self.yolo_mode = app.yolo_mode;
        self.session_id = app.session_id.clone();
        self.model = app.model.clone();
        self.strategy = app
            .latest_execution_projection
            .as_ref()
            .and_then(|projection| projection.strategy.clone());
        self.projection_admission_status.clear();
        self.projection_admission_queue_ms = 0;
        self.projection_admission_wait_reason.clear();
        self.projection_outcome_status.clear();
        self.projection_outcome_duration_ms = 0;
        self.projection_outcome_tools = 0;
        self.projection_outcome_quality.clear();
        self.projection_evidence_count = 0;
        self.projection_evidence_completeness.clear();
        self.execution_work_width = 0;
        self.execution_work_depth = 0;
        self.execution_work_expected_ms = 0;
        self.execution_work_actual_ms = 0;
        self.execution_work_speedup_bp = None;
        if let Some(projection) = app.latest_execution_projection.as_ref() {
            if let Some(work) = projection.graph.work.as_ref() {
                self.execution_work_width = work.width;
                self.execution_work_depth = work.depth;
                self.execution_work_expected_ms = work.expected_critical_path_ms;
                self.execution_work_actual_ms = work.actual_critical_path_ms;
                self.execution_work_speedup_bp = work
                    .actual_speedup_basis_points
                    .or(work.expected_speedup_basis_points);
            }
            if let Some(admission) = projection
                .admissions
                .iter()
                .filter_map(|entity| match entity.payload.as_ref() {
                    Some(ProjectionEntityPayload::Admission(admission)) => {
                        Some((entity.revision, admission))
                    }
                    _ => None,
                })
                .max_by_key(|(revision, _)| *revision)
                .map(|(_, admission)| admission)
            {
                self.projection_admission_status =
                    format!("{:?}", admission.status).to_ascii_lowercase();
                self.projection_admission_queue_ms = admission.queue_age_ms;
                self.projection_admission_wait_reason =
                    admission.wait_reason.clone().unwrap_or_default();
            }
            if let Some(outcome) = projection
                .outcomes
                .iter()
                .filter_map(|entity| match entity.payload.as_ref() {
                    Some(ProjectionEntityPayload::Outcome(outcome)) => {
                        Some((entity.revision, outcome))
                    }
                    _ => None,
                })
                .max_by_key(|(revision, _)| *revision)
                .map(|(_, outcome)| outcome)
            {
                self.projection_outcome_status = outcome.terminal_class.clone();
                self.projection_outcome_duration_ms = outcome.duration_ms;
                self.projection_outcome_tools = outcome.tool_calls;
                self.projection_outcome_quality =
                    format!("{:?}", outcome.quality).to_ascii_lowercase();
                self.projection_evidence_completeness =
                    format!("{:?}", outcome.evidence_completeness).to_ascii_lowercase();
            }
            self.projection_evidence_count = projection.evidence.len();
        }
        self.strategy_agent_ids = app
            .latest_execution_projection
            .as_ref()
            .and_then(|projection| {
                projection
                    .strategy
                    .as_ref()
                    .map(|strategy| strategy_agent_ids(strategy, &projection.agents))
            })
            .unwrap_or_default();
        self.strategy_backlink_targets = self
            .strategy
            .as_ref()
            .map(|strategy| strategy_runtime_backlink_targets(strategy, &self.strategy_agent_ids))
            .unwrap_or_default();
        self.strategy_backlink_index = self
            .strategy_backlink_index
            .min(self.strategy_backlink_targets.len().saturating_sub(1));
        if let Some(resolution) = self
            .focused_backlink_target
            .as_deref()
            .and_then(|target| resolve_runtime_backlink(app, target))
        {
            self.focused_backlink_resolution = Some(resolution);
        }

        let mut provider_names = app
            .gateway_connector_accounts
            .iter()
            .map(|account| account.provider.clone())
            .collect::<Vec<_>>();
        provider_names.sort();
        provider_names.dedup();
        self.provider_count = provider_names.len();
        self.provider_model_count = app.available_models.len();
        self.provider_names = if provider_names.is_empty() {
            "none".to_string()
        } else {
            preview(&provider_names.join(","), 36)
        };
        self.provider_route = provider_names
            .first()
            .cloned()
            .unwrap_or_else(|| "unresolved".to_string());
        self.provider_status = if self.provider_count == 0 {
            "unconfigured".to_string()
        } else if self.provider_route == "unresolved" {
            "degraded".to_string()
        } else {
            "available".to_string()
        };

        let has_context = app.latest_context_envelope.is_some();
        if let Some(envelope) = &app.latest_context_envelope {
            let pressure_bp = envelope
                .pointer("/diagnostics/pressure_bp")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            self.profile = envelope
                .get("profile")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            self.pressure_pct = (pressure_bp / 100) as u16;
            self.pressure_level = if pressure_bp > 8_500 {
                "High".to_string()
            } else if pressure_bp > 7_000 {
                "Elevated".to_string()
            } else {
                "Nominal".to_string()
            };
            self.degradation_path = envelope
                .pointer("/diagnostics/degraded_sources")
                .and_then(serde_json::Value::as_array)
                .map(|values| values.len().to_string())
                .unwrap_or_else(|| "None".to_string());
            self.policy_action = app
                .latest_runtime_policy
                .as_ref()
                .map(|policy| policy.recommended_profile.clone())
                .unwrap_or_else(|| "None".to_string());
            self.policy_reason = app
                .latest_runtime_policy
                .as_ref()
                .map(|policy| format!("{} signals", policy.signal_count))
                .unwrap_or_default();
            self.selected_count = envelope
                .get("selected")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            self.omitted_count = envelope
                .get("omitted")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            self.stable_hash = short_hash(
                envelope
                    .pointer("/diagnostics/stable_head_hash")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            );
            self.runtime_hash = short_hash(
                envelope
                    .pointer("/diagnostics/runtime_header_hash")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            );
            self.dynamic_hash = short_hash(
                envelope
                    .pointer("/diagnostics/dynamic_tail_hash")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            );
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
            self.policy_reason = "".to_string();
            self.selected_count = 0;
            self.omitted_count = 0;
            self.stable_hash = "n/a".to_string();
            self.runtime_hash = "n/a".to_string();
            self.dynamic_hash = "n/a".to_string();
        }
        if let Some(policy) = &app.latest_runtime_policy {
            self.runtime_policy_level = policy.level.clone();
            self.runtime_policy_score = policy.score;
            self.runtime_policy_agent = policy.agent_mode.clone();
            self.runtime_policy_review = policy.requires_review;
            self.runtime_policy_signals = policy.signal_count;
        } else {
            self.runtime_policy_level = "Simple".to_string();
            self.runtime_policy_score = 0;
            self.runtime_policy_agent = "Off".to_string();
            self.runtime_policy_review = false;
            self.runtime_policy_signals = 0;
        }
        if let Some(summary) = &app.latest_execution_graph_summary {
            self.execution_graph_status = summary.status.clone();
            self.execution_graph_graph_id = summary
                .graph_id
                .as_deref()
                .map(short_id)
                .unwrap_or_else(|| "n/a".to_string());
            self.execution_graph_board_id = summary
                .board_id
                .as_deref()
                .map(short_id)
                .unwrap_or_else(|| "n/a".to_string());
            self.execution_graph_agent_tasks = summary.agent_tasks;
            self.execution_graph_children = summary.child_executions;
            self.execution_graph_candidates = summary.memory_candidates;
            self.execution_graph_conflicts = summary.conflicts;
            self.execution_graph_completion_pct = summary
                .completion_rate
                .map(|value| format!("{}%", (value * 100.0).round() as u16))
                .unwrap_or_else(|| "n/a".to_string());
        } else {
            self.execution_graph_status = "n/a".to_string();
            self.execution_graph_graph_id = "n/a".to_string();
            self.execution_graph_board_id = "n/a".to_string();
            self.execution_graph_agent_tasks = 0;
            self.execution_graph_children = 0;
            self.execution_graph_candidates = 0;
            self.execution_graph_conflicts = 0;
            self.execution_graph_completion_pct = "n/a".to_string();
        }
        if let Some(projection) = &app.latest_run_projection {
            self.projection_run_count = projection
                .pointer("/team_session/runtime_run_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default() as usize;
            self.projection_tool_count = projection
                .pointer("/tool_summary/count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default() as usize;
            self.projection_selected_count = projection
                .pointer("/memory_context/selected_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default() as usize;
            self.projection_omitted_count = projection
                .pointer("/memory_context/omitted_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default() as usize;
            self.projection_team_event_count = projection
                .pointer("/team_session/agent_events")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            self.projection_approval_count = projection
                .pointer("/risk_approval/count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default() as usize;
            self.projection_model_speed = projection
                .pointer("/token_speed/model_telemetry/wall_tokens_per_second")
                .or_else(|| projection.pointer("/token_speed/model_telemetry/tokens_per_second"))
                .and_then(serde_json::Value::as_f64)
                .map(|value| format!("{value:.1} tok/s"))
                .or_else(|| {
                    projection
                        .pointer("/token_speed/model_telemetry/model")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string)
                })
                .unwrap_or_else(|| "n/a".to_string());
        } else {
            self.projection_run_count = 0;
            self.projection_tool_count = 0;
            self.projection_selected_count = 0;
            self.projection_omitted_count = 0;
            self.projection_team_event_count = 0;
            self.projection_approval_count = 0;
            self.projection_model_speed = "n/a".to_string();
        }
        self.control_plane_status = if self.session_id.trim().is_empty() {
            "degraded".to_string()
        } else if self.provider_status == "degraded" {
            "degraded".to_string()
        } else if !has_context || self.runtime_policy_agent == "Off" {
            "attention".to_string()
        } else {
            "healthy".to_string()
        };
        self.control_plane_reason = format!(
            "session {} context {} agent {} graph-agents {} provider {} yolo {}",
            if self.session_id.trim().is_empty() {
                "missing"
            } else {
                "active"
            },
            if has_context { "ready" } else { "pending" },
            self.runtime_policy_agent,
            self.execution_graph_agent_tasks,
            self.provider_status,
            if self.yolo_mode { "on" } else { "off" }
        );

        // ── Runtime counters and current activity snapshot ──
        let timeline = app.timeline_clone_vec();
        self.event_count = timeline.len();
        self.message_count = 0;
        self.tool_event_count = 0;
        self.open_tool_count = 0;
        self.recent_tools.clear();
        self.recent_process.clear();
        self.turn_activity = TurnActivity::default();
        self.turn_activity.active = app.turn_is_active();
        let mut turn_index = 0usize;
        for entry in &timeline {
            match entry {
                TimelineEntry::Message { role, content, .. } => {
                    self.message_count += 1;
                    if role == "user" {
                        turn_index += 1;
                    }
                    if role == "assistant" {
                        self.recent_process.push(ProcessEvent {
                            kind: ProcessKind::Output,
                            text: preview(content, 96),
                            complete: true,
                            exit_code: None,
                            turn_index: turn_index.max(1),
                        });
                    }
                }
                TimelineEntry::Thinking {
                    content, complete, ..
                } => {
                    if !complete {
                        self.turn_activity.thinking = true;
                    }
                    let lines = content
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .count();
                    let detail = if content.trim().is_empty() {
                        format!("{} lines", lines)
                    } else {
                        format!("{} lines - {}", lines, preview(content, 96))
                    };
                    self.recent_process.push(ProcessEvent {
                        kind: ProcessKind::Thinking,
                        text: detail,
                        complete: *complete,
                        exit_code: None,
                        turn_index: turn_index.max(1),
                    });
                }
                TimelineEntry::ToolCall {
                    name,
                    preview,
                    output,
                    done,
                    exit_code,
                    ..
                } => {
                    self.tool_event_count += 1;
                    if !*done {
                        self.open_tool_count += 1;
                        self.turn_activity.tool_count += 1;
                        if self.turn_activity.tool_names.len() < 5 {
                            self.turn_activity.tool_names.push(name.clone());
                        }
                    } else {
                        self.turn_activity.tool_count += 1;
                    }
                    self.recent_tools.push(format_tool_process_line(
                        name, preview, output, *done, *exit_code,
                    ));
                    self.recent_process.push(ProcessEvent {
                        kind: ProcessKind::Tool,
                        text: format_tool_process_line(name, preview, output, *done, *exit_code),
                        complete: *done,
                        exit_code: *exit_code,
                        turn_index: turn_index.max(1),
                    });
                }
                TimelineEntry::SlashOutput {
                    command, output, ..
                } => {
                    self.recent_process.push(ProcessEvent {
                        kind: ProcessKind::Slash,
                        text: format!("/{command} - {}", preview(output, 96)),
                        complete: true,
                        exit_code: None,
                        turn_index: turn_index.max(1),
                    });
                }
            }
        }
        if let Some(entry) = timeline.last() {
            self.turn_activity.last_phase = match entry {
                TimelineEntry::Thinking { complete, .. } => {
                    if *complete {
                        "thinking done".into()
                    } else {
                        "thinking".into()
                    }
                }
                TimelineEntry::ToolCall { name, done, .. } => {
                    if *done {
                        format!("tool {} done", name)
                    } else {
                        format!("tool {} running", name)
                    }
                }
                TimelineEntry::Message { role, .. } => {
                    format!("{} responded", role)
                }
                TimelineEntry::SlashOutput { command, .. } => {
                    format!("/{} done", command)
                }
            };
        }
    }

    pub fn focus_backlink_target(&mut self, target: impl Into<String>) {
        self.focused_backlink_target = Some(target.into());
        self.scroll.top();
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
        self.focused_backlink_resolution = Some(format!(
            "{} status {}",
            object
                .get("id")
                .or_else(|| object.get("task_id"))
                .or_else(|| object.get("execution_id"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("canonical object"),
            object
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("loaded")
        ));
        self.scroll.top();
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
        self.scroll.top();
    }
}

impl Component for RuntimeActivityPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let mut lines: Vec<Line> = Vec::new();
        if let Some(strategy) = self.strategy.as_ref() {
            lines.extend(strategy_summary_lines(
                strategy,
                usize::from(area.width.saturating_sub(2)),
                &self.strategy_agent_ids,
            ));
            if !self.strategy_backlink_targets.is_empty() {
                lines.push(Line::from(Span::styled(
                    "Strategy links · [/] select · Enter resolve",
                    Style::default().fg(Color::DarkGray),
                )));
                for (index, target) in self.strategy_backlink_targets.iter().enumerate() {
                    let marker = if index == self.strategy_backlink_index {
                        "›"
                    } else {
                        " "
                    };
                    lines.push(Line::from(format!("{marker} {}", preview(target, 76))));
                }
            }
            lines.push(Line::raw(""));
        }
        if let Some(target) = self.focused_backlink_target.as_deref() {
            lines.push(Line::from(vec![
                Span::styled(
                    "Backlink target: ",
                    Style::default().fg(Color::LightMagenta),
                ),
                Span::styled(preview(target, 72), Style::default().fg(Color::White)),
            ]));
            lines.push(Line::from(vec![
                Span::styled(
                    "Resolved object: ",
                    Style::default().fg(Color::LightMagenta),
                ),
                Span::styled(
                    self.focused_backlink_resolution
                        .as_deref()
                        .unwrap_or("loading canonical Runtime projection")
                        .to_string(),
                    Style::default().fg(Color::White),
                ),
            ]));
        }

        if !self.policy_action.is_empty() && self.policy_action != "None" {
            lines.push(Line::from(vec![
                Span::styled("Policy:", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    preview(
                        &format!("{} - {}", self.policy_action, self.policy_reason),
                        60,
                    ),
                    Style::default().fg(Color::White),
                ),
            ]));
        }
        if self.execution_graph_agent_tasks > 0 {
            lines.push(Line::from(vec![
                Span::styled("Graph:", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        "{} {} agents {} children {}",
                        self.execution_graph_status,
                        self.execution_graph_completion_pct,
                        self.execution_graph_agent_tasks,
                        self.execution_graph_children
                    ),
                    Style::default().fg(if self.execution_graph_status == "completed" {
                        Color::Green
                    } else {
                        Color::Cyan
                    }),
                ),
                Span::styled(
                    format!("  conflicts {}", self.execution_graph_conflicts),
                    Style::default().fg(if self.execution_graph_conflicts > 0 {
                        Color::Yellow
                    } else {
                        Color::DarkGray
                    }),
                ),
            ]));
        }
        if self.execution_work_width > 0 || self.execution_work_depth > 0 {
            lines.push(Line::from(vec![
                Span::styled("Work:", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        " width {} depth {} critical {}ms/{}ms speedup {}",
                        self.execution_work_width,
                        self.execution_work_depth,
                        self.execution_work_actual_ms,
                        self.execution_work_expected_ms,
                        self.execution_work_speedup_bp
                            .map(|value| format!("{:.2}x", f64::from(value) / 10_000.0))
                            .unwrap_or_else(|| "n/a".to_string())
                    ),
                    Style::default().fg(Color::Cyan),
                ),
            ]));
        }
        if self.projection_run_count > 0
            || self.projection_tool_count > 0
            || self.projection_selected_count > 0
            || self.projection_team_event_count > 0
        {
            lines.push(Line::from(vec![
                Span::styled("Projection:", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        " runs {} tools {} mem {}/{} team {} approvals {} speed {}",
                        self.projection_run_count,
                        self.projection_tool_count,
                        self.projection_selected_count,
                        self.projection_omitted_count,
                        self.projection_team_event_count,
                        self.projection_approval_count,
                        self.projection_model_speed
                    ),
                    Style::default().fg(Color::Cyan),
                ),
            ]));
        }
        if !self.projection_admission_status.is_empty()
            || !self.projection_outcome_status.is_empty()
            || self.projection_evidence_count > 0
        {
            let admission = if self.projection_admission_status.is_empty() {
                "unknown".to_string()
            } else if self.projection_admission_wait_reason.is_empty() {
                format!(
                    "{} {}ms",
                    self.projection_admission_status, self.projection_admission_queue_ms
                )
            } else {
                format!(
                    "{} {}ms {}",
                    self.projection_admission_status,
                    self.projection_admission_queue_ms,
                    preview(&self.projection_admission_wait_reason, 24)
                )
            };
            let outcome = if self.projection_outcome_status.is_empty() {
                "pending".to_string()
            } else {
                format!(
                    "{} {}ms tools {} quality {}",
                    self.projection_outcome_status,
                    self.projection_outcome_duration_ms,
                    self.projection_outcome_tools,
                    self.projection_outcome_quality
                )
            };
            lines.push(Line::from(vec![
                Span::styled("Lifecycle:", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        " admission {admission} | outcome {outcome} | evidence {} {}",
                        self.projection_evidence_count, self.projection_evidence_completeness
                    ),
                    Style::default().fg(Color::Cyan),
                ),
            ]));
        }

        if !self.recent_process.is_empty() {
            if !lines.is_empty() {
                lines.push(Line::raw(""));
            }
            let remaining_rows = area
                .height
                .saturating_sub(u16::try_from(lines.len()).unwrap_or(u16::MAX))
                .saturating_sub(3)
                .max(6) as usize;
            let start = self.recent_process.len().saturating_sub(remaining_rows);
            let mut last_turn = 0usize;
            for item in self.recent_process.iter().skip(start) {
                if item.turn_index != last_turn {
                    last_turn = item.turn_index;
                    lines.push(process_separator_line(
                        last_turn,
                        area.width.saturating_sub(2),
                    ));
                }
                lines.push(process_line(item));
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));
        self.scroll.sync(
            lines.len(),
            usize::from(area.height.saturating_sub(2).max(1)),
        );
        let visible = lines
            .into_iter()
            .skip(self.scroll.offset)
            .take(self.scroll.viewport_len)
            .collect::<Vec<_>>();
        ctx.frame_mut().render_widget(
            Paragraph::new(visible)
                .block(block)
                .scroll((0, 0))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Char('j') | KeyCode::Down => self.scroll.line_down(),
                KeyCode::Char('k') | KeyCode::Up => self.scroll.line_up(),
                KeyCode::PageDown => self.scroll.page_down(),
                KeyCode::PageUp => self.scroll.page_up(),
                KeyCode::Home => self.scroll.top(),
                KeyCode::End => self.scroll.bottom(),
                KeyCode::Char('[') if !self.strategy_backlink_targets.is_empty() => {
                    self.strategy_backlink_index = self.strategy_backlink_index.saturating_sub(1);
                }
                KeyCode::Char(']') if !self.strategy_backlink_targets.is_empty() => {
                    self.strategy_backlink_index = (self.strategy_backlink_index + 1)
                        .min(self.strategy_backlink_targets.len().saturating_sub(1));
                }
                KeyCode::Enter if !self.strategy_backlink_targets.is_empty() => {
                    self.focused_backlink_target = self
                        .strategy_backlink_targets
                        .get(self.strategy_backlink_index)
                        .cloned();
                    self.focused_backlink_resolution = None;
                    self.scroll.top();
                }
                _ => return EventResult::NotConsumed,
            },
            Event::Mouse(mouse) => match mouse.kind {
                MouseEventKind::ScrollDown => self.scroll.line_down(),
                MouseEventKind::ScrollUp => self.scroll.line_up(),
                _ => return EventResult::NotConsumed,
            },
            _ => return EventResult::NotConsumed,
        }
        EventResult::Consumed
    }

    fn focusable(&self) -> bool {
        false
    }

    fn id(&self) -> &str {
        "runtime_activity_panel"
    }
}

impl RuntimeActivityPanel {
    pub fn copy_text(&self) -> bool {
        let mut out = String::new();
        let mut last_turn = 0usize;
        for item in &self.recent_process {
            if item.turn_index != last_turn {
                last_turn = item.turn_index;
                out.push_str(&format!("\n#{}\n", last_turn));
            }
            out.push_str(&format!("{}\n", item.text));
        }
        if out.trim().is_empty() {
            return false;
        }
        crate::osc52::write_osc52_clipboard(out.trim())
    }
}

fn process_line(item: &ProcessEvent) -> Line<'static> {
    let (branch, icon, color) = match item.kind {
        ProcessKind::Thinking => (
            "├─ ",
            "🧠",
            if item.complete {
                Color::DarkGray
            } else {
                Color::Yellow
            },
        ),
        ProcessKind::Tool => (
            "├─ ",
            "⚙",
            if item.complete && item.exit_code.unwrap_or(0) != 0 {
                Color::Red
            } else {
                Color::Green
            },
        ),
        ProcessKind::Output => ("└─ ", "", Color::Green),
        ProcessKind::Slash => ("├─ ", "⌘", Color::Magenta),
    };
    Line::from(vec![
        Span::styled(branch.to_string(), Style::default().fg(color).bold()),
        Span::styled(
            if icon.is_empty() {
                String::new()
            } else {
                format!("{icon} ")
            },
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            preview(&item.text, 88),
            Style::default().fg(match item.kind {
                ProcessKind::Output => Color::Gray,
                _ => Color::White,
            }),
        ),
    ])
}

fn process_separator_line(turn: usize, width: u16) -> Line<'static> {
    let label = format!(" #{} ", turn);
    let label_width = label.chars().count();
    let width = usize::from(width).max(label_width);
    let side = width.saturating_sub(label_width) / 2;
    let right = width.saturating_sub(label_width).saturating_sub(side);
    Line::from(Span::styled(
        format!("{}{}{}", "─".repeat(side), label, "─".repeat(right)),
        Style::default().fg(Color::DarkGray),
    ))
}

fn preview(value: &str, max: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max {
        normalized
    } else {
        normalized.chars().take(max).collect::<String>() + "..."
    }
}

fn format_tool_process_line(
    name: &str,
    tool_preview: &str,
    output: &str,
    done: bool,
    exit_code: Option<i32>,
) -> String {
    let state = if done {
        format!("done exit:{}", exit_code.unwrap_or(0))
    } else {
        "running".to_string()
    };
    let detail = if !output.trim().is_empty() {
        output
    } else {
        tool_preview
    };
    if detail.trim().is_empty() {
        format!("{name} {state}")
    } else {
        format!("{name} {state} - {}", preview(detail, 72))
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

fn resolve_runtime_backlink(app: &App, target: &str) -> Option<String> {
    let target = target.trim();
    let query = target.split_once('?').map(|(_, query)| query);
    let target = target
        .strip_prefix("runtime-execution://")
        .unwrap_or(target)
        .trim_start_matches("execution://")
        .trim_start_matches("execution:")
        .trim_start_matches("task://")
        .trim_start_matches("task:");
    let target_id = target
        .split_once(":execution:")
        .map_or_else(|| target, |(_, execution_id)| execution_id)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if target_id.is_empty() {
        return None;
    }
    if let Some(projection) = app
        .latest_execution_projection
        .as_ref()
        .filter(|projection| projection.execution_id == target_id)
    {
        if let Some(agent_id) = query.and_then(|query| {
            query
                .split('&')
                .find_map(|item| item.strip_prefix("agent_id="))
        }) {
            let agent = projection
                .agents
                .iter()
                .find(|agent| agent.id == agent_id)?;
            return Some(format!(
                "execution {} agent {} status {}",
                projection.execution_id,
                agent.id,
                agent.status.as_deref().unwrap_or("unknown")
            ));
        }
        return Some(format!(
            "execution {} revision {} cursor {} nodes {}",
            projection.execution_id,
            projection.revision,
            projection.cursor,
            projection.graph.nodes.len()
        ));
    }
    app.gateway_tasks
        .iter()
        .find(|task| task.id == target_id)
        .map(|task| {
            format!(
                "task {} status {} phase {}",
                task.id,
                task.status,
                task.current_phase.as_deref().unwrap_or("none")
            )
        })
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;
    use crate::CowdEvent;

    fn render_panel(panel: &mut RuntimeActivityPanel, width: u16, height: u16) -> String {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines().join("\n")
    }

    fn strategy_projection() -> crate::protocol::ExecutionProjection {
        let strategy: StrategyDecisionProjection = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-projection-v1.json"
        ))
        .expect("shared strategy fixture");
        crate::protocol::ExecutionProjection {
            schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
            execution_id: "execution-547".to_string(),
            revision: 4,
            cursor: 4,
            detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
            authorization_revision: 1,
            redaction_revision: "redaction-1".to_string(),
            session_id: Some("session-547".to_string()),
            mission_id: Some("mission-547".to_string()),
            strategy: Some(strategy),
            graph: harness_contract::execution_graph::project_execution_graph(
                &harness_contract::execution_graph::ExecutionGraph::new("strategy"),
            ),
            child_executions: Vec::new(),
            goals: Vec::new(),
            agents: vec![harness_contract::projection::ProjectionEntity {
                id: "agent-547".to_string(),
                kind: "agent".to_string(),
                revision: 1,
                status: Some("running".to_string()),
                summary: None,
                evidence_refs: Vec::new(),
                payload: None,
                detail: Some(serde_json::json!({"graph_id": "execution-547"})),
            }],
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
            admissions: Vec::new(),
            outcomes: Vec::new(),
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
    fn syncs_runtime_and_context_from_app() {
        let mut app = App::new("m", "session-runtime-123456789");
        app.yolo_mode = true;
        app.available_models = vec!["m".to_string(), "m-fast".to_string()];
        app.gateway_connector_accounts =
            vec![crate::runtime_control_store::ConnectorAccountSummary {
                provider: "anthropic".to_string(),
                account_id: "account-1".to_string(),
                auth_mode: "token".to_string(),
                status: "available".to_string(),
                reason: None,
                binding_count: 1,
            }];
        app.token_count = 42_000;
        app.context_window = 200_000;
        app.turn_input_tokens = 12_000;
        app.turn_output_tokens = 3_000;
        app.add_message("user", "ship the runtime console");
        app.apply_event(CowdEvent::ToolStart {
            id: "tool-1".to_string(),
            name: "bash".to_string(),
            preview: "cargo test".to_string(),
        });
        app.apply_event(CowdEvent::ToolComplete {
            id: "tool-1".to_string(),
            name: "bash".to_string(),
            summary: "ok".to_string(),
            exit_code: Some(0),
        });
        app.apply_event(CowdEvent::ExecutionGraphSummary {
            summary: crate::RuntimeExecutionGraphSummary {
                graph_id: Some("graph-run".to_string()),
                board_id: Some("board-run".to_string()),
                status: "completed".to_string(),
                agent_tasks: 2,
                child_executions: 0,
                memory_candidates: 1,
                conflicts: 1,
                completion_rate: Some(1.0),
                synthesis_lift: Some(1.2),
                complementarity_score: Some(0.75),
            },
        });
        app.apply_event(CowdEvent::RuntimePolicyDecision {
            summary: crate::RuntimePolicyDecisionSummary {
                level: "Complex".to_string(),
                score: 70,
                recommended_profile: "Collaboration".to_string(),
                agent_mode: "Parallel".to_string(),
                requires_review: false,
                signal_count: 3,
            },
        });

        app.latest_context_envelope = Some(crate::test_utils::context_envelope_fixture());
        app.apply_run_projection(serde_json::json!({
            "kind": "session.run_projection",
            "team_session": {
                "runtime_run_count": 2,
                "agent_events": [{"type": "AgentTeamStatus"}]
            },
            "tool_summary": {
                "count": 3
            },
            "memory_context": {
                "selected_count": 4,
                "omitted_count": 1,
                "context_envelope": crate::test_utils::context_envelope_fixture()
            },
            "risk_approval": {
                "count": 1
            },
            "token_speed": {
                "stats": {
                    "tokens": {
                        "total": 42000
                    }
                },
                "model_telemetry": {
                    "wall_tokens_per_second": 21.25,
                    "tokens_per_second": 21.25
                }
            }
        }));

        let mut panel = RuntimeActivityPanel::new();
        panel.sync_from_app(&app);
        let rendered = render_panel(&mut panel, 96, 27);

        // Runtime header/model/token duplication is intentionally omitted here.
        assert!(!rendered.contains("42.0k"));
        assert!(!rendered.contains("200.0k"));
        assert!(!rendered.contains("Model:"));
        assert!(!rendered.contains("Activity:"));
        assert!(rendered.contains("Graph:"));
        assert!(rendered.contains("Projection:"));
        assert!(rendered.contains("runs 2 tools 3 mem 4/1 team 1 approvals 1 speed 21.2 tok/s"));
        assert!(!rendered.contains("Process"));
        assert!(rendered.contains("#1"));
        assert!(rendered.contains("bash done exit:0 - ok"));

        // The runtime panel owns tool process details; the separate Activity
        // panel remains a manually opened recent-event stream.
        assert!(!rendered.contains("user: ship the runtime console"));
    }

    #[test]
    fn renders_canonical_admission_outcome_and_evidence_without_prose_inference() {
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/projection-v2/materialization.json"
        ))
        .expect("canonical projection corpus");
        let projection: crate::protocol::ExecutionProjection =
            serde_json::from_value(corpus["expected"].clone()).expect("expected projection");
        let mut app = App::new("m", "session-golden");
        app.latest_execution_projection = Some(projection);

        let mut panel = RuntimeActivityPanel::new();
        panel.sync_from_app(&app);
        let rendered = render_panel(&mut panel, 120, 16);

        assert!(rendered.contains("Lifecycle:"));
        assert!(rendered.contains("admission materialized 8ms"));
        assert!(rendered.contains("outcome completed 12ms tools 1 quality unknown"));
        assert!(rendered.contains("evidence 1 sufficient"));
    }

    #[test]
    fn runtime_status_panel_consumes_scroll_keys() {
        let mut panel = RuntimeActivityPanel::new();
        let event = crossterm::event::Event::Key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Down,
            crossterm::event::KeyModifiers::NONE,
        ));

        assert_eq!(panel.handle_event(&event), EventResult::Consumed);
        assert!(!panel.focusable());
    }

    #[test]
    fn strategy_agent_backlink_keyboard_activation_resolves_canonical_focus() {
        let mut app = App::new("m", "session-547");
        app.latest_execution_projection = Some(strategy_projection());
        let mut panel = RuntimeActivityPanel::new();
        panel.sync_from_app(&app);
        assert_eq!(
            panel.strategy_backlink_targets,
            vec![
                "runtime-execution://execution-547".to_string(),
                "runtime-execution://execution-547?agent_id=agent-547".to_string()
            ]
        );

        let next = Event::Key(crossterm::event::KeyEvent::new(
            KeyCode::Char(']'),
            crossterm::event::KeyModifiers::NONE,
        ));
        let activate = Event::Key(crossterm::event::KeyEvent::new(
            KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&next), EventResult::Consumed);
        assert_eq!(panel.handle_event(&activate), EventResult::Consumed);
        panel.sync_from_app(&app);

        assert_eq!(
            panel.focused_backlink_target.as_deref(),
            Some("runtime-execution://execution-547?agent_id=agent-547")
        );
        assert!(panel
            .focused_backlink_resolution
            .as_deref()
            .is_some_and(|resolution| resolution.contains("agent agent-547 status running")));
    }

    #[test]
    fn strategy_runtime_links_never_render_or_activate_unsafe_legacy_identity() {
        let corpus: Vec<String> = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-public-redaction-corpus.json"
        ))
        .expect("shared redaction corpus");
        let activate = Event::Key(crossterm::event::KeyEvent::new(
            KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        ));
        for secret in corpus {
            let mut projection = strategy_projection();
            let strategy = projection.strategy.as_mut().expect("strategy fixture");
            strategy.team_execution_id = Some(secret.clone());
            projection.agents[0].id = format!("agent-{secret}");
            projection.agents[0].detail = Some(serde_json::json!({"graph_id": secret}));
            let mut app = App::new("m", "session-safe-links");
            app.latest_execution_projection = Some(projection);
            let mut panel = RuntimeActivityPanel::new();
            panel.sync_from_app(&app);

            assert!(
                panel.strategy_backlink_targets.is_empty(),
                "unsafe target {secret}"
            );
            assert_eq!(panel.handle_event(&activate), EventResult::NotConsumed);
            assert!(panel.focused_backlink_target.is_none());
            assert!(!render_panel(&mut panel, 96, 27).contains(&secret));
        }
    }

    #[test]
    fn process_lines_start_with_tree_branch_and_icons() {
        let line = process_line(&ProcessEvent {
            kind: ProcessKind::Tool,
            text: "bash done exit:0".to_string(),
            complete: true,
            exit_code: Some(0),
            turn_index: 1,
        });
        let rendered = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(rendered.starts_with("├─ ⚙ bash"), "{rendered}");
        assert!(!rendered.contains("tool"), "{rendered}");

        let line = process_line(&ProcessEvent {
            kind: ProcessKind::Thinking,
            text: "1 lines".to_string(),
            complete: true,
            exit_code: None,
            turn_index: 1,
        });
        let rendered = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(rendered.starts_with("├─ 🧠 1 lines"), "{rendered}");
        assert!(!rendered.contains("think"), "{rendered}");
    }
}
