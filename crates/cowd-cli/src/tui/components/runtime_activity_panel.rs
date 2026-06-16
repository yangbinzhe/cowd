use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::{App, TimelineEntry};
use crate::tui::components::panel_scroll::{offset_to_u16, PanelScrollState};
use crate::tui::components::{Component, EventResult, RenderContext};

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
    workgraph_status: String,
    workgraph_graph_id: String,
    workgraph_board_id: String,
    workgraph_agent_tasks: usize,
    workgraph_candidates: usize,
    workgraph_conflicts: usize,
    workgraph_completion_pct: String,
    session_id: String,
    model: String,
    provider_status: String,
    provider_count: usize,
    provider_model_count: usize,
    provider_route: String,
    provider_names: String,
    control_plane_status: String,
    control_plane_reason: String,
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

        let mut provider_names = runtime::list_all_providers();
        provider_names.sort();
        self.provider_count = provider_names.len();
        self.provider_model_count = runtime::list_all_models().len();
        self.provider_names = if provider_names.is_empty() {
            "none".to_string()
        } else {
            preview(&provider_names.join(","), 36)
        };
        self.provider_route = runtime::resolve_global_provider(&app.model)
            .map(|provider| {
                format!(
                    "{} ({})",
                    provider.name,
                    provider.protocol.as_deref().unwrap_or("openai-compat")
                )
            })
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
        if let Some(summary) = &app.latest_workgraph_summary {
            self.workgraph_status = summary.status.clone();
            self.workgraph_graph_id = summary
                .graph_id
                .as_deref()
                .map(short_id)
                .unwrap_or_else(|| "n/a".to_string());
            self.workgraph_board_id = summary
                .board_id
                .as_deref()
                .map(short_id)
                .unwrap_or_else(|| "n/a".to_string());
            self.workgraph_agent_tasks = summary.agent_tasks;
            self.workgraph_candidates = summary.memory_candidates;
            self.workgraph_conflicts = summary.conflicts;
            self.workgraph_completion_pct = summary
                .completion_rate
                .map(|value| format!("{}%", (value * 100.0).round() as u16))
                .unwrap_or_else(|| "n/a".to_string());
        } else {
            self.workgraph_status = "n/a".to_string();
            self.workgraph_graph_id = "n/a".to_string();
            self.workgraph_board_id = "n/a".to_string();
            self.workgraph_agent_tasks = 0;
            self.workgraph_candidates = 0;
            self.workgraph_conflicts = 0;
            self.workgraph_completion_pct = "n/a".to_string();
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
            self.workgraph_agent_tasks,
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
        self.turn_activity.active = app.turn_active;
        for entry in &timeline {
            match entry {
                TimelineEntry::Message { role, content, .. } => {
                    self.message_count += 1;
                    if role == "assistant" {
                        self.recent_process.push(ProcessEvent {
                            kind: ProcessKind::Output,
                            text: preview(content, 96),
                            complete: true,
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
                    });
                }
                TimelineEntry::SlashOutput { command, output, .. } => {
                    self.recent_process.push(ProcessEvent {
                        kind: ProcessKind::Slash,
                        text: format!("/{command} - {}", preview(output, 96)),
                        complete: true,
                    });
                }
            }
        }
        if self.recent_process.len() > 80 {
            let overflow = self.recent_process.len().saturating_sub(80);
            self.recent_process.drain(0..overflow);
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
}

impl Component for RuntimeActivityPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        // ── Compact context header (top section) ──────────────────
        let pct = if self.context_window > 0 {
            (self.token_count as f64 / self.context_window as f64 * 100.0).min(100.0)
        } else {
            0.0
        };
        let bar_color = if pct > 90.0 {
            Color::Red
        } else if pct > 70.0 {
            Color::Yellow
        } else {
            Color::Green
        };
        let bar = Self::progress_bar(self.token_count, self.context_window, 8);

        let mut lines: Vec<Line> = Vec::new();

        // Token bar
        lines.push(Line::from(vec![
            Span::styled(bar, Style::default().fg(bar_color)),
            Span::styled(
                format!(
                    " {:.0}% {} / {}",
                    pct,
                    fmt_tokens(self.token_count),
                    fmt_tokens(self.context_window)
                ),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("In:", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.turn_input_tokens),
                Style::default().fg(Color::White),
            ),
            Span::styled(" Out:", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.turn_output_tokens),
                Style::default().fg(Color::White),
            ),
            Span::styled(
                format!("  {:.4} USD", self.cost_estimate()),
                Style::default().fg(Color::Yellow),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Sess:", Style::default().fg(Color::DarkGray)),
            Span::styled(self.session_id.clone(), Style::default().fg(Color::White)),
            Span::styled(
                format!("  {} {}%", self.profile, self.pressure_pct),
                Style::default().fg(if self.pressure_pct > 85 {
                    Color::Red
                } else if self.pressure_pct > 70 {
                    Color::Yellow
                } else {
                    Color::White
                }),
            ),
        ]));
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
        lines.push(Line::from(vec![
            Span::styled("Model:", Style::default().fg(Color::DarkGray)),
            Span::styled(preview(&self.model, 32), Style::default().fg(Color::White)),
            Span::styled(
                format!(
                    " via {}  {}p/{}m",
                    preview(&self.provider_route, 28),
                    self.provider_count,
                    self.provider_model_count
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        if self.workgraph_agent_tasks > 0 {
            lines.push(Line::from(vec![
                Span::styled("WG:", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        "{} {} agents {}",
                        self.workgraph_status,
                        self.workgraph_completion_pct,
                        self.workgraph_agent_tasks
                    ),
                    Style::default().fg(if self.workgraph_status == "completed" {
                        Color::Green
                    } else {
                        Color::Cyan
                    }),
                ),
                Span::styled(
                    format!("  conflicts {}", self.workgraph_conflicts),
                    Style::default().fg(if self.workgraph_conflicts > 0 {
                        Color::Yellow
                    } else {
                        Color::DarkGray
                    }),
                ),
            ]));
        }
        lines.push(Line::from(vec![
            Span::styled("Agent:", Style::default().fg(Color::DarkGray)),
            Span::styled(
                if self.yolo_mode {
                    "YoloGoal".to_string()
                } else {
                    self.runtime_policy_agent.clone()
                },
                Style::default().fg(if self.runtime_policy_agent == "Off" {
                    Color::DarkGray
                } else {
                    Color::Cyan
                }),
            ),
            Span::styled(
                format!(
                    "  policy {} score:{} review:{} signals:{}",
                    self.runtime_policy_level,
                    self.runtime_policy_score,
                    if self.runtime_policy_review { "required" } else { "clear" },
                    self.runtime_policy_signals
                ),
                Style::default().fg(match (self.yolo_mode, self.runtime_policy_review) {
                    (true, _) => Color::Magenta,
                    (_, true) => Color::Yellow,
                    _ => Color::Yellow,
                }),
            ),
        ]));

        lines.push(Line::raw(""));
        lines.push(metric_line(
            "Runtime:",
            &self.control_plane_status,
            &preview(&self.control_plane_reason, 72),
            match self.control_plane_status.as_str() {
                "healthy" => Color::Green,
                "degraded" => Color::Red,
                _ => Color::Yellow,
            },
        ));
        lines.push(metric_line(
            "Workgraph:",
            &self.workgraph_status,
            &format!(
                "graph {} board {} candidates {} conflicts {}",
                self.workgraph_graph_id,
                self.workgraph_board_id,
                self.workgraph_candidates,
                self.workgraph_conflicts
            ),
            if self.workgraph_conflicts > 0 {
                Color::Yellow
            } else {
                Color::White
            },
        ));
        lines.push(metric_line(
            "Activity:",
            &format!("{} events", self.event_count),
            &format!(
                "{} messages {} tools {} open last {}",
                self.message_count,
                self.tool_event_count,
                self.open_tool_count,
                preview(&self.turn_activity.last_phase, 32)
            ),
            if self.open_tool_count > 0 {
                Color::Yellow
            } else {
                Color::White
            },
        ));

        if !self.recent_process.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Process",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )));
            let remaining_rows = area
                .height
                .saturating_sub(lines.len() as u16)
                .saturating_sub(3)
                .max(6) as usize;
            let start = self.recent_process.len().saturating_sub(remaining_rows);
            for item in &self.recent_process[start..] {
                lines.push(process_line(item));
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Runtime ");
        self.scroll
            .sync(lines.len(), area.height.saturating_sub(2).max(1) as usize);
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(block)
                .scroll((offset_to_u16(self.scroll.offset), 0))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        if key.kind != KeyEventKind::Press {
            return EventResult::NotConsumed;
        }

        match key.code {
            KeyCode::Char('j') | KeyCode::Down => self.scroll.line_down(),
            KeyCode::Char('k') | KeyCode::Up => self.scroll.line_up(),
            KeyCode::PageDown => self.scroll.page_down(),
            KeyCode::PageUp => self.scroll.page_up(),
            KeyCode::Home => self.scroll.top(),
            KeyCode::End => self.scroll.bottom(),
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

// ── Helper methods ───────────────────────────────────────────────────

impl RuntimeActivityPanel {
    fn progress_bar(used: u64, total: u64, bar_width: usize) -> String {
        if total == 0 {
            return "░".repeat(bar_width);
        }
        let pct = (used as f64 / total as f64).min(1.0);
        let filled = (pct * bar_width as f64).round() as usize;
        let empty = bar_width.saturating_sub(filled);
        let mut bar = String::with_capacity(bar_width);
        for _ in 0..filled {
            bar.push('█');
        }
        for _ in 0..empty {
            bar.push('░');
        }
        bar
    }

    fn cost_estimate(&self) -> f64 {
        let input_cost = self.token_count as f64 * 3.0 / 1_000_000.0;
        let output_cost = self.turn_output_tokens as f64 * 15.0 / 1_000_000.0;
        input_cost + output_cost
    }
}

// ── Free functions ───────────────────────────────────────────────────

fn metric_line(
    label: impl Into<String>,
    value: impl Into<String>,
    detail: impl Into<String>,
    value_color: Color,
) -> Line<'static> {
    Line::from(vec![
        Span::styled(label.into(), Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!(" {}", value.into()),
            Style::default().fg(value_color),
        ),
        Span::styled(
            format!("  {}", detail.into()),
            Style::default().fg(Color::White),
        ),
    ])
}

fn process_line(item: &ProcessEvent) -> Line<'static> {
    let (icon, label, color) = match item.kind {
        ProcessKind::Thinking => (
            if item.complete { "◌" } else { "●" },
            "think",
            if item.complete { Color::DarkGray } else { Color::Yellow },
        ),
        ProcessKind::Tool => (
            if item.complete { "✓" } else { "⚙" },
            "tool",
            if item.complete { Color::Green } else { Color::Cyan },
        ),
        ProcessKind::Output => ("↳", "reply", Color::White),
        ProcessKind::Slash => ("/", "cmd", Color::Magenta),
    };
    Line::from(vec![
        Span::styled(format!("{icon} "), Style::default().fg(color).bold()),
        Span::styled(
            format!("{label:<5} "),
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

fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        n.to_string()
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

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;
    use std::collections::HashMap;

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
    fn syncs_runtime_and_context_from_app() {
        runtime::init_global_providers(runtime::ProvidersConfig {
            providers: HashMap::from([(
                "anthropic".to_string(),
                runtime::ProviderConfig {
                    name: "anthropic".to_string(),
                    base_url: "https://anthropic.example/v1".to_string(),
                    api_key: "secret".to_string(),
                    models: vec!["m".to_string(), "m-fast".to_string()],
                    protocol: Some("anthropic".to_string()),
                },
            )]),
        });
        let mut app = App::new("m", "session-runtime-123456789");
        app.yolo_mode = true;
        app.token_count = 42_000;
        app.context_window = 200_000;
        app.turn_input_tokens = 12_000;
        app.turn_output_tokens = 3_000;
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
        app.apply_event(runtime::CowdEvent::WorkGraphSummary {
            summary: runtime::RuntimeWorkGraphSummary {
                graph_id: Some("graph-run".to_string()),
                board_id: Some("board-run".to_string()),
                status: "completed".to_string(),
                agent_tasks: 2,
                memory_candidates: 1,
                conflicts: 1,
                completion_rate: Some(1.0),
                synthesis_lift: Some(1.2),
                complementarity_score: Some(0.75),
            },
        });
        app.apply_event(runtime::CowdEvent::RuntimePolicyDecision {
            summary: runtime::RuntimePolicyDecisionSummary {
                level: "Complex".to_string(),
                score: 70,
                recommended_profile: "Collaboration".to_string(),
                agent_mode: "Parallel".to_string(),
                requires_review: false,
                signal_count: 3,
            },
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
        let rendered = render_panel(&mut panel, 96, 27);

        // Verify context token info is shown
        assert!(rendered.contains("42.0k"));
        assert!(rendered.contains("200.0k"));
        assert!(rendered.contains("12.0k"));
        assert!(rendered.contains("3.0k"));

        // Verify runtime status summary
        assert!(rendered.contains("YoloGoal"));
        assert!(rendered.contains("Parallel"));
        assert!(rendered.contains("Runtime Status"));
        assert!(rendered.contains("Status"));
        assert!(rendered.contains("Provider:"));
        assert!(rendered.contains("Context:"));
        assert!(rendered.contains("Policy:"));
        assert!(rendered.contains("Workgraph:"));
        assert!(rendered.contains("Activity:"));
        assert!(rendered.contains("2 events"));
        assert!(rendered.contains("1 messages"));
        assert!(rendered.contains("1 tools"));
        assert!(rendered.contains("Tool Process"));
        assert!(rendered.contains("bash done exit:0 - ok"));

        // The runtime panel owns tool process details; the separate Activity
        // panel remains a manually opened recent-event stream.
        assert!(!rendered.contains("user: ship the runtime console"));

        runtime::init_global_providers(runtime::ProvidersConfig::default());
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
}
