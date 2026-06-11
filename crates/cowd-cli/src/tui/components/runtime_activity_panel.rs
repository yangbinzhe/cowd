use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::{App, TimelineEntry};
use crate::tui::components::{Component, EventResult, RenderContext};

/// Scrolling state for the Recent activities list.
#[derive(Debug, Clone, Default)]
struct ActivityScroll {
    /// Topmost visible entry index in the Recent list.
    offset: usize,
    /// Total number of entries.
    total: usize,
}

impl ActivityScroll {
    fn scroll_down(&mut self, visible_rows: usize) {
        let max = self.total.saturating_sub(visible_rows);
        if self.offset < max {
            self.offset += 1;
        }
    }

    fn scroll_up(&mut self) {
        self.offset = self.offset.saturating_sub(1);
    }

    fn scroll_page_down(&mut self, visible_rows: usize) {
        let max = self.total.saturating_sub(visible_rows);
        self.offset = (self.offset + visible_rows).min(max);
    }

    fn scroll_page_up(&mut self, visible_rows: usize) {
        self.offset = self.offset.saturating_sub(visible_rows);
    }

    fn update_total(&mut self, total: usize, visible_rows: usize) {
        self.total = total;
        let max = total.saturating_sub(visible_rows);
        if self.offset > max {
            self.offset = max;
        }
    }
}

/// Whether the Recent section has keyboard focus for scrolling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Focus {
    #[default]
    None,
    Recent,
}

/// Turn-level activity summary for "what's happening now" display.
#[derive(Debug, Clone, Default)]
struct TurnActivity {
    active: bool,
    thinking: bool,
    tool_count: u32,
    tool_names: Vec<String>,
    last_phase: String,
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

    // ── Recent activities ─────────────────────────────────────────
    /// All timeline entries as labeled strings (newest first).
    recent_labels: Vec<String>,
    /// Full entries for scrollable detail.
    recent_entries: Vec<TimelineEntry>,
    activity_scroll: ActivityScroll,
    focus: Focus,
    /// Highlighted entry index in the Recent list (None = no focus).
    recent_cursor: Option<usize>,
    /// Current turn activity summary.
    turn_activity: TurnActivity,
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

        // ── Recent activities — all entries, newest first ─────────
        self.recent_entries = app.timeline_clone_vec();
        self.recent_entries.reverse();
        self.recent_labels = self.recent_entries.iter().map(activity_label).collect();

        // ── Turn activity snapshot ──
        self.turn_activity = TurnActivity::default();
        self.turn_activity.active = app.turn_active;
        for entry in &self.recent_entries {
            match entry {
                TimelineEntry::Thinking { complete, .. } => {
                    if !complete {
                        self.turn_activity.thinking = true;
                    }
                }
                TimelineEntry::ToolCall { name, done, .. } => {
                    if !*done {
                        self.turn_activity.tool_count += 1;
                        if self.turn_activity.tool_names.len() < 5 {
                            self.turn_activity.tool_names.push(name.clone());
                        }
                    } else {
                        self.turn_activity.tool_count += 1;
                    }
                }
                _ => {}
            }
        }
        if !self.recent_entries.is_empty() {
            self.turn_activity.last_phase = match &self.recent_entries[0] {
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

    /// Compute approximate visible rows for the Recent section.
    fn recent_visible_rows(&self, area: Rect) -> usize {
        // Rough: header rows + empty lines take ~3 rows; the rest for Recent.
        let ctx_rows = 10usize; // turn activity + token bar + context header
        let title_rows = 3usize; // "Recent" header + separator
        let available = area
            .height
            .saturating_sub(ctx_rows as u16)
            .saturating_sub(title_rows as u16) as usize;
        available.max(1)
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

        // ── Turn activity status (what's happening now) ──
        if self.turn_activity.active {
            let mut status_parts: Vec<Span> = Vec::new();
            status_parts.push(Span::styled(
                "● ",
                Style::default().fg(Color::Yellow).bold(),
            ));
            if self.turn_activity.thinking {
                status_parts.push(Span::styled(
                    "Thinking ",
                    Style::default().fg(Color::Cyan).bold(),
                ));
            }
            if self.turn_activity.tool_count > 0 {
                status_parts.push(Span::styled(
                    format!("Tools:{} ", self.turn_activity.tool_count),
                    Style::default().fg(Color::Yellow),
                ));
                let names: String = self
                    .turn_activity
                    .tool_names
                    .iter()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");
                if !names.is_empty() {
                    status_parts.push(Span::styled(
                        format!("[{}]", names),
                        Style::default().fg(Color::DarkGray),
                    ));
                }
            }
            status_parts.push(Span::styled(
                format!(" {}", self.turn_activity.last_phase),
                Style::default().fg(Color::White),
            ));
            lines.push(Line::from(status_parts));
            lines.push(Line::raw(""));
        }

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
            Span::styled(
                short_id(&self.session_id),
                Style::default().fg(Color::White),
            ),
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
                format!("  sel:{} omi:{}", self.selected_count, self.omitted_count),
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
                format!("{}", self.runtime_policy_agent),
                Style::default().fg(if self.runtime_policy_agent == "Off" {
                    Color::DarkGray
                } else {
                    Color::Cyan
                }),
            ),
            Span::styled(
                format!(
                    "  kernel {} provider {}",
                    self.control_plane_status, self.provider_status
                ),
                Style::default().fg(match self.control_plane_status.as_str() {
                    "healthy" => Color::Green,
                    "degraded" => Color::Red,
                    _ => Color::Yellow,
                }),
            ),
        ]));

        // ── Recent activities (bottom section, scrollable) ────────
        let _recent_header_y = lines.len();

        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            format!(
                "Recent ({})  ↑↓/j/k:scroll  Ctrl+U/D:page",
                self.recent_labels.len()
            ),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));

        if self.recent_labels.is_empty() {
            lines.push(Line::from(Span::styled(
                "No activity yet",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            // Determine visible range with scrolling
            let visible_rows = self.recent_visible_rows(area);
            self.activity_scroll
                .update_total(self.recent_labels.len(), visible_rows);

            let start = self.activity_scroll.offset;
            let end = (start + visible_rows).min(self.recent_labels.len());

            for (i, label) in self
                .recent_labels
                .iter()
                .enumerate()
                .skip(start)
                .take(end - start)
            {
                // Show scroll indicator if not at top
                let prefix = if start > 0 && i == start {
                    "↑"
                } else if end < self.recent_labels.len()
                    && i == end - 1
                    && self.recent_labels.len() > visible_rows
                {
                    "↓"
                } else {
                    " "
                };

                let is_cursor = self.recent_cursor == Some(i);
                let cursor_mark = if is_cursor { "▶ " } else { "  " };

                lines.push(Line::from(vec![
                    Span::styled(prefix, Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        cursor_mark,
                        Style::default().fg(if is_cursor {
                            Color::Cyan
                        } else {
                            Color::DarkGray
                        }),
                    ),
                    Span::styled(
                        label.clone(),
                        if is_cursor {
                            Style::default()
                                .fg(Color::White)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::White)
                        },
                    ),
                ]));
            }

            // Show indicator if entries are scrolled
            if self.recent_labels.len() > visible_rows {
                let progress = if start == 0 {
                    "Top".to_string()
                } else if end >= self.recent_labels.len() {
                    "End".to_string()
                } else {
                    format!("{}/{}", start + 1, self.recent_labels.len())
                };
                lines.push(Line::from(Span::styled(
                    format!("  ↕ {}", progress),
                    Style::default().fg(Color::DarkGray),
                )));
            }
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(format!(
                " Run {}",
                match self.focus {
                    Focus::Recent => "[Recent ⇅]",
                    Focus::None => "",
                }
            ));
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(block)
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &crossterm::event::Event) -> EventResult {
        let visible_rows = 10usize; // will be refined during render
        match event {
            crossterm::event::Event::Key(key)
                if key.kind == crossterm::event::KeyEventKind::Press =>
            {
                match key.code {
                    crossterm::event::KeyCode::Char('j') | crossterm::event::KeyCode::Down => {
                        self.focus = Focus::Recent;
                        // Move cursor or scroll
                        if let Some(cursor) = self.recent_cursor {
                            let next = (cursor + 1).min(self.recent_labels.len().saturating_sub(1));
                            self.recent_cursor = Some(next);
                            // Auto-scroll to keep cursor visible
                            if next >= self.activity_scroll.offset + visible_rows {
                                self.activity_scroll.scroll_down(visible_rows);
                            }
                        } else if !self.recent_labels.is_empty() {
                            self.recent_cursor = Some(0);
                        } else {
                            self.activity_scroll.scroll_down(visible_rows);
                        }
                        EventResult::Consumed
                    }
                    crossterm::event::KeyCode::Char('k') | crossterm::event::KeyCode::Up => {
                        self.focus = Focus::Recent;
                        if let Some(cursor) = self.recent_cursor {
                            let prev = cursor.saturating_sub(1);
                            self.recent_cursor = Some(prev);
                            if prev < self.activity_scroll.offset {
                                self.activity_scroll.scroll_up();
                            }
                        } else if !self.recent_labels.is_empty() {
                            self.recent_cursor = Some(0);
                        } else {
                            self.activity_scroll.scroll_up();
                        }
                        EventResult::Consumed
                    }
                    crossterm::event::KeyCode::Char('d')
                        if key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                    {
                        self.focus = Focus::Recent;
                        self.activity_scroll.scroll_page_down(visible_rows);
                        EventResult::Consumed
                    }
                    crossterm::event::KeyCode::Char('u')
                        if key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                    {
                        self.focus = Focus::Recent;
                        self.activity_scroll.scroll_page_up(visible_rows);
                        EventResult::Consumed
                    }
                    crossterm::event::KeyCode::Enter => {
                        self.focus = Focus::Recent;
                        // Toggle Recent focus
                        if self.recent_cursor.is_some() {
                            self.recent_cursor = None;
                        } else if !self.recent_labels.is_empty() {
                            self.recent_cursor = Some(
                                self.activity_scroll
                                    .offset
                                    .min(self.recent_labels.len().saturating_sub(1)),
                            );
                        }
                        EventResult::Consumed
                    }
                    crossterm::event::KeyCode::Esc => {
                        self.recent_cursor = None;
                        self.focus = Focus::None;
                        EventResult::Consumed
                    }
                    _ => EventResult::NotConsumed,
                }
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
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

fn activity_label(entry: &TimelineEntry) -> String {
    match entry {
        TimelineEntry::Message { role, content, .. } => {
            format!("{}: {}", role, preview(content, 72))
        }
        TimelineEntry::Thinking {
            content, complete, ..
        } => {
            format!(
                "{}thinking: {}",
                if *complete { "" } else { "⚡" },
                preview(content, 72)
            )
        }
        TimelineEntry::ToolCall {
            name,
            preview: tool_preview,
            done,
            exit_code,
            output,
            ..
        } => {
            let status = exit_code
                .map(|code| format!("exit {}", code))
                .unwrap_or_else(|| {
                    if *done {
                        "done".to_string()
                    } else {
                        "running".to_string()
                    }
                });
            let out_hint = if output.is_empty() {
                String::new()
            } else {
                format!(" → {}", preview(output, 40))
            };
            format!(
                "tool {} {}: {}{}",
                name,
                status,
                preview(tool_preview, 48),
                out_hint
            )
        }
        TimelineEntry::SlashOutput {
            command, output, ..
        } => {
            format!("/{}: {}", command, preview(output, 72))
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

        // Verify runtime activity
        assert!(rendered.contains("YoloGoal"));
        assert!(rendered.contains("Parallel"));
        assert!(rendered.contains("tool bash exit 0"));
        assert!(rendered.contains("user: ship the runtime console"));

        // Verify scroll indicator exists
        assert!(rendered.contains("Recent"));

        runtime::init_global_providers(runtime::ProvidersConfig::default());
    }

    #[test]
    fn activity_scroll_bounds() {
        let mut scroll = ActivityScroll::default();
        scroll.update_total(10, 5);
        assert_eq!(scroll.offset, 0);

        scroll.scroll_down(5);
        assert_eq!(scroll.offset, 1);

        scroll.scroll_page_down(5);
        assert_eq!(scroll.offset, 5); // max = 10 - 5 = 5

        scroll.scroll_down(5); // at max, should stay
        assert_eq!(scroll.offset, 5);

        scroll.scroll_up();
        assert_eq!(scroll.offset, 4);
    }
}
