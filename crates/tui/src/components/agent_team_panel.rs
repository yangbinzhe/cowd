// ── Agent Team Panel ─────────────────────────────────────────────
// Displays delegated sub-agent projections from App state.
//
// Shows:
//   - Agent ID and role with emoji indicators
//   - Live status icons (● Active / ◉ Busy / ○ Idle / ✕ Offline)
//   - Capability tags
//   - Composite reputation scores (when available)
//   - Keyboard navigation (j/k/↑/↓, Enter detail, Tab toggle)
//
// Data source: `App::delegate_tasks` and runtime execution graph summaries.
// Reputation scores are read from each AgentInfo's optional field and
// formatted via `ReputationScore::composite()`.

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::app::{App, DelegateTask};
use crate::components::{Component, EventResult, RenderContext};

#[derive(Debug, Clone)]
pub struct AgentInfo {
    pub agent_id: String,
    pub role: String,
    pub capabilities: Vec<String>,
    pub status: AgentStatus,
    pub registered_at_ms: u64,
    pub last_heartbeat_ms: u64,
    pub reputation: Option<ReputationScore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentStatus {
    Active,
    Busy,
    Idle,
    Offline,
}

#[derive(Debug, Clone)]
pub struct ReputationScore {
    pub composite: f64,
}

impl ReputationScore {
    fn composite(&self) -> f64 {
        self.composite
    }
}

// ── AgentTeamPanel ─────────────────────────────────────────────────

/// Panel displaying the live agent team roster from App projections.
///
/// Features:
/// - Real-time roster from the current App projection
/// - Status icons: ● Active, ◉ Busy, ○ Idle, ✕ Offline
/// - Role emoji: 📋 Planner, 🔧 Executor, 🔍 Reviewer, 🤖 Other
/// - Capability tag display
/// - Composite reputation score display
/// - Keyboard navigation: j/k/↑/↓, Enter select, Tab toggle
/// - Scroll offset for long lists
pub struct AgentTeamPanel {
    /// Current agent roster snapshot.
    pub agents: Vec<AgentInfo>,
    /// Currently selected agent index (None when roster is empty).
    pub selected_idx: usize,
    /// Whether the panel is visible (Tab toggles).
    pub visible: bool,
    /// Scroll offset for long lists.
    pub scroll_offset: u16,
    /// Index of the agent whose detail view is expanded (None = collapsed).
    pub detail_idx: Option<usize>,
    /// Latest execution graph summary emitted by the runtime.
    pub execution_graph_summary: Option<crate::RuntimeExecutionGraphSummary>,
    /// Delegated task summaries from the current App state.
    pub delegate_tasks: Vec<DelegateTask>,
    /// Last operator action status.
    pub last_action_status: Option<String>,
    /// Last Gateway receipt summary.
    pub last_action_receipt: Option<String>,
}

impl AgentTeamPanel {
    /// Create a new AgentTeamPanel in hidden state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            agents: Vec::new(),
            selected_idx: 0,
            visible: false,
            scroll_offset: 0,
            detail_idx: None,
            execution_graph_summary: None,
            delegate_tasks: Vec::new(),
            last_action_status: None,
            last_action_receipt: None,
        }
    }

    /// Sync agent roster from the latest App projection.
    pub fn sync(&mut self) {
        if self.agents.is_empty() {
            self.selected_idx = 0;
        } else if self.selected_idx >= self.agents.len() {
            self.selected_idx = self.agents.len().saturating_sub(1);
        }
    }

    /// Sync from App state.
    pub fn sync_from_app(&mut self, app: &App) {
        let prev_len = self.agents.len();
        self.agents = app
            .delegate_tasks
            .iter()
            .map(delegate_task_to_agent)
            .collect();
        if self.agents.len() != prev_len {
            self.detail_idx = None;
        }
        self.sync();
        self.execution_graph_summary = app.latest_execution_graph_summary.clone();
        self.delegate_tasks = app.delegate_tasks.clone();
    }

    /// Toggle panel visibility.
    pub fn toggle(&mut self) {
        self.visible = !self.visible;
        if !self.visible {
            self.detail_idx = None;
        }
    }

    /// Handle navigation key events directly.
    /// Returns `true` if the key was consumed.
    ///
    /// Used by `state.rs` for j/k/Up/Down focus trap routing.
    pub fn handle_key(&mut self, key: &crossterm::event::KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                if self.selected_idx + 1 < self.agents.len() {
                    self.selected_idx += 1;
                }
                let max_visible = 10usize;
                if self.selected_idx >= self.scroll_offset as usize + max_visible {
                    self.scroll_offset = (self.selected_idx.saturating_sub(max_visible - 1)) as u16;
                }
                true
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.selected_idx = self.selected_idx.saturating_sub(1);
                if self.selected_idx < self.scroll_offset as usize {
                    self.scroll_offset = self.selected_idx as u16;
                }
                true
            }
            _ => false,
        }
    }

    /// Return a reference to the currently selected AgentInfo, if any.
    #[must_use]
    pub fn selected_agent(&self) -> Option<&AgentInfo> {
        if self.agents.is_empty() || self.selected_idx >= self.agents.len() {
            None
        } else {
            Some(&self.agents[self.selected_idx])
        }
    }

    #[must_use]
    pub fn selected_agent_id_owned(&self) -> Option<String> {
        self.selected_agent().map(|agent| agent.agent_id.clone())
    }

    pub fn record_action_result(&mut self, label: &str, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => {
                self.last_action_status = Some(format!("{label} succeeded"));
                self.last_action_receipt = Some(agent_receipt_summary(&payload));
            }
            Err(error) => {
                self.last_action_status = Some(format!("{label} failed: {error}"));
                self.last_action_receipt = None;
            }
        }
    }

    /// Toggle the detail view for the selected agent.
    fn toggle_detail(&mut self) {
        if self.agents.is_empty() {
            self.detail_idx = None;
            return;
        }
        if self.detail_idx == Some(self.selected_idx) {
            self.detail_idx = None;
        } else {
            self.detail_idx = Some(self.selected_idx);
        }
    }

    // ── Rendering helpers ────────────────────────────────────────

    /// Map an AgentStatus to its display icon character.
    fn status_icon(status: AgentStatus) -> &'static str {
        match status {
            AgentStatus::Active => "●",
            AgentStatus::Busy => "◉",
            AgentStatus::Idle => "○",
            AgentStatus::Offline => "✕",
        }
    }

    /// Map an AgentStatus to its display colour.
    fn status_color(status: AgentStatus) -> Color {
        match status {
            AgentStatus::Active => Color::Green,
            AgentStatus::Busy => Color::Yellow,
            AgentStatus::Idle => Color::DarkGray,
            AgentStatus::Offline => Color::Red,
        }
    }

    /// Map an agent's role string to a representative emoji.
    fn role_emoji(role: &str) -> &'static str {
        let lower = role.to_lowercase();
        if lower.contains("planner") || lower.contains("plan") {
            "📋"
        } else if lower.contains("executor") || lower.contains("execute") || lower.contains("build")
        {
            "🔧"
        } else if lower.contains("reviewer") || lower.contains("review") || lower.contains("audit")
        {
            "🔍"
        } else {
            "🤖"
        }
    }

    /// Build the title string for the bordered block.
    fn build_title(&self) -> String {
        let base = if self.visible {
            " Agent Team "
        } else {
            " Agent Team (hidden) "
        };
        if !self.agents.is_empty() {
            format!("{base}[{}]", self.agents.len())
        } else {
            base.to_string()
        }
    }

    /// Format a reputation composite score as a human-readable string.
    fn format_reputation(score: f64) -> String {
        if score >= 8.0 {
            format!("★★★ {score:.1}")
        } else if score >= 5.0 {
            format!("★★☆ {score:.1}")
        } else if score >= 2.0 {
            format!("★☆☆ {score:.1}")
        } else {
            format!("☆☆☆ {score:.1}")
        }
    }

    /// Render a compact capability tag.
    fn render_capability_tag(cap: &str) -> Span<'static> {
        Span::styled(
            format!("[{cap}]"),
            Style::default().fg(Color::Cyan).add_modifier(Modifier::DIM),
        )
    }

    /// Render the detail view for an agent (expanded below its line).
    fn render_detail_lines(&self, agent: &AgentInfo) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();

        // Divider
        lines.push(Line::from(Span::styled(
            "   ── Details ──",
            Style::default().fg(Color::DarkGray),
        )));

        // Agent ID
        let agent_id = agent.agent_id.clone();
        lines.push(Line::from(vec![
            Span::styled("   ID:       ", Style::default().fg(Color::DarkGray)),
            Span::styled(agent_id, Style::default().fg(Color::White)),
        ]));

        // Role
        let emoji = Self::role_emoji(&agent.role);
        let role_text = format!("{emoji} {}", agent.role);
        lines.push(Line::from(vec![
            Span::styled("   Role:     ", Style::default().fg(Color::DarkGray)),
            Span::styled(role_text, Style::default().fg(Color::White)),
        ]));

        // Status
        let icon = Self::status_icon(agent.status);
        let status_label = format!("{:?}", agent.status);
        lines.push(Line::from(vec![
            Span::styled("   Status:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{icon} {status_label}"),
                Style::default().fg(Self::status_color(agent.status)),
            ),
        ]));

        // Capabilities
        if !agent.capabilities.is_empty() {
            let mut cap_spans: Vec<Span<'static>> = vec![Span::styled(
                "   Caps:     ",
                Style::default().fg(Color::DarkGray),
            )];
            for (i, cap) in agent.capabilities.iter().enumerate() {
                if i > 0 {
                    cap_spans.push(Span::raw(" "));
                }
                cap_spans.push(Self::render_capability_tag(cap));
            }
            lines.push(Line::from(cap_spans));
        }

        // Reputation
        if let Some(ref rep) = agent.reputation {
            let composite = rep.composite();
            let rep_display = Self::format_reputation(composite);
            lines.push(Line::from(vec![
                Span::styled("   Rep:      ", Style::default().fg(Color::DarkGray)),
                Span::styled(rep_display, Style::default().fg(Color::Magenta)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("   Tasks:    ", Style::default().fg(Color::DarkGray)),
                Span::styled("projected", Style::default().fg(Color::White)),
            ]));
        } else {
            lines.push(Line::from(vec![
                Span::styled("   Rep:      ", Style::default().fg(Color::DarkGray)),
                Span::styled("— no data —", Style::default().fg(Color::DarkGray)),
            ]));
        }

        lines
    }

    fn render_collaboration_lines(&self) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        if let Some(summary) = self.execution_graph_summary.as_ref() {
            lines.push(Line::from(vec![
                Span::styled("Execution graph: ", Style::default().fg(Color::DarkGray)),
                Span::styled(summary.status.clone(), Style::default().fg(Color::Yellow)),
                Span::styled(
                    format!(
                        "  tasks {} children {} memory {} conflicts {}",
                        summary.agent_tasks,
                        summary.child_executions,
                        summary.memory_candidates,
                        summary.conflicts
                    ),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
            let lift = summary
                .synthesis_lift
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "n/a".to_string());
            let complementarity = summary
                .complementarity_score
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "n/a".to_string());
            let completion = summary
                .completion_rate
                .map(|v| format!("{:.0}%", v * 100.0))
                .unwrap_or_else(|| "n/a".to_string());
            lines.push(Line::from(vec![
                Span::styled("Synthesis: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("lift {lift}  complement {complementarity}  done {completion}"),
                    Style::default().fg(Color::Green),
                ),
            ]));
        } else {
            lines.push(Line::from(Span::styled(
                "Execution graph: no collaboration summary yet",
                Style::default().fg(Color::DarkGray),
            )));
        }

        if !self.delegate_tasks.is_empty() {
            lines.push(Line::from(vec![
                Span::styled("Delegates: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", self.delegate_tasks.len()),
                    Style::default().fg(Color::Cyan),
                ),
            ]));
            for task in self.delegate_tasks.iter().take(3) {
                lines.push(Line::from(vec![
                    Span::styled("  - ", Style::default().fg(Color::DarkGray)),
                    Span::styled(task.status.clone(), Style::default().fg(Color::Yellow)),
                    Span::raw(" "),
                    Span::styled(preview(&task.description, 56), Style::default()),
                ]));
            }
        }
        lines.push(Line::raw(""));
        lines
    }
}

fn delegate_task_to_agent(task: &DelegateTask) -> AgentInfo {
    let status = match task.status.to_ascii_lowercase().as_str() {
        "running" | "active" => AgentStatus::Busy,
        "completed" | "done" => AgentStatus::Idle,
        "failed" | "error" => AgentStatus::Offline,
        _ => AgentStatus::Active,
    };
    AgentInfo {
        agent_id: task.id.clone(),
        role: "Delegate".to_string(),
        capabilities: vec![task.description.clone()],
        status,
        registered_at_ms: 0,
        last_heartbeat_ms: 0,
        reputation: None,
    }
}

// ── Default impl ─────────────────────────────────────────────────

impl Default for AgentTeamPanel {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component Trait ──────────────────────────────────────────────

impl Component for AgentTeamPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let title = self.build_title();
        let block = Block::default().title(title).borders(Borders::ALL);

        let _inner_width = area.width.saturating_sub(2) as usize;
        let inner_height = area.height.saturating_sub(2) as usize;

        if inner_height < 3 {
            let para = Paragraph::new("Too small").block(block);
            ctx.frame_mut().render_widget(para, area);
            return;
        }

        let mut lines: Vec<Line> = Vec::new();

        // ── Header ───────────────────────────────────────────
        lines.extend(self.render_collaboration_lines());
        if self.agents.is_empty() {
            lines.push(Line::from(Span::styled(
                "No agents registered.",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Delegated runtime work appears here when projected by App state.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            let status_label = format!(
                "{} agents | j↓ k↑ select | Enter detail | Tab toggle",
                self.agents.len()
            );
            lines.push(Line::from(Span::styled(
                status_label,
                Style::default().fg(Color::Yellow),
            )));
            lines.push(Line::raw(""));

            let total = self.agents.len();
            let visible_start = self.scroll_offset as usize;
            // Estimate how many lines remain after header
            let header_consumed = 2;
            let max_visible = inner_height.saturating_sub(header_consumed + 1);
            let visible_end = (visible_start + max_visible).min(total);

            for i in visible_start..visible_end {
                let agent = &self.agents[i];
                let is_selected = self.selected_idx == i;
                let is_detail = self.detail_idx == Some(i);

                let cursor = if is_selected { " > " } else { "   " };
                let cursor_style = if is_selected {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                };

                let icon = Self::status_icon(agent.status);
                let icon_color = Self::status_color(agent.status);
                let emoji = Self::role_emoji(&agent.role);

                let name_style = if is_selected {
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                };

                // First line: cursor + icon + emoji + agent_id + role
                lines.push(Line::from(vec![
                    Span::styled(cursor, cursor_style),
                    Span::styled(format!("{icon} "), Style::default().fg(icon_color)),
                    Span::styled(emoji, Style::default()),
                    Span::styled(format!(" {} ", &agent.agent_id), name_style),
                    Span::styled(
                        format!("({})", &agent.role),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));

                // Second line: capability tags (indented)
                if !agent.capabilities.is_empty() {
                    let mut cap_spans: Vec<Span> = vec![Span::styled("      ", Style::default())];
                    for (j, cap) in agent.capabilities.iter().enumerate() {
                        if j > 0 {
                            cap_spans.push(Span::raw(" "));
                        }
                        cap_spans.push(Self::render_capability_tag(cap));
                    }
                    lines.push(Line::from(cap_spans));
                }

                // Reputation score (compact inline)
                if let Some(ref rep) = agent.reputation {
                    let composite = rep.composite();
                    let rep_color = if composite >= 5.0 {
                        Color::Magenta
                    } else {
                        Color::DarkGray
                    };
                    lines.push(Line::from(vec![
                        Span::styled("      rep: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(format!("{composite:.1}/10"), Style::default().fg(rep_color)),
                    ]));
                }

                // Detail view (expanded)
                if is_detail {
                    let detail_lines = self.render_detail_lines(agent);
                    lines.extend(detail_lines);
                }
            }

            if visible_end < total {
                lines.push(Line::styled(
                    format!("... {} more agents (j/k to scroll)", total - visible_end),
                    Style::default().fg(Color::DarkGray),
                ));
            }
        }

        // ── Keyboard hint bar ──────────────────────────────────
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "j↓ k↑  Enter detail  i input  ! interrupt  X shutdown  Tab toggle  Esc hide",
            Style::default().fg(Color::DarkGray),
        )));
        if let Some(status) = &self.last_action_status {
            lines.push(Line::from(vec![
                Span::styled("Status: ", Style::default().fg(Color::DarkGray)),
                Span::styled(status.clone(), Style::default().fg(Color::Yellow)),
            ]));
        }
        if let Some(receipt) = &self.last_action_receipt {
            lines.push(Line::from(vec![
                Span::styled("Receipt: ", Style::default().fg(Color::DarkGray)),
                Span::styled(receipt.clone(), Style::default().fg(Color::Green)),
            ]));
        }

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        if key.kind != KeyEventKind::Press {
            return EventResult::NotConsumed;
        }

        // Global keys regardless of visibility
        match key.code {
            KeyCode::Tab => {
                self.toggle();
                return EventResult::Consumed;
            }
            _ => {}
        }

        if !self.visible || self.agents.is_empty() {
            return EventResult::NotConsumed;
        }

        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                if self.selected_idx + 1 < self.agents.len() {
                    self.selected_idx += 1;
                }
                // Auto-scroll
                let max_visible = 10usize;
                if self.selected_idx >= self.scroll_offset as usize + max_visible {
                    self.scroll_offset = (self.selected_idx.saturating_sub(max_visible - 1)) as u16;
                }
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.selected_idx = self.selected_idx.saturating_sub(1);
                if self.selected_idx < self.scroll_offset as usize {
                    self.scroll_offset = self.selected_idx as u16;
                }
                EventResult::Consumed
            }
            KeyCode::Char('g') => {
                self.selected_idx = 0;
                self.scroll_offset = 0;
                EventResult::Consumed
            }
            KeyCode::Char('G') => {
                self.selected_idx = self.agents.len().saturating_sub(1);
                // Scroll to bottom
                if self.selected_idx >= 10 {
                    self.scroll_offset = (self.selected_idx.saturating_sub(9)) as u16;
                }
                EventResult::Consumed
            }
            KeyCode::Enter => {
                self.toggle_detail();
                EventResult::Consumed
            }
            KeyCode::Esc => {
                if self.detail_idx.is_some() {
                    self.detail_idx = None;
                } else {
                    self.visible = false;
                }
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        self.visible
    }

    fn id(&self) -> &str {
        "agent_team_panel"
    }
}

fn preview(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out = text
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    out.push_str("...");
    out
}

fn agent_receipt_summary(receipt: &serde_json::Value) -> String {
    let kind = receipt
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("agent.receipt");
    let status = receipt
        .get("status")
        .or_else(|| receipt.get("ok"))
        .map(serde_json::Value::to_string)
        .unwrap_or_else(|| "recorded".to_string());
    format!("{kind} status={status}")
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;
    use crossterm::event::KeyEvent;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    static AGENT_DIRECTORY_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn agent_directory_test_guard() -> MutexGuard<'static, ()> {
        AGENT_DIRECTORY_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("agent directory test lock poisoned")
    }

    struct AgentDirectory;

    impl AgentDirectory {
        fn new() -> Self {
            Self
        }

        fn clear_all(&self) {
            test_agents().lock().unwrap().clear();
        }

        fn register(&self, agent: AgentInfo) {
            test_agents().lock().unwrap().push(agent);
        }

        fn unregister(&self, agent_id: &str) {
            test_agents()
                .lock()
                .unwrap()
                .retain(|agent| agent.agent_id != agent_id);
        }

        fn list_active(&self) -> Vec<AgentInfo> {
            test_agents().lock().unwrap().clone()
        }
    }

    fn sync_from_test_directory(panel: &mut AgentTeamPanel, dir: &AgentDirectory) {
        let prev_len = panel.agents.len();
        panel.agents = dir.list_active();
        if panel.agents.len() != prev_len {
            panel.detail_idx = None;
        }
        panel.sync();
    }

    fn test_agents() -> &'static Mutex<Vec<AgentInfo>> {
        static AGENTS: OnceLock<Mutex<Vec<AgentInfo>>> = OnceLock::new();
        AGENTS.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn dummy_agent(id: &str, role: &str, status: AgentStatus, caps: Vec<&str>) -> AgentInfo {
        AgentInfo {
            agent_id: id.to_string(),
            role: role.to_string(),
            capabilities: caps.into_iter().map(String::from).collect(),
            status,
            registered_at_ms: 1000,
            last_heartbeat_ms: 2000,
            reputation: None,
        }
    }

    fn render_panel(panel: &mut AgentTeamPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    #[test]
    fn new_panel_starts_hidden() {
        let panel = AgentTeamPanel::new();
        assert!(!panel.visible);
        assert!(panel.agents.is_empty());
        assert_eq!(panel.selected_idx, 0);
    }

    #[test]
    fn toggle_flips_visibility() {
        let mut panel = AgentTeamPanel::new();
        assert!(!panel.visible);
        panel.toggle();
        assert!(panel.visible);
        panel.toggle();
        assert!(!panel.visible);
    }

    #[test]
    fn sync_populates_agents() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent(
            "test-1",
            "Executor",
            AgentStatus::Active,
            vec!["rust"],
        ));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        assert!(!panel.agents.is_empty());
        assert!(panel.agents.iter().any(|a| a.agent_id == "test-1"));

        dir.clear_all();
    }

    #[test]
    fn selected_agent_returns_correct_entry() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent(
            "alpha",
            "Planner",
            AgentStatus::Active,
            vec!["plan"],
        ));
        dir.register(dummy_agent(
            "beta",
            "Executor",
            AgentStatus::Busy,
            vec!["rust"],
        ));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        assert!(
            panel.agents.len() >= 2,
            "Expected at least 2 agents, got {}",
            panel.agents.len()
        );

        // Find "beta" by iterating (HashMap order is non-deterministic)
        let beta_pos = panel.agents.iter().position(|a| a.agent_id == "beta");
        assert!(beta_pos.is_some(), "beta should be registered");
        panel.selected_idx = beta_pos.unwrap();

        let selected = panel.selected_agent();
        assert!(selected.is_some());
        assert_eq!(selected.unwrap().agent_id, "beta");

        dir.clear_all();
    }

    #[test]
    fn selected_agent_returns_none_when_empty() {
        let panel = AgentTeamPanel::new();
        assert!(panel.selected_agent().is_none());
    }

    #[test]
    fn status_icon_mapping() {
        assert_eq!(AgentTeamPanel::status_icon(AgentStatus::Active), "●");
        assert_eq!(AgentTeamPanel::status_icon(AgentStatus::Busy), "◉");
        assert_eq!(AgentTeamPanel::status_icon(AgentStatus::Idle), "○");
        assert_eq!(AgentTeamPanel::status_icon(AgentStatus::Offline), "✕");
    }

    #[test]
    fn status_color_mapping() {
        assert_eq!(
            AgentTeamPanel::status_color(AgentStatus::Active),
            Color::Green
        );
        assert_eq!(
            AgentTeamPanel::status_color(AgentStatus::Busy),
            Color::Yellow
        );
        assert_eq!(
            AgentTeamPanel::status_color(AgentStatus::Idle),
            Color::DarkGray
        );
        assert_eq!(
            AgentTeamPanel::status_color(AgentStatus::Offline),
            Color::Red
        );
    }

    #[test]
    fn role_emoji_categorization() {
        assert_eq!(AgentTeamPanel::role_emoji("Planner"), "📋");
        assert_eq!(AgentTeamPanel::role_emoji("Senior Planner"), "📋");
        assert_eq!(AgentTeamPanel::role_emoji("Executor"), "🔧");
        assert_eq!(AgentTeamPanel::role_emoji("Build Engineer"), "🔧");
        assert_eq!(AgentTeamPanel::role_emoji("Reviewer"), "🔍");
        assert_eq!(AgentTeamPanel::role_emoji("Code Auditor"), "🔍");
        assert_eq!(AgentTeamPanel::role_emoji("UnknownThing"), "🤖");
    }

    #[test]
    fn format_reputation_ranges() {
        assert!(AgentTeamPanel::format_reputation(9.0).contains("★★★"));
        assert!(AgentTeamPanel::format_reputation(6.5).contains("★★☆"));
        assert!(AgentTeamPanel::format_reputation(3.0).contains("★☆☆"));
        assert!(AgentTeamPanel::format_reputation(1.0).contains("☆☆☆"));
    }

    #[test]
    fn render_empty_state() {
        let mut panel = AgentTeamPanel::new();
        let lines = render_panel(&mut panel, 50, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("No agents registered"),
            "Empty panel should show no-agents message, got: {joined}"
        );
    }

    #[test]
    fn render_with_agents() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent(
            "agent-1",
            "Executor",
            AgentStatus::Active,
            vec!["rust", "tui"],
        ));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        panel.visible = true;

        let lines = render_panel(&mut panel, 60, 15);
        let joined = lines.join("\n");
        assert!(
            joined.contains("agent-1"),
            "Should show agent ID, got: {joined}"
        );
        assert!(
            joined.contains("Executor"),
            "Should show role, got: {joined}"
        );
        assert!(
            joined.contains("●"),
            "Should show Active status icon, got: {joined}"
        );

        dir.clear_all();
    }

    #[test]
    fn keyboard_navigation_jk() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent("a", "Planner", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("b", "Executor", AgentStatus::Busy, vec![]));
        dir.register(dummy_agent("c", "Reviewer", AgentStatus::Idle, vec![]));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        panel.visible = true;
        let agent_count = panel.agents.len();
        assert!(agent_count >= 3, "Expected at least 3 agents");

        assert_eq!(panel.selected_idx, 0);

        let press_j = Event::Key(KeyEvent::new(
            KeyCode::Char('j'),
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_j);
        assert_eq!(panel.selected_idx, 1);

        let press_down = Event::Key(KeyEvent::new(
            KeyCode::Down,
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_down);
        assert_eq!(panel.selected_idx, 2);

        // Should not overflow past agent_count-1
        panel.handle_event(&press_j);
        assert!(
            panel.selected_idx < agent_count,
            "selected_idx should not exceed agent_count-1"
        );

        let press_k = Event::Key(KeyEvent::new(
            KeyCode::Char('k'),
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_k);
        assert_eq!(panel.selected_idx, 1);

        let press_up = Event::Key(KeyEvent::new(
            KeyCode::Up,
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_up);
        assert_eq!(panel.selected_idx, 0);

        // Should not underflow
        panel.handle_event(&press_up);
        assert_eq!(panel.selected_idx, 0);

        dir.clear_all();
    }

    #[test]
    fn keyboard_gg_jumps() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent("a", "P", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("b", "E", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("c", "R", AgentStatus::Active, vec![]));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        panel.visible = true;

        // Jump to bottom
        let press_g = Event::Key(KeyEvent::new(
            KeyCode::Char('G'),
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_g);
        assert_eq!(panel.selected_idx, 2);

        // Jump to top
        let press_gg = Event::Key(KeyEvent::new(
            KeyCode::Char('g'),
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_gg);
        assert_eq!(panel.selected_idx, 0);

        dir.clear_all();
    }

    #[test]
    fn enter_toggles_detail() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent(
            "det",
            "Executor",
            AgentStatus::Active,
            vec!["rust"],
        ));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        panel.visible = true;
        assert!(panel.detail_idx.is_none());

        let press_enter = Event::Key(KeyEvent::new(
            KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_enter);
        assert_eq!(panel.detail_idx, Some(0));

        // Second Enter collapses
        panel.handle_event(&press_enter);
        assert!(panel.detail_idx.is_none());

        dir.unregister("det");
    }

    #[test]
    fn esc_collapses_detail_then_hides() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent("esc", "Executor", AgentStatus::Active, vec![]));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        panel.visible = true;
        panel.detail_idx = Some(0);

        let press_esc = Event::Key(KeyEvent::new(
            KeyCode::Esc,
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_esc);
        assert!(
            panel.detail_idx.is_none(),
            "First Esc should collapse detail"
        );
        assert!(
            panel.visible,
            "Panel should still be visible after collapsing detail"
        );

        panel.handle_event(&press_esc);
        assert!(!panel.visible, "Second Esc should hide panel");

        dir.clear_all();
    }

    #[test]
    fn tab_toggles_visibility() {
        let mut panel = AgentTeamPanel::new();

        let press_tab = Event::Key(KeyEvent::new(
            KeyCode::Tab,
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_tab);
        assert!(panel.visible);

        panel.handle_event(&press_tab);
        assert!(!panel.visible);
    }

    #[test]
    fn component_trait_methods() {
        let panel = AgentTeamPanel::new();
        assert!(!panel.focusable()); // not focusable when hidden
        assert_eq!(panel.id(), "agent_team_panel");
    }

    #[test]
    fn component_focusable_when_visible() {
        let mut panel = AgentTeamPanel::new();
        panel.visible = true;
        assert!(panel.focusable());
    }

    #[test]
    fn sync_from_app_delegates_to_sync() {
        let mut app = App::new("m", "s");
        app.delegate_tasks = vec![DelegateTask {
            id: "via-app".to_string(),
            description: "plan delegated work".to_string(),
            status: "running".to_string(),
        }];
        let mut panel = AgentTeamPanel::new();
        panel.sync_from_app(&app);
        assert!(
            panel.agents.iter().any(|a| a.agent_id == "via-app"),
            "Agent 'via-app' should be present after sync_from_app"
        );
    }

    #[test]
    fn sync_from_app_renders_execution_graph_and_delegate_tasks() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent(
            "planner-1",
            "Planner",
            AgentStatus::Active,
            vec!["planning"],
        ));

        let mut app = App::new("m", "s");
        app.latest_execution_graph_summary = Some(crate::RuntimeExecutionGraphSummary {
            graph_id: Some("graph-1".to_string()),
            board_id: Some("board-1".to_string()),
            status: "running".to_string(),
            agent_tasks: 3,
            child_executions: 0,
            memory_candidates: 2,
            conflicts: 1,
            completion_rate: Some(0.5),
            synthesis_lift: Some(1.25),
            complementarity_score: Some(0.82),
        });
        app.delegate_tasks = vec![DelegateTask {
            id: "delegate-1".to_string(),
            description: "review context-memory integration".to_string(),
            status: "running".to_string(),
        }];

        let mut panel = AgentTeamPanel::new();
        panel.visible = true;
        panel.sync_from_app(&app);

        let lines = render_panel(&mut panel, 92, 20);
        let joined = lines.join("\n");
        assert!(joined.contains("Execution graph:"), "{joined}");
        assert!(
            joined.contains("tasks 3 children 0 memory 2 conflicts 1"),
            "{joined}"
        );
        assert!(joined.contains("lift 1.25"), "{joined}");
        assert!(joined.contains("Delegates:"), "{joined}");
        assert!(
            joined.contains("review context-memory integration"),
            "{joined}"
        );

        dir.clear_all();
    }

    #[test]
    fn sync_resets_detail_when_roster_changes() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent("x", "E", AgentStatus::Active, vec![]));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        panel.detail_idx = Some(0);

        dir.register(dummy_agent("y", "P", AgentStatus::Active, vec![]));
        sync_from_test_directory(&mut panel, &dir);
        // Roster length changed, detail should reset
        assert!(panel.detail_idx.is_none());

        dir.clear_all();
    }

    #[test]
    fn detail_toggle_on_empty_roster_is_noop() {
        let mut panel = AgentTeamPanel::new();
        assert!(panel.agents.is_empty());
        // Call toggle_detail via Enter event
        let press_enter = Event::Key(KeyEvent::new(
            KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        ));
        panel.handle_event(&press_enter);
        assert!(panel.detail_idx.is_none());
    }

    #[test]
    fn selection_clamped_after_roster_shrinks() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        dir.register(dummy_agent("a", "P", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("b", "E", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("c", "R", AgentStatus::Active, vec![]));

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        panel.selected_idx = 2; // last entry

        dir.unregister("c");
        sync_from_test_directory(&mut panel, &dir);
        // selected_idx should be clamped to 1 (the new last index)
        assert_eq!(panel.selected_idx, 1);

        dir.clear_all();
    }

    #[test]
    fn scroll_offset_tracks_selection_on_j() {
        let _guard = agent_directory_test_guard();
        let dir = AgentDirectory::new();
        dir.clear_all();
        for i in 0..15 {
            dir.register(dummy_agent(
                &format!("agent-{i}"),
                "E",
                AgentStatus::Active,
                vec![],
            ));
        }

        let mut panel = AgentTeamPanel::new();
        sync_from_test_directory(&mut panel, &dir);
        panel.visible = true;
        panel.selected_idx = 0;
        panel.scroll_offset = 0;

        let agent_count = panel.agents.len();
        assert!(
            agent_count >= 10,
            "Expected at least 10 agents, got {agent_count}"
        );

        let press_j = Event::Key(KeyEvent::new(
            KeyCode::Char('j'),
            crossterm::event::KeyModifiers::NONE,
        ));
        for _ in 0..12 {
            panel.handle_event(&press_j);
        }
        // selected_idx should be at the clamped maximum
        assert!(panel.selected_idx > 0, "selection should advance");
        assert!(
            panel.selected_idx < agent_count,
            "selection should be clamped"
        );
        if agent_count >= 10 {
            assert!(
                panel.scroll_offset > 0,
                "scroll should track selection for long lists"
            );
        }

        dir.clear_all();
    }
}
