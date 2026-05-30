// ── Agent Team Panel ─────────────────────────────────────────────
// Displays registered sub-agents from the global AgentDirectory.
//
// Shows:
//   - Agent ID and role with emoji indicators
//   - Live status icons (● Active / ◉ Busy / ○ Idle / ✕ Offline)
//   - Capability tags
//   - Composite reputation scores (when available)
//   - Keyboard navigation (j/k/↑/↓, Enter detail, Tab toggle)
//
// Data source: `AgentDirectory::global().list_active()` provides
// a snapshot of all non-offline agents from the shared global registry.
// Reputation scores are read from each AgentInfo's optional
// `reputation` field and formatted via `ReputationScore::composite()`.

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use memory::{AgentDirectory, AgentInfo, AgentStatus};

use crate::tui::app::App;
use crate::tui::components::{Component, EventResult, RenderContext};

// ── AgentTeamPanel ─────────────────────────────────────────────────

/// Panel displaying the live agent team roster from AgentDirectory.
///
/// Features:
/// - Real-time roster from `AgentDirectory::global().list_active()`
/// - Status icons: ● Active, ◉ Busy, ○ Idle, ✕ Offline
/// - Role emoji: 📋 Planner, 🔧 Executor, 🔍 Reviewer, 🤖 Other
/// - Capability tag display
/// - Composite reputation score display
/// - Keyboard navigation: j/k/↑/↓, Enter select, Tab toggle
/// - Scroll offset for long lists
pub struct AgentTeamPanel {
    /// Current agent roster snapshot (from AgentDirectory).
    pub agents: Vec<AgentInfo>,
    /// Currently selected agent index (None when roster is empty).
    pub selected_idx: usize,
    /// Whether the panel is visible (Tab toggles).
    pub visible: bool,
    /// Scroll offset for long lists.
    pub scroll_offset: u16,
    /// Index of the agent whose detail view is expanded (None = collapsed).
    pub detail_idx: Option<usize>,
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
        }
    }

    /// Sync agent roster from the global AgentDirectory singleton.
    ///
    /// Calls `AgentDirectory::global().list_active()` to obtain a fresh
    /// snapshot of all non-offline agents. Resets selection and detail
    /// state if the roster has changed.
    pub fn sync(&mut self) {
        let prev_len = self.agents.len();
        self.agents = AgentDirectory::global().list_active();
        if self.agents.len() != prev_len {
            self.detail_idx = None;
        }
        if self.agents.is_empty() {
            self.selected_idx = 0;
        } else if self.selected_idx >= self.agents.len() {
            self.selected_idx = self.agents.len().saturating_sub(1);
        }
    }

    /// Sync from App state. Delegates to `self.sync()` since the
    /// AgentDirectory is a global singleton, not stored on App.
    pub fn sync_from_app(&mut self, _app: &App) {
        self.sync();
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
                    self.scroll_offset =
                        (self.selected_idx.saturating_sub(max_visible - 1)) as u16;
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
        } else if lower.contains("executor") || lower.contains("execute") || lower.contains("build") {
            "🔧"
        } else if lower.contains("reviewer") || lower.contains("review") || lower.contains("audit") {
            "🔍"
        } else {
            "🤖"
        }
    }

    /// Build the title string for the bordered block.
    fn build_title(&self) -> String {
        let base = if self.visible { " Agent Team " } else { " Agent Team (hidden) " };
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
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::DIM),
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
            let mut cap_spans: Vec<Span<'static>> = vec![
                Span::styled("   Caps:     ", Style::default().fg(Color::DarkGray)),
            ];
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
                Span::styled(
                    format!("{}", rep.task_count),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("  SR: {:.1}%", rep.success_rate * 100.0),
                    Style::default().fg(Color::Gray),
                ),
                Span::styled(
                    format!("  Peer: {:.1}/5.0", rep.peer_rating),
                    Style::default().fg(Color::Gray),
                ),
                Span::styled(
                    if rep.recent_failures > 0 {
                        format!("  ⚠{} recent fails", rep.recent_failures)
                    } else {
                        String::new()
                    },
                    Style::default().fg(Color::Red),
                ),
            ]));
        } else {
            lines.push(Line::from(vec![
                Span::styled("   Rep:      ", Style::default().fg(Color::DarkGray)),
                Span::styled("— no data —", Style::default().fg(Color::DarkGray)),
            ]));
        }

        lines
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
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL);

        let _inner_width = area.width.saturating_sub(2) as usize;
        let inner_height = area.height.saturating_sub(2) as usize;

        if inner_height < 3 {
            let para = Paragraph::new("Too small").block(block);
            ctx.frame_mut().render_widget(para, area);
            return;
        }

        let mut lines: Vec<Line> = Vec::new();

        // ── Header ───────────────────────────────────────────
        if self.agents.is_empty() {
            lines.push(Line::from(Span::styled(
                "No agents registered.",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Agents register via AgentDirectory when spawned by the runtime.",
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
                    Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
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
                    let mut cap_spans: Vec<Span> = vec![
                        Span::styled("      ", Style::default()),
                    ];
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
                    let rep_color = if composite >= 5.0 { Color::Magenta } else { Color::DarkGray };
                    lines.push(Line::from(vec![
                        Span::styled("      rep: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            format!("{composite:.1}/10  tasks:{}", rep.task_count),
                            Style::default().fg(rep_color),
                        ),
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
            "j↓ k↑  Enter detail  Tab toggle  g top  G bottom  Esc hide",
            Style::default().fg(Color::DarkGray),
        )));

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
                    self.scroll_offset =
                        (self.selected_idx.saturating_sub(max_visible - 1)) as u16;
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

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyEvent;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

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
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("test-1", "Executor", AgentStatus::Active, vec!["rust"]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
        assert!(!panel.agents.is_empty());
        assert!(panel.agents.iter().any(|a| a.agent_id == "test-1"));

        dir.clear_all();
    }

    #[test]
    fn selected_agent_returns_correct_entry() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("alpha", "Planner", AgentStatus::Active, vec!["plan"]));
        dir.register(dummy_agent("beta", "Executor", AgentStatus::Busy, vec!["rust"]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
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
        assert_eq!(AgentTeamPanel::status_color(AgentStatus::Active), Color::Green);
        assert_eq!(AgentTeamPanel::status_color(AgentStatus::Busy), Color::Yellow);
        assert_eq!(AgentTeamPanel::status_color(AgentStatus::Idle), Color::DarkGray);
        assert_eq!(AgentTeamPanel::status_color(AgentStatus::Offline), Color::Red);
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
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("agent-1", "Executor", AgentStatus::Active, vec!["rust", "tui"]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
        panel.visible = true;

        let lines = render_panel(&mut panel, 60, 15);
        let joined = lines.join("\n");
        assert!(joined.contains("agent-1"), "Should show agent ID, got: {joined}");
        assert!(joined.contains("Executor"), "Should show role, got: {joined}");
        assert!(joined.contains("●"), "Should show Active status icon, got: {joined}");

        dir.clear_all();
    }

    #[test]
    fn keyboard_navigation_jk() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("a", "Planner", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("b", "Executor", AgentStatus::Busy, vec![]));
        dir.register(dummy_agent("c", "Reviewer", AgentStatus::Idle, vec![]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
        panel.visible = true;
        let agent_count = panel.agents.len();
        assert!(agent_count >= 3, "Expected at least 3 agents");

        assert_eq!(panel.selected_idx, 0);

        let press_j = Event::Key(KeyEvent::new(KeyCode::Char('j'), crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_j);
        assert_eq!(panel.selected_idx, 1);

        let press_down = Event::Key(KeyEvent::new(KeyCode::Down, crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_down);
        assert_eq!(panel.selected_idx, 2);

        // Should not overflow past agent_count-1
        panel.handle_event(&press_j);
        assert!(panel.selected_idx < agent_count,
            "selected_idx should not exceed agent_count-1");

        let press_k = Event::Key(KeyEvent::new(KeyCode::Char('k'), crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_k);
        assert_eq!(panel.selected_idx, 1);

        let press_up = Event::Key(KeyEvent::new(KeyCode::Up, crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_up);
        assert_eq!(panel.selected_idx, 0);

        // Should not underflow
        panel.handle_event(&press_up);
        assert_eq!(panel.selected_idx, 0);

        dir.clear_all();
    }

    #[test]
    fn keyboard_gg_jumps() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("a", "P", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("b", "E", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("c", "R", AgentStatus::Active, vec![]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
        panel.visible = true;

        // Jump to bottom
        let press_g = Event::Key(KeyEvent::new(KeyCode::Char('G'), crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_g);
        assert_eq!(panel.selected_idx, 2);

        // Jump to top
        let press_gg = Event::Key(KeyEvent::new(KeyCode::Char('g'), crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_gg);
        assert_eq!(panel.selected_idx, 0);

        dir.clear_all();
    }

    #[test]
    fn enter_toggles_detail() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("det", "Executor", AgentStatus::Active, vec!["rust"]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
        panel.visible = true;
        assert!(panel.detail_idx.is_none());

        let press_enter = Event::Key(KeyEvent::new(KeyCode::Enter, crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_enter);
        assert_eq!(panel.detail_idx, Some(0));

        // Second Enter collapses
        panel.handle_event(&press_enter);
        assert!(panel.detail_idx.is_none());

        dir.unregister("det");
    }

    #[test]
    fn esc_collapses_detail_then_hides() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("esc", "Executor", AgentStatus::Active, vec![]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
        panel.visible = true;
        panel.detail_idx = Some(0);

        let press_esc = Event::Key(KeyEvent::new(KeyCode::Esc, crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_esc);
        assert!(panel.detail_idx.is_none(), "First Esc should collapse detail");
        assert!(panel.visible, "Panel should still be visible after collapsing detail");

        panel.handle_event(&press_esc);
        assert!(!panel.visible, "Second Esc should hide panel");

        dir.clear_all();
    }

    #[test]
    fn tab_toggles_visibility() {
        let mut panel = AgentTeamPanel::new();

        let press_tab = Event::Key(KeyEvent::new(KeyCode::Tab, crossterm::event::KeyModifiers::NONE));
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
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("via-app", "Planner", AgentStatus::Active, vec![]));

        let app = App::new("m", "s");
        let mut panel = AgentTeamPanel::new();
        panel.sync_from_app(&app);
        assert!(
            panel.agents.iter().any(|a| a.agent_id == "via-app"),
            "Agent 'via-app' should be present after sync_from_app"
        );

        dir.clear_all();
    }

    #[test]
    fn sync_resets_detail_when_roster_changes() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("x", "E", AgentStatus::Active, vec![]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
        panel.detail_idx = Some(0);

        dir.register(dummy_agent("y", "P", AgentStatus::Active, vec![]));
        panel.sync();
        // Roster length changed, detail should reset
        assert!(panel.detail_idx.is_none());

        dir.clear_all();
    }

    #[test]
    fn detail_toggle_on_empty_roster_is_noop() {
        let mut panel = AgentTeamPanel::new();
        assert!(panel.agents.is_empty());
        // Call toggle_detail via Enter event
        let press_enter = Event::Key(KeyEvent::new(KeyCode::Enter, crossterm::event::KeyModifiers::NONE));
        panel.handle_event(&press_enter);
        assert!(panel.detail_idx.is_none());
    }

    #[test]
    fn selection_clamped_after_roster_shrinks() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        dir.register(dummy_agent("a", "P", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("b", "E", AgentStatus::Active, vec![]));
        dir.register(dummy_agent("c", "R", AgentStatus::Active, vec![]));

        let mut panel = AgentTeamPanel::new();
        panel.sync();
        panel.selected_idx = 2; // last entry

        dir.unregister("c");
        panel.sync();
        // selected_idx should be clamped to 1 (the new last index)
        assert_eq!(panel.selected_idx, 1);

        dir.clear_all();
    }

    #[test]
    fn scroll_offset_tracks_selection_on_j() {
        let dir = AgentDirectory::global();
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
        panel.sync();
        panel.visible = true;
        panel.selected_idx = 0;
        panel.scroll_offset = 0;

        let agent_count = panel.agents.len();
        assert!(agent_count >= 10, "Expected at least 10 agents, got {agent_count}");

        let press_j = Event::Key(KeyEvent::new(KeyCode::Char('j'), crossterm::event::KeyModifiers::NONE));
        for _ in 0..12 {
            panel.handle_event(&press_j);
        }
        // selected_idx should be at the clamped maximum
        assert!(panel.selected_idx > 0, "selection should advance");
        assert!(panel.selected_idx < agent_count, "selection should be clamped");
        if agent_count >= 10 {
            assert!(panel.scroll_offset > 0, "scroll should track selection for long lists");
        }

        dir.clear_all();
    }
}
