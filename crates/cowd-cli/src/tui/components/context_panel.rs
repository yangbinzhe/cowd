#![allow(dead_code)]

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};
use runtime::ContextEnvelope;

use crate::tui::app::App;
use crate::tui::components::{Component, EventResult, RenderContext};

/// Context panel showing token usage, progress bar, and cost estimate.
///
/// Features:
/// - Token count: used / context window
/// - Progress bar: ████░░ 75%
/// - Cost estimate in USD
/// - Auto-updates from App state on sync
pub struct ContextPanel {
    /// Token count used so far.
    pub token_count: u64,
    /// Total context window size.
    pub context_window: u64,
    /// Input tokens for current turn.
    pub turn_input_tokens: u64,
    /// Output tokens for current turn.
    pub turn_output_tokens: u64,
    /// Latest runtime context envelope, if one has been assembled for a turn.
    pub latest_envelope: Option<ContextEnvelope>,
    evidence_detail_open: bool,
    evidence_cursor: usize,
}

impl ContextPanel {
    pub fn new() -> Self {
        Self {
            token_count: 0,
            context_window: 0,
            turn_input_tokens: 0,
            turn_output_tokens: 0,
            latest_envelope: None,
            evidence_detail_open: false,
            evidence_cursor: 0,
        }
    }

    pub fn sync_from_app(&mut self, app: &App) {
        self.token_count = app.token_count;
        self.context_window = app.context_window;
        self.turn_input_tokens = app.turn_input_tokens;
        self.turn_output_tokens = app.turn_output_tokens;
        self.latest_envelope = app.latest_context_envelope.clone();
    }

    pub fn from_app(app: &App) -> Self {
        let mut cp = Self::new();
        cp.sync_from_app(app);
        cp
    }

    /// Build a unicode progress bar string: "████░░░░"
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

    fn usage_pct(&self) -> f64 {
        if self.context_window == 0 {
            0.0
        } else {
            (self.token_count as f64 / self.context_window as f64 * 100.0).min(100.0)
        }
    }

    /// Estimate cost in USD based on token counts.
    fn cost_estimate(&self) -> f64 {
        let input_cost = self.token_count as f64 * 3.0 / 1_000_000.0;
        let output_cost = self.turn_output_tokens as f64 * 15.0 / 1_000_000.0;
        input_cost + output_cost
    }

    fn pressure_pct(envelope: &ContextEnvelope) -> u16 {
        envelope.diagnostics.pressure_bp / 100
    }

    fn short_hash(hash: &str) -> String {
        if hash.is_empty() {
            "n/a".to_string()
        } else {
            hash.chars().take(10).collect()
        }
    }

    fn preview(text: &str, max: usize) -> String {
        let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
        if normalized.chars().count() <= max {
            normalized
        } else {
            normalized.chars().take(max).collect::<String>() + "..."
        }
    }

    fn evidence_refs(&self) -> Vec<String> {
        let Some(envelope) = &self.latest_envelope else {
            return Vec::new();
        };
        let mut refs = Vec::new();
        for item in &envelope.selected {
            for evidence_ref in &item.evidence {
                if !refs.contains(evidence_ref) {
                    refs.push(evidence_ref.clone());
                }
            }
        }
        refs
    }

    fn selected_evidence_ref(&self) -> Option<String> {
        let refs = self.evidence_refs();
        refs.get(self.evidence_cursor.min(refs.len().saturating_sub(1)))
            .cloned()
    }

    fn evidence_kind(evidence_ref: &str) -> &str {
        evidence_ref.split("://").next().unwrap_or("unknown")
    }

    fn evidence_related_preview(&self, evidence_ref: &str) -> Option<String> {
        let envelope = self.latest_envelope.as_ref()?;
        envelope
            .selected
            .iter()
            .find(|item| {
                item.evidence
                    .iter()
                    .any(|candidate| candidate == evidence_ref)
            })
            .map(|item| Self::preview(&item.content, 96))
    }

    fn clamp_evidence_cursor(&mut self) {
        let len = self.evidence_refs().len();
        if len == 0 {
            self.evidence_cursor = 0;
            self.evidence_detail_open = false;
        } else if self.evidence_cursor >= len {
            self.evidence_cursor = len - 1;
        }
    }
}

impl Default for ContextPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for ContextPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let pct = self.usage_pct();
        let bar = Self::progress_bar(self.token_count, self.context_window, 12);
        let cost = self.cost_estimate();

        let mut lines = Vec::new();
        lines.push(Line::from(Span::styled(
            "Context Usage",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));

        // Progress bar line
        let bar_color = if pct > 90.0 {
            Color::Red
        } else if pct > 70.0 {
            Color::Yellow
        } else {
            Color::Green
        };
        lines.push(Line::from(vec![
            Span::styled(bar, Style::default().fg(bar_color)),
            Span::styled(
                format!(" {:.0}%", pct),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
        lines.push(Line::raw(""));

        // Token counts
        lines.push(Line::from(vec![
            Span::styled("Used:     ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.token_count),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Window:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.context_window),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Input:    ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.turn_input_tokens),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Output:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                fmt_tokens(self.turn_output_tokens),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::raw(""));

        // Cost
        lines.push(Line::from(vec![
            Span::styled("Cost:     ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("${:.4}", cost), Style::default().fg(Color::Yellow)),
        ]));
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "Runtime Envelope",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));

        if let Some(envelope) = &self.latest_envelope {
            let evidence_refs = self.evidence_refs();
            lines.push(Line::from(vec![
                Span::styled("Profile:  ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{:?}", envelope.profile),
                    Style::default().fg(Color::White),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Pressure: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}%", Self::pressure_pct(envelope)),
                    Style::default().fg(if envelope.diagnostics.pressure_bp > 8_500 {
                        Color::Red
                    } else if envelope.diagnostics.pressure_bp > 7_000 {
                        Color::Yellow
                    } else {
                        Color::Green
                    }),
                ),
                Span::styled(
                    format!(
                        "  selected {} omitted {}",
                        envelope.selected.len(),
                        envelope.omitted.len()
                    ),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Hash:     ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        "stable {} runtime {} dynamic {}",
                        Self::short_hash(&envelope.diagnostics.stable_head_hash),
                        Self::short_hash(&envelope.diagnostics.runtime_header_hash),
                        Self::short_hash(&envelope.diagnostics.dynamic_tail_hash)
                    ),
                    Style::default().fg(Color::White),
                ),
            ]));
            if !envelope.diagnostics.degraded_sources.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("Degraded: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        envelope
                            .diagnostics
                            .degraded_sources
                            .iter()
                            .map(|source| format!("{source:?}"))
                            .collect::<Vec<_>>()
                            .join(", "),
                        Style::default().fg(Color::Yellow),
                    ),
                ]));
            }
            if !envelope.diagnostics.recommendations.is_empty() {
                lines.push(Line::from(Span::styled(
                    "Recommendations",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )));
                for recommendation in envelope.diagnostics.recommendations.iter().take(3) {
                    lines.push(Line::from(vec![
                        Span::styled("- ", Style::default().fg(Color::DarkGray)),
                        Span::styled(Self::preview(recommendation, 72), Style::default()),
                    ]));
                }
            }
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Selected",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )));
            if envelope.selected.is_empty() {
                lines.push(Line::from(Span::styled(
                    "No selected context",
                    Style::default().fg(Color::DarkGray),
                )));
            } else {
                for item in envelope.selected.iter().take(5) {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{:?}: ", item.role),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::styled(Self::preview(&item.content, 52), Style::default()),
                    ]));
                    if !item.evidence.is_empty() {
                        let refs = item
                            .evidence
                            .iter()
                            .take(2)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ");
                        lines.push(Line::from(vec![
                            Span::styled("  refs:   ", Style::default().fg(Color::DarkGray)),
                            Span::styled(
                                Self::preview(&refs, 64),
                                Style::default().fg(Color::DarkGray),
                            ),
                        ]));
                    }
                }
            }
            if !envelope.omitted.is_empty() {
                lines.push(Line::raw(""));
                lines.push(Line::from(Span::styled(
                    "Omitted",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )));
                for omitted in envelope.omitted.iter().take(4) {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{:?}: ", omitted.source),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(Self::preview(&omitted.reason, 54), Style::default()),
                    ]));
                }
            }
            if self.evidence_detail_open {
                lines.push(Line::raw(""));
                lines.push(Line::from(Span::styled(
                    "Evidence Detail",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )));
                if let Some(evidence_ref) = self.selected_evidence_ref() {
                    let total = evidence_refs.len();
                    lines.push(Line::from(vec![
                        Span::styled("Ref:      ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            Self::preview(&evidence_ref, 88),
                            Style::default().fg(Color::White),
                        ),
                    ]));
                    lines.push(Line::from(vec![
                        Span::styled("Type:     ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            Self::evidence_kind(&evidence_ref).to_string(),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::styled(
                            format!(
                                "  {}/{}",
                                self.evidence_cursor.min(total.saturating_sub(1)) + 1,
                                total
                            ),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                    if let Some(preview) = self.evidence_related_preview(&evidence_ref) {
                        lines.push(Line::from(vec![
                            Span::styled("Context:  ", Style::default().fg(Color::DarkGray)),
                            Span::styled(preview, Style::default()),
                        ]));
                    }
                } else {
                    lines.push(Line::from(Span::styled(
                        "No evidence refs in selected context",
                        Style::default().fg(Color::DarkGray),
                    )));
                }
            }
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "Segments",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(format!(
                "stable {}  runtime {}  dynamic {}",
                envelope.assembled.stable_head.len(),
                envelope.assembled.runtime_header.len(),
                envelope.assembled.dynamic_tail.len()
            )));
        } else {
            lines.push(Line::from(Span::styled(
                "No runtime envelope yet.",
                Style::default().fg(Color::DarkGray),
            )));
        }

        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "Keys: Tab:switch panel  r:refresh  e:evidence  n/p:next/prev ref",
            Style::default().fg(Color::DarkGray),
        )));

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Context ");
        let paragraph = Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    fn handle_event(&mut self, event: &crossterm::event::Event) -> EventResult {
        use crossterm::event::{Event, KeyCode};

        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        match key.code {
            KeyCode::Char('e') => {
                self.evidence_detail_open = !self.evidence_detail_open;
                self.clamp_evidence_cursor();
                EventResult::Consumed
            }
            KeyCode::Char('n') if self.evidence_detail_open => {
                let len = self.evidence_refs().len();
                if len > 0 {
                    self.evidence_cursor = (self.evidence_cursor + 1).min(len - 1);
                }
                EventResult::Consumed
            }
            KeyCode::Char('p') if self.evidence_detail_open => {
                self.evidence_cursor = self.evidence_cursor.saturating_sub(1);
                EventResult::Consumed
            }
            KeyCode::Esc if self.evidence_detail_open => {
                self.evidence_detail_open = false;
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "context_panel"
    }
}

fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 10_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else if n >= 1_000 {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    fn render_panel(panel: &mut ContextPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    fn test_envelope_with_evidence() -> ContextEnvelope {
        let identity = runtime::ContextIdentity::main("session-1".to_string());
        runtime::ContextRuntimeKernel::build_envelope(runtime::ContextEnvelopeRequest {
            profile: runtime::ContextProfile::from(identity.mode),
            identity,
            intent: "inspect".to_string(),
            stable_head: vec!["stable system prompt".to_string()],
            runtime_header: vec!["session:session-1 agent:primary".to_string()],
            dynamic_items: vec![
                {
                    let mut item = runtime::ContextItem::new(
                        "mem-1",
                        runtime::ContextSourceKind::Memory,
                        runtime::ContextRole::Evidence,
                        "SessionKernel owns durable sessions",
                    );
                    item.evidence = vec!["session://session-1/memory/mem-1".to_string()];
                    item
                },
                {
                    let mut item = runtime::ContextItem::new(
                        "tool-1",
                        runtime::ContextSourceKind::ToolTrace,
                        runtime::ContextRole::Evidence,
                        "cargo test completed successfully",
                    );
                    item.evidence = vec!["tool://tool-1".to_string()];
                    item
                },
            ],
            omitted: vec![runtime::ContextOmission {
                source: runtime::ContextSourceKind::Memory,
                reason: "context lease exhausted".to_string(),
                token_estimate: 24,
            }],
            total_budget_tokens: 8_000,
        })
    }

    #[test]
    fn context_panel_shows_token_count() {
        let mut panel = ContextPanel::new();
        panel.token_count = 1500;
        panel.context_window = 128_000;

        let lines = render_panel(&mut panel, 40, 12);
        let joined = lines.join("\n");
        assert!(
            joined.contains("1k") || joined.contains("1500"),
            "Should show token count"
        );
        assert!(
            joined.contains("128") || joined.contains("128k"),
            "Should show context window"
        );
    }

    #[test]
    fn shows_percent() {
        let mut panel = ContextPanel::new();
        panel.token_count = 64_000;
        panel.context_window = 128_000;

        let lines = render_panel(&mut panel, 40, 12);
        let joined = lines.join("\n");
        assert!(joined.contains("50%"), "Should show 50% for half usage");
    }

    #[test]
    fn updates_on_sync() {
        let mut app = App::new("m", "s");
        app.token_count = 10_000;
        app.context_window = 100_000;
        app.turn_input_tokens = 500;
        app.turn_output_tokens = 200;

        let mut panel = ContextPanel::from_app(&app);
        assert_eq!(panel.token_count, 10_000);
        assert_eq!(panel.context_window, 100_000);
        assert_eq!(panel.turn_input_tokens, 500);
        assert_eq!(panel.turn_output_tokens, 200);

        // Re-sync with updated values
        app.token_count = 20_000;
        panel.sync_from_app(&app);
        assert_eq!(panel.token_count, 20_000);
    }

    #[test]
    fn progress_bar_zero_window() {
        let bar = ContextPanel::progress_bar(100, 0, 12);
        assert_eq!(bar.chars().count(), 12, "bar should have 12 chars");
        assert!(bar.chars().all(|c| c == '░'), "zero window = empty bar");
    }

    #[test]
    fn cost_estimate_non_zero() {
        let mut panel = ContextPanel::new();
        panel.token_count = 100_000;
        panel.turn_output_tokens = 10_000;
        let cost = panel.cost_estimate();
        assert!(cost > 0.0, "Cost should be positive");
    }

    #[test]
    fn renders_runtime_envelope_diagnostics() {
        let envelope = test_envelope_with_evidence();
        let stable_hash = ContextPanel::short_hash(&envelope.diagnostics.stable_head_hash);
        let mut panel = ContextPanel::new();
        panel.latest_envelope = Some(envelope);

        let lines = render_panel(&mut panel, 88, 32);
        let joined = lines.join("\n");
        assert!(joined.contains("Runtime Envelope"));
        assert!(joined.contains("SessionKernel owns durable sessions"));
        assert!(joined.contains("session://session-1/memory/mem-1"));
        assert!(joined.contains("context lease exhausted"));
        assert!(joined.contains(&stable_hash));
        assert!(joined.contains("stable 1"));
        assert!(joined.contains("Recommendations"));
    }

    #[test]
    fn evidence_detail_opens_and_renders_selected_ref() {
        let mut panel = ContextPanel::new();
        panel.latest_envelope = Some(test_envelope_with_evidence());

        let event = crossterm::event::Event::Key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('e'),
            crossterm::event::KeyModifiers::NONE,
        ));

        assert!(panel.handle_event(&event).is_consumed());
        let lines = render_panel(&mut panel, 96, 36);
        let joined = lines.join("\n");

        assert!(joined.contains("Evidence Detail"));
        assert!(joined.contains("session://session-1/memory/mem-1"));
        assert!(joined.contains("SessionKernel owns durable sessions"));
    }

    #[test]
    fn evidence_detail_can_navigate_refs_and_close() {
        let mut panel = ContextPanel::new();
        panel.latest_envelope = Some(test_envelope_with_evidence());
        panel.evidence_detail_open = true;

        let next = crossterm::event::Event::Key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('n'),
            crossterm::event::KeyModifiers::NONE,
        ));
        let close = crossterm::event::Event::Key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Esc,
            crossterm::event::KeyModifiers::NONE,
        ));

        assert!(panel.handle_event(&next).is_consumed());
        assert_eq!(
            panel.selected_evidence_ref().as_deref(),
            Some("tool://tool-1")
        );
        assert!(panel.handle_event(&close).is_consumed());
        assert!(!panel.evidence_detail_open);
    }

    #[test]
    fn component_trait_methods() {
        let panel = ContextPanel::new();
        assert!(panel.focusable());
        assert_eq!(panel.id(), "context_panel");
    }
}
