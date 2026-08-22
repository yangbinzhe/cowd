#![allow(dead_code)]

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};
use serde_json::Value;

use crate::app::App;
use crate::components::{Component, EventResult, RenderContext};

/// Context panel showing token usage and context pressure.
///
/// Features:
/// - Token count: used / context window
/// - Progress bar: ████░░ 75%
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
    pub latest_envelope: Option<Value>,
    /// Memory subsystem status surfaced into the context plane.
    pub memory_status: Option<String>,
    /// Number of memory entries visible in App fallback state.
    pub memory_entry_count: usize,
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
            memory_status: None,
            memory_entry_count: 0,
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
        self.memory_status = app.memory_status.clone();
        self.memory_entry_count = app.memory_entries.len();
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

    fn pressure_bp(envelope: &Value) -> u64 {
        envelope
            .pointer("/diagnostics/pressure_bp")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    }

    fn pressure_pct(envelope: &Value) -> u64 {
        Self::pressure_bp(envelope) / 100
    }

    fn envelope_profile(envelope: &Value) -> String {
        envelope
            .get("profile")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string()
    }

    fn array_len(envelope: &Value, path: &str) -> usize {
        envelope
            .pointer(path)
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default()
    }

    fn string_at<'a>(envelope: &'a Value, path: &str) -> &'a str {
        envelope
            .pointer(path)
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    fn string_vec_at(envelope: &Value, path: &str) -> Vec<String> {
        envelope
            .pointer(path)
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn selected_items(envelope: &Value) -> &[Value] {
        envelope
            .get("selected")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    fn omitted_items(envelope: &Value) -> &[Value] {
        envelope
            .get("omitted")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    fn item_string<'a>(item: &'a Value, field: &str) -> &'a str {
        item.get(field).and_then(Value::as_str).unwrap_or_default()
    }

    fn item_evidence(item: &Value) -> Vec<String> {
        item.get("evidence")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default()
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
        for item in Self::selected_items(envelope) {
            for evidence_ref in Self::item_evidence(item) {
                if !refs.contains(&evidence_ref) {
                    refs.push(evidence_ref);
                }
            }
        }
        refs
    }

    fn selected_memory_count(envelope: &Value) -> usize {
        Self::selected_items(envelope)
            .iter()
            .filter(|item| Self::item_string(item, "source").eq_ignore_ascii_case("memory"))
            .count()
    }

    fn segment_len(envelope: &Value, segment: &str) -> usize {
        let assembled_path = format!("/assembled/{segment}");
        if let Some(values) = envelope.pointer(&assembled_path).and_then(Value::as_array) {
            return values.len();
        }
        match segment {
            "stable_head" => Self::array_len(envelope, "/render_manifest/stable_head"),
            "runtime_header" => Self::array_len(envelope, "/render_manifest/runtime_header"),
            "dynamic_tail" => Self::array_len(envelope, "/selected"),
            _ => 0,
        }
    }

    fn stable_head_reuse_hint(envelope: &Value) -> &'static str {
        if Self::string_at(envelope, "/diagnostics/stable_head_hash").is_empty() {
            "unavailable"
        } else if Self::segment_len(envelope, "stable_head") == 0 {
            "empty"
        } else {
            "cache-friendly"
        }
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
        Self::selected_items(envelope)
            .iter()
            .find(|item| {
                Self::item_evidence(item)
                    .iter()
                    .any(|candidate| candidate == evidence_ref)
            })
            .map(|item| Self::preview(Self::item_string(item, "content"), 96))
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

        lines.push(Line::from(Span::styled(
            "Runtime Envelope",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));

        if let Some(envelope) = &self.latest_envelope {
            let evidence_refs = self.evidence_refs();
            let memory_selected = Self::selected_memory_count(envelope);
            let pressure_bp = Self::pressure_bp(envelope);
            lines.push(Line::from(vec![
                Span::styled("Profile:  ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    Self::envelope_profile(envelope),
                    Style::default().fg(Color::White),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Pressure: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}%", Self::pressure_pct(envelope)),
                    Style::default().fg(if pressure_bp > 8_500 {
                        Color::Red
                    } else if pressure_bp > 7_000 {
                        Color::Yellow
                    } else {
                        Color::Green
                    }),
                ),
                Span::styled(
                    format!(
                        "  selected {} omitted {}",
                        Self::array_len(envelope, "/selected"),
                        Self::array_len(envelope, "/omitted")
                    ),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Hash:     ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        "stable {} runtime {} dynamic {}",
                        Self::short_hash(Self::string_at(
                            envelope,
                            "/diagnostics/stable_head_hash"
                        )),
                        Self::short_hash(Self::string_at(
                            envelope,
                            "/diagnostics/runtime_header_hash"
                        )),
                        Self::short_hash(Self::string_at(
                            envelope,
                            "/diagnostics/dynamic_tail_hash"
                        ))
                    ),
                    Style::default().fg(Color::White),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Stable:   ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    Self::stable_head_reuse_hint(envelope),
                    Style::default().fg(Color::Green),
                ),
                Span::styled(
                    format!("  segments {}", Self::segment_len(envelope, "stable_head")),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Memory:   ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.memory_status.as_deref().unwrap_or("unknown"),
                    Style::default().fg(if memory_selected > 0 {
                        Color::Green
                    } else {
                        Color::DarkGray
                    }),
                ),
                Span::styled(
                    format!(
                        "  selected {} fallback {}",
                        memory_selected, self.memory_entry_count
                    ),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
            let degraded_sources = Self::string_vec_at(envelope, "/diagnostics/degraded_sources");
            if !degraded_sources.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("Degraded: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        degraded_sources.join(", "),
                        Style::default().fg(Color::Yellow),
                    ),
                ]));
            }
            let recommendations = Self::string_vec_at(envelope, "/diagnostics/recommendations");
            if !recommendations.is_empty() {
                lines.push(Line::from(Span::styled(
                    "Recommendations",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )));
                for recommendation in recommendations.iter().take(3) {
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
            let selected = Self::selected_items(envelope);
            if selected.is_empty() {
                lines.push(Line::from(Span::styled(
                    "No selected context",
                    Style::default().fg(Color::DarkGray),
                )));
            } else {
                for item in selected.iter().take(5) {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{}: ", Self::item_string(item, "role")),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::styled(
                            Self::preview(Self::item_string(item, "content"), 52),
                            Style::default(),
                        ),
                    ]));
                    let item_evidence = Self::item_evidence(item);
                    if !item_evidence.is_empty() {
                        let refs = item_evidence
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
            let omitted_items = Self::omitted_items(envelope);
            if !omitted_items.is_empty() {
                lines.push(Line::raw(""));
                lines.push(Line::from(Span::styled(
                    "Omitted",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )));
                for omitted in omitted_items.iter().take(4) {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{}: ", Self::item_string(omitted, "source")),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(
                            Self::preview(Self::item_string(omitted, "reason"), 54),
                            Style::default(),
                        ),
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
                Self::segment_len(envelope, "stable_head"),
                Self::segment_len(envelope, "runtime_header"),
                Self::segment_len(envelope, "dynamic_tail")
            )));
        } else {
            lines.push(Line::from(Span::styled(
                "No runtime envelope yet.",
                Style::default().fg(Color::DarkGray),
            )));
            lines.push(Line::from(vec![
                Span::styled("Memory:   ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.memory_status.as_deref().unwrap_or("unknown"),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("  fallback {}", self.memory_entry_count),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
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
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;

    fn render_panel(panel: &mut ContextPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    fn test_envelope_with_evidence() -> serde_json::Value {
        crate::test_utils::context_envelope_fixture()
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
        app.memory_status = Some("available".to_string());
        app.memory_entries = vec![crate::app::MemoryEntry {
            id: Some("m1".to_string()),
            layer: "L4".to_string(),
            content: "durable note".to_string(),
            priority: "high".to_string(),
        }];

        let mut panel = ContextPanel::from_app(&app);
        assert_eq!(panel.token_count, 10_000);
        assert_eq!(panel.context_window, 100_000);
        assert_eq!(panel.turn_input_tokens, 500);
        assert_eq!(panel.turn_output_tokens, 200);
        assert_eq!(panel.memory_status.as_deref(), Some("available"));
        assert_eq!(panel.memory_entry_count, 1);

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
    fn renders_runtime_envelope_diagnostics() {
        let envelope = test_envelope_with_evidence();
        let stable_hash = ContextPanel::short_hash(
            envelope
                .pointer("/diagnostics/stable_head_hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
        );
        let mut panel = ContextPanel::new();
        panel.latest_envelope = Some(envelope);
        panel.memory_status = Some("available".to_string());
        panel.memory_entry_count = 4;

        let lines = render_panel(&mut panel, 88, 40);
        let joined = lines.join("\n");
        assert!(joined.contains("Runtime Envelope"));
        assert!(joined.contains("SessionKernel owns durable sessions"));
        assert!(joined.contains("session://session-1/memory/mem-1"));
        assert!(joined.contains("context lease exhausted"));
        assert!(joined.contains(&stable_hash));
        assert!(joined.contains("cache-friendly"));
        assert!(joined.contains("Memory:"));
        assert!(joined.contains("selected 1 fallback 4"));
        assert!(joined.contains("stable 1"));
        assert!(joined.contains("Recommendations"));
    }

    #[test]
    fn renders_canonical_persisted_envelope_segments() {
        let mut envelope = test_envelope_with_evidence();
        envelope
            .as_object_mut()
            .expect("envelope object")
            .remove("assembled");
        envelope["render_manifest"] = serde_json::json!({
            "formatter_version": 2,
            "stable_head": ["stable"],
            "runtime_header": ["runtime"],
            "dynamic_tail_source": "selected",
        });
        let selected = envelope["selected"].as_array().unwrap().len();
        let mut panel = ContextPanel::new();
        panel.latest_envelope = Some(envelope);

        let lines = render_panel(&mut panel, 88, 40);
        let joined = lines.join("\n");

        assert!(joined.contains("cache-friendly"));
        assert!(joined.contains(&format!("stable 1  runtime 1  dynamic {selected}")));
    }

    #[test]
    fn renders_memory_status_without_runtime_envelope() {
        let mut panel = ContextPanel::new();
        panel.memory_status = Some("warming".to_string());
        panel.memory_entry_count = 2;

        let lines = render_panel(&mut panel, 72, 18);
        let joined = lines.join("\n");
        assert!(joined.contains("No runtime envelope yet."));
        assert!(joined.contains("Memory:"));
        assert!(joined.contains("warming"));
        assert!(joined.contains("fallback 2"));
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
