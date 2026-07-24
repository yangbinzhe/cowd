// ── Status Bar Component — Modular Section Registration ──────────
// Ported from widgets/status_bar.rs to Component trait.
// Each status section registers independently with Fixed or Fill width.
// Colors provided by ThemeEngine::compute_style (via RenderContext).

#![allow(dead_code)]

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
};

use crate::app::App;
use crate::components::{Component, EventResult, RenderContext};

// ── SectionWidth ─────────────────────────────────────────────────

/// How a section's width is allocated in the status bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionWidth {
    /// Fixed width in cells.
    Fixed(u16),
    /// Takes remaining space proportionally after Fixed sections.
    Fill,
}

// ── StatusSection ────────────────────────────────────────────────

/// A registered status bar section.
///
/// Each section renders as `content` with `style`.
/// Set `content` to `None` to hide the section entirely.
#[derive(Debug, Clone)]
pub struct StatusSection {
    /// Unique identifier (e.g. "brand", "token_bar").
    pub id: String,
    /// Display text content. `None` = hidden.
    pub content: Option<String>,
    /// Style applied to this section's spans.
    pub style: Style,
    /// Width allocation strategy (used for truncation).
    pub width: SectionWidth,
}

// ── WaveState ─────────────────────────────────────────────────────

/// Tracks the current agentic loop wave execution state.
#[derive(Debug, Clone, Default)]
pub struct WaveState {
    /// Current wave index (1-based).
    pub current: u32,
    /// Total number of waves planned.
    pub total: u32,
    /// Tasks waiting to be dispatched.
    pub pending: usize,
    /// Tasks currently running.
    pub running: usize,
    /// Tasks completed successfully.
    pub done: usize,
    /// Tasks that failed.
    pub failed: usize,
}

// ── StatusBar ────────────────────────────────────────────────────

/// A modular status bar rendered as a single-line bar at the top.
///
/// Sections are registered via [`add_section`](Self::add_section) or
/// [`remove_section`](Self::remove_section) and rendered left-to-right,
/// concatenated with ` │ ` separators. If the total content exceeds the
/// given area width, sections are truncated from the right.
pub struct StatusBar {
    sections: Vec<StatusSection>,
    essential_projection_enabled: bool,
    compact_model: String,
    compact_status: String,
    compact_context: String,
    notification: Option<(String, Style)>,
    notification_ttl: u32,
}

impl StatusBar {
    /// Create a new empty status bar with no sections.
    pub fn new() -> Self {
        Self {
            sections: Vec::new(),
            essential_projection_enabled: false,
            compact_model: "—".to_string(),
            compact_status: "idle".to_string(),
            compact_context: "—".to_string(),
            notification: None,
            notification_ttl: 0,
        }
    }

    /// Create a status bar pre-populated with all default sections
    /// matching the original `widgets/status_bar.rs` functionality.
    pub fn with_default_sections() -> Self {
        let mut sb = Self::new();
        sb.essential_projection_enabled = true;
        sb.add_section(StatusSection {
            id: "model".into(),
            content: None,
            style: Style::default().fg(Color::White),
            width: SectionWidth::Fixed(22),
        });
        sb.add_section(StatusSection {
            id: "run_status".into(),
            content: None,
            style: Style::default().fg(Color::Cyan),
            width: SectionWidth::Fixed(34),
        });
        sb.add_section(StatusSection {
            id: "model_telemetry".into(),
            content: None,
            style: Style::default().fg(Color::Cyan),
            width: SectionWidth::Fixed(36),
        });
        sb.add_section(StatusSection {
            id: "context".into(),
            content: None,
            style: Style::default().fg(Color::Green),
            width: SectionWidth::Fixed(48),
        });
        sb.add_section(StatusSection {
            id: "turn_tokens".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(24),
        });
        sb.add_section(StatusSection {
            id: "session_tokens".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(30),
        });
        sb.add_section(StatusSection {
            id: "memory_stats".into(),
            content: None,
            style: Style::default().fg(Color::Gray),
            width: SectionWidth::Fixed(18),
        });
        sb.add_section(Self::search_section());
        sb.add_section(Self::history_section());
        sb.add_section(StatusSection {
            id: "causal_health".into(),
            content: None,
            style: Style::default().fg(Color::Red),
            width: SectionWidth::Fixed(16),
        });
        sb
    }

    // ── Default section factories ────────────────────────────────

    fn brand_section() -> StatusSection {
        StatusSection {
            id: "brand".into(),
            content: None,
            style: Style::default(),
            width: SectionWidth::Fixed(6),
        }
    }

    fn panel_model_status_section() -> StatusSection {
        StatusSection {
            id: "panel_model_status".into(),
            content: None,
            style: Style::default(),
            width: SectionWidth::Fill,
        }
    }

    fn token_bar_section() -> StatusSection {
        StatusSection {
            id: "token_bar".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(32),
        }
    }

    fn token_count_section() -> StatusSection {
        StatusSection {
            id: "token_count".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(20),
        }
    }

    fn turn_token_section() -> StatusSection {
        StatusSection {
            id: "turn_token".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(22),
        }
    }

    fn compaction_section() -> StatusSection {
        StatusSection {
            id: "compaction".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(10),
        }
    }

    fn cache_section() -> StatusSection {
        StatusSection {
            id: "cache".into(),
            content: None,
            style: Style::default().fg(Color::Green),
            width: SectionWidth::Fixed(12),
        }
    }

    fn search_section() -> StatusSection {
        StatusSection {
            id: "search".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(30),
        }
    }

    fn history_section() -> StatusSection {
        StatusSection {
            id: "history".into(),
            content: None,
            style: Style::default().fg(Color::DarkGray),
            width: SectionWidth::Fixed(10),
        }
    }

    fn task_section() -> StatusSection {
        StatusSection {
            id: "task".into(),
            content: None,
            style: Style::default().fg(Color::Magenta),
            width: SectionWidth::Fixed(28),
        }
    }

    fn lease_section() -> StatusSection {
        StatusSection {
            id: "lease".into(),
            content: None,
            style: Style::default().fg(Color::Cyan),
            width: SectionWidth::Fixed(30),
        }
    }

    fn wave_section() -> StatusSection {
        StatusSection {
            id: "wave".into(),
            content: None,
            style: Style::default().fg(Color::Cyan),
            width: SectionWidth::Fixed(28),
        }
    }

    // ── Reputation section ──────────────────────────────────────

    fn reputation_section() -> StatusSection {
        StatusSection {
            id: "reputation".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(8),
        }
    }

    // ── Task 12: Footer status sections ─────────────────────────

    fn mcp_status_section() -> StatusSection {
        StatusSection {
            id: "mcp_status".into(),
            content: None,
            style: Style::default().fg(Color::Green),
            width: SectionWidth::Fixed(16),
        }
    }

    fn lsp_status_section() -> StatusSection {
        StatusSection {
            id: "lsp_status".into(),
            content: None,
            style: Style::default().fg(Color::Green),
            width: SectionWidth::Fixed(16),
        }
    }

    fn permission_status_section() -> StatusSection {
        StatusSection {
            id: "permission_status".into(),
            content: None,
            style: Style::default().fg(Color::Yellow),
            width: SectionWidth::Fixed(12),
        }
    }

    // ── Section management ───────────────────────────────────────

    /// Register a new section (appended to the right).
    pub fn add_section(&mut self, section: StatusSection) {
        self.sections.push(section);
    }

    /// Remove a section by its `id`. Returns the removed section if found.
    pub fn remove_section(&mut self, id: &str) -> Option<StatusSection> {
        let idx = self.sections.iter().position(|s| s.id == id)?;
        Some(self.sections.remove(idx))
    }

    /// Mutable access to a section by id (for updating content or style).
    pub fn section_mut(&mut self, id: &str) -> Option<&mut StatusSection> {
        self.sections.iter_mut().find(|s| s.id == id)
    }

    /// Iterate over all sections.
    pub fn sections(&self) -> &[StatusSection] {
        &self.sections
    }

    // ── Notifications ────────────────────────────────────────────

    /// Set a notification banner that overlays the status line.
    ///
    /// The notification is shown for `ttl` ticks then auto-dismisses.
    /// Call [`tick`](Self::tick) each frame to count down.
    pub fn set_notification(&mut self, text: impl Into<String>, style: Style, ttl: u32) {
        self.notification = Some((text.into(), style));
        self.notification_ttl = ttl;
    }

    /// Clear the notification immediately.
    pub fn clear_notification(&mut self) {
        self.notification = None;
        self.notification_ttl = 0;
    }

    /// Advance one tick (for notification auto-dismiss).
    /// Call this once per frame from the main loop.
    pub fn tick(&mut self) {
        if self.notification_ttl > 0 {
            self.notification_ttl -= 1;
            if self.notification_ttl == 0 {
                self.notification = None;
            }
        }
    }

    // ── Sync from App state ──────────────────────────────────────

    /// Populate each built-in section's `content` from `App` state.
    ///
    /// Call this before `render()` to reflect current App state.
    /// Only sections with recognized built-in IDs are populated;
    /// custom sections are left unchanged.
    pub fn sync_from_app(&mut self, app: &App) {
        if let Some(message) = app.notification.as_ref() {
            let changed = self
                .notification
                .as_ref()
                .is_none_or(|(current, _)| current != message);
            if changed {
                self.set_notification(
                    message.clone(),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                    30,
                );
            }
        }
        self.compact_model = compact_model(app);
        self.compact_status = compact_run_status(app);
        self.compact_context = app
            .context_usage_percent_bp
            .map(|value| format!("{:.0}%", f64::from(value) / 100.0))
            .unwrap_or_else(|| "—".to_string());
        for section in &mut self.sections {
            section.content = match section.id.as_str() {
                "version" => Some(format!("v{}", env!("CARGO_PKG_VERSION"))),
                "model" => {
                    let mode = if app.yolo_mode { "YOLO" } else { "STD" };
                    let model = match (
                        app.requested_model.as_deref(),
                        app.effective_model.as_deref(),
                    ) {
                        (Some(requested), Some(effective)) if requested != effective => {
                            section.style = Style::default().fg(Color::Yellow);
                            format!("{}→{}", preview(requested, 10), preview(effective, 10))
                        }
                        (_, Some(effective)) => {
                            section.style = Style::default().fg(Color::White);
                            preview(effective, 18)
                        }
                        (Some(requested), None) => {
                            section.style = Style::default().fg(Color::DarkGray);
                            preview(requested, 18)
                        }
                        (None, None) => {
                            section.style = Style::default().fg(Color::DarkGray);
                            "model —".to_string()
                        }
                    };
                    Some(format!("{model} {mode}"))
                }
                "run_status" => Some(format_run_status(app)),
                "model_telemetry" => {
                    let mut facts = Vec::new();
                    if let Some(telemetry) = app.latest_model_telemetry.as_ref() {
                        if let Some(latency) = telemetry.first_token_latency_ms {
                            facts.push(format!("first:{}ms", latency));
                        }
                        if let Some(speed) = telemetry
                            .active_tokens_per_second
                            .or(telemetry.tokens_per_second)
                        {
                            facts.push(format!("{speed:.1} tok/s"));
                        }
                        if !telemetry.models_used.is_empty() {
                            facts.push(format!(
                                "models:{}",
                                telemetry
                                    .models_used
                                    .iter()
                                    .map(|model| preview(model, 10))
                                    .collect::<Vec<_>>()
                                    .join("→")
                            ));
                        }
                    }
                    if let Some(source) = app.model_source.as_deref() {
                        facts.push(format!("src:{}", preview(source, 18)));
                    }
                    (!facts.is_empty()).then(|| facts.join(" "))
                }
                "context" => {
                    let pct = app
                        .context_usage_percent_bp
                        .map_or(0.0, |value| f64::from(value) / 100.0);
                    section.style = Style::default().fg(context_color(pct));
                    token_bar(app).map(|bar| format!("ctx {bar}"))
                }
                "turn_tokens" => {
                    if !app.turn_usage_known
                        && (app.turn_is_active() || app.current_run_metrics.is_some())
                    {
                        Some("in:— out:— Σ—".to_string())
                    } else {
                        app.current_run_metrics.as_ref().map_or_else(
                            || {
                                (app.input_tokens > 0 || app.output_tokens > 0).then(|| {
                                    format!(
                                        "in:{} out:{} Σ{}",
                                        fmt_tokens(app.input_tokens),
                                        fmt_tokens(app.output_tokens),
                                        fmt_tokens(
                                            app.input_tokens.saturating_add(app.output_tokens)
                                        )
                                    )
                                })
                            },
                            |metrics| {
                                Some(format!(
                                    "in:{} out:{} Σ{}",
                                    fmt_tokens(metrics.input_tokens),
                                    fmt_tokens(metrics.output_tokens),
                                    fmt_tokens(metrics.total_tokens)
                                ))
                            },
                        )
                    }
                }
                "session_tokens" => {
                    let authoritative = app.authoritative_session_input_tokens.is_some()
                        || app.authoritative_session_output_tokens.is_some();
                    let input = app
                        .authoritative_session_input_tokens
                        .unwrap_or(app.durable_session_input_tokens);
                    let output = app
                        .authoritative_session_output_tokens
                        .unwrap_or(app.durable_session_output_tokens);
                    (input > 0 || output > 0).then(|| {
                        format!(
                            "{} in:{} out:{} Σ{}",
                            if authoritative { "session" } else { "window" },
                            fmt_tokens(input),
                            fmt_tokens(output),
                            fmt_tokens(input.saturating_add(output))
                        )
                    })
                }
                "memory_stats" => app.current_run_metrics.as_ref().map_or_else(
                    || {
                        app.memory_total_entries.map(|total| {
                            format!(
                                "mem:{} vec:{} [{},{},{},{},{}]",
                                total,
                                app.memory_vector_count.unwrap_or_default(),
                                app.memory_layer_counts[0],
                                app.memory_layer_counts[1],
                                app.memory_layer_counts[2],
                                app.memory_layer_counts[3],
                                app.memory_layer_counts[4],
                            )
                        })
                    },
                    |metrics| {
                        Some(format!(
                            "⚙{} 🧠{}/{} ✓{} files:{}",
                            metrics.tool_calls,
                            metrics.memory_recalls,
                            metrics.memory_evidence,
                            metrics.approvals,
                            metrics.files_touched,
                        ))
                    },
                ),
                "search" => {
                    if app.search_active {
                        Some(format!("/{}", app.search_query))
                    } else if !app.search_matches.is_empty() {
                        Some(format!(
                            "/{} [{}/{}]",
                            app.search_query,
                            app.search_current + 1,
                            app.search_matches.len()
                        ))
                    } else {
                        None
                    }
                }
                "history" => app.history_idx.map(|hidx| format!("hist:{}", hidx + 1)),
                "causal_health" => (app.telemetry.orphan_event_count > 0)
                    .then(|| format!("⚠ orphan:{}", app.telemetry.orphan_event_count)),
                "permission_status" => None,
                _ => None,
            };
        }
    }

    /// Build a status bar from App state in one call.
    pub fn from_app(app: &App) -> Self {
        let mut sb = Self::with_default_sections();
        sb.sync_from_app(app);
        sb
    }
}

impl Default for StatusBar {
    fn default() -> Self {
        Self::with_default_sections()
    }
}

// ── Component Trait ──────────────────────────────────────────────

impl Component for StatusBar {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let available = area.width;

        // Determine brand style from theme engine or skin config
        let brand_style = if let Some(engine) = ctx.theme_engine() {
            engine.compute_style("heading1")
        } else {
            Style::default()
                .fg(ctx.theme().accent_color())
                .add_modifier(Modifier::BOLD)
        };

        // Narrow terminals use a dedicated essential projection. Rendering
        // the wide sections and breaking at the first overflow used to hide
        // context completely at 40–90 columns.
        let spans: Vec<Span<'static>> = if self.essential_projection_enabled && available < 120 {
            let model_limit = if available >= 72 { 20 } else { 12 };
            let status_limit = if available >= 72 { 18 } else { 10 };
            let model = truncate_cells(&self.compact_model, model_limit);
            let status = truncate_cells(&self.compact_status, status_limit);
            let essential = format!("m:{model}  s:{status}  ctx {}", self.compact_context);
            vec![Span::styled(
                truncate_cells(&essential, usize::from(available)),
                Style::default().fg(Color::White),
            )]
        } else {
            let mut spans = Vec::new();
            let mut first = true;
            let mut used = 0usize;
            let sep = " │ ";

            for section in &self.sections {
                if !status_section_visible_for_width(section.id.as_str(), available) {
                    continue;
                }
                let text = match &section.content {
                    Some(c) if !c.is_empty() => c.clone(),
                    _ => continue,
                };

                let style = if section.id == "brand" {
                    brand_style
                } else {
                    section.style
                };

                let display = if first {
                    first = false;
                    text
                } else {
                    format!("{sep}{text}")
                };
                let display_width = unicode_width::UnicodeWidthStr::width(display.as_str());
                if used.saturating_add(display_width) > usize::from(available) {
                    break;
                }

                used = used.saturating_add(display_width);
                spans.push(Span::styled(display, style));
            }
            spans
        };

        // Render the status line
        let bg = ctx.theme().bg_color();
        let par = Paragraph::new(Line::from(spans)).style(Style::default().bg(bg));
        ctx.frame_mut().render_widget(par, area);

        // ── Notification overlay ──────────────────────────────────
        if let Some((ref text, ref style)) = self.notification {
            let note_bg = style.bg.unwrap_or(Color::Cyan);
            let note_line = Line::from(Span::styled(text.clone(), *style));
            ctx.frame_mut().render_widget(
                Paragraph::new(note_line).style(Style::default().bg(note_bg)),
                area,
            );
        }
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        false
    }

    fn id(&self) -> &str {
        "status_bar"
    }
}

fn status_section_visible_for_width(id: &str, available: u16) -> bool {
    match id {
        "model" | "run_status" | "context" => true,
        "session_tokens" => available >= 160,
        "memory_stats" | "permission_status" => available >= 120,
        "input_hint" => available >= 150,
        "search" | "history" | "compaction" | "cache" | "task" | "lease" | "wave"
        | "reputation" | "mcp_status" | "lsp_status" => available >= 132,
        _ => true,
    }
}

// ── Helpers (ported from widgets/status_bar.rs) ──────────────────

/// Format a token count for display: 1.2M, 6.2K, 128, etc.
pub fn fmt_tokens(n: u64) -> String {
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

fn short_id(value: &str) -> String {
    if value.chars().count() <= 10 {
        value.to_string()
    } else {
        value.chars().take(10).collect()
    }
}

fn context_color(pct: f64) -> Color {
    if pct >= 85.0 {
        Color::Red
    } else if pct >= 65.0 {
        Color::Yellow
    } else {
        Color::Green
    }
}

fn preview(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let mut out: String = value.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Build a character-based progress bar: "████░░░░ 6.2K/128K (39%)"
pub fn token_bar(app: &App) -> Option<String> {
    let window = app
        .context_window_tokens
        .or_else(|| (app.context_window > 0).then_some(app.context_window))?;
    let used = app.context_used_tokens?;
    let pct = app.context_usage_percent_bp.map_or_else(
        || used as f64 / window.max(1) as f64 * 100.0,
        |bp| f64::from(bp) / 100.0,
    );
    let pct = pct.min(100.0);
    let bar_width: i32 = 12;
    let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
    let empty = (bar_width as usize).saturating_sub(filled);

    let mut bar = String::with_capacity(bar_width as usize + 32);
    for _ in 0..filled {
        bar.push('█');
    }
    for _ in 0..empty {
        bar.push('░');
    }

    let remaining = app
        .context_remaining_tokens
        .map(|value| format!(" rem:{}", fmt_tokens(value)))
        .unwrap_or_default();
    let source = app
        .context_usage_source
        .as_deref()
        .filter(|source| !source.trim().is_empty())
        .map(|source| format!(" src:{}", preview(source, 12)))
        .unwrap_or_default();
    Some(format!(
        "{} {}/{} ({:.0}%){remaining}{source}",
        bar,
        fmt_tokens(used),
        fmt_tokens(window),
        pct,
    ))
}

fn compact_model(app: &App) -> String {
    match (
        app.requested_model.as_deref(),
        app.effective_model.as_deref(),
    ) {
        (Some(requested), Some(effective)) if requested != effective => {
            // At narrow widths the provider-observed model is the operational
            // truth, so keep it before the requested fallback source.
            format!("{effective}←{requested}")
        }
        (_, Some(effective)) => effective.to_string(),
        (Some(requested), None) => format!("{requested}…"),
        (None, None) => "—".to_string(),
    }
}

fn compact_run_status(app: &App) -> String {
    let status = app
        .current_execution_status
        .map(execution_status_text)
        .unwrap_or_else(|| {
            if app.turn_is_active() {
                "submitting"
            } else {
                "idle"
            }
        });
    if app.gateway_lease_mode.as_deref() == Some("read-only") {
        if status == "idle" {
            "read-only".to_string()
        } else {
            format!("read-only/{status}")
        }
    } else {
        status.to_string()
    }
}

fn format_run_status(app: &App) -> String {
    let mut parts = vec![compact_run_status(app)];
    if app.turn_interaction.presentation.stale {
        parts.push("stale".to_string());
    }
    if let Some(execution_id) = app
        .current_execution_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
    {
        parts.push(format!("exec:{}", preview(execution_id, 10)));
    }
    if let Some(turn_id) = app
        .current_turn_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
    {
        parts.push(format!("turn:{}", preview(turn_id, 10)));
    }
    if let Some(detail) = app
        .current_execution_status_detail
        .as_deref()
        .filter(|detail| !detail.trim().is_empty())
    {
        parts.push(preview(detail, 24));
    }
    let now = current_time_ms();
    if let Some(started) = app.execution_started_at_ms {
        let terminal = app
            .current_execution_status
            .is_some_and(harness_contract::projection::ExecutionLiveStatus::is_terminal);
        let end = if terminal {
            app.last_progress_at_ms.unwrap_or(now)
        } else {
            now
        };
        parts.push(format!("{}s", end.saturating_sub(started) / 1_000));
    }
    if app.turn_is_active() {
        if let Some(last_progress) = app.last_progress_at_ms {
            parts.push(format!(
                "progress {}s",
                now.saturating_sub(last_progress) / 1_000
            ));
        }
    }
    parts.join(" · ")
}

fn execution_status_text(
    status: harness_contract::projection::ExecutionLiveStatus,
) -> &'static str {
    use harness_contract::projection::ExecutionLiveStatus;
    match status {
        ExecutionLiveStatus::Queued => "queued",
        ExecutionLiveStatus::PreparingContext => "context",
        ExecutionLiveStatus::CallingModel => "model",
        ExecutionLiveStatus::Thinking => "thinking",
        ExecutionLiveStatus::CallingTool => "tool",
        ExecutionLiveStatus::WaitingApproval => "approval",
        ExecutionLiveStatus::Finalizing => "finalizing",
        ExecutionLiveStatus::Complete => "complete",
        ExecutionLiveStatus::Cancelled => "cancelled",
        ExecutionLiveStatus::Error => "error",
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn truncate_cells(value: &str, max_width: usize) -> String {
    if unicode_width::UnicodeWidthStr::width(value) <= max_width {
        return value.to_string();
    }
    if max_width == 0 {
        return String::new();
    }
    let ellipsis_width = unicode_width::UnicodeWidthChar::width('…').unwrap_or(1);
    let target = max_width.saturating_sub(ellipsis_width);
    let mut output = String::new();
    let mut width = 0usize;
    for ch in value.chars() {
        let ch_width = unicode_width::UnicodeWidthChar::width(ch).unwrap_or(0);
        if width.saturating_add(ch_width) > target {
            break;
        }
        output.push(ch);
        width = width.saturating_add(ch_width);
    }
    output.push('…');
    output
}

// ── Tests ────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::RenderContext;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;

    // ── Section management ───────────────────────────────────────

    #[test]
    fn add_and_remove_section() {
        let mut bar = StatusBar::new();
        assert_eq!(bar.sections().len(), 0);

        bar.add_section(StatusSection {
            id: "test".into(),
            content: Some("hello".into()),
            style: Style::default(),
            width: SectionWidth::Fixed(10),
        });
        assert_eq!(bar.sections().len(), 1);

        let removed = bar.remove_section("test");
        assert!(removed.is_some());
        assert_eq!(bar.sections().len(), 0);
    }

    #[test]
    fn remove_nonexistent_section_returns_none() {
        let mut bar = StatusBar::new();
        assert!(bar.remove_section("nope").is_none());
    }

    // ── Default sections ─────────────────────────────────────────

    #[test]
    fn with_default_sections_has_all_parts() {
        let bar = StatusBar::with_default_sections();
        let ids: Vec<&str> = bar.sections().iter().map(|s| s.id.as_str()).collect();
        assert!(ids.contains(&"model"));
        assert!(ids.contains(&"context"));
        assert!(ids.contains(&"turn_tokens"));
        assert!(ids.contains(&"memory_stats"));
        assert!(!ids.contains(&"approvals"));
        assert!(!ids.contains(&"permission_status"));
        assert!(!ids.contains(&"version"));
        assert!(!ids.contains(&"session"));
        assert!(!ids.contains(&"focus"));
        assert!(ids.contains(&"search"));
        assert!(ids.contains(&"history"));
        assert!(!ids.contains(&"input_hint"));
    }

    // ── Notification ─────────────────────────────────────────────

    #[test]
    fn notification_set_and_clear() {
        let mut bar = StatusBar::new();
        assert!(bar.notification.is_none());

        bar.set_notification(
            "test notice",
            Style::default().fg(Color::Black).bg(Color::Cyan),
            10,
        );
        assert!(bar.notification.is_some());
        assert_eq!(bar.notification.as_ref().unwrap().0, "test notice");
        assert_eq!(bar.notification_ttl, 10);

        bar.clear_notification();
        assert!(bar.notification.is_none());
        assert_eq!(bar.notification_ttl, 0);
    }

    #[test]
    fn notification_auto_dismiss_via_tick() {
        let mut bar = StatusBar::new();
        bar.set_notification("dismiss me", Style::default(), 3);

        assert!(bar.notification.is_some());
        bar.tick();
        assert!(bar.notification.is_some());
        bar.tick();
        assert!(bar.notification.is_some());
        bar.tick();
        assert!(bar.notification.is_none());
    }

    // ── sync_from_app ────────────────────────────────────────────

    #[test]
    fn sync_from_app_populates_model_section() {
        let app = App::new("test-model", "test-session");
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let model = bar.section_mut("model").unwrap();
        assert_eq!(model.content.as_deref(), Some("test-model STD"));
    }

    #[test]
    fn sync_from_app_populates_context_section() {
        let mut app = App::new("claude-sonnet-4", "test-session");
        app.context_window_tokens = Some(200_000);
        app.context_used_tokens = Some(50_000);
        app.context_usage_percent_bp = Some(2_500);
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let section = bar.section_mut("context").unwrap();
        let content = section.content.as_deref().unwrap();
        assert!(content.contains("ctx "));
    }

    #[test]
    fn sync_from_app_shows_yolo_mode() {
        let mut app = App::new("claude-sonnet-4", "test-session");
        app.yolo_mode = true;
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let section = bar.section_mut("model").unwrap();
        let content = section.content.as_ref().unwrap();
        assert!(content.contains("YOLO"));
    }

    #[test]
    fn sync_from_app_keeps_read_only_lease_state_persistently_visible() {
        let mut app = App::new("claude-sonnet-4", "test-session");
        app.gateway_lease_mode = Some("read-only".to_string());
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let section = bar.section_mut("run_status").unwrap();
        assert_eq!(section.content.as_deref(), Some("read-only"));
    }

    #[test]
    fn sync_from_app_shows_memory_stats() {
        let mut app = App::new("claude-sonnet-4", "test-session");
        app.memory_total_entries = Some(12);
        app.memory_vector_count = Some(7);
        app.memory_layer_counts = [1, 2, 3, 4, 5];
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        let section = bar.section_mut("memory_stats").unwrap();
        let content = section.content.as_ref().unwrap();
        assert_eq!(content, "mem:12 vec:7 [1,2,3,4,5]");
    }

    #[test]
    fn default_footer_does_not_include_session_id() {
        let app = App::new("claude-sonnet-4", "test-session-abcdef");
        let mut bar = StatusBar::with_default_sections();

        bar.sync_from_app(&app);

        assert!(bar.section_mut("session").is_none());
    }

    // ── Render tests ─────────────────────────────────────────────

    #[test]
    fn render_shows_section_content() {
        let mut bar = StatusBar::new();
        bar.add_section(StatusSection {
            id: "left".into(),
            content: Some("Cowd".into()),
            style: Style::default(),
            width: SectionWidth::Fixed(6),
        });
        bar.add_section(StatusSection {
            id: "right".into(),
            content: Some("Ready".into()),
            style: Style::default(),
            width: SectionWidth::Fill,
        });

        let mut terminal = MockTerminal::new(40, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 40, 1));
        });

        terminal.assert_line_contains("Ready");
    }

    #[test]
    fn render_default_status_keeps_model_on_narrow_width() {
        let app = App::new("deepseek-v4-pro", "session-status-narrow");
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);

        let mut terminal = MockTerminal::new(88, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 88, 1));
        });

        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("deepseek-v4-pro"),
            "model should stay visible on narrow status bars: {joined}"
        );
        assert!(!joined.contains("model:"), "model prefix removed: {joined}");
    }

    #[test]
    fn forty_columns_keep_model_typed_status_and_context_visible() {
        let mut app = App::new("requested-model", "session-status-40");
        app.effective_model = Some("effective-model".to_string());
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::CallingTool);
        app.context_usage_percent_bp = Some(6_250);
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);

        let mut terminal = MockTerminal::new(40, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 40, 1));
        });

        let joined = terminal.buffer_lines().join("\n");
        assert!(joined.contains("m:"), "model identity missing: {joined}");
        assert!(joined.contains("s:tool"), "typed status missing: {joined}");
        assert!(
            joined.contains("ctx 62%"),
            "context usage missing: {joined}"
        );
    }

    #[test]
    fn render_default_status_suppresses_low_priority_sections_on_medium_width() {
        let mut app = App::new("deepseek-v4-pro", "session-status-medium");
        app.context_window_tokens = Some(200_000);
        app.context_used_tokens = Some(50_000);
        app.context_usage_percent_bp = Some(2_500);
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);

        let mut terminal = MockTerminal::new(110, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 110, 1));
        });

        let joined = terminal.buffer_lines().join("\n");
        assert!(!joined.contains("focus:"));
        assert!(joined.contains("ctx "));
        assert!(!joined.contains("approvals:"));
        assert!(!joined.contains("perm:"));
        assert!(!joined.contains("Enter send"));
    }

    #[test]
    fn render_hidden_sections_skipped() {
        let mut bar = StatusBar::new();
        bar.add_section(StatusSection {
            id: "visible".into(),
            content: Some("Hello".into()),
            style: Style::default(),
            width: SectionWidth::Fill,
        });
        bar.add_section(StatusSection {
            id: "hidden".into(),
            content: None,
            style: Style::default(),
            width: SectionWidth::Fixed(10),
        });

        let mut terminal = MockTerminal::new(40, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 40, 1));
        });

        terminal.assert_line_contains("Hello");
        let lines = terminal.buffer_lines();
        let any_hidden = lines.iter().any(|l| l.contains("hidden"));
        assert!(!any_hidden, "hidden sections should not render");
    }

    // ── Section management extras ────────────────────────────────

    #[test]
    fn section_mut_updates_content() {
        let mut bar = StatusBar::new();
        bar.add_section(StatusSection {
            id: "test".into(),
            content: Some("old".into()),
            style: Style::default(),
            width: SectionWidth::Fixed(5),
        });

        if let Some(s) = bar.section_mut("test") {
            s.content = Some("new".into());
        }
        assert_eq!(bar.sections()[0].content.as_deref(), Some("new"));
    }

    // ── fmt_tokens ───────────────────────────────────────────────

    #[test]
    fn fmt_tokens_values() {
        assert_eq!(fmt_tokens(0), "0");
        assert_eq!(fmt_tokens(999), "999");
        assert_eq!(fmt_tokens(1_000), "1k");
        assert_eq!(fmt_tokens(1_500), "1k");
        assert_eq!(fmt_tokens(9_999), "9k");
        assert_eq!(fmt_tokens(10_000), "10.0k");
        assert_eq!(fmt_tokens(12_345), "12.3k");
        assert_eq!(fmt_tokens(999_999), "1000.0k");
        assert_eq!(fmt_tokens(1_000_000), "1.0M");
        assert_eq!(fmt_tokens(1_234_567), "1.2M");
    }

    // ── Notification render ──────────────────────────────────────

    #[test]
    fn permission_status_is_not_in_footer() {
        let mut app = App::new("test-model", "test-session");
        app.permission_count = 2;
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);

        assert!(bar.section_mut("permission_status").is_none());
    }

    #[test]
    fn session_tokens_show_after_context() {
        let mut app = App::new("test-model", "test-session");
        app.input_tokens = 1200;
        app.output_tokens = 3400;
        let mut bar = StatusBar::with_default_sections();
        bar.sync_from_app(&app);

        let section = bar.section_mut("turn_tokens").unwrap();
        let content = section.content.as_deref().unwrap();
        assert!(content.contains("in:1k"));
        assert!(content.contains("out:3k"));
    }

    #[test]
    fn with_default_sections_includes_footer_sections() {
        let bar = StatusBar::with_default_sections();
        let ids: Vec<&str> = bar.sections().iter().map(|s| s.id.as_str()).collect();
        assert!(!ids.contains(&"permission_status"));
        assert!(ids.contains(&"model"), "Should have model section");
        assert!(ids.contains(&"context"), "Should have context section");
        assert!(
            ids.contains(&"memory_stats"),
            "Should have memory_stats section"
        );
    }

    #[test]
    fn render_notification_overlay() {
        let mut bar = StatusBar::new();
        bar.add_section(StatusSection {
            id: "main".into(),
            content: Some("status content".into()),
            style: Style::default(),
            width: SectionWidth::Fill,
        });
        bar.set_notification(
            "NOTICE",
            Style::default().fg(Color::Black).bg(Color::Cyan),
            5,
        );

        let mut terminal = MockTerminal::new(40, 3);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &theme);
            bar.render(&mut ctx, Rect::new(0, 0, 40, 1));
        });

        // Notification text should be visible
        terminal.assert_line_contains("NOTICE");
    }

    #[test]
    fn token_bar_returns_some_when_window_nonzero() {
        use crate::app::App;
        let mut app = App::new("test", "test-session");
        app.context_window_tokens = Some(200_000);
        app.context_used_tokens = Some(50_000);
        app.context_usage_percent_bp = Some(2_500);
        let bar = token_bar(&app);
        assert!(
            bar.is_some(),
            "token_bar should return Some when context_window > 0"
        );
        assert!(bar.unwrap().contains("25%"), "should show 25% usage");
    }

    #[test]
    fn token_bar_returns_none_when_window_zero() {
        use crate::app::App;
        let app = App::new("test", "test-session");
        assert!(
            token_bar(&app).is_none(),
            "token_bar should return None when context_window == 0"
        );
    }
}
