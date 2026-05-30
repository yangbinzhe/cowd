// ── Performance Dashboard ──────────────────────────────────────────────
// Overlay panel showing memory subsystem performance metrics:
//   - Sparkline for prepare-context latency history
//   - Gauge for cache hit rate
//   - Bar chart for compression ratio
//   - Tuning status with last-adjustment timestamp
//
// Toggled via Ctrl+P; auto-refreshes every 2 seconds.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{Event, KeyCode, KeyEventKind};
use memory::{MemoryOrchestrator, PerformanceReport};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Gauge, Paragraph, Sparkline, Wrap},
};

use crate::tui::components::{Component, EventResult, RenderContext};

const REFRESH_INTERVAL: Duration = Duration::from_secs(2);

pub struct PerformanceDashboard {
    pub visible: bool,
    pub last_report: Option<PerformanceReport>,
    last_sync: Instant,
    sparkline_data: Vec<u64>,
    ticks: u64,
}

impl PerformanceDashboard {
    pub fn new() -> Self {
        Self {
            visible: false,
            last_report: None,
            last_sync: Instant::now(),
            sparkline_data: Vec::new(),
            ticks: 0,
        }
    }

    pub fn toggle(&mut self) {
        self.visible = !self.visible;
    }

    pub fn tick(&mut self) {
        self.ticks = self.ticks.wrapping_add(1);
    }

    /// Sync from the memory orchestrator (rate-limited to REFRESH_INTERVAL).
    pub fn sync(&mut self, orchestrator: &Option<Arc<MemoryOrchestrator>>) {
        if !self.visible {
            return;
        }
        if self.last_sync.elapsed() < REFRESH_INTERVAL {
            return;
        }
        let Some(ref orch) = orchestrator else {
            self.last_report = None;
            self.last_sync = Instant::now();
            return;
        };

        let report = orch.performance_report();
        self.last_sync = Instant::now();

        // Update sparkline history (keep last 60 data points)
        let latency = report.avg_prepare_context_latency_ms as u64;
        self.sparkline_data.push(latency);
        if self.sparkline_data.len() > 60 {
            self.sparkline_data.remove(0);
        }

        self.last_report = Some(report);
    }

    // ── Rendering ──────────────────────────────────────────────────────

    fn render_content(&self, area: Rect, ctx: &mut RenderContext) {
        let Some(ref report) = self.last_report else {
            let no_data = Paragraph::new("No performance data available.\nStart a session to collect metrics.")
                .style(Style::default().fg(Color::DarkGray))
                .wrap(Wrap { trim: false });
            ctx.frame_mut().render_widget(no_data, area);
            return;
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(7),  // Sparkline row
                Constraint::Length(3),  // Gauge row
                Constraint::Length(3),  // Compression bar
                Constraint::Length(4),  // Stats footer
            ])
            .split(area);

        // ── Row 1: Latency Sparkline ────────────────────────────────
        let spark_data: Vec<u64> = if self.sparkline_data.is_empty() {
            vec![0]
        } else {
            self.sparkline_data.clone()
        };
        let max_val = spark_data.iter().max().copied().unwrap_or(1).max(1);

        let sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title(format!(
                        " Prepare Latency (avg: {:.0}ms) ",
                        report.avg_prepare_context_latency_ms
                    ))
                    .borders(Borders::ALL),
            )
            .data(&spark_data)
            .max(max_val)
            .style(Style::default().fg(Color::Cyan));
        ctx.frame_mut().render_widget(sparkline, chunks[0]);

        // ── Row 2: Cache Hit Rate Gauge ─────────────────────────────
        let hit_pct = (report.cache_hit_rate * 100.0) as u16;
        let gauge_color = if hit_pct >= 75 {
            Color::Green
        } else if hit_pct >= 40 {
            Color::Yellow
        } else {
            Color::Red
        };
        let gauge = Gauge::default()
            .block(Block::default().title(" Cache Hit Rate ").borders(Borders::ALL))
            .gauge_style(Style::default().fg(gauge_color).add_modifier(Modifier::BOLD))
            .percent(hit_pct.min(100))
            .label(format!("{:.1}%", report.cache_hit_rate * 100.0));
        ctx.frame_mut().render_widget(gauge, chunks[1]);

        // ── Row 3: Compression Ratio Bar ────────────────────────────
        let comp_pct = (report.avg_compression_ratio * 100.0).min(100.0) as u16;
        let comp_color = if comp_pct >= 60 {
            Color::Green
        } else if comp_pct >= 30 {
            Color::Yellow
        } else {
            Color::Red
        };
        let comp_gauge = Gauge::default()
            .block(
                Block::default()
                    .title(" Compression Ratio ")
                    .borders(Borders::ALL),
            )
            .gauge_style(Style::default().fg(comp_color).add_modifier(Modifier::BOLD))
            .percent(comp_pct)
            .label(format!(
                "{:.1}%  (extract: {:.0}ms)",
                report.avg_compression_ratio * 100.0,
                report.avg_extract_duration_ms
            ));
        ctx.frame_mut().render_widget(comp_gauge, chunks[2]);

        // ── Row 4: Stats Footer ─────────────────────────────────────
        let mut lines: Vec<Line> = Vec::new();

        let tuning_status = if report.tuning_applied {
            Span::styled("ACTIVE", Style::default().fg(Color::Green))
        } else {
            Span::styled("IDLE", Style::default().fg(Color::DarkGray))
        };

        let last_tuning_str = match report.last_tuning {
            Some(ref dt) => format!("{}", dt.format("%H:%M:%S")),
            None => "never".to_string(),
        };

        lines.push(Line::from(vec![
            Span::styled(" Samples: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", report.total_samples),
                Style::default().fg(Color::White),
            ),
            Span::styled("  Window: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", report.window_size),
                Style::default().fg(Color::White),
            ),
            Span::styled("  Tuning: ", Style::default().fg(Color::DarkGray)),
            tuning_status,
            Span::styled(
                format!(" (last: {})", last_tuning_str),
                Style::default().fg(Color::DarkGray),
            ),
        ]));

        let prefetch = report.current_tuning.prefetch_hot_topics;
        let l0_ttl = report.current_tuning.l0_cache_ttl_secs;
        let sandbox = report.current_tuning.sandbox_min_lines;
        let freshness = report.current_tuning.freshness_trigger_ratio;

        lines.push(Line::from(vec![
            Span::styled(" Prefetch: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", prefetch),
                Style::default().fg(Color::White),
            ),
            Span::styled(
                format!("  L0 TTL: {}s", l0_ttl),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                format!("  Sandbox: {} lines", sandbox),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                format!("  Freshness: {:.2}", freshness),
                Style::default().fg(Color::DarkGray),
            ),
        ]));

        lines.push(Line::from(Span::styled(
            format!(
                " Updated: {}  |  Ctrl+P: close",
                report.last_updated.format("%H:%M:%S")
            ),
            Style::default().fg(Color::DarkGray),
        )));

        let footer_block = Block::default().borders(Borders::ALL).title(" Tuning Config ");
        let footer = Paragraph::new(Text::from(lines))
            .block(footer_block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(footer, chunks[3]);
    }
}

// ── Default ──────────────────────────────────────────────────────────

impl Default for PerformanceDashboard {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component Trait ──────────────────────────────────────────────────

impl Component for PerformanceDashboard {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Performance Dashboard ")
            .border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(area);
        ctx.frame_mut().render_widget(block, area);

        // Fade-in animation: start transparent, fade to full opacity over 6 ticks
        let alpha = (self.ticks.min(6) as f32 / 6.0).min(1.0);
        if alpha < 1.0 {
            // Simple approach: skip rendering at low alpha (flicker-free fade-in)
            self.render_content(inner, ctx);
            // Overlay a dimming effect for fade
            let dim = Paragraph::new("")
                .style(
                    Style::default()
                        .bg(Color::Black)
                        .fg(Color::Black),
                );
            if alpha < 0.5 {
                ctx.frame_mut().render_widget(dim, inner);
            }
        } else {
            self.render_content(inner, ctx);
        }
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        if key.kind != KeyEventKind::Press {
            return EventResult::NotConsumed;
        }
        if key.code == KeyCode::Esc {
            self.visible = false;
            return EventResult::Consumed;
        }
        EventResult::NotConsumed
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "performance_dashboard"
    }
}
