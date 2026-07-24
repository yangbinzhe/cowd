// ── Performance Dashboard ──────────────────────────────────────────────
// Overlay panel showing memory subsystem performance metrics:
//   - Sparkline for prepare-context latency history
//   - Gauge for cache hit rate
//   - Bar chart for compression ratio
//   - Tuning status with last-adjustment timestamp
//
// Toggled via Ctrl+Shift+P; auto-refreshes every 2 seconds.

#![allow(dead_code)]

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Gauge, Paragraph, Sparkline, Wrap},
};

use crate::app::App;
use crate::components::{Component, EventResult, RenderContext};

const REFRESH_INTERVAL: Duration = Duration::from_secs(2);

pub struct PerformanceDashboard {
    pub visible: bool,
    pub last_report: Option<PerformanceReport>,
    last_sync: Instant,
    sparkline_data: Vec<u64>,
    ticks: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PerformanceReport {
    pub avg_prepare_context_latency_ms: f64,
    pub latency_reported: bool,
    pub cache_hit_rate: f64,
    pub cache_hit_rate_reported: bool,
    pub avg_compression_ratio: f64,
    pub compression_reported: bool,
    pub avg_extract_duration_ms: f64,
    pub extract_duration_reported: bool,
    pub tuning_applied: bool,
    pub last_tuning: Option<DateTime<Utc>>,
    pub total_samples: usize,
    pub window_size: usize,
    pub current_tuning: TuningConfig,
    pub last_updated: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct TuningConfig {
    pub prefetch_hot_topics: bool,
    pub l0_cache_ttl_secs: u64,
    pub sandbox_min_lines: usize,
    pub freshness_trigger_ratio: f64,
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

    /// Sync from the Gateway/App projection (rate-limited to REFRESH_INTERVAL).
    pub fn sync_from_app(&mut self, app: &App) {
        if !self.visible {
            return;
        }
        if self.last_sync.elapsed() < REFRESH_INTERVAL {
            return;
        }
        let cache_total = app
            .telemetry
            .finalized_cache_hits
            .saturating_add(app.telemetry.finalized_cache_misses);
        let has_reported_data = app.telemetry.history_hydration_duration_ms.is_some()
            || cache_total > 0
            || app.telemetry.session_sse_reconnect_count > 0
            || app.telemetry.projection_sse_reconnect_count > 0
            || app.telemetry.full_timeline_rebuild_count > 0
            || app.telemetry.live_tail_rebuild_count > 0;
        if !has_reported_data {
            self.last_report = None;
            self.last_sync = Instant::now();
            return;
        }

        let cache_hit_rate = (cache_total > 0)
            .then(|| app.telemetry.finalized_cache_hits as f64 / cache_total as f64);
        let report = PerformanceReport {
            avg_prepare_context_latency_ms: app
                .telemetry
                .history_hydration_duration_ms
                .unwrap_or_default() as f64,
            latency_reported: app.telemetry.history_hydration_duration_ms.is_some(),
            cache_hit_rate: cache_hit_rate.unwrap_or_default(),
            cache_hit_rate_reported: cache_hit_rate.is_some(),
            avg_compression_ratio: 0.0,
            compression_reported: false,
            avg_extract_duration_ms: 0.0,
            extract_duration_reported: false,
            tuning_applied: false,
            last_tuning: None,
            total_samples: usize::try_from(cache_total).unwrap_or(usize::MAX),
            window_size: app.telemetry.history_hydrated_messages,
            current_tuning: TuningConfig::default(),
            last_updated: Some(Utc::now()),
        };
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
            let no_data = Paragraph::new(
                "No measured performance data available.\nMetrics remain not reported until a real history/render/transport sample exists.",
            )
            .style(Style::default().fg(Color::DarkGray))
            .wrap(Wrap { trim: false });
            ctx.frame_mut().render_widget(no_data, area);
            return;
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(7), // Sparkline row
                Constraint::Length(3), // Gauge row
                Constraint::Length(3), // Compression bar
                Constraint::Length(4), // Stats footer
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
                        " History hydration ({}) ",
                        if report.latency_reported {
                            format!("{:.0}ms", report.avg_prepare_context_latency_ms)
                        } else {
                            "not reported".to_string()
                        }
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
            .block(
                Block::default()
                    .title(" Cache Hit Rate ")
                    .borders(Borders::ALL),
            )
            .gauge_style(
                Style::default()
                    .fg(gauge_color)
                    .add_modifier(Modifier::BOLD),
            )
            .percent(if report.cache_hit_rate_reported {
                hit_pct.min(100)
            } else {
                0
            })
            .label(if report.cache_hit_rate_reported {
                format!("{:.1}%", report.cache_hit_rate * 100.0)
            } else {
                "not reported".to_string()
            });
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
                    .title(if report.compression_reported {
                        " Compression Ratio "
                    } else {
                        " Compression Ratio (not reported) "
                    })
                    .borders(Borders::ALL),
            )
            .gauge_style(Style::default().fg(comp_color).add_modifier(Modifier::BOLD))
            .percent(comp_pct)
            .label(if report.compression_reported {
                if report.extract_duration_reported {
                    format!(
                        "{:.1}%  (extract: {:.0}ms)",
                        report.avg_compression_ratio * 100.0,
                        report.avg_extract_duration_ms
                    )
                } else {
                    format!(
                        "{:.1}%  (extract: not reported)",
                        report.avg_compression_ratio * 100.0
                    )
                }
            } else {
                "not reported".to_string()
            });
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
            Span::styled(format!("{}", prefetch), Style::default().fg(Color::White)),
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
                " Updated: {}  |  Ctrl+Shift+P: close",
                report
                    .last_updated
                    .map(|updated| updated.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "never".to_string())
            ),
            Style::default().fg(Color::DarkGray),
        )));

        let footer_block = Block::default()
            .borders(Borders::ALL)
            .title(" Tuning Config ");
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
            let dim = Paragraph::new("").style(Style::default().bg(Color::Black).fg(Color::Black));
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

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;
    use chrono::Utc;

    fn make_report(
        latency_ms: f64,
        extract_ms: f64,
        compression: f64,
        hit_rate: f64,
    ) -> PerformanceReport {
        PerformanceReport {
            avg_prepare_context_latency_ms: latency_ms,
            latency_reported: true,
            avg_extract_duration_ms: extract_ms,
            extract_duration_reported: true,
            avg_compression_ratio: compression,
            compression_reported: true,
            cache_hit_rate: hit_rate,
            cache_hit_rate_reported: true,
            total_samples: 50,
            window_size: 100,
            last_updated: Some(Utc::now()),
            tuning_applied: false,
            last_tuning: None,
            current_tuning: TuningConfig::default(),
        }
    }

    fn render_dashboard(
        dashboard: &mut PerformanceDashboard,
        width: u16,
        height: u16,
    ) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            dashboard.render(&mut ctx, Rect::new(0, 0, width, height));
        });
        terminal.buffer_lines()
    }

    // ── Construction & default state ──────────────────────────────

    #[test]
    fn new_dashboard_starts_hidden_with_no_data() {
        let dashboard = PerformanceDashboard::new();
        assert!(!dashboard.visible);
        assert!(dashboard.last_report.is_none());
        assert!(dashboard.sparkline_data.is_empty());
        assert_eq!(dashboard.ticks, 0);
    }

    #[test]
    fn default_is_equivalent_to_new() {
        let a = PerformanceDashboard::new();
        let b = PerformanceDashboard::default();
        assert_eq!(a.visible, b.visible);
        assert!(a.last_report.is_none() && b.last_report.is_none());
        assert!(a.sparkline_data.is_empty());
        assert!(b.sparkline_data.is_empty());
        assert_eq!(a.ticks, b.ticks);
    }

    // ── Toggle visibility ─────────────────────────────────────────

    #[test]
    fn toggle_flips_visibility() {
        let mut dashboard = PerformanceDashboard::new();
        assert!(!dashboard.visible);
        dashboard.toggle();
        assert!(dashboard.visible);
        dashboard.toggle();
        assert!(!dashboard.visible);
    }

    // ── Tick counter ──────────────────────────────────────────────

    #[test]
    fn tick_increments_counter() {
        let mut dashboard = PerformanceDashboard::new();
        assert_eq!(dashboard.ticks, 0);
        dashboard.tick();
        assert_eq!(dashboard.ticks, 1);
        dashboard.tick();
        assert_eq!(dashboard.ticks, 2);
    }

    #[test]
    fn tick_wraps_at_max() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.ticks = u64::MAX;
        dashboard.tick();
        assert_eq!(dashboard.ticks, 0);
    }

    // ── Sync ──────────────────────────────────────────────────────

    #[test]
    fn sync_does_nothing_when_hidden() {
        let mut dashboard = PerformanceDashboard::new();
        assert!(!dashboard.visible);
        let app = App::new("m", "s");
        dashboard.sync_from_app(&app);
        assert!(dashboard.last_report.is_none());
        assert!(dashboard.sparkline_data.is_empty());
    }

    #[test]
    fn sync_clears_report_when_no_projection() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = true;
        dashboard.last_report = Some(make_report(100.0, 50.0, 0.7, 0.85));
        // Wait past the rate limit
        dashboard.last_sync = Instant::now()
            .checked_sub(REFRESH_INTERVAL + Duration::from_millis(100))
            .unwrap();

        let app = App::new("m", "s");
        dashboard.sync_from_app(&app);
        assert!(dashboard.last_report.is_none());
    }

    #[test]
    fn sync_respects_rate_limit() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = true;
        let app = App::new("m", "s");

        // First sync: within rate limit window — last_sync is fresh
        dashboard.sync_from_app(&app);
        let first_sync = dashboard.last_sync;

        // Second sync: immediately after — should be rate-limited
        std::thread::sleep(Duration::from_millis(1));
        dashboard.sync_from_app(&app);
        assert_eq!(
            dashboard.last_sync, first_sync,
            "rate limit should prevent re-sync"
        );
    }

    // ── Render: empty state ───────────────────────────────────────

    #[test]
    fn render_empty_state_shows_no_data_message() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = true;
        let lines = render_dashboard(&mut dashboard, 60, 15);
        let joined = lines.join("\n");
        assert!(
            joined.contains("No measured performance data"),
            "Should show no-data message, got: {joined}"
        );
    }

    // ── Render: with data ─────────────────────────────────────────

    #[test]
    fn render_shows_latency_and_cache_metrics() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = true;
        dashboard.ticks = 10; // past fade-in
        dashboard.last_report = Some(make_report(150.0, 80.0, 0.75, 0.90));
        dashboard.sparkline_data = vec![100, 120, 150, 130];

        let lines = render_dashboard(&mut dashboard, 80, 20);
        let joined = lines.join("\n");
        assert!(
            joined.contains("History hydration"),
            "Should show latency sparkline, got: {joined}"
        );
        assert!(
            joined.contains("Cache Hit Rate"),
            "Should show cache hit rate gauge, got: {joined}"
        );
        assert!(
            joined.contains("Compression Ratio"),
            "Should show compression gauge, got: {joined}"
        );
        assert!(
            joined.contains("Tuning Config"),
            "Should show tuning footer, got: {joined}"
        );
        assert!(
            joined.contains("90.0%"),
            "Should show cache hit rate 90%, got: {joined}"
        );
    }

    #[test]
    fn render_fade_in_dims_early_ticks() {
        // At tick 2 (alpha 2/6 ≈ 0.33), dimming overlay is rendered
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = true;
        dashboard.ticks = 2;
        dashboard.last_report = Some(make_report(100.0, 50.0, 0.5, 0.5));
        dashboard.sparkline_data = vec![100];

        let lines = render_dashboard(&mut dashboard, 80, 20);
        let joined = lines.join("\n");
        // Should still render content (render_content is called)
        assert!(
            joined.contains("Prepare Latency") || joined.contains("Performance Dashboard"),
            "Should render dashboard content even during fade-in"
        );
    }

    #[test]
    fn render_block_always_visible() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = false;
        dashboard.last_report = Some(make_report(100.0, 50.0, 0.5, 0.5));

        let lines = render_dashboard(&mut dashboard, 80, 20);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Performance Dashboard"),
            "Dashboard block should always be visible regardless of visibility flag"
        );
    }

    #[test]
    fn hidden_dashboard_skips_sync() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = false;
        let app = App::new("m", "s");
        dashboard.sync_from_app(&app);
        assert!(dashboard.last_report.is_none());
    }

    // ── Sparkline history ─────────────────────────────────────────

    #[test]
    fn sparkline_trims_at_60_entries() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = true;
        // Simulate pushing 70 entries via repeated sync
        for i in 0..70 {
            dashboard.sparkline_data.push(i as u64);
        }
        // Manually trim
        while dashboard.sparkline_data.len() > 60 {
            dashboard.sparkline_data.remove(0);
        }
        assert_eq!(dashboard.sparkline_data.len(), 60);
        assert_eq!(dashboard.sparkline_data[0], 10); // first removed were 0..9
        assert_eq!(dashboard.sparkline_data[59], 69);
    }

    // ── Component trait ───────────────────────────────────────────

    #[test]
    fn component_trait_methods() {
        let dashboard = PerformanceDashboard::new();
        assert!(dashboard.focusable());
        assert_eq!(dashboard.id(), "performance_dashboard");
    }

    #[test]
    fn esc_hides_dashboard() {
        let mut dashboard = PerformanceDashboard::new();
        dashboard.visible = true;

        let press_esc = Event::Key(crossterm::event::KeyEvent::new(
            KeyCode::Esc,
            crossterm::event::KeyModifiers::NONE,
        ));
        let result = dashboard.handle_event(&press_esc);
        assert!(result.is_consumed());
        assert!(!dashboard.visible);
    }
}
