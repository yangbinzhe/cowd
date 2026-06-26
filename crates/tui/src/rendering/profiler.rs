// ── Render Profiler — Frame timing, render cache, CPU budget ─────
// Tracks per-frame render times, optionally skips re-renders when
// msg_version is unchanged, and provides a simple timing logger.
//
// Idle target: <5% CPU, frame times <16ms (60fps).
// Streaming target: <30fps budget.
// -------------------------------------------------------------------

#![allow(dead_code)]

use std::time::{Duration, Instant};

/// Tracks timing data for a single render frame.
#[derive(Debug, Clone, Copy)]
pub struct FrameStats {
    /// Wall-clock time spent inside the render call.
    pub render_us: u64,
    /// Whether this frame performed actual rendering (not skipped).
    pub did_render: bool,
    /// Number of consecutive skipped frames before this one.
    pub skipped_frames: u32,
}

/// Accumulated profiling data over a measurement window.
#[derive(Debug, Clone)]
pub struct ProfilerSnapshot {
    pub total_frames: u64,
    pub rendered_frames: u64,
    pub skipped_frames: u64,
    pub avg_render_us: u64,
    pub max_render_us: u64,
    pub window_duration_ms: u64,
}

// ── FrameTimer ───────────────────────────────────────────────────

/// Low-overhead frame timer with render-skip optimization.
///
/// Usage in the main loop:
/// ```ignore
/// let mut timer = FrameTimer::new();
/// loop {
///     if timer.should_render(msg_version, last_drawn_version) {
///         terminal.draw(|f| state.render(f))?;
///         timer.mark_rendered();
///     }
///     timer.end_frame();
/// }
/// ```
pub struct FrameTimer {
    /// Instant when the current frame began.
    frame_start: Instant,
    /// Instant when the last render call happened.
    last_render: Instant,
    /// Consecutive frames skipped since last render.
    skip_count: u32,
    /// Rolling average of render times in microseconds.
    rolling_avg_us: f64,
    /// Maximum render time observed this window.
    max_us: u64,
    /// Total frames elapsed.
    total_frames: u64,
    /// Total frames actually rendered.
    rendered_frames: u64,
    /// Total frames skipped.
    total_skipped: u64,
    /// Start of current profiling window.
    window_start: Instant,
    /// Whether profiling logging is enabled.
    logging: bool,
}

impl FrameTimer {
    /// Create a new frame timer, starting the first frame now.
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            frame_start: now,
            last_render: now,
            skip_count: 0,
            rolling_avg_us: 0.0,
            max_us: 0,
            total_frames: 0,
            rendered_frames: 0,
            total_skipped: 0,
            window_start: now,
            logging: false,
        }
    }

    /// Enable or disable periodic profiling log output.
    pub fn set_logging(&mut self, enabled: bool) {
        self.logging = enabled;
    }

    /// Decide whether to render this frame based on version comparison
    /// and throttle budget.
    ///
    /// Returns `true` if:
    /// - The message version has changed since last draw, OR
    /// - Sufficient time has passed since last render (>budget).
    ///
    /// `msg_version` is the current App message version.
    /// `last_drawn_version` is updated by the caller after drawing.
    /// `budget` is the minimum interval between renders.
    pub fn should_render(
        &mut self,
        msg_version: u64,
        last_drawn_version: u64,
        budget: Duration,
    ) -> bool {
        use std::cmp::Ordering;
        match msg_version.cmp(&last_drawn_version) {
            Ordering::Equal => {
                // Version unchanged — skip if within budget
                if self.last_render.elapsed() < budget {
                    self.skip_count += 1;
                    return false;
                }
                true
            }
            _ => {
                // Version changed — always render
                true
            }
        }
    }

    /// Mark the current frame as having performed a render call.
    /// Must be called after `terminal.draw()` completes.
    pub fn mark_rendered(&mut self) {
        let elapsed = self.frame_start.elapsed();
        let us = elapsed.as_micros() as u64;

        // EWMA: 90% old, 10% new
        self.rolling_avg_us = self.rolling_avg_us * 0.9 + (us as f64) * 0.1;
        self.max_us = self.max_us.max(us);
        self.rendered_frames += 1;
        self.skip_count = 0;
        self.last_render = Instant::now();
    }

    /// End the current frame, updating counters. Call at the bottom of
    /// the event loop iteration.
    pub fn end_frame(&mut self) {
        self.total_frames += 1;
        self.total_skipped += self.skip_count as u64;
        self.frame_start = Instant::now();

        // Log profiling snapshots every ~5 seconds if logging is enabled
        if self.logging && self.window_start.elapsed() > Duration::from_secs(5) {
            let snap = self.snapshot();
            eprintln!(
                "[cowd profiler] frames={} rendered={} skipped={} avg_render={}us max={}us",
                snap.total_frames,
                snap.rendered_frames,
                snap.skipped_frames,
                snap.avg_render_us,
                snap.max_render_us,
            );
            self.max_us = 0;
            self.window_start = Instant::now();
        }
    }

    /// Get a snapshot of accumulated profiling data.
    pub fn snapshot(&self) -> ProfilerSnapshot {
        ProfilerSnapshot {
            total_frames: self.total_frames,
            rendered_frames: self.rendered_frames,
            skipped_frames: self.total_skipped,
            avg_render_us: self.rolling_avg_us as u64,
            max_render_us: self.max_us,
            window_duration_ms: self.window_start.elapsed().as_millis() as u64,
        }
    }

    /// Rolling average render time in microseconds.
    pub fn avg_render_us(&self) -> u64 {
        self.rolling_avg_us as u64
    }

    /// Maximum render time this window in microseconds.
    pub fn max_render_us(&self) -> u64 {
        self.max_us
    }
}

impl Default for FrameTimer {
    fn default() -> Self {
        Self::new()
    }
}

// ── RenderProfiler ────────────────────────────────────────────────

/// Per-component render timing logger.
///
/// Use RAII guard pattern to time individual component renders:
/// ```ignore
/// let _guard = profiler.guard("chat_view");
/// chat_view.render(&mut ctx, area);
/// // guard drops here, recording elapsed time
/// ```
pub struct RenderProfiler {
    enabled: bool,
}

/// RAII guard that records elapsed time on drop.
pub struct ProfilerGuard<'a> {
    name: &'static str,
    start: Instant,
    profiler: &'a RenderProfiler,
}

impl<'a> Drop for ProfilerGuard<'a> {
    fn drop(&mut self) {
        if self.profiler.enabled {
            let us = self.start.elapsed().as_micros() as u64;
            // Log slow renders (>1ms) as warnings
            if us > 1000 {
                eprintln!(
                    "[cowd perf] component '{}' render: {}us (WARN: >1ms)",
                    self.name, us
                );
            }
        }
    }
}

impl RenderProfiler {
    /// Create a new profiler. Enable with `set_enabled(true)`.
    pub fn new() -> Self {
        Self { enabled: false }
    }

    /// Enable or disable component-level profiling.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Start timing a component render. Returns a guard that auto-records
    /// on drop.
    pub fn guard<'a>(&'a self, component_name: &'static str) -> ProfilerGuard<'a> {
        ProfilerGuard {
            name: component_name,
            start: Instant::now(),
            profiler: self,
        }
    }

    /// Whether profiling is active.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Default for RenderProfiler {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_timer_new_starts_counting() {
        let timer = FrameTimer::new();
        assert_eq!(timer.total_frames, 0);
        assert_eq!(timer.rendered_frames, 0);
        assert_eq!(timer.total_skipped, 0);
    }

    #[test]
    fn should_render_when_version_changed() {
        let mut timer = FrameTimer::new();
        // Version 5 != last_drawn 3 → should render
        assert!(timer.should_render(5, 3, Duration::from_millis(16)));
    }

    #[test]
    fn should_skip_when_version_unchanged_within_budget() {
        let mut timer = FrameTimer::new();
        timer.mark_rendered();
        // Same version, within budget → skip
        assert!(!timer.should_render(5, 5, Duration::from_millis(100)));
    }

    #[test]
    fn should_render_when_version_unchanged_but_budget_exceeded() {
        let mut timer = FrameTimer::new();
        timer.last_render = Instant::now()
            .checked_sub(Duration::from_millis(200))
            .unwrap_or_else(Instant::now);
        // Same version, but budget 16ms exceeded → render anyway
        assert!(timer.should_render(5, 5, Duration::from_millis(16)));
    }

    #[test]
    fn end_frame_increments_counters() {
        let mut timer = FrameTimer::new();
        timer.end_frame();
        assert_eq!(timer.total_frames, 1);
    }

    #[test]
    fn mark_rendered_resets_skip_count() {
        let mut timer = FrameTimer::new();
        timer.should_render(5, 3, Duration::from_millis(16));
        timer.should_render(3, 3, Duration::from_millis(16));
        timer.should_render(3, 3, Duration::from_millis(16));
        // Two skipped frames
        timer.mark_rendered();
        assert_eq!(timer.skip_count, 0);
        assert_eq!(timer.rendered_frames, 1);
    }

    #[test]
    fn snapshot_counts_correct() {
        let mut timer = FrameTimer::new();
        timer.mark_rendered();
        timer.end_frame();
        // Simulate a skipped frame: version unchanged, within budget
        assert!(!timer.should_render(5, 5, Duration::from_millis(100)));
        timer.end_frame();
        let snap = timer.snapshot();
        assert_eq!(snap.total_frames, 2);
        assert_eq!(snap.rendered_frames, 1);
        assert_eq!(snap.skipped_frames, 1);
    }

    #[test]
    fn profiler_guard_disabled_does_nothing() {
        let profiler = RenderProfiler::new();
        assert!(!profiler.is_enabled());
        // Guard drops silently when profiling is disabled
        {
            let _guard = profiler.guard("test");
        }
    }

    #[test]
    fn profiler_guard_enabled() {
        let mut profiler = RenderProfiler::new();
        profiler.set_enabled(true);
        assert!(profiler.is_enabled());
        {
            let _guard = profiler.guard("fast_component");
            // <1ms → no warning emitted
        }
    }
}
