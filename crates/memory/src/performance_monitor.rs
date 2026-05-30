//! Performance monitoring and auto-tuning for the memory subsystem.
//!
//! Provides [`PerformanceMonitor`] for tracking rolling-window metrics,
//! [`AutoTuner`] for adjusting [`TuningConfig`] based on observed performance,
//! and [`PerformanceReport`] for JSON-serializable reporting.
//!
//! # Architecture
//!
//! ```text
//! CognitiveContextManager
//!   ├── PerformanceMonitor  ← records latencies, ratios, cache hits
//!   └── AutoTuner           ← reads monitor, adjusts TuningConfig
//! ```
//!
//! The monitor is write-mostly (cheap atomic ops + bounded VecDeque).
//! The tuner is read-heavy and rate-limited (every 5 min by default).

use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;

use crate::config::TuningConfig;

// ── Constants ──────────────────────────────────────────────────────────────

/// Default rolling window size for performance metrics.
const DEFAULT_WINDOW_SIZE: usize = 100;

/// Minimum number of samples before AutoTuner makes adjustments.
const DEFAULT_MIN_SAMPLES: usize = 30;

/// How often the AutoTuner re-evaluates (seconds).
const DEFAULT_TUNING_INTERVAL_SECS: u64 = 300; // 5 minutes

// ── PerformanceReport ──────────────────────────────────────────────────────

/// JSON-serializable performance snapshot for the memory subsystem.
#[derive(Debug, Clone, Serialize, Default)]
pub struct PerformanceReport {
    /// Rolling average latency of `prepare_context` (ms).
    pub avg_prepare_context_latency_ms: f64,
    /// Rolling average duration of `extract_and_remember` (ms).
    pub avg_extract_duration_ms: f64,
    /// Rolling average compression ratio (e.g., output_tokens / input_tokens).
    pub avg_compression_ratio: f64,
    /// Cache hit rate (0.0 – 1.0) across L0/L1/L2 layers.
    pub cache_hit_rate: f64,
    /// Total number of metric samples recorded.
    pub total_samples: usize,
    /// Rolling window capacity.
    pub window_size: usize,
    /// Timestamp of the last metric update.
    pub last_updated: DateTime<Utc>,
    /// Whether the auto-tuner has ever applied adjustments.
    pub tuning_applied: bool,
    /// When the last tuning adjustment occurred.
    pub last_tuning: Option<DateTime<Utc>>,
    /// Current tuning-configuration values.
    pub current_tuning: TuningConfig,
}

// ── PerformanceMonitor ─────────────────────────────────────────────────────

/// Rolling-window performance metrics collector.
///
/// Thread-safe: all mutable state uses interior mutability
/// (`AtomicU64` / `parking_lot::Mutex`), so all recording methods take `&self`.
pub struct PerformanceMonitor {
    prepare_context_latencies: Mutex<VecDeque<f64>>,
    extract_durations: Mutex<VecDeque<f64>>,
    compression_ratios: Mutex<VecDeque<f64>>,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    window_size: usize,
    total_calls: AtomicU64,
    last_updated: Mutex<DateTime<Utc>>,
}

impl PerformanceMonitor {
    /// Create a new monitor with a fixed rolling window size.
    pub fn new(window_size: usize) -> Self {
        Self {
            prepare_context_latencies: Mutex::new(VecDeque::with_capacity(window_size + 1)),
            extract_durations: Mutex::new(VecDeque::with_capacity(window_size + 1)),
            compression_ratios: Mutex::new(VecDeque::with_capacity(window_size + 1)),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            window_size,
            total_calls: AtomicU64::new(0),
            last_updated: Mutex::new(Utc::now()),
        }
    }

    /// Record the wall-clock latency of a `prepare_context` call.
    pub fn record_prepare_context(&self, latency: Duration) {
        let ms = latency.as_secs_f64() * 1000.0;
        let buf = self.prepare_context_latencies.lock();
        Self::push_rolling(buf, ms, self.window_size);
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        *self.last_updated.lock() = Utc::now();
    }

    /// Record the wall-clock duration of an `extract` call.
    pub fn record_extract(&self, duration: Duration) {
        let ms = duration.as_secs_f64() * 1000.0;
        let buf = self.extract_durations.lock();
        Self::push_rolling(buf, ms, self.window_size);
    }

    /// Record a compression ratio (output tokens ÷ input tokens).
    pub fn record_compression_ratio(&self, ratio: f64) {
        let buf = self.compression_ratios.lock();
        Self::push_rolling(buf, ratio, self.window_size);
    }

    /// Record a cache hit.
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// Rolling average of `prepare_context` latencies (ms).
    pub fn avg_prepare_context_latency_ms(&self) -> f64 {
        Self::avg(&self.prepare_context_latencies.lock())
    }

    /// Rolling average of `extract` durations (ms).
    pub fn avg_extract_duration_ms(&self) -> f64 {
        Self::avg(&self.extract_durations.lock())
    }

    /// Rolling average compression ratio.
    pub fn avg_compression_ratio(&self) -> f64 {
        Self::avg(&self.compression_ratios.lock())
    }

    /// Cache hit rate (0.0 – 1.0). Returns 0.0 when no cache operations recorded.
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Total number of samples recorded across all metric types.
    pub fn total_samples(&self) -> u64 {
        self.total_calls.load(Ordering::Relaxed)
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    /// Build a [`PerformanceReport`] snapshot.
    pub fn report(
        &self,
        tuning_config: &TuningConfig,
        tuning_applied: bool,
        last_tuning: Option<DateTime<Utc>>,
    ) -> PerformanceReport {
        PerformanceReport {
            avg_prepare_context_latency_ms: self.avg_prepare_context_latency_ms(),
            avg_extract_duration_ms: self.avg_extract_duration_ms(),
            avg_compression_ratio: self.avg_compression_ratio(),
            cache_hit_rate: self.cache_hit_rate(),
            total_samples: self.total_samples() as usize,
            window_size: self.window_size,
            last_updated: *self.last_updated.lock(),
            tuning_applied,
            last_tuning,
            current_tuning: tuning_config.clone(),
        }
    }

    // ── Internal helpers ────────────────────────────────────────────────

    /// Push a value into a rolling-window deque (O(1) amortized).
    fn push_rolling(mut buf: parking_lot::MutexGuard<'_, VecDeque<f64>>, val: f64, cap: usize) {
        if buf.len() >= cap {
            let _ = buf.pop_front();
        }
        buf.push_back(val);
    }

    /// Arithmetic mean; returns 0.0 for an empty slice.
    fn avg(values: &VecDeque<f64>) -> f64 {
        let len = values.len();
        if len == 0 {
            return 0.0;
        }
        values.iter().sum::<f64>() / len as f64
    }
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        Self::new(DEFAULT_WINDOW_SIZE)
    }
}

// ── AutoTuner ──────────────────────────────────────────────────────────────

/// Auto-adjusts [`TuningConfig`] based on observed performance.
///
/// Heuristics map metric regressions to targeted config changes:
///
/// | Symptom               | Adjustment(s)                                      |
/// |-----------------------|----------------------------------------------------|
/// | Slow prepare_context  | ↓ `prefetch_hot_topics`, ↑ cache TTLs             |
/// | Slow extract          | ↓ `sandbox_min_lines`                              |
/// | Poor compression      | ↓ `freshness_trigger_ratio` (compress earlier)     |
/// | Low cache hit rate    | ↑ L0/L1/L2 cache TTLs                              |
/// | Very fast (headroom)  | ↑ `prefetch_hot_topics` (up to 20)                 |
///
/// Tuning is rate-limited (`tuning_interval`, default 5 min) and won't
/// start until `min_samples` (default 30) have been collected.
pub struct AutoTuner {
    tuning_config: Mutex<TuningConfig>,
    min_samples: usize,
    tuning_interval: Duration,
    last_tuning: Mutex<Option<Instant>>,
    adjustments_applied: AtomicU64,
    target_prepare_latency_ms: f64,
    target_extract_duration_ms: f64,
    target_compression_ratio: f64,
    target_cache_hit_rate: f64,
}

impl AutoTuner {
    /// Wrap an existing [`TuningConfig`] for adaptive control.
    pub fn new(tuning_config: TuningConfig) -> Self {
        Self {
            tuning_config: Mutex::new(tuning_config),
            min_samples: DEFAULT_MIN_SAMPLES,
            tuning_interval: Duration::from_secs(DEFAULT_TUNING_INTERVAL_SECS),
            last_tuning: Mutex::new(None),
            adjustments_applied: AtomicU64::new(0),
            target_prepare_latency_ms: 500.0,
            target_extract_duration_ms: 200.0,
            target_compression_ratio: 0.6,
            target_cache_hit_rate: 0.5,
        }
    }

    // ── Builder methods ────────────────────────────────────────────────

    /// Override the minimum number of samples before tuning.
    pub fn with_min_samples(mut self, n: usize) -> Self {
        self.min_samples = n;
        self
    }

    /// Override the cooldown between tuning evaluations.
    pub fn with_tuning_interval(mut self, interval: Duration) -> Self {
        self.tuning_interval = interval;
        self
    }

    // ── Accessors ──────────────────────────────────────────────────────

    /// Current tuning config (read-only clone).
    pub fn config(&self) -> TuningConfig {
        self.tuning_config.lock().clone()
    }

    /// Mutable access to the underlying tuning config.
    ///
    /// Callers can adjust fields directly; changes are visible to subsequent
    /// [`evaluate`](Self::evaluate) calls and the `performance_report` API.
    pub fn with_config_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut TuningConfig) -> R,
    {
        let mut guard = self.tuning_config.lock();
        f(&mut guard)
    }

    /// Number of times the tuner has applied adjustments.
    pub fn adjustments_applied(&self) -> u64 {
        self.adjustments_applied.load(Ordering::Relaxed)
    }

    /// Wall-clock instant of the last tuning (if any).
    pub fn last_tuning_instant(&self) -> Option<Instant> {
        *self.last_tuning.lock()
    }

    // ── Evaluation ─────────────────────────────────────────────────────

    /// Evaluate current performance and apply heuristics.
    ///
    /// Returns `true` if one or more tuning parameters were changed.
    pub fn evaluate(&self, monitor: &PerformanceMonitor) -> bool {
        // ── Guard: enough samples? ──────────────────────────────────
        if monitor.total_samples() < self.min_samples as u64 {
            return false;
        }

        // ── Guard: rate limit ───────────────────────────────────────
        {
            let last = self.last_tuning.lock();
            if let Some(ref t) = *last {
                if t.elapsed() < self.tuning_interval {
                    return false;
                }
            }
        }

        let avg_prepare = monitor.avg_prepare_context_latency_ms();
        let avg_extract = monitor.avg_extract_duration_ms();
        let avg_compression = monitor.avg_compression_ratio();
        let cache_rate = monitor.cache_hit_rate();

        let mut adjusted = false;

        // ── 1. Prepare-context latency ───────────────────────────────
        if avg_prepare > self.target_prepare_latency_ms && avg_prepare > 0.0 {
            let mut cfg = self.tuning_config.lock();
            // Reduce prefetch load
            if cfg.prefetch_hot_topics > 1 {
                cfg.prefetch_hot_topics = cfg.prefetch_hot_topics.saturating_sub(1);
                adjusted = true;
            }
            // Extend cache TTLs (cap at 7 days)
            if cfg.l0_cache_ttl_secs < 604_800 {
                cfg.l0_cache_ttl_secs = (cfg.l0_cache_ttl_secs * 12 / 10).min(604_800);
                adjusted = true;
            }
            if cfg.l1_cache_ttl_secs < 604_800 {
                cfg.l1_cache_ttl_secs = (cfg.l1_cache_ttl_secs * 12 / 10).min(604_800);
                adjusted = true;
            }
            if cfg.l2_cache_ttl_secs < 604_800 {
                cfg.l2_cache_ttl_secs = (cfg.l2_cache_ttl_secs * 12 / 10).min(604_800);
                adjusted = true;
            }
        } else if avg_prepare > 0.0 && avg_prepare < self.target_prepare_latency_ms * 0.3 {
            // Headroom: increase prefetch (up to 20)
            let mut cfg = self.tuning_config.lock();
            if cfg.prefetch_hot_topics < 20 {
                cfg.prefetch_hot_topics = (cfg.prefetch_hot_topics + 1).min(20);
                adjusted = true;
            }
        }

        // ── 2. Extract duration ──────────────────────────────────────
        if avg_extract > self.target_extract_duration_ms && avg_extract > 0.0 {
            let mut cfg = self.tuning_config.lock();
            if cfg.sandbox_min_lines > 500 {
                cfg.sandbox_min_lines = cfg.sandbox_min_lines.saturating_sub(100);
                adjusted = true;
            }
        }

        // ── 3. Compression ratio ─────────────────────────────────────
        if avg_compression > self.target_compression_ratio && avg_compression > 0.0 {
            let mut cfg = self.tuning_config.lock();
            if cfg.freshness_trigger_ratio > 0.3 {
                cfg.freshness_trigger_ratio =
                    (cfg.freshness_trigger_ratio - 0.05).max(0.3);
                adjusted = true;
            }
        } else if avg_compression > 0.0 && avg_compression < self.target_compression_ratio * 0.5 {
            // Excessively good compression → relax trigger
            let mut cfg = self.tuning_config.lock();
            if cfg.freshness_trigger_ratio < 0.9 {
                cfg.freshness_trigger_ratio =
                    (cfg.freshness_trigger_ratio + 0.05).min(0.9);
                adjusted = true;
            }
        }

        // ── 4. Cache hit rate ────────────────────────────────────────
        if cache_rate > 0.0 && cache_rate < self.target_cache_hit_rate {
            let mut cfg = self.tuning_config.lock();
            if cfg.l0_cache_ttl_secs < 604_800 {
                cfg.l0_cache_ttl_secs = (cfg.l0_cache_ttl_secs * 12 / 10).min(604_800);
                adjusted = true;
            }
            if cfg.l1_cache_ttl_secs < 604_800 {
                cfg.l1_cache_ttl_secs = (cfg.l1_cache_ttl_secs * 12 / 10).min(604_800);
                adjusted = true;
            }
            if cfg.l2_cache_ttl_secs < 604_800 {
                cfg.l2_cache_ttl_secs = (cfg.l2_cache_ttl_secs * 12 / 10).min(604_800);
                adjusted = true;
            }
        }

        if adjusted {
            *self.last_tuning.lock() = Some(Instant::now());
            self.adjustments_applied.fetch_add(1, Ordering::Relaxed);
        }

        adjusted
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitor_default() {
        let pm = PerformanceMonitor::default();
        assert_eq!(pm.window_size, DEFAULT_WINDOW_SIZE);
        assert_eq!(pm.cache_hit_rate(), 0.0);
        assert_eq!(pm.total_samples(), 0);
    }

    #[test]
    fn test_record_prepare_context() {
        let pm = PerformanceMonitor::default();
        pm.record_prepare_context(Duration::from_millis(150));
        assert_eq!(pm.total_samples(), 1);
        assert!((pm.avg_prepare_context_latency_ms() - 150.0).abs() < 1.0);
    }

    #[test]
    fn test_cache_hit_rate() {
        let pm = PerformanceMonitor::default();
        assert_eq!(pm.cache_hit_rate(), 0.0);

        pm.record_cache_hit();
        pm.record_cache_hit();
        pm.record_cache_miss();
        let rate = pm.cache_hit_rate();
        assert!((rate - 2.0 / 3.0).abs() < 1e-6, "expected 0.666, got {rate}");
    }

    #[test]
    fn test_report_basic() {
        let pm = PerformanceMonitor::default();
        pm.record_prepare_context(Duration::from_millis(100));
        pm.record_cache_hit();
        pm.record_cache_miss();

        let config = TuningConfig::default();
        let report = pm.report(&config, false, None);
        assert!((report.avg_prepare_context_latency_ms - 100.0).abs() < 1.0);
        assert!((report.cache_hit_rate - 0.5).abs() < 1e-6);
        assert!(!report.tuning_applied);
    }

    #[test]
    fn test_tuner_min_samples_guard() {
        let tuner = AutoTuner::new(TuningConfig::default());
        let pm = PerformanceMonitor::default();
        assert!(!tuner.evaluate(&pm), "should not tune with zero samples");
    }

    #[test]
    fn test_tuner_adjusts_prefetch_on_high_latency() {
        let tuner = AutoTuner::new(TuningConfig::default()).with_min_samples(1);
        let pm = PerformanceMonitor::default();
        // Simulate high prepare-context latency
        for _ in 0..5 {
            pm.record_prepare_context(Duration::from_millis(600));
        }
        assert!(tuner.evaluate(&pm), "expected tuning to fire");
        let cfg = tuner.config();
        // Default is 5, should have been reduced
        assert!(cfg.prefetch_hot_topics < 5, "prefetch should decrease");
    }

    #[test]
    fn test_tuner_rate_limit() {
        let tuner = AutoTuner::new(TuningConfig::default())
            .with_min_samples(1)
            .with_tuning_interval(Duration::from_secs(3600));
        let pm = PerformanceMonitor::default();
        pm.record_prepare_context(Duration::from_millis(600));
        assert!(tuner.evaluate(&pm), "first eval should succeed");
        assert!(!tuner.evaluate(&pm), "second eval should be rate-limited");
    }

    #[test]
    fn test_window_eviction() {
        let pm = PerformanceMonitor::new(3);
        for i in 0..10 {
            pm.record_prepare_context(Duration::from_millis(i * 100));
        }
        // After 10 pushes into window=3, avg should be based on last 3 values
        let avg = pm.avg_prepare_context_latency_ms();
        // Last 3: 7*100=700, 8*100=800, 9*100=900 → avg = 800
        assert!((avg - 800.0).abs() < 1.0, "expected 800, got {avg}");
    }

    #[test]
    fn test_tuner_noop_on_good_performance() {
        let tuner = AutoTuner::new(TuningConfig::default()).with_min_samples(1);
        let pm = PerformanceMonitor::default();
        // Well within targets
        pm.record_prepare_context(Duration::from_millis(50));
        pm.record_extract(Duration::from_millis(30));
        assert!(!tuner.evaluate(&pm), "should not tune when performance is good");
    }

    #[test]
    fn test_tuner_adjusts_compression() {
        let tuner = AutoTuner::new(TuningConfig::default()).with_min_samples(1);
        let pm = PerformanceMonitor::default();
        for _ in 0..5 {
            pm.record_compression_ratio(0.85); // poor compression
        }
        assert!(tuner.evaluate(&pm), "expected tuning on poor compression");
        let cfg = tuner.config();
        assert!(cfg.freshness_trigger_ratio < 0.8, "freshness_trigger should decrease");
    }

    #[test]
    fn test_tuner_adjusts_cache_ttl() {
        let tuner = AutoTuner::new(TuningConfig::default()).with_min_samples(1);
        let pm = PerformanceMonitor::default();
        pm.record_cache_miss();
        pm.record_cache_miss();
        pm.record_cache_miss();
        pm.record_cache_hit(); // 25% hit rate, below 50% target
        assert!(tuner.evaluate(&pm), "expected tuning on low cache hit rate");
        let cfg = tuner.config();
        assert!(cfg.l0_cache_ttl_secs > 86400, "L0 TTL should increase");
    }

    #[test]
    fn test_with_config_mut() {
        let tuner = AutoTuner::new(TuningConfig::default());
        tuner.with_config_mut(|cfg| {
            cfg.prefetch_hot_topics = 42;
        });
        assert_eq!(tuner.config().prefetch_hot_topics, 42);
    }
}
