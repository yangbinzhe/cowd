//! `context_rot` — context window health monitor with agent-facing warnings.
//!
//! Implements the GSD (Generalized Sisyphus Daemon) context-monitor pattern:
//! - **Warning** at >65% context usage (35% remaining): inject agent-facing message.
//! - **Critical** at >75% context usage (25% remaining): auto-record session state.
//! - 5-call debounce for repeated same-severity alerts; severity upgrades bypass
//!   debounce.
//!
//! # Usage
//!
//! ```rust,ignore
//! let mut monitor = ContextRotMonitor::new(RotMetrics::default());
//! match monitor.check(used, total) {
//!     RotAlert::Warning(msg) => tracing::warn!("{msg}"),
//!     RotAlert::Critical(msg) => tracing::error!("{msg}"),
//!     RotAlert::None => {}
//! }
//! ```

// ---------------------------------------------------------------------------
// RotMetrics
// ---------------------------------------------------------------------------

/// Accumulated metrics tracked by the context rotation monitor.
#[derive(Debug, Clone, Default)]
pub struct RotMetrics {
    /// Ratio of used tokens to total context window (0.0–1.0).
    pub context_usage_ratio: f32,
    /// Savings ratio from the last compression run (0.0–1.0).
    pub compression_effectiveness: f32,
    /// Tool output tokens as a ratio of total token usage.
    pub token_waste_ratio: f32,
    /// Number of warnings issued since the monitor was created.
    pub warning_count: u32,
    /// Number of critical alerts issued since the monitor was created.
    pub critical_count: u32,
}

// ---------------------------------------------------------------------------
// RotAlert
// ---------------------------------------------------------------------------

/// Alert level produced by a context rotation health check.
#[derive(Debug, Clone, PartialEq)]
pub enum RotAlert {
    /// No alert — context window usage is within healthy bounds.
    None,
    /// Warning — context usage exceeds 65%. Agent-facing message recommended.
    Warning(String),
    /// Critical — context usage exceeds 75%. Escalation bypasses debounce,
    /// and the caller should auto-record session state.
    Critical(String),
}

// ---------------------------------------------------------------------------
// ContextRotMonitor
// ---------------------------------------------------------------------------

/// Context rotation monitor — tracks context window health and injects
/// agent-facing warnings when the context window approaches dangerous
/// usage levels.
///
/// Implements the **GSD context-monitor pattern**:
///
/// | Condition            | Alert   | Action                        |
/// |----------------------|---------|-------------------------------|
/// | usage ≤ 65%          | None    | —                             |
/// | 65% < usage ≤ 75%    | Warning | inject agent-facing message   |
/// | usage > 75%          | Critical| auto-record session state     |
///
/// The 5-call debounce suppresses repeated warnings at the same severity so
/// the agent stream is not flooded. A severity **upgrade** (Warning →
/// Critical) always fires immediately.
pub struct ContextRotMonitor {
    /// Accumulated metrics for external reporting.
    pub metrics: RotMetrics,
    /// Consecutive checks since the last alert fired.
    debounce_count: u32,
    /// Last alert severity for debounce comparison.
    last_severity: Option<RotAlert>,
    /// Maximum number of calls to suppress duplicate warnings.
    debounce_window: u32,
}

impl ContextRotMonitor {
    /// Create a new monitor seeded with the given `metrics`.
    pub fn new(metrics: RotMetrics) -> Self {
        Self {
            metrics,
            debounce_count: 0,
            last_severity: None,
            debounce_window: 5,
        }
    }

    /// Check context health given current `used_tokens` and `total_tokens`.
    ///
    /// Returns a [`RotAlert`] when context usage exceeds the configured
    /// thresholds.  Identical non-critical alerts are suppressed for
    /// `debounce_window` consecutive calls; critical alerts always fire.
    pub fn check(&mut self, used_tokens: u64, total_tokens: u64) -> RotAlert {
        if total_tokens == 0 {
            return RotAlert::None;
        }

        let ratio = used_tokens as f32 / total_tokens as f32;
        self.metrics.context_usage_ratio = ratio;

        let alert = if ratio > 0.75 {
            RotAlert::Critical(format!(
                "⚠ CONTEXT ROT: {:.1}% usage ({} / {} tokens). Auto-record session state.",
                ratio * 100.0,
                used_tokens,
                total_tokens
            ))
        } else if ratio > 0.65 {
            RotAlert::Warning(format!(
                "⚠ Context usage at {:.1}% — inject agent-facing message.",
                ratio * 100.0
            ))
        } else {
            RotAlert::None
        };

        // ── Debounce logic ──────────────────────────────────────────────
        match &alert {
            RotAlert::None => {
                // Healthy state — reset debounce so next alert fires fresh.
                self.debounce_count = 0;
                self.last_severity = None;
            }
            RotAlert::Critical(_) => {
                // Critical alerts always fire — bypass debounce.
                self.debounce_count = 0;
                self.last_severity = Some(alert.clone());
                self.metrics.critical_count = self.metrics.critical_count.saturating_add(1);
            }
            RotAlert::Warning(_) => {
                self.debounce_count = self.debounce_count.saturating_add(1);

                // Suppress if:
                // - we have already seen at least one warning in a row, AND
                // - we are still inside the debounce window.
                let should_suppress = self.debounce_count > 1
                    && self.debounce_count <= self.debounce_window
                    && matches!(&self.last_severity, Some(RotAlert::Warning(_)));

                if should_suppress {
                    return RotAlert::None;
                }

                self.last_severity = Some(alert.clone());
                self.metrics.warning_count = self.metrics.warning_count.saturating_add(1);
            }
        }

        alert
    }

    /// Reset the monitor to its initial state (metrics preserved).
    pub fn reset(&mut self) {
        self.debounce_count = 0;
        self.last_severity = None;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // RotAlert
    // -----------------------------------------------------------------------

    #[test]
    fn alert_none_equality() {
        assert_eq!(RotAlert::None, RotAlert::None);
    }

    #[test]
    fn alert_warning_equality() {
        assert_eq!(
            RotAlert::Warning("warn!".into()),
            RotAlert::Warning("warn!".into()),
        );
    }

    // -----------------------------------------------------------------------
    // ContextRotMonitor — thresholds
    // -----------------------------------------------------------------------

    #[test]
    fn healthy_usage_returns_none() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        // 5000 / 10000 = 50% → below 65%
        assert_eq!(m.check(5000, 10000), RotAlert::None);
    }

    #[test]
    fn exactly_65_pct_returns_none() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        assert_eq!(m.check(6500, 10000), RotAlert::None);
    }

    #[test]
    fn above_65_pct_returns_warning() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        let alert = m.check(6600, 10000);
        assert!(matches!(alert, RotAlert::Warning(_)));
    }

    #[test]
    fn exactly_75_pct_returns_warning() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        let alert = m.check(7500, 10000);
        assert!(matches!(alert, RotAlert::Warning(_)));
    }

    #[test]
    fn above_75_pct_returns_critical() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        let alert = m.check(7600, 10000);
        assert!(matches!(alert, RotAlert::Critical(_)));
    }

    #[test]
    fn near_full_returns_critical() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        let alert = m.check(9800, 10000);
        assert!(matches!(alert, RotAlert::Critical(_)));
    }

    #[test]
    fn zero_total_returns_none() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        assert_eq!(m.check(100, 0), RotAlert::None);
    }

    // -----------------------------------------------------------------------
    // ContextRotMonitor — debounce (warning → warning → …)
    // -----------------------------------------------------------------------

    #[test]
    fn warning_debounce_suppresses_duplicates_within_window() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        // First warning fires.
        let a1 = m.check(7000, 10000);
        assert!(matches!(a1, RotAlert::Warning(_)));

        // 4 subsequent checks should be suppressed.
        for _ in 0..4 {
            let a = m.check(7000, 10000);
            assert_eq!(a, RotAlert::None, "warning should be debounced");
        }
    }

    #[test]
    fn warning_fires_again_after_debounce_window() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        // Fire once.
        assert!(matches!(m.check(7000, 10000), RotAlert::Warning(_)));
        // 5 suppressed calls.
        for _ in 0..5 {
            let _ = m.check(7000, 10000);
        }
        // The 6th call (debounce_window + 1) should fire again.
        let alert = m.check(7000, 10000);
        assert!(
            matches!(alert, RotAlert::Warning(_)),
            "should fire again after debounce window"
        );
    }

    // -----------------------------------------------------------------------
    // ContextRotMonitor — severity upgrade bypasses debounce
    // -----------------------------------------------------------------------

    #[test]
    fn warning_to_critical_upgrade_fires_immediately() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        // Fire a warning.
        let a1 = m.check(7000, 10000);
        assert!(matches!(a1, RotAlert::Warning(_)));

        // Next call: severity upgrades to Critical — must fire immediately.
        let a2 = m.check(8000, 10000);
        assert!(
            matches!(a2, RotAlert::Critical(_)),
            "severity upgrade must bypass debounce"
        );
    }

    #[test]
    fn healthy_resets_debounce() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        // Fire warning.
        assert!(matches!(m.check(7000, 10000), RotAlert::Warning(_)));
        // Debounce would suppress… but then health returns.
        assert_eq!(m.check(5000, 10000), RotAlert::None);
        // Now a new warning should fire fresh (not debounced).
        assert!(matches!(m.check(7000, 10000), RotAlert::Warning(_)));
    }

    // -----------------------------------------------------------------------
    // ContextRotMonitor — metrics
    // -----------------------------------------------------------------------

    #[test]
    fn warning_increments_warning_count() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        let _ = m.check(7000, 10000);
        assert_eq!(m.metrics.warning_count, 1);
    }

    #[test]
    fn critical_increments_critical_count() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        let _ = m.check(8000, 10000);
        assert_eq!(m.metrics.critical_count, 1);
    }

    #[test]
    fn usage_ratio_updated_every_check() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        let _ = m.check(4200, 10000);
        assert!((m.metrics.context_usage_ratio - 0.42).abs() < 0.001);
    }

    #[test]
    fn reset_clears_debounce_state() {
        let mut m = ContextRotMonitor::new(RotMetrics::default());
        let _ = m.check(7000, 10000);
        assert_eq!(m.metrics.warning_count, 1);
        m.reset();
        // Metrics preserved.
        assert_eq!(m.metrics.warning_count, 1);
        // New warning fires fresh (not debounced).
        assert!(matches!(m.check(7000, 10000), RotAlert::Warning(_)));
    }
}
