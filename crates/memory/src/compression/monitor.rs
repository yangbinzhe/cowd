//! Real-time context window monitor.
//!
//! Periodically samples token usage and recommends an action (continue,
//! avoid complex work, save state and pause).
//!
//! # Alert levels
//!
//! | Remaining% | Level    | Action             |
//! |------------|----------|--------------------|
//! | ≥ warning  | Normal   | Continue           |
//! | ≥ critical | Warning  | `AvoidComplexWork`   |
//! | < critical | Critical | `SaveStateAndPause`  |
//!
//! # Debounce
//!
//! Same-level alerts are suppressed until `debounce_interval` tool calls have
//! elapsed.  If the level *upgrades* (gets more severe), the alert fires
//! immediately, bypassing the debounce counter.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;

use chrono::Utc;

use crate::{
    compression::budget::BudgetManager,
    types::{AlertLevel, ContextAction, ContextMonitor, HandoffData},
};

// ─── Default thresholds ───────────────────────────────────────────────────────

/// Default fraction of the window that must *remain* before a Warning fires.
const DEFAULT_WARNING_REMAINING: f32 = 0.35;
/// Default fraction of the window that must *remain* before Critical fires.
const DEFAULT_CRITICAL_REMAINING: f32 = 0.25;
/// Default number of tool calls between same-level alerts (debounce).
const DEFAULT_DEBOUNCE_INTERVAL: u32 = 5;

// ─── ContextWindowMonitor ────────────────────────────────────────────────────

/// Monitors context window pressure and emits recommended actions.
///
/// # Thread safety
/// All mutable state uses atomic primitives or `Mutex`, so this type is
/// `Send + Sync` and can be shared via `Arc`.
pub struct ContextWindowMonitor {
    budget: BudgetManager,

    /// Remaining-token fraction at which the warning level is entered.
    warning_threshold: f32,
    /// Remaining-token fraction at which the critical level is entered.
    critical_threshold: f32,
    /// Minimum tool calls between same-level alerts.
    debounce_interval: u32,

    // ─── mutable state ───────────────────────────────────────────────────────
    /// Number of tool-call ticks since the last alert was emitted.
    tool_calls_since_alert: AtomicU32,
    /// The alert level observed on the previous `check` call.
    last_level: RwLock<AlertLevel>,
}

impl ContextWindowMonitor {
    /// Create a monitor backed by `budget` with default thresholds.
    #[must_use]
    pub fn new(budget: BudgetManager) -> Self {
        Self::with_thresholds(
            budget,
            DEFAULT_WARNING_REMAINING,
            DEFAULT_CRITICAL_REMAINING,
            DEFAULT_DEBOUNCE_INTERVAL,
        )
    }

    /// Create a monitor with explicit thresholds.
    ///
    /// * `warning_remaining`  – fraction of context window that must *remain* to
    ///   stay at Normal (e.g. `0.35` = warn when < 35 % left).
    /// * `critical_remaining` – fraction that must remain to stay at Warning.
    /// * `debounce_interval`  – tool calls between repeated same-level alerts.
    #[must_use]
    pub fn with_thresholds(
        budget: BudgetManager,
        warning_remaining: f32,
        critical_remaining: f32,
        debounce_interval: u32,
    ) -> Self {
        Self {
            budget,
            warning_threshold: warning_remaining,
            critical_threshold: critical_remaining,
            debounce_interval,
            tool_calls_since_alert: AtomicU32::new(0),
            last_level: RwLock::new(AlertLevel::Normal),
        }
    }

    // ─── Primary API ─────────────────────────────────────────────────────────

    /// Sample the current state and produce a `ContextMonitor` snapshot
    /// (backwards-compatible with the original `sample` interface).
    #[must_use]
    pub fn sample(&self, used_tokens: u64) -> ContextMonitor {
        let total_tokens = self.budget.initial_budget().total;
        let action = self.check(used_tokens as u32);
        let alert_level = match &action {
            ContextAction::Continue => AlertLevel::Normal,
            ContextAction::AvoidComplexWork => AlertLevel::Warning,
            ContextAction::SaveStateAndPause { .. } => AlertLevel::Critical,
        };
        ContextMonitor {
            used_tokens,
            total_tokens,
            alert_level,
            recommended_action: action,
            sampled_at: Utc::now(),
        }
    }

    /// Evaluate context pressure for `current_tokens` and return the
    /// recommended [`ContextAction`].
    ///
    /// * Upgrades (Normal → Warning, Warning → Critical) bypass debounce.
    /// * Same-level repeats are suppressed until `debounce_interval` ticks.
    pub fn check(&self, current_tokens: u32) -> ContextAction {
        let total = self.budget.initial_budget().total as f32;
        let remaining_pct = if total > 0.0 {
            1.0 - (current_tokens as f32 / total)
        } else {
            1.0
        };

        let level = self.classify(remaining_pct);

        // Fetch the previous level (acquire the lock briefly).
        let mut last_guard = self
            .last_level
            .write()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("ContextWindowMonitor last_level lock poisoned; recovering");
                poisoned.into_inner()
            });

        if level > *last_guard {
            // Level upgraded – fire immediately.
            *last_guard = level;
            self.tool_calls_since_alert.store(0, Ordering::Relaxed);
            return self.build_action(level);
        }

        // Same or lower level – apply debounce.
        let calls = self
            .tool_calls_since_alert
            .fetch_add(1, Ordering::Relaxed);
        if calls >= self.debounce_interval {
            self.tool_calls_since_alert.store(0, Ordering::Relaxed);
            *last_guard = level;
            return self.build_action(level);
        }

        // Suppressed.
        ContextAction::Continue
    }

    // ─── Accessors ───────────────────────────────────────────────────────────

    /// Return the current debounce counter value (useful for testing).
    #[must_use]
    pub fn tool_calls_since_alert(&self) -> u32 {
        self.tool_calls_since_alert.load(Ordering::Relaxed)
    }

    /// Reset the debounce counter and last-level state.
    pub fn reset(&self) {
        self.tool_calls_since_alert.store(0, Ordering::Relaxed);
        if let Ok(mut l) = self.last_level.write() {
            *l = AlertLevel::Normal;
        }
    }

    // ─── Private helpers ─────────────────────────────────────────────────────

    fn classify(&self, remaining_pct: f32) -> AlertLevel {
        if remaining_pct < self.critical_threshold {
            AlertLevel::Critical
        } else if remaining_pct < self.warning_threshold {
            AlertLevel::Warning
        } else {
            AlertLevel::Normal
        }
    }

    fn build_action(&self, level: AlertLevel) -> ContextAction {
        match level {
            AlertLevel::Normal => ContextAction::Continue,
            AlertLevel::Warning => ContextAction::AvoidComplexWork,
            AlertLevel::Critical => ContextAction::SaveStateAndPause {
                handoff: HandoffData {
                    session_id: String::new(),
                    timestamp: Utc::now(),
                    work_items: vec![],
                    decisions: vec![],
                    blockers: vec![],
                    task_states: vec![],
                    summary: "Context window critical – saving state.".into(),
                },
            },
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compression::budget::BudgetManager,
        config::BudgetConfig,
    };

    fn make_monitor() -> ContextWindowMonitor {
        let cfg = BudgetConfig {
            context_window: 1000,
            reserved_system: 0,
            reserved_response: 0,
            warning_threshold: 0.65, // used ≥ 65 % → Warning via budget
            critical_threshold: 0.90,
        };
        ContextWindowMonitor::with_thresholds(
            BudgetManager::new(cfg),
            0.35, // remaining < 35 % → Warning
            0.25, // remaining < 25 % → Critical
            5,
        )
    }

    #[test]
    fn normal_when_plenty_of_space() {
        let m = make_monitor();
        // 500/1000 used → 50 % remaining → Normal
        assert!(matches!(m.check(500), ContextAction::Continue));
    }

    #[test]
    fn warning_when_below_warning_threshold() {
        let m = make_monitor();
        // 700/1000 used → 30 % remaining → Warning (immediate upgrade)
        assert!(matches!(m.check(700), ContextAction::AvoidComplexWork));
    }

    #[test]
    fn critical_when_below_critical_threshold() {
        let m = make_monitor();
        // 800/1000 used → 20 % remaining → Critical (immediate upgrade)
        assert!(matches!(
            m.check(800),
            ContextAction::SaveStateAndPause { .. }
        ));
    }

    #[test]
    fn debounce_suppresses_repeated_same_level() {
        let m = make_monitor();
        // First call at Warning level → fires (level upgrade: Normal → Warning).
        assert!(matches!(m.check(700), ContextAction::AvoidComplexWork));
        // Next `debounce_interval` (5) calls at same level → suppressed.
        // fetch_add returns 0,1,2,3,4 which are all < 5.
        for _ in 0..5 {
            assert!(matches!(m.check(700), ContextAction::Continue));
        }
        // The 6th same-level call: fetch_add returns 5 which >= debounce_interval → fires again.
        assert!(matches!(m.check(700), ContextAction::AvoidComplexWork));
    }

    #[test]
    fn upgrade_bypasses_debounce() {
        let m = make_monitor();
        // Saturate debounce at Warning.
        m.check(700);
        for _ in 0..3 {
            m.check(700);
        }
        // Upgrade to Critical bypasses debounce.
        assert!(matches!(
            m.check(800),
            ContextAction::SaveStateAndPause { .. }
        ));
    }
}

// ─── Adaptive Threshold ────────────────────────────────────────────────────

/// Adaptive compression thresholds based on model context window size.
///
/// Different models have different context windows, so the point at which
/// compression should be triggered varies. This struct defines the ratios
/// at which each action level fires.
#[derive(Debug, Clone, Copy)]
pub struct AdaptiveThreshold {
    /// Ratio of context window usage at which a warning is emitted.
    /// Default: 0.70 (70% used).
    pub warn_ratio: f64,
    /// Ratio at which automatic compaction is triggered.
    /// Default: 0.85 (85% used).
    pub compact_ratio: f64,
    /// Ratio at which new input is blocked (hard limit).
    /// Default: 0.95 (95% used).
    pub hard_limit_ratio: f64,
}

impl Default for AdaptiveThreshold {
    fn default() -> Self {
        Self {
            warn_ratio: 0.70,
            compact_ratio: 0.85,
            hard_limit_ratio: 0.95,
        }
    }
}

impl AdaptiveThreshold {
    /// Create thresholds for a small context window (8K-16K tokens).
    pub fn small_model() -> Self {
        Self {
            warn_ratio: 0.60,
            compact_ratio: 0.75,
            hard_limit_ratio: 0.90,
        }
    }

    /// Create thresholds for a large context window (128K+ tokens).
    pub fn large_model() -> Self {
        Self {
            warn_ratio: 0.75,
            compact_ratio: 0.88,
            hard_limit_ratio: 0.96,
        }
    }

    /// Determine the context window size for a given model name.
    ///
    /// Returns the context window in tokens. Falls back to 128K if unknown.
    pub fn context_window_for_model(model: &str) -> usize {
        let lower = model.to_ascii_lowercase();

        // Claude models
        if lower.contains("claude-3-opus") || lower.contains("claude-3.5-opus") || lower.contains("claude-opus-4") {
            return 200_000;
        }
        if lower.contains("claude-3.5-sonnet") || lower.contains("claude-3-sonnet") || lower.contains("claude-sonnet-4") {
            return 200_000;
        }
        if lower.contains("claude-3-haiku") || lower.contains("claude-3.5-haiku") || lower.contains("claude-haiku") {
            return 200_000;
        }

        // GPT-4 models
        if lower.contains("gpt-4o") || lower.contains("gpt-4-turbo") {
            return 128_000;
        }
        if lower.contains("gpt-4-32k") {
            return 32_000;
        }
        if lower.contains("gpt-4") {
            return 8_192;
        }

        // GPT-3.5
        if lower.contains("gpt-3.5-16k") {
            return 16_000;
        }
        if lower.contains("gpt-3.5") {
            return 4_096;
        }

        // Llama / local models
        if lower.contains("llama-3") || lower.contains("llama3") {
            return 8_000;
        }
        if lower.contains("mistral") || lower.contains("mixtral") {
            return 32_000;
        }
        if lower.contains("qwen") || lower.contains("deepseek") {
            return 32_000;
        }

        // Default: assume 128K
        128_000
    }

    /// Select the appropriate threshold based on context window size.
    pub fn for_model(model: &str) -> Self {
        let window = Self::context_window_for_model(model);
        if window <= 16_000 {
            Self::small_model()
        } else if window >= 128_000 {
            Self::large_model()
        } else {
            Self::default()
        }
    }

    /// Check if the given token usage triggers the compact threshold.
    pub fn should_compact(&self, used_tokens: usize, total_tokens: usize) -> bool {
        if total_tokens == 0 {
            return false;
        }
        (used_tokens as f64 / total_tokens as f64) >= self.compact_ratio
    }

    /// Check if the given token usage triggers the hard limit.
    pub fn is_hard_limited(&self, used_tokens: usize, total_tokens: usize) -> bool {
        if total_tokens == 0 {
            return false;
        }
        (used_tokens as f64 / total_tokens as f64) >= self.hard_limit_ratio
    }
}

#[cfg(test)]
mod adaptive_tests {
    use super::*;

    #[test]
    fn claude_opus_200k() {
        assert_eq!(AdaptiveThreshold::context_window_for_model("claude-3-opus"), 200_000);
    }

    #[test]
    fn gpt4o_128k() {
        assert_eq!(AdaptiveThreshold::context_window_for_model("gpt-4o"), 128_000);
    }

    #[test]
    fn gpt4_base_8k() {
        assert_eq!(AdaptiveThreshold::context_window_for_model("gpt-4"), 8_192);
    }

    #[test]
    fn unknown_defaults_128k() {
        assert_eq!(AdaptiveThreshold::context_window_for_model("unknown-model"), 128_000);
    }

    #[test]
    fn small_model_thresholds() {
        let t = AdaptiveThreshold::small_model();
        assert!(t.warn_ratio < 0.70);
        assert!(t.compact_ratio < 0.85);
    }

    #[test]
    fn for_model_selects_small_for_8k() {
        let t = AdaptiveThreshold::for_model("gpt-4");
        assert!(t.warn_ratio < 0.70); // small model thresholds
    }

    #[test]
    fn for_model_selects_large_for_200k() {
        let t = AdaptiveThreshold::for_model("claude-3-opus");
        assert!(t.warn_ratio > 0.70); // large model thresholds
    }

    #[test]
    fn should_compact_triggers_at_threshold() {
        let t = AdaptiveThreshold::default();
        assert!(t.should_compact(85_000, 100_000)); // 85%
        assert!(!t.should_compact(84_000, 100_000)); // 84%
    }

    #[test]
    fn hard_limit_triggers() {
        let t = AdaptiveThreshold::default();
        assert!(t.is_hard_limited(95_000, 100_000));
        assert!(!t.is_hard_limited(94_000, 100_000));
    }
}
