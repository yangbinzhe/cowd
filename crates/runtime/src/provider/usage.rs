use crate::session::Session;

use model_protocol::usage::{ModelPricing, TokenUsage};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::cowd_event::RunModelTelemetry;

/// Returns pricing metadata for a known model alias or family.
///
/// Delegates to the global [`ModelRegistry`] loaded from `~/.cowd/models.yaml`.
/// Falls back to heuristic matching for Claude models when the registry is
/// unavailable or the model is not found.
#[must_use]
pub fn pricing_for_model(model: &str) -> Option<ModelPricing> {
    model_protocol::model_registry::pricing_for_model(model)
}

/// Aggregates token usage across a running session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageTracker {
    latest_turn: TokenUsage,
    cumulative: TokenUsage,
    turns: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRouteIntent {
    Quick,
    Standard,
    Deep,
    Recovery,
}

impl ModelRouteIntent {
    #[must_use]
    pub fn from_task(intent: &str) -> Self {
        let lower = intent.to_lowercase();
        if contains_any(
            &lower,
            &[
                "deep",
                "architecture",
                "refactor",
                "审计",
                "架构",
                "重构",
                "复杂",
                "全盘",
            ],
        ) {
            Self::Deep
        } else if contains_any(
            &lower,
            &["stalled", "retry", "恢复", "失败", "卡住", "循环"],
        ) {
            Self::Recovery
        } else if contains_any(&lower, &["quick", "simple", "快速", "简单"]) {
            Self::Quick
        } else {
            Self::Standard
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelPerformanceStats {
    pub model: String,
    pub samples: u64,
    pub total_tokens: u64,
    pub output_tokens: u64,
    pub avg_first_token_latency_ms: Option<f64>,
    pub avg_tokens_per_second: Option<f64>,
    pub provider_usage_samples: u64,
    pub estimated_usage_samples: u64,
    pub failure_count: u64,
    pub quality_score_sum: f64,
}

impl ModelPerformanceStats {
    #[must_use]
    pub fn quality_average(&self) -> Option<f64> {
        (self.samples > 0).then_some(self.quality_score_sum / self.samples as f64)
    }

    #[must_use]
    pub fn failure_rate(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.failure_count as f64 / self.samples as f64
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRouteDecision {
    pub selected_model: String,
    pub intent: ModelRouteIntent,
    pub reason: String,
    pub score: f64,
    pub candidates: Vec<ModelRouteCandidate>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRouteCandidate {
    pub model: String,
    pub score: f64,
    pub tokens_per_second: Option<f64>,
    pub first_token_latency_ms: Option<f64>,
    pub quality_average: Option<f64>,
    pub failure_rate: f64,
    pub samples: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelPerformanceRegistry {
    stats: BTreeMap<String, ModelPerformanceStats>,
}

impl ModelPerformanceRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_telemetry(
        &mut self,
        telemetry: &RunModelTelemetry,
        quality_score: Option<f64>,
        failed: bool,
    ) {
        let Some(model) = telemetry
            .model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
        else {
            return;
        };
        let entry = self
            .stats
            .entry(model.to_string())
            .or_insert_with(|| ModelPerformanceStats {
                model: model.to_string(),
                samples: 0,
                total_tokens: 0,
                output_tokens: 0,
                avg_first_token_latency_ms: None,
                avg_tokens_per_second: None,
                provider_usage_samples: 0,
                estimated_usage_samples: 0,
                failure_count: 0,
                quality_score_sum: 0.0,
            });
        entry.samples += 1;
        entry.total_tokens = entry.total_tokens.saturating_add(telemetry.total_tokens);
        entry.output_tokens = entry.output_tokens.saturating_add(telemetry.output_tokens);
        entry.avg_first_token_latency_ms = rolling_average(
            entry.avg_first_token_latency_ms,
            telemetry.first_token_latency_ms.map(|value| value as f64),
            entry.samples,
        );
        entry.avg_tokens_per_second = rolling_average(
            entry.avg_tokens_per_second,
            telemetry.tokens_per_second,
            entry.samples,
        );
        if telemetry.usage_source == "provider" {
            entry.provider_usage_samples += 1;
        } else {
            entry.estimated_usage_samples += 1;
        }
        if failed {
            entry.failure_count += 1;
        }
        entry.quality_score_sum += quality_score.unwrap_or_else(|| {
            if failed {
                0.0
            } else if telemetry.usage_source == "provider" {
                0.75
            } else {
                0.6
            }
        });
    }

    #[must_use]
    pub fn stats_for(&self, model: &str) -> Option<&ModelPerformanceStats> {
        self.stats.get(model)
    }

    #[must_use]
    pub fn all_stats(&self) -> Vec<ModelPerformanceStats> {
        self.stats.values().cloned().collect()
    }

    #[must_use]
    pub fn route(
        &self,
        intent: ModelRouteIntent,
        fallback_models: &[String],
    ) -> ModelRouteDecision {
        let mut candidates = self
            .stats
            .values()
            .map(|stats| candidate_for_stats(stats, intent))
            .collect::<Vec<_>>();
        for model in fallback_models {
            if !candidates.iter().any(|candidate| candidate.model == *model) {
                candidates.push(cold_start_candidate(model, intent));
            }
        }
        if candidates.is_empty() {
            candidates.push(cold_start_candidate("default", intent));
        }
        candidates.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.model.cmp(&right.model))
        });
        let selected = candidates[0].clone();
        ModelRouteDecision {
            selected_model: selected.model.clone(),
            intent,
            reason: route_reason(intent, &selected),
            score: selected.score,
            candidates,
        }
    }
}

impl UsageTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn from_session(session: &Session) -> Self {
        let mut tracker = Self::new();
        for message in &session.messages {
            if let Some(usage) = message.usage {
                tracker.record(usage);
            }
        }
        tracker
    }

    pub fn record(&mut self, usage: TokenUsage) {
        self.latest_turn = usage;
        self.cumulative.input_tokens += usage.input_tokens;
        self.cumulative.output_tokens += usage.output_tokens;
        self.cumulative.cache_creation_input_tokens += usage.cache_creation_input_tokens;
        self.cumulative.cache_read_input_tokens += usage.cache_read_input_tokens;
        self.turns += 1;
    }

    #[must_use]
    pub fn current_turn_usage(&self) -> TokenUsage {
        self.latest_turn
    }

    #[must_use]
    pub fn cumulative_usage(&self) -> TokenUsage {
        self.cumulative
    }

    #[must_use]
    pub fn turns(&self) -> u32 {
        self.turns
    }
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

fn rolling_average(current: Option<f64>, sample: Option<f64>, samples: u64) -> Option<f64> {
    let sample = sample?;
    Some(match current {
        Some(current) if samples > 1 => {
            ((current * (samples.saturating_sub(1) as f64)) + sample) / samples as f64
        }
        _ => sample,
    })
}

fn candidate_for_stats(
    stats: &ModelPerformanceStats,
    intent: ModelRouteIntent,
) -> ModelRouteCandidate {
    let speed = stats.avg_tokens_per_second.unwrap_or(0.0);
    let latency = stats.avg_first_token_latency_ms.unwrap_or(3_000.0);
    let quality = stats.quality_average().unwrap_or(0.5);
    let reliability = 1.0 - stats.failure_rate();
    let score = match intent {
        ModelRouteIntent::Quick => speed * 1.4 + reliability * 20.0 - latency / 1_000.0,
        ModelRouteIntent::Standard => {
            speed + quality * 25.0 + reliability * 15.0 - latency / 2_000.0
        }
        ModelRouteIntent::Deep => quality * 80.0 + reliability * 30.0 + speed * 0.02,
        ModelRouteIntent::Recovery => reliability * 35.0 + quality * 20.0 + speed * 0.5,
    };
    ModelRouteCandidate {
        model: stats.model.clone(),
        score,
        tokens_per_second: stats.avg_tokens_per_second,
        first_token_latency_ms: stats.avg_first_token_latency_ms,
        quality_average: stats.quality_average(),
        failure_rate: stats.failure_rate(),
        samples: stats.samples,
    }
}

fn cold_start_candidate(model: &str, intent: ModelRouteIntent) -> ModelRouteCandidate {
    let lower = model.to_lowercase();
    let mut score = 10.0;
    if matches!(intent, ModelRouteIntent::Quick) && contains_any(&lower, &["flash", "step", "fast"])
    {
        score += 15.0;
    }
    if matches!(intent, ModelRouteIntent::Deep) && contains_any(&lower, &["deep", "reason", "r1"]) {
        score += 15.0;
    }
    ModelRouteCandidate {
        model: model.to_string(),
        score,
        tokens_per_second: None,
        first_token_latency_ms: None,
        quality_average: None,
        failure_rate: 0.0,
        samples: 0,
    }
}

fn route_reason(intent: ModelRouteIntent, selected: &ModelRouteCandidate) -> String {
    match intent {
        ModelRouteIntent::Quick => format!(
            "quick route favors high throughput and low latency; selected {} score {:.2}",
            selected.model, selected.score
        ),
        ModelRouteIntent::Standard => format!(
            "standard route balances speed, quality, and reliability; selected {} score {:.2}",
            selected.model, selected.score
        ),
        ModelRouteIntent::Deep => format!(
            "deep route favors quality and reliability over raw speed; selected {} score {:.2}",
            selected.model, selected.score
        ),
        ModelRouteIntent::Recovery => format!(
            "recovery route favors reliable models after stalled or failed work; selected {} score {:.2}",
            selected.model, selected.score
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::session::{ContentBlock, ConversationMessage, MessageRole, Session};
    use model_protocol::usage::{format_usd, TokenUsage};

    use super::{pricing_for_model, ModelPerformanceRegistry, ModelRouteIntent, UsageTracker};
    use crate::cowd_event::RunModelTelemetry;

    #[test]
    fn tracks_true_cumulative_usage() {
        let mut tracker = UsageTracker::new();
        tracker.record(TokenUsage {
            input_tokens: 10,
            output_tokens: 4,
            cache_creation_input_tokens: 2,
            cache_read_input_tokens: 1,
        });
        tracker.record(TokenUsage {
            input_tokens: 20,
            output_tokens: 6,
            cache_creation_input_tokens: 3,
            cache_read_input_tokens: 2,
        });

        assert_eq!(tracker.turns(), 2);
        assert_eq!(tracker.current_turn_usage().input_tokens, 20);
        assert_eq!(tracker.current_turn_usage().output_tokens, 6);
        assert_eq!(tracker.cumulative_usage().output_tokens, 10);
        assert_eq!(tracker.cumulative_usage().input_tokens, 30);
        assert_eq!(tracker.cumulative_usage().total_tokens(), 48);
    }

    #[test]
    fn computes_cost_summary_lines() {
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 500_000,
            cache_creation_input_tokens: 100_000,
            cache_read_input_tokens: 200_000,
        };

        let cost = usage.estimate_cost_usd();
        assert_eq!(format_usd(cost.input_cost_usd), "$15.0000");
        assert_eq!(format_usd(cost.output_cost_usd), "$37.5000");
        let model_pricing =
            pricing_for_model("claude-sonnet-4-6").expect("known model pricing should resolve");
        let model_cost = usage.estimate_cost_usd_with_pricing(model_pricing);
        let lines = usage.summary_lines_for_model("usage", Some("claude-sonnet-4-6"));
        assert!(lines[0].contains(&format!(
            "estimated_cost={}",
            format_usd(model_cost.total_cost_usd())
        )));
        assert!(lines[0].contains("model=claude-sonnet-4-6"));
        assert!(lines[1].contains(&format!(
            "cache_read={}",
            format_usd(model_cost.cache_read_cost_usd)
        )));
    }

    #[test]
    fn supports_model_specific_pricing() {
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 500_000,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        };

        let haiku = pricing_for_model("claude-haiku-4-5-20251001").expect("haiku pricing");
        let opus = pricing_for_model("claude-opus-4-6").expect("opus pricing");
        let haiku_cost = usage.estimate_cost_usd_with_pricing(haiku);
        let opus_cost = usage.estimate_cost_usd_with_pricing(opus);
        assert_eq!(format_usd(haiku_cost.total_cost_usd()), "$3.5000");
        assert_eq!(format_usd(opus_cost.total_cost_usd()), "$52.5000");
    }

    #[test]
    fn marks_unknown_model_pricing_as_fallback() {
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 100,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        };
        let lines = usage.summary_lines_for_model("usage", Some("custom-model"));
        assert!(lines[0].contains("pricing=estimated-default"));
    }

    #[test]
    fn reconstructs_usage_from_session_messages() {
        let mut session = Session::new();
        session.messages = vec![ConversationMessage {
            role: MessageRole::Assistant,
            blocks: vec![ContentBlock::Text {
                text: "done".to_string(),
            }],
            usage: Some(TokenUsage {
                input_tokens: 5,
                output_tokens: 2,
                cache_creation_input_tokens: 1,
                cache_read_input_tokens: 0,
            }),
        }];

        let tracker = UsageTracker::from_session(&session);
        assert_eq!(tracker.turns(), 1);
        assert_eq!(tracker.cumulative_usage().total_tokens(), 8);
    }

    #[test]
    fn pricing_for_model_still_works() {
        // Verify money code is not broken by our changes
        assert!(pricing_for_model("claude-sonnet-4-6-20250514").is_some());
        assert!(pricing_for_model("claude-opus-4-6").is_some());
        assert!(pricing_for_model("claude-haiku-4-5-20251213").is_some());
    }

    #[test]
    fn model_performance_registry_routes_quick_and_deep_work_differently() {
        let mut registry = ModelPerformanceRegistry::new();
        registry.record_telemetry(
            &RunModelTelemetry {
                model: Some("stepfun-fast".to_string()),
                models_used: vec!["stepfun-fast".to_string()],
                first_token_latency_ms: Some(180),
                active_stream_duration_ms: Some(1_000),
                wall_duration_ms: 1_300,
                output_chars: 1_000,
                output_chunks: 10,
                input_tokens: 500,
                output_tokens: 180,
                cache_create_tokens: 0,
                cache_read_tokens: 0,
                total_tokens: 680,
                usage_source: "provider".to_string(),
                chars_per_second: Some(1_000.0),
                tokens_per_second: Some(180.0),
            },
            Some(0.72),
            false,
        );
        registry.record_telemetry(
            &RunModelTelemetry {
                model: Some("deepseek-depth".to_string()),
                models_used: vec!["deepseek-depth".to_string()],
                first_token_latency_ms: Some(900),
                active_stream_duration_ms: Some(4_000),
                wall_duration_ms: 5_000,
                output_chars: 4_000,
                output_chunks: 20,
                input_tokens: 900,
                output_tokens: 360,
                cache_create_tokens: 0,
                cache_read_tokens: 0,
                total_tokens: 1_260,
                usage_source: "provider".to_string(),
                chars_per_second: Some(1_000.0),
                tokens_per_second: Some(90.0),
            },
            Some(0.95),
            false,
        );

        let quick = registry.route(ModelRouteIntent::Quick, &[]);
        let deep = registry.route(ModelRouteIntent::Deep, &[]);

        assert_eq!(quick.selected_model, "stepfun-fast");
        assert_eq!(deep.selected_model, "deepseek-depth");
        assert_eq!(
            registry
                .stats_for("deepseek-depth")
                .expect("deep stats")
                .provider_usage_samples,
            1
        );
    }
}
