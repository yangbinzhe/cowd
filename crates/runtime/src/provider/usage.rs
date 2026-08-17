use crate::session::Session;

use model_protocol::usage::{ModelPricing, TokenUsage};

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

impl UsageTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn from_session(session: &Session) -> Self {
        let mut tracker = Self::new();
        for message in session.messages() {
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

    /// Cache hit ratio in basis points (0..=10000) over billed input tokens.
    /// `0` means no cacheable input was observed yet.
    #[must_use]
    pub fn cache_hit_ratio_bp(&self) -> u32 {
        let read = u64::from(self.cumulative.cache_read_input_tokens);
        let creation = u64::from(self.cumulative.cache_creation_input_tokens);
        let billed = read.saturating_add(creation);
        if billed == 0 {
            0
        } else {
            u32::try_from(read.saturating_mul(10_000) / billed).unwrap_or(0)
        }
    }

    /// Tokens served from provider cache across the whole session.
    #[must_use]
    pub fn cache_saved_tokens(&self) -> u64 {
        u64::from(self.cumulative.cache_read_input_tokens)
    }

    #[must_use]
    pub fn turns(&self) -> u32 {
        self.turns
    }
}

#[cfg(test)]
mod tests {
    use crate::session::{ContentBlock, ConversationMessage, MessageRole, Session};
    use model_protocol::usage::{format_usd, TokenUsage};

    use super::{pricing_for_model, UsageTracker};

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
    fn cache_hit_ratio_and_saved_tokens_are_reported() {
        let mut tracker = UsageTracker::new();
        tracker.record(TokenUsage {
            input_tokens: 8,
            output_tokens: 4,
            cache_creation_input_tokens: 2,
            cache_read_input_tokens: 8,
        });
        assert_eq!(tracker.cache_hit_ratio_bp(), 8_000);
        assert_eq!(tracker.cache_saved_tokens(), 8);

        let empty = UsageTracker::new();
        assert_eq!(empty.cache_hit_ratio_bp(), 0);
        assert_eq!(empty.cache_saved_tokens(), 0);
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
        session.replace_messages(vec![ConversationMessage {
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
        }]);

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
}
