use crate::session::Session;

use model_protocol::usage::TokenUsage;

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

    /// Cache hit ratio in basis points (0..=10000) over all Provider prompt
    /// input. The normalized uncached input is part of the denominator.
    /// `0` means no cacheable input was observed yet.
    #[must_use]
    pub fn cache_hit_ratio_bp(&self) -> u32 {
        self.cumulative.cache_hit_ratio_bp()
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
    use model_protocol::usage::TokenUsage;

    use super::UsageTracker;

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
        assert_eq!(tracker.cache_hit_ratio_bp(), 4_444);
        assert_eq!(tracker.cache_saved_tokens(), 8);

        let empty = UsageTracker::new();
        assert_eq!(empty.cache_hit_ratio_bp(), 0);
        assert_eq!(empty.cache_saved_tokens(), 0);
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
}
