/// Raw token counters reported by a provider for one completion.
///
/// These values are technical telemetry: context packing, output clipping,
/// rate-limit observation, and performance diagnostics. They must never be
/// converted into money, authorization, approval, or execution outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_creation_input_tokens: u32,
    pub cache_read_input_tokens: u32,
}

impl TokenUsage {
    /// Provider-billed prompt tokens for this attempt.
    ///
    /// `input_tokens` is the uncached/miss portion after provider-specific
    /// normalization. Explicit cache creation and cache reads are separate
    /// portions and must all remain in the denominator.
    #[must_use]
    pub fn prompt_input_tokens(self) -> u64 {
        u64::from(self.input_tokens)
            .saturating_add(u64::from(self.cache_creation_input_tokens))
            .saturating_add(u64::from(self.cache_read_input_tokens))
    }

    /// Cache-read ratio over all Provider prompt input, in basis points.
    /// Returns zero when the Provider reported no prompt usage.
    #[must_use]
    pub fn cache_hit_ratio_bp(self) -> u32 {
        let prompt = self.prompt_input_tokens();
        if prompt == 0 {
            return 0;
        }
        u32::try_from(u64::from(self.cache_read_input_tokens).saturating_mul(10_000) / prompt)
            .unwrap_or(10_000)
            .min(10_000)
    }

    #[must_use]
    pub fn total_tokens(self) -> u32 {
        self.input_tokens
            + self.output_tokens
            + self.cache_creation_input_tokens
            + self.cache_read_input_tokens
    }

    #[must_use]
    pub fn summary_lines(self, label: &str) -> Vec<String> {
        vec![format!(
            "{label}: total_tokens={} input={} output={} cache_write={} cache_read={}",
            self.total_tokens(),
            self.input_tokens,
            self.output_tokens,
            self.cache_creation_input_tokens,
            self.cache_read_input_tokens,
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::TokenUsage;

    #[test]
    fn summarizes_technical_token_usage_without_money() {
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 500_000,
            cache_creation_input_tokens: 100_000,
            cache_read_input_tokens: 200_000,
        };

        let lines = usage.summary_lines("usage");
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("total_tokens=1800000"));
        assert!(!lines[0].contains("cost"));
        assert!(!lines[0].contains('$'));
    }

    #[test]
    fn cache_ratio_includes_uncached_and_creation_input() {
        let usage = TokenUsage {
            input_tokens: 7_101,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 3_328,
        };
        assert_eq!(usage.prompt_input_tokens(), 10_429);
        assert_eq!(usage.cache_hit_ratio_bp(), 3_191);

        let with_creation = TokenUsage {
            input_tokens: 2,
            output_tokens: 99,
            cache_creation_input_tokens: 3,
            cache_read_input_tokens: 5,
        };
        assert_eq!(with_creation.cache_hit_ratio_bp(), 5_000);
        assert_eq!(TokenUsage::default().cache_hit_ratio_bp(), 0);
    }
}
