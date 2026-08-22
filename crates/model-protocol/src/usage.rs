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
}
