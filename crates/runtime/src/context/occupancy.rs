//! Shared-context occupancy estimation.
//!
//! Before a multi-agent/team collaboration starts, operators (and the
//! strategy layer) need a prediction of how much model context each role will
//! occupy: its base prompt, its own evidence, and the coordination content it
//! consumes (team_board revisions etc.). This module is a pure estimator; it
//! never admits or rejects execution.

/// Approximate token footprint for UTF-8 text (~0.75 tokens/char for mixed
/// CJK/ASCII content, clamped to a minimum of 1).
#[must_use]
pub fn estimate_text_tokens(chars: usize) -> u64 {
    (u64::try_from(chars).unwrap_or(u64::MAX) * 3 / 4).max(1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextOccupancyEstimate {
    /// Team/agent/role owner of this estimate.
    pub owner: String,
    pub base_prompt_tokens: u64,
    pub evidence_tokens: u64,
    pub coordination_tokens: u64,
    pub window_tokens: u64,
    /// Estimated utilization in basis points of the model window.
    pub utilization_bp: u32,
}

#[must_use]
pub fn estimate_role_occupancy(
    owner: impl Into<String>,
    base_prompt_chars: usize,
    evidence_chars: usize,
    coordination_chars: usize,
    window_tokens: u64,
) -> ContextOccupancyEstimate {
    let base_prompt_tokens = estimate_text_tokens(base_prompt_chars);
    let evidence_tokens = estimate_text_tokens(evidence_chars);
    let coordination_tokens = estimate_text_tokens(coordination_chars);
    let total = base_prompt_tokens
        .saturating_add(evidence_tokens)
        .saturating_add(coordination_tokens);
    let utilization_bp = if window_tokens == 0 {
        0
    } else {
        u32::try_from(
            total
                .saturating_mul(10_000)
                .checked_div(window_tokens)
                .unwrap_or(u64::MAX),
        )
        .unwrap_or(10_000)
        .min(10_000)
    };
    ContextOccupancyEstimate {
        owner: owner.into(),
        base_prompt_tokens,
        evidence_tokens,
        coordination_tokens,
        window_tokens,
        utilization_bp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_estimation_is_positive_and_roughly_three_quarters_per_char() {
        assert_eq!(estimate_text_tokens(0), 1);
        assert_eq!(estimate_text_tokens(4), 3);
        assert_eq!(estimate_text_tokens(100), 75);
    }

    #[test]
    fn role_occupancy_sums_components_and_reports_utilization() {
        let estimate = estimate_role_occupancy(
            "cto",
            4_000,
            8_000,
            2_000,
            100_000,
        );
        assert_eq!(estimate.base_prompt_tokens, 3_000);
        assert_eq!(estimate.evidence_tokens, 6_000);
        assert_eq!(estimate.coordination_tokens, 1_500);
        assert_eq!(estimate.utilization_bp, 1_050);
    }

    #[test]
    fn utilization_is_clamped_to_full_window() {
        let estimate = estimate_role_occupancy("it", 10_000, 10_000, 10_000, 1_000);
        assert_eq!(estimate.utilization_bp, 10_000);
    }
}
