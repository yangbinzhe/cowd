//! Provider-request transport policy.
//!
//! This governs a single provider stream's idle tolerance. It never marks a
//! Goal complete or treats a client SSE disconnect as an execution failure.

use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderTransportPolicy {
    pub idle_timeout: Duration,
    pub heartbeat_grace: Duration,
}

impl ProviderTransportPolicy {
    #[must_use]
    pub fn derive(context_window: u32, message_chars: usize) -> Self {
        // Provider silence must be governed by the *actual* request, not by
        // the model's advertised maximum context. A 1M-capability model with
        // a 15K-token prompt should not receive a multi-minute no-first-byte
        // allowance merely because it could have accepted a much larger one.
        let estimated_prompt_tokens = u64::try_from(message_chars / 3).unwrap_or(u64::MAX).max(1);
        let prompt_scale = estimated_prompt_tokens
            .saturating_add(4_095)
            .saturating_div(4_096)
            .clamp(1, 32);
        let occupancy_basis_points = estimated_prompt_tokens
            .saturating_mul(10_000)
            .saturating_div(u64::from(context_window.max(8_000)))
            .clamp(0, 10_000);
        let pressure_scale = occupancy_basis_points.saturating_div(1_000).min(8);
        let idle_seconds = 35_u64
            .saturating_add(prompt_scale.saturating_mul(12))
            .saturating_add(pressure_scale.saturating_mul(6))
            .clamp(45, 480);
        Self {
            idle_timeout: Duration::from_secs(idle_seconds),
            heartbeat_grace: Duration::from_secs((idle_seconds / 3).clamp(15, 120)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn larger_context_or_request_gets_more_idle_tolerance() {
        let small = ProviderTransportPolicy::derive(32_768, 100);
        let large = ProviderTransportPolicy::derive(1_000_000, 20_000);
        assert!(large.idle_timeout > small.idle_timeout);
    }

    #[test]
    fn advertised_large_window_does_not_overextend_small_request_timeout() {
        let small_request = ProviderTransportPolicy::derive(1_000_000, 2_000);
        assert!(small_request.idle_timeout <= Duration::from_secs(60));
        assert!(small_request.heartbeat_grace <= Duration::from_secs(30));
    }
}
