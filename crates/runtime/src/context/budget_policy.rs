//! Runtime-owned token budget policy.
//!
//! Provider/model registry supplies model facts. Runtime owns execution budget
//! derivation and hands concrete leases to memory, tools, subagents, and
//! projection layers.

use serde::{Deserialize, Serialize};

use crate::context_runtime::ContextProfile;
pub const DEFAULT_SUBSYSTEM_BUDGET_RATIO_BP: u32 = 7_000;
pub const MIN_CONTEXT_BUDGET_RATIO_BP: u32 = 1_000;
pub const MAX_CONTEXT_BUDGET_RATIO_BP: u32 = 9_500;
pub const FALLBACK_MODEL_CONTEXT_WINDOW: u32 = 128_000;
pub const DEFAULT_SUBAGENT_BUDGET_TOKENS: usize = 20_000;
const MIN_PREFERRED_OUTPUT_TOKENS: u64 = 4_000;
const MAX_PREFERRED_OUTPUT_TOKENS: u64 = 32_000;
const MIN_OUTPUT_FLOOR_TOKENS: u64 = 2_000;
const MAX_OUTPUT_FLOOR_TOKENS: u64 = 8_000;

/// Facts available when Runtime materializes one concrete provider attempt.
/// `fixed_input_tokens` and `required_input_tokens` are request-local; no
/// subsystem budget is allowed to masquerade as provider capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderOutputBudgetInputs {
    pub context_window_tokens: u64,
    pub max_output_tokens: u64,
    pub fixed_input_tokens: u64,
    pub required_input_tokens: u64,
    pub protocol_overhead_tokens: u64,
    pub safety_margin_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderOutputBudget {
    pub preferred_output_tokens: u64,
    pub floor_output_tokens: u64,
    pub available_output_tokens: u64,
    pub requested_output_tokens: u64,
    pub executable: bool,
}

impl ProviderOutputBudget {
    /// Derive the per-attempt output lease:
    ///
    /// P = provider maximum output, W = context window, F = fixed request,
    /// R = required dynamic context, H = protocol overhead, S = safety margin.
    #[must_use]
    pub fn derive(inputs: ProviderOutputBudgetInputs) -> Self {
        let window = inputs.context_window_tokens;
        let scaled_preferred =
            (window / 16).clamp(MIN_PREFERRED_OUTPUT_TOKENS, MAX_PREFERRED_OUTPUT_TOKENS);
        let preferred_output_tokens = inputs
            .max_output_tokens
            .min(window / 2)
            .min(scaled_preferred);
        let scaled_floor = (window / 64).clamp(MIN_OUTPUT_FLOOR_TOKENS, MAX_OUTPUT_FLOOR_TOKENS);
        let floor_output_tokens = preferred_output_tokens.min(scaled_floor);
        let available_output_tokens = window
            .saturating_sub(inputs.fixed_input_tokens)
            .saturating_sub(inputs.required_input_tokens)
            .saturating_sub(inputs.protocol_overhead_tokens)
            .saturating_sub(inputs.safety_margin_tokens);
        let executable = available_output_tokens >= floor_output_tokens;
        Self {
            preferred_output_tokens,
            floor_output_tokens,
            available_output_tokens,
            requested_output_tokens: if executable {
                preferred_output_tokens.min(available_output_tokens)
            } else {
                0
            },
            executable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBudgetInputs {
    pub model_context_window: u32,
    pub model_max_output_tokens: u32,
    /// Runtime-internal resource allocation only. It must not cap provider
    /// request packing or trigger session compaction.
    pub subsystem_budget_ratio_bp: u32,
    pub profile: ContextProfile,
    pub autonomy_mode: Option<String>,
}

impl RuntimeBudgetInputs {
    #[must_use]
    pub fn new(model_context_window: u32, model_max_output_tokens: u32) -> Self {
        Self {
            model_context_window,
            model_max_output_tokens,
            subsystem_budget_ratio_bp: DEFAULT_SUBSYSTEM_BUDGET_RATIO_BP,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryBudgetLease {
    pub context_window: u64,
    /// Broad source-scan budget. Runtime's adaptive allocator owns final
    /// selection after Memory, Session, Reality, and Mission are combined.
    pub retrieval_budget: u64,
    pub reserved_system: u64,
    pub reserved_response: u64,
    /// Resource-safety ceiling for candidate discovery, not a final context
    /// item cap. It scales with the effective model window.
    pub candidate_scan_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutputBudgetLease {
    pub max_total_tokens: usize,
    pub per_tool_max_tokens: usize,
    pub head_chars: usize,
    pub tail_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeControlBudgetLease {
    pub yolo_budget_tokens: u64,
    pub collaboration_budget_tokens: u64,
    pub review_budget_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBudgetPlan {
    pub model_context_window: u64,
    pub max_output_tokens: u64,
    pub subsystem_budget_tokens: u64,
    pub memory_retrieval_budget: MemoryBudgetLease,
    pub tool_result_budget: ToolOutputBudgetLease,
    pub subagent_default_budget: u64,
    pub team_total_budget: u64,
    pub runtime_control_budget: RuntimeControlBudgetLease,
    pub projection_budget_hint: u64,
}

impl RuntimeBudgetPlan {
    #[must_use]
    pub fn derive(inputs: RuntimeBudgetInputs) -> Self {
        let model_context_window = effective_model_window(inputs.model_context_window);
        let subsystem_budget_tokens =
            resolve_context_budget_tokens(model_context_window, inputs.subsystem_budget_ratio_bp)
                as u64;

        let memory_context_window = subsystem_budget_tokens;
        let reserved_system = ((memory_context_window as f64 * 0.05).min(20_000.0)) as u64;
        let reserved_response = u64::from(inputs.model_max_output_tokens).min(32_000);
        let memory_available = memory_context_window
            .saturating_sub(reserved_system)
            .saturating_sub(reserved_response);

        let profile_multiplier = profile_multiplier(inputs.profile);
        let tool_total = scaled_budget(subsystem_budget_tokens, 0.08, 256, 120_000) as usize;
        let tool_single =
            scaled_budget(tool_total as u64, 0.24, 128, 32_000).min(tool_total as u64) as usize;
        let subagent_default_budget =
            scaled_budget(subsystem_budget_tokens, profile_multiplier, 512, 160_000);
        let team_total_budget = scaled_budget(subsystem_budget_tokens, 0.35, 1_024, 320_000);

        let memory_retrieval_budget = memory_available.max(1);
        let candidate_scan_limit = usize::try_from(memory_retrieval_budget / 256)
            .unwrap_or(4_096)
            .clamp(32, 4_096);

        Self {
            model_context_window: u64::from(model_context_window),
            max_output_tokens: u64::from(inputs.model_max_output_tokens),
            subsystem_budget_tokens,
            memory_retrieval_budget: MemoryBudgetLease {
                context_window: memory_context_window,
                retrieval_budget: memory_retrieval_budget,
                reserved_system,
                reserved_response,
                candidate_scan_limit,
            },
            tool_result_budget: ToolOutputBudgetLease {
                max_total_tokens: tool_total,
                per_tool_max_tokens: tool_single,
                head_chars: 3_000,
                tail_chars: 2_000,
            },
            subagent_default_budget,
            team_total_budget,
            runtime_control_budget: RuntimeControlBudgetLease {
                yolo_budget_tokens: scaled_budget(subsystem_budget_tokens, 0.10, 256, 96_000),
                collaboration_budget_tokens: scaled_budget(
                    subsystem_budget_tokens,
                    1.0 / 12.0,
                    256,
                    80_000,
                ),
                review_budget_tokens: scaled_budget(
                    subsystem_budget_tokens,
                    1.0 / 16.0,
                    256,
                    64_000,
                ),
            },
            projection_budget_hint: scaled_budget(subsystem_budget_tokens, 1.0 / 64.0, 128, 32_000),
        }
    }
}

#[must_use]
pub fn effective_model_window(model_context_window: u32) -> u32 {
    if model_context_window == 0 {
        FALLBACK_MODEL_CONTEXT_WINDOW
    } else {
        model_context_window
    }
}

#[must_use]
pub fn clamp_context_budget_ratio_bp(ratio_bp: u32) -> u32 {
    ratio_bp.clamp(MIN_CONTEXT_BUDGET_RATIO_BP, MAX_CONTEXT_BUDGET_RATIO_BP)
}

#[must_use]
pub fn resolve_context_budget_tokens(model_ctx_window: u32, ratio_bp: u32) -> u32 {
    let model_ctx_window = effective_model_window(model_ctx_window);
    let ratio_bp = clamp_context_budget_ratio_bp(ratio_bp);
    let budget = (u64::from(model_ctx_window) * u64::from(ratio_bp)) / 10_000;
    budget.min(u64::from(u32::MAX)) as u32
}

fn profile_multiplier(profile: ContextProfile) -> f64 {
    match profile {
        ContextProfile::SubAgent => 0.18,
        ContextProfile::Collaboration => 0.22,
        ContextProfile::YoloGoal => 0.24,
        ContextProfile::AutonomousGoal => 0.22,
        ContextProfile::Review => 0.16,
        ContextProfile::Resume => 0.14,
        ContextProfile::Cron => 0.14,
        ContextProfile::SurfaceQuickReply => 0.08,
        ContextProfile::SurfaceTaskIntake => 0.12,
        ContextProfile::DeepInvestigation => 0.28,
        ContextProfile::MainTurn => 0.20,
    }
}

fn scaled_budget(total: u64, ratio: f64, floor: u64, ceiling: u64) -> u64 {
    let upper = ceiling.min(total).max(1);
    let lower = floor.min(upper);
    ((total as f64 * ratio) as u64).clamp(lower, upper)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subsystem_budget_is_separate_from_request_capacity() {
        let plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: 1_000_000,
            model_max_output_tokens: 32_000,
            subsystem_budget_ratio_bp: 7_000,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
        });

        assert_eq!(plan.subsystem_budget_tokens, 700_000);
        assert!(plan.memory_retrieval_budget.context_window > 200_000);
        assert!(plan.subagent_default_budget > DEFAULT_SUBAGENT_BUDGET_TOKENS as u64);
        assert!(plan.tool_result_budget.per_tool_max_tokens > 10_000);
    }

    #[test]
    fn budget_policy_ratio_is_clamped_to_safe_bounds() {
        assert_eq!(resolve_context_budget_tokens(1_000_000, 99_999), 950_000);
        assert_eq!(resolve_context_budget_tokens(1_000_000, 1), 100_000);
    }

    #[test]
    fn budget_policy_falls_back_when_model_window_unknown() {
        let plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs::new(0, 4_096));

        assert_eq!(plan.model_context_window, 128_000);
        assert_eq!(plan.subsystem_budget_tokens, 89_600);
        assert!(plan.tool_result_budget.max_total_tokens < 89_600);
        assert!(plan.subagent_default_budget <= 89_600);
        assert!(plan.team_total_budget <= 89_600);
    }

    #[test]
    fn memory_candidate_scan_scales_with_effective_model_window() {
        let small = RuntimeBudgetPlan::derive(RuntimeBudgetInputs::new(8_000, 2_000));
        let medium = RuntimeBudgetPlan::derive(RuntimeBudgetInputs::new(128_000, 8_000));
        let large = RuntimeBudgetPlan::derive(RuntimeBudgetInputs::new(1_000_000, 32_000));

        assert!(
            small.memory_retrieval_budget.candidate_scan_limit
                < medium.memory_retrieval_budget.candidate_scan_limit
        );
        assert!(
            medium.memory_retrieval_budget.candidate_scan_limit
                < large.memory_retrieval_budget.candidate_scan_limit
        );
        assert!(large.memory_retrieval_budget.candidate_scan_limit > 80);
    }

    #[test]
    fn provider_output_budget_scales_without_starving_input() {
        let budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
            context_window_tokens: 16_384,
            max_output_tokens: 64_000,
            fixed_input_tokens: 6_000,
            required_input_tokens: 2_000,
            protocol_overhead_tokens: 200,
            safety_margin_tokens: 164,
        });

        assert!(budget.executable);
        assert_eq!(budget.preferred_output_tokens, 4_000);
        assert_eq!(budget.floor_output_tokens, 2_000);
        assert_eq!(budget.requested_output_tokens, 4_000);
    }

    #[test]
    fn provider_output_budget_rejects_when_minimum_continuation_cannot_fit() {
        let budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
            context_window_tokens: 16_384,
            max_output_tokens: 64_000,
            fixed_input_tokens: 13_000,
            required_input_tokens: 1_500,
            protocol_overhead_tokens: 200,
            safety_margin_tokens: 164,
        });

        assert!(!budget.executable);
        assert_eq!(budget.requested_output_tokens, 0);
        assert!(budget.available_output_tokens < budget.floor_output_tokens);
    }

    #[test]
    fn provider_output_budget_shrinks_to_real_remaining_capacity() {
        let budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
            context_window_tokens: 16_384,
            max_output_tokens: 64_000,
            fixed_input_tokens: 10_000,
            required_input_tokens: 3_000,
            protocol_overhead_tokens: 200,
            safety_margin_tokens: 184,
        });

        assert!(budget.executable);
        assert_eq!(budget.available_output_tokens, 3_000);
        assert_eq!(budget.requested_output_tokens, 3_000);
    }

    #[test]
    fn provider_output_budget_honors_small_provider_maximum() {
        let budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
            context_window_tokens: 1_000_000,
            max_output_tokens: 6_000,
            fixed_input_tokens: 10_000,
            required_input_tokens: 10_000,
            protocol_overhead_tokens: 128,
            safety_margin_tokens: 2_048,
        });

        assert_eq!(budget.preferred_output_tokens, 6_000);
        assert_eq!(budget.floor_output_tokens, 6_000);
        assert_eq!(budget.requested_output_tokens, 6_000);
    }

    #[test]
    fn provider_output_budget_matches_supported_window_matrix() {
        for (window, provider_max, expected_preferred, expected_floor) in [
            (16_384, 64_000, 4_000, 2_000),
            (128_000, 64_000, 8_000, 2_000),
            (1_000_000, 384_000, 32_000, 8_000),
        ] {
            let budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
                context_window_tokens: window,
                max_output_tokens: provider_max,
                fixed_input_tokens: 1_000,
                required_input_tokens: 1_000,
                protocol_overhead_tokens: 128,
                safety_margin_tokens: 128,
            });
            assert!(budget.executable, "window={window}");
            assert_eq!(
                budget.preferred_output_tokens, expected_preferred,
                "window={window}"
            );
            assert_eq!(
                budget.floor_output_tokens, expected_floor,
                "window={window}"
            );
        }
    }
}
