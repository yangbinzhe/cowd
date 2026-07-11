//! Runtime-owned token budget policy.
//!
//! Provider/model registry supplies model facts. Runtime owns execution budget
//! derivation and hands concrete leases to memory, tools, subagents, and
//! projection layers.

use serde::{Deserialize, Serialize};

use crate::context_runtime::ContextProfile;
pub const DEFAULT_CONTEXT_BUDGET_RATIO_BP: u32 = 7_000;
pub const MIN_CONTEXT_BUDGET_RATIO_BP: u32 = 1_000;
pub const MAX_CONTEXT_BUDGET_RATIO_BP: u32 = 9_500;
pub const FALLBACK_MODEL_CONTEXT_WINDOW: u32 = 8_000;
pub const DEFAULT_SUBAGENT_BUDGET_TOKENS: usize = 20_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBudgetInputs {
    pub model_context_window: u32,
    pub model_max_output_tokens: u32,
    pub context_budget_ratio_bp: u32,
    pub compact_threshold_ratio_bp: u32,
    pub profile: ContextProfile,
    pub autonomy_mode: Option<String>,
}

impl RuntimeBudgetInputs {
    #[must_use]
    pub fn new(model_context_window: u32, model_max_output_tokens: u32) -> Self {
        Self {
            model_context_window,
            model_max_output_tokens,
            context_budget_ratio_bp: DEFAULT_CONTEXT_BUDGET_RATIO_BP,
            compact_threshold_ratio_bp: DEFAULT_CONTEXT_BUDGET_RATIO_BP,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryBudgetLease {
    pub context_window: u64,
    pub retrieval_budget: u64,
    pub reserved_system: u64,
    pub reserved_response: u64,
    pub l0_reserved: u64,
    pub l1_working: u64,
    pub l2_project: u64,
    pub l3_deep: u64,
    pub l3_checkpoint: u64,
    pub l4_shared: u64,
    pub selected_item_limit: usize,
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
    pub effective_context_budget: u64,
    pub compaction_threshold_tokens: u64,
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
        let effective_context_budget =
            resolve_context_budget_tokens(model_context_window, inputs.context_budget_ratio_bp)
                as u64;
        let compaction_threshold_tokens =
            resolve_context_budget_tokens(model_context_window, inputs.compact_threshold_ratio_bp)
                as u64;

        let memory_context_window = effective_context_budget;
        let reserved_system = ((memory_context_window as f64 * 0.05).min(20_000.0)) as u64;
        let reserved_response = u64::from(inputs.model_max_output_tokens).min(32_000);
        let memory_available = memory_context_window
            .saturating_sub(reserved_system)
            .saturating_sub(reserved_response);

        let profile_multiplier = profile_multiplier(inputs.profile);
        let tool_total = scaled_budget(effective_context_budget, 0.08, 256, 120_000) as usize;
        let tool_single =
            scaled_budget(tool_total as u64, 0.24, 128, 32_000).min(tool_total as u64) as usize;
        let subagent_default_budget =
            scaled_budget(effective_context_budget, profile_multiplier, 512, 160_000);
        let team_total_budget = scaled_budget(effective_context_budget, 0.35, 1_024, 320_000);

        let memory_retrieval_budget = ((memory_available as f64 * 0.45) as u64).max(1);
        let selected_item_limit = ((memory_retrieval_budget / 600) as usize).clamp(12, 80);
        let l0_reserved = ((memory_retrieval_budget as f64 * 0.08) as u64).clamp(512, 8_000);
        let l1_working = ((memory_retrieval_budget as f64 * 0.20) as u64).max(1);
        let l2_project = ((memory_retrieval_budget as f64 * 0.22) as u64).max(1);
        let l3_deep = ((memory_retrieval_budget as f64 * 0.18) as u64).max(1);
        let l3_checkpoint = ((memory_retrieval_budget as f64 * 0.22) as u64).max(1);
        let l4_shared = memory_retrieval_budget
            .saturating_sub(l0_reserved)
            .saturating_sub(l1_working)
            .saturating_sub(l2_project)
            .saturating_sub(l3_deep)
            .saturating_sub(l3_checkpoint)
            .max(1);

        Self {
            model_context_window: u64::from(model_context_window),
            max_output_tokens: u64::from(inputs.model_max_output_tokens),
            effective_context_budget,
            compaction_threshold_tokens,
            memory_retrieval_budget: MemoryBudgetLease {
                context_window: memory_context_window,
                retrieval_budget: memory_retrieval_budget,
                reserved_system,
                reserved_response,
                l0_reserved,
                l1_working,
                l2_project,
                l3_deep,
                l3_checkpoint,
                l4_shared,
                selected_item_limit,
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
                yolo_budget_tokens: scaled_budget(effective_context_budget, 0.10, 256, 96_000),
                collaboration_budget_tokens: scaled_budget(
                    effective_context_budget,
                    1.0 / 12.0,
                    256,
                    80_000,
                ),
                review_budget_tokens: scaled_budget(
                    effective_context_budget,
                    1.0 / 16.0,
                    256,
                    64_000,
                ),
            },
            projection_budget_hint: scaled_budget(
                effective_context_budget,
                1.0 / 64.0,
                128,
                32_000,
            ),
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

#[must_use]
pub fn resolve_compact_threshold(model_ctx_window: u32, ratio_bp: u32) -> u32 {
    resolve_context_budget_tokens(model_ctx_window, ratio_bp)
}

fn profile_multiplier(profile: ContextProfile) -> f64 {
    match profile {
        ContextProfile::SubAgent => 0.18,
        ContextProfile::Collaboration => 0.22,
        ContextProfile::YoloGoal => 0.24,
        ContextProfile::SoloGoal => 0.22,
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
    fn budget_policy_defaults_to_seventy_percent_of_model_window() {
        let plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: 1_000_000,
            model_max_output_tokens: 32_000,
            context_budget_ratio_bp: 7_000,
            compact_threshold_ratio_bp: 7_000,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
        });

        assert_eq!(plan.effective_context_budget, 700_000);
        assert_eq!(plan.compaction_threshold_tokens, 700_000);
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

        assert_eq!(plan.model_context_window, 8_000);
        assert_eq!(plan.effective_context_budget, 5_600);
        assert!(plan.tool_result_budget.max_total_tokens < 5_600);
        assert!(plan.subagent_default_budget <= 5_600);
        assert!(plan.team_total_budget <= 5_600);
    }
}
