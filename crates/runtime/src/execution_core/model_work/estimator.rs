use harness_contract::execution_graph::{project_work_graph, ExecutionGraph};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelWorkTopology {
    InlineBatch,
    Pipelined,
    Team,
    Downgraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelWorkEstimateInput {
    pub provider_effective_limit: usize,
    pub provider_available: usize,
    pub tool_available: usize,
    pub agent_available: usize,
    pub provider_queue_p95_ms: u64,
    pub provider_service_p95_ms: u64,
    pub provider_failure_timeout_upper_bound_basis_points: u16,
    pub provider_samples: usize,
    pub merge_overhead_ms: u64,
    pub maximum_token_amplification_basis_points: u32,
    pub minimum_speedup_basis_points: u32,
    pub requires_cross_check: bool,
    /// The user explicitly required this parallel/Team topology. Observed cost
    /// may surface a warning, but only hard capacity may reject it. Automatic
    /// topology selection remains governed by the optimization thresholds.
    pub user_mandated_topology: bool,
}

impl Default for ModelWorkEstimateInput {
    fn default() -> Self {
        Self {
            provider_effective_limit: 1,
            provider_available: 1,
            tool_available: 1,
            agent_available: 1,
            provider_queue_p95_ms: 0,
            provider_service_p95_ms: 0,
            provider_failure_timeout_upper_bound_basis_points: 0,
            provider_samples: 0,
            merge_overhead_ms: 0,
            maximum_token_amplification_basis_points: 30_000,
            minimum_speedup_basis_points: 11_000,
            requires_cross_check: false,
            user_mandated_topology: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelWorkEstimate {
    pub topology: ModelWorkTopology,
    pub automatic: bool,
    pub width: usize,
    pub expected_serial_ms: u64,
    pub expected_parallel_ms: u64,
    pub expected_speedup_basis_points: Option<u32>,
    pub token_amplification_basis_points: Option<u32>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ModelWorkGraphEstimator;

impl ModelWorkGraphEstimator {
    #[must_use]
    pub fn estimate(
        &self,
        graph: &ExecutionGraph,
        input: &ModelWorkEstimateInput,
    ) -> ModelWorkEstimate {
        let projection = project_work_graph(graph).unwrap_or_default();
        let automatic = input.provider_samples >= 3;
        let requires_tool = graph.nodes.iter().any(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
        });
        let requires_agent = graph.nodes.iter().any(|node| {
            matches!(
                node.kind,
                harness_contract::execution_graph::ExecutionNodeKind::AgentTask
                    | harness_contract::execution_graph::ExecutionNodeKind::Subgraph
            )
        });
        let mut usable_width = projection
            .width
            .min(input.provider_effective_limit)
            .min(input.provider_available);
        if requires_tool {
            usable_width = usable_width.min(input.tool_available);
        }
        if requires_agent {
            usable_width = usable_width.min(input.agent_available);
        }
        let capacity_floor_ms = if usable_width == 0 {
            projection.expected_serial_ms
        } else {
            projection
                .expected_serial_ms
                .saturating_add(usable_width as u64 - 1)
                .saturating_div(usable_width as u64)
        };
        let expected_parallel_ms = projection
            .expected_critical_path_ms
            .max(capacity_floor_ms)
            .saturating_add(input.merge_overhead_ms);
        let expected_speedup_basis_points = (expected_parallel_ms > 0).then(|| {
            projection
                .expected_serial_ms
                .saturating_mul(10_000)
                .saturating_div(expected_parallel_ms)
                .min(u64::from(u32::MAX)) as u32
        });
        let largest_node_tokens = graph
            .nodes
            .iter()
            .filter_map(|node| node.work.as_ref())
            .map(|work| {
                work.expected_input_tokens
                    .saturating_add(work.expected_output_tokens)
            })
            .max()
            .unwrap_or_default();
        let total_tokens = graph
            .nodes
            .iter()
            .filter_map(|node| node.work.as_ref())
            .fold(0_u64, |total, work| {
                total
                    .saturating_add(work.expected_input_tokens)
                    .saturating_add(work.expected_output_tokens)
            });
        let token_amplification_basis_points = (largest_node_tokens > 0).then(|| {
            total_tokens
                .saturating_mul(10_000)
                .saturating_div(largest_node_tokens)
                .min(u64::from(u32::MAX)) as u32
        });
        let mut reasons = Vec::new();
        if projection.width <= 1 {
            reasons.push("graph_has_no_parallel_frontier".to_string());
        }
        if usable_width <= 1 && projection.width > 1 {
            reasons.push("provider_parallel_capacity_unavailable".to_string());
        }
        if requires_tool && input.tool_available == 0 {
            reasons.push("tool_parallel_capacity_unavailable".to_string());
        }
        if requires_agent && input.agent_available == 0 {
            reasons.push("agent_parallel_capacity_unavailable".to_string());
        }
        if automatic && input.provider_failure_timeout_upper_bound_basis_points >= 2_000 {
            reasons.push("provider_failure_upper_bound_is_high".to_string());
        }
        if automatic
            && input.provider_queue_p95_ms > input.provider_service_p95_ms.max(1).saturating_mul(2)
        {
            reasons.push("provider_queue_dominates_service_time".to_string());
        }
        if automatic
            && expected_speedup_basis_points
                .is_some_and(|speedup| speedup < input.minimum_speedup_basis_points)
        {
            reasons.push("parallel_speedup_below_threshold".to_string());
        }
        if automatic
            && token_amplification_basis_points.is_some_and(|amplification| {
                amplification > input.maximum_token_amplification_basis_points
            })
        {
            reasons.push("token_amplification_above_threshold".to_string());
        }
        let hard_rejected = projection.width > 1 && usable_width <= 1;
        let optimization_rejected = automatic
            && reasons.iter().any(|reason| {
                matches!(
                    reason.as_str(),
                    "provider_failure_upper_bound_is_high"
                        | "provider_queue_dominates_service_time"
                        | "parallel_speedup_below_threshold"
                        | "token_amplification_above_threshold"
                )
            });
        if input.user_mandated_topology && optimization_rejected {
            reasons.push("user_mandated_topology_retained_with_cost_warning".to_string());
        }
        let observed_rejected = !input.user_mandated_topology && optimization_rejected;
        let topology = if hard_rejected || observed_rejected {
            ModelWorkTopology::Downgraded
        } else if projection.width <= 1 {
            ModelWorkTopology::InlineBatch
        } else if input.requires_cross_check {
            ModelWorkTopology::Team
        } else {
            ModelWorkTopology::Pipelined
        };
        ModelWorkEstimate {
            topology,
            automatic,
            width: usable_width,
            expected_serial_ms: projection.expected_serial_ms,
            expected_parallel_ms,
            expected_speedup_basis_points,
            token_amplification_basis_points,
            reasons,
        }
    }
}
