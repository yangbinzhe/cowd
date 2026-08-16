use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{
    ExecutionAcceptance, ExecutionDependencyPolicy, ExecutionEdgeKind, ExecutionFailure,
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeStatus, ExecutionOrchestrationMetadata,
    ExecutionParentBinding, ExecutionServiceClass, ExecutionUsage, ExecutionWorkContract,
    ExecutionWorkRole,
};
use crate::context::EvidenceAccessRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionNodeProjection {
    pub node_id: String,
    pub kind: ExecutionNodeKind,
    pub status: ExecutionNodeStatus,
    pub executor_kind: String,
    /// Safe input identity for surfaces. The referenced payload and private
    /// prompt remain Runtime-owned and must be resolved through governed
    /// evidence or activity projections.
    #[serde(default)]
    pub payload_ref: String,
    #[serde(default)]
    pub acceptance: ExecutionAcceptance,
    #[serde(default)]
    pub resource_scopes: Vec<String>,
    pub result_ref: Option<String>,
    /// Bounded semantic output suitable for operator inspection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<ExecutionFailure>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    /// Canonical node-level usage. Keeping it on the projection makes
    /// execution metrics traceable across nested graphs without asking a
    /// surface to infer cost from prose timeline events.
    #[serde(default)]
    pub usage: ExecutionUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<ExecutionWorkProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionWorkProjection {
    pub role: ExecutionWorkRole,
    pub required: bool,
    pub dependency: ExecutionDependencyPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_group: Option<String>,
    pub expected_input_tokens: u64,
    pub expected_output_tokens: u64,
    pub expected_duration_ms: u64,
}

impl From<&ExecutionWorkContract> for ExecutionWorkProjection {
    fn from(work: &ExecutionWorkContract) -> Self {
        Self {
            role: work.role,
            required: work.required,
            dependency: work.dependency.clone(),
            cancellation_group: work.cancellation_group.clone(),
            expected_input_tokens: work.expected_input_tokens,
            expected_output_tokens: work.expected_output_tokens,
            expected_duration_ms: work.expected_duration_ms,
        }
    }
}

/// Read-only graph relation safe for surfaces. Execution payloads and private
/// prompts stay in Runtime; consumers only need stable topology to render and
/// control the durable graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionEdgeProjection {
    pub from: String,
    pub to: String,
    pub kind: ExecutionEdgeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionGraphProjection {
    pub graph_id: String,
    pub revision: u64,
    pub objective: String,
    #[serde(default)]
    pub service_class: ExecutionServiceClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<super::ExecutionGraphLineage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestration: Option<ExecutionOrchestrationMetadata>,
    pub nodes: Vec<ExecutionNodeProjection>,
    pub edges: Vec<ExecutionEdgeProjection>,
    pub commit_cursor: u64,
    pub terminal_result_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_envelope: Option<crate::outcome::DeliveryEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_presentation: Option<crate::outcome::TerminalPresentation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<ExecutionWorkGraphProjection>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionWorkGraphProjection {
    pub node_count: usize,
    pub width: usize,
    pub depth: usize,
    pub expected_serial_ms: u64,
    pub expected_critical_path_ms: u64,
    pub expected_speedup_basis_points: Option<u32>,
    pub actual_serial_ms: u64,
    pub actual_critical_path_ms: u64,
    pub actual_speedup_basis_points: Option<u32>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub optional_nodes: usize,
    pub cancelled_optional_nodes: usize,
}

#[must_use]
pub fn project_execution_graph(graph: &ExecutionGraph) -> ExecutionGraphProjection {
    ExecutionGraphProjection {
        graph_id: graph.id.clone(),
        revision: graph.revision,
        objective: graph.objective.clone(),
        service_class: graph.service_class,
        parent_execution: graph.parent_execution.clone(),
        lineage: graph.lineage.clone(),
        orchestration: graph.orchestration.clone(),
        nodes: graph
            .nodes
            .iter()
            .map(|node| {
                let result = graph.node_results.get(&node.id);
                ExecutionNodeProjection {
                    node_id: node.id.clone(),
                    kind: node.kind,
                    status: graph
                        .node_statuses
                        .get(&node.id)
                        .copied()
                        .unwrap_or(ExecutionNodeStatus::Planned),
                    executor_kind: node.executor_kind.clone(),
                    payload_ref: node.payload_ref.clone(),
                    acceptance: node.acceptance.clone(),
                    resource_scopes: node.resource_scopes.clone(),
                    result_ref: result.and_then(|value| value.result_ref.clone()),
                    summary: result.and_then(|value| value.summary.clone()),
                    failure: result.and_then(|value| value.failure.clone()),
                    evidence_refs: result
                        .map(|value| value.evidence_refs.clone())
                        .unwrap_or_default(),
                    usage: result.map(|value| value.usage.clone()).unwrap_or_default(),
                    work: node.work.as_ref().map(ExecutionWorkProjection::from),
                }
            })
            .collect(),
        edges: graph
            .edges
            .iter()
            .map(|edge| ExecutionEdgeProjection {
                from: edge.from.clone(),
                to: edge.to.clone(),
                kind: edge.kind,
            })
            .collect(),
        commit_cursor: graph.recovery_cursor.commit_cursor,
        terminal_result_ref: graph
            .nodes
            .iter()
            .rev()
            .filter(|node| node.kind == ExecutionNodeKind::Synthesize)
            .find_map(|node| graph.node_results.get(&node.id))
            .and_then(|result| result.result_ref.clone()),
        delivery_envelope: graph.delivery_envelope.clone(),
        terminal_presentation: graph.terminal_presentation.clone(),
        work: project_work_graph(graph),
    }
}

#[must_use]
pub fn project_work_graph(graph: &ExecutionGraph) -> Option<ExecutionWorkGraphProjection> {
    let work_nodes = graph
        .nodes
        .iter()
        .filter(|node| node.work.is_some())
        .collect::<Vec<_>>();
    if work_nodes.is_empty() {
        return None;
    }
    let work_ids = work_nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    let dependencies = graph
        .edges
        .iter()
        .filter(|edge| {
            edge.kind == ExecutionEdgeKind::DependsOn
                && work_ids.contains(edge.from.as_str())
                && work_ids.contains(edge.to.as_str())
        })
        .collect::<Vec<_>>();
    let expected = work_nodes
        .iter()
        .map(|node| {
            (
                node.id.as_str(),
                node.work
                    .as_ref()
                    .map_or(0, |work| work.expected_duration_ms),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let actual = work_nodes
        .iter()
        .map(|node| {
            (
                node.id.as_str(),
                graph
                    .node_results
                    .get(&node.id)
                    .map_or(0, |result| result.usage.duration_ms),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let policies = work_nodes
        .iter()
        .map(|node| {
            (
                node.id.as_str(),
                node.work
                    .as_ref()
                    .map(|work| work.dependency.clone())
                    .unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let required_ids = work_nodes
        .iter()
        .filter_map(|node| {
            node.work
                .as_ref()
                .is_none_or(|work| work.required)
                .then_some(node.id.as_str())
        })
        .collect::<BTreeSet<_>>();
    let (width, depth) = graph_shape(&work_ids, &dependencies);
    let expected_serial_ms = expected.values().copied().sum();
    let actual_serial_ms = actual.values().copied().sum();
    let expected_critical_path_ms = critical_path(
        &work_ids,
        &required_ids,
        &dependencies,
        &expected,
        &policies,
        None,
    );
    let actual_critical_path_ms = critical_path(
        &work_ids,
        &required_ids,
        &dependencies,
        &actual,
        &policies,
        Some(&graph.node_statuses),
    );
    let usage = work_nodes
        .iter()
        .filter_map(|node| graph.node_results.get(&node.id))
        .fold(ExecutionUsage::default(), |mut total, result| {
            total.input_tokens = total.input_tokens.saturating_add(result.usage.input_tokens);
            total.output_tokens = total
                .output_tokens
                .saturating_add(result.usage.output_tokens);
            total.cached_tokens = total
                .cached_tokens
                .saturating_add(result.usage.cached_tokens);
            total
        });
    Some(ExecutionWorkGraphProjection {
        node_count: work_nodes.len(),
        width,
        depth,
        expected_serial_ms,
        expected_critical_path_ms,
        expected_speedup_basis_points: speedup_basis_points(
            expected_serial_ms,
            expected_critical_path_ms,
        ),
        actual_serial_ms,
        actual_critical_path_ms,
        actual_speedup_basis_points: speedup_basis_points(
            actual_serial_ms,
            actual_critical_path_ms,
        ),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cached_tokens: usage.cached_tokens,
        optional_nodes: work_nodes
            .iter()
            .filter(|node| node.work.as_ref().is_some_and(|work| !work.required))
            .count(),
        cancelled_optional_nodes: work_nodes
            .iter()
            .filter(|node| {
                node.work.as_ref().is_some_and(|work| !work.required)
                    && graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::Cancelled)
            })
            .count(),
    })
}

fn speedup_basis_points(serial_ms: u64, critical_path_ms: u64) -> Option<u32> {
    (critical_path_ms > 0).then(|| {
        serial_ms
            .saturating_mul(10_000)
            .saturating_div(critical_path_ms)
            .min(u64::from(u32::MAX)) as u32
    })
}

fn graph_shape(
    node_ids: &BTreeSet<&str>,
    dependencies: &[&super::ExecutionEdge],
) -> (usize, usize) {
    let mut indegree = node_ids
        .iter()
        .map(|id| (*id, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = BTreeMap::<&str, Vec<&str>>::new();
    for edge in dependencies {
        *indegree.entry(edge.to.as_str()).or_default() += 1;
        outgoing
            .entry(edge.from.as_str())
            .or_default()
            .push(edge.to.as_str());
    }
    let mut frontier = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect::<Vec<_>>();
    let mut width = 0;
    let mut depth = 0;
    while !frontier.is_empty() {
        width = width.max(frontier.len());
        depth += 1;
        let current = std::mem::take(&mut frontier);
        for id in current {
            for target in outgoing.get(id).into_iter().flatten() {
                if let Some(count) = indegree.get_mut(target) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        frontier.push(target);
                    }
                }
            }
        }
    }
    (width, depth)
}

fn critical_path(
    node_ids: &BTreeSet<&str>,
    required_ids: &BTreeSet<&str>,
    dependencies: &[&super::ExecutionEdge],
    weights: &BTreeMap<&str, u64>,
    policies: &BTreeMap<&str, super::ExecutionDependencyPolicy>,
    statuses: Option<&BTreeMap<String, ExecutionNodeStatus>>,
) -> u64 {
    let mut indegree = node_ids
        .iter()
        .map(|id| (*id, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = BTreeMap::<&str, Vec<&str>>::new();
    let mut predecessors = BTreeMap::<&str, Vec<&str>>::new();
    for edge in dependencies {
        *indegree.entry(edge.to.as_str()).or_default() += 1;
        outgoing
            .entry(edge.from.as_str())
            .or_default()
            .push(edge.to.as_str());
        predecessors
            .entry(edge.to.as_str())
            .or_default()
            .push(edge.from.as_str());
    }
    let mut frontier = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect::<Vec<_>>();
    let mut completion = BTreeMap::<&str, u64>::new();
    while let Some(id) = frontier.pop() {
        let mut predecessor_completion = predecessors
            .get(id)
            .into_iter()
            .flatten()
            .filter(|predecessor| {
                statuses.is_none_or(|statuses| {
                    statuses.get(**predecessor) == Some(&ExecutionNodeStatus::Completed)
                })
            })
            .filter_map(|predecessor| completion.get(predecessor).copied())
            .collect::<Vec<_>>();
        predecessor_completion.sort_unstable();
        let prerequisite = dependency_completion(
            policies.get(id).cloned().unwrap_or_default(),
            &predecessor_completion,
            predecessors.get(id).map_or(0, Vec::len),
        );
        let end = prerequisite
            .unwrap_or_default()
            .saturating_add(weights.get(id).copied().unwrap_or_default());
        completion.insert(id, end);
        for target in outgoing.get(id).into_iter().flatten() {
            if let Some(count) = indegree.get_mut(target) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    frontier.push(target);
                }
            }
        }
    }
    let required_completion = required_ids
        .iter()
        .filter_map(|id| completion.get(id).copied())
        .max();
    required_completion
        .or_else(|| completion.values().copied().max())
        .unwrap_or_default()
}

fn dependency_completion(
    policy: super::ExecutionDependencyPolicy,
    predecessor_completion: &[u64],
    predecessor_count: usize,
) -> Option<u64> {
    use super::ExecutionDependencyPolicy;

    if predecessor_count == 0 {
        return Some(0);
    }
    match policy {
        ExecutionDependencyPolicy::All | ExecutionDependencyPolicy::Finally => {
            (predecessor_completion.len() == predecessor_count)
                .then(|| predecessor_completion.last().copied().unwrap_or_default())
        }
        ExecutionDependencyPolicy::Any { .. } => predecessor_completion.first().copied(),
        ExecutionDependencyPolicy::Quorum { minimum, .. } => predecessor_completion
            .get(usize::from(minimum).saturating_sub(1))
            .copied(),
        ExecutionDependencyPolicy::EvidenceReady { predicate, .. } => match &predicate {
            super::DependencyPredicate::EvidenceReady { minimum, .. } => predecessor_completion
                .get(usize::from(*minimum).saturating_sub(1))
                .copied(),
        },
    }
}
