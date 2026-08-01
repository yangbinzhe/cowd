use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};

use harness_contract::execution_graph::ExecutionGraph;

use super::{HotResidencyRegistry, HotResidentClass, HotStateMetrics, RecoveryPermit};

pub struct HotExecutionGraphRegistry {
    shards: Vec<RwLock<HashMap<String, Arc<ExecutionGraph>>>>,
    recovery_flights:
        Mutex<HashMap<String, Arc<(Mutex<super::recovery::RecoveryState>, std::sync::Condvar)>>>,
    residency: Arc<HotResidencyRegistry>,
    metrics: Arc<HotStateMetrics>,
}

impl HotExecutionGraphRegistry {
    pub(super) fn new(
        shards: usize,
        residency: Arc<HotResidencyRegistry>,
        metrics: Arc<HotStateMetrics>,
    ) -> Self {
        Self {
            shards: (0..shards).map(|_| RwLock::new(HashMap::new())).collect(),
            recovery_flights: Mutex::new(HashMap::new()),
            residency,
            metrics,
        }
    }

    #[must_use]
    pub fn get(&self, graph_id: &str) -> Option<Arc<ExecutionGraph>> {
        let value = self.shards[self.shard(graph_id)]
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(graph_id)
            .cloned();
        if value.is_some() {
            self.metrics.graph_hit();
            self.residency.touch(&resident_id(graph_id));
        } else {
            self.metrics.graph_miss();
        }
        value
    }

    /// Publish only a monotonically newer committed snapshot.
    pub fn publish(&self, graph: ExecutionGraph) -> bool {
        self.publish_snapshot(Arc::new(graph))
    }

    /// Publish an already shared committed snapshot without cloning its
    /// immutable topology and state maps.
    pub fn publish_snapshot(&self, graph: Arc<ExecutionGraph>) -> bool {
        let graph_id = graph.id.clone();
        let revision = graph.revision;
        let estimated_bytes = estimate_graph_bytes(&graph);
        let mut shard = self.shards[self.shard(&graph_id)]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if shard
            .get(&graph_id)
            .is_some_and(|current| current.revision >= revision)
        {
            return false;
        }
        let terminal = graph_is_terminal(&graph);
        shard.insert(graph_id.clone(), graph);
        drop(shard);
        self.residency.upsert(
            resident_id(&graph_id),
            HotResidentClass::ExecutionGraph,
            graph_id.clone(),
            estimated_bytes,
            Some(revision),
        );
        self.metrics.graph_published();
        if terminal {
            self.residency
                .unpin(&resident_id(&graph_id), "active_execution");
        } else {
            self.residency
                .pin(&resident_id(&graph_id), "active_execution");
        }
        self.evict_completed_under_pressure();
        true
    }

    pub fn recovery_permit(&self, graph_id: &str) -> RecoveryPermit {
        RecoveryPermit::acquire(&self.recovery_flights, graph_id)
    }

    pub fn record_recovery(&self) {
        self.metrics.graph_recovered();
    }

    fn evict_completed_under_pressure(&self) {
        if !self.residency.pressure_high() {
            return;
        }
        for candidate in self
            .residency
            .eviction_candidates(HotResidentClass::ExecutionGraph)
        {
            if self.residency.resident_bytes() <= self.residency.target_low_watermark() {
                break;
            }
            let graph_id = candidate.owner_id;
            let shard_index = self.shard(&graph_id);
            let mut shard = self.shards[shard_index]
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let terminal = shard
                .get(&graph_id)
                .is_some_and(|graph| graph_is_terminal(graph));
            if terminal {
                shard.remove(&graph_id);
                drop(shard);
                self.residency.remove(&resident_id(&graph_id));
            }
        }
    }

    fn shard(&self, key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & (self.shards.len() - 1)
    }
}

fn resident_id(graph_id: &str) -> String {
    format!("graph:{graph_id}")
}

fn graph_is_terminal(graph: &ExecutionGraph) -> bool {
    !graph.nodes.is_empty()
        && graph.nodes.iter().all(|node| {
            graph
                .node_statuses
                .get(&node.id)
                .is_some_and(|status| status.is_terminal())
        })
}

pub(crate) fn estimate_graph_bytes(graph: &ExecutionGraph) -> u64 {
    let mut bytes = std::mem::size_of::<ExecutionGraph>()
        .saturating_add(graph.id.len())
        .saturating_add(graph.objective.len());
    if let Some(parent) = &graph.parent_execution {
        bytes = bytes
            .saturating_add(parent.execution_id.len())
            .saturating_add(parent.node_id.len());
    }
    if let Some(orchestration) = &graph.orchestration {
        bytes = bytes
            .saturating_add(orchestration.mutation_id.len())
            .saturating_add(
                orchestration
                    .applied_mutation_ids
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            )
            .saturating_add(
                orchestration
                    .completion
                    .required_node_ids
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            )
            .saturating_add(
                orchestration
                    .completion
                    .required_artifact_kinds
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            );
    }
    for node in &graph.nodes {
        bytes = bytes
            .saturating_add(std::mem::size_of_val(node))
            .saturating_add(node.id.len())
            .saturating_add(node.payload_ref.len())
            .saturating_add(node.executor_kind.len())
            .saturating_add(node.idempotency_key.len())
            .saturating_add(node.resource_scopes.iter().map(String::len).sum::<usize>())
            .saturating_add(
                node.acceptance
                    .criteria
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            )
            .saturating_add(
                node.acceptance
                    .required_evidence
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            )
            .saturating_add(
                node.retry_policy
                    .retryable_failure_kinds
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            );
        if let Some(work) = &node.work {
            bytes = bytes
                .saturating_add(
                    work.required_evidence_refs
                        .iter()
                        .map(String::len)
                        .sum::<usize>(),
                )
                .saturating_add(work.cancellation_group.as_ref().map_or(0, String::len))
                .saturating_add(work.context_view_ref.as_ref().map_or(0, String::len))
                .saturating_add(work.model_profile.as_ref().map_or(0, String::len))
                .saturating_add(work.reasoning_effort.as_ref().map_or(0, String::len));
        }
    }
    for edge in &graph.edges {
        bytes = bytes
            .saturating_add(std::mem::size_of_val(edge))
            .saturating_add(edge.from.len())
            .saturating_add(edge.to.len());
    }
    bytes = bytes.saturating_add(graph.node_statuses.keys().map(String::len).sum::<usize>());
    for (node_id, result) in &graph.node_results {
        bytes = bytes
            .saturating_add(node_id.len())
            .saturating_add(std::mem::size_of_val(result))
            .saturating_add(result.result_ref.as_ref().map_or(0, String::len))
            .saturating_add(result.summary.as_ref().map_or(0, String::len))
            .saturating_add(
                result
                    .usage
                    .runtime_write_attempt_paths
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            )
            .saturating_add(
                result
                    .usage
                    .runtime_observed_resource_scopes
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            );
        if let Some(failure) = &result.failure {
            bytes = bytes
                .saturating_add(failure.kind.len())
                .saturating_add(failure.message.len());
        }
    }
    bytes = bytes.saturating_add(
        graph
            .recovery_cursor
            .node_attempts
            .keys()
            .map(String::len)
            .sum::<usize>(),
    );
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock as StdRwLock;

    use harness_contract::execution_graph::{
        ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
    };

    use super::*;
    use crate::execution_core::hot_state::{
        HotMemoryBudget, HotStateMemoryConfig, HotStateMetrics,
    };

    fn registry() -> HotExecutionGraphRegistry {
        let metrics = Arc::new(HotStateMetrics::default());
        let budget = Arc::new(StdRwLock::new(HotMemoryBudget::resolve(
            &HotStateMemoryConfig::default(),
        )));
        let residency = Arc::new(HotResidencyRegistry::new(budget, metrics.clone()));
        HotExecutionGraphRegistry::new(4, residency, metrics)
    }

    #[test]
    fn active_graph_is_pinned_and_terminal_graph_is_reconstructable() {
        let registry = registry();
        let mut graph = ExecutionGraph::new("test graph");
        graph.revision = 1;
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "model", "{}");
        graph.nodes.push(node.clone());
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        assert!(registry.publish(graph.clone()));
        let resident = registry
            .residency
            .snapshot(&resident_id(&graph.id))
            .expect("resident graph");
        assert_eq!(resident.pin_reasons, vec!["active_execution"]);

        graph.revision = 2;
        graph
            .node_statuses
            .insert(node.id, ExecutionNodeStatus::Completed);
        assert!(registry.publish(graph.clone()));
        let resident = registry
            .residency
            .snapshot(&resident_id(&graph.id))
            .expect("resident graph");
        assert!(resident.pin_reasons.is_empty());
        assert_eq!(resident.reconstruct_cursor, Some(2));
    }
}
