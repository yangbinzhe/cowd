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
            .saturating_add(observed_acceptance_heap_bytes(
                &result.usage.observed_acceptance,
            ));
        if result.usage.observed_acceptance.is_empty() {
            bytes = bytes.saturating_add(
                result
                    .usage
                    .runtime_observed_resource_scopes
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            );
        }
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

fn observed_acceptance_heap_bytes(
    acceptance: &harness_contract::context::ObservedAcceptance,
) -> usize {
    let mut bytes = acceptance
        .satisfied_criteria
        .iter()
        .chain(&acceptance.unresolved_obligation_ids)
        .map(String::len)
        .sum::<usize>();
    bytes = bytes.saturating_add(acceptance.observed_evidence.capacity().saturating_mul(
        std::mem::size_of::<harness_contract::context::ObservedEvidence>(),
    ));
    for evidence in &acceptance.observed_evidence {
        bytes = bytes
            .saturating_add(evidence.obligation_id.len())
            .saturating_add(evidence.tool_name.len());
        bytes = bytes.saturating_add(match &evidence.target {
            harness_contract::context::EvidenceTargetIdentity::ToolExecution { execution } => {
                tool_execution_heap_bytes(execution)
            }
            harness_contract::context::EvidenceTargetIdentity::Workspace { scope } => scope
                .path
                .workspace_id
                .len()
                .saturating_add(scope.path.repository_id.len())
                .saturating_add(scope.path.workspace_relative_path.len())
                .saturating_add(scope.path.repository_relative_path.len())
                .saturating_add(
                    scope
                        .path
                        .observed_revision_or_digest
                        .as_ref()
                        .map_or(0, String::len),
                ),
            harness_contract::context::EvidenceTargetIdentity::LocalNetwork { operation } => {
                operation.binding.call_key.len()
                    + operation.binding.tool_use_id.len() + 192
                    + operation.operations.iter().map(|operation| {
                        operation.endpoint.len() + operation.input_sha256.len()
                            + match &operation.terminal {
                                harness_contract::context::LocalNetworkTerminal::BodyDone { body_sha256, .. } => body_sha256.len(),
                                harness_contract::context::LocalNetworkTerminal::NotStarted { reason_code }
                                | harness_contract::context::LocalNetworkTerminal::Unknown { reason_code } => reason_code.len(),
                            }
                            + std::mem::size_of::<harness_contract::context::LocalNetworkOperation>()
                    }).sum::<usize>()
            }
            harness_contract::context::EvidenceTargetIdentity::Network { endpoint } => {
                endpoint.len()
            }
            harness_contract::context::EvidenceTargetIdentity::AmbiguousWorkspace {
                display_alias,
                candidates,
            } => display_alias
                .len()
                .saturating_add(candidates.iter().map(String::len).sum::<usize>()),
            harness_contract::context::EvidenceTargetIdentity::UnavailableWorkspace {
                display_alias,
                reason,
            } => display_alias.len().saturating_add(reason.len()),
        });
        if let Some(reference) = &evidence.evidence_ref {
            bytes = bytes
                .saturating_add(reference.evidence_ref.ref_type.len())
                .saturating_add(reference.evidence_ref.id.len())
                .saturating_add(reference.sha256.len())
                .saturating_add(reference.media_type.len())
                .saturating_add(reference.retrieval_selector.len())
                .saturating_add(reference.visibility_scope.len());
        }
    }
    bytes
}

fn tool_execution_heap_bytes(execution: &harness_contract::context::ToolExecutionFact) -> usize {
    use harness_contract::context::ToolExecutionTerminal;
    let admission = &execution.admission;
    let binding = &admission.binding;
    let mut bytes = binding
        .call_key
        .capacity()
        .saturating_add(binding.tool_use_id.capacity())
        .saturating_add(binding.input_sha256.capacity())
        .saturating_add(binding.descriptor_sha256.capacity())
        .saturating_add(binding.authority_sha256.capacity())
        .saturating_add(admission.code_sha256.capacity())
        .saturating_add(execution.stdout_sha256.capacity())
        .saturating_add(execution.stderr_sha256.capacity())
        .saturating_add(
            admission
                .inputs
                .capacity()
                .saturating_mul(std::mem::size_of::<
                    harness_contract::context::ToolExecutionInput,
                >()),
        );
    for input in &admission.inputs {
        bytes = bytes
            .saturating_add(input.relative_path.capacity())
            .saturating_add(input.sha256.capacity());
    }
    for artifact in [&admission.source_bundle, &execution.body] {
        bytes = bytes
            .saturating_add(artifact.selector.capacity())
            .saturating_add(artifact.sha256.capacity())
            .saturating_add(artifact.media_type.capacity())
            .saturating_add(artifact.visibility_scope.capacity());
    }
    bytes.saturating_add(match &execution.terminal {
        ToolExecutionTerminal::NotStarted { reason_code }
        | ToolExecutionTerminal::Unknown { reason_code } => reason_code.capacity(),
        ToolExecutionTerminal::Settled { closure, .. } => closure.backend.capacity(),
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn bounded_process_heap_accounts_inputs_artifact_refs_and_terminal_without_body_283() {
        let observation = crate::path_identity::untrusted_process_observation_283();
        let harness_contract::context::EvidenceTargetIdentity::ToolExecution { mut execution } =
            observation.target
        else {
            panic!("negative DTO target")
        };
        let before = super::tool_execution_heap_bytes(&execution);
        execution.body.bytes = u64::MAX;
        execution.stdout_bytes = u64::MAX;
        assert_eq!(super::tool_execution_heap_bytes(&execution), before);
        execution.admission.inputs[0].relative_path.reserve(4096);
        execution.body.selector.reserve(4096);
        execution.admission.source_bundle.selector.reserve(4096);
        assert!(super::tool_execution_heap_bytes(&execution) >= before + 3 * 4096);
        let mut second = execution.admission.inputs[0].clone();
        second.relative_path = "another.py".into();
        execution.admission.inputs.push(second);
        assert!(super::tool_execution_heap_bytes(&execution) > before + 19 + 64);
        execution.terminal = harness_contract::context::ToolExecutionTerminal::Unknown {
            reason_code: "x".repeat(128),
        };
        assert!(super::tool_execution_heap_bytes(&execution) >= 128);
        execution.terminal = harness_contract::context::ToolExecutionTerminal::NotStarted {
            reason_code: String::new(),
        };
        let empty_reason = super::tool_execution_heap_bytes(&execution);
        execution.terminal = harness_contract::context::ToolExecutionTerminal::NotStarted {
            reason_code: "reason".repeat(512),
        };
        assert_eq!(
            super::tool_execution_heap_bytes(&execution),
            empty_reason + 6 * 512
        );
        let mut backend = String::new();
        backend.reserve(4096);
        let capacity = backend.capacity();
        execution.terminal = harness_contract::context::ToolExecutionTerminal::Settled {
            exit_code: None,
            signal: None,
            stop_reason: harness_contract::context::ToolExecutionStopReason::Cancelled,
            closure: harness_contract::context::ToolProcessClosure {
                backend,
                namespace_inode: 0,
                init_host_pid: 0,
                init_start_ticks: 0,
                all_children_reaped: false,
                init_reaped: false,
                kernel_hardening_applied: false,
                network_disabled: false,
                project_read_only: false,
                status_channel_protected: false,
            },
        };
        assert_eq!(
            super::tool_execution_heap_bytes(&execution),
            empty_reason + capacity
        );
    }
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
