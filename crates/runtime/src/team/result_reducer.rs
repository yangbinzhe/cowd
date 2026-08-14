use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::agent::{AgentTaskPacket, AgentTerminalStatus};
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
};
use harness_contract::outcome::{
    AnswerContentKind, AnswerObjectiveScope, AnswerOrigin, AnswerValidation,
    AnswerValidationStatus, DeliveryBranchStatus, DeliveryBranchTerminal, DeliveryCoverage,
    DeliveryEnvelope, DeliveryStatus, DeliveryUnresolved, PipelineStatus, PresentationModelAttempt,
    TerminalPresentation, TerminalPresentationState, UserAnswerContract, VerifiedDeliveryEffect,
    VerifiedDeliveryReference, VerifiedEffectStatus,
};

use crate::execution_core::graph::executors::{SynthesizeBackend, SynthesizeBackendResolver};
use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionOutcome, NodeExecutionTicket,
};
use crate::execution_core::{ImmutableWorkKey, InFlightCoalescer};
use crate::AgentRuntime;

/// Reduces durable graph terminal facts into one DeliveryEnvelope. A
/// process-local AgentRuntime packet is consulted only for an optional wording
/// candidate and never replaces committed branch/evidence/effect truth.
pub struct TeamResultReducer {
    state_store: ExecutionGraphStateStore,
    agents: Arc<AgentRuntime>,
    coalescer: Arc<InFlightCoalescer<ImmutableWorkKey, NodeExecutionOutcome, String>>,
}

impl TeamResultReducer {
    #[must_use]
    pub fn new(state_store: ExecutionGraphStateStore, agents: Arc<AgentRuntime>) -> Self {
        Self {
            state_store,
            agents,
            coalescer: Arc::new(InFlightCoalescer::default()),
        }
    }

    async fn synthesize_uncached(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let graph = self
            .state_store
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| error.to_string())?;
        let mut evidence = Vec::new();
        let mut usage = ExecutionUsage::default();
        let mut returned_by_node = BTreeMap::new();
        let terminal_agent_nodes = terminal_agent_node_ids(&graph);

        for node in graph.nodes.iter().filter(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
        }) {
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|_| format!("team node {} is not an AgentTask packet", node.id))?;
            if let Some(result) = graph.node_results.get(&node.id) {
                merge_usage(&mut usage, &result.usage);
                evidence.extend(result.evidence_refs.clone());
            }
            if let Some(returned) = self.agents.terminal_return(packet.agent_id()) {
                if returned.run_id == packet.run_id()
                    && returned.graph_id == graph.id
                    && returned.node_id == node.id
                    && returned.attempt == packet.attempt
                    && returned.expected_graph_revision == packet.expected_graph_revision
                {
                    returned_by_node.insert(node.id.clone(), returned);
                }
            }
        }

        evidence.sort_by(|left, right| {
            left.evidence_ref
                .ref_type
                .cmp(&right.evidence_ref.ref_type)
                .then_with(|| left.evidence_ref.id.cmp(&right.evidence_ref.id))
        });
        evidence.dedup_by(|left, right| left.evidence_ref == right.evidence_ref);
        usage.runtime_write_attempt_paths.sort();
        usage.runtime_write_attempt_paths.dedup();
        usage.runtime_observed_resource_scopes.sort();
        usage.runtime_observed_resource_scopes.dedup();

        let envelope = build_delivery_envelope(&graph);
        let reusable = terminal_agent_nodes.iter().find_map(|node_id| {
            let node = graph.nodes.iter().find(|node| node.id == *node_id)?;
            let packet = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok()?;
            let returned = returned_by_node.get(node_id)?;
            eligible_team_synthesizer(returned, &packet, &envelope)
        });
        let (result_ref, summary, terminal_presentation) = reusable.map_or_else(
            || {
                (
                    Some(format!("delivery-envelope: {}", envelope.envelope_id)),
                    Some(mechanical_delivery_summary(&envelope)),
                    None,
                )
            },
            |(answer, presentation)| {
                (
                    serde_json::to_string(&answer.text)
                        .ok()
                        .map(|text| format!("assistant_json:{text}")),
                    Some(answer.text),
                    Some(presentation),
                )
            },
        );
        let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
            // The reducer itself completed even when business delivery is
            // partial or unavailable. DeliveryEnvelope owns that distinction.
            status: ExecutionNodeStatus::Completed,
            result_ref,
            summary,
            evidence_refs: evidence,
            failure: None,
            usage,
            finished_at_ms: crate::tool_invocation::now_ms(),
        });
        outcome.delivery_envelope = Some(envelope);
        outcome.terminal_presentation = terminal_presentation;
        Ok(outcome)
    }
}

fn merge_usage(aggregate: &mut ExecutionUsage, observed: &ExecutionUsage) {
    aggregate.input_tokens = aggregate.input_tokens.saturating_add(observed.input_tokens);
    aggregate.output_tokens = aggregate
        .output_tokens
        .saturating_add(observed.output_tokens);
    aggregate.cached_tokens = aggregate
        .cached_tokens
        .saturating_add(observed.cached_tokens);
    aggregate.tool_calls = aggregate.tool_calls.saturating_add(observed.tool_calls);
    aggregate.duplicate_tool_calls = aggregate
        .duplicate_tool_calls
        .saturating_add(observed.duplicate_tool_calls);
    aggregate.max_tool_concurrency_observed = aggregate
        .max_tool_concurrency_observed
        .max(observed.max_tool_concurrency_observed);
    aggregate.parallel_tool_batches = aggregate
        .parallel_tool_batches
        .saturating_add(observed.parallel_tool_batches);
    aggregate
        .runtime_write_attempt_paths
        .extend(observed.runtime_write_attempt_paths.iter().cloned());
    aggregate
        .observed_acceptance
        .merge_from(&observed.observed_acceptance);
}

fn build_delivery_envelope(graph: &ExecutionGraph) -> DeliveryEnvelope {
    let agent_nodes = graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .collect::<Vec<_>>();
    let mut branch_terminals = Vec::with_capacity(agent_nodes.len());
    let mut verified_receipts = Vec::new();
    let mut verified_artifacts = Vec::new();
    let mut verified_effects = Vec::new();
    let mut required_obligation_ids = Vec::new();
    let mut satisfied_obligation_ids = Vec::new();
    let mut unresolved = Vec::new();
    let mut applied_writes = BTreeSet::new();

    for node in &agent_nodes {
        let status = graph
            .node_statuses
            .get(&node.id)
            .copied()
            .unwrap_or(ExecutionNodeStatus::Blocked);
        let result = graph.node_results.get(&node.id);
        let branch_status = match status {
            ExecutionNodeStatus::Completed => DeliveryBranchStatus::Completed,
            ExecutionNodeStatus::Failed => DeliveryBranchStatus::Failed,
            ExecutionNodeStatus::Cancelled => DeliveryBranchStatus::Cancelled,
            _ => DeliveryBranchStatus::Blocked,
        };
        let result_ref = result.and_then(|result| result.result_ref.clone());
        let execution_id = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
            .ok()
            .map(|packet| packet.run_id().to_string());
        let failure_ref = result.and_then(|result| {
            result
                .failure
                .as_ref()
                .map(|_| format!("execution-graph:{}:node:{}:failure", graph.id, node.id))
        });
        branch_terminals.push(DeliveryBranchTerminal {
            branch_id: node.id.clone(),
            execution_id,
            status: branch_status,
            result_ref: result_ref.clone(),
            failure_ref,
        });

        if branch_status != DeliveryBranchStatus::Completed
            || result_ref
                .as_deref()
                .is_some_and(|reference| reference.ends_with(":unresolved"))
        {
            unresolved.push(DeliveryUnresolved {
                unresolved_id: format!("branch:{}", node.id),
                kind: format!("branch_{branch_status:?}").to_ascii_lowercase(),
                summary: format!(
                    "Team branch `{}` reached terminal state {:?} without satisfying the full delivery contract.",
                    node.id, branch_status
                ),
                source_execution_id: Some(node.id.clone()),
                obligation_id: None,
            });
        }

        if let Some(result) = result {
            for (index, reference) in result.evidence_refs.iter().enumerate() {
                if reference.is_durable() && !reference.retrieval_selector.trim().is_empty() {
                    verified_artifacts.push(VerifiedDeliveryReference {
                        reference_id: reference.retrieval_selector.clone(),
                        kind: reference.evidence_ref.ref_type.clone(),
                        source_execution_id: Some(node.id.clone()),
                    });
                }
                if reference.evidence_ref.ref_type == "runtime_change" {
                    let Ok(receipt) = serde_json::from_str::<
                        harness_contract::agent::AgentChangeReceipt,
                    >(&reference.evidence_ref.id) else {
                        continue;
                    };
                    verified_receipts.push(VerifiedDeliveryReference {
                        reference_id: format!("execution-node:{}:receipt:{index}", node.id),
                        kind: "runtime_change".to_string(),
                        source_execution_id: Some(node.id.clone()),
                    });
                    applied_writes.insert((node.id.clone(), receipt.path.clone()));
                    verified_effects.push(VerifiedDeliveryEffect {
                        effect_id: stable_effect_id(&node.id, &receipt.path),
                        kind: "workspace_write".to_string(),
                        status: VerifiedEffectStatus::Applied,
                        receipt_ref: Some(format!(
                            "execution-node:{}:write-sequence:{}",
                            node.id, receipt.write_sequence
                        )),
                        source_execution_id: Some(node.id.clone()),
                    });
                }
            }
        }

        let required_acceptance = if !node.acceptance.required.is_empty() {
            &node.acceptance.required
        } else if let Some(result) = result {
            &result.usage.required_acceptance
        } else {
            &node.acceptance.required
        };
        let observed_acceptance = result.map(|result| &result.usage.observed_acceptance);
        for criterion in &required_acceptance.criteria {
            let id = format!("criterion:{}:{criterion}", node.id);
            required_obligation_ids.push(id.clone());
            if observed_acceptance
                .is_some_and(|observed| observed.satisfied_criteria.contains(criterion))
            {
                satisfied_obligation_ids.push(id);
            }
        }
        for obligation in &required_acceptance.evidence_obligations {
            let id = format!("evidence:{}:{}", node.id, obligation.obligation_id);
            required_obligation_ids.push(id.clone());
            let satisfied = observed_acceptance.is_some_and(|acceptance| {
                acceptance
                    .observed_evidence
                    .iter()
                    .any(|observed| observed.obligation_id == obligation.obligation_id)
            });
            if satisfied {
                satisfied_obligation_ids.push(id.clone());
            } else {
                unresolved.push(DeliveryUnresolved {
                    unresolved_id: id.clone(),
                    kind: "acceptance_obligation".to_string(),
                    summary: format!(
                        "Runtime did not observe required acceptance obligation `{}`.",
                        obligation.obligation_id
                    ),
                    source_execution_id: Some(node.id.clone()),
                    obligation_id: Some(obligation.obligation_id.clone()),
                });
            }
            if obligation.kind == harness_contract::context::EvidenceObligationKind::WriteEffect
                && !satisfied
                && applied_writes.iter().all(|(source, _)| source != &node.id)
            {
                verified_effects.push(VerifiedDeliveryEffect {
                    effect_id: format!("required-write:{}:{}", node.id, obligation.obligation_id),
                    kind: "workspace_write".to_string(),
                    status: VerifiedEffectStatus::NotApplied,
                    receipt_ref: None,
                    source_execution_id: Some(node.id.clone()),
                });
            }
        }
        if let Some(result) = result {
            for path in &result.usage.runtime_write_attempt_paths {
                if !applied_writes.contains(&(node.id.clone(), path.clone())) {
                    verified_effects.push(VerifiedDeliveryEffect {
                        effect_id: stable_effect_id(&node.id, path),
                        kind: "workspace_write".to_string(),
                        status: VerifiedEffectStatus::NotApplied,
                        receipt_ref: None,
                        source_execution_id: Some(node.id.clone()),
                    });
                }
            }
        }
    }

    branch_terminals.sort_by(|left, right| left.branch_id.cmp(&right.branch_id));
    verified_receipts.sort_by(|left, right| left.reference_id.cmp(&right.reference_id));
    verified_receipts.dedup_by(|left, right| left.reference_id == right.reference_id);
    verified_artifacts.sort_by(|left, right| left.reference_id.cmp(&right.reference_id));
    verified_artifacts.dedup_by(|left, right| left.reference_id == right.reference_id);
    verified_effects.sort_by(|left, right| left.effect_id.cmp(&right.effect_id));
    verified_effects
        .dedup_by(|left, right| left.effect_id == right.effect_id && left.status == right.status);
    required_obligation_ids.sort();
    required_obligation_ids.dedup();
    satisfied_obligation_ids.sort();
    satisfied_obligation_ids.dedup();
    unresolved.sort_by(|left, right| left.unresolved_id.cmp(&right.unresolved_id));
    unresolved.dedup_by(|left, right| left.unresolved_id == right.unresolved_id);

    let coverage_basis_points = if required_obligation_ids.is_empty() {
        10_000
    } else {
        u16::try_from(
            satisfied_obligation_ids.len().saturating_mul(10_000) / required_obligation_ids.len(),
        )
        .unwrap_or(10_000)
    };
    let completed = branch_terminals
        .iter()
        .filter(|branch| branch.status == DeliveryBranchStatus::Completed)
        .count();
    let all_terminal = agent_nodes.iter().all(|node| {
        graph
            .node_statuses
            .get(&node.id)
            .is_some_and(|status| status.is_terminal())
    });
    let verification_satisfied = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::Verify)
        .is_some_and(|node| {
            graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::Completed)
                && graph
                    .node_results
                    .get(&node.id)
                    .and_then(|result| result.result_ref.as_deref())
                    .is_some_and(|reference| reference.ends_with(":satisfied"))
        });
    let has_not_applied = verified_effects
        .iter()
        .any(|effect| effect.status == VerifiedEffectStatus::NotApplied);
    let delivery_status = if !branch_terminals.is_empty()
        && completed == branch_terminals.len()
        && verification_satisfied
        && coverage_basis_points == 10_000
        && unresolved.is_empty()
        && !has_not_applied
    {
        DeliveryStatus::Satisfied
    } else if completed > 0 {
        DeliveryStatus::Partial
    } else {
        // NotApplied proves that an effect did not occur, not why. Until an
        // authoritative approval/policy denial receipt is present, 0/N stays
        // unavailable rather than guessing a Denied business classification.
        DeliveryStatus::Unavailable
    };
    let revision = graph.revision.saturating_add(1);
    DeliveryEnvelope {
        envelope_id: format!("delivery:{}:{revision}", graph.id),
        revision,
        objective_id: graph.id.clone(),
        pipeline_status: if all_terminal {
            PipelineStatus::Completed
        } else {
            PipelineStatus::Waiting
        },
        delivery_status,
        branch_terminals,
        verified_receipts,
        verified_artifacts,
        verified_effects,
        coverage: DeliveryCoverage {
            required_obligation_ids,
            satisfied_obligation_ids,
            coverage_basis_points,
        },
        unresolved,
        conflicts: Vec::new(),
        cancellation: None,
        user_answer_contract: UserAnswerContract::default(),
        created_at_ms: crate::tool_invocation::now_ms(),
    }
}

fn stable_effect_id(node_id: &str, path: &str) -> String {
    let digest = model_protocol::fingerprint::stable_hash_bytes(path.as_bytes());
    format!("effect:{node_id}:{digest:016x}")
}

fn eligible_team_synthesizer(
    returned: &harness_contract::agent::AgentReturnPacket,
    packet: &AgentTaskPacket,
    envelope: &DeliveryEnvelope,
) -> Option<(
    harness_contract::outcome::AnswerCandidate,
    TerminalPresentation,
)> {
    let candidate = returned.answer_candidate.as_ref()?;
    let role = packet
        .constraints
        .iter()
        .find_map(|constraint| constraint.strip_prefix("team_role:"))
        .map(str::trim);
    if returned.status != AgentTerminalStatus::Completed
        || !matches!(
            role,
            Some("synthesizer" | "decision_synthesis" | "finalizer")
        )
        || candidate.source_execution_id != returned.run_id
        || candidate.consumed_envelope_revision != Some(envelope.revision)
        || candidate.validation.status != AnswerValidationStatus::Valid
        || candidate.validation.envelope_revision != Some(envelope.revision)
        || !(candidate.objective_scope == AnswerObjectiveScope::Root || candidate.terminal_delegate)
        || !matches!(
            candidate.content_kind,
            AnswerContentKind::UserText | AnswerContentKind::StrictJson
        )
        || candidate.text.trim().is_empty()
        || matches!(
            candidate.origin,
            AnswerOrigin::ProgrammaticFallback | AnswerOrigin::CancellationReceipt
        )
    {
        return None;
    }
    let mut answer = candidate.clone();
    answer.origin = AnswerOrigin::TeamSynthesizer;
    answer.consumed_envelope_revision = Some(envelope.revision);
    answer.validation = AnswerValidation {
        status: AnswerValidationStatus::Pending,
        findings: Vec::new(),
        envelope_revision: Some(envelope.revision),
    };
    let models_attempted = answer
        .provider
        .as_ref()
        .zip(answer.model.as_ref())
        .map(|(provider, model)| {
            vec![PresentationModelAttempt {
                provider: provider.clone(),
                model: model.clone(),
                failure: None,
            }]
        })
        .unwrap_or_default();
    let presentation = TerminalPresentation {
        presentation_id: format!("team-presentation:{}", answer.candidate_id),
        attempt_id: answer.candidate_id.clone(),
        envelope_id: envelope.envelope_id.clone(),
        envelope_revision: envelope.revision,
        state: TerminalPresentationState::Validating,
        answer_origin: AnswerOrigin::TeamSynthesizer,
        source_execution_id: Some(returned.run_id.clone()),
        narrator_model: answer.model.clone(),
        narrator_provider: answer.provider.clone(),
        models_attempted,
        validation: answer.validation.clone(),
        fallback_reason: None,
        generated_at_ms: answer.completed_at_ms,
        committed_at_ms: None,
    };
    Some((answer, presentation))
}

fn mechanical_delivery_summary(envelope: &DeliveryEnvelope) -> String {
    let completed = envelope
        .branch_terminals
        .iter()
        .filter(|branch| branch.status == DeliveryBranchStatus::Completed)
        .count();
    let applied = envelope
        .verified_effects
        .iter()
        .filter(|effect| effect.status == VerifiedEffectStatus::Applied)
        .count();
    let not_applied = envelope
        .verified_effects
        .iter()
        .filter(|effect| effect.status == VerifiedEffectStatus::NotApplied)
        .count();
    format!(
        "Runtime delivery facts: {completed}/{} Team branches completed; delivery={:?}; coverage={}bp; unresolved={}; effects_applied={applied}; effects_not_applied={not_applied}.",
        envelope.branch_terminals.len(),
        envelope.delivery_status,
        envelope.coverage.coverage_basis_points,
        envelope.unresolved.len(),
    )
}

impl SynthesizeBackendResolver for TeamResultReducer {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
        ticket.payload_ref.starts_with("team:").then(|| {
            Arc::new(Self {
                state_store: self.state_store.clone(),
                agents: Arc::clone(&self.agents),
                coalescer: Arc::clone(&self.coalescer),
            }) as Arc<dyn SynthesizeBackend>
        })
    }
}

#[async_trait]
impl SynthesizeBackend for TeamResultReducer {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let graph_revision = self
            .state_store
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| error.to_string())?
            .revision;
        let key = ImmutableWorkKey {
            authority_scope: "runtime:team-reducer".to_string(),
            session_scope: ticket.graph_id.clone(),
            source_revision: format!("graph:{graph_revision}:attempt:{}", ticket.attempt),
            model_profile: ticket.executor_kind.clone(),
            prompt_contract: ticket.idempotency_key.clone(),
            evidence_digest: ticket.payload_ref.clone(),
        };
        self.coalescer
            .run(key, || self.synthesize_uncached(ticket))
            .await
            .map(|result| result.value)
    }
}

fn terminal_agent_node_ids(
    graph: &harness_contract::execution_graph::ExecutionGraph,
) -> BTreeSet<String> {
    let agent_nodes = graph
        .nodes
        .iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    agent_nodes
        .iter()
        .filter(|node_id| {
            !graph.edges.iter().any(|edge| {
                edge.from == **node_id
                    && edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::DependsOn
                    && agent_nodes.contains(edge.to.as_str())
            })
        })
        .map(|node_id| (*node_id).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};
    use harness_contract::context::{
        ChildExecutionBudgetReservation, EvidenceObligation, EvidenceObligationKind,
        EvidenceTargetIdentity, RequiredAcceptance,
    };
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionFailure, ExecutionGraph, ExecutionNodeKind,
        ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
    };
    use harness_contract::outcome::{
        AnswerCandidate, AnswerContentKind, AnswerObjectiveScope, AnswerOrigin, AnswerValidation,
        AnswerValidationStatus, DeliveryStatus, VerifiedEffectStatus,
    };

    use super::{
        build_delivery_envelope, eligible_team_synthesizer, mechanical_delivery_summary,
        terminal_agent_node_ids,
    };

    fn result(status: ExecutionNodeStatus, usage: ExecutionUsage) -> ExecutionNodeResult {
        ExecutionNodeResult {
            status,
            result_ref: Some(format!("agent-result:{status:?}")),
            summary: None,
            evidence_refs: Vec::new(),
            failure: (status != ExecutionNodeStatus::Completed).then(|| ExecutionFailure {
                kind: "fixture".to_string(),
                message: "fixture terminal failure".to_string(),
                retryable: false,
                evidence_refs: Vec::new(),
            }),
            usage,
            finished_at_ms: 1,
        }
    }

    fn add_agent(graph: &mut ExecutionGraph, id: &str, status: ExecutionNodeStatus) {
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        node.id = id.to_string();
        graph.node_statuses.insert(node.id.clone(), status);
        if status == ExecutionNodeStatus::Completed {
            graph
                .node_results
                .insert(node.id.clone(), result(status, ExecutionUsage::default()));
        }
        graph.nodes.push(node);
    }

    fn add_verify(graph: &mut ExecutionGraph, satisfied: bool) {
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::Verify, "verify", "team:fixture");
        node.id = "verify".to_string();
        let status = if satisfied {
            ExecutionNodeStatus::Completed
        } else {
            ExecutionNodeStatus::Blocked
        };
        graph.node_statuses.insert(node.id.clone(), status);
        let mut verdict = result(status, ExecutionUsage::default());
        verdict.result_ref = Some(format!(
            "verification:fixture:{}",
            if satisfied {
                "satisfied"
            } else {
                "not_satisfied"
            }
        ));
        graph.node_results.insert(node.id.clone(), verdict);
        graph.nodes.push(node);
    }

    fn synthesizer_packet() -> AgentTaskPacket {
        AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                "agent-1",
                "run-1",
                "task-1",
                "session-1",
                "mission-1",
                Some("team-1"),
                "graph-1",
                "node-1",
            ),
            attempt: 1,
            expected_graph_revision: 2,
            policy_revision: 1,
            objective: "synthesize".to_string(),
            required_acceptance: Default::default(),
            acceptance: Vec::new(),
            constraints: vec!["team_role:synthesizer".to_string()],
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "model".to_string(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget",
                "agent-1",
                "agent",
                1_000,
                75_000,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "agent:1".to_string(),
        }
    }

    fn returned_candidate(revision: u64) -> AgentReturnPacket {
        AgentReturnPacket {
            run_id: "run-1".to_string(),
            agent_id: "agent-1".to_string(),
            task_id: "task-1".to_string(),
            session_id: "session-1".to_string(),
            mission_id: "mission-1".to_string(),
            team_id: Some("team-1".to_string()),
            graph_id: "graph-1".to_string(),
            node_id: "node-1".to_string(),
            attempt: 1,
            expected_graph_revision: 2,
            status: AgentTerminalStatus::Completed,
            outcome: "answer".to_string(),
            answer_candidate: Some(AnswerCandidate {
                candidate_id: "candidate-1".to_string(),
                origin: AnswerOrigin::ModelDirect,
                objective_scope: AnswerObjectiveScope::Root,
                source_execution_id: "run-1".to_string(),
                consumed_envelope_revision: Some(revision),
                model: Some("model".to_string()),
                provider: Some("provider".to_string()),
                completed_at_ms: 1,
                text: "answer".to_string(),
                content_kind: AnswerContentKind::UserText,
                terminal_delegate: false,
                validation: AnswerValidation {
                    status: AnswerValidationStatus::Valid,
                    findings: Vec::new(),
                    envelope_revision: Some(revision),
                },
            }),
            observed_acceptance: Default::default(),
            acceptance: Vec::new(),
            evidence_refs: Vec::new(),
            changes: Vec::new(),
            runtime_change_receipts: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 1,
            output_tokens: 1,
            cached_tokens: 0,
            model: "model".to_string(),
            provider: "provider".to_string(),
            tool_calls: 0,
            duplicate_tool_calls: 0,
            max_tool_concurrency_observed: 0,
            parallel_tool_batches: 0,
            runtime_write_attempt_paths: Vec::new(),
            runtime_observed_resource_scopes: Vec::new(),
            failure: None,
        }
    }

    #[test]
    fn only_topology_terminal_agent_publishes_the_team_answer() {
        let mut graph = ExecutionGraph::new("research");
        let mut researcher =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        researcher.id = "researcher".into();
        let mut synthesizer =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        synthesizer.id = "synthesizer".into();
        graph.nodes.extend([researcher, synthesizer]);
        graph.edges.push(ExecutionEdge {
            from: "researcher".into(),
            to: "synthesizer".into(),
            kind: ExecutionEdgeKind::DependsOn,
        });

        assert_eq!(
            terminal_agent_node_ids(&graph),
            std::collections::BTreeSet::from(["synthesizer".to_string()])
        );
    }

    #[test]
    fn delivery_envelope_preserves_complete_partial_and_zero_success() {
        let mut complete = ExecutionGraph::new("complete");
        add_agent(&mut complete, "agent-a", ExecutionNodeStatus::Completed);
        add_verify(&mut complete, true);
        let complete = build_delivery_envelope(&complete);
        assert_eq!(complete.delivery_status, DeliveryStatus::Satisfied);
        assert_eq!(complete.branch_terminals.len(), 1);

        let mut partial = ExecutionGraph::new("partial");
        add_agent(&mut partial, "agent-a", ExecutionNodeStatus::Completed);
        add_agent(&mut partial, "agent-b", ExecutionNodeStatus::Failed);
        add_verify(&mut partial, false);
        let partial = build_delivery_envelope(&partial);
        assert_eq!(partial.delivery_status, DeliveryStatus::Partial);
        assert_eq!(partial.branch_terminals.len(), 2);
        assert_eq!(partial.unresolved.len(), 1);

        let mut unavailable = ExecutionGraph::new("unavailable");
        add_agent(&mut unavailable, "agent-a", ExecutionNodeStatus::Failed);
        add_agent(&mut unavailable, "agent-b", ExecutionNodeStatus::Cancelled);
        add_verify(&mut unavailable, false);
        let unavailable = build_delivery_envelope(&unavailable);
        assert_eq!(unavailable.delivery_status, DeliveryStatus::Unavailable);
        assert_eq!(unavailable.branch_terminals.len(), 2);
        assert_eq!(unavailable.unresolved.len(), 2);
    }

    #[test]
    fn denied_write_is_never_promoted_by_mechanical_reduction() {
        let mut graph = ExecutionGraph::new("denied write");
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        node.id = "writer".to_string();
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Failed);
        node.acceptance.required = RequiredAcceptance {
            criteria: Vec::new(),
            evidence_obligations: vec![EvidenceObligation {
                obligation_id: "write-required".to_string(),
                kind: EvidenceObligationKind::WriteEffect,
                target: EvidenceTargetIdentity::Network {
                    endpoint: "fixture".to_string(),
                },
            }],
        };
        graph.nodes.push(node);
        add_verify(&mut graph, false);

        let envelope = build_delivery_envelope(&graph);
        assert_eq!(envelope.delivery_status, DeliveryStatus::Unavailable);
        assert!(envelope
            .verified_effects
            .iter()
            .any(|effect| effect.status == VerifiedEffectStatus::NotApplied));
        assert!(!mechanical_delivery_summary(&envelope).contains("assistant_json"));
    }

    #[test]
    fn only_current_envelope_consuming_terminal_candidate_is_team_synthesizer() {
        let mut graph = ExecutionGraph::new("candidate");
        add_agent(&mut graph, "agent-a", ExecutionNodeStatus::Completed);
        add_verify(&mut graph, true);
        let envelope = build_delivery_envelope(&graph);
        let packet = synthesizer_packet();

        let eligible =
            eligible_team_synthesizer(&returned_candidate(envelope.revision), &packet, &envelope);
        assert!(eligible.is_some());
        assert_eq!(
            eligible.unwrap().1.answer_origin,
            AnswerOrigin::TeamSynthesizer
        );
        assert!(eligible_team_synthesizer(
            &returned_candidate(envelope.revision.saturating_sub(1)),
            &packet,
            &envelope,
        )
        .is_none());
    }
}
