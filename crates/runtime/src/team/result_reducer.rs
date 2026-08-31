use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::agent::AgentTaskPacket;
#[cfg(test)]
use harness_contract::agent::AgentTerminalStatus;
use harness_contract::context::EvidenceTargetIdentity;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
};
#[cfg(test)]
use harness_contract::outcome::{
    AnswerContentKind, AnswerObjectiveScope, AnswerOrigin, AnswerValidation,
    AnswerValidationStatus, PresentationModelAttempt, TerminalPresentation,
    TerminalPresentationState,
};
use harness_contract::outcome::{
    DeliveryBranchStatus, DeliveryBranchTerminal, DeliveryCoverage, DeliveryEnvelope,
    DeliveryStatus, DeliveryUnresolved, PipelineStatus, UserAnswerContract, VerifiedDeliveryEffect,
    VerifiedDeliveryReference, VerifiedEffectStatus,
};
use harness_contract::team::RoleBehaviorFacet;
#[cfg(test)]
use harness_contract::team::TeamBindingSnapshot;

use crate::execution_core::graph::executors::{SynthesizeBackend, SynthesizeBackendResolver};
use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionOutcome, NodeExecutionTicket,
};
use crate::execution_core::{ImmutableWorkKey, InFlightCoalescer};
use crate::AgentRuntime;

/// Reduces durable graph terminal facts into one DeliveryEnvelope. A
/// wording candidates are derived from committed node summaries, never from
/// process-local AgentRuntime terminal packets.
pub struct TeamResultReducer {
    state_store: ExecutionGraphStateStore,
    coalescer: Arc<InFlightCoalescer<ImmutableWorkKey, NodeExecutionOutcome, String>>,
}

impl TeamResultReducer {
    #[must_use]
    pub fn new(state_store: ExecutionGraphStateStore, _agents: Arc<AgentRuntime>) -> Self {
        Self {
            state_store,
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
        for node in graph.nodes.iter().filter(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
        }) {
            let _: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|_| format!("team node {} is not an AgentTask packet", node.id))?;
            if let Some(result) = graph.node_results.get(&node.id) {
                merge_usage(&mut usage, &result.usage);
                evidence.extend(result.evidence_refs.clone());
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
        // A graph reducer owns durable delivery facts, not a model-owned
        // answer candidate. It may, however, retain a lossless evidence bundle
        // derived solely from committed child summaries and observed workspace
        // paths. The parent uses that typed bundle only when every Team branch
        // has already satisfied the delivery contract; this prevents a
        // provider failure at the root from erasing completed Team evidence.
        let evidence_bundle = verified_team_evidence_bundle(&graph, &envelope);
        // The result reference is deliberately still the mechanical delivery
        // envelope. A summary is not an answer candidate and must not change
        // the child graph's terminal-result contract.
        let result_ref = Some(format!("delivery-envelope: {}", envelope.envelope_id));
        let summary = evidence_bundle.or_else(|| Some(mechanical_delivery_summary(&envelope)));
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
        Ok(outcome)
    }
}

/// Build one deterministic wording input from every completed evidence-bearing
/// worker branch.  A Team result must never depend on whichever branch happens
/// to appear first in the graph, and a useful plain-text result must not be
/// discarded merely because the delegated model omitted an optional JSON
/// wrapper.
fn aggregate_positive_evidence_summary(
    graph: &ExecutionGraph,
    include_reducer_roles: bool,
) -> Option<String> {
    let mut branch_summaries = BTreeMap::new();
    for node in &graph.nodes {
        if node.kind != ExecutionNodeKind::AgentTask {
            continue;
        }
        let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref) else {
            continue;
        };
        if !include_reducer_roles && is_reducer_agent(&packet) {
            continue;
        }
        let Some(result) = graph.node_results.get(&node.id) else {
            continue;
        };
        if result.status != ExecutionNodeStatus::Completed
            || !result
                .evidence_refs
                .iter()
                .any(|evidence| evidence.is_durable())
        {
            continue;
        }
        let Some(raw_summary) = result.summary.as_deref().map(str::trim) else {
            continue;
        };
        if raw_summary.is_empty() {
            continue;
        }
        let mut sections = crate::agent_in_process_worker::structured_agent_output(raw_summary)
            .map(|object| {
                ["findings", "summary", "evidence", "risks", "unresolved"]
                    .into_iter()
                    .filter_map(|field| {
                        positive_field_text(object.get(field)?)
                            .map(|text| format!("{field}: {text}"))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![raw_summary.to_string()]);
        let observed_paths = result
            .usage
            .observed_acceptance
            .observed_evidence
            .iter()
            .filter_map(|evidence| match &evidence.target {
                EvidenceTargetIdentity::Workspace { scope } => {
                    Some(scope.path.workspace_relative_path.clone())
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        if !observed_paths.is_empty() {
            sections.push(format!(
                "observed_source_paths: {}",
                observed_paths.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
        let summary = sections.join("\n");
        if !summary.trim().is_empty() {
            branch_summaries.insert(node.id.clone(), summary);
        }
    }
    (!branch_summaries.is_empty()).then(|| {
        branch_summaries
            .into_iter()
            .map(|(branch_id, summary)| format!("[{branch_id}] {summary}"))
            .collect::<Vec<_>>()
            .join("\n")
    })
}

fn is_reducer_agent(packet: &AgentTaskPacket) -> bool {
    packet.team_role_assignment().is_some_and(|assignment| {
        assignment
            .behavior
            .iter()
            .any(|facet| matches!(facet, RoleBehaviorFacet::Reducer { .. }))
    })
}

/// Return a deterministic Team evidence bundle only when all worker branches
/// are completed, evidence-bearing, and the graph's own delivery envelope is
/// fully satisfied. This is a transport carrier, not an `AnswerCandidate` and
/// must never be attributed to a model or a TeamSynthesizer presentation.
fn verified_team_evidence_bundle(
    graph: &ExecutionGraph,
    envelope: &DeliveryEnvelope,
) -> Option<String> {
    if envelope.pipeline_status != PipelineStatus::Completed
        || envelope.delivery_status != DeliveryStatus::Satisfied
        || !envelope.unresolved.is_empty()
        || envelope.coverage.required_obligation_ids != envelope.coverage.satisfied_obligation_ids
    {
        return None;
    }
    let independent_worker_count = graph
        .nodes
        .iter()
        .filter(|node| {
            node.kind == ExecutionNodeKind::AgentTask
                && serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                    .map(|packet| !is_reducer_agent(&packet))
                    .unwrap_or(false)
        })
        .count();
    // A final Team may deliberately contain only one upstream-consuming
    // reducer/arbiter. It has no independent evidence producer by design,
    // but its bounded terminal narrative is still the required user-facing
    // report. Include that reducer only in this all-reducer topology; mixed
    // Teams continue to derive their evidence bundle from independent workers
    // and never let a reducer overwrite their source findings.
    let include_reducer_roles = independent_worker_count == 0;
    let worker_count = if include_reducer_roles {
        graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .count()
    } else {
        independent_worker_count
    };
    let summary = aggregate_positive_evidence_summary(graph, include_reducer_roles)?;
    let summarized_worker_count = summary.lines().filter(|line| line.starts_with('[')).count();
    (worker_count > 0 && summarized_worker_count == worker_count).then(|| {
        format!(
            "# Verified Team evidence bundle\n\n## Delivery contract\n\nRuntime verification satisfied every declared delivery obligation for this Team. Semantic risks and unresolved research questions, if any, remain exactly as reported below.\n\n{summary}\n\n{}",
            mechanical_delivery_summary(envelope)
        )
    })
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
    let mut workspace_materializations = Vec::new();
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
        let acceptance_evaluation = result.and_then(|result| {
            result
                .usage
                .acceptance_evaluation
                .as_ref()
                .filter(|evaluation| {
                    evaluation.evaluator_revision
                        == crate::acceptance_evaluator::AcceptanceEvaluator::REVISION
                })
        });
        for criterion in &required_acceptance.criteria {
            let id = format!("criterion:{}:{criterion}", node.id);
            required_obligation_ids.push(id.clone());
            if acceptance_evaluation.is_some_and(|evaluation| {
                evaluation.verdict == harness_contract::acceptance::AcceptanceVerdict::Satisfied
            }) && observed_acceptance
                .is_some_and(|observed| observed.satisfied_criteria.contains(criterion))
            {
                satisfied_obligation_ids.push(id);
            }
        }
        for obligation in &required_acceptance.evidence_obligations {
            let id = format!("evidence:{}:{}", node.id, obligation.obligation_id);
            required_obligation_ids.push(id.clone());
            // The terminal evaluator already matched the evidence set.  The
            // reducer reads its unresolved-id footprint instead of invoking a
            // second path matcher over raw observations.
            let satisfied = acceptance_evaluation.is_some_and(|evaluation| {
                evaluation.verdict == harness_contract::acceptance::AcceptanceVerdict::Satisfied
                    && !observed_acceptance.is_some_and(|acceptance| {
                        acceptance
                            .unresolved_obligation_ids
                            .contains(&obligation.obligation_id)
                    })
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

    let materialize_nodes = graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::Materialize)
        .collect::<Vec<_>>();
    for node in &materialize_nodes {
        let status = graph
            .node_statuses
            .get(&node.id)
            .copied()
            .unwrap_or(ExecutionNodeStatus::Blocked);
        let result = graph.node_results.get(&node.id);
        if status != ExecutionNodeStatus::Completed {
            unresolved.push(DeliveryUnresolved {
                unresolved_id: format!("materialization:{}", node.id),
                kind: "workspace_materialization".to_string(),
                summary: result
                    .and_then(|result| result.summary.clone())
                    .unwrap_or_else(|| {
                        format!(
                            "Required workspace materialization `{}` did not complete.",
                            node.id
                        )
                    }),
                source_execution_id: Some(node.id.clone()),
                obligation_id: None,
            });
        }
        if let Some(result) = result {
            if let Some(receipt) = result
                .summary
                .as_deref()
                .and_then(|summary| {
                    serde_json::from_str::<
                    harness_contract::outcome::WorkspaceMaterializationReceipt,
                >(summary).ok()
                })
                .filter(|receipt| receipt.reread_verified)
            {
                verified_receipts.push(VerifiedDeliveryReference {
                    reference_id: receipt.receipt_id.clone(),
                    kind: "workspace_materialization".to_string(),
                    source_execution_id: Some(node.id.clone()),
                });
                workspace_materializations.push(receipt);
            }
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
                    applied_writes.insert((node.id.clone(), receipt.path.clone()));
                    verified_effects.push(VerifiedDeliveryEffect {
                        effect_id: stable_effect_id(&node.id, &receipt.path),
                        kind: "workspace_write".to_string(),
                        status: VerifiedEffectStatus::Applied,
                        receipt_ref: Some(format!(
                            "execution-node:{}:write-sequence:{}:{index}",
                            node.id, receipt.write_sequence
                        )),
                        source_execution_id: Some(node.id.clone()),
                    });
                }
            }
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
    workspace_materializations.sort_by(|left, right| left.receipt_id.cmp(&right.receipt_id));
    workspace_materializations.dedup_by(|left, right| left.receipt_id == right.receipt_id);
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
    let materializations_satisfied = materialize_nodes.iter().all(|node| {
        graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::Completed)
            && workspace_materializations
                .iter()
                .any(|receipt| receipt.receipt_id.ends_with(&node.id))
    });
    let delivery_status = if !branch_terminals.is_empty()
        && completed == branch_terminals.len()
        && verification_satisfied
        && coverage_basis_points == 10_000
        && unresolved.is_empty()
        && !has_not_applied
        && materializations_satisfied
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
        workspace_materializations,
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

#[cfg(test)]
fn eligible_team_synthesizer(
    returned: &harness_contract::agent::AgentReturnPacket,
    packet: &AgentTaskPacket,
    envelope: &DeliveryEnvelope,
    positive_evidence_summary: Option<&str>,
    binding: Option<&TeamBindingSnapshot>,
) -> Option<(
    harness_contract::outcome::AnswerCandidate,
    TerminalPresentation,
)> {
    let _ = positive_evidence_summary;
    let candidate = returned.answer_candidate.as_ref()?;
    if returned.status != AgentTerminalStatus::Completed
        || !is_synthesizer_role(packet, binding)
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

/// Synthesizer eligibility is driven by typed `RoleBehaviorFacet::Reducer`
/// facts from the frozen Team Binding, never by a role-name heuristic. The
/// helper exists only for deterministic reducer tests; production never
/// promotes an Agent result to a user answer without an explicit governed
/// answer candidate.
#[cfg(test)]
fn is_synthesizer_role(packet: &AgentTaskPacket, _binding: Option<&TeamBindingSnapshot>) -> bool {
    packet.team_role_assignment().is_some_and(|assignment| {
        assignment
            .behavior
            .iter()
            .any(|facet| matches!(facet, RoleBehaviorFacet::Reducer { .. }))
    })
}

fn positive_field_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => {
            let text = text.trim();
            (!text.is_empty()).then(|| text.to_string())
        }
        serde_json::Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("；");
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
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

#[cfg(test)]
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
        ChildExecutionBudgetReservation, EvidenceAccessRef, EvidenceObligation,
        EvidenceObligationKind, EvidenceRef, EvidenceTargetIdentity, RequiredAcceptance,
    };
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionFailure, ExecutionGraph, ExecutionNodeKind,
        ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
    };
    use harness_contract::outcome::{
        AnswerCandidate, AnswerContentKind, AnswerObjectiveScope, AnswerOrigin, AnswerValidation,
        AnswerValidationStatus, DeliveryStatus, VerifiedEffectStatus,
        WorkspaceMaterializationReceipt,
    };
    use harness_contract::team::RoleBehaviorFacet;

    use super::{
        aggregate_positive_evidence_summary, build_delivery_envelope, eligible_team_synthesizer,
        is_synthesizer_role, mechanical_delivery_summary, terminal_agent_node_ids,
        verified_team_evidence_bundle,
    };

    fn frozen_role(
        role_id: &str,
        behavior: Vec<harness_contract::team::RoleBehaviorFacet>,
    ) -> harness_contract::team::TeamRoleAssignment {
        use harness_contract::team::{TeamRoleAssignment, TeamRoleIdentity};

        TeamRoleAssignment {
            team_binding_id: "team-binding:fixture".to_string(),
            team_binding_digest: "f".repeat(64),
            identity: TeamRoleIdentity {
                role_id: role_id.to_string(),
                slot: 1,
                focus_id: format!("{role_id}:focus"),
                focus_boundary: "fixture role-local boundary".to_string(),
                evidence_responsibility: "fixture evidence".to_string(),
                focus_scope_hash: "a".repeat(64),
                overlap_budget_bp: 0,
                novelty_target_bp: 0,
                output_acceptance: Vec::new(),
            },
            behavior,
        }
    }

    fn set_frozen_role(
        packet: &mut AgentTaskPacket,
        role_id: &str,
        behavior: Vec<harness_contract::team::RoleBehaviorFacet>,
    ) {
        let role = frozen_role(role_id, behavior);
        packet.assignment.role_id = role.identity.role_id.clone();
        packet.team_role_identity = Some(role.identity.clone());
        packet.team_role = Some(role);
        packet.constraints.clear();
    }

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
            output_acceptance: Vec::new(),
            requires_managed_collaboration_escalation: false,
            acceptance: Vec::new(),
            team_role_identity: Some(
                frozen_role(
                    "synthesizer",
                    vec![RoleBehaviorFacet::Reducer {
                        mode: "finally".to_string(),
                    }],
                )
                .identity,
            ),
            team_role: Some(frozen_role(
                "synthesizer",
                vec![RoleBehaviorFacet::Reducer {
                    mode: "finally".to_string(),
                }],
            )),
            constraints: Vec::new(),
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
            acceptance_evaluation: None,
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

    fn add_evidence_branch(
        graph: &mut ExecutionGraph,
        node_id: &str,
        summary: &str,
        structured: bool,
    ) {
        let mut packet = synthesizer_packet();
        set_frozen_role(&mut packet, "researcher", Vec::new());
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            serde_json::to_string(&packet).expect("packet"),
        );
        node.id = node_id.to_string();
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Completed);
        let mut branch_result = result(ExecutionNodeStatus::Completed, ExecutionUsage::default());
        branch_result.summary = Some(if structured {
            serde_json::json!({"findings": [summary]}).to_string()
        } else {
            summary.to_string()
        });
        branch_result.evidence_refs.push(EvidenceAccessRef::durable(
            EvidenceRef::durable(format!("evidence:{node_id}")),
            "0".repeat(64),
            1,
            "text/plain",
            format!("artifact:{node_id}"),
            "team:fixture",
        ));
        graph.node_results.insert(node.id.clone(), branch_result);
        graph.nodes.push(node);
    }

    fn add_reducer_branch(graph: &mut ExecutionGraph, node_id: &str) {
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            serde_json::to_string(&synthesizer_packet()).expect("reducer packet"),
        );
        node.id = node_id.to_string();
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Completed);
        let mut reducer_result = result(ExecutionNodeStatus::Completed, ExecutionUsage::default());
        reducer_result.summary = Some("mechanical reducer branch".to_string());
        reducer_result
            .evidence_refs
            .push(EvidenceAccessRef::durable(
                EvidenceRef::durable(format!("evidence:{node_id}")),
                "0".repeat(64),
                1,
                "text/plain",
                format!("artifact:{node_id}"),
                "team:fixture",
            ));
        graph.node_results.insert(node.id.clone(), reducer_result);
        graph.nodes.push(node);
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
    fn team_evidence_summary_aggregates_every_branch_in_stable_order() {
        let mut graph = ExecutionGraph::new("all branches");
        // Insert in reverse order to prove scheduling/insertion order cannot
        // select the visible Team result.
        add_evidence_branch(&mut graph, "branch-b", "plain text finding B", false);
        add_evidence_branch(&mut graph, "branch-a", "structured finding A", true);
        // A real Team graph also has a reducer AgentTask. It participates in
        // delivery status but must not be counted as evidence prose.
        add_reducer_branch(&mut graph, "synthesizer");
        add_verify(&mut graph, true);

        assert_eq!(
            aggregate_positive_evidence_summary(&graph, false).as_deref(),
            Some("[branch-a] findings: structured finding A\n[branch-b] plain text finding B")
        );
        let envelope = build_delivery_envelope(&graph);
        let bundle = verified_team_evidence_bundle(&graph, &envelope)
            .expect("completed, evidenced Team branches produce a typed transport bundle");
        assert!(bundle.starts_with("# Verified Team evidence bundle"));
        assert!(
            bundle.contains("Runtime verification satisfied every declared delivery obligation")
        );
        assert!(bundle.contains("Semantic risks and unresolved research questions"));
        assert!(bundle.contains("[branch-a] findings: structured finding A"));
    }

    #[test]
    fn all_reducer_terminal_team_keeps_its_attested_report() {
        let mut graph = ExecutionGraph::new("upstream-only final arbiter");
        add_reducer_branch(&mut graph, "arbiter");
        add_verify(&mut graph, true);

        let envelope = build_delivery_envelope(&graph);
        let bundle = verified_team_evidence_bundle(&graph, &envelope)
            .expect("an upstream-only terminal reducer must retain its nonempty report");
        assert!(bundle.contains("[arbiter] mechanical reducer branch"));
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
    fn delivery_matches_typed_evidence_when_audit_ids_differ() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("Cargo.toml"), "[workspace]\n")
            .expect("workspace manifest");
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .expect("resolver");
        let required = resolver
            .compile_obligation("read:Cargo.toml")
            .expect("required identity");
        let observed = resolver
            .observe_tool_scope("read_file", "read:Cargo.toml", Some("digest"), 1)
            .expect("observed receipt");
        assert_ne!(required.obligation_id, observed.obligation_id);

        let mut graph = ExecutionGraph::new("typed evidence identity");
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        node.id = "researcher".to_string();
        node.acceptance.required = RequiredAcceptance {
            criteria: Vec::new(),
            evidence_obligations: vec![required.clone()],
        };
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Completed);
        let mut usage = ExecutionUsage {
            required_acceptance: node.acceptance.required.clone(),
            ..ExecutionUsage::default()
        };
        usage.observed_acceptance.observed_evidence.push(observed);
        usage.acceptance_evaluation = Some(harness_contract::acceptance::AcceptanceEvaluation {
            evaluator_revision: crate::acceptance_evaluator::AcceptanceEvaluator::REVISION,
            contract_digest: "fixture-contract".to_string(),
            receipt_set_digest: "fixture-receipts".to_string(),
            derived_obligations: vec![required.obligation_id.clone()],
            verdict: harness_contract::acceptance::AcceptanceVerdict::Satisfied,
        });
        graph.node_results.insert(
            node.id.clone(),
            result(ExecutionNodeStatus::Completed, usage),
        );
        graph.nodes.push(node);
        add_verify(&mut graph, true);

        let envelope = build_delivery_envelope(&graph);
        assert_eq!(envelope.delivery_status, DeliveryStatus::Satisfied);
        assert_eq!(envelope.coverage.coverage_basis_points, 10_000);
        assert!(envelope.unresolved.is_empty());
        assert_eq!(
            envelope.coverage.satisfied_obligation_ids,
            envelope.coverage.required_obligation_ids
        );
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
                observation_requirement: Default::default(),
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

        let eligible = eligible_team_synthesizer(
            &returned_candidate(envelope.revision),
            &packet,
            &envelope,
            None,
            None,
        );
        assert!(eligible.is_some());
        assert_eq!(
            eligible.unwrap().1.answer_origin,
            AnswerOrigin::TeamSynthesizer
        );
        assert!(eligible_team_synthesizer(
            &returned_candidate(envelope.revision.saturating_sub(1)),
            &packet,
            &envelope,
            None,
            None,
        )
        .is_none());
    }

    #[test]
    fn terminal_summary_without_an_explicit_candidate_is_not_a_user_answer() {
        let mut graph = ExecutionGraph::new("normalized narrative candidate");
        add_agent(&mut graph, "agent-a", ExecutionNodeStatus::Completed);
        add_verify(&mut graph, true);
        let envelope = build_delivery_envelope(&graph);
        let packet = synthesizer_packet();
        let mut returned = returned_candidate(envelope.revision);
        returned.answer_candidate = None;
        returned.outcome = serde_json::json!({
            "summary": "Team one verified the runtime manifest.",
            "unresolved": []
        })
        .to_string();

        assert!(
            eligible_team_synthesizer(&returned, &packet, &envelope, None, None).is_none(),
            "a summary is evidence, not a validated envelope-consuming answer candidate"
        );
    }

    #[test]
    fn verified_evidence_does_not_upgrade_an_absent_candidate() {
        let mut graph = ExecutionGraph::new("positive evidence fallback");
        add_agent(&mut graph, "agent-a", ExecutionNodeStatus::Completed);
        add_verify(&mut graph, true);
        let envelope = build_delivery_envelope(&graph);
        let packet = synthesizer_packet();
        let mut returned = returned_candidate(envelope.revision);
        returned.answer_candidate = None;
        returned.outcome = serde_json::json!({
            "summary": "Framework did not produce a qualified root answer.",
            "unresolved": ["peer lane unavailable"]
        })
        .to_string();

        assert!(
            eligible_team_synthesizer(
                &returned,
                &packet,
                &envelope,
                Some("Verified manifest finding."),
                None,
            )
            .is_none(),
            "verified evidence belongs in the envelope until a governed narrator produces a candidate"
        );
    }

    #[test]
    fn synthesizer_eligibility_uses_typed_reducer_facet_not_role_name() {
        use harness_contract::team::{
            RoleBehaviorFacet, RoleCardinalityPolicy, RolePartitionPolicy, TeamBindingSnapshot,
            TeamDisplayIdentity, TeamRoleBindingSnapshot,
        };

        let binding = TeamBindingSnapshot {
            binding_id: "team-binding:test".to_string(),
            template_ref: "builtin/cowd/test@1".to_string(),
            template_digest: "digest".to_string(),
            template_name: "Test".to_string(),
            template_description: "Test".to_string(),
            team_instructions: "# Test\n".to_string(),
            roles: vec![TeamRoleBindingSnapshot {
                role_id: "implementer".to_string(),
                slot: 1,
                focus: Some("focus-1".to_string()),
                role_name: "实现者".to_string(),
                role_description: "实现有界变更".to_string(),
                behavior: vec![
                    RoleBehaviorFacet::Reducer {
                        mode: "finally".to_string(),
                    },
                    RoleBehaviorFacet::TerminalCandidate { required: true },
                ],
                agent_definition_ref: "builtin/cowd/execute".to_string(),
                agent_name: "Execute".to_string(),
                agent_description: "Executes".to_string(),
                agent_definition_digest: "digest".to_string(),
                responsibility: "实现有界变更".to_string(),
                cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
                partition: RolePartitionPolicy::Single,
                task_contract_ref: "task/implementer@1".to_string(),
                acceptance: vec!["evidence".to_string()],
                team_markdown_fragment: Some("# Test\n".to_string()),
            }],
            strategy_decision_id: String::new(),
            strategy_decision_revision: 0,
            strategy_decision_lease: String::new(),
            strategy_turn_ref: String::new(),
            display_identity: TeamDisplayIdentity {
                label: "Test".to_string(),
                team_display_name: None,
                role_label: "实现者".to_string(),
                focus_label: Some("focus-1".to_string()),
                locale: "auto".to_string(),
                provenance: "runtime.team.compile".to_string(),
                digest: "digest".to_string(),
            },
            binding_digest: "binding-digest".to_string(),
        };

        let mut packet = synthesizer_packet();
        set_frozen_role(
            &mut packet,
            "implementer",
            vec![RoleBehaviorFacet::Reducer {
                mode: "finally".to_string(),
            }],
        );
        assert!(
            is_synthesizer_role(&packet, Some(&binding)),
            "typed Reducer facet makes the role eligible regardless of its name"
        );
        assert!(
            is_synthesizer_role(&packet, None),
            "the packet carries the frozen behavior, so no mutable Binding lookup is needed"
        );

        let mut renamed = binding.clone();
        renamed.roles[0].role_name = "完全不同的中文名称".to_string();
        renamed.roles[0].role_id = "implementer".to_string();
        assert!(
            is_synthesizer_role(&packet, Some(&renamed)),
            "display label changes never affect behavior eligibility"
        );

        set_frozen_role(&mut packet, "implementer", Vec::new());
        assert!(
            !is_synthesizer_role(&packet, Some(&binding)),
            "a role without the typed Reducer facet is never a synthesizer"
        );
    }

    #[test]
    fn delivery_envelope_carries_reread_verified_materialization_truth() {
        let mut graph = ExecutionGraph::new("materialized Team delivery");
        add_evidence_branch(&mut graph, "researcher", "checked evidence", false);
        let mut materialize =
            ExecutionNodeSpec::new(ExecutionNodeKind::Materialize, "materialize", "{}");
        materialize.id = "materialize-report".to_string();
        graph
            .node_statuses
            .insert(materialize.id.clone(), ExecutionNodeStatus::Completed);
        let receipt = WorkspaceMaterializationReceipt {
            receipt_id: "materialization:graph:materialize-report".to_string(),
            source_execution_id: "graph".to_string(),
            source_node_id: "researcher".to_string(),
            source_result_ref: "result:researcher".to_string(),
            target_path: "reports/final.md".to_string(),
            artifact_kind: "report".to_string(),
            before_sha256: None,
            sha256: "sha256:abcd".to_string(),
            bytes: 42,
            write_effect_id: "write:report".to_string(),
            reread_verified: true,
            materialized_at_ms: 1,
        };
        let change = harness_contract::agent::AgentChangeReceipt {
            path: "reports/final.md".to_string(),
            before_sha256: None,
            after_sha256: "sha256:abcd".to_string(),
            write_sequence: 1,
        };
        let mut usage = ExecutionUsage::default();
        usage.runtime_write_attempt_paths = vec!["reports/final.md".to_string()];
        let mut materialized_result = result(ExecutionNodeStatus::Completed, usage);
        materialized_result.summary = Some(serde_json::to_string(&receipt).unwrap());
        materialized_result.evidence_refs = vec![
            EvidenceAccessRef::durable(
                EvidenceRef::observed("runtime_change", serde_json::to_string(&change).unwrap()),
                "a".repeat(64),
                1,
                "application/json",
                "execution-graph://graph/node/materialize-report".to_string(),
                "workspace",
            ),
            EvidenceAccessRef::durable(
                EvidenceRef::observed("report", "reports/final.md"),
                "b".repeat(64),
                42,
                "text/markdown",
                "workspace://reports/final.md".to_string(),
                "workspace",
            ),
        ];
        graph
            .node_results
            .insert(materialize.id.clone(), materialized_result);
        graph.nodes.push(materialize);

        let envelope = build_delivery_envelope(&graph);

        assert_eq!(envelope.workspace_materializations, vec![receipt]);
        assert!(envelope
            .verified_effects
            .iter()
            .any(|effect| effect.status == VerifiedEffectStatus::Applied));
        assert!(envelope
            .verified_artifacts
            .iter()
            .any(|artifact| artifact.reference_id == "workspace://reports/final.md"));
    }
}
