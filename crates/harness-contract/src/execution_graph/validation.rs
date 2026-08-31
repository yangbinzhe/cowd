use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use super::{ExecutionDependencyPolicy, ExecutionGraph, ExecutionNodeKind};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ExecutionGraphValidationError {
    #[error("execution graph objective is empty")]
    EmptyObjective,
    #[error("duplicate execution node `{0}`")]
    DuplicateNode(String),
    #[error("execution node `{0}` has no payload reference")]
    MissingPayload(String),
    #[error("execution node `{0}` has no executor binding")]
    MissingExecutor(String),
    #[error("execution edge references missing node `{0}`")]
    MissingNode(String),
    #[error("execution graph contains a dependency cycle")]
    Cycle,
    #[error("execution completion contract references missing node `{0}`")]
    MissingCompletionNode(String),
    #[error("execution orchestration mutation id is empty")]
    MissingMutationId,
    #[error("timer executor is unavailable before schedule support is installed")]
    TimerUnavailable,
    #[error("execution node `{node_id}` has invalid dependency policy: {reason}")]
    InvalidDependencyPolicy { node_id: String, reason: String },
    #[error("optional execution node `{node_id}` may own a critical or mutating effect")]
    OptionalEffectOwner { node_id: String },
    #[error("execution node `{node_id}` has invalid work contract: {reason}")]
    InvalidWorkContract { node_id: String, reason: String },
    #[error("duplicate collaboration work id `{0}`")]
    DuplicateWorkId(String),
    #[error("work runtime state references missing or non-work node `{0}`")]
    InvalidWorkStateNode(String),
}

pub fn validate_execution_graph(
    graph: &ExecutionGraph,
) -> Result<Vec<Vec<String>>, ExecutionGraphValidationError> {
    if graph.objective.trim().is_empty() {
        return Err(ExecutionGraphValidationError::EmptyObjective);
    }
    let mut ids = BTreeSet::new();
    let mut work_ids = BTreeSet::new();
    for node in &graph.nodes {
        if !ids.insert(node.id.clone()) {
            return Err(ExecutionGraphValidationError::DuplicateNode(
                node.id.clone(),
            ));
        }
        if node.payload_ref.trim().is_empty() {
            return Err(ExecutionGraphValidationError::MissingPayload(
                node.id.clone(),
            ));
        }
        if node.executor_kind.trim().is_empty() {
            return Err(ExecutionGraphValidationError::MissingExecutor(
                node.id.clone(),
            ));
        }
        if node.kind == ExecutionNodeKind::Timer {
            return Err(ExecutionGraphValidationError::TimerUnavailable);
        }
        if let Some(work) = &node.work {
            if let Some(work_id) = &work.collaboration_work_id {
                if work_id.trim().is_empty() {
                    return Err(ExecutionGraphValidationError::InvalidWorkContract {
                        node_id: node.id.clone(),
                        reason: "collaboration_work_id is empty".to_string(),
                    });
                }
                if !work_ids.insert(work_id.clone()) {
                    return Err(ExecutionGraphValidationError::DuplicateWorkId(
                        work_id.clone(),
                    ));
                }
            }
            let invalid_value = work
                .eligibility
                .allowed_agent_instance_ids
                .iter()
                .chain(work.eligibility.allowed_role_ids.iter())
                .chain(work.eligibility.required_capabilities.iter())
                .chain(work.input_artifact_refs.iter())
                .chain(work.output_artifact_kinds.iter())
                .any(|value| value.trim().is_empty());
            if invalid_value {
                return Err(ExecutionGraphValidationError::InvalidWorkContract {
                    node_id: node.id.clone(),
                    reason: "eligibility and artifact contract values must be non-empty"
                        .to_string(),
                });
            }
            if matches!(
                work.review_policy,
                super::ExecutionWorkReviewPolicy::Peer {
                    minimum_reviewers: 0,
                    ..
                }
            ) {
                return Err(ExecutionGraphValidationError::InvalidWorkContract {
                    node_id: node.id.clone(),
                    reason: "peer review requires at least one reviewer".to_string(),
                });
            }
        }
    }
    for (work_id, work) in &graph.autonomous_work {
        if work_id.trim().is_empty()
            || work.collaboration_work_id.as_deref() != Some(work_id.as_str())
            || work
                .objective
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            || work
                .proposed_by
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err(ExecutionGraphValidationError::InvalidWorkContract {
                node_id: work_id.clone(),
                reason: "autonomous work identity, objective and proposer are required".to_string(),
            });
        }
        if !work_ids.insert(work_id.clone()) {
            return Err(ExecutionGraphValidationError::DuplicateWorkId(
                work_id.clone(),
            ));
        }
        let invalid_value = work
            .eligibility
            .allowed_agent_instance_ids
            .iter()
            .chain(work.eligibility.allowed_role_ids.iter())
            .chain(work.eligibility.required_capabilities.iter())
            .chain(work.input_artifact_refs.iter())
            .chain(work.output_artifact_kinds.iter())
            .chain(work.proposal_evidence_refs.iter())
            .any(|value| value.trim().is_empty());
        if invalid_value || work.output_artifact_kinds.is_empty() {
            return Err(ExecutionGraphValidationError::InvalidWorkContract {
                node_id: work_id.clone(),
                reason:
                    "autonomous work capability, evidence and artifact values must be non-empty"
                        .to_string(),
            });
        }
        if matches!(
            work.review_policy,
            super::ExecutionWorkReviewPolicy::Peer {
                minimum_reviewers: 0,
                ..
            }
        ) {
            return Err(ExecutionGraphValidationError::InvalidWorkContract {
                node_id: work_id.clone(),
                reason: "peer review requires at least one reviewer".to_string(),
            });
        }
        if !graph.work_states.contains_key(work_id) {
            return Err(ExecutionGraphValidationError::InvalidWorkStateNode(
                work_id.clone(),
            ));
        }
    }
    for node_id in graph.work_states.keys() {
        if !graph
            .nodes
            .iter()
            .any(|node| node.id == *node_id && node.work.is_some())
            && !graph.autonomous_work.contains_key(node_id)
        {
            return Err(ExecutionGraphValidationError::InvalidWorkStateNode(
                node_id.clone(),
            ));
        }
    }
    for (work_id, state) in &graph.work_states {
        if state.bids.len() > 64
            || state.reviews.len() > 128
            || state.bids.iter().any(|bid| {
                bid.bidder_instance_id.trim().is_empty()
                    || bid.rationale.trim().is_empty()
                    || bid.bid_at_ms == 0
            })
            || state.reviews.iter().any(|review| {
                review.reviewer_instance_id.trim().is_empty()
                    || review.submission_ref.trim().is_empty()
                    || review.reviewed_at_ms == 0
                    || matches!(review.verdict, super::ExecutionWorkReviewVerdict::Challenge)
                        && review
                            .finding
                            .as_deref()
                            .is_none_or(|finding| finding.trim().is_empty())
            })
        {
            return Err(ExecutionGraphValidationError::InvalidWorkContract {
                node_id: work_id.clone(),
                reason: "work bids or review receipts are invalid or exceed their durable bounds"
                    .to_string(),
            });
        }
        let mut bid_agents = BTreeSet::new();
        if state
            .bids
            .iter()
            .any(|bid| !bid_agents.insert(bid.bidder_instance_id.as_str()))
        {
            return Err(ExecutionGraphValidationError::InvalidWorkContract {
                node_id: work_id.clone(),
                reason: "work contains duplicate bidder identities".to_string(),
            });
        }
        let mut reviews = BTreeSet::new();
        if state.reviews.iter().any(|review| {
            !reviews.insert((
                review.submission_ref.as_str(),
                review.reviewer_instance_id.as_str(),
            ))
        }) {
            return Err(ExecutionGraphValidationError::InvalidWorkContract {
                node_id: work_id.clone(),
                reason: "work contains duplicate reviewer identities for one submission"
                    .to_string(),
            });
        }
        let Some(contract) = graph.autonomous_work.get(work_id).or_else(|| {
            graph
                .nodes
                .iter()
                .find(|node| node.id == *work_id)
                .and_then(|node| node.work.as_ref())
        }) else {
            return Err(ExecutionGraphValidationError::InvalidWorkStateNode(
                work_id.clone(),
            ));
        };
        if state.status == super::ExecutionWorkRuntimeStatus::Accepted {
            if let super::ExecutionWorkReviewPolicy::Peer {
                minimum_reviewers, ..
            } = &contract.review_policy
            {
                let current_submission = state.submission_ref.as_deref().unwrap_or_default();
                let accepted_reviewers = state
                    .reviews
                    .iter()
                    .filter(|review| {
                        review.submission_ref == current_submission
                            && review.verdict == super::ExecutionWorkReviewVerdict::Accept
                    })
                    .map(|review| review.reviewer_instance_id.as_str())
                    .collect::<BTreeSet<_>>();
                if current_submission.is_empty()
                    || accepted_reviewers.len() < usize::from(*minimum_reviewers)
                {
                    return Err(ExecutionGraphValidationError::InvalidWorkContract {
                        node_id: work_id.clone(),
                        reason: "accepted peer-reviewed work lacks its required durable reviewer receipts"
                            .to_string(),
                    });
                }
            }
        }
    }
    if let Some(orchestration) = &graph.orchestration {
        if orchestration.mutation_id.trim().is_empty() {
            return Err(ExecutionGraphValidationError::MissingMutationId);
        }
        for node_id in &orchestration.completion.required_node_ids {
            if !ids.contains(node_id) {
                return Err(ExecutionGraphValidationError::MissingCompletionNode(
                    node_id.clone(),
                ));
            }
        }
    }
    let mut indegree = ids
        .iter()
        .map(|id| (id.clone(), 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = BTreeMap::<String, Vec<String>>::new();
    for edge in graph.edges.iter().filter(|edge| edge.kind.is_dependency()) {
        if !ids.contains(&edge.from) {
            return Err(ExecutionGraphValidationError::MissingNode(
                edge.from.clone(),
            ));
        }
        if !ids.contains(&edge.to) {
            return Err(ExecutionGraphValidationError::MissingNode(edge.to.clone()));
        }
        let count = indegree
            .get_mut(&edge.to)
            .ok_or_else(|| ExecutionGraphValidationError::MissingNode(edge.to.clone()))?;
        *count += 1;
        outgoing
            .entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
    }
    for node in &graph.nodes {
        let Some(work) = node.work.as_ref() else {
            continue;
        };
        let predecessor_count = graph
            .edges
            .iter()
            .filter(|edge| edge.kind.is_dependency() && edge.to == node.id)
            .count();
        match work.dependency {
            ExecutionDependencyPolicy::All | ExecutionDependencyPolicy::Finally => {}
            ExecutionDependencyPolicy::Any { .. } if predecessor_count == 0 => {
                return Err(ExecutionGraphValidationError::InvalidDependencyPolicy {
                    node_id: node.id.clone(),
                    reason: "any requires at least one dependency".to_string(),
                });
            }
            ExecutionDependencyPolicy::Quorum { minimum, .. }
                if minimum == 0 || usize::from(minimum) > predecessor_count =>
            {
                return Err(ExecutionGraphValidationError::InvalidDependencyPolicy {
                    node_id: node.id.clone(),
                    reason: format!(
                        "quorum minimum {minimum} exceeds {predecessor_count} dependencies"
                    ),
                });
            }
            ExecutionDependencyPolicy::EvidenceReady { ref predicate, .. } => {
                let minimum = match predicate {
                    super::DependencyPredicate::EvidenceReady { minimum, .. } => *minimum,
                };
                if minimum == 0 || usize::from(minimum) > predecessor_count {
                    return Err(ExecutionGraphValidationError::InvalidDependencyPolicy {
                        node_id: node.id.clone(),
                        reason: format!(
                            "evidence-ready minimum {minimum} exceeds {predecessor_count} dependencies"
                        ),
                    });
                }
            }
            ExecutionDependencyPolicy::Any { .. } | ExecutionDependencyPolicy::Quorum { .. } => {}
        }
        if !work.required
            && (matches!(
                node.kind,
                ExecutionNodeKind::Subgraph
                    | ExecutionNodeKind::Materialize
                    | ExecutionNodeKind::Synthesize
                    | ExecutionNodeKind::Approval
                    | ExecutionNodeKind::SessionDispatch
            ) || matches!(work.role, super::ExecutionWorkRole::Synthesize)
                || node.resource_scopes.iter().any(|scope| {
                    scope.starts_with("write:")
                        || scope.starts_with("worktree:")
                        || scope.starts_with("network:")
                        || scope.starts_with("system:")
                }))
        {
            return Err(ExecutionGraphValidationError::OptionalEffectOwner {
                node_id: node.id.clone(),
            });
        }
    }
    let mut frontier = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(id.clone()))
        .collect::<Vec<_>>();
    let mut batches = Vec::new();
    let mut visited = 0usize;
    while !frontier.is_empty() {
        frontier.sort();
        let batch = frontier;
        visited += batch.len();
        let mut next = Vec::new();
        for id in &batch {
            for target in outgoing.get(id).into_iter().flatten() {
                let count = indegree
                    .get_mut(target)
                    .ok_or_else(|| ExecutionGraphValidationError::MissingNode(target.clone()))?;
                *count -= 1;
                if *count == 0 {
                    next.push(target.clone());
                }
            }
        }
        batches.push(batch);
        frontier = next;
    }
    if visited != graph.nodes.len() {
        return Err(ExecutionGraphValidationError::Cycle);
    }
    Ok(batches)
}
