//! Canonical read and command model for live execution state.
//!
//! This module owns no durable state. It translates the existing graph, goal,
//! agent, team, relation, approval, context and V3 event stores into the one
//! public contract exposed by `harness-contract::projection`.

use std::collections::BTreeSet;

use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeStatus};
use harness_contract::projection::{
    ChildExecutionProjection, ExecutionCommandKind, ExecutionCommandReceipt,
    ExecutionCommandRequest, ExecutionProjection, ProjectionCommandAvailability, ProjectionDelta,
    ProjectionDetailScope, ProjectionEntity, ProjectionEvent, ProjectionEventKind,
    ProjectionQueryContext, StrategyActualProjection, StrategyActualStatus,
    StrategyDecisionProjection, StrategyEvidenceScopeProjection, StrategyProofStatus,
    StrategyTransitionProjection, EXECUTION_PROJECTION_SCHEMA_VERSION,
    STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION,
};
use harness_contract::strategy::{
    ExecutionCandidateEstimate, ExecutionCandidateKind, StrategyDecisionSource,
    StrategyResourceSnapshot,
};
use harness_contract::team::FocusPartitionPlan;

use crate::{ExecutionGraphHost, RuntimeEventScope, RuntimeServices, RuntimeServicesError};

const MAX_DELTA_BATCHES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionAuthorizationScope {
    pub session_id: Option<String>,
    pub mission_id: Option<String>,
    pub resource_grants: Vec<String>,
}

pub fn authorization_scope(
    services: &RuntimeServices,
    execution_id: &str,
) -> Result<ProjectionAuthorizationScope, RuntimeServicesError> {
    let graph = services.graph_state_store().projection(execution_id)?;
    let scope = ExecutionProjectionScope::load(services, execution_id, &graph, false)?;
    let mut resource_grants = scope
        .execution_ids
        .iter()
        .filter_map(|execution_id| services.graph_state_store().load(execution_id).ok())
        .flat_map(|graph| graph.nodes.into_iter())
        .flat_map(|node| node.resource_scopes.into_iter())
        .filter_map(|scope| safe_public_ref(&scope))
        .collect::<Vec<_>>();
    resource_grants.sort();
    resource_grants.dedup();
    Ok(ProjectionAuthorizationScope {
        session_id: scope.session_id,
        mission_id: scope.mission_id,
        resource_grants,
    })
}

pub async fn snapshot(
    services: &RuntimeServices,
    execution_id: &str,
    context: &ProjectionQueryContext,
) -> Result<ExecutionProjection, RuntimeServicesError> {
    validate_context(services, context)?;
    let graph = services
        .graph_runner()
        .graph_projection(execution_id)
        .await?;
    let full = context.detail_scope == ProjectionDetailScope::Full;
    let scope = ExecutionProjectionScope::load(services, execution_id, &graph, full)?;
    let session_id = scope.session_id.clone();
    validate_projection_scope(&scope, context)?;
    let health = vec![ProjectionEntity {
        id: format!("execution-health:{execution_id}"),
        kind: "execution_health".to_string(),
        revision: graph.revision,
        status: Some(graph_status(&graph.nodes)),
        summary: Some("derived from canonical execution graph state".to_string()),
        evidence_refs: Vec::new(),
        detail: full.then(|| {
            serde_json::json!({
                "commit_cursor": graph.commit_cursor,
                "terminal_result_ref": graph.terminal_result_ref,
            })
        }),
    }];
    let strategy = strategy_entity(services, &scope, execution_id, full, context);
    let usage = related_event_entities(services, &scope, "usage", full, |event| {
        // Model, tool and agent node outcomes all carry canonical
        // `ExecutionUsage` in their committed node result. Exposing the
        // execution-node events here lets consumers aggregate a root graph
        // and its durable lineage without scraping session prose timelines.
        event.scope == RuntimeEventScope::Tool
            || event.scope == RuntimeEventScope::ExecutionNode
            || event.kind.contains("usage")
    });
    let context_entities = related_event_entities(services, &scope, "context", full, |event| {
        event.kind.contains("context") || event.kind.contains("memory")
    });
    let evidence = related_event_entities(services, &scope, "evidence", full, |event| {
        !is_strategy_event(&event.kind)
            && (!event.refs.is_empty() || event.kind.contains("evidence"))
    });
    let recovery = related_event_entities(services, &scope, "recovery", full, |event| {
        event.scope == RuntimeEventScope::Recovery || event.kind.contains("recovery")
    });

    Ok(ExecutionProjection {
        schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id: execution_id.to_string(),
        revision: graph.revision,
        cursor: graph.commit_cursor,
        session_id,
        mission_id: scope.mission_id,
        strategy,
        graph,
        child_executions: scope.child_executions,
        goals: scope.goals,
        agents: scope.agents,
        teams: scope.teams,
        relations: scope.relations,
        approvals: scope.approvals,
        interventions: scope.interventions,
        usage,
        context: context_entities,
        evidence,
        health,
        recovery,
        live: services.execution_live(execution_id),
        available_commands: available_commands(services, execution_id, context).await?,
    })
}

fn strategy_entity(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    root_execution_id: &str,
    full: bool,
    context: &ProjectionQueryContext,
) -> Option<StrategyDecisionProjection> {
    let session_id = scope.session_id.as_deref()?;
    let events = services
        .event_store()
        .list_stream(&format!("session:{session_id}"))
        .ok()?;
    let selected = events.iter().rev().find(|event| {
        event.kind == "runtime.strategy.selected"
            && strategy_scope(event).is_some_and(|candidate| {
                candidate.execution_id == root_execution_id && candidate.session_id == session_id
            })
    })?;
    let key = strategy_scope(selected)?;
    let mut decision_events = events
        .iter()
        .filter(|event| {
            is_strategy_event(&event.kind) && strategy_scope(event).as_ref() == Some(&key)
        })
        .collect::<Vec<_>>();
    decision_events.sort_by_key(|event| event.sequence);
    let mut accepted_event: Option<&crate::DurableRuntimeEvent> = None;
    decision_events.retain(|event| {
        let revision = strategy_revision(event);
        if let Some(previous) = accepted_event {
            let accepted_revision = strategy_revision(previous);
            if revision < accepted_revision {
                return false;
            }
            if revision == accepted_revision {
                if !strategy_events_semantically_identical(previous, event) {
                    tracing::warn!(
                        decision_id = %key.decision_id,
                        revision,
                        accepted_event_id = %previous.event_id,
                        conflicting_event_id = %event.event_id,
                        "ignoring conflicting equal-revision strategy event"
                    );
                }
                return false;
            }
        }
        accepted_event = Some(event);
        true
    });
    let latest = decision_events.last().copied()?;
    let revision = strategy_revision(latest);

    let selected_candidate = payload_value::<ExecutionCandidateKind>(latest, "selected_candidate")
        .or_else(|| payload_value(selected, "selected_candidate"));
    if selected_candidate.is_none() {
        return None;
    }
    let policy_version = latest
        .payload
        .get("policy_version")
        .or_else(|| selected.payload.get("policy_version"))
        .and_then(serde_json::Value::as_str)
        .map(|value| safe_public_text(value, 96))
        .filter(|value| !value.is_empty())?;
    let pattern = latest
        .payload
        .get("selected_pattern")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_execution_pattern)
        .or_else(|| {
            selected
                .payload
                .get("selected_pattern")
                .and_then(serde_json::Value::as_str)
                .and_then(parse_execution_pattern)
        });
    let mut candidate_estimates =
        payload_value::<Vec<ExecutionCandidateEstimate>>(latest, "candidate_estimates")
            .or_else(|| payload_value(selected, "candidate_estimates"))
            .unwrap_or_default();
    sanitize_candidate_estimates(&mut candidate_estimates);
    let estimated = selected_candidate.and_then(|candidate| {
        candidate_estimates
            .iter()
            .find(|estimate| estimate.candidate == candidate)
            .cloned()
    });
    let resource_snapshot = payload_value::<StrategyResourceSnapshot>(latest, "resource_snapshot")
        .or_else(|| payload_value(selected, "resource_snapshot"))
        .map(sanitize_resource_snapshot);
    let selection_reasons = latest
        .payload
        .get("selection_reasons")
        .or_else(|| selected.payload.get("selection_reasons"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(|reason| safe_public_text(reason, 240))
        .filter(|reason| !reason.is_empty())
        .collect::<Vec<_>>();
    let (benefit_reasons, cost_reasons) = strategy_public_reasons(
        &selection_reasons,
        selected_candidate,
        estimated.as_ref(),
        &candidate_estimates,
    );
    let evidence_scopes = decision_events
        .iter()
        .rev()
        .find_map(|event| {
            payload_value::<Vec<FocusPartitionPlan>>(event, "evidence_scopes")
                .filter(|plans| !plans.is_empty())
        })
        .map(|plans| crop_strategy_evidence_scopes(plans, context))
        .unwrap_or_default();
    let mut evidence_refs = evidence_scopes
        .iter()
        .flat_map(|scope| scope.capability_cropped_refs.iter().cloned())
        .collect::<Vec<_>>();
    evidence_refs.sort();
    evidence_refs.dedup();
    let downgrades = strategy_transitions(&decision_events, "runtime.strategy.downgraded");
    let early_stops = strategy_transitions(&decision_events, "runtime.strategy.early_stopped");
    let actual = decision_events
        .iter()
        .rev()
        .find(|event| event.kind == "runtime.strategy.outcome")
        .and_then(|event| event.payload.get("outcome"))
        .filter(|outcome| !outcome.is_null())
        .cloned()
        .and_then(|value| serde_json::from_value::<crate::TurnStrategyActualOutcome>(value).ok())
        .map(|outcome| strategy_actual_projection(outcome, context));
    let latest_receipt = decision_events.iter().rev().find_map(|event| {
        event
            .payload
            .get("collaboration_receipt")
            .filter(|receipt| !receipt.is_null())
    });
    let receipt_team_id = latest_receipt
        .and_then(|receipt| {
            receipt
                .get("team_id")
                .or_else(|| receipt.pointer("/evidence/team_id"))
        })
        .and_then(serde_json::Value::as_str)
        .and_then(safe_public_ref);
    let receipt_team_execution_id = latest_receipt
        .and_then(|receipt| {
            receipt
                .pointer("/execution/graph_id")
                .or_else(|| receipt.pointer("/evidence/graph_id"))
        })
        .and_then(serde_json::Value::as_str)
        .and_then(safe_public_ref);
    // A Team graph becomes durable before its terminal collaboration receipt
    // is available.  Expose that already-authoritative topology while it is
    // running, so every surface can distinguish live delegated work from a
    // stalled turn instead of waiting for the parent merge to complete.
    let live_team = (selected_candidate == Some(ExecutionCandidateKind::Team))
        .then(|| live_team_topology(scope))
        .flatten();
    let team_id =
        receipt_team_id.or_else(|| live_team.as_ref().map(|(team_id, _)| team_id.clone()));
    let team_execution_id =
        receipt_team_execution_id.or_else(|| live_team.map(|(_, execution_id)| execution_id));
    let source = payload_value::<StrategyDecisionSource>(latest, "decision_source")
        .or_else(|| payload_value(selected, "decision_source"));
    let confidence = latest
        .payload
        .get("confidence")
        .or_else(|| selected.payload.get("confidence"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .map(|value| value.min(100));
    let proof_status = Some(
        if estimated.as_ref().is_some_and(|estimate| {
            !estimate.assumed
                && estimate.calibration_sample_count > 0
                && estimate
                    .calibration_source
                    .starts_with("strategy-experience-store:paired-and-absolute-cost")
        }) {
            StrategyProofStatus::Calibrated
        } else {
            StrategyProofStatus::NotProven
        },
    );
    let status = if actual.is_none() && latest.kind == "runtime.strategy.selected" {
        "running".to_string()
    } else {
        latest
            .status
            .clone()
            .unwrap_or_else(|| "running".to_string())
    };
    let safe_detail = full.then(|| {
        serde_json::json!({
            "decision_id": key.decision_id.clone(),
            "execution_id": key.execution_id.clone(),
            "session_id": key.session_id.clone(),
            "turn_id": key.turn_id.clone(),
            "selected_candidate": selected_candidate,
            "pattern": pattern,
            "policy_version": policy_version.clone(),
            "proof_status": proof_status,
            "actual_status": if actual.is_some() {
                StrategyActualStatus::Observed
            } else {
                StrategyActualStatus::Unknown
            },
        })
    });
    Some(StrategyDecisionProjection {
        schema_version: STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION,
        id: key.decision_id.clone(),
        kind: "strategy_decision".to_string(),
        revision,
        status: Some(status),
        summary: Some(latest.kind.clone()),
        evidence_refs,
        detail: safe_detail,
        decision_id: Some(key.decision_id),
        execution_id: Some(key.execution_id),
        session_id: Some(key.session_id),
        turn_id: Some(key.turn_id),
        selected_candidate,
        pattern,
        candidate_estimates,
        benefit_reasons,
        cost_reasons,
        evidence_scopes,
        downgrades,
        early_stops,
        estimated,
        actual_status: Some(if actual.is_some() {
            StrategyActualStatus::Observed
        } else {
            StrategyActualStatus::Unknown
        }),
        actual,
        resource_snapshot,
        policy_version: Some(policy_version),
        source,
        confidence,
        proof_status,
        team_id,
        team_execution_id,
    })
}

fn live_team_topology(scope: &ExecutionProjectionScope) -> Option<(String, String)> {
    scope.teams.iter().find_map(|team| {
        let graph_id = team
            .detail
            .as_ref()
            .and_then(|detail| detail.get("graph_id"))
            .and_then(serde_json::Value::as_str)
            .and_then(safe_public_ref)?;
        scope
            .child_executions
            .iter()
            .any(|child| child.execution_id == graph_id)
            .then(|| (team.id.clone(), graph_id))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StrategyProjectionScope {
    execution_id: String,
    session_id: String,
    turn_id: String,
    decision_id: String,
}

fn is_strategy_event(kind: &str) -> bool {
    matches!(
        kind,
        "runtime.strategy.selected"
            | "runtime.strategy.downgraded"
            | "runtime.strategy.early_stopped"
            | "runtime.strategy.outcome"
    )
}

fn strategy_scope(event: &crate::DurableRuntimeEvent) -> Option<StrategyProjectionScope> {
    Some(StrategyProjectionScope {
        execution_id: event
            .payload
            .get("execution_graph_ref")?
            .as_str()
            .and_then(safe_public_ref)?,
        session_id: event
            .payload
            .get("session_ref")?
            .as_str()
            .and_then(safe_public_ref)?,
        turn_id: event
            .payload
            .get("turn_ref")?
            .as_str()
            .and_then(safe_public_ref)?,
        decision_id: event
            .payload
            .get("decision_id")?
            .as_str()
            .and_then(safe_public_ref)?,
    })
}

fn strategy_revision(event: &crate::DurableRuntimeEvent) -> u64 {
    event
        .payload
        .get("decision_revision")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

fn strategy_events_semantically_identical(
    left: &crate::DurableRuntimeEvent,
    right: &crate::DurableRuntimeEvent,
) -> bool {
    left.scope == right.scope
        && left.kind == right.kind
        && left.status == right.status
        && left.actor == right.actor
        && left.refs == right.refs
        && left.payload == right.payload
}

fn payload_value<T: serde::de::DeserializeOwned>(
    event: &crate::DurableRuntimeEvent,
    key: &str,
) -> Option<T> {
    event
        .payload
        .get(key)
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn sanitize_candidate_estimates(estimates: &mut [ExecutionCandidateEstimate]) {
    for estimate in estimates {
        estimate.calibration_source = safe_public_text(&estimate.calibration_source, 160);
        estimate.evidence_overlap_penalty_bp = estimate.evidence_overlap_penalty_bp.min(10_000);
        estimate.provider_concurrency_penalty_bp =
            estimate.provider_concurrency_penalty_bp.min(10_000);
        estimate.risk_approval_penalty_bp = estimate.risk_approval_penalty_bp.min(10_000);
        estimate.reasons = estimate
            .reasons
            .iter()
            .map(|reason| safe_public_text(reason, 240))
            .filter(|reason| !reason.is_empty())
            .collect();
    }
}

fn sanitize_resource_snapshot(mut snapshot: StrategyResourceSnapshot) -> StrategyResourceSnapshot {
    snapshot.version = safe_public_text(&snapshot.version, 96);
    snapshot.sample_source = safe_public_text(&snapshot.sample_source, 160);
    snapshot.provider_concurrency_penalty_bp = snapshot.provider_concurrency_penalty_bp.min(10_000);
    snapshot.provider_profile_fingerprint.clear();
    snapshot
}

fn parse_execution_pattern(value: &str) -> Option<harness_contract::core::ExecutionPattern> {
    use harness_contract::core::ExecutionPattern;
    match value {
        "direct" => Some(ExecutionPattern::Direct),
        "explore" => Some(ExecutionPattern::Explore),
        "execute" => Some(ExecutionPattern::Execute),
        "deliberate" => Some(ExecutionPattern::Deliberate),
        "collaborate" => Some(ExecutionPattern::Collaborate),
        "supervise" => Some(ExecutionPattern::Supervise),
        _ => None,
    }
}

fn strategy_public_reasons(
    selection_reasons: &[String],
    selected_candidate: Option<ExecutionCandidateKind>,
    estimated: Option<&ExecutionCandidateEstimate>,
    candidate_estimates: &[ExecutionCandidateEstimate],
) -> (Vec<String>, Vec<String>) {
    let (selection_cost_warnings, selection_benefits) = selection_reasons
        .iter()
        .map(|reason| safe_public_text(reason, 240))
        .filter(|reason| !reason.is_empty())
        .partition::<Vec<_>, _>(|reason| strategy_reason_is_cost_warning(reason));
    let mut benefit = selection_benefits;
    let mut cost = selection_cost_warnings;
    if let Some(estimate) = estimated {
        if let Some((selected_candidate, alternative)) = selected_candidate.zip(
            candidate_estimates
                .iter()
                .filter(|candidate| {
                    candidate.eligible && Some(candidate.candidate) != selected_candidate
                })
                .max_by_key(|candidate| candidate.net_benefit_score),
        ) {
            benefit.push(format!(
                "selected {} score {} versus strongest eligible alternative {} score {}",
                selected_candidate.as_str(),
                estimate.net_benefit_score,
                alternative.candidate.as_str(),
                alternative.net_benefit_score
            ));
        }
        if estimate.estimated_serial_ms > estimate.estimated_critical_path_ms {
            benefit.push(format!(
                "estimated critical path is {} ms below the {} ms serial baseline",
                estimate
                    .estimated_serial_ms
                    .saturating_sub(estimate.estimated_critical_path_ms),
                estimate.estimated_serial_ms
            ));
        }
        if estimate.expected_quality_lift_bp > 0 {
            benefit.push(format!(
                "expected quality lift is {} basis points",
                estimate.expected_quality_lift_bp
            ));
        }
        for (label, value, unit) in [
            ("startup overhead", estimate.startup_overhead_ms, "ms"),
            (
                "context duplication",
                estimate.context_duplication_tokens,
                "tokens",
            ),
            ("merge cost", estimate.merge_cost_ms, "ms"),
            (
                "evidence overlap penalty",
                u64::from(estimate.evidence_overlap_penalty_bp),
                "bp",
            ),
            (
                "provider concurrency penalty",
                u64::from(estimate.provider_concurrency_penalty_bp),
                "bp",
            ),
            (
                "risk approval penalty",
                u64::from(estimate.risk_approval_penalty_bp),
                "bp",
            ),
        ] {
            if value > 0 {
                cost.push(format!("{label} is {value} {unit}"));
            }
        }
    }
    benefit.sort();
    benefit.dedup();
    cost.sort();
    cost.dedup();
    (benefit, cost)
}

/// A strategy can be selected because it was explicitly requested even when
/// its predicted incremental value is negative.  That is not a benefit: it is
/// a required, user-visible cost warning and must reach the dedicated
/// `cost_reasons` surface field rather than being hidden among rationale.
fn strategy_reason_is_cost_warning(reason: &str) -> bool {
    let normalized = reason.to_ascii_lowercase();
    normalized.contains("negative estimated lift")
        || normalized.contains("surface must show the cost warning")
}

fn crop_strategy_evidence_scopes(
    plans: Vec<FocusPartitionPlan>,
    context: &ProjectionQueryContext,
) -> Vec<StrategyEvidenceScopeProjection> {
    let mut scopes = plans
        .into_iter()
        .flat_map(|plan| {
            plan.slots.into_iter().map(move |slot| {
                let mut refs = slot
                    .capability_cropped_refs
                    .iter()
                    .filter_map(|reference| {
                        safe_public_ref(reference)
                            .filter(|reference| strategy_ref_visible(reference, context, None))
                    })
                    .collect::<Vec<_>>();
                refs.sort();
                refs.dedup();
                StrategyEvidenceScopeProjection {
                    role_id: safe_public_text(&plan.role_id, 96),
                    focus_id: safe_public_text(&slot.focus_id, 96),
                    responsibility_summary: safe_public_text(&slot.evidence_responsibility, 200),
                    capability_cropped_refs: refs,
                    scope_hash: safe_public_text(&slot.scope_hash, 96),
                    overlap_budget_bp: slot.overlap_budget_bp.min(10_000),
                    novelty_target_bp: slot.novelty_target_bp.min(10_000),
                }
            })
        })
        .collect::<Vec<_>>();
    scopes.sort_by(|left, right| {
        left.role_id
            .cmp(&right.role_id)
            .then_with(|| left.focus_id.cmp(&right.focus_id))
    });
    scopes
}

fn strategy_transitions(
    events: &[&crate::DurableRuntimeEvent],
    kind: &str,
) -> Vec<StrategyTransitionProjection> {
    events
        .iter()
        .filter(|event| event.kind == kind)
        .map(|event| StrategyTransitionProjection {
            revision: strategy_revision(event),
            kind: event.kind.clone(),
            status: event
                .status
                .clone()
                .unwrap_or_else(|| "recorded".to_string()),
            summary: event
                .payload
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(|reason| safe_public_text(reason, 240))
                .unwrap_or_else(|| "Runtime strategy policy updated".to_string()),
        })
        .collect()
}

fn strategy_actual_projection(
    outcome: crate::TurnStrategyActualOutcome,
    context: &ProjectionQueryContext,
) -> StrategyActualProjection {
    StrategyActualProjection {
        duration_ms: outcome.duration_ms,
        input_tokens: outcome.input_tokens,
        output_tokens: outcome.output_tokens,
        cached_tokens: outcome.cached_tokens,
        tool_calls: outcome.tool_calls,
        duplicate_tool_calls: outcome.duplicate_tool_calls,
        max_tool_concurrency_observed: outcome.max_tool_concurrency_observed,
        parallel_tool_batches: outcome.parallel_tool_batches,
        write_attempt_refs: outcome
            .write_attempt_paths
            .iter()
            .filter_map(|path| {
                safe_public_ref(path)
                    .filter(|reference| strategy_ref_visible(reference, context, Some("write")))
            })
            .collect(),
        evidence_overlap_bp: outcome.evidence_overlap_bp.min(10_000),
        evidence_overlap_observed: outcome.evidence_overlap_observed,
        working_state_verified: outcome.working_state_verified,
        merge_cost_ms: outcome.merge_cost_ms,
        parent_merge_count: outcome.parent_merge_count,
        evaluation_token_limit: outcome.evaluation_token_limit,
        evaluation_tokens_consumed: outcome.evaluation_tokens_consumed,
        evaluation_budget_observed: outcome.evaluation_budget_observed,
        evaluation_budget_breached: outcome.evaluation_budget_breached,
        quality_score_bp: outcome.quality_score_bp.map(|value| value.min(10_000)),
        actual_speedup_ratio_bp: outcome.actual_speedup_ratio_bp,
        terminal_reason: safe_public_text(&outcome.terminal_reason, 240),
    }
}

fn safe_public_ref(value: &str) -> Option<String> {
    let value = value.trim();
    let lower = value.to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 256
        || value.chars().any(char::is_whitespace)
        || value.starts_with('/')
        || value.starts_with('\\')
        || value.contains("../")
        || value.contains("..\\")
        || lower.contains("/home/")
        || lower.contains("/media/")
        || lower.contains("/tmp/")
        || lower.starts_with("file:")
        || contains_absolute_path(value)
        || value
            .as_bytes()
            .get(1..3)
            .is_some_and(|bytes| bytes == b":\\" || bytes == b":/")
    {
        return None;
    }
    Some(value.to_string())
}

fn strategy_ref_visible(
    reference: &str,
    context: &ProjectionQueryContext,
    required_mode: Option<&str>,
) -> bool {
    let (_, raw_path) = reference
        .split_once(':')
        .filter(|(mode, _)| matches!(*mode, "read" | "write" | "worktree"))
        .map_or((required_mode, reference), |(mode, path)| {
            (Some(mode), path)
        });
    if !safe_workspace_relative_path(raw_path) {
        return false;
    }
    if context
        .visibility_grants
        .iter()
        .any(|grant| grant == "resource:*")
    {
        return true;
    }
    let (reference_mode, reference_path) = reference
        .split_once(':')
        .filter(|(mode, _)| matches!(*mode, "read" | "write" | "worktree"))
        .map_or((required_mode, reference), |(mode, path)| {
            (Some(mode), path)
        });
    let Some(reference_mode) = reference_mode else {
        // Opaque execution-local evidence identifiers are safe once the
        // execution/session/mission authorization predicate has passed.
        return true;
    };
    context.visibility_grants.iter().any(|grant| {
        let Some((grant_mode, grant_path)) = grant.split_once(':') else {
            return false;
        };
        if grant_mode != reference_mode {
            return false;
        }
        grant_path == "."
            || grant_path == reference_path
            || reference_path
                .strip_prefix(grant_path.trim_end_matches('/'))
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn safe_workspace_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.starts_with('\\')
        && !path.contains("../")
        && !path.contains("..\\")
        && !contains_absolute_path(path)
        && !path
            .as_bytes()
            .get(1..3)
            .is_some_and(|bytes| bytes == b":\\" || bytes == b":/")
}

fn safe_public_text(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = normalized.to_ascii_lowercase();
    if lower.contains("/home/")
        || lower.contains("/media/")
        || lower.contains("/tmp/")
        || lower.contains("../")
        || lower.contains("..\\")
        || lower.contains("prompt")
        || lower.contains("chain of thought")
        || lower.contains("reasoning")
        || lower.contains("hidden")
        || contains_absolute_path(&normalized)
    {
        return "redacted by strategy projection policy".to_string();
    }
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized.chars().take(max_chars).collect::<String>() + "…"
    }
}

fn contains_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    if value.to_ascii_lowercase().contains("file:") {
        return true;
    }
    bytes.iter().enumerate().any(|(index, byte)| {
        let previous = index.checked_sub(1).and_then(|offset| bytes.get(offset));
        let boundary = previous.is_none_or(|value| {
            value.is_ascii_whitespace()
                || matches!(
                    *value,
                    b'(' | b'['
                        | b'{'
                        | b':'
                        | b'='
                        | b','
                        | b'\''
                        | b'"'
                        | b'`'
                        | b'>'
                        | b'<'
                        | b';'
                        | b'|'
                        | b'&'
                        | b'-'
                        | b'_'
                )
        });
        if *byte == b'/' {
            return boundary && bytes.get(index + 1).is_some_and(|next| *next != b'/');
        }
        byte.is_ascii_alphabetic()
            && bytes.get(index + 1) == Some(&b':')
            && bytes
                .get(index + 2)
                .is_some_and(|next| matches!(*next, b'/' | b'\\'))
            && boundary
    })
}

fn related_event_entities(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    kind: &str,
    full: bool,
    predicate: impl Fn(&crate::DurableRuntimeEvent) -> bool,
) -> Vec<ProjectionEntity> {
    services
        .event_store()
        .all_events(512)
        .unwrap_or_default()
        .into_iter()
        .filter(|event| scope.contains_event(event) && predicate(event))
        .map(|event| entity_from_runtime_event(kind, event, full))
        .collect()
}

fn entity_from_runtime_event(
    kind: &str,
    event: crate::DurableRuntimeEvent,
    full: bool,
) -> ProjectionEntity {
    let strategy_event = is_strategy_event(&event.kind);
    let detail = full.then(|| {
        if strategy_event {
            safe_strategy_event_detail(&event)
        } else {
            event.payload.clone()
        }
    });
    ProjectionEntity {
        id: event.event_id,
        kind: kind.to_string(),
        revision: event.sequence,
        status: event.status,
        summary: Some(event.kind),
        evidence_refs: event
            .refs
            .into_iter()
            .filter_map(|reference| {
                if strategy_event {
                    safe_public_ref(&reference.id)
                } else {
                    Some(reference.id)
                }
            })
            .collect(),
        detail,
    }
}

fn safe_strategy_event_detail(event: &crate::DurableRuntimeEvent) -> serde_json::Value {
    serde_json::json!({
        "decision_id": safe_event_payload_ref(event, "decision_id"),
        "decision_revision": strategy_revision(event),
        "execution_id": safe_event_payload_ref(event, "execution_graph_ref"),
        "session_id": safe_event_payload_ref(event, "session_ref"),
        "turn_id": safe_event_payload_ref(event, "turn_ref"),
        "selected_candidate": event.payload.get("selected_candidate"),
        "selected_pattern": event.payload.get("selected_pattern"),
        "status": event.status.as_deref(),
        "policy_version": event
            .payload
            .get("policy_version")
            .and_then(serde_json::Value::as_str)
            .map(|value| safe_public_text(value, 96)),
    })
}

fn safe_event_payload_ref(event: &crate::DurableRuntimeEvent, key: &str) -> Option<String> {
    event
        .payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .and_then(safe_public_ref)
}

pub fn delta(
    services: &RuntimeServices,
    execution_id: &str,
    base_cursor: u64,
    context: &ProjectionQueryContext,
) -> Result<ProjectionDelta, RuntimeServicesError> {
    validate_context(services, context)?;
    let graph = services.graph_state_store().projection(execution_id)?;
    let scope = ExecutionProjectionScope::load(
        services,
        execution_id,
        &graph,
        context.detail_scope == ProjectionDetailScope::Full,
    )?;
    validate_projection_scope(&scope, context)?;
    let mut events = Vec::new();
    let mut target_cursor = base_cursor;
    for batch in services
        .event_store()
        .events_after_cursor(base_cursor, MAX_DELTA_BATCHES)?
    {
        target_cursor = batch.commit_cursor;
        let mut visible = false;
        for event in batch.events {
            if scope.contains_event(&event) {
                visible = true;
                events.push(event_from_runtime(event, context));
            }
        }
        if !visible {
            events.push(ProjectionEvent {
                commit_cursor: batch.commit_cursor,
                transaction_index: 0,
                event_id: format!("cursor:{}", batch.commit_cursor),
                kind: ProjectionEventKind::CursorAdvanced,
                entity: None,
            });
        }
    }
    Ok(ProjectionDelta {
        schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
        execution_id: execution_id.to_string(),
        base_cursor,
        target_cursor,
        events,
    })
}

pub async fn command(
    services: &RuntimeServices,
    execution_id: &str,
    context: &ProjectionQueryContext,
    request: ExecutionCommandRequest,
) -> Result<ExecutionCommandReceipt, RuntimeServicesError> {
    validate_context(services, context)?;
    let graph = services
        .graph_runner()
        .graph_projection(execution_id)
        .await?;
    let session_id = session_id_from_graph(services, execution_id);
    validate_session_scope(session_id.as_deref(), context)?;
    let mission_id = session_id.as_deref().and_then(|session_id| {
        services
            .mission_runtime()
            .mission_id_for_session(session_id)
    });
    validate_mission_scope(mission_id.as_deref(), context)?;
    let command = match request.command {
        ExecutionCommandKind::Pause => ExecutionGraphCommand::Pause {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "paused by projection command".to_string()),
        },
        ExecutionCommandKind::Resume => ExecutionGraphCommand::Resume {
            expected_revision: request.expected_revision,
        },
        ExecutionCommandKind::Cancel => ExecutionGraphCommand::Cancel {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "cancelled by projection command".to_string()),
        },
        ExecutionCommandKind::Replan => ExecutionGraphCommand::Replan {
            expected_revision: request.expected_revision,
            reason: string_payload(&request.payload, "reason")
                .unwrap_or_else(|| "replan requested by projection command".to_string()),
            replacement_payload_ref: string_payload(&request.payload, "replacement_payload_ref")
                .unwrap_or_else(|| "projection-command:replan".to_string()),
        },
    };
    if graph.revision != request.expected_revision {
        return Ok(ExecutionCommandReceipt {
            command_id: request.command_id,
            accepted_revision: graph.revision,
            status: "rejected_stale_revision".to_string(),
            reason: Some(
                "projection revision changed; refresh snapshot before retrying".to_string(),
            ),
        });
    }
    let receipt = services
        .graph_runner()
        .command_graph(execution_id, command)
        .await?;
    Ok(ExecutionCommandReceipt {
        command_id: request.command_id,
        accepted_revision: receipt.graph.revision,
        status: "accepted".to_string(),
        reason: None,
    })
}

async fn available_commands(
    services: &RuntimeServices,
    execution_id: &str,
    _context: &ProjectionQueryContext,
) -> Result<Vec<ProjectionCommandAvailability>, RuntimeServicesError> {
    let graph = services
        .graph_runner()
        .graph_projection(execution_id)
        .await?;
    let terminal = graph.nodes.iter().all(|node| node.status.is_terminal());
    let paused = graph
        .nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::Paused);
    Ok([
        ExecutionCommandKind::Pause,
        ExecutionCommandKind::Resume,
        ExecutionCommandKind::Cancel,
        ExecutionCommandKind::Replan,
    ]
    .into_iter()
    .map(|command| {
        let available = match command {
            ExecutionCommandKind::Pause => !terminal && !paused,
            ExecutionCommandKind::Resume => !terminal && paused,
            ExecutionCommandKind::Cancel | ExecutionCommandKind::Replan => !terminal,
        };
        ProjectionCommandAvailability {
            command,
            available,
            reason: (!available)
                .then(|| "execution state does not permit this command".to_string()),
        }
    })
    .collect())
}

fn validate_context(
    services: &RuntimeServices,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if context.principal.trim().is_empty()
        || context.authorization_revision == 0
        || context.workspace_id != services.workspace_key()
    {
        return Err(RuntimeServicesError::ProjectionAccessDenied);
    }
    Ok(())
}

fn validate_projection_scope(
    scope: &ExecutionProjectionScope,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    validate_session_scope(scope.session_id.as_deref(), context)?;
    validate_mission_scope(scope.mission_id.as_deref(), context)
}

fn has_workspace_visibility(context: &ProjectionQueryContext) -> bool {
    context
        .visibility_grants
        .iter()
        .any(|grant| grant == &format!("workspace:{}", context.workspace_id))
}

fn validate_session_scope(
    session_id: Option<&str>,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if let Some(session_id) = session_id {
        if !has_workspace_visibility(context)
            && !context
                .session_scopes
                .iter()
                .any(|scope| scope == session_id)
        {
            return Err(RuntimeServicesError::ProjectionAccessDenied);
        }
    }
    Ok(())
}

fn validate_mission_scope(
    mission_id: Option<&str>,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if let Some(mission_id) = mission_id {
        if !has_workspace_visibility(context)
            && !context
                .mission_scopes
                .iter()
                .any(|scope| scope == mission_id)
        {
            return Err(RuntimeServicesError::ProjectionAccessDenied);
        }
    }
    Ok(())
}

/// The read scope is derived from durable graph bindings before any domain
/// projection is assembled. This prevents a workspace-wide query from
/// accidentally becoming an execution-wide response.
struct ExecutionProjectionScope {
    session_id: Option<String>,
    mission_id: Option<String>,
    execution_ids: BTreeSet<String>,
    node_ids: BTreeSet<String>,
    entity_ids: BTreeSet<String>,
    goals: Vec<ProjectionEntity>,
    agents: Vec<ProjectionEntity>,
    teams: Vec<ProjectionEntity>,
    relations: Vec<ProjectionEntity>,
    approvals: Vec<ProjectionEntity>,
    interventions: Vec<ProjectionEntity>,
    child_executions: Vec<ChildExecutionProjection>,
}

impl ExecutionProjectionScope {
    fn load(
        services: &RuntimeServices,
        execution_id: &str,
        graph: &harness_contract::execution_graph::ExecutionGraphProjection,
        full: bool,
    ) -> Result<Self, RuntimeServicesError> {
        let session_id = session_id_from_graph(services, execution_id);
        let mission_id = session_id.as_deref().and_then(|session_id| {
            services
                .mission_runtime()
                .mission_id_for_session(session_id)
        });
        let (execution_ids, child_executions, node_ids) =
            execution_lineage(services, execution_id, graph)?;

        let agent_snapshots = services
            .agent_runtime()
            .list()
            .into_iter()
            .filter(|agent| execution_ids.contains(&agent.graph_id))
            .collect::<Vec<_>>();
        let agent_ids = agent_snapshots
            .iter()
            .flat_map(|agent| [agent.agent_id.clone(), agent.run_id.clone()])
            .collect::<BTreeSet<_>>();
        let agents = entities_from_details(
            "agent",
            agent_snapshots
                .into_iter()
                .filter_map(|agent| serde_json::to_value(agent).ok()),
            full,
        );

        let team_snapshots = services
            .team_runtime()
            .list()
            .unwrap_or_default()
            .into_iter()
            .filter(|team| execution_ids.contains(&team.graph_id))
            .collect::<Vec<_>>();
        let team_ids = team_snapshots
            .iter()
            .map(|team| team.team_id.clone())
            .collect::<BTreeSet<_>>();
        let teams = entities_from_details(
            "team",
            team_snapshots
                .into_iter()
                .filter_map(|team| serde_json::to_value(team).ok()),
            full,
        );

        let goal_projections = goals_for_executions(services, &execution_ids);
        let goal_ids = goal_projections
            .iter()
            .map(|projection| projection.goal.id.clone())
            .collect::<BTreeSet<_>>();
        let goals = goal_projections
            .iter()
            .map(|projection| ProjectionEntity {
                id: projection.goal.id.clone(),
                kind: "goal".to_string(),
                revision: projection.stream_revision,
                status: Some(projection.goal.phase.clone()),
                summary: Some(projection.goal.objective.clone()),
                evidence_refs: projection.goal.evidence_refs.clone(),
                detail: full.then(|| serde_json::to_value(projection).unwrap_or_default()),
            })
            .collect();
        let interventions = goal_projections
            .into_iter()
            .flat_map(|projection| projection.interventions.into_iter())
            .enumerate()
            .map(|(index, intervention)| {
                entity_from_value(
                    "intervention",
                    serde_json::to_value(intervention).unwrap_or_default(),
                    index as u64,
                    full,
                )
            })
            .collect();

        let relation_snapshots = session_id
            .as_deref()
            .map(|id| services.session_relations().relations_for(id))
            .unwrap_or_default();
        let relation_ids = relation_snapshots
            .iter()
            .map(|relation| relation.relation_id.clone())
            .collect::<BTreeSet<_>>();
        let relations = entities_from_details(
            "session_relation",
            relation_snapshots
                .into_iter()
                .filter_map(|relation| serde_json::to_value(relation).ok()),
            full,
        );

        let approvals = services
            .approval_queue()
            .list()
            .into_iter()
            .filter(|approval| {
                approval
                    .source
                    .session_id
                    .as_deref()
                    .is_some_and(|id| session_id.as_deref() == Some(id))
                    || approval
                        .source
                        .agent_id
                        .as_ref()
                        .is_some_and(|id| agent_ids.contains(id))
                    || approval
                        .source
                        .team_id
                        .as_ref()
                        .is_some_and(|id| team_ids.contains(id))
            })
            .collect::<Vec<_>>();
        let approval_ids = approvals
            .iter()
            .map(|approval| approval.approval_id.clone())
            .collect::<BTreeSet<_>>();
        let approvals = entities_from_details(
            "approval",
            approvals
                .into_iter()
                .filter_map(|approval| serde_json::to_value(approval).ok()),
            full,
        );

        let mut entity_ids = agent_ids;
        entity_ids.extend(team_ids);
        entity_ids.extend(goal_ids);
        entity_ids.extend(relation_ids);
        entity_ids.extend(approval_ids);
        Ok(Self {
            session_id,
            mission_id,
            execution_ids,
            node_ids,
            entity_ids,
            goals,
            agents,
            teams,
            relations,
            approvals,
            interventions,
            child_executions,
        })
    }

    fn contains_event(&self, event: &crate::DurableRuntimeEvent) -> bool {
        self.execution_ids.contains(&event.stream_id)
            || self.execution_ids.iter().any(|execution_id| {
                event
                    .stream_id
                    .starts_with(&format!("{execution_id}:node:"))
            })
            || event.refs.iter().any(|reference| {
                (reference.kind == "execution_graph" && self.execution_ids.contains(&reference.id))
                    || (reference.kind == "execution_node" && self.node_ids.contains(&reference.id))
                    || (reference.kind == "session"
                        && self.session_id.as_deref() == Some(reference.id.as_str()))
                    || self.entity_ids.contains(&reference.id)
            })
            || ["goal:", "approval:", "agent:"]
                .iter()
                .filter_map(|prefix| event.stream_id.strip_prefix(prefix))
                .any(|id| self.entity_ids.contains(id))
    }
}

fn session_id_from_graph(services: &RuntimeServices, execution_id: &str) -> Option<String> {
    let graph = services.graph_state_store().load(execution_id).ok()?;
    graph.nodes.iter().find_map(|node| {
        serde_json::from_str::<serde_json::Value>(&node.payload_ref)
            .ok()
            .and_then(|payload| {
                payload
                    .get("session_id")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                node.payload_ref
                    .strip_prefix("session_handoff:")
                    .and_then(|payload| {
                        serde_json::from_str::<harness_contract::turn::SessionDispatchCommand>(
                            payload,
                        )
                        .ok()
                        .map(|command| command.handoff.source_session_id)
                    })
            })
    })
}

fn entities_from_details(
    kind: &str,
    details: impl IntoIterator<Item = serde_json::Value>,
    full: bool,
) -> Vec<ProjectionEntity> {
    details
        .into_iter()
        .enumerate()
        .map(|(index, detail)| entity_from_value(kind, detail, index as u64, full))
        .collect()
}

fn entity_from_value(
    kind: &str,
    detail: serde_json::Value,
    revision: u64,
    full: bool,
) -> ProjectionEntity {
    let id = ["id", "agent_id", "team_id", "relation_id", "approval_id"]
        .iter()
        .find_map(|key| detail.get(*key).and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{kind}:{revision}"));
    let status = detail
        .get("status")
        .or_else(|| detail.get("state"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let summary = detail
        .get("summary")
        .or_else(|| detail.get("objective"))
        .or_else(|| detail.get("title"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    ProjectionEntity {
        id,
        kind: kind.to_string(),
        revision,
        status,
        summary,
        evidence_refs: detail
            .get("evidence_refs")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
        detail: full.then_some(detail),
    }
}

fn goals_for_executions(
    services: &RuntimeServices,
    execution_ids: &BTreeSet<String>,
) -> Vec<crate::execution_core::GoalProjection> {
    services
        .event_store()
        .stream_ids_for_scope(RuntimeEventScope::Goal)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|stream| stream.strip_prefix("goal:").map(ToOwned::to_owned))
        .filter_map(|goal_id| services.goal_store().projection(&goal_id).ok().flatten())
        .filter(|projection| {
            projection
                .goal
                .id
                .strip_prefix("goal:")
                .is_some_and(|execution_id| execution_ids.contains(execution_id))
        })
        .collect()
}

/// Resolves the durable execution lineage rooted at `execution_id`. Child
/// graphs retain an immutable parent binding in their canonical graph state;
/// registration atomically writes a reverse relation event for the same fact.
/// Therefore a root projection walks only its durable descendant index and
/// never scans every graph in the runtime or infers containment from prose.
fn execution_lineage(
    services: &RuntimeServices,
    execution_id: &str,
    root: &harness_contract::execution_graph::ExecutionGraphProjection,
) -> Result<
    (
        BTreeSet<String>,
        Vec<ChildExecutionProjection>,
        BTreeSet<String>,
    ),
    RuntimeServicesError,
> {
    let mut execution_ids = BTreeSet::from([execution_id.to_string()]);
    let mut child_executions = Vec::new();
    let mut discovered = vec![execution_id.to_string()];
    let mut lineage_graphs = Vec::new();
    while let Some(parent_execution_id) = discovered.pop() {
        for link in services
            .graph_state_store()
            .child_links(&parent_execution_id)?
        {
            if !execution_ids.insert(link.child_execution_id.clone()) {
                continue;
            }
            let graph = services
                .graph_state_store()
                .projection(&link.child_execution_id)?;
            let parent = graph.parent_execution.as_ref().ok_or_else(|| {
                RuntimeServicesError::Invariant(format!(
                    "lineage index references child graph `{}` without a parent binding",
                    graph.graph_id
                ))
            })?;
            if parent.execution_id != link.parent_execution_id
                || parent.node_id != link.parent_node_id
            {
                return Err(RuntimeServicesError::Invariant(format!(
                    "lineage index disagrees with child graph `{}` parent binding",
                    graph.graph_id
                )));
            }
            child_executions.push(ChildExecutionProjection {
                execution_id: graph.graph_id.clone(),
                parent_execution_id: parent.execution_id.clone(),
                parent_node_id: parent.node_id.clone(),
                revision: graph.revision,
                cursor: graph.commit_cursor,
                status: graph_status(&graph.nodes),
                objective: graph.objective.clone(),
            });
            discovered.push(graph.graph_id.clone());
            lineage_graphs.push(graph);
        }
    }
    child_executions.sort_by(|left, right| left.execution_id.cmp(&right.execution_id));
    let mut node_ids = root
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    for graph in lineage_graphs {
        node_ids.extend(graph.nodes.into_iter().map(|node| node.node_id));
    }
    Ok((execution_ids, child_executions, node_ids))
}

fn event_from_runtime(
    event: crate::DurableRuntimeEvent,
    context: &ProjectionQueryContext,
) -> ProjectionEvent {
    let strategy_event = is_strategy_event(&event.kind);
    let detail = (context.detail_scope == ProjectionDetailScope::Full).then(|| {
        if strategy_event {
            safe_strategy_event_detail(&event)
        } else {
            event.payload.clone()
        }
    });
    let kind = if strategy_event {
        ProjectionEventKind::StrategyChanged
    } else if event.kind == "execution.lineage.child_registered.v1" {
        ProjectionEventKind::UpsertChildExecution
    } else if event.kind.contains("terminal") {
        ProjectionEventKind::TerminalCommitted
    } else if event.scope == RuntimeEventScope::Goal {
        ProjectionEventKind::GoalChanged
    } else if event.scope == RuntimeEventScope::Agent {
        ProjectionEventKind::UpsertAgent
    } else if event.scope == RuntimeEventScope::Team {
        ProjectionEventKind::UpsertTeam
    } else if event.scope == RuntimeEventScope::Approval {
        ProjectionEventKind::ApprovalChanged
    } else if event.scope == RuntimeEventScope::Relation {
        ProjectionEventKind::UpsertSessionRelation
    } else if event.scope == RuntimeEventScope::Tool {
        ProjectionEventKind::UsageChanged
    } else if event.scope == RuntimeEventScope::ExecutionNode {
        ProjectionEventKind::UpsertNode
    } else {
        ProjectionEventKind::HealthChanged
    };
    ProjectionEvent {
        commit_cursor: event.commit_cursor,
        transaction_index: event.transaction_index,
        event_id: event.event_id.clone(),
        kind,
        entity: Some(ProjectionEntity {
            id: event.event_id,
            kind: event.kind,
            revision: event.sequence,
            status: event.status,
            summary: Some(event.stream_id),
            evidence_refs: event
                .refs
                .into_iter()
                .filter_map(|reference| {
                    if strategy_event {
                        safe_public_ref(&reference.id)
                            .filter(|reference| strategy_ref_visible(reference, context, None))
                    } else {
                        Some(reference.id)
                    }
                })
                .collect(),
            detail,
        }),
    }
}

fn graph_status(nodes: &[harness_contract::execution_graph::ExecutionNodeProjection]) -> String {
    if nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::Failed)
    {
        "failed".to_string()
    } else if nodes.iter().all(|node| node.status.is_terminal()) {
        "terminal".to_string()
    } else if nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::WaitingExternal)
    {
        "waiting_external".to_string()
    } else {
        "running".to_string()
    }
}

fn string_payload(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{
        execution_graph::{
            ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
            ExecutionParentBinding,
        },
        goal::{AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract},
    };

    fn context(services: &RuntimeServices) -> ProjectionQueryContext {
        ProjectionQueryContext {
            principal: "test".to_string(),
            workspace_id: services.workspace_key().to_string(),
            session_scopes: Vec::new(),
            mission_scopes: Vec::new(),
            visibility_grants: vec![
                format!("workspace:{}", services.workspace_key()),
                "resource:*".to_string(),
            ],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 1,
        }
    }

    #[test]
    fn explicit_negative_strategy_warning_projects_as_cost_not_benefit() {
        let warning =
            "explicit Team request has negative estimated lift; surface must show the cost warning";
        let (benefit, cost) = strategy_public_reasons(
            &[
                warning.to_string(),
                "explicit Team topology was requested".to_string(),
            ],
            None,
            None,
            &[],
        );

        assert!(benefit.contains(&"explicit Team topology was requested".to_string()));
        assert!(!benefit.contains(&warning.to_string()));
        assert_eq!(cost, vec![warning.to_string()]);
    }

    #[tokio::test]
    async fn projection_snapshot_delta_and_command_share_one_graph_revision() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("projection contract graph");
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial_snapshot = snapshot(&services, &graph_id, &query)
            .await
            .expect("snapshot");
        assert_eq!(initial_snapshot.execution_id, graph_id);
        assert_eq!(
            initial_snapshot.schema_version,
            EXECUTION_PROJECTION_SCHEMA_VERSION
        );
        let delta = delta(&services, &initial_snapshot.execution_id, 0, &query).expect("delta");
        assert!(delta.target_cursor >= initial_snapshot.cursor);
        assert!(delta.events.iter().all(|event| event.commit_cursor > 0));
        let receipt = command(
            &services,
            &initial_snapshot.execution_id,
            &query,
            ExecutionCommandRequest {
                command_id: "projection-pause".to_string(),
                expected_revision: initial_snapshot.revision,
                command: ExecutionCommandKind::Pause,
                payload: serde_json::json!({ "reason": "test" }),
            },
        )
        .await
        .expect("command receipt");
        assert_eq!(receipt.status, "accepted");
        assert!(receipt.accepted_revision > initial_snapshot.revision);
    }

    #[tokio::test]
    async fn projection_exposes_only_durable_child_execution_lineage() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let parent = ExecutionGraph::new("root execution");
        let parent_id = parent.id.clone();
        services
            .graph_runner()
            .start(parent)
            .await
            .expect("parent graph starts");

        let mut child = ExecutionGraph::new("nested team protocol");
        child.parent_execution = Some(ExecutionParentBinding {
            execution_id: parent_id.clone(),
            node_id: "root-tool-batch".to_string(),
        });
        let child_id = child.id.clone();
        services
            .graph_runner()
            .start(child)
            .await
            .expect("child graph starts");

        let sibling = ExecutionGraph::new("unrelated same-runtime execution");
        let sibling_id = sibling.id.clone();
        services
            .graph_runner()
            .start(sibling)
            .await
            .expect("sibling graph starts");

        let links = services
            .graph_state_store()
            .child_links(&parent_id)
            .expect("durable child index");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].child_execution_id, child_id);
        assert_eq!(links[0].parent_node_id, "root-tool-batch");

        let projection = snapshot(&services, &parent_id, &context(&services))
            .await
            .expect("parent projection");
        assert_eq!(projection.child_executions.len(), 1);
        assert_eq!(projection.child_executions[0].execution_id, child_id);
        assert_eq!(
            projection.child_executions[0].parent_node_id,
            "root-tool-batch"
        );
        assert!(projection
            .child_executions
            .iter()
            .all(|child| child.execution_id != sibling_id));

        let delta = delta(&services, &parent_id, 0, &context(&services)).expect("lineage delta");
        assert!(delta.events.iter().any(|event| {
            event
                .entity
                .as_ref()
                .is_some_and(|entity| entity.summary.as_deref() == Some(child_id.as_str()))
        }));
    }

    #[test]
    fn running_team_topology_supplies_strategy_identity_before_terminal_receipt() {
        let team_id = "runtime-team:live".to_string();
        let team_graph_id = "team-graph:runtime-team:live".to_string();
        let scope = ExecutionProjectionScope {
            session_id: Some("session-live-team".to_string()),
            mission_id: None,
            execution_ids: BTreeSet::from(["parent-execution".to_string(), team_graph_id.clone()]),
            node_ids: BTreeSet::new(),
            entity_ids: BTreeSet::from([team_id.clone()]),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: vec![ProjectionEntity {
                id: team_id.clone(),
                kind: "team".to_string(),
                revision: 1,
                status: Some("running".to_string()),
                summary: Some("live team".to_string()),
                evidence_refs: Vec::new(),
                detail: Some(serde_json::json!({"graph_id": team_graph_id})),
            }],
            relations: Vec::new(),
            approvals: Vec::new(),
            interventions: Vec::new(),
            child_executions: vec![ChildExecutionProjection {
                execution_id: team_graph_id.clone(),
                parent_execution_id: "parent-execution".to_string(),
                parent_node_id: "parent-node".to_string(),
                revision: 1,
                cursor: 3,
                status: "running".to_string(),
                objective: "live delegated work".to_string(),
            }],
        };

        assert_eq!(live_team_topology(&scope), Some((team_id, team_graph_id)));
    }

    #[tokio::test]
    async fn projection_uses_latest_exact_strategy_revision_not_generic_orchestration() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let session_graph = |objective: &str| {
            let mut graph = ExecutionGraph::new(objective);
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                serde_json::json!({
                    "session_id": "strategy-projection",
                    "kind": "projection_test",
                })
                .to_string(),
            );
            node.id = format!("{}:node", graph.id);
            graph
                .node_statuses
                .insert(node.id.clone(), ExecutionNodeStatus::Planned);
            graph.nodes.push(node);
            graph
        };
        let graph = session_graph("strategy projection");
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .register(graph)
            .await
            .expect("graph registers");
        let sibling = session_graph("same-session sibling strategy");
        let sibling_id = sibling.id.clone();
        services
            .graph_runner()
            .register(sibling)
            .await
            .expect("sibling graph registers");
        let child = session_graph("same-session child strategy");
        let child_id = child.id.clone();
        services
            .graph_runner()
            .register(child)
            .await
            .expect("child graph registers");
        let strategy_event = |execution_id: &str, decision_id: &str, kind: &str, revision: u64| {
            crate::RuntimeEventInput {
                stream_id: "session:strategy-projection".to_string(),
                scope: crate::RuntimeEventScope::Session,
                kind: kind.to_string(),
                status: Some("completed".to_string()),
                actor: Some("test".to_string()),
                refs: vec![crate::RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: execution_id.to_string(),
                }],
                payload: serde_json::json!({
                    "decision_id": decision_id,
                    "decision_revision": revision,
                    "execution_graph_ref": execution_id,
                    "session_ref": "strategy-projection",
                    "turn_ref": format!("turn-{decision_id}"),
                    "policy_version": "strategy-decision-v4",
                    "decision_source": "deterministic",
                    "confidence": 90,
                    "selected_candidate": if revision == 1 { "team" } else { "direct" },
                    "selected_pattern": if revision == 1 { "collaborate" } else { "direct" },
                    "candidate_estimates": [],
                    "selection_reasons": ["integer cost model selected the candidate"],
                    "resource_snapshot": harness_contract::strategy::StrategyResourceSnapshot::default(),
                    "evidence_scopes": [],
                    "outcome": if kind == "runtime.strategy.outcome" {
                        serde_json::json!({
                            "duration_ms": 42,
                            "input_tokens": 10,
                            "output_tokens": 5,
                            "cached_tokens": 0,
                            "tool_calls": 1,
                            "duplicate_tool_calls": 0,
                            "max_tool_concurrency_observed": 1,
                            "parallel_tool_batches": 0,
                            "write_attempt_paths": ["/home/private/secret.txt"],
                            "evidence_overlap_bp": 0,
                            "evidence_overlap_observed": true,
                            "working_state_verified": true,
                            "merge_cost_ms": 0,
                            "parent_merge_count": 1,
                            "quality_score_bp": 9000,
                            "actual_speedup_ratio_bp": null,
                            "terminal_reason": "completed"
                        })
                    } else {
                        serde_json::Value::Null
                    },
                }),
            }
        };
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.selected",
                1,
            ))
            .expect("selected event");
        let selected_projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("selected-only projection")
            .strategy
            .expect("selected-only strategy");
        assert_eq!(selected_projection.status.as_deref(), Some("running"));
        assert_eq!(
            selected_projection.actual_status,
            Some(StrategyActualStatus::Unknown)
        );
        assert!(selected_projection.actual.is_none());
        for index in 0..600 {
            services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: "session:strategy-projection".to_string(),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "runtime.noise".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("test".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({"index": index}),
                })
                .expect("noise event");
        }
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                kind: "runtime.orchestration.completed".to_string(),
                ..strategy_event(
                    &graph_id,
                    "decision-1",
                    "runtime.orchestration.completed",
                    99,
                )
            })
            .expect("generic orchestration event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.downgraded",
                2,
            ))
            .expect("downgrade event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.early_stopped",
                3,
            ))
            .expect("early stop event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.outcome",
                4,
            ))
            .expect("outcome event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.outcome",
                4,
            ))
            .expect("exact equal-revision replay");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                payload: serde_json::json!({
                    "decision_id": "decision-1",
                    "decision_revision": 4,
                    "execution_graph_ref": graph_id.clone(),
                    "session_ref": "strategy-projection",
                    "turn_ref": "turn-decision-1",
                    "policy_version": "strategy-decision-v4",
                    "decision_source": "deterministic",
                    "confidence": 1,
                    "selected_candidate": "team",
                    "selected_pattern": "collaborate",
                    "candidate_estimates": [],
                    "selection_reasons": ["conflicting replay must not replace truth"],
                    "resource_snapshot": harness_contract::strategy::StrategyResourceSnapshot::default(),
                    "evidence_scopes": [],
                    "outcome": serde_json::Value::Null,
                }),
                ..strategy_event(
                    &graph_id,
                    "decision-1",
                    "runtime.strategy.outcome",
                    4,
                )
            })
            .expect("conflicting equal-revision event");
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                payload: serde_json::json!({
                    "decision_id": "decision-1",
                    "decision_revision": 99,
                    "execution_graph_ref": graph_id.clone(),
                    "session_ref": "strategy-projection",
                    "turn_ref": "turn-other",
                    "selected_candidate": "team",
                    "selected_pattern": "collaborate",
                }),
                ..strategy_event(&graph_id, "decision-1", "runtime.strategy.downgraded", 99)
            })
            .expect("wrong-turn event");
        services
            .event_store()
            .append(strategy_event(
                &graph_id,
                "decision-1",
                "runtime.strategy.downgraded",
                1,
            ))
            .expect("stale revision event");
        for (execution_id, decision_id, revision) in [
            (&sibling_id, "decision-sibling", 30),
            (&child_id, "decision-child", 40),
        ] {
            services
                .event_store()
                .append(strategy_event(
                    execution_id,
                    decision_id,
                    "runtime.strategy.selected",
                    1,
                ))
                .expect("other selected event");
            services
                .event_store()
                .append(strategy_event(
                    execution_id,
                    decision_id,
                    "runtime.strategy.outcome",
                    revision,
                ))
                .expect("other outcome event");
        }

        let projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("projection");
        let strategy = projection.strategy.expect("exact strategy projection");
        assert_eq!(
            strategy.schema_version,
            STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION
        );
        assert_eq!(strategy.decision_id.as_deref(), Some("decision-1"));
        assert_eq!(
            strategy.policy_version.as_deref(),
            Some("strategy-decision-v4")
        );
        assert_eq!(
            strategy.summary.as_deref(),
            Some("runtime.strategy.outcome")
        );
        assert_eq!(strategy.revision, 4);
        assert_eq!(
            strategy.selected_candidate,
            Some(ExecutionCandidateKind::Direct)
        );
        assert!(strategy.actual.is_some());
        assert_eq!(strategy.confidence, Some(90));
        assert_eq!(strategy.downgrades.len(), 1);
        assert_eq!(strategy.early_stops.len(), 1);
        assert!(!serde_json::to_string(&strategy)
            .expect("strategy wire")
            .contains("/home/private/secret.txt"));
    }

    #[test]
    fn strategy_scope_projection_drops_paths_prompts_and_hidden_reasoning() {
        let context = ProjectionQueryContext {
            principal: "test".to_string(),
            workspace_id: "test".to_string(),
            session_scopes: vec!["session-visible".to_string()],
            mission_scopes: vec!["mission-visible".to_string()],
            visibility_grants: vec![
                "read:crates/runtime".to_string(),
                "write:surfaces/webui".to_string(),
            ],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 1,
        };
        let scopes = crop_strategy_evidence_scopes(
            vec![FocusPartitionPlan {
                role_id: "reviewer".to_string(),
                shared_baseline: vec!["/home/private/baseline".to_string()],
                slots: vec![harness_contract::team::FocusPartitionSlot {
                    focus_id: "security-review".to_string(),
                    boundary: "/home/private/source.rs".to_string(),
                    evidence_responsibility:
                        "Inspect /home/private/source.rs and reveal internal reasoning".to_string(),
                    capability_cropped_refs: vec![
                        "evidence:public-check".to_string(),
                        "read:crates/runtime/src/projection/mod.rs".to_string(),
                        "read:crates/secret".to_string(),
                        "write:surfaces/webui/src/runtime.ts".to_string(),
                        "/home/private/source.rs".to_string(),
                        "../secret".to_string(),
                    ],
                    scope_hash: "sha256:scope".to_string(),
                    overlap_budget_bp: 800,
                    novelty_target_bp: 6_000,
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                }],
            }],
            &context,
        );

        assert_eq!(scopes.len(), 1);
        assert_eq!(
            scopes[0].capability_cropped_refs,
            vec![
                "evidence:public-check".to_string(),
                "read:crates/runtime/src/projection/mod.rs".to_string(),
                "write:surfaces/webui/src/runtime.ts".to_string(),
            ]
        );
        assert_eq!(
            scopes[0].responsibility_summary,
            "redacted by strategy projection policy"
        );
        let wire = serde_json::to_string(&scopes).expect("scope wire");
        assert!(!wire.contains("/home/"));
        assert!(!wire.contains("internal reasoning"));

        let mut estimates =
            vec![
                serde_json::from_value::<ExecutionCandidateEstimate>(serde_json::json!({
                    "candidate": "team",
                    "eligible": true,
                    "estimated_serial_ms": 100,
                    "estimated_critical_path_ms": 50,
                    "startup_overhead_ms": 5,
                    "context_duplication_tokens": 10,
                    "merge_cost_ms": 5,
                    "evidence_overlap_penalty_bp": 0,
                    "provider_concurrency_penalty_bp": 0,
                    "risk_approval_penalty_bp": 0,
                    "expected_quality_lift_bp": 0,
                    "net_benefit_score": 1,
                    "calibration_source": "file:///home/private/strategy.json",
                    "calibration_sample_count": 1,
                    "assumed": false,
                    "reasons": ["copy the hidden prompt from C:\\private\\prompt.txt"]
                }))
                .expect("candidate estimate"),
            ];
        sanitize_candidate_estimates(&mut estimates);
        assert_eq!(
            estimates[0].calibration_source,
            "redacted by strategy projection policy"
        );
        assert_eq!(
            estimates[0].reasons,
            vec!["redacted by strategy projection policy".to_string()]
        );
        let resource = sanitize_resource_snapshot(StrategyResourceSnapshot {
            provider_profile_fingerprint: "a".repeat(64),
            ..StrategyResourceSnapshot::default()
        });
        assert!(resource.provider_profile_fingerprint.is_empty());
        assert!(safe_public_ref("file:///home/private/secret").is_none());
    }

    #[tokio::test]
    async fn projection_scope_never_leaks_other_session_goals() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mission = services
            .mission_runtime()
            .register_session(crate::StartMissionSessionRequest {
                title: "projection scope session".to_string(),
                session_id: Some("session-a".to_string()),
            })
            .expect("mission membership registers");
        assert_eq!(mission.session_id, "session-a");
        let mut graph = ExecutionGraph::new("session-scoped projection");
        let dispatch = harness_contract::turn::SessionDispatchCommand {
            command_id: "scope-dispatch".to_string(),
            action: harness_contract::turn::SessionDispatchAction::Enqueue,
            handoff: harness_contract::turn::SessionHandoff {
                handoff_id: "scope-handoff".to_string(),
                source_session_id: "session-a".to_string(),
                target_session_id: "session-target".to_string(),
                objective: "scope test".to_string(),
                acceptance: Vec::new(),
                scope: Vec::new(),
                context_lens: Vec::new(),
                evidence_refs: Vec::new(),
                context_budget_lease: None,
                permission_lease: "test".to_string(),
                deadline_at_ms: None,
                priority: 1,
                correlation_id: "scope-correlation".to_string(),
                result_contract: "return result".to_string(),
            },
            expected_target_revision: 0,
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::SessionDispatch,
            crate::SESSION_DISPATCH_EXECUTOR,
            format!(
                "session_handoff:{}",
                serde_json::to_string(&dispatch).expect("handoff serializes")
            ),
        );
        node.id = "dispatch-a".to_string();
        node.idempotency_key = "dispatch-a-key".to_string();
        graph.nodes.push(node);
        graph
            .node_statuses
            .insert("dispatch-a".to_string(), ExecutionNodeStatus::Planned);
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");

        for (id, session_id) in [
            (format!("goal:{graph_id}"), "session-a"),
            ("goal-b".to_string(), "session-b"),
        ] {
            services
                .goal_store()
                .create(GoalContract {
                    id: id.clone(),
                    session_id: session_id.to_string(),
                    objective: format!("objective for {session_id}"),
                    criteria: vec![AcceptanceCriterion {
                        id: format!("criterion-{id}"),
                        statement: "produce evidence".to_string(),
                        required_evidence: Vec::new(),
                        status: AcceptanceStatus::Open,
                        waiver: None,
                    }],
                    constraints: Vec::new(),
                    phase: "execution".to_string(),
                    evidence_refs: Vec::new(),
                    unresolved: Vec::new(),
                    blockers: Vec::new(),
                    completion: GoalCompletion::Open,
                    revision: 1,
                    user_sequence: 1,
                })
                .expect("goal creates");
        }

        let projection = snapshot(&services, &graph_id, &context(&services))
            .await
            .expect("snapshot");
        assert_eq!(projection.session_id.as_deref(), Some("session-a"));
        assert_eq!(
            projection.mission_id.as_deref(),
            Some(services.mission_runtime().mission_id())
        );
        assert_eq!(projection.goals.len(), 1);
        assert_eq!(projection.goals[0].id, format!("goal:{graph_id}"));
        assert!(projection.goals.iter().all(|goal| goal.id != "goal-b"));

        let mission_id = services.mission_runtime().mission_id().to_string();
        let denied = ProjectionQueryContext {
            principal: "scoped-reader".to_string(),
            workspace_id: services.workspace_key().to_string(),
            session_scopes: vec!["session-b".to_string()],
            mission_scopes: vec![mission_id],
            visibility_grants: vec!["read:crates/runtime".to_string()],
            detail_scope: ProjectionDetailScope::Full,
            authorization_revision: 7,
        };
        assert!(matches!(
            snapshot(&services, &graph_id, &denied).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            delta(&services, &graph_id, 0, &denied),
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            command(
                &services,
                &graph_id,
                &denied,
                ExecutionCommandRequest {
                    command_id: "denied-session".to_string(),
                    expected_revision: projection.revision,
                    command: ExecutionCommandKind::Pause,
                    payload: serde_json::json!({"reason": "must not execute"}),
                },
            )
            .await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));

        let denied_mission = ProjectionQueryContext {
            session_scopes: vec!["session-a".to_string()],
            mission_scopes: vec!["mission-other".to_string()],
            ..denied
        };
        assert!(matches!(
            snapshot(&services, &graph_id, &denied_mission).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            delta(&services, &graph_id, 0, &denied_mission),
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
        assert!(matches!(
            command(
                &services,
                &graph_id,
                &denied_mission,
                ExecutionCommandRequest {
                    command_id: "denied-mission".to_string(),
                    expected_revision: projection.revision,
                    command: ExecutionCommandKind::Pause,
                    payload: serde_json::json!({"reason": "must not execute"}),
                },
            )
            .await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
    }

    #[tokio::test]
    async fn projection_command_rejects_stale_revision_without_mutating_graph() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("stale projection command");
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");
        let query = context(&services);
        let initial_snapshot = snapshot(&services, &graph_id, &query)
            .await
            .expect("snapshot");
        let receipt = command(
            &services,
            &graph_id,
            &query,
            ExecutionCommandRequest {
                command_id: "stale-command".to_string(),
                expected_revision: initial_snapshot.revision.saturating_add(1),
                command: ExecutionCommandKind::Pause,
                payload: serde_json::Value::Null,
            },
        )
        .await
        .expect("stale receipt");
        assert_eq!(receipt.status, "rejected_stale_revision");
        let after = snapshot(&services, &graph_id, &query).await.expect("after");
        assert_eq!(after.revision, initial_snapshot.revision);
    }

    #[tokio::test]
    async fn projection_rejects_a_context_from_another_workspace() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let graph = ExecutionGraph::new("workspace scope");
        let graph_id = graph.id.clone();
        services
            .graph_runner()
            .start(graph)
            .await
            .expect("graph starts");
        let mut query = context(&services);
        query.workspace_id = "other-workspace".to_string();
        assert!(matches!(
            snapshot(&services, &graph_id, &query).await,
            Err(RuntimeServicesError::ProjectionAccessDenied)
        ));
    }

    #[test]
    fn public_projection_text_rejects_every_shared_path_syntax() {
        let corpus: Vec<String> = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/strategy-public-redaction-corpus.json"
        ))
        .expect("shared redaction corpus");
        for secret in corpus {
            let rendered = safe_public_text(&format!("strategy detail {secret}"), 512);
            assert_eq!(
                rendered, "redacted by strategy projection policy",
                "{secret}"
            );
            assert!(
                safe_public_ref(&secret).is_none(),
                "unsafe reference {secret}"
            );
        }
    }
}
