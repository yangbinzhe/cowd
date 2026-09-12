//! Read the existing Runtime strategy and tool journal; do not invent policy.
use crate::agentic::program::AgenticProgramProjection;
use crate::{ExpectedStreamRevision, RuntimeEventStore};
use harness_contract::core::{ExecutionModifier, ExecutionPolicyGate, TaskRisk};
use harness_contract::goal::GoalContract;
use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(crate) struct RootReviewPolicySnapshot {
    pub digest: String,
    pub source: ExpectedStreamRevision,
    pub last_effect_cursor: u64,
    pub additional_sources: Vec<ExpectedStreamRevision>,
}

pub(super) fn goal_metadata_tool(name: &str) -> bool {
    matches!(
        name,
        "objective_update"
            | "objective_review"
            | "objective_complete_request"
            | "state_inspect"
            | "artifact_commit"
            | "artifact_publish"
            | "working_context"
    )
}

fn permits_low_risk_self_review(effect: &ToolEffectDescriptor) -> bool {
    use harness_contract::policy::*;
    use harness_contract::tool::*;
    if effect.effect_kind == ToolEffectKind::Read {
        return true;
    }
    effect.effect_kind == ToolEffectKind::Write
        && effect.required_permission == ToolPermissionMode::WorkspaceWrite
        && matches!(
            effect.idempotency,
            ToolIdempotency::Idempotent | ToolIdempotency::IdempotentWithKey
        )
        && matches!(
            effect.approval_class,
            ToolApprovalClass::None | ToolApprovalClass::Policy
        )
        && !effect.uses_network
        && !effect.spawns_process
        && !effect.mutates_packages
        && !effect.mutates_system
        && matches!(
            effect.assessment.reversibility,
            EffectReversibility::Reversible | EffectReversibility::Compensatable
        )
        && matches!(
            effect.assessment.externality,
            EffectExternality::Internal | EffectExternality::Workspace
        )
        && matches!(
            effect.assessment.data_sensitivity,
            DataClassification::Public | DataClassification::Internal
        )
        && matches!(
            effect.assessment.novelty,
            EffectNovelty::Routine | EffectNovelty::NewTarget
        )
        && matches!(
            effect.assessment.blast_radius,
            EffectBlastRadius::Item | EffectBlastRadius::Workspace
        )
        && !effect.scopes.is_empty()
        && effect.scopes.iter().all(|scope| {
            scope.resource == PermissionResource::File
                && matches!(
                    scope.operation,
                    PermissionOperation::Read | PermissionOperation::Write
                )
                && scope.target.as_deref().is_some_and(|target| {
                    !target.is_empty()
                        && !target.starts_with('~')
                        && !target.contains(['*', '?', '[', ']'])
                        && std::path::Path::new(target)
                            .components()
                            .all(|component| matches!(component, std::path::Component::Normal(_)))
                })
        })
}

pub(crate) fn root_self_review_policy(
    store: &RuntimeEventStore,
    program: &AgenticProgramProjection,
    goal: &GoalContract,
) -> Result<Option<RootReviewPolicySnapshot>, String> {
    if !program.tasks.is_empty()
        || !program.agents.is_empty()
        || !program.teams.is_empty()
        || program.required_team_count != 0
        || goal.participation_requirement.is_some()
        || goal.obligations.iter().any(|obligation| {
            obligation
                .evidence_requirement
                .independent_verifier_required
        })
    {
        return Ok(None);
    }
    let root = program
        .root_execution_id
        .as_deref()
        .ok_or("review policy requires root identity")?;
    let stream_id = format!("session:{}", program.session_id);
    let revision = store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?;
    let goal_events =
        store.list_stream_after(&format!("goal:{}", goal.id), 0, 1, 1, 4 * 1024 * 1024)?;
    let Some(created) = goal_events.first() else {
        return Ok(None);
    };
    let mut offset = 0;
    let mut decision = None;
    let mut root_plans = BTreeSet::new();
    let mut safe_plans = BTreeSet::new();
    let mut unsafe_plans = BTreeSet::new();
    let mut unsafe_call = false;
    loop {
        let events = store.list_stream_page_desc(&stream_id, 64, offset)?;
        if events.is_empty() {
            break;
        }
        offset += events.len();
        let mut before_goal = false;
        for event in events {
            if event.sequence > revision {
                continue;
            }
            before_goal |= event.commit_cursor < created.commit_cursor;
            if event.kind.starts_with("runtime.strategy.")
                && event.actor.as_deref() == Some("conversation_runtime.strategy_owner")
                && event.payload["execution_graph_ref"].as_str() == Some(root)
                && event.payload["session_ref"].as_str() == Some(program.session_id.as_str())
                && event.payload["turn_ref"].as_str() == Some(program.turn_id.as_str())
                && decision.is_none()
            {
                decision = Some(event.payload.clone());
            }
            if event.kind == "tool.execution_plan.created"
                && event.actor.as_deref() == Some("conversation_runtime")
            {
                if let (Some(id), Some(tasks)) = (
                    event.payload["plan_id"].as_str(),
                    event.payload["tasks"].as_array(),
                ) {
                    let safe = tasks.iter().all(|task| {
                        task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                            || serde_json::from_value::<ToolEffectDescriptor>(
                                task["effect"].clone(),
                            )
                            .is_ok_and(|effect| permits_low_risk_self_review(&effect))
                    });
                    if safe {
                        safe_plans.insert(id.to_string());
                    } else {
                        unsafe_plans.insert(id.to_string());
                    }
                    if event
                        .refs
                        .iter()
                        .any(|reference| reference.kind == "execution" && reference.id == root)
                    {
                        root_plans.insert(id.to_string());
                    }
                }
            }
            if event.kind.starts_with("tool.invocation.")
                && event.activity_binding().is_some_and(|binding| {
                    binding.root_execution_id == root
                        && binding.session_id == program.session_id
                        && binding.turn_id == program.turn_id
                        && binding.agent_run_id.is_none()
                })
            {
                let name = event.payload["tool_name"].as_str().unwrap_or_default();
                if goal_metadata_tool(name) {
                    continue;
                }
                if let Some(plan) = event.payload["governed_plan_id"].as_str() {
                    root_plans.insert(plan.to_string());
                } else {
                    // Old unclassified effects cannot authorize self review.
                    unsafe_call = true;
                }
            }
        }
        if before_goal && decision.is_some() {
            break;
        }
    }
    if store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?
        != revision
    {
        return Err(
            "review policy source changed; retry against current strategy and tool plans".into(),
        );
    }
    let Some(decision) = decision else {
        return Ok(None);
    };
    let risk = serde_json::from_value::<TaskRisk>(decision["risk"].clone()).ok();
    let modifiers =
        serde_json::from_value::<Vec<ExecutionModifier>>(decision["modifiers"].clone()).ok();
    let gates = serde_json::from_value::<Vec<ExecutionPolicyGate>>(decision["gates"].clone()).ok();
    if risk != Some(TaskRisk::Low)
        || modifiers.as_ref().is_none_or(|values| {
            values.iter().any(|value| {
                matches!(
                    value,
                    ExecutionModifier::WithVerifier | ExecutionModifier::WithReviewer
                )
            })
        })
        || gates
            .as_ref()
            .is_none_or(|values| values.contains(&ExecutionPolicyGate::Approval))
        || unsafe_call
        || root_plans
            .iter()
            .any(|plan| !safe_plans.contains(plan) || unsafe_plans.contains(plan))
    {
        return Ok(None);
    }
    let value = serde_json::json!({"review_policy_version":2,"root":root,"session":program.session_id,"turn":program.turn_id,
        "decision_id":decision["decision_id"],"risk":risk,"modifiers":modifiers,"gates":gates});
    Ok(Some(RootReviewPolicySnapshot {
        digest: format!("{:x}", Sha256::digest(value.to_string().as_bytes())),
        last_effect_cursor: 0,
        additional_sources: Vec::new(),
        source: ExpectedStreamRevision {
            stream_id,
            expected_revision: revision,
        },
    }))
}

/// The existing Session journal owns admitted writes and their outcomes. Reads
/// and Goal metadata may advance its revision without changing this digest.
pub(crate) fn effect_review_snapshot(
    store: &RuntimeEventStore,
    program: &AgenticProgramProjection,
    goal: Option<&GoalContract>,
    effect_sources: &[String],
) -> Result<RootReviewPolicySnapshot, String> {
    let stream_id = format!("session:{}", program.session_id);
    let revision = store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?;
    let created_cursor = if let Some(goal) = goal {
        store
            .list_stream_after(&format!("goal:{}", goal.id), 0, 1, 1, 4 * 1024 * 1024)?
            .into_iter()
            .next()
            .ok_or("effect review has no durable Goal genesis")?
            .commit_cursor
    } else {
        0
    };
    let mut digest = Sha256::new();
    digest.update(
        goal.map_or(program.objective_id.as_str(), |goal| goal.id.as_str())
            .as_bytes(),
    );
    let mut offset = 0;
    let mut last_effect_cursor = 0;
    let mut planned_effects = BTreeSet::new();
    let mut terminal_calls = BTreeSet::new();
    loop {
        let events = store.list_stream_page_desc(&stream_id, 64, offset)?;
        if events.is_empty() {
            break;
        }
        offset += events.len();
        let mut before_goal = false;
        for event in events {
            if event.sequence > revision {
                continue;
            }
            if event.commit_cursor < created_cursor {
                before_goal = true;
                continue;
            }
            if event.actor.as_deref() != Some("conversation_runtime") {
                continue;
            }
            if matches!(
                event.kind.as_str(),
                "tool.invocation.completed" | "tool.invocation.failed" | "tool.invocation.denied"
            ) {
                if let (Some(plan), Some(call)) = (
                    event.payload["governed_plan_id"].as_str(),
                    event.payload["tool_call_id"].as_str(),
                ) {
                    terminal_calls.insert((plan.to_string(), call.to_string()));
                }
            }
            if event.kind == "tool.execution_plan.created" {
                if let (Some(plan), Some(tasks)) = (
                    event.payload["plan_id"].as_str(),
                    event.payload["tasks"].as_array(),
                ) {
                    for task in tasks {
                        if !task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                            && serde_json::from_value::<ToolEffectKind>(
                                task["effect"]["effect_kind"].clone(),
                            )
                            .ok()
                                != Some(ToolEffectKind::Read)
                        {
                            if let Some(call) = task["tool_call_id"].as_str() {
                                planned_effects.insert((plan.to_string(), call.to_string()));
                            }
                        }
                    }
                }
            }
            let relevant = if event.kind == "tool.execution_plan.created" {
                event.payload["tasks"].as_array().is_none_or(|tasks| {
                    tasks.iter().any(|task| {
                        !task["tool_name"].as_str().is_some_and(goal_metadata_tool)
                            && serde_json::from_value::<ToolEffectKind>(
                                task["effect"]["effect_kind"].clone(),
                            )
                            .ok()
                                != Some(ToolEffectKind::Read)
                    })
                })
            } else if event.kind.starts_with("tool.invocation.") {
                !event.payload["tool_name"]
                    .as_str()
                    .is_some_and(goal_metadata_tool)
                    && !super::results::tool_effect_from_store(store, &event)?
                        .is_some_and(|effect| effect.effect_kind == ToolEffectKind::Read)
            } else {
                false
            };
            if relevant {
                last_effect_cursor = last_effect_cursor.max(event.commit_cursor);
                digest.update(event.event_id.as_bytes());
                digest.update(event.payload.to_string().as_bytes());
            }
        }
        if before_goal {
            break;
        }
    }
    if store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?
        != revision
    {
        return Err("effect review source changed; retry current effects".into());
    }
    if !planned_effects.is_subset(&terminal_calls) {
        return Err(
            "effect_review_pending: admitted writes must settle before current target verification"
                .into(),
        );
    }
    let mut additional_sources = Vec::new();
    for source in effect_sources.iter().collect::<BTreeSet<_>>() {
        if !source.starts_with("execution-agent-receipts:") {
            return Err("effect review source is not a canonical Agent receipt stream".into());
        }
        let revision = store
            .stream_revision(source)
            .map_err(|error| error.to_string())?;
        let mut pending = BTreeSet::new();
        let mut completed = BTreeSet::new();
        digest.update(source.as_bytes());
        let mut offset = 0;
        loop {
            let events = store.list_stream_page_desc(source, 64, offset)?;
            if events.is_empty() {
                break;
            }
            offset += events.len();
            for event in events {
                if event.sequence > revision
                    || event.actor.as_deref() != Some("governed_tool")
                    || !matches!(
                        event.kind.as_str(),
                        "execution.agent_tool.intent" | "execution.agent_tool.receipt"
                    )
                    || event.payload["tool_name"]
                        .as_str()
                        .or_else(|| event.payload["outcome"]["tool_name"].as_str())
                        .is_some_and(goal_metadata_tool)
                    || serde_json::from_value::<ToolEffectKind>(
                        event.payload["effect_kind"].clone(),
                    )
                    .ok()
                        == Some(ToolEffectKind::Read)
                {
                    continue;
                }
                if let Some(key) = event.payload["idempotency_key"].as_str() {
                    if event.kind == "execution.agent_tool.intent" {
                        pending.insert(key.to_owned());
                    } else {
                        completed.insert(key.to_owned());
                    }
                } else if event.kind == "execution.agent_tool.intent" {
                    return Err("effect review intent has no canonical effect identity".into());
                }
                last_effect_cursor = last_effect_cursor.max(event.commit_cursor);
                digest.update(event.event_id.as_bytes());
                digest.update(event.payload.to_string().as_bytes());
            }
        }
        if store
            .stream_revision(source)
            .map_err(|error| error.to_string())?
            != revision
        {
            return Err("delegated effect source changed; retry current effects".into());
        }
        if !pending.is_subset(&completed) {
            return Err("effect_review_pending: delegated writes must settle before current target verification".into());
        }
        additional_sources.push(ExpectedStreamRevision {
            stream_id: source.clone(),
            expected_revision: revision,
        });
    }
    Ok(RootReviewPolicySnapshot {
        additional_sources,
        digest: format!("{:x}", digest.finalize()),
        last_effect_cursor,
        source: ExpectedStreamRevision {
            stream_id,
            expected_revision: revision,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{policy::*, tool::*};

    #[test]
    fn low_risk_effect_policy_rejects_unbounded_external_unknown_or_independently_gated_writes() {
        let effect = ToolEffectDescriptor {
            tool_id: "write_file".into(),
            descriptor_hash: "registered-file-effect".into(),
            effect_kind: ToolEffectKind::Write,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![PermissionScope {
                resource: PermissionResource::File,
                operation: PermissionOperation::Write,
                target: Some("notes/result.txt".into()),
            }],
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: EffectAssessment {
                reversibility: EffectReversibility::Compensatable,
                externality: EffectExternality::Workspace,
                data_sensitivity: DataClassification::Internal,
                novelty: EffectNovelty::Routine,
                blast_radius: EffectBlastRadius::Workspace,
            },
        };
        assert!(permits_low_risk_self_review(&effect));
        for kind in [
            ToolEffectKind::Network,
            ToolEffectKind::Process,
            ToolEffectKind::Package,
            ToolEffectKind::System,
            ToolEffectKind::Destructive,
            ToolEffectKind::Unknown,
        ] {
            assert!(!permits_low_risk_self_review(&ToolEffectDescriptor {
                effect_kind: kind,
                ..effect.clone()
            }));
        }
        for target in [
            "",
            ".",
            "../outside",
            "/etc/config",
            "~/config",
            "workspace/*",
            "a/../b",
            "files/?.txt",
        ] {
            let mut candidate = effect.clone();
            candidate.scopes[0].target = Some(target.into());
            assert!(!permits_low_risk_self_review(&candidate), "{target}");
        }
        let candidates = [
            ToolEffectDescriptor {
                scopes: vec![],
                ..effect.clone()
            },
            ToolEffectDescriptor {
                uses_network: true,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                spawns_process: true,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                mutates_packages: true,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                mutates_system: true,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                idempotency: ToolIdempotency::NonIdempotent,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                idempotency: ToolIdempotency::Unknown,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                approval_class: ToolApprovalClass::User,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                approval_class: ToolApprovalClass::Administrator,
                ..effect.clone()
            },
            ToolEffectDescriptor {
                assessment: EffectAssessment::default(),
                ..effect.clone()
            },
        ];
        for candidate in candidates {
            assert!(!permits_low_risk_self_review(&candidate));
        }
        for assessment in [
            EffectAssessment {
                reversibility: EffectReversibility::Irreversible,
                ..effect.assessment.clone()
            },
            EffectAssessment {
                externality: EffectExternality::ExternalMutation,
                ..effect.assessment.clone()
            },
            EffectAssessment {
                data_sensitivity: DataClassification::Secret,
                ..effect.assessment.clone()
            },
            EffectAssessment {
                novelty: EffectNovelty::NewCapability,
                ..effect.assessment.clone()
            },
            EffectAssessment {
                blast_radius: EffectBlastRadius::Unbounded,
                ..effect.assessment.clone()
            },
        ] {
            assert!(!permits_low_risk_self_review(&ToolEffectDescriptor {
                assessment,
                ..effect.clone()
            }));
        }
    }
}
