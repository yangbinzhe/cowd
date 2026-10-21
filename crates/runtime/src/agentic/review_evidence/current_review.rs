//! The exact identity of the review control call a proof is built under.
//!
//! A review is performed as a governed tool call, so while the proof is
//! assembled that very call is still in flight: the proof's "every write must
//! be settled" check would trip on the review itself. Excluding it is only
//! safe when it names ONE exact call.
//!
//! The identity is resolved from evidence the proof owner ALREADY pinned:
//! the Session's governed tool plans and the delegated receipt index. No
//! history scan, no second read set, and the same source CAS as the rest of
//! the proof.
//!
//! The context travels with the live action, never inside the persisted
//! proof: a proof is a record, and a record must not be able to grant an
//! exemption. Every entry that rebuilds, submits or re-verifies a proof
//! re-derives the identity from the action it is actually applying, and an
//! entry without that context exempts nothing.

use harness_contract::agent_action::{
    AgentAction, AgentActorBinding, AgentActorKind, OBJECTIVE_REVIEW_TOOL_ID, TASK_REVIEW_TOOL_ID,
};
use serde_json::Value;

/// The review action that is being applied, with the durable invocation id
/// the caller presented for it and the actor it was authenticated for.
pub(crate) struct CurrentReviewContext<'a> {
    pub action_id: &'a str,
    pub action: &'a AgentAction,
    pub actor: &'a AgentActorBinding,
}

fn review_control_tool(action: &AgentAction) -> Option<&'static str> {
    match action {
        AgentAction::TaskReview(_) => Some(TASK_REVIEW_TOOL_ID),
        AgentAction::ObjectiveReview(_) => Some(OBJECTIVE_REVIEW_TOOL_ID),
        _ => None,
    }
}

/// Resolve the ONE review control call that is currently executing.
///
/// Returns `None` when the action is not a review, when the reviewer is not a
/// delegated Agent (a root review is a goal-metadata control action and is
/// already outside the business write set), when no pinned plan task carries
/// the presented invocation id, or when the candidate does not authenticate
/// against the reviewer and the action. An ambiguous resolution is an error:
/// two different calls could both claim the exclusion.
pub(crate) fn resolve_current_review_effect_key(
    session_events: &[crate::DurableRuntimeEvent],
    reviewer_execution_id: &str,
    context: &CurrentReviewContext<'_>,
) -> Result<Option<String>, String> {
    let Some(expected_tool) = review_control_tool(context.action) else {
        return Ok(None);
    };
    // Only a delegated reviewer's own call can be in flight while the proof
    // is assembled. A root review is settled through the Goal lane, so it has
    // no tool effect to exempt and no run to bind the call to.
    let Some(reviewer_run) = reviewer_execution_id.strip_prefix("agent-run:") else {
        return Ok(None);
    };
    if reviewer_run.is_empty() || context.actor.kind != AgentActorKind::Agent {
        return Ok(None);
    }
    let mut resolved: Vec<String> = Vec::new();
    for event in session_events {
        if event.kind != "tool.execution_plan.created"
            || event.actor.as_deref() != Some("conversation_runtime")
            || event.scope != crate::RuntimeEventScope::Tool
        {
            continue;
        }
        let Some(tasks) = event.payload["tasks"].as_array() else {
            continue;
        };
        for task in tasks {
            let Ok(step) = serde_json::from_value::<crate::step_snapshot::StepCallBinding>(
                task["step_binding"].clone(),
            ) else {
                continue;
            };
            let effect_key = step.effect_idempotency_key();
            // A direct call presents its own effect identity.
            if effect_key == context.action_id {
                if task["tool_name"].as_str() == Some(expected_tool)
                    && step_belongs_to_reviewer(&step, reviewer_run, context.actor)
                    && action_matches(&task["normalized_input"], context.action)
                {
                    resolved.push(effect_key);
                }
                continue;
            }
            // A batch child presents the parent's identity plus its own index,
            // exactly as the producer builds it. The in-flight effect is the
            // batch call itself, so that is what would be excluded - and only
            // when the batch carries that one action. A batch with siblings
            // would exempt their effects too, so it is not exempt at all and
            // the siblings keep blocking until they really land.
            if task["tool_name"].as_str() != Some("agent_action_batch") {
                continue;
            }
            let Some(actions) = task["normalized_input"]["actions"].as_array() else {
                continue;
            };
            if actions.len() != 1 {
                continue;
            }
            let inner = &actions[0];
            if inner["tool"].as_str() != Some(expected_tool)
                || format!("{effect_key}:0") != context.action_id
                || !step_belongs_to_reviewer(&step, reviewer_run, context.actor)
                || !action_matches(&inner["input"], context.action)
            {
                continue;
            }
            resolved.push(effect_key);
        }
    }
    match resolved.len() {
        0 => Ok(None),
        1 => Ok(Some(resolved.remove(0))),
        _ => Err("current review call is ambiguous".into()),
    }
}

/// The Step binding must belong to the reviewer's own execution: the same
/// session and turn, and the same delegated run. The run binds the agent, the
/// graph and the attempt together, so a second call of the same tool in that
/// run - a different Step, a different attempt - carries a different effect
/// identity and is never excluded.
fn step_belongs_to_reviewer(
    step: &crate::step_snapshot::StepCallBinding,
    reviewer_run: &str,
    actor: &AgentActorBinding,
) -> bool {
    let identity = &step.snapshot.identity;
    identity.session_id == actor.session_id
        && identity.turn_id == actor.turn_id
        && identity.execution_id.as_deref() == Some(reviewer_run)
        && identity.run_ref.as_deref() == Some(reviewer_run)
}

/// The recorded tool input must be the action being submitted: the same
/// review, of the same focus, with the same verdict and the same evidence.
fn action_matches(recorded: &Value, action: &AgentAction) -> bool {
    match action {
        AgentAction::TaskReview(input) => {
            recorded_str(recorded, "task_ref").as_deref() == Some(input.task_ref.as_str())
                && recorded_decision(recorded) == decision_text(&input.decision)
                && recorded_refs(recorded, "evidence_refs") == normalized_refs(&input.evidence_refs)
        }
        AgentAction::ObjectiveReview(input) => {
            recorded_str(recorded, "criterion_ref").as_deref() == Some(input.criterion_ref.as_str())
                && recorded_decision(recorded) == decision_text(&input.decision)
                && recorded_refs(recorded, "result_refs") == normalized_refs(&input.result_refs)
                && recorded_refs(recorded, "evidence_refs") == normalized_refs(&input.evidence_refs)
        }
        _ => false,
    }
}

fn recorded_str(recorded: &Value, field: &str) -> Option<String> {
    recorded
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .map(str::to_string)
}

fn recorded_decision(recorded: &Value) -> Option<String> {
    recorded
        .get("decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .map(str::to_string)
}

fn decision_text<T: serde::Serialize>(decision: &T) -> Option<String> {
    serde_json::to_value(decision)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
}

fn recorded_refs(recorded: &Value, field: &str) -> Vec<String> {
    let mut refs = recorded
        .get(field)
        .and_then(Value::as_array)
        .map(|array| {
            array
                .iter()
                .filter_map(Value::as_str)
                .map(crate::normalize_bare_evidence_ref)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    refs.sort();
    refs
}

fn normalized_refs(refs: &[String]) -> Vec<String> {
    let mut refs = refs
        .iter()
        .map(|reference| crate::normalize_bare_evidence_ref(reference))
        .collect::<Vec<_>>();
    refs.sort();
    refs
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent_action::{
        AgentActorBinding, AgentActorKind, TaskReviewDecision, TaskReviewInput,
    };

    const REVIEWER_RUN: &str = "run:agentic-graph:review";

    fn step_binding(call_id: &str, run: &str, attempt: Option<u32>) -> Value {
        serde_json::json!({
            "origin": {
                "origin_step_id": format!("step-{call_id}"),
                "call_id": call_id,
                "catalog_digest": "catalog",
                "source": {"kind": "provider", "attempt_id": "attempt-1"},
            },
            "snapshot": {
                "contract_version": 1,
                "identity": {
                    "step_id": format!("step-{call_id}"),
                    "session_id": "session",
                    "turn_id": "turn",
                    "execution_id": run,
                    "actor_ref": "runtime.agentic",
                    "run_ref": run,
                    "attempt": attempt,
                    "build_ref": "build",
                },
                "provider_attempt": {
                    "attempt_id": "attempt-1",
                    "origin_step_id": format!("step-{call_id}"),
                    "model_profile_digest": "profile",
                    "request_digest": "request",
                },
                "catalog_digest": "catalog",
                "tools": [],
                "related_revisions": {},
                "context_refs": [],
                "expires_at_ms": 0,
            }
        })
    }

    fn review_task(call_id: &str, task_ref: &str, run: &str) -> Value {
        serde_json::json!({
            "tool_name": "task_review",
            "tool_call_id": call_id,
            "normalized_input": {
                "task_ref": task_ref,
                "decision": "accept",
                "reason": "independently read",
                "evidence_refs": ["artifact://read"],
            },
            "step_binding": step_binding(call_id, run, None),
        })
    }

    fn plan_event(tasks: Value) -> crate::DurableRuntimeEvent {
        crate::DurableRuntimeEvent {
            event_id: "plan-event".into(),
            stream_id: "session:session".into(),
            sequence: 1,
            scope: crate::RuntimeEventScope::Tool,
            kind: "tool.execution_plan.created".into(),
            status: Some("planned".into()),
            actor: Some("conversation_runtime".into()),
            refs: Vec::new(),
            payload: serde_json::json!({"plan_id": "plan", "tasks": tasks}),
            created_at_ms: 0,
            commit_cursor: 1,
            transaction_id: "tx".into(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        }
    }

    fn actor() -> AgentActorBinding {
        AgentActorBinding {
            objective_id: "objective".into(),
            program_id: "program".into(),
            session_id: "session".into(),
            turn_id: "turn".into(),
            root_execution_id: Some("graph".into()),
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: String::new(),
            model_lease: "model".into(),
            permission_ceiling: None,
            resource_scopes: Vec::new(),
            actor_id: "agent".into(),
            kind: AgentActorKind::Agent,
            execution_id: Some("graph".into()),
            team_id: Some("team".into()),
            agent_id: Some("agent".into()),
        }
    }

    fn review_action(task_ref: &str) -> AgentAction {
        AgentAction::TaskReview(TaskReviewInput {
            task_ref: task_ref.into(),
            decision: TaskReviewDecision::Accept,
            reason: "independently read".into(),
            operator_requirement: None,
            evidence_refs: vec!["artifact://read".into()],
        })
    }

    fn effect_key(task: &Value) -> String {
        let step: crate::step_snapshot::StepCallBinding =
            serde_json::from_value(task["step_binding"].clone()).expect("bound step");
        step.effect_idempotency_key()
    }

    fn resolve(
        events: &[crate::DurableRuntimeEvent],
        context: &CurrentReviewContext<'_>,
    ) -> Option<String> {
        resolve_current_review_effect_key(events, &format!("agent-run:{REVIEWER_RUN}"), context)
            .expect("an unambiguous resolution")
    }

    /// Two review calls in the SAME run, with the SAME tool: only the one the
    /// submitted action names may be exempt. The other one is a second
    /// unfinished review and must keep blocking.
    #[test]
    fn r1_second_unfinished_review_in_the_same_run_is_not_exempt() {
        let first = review_task("call-a", "task:one", REVIEWER_RUN);
        let second = review_task("call-b", "task:one", REVIEWER_RUN);
        let events = [plan_event(serde_json::json!([
            first.clone(),
            second.clone()
        ]))];
        let context = CurrentReviewContext {
            action_id: &effect_key(&first),
            action: &review_action("task:one"),
            actor: &actor(),
        };
        let resolved = resolve(&events, &context).expect("the presented call resolves");
        assert_eq!(resolved, effect_key(&first));
        assert_ne!(
            resolved,
            effect_key(&second),
            "the other unfinished review keeps its own identity"
        );
    }

    /// A forged invocation id, an action the call did not perform, a call
    /// from another run, a call with no Step identity, and a review performed
    /// by the root all resolve to nothing: nothing is exempt.
    #[test]
    fn r1_unauthenticated_review_resolves_to_nothing() {
        let task = review_task("call-a", "task:one", REVIEWER_RUN);
        let events = [plan_event(serde_json::json!([task.clone()]))];
        let key = effect_key(&task);
        let action = review_action("task:one");

        let forged = CurrentReviewContext {
            action_id: "forged-invocation-id",
            action: &action,
            actor: &actor(),
        };
        assert!(
            resolve(&events, &forged).is_none(),
            "a forged invocation id names no call"
        );

        let other_action = CurrentReviewContext {
            action_id: &key,
            action: &review_action("task:two"),
            actor: &actor(),
        };
        assert!(
            resolve(&events, &other_action).is_none(),
            "a review of another Task is not this call"
        );

        let other_verdict = CurrentReviewContext {
            action_id: &key,
            action: &AgentAction::TaskReview(TaskReviewInput {
                task_ref: "task:one".into(),
                decision: TaskReviewDecision::Rework,
                reason: "independently read".into(),
                operator_requirement: None,
                evidence_refs: vec!["artifact://read".into()],
            }),
            actor: &actor(),
        };
        assert!(
            resolve(&events, &other_verdict).is_none(),
            "another verdict is another action"
        );

        let foreign = review_task("call-foreign", "task:one", "run:agentic-graph:other");
        let foreign_events = [plan_event(serde_json::json!([foreign.clone()]))];
        let foreign_context = CurrentReviewContext {
            action_id: &effect_key(&foreign),
            action: &action,
            actor: &actor(),
        };
        assert!(
            resolve(&foreign_events, &foreign_context).is_none(),
            "another run is not this call"
        );

        let mut unbound = review_task("call-unbound", "task:one", REVIEWER_RUN);
        unbound["step_binding"] = Value::Null;
        let unbound_events = [plan_event(serde_json::json!([unbound]))];
        let unbound_context = CurrentReviewContext {
            action_id: "unbound-effect-key",
            action: &action,
            actor: &actor(),
        };
        assert!(
            resolve(&unbound_events, &unbound_context).is_none(),
            "a call with no Step identity is not exempt"
        );
    }

    /// A batch carries its children under the parent's identity. A batch with
    /// siblings would exempt their effects too, so it is not exempt at all and
    /// the siblings keep blocking. A batch with exactly one action is that
    /// action, and the excluded effect is the batch call itself.
    #[test]
    fn r1_batch_siblings_are_not_exempt() {
        let action = review_action("task:one");
        let sibling = serde_json::json!({
            "tool": "task_submit",
            "input": {"task_ref": "task:two", "artifact_refs": ["artifact://x"]},
        });
        let review = serde_json::json!({
            "tool": "task_review",
            "input": {
                "task_ref": "task:one",
                "decision": "accept",
                "reason": "independently read",
                "evidence_refs": ["artifact://read"],
            },
        });
        let batch = |actions: Value| {
            serde_json::json!({
                "tool_name": "agent_action_batch",
                "tool_call_id": "call-batch",
                "normalized_input": {"actions": actions},
                "step_binding": step_binding("call-batch", REVIEWER_RUN, None),
            })
        };

        let with_sibling = batch(serde_json::json!([review.clone(), sibling]));
        let sibling_events = [plan_event(serde_json::json!([with_sibling.clone()]))];
        let sibling_key = effect_key(&with_sibling);
        let sibling_context = CurrentReviewContext {
            action_id: &format!("{sibling_key}:0"),
            action: &action,
            actor: &actor(),
        };
        assert!(
            resolve(&sibling_events, &sibling_context).is_none(),
            "a batch with siblings is not exempt"
        );

        let alone = batch(serde_json::json!([review]));
        let alone_events = [plan_event(serde_json::json!([alone.clone()]))];
        let alone_key = effect_key(&alone);
        let alone_context = CurrentReviewContext {
            action_id: &format!("{alone_key}:0"),
            action: &action,
            actor: &actor(),
        };
        assert_eq!(
            resolve(&alone_events, &alone_context),
            Some(alone_key.clone()),
            "a batch that carries exactly one action is that action"
        );

        let missing_child = CurrentReviewContext {
            action_id: &format!("{alone_key}:1"),
            action: &action,
            actor: &actor(),
        };
        assert!(
            resolve(&alone_events, &missing_child).is_none(),
            "a child index that does not exist names no call"
        );
    }

    /// A root review is settled through the Goal lane and has no delegated run
    /// to bind a call to, so it is never exempt.
    #[test]
    fn r1_root_reviewer_is_never_exempt() {
        let task = review_task("call-a", "task:one", REVIEWER_RUN);
        let events = [plan_event(serde_json::json!([task.clone()]))];
        let mut root = actor();
        root.kind = AgentActorKind::Root;
        root.execution_id = None;
        root.agent_id = None;
        root.team_id = None;
        root.actor_id = "root:session".into();
        let context = CurrentReviewContext {
            action_id: &effect_key(&task),
            action: &review_action("task:one"),
            actor: &root,
        };
        assert!(
            resolve_current_review_effect_key(&events, "root-execution:graph", &context)
                .expect("an unambiguous resolution")
                .is_none(),
            "a root review has no in-flight delegated call to exempt"
        );
    }
}
