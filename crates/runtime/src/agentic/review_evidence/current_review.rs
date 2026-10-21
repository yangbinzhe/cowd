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
    AgentAction, AgentActorBinding, OBJECTIVE_REVIEW_TOOL_ID, TASK_REVIEW_TOOL_ID,
};
use harness_contract::agent::AgentTaskPacket;
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

/// One governed tool call of the Session's plans, with the identity the
/// proof would exempt.
struct PlanCandidate {
    effect_key: String,
    tool_name: String,
    step: crate::step_snapshot::StepCallBinding,
}

/// Every governed tool call of the pinned Session plans.
fn plan_candidates(session_events: &[crate::DurableRuntimeEvent]) -> Vec<PlanCandidate> {
    let mut candidates = Vec::new();
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
            candidates.push(PlanCandidate {
                effect_key: step.effect_idempotency_key(),
                tool_name: task["tool_name"].as_str().unwrap_or_default().to_string(),
                step,
            });
        }
    }
    candidates
}

/// Re-bind a stored identity against the durable evidence.
///
/// A proof carries the effect identity of the review call it was built
/// under, but a stored value is only a CANDIDATE. This entry re-derives the
/// call it names from the pinned plans and checks that the call is a review
/// control action of THIS reviewer. A candidate that does not re-bind exempts
/// nothing, so a proof can never grant an exemption by carrying a key.
pub(crate) fn rebind_current_review_effect_key(
    session_events: &[crate::DurableRuntimeEvent],
    packet: &AgentTaskPacket,
    carried: Option<&str>,
    original_action_id: Option<&str>,
) -> Result<Option<String>, String> {
    let Some(carried) = carried else {
        return Ok(None);
    };
    // The stored candidate is only the review's own call when it is the
    // invocation the ORIGINAL applied action recorded. A candidate that names
    // another call of the same reviewer is another review, not this one.
    if original_action_id != Some(carried) {
        return Ok(None);
    }
    let mut rebound = Vec::new();
    for candidate in plan_candidates(session_events) {
        if candidate.effect_key != carried
            || !matches!(
                candidate.tool_name.as_str(),
                TASK_REVIEW_TOOL_ID | OBJECTIVE_REVIEW_TOOL_ID
            )
            || !step_belongs_to_packet(&candidate.step, packet)
        {
            continue;
        }
        rebound.push(candidate.effect_key);
    }
    match rebound.len() {
        0 => Ok(None),
        1 => Ok(Some(rebound.remove(0))),
        _ => Err("current review call is ambiguous".into()),
    }
}

/// Resolve the ONE review control call that is currently executing.
///
/// The reviewer is the TRUSTED physical packet the proof already verified:
/// its digest was checked against the proof's packet digests and its owner
/// against the reviewer's execution identity. That is the same identity the
/// read-delivery boundary checks, so the exemption is bound exactly as
/// tightly as the delivery evidence is.
///
/// Returns `None` when the action is not a review, when the reviewer has no
/// trusted packet (a root review is a goal-metadata control action and is
/// already outside the business write set), when no pinned plan task carries
/// the presented invocation id, or when the candidate does not authenticate
/// against the packet, the actor and the action. An ambiguous resolution is
/// an error: two different calls could both claim the exclusion.
pub(crate) fn resolve_current_review_effect_key(
    session_events: &[crate::DurableRuntimeEvent],
    packet: &AgentTaskPacket,
    context: &CurrentReviewContext<'_>,
) -> Result<Option<String>, String> {
    let Some(expected_tool) = review_control_tool(context.action) else {
        return Ok(None);
    };
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
                    && step_belongs_to_reviewer(&step, packet, context.actor)
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
                || !step_belongs_to_reviewer(&step, packet, context.actor)
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

/// The receipt index stream that carries the reviewer's OWN admitted calls.
///
/// The stream names the graph, the agent node and the attempt, so an intent
/// found in it is the reviewer's call in THAT attempt. An intent with the
/// same effect identity in any other stream belongs to another attempt,
/// another node or another graph, and is never the review's own call.
pub(crate) fn reviewer_receipt_stream(packet: &AgentTaskPacket) -> String {
    format!(
        "execution-agent-receipts:{}:{}:{}",
        packet.graph_id(),
        packet.node_id(),
        packet.attempt
    )
}

/// The Step binding must belong to the reviewer's own execution.
///
/// Every coordinate is compared against the TRUSTED packet the proof
/// verified: the session, the turn, the run, the principal, the Program and
/// the Task the reviewer is bound to. The actor is compared too: its
/// execution id is the graph the packet belongs to, so an actor from
/// another graph cannot claim this packet's call.
fn step_belongs_to_reviewer(
    step: &crate::step_snapshot::StepCallBinding,
    packet: &AgentTaskPacket,
    actor: &AgentActorBinding,
) -> bool {
    step_belongs_to_packet(step, packet)
        && actor.session_id == packet.session_id()
        && actor.turn_id == packet.assignment.execution_identity.turn_id().unwrap_or_default()
        && packet
            .agentic_binding
            .as_ref()
            .is_some_and(|binding| actor.program_id == binding.program_id)
        && actor.execution_id.as_deref() == Some(packet.graph_id())
}

/// The Step binding must belong to the reviewer's own execution.
///
/// Every coordinate is compared against the TRUSTED packet the proof
/// verified: the session, the turn, the run, the principal, the Program and
/// the Task the reviewer is bound to.
fn step_belongs_to_packet(
    step: &crate::step_snapshot::StepCallBinding,
    packet: &AgentTaskPacket,
) -> bool {
    let identity = &step.snapshot.identity;
    let principal = packet.assignment.execution_identity.principal_id();
    let agent = packet.agent_id();
    let turn = packet.assignment.execution_identity.turn_id();
    let focus_task = packet
        .agentic_binding
        .as_ref()
        .and_then(|binding| match &binding.focus {
            harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
            | harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => {
                Some(task_ref.as_str())
            }
            _ => None,
        });
    identity.session_id == packet.session_id()
        && turn.is_some_and(|turn| identity.turn_id == turn)
        && identity.execution_id.as_deref() == Some(packet.run_id())
        && identity.run_ref.as_deref() == Some(packet.run_id())
        // The native provider path does not carry the attempt in the Step
        // identity; when it does, it has to be the reviewer's attempt. The
        // attempt is bound in full by the receipt index stream the call was
        // admitted in.
        && identity.attempt.is_none_or(|attempt| attempt == packet.attempt)
        && identity
            .actor_ref
            .as_deref()
            .is_some_and(|actor_ref| actor_ref == principal || actor_ref == agent)
        && packet.agentic_binding.as_ref().is_some_and(|binding| {
            identity.program_ref.as_deref() == Some(binding.program_id.as_str())
        })
        && focus_task.is_some_and(|task| identity.task_ref.as_deref() == Some(task))
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
    use harness_contract::agent::AgenticExecutionBinding;
    use harness_contract::agent_action::{
        AgentActorBinding, AgentActorKind, TaskReviewDecision, TaskReviewInput,
    };

    const RUN: &str = "run:agentic-graph:review";
    const SESSION: &str = "session";
    // The trusted packet's own turn identity: Runtime binds the packet's
    // lineage turn, the Program turn and the actor's turn to one value.
    const TURN: &str = "test-turn";

    fn packet() -> AgentTaskPacket {
        let mut packet = AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                "agent",
                RUN,
                "task",
                SESSION,
                "mission",
                Some("team"),
                "graph",
                "node",
            ),
            attempt: 2,
            expected_graph_revision: 7,
            objective: "review".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            cohort_prompt_package: None,
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::DangerFullAccess,
            policy_revision: 1,
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "budget",
                "agent",
                "agent",
                1,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "key".into(),
            agentic_binding: None,
        };
        packet.agentic_binding = Some(AgenticExecutionBinding {
            program_id: "program".into(),
            agent_id: "agent".into(),
            membership_id: "membership".into(),
            team_id: "team".into(),
            task_team_id: "team".into(),
            source_spec_revision: 1,
            task_purpose: None,
            focus: harness_contract::agent::AgenticExecutionFocus::TaskReview {
                task_ref: "task:one".into(),
            },
        });
        packet
    }

    fn actor() -> AgentActorBinding {
        AgentActorBinding {
            objective_id: "objective".into(),
            program_id: "program".into(),
            session_id: SESSION.into(),
            turn_id: TURN.into(),
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

    fn step_binding(call_id: &str) -> Value {
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
                    "session_id": SESSION,
                    "turn_id": TURN,
                    "execution_id": RUN,
                    "actor_ref": "test.principal",
                    "run_ref": RUN,
                    "attempt": 2,
                    "epoch": "revision-7",
                    "program_ref": "program",
                    "task_ref": "task:one",
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

    fn review_task(call_id: &str) -> Value {
        serde_json::json!({
            "tool_name": "task_review",
            "tool_call_id": call_id,
            "normalized_input": {
                "task_ref": "task:one",
                "decision": "accept",
                "reason": "independently read",
                "evidence_refs": ["artifact://read"],
            },
            "step_binding": step_binding(call_id),
        })
    }

    fn plan_event(tasks: Value) -> crate::DurableRuntimeEvent {
        crate::DurableRuntimeEvent {
            event_id: "plan-event".into(),
            stream_id: format!("session:{SESSION}"),
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

    fn review_action() -> AgentAction {
        AgentAction::TaskReview(TaskReviewInput {
            task_ref: "task:one".into(),
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
        packet: &AgentTaskPacket,
        context: &CurrentReviewContext<'_>,
    ) -> Option<String> {
        resolve_current_review_effect_key(events, packet, context).expect("an unambiguous resolution")
    }

    /// A stored candidate is only the review's own call when it is the
    /// invocation the ORIGINAL applied action recorded. A candidate that names
    /// another review call of the same reviewer is another review.
    #[test]
    fn r1_a_stored_candidate_must_be_the_original_action_invocation() {
        let packet = packet();
        let applied = review_task("call-a");
        let other = review_task("call-b");
        let events = [plan_event(serde_json::json!([applied.clone(), other.clone()]))];
        let applied_key = effect_key(&applied);
        let other_key = effect_key(&other);

        // The original action recorded the applied review's invocation.
        assert_eq!(
            rebind_current_review_effect_key(
                &events,
                &packet,
                Some(applied_key.as_str()),
                Some(applied_key.as_str()),
            )
            .expect("an unambiguous rebinding"),
            Some(applied_key.clone())
        );
        // The same candidate, but the original action recorded another
        // invocation: this is not the review's own call.
        assert_eq!(
            rebind_current_review_effect_key(
                &events,
                &packet,
                Some(applied_key.as_str()),
                Some(other_key.as_str()),
            )
            .expect("an unambiguous rebinding"),
            None,
            "a candidate that is not the original invocation exempts nothing"
        );
        // Another review call of the same reviewer is another review, even
        // when the original action recorded it.
        assert_eq!(
            rebind_current_review_effect_key(
                &events,
                &packet,
                Some(other_key.as_str()),
                Some(other_key.as_str()),
            )
            .expect("an unambiguous rebinding"),
            Some(other_key.clone()),
            "the call the original action recorded is the review's own call"
        );
        // No candidate, no exemption.
        assert_eq!(
            rebind_current_review_effect_key(&events, &packet, None, None)
                .expect("an unambiguous rebinding"),
            None
        );
    }
    /// Two review calls in the SAME run, with the SAME tool: only the one the
    /// submitted action names may be exempt. The other one is a second
    /// unfinished review and must keep blocking.
    #[test]
    fn r1_second_unfinished_review_in_the_same_run_is_not_exempt() {
        let first = review_task("call-a");
        let second = review_task("call-b");
        let events = [plan_event(serde_json::json!([first.clone(), second.clone()]))];
        let packet = packet();
        let action = review_action();
        let context = CurrentReviewContext {
            action_id: &effect_key(&first),
            action: &action,
            actor: &actor(),
        };
        let resolved = resolve(&events, &packet, &context).expect("the presented call resolves");
        assert_eq!(resolved, effect_key(&first));
        assert_ne!(
            resolved,
            effect_key(&second),
            "the other unfinished review keeps its own identity"
        );
    }

    /// A forged invocation id, an action the call did not perform, another
    /// reviewer, a call with no Step identity and a root reviewer all resolve
    /// to nothing: nothing is exempt.
    #[test]
    fn r1_unauthenticated_review_resolves_to_nothing() {
        let task = review_task("call-a");
        let events = [plan_event(serde_json::json!([task.clone()]))];
        let packet = packet();
        let action = review_action();
        let key = effect_key(&task);

        let forged = CurrentReviewContext {
            action_id: "forged-invocation-id",
            action: &action,
            actor: &actor(),
        };
        assert!(
            resolve(&events, &packet, &forged).is_none(),
            "a forged invocation id names no call"
        );

        let other_action = CurrentReviewContext {
            action_id: &key,
            action: &AgentAction::TaskReview(TaskReviewInput {
                task_ref: "task:two".into(),
                decision: TaskReviewDecision::Accept,
                reason: "independently read".into(),
                operator_requirement: None,
                evidence_refs: vec!["artifact://read".into()],
            }),
            actor: &actor(),
        };
        assert!(
            resolve(&events, &packet, &other_action).is_none(),
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
            resolve(&events, &packet, &other_verdict).is_none(),
            "another verdict is another action"
        );

        let mut unbound = review_task("call-unbound");
        unbound["step_binding"] = Value::Null;
        let unbound_events = [plan_event(serde_json::json!([unbound]))];
        let unbound_context = CurrentReviewContext {
            action_id: "unbound-effect-key",
            action: &action,
            actor: &actor(),
        };
        assert!(
            resolve(&unbound_events, &packet, &unbound_context).is_none(),
            "a call with no Step identity is not exempt"
        );
    }

    /// The same call, the same session, turn, run, Step and model call - with
    /// ONE coordinate changed - must not be exempt. Each mutation is a
    /// different reviewer or a different attempt of the same reviewer.
    #[test]
    fn r1_a_mutated_step_or_actor_is_not_exempt() {
        let packet = packet();
        let action = review_action();

        let mutate = |change: &str| {
            let mut task = review_task("call-a");
            let step = &mut task["step_binding"]["snapshot"]["identity"];
            match change {
                "actor_ref" => step["actor_ref"] = Value::String("someone.else".into()),
                "attempt" => step["attempt"] = serde_json::json!(99),
                "program" => step["program_ref"] = Value::String("other-program".into()),
                "task" => step["task_ref"] = Value::String("task:other".into()),
                "session" => step["session_id"] = Value::String("other-session".into()),
                "turn" => step["turn_id"] = Value::String("other-turn".into()),
                "run" => {
                    step["execution_id"] = Value::String("run:other".into());
                    step["run_ref"] = Value::String("run:other".into());
                }
                other => panic!("unknown mutation {other}"),
            }
            task
        };

        for change in [
            "actor_ref",
            "attempt",
            "program",
            "task",
            "session",
            "turn",
            "run",
        ] {
            let task = mutate(change);
            let events = [plan_event(serde_json::json!([task.clone()]))];
            let context = CurrentReviewContext {
                action_id: &effect_key(&task),
                action: &action,
                actor: &actor(),
            };
            assert!(
                resolve(&events, &packet, &context).is_none(),
                "a call with a mutated {change} is not the reviewer's own call"
            );
        }
    }

    /// The actor is the reviewer's own: its execution id is the graph the
    /// trusted packet belongs to. An actor from another graph cannot claim
    /// this packet's call, and neither can an actor from another session.
    #[test]
    fn r1_a_mutated_actor_is_not_exempt() {
        let task = review_task("call-a");
        let events = [plan_event(serde_json::json!([task.clone()]))];
        let packet = packet();
        let action = review_action();

        let mut foreign_graph = actor();
        foreign_graph.execution_id = Some("other-graph".into());
        let context = CurrentReviewContext {
            action_id: &effect_key(&task),
            action: &action,
            actor: &foreign_graph,
        };
        assert!(
            resolve(&events, &packet, &context).is_none(),
            "an actor of another graph is not the reviewer"
        );

        let mut foreign_session = actor();
        foreign_session.session_id = "other-session".into();
        let context = CurrentReviewContext {
            action_id: &effect_key(&task),
            action: &action,
            actor: &foreign_session,
        };
        assert!(
            resolve(&events, &packet, &context).is_none(),
            "an actor of another session is not the reviewer"
        );
    }

    /// A batch carries its children under the parent's identity. A batch with
    /// siblings would exempt their effects too, so it is not exempt at all and
    /// the siblings keep blocking. A batch with exactly one action is that
    /// action, and the excluded effect is the batch call itself.
    #[test]
    fn r1_batch_siblings_are_not_exempt() {
        let packet = packet();
        let action = review_action();
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
                "step_binding": step_binding("call-batch"),
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
            resolve(&sibling_events, &packet, &sibling_context).is_none(),
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
            resolve(&alone_events, &packet, &alone_context),
            Some(alone_key.clone()),
            "a batch that carries exactly one action is that action"
        );

        let missing_child = CurrentReviewContext {
            action_id: &format!("{alone_key}:1"),
            action: &action,
            actor: &actor(),
        };
        assert!(
            resolve(&alone_events, &packet, &missing_child).is_none(),
            "a child index that does not exist names no call"
        );
    }
}
