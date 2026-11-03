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

use std::collections::BTreeSet;

#[path = "current_review_retirement.rs"]
mod retirement;
pub(crate) use retirement::{graph_tail_is_own_review_retirement, graph_tail_preserves_work};

use harness_contract::agent::AgentTaskPacket;
use harness_contract::agent_action::{
    AgentAction, AgentActorBinding, OBJECTIVE_REVIEW_TOOL_ID, TASK_REVIEW_TOOL_ID,
};
use serde_json::Value;

/// The review action that is being applied, with the durable invocation id
/// the caller presented for it and the actor it was authenticated for.
pub(crate) struct CurrentReviewContext<'a> {
    pub action_id: &'a str,
    pub action: &'a AgentAction,
    pub actor: &'a AgentActorBinding,
}

/// The identity of the review action a proof is being re-verified against.
///
/// A proof is re-verified either while its review call is still live (the
/// action being applied) or after it has landed (the durable record the Goal
/// conclusion reads). Both resolve to ONE thing: the applied Program action id
/// that carries this proof. Nothing else about the review is needed to decide
/// whether a Program advance is that review's own application.
#[derive(Clone, Copy)]
pub(crate) enum ReviewActionIdentity<'a> {
    Live(&'a CurrentReviewContext<'a>),
    Applied(&'a AppliedReviewContext<'a>),
}

impl<'a> ReviewActionIdentity<'a> {
    pub(crate) fn action_id(&self) -> &'a str {
        match self {
            Self::Live(context) => context.action_id,
            Self::Applied(context) => context.action_id,
        }
    }
}

fn review_control_tool(action: &AgentAction) -> Option<&'static str> {
    match action {
        AgentAction::TaskReview(_) => Some(TASK_REVIEW_TOOL_ID),
        AgentAction::ObjectiveReview(_) => Some(OBJECTIVE_REVIEW_TOOL_ID),
        _ => None,
    }
}

/// True when this Program event is the review's own application.
///
/// The review is a governed Program action, so while the proof is being
/// assembled its own application is not in the stream yet and it lands one
/// revision later. Pinning the Program at the cut the proof was built against
/// therefore compares a revision that can never be reached: the review's own
/// application always follows its own discovery. The event that records THIS
/// action's application is the only advance the review may absorb.
pub(crate) fn is_current_review_application(event: &crate::DurableRuntimeEvent) -> bool {
    event.kind == "agentic.action_applied"
        && event.payload["envelope"]["action"]["kind"].as_str() == Some(OBJECTIVE_REVIEW_TOOL_ID)
        || event.kind == "agentic.action_applied"
            && event.payload["envelope"]["action"]["kind"].as_str() == Some(TASK_REVIEW_TOOL_ID)
}

pub(crate) fn program_cut_absorbs_own_review(
    events: &[crate::DurableRuntimeEvent],
    program_source: &str,
    pinned: u64,
    current: u64,
    action_id: &str,
) -> bool {
    program_cut_absorbs_review_records(
        events,
        program_source,
        pinned,
        current,
        action_id,
        &BTreeSet::new(),
    )
}

/// Additional records are authenticated by the original proof policy reader.
/// This admits their carrier advance only; it grants no effect/call exemption.
pub(crate) fn program_cut_absorbs_review_records(
    events: &[crate::DurableRuntimeEvent],
    program_source: &str,
    pinned: u64,
    current: u64,
    action_id: &str,
    authenticated_review_events: &BTreeSet<String>,
) -> bool {
    current.checked_sub(pinned) == Some(events.len() as u64)
        && events.iter().enumerate().all(|(offset, event)| {
            event.stream_id == program_source
                && event.sequence == pinned + offset as u64 + 1
                && event.scope == crate::RuntimeEventScope::Program
                && event.schema_version == 1
                && (is_current_review_application(event)
                    && (event.payload["envelope"]["action_id"].as_str() == Some(action_id)
                        || authenticated_review_events.contains(&event.event_id))
                    || is_worker_lifecycle_event(event)
                    || is_completion_request_event(event)
                    || is_conclusion_process_event(event))
        })
}

/// A bounded operation-local source slice. No full history scan or fixed
/// eight-event liveness barrier. All cuts remain part of the original CAS.
pub(crate) fn program_events_after_cut(
    store: &crate::RuntimeEventStore,
    source: &str,
    pinned: u64,
    through: u64,
) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
    crate::execution_core::goal::assessment::read_bounded_event_tail(store, source, pinned, through)
        .map_err(|error| error.to_string())
}

/// One governed tool call of the Session's plans, with the identity the
/// proof would exempt.
struct PlanCandidate {
    effect_key: String,
    tool_name: String,
    step: crate::step_snapshot::StepCallBinding,
}

/// An already applied review, resolved from the durable action record.
///
/// The conclusion path re-verifies a review that has landed, so the action it
/// names must exist in the Program journal and must be the action that
/// carried THIS proof. It is resolved from the durable record rather than from
/// the proof, so a stored value can never grant its own exemption.
pub(crate) struct AppliedReviewContext<'a> {
    pub action_id: &'a str,
    pub program_source: &'a str,
    pub pinned: u64,
    pub through: u64,
    pub program_events: &'a [crate::DurableRuntimeEvent],
}

/// Select the already authenticated identity, never recover a failed live
/// authentication from historical evidence. Applied reviews have no live call.
pub(crate) fn select_current_review_effect_key(
    identity: Option<ReviewActionIdentity<'_>>,
    live: Option<String>,
    rebound: Option<String>,
) -> Result<Option<String>, String> {
    match (identity, live, rebound) {
        (Some(ReviewActionIdentity::Live(_)), Some(live), Some(rebound)) => {
            if live != rebound {
                return Err(
                    "proof review call identity changed between the review and its application"
                        .into(),
                );
            }
            Ok(Some(live))
        }
        (Some(ReviewActionIdentity::Live(_)), live, _) => Ok(live),
        (Some(ReviewActionIdentity::Applied(context)), _, rebound) => {
            if rebound
                .as_deref()
                .is_some_and(|rebound| rebound != context.action_id)
            {
                return Err(
                    "proof review call identity differs from the durable applied review action"
                        .into(),
                );
            }
            Ok(Some(context.action_id.to_owned()))
        }
        (None, _, rebound) => Ok(rebound),
    }
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
    let mut rebound = BTreeSet::new();
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
            if task["tool_name"]
                .as_str()
                .is_some_and(|tool| matches!(tool, TASK_REVIEW_TOOL_ID | OBJECTIVE_REVIEW_TOOL_ID))
                && effect_key == carried
                && step_belongs_to_packet(&step, packet)
            {
                rebound.insert(effect_key);
                continue;
            }
            if task["tool_name"].as_str() != Some("agent_action_batch")
                || !step_belongs_to_packet(&step, packet)
            {
                continue;
            }
            if let Some(actions) = task["normalized_input"]["actions"].as_array() {
                for (index, inner) in actions.iter().enumerate() {
                    if inner["tool"].as_str().is_some_and(|tool| {
                        matches!(tool, TASK_REVIEW_TOOL_ID | OBJECTIVE_REVIEW_TOOL_ID)
                    }) && format!("{effect_key}:{index}") == carried
                    {
                        rebound.insert(format!("{effect_key}:{index}"));
                    }
                }
            }
        }
    }
    match rebound.len() {
        0 => Ok(None),
        1 => Ok(rebound.pop_first()),
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
            // exactly as the producer builds it. Exempt only that child: a
            // legal multi-action batch keeps every sibling effect blocking.
            if task["tool_name"].as_str() != Some("agent_action_batch") {
                continue;
            }
            let Some(actions) = task["normalized_input"]["actions"].as_array() else {
                continue;
            };
            for (index, inner) in actions.iter().enumerate() {
                let child_effect_key = format!("{effect_key}:{index}");
                if inner["tool"].as_str() != Some(expected_tool)
                    || child_effect_key != context.action_id
                    || !step_belongs_to_reviewer(&step, packet, context.actor)
                    || !action_matches(&inner["input"], context.action)
                {
                    continue;
                }
                resolved.push(child_effect_key);
            }
        }
    }
    match resolved.len() {
        0 => Ok(None),
        1 => Ok(Some(resolved.remove(0))),
        _ => Err("current review call is ambiguous".into()),
    }
}

// A historical source must already be part of this original proof. Discovering
// a new live head cannot manufacture an old cut.
fn require_original_cut(
    store: &crate::RuntimeEventStore, source: &str,
    cuts: &std::collections::BTreeMap<String, u64>,
) -> Result<u64, String> {
    let cut = *cuts.get(source).ok_or("original witness source cut missing")?;
    if cut == 0 || store.stream_revision(source).map_err(|error| error.to_string())? < cut {
        return Err("original witness source cut unavailable".into());
    }
    Ok(cut)
}

fn result_event_by_key_at_original_cut(
    store: &crate::RuntimeEventStore, source: &str, key: &str,
    cuts: &std::collections::BTreeMap<String, u64>,
) -> Result<Option<crate::DurableRuntimeEvent>, String> {
    let cut = require_original_cut(store, source, cuts)?;
    let event = store.event_by_idempotency_key(source, key).map_err(|error| error.to_string())?;
    if event.as_ref().is_some_and(|event| event.stream_id != source || event.sequence > cut) {
        return Err("original keyed witness is outside proof cut".into());
    }
    Ok(event)
}

fn unique_historical_start_at_original_cut(
    store: &std::sync::Arc<crate::RuntimeEventStore>, graphs: &crate::ExecutionGraphStateStore,
    graph_id: &str, node_id: &str, expected_attempt: Option<u32>, before_cursor: u64,
    cuts: &std::collections::BTreeMap<String, u64>,
) -> Result<crate::execution_core::graph::NodeExecutionTicket, String> {
    use crate::execution_core::graph::ExecutionGraphEvent;
    let cut = require_original_cut(store, graph_id, cuts)?;
    let mut after = 0;
    let mut selected = None;
    let budget = crate::SourceCheckBudget::default();
    let (mut pages, mut count, mut bytes) = (0usize, 0usize, 0usize);
    while after < cut {
        pages += 1;
        if pages > budget.max_pages { return Err("historical start page budget exceeded".into()); }
        let page = store.list_stream_after(graph_id, after, cut, 64, 4 * 1024 * 1024)?;
        if page.is_empty() { return Err("historical start cut incomplete".into()); }
        for record in page {
            count += 1;
            bytes = bytes.saturating_add(serde_json::to_vec(&record).map_err(|error| error.to_string())?.len());
            if count > budget.max_events || bytes > budget.max_bytes || record.stream_id != graph_id
                || record.sequence != after + 1 || record.sequence > cut {
                return Err("historical start cut invalid or over budget".into());
            }
            after = record.sequence;
            if record.commit_cursor >= before_cursor { continue; }
            let event: ExecutionGraphEvent = serde_json::from_value(store.resolve_graph_record(&record)?)
                .map_err(|_| "historical start graph event malformed")?;
            if event.kind() != record.kind { return Err("historical start graph kind differs".into()); }
            let binding = match &event {
                ExecutionGraphEvent::NodeTransitioned { node_id: node, binding: Some(binding), .. }
                    if node == node_id => Some(binding),
                ExecutionGraphEvent::NodesStarted { bindings, .. } => bindings.get(node_id),
                ExecutionGraphEvent::Checkpoint { cause, bindings, .. }
                    if matches!(cause.as_str(), "execution_graph.nodes_started" | "execution_graph.node_transitioned") => bindings.get(node_id),
                _ => None,
            };
            let Some(binding) = binding else { continue; };
            if expected_attempt.is_some_and(|attempt| attempt != binding.attempt) { continue; }
            let graph = graphs.load_graph_at_revision(graph_id, record.sequence).map_err(|error| error.to_string())?;
            let node = graph.nodes.iter().find(|node| node.id == node_id).ok_or("historical start node missing")?;
            let ticket = crate::execution_core::graph::NodeExecutionTicket {
                graph_id: graph_id.into(), node_id: node_id.into(), executor_kind: binding.executor_kind.clone(),
                service_class: graph.service_class, attempt: binding.attempt,
                idempotency_key: binding.ticket_idempotency_key.clone(), payload_ref: node.payload_ref.clone(),
            };
            crate::execution_core::graph::validate_node_ticket_start_at_revision(store, &ticket, record.sequence)
                .map_err(|error| error.to_string())?;
            if selected.replace(ticket).is_some() { return Err("historical physical node start is ambiguous".into()); }
        }
    }
    selected.ok_or_else(|| "historical physical node start missing".into())
}

pub(crate) struct ProcessReviewWitness {
    pub action_id: String,
    pub request: crate::RuntimeToolExecutionRequest,
    pub child_graph_id: String,
    pub child_node_id: String,
    pub child_attempt: u32,
    child_ticket: crate::execution_core::graph::NodeExecutionTicket,
    parent_ticket: crate::execution_core::graph::NodeExecutionTicket,
}

pub(crate) fn resolve_process_current_review_witness(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    packet: &AgentTaskPacket,
    context: &CurrentReviewContext<'_>,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Option<ProcessReviewWitness>, String> {
    use crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_graph;
    use harness_contract::step::CallSource;
    let Some(tool) = review_control_tool(context.action) else { return Ok(None); };
    if context.actor.session_id != packet.session_id()
        || Some(context.actor.turn_id.as_str()) != packet.assignment.execution_identity.turn_id()
        || context.actor.execution_id.as_deref() != Some(packet.graph_id())
        || !matches!(context.actor.kind, harness_contract::agent_action::AgentActorKind::Agent | harness_contract::agent_action::AgentActorKind::TeamLead)
        || packet.agentic_binding.as_ref().is_none_or(|binding| binding.program_id != context.actor.program_id
            || context.actor.actor_id != binding.agent_id
            || context.actor.agent_id.as_deref() != Some(binding.agent_id.as_str())
            || context.actor.team_id.as_deref() != Some(binding.team_id.as_str()))
    { return Ok(None); }
    let graphs = crate::ExecutionGraphStateStore::new(store.clone());
    let lineage = format!("execution-lineage:{}", packet.graph_id());
    let lineage_cut = require_original_cut(store, &lineage, cuts)?;
    let links = graphs.child_links_at_cut(packet.graph_id(), lineage_cut).map_err(|error| error.to_string())?;
    let mut found = None;
    for (link, registration) in links {
        if link.parent_node_id != packet.node_id() || !link.child_execution_id.starts_with("agent-tool-batch:") { continue; }
        let (graph, _) = graphs.child_planned_at_registration(&link, &registration).map_err(|error| error.to_string())?;
        let bound = decode_bound_agent_tool_graph(&graph).map_err(|_| "review original child graph malformed")?;
        let request = &bound.request;
        let batch_index = if request.tool_name == "agent_action_batch" {
            let prefix = format!("{}:", request.idempotency_key);
            let Some(suffix) = context.action_id.strip_prefix(&prefix) else { continue; };
            let Ok(index) = suffix.parse::<usize>() else { continue; };
            if suffix != index.to_string() { continue; }
            Some(index)
        } else {
            if request.idempotency_key != context.action_id { continue; }
            None
        };
        let Some(step) = request.step_binding.as_ref() else { return Err("review original Process Step missing".into()); };
        let identity = &step.snapshot.identity;
        let input = harness_contract::step::parse_strict_json(&request.input).map_err(|error| error.to_string())?;
        let mut action_input = if let Some(position) = batch_index {
            let actions = input["actions"].as_array().ok_or("review original batch actions missing")?;
            let entry = actions.get(position).ok_or("review original batch child missing")?;
            if entry["tool"] != tool { return Err("review original batch child tool differs".into()); }
            entry.get("input").cloned().ok_or("review original batch child input missing")?
        } else { input.clone() };
        let Some(action_object) = action_input.as_object_mut() else { return Err("review original input is not an object".into()); };
        if action_object.get("expected_revision").is_some_and(|value| !value.is_null() && value.as_u64().is_none()) {
            return Err("review original expected revision is invalid".into());
        }
        if batch_index.is_some() {
            for field in ["reference", "evidence_ref"] {
                if let Some(raw) = action_object.get(field).and_then(|value| value.as_str()) {
                    action_object.insert(field.into(), Value::String(crate::normalize_bare_evidence_ref(raw)));
                }
            }
            for field in ["evidence_refs", "result_refs", "artifact_refs"] {
                if let Some(values) = action_object.get_mut(field).and_then(|value| value.as_array_mut()) {
                    for value in values {
                        if let Some(raw) = value.as_str() {
                            *value = Value::String(crate::normalize_bare_evidence_ref(raw));
                        }
                    }
                }
            }
        }
        action_object.remove("expected_revision");
        let original_action = serde_json::from_value::<AgentAction>(serde_json::json!({"kind": tool, "input": action_input}))
            .map_err(|_| "review original action input is invalid")?;
        let step_task = packet.agentic_binding.as_ref().and_then(|binding| match &binding.focus {
            harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } => Some(task_ref.as_str()),
            _ => None,
        });
        let review_target_matches = match (packet.agentic_binding.as_ref().map(|binding| &binding.focus), context.action) {
            (Some(harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref }), AgentAction::TaskReview(input)) => input.task_ref == *task_ref,
            _ => true,
        };
        if bound.agent_id != packet.agent_id() || bound.run_id != packet.run_id()
            || (batch_index.is_none() && request.tool_name != tool) || request.session_id.as_deref() != Some(packet.session_id())
            || request.parent_execution.as_ref().is_none_or(|parent| parent.execution_id != packet.graph_id() || parent.node_id != packet.node_id())
            || request.parent_execution_attempt != Some(packet.attempt)
            || request.tool_use_id != step.origin.call_id || request.idempotency_key != step.effect_idempotency_key()
            || step.origin.origin_step_id != identity.step_id || step.origin.catalog_digest != step.snapshot.catalog_digest
            || !matches!(&step.origin.source, CallSource::Process { worker_id, sequence } if worker_id == packet.run_id() && *sequence > 0)
            || identity.session_id != packet.session_id() || Some(identity.turn_id.as_str()) != packet.assignment.execution_identity.turn_id()
            || identity.execution_id.as_deref() != Some(packet.graph_id()) || identity.run_ref.as_deref() != Some(packet.run_id())
            || identity.attempt != Some(packet.attempt) || identity.actor_ref.as_deref() != Some(packet.agent_id())
            || identity.program_ref.as_deref() != Some(context.actor.program_id.as_str())
            || identity.task_ref.as_deref() != step_task || !review_target_matches
            || (batch_index.is_none() && original_action != *context.action)
        { return Err("review original Process call differs from its owner or action".into()); }
        let source = format!("execution-effect:{}", request.idempotency_key);
        let index_source = reviewer_receipt_stream(packet);
        let intent = result_event_by_key_at_original_cut(store, &source,
            &format!("{}:intent", request.idempotency_key), cuts)?.ok_or("review original intent missing")?;
        let index = result_event_by_key_at_original_cut(store, &index_source,
            &format!("agent-tool-intent:{}", request.idempotency_key), cuts)?.ok_or("review original intent index missing")?;
        if intent.kind != "execution.effect.intent" || index.kind != "execution.agent_tool.intent"
            || intent.scope != crate::RuntimeEventScope::ExecutionNode || index.scope != intent.scope
            || intent.schema_version != 1 || index.schema_version != 1
            || intent.status.as_deref() != Some("inflight") || index.status != intent.status
            || intent.transaction_id != index.transaction_id || intent.commit_cursor != index.commit_cursor
            || intent.transaction_index >= index.transaction_index || registration.commit_cursor >= intent.commit_cursor
        { return Err("review original Process intent transaction differs".into()); }
        crate::ExecutionCommitService::validate_proof_bound_tool_request(request, &intent, &index)
            .map_err(|error| error.to_string())?;
        let node = graph.nodes.first().ok_or("review original child node missing")?;
        let child = unique_historical_start_at_original_cut(store, &graphs, &graph.id, &node.id, None, intent.commit_cursor, cuts)?;
        if child.payload_ref != node.payload_ref { return Err("review original child start differs".into()); }
        let parent = unique_historical_start_at_original_cut(store, &graphs, packet.graph_id(), packet.node_id(), Some(packet.attempt), intent.commit_cursor, cuts)?;
        let stored: AgentTaskPacket = serde_json::from_str(&parent.payload_ref).map_err(|_| "review original parent packet malformed")?;
        let mut physical = stored;
        physical.attempt = packet.attempt;
        if physical != *packet { return Err("review original parent packet differs".into()); }
        let witness = ProcessReviewWitness { action_id: context.action_id.to_owned(), request: request.clone(),
            child_graph_id: graph.id.clone(), child_node_id: node.id.clone(), child_attempt: child.attempt,
            child_ticket: child, parent_ticket: parent };
        if found.replace(witness).is_some() { return Err("review original Process call is ambiguous".into()); }
    }
    Ok(found)
}

/// Private operation-local facts. Only this owner may construct these values;
/// serialized proof payloads cannot supply a policy call or approval.
#[derive(Clone, PartialEq)]
pub(crate) struct AuthenticatedApprovalRef {
    approval_id: String,
    grant_id: String,
    context: harness_contract::policy::approval::ApprovalContext,
    risk: harness_contract::core::TaskRisk,
}
impl AuthenticatedApprovalRef {
    pub(crate) fn approval_id(&self) -> &str { &self.approval_id }
    pub(crate) fn grant_id(&self) -> &str { &self.grant_id }
    pub(crate) fn context(&self) -> &harness_contract::policy::approval::ApprovalContext { &self.context }
    pub(crate) fn risk(&self) -> harness_contract::core::TaskRisk { self.risk }
}

/// Constructible only by the original root-call authentication path.
#[derive(Clone, PartialEq)]
pub(crate) struct AuthenticatedRootCall {
    original_action: crate::DurableRuntimeEvent,
    original_receipt: crate::DurableRuntimeEvent,
    original_owner: crate::DurableRuntimeEvent,
}

#[derive(Clone, PartialEq)]
pub(crate) enum AuthenticatedPolicyOwner {
    Process(AgentTaskPacket),
    Root(AuthenticatedRootCall),
}

#[derive(Clone, PartialEq)]
pub(crate) struct AuthenticatedHistoricalPolicyCall {
    owner: AuthenticatedPolicyOwner,
    request: crate::AuthorizationRequest,
    original_authorization: Option<harness_contract::tool::ToolExecutionAuthorization>,
    approval: Option<AuthenticatedApprovalRef>,
    receipt_cursor: u64,
    current_fences: Vec<crate::ExpectedStreamRevision>,
    original_action: crate::DurableRuntimeEvent,
    original_assessment: crate::DurableRuntimeEvent,
    original_receipt: crate::DurableRuntimeEvent,
    original_ticket: Option<crate::execution_core::graph::NodeExecutionTicket>,
    original_packet: Option<AgentTaskPacket>,
}
impl AuthenticatedHistoricalPolicyCall {
    pub(crate) fn policy_owner(&self) -> &AuthenticatedPolicyOwner { &self.owner }
    pub(crate) fn process_packet(&self) -> Option<&AgentTaskPacket> {
        match &self.owner { AuthenticatedPolicyOwner::Process(packet) => Some(packet), AuthenticatedPolicyOwner::Root(_) => None }
    }
    pub(crate) fn request(&self) -> &crate::AuthorizationRequest { &self.request }
    pub(crate) fn original_authorization(&self) -> Option<&harness_contract::tool::ToolExecutionAuthorization> { self.original_authorization.as_ref() }
    pub(crate) fn approval(&self) -> Option<&AuthenticatedApprovalRef> { self.approval.as_ref() }
    pub(crate) fn receipt_cursor(&self) -> u64 { self.receipt_cursor }
    pub(crate) fn current_fences(&self) -> &[crate::ExpectedStreamRevision] { &self.current_fences }
    pub(crate) fn binding_digest(&self) -> String {
        super::proof_value_digest(&(&self.original_action, &self.original_assessment,
            &self.original_receipt, &self.original_ticket, &self.original_packet, &self.current_fences))
    }
}

/// Evidence already authenticated against one original checked member and
/// its later physical receipt. Only this module can construct it. It never
/// grants authority and has no deserialization path.
pub(crate) struct AuthenticatedMemberApprovalEvidence {
    store: std::sync::Arc<crate::RuntimeEventStore>,
    action: crate::DurableRuntimeEvent,
    index: crate::DurableRuntimeEvent,
    transaction: crate::AppendTransactionReceipt,
    consume: crate::DurableRuntimeEvent,
    assessment: crate::DurableRuntimeEvent,
    effect_receipt: crate::DurableRuntimeEvent,
    context: harness_contract::policy::approval::ApprovalContext,
    risk: harness_contract::core::TaskRisk,
}
impl AuthenticatedMemberApprovalEvidence {
    pub(crate) fn belongs_to(&self, store: &std::sync::Arc<crate::RuntimeEventStore>) -> bool { std::sync::Arc::ptr_eq(&self.store, store) }
    pub(crate) fn action(&self) -> &crate::DurableRuntimeEvent { &self.action }
    pub(crate) fn index(&self) -> &crate::DurableRuntimeEvent { &self.index }
    pub(crate) fn transaction(&self) -> &crate::AppendTransactionReceipt { &self.transaction }
    pub(crate) fn consume(&self) -> &crate::DurableRuntimeEvent { &self.consume }
    pub(crate) fn assessment(&self) -> &crate::DurableRuntimeEvent { &self.assessment }
    pub(crate) fn effect_receipt(&self) -> &crate::DurableRuntimeEvent { &self.effect_receipt }
    pub(crate) fn context(&self) -> &harness_contract::policy::approval::ApprovalContext { &self.context }
    pub(crate) fn risk(&self) -> harness_contract::core::TaskRisk { self.risk }
}

/// Original member lease facts for the lease owner; construction is private
/// to the original review authenticator, never from serialized proof input.
pub(crate) struct AuthenticatedMemberLeaseEvidence {
    store: std::sync::Arc<crate::RuntimeEventStore>,
    action: crate::DurableRuntimeEvent,
    index: crate::DurableRuntimeEvent,
    transaction: crate::AppendTransactionReceipt,
    issuer: crate::DurableRuntimeEvent,
    consume: crate::DurableRuntimeEvent,
    assessment: crate::DurableRuntimeEvent,
    effect_receipt: crate::DurableRuntimeEvent,
    authorization: harness_contract::tool::ToolExecutionAuthorization,
    principal_id: String,
}
impl AuthenticatedMemberLeaseEvidence {
    pub(crate) fn belongs_to(&self, store: &std::sync::Arc<crate::RuntimeEventStore>) -> bool { std::sync::Arc::ptr_eq(&self.store, store) }
    pub(crate) fn action(&self) -> &crate::DurableRuntimeEvent { &self.action }
    pub(crate) fn index(&self) -> &crate::DurableRuntimeEvent { &self.index }
    pub(crate) fn transaction(&self) -> &crate::AppendTransactionReceipt { &self.transaction }
    pub(crate) fn issuer(&self) -> &crate::DurableRuntimeEvent { &self.issuer }
    pub(crate) fn consume(&self) -> &crate::DurableRuntimeEvent { &self.consume }
    pub(crate) fn assessment(&self) -> &crate::DurableRuntimeEvent { &self.assessment }
    pub(crate) fn effect_receipt(&self) -> &crate::DurableRuntimeEvent { &self.effect_receipt }
    pub(crate) fn authorization(&self) -> &harness_contract::tool::ToolExecutionAuthorization { &self.authorization }
    pub(crate) fn principal_id(&self) -> &str { &self.principal_id }
}

/// A member lease transition authenticated by its original checked
/// transaction, action and member index. It is not an authorization grant.
pub(crate) struct AuthenticatedMemberLeaseTransition {
    transaction: crate::AppendTransactionReceipt,
    action: crate::DurableRuntimeEvent,
    index: crate::DurableRuntimeEvent,
    transition: crate::DurableRuntimeEvent,
}
impl AuthenticatedMemberLeaseTransition {
    pub(crate) fn transaction(&self) -> &crate::AppendTransactionReceipt { &self.transaction }
    pub(crate) fn action(&self) -> &crate::DurableRuntimeEvent { &self.action }
    pub(crate) fn index(&self) -> &crate::DurableRuntimeEvent { &self.index }
    pub(crate) fn transition(&self) -> &crate::DurableRuntimeEvent { &self.transition }
}

/// Follow only the source ranges actually named by the original transaction
/// receipt. The member index is a locator; receipt, source ranges and Program
/// action independently bind it to the same committed transaction.
pub(crate) fn authenticate_original_member_lease_transition(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    transition: &crate::DurableRuntimeEvent,
) -> Result<AuthenticatedMemberLeaseTransition, String> {
    let receipt = store.original_transaction_receipt(&transition.transaction_id).map_err(|e| e.to_string())?
        .ok_or("original member lease transaction receipt missing")?;
    if transition.kind != "authorization.lease_transition" || transition.scope != crate::RuntimeEventScope::Tool
        || transition.schema_version != 1 || receipt.transaction_id != transition.transaction_id
        || receipt.commit_cursor == 0 || receipt.commit_cursor != transition.commit_cursor
        || receipt.event_ids.get(transition.transaction_index as usize) != Some(&transition.event_id)
        || !receipt.stream_revisions.iter().any(|source| source.stream_id == transition.stream_id
            && source.expected_revision < transition.sequence && source.committed_revision >= transition.sequence) {
        return Err("original member lease transition receipt differs".into());
    }
    let budget = crate::SourceCheckBudget::default();
    let (mut pages, mut count, mut bytes) = (0usize, 0usize, 0usize);
    let mut indices = Vec::new();
    for source in &receipt.stream_revisions {
        if source.expected_revision > source.committed_revision { return Err("member transaction source range invalid".into()); }
        let mut after = source.expected_revision;
        while after < source.committed_revision {
            pages += 1;
            if pages > budget.max_pages { return Err("member transaction source page budget exceeded".into()); }
            let page = store.list_stream_after(&source.stream_id, after, source.committed_revision, 64, 4 * 1024 * 1024)?;
            if page.is_empty() { return Err("member transaction source range incomplete".into()); }
            for event in page {
                count += 1;
                bytes = bytes.saturating_add(serde_json::to_vec(&event).map_err(|e| e.to_string())?.len());
                if count > budget.max_events || bytes > budget.max_bytes || event.stream_id != source.stream_id
                    || event.sequence != after + 1 || event.sequence > source.committed_revision {
                    return Err("member transaction source range invalid or over budget".into());
                }
                after = event.sequence;
                if event.transaction_id != receipt.transaction_id || event.commit_cursor != receipt.commit_cursor
                    || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id) {
                    return Err("member transaction source event differs from receipt".into());
                }
                if event.kind == "agentic.batch_member.index" { indices.push(event); }
            }
        }
    }
    if indices.len() != 1 { return Err("member lease original transaction index missing or ambiguous".into()); }
    let index = indices.pop().expect("one member index");
    let outer = index.payload["outer_key"].as_str().filter(|v| !v.is_empty()).ok_or("member lease outer key missing")?;
    let ordinal = index.payload["member_index"].as_u64().ok_or("member lease ordinal missing")?;
    let identity = index.payload["member_identity"].as_str().filter(|v| !v.is_empty()).ok_or("member lease identity missing")?;
    let refs = index.payload["authority_events"].as_array().ok_or("member lease authority coordinates missing")?;
    if index.scope != crate::RuntimeEventScope::ExecutionNode || index.schema_version != 1
        || index.actor.as_deref() != Some("runtime.agentic_action") || index.status.as_deref() != Some("committed")
        || index.stream_id != format!("execution-effect:{outer}")
        || index.idempotency_key.as_deref() != Some(format!("agentic-member:{ordinal}").as_str())
        || refs.iter().filter(|reference| reference["stream_id"].as_str() == Some(transition.stream_id.as_str())
            && reference["kind"].as_str() == Some(transition.kind.as_str())
            && reference["idempotency_key"].as_str() == transition.idempotency_key.as_deref()
            && reference["transaction_index"].as_u64() == Some(u64::from(transition.transaction_index))).count() != 1 {
        return Err("member lease original index binding differs".into());
    }
    let program = index.payload["program_stream"].as_str().ok_or("member lease original Program missing")?;
    let key = index.payload["program_event_key"].as_str().ok_or("member lease original action key missing")?;
    let action = store.event_by_idempotency_key(program, key).map_err(|e| e.to_string())?
        .ok_or("member lease original action missing")?;
    if action.scope != crate::RuntimeEventScope::Program || action.schema_version != 1
        || !matches!(action.kind.as_str(), "agentic.action_applied" | "agentic.action_rejected")
        || action.transaction_id != receipt.transaction_id || action.commit_cursor != receipt.commit_cursor
        || receipt.event_ids.get(action.transaction_index as usize) != Some(&action.event_id)
        || index.payload["program_event_index"].as_u64() != Some(u64::from(action.transaction_index))
        || index.payload["program_event_kind"].as_str() != Some(action.kind.as_str())
        || action.payload["envelope"]["action_id"] != format!("{outer}:{ordinal}")
        || action.payload["member_admission"]["prepared"]["identity"] != identity
        || action.payload["member_admission"]["ticket"] != index.payload["ticket"] {
        return Err("member lease original Program action differs".into());
    }
    Ok(AuthenticatedMemberLeaseTransition { transaction: receipt, action, index, transition: transition.clone() })
}

/// Recover one already-committed member from the original node transaction.
/// A node result alone is not physical closure: require the original binding
/// closure, and independently bind both records to their actual receipts.
fn authenticate_closed_node_review_member(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    ticket: &crate::execution_core::graph::NodeExecutionTicket,
    parent_ticket: Option<&crate::execution_core::graph::NodeExecutionTicket>,
    outer: &str,
    position: usize,
    action: &crate::DurableRuntimeEvent,
    context: &CurrentReviewContext<'_>,
    sources: &mut std::collections::BTreeMap<String, u64>,
) -> Result<harness_contract::agent_action::AgentActionObservation, String> {
    let graphs = crate::ExecutionGraphStateStore::new(store.clone());
    let graph = graphs.load(&ticket.graph_id).map_err(|e| e.to_string())?;
    let graph_revision = super::results::pin_result_source(store, &ticket.graph_id, sources)?;
    if graph.revision != graph_revision { return Err("member node graph changed during authentication".into()); }
    let binding = graphs.current_binding(&graph, &ticket.node_id, ticket.attempt).map_err(|e| e.to_string())?;
    let (original, _) = graphs.binding_start_ticket(&graph, &ticket.node_id, &binding).map_err(|e| e.to_string())?;
    if original != *ticket { return Err("member node original ticket differs".into()); }
    let start = graphs.binding_start_position(&graph, &ticket.node_id, &binding).map_err(|e| e.to_string())?
        .ok_or("member node original start missing")?;
    if !graphs.binding_lease_closure(&graph, &ticket.node_id, &binding, start).map_err(|e| e.to_string())? {
        return Err("member node original physical closure unavailable".into());
    }
    let closure_key = binding.lease_closure_key(&graph, &ticket.node_id);
    let closure = store.event_by_idempotency_key(&ticket.graph_id, &closure_key).map_err(|e| e.to_string())?
        .ok_or("member node original closure record missing")?;
    let closure_receipt = store.original_transaction_receipt(&closure.transaction_id).map_err(|e| e.to_string())?
        .ok_or("member node original closure receipt missing")?;
    if closure_receipt.transaction_id != closure.transaction_id || closure_receipt.commit_cursor != closure.commit_cursor
        || closure_receipt.event_ids.get(closure.transaction_index as usize) != Some(&closure.event_id)
        || !closure_receipt.stream_revisions.iter().any(|source| source.stream_id == closure.stream_id
            && source.expected_revision < closure.sequence && source.committed_revision >= closure.sequence) {
        return Err("member node closure transaction differs".into());
    }
    let commits = crate::ExecutionCommitService::new(store.clone());
    let members = commits.original_batch_members_after_commit(ticket, outer).map_err(|e| e.to_string())?;
    let member = members.get(position).ok_or("original committed member is missing")?;
    let payload = member.payload();
    if let Some(parent) = parent_ticket {
        let original = &payload["original_child_registration"];
        if payload.get("source_ticket") != Some(&serde_json::to_value(parent).map_err(|e| e.to_string())?)
            || original.get("parent_ticket") != Some(&serde_json::to_value(parent).map_err(|e| e.to_string())?)
            || original.get("child_ticket") != Some(&serde_json::to_value(ticket).map_err(|e| e.to_string())?) {
            return Err("original member parent/child tickets differ".into());
        }
        let lineage = crate::execution_core::graph::execution_lineage_stream_id(&parent.graph_id);
        let lineage_head = super::results::pin_result_source(store, &lineage, sources)?;
        let key = format!("{}:child:{}", parent.graph_id, ticket.graph_id);
        let registration = store.event_by_idempotency_key(&lineage, &key).map_err(|e| e.to_string())?
            .ok_or("original member child registration missing")?;
        if registration.sequence > lineage_head || registration.kind != "execution.lineage.child_registered.v1"
            || registration.scope != crate::RuntimeEventScope::Relation
            || registration.actor.as_deref() != Some("execution_commit_service")
            || registration.status.as_deref() != Some("registered") || registration.schema_version != 1 {
            return Err("original member child registration differs".into());
        }
        let links = graphs.child_links(&parent.graph_id).map_err(|e| e.to_string())?;
        let matches: Vec<_> = links.iter().filter(|link| link.parent_execution_id == parent.graph_id
            && link.parent_node_id == parent.node_id && link.child_execution_id == ticket.graph_id
            && serde_json::to_value(link).ok().as_ref() == Some(&registration.payload)).collect();
        if matches.len() != 1 { return Err("original member child lineage differs or is ambiguous".into()); }
        let link = matches[0];
        let (_, planned) = graphs.child_planned_at_registration(link, &registration).map_err(|e| e.to_string())?;
        let receipt = store.original_transaction_receipt(&registration.transaction_id).map_err(|e| e.to_string())?
            .ok_or("original member child registration receipt missing")?;
        if original["registration_event_id"] != registration.event_id || original["planned_event_id"] != planned.event_id
            || original["transaction_id"] != receipt.transaction_id || original["commit_cursor"].as_u64() != Some(receipt.commit_cursor)
            || receipt.commit_cursor != registration.commit_cursor || planned.transaction_id != registration.transaction_id
            || receipt.event_ids.get(registration.transaction_index as usize) != Some(&registration.event_id)
            || receipt.event_ids.get(planned.transaction_index as usize) != Some(&planned.event_id)
            || !receipt.stream_revisions.iter().any(|src| src.stream_id == registration.stream_id && src.expected_revision < registration.sequence && registration.sequence <= src.committed_revision)
            || !receipt.stream_revisions.iter().any(|src| src.stream_id == planned.stream_id && src.expected_revision < planned.sequence && planned.sequence <= src.committed_revision) {
            return Err("original member child registration transaction differs".into());
        }
    } else if payload.get("source_ticket").is_some_and(|v| !v.is_null())
        || payload.get("original_child_registration").is_some_and(|v| !v.is_null()) {
        return Err("unexpected original member child source".into());
    }
    if payload["member_index"].as_u64() != u64::try_from(position).ok() || payload["outer_key"] != outer
        || payload["action_event_id"] != action.event_id || payload["action_transaction_id"] != action.transaction_id
        || payload["action_commit_cursor"].as_u64() != Some(action.commit_cursor)
        || payload["business"] != "applied" || payload["observation"]["state"] != "available" {
        return Err("original committed member result is unavailable or mismatched".into());
    }
    let node_source = format!("{}:node:{}", ticket.graph_id, ticket.node_id);
    let node_revision = super::results::pin_result_source(store, &node_source, sources)?;
    let key = format!("{}:batch:{}:{}:{}", ticket.idempotency_key, outer.len(), outer, position);
    let member_event = store.event_by_idempotency_key(&node_source, &key).map_err(|e| e.to_string())?
        .ok_or("original committed member event missing")?;
    let member_receipt = store.original_transaction_receipt(&member_event.transaction_id).map_err(|e| e.to_string())?
        .ok_or("original committed member transaction receipt missing")?;
    if member_event.sequence > node_revision || member_event.payload != *payload
        || member_receipt.transaction_id != member_event.transaction_id || member_receipt.commit_cursor != member_event.commit_cursor
        || member_receipt.event_ids.get(member_event.transaction_index as usize) != Some(&member_event.event_id)
        || !member_receipt.stream_revisions.iter().any(|source| source.stream_id == node_source
            && source.expected_revision < member_event.sequence && source.committed_revision >= member_event.sequence)
        || member_event.commit_cursor <= action.commit_cursor || closure.commit_cursor <= member_event.commit_cursor {
        return Err("original member/node/closure transaction ordering differs".into());
    }
    let observation: harness_contract::agent_action::AgentActionObservation = serde_json::from_value(payload["observation"]["value"].clone())
        .map_err(|_| "original committed member observation malformed")?;
    if observation.action_id != context.action_id || observation.program_id != context.actor.program_id
        || observation.revision != action.sequence || observation.status != harness_contract::agent_action::AgentActionStatus::Applied
        || observation.duplicate || observation.error.is_some() {
        return Err("original committed member observation differs from review action".into());
    }
    if store.stream_revision(&ticket.graph_id).map_err(|e| e.to_string())? != graph_revision
        || store.stream_revision(&node_source).map_err(|e| e.to_string())? != node_revision {
        return Err("original member closure sources changed during authentication".into());
    }
    Ok(observation)
}

/// A returned review call, authenticated by the original Program and effect
/// owners. The later receipt fences are separate from the proof material cut.
pub(crate) struct CompletedProcessReviewWitness {
    pub(super) request: crate::RuntimeToolExecutionRequest,
    pub(super) action: crate::DurableRuntimeEvent,
    pub(super) intent: crate::DurableRuntimeEvent,
    pub(super) receipt: crate::DurableRuntimeEvent,
    child_ticket: crate::execution_core::graph::NodeExecutionTicket,
    parent_ticket: crate::execution_core::graph::NodeExecutionTicket,
    packet: AgentTaskPacket,
    pub(super) source_fences: std::collections::BTreeMap<String, u64>,
}

pub(crate) fn authenticate_completed_process_review(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &super::super::program::AgenticProgramProjection,
    packet: &AgentTaskPacket,
    context: &CurrentReviewContext<'_>,
    proof_cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Option<CompletedProcessReviewWitness>, String> {
    use harness_contract::agent_action::{AgentActionEnvelope, AgentActionObservation, AgentActionStatus};
    let Some(witness) = resolve_process_current_review_witness(store, packet, context, proof_cuts)? else { return Ok(None); };
    let request = &witness.request;
    let mut fences = std::collections::BTreeMap::new();
    let program_source = format!("agentic-program:{}", program.program_id);
    let head = super::results::pin_result_source(store, &program_source, &mut fences)?;
    if head != program.revision { return Err("completed review Program changed while authenticating".into()); }
    let action = store.event_by_idempotency_key(&program_source,
        &crate::agentic::action_service::action_key(context.action_id))
        .map_err(|error| error.to_string())?.ok_or("completed review applied action missing")?;
    if action.sequence > head || action.scope != crate::RuntimeEventScope::Program
        || action.kind != "agentic.action_applied" || action.schema_version != 1
        || action.commit_cursor == 0
        || (request.tool_name != "agent_action_batch" && action.transaction_index != 0) {
        return Err("completed review applied action identity differs".into());
    }
    let envelope: AgentActionEnvelope = serde_json::from_value(action.payload["envelope"].clone())
        .map_err(|_| "completed review original envelope malformed")?;
    if envelope.action_id != context.action_id || &envelope.action != context.action
        || &envelope.actor != context.actor || envelope.actor.program_id != program.program_id
        || envelope.actor.session_id != program.session_id || envelope.actor.turn_id != program.turn_id
        || action.status.as_deref() != Some("applied")
        || action.actor.as_deref() != Some(envelope.actor.actor_id.as_str())
        || action.transaction_id != format!("agent-action:{}:{}", program.program_id, context.action_id) {
        return Err("completed review action and original owner differ".into());
    }
    let action_receipt = store.original_transaction_receipt(&action.transaction_id)
        .map_err(|error| error.to_string())?.ok_or("completed review action transaction receipt missing")?;
    if action_receipt.transaction_id != action.transaction_id || action_receipt.commit_cursor != action.commit_cursor
        || action_receipt.event_ids.get(action.transaction_index as usize) != Some(&action.event_id)
        || !action_receipt.stream_revisions.iter().any(|revision|
            revision.stream_id == program_source && revision.committed_revision == action.sequence) {
        return Err("completed review action transaction receipt differs".into());
    }
    let action_source = store.transaction_stream_revision_exact(&action.transaction_id, &program_source)
        .map_err(|error| error.to_string())?.ok_or("completed review Program transaction source missing")?;
    if action_source.stream_id != program_source || action_source.committed_revision != action.sequence
        || action_source.expected_revision.checked_add(1) != Some(action.sequence) {
        return Err("completed review Program prior cut differs from action transaction".into());
    }
    let index_source = reviewer_receipt_stream(packet);
    let index_head = super::results::pin_result_source(store, &index_source, &mut fences)?;
    let intent = store.event_by_idempotency_key(&index_source, &format!("agent-tool-intent:{}", request.idempotency_key))
        .map_err(|error| error.to_string())?.ok_or("completed review original intent missing")?;
    let receipt = store.event_by_idempotency_key(&index_source, &format!("agent-tool-receipt:{}", request.idempotency_key))
        .map_err(|error| error.to_string())?.ok_or("completed review original receipt missing")?;
    if intent.sequence > index_head || receipt.sequence > index_head
        || intent.commit_cursor >= action.commit_cursor || action.commit_cursor >= receipt.commit_cursor
        || (request.tool_name != "agent_action_batch" && receipt.payload["outcome"]["status"] != "executed") {
        return Err("completed review original intent/action/receipt ordering differs".into());
    }
    let effect = super::policy::authenticate_settled_effect(store, &intent, &receipt, &mut fences)?;
    if request.compiled_invocation.as_ref().is_none_or(|invocation| invocation.effect != effect) {
        return Err("completed review receipt effect differs from original call".into());
    }
    let observation = if request.tool_name == "agent_action_batch" {
        let suffix = context.action_id.strip_prefix(&format!("{}:", request.idempotency_key))
            .ok_or("completed batch review index missing")?;
        let index: usize = suffix.parse().map_err(|_| "completed batch review index invalid")?;
        if index.to_string() != suffix { return Err("completed batch review index is noncanonical".into()); }
        authenticate_closed_node_review_member(store, &witness.child_ticket, Some(&witness.parent_ticket), &request.idempotency_key,
            index, &action, context, &mut fences)?
    } else {
        let output = receipt.payload["outcome"]["output"].as_str().ok_or("completed review output missing")?;
        serde_json::from_str::<AgentActionObservation>(output).map_err(|_| "completed review observation malformed")?
    };
    if observation.action_id != context.action_id || observation.program_id != program.program_id
        || observation.revision != action.sequence || observation.status != AgentActionStatus::Applied
        || observation.duplicate || observation.error.is_some() {
        return Err("completed review original observation differs".into());
    }
    for (source, head) in &fences {
        if store.stream_revision(source).map_err(|error| error.to_string())? != *head {
            return Err("completed review witness source changed during authentication".into());
        }
    }
    Ok(Some(CompletedProcessReviewWitness { request: request.clone(), action, intent, receipt,
        child_ticket: witness.child_ticket, parent_ticket: witness.parent_ticket, packet: packet.clone(), source_fences: fences }))
}

/// Recover the original single-call coordinates from the Program action, and
/// independently check their original transaction receipts.  This is an
/// intermediate witness, not authority to construct an authenticated call.
/// Select the original Session call through the existing result owner. This
/// helper does not mint authority: the ticket, request and both committed
/// transaction pairs must match the original call before it is returned.
fn original_node_review_index(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    request: &crate::RuntimeToolExecutionRequest,
    ticket: &crate::execution_core::graph::NodeExecutionTicket,
    intent: &crate::DurableRuntimeEvent,
    physical: &crate::DurableRuntimeEvent,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<crate::DurableRuntimeEvent, String> {
    let parent = request.parent_execution.as_ref().ok_or("original Node request parent missing")?;
    if parent.execution_id != ticket.graph_id || parent.node_id != ticket.node_id
        || request.parent_execution_attempt != Some(ticket.attempt) {
        return Err("original Node request does not match its ticket".into());
    }
    let session = request.session_id.as_deref().filter(|session| !session.is_empty())
        .ok_or("original Node request Session missing")?;
    let stream = format!("session:{session}");
    let head = store.stream_revision(&stream).map_err(|error| error.to_string())?;
    let mut after = 0;
    let mut events = Vec::new();
    while after < head {
        let page = store.list_stream_after(&stream, after, head, 64, 4 * 1024 * 1024)
            .map_err(|error| error.to_string())?;
        if page.is_empty() { return Err("original Node Session page missing".into()); }
        for event in page {
            if event.stream_id != stream || event.sequence != after + 1 || event.sequence > head {
                return Err("original Node Session sequence differs".into());
            }
            after = event.sequence;
            if event.kind == "tool.execution_plan.created" || event.kind == "tool.invocation.completed"
                && event.payload["tool_call_id"] == request.tool_use_id {
                events.push(event);
            }
        }
    }
    let expected_step = serde_json::to_value(request.step_binding.as_ref()
        .ok_or("original Node request Step missing")?).map_err(|error| error.to_string())?;
    let mut selected = None;
    for owner in events.iter().filter(|event| event.kind == "tool.invocation.completed"
        && event.payload["tool_call_id"] == request.tool_use_id && event.payload["tool_name"] == request.tool_name) {
        let Some(task) = super::results::result_tool_plan(&events, owner)? else { continue; };
        if task["step_binding"] != expected_step { continue; }
        if selected.replace((owner, task)).is_some() {
            return Err("original Node completed call ambiguous".into());
        }
    }
    let (owner, task) = selected.ok_or("original Node completed call missing")?;
    if owner.scope != crate::RuntimeEventScope::Tool || owner.schema_version != 1
        || owner.status.as_deref() != Some("completed") || owner.actor.as_deref() != Some("conversation_runtime") {
        return Err("original Node completed call owner differs".into());
    }
    let plan_id = owner.payload["governed_plan_id"].as_str().ok_or("original Node plan identity missing")?;
    let mut plans = events.iter().filter(|event| event.kind == "tool.execution_plan.created"
        && event.payload["plan_id"] == plan_id);
    let plan = plans.next().ok_or("original Node plan event missing")?;
    if plans.next().is_some() || plan.scope != crate::RuntimeEventScope::Tool || plan.schema_version != 1
        || plan.status.as_deref() != Some("planned") || plan.actor.as_deref() != Some("conversation_runtime") {
        return Err("original Node plan owner or uniqueness differs".into());
    }
    let (selected, index) = super::results::original_tool_primary_and_index(store, owner, task, cuts)?;
    if selected.event_id != physical.event_id || selected.transaction_id != physical.transaction_id
        || selected.commit_cursor != physical.commit_cursor {
        return Err("original Node selected primary differs".into());
    }
    let index_source = format!("execution-agent-receipts:{}:{}:{}", ticket.graph_id, ticket.node_id, ticket.attempt);
    let intent_index = store.event_by_idempotency_key(&index_source, &format!("agent-tool-intent:{}", request.idempotency_key))
        .map_err(|error| error.to_string())?.ok_or("original Node intent index missing")?;
    crate::ExecutionCommitService::validate_proof_bound_tool_request(request, intent, &intent_index)
        .map_err(|error| error.to_string())?;
    let verify_event = |event: &crate::DurableRuntimeEvent| -> Result<(), String> {
        let receipt = store.original_transaction_receipt(&event.transaction_id)
            .map_err(|error| error.to_string())?.ok_or("original Node transaction receipt missing")?;
        let source = store.transaction_stream_revision_exact(&event.transaction_id, &event.stream_id)
            .map_err(|error| error.to_string())?.ok_or("original Node transaction source missing")?;
        if receipt.transaction_id != event.transaction_id || receipt.commit_cursor != event.commit_cursor
            || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id)
            || source.expected_revision >= event.sequence || event.sequence > source.committed_revision
            || receipt.stream_revisions.iter().filter(|revision| revision.stream_id == event.stream_id).count() != 1
            || !receipt.stream_revisions.iter().any(|revision| revision.stream_id == source.stream_id
                && revision.expected_revision == source.expected_revision && revision.committed_revision == source.committed_revision) {
            return Err("original Node transaction position or source differs".into());
        }
        Ok(())
    };
    for event in [&intent_index, intent, physical, &index, plan, owner] { verify_event(event)?; }
    if intent.scope != crate::RuntimeEventScope::ExecutionNode || intent.schema_version != 1
        || intent.status.as_deref() != Some("inflight") || intent.actor.as_deref() != Some("governed_tool")
        || intent_index.scope != crate::RuntimeEventScope::ExecutionNode || intent_index.schema_version != 1
        || intent_index.status.as_deref() != Some("inflight") || intent_index.actor.as_deref() != Some("governed_tool")
        || intent.transaction_id != intent_index.transaction_id || intent.commit_cursor != intent_index.commit_cursor
        || intent.transaction_index >= intent_index.transaction_index
        || physical.transaction_id != index.transaction_id || physical.commit_cursor != index.commit_cursor
        || physical.transaction_index >= index.transaction_index || index.stream_id != index_source
        || intent.payload["replay"] != physical.payload["replay"]
        || intent.payload["effect"] != physical.payload["effect"]
        || intent.payload["input_sha256"] != physical.payload["input_sha256"]
        || intent.payload["resolved_effect_scope"] != physical.payload["resolved_effect_scope"]
        || index.payload["effect"] != physical.payload["effect"]
        || index.payload["outcome"] != physical.payload["outcome"]
        || index.payload["resolved_effect_scope"] != physical.payload["resolved_effect_scope"] {
        return Err("original Node intent or physical receipt pair differs".into());
    }
    Ok(index)
}

pub(crate) fn authenticate_original_single_review(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    action: &crate::DurableRuntimeEvent,
    context: &CurrentReviewContext<'_>,
    _proof_cuts: &std::collections::BTreeMap<String, u64>,
) -> Result<AuthenticatedHistoricalPolicyCall, String> {
    use sha2::{Digest as _, Sha256};
    let facts = action.payload.get("original_single_call").ok_or("single review original call unavailable")?;
    let ticket: crate::execution_core::graph::NodeExecutionTicket =
        serde_json::from_value(facts["ticket"].clone()).map_err(|_| "single review original ticket malformed")?;
    let request: crate::RuntimeToolExecutionRequest =
        serde_json::from_value(facts["request"].clone()).map_err(|_| "single review original request malformed")?;
    let step = request.step_binding.as_ref().ok_or("single review original Step missing")?;
    let original = context.actor.original_execution.as_ref().ok_or("single review original physical identity missing")?;
    if request.tool_name != review_control_tool(context.action).ok_or("single review tool missing")?
        || request.idempotency_key != context.action_id || request.session_id.as_deref() != Some(context.actor.session_id.as_str())
        || request.tool_use_id != step.origin.call_id || request.idempotency_key != step.effect_idempotency_key()
        || step.snapshot.identity.session_id != context.actor.session_id
        || step.snapshot.identity.turn_id != context.actor.turn_id
        || original.graph_id != ticket.graph_id || original.node_id != ticket.node_id || original.attempt != ticket.attempt
        || action.payload["envelope"]["action_id"] != context.action_id {
        return Err("single review original call/owner binding differs".into());
    }
    let parsed = harness_contract::step::parse_strict_json(&request.input)
        .map_err(|_| "single review original input is not strict JSON")?;
    let mut normalized = parsed.clone();
    let object = normalized.as_object_mut().ok_or("single review original action input is not an object")?;
    if object.get("expected_revision").is_some_and(|value| !value.is_null() && value.as_u64().is_none()) {
        return Err("single review original expected revision is invalid".into());
    }
    object.remove("expected_revision");
    let original_action: AgentAction = serde_json::from_value(serde_json::json!({
        "kind": request.tool_name, "input": normalized,
    })).map_err(|_| "single review original action is malformed")?;
    if &original_action != context.action {
        return Err("single review original action input differs".into());
    }
    let transaction = store.original_transaction_receipt(&action.transaction_id).map_err(|error| error.to_string())?
        .ok_or("single review action transaction receipt missing")?;
    if transaction.transaction_id != action.transaction_id || transaction.commit_cursor != action.commit_cursor
        || transaction.event_ids.get(action.transaction_index as usize) != Some(&action.event_id) {
        return Err("single review action transaction receipt differs".into());
    }
    let source = format!("execution-effect:{}", request.idempotency_key);
    let intent = store.event_by_idempotency_key(&source, &format!("{}:intent", request.idempotency_key))
        .map_err(|error| error.to_string())?.ok_or("single review original intent missing")?;
    let physical = store.event_by_idempotency_key(&source, &format!("{}:receipt", request.idempotency_key))
        .map_err(|error| error.to_string())?.ok_or("single review physical receipt missing")?;
    let graph = crate::execution_core::graph::validate_node_ticket_start(store, &ticket, Some(&intent))
        .map_err(|error| error.to_string())?;
    let objective = harness_contract::agent_action::root_objective_id(&context.actor.session_id, &context.actor.turn_id);
    if graph.parent_execution.is_some() || context.actor.kind != harness_contract::agent_action::AgentActorKind::Root
        || context.actor.actor_id != format!("root:{}", context.actor.session_id)
        || context.actor.agent_id.is_some() || context.actor.team_id.is_some()
        || context.actor.execution_id.is_some() || context.actor.root_execution_id.as_deref() != Some(graph.id.as_str())
        || context.actor.objective_id != objective
        || context.actor.program_id != harness_contract::agent_action::program_id_for_objective(&objective)
        || graph.lineage.as_ref().is_none_or(|lineage| lineage.session_id != context.actor.session_id || lineage.turn_id != context.actor.turn_id) {
        return Err("single review original root actor differs from physical graph".into());
    }
    if intent.commit_cursor >= action.commit_cursor || action.commit_cursor >= physical.commit_cursor {
        return Err("single review original intent/action/receipt ordering differs".into());
    }
    let mut sources = std::collections::BTreeMap::new();
    let receipt = store.original_transaction_receipt(&physical.transaction_id).map_err(|error| error.to_string())?
        .ok_or("single review physical transaction receipt missing")?;
    if receipt.transaction_id != physical.transaction_id || receipt.commit_cursor != physical.commit_cursor
        || receipt.event_ids.get(physical.transaction_index as usize) != Some(&physical.event_id) {
        return Err("single review physical transaction receipt differs".into());
    }
    let index = if request.parent_execution.is_some() {
        original_node_review_index(store, &request, &ticket, &intent, &physical, &mut sources)?
    } else {
        super::results::authenticate_root_receipt_owner(store, &physical, &mut sources)?;
        let (owner, original_attempt, _) = super::results::original_root_effect_owner(&physical)?;
        let node = owner.node_id.as_deref().ok_or("single review original activity node missing")?;
        if owner.session_id != context.actor.session_id || owner.turn_id != context.actor.turn_id
            || owner.root_execution_id != ticket.graph_id || node != ticket.node_id || original_attempt != ticket.attempt {
            return Err("single review physical owner is not its original ticket/turn".into());
        }
        let index_source = format!("execution-agent-receipts:{}:{node}:{}", owner.root_execution_id, ticket.attempt);
        store.event_by_idempotency_key(&index_source, &format!("agent-tool-receipt:{}", request.idempotency_key))
            .map_err(|error| error.to_string())?.ok_or("single review original physical index missing")?
    };
    if index.transaction_id != physical.transaction_id || index.commit_cursor != physical.commit_cursor
        || index.transaction_index <= physical.transaction_index
        || receipt.event_ids.get(index.transaction_index as usize) != Some(&index.event_id)
        || !receipt.stream_revisions.iter().any(|revision| revision.stream_id == index.stream_id
            && revision.expected_revision < index.sequence && revision.committed_revision >= index.sequence) {
        return Err("single review original physical index transaction differs".into());
    }
    let invocation = request.compiled_invocation.as_ref().ok_or("single review original invocation missing")?;
    let digest = harness_contract::step::canonical_digest("tool-effect-payload", &invocation.intent.normalized_input)
        .map_err(|error| error.to_string())?;
    if invocation.intent.normalized_input != parsed
        || physical.payload["replay"]["origin"] != serde_json::to_value(&step.origin).map_err(|e| e.to_string())?
        || physical.payload["replay"]["call_id"] != request.tool_use_id
        || physical.payload["replay"]["payload_digest"] != digest
        || physical.payload["replay"]["invocation"]["effect"] != serde_json::to_value(&invocation.effect).map_err(|e| e.to_string())?
        || physical.payload["outcome"]["status"] != "executed"
        || !physical.payload["outcome"]["error"].is_null() {
        return Err("single review physical effect or original input differs".into());
    }
    let output = physical.payload["outcome"]["output"].as_str()
        .ok_or("single review original observation missing")?;
    let observation: harness_contract::agent_action::AgentActionObservation = serde_json::from_str(output)
        .map_err(|_| "single review original observation malformed")?;
    if observation.action_id != context.action_id || observation.program_id != context.actor.program_id
        || observation.revision != action.sequence
        || observation.status != harness_contract::agent_action::AgentActionStatus::Applied
        || observation.duplicate || observation.error.is_some() {
        return Err("single review physical outcome does not match original action".into());
    }
    let refs = facts["authorization_events"].as_array().filter(|refs| !refs.is_empty())
        .ok_or("single review original authorization coordinates missing")?;
    let mut seen = BTreeSet::new();
    let mut original_events = Vec::with_capacity(refs.len());
    for coordinate in refs {
        let stream = coordinate["stream_id"].as_str().ok_or("single authorization source missing")?;
        let sequence = coordinate["sequence"].as_u64().filter(|n| *n > 0).ok_or("single authorization sequence missing")?;
        if !seen.insert((stream.to_owned(), sequence)) { return Err("single authorization coordinate duplicated".into()); }
        let event = store.list_stream_before(stream, sequence, 1, 1024 * 1024)
            .map_err(|error| error.to_string())?.pop().ok_or("single authorization event missing")?;
        let original_tx = store.original_transaction_receipt(&event.transaction_id).map_err(|error| error.to_string())?
            .ok_or("single authorization transaction receipt missing")?;
        let source_revision = store.transaction_stream_revision_exact(&action.transaction_id, stream)
            .map_err(|error| error.to_string())?.ok_or("single authorization source was not locked")?;
        if event.stream_id != stream || event.sequence != sequence || event.commit_cursor >= action.commit_cursor
            || coordinate["event_id"].as_str() != Some(event.event_id.as_str())
            || coordinate["transaction_id"].as_str() != Some(event.transaction_id.as_str())
            || coordinate["transaction_index"].as_u64() != Some(u64::from(event.transaction_index))
            || coordinate["commit_cursor"].as_u64() != Some(event.commit_cursor)
            || original_tx.commit_cursor != event.commit_cursor || original_tx.transaction_id != event.transaction_id
            || original_tx.event_ids.get(event.transaction_index as usize) != Some(&event.event_id)
            || source_revision.expected_revision < sequence {
            return Err("single authorization original coordinate or transaction differs".into());
        }
        original_events.push(event);
    }
    let authorization = request.authorization.as_ref().ok_or("single review original authorization missing")?;
    let mut assessments = original_events.iter().filter(|event|
        event.kind == "authorization.capability_assessed"
            && event.payload.pointer("/lease/lease_id").and_then(Value::as_str)
                == Some(authorization.authorization_lease.lease_id.as_str()));
    let assessment_event = assessments.next().ok_or("single review original assessment missing")?;
    if assessments.next().is_some() { return Err("single review original assessment ambiguous".into()); }
    let binding = &assessment_event.payload["_call_refusal_binding"];
    let assessed: harness_contract::policy::CapabilityAssessment = serde_json::from_value(assessment_event.payload.clone())
        .map_err(|_| "single review original assessment malformed")?;
    let assessed_lease = assessed.lease.as_ref().ok_or("single review assessment lease missing")?;
    let invocation = request.compiled_invocation.as_ref().ok_or("single review invocation missing")?;
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(&invocation.effect, &request.input);
    if assessment_event.stream_id != format!("session:{}", context.actor.session_id)
        || assessment_event.scope != crate::RuntimeEventScope::Tool || assessment_event.schema_version != 1
        || assessment_event.actor.as_deref() != Some("conversation_runtime")
        || [("execution", ticket.graph_id.as_str()), ("session", context.actor.session_id.as_str()),
            ("turn", context.actor.turn_id.as_str())].iter().any(|(kind, id)|
            !assessment_event.refs.iter().any(|reference| reference.kind == *kind && reference.id == *id))
        || binding["version"] != 1 || binding["owner"]["kind"] != "node"
        || binding["owner"]["ticket"] != serde_json::to_value(&ticket).map_err(|e| e.to_string())?
        || binding["step_binding"] != serde_json::to_value(&request.step_binding).map_err(|e| e.to_string())?
        || binding["call_id"] != request.tool_use_id || binding["tool_name"] != request.tool_name
        || binding["effect_key"] != request.idempotency_key
        || binding["input_sha256"] != format!("sha256:{:x}", Sha256::digest(request.input.as_bytes()))
        || assessment_event.payload.get("_original_permission_context").is_none()
        || assessed.capability != request.tool_name || assessed.effect != effective.descriptor.assessment
        || assessed.requested_scopes != effective.descriptor.scopes
        || assessed.required_mode != effective.descriptor.required_permission || assessed.gap.is_some()
        || assessed.risk != crate::authorization_negotiator::risk_for_effect(&effective.descriptor.assessment)
        || !matches!(assessed.path, harness_contract::policy::AuthorizationPath::ExistingLease
            | harness_contract::policy::AuthorizationPath::StandingGrant
            | harness_contract::policy::AuthorizationPath::PolicyAutoGrant
            | harness_contract::policy::AuthorizationPath::HumanApproval)
        || authorization.tool_id != request.tool_name || authorization.descriptor_hash != effective.descriptor.descriptor_hash
        || !crate::AuthorizationNegotiator::verify_lease_signature(assessed_lease)
        || !crate::AuthorizationNegotiator::verify_lease_signature(&authorization.authorization_lease)
        || crate::authorization_negotiator::authorization_lease_signed_payload(assessed_lease)
            != crate::authorization_negotiator::authorization_lease_signed_payload(&authorization.authorization_lease)
        || assessed_lease.signature != authorization.authorization_lease.signature {
        return Err("single review assessment or original signed binding differs".into());
    }
    let mut pre_authorization: crate::RuntimeToolExecutionRequest =
        serde_json::from_value(binding["request"].clone())
            .map_err(|_| "single review assessment original request malformed")?;
    if pre_authorization.authorization.is_some() {
        return Err("single review assessment request was already authorized".into());
    }
    pre_authorization.authorization = Some(authorization.clone());
    if serde_json::to_value(&pre_authorization).map_err(|e| e.to_string())?
        != serde_json::to_value(&request).map_err(|e| e.to_string())? {
        return Err("single review authorized request differs from original assessment".into());
    }
    let consumptions: Vec<_> = original_events.iter().filter(|event|
        event.kind == "authorization.lease_transition" && event.status.as_deref() == Some("consumed")
            && event.stream_id == format!("authorization-lease:{}", authorization.authorization_lease.lease_id))
        .collect();
    if consumptions.len() != 1 { return Err("single review original consumption missing or ambiguous".into()); }
    let consume: harness_contract::policy::AuthorizationLeaseTransition =
        serde_json::from_value(consumptions[0].payload.clone()).map_err(|_| "single review original consumption malformed")?;
    if !crate::authorization_negotiator::consumed_transition_matches_original_execution(&consume, authorization)
        || consume.lease.signature != authorization.authorization_lease.signature
        || crate::authorization_negotiator::authorization_lease_signed_payload(&consume.lease)
            != crate::authorization_negotiator::authorization_lease_signed_payload(&authorization.authorization_lease)
        || !crate::AuthorizationNegotiator::verify_lease_signature(&consume.lease) {
        return Err("single review original consumption differs".into());
    }
    let memory = request.memory_context.as_ref().ok_or("single review original memory identity missing")?;
    if memory.session_id != context.actor.session_id || memory.turn_ref.as_deref() != Some(context.actor.turn_id.as_str())
        || memory.agent_id.trim().is_empty() {
        return Err("single review original memory identity differs".into());
    }
    let (principal, parent_lease_id) = crate::conversation::original_memory_authorization_identity(
        &memory.session_id, &memory.agent_id);
    if assessed_lease.principal_id != principal || authorization.policy_revision != assessed_lease.policy_revision
        || assessed_lease.parent_lease_id != parent_lease_id
        || !assessed_lease.permits(&assessed.capability, assessed.required_mode)
        || !assessed.requested_scopes.iter().all(|scope| assessed_lease.scopes.contains(scope)) {
        return Err("single review original lease principal or scope differs".into());
    }
    let original_context = assessment_event.payload["_original_permission_context"].as_object()
        .ok_or("single review original permission context malformed")?;
    if original_context.len() != 2 || !original_context.contains_key("override_decision")
        || !original_context.contains_key("override_reason") {
        return Err("single review original permission context shape differs".into());
    }
    let decision = match &original_context["override_decision"] {
        Value::Null => None,
        Value::String(value) if value == "allow" => Some(crate::permissions::PermissionOverride::Allow),
        Value::String(value) if value == "deny" => Some(crate::permissions::PermissionOverride::Deny),
        Value::String(value) if value == "ask" => Some(crate::permissions::PermissionOverride::Ask),
        _ => return Err("single review permission override invalid".into()),
    };
    let reason = match &original_context["override_reason"] {
        Value::Null => None, Value::String(value) => Some(value.clone()),
        _ => return Err("single review permission reason invalid".into()),
    };
    let lease_source = format!("authorization-lease:{}", authorization.authorization_lease.lease_id);
    let issuer = store.list_stream_after(&lease_source, 0, 1, 1, 1024 * 1024)?
        .into_iter().next().ok_or("single review original lease issuer missing")?;
    let issuer_receipt = store.original_transaction_receipt(&issuer.transaction_id).map_err(|e| e.to_string())?
        .ok_or("single review original lease issuer receipt missing")?;
    if issuer_receipt.transaction_id != issuer.transaction_id || issuer_receipt.commit_cursor != issuer.commit_cursor
        || issuer_receipt.event_ids.get(issuer.transaction_index as usize) != Some(&issuer.event_id)
        || !issuer_receipt.stream_revisions.iter().any(|source| source.stream_id == lease_source
            && source.expected_revision < issuer.sequence && source.committed_revision >= issuer.sequence) {
        return Err("single review original lease issuer transaction differs".into());
    }
    let lease_control = crate::authorization_negotiator::capture_proof_lease_control(
        store, authorization, &principal, physical.commit_cursor)?;
    if lease_control.explicit_revocation().is_some() {
        return Err("single review original authorization explicitly revoked; relevance unknown".into());
    }
    let approval = if assessment_event.payload["_original_approval_grant"].is_null() {
        if assessed.path == harness_contract::policy::AuthorizationPath::HumanApproval {
            return Err("single review original human approval missing".into());
        }
        None
    } else {
        if assessed.path != harness_contract::policy::AuthorizationPath::HumanApproval {
            return Err("single review original approval path differs".into());
        }
        let grant: harness_contract::policy::ApprovalGrant = serde_json::from_value(
            assessment_event.payload["_original_approval_grant"].clone())
            .map_err(|_| "single review original approval malformed")?;
        let approval_source = format!("approval:{}", grant.approval_id);
        let consume_tx = format!("approval-grant-consume:{}", grant.grant_id);
        let consume_receipt = store.original_transaction_receipt(&consume_tx).map_err(|e| e.to_string())?
            .ok_or("single review original approval consume transaction missing")?;
        let mut approval_sources = consume_receipt.stream_revisions.iter().filter(|source| source.stream_id == approval_source);
        let approval_revision = approval_sources.next().ok_or("single review original approval consume source missing")?;
        if approval_sources.next().is_some() || consume_receipt.transaction_id != consume_tx
            || consume_receipt.commit_cursor >= assessment_event.commit_cursor {
            return Err("single review original approval transaction differs".into());
        }
        let (original, _) = crate::ApprovalQueue::decision_result_at_cut(store, &context.actor.session_id,
            &grant.approval_id, approval_revision.expected_revision)?;
        let approval_context = original.context;
        let risk = crate::approval_coordinator::task_risk_for_effect(&effective.descriptor);
        let targets = effective.descriptor.scopes.iter().filter_map(|scope| scope.target.clone()).collect::<Vec<_>>();
        if approval_context.principal_id != principal || approval_context.session_id.as_deref() != Some(context.actor.session_id.as_str())
            || approval_context.turn_id.as_deref() != Some(context.actor.turn_id.as_str())
            || approval_context.execution_id.as_deref() != Some(ticket.graph_id.as_str())
            || approval_context.invocation_id.as_deref() != Some(authorization.request_id.as_str())
            || approval_context.policy_revision != authorization.policy_revision
            || approval_context.capability != request.tool_name
            || approval_context.effect.as_ref() != Some(&effective.descriptor)
            || approval_context.resource_targets != targets {
            return Err("single review original approval does not bind this call".into());
        }
        let (original_grant, _) = crate::ApprovalQueue::original_exact_once_grant_at_cut(
            store, &context.actor.session_id, &grant.approval_id, &grant.grant_id, approval_revision.committed_revision,
            assessment_event.commit_cursor, &approval_context, risk)?;
        if original_grant != grant { return Err("single review original approval grant differs".into()); }
        Some(AuthenticatedApprovalRef { approval_id: grant.approval_id, grant_id: grant.grant_id,
            context: approval_context, risk })
    };
    let policy_request = crate::AuthorizationRequest {
        principal_id: principal, capability: request.tool_name.clone(), input: request.input.clone(),
        idempotency_key: authorization.request_id.clone(), effect: invocation.effect.clone(),
        parent_ceiling: assessed.parent_ceiling, parent_lease_id,
        policy_revision: authorization.policy_revision, recovery_scope: format!("turn:{}", context.actor.turn_id),
        context: crate::permissions::PermissionContext::new(decision, reason),
        safe_alternatives: crate::authorization_negotiator::safe_alternatives_for_effect(invocation.effect.effect_kind),
    };
    let lease_source = lease_control.source();
    if sources.insert(lease_source.stream_id.clone(), lease_source.expected_revision)
        .is_some_and(|old| old != lease_source.expected_revision) {
        return Err("single review physical and lease sources conflict".into());
    }
    let current_fences = sources.into_iter().map(|(stream_id, expected_revision)|
        crate::ExpectedStreamRevision { stream_id, expected_revision }).collect();
    Ok(AuthenticatedHistoricalPolicyCall {
        owner: AuthenticatedPolicyOwner::Root(AuthenticatedRootCall {
            original_action: action.clone(), original_receipt: physical.clone(), original_owner: intent,
        }), request: policy_request, original_authorization: Some(authorization.clone()), approval,
        receipt_cursor: physical.commit_cursor, current_fences,
        original_action: action.clone(), original_assessment: assessment_event.clone(),
        original_receipt: physical, original_ticket: Some(ticket), original_packet: None,
    })
}

/// Authenticate the original Root batch member from its same checked action,
/// index, authority events, and the actual completed outer call. A member
/// payload locates those facts; it cannot grant authority by itself.
pub(crate) fn authenticate_original_root_member_review(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    action: &crate::DurableRuntimeEvent,
    context: &CurrentReviewContext<'_>,
    _proof_cuts: &std::collections::BTreeMap<String, u64>,
) -> Result<AuthenticatedHistoricalPolicyCall, String> {
    let facts = action.payload.get("member_admission").ok_or("Root member original admission missing")?;
    let hook = &facts["original_hook"];
    let request: crate::RuntimeToolExecutionRequest = serde_json::from_value(hook["outer_request"].clone())
        .map_err(|_| "Root member original outer request malformed")?;
    let ticket: crate::execution_core::graph::NodeExecutionTicket = serde_json::from_value(facts["ticket"].clone())
        .map_err(|_| "Root member original ticket malformed")?;
    let step = request.step_binding.as_ref().ok_or("Root member original Step missing")?;
    let prepared = &facts["prepared"];
    let position = prepared["index"].as_u64().and_then(|n| usize::try_from(n).ok())
        .ok_or("Root member index invalid")?;
    if hook["owner"] != "host" || request.tool_name != "agent_action_batch"
        || request.session_id.as_deref() != Some(context.actor.session_id.as_str())
        || step.snapshot.identity.session_id != context.actor.session_id || step.snapshot.identity.turn_id != context.actor.turn_id
        || request.idempotency_key != step.effect_idempotency_key() || request.tool_use_id != step.origin.call_id
        || context.action_id != format!("{}:{position}", request.idempotency_key)
        || prepared["outer_key"] != request.idempotency_key || prepared["tool"].as_str() != review_control_tool(context.action)
        || hook["step"] != serde_json::to_value(step).map_err(|e| e.to_string())?
        || hook["effective_prepared"] != *prepared {
        return Err("Root member original call, Step or hook owner differs".into());
    }
    let authority = original_checked_member_authority_events(store, action, &request)?;
    let source = format!("execution-effect:{}", request.idempotency_key);
    let intent = store.event_by_idempotency_key(&source, &format!("{}:intent", request.idempotency_key))
        .map_err(|e| e.to_string())?.ok_or("Root member original intent missing")?;
    let primary = store.event_by_idempotency_key(&source, &format!("{}:receipt", request.idempotency_key))
        .map_err(|e| e.to_string())?.ok_or("Root member original receipt missing")?;
    let graph = crate::execution_core::graph::validate_node_ticket_start(store, &ticket, Some(&intent))
        .map_err(|e| e.to_string())?;
    let physical = context.actor.original_execution.as_ref().ok_or("Root member physical identity missing")?;
    let objective = harness_contract::agent_action::root_objective_id(&context.actor.session_id, &context.actor.turn_id);
    if graph.parent_execution.is_some() || context.actor.kind != harness_contract::agent_action::AgentActorKind::Root
        || context.actor.actor_id != format!("root:{}", context.actor.session_id)
        || context.actor.agent_id.is_some() || context.actor.team_id.is_some() || context.actor.execution_id.is_some()
        || context.actor.root_execution_id.as_deref() != Some(graph.id.as_str())
        || context.actor.objective_id != objective
        || context.actor.program_id != harness_contract::agent_action::program_id_for_objective(&objective)
        || physical.graph_id != ticket.graph_id || physical.node_id != ticket.node_id || physical.attempt != ticket.attempt
        || graph.lineage.as_ref().is_none_or(|lineage| lineage.session_id != context.actor.session_id || lineage.turn_id != context.actor.turn_id)
        || intent.commit_cursor >= action.commit_cursor || action.commit_cursor >= primary.commit_cursor {
        return Err("Root member physical owner or ordering differs".into());
    }
    let mut physical_cuts = std::collections::BTreeMap::new();
    let index = if request.parent_execution.is_some() {
        original_node_review_index(store, &request, &ticket, &intent, &primary, &mut physical_cuts)?
    } else {
        super::results::authenticate_root_receipt_owner(store, &primary, &mut physical_cuts)?;
        let (owner, attempt, _) = super::results::original_root_effect_owner(&primary)?;
        let node = owner.node_id.as_deref().ok_or("Root member physical node missing")?;
        if owner.root_execution_id != ticket.graph_id || node != ticket.node_id || attempt != ticket.attempt
            || owner.session_id != context.actor.session_id || owner.turn_id != context.actor.turn_id {
            return Err("Root member original physical owner differs".into());
        }
        let index_source = format!("execution-agent-receipts:{}:{node}:{attempt}", owner.root_execution_id);
        store.event_by_idempotency_key(&index_source, &format!("agent-tool-receipt:{}", request.idempotency_key))
            .map_err(|e| e.to_string())?.ok_or("Root member physical index missing")?
    };
    let receipt = store.original_transaction_receipt(&primary.transaction_id).map_err(|e| e.to_string())?
        .ok_or("Root member physical transaction missing")?;
    if receipt.transaction_id != primary.transaction_id || receipt.commit_cursor != primary.commit_cursor
        || index.transaction_id != primary.transaction_id || index.commit_cursor != primary.commit_cursor
        || receipt.event_ids.get(primary.transaction_index as usize) != Some(&primary.event_id)
        || receipt.event_ids.get(index.transaction_index as usize) != Some(&index.event_id)
        || !receipt.stream_revisions.iter().any(|source| source.stream_id == primary.stream_id
            && source.expected_revision < primary.sequence && source.committed_revision >= primary.sequence)
        || !receipt.stream_revisions.iter().any(|source| source.stream_id == index.stream_id
            && source.expected_revision < index.sequence && source.committed_revision >= index.sequence)
        || index.transaction_index <= primary.transaction_index {
        return Err("Root member original physical receipt/index differs".into());
    }
    let input = harness_contract::step::parse_strict_json(&request.input).map_err(|_| "Root batch original input malformed")?;
    let invocation = request.compiled_invocation.as_ref().ok_or("Root batch invocation missing")?;
    let digest = harness_contract::step::canonical_digest("tool-effect-payload", &invocation.intent.normalized_input)
        .map_err(|e| e.to_string())?;
    if input != invocation.intent.normalized_input || primary.payload["replay"]["origin"] != serde_json::to_value(&step.origin).map_err(|e| e.to_string())?
        || primary.payload["replay"]["call_id"] != request.tool_use_id || primary.payload["replay"]["payload_digest"] != digest
        || primary.payload["replay"]["invocation"]["effect"] != serde_json::to_value(&invocation.effect).map_err(|e| e.to_string())? {
        return Err("Root batch original physical input or outcome differs".into());
    }
    let original = input["actions"].as_array().and_then(|actions| actions.get(position))
        .ok_or("Root member absent from original outer input")?;
    let mut original_input = original.get("input").cloned().unwrap_or_else(|| serde_json::json!({}));
    let original_object = original_input.as_object_mut().ok_or("Root member original input is not an object")?;
    for field in ["reference", "evidence_ref"] {
        if let Some(raw) = original_object.get(field).and_then(Value::as_str) {
            original_object.insert(field.into(), Value::String(crate::normalize_bare_evidence_ref(raw)));
        }
    }
    for field in ["evidence_refs", "result_refs", "artifact_refs"] {
        if let Some(values) = original_object.get_mut(field).and_then(Value::as_array_mut) {
            for value in values { if let Some(raw) = value.as_str() {
                *value = Value::String(crate::normalize_bare_evidence_ref(raw));
            }}
        }
    }
    if original["tool"] != prepared["tool"] || hook["original_prepared"]["outer_key"] != prepared["outer_key"]
        || hook["original_prepared"]["index"] != prepared["index"] || hook["original_prepared"]["tool"] != prepared["tool"]
        || hook["original_prepared"]["input"] != original_input {
        return Err("Root member original outer action differs".into());
    }
    let mut normalized = prepared["input"].clone();
    normalized.as_object_mut().ok_or("Root member effective input is not an object")?.remove("expected_revision");
    let resolved: AgentAction = serde_json::from_value(serde_json::json!({"kind":prepared["tool"], "input":normalized}))
        .map_err(|_| "Root member effective action malformed")?;
    if &resolved != context.action { return Err("Root member effective action differs".into()); }
    authenticate_closed_node_review_member(store, &ticket, None, &request.idempotency_key, position,
        action, context, &mut physical_cuts)?;
    let matching: Vec<_> = authority.events.iter().filter(|event| event.kind == "authorization.capability_assessed").collect();
    if matching.len() != 1 { return Err("Root member original assessment missing or ambiguous".into()); }
    let binding = &matching[0].payload["_original_batch_member_binding"];
    if matching[0].actor.as_deref() != Some("conversation_runtime") || binding["version"] != 2
        || [("execution", ticket.graph_id.as_str()), ("session", context.actor.session_id.as_str()),
            ("turn", context.actor.turn_id.as_str())].iter().any(|(kind, id)|
            !matching[0].refs.iter().any(|reference| reference.kind == *kind && reference.id == *id))
        || binding["ticket"] != facts["ticket"] || binding["step"] != hook["step"]
        || binding["outer_key"] != prepared["outer_key"] || binding["index"] != prepared["index"]
        || binding["member_identity"] != prepared["identity"] || binding["member_tool"] != prepared["tool"]
        || binding["member_input"] != prepared["input"] || binding["member_effect"] != prepared["effect"]
        || binding["original_prepared"] != hook["original_prepared"]
        || binding["outer_input"].as_str() != Some(request.input.as_str())
        || binding["outer_invocation"] != serde_json::to_value(&request.compiled_invocation).map_err(|e| e.to_string())?
        || matching[0].payload["_original_permission_context"] != hook["permission_context"] {
        return Err("Root member assessment and original hook differ".into());
    }
    let assessed: harness_contract::policy::CapabilityAssessment = serde_json::from_value(matching[0].payload.clone())
        .map_err(|_| "Root member original assessment malformed")?;
    let authorization: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(facts["authorization"].clone())
        .map_err(|_| "Root member original authorization malformed")?;
    let descriptor: harness_contract::tool::ToolEffectDescriptor = serde_json::from_value(prepared["effect"].clone())
        .map_err(|_| "Root member effect malformed")?;
    let effective_input = binding["normalized_effective"].as_str().ok_or("Root member normalized input missing")?;
    let normalized = harness_contract::step::parse_strict_json(effective_input).map_err(|_| "Root member normalized input malformed")?;
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(&descriptor, effective_input);
    let lease = &authorization.authorization_lease;
    let assessed_lease = assessed.lease.as_ref().ok_or("Root member assessment lease missing")?;
    if normalized != prepared["input"] || authorization.request_id != prepared["identity"]
        || authorization.tool_id != prepared["tool"] || authorization.descriptor_hash != effective.descriptor.descriptor_hash
        || assessed.capability != prepared["tool"] || assessed.effect != effective.descriptor.assessment
        || assessed.requested_scopes != effective.descriptor.scopes || assessed.required_mode != effective.descriptor.required_permission
        || assessed.risk != crate::authorization_negotiator::risk_for_effect(&effective.descriptor.assessment)
        || assessed.gap.is_some()
        || !matches!(assessed.path, harness_contract::policy::AuthorizationPath::ExistingLease
            | harness_contract::policy::AuthorizationPath::StandingGrant
            | harness_contract::policy::AuthorizationPath::PolicyAutoGrant
            | harness_contract::policy::AuthorizationPath::HumanApproval)
        || !assessed_lease.permits(&assessed.capability, assessed.required_mode)
        || !assessed.requested_scopes.iter().all(|scope| assessed_lease.scopes.contains(scope))
        || !crate::AuthorizationNegotiator::verify_lease_signature(lease)
        || !crate::AuthorizationNegotiator::verify_lease_signature(assessed_lease)
        || assessed_lease.signature != lease.signature
        || crate::authorization_negotiator::authorization_lease_signed_payload(assessed_lease)
            != crate::authorization_negotiator::authorization_lease_signed_payload(lease) {
        return Err("Root member original assessment, effect or lease differs".into());
    }
    let (issuer, consumption) = validate_checked_member_lease_events(store, &authority, &authorization, matching[0])?;
    let memory = request.memory_context.as_ref().ok_or("Root member original memory identity missing")?;
    if memory.session_id != context.actor.session_id || memory.turn_ref.as_deref() != Some(context.actor.turn_id.as_str())
        || memory.agent_id.trim().is_empty() {
        return Err("Root member original memory identity differs".into());
    }
    let (principal, parent_lease_id) = crate::conversation::original_memory_authorization_identity(
        &memory.session_id, &memory.agent_id);
    if lease.principal_id != principal || lease.parent_lease_id != parent_lease_id {
        return Err("Root member original lease identity differs".into());
    }
    let lease_evidence = AuthenticatedMemberLeaseEvidence {
        store: store.clone(), action: action.clone(), index: authority.index.clone(), transaction: authority.receipt.clone(),
        issuer, consume: consumption, assessment: matching[0].clone(),
        effect_receipt: primary.clone(), authorization: authorization.clone(), principal_id: lease.principal_id.clone(),
    };
    let original_hook_input = binding["hook_effective"].as_str().ok_or("Root member original hook input missing")?;
    if harness_contract::step::parse_strict_json(original_hook_input).map_err(|_| "Root member hook input malformed")?
        != hook["hook_effective_input"] {
        return Err("Root member original hook input differs".into());
    }
    let lease_control = crate::authorization_negotiator::capture_member_proof_lease_control(store, &lease_evidence)?;
    if lease_control.explicit_revocation().is_some() {
        return Err("Root member original lease explicitly revoked; relevance unknown".into());
    }
    let approval = if !matching[0].payload["_original_approval_grant"].is_null() {
        let grant: harness_contract::policy::ApprovalGrant = serde_json::from_value(matching[0].payload["_original_approval_grant"].clone())
            .map_err(|_| "Root member original approval malformed")?;
        let consumes: Vec<_> = authority.events.iter().filter(|event| event.kind == "approval.grant_consumed"
            && event.payload["grant_id"].as_str() == Some(grant.grant_id.as_str())).collect();
        if consumes.len() != 1 { return Err("Root member approval consume missing or ambiguous".into()); }
        let consume = consumes[0];
        let original_cut = consume.sequence.checked_sub(1).filter(|cut| *cut > 0).ok_or("Root member original approval cut missing")?;
        let (original, _) = crate::ApprovalQueue::decision_result_at_cut(store, &context.actor.session_id, &grant.approval_id, original_cut)?;
        let approval_context = original.context;
        let risk = crate::approval_coordinator::task_risk_for_effect(&effective.descriptor);
        let targets = effective.descriptor.scopes.iter().filter_map(|scope| scope.target.clone()).collect::<Vec<_>>();
        if assessed.path != harness_contract::policy::AuthorizationPath::HumanApproval
            || approval_context.principal_id != lease.principal_id
            || approval_context.session_id.as_deref() != Some(context.actor.session_id.as_str())
            || approval_context.turn_id.as_deref() != Some(context.actor.turn_id.as_str())
            || approval_context.invocation_id.as_deref() != Some(authorization.request_id.as_str())
            || approval_context.execution_id.as_deref() != Some(ticket.graph_id.as_str())
            || approval_context.capability != authorization.tool_id || approval_context.policy_revision != authorization.policy_revision
            || approval_context.effect.as_ref() != Some(&effective.descriptor) || approval_context.resource_targets != targets {
            return Err("Root member original approval is not bound to this call".into());
        }
        let evidence = AuthenticatedMemberApprovalEvidence {
            store: store.clone(), action: action.clone(), index: authority.index.clone(), transaction: authority.receipt.clone(),
            consume: consume.clone(), assessment: matching[0].clone(), effect_receipt: primary.clone(),
            context: approval_context, risk,
        };
        let approval_source = format!("approval:{}", grant.approval_id);
        let mut sources = authority.receipt.stream_revisions.iter().filter(|source| source.stream_id == approval_source);
        let authorization_cut = sources.next().ok_or("Root member original approval transaction source missing")?.expected_revision;
        if sources.next().is_some() || authorization_cut != original_cut {
            return Err("Root member original approval authorization cut differs".into());
        }
        let (original_grant, _) = crate::ApprovalQueue::original_exact_once_grant_from_authenticated_member(
            store, &evidence, &grant.approval_id, &grant.grant_id)?;
        if original_grant != grant { return Err("Root member original approval grant differs".into()); }
        Some(AuthenticatedApprovalRef { approval_id: grant.approval_id, grant_id: grant.grant_id,
            context: evidence.context.clone(), risk })
    } else {
        if assessed.path == harness_contract::policy::AuthorizationPath::HumanApproval {
            return Err("Root member original human approval missing".into());
        }
        None
    };
    let original_context = hook["permission_context"].as_object().ok_or("Root member original permission context malformed")?;
    if original_context.len() != 2 || !original_context.contains_key("override_decision")
        || !original_context.contains_key("override_reason") { return Err("Root member permission context shape differs".into()); }
    let decision = match &original_context["override_decision"] {
        Value::Null => None,
        Value::String(value) if value == "allow" => Some(crate::permissions::PermissionOverride::Allow),
        Value::String(value) if value == "deny" => Some(crate::permissions::PermissionOverride::Deny),
        Value::String(value) if value == "ask" => Some(crate::permissions::PermissionOverride::Ask),
        _ => return Err("Root member permission override invalid".into()),
    };
    let reason = match &original_context["override_reason"] {
        Value::Null => None, Value::String(value) => Some(value.clone()),
        _ => return Err("Root member permission reason invalid".into()),
    };
    let policy_request = crate::AuthorizationRequest {
        principal_id: lease.principal_id.clone(), capability: authorization.tool_id.clone(), input: effective_input.to_owned(),
        idempotency_key: authorization.request_id.clone(), effect: descriptor.clone(),
        parent_ceiling: assessed.parent_ceiling, parent_lease_id,
        policy_revision: authorization.policy_revision, recovery_scope: format!("turn:{}", context.actor.turn_id),
        context: crate::permissions::PermissionContext::new(decision, reason),
        safe_alternatives: crate::authorization_negotiator::safe_alternatives_for_effect(descriptor.effect_kind),
    };
    Ok(AuthenticatedHistoricalPolicyCall {
        owner: AuthenticatedPolicyOwner::Root(AuthenticatedRootCall {
            original_action: action.clone(), original_receipt: primary.clone(), original_owner: intent,
        }), request: policy_request, original_authorization: Some(authorization), approval,
        receipt_cursor: primary.commit_cursor, current_fences: {
            if physical_cuts.insert(lease_control.source().stream_id.clone(), lease_control.source().expected_revision)
                .is_some_and(|old| old != lease_control.source().expected_revision) {
                return Err("Root member closure and lease sources conflict".into());
            }
            physical_cuts.into_iter().map(|(stream_id, expected_revision)| crate::ExpectedStreamRevision {
                stream_id, expected_revision,
            }).collect()
        },
        original_action: action.clone(), original_assessment: matching[0].clone(), original_receipt: primary,
        original_ticket: Some(ticket), original_packet: None,
    })
}

/// The original call assessment, recovered only after the exact Process
/// call and its completed receipt have been authenticated. It records the
/// historical hook context; it is not current authorization.
pub(crate) struct OriginalProcessReviewAssessment {
    pub(super) context: crate::permissions::PermissionContext,
    pub(super) assessment: harness_contract::policy::CapabilityAssessment,
    pub(super) original_input: String,
    approval: Option<AuthenticatedApprovalRef>,
    pub(super) event: crate::DurableRuntimeEvent,
    member: Option<AuthenticatedProcessMemberCall>,
}
struct AuthenticatedProcessMemberCall {
    authorization: harness_contract::tool::ToolExecutionAuthorization,
    effect: harness_contract::tool::ToolEffectDescriptor,
    ticket: crate::execution_core::graph::NodeExecutionTicket,
    current_fence: crate::ExpectedStreamRevision,
}

/// Read the new member's exact authority events from the original member
/// index and checked transaction receipt. An index payload is only a
/// locator; the original transaction owns every returned event.
struct OriginalCheckedMemberAuthority {
    index: crate::DurableRuntimeEvent,
    receipt: crate::AppendTransactionReceipt,
    events: Vec<crate::DurableRuntimeEvent>,
}

fn original_checked_member_authority_events(
    store: &crate::RuntimeEventStore,
    action: &crate::DurableRuntimeEvent,
    request: &crate::RuntimeToolExecutionRequest,
) -> Result<OriginalCheckedMemberAuthority, String> {
    let facts = &action.payload["member_admission"];
    let prepared = &facts["prepared"];
    let outer = prepared["outer_key"].as_str().filter(|value| !value.is_empty())
        .ok_or("member original outer key missing")?;
    let ordinal = prepared["index"].as_u64().ok_or("member original ordinal missing")?;
    let identity = prepared["identity"].as_str().filter(|value| !value.is_empty())
        .ok_or("member original identity missing")?;
    if outer != request.idempotency_key || ordinal.to_string().parse::<usize>().is_err() {
        return Err("member original outer call differs".into());
    }
    let stream = format!("execution-effect:{outer}");
    let index = store.event_by_idempotency_key(&stream, &format!("agentic-member:{ordinal}"))
        .map_err(|error| error.to_string())?.ok_or("original checked member index missing")?;
    let receipt = store.original_transaction_receipt(&action.transaction_id)
        .map_err(|error| error.to_string())?.ok_or("original checked member receipt missing")?;
    if index.stream_id != stream || index.kind != "agentic.batch_member.index"
        || index.scope != crate::RuntimeEventScope::ExecutionNode || index.schema_version != 1
        || index.status.as_deref() != Some("committed") || index.actor.as_deref() != Some("runtime.agentic_action")
        || index.transaction_id != action.transaction_id || index.commit_cursor != action.commit_cursor
        || receipt.transaction_id != action.transaction_id || receipt.commit_cursor != action.commit_cursor
        || receipt.event_ids.get(action.transaction_index as usize) != Some(&action.event_id)
        || receipt.event_ids.get(index.transaction_index as usize) != Some(&index.event_id)
        || action.payload["envelope"]["action_id"] != format!("{outer}:{ordinal}")
        || index.payload["outer_key"] != outer || index.payload["member_index"].as_u64() != Some(ordinal)
        || index.payload["member_identity"] != identity || index.payload["ticket"] != facts["ticket"]
        || index.payload["program_stream"] != action.stream_id || index.payload["program_event_kind"] != action.kind
        || index.payload["program_event_index"].as_u64() != Some(u64::from(action.transaction_index))
        || index.payload["program_event_key"].as_str() != action.idempotency_key.as_deref() {
        return Err("original checked member index/action/receipt differs".into());
    }
    let index_revision = store.transaction_stream_revision_exact(&action.transaction_id, &stream)
        .map_err(|error| error.to_string())?.ok_or("original member index source missing")?;
    if index_revision.expected_revision >= index.sequence || index_revision.committed_revision < index.sequence
        || !receipt.stream_revisions.iter().any(|revision| revision == &index_revision) {
        return Err("original member index source revision differs".into());
    }
    let mut positions = BTreeSet::new();
    let mut events = Vec::new();
    let references = index.payload["authority_events"].as_array().filter(|events| !events.is_empty())
        .ok_or("original checked member authority coordinates missing")?;
    for reference in references {
        let source = reference["stream_id"].as_str().filter(|value| !value.is_empty())
            .ok_or("member authority source missing")?;
        let key = reference["idempotency_key"].as_str().filter(|value| !value.is_empty())
            .ok_or("member authority key missing")?;
        let position = reference["transaction_index"].as_u64().and_then(|value| usize::try_from(value).ok())
            .ok_or("member authority position invalid")?;
        if !positions.insert(position) || position == action.transaction_index as usize
            || position == index.transaction_index as usize { return Err("member authority position duplicated".into()); }
        let event = store.event_by_idempotency_key(source, key).map_err(|error| error.to_string())?
            .ok_or("original checked member authority event missing")?;
        if event.stream_id != source || event.idempotency_key.as_deref() != Some(key)
            || event.kind.as_str() != reference["kind"].as_str().ok_or("member authority kind missing")?
            || event.transaction_id != action.transaction_id || event.commit_cursor != action.commit_cursor
            || event.transaction_index as usize != position || receipt.event_ids.get(position) != Some(&event.event_id) {
            return Err("member authority does not belong to original checked transaction".into());
        }
        let source_revision = store.transaction_stream_revision_exact(&action.transaction_id, source)
            .map_err(|error| error.to_string())?.ok_or("member authority transaction source missing")?;
        if source_revision.stream_id != source || source_revision.expected_revision >= event.sequence
            || source_revision.committed_revision < event.sequence
            || !receipt.stream_revisions.iter().any(|revision| revision == &source_revision) {
            return Err("member authority source revision differs".into());
        }
        events.push(event);
    }
    Ok(OriginalCheckedMemberAuthority { index, receipt, events })
}

fn validate_checked_member_lease_events(
    store: &crate::RuntimeEventStore,
    authority: &OriginalCheckedMemberAuthority,
    member_auth: &harness_contract::tool::ToolExecutionAuthorization,
    assessment: &crate::DurableRuntimeEvent,
) -> Result<(crate::DurableRuntimeEvent, crate::DurableRuntimeEvent), String> {
    let lease = &member_auth.authorization_lease;
    let source = format!("authorization-lease:{}", lease.lease_id);
    let issuer = store.list_stream_after(&source, 0, 1, 1, 4 * 1024 * 1024)?
        .into_iter().next().ok_or("original member lease issuer missing")?;
    let issuer_tx = store.original_transaction_receipt(&issuer.transaction_id).map_err(|e| e.to_string())?
        .ok_or("original member lease issuer receipt missing")?;
    let mut consumptions = authority.events.iter().filter(|event|
        event.kind == "authorization.lease_transition" && event.status.as_deref() == Some("consumed")
            && event.stream_id == source);
    let consume = consumptions.next().ok_or("original member lease consumption missing")?;
    if consumptions.next().is_some() { return Err("original member lease consumption ambiguous".into()); }
    let issued: harness_contract::policy::AuthorizationLeaseTransition = serde_json::from_value(issuer.payload.clone())
        .map_err(|_| "original member lease issuer malformed")?;
    let used: harness_contract::policy::AuthorizationLeaseTransition = serde_json::from_value(consume.payload.clone())
        .map_err(|_| "original member lease consumption malformed")?;
    let member_source = authority.receipt.stream_revisions.iter().find(|revision| revision.stream_id == source)
        .ok_or("original member lease source missing")?;
    if issuer.stream_id != source || issuer.sequence != 1 || issuer.kind != "authorization.lease_transition"
        || issuer.scope != crate::RuntimeEventScope::Tool || issuer.schema_version != 1
        || issuer.status.as_deref() != Some("issued") || issued.kind != harness_contract::policy::AuthorizationLeaseTransitionKind::Issued
        || issuer.idempotency_key.as_deref() != Some(issued.transition_id.as_str())
        || issuer_tx.transaction_id != issuer.transaction_id || issuer_tx.commit_cursor != issuer.commit_cursor
        || issuer_tx.event_ids.get(issuer.transaction_index as usize) != Some(&issuer.event_id)
        || !issuer_tx.stream_revisions.iter().any(|revision| revision.stream_id == source
            && revision.expected_revision < issuer.sequence && revision.committed_revision >= issuer.sequence)
        || consume.transaction_id != authority.receipt.transaction_id || consume.commit_cursor != authority.receipt.commit_cursor
        || consume.transaction_index >= assessment.transaction_index
        || used.kind != harness_contract::policy::AuthorizationLeaseTransitionKind::Consumed
        || used.idempotency_key.as_deref() != Some(member_auth.request_id.as_str())
        || consume.idempotency_key.as_deref() != Some(used.transition_id.as_str())
        || issuer.commit_cursor > consume.commit_cursor
        || (issuer.transaction_id == consume.transaction_id && issuer.transaction_index >= consume.transaction_index)
        || (issuer.transaction_id != consume.transaction_id
            && (issuer.commit_cursor >= consume.commit_cursor || issuer.sequence > member_source.expected_revision))
        || issuer.sequence >= consume.sequence || issued.lease.signature != lease.signature || used.lease.signature != lease.signature
        || crate::authorization_negotiator::authorization_lease_signed_payload(&issued.lease)
            != crate::authorization_negotiator::authorization_lease_signed_payload(lease)
        || crate::authorization_negotiator::authorization_lease_signed_payload(&used.lease)
            != crate::authorization_negotiator::authorization_lease_signed_payload(lease)
        || !crate::AuthorizationNegotiator::verify_lease_signature(&issued.lease)
        || !crate::AuthorizationNegotiator::verify_lease_signature(&used.lease) {
        return Err("original member lease issuer, consume or transaction differs".into());
    }
    Ok((issuer, consume.clone()))
}

pub(crate) fn authenticate_original_process_review_assessment(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    completed: &CompletedProcessReviewWitness,
    proof_cuts: &std::collections::BTreeMap<String, u64>,
) -> Result<OriginalProcessReviewAssessment, String> {
    use sha2::{Digest as _, Sha256};
    let request = &completed.request;
    let session = request.session_id.as_deref().ok_or("review assessment Session missing")?;
    let source = format!("session:{session}");
    let cut = require_original_cut(store, &source, proof_cuts)?;
    let step = request.step_binding.as_ref().ok_or("review assessment Step missing")?;
    if request.tool_name == "agent_action_batch" {
        let authority = original_checked_member_authority_events(store, &completed.action, request)?;
        let facts = &completed.action.payload["member_admission"];
        let prepared = &facts["prepared"];
        let hook = &facts["original_hook"];
        let member_auth: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(facts["authorization"].clone())
            .map_err(|_| "Process member original authorization malformed")?;
        let ticket: crate::execution_core::graph::NodeExecutionTicket = serde_json::from_value(facts["ticket"].clone())
            .map_err(|_| "Process member original ticket malformed")?;
        if ticket != completed.parent_ticket || hook["owner"] != "process"
            || hook["step"] != serde_json::to_value(step).map_err(|e| e.to_string())?
            || hook["outer_request"] != serde_json::to_value(request).map_err(|e| e.to_string())?
            || hook["effective_prepared"] != *prepared {
            return Err("Process member original ticket, Step or hook differs".into());
        }
        let assessments: Vec<_> = authority.events.iter().filter(|event|
            event.kind == "authorization.capability_assessed"
                && event.payload.pointer("/lease/lease_id").and_then(Value::as_str)
                    == Some(member_auth.authorization_lease.lease_id.as_str())).collect();
        if assessments.len() != 1 { return Err("Process member original assessment missing or ambiguous".into()); }
        let event = assessments[0];
        let binding = &event.payload["_original_batch_member_binding"];
        let effective_input = binding["normalized_effective"].as_str().ok_or("Process member normalized input missing")?;
        let descriptor: harness_contract::tool::ToolEffectDescriptor = serde_json::from_value(prepared["effect"].clone())
            .map_err(|_| "Process member original effect malformed")?;
        let assessment: harness_contract::policy::CapabilityAssessment = serde_json::from_value(event.payload.clone())
            .map_err(|_| "Process member original assessment malformed")?;
        let effect = crate::AuthorizationNegotiator::compile_effective_descriptor(&descriptor, effective_input);
        let lease = &member_auth.authorization_lease;
        let assessed_lease = assessment.lease.as_ref().ok_or("Process member assessment lease missing")?;
        let outer_input = harness_contract::step::parse_strict_json(&request.input)
            .map_err(|_| "Process member original outer input malformed")?;
        let ordinal = prepared["index"].as_u64().and_then(|n| usize::try_from(n).ok())
            .ok_or("Process member original index invalid")?;
        let original = outer_input["actions"].as_array().and_then(|actions| actions.get(ordinal))
            .ok_or("Process member absent from original outer input")?;
        let mut original_input = original.get("input").cloned().unwrap_or_else(|| serde_json::json!({}));
        let original_object = original_input.as_object_mut().ok_or("Process member original input is not an object")?;
        for field in ["reference", "evidence_ref"] {
            if let Some(raw) = original_object.get(field).and_then(Value::as_str) {
                original_object.insert(field.into(), Value::String(crate::normalize_bare_evidence_ref(raw)));
            }
        }
        for field in ["evidence_refs", "result_refs", "artifact_refs"] {
            if let Some(values) = original_object.get_mut(field).and_then(Value::as_array_mut) {
                for value in values { if let Some(raw) = value.as_str() {
                    *value = Value::String(crate::normalize_bare_evidence_ref(raw));
                }}
            }
        }
        if original["tool"] != prepared["tool"] || hook["original_prepared"]["outer_key"] != prepared["outer_key"]
            || hook["original_prepared"]["index"] != prepared["index"] || hook["original_prepared"]["tool"] != prepared["tool"]
            || hook["original_prepared"]["input"] != original_input {
            return Err("Process member original outer action differs".into());
        }
        let mut effective_action_input = prepared["input"].clone();
        effective_action_input.as_object_mut().ok_or("Process member effective input is not an object")?.remove("expected_revision");
        let effective_action: AgentAction = serde_json::from_value(serde_json::json!({"kind":prepared["tool"],"input":effective_action_input}))
            .map_err(|_| "Process member effective action malformed")?;
        let envelope: harness_contract::agent_action::AgentActionEnvelope = serde_json::from_value(completed.action.payload["envelope"].clone())
            .map_err(|_| "Process member original envelope malformed")?;
        if envelope.action != effective_action || event.actor.as_deref() != Some("runtime.process_jsonl")
            || event.scope != crate::RuntimeEventScope::Tool || event.schema_version != 1
            || binding["version"] != 2 || binding["step"] != hook["step"] || binding["ticket"] != facts["ticket"]
            || binding["packet"] != serde_json::to_value(&completed.packet).map_err(|e| e.to_string())?
            || binding["outer_invocation"] != serde_json::to_value(&request.compiled_invocation).map_err(|e| e.to_string())?
            || binding["outer_input"].as_str() != Some(request.input.as_str())
            || binding["parent"] != serde_json::to_value(&request.parent_execution).map_err(|e| e.to_string())?
            || binding["attempt"] != serde_json::to_value(request.parent_execution_attempt).map_err(|e| e.to_string())?
            || binding["outer_key"] != prepared["outer_key"] || binding["index"] != prepared["index"]
            || binding["member_tool"] != prepared["tool"] || binding["member_input"] != prepared["input"]
            || binding["member_effect"] != prepared["effect"] || binding["member_identity"] != prepared["identity"]
            || binding["original_prepared"] != hook["original_prepared"]
            || binding["timeout_secs"].as_u64().is_none_or(|timeout| timeout == 0)
            || event.payload["_original_permission_context"] != hook["permission_context"]
            || harness_contract::step::parse_strict_json(binding["hook_effective"].as_str().ok_or("Process hook input missing")?)
                .map_err(|_| "Process hook input malformed")? != hook["hook_effective_input"]
            || harness_contract::step::parse_strict_json(effective_input).map_err(|_| "Process effective input malformed")? != prepared["input"]
            || member_auth.request_id != prepared["identity"] || member_auth.tool_id != prepared["tool"]
            || member_auth.descriptor_hash != effect.descriptor.descriptor_hash
            || assessment.capability != prepared["tool"] || assessment.effect != effect.descriptor.assessment
            || assessment.requested_scopes != effect.descriptor.scopes || assessment.required_mode != effect.descriptor.required_permission
            || assessment.risk != crate::authorization_negotiator::risk_for_effect(&effect.descriptor.assessment)
            || assessment.gap.is_some() || lease.principal_id != format!("agent:{}", completed.packet.agent_id())
            || !matches!(assessment.path, harness_contract::policy::AuthorizationPath::ExistingLease
                | harness_contract::policy::AuthorizationPath::StandingGrant
                | harness_contract::policy::AuthorizationPath::PolicyAutoGrant
                | harness_contract::policy::AuthorizationPath::HumanApproval)
            || member_auth.policy_revision == 0 || member_auth.policy_revision != lease.policy_revision
            || assessed_lease.idempotency_key != member_auth.request_id
            || !assessed_lease.permits(&assessment.capability, assessment.required_mode)
            || !assessment.requested_scopes.iter().all(|scope| assessed_lease.scopes.contains(scope))
            || !crate::AuthorizationNegotiator::verify_lease_signature(lease)
            || !crate::AuthorizationNegotiator::verify_lease_signature(assessed_lease)
            || assessed_lease.signature != lease.signature
            || crate::authorization_negotiator::authorization_lease_signed_payload(assessed_lease)
                != crate::authorization_negotiator::authorization_lease_signed_payload(lease) {
            return Err("Process member original assessment, packet, hook or lease differs".into());
        }
        let (issuer, consume) = validate_checked_member_lease_events(store, &authority, &member_auth, event)?;
        let lease_evidence = AuthenticatedMemberLeaseEvidence {
            store: store.clone(), action: completed.action.clone(), index: authority.index.clone(), transaction: authority.receipt.clone(),
            issuer, consume, assessment: event.clone(), effect_receipt: completed.receipt.clone(),
            authorization: member_auth.clone(), principal_id: lease.principal_id.clone(),
        };
        let lease_control = crate::authorization_negotiator::capture_member_proof_lease_control(store, &lease_evidence)?;
        if lease_control.explicit_revocation().is_some() {
            return Err("Process member original lease explicitly revoked; relevance unknown".into());
        }
        let original_context = event.payload["_original_permission_context"].as_object().ok_or("Process member context missing")?;
        if original_context.len() != 2 || !original_context.contains_key("override_decision") || !original_context.contains_key("override_reason") {
            return Err("Process member context shape differs".into());
        }
        let decision = match &original_context["override_decision"] {
            Value::Null => None, Value::String(v) if v == "allow" => Some(crate::permissions::PermissionOverride::Allow),
            Value::String(v) if v == "deny" => Some(crate::permissions::PermissionOverride::Deny),
            Value::String(v) if v == "ask" => Some(crate::permissions::PermissionOverride::Ask),
            _ => return Err("Process member permission override invalid".into()),
        };
        let reason = match &original_context["override_reason"] {
            Value::Null => None, Value::String(v) => Some(v.clone()),
            _ => return Err("Process member permission reason invalid".into()),
        };
        let approval = if event.payload["_original_approval_grant"].is_null() {
            if assessment.path == harness_contract::policy::AuthorizationPath::HumanApproval {
                return Err("Process member original human approval missing".into());
            }
            None
        } else {
            let grant: harness_contract::policy::ApprovalGrant = serde_json::from_value(event.payload["_original_approval_grant"].clone())
                .map_err(|_| "Process member original grant malformed")?;
            let consumes: Vec<_> = authority.events.iter().filter(|e| e.kind == "approval.grant_consumed"
                && e.payload["grant_id"].as_str() == Some(grant.grant_id.as_str())).collect();
            if consumes.len() != 1 { return Err("Process member original approval consume missing or ambiguous".into()); }
            let consume = consumes[0];
            let before = consume.sequence.checked_sub(1).filter(|cut| *cut > 0).ok_or("Process member approval cut missing")?;
            let (original, _) = crate::ApprovalQueue::decision_result_at_cut(store, session, &grant.approval_id, before)?;
            let approval_context = original.context;
            let risk = crate::agent_in_process_worker::process_approval_task_risk(assessment.risk);
            let targets = effect.descriptor.scopes.iter().filter_map(|scope| scope.target.clone()).collect::<Vec<_>>();
            if approval_context.principal_id != lease.principal_id || approval_context.session_id.as_deref() != Some(session)
                || approval_context.turn_id.as_deref() != completed.packet.assignment.execution_identity.turn_id()
                || approval_context.capability != member_auth.tool_id
                || approval_context.execution_id.as_deref() != Some(completed.packet.graph_id())
                || approval_context.task_id.as_deref() != Some(completed.packet.task_id())
                || approval_context.invocation_id.as_deref() != Some(member_auth.request_id.as_str())
                || approval_context.policy_revision != member_auth.policy_revision
                || approval_context.effect.as_ref() != Some(&effect.descriptor) || approval_context.resource_targets != targets {
                return Err("Process member approval is not bound to original call".into());
            }
            let evidence = AuthenticatedMemberApprovalEvidence {
                store: store.clone(), action: completed.action.clone(), index: authority.index.clone(), transaction: authority.receipt.clone(),
                consume: consume.clone(), assessment: event.clone(), effect_receipt: completed.receipt.clone(),
                context: approval_context.clone(), risk,
            };
            let (original_grant, _) = crate::ApprovalQueue::original_exact_once_grant_from_authenticated_member(
                store, &evidence, &grant.approval_id, &grant.grant_id)?;
            if original_grant != grant { return Err("Process member original approval grant differs".into()); }
            Some(AuthenticatedApprovalRef { approval_id: grant.approval_id, grant_id: grant.grant_id,
                context: approval_context, risk })
        };
        return Ok(OriginalProcessReviewAssessment {
            context: crate::permissions::PermissionContext::new(decision, reason), assessment,
            original_input: effective_input.to_owned(), approval, event: event.clone(),
            member: Some(AuthenticatedProcessMemberCall { authorization: member_auth, effect: descriptor,
                ticket, current_fence: lease_control.source().clone() }),
        });
    }
    let auth = request.authorization.as_ref().ok_or("review assessment authorization missing")?;
    let packet = &completed.packet;
    let expected_step = serde_json::to_value(step).map_err(|error| error.to_string())?;
    let expected_auth = serde_json::to_value(auth).map_err(|error| error.to_string())?;
    let authorization_id = format!("process-tool:{:x}", Sha256::digest(
        serde_json::to_vec(&(packet.graph_id(), packet.attempt, request.tool_use_id.as_str()))
            .map_err(|error| error.to_string())?));
    let mut selected = None;
    let mut after = 0;
    let budget = crate::SourceCheckBudget::default();
    let (mut pages, mut count, mut bytes) = (0usize, 0usize, 0usize);
    while after < cut {
        pages += 1;
        if pages > budget.max_pages { return Err("review assessment page budget exceeded".into()); }
        let page = store.list_stream_after(&source, after, cut, 64, 4 * 1024 * 1024)?;
        if page.is_empty() { return Err("review assessment original cut incomplete".into()); }
        for event in page {
            count += 1;
            bytes = bytes.saturating_add(serde_json::to_vec(&event).map_err(|error| error.to_string())?.len());
            if count > budget.max_events || bytes > budget.max_bytes || event.stream_id != source
                || event.sequence != after + 1 || event.sequence > cut { return Err("review assessment cut invalid or over budget".into()); }
            after = event.sequence;
            if event.kind != "authorization.process_capability_assessed" { continue; }
            if event.payload["process_request_id"] != request.tool_use_id { continue; }
            let policy = &event.payload["policy_request"];
            let original_input = policy["input"].as_str().ok_or("Process original input missing")?;
            let original_value = harness_contract::step::parse_strict_json(original_input)
                .map_err(|_| "Process original input invalid")?;
            let request_value = harness_contract::step::parse_strict_json(&request.input)
                .map_err(|_| "Process bound request input invalid")?;
            let invocation = request.compiled_invocation.as_ref().ok_or("review assessment invocation missing")?;
            if original_value != request_value || original_value != invocation.intent.normalized_input {
                return Err("Process original and bound inputs differ".into());
            }
            if event.scope != crate::RuntimeEventScope::Tool || event.schema_version != 1
                || event.status.as_deref() != Some("authorized") || event.actor.as_deref() != Some("runtime.process_jsonl")
                || event.commit_cursor == 0 || event.commit_cursor >= completed.intent.commit_cursor || event.transaction_index != 0
                || event.payload["version"] != 1 || event.payload["step_binding"] != expected_step
                || event.payload["packet_session_id"] != packet.session_id()
                || event.payload["packet_graph_id"] != packet.graph_id() || event.payload["packet_node_id"] != packet.node_id()
                || event.payload["packet_run_id"] != packet.run_id() || event.payload["packet_agent_id"] != packet.agent_id()
                || event.payload["execution_id"] != packet.graph_id() || event.payload["attempt"].as_u64() != Some(u64::from(packet.attempt))
                || policy["principal_id"] != format!("agent:{}", packet.agent_id())
                || policy["capability"] != request.tool_name
                || policy["idempotency_key"] != authorization_id || auth.request_id != authorization_id
                || policy["effect"] != serde_json::to_value(&invocation.effect).map_err(|error| error.to_string())?
                || policy["parent_ceiling"] != serde_json::to_value(packet.permission_ceiling).map_err(|error| error.to_string())?
                || !policy["parent_lease_id"].is_null() || policy["policy_revision"].as_u64() != Some(auth.policy_revision)
                || policy["recovery_scope"] != format!("execution:{}", packet.graph_id())
                || policy["safe_alternatives"] != serde_json::json!([]) || event.payload["authorization"] != expected_auth
                || event.refs != vec![
                    crate::RuntimeEventRef { kind: "execution".into(), id: packet.graph_id().into() },
                    crate::RuntimeEventRef { kind: "authorization_lease".into(), id: auth.authorization_lease.lease_id.clone() },
                    crate::RuntimeEventRef { kind: "capability".into(), id: request.tool_name.clone() },
                ]
            { return Err("Process original assessment call/packet binding differs".into()); }
            let receipt = store.original_transaction_receipt(&event.transaction_id).map_err(|error| error.to_string())?
                .ok_or("review assessment original transaction receipt missing")?;
            if receipt.transaction_id != event.transaction_id || receipt.commit_cursor != event.commit_cursor
                || event.transaction_index != 0 || receipt.event_ids.as_slice() != [event.event_id.clone()]
                || receipt.stream_revisions.len() != 1
                || receipt.stream_revisions[0].stream_id != source
                || receipt.stream_revisions[0].committed_revision != event.sequence
                || receipt.stream_revisions[0].expected_revision.checked_add(1) != Some(event.sequence) {
                return Err("review assessment original single-event transaction receipt differs".into());
            }
            let original = event.payload.get("permission_context").and_then(Value::as_object)
                .ok_or("review assessment original permission context missing")?;
            if original.len() != 2 || !original.contains_key("override_decision") || !original.contains_key("override_reason") {
                return Err("review assessment permission context shape differs".into());
            }
            let decision = match &original["override_decision"] {
                Value::Null => None,
                Value::String(value) if value == "allow" => Some(crate::permissions::PermissionOverride::Allow),
                Value::String(value) if value == "deny" => Some(crate::permissions::PermissionOverride::Deny),
                Value::String(value) if value == "ask" => Some(crate::permissions::PermissionOverride::Ask),
                _ => return Err("review assessment permission override invalid".into()),
            };
            let reason = match &original["override_reason"] {
                Value::Null => None, Value::String(value) => Some(value.clone()),
                _ => return Err("review assessment permission reason invalid".into()),
            };
            let assessment: harness_contract::policy::CapabilityAssessment = serde_json::from_value(event.payload["assessment"].clone())
                .map_err(|_| "review assessment payload malformed")?;
            let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(&invocation.effect, original_input);
            if event.payload["effective_descriptor"] != serde_json::to_value(&effective.descriptor).map_err(|error| error.to_string())? {
                return Err("Process assessment effective descriptor differs".into());
            }
            let assessment_lease = assessment.lease.as_ref().ok_or("review assessment signed lease missing")?;
            let supplied_lease = &auth.authorization_lease;
            if assessment.capability != request.tool_name || assessment.capability != effective.descriptor.tool_id
                || assessment.effect != effective.descriptor.assessment
                || assessment.requested_scopes != effective.descriptor.scopes
                || assessment.required_mode != effective.descriptor.required_permission
                || assessment.risk != crate::authorization_negotiator::risk_for_effect(&effective.descriptor.assessment)
                || assessment.gap.is_some()
                || !matches!(assessment.path, harness_contract::policy::AuthorizationPath::ExistingLease
                    | harness_contract::policy::AuthorizationPath::StandingGrant
                    | harness_contract::policy::AuthorizationPath::PolicyAutoGrant
                    | harness_contract::policy::AuthorizationPath::HumanApproval)
                || auth.tool_id != request.tool_name || auth.descriptor_hash != effective.descriptor.descriptor_hash
                || auth.policy_revision == 0 || auth.policy_revision != supplied_lease.policy_revision
                || assessment_lease.idempotency_key != auth.request_id
                || supplied_lease.idempotency_key != auth.request_id
                || !crate::AuthorizationNegotiator::verify_lease_signature(assessment_lease)
                || !crate::AuthorizationNegotiator::verify_lease_signature(supplied_lease)
                || crate::authorization_negotiator::authorization_lease_signed_payload(assessment_lease)
                    != crate::authorization_negotiator::authorization_lease_signed_payload(supplied_lease)
                || assessment_lease.signature != supplied_lease.signature
                || step.snapshot.identity.actor_ref.as_deref().is_none_or(|actor|
                    actor.is_empty() || assessment_lease.principal_id != format!("agent:{actor}"))
                || !assessment_lease.permits(&assessment.capability, assessment.required_mode)
                || !assessment.requested_scopes.iter().all(|scope| assessment_lease.scopes.contains(scope))
                || !event.refs.iter().any(|reference| reference.kind == "capability" && reference.id == assessment.capability)
            { return Err("review assessment original lease, principal, scope or path differs".into()); }
            let context = crate::permissions::PermissionContext::new(decision, reason);
            let approval = if event.payload["approval"].is_null() {
                if assessment.path == harness_contract::policy::AuthorizationPath::HumanApproval {
                    return Err("Process original human approval facts missing".into());
                }
                None
            } else {
                if assessment.path != harness_contract::policy::AuthorizationPath::HumanApproval {
                    return Err("Process original approval path differs".into());
                }
                let grant: harness_contract::policy::ApprovalGrant = serde_json::from_value(event.payload["approval"]["grant"].clone())
                    .map_err(|_| "Process original approval grant malformed")?;
                let approval_context: harness_contract::policy::approval::ApprovalContext =
                    serde_json::from_value(event.payload["approval"]["context"].clone())
                        .map_err(|_| "Process original approval context malformed")?;
                let risk: harness_contract::core::TaskRisk = serde_json::from_value(event.payload["approval"]["risk"].clone())
                    .map_err(|_| "Process original approval risk malformed")?;
                let targets = effective.descriptor.scopes.iter().filter_map(|scope| scope.target.clone()).collect::<Vec<_>>();
                if approval_context.principal_id != format!("agent:{}", packet.agent_id())
                    || approval_context.session_id.as_deref() != Some(packet.session_id())
                    || approval_context.execution_id.as_deref() != Some(packet.graph_id())
                    || approval_context.task_id.as_deref() != Some(packet.task_id())
                    || approval_context.invocation_id.as_deref() != Some(authorization_id.as_str())
                    || approval_context.policy_revision != auth.policy_revision
                    || approval_context.effect.as_ref() != Some(&effective.descriptor)
                    || approval_context.resource_targets != targets
                    || risk != crate::agent_in_process_worker::process_approval_task_risk(assessment.risk) {
                    return Err("Process original approval is not bound to this call".into());
                }
                let approval_stream = format!("approval:{}", grant.approval_id);
                let original_cut = *proof_cuts.get(&approval_stream).ok_or("original approval proof cut missing")?;
                let (original_grant, _) = crate::ApprovalQueue::original_exact_once_grant_at_cut(
                    store, session, &grant.approval_id, &grant.grant_id, original_cut,
                    event.commit_cursor, &approval_context, risk,
                )?;
                let (original_request, _) = crate::ApprovalQueue::decision_result_at_cut(
                    store, session, &grant.approval_id, original_cut,
                )?;
                if original_grant != grant || original_request.context != approval_context {
                    return Err("Process original approved grant/context differs from approval owner".into());
                }
                Some(AuthenticatedApprovalRef { approval_id: grant.approval_id, grant_id: grant.grant_id,
                    context: approval_context, risk })
            };
            if selected.replace(OriginalProcessReviewAssessment { context, assessment, original_input: original_input.to_owned(), approval, event, member: None }).is_some() {
                return Err("review assessment original call is ambiguous".into());
            }
        }
    }
    selected.ok_or_else(|| "review assessment original call missing or unsupported".into())
}

/// Recover this review call from the original Process owner. This function is
/// meaningful only after the original applied proof itself has been checked;
/// a caller-supplied proof payload cannot construct the returned private type.
pub(crate) fn authenticate_process_review_policy_call(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &super::super::program::AgenticProgramProjection,
    packet: &AgentTaskPacket,
    context: &CurrentReviewContext<'_>,
    proof_cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Option<AuthenticatedHistoricalPolicyCall>, String> {
    let Some(completed) = authenticate_completed_process_review(store, program, packet, context, proof_cuts)? else { return Ok(None); };
    let assessment = authenticate_original_process_review_assessment(store, &completed, proof_cuts)?;
    let program_source = format!("agentic-program:{}", program.program_id);
    let prior_revision = completed.action.sequence.checked_sub(1).filter(|revision| *revision > 0)
        .ok_or("review original Program prior revision missing")?;
    let opening = store.list_stream_after(&program_source, 0, 1, 1, 1024 * 1024)?
        .into_iter().next().ok_or("review original Program opening missing")?;
    let owner = crate::AgentActionService::new(store.clone());
    let budget = crate::SourceCheckBudget::default();
    let (mut pages, mut count, mut bytes) = (0usize, 0usize, 0usize);
    let original = owner.project_snapshot_bounded(
        &program.program_id, prior_revision, &opening, None,
        |after, through| {
            pages += 1;
            if pages > budget.max_pages { return Err("original review Program page budget exceeded".to_string()); }
            let page = store.list_stream_after(&program_source, after, through, 100, 1024 * 1024)?;
            count = count.saturating_add(page.len());
            for event in &page {
                bytes = bytes.saturating_add(serde_json::to_vec(event).map_err(|error| error.to_string())?.len());
            }
            if count > budget.max_events || bytes > budget.max_bytes {
                return Err("original review Program event budget exceeded".to_string());
            }
            Ok(page)
        },
        |error| error.to_string(),
    )?;
    let binding = packet.agentic_binding.as_ref().ok_or("review original agentic binding missing")?;
    if binding.program_id != original.program_id || binding.agent_id != context.actor.actor_id
        || context.actor.agent_id.as_deref() != Some(binding.agent_id.as_str())
        || context.actor.team_id.as_deref() != Some(binding.team_id.as_str())
        || binding.membership_id != super::super::program::AgenticProgramProjection::membership_id(&binding.agent_id, &binding.team_id)
        || !original.agent_is_active_in(&binding.agent_id, &binding.team_id)
        || packet.binding.as_ref().is_none_or(|physical|
            crate::agent::binding::recompute_binding_digest(physical).map_or(true, |digest| digest != physical.binding_digest)) {
        return Err("review original actor/membership/binding does not authenticate".into());
    }
    let request = &completed.request;
    let (authorization, effect, ticket, member_fence) = if let Some(member) = assessment.member {
        (member.authorization, member.effect, member.ticket, Some(member.current_fence))
    } else {
        (request.authorization.clone().ok_or("review original authorization missing")?,
            request.compiled_invocation.as_ref().ok_or("review compiled invocation missing")?.effect.clone(),
            completed.child_ticket.clone(), None)
    };
    let mut current = completed.source_fences.clone();
    if let Some(source) = member_fence {
        if current.insert(source.stream_id, source.expected_revision).is_some_and(|old| old != source.expected_revision) {
            return Err("review member current lease source changed".into());
        }
    }
    let current_fences = current.into_iter().map(|(stream_id, expected_revision)|
        crate::ExpectedStreamRevision { stream_id, expected_revision }).collect();
    let policy_request = crate::AuthorizationRequest {
        principal_id: format!("agent:{}", packet.agent_id()), capability: authorization.tool_id.clone(),
        input: assessment.original_input, idempotency_key: authorization.request_id.clone(), effect,
        parent_ceiling: packet.permission_ceiling, parent_lease_id: None,
        policy_revision: authorization.policy_revision, recovery_scope: format!("execution:{}", packet.graph_id()),
        context: assessment.context, safe_alternatives: Vec::new(),
    };
    Ok(Some(AuthenticatedHistoricalPolicyCall {
        owner: AuthenticatedPolicyOwner::Process(packet.clone()), request: policy_request, original_authorization: Some(authorization), approval: assessment.approval,
        receipt_cursor: completed.receipt.commit_cursor, current_fences,
        original_action: completed.action, original_assessment: assessment.event,
        original_receipt: completed.receipt, original_ticket: Some(ticket),
        original_packet: Some(completed.packet),
    }))
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
        && actor.turn_id
            == packet
                .assignment
                .execution_identity
                .turn_id()
                .unwrap_or_default()
        && packet
            .agentic_binding
            .as_ref()
            .is_some_and(|binding| actor.program_id == binding.program_id)
        && actor.execution_id.as_deref() == Some(packet.graph_id())
        && actor.original_execution.as_ref().is_some_and(|original| {
            original.graph_id == packet.graph_id()
                && original.node_id == packet.node_id()
                && original.attempt == packet.attempt
        })
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
        // The native provider path does not carry the Program in the Step
        // identity, so the Program is bound by the TRUSTED packet and the
        // actor. When the Step does name one, it has to be that Program.
        && packet.agentic_binding.as_ref().is_some_and(|binding| {
            identity
                .program_ref
                .as_deref()
                .is_none_or(|program| program == binding.program_id.as_str())
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
            original_execution: Some(harness_contract::agent_action::OriginalAgentExecution {
                graph_id: "graph".into(), node_id: "node".into(), attempt: 2,
            }),
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

    fn objective_review_task(call_id: &str) -> Value {
        serde_json::json!({
            "tool_name": "objective_review",
            "tool_call_id": call_id,
            "normalized_input": {
                "criterion_ref": "user_intent",
                "decision": "satisfied",
                "result_refs": ["artifact:one"],
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

    fn objective_review_action() -> AgentAction {
        AgentAction::ObjectiveReview(harness_contract::agent_action::ObjectiveReviewInput {
            criterion_ref: "user_intent".into(),
            decision: harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
            result_refs: vec!["artifact:one".into()],
            evidence_refs: vec!["artifact://read".into()],
            reason_ref: String::new(),
            reason: "independently read".into(),
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
        resolve_current_review_effect_key(events, packet, context)
            .expect("an unambiguous resolution")
    }

    /// A stored candidate is only the review's own call when it is the
    /// invocation the ORIGINAL applied action recorded. A candidate that names
    /// another review call of the same reviewer is another review.
    #[test]
    fn r1_a_stored_candidate_must_be_the_original_action_invocation() {
        let packet = packet();
        let applied = review_task("call-a");
        let other = review_task("call-b");
        let events = [plan_event(serde_json::json!([
            applied.clone(),
            other.clone()
        ]))];
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
        let events = [plan_event(serde_json::json!([
            first.clone(),
            second.clone()
        ]))];
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

    /// An objective review executes its criterion Task and therefore runs
    /// under TaskExecute rather than TaskReview focus. The focus shape does
    /// not change which exact call is the review; changing the focused Task or
    /// the actor still resolves to nothing.
    #[test]
    fn r1_a_objective_review_exempts_its_own_call_under_task_execute_focus() {
        let mut packet = packet();
        packet.agentic_binding.as_mut().unwrap().focus =
            harness_contract::agent::AgenticExecutionFocus::TaskExecute {
                task_ref: "task:one".into(),
            };
        let task = objective_review_task("call-one");
        let key = effect_key(&task);
        let events = [plan_event(serde_json::json!([task]))];
        let action = objective_review_action();
        let reviewer = actor();
        let context = CurrentReviewContext {
            action_id: &key,
            action: &action,
            actor: &reviewer,
        };
        assert_eq!(resolve(&events, &packet, &context), Some(key.clone()));

        let mut other_focus = packet.clone();
        other_focus.agentic_binding.as_mut().unwrap().focus =
            harness_contract::agent::AgenticExecutionFocus::TaskExecute {
                task_ref: "task:other".into(),
            };
        assert_eq!(resolve(&events, &other_focus, &context), None);

        let mut foreign = reviewer.clone();
        foreign.execution_id = Some("other-graph".into());
        let foreign_context = CurrentReviewContext {
            action_id: &key,
            action: &action,
            actor: &foreign,
        };
        assert_eq!(resolve(&events, &packet, &foreign_context), None);
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

        for physical in [
            None,
            Some(harness_contract::agent_action::OriginalAgentExecution {
                graph_id: "other-graph".into(), node_id: "node".into(), attempt: 2,
            }),
            Some(harness_contract::agent_action::OriginalAgentExecution {
                graph_id: "graph".into(), node_id: "other-node".into(), attempt: 2,
            }),
            Some(harness_contract::agent_action::OriginalAgentExecution {
                graph_id: "graph".into(), node_id: "node".into(), attempt: 1,
            }),
        ] {
            let mut stale = actor();
            stale.original_execution = physical;
            let context = CurrentReviewContext {
                action_id: &effect_key(&task), action: &action, actor: &stale,
            };
            assert!(resolve(&events, &packet, &context).is_none(),
                "missing, foreign or stale original physical actor must not be exempt");
        }

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

    /// A batch carries its children under the parent's identity. The review
    /// child has its own composite effect identity and may be exempt without
    /// hiding any sibling's ordinary effect.
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
        let review_child_key = format!("{sibling_key}:0");
        assert_eq!(
            resolve(&sibling_events, &packet, &sibling_context),
            Some(review_child_key.clone()),
            "the review child in a legal multi-action batch is exempt"
        );
        assert_ne!(
            review_child_key,
            format!("{sibling_key}:1"),
            "the sibling effect keeps its own identity and remains blocking"
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
            Some(format!("{alone_key}:0")),
            "a one-child batch still exempts only that child"
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

    #[test]
    fn r1_applied_batch_rebinds_only_the_review_child() {
        let packet = packet();
        let task = review_task("call-review");
        let review = serde_json::json!({"tool": "task_review", "input": task["normalized_input"]});
        let sibling = serde_json::json!({
            "tool": "task_submit",
            "input": {"task_ref": "task:two", "artifact_refs": ["artifact://x"]},
        });
        let batch = serde_json::json!({
            "tool_name": "agent_action_batch",
            "tool_call_id": "call-batch",
            "normalized_input": {"actions": [review, sibling]},
            "step_binding": step_binding("call-batch"),
        });
        let events = [plan_event(serde_json::json!([batch.clone()]))];
        let parent = effect_key(&batch);
        let review_child = format!("{parent}:0");
        assert_eq!(
            rebind_current_review_effect_key(
                &events,
                &packet,
                Some(&review_child),
                Some(&review_child),
            )
            .expect("applied batch review rebinds"),
            Some(review_child.clone())
        );
        let sibling_child = format!("{parent}:1");
        assert_eq!(
            rebind_current_review_effect_key(
                &events,
                &packet,
                Some(&sibling_child),
                Some(&sibling_child),
            )
            .expect("sibling lookup is unambiguous"),
            None,
            "the ordinary sibling keeps blocking"
        );
    }

    fn test_program_cut_absorbs_own_review(
        store: &crate::RuntimeEventStore,
        source: &str,
        pinned: u64,
        current: u64,
        action: &str,
    ) -> bool {
        program_events_after_cut(store, source, pinned, current)
            .map(|events| program_cut_absorbs_own_review(&events, source, pinned, current, action))
            .unwrap_or(false)
    }

    fn append_program_event(
        store: &std::sync::Arc<crate::RuntimeEventStore>,
        kind: &str,
        action_id: &str,
    ) {
        store
            .append(crate::RuntimeEventInput {
                stream_id: "agentic-program:program".into(),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.action_applied".into(),
                status: Some("applied".into()),
                actor: Some("agent".into()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "envelope": {
                        "action_id": action_id,
                        "action": {"kind": kind, "input": {"criterion_ref": "user_intent"}}
                    }
                }),
            })
            .unwrap();
    }

    /// Append a Program-scope bookkeeping event that carries no action
    /// envelope (a lease renewal, a completion reopen).
    fn append_program_bookkeeping(store: &std::sync::Arc<crate::RuntimeEventStore>, kind: &str) {
        store
            .append(crate::RuntimeEventInput {
                stream_id: "agentic-program:program".into(),
                scope: crate::RuntimeEventScope::Program,
                kind: kind.into(),
                status: Some("open".into()),
                actor: Some("runtime.program-supervisor".into()),
                refs: Vec::new(),
                payload: serde_json::json!({"gaps": ["review_validation_failed"]}),
            })
            .unwrap();
    }

    /// The exact Program advance the normal business chain produced when the
    /// Goal refused to converge.
    ///
    /// The review is applied, the worker settles around it, the root asks for
    /// completion twice, and the Program records that it could not close the
    /// Goal twice. The last two records are the Program's own account of the
    /// judgement that failed - a proof that invalidated itself on them could
    /// never be judged, so the Goal reopened forever.
    #[test]
    fn r1_a_proof_absorbs_the_programs_own_record_of_a_failed_judgement() {
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        append_program_event(&store, "task_claim", "worker-claim");
        let pinned = store.stream_revision("agentic-program:program").unwrap();
        append_program_event(&store, OBJECTIVE_REVIEW_TOOL_ID, "this-review");
        append_program_bookkeeping(&store, "agentic.task_lease_renewed");
        append_program_event(&store, "task_withdraw", "root-withdraw");
        append_program_event(&store, "task_attempt_fail", "runtime-attempt-settle");
        append_program_event(&store, "objective_complete_request", "root-completion-1");
        append_program_bookkeeping(&store, "agentic.completion_reopened");
        append_program_event(&store, "objective_complete_request", "root-completion-2");
        append_program_bookkeeping(&store, "agentic.completion_reopened");
        let current = store.stream_revision("agentic-program:program").unwrap();
        assert_eq!(
            current - pinned,
            8,
            "the recorded advance is eight events long"
        );
        assert!(
            test_program_cut_absorbs_own_review(
                store.as_ref(),
                "agentic-program:program",
                pinned,
                current,
                "this-review"
            ),
            "the Program's own conclusion bookkeeping is not a change to the work"
        );
        // A business write in the same advance is still a reason to rebuild.
        append_program_event(&store, "task_submit", "worker-submit");
        let advanced = store.stream_revision("agentic-program:program").unwrap();
        assert!(!test_program_cut_absorbs_own_review(
            store.as_ref(),
            "agentic-program:program",
            pinned,
            advanced,
            "this-review"
        ));
    }

    /// The review's own application is the one Program advance its proof may
    /// absorb: the proof is assembled before the action is applied, so the
    /// pinned cut always precedes it.
    #[test]
    fn r1_a_proof_absorbs_its_own_review_application_and_nothing_else() {
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        append_program_event(&store, "task_claim", "worker-claim");
        let pinned = store.stream_revision("agentic-program:program").unwrap();
        append_program_event(&store, OBJECTIVE_REVIEW_TOOL_ID, "this-review");
        let current = store.stream_revision("agentic-program:program").unwrap();
        assert!(test_program_cut_absorbs_own_review(
            store.as_ref(),
            "agentic-program:program",
            pinned,
            current,
            "this-review"
        ));
        // Another actor's review, or the same review under another id, is a
        // different action and must not be absorbed.
        assert!(!test_program_cut_absorbs_own_review(
            store.as_ref(),
            "agentic-program:program",
            pinned,
            current,
            "another-review"
        ));
        // A business write in the same advance is not the review.
        append_program_event(&store, "task_submit", "worker-submit");
        let advanced = store.stream_revision("agentic-program:program").unwrap();
        assert!(!test_program_cut_absorbs_own_review(
            store.as_ref(),
            "agentic-program:program",
            pinned,
            advanced,
            "this-review"
        ));
        // An unmoved cut always matches.
        assert!(test_program_cut_absorbs_own_review(
            store.as_ref(),
            "agentic-program:program",
            advanced,
            advanced,
            "this-review"
        ));
    }

    /// The root's withdrawal/settlement around the review landing is the same
    /// worker bookkeeping the Program owner already treats as semantically
    /// neutral, so it must not invalidate a proof either.
    #[test]
    fn r1_a_proof_absorbs_worker_bookkeeping_around_its_review() {
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        append_program_event(&store, "task_claim", "worker-claim");
        let pinned = store.stream_revision("agentic-program:program").unwrap();
        append_program_event(&store, OBJECTIVE_REVIEW_TOOL_ID, "this-review");
        append_program_event(&store, "task_withdraw", "root-withdraw");
        append_program_event(&store, "task_attempt_fail", "runtime-attempt-settle");
        store
            .append(crate::RuntimeEventInput {
                stream_id: "agentic-program:program".into(),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.task_lease_renewed".into(),
                status: Some("renewed".into()),
                actor: Some("agent".into()),
                refs: Vec::new(),
                payload: serde_json::json!({"claim_generation": 2}),
            })
            .unwrap();
        let current = store.stream_revision("agentic-program:program").unwrap();
        assert!(
            test_program_cut_absorbs_own_review(
                store.as_ref(),
                "agentic-program:program",
                pinned,
                current,
                "this-review"
            ),
            "the review's own application and the worker bookkeeping around it are both neutral"
        );
        // A real business write in the same advance is still a reason to rebuild.
        append_program_event(&store, "task_submit", "worker-submit");
        let advanced = store.stream_revision("agentic-program:program").unwrap();
        assert!(!test_program_cut_absorbs_own_review(
            store.as_ref(),
            "agentic-program:program",
            pinned,
            advanced,
            "this-review"
        ));
    }

    /// The root's completion request lands after the review by construction:
    /// it is the action that asks this proof to be judged. The conclusion path
    /// already binds that request by its own digest and revision, so its
    /// application is an advance the proof may absorb too.
    #[test]
    fn r1_a_proof_absorbs_the_completion_request_it_is_judging() {
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        append_program_event(&store, "task_claim", "worker-claim");
        let pinned = store.stream_revision("agentic-program:program").unwrap();
        append_program_event(&store, OBJECTIVE_REVIEW_TOOL_ID, "this-review");
        append_program_event(&store, "task_withdraw", "root-withdraw");
        append_program_event(&store, "objective_complete_request", "root-completion");
        let current = store.stream_revision("agentic-program:program").unwrap();
        assert!(
            test_program_cut_absorbs_own_review(
                store.as_ref(),
                "agentic-program:program",
                pinned,
                current,
                "this-review"
            ),
            "the review and the completion request it is judged under both land after its cut"
        );
        // A different deliverable, or a spec change, still rebuilds the proof.
        append_program_event(&store, "task_submit", "worker-submit");
        let advanced = store.stream_revision("agentic-program:program").unwrap();
        assert!(!test_program_cut_absorbs_own_review(
            store.as_ref(),
            "agentic-program:program",
            pinned,
            advanced,
            "this-review"
        ));
    }
    include!("current_review_applied_tests.rs");
}
/// True when this Program event is worker lifecycle bookkeeping.
///
/// The Program owner already treats a lease renewal as semantically neutral
/// for its own carrier cut (`commit_program_conclusion` tolerates exactly
/// `agentic.task_lease_renewed`). A review lands one revision after its own
/// discovery, and the withdrawal/settlement the root performs around that
/// landing is the same kind of bookkeeping: it changes who holds the Task,
/// never what the Task asked for.
pub(crate) fn is_worker_lifecycle_event(event: &crate::DurableRuntimeEvent) -> bool {
    event.scope == crate::RuntimeEventScope::Program
        && (event.kind == "agentic.task_lease_renewed"
            || event.kind == "agentic.action_applied"
                && matches!(
                    event.payload["envelope"]["action"]["kind"].as_str(),
                    Some("task_withdraw") | Some("task_release") | Some("task_attempt_fail")
                ))
}

/// True when this Program event records the completion request the conclusion
/// is being prepared for.
///
/// A proof is assembled while the review is being written, so the root's
/// completion request - the action that asks this very proof to be judged -
/// necessarily lands afterwards. The conclusion path already binds that request
/// by its own digest and revision, so its application is the second advance the
/// proof may absorb.
pub(crate) fn is_completion_request_event(event: &crate::DurableRuntimeEvent) -> bool {
    event.kind == "agentic.action_applied"
        && event.payload["envelope"]["action"]["kind"].as_str()
            == Some("objective_complete_request")
}

/// True when this Program event records the conclusion PROCESS itself.
///
/// A Program that cannot close its Goal says so: it reopens the completion
/// when a conclusion attempt fails, and it records a rejected completion
/// request when the request never became an action. Both are records ABOUT
/// the conclusion, not changes to the work the proof fenced - the Tasks, the
/// artifacts and the results are exactly what they were.
///
/// They have to be absorbed for the same reason the completion request
/// itself is: a proof that invalidated itself on the record of its own
/// failed judgement could never be judged. The record is written BECAUSE the
/// proof was not accepted, so leaving it outside the absorb-list turns one
/// rejected proof into an endless rejection.
pub(crate) fn is_conclusion_process_event(event: &crate::DurableRuntimeEvent) -> bool {
    event.scope == crate::RuntimeEventScope::Program
        && matches!(
            event.kind.as_str(),
            "agentic.completion_reopened" | "agentic.completion_rejected"
        )
}
