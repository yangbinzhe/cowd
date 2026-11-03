use serde::{Deserialize, Serialize};

use harness_contract::execution_graph::ExecutionParentBinding;

use crate::tool_dispatch::ToolRequest;
use crate::tool_orchestrator::ToolSafetyCategory;

/// Original node owner's publication state; it carries no execution authority.
#[derive(Debug, Default)]
pub(crate) struct OriginalMemberSettlement(std::sync::atomic::AtomicBool);
impl OriginalMemberSettlement {
    pub(crate) fn is_settled(&self) -> bool { self.0.load(std::sync::atomic::Ordering::Acquire) }
    pub(crate) fn mark_settled(&self) { self.0.store(true, std::sync::atomic::Ordering::Release); }
}

/// Same-call transport retained by the Runtime producer. This is not proof of
/// ownership: the original node owner must authenticate the ticket and Step.
#[derive(Clone)]
pub struct OriginalMemberCallCarrier {
    original_request: std::sync::Arc<RuntimeToolExecutionRequest>,
    outcomes: Vec<(usize, std::sync::Arc<OriginalMemberSubmitOutcome>)>,
    stop_publications: Vec<std::sync::Arc<OriginalStopPublication>>,
    host_members: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<usize, (std::sync::Arc<crate::conversation::OriginalHostPreparedMember>, std::sync::Arc<OriginalMemberSettlement>)>>>,
}
impl OriginalMemberCallCarrier {
    pub(crate) fn from_original_request(original_request: std::sync::Arc<RuntimeToolExecutionRequest>) -> Self {
        let outcomes = original_request.tool_progress.original_members();
        let stop_publications = original_request.tool_progress.original_stop_publications();
        Self { original_request, outcomes, stop_publications, host_members: Default::default() }
    }
    /// Retain the exact Host preparation under its original node carrier.
    pub(crate) fn retain_host_member(
        &self,
        ticket: &crate::execution_core::NodeExecutionTicket,
        member: &std::sync::Arc<crate::conversation::OriginalHostPreparedMember>,
        observation: &crate::execution_core::graph::executors::agent_tool::NoDispatchObservation,
    ) -> Result<(), String> {
        let request = member.original_request();
        let original = self.original_request.as_ref();
        let index = member.prepared().index;
        if index >= 25 || member.ticket() != ticket || request != original
            || !request.tool_progress.same_original_channels(&original.tool_progress)
            || request.step_binding.as_ref() != Some(member.step()) {
            return Err("original Host member carrier identity differs".into());
        }
        let unit = {
            let members = self.host_members.lock().map_err(|_| "original Host member carrier unavailable")?;
            match members.get(&index) {
                Some((existing, unit)) if std::sync::Arc::ptr_eq(existing, member) => std::sync::Arc::clone(unit),
                Some(_) => return Err("original Host member carrier conflicts".into()),
                None => std::sync::Arc::new(OriginalMemberSettlement::default()),
            }
        };
        observation.register_member_custody(ticket, &original.tool_use_id, index, std::sync::Arc::clone(&unit))?;
        let mut members = self.host_members.lock().map_err(|_| "original Host member carrier unavailable")?;
        match members.get(&index) {
            Some((existing, previous)) if std::sync::Arc::ptr_eq(existing, member) && std::sync::Arc::ptr_eq(previous, &unit) => Ok(()),
            Some(_) => Err("original Host member carrier conflicts".into()),
            None => { members.insert(index, (std::sync::Arc::clone(member), unit)); Ok(()) }
        }
    }
    pub(crate) fn has_unsettled_host_members(&self) -> bool {
        match self.host_members.lock() {
            Ok(members) => members.values().any(|(_, unit)| !unit.is_settled()),
            Err(_) => true,
        }
    }
    /// Transfer only members already authenticated in this node's committed prefix.
    pub(crate) fn verified_host_settlements(
        &self, ticket: &crate::execution_core::NodeExecutionTicket,
        store: &std::sync::Arc<crate::RuntimeEventStore>, facts: &[serde_json::Value],
    ) -> Result<Vec<std::sync::Arc<OriginalMemberSettlement>>, String> {
        let outcomes = self.original_request.tool_progress.original_members();
        let members = self.host_members.lock().map_err(|_| "original Host member carrier unavailable")?;
        for (index, (original, _)) in members.iter() {
            if original.ticket() != ticket || original.prepared().index != *index {
                return Err("original Host member settlement identity differs".into());
            }
            let mut matching = outcomes.iter().filter(|(actual, _)| actual == index);
            let (_, outcome) = matching.next().ok_or("original Host member result unavailable")?;
            if matching.next().is_some() { return Err("original Host member results conflict".into()); }
            let OriginalMemberSubmitOutcome::Committed(member) = outcome.as_ref() else {
                return Err("original Host member result remains unresolved".into());
            };
            if member.index() != *index
                || member.action().payload().pointer("/member_admission/prepared/identity").and_then(serde_json::Value::as_str)
                    != Some(original.prepared().identity.as_str()) {
                return Err("original Host member result differs".into());
            }
            let fact = member.node_commit_fact(store, ticket)?;
            if !facts.iter().any(|committed| committed == &fact) {
                return Err("original Host member is absent from committed node".into());
            }
        }
        Ok(members.values().map(|(_, unit)| std::sync::Arc::clone(unit)).collect())
    }
    pub fn original_request(&self) -> &RuntimeToolExecutionRequest { &self.original_request }
    pub fn outcomes(&self) -> &[(usize, std::sync::Arc<OriginalMemberSubmitOutcome>)] { &self.outcomes }
    pub fn stop_publications(&self) -> &[std::sync::Arc<OriginalStopPublication>] { &self.stop_publications }
}

/// In-process progress sink for long-running tools such as bash (T4). The
/// wrapper keeps the request struct's derives (Debug/PartialEq) valid while
/// serde always skips the callback; only its presence is ever compared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolLifecycleAnchor {
    pub invocation_id: String,
    pub session_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub input_hash: String,
    pub producer: harness_contract::projection::RuntimeActivityBinding,
}

/// Result of resolving a previously committed member before new authorization.
pub enum AgentBatchMemberResolution {
    Replay(AuthenticatedBatchMember),
    Unknown(BatchMemberUnknown),
    Conflict(BatchMemberConflict),
}

/// A candidate is non-consuming preparation and carries no Fresh decision.
/// Only the original checked action owner may resolve it to a commit.
pub enum AgentBatchMemberPregrant {
    Replay(AuthenticatedBatchMember),
    Candidate(crate::step_snapshot::OriginalPreparedMemberCapability),
    /// A stop exists only after the original ticket/call was verified.
    Unknown { failure: BatchMemberUnknown, stop: Option<OriginalStopPublication> },
}

// Type erasure here keeps the stop from recursively containing its own
// mutex type; the channel is still the original request's single sink.
trait OriginalStopSink: Send + Sync {
    fn record(&self, stop: std::sync::Arc<OriginalBatchMemberStop>) -> OriginalStopPublication;
    fn current(&self) -> Option<std::sync::Arc<OriginalBatchMemberStop>>;
}

#[derive(Default)]
struct OriginalStopChannel {
    slot: std::sync::Mutex<Option<std::sync::Arc<OriginalBatchMemberStop>>>,
}

impl OriginalStopSink for OriginalStopChannel {
    fn record(&self, stop: std::sync::Arc<OriginalBatchMemberStop>) -> OriginalStopPublication {
        OriginalStopPublication::record(stop, &self.slot)
    }
    fn current(&self) -> Option<std::sync::Arc<OriginalBatchMemberStop>> {
        match self.slot.lock() {
            Ok(slot) => slot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

/// Original owner-authenticated coordinates; callers cannot construct them.
pub struct OriginalBatchMemberCoordinate {
    outer_key: String,
    call_id: String,
    ticket: crate::execution_core::NodeExecutionTicket,
    step: Option<crate::step_snapshot::StepCallBinding>,
    sink: std::sync::Weak<dyn OriginalStopSink>,
    index: usize,
}
impl OriginalBatchMemberCoordinate {
    pub(crate) fn from_original(original: &RuntimeToolExecutionRequest, ticket: &crate::execution_core::NodeExecutionTicket, index: usize) -> Self {
        let sink: std::sync::Arc<dyn OriginalStopSink> = original.tool_progress.member_stop.clone();
        Self { outer_key: original.idempotency_key.clone(), call_id: original.tool_use_id.clone(),
            ticket: ticket.clone(), step: original.step_binding.clone(), sink: std::sync::Arc::downgrade(&sink), index }
    }
    pub fn outer_key(&self) -> &str { &self.outer_key }
    pub fn call_id(&self) -> &str { &self.call_id }
    pub fn index(&self) -> usize { self.index }
    pub fn ticket(&self) -> &crate::execution_core::NodeExecutionTicket { &self.ticket }
    pub fn step(&self) -> Option<&crate::step_snapshot::StepCallBinding> { self.step.as_ref() }
}

/// Stop at this exact member; the prefix and this fact are independent.
pub struct OriginalBatchMemberStop {
    coordinate: OriginalBatchMemberCoordinate,
    reason: OriginalBatchMemberStopReason,
    recorded: std::sync::atomic::AtomicBool,
    reservation: Option<std::sync::Arc<crate::runtime_event_store::OriginalMemberReservation>>,
    effect_fence: Option<std::sync::Arc<crate::runtime_event_store::OriginalMemberEffectFence>>,
}
impl OriginalBatchMemberStop {
    pub(crate) fn publish(coordinate: OriginalBatchMemberCoordinate, reason: OriginalBatchMemberStopReason) -> OriginalStopPublication {
        Self::publish_owned(coordinate, reason, None, None)
    }
    pub(crate) fn publish_with_reservation(coordinate: OriginalBatchMemberCoordinate, reason: OriginalBatchMemberStopReason,
        reservation: std::sync::Arc<crate::runtime_event_store::OriginalMemberReservation>) -> OriginalStopPublication {
        Self::publish_owned(coordinate, reason, Some(reservation), None)
    }
    pub(crate) fn publish_with_owners(coordinate: OriginalBatchMemberCoordinate, reason: OriginalBatchMemberStopReason,
        reservation: std::sync::Arc<crate::runtime_event_store::OriginalMemberReservation>,
        effect_fence: std::sync::Arc<crate::runtime_event_store::OriginalMemberEffectFence>) -> OriginalStopPublication {
        Self::publish_owned(coordinate, reason, Some(reservation), Some(effect_fence))
    }
    fn publish_owned(coordinate: OriginalBatchMemberCoordinate, reason: OriginalBatchMemberStopReason,
        reservation: Option<std::sync::Arc<crate::runtime_event_store::OriginalMemberReservation>>,
        effect_fence: Option<std::sync::Arc<crate::runtime_event_store::OriginalMemberEffectFence>>) -> OriginalStopPublication {
        let sink = coordinate.sink.upgrade();
        let stop = std::sync::Arc::new(Self { coordinate, reason, recorded: std::sync::atomic::AtomicBool::new(false), reservation, effect_fence });
        match sink {
            Some(sink) => sink.record(stop),
            None => OriginalStopPublication::Unknown { current: stop, previous: None, failure: OriginalStopRecordFailure::SinkUnavailable },
        }
    }
    pub fn coordinate(&self) -> &OriginalBatchMemberCoordinate { &self.coordinate }
    pub fn reason(&self) -> &OriginalBatchMemberStopReason { &self.reason }
    pub fn recorded_in_original_sink(&self) -> bool { self.recorded.load(std::sync::atomic::Ordering::Acquire) }
    pub fn has_original_reservation(&self) -> bool { self.reservation.is_some() }
    pub fn has_original_effect_fence(&self) -> bool { self.effect_fence.is_some() }
    pub(crate) fn original_reservation(&self) -> Option<&std::sync::Arc<crate::runtime_event_store::OriginalMemberReservation>> { self.reservation.as_ref() }
    /// Checks the original sink identity only; publication may have failed.
    pub fn belongs_to_original_sink(&self, sink: &ToolProgressSink) -> bool {
        let original: std::sync::Arc<dyn OriginalStopSink> = sink.member_stop.clone();
        std::sync::Weak::ptr_eq(&self.coordinate.sink, &std::sync::Arc::downgrade(&original))
    }
}
/// Publication failure never discards the new stop. A conflicting or
/// poisoned slot also returns its existing stop to the original caller.
pub enum OriginalStopPublication {
    Recorded(std::sync::Arc<OriginalBatchMemberStop>),
    Unknown {
        current: std::sync::Arc<OriginalBatchMemberStop>,
        previous: Option<std::sync::Arc<OriginalBatchMemberStop>>,
        failure: OriginalStopRecordFailure,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginalStopRecordFailure { SinkUnavailable, Poisoned, Occupied, WrongSink }
/// Reading a poisoned slot returns its retained stop and does not clear it.
pub enum OriginalStopRead {
    Available(Option<std::sync::Arc<OriginalBatchMemberStop>>),
    Poisoned(Option<std::sync::Arc<OriginalBatchMemberStop>>),
}
impl OriginalStopRead {
    pub fn retained(&self) -> Option<&std::sync::Arc<OriginalBatchMemberStop>> {
        match self { Self::Available(stop) | Self::Poisoned(stop) => stop.as_ref() }
    }
    pub fn is_poisoned(&self) -> bool { matches!(self, Self::Poisoned(_)) }
}
impl OriginalStopPublication {
    fn record(stop: std::sync::Arc<OriginalBatchMemberStop>, sink: &std::sync::Mutex<Option<std::sync::Arc<OriginalBatchMemberStop>>>) -> Self {
        match sink.lock() {
            Ok(mut slot) => {
                if let Some(previous) = slot.as_ref() {
                    if std::sync::Arc::ptr_eq(previous, &stop) {
                        return Self::Recorded(stop);
                    }
                    return Self::Unknown { current: stop, previous: Some(std::sync::Arc::clone(previous)), failure: OriginalStopRecordFailure::Occupied };
                }
                stop.recorded.store(true, std::sync::atomic::Ordering::Release);
                *slot = Some(std::sync::Arc::clone(&stop));
                Self::Recorded(stop)
            }
            Err(poisoned) => {
                let previous = poisoned.into_inner().clone();
                Self::Unknown { current: stop, previous, failure: OriginalStopRecordFailure::Poisoned }
            }
        }
    }
    pub fn current(&self) -> &std::sync::Arc<OriginalBatchMemberStop> {
        match self { Self::Recorded(current) | Self::Unknown { current, .. } => current }
    }
    pub fn previous(&self) -> Option<&std::sync::Arc<OriginalBatchMemberStop>> {
        match self { Self::Recorded(_) => None, Self::Unknown { previous, .. } => previous.as_ref() }
    }
    pub fn failure(&self) -> Option<OriginalStopRecordFailure> {
        match self { Self::Recorded(_) => None, Self::Unknown { failure, .. } => Some(*failure) }
    }
    pub fn into_parts(self) -> (std::sync::Arc<OriginalBatchMemberStop>, Option<std::sync::Arc<OriginalBatchMemberStop>>, Option<OriginalStopRecordFailure>) {
        match self {
            Self::Recorded(current) => (current, None, None),
            Self::Unknown { current, previous, failure } => (current, previous, Some(failure)),
        }
    }
}
pub enum OriginalBatchMemberStopReason {
    BeforeOutcomeUnknown(String),
    BeforeOutcomeError(crate::agentic::AgentActionServiceError),
    BeforeOutcomeStoreError(std::sync::Arc<crate::RuntimeEventStoreError>),
    UnknownNoReceipt(harness_contract::agent_action::AgentActionObservation),
    /// The action committed at this index, but its observation is unavailable.
    CommittedObservationUnavailable(AuthenticatedBatchMember),
    /// The original commit exists; owner reconciliation/binding is pending.
    CommittedUnbound(OriginalMemberUnboundCommit),
}
pub enum PreparedMemberSubmitOutcome {
    Committed(AuthenticatedBatchMember),
    Stopped(OriginalStopPublication),
}

/// Reading a committed prefix never grants admission for any member.
pub enum AgentBatchCommittedPrefixRead {
    /// All members have committed; individual Host deliveries may be pending.
    AllCommitted(AuthenticatedBatchPrefix),
    /// A later member could not be resolved. Earlier committed members may
    /// independently have pending Host delivery.
    Stopped(IncompleteAgentBatch),
}

impl AgentBatchCommittedPrefixRead {
    pub fn committed(&self) -> &AuthenticatedBatchPrefix {
        match self {
            Self::AllCommitted(prefix) => prefix,
            Self::Stopped(incomplete) => incomplete.prefix(),
        }
    }

    pub fn pending_deliveries(&self) -> impl Iterator<Item = BatchMemberDeliveryPending<'_>> {
        self.committed().pending_deliveries()
    }

    /// The next member which could not be resolved, if any. This is separate
    /// from delivery of any previously committed member.
    pub fn stopped_at(&self) -> Option<&AgentBatchMemberFailure> {
        match self {
            Self::AllCommitted(_) => None,
            Self::Stopped(incomplete) => Some(incomplete.failure()),
        }
    }
}

/// A committed action and its original member receipt, authenticated by the
/// action owner. Private fields prevent a consumer from turning a projection
/// or an arbitrary event into a replay.
#[derive(Clone)]
pub struct AuthenticatedBatchMember(std::sync::Arc<AuthenticatedBatchMemberFacts>);

struct AuthenticatedBatchMemberFacts {
    source: OriginalMemberCommitSource,
    delivery: BatchMemberDelivery,
}

enum OriginalMemberCommitSource {
    Recovered(std::sync::Arc<crate::host::OriginalMemberReplayWitness>),
    SameOwner { handle: std::sync::Arc<crate::agentic::OriginalCommittedMember>, index: usize, action_index: usize },
}

impl AuthenticatedBatchMember {
    /// Borrow only the original in-process checked owner; a recovered
    /// witness cannot be promoted into a same-owner submission handle.
    pub(crate) fn same_owner_committed(&self) -> Option<&std::sync::Arc<crate::agentic::OriginalCommittedMember>> {
        match &self.0.source {
            OriginalMemberCommitSource::SameOwner { handle, .. } => Some(handle),
            OriginalMemberCommitSource::Recovered(_) => None,
        }
    }

    /// Only the original owner can mint the private witness. This constructor
    /// never accepts a caller-supplied receipt or a projected observation.
    pub(crate) fn from_original_witness(witness: std::sync::Arc<crate::host::OriginalMemberReplayWitness>) -> Self {
        Self(std::sync::Arc::new(AuthenticatedBatchMemberFacts {
            source: OriginalMemberCommitSource::Recovered(witness), delivery: BatchMemberDelivery::Incomplete,
        }))
    }

    /// The handle is private to the original checked append owner. Its request
    /// and receipt are borrowed directly; no database read or DTO can mint it.
    pub(crate) fn from_original_committed(
        handle: std::sync::Arc<crate::agentic::OriginalCommittedMember>,
        store: &std::sync::Arc<crate::RuntimeEventStore>,
        envelope: &harness_contract::agent_action::AgentActionEnvelope,
    ) -> Result<Self, (std::sync::Arc<crate::agentic::OriginalCommittedMember>, String)> {
        let verify = || -> Result<(usize, usize), String> {
        if !handle.belongs_to(store) { return Err("original member store mismatch".into()); }
        let transaction = &handle.request().transaction;
        let mut indices = transaction.events.iter().filter(|event| event.event.kind == "agentic.batch_member.index");
        let marker = indices.next().ok_or("original member index missing from checked request")?;
        if indices.next().is_some() { return Err("original member index ambiguous".into()); }
        let index = marker.event.payload["member_index"].as_u64()
            .and_then(|value| usize::try_from(value).ok()).ok_or("original member index invalid")?;
        let action_index = marker.event.payload["program_event_index"].as_u64()
            .and_then(|value| usize::try_from(value).ok()).ok_or("original member action position invalid")?;
        let action = transaction.events.get(action_index).ok_or("original member action absent")?;
        let facts = action.event.payload.get("member_admission").ok_or("original member facts absent")?;
        let outer = facts.pointer("/prepared/outer_key").and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty()).ok_or("original member outer missing")?;
        let identity = facts.pointer("/prepared/identity").and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty()).ok_or("original member identity missing")?;
        let original: harness_contract::agent_action::AgentActionEnvelope = serde_json::from_value(
            action.event.payload.get("envelope").cloned().ok_or("original member envelope absent")?,
        ).map_err(|error| error.to_string())?;
        let receipt = &handle.receipt().commit;
        // The original store owner sealed this exact post-validation request
        // with its append receipt; do not hash it again in the same owner.
        if original != *envelope || transaction.transaction_id != receipt.transaction_id
            || receipt.commit_cursor == 0
            || receipt.event_ids.len() != transaction.events.len()
            || marker.event.stream_id != format!("execution-effect:{outer}")
            || marker.event.scope != crate::RuntimeEventScope::ExecutionNode
            || marker.event.actor.as_deref() != Some("runtime.agentic_action")
            || marker.event.status.as_deref() != Some("committed") || marker.schema_version != 1
            || marker.idempotency_key.as_deref() != Some(format!("agentic-member:{index}").as_str())
            || marker.event.payload["outer_key"] != outer
            || marker.event.payload["member_identity"] != identity
            || facts.pointer("/prepared/index").and_then(serde_json::Value::as_u64) != u64::try_from(index).ok()
            || facts.get("ticket").is_none_or(serde_json::Value::is_null)
            || marker.event.payload.get("ticket") != facts.get("ticket")
            || action.event.stream_id != format!("agentic-program:{}", envelope.actor.program_id)
            || action.event.scope != crate::RuntimeEventScope::Program || action.schema_version != 1
            || action.event.actor.as_deref() != Some(envelope.actor.actor_id.as_str())
            || marker.event.payload["program_stream"] != action.event.stream_id
            || marker.event.payload["program_event_kind"] != action.event.kind
            || marker.event.payload["program_event_key"].as_str() != action.idempotency_key.as_deref()
            || !matches!(action.event.kind.as_str(), "agentic.action_applied" | "agentic.completion_rejected")
            || (action.event.kind == "agentic.action_applied" && action.event.status.as_deref() != Some("applied"))
            || (action.event.kind == "agentic.completion_rejected" && action.event.status.as_deref() != Some("rejected"))
        { return Err("original checked member transaction mismatch".into()); }
        Ok((index, action_index))
        };
        let (index, action_index) = match verify() {
            Ok(indices) => indices,
            Err(reason) => return Err((handle, reason)),
        };
        Ok(Self(std::sync::Arc::new(AuthenticatedBatchMemberFacts {
            source: OriginalMemberCommitSource::SameOwner { handle, index, action_index },
            delivery: BatchMemberDelivery::Incomplete,
        })))
    }

    /// Facts for the original node owner; callers cannot manufacture this
    /// from a deserialized receipt. Delivery is still pending until the node
    /// transaction and its existing after_commit publication have succeeded.
    pub(crate) fn node_commit_fact(
        &self,
        store: &std::sync::Arc<crate::RuntimeEventStore>,
        ticket: &crate::execution_core::NodeExecutionTicket,
    ) -> Result<serde_json::Value, String> {
        let (source_ticket, event_id, outer_key) = match &self.0.source {
            OriginalMemberCommitSource::Recovered(witness) => {
                if !witness.belongs_to_store(store) { return Err("member store mismatch".into()); }
                (serde_json::to_value(witness.ticket()).map_err(|e| e.to_string())?, witness.action().event_id.clone(), witness.prepared().outer_key.clone())
            }
            OriginalMemberCommitSource::SameOwner { handle, action_index, .. } => {
                if !handle.belongs_to(store) { return Err("member store mismatch".into()); }
                let marker = handle.request().transaction.events.iter()
                    .find(|event| event.event.kind == "agentic.batch_member.index")
                    .ok_or("member index missing")?;
                let ticket = marker.event.payload.get("ticket").cloned().ok_or("member ticket missing")?;
                let event_id = handle.receipt().commit.event_ids.get(*action_index)
                    .cloned().ok_or("member event identity missing")?;
                (ticket, event_id, marker.event.payload["outer_key"].as_str().filter(|key| !key.is_empty()).ok_or("outer key missing")?.to_owned())
            }
        };
        if source_ticket != serde_json::to_value(ticket).map_err(|e| e.to_string())? {
            return Err("member original node ticket mismatch".into());
        }
        if !matches!(self.delivery(), BatchMemberDelivery::Incomplete) {
            return Err("completed member delivery requires its original Host receipt".into());
        }
        let fact = self.committed_fact().ok_or("member original action fact missing")?;
        let business = match fact {
            OriginalMemberCommittedFact::Applied { .. } => "applied",
            OriginalMemberCommittedFact::Rejected { .. } => "rejected",
        };
        let observation = match self.observation() {
            OriginalMemberObservation::Available(value) => serde_json::json!({"state":"available", "value":value}),
            OriginalMemberObservation::Unavailable => serde_json::json!({"state":"unavailable"}),
        };
        let receipt = self.receipt();
        Ok(serde_json::json!({"member_index":self.index(), "outer_key":outer_key, "ticket":source_ticket,
            "action_event_id":event_id, "action_transaction_id":receipt.transaction_id,
            "action_commit_cursor":receipt.commit_cursor, "business":business,
            "observation":observation, "delivery":"pending"}))
    }

    pub fn index(&self) -> usize { match &self.0.source {
        OriginalMemberCommitSource::Recovered(witness) => witness.index(),
        OriginalMemberCommitSource::SameOwner { index, .. } => *index,
    }}
    pub fn action(&self) -> OriginalMemberEvent<'_> { match &self.0.source {
        OriginalMemberCommitSource::Recovered(witness) => OriginalMemberEvent::Recovered(witness.action()),
        OriginalMemberCommitSource::SameOwner { handle, action_index, .. } =>
            OriginalMemberEvent::SameOwner(&handle.request().transaction.events[*action_index].event),
    }}
    pub fn receipt(&self) -> &crate::AppendTransactionReceipt { match &self.0.source {
        OriginalMemberCommitSource::Recovered(witness) => witness.action_receipt(),
        OriginalMemberCommitSource::SameOwner { handle, .. } => &handle.receipt().commit,
    }}
    pub fn delivery(&self) -> &BatchMemberDelivery { &self.0.delivery }

    /// Business-result availability is independent from Host delivery. In
    /// particular, post-commit processing may fail without undoing the commit.
    pub fn observation(&self) -> OriginalMemberObservation<'_> {
        match &self.0.source {
            OriginalMemberCommitSource::SameOwner { handle, .. } => match handle.observation() {
                Some(observation) => OriginalMemberObservation::Available(observation),
                None => OriginalMemberObservation::Unavailable,
            },
            OriginalMemberCommitSource::Recovered(_) => OriginalMemberObservation::Unavailable,
        }
    }

    pub fn observation_error(&self) -> Option<&str> { match &self.0.source {
        OriginalMemberCommitSource::SameOwner { handle, .. } => handle.observation_error(),
        OriginalMemberCommitSource::Recovered(_) => None,
    }}

    /// Only facts present in the original committed event. This does not
    /// reconstruct an observation from the current Program projection.
    pub fn committed_fact(&self) -> Option<OriginalMemberCommittedFact<'_>> {
        let event = self.action();
        match event.kind() {
            "agentic.action_applied" => Some(OriginalMemberCommittedFact::Applied { event }),
            "agentic.completion_rejected" => Some(OriginalMemberCommittedFact::Rejected {
                event,
                code: event.payload().get("code")?.as_str()?,
                reason: event.payload().get("gap")?.as_str()?,
            }),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub enum OriginalMemberEvent<'a> {
    /// Re-entered storage, authenticated once by the original owner.
    Recovered(&'a crate::DurableRuntimeEvent),
    /// Exact input and receipt returned from this owner's checked append.
    SameOwner(&'a crate::RuntimeEventInput),
}
impl<'a> OriginalMemberEvent<'a> {
    pub fn kind(self) -> &'a str { match self { Self::Recovered(event) => &event.kind, Self::SameOwner(event) => &event.kind } }
    pub fn payload(self) -> &'a serde_json::Value { match self { Self::Recovered(event) => &event.payload, Self::SameOwner(event) => &event.payload } }
}

/// NoCommittedReceipt remains unresolved and is never a Fresh grant.
pub enum OriginalMemberSubmitOutcome {
    Committed(AuthenticatedBatchMember),
    /// A checked append committed, but its identity could not be bound to the
    /// requested member. Keep the sealed owner handle for reconciliation.
    CommittedUnbound(OriginalMemberUnboundCommit),
    NoCommittedReceipt(harness_contract::agent_action::AgentActionObservation),
}

impl OriginalMemberSubmitOutcome {
    /// Called by Runtime ingress with the actual owner result. In particular,
    /// a binding error retains the sealed committed handle rather than
    /// turning an already committed action into a retryable failure.
    pub(crate) fn from_original_submission(
        submission: crate::agentic::OriginalMemberSubmission,
        store: &std::sync::Arc<crate::RuntimeEventStore>,
        envelope: &harness_contract::agent_action::AgentActionEnvelope,
    ) -> Self {
        match submission {
            crate::agentic::OriginalMemberSubmission::Committed(handle) => {
                match AuthenticatedBatchMember::from_original_committed(handle, store, envelope) {
                    Ok(member) => Self::Committed(member),
                    Err((handle, reason)) => Self::CommittedUnbound(OriginalMemberUnboundCommit { handle, reason }),
                }
            }
            crate::agentic::OriginalMemberSubmission::NoCommittedReceipt(observation) =>
                Self::NoCommittedReceipt(observation),
        }
    }
}

pub struct OriginalMemberUnboundCommit {
    pub(crate) handle: std::sync::Arc<crate::agentic::OriginalCommittedMember>,
    pub(crate) reason: String,
}
impl OriginalMemberUnboundCommit {
    pub fn reason(&self) -> &str { &self.reason }
    pub(crate) fn original(&self) -> &std::sync::Arc<crate::agentic::OriginalCommittedMember> { &self.handle }
}

pub enum OriginalMemberObservation<'a> {
    Available(&'a harness_contract::agent_action::AgentActionObservation),
    Unavailable,
}

/// A partial original business result, not a completed Host delivery or a
/// replacement for the original full observation.
pub enum OriginalMemberCommittedFact<'a> {
    Applied { event: OriginalMemberEvent<'a> },
    Rejected { event: OriginalMemberEvent<'a>, code: &'a str, reason: &'a str },
}

/// Business commit and Host delivery are distinct facts.
pub enum BatchMemberDelivery {
    Incomplete,
}

/// An already-committed member whose Host delivery is not proven complete.
/// The borrowed member carries the original action and transaction receipt;
/// creating this view does not create another receipt or authorize dispatch.
pub struct BatchMemberDeliveryPending<'a> {
    member: &'a AuthenticatedBatchMember,
}

impl<'a> BatchMemberDeliveryPending<'a> {
    pub fn index(&self) -> usize { self.member.index() }
    pub fn member(&self) -> &'a AuthenticatedBatchMember { self.member }
}

pub struct BatchMemberUnknown {
    pub index: usize,
    pub reason: BatchMemberUnknownReason,
}

pub enum BatchMemberUnknownReason {
    /// The original ticket or request could not be authenticated.
    OriginalRequestNotAuthenticated(String),
    /// The action owner has not supplied the same-transaction member facts.
    OriginalTransactionFactsUnavailable,
    /// The original non-consuming preparation could not be completed.
    Preparation(String),
    /// Preserve an original owner store error and its Unknown status.
    OriginalOwnerStore(std::sync::Arc<crate::RuntimeEventStoreError>),
    /// Preserve the original transaction error, including an unknown commit.
    Commit(crate::agentic::AgentActionServiceError),
}

pub struct BatchMemberConflict {
    pub index: usize,
    pub reason: String,
}

/// In-process delivery carrier for an ordered prefix of original, authenticated
/// transaction receipts. It is not a durable ledger or proof of Host delivery.
#[derive(Default)]
pub struct AuthenticatedBatchPrefix {
    members: Vec<AuthenticatedBatchMember>,
}

impl AuthenticatedBatchPrefix {
    pub fn new() -> Self { Self::default() }

    pub fn push(&mut self, member: AuthenticatedBatchMember) -> Result<(), AuthenticatedBatchMember> {
        if member.index() != self.members.len() { return Err(member); }
        self.members.push(member);
        Ok(())
    }

    pub fn members(&self) -> &[AuthenticatedBatchMember] { &self.members }
    pub fn into_members(self) -> Vec<AuthenticatedBatchMember> { self.members }

    pub fn pending_deliveries(&self) -> impl Iterator<Item = BatchMemberDeliveryPending<'_>> {
        self.members.iter().map(|member| BatchMemberDeliveryPending { member })
    }
}

/// Carries only authenticated prefix facts; later indices have not been resolved.
pub struct IncompleteAgentBatch {
    prefix: AuthenticatedBatchPrefix,
    failure: AgentBatchMemberFailure,
}

pub enum AgentBatchMemberFailure {
    Unknown(BatchMemberUnknown),
    Conflict(BatchMemberConflict),
    Commit { index: usize, error: crate::agentic::AgentActionServiceError },
}

impl AgentBatchMemberFailure {
    pub fn index(&self) -> usize {
        match self {
            Self::Unknown(failure) => failure.index,
            Self::Conflict(failure) => failure.index,
            Self::Commit { index, .. } => *index,
        }
    }
}

impl IncompleteAgentBatch {
    pub fn new(prefix: AuthenticatedBatchPrefix, failure: AgentBatchMemberFailure)
        -> Result<Self, (AuthenticatedBatchPrefix, AgentBatchMemberFailure)> {
        let index = failure.index();
        if index != prefix.members.len() { return Err((prefix, failure)); }
        Ok(Self { prefix, failure })
    }

    pub fn prefix(&self) -> &AuthenticatedBatchPrefix { &self.prefix }
    pub fn failure(&self) -> &AgentBatchMemberFailure { &self.failure }
    pub fn into_parts(self) -> (AuthenticatedBatchPrefix, AgentBatchMemberFailure) {
        (self.prefix, self.failure)
    }
    pub fn pending_deliveries(&self) -> impl Iterator<Item = BatchMemberDeliveryPending<'_>> {
        self.prefix.pending_deliveries()
    }
}

/// The original Runtime policy owner issues one independently assessed member.
#[async_trait::async_trait]
pub trait AgentBatchPolicyOwner: Send + Sync {
    async fn prepare_member_pregrant(&self, original: &RuntimeToolExecutionRequest, index: usize)
        -> AgentBatchMemberPregrant;

}

tokio::task_local! {
    pub(crate) static DELEGATED_BATCH_POLICY_OWNER: std::sync::Arc<dyn AgentBatchPolicyOwner>;
    pub(crate) static ORIGINAL_MEMBER_PROGRESS: ToolProgressSink;
    pub(crate) static ORIGINAL_EFFECT_REQUEST: std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<RuntimeToolExecutionRequest>>>>;
}

/// Same-call origin installed only by the original execution producer.
#[derive(Clone)]
pub(crate) enum OriginalEffectCustodyOrigin {
    ToolBatch(crate::execution_core::NodeExecutionTicket),
    DirectProcess,
}

#[derive(Clone, Default)]
pub struct ToolProgressSink {
    callback: Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
    child_reads: std::sync::Arc<std::sync::Mutex<Option<Vec<tools::OrchestratedRead>>>>,
    lifecycle_anchor: std::sync::Arc<std::sync::Mutex<Option<ToolLifecycleAnchor>>>,
    batch_policy_owner: std::sync::Arc<std::sync::Mutex<Option<std::sync::Weak<dyn AgentBatchPolicyOwner>>>>,
    original_fresh_effect: std::sync::Arc<std::sync::Mutex<Option<std::sync::Weak<crate::execution_core::graph::OriginalFreshEffectOwner>>>>,
    original_effect_custody: std::sync::Arc<std::sync::Mutex<Option<OriginalEffectCustodyOrigin>>>,
    original_conclusions: std::sync::Arc<std::sync::Mutex<Vec<(Option<usize>, std::sync::Arc<crate::agentic::OriginalCommittedConclusion>)>>>,
    original_singles: std::sync::Arc<std::sync::Mutex<Vec<std::sync::Arc<crate::agentic::OriginalCommittedSingle>>>>,
    original_governed_hook: std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<crate::conversation::tool_plane::OriginalGovernedHook>>>>,
    member_results: std::sync::Arc<std::sync::Mutex<Vec<(usize, std::sync::Arc<OriginalMemberSubmitOutcome>)>>>,
    member_stop: std::sync::Arc<OriginalStopChannel>,
    stop_publications: std::sync::Arc<std::sync::Mutex<Vec<std::sync::Arc<OriginalStopPublication>>>>,
}

impl ToolProgressSink {
    pub fn new(callback: Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>) -> Self {
        Self { callback, ..Self::default() }
    }
    pub(crate) fn same_original_channels(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.child_reads, &other.child_reads)
            && std::sync::Arc::ptr_eq(&self.lifecycle_anchor, &other.lifecycle_anchor)
            && std::sync::Arc::ptr_eq(&self.batch_policy_owner, &other.batch_policy_owner)
            && std::sync::Arc::ptr_eq(&self.original_fresh_effect, &other.original_fresh_effect)
            && std::sync::Arc::ptr_eq(&self.original_effect_custody, &other.original_effect_custody)
            && std::sync::Arc::ptr_eq(&self.original_conclusions, &other.original_conclusions)
            && std::sync::Arc::ptr_eq(&self.original_singles, &other.original_singles)
            && std::sync::Arc::ptr_eq(&self.original_governed_hook, &other.original_governed_hook)
            && std::sync::Arc::ptr_eq(&self.member_results, &other.member_results)
            && std::sync::Arc::ptr_eq(&self.member_stop, &other.member_stop)
            && std::sync::Arc::ptr_eq(&self.stop_publications, &other.stop_publications)
            && match (&self.callback, &other.callback) {
                (Some(left), Some(right)) => std::sync::Arc::ptr_eq(left, right),
                (None, None) => true,
                _ => false,
            }
    }
    pub fn callback(&self) -> Option<&std::sync::Arc<dyn Fn(&str) + Send + Sync>> {
        self.callback.as_ref()
    }
    pub(crate) fn install_original_effect_custody(&self, origin: OriginalEffectCustodyOrigin) -> Result<(), String> {
        let mut slot = self.original_effect_custody.lock().map_err(|_| "original custody channel poisoned")?;
        if slot.is_some() { return Err("original effect custody already installed".into()); }
        *slot = Some(origin);
        Ok(())
    }
    pub(crate) fn original_effect_custody(&self) -> Result<Option<OriginalEffectCustodyOrigin>, String> {
        self.original_effect_custody.lock().map(|value| value.clone()).map_err(|_| "original custody channel poisoned".into())
    }
    pub(crate) fn install_original_fresh_effect(
        &self,
        owner: std::sync::Arc<crate::execution_core::graph::OriginalFreshEffectOwner>,
    ) -> Result<(), String> {
        let mut slot = self.original_fresh_effect.lock().map_err(|_| "original effect owner channel poisoned")?;
        if slot.is_some() { return Err("original effect admission already installed".into()); }
        *slot = Some(std::sync::Arc::downgrade(&owner));
        Ok(())
    }
    pub(crate) fn original_fresh_effect(
        &self,
    ) -> Result<Option<std::sync::Arc<crate::execution_core::graph::OriginalFreshEffectOwner>>, String> {
        let slot = self.original_fresh_effect.lock().map_err(|_| "original effect owner channel poisoned")?;
        match slot.as_ref() {
            Some(owner) => owner.upgrade().map(Some).ok_or_else(|| "original effect owner no longer retained".into()),
            None => Ok(None),
        }
    }
    /// Preserve the original checked result under its single call or batch member index.
    /// This channel is not proof of physical delivery.
    pub(crate) fn record_original_conclusion(
        &self, index: Option<usize>, result: std::sync::Arc<crate::agentic::OriginalCommittedConclusion>,
    ) -> Result<(), String> {
        let poisoned = self.original_conclusions.is_poisoned();
        let mut values = self.original_conclusions.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let wrong_index = result.member_index() != index;
        let conflict = values.iter().any(|(old_index, old)|
            (*old_index == index || old.action_id() == result.action_id())
                && !(*old_index == index && old.same_original_commit(&result)));
        if !values.iter().any(|(old_index, old)| *old_index == index && old.same_original_commit(&result)) {
            values.push((index, result));
        }
        if poisoned { return Err("original conclusion channel poisoned".into()); }
        if wrong_index { return Err("original conclusion member index differs".into()); }
        if conflict { return Err("original conclusion channel conflict".into()); }
        Ok(())
    }
    pub(crate) fn original_conclusion(
        &self, index: Option<usize>, action_id: &str,
    ) -> Result<Option<std::sync::Arc<crate::agentic::OriginalCommittedConclusion>>, String> {
        let values = self.original_conclusions.lock().map_err(|_| "original conclusion channel poisoned")?;
        let mut matches = values.iter().filter(|(old_index, old)| *old_index == index || old.action_id() == action_id);
        let Some((old_index, result)) = matches.next() else { return Ok(None); };
        if matches.next().is_some() || *old_index != index || result.member_index() != index || result.action_id() != action_id {
            return Err("original conclusion channel conflict".into());
        }
        Ok(Some(std::sync::Arc::clone(result)))
    }
    /// Snapshot only original same-call conclusion handles; not dispatch authority.
    pub(crate) fn original_conclusion_entries(
        &self,
    ) -> Result<Vec<(Option<usize>, std::sync::Arc<crate::agentic::OriginalCommittedConclusion>)>, String> {
        let values = self.original_conclusions.lock().map_err(|_| "original conclusion channel poisoned")?;
        Ok(values.clone())
    }
    /// Same-call checked result transport; this does not attest physical delivery.
    pub(crate) fn record_original_single(
        &self, result: std::sync::Arc<crate::agentic::OriginalCommittedSingle>,
    ) -> Result<(), String> {
        let poisoned = self.original_singles.is_poisoned();
        let mut values = self.original_singles.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !values.iter().any(|old| std::sync::Arc::ptr_eq(old, &result)) { values.push(result); }
        if poisoned { return Err("original single channel poisoned".into()); }
        if values.len() != 1 { return Err("original single channel conflict".into()); }
        Ok(())
    }
    pub(crate) fn original_single(
        &self,
    ) -> Result<Option<std::sync::Arc<crate::agentic::OriginalCommittedSingle>>, String> {
        let values = self.original_singles.lock().map_err(|_| "original single channel poisoned")?;
        if values.len() > 1 { return Err("original single channel conflict".into()); }
        Ok(values.first().cloned())
    }
    /// In-process transport only: values were minted by the original owner.
    /// This slot is not a durable receipt, admission, or proof of delivery.
    pub fn record_original_member(&self, index: usize, result: std::sync::Arc<OriginalMemberSubmitOutcome>) {
        let mut values = self.member_results.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        values.push((index, result));
    }
    pub fn original_members(&self) -> Vec<(usize, std::sync::Arc<OriginalMemberSubmitOutcome>)> {
        self.member_results.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
    /// A poisoned stop slot is unresolved even if it contains no value.
    /// This only selects same-call transport; it establishes no authority.
    pub fn has_original_member_stop(&self) -> bool {
        !self.stop_publications.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_empty()
            || !matches!(self.original_member_stop(), OriginalStopRead::Available(None))
    }
    /// Same-call transport for direct owner publications; it grants nothing.
    pub fn record_original_stop_publication(&self, publication: std::sync::Arc<OriginalStopPublication>) {
        let mut values = self.stop_publications.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !values.iter().any(|old| std::sync::Arc::ptr_eq(old, &publication)) { values.push(publication); }
    }
    pub fn original_stop_publications(&self) -> Vec<std::sync::Arc<OriginalStopPublication>> {
        self.stop_publications.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
    /// Publish an owner-created stop only to the original request's own sink.
    pub fn record_original_member_stop(&self, stop: std::sync::Arc<OriginalBatchMemberStop>) -> OriginalStopPublication {
        if !stop.belongs_to_original_sink(self) {
            let previous = stop.coordinate.sink.upgrade().and_then(|sink| sink.current());
            return OriginalStopPublication::Unknown { current: stop, previous, failure: OriginalStopRecordFailure::WrongSink };
        }
        OriginalStopPublication::record(stop, &self.member_stop.slot)
    }
    pub fn original_member_stop(&self) -> OriginalStopRead {
        match self.member_stop.slot.lock() {
            Ok(slot) => OriginalStopRead::Available(slot.clone()),
            Err(poisoned) => OriginalStopRead::Poisoned(poisoned.into_inner().clone()),
        }
    }
    /// The node owner takes the stop after incorporating it in its original
    /// transaction/after_commit path; this also releases the sink's copy.
    pub fn take_original_member_stop(&self) -> OriginalStopRead {
        match self.member_stop.slot.lock() {
            Ok(mut slot) => OriginalStopRead::Available(slot.take()),
            Err(poisoned) => OriginalStopRead::Poisoned(poisoned.into_inner().clone()),
        }
    }
    pub(crate) fn install_original_governed_hook(
        &self,
        hook: std::sync::Arc<crate::conversation::tool_plane::OriginalGovernedHook>,
    ) -> Result<(), String> {
        let mut slot = self.original_governed_hook.lock().map_err(|_| "original hook channel poisoned")?;
        if slot.is_some() { return Err("original governed hook already installed".into()); }
        *slot = Some(hook);
        Ok(())
    }
    pub(crate) fn original_governed_hook(
        &self,
    ) -> Result<Option<std::sync::Arc<crate::conversation::tool_plane::OriginalGovernedHook>>, String> {
        Ok(self.original_governed_hook.lock().map_err(|_| "original hook channel poisoned")?.clone())
    }
    pub fn install_batch_policy_owner(&self, owner: std::sync::Arc<dyn AgentBatchPolicyOwner>) -> Result<(), String> {
        let mut slot = self.batch_policy_owner.lock().map_err(|_| "batch policy channel poisoned")?;
        if slot.is_some() { return Err("batch policy owner already installed".into()); }
        *slot = Some(std::sync::Arc::downgrade(&owner));
        Ok(())
    }
    pub fn batch_policy_owner(&self) -> Result<Option<std::sync::Arc<dyn AgentBatchPolicyOwner>>, String> {
        let slot = self.batch_policy_owner.lock().map_err(|_| "batch policy channel poisoned")?;
        match slot.as_ref() {
            Some(owner) => owner.upgrade().map(Some).ok_or_else(|| "original batch policy owner unavailable".into()),
            None => Ok(None),
        }
    }
    pub fn record_child_reads(&self, reads: Vec<tools::OrchestratedRead>) -> Result<(), String> {
        let mut slot = self.child_reads.lock().map_err(|_| "child read channel poisoned")?;
        if slot.is_some() { return Err("child read channel already populated".into()); }
        *slot = Some(reads);
        Ok(())
    }
    pub(crate) fn child_reads(&self) -> Result<Option<Vec<tools::OrchestratedRead>>, String> {
        Ok(self.child_reads.lock().map_err(|_| "child read channel poisoned")?.clone())
    }
    pub(crate) fn record_lifecycle_anchor(&self, anchor: ToolLifecycleAnchor) -> Result<(), String> {
        let mut slot = self.lifecycle_anchor.lock().map_err(|_| "tool lifecycle channel poisoned")?;
        if slot.is_some() { return Err("tool lifecycle anchor already populated".into()); }
        *slot = Some(anchor);
        Ok(())
    }
    pub(crate) fn lifecycle_anchor(&self) -> Result<Option<ToolLifecycleAnchor>, String> {
        Ok(self.lifecycle_anchor.lock().map_err(|_| "tool lifecycle channel poisoned")?.clone())
    }
}

impl std::fmt::Debug for ToolProgressSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ToolProgressSink")
            .field(&self.callback.is_some())
            .finish()
    }
}

impl PartialEq for ToolProgressSink {
    fn eq(&self, other: &Self) -> bool {
        self.callback.is_some() == other.callback.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_invocation: Option<harness_contract::tool::GovernedToolInvocation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_binding: Option<crate::step_snapshot::StepCallBinding>,
    pub governed_plan_id: String,
    pub governed_plan_revision: u64,
    /// Stable causal wave allocated before execution. Parallel calls in one
    /// ToolBatch share it; retry/replay retains it. It is never reconstructed
    /// from plan revision or post-hoc Goal observations.
    #[serde(default)]
    pub observation_wave_sequence: u64,
    pub idempotency_key: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub input: String,
    pub category: ToolSafetyCategory,
    /// Runtime-issued authorization for an ordinary tool effect. Gateway
    /// control tools do not use this field; delegated Agent tools carry it so
    /// the concrete ToolHost can verify the same descriptor before execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<harness_contract::tool::ToolExecutionAuthorization>,
    /// Logical parent session that owns this graph node. It is not inferred
    /// from a process-global tool host, because a single Gateway serves many
    /// concurrent sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Runtime-derived sandbox boundary for this invocation. It is derived
    /// from the owning Session autonomy profile, never from model input.
    #[serde(default)]
    pub sandbox_posture: harness_contract::policy::SandboxPosture,
    /// Exact Session policy generation paired with `sandbox_posture` and the
    /// authorization lease. Production ToolHosts reject zero or stale values.
    #[serde(default)]
    pub policy_revision: u64,
    /// Runtime-derived evidence/artifact scopes the caller is authorized to
    /// read. Derived from the owning Agent binding and session, never from
    /// model input. Empty means no scoped evidence access.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorized_scopes: Vec<String>,
    /// Exact Memory read binding compiled by the owning ConversationRuntime.
    ///
    /// Context retrieval must not reconstruct Agent/Definition/Team authority
    /// from a Session id at the Gateway boundary. Primary and delegated
    /// runtimes carry the same lease that passive context assembly uses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_context: Option<memory::MemoryTurnContext>,
    /// Validated Runtime Binding data lease shared with passive Fact/Matrix recall.
    /// Never populated from the model tool input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reality_context: Option<harness_contract::agent::AgentDataLease>,
    /// Selected parent model lease. Runtime control tools inherit this binding
    /// for any child AgentTask graph instead of resolving an imaginary default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_lease: Option<String>,
    /// Canonical parent graph/node that issued this tool call. It is supplied
    /// by the Runner, never parsed from model-generated tool JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    /// Attempt fence for a delegated Agent tool receipt.  Together with the
    /// parent graph/node it gives the durable receipt index an exact owner;
    /// it is never inferred from a tool-use label during recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_attempt: Option<u32>,
    /// Immutable execution decision for the owning conversation turn. Runtime
    /// supplies this lease so orchestration validation and Team identity never
    /// consult process-global Gateway state in concurrent sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_decision: Option<crate::RuntimeExecutionDecision>,
    /// Runtime-issued paired evaluation traffic. Tool adapters use this to
    /// enforce the evaluation isolation policy rather than trusting a model
    /// supplied session or tool parameter.
    #[serde(default)]
    pub evaluation_isolated: bool,
    /// Runtime-issued Managed Agent invocation fence.  Gateway may execute a
    /// side effect only after Runtime has recorded and claimed the matching
    /// outbox entry; conversational calls always leave this empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_invocation: Option<harness_contract::managed_agent::ManagedAgentInvocationFence>,
    /// Optional per-invocation progress sink used by bash's tokio path (T4).
    /// The callback is in-process only; it is never serialized.
    #[serde(skip)]
    pub tool_progress: ToolProgressSink,
}

impl RuntimeToolExecutionRequest {
    #[must_use]
    pub fn from_tool_request(request: &ToolRequest) -> Self {
        Self {
            compiled_invocation: None,
            step_binding: None,
            governed_plan_id: "offline-unknown".to_string(),
            governed_plan_revision: 0,
            observation_wave_sequence: 0,
            idempotency_key: request.tool_use_id.clone(),
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input: request.input.clone(),
            category: ToolSafetyCategory::Destructive,
            authorization: None,
            session_id: None,
            // Offline adapters have no Session policy. ReadOnly is the only
            // safe decode/default posture and production admission still
            // requires a positive policy revision plus exact live equality.
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 0,
            authorized_scopes: Vec::new(),
            memory_context: None,
            reality_context: None,
            model_lease: None,
            parent_execution: None,
            parent_execution_attempt: None,
            execution_decision: None,
            evaluation_isolated: false,
            managed_invocation: None,
            tool_progress: ToolProgressSink::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeToolExecutionStatus {
    Executed,
    BlockedPermission,
    Failed,
    /// The owner cannot yet determine whether the effect committed.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeToolExecutionOutcome {
    pub tool_use_id: String,
    pub tool_name: String,
    pub status: RuntimeToolExecutionStatus,
    pub category: ToolSafetyCategory,
    pub output: Option<String>,
    pub error: Option<String>,
    pub evidence_ref: String,
    /// Typed facts from the original execution, including bounded process
    /// failures, cancellation and unknown physical outcomes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_evidence: Vec<harness_contract::context::ObservedEvidence>,
    /// The original call's in-process batch facts, carried to the node owner.
    /// This is neither serialized nor a durable receipt or authorization.
    #[serde(skip)]
    pub pending_agent_batch: Option<crate::PendingAgentBatch>,
}

impl RuntimeToolExecutionOutcome {
    pub(crate) fn model_feedback_is_error(&self) -> bool {
        self.status != RuntimeToolExecutionStatus::Executed
            || bounded_process_feedback_is_error(&self.tool_name, &self.observed_evidence)
    }
}

pub(crate) fn bounded_process_feedback_is_error(
    tool_name: &str,
    observations: &[harness_contract::context::ObservedEvidence],
) -> bool {
    use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionStopReason, ToolExecutionTerminal};
    if tool_name != "execute_code" {
        return false;
    }
    let mut facts = observations.iter().filter_map(|observed| match &observed.target {
        EvidenceTargetIdentity::ToolExecution { execution } => Some((observed.tool_name.as_str(), execution)),
        _ => None,
    });
    let Some((observed_tool_name, fact)) = facts.next() else {
        return true;
    };
    facts.next().is_some()
        || observed_tool_name != tool_name
        || !matches!(&fact.terminal,
            ToolExecutionTerminal::Settled {
                exit_code: Some(0),
                signal: None,
                stop_reason: ToolExecutionStopReason::CommandExit,
                ..
            })
}

pub struct BoundedProcessAuthority {
    services: std::sync::Arc<crate::RuntimeServices>,
    request: RuntimeToolExecutionRequest,
    managed_started: bool,
    finalized: bool,
    #[cfg(test)]
    checkpoint: Option<(
        &'static str,
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
}

tokio::task_local! {
    static BOUNDED_PROCESS_COMMIT_AUTHORITY: (std::sync::Arc<crate::RuntimeServices>,RuntimeToolExecutionRequest);
}

pub(crate) fn check_bounded_process_commit_authority(
    request: &RuntimeToolExecutionRequest,
) -> Result<(), crate::ToolError> {
    BOUNDED_PROCESS_COMMIT_AUTHORITY
        .try_with(|(services, original)| {
            BoundedProcessAuthority::validate_context(services, original, request)
        })
        .map_err(|_| {
            crate::ToolError::new("bounded process terminal requires its owned live authority")
        })?
}

pub(crate) struct BoundedProcessAuthorityMonitor {
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Drop for BoundedProcessAuthorityMonitor {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl BoundedProcessAuthority {
    pub fn bind(
        services: std::sync::Arc<crate::RuntimeServices>,
        request: RuntimeToolExecutionRequest,
    ) -> Result<Self, crate::ToolError> {
        let authority = Self {
            services,
            request,
            managed_started: false,
            finalized: false,
            #[cfg(test)]
            checkpoint: None,
        };
        authority.check_context(&authority.request)?;
        Ok(authority)
    }

    #[cfg(test)]
    pub(crate) fn observe_checkpoint_for_test(
        &mut self,
        phase: &'static str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        assert!(self.checkpoint.is_none());
        let (notify, observed) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        self.checkpoint = Some((phase, notify, released));
        (observed, release)
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_for_test(&mut self, phase: &'static str) {
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|(selected, _, _)| *selected == phase)
        {
            let (_, notify, released) = self.checkpoint.take().unwrap();
            let _ = notify.send(());
            let _ = released.recv();
        }
    }

    pub(crate) fn check_context(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        Self::validate_context(&self.services, &self.request, request)
    }

    pub(crate) fn shares_store(
        &self,
        service: &crate::execution_core::graph::ExecutionCommitService,
    ) -> bool {
        service.bounded_process_uses_store(self.services.event_store())
    }

    pub(crate) fn shares_plane(&self, plane: &std::sync::Arc<crate::ToolExecutionPlane>) -> bool {
        std::sync::Arc::ptr_eq(self.services.tool_execution_plane(), plane)
    }

    pub(crate) async fn with_commit_context<Output, Operation>(
        &self,
        operation: Operation,
    ) -> Output
    where
        Operation: std::future::Future<Output = Output>,
    {
        BOUNDED_PROCESS_COMMIT_AUTHORITY
            .scope((self.services.clone(), self.request.clone()), operation)
            .await
    }

    fn validate_context(
        services: &crate::RuntimeServices,
        original: &RuntimeToolExecutionRequest,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        let error = |message: &str| crate::ToolError::new(message);
        if serde_json::to_value(original).map_err(|failure| error(&failure.to_string()))?
            != serde_json::to_value(request).map_err(|failure| error(&failure.to_string()))?
        {
            return Err(error("bounded process immutable authority request changed"));
        }
        let session = request
            .session_id
            .as_deref()
            .ok_or_else(|| error("bounded process Session missing"))?;
        let policy = services
            .session_execution_policy(session)
            .ok_or_else(|| error("bounded process live Session policy missing"))?;
        if request.policy_revision == 0
            || request.policy_revision != policy.revision
            || request.sandbox_posture != policy.sandbox_posture
        {
            return Err(error(
                "bounded process live Session authority revoked or changed",
            ));
        }
        if let Some(fence) = &request.managed_invocation {
            fence
                .validate()
                .map_err(|failure| error(&failure.to_string()))?;
            let invocation = services
                .managed_agents()
                .invocations()
                .map_err(|failure| error(&failure))?
                .into_iter()
                .find(|invocation| invocation.invocation_id == fence.invocation_id)
                .ok_or_else(|| error("bounded managed invocation missing"))?;
            if invocation.status != crate::ManagedAgentInvocationStatus::Running
                || invocation.fence_generation != fence.fence_generation
                || invocation.attempt_no != fence.attempt_no
                || invocation.definition_id != fence.managed_agent_id
                || invocation.definition_revision != fence.definition_revision
                || invocation.claimed_by.as_deref() != Some(fence.dispatcher_id.as_str())
            {
                return Err(error("bounded process managed invocation fence revoked"));
            }
        }
        Ok(())
    }

    pub(crate) fn check(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        self.check_context(request)?;
        let effect = &request
            .compiled_invocation
            .as_ref()
            .ok_or_else(|| crate::ToolError::new("bounded process compiled effect missing"))?
            .effect;
        self.services
            .commit_service()
            .bounded_process_binding(request, effect)
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        Ok(())
    }

    pub(crate) fn check_settlement(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        // Session/managed immutable context checks remain. Only fresh Root /
        // Program running authority is replaced by the original admission.
        self.check_context(request)?;
        let effect = &request
            .compiled_invocation
            .as_ref()
            .ok_or_else(|| crate::ToolError::new("Process settlement compiled effect missing"))?
            .effect;
        self.services
            .commit_service()
            .bounded_process_settlement_binding(request, effect)
            .map(|_| ())
            .map_err(|error| crate::ToolError::new(error.to_string()))
    }

    pub(crate) fn begin(
        &mut self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<(), crate::ToolError> {
        self.check(request)?;
        if self.managed_started {
            return Err(crate::ToolError::new(
                "bounded managed permit already consumed",
            ));
        }
        if let Some(fence) = &request.managed_invocation {
            let effect_id = format!("tool:{}:{}", request.tool_name, request.tool_use_id);
            match self
                .services
                .begin_managed_agent_effect(
                    fence,
                    &effect_id,
                    format!("runtime_tool:{:?}", request.category).to_ascii_lowercase(),
                    request.idempotency_key.clone(),
                    format!(
                        "runtime-tool:{}:{}",
                        request.tool_name, request.idempotency_key
                    ),
                )
                .map_err(|error| crate::ToolError::new(error.to_string()))?
            {
                crate::ManagedAgentEffectPermit::Execute { .. } => self.managed_started = true,
                crate::ManagedAgentEffectPermit::AlreadyCompleted { .. } => {
                    return Err(crate::ToolError::new(
                        "bounded managed effect already completed; refusing new execution",
                    ))
                }
            }
        }
        Ok(())
    }

    pub(crate) fn monitor(
        &self,
        request: &RuntimeToolExecutionRequest,
        cancellation: tools::ToolExecutionCancellation,
    ) -> Result<BoundedProcessAuthorityMonitor, crate::ToolError> {
        self.check(request)?;
        let services = self.services.clone();
        let original = self.request.clone();
        let request = request.clone();
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let captured = stopped.clone();
        let worker = std::thread::Builder::new()
            .name("bounded-process-authority".into())
            .spawn(move || {
                while !captured.load(std::sync::atomic::Ordering::Acquire) {
                    let valid =
                        Self::validate_context(&services, &original, &request).and_then(|()| {
                            let effect = &request
                                .compiled_invocation
                                .as_ref()
                                .ok_or_else(|| {
                                    crate::ToolError::new("bounded process compiled effect missing")
                                })?
                                .effect;
                            services
                                .commit_service()
                                .bounded_process_binding(&request, effect)
                                .map(|_| ())
                                .map_err(|error| crate::ToolError::new(error.to_string()))
                        });
                    if valid.is_err() {
                        cancellation.cancel();
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            })
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        Ok(BoundedProcessAuthorityMonitor {
            stopped,
            worker: Some(worker),
        })
    }

    pub(crate) fn complete(
        &mut self,
        request: &RuntimeToolExecutionRequest,
        receipt: &str,
    ) -> Result<(), crate::ToolError> {
        if self.finalized {
            return Err(crate::ToolError::new(
                "bounded process lifecycle already finalized",
            ));
        }
        self.check_settlement(request)?;
        if self.managed_started {
            self.services
                .complete_managed_agent_effect(
                    request.managed_invocation.as_ref().unwrap(),
                    &format!("tool:{}:{}", request.tool_name, request.tool_use_id),
                    receipt.into(),
                )
                .map_err(|error| crate::ToolError::new(error.to_string()))?;
        }
        self.finalized = true;
        Ok(())
    }

    pub(crate) fn reconcile(&mut self, reason: String) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        if self.managed_started {
            let _ = self.services.reconcile_managed_agent_effect(
                self.request.managed_invocation.as_ref().unwrap(),
                &format!(
                    "tool:{}:{}",
                    self.request.tool_name, self.request.tool_use_id
                ),
                reason,
            );
        }
    }
}

impl Drop for BoundedProcessAuthority {
    fn drop(&mut self) {
        self.reconcile(
            "bounded physical operation ended without an authoritative terminal completion".into(),
        );
    }
}

/// Thin adapter used by the canonical ToolBatch node executor.
///
/// Graph scheduling, dependency resolution, policy, retries, and lifecycle
/// commits are deliberately outside this contract and belong to the Runner.
#[async_trait::async_trait]
pub trait RuntimeExecutionHost: Send + Sync {
    fn bounded_process_authority(
        &self,
        _request: &RuntimeToolExecutionRequest,
    ) -> Result<BoundedProcessAuthority, crate::ToolError> {
        Err(crate::ToolError::new(
            "bounded process live authority is unsupported by this host",
        ))
    }
    fn bounded_process_lease(
        &self,
        _request: &RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, crate::ToolError> {
        Err(crate::ToolError::new(
            "bounded process execution is unsupported by this host",
        ))
    }

    fn bounded_process_plane(&self) -> Option<std::sync::Arc<crate::ToolExecutionPlane>> {
        None
    }

    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
        None
    }

    async fn execute_runtime_tool(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> RuntimeToolExecutionOutcome;

    /// Explicit in-process carrier; adapters must forward it or fail closed.
    async fn execute_runtime_tool_with_action_invocation(
        &self,
        request: &RuntimeToolExecutionRequest,
        _capability: &crate::agentic::action_invocation::VerifiedActionInvocation,
    ) -> RuntimeToolExecutionOutcome {
        RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: RuntimeToolExecutionStatus::BlockedPermission,
            category: request.category,
            output: None,
            error: Some("host cannot forward original action invocation".into()),
            evidence_ref: String::new(),
            observed_evidence: Vec::new(),
            pending_agent_batch: None,
        }
    }

    fn delegated_tool_definitions(
        &self,
        tool_names: &[String],
    ) -> Vec<crate::ProviderToolDefinition> {
        let Some(lease) = self.pin_step_catalog() else {
            return Vec::new();
        };
        let requested = tool_names.iter().cloned().collect();
        lease
            .snapshot()
            .catalog
            .definitions(Some(&requested))
            .into_iter()
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect()
    }

    /// Return the concrete host descriptor used to authorize a delegated
    /// tool. `None` means that the current host snapshot cannot execute it.
    fn delegated_tool_effect_descriptor(
        &self,
        _tool_name: &str,
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        None
    }
}

/// Whether a concrete effect descriptor is safe to carry through a bounded
/// delegated Team lease. Admission and Agent activation must share this
/// predicate or the latter can silently erase tools promised by the former.
#[must_use]
pub fn delegated_tool_effect_is_bounded(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> bool {
    use harness_contract::policy::{EffectBlastRadius, EffectExternality, EffectReversibility};
    use harness_contract::tool::{ToolApprovalClass, ToolEffectKind, ToolPermissionMode};

    if descriptor.mutates_packages
        || descriptor.mutates_system
        || matches!(
            descriptor.effect_kind,
            ToolEffectKind::Package
                | ToolEffectKind::System
                | ToolEffectKind::Destructive
                | ToolEffectKind::Unknown
        )
    {
        return false;
    }
    if !descriptor.spawns_process {
        return true;
    }

    // Process creation is delegable only when the concrete registered
    // effect proves a workspace-bounded sandbox. Arbitrary shell commands
    // remain DangerFullAccess/User effects and therefore fail this gate;
    // a model cannot manufacture these descriptor fields.
    !descriptor.uses_network
        && descriptor.required_permission != ToolPermissionMode::DangerFullAccess
        && matches!(
            descriptor.approval_class,
            ToolApprovalClass::None | ToolApprovalClass::Policy
        )
        && matches!(
            descriptor.assessment.externality,
            EffectExternality::Internal | EffectExternality::Workspace
        )
        && matches!(
            descriptor.assessment.blast_radius,
            EffectBlastRadius::Item | EffectBlastRadius::Workspace
        )
        && matches!(
            descriptor.assessment.reversibility,
            EffectReversibility::Reversible | EffectReversibility::Compensatable
        )
}

pub(crate) fn effect_file_accesses(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Vec<(String, bool)> {
    use harness_contract::policy::{PermissionOperation, PermissionResource};

    let mut accesses = descriptor
        .scopes
        .iter()
        .filter(|scope| scope.resource == PermissionResource::File)
        .map(|scope| {
            (
                scope
                    .target
                    .as_deref()
                    .filter(|target| !target.trim().is_empty())
                    .unwrap_or(".")
                    .to_string(),
                scope.operation != PermissionOperation::Read,
            )
        })
        .collect::<Vec<_>>();
    // A spawned process widens to the workspace root only when the tool's own
    // declared effect is unbounded. `bash` always declares an unbounded `Shell`
    // scope, so it keeps the widening. A tool that declares an explicitly
    // bounded scope set - the read-only sandboxed `compute`, whose only file
    // content is its declared `input_files` - runs its child inside that bound,
    // so the process adds no access of its own. Without this distinction a
    // bounded reduction could never be certified by a lease that covers the
    // declared inputs.
    // The bound that keeps a child inside it is a declared *file* scope: a
    // targeted Tool/Session scope says nothing about which files the child
    // touches, so a process-spawning tool that declares no bounded file scope
    // still reads the workspace and must keep the widening.
    // The widening follows the tool's own declared effect: a process that
    // declares an unbounded scope (a file scope without a target, or any
    // shell scope) can reach the workspace, so the workspace root is added.
    // A process that declares *bounded* file scopes - the read-only sandboxed
    // compute, whose only file content is its declared `input_files` - or no
    // file scope at all - the same compute with an empty file set, which
    // materialises no project file - is already confined and must not be
    // widened. Widening it would demand a whole-workspace lease for a
    // computation that observes exactly what it declared.
    let declares_unbounded_scope = descriptor.scopes.iter().any(|scope| {
        match scope.resource {
            PermissionResource::File => scope.target.as_deref().map(str::trim).unwrap_or("").is_empty(),
            PermissionResource::Shell => true,
            _ => false,
        }
    });
    if descriptor.spawns_process && declares_unbounded_scope {
        accesses.push((".".to_string(), false));
    }
    accesses.sort();
    accesses.dedup();
    accesses
}

pub(crate) fn effect_has_resource_lease(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    allowed_scopes: &[String],
    path_authorized: impl FnMut(&str, &[String], bool) -> bool,
) -> bool {
    delegated_tool_effect_is_bounded(descriptor)
        && effect_resource_grants_cover(descriptor, allowed_scopes, path_authorized)
}

fn effect_resource_grants_cover(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    allowed_scopes: &[String],
    mut path_authorized: impl FnMut(&str, &[String], bool) -> bool,
) -> bool {
    use harness_contract::policy::{PermissionOperation, PermissionResource};

    (!descriptor.uses_network || allowed_scopes.iter().any(|scope| scope == "network:*"))
        && descriptor.scopes.iter().all(|scope| {
            scope.resource != PermissionResource::File
                || matches!(
                    scope.operation,
                    PermissionOperation::Read | PermissionOperation::Write
                )
        })
        && effect_file_accesses(descriptor)
            .iter()
            .all(|(path, write)| {
                if crate::governed_tool_plan::normalize_resource_path(path) == "." {
                    whole_workspace_resource_lease(allowed_scopes, *write)
                } else {
                    path_authorized(path, allowed_scopes, *write)
                }
            })
}

pub(crate) fn whole_workspace_resource_lease(scopes: &[String], write: bool) -> bool {
    scopes.iter().any(|scope| {
        scope.split_once(':').is_some_and(|(mode, target)| {
            matches!(target.trim(), "." | "./")
                && (matches!(mode, "write" | "workspace") || (!write && mode == "read"))
        })
    })
}

pub(crate) fn effect_resource_evidence(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Vec<String> {
    let mut scopes = effect_file_accesses(descriptor)
        .into_iter()
        .map(|(path, write)| format!("{}:{path}", if write { "write" } else { "read" }))
        .collect::<Vec<_>>();
    if descriptor.uses_network {
        scopes.push("network:*".to_string());
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

pub(crate) fn lexical_resource_path_is_authorized(
    path: &str,
    scopes: &[String],
    write: bool,
) -> bool {
    let path = crate::governed_tool_plan::normalize_resource_path(path);
    if crate::governed_tool_plan::validate_relative_scope_path(&path).is_err() {
        return false;
    }
    scopes.iter().any(|scope| {
        let Some((mode, target)) = scope.split_once(':') else {
            return false;
        };
        if target.trim().is_empty() {
            return false;
        }
        if !matches!(mode, "write" | "workspace") && (write || mode != "read") {
            return false;
        }
        let target = crate::governed_tool_plan::normalize_resource_path(target);
        crate::governed_tool_plan::validate_relative_scope_path(&target).is_ok()
            && (target == "."
                || path == target
                || path
                    .strip_prefix(&target)
                    .is_some_and(|suffix| suffix.starts_with('/')))
    })
}

pub(crate) struct DelegatedCatalogProjection<'lease> {
    lease: &'lease tools::ToolHostLease,
    metadata: &'lease harness_contract::tool::ToolDiscoveryReceipt,
    registered: std::collections::BTreeMap<
        String,
        (
            crate::ProviderToolDefinition,
            harness_contract::tool::ToolPermissionMode,
        ),
    >,
    descriptors: std::collections::BTreeMap<
        &'lease str,
        Option<&'lease harness_contract::tool::ToolDescriptorRef>,
    >,
    original_descriptors: std::collections::BTreeMap<
        String,
        harness_contract::tool::ToolDescriptorRef,
    >,
}

impl<'lease> DelegatedCatalogProjection<'lease> {
    pub(crate) fn new(
        lease: &'lease tools::ToolHostLease,
        metadata: &'lease harness_contract::tool::ToolDiscoveryReceipt,
        requested: &std::collections::BTreeSet<String>,
    ) -> Self {
        Self {
            lease,
            metadata,
            registered: lease
                .snapshot()
                .catalog
                .kernel_definitions(Some(requested))
                .into_iter()
                .map(|definition| {
                    (
                        definition.name.clone(),
                        (
                            crate::ProviderToolDefinition {
                                name: definition.name,
                                description: definition.description,
                                input_schema: definition.input_schema,
                            },
                            definition.required_permission,
                        ),
                    )
                })
                .collect(),
            descriptors: {
                let mut descriptors = std::collections::BTreeMap::new();
                for descriptor in metadata.descriptors.iter().filter(|d| requested.contains(&d.canonical_id)) {
                    descriptors.entry(descriptor.canonical_id.as_str())
                        .and_modify(|entry| *entry = None)
                        .or_insert(Some(descriptor));
                }
                descriptors
            },
            original_descriptors: lease.catalog_receipt().descriptors.into_iter()
                .map(|descriptor| (descriptor.canonical_id.clone(), descriptor)).collect(),
        }
    }

    pub(crate) fn definitions(&self) -> impl Iterator<Item = &crate::ProviderToolDefinition> {
        self.registered.values().map(|(definition, _)| definition)
    }

    pub(crate) fn registered(&self, definition: &crate::ProviderToolDefinition) -> bool {
        self.registered
            .get(&definition.name)
            .is_some_and(|(original, _)| {
                original.input_schema == definition.input_schema
                    && original.description == definition.description
            })
    }

    pub(crate) fn discover(
        &self,
        definition: &crate::ProviderToolDefinition,
        requested: &std::collections::BTreeSet<String>,
        permission_ceiling: harness_contract::tool::ToolPermissionMode,
        _scopes: Option<&[String]>,
    ) -> Option<harness_contract::tool::ToolDescriptorRef> {
        use harness_contract::tool::ToolDescriptorHealth;

        let tool_name = &definition.name;
        let lease = self.lease;
        let metadata = self.metadata;
        let catalog = &lease.snapshot().catalog;
        if !requested.contains(tool_name)
            || catalog.canonical_name(tool_name).as_ref() != Some(tool_name)
            || metadata.catalog_revision != lease.revision()
        {
            return None;
        }
        let (registered, floor) = self.registered.get(tool_name)?;
        if registered.input_schema != definition.input_schema
            || registered.description != definition.description
        {
            return None;
        }
        let descriptor = (*self.descriptors.get(tool_name.as_str())?)?;
        if descriptor.health != ToolDescriptorHealth::Healthy
            || !permission_ceiling.permits(*floor)
            || !permission_ceiling.permits(descriptor.required_permission)
            || self.original_descriptors.get(tool_name) != Some(descriptor)
        {
            return None;
        }
        let resolver = catalog.effect_resolver(tool_name);
        // Discovery authenticates the registered interface, not the effect of
        // a fictional empty invocation. Actual input and scope are checked by
        // the original per-call admission before any effect can run.
        let registered_resolver = match descriptor.source.as_str() {
            "builtin" => resolver == tools::builtin_effect_resolver_spec(tool_name)
                && resolver.resolver_id != "builtin.unknown",
            "runtime" => catalog.has_runtime_tool(tool_name)
                && resolver.resolver_id.starts_with("runtime.")
                && resolver.resolver_id != "runtime.unknown"
                // `runtime.action_*` resolvers describe a particular builtin
                // action and cannot be transplanted into a runtime registration.
                && !resolver.resolver_id.starts_with("runtime.action_")
                && resolver.resolver_version != 0,
            _ => false,
        };
        registered_resolver.then(|| descriptor.clone())
    }
}

#[cfg(test)]
pub(crate) fn discover_delegated_tool(
    lease: &tools::ToolHostLease,
    metadata: &harness_contract::tool::ToolDiscoveryReceipt,
    definition: &crate::ProviderToolDefinition,
    requested: &std::collections::BTreeSet<String>,
    permission_ceiling: harness_contract::tool::ToolPermissionMode,
    scopes: Option<&[String]>,
) -> Option<harness_contract::tool::ToolDescriptorRef> {
    DelegatedCatalogProjection::new(lease, metadata, requested).discover(
        definition,
        requested,
        permission_ceiling,
        scopes,
    )
}

#[cfg(test)]
mod resource_projection_tests {
    use super::*;

    /// A process that declares exactly its own file scope is not widened to
    /// the workspace root: the sandbox copies only those files, so demanding a
    /// whole-workspace lease for it would be a wider authority than the tool
    /// can actually use.
    #[test]
    fn a_bounded_process_is_not_widened_to_the_workspace_root() {
        let mut descriptor = harness_contract::tool::ToolEffectDescriptor {
            tool_id: "execute_code".to_string(),
            descriptor_hash: "sha256:bounded".to_string(),
            effect_kind: harness_contract::tool::ToolEffectKind::Process,
            idempotency: harness_contract::tool::ToolIdempotency::Unknown,
            scopes: vec![harness_contract::policy::PermissionScope {
                resource: harness_contract::policy::PermissionResource::File,
                operation: harness_contract::policy::PermissionOperation::Read,
                target: Some("crates/tools/src/lib.rs".to_string()),
            }],
            required_permission: harness_contract::tool::ToolPermissionMode::ReadOnly,
            approval_class: harness_contract::tool::ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        };
        let accesses = effect_file_accesses(&descriptor);
        assert!(
            !accesses.iter().any(|(target, _)| target == "."),
            "a bounded compute must not be widened: {accesses:?}"
        );
        // A process that declares no file scope at all observes no project
        // file either (the sandbox materialises none), so it is not widened.
        descriptor.scopes.clear();
        let accesses = effect_file_accesses(&descriptor);
        assert!(
            !accesses.iter().any(|(target, _)| target == "."),
            "a compute with no project file must not be widened: {accesses:?}"
        );
        // An unbounded scope is still widened: the process can reach the
        // workspace, so the honest scope is the workspace root.
        descriptor.scopes.push(harness_contract::policy::PermissionScope {
            resource: harness_contract::policy::PermissionResource::File,
            operation: harness_contract::policy::PermissionOperation::Read,
            target: None,
        });
        let accesses = effect_file_accesses(&descriptor);
        assert!(
            accesses.iter().any(|(target, _)| target == "."),
            "an unbounded process must keep the workspace root: {accesses:?}"
        );
    }

    #[test]
    fn process_frame_projection_preserves_each_mutation_and_network_grant() {
        let root = tempfile::tempdir().unwrap();
        let lease = tools::ToolHost::builtin("process-frame-contract", root.path()).pin_snapshot();
        let input = serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"target.txt","content":"x"}},
            {"id":"compute","op":"compute","language":"python","code":"print(1)"}
        ]});
        let effect = lease.describe_effect("tool_orchestrate", &input);
        assert!(effect.spawns_process);
        assert!(!effect.uses_network);
        projection(
            &effect,
            &[
                (".", false),
                (".cowd/checkpoints", true),
                ("target.txt", false),
                ("target.txt", true),
            ],
        );
        let projected = crate::governed_tool_plan::resource_scope_from_effect(&effect);
        assert_eq!(projected.kind, "workspace");
        assert_eq!(projected.paths, vec!["."]);
        assert!(!projected.network && !projected.unknown);
        let grants = ["read:.", "write:target.txt", "write:.cowd/checkpoints"];
        assert!(permits(&effect, &grants));
        for missing in 0..grants.len() {
            let mut narrow = grants.to_vec();
            narrow.remove(missing);
            assert!(!permits(&effect, &narrow), "missing {}", grants[missing]);
        }
        assert!(!permits(
            &effect,
            &[
                "read:target.txt",
                "write:target.txt",
                "write:.cowd/checkpoints"
            ]
        ));
        let mixed = lease.describe_effect("tool_orchestrate", &serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"target.txt","content":"x"}},
            {"id":"compute","op":"compute","language":"python","code":"print(1)"},
            {"id":"fetch","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"metadata only"}}
        ]}));
        assert!(mixed.spawns_process && mixed.uses_network);
        let projected = crate::governed_tool_plan::resource_scope_from_effect(&mixed);
        assert_eq!(projected.kind, "workspace");
        assert_eq!(projected.paths, vec!["."]);
        assert!(projected.network && !projected.unknown);
        projection(
            &mixed,
            &[
                (".", false),
                (".cowd/checkpoints", true),
                ("target.txt", false),
                ("target.txt", true),
            ],
        );
        let covers = |descriptor: &ToolEffectDescriptor, grants: &[&str]| {
            let grants = grants
                .iter()
                .map(|grant| grant.to_string())
                .collect::<Vec<_>>();
            effect_resource_grants_cover(descriptor, &grants, lexical_resource_path_is_authorized)
        };
        let complete = [
            "read:.",
            "write:target.txt",
            "write:.cowd/checkpoints",
            "network:*",
        ];
        assert!(covers(&mixed, &complete));
        assert!(!delegated_tool_effect_is_bounded(&mixed));
        assert!(!permits(&mixed, &complete));
        assert!(!permits(&mixed, &grants));
        for missing in 0..complete.len() {
            let mut narrow = complete.to_vec();
            narrow.remove(missing);
            assert!(!covers(&mixed, &narrow), "missing {}", complete[missing]);
            assert!(!permits(&mixed, &narrow));
        }
        for wrong in [
            vec![
                "read:target.txt",
                "write:target.txt",
                "write:.cowd/checkpoints",
                "network:*",
            ],
            vec![
                "read:.",
                "write:target.txt-sibling",
                "write:.cowd/checkpoints",
                "network:*",
            ],
            vec![
                "read:.",
                "read:target.txt",
                "write:.cowd/checkpoints",
                "network:*",
            ],
            vec![
                "read:.",
                "write:target.txt",
                "read:.cowd/checkpoints",
                "network:*",
            ],
            vec![
                "read:.",
                "write:target.txt",
                "write:.cowd/checkpoints-sibling",
                "network:*",
            ],
            vec![
                "read:.",
                "write:target.txt",
                "write:.cowd/checkpoints",
                "network:example.invalid",
            ],
        ] {
            assert!(!covers(&mixed, &wrong));
            assert!(!permits(&mixed, &wrong));
        }
        let network = lease.describe_effect("tool_orchestrate", &serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"target.txt","content":"x"}},
            {"id":"fetch","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"metadata only"}}
        ]}));
        assert!(network.uses_network && !network.spawns_process);
        assert!(delegated_tool_effect_is_bounded(&network));
        let projected = crate::governed_tool_plan::resource_scope_from_effect(&network);
        assert_eq!(projected.kind, "paths");
        assert_eq!(projected.paths, vec![".cowd/checkpoints", "target.txt"]);
        assert!(projected.network && !projected.unknown);
        projection(
            &network,
            &[
                (".cowd/checkpoints", true),
                ("target.txt", false),
                ("target.txt", true),
            ],
        );
        let network_grants = ["write:target.txt", "write:.cowd/checkpoints", "network:*"];
        assert!(covers(&network, &network_grants));
        assert!(permits(&network, &network_grants));
        for missing in 0..network_grants.len() {
            let mut narrow = network_grants.to_vec();
            narrow.remove(missing);
            assert!(
                !covers(&network, &narrow),
                "missing {}",
                network_grants[missing]
            );
            assert!(!permits(&network, &narrow));
        }
        let mut unsupported = network.clone();
        unsupported.scopes.push(scope(
            PermissionResource::File,
            PermissionOperation::Delete,
            Some("target.txt"),
        ));
        assert!(!covers(&unsupported, &complete));
        assert!(!permits(&unsupported, &complete));
    }
    use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
    use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind, ToolPermissionMode};

    fn scope(
        resource: PermissionResource,
        operation: PermissionOperation,
        target: Option<&str>,
    ) -> PermissionScope {
        PermissionScope {
            resource,
            operation,
            target: target.map(str::to_string),
        }
    }

    fn permits(descriptor: &ToolEffectDescriptor, allowed: &[&str]) -> bool {
        let allowed = allowed
            .iter()
            .map(|scope| scope.to_string())
            .collect::<Vec<_>>();
        effect_has_resource_lease(descriptor, &allowed, lexical_resource_path_is_authorized)
    }

    fn projection(descriptor: &ToolEffectDescriptor, expected: &[(&str, bool)]) {
        let expected = expected
            .iter()
            .map(|(path, write)| (path.to_string(), *write))
            .collect::<Vec<_>>();
        assert_eq!(effect_file_accesses(descriptor), expected);
        let mut evidence = expected
            .iter()
            .map(|(path, write)| format!("{}:{path}", if *write { "write" } else { "read" }))
            .collect::<Vec<_>>();
        if descriptor.uses_network {
            evidence.push("network:*".to_string());
        }
        evidence.sort();
        evidence.dedup();
        assert_eq!(effect_resource_evidence(descriptor), evidence);
    }

    #[test]
    fn r1_composite_discovery_pinned_catalog_does_not_authorize_unknown_calls() {
        let directory = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("pinned-discovery-contract", directory.path());
        let lease = host.pin_snapshot();
        let metadata = lease.catalog_receipt();
        let registered = lease
            .snapshot()
            .catalog
            .kernel_definitions(None)
            .into_iter()
            .find(|definition| definition.name == "read_file")
            .unwrap();
        let definition = crate::ProviderToolDefinition {
            name: registered.name,
            description: registered.description,
            input_schema: registered.input_schema,
        };
        let requested = std::collections::BTreeSet::from([definition.name.clone()]);
        let scopes = vec!["read:.".to_string()];
        assert!(discover_delegated_tool(
            &lease,
            &metadata,
            &definition,
            &requested,
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_some());
        let mut duplicate = metadata.clone();
        duplicate.descriptors.push(metadata.descriptors.iter()
            .find(|descriptor| descriptor.canonical_id == definition.name).unwrap().clone());
        assert!(discover_delegated_tool(
            &lease, &duplicate, &definition, &requested, ToolPermissionMode::ReadOnly, Some(&scopes)
        ).is_none(), "duplicate catalog identity must not be resolved by map overwrite");
        let mut forged = definition.clone();
        forged.name = "unregistered_discovery_call".to_string();
        let mut forged_metadata = metadata.clone();
        let mut descriptor = metadata
            .descriptors
            .iter()
            .find(|descriptor| descriptor.canonical_id == definition.name)
            .unwrap()
            .clone();
        descriptor.canonical_id = forged.name.clone();
        descriptor.required_permission = ToolPermissionMode::ReadOnly;
        descriptor.health = harness_contract::tool::ToolDescriptorHealth::Healthy;
        forged_metadata.descriptors.push(descriptor);
        let forged_requested = std::collections::BTreeSet::from([forged.name.clone()]);
        assert!(discover_delegated_tool(
            &lease,
            &forged_metadata,
            &forged,
            &forged_requested,
            ToolPermissionMode::DangerFullAccess,
            Some(&scopes)
        )
        .is_none());
        assert_eq!(
            lease
                .describe_effect(&forged.name, &serde_json::json!({}))
                .effect_kind,
            ToolEffectKind::Unknown
        );
        let mut stale = metadata.clone();
        stale.catalog_revision = stale.catalog_revision.checked_add(1).unwrap();
        assert!(discover_delegated_tool(
            &lease,
            &stale,
            &definition,
            &requested,
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_none());
        let mut missing = metadata.clone();
        missing
            .descriptors
            .retain(|descriptor| descriptor.canonical_id != definition.name);
        assert!(discover_delegated_tool(
            &lease,
            &missing,
            &definition,
            &requested,
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_none());
        assert!(discover_delegated_tool(
            &lease,
            &metadata,
            &definition,
            &std::collections::BTreeSet::new(),
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_none());
        let mut changed = definition.clone();
        changed.input_schema = serde_json::json!({"type":"string"});
        assert!(discover_delegated_tool(
            &lease,
            &metadata,
            &changed,
            &requested,
            ToolPermissionMode::ReadOnly,
            Some(&scopes)
        )
        .is_none());
        assert_eq!(host.pin_snapshot().revision(), lease.revision());
    }

    #[test]
    fn parameterized_discovery_does_not_authorize_unbounded_or_out_of_scope_calls() {
        let directory = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("bounded-discovery", directory.path());
        let lease = host.pin_snapshot();
        let metadata = lease.catalog_receipt();
        let name = "execute_code";
        let definition = lease.snapshot().catalog.kernel_definitions(None).into_iter()
            .find(|definition| definition.name == name).unwrap();
        let definition = crate::ProviderToolDefinition {
            name: definition.name, description: definition.description, input_schema: definition.input_schema,
        };
        let requested = std::collections::BTreeSet::from([name.to_string()]);
        let scope = vec!["read:src/a.rs".to_string()];
        assert!(discover_delegated_tool(&lease, &metadata, &definition, &requested,
            ToolPermissionMode::ReadOnly, Some(&scope)).is_some());
        let input = serde_json::json!({"language":"python","code":"print(1)","input_files":["src/a.rs"]});
        let outside = serde_json::json!({"language":"python","code":"print(1)","input_files":["src/b.rs"]});
        assert!(effect_has_resource_lease(&lease.describe_effect(name, &input), &scope,
            lexical_resource_path_is_authorized));
        assert!(!effect_has_resource_lease(&lease.describe_effect(name, &outside), &scope,
            lexical_resource_path_is_authorized));
        let mut forged = metadata.clone();
        forged.descriptors.iter_mut().find(|d| d.canonical_id == name).unwrap().source = "runtime".into();
        assert!(discover_delegated_tool(&lease, &forged, &definition, &requested,
            ToolPermissionMode::ReadOnly, Some(&scope)).is_none());
    }

    #[test]
    fn r1_composite_discovery_edit_family_keeps_concrete_checkpoint_authority() {
        let directory = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("edit-discovery-contract", directory.path())
            .with_authorization_lease_verifier(std::sync::Arc::new(
                crate::AuthorizationNegotiator::verify_lease_signature,
            ));
        let lease = host.pin_snapshot();
        let metadata = lease.catalog_receipt();
        let definition = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .find(|definition| definition.name == "edit")
            .unwrap();
        let definition = crate::ProviderToolDefinition {
            name: definition.name,
            description: definition.description,
            input_schema: definition.input_schema,
        };
        let requested = std::collections::BTreeSet::from(["edit".to_string()]);
        let discover = |receipt: &harness_contract::tool::ToolDiscoveryReceipt,
                        definition: &crate::ProviderToolDefinition,
                        ceiling,
                        allowed: &[&str]| {
            discover_delegated_tool(
                &lease,
                receipt,
                definition,
                &requested,
                ceiling,
                Some(
                    &allowed
                        .iter()
                        .map(|scope| scope.to_string())
                        .collect::<Vec<_>>(),
                ),
            )
        };
        let grants = ["read:.", "write:out/a", "write:.cowd/checkpoints"];
        assert!(discover(
            &metadata,
            &definition,
            ToolPermissionMode::WorkspaceWrite,
            &grants
        )
        .is_some());
        assert!(discover(
            &metadata,
            &definition,
            ToolPermissionMode::WorkspaceWrite,
            &["read:.", "write:out/a"]
        )
        .is_some());
        assert!(discover(
            &metadata,
            &definition,
            ToolPermissionMode::ReadOnly,
            &grants
        )
        .is_none());
        for allowed in [
            vec![],
            vec!["write:../escape"],
            vec!["write:/absolute"],
            vec!["write:"],
            vec!["network:*"],
        ] {
            assert!(discover(
                &metadata,
                &definition,
                ToolPermissionMode::WorkspaceWrite,
                &allowed
            )
            .is_some(), "discovery does not authorize a particular input");
        }
        for mutation in 0..5 {
            let mut altered = metadata.clone();
            let descriptor = altered
                .descriptors
                .iter_mut()
                .find(|descriptor| descriptor.canonical_id == "edit")
                .unwrap();
            match mutation {
                0 => descriptor.source = "custom".into(),
                1 => descriptor.health = harness_contract::tool::ToolDescriptorHealth::Unavailable,
                2 => descriptor.required_permission = ToolPermissionMode::DangerFullAccess,
                3 => descriptor.canonical_id = "shadow_edit".into(),
                _ => altered.catalog_revision += 1,
            }
            assert!(discover(
                &altered,
                &definition,
                ToolPermissionMode::WorkspaceWrite,
                &grants
            )
            .is_none());
        }
        for mutation in 0..3 {
            let mut altered = definition.clone();
            match mutation {
                0 => altered.name = "write_file".into(),
                1 => altered.description = Some("shadow".into()),
                _ => altered.input_schema = serde_json::json!({"type":"string"}),
            }
            assert!(discover(
                &metadata,
                &altered,
                ToolPermissionMode::WorkspaceWrite,
                &grants
            )
            .is_none());
        }
        let input = serde_json::json!({"action":"write","path":"out/a","content":"actual target"});
        let concrete = lease.describe_effect("edit", &input);
        assert!(!effect_has_resource_lease(&concrete, &[], lexical_resource_path_is_authorized));
        assert_eq!(
            concrete.assessment.data_sensitivity,
            harness_contract::policy::DataClassification::Internal
        );
        assert_eq!(
            tools::builtin_effect_resolver_spec("edit").resolver_version,
            3
        );
        assert_eq!(
            crate::AuthorizationNegotiator::compile_effective_descriptor(
                &concrete,
                &input.to_string()
            )
            .descriptor
            .required_permission,
            ToolPermissionMode::WorkspaceWrite
        );
        projection(
            &concrete,
            &[
                (".cowd/checkpoints", true),
                ("out/a", false),
                ("out/a", true),
            ],
        );
        for target in [".env", ".ssh/id_rsa", "credentials.json"] {
            let sensitive = lease.describe_effect(
                "edit",
                &serde_json::json!({"action":"write","path":target,"content":"secret"}),
            );
            assert_eq!(
                sensitive.assessment.data_sensitivity,
                harness_contract::policy::DataClassification::Secret
            );
            assert_eq!(
                crate::AuthorizationNegotiator::compile_effective_descriptor(&sensitive, "{}")
                    .descriptor
                    .required_permission,
                ToolPermissionMode::DangerFullAccess
            );
        }
        const SDK2_EFFECT: &str = r###"{
    "approval_class": "policy",
    "assessment": {
      "blast_radius": "workspace",
      "data_sensitivity": "secret",
      "externality": "workspace",
      "novelty": "routine",
      "reversibility": "compensatable"
    },
    "descriptor_hash": "a556282fb072b286",
    "effect_kind": "write",
    "idempotency": "idempotent_with_key",
    "mutates_packages": false,
    "mutates_system": false,
    "required_permission": "workspace-write",
    "scopes": [
      {
        "operation": "read",
        "resource": "file",
        "target": null
      },
      {
        "operation": "write",
        "resource": "file",
        "target": ".cowd/checkpoints"
      },
      {
        "operation": "write",
        "resource": "file",
        "target": "out/a"
      }
    ],
    "spawns_process": false,
    "tool_id": "edit",
    "uses_network": false
  }"###;
        const SDK2_INPUT: &str = r###"{
    "action": "write",
    "content": "captured-v2-edit",
    "path": "out/a"
  }"###;
        let historical: ToolEffectDescriptor = serde_json::from_str(SDK2_EFFECT).unwrap();
        let historical_input: serde_json::Value = serde_json::from_str(SDK2_INPUT).unwrap();
        assert_eq!(historical.descriptor_hash, "a556282fb072b286");
        assert_eq!(historical.effect_kind, ToolEffectKind::Write);
        assert_eq!(
            historical.assessment.data_sensitivity,
            harness_contract::policy::DataClassification::Secret
        );
        projection(
            &historical,
            &[(".", false), (".cowd/checkpoints", true), ("out/a", true)],
        );
        let narrow = ["read:out/a", "write:out/a", "write:.cowd/checkpoints"];
        assert!(!permits(&historical, &narrow));
        assert!(!permits(
            &historical,
            &["write:out/a", "write:.cowd/checkpoints"]
        ));
        let current = lease.describe_effect("edit", &historical_input);
        assert_ne!(current.descriptor_hash, historical.descriptor_hash);
        assert!(permits(&current, &narrow));
        assert!(permits(
            &current,
            &["write:out/a", "write:.cowd/checkpoints"]
        ));
        let historical_effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
            &historical,
            &historical_input.to_string(),
        );
        assert_eq!(
            historical_effective.descriptor.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
        let request = crate::AuthorizationRequest {
            principal_id: "sdk3-consumer".into(),
            capability: "edit".into(),
            input: historical_input.to_string(),
            idempotency_key: "sdk3-consumer".into(),
            effect: current,
            parent_ceiling: ToolPermissionMode::WorkspaceWrite,
            parent_lease_id: None,
            policy_revision: 1,
            recovery_scope: "sdk3-consumer".into(),
            context: crate::PermissionContext::default(),
            safe_alternatives: vec![],
        };
        let evaluated = crate::AuthorizationNegotiator::new().assess_effective(
            &crate::permissions::PermissionPolicy::new(ToolPermissionMode::WorkspaceWrite),
            &request,
        );
        let signed = evaluated
            .assessment
            .lease
            .clone()
            .expect("current v3 exact edit receives its own signed lease");
        assert!(crate::AuthorizationNegotiator::verify_lease_signature(
            &signed
        ));
        let authorized = crate::ToolPolicy
            .authorize(
                &evaluated.effective,
                &evaluated.assessment,
                &request.idempotency_key,
                signed.clone(),
                30,
            )
            .unwrap();
        assert!(lease
            .validate_authorization(&authorized.authorization, "edit", &historical_input)
            .is_ok());
        let mut forged = authorized.authorization.clone();
        forged.authorization_lease.signature.push_str("-forged");
        assert_eq!(
            lease
                .validate_authorization(&forged, "edit", &historical_input)
                .unwrap_err(),
            tools::ToolHostError::InvalidLeaseSignature
        );
        let error = crate::ToolPolicy
            .authorize(
                &historical_effective,
                &evaluated.assessment,
                &request.idempotency_key,
                signed,
                30,
            )
            .unwrap_err()
            .to_string();
        assert_eq!(error, "authorization lease does not permit required mode");
        assert_eq!(historical.descriptor_hash, "a556282fb072b286");
        assert!(permits(&concrete, &grants));
        assert!(permits(
            &concrete,
            &["write:out/a", "write:.cowd/checkpoints"]
        ));
        for allowed in [
            vec!["read:.", "write:out/a"],
            vec!["read:.", "write:.cowd/checkpoints"],
            vec!["read:.", "write:out/b", "write:.cowd/checkpoints"],
        ] {
            assert!(!permits(&concrete, &allowed));
        }
        for target in ["out/b", ".", "../escape", "/tmp/escape", ".cowd/outside"] {
            let effect = lease.describe_effect(
                "edit",
                &serde_json::json!({"action":"write","path":target,"content":"no"}),
            );
            assert!(!permits(&effect, &grants), "{target}");
        }
        let sibling = lease.describe_effect(
            "edit",
            &serde_json::json!({"action":"write","path":"out/b","content":"other"}),
        );
        assert!(effect_file_accesses(&concrete).contains(&(".cowd/checkpoints".into(), true)));
        assert!(
            effect_file_accesses(&sibling).contains(&(".cowd/checkpoints".into(), true)),
            "distinct targets still overlap on checkpoint authority"
        );
        let custom = tools::RuntimeToolDefinition {
            name: "custom_edit".into(),
            description: Some("not builtin".into()),
            input_schema: serde_json::json!({"type":"object"}),
            required_permission: ToolPermissionMode::WorkspaceWrite,
            effect_resolver: tools::builtin_effect_resolver_spec("edit"),
        };
        let mut shadow = custom.clone();
        shadow.name = "edit".into();
        assert!(tools::ToolCatalog::builtin()
            .with_runtime_tools(vec![shadow])
            .is_err());
        let catalog = tools::ToolCatalog::builtin()
            .with_runtime_tools(vec![custom])
            .unwrap();
        let custom_host = tools::ToolHost::new(
            "custom-edit",
            directory.path(),
            tools::ToolHostSnapshot::new(
                std::sync::Arc::new(catalog),
                std::sync::Arc::new(tools::lsp_client::LspRegistry::default()),
                None,
            ),
        );
        let custom_lease = custom_host.pin_snapshot();
        let custom_metadata = custom_lease.catalog_receipt();
        let custom_definition = custom_lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .find(|definition| definition.name == "custom_edit")
            .unwrap();
        assert!(discover_delegated_tool(
            &custom_lease,
            &custom_metadata,
            &crate::ProviderToolDefinition {
                name: custom_definition.name,
                description: custom_definition.description,
                input_schema: custom_definition.input_schema,
            },
            &std::collections::BTreeSet::from(["custom_edit".into()]),
            ToolPermissionMode::WorkspaceWrite,
            Some(
                &grants
                    .iter()
                    .map(|scope| scope.to_string())
                    .collect::<Vec<_>>()
            )
        )
        .is_none());
    }

    #[test]
    fn r1_resource_projection_preserves_typed_nonfile_and_file_boundaries() {
        let workspace = std::env::current_dir().unwrap();
        let lease =
            tools::ToolHost::builtin("typed-resource-projection", &workspace).pin_snapshot();
        let metadata = lease.describe_effect("tool_search", &serde_json::json!({"query":"read"}));
        assert_eq!(metadata.effect_kind, ToolEffectKind::Read);
        assert!(delegated_tool_effect_is_bounded(&metadata));
        projection(&metadata, &[]);
        assert!(permits(&metadata, &[]));
        assert!(permits(&metadata, &["read:src/a"]));

        for resource in [PermissionResource::Tool, PermissionResource::Session] {
            for target in [Some("tool_search"), Some("secret.txt"), None, Some("")] {
                let mut descriptor = metadata.clone();
                descriptor.scopes =
                    vec![scope(resource.clone(), PermissionOperation::Read, target)];
                projection(&descriptor, &[]);
                for allowed in [vec![], vec!["read:src/a".to_string()]] {
                    assert!(effect_has_resource_lease(
                        &descriptor,
                        &allowed,
                        |_, _, _| {
                            panic!("non-File metadata must not request a file authorization")
                        }
                    ));
                }
            }
        }

        let mut mixed = metadata.clone();
        mixed.scopes = vec![
            scope(
                PermissionResource::Tool,
                PermissionOperation::Read,
                Some("tool_search"),
            ),
            scope(
                PermissionResource::Session,
                PermissionOperation::Read,
                Some("secret.txt"),
            ),
            scope(
                PermissionResource::File,
                PermissionOperation::Read,
                Some("src/a"),
            ),
            scope(
                PermissionResource::File,
                PermissionOperation::Write,
                Some("out/b"),
            ),
            scope(
                PermissionResource::File,
                PermissionOperation::Read,
                Some("src/a"),
            ),
        ];
        projection(&mixed, &[("out/b", true), ("src/a", false)]);
        assert!(permits(&mixed, &["read:src/a", "write:out/b"]));
        for allowed in [
            vec![],
            vec!["read:src/a"],
            vec!["write:out/b"],
            vec!["read:src/a", "read:out/b"],
            vec!["read:src/ab", "write:out/b"],
            vec!["read:src/a", "write:out/bb"],
        ] {
            assert!(!permits(&mixed, &allowed));
        }

        for target in [None, Some(""), Some(" \t ")] {
            for (operation, write) in [
                (PermissionOperation::Read, false),
                (PermissionOperation::Write, true),
            ] {
                let mut descriptor = metadata.clone();
                descriptor.scopes = vec![scope(PermissionResource::File, operation, target)];
                projection(&descriptor, &[(".", write)]);
                for allowed in [
                    vec![],
                    vec!["read:src/a"],
                    vec!["write:out/b"],
                    vec!["network:*"],
                ] {
                    assert!(!permits(&descriptor, &allowed));
                }
                assert_eq!(permits(&descriptor, &["read:."]), !write);
                for whole in ["write:.", "workspace:.", "write:./", "workspace:./"] {
                    assert!(permits(&descriptor, &[whole]));
                }
            }
        }
        for operation in [
            PermissionOperation::Delete,
            PermissionOperation::Execute,
            PermissionOperation::Send,
            PermissionOperation::Control,
            PermissionOperation::Call,
        ] {
            for target in [None, Some("src/a")] {
                let mut descriptor = metadata.clone();
                descriptor.scopes =
                    vec![scope(PermissionResource::File, operation.clone(), target)];
                projection(&descriptor, &[(target.unwrap_or("."), true)]);
                assert!(!permits(&descriptor, &["workspace:.", "network:*"]));
            }
        }

        let mut network = metadata.clone();
        network.uses_network = true;
        projection(&network, &[]);
        assert!(!permits(&network, &[]));
        assert!(!permits(&network, &["read:."]));
        assert!(permits(&network, &["network:*"]));
        network.scopes.push(scope(
            PermissionResource::File,
            PermissionOperation::Read,
            Some("src/a"),
        ));
        projection(&network, &[("src/a", false)]);
        for file_grant in [false, true] {
            for network_grant in [false, true] {
                let mut allowed = Vec::new();
                if file_grant {
                    allowed.push("read:src/a");
                }
                if network_grant {
                    allowed.push("network:*");
                }
                assert_eq!(permits(&network, &allowed), file_grant && network_grant);
            }
        }

        let mut process = lease.describe_effect(
            "execute_code",
            &serde_json::json!({
                "language":"python", "code":"print(1)"
            }),
        );
        assert!(process.spawns_process);
        assert!(delegated_tool_effect_is_bounded(&process));
        process
            .scopes
            .retain(|scope| scope.resource != PermissionResource::File);
        projection(&process, &[(".", false)]);
        for allowed in [vec![], vec!["read:src/a"], vec!["network:*"]] {
            assert!(!permits(&process, &allowed));
        }
        assert!(permits(&process, &["read:."]));
        process.scopes.push(scope(
            PermissionResource::File,
            PermissionOperation::Write,
            Some("out/b"),
        ));
        // A declared bounded scope keeps the spawned process inside that
        // bound, so the process adds no workspace access of its own: the
        // lease needs exactly the declared write and nothing wider.
        projection(&process, &[("out/b", true)]);
        assert!(permits(&process, &["write:out/b"]));
        assert!(permits(&process, &["read:.", "write:out/b"]));
        assert!(!permits(&process, &["read:."]));
        assert!(!permits(&process, &["read:out/b"]));
        assert!(!permits(&process, &["read:.", "read:out/b"]));
        let mut unbounded = process.clone();
        unbounded.required_permission = ToolPermissionMode::DangerFullAccess;
        assert!(!permits(&unbounded, &["workspace:.", "network:*"]));
        unbounded = process.clone();
        unbounded.uses_network = true;
        assert!(!permits(&unbounded, &["workspace:.", "network:*"]));

        let unknown = lease.describe_effect(
            "unregistered_resource_projection_tool",
            &serde_json::json!({}),
        );
        assert_eq!(unknown.effect_kind, ToolEffectKind::Unknown);
        assert!(lease
            .catalog_receipt()
            .descriptors
            .iter()
            .all(|descriptor| descriptor.canonical_id != "unregistered_resource_projection_tool"));
        projection(&unknown, &[(".", false)]);
        assert!(!permits(&unknown, &["workspace:.", "network:*"]));
        for scopes in [
            vec![],
            vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Read,
                Some("secret.txt"),
            )],
            vec![scope(
                PermissionResource::File,
                PermissionOperation::Read,
                Some("src/a"),
            )],
        ] {
            let mut descriptor = unknown.clone();
            descriptor.spawns_process = false;
            descriptor.uses_network = false;
            descriptor.scopes = scopes;
            let expected = if descriptor
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::File)
            {
                vec![("src/a", false)]
            } else {
                vec![]
            };
            projection(&descriptor, &expected);
            assert!(!permits(&descriptor, &[]));
            assert!(!permits(&descriptor, &["workspace:.", "network:*"]));
        }
        for kind in [
            ToolEffectKind::Package,
            ToolEffectKind::System,
            ToolEffectKind::Destructive,
        ] {
            let mut descriptor = metadata.clone();
            descriptor.scopes.clear();
            descriptor.effect_kind = kind;
            projection(&descriptor, &[]);
            assert!(!permits(&descriptor, &["workspace:.", "network:*"]));
        }
        for (packages, system) in [(true, false), (false, true), (true, true)] {
            let mut descriptor = metadata.clone();
            descriptor.scopes.clear();
            descriptor.mutates_packages = packages;
            descriptor.mutates_system = system;
            projection(&descriptor, &[]);
            assert!(!permits(&descriptor, &["workspace:.", "network:*"]));
        }

        let mut plugin = metadata.clone();
        plugin.tool_id = "plugin.resource_projection".to_string();
        plugin.scopes.clear();
        projection(&plugin, &[]);
        assert!(permits(&plugin, &[]));
        plugin.scopes.push(scope(
            PermissionResource::Tool,
            PermissionOperation::Read,
            Some("secret.txt"),
        ));
        projection(&plugin, &[]);
        assert!(permits(&plugin, &[]));
        plugin.scopes.push(scope(
            PermissionResource::File,
            PermissionOperation::Write,
            Some("out/b"),
        ));
        projection(&plugin, &[("out/b", true)]);
        assert!(!permits(&plugin, &[]));
        assert!(!permits(&plugin, &["read:out/b"]));
        assert!(!permits(&plugin, &["write:out/bb"]));
        assert!(permits(&plugin, &["write:out/b"]));
        assert!(!permits(&mixed, &[]));
        assert!(!permits(&process, &[]));
        assert!(!permits(&network, &[]));
    }
}

#[cfg(test)]
mod effect_file_access_tests {
    use super::*;
    use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
    use harness_contract::tool::ToolEffectDescriptor;

    fn descriptor(scopes: Vec<PermissionScope>, spawns_process: bool) -> ToolEffectDescriptor {
        ToolEffectDescriptor {
            tool_id: "tool".into(),
            descriptor_hash: "hash".into(),
            effect_kind: harness_contract::tool::ToolEffectKind::Process,
            idempotency: harness_contract::tool::ToolIdempotency::Unknown,
            scopes,
            required_permission: harness_contract::policy::PermissionMode::ReadOnly,
            approval_class: harness_contract::tool::ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        }
    }

    fn scope(resource: PermissionResource, operation: PermissionOperation, target: Option<&str>) -> PermissionScope {
        PermissionScope {
            resource,
            operation,
            target: target.map(str::to_owned),
        }
    }

    /// An unbounded host command still widens to the workspace root: the
    /// process could touch anything, so only a whole-workspace lease certifies
    /// it. This is the boundary the bounded-sandbox change must never weaken.
    #[test]
    fn an_unbounded_process_still_widens_to_the_workspace_root() {
        let unbounded = descriptor(
            vec![scope(
                PermissionResource::Shell,
                PermissionOperation::Execute,
                None,
            )],
            true,
        );
        assert_eq!(effect_file_accesses(&unbounded), vec![(".".to_string(), false)]);
    }

    /// A descriptor that declares an explicitly bounded scope set does not gain
    /// workspace-root access just because it spawns a process.
    #[test]
    fn a_bounded_process_does_not_widen_to_the_workspace_root() {
        let bounded = descriptor(
            vec![scope(
                PermissionResource::File,
                PermissionOperation::Read,
                Some("crates/runtime/src/lib.rs"),
            )],
            true,
        );
        assert_eq!(
            effect_file_accesses(&bounded),
            vec![("crates/runtime/src/lib.rs".to_string(), false)]
        );
    }

    /// A bounded root declaration (an explicit empty input set) keeps exactly
    /// the root access it declared - the process adds nothing on top.
    #[test]
    fn a_bounded_root_declaration_adds_no_extra_access() {
        let root = descriptor(
            vec![scope(PermissionResource::File, PermissionOperation::Read, Some("."))],
            true,
        );
        assert_eq!(effect_file_accesses(&root), vec![(".".to_string(), false)]);
    }
}
