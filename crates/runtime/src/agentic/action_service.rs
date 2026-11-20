use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActionErrorObservation, AgentActionObservation,
    AgentActionStatus, AgentActorKind,
};
use harness_contract::goal::ObjectiveReviewVerification;
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreError, RuntimeTransactionEventInput,
};

use super::program::{AgenticProgramProjection, AgenticTaskStatus, AgenticTopicEntryProjection};

mod authorization;
mod delegation;

#[cfg(test)]
mod native_effect_history_tests;

#[cfg(test)]
use authorization::validate_transition;
use authorization::validate_transition_with_admission_contexts;

/// Original action transaction context, held only within the authenticated
/// owner’s policy and stream-lock closure. This context grants no authority.
/// Single-call and batch producers must independently authenticate their sources.
struct OriginalActionCommit<'a> {
    sources: &'a [ExpectedStreamRevision],
    locks: &'a crate::runtime_event_store::HeldStreamLocks<'a>,
    control: Arc<crate::conversation::MemberCommitControl>,
    claim: &'a crate::execution_core::services::OriginalSessionClaim,
    consumed_input_sequence: usize,
    single_call_facts: Option<serde_json::Value>,
    member_call_facts: Option<serde_json::Value>,
    authority: Option<Arc<crate::authorization_negotiator::PreparedMemberAuthority>>,
    reservation: Option<&'a crate::runtime_event_store::OriginalMemberReservation>,
    effect_fence: Option<&'a crate::runtime_event_store::OriginalMemberEffectFence>,
    committed_capture: Option<&'a std::cell::RefCell<Option<CommittedActionAppend>>>,
}

/// A call that was admitted and then refused while it ran.
///
/// The call's own effect key is the identity the refusal is written under:
/// the consumer looks the fact up by the admitted call's own key, so a
/// refusal written under any other key would never be read.
pub struct RefusedCall<'a> {
    /// The refused call's own effect key.
    pub effect_key: &'a str,
    /// The refused call's own tool use identity.
    pub tool_use_id: &'a str,
    pub tool_name: &'a str,
    /// Why the call was refused, in the caller's own words.
    pub reason: &'a str,
}

/// What became of a refused call's durable fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalOutcome {
    /// The refusal is now a durable fact.
    Recorded,
    /// The call carried no admission intent, so no fact was written: there
    /// is no half-written rejection for a consumer to read as one.
    NotAdmitted,
}

/// The effect-stream twin of one durable rejection fact.
///
/// The twin is written on the rejected call's own effect stream, so a
/// consumer finds the fact by the call's own key. The member shape and the
/// refused-call shape differ only here: the key the twin is written under,
/// and the member coordinates it carries.
struct RejectionTwin {
    /// The call's own effect key.
    outer_key: String,
    /// The twin's own idempotency key inside that stream.
    idempotency_key: String,
    /// The member ordinal this twin stands for, when the fact is a member's.
    member_index: Option<usize>,
    member_identity: Option<String>,
    ticket: serde_json::Value,
}

/// The identity one durable rejection fact is written under.
///
/// The program's rejection event and its effect-stream twin are one fact:
/// the program event names the rejected call, and the twin names the program
/// event it belongs to, so the pair is authenticated as the one transaction
/// that wrote both.
struct RejectionIdentity<'a> {
    program_id: &'a str,
    session_id: &'a str,
    program_revision: u64,
    /// The program event's actor, and the objective it references.
    actor_id: &'a str,
    objective_id: &'a str,
    /// The idempotency basis of the program event: the rejected call's id.
    action_id: &'a str,
    code: &'a str,
    gap: &'a str,
    /// The program event's own payload.
    payload: serde_json::Value,
    /// What a re-entry read-back compares, field by field.
    replay: RejectionReplay<'a>,
    /// The effect-stream twin, when this fact names the call's own effect.
    twin: Option<RejectionTwin>,
}

/// What a re-entry read-back compares, field by field.
enum RejectionReplay<'a> {
    /// An Agent action's rejection: its envelope, and the fields beside it.
    AgentAction { envelope: &'a AgentActionEnvelope },
    /// A refused call's own coordinates.
    RefusedCall { effect_key: &'a str },
}

/// Created only from this store owner's successful checked append. A public
/// deserialized receipt cannot construct this capability.
/// Original checked commits and verified unknown-outcome reads share the same
/// durable commit receipt. Validation detail exists only for a direct return.
#[derive(Clone)]
pub(crate) struct OriginalMemberCommitReceipt {
    pub commit: crate::AppendTransactionReceipt,
    pub validation: Option<crate::CheckedSourceEvidence>,
}
struct CommittedActionAppend {
    store: std::sync::Weak<RuntimeEventStore>,
    request: Arc<crate::CheckedAppendRequest>,
    receipt: OriginalMemberCommitReceipt,
    authority: Option<Arc<crate::authorization_negotiator::PreparedMemberAuthority>>,
}

/// Result returned by the original single-action checked append.  It records
/// the same call's action transaction, not physical delivery or completion.
pub(crate) struct OriginalCommittedSingle {
    store: std::sync::Weak<RuntimeEventStore>,
    request: Arc<crate::CheckedAppendRequest>,
    receipt: OriginalMemberCommitReceipt,
    observation: Option<Arc<AgentActionObservation>>,
    observation_error: Option<String>,
}
pub(crate) struct OriginalSingleSubmission {
    pub(crate) observation: Result<AgentActionObservation, AgentActionServiceError>,
    pub(crate) committed: Option<Arc<OriginalCommittedSingle>>,
}
impl OriginalCommittedSingle {
    pub(crate) fn belongs_to(&self, store: &Arc<RuntimeEventStore>) -> bool {
        self.store.upgrade().is_some_and(|owner| Arc::ptr_eq(&owner, store))
    }
    pub(crate) fn request(&self) -> &crate::CheckedAppendRequest { &self.request }
    pub(crate) fn receipt(&self) -> &OriginalMemberCommitReceipt { &self.receipt }
    pub(crate) fn observation(&self) -> Option<&Arc<AgentActionObservation>> { self.observation.as_ref() }
    pub(crate) fn observation_error(&self) -> Option<&str> { self.observation_error.as_deref() }
}

/// Facts read once by the original replay owner; never a reconstructed
/// checked request and never proof that physical delivery is complete.
#[derive(Clone)]
pub(crate) struct OriginalReplayedAction {
    store: std::sync::Weak<RuntimeEventStore>,
    ticket: crate::execution_core::graph::NodeExecutionTicket,
    call_id: String,
    observation: Option<AgentActionObservation>,
    program_event: crate::DurableRuntimeEvent,
    goal_event: Option<crate::DurableRuntimeEvent>,
    receipt: crate::AppendTransactionReceipt,
}
impl OriginalReplayedAction {
    pub(crate) fn belongs_to(&self, store: &Arc<RuntimeEventStore>) -> bool { self.store.upgrade().is_some_and(|original| Arc::ptr_eq(&original, store)) }
    pub(crate) fn ticket(&self) -> &crate::execution_core::graph::NodeExecutionTicket { &self.ticket }
    pub(crate) fn call_id(&self) -> &str { &self.call_id }
    pub(crate) fn observation(&self) -> Option<&AgentActionObservation> { self.observation.as_ref() }
    pub(crate) fn program_event(&self) -> &crate::DurableRuntimeEvent { &self.program_event }
    pub(crate) fn goal_event(&self) -> Option<&crate::DurableRuntimeEvent> { self.goal_event.as_ref() }
    pub(crate) fn receipt(&self) -> &crate::AppendTransactionReceipt { &self.receipt }
}

/// A same-call conclusion transaction returned by its original checked owner.
/// The weak store identity prevents sink -> result -> store -> retained call cycles.
pub(crate) struct OriginalCommittedConclusion {
    store: std::sync::Weak<RuntimeEventStore>,
    request: Arc<crate::CheckedAppendRequest>,
    receipt: OriginalMemberCommitReceipt,
    action_id: String,
    member_index: Option<usize>,
}
impl OriginalCommittedConclusion {
    pub(crate) fn belongs_to(&self, store: &Arc<RuntimeEventStore>) -> bool {
        self.store.upgrade().is_some_and(|owner| Arc::ptr_eq(&owner, store))
    }
    pub(crate) fn request(&self) -> &crate::CheckedAppendRequest { &self.request }
    pub(crate) fn receipt(&self) -> &OriginalMemberCommitReceipt { &self.receipt }
    pub(crate) fn action_id(&self) -> &str { &self.action_id }
    pub(crate) fn member_index(&self) -> Option<usize> { self.member_index }
    pub(crate) fn same_original_commit(&self, other: &Self) -> bool {
        self.store.upgrade().zip(other.store.upgrade()).is_some_and(|(left, right)| Arc::ptr_eq(&left, &right))
            && Arc::ptr_eq(&self.request, &other.request)
            && self.receipt.commit == other.receipt.commit
            && self.action_id == other.action_id && self.member_index == other.member_index
    }
}

/// Same original owner result; it does not attest physical Host delivery.
pub(crate) struct ProcessOriginalCommittedReplay {
    pub(crate) committed: Arc<OriginalCommittedMember>,
    pub(crate) member: crate::tool_host::AuthenticatedBatchMember,
    pub(crate) owner_applied: Result<(), String>,
}

/// Resolve the same retained Process transaction without replaying policy,
/// reloading a current projection, or turning missing custody into Fresh.
pub(crate) fn process_original_member_replay(
    process: Arc<crate::agent_in_process_worker::OriginalProcessBatchCall>,
    actions: &AgentActionService, original: &crate::RuntimeToolExecutionRequest, index: usize,
    expected: Option<&Arc<OriginalCommittedMember>>,
) -> Result<Option<ProcessOriginalCommittedReplay>, crate::tool_host::BatchMemberUnknownReason> {
    use crate::tool_host::BatchMemberUnknownReason;
    let unavailable = || BatchMemberUnknownReason::OriginalTransactionFactsUnavailable;
    if !Arc::ptr_eq(process.store(), &actions.store) { return Err(unavailable()); }
    let Some((transaction_id, member_guard)) = actions.store.retained_original_process_member_transaction(&process, original, index)
        .map_err(|_| unavailable())? else { return Ok(None); };
    let committed = actions.recover_retained_original_member(&transaction_id)
        .map_err(|_| unavailable())?.ok_or_else(unavailable)?;
    // A stopped winner must be this exact retained checked identity, before
    // either hot authority application or release can run.
    if expected.is_some_and(|winner| !winner.same_original_commit(&committed)) { return Err(unavailable()); }
    let authority = committed.authority().cloned().ok_or_else(unavailable)?;
    if !authority.member().matches(&member_guard) { return Err(unavailable()); }
    let approval_guard = authority.approval_guard(&member_guard).map_err(|_| unavailable())?;
    let member_owner = member_guard.process().ok_or_else(unavailable)?;
    if !Arc::ptr_eq(member_owner.owner(), &process) || !authority.matches_committed(&member_guard, &committed) {
        return Err(unavailable());
    }
    let transaction = &committed.request().transaction;
    let mut markers = transaction.events.iter().filter(|entry| entry.event.kind == "agentic.batch_member.index");
    let marker = markers.next().ok_or_else(unavailable)?;
    if markers.next().is_some() { return Err(unavailable()); }
    let action_index = marker.event.payload["program_event_index"].as_u64()
        .and_then(|value| usize::try_from(value).ok()).ok_or_else(unavailable)?;
    let action = transaction.events.get(action_index).ok_or_else(unavailable)?;
    let envelope: AgentActionEnvelope = serde_json::from_value(action.event.payload.get("envelope").cloned().ok_or_else(unavailable)?)
        .map_err(|_| unavailable())?;
    let member = crate::tool_host::AuthenticatedBatchMember::from_original_committed(Arc::clone(&committed), process.store(), &envelope)
        .map_err(|_| unavailable())?;
    let owner_applied = process.negotiator().apply_recovered_member_authority(&authority, &member_guard, approval_guard.as_deref(), &committed)
        .and_then(|()| actions.release_original_member_after_owner_apply(&committed).map_err(|error| error.to_string()));
    let (committed, member) = if owner_applied.is_ok() {
        let completed = committed.after_original_authority_release();
        let member = crate::tool_host::AuthenticatedBatchMember::from_original_committed(Arc::clone(&completed), process.store(), &envelope)
            .map_err(|_| unavailable())?;
        (completed, member)
    } else { (committed, member) };
    Ok(Some(ProcessOriginalCommittedReplay { committed, member, owner_applied }))
}

/// Same-owner result of a member commit. Missing observation remains a
/// partial committed result, never permission to repeat the action.
pub struct OriginalCommittedMember {
    append: CommittedActionAppend,
    observation: Option<Arc<AgentActionObservation>>,
    observation_error: Option<String>,
}
impl OriginalCommittedMember {
    pub(crate) fn recover_original(store: &Arc<RuntimeEventStore>, transaction_id: &str)
        -> crate::RuntimeEventStoreResult<Option<Arc<Self>>> {
        let Some(original) = store.recover_retained_member(transaction_id)? else { return Ok(None); };
        Ok(Some(Arc::new(OriginalCommittedMember {
            append: CommittedActionAppend {
                store: Arc::downgrade(store), request: original.request,
                receipt: OriginalMemberCommitReceipt { commit: original.receipt, validation: None },
                authority: Some(original.authority),
            },
            observation: None,
            observation_error: Some("original committed observation is not available after recovery".into()),
        })))
    }

    pub(crate) fn same_original_commit(&self, other: &Self) -> bool {
        std::sync::Weak::ptr_eq(&self.append.store, &other.append.store)
            && Arc::ptr_eq(&self.append.request, &other.append.request)
            && self.append.receipt.commit == other.append.receipt.commit
    }

    pub fn belongs_to(&self, store: &Arc<RuntimeEventStore>) -> bool {
        self.append.store.upgrade().is_some_and(|owner| Arc::ptr_eq(&owner, store))
    }
    /// Only the original authority owner may call this after its apply and release succeed.
    pub(crate) fn after_original_authority_release(&self) -> Arc<Self> {
        Arc::new(Self {
            append: CommittedActionAppend {
                store: self.append.store.clone(), request: Arc::clone(&self.append.request),
                receipt: self.append.receipt.clone(), authority: None,
            },
            observation: self.observation.clone(), observation_error: self.observation_error.clone(),
        })
    }
    pub fn request(&self) -> &crate::CheckedAppendRequest { &self.append.request }
    pub(crate) fn receipt(&self) -> &OriginalMemberCommitReceipt { &self.append.receipt }
    pub fn observation(&self) -> Option<&Arc<AgentActionObservation>> { self.observation.as_ref() }
    pub fn observation_error(&self) -> Option<&str> { self.observation_error.as_deref() }
    pub(crate) fn authority(&self) -> Option<&Arc<crate::authorization_negotiator::PreparedMemberAuthority>> { self.append.authority.as_ref() }
}

pub enum OriginalMemberSubmission {
    Committed(Arc<OriginalCommittedMember>),
    /// This invocation produced no original committed receipt. Its status is
    /// not evidence that any earlier attempt rolled back.
    NoCommittedReceipt(AgentActionObservation),
}

struct StrictOriginalActionSources { digest: String, authority_deadlines: Option<(u64, Option<u64>)> }
impl crate::CheckedSourceValidator for StrictOriginalActionSources {
    fn frame_digest(&self) -> &str { &self.digest }
    fn validate_tail(&self, source: &ExpectedStreamRevision, observed_revision: u64,
        events: &[crate::DurableRuntimeEvent]) -> crate::RuntimeEventStoreResult<()> {
        if let Some((lease, approval)) = self.authority_deadlines {
            let now: u64 = SystemTime::now().duration_since(UNIX_EPOCH)
                .map_err(|_| RuntimeEventStoreError::CurrentMemberAuthorityExpired)?
                .as_millis().try_into().map_err(|_| RuntimeEventStoreError::CurrentMemberAuthorityExpired)?;
            if now > lease || approval.is_some_and(|deadline| now >= deadline) {
                return Err(RuntimeEventStoreError::CurrentMemberAuthorityExpired);
            }
        }
        if observed_revision == source.expected_revision && events.is_empty() { return Ok(()); }
        Err(RuntimeEventStoreError::StaleRevision {
            stream_id: source.stream_id.clone(), expected: source.expected_revision, actual: observed_revision,
        })
    }
}

/// Original trigger identity for a non-request terminal; a generic blocked
/// verdict is not enough to identify which trigger won.
pub(crate) enum NonRequestConclusionTrigger<'a> {
    Runtime(&'a harness_contract::goal::RuntimeObservation),
    Rejection(&'a str),
}

/// Immutable claim identities at the original conclusion cut. Only an exact
/// renewal of one of these claims is neutral to that conclusion.
struct ProgramConclusionSources {
    digest: String,
    program_id: String,
    claims: BTreeMap<String, (String, String)>,
}

impl crate::CheckedSourceValidator for ProgramConclusionSources {
    fn frame_digest(&self) -> &str {
        &self.digest
    }

    fn validate_tail(
        &self,
        source: &ExpectedStreamRevision,
        observed_revision: u64,
        events: &[crate::DurableRuntimeEvent],
    ) -> crate::RuntimeEventStoreResult<()> {
        let reject = || RuntimeEventStoreError::StaleRevision {
            stream_id: source.stream_id.clone(),
            expected: source.expected_revision,
            actual: observed_revision,
        };
        if observed_revision == source.expected_revision && events.is_empty() { return Ok(()); }
        if source.stream_id != program_stream(&self.program_id)
            || observed_revision.checked_sub(source.expected_revision) != Some(events.len() as u64)
        {
            return Err(reject());
        }
        for (offset, event) in events.iter().enumerate() {
            let task = event.payload["task_ref"].as_str().ok_or_else(reject)?;
            let (agent, execution) = self.claims.get(task).ok_or_else(reject)?;
            let generation = source.expected_revision + offset as u64;
            if event.stream_id != source.stream_id
                || event.sequence != generation + 1
                || event.scope != RuntimeEventScope::Program
                || event.kind != TASK_LEASE_RENEWED_EVENT_KIND
                || event.schema_version != 1
                || event.status.as_deref() != Some("renewed")
                || !event.refs.is_empty()
                || event.actor.as_deref() != Some(agent.as_str())
                || event.payload["agent_id"].as_str() != Some(agent.as_str())
                || event.payload["execution_id"].as_str() != Some(execution.as_str())
                || event.idempotency_key.as_deref()
                    != Some(format!("claim-lease-renewed:{execution}:{generation}").as_str())
                || event.transaction_id
                    != format!(
                        "claim-lease-renewed:{}:{execution}:{generation}",
                        self.program_id
                    )
                || event.transaction_index != 0
            {
                return Err(reject());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod conclusion_source_tests {
    use super::*;
    use crate::CheckedSourceValidator;

    #[test]
    fn n2_conclusion_neutrality_requires_exact_original_claim_and_writer() {
        let frame = ProgramConclusionSources {
            digest: "frame".into(),
            program_id: "p".into(),
            claims: BTreeMap::from([("task".into(), ("agent".into(), "execution".into()))]),
        };
        let source = ExpectedStreamRevision {
            stream_id: "agentic-program:p".into(),
            expected_revision: 39,
        };
        let event = crate::DurableRuntimeEvent {
            event_id: "event".into(),
            stream_id: "agentic-program:p".into(),
            sequence: 40,
            scope: RuntimeEventScope::Program,
            kind: "agentic.task_lease_renewed".into(),
            status: Some("renewed".into()),
            actor: Some("agent".into()),
            refs: vec![],
            payload: json!({"task_ref":"task", "agent_id":"agent", "execution_id":"execution"}),
            created_at_ms: 42,
            commit_cursor: 100,
            transaction_id: "claim-lease-renewed:p:execution:39".into(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: Some("claim-lease-renewed:execution:39".into()),
        };
        frame
            .validate_tail(&source, 40, std::slice::from_ref(&event))
            .unwrap();
        let mut bad = event.clone();
        bad.actor = Some("foreign".into());
        assert!(frame.validate_tail(&source, 40, &[bad]).is_err());
        let mut bad = event.clone();
        bad.payload["execution_id"] = json!("old-attempt");
        assert!(frame.validate_tail(&source, 40, &[bad]).is_err());
        let mut bad = event.clone();
        bad.payload["task_ref"] = json!("unclaimed");
        assert!(frame.validate_tail(&source, 40, &[bad]).is_err());
        let mut bad = event.clone();
        bad.kind = "agentic.action".into();
        assert!(frame.validate_tail(&source, 40, &[bad]).is_err());
        let mut bad = event.clone();
        bad.idempotency_key = Some("claim-lease-renewed:execution:38".into());
        assert!(frame.validate_tail(&source, 40, &[bad]).is_err());
        assert!(frame.validate_tail(&source, 41, &[event]).is_err());
        let foreign = ExpectedStreamRevision {
            stream_id: "reviewer-graph".into(),
            expected_revision: 4,
        };
        assert!(frame.validate_tail(&foreign, 5, &[]).is_err());
    }
}

pub(crate) fn validate_receipt_index_record(
    event: &crate::DurableRuntimeEvent,
    source: &str,
    sequence: u64,
    revision: u64,
) -> Result<(), String> {
    if sequence == 0
        || event.stream_id != source
        || event.sequence != sequence
        || event.sequence > revision
        || (event.kind == "execution.agent_tool.intent" && event.schema_version != 1)
        || (event.kind == "execution.agent_tool.receipt"
            && crate::ExecutionCommitService::receipt_observation_sequence(event).is_none())
        || event.scope != RuntimeEventScope::ExecutionNode
        || event.actor.as_deref() != Some("governed_tool")
    {
        return Err("receipt-index structural identity mismatch".into());
    }
    let identity = event.payload["idempotency_key"]
        .as_str()
        .filter(|identity| !identity.is_empty())
        .ok_or("receipt-index structural payload identity missing")?;
    let prefix = match event.kind.as_str() {
        "execution.agent_tool.intent" => "agent-tool-intent",
        "execution.agent_tool.receipt" => "agent-tool-receipt",
        _ => return Err("receipt-index structural kind unsupported".into()),
    };
    if event.idempotency_key.as_deref() != Some(format!("{prefix}:{identity}").as_str()) {
        return Err("receipt-index structural idempotency identity mismatch".into());
    }
    Ok(())
}

#[cfg(test)]
mod receipt_index_791_tests;

fn retirement_execute_claim_matches(
    task: &super::program::AgenticTaskProjection,
    attempt: &super::program::AgenticTaskAttemptProjection,
) -> bool {
    task.claim_generation > attempt.generation
        && match (task.claim_execution_id.as_deref(), task.claimant.as_deref()) {
            (None, None) => true,
            (Some(execution), Some(agent)) => {
                execution == attempt.execution_id && agent == attempt.agent_id
            }
            _ => false,
        }
}

const ACTION_EVENT_KIND: &str = "agentic.action_applied";
/// Durable record of what the root barrier's resolver decided.
///
/// The resolver used to report only a count, so a barrier that never released could
/// not be distinguished from one that was never asked. In paired run 3 all three
/// Tasks were `accepted`, the barrier was `WaitingExternal`, its payload matched the
/// projection's program_id and root_execution_id word for word, and the resolver's own
/// cursor was level with the event stream head - yet `resolved_waits` was 0 and no
/// `agentic-program-checkpoint` command was ever issued. This record names the
/// disposition, the reason, the Task statuses the resolver actually read, and whether
/// it found a candidate barrier node, so the next run answers the question instead of
/// leaving it to inference.
pub(crate) struct AgenticWaitDisposition<'a> {
    pub program_id: &'a str,
    pub root_execution_id: &'a str,
    pub revision: u64,
    pub disposition: &'static str,
    pub reason: Option<&'static str>,
    pub all_tasks_accepted: bool,
    pub task_statuses: Vec<(String, String)>,
    pub barrier_candidates: usize,
    pub released: usize,
    pub observed_revision: u64,
    /// Every barrier node the resolver saw and why each was rejected. A zero
    /// `barrier_candidates` is otherwise unreadable: it cannot distinguish "the node
    /// was never created" from "the node is there but one of the four admission
    /// conditions was false".
    pub barrier_rejections: serde_json::Value,
}

#[cfg(any(test, feature = "test-fixtures"))]
thread_local! {
    static CONCLUSION_APPEND_HOOK: std::cell::RefCell<Option<(Option<String>, Box<dyn FnOnce(&AppendTransactionRequest)>)>> =
        std::cell::RefCell::new(None);
    #[cfg(feature = "test-fixtures")]
    static CONCLUSION_ATTEMPT_OBSERVER: std::cell::RefCell<Option<(String, std::rc::Rc<std::cell::RefCell<Option<crate::execution_core::services::ConclusionAttemptObservation>>>)>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
pub(crate) fn with_conclusion_append_hook<Output>(
    hook: impl FnOnce(&AppendTransactionRequest) + 'static,
    commit: impl FnOnce() -> Output,
) -> Output {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            CONCLUSION_APPEND_HOOK.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    CONCLUSION_APPEND_HOOK.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some((None, Box::new(hook)));
    });
    let _reset = Reset;
    commit()
}

#[cfg(any(test, feature = "test-fixtures"))]
fn run_conclusion_append_hook(transaction: &AppendTransactionRequest) {
    let hook = CONCLUSION_APPEND_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|(target, _)| target.as_ref().is_none_or(|id| id == &transaction.transaction_id)) {
            slot.take().map(|(_, hook)| hook)
        } else {
            None
        }
    });
    if let Some(hook) = hook { hook(transaction); }
}

impl AgentActionService {
    /// Installs a same-thread, single-transaction test hook. The caller must
    /// synchronously drive the original guarded owner while this closure runs.
    #[cfg(feature = "test-fixtures")]
    pub(crate) fn with_targeted_conclusion_append_hook_for_test<Output>(
        transaction_id: String,
        hook: impl FnOnce(&AppendTransactionRequest) + 'static,
        commit: impl FnOnce() -> Output,
    ) -> (Output, bool, Option<crate::execution_core::services::ConclusionAttemptObservation>) {
        // Check both slots before installing either, so a nested invocation
        // cannot clear an outer owner's observer on panic.
        CONCLUSION_APPEND_HOOK.with(|slot| assert!(slot.borrow().is_none()));
        CONCLUSION_ATTEMPT_OBSERVER.with(|slot| assert!(slot.borrow().is_none()));
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                CONCLUSION_APPEND_HOOK.with(|slot| { slot.borrow_mut().take(); });
                CONCLUSION_ATTEMPT_OBSERVER.with(|slot| { slot.borrow_mut().take(); });
            }
        }
        let _reset = Reset;
        let observed = std::rc::Rc::new(std::cell::RefCell::new(None));
        CONCLUSION_ATTEMPT_OBSERVER.with(|slot| {
            *slot.borrow_mut() = Some((transaction_id.clone(), std::rc::Rc::clone(&observed)));
        });
        let reached = std::rc::Rc::new(std::cell::Cell::new(false));
        let reached_by_hook = std::rc::Rc::clone(&reached);
        CONCLUSION_APPEND_HOOK.with(|slot| {
            *slot.borrow_mut() = Some((Some(transaction_id), Box::new(move |transaction| {
                reached_by_hook.set(true);
                hook(transaction);
            })));
        });
        let result = commit();
        let observation = observed.borrow_mut().take();
        (result, reached.get(), observation)
    }

    #[cfg(test)]
    pub(crate) fn with_team_quantity_commit_hook_for_test<Output>(
        hook: impl FnOnce(&AppendTransactionRequest) + 'static,
        commit: impl FnOnce() -> Output,
    ) -> Output {
        with_conclusion_append_hook(hook, commit)
    }

    /// Append one barrier disposition record. It goes on its own stream so it never
    /// advances the semantic Program revision, and its scope is deliberately not
    /// `Program`: the resolver lane's interest is `agentic.action_applied`, and a
    /// diagnostic record must not be able to re-arm that lane.
    pub(crate) fn record_wait_disposition(&self, record: AgenticWaitDisposition<'_>) {
        if self
            .program_recovery_is_excluded(record.program_id)
            .unwrap_or(true)
        {
            return;
        }
        let stream_id = format!("agentic-program-wait:{}", record.program_id);
        // Fence the append on the stream head and retry the fence, not the write.
        // The synchronous reconcile in the tool path and the barrier's own bounded
        // self-rearm run concurrently and both record here, so a single-shot fence
        // silently drops whichever arrives second - and a dropped record is a
        // diagnosis built on an undercount. Nine fixes were derived from exactly
        // that undercount before this was noticed.
        let mut attempt = 0_u8;
        let payload = serde_json::json!({
            "program_id": record.program_id,
            "root_execution_id": record.root_execution_id,
            "revision": record.revision,
            "disposition": record.disposition,
            "reason": record.reason,
            "all_tasks_accepted": record.all_tasks_accepted,
            "task_statuses": record.task_statuses,
            "barrier_candidates": record.barrier_candidates,
            "released": record.released,
            "observed_revision": record.observed_revision,
        });
        let request = AppendTransactionRequest {
            transaction_id: format!(
                "agentic-wait-disposition:{}:{revision}:{disposition}",
                record.program_id,
                revision = record.revision,
                disposition = record.disposition
            ),
            expected_streams: Vec::new(),
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("agentic-program-wait:{}", record.program_id),
                    scope: RuntimeEventScope::Relation,
                    kind: "agentic.program_wait_disposition".to_string(),
                    status: Some(record.disposition.to_string()),
                    actor: Some(record.root_execution_id.to_string()),
                    refs: Vec::new(),
                    payload,
                },
                idempotency_key: None,
                schema_version: 1,
            }],
        };
        loop {
            let expected = self.store.stream_revision(&stream_id).unwrap_or_default();
            let request = AppendTransactionRequest {
                transaction_id: request.transaction_id.clone(),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: expected,
                }],
                events: request.events.clone(),
            };
            match self.store.append_transaction(request) {
                Ok(_) => return,
                Err(error) if attempt < 8 && error.to_string().contains("revision") => {
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(error) => {
                    tracing::debug!(
                        program_id = record.program_id,
                        %error,
                        "agentic barrier disposition record not appended"
                    );
                    return;
                }
            }
        }
    }

    /// Append one dispatch decline record.
    ///
    /// The dispatch machinery declines for nine different reasons and, until now,
    /// left no durable trace of any of them. A Task that sat `Submitted` with no
    /// reviewer was therefore indistinguishable from a Task that was never asked:
    /// both looked like "no review dispatch happened", and the only way to tell
    /// them apart was a log line that lives in a file the evaluation never
    /// surfaces, at a level a release build filters out. Two root causes were
    /// guessed wrong from exactly that silence.
    ///
    /// Same shape and same stream discipline as [`Self::record_wait_disposition`]:
    /// its own stream, `Relation` scope so it cannot re-arm the `Program` lane,
    /// and a fenced append that retries the fence rather than dropping the
    /// second writer.
    pub(crate) fn record_dispatch_decline(&self, record: AgenticDispatchDecline<'_>) {
        if self
            .program_recovery_is_excluded(record.program_id)
            .unwrap_or(true)
        {
            return;
        }
        let stream_id = format!("agentic-dispatch:{}", record.program_id);
        let payload = serde_json::json!({
            "program_id": record.program_id,
            "task_ref": record.task_ref,
            "mode": record.mode,
            "reason": record.reason.as_str(),
            "task_status": record.task_status,
            "eligible_members": record.eligible_members,
            "busy_members": record.busy_members,
            "consecutive_deferrals": record.consecutive_deferrals,
            "review_generation": record.review_generation,
        });
        // One record per distinct (task, mode, reason). `dispatch_ready_agentic_work`
        // runs on every worker exit, so a Program that cannot be dispatched would
        // otherwise emit the same decline forever and bury the one that mattered.
        let idempotency_key = format!(
            "{}:{}:{}:{}",
            record.task_ref,
            record.mode,
            record.reason.as_str(),
            record.task_status
        );
        let mut attempt = 0_u8;
        let request = AppendTransactionRequest {
            transaction_id: format!("agentic-dispatch-decline:{idempotency_key}"),
            expected_streams: Vec::new(),
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: RuntimeEventScope::Relation,
                    kind: DISPATCH_DECLINED_EVENT_KIND.to_string(),
                    status: Some("declined".to_string()),
                    actor: Some("runtime.agentic-dispatch".to_string()),
                    refs: Vec::new(),
                    payload,
                },
                // The deduplication the plan asks for: an unchanged reason is not
                // news, and re-appending it would advance the stream without
                // adding anything a reader could act on.
                idempotency_key: Some(idempotency_key),
                schema_version: 1,
            }],
        };
        loop {
            let expected = self.store.stream_revision(&stream_id).unwrap_or_default();
            let request = AppendTransactionRequest {
                transaction_id: request.transaction_id.clone(),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: expected,
                }],
                events: request.events.clone(),
            };
            match self.store.append_transaction(request) {
                Ok(_) => return,
                Err(error) if attempt < 8 && error.to_string().contains("revision") => {
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(error) => {
                    tracing::debug!(%error, "agentic dispatch decline record not appended");
                    return;
                }
            }
        }
    }
}

/// Why Runtime declined to dispatch one Task in one mode.
///
/// An enum rather than a formatted string: the reader is a query against
/// `runtime_events`, and matching on prose is how the previous two root causes
/// were guessed wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgenticDispatchDeclineReason {
    ProgramNotOpen,
    StarvationBackoff,
    RootGraphTerminal,
    CoordinationWakeAbsent,
    CoordinationIntentMismatch,
    CoordinationNotCurrent,
    CoordinationAlreadySettled,
    ModeStateMismatch,
    NoEligibleMember,
    NoFreeEligibleMember,
    FlightBusy,
}

impl AgenticDispatchDeclineReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProgramNotOpen => "program_not_open",
            Self::StarvationBackoff => "starvation_backoff",
            Self::RootGraphTerminal => "root_graph_terminal",
            Self::CoordinationWakeAbsent => "coordination_wake_absent",
            Self::CoordinationIntentMismatch => "coordination_intent_mismatch",
            Self::CoordinationNotCurrent => "coordination_not_current",
            Self::CoordinationAlreadySettled => "coordination_already_settled",
            Self::ModeStateMismatch => "mode_state_mismatch",
            Self::NoEligibleMember => "no_eligible_member",
            Self::NoFreeEligibleMember => "no_free_eligible_member",
            Self::FlightBusy => "flight_busy",
        }
    }
}

/// One "Runtime did not dispatch this" record.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AgenticDispatchDecline<'a> {
    pub program_id: &'a str,
    pub task_ref: &'a str,
    pub mode: &'static str,
    pub reason: AgenticDispatchDeclineReason,
    /// The Task's status at the moment of the decline. A `mode_state_mismatch` is
    /// unreadable without it: the reader needs to see which status was expected.
    pub task_status: &'a str,
    pub eligible_members: usize,
    pub busy_members: usize,
    pub consecutive_deferrals: usize,
    pub review_generation: u64,
}

/// Runtime declined to dispatch one Task in one mode. Durable for the same reason
/// `agentic.program_wait_disposition` is: a decline nobody can read is a decline
/// nobody can diagnose, and the dispatch path declines for nine reasons.
const DISPATCH_DECLINED_EVENT_KIND: &str = "agentic.dispatch.declined";
const PROGRAM_OPENED_EVENT_KIND: &str = "agentic.program_opened";
const OBJECTIVE_VERDICT_EVENT_KIND: &str = "agentic.objective_verdict_bound";
const COMPLETION_REOPENED_EVENT_KIND: &str = "agentic.completion_reopened";
/// A lightweight in-place claim-lease renewal. Unlike `agentic.action_applied`
/// (a full governed action), this only extends the existing claim's lease, so a
/// long-running Agent does not churn durable actions just to stay alive.
const TASK_LEASE_RENEWED_EVENT_KIND: &str = "agentic.task_lease_renewed";
/// A completion request rejected at *authorization* (before it is applied).
/// These never became a durable reopen, so a Program could loop requesting
/// completion forever without the bounded-convergence reopen count ever
/// seeing it. Recorded durably so that loop is still bounded.
const COMPLETION_REJECTED_EVENT_KIND: &str = "agentic.completion_rejected";
/// A reviewer's rework refused for resting on a reading the operator ruled out.
///
/// Durable for the same reason `agentic.completion_rejected` is: without a record,
/// a reviewer can send the same Task back on the same ruled-out reading once per
/// attempt, forever, and nothing in the Program ever shows it happened. The event
/// is folded into `program_warnings`, which is the field the root model and the
/// monitoring layer already read.
const REVIEW_DISCIPLINE_CONFLICT_EVENT_KIND: &str = "agentic.review_discipline_conflict";
const TOPIC_CURSOR_EVENT_KIND: &str = "agentic.topic_observation_acknowledged";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgenticTopicObservationPage {
    pub entries: Vec<AgenticTopicObservation>,
    pub from_revision: u64,
    pub to_revision: u64,
    pub cursor_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct AgenticTopicObservation {
    pub topic_ref: String,
    #[serde(flatten)]
    pub entry: AgenticTopicEntryProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TopicObservationKind {
    ProviderModel,
    WorkerTransport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgenticTopicObservationAck {
    pub program_id: String,
    pub execution_id: String,
    pub through_revision: u64,
    pub expected_cursor_revision: u64,
    pub observation_kind: TopicObservationKind,
}

#[derive(Debug, Error)]
pub enum AgentActionServiceError {
    #[error(transparent)]
    Store(#[from] RuntimeEventStoreError),
    #[error("agent action event store failed: {0}")]
    EventStore(String),
    #[error("agent action serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("agent action journal is corrupt: {0}")]
    Corrupt(String),
    #[error("member action transaction was cancelled before commit")]
    MemberCancelled,
    #[error("member action transaction outcome is unknown: {0}")]
    MemberUnknown(String),
}

impl AgentActionServiceError {
    pub(crate) fn fixed_kind(&self) -> &'static str {
        match self {
            Self::Store(_) => "store",
            Self::EventStore(_) => "event_store",
            Self::Serialization(_) => "serialization",
            Self::Corrupt(_) => "corrupt",
            Self::MemberCancelled => "member_cancelled",
            Self::MemberUnknown(_) => "member_unknown",
        }
    }

    pub(crate) fn fixed_store_kind(&self) -> Option<&'static str> {
        match self {
            Self::Store(error) => Some(error.fixed_kind()),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct AgentActionService {
    store: Arc<RuntimeEventStore>,
    artifacts: Option<Arc<crate::ArtifactStore>>,
    process_input_verifier: Option<crate::agentic::review_evidence::ProcessInputVerifier>,
    proof_session_queries: Option<Arc<OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>>,
    graphs: Option<crate::ExecutionGraphStateStore>,
    read_model: Arc<super::AgenticReadModel>,
}

struct RuntimeRetirementAdmission {
    request_digest: String,
    program_id: String,
    program_revision: u64,
    sources: Vec<ExpectedStreamRevision>,
}

impl RuntimeRetirementAdmission {
    fn covers(
        &self,
        projection: &AgenticProgramProjection,
        envelope: &AgentActionEnvelope,
    ) -> bool {
        self.program_revision == projection.revision
            && self.program_id == projection.program_id
            && projection.program_id == envelope.actor.program_id
            && envelope.actor.kind == AgentActorKind::Supervisor
            && envelope.actor.actor_id == "runtime.program-supervisor"
            && self.request_digest == super::review_evidence::proof_value_digest(envelope)
            && matches!(&envelope.action, AgentAction::TaskAttemptFail(_))
    }
}

impl AgentActionService {
    fn native_retirement_original_model<'a>(
        original: &'a harness_contract::execution_graph::ExecutionGraph,
        child: &harness_contract::execution_graph::ExecutionGraph,
    ) -> Result<&'a harness_contract::execution_graph::ExecutionNodeSpec, String> {
        use harness_contract::execution_graph::ExecutionNodeKind;
        let mut models = original.nodes.iter().filter(|node| {
            node.kind == ExecutionNodeKind::InlineModel && node.executor_kind == "inline_model"
        });
        let base = models
            .next()
            .ok_or("runtime retirement Native original model missing")?;
        if models.next().is_some() {
            return Err("runtime retirement Native original model is ambiguous".into());
        }
        let mut retained = child.nodes.iter().filter(|node| node.id == base.id);
        if retained.next() != Some(base) || retained.next().is_some() {
            return Err("runtime retirement Native original model changed".into());
        }
        Ok(base)
    }

    fn validate_native_retirement_target_guard(
        original: &harness_contract::execution_graph::ExecutionGraph,
        child: &harness_contract::execution_graph::ExecutionGraph,
        guard: &harness_contract::execution_graph::ExecutionNodeSpec,
    ) -> Result<(), String> {
        use crate::execution_core::graph::executors::CompileTargetGuardExecutor;
        use crate::execution_core::graph::{ExecutionCompileRequest, ExecutionGraphCompiler};
        use crate::execution_core::RuntimeCompileTarget;
        use harness_contract::execution_graph::ExecutionNodeKind;
        if original.id != child.id
            || original.parent_execution.is_none()
            || original.lineage.is_none()
            || original.parent_execution != child.parent_execution
            || original.lineage != child.lineage
            || original.service_class != child.service_class
            || !original.nodes.iter().any(|node| node == guard)
            || !child.nodes.iter().any(|node| node == guard)
        {
            return Err("runtime retirement Native target guard owner changed".into());
        }
        let bootstrap = original
            .nodes
            .iter()
            .filter(|node| node.kind != ExecutionNodeKind::SessionDispatch)
            .collect::<Vec<_>>();
        let base = Self::native_retirement_original_model(original, child)?;
        if guard.kind != ExecutionNodeKind::Verify
            || guard.executor_kind != CompileTargetGuardExecutor::KIND
            || guard.payload_ref != base.payload_ref
        {
            return Err("runtime retirement Native target guard payload changed".into());
        }
        let initial: serde_json::Value =
            serde_json::from_str(&base.payload_ref).map_err(|error| error.to_string())?;
        let target: RuntimeCompileTarget =
            serde_json::from_value(initial["compile_target"].clone())
                .map_err(|error| error.to_string())?;
        let scopes = bootstrap
            .iter()
            .find(|node| {
                node.kind == ExecutionNodeKind::Verify
                    && node.executor_kind == CompileTargetGuardExecutor::KIND
                    && node.acceptance.criteria == ["mutation_resources_must_be_leased"]
            })
            .map(|node| node.resource_scopes.clone())
            .unwrap_or_default();
        let mut compiled = ExecutionGraphCompiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: original.objective.clone(),
                payload_ref: base.payload_ref.clone(),
                target,
                resource_scopes: scopes,
            })
            .map_err(|error| error.to_string())?;
        let root = bootstrap
            .first()
            .ok_or("runtime retirement Native bootstrap is empty")?;
        let ingress = initial
            .get("ingress")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value::<crate::host::TurnIngressRef>(value.clone()))
            .transpose()
            .map_err(|error| error.to_string())?;
        let initial_compile =
            ingress.is_none() && base.idempotency_key == format!("{}:attempt", base.id);
        ExecutionGraphCompiler.bind_conversation_graph_identity(
            &mut compiled,
            &original.id,
            (!initial_compile).then_some(root.id.as_str()),
            ingress.as_ref().map(|value| value.request_id.as_str()),
        )?;
        if !compiled.nodes.iter().eq(bootstrap.iter().copied()) {
            return Err("runtime retirement Native target guard shape/criterion mismatch".into());
        }
        if let Some(dispatch) = original
            .nodes
            .iter()
            .find(|node| node.kind == ExecutionNodeKind::SessionDispatch)
        {
            compiled.nodes.insert(0, dispatch.clone());
            compiled.edges.insert(
                0,
                harness_contract::execution_graph::ExecutionEdge {
                    from: dispatch.id.clone(),
                    to: root.id.clone(),
                    kind: harness_contract::execution_graph::ExecutionEdgeKind::DependsOn,
                },
            );
        }
        harness_contract::execution_graph::validate_execution_graph(original)
            .map_err(|error| error.to_string())?;
        harness_contract::execution_graph::validate_execution_graph(child)
            .map_err(|error| error.to_string())?;
        if compiled.nodes != original.nodes
            || compiled.edges.len() != original.edges.len()
            || compiled
                .edges
                .iter()
                .any(|edge| !original.edges.contains(edge))
            || compiled
                .nodes
                .iter()
                .any(|node| !child.nodes.contains(node))
            || compiled
                .edges
                .iter()
                .any(|edge| !child.edges.contains(edge))
        {
            return Err("runtime retirement Native target guard topology changed".into());
        }
        Ok(())
    }

    fn validate_retired_native_child(
        &self,
        child: &harness_contract::execution_graph::ExecutionGraph,
        packet: &harness_contract::agent::AgentTaskPacket,
        pin: &mut impl FnMut(&str) -> Result<u64, String>,
    ) -> Result<(), String> {
        use crate::execution_core::graph::{
            ExecutionCommitService, ExecutionGraphStateStore, ExecutionNodeBinding,
        };
        use harness_contract::agent::AgentExecutorPolicy;
        use harness_contract::execution_graph::{
            ExecutionNodeKind, ExecutionNodeStatus, ExecutionServiceClass,
        };
        let compiled = packet
            .binding
            .as_ref()
            .ok_or("runtime retirement Native binding missing")?;
        if compiled.executor != AgentExecutorPolicy::CowdNative {
            return Err("runtime retirement non-Native conversation child".into());
        }
        if !matches!(
            child.service_class,
            ExecutionServiceClass::Foreground | ExecutionServiceClass::Background
        ) || child.nodes.is_empty()
        {
            return Err("runtime retirement Native child service class/shape mismatch".into());
        }
        let turn_id = packet
            .assignment
            .execution_identity
            .turn_id()
            .ok_or("runtime retirement Native original turn missing")?;
        let (_, exit_sources) = crate::agent_runtime::confirmed_run_exit(&self.store, packet)?
            .ok_or("runtime retirement Native child has no exact exit")?;
        for source in exit_sources {
            if pin(&source.stream_id)? != source.expected_revision {
                return Err("runtime retirement Native lifecycle changed".into());
            }
        }
        let agent_stream = format!("agent:{}", packet.agent_id());
        let backend = self
            .store
            .event_by_idempotency_key(
                &agent_stream,
                &format!("agent.backend_admitted:{}", packet.run_id()),
            )
            .map_err(|error| error.to_string())?
            .ok_or("runtime retirement Native child has no original backend admission")?;
        let exit = self
            .store
            .event_by_idempotency_key(
                &agent_stream,
                &format!("agent.local_exit:{}", packet.run_id()),
            )
            .map_err(|error| error.to_string())?
            .ok_or("runtime retirement Native child exit missing")?;
        let genesis = self
            .store
            .list_stream_after(&child.id, 0, 1, 1, 4 * 1024 * 1024)?;
        if genesis.first().is_none_or(|event| {
            event.commit_cursor <= backend.commit_cursor
                || event.commit_cursor >= exit.commit_cursor
        }) {
            return Err(
                "runtime retirement Native child is outside original backend lifetime".into(),
            );
        }
        let original = self.store.resolve_graph_record(&genesis[0])?;
        let original: crate::execution_core::graph::ExecutionGraphEvent =
            serde_json::from_value(original).map_err(|error| error.to_string())?;
        let crate::execution_core::graph::ExecutionGraphEvent::Planned { graph: original } =
            original
        else {
            return Err("runtime retirement Native original graph plan missing".into());
        };
        if original.id != child.id
            || original.service_class != child.service_class
            || original.lineage != child.lineage
            || original.parent_execution != child.parent_execution
        {
            return Err("runtime retirement Native original graph ownership changed".into());
        }
        let base = Self::native_retirement_original_model(&original, child)?;
        let initial: serde_json::Value =
            serde_json::from_str(&base.payload_ref).map_err(|error| error.to_string())?;
        if base.kind != ExecutionNodeKind::InlineModel
            || base.executor_kind != "inline_model"
            || initial["kind"] != "conversation_turn"
            || initial["session_id"] != packet.session_id()
            || initial["objective"] != child.objective
        {
            return Err("runtime retirement Native conversation origin mismatch".into());
        }
        let commits = ExecutionCommitService::new(Arc::clone(&self.store));
        let state = ExecutionGraphStateStore::new(Arc::clone(&self.store));
        let mut effect_streams = std::collections::BTreeSet::new();
        for child_node in &child.nodes {
            let status = child
                .node_statuses
                .get(&child_node.id)
                .filter(|status| status.is_terminal())
                .ok_or("runtime retirement Native child node is not terminal")?;
            let expected_executor = match child_node.kind {
                ExecutionNodeKind::InlineModel => "inline_model",
                ExecutionNodeKind::Verify => {
                    Self::validate_native_retirement_target_guard(&original, child, child_node)?;
                    crate::execution_core::graph::executors::CompileTargetGuardExecutor::KIND
                }
                ExecutionNodeKind::ToolBatch => "tool_batch",
                ExecutionNodeKind::Synthesize => "synthesize",
                _ => return Err("runtime retirement Native child has unmodelled node kind".into()),
            };
            if child_node.executor_kind != expected_executor
                || !child_node.id.starts_with(&format!("{}:", child.id))
            {
                return Err("runtime retirement Native child executor/identity mismatch".into());
            }
            if child_node.kind == ExecutionNodeKind::ToolBatch {
                let payload = crate::step_snapshot::decode_persisted_tool_batch_value(
                    &child_node.payload_ref,
                )
                .map_err(|error| error.to_string())?;
                let steps = payload["step_snapshots"]
                    .as_object()
                    .filter(|steps| !steps.is_empty())
                    .ok_or("runtime retirement Native tool Steps missing")?;
                for step in steps.values() {
                    let identity = &step["identity"];
                    if identity["session_id"] != packet.session_id()
                        || identity["turn_id"] != turn_id
                        || identity["execution_id"] != packet.run_id()
                        || identity["run_ref"] != packet.run_id()
                        || identity["task_ref"] != packet.task_id()
                    {
                        return Err(
                            "runtime retirement Native tool Step has a foreign owner".into()
                        );
                    }
                }
                continue;
            }
            if child_node.idempotency_key != format!("{}:attempt", child_node.id) {
                return Err("runtime retirement Native effect key mismatch".into());
            }
            let attempt = child
                .recovery_cursor
                .node_attempts
                .get(&child_node.id)
                .copied()
                .unwrap_or(0);
            if attempt == 0 {
                if *status == ExecutionNodeStatus::Cancelled {
                    continue;
                }
                return Err("runtime retirement Native child has no original attempt".into());
            }
            let binding = state
                .current_binding(child, &child_node.id, attempt)
                .map_err(|error| error.to_string())?;
            let (ticket, original_spec) = state
                .binding_start_ticket(child, &child_node.id, &binding)
                .map_err(|error| error.to_string())?;
            if original_spec != *child_node {
                return Err("runtime retirement Native current attempt spec changed".into());
            }
            let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
            effect_streams.insert(stream);
        }
        self.validate_native_effect_history(child, &commits, effect_streams, pin)?;
        Ok(())
    }

    fn validate_native_effect_history(
        &self,
        child: &harness_contract::execution_graph::ExecutionGraph,
        commits: &crate::execution_core::graph::ExecutionCommitService,
        mut effect_streams: std::collections::BTreeSet<String>,
        pin: &mut impl FnMut(&str) -> Result<u64, String>,
    ) -> Result<(), String> {
        use crate::execution_core::graph::{
            ExecutionEffectState, ExecutionGraphStateStore, ExecutionNodeBinding,
            NodeExecutionTicket,
        };
        use harness_contract::execution_graph::ExecutionNodeKind;
        let state = ExecutionGraphStateStore::new(Arc::clone(&self.store));
        let mut after = 0;
        let mut historical_tickets = BTreeMap::new();
        let mut expected_records = 0usize;
        while after < child.revision {
            let (next, starts) = state
                .binding_starts_page(&child.id, after, child.revision)
                .map_err(|error| error.to_string())?;
            if next <= after {
                return Err("runtime retirement Native start history did not advance".into());
            }
            for (node_id, binding, position) in starts {
                let node =
                    child.nodes.iter().find(|node| node.id == node_id).ok_or(
                        "runtime retirement Native child has an unmodelled historical node",
                    )?;
                let (ticket, original_spec) = state
                    .binding_start_ticket(child, &node_id, &binding)
                    .map_err(|error| error.to_string())?;
                if original_spec != *node {
                    return Err("runtime retirement Native historical spec changed".into());
                }
                let completed = state
                    .committed_binding_result(child, &node_id, &binding)
                    .map_err(|error| error.to_string())?;
                let retirement = state
                    .binding_retirement_record(child, &node_id, &binding)
                    .map_err(|error| error.to_string())?;
                if completed.is_none() && retirement.is_none() {
                    return Err("runtime retirement Native historical attempt is unfinished".into());
                }
                if !state
                    .binding_lease_closure(child, &node_id, &binding, position)
                    .map_err(|error| error.to_string())?
                {
                    return Err(
                        "runtime retirement Native historical callback or physical closure missing"
                            .into(),
                    );
                }
                if node.kind == ExecutionNodeKind::ToolBatch {
                    if let Some(sources) = commits
                        .authenticated_no_dispatch_sources(child, &node_id, &binding)
                        .map_err(|error| error.to_string())?
                    {
                        for source in sources {
                            if pin(&source.stream_id)? != source.expected_revision {
                                return Err(
                                    "runtime retirement Native NoDispatch physical source changed"
                                        .into(),
                                );
                            }
                        }
                    }
                    continue;
                }
                let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
                if historical_tickets.contains_key(&stream) {
                    return Err("runtime retirement Native duplicate historical effect".into());
                }
                let records = if let Some((result, finished)) = completed {
                    match commits
                        .inspect_execution_effect(&ticket)
                        .map_err(|error| error.to_string())?
                    {
                        ExecutionEffectState::Completed(outcome) if outcome.result == result => {}
                        _ => {
                            return Err(
                                "runtime retirement Native historical result conflict".into()
                            )
                        }
                    }
                    let receipt = self
                        .store
                        .event_by_idempotency_key(
                            &stream,
                            &format!("{}:receipt", ticket.idempotency_key),
                        )
                        .map_err(|error| error.to_string())?
                        .ok_or("runtime retirement Native historical receipt missing")?;
                    if receipt.transaction_id != finished.transaction_id
                        || receipt.commit_cursor != finished.commit_cursor
                    {
                        return Err(
                            "runtime retirement Native historical result is not atomic".into()
                        );
                    }
                    2u64
                } else {
                    if !matches!(
                        commits
                            .inspect_execution_effect(&ticket)
                            .map_err(|error| error.to_string())?,
                        ExecutionEffectState::Uncertain
                    ) {
                        return Err("runtime retirement Native retired attempt has no exact original intent".into());
                    }
                    1u64
                };
                if pin(&stream)? != records {
                    return Err(
                        "runtime retirement Native effect head conflicts with original disposition"
                            .into(),
                    );
                }
                expected_records = expected_records
                    .checked_add(usize::try_from(records).map_err(|error| error.to_string())?)
                    .ok_or("runtime retirement Native effect count overflow")?;
                effect_streams.insert(stream.clone());
                historical_tickets.insert(stream, ticket);
            }
            after = next;
        }
        if effect_streams.len() != historical_tickets.len() {
            return Err("runtime retirement Native effect has no canonical start".into());
        }
        let prefix = ExecutionNodeBinding::effect_stream_prefix(&child.id);
        let max_records = expected_records;
        let mut position = None;
        let mut observed = 0usize;
        loop {
            let page = self.store.list_scope_stream_prefix_page_asc(
                RuntimeEventScope::ExecutionNode,
                &prefix,
                position,
                16,
            )?;
            if page.is_empty() {
                break;
            }
            if page.len() > 16 {
                return Err("runtime retirement Native effect page overflow".into());
            }
            for record in page {
                let next = (record.commit_cursor, record.transaction_index);
                if record.commit_cursor == 0
                    || position.is_some_and(|previous| next <= previous)
                    || record.scope != RuntimeEventScope::ExecutionNode
                    || !record.stream_id.starts_with(&prefix)
                {
                    return Err("runtime retirement Native effect page source mismatch".into());
                }
                position = Some(next);
                let ticket = historical_tickets
                    .get(&record.stream_id)
                    .ok_or("runtime retirement Native child has an unmodelled historical effect")?;
                let (suffix, status, sequence) = match record.kind.as_str() {
                    "execution.effect.intent" => ("intent", "inflight", 1),
                    "execution.effect.receipt" => ("receipt", "completed", 2),
                    _ => return Err("runtime retirement Native effect kind mismatch".into()),
                };
                if record.schema_version != 1
                    || record.sequence != sequence
                    || record.actor.as_deref() != Some(ticket.executor_kind.as_str())
                    || record.status.as_deref() != Some(status)
                    || record.idempotency_key.as_deref()
                        != Some(format!("{}:{suffix}", ticket.idempotency_key).as_str())
                {
                    return Err("runtime retirement Native effect identity mismatch".into());
                }
                if suffix == "intent" {
                    let original: NodeExecutionTicket = serde_json::from_value(record.payload)
                        .map_err(|error| error.to_string())?;
                    if original != *ticket
                        || ExecutionNodeBinding::effect_stream_id(&original) != record.stream_id
                    {
                        return Err("runtime retirement Native effect ticket mismatch".into());
                    }
                }
                observed += 1;
                if observed > max_records {
                    return Err(
                        "runtime retirement Native effect history exceeds original protocol".into(),
                    );
                }
            }
        }
        if observed != max_records {
            return Err("runtime retirement Native effect history is incomplete".into());
        }
        Ok(())
    }

    /// A failed local control operation can be physically finished without
    /// succeeding at its business action. This narrow admission is used only
    /// after the original exact run/graph/closure checks; it grants no review
    /// or Goal credit and does not admit failed remote or Process work.
    fn authenticate_failed_control_retirement(
        &self,
        packet: &harness_contract::agent::AgentTaskPacket,
        attempt: u32,
        index: &crate::DurableRuntimeEvent,
        pin: &mut impl FnMut(&str) -> Result<u64, String>,
    ) -> Result<(), String> {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind};
        let denied =
            || "runtime retirement failed control receipt is not authenticated".to_string();
        let key = index.payload["idempotency_key"]
            .as_str()
            .ok_or_else(denied)?;
        let source = format!("execution-effect:{key}");
        if pin(&source)? != 2 {
            return Err(denied());
        }
        let primary = self
            .store
            .event_by_idempotency_key(&source, &format!("{key}:receipt"))
            .map_err(|error| error.to_string())?
            .ok_or_else(denied)?;
        let intent = self
            .store
            .event_by_idempotency_key(&source, &format!("{key}:intent"))
            .map_err(|error| error.to_string())?
            .ok_or_else(denied)?;
        let effect: ToolEffectDescriptor =
            serde_json::from_value(intent.payload["effect"].clone()).map_err(|_| denied())?;
        let outcome: crate::RuntimeToolExecutionOutcome =
            serde_json::from_value(primary.payload["outcome"].clone()).map_err(|_| denied())?;
        let expected_parent = json!({"execution_id":packet.graph_id(),"node_id":packet.node_id()});
        let expected_index = format!(
            "execution-agent-receipts:{}:{}:{attempt}",
            packet.graph_id(),
            packet.node_id()
        );
        let index_revision = pin(&expected_index)?;
        validate_receipt_index_record(index, &expected_index, index.sequence, index_revision)?;
        let intent_index = self
            .store
            .event_by_idempotency_key(&expected_index, &format!("agent-tool-intent:{key}"))
            .map_err(|error| error.to_string())?
            .ok_or_else(denied)?;
        validate_receipt_index_record(
            &intent_index,
            &expected_index,
            intent_index.sequence,
            index_revision,
        )?;
        let expected_refs = vec![
            crate::RuntimeEventRef {
                kind: "tool_invocation".into(),
                id: outcome.tool_use_id.clone(),
            },
            crate::RuntimeEventRef {
                kind: "execution_graph".into(),
                id: packet.graph_id().into(),
            },
            crate::RuntimeEventRef {
                kind: "execution_node".into(),
                id: packet.node_id().into(),
            },
            crate::RuntimeEventRef {
                kind: "agent_attempt".into(),
                id: attempt.to_string(),
            },
        ];
        if effect.effect_kind != ToolEffectKind::Write
            || effect.uses_network
            || effect.spawns_process
            || effect.mutates_packages
            || effect.mutates_system
            || effect.scopes.is_empty()
            || effect.scopes.iter().any(|scope| {
                scope.resource != PermissionResource::Tool
                    || scope.operation != PermissionOperation::Control
                    || scope.target.is_some()
            })
            || outcome.status != crate::RuntimeToolExecutionStatus::Failed
            || outcome.tool_name != effect.tool_id
            || !packet.allowed_tools.contains(&effect.tool_id)
            || packet
                .binding
                .as_ref()
                .is_none_or(|binding| !binding.tool_contract_refs.contains(&effect.tool_id))
            || index.stream_id != expected_index
            || index.kind != "execution.agent_tool.receipt"
            || primary.kind != "execution.effect.receipt"
            || primary.status.as_deref() != Some("completed")
            || primary.sequence != 2
            || primary.schema_version != 1
            || intent.kind != "execution.effect.intent"
            || intent.status.as_deref() != Some("inflight")
            || intent.sequence != 1
            || intent.schema_version != 1
            || [index, &primary, &intent].iter().any(|event| {
                event.actor.as_deref() != Some("governed_tool")
                    || event.scope != RuntimeEventScope::ExecutionNode
            })
            || primary.transaction_id != index.transaction_id
            || primary.commit_cursor != index.commit_cursor
            || primary.transaction_index >= index.transaction_index
            || intent.commit_cursor >= primary.commit_cursor
            || primary.refs != expected_refs
            || primary.refs != index.refs
            || intent.refs != primary.refs
            || intent_index.refs != intent.refs
            || intent_index.kind != "execution.agent_tool.intent"
            || intent_index.status.as_deref() != Some("inflight")
            || intent_index.sequence >= index.sequence
            || intent.transaction_id != intent_index.transaction_id
            || intent.commit_cursor != intent_index.commit_cursor
            || intent.transaction_index >= intent_index.transaction_index
            || intent_index.payload["effect_kind"] != "write"
            || intent_index.payload["tool_name"] != effect.tool_id
            || intent_index.payload["descriptor_hash"] != effect.descriptor_hash
            || intent_index.payload["resolved_effect_scope"]
                != intent.payload["resolved_effect_scope"]
            || intent_index.payload["effect_scope"] != index.payload["effect_scope"]
            || intent_index.payload["effect_scopes"] != index.payload["effect_scopes"]
            || primary.payload["input_sha256"] != intent.payload["input_sha256"]
            || primary.payload["input_sha256"]
                .as_str()
                .is_none_or(|digest| digest.is_empty())
            || !primary.refs.iter().any(|reference| {
                reference.kind == "agent_attempt" && reference.id == attempt.to_string()
            })
            || primary.payload["idempotency_key"] != key
            || intent.payload["idempotency_key"] != key
            || primary.payload["outcome"] != index.payload["outcome"]
            || primary.payload["effect"] != intent.payload["effect"]
            || primary.payload["descriptor_hash"] != effect.descriptor_hash
            || primary.payload["tool_name"] != effect.tool_id
            || intent.payload["tool_name"] != effect.tool_id
            || primary.payload["replay"] != intent.payload["replay"]
            || primary.payload["replay"]["session_id"] != packet.session_id()
            || primary.payload["replay"]["parent_execution"] != expected_parent
            || primary.payload["replay"]["call_id"] != outcome.tool_use_id
            || primary.payload["replay"]["invocation"]["effect"] != intent.payload["effect"]
            || primary.payload["resolved_effect_scope"] != intent.payload["resolved_effect_scope"]
            || primary.payload["resolved_effect_scope"] != index.payload["resolved_effect_scope"]
            || index.payload["effect_kind"] != "write"
        {
            return Err(denied());
        }
        super::review_evidence::validate_original_effect_scope(
            &primary.payload["resolved_effect_scope"],
            &effect,
        )?;
        Ok(())
    }

    fn runtime_retirement_admission(
        &self,
        projection: &AgenticProgramProjection,
        envelope: &AgentActionEnvelope,
    ) -> Result<Option<RuntimeRetirementAdmission>, String> {
        use harness_contract::agent::{AgentTaskPacket, AgenticExecutionFocus};
        use harness_contract::agent_action::AgentAttemptMode;
        use harness_contract::execution_graph::ExecutionNodeKind;
        let AgentAction::TaskAttemptFail(input) = &envelope.action else {
            return Ok(None);
        };
        let Some(task) = projection.tasks.get(&input.task_ref) else {
            return Ok(None);
        };
        if task.status != AgenticTaskStatus::CancelRequested
            || task.pending_retirement != Some(super::program::AgenticTaskRetirement::Withdrawn)
            || envelope.actor.kind != AgentActorKind::Supervisor
        {
            return Ok(None);
        }
        if envelope.actor.actor_id != "runtime.program-supervisor"
            || envelope.actor.execution_id.as_deref() != Some(input.execution_id.as_str())
            || envelope.actor.agent_id.is_some()
            || envelope.actor.team_id.is_some()
            || task
                .cancel_requested_by
                .as_ref()
                .is_none_or(|value| value.is_empty())
            || task
                .cancel_reason_ref
                .as_ref()
                .is_none_or(|value| value.is_empty())
            || task.cancel_evidence_refs.is_empty()
        {
            return Err("runtime retirement actor or withdrawal binding mismatch".into());
        }
        let attempt = task
            .active_attempts
            .get(&input.execution_id)
            .ok_or("runtime retirement has no registered attempt")?;
        if attempt.mode != input.mode || attempt.execution_id != input.execution_id {
            return Err("runtime retirement registered mode mismatch".into());
        }
        let graphs = crate::ExecutionGraphStateStore::new(Arc::clone(&self.store));
        let graph = graphs
            .load(&input.execution_id)
            .map_err(|error| error.to_string())?;
        let mut nodes = graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask);
        let node = nodes
            .next()
            .ok_or("runtime retirement Agent node missing")?;
        if nodes.next().is_some() {
            return Err("runtime retirement Agent node ambiguous".into());
        }
        let mut packet: AgentTaskPacket =
            serde_json::from_str(&node.payload_ref).map_err(|error| error.to_string())?;
        let physical_attempt = graph.recovery_cursor.node_attempts.get(&node.id)
            .copied().filter(|attempt| *attempt > 0)
            .ok_or("runtime retirement current node attempt missing")?;
        graphs.current_binding(&graph, &node.id, physical_attempt)
            .map_err(|error| error.to_string())?;
        packet.attempt = physical_attempt;
        let binding = packet
            .agentic_binding
            .as_ref()
            .ok_or("runtime retirement Program binding missing")?;
        let focus_matches = match (&binding.focus, input.mode) {
            (AgenticExecutionFocus::TaskExecute { task_ref }, AgentAttemptMode::Execute) => {
                task_ref == &input.task_ref && retirement_execute_claim_matches(task, attempt)
            }
            (AgenticExecutionFocus::TaskReview { task_ref }, AgentAttemptMode::Review) => {
                task_ref == &input.task_ref && task.review_generation == attempt.generation
            }
            _ => false,
        };
        if !focus_matches
            || packet.graph_id() != graph.id
            || graph.id != input.execution_id
            || packet.node_id() != node.id
            || packet.session_id() != projection.session_id
            || packet.attempt == 0
            || graph.id != super::execution::helpers::deterministic_graph_id(
                &projection.program_id,
                &input.task_ref,
                &attempt.agent_id,
                match input.mode {
                    AgentAttemptMode::Execute => super::execution::helpers::DispatchMode::Execute,
                    AgentAttemptMode::Review => super::execution::helpers::DispatchMode::Review,
                    AgentAttemptMode::Coordination => return Err("runtime retirement coordination unsupported".into()),
                },
                attempt.generation,
            )
            || binding.program_id != projection.program_id
            || binding.agent_id != attempt.agent_id
            || binding.membership_id != attempt.membership_id
            || binding.task_team_id != task.team_id
            || !projection
                .membership_for(&binding.agent_id, &binding.team_id)
                .is_some_and(|member| member.membership_id == binding.membership_id)
            || packet.binding.as_ref().is_none_or(|compiled| {
                !crate::agent::binding::recompute_binding_digest(compiled)
                    .is_ok_and(|digest| digest == compiled.binding_digest)
            })
            || graph.lineage.as_ref().is_none_or(|lineage| {
                lineage.session_id != projection.session_id
                    || lineage.turn_id != projection.turn_id
                    || lineage.task_id != input.task_ref
            })
            || graph.recovery_cursor.node_attempts.get(&node.id) != Some(&packet.attempt)
        {
            return Err("runtime retirement immutable attempt binding mismatch".into());
        }
        let (_, exit_sources) = crate::agent_runtime::confirmed_run_exit(&self.store, &packet)?
            .ok_or("runtime retirement has no authenticated exact local backend exit")?;
        let mut sources = std::collections::BTreeMap::new();
        for source in exit_sources {
            if sources
                .insert(source.stream_id, source.expected_revision)
                .is_some_and(|previous| previous != source.expected_revision)
            {
                return Err("runtime retirement local exit source changed".into());
            }
        }
        if matches!(
            packet.binding.as_ref().map(|binding| &binding.executor),
            Some(harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. })
        ) {
            let lifecycle =
                crate::agent_runtime::authenticated_run_lifecycle(&self.store, &packet)?;
            if !lifecycle.no_direct_effects() {
                return Err(
                    "effect_review_pending: reaped Process retains opaque past effects".into(),
                );
            }
            for source in lifecycle.sources {
                if sources
                    .insert(source.stream_id, source.expected_revision)
                    .is_some_and(|previous| previous != source.expected_revision)
                {
                    return Err("runtime retirement physical envelope source changed".into());
                }
            }
        }
        let mut process_sources = std::collections::BTreeMap::new();
        let mut pin = |source: &str| -> Result<u64, String> {
            let revision = self
                .store
                .stream_revision(source)
                .map_err(|error| error.to_string())?;
            if sources
                .insert(source.to_string(), revision)
                .is_some_and(|previous| previous != revision)
            {
                return Err("runtime retirement source changed during admission".into());
            }
            Ok(revision)
        };
        let mut allowed_programs =
            std::collections::BTreeSet::from([projection.program_id.clone()]);
        let mut roots = projection
            .root_execution_id
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut continuation = projection.continuation.clone();
        while let Some(binding) = continuation {
            if !allowed_programs.insert(binding.source_program_id.clone()) {
                return Err("runtime retirement has cyclic Program provenance".into());
            }
            let origin = self
                .project(&binding.source_program_id)
                .map_err(|error| error.to_string())?;
            if origin.session_id != projection.session_id
                || origin.objective_id != binding.source_objective_id
                || origin.root_execution_id.as_deref() != Some(binding.source_root_id.as_str())
                || pin(&format!("agentic-program:{}", origin.program_id))? != origin.revision
            {
                return Err("runtime retirement historical Program binding changed".into());
            }
            roots.push(binding.source_root_id);
            continuation = origin.continuation;
        }
        if roots.is_empty() {
            return Err("runtime retirement physical history has no authenticated root".into());
        }
        let discovery_cursor = self
            .store
            .durable_commit_cursor()
            .map_err(|error| error.to_string())?;
        let mut pending = roots
            .into_iter()
            .map(|root| (root, None, false))
            .collect::<Vec<_>>();
        let mut parents = std::collections::BTreeMap::new();
        while let Some((graph_id, expected_parent, inherited_relevance)) = pending.pop() {
            if let Some(previous) = parents.insert(graph_id.clone(), expected_parent.clone()) {
                if previous != expected_parent {
                    return Err(
                        "runtime retirement physical lineage has ambiguous ownership".into(),
                    );
                }
                continue;
            }
            let historical = graphs.load(&graph_id).map_err(|error| error.to_string())?;
            let genesis = self
                .store
                .list_stream_after(&graph_id, 0, 1, 1, 4 * 1024 * 1024)?;
            if genesis
                .first()
                .is_none_or(|event| event.commit_cursor > discovery_cursor)
                || historical.parent_execution != expected_parent
                || historical
                    .lineage
                    .as_ref()
                    .is_none_or(|lineage| lineage.session_id != projection.session_id)
                || pin(&graph_id)? != historical.revision
            {
                return Err("runtime retirement physical history changed during discovery".into());
            }
            pin(&format!("execution-lineage:{graph_id}"))?;
            let mut relevant_graph = inherited_relevance;
            for historical_node in &historical.nodes {
                if historical_node.kind != ExecutionNodeKind::AgentTask {
                    continue;
                }
                let original: AgentTaskPacket = serde_json::from_str(&historical_node.payload_ref)
                    .map_err(|error| error.to_string())?;
                let relevant_packet = inherited_relevance
                    || original.agentic_binding.as_ref().is_none_or(|binding| {
                        !allowed_programs.contains(&binding.program_id)
                            || (binding.program_id == projection.program_id
                                && match &binding.focus {
                                    AgenticExecutionFocus::TaskExecute { task_ref }
                                    | AgenticExecutionFocus::TaskReview { task_ref } => {
                                        task_ref == &input.task_ref
                                    }
                                    AgenticExecutionFocus::Coordination { wake_ref } => projection
                                        .coordination_wake_ref(wake_ref)
                                        .and_then(|(_, wake)| wake.intent.as_ref())
                                        .is_none_or(|intent| intent.task_ref == input.task_ref),
                                    _ => true,
                                })
                    });
                relevant_graph |= relevant_packet;
                if original.binding.as_ref().is_some_and(|binding| {
                    binding.executor == harness_contract::agent::AgentExecutorPolicy::CowdNative
                }) {
                    continue;
                }
                let lifecycle =
                    crate::agent_runtime::authenticated_run_lifecycle(&self.store, &original)?;
                for source in &lifecycle.sources {
                    if pin(&source.stream_id)? != source.expected_revision {
                        return Err("runtime retirement historical Agent cut changed".into());
                    }
                }
                if original.graph_id() != graph_id
                    || original.node_id() != historical_node.id
                    || original
                        .agentic_binding
                        .as_ref()
                        .is_none_or(|binding| !allowed_programs.contains(&binding.program_id))
                    || (relevant_packet
                        && (historical
                            .recovery_cursor
                            .node_attempts
                            .get(&historical_node.id)
                            .copied()
                            .unwrap_or(original.attempt)
                            > original.attempt
                            || !lifecycle.no_direct_effects()))
                {
                    return Err(
                        "effect_review_pending: opaque Process history remains after physical reap"
                            .into(),
                    );
                }
            }
            for link in graphs
                .child_links(&graph_id)
                .map_err(|error| error.to_string())?
            {
                if link.parent_execution_id != graph_id
                    || link.child_execution_id == graph_id
                    || !historical
                        .nodes
                        .iter()
                        .any(|node| node.id == link.parent_node_id)
                {
                    return Err("runtime retirement historical child binding invalid".into());
                }
                pending.push((
                    link.child_execution_id,
                    Some(harness_contract::execution_graph::ExecutionParentBinding {
                        execution_id: graph_id.clone(),
                        node_id: link.parent_node_id,
                    }),
                    relevant_graph,
                ));
            }
        }
        if pin(&graph.id)? != graph.revision {
            return Err("runtime retirement graph changed during admission".into());
        }
        if graph.nodes.is_empty()
            || graph.nodes.iter().any(|node| {
                graph
                    .node_statuses
                    .get(&node.id)
                    .is_none_or(|status| !status.is_terminal())
            })
        {
            return Err("runtime retirement physical graph is not terminal".into());
        }
        pin(&format!("execution-lineage:{}", graph.id))?;
        let children = graphs
            .child_links(&graph.id)
            .map_err(|error| error.to_string())?;
        for link in children {
            if link.parent_execution_id != graph.id || link.parent_node_id != node.id {
                return Err("runtime retirement child has a foreign parent".into());
            }
            let child = graphs
                .load(&link.child_execution_id)
                .map_err(|error| error.to_string())?;
            if pin(&child.id)? != child.revision
                || child.lineage != graph.lineage
                || child.parent_execution.as_ref().is_none_or(|parent| {
                    parent.execution_id != graph.id || parent.node_id != node.id
                })
                || child.nodes.iter().any(|node| {
                    child
                        .node_statuses
                        .get(&node.id)
                        .is_none_or(|status| !status.is_terminal())
                })
            {
                return Err(
                    "runtime retirement child is not an authenticated terminal graph".into(),
                );
            }
            pin(&format!("execution-lineage:{}", child.id))?;
            if !graphs
                .child_links(&child.id)
                .map_err(|error| error.to_string())?
                .is_empty()
            {
                return Err("runtime retirement tool child has unmodelled descendants".into());
            }
            if !crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(
                &child,
            ) {
                self.validate_retired_native_child(&child, &packet, &mut pin)?;
                continue;
            }
            if child.service_class != graph.service_class {
                return Err("runtime retirement tool child service class mismatch".into());
            }
            let bound =
                crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_graph(
                    &child,
                )
                .map_err(|error| error.to_string())?;
            let request = bound.request;
            if bound.agent_id != packet.agent_id()
                || bound.run_id != packet.run_id()
                || request.parent_execution.as_ref().is_none_or(|parent| {
                    parent.execution_id != graph.id || parent.node_id != node.id
                })
                || request.parent_execution_attempt != Some(packet.attempt)
                || request.session_id.as_deref() != Some(projection.session_id.as_str())
                || request.governed_plan_id != graph.id
                || !packet.allowed_tools.contains(&request.tool_name)
                || !packet.binding.as_ref().is_some_and(|compiled| {
                    compiled.tool_contract_refs.contains(&request.tool_name)
                })
            {
                return Err("runtime retirement tool child request mismatch".into());
            }
            let tool_node = &child.nodes[0];
            let attempt = child
                .recovery_cursor
                .node_attempts
                .get(&tool_node.id)
                .copied()
                .ok_or("runtime retirement NoDispatch tool original attempt missing")?;
            let binding = graphs
                .current_binding(&child, &tool_node.id, attempt)
                .map_err(|error| error.to_string())?;
            let proof_commits =
                crate::execution_core::graph::ExecutionCommitService::new(self.store.clone());
            if let Some(sources) = proof_commits
                .authenticated_no_dispatch_sources(&child, &tool_node.id, &binding)
                .map_err(|error| error.to_string())?
            {
                for source in sources {
                    if pin(&source.stream_id)? != source.expected_revision {
                        return Err("runtime retirement bound NoDispatch source changed".into());
                    }
                }
                continue;
            }
            let primary_source = format!("execution-effect:{}", request.idempotency_key);
            let index_source = format!(
                "execution-agent-receipts:{}:{}:{}",
                graph.id, node.id, packet.attempt
            );
            pin(&primary_source)?;
            pin(&index_source)?;
            let intent = self
                .store
                .event_by_idempotency_key(
                    &primary_source,
                    &format!("{}:intent", request.idempotency_key),
                )
                .map_err(|error| error.to_string())?;
            let read = intent.is_none();
            let primary = if let Some(intent) = intent {
                intent
            } else {
                self.store
                    .event_by_idempotency_key(
                        &primary_source,
                        &format!("{}:read-receipt", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("runtime retirement tool read receipt missing")?
            };
            let index = self
                .store
                .event_by_idempotency_key(
                    &index_source,
                    &format!(
                        "agent-tool-{}:{}",
                        if read { "receipt" } else { "intent" },
                        request.idempotency_key
                    ),
                )
                .map_err(|error| error.to_string())?
                .ok_or("runtime retirement original tool index missing")?;
            crate::execution_core::graph::ExecutionCommitService::validate_proof_bound_tool_request(&request, &primary, &index)
                .map_err(|error| error.to_string())?;
            let descriptor_value = if read {
                &primary.payload["replay"]["invocation"]["effect"]
            } else {
                &primary.payload["effect"]
            };
            let descriptor: harness_contract::tool::ToolEffectDescriptor =
                serde_json::from_value(descriptor_value.clone())
                    .map_err(|_| "runtime retirement original typed descriptor missing")?;
            if primary.payload["resolved_effect_scope"] != index.payload["resolved_effect_scope"] {
                return Err("runtime retirement original primary/index scope conflict".into());
            }
            if read && primary.payload["resolved_effect_scope"].is_null() {
                let seal = crate::context_evidence::decode_read_acquisition(
                    &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                )
                .map_err(str::to_string)?;
                if !matches!(
                    seal.body.extent,
                    crate::context_evidence::ReadExtent::ArtifactChunks { .. }
                ) || descriptor.effect_kind != harness_contract::tool::ToolEffectKind::Read
                    || descriptor.scopes.iter().any(|scope| {
                        scope.resource == harness_contract::policy::PermissionResource::File
                    })
                    || descriptor.uses_network
                    || descriptor.spawns_process
                    || descriptor.mutates_packages
                    || descriptor.mutates_system
                {
                    return Err("runtime retirement read lacks original physical scope v2".into());
                }
            } else {
                super::review_evidence::validate_original_effect_scope(
                    &primary.payload["resolved_effect_scope"],
                    &descriptor,
                )?;
            }
            if primary.commit_cursor != index.commit_cursor {
                return Err(
                    "runtime retirement tool provenance was not atomically admitted".into(),
                );
            }
            if !read {
                let receipt = self
                    .store
                    .event_by_idempotency_key(
                        &primary_source,
                        &format!("{}:receipt", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("runtime retirement physical effect is still pending")?;
                let index_receipt = self
                    .store
                    .event_by_idempotency_key(
                        &index_source,
                        &format!("agent-tool-receipt:{}", request.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("runtime retirement effect index is still pending")?;
                if receipt.kind != "execution.effect.receipt"
                    || receipt.actor.as_deref() != Some("governed_tool")
                    || index_receipt.kind != "execution.agent_tool.receipt"
                    || index_receipt.actor.as_deref() != Some("governed_tool")
                    || receipt.commit_cursor != index_receipt.commit_cursor
                    || receipt.payload["idempotency_key"] != request.idempotency_key
                    || index_receipt.payload["idempotency_key"] != request.idempotency_key
                    || receipt.payload["tool_name"] != request.tool_name
                    || crate::ExecutionCommitService::receipt_observation_sequence(&index_receipt)
                        != Some(request.observation_wave_sequence)
                    || index_receipt.payload["effect_kind"] != index.payload["effect_kind"]
                    || index_receipt.payload["effect_scope"] != index.payload["effect_scope"]
                    || index_receipt.payload["outcome"]["tool_use_id"] != request.tool_use_id
                    || index_receipt.payload["outcome"]["tool_name"] != request.tool_name
                    || receipt.payload["descriptor_hash"]
                        != primary.payload["effect"]["descriptor_hash"]
                    || receipt.payload["replay"] != primary.payload["replay"]
                    || receipt.payload["resolved_effect_scope"]
                        != primary.payload["resolved_effect_scope"]
                    || receipt.payload["resolved_effect_scope"]
                        != index_receipt.payload["resolved_effect_scope"]
                    || receipt.payload["outcome"] != index_receipt.payload["outcome"]
                {
                    return Err("runtime retirement effect receipt is not known settled".into());
                }
                match receipt.payload["outcome"]["status"].as_str() {
                    Some("executed" | "blocked_permission") => {}
                    Some("failed") => self.authenticate_failed_control_retirement(
                        &packet,
                        packet.attempt,
                        &index_receipt,
                        &mut pin,
                    )?,
                    _ => {
                        return Err("runtime retirement effect receipt is not known settled".into())
                    }
                }
                if descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Process {
                    for cut in super::review_evidence::validate_bounded_process_retirement(
                        &self.store,
                        &receipt,
                        self.artifacts.as_deref(),
                    )? {
                        if process_sources
                            .insert(cut.stream_id, cut.expected_revision)
                            .is_some_and(|previous| previous != cut.expected_revision)
                        {
                            return Err("runtime retirement process source moved".into());
                        }
                    }
                }
            } else if !matches!(
                primary.payload["outcome"]["status"].as_str(),
                Some("executed" | "blocked_permission")
            ) {
                return Err("runtime retirement tool read is not known settled".into());
            }
        }
        for attempt_number in 1..=packet.attempt {
            let source = format!(
                "execution-agent-receipts:{}:{}:{attempt_number}",
                graph.id, node.id
            );
            let revision = pin(&source)?;
            let mut after = 0;
            let mut pending = std::collections::BTreeSet::new();
            let mut settled = std::collections::BTreeSet::new();
            while after < revision {
                let events =
                    self.store
                        .list_stream_after(&source, after, revision, 64, 4 * 1024 * 1024)?;
                if events.is_empty() {
                    return Err("runtime retirement receipt history is incomplete".into());
                }
                for event in events {
                    validate_receipt_index_record(&event, &source, after + 1, revision)?;
                    after = event.sequence;
                    if event.actor.as_deref() != Some("governed_tool") {
                        return Err("runtime retirement receipt owner mismatch".into());
                    }
                    let identity = event.payload["idempotency_key"]
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .ok_or("runtime retirement receipt identity missing")?;
                    match event.kind.as_str() {
                        "execution.agent_tool.intent" => {
                            pending.insert(identity.to_string());
                        }
                        "execution.agent_tool.receipt"
                            if matches!(
                                event.payload["outcome"]["status"].as_str(),
                                Some("executed" | "blocked_permission")
                            ) =>
                        {
                            if event.payload["effect_kind"] == "process" {
                                let stream = format!("execution-effect:{identity}");
                                let primary = self
                                    .store
                                    .event_by_idempotency_key(
                                        &stream,
                                        &format!("{identity}:receipt"),
                                    )
                                    .map_err(|error| error.to_string())?
                                    .ok_or(
                                        "runtime retirement original process terminal missing",
                                    )?;
                                for cut in
                                    super::review_evidence::validate_bounded_process_retirement(
                                        &self.store,
                                        &primary,
                                        self.artifacts.as_deref(),
                                    )?
                                {
                                    if process_sources
                                        .insert(cut.stream_id, cut.expected_revision)
                                        .is_some_and(|previous| previous != cut.expected_revision)
                                    {
                                        return Err(
                                            "runtime retirement process source moved".into()
                                        );
                                    }
                                }
                            }
                            settled.insert(identity.to_string());
                        }
                        "execution.agent_tool.receipt"
                            if event.payload["outcome"]["status"] == "failed" =>
                        {
                            self.authenticate_failed_control_retirement(
                                &packet,
                                attempt_number,
                                &event,
                                &mut pin,
                            )?;
                            settled.insert(identity.to_string());
                        }
                        _ => {
                            return Err("runtime retirement receipt is unknown or unsettled".into());
                        }
                    }
                }
            }
            if !pending.is_subset(&settled) {
                return Err("runtime retirement still has a pending physical effect".into());
            }
        }
        for (source, revision) in process_sources {
            if sources
                .insert(source, revision)
                .is_some_and(|previous| previous != revision)
            {
                return Err("runtime retirement process source changed before settlement".into());
            }
        }
        for (source, revision) in &sources {
            if self
                .store
                .stream_revision(source)
                .map_err(|error| error.to_string())?
                != *revision
            {
                return Err("runtime retirement source changed before settlement".into());
            }
        }
        Ok(Some(RuntimeRetirementAdmission {
            request_digest: super::review_evidence::proof_value_digest(envelope),
            program_id: projection.program_id.clone(),
            program_revision: projection.revision,
            sources: sources
                .into_iter()
                .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                })
                .collect(),
        }))
    }

    #[must_use]
    pub fn new(store: Arc<RuntimeEventStore>) -> Self {
        Self {
            store,
            artifacts: None,
            process_input_verifier: None,
            proof_session_queries: None,
            graphs: None,
            read_model: Arc::new(super::AgenticReadModel::default()),
        }
    }

    #[must_use]
    pub(crate) fn program_recovery_is_excluded(&self, program_id: &str) -> Result<bool, String> {
        if !self.store.has_session_recovery_exclusions() {
            return Ok(false);
        }
        let stream = program_stream(program_id);
        let opened = self
            .store
            .event_by_idempotency_key(&stream, "agentic-program-opened")
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "session_recovery_program_opening_missing".to_string())?;
        if opened.stream_id != stream
            || opened.scope != RuntimeEventScope::Program
            || opened.kind != PROGRAM_OPENED_EVENT_KIND
            || opened.schema_version != 1
            || opened.sequence != 1
        {
            return Err("session_recovery_program_opening_invalid".into());
        }
        let session_id = opened
            .payload
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .filter(|session| !session.trim().is_empty())
            .ok_or_else(|| "session_recovery_program_session_missing".to_string())?;
        Ok(self.store.is_session_recovery_excluded(session_id))
    }

    pub(crate) fn recovery_session_is_excluded(&self, session_id: &str) -> bool {
        self.store.is_session_recovery_excluded(session_id)
    }

    fn ensure_recovery_session_admitted(
        &self,
        session_id: &str,
    ) -> Result<(), AgentActionServiceError> {
        self.store
            .ensure_session_recovery_admitted(session_id)
            .map_err(|reason| {
                AgentActionServiceError::Store(RuntimeEventStoreError::InvalidTransaction(reason))
            })
    }

    pub(crate) fn with_graph_store(mut self, graphs: crate::ExecutionGraphStateStore) -> Self {
        self.graphs = Some(graphs);
        self
    }

    #[must_use]
    pub(crate) fn with_read_model(mut self, read_model: Arc<super::AgenticReadModel>) -> Self {
        self.read_model = read_model;
        self
    }

    /// Bind the physical artifact authority used by production Runtime
    /// services. Unit projections may omit it; an active Runtime never does.
    #[must_use]
    pub(crate) fn with_process_input_verifier(
        mut self,
        verifier: crate::agentic::review_evidence::ProcessInputVerifier,
    ) -> Self {
        self.process_input_verifier = Some(verifier);
        self
    }

    pub fn with_artifact_store(mut self, artifacts: Arc<crate::ArtifactStore>) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    #[must_use]
    pub(crate) fn with_session_query_slot(
        mut self,
        queries: Arc<OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>,
    ) -> Self {
        self.proof_session_queries = Some(queries);
        self
    }

    /// Re-enter only the retained original transaction after an unknown
    /// response. The store checks the complete checked identity and receipt;
    /// no current projection or new grant is used to reconstruct the result.
    pub(crate) fn recover_retained_original_member(
        &self, transaction_id: &str,
    ) -> Result<Option<Arc<OriginalCommittedMember>>, AgentActionServiceError> {
        OriginalCommittedMember::recover_original(&self.store, transaction_id).map_err(AgentActionServiceError::Store)
    }

    /// The original authorization owner calls this only after matching the
    /// committed member and applying the same transition to its hot state.
    pub(crate) fn release_original_member_after_owner_apply(
        &self, committed: &OriginalCommittedMember,
    ) -> Result<(), AgentActionServiceError> {
        let authority = committed.authority().ok_or_else(|| AgentActionServiceError::Corrupt(
            "committed member authority missing".into()))?;
        self.store.release_reconciled_member(&committed.append.receipt.commit.transaction_id,
            &committed.append.request, authority, &committed.append.receipt.commit)?;
        Ok(())
    }

    /// Resolve an unknown response through the original retained request.
    /// The committed handle is returned even when hot reconciliation fails;
    /// only the original authorization owner's successful apply releases it.
    pub(crate) fn reconcile_retained_original_member(
        &self, transaction_id: &str, negotiator: &crate::AuthorizationNegotiator,
    ) -> Result<Option<(Arc<OriginalCommittedMember>, Result<(), String>)>, AgentActionServiceError> {
        let Some(committed) = self.recover_retained_original_member(transaction_id)? else { return Ok(None); };
        let authority = committed.authority().cloned().ok_or_else(|| AgentActionServiceError::Corrupt(
            "retained committed member authority missing".into()))?;
        let member_guard = authority.member().upgrade().ok_or_else(|| AgentActionServiceError::MemberUnknown("original member owner unavailable".into()))?;
        let approval_guard = authority.approval_guard(&member_guard).map_err(AgentActionServiceError::MemberUnknown)?;
        let applied = negotiator.apply_recovered_member_authority(&authority, &member_guard, approval_guard.as_deref(), &committed)
            .and_then(|()| self.store.release_reconciled_member(transaction_id, &committed.append.request,
                &authority, &committed.append.receipt.commit).map_err(|error| error.to_string()));
        let committed = if applied.is_ok() { committed.after_original_authority_release() } else { committed };
        Ok(Some((committed, applied)))
    }

    /// Consume the original member under the policy and stream locks acquired
    /// by its capability owner. Goal/review preparation is never discarded.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_member_locked(
        &self,
        envelope: &AgentActionEnvelope,
        goal: Option<(String, u64, RuntimeTransactionEventInput)>,
        verification: Option<&ObjectiveReviewVerification>,
        required_write_failures: &[serde_json::Value],
        sources: &[ExpectedStreamRevision],
        locks: &crate::runtime_event_store::HeldStreamLocks<'_>,
        control: Arc<crate::conversation::MemberCommitControl>,
        claim: &crate::execution_core::services::OriginalSessionClaim,
        consumed_input_sequence: usize,
        prepared: &crate::step_snapshot::PreparedAgentBatchMember,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        ticket: &crate::execution_core::graph::NodeExecutionTicket,
        original_events: &[crate::step_snapshot::OriginalMemberEventRef],
        authority: Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
        member_guard: &crate::authorization_negotiator::PreparedMemberBinding,
        reservation: &crate::runtime_event_store::OriginalMemberReservation,
        effect_fence: &crate::runtime_event_store::OriginalMemberEffectFence,
        outer_hook: Option<&Arc<crate::conversation::tool_plane::OriginalGovernedHook>>,
    ) -> Result<OriginalMemberSubmission, AgentActionServiceError> {
        if !authority.member().matches(member_guard) || member_guard.prepared() != prepared
            || !Arc::ptr_eq(member_guard.store(), &self.store)
            || member_guard.ticket() != ticket {
            return Err(AgentActionServiceError::Corrupt("member authority preparation or owner mismatch".into()));
        }
        if !authority.matches_staged_authorization(authorization) {
            return Err(AgentActionServiceError::MemberUnknown("original member staged authorization unavailable or mismatched".into()));
        }
        // The private original producer, not caller payload, owns both sides
        // of this same invocation's hook and its permission context.
        let (kind, original, effective, hook_raw, permission_context, outer_request, step) =
            if let Some(host) = member_guard.host() {
                ("host", host.original_prepared(), host.prepared(), host.hook_effective_input(),
                    host.permission_context(), host.original_request(), host.step())
            } else if let Some(process) = member_guard.process() {
                let facts = process.original_hook_facts().ok_or_else(|| {
                    AgentActionServiceError::MemberUnknown("original Process member hook facts unavailable".into())
                })?;
                if facts.validation_error().is_some() || !facts.owner_live_after_hook() || !facts.authorization_compatible() {
            return Err(AgentActionServiceError::MemberUnknown("original Process member hook is not admissible".into()));
                }
                let effective = facts.effective().ok_or_else(|| {
                    AgentActionServiceError::MemberUnknown("original Process effective member unavailable".into())
                })?;
                ("process", facts.original(), effective, facts.hook_effective_input(), facts.context(),
                    process.original_request(), process.step())
            } else {
            return Err(AgentActionServiceError::MemberUnknown("original member hook owner unavailable".into()));
            };
        if original.outer_key != prepared.outer_key || original.index != prepared.index
            || original.tool != prepared.tool || effective != prepared {
            return Err(AgentActionServiceError::Corrupt("member original hook identity mismatch".into()));
        }
        let outer_hook_facts = if kind == "host" {
            let hook = outer_hook.ok_or_else(|| {
                AgentActionServiceError::MemberUnknown("original Host outer hook owner unavailable".into())
            })?;
            hook.publication_facts(ticket, outer_request).map_err(|error| {
                AgentActionServiceError::MemberUnknown(error)
            })?
        } else {
            if outer_hook.is_some() { return Err(AgentActionServiceError::Corrupt("Process member cannot use a Host outer hook".into())); }
            let process = member_guard.process().ok_or_else(|| {
                AgentActionServiceError::MemberUnknown("original Process owner unavailable".into())
            })?;
            let facts = process.owner().outer_input_facts().ok_or_else(|| {
                AgentActionServiceError::MemberUnknown("original Process outer input facts unavailable".into())
            })?;
            if facts.effective_input() != outer_request.input || outer_request.compiled_invocation.as_ref() != Some(facts.invocation()) {
            return Err(AgentActionServiceError::Corrupt("original Process outer input differs".into()));
            }
            json!({"owner":"process", "model_input":facts.model_input(), "effective_input":facts.effective_input(), "invocation":facts.invocation()})
        };
        let hook_input = harness_contract::step::parse_strict_json(hook_raw)
            .map_err(|error| {
                AgentActionServiceError::Corrupt(error.to_string())
            })?;
        let decision = permission_context.override_decision().map(|value| match value {
            crate::permissions::PermissionOverride::Allow => "allow",
            crate::permissions::PermissionOverride::Deny => "deny",
            crate::permissions::PermissionOverride::Ask => "ask",
        });
        let original_hook = json!({"owner":kind, "original_prepared":original,
            "effective_prepared":effective, "hook_effective_input":hook_input,
            "permission_context":{"override_decision":decision,"override_reason":permission_context.override_reason()},
            "outer_request":outer_request, "step":step});
        let requires_goal = matches!(envelope.action, AgentAction::ObjectiveUpdate(_) | AgentAction::ObjectiveReview(_));
        let requires_review = matches!(envelope.action, AgentAction::TaskReview(_))
            || matches!(&envelope.action, AgentAction::ObjectiveReview(input)
                if input.decision == harness_contract::agent_action::ObjectiveReviewDecision::Satisfied);
        if requires_goal != goal.is_some() || (requires_review && verification.is_none())
            || envelope.actor.session_id != claim.session_id() || envelope.actor.turn_id != claim.turn_id()
            || envelope.actor.original_execution.as_ref().is_none_or(|physical|
                physical.graph_id != ticket.graph_id || physical.node_id != ticket.node_id || physical.attempt != ticket.attempt)
            || authorization.authorization_lease.capability != prepared.tool
        {
            return Err(AgentActionServiceError::Corrupt("member original preparation or owner mismatch".into()));
        }
        // Original events, when present, remain independently fenced. New
        // lease/approval transitions are authored by this checked transaction
        // and therefore cannot be required as already-committed events here.
        if original_events.iter().any(|event|
            !sources.iter().any(|source| source.stream_id == event.stream_id
                && source.expected_revision >= event.sequence)) {
            return Err(AgentActionServiceError::Corrupt("member original event source is not fenced".into()));
        }
        let facts = json!({"prepared":prepared, "authorization":authorization,
            "ticket":ticket, "original_events":original_events, "original_hook":original_hook, "outer_hook":outer_hook_facts});
        let captured = std::cell::RefCell::new(None);
        let context = OriginalActionCommit { sources, locks, control, claim, consumed_input_sequence,
            single_call_facts: None, member_call_facts: Some(facts), authority: Some(authority), reservation: Some(reservation), effect_fence: Some(effect_fence), committed_capture: Some(&captured) };
        let result = self.apply_original_action_locked(envelope, goal, verification, required_write_failures, &context);
        drop(context);
        let result = (captured.into_inner(), result);
        match result {
            (Some(append), Ok(observation)) => Ok(OriginalMemberSubmission::Committed(Arc::new(
                OriginalCommittedMember { append, observation: Some(Arc::new(observation)), observation_error: None }
            ))),
            (Some(append), Err(error)) => Ok(OriginalMemberSubmission::Committed(Arc::new(
                OriginalCommittedMember { append, observation: None, observation_error: Some(error.to_string()) }
            ))),
            (None, Ok(observation)) => Ok(OriginalMemberSubmission::NoCommittedReceipt(observation)),
            (None, Err(error)) => Err(error),
        }
    }

    /// Recover an already committed action for the exact original call before
    /// trying to consume its grant again. `None` means unobserved, not that a
    /// previous attempt rolled back; a missing or mismatched receipt is an error.
    pub(super) fn replay_single_original(
        &self,
        witness: &crate::conversation::tool_plane::OriginalRootReplayWitness<'_>,
    ) -> Result<OriginalReplayedAction, AgentActionServiceError> {
        let envelope = witness.original_envelope();
        witness.validate_envelope(envelope, witness.tool(), witness.input())
            .map_err(|error| AgentActionServiceError::Corrupt(error.into()))?;
        // The private witness owner already selected and authenticated this
        // exact durable action; do not query that same event a second time.
        self.verify_original_single_replay(envelope, witness.original_request(), witness.ticket())?;
        let event = witness.original_action_event();
        let observation = match event.payload.get("original_action_observation") {
            Some(value) => {
                let observation: AgentActionObservation = serde_json::from_value(value.clone())
                    .map_err(|error| AgentActionServiceError::MemberUnknown(format!("invalid original action observation: {error}")))?;
                if observation.action_id != envelope.action_id || observation.program_id != envelope.actor.program_id
                    || observation.action != envelope.action.kind() || observation.status != AgentActionStatus::Applied {
                    return Err(AgentActionServiceError::MemberUnknown("original action observation identity mismatch".into()));
                }
                Some(observation)
            }
            None => None,
        };
        let receipt = witness.original_action_receipt();
        if receipt.transaction_id != event.transaction_id || receipt.commit_cursor != event.commit_cursor
            || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id) {
            return Err(AgentActionServiceError::MemberUnknown("original action receipt differs".into()));
        }
        let root = envelope.actor.root_execution_id.as_deref().ok_or_else(||
            AgentActionServiceError::MemberUnknown("original action Goal owner missing".into()))?;
        let goal_stream = format!("goal:goal:{root}");
        let goal_sources = receipt.stream_revisions.iter().filter(|source| source.stream_id == goal_stream
            && source.committed_revision > source.expected_revision).collect::<Vec<_>>();
        if goal_sources.len() > 1 { return Err(AgentActionServiceError::MemberUnknown("original Goal source ambiguous".into())); }
        let goal_event = if let Some(source) = goal_sources.first() {
            if source.expected_revision.checked_add(1) != Some(source.committed_revision) {
                return Err(AgentActionServiceError::MemberUnknown("original Goal transaction is ambiguous".into()));
            }
            let mut events = self.store.list_stream_before(&goal_stream, source.committed_revision, 1, 4 * 1024 * 1024)
                .map_err(AgentActionServiceError::EventStore)?;
            let goal = events.pop().ok_or_else(|| AgentActionServiceError::MemberUnknown("original Goal event unavailable".into()))?;
            if goal.sequence != source.committed_revision || goal.transaction_id != event.transaction_id
                || goal.commit_cursor != event.commit_cursor || goal.scope != RuntimeEventScope::Goal
                || receipt.event_ids.get(goal.transaction_index as usize) != Some(&goal.event_id)
                || goal.payload["action_entity_ref"] != event.payload["entity_ref"] {
                return Err(AgentActionServiceError::MemberUnknown("original Goal event differs".into()));
            }
            Some(goal)
        } else { None };
        Ok(OriginalReplayedAction { store: Arc::downgrade(&self.store), ticket: witness.ticket().clone(),
            call_id: witness.call_id().to_owned(), observation, program_event: event.clone(), goal_event, receipt: receipt.clone() })
    }

    /// Authenticate the original same-call facts from the action transaction;
    /// caller-supplied ticket or request alone can never establish replay.
    fn verify_original_single_replay(
        &self,
        envelope: &AgentActionEnvelope,
        request: &crate::RuntimeToolExecutionRequest,
        ticket: &crate::execution_core::graph::NodeExecutionTicket,
    ) -> Result<(), AgentActionServiceError> {
        let missing = || AgentActionServiceError::Corrupt("original single action replay facts unavailable or mismatched".into());
        let event = self.store.event_by_idempotency_key(&program_stream(&envelope.actor.program_id), &action_key(&envelope.action_id))?
            .ok_or_else(missing)?;
        let facts = event.payload.get("original_single_call").ok_or_else(missing)?;
        if facts.get("ticket") != Some(&serde_json::to_value(ticket)?)
            || facts.get("request") != Some(&serde_json::to_value(request)?) {
            return Err(missing());
        }
        let references = facts.get("authorization_events").and_then(serde_json::Value::as_array).ok_or_else(missing)?;
        let auth = request.authorization.as_ref().ok_or_else(missing)?;
        let mut assessed = false;
        let mut consumed = false;
        let mut original_events = Vec::with_capacity(references.len());
        for reference in references {
            let stream = reference.get("stream_id").and_then(serde_json::Value::as_str).ok_or_else(missing)?;
            let sequence = reference.get("sequence").and_then(serde_json::Value::as_u64).ok_or_else(missing)?;
            let expected = self.store.transaction_stream_revision_exact(&event.transaction_id, stream)?
                .ok_or_else(missing)?;
            if expected.expected_revision < sequence || expected.expected_revision != expected.committed_revision {
                return Err(missing());
            }
            let mut originals = self.store.list_stream_before(stream, sequence, 1, 1024 * 1024)
                .map_err(AgentActionServiceError::EventStore)?;
            let original = originals.pop().ok_or_else(missing)?;
            if original.sequence != sequence
                || reference.get("event_id").and_then(serde_json::Value::as_str) != Some(original.event_id.as_str())
                || reference.get("transaction_id").and_then(serde_json::Value::as_str) != Some(original.transaction_id.as_str())
                || reference.get("transaction_index").and_then(serde_json::Value::as_u64) != Some(original.transaction_index as u64)
                || reference.get("commit_cursor").and_then(serde_json::Value::as_u64) != Some(original.commit_cursor) {
                return Err(missing());
            }
            let receipt = self.store.original_transaction_receipt(&original.transaction_id)?.ok_or_else(missing)?;
            if receipt.transaction_id != original.transaction_id || receipt.commit_cursor != original.commit_cursor
                || receipt.event_ids.get(original.transaction_index as usize) != Some(&original.event_id) {
                return Err(missing());
            }
            if original.payload.pointer("/lease/lease_id").and_then(serde_json::Value::as_str)
                == Some(auth.authorization_lease.lease_id.as_str()) {
                assessed |= original.kind == "authorization.capability_assessed";
                consumed |= original.kind == "authorization.lease_transition" && original.status.as_deref() == Some("consumed");
            }
            original_events.push(original);
        }
        if !assessed || !consumed { return Err(missing()); }
        if let Some(coordinate) = self.original_completion_authorization_coordinate(envelope, request, ticket, &original_events, None)? {
            if facts.get("call_authorized") != Some(&coordinate) { return Err(missing()); }
        }
        Ok(())
    }

    /// Submit one original action while its original policy, cancellation,
    /// Session claim, and the complete source lock set are held by the owner.
    /// The owner must authenticate the exact assessment and lease events under
    /// those locks before invoking this function; this is not a signer.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_single_locked(
        &self,
        envelope: &AgentActionEnvelope,
        goal: Option<(String, u64, RuntimeTransactionEventInput)>,
        verification: Option<&ObjectiveReviewVerification>,
        required_write_failures: &[serde_json::Value],
        invocation: &super::action_invocation::VerifiedActionInvocation,
        tool: &str,
        input: &serde_json::Value,
        original: &crate::conversation::tool_plane::OriginalSingleCheckedInputs<'_>,
    ) -> Result<OriginalSingleSubmission, AgentActionServiceError> {
        let request = original.request();
        let ticket = original.ticket();
        let original_events = original.original_events();
        let original_decision = original.original_decision();
        let derived_read = original.read_ref_target().is_some_and(|target|
            request.tool_name == "read_ref" && tool == "state_inspect"
                && (original.is_delegated() || target.ticket == *ticket) && target.input == *input);
        let sources = original.sources();
        let locks = original.held();
        let control = original.control();
        let claim = original.claim();
        let consumed_input_sequence = original.consumed_input_sequence();
        invocation.validate_request(request).map_err(|error| AgentActionServiceError::Corrupt(error.into()))?;
        invocation.validate_envelope(envelope, tool, input).map_err(|error| AgentActionServiceError::Corrupt(error.into()))?;
        let requires_goal = matches!(envelope.action, AgentAction::ObjectiveUpdate(_) | AgentAction::ObjectiveReview(_));
        let requires_review = matches!(envelope.action, AgentAction::TaskReview(_))
            || matches!(&envelope.action, AgentAction::ObjectiveReview(input)
                if input.decision == harness_contract::agent_action::ObjectiveReviewDecision::Satisfied);
        if requires_goal != goal.is_some() || (requires_review && verification.is_none())
            || envelope.actor.session_id != claim.session_id() || envelope.actor.turn_id != claim.turn_id()
            || envelope.actor.original_execution.as_ref().is_none_or(|physical|
                physical.graph_id != ticket.graph_id || physical.node_id != ticket.node_id || physical.attempt != ticket.attempt)
            || (request.tool_name != tool && !derived_read) || request.session_id.as_deref() != Some(claim.session_id())
            || request.execution_decision.as_ref() != original_decision
            || (invocation.root_physical_owner().is_some() == original.is_delegated())
            || !sources.iter().any(|source| source.stream_id == ticket.graph_id)
        {
            return Err(AgentActionServiceError::Corrupt("single action original preparation or owner mismatch".into()));
        }
        crate::execution_core::graph::validate_node_ticket_start(&self.store, ticket, None)
            .map_err(|error| AgentActionServiceError::Corrupt(error.to_string()))?;
        let authorization = request.authorization.as_ref()
            .ok_or_else(|| AgentActionServiceError::Corrupt("single action original authorization missing".into()))?;
        let lease_id = &authorization.authorization_lease.lease_id;
        let mut assessed = false;
        let mut consumed = false;
        let mut event_refs = Vec::with_capacity(original_events.len());
        for event in original_events {
            if !sources.iter().any(|source| source.stream_id == event.stream_id && source.expected_revision >= event.sequence) {
                return Err(AgentActionServiceError::Corrupt("single action authorization source not locked".into()));
            }
            // The sealed original owner already authenticated these immutable
            // events and their store; do not read its own appends again.
            if event.payload.pointer("/lease/lease_id").and_then(serde_json::Value::as_str) == Some(lease_id.as_str()) {
                assessed |= event.kind == "authorization.capability_assessed";
                consumed |= event.kind == "authorization.lease_transition" && event.status.as_deref() == Some("consumed");
            }
            event_refs.push(json!({"stream_id":event.stream_id, "sequence":event.sequence,
                "event_id":event.event_id, "transaction_id":event.transaction_id,
                "transaction_index":event.transaction_index, "commit_cursor":event.commit_cursor}));
        }
        if !assessed || !consumed {
            return Err(AgentActionServiceError::Corrupt("single action original assessment or consumption missing".into()));
        }
        let mut facts = json!({"ticket":ticket, "request":request, "authorization_events":event_refs});
        if derived_read {
            let target = original.read_ref_target().ok_or_else(|| AgentActionServiceError::Corrupt("original read_ref target missing".into()))?;
            facts["read_ref_target"] = json!({"input":target.input, "effect":target.effect, "authorization":target.authorization});
        }
        if !original.is_delegated() && matches!(envelope.action, AgentAction::ObjectiveCompleteRequest(_)) {
            // The sealed Root owner already authenticated this unique original
            // producer event under the source locks. Retain its coordinates,
            // not a second assessment or a reconstructed authorization.
            let mut events = original_events.iter().filter(|event| event.kind == "authorization.call_authorized");
            let event = events.next().ok_or_else(|| AgentActionServiceError::Corrupt(
                "original Root completion authorization unavailable".into()))?;
            if events.next().is_some() { return Err(AgentActionServiceError::Corrupt(
                "original Root completion authorization ambiguous".into())); }
            facts["call_authorized"] = json!({"stream_id":event.stream_id, "sequence":event.sequence,
                "event_id":event.event_id, "transaction_id":event.transaction_id,
                "transaction_index":event.transaction_index, "commit_cursor":event.commit_cursor});
        }
        let captured = std::cell::RefCell::new(None);
        let context = OriginalActionCommit { sources, locks, control, claim, consumed_input_sequence, single_call_facts: Some(facts), member_call_facts: None, authority: None, reservation: None, effect_fence: None, committed_capture: Some(&captured) };
        let observation = self.apply_original_action_locked(envelope, goal, verification, required_write_failures, &context);
        let committed = captured.into_inner().map(|append| Arc::new(OriginalCommittedSingle {
            store: append.store, request: append.request, receipt: append.receipt,
            observation: observation.as_ref().ok().map(|value| Arc::new(value.clone())),
            observation_error: observation.as_ref().err().map(ToString::to_string),
        }));
        Ok(OriginalSingleSubmission { observation, committed })
    }

    /// Link the original completion authorization producer to the exact
    /// assessment and consume events; this does not replace the owner's
    /// policy, decision, or locked source authentication.
    fn original_completion_authorization_coordinate(
        &self,
        envelope: &AgentActionEnvelope,
        request: &crate::RuntimeToolExecutionRequest,
        ticket: &crate::execution_core::graph::NodeExecutionTicket,
        original_events: &[crate::DurableRuntimeEvent],
        original_decision: Option<&crate::execution_core::RuntimeExecutionDecision>,
    ) -> Result<Option<serde_json::Value>, AgentActionServiceError> {
        let authorization = request.authorization.as_ref()
            .ok_or_else(|| AgentActionServiceError::Corrupt("single action original authorization missing".into()))?;
        if matches!(envelope.action, AgentAction::ObjectiveCompleteRequest(_)) {
            let missing = || AgentActionServiceError::Corrupt("original completion authorization fact is missing or mismatched".into());
            let mut authorizations = original_events.iter().filter(|event| event.kind == "authorization.call_authorized");
            let original = authorizations.next().ok_or_else(missing)?;
            if authorizations.next().is_some() || original.stream_id != format!("session:{}", envelope.actor.session_id)
                || original.scope != RuntimeEventScope::Tool || original.status.as_deref() != Some("authorized")
                || original.actor.as_deref() != Some("conversation_runtime") || original.schema_version != 1
                || original.payload["version"] != 1 || original.payload["authorization"] != serde_json::to_value(authorization)? {
                return Err(missing());
            }
            let binding = &original.payload["binding"];
            if binding["version"] != 1 || binding["owner"]["kind"] != "node"
                || binding["owner"]["ticket"] != serde_json::to_value(ticket)?
                || binding["step_binding"] != serde_json::to_value(&request.step_binding)?
                || binding["call_id"] != request.tool_use_id || binding["tool_name"] != request.tool_name
                || binding["effect_key"] != request.idempotency_key
                || binding["input_sha256"] != format!("sha256:{:x}", Sha256::digest(request.input.as_bytes()))
            { return Err(missing()); }
            let mut assessments = original_events.iter().filter(|event| event.kind == "authorization.capability_assessed"
                && event.payload.get("_call_refusal_binding") == Some(binding));
            let assessment = assessments.next().ok_or_else(missing)?;
            if assessments.next().is_some() || assessment.payload.get("_original_permission_context").is_none()
                || assessment.stream_id != format!("session:{}", envelope.actor.session_id)
                || assessment.scope != RuntimeEventScope::Tool || assessment.actor.as_deref() != Some("conversation_runtime")
                || assessment.schema_version != 1 {
                return Err(missing());
            }
            let evaluated: harness_contract::policy::CapabilityAssessment = serde_json::from_value(assessment.payload.clone())?;
            if evaluated.capability != request.tool_name || evaluated.lease.as_ref() != Some(&authorization.authorization_lease)
                || evaluated.gap.is_some() {
                return Err(missing());
            }
            // The assessment binds the complete pre-authorization request.
            // A changed execution_decision needs its own original-owner proof;
            // until available, do not infer it from the eventual request.
            let mut before: crate::RuntimeToolExecutionRequest = serde_json::from_value(binding["request"].clone())?;
            if before.authorization.is_some() { return Err(missing()); }
            before.authorization = Some(authorization.clone());
            if let Some(decision) = original_decision {
                if request.execution_decision.as_ref() != Some(decision) { return Err(missing()); }
                before.execution_decision = Some(decision.clone());
            }
            // Compare every persisted request field. The callback sink is
            // intentionally not serialized; its live owner was checked by
            // `validate_request` and must not be reconstructed from evidence.
            if serde_json::to_value(&before)? != serde_json::to_value(request)? { return Err(missing()); }
            let coordinates = |event: &crate::DurableRuntimeEvent| json!({
                "stream_id":event.stream_id, "sequence":event.sequence, "event_id":event.event_id,
                "transaction_id":event.transaction_id, "transaction_index":event.transaction_index,
                "commit_cursor":event.commit_cursor,
            });
            if original.payload["assessment"] != coordinates(assessment) { return Err(missing()); }
            let mut consumptions = original_events.iter().filter(|event| event.kind == "authorization.lease_transition"
                && event.status.as_deref() == Some("consumed") && original.payload["consume"] == coordinates(event));
            let consumption = consumptions.next().ok_or_else(missing)?;
            if consumptions.next().is_some() || assessment.commit_cursor > original.commit_cursor
                || consumption.commit_cursor > original.commit_cursor
                || consumption.stream_id != format!("authorization-lease:{}", authorization.authorization_lease.lease_id)
                || consumption.scope != RuntimeEventScope::Tool || consumption.actor.as_deref() != Some("conversation_runtime")
                || consumption.schema_version != 1 || consumption.transaction_index != 0 {
                return Err(missing());
            }
            let transition: harness_contract::policy::AuthorizationLeaseTransition = serde_json::from_value(consumption.payload.clone())?;
            if transition.kind != harness_contract::policy::AuthorizationLeaseTransitionKind::Consumed
                || transition.idempotency_key.as_deref() != Some(authorization.request_id.as_str())
                || consumption.idempotency_key.as_deref() != Some(transition.transition_id.as_str())
                || consumption.transaction_id != format!("authorization-transition:{}:{}", consumption.stream_id, transition.transition_id)
                || !crate::authorization_negotiator::consumed_transition_matches_original_execution(&transition, authorization) {
                return Err(missing());
            }
            return Ok(Some(coordinates(original)));
        }
        Ok(None)
    }

    /// Shared transaction owner for an already authenticated original call.
    /// The caller holds the original policy and the complete ordered stream
    /// lock set for the entire synchronous call. This method must not be used
    /// to authenticate an invocation or to construct an owner from payloads.
    fn apply_original_action_locked(
        &self,
        envelope: &AgentActionEnvelope,
        goal: Option<(String, u64, RuntimeTransactionEventInput)>,
        verification: Option<&ObjectiveReviewVerification>,
        required_write_failures: &[serde_json::Value],
        context: &OriginalActionCommit<'_>,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        self.apply_locked(envelope, &program_stream(&envelope.actor.program_id), goal, verification,
            required_write_failures, context.sources, false, false, Some(context), None)
    }

    /// Locate a committed member from its original outer effect and ordinal.
    /// This index is appended in the same original transaction as the Program
    /// event; it grants no authority and its reader must authenticate both
    /// events and their shared transaction receipt.
    /// The effect-stream twin of one rejection fact.
    ///
    /// The member shape and the refused-call shape are the same fact: the
    /// kind, scope, actor, status, schema and payload fields are identical,
    /// so one consumer reads one shape. Only the key the twin is written
    /// under, and the member coordinates it carries, differ.
    fn rejection_index_event(
        program_id: &str,
        twin: &RejectionTwin,
        program_event_index: u32,
        program_event_kind: &str,
        program_event_key: &str,
    ) -> RuntimeTransactionEventInput {
        RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: format!("execution-effect:{}", twin.outer_key),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "agentic.batch_member.index".into(),
                status: Some("committed".into()),
                actor: Some("runtime.agentic_action".into()),
                refs: vec![RuntimeEventRef { kind: "agentic_program".into(), id: program_id.to_string() }],
                payload: json!({
                    "outer_key": twin.outer_key, "member_index": twin.member_index,
                    "member_identity": twin.member_identity, "ticket": twin.ticket,
                    "program_stream": program_stream(program_id),
                    "program_event_kind": program_event_kind, "program_event_key": program_event_key,
                    "program_event_index": program_event_index,
                }),
            },
            idempotency_key: Some(twin.idempotency_key.clone()),
            schema_version: 1,
        }
    }

    /// The member's own effect-stream twin, when this fact is a member's.
    ///
    /// The twin is written on the member batch's outer effect stream, and
    /// the outer source must be one this owner already holds locked.
    fn original_member_twin(
        context: Option<&OriginalActionCommit<'_>>,
    ) -> Result<Option<RejectionTwin>, AgentActionServiceError> {
        let Some(facts) = context.and_then(|owner| owner.member_call_facts.as_ref()) else { return Ok(None); };
        let owner = context.ok_or_else(|| AgentActionServiceError::Corrupt("member owner missing".into()))?;
        let prepared: crate::step_snapshot::PreparedAgentBatchMember = serde_json::from_value(
            facts.get("prepared").cloned().ok_or_else(|| {
                AgentActionServiceError::Corrupt("member preparation missing".into())
            })?
        ).map_err(|error| {
            error
        })?;
        if prepared.outer_key.is_empty() || prepared.identity.is_empty() {
            return Err(AgentActionServiceError::Corrupt("member index identity missing".into()));
        }
        let stream_id = format!("execution-effect:{}", prepared.outer_key);
        if !owner.sources.iter().any(|source| source.stream_id == stream_id) {
            return Err(AgentActionServiceError::Corrupt("member original outer effect source is not locked".into()));
        }
        Ok(Some(RejectionTwin {
            outer_key: prepared.outer_key.clone(),
            idempotency_key: format!("agentic-member:{}", prepared.index),
            member_index: Some(prepared.index),
            member_identity: Some(prepared.identity.clone()),
            ticket: facts["ticket"].clone(),
        }))
    }

    /// The effect-stream twin for a single call's own rejection.
    ///
    /// A single Agent action is refused under its own effect key, so its
    /// twin belongs on that call's own stream. The twin exists only when
    /// the call carries its own admission intent: a fact nobody can read
    /// is not written.
    fn single_call_rejection_twin(
        &self,
        member: Option<&OriginalActionCommit<'_>>,
        action_id: &str,
    ) -> Result<Option<RejectionTwin>, AgentActionServiceError> {
        let Some(context) = member else { return Ok(None); };
        let Some(facts) = context.single_call_facts.as_ref() else { return Ok(None); };
        if self.authenticate_call_admission(action_id, None, None)?.is_none() {
            return Ok(None);
        }
        Ok(Some(RejectionTwin {
            outer_key: action_id.to_string(),
            idempotency_key: format!("agentic-single:{}", action_id),
            member_index: None,
            member_identity: None,
            ticket: facts["ticket"].clone(),
        }))
    }

    fn original_member_index_event(
        envelope: &AgentActionEnvelope,
        context: Option<&OriginalActionCommit<'_>>,
        program_event_index: usize,
        program_event_kind: &str,
        program_event_key: &str,
    ) -> Result<Option<RuntimeTransactionEventInput>, AgentActionServiceError> {
        let Some(twin) = Self::original_member_twin(context)? else { return Ok(None); };
        let program_event_index = u32::try_from(program_event_index)
            .map_err(|_| {
                AgentActionServiceError::Corrupt("member program event index overflow".into())
            })?;
        Ok(Some(Self::rejection_index_event(
            &envelope.actor.program_id,
            &twin,
            program_event_index,
            program_event_kind,
            program_event_key,
        )))
    }

    fn append_original_action_transaction(
        &self, mut transaction: AppendTransactionRequest, member: Option<&OriginalActionCommit<'_>>,
    ) -> Result<(), AgentActionServiceError> {
        let Some(member) = member else {
            self.store.append_transaction_locked(transaction)?;
            return Ok(());
        };
        for source in member.sources {
            if let Some(existing) = transaction.expected_streams.iter().find(|item| item.stream_id == source.stream_id) {
                if existing != source {
                    return Err(AgentActionServiceError::Corrupt("member transaction source mismatch".into()));
                }
            } else {
                transaction.expected_streams.push(source.clone());
            }
        }
        if let Some(authority) = &member.authority {
            for source in authority.sources() {
                if let Some(existing) = transaction.expected_streams.iter().find(|item| item.stream_id == source.stream_id) {
                    if existing != source { return Err(AgentActionServiceError::Corrupt("member authority source mismatch".into())); }
                } else { transaction.expected_streams.push(source.clone()); }
            }
            let mut indices = Vec::new();
            for event in authority.events() {
                let position = transaction.events.len();
                if event.idempotency_key.is_none() || !transaction.expected_streams.iter().any(|source| source.stream_id == event.event.stream_id) {
                    return Err(AgentActionServiceError::Corrupt("member authority event source missing".into()));
                }
                indices.push(json!({"stream_id":event.event.stream_id, "kind":event.event.kind,
                    "idempotency_key":event.idempotency_key, "transaction_index":position}));
                transaction.events.push(event.clone());
            }
            if indices.is_empty() {
                    return Err(AgentActionServiceError::Corrupt("member authority events missing".into()));
            }
            let mut markers = transaction.events.iter_mut().filter(|event| event.event.kind == "agentic.batch_member.index");
            let marker = markers.next().ok_or_else(|| {
                AgentActionServiceError::Corrupt("member index missing for authority".into())
            })?;
            marker.event.payload["authority_events"] = json!(indices);
            if markers.next().is_some() { return Err(AgentActionServiceError::Corrupt("member index ambiguous".into())); }
        }
        transaction.expected_streams.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
        let digest = super::review_evidence::proof_value_digest(&transaction);
        let validator = StrictOriginalActionSources { digest: digest.clone(), authority_deadlines: member.authority.as_ref().map(|a| a.deadlines()) };
        let fence = member.claim.fence().map_err(|error| {
            AgentActionServiceError::Corrupt(error)
        })?;
        let claim = crate::RuntimeOriginalSessionClaim {
            session_id: member.claim.session_id(), ingress_message_id: member.claim.message_id(),
            turn_id: member.claim.turn_id(), fence: &fence,
            consumed_input_sequence: member.consumed_input_sequence,
        };
        let request = crate::CheckedAppendRequest { transaction, frame_digest: digest,
            budget: crate::SourceCheckBudget::default() };
        match self.store.append_checked_transaction_cancellable_locked(
            request, &validator, Arc::clone(&member.control), &claim, member.locks, member.authority.clone(), member.reservation, member.effect_fence, None,
        ) {
            Ok(Some(committed)) => {
                if let Some(slot) = member.committed_capture {
                    let (request, receipt, authority) = committed.into_parts();
                    *slot.borrow_mut() = Some(CommittedActionAppend {
                        store: Arc::downgrade(&self.store), request,
                        receipt: OriginalMemberCommitReceipt { commit: receipt.commit, validation: Some(receipt.validation) }, authority,
                    });
                }
                Ok(())
            },
            Ok(None) => {
                Err(AgentActionServiceError::MemberCancelled)
            },
            Err(RuntimeEventStoreError::CheckedAttemptRolledBack(cause)) => {
                Err(AgentActionServiceError::Store(*cause))
            },
            Err(error) => {
                Err(AgentActionServiceError::MemberUnknown(error.to_string()))
            },
        }
    }

    pub(crate) fn apply(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        self.apply_with_root_owner(envelope, false, false, None)
    }

    pub(super) fn apply_runtime_root_completion(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if envelope.actor.kind != AgentActorKind::Root
            || !matches!(&envelope.action, AgentAction::ObjectiveCompleteRequest(_))
        {
            return Err(AgentActionServiceError::Corrupt("invalid Runtime root completion".into()));
        }
        self.apply_with_root_owner(envelope, true, false, None)
    }

    pub(super) fn apply_runtime_root_completion_owned(
        &self, envelope: &AgentActionEnvelope,
        owner: &crate::execution_core::services::OriginalGraphRegistrationOwner,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if envelope.actor.kind != AgentActorKind::Root
            || !matches!(&envelope.action, AgentAction::ObjectiveCompleteRequest(_))
        { return Err(AgentActionServiceError::Corrupt("invalid Runtime root completion".into())); }
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let stream = program_stream(&envelope.actor.program_id);
        let mut streams = vec![stream.clone()];
        if let Some(physical) = envelope.actor.original_execution.as_ref() { streams.push(physical.graph_id.clone()); }
        owner.with_action(|claim, control, consumed| {
            Ok(self.store.with_authenticated_stream_locks(&streams, |locks| {
                let context = OriginalActionCommit { sources: &[], locks, control, claim,
                    consumed_input_sequence: consumed, single_call_facts: None, member_call_facts: None,
                    authority: None, reservation: None, effect_fence: None, committed_capture: None };
                self.apply_locked(envelope, &stream, None, None, &[], context.sources, true, false, Some(&context), None)
            }))
        }).map_err(AgentActionServiceError::EventStore)?
    }

    pub(super) fn apply_runtime_supervisor(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if envelope.actor.kind != AgentActorKind::Supervisor || envelope.actor.original_execution.is_some() {
            return Err(AgentActionServiceError::Corrupt("invalid Runtime supervisor action".into()));
        }
        self.apply_with_root_owner(envelope, false, true, None)
    }

    pub(super) fn apply_runtime_supervisor_review_cut(
        &self,
        envelope: &AgentActionEnvelope,
        origin: &super::ingress::ReviewCutSettlement,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        origin.event_metadata(envelope).map_err(|error| AgentActionServiceError::Corrupt(error.into()))?;
        self.apply_with_root_owner(envelope, false, true, Some(origin))
    }

    /// Commit a new supervisor dispatch under the original Session owner.
    pub(super) fn apply_runtime_supervisor_dispatch_owned(
        &self,
        envelope: &AgentActionEnvelope,
        owner: &crate::execution_core::services::OriginalConclusionOwner,
        consumed_input_sequence: usize,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if envelope.actor.kind != AgentActorKind::Supervisor
            || envelope.actor.original_execution.is_some()
            || !matches!(&envelope.action, AgentAction::TaskAttemptDispatch(_))
            || owner.claim.session_id() != envelope.actor.session_id
            || owner.claim.turn_id() != envelope.actor.turn_id
        {
            return Err(AgentActionServiceError::Corrupt("invalid original supervisor dispatch owner".into()));
        }
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let stream_id = program_stream(&envelope.actor.program_id);
        let streams = vec![stream_id.clone()];
        self.store.with_authenticated_stream_locks(&streams, |locks| {
            let context = OriginalActionCommit {
                sources: &[], locks, control: Arc::clone(&owner.control),
                claim: &owner.claim, consumed_input_sequence,
                single_call_facts: None, member_call_facts: None, authority: None,
                reservation: None, effect_fence: None, committed_capture: None,
            };
            self.apply_locked(envelope, &stream_id, None, None, &[], context.sources, false, true, Some(&context), None)
        })
    }

    fn apply_with_root_owner(
        &self,
        envelope: &AgentActionEnvelope,
        internal_root_owner: bool,
        internal_supervisor: bool,
        review_cut: Option<&super::ingress::ReviewCutSettlement>,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![stream_id.clone()];
        if matches!(&envelope.action, AgentAction::TeamCreate(_))
            || matches!(&envelope.action, AgentAction::TaskPublish(input) if !input.obligation_refs.is_empty())
        {
            if let Some(root) = &envelope.actor.root_execution_id {
                streams.push(format!("goal:goal:{root}"));
            }
        }
        // Gather the original graph with the Program before taking ordered locks.
        if let Some(physical) = envelope.actor.original_execution.as_ref() {
            streams.push(physical.graph_id.clone());
        }
        if let Some(origin) = review_cut {
            streams.push(origin.graph_id().to_string());
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(envelope, &stream_id, None, None, &[], &[], internal_root_owner, internal_supervisor, None, review_cut)
        })
    }

    /// Commit one Program action and its prepared Goal revision atomically.
    /// The Goal mutation is prepared from durable state before acquiring the
    /// ordered two-stream lock; expected revisions make a stale preparation a
    /// whole-transaction conflict rather than a partial Objective update.
    pub(crate) fn apply_with_goal_event(
        &self,
        envelope: &AgentActionEnvelope,
        goal_stream_id: String,
        expected_goal_stream_revision: u64,
        goal_event: RuntimeTransactionEventInput,
        verification: Option<&ObjectiveReviewVerification>,
        required_write_failures: &[serde_json::Value],
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        self.apply_with_goal_event_sources_inner(
            envelope,
            goal_stream_id,
            expected_goal_stream_revision,
            goal_event,
            verification,
            required_write_failures,
            &[],
            false,
        )
    }

    pub(super) fn apply_with_goal_event_sources(
        &self,
        envelope: &AgentActionEnvelope,
        goal_stream_id: String,
        expected_goal_stream_revision: u64,
        goal_event: RuntimeTransactionEventInput,
        verification: Option<&ObjectiveReviewVerification>,
        required_write_failures: &[serde_json::Value],
        extra_sources: &[ExpectedStreamRevision],
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if envelope.actor.kind != AgentActorKind::Root
            || !matches!(&envelope.action, AgentAction::ObjectiveUpdate(_))
        {
            return Err(AgentActionServiceError::Corrupt("invalid authenticated user Goal revision".into()));
        }
        self.apply_with_goal_event_sources_inner(envelope, goal_stream_id, expected_goal_stream_revision,
            goal_event, verification, required_write_failures, extra_sources, true)
    }

    fn apply_with_goal_event_sources_inner(
        &self,
        envelope: &AgentActionEnvelope,
        goal_stream_id: String,
        expected_goal_stream_revision: u64,
        goal_event: RuntimeTransactionEventInput,
        verification: Option<&ObjectiveReviewVerification>,
        required_write_failures: &[serde_json::Value],
        extra_sources: &[ExpectedStreamRevision],
        internal_root_owner: bool,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        if let Err(error) = envelope.validate() {
            return Ok(rejected(envelope, 0, "invalid_action", &error.to_string()));
        }
        let program_stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![program_stream_id.clone(), goal_stream_id.clone()];
        streams.extend(extra_sources.iter().map(|s| s.stream_id.clone()));
        streams.extend(
            verification
                .and_then(|proof| proof.input_set.as_ref())
                .into_iter()
                .flat_map(|inputs| inputs.source_revisions.keys().cloned()),
        );
        streams.extend(
            verification
                .into_iter()
                .flat_map(|proof| proof.result_source_revisions.keys().cloned()),
        );
        if verification.is_some_and(|proof| {
            !proof.independence_required || proof.effect_manifest_digest.is_some()
        }) {
            streams.push(format!("session:{}", envelope.actor.session_id));
            streams.extend(
                verification
                    .into_iter()
                    .flat_map(|proof| proof.effect_source_refs.iter().cloned()),
            );
        }
        // Include the untrusted claimed graph in the ordered lock set before
        // loading it. apply_locked authenticates the physical owner while the
        // same lock is held and carries its revision into the transaction.
        if let Some(physical) = envelope.actor.original_execution.as_ref() {
            streams.push(physical.graph_id.clone());
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(
                envelope,
                &program_stream_id,
                Some((goal_stream_id, expected_goal_stream_revision, goal_event)),
                verification,
                required_write_failures,
                extra_sources,
                internal_root_owner,
                false,
                None,
                None,
            )
        })
    }

    pub(crate) fn replay_if_applied(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<Option<AgentActionObservation>, AgentActionServiceError> {
        let stream_id = program_stream(&envelope.actor.program_id);
        if let Some(existing) = self
            .store
            .event_by_idempotency_key(&stream_id, &action_key(&envelope.action_id))?
        {
            let original: AgentActionEnvelope = serde_json::from_value(
                existing.payload.get("envelope").cloned().ok_or_else(|| {
                    AgentActionServiceError::Corrupt(
                        "applied action has no durable envelope".into(),
                    )
                })?,
            )?;
            if existing.stream_id != stream_id || existing.scope != RuntimeEventScope::Program
                || existing.kind != ACTION_EVENT_KIND || existing.schema_version != 1
                || existing.status.as_deref() != Some("applied")
                || existing.idempotency_key.as_deref() != Some(action_key(&envelope.action_id).as_str())
                || original.action_id != envelope.action_id
                || original.actor.program_id != envelope.actor.program_id
                || existing.actor.as_deref() != Some(original.actor.actor_id.as_str())
            {
                return Err(AgentActionServiceError::Corrupt("applied action replay source mismatch".into()));
            }
            let receipt = self.store.original_transaction_receipt(&existing.transaction_id)?
                .ok_or_else(|| AgentActionServiceError::Corrupt("applied action original transaction receipt missing".into()))?;
            if receipt.transaction_id != existing.transaction_id
                || receipt.commit_cursor != existing.commit_cursor
                || receipt.event_ids.get(existing.transaction_index as usize) != Some(&existing.event_id)
                || receipt.stream_revisions.iter().filter(|source| source.stream_id == stream_id
                    && source.expected_revision < existing.sequence
                    && existing.sequence <= source.committed_revision).count() != 1
            {
                return Err(AgentActionServiceError::Corrupt("applied action original transaction receipt mismatch".into()));
            }
            if original != *envelope {
                return Err(AgentActionServiceError::Corrupt("action_id conflicts with original action".into()));
            }
            let entity = match existing.payload.get("entity_ref") {
                Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(value)) => Some(value.clone()),
                _ => return Err(AgentActionServiceError::MemberUnknown("original action entity unavailable".into())),
            };
            if let Some(value) = existing.payload.get("original_action_observation") {
                let observation: AgentActionObservation = serde_json::from_value(value.clone())
                    .map_err(|error| AgentActionServiceError::MemberUnknown(format!("invalid original action observation: {error}")))?;
                if observation.action_id != envelope.action_id || observation.program_id != envelope.actor.program_id
                    || observation.action != envelope.action.kind() || observation.status != AgentActionStatus::Applied
                    || observation.receipt_id != format!("receipt:{}:{}", envelope.actor.program_id, envelope.action_id)
                    || observation.changed_refs != entity.iter().cloned().collect::<Vec<_>>()
                {
                    return Err(AgentActionServiceError::MemberUnknown("original action observation receipt mismatch".into()));
                }
            }
            let derived = entity_ref(envelope);
            if derived.is_none() {
                let goal_stream = envelope.actor.root_execution_id.as_deref().map(|root| format!("goal:goal:{root}"));
                let sources: Vec<_> = receipt.stream_revisions.iter().filter(|source|
                    Some(&source.stream_id) == goal_stream.as_ref() && source.committed_revision > source.expected_revision).collect();
                if sources.len() > 1 || (sources.is_empty() && entity.is_some()) {
                    return Err(AgentActionServiceError::MemberUnknown("original Goal receipt unavailable".into()));
                }
                if let Some(source) = sources.first() {
                if source.expected_revision.checked_add(1) != Some(source.committed_revision) {
                    return Err(AgentActionServiceError::MemberUnknown("original Goal transaction is ambiguous".into()));
                }
                let goal = self.store.list_stream_before(&source.stream_id, source.committed_revision, 1, 4 * 1024 * 1024)
                    .map_err(AgentActionServiceError::EventStore)?
                    .pop().ok_or_else(|| AgentActionServiceError::MemberUnknown("original Goal event unavailable".into()))?;
                let goal_entity = match goal.payload.get("action_entity_ref") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(serde_json::Value::String(value)) => Some(value.as_str()),
                    _ => return Err(AgentActionServiceError::MemberUnknown("original Goal entity invalid".into())),
                };
                if goal.sequence != source.committed_revision || goal.transaction_id != existing.transaction_id
                    || goal.commit_cursor != existing.commit_cursor || goal.scope != RuntimeEventScope::Goal
                    || receipt.event_ids.get(goal.transaction_index as usize) != Some(&goal.event_id)
                    || goal_entity != entity.as_deref()
                {
                    return Err(AgentActionServiceError::MemberUnknown("original Goal event mismatch".into()));
                }
                }
            }
            if derived.is_some_and(|expected| Some(expected) != entity)
                || existing.refs != action_refs(envelope, entity.as_deref())
            {
                return Err(AgentActionServiceError::MemberUnknown("original action receipt core mismatch".into()));
            }
            let projection = self.project_snapshot(&envelope.actor.program_id)?;
            return applied_observation(&self.store, envelope, &projection, entity, true).map(Some);
        }

        Ok(None)
    }

    pub(crate) fn apply_verified_review(
        &self,
        envelope: &AgentActionEnvelope,
        verification: &ObjectiveReviewVerification,
        required_write_failures: &[serde_json::Value],
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        let explicit = match &envelope.action {
            AgentAction::TaskReview(input) => input.evidence_refs.as_slice(),
            AgentAction::ObjectiveReview(input) => input.evidence_refs.as_slice(),
            _ => &[],
        };
        super::review_evidence::validate_explicit_process_evidence(
            &self.store,
            verification,
            explicit,
        )
        .map_err(|error| AgentActionServiceError::Corrupt(error.to_string()))?;
        let stream_id = program_stream(&envelope.actor.program_id);
        let mut streams = vec![stream_id.clone()];
        streams.extend(
            verification
                .input_set
                .iter()
                .flat_map(|inputs| inputs.source_revisions.keys().cloned()),
        );
        streams.extend(verification.result_source_revisions.keys().cloned());
        if verification.effect_manifest_digest.is_some() {
            streams.push(format!("session:{}", envelope.actor.session_id));
            streams.extend(verification.effect_source_refs.iter().cloned());
        }
        // Lock the claimed graph before authenticating its physical owner.
        if let Some(physical) = envelope.actor.original_execution.as_ref() {
            streams.push(physical.graph_id.clone());
        }
        self.store.with_stream_locks(&streams, || {
            self.apply_locked(envelope, &stream_id, None, Some(verification), required_write_failures, &[], false, false, None, None)
        })
    }

    // Authenticate the running graph and its original owner under the same
    // ordered locks; the returned revision fences the action transaction.
    fn validate_physical_action_owner(
        &self,
        envelope: &AgentActionEnvelope,
        internal_root_owner: bool,
        internal_supervisor: bool,
    ) -> Result<Option<ExpectedStreamRevision>, AgentActionServiceError> {
        let actor = &envelope.actor;
        let physical = actor.original_execution.as_ref();
        if actor.kind == AgentActorKind::Supervisor && !internal_supervisor {
            return Err(AgentActionServiceError::Corrupt("Runtime supervisor owner required".into()));
        }
        if actor.kind == AgentActorKind::Root && physical.is_none()
            && envelope.action.requires_original_physical_execution() && !internal_root_owner
        {
            return Err(AgentActionServiceError::Corrupt("new root action requires original physical execution".into()));
        }
        if matches!(actor.kind, AgentActorKind::Agent | AgentActorKind::TeamLead) && physical.is_none() {
            // An Agent actor proves its physical ownership either with an original
            // execution or with a live attempt lease. The lease proof is read here,
            // before the action's own transition is validated, so a caller that owns
            // neither dies at the identity gate instead of at a later requirement.
            let leased = self
                .project(&actor.program_id)
                .ok()
                .is_some_and(|projection| self.agent_actor_lease_proof(envelope, &projection));
            if !leased {
                return Err(AgentActionServiceError::Corrupt("agent actor original execution required for new action".into()));
            }
        }
        if actor.kind == AgentActorKind::Supervisor && physical.is_some() {
            return Err(AgentActionServiceError::Corrupt("supervisor cannot borrow physical agent identity".into()));
        }
        let Some(physical) = physical else { return Ok(None); };
        let invalid = |message: String| AgentActionServiceError::Corrupt(message);
        let graphs = self.graphs.as_ref().ok_or_else(|| invalid("agent actor graph store unavailable".into()))?;
        let graph = graphs.load(&physical.graph_id).map_err(|e| invalid(e.to_string()))?;
        let matches_running = physical.attempt != 0
            && graph.id == physical.graph_id
            && match actor.kind {
                AgentActorKind::Agent | AgentActorKind::TeamLead => actor.execution_id.as_deref() == Some(physical.graph_id.as_str()),
                AgentActorKind::Root => actor.execution_id.is_none() && actor.root_execution_id.as_deref() == Some(physical.graph_id.as_str()),
                _ => false,
            }
            && graph.node_statuses.get(&physical.node_id)
                == Some(&harness_contract::execution_graph::ExecutionNodeStatus::Running)
            && graph.recovery_cursor.node_attempts.get(&physical.node_id) == Some(&physical.attempt)
            && graph.lineage.as_ref().is_some_and(|lineage|
                lineage.session_id == actor.session_id && lineage.turn_id == actor.turn_id);
        if !matches_running {
            return Err(invalid("agent actor physical execution no longer active".into()));
        }
        let binding = graphs.current_binding(&graph, &physical.node_id, physical.attempt)
            .map_err(|e| invalid(e.to_string()))?;
        let (ticket, original) = graphs.binding_start_ticket(&graph, &physical.node_id, &binding)
            .map_err(|e| invalid(e.to_string()))?;
        let start = crate::execution_core::graph::validate_node_ticket_start(&self.store, &ticket, None)
            .map_err(|e| invalid(e.to_string()))?;
        if ticket.graph_id != physical.graph_id || ticket.node_id != physical.node_id
            || ticket.attempt != physical.attempt
            || start.nodes.iter().find(|node| node.id == physical.node_id) != Some(&original)
            || graph.nodes.iter().find(|node| node.id == physical.node_id) != Some(&original)
        {
            return Err(invalid("agent actor original execution ticket mismatch".into()));
        }
        let node = &original;
        use harness_contract::agent::{AgentTaskPacket, AgenticExecutionFocus};
        use harness_contract::agent_action::AgentAttemptMode;
        use harness_contract::execution_graph::ExecutionNodeKind;
        let bad = |message: &str| AgentActionServiceError::Corrupt(message.into());
        let lineage = graph.lineage.as_ref().ok_or_else(|| bad("actor graph lineage missing"))?;
        if node.kind != ExecutionNodeKind::AgentTask {
            let objective = harness_contract::agent_action::root_objective_id(&lineage.session_id, &lineage.turn_id);
            if graph.parent_execution.is_some() || actor.kind != AgentActorKind::Root
                || actor.actor_id != format!("root:{}", lineage.session_id)
                || actor.agent_id.is_some() || actor.team_id.is_some()
                || actor.root_execution_id.as_deref() != Some(graph.id.as_str())
                || actor.objective_id != objective
                || actor.program_id != harness_contract::agent_action::program_id_for_objective(&objective)
            {
                return Err(bad("root actor physical owner mismatch"));
            }
            return Ok(Some(ExpectedStreamRevision { stream_id: graph.id, expected_revision: graph.revision }));
        }
        let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
            .map_err(|_| bad("actor original packet invalid"))?;
        let scope = packet.agentic_binding.as_ref().ok_or_else(|| bad("actor packet scope missing"))?;
        scope.validate().map_err(|_| bad("actor packet scope invalid"))?;
        let compiled = packet.binding.as_ref().ok_or_else(|| bad("actor packet binding missing"))?;
        compiled.validate().map_err(|_| bad("actor packet binding invalid"))?;
        packet.validate_cohort_prompt_package().map_err(|_| bad("actor packet cohort mismatch"))?;
        if crate::agent::binding::recompute_binding_digest(compiled).ok().as_deref() != Some(compiled.binding_digest.as_str())
            || compiled.data_lease.session_id != packet.session_id()
            || compiled.data_lease.task_id != packet.task_id()
            || compiled.data_lease.team_id.as_deref() != packet.team_id()
            || packet.graph_id() != graph.id || packet.node_id() != node.id
            || packet.session_id() != lineage.session_id || packet.task_id() != lineage.task_id
            || packet.assignment.root_task_id != lineage.root_task_id
            || packet.assignment.execution_identity.turn_id() != Some(lineage.turn_id.as_str())
            || packet.agent_id() != compiled.instance.instance_id
            || compiled.display.as_ref().is_none_or(|display| {
                display.agent_id != scope.agent_id
                    || display.provenance != format!("runtime.agentic:{}", scope.program_id)
            })
            || !matches!(actor.kind, AgentActorKind::Agent | AgentActorKind::TeamLead) || actor.actor_id != scope.agent_id
            || actor.agent_id.as_deref() != Some(scope.agent_id.as_str())
            || actor.team_id.as_deref() != Some(scope.team_id.as_str())
            || actor.program_id != scope.program_id
            || graph.parent_execution.as_ref().map(|p| p.execution_id.as_str()) != actor.root_execution_id.as_deref()
        {
            return Err(bad("agent actor original packet owner mismatch"));
        }
        let program = self.project_snapshot(&scope.program_id)?;
        if program.session_id != lineage.session_id || program.turn_id != lineage.turn_id
            || program.objective_id != actor.objective_id || program.root_execution_id != actor.root_execution_id
            || !program.agent_is_active_in(&scope.agent_id, &scope.team_id)
            || program.membership_for(&scope.agent_id, &scope.team_id)
                .is_none_or(|member| member.membership_id != scope.membership_id)
        {
            return Err(bad("agent actor program membership mismatch"));
        }
        let (task_ref, mode) = match &scope.focus {
            AgenticExecutionFocus::TaskExecute { task_ref } => (task_ref.as_str(), AgentAttemptMode::Execute),
            AgenticExecutionFocus::TaskReview { task_ref } => (task_ref.as_str(), AgentAttemptMode::Review),
            AgenticExecutionFocus::Coordination { wake_ref } => {
                let (_, wake) = program.coordination_wake_ref(wake_ref).ok_or_else(|| bad("coordination wake missing"))?;
                let owner = wake.coordination.as_ref().ok_or_else(|| bad("coordination owner missing"))?;
                if owner.settled || owner.execution_id != graph.id || owner.agent_id != scope.agent_id
                    || owner.membership_id != scope.membership_id {
                    return Err(bad("coordination owner mismatch"));
                }
                (wake.intent.as_ref().ok_or_else(|| bad("coordination intent missing"))?.task_ref.as_str(), AgentAttemptMode::Coordination)
            }
            _ => return Err(bad("agent actor packet focus unsupported")),
        };
        let task = program.tasks.get(task_ref).ok_or_else(|| bad("agent actor task missing"))?;
        let attempt = task.active_attempts.get(&graph.id).ok_or_else(|| bad("agent actor registered business owner missing"))?;
        if task.team_id != scope.task_team_id || program.execution_task_id(task_ref).ok().as_deref() != Some(packet.task_id())
            || attempt.execution_id != graph.id || attempt.agent_id != scope.agent_id
            || attempt.membership_id != scope.membership_id || attempt.mode != mode
            || (mode == AgentAttemptMode::Review && task.review_generation != attempt.generation)
            || (mode == AgentAttemptMode::Execute && !match (task.claim_execution_id.as_deref(), task.claimant.as_deref()) {
                (None, None) => task.claim_generation == attempt.generation,
                (Some(execution), Some(agent)) => execution == graph.id && agent == scope.agent_id
                    && attempt.generation.checked_add(1) == Some(task.claim_generation),
                _ => false,
            })
        {
            return Err(bad("agent actor registered business owner mismatch"));
        }
        let graphs = self.graphs.as_ref().ok_or_else(|| bad("actor graph store missing"))?;
        let start = graphs.binding_start_position(&graph, &node.id, &binding)
            .map_err(|_| bad("actor original start unavailable"))?
            .ok_or_else(|| bad("actor original start missing"))?;
        let mut matches = 0;
        for event in self.store.list_stream(&program_stream(&scope.program_id))
            .map_err(|_| bad("actor dispatch history unavailable"))? {
            if event.kind != ACTION_EVENT_KIND || (event.commit_cursor, event.transaction_index) >= (start.1, start.2) {
                continue;
            }
            let Ok(recorded) = serde_json::from_value::<AgentActionEnvelope>(event.payload["envelope"].clone()) else { continue; };
            if let AgentAction::TaskAttemptDispatch(dispatch) = recorded.action {
                if dispatch.execution_id == graph.id && dispatch.task_ref == task_ref
                    && dispatch.agent_ref == scope.agent_id && dispatch.membership_id == scope.membership_id
                    && dispatch.mode == mode && dispatch.generation == attempt.generation
                    && recorded.actor.program_id == scope.program_id
                    && recorded.actor.kind == AgentActorKind::Supervisor
                    && recorded.actor.actor_id == "runtime.program-supervisor"
                    && recorded.actor.session_id == actor.session_id
                    && recorded.actor.turn_id == actor.turn_id
                    && recorded.actor.root_execution_id == actor.root_execution_id
                    && recorded.actor.execution_id.as_deref() == Some(graph.id.as_str())
                    && event.status.as_deref() == Some("applied")
                    && event.scope == RuntimeEventScope::Program && event.schema_version == 1
                    && event.stream_id == program_stream(&scope.program_id)
                    && event.idempotency_key.as_deref() == Some(action_key(&recorded.action_id).as_str())
                    && event.actor.as_deref() == Some(recorded.actor.actor_id.as_str())
                { matches += 1; }
            }
        }
        if matches != 1 { return Err(bad("agent actor original business dispatch missing or ambiguous")); }
        Ok(Some(ExpectedStreamRevision { stream_id: graph.id, expected_revision: graph.revision }))
    }

    /// The two proofs an Agent actor may use instead of an original execution:
    /// an active attempt on a Program task, or an AgentTask node lease.
    fn agent_actor_lease_proof(
        &self,
        envelope: &AgentActionEnvelope,
        projection: &AgenticProgramProjection,
    ) -> bool {
        if envelope.action.requires_original_physical_execution() {
            return false;
        }
        let active_attempt = envelope.actor.execution_id.as_ref().and_then(|execution| {
            projection.tasks.values().find_map(|task| {
                task.active_attempts
                    .get(execution)
                    .map(|attempt| (task, attempt))
            })
        });
        let leased = active_attempt.is_some_and(|(task, attempt)| {
            envelope.actor.agent_id.as_deref() == Some(attempt.agent_id.as_str())
                && envelope.actor.agent_id.as_deref() == Some(envelope.actor.actor_id.as_str())
                && envelope.actor.team_id.as_deref() == Some(task.team_id.as_str())
                && projection.agent_is_active_in(&attempt.agent_id, &task.team_id)
        });
        leased || self.agent_graph_lease_matches(envelope, projection)
    }

    fn agent_graph_lease_matches(
        &self,
        envelope: &AgentActionEnvelope,
        projection: &AgenticProgramProjection,
    ) -> bool {
        use harness_contract::agent::AgentTaskPacket;
        use harness_contract::execution_graph::ExecutionNodeKind;
        let graphs = self
            .graphs
            .clone()
            .unwrap_or_else(|| crate::ExecutionGraphStateStore::new(Arc::clone(&self.store)));
        let Some(execution_id) = envelope.actor.execution_id.as_deref() else {
            return false;
        };
        let Ok(graph) = graphs.load(execution_id) else {
            return false;
        };
        let Some(lineage) = graph.lineage.as_ref() else {
            return false;
        };
        if graph.id != execution_id
            || lineage.session_id != envelope.actor.session_id
            || lineage.turn_id != envelope.actor.turn_id
        {
            return false;
        }
        graph.nodes.iter().any(|node| {
            if node.kind != ExecutionNodeKind::AgentTask {
                return false;
            }
            let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref) else {
                return false;
            };
            let Some(scope) = packet.agentic_binding.as_ref() else {
                return false;
            };
            packet.graph_id() == graph.id
                && packet.session_id() == envelope.actor.session_id
                && scope.program_id == envelope.actor.program_id
                && scope.agent_id == envelope.actor.actor_id
                && envelope.actor.agent_id.as_deref() == Some(scope.agent_id.as_str())
                && envelope.actor.team_id.as_deref() == Some(scope.team_id.as_str())
                && projection.agent_is_active_in(&scope.agent_id, &scope.team_id)
                && projection
                    .membership_for(&scope.agent_id, &scope.team_id)
                    .is_some_and(|membership| membership.membership_id == scope.membership_id)
        })
    }

    fn apply_locked(
        &self,
        envelope: &AgentActionEnvelope,
        stream_id: &str,
        goal_event: Option<(String, u64, RuntimeTransactionEventInput)>,
        verification: Option<&ObjectiveReviewVerification>,
        required_write_failures: &[serde_json::Value],
        extra_sources: &[ExpectedStreamRevision],
        internal_root_owner: bool,
        internal_supervisor: bool,
        member: Option<&OriginalActionCommit<'_>>,
        review_cut: Option<&super::ingress::ReviewCutSettlement>,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        self.ensure_recovery_session_admitted(&envelope.actor.session_id).map_err(|error| {
            error
        })?;
        if let Some(replay) = self.replay_if_applied(envelope).map_err(|error| {
            error
        })? {
            return Ok(replay);
        }
        if matches!(envelope.action, AgentAction::ObjectiveCompleteRequest(_)) {
            if let Some(original) = self.store.event_by_idempotency_key(
                stream_id, &format!("completion-rejected:{}", envelope.action_id),
            )? {
                if let Some(value) = original.payload.get("envelope") {
                    let previous: AgentActionEnvelope = serde_json::from_value(value.clone())?;
                    if previous != *envelope {
                        return Err(AgentActionServiceError::Corrupt("completion rejection replay conflicts with original action".into()));
                    }
                    if !self.completion_rejection_replay_is_fenced(&original, envelope)? {
                        return Err(AgentActionServiceError::Corrupt("completion rejection replay source fence mismatch".into()));
                    }
                    let code = original.payload["code"].as_str().ok_or_else(|| AgentActionServiceError::Corrupt("completion rejection code missing".into()))?;
                    let gap = original.payload["gap"].as_str().ok_or_else(|| AgentActionServiceError::Corrupt("completion rejection reason missing".into()))?;
                    let mut replay = rejected(envelope, original.sequence.saturating_sub(1), code, gap);
                    replay.duplicate = true;
                    return Ok(replay);
                }
            }
        }

        // Replays above remain readable after the producer finishes. The
        // source revision returned here is checked by the action append.
        let physical_source = self.validate_physical_action_owner(
            envelope, internal_root_owner, internal_supervisor,
        )?;

        let review_cut_source = if let Some(origin) = review_cut {
            let bad = |message: &str| AgentActionServiceError::Corrupt(message.into());
            let graphs = self.graphs.as_ref().ok_or_else(|| bad("review_cut_original_graph_unavailable"))?;
            let graph = graphs.load(origin.graph_id()).map_err(|_| bad("review_cut_original_graph_unavailable"))?;
            if graph.id != origin.graph_id()
                || graph.node_statuses.get(origin.node_id()) != Some(&harness_contract::execution_graph::ExecutionNodeStatus::Running)
                || graph.recovery_cursor.node_attempts.get(origin.node_id()) != Some(&origin.attempt())
            {
                return Err(bad("review_cut_original_attempt_changed"));
            }
            let binding = graphs.current_binding(&graph, origin.node_id(), origin.attempt())
                .map_err(|_| bad("review_cut_original_binding_unavailable"))?;
            let (ticket, original) = graphs.binding_start_ticket(&graph, origin.node_id(), &binding)
                .map_err(|_| bad("review_cut_original_start_unavailable"))?;
            let start = crate::execution_core::graph::validate_node_ticket_start(&self.store, &ticket, None)
                .map_err(|_| bad("review_cut_original_start_unavailable"))?;
            if ticket.graph_id != origin.graph_id() || ticket.node_id != origin.node_id()
                || ticket.attempt != origin.attempt()
                || start.nodes.iter().find(|node| node.id == origin.node_id()) != Some(&original)
                || graph.nodes.iter().find(|node| node.id == origin.node_id()) != Some(&original)
            {
                return Err(bad("review_cut_original_binding_changed"));
            }
            Some(ExpectedStreamRevision { stream_id: graph.id, expected_revision: graph.revision })
        } else { None };

        let history = self
            .proof_session_queries
            .as_ref()
            .and_then(|queries| queries.get())
            .and_then(|queries| queries.history_reader());
        let process_frame = self
            .process_input_verifier
            .as_ref()
            .map(super::review_evidence::ProcessInputVerifier::frame);
        let readers = super::review_evidence::ResultContentReaders {
            artifacts: self.artifacts.as_deref(),
            history: history.as_deref(),
            process_inputs: process_frame.as_ref(),
        };
        let mut projection = match self.project_snapshot(&envelope.actor.program_id) {
            Ok(projection) => projection,
            Err(AgentActionServiceError::Corrupt(message)) if message == "program_not_found" => {
                let mut projection = AgenticProgramProjection::empty(
                    &envelope.actor.program_id,
                    &envelope.actor.objective_id,
                );
                projection.required_team_count = envelope.actor.required_team_count;
                projection.exact_team_count = envelope.actor.exact_team_count;
                projection
                    .objective_summary
                    .clone_from(&envelope.actor.objective_summary);
                if !envelope.actor.model_lease.trim().is_empty() {
                    projection
                        .model_lease
                        .clone_from(&envelope.actor.model_lease);
                }
                projection.permission_ceiling = envelope
                    .actor
                    .permission_ceiling
                    .unwrap_or(harness_contract::policy::PermissionMode::ReadOnly);
                projection
                    .resource_scopes
                    .clone_from(&envelope.actor.resource_scopes);
                projection.session_id.clone_from(&envelope.actor.session_id);
                projection.turn_id.clone_from(&envelope.actor.turn_id);
                projection
                    .root_execution_id
                    .clone_from(&envelope.actor.root_execution_id);
                Arc::new(projection)
            }
            Err(error) => {
                return Err(error);
            },
        };
        self.ensure_recovery_session_admitted(&projection.session_id).map_err(|error| {
            error
        })?;
        if projection.objective_id != envelope.actor.objective_id {
            return Ok(rejected(
                envelope,
                projection.revision,
                "objective_binding_mismatch",
                "the Program is bound to a different Objective",
            ));
        }
        if projection.session_id != envelope.actor.session_id
            || projection.turn_id != envelope.actor.turn_id
            || projection.root_execution_id != envelope.actor.root_execution_id
            || projection.required_team_count != envelope.actor.required_team_count
            || projection.objective_summary != envelope.actor.objective_summary
            || projection.model_lease != envelope.actor.model_lease
            || Some(projection.permission_ceiling) != envelope.actor.permission_ceiling
            || projection.resource_scopes != envelope.actor.resource_scopes
        {
            return Ok(rejected(
                envelope,
                projection.revision,
                "program_binding_mismatch",
                "session, turn, Objective, execution policy, or completion obligation differs from the frozen Program binding",
            ));
        }

        // ── L1: `existing_agent_ref` must resolve to a durable Agent ────────
        // The runnable catalog publishes Definition ids like
        // `builtin/cowd/<tool>`, and a model can pass one as an invite's
        // `existing_agent_ref`. The invite then effects under that raw string,
        // while Program certification parses entity ids as `kind:id` against a
        // known kind set - `builtin` never carries, so a later publish batch
        // on the same node blocks as `effect_completion_uncertain` with
        // `retryable: false` and the run stalls before review can exist.
        // Reject the unresolvable ref before any durable write, naming the
        // Agents the Program actually has so the model can self-correct;
        // never rewrite a reference on the model's behalf.
        if let AgentAction::AgentInvite(input) = &envelope.action {
            if let Some(provided) = input.existing_agent_ref.as_deref() {
                // L1b (e2 ②): a blank-but-present ref is the model's own malformed
                // input, not an intent to create; never silently convert it.
                let reference = provided.trim();
                if reference.is_empty() {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "unknown_existing_agent_ref",
                        "`existing_agent_ref` is blank; omit the field to invite a new Agent",
                    ));
                }
                if !projection.agents.contains_key(reference) {
                    let mut known = projection.agents.keys().cloned().collect::<Vec<_>>();
                    known.sort();
                    let listed = if known.is_empty() {
                        "no Agent exists in this Program yet; invite a new Agent by omitting `existing_agent_ref`".to_string()
                    } else {
                        known.truncate(8);
                        format!(
                            "durable Agents in this Program: {}; catalog entries such as `builtin/cowd/<tool>` are Definition ids, not invite references",
                            known.join(", ")
                        )
                    };
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "unknown_existing_agent_ref",
                        &format!(
                            "`existing_agent_ref` {reference} matches no Agent of this Program; {listed}"
                        ),
                    ));
                }
            }
        }

        // ── Idempotency gate ────────────────────────────────────────────────
        // An already-Accepted Task is closed. Re-reviewing or re-submitting it
        // burns a provider round and an event without changing any fact. A live
        // 1-team run spent 169 of its 239 tool calls here and never reached the
        // terminal action. To change an accepted conclusion the model publishes a
        // superseding Task, which keeps the audit trail; supersede/withdraw/
        // release are lifecycle actions and are deliberately NOT gated.
        if matches!(
            envelope.action,
            AgentAction::TaskReview(_) | AgentAction::TaskSubmit(_)
        ) {
            let gated_task_ref = match &envelope.action {
                AgentAction::TaskReview(input) => Some(input.task_ref.as_str()),
                AgentAction::TaskSubmit(input) => Some(input.task_ref.as_str()),
                _ => None,
            };
            if let Some(task_ref) = gated_task_ref {
                if let Some(task) = projection.tasks.get(task_ref) {
                    if task.status == AgenticTaskStatus::Accepted {
                        return Ok(rejected(
                            envelope,
                            projection.revision,
                            "task_already_accepted",
                            "this Task is already Accepted; publish a superseding Task instead of re-reviewing or re-submitting it",
                        ));
                    }
                }
            }
        }

        if matches!(envelope.action, AgentAction::StateInspect(_)) {
            let goal = projection
                .root_execution_id
                .as_ref()
                .map(|root| {
                    crate::execution_core::goal::GoalStore::new(Arc::clone(&self.store))
                        .projection(&format!("goal:{root}"))
                })
                .transpose()
                .map_err(AgentActionServiceError::Corrupt)?
                .flatten();
            if matches!(&envelope.action, AgentAction::StateInspect(input)
                if input.scope_ref.as_deref() == Some("collaboration_patterns"))
            {
                let patterns =
                    match crate::evolution::collaboration_experience::read_patterns(&self.store, 8)
                    {
                        Ok(patterns) => patterns,
                        Err(error) => {
                            return Ok(rejected(
                                envelope,
                                projection.revision,
                                "advisory_evidence_unavailable",
                                &error,
                            ));
                        }
                    };
                let mut observation = inspected_observation(
                    &self.store,
                    envelope,
                    &projection,
                    goal.as_ref().map(|projection| &projection.goal),
                );
                observation.projection = Some(json!({
                    "advisory_only": true,
                    "instruction": "These are structural observations from independent completed Turns, not prescribed plans, quality guarantees, executable definitions, or capability grants. Adopt, adapt, or ignore them. Inspect Runtime capabilities before using any suggested tool or skill.",
                    "patterns": patterns.iter().filter(|pattern| pattern.is_actionable()).map(|pattern| json!({
                        "pattern_ref": pattern.pattern_id,
                        "revision": pattern.pattern_revision,
                        "suggestion": pattern.semantic_suggestion,
                        "evidence_summary": pattern.evidence_summary,
                    })).collect::<Vec<_>>(),
                }));
                attach_completion_assessment(
                    &mut observation,
                    &envelope.actor,
                    &projection,
                    || {
                        crate::execution_core::goal::assessment::assess_bound_goal_projection(
                            &projection,
                            goal.as_ref(),
                        )
                    },
                );
                return Ok(observation);
            }
            let mut observation = inspected_observation(
                &self.store,
                envelope,
                &projection,
                goal.as_ref().map(|projection| &projection.goal),
            );
            attach_completion_assessment(&mut observation, &envelope.actor, &projection, || {
                crate::execution_core::goal::assessment::assess_bound_goal_projection(
                    &projection,
                    goal.as_ref(),
                )
            });
            return Ok(observation);
        }
        let mut review_source = None;
        if let AgentAction::TaskReview(input) = &envelope.action {
            if input.decision != harness_contract::agent_action::TaskReviewDecision::Accept {
                let Some(requirement) = input
                    .operator_requirement
                    .as_deref()
                    .map(str::trim)
                    .filter(|quote| !quote.is_empty())
                else {
                    return Ok(rejected(
                        envelope, projection.revision, "review_requirement_missing",
                        "Rework or challenge requires operator_requirement: quote the exact relevant original operator wording from state_inspect's Goal objective, or the frozen Program objective if no Goal exists. Explain the missing deliverable in reason; do not accept incomplete work or invent a stricter requirement.",
                    ));
                };
                let goal = projection
                    .root_execution_id
                    .as_ref()
                    .map(|root| {
                        crate::execution_core::goal::GoalStore::new(Arc::clone(&self.store))
                            .projection(&format!("goal:{root}"))
                    })
                    .transpose()
                    .map_err(AgentActionServiceError::Corrupt)?
                    .flatten();
                let objective = if let Some(goal) = goal.as_ref() {
                    if !goal.goal.execution_binding.as_ref().is_some_and(|binding| {
                        binding.agentic_program_id == projection.program_id
                            && binding.objective_id == projection.objective_id
                            && binding.session_id == projection.session_id
                            && binding.turn_id == projection.turn_id
                            && Some(&binding.root_execution_id)
                                == projection.root_execution_id.as_ref()
                    }) {
                        return Ok(rejected(envelope, projection.revision,
                            "review_goal_binding_mismatch",
                            "The durable Goal is not bound to this Program, root execution, Session, and Turn; review cannot borrow another objective."));
                    }
                    review_source = Some(ExpectedStreamRevision {
                        stream_id: format!("goal:{}", goal.goal.id),
                        expected_revision: goal.stream_revision,
                    });
                    goal.goal.objective.as_str()
                } else {
                    review_source =
                        projection
                            .root_execution_id
                            .as_ref()
                            .map(|root| ExpectedStreamRevision {
                                stream_id: format!("goal:goal:{root}"),
                                expected_revision: 0,
                            });
                    projection.objective_summary.as_str()
                };
                if !objective.contains(requirement) {
                    return Ok(rejected(envelope, projection.revision,
                        "review_requirement_not_in_objective",
                        &format!("operator_requirement does not occur in the bound original operator objective. Copy an exact relevant substring of the original objective below, not a translation or added Task acceptance. Explain the actual missing deliverable separately in reason. Bound original operator objective: {objective:?}. Rejected operator_requirement: {requirement:?}")));
                }
            }
        }
        let review_requires_proof = matches!(&envelope.action, AgentAction::TaskReview(_))
            || matches!(&envelope.action, AgentAction::ObjectiveReview(input)
                if input.decision == harness_contract::agent_action::ObjectiveReviewDecision::Satisfied);
        if (review_requires_proof && verification.is_none())
            || verification.is_some_and(|proof| {
                !proof
                    .input_set
                    .as_ref()
                    .is_some_and(|inputs| inputs.version == 2)
            })
        {
            return Ok(rejected(
                envelope,
                projection.revision,
                "unsupported_review_inputs",
                "new reviews require complete supported v2 verification",
            ));
        }
        let mut policy_sources = Vec::new();
        if let Some(proof) = verification {
            let goal = envelope
                .actor
                .root_execution_id
                .as_deref()
                .map(|root| {
                    crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                        .get(&format!("goal:{root}"))
                })
                .transpose()
                .map_err(AgentActionServiceError::EventStore)?
                .flatten();
            // The proof is re-verified against the action it is applying, so
            // the review's own in-flight call is resolved again from the
            // durable evidence - never from a value the proof carries.
            match super::review_evidence::validate_proof_inputs_for_action(
                &self.store,
                &projection,
                goal.as_ref(),
                proof,
                readers,
                Some(super::review_evidence::ReviewActionIdentity::Live(
                    &super::review_evidence::CurrentReviewContext {
                        action_id: &envelope.action_id,
                        action: &envelope.action,
                        actor: &envelope.actor,
                    },
                )),
            ) {
                Ok(snapshot) => policy_sources.extend(snapshot.sources),
                Err(error) => {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "stale_review_evidence",
                        &error,
                    ));
                }
            }
        }
        if let Some(proof) = verification.filter(|proof| !proof.independence_required) {
            if envelope.actor.kind != AgentActorKind::Root
                || !matches!(envelope.action, AgentAction::ObjectiveReview(_))
                || goal_event.is_none()
            {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "invalid_review_policy",
                    "self review is confined to the bound Root Objective",
                ));
            }
            let root = envelope
                .actor
                .root_execution_id
                .as_deref()
                .ok_or_else(|| AgentActionServiceError::Corrupt("review has no root".into()))?;
            let goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                .get(&format!("goal:{root}"))
                .map_err(AgentActionServiceError::EventStore)?
                .ok_or_else(|| AgentActionServiceError::Corrupt("review has no Goal".into()))?;
            let policy =
                super::review_evidence::root_self_review_policy(&self.store, &projection, &goal)
                    .map_err(AgentActionServiceError::EventStore)?;
            let Some(policy) = policy else {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "stale_review_policy",
                    "Runtime risk or tool effects no longer permit this review",
                ));
            };
            policy_sources.push(policy.source);
        }
        if let Some(expected) = envelope.expected_revision {
            if expected != projection.revision {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "stale_revision",
                    &format!(
                        "expected Program revision {expected}, actual {}",
                        projection.revision
                    ),
                ));
            }
        }
        // ── Exception exit (blocked) ────────────────────────────────────────
        // The DECISION that a goal is unreachable belongs to the model. Runtime
        // only verifies that progress really has stopped, and never masks a real
        // gap: an accepted blocked exit ends as an honest Partial whose
        // `unresolved` stays visible.
        if let AgentAction::ObjectiveCompleteRequest(input) = &envelope.action {
            if input.blocked {
                if input
                    .blocked_reason
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                    || !input.human_review_required
                    || input.recommended_next_actions.is_empty()
                {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "blocked_exit_incomplete",
                        "a blocked exit requires a non-empty blocked_reason, human_review_required=true and at least one recommended_next_action",
                    ));
                }
                if !agentic_program_is_quiescent(&projection) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "blocked_exit_unearned",
                        "the Program still has work in flight; a blocked exit is not permitted until the remaining work is exhausted",
                    ));
                }
            }
        }
        let completion_context = if matches!(&envelope.action, AgentAction::ObjectiveCompleteRequest(input) if !input.blocked)
            && matches!(
                envelope.actor.kind,
                AgentActorKind::Root | AgentActorKind::Supervisor
            )
            && matches!(
                projection.status,
                super::program::AgenticProgramStatus::Open
                    | super::program::AgenticProgramStatus::Waiting
                    | super::program::AgenticProgramStatus::Draining
            ) {
            match super::review_evidence::completion_admission_context(
                &self.store,
                &projection,
                envelope,
                readers,
            ) {
                Ok(context) => context,
                Err(message) => {
                    self.record_completion_rejection(
                        &projection,
                        envelope,
                        "objective_not_verifiable",
                        &message,
                        physical_source.as_ref(),
                        member,
                    )?;
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "objective_not_verifiable",
                        &message,
                    ));
                }
            }
        } else {
            None
        };
        let retirement_context = match self.runtime_retirement_admission(&projection, envelope) {
            Ok(context) => context,
            Err(message) => {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "runtime_retirement_not_settled",
                    &message,
                ));
            }
        };
        let mut quantity_source = None;
        let mut current_team_bounds = None;
        if matches!(envelope.action, AgentAction::TeamCreate(_)) {
            if let Some(root) = envelope.actor.root_execution_id.as_deref() {
                let goal_id = format!("goal:{root}");
                let goal_stream = format!("goal:{goal_id}");
                let goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                    .projection(&goal_id)
                    .map_err(|error| {
                        AgentActionServiceError::Corrupt(error)
                    })?;
                quantity_source = Some(ExpectedStreamRevision {
                    stream_id: goal_stream,
                    expected_revision: goal.as_ref().map_or(0, |goal| goal.stream_revision),
                });
                if let Some(goal) = goal {
                    current_team_bounds =
                        crate::execution_core::goal::current_goal_team_bounds(&goal.goal);
                    if current_team_bounds.is_some() {
                        let bound = goal.goal.execution_binding.as_ref().is_some_and(|binding| {
                            binding.root_execution_id == root
                                && binding.session_id == envelope.actor.session_id
                                && goal.goal.session_id == envelope.actor.session_id
                                && binding.turn_id == envelope.actor.turn_id
                                && binding.agentic_program_id == envelope.actor.program_id
                                && binding.objective_id == envelope.actor.objective_id
                        });
                        if !bound {
                            return Ok(rejected(
                                envelope,
                                projection.revision,
                                "current_goal_binding_mismatch",
                                "current business quantities belong to another Goal binding",
                            ));
                        }
                    }
                }
            }
        }
        let transition = validate_transition_with_admission_contexts(
            &projection,
            envelope,
            now_ms(),
            self.graphs.as_ref(),
            completion_context.as_ref(),
            retirement_context.as_ref(),
            current_team_bounds,
        );
        if let Some((code, message)) = transition {
            if matches!(
                code,
                "objective_not_verifiable" | "objective_blocked_for_human_review"
            ) && matches!(envelope.action, AgentAction::ObjectiveCompleteRequest(_))
            {
                // 033 S8: the terminal obligation list leaves with the
                // verdict. A close-out that is refused must say which
                // obligation is still open and which family blocked it -
                // otherwise the root sees "not verifiable" and has to guess.
                let obligations = projection
                    .tasks
                    .values()
                    .map(super::work_market::terminal_obligation_list)
                    .flatten()
                    .collect::<Vec<_>>();
                if !obligations.is_empty() {
                    let message = format!(
                        "{message}; terminal_obligations={}",
                        serde_json::to_string(&obligations).unwrap_or_else(|_| "[]".into())
                    );
                    // The durable rejection record is the fact that bounds
                    // the completion loop; a record that could not be written
                    // must not be reported as if it had been. Propagate the
                    // failure instead of dropping it (the F2b call below
                    // does the same), so the close-out attempt fails loudly
                    // rather than pretending it was recorded.
                    self.record_completion_rejection(
                        &projection,
                        envelope,
                        code,
                        &message,
                        physical_source.as_ref(),
                        member,
                    )?;
                    return Ok(rejected(envelope, projection.revision, code, &message));
                }
                // F2b: an authorization-time completion rejection never became a
                // durable reopen, so record it durably to keep the completion
                // loop bounded by the reopen count.
                self.record_completion_rejection(&projection, envelope, code, &message, physical_source.as_ref(), member)?;
            }
            // T3.0 diagnostic: surface the exact rejection code so a failing
            // tool call (e.g. task_review accept) is readable, not a bare
            // "execution_error". Logs only; no behaviour change.
            tracing::warn!(
                code,
                action = %envelope.action.kind(),
                program_id = %envelope.actor.program_id,
                agent_id = envelope.actor.agent_id.as_deref().unwrap_or("-"),
                message = %message,
                "agent action rejected at transition validation"
            );
            return Ok(rejected(envelope, projection.revision, code, &message));
        }
        if matches!(envelope.actor.kind, AgentActorKind::Agent | AgentActorKind::TeamLead)
            && envelope.actor.original_execution.is_none()
            && !self.agent_actor_lease_proof(envelope, &projection)
        {
            return Err(AgentActionServiceError::Corrupt(
                "agent actor original execution required for new action".into(),
            ));
        }
        if let Some(context) = completion_context {
            policy_sources.extend(context.sources().iter().cloned());
        }
        // A Task may support several original Goal conditions, but it cannot
        // manufacture them. Keep the read revision in this same transaction so
        // a concurrent Goal change cannot admit a dangling reference.
        let mut obligation_source = None;
        if let AgentAction::TaskPublish(input) = &envelope.action {
            if !input.obligation_refs.is_empty() {
                let goal = projection
                    .root_execution_id
                    .as_ref()
                    .map(|root| {
                        crate::execution_core::goal::GoalStore::new(Arc::clone(&self.store))
                            .projection(&format!("goal:{root}"))
                    })
                    .transpose()
                    .map_err(AgentActionServiceError::Corrupt)?
                    .flatten();
                let Some(goal) = goal else {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_goal_not_found",
                        "Task obligation references require the bound durable Goal",
                    ));
                };
                if !goal.goal.execution_binding.as_ref().is_some_and(|binding| {
                    binding.agentic_program_id == projection.program_id
                        && binding.objective_id == projection.objective_id
                        && binding.session_id == projection.session_id
                        && binding.turn_id == projection.turn_id
                        && Some(&binding.root_execution_id) == projection.root_execution_id.as_ref()
                }) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_goal_binding_mismatch",
                        "Task references must belong to this Program's immutable Goal binding",
                    ));
                }
                if let Some(missing) = input.obligation_refs.iter().find(|reference| {
                    !goal
                        .goal
                        .criteria
                        .iter()
                        .any(|criterion| &criterion.id == *reference)
                        && !goal
                            .goal
                            .obligations
                            .iter()
                            .any(|obligation| &obligation.obligation_id == *reference)
                }) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "obligation_not_found",
                        &format!(
                            "Goal condition {missing} does not exist; inspect the current Goal"
                        ),
                    ));
                }
                obligation_source = Some(ExpectedStreamRevision {
                    stream_id: format!("goal:{}", goal.goal.id),
                    expected_revision: goal.stream_revision,
                });
            }
        }
        if let AgentAction::ArtifactCommit(input) = &envelope.action {
            if let Some(artifacts) = &self.artifacts {
                if let Err(error) = artifacts.resolve(&input.content_ref) {
                    return Ok(rejected(
                        envelope,
                        projection.revision,
                        "artifact_content_not_durable",
                        &format!("content_ref must resolve in Runtime ArtifactStore: {error}"),
                    ));
                }
            }
        }
        if let Some(artifacts) = &self.artifacts {
            let evidence_refs: &[String] = match &envelope.action {
                AgentAction::TaskSubmit(input) => &input.evidence_refs,
                AgentAction::TaskReview(input) => &input.evidence_refs,
                AgentAction::TaskSupersede(input) => &input.evidence_refs,
                AgentAction::TaskWithdraw(input) => &input.evidence_refs,
                AgentAction::ObjectiveReview(input) => &input.evidence_refs,
                AgentAction::ObjectiveCompleteRequest(input) => &input.evidence_refs,
                _ => &[],
            };
            // Opaque artifact selectors can be verified locally. Stable
            // logical tool:// evidence is resolved and authenticated against
            // the Session journal by the async Gateway ingress before this
            // synchronous Program transition is applied.
            if let Some(reference) = evidence_refs.iter().find(|reference| {
                reference.starts_with("artifact://")
                    && artifacts.resolve(reference).is_err()
                    && !projection
                        .artifact_content_selector(reference)
                        .is_some_and(|selector| artifacts.resolve(selector).is_ok())
            }) {
                return Ok(rejected(
                    envelope,
                    projection.revision,
                    "evidence_not_durable",
                    &format!(
                        "evidence reference does not resolve in Runtime ArtifactStore: {reference}"
                    ),
                ));
            }
        }

        let entity_ref = entity_ref(envelope).or_else(|| {
            goal_event.as_ref().and_then(|(_, _, event)| {
                event
                    .event
                    .payload
                    .get("action_entity_ref")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
        });
        let mut rework_history_rebuilt = false;
        if let Some(origin) = review_cut {
            origin.validate_active(&projection)
                .map_err(|error| AgentActionServiceError::Corrupt(error.into()))?;
        }
        if let AgentAction::TaskReview(input) = &envelope.action {
            if matches!(input.decision, harness_contract::agent_action::TaskReviewDecision::Challenge | harness_contract::agent_action::TaskReviewDecision::Rework)
                && projection.tasks.contains_key(&input.task_ref)
                && projection.applied_rework_rounds.as_ref().and_then(|rounds| rounds.get(&input.task_ref)).is_none()
            {
                let rebuilt = rebuild_rework_rounds(&self.store, &projection.program_id, projection.revision, &mut Vec::new())?;
                Arc::make_mut(&mut projection).applied_rework_rounds = Some(rebuilt);
                rework_history_rebuilt = true;
                if projection.applied_rework_rounds.as_ref().and_then(|rounds| rounds.get(&input.task_ref)).is_none() {
                    return Err(AgentActionServiceError::MemberUnknown(format!("review_rework_history_unknown before commit: task {} at Program revision {}", input.task_ref, projection.revision)));
                }
            }
        }
        let mut events = Vec::with_capacity(if projection.revision == 0 { 2 } else { 1 });
        if projection.revision == 0 {
            events.push(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream_id.to_string(),
                    scope: RuntimeEventScope::Program,
                    kind: PROGRAM_OPENED_EVENT_KIND.to_string(),
                    status: Some("open".to_string()),
                    actor: Some(envelope.actor.actor_id.clone()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "objective".to_string(),
                            id: envelope.actor.objective_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "session".to_string(),
                            id: envelope.actor.session_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: envelope.actor.turn_id.clone(),
                        },
                    ]
                    .into_iter()
                    .chain(
                        envelope
                            .actor
                            .root_execution_id
                            .iter()
                            .map(|id| RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: id.clone(),
                            }),
                    )
                    .collect(),
                    payload: json!({
                        "program_id": envelope.actor.program_id,
                        "objective_id": envelope.actor.objective_id,
                        "session_id": envelope.actor.session_id,
                        "turn_id": envelope.actor.turn_id,
                        "root_execution_id": envelope.actor.root_execution_id,
                        "required_team_count": envelope.actor.required_team_count,
                        "exact_team_count": envelope.actor.exact_team_count,
                        "team_count_contract_version": 1,
                        "objective_summary": envelope.actor.objective_summary,
                        "model_lease": envelope.actor.model_lease,
                        "permission_ceiling": envelope.actor.permission_ceiling,
                        "resource_scopes": envelope.actor.resource_scopes,
                    }),
                },
                idempotency_key: Some("agentic-program-opened".to_string()),
                schema_version: 1,
            });
        }
        // A lease is a duration, not a fact of the moment: recording it with
        // the action keeps the rebuilt projection's fence on the duration that
        // was in force when the action was applied.  An Execute dispatch writes
        // the same fence as a claim, so it records the same duration.
        let claim_lease_ms = match &envelope.action {
            AgentAction::TaskClaim(_) => Some(super::work_market::claim_lease_ms()),
            AgentAction::TaskAttemptDispatch(input)
                if input.mode == harness_contract::agent_action::AgentAttemptMode::Execute =>
            {
                Some(super::work_market::claim_lease_ms())
            }
            _ => None,
        };
        let mut action_payload = json!({
            "envelope": envelope,
            "entity_ref": entity_ref,
            "review_verification": verification,
        });
        if let Some(origin) = review_cut {
            if !internal_supervisor {
                return Err(AgentActionServiceError::Corrupt("review_cut_settlement_owner_required".into()));
            }
            action_payload["review_cut_settlement"] = origin.event_metadata(envelope)
                .map_err(|error| AgentActionServiceError::Corrupt(error.into()))?;
        }
        // The journal owner fixes the new review-round rule in the same
        // transaction as the original applied verdict, never in model input.
        if matches!(&envelope.action, AgentAction::TaskReview(input)
            if matches!(input.decision, harness_contract::agent_action::TaskReviewDecision::Challenge | harness_contract::agent_action::TaskReviewDecision::Rework)) {
            action_payload["review_round_rule"] = json!("verdict_v1");
        }
        // The named failures of the required writes this review settled as
        // failed. They are written only when there are any, beside the
        // review's own verification.
        if !required_write_failures.is_empty() {
            action_payload["required_write_failures"] = json!(required_write_failures);
        }
        if let Some(facts) = member.and_then(|context| context.single_call_facts.as_ref()) {
            action_payload["original_single_call"] = facts.clone();
        }
        if let Some(facts) = member.and_then(|context| context.member_call_facts.as_ref()) {
            // The action event keeps one copy of the persisted batch payload
            // and one copy of the Step snapshot it carries; every other copy
            // points at them. The facts a reader rebuilds are byte identical.
            let mut facts = facts.clone();
            crate::step_snapshot::dedupe_member_admission_facts(&mut facts).map_err(|error| {
                AgentActionServiceError::Corrupt(error)
            })?;
            action_payload["member_admission"] = facts;
        }
        if let Some(lease_ms) = claim_lease_ms {
            action_payload["claim_lease_ms"] = json!(lease_ms);
        }
        let action_event_index = events.len();
        events.push(RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: stream_id.to_string(),
                scope: RuntimeEventScope::Program,
                kind: ACTION_EVENT_KIND.to_string(),
                status: Some("applied".to_string()),
                actor: Some(envelope.actor.actor_id.clone()),
                refs: action_refs(envelope, entity_ref.as_deref()),
                payload: action_payload,
            },
            idempotency_key: Some(action_key(&envelope.action_id)),
            schema_version: 1,
        });
        if let Some(index) = Self::original_member_index_event(
            envelope, member, action_event_index, ACTION_EVENT_KIND, &action_key(&envelope.action_id),
        ).map_err(|error| {
            error
        })? { events.push(index); }
        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: stream_id.to_string(),
            expected_revision: projection.revision,
        }];
        expected_streams.extend(extra_sources.iter().cloned());
        expected_streams.extend(physical_source);
        expected_streams.extend(review_cut_source);
        expected_streams.extend(quantity_source);
        expected_streams.extend(policy_sources);
        expected_streams.extend(
            retirement_context
                .iter()
                .flat_map(|context| context.sources.iter().cloned()),
        );
        expected_streams.extend(
            verification
                .into_iter()
                .flat_map(|proof| &proof.result_source_revisions)
                .map(|(stream_id, revision)| ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: *revision,
                }),
        );
        if let Some(source) = review_source {
            expected_streams.push(source);
        }
        if let Some(source) = obligation_source {
            expected_streams.push(source);
        }
        if let Some((goal_stream_id, expected_goal_stream_revision, goal_event)) = goal_event {
            expected_streams.push(ExpectedStreamRevision {
                stream_id: goal_stream_id,
                expected_revision: expected_goal_stream_revision,
            });
            events.push(goal_event);
        }
        let mut unique_sources = std::collections::BTreeMap::new();
        for source in expected_streams {
            if unique_sources
                .insert(source.stream_id, source.expected_revision)
                .is_some_and(|previous| previous != source.expected_revision)
            {
                return Err(AgentActionServiceError::Corrupt(
                    "review source changed during preparation".into(),
                ));
            }
        }
        let expected_streams = unique_sources
            .into_iter()
            .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                stream_id,
                expected_revision,
            })
            .collect();
        let transaction = AppendTransactionRequest {
            transaction_id: format!(
                "agent-action:{}:{}",
                envelope.actor.program_id, envelope.action_id
            ),
            expected_streams,
            events,
        };
        #[cfg(test)]
        if matches!(envelope.action, AgentAction::TeamCreate(_)) {
            run_conclusion_append_hook(&transaction);
        }
        self.append_original_action_transaction(transaction, member).map_err(|error| {
            error
        })?;
        // S1: task success/rework is measured at the commit boundary (after the
        // replay guard and the durable append), never in the replayable
        // projection fold, so replays and rebuilds cannot inflate the counts.
        if let AgentAction::TaskReview(input) = &envelope.action {
            match input.decision {
                harness_contract::agent_action::TaskReviewDecision::Accept => {
                    crate::repetition_guard::clear_failure_breakers(
                        &self
                            .store
                            .repetition_scope(&envelope.actor.session_id, &envelope.actor.turn_id),
                    );
                    crate::execution_core::performance::observe_count("agentic_task_accepted", 1);
                }
                harness_contract::agent_action::TaskReviewDecision::Challenge
                | harness_contract::agent_action::TaskReviewDecision::Rework => {
                    crate::execution_core::performance::observe_count("agentic_task_reworked", 1);
                }
            }
        }
        if rework_history_rebuilt {
            // Keep the source-verified original projection through this commit:
            // an older cache must not force a second read of legacy sources.
            let key = action_key(&envelope.action_id);
            let committed = self.store.event_by_idempotency_key(stream_id, &key)
                .map_err(|error| AgentActionServiceError::MemberUnknown(format!("review committed; original event unavailable: {error}")))?
                .ok_or_else(|| AgentActionServiceError::MemberUnknown("review committed; original event unavailable".into()))?;
            let expected_envelope = serde_json::to_value(envelope)
                .map_err(|error| AgentActionServiceError::MemberUnknown(format!("review committed; original envelope unavailable: {error}")))?;
            if committed.sequence != projection.revision.saturating_add(1)
                || committed.stream_id != stream_id || committed.kind != ACTION_EVENT_KIND
                || committed.status.as_deref() != Some("applied")
                || committed.transaction_id != format!("agent-action:{}:{}", envelope.actor.program_id, envelope.action_id)
                || committed.payload.get("envelope") != Some(&expected_envelope)
            {
                return Err(AgentActionServiceError::MemberUnknown("review committed; original event identity unavailable".into()));
            }
            apply_projection_events(&self.store, Arc::make_mut(&mut projection), vec![committed])
                .map_err(|error| AgentActionServiceError::MemberUnknown(format!("review committed; Program observation unavailable: {error}")))?;
            let head = self.store.stream_revision(stream_id)
                .map_err(|error| AgentActionServiceError::MemberUnknown(format!("review committed; Program head unavailable: {error}")))?;
            if head < projection.revision {
                return Err(AgentActionServiceError::MemberUnknown("review committed; Program head regressed".into()));
            }
            self.catch_up_projection(Arc::make_mut(&mut projection), stream_id, head)
                .map_err(|error| AgentActionServiceError::MemberUnknown(format!("review committed; Program tail unavailable: {error}")))?;
            self.remember_projection(&projection);
        } else {
            projection = self.project_snapshot(&envelope.actor.program_id)?;
        }
        let observation = applied_observation(&self.store, envelope, &projection, entity_ref, false);
        if rework_history_rebuilt {
            observation.map_err(|error| AgentActionServiceError::MemberUnknown(format!("review committed; observation unavailable: {error}")))
        } else {
            observation
        }
    }

    /// Durable head revision of one Program's journal, without folding it.
    ///
    /// Used by the barrier resolver to notice that the Program moved on since the
    /// projection it just read, so it can re-read once instead of waiting for a
    /// wake that is not guaranteed to arrive.
    pub fn program_revision(&self, program_id: &str) -> Result<u64, AgentActionServiceError> {
        Ok(self.store.stream_revision(&program_stream(program_id))?)
    }

    pub fn project(
        &self,
        program_id: &str,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        // Callers that need an independently mutable projection explicitly own a copy.
        // Bounded read consumers use the shared immutable snapshot below.
        self.project_snapshot(program_id)
            .map(|snapshot| (*snapshot).clone())
    }

    /// Immutable causal view for read consumers. Unlike `project`, this does
    /// not copy the complete Program on an unchanged durable-head cache hit.
    pub fn project_snapshot(
        &self,
        program_id: &str,
    ) -> Result<Arc<AgenticProgramProjection>, AgentActionServiceError> {
        let stream_id = program_stream(program_id);
        let durable_revision = self.store.stream_revision(&stream_id)?;
        if let Some(mut cached) = self.read_model.get(program_id) {
            if cached.revision == durable_revision {
                return Ok(cached);
            }
            if cached.revision < durable_revision {
                self.catch_up_projection(Arc::make_mut(&mut cached), &stream_id, durable_revision)?;
                self.remember_projection(&cached);
                return Ok(cached);
            }
        }
        if let Some(mut persisted) = self.persisted_projection(program_id, durable_revision)? {
            self.catch_up_projection(&mut persisted, &stream_id, durable_revision)?;
            let persisted = Arc::new(persisted);
            self.remember_projection(&persisted);
            return Ok(persisted);
        }
        let events = self
            .store
            .list_stream_after(&stream_id, 0, durable_revision, 100, 1024 * 1024)
            .map_err(AgentActionServiceError::EventStore)?;
        let opened = events
            .iter()
            .find(|event| event.kind == PROGRAM_OPENED_EVENT_KIND)
            .ok_or_else(|| AgentActionServiceError::Corrupt("program_not_found".to_string()))?;
        let mut projection = projection_from_opened(program_id, opened)?;
        // The named failures the Program's reviews settled: read from the
        // durable applied events, in the order they were applied. The read
        // happens before the events are moved into the projection.
        merge_required_write_failures(&mut projection, &events);
        apply_projection_events(&self.store, &mut projection, events)?;
        self.catch_up_projection(&mut projection, &stream_id, durable_revision)?;
        let projection = Arc::new(projection);
        self.remember_projection(&projection);
        Ok(projection)
    }

    pub(crate) fn project_snapshot_bounded<Failure>(
        &self,
        program_id: &str,
        durable_revision: u64,
        opened: &crate::DurableRuntimeEvent,
        goal: Option<&harness_contract::goal::GoalContract>,
        mut read_page: impl FnMut(u64, u64) -> Result<Vec<crate::DurableRuntimeEvent>, Failure>,
        map_error: impl Fn(AgentActionServiceError) -> Failure,
    ) -> Result<Arc<AgenticProgramProjection>, Failure> {
        let stream = program_stream(program_id);
        if durable_revision == 0
            || opened.stream_id != stream
            || opened.sequence != 1
            || opened.scope != RuntimeEventScope::Program
            || opened.schema_version != 1
            || opened.kind != PROGRAM_OPENED_EVENT_KIND
        {
            return Err(map_error(AgentActionServiceError::Corrupt(
                "invalid bounded Program opening".into(),
            )));
        }
        let cached = self.read_model.get(program_id);
        let cached_revision = cached.as_ref().map(|snapshot| snapshot.revision);
        let mut snapshot = match cached {
            Some(cached) if cached.revision <= durable_revision => cached,
            _ => {
                let mut projection =
                    projection_from_opened(program_id, opened).map_err(&map_error)?;
                apply_projection_events(&self.store, &mut projection, vec![opened.clone()])
                    .map_err(&map_error)?;
                Arc::new(projection)
            }
        };
        while snapshot.revision < durable_revision {
            let page = read_page(snapshot.revision, durable_revision)?;
            if page.is_empty() {
                return Err(map_error(AgentActionServiceError::Corrupt(
                    "bounded Program sequence gap".into(),
                )));
            }
            let mut expected = snapshot.revision;
            for event in &page {
                expected = expected.checked_add(1).ok_or_else(|| {
                    map_error(AgentActionServiceError::Corrupt(
                        "Program sequence overflow".into(),
                    ))
                })?;
                if event.stream_id != stream
                    || event.sequence != expected
                    || expected > durable_revision
                    || event.scope != RuntimeEventScope::Program
                    || event.schema_version != 1
                {
                    return Err(map_error(AgentActionServiceError::Corrupt(
                        "invalid bounded Program tail".into(),
                    )));
                }
            }
            apply_projection_events(&self.store, Arc::make_mut(&mut snapshot), page).map_err(&map_error)?;
            if snapshot.revision != expected {
                return Err(map_error(AgentActionServiceError::Corrupt(
                    "bounded Program reducer coverage".into(),
                )));
            }
        }
        if snapshot.program_id != program_id
            || snapshot.objective_id != opened.payload["objective_id"].as_str().unwrap_or_default()
            || snapshot.root_execution_id.as_deref() != opened.payload["root_execution_id"].as_str()
            || snapshot.session_id != opened.payload["session_id"].as_str().unwrap_or_default()
            || snapshot.turn_id != opened.payload["turn_id"].as_str().unwrap_or_default()
        {
            return Err(map_error(AgentActionServiceError::Corrupt(
                "bounded Program identity mismatch".into(),
            )));
        }
        if let Some(goal) = goal {
            if goal.session_id != snapshot.session_id
                || goal.execution_binding.as_ref().is_some_and(|binding| {
                    Some(binding.root_execution_id.as_str())
                        != snapshot.root_execution_id.as_deref()
                        || binding.agentic_program_id != snapshot.program_id
                        || binding.objective_id != snapshot.objective_id
                        || binding.session_id != snapshot.session_id
                        || binding.turn_id != snapshot.turn_id
                })
            {
                return Err(map_error(AgentActionServiceError::Corrupt(
                    "bounded Program Goal binding mismatch".into(),
                )));
            }
        }
        if cached_revision.is_none_or(|revision| revision < snapshot.revision) {
            self.read_model.put(Arc::clone(&snapshot));
        }
        Ok(snapshot)
    }

    fn catch_up_projection(
        &self,
        projection: &mut AgenticProgramProjection,
        stream: &str,
        head: u64,
    ) -> Result<(), AgentActionServiceError> {
        while projection.revision < head {
            let page = self
                .store
                .list_stream_after(stream, projection.revision, head, 100, 1024 * 1024)
                .map_err(AgentActionServiceError::EventStore)?;
            if page.is_empty() {
                return Err(AgentActionServiceError::Corrupt(
                    "Program tail has a sequence gap".into(),
                ));
            }
            let mut expected = projection.revision;
            for event in &page {
                expected = expected.checked_add(1).ok_or_else(|| {
                    AgentActionServiceError::Corrupt("Program sequence overflow".into())
                })?;
                if event.stream_id != stream
                    || event.sequence != expected
                    || event.schema_version != 1
                {
                    return Err(AgentActionServiceError::Corrupt(
                        "unsupported or discontinuous Program tail".into(),
                    ));
                }
            }
            apply_projection_events(&self.store, projection, page)?;
            if projection.revision != expected {
                return Err(AgentActionServiceError::Corrupt(
                    "Program reducer did not consume tail".into(),
                ));
            }
        }
        Ok(())
    }

    fn persisted_projection(
        &self,
        program_id: &str,
        durable_revision: u64,
    ) -> Result<Option<AgenticProgramProjection>, AgentActionServiceError> {
        let Some(checkpoint) = self
            .store
            .projection_checkpoint(&read_model_projection_id(program_id))?
        else {
            return Ok(None);
        };
        if checkpoint.source_cursor > durable_revision {
            tracing::warn!(
                program_id,
                checkpoint_revision = checkpoint.source_cursor,
                durable_revision,
                "discarding Agentic read snapshot ahead of its durable Program"
            );
            return Ok(None);
        }
        Ok(decode_read_snapshot(&checkpoint, program_id, &self.store))
    }

    fn remember_projection(&self, projection: &Arc<AgenticProgramProjection>) {
        self.read_model.put(Arc::clone(projection));
        let projection_id = read_model_projection_id(&projection.program_id);
        let payload = match serde_json::to_value(projection.as_ref()) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(program_id = %projection.program_id, %error, "Agentic read snapshot serialization failed");
                return;
            }
        };
        let body = match self.store.prepare_content(
            &format!("session:{}", projection.session_id),
            &format!("{projection_id}:{}", projection.revision),
            &payload,
            &crate::PreparedArtifactOrigin::CheckpointBody {
                projection_id: projection_id.clone(),
                source_cursor: projection.revision,
            },
        ) {
            Ok(body) => body,
            Err(error) => {
                tracing::warn!(program_id=%projection.program_id,%error,"current Program checkpoint preparation failed");
                return;
            }
        };
        let payload = json!({"schema_version":3,"session_id":projection.session_id,"body":body});
        // Concurrent Program readers race the same snapshot row. Re-read the
        // authoritative row revision and retry the CAS instead of dropping the
        // snapshot on the first stale revision.
        for attempt in 0..3 {
            let checkpoint = self
                .store
                .projection_checkpoint(&projection_id)
                .ok()
                .flatten();
            if checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.source_cursor >= projection.revision
                    && self
                        .store
                        .stream_revision(&program_stream(&projection.program_id))
                        .is_ok_and(|head| checkpoint.source_cursor <= head)
                    && decode_read_snapshot(checkpoint, &projection.program_id, &self.store)
                        .is_some()
            }) {
                return;
            }
            let repair_ahead = checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.source_cursor > projection.revision
                    && self
                        .store
                        .stream_revision(&program_stream(&projection.program_id))
                        .is_ok_and(|head| checkpoint.source_cursor > head)
            });
            let write = if repair_ahead {
                RuntimeEventStore::compare_and_repair_projection_checkpoint
            } else {
                RuntimeEventStore::compare_and_put_projection_checkpoint
            };
            match write(
                &self.store,
                &projection_id,
                projection.revision,
                checkpoint
                    .as_ref()
                    .map_or(0, |checkpoint| checkpoint.revision),
                &payload,
                now_ms(),
            ) {
                Ok(_) => return,
                Err(RuntimeEventStoreError::StaleRevision { .. }) if attempt < 2 => {
                    continue;
                }
                Err(error) => {
                    tracing::warn!(program_id = %projection.program_id, %error, "Agentic read snapshot persistence failed");
                    return;
                }
            }
        }
    }

    /// Return the bounded, durable Program/own-Team topic delta for one
    /// physical Agent execution. Program revisions order both topics, while a
    /// separate execution cursor makes delivery restart-safe without mutating
    /// Program truth or repeatedly injecting the full projection.
    pub(crate) fn topic_observations(
        &self,
        program_id: &str,
        agent_id: &str,
        team_id: &str,
        execution_id: &str,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Option<AgenticTopicObservationPage>, AgentActionServiceError> {
        if program_id.trim().is_empty()
            || agent_id.trim().is_empty()
            || execution_id.trim().is_empty()
            || max_entries == 0
            || max_bytes == 0
        {
            return Ok(None);
        }
        let projection = self.project_snapshot(program_id)?;
        let _member = projection.agents.get(agent_id).ok_or_else(|| {
            AgentActionServiceError::Corrupt(format!(
                "topic_observer_not_in_program_roster:{agent_id}"
            ))
        })?;
        let team_topics = readable_topic_refs(&projection, agent_id, team_id);
        let cursor_stream = topic_cursor_stream(program_id, execution_id);
        let (from_revision, cursor_revision) = self.topic_cursor(&cursor_stream)?;
        let mut candidates = projection
            .topics
            .iter()
            .filter(|(topic_ref, _)| team_topics.contains(topic_ref.as_str()))
            .flat_map(|(topic_ref, entries)| {
                let start = entries.partition_point(|entry| entry.revision <= from_revision);
                entries[start..]
                    .iter()
                    .filter(|entry| {
                        entry.actor_id != agent_id && topic_entry_visible(entry, agent_id)
                    })
                    .take(max_entries)
                    .map(move |entry| (topic_ref, entry))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.1
                .revision
                .cmp(&right.1.revision)
                .then_with(|| left.1.entry_id.cmp(&right.1.entry_id))
        });

        let mut entries = Vec::new();
        let mut observed_bytes = 0usize;
        for (topic_ref, source) in candidates {
            if entries.len() >= max_entries {
                break;
            }
            let mut entry = AgenticTopicObservation {
                topic_ref: topic_ref.clone(),
                entry: source.clone(),
            };
            let mut entry_bytes = serde_json::to_vec(&entry)?.len();
            if !entries.is_empty()
                && (entries.len() >= max_entries
                    || observed_bytes.saturating_add(entry_bytes) > max_bytes)
            {
                break;
            }
            if entry_bytes > max_bytes {
                // Never inject an unbounded model payload. Preserve routing,
                // identity and revision, but require an explicit scoped read
                // for a producer that violated the compact-message contract.
                entry.entry.summary = Some(
                    "Message metadata exceeded the observation page; inspect this topic/entry explicitly"
                        .to_string(),
                );
                entry.entry.content_ref = None;
                entry.entry.refs.clear();
                entry_bytes = serde_json::to_vec(&entry)?.len();
                if entry_bytes > max_bytes {
                    return Err(AgentActionServiceError::Corrupt(
                        "topic_observation_identity_exceeds_page_budget".to_string(),
                    ));
                }
            }
            observed_bytes = observed_bytes.saturating_add(entry_bytes);
            entries.push(entry);
        }
        let Some(to_revision) = entries.last().map(|entry| entry.entry.revision) else {
            return Ok(None);
        };
        Ok(Some(AgenticTopicObservationPage {
            entries,
            from_revision,
            to_revision,
            cursor_revision,
        }))
    }

    /// Monotonic acknowledgement for a topic page. The cursor is a projection
    /// offset owned by Runtime, never a second writer of Program/Topic state.
    pub(crate) fn acknowledge_topic_observations(
        &self,
        request: AgenticTopicObservationAck,
    ) -> Result<(), AgentActionServiceError> {
        if self
            .program_recovery_is_excluded(&request.program_id)
            .map_err(AgentActionServiceError::EventStore)?
        {
            return Err(AgentActionServiceError::Store(
                RuntimeEventStoreError::InvalidTransaction("session_recovery_quarantined".into()),
            ));
        }
        let stream_id = topic_cursor_stream(&request.program_id, &request.execution_id);
        self.store.with_stream_lock(&stream_id, || {
            let (through_revision, cursor_revision) = self.topic_cursor(&stream_id)?;
            if request.through_revision <= through_revision {
                return Ok(());
            }
            if cursor_revision != request.expected_cursor_revision {
                return Err(AgentActionServiceError::Store(
                    RuntimeEventStoreError::StaleRevision {
                        stream_id: stream_id.clone(),
                        expected: request.expected_cursor_revision,
                        actual: cursor_revision,
                    },
                ));
            }
            self.store
                .append_transaction_locked(AppendTransactionRequest {
                    transaction_id: format!(
                        "agentic-topic-ack:{}:{}:{}",
                        request.program_id, request.execution_id, request.through_revision
                    ),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: stream_id.clone(),
                        expected_revision: cursor_revision,
                    }],
                    events: vec![RuntimeTransactionEventInput {
                        event: RuntimeEventInput {
                            stream_id: stream_id.clone(),
                            scope: RuntimeEventScope::Agent,
                            kind: TOPIC_CURSOR_EVENT_KIND.to_string(),
                            status: Some("acknowledged".to_string()),
                            actor: Some("runtime.agent_topic_observer".to_string()),
                            refs: vec![RuntimeEventRef {
                                kind: "program".to_string(),
                                id: request.program_id.clone(),
                            }],
                            payload: json!({
                                "program_id": request.program_id,
                                "execution_id": request.execution_id,
                                "through_revision": request.through_revision,
                                "observation_kind": request.observation_kind,
                            }),
                        },
                        idempotency_key: Some(format!(
                            "agentic-topic-ack:{}",
                            request.through_revision
                        )),
                        schema_version: 1,
                    }],
                })?;
            Ok(())
        })
    }

    fn topic_cursor(&self, stream_id: &str) -> Result<(u64, u64), AgentActionServiceError> {
        let events = self
            .store
            .list_stream(stream_id)
            .map_err(AgentActionServiceError::EventStore)?;
        let cursor_revision = events.last().map_or(0, |event| event.sequence);
        let through_revision = events
            .iter()
            .rev()
            .find(|event| event.kind == TOPIC_CURSOR_EVENT_KIND)
            .and_then(|event| event.payload.get("through_revision"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        Ok((through_revision, cursor_revision))
    }

    fn completion_rejection_replay_is_fenced(
        &self, event: &crate::DurableRuntimeEvent, envelope: &AgentActionEnvelope,
    ) -> Result<bool, AgentActionServiceError> {
        let stream = program_stream(&envelope.actor.program_id);
        let transaction = format!("completion-rejected:{}:{}", envelope.actor.program_id, envelope.action_id);
        let program = self.store.transaction_stream_revision_exact(&transaction, &stream)?;
        if event.stream_id != stream || event.transaction_id != transaction || event.transaction_index != 0
            || event.kind != COMPLETION_REJECTED_EVENT_KIND || event.scope != RuntimeEventScope::Program
            || event.schema_version != 1 || event.status.as_deref() != Some("rejected")
            || event.actor.as_deref() != Some(envelope.actor.actor_id.as_str())
            || event.refs != vec![RuntimeEventRef { kind:"objective".into(), id:envelope.actor.objective_id.clone() }]
            || event.idempotency_key.as_deref() != Some(format!("completion-rejected:{}", envelope.action_id).as_str())
            || serde_json::from_value::<AgentActionEnvelope>(event.payload["envelope"].clone())? != *envelope
            || program.is_none_or(|fence| fence.expected_revision.checked_add(1) != Some(event.sequence)
                || fence.committed_revision != event.sequence) {
            return Ok(false);
        }
        if let Some(physical) = envelope.actor.original_execution.as_ref() {
            let source = self.store.transaction_stream_revision_exact(&transaction, &physical.graph_id)?;
            if source.is_none_or(|fence| fence.expected_revision == 0 || fence.expected_revision != fence.committed_revision) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Record a durable marker for a completion request rejected at
    /// authorization. A failed journal write is an infrastructure failure,
    /// not a durable rejection that the convergence owner can count.
    fn record_completion_rejection(
        &self,
        projection: &AgenticProgramProjection,
        envelope: &AgentActionEnvelope,
        code: &str,
        gap: &str,
        physical_source: Option<&ExpectedStreamRevision>,
        member: Option<&OriginalActionCommit<'_>>,
    ) -> Result<(), AgentActionServiceError> {
        let mut rejection_payload = json!({
            "program_id": projection.program_id,
            "request_action_id": envelope.action_id,
            "code": code,
            "objective_id": envelope.actor.objective_id,
            "action": envelope.action,
            "envelope": envelope,
            "gap": gap,
        });
        if let Some(facts) = member.and_then(|context| context.single_call_facts.as_ref()) {
            rejection_payload["original_single_call"] = facts.clone();
        }
        if let Some(facts) = member.and_then(|context| context.member_call_facts.as_ref()) {
            // The rejection event carries the same same-source facts as the
            // applied one: one batch payload copy, one Step snapshot copy.
            let mut facts = facts.clone();
            crate::step_snapshot::dedupe_member_admission_facts(&mut facts).map_err(|error| {
                AgentActionServiceError::Corrupt(error)
            })?;
            rejection_payload["member_admission"] = facts;
        }
        let identity = RejectionIdentity {
            program_id: &projection.program_id,
            session_id: &projection.session_id,
            program_revision: projection.revision,
            actor_id: &envelope.actor.actor_id,
            objective_id: &envelope.actor.objective_id,
            action_id: &envelope.action_id,
            code,
            gap,
            payload: rejection_payload,
            replay: RejectionReplay::AgentAction { envelope },
            twin: match Self::original_member_twin(member)? {
                Some(twin) => Some(twin),
                None => self.single_call_rejection_twin(member, &envelope.action_id)?,
            },
        };
        self.record_rejection_fact(&identity, physical_source, member)
    }

    /// Write one durable rejection fact.
    ///
    /// The program event and its effect-stream twin are written in one
    /// transaction: the twin names the program event it belongs to, and the
    /// pair is authenticated as the one transaction that wrote both. A fact
    /// that is already there is read back field by field - the same
    /// rejection is not written twice, and a different one under the same
    /// key is a corruption.
    fn record_rejection_fact(
        &self,
        identity: &RejectionIdentity<'_>,
        physical_source: Option<&ExpectedStreamRevision>,
        member: Option<&OriginalActionCommit<'_>>,
    ) -> Result<(), AgentActionServiceError> {
        if self
            .store
            .is_session_recovery_excluded(identity.session_id)
        {
            return Err(AgentActionServiceError::EventStore(
                "completion rejection journal excluded".into(),
            ));
        }
        let stream_id = program_stream(identity.program_id);
        let program_event_key = format!("completion-rejected:{}", identity.action_id);
        if let Some(existing) = self
            .store
            .event_by_idempotency_key(&stream_id, &program_event_key)?
        {
            if !self.rejection_replay_is_fenced(&existing, identity)? {
                return Err(AgentActionServiceError::Corrupt("completion rejection replay conflicts with original event".into()));
            }
            return Ok(());
        }
        let event = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: stream_id.clone(),
                scope: RuntimeEventScope::Program,
                kind: COMPLETION_REJECTED_EVENT_KIND.to_string(),
                status: Some("rejected".to_string()),
                actor: Some(identity.actor_id.to_string()),
                refs: vec![RuntimeEventRef {
                    kind: "objective".to_string(),
                    id: identity.objective_id.to_string(),
                }],
                payload: identity.payload.clone(),
            },
            idempotency_key: Some(program_event_key.clone()),
            schema_version: 1,
        };
        // Same two corrections as the discipline-conflict marker: append while
        // already holding the stream lock, and carry the expected revision. This
        // marker was written with `append_transaction` and an empty
        // `expected_streams`, which the store rejects - so the bounded-convergence
        // count it exists to feed never saw a single one, behind a `let _ =` that
        // swallowed the error for the life of the feature.
        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id,
            expected_revision: identity.program_revision,
        }];
        if let Some(source) = physical_source {
            if let Some(existing) = expected_streams.iter().find(|entry| entry.stream_id == source.stream_id) {
                if existing != source {
                    return Err(AgentActionServiceError::Corrupt("completion rejection source revision conflict".into()));
                }
            } else {
                expected_streams.push(source.clone());
            }
        }
        let mut events = vec![event];
        if let Some(twin) = &identity.twin {
            let twin_stream = format!("execution-effect:{}", twin.outer_key);
            if !expected_streams.iter().any(|entry| entry.stream_id == twin_stream) {
                let revision = self.store.stream_revision(&twin_stream)?;
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: twin_stream,
                    expected_revision: revision,
                });
            }
            events.push(Self::rejection_index_event(
                identity.program_id,
                twin,
                0,
                COMPLETION_REJECTED_EVENT_KIND,
                &program_event_key,
            ));
        }
        self.append_original_action_transaction(AppendTransactionRequest {
            transaction_id: format!("completion-rejected:{}:{}", identity.program_id, identity.action_id),
            expected_streams,
            events,
        }, member)?;
        Ok(())
    }

    /// Authenticate the admission intent of one call, by the call's own key.
    ///
    /// A refusal is only durable when the call it refuses was admitted. The
    /// consumer reads refusals by the admitted call's own key, so a refusal
    /// of a call that carries no admission intent would be a fact nobody
    /// reads, and would claim a decision that was never taken.
    fn authenticate_call_admission(
        &self,
        effect_key: &str,
        tool_use_id: Option<&str>,
        tool_name: Option<&str>,
    ) -> Result<Option<crate::DurableRuntimeEvent>, AgentActionServiceError> {
        // The admission of a call is read by the runtime's single reader: the
        // same question the closure and the delegated-action gate ask, so
        // this writer can never answer differently from them. An admission
        // the reader cannot interpret is no admission here - nothing is
        // written on the account of a fact this side cannot read.
        let admission =
            crate::execution_core::graph::read_effect_admission(&self.store, effect_key)
                .map_err(|error| AgentActionServiceError::EventStore(error.to_string()))?;
        if admission != crate::execution_core::graph::EffectAdmission::Admitted {
            return Ok(None);
        }
        let Some(intent) =
            crate::execution_core::graph::read_effect_admission_intent(&self.store, effect_key)
                .map_err(|error| AgentActionServiceError::EventStore(error.to_string()))?
        else {
            return Ok(None);
        };
        // On top of that reading, this side keeps one narrower check of its
        // own: the admission must name this call's own tool use and tool.
        if intent.payload["idempotency_key"] != effect_key
            || tool_use_id.is_some_and(|value| intent.payload["tool_use_id"] != value)
            || tool_name.is_some_and(|value| intent.payload["tool_name"] != value)
        {
            return Ok(None);
        }
        Ok(Some(intent))
    }

    /// Record the durable terminal refusal of a call that was admitted and
    /// then refused while it ran.
    ///
    /// The fact is the program's rejection event and its effect-stream twin,
    /// written as one transaction under the refused call's own effect key.
    /// A call without its own admission intent gets no fact at all: the
    /// consumer would never read it, and writing one would claim a refusal
    /// the runtime never decided.
    pub fn record_refused_admitted_call(
        &self,
        projection: &AgenticProgramProjection,
        call: &RefusedCall<'_>,
    ) -> Result<RefusalOutcome, AgentActionServiceError> {
        let Some(intent) =
            self.authenticate_call_admission(call.effect_key, Some(call.tool_use_id), Some(call.tool_name))?
        else {
            return Ok(RefusalOutcome::NotAdmitted);
        };
        let code = "call_refused_after_admission";
        let identity = RejectionIdentity {
            program_id: &projection.program_id,
            session_id: &projection.session_id,
            program_revision: projection.revision,
            actor_id: "runtime.agentic_action",
            objective_id: &projection.objective_id,
            action_id: call.effect_key,
            code,
            gap: call.reason,
            payload: json!({
                "program_id": projection.program_id,
                "request_action_id": call.effect_key,
                "code": code,
                "objective_id": projection.objective_id,
                "action": call.tool_name,
                "envelope": serde_json::Value::Null,
                "gap": call.reason,
                "refused_call": {
                    "effect_key": call.effect_key,
                    "tool_use_id": call.tool_use_id,
                    "tool_name": call.tool_name,
                    "admission": {
                        "stream_id": intent.stream_id,
                        "event_id": intent.event_id,
                        "kind": intent.kind,
                        "commit_cursor": intent.commit_cursor,
                    },
                },
            }),
            replay: RejectionReplay::RefusedCall { effect_key: call.effect_key },
            twin: Some(RejectionTwin {
                outer_key: call.effect_key.to_string(),
                idempotency_key: format!("agentic-single:{}", call.effect_key),
                member_index: None,
                member_identity: None,
                ticket: serde_json::Value::Null,
            }),
        };
        self.record_rejection_fact(&identity, None, None)?;
        Ok(RefusalOutcome::Recorded)
    }

    /// The fence a rejection fact's re-entry read-back must satisfy.
    ///
    /// Each shape compares the fields that identify it: an Agent action's
    /// rejection is fenced by its envelope, a refused call's by the call's
    /// own coordinates. A fact that cannot prove its own identity is not a
    /// replay of this rejection.
    fn rejection_replay_is_fenced(
        &self,
        event: &crate::DurableRuntimeEvent,
        identity: &RejectionIdentity<'_>,
    ) -> Result<bool, AgentActionServiceError> {
        match identity.replay {
            RejectionReplay::AgentAction { envelope } => {
                Ok(self.completion_rejection_replay_is_fenced(event, envelope)?
                    && event.payload["program_id"] == identity.program_id
                    && event.payload["code"] == identity.code
                    && event.payload["objective_id"] == identity.objective_id
                    && event.payload["action"] == serde_json::to_value(&envelope.action)?
                    && serde_json::from_value::<AgentActionEnvelope>(event.payload["envelope"].clone())? == *envelope
                    && event.payload["gap"] == identity.gap
                    && event.actor.as_deref() == Some(identity.actor_id))
            }
            RejectionReplay::RefusedCall { effect_key } => {
                Ok(event.stream_id == program_stream(identity.program_id)
                    && event.kind == COMPLETION_REJECTED_EVENT_KIND
                    && event.scope == RuntimeEventScope::Program
                    && event.schema_version == 1
                    && event.status.as_deref() == Some("rejected")
                    && event.actor.as_deref() == Some(identity.actor_id)
                    && event.idempotency_key.as_deref()
                        == Some(format!("completion-rejected:{}", identity.action_id).as_str())
                    && event.payload["request_action_id"] == effect_key
                    && event.payload["code"] == identity.code)
            }
        }
    }

    /// Borrow the original in-process call custody, if the current request
    /// came from a tool. An absent retained owner cannot prove non-commit.
    fn original_conclusion_sink(
        &self, projection: &AgenticProgramProjection,
    ) -> Result<Option<(String, Option<usize>, crate::ToolProgressSink)>, AgentActionServiceError> {
        let Some(completion) = projection.completion_request.as_ref() else { return Ok(None); };
        let unavailable = || AgentActionServiceError::MemberUnknown("original completion call custody unavailable".into());
        let retained = self.store.retained_original_first_effect(&format!("{}:intent", completion.action_id))?;
        let Some((_, original, owner)) = retained else {
            // An internally authored completion has no tool effect or sink.
            // A previously physical call without its same-owner custody is
            // unknown; absence is not a rollback or a fresh invocation.
            let action = self.store.event_by_idempotency_key(&program_stream(&projection.program_id), &action_key(&completion.action_id))?
                .ok_or_else(|| AgentActionServiceError::Corrupt("original completion action missing".into()))?;
            if let Some(member) = action.payload.get("member_admission") {
                // A batch member's action key is not its outer physical effect
                // key. Authenticate the original member index and action in
                // their one checked transaction before borrowing outer custody.
                let prepared: crate::step_snapshot::PreparedAgentBatchMember = serde_json::from_value(member["prepared"].clone())
                    .map_err(|_| unavailable())?;
                let (_, original, owner) = self.store.retained_original_first_effect(&format!("{}:intent", prepared.outer_key))?
                    .ok_or_else(unavailable)?;
                let owner = owner.ok_or_else(unavailable)?;
                let ticket: crate::execution_core::graph::NodeExecutionTicket = serde_json::from_value(member["ticket"].clone())
                    .map_err(|_| unavailable())?;
                let index_stream = format!("execution-effect:{}", prepared.outer_key);
                let index = self.store.event_by_idempotency_key(&index_stream, &format!("agentic-member:{}", prepared.index))?
                    .ok_or_else(unavailable)?;
                let receipt = self.store.original_transaction_receipt(&action.transaction_id)?.ok_or_else(unavailable)?;
                if !owner.matches_original(&self.store, &original) || original.tool_name != "agent_action_batch"
                    || original.idempotency_key != prepared.outer_key || prepared.tool != "objective_complete_request"
                    || original.session_id.as_deref() != Some(projection.session_id.as_str())
                    || original.parent_execution.as_ref().is_none_or(|parent|
                        parent.execution_id != ticket.graph_id || parent.node_id != ticket.node_id)
                    || original.parent_execution_attempt != Some(ticket.attempt)
                    || action.kind != ACTION_EVENT_KIND || action.status.as_deref() != Some("applied")
                    || action.sequence != completion.program_revision || action.scope != RuntimeEventScope::Program
                    || action.payload["envelope"]["action_id"] != completion.action_id
                    || action.payload["envelope"]["actor"]["actor_id"] != completion.requested_by
                    || action.actor.as_deref() != Some(completion.requested_by.as_str())
                    || action.payload["envelope"]["actor"]["original_execution"]["graph_id"] != ticket.graph_id
                    || action.payload["envelope"]["actor"]["original_execution"]["node_id"] != ticket.node_id
                    || action.payload["envelope"]["actor"]["original_execution"]["attempt"].as_u64() != Some(u64::from(ticket.attempt))
                    || action.payload["envelope"]["actor"]["program_id"] != projection.program_id
                    || action.payload["envelope"]["actor"]["session_id"] != projection.session_id
                    || action.payload["envelope"]["actor"]["turn_id"] != projection.turn_id
                    || index.kind != "agentic.batch_member.index" || index.scope != RuntimeEventScope::ExecutionNode
                    || index.status.as_deref() != Some("committed") || index.actor.as_deref() != Some("runtime.agentic_action")
                    || index.schema_version != 1 || index.payload["outer_key"] != prepared.outer_key
                    || index.payload["member_index"].as_u64() != u64::try_from(prepared.index).ok()
                    || index.payload["member_identity"] != prepared.identity
                    || index.payload["ticket"] != member["ticket"]
                    || index.payload["program_stream"] != action.stream_id
                    || index.payload["program_event_kind"] != action.kind
                    || index.payload["program_event_key"] != action_key(&completion.action_id)
                    || index.payload["program_event_index"].as_u64() != Some(u64::from(action.transaction_index))
                    || index.transaction_id != action.transaction_id || index.commit_cursor != action.commit_cursor
                    || receipt.transaction_id != action.transaction_id || receipt.commit_cursor != action.commit_cursor
                    || receipt.event_ids.get(action.transaction_index as usize) != Some(&action.event_id)
                    || receipt.event_ids.get(index.transaction_index as usize) != Some(&index.event_id)
                    || ![&action, &index].iter().all(|event| receipt.stream_revisions.iter().any(|source|
                        source.stream_id == event.stream_id && source.expected_revision < event.sequence
                            && event.sequence <= source.committed_revision)) {
                    return Err(unavailable());
                }
                return Ok(Some((completion.action_id.clone(), Some(prepared.index), original.tool_progress.clone())));
            }
            return if action.payload.get("original_single_call").is_none() { Ok(None) } else { Err(unavailable()) };
        };
        let owner = owner.ok_or_else(unavailable)?;
        let Some(single) = original.tool_progress.original_single().map_err(|_| unavailable())? else {
            // The background supervisor may observe the committed action
            // before its still-running caller has published the same-owner
            // handle. Authenticate that one durable boundary exactly once;
            // absence or an unrelated event is never treated as Fresh.
            let action = self.store.event_by_idempotency_key(&program_stream(&projection.program_id), &action_key(&completion.action_id))?
                .ok_or_else(unavailable)?;
            let receipt = self.store.original_transaction_receipt(&action.transaction_id)?.ok_or_else(unavailable)?;
            if action.kind != ACTION_EVENT_KIND || action.status.as_deref() != Some("applied")
                || action.sequence != completion.program_revision
                || action.payload["envelope"]["action_id"] != completion.action_id
                || action.payload["envelope"]["actor"]["actor_id"] != completion.requested_by
                || action.payload["envelope"]["actor"]["program_id"] != projection.program_id
                || action.payload["envelope"]["actor"]["session_id"] != projection.session_id
                || action.payload["envelope"]["actor"]["turn_id"] != projection.turn_id
                || original.idempotency_key != completion.action_id || !owner.matches_original(&self.store, &original)
                || action.payload["original_single_call"]["request"] != serde_json::to_value(original.as_ref())?
                || receipt.transaction_id != action.transaction_id || receipt.commit_cursor != action.commit_cursor
                || receipt.event_ids.get(action.transaction_index as usize) != Some(&action.event_id)
                || !receipt.stream_revisions.iter().any(|source| source.stream_id == action.stream_id
                    && source.expected_revision < action.sequence && action.sequence <= source.committed_revision) {
                return Err(unavailable());
            }
            return Ok(Some((completion.action_id.clone(), None, original.tool_progress.clone())));
        };
        let transaction = &single.request().transaction;
        let receipt = &single.receipt().commit;
        let mut actions = transaction.events.iter().enumerate().filter(|(_, entry)|
            entry.event.stream_id == program_stream(&projection.program_id)
                && entry.event.kind == ACTION_EVENT_KIND
                && entry.idempotency_key.as_deref() == Some(action_key(&completion.action_id).as_str()));
        let (position, action) = actions.next().ok_or_else(unavailable)?;
        let facts = action.event.payload.get("original_single_call").ok_or_else(unavailable)?;
        let expected = receipt.stream_revisions.iter().find(|source| source.stream_id == action.event.stream_id).ok_or_else(unavailable)?;
        if actions.next().is_some() || !single.belongs_to(&self.store)
            || action.event.status.as_deref() != Some("applied")
            || expected.committed_revision != completion.program_revision
            || expected.expected_revision.checked_add(1) != Some(completion.program_revision)
            || action.event.payload["envelope"]["action_id"] != completion.action_id
            || action.event.payload["envelope"]["actor"]["actor_id"] != completion.requested_by
            || action.event.payload["envelope"]["actor"]["program_id"] != projection.program_id
            || action.event.payload["envelope"]["actor"]["session_id"] != projection.session_id
            || action.event.payload["envelope"]["actor"]["turn_id"] != projection.turn_id
            || original.idempotency_key != completion.action_id || !owner.matches_original(&self.store, &original)
            || facts.get("request") != Some(&serde_json::to_value(original.as_ref())?)
            || transaction.transaction_id != receipt.transaction_id || receipt.commit_cursor == 0
            || receipt.event_ids.get(position).is_none() {
            return Err(unavailable());
        }
        Ok(Some((completion.action_id.clone(), None, original.tool_progress.clone())))
    }

    /// A winner may become durable after the original checked call returned
    /// Unknown. Reuse its retained request/control and publish that exact
    /// result before the winner read is allowed to return.
    fn publish_retained_conclusion_winner(
        &self, original: &AgenticProgramProjection,
    ) -> Result<(), AgentActionServiceError> {
        let Some(completion) = original.completion_request.as_ref() else { return Ok(()); };
        let transaction_id = format!("program-conclusion:{}:{}", original.program_id, completion.program_revision);
        if !self.store.has_retained_conclusion(&transaction_id)? { return Ok(()); }
        let (action_id, member_index, sink) = self.original_conclusion_sink(original)?
            .ok_or_else(|| AgentActionServiceError::MemberUnknown("original conclusion sink unavailable".into()))?;
        let Some((request, receipt)) = self.store.recover_retained_conclusion(&transaction_id)? else {
            return Err(AgentActionServiceError::MemberUnknown("original conclusion receipt remains unknown".into()));
        };
        let result = Arc::new(OriginalCommittedConclusion {
            store: Arc::downgrade(&self.store), request: Arc::clone(&request),
            receipt: OriginalMemberCommitReceipt { commit: receipt.clone(), validation: None },
            action_id, member_index,
        });
        sink.record_original_conclusion(member_index, result).map_err(AgentActionServiceError::MemberUnknown)?;
        self.store.release_reconciled_conclusion(&request, &receipt)?;
        Ok(())
    }

    /// Commit the Goal decision and the exact Program request disposition or
    /// authenticated Runtime non-success trigger in one transaction. Neither
    /// domain can become terminal on its own here.
    /// Caller holds the original Session policy guard. All source streams are
    /// collected after revalidation and locked by this original owner.
    pub(crate) fn commit_program_conclusion_guarded(
        &self,
        projection: &AgenticProgramProjection,
        mut prepared: crate::execution_core::goal::PreparedProgramConclusion,
        owner: &crate::execution_core::services::OriginalConclusionOwner,
        consumed_input_sequence: usize,
    ) -> Result<AgenticProgramProjection, AgentActionServiceError> {
        if owner.claim.session_id() != projection.session_id || owner.claim.turn_id() != projection.turn_id {
            return Err(AgentActionServiceError::Corrupt("conclusion original Session owner mismatch".into()));
        }
        self.ensure_recovery_session_admitted(&projection.session_id)?;
        self.ensure_recovery_session_admitted(&prepared.goal.session_id)?;
        let original_proof_custody = prepared.original_proof_custody(&self.store, projection)
            .map_err(AgentActionServiceError::Corrupt)?;
        prepared
            .source_frame
            .verify_program(projection)
            .map_err(|error| AgentActionServiceError::Corrupt(error.to_string()))?;
        // A same-request conclusion may already have atomically committed
        // while this preparation was in flight. Authenticate that original
        // winner before rereading its now-changed Goal or proof inputs.
        if prepared.runtime_trigger.is_none() && prepared.rejection_trigger.is_none()
            && projection.completion_request.is_some()
            && self.authenticated_existing_request_conclusion(projection)? {
            return self.project(&projection.program_id);
        }
        let original_sink = self.original_conclusion_sink(projection)?;
        let original_revision = projection.revision;
        let own_stream = program_stream(&projection.program_id);
        if prepared.goal.completion == harness_contract::goal::GoalCompletion::Satisfied
            && !prepared.source_frame.sources().any(|source| {
                source.stream_id == own_stream && source.expected_revision == original_revision
            })
        {
            return Err(AgentActionServiceError::Corrupt(
                "prepared own Program carrier cut is missing".into(),
            ));
        }
        if prepared.goal.completion == harness_contract::goal::GoalCompletion::Satisfied {
            let mut current_sources = prepared.source_frame.clone();
            let request = projection.completion_request.as_ref().ok_or_else(|| {
                AgentActionServiceError::Corrupt("missing completion request before A".into())
            })?;
            let event = self
                .store
                .event_by_idempotency_key(
                    &program_stream(&projection.program_id),
                    &action_key(&request.action_id),
                )?
                .ok_or_else(|| {
                    AgentActionServiceError::Corrupt(
                        "completion request action disappeared before A".into(),
                    )
                })?;
            let envelope: AgentActionEnvelope =
                serde_json::from_value(event.payload["envelope"].clone())?;
            let AgentAction::ObjectiveCompleteRequest(input) = &envelope.action else {
                return Err(AgentActionServiceError::Corrupt(
                    "completion request action changed before A".into(),
                ));
            };
            if event.kind != ACTION_EVENT_KIND
                || event.sequence != request.program_revision
                || envelope.action_id != request.action_id
                || envelope.actor.actor_id != request.requested_by
                || envelope.actor.program_id != projection.program_id
                || envelope.actor.objective_id != projection.objective_id
                || envelope.actor.session_id != projection.session_id
                || envelope.actor.turn_id != projection.turn_id
                || envelope.actor.root_execution_id != projection.root_execution_id
                || request.result_refs != input.result_refs
                || request.evidence_refs != input.evidence_refs
                || request.unresolved != input.unresolved
                || input.blocked
            {
                return Err(AgentActionServiceError::Corrupt(
                    "completion request provenance changed before A".into(),
                ));
            }
            let context = super::review_evidence::completion_admission_context_from_prepared(
                projection,
                &envelope,
                &prepared,
            )
            .map_err(AgentActionServiceError::EventStore)?;
            if context.as_ref().is_some_and(|context| {
                !context.matches_goal(&prepared.goal, prepared.expected_stream_revision)
            }) {
                return Err(AgentActionServiceError::EventStore(
                    "completion context Goal cut changed before A".into(),
                ));
            }
            if let Some(gap) =
                super::supervision::completion_gap_with_context(projection, input, context.as_ref())
            {
                return Err(AgentActionServiceError::EventStore(format!(
                    "completion admission changed before A:{gap}"
                )));
            }
            if let Some(context) = context {
                for source in context.sources() {
                    current_sources
                        .pin(ExpectedStreamRevision {
                            stream_id: source.stream_id.clone(),
                            expected_revision: if source.stream_id == own_stream {
                                original_revision
                            } else {
                                source.expected_revision
                            },
                        })
                        .map_err(|error| AgentActionServiceError::EventStore(error.to_string()))?;
                }
            }
            if prepared.goal.criteria.iter().any(|criterion| {
                use harness_contract::goal::AcceptanceStatus;
                !matches!(
                    criterion.status,
                    AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
                ) || (criterion.status == AcceptanceStatus::Waived
                    && !criterion.waiver.as_ref().is_some_and(|waiver| {
                        !waiver.actor.trim().is_empty()
                            && !waiver.reason.trim().is_empty()
                            && !waiver.permission_receipt.trim().is_empty()
                    }))
            }) {
                return Err(AgentActionServiceError::Corrupt(
                    "prepared acceptance status cannot bypass required review or waiver".into(),
                ));
            }
            let conditions = super::review_evidence::required_review_conditions(&prepared.goal);
            let mut used_verified_proofs = vec![false; prepared.verified_proofs().len()];
            for condition in conditions {
                let review = prepared.goal.reviews.iter().rev()
                    .find(|review| review.criterion_ref == condition)
                    .ok_or_else(|| AgentActionServiceError::Corrupt(
                        "required review disappeared before A".into(),
                    ))?;
                let proof = review.verification.as_ref().ok_or_else(||
                    AgentActionServiceError::Corrupt(format!(
                        "required review {condition} has no verification"
                    )))?;
                let index = prepared.verified_proofs().iter().enumerate()
                    .find_map(|(index, candidate)|
                        (!used_verified_proofs[index] && candidate.condition() == condition
                            && candidate.proof().matches_proof(proof)).then_some(index))
                    .ok_or_else(|| AgentActionServiceError::Corrupt(
                        "conclusion proof missing or exhausted".into(),
                    ))?;
                used_verified_proofs[index] = true;
            }
            if used_verified_proofs.iter().any(|used| !used) {
                return Err(AgentActionServiceError::Corrupt(
                    "conclusion has an unused authenticated proof".into(),
                ));
            }
            let stored_goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
                .projection(&prepared.goal.id)
                .map_err(AgentActionServiceError::EventStore)?
                .ok_or_else(|| {
                    AgentActionServiceError::Corrupt("prepared Goal disappeared before A".into())
                })?;
            current_sources
                .verify_goal(&stored_goal)
                .map_err(|error| AgentActionServiceError::Corrupt(error.to_string()))?;
            let terminal = prepared.goal.terminal.as_ref().ok_or_else(|| {
                AgentActionServiceError::Corrupt("Satisfied A has no terminal".into())
            })?;
            if stored_goal.stream_revision != prepared.expected_stream_revision
                || stored_goal.goal.scope != prepared.goal.scope
                || stored_goal.goal.session_id != prepared.goal.session_id
                || stored_goal.goal.objective != prepared.goal.objective
                || stored_goal.goal.spec_revision != prepared.goal.spec_revision
                || stored_goal.goal.spec_digest != prepared.goal.spec_digest
                || stored_goal.goal.user_sequence != prepared.goal.user_sequence
                || stored_goal.goal.reviews != prepared.goal.reviews
                || stored_goal.goal.obligations != prepared.goal.obligations
                || stored_goal.goal.execution_binding != prepared.goal.execution_binding
                || stored_goal.goal.user_intent_criterion_id
                    != prepared.goal.user_intent_criterion_id
                || prepared.goal.user_intent_criterion_id.is_none()
                || crate::execution_core::goal::goal_spec_digest(&stored_goal.goal)
                    != crate::execution_core::goal::goal_spec_digest(&prepared.goal)
                || prepared.goal.revision != stored_goal.goal.revision.saturating_add(1)
                || terminal.kind != harness_contract::goal::ObjectiveTerminalKind::Satisfied
                || terminal.authority_revision != request.program_revision
                || terminal.terminal_fence
                    != format!(
                        "agentic-objective:{}:request:{}",
                        projection.program_id, request.program_revision
                    )
                || prepared.event.event.stream_id != format!("goal:{}", prepared.goal.id)
                || prepared.event.event.payload["goal"] != serde_json::to_value(&prepared.goal)?
            {
                return Err(AgentActionServiceError::Corrupt(
                    "prepared Goal, review or request identity changed before A".into(),
                ));
            }
            prepared.source_frame = current_sources;
        }
        let request = projection.completion_request.as_ref();
        let conclusion_revision = if let Some(trigger) = prepared.runtime_trigger.as_ref() {
            let goals = crate::execution_core::GoalStore::new(Arc::clone(&self.store));
            let goal_projection = goals
                .projection(&prepared.goal.id)
                .map_err(AgentActionServiceError::Corrupt)?
                .ok_or_else(|| AgentActionServiceError::Corrupt("runtime conclusion Goal missing".into()))?;
            prepared.source_frame.verify_goal(&goal_projection)
                .map_err(|error| AgentActionServiceError::Corrupt(error.to_string()))?;
            let terminal = prepared.goal.terminal.as_ref().ok_or_else(||
                AgentActionServiceError::Corrupt("runtime conclusion is not terminal".into()))?;
            let mut expected = goals.prepare_runtime_program_conclusion(projection, trigger)
                .map_err(AgentActionServiceError::Corrupt)?;
            if let Some(expected_terminal) = expected.goal.terminal.as_mut() {
                expected_terminal.committed_at_ms = terminal.committed_at_ms;
            }
            expected.event.event.payload["goal"] = serde_json::to_value(&expected.goal)?;
            if request.is_some()
                || prepared.rejection_trigger.is_some()
                || prepared.goal != expected.goal
                || prepared.event != expected.event
                || prepared.gaps != expected.gaps
                || prepared.expected_stream_revision != expected.expected_stream_revision
                || !expected.source_frame.sources().all(|source| prepared.source_frame.sources().any(|actual| actual == source))
                || trigger.source != "runtime.program_stall"
                || !goal_projection.observations.contains(trigger)
                || prepared.goal.completion != harness_contract::goal::GoalCompletion::Blocked
                || terminal.kind != harness_contract::goal::ObjectiveTerminalKind::Blocked
                || terminal.authority_revision != projection.revision
                || terminal.terminal_fence != format!("agentic-objective:{}:runtime:{}", projection.program_id, projection.revision)
                || terminal.reason != trigger.summary
                || prepared.event.event.stream_id != format!("goal:{}", prepared.goal.id)
                || prepared.event.event.kind != "goal.blocked"
                || prepared.event.event.payload["goal"] != serde_json::to_value(&prepared.goal)?
            {
                return Err(AgentActionServiceError::Corrupt("invalid runtime conclusion carrier".into()));
            }
            projection.revision
        } else if let Some(action_id) = prepared.rejection_trigger.as_deref() {
            let goals = crate::execution_core::GoalStore::new(Arc::clone(&self.store));
            let current_goal = goals.projection(&prepared.goal.id)
                .map_err(AgentActionServiceError::Corrupt)?
                .ok_or_else(|| AgentActionServiceError::Corrupt("rejection conclusion Goal missing".into()))?;
            prepared.source_frame.verify_goal(&current_goal)
                .map_err(|error| AgentActionServiceError::Corrupt(error.to_string()))?;
            let terminal = prepared.goal.terminal.as_ref().ok_or_else(||
                AgentActionServiceError::Corrupt("rejection conclusion is not terminal".into()))?;
            let mut expected = goals.prepare_rejected_program_conclusion(projection, action_id)
                .map_err(AgentActionServiceError::Corrupt)?;
            if let Some(expected_terminal) = expected.goal.terminal.as_mut() {
                expected_terminal.committed_at_ms = terminal.committed_at_ms;
            }
            expected.event.event.payload["goal"] = serde_json::to_value(&expected.goal)?;
            if request.is_some()
                || prepared.goal != expected.goal
                || prepared.event != expected.event
                || prepared.gaps != expected.gaps
                || prepared.expected_stream_revision != expected.expected_stream_revision
                || !expected.source_frame.sources().all(|source| prepared.source_frame.sources().any(|actual| actual == source))
                || goals.exhausted_completion_rejection(projection).map_err(AgentActionServiceError::Corrupt)?.as_deref() != Some(action_id)
                || prepared.goal.completion != harness_contract::goal::GoalCompletion::Blocked
                || terminal.kind != harness_contract::goal::ObjectiveTerminalKind::Blocked
                || terminal.authority_revision != projection.revision
                || terminal.terminal_fence != format!("agentic-objective:{}:runtime:{}", projection.program_id, projection.revision)
                || prepared.event.event.stream_id != format!("goal:{}", prepared.goal.id)
                || prepared.event.event.kind != "goal.blocked"
                || prepared.event.event.payload["goal"] != serde_json::to_value(&prepared.goal)?
                || prepared.event.event.payload["rejection_action_id"] != action_id
            {
                return Err(AgentActionServiceError::Corrupt("invalid rejection conclusion carrier".into()));
            }
            projection.revision
        } else {
            request.ok_or_else(|| AgentActionServiceError::Corrupt("missing completion request".into()))?.program_revision
        };
        let program_stream_id = program_stream(&projection.program_id);
        let goal_stream_id = prepared.event.event.stream_id.clone();
        let (kind, status, payload) = if let Some(terminal) = prepared.goal.terminal.as_ref() {
            let verdict = super::program::AgenticObjectiveVerdictProjection {
                goal_id: prepared.goal.id.clone(),
                goal_revision: prepared.goal.revision,
                terminal_fence: terminal.terminal_fence.clone(),
                authority_revision: terminal.authority_revision,
                kind: terminal.kind,
            };
            (
                OBJECTIVE_VERDICT_EVENT_KIND,
                if terminal.kind == harness_contract::goal::ObjectiveTerminalKind::Satisfied {
                    "verified"
                } else {
                    "blocked"
                },
                json!({"program_id": projection.program_id, "verdict": verdict}),
            )
        } else {
            (
                COMPLETION_REOPENED_EVENT_KIND,
                "open",
                json!({
                    "program_id": projection.program_id,
                    "request_revision": conclusion_revision,
                    "gaps": prepared.gaps,
                }),
            )
        };
        prepared
            .source_frame
            .pin(ExpectedStreamRevision {
                stream_id: program_stream_id.clone(),
                expected_revision: projection.revision,
            })
            .map_err(|error| AgentActionServiceError::EventStore(error.to_string()))?;
        prepared
            .source_frame
            .pin(ExpectedStreamRevision {
                stream_id: goal_stream_id,
                expected_revision: prepared.expected_stream_revision,
            })
            .map_err(|error| AgentActionServiceError::EventStore(error.to_string()))?;
        let allow_claim_renewal = prepared.runtime_trigger.is_none() && prepared.rejection_trigger.is_none() && prepared.verified_proofs().is_empty();
        let expected_streams = prepared.source_frame.into_sources();
        let transaction = AppendTransactionRequest {
            transaction_id: format!(
                "program-conclusion:{}:{}",
                projection.program_id, conclusion_revision
            ),
            expected_streams,
            events: vec![
                prepared.event,
                RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: program_stream_id,
                        scope: RuntimeEventScope::Program,
                        kind: kind.into(),
                        status: Some(status.into()),
                        actor: Some("runtime.objective_supervisor".into()),
                        refs: vec![RuntimeEventRef {
                            kind: "goal".into(),
                            id: prepared.goal.id,
                        }],
                        payload,
                    },
                    idempotency_key: Some(format!(
                        "program-conclusion:{}",
                        conclusion_revision
                    )),
                    schema_version: 1,
                },
            ],
        };
        let claims = projection
            .tasks
            .iter()
            .filter_map(|(id, task)| {
                (allow_claim_renewal && task.status == AgenticTaskStatus::Claimed).then(|| {
                    Some((
                        id.clone(),
                        (task.claimant.clone()?, task.claim_execution_id.clone()?),
                    ))
                })?
            })
            .collect::<BTreeMap<_, _>>();
        let digest = super::review_evidence::proof_value_digest(&(&transaction, &claims));
        let sources = ProgramConclusionSources {
            digest, program_id: projection.program_id.clone(), claims,
        };
        #[cfg(any(test, feature = "test-fixtures"))]
        run_conclusion_append_hook(&transaction);
        let request = crate::CheckedAppendRequest {
            transaction,
            frame_digest: sources.digest.clone(),
            budget: crate::SourceCheckBudget::default(),
        };
        let fence = owner.claim.fence().map_err(AgentActionServiceError::Corrupt)?;
        let claim = crate::RuntimeOriginalSessionClaim {
            session_id: owner.claim.session_id(),
            ingress_message_id: owner.claim.message_id(),
            turn_id: owner.claim.turn_id(),
            fence: &fence,
            consumed_input_sequence,
        };
        let stream_ids = request.transaction.expected_streams.iter()
            .map(|source| source.stream_id.clone()).collect::<Vec<_>>();
        #[cfg(feature = "test-fixtures")]
        let observed_transaction_id = request.transaction.transaction_id.clone();
        #[cfg(feature = "test-fixtures")]
        let observed_event_keys = request.transaction.events.iter().map(|entry|
            (entry.event.stream_id.clone(), entry.idempotency_key.clone())
        ).collect::<Vec<_>>();
        let append = self.store.with_authenticated_stream_locks(&stream_ids, |held| {
            self.store.append_checked_transaction_cancellable_locked(
                request, &sources, Arc::clone(&owner.control), &claim, held, None, None, None,
                original_proof_custody,
            )
        });
        // This existing conclusion owner retries only backend-proven
        // rollbacks; preserve its original stale/conflict handling.
        let mut confirmed_rollback = false;
        let append = append.map_err(|error| match error {
            RuntimeEventStoreError::CheckedAttemptRolledBack(cause) => {
                confirmed_rollback = true;
                *cause
            },
            other => other,
        });
        #[cfg(feature = "test-fixtures")]
        CONCLUSION_ATTEMPT_OBSERVER.with(|slot| {
            let observer = slot.borrow();
            let Some((target, observation)) = observer.as_ref() else { return; };
            if target != &observed_transaction_id || observation.borrow().is_some() { return; }
            use crate::execution_core::services::{ConclusionAttemptEvent, ConclusionAttemptObservation, ConclusionAttemptOutcome};
            let outcome = match &append {
                Ok(Some(_)) => ConclusionAttemptOutcome::Committed,
                Ok(None) => ConclusionAttemptOutcome::Cancelled,
                Err(RuntimeEventStoreError::StaleRevision { stream_id, expected, actual }) =>
                    ConclusionAttemptOutcome::Stale { stream_id: stream_id.clone(), expected: *expected, actual: *actual },
                Err(RuntimeEventStoreError::TransactionConflict { .. }) => ConclusionAttemptOutcome::Conflict,
                Err(error) => ConclusionAttemptOutcome::Error(error.to_string()),
            };
            let original_receipt = self.store.original_transaction_receipt(&observed_transaction_id)
                .map_err(|error| error.to_string());
            let events = observed_event_keys.iter().map(|(stream_id, key)| {
                let observed = match key {
                    Some(key) => self.store.event_by_idempotency_key(stream_id, key).map_err(|error| error.to_string()),
                    None => Err("original event has no idempotency key".into()),
                };
                ConclusionAttemptEvent { stream_id: stream_id.clone(), idempotency_key: key.clone(), observed }
            }).collect();
            *observation.borrow_mut() = Some(ConclusionAttemptObservation {
                transaction_id: observed_transaction_id.clone(), outcome, original_receipt, events,
            });
        });
        let append = match append {
            Ok(Some(committed)) => {
                if let Some((action_id, member_index, sink)) = original_sink.as_ref() {
                    let (request, receipt, authority) = committed.into_parts();
                    if authority.is_some() {
                        return Err(AgentActionServiceError::MemberUnknown("unexpected conclusion authority owner".into()));
                    }
                    let result = Arc::new(OriginalCommittedConclusion {
                        store: Arc::downgrade(&self.store), request,
                        receipt: OriginalMemberCommitReceipt { commit: receipt.commit, validation: Some(receipt.validation) },
                        action_id: action_id.clone(), member_index: *member_index,
                    });
                    sink.record_original_conclusion(*member_index, result)
                        .map_err(AgentActionServiceError::MemberUnknown)?;
                }
                Ok(())
            },
            Ok(None) => return Err(AgentActionServiceError::MemberCancelled),
            Err(error) => Err(error),
        };
        if let Err(error) = append {
            // The original checked owner retains request and control for an
            // uncertain reply. Read that exact transaction once; absence is
            // still Unknown and never permission to submit again.
            if let Some((action_id, member_index, sink)) = original_sink.as_ref() {
                let transaction_id = format!("program-conclusion:{}:{}", projection.program_id, conclusion_revision);
                if let Ok(Some((request, receipt))) = self.store.recover_retained_conclusion(&transaction_id) {
                    let result = Arc::new(OriginalCommittedConclusion {
                        store: Arc::downgrade(&self.store), request: Arc::clone(&request),
                        receipt: OriginalMemberCommitReceipt { commit: receipt.clone(), validation: None },
                        action_id: action_id.clone(), member_index: *member_index,
                    });
                    sink.record_original_conclusion(*member_index, result).map_err(AgentActionServiceError::MemberUnknown)?;
                    self.store.release_reconciled_conclusion(&request, &receipt)?;
                    return self.project(&projection.program_id);
                }
            }
            // A competing preparation can use the same original request but
            // a different immutable cut or timestamp. Do not reinterpret the
            // hash conflict as success: only the original owner's committed
            // Goal+Program transaction can establish the winner.
            if matches!(&error, RuntimeEventStoreError::TransactionConflict { .. } | RuntimeEventStoreError::StaleRevision { .. }) {
                let winner = self.project(&projection.program_id)?;
                let authenticated = match (&prepared.runtime_trigger, &prepared.rejection_trigger) {
                    (Some(trigger), None) => self.authenticated_existing_trigger_conclusion(
                        &winner, NonRequestConclusionTrigger::Runtime(trigger),
                    )?,
                    (None, Some(action_id)) => self.authenticated_existing_trigger_conclusion(
                        &winner, NonRequestConclusionTrigger::Rejection(action_id),
                    )?,
                    (None, None) => self.authenticated_existing_request_conclusion(projection)?,
                    (Some(_), Some(_)) => false,
                };
                if authenticated { return Ok(winner); }
            }
            return if confirmed_rollback {
                Err(error.into())
            } else {
                Err(AgentActionServiceError::MemberUnknown(error.to_string()))
            };
        }
        self.project(&projection.program_id)
    }

    /// Read back only an exact original same-trigger Goal+Program winner.
    /// Missing, displaced or differently triggered facts remain Unknown.
    /// Discover a non-request trigger only from the original committed Goal
    /// event. The candidate still goes through full same-owner verification.
    pub(crate) fn authenticated_existing_nonrequest_conclusion(
        &self,
        current: &AgenticProgramProjection,
    ) -> Result<bool, AgentActionServiceError> {
        let Some(root) = current.root_execution_id.as_deref() else { return Ok(false); };
        let Some(verdict) = current.objective_verdict.as_ref() else { return Ok(false); };
        let goal_stream = format!("goal:goal:{root}");
        let key = format!("program-conclusion:agentic-objective:{}:runtime:{}",
            current.program_id, verdict.authority_revision);
        let Some(goal_event) = self.store.event_by_idempotency_key(&goal_stream, &key)? else {
            return Ok(false);
        };
        let rejection = goal_event.payload.get("rejection_action_id").and_then(serde_json::Value::as_str);
        let runtime = goal_event.payload.get("runtime_trigger").and_then(serde_json::Value::as_str);
        match (rejection, runtime) {
            (Some(action_id), None) if !action_id.is_empty() =>
                self.authenticated_existing_trigger_conclusion(current, NonRequestConclusionTrigger::Rejection(action_id)),
            (None, Some(fingerprint)) if !fingerprint.is_empty() => {
                let Some(record) = self.store.event_by_idempotency_key(&goal_stream, &format!("direct:{fingerprint}"))? else {
                    return Ok(false);
                };
                let value = self.store.resolve_observation_record(&record).map_err(AgentActionServiceError::EventStore)?;
                let observation: harness_contract::goal::RuntimeObservation = serde_json::from_value(value)?;
                if observation.idempotency_fingerprint() != fingerprint { return Ok(false); }
                self.authenticated_existing_trigger_conclusion(current, NonRequestConclusionTrigger::Runtime(&observation))
            }
            _ => Ok(false),
        }
    }

    /// Discover only a candidate trigger identity from the committed Goal
    /// carrier, then authenticate it through the original transaction owner.
    pub(crate) fn authenticated_existing_rejection_conclusion(
        &self,
        current: &AgenticProgramProjection,
    ) -> Result<bool, AgentActionServiceError> {
        let Some(root) = current.root_execution_id.as_deref() else { return Ok(false); };
        let Some(verdict) = current.objective_verdict.as_ref() else { return Ok(false); };
        let key = format!("program-conclusion:agentic-objective:{}:runtime:{}",
            current.program_id, verdict.authority_revision);
        let Some(goal_event) = self.store.event_by_idempotency_key(&format!("goal:goal:{root}"), &key)? else {
            return Ok(false);
        };
        let Some(action_id) = goal_event.payload.get("rejection_action_id")
            .and_then(serde_json::Value::as_str).filter(|value| !value.is_empty()) else { return Ok(false); };
        self.authenticated_existing_trigger_conclusion(current, NonRequestConclusionTrigger::Rejection(action_id))
    }

    pub(crate) fn authenticated_existing_trigger_conclusion(
        &self,
        current: &AgenticProgramProjection,
        trigger: NonRequestConclusionTrigger<'_>,
    ) -> Result<bool, AgentActionServiceError> {
        use harness_contract::goal::{GoalCompletion, GoalContract, ObjectiveTerminalKind};
        let Some(root) = current.root_execution_id.as_deref() else { return Ok(false); };
        let Some(verdict) = current.objective_verdict.as_ref() else { return Ok(false); };
        if verdict.kind != ObjectiveTerminalKind::Blocked { return Ok(false); }
        let revision = verdict.authority_revision;
        let program_stream = program_stream(&current.program_id);
        let goal_id = format!("goal:{root}");
        let goal_stream = format!("goal:{goal_id}");
        let transaction = format!("program-conclusion:{}:{revision}", current.program_id);
        let fence = format!("agentic-objective:{}:runtime:{revision}", current.program_id);
        let Some(program_event) = self.store.event_by_idempotency_key(
            &program_stream, &format!("program-conclusion:{revision}"),
        )? else { return Ok(false); };
        let Some(goal_event) = self.store.event_by_idempotency_key(
            &goal_stream, &format!("program-conclusion:{fence}"),
        )? else { return Ok(false); };
        let Some(program_cut) = self.store.transaction_stream_revision_exact(&transaction, &program_stream)? else { return Ok(false); };
        let Some(goal_cut) = self.store.transaction_stream_revision_exact(&transaction, &goal_stream)? else { return Ok(false); };
        if program_cut.expected_revision != revision || program_cut.committed_revision != program_event.sequence
            || program_cut.expected_revision.checked_add(1) != Some(program_cut.committed_revision)
            || goal_cut.expected_revision.checked_add(1) != Some(goal_cut.committed_revision)
            || goal_event.sequence != goal_cut.committed_revision
            || goal_event.transaction_id != transaction || program_event.transaction_id != transaction
            || goal_event.transaction_index != 0 || program_event.transaction_index != 1
            || goal_event.commit_cursor == 0 || goal_event.commit_cursor != program_event.commit_cursor
            || goal_event.scope != RuntimeEventScope::Goal || goal_event.schema_version != 1
            || goal_event.kind != "goal.blocked" || goal_event.status.as_deref() != Some("blocked")
            || goal_event.actor.as_deref() != Some("runtime.objective_supervisor")
            || goal_event.refs != vec![RuntimeEventRef { kind: "program".into(), id: current.program_id.clone() }]
            || program_event.scope != RuntimeEventScope::Program || program_event.schema_version != 1
            || program_event.kind != OBJECTIVE_VERDICT_EVENT_KIND || program_event.status.as_deref() != Some("blocked")
            || program_event.actor.as_deref() != Some("runtime.objective_supervisor")
            || program_event.refs != vec![RuntimeEventRef { kind: "goal".into(), id: goal_id.clone() }]
            || program_event.payload != json!({"program_id":current.program_id,"verdict":verdict}) {
            return Ok(false);
        }
        let goal: GoalContract = serde_json::from_value(goal_event.payload["goal"].clone())?;
        let Some(terminal) = goal.terminal.as_ref() else { return Ok(false); };
        if goal.id != goal_id || goal.session_id != current.session_id || goal.completion != GoalCompletion::Blocked
            || goal.revision != verdict.goal_revision || terminal.kind != ObjectiveTerminalKind::Blocked
            || terminal.terminal_fence != fence || terminal.authority_revision != revision
            || verdict.goal_id != goal_id || verdict.terminal_fence != fence
            || goal.execution_binding.as_ref().is_none_or(|binding|
                binding.objective_id != current.objective_id || binding.session_id != current.session_id
                || binding.turn_id != current.turn_id || binding.root_execution_id != root
                || binding.agentic_program_id != current.program_id) {
            return Ok(false);
        }
        match trigger {
            NonRequestConclusionTrigger::Runtime(observation) => {
                if observation.source != "runtime.program_stall" || observation.goal_id() != goal_id
                    || observation.identity.graph_id != root || observation.source_revision != revision
                    || observation.result_class != harness_contract::goal::ObservationResultClass::Failed
                    || terminal.reason != observation.summary
                    || goal_event.payload != json!({"goal":goal,"runtime_trigger":observation.idempotency_fingerprint()}) {
                    return Ok(false);
                }
                let Some(original) = self.store.event_by_idempotency_key(
                    &goal_stream, &format!("direct:{}", observation.idempotency_fingerprint()),
                )? else { return Ok(false); };
                if original.sequence == 0 || original.sequence > goal_cut.expected_revision
                    || original.commit_cursor == 0 || original.commit_cursor >= goal_event.commit_cursor
                    || serde_json::from_value::<harness_contract::goal::RuntimeObservation>(
                        self.store.resolve_observation_record(&original).map_err(AgentActionServiceError::EventStore)?,
                    )? != *observation { return Ok(false); }
            }
            NonRequestConclusionTrigger::Rejection(action_id) => {
                if action_id.is_empty() || goal_event.payload != json!({"goal":goal,"rejection_action_id":action_id}) {
                    return Ok(false);
                }
                let Some(original) = self.store.event_by_idempotency_key(
                    &program_stream, &format!("completion-rejected:{action_id}"),
                )? else { return Ok(false); };
                let envelope: AgentActionEnvelope = serde_json::from_value(original.payload["envelope"].clone())?;
                let Some(code) = original.payload["code"].as_str().filter(|value| !value.is_empty()) else { return Ok(false); };
                let Some(gap) = original.payload["gap"].as_str() else { return Ok(false); };
                let reason = format!("The configured completion resource bound was exhausted by the same durable rejection without verified progress; the objective remains unmet: {code}: {gap}");
                if envelope.action_id != action_id || envelope.actor.program_id != current.program_id
                    || envelope.actor.session_id != current.session_id || envelope.actor.turn_id != current.turn_id
                    || envelope.actor.objective_id != current.objective_id || envelope.actor.root_execution_id != current.root_execution_id
                    || !matches!(envelope.action, AgentAction::ObjectiveCompleteRequest(_))
                    || original.payload != json!({"program_id":current.program_id,"request_action_id":action_id,
                        "code":code,"objective_id":current.objective_id,"action":envelope.action,"envelope":envelope,"gap":gap})
                    || terminal.reason != reason || original.sequence > revision
                    || original.commit_cursor >= goal_event.commit_cursor
                    || !self.completion_rejection_replay_is_fenced(&original, &envelope)? {
                    return Ok(false);
                }
            }
        }
        let current_goal = crate::execution_core::GoalStore::new(Arc::clone(&self.store))
            .get(&goal_id).map_err(AgentActionServiceError::EventStore)?;
        Ok(current_goal.as_ref() == Some(&goal))
    }

    pub(crate) fn authenticated_existing_request_conclusion(
        &self,
        original: &AgenticProgramProjection,
    ) -> Result<bool, AgentActionServiceError> {
        let Some(request) = original.completion_request.as_ref() else { return Ok(false); };
        let Some(root) = original.root_execution_id.as_deref() else { return Ok(false); };
        let program_stream = program_stream(&original.program_id);
        let goal_id = format!("goal:{root}");
        let goal_stream = format!("goal:{goal_id}");
        let transaction = format!("program-conclusion:{}:{}", original.program_id, request.program_revision);
        let key = format!("program-conclusion:{}", request.program_revision);
        let Some(program_event) = self.store.event_by_idempotency_key(&program_stream, &key)? else { return Ok(false); };
        let Some(request_event) = self.store.event_by_idempotency_key(&program_stream, &action_key(&request.action_id))? else { return Ok(false); };
        let envelope: AgentActionEnvelope = serde_json::from_value(request_event.payload["envelope"].clone())?;
        let AgentAction::ObjectiveCompleteRequest(input) = &envelope.action else { return Ok(false); };
        let Some(opened) = self.store.event_by_idempotency_key(&program_stream, "agentic-program-opened")? else { return Ok(false); };
        let at_request = self.project_snapshot_bounded(
            &original.program_id, request.program_revision, &opened, None,
            |after, through| self.store.list_stream_after(&program_stream, after, through, 100, 1024 * 1024)
                .map_err(AgentActionServiceError::EventStore),
            |error| error,
        )?;
        if request_event.kind != ACTION_EVENT_KIND || request_event.scope != RuntimeEventScope::Program
            || request_event.status.as_deref() != Some("applied") || request_event.schema_version != 1
            || request_event.actor.as_deref() != Some(envelope.actor.actor_id.as_str())
            || request_event.sequence != request.program_revision
            || envelope.action_id != request.action_id || envelope.actor.actor_id != request.requested_by
            || envelope.actor.program_id != original.program_id || envelope.actor.objective_id != original.objective_id
            || envelope.actor.session_id != original.session_id || envelope.actor.turn_id != original.turn_id
            || envelope.actor.root_execution_id != original.root_execution_id
            || request.result_refs != input.result_refs || request.evidence_refs != input.evidence_refs
            || request.unresolved != input.unresolved || at_request.completion_request.as_ref() != Some(request) {
            return Ok(false);
        }
        let Some(program_fence) = self.store.transaction_stream_revision_exact(&transaction, &program_stream)? else { return Ok(false); };
        let Some(goal_fence) = self.store.transaction_stream_revision_exact(&transaction, &goal_stream)? else { return Ok(false); };
        if program_fence.expected_revision < request.program_revision
            || program_fence.committed_revision != program_event.sequence
            || program_fence.expected_revision >= program_event.sequence
            || goal_fence.expected_revision.checked_add(1) != Some(goal_fence.committed_revision)
            || program_event.transaction_id != transaction || program_event.transaction_index != 1
            || program_event.scope != RuntimeEventScope::Program || program_event.schema_version != 1
            || program_event.actor.as_deref() != Some("runtime.objective_supervisor")
            || program_event.refs != vec![RuntimeEventRef { kind:"goal".into(), id:goal_id.clone() }] {
            return Ok(false);
        }
        let tail_head = program_event.sequence - 1;
        if tail_head != program_fence.expected_revision {
            let tail = self.store.list_stream_after(&program_stream, program_fence.expected_revision,
                tail_head, 100, 1024 * 1024).map_err(AgentActionServiceError::EventStore)?;
            let claims = original.tasks.iter().filter_map(|(id, task)| {
                (task.status == AgenticTaskStatus::Claimed).then(|| {
                    Some((id.clone(), (task.claimant.clone()?, task.claim_execution_id.clone()?)))
                })?
            }).collect::<BTreeMap<_, _>>();
            let validator = ProgramConclusionSources { digest:String::new(), program_id:original.program_id.clone(), claims };
            if crate::CheckedSourceValidator::validate_tail(&validator, &ExpectedStreamRevision {
                stream_id:program_stream.clone(), expected_revision:program_fence.expected_revision,
            }, tail_head, &tail).is_err() {
                return Ok(false);
            }
        }
        let goal_events = self.store.list_stream_after(&goal_stream, goal_fence.expected_revision,
            goal_fence.committed_revision, 1, 4 * 1024 * 1024).map_err(AgentActionServiceError::EventStore)?;
        let Some(goal_event) = goal_events.first().filter(|_| goal_events.len() == 1) else { return Ok(false); };
        if goal_event.transaction_id != transaction || goal_event.transaction_index != 0
            || goal_event.commit_cursor != program_event.commit_cursor || goal_event.schema_version != 1
            || goal_event.scope != RuntimeEventScope::Goal
            || goal_event.actor.as_deref() != Some("runtime.objective_supervisor")
            || goal_event.refs != vec![RuntimeEventRef { kind:"program".into(), id:original.program_id.clone() }]
            || goal_event.payload["request_revision"] != request.program_revision {
            return Ok(false);
        }
        let goal: harness_contract::goal::GoalContract = serde_json::from_value(goal_event.payload["goal"].clone())?;
        if goal.id != goal_id || goal.session_id != original.session_id
            || goal_event.payload != json!({"goal":goal,"request_revision":request.program_revision})
            || goal.execution_binding.as_ref().is_none_or(|binding| binding.objective_id != original.objective_id
                || binding.session_id != original.session_id || binding.turn_id != original.turn_id
                || binding.root_execution_id != root || binding.agentic_program_id != original.program_id) {
            return Ok(false);
        }
        if let Some(terminal) = goal.terminal.as_ref() {
            let valid_terminal = matches!((terminal.kind, goal.completion, goal_event.kind.as_str(), goal_event.status.as_deref()),
                (harness_contract::goal::ObjectiveTerminalKind::Satisfied, harness_contract::goal::GoalCompletion::Satisfied, "goal.completed", Some("satisfied"))
                | (harness_contract::goal::ObjectiveTerminalKind::Blocked, harness_contract::goal::GoalCompletion::Blocked, "goal.blocked", Some("blocked")));
            let verdict = super::program::AgenticObjectiveVerdictProjection { goal_id:goal.id.clone(), goal_revision:goal.revision,
                terminal_fence:terminal.terminal_fence.clone(), authority_revision:terminal.authority_revision, kind:terminal.kind };
            let expected_fence = format!("agentic-objective:{}:request:{}", original.program_id, request.program_revision);
            if !valid_terminal || goal_event.idempotency_key.as_deref() != Some(format!("program-conclusion:{expected_fence}").as_str())
                || terminal.terminal_fence != expected_fence || terminal.authority_revision != request.program_revision
                || program_event.kind != OBJECTIVE_VERDICT_EVENT_KIND
                || program_event.status.as_deref() != Some(if terminal.kind == harness_contract::goal::ObjectiveTerminalKind::Satisfied { "verified" } else { "blocked" })
                || program_event.payload != json!({"program_id":original.program_id,"verdict":verdict}) {
                return Ok(false);
            }
        } else if goal_event.kind != "goal.completion_waiting" || goal_event.status.as_deref() != Some("open")
            || goal_event.idempotency_key.as_deref() != Some(format!("program-conclusion:agentic-objective:{}:request:{}", original.program_id, request.program_revision).as_str())
            || program_event.kind != COMPLETION_REOPENED_EVENT_KIND || program_event.status.as_deref() != Some("open")
            || goal.completion != harness_contract::goal::GoalCompletion::Open
            || goal.waiting.as_ref().is_none_or(|waiting| waiting.reason_code != "completion_gap"
                || program_event.payload != json!({"program_id":original.program_id,
                    "request_revision":request.program_revision,"gaps":waiting.required_refs})) {
            return Ok(false);
        }
        let current = self.project(&original.program_id)?;
        if current.revision < program_event.sequence || current.session_id != original.session_id
            || current.turn_id != original.turn_id || current.objective_id != original.objective_id
            || current.root_execution_id != original.root_execution_id {
            return Ok(false);
        }
        if program_event.kind == OBJECTIVE_VERDICT_EVENT_KIND {
            let committed: super::program::AgenticObjectiveVerdictProjection =
                serde_json::from_value(program_event.payload["verdict"].clone())?;
            if current.objective_verdict.as_ref() != Some(&committed) { return Ok(false); }
        }
        self.publish_retained_conclusion_winner(original)?;
        Ok(true)
    }

    /// Resolve the already-frozen Program authority without treating a
    /// not-yet-opened Program as corruption. Root model continuations use
    /// this to inherit the first admitted binding instead of rebuilding
    /// policy and evidence scope from a later replan node.
    pub fn project_if_exists(
        &self,
        program_id: &str,
    ) -> Result<Option<AgenticProgramProjection>, AgentActionServiceError> {
        let stream_id = program_stream(program_id);
        if self.store.stream_revision(&stream_id)? == 0 {
            return Ok(None);
        }
        self.project(program_id).map(Some)
    }

    /// Enumerate every durable Agentic Program from the authoritative event
    /// store. This is the only supported cross-program carrier/projection
    /// source; callers must not reconstruct Team state from graph payloads.
    pub fn list_programs(&self) -> Result<Vec<AgenticProgramProjection>, AgentActionServiceError> {
        let mut programs = self
            .store
            .stream_ids_for_scope(RuntimeEventScope::Program)?
            .into_iter()
            .filter_map(|stream| stream.strip_prefix("agentic-program:").map(str::to_string))
            .filter_map(|program_id| self.project_if_exists(&program_id).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        programs.sort_by(|left, right| left.program_id.cmp(&right.program_id));
        Ok(programs)
    }

    /// Compact model-facing working set. Mutations return a summary rather
    /// than re-injecting the entire growing Program on every action;
    /// `state_inspect` can request one concrete Team/Agent/Task/Topic/Artifact
    /// or the whole projection when the model genuinely needs it.
    #[must_use]
    pub fn compact_projection(
        projection: &AgenticProgramProjection,
        scope_ref: Option<&str>,
    ) -> serde_json::Value {
        if let Some(scope_ref) = scope_ref {
            if let Some(team) = projection.teams.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "team": team,
                    "agents": team.member_ids.iter().take(STATE_INSPECT_PAGE_SIZE).filter_map(|id| projection.agents.get(id)).collect::<Vec<_>>(),
                    "tasks": team.task_ids.iter().take(STATE_INSPECT_PAGE_SIZE).filter_map(|id| projection.tasks.get(id)).collect::<Vec<_>>(),
                    "topics": projection.topics.get(&team.topic_ref).map(|entries| entries.iter().rev().take(STATE_INSPECT_PAGE_SIZE).collect::<Vec<_>>()),
                    "directory_request":{"query":team.team_id},
                    "coverage":{"nested_lists":"bounded_previews","complete":false,"instruction":"use directory_request for all related entries"},
                });
            }
            if let Some(agent) = projection.agents.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "agent": agent,
                });
            }
            if let Some(task) = projection.tasks.get(scope_ref) {
                let artifacts = task
                    .artifact_refs
                    .iter()
                    .filter_map(|id| projection.artifacts.get(id))
                    .collect::<Vec<_>>();
                // A reviewer must be able to read the submitted deliverable
                // without parsing nested artifact JSON or rebuilding an opaque
                // content hash. Hand back ready-to-read content references, so
                // `read("task://X")` is immediately followed by `read(<ref>)`.
                let read_requests = artifacts
                    .iter()
                    .map(|artifact| {
                        json!({
                            "artifact_ref": artifact.artifact_ref,
                            "read_request": { "evidence_ref": artifact.content_ref },
                        })
                    })
                    .collect::<Vec<_>>();
                let read_request = artifacts
                    .first()
                    .map(|artifact| json!({ "evidence_ref": artifact.content_ref }));
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "task": task,
                    "issues": super::issues::issues(projection).into_iter().filter(|issue| issue.source_ref == task.task_id).collect::<Vec<_>>(),
                    "artifacts": artifacts,
                    "read_requests": read_requests,
                    "read_request": read_request,
                });
            }
            if let Some(artifact) = projection.artifacts.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "artifact": artifact,
                    "read_request": { "evidence_ref": artifact.content_ref },
                });
            }
            if let Some(entries) = projection.topics.get(scope_ref) {
                return json!({
                    "program_id": projection.program_id,
                    "revision": projection.revision,
                    "topic_ref": scope_ref,
                    "entries": entries.iter().rev().take(STATE_INSPECT_PAGE_SIZE).collect::<Vec<_>>(),
                    "directory_request":{"query":scope_ref},
                    "coverage":{"loaded":entries.len().min(STATE_INSPECT_PAGE_SIZE),"total":entries.len(),"complete":entries.len()<=STATE_INSPECT_PAGE_SIZE},
                });
            }
        }
        serde_json::to_value(projection).unwrap_or_else(
            |_| json!({"program_id": projection.program_id, "revision": projection.revision}),
        )
    }
}

// Missing is the legacy event format. Never
// reinterpret a malformed recorded lease as a legacy event.
fn recorded_claim_lease_ms(
    payload: &serde_json::Value,
) -> Result<Option<u64>, AgentActionServiceError> {
    match payload.get("claim_lease_ms") {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .filter(|value| *value > 0)
            .map(Some)
            .ok_or_else(|| {
                AgentActionServiceError::Corrupt("invalid recorded claim lease duration".into())
            }),
    }
}

fn projection_from_opened(
    program_id: &str,
    opened: &crate::DurableRuntimeEvent,
) -> Result<AgenticProgramProjection, AgentActionServiceError> {
    let objective_id = opened
        .payload
        .get("objective_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            AgentActionServiceError::Corrupt("program_opened_missing_objective".to_string())
        })?;
    let mut projection = AgenticProgramProjection::empty(program_id, objective_id);
    projection.session_id = opened
        .payload
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    projection.turn_id = opened
        .payload
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    projection.root_execution_id = opened
        .payload
        .get("root_execution_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    // Absence is the explicit historical schema. A malformed present field
    // must not silently erase an exact ceiling or impersonate that schema.
    projection.required_team_count = serde_json::from_value(
        opened
            .payload
            .get("required_team_count")
            .cloned()
            .unwrap_or(json!(0)),
    )?;
    projection.team_count_contract_version = serde_json::from_value(
        opened
            .payload
            .get("team_count_contract_version")
            .cloned()
            .unwrap_or(json!(0)),
    )?;
    projection.exact_team_count = serde_json::from_value(
        opened
            .payload
            .get("exact_team_count")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )?;
    projection.objective_summary = opened
        .payload
        .get("objective_summary")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    projection.model_lease = opened
        .payload
        .get("model_lease")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("default")
        .to_string();
    projection.permission_ceiling = opened
        .payload
        .get("permission_ceiling")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or(harness_contract::policy::PermissionMode::ReadOnly);
    projection.resource_scopes = opened
        .payload
        .get("resource_scopes")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    Ok(projection)
}

/// The named failures the Program's reviews settled, read from the durable
/// applied events.
///
/// One helper, two callers: the rebuild path and the catch-up path read the
/// same durable facts the same way, so a cached projection and a rebuilt one
/// carry the same entries. An entry is kept once: the same named failure is
/// never listed twice.
fn merge_required_write_failures(
    projection: &mut AgenticProgramProjection,
    events: &[crate::DurableRuntimeEvent],
) {
    for event in events {
        let Some(found) = event
            .payload
            .get("required_write_failures")
            .and_then(|entries| entries.as_array())
        else {
            continue;
        };
        for entry in found {
            let name = entry.get("name").and_then(|name| name.as_str()).unwrap_or_default();
            if projection.required_write_failures.iter().any(|existing| {
                existing.get("name").and_then(|name| name.as_str()) == Some(name)
            }) {
                continue;
            }
            projection.required_write_failures.push(entry.clone());
        }
    }
}

// Reuse the Program reducer at the original source revision to reconstruct
// only the missing derived field from legacy checkpoints/continuation seeds.
fn rebuild_rework_rounds(
    store: &RuntimeEventStore,
    program_id: &str,
    through: u64,
    stack: &mut Vec<String>,
) -> Result<std::collections::BTreeMap<String, u8>, AgentActionServiceError> {
    if through == 0 || stack.iter().any(|id| id == program_id) {
        return Err(AgentActionServiceError::Corrupt("invalid review history source or continuation cycle".into()));
    }
    stack.push(program_id.to_string());
    let result = (|| {
        let stream = program_stream(program_id);
        let mut after = 0u64;
        let mut rebuilt: Option<AgenticProgramProjection> = None;
        while after < through {
            let mut page = store.list_stream_after(&stream, after, through, 100, 1024 * 1024)
                .map_err(AgentActionServiceError::EventStore)?;
            if page.is_empty() {
                return Err(AgentActionServiceError::MemberUnknown(format!("review_rework_history_unknown: missing {stream} after {after} through {through}")));
            }
            for event in &mut page {
                after = after.checked_add(1).ok_or_else(|| AgentActionServiceError::Corrupt("review history sequence overflow".into()))?;
                if event.stream_id != stream || event.sequence != after || event.schema_version != 1 || after > through {
                    return Err(AgentActionServiceError::Corrupt(format!("invalid review history {stream} sequence {after}")));
                }
                if rebuilt.is_none() {
                    if after != 1 || event.kind != PROGRAM_OPENED_EVENT_KIND {
                        return Err(AgentActionServiceError::Corrupt(format!("review history missing opening for {stream}")));
                    }
                    if let Some(seed) = event.payload.get_mut("continuation_seed") {
                        let inherited: AgenticProgramProjection = serde_json::from_value(seed.clone())
                            .map_err(|error| AgentActionServiceError::Corrupt(format!("invalid continuation seed: {error}")))?;
                        if inherited.applied_rework_rounds.is_none() {
                            let source = inherited.continuation.as_ref().ok_or_else(|| AgentActionServiceError::Corrupt("review history continuation source missing".into()))?;
                            let rounds = rebuild_rework_rounds(store, &source.source_program_id, source.source_revision, stack)?;
                            if inherited.tasks.keys().any(|task| !rounds.contains_key(task)) {
                                return Err(AgentActionServiceError::Corrupt("review history continuation task missing".into()));
                            }
                            // This changes only the in-memory replay copy; the
                            // original persisted continuation event is untouched.
                            seed["applied_rework_rounds"] = serde_json::to_value(rounds)
                                .map_err(|error| AgentActionServiceError::Corrupt(format!("cannot rebuild continuation count: {error}")))?;
                        }
                    }
                    rebuilt = Some(projection_from_opened(program_id, event)
                        .map_err(|error| AgentActionServiceError::Corrupt(format!("invalid Program opening: {error}")))?);
                }
            }
            let projection = rebuilt.as_mut().ok_or_else(|| AgentActionServiceError::Corrupt("review history opening unavailable".into()))?;
            apply_projection_events(store, projection, page)?;
            if projection.revision != after {
                return Err(AgentActionServiceError::Corrupt("incomplete review history fold".into()));
            }
        }
        rebuilt.and_then(|projection| projection.applied_rework_rounds)
            .ok_or_else(|| AgentActionServiceError::MemberUnknown("review_rework_history_unknown: derived count missing".into()))
    })();
    stack.pop();
    result
}

fn apply_projection_events(
    store: &RuntimeEventStore,
    projection: &mut AgenticProgramProjection,
    events: Vec<crate::DurableRuntimeEvent>,
) -> Result<(), AgentActionServiceError> {
    // The same durable facts, read the same way: the catch-up path carries
    // the named failures the rebuild path carries.
    merge_required_write_failures(projection, &events);
    for event in events {
        if event.sequence <= projection.revision {
            continue;
        }
        if event.kind == PROGRAM_OPENED_EVENT_KIND {
            if let Some(seed) = event.payload.get("continuation_seed") {
                let inherited: AgenticProgramProjection = serde_json::from_value(seed.clone())?;
                if inherited.program_id != projection.program_id
                    || inherited.objective_id != projection.objective_id
                    || inherited.continuation.is_none()
                    || event.sequence != 1
                {
                    return Err(AgentActionServiceError::Corrupt(
                        "invalid continuation seed".into(),
                    ));
                }
                *projection = inherited;
            }
            projection.revision = event.sequence;
            continue;
        }
        if event.kind == REVIEW_DISCIPLINE_CONFLICT_EVENT_KIND {
            // Derived, never stored: `program_warnings` is a fold of the durable
            // journal, exactly like the authoring-side warning. A reviewer that
            // tried a reading the operator ruled out stays visible for the rest of
            // the Program's life, so the root can weigh later rework accordingly
            // and an operator can see why work kept going back and forth.
            let task_ref = event
                .payload
                .get("task_ref")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let discipline = event
                .payload
                .get("discipline")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if !task_ref.is_empty() && !discipline.is_empty() {
                let warning = format!(
                    "Historical review discipline refusal for Task `{task_ref}`: \
                     {discipline}. This legacy diagnostic is not a current review rule."
                );
                if !projection.program_warnings.contains(&warning) {
                    projection.program_warnings.push(warning);
                }
            }
            projection.revision = event.sequence;
            continue;
        }
        if event.kind == OBJECTIVE_VERDICT_EVENT_KIND {
            let verdict =
                serde_json::from_value::<super::program::AgenticObjectiveVerdictProjection>(
                    event.payload.get("verdict").cloned().ok_or_else(|| {
                        AgentActionServiceError::Corrupt(
                            "objective_verdict_event_missing_verdict".to_string(),
                        )
                    })?,
                )?;
            projection.apply_objective_verdict(verdict, event.sequence);
            continue;
        }
        if event.kind == COMPLETION_REOPENED_EVENT_KIND {
            let request_revision = event
                .payload
                .get("request_revision")
                .and_then(serde_json::Value::as_u64);
            if projection
                .completion_request
                .as_ref()
                .map(|request| request.program_revision)
                != request_revision
            {
                return Err(AgentActionServiceError::Corrupt(
                    "completion reopen request fence mismatch".into(),
                ));
            }
            projection.status = super::program::AgenticProgramStatus::Open;
            projection.completion_request = None;
            projection.final_artifact_ref = None;
            // The reopen event records what THIS request was missing at THIS
            // revision. That is history: the model may have closed every one of
            // those gaps a revision later. Keep the record for audit and let the
            // caller supply the gaps that still hold, so a satisfied review
            // clears the blockage instead of being contradicted forever.
            projection.last_reopen_gaps = serde_json::from_value(
                event
                    .payload
                    .get("gaps")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            )?;
            // Current Goal gaps are an explicit assessment, never a read-time Program mutation.
            projection.unresolved = Vec::new();
            projection.revision = event.sequence;
            continue;
        }
        if event.kind == TASK_LEASE_RENEWED_EVENT_KIND {
            let task_ref = event
                .payload
                .get("task_ref")
                .and_then(serde_json::Value::as_str);
            let agent_id = event
                .payload
                .get("agent_id")
                .and_then(serde_json::Value::as_str);
            let execution_id = event
                .payload
                .get("execution_id")
                .and_then(serde_json::Value::as_str);
            if let Some(task) = task_ref.and_then(|task_ref| projection.tasks.get_mut(task_ref)) {
                let same_owner = task.status == AgenticTaskStatus::Claimed
                    && task.claimant.as_deref() == agent_id
                    && task.claim_execution_id.as_deref() == execution_id;
                if same_owner {
                    task.lease_expires_at_ms = Some(
                        event.created_at_ms.saturating_add(
                            recorded_claim_lease_ms(&event.payload)?
                                .unwrap_or_else(super::work_market::claim_lease_ms),
                        ),
                    );
                }
            }
            projection.revision = event.sequence;
            continue;
        }
        if event.kind != ACTION_EVENT_KIND {
            projection.revision = event.sequence;
            continue;
        }
        let envelope: AgentActionEnvelope =
            serde_json::from_value(event.payload.get("envelope").cloned().ok_or_else(|| {
                AgentActionServiceError::Corrupt("action_event_missing_envelope".to_string())
            })?)?;
        let entity_ref = event
            .payload
            .get("entity_ref")
            .and_then(serde_json::Value::as_str);
        let verdict_rule = match event.payload.get("review_round_rule") {
            None => false,
            Some(value) if value.as_str() == Some("verdict_v1")
                && matches!(&envelope.action, AgentAction::TaskReview(input)
                    if matches!(input.decision, harness_contract::agent_action::TaskReviewDecision::Challenge | harness_contract::agent_action::TaskReviewDecision::Rework)) => true,
            Some(_) => return Err(AgentActionServiceError::Corrupt("invalid_review_round_rule".into())),
        };
        if verdict_rule && matches!(&envelope.action, AgentAction::TaskReview(input)
            if projection.applied_rework_rounds.as_ref().and_then(|rounds| rounds.get(&input.task_ref)).is_none()) {
            projection.applied_rework_rounds = Some(rebuild_rework_rounds(store, &projection.program_id, projection.revision, &mut Vec::new())?);
        }
        let trusted_review_cut = match event.payload.get("review_cut_settlement") {
            None => false,
            Some(origin) => {
                let AgentAction::TaskAttemptFail(input) = &envelope.action else {
                    return Err(AgentActionServiceError::Corrupt("invalid_review_cut_settlement".into()));
                };
                if origin["kind"] != "review_cut_exhausted_v1"
                    || origin["program_id"] != envelope.actor.program_id
                    || origin["task_ref"] != input.task_ref
                    || origin["execution_id"] != input.execution_id
                    || origin["node_id"].as_str().is_none_or(str::is_empty)
                    || origin["attempt"].as_u64().is_none_or(|value| value == 0 || value > u64::from(u32::MAX))
                    || origin["rebuild_bound"].as_u64().is_none_or(|value| value == 0 || value > u64::from(u32::MAX))
                    || origin["rebuilds_used"].as_u64().is_none_or(|value| value > u64::from(u32::MAX)
                        || value <= origin["rebuild_bound"].as_u64().unwrap_or(u64::MAX))
                    || envelope.actor.kind != AgentActorKind::Supervisor
                    || envelope.actor.actor_id != "runtime.program-supervisor"
                    || envelope.actor.execution_id.as_deref() != Some(input.execution_id.as_str())
                    || input.mode != harness_contract::agent_action::AgentAttemptMode::Review
                {
                    return Err(AgentActionServiceError::Corrupt("invalid_review_cut_settlement".into()));
                }
                true
            }
        };
        projection.apply_with_round_rule(&envelope, entity_ref, event.sequence, event.created_at_ms, verdict_rule, trusted_review_cut)
            .map_err(|error| AgentActionServiceError::MemberUnknown(error.into()))?;
        match &envelope.action {
            AgentAction::TaskClaim(input) => {
                if let Some(lease_ms) = recorded_claim_lease_ms(&event.payload)? {
                    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
                        task.lease_expires_at_ms =
                            Some(event.created_at_ms.saturating_add(lease_ms));
                    }
                }
            }
            AgentAction::TaskAttemptDispatch(input)
                if input.mode == harness_contract::agent_action::AgentAttemptMode::Execute =>
            {
                if let Some(lease_ms) = recorded_claim_lease_ms(&event.payload)? {
                    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
                        task.lease_expires_at_ms =
                            Some(event.created_at_ms.saturating_add(lease_ms));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn is_durable_evidence_ref(reference: &str) -> bool {
    let reference = reference.trim();
    ["tool://", "artifact://"]
        .iter()
        .any(|prefix| reference.starts_with(prefix) && reference.len() > prefix.len())
        && !reference.chars().any(char::is_whitespace)
}

fn actor_agent_id(envelope: &AgentActionEnvelope) -> Option<&str> {
    envelope
        .actor
        .agent_id
        .as_deref()
        .or(Some(envelope.actor.actor_id.as_str()))
}

fn action_refs(envelope: &AgentActionEnvelope, entity_ref: Option<&str>) -> Vec<RuntimeEventRef> {
    let mut refs = vec![
        RuntimeEventRef {
            kind: "objective".to_string(),
            id: envelope.actor.objective_id.clone(),
        },
        RuntimeEventRef {
            kind: "program".to_string(),
            id: envelope.actor.program_id.clone(),
        },
        RuntimeEventRef {
            kind: "actor".to_string(),
            id: envelope.actor.actor_id.clone(),
        },
        RuntimeEventRef {
            kind: "session".to_string(),
            id: envelope.actor.session_id.clone(),
        },
        RuntimeEventRef {
            kind: "turn".to_string(),
            id: envelope.actor.turn_id.clone(),
        },
    ];
    if let Some(root_execution_id) = &envelope.actor.root_execution_id {
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: root_execution_id.clone(),
        });
    }
    if let Some(entity_ref) = entity_ref {
        refs.push(RuntimeEventRef {
            kind: entity_ref.split(':').next().unwrap_or("entity").to_string(),
            id: entity_ref.to_string(),
        });
    }
    if let AgentAction::TaskSupersede(input) = &envelope.action {
        refs.extend(
            input
                .replacement_task_refs
                .iter()
                .map(|id| RuntimeEventRef {
                    kind: "replacement_task".to_string(),
                    id: id.clone(),
                }),
        );
        refs.extend(input.evidence_refs.iter().map(|id| RuntimeEventRef {
            kind: "evidence".to_string(),
            id: id.clone(),
        }));
    }
    if let AgentAction::TaskWithdraw(input) = &envelope.action {
        refs.push(RuntimeEventRef {
            kind: "task".to_string(),
            id: input.task_ref.clone(),
        });
        refs.extend(input.evidence_refs.iter().map(|id| RuntimeEventRef {
            kind: "evidence".to_string(),
            id: id.clone(),
        }));
    }
    if let AgentAction::TaskAttemptDispatch(input) = &envelope.action {
        refs.push(RuntimeEventRef {
            kind: "task".to_string(),
            id: input.task_ref.clone(),
        });
        refs.push(RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: input.execution_id.clone(),
        });
    }
    refs
}

fn entity_ref(envelope: &AgentActionEnvelope) -> Option<String> {
    if let AgentAction::AgentInvite(input) = &envelope.action {
        if let Some(existing_agent_ref) = input.existing_agent_ref.as_ref() {
            return Some(existing_agent_ref.clone());
        }
    }
    // A claim, a submission and a review each settle one Task. They name that Task
    // in their input, so the entity they changed is the Task itself - the same
    // treatment `TaskSupersede` and `TaskWithdraw` already get. Without this, the
    // three actions that constitute the substance of a delegated Task produced an
    // empty `changed_refs`, and the progress measure that reads it could never see
    // them: in one live run 30 of 31 tool-batch observations reported no verified
    // gain while their own summaries reported new coverage, and the turn was ended
    // by the no-progress fuse after 21 replans that contradicted their observations.
    let prefix = match envelope.action {
        AgentAction::TeamCreate(_) => "team",
        AgentAction::AgentInvite(_) => "agent",
        AgentAction::TaskPublish(_) => "task",
        AgentAction::TaskClaim(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskSubmit(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskReview(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskSupersede(ref input) => return Some(input.task_ref.clone()),
        AgentAction::TaskWithdraw(ref input) => return Some(input.task_ref.clone()),
        AgentAction::MessagePublish(_) => "message",
        AgentAction::ArtifactCommit(_) => "artifact",
        _ => return None,
    };
    let digest = Sha256::digest(
        format!(
            "{}|{}|{}|{}",
            envelope.actor.program_id,
            envelope.actor.actor_id,
            envelope.action_id,
            envelope.action.kind()
        )
        .as_bytes(),
    );
    let encoded = format!("{digest:x}");
    Some(format!("{prefix}:{}", &encoded[..24]))
}

fn applied_observation(
    store: &Arc<RuntimeEventStore>,
    envelope: &AgentActionEnvelope,
    projection: &AgenticProgramProjection,
    entity_ref: Option<String>,
    duplicate: bool,
) -> Result<AgentActionObservation, AgentActionServiceError> {
    let artifact_access = entity_ref
        .as_ref()
        .and_then(|reference| projection.artifacts.get(reference))
        .map(|artifact| {
            json!({
                "artifact_ref": artifact.artifact_ref,
                "content_ref": artifact.content_ref,
                "read_tool": "evidence_retrieve",
                "read_request": { "evidence_ref": artifact.content_ref },
                "submit_ref": artifact.artifact_ref,
            })
        });
    let changed_refs = entity_ref.into_iter().collect::<Vec<_>>();
    let mut observation = AgentActionObservation {
        receipt_id: format!(
            "receipt:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision: projection.revision,
        status: AgentActionStatus::Applied,
        duplicate,
        changed_refs,
        actionable: actionable_for_actor(projection, &envelope.actor, duplicate),
        projection: Some(json!({
            "program_id": projection.program_id,
            "objective_id": projection.objective_id,
            "revision": projection.revision,
            "status": projection.status,
            "counts": {
                "teams": projection.teams.len(),
                "agents": projection.agents.len(),
                "tasks": projection.tasks.len(),
                "artifacts": projection.artifacts.len(),
            },
            "final_artifact_ref": projection.final_artifact_ref,
            "unresolved": projection.unresolved,
            "artifact_access": artifact_access,
        })),
        error: None,
    };
    attach_completion_assessment(&mut observation, &envelope.actor, projection, || {
        crate::execution_core::goal::assessment::read_bound_goal_assessment(store, projection)
    });
    Ok(observation)
}

pub(super) fn readable_topic_refs(
    projection: &AgenticProgramProjection,
    agent_id: &str,
    team_id: &str,
) -> std::collections::BTreeSet<String> {
    std::iter::once(format!("topic:{}", projection.program_id))
        .chain(
            projection
                .teams
                .get(team_id)
                .filter(|_| projection.agent_is_active_in(agent_id, team_id))
                .map(|team| team.topic_ref.clone()),
        )
        .collect()
}
pub(super) fn topic_entry_visible(entry: &AgenticTopicEntryProjection, agent_id: &str) -> bool {
    entry.actor_id == agent_id
        || entry.recipients.is_empty()
        || entry
            .recipients
            .iter()
            .any(|recipient| recipient == agent_id)
}

/// Remove the fields that describe the read, not the thing that was read.
///
/// A `state_inspect` directory page carries the program revision on the envelope,
/// on every entry, and in the program-wide `counts` block. None of those say whether
/// the entity the model asked about moved - a Task stays `published` while the
/// program advances elsewhere, and the entry's `revision` still climbs. Comparing the
/// raw page therefore never sees a stall in exactly the case a model polls one Task
/// while other work proceeds, which is the commonest loop of all.
fn strip_envelope_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("revision");
            object.remove("next_rungs");
            object.remove("counts");
            object.remove("coverage");
            // Caller identity, not entity state. The receipt echoes the action id
            // and receipt id of whoever asked, so two identical questions produce
            // different bytes and an unchanged entity reads as a change.
            object.remove("action_id");
            object.remove("receipt_id");
            object.remove("actionable");
            for nested in object.values_mut() {
                strip_envelope_fields(nested);
            }
        }
        serde_json::Value::Array(entries) => {
            for nested in entries.iter_mut() {
                strip_envelope_fields(nested);
            }
        }
        _ => {}
    }
}

fn inspected_observation(
    store: &RuntimeEventStore,
    envelope: &AgentActionEnvelope,
    projection: &AgenticProgramProjection,
    goal: Option<&harness_contract::goal::GoalContract>,
) -> AgentActionObservation {
    let input = match &envelope.action {
        AgentAction::StateInspect(input) => input,
        _ => unreachable!("only state_inspect reaches inspected_observation"),
    };
    let reader = format!(
        "{:?}:{}:{:?}:{:?}",
        envelope.actor.kind,
        envelope.actor.actor_id,
        envelope.actor.execution_id,
        envelope.actor.team_id
    );
    let topic_scope = if matches!(
        envelope.actor.kind,
        AgentActorKind::Agent | AgentActorKind::TeamLead
    ) {
        let Some(agent_id) = envelope
            .actor
            .agent_id
            .as_deref()
            .filter(|id| projection.agents.contains_key(*id))
        else {
            return rejected(
                envelope,
                projection.revision,
                "reader_not_authorized",
                "reader is not in the current Program roster",
            );
        };
        let Some(team_id) = envelope.actor.team_id.as_deref() else {
            return rejected(
                envelope,
                projection.revision,
                "reader_not_authorized",
                "reader has no bound Team",
            );
        };
        Some((agent_id, readable_topic_refs(projection, agent_id, team_id)))
    } else {
        None
    };
    let unchanged = goal.is_none()
        && input.after_revision == Some(projection.revision)
        && input.entry_ref.is_none()
        && input.scope_ref.is_none()
        && input.page_cursor.is_none()
        && input.query.is_none();
    let mut value = if unchanged {
        json!({
            "program_id": projection.program_id,
            "revision": projection.revision,
            "unchanged": true,
        })
    } else {
        match inspect_projection_page(projection, input, &reader, goal, topic_scope.as_ref()) {
            Ok(value) => value,
            Err(error) => {
                return rejected(
                    envelope,
                    projection.revision,
                    "invalid_state_cursor",
                    &error,
                );
            }
        }
    };
    // Name the outstanding rungs on a program-level read, from the same function
    // the barrier checkpoint renders them from.
    //
    // `state_inspect` used to return bounded state pages and nothing else, so the
    // one authoritative statement of what to do next existed only on the barrier
    // path. A root that polled instead of waiting was therefore told that a rung
    // was owed - the model-step invariant forces a tool call whenever one is -
    // without ever being told which. One live run, forced to call something and
    // given nothing to aim at, issued the same generic `{"query": "task"}` five
    // times while two of its Tasks sat Submitted and unreviewed, and its next rung
    // was `independent_review` the whole time.
    //
    // Not attached to the `unchanged` receipt: that path exists to stay a few
    // bytes, and an unchanged revision means unchanged rungs, which the reader
    // already has from the page that produced this revision.
    if !unchanged && input.entry_ref.is_none() && input.scope_ref.is_none() {
        if let Some(object) = value.as_object_mut() {
            let rungs = serde_json::Value::Array(
                crate::execution_core::goal::assessment::closure_rungs_for_program(projection),
            );
            if matches!(
                envelope.actor.kind,
                AgentActorKind::Agent | AgentActorKind::TeamLead
            ) {
                object.insert(
                    "program_progress".to_string(),
                    json!({
                        "owner": "RootTurn",
                        "next_rungs": rungs,
                        "role_note": "Program-wide closure is read-only background, not an assignment. Continue only the Task, review, or coordination work bound to this execution.",
                    }),
                );
            } else {
                object.insert("next_rungs".to_string(), rungs);
            }
        }
    }
    // Say the cheap way to ask, at the moment asking again is pointless.
    //
    // A model waiting for a worker polls, and every poll pulls the whole state back
    // into context. Because the state carries the program revision the bytes are
    // never identical, so the duplicate-result stub never fires: one live run issued
    // the same `state_inspect` on one `task_ref` eleven times and another ten, each
    // time re-sending the full state.
    //
    // The revision is Runtime's own progress signal, so "same input, same revision"
    // is the honest observation, and the notice names the two parameters that already
    // exist to make the next ask cheap. It does not refuse, stub, or narrow anything.
    // Program-level reads only. An exact `entry_ref`/`scope_ref` asks about one
    // entity, and re-asking is legitimate: whether a topic became visible, whether a
    // Task moved, whether a ref resolved are all questions whose answer can change
    // without the program revision moving. One live test asserts exactly that - a
    // cross-team read answering `not_found` - and collapsing it into an unchanged
    // receipt would hide the answer.
    if !unchanged && input.entry_ref.is_none() && input.scope_ref.is_none() {
        let input_hash =
            crate::tool_invocation::stable_hash(&serde_json::to_string(input).unwrap_or_default());
        // The token is what was actually asked about, not the program revision.
        //
        // Keying on the revision missed the commonest loop: a model polling
        // `state_inspect {"query": "task:X"}` while the program advances elsewhere.
        // The revision moves, so the check never fires, and the same query is
        // re-issued against a Task that has not moved at all - one live run did
        // exactly that and was stopped for spinning with this shape in the evidence.
        //
        // So the token is a digest of the answer with the envelope's `revision`
        // stripped out. What remains is the entity content, and if that is identical
        // the question was answered identically, whatever the rest of the program did.
        let mut answer = value.clone();
        strip_envelope_fields(&mut answer);
        let state_token = serde_json::to_string(&answer).unwrap_or_default();
        let inspection_scope = envelope.actor.execution_id.as_deref().map_or_else(
            || store.repetition_scope(&envelope.actor.session_id, &envelope.actor.turn_id),
            |execution_id| {
                store.repetition_execution_scope(&envelope.actor.session_id, execution_id)
            },
        );
        if crate::repetition_guard::record_unchanged_inspection(
            &inspection_scope,
            &input_hash,
            &state_token,
        ) {
            // Answer the question that was asked, not the bytes that were asked for.
            //
            // A notice alone was tried first and measured: it reached the model
            // twenty-two times in one live run and the share of calls that repeated an
            // actor's own exact input did not move. Telling a model that polling is
            // expensive is advice; the model has already decided what it needs by the
            // time it reads the result.
            //
            // At an unchanged revision the honest answer to "what is the state" is
            // "the state you already have" - which is exactly what `after_revision`
            // would have returned. Runtime serves that instead of re-sending the whole
            // page, and names the ways to get more: a newer `after_revision`, a
            // different query, or one exact `entry_ref`.
            let revision = i64::try_from(projection.revision).unwrap_or(i64::MAX);
            value = json!({
                "program_id": projection.program_id,
                "revision": revision,
                "unchanged": true,
                "notice": "Runtime answered this with the same receipt `after_revision` \
                           would have returned: the program revision has not moved since \
                           your last identical query, so the state you already hold is \
                           current. To wait cheaply, re-issue with \
                           `after_revision: <revision>`, or with `wait_for_workers: true` \
                           to park until a worker moves. For something specific, read one \
                           exact `entry_ref` or ask a different query - a broad listing \
                           is only re-sent when the revision actually advances."
            });
        }
    }
    AgentActionObservation {
        receipt_id: format!(
            "observation:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision: projection.revision,
        status: AgentActionStatus::Observed,
        duplicate: false,
        changed_refs: Vec::new(),
        actionable: actionable_for_actor(projection, &envelope.actor, false),
        projection: Some(value),
        error: None,
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StateInspectCursor {
    reader: String,
    version: u8,
    program_id: String,
    revision: u64,
    #[serde(default)]
    goal_revision: Option<u64>,
    query: Option<String>,
    after_ref: String,
}

const STATE_INSPECT_PAGE_SIZE: usize = 32;

fn goal_summary(goal: &harness_contract::goal::GoalContract) -> serde_json::Value {
    json!({"goal_id":goal.id,"revision":goal.revision,"spec_revision":goal.spec_revision,
        "spec_digest":goal.spec_digest,"source_intent_ref":goal.source_intent_ref,"completion":goal.completion,
        "participation_requirement":goal.participation_requirement,
        "criteria_count":goal.criteria.len(),"obligation_count":goal.obligations.len(),"review_count":goal.reviews.len(),
        "inspect_request":{"entry_ref":goal.id}})
}

fn goal_directory_entries(goal: &harness_contract::goal::GoalContract) -> Vec<serde_json::Value> {
    std::iter::once(json!({"entry_ref":goal.id,"goal_id":goal.id,"kind":"goal",
        "label":goal.objective.chars().take(480).collect::<String>()}))
        .chain(goal.criteria.iter().map(|criterion| json!({"entry_ref":criterion.id,"goal_id":goal.id,
            "kind":"criterion","label":criterion.statement.chars().take(480).collect::<String>(),
            "status":criterion.status,"statement_ref":criterion.statement_ref})))
        .chain(goal.obligations.iter().map(|obligation| json!({"entry_ref":obligation.obligation_id,"goal_id":goal.id,
            "kind":"obligation","label":obligation.success_predicate.chars().take(480).collect::<String>(),
            "state":obligation.state,"required":obligation.required,
            "review_request":{"criterion_ref":obligation.obligation_id}})))
        .chain(goal.reviews.iter().map(|review| json!({"entry_ref":review.review_id,"goal_id":goal.id,
            "kind":"objective_review","criterion_ref":review.criterion_ref,"decision":review.decision,
            "spec_revision":review.spec_revision}))).collect()
}

/// Return a bounded navigation page or one exact entity.  The output never
/// contains an unbounded Program dump: models use the returned stable refs to
/// retrieve only the Task, Team, topic, or artifact relevant to their next
/// decision.  The cursor is a deterministic index over a revision snapshot,
/// so it is safe to discard after any revision change.
fn inspect_projection_page(
    projection: &AgenticProgramProjection,
    input: &harness_contract::agent_action::StateInspectInput,
    reader: &str,
    goal: Option<&harness_contract::goal::GoalContract>,
    topic_scope: Option<&(&str, std::collections::BTreeSet<String>)>,
) -> Result<serde_json::Value, String> {
    let readable_topic = |topic: &str| topic_scope.is_none_or(|(_, topics)| topics.contains(topic));
    let readable_entry = |entry: &AgenticTopicEntryProjection| {
        topic_scope.is_none_or(|(agent, _)| topic_entry_visible(entry, agent))
    };
    let exact_ref = input.entry_ref.as_deref().or(input.scope_ref.as_deref());
    if let Some(reference) = exact_ref {
        if reference == projection.program_id {
            // A Program reference names the current bounded directory, not an
            // entity lookup. Reuse its existing reader and topic visibility.
            let mut directory = input.clone();
            directory.entry_ref = None;
            directory.scope_ref = None;
            directory.page_cursor = None;
            directory.query = None;
            let mut page = inspect_projection_page(projection, &directory, reader, goal, topic_scope)?;
            page["program"] = json!({
                "program_id": projection.program_id,
                "objective_id": projection.objective_id,
                "objective_summary": projection.objective_summary,
                "status": projection.status,
            });
            return Ok(page);
        }
        if let Some(goal) = goal {
            let value = if reference == goal.id {
                // Exact entity reads return the full objective. Directory pages keep
                // their bounded labels, but a goal read must not hide the requirement
                // behind the old 12000/4000 char thresholds.
                Some({
                    let mut summary = goal_summary(goal);
                    if let Some(object) = summary.as_object_mut() {
                        object.insert(
                            "objective".to_string(),
                            serde_json::Value::String(goal.objective.clone()),
                        );
                    }
                    summary
                })
            } else if let Some(criterion) = goal.criteria.iter().find(|item| item.id == reference) {
                Some(json!({"criterion":criterion}))
            } else if let Some(obligation) = goal
                .obligations
                .iter()
                .find(|item| item.obligation_id == reference)
            {
                Some(json!({"obligation":obligation}))
            } else {
                goal.reviews
                    .iter()
                    .find(|item| item.review_id == reference)
                    .map(|review| json!({"review":review}))
            };
            if let Some(value) = value {
                return Ok(
                    json!({"program_id":projection.program_id,"revision":projection.revision,
                    "goal_id":goal.id,"goal_revision":goal.revision,"spec_revision":goal.spec_revision,
                    "spec_digest":goal.spec_digest,"goal_entry":value}),
                );
            }
        }
        if let Some(membership) = projection.memberships.get(reference) {
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,"membership":membership}),
            );
        }
        if let Some((topic_ref, entry)) = projection
            .topics
            .iter()
            .filter(|(topic, _)| readable_topic(topic))
            .find_map(|(topic_ref, entries)| {
                entries
                    .iter()
                    .find(|entry| entry.entry_id == reference && readable_entry(entry))
                    .map(|entry| (topic_ref, entry))
            })
        {
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,"topic_ref":topic_ref,"entry":entry,
                "read_request":entry.content_ref.as_ref().map(|content| json!({"evidence_ref":content}))}),
            );
        }
        if let Some(issue) = super::issues::issues(projection)
            .into_iter()
            .find(|issue| issue.issue_ref == reference)
        {
            return Ok(
                json!({"program_id": projection.program_id, "revision": projection.revision, "issue": issue}),
            );
        }
        if let Some(entries) = projection
            .topics
            .get(reference)
            .filter(|_| readable_topic(reference))
        {
            let total = entries.iter().filter(|entry| readable_entry(entry)).count();
            let page = entries
                .iter()
                .rev()
                .filter(|entry| readable_entry(entry))
                .take(STATE_INSPECT_PAGE_SIZE)
                .collect::<Vec<_>>();
            return Ok(
                json!({"program_id":projection.program_id,"revision":projection.revision,
                "topic_ref":reference,"entries":page,"directory_request":{"query":reference},
                "coverage":{"loaded":page.len(),"total":total,"complete":total<=STATE_INSPECT_PAGE_SIZE}}),
            );
        }
        if projection.teams.contains_key(reference)
            || projection.agents.contains_key(reference)
            || projection.tasks.contains_key(reference)
            || projection.artifacts.contains_key(reference)
        {
            return Ok(AgentActionService::compact_projection(
                projection,
                Some(reference),
            ));
        }
        return Ok(json!({
            "program_id": projection.program_id,
            "revision": projection.revision,
            "entry_ref": reference,
            "not_found": true,
        }));
    }
    let query = input
        .query
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(str::to_lowercase);
    let cursor = input
        .page_cursor
        .as_deref()
        .map(|cursor| {
            serde_json::from_str::<StateInspectCursor>(cursor).map_err(|_| {
                "invalid Program cursor; copy next_request from a fresh directory".to_string()
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.version != 1
            || cursor.reader != reader
            || cursor.program_id != projection.program_id
            || cursor.revision != projection.revision
            || cursor.goal_revision != goal.map(|goal| goal.revision)
            || cursor.query != query
    }) {
        return Err(
            "Program source, revision or query changed; restart directory discovery".into(),
        );
    }
    let directory = projection
        .teams
        .values()
        .map(|team| {
            json!({
                "entry_ref": team.team_id,
                "kind": "team",
                "label": team.name,
                "lifecycle": team.lifecycle,
            })
        })
        .chain(projection.agents.values().map(|agent| {
            json!({
                "entry_ref": agent.agent_id,
                "kind": "agent",
                "label": agent.role,
                "memberships": agent.membership_ids,
            })
        }))
        .chain(projection.tasks.values().map(|task| {
            json!({
                "entry_ref": task.task_id,
                "kind": "task",
                "label": task.title,
                "status": task.status,
                "team_ref": task.team_id,
                "active_attempt_count": task.active_attempts.len(),
            })
        }))
        .chain(projection.topics.keys().filter(|topic| readable_topic(topic)).map(|topic_ref| {
            json!({
                "entry_ref": topic_ref,
                "kind": "topic",
            })
        }))
        .chain(projection.memberships.values().map(|membership| json!({
            "entry_ref":membership.membership_id,"kind":"membership","agent_ref":membership.agent_id,"team_ref":membership.team_id,"lifecycle":membership.lifecycle
        })))
        .chain(projection.topics.iter().filter(|(topic, _)| readable_topic(topic)).flat_map(|(topic_ref,entries)| entries.iter().filter(|entry| readable_entry(entry)).map(move |entry| json!({
            "entry_ref":entry.entry_id,"kind":"topic_entry","topic_ref":topic_ref,"author":entry.actor_id,
            "label":entry.summary.as_deref().unwrap_or("").chars().take(480).collect::<String>(),"entry_revision":entry.revision
        }))))
        .chain(projection.artifacts.values().map(|artifact| {
            json!({
                "entry_ref": artifact.artifact_ref,
                "kind": "artifact",
                "label": artifact.title,
                "read_request": { "evidence_ref": artifact.content_ref },
            })
        }))
        .chain(super::issues::issues(projection).into_iter().map(|issue| json!({
            "entry_ref": issue.issue_ref, "kind": "issue", "label": issue.description,
            "source_ref": issue.source_ref, "disposition": issue.disposition,
            "unclassified": issue.disposition.is_none(),
        })))
        .chain(goal.into_iter().flat_map(goal_directory_entries));
    // Keep only the next page plus one look-ahead entry. Discovery can scan
    // metadata, but must not materialize the complete Program directory.
    let mut entries: Vec<serde_json::Value> = Vec::with_capacity(STATE_INSPECT_PAGE_SIZE + 1);
    let mut cursor_found = cursor.is_none();
    for entry in directory {
        if query
            .as_ref()
            .is_some_and(|query| !entry.to_string().to_lowercase().contains(query))
        {
            continue;
        }
        let reference = entry["entry_ref"].as_str().unwrap_or("");
        if let Some(cursor) = &cursor {
            cursor_found |= reference == cursor.after_ref;
            if reference <= cursor.after_ref.as_str() {
                continue;
            }
        }
        let position = entries.partition_point(|candidate| {
            candidate["entry_ref"].as_str().unwrap_or("") <= reference
        });
        if position <= STATE_INSPECT_PAGE_SIZE {
            entries.insert(position, entry);
            if entries.len() > STATE_INSPECT_PAGE_SIZE + 1 {
                entries.pop();
            }
        }
    }
    if !cursor_found {
        return Err("Program cursor position is not present in this directory".into());
    }
    let start: usize = 0;
    let end = start
        .saturating_add(STATE_INSPECT_PAGE_SIZE)
        .min(entries.len());
    let next_cursor = (end < entries.len())
        .then(|| {
            serde_json::to_string(&StateInspectCursor {
                version: 1,
                reader: reader.into(),
                program_id: projection.program_id.clone(),
                revision: projection.revision,
                goal_revision: goal.map(|goal| goal.revision),
                query: query.clone(),
                after_ref: entries[end - 1]["entry_ref"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            })
        })
        .transpose()
        .map_err(|error| error.to_string())?;
    for entry in &mut entries[start..end] {
        entry["source_kind"] = json!("program");
        entry["revision"] = json!(projection.revision);
        entry["scope"] = json!(projection.program_id);
        entry["inspect_request"] = json!({"entry_ref":entry["entry_ref"]});
    }
    Ok(json!({
        "program_id": projection.program_id,
        "objective_id": projection.objective_id,
        "continuation": projection.continuation,
        "revision": projection.revision,
        "status": projection.status,
        "counts": {
            "teams": projection.teams.len(),
            "agents": projection.agents.len(),
            "memberships": projection.memberships.len(),
            "tasks": projection.tasks.len(),
            "artifacts": projection.artifacts.len(),
        },
        "goal": goal.map(goal_summary),
        "entries": entries[start..end].to_vec(),
        "next_page_cursor": next_cursor,
        "next_request": next_cursor.as_ref().map(|cursor| {
            let mut request = json!({"page_cursor":cursor});
            if let Some(query) = &input.query { request["query"] = json!(query); }
            request
        }),
        "coverage": {"kind":"program_metadata_directory", "complete":next_cursor.is_none(), "revision":projection.revision},
    }))
}

fn rejected(
    envelope: &AgentActionEnvelope,
    revision: u64,
    code: &str,
    message: &str,
) -> AgentActionObservation {
    AgentActionObservation {
        receipt_id: format!(
            "rejection:{}:{}",
            envelope.actor.program_id, envelope.action_id
        ),
        action_id: envelope.action_id.clone(),
        action: envelope.action.kind().to_string(),
        program_id: envelope.actor.program_id.clone(),
        revision,
        status: AgentActionStatus::Rejected,
        duplicate: false,
        changed_refs: Vec::new(),
        actionable: vec!["inspect current state and revise only this action".to_string()],
        projection: None,
        error: Some(AgentActionErrorObservation {
            code: code.to_string(),
            message: message.to_string(),
            recoverable: true,
        }),
    }
}

fn actionable_for_actor(
    projection: &AgenticProgramProjection,
    actor: &harness_contract::agent_action::AgentActorBinding,
    duplicate: bool,
) -> Vec<String> {
    if !matches!(actor.kind, AgentActorKind::Agent | AgentActorKind::TeamLead)
        || projection.status != super::program::AgenticProgramStatus::Open
    {
        return actionable(projection, duplicate, None);
    }
    let mut actions = Vec::new();
    if duplicate {
        actions.push("idempotent replay: no state was duplicated".to_string());
    }
    let bound_attempt = actor
        .execution_id
        .as_deref()
        .zip(actor.agent_id.as_deref())
        .and_then(|(execution_id, agent_id)| {
            projection.tasks.values().find_map(|task| {
                task.active_attempts
                    .get(execution_id)
                    .filter(|attempt| attempt.agent_id == agent_id)
                    .map(|attempt| (task, attempt))
            })
        });
    if let Some((task, attempt)) = bound_attempt {
        use harness_contract::agent_action::AgentAttemptMode;
        actions.push(match attempt.mode {
            AgentAttemptMode::Execute if task.purpose == harness_contract::agent_action::TaskPurpose::Exploration => format!(
                "Complete only your bound Task `{}`. {}",
                task.task_id,
                super::order_contract::EXPLORATION_CLOSEOUT_CONTRACT,
            ),
            AgentAttemptMode::Execute => format!(
                "Complete only your bound Task `{}`. Prefer one `task_submit` with `deliverable` selecting this response's report or the latest saved report from this same Task, Agent, claim and attempt. Use `artifact_commit` only for file-backed or multi-part deliverables. Include real durable evidence and disclose genuine gaps in `unresolved`.",
                task.task_id,
            ),
            AgentAttemptMode::Review => format!(
                "Read the submitted artifact for your bound Task `{}` and issue the independent `task_review` verdict. Do not execute or resubmit another Task.",
                task.task_id,
            ),
            AgentAttemptMode::Coordination => format!(
                "Respond only to the bound coordination work for Task `{}` using your authorized collaboration actions.",
                task.task_id,
            ),
        });
    }
    actions.push(
        "The root coordinator owns Program-level closure. Global Program progress is read-only background; follow only the Task, review, or coordination assignment in your private role brief."
            .to_string(),
    );
    actions
}

fn actionable(
    projection: &AgenticProgramProjection,
    duplicate: bool,
    completion_gap: Option<&str>,
) -> Vec<String> {
    let goal_gaps = completion_gap.into_iter().collect::<Vec<_>>();
    let mut actions = Vec::new();
    if duplicate {
        actions.push("idempotent replay: no state was duplicated".to_string());
    }
    match projection.status {
        super::program::AgenticProgramStatus::CompletionRequested => {
            actions.push(
                "completion request is durable; await the ObjectiveSupervisor verdict".to_string(),
            );
            return actions;
        }
        super::program::AgenticProgramStatus::Verified
        | super::program::AgenticProgramStatus::Partial
        | super::program::AgenticProgramStatus::Blocked
        | super::program::AgenticProgramStatus::Failed
        | super::program::AgenticProgramStatus::Cancelled => return actions,
        super::program::AgenticProgramStatus::Waiting => {
            actions.push("await the recorded external dependency or permission change".to_string());
            return actions;
        }
        super::program::AgenticProgramStatus::Draining => {
            actions
                .push("await cancellation and effect verification before finalizing".to_string());
            return actions;
        }
        super::program::AgenticProgramStatus::Open => {}
    }
    // A Task Runtime already deferred past its own dispatch bound is not slow
    // work: no member can take it. Say so with the Task named, because a model
    // told only "wait" would keep polling a Task that can never start.
    let stranded = crate::agentic::execution::dispatch::stranded_task_refs(projection);
    if !stranded.is_empty() {
        actions.push(format!(
            "Tasks [{}] have no Agent able to progress them: Runtime already deferred their dispatch past its bound. Replan, replace or withdraw them - waiting cannot start them.",
            stranded.join(", ")
        ));
    }
    if !goal_gaps.is_empty() {
        // Explicit assessment supplies current Goal gaps separately; this names what is
        // still missing rather than what one reopen happened to record. The
        // reopen event's own wording is history and is shown separately:
        // presenting it as the current state is what once told a model to keep
        // resolving a criterion it had already satisfied.
        actions.push(format!("The previous completion request was reopened. Resolve the recorded gaps before requesting completion again: {}", goal_gaps.join("; ")));
        if !projection.last_reopen_gaps.is_empty() {
            actions.push(format!(
                "For reference, the last reopen recorded: {}",
                projection.last_reopen_gaps.join("; ")
            ));
        }
        // The gap a completion request is reopened with is assembled by
        // `objective_completion_waiting_gap`, which prefixes the recorded
        // reason: `objective_completion_gap:original_intent_review_required:
        // user_intent; ...`. Matching the bare prefix therefore never fired,
        // and the one sentence that names the tool the model must call was
        // silently dropped - a live run cycled seven completion requests
        // without ever issuing a review, because the only instruction it got
        // was a gap string it could not act on. Match the marker wherever it
        // appears in the assembled gap.
        let review_required = goal_gaps.iter().any(|gap| {
            gap.contains("original_intent_review_required:")
                || gap.contains("criterion_review_required:")
        });
        if review_required {
            // The how-to names the tool and the shape; the scope discipline is
            // shared with the terminal presentation and therefore comes from
            // the order contract, so a change cannot reach one layer and miss
            // the other.
            actions.push(format!(
                "Use objective_review for the original criterion with the current delivered result and durable evidence. {}",
                crate::agentic::order_contract::OBJECTIVE_REVIEW_SCOPE_DISCIPLINE
            ));
            // Name the exact refs that still have no review, so the model does
            // not have to parse them out of the gap string itself.
            let uncovered = goal_gaps
                .iter()
                .filter_map(|gap| gap.split("uncovered_result_refs:").nth(1))
                .flat_map(|tail| tail.split(';').next().unwrap_or("").split(','))
                .map(|reference| reference.trim().to_string())
                .filter(|reference| !reference.is_empty())
                .collect::<Vec<_>>();
            if !uncovered.is_empty() {
                actions.push(format!(
                    "These delivered results have no review yet: {}. Read each one, then submit exactly one objective_review whose result_refs cover all of them.",
                    uncovered.join(", ")
                ));
            }
        }
        // A reopened Program must never also advertise the exit action in the
        // same receipt: one live run received "resolve the recorded gaps" and
        // "request Objective completion" together, re-requested completion, and
        // looped until the observation window expired. While gaps remain, the
        // gap guidance above is the entire instruction.
        return actions;
    }
    // A Task no Agent will settle is a fact the root must be able to read. Runtime
    // used to keep it private in the dispatch starvation ledger, so the model had no
    // step and no explanation: in one live run a reworked Task was re-submitted and
    // never reviewed again, the root graph had no ready node, and the turn ended by
    // the no-progress fuse. Runtime states the fact; it does not choose the remedy.
    let stranded = crate::agentic::execution::dispatch::stranded_task_refs(projection);
    if !stranded.is_empty() {
        actions.push(format!(
            "These Tasks have no Agent able to progress them (Runtime could not dispatch an eligible member): {}. They remain open work in the Program. You can invite a reviewer, withdraw or supersede the Task, or record it as unresolved when requesting completion.",
            stranded.join(", ")
        ));
    }
    if projection.teams.is_empty() {
        if projection.artifacts.is_empty() {
            actions.push(super::order_contract::ARTIFACT_CONTENT_REF_CONTRACT.to_string());
        } else {
            actions.push(super::order_contract::OBJECTIVE_REVIEW_SCOPE_DISCIPLINE.to_string());
        }
    }
    // Authoring facts recorded when they were created. A model that invented an
    // acceptance standard sees it named as its own on every subsequent receipt,
    // which is the shortest correction loop available: the cost was paid at
    // authoring time, so the notice belongs there too.
    for warning in &projection.program_warnings {
        actions.push(warning.clone());
    }
    if projection
        .teams
        .values()
        .any(|team| team.member_ids.is_empty())
    {
        actions.push("invite an Agent into an unstaffed Team".to_string());
    }
    if projection.tasks.values().any(|task| {
        matches!(
            task.status,
            AgenticTaskStatus::Published | AgenticTaskStatus::Rework
        )
    }) {
        actions.push("claim an available Task".to_string());
    }
    if projection.tasks.values().any(|task| {
        task.status == AgenticTaskStatus::Published
            && (task.failed_attempts > 0 || task.failed_review_attempts > 0)
    }) {
        actions.push(
            "reassign the failed work, or publish concrete successor Task(s) and then call task_supersede with durable failure/replan evidence"
                .to_string(),
        );
    }
    // Make the execution Task's lowest-cost closeout the default. The previous text
    // listed several commit shapes without saying that one `task_submit` can bind a
    // same-attempt draft written in an earlier model step. That pushed a live Agent
    // through a separate `artifact_commit` and made a mistaken current-block index look
    // like a permanent delivery failure.
    let claimed = projection
        .tasks
        .values()
        .any(|task| task.status == AgenticTaskStatus::Claimed);
    if claimed
        || projection
            .tasks
            .values()
            .any(|t| t.status == AgenticTaskStatus::Submitted)
    {
        actions.push(format!(
            "For an Agent executing its bound claimed Task: {} Disclose genuine gaps in unresolved. An Agent bound in review mode must read the submitted artifact and issue task_review instead of resubmitting it.",
            crate::agentic::order_contract::TASK_REPORT_SUBMISSION_CONTRACT,
        ));
    }
    if !projection.artifacts.is_empty()
        && projection
            .tasks
            .values()
            .any(|task| task.status != AgenticTaskStatus::Superseded)
        && projection
            .tasks
            .values()
            .filter(|task| task.status != AgenticTaskStatus::Superseded)
            .all(|task| task.status == AgenticTaskStatus::Accepted)
    {
        actions.push("request Objective completion with the final artifact".to_string());
    }
    actions
}

fn program_stream(program_id: &str) -> String {
    format!("agentic-program:{program_id}")
}

fn topic_cursor_stream(program_id: &str, execution_id: &str) -> String {
    format!("agentic-topic-cursor:{program_id}:{execution_id}")
}

pub(super) fn action_key(action_id: &str) -> String {
    format!("agent-action:{action_id}")
}

fn read_model_projection_id(program_id: &str) -> String {
    format!(
        "runtime:agentic-read-model:v2:{:x}",
        Sha256::digest(program_id.as_bytes())
    )
}

fn decode_read_snapshot(
    checkpoint: &crate::RuntimeProjectionCheckpoint,
    program_id: &str,
    store: &RuntimeEventStore,
) -> Option<AgenticProgramProjection> {
    let payload = &checkpoint.payload;
    if payload["schema_version"] != 3 {
        return None;
    }
    let session = payload["session_id"].as_str()?;
    let body = store
        .resolve_content(
            &format!("session:{session}"),
            &payload["body"],
            &crate::PreparedArtifactOrigin::CheckpointBody {
                projection_id: read_model_projection_id(program_id),
                source_cursor: checkpoint.source_cursor,
            },
        )
        .ok()?;
    let projection: AgenticProgramProjection = serde_json::from_value(body).ok()?;
    if projection.program_id != program_id
        || projection.revision != checkpoint.source_cursor
        || projection.session_id != session
    {
        return None;
    }
    Some(projection)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod native_retirement_guard_tests {
    use super::AgentActionService;
    use crate::execution_core::graph::executors::CompileTargetGuardExecutor;
    use crate::execution_core::graph::{
        ExecutionCompileRequest, ExecutionGraphCompiler, NodeExecutor,
    };
    use crate::execution_core::RuntimeCompileTarget;
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionParentBinding,
    };

    fn compiled_child(target: RuntimeCompileTarget) -> ExecutionGraph {
        let objective = "Real compiler Native retirement guards";
        let payload = serde_json::json!({
            "kind":"conversation_turn", "compile_target":target,
            "session_id":"guard-session", "objective":objective, "content":objective,
        })
        .to_string();
        let mut graph = ExecutionGraphCompiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: objective.into(),
                payload_ref: payload,
                target,
                resource_scopes: vec!["read:.".into(), "write:out/a".into()],
            })
            .unwrap();
        graph.service_class = harness_contract::execution_graph::ExecutionServiceClass::Foreground;
        graph.parent_execution = Some(ExecutionParentBinding {
            execution_id: "original-agent-graph".into(),
            node_id: "original-agent-node".into(),
        });
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "guard-session".into(),
            turn_id: "guard-turn".into(),
            root_task_id: "guard-root-task".into(),
            task_id: "guard-task".into(),
            generation: 1,
        });
        graph
    }

    #[test]
    fn r1_native_retirement_accepts_all_conversation_compiler_guards() {
        for (target, criteria) in [
            (
                RuntimeCompileTarget::InlineModel,
                vec!["mutation_resources_must_be_leased"],
            ),
            (
                RuntimeCompileTarget::EvidenceGraph,
                vec![
                    "evidence_read_before_synthesis",
                    "mutation_resources_must_be_leased",
                ],
            ),
            (
                RuntimeCompileTarget::ExecutionGraph,
                vec![
                    "permission_and_policy_gate_required",
                    "mutation_resources_must_be_leased",
                ],
            ),
        ] {
            let original = compiled_child(target);
            let guards = original
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::Verify)
                .collect::<Vec<_>>();
            assert_eq!(guards.len(), criteria.len());
            for (guard, criterion) in guards.into_iter().zip(criteria) {
                assert_eq!(guard.executor_kind, CompileTargetGuardExecutor::KIND);
                assert_eq!(guard.acceptance.criteria, vec![criterion.to_string()]);
                CompileTargetGuardExecutor.validate(guard).unwrap();
                AgentActionService::validate_native_retirement_target_guard(
                    &original, &original, guard,
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn r1_native_retirement_accepts_stable_parent_and_ingress_guard_identity() {
        for target in [
            RuntimeCompileTarget::InlineModel,
            RuntimeCompileTarget::EvidenceGraph,
            RuntimeCompileTarget::ExecutionGraph,
        ] {
            for ingress in [false, true] {
                let mut original = compiled_child(target);
                let mut remapped = std::collections::BTreeMap::new();
                let parent = format!("{}:model", original.id);
                for (index, node) in original.nodes.iter_mut().enumerate() {
                    let previous = node.id.clone();
                    let suffix = previous.strip_prefix(&format!("{}:", original.id)).unwrap();
                    if index == 0 {
                        node.id.clone_from(&parent);
                    } else if node.id == parent {
                        node.id.push_str(":retarget");
                    }
                    node.idempotency_key = if ingress {
                        format!("request:{suffix}")
                    } else {
                        node.id.clone()
                    };
                    if ingress {
                        let mut payload: serde_json::Value =
                            serde_json::from_str(&node.payload_ref).unwrap();
                        payload["ingress"] = serde_json::json!({
                            "request_id":"request", "turn_id":"guard-turn", "message_id":"message",
                            "session_id":"guard-session", "primary_task_id":"guard-task", "root_task_id":"guard-root-task",
                            "session_generation":1, "input_sequence":1, "claim_owner":"owner", "claim_token":"token", "claim_revision":1
                        });
                        node.payload_ref = payload.to_string();
                    }
                    remapped.insert(previous, node.id.clone());
                }
                for edge in &mut original.edges {
                    edge.from = remapped[&edge.from].clone();
                    edge.to = remapped[&edge.to].clone();
                }
                if ingress {
                    let mut dispatch = harness_contract::execution_graph::ExecutionNodeSpec::new(
                        ExecutionNodeKind::SessionDispatch,
                        "session_dispatch",
                        "dispatch",
                    );
                    dispatch.id = format!("{}:dispatch", original.id);
                    original
                        .edges
                        .push(harness_contract::execution_graph::ExecutionEdge {
                            from: dispatch.id.clone(),
                            to: parent.clone(),
                            kind: harness_contract::execution_graph::ExecutionEdgeKind::DependsOn,
                        });
                    original.nodes.insert(0, dispatch);
                }
                harness_contract::execution_graph::validate_execution_graph(&original).unwrap();
                let base =
                    AgentActionService::native_retirement_original_model(&original, &original)
                        .unwrap();
                assert_eq!(base.id, format!("{}:model:retarget", original.id));
                for mutation in ["missing", "changed", "duplicate"] {
                    let mut child = original.clone();
                    let index = child
                        .nodes
                        .iter()
                        .position(|node| node.id == base.id)
                        .unwrap();
                    match mutation {
                        "missing" => {
                            child.nodes.remove(index);
                        }
                        "changed" => child.nodes[index].payload_ref.push_str("foreign"),
                        "duplicate" => child.nodes.push(base.clone()),
                        _ => unreachable!(),
                    }
                    assert_eq!(
                        AgentActionService::native_retirement_original_model(&original, &child)
                            .unwrap_err(),
                        "runtime retirement Native original model changed"
                    );
                }
                let mut ambiguous = original.clone();
                let mut extra_model = base.clone();
                extra_model.id.push_str(":extra");
                ambiguous.nodes.push(extra_model);
                assert_eq!(
                    AgentActionService::native_retirement_original_model(&ambiguous, &ambiguous)
                        .unwrap_err(),
                    "runtime retirement Native original model is ambiguous"
                );
                for guard in original
                    .nodes
                    .iter()
                    .filter(|node| node.kind == ExecutionNodeKind::Verify)
                {
                    AgentActionService::validate_native_retirement_target_guard(
                        &original, &original, guard,
                    )
                    .unwrap();
                    for mutation in [
                        "node",
                        "idempotency",
                        "criterion",
                        "executor",
                        "edge",
                        "extra-node",
                    ] {
                        let mut forged = original.clone();
                        let index = forged
                            .nodes
                            .iter()
                            .position(|node| node.id == guard.id)
                            .unwrap();
                        match mutation {
                            "node" => forged.nodes[index].id.push_str(":foreign"),
                            "idempotency" => {
                                forged.nodes[index].idempotency_key.push_str(":foreign")
                            }
                            "criterion" => forged.nodes[index]
                                .acceptance
                                .criteria
                                .push("foreign".into()),
                            "executor" => forged.nodes[index].executor_kind = "verify".into(),
                            "edge" => {
                                forged.edges.remove(0);
                            }
                            "extra-node" => {
                                let mut extra = forged.nodes[index].clone();
                                extra.id.push_str(":extra");
                                forged.nodes.push(extra);
                            }
                            _ => unreachable!(),
                        }
                        assert!(
                            AgentActionService::validate_native_retirement_target_guard(
                                &forged,
                                &forged,
                                &forged.nodes[index]
                            )
                            .is_err(),
                            "{target:?}/{ingress}/{mutation}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn r1_native_retirement_rejects_foreign_compiler_guard_executor_criterion_and_owner() {
        for target in [
            RuntimeCompileTarget::InlineModel,
            RuntimeCompileTarget::EvidenceGraph,
            RuntimeCompileTarget::ExecutionGraph,
        ] {
            let original = compiled_child(target);
            let guards = original
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::Verify)
                .cloned()
                .collect::<Vec<_>>();
            for guard in guards {
                for mutation in [
                    "executor",
                    "legacy-verify",
                    "criterion",
                    "extra-criterion",
                    "parent",
                    "parent-graph",
                    "missing-owner",
                    "lineage",
                    "graph",
                    "node",
                    "kind",
                    "idempotency",
                    "edge",
                    "payload",
                ] {
                    let mut before = original.clone();
                    let mut child = original.clone();
                    let index = child
                        .nodes
                        .iter()
                        .position(|node| node.id == guard.id)
                        .unwrap();
                    match mutation {
                        "executor" => child.nodes[index].executor_kind = "foreign-guard".into(),
                        "legacy-verify" => child.nodes[index].executor_kind = "verify".into(),
                        "criterion" => {
                            child.nodes[index].acceptance.criteria =
                                vec!["foreign-criterion".into()]
                        }
                        "extra-criterion" => child.nodes[index]
                            .acceptance
                            .criteria
                            .push("foreign-criterion".into()),
                        "parent" => child
                            .parent_execution
                            .as_mut()
                            .unwrap()
                            .node_id
                            .push_str(":foreign"),
                        "parent-graph" => child
                            .parent_execution
                            .as_mut()
                            .unwrap()
                            .execution_id
                            .push_str(":foreign"),
                        "missing-owner" => child.parent_execution = None,
                        "lineage" => child.lineage.as_mut().unwrap().generation += 1,
                        "graph" => child.id.push_str(":foreign"),
                        "node" => child.nodes[index].id.push_str(":foreign"),
                        "kind" => child.nodes[index].kind = ExecutionNodeKind::ToolBatch,
                        "idempotency" => child.nodes[index].idempotency_key.push_str(":foreign"),
                        "edge" => {
                            child.edges.remove(0);
                        }
                        "payload" => child.nodes[index].payload_ref = "foreign-payload".into(),
                        _ => unreachable!(),
                    }
                    if matches!(
                        mutation,
                        "executor"
                            | "legacy-verify"
                            | "criterion"
                            | "extra-criterion"
                            | "node"
                            | "kind"
                            | "idempotency"
                    ) {
                        before.nodes[index] = child.nodes[index].clone();
                    }
                    assert!(
                        AgentActionService::validate_native_retirement_target_guard(
                            &before,
                            &child,
                            &child.nodes[index]
                        )
                        .is_err(),
                        "{target:?}/{mutation}"
                    );
                }
            }
        }
    }
}

/// A blocked exit is only "earned" when the Program is quiescent — the model has
/// already exhausted the work it can do. While any Task can still be claimed,
/// submitted or reviewed, the model is not stuck and must keep working instead.
pub(crate) const BLOCKED_EXIT_MAX_REMAINING_WORK: usize = 0;

/// True when every Task is terminal, i.e. the Program can make no further
/// progress on its own and only the model's judgement remains.
fn agentic_program_is_quiescent(projection: &AgenticProgramProjection) -> bool {
    projection
        .tasks
        .values()
        .filter(|task| {
            !matches!(
                task.status,
                AgenticTaskStatus::Accepted
                    | AgenticTaskStatus::Withdrawn
                    | AgenticTaskStatus::Superseded
            )
        })
        .count()
        <= BLOCKED_EXIT_MAX_REMAINING_WORK
}

/// Attach explicit cross-source assessment without mutating the Program read
/// model. A committed action stays Applied even if its subsequent read fails.
fn attach_completion_assessment(
    observation: &mut AgentActionObservation,
    actor: &harness_contract::agent_action::AgentActorBinding,
    program: &AgenticProgramProjection,
    assessment: impl FnOnce() -> Result<
        crate::execution_core::goal::assessment::BoundGoalAssessment,
        crate::execution_core::goal::assessment::AssessmentReadError,
    >,
) {
    if !matches!(
        actor.kind,
        AgentActorKind::Root | AgentActorKind::Supervisor
    ) {
        return;
    }
    match assessment() {
        Ok(assessment) => {
            let view = assessment.view(program);
            if let Some(value) = observation
                .projection
                .as_mut()
                .and_then(serde_json::Value::as_object_mut)
            {
                value.insert(
                    "completion_assessment".into(),
                    view["completion_assessment"].clone(),
                );
                value.insert("next_rungs".into(), view["remaining"].clone());
            }
            if let Some(gap) = assessment.gap() {
                observation.actionable = actionable(program, observation.duplicate, Some(gap));
            } else if !view["blocked"].is_null() {
                observation.actionable = vec![format!(
                    "Continue the original bound work; completion is not ready: {}",
                    view["blocked"]
                )];
            }
        }
        Err(error) => {
            if let Some(value) = observation
                .projection
                .as_mut()
                .and_then(serde_json::Value::as_object_mut)
            {
                value.insert(
                    "completion_assessment".into(),
                    json!({"status":"unknown","error":error.to_string()}),
                );
            }
            // Append rather than replace: an unavailable assessment is one fact
            // about the page, not a reason to drop the hints that still name
            // work the root must act on.
            observation.actionable.push(format!("Completion assessment is unavailable: {error}. Inspect the original bound sources; no completion readiness is established."));
        }
    }
}

#[cfg(test)]
impl AgentActionService {
    /// Test-only entry for fixtures outside this module to commit Runtime's
    /// supervisor dispatch without widening production visibility.
    pub(crate) fn apply_runtime_supervisor_internal_for_test(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        self.apply_runtime_supervisor(envelope)
    }
}
