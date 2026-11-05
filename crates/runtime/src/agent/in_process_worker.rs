use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommandRequest, AgentInput, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
};
use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};
use sha2::{Digest, Sha256};

use crate::{
    ContextProfile, PermissionMode, PermissionPolicy, RuntimeExecutionHost, RuntimeServices,
    RuntimeToolExecutionRequest, RuntimeToolExecutionStatus, Session, SharedPrompter,
    StandardRuntimeHost, StandardRuntimeHostConfig, ToolError, ToolExecutor,
};

use crate::agent_model_selector::AgentModelSelection;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
use crate::agent_runtime::AgentRuntimeBackend;
use crate::execution_core::graph::ScopeLockManager;

#[cfg(any(test, feature = "test-fixtures"))]
#[path = "in_process/exit_hold.rs"]
mod exit_hold;

#[path = "in_process/stages.rs"]
mod stages;
use stages::*;
#[path = "in_process/terminal.rs"]
mod terminal;
use terminal::*;
#[path = "in_process/model_loop.rs"]
mod model_loop;
#[path = "in_process/tool_turn.rs"]
mod tool_turn;
use tool_turn::*;
pub(crate) use tool_turn::OriginalPacketScopeDenial;
#[path = "in_process/path_scope.rs"]
mod path_scope;
pub(crate) use path_scope::resource_path_is_authorized;
#[path = "in_process/tool_scope.rs"]
mod tool_scope;
use path_scope::*;
#[path = "in_process/prompt.rs"]
mod prompt;
use prompt::*;
#[path = "in_process/evidence_collector.rs"]
mod evidence_collector;
use evidence_collector::*;
pub(crate) use evidence_collector::{
    structured_agent_output, structured_agent_output_for_fields,
    structured_contract_field_materialized,
};

/// Runtime-owned ToolHost bridge for an approved ProcessJsonl worker.
///
/// The external process may decide which allowed tool to request, but every
/// effect, receipt, scope lock and acceptance fact remains in the exact same
/// `ScopedRuntimeToolExecutor` used by the native worker. The process never
/// receives a raw ToolHost capability and therefore cannot mint evidence or
/// change receipts in its terminal JSON.
#[derive(Clone)]
pub(crate) struct ProcessJsonlToolSession {
    services: Arc<RuntimeServices>,
    executor: Arc<ScopedRuntimeToolExecutor>,
    artifact_store: Arc<crate::ArtifactStore>,
    external_model_profile: String,
    topic_actions: crate::AgentActionService,
    topic_packet: AgentTaskPacket,
    topic_transport: Arc<Mutex<crate::agentic::topic_delivery::TopicTransport>>,
    permission_policy: PermissionPolicy,
    authorization_negotiator: crate::AuthorizationNegotiator,
    event_store: Arc<crate::RuntimeEventStore>,
    requests: Arc<Mutex<ProcessRequestScope>>,
    steps: Arc<Mutex<VecDeque<Arc<crate::step_snapshot::BoundStep>>>>,
}

/// Live Process call provenance. This carries no member admission: the
/// original Process negotiator must still authorize each member separately.
/// Constructed only from the running Process slot and active Session ingress.
pub(crate) struct OriginalProcessBatchCall {
    ticket: crate::execution_core::graph::NodeExecutionTicket,
    binding: crate::step_snapshot::StepCallBinding,
    input: serde_json::Value,
    model_input: String,
    outer_input_facts: std::sync::OnceLock<OriginalProcessOuterInputFacts>,
    packet: AgentTaskPacket,
    store: Arc<crate::RuntimeEventStore>,
    actions: crate::AgentActionService,
    policy: PermissionPolicy,
    member_policy: Arc<crate::conversation::SharedPolicyOwner>,
    negotiator: crate::AuthorizationNegotiator,
    fence: crate::SessionExecutionFence,
    root_control: Arc<crate::execution_core::services::TurnCancellationControl>,
    claim: Arc<crate::execution_core::services::OriginalSessionClaim>,
    session_input: crate::session_input::SessionInputStream,
    control_requested: Arc<crate::conversation::OriginalTurnInputControl>,
    stop: crate::bound_tool_batch::BoundToolStop,
}

/// Process adapter's admitted input representation and the exact request
/// emitted by that call. The adapter has already serialized the worker's JSON
/// value; these are not the worker's original wire bytes. This attests only
/// the current unmodified Process path, not a future outer hook result.
pub(crate) struct OriginalProcessOuterInputFacts {
    model_input: String,
    effective_input: String,
    invocation: harness_contract::tool::GovernedToolInvocation,
}
impl OriginalProcessOuterInputFacts {
    pub(crate) fn model_input(&self) -> &str { &self.model_input }
    pub(crate) fn effective_input(&self) -> &str { &self.effective_input }
    pub(crate) fn invocation(&self) -> &harness_contract::tool::GovernedToolInvocation { &self.invocation }
}

/// Same-call member preparation. Only the Process producer can construct it;
/// it is neither a lease nor a decision that the member is Fresh.
pub(crate) struct OriginalProcessPreparedMember {
    owner: Arc<OriginalProcessBatchCall>,
    original: Arc<crate::RuntimeToolExecutionRequest>,
    prepared: crate::step_snapshot::PreparedAgentBatchMember,
    pinned: tools::ToolHostLease,
    original_hook: tokio::sync::OnceCell<Result<OriginalProcessMemberHookFacts, String>>,
    reservation: std::sync::OnceLock<OriginalProcessMemberReservation>,
    effect_fence: std::sync::OnceLock<Arc<crate::runtime_event_store::OriginalMemberEffectFence>>,
    member_timeout_secs: std::sync::OnceLock<u64>,
}

struct OriginalProcessMemberReservation {
    token: Arc<crate::runtime_event_store::OriginalMemberReservation>,
}

/// Immutable output of the real original turn hook for exactly one member.
/// Constructed only inside the Process member owner, never deserialized.
pub(crate) struct OriginalProcessMemberHookFacts {
    original: crate::step_snapshot::PreparedAgentBatchMember,
    hook_effective_input: String,
    effective: Option<crate::step_snapshot::PreparedAgentBatchMember>,
    validation_error: Option<String>,
    context: crate::PermissionContext,
    owner_live_after_hook: bool,
    authorization_compatible: bool,
}
impl OriginalProcessMemberHookFacts {
    pub(crate) fn original(&self) -> &crate::step_snapshot::PreparedAgentBatchMember { &self.original }
    pub(crate) fn hook_effective_input(&self) -> &str { &self.hook_effective_input }
    pub(crate) fn effective(&self) -> Option<&crate::step_snapshot::PreparedAgentBatchMember> { self.effective.as_ref() }
    pub(crate) fn validation_error(&self) -> Option<&str> { self.validation_error.as_deref() }
    pub(crate) fn context(&self) -> &crate::PermissionContext { &self.context }
    pub(crate) fn owner_live_after_hook(&self) -> bool { self.owner_live_after_hook }
    pub(crate) fn authorization_compatible(&self) -> bool { self.authorization_compatible }
}

impl OriginalProcessPreparedMember {
    pub(crate) fn outer_key(&self) -> &str { &self.prepared.outer_key }
    pub(crate) fn index(&self) -> usize { self.prepared.index }
    pub(crate) fn identity(&self) -> &str { &self.prepared.identity }
    pub(crate) fn tool(&self) -> &str { &self.prepared.tool }
    pub(crate) fn input(&self) -> &serde_json::Value { &self.prepared.input }
    pub(crate) fn effect(&self) -> &harness_contract::tool::ToolEffectDescriptor { &self.prepared.effect }
    pub(crate) fn step(&self) -> &crate::step_snapshot::StepCallBinding { self.owner.step() }
    pub(crate) fn ticket(&self) -> &crate::execution_core::graph::NodeExecutionTicket { self.owner.ticket() }
    pub(crate) fn packet(&self) -> &AgentTaskPacket { self.owner.packet() }
    pub(crate) fn claim(&self) -> &Arc<crate::execution_core::services::OriginalSessionClaim> { self.owner.claim() }
    pub(crate) fn owner(&self) -> &Arc<OriginalProcessBatchCall> { &self.owner }
    pub(crate) fn original_request(&self) -> &crate::RuntimeToolExecutionRequest { &self.original }
    pub(crate) fn uses_original_policy_owner(&self, policy: &crate::conversation::SharedPolicyOwner) -> bool {
        std::ptr::eq(self.owner.member_policy.as_ref(), policy)
    }
    pub(crate) fn original_pinned_host(&self) -> &tools::ToolHostLease { &self.pinned }
    pub(crate) fn original_prepared(&self) -> &crate::step_snapshot::PreparedAgentBatchMember { &self.prepared }
    pub(crate) fn effective_prepared(&self) -> Option<&crate::step_snapshot::PreparedAgentBatchMember> {
        self.original_hook_facts().filter(|facts| facts.authorization_compatible()).and_then(|facts| facts.effective())
    }
    /// Read original turn state only inside the checked policy/source closure.
    /// Register both original cancellation paths before returning the control.
    pub(crate) fn locked_commit_context(&self)
        -> Result<(Arc<crate::conversation::MemberCommitControl>, Arc<crate::execution_core::services::OriginalSessionClaim>, usize), String> {
        let owner = &self.owner;
        owner.check_original_stop()?;
        let turn = harness_contract::turn::TurnId::from_string(owner.claim.turn_id());
        if owner.root_control.token().is_cancelled()
            || owner.control_requested.load(Ordering::Acquire)
            || owner.session_input.active_turn_id().as_ref() != Some(&turn)
            || chrono::Utc::now().timestamp_millis() >= self.step().snapshot.expires_at_ms
            || self.effective_prepared().is_none() || owner.outer_input_facts().is_none() {
            return Err("original Process member turn, Step or facts unavailable".into());
        }
        let fence = owner.claim.fence()?;
        let cursor = owner.session_input.highest_consumed_cursor(&turn);
        if owner.session_input.active_turn_id().as_ref() != Some(&turn) {
            return Err("original Process turn changed while reading cursor".into());
        }
        let consumed = match cursor {
            Some(cursor) if cursor.generation == fence.session_generation =>
                usize::try_from(cursor.sequence).map_err(|_| "original consumed cursor overflow")?.max(fence.input_sequence),
            Some(_) => return Err("original consumed cursor generation mismatch".into()),
            None => fence.input_sequence,
        };
        let control = owner.root_control.token().member_commit_control();
        owner.control_requested.register(&control);
        owner.stop.register_commit_control(&control);
        let child_stop = crate::bound_tool_batch::current_tool_stop().ok_or("original Process child stop unavailable")?;
        child_stop.register_commit_control(&control);
        Ok((control, Arc::clone(&owner.claim), consumed))
    }
    pub(crate) fn original_permission_context(&self) -> Option<&crate::PermissionContext> {
        self.original_hook_facts().filter(|facts| facts.authorization_compatible()).map(|facts| facts.context())
    }
    pub(crate) fn original_member_timeout_secs(&self) -> Option<u64> {
        self.member_timeout_secs.get().copied()
    }
    /// Prepare the assessment of this member from the hook and policy that
    /// actually ran for it. The original checked member transaction publishes
    /// the event with the corresponding lease and action.
    pub(crate) fn prepare_member_assessment(
        &self,
        assessment: &harness_contract::policy::CapabilityAssessment,
        approval: Option<&harness_contract::policy::ApprovalGrant>,
    ) -> Result<crate::conversation::PreparedHostMemberAssessment, String> {
        let hook = self.original_hook_facts().filter(|facts| facts.authorization_compatible())
            .ok_or("original Process member hook unavailable")?;
        let member = hook.effective().ok_or("original Process member effective input unavailable")?;
        if !self.original_reservation_matches() || assessment.capability != member.tool
            || assessment.lease.as_ref().is_none_or(|lease| lease.idempotency_key != member.identity) {
            return Err("original Process member assessment owner differs".into());
        }
        let lease = assessment.lease.as_ref().ok_or("original Process member assessment lease missing")?;
        let mut payload = serde_json::to_value(assessment).map_err(|e| e.to_string())?;
        let object = payload.as_object_mut().ok_or("original Process member assessment malformed")?;
        let context = hook.context();
        let decision = context.override_decision().map(|value| match value {
            crate::permissions::PermissionOverride::Allow => "allow",
            crate::permissions::PermissionOverride::Deny => "deny",
            crate::permissions::PermissionOverride::Ask => "ask",
        });
        object.insert("_original_permission_context".into(), serde_json::json!({
            "override_decision": decision, "override_reason": context.override_reason(),
        }));
        if let Some(grant) = approval {
            object.insert("_original_approval_grant".into(), serde_json::to_value(grant).map_err(|e| e.to_string())?);
        }
        object.insert("_original_batch_member_binding".into(), serde_json::json!({
            "version": 2, "step": self.step(), "ticket": self.ticket(), "packet": self.packet(),
            "outer_invocation": self.original.compiled_invocation, "outer_input": self.original.input,
            "parent": self.original.parent_execution, "attempt": self.original.parent_execution_attempt,
            "index": member.index, "outer_key": member.outer_key, "member_tool": member.tool,
            "member_input": member.input, "member_effect": member.effect, "member_identity": member.identity,
            "original_prepared": hook.original(), "hook_effective": hook.hook_effective_input(),
            "normalized_effective": serde_json::to_string(&member.input).map_err(|e| e.to_string())?,
            "timeout_secs": self.original_member_timeout_secs().ok_or("original Process member timeout unavailable")?,
        }));
        let stream_id = format!("session:{}", self.packet().session_id());
        let expected_revision = self.owner.store.stream_revision(&stream_id).map_err(|e| e.to_string())?;
        Ok(crate::conversation::PreparedHostMemberAssessment {
            source: crate::ExpectedStreamRevision { stream_id: stream_id.clone(), expected_revision },
            event: crate::RuntimeTransactionEventInput { event: crate::RuntimeEventInput {
                stream_id, scope: crate::RuntimeEventScope::Tool, kind: "authorization.capability_assessed".into(),
                status: Some(format!("{:?}", assessment.path).to_ascii_lowercase()),
                actor: Some("runtime.process_jsonl".into()),
                refs: vec![crate::RuntimeEventRef { kind: "capability".into(), id: assessment.capability.clone() },
                    crate::RuntimeEventRef { kind: "authorization_lease".into(), id: lease.lease_id.clone() }], payload,
            }, idempotency_key: Some(format!("member-assessment:{}", member.identity)), schema_version: 1 },
        })
    }
    pub(crate) fn original_hook_facts(&self) -> Option<&OriginalProcessMemberHookFacts> {
        self.original_hook.get().and_then(|result| result.as_ref().ok())
    }
    pub(crate) fn original_reservation(&self) -> Option<&crate::runtime_event_store::OriginalMemberReservation> {
        self.reservation.get().map(|reservation| reservation.token.as_ref())
    }
    pub(crate) fn original_reservation_matches(&self) -> bool {
        let Ok(index) = u64::try_from(self.index()) else { return false; };
        self.original_reservation().is_some_and(|token|
            token.matches_original(&self.owner.store, self.outer_key(), index, self.ticket()))
    }
    pub(crate) fn original_effect_fence(&self) -> Option<&Arc<crate::runtime_event_store::OriginalMemberEffectFence>> {
        self.effect_fence.get()
    }
    pub(crate) fn original_preparation_fence_live(&self) -> bool {
        self.original_reservation_matches() && self.effect_fence.get().is_some()
    }
    fn reserve_original_member(&self) -> Result<(), String> {
        if self.reservation.get().is_some() { return Err("Process member reservation already installed".into()); }
        self.owner.check_original_stop()?;
        let index = u64::try_from(self.index()).map_err(|_| "Process member index overflow")?;
        let token = self.owner.store.reserve_original_member(self.outer_key(), index, self.ticket())
            .map_err(|error| error.to_string())?;
        let reservation = OriginalProcessMemberReservation { token: Arc::new(token) };
        self.reservation.set(reservation).map_err(|_| "Process member reservation raced".to_string())?;
        if !self.original_reservation_matches() { return Err("Process member reservation owner changed".into()); }
        Ok(())
    }
    /// Only the original decision owner may call this after resolving any
    /// committed retry. It prepares a candidate and consumes no lease.
    async fn original_authorization_request(&self) -> Result<crate::AuthorizationRequest, String> {
        self.owner.check_original_stop()?;
        if !self.original_reservation_matches() || self.effect_fence.get().is_none() { return Err("original Process member preparation fence unavailable".into()); }
        let timeout = crate::ToolPolicy.agent_action_member_timeout_secs();
        match self.member_timeout_secs.set(timeout) {
            Ok(()) => (),
            Err(value) if self.original_member_timeout_secs() == Some(value) => (),
            Err(_) => return Err("original Process member timeout changed".into()),
        }
        let context = self.capture_original_permission_context().await?.clone();
        let prepared = self.effective_prepared().ok_or("original Process effective member unavailable")?;
        let input = serde_json::to_string(&prepared.input).map_err(|error| error.to_string())?;
        Ok(crate::AuthorizationRequest {
            principal_id: format!("agent:{}", self.packet().agent_id()), capability: prepared.tool.clone(), input,
            idempotency_key: prepared.identity.clone(), effect: prepared.effect.clone(),
            parent_ceiling: self.packet().permission_ceiling, parent_lease_id: None,
            policy_revision: self.original.policy_revision,
            recovery_scope: format!("execution:{}", self.packet().graph_id()), context, safe_alternatives: vec![],
        })
    }
    async fn prepare_authority_after_replay(
        self: &Arc<Self>,
    ) -> Result<Arc<crate::authorization_negotiator::PreparedMemberAuthority>, String> {
        let request = self.original_authorization_request().await?;
        match self.owner.negotiator().prepare_process_member_once(Arc::clone(self), request)? {
            crate::authorization_negotiator::ProcessMemberPreparation::Ready(authority) => Ok(authority),
            crate::authorization_negotiator::ProcessMemberPreparation::Approval(evaluation) =>
                self.owner.member_policy.prepare_process_member_approval(&evaluation).await,
        }
    }
    /// Called by the original action ingress after replay resolution. The
    /// synchronous closure owns the policy/source locks and checked append;
    /// the candidate's approval and lease owners remain outside those locks.
    pub(crate) fn commit_checked(
        self: &Arc<Self>, services: &crate::RuntimeServices, authority: &Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
        commit: impl FnOnce(&crate::authorization_negotiator::PreparedMemberAuthority, &crate::runtime_event_store::OriginalMemberReservation, &crate::runtime_event_store::OriginalMemberEffectFence)
            -> Result<Arc<crate::OriginalCommittedMember>, crate::AgentActionServiceError>,
    ) -> Result<(Arc<crate::OriginalCommittedMember>, bool), crate::AgentActionServiceError> {
        let member_guard = authority.member().upgrade().ok_or_else(|| crate::AgentActionServiceError::MemberUnknown("original Process member owner unavailable".into()))?;
        if member_guard.process().is_none_or(|original| !Arc::ptr_eq(original, self))
            || self.effective_prepared().is_none_or(|effective| effective != authority.member().prepared()) {
            return Err(crate::AgentActionServiceError::Corrupt("Process member authority owner changed".into()));
        }
        self.owner.check_original_stop().map_err(crate::AgentActionServiceError::Corrupt)?;
        if !self.original_reservation_matches() {
            return Err(crate::AgentActionServiceError::MemberUnknown("original Process member reservation mismatched".into()));
        }
        let reservation = self.original_reservation().ok_or_else(|| crate::AgentActionServiceError::MemberUnknown("original Process member reservation unavailable".into()))?;
        let effect_fence = self.effect_fence.get().ok_or_else(|| crate::AgentActionServiceError::MemberUnknown("original Process member preparation fence unavailable".into()))?;
        authority.with_original_owners_for_checked_commit(services, &member_guard, |candidate| commit(candidate, reservation, effect_fence))
    }
    /// The fresh effect owner must already have decided this member. The
    /// original Session policy owner executes the one hook and derives the
    /// effective member; concurrent callers share its immutable result.
    async fn capture_original_permission_context(&self) -> Result<&crate::PermissionContext, String> {
        let facts = self.capture_original_hook_facts().await?;
        if !facts.authorization_compatible() { return Err("Process member hook input requires replan".into()); }
        Ok(facts.context())
    }
    async fn capture_original_hook_facts(&self) -> Result<&OriginalProcessMemberHookFacts, String> {
        let result = self.original_hook.get_or_init(|| async {
            if !self.original_reservation_matches() || self.effect_fence.get().is_none() {
                return Err("original Process member preparation fence unavailable".into());
            }
            let facts = self.owner.member_policy.prepare_original_process_batch_policy_input(self)
                .await.map_err(|error| error.to_string())?;
            if facts.original() != &self.prepared || !Arc::ptr_eq(facts.claim(), &self.owner.claim) {
                return Err("Process original member or Session changed during hook".into());
            }
            self.owner.check_original_stop()?;
            Ok(OriginalProcessMemberHookFacts {
                original: facts.original().clone(),
                hook_effective_input: facts.hook_effective().to_string(),
                effective: Some(facts.prepared().clone()),
                validation_error: None,
                context: facts.context().clone(),
                owner_live_after_hook: true,
                authorization_compatible: true,
            })
        }).await;
        result.as_ref().map_err(Clone::clone)
    }

}


impl std::fmt::Debug for OriginalProcessBatchCall {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("OriginalProcessBatchCall").finish_non_exhaustive()
    }
}

struct OriginalProcessBatchScope {
    owner: Arc<OriginalProcessBatchCall>,
    call_slot: std::sync::Weak<ProcessCallSlot>,
    original_request: std::sync::OnceLock<Arc<crate::RuntimeToolExecutionRequest>>,
    members: Mutex<Option<Vec<Result<Arc<OriginalProcessPreparedMember>, String>>>>,
    policy_owner: std::sync::OnceLock<Arc<dyn crate::tool_host::AgentBatchPolicyOwner>>,
}

/// Same-call publication state; it does not own the Process slot or request.
pub(crate) struct ProcessMemberCustody {
    binding: Mutex<Option<(crate::execution_core::NodeExecutionTicket, std::sync::Weak<crate::execution_core::graph::executors::agent_tool::NoDispatchObservation>)>>,
    members: Mutex<BTreeMap<usize, (std::sync::Weak<OriginalProcessPreparedMember>, Arc<crate::tool_host::OriginalMemberSettlement>)>>,
    unregistered: std::sync::atomic::AtomicBool,
}
impl ProcessMemberCustody {
    fn new() -> Self { Self { binding: Mutex::new(None), members: Mutex::new(BTreeMap::new()), unregistered: Default::default() } }

    pub(crate) fn bind(&self, ticket: &crate::execution_core::NodeExecutionTicket, observation: Arc<crate::execution_core::graph::executors::agent_tool::NoDispatchObservation>) -> Result<(), String> {
        let mut binding = self.binding.lock().map_err(|_| "Process child binding unavailable")?;
        match binding.as_ref() {
            Some((old, obs)) if old == ticket && obs.upgrade().is_some_and(|obs| Arc::ptr_eq(&obs, &observation)) => Ok(()),
            Some(_) => Err("Process child binding conflicts".into()),
            None => { *binding = Some((ticket.clone(), Arc::downgrade(&observation))); Ok(()) }
        }
    }

    fn register(&self, member: &Arc<OriginalProcessPreparedMember>) -> Result<(), String> {
        let binding = self.binding.lock().map_err(|_| "Process child binding unavailable")?;
        let (ticket, observation) = binding.as_ref().ok_or("Process child binding missing")?;
        let observation = observation.upgrade().ok_or("Process child observation unavailable")?;
        let index = member.index();
        let mut members = self.members.lock().map_err(|_| "Process member custody unavailable")?;
        if let Some((old, _)) = members.get(&index) {
            return if old.upgrade().is_some_and(|old| Arc::ptr_eq(&old, member)) { Ok(()) }
                else { Err("Process member custody conflicts".into()) };
        }
        let unit = Arc::new(crate::tool_host::OriginalMemberSettlement::default());
        observation.register_member_custody(ticket, &member.original_request().tool_use_id, index, Arc::clone(&unit))?;
        members.insert(index, (Arc::downgrade(member), unit));
        Ok(())
    }

    /// The named admission refusal this node's owner recorded for one call,
    /// read back through the observation this child was bound to. It is the
    /// parent's own reason, so a delegated action that was never admitted can
    /// report which conjunct refused it instead of a generic wording.
    pub(crate) fn parent_refusal_reason(&self, call_id: &str) -> Option<String> {
        let binding = self.binding.lock().ok()?;
        let (_, observation) = binding.as_ref()?;
        observation.upgrade()?.refusal_reason(call_id)
    }

    pub(crate) fn verified_units(
        &self, ticket: &crate::execution_core::NodeExecutionTicket,
        child: &crate::execution_core::graph::executors::OriginalAgentChildCall,
        facts: &[serde_json::Value], pending: Option<&serde_json::Value>,
    ) -> Result<Vec<Arc<crate::tool_host::OriginalMemberSettlement>>, String> {
        let binding = self.binding.lock().map_err(|_| "Process child binding unavailable")?;
        if binding.as_ref().is_none_or(|(original, _)| original != ticket) { return Err("Process child ticket differs".into()); }
        let members = self.members.lock().map_err(|_| "Process member custody unavailable")?;
        let outcomes = child.request.tool_progress.original_members();
        if let Some(fact) = pending {
            let index = fact.get("index").and_then(serde_json::Value::as_u64)
                .and_then(|value| usize::try_from(value).ok()).ok_or("Process terminal index unavailable")?;
            if !matches!(fact.get("state").and_then(serde_json::Value::as_str), Some("confirmed_noncommit" | "before_outcome_unknown" | "before_outcome_error" | "before_outcome_store_unknown"))
                || !members.contains_key(&index) { return Err("Process terminal custody unavailable".into()); }
        }
        let mut units = Vec::with_capacity(members.len());
        for (index, (original, unit)) in members.iter() {
            let original = original.upgrade().ok_or("Process member owner unavailable")?;
            if original.index() != *index || original.ticket() != &child.parent_ticket
                || original.original_request() != child.request.as_ref()
                || !original.original_request().tool_progress.same_original_channels(&child.request.tool_progress) {
                return Err("Process member original identity differs".into());
            }
            let mut matching = outcomes.iter().filter(|(actual, _)| actual == index);
            let Some((_, outcome)) = matching.next() else {
                let pending = pending.ok_or("Process member outcome unavailable")?;
                child.request.tool_progress.reconcile_original_member_stop();
                let publications = child.request.tool_progress.checked_original_stop_publications()?;
                let [publication] = publications.as_slice() else { return Err("Process terminal publications conflict".into()); };
                let crate::tool_host::OriginalStopPublication::Recorded(stop) = publication.as_ref() else { return Err("Process terminal publication unresolved".into()); };
                let crate::tool_host::OriginalStopRead::Available(Some(current)) = child.request.tool_progress.original_member_stop() else { return Err("Process terminal slot unavailable".into()); };
                if !Arc::ptr_eq(stop, &current) || !stop.recorded_in_original_sink()
                    || !stop.belongs_to_original_sink(&child.request.tool_progress)
                    || stop.coordinate().ticket() != &child.parent_ticket || stop.coordinate().index() != *index
                    || stop.coordinate().call_id() != child.request.tool_use_id
                    || stop.coordinate().outer_key() != child.request.idempotency_key
                    || stop.coordinate().step() != child.request.step_binding.as_ref() { return Err("Process terminal stop differs".into()); }
                let crate::tool_host::OriginalBatchMemberStopReason::ConfirmedNoncommit(terminal) = stop.reason() else { return Err("Process terminal unresolved".into()); };
                child.store.validate_original_member_noncommit_node(terminal, &child.parent_ticket,
                    &child.request.idempotency_key, *index as u64, pending).map_err(|_| "Process terminal node fact differs")?;
                if !terminal.matches_prepared(original.identity(), &child.request) {
                    return Err("Process original terminal member differs".into());
                }
                units.push(Arc::clone(unit));
                continue;
            };
            if matching.next().is_some() || pending.is_some_and(|fact|
                fact.get("index").and_then(serde_json::Value::as_u64) == u64::try_from(*index).ok()) {
                return Err("Process member outcome conflicts".into());
            }
            let crate::tool_host::OriginalMemberSubmitOutcome::Committed(member) = outcome.as_ref() else {
                return Err("Process member outcome unresolved".into());
            };
            if member.index() != *index || member.action().payload().pointer("/member_admission/prepared/identity")
                .and_then(serde_json::Value::as_str) != Some(original.identity()) {
                return Err("Process committed member identity differs".into());
            }
            let expected = member.node_commit_fact(&child.store, &child.parent_ticket)?;
            if !facts.iter().any(|fact| {
                let mut source = fact.clone();
                source["ticket"] = serde_json::to_value(&child.parent_ticket).unwrap_or_default();
                source.as_object_mut().is_some_and(|obj| { obj.remove("source_ticket"); obj.remove("original_child_registration"); true })
                    && source == expected
            }) { return Err("Process member absent from original node".into()); }
            units.push(Arc::clone(unit));
        }
        Ok(units)
    }
    pub(crate) fn verified_release_units(
        &self, child: &crate::execution_core::graph::executors::OriginalAgentChildCall,
        expected: &[std::sync::Weak<crate::tool_host::OriginalMemberSettlement>],
    ) -> Result<Vec<Arc<crate::tool_host::OriginalMemberSettlement>>, String> {
        if self.unregistered.load(Ordering::Acquire) { return Err("Process member custody unresolved".into()); }
        let members = self.members.lock().map_err(|_| "Process member custody unavailable")?;
        if members.len() != expected.len() { return Err("Process release member set differs".into()); }
        let mut units = Vec::with_capacity(members.len());
        for ((index, (member, unit)), prior) in members.iter().zip(expected) {
            let member = member.upgrade().ok_or("Process member owner unavailable")?;
            if member.index() != *index || member.ticket() != &child.parent_ticket
                || member.original_request() != child.request.as_ref()
                || !member.original_request().tool_progress.same_original_channels(&child.request.tool_progress)
                || !std::sync::Weak::ptr_eq(prior, &Arc::downgrade(unit)) {
                return Err("Process release member identity differs".into());
            }
            units.push(Arc::clone(unit));
        }
        Ok(units)
    }
    fn unresolved(&self) -> bool {
        self.unregistered.load(Ordering::Acquire)
            || self.members.lock().map_or(true, |members| members.values().any(|(_, unit)| !unit.is_settled()))
    }
}

pub(crate) fn current_process_member_custody(request: &crate::RuntimeToolExecutionRequest) -> Result<Option<Arc<ProcessMemberCustody>>, String> {
    match ORIGINAL_PROCESS_BATCH_CALL.try_with(|scope| {
        let captured = scope.original_request.get().ok_or("Process original request unavailable")?;
        if captured.as_ref() != request || !captured.tool_progress.same_original_channels(&request.tool_progress) {
            return Err("Process original request differs".into());
        }
        let slot = scope.call_slot.upgrade().ok_or("Process original call slot unavailable")?;
        Ok(Arc::clone(&slot.member_custody))
    }) { Ok(result) => result.map(Some), Err(_) => Ok(None) }
}

tokio::task_local! {
    static ORIGINAL_PROCESS_BATCH_CALL: Arc<OriginalProcessBatchScope>;
}

pub(crate) fn current_original_process_batch_call() -> Option<Arc<OriginalProcessBatchCall>> {
    ORIGINAL_PROCESS_BATCH_CALL.try_with(|scope| Arc::clone(&scope.owner)).ok()
}

pub(crate) fn capture_original_process_members(
    request: &crate::RuntimeToolExecutionRequest,
    pinned: tools::ToolHostLease,
) -> Result<(), String> {
    ORIGINAL_PROCESS_BATCH_CALL.try_with(|scope| {
        scope.owner.validate_locked_child(request, scope.owner.ticket(), scope.owner.store().as_ref())?;
        scope.owner.capture_outer_input_facts(request)?;
        let mut members = scope.members.lock().map_err(|_| "Process member capture poisoned")?;
        if members.is_some() { return Err("Process members already captured for original call".into()); }
        let count = scope.owner.outer_input().get("actions").and_then(serde_json::Value::as_array)
            .filter(|actions| !actions.is_empty() && actions.len() <= 25)
            .ok_or("Process original batch members missing")?.len();
        let original = Arc::new(request.clone());
        scope.original_request.set(Arc::clone(&original)).map_err(|_| "Process original request already captured".to_string())?;
        *members = Some((0..count).map(|index| scope.owner.prepare_member(
            Arc::clone(&original), index, pinned.clone(), chrono::Utc::now().timestamp_millis(),
        )).collect());
        Ok(())
    }).map_err(|_| "Process original batch call scope missing".to_string())?
}

pub(crate) fn current_original_process_members()
    -> Option<Vec<Result<Arc<OriginalProcessPreparedMember>, String>>> {
    ORIGINAL_PROCESS_BATCH_CALL.try_with(|scope| scope.members.lock().ok().and_then(|members| members.clone())).ok().flatten()
}

struct OriginalProcessBatchPolicyOwner {
    process: Arc<OriginalProcessBatchCall>,
    members: Vec<Result<std::sync::Weak<OriginalProcessPreparedMember>, String>>,
    scope: std::sync::Weak<OriginalProcessBatchScope>,
}

pub(crate) fn original_process_batch_policy_owner() -> Result<Arc<dyn crate::tool_host::AgentBatchPolicyOwner>, String> {
    ORIGINAL_PROCESS_BATCH_CALL.try_with(|scope| {
        let members = scope.members.lock().map_err(|_| "Process member capture poisoned")?.clone()
            .ok_or("Process original members not captured")?;
        let members = members.into_iter().map(|member| member.map(|member| Arc::downgrade(&member))).collect();
        let owner = Arc::new(OriginalProcessBatchPolicyOwner { process: Arc::clone(&scope.owner), members, scope: Arc::downgrade(scope) })
            as Arc<dyn crate::tool_host::AgentBatchPolicyOwner>;
        scope.policy_owner.set(Arc::clone(&owner)).map_err(|_| "Process original policy owner already captured")?;
        Ok(owner)
    }).map_err(|_| "Process original batch scope unavailable".to_string())?
}

impl OriginalProcessBatchPolicyOwner {
    fn validate_original(&self, original: &crate::RuntimeToolExecutionRequest, index: usize) -> Result<(), String> {
        self.process.validate_locked_child(original, self.process.ticket(), self.process.store().as_ref())?;
        let scope = self.scope.upgrade().ok_or("Process original call scope unavailable")?;
        let request = scope.original_request.get().ok_or("Process original request unavailable")?;
        if request.as_ref() != original || !request.tool_progress.same_original_channels(&original.tool_progress)
            || self.process.input.get("actions").and_then(serde_json::Value::as_array)
                .and_then(|actions| actions.get(index)).is_none() {
            return Err("Process original request, sink or member index changed".into());
        }
        Ok(())
    }
    fn member(&self, original: &crate::RuntimeToolExecutionRequest, index: usize) -> Result<Arc<OriginalProcessPreparedMember>, String> {
        self.validate_original(original, index)?;
        let member = self.members.get(index).ok_or("Process original member index missing")?
            .as_ref().map_err(Clone::clone)?.upgrade().ok_or("Process original member no longer retained")?;
        if member.original_request() != original
            || !member.original_request().tool_progress.same_original_channels(&original.tool_progress) {
            return Err("Process original member request or sink changed".into());
        }
        Ok(member)
    }
}

#[async_trait::async_trait]
impl crate::tool_host::AgentBatchPolicyOwner for OriginalProcessBatchPolicyOwner {
    async fn prepare_member_pregrant(&self, original: &crate::RuntimeToolExecutionRequest, index: usize)
        -> crate::tool_host::AgentBatchMemberPregrant {
        use crate::tool_host::{AgentBatchMemberPregrant, BatchMemberUnknown, BatchMemberUnknownReason,
            OriginalBatchMemberCoordinate, OriginalBatchMemberStop, OriginalBatchMemberStopReason, OriginalMemberObservation};
        if let Err(reason) = self.validate_original(original, index) {
            return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                index, reason: BatchMemberUnknownReason::OriginalRequestNotAuthenticated(reason) }, stop: None };
        }
        let coordinate = || OriginalBatchMemberCoordinate::from_original(original, self.process.ticket(), index);
        let unknown = |reason: BatchMemberUnknownReason, detail: String| AgentBatchMemberPregrant::Unknown {
            failure: BatchMemberUnknown { index, reason },
            stop: Some(OriginalBatchMemberStop::publish(coordinate(), OriginalBatchMemberStopReason::BeforeOutcomeUnknown(detail))),
        };
        // A retained terminal is a result of this member, not a new admission.
        // Preserve the original publication and require the same live owner.
        let publications = match original.tool_progress.checked_original_stop_publications() {
            Ok(values) => values,
            Err(_) => return unknown(BatchMemberUnknownReason::OriginalTransactionFactsUnavailable,
                "original member stop publication unavailable".into()),
        };
        let retained_unknown = || AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
            index, reason: BatchMemberUnknownReason::OriginalTransactionFactsUnavailable },
            stop: if let [publication] = publications.as_slice() { Some(publication.as_ref().clone()) } else { None } };
        let stopped_winner = if !publications.is_empty() {
            let stopped = (|| {
                if publications.len() != 1 { return None; }
                let publication = &publications[0];
                if publication.failure().is_some() { return None; }
                let stop = publication.current();
                let slot = original.tool_progress.original_member_stop();
                if !matches!(slot, crate::tool_host::OriginalStopRead::Available(Some(ref value)) if Arc::ptr_eq(value, stop))
                    || !stop.recorded_in_original_sink() || !stop.belongs_to_original_sink(&original.tool_progress)
                    || stop.coordinate().ticket() != self.process.ticket()
                    || stop.coordinate().call_id() != original.tool_use_id
                    || stop.coordinate().outer_key() != original.idempotency_key
                    || stop.coordinate().step() != original.step_binding.as_ref()
                    || stop.coordinate().index() != index { return None; }
                stop.reconcile_original();
                Some(stop)
            })();
            let Some(stop) = stopped else { return retained_unknown(); };
            match stop.reason() {
                OriginalBatchMemberStopReason::ConfirmedNoncommit(terminal) => {
                    let valid = (|| {
                        let reservation = stop.original_reservation()?;
                        let fence = stop.original_effect_fence()?;
                        let recovered = self.process.store().original_member_noncommit(reservation, fence).ok()??;
                        if !terminal.same_original(&recovered) || !terminal.matches_request(original) { return None; }
                        self.process.store().validate_original_member_noncommit(terminal, self.process.ticket(),
                            &original.idempotency_key, u64::try_from(index).ok()?).ok()
                    })();
                    return if valid.is_some() { AgentBatchMemberPregrant::Terminal(Arc::clone(&publications[0])) }
                        else { retained_unknown() };
                },
                OriginalBatchMemberStopReason::CommittedUnbound(winner) => {
                    // Reentry after owner apply uses the same sink's typed committed
                    // result. Missing observation still cannot authorize another action.
                    let outcomes = original.tool_progress.original_members();
                    let mut matching = outcomes.iter().filter(|(actual, _)| *actual == index);
                    if let Some((_, outcome)) = matching.next() {
                        if matching.next().is_some() { return retained_unknown(); }
                        let crate::tool_host::OriginalMemberSubmitOutcome::Committed(member) = outcome.as_ref() else { return retained_unknown(); };
                        if member.same_owner_committed().is_some_and(|handle| handle.authority().is_none()
                            && handle.belongs_to(self.process.store()) && winner.original().same_original_commit(handle)) {
                            return AgentBatchMemberPregrant::Replay(member.clone());
                        }
                        return retained_unknown();
                    }
                    Some(Arc::clone(winner.original()))
                },
                _ => return retained_unknown(),
            }
        } else { None };
        match crate::agentic::process_original_member_replay(Arc::clone(&self.process), &self.process.actions, original, index, stopped_winner.as_ref()) {
            Ok(Some(replay)) => {
                if let Err(reason) = replay.owner_applied {
                    if stopped_winner.is_some() { return retained_unknown(); }
                    let stop = OriginalBatchMemberStop::publish(coordinate(), OriginalBatchMemberStopReason::CommittedUnbound(
                        crate::tool_host::OriginalMemberUnboundCommit { handle: replay.committed, reason }));
                    return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown { index,
                        reason: BatchMemberUnknownReason::OriginalTransactionFactsUnavailable }, stop: Some(stop) };
                }
                if stopped_winner.is_some() {
                    // Retain the completed owner handoff before returning across the
                    // async caller boundary. Keep the original stop/publication intact.
                    original.tool_progress.record_original_member(index, Arc::new(
                        crate::tool_host::OriginalMemberSubmitOutcome::Committed(replay.member.clone())));
                    return AgentBatchMemberPregrant::Replay(replay.member);
                }
                if matches!(replay.member.observation(), OriginalMemberObservation::Unavailable) {
                    let stop = OriginalBatchMemberStop::publish(coordinate(), OriginalBatchMemberStopReason::CommittedObservationUnavailable(replay.member));
                    return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown { index,
                        reason: BatchMemberUnknownReason::OriginalTransactionFactsUnavailable }, stop: Some(stop) };
                }
                return AgentBatchMemberPregrant::Replay(replay.member);
            }
            Ok(None) if stopped_winner.is_some() => return retained_unknown(),
            Ok(None) => {},
            Err(_) if stopped_winner.is_some() => return retained_unknown(),
            Err(reason) => return unknown(reason, "original Process member could not be authenticated".into()),
        }
        let member = match self.member(original, index) {
            Ok(member) => member,
            Err(reason) => return unknown(BatchMemberUnknownReason::Preparation(reason.clone()), reason),
        };
        // Only the effect owner produced by this original call can authorize
        // the durable preparation fence; an absent owner is never Fresh.
        let fresh_effect = match original.tool_progress.original_fresh_effect() {
            Ok(Some(owner)) => owner,
            _ => return unknown(BatchMemberUnknownReason::OriginalTransactionFactsUnavailable,
                "original fresh effect owner unavailable".into()),
        };
        let Some(slot) = self.scope.upgrade().and_then(|scope| scope.call_slot.upgrade()) else {
            let reason = "original Process call slot unavailable".to_string();
            return unknown(BatchMemberUnknownReason::Preparation(reason.clone()), reason);
        };
        // Keep the same private member and hook facts after a marker attempt,
        // including when preparation or the marker reply is unknown.
        slot.retained_members.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(Arc::clone(&member));
        if let Err(reason) = slot.member_custody.register(&member) {
            slot.member_custody.unregistered.store(true, Ordering::Release);
            return unknown(BatchMemberUnknownReason::Preparation(reason.clone()), reason);
        }
        if let Err(reason) = member.reserve_original_member() {
            return unknown(BatchMemberUnknownReason::Preparation(reason.clone()), reason);
        }
        let reservation = member.original_reservation().expect("reservation was installed by this member owner");
        // A retains the original append request and token before attempting the
        // effect marker. Any failure here can be an unknown append outcome.
        let fence = match self.process.store.acquire_original_member_preparation_fence(
            reservation, original, self.process.ticket(), index, &fresh_effect, Arc::clone(self.process.claim()),
        ) {
            Ok(fence) => fence,
            Err(error) => {
                let error = Arc::new(error);
                let reservation = Arc::clone(&member.reservation.get().expect("original reservation retained").token);
                let stop = OriginalBatchMemberStop::publish_with_reservation(coordinate(),
                    OriginalBatchMemberStopReason::BeforeOutcomeStoreError(Arc::clone(&error)), reservation);
                return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                    index, reason: BatchMemberUnknownReason::OriginalOwnerStore(error) }, stop: Some(stop) };
            }
        };
        if member.effect_fence.set(Arc::clone(&fence)).is_err() {
            return unknown(BatchMemberUnknownReason::OriginalTransactionFactsUnavailable,
                "original Process preparation fence already installed".into());
        }
        // This candidate consumes no lease. If approval is required the E1
        // owner must supply the original approval; do not manufacture one.
        let authority = match member.prepare_authority_after_replay().await {
            Ok(authority) => authority,
            Err(reason) => {
                let reservation = Arc::clone(&member.reservation.get().expect("original reservation retained").token);
                let stop = OriginalBatchMemberStop::publish_with_owners(coordinate(),
                    OriginalBatchMemberStopReason::BeforeOutcomeUnknown(reason.clone()), reservation, Arc::clone(&fence));
                return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                    index, reason: BatchMemberUnknownReason::Preparation(reason) }, stop: Some(stop) };
            }
        };
        let reservation = Arc::clone(&member.reservation.get().expect("original reservation retained").token);
        match crate::step_snapshot::OriginalPreparedMemberCapability::from_process(member, authority, Arc::clone(&reservation), Arc::clone(&fence)) {
            Ok(candidate) => AgentBatchMemberPregrant::Candidate(candidate),
            Err(reason) => {
                let stop = OriginalBatchMemberStop::publish_with_owners(coordinate(),
                    OriginalBatchMemberStopReason::BeforeOutcomeUnknown(reason.clone()), reservation, fence);
                AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                    index, reason: BatchMemberUnknownReason::Preparation(reason) }, stop: Some(stop) }
            }
        }
    }

}


impl OriginalProcessBatchCall {
    pub(crate) fn outer_input_facts(&self) -> Option<&OriginalProcessOuterInputFacts> { self.outer_input_facts.get() }
    fn capture_outer_input_facts(&self, request: &crate::RuntimeToolExecutionRequest) -> Result<(), String> {
        let model = harness_contract::step::parse_strict_json(&self.model_input).map_err(|error| error.to_string())?;
        let effective = harness_contract::step::parse_strict_json(&request.input).map_err(|error| error.to_string())?;
        let invocation = request.compiled_invocation.as_ref().ok_or("Process original outer invocation missing")?;
        if model != effective || model != self.input || invocation.intent.normalized_input != effective
            || invocation.origin.as_ref() != Some(&self.binding.origin) {
            return Err("Process outer input changed without original hook facts".into());
        }
        self.outer_input_facts.set(OriginalProcessOuterInputFacts {
            model_input: self.model_input.clone(), effective_input: request.input.clone(), invocation: invocation.clone(),
        }).map_err(|_| "Process original outer input already captured".to_string())
    }
    pub(crate) fn ticket(&self) -> &crate::execution_core::graph::NodeExecutionTicket { &self.ticket }
    pub(crate) fn packet(&self) -> &AgentTaskPacket { &self.packet }
    pub(crate) fn step(&self) -> &crate::step_snapshot::StepCallBinding { &self.binding }
    pub(crate) fn outer_input(&self) -> &serde_json::Value { &self.input }
    pub(crate) fn policy(&self) -> &PermissionPolicy { &self.policy }
    pub(crate) fn member_policy(&self) -> &Arc<crate::conversation::SharedPolicyOwner> { &self.member_policy }
    pub(crate) fn negotiator(&self) -> &crate::AuthorizationNegotiator { &self.negotiator }
    pub(crate) fn store(&self) -> &Arc<crate::RuntimeEventStore> { &self.store }
    pub(crate) fn fence(&self) -> &crate::SessionExecutionFence { &self.fence }
    pub(crate) fn claim(&self) -> &Arc<crate::execution_core::services::OriginalSessionClaim> { &self.claim }
    pub(crate) fn session_input(&self) -> &crate::session_input::SessionInputStream { &self.session_input }
    pub(crate) fn control_requested(&self) -> &Arc<crate::conversation::OriginalTurnInputControl> { &self.control_requested }
    pub(crate) fn cancellation(&self) -> crate::CancellationToken { self.root_control.token() }
    pub(crate) fn check_original_stop(&self) -> Result<(), String> {
        self.stop.check().map_err(|error| error.to_string())
    }

    /// Resolve a member solely from the original call and pinned Step. This is
    /// preparation, not evidence of Fresh and not a lease or authorization.
    pub(crate) fn prepare_member(
        self: &Arc<Self>, original: Arc<crate::RuntimeToolExecutionRequest>, index: usize,
        pinned: tools::ToolHostLease, now_ms: i64,
    ) -> Result<Arc<OriginalProcessPreparedMember>, String> {
        self.validate_locked_child(&original, &self.ticket, self.store.as_ref())?;
        let invocation = original.compiled_invocation.as_ref().ok_or("Process original invocation missing")?;
        let parent = original.parent_execution.as_ref().ok_or("Process original parent missing")?;
        let member = crate::step_snapshot::prepare_agent_batch_member(
            &self.binding, Some(pinned.clone()), self.packet.session_id(), invocation, &original.input,
            parent, self.packet.attempt, index, now_ms,
        ).map_err(|error| error.to_string())?;
        if member.outer_key != original.idempotency_key { return Err("Process original outer key mismatch".into()); }
        Ok(Arc::new(OriginalProcessPreparedMember {
            owner: Arc::clone(self), original, prepared: member, pinned,
            original_hook: tokio::sync::OnceCell::new(),
            reservation: std::sync::OnceLock::new(), effect_fence: std::sync::OnceLock::new(), member_timeout_secs: std::sync::OnceLock::new(),
        }))
    }
    pub(crate) fn bind_member_control(
        &self,
        policy: &crate::permissions::SessionExecutionPolicyControl,
        claim: &Arc<crate::execution_core::services::OriginalSessionClaim>,
        requested: &Arc<crate::conversation::OriginalTurnInputControl>,
        store: &Arc<crate::RuntimeEventStore>,
        control: &Arc<crate::conversation::MemberCommitControl>,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
    ) -> Result<(), String> {
        if !self.policy.execution_policy_control().same_owner(policy)
            || !Arc::ptr_eq(&self.claim, claim) || !Arc::ptr_eq(&self.control_requested, requested)
            || !Arc::ptr_eq(&self.store, store) {
            return Err("Process member owner sources do not match".into());
        }
        let consumed = self.negotiator.original_consumption(
            &authorization.authorization_lease, &authorization.request_id,
        ).ok_or("Process member original consumption missing")?;
        if !consumed.belongs_to(&self.negotiator) || consumed.lease != authorization.authorization_lease
            || consumed.transition.idempotency_key.as_deref() != Some(authorization.request_id.as_str())
            || consumed.transition.kind != harness_contract::policy::AuthorizationLeaseTransitionKind::Consumed {
            return Err("Process member original consumption changed".into());
        }
        self.root_control.token().register_member_commit_control(control);
        self.stop.register_commit_control(control);
        Ok(())
    }
    pub(crate) fn validate_locked_child(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        parent_ticket: &crate::execution_core::graph::NodeExecutionTicket,
        store: &crate::RuntimeEventStore,
    ) -> Result<(), String> {
        let parent = request.parent_execution.as_ref().ok_or("Process original parent missing")?;
        let input: serde_json::Value = serde_json::from_str(&request.input).map_err(|e| e.to_string())?;
        if !std::ptr::eq(self.store.as_ref(), store) || &self.ticket != parent_ticket
            || request.tool_name != "agent_action_batch" || request.step_binding.as_ref() != Some(&self.binding)
            || request.tool_use_id != self.binding.origin.call_id || input != self.input
            || parent.execution_id != self.packet.graph_id() || parent.node_id != self.packet.node_id()
            || request.parent_execution_attempt != Some(self.packet.attempt)
            || request.session_id.as_deref() != Some(self.claim.session_id())
            || self.claim.turn_id() != self.binding.snapshot.identity.turn_id.as_str()
        { return Err("Process original batch call changed across child graph".into()); }
        Ok(())
    }
    pub(crate) fn validate_child(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        parent_ticket: &crate::execution_core::graph::NodeExecutionTicket,
        store: &Arc<crate::RuntimeEventStore>,
    ) -> Result<(), String> {
        self.validate_locked_child(request, parent_ticket, store.as_ref())
    }
}



// One registry for the original authenticated run. The key is request_id,
// never fingerprint: changed input/Step cannot acquire a second slot.
struct ProcessRequestScope {
    accepting: bool,
    #[cfg(test)]
    stop_after_child_registration: bool,
    calls: BTreeMap<String, Arc<ProcessCallSlot>>,
}
impl Default for ProcessRequestScope {
    fn default() -> Self {
        Self {
            accepting: true,
            #[cfg(test)]
            stop_after_child_registration: false,
            calls: BTreeMap::new(),
        }
    }
}
impl ProcessRequestScope {
    fn admit(
        &mut self,
        request_id: &str,
        fingerprint: String,
    ) -> Result<(Arc<ProcessCallSlot>, bool), String> {
        if request_id.is_empty() {
            return Err("Process request_id missing".into());
        }
        if let Some(slot) = self.calls.get(request_id) {
            if slot.fingerprint != fingerprint {
                return Err(
                    "Process request_id changed original Step/tool/input/response kind".into(),
                );
            }
            return Ok((slot.clone(), false));
        }
        if !self.accepting {
            return Err("Process tool admission closed".into());
        }
        let stop = crate::bound_tool_batch::BoundToolStop::default();
        #[cfg(test)]
        let stop = {
            let mut fixture_stop = stop;
            fixture_stop.stop_after_child_registration = self.stop_after_child_registration;
            fixture_stop
        };
        let slot = Arc::new(ProcessCallSlot {
            fingerprint,
            stop,
            result: Mutex::new(None),
            joined: Mutex::new(None),
            retained_members: Mutex::new(Vec::new()),
            member_custody: Arc::new(ProcessMemberCustody::new()),
            driver_joined: std::sync::atomic::AtomicBool::new(false),
            returned: std::sync::atomic::AtomicBool::new(false),
            settled: std::sync::atomic::AtomicBool::new(false),
            completed: Default::default(),
        });
        self.calls.insert(request_id.into(), slot.clone());
        Ok((slot, true))
    }
}
struct ProcessCallSlot {
    fingerprint: String,
    stop: crate::bound_tool_batch::BoundToolStop,
    result: Mutex<Option<Result<String, String>>>,
    joined: Mutex<Option<tokio::task::JoinHandle<()>>>,
    retained_members: Mutex<Vec<Arc<OriginalProcessPreparedMember>>>,
    member_custody: Arc<ProcessMemberCustody>,
    driver_joined: std::sync::atomic::AtomicBool,
    returned: std::sync::atomic::AtomicBool,
    settled: std::sync::atomic::AtomicBool,
    completed: tokio::sync::Notify,
}
impl ProcessCallSlot {
    async fn replay(&self) -> Result<String, String> {
        loop {
            let ready = self.completed.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            {
                return result;
            }
            ready.await;
        }
    }
}

pub(crate) fn process_tool_approval_ceiling_allows(
    policy: &PermissionPolicy,
    packet_ceiling: PermissionMode,
    required: PermissionMode,
) -> bool {
    policy.active_mode().permits(required) && packet_ceiling.permits(required)
}

/// The Process approval owner uses its actual capability-assessment risk;
/// this mapping is shared with historical authentication of the same call.
pub(crate) fn process_approval_task_risk(
    risk: harness_contract::policy::RiskLevel,
) -> harness_contract::core::TaskRisk {
    use harness_contract::{core::TaskRisk, policy::RiskLevel};
    match risk {
        RiskLevel::Low => TaskRisk::Low,
        RiskLevel::Medium => TaskRisk::Medium,
        RiskLevel::High => TaskRisk::High,
        RiskLevel::Critical => TaskRisk::Critical,
    }
}

pub(crate) fn process_tool_approval_context(
    packet: &AgentTaskPacket,
    policy: &harness_contract::policy::SessionExecutionPolicy,
    effective: &crate::EffectiveToolAuthorizationDescriptor,
    workspace_key: &str,
    invocation_id: &str,
) -> harness_contract::policy::ApprovalContext {
    let source = harness_contract::policy::ApprovalSource {
        kind: harness_contract::policy::ApprovalSourceKind::Agent,
        session_id: Some(packet.session_id().into()),
        agent_id: Some(packet.agent_id().into()),
        team_id: packet.team_id().map(str::to_string),
        mission_id: None, resource_ref: None, review_ref: None, application: None,
    };
    let mut context = harness_contract::policy::ApprovalContext::owned(
        &source, &effective.descriptor.tool_id, workspace_key).with_execution_policy(policy);
    context.principal_id = format!("agent:{}", packet.agent_id());
    context.invocation_id = Some(invocation_id.into());
    context.execution_id = Some(packet.graph_id().into());
    context.task_id = Some(packet.task_id().into());
    context.effect = Some(effective.descriptor.clone());
    context.resource_targets = effective.descriptor.scopes.iter()
        .filter_map(|scope| scope.target.clone()).collect();
    context
}

impl ProcessJsonlToolSession {
    fn original_batch_call(
        &self,
        binding: &crate::step_snapshot::StepCallBinding,
        input: serde_json::Value,
        model_input: String,
        stop: crate::bound_tool_batch::BoundToolStop,
    ) -> Result<Arc<OriginalProcessBatchCall>, String> {
        let state = self.services.graph_state_store();
        let graph = state.load(self.topic_packet.graph_id()).map_err(|error| error.to_string())?;
        let node_binding = state.current_binding(
            &graph, self.topic_packet.node_id(), self.topic_packet.attempt,
        ).map_err(|error| error.to_string())?;
        let (ticket, spec) = state.binding_start_ticket(
            &graph, self.topic_packet.node_id(), &node_binding,
        ).map_err(|error| error.to_string())?;
        crate::execution_core::graph::validate_node_ticket_start(&self.event_store, &ticket, None)
            .map_err(|error| error.to_string())?;
        let packet: AgentTaskPacket = serde_json::from_str(&spec.payload_ref).map_err(|error| error.to_string())?;
        if spec.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask
            || serde_json::to_value(&packet).map_err(|error| error.to_string())?
                != serde_json::to_value(&self.topic_packet).map_err(|error| error.to_string())?
        { return Err("Process original AgentTask ticket changed".into()); }
        let lineage = graph.lineage.as_ref().ok_or("Process original graph lineage unavailable")?;
        lineage.validate().map_err(str::to_string)?;
        if lineage.session_id != packet.session_id() || lineage.root_task_id != packet.assignment.root_task_id {
            return Err("Process original packet differs from graph lineage".into());
        }
        let parent = graph.parent_execution.as_ref().ok_or("Process original parent execution unavailable")?;
        let (_, _, active) = self.services.resolve_active_execution_bus_with_fence(&parent.execution_id)
            .ok_or("Process original Session ingress unavailable")?;
        let active = active.ok_or("Process original Session fence unavailable")?;
        let member_policy = active.policy_owner_for_lineage(packet.session_id(), &lineage.turn_id, &self.event_store)
            .ok_or("Process original member policy unavailable")?;
        if !member_policy.original_policy_control().same_owner(&self.permission_policy.execution_policy_control()) {
            return Err("Process original Session policy changed".into());
        }
        let (fence, root_control, claim, session_input, control_requested) = active
            .for_lineage(packet.session_id(), &lineage.turn_id).ok_or("Process original Session turn mismatch")?;
        if claim.session_id() != packet.session_id() || claim.turn_id() != lineage.turn_id {
            return Err("Process original Session claim mismatch".into());
        }
        Ok(Arc::new(OriginalProcessBatchCall {
            ticket, binding: binding.clone(), input, model_input, outer_input_facts: std::sync::OnceLock::new(), packet, store: Arc::clone(&self.event_store), actions: self.topic_actions.clone(),
            policy: self.permission_policy.clone(), member_policy, negotiator: self.authorization_negotiator.clone(),
            fence, root_control, claim, session_input, control_requested, stop,
        }))
    }

    pub(crate) fn prepare(
        services: &Arc<RuntimeServices>,
        packet: &AgentTaskPacket,
        selection: &AgentModelSelection,
    ) -> Result<Self, String> {
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "ProcessJsonl tool bridge requires a Runtime-compiled Binding".to_string()
        })?;
        binding.validate().map_err(|error| error.to_string())?;
        if packet
            .allowed_tools
            .iter()
            .any(|tool| !binding.tool_contract_refs.contains(tool))
        {
            return Err("AgentTaskPacket tool allow-list exceeds its Binding contract".to_string());
        }
        let execution_graph = services
            .graph_state_store()
            .load(packet.graph_id())
            .map_err(|error| {
                format!(
                    "ProcessJsonl Agent graph `{}` is unavailable: {error}",
                    packet.graph_id()
                )
            })?;
        let lineage = execution_graph.lineage.as_ref().ok_or_else(|| {
            format!(
                "ProcessJsonl Agent graph `{}` has no canonical Session/Turn/Task lineage",
                packet.graph_id()
            )
        })?;
        lineage.validate().map_err(str::to_string)?;
        if lineage.session_id != packet.session_id()
            || lineage.root_task_id != packet.assignment.root_task_id
        {
            return Err(format!(
                "ProcessJsonl AgentTaskPacket lineage does not match parent graph `{}`",
                packet.graph_id()
            ));
        }
        let host = services.tool_execution_host().cloned().ok_or_else(|| {
            "RuntimeServices has no ToolHost for the ProcessJsonl bridge".to_string()
        })?;
        let packet_allowed_tools = packet
            .allowed_tools
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let bounded_resource_lease = packet.team_id().is_some()
            || packet.agentic_binding.is_some()
            || binding.evaluation.is_some();
        let requested_tool_names = packet_allowed_tools.iter().cloned().collect::<Vec<_>>();
        let discovery_lease = host.pin_step_catalog();
        let discovery_metadata = discovery_lease
            .as_ref()
            .map(tools::ToolHostLease::catalog_receipt);
        let projection = discovery_lease
            .as_ref()
            .zip(discovery_metadata.as_ref())
            .map(|(lease, metadata)| {
                crate::tool_host::DelegatedCatalogProjection::new(
                    lease,
                    metadata,
                    &packet_allowed_tools,
                )
            });
        let definitions = projection
            .as_ref()
            .map(|projection| projection.definitions().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let exposed = host.delegated_tool_definitions(&requested_tool_names);
        if definitions.iter().any(|definition| {
            !exposed.iter().any(|actual| {
                actual.name == definition.name
                    && actual.input_schema == definition.input_schema
                    && actual.description == definition.description
            })
        }) {
            return Err("agent_tool_inventory_drift: host exposure differs from catalog".into());
        }
        let allowed_tools = definitions
            .into_iter()
            .filter(|definition| {
                projection.as_ref().is_some_and(|projection| {
                    projection
                        .discover(
                            definition,
                            &packet_allowed_tools,
                            packet.permission_ceiling,
                            bounded_resource_lease.then_some(packet.resource_scopes.as_slice()),
                        )
                        .is_some()
                })
            })
            .map(|definition| definition.name)
            .collect::<BTreeSet<_>>();
        let unavailable_tools = packet_allowed_tools
            .difference(&allowed_tools)
            .filter(|tool| !matches!(tool.as_str(), "context_retrieve" | "evidence_retrieve"))
            .cloned()
            .collect::<Vec<_>>();
        if !unavailable_tools.is_empty() {
            return Err(format!(
                "agent_tool_inventory_drift: admitted Tool contracts are unavailable or no longer bounded on the active host: {}",
                unavailable_tools.join(", ")
            ));
        }
        let memory_context = memory::MemoryTurnContext::new(
            packet.session_id(),
            binding.instance.instance_id.clone(),
        )
        .with_definition_lineage_id(Some(
            binding.definition_ref.definition_id.as_str().to_string(),
        ))
        .with_project_id(Some(crate::memory_project_id_for_workspace(
            services.workspace_root(),
        )))
        .with_task_id(Some(binding.data_lease.task_id.clone()))
        .with_team_id(binding.data_lease.team_id.clone())
        .with_cognitive_read_scopes(binding.data_lease.read_scopes.clone());
        let live_control = services.session_execution_policy_control(packet.session_id());
        let session_policy = live_control
            .as_ref()
            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
            .ok_or_else(|| {
                format!(
                    "agent_session_policy_missing: session `{}` has no executable policy snapshot",
                    packet.session_id()
                )
            })?;
        if packet.policy_revision != 0 && packet.policy_revision != session_policy.revision {
            return Err(format!(
                "agent_policy_revision_stale: packet rev {} current rev {}; replan before tool execution",
                packet.policy_revision, session_policy.revision
            ));
        }
        let commit_service = crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(
            services.event_store(),
        ))
        .with_artifact_store(Arc::clone(services.artifact_store()));
        let durable_receipts = commit_service
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .map_err(|error| {
                format!(
                    "ProcessJsonl tool receipt recovery is invalid for {}:{}:{}: {error}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                )
            })?
            .into_iter()
            .map(scoped_receipt_from_durable)
            .collect::<Vec<_>>();
        let recovered_sequence = commit_service
            .delegated_observation_high_water(packet.graph_id(), packet.node_id(), packet.attempt)
            .map_err(|error| error.to_string())?;
        let provider_model_obligations = packet
            .required_acceptance
            .evidence_obligations
            .iter()
            .filter(|obligation| {
                obligation.observation_requirement
                    == harness_contract::context::EvidenceObservationRequirement::ProviderModel
            })
            .cloned()
            .collect();
        let process_permission_policy = permission_policy(
            live_control.clone(),
            packet.permission_ceiling,
            &allowed_tools,
        );
        Ok(Self {
            services: Arc::clone(services),
            executor: Arc::new(ScopedRuntimeToolExecutor {
                tool_batch: Some(crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(services, packet)),
                host,
                allowed_tools,
                session_id: packet.session_id().to_string(),
                sandbox_posture: session_policy.sandbox_posture,
                policy_revision: session_policy.revision,
                memory_context,
                reality_context: Some(binding.data_lease.clone()),
                model_lease: selection.model.clone(),
                execution_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                workspace_root: services.workspace_root().to_path_buf(),
                path_identity_resolver: Arc::clone(services.path_identity_resolver()),
                scope_locks: Arc::clone(services.scope_locks()),
                commit_service: Some(commit_service),
                resource_scopes: bounded_resource_lease.then(|| packet.resource_scopes.clone()),
                managed_invocation: packet.managed_invocation.clone(),
                next_receipt_sequence: AtomicU64::new(recovered_sequence),
                receipts: Mutex::new(durable_receipts),
                provider_model_obligations,
            }),
            artifact_store: Arc::clone(services.artifact_store()),
            external_model_profile: selection.model.clone(),
            topic_actions: services.agent_action_service(),
            topic_packet: packet.clone(),
            topic_transport: Arc::new(Mutex::new(Default::default())),
            permission_policy: process_permission_policy,
            authorization_negotiator: crate::AuthorizationNegotiator::new(),
            event_store: Arc::clone(services.event_store()),
            requests: Arc::new(Mutex::new(ProcessRequestScope::default())),
            steps: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    pub(crate) fn working_context_delta(
        &self,
        handle: &tokio::runtime::Handle,
    ) -> Result<Option<serde_json::Value>, String> {
        self.validate_current_policy()?;
        let window = handle.block_on(
            self.services
                .working_context_window(&self.executor.memory_context),
        )?;
        Ok((window["coverage"]["active_references"].as_u64() != Some(0)).then_some(window))
    }

    pub(crate) fn topic_delta(&self) -> Result<Option<serde_json::Value>, String> {
        self.topic_transport
            .lock()
            .map_err(|_| "Topic transport lock poisoned".to_string())?
            .issue(&self.topic_actions, &self.topic_packet)
    }

    pub(crate) fn acknowledge_topic_delta(&self, delivery_id: &str) -> Result<(), String> {
        self.topic_transport
            .lock()
            .map_err(|_| "Topic transport lock poisoned".to_string())?
            .acknowledge(&self.topic_actions, delivery_id)
    }

    pub(crate) fn validate_current_policy(&self) -> Result<(), String> {
        let snapshot = self.permission_policy.execution_policy_control().snapshot();
        if snapshot.revision != self.executor.policy_revision {
            return Err(
                "ProcessJsonl packet policy revision is stale; reauthorize before continuing"
                    .into(),
            );
        }
        Ok(())
    }

    pub(crate) fn begin_step(&self) -> Result<serde_json::Value, String> {
        let snapshot = self.permission_policy.execution_policy_control().snapshot();
        if snapshot.revision != self.executor.policy_revision {
            return Err(
                "ProcessJsonl packet policy revision is stale; reauthorize before continuing"
                    .into(),
            );
        }
        let ceiling = self
            .permission_policy
            .bound_to_snapshot(&snapshot)
            .active_mode();
        let lease = self
            .executor
            .prepare_step_catalog(ceiling)
            .map_err(|error| error.to_string())?
            .ok_or("Process step requires a pinned ToolHost")?;
        let discovery = self
            .executor
            .tool_discovery_receipt_for_step(&lease, ceiling);
        let bootstrap = discovery
            .descriptors
            .iter()
            .any(|descriptor| descriptor.canonical_id == "tool_search");
        let mut advertised_names = self.executor.allowed_tools.clone();
        if bootstrap {
            advertised_names.insert("tool_search".into());
        }
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(Some(&advertised_names))
            .into_iter()
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        let expected_count = self.executor.allowed_tools.len()
            + usize::from(bootstrap && !self.executor.allowed_tools.contains("tool_search"));
        if definitions.len() != expected_count {
            return Err("Process advertised tools do not match the pinned catalog".into());
        }
        let binding = self.topic_packet.agentic_binding.as_ref();
        let identity = harness_contract::step::StepIdentity {
            step_id: uuid::Uuid::new_v4().to_string(),
            session_id: self.executor.session_id.clone(),
            turn_id: self
                .topic_packet
                .assignment
                .execution_identity
                .turn_id()
                .ok_or("Process step requires an admitted turn identity")?
                .to_string(),
            execution_id: Some(self.executor.execution_id.clone()),
            actor_ref: Some(self.topic_packet.agent_id().to_string()),
            run_ref: Some(self.topic_packet.run_id().to_string()),
            attempt: Some(self.executor.attempt),
            epoch: Some(self.topic_packet.expected_graph_revision.to_string()),
            goal_ref: None,
            program_ref: binding.map(|binding| binding.program_id.clone()),
            task_ref: binding.and_then(|binding| match &binding.focus {
                harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } => {
                    Some(task_ref.clone())
                }
                _ => None,
            }),
            build_ref: option_env!("COWD_GIT_SHA")
                .unwrap_or("unverified")
                .to_string(),
        };
        let step = Arc::new(crate::step_snapshot::BoundStep::bind(
            identity, lease, &definitions,
            &serde_json::json!({"model":self.external_model_profile,"control":"external_configured"}),
            &serde_json::json!({"claim":"runtime_boundary_only","worker_run":self.topic_packet.run_id()}),
            BTreeMap::from([
                ("worker_run".to_string(), self.topic_packet.run_id().to_string()),
                ("policy".to_string(), snapshot.revision.to_string()),
                ("attempt".to_string(), self.executor.attempt.to_string()),
            ]), chrono::Utc::now().timestamp_millis().saturating_add(3_600_000),
        ).map_err(|error| error.to_string())?);
        self.event_store
            .append(crate::runtime_event_store::RuntimeEventInput {
                stream_id: format!("session:{}", self.executor.session_id),
                scope: crate::runtime_event_store::RuntimeEventScope::Session,
                kind: "process.step_bound".to_string(),
                status: Some("prepared".to_string()),
                actor: Some(self.topic_packet.agent_id().to_string()),
                refs: vec![],
                payload: serde_json::to_value(step.snapshot())
                    .map_err(|error| error.to_string())?,
            })
            .map_err(|error| error.to_string())?;
        let response = serde_json::json!({"step":step.snapshot(),"tools":definitions,"claim":"runtime_boundary_only"});
        let mut steps = self
            .steps
            .lock()
            .map_err(|_| "Process step registry poisoned")?;
        if steps.len() == 16 {
            steps.pop_front();
        }
        steps.push_back(step);
        Ok(response)
    }

    #[cfg(test)]
    pub(crate) fn latest_process_step_id_for_test(&self) -> Option<String> {
        self.steps
            .lock()
            .ok()?
            .back()
            .map(|step| step.snapshot().identity.step_id.clone())
    }

    pub(crate) async fn execute_step_tool(
        &self,
        step_id: &str,
        sequence: u64,
        request_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        self.execute_step_tool_response(
            step_id,
            sequence,
            request_id,
            tool_name,
            input,
            "tool_result",
        )
        .await
    }

    pub(crate) async fn execute_step_tool_response(
        &self,
        step_id: &str,
        sequence: u64,
        request_id: &str,
        tool_name: &str,
        input: &str,
        response_kind: &str,
    ) -> Result<String, String> {
        let value: serde_json::Value = serde_json::from_str(input).map_err(|error| error.to_string())?;
        let fingerprint = crate::agent_process_jsonl_adapter::process_call_fingerprint(
            step_id, response_kind, tool_name, &value,
        )?;
        let fingerprint = format!("{sequence}:{fingerprint}");
        // A same-call slot is the original owner of its result. Resolve it
        // before applying today's Step expiry to a new execution.
        let existing = {
            let scope = self.requests.lock().map_err(|_| "Process request scope poisoned")?;
            match scope.calls.get(request_id) {
                Some(slot) if slot.fingerprint == fingerprint => Some(Arc::clone(slot)),
                Some(_) => return Err("Process request_id changed original Step/tool/input/response kind".into()),
                None => None,
            }
        };
        if let Some(slot) = existing {
            return tokio::select! { biased;
                _=slot.stop.cancelled()=>Err("Process original tool stop requested; completion retained".into()),
                result=slot.replay()=>result,
            };
        }
        let step = self
            .steps
            .lock()
            .map_err(|_| "Process step registry poisoned")?
            .iter()
            .find(|step| step.snapshot().identity.step_id == step_id)
            .cloned()
            .ok_or("Process step is missing, foreign, expired or evicted")?;
        let binding = crate::step_snapshot::StepCallBinding::process(
            step.clone(),
            self.topic_packet.run_id(),
            sequence,
            request_id,
        );
        step.validate_call(
            &binding.origin,
            &self.executor.session_id,
            request_id,
            tool_name,
            input,
            chrono::Utc::now().timestamp_millis(),
        )
        .map_err(|error| error.to_string())?;
        // The original in-memory binding deliberately retains its pinned Step.
        // Compare that same snapshot with the current original host lease before
        // a new call can consume authorization or create a child.
        let current_step = self.executor.host.pin_step_catalog()
            .ok_or_else(|| "step_tool_host_unavailable".to_string())
            .and_then(|lease| crate::step_snapshot::BoundStep::restore(
                step.snapshot().clone(), lease,
            ).map(|_| ()).map_err(|error| error.to_string()));
        if let Err(error) = current_step {
            // Another invocation may have admitted this exact call while the
            // current lease was checked. Its original result still owns replay.
            let existing = {
                let scope = self.requests.lock().map_err(|_| "Process request scope poisoned")?;
                match scope.calls.get(request_id) {
                    Some(slot) if slot.fingerprint == fingerprint => Some(Arc::clone(slot)),
                    Some(_) => return Err("Process request_id changed original Step/tool/input/response kind".into()),
                    None => None,
                }
            };
            if let Some(slot) = existing {
                return tokio::select! { biased;
                    _=slot.stop.cancelled()=>Err("Process original tool stop requested; completion retained".into()),
                    result=slot.replay()=>result,
                };
            }
            return Err(error);
        }
        let (slot, first) = self.admit_call(request_id, fingerprint)?;
        if !first {
            return tokio::select! { biased;
                _=slot.stop.cancelled()=>Err("Process original tool stop requested; completion retained".into()),
                result=slot.replay()=>result,
            };
        }
        let session = self.clone();
        let task_slot = slot.clone();
        let binding = binding.clone();
        let process_batch = if tool_name == "agent_action_batch" {
            match self.original_batch_call(&binding, value, input.to_string(), slot.stop.clone()) {
                Ok(owner) => Some(owner),
                Err(error) => {
                    *slot.result.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Err(error.clone()));
                    slot.returned.store(true, Ordering::Release);
                    slot.settled.store(true, Ordering::Release);
                    slot.completed.notify_waiters();
                    return Err(error);
                }
            }
        } else { None };
        let request_id = request_id.to_string();
        let tool_name = tool_name.to_string();
        let input = input.to_string();
        // Tokio task locals are not inherited by spawned tasks. Retain the
        // actual owner, when this call is already inside its scope, for the
        // lifetime of the original Process call slot. Its own ticket and
        // member checks still decide whether it can serve this request.
        let batch_owner = crate::tool_host::DELEGATED_BATCH_POLICY_OWNER
            .try_with(Arc::clone)
            .ok();
        use futures::FutureExt;
        let handle = tokio::spawn(async move {
            let original_slot = Arc::downgrade(&task_slot);
            let work = crate::bound_tool_batch::with_tool_stop(
                task_slot.stop.clone(),
                session.execute_tool_bound(Some(&binding), &request_id, &tool_name, &input),
            );
            let result = std::panic::AssertUnwindSafe(async move {
                let scoped = async move {
                    match batch_owner {
                        Some(owner) => crate::tool_host::DELEGATED_BATCH_POLICY_OWNER.scope(owner, work).await,
                        None => work.await,
                    }
                };
                match process_batch {
                    Some(owner) => ORIGINAL_PROCESS_BATCH_CALL.scope(Arc::new(OriginalProcessBatchScope {
                        owner, call_slot: original_slot, original_request: std::sync::OnceLock::new(), members: Mutex::new(None), policy_owner: std::sync::OnceLock::new(),
                    }), scoped).await,
                    None => scoped.await,
                }
            })
                .catch_unwind()
                .await;
            let returned = result.is_ok();
            let result = result.unwrap_or_else(|_| {
                Err("Process original tool task panicked; physical settlement unknown".into())
            });
            let no_dispatch = !task_slot.stop.effect_entered.load(Ordering::Acquire)
                && task_slot
                    .stop
                    .child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_none();
            let settled = returned
                && (result.is_ok()
                    || no_dispatch
                    || task_slot.stop.effect_settled.load(Ordering::Acquire));
            task_slot.settled.store(settled, Ordering::Release);
            task_slot.returned.store(returned, Ordering::Release);
            *task_slot
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
            task_slot.completed.notify_waiters();
        });
        *slot
            .joined
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
        tokio::select! {
            biased;
            _=slot.stop.cancelled()=>Err("Process original tool stop requested; completion retained".into()),
            result=slot.replay()=>result,
        }
    }

    fn admit_call(
        &self,
        request_id: &str,
        fingerprint: String,
    ) -> Result<(Arc<ProcessCallSlot>, bool), String> {
        self.requests
            .lock()
            .map_err(|_| "Process request scope poisoned")?
            .admit(request_id, fingerprint)
    }

    pub(crate) fn close_admission_and_request_stop(&self) {
        let calls = {
            let mut scope = self
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scope.accepting = false;
            scope.calls.values().cloned().collect::<Vec<_>>()
        };
        for call in calls {
            call.stop.request();
        }
    }

    pub(crate) async fn wait_drained(&self, deadline: tokio::time::Instant) -> Result<(), String> {
        let calls = {
            let scope = self
                .requests
                .lock()
                .map_err(|_| "Process request scope poisoned")?;
            if scope.accepting {
                return Err("Process tool admission is still open".into());
            }
            scope.calls.values().cloned().collect::<Vec<_>>()
        };
        for call in calls {
            let _ = tokio::time::timeout_at(deadline, call.replay())
                .await
                .map_err(|_| "Process accepted tool work remains pending at deadline")?;
            if !call.settled.load(Ordering::Acquire) {
                return Err(
                    "Process original tool receipt/unknown settlement remains unproven".into(),
                );
            }
            if call.member_custody.unresolved() {
                return Err("Process original member custody remains unresolved".into());
            }
            if !call.stop.child_drained.load(Ordering::Acquire) {
                return Err("Process original child physical driver remains unproven".into());
            }
            if !call.returned.load(Ordering::Acquire) {
                return Err("Process original tool task did not return normally".into());
            }
            let handle = call
                .joined
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(mut handle) = handle {
                match tokio::time::timeout_at(deadline, &mut handle).await {
                    Ok(Ok(())) => call.driver_joined.store(true, Ordering::Release),
                    Ok(Err(_)) => return Err("Process original tool join failed".into()),
                    Err(_) => {
                        *call
                            .joined
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
                        return Err("Process original tool join remains pending".into());
                    }
                }
            }
            if !call.driver_joined.load(Ordering::Acquire) {
                return Err("Process original tool driver handle is not joined".into());
            }
            call.retained_members.lock().map_err(|_| "Process member custody unavailable")?.clear();
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) async fn execute_tool(
        &self,
        request_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        self.execute_tool_bound(None, request_id, tool_name, input)
            .await
    }

    async fn execute_tool_bound(
        &self,
        binding: Option<&crate::step_snapshot::StepCallBinding>,
        request_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        self.validate_current_policy()?;
        if tool_name == "checkpoint" {
            return Err("checkpoint_create is Runtime-internal".into());
        }

        let value = serde_json::from_str(input)
            .map_err(|e| format!("invalid ProcessJsonl tool input: {e}"))?;
        let effect = self
            .executor
            .registered_tool_effect(tool_name, &value)
            .ok_or_else(|| {
                format!("tool `{tool_name}` is outside the admitted Agent tool contracts")
            })?;
        let snapshot = self.permission_policy.execution_policy_control().snapshot();
        let policy = self.permission_policy.bound_to_snapshot(&snapshot);
        let authorization_id = format!(
            "process-tool:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    &self.executor.execution_id,
                    self.executor.attempt,
                    request_id
                ))
                .map_err(|e| e.to_string())?
            )
        );
        let authorization_request = crate::AuthorizationRequest {
            principal_id: format!("agent:{}", self.topic_packet.agent_id()),
            capability: tool_name.into(),
            input: input.into(),
            idempotency_key: authorization_id.clone(),
            effect,
            parent_ceiling: self.topic_packet.permission_ceiling,
            parent_lease_id: None,
            policy_revision: snapshot.revision,
            recovery_scope: format!("execution:{}", self.executor.execution_id),
            context: crate::PermissionContext::default(),
            safe_alternatives: vec![],
        };
        let evaluated = self
            .authorization_negotiator
            .assess_effective(&policy, &authorization_request);
        let (evaluated, approved_grant) = {
            let mut evaluated = evaluated;
            let mut approved_grant = None;
            if tool_name == "execute_code"
                && evaluated.assessment.lease.is_none()
                && evaluated.assessment.path
                    == harness_contract::policy::AuthorizationPath::HumanApproval
                && process_tool_approval_ceiling_allows(
                    &policy,
                    self.topic_packet.permission_ceiling,
                    evaluated.effective.descriptor.required_permission,
                )
            {
                let context = process_tool_approval_context(
                    &self.topic_packet,
                    &snapshot,
                    &evaluated.effective,
                    self.executor.path_identity_resolver.workspace_id(),
                    &authorization_id,
                );
                let risk = match evaluated.assessment.risk {
                    harness_contract::policy::RiskLevel::Low => {
                        harness_contract::core::TaskRisk::Low
                    }
                    harness_contract::policy::RiskLevel::Medium => {
                        harness_contract::core::TaskRisk::Medium
                    }
                    harness_contract::policy::RiskLevel::High => {
                        harness_contract::core::TaskRisk::High
                    }
                    harness_contract::policy::RiskLevel::Critical => {
                        harness_contract::core::TaskRisk::Critical
                    }
                };
                if let Some(grant) = self
                    .services
                    .approval_queue()
                    .matching_exact_once_grant(&context, risk)
                {
                    let approved = self.authorization_negotiator.approve_effective(
                        &policy,
                        &authorization_request,
                        &evaluated.effective,
                        &grant,
                    );
                    if approved.lease.is_some() {
                        evaluated.assessment = approved;
                        approved_grant = Some((grant, context, risk));
                    }
                }
            }
            (evaluated, approved_grant)
        };
        #[cfg(test)]
        if tool_name == "execute_code" {
            eprintln!(
                "Own848_process_authorization {}",
                serde_json::json!({
                    "packet_ceiling": self.topic_packet.permission_ceiling,
                    "active_mode": policy.active_mode(),
                    "registered_required": authorization_request.effect.required_permission,
                    "effective_required": evaluated.effective.descriptor.required_permission,
                    "path": evaluated.assessment.path,
                    "gap": evaluated.assessment.gap.as_ref().map(|gap| gap.kind),
                    "lease_ceiling": evaluated.assessment.lease.as_ref().map(|lease| lease.ceiling),
                    "lease_permits": evaluated.assessment.lease.as_ref().map(|lease|
                        lease.permits(&evaluated.assessment.capability, evaluated.assessment.required_mode)),
                    "capability_matches": evaluated.assessment.capability == evaluated.effective.descriptor.tool_id,
                    "effect_matches": evaluated.assessment.effect == evaluated.effective.descriptor.assessment,
                    "scopes_match": evaluated.assessment.requested_scopes == evaluated.effective.descriptor.scopes,
                    "required_matches": evaluated.assessment.required_mode == evaluated.effective.descriptor.required_permission,
                    "lease_policy_revision": evaluated.assessment.lease.as_ref().map(|lease| lease.policy_revision),
                    "lease_descriptor_matches": evaluated.assessment.lease.as_ref().map(|lease|
                        lease.effect_descriptor_hash == evaluated.effective.descriptor.descriptor_hash),
                })
            );
        }
        let persisted_in_call = std::sync::Mutex::new(Vec::<crate::DurableRuntimeEvent>::new());
        let persist_authorization = || -> Result<(), String> {
            let _ = self
                .authorization_negotiator
                .take_transitions_for_persistence();
            for transition in self
                .authorization_negotiator
                .transitions_awaiting_persistence()
            {
                let event = crate::authorization_negotiator::persist_authorization_transition_event(
                    &self.event_store,
                    &format!("authorization-lease:{}", transition.lease.lease_id),
                    "runtime.process_jsonl",
                    &transition,
                )?;
                persisted_in_call.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(event);
                self.authorization_negotiator
                    .acknowledge_persisted_transitions(std::slice::from_ref(
                        &transition.transition_id,
                    ));
            }
            Ok(())
        };
        if evaluated.assessment.lease.is_none() {
            persist_authorization()?;
        }
        let lease = evaluated.assessment.lease.clone().ok_or_else(|| {
            evaluated.assessment.gap.as_ref().map_or_else(
                || "ProcessJsonl tool authorization unavailable".to_string(),
                |gap| format!("ProcessJsonl tool authorization denied: {}", gap.reason),
            )
        })?;
        let lease_id = lease.lease_id.clone();
        let decision = crate::ToolPolicy
            .authorize(
                &evaluated.effective,
                &evaluated.assessment,
                authorization_id.clone(),
                lease,
                60,
            )
            .map_err(|error| {
                self.authorization_negotiator.revoke(&lease_id);
                error.to_string()
            });
        let decision = match decision {
            Ok(decision) => decision,
            Err(error) => {
                persist_authorization()?;
                return Err(error);
            }
        };
        if tool_name == "execute_code" {
            let current = self.permission_policy.execution_policy_control().snapshot();
            if current.revision != snapshot.revision
                || !process_tool_approval_ceiling_allows(
                    &self.permission_policy.bound_to_snapshot(&current),
                    self.topic_packet.permission_ceiling,
                    evaluated.effective.descriptor.required_permission,
                )
            {
                self.authorization_negotiator.revoke(&lease_id);
                persist_authorization()?;
                return Err("ProcessJsonl policy changed before approved execution".into());
            }
        }
        if let Some((grant, context, risk)) = approved_grant {
            if let Err(error) = self.services.approval_queue().consume_exact_once_grant(
                &grant.grant_id,
                &context,
                risk,
            ) {
                self.authorization_negotiator
                    .revoke(&decision.authorization.authorization_lease.lease_id);
                persist_authorization()?;
                return Err(format!(
                    "ProcessJsonl exact approval consumption failed: {error}"
                ));
            }
        }
        if let Err(error) = persist_authorization() {
            self.authorization_negotiator.revoke(&lease_id);
            let revoke_result = persist_authorization();
            return Err(match revoke_result {
                Ok(()) => format!("ProcessJsonl authorization persistence failed; lease revoked: {error}"),
                Err(revoke_error) => format!("ProcessJsonl authorization persistence failed: {error}; lease disabled in memory, durable revoke unconfirmed: {revoke_error}"),
            });
        }
        // Preserve the facts from this Process producer before dispatch. The
        // child ToolBatch carries a different physical ticket; its request is
        // not allowed to supply or reconstruct this authorization evidence.
        if harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&tool_name) {
            let record = || -> Result<(), String> {
                let step = binding.ok_or("Process action original Step unavailable")?;
                let authorization = &decision.authorization;
                let consumed = self.authorization_negotiator.original_consumption(
                    &authorization.authorization_lease, &authorization_id,
                ).ok_or("Process action original consume unavailable")?;
                if !consumed.belongs_to(&self.authorization_negotiator)
                    || consumed.lease != authorization.authorization_lease
                    || consumed.transition.kind != harness_contract::policy::AuthorizationLeaseTransitionKind::Consumed
                    || consumed.transition.idempotency_key.as_deref() != Some(authorization_id.as_str())
                    || authorization.request_id != authorization_id
                    || step.origin.call_id != request_id
                { return Err("Process action original producer identity mismatch".into()); }
                let stream = format!("authorization-lease:{}", consumed.lease.lease_id);
                let captured = persisted_in_call.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter()
                    .find(|event| event.idempotency_key.as_deref() == Some(consumed.transition.transition_id.as_str())).cloned();
                let event = match captured {
                    Some(event) => event,
                    None => {
                        // Another concurrent drain may have persisted this
                        // exact producer transition. Authenticate that one
                        // cross-operation re-entry before using its coordinates.
                        let event = self.event_store.event_by_idempotency_key(&stream, &consumed.transition.transition_id)
                            .map_err(|error| error.to_string())?.ok_or("Process action consume event missing")?;
                        let receipt = self.event_store.original_transaction_receipt(&event.transaction_id)
                            .map_err(|error| error.to_string())?.ok_or("Process action consume receipt missing")?;
                        if receipt.transaction_id != event.transaction_id || receipt.commit_cursor != event.commit_cursor
                            || receipt.event_ids.as_slice() != [event.event_id.clone()] {
                            return Err("Process action consume receipt mismatch".into());
                        }
                        event
                    }
                };
                if event.scope != crate::RuntimeEventScope::Tool || event.kind != "authorization.lease_transition"
                    || event.actor.as_deref() != Some("runtime.process_jsonl")
                    || event.status.as_deref() != Some("consumed") || event.schema_version != 1
                    || event.stream_id != stream || event.transaction_index != 0
                    || event.payload != serde_json::to_value(&consumed.transition).map_err(|e| e.to_string())?
                    || event.idempotency_key.as_deref() != Some(consumed.transition.transition_id.as_str())
                { return Err("Process action consume evidence mismatch".into()); }
                let mut payload = serde_json::to_value(&evaluated.assessment).map_err(|e| e.to_string())?;
                let object = payload.as_object_mut().ok_or("Process assessment is not an object")?;
                object.insert("_original_process_call".into(), serde_json::json!({
                    "version": 1,
                    "packet_identity": {"graph_id":self.topic_packet.graph_id(), "node_id":self.topic_packet.node_id(),
                        "attempt":self.topic_packet.attempt, "run_id":self.topic_packet.run_id(),
                        "agent_id":self.topic_packet.agent_id(), "session_id":self.topic_packet.session_id()},
                    "step_id":step.snapshot.identity.step_id, "step_origin":step.origin,
                    "effect_key":step.effect_idempotency_key(),
                    "request_id": request_id, "authorization_request_id": authorization_id,
                    "tool": tool_name, "input": input,
                    "effect": authorization_request.effect,
                    "effective_effect": evaluated.effective.descriptor,
                    "effective_fingerprint": evaluated.effective.fingerprint,
                    "policy_revision": snapshot.revision,
                    "authorization": authorization,
                    "consume": { "stream_id": event.stream_id, "sequence": event.sequence,
                        "event_id": event.event_id, "transaction_id": event.transaction_id,
                        "transaction_index": event.transaction_index, "commit_cursor": event.commit_cursor }
                }));
                let session_stream = format!("session:{}", self.topic_packet.session_id());
                let key = format!("process-action-assessment:{authorization_id}");
                self.event_store.with_stream_lock(&session_stream, || {
                    for attempt in 0..3 {
                        let revision = self.event_store.stream_revision(&session_stream).map_err(|e| e.to_string())?;
                        let append = self.event_store.append_transaction_locked(crate::AppendTransactionRequest {
                            transaction_id: format!("{key}:transaction"),
                            expected_streams: vec![crate::ExpectedStreamRevision {
                                stream_id: session_stream.clone(), expected_revision: revision,
                            }],
                            events: vec![crate::RuntimeTransactionEventInput {
                                event: crate::RuntimeEventInput {
                                    stream_id: session_stream.clone(), scope: crate::RuntimeEventScope::Tool,
                                    kind: "authorization.capability_assessed".into(), status: Some("authorized".into()),
                                    actor: Some("runtime.process_jsonl".into()),
                                    refs: vec![crate::RuntimeEventRef { kind: "authorization_lease".into(), id: consumed.lease.lease_id.clone() }],
                                    payload: payload.clone(),
                                },
                                idempotency_key: Some(key.clone()), schema_version: 1,
                            }],
                        });
                        match append {
                            Ok(_) => return Ok(()),
                            Err(crate::RuntimeEventStoreError::StaleRevision { .. }) if attempt < 2 => continue,
                            Err(error) => return Err(error.to_string()),
                        }
                    }
                    Err("original Process assessment append did not settle".to_string())
                })?;
                Ok(())
            };
            if let Err(error) = record() {
                self.authorization_negotiator.revoke(&lease_id);
                let _ = persist_authorization();
                return Err(format!("Process original action evidence persistence failed: {error}"));
            }
        }
        self.executor
            .execute_step_authorized_invocation_output(
                binding,
                &authorization_id,
                &decision.authorization,
                tool_name,
                input,
            )
            .await
            .map(|output| output.model_text().to_string())
            .map_err(|error| error.to_string())
    }


    fn transport_request_identity(&self, request_id: &str) -> (String, String) {
        let packet = &self.topic_packet;
        let scope = serde_json::json!([
            packet.session_id(),
            packet.graph_id(),
            packet.node_id(),
            packet.run_id(),
            packet.agent_id(),
            self.executor.attempt,
            packet
                .binding
                .as_ref()
                .map(|binding| binding.binding_digest.as_str())
        ]);
        (
            format!(
                "agent-process-transport:{:x}",
                Sha256::digest(scope.to_string().as_bytes())
            ),
            format!("{:x}", Sha256::digest(request_id.as_bytes())),
        )
    }

    /// Protocol replay metadata only. Physical effect and acceptance authority
    /// remains with the original ToolHost invocation and Session receipts.
    pub(crate) fn replay_or_begin_transport_request(
        &self,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<Option<serde_json::Value>, String> {
        self.validate_current_policy()?;
        if request_id.trim().is_empty() {
            return Err("ProcessJsonl request_id must not be empty".into());
        }
        let (stream, key) = self.transport_request_identity(request_id);
        for stage in ["completed", "started"] {
            if let Some(record) = self
                .event_store
                .event_by_idempotency_key(&stream, &format!("{stage}:{key}"))
                .map_err(|error| error.to_string())?
            {
                if record.payload["fingerprint"].as_str() != Some(fingerprint)
                    || record.payload["request_id"].as_str() != Some(request_id)
                {
                    return Err(
                        "ProcessJsonl request_id was reused for a different tool invocation".into(),
                    );
                }
                if stage == "completed" {
                    let response = record
                        .payload
                        .get("response")
                        .filter(|response| response.is_object())
                        .cloned()
                        .ok_or("ProcessJsonl durable transport response is corrupt")?;
                    return Ok(Some(response));
                }
                return Err("ProcessJsonl prior request has unresolved transport completion; reconcile its recorded effects before retrying, do not blindly repeat the tool".into());
            }
        }
        if self.record_transport_request("started", request_id, fingerprint, None)? {
            return Err("ProcessJsonl prior request has unresolved transport completion; another reader already owns this request, reconcile its recorded effects before retrying".into());
        }
        Ok(None)
    }

    pub(crate) fn complete_transport_request(
        &self,
        request_id: &str,
        fingerprint: &str,
        response: &serde_json::Value,
    ) -> Result<(), String> {
        self.record_transport_request("completed", request_id, fingerprint, Some(response))
            .map(|_| ())
    }

    pub(crate) async fn bounded_transport_response(
        &self,
        request_id: &str,
        tool_name: &str,
        mut response: serde_json::Value,
        frame_bytes: usize,
    ) -> Result<serde_json::Value, String> {
        use crate::context_evidence::raw::{
            RawEvidenceStore, RawEvidenceWrite, SessionPortRawEvidenceStore,
        };
        let encoded_bytes = serde_json::to_vec(&response)
            .map_err(|error| error.to_string())?
            .len();
        if encoded_bytes.saturating_add(256) <= frame_bytes {
            return Ok(response);
        }
        let Some(serde_json::Value::String(output)) = response
            .as_object_mut()
            .and_then(|object| object.remove("output"))
        else {
            return Err("Process response metadata exceeds the original frame budget".into());
        };
        if crate::context_evidence::read_body_seal(tool_name, &output).is_none() {
            return Err(
                "Process response exceeds its frame budget without a complete readable source"
                    .into(),
            );
        }
        let journal = self
            .services
            .session_journal_port()
            .ok_or("Process large read requires the original Session evidence journal")?;
        let output_sha256 = crate::context_evidence::read_sha256(output.as_bytes());
        let identity =
            serde_json::to_vec(&(self.topic_packet.run_id(), request_id, &output_sha256))
                .map_err(|error| error.to_string())?;
        let evidence_id = format!("process-read:{:x}", Sha256::digest(identity));
        let access = SessionPortRawEvidenceStore::new(journal, Arc::clone(&self.artifact_store))
            .persist(RawEvidenceWrite {
                evidence_ref: harness_contract::reality::EvidenceRef::observed("tool", evidence_id),
                session_id: self.topic_packet.session_id().into(),
                media_type: "application/json".into(),
                visibility_scope: format!("session:{}", self.topic_packet.session_id()),
                payload: output.clone(),
                metadata: serde_json::json!({
                    "source":"process_tool_output",
                    "request_id_sha256":crate::context_evidence::read_sha256(request_id.as_bytes()),
                    "run_id":self.topic_packet.run_id(), "tool_name":tool_name,
                }),
            })
            .await
            .map_err(|error| error.to_string())?;
        let recovery = if self
            .topic_packet
            .allowed_tools
            .iter()
            .any(|tool| tool == "evidence_retrieve")
        {
            serde_json::json!({
                "tool_name":"evidence_retrieve",
                "input":{"evidence_ref":access.retrieval_selector,"limit":8},
            })
        } else {
            serde_json::json!({"status":"unavailable_in_immutable_packet",
                "reason":"Ask the original owner for authorized bounded access to this immutable output"})
        };
        let mut preview_chars = (frame_bytes / 16).min(4_000);
        loop {
            let preview = output.chars().take(preview_chars).collect::<String>();
            let bounded = serde_json::json!({
                "kind":"process_tool_preview", "truncated":true,
                "original_output_ref":access.retrieval_selector,
                "original_output_sha256":output_sha256,
                "original_output_bytes":access.bytes,
                "content_preview":preview, "next_request":recovery,
            })
            .to_string();
            let mut candidate = response.clone();
            candidate["output"] = serde_json::Value::String(bounded);
            if serde_json::to_vec(&candidate)
                .map_err(|error| error.to_string())?
                .len()
                .saturating_add(256)
                <= frame_bytes
            {
                response = candidate;
                return Ok(response);
            }
            if preview_chars == 0 {
                return Err("Process response metadata exceeds the original frame budget".into());
            }
            preview_chars /= 2;
        }
    }

    pub(crate) fn process_read_confirmation_revision(
        &self,
        tool_name: &str,
        response: &serde_json::Value,
    ) -> Result<Option<u64>, String> {
        if !matches!(tool_name, "read_file" | "read_ref" | "evidence_retrieve")
            || response.get("error").is_some()
        {
            return Ok(None);
        }
        self.validate_current_policy()?;
        self.event_store
            .stream_revision(&format!("session:{}", self.topic_packet.session_id()))
            .map(Some)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn record_process_read_delivery(
        &self,
        tool_name: &str,
        fingerprint: &str,
        sent: &serde_json::Value,
        frame_sha256: &str,
        frame_bytes: usize,
        confirmation_revision: u64,
    ) -> Result<(), String> {
        use crate::context_evidence::{
            ReadDeliveryGrade, ReadDeliveryMaterial, ReadDeliveryTransport,
        };
        if !matches!(tool_name, "read_file" | "read_ref" | "evidence_retrieve")
            || sent.get("error").is_some()
        {
            return Ok(());
        }
        self.validate_current_policy()?;
        let packet = &self.topic_packet;
        let request_id = sent["request_id"]
            .as_str()
            .ok_or("sent Process read lacks request identity")?;
        let step_id = sent["origin_step_id"]
            .as_str()
            .ok_or("sent Process read lacks bound step")?;
        let output = sent["output"]
            .as_str()
            .ok_or("sent Process read lacks output")?;
        let sequence = sent["sequence"]
            .as_u64()
            .ok_or("sent Process frame lacks sequence")?;
        let frame = serde_json::to_vec(sent).map_err(|error| error.to_string())?;
        if sent["run_id"] != packet.run_id()
            || sent["agent_id"] != packet.agent_id()
            || sent["protocol_version"] != 3
            || frame.len() != frame_bytes
            || crate::context_evidence::read_sha256(&frame) != frame_sha256
        {
            return Err("sent Process frame differs from its authenticated worker output".into());
        }
        let (transport_stream, transport_key) = self.transport_request_identity(request_id);
        let completed = self
            .event_store
            .event_by_idempotency_key(&transport_stream, &format!("completed:{transport_key}"))
            .map_err(|error| error.to_string())?
            .ok_or("sent Process response has no original completed transport record")?;
        if completed.kind != "agent.process_request_completed"
            || completed.payload["fingerprint"] != fingerprint
            || completed.payload["response"]["output"] != sent["output"]
            || completed.payload["response"]["origin_step_id"] != step_id
        {
            return Err("sent Process response does not match original transport identity".into());
        }
        let index = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let head = self
            .event_store
            .stream_revision(&index)
            .map_err(|error| error.to_string())?;
        let mut after = 0;
        let mut acquisition = None;
        while after < head {
            let events =
                self.event_store
                    .list_stream_after(&index, after, head, 128, 2 * 1024 * 1024)?;
            if events.is_empty() {
                return Err("Process read acquisition index is incomplete".into());
            }
            for event in events {
                if event.sequence <= after || event.sequence > head {
                    return Err("Process read acquisition cursor is invalid".into());
                }
                after = event.sequence;
                if event.kind != "execution.agent_tool.receipt"
                    || event.actor.as_deref() != Some("governed_tool")
                {
                    continue;
                }
                let Some(value) = event
                    .payload
                    .get(crate::context_evidence::READ_ACQUISITION_FIELD)
                else {
                    continue;
                };
                let Ok(seal) = crate::context_evidence::decode_read_acquisition(value) else {
                    continue;
                };
                if seal.invocation_id != request_id
                    || seal.tool_name != tool_name
                    || seal.step.step_id != step_id
                    || seal.index_stream != index
                    || seal.attempt != packet.attempt
                    || seal.graph_id != packet.graph_id()
                    || seal.node_id != packet.node_id()
                    || seal.session_id != packet.session_id()
                    || seal.step.run_ref.as_deref() != Some(packet.run_id())
                    || event.idempotency_key.as_deref() != Some(seal.index_key.as_str())
                {
                    continue;
                }
                let origin: harness_contract::step::CallOrigin =
                    serde_json::from_value(seal.origin.clone())
                        .map_err(|error| error.to_string())?;
                if !matches!(origin.source, harness_contract::step::CallSource::Process { worker_id, .. }
                    if worker_id == packet.run_id())
                {
                    continue;
                }
                let primary = self
                    .event_store
                    .event_by_idempotency_key(&seal.primary_stream, &seal.primary_key)
                    .map_err(|error| error.to_string())?
                    .ok_or("Process read primary receipt is missing")?;
                if primary.kind != "execution.read.receipt"
                    || primary.actor.as_deref() != Some("governed_tool")
                    || primary.transaction_id != event.transaction_id
                    || primary
                        .payload
                        .get(crate::context_evidence::READ_ACQUISITION_FIELD)
                        != Some(value)
                    || primary.payload["outcome"]["status"] != "executed"
                {
                    return Err(
                        "Process read primary and index do not authenticate one acquisition".into(),
                    );
                }
                if acquisition
                    .replace((seal, primary, event.event_id))
                    .is_some()
                {
                    return Err("Process read has ambiguous acquisition identity".into());
                }
            }
        }
        let Some((seal, primary, index_event_id)) = acquisition else {
            return Ok(());
        };
        let body_omitted =
            crate::context_evidence::read_body_seal(tool_name, output).as_ref() != Some(&seal.body);
        let material = ReadDeliveryMaterial {
            schema_version: 1,
            grade: ReadDeliveryGrade::AgentChannelSend,
            acquisition_event_id: primary.event_id,
            acquisition_cursor: primary.commit_cursor,
            index_event_id,
            session_id: packet.session_id().into(),
            execution_id: packet.graph_id().into(),
            turn_id: packet
                .assignment
                .execution_identity
                .turn_id()
                .ok_or("Process read recipient has no turn identity")?
                .into(),
            tool_invocation_id: request_id.into(),
            recipient: crate::context_evidence::ReadDeliveryRecipient::AgentTask {
                identity: packet.assignment.execution_identity.clone(),
            },
            presentation_sha256: crate::context_evidence::read_sha256(output.as_bytes()),
            presentation_bytes: output.len() as u64,
            delivered_extent: (!body_omitted).then(|| seal.body.extent.clone()),
            body_omitted,
            acquisition: seal,
            transport: ReadDeliveryTransport::ProcessChannelSend {
                worker_id: packet.run_id().into(),
                request_id: request_id.into(),
                sequence,
                step_id: step_id.into(),
                request_fingerprint: fingerprint.into(),
                response_sha256: frame_sha256.into(),
                response_bytes: frame_bytes as u64,
            },
        };
        let payload = serde_json::to_value(material).map_err(|error| error.to_string())?;
        crate::context_evidence::decode_read_delivery(&payload).map_err(str::to_string)?;
        let stream = format!("session:{}", packet.session_id());
        let key = format!("process-read:{}:{sequence}", packet.run_id());
        if let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&stream, &key)
            .map_err(|error| error.to_string())?
        {
            return if existing.kind == crate::context_evidence::PROCESS_DELIVERY_KIND
                && existing.actor.as_deref() == Some("runtime.process_jsonl")
                && existing.status.as_deref() == Some("sent")
                && existing.schema_version == 1
                && existing.scope == crate::RuntimeEventScope::Session
                && existing.payload == payload
            {
                Ok(())
            } else {
                Err("conflicting Process read delivery material".into())
            };
        }
        self.event_store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: format!(
                    "process-read:{}",
                    crate::context_evidence::read_sha256(key.as_bytes())
                ),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: stream.clone(),
                    expected_revision: confirmation_revision,
                }],
                events: vec![crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: stream,
                        scope: crate::RuntimeEventScope::Session,
                        kind: crate::context_evidence::PROCESS_DELIVERY_KIND.into(),
                        status: Some("sent".into()),
                        actor: Some("runtime.process_jsonl".into()),
                        refs: vec![
                            crate::RuntimeEventRef {
                                kind: "read_acquisition".into(),
                                id: payload["acquisition_event_id"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .into(),
                            },
                            crate::RuntimeEventRef {
                                kind: "tool_invocation".into(),
                                id: request_id.into(),
                            },
                        ],
                        payload,
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            })
            .map_err(|error| {
                format!("Process delivery confirmation unknown after actual send: {error}")
            })?;
        Ok(())
    }

    fn record_transport_request(
        &self,
        stage: &str,
        request_id: &str,
        fingerprint: &str,
        response: Option<&serde_json::Value>,
    ) -> Result<bool, String> {
        let (stream, key) = self.transport_request_identity(request_id);
        let head = self
            .event_store
            .stream_revision(&stream)
            .map_err(|error| error.to_string())?;
        self.event_store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("{stream}:{stage}:{key}"),
            expected_streams: vec![crate::ExpectedStreamRevision {stream_id: stream.clone(), expected_revision: head}],
            events: vec![crate::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream, scope: crate::RuntimeEventScope::Agent,
                    kind: format!("agent.process_request_{stage}"), status: Some(stage.into()),
                    actor: Some(self.topic_packet.agent_id().into()),
                    refs: vec![crate::RuntimeEventRef {kind:"agent_run".into(), id:self.topic_packet.run_id().into()},
                        crate::RuntimeEventRef {kind:"session".into(), id:self.topic_packet.session_id().into()},
                        crate::RuntimeEventRef {kind:"execution_graph".into(), id:self.topic_packet.graph_id().into()}],
                    payload: serde_json::json!({"request_id":request_id,"fingerprint":fingerprint,
                        "response":response,"authority":"transport_replay_only"}),
                }, idempotency_key: Some(format!("{stage}:{key}")), schema_version: 1,
            }],
        }).map(|receipt| receipt.duplicate).map_err(|error| error.to_string())
    }

    pub(crate) fn artifact_store(&self) -> &Arc<crate::ArtifactStore> {
        &self.artifact_store
    }

    /// Replace all child-authored receipt/evidence claims with facts derived
    /// from canonical ToolHost receipts. Provider-model observation is left
    /// unsatisfied for this bridge because an arbitrary child process cannot
    /// attest that it semantically consumed a source; the evaluator records
    /// that gap instead of pretending the external model saw it.
    pub(crate) fn apply_terminal(
        &self,
        packet: &AgentTaskPacket,
        terminal: &mut AgentReturnPacket,
    ) {
        let receipts = self
            .executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let observed_evidence =
            model_observed_evidence(&packet.required_acceptance, &[], &receipts);
        let evidence_refs = agent_evidence_refs(packet, &[], &receipts);
        let (acceptance, runtime_change_receipts) = derive_receipt_backed_satisfied_criteria(
            packet,
            &terminal.outcome,
            &evidence_refs,
            &self.executor,
            &observed_evidence,
        );
        let required = crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
            &packet.required_acceptance,
            &packet.acceptance,
        );
        let (observed_acceptance, acceptance_evaluation) =
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_snapshot(
                crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                    required,
                    acceptance.clone(),
                    observed_evidence,
                ),
            );
        let mut runtime_write_attempt_paths = receipts
            .iter()
            .filter(|receipt| receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write)
            .flat_map(|receipt| receipt.paths.iter().cloned())
            .collect::<Vec<_>>();
        runtime_write_attempt_paths.sort();
        runtime_write_attempt_paths.dedup();
        terminal.observed_acceptance = observed_acceptance;
        terminal.acceptance_evaluation = Some(acceptance_evaluation);
        terminal.acceptance = acceptance;
        terminal.evidence_refs = evidence_refs;
        terminal.changes = runtime_change_receipts
            .iter()
            .map(|receipt| receipt.path.clone())
            .collect();
        terminal.runtime_change_receipts = runtime_change_receipts;
        terminal.tool_calls = u64::try_from(receipts.len()).unwrap_or(u64::MAX);
        terminal.duplicate_tool_calls = 0;
        terminal.max_tool_concurrency_observed = 1;
        terminal.parallel_tool_batches = 0;
        terminal.runtime_write_attempt_paths = runtime_write_attempt_paths;
        terminal.runtime_observed_resource_scopes = Vec::new();
        terminal.model.clone_from(&self.external_model_profile);
        terminal.provider = "external_process".to_string();
    }
}

/// Executes a delegated task through the same RuntimeServices/Runner/provider
/// path as a primary turn. It never calls `ConversationRuntime` directly.
pub struct InProcessAgentWorker {
    services: Weak<RuntimeServices>,
    active_runs: Mutex<BTreeMap<String, ActiveInProcessRun>>,
    pending_cancellations: Mutex<BTreeSet<String>>,
    completed_runs: Mutex<VecDeque<String>>,
}
#[cfg(test)]
#[path = "in_process/tests.rs"]
mod tests;

#[cfg(test)]
mod discovery_process_tests {
    use super::*;

    #[tokio::test]
    async fn r1_composite_discovery_process_prepare_and_steps_share_pinned_catalog() {
        struct NoPinHost;
        #[async_trait::async_trait]
        impl RuntimeExecutionHost for NoPinHost {
            async fn execute_runtime_tool(
                &self,
                _: &RuntimeToolExecutionRequest,
            ) -> crate::RuntimeToolExecutionOutcome {
                panic!("unregistered defaults must not execute")
            }
        }
        let root = tempfile::tempdir().unwrap();
        let (services, host, packet) = tests::discovery_test_services(root.path(), true);
        assert!(matches!(
            packet.binding.as_ref().unwrap().executor,
            harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }
        ));
        let selection = AgentModelSelection {
            model: packet.model_lease.clone(),
            provider: "local-real-toolhost".into(),
            registry_revision: 0,
        };
        let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        let no_pin_root = tempfile::tempdir().unwrap();
        let no_pin = RuntimeServices::test_builder(
            no_pin_root.path().join("home"),
            services.workspace_root(),
        )
        .tool_execution_host(Arc::new(NoPinHost))
        .build()
        .unwrap();
        no_pin
            .commit_service()
            .register_graph(
                services
                    .graph_state_store()
                    .load(packet.graph_id())
                    .unwrap(),
            )
            .unwrap();
        assert!(ProcessJsonlToolSession::prepare(&no_pin, &packet, &selection).is_err());
        let receipt = bridge.executor.tool_discovery_receipt();
        assert_eq!(
            receipt.catalog_revision,
            host.tools.pin_snapshot().revision()
        );
        assert_eq!(
            receipt
                .activation_candidates
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            packet.allowed_tools.iter().cloned().collect()
        );
        let step = bridge.begin_step().unwrap();
        let step_id = step["step"]["identity"]["step_id"].as_str().unwrap();
        assert_eq!(step["tools"].as_array().unwrap().len(), 3);
        assert_eq!(
            step["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|definition| definition["name"].as_str().unwrap())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["read_file", "tool_orchestrate", "tool_search"])
        );
        for (sequence, call_id, tool, input) in [
            (
                1,
                "process-read",
                "read_file",
                serde_json::json!({"path":"src/a.rs"}),
            ),
            (
                2,
                "process-composite",
                "tool_orchestrate",
                serde_json::json!({"steps":[
                    {"id":"read","op":"call","tool":"read_file","input":{"path":"src/a.rs"}}
                ]}),
            ),
        ] {
            let output = bridge
                .execute_step_tool(step_id, sequence, call_id, tool, &input.to_string())
                .await
                .unwrap();
            assert!(output.contains("narrow-read-proof"));
        }
        assert_eq!(host.calls.load(Ordering::SeqCst), 2);
        let receipts_before = bridge.executor.receipts.lock().unwrap().len();
        assert_eq!(receipts_before, 2);
        for (index, (tool, input)) in tests::discovery_denied_calls().into_iter().enumerate() {
            assert!(bridge
                .execute_step_tool(
                    step_id,
                    index as u64 + 3,
                    &format!("process-denied-{index}"),
                    tool,
                    &input.to_string()
                )
                .await
                .is_err());
            assert_eq!(host.calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("src/a.rs")).unwrap(),
                "narrow-read-proof"
            );
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("src/sibling.rs")).unwrap(),
                "untouched-sibling"
            );
        }
        assert_eq!(
            bridge.executor.receipts.lock().unwrap().len(),
            receipts_before
        );
        assert!(bridge
            .execute_step_tool(
                "foreign-step",
                99,
                "foreign",
                "read_file",
                r#"{"path":"src/a.rs"}"#
            )
            .await
            .is_err());
        assert!(bridge
            .execute_step_tool(step_id, 100, "checkpoint", "checkpoint", "{}")
            .await
            .is_err());
        host.tools
            .replace_snapshot(host.tools.pin_snapshot().snapshot().clone());
        let reloaded = bridge.executor.tool_discovery_receipt();
        assert_ne!(reloaded.catalog_revision, receipt.catalog_revision);
        assert_eq!(reloaded.descriptors, receipt.descriptors);
        assert_eq!(
            reloaded.activation_candidates,
            receipt.activation_candidates
        );
        assert!(bridge.begin_step().is_ok());
        for scopes in [
            vec![],
            vec!["network:*".into()],
            vec!["read:src/../a.rs".into()],
        ] {
            let mut denied = packet.clone();
            denied.resource_scopes = scopes;
            assert!(ProcessJsonlToolSession::prepare(&services, &denied, &selection).is_err());
        }
        let mut edit = packet.clone();
        edit.allowed_tools = vec!["edit".into()];
        assert!(ProcessJsonlToolSession::prepare(&services, &edit, &selection).is_err());
        let bootstrap_step = bridge.begin_step().unwrap();
        let bootstrap_id = bootstrap_step["step"]["identity"]["step_id"]
            .as_str()
            .unwrap();
        let bootstrap_input = r#"{"query":"select:read_file,edit,web_fetch"}"#;
        let bootstrap_output = bridge
            .execute_step_tool(
                bootstrap_id,
                1,
                "process-real-bootstrap",
                "tool_search",
                bootstrap_input,
            )
            .await
            .unwrap();
        let bootstrap: harness_contract::tool::ToolDiscoveryReceipt =
            serde_json::from_str(&bootstrap_output).unwrap();
        assert_eq!(bootstrap.activation_candidates, vec!["read_file"]);
        assert_eq!(
            bootstrap
                .descriptors
                .iter()
                .map(|descriptor| descriptor.canonical_id.as_str())
                .collect::<Vec<_>>(),
            vec!["read_file"]
        );
        assert!(!packet.allowed_tools.contains(&"tool_search".into()));
        assert!(!packet
            .binding
            .as_ref()
            .unwrap()
            .effective_capabilities
            .contains(&harness_contract::agent::AgentCapability::Search));
        assert_eq!(host.calls.load(Ordering::SeqCst), 3);
        let control = services
            .session_execution_policy_control(packet.session_id())
            .unwrap();
        let mut snapshot = control.snapshot();
        snapshot.revision += 1;
        control.replace(snapshot).unwrap();
        assert!(bridge.begin_step().is_err());
        assert!(bridge
            .execute_step_tool(step_id, 101, "stale", "read_file", r#"{"path":"src/a.rs"}"#)
            .await
            .is_err());
        assert_eq!(host.calls.load(Ordering::SeqCst), 3);
        assert!(services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
        assert!(no_pin
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }
}

const COMPLETED_RUN_TOMBSTONE_LIMIT: usize = 1_024;

#[derive(Clone)]
struct ActiveInProcessRun {
    cancellation: crate::CancellationToken,
    session_id: String,
    input_stream: Option<crate::SessionInputStream>,
    packet_sha256: String,
    hook_scope: crate::HookExecutionScope,
    owner_ended: bool,
}

struct ActiveRunCleanup<'a> {
    worker: &'a InProcessAgentWorker,
    run_id: String,
}

impl Drop for ActiveRunCleanup<'_> {
    fn drop(&mut self) {
        // Returning/aborting a future is not a physical exit receipt. Keep the
        // exact original scope until the Kernel's single durable exit publisher
        // acknowledges it. Pending children survive errors and unwinding.
        if let Some(active) = self
            .worker
            .active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&self.run_id)
        {
            active.owner_ended = true;
        }
    }
}

impl InProcessAgentWorker {
    #[must_use]
    pub fn new(services: Weak<RuntimeServices>) -> Self {
        Self {
            services,
            active_runs: Mutex::new(BTreeMap::new()),
            pending_cancellations: Mutex::new(BTreeSet::new()),
            completed_runs: Mutex::new(VecDeque::new()),
        }
    }

    /// Pause this exact original native attempt after all business calls and
    /// before its backend exits. Available only to controlled fixture builds.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn hold_exit_for_test(
        packet: &AgentTaskPacket,
    ) -> Result<exit_hold::BackendExitHold, String> {
        exit_hold::register(packet)
    }

    fn record_completed_run(&self, run_id: &str) {
        let mut completed = self
            .completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !completed.iter().any(|candidate| candidate == run_id) {
            completed.push_back(run_id.to_string());
        }
        while completed.len() > COMPLETED_RUN_TOMBSTONE_LIMIT {
            completed.pop_front();
        }
    }

    fn run_completed(&self, run_id: &str) -> bool {
        self.completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|candidate| candidate == run_id)
    }
}

#[cfg(test)]
mod process_request_scope_tests {
    use super::*;
    #[tokio::test]
    async fn original_registered_child_cancel_before_drive_leaves_no_work_or_orphan() {
        let root = tempfile::tempdir().unwrap();
        let (services, host, packet) = tests::discovery_test_services(root.path(), true);
        let selection = AgentModelSelection {
            model: packet.model_lease.clone(),
            provider: "local-fixture".into(),
            registry_revision: 0,
        };
        let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        bridge
            .requests
            .lock()
            .unwrap()
            .stop_after_child_registration = true;
        let parent_before = services
            .graph_state_store()
            .load(packet.graph_id())
            .unwrap();
        let step = bridge.begin_step().unwrap();
        let step_id = step["step"]["identity"]["step_id"].as_str().unwrap();
        let result = bridge
            .execute_step_tool(
                step_id,
                1,
                "registered-stop",
                "read_file",
                r#"{"path":"src/a.rs"}"#,
            )
            .await;
        assert!(
            result.is_err(),
            "registered original call must be stopped before dispatch"
        );
        bridge.close_admission_and_request_stop();
        let call = bridge.requests.lock().unwrap().calls["registered-stop"].clone();
        let child_id = call
            .stop
            .child
            .lock()
            .unwrap()
            .as_ref()
            .expect("fault must hit after durable registration")
            .0
            .clone();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), call.replay())
                .await
                .unwrap()
                .is_err(),
            "await original cancellation completion, not only its request"
        );
        let child = services
            .graph_state_store()
            .load_async(&child_id)
            .await
            .unwrap();
        assert_eq!(child.nodes.len(), 1);
        assert_eq!(
            child.node_statuses[&child.nodes[0].id],
            harness_contract::execution_graph::ExecutionNodeStatus::Cancelled
        );
        assert!(
            child.recovery_cursor.node_attempts.is_empty(),
            "fault must be before original child drive"
        );
        assert_eq!(host.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("src/a.rs")).unwrap(),
            "narrow-read-proof"
        );
        assert_eq!(
            services
                .graph_state_store()
                .load(packet.graph_id())
                .unwrap(),
            parent_before,
            "exact child cancellation must leave parent unchanged"
        );
        bridge
            .wait_drained(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .expect("never-driven original child must not strand the Process tool scope");
        assert!(call.driver_joined.load(Ordering::Acquire));
        let revision = child.revision;
        bridge
            .wait_drained(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            services
                .graph_state_store()
                .load(&child_id)
                .unwrap()
                .revision,
            revision
        );
        assert_eq!(host.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn original_admitted_child_keeps_attempt_receipt_drain_when_stop_follows_start() {
        let root = tempfile::tempdir().unwrap();
        let (services, host, packet) = tests::discovery_test_services(root.path(), true);
        let runner = crate::execution_core::graph::ExecutionGraphRunner::new(
            services.executor_registry().clone(),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
            services.resource_manager().clone(),
            services.scope_locks().clone(),
            services.worktree_leases().clone(),
            services.workspace_key(),
            services.workspace_root(),
        );
        let snapshot = runner.pump_snapshot(packet.graph_id(), &std::collections::BTreeSet::new()).await.unwrap();
        assert_eq!(snapshot.ready.len(), 1);
        let wave = runner.prepare_ready_wave(packet.graph_id(), snapshot.ready).await.unwrap();
        assert_eq!(wave.len(), 1);
        let binding = wave[0].binding.as_ref().expect("original scheduler binding");
        assert_eq!(binding.attempt, packet.attempt);
        let running = services.graph_state_store().load(packet.graph_id()).unwrap();
        let (ticket, _) = services.graph_state_store()
            .binding_start_ticket(&running, packet.node_id(), binding).unwrap();
        crate::execution_core::graph::validate_node_ticket_start(services.event_store(), &ticket, None).unwrap();
        let selection = AgentModelSelection {
            model: packet.model_lease.clone(),
            provider: "local-fixture".into(),
            registry_revision: 0,
        };
        let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        let step = bridge.begin_step().unwrap();
        let result = bridge
            .execute_step_tool(
                step["step"]["identity"]["step_id"].as_str().unwrap(),
                1,
                "start-wins",
                "read_file",
                r#"{"path":"src/a.rs"}"#,
            )
            .await
            .unwrap();
        assert!(result.contains("narrow-read-proof"));
        bridge.close_admission_and_request_stop();
        let call = bridge.requests.lock().unwrap().calls["start-wins"].clone();
        let child_id = call.stop.child.lock().unwrap().as_ref().unwrap().0.clone();
        let child = services
            .graph_state_store()
            .load_async(&child_id)
            .await
            .unwrap();
        assert_eq!(child.recovery_cursor.node_attempts[&child.nodes[0].id], 1);
        assert_eq!(
            child.node_statuses[&child.nodes[0].id],
            harness_contract::execution_graph::ExecutionNodeStatus::Completed
        );
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert!(
            call.stop.effect_settled.load(Ordering::Acquire),
            "original attempted receipt path must settle"
        );
        assert!(
            services
                .execution_supervisor()
                .confirm_cancelled_bound_child_never_admitted(&child)
                .await
                .is_err(),
            "attempted history cannot borrow never-start fact"
        );
        bridge
            .wait_drained(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert!(call.driver_joined.load(Ordering::Acquire));
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn original_call_drain_requires_registered_join_not_result_flags() {
        let root = tempfile::tempdir().unwrap();
        let (services, _host, packet) = tests::discovery_test_services(root.path(), true);
        let selection = AgentModelSelection {
            model: packet.model_lease.clone(),
            provider: "local-fixture".into(),
            registry_revision: 0,
        };
        let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        let (call, fresh) = bridge
            .admit_call("original-retained-call", "fixture-immutable-call".into())
            .unwrap();
        assert!(fresh);
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let driver = tokio::spawn(async move {
            entered.send(()).unwrap();
            let _ = released.await;
        });
        observed.await.unwrap();
        // A business result may be visible before the physical task returns.
        // Keep its real handle locally to hit the registration gap deterministically.
        *call.result.lock().unwrap() = Some(Ok("business result".into()));
        call.returned.store(true, Ordering::Release);
        call.settled.store(true, Ordering::Release);
        bridge.close_admission_and_request_stop();
        let without_handle = bridge
            .wait_drained(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await;
        assert!(
            without_handle.is_err(),
            "result flags without an actual joined handle cannot prove physical drain"
        );
        *call.joined.lock().unwrap() = Some(driver);
        let pending = bridge
            .wait_drained(tokio::time::Instant::now() + std::time::Duration::from_millis(10))
            .await;
        assert!(
            pending.is_err(),
            "running original driver must remain pending"
        );
        assert!(
            call.joined.lock().unwrap().is_some(),
            "timeout retains the real driver"
        );
        release.send(()).unwrap();
        bridge
            .wait_drained(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert!(call.joined.lock().unwrap().is_none());
        bridge
            .wait_drained(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn original_request_key_replays_and_closed_admission_rejects_new_work() {
        let mut scope = ProcessRequestScope::default();
        let (first, is_new) = scope
            .admit("call-7", "original-Step-tool-input-kind".into())
            .unwrap();
        assert!(is_new);
        for fingerprint in [
            "changed-Step",
            "changed-tool",
            "changed-input",
            "changed-response-kind",
        ] {
            assert!(scope
                .admit("call-7", fingerprint.into())
                .err()
                .unwrap()
                .contains("changed"));
        }
        assert_eq!(scope.calls.len(), 1);
        let (same, is_new) = scope
            .admit("call-7", "original-Step-tool-input-kind".into())
            .unwrap();
        assert!(!is_new);
        assert!(Arc::ptr_eq(&same, &first));
        scope.accepting = false;
        first.stop.request();
        assert!(scope
            .admit("fresh-after-stop", "original-Step-tool-input-kind".into())
            .err()
            .unwrap()
            .contains("closed"));
        assert!(scope
            .admit("call-7", "original-Step-tool-input-kind".into())
            .is_ok());
        assert!(first.stop.check().is_err());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), first.replay())
                .await
                .is_err(),
            "stop is not a completion"
        );
        *first.result.lock().unwrap() = Some(Ok("late real result".into()));
        first.completed.notify_waiters();
        assert_eq!(same.replay().await.unwrap(), "late real result");
        assert!(!scope.accepting, "late completion cannot reopen admission");
    }
}
