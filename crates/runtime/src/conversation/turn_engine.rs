//! Conversation turn construction, policy, session, and compaction control.

use super::*;

/// Evidence collected only while the original single-call policy producer runs.
/// This collector is separate from the batch-member admission machinery.
#[derive(Default)]
pub(crate) struct OriginalSingleProducerCapture {
    events: std::sync::Mutex<Vec<crate::DurableRuntimeEvent>>,
    owner: std::sync::Mutex<Option<Arc<SharedPolicyOwner>>>,
    permission_context: std::sync::Mutex<Option<PermissionContext>>,
    original_decision: std::sync::Mutex<Option<crate::execution_core::RuntimeExecutionDecision>>,
    decision_conflict: std::sync::atomic::AtomicBool,
    read_ref_target: std::sync::Mutex<Option<OriginalReadRefTarget>>,
}

/// Retirement controls remain available after a turn is cancelled: they are
/// how the owning Agent stops the work whose cancellation just happened.
/// Every other tool effect stays refused.
fn retirement_control_tool(tool_id: &str) -> bool {
    matches!(
        tool_id,
        "task_withdraw" | "task_release" | "task_attempt_fail"
    )
}

#[cfg(test)]
mod retirement_control_gate_tests {
    use super::retirement_control_tool;

    #[test]
    fn only_retirement_controls_survive_a_cancelled_turn() {
        for tool in ["task_withdraw", "task_release", "task_attempt_fail"] {
            assert!(retirement_control_tool(tool), "{tool}");
        }
        for tool in ["edit", "write", "agent_action_batch", "task_publish", "task_claim"] {
            assert!(!retirement_control_tool(tool), "{tool}");
        }
    }
}

/// The permission context of one original call, in the shape the Runtime
/// writes into the original assessment event. The read_ref target decision and
/// the check of its assessment event must use the same context: demanding a
/// default one while deciding with the real one would refuse every session
/// that carries an operator override.
pub(crate) fn original_permission_context_json(context: &PermissionContext) -> serde_json::Value {
    serde_json::json!({
        "override_decision": context.override_decision().map(|value| match value {
            crate::permissions::PermissionOverride::Allow => "allow",
            crate::permissions::PermissionOverride::Deny => "deny",
            crate::permissions::PermissionOverride::Ask => "ask",
        }),
        "override_reason": context.override_reason(),
    })
}

/// Target granted by the same policy producer for one original read_ref call.
/// This value is private and is not itself evidence that an action committed.
#[derive(Debug, Clone)]
pub(crate) struct OriginalReadRefTarget {
    pub(crate) ticket: crate::execution_core::NodeExecutionTicket,
    pub(crate) original_channel: std::sync::Weak<crate::tool_host::OriginalStopChannel>,
    pub(crate) original_request: serde_json::Value,
    pub(crate) input: serde_json::Value,
    pub(crate) effect: harness_contract::tool::ToolEffectDescriptor,
    pub(crate) authorization: harness_contract::tool::ToolExecutionAuthorization,
}
tokio::task_local! {
    static SINGLE_ORIGINAL_EVIDENCE: Arc<OriginalSingleProducerCapture>;
}
impl OriginalSingleProducerCapture {
    pub(crate) fn read_ref_target(&self) -> Result<Option<OriginalReadRefTarget>, String> {
        self.read_ref_target.lock().map(|value| value.clone())
            .map_err(|_| "original read_ref target owner unavailable".to_string())
    }
    pub(crate) fn bind_original_decision(&self, decision: &crate::execution_core::RuntimeExecutionDecision) {
        let mut original = self.original_decision.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match original.as_ref() {
            None => *original = Some(decision.clone()),
            Some(existing) if existing == decision => {},
            Some(_) => self.decision_conflict.store(true, std::sync::atomic::Ordering::Release),
        }
    }
    pub(crate) fn with_original_decision<T>(&self, use_decision: impl FnOnce(Option<&crate::execution_core::RuntimeExecutionDecision>) -> Result<T, String>) -> Result<T, String> {
        let original = self.original_decision.lock().map_err(|_| "original decision owner unavailable")?;
        if self.decision_conflict.load(std::sync::atomic::Ordering::Acquire) {
            return Err("original decision owner conflict".into());
        }
        use_decision(original.as_ref())
    }
    pub(crate) fn original_decision(&self) -> Option<crate::execution_core::RuntimeExecutionDecision> {
        let original = self.original_decision.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.decision_conflict.load(std::sync::atomic::Ordering::Acquire) { return None; }
        original.clone()
    }
    pub(crate) fn permission_context(&self) -> Option<PermissionContext> {
        self.permission_context.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
    pub(crate) fn permission_context_json(&self) -> Option<serde_json::Value> {
        self.permission_context().as_ref().map(original_permission_context_json)
    }
    pub(crate) fn owner(&self) -> Option<Arc<SharedPolicyOwner>> {
        self.owner.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
    pub(crate) fn events(&self) -> Vec<crate::DurableRuntimeEvent> {
        self.events.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
    pub(crate) async fn scope<F: std::future::Future>(self: Arc<Self>, work: F) -> F::Output {
        SINGLE_ORIGINAL_EVIDENCE.scope(self, work).await
    }
}

/// Inputs for the future single-call checked append, borrowed only while the
/// original policy and ordered stream locks are held. No public constructor:
/// the producer and its commit owner must authenticate every field first.
pub(crate) struct RootSingleCheckedInputs<'a> {
    pub(crate) held: &'a crate::runtime_event_store::HeldStreamLocks<'a>,
    pub(crate) sources: &'a [crate::ExpectedStreamRevision],
    pub(crate) control: Arc<crate::conversation::MemberCommitControl>,
    pub(crate) claim: &'a crate::execution_core::services::OriginalSessionClaim,
    pub(crate) consumed_input_sequence: usize,
    pub(crate) ticket: &'a crate::execution_core::graph::NodeExecutionTicket,
    pub(crate) request: &'a crate::RuntimeToolExecutionRequest,
    pub(crate) assessment_event: &'a crate::DurableRuntimeEvent,
    pub(crate) lease_event: &'a crate::DurableRuntimeEvent,
    pub(crate) original_events: &'a [crate::DurableRuntimeEvent],
    pub(crate) original_decision: &'a crate::execution_core::RuntimeExecutionDecision,
    pub(crate) read_ref_target: Option<&'a OriginalReadRefTarget>,
    _original_owner_seal: (),
}

/// Both variants carry a private producer seal. `None` is only exposed for
/// an authenticated delegated owner, never as an authorization by itself.
pub(crate) enum OriginalSingleCheckedInputs<'a> {
    Root(RootSingleCheckedInputs<'a>),
    Delegated(crate::agentic::action_invocation::DelegatedSingleCheckedInputs<'a>),
}
impl<'a> OriginalSingleCheckedInputs<'a> {
    pub(crate) fn held(&self) -> &crate::runtime_event_store::HeldStreamLocks<'a> {
        match self { Self::Root(v) => v.held, Self::Delegated(v) => v.held }
    }
    pub(crate) fn sources(&self) -> &[crate::ExpectedStreamRevision] {
        match self { Self::Root(v) => v.sources, Self::Delegated(v) => v.sources }
    }
    pub(crate) fn control(&self) -> Arc<crate::conversation::MemberCommitControl> {
        match self { Self::Root(v) => Arc::clone(&v.control), Self::Delegated(v) => Arc::clone(&v.control) }
    }
    pub(crate) fn claim(&self) -> &crate::execution_core::services::OriginalSessionClaim {
        match self { Self::Root(v) => v.claim, Self::Delegated(v) => v.claim }
    }
    pub(crate) fn consumed_input_sequence(&self) -> usize {
        match self { Self::Root(v) => v.consumed_input_sequence, Self::Delegated(v) => v.consumed_input_sequence }
    }
    pub(crate) fn ticket(&self) -> &crate::execution_core::graph::NodeExecutionTicket {
        match self { Self::Root(v) => v.ticket, Self::Delegated(v) => v.ticket }
    }
    pub(crate) fn request(&self) -> &crate::RuntimeToolExecutionRequest {
        match self { Self::Root(v) => v.request, Self::Delegated(v) => v.request }
    }
    pub(crate) fn original_events(&self) -> &[crate::DurableRuntimeEvent] {
        match self { Self::Root(v) => v.original_events, Self::Delegated(v) => v.original_events }
    }
    pub(crate) fn original_decision(&self) -> Option<&crate::execution_core::RuntimeExecutionDecision> {
        match self { Self::Root(v) => Some(v.original_decision), Self::Delegated(v) => v.original_decision }
    }
    pub(crate) fn read_ref_target(&self) -> Option<&OriginalReadRefTarget> {
        match self { Self::Root(v) => v.read_ref_target, Self::Delegated(v) => v.read_ref_target }
    }
    pub(crate) fn is_delegated(&self) -> bool { matches!(self, Self::Delegated(_)) }
}

// Captures only events actually returned by the original assessment/lease writer.
tokio::task_local! {
    static MEMBER_ORIGINAL_EVIDENCE: Arc<std::sync::Mutex<Vec<crate::DurableRuntimeEvent>>>;
}
fn capture_member_event(event: crate::DurableRuntimeEvent) {
    let _ = SINGLE_ORIGINAL_EVIDENCE.try_with(|capture| {
        capture.events.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(event.clone());
    });
    let _ = MEMBER_ORIGINAL_EVIDENCE.try_with(|events| {
        events.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(event);
    });
}

tokio::task_local! {
    static MEMBER_ORIGINAL_CONSUMPTION: Arc<std::sync::Mutex<Option<crate::authorization_negotiator::OriginalLeaseConsumption>>>;
}
pub(crate) fn capture_member_consumption(consumption: crate::authorization_negotiator::OriginalLeaseConsumption) {
    let _ = MEMBER_ORIGINAL_CONSUMPTION.try_with(|slot| {
        let mut current = slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.is_none() { *current = Some(consumption); }
    });
}

// Exact policy-request identity, carrying the original live policy control.
// This is only the first mechanical extraction; no approval owner is inferred.
struct SharedAuthorizationIdentity {
    session_id: String,
    agent_id: String,
    execution: Option<crate::CowdExecutionContext>,
    permission_policy: PermissionPolicy,
}

/// The original conversation policy owner's identity mapping. Callers must
/// authenticate the Session and memory instance from the original call first.
pub(crate) fn original_memory_authorization_identity(session: &str, agent: &str) -> (String, Option<String>) {
    if agent == "primary" {
        (format!("session:{session}"), None)
    } else {
        (format!("agent:{agent}"), Some(format!("binding:{agent}")))
    }
}

impl SharedAuthorizationIdentity {
    fn authorization_request(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        execution_policy: &harness_contract::policy::SessionExecutionPolicy,
    ) -> crate::AuthorizationRequest {
        let execution_context = self.execution.as_ref();
        let delegated = self.agent_id != "primary";
        let (principal_id, parent_lease_id) = original_memory_authorization_identity(&self.session_id, &self.agent_id);
        let recovery_scope = execution_context.as_ref().map_or_else(
            || format!("session:{}", self.session_id.as_str()),
            |context| format!("turn:{}", context.turn_id),
        );
        let safe_alternatives = crate::authorization_negotiator::safe_alternatives_for_effect(descriptor.effect_kind);
        crate::AuthorizationRequest {
            principal_id,
            capability: descriptor.tool_id.clone(),
            input: input.to_string(),
            idempotency_key,
            effect: descriptor.clone(),
            parent_ceiling: if delegated {
                self.permission_policy.active_mode()
            } else {
                crate::PermissionMode::DangerFullAccess
            },
            parent_lease_id,
            policy_revision: execution_policy.revision,
            recovery_scope,
            context: permission_context,
            safe_alternatives,
        }
    }

}

// The same original writer is used by the turn and by member policy work.
// It retains B's call-refusal binding and the original event/transition writer.
struct SharedAssessmentOwner {
    session_id: String,
    execution: Option<crate::CowdExecutionContext>,
    cowd_bus: Option<Arc<crate::CowdEventBus>>,
    runtime_event_store: Option<Arc<RuntimeEventStore>>,
    authorization_negotiator: crate::AuthorizationNegotiator,
}
impl SharedAssessmentOwner {
    fn cowd_bus(&self) -> Option<&crate::CowdEventBus> { self.cowd_bus.as_deref() }
    fn record_capability_assessment_for_call(
        &self,
        assessment: &harness_contract::policy::CapabilityAssessment,
        binding: Option<&serde_json::Value>,
        permission_context: Option<&PermissionContext>,
    ) -> Option<BoundPolicyAssessment> {
        if let Some(cowd) = self.cowd_bus() {
            cowd.emit(crate::cowd_event::CowdEvent::CapabilityAssessed {
                assessment: assessment.clone(),
            });
        }
        let mut refs = vec![RuntimeEventRef {
            kind: "capability".to_string(),
            id: assessment.capability.clone(),
        }];
        if let Some(lease) = assessment.lease.as_ref() {
            refs.push(RuntimeEventRef {
                kind: "authorization_lease".to_string(),
                id: lease.lease_id.clone(),
            });
        }
        if let Some(context) = self.execution.clone() {
            for (kind, id) in [("execution", context.execution_id), ("session", context.session_id), ("turn", context.turn_id)] {
                if !refs.iter().any(|reference| reference.kind == kind && reference.id == id) {
                    refs.push(RuntimeEventRef { kind: kind.to_string(), id });
                }
            }
        }
        let assessment_event = self.runtime_event_store.as_ref().and_then(|store| {
            let mut payload = match serde_json::to_value(assessment) {
                Ok(payload) => payload,
                Err(error) => { tracing::warn!(%error, "capability assessment serialization failed"); return None; }
            };
            if let Some(binding) = binding {
                let Some(object) = payload.as_object_mut() else { return None; };
                object.insert("_call_refusal_binding".into(), binding.clone());
            }
            if let Some(context) = permission_context {
                let Some(object) = payload.as_object_mut() else { return None; };
                let decision = context.override_decision().map(|decision| match decision {
                    crate::permissions::PermissionOverride::Allow => "allow",
                    crate::permissions::PermissionOverride::Deny => "deny",
                    crate::permissions::PermissionOverride::Ask => "ask",
                });
                object.insert("_original_permission_context".into(), serde_json::json!({
                    "override_decision": decision, "override_reason": context.override_reason(),
                }));
            }
            match store.append(crate::RuntimeEventInput {
                stream_id: format!("session:{}", self.session_id.as_str()),
                scope: RuntimeEventScope::Tool,
                kind: "authorization.capability_assessed".into(),
                status: Some(format!("{:?}", assessment.path).to_ascii_lowercase()),
                actor: Some("conversation_runtime".into()), refs, payload,
            }) {
                Ok(event) => {
                    capture_member_event(event.clone());
                    binding.map(|_| BoundPolicyAssessment { event })
                },
                Err(error) => { tracing::warn!(%error, "capability assessment append failed"); None }
            }
        });
        let Some(store) = self.runtime_event_store.as_ref() else {
            for transition in self.authorization_negotiator.drain_transitions() {
                if let Some(cowd) = self.cowd_bus() {
                    cowd.emit(crate::cowd_event::CowdEvent::AuthorizationLeaseTransition {
                        transition,
                    });
                }
            }
            return assessment_event;
        };
        let _ = self
            .authorization_negotiator
            .take_transitions_for_persistence();
        for transition in self
            .authorization_negotiator
            .transitions_awaiting_persistence()
        {
            // Authorization leases live on their own stream so parallel Team
            // agents and early-tool grants never contend with session model
            // events on the shared `session:<id>` stream.
            let stream_id = format!("authorization-lease:{}", transition.lease.lease_id);
            match crate::authorization_negotiator::persist_authorization_transition_event(
                store,
                &stream_id,
                "conversation_runtime",
                &transition,
            ) {
                Ok(event) => capture_member_event(event),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        transition_id = transition.transition_id,
                        "authorization transition remains hot because durable append failed"
                    );
                    break;
                }
            }
            if self
                .authorization_negotiator
                .acknowledge_persisted_transitions(std::slice::from_ref(&transition.transition_id))
                == 1
            {
                if let Some(cowd) = self.cowd_bus() {
                    cowd.emit(crate::cowd_event::CowdEvent::AuthorizationLeaseTransition {
                        transition,
                    });
                }
            }
        }
        assessment_event
    }
}

// Both the direct turn and member admission must use these original owners.
pub(crate) struct SharedPolicyOwner {
    identity: SharedAuthorizationIdentity,
    assessment: SharedAssessmentOwner,
    approval_coordinator: Option<Arc<crate::ApprovalCoordinator>>,
    cancellation: crate::CancellationToken,
    session_input_stream: crate::session_input::SessionInputStream,
    control_requested: Arc<crate::conversation::OriginalTurnInputControl>,
    checkpoint_workspace_id: String,
    memory_team_id: Option<String>,
    activity: Option<harness_contract::projection::RuntimeActivityBinding>,
    session_execution_fence: Option<crate::SessionExecutionFence>,
    original_session_claim: Option<Arc<crate::execution_core::services::OriginalSessionClaim>>,
    tool_lease: Option<tools::ToolHostLease>,
    hook_runner: crate::HookRunner,
    hook_abort_signal: crate::HookAbortSignal,
    hook_progress_reporter: Arc<std::sync::Mutex<Option<Box<dyn crate::HookProgressReporter + Send>>>>,
}

/// Original member preparation owned by this policy instance. It is not an
/// admission or a Fresh proof and cannot be serialized or constructed by callers.
pub(crate) struct PreparedOriginalBatchPolicyInput {
    claim: Arc<crate::execution_core::services::OriginalSessionClaim>,
    original_turn: harness_contract::turn::TurnId,
    fence: crate::SessionExecutionFence,
    original_prepared: Box<crate::step_snapshot::PreparedAgentBatchMember>,
    prepared: Box<crate::step_snapshot::PreparedAgentBatchMember>,
    hook_effective: String,
    effective: String,
    context: PermissionContext,
}
impl PreparedOriginalBatchPolicyInput {
    pub(crate) fn claim(&self) -> &Arc<crate::execution_core::services::OriginalSessionClaim> { &self.claim }
    pub(crate) fn original_turn(&self) -> &harness_contract::turn::TurnId { &self.original_turn }
    pub(crate) fn original(&self) -> &crate::step_snapshot::PreparedAgentBatchMember { &self.original_prepared }
    pub(crate) fn prepared(&self) -> &crate::step_snapshot::PreparedAgentBatchMember { &self.prepared }
    pub(crate) fn hook_effective(&self) -> &str { &self.hook_effective }
    pub(crate) fn context(&self) -> &PermissionContext { &self.context }
}

/// Same-member assessment prepared by the original Host policy owner.
/// It is written only by the original member checked transaction.
pub(crate) struct PreparedHostMemberAssessment {
    pub(crate) source: crate::ExpectedStreamRevision,
    pub(crate) event: crate::RuntimeTransactionEventInput,
}
impl PreparedHostMemberAssessment {
    pub(crate) fn source(&self) -> &crate::ExpectedStreamRevision { &self.source }
    pub(crate) fn event(&self) -> &crate::RuntimeTransactionEventInput { &self.event }
}

/// Same original Host call and member preparation. This object is not a
/// grant and does not establish a Fresh or committed member outcome.
pub(crate) struct OriginalHostPreparedMember {
    owner: Arc<SharedPolicyOwner>,
    store: Arc<crate::RuntimeEventStore>,
    reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
    fresh_effect: Option<Arc<crate::execution_core::graph::OriginalFreshEffectOwner>>,
    ticket: crate::execution_core::graph::NodeExecutionTicket,
    original: Arc<crate::RuntimeToolExecutionRequest>,
    step: crate::step_snapshot::StepCallBinding,
    original_prepared: Box<crate::step_snapshot::PreparedAgentBatchMember>,
    prepared: Box<crate::step_snapshot::PreparedAgentBatchMember>,
    hook_effective: String,
    effective: String,
    context: PermissionContext,
    timeout_secs: u64,
    claim: Arc<crate::execution_core::services::OriginalSessionClaim>,
    fence: crate::SessionExecutionFence,
}
impl OriginalHostPreparedMember {
    pub(crate) fn owner(&self) -> &Arc<SharedPolicyOwner> { &self.owner }
    pub(crate) fn store(&self) -> &Arc<crate::RuntimeEventStore> { &self.store }
    pub(crate) fn ticket(&self) -> &crate::execution_core::graph::NodeExecutionTicket { &self.ticket }
    pub(crate) fn reservation(&self) -> &Arc<crate::runtime_event_store::OriginalMemberReservation> { &self.reservation }
    fn reservation_is_current(&self) -> bool {
        // The effect owner answers the identity question for the families
        // that install one. An absent owner is not a mismatch: the Read
        // family installs no owner, and the durable preparation fence
        // carries the identity of the original call from its own durable
        // admission intent instead.
        self.fresh_effect.as_ref().is_none_or(|owner| owner.matches_original(&self.store, &self.original))
            && u64::try_from(self.prepared.index).is_ok_and(|index| self.reservation.matches_original(
                &self.store, &self.prepared.outer_key, index, &self.ticket))
    }
    pub(crate) fn step(&self) -> &crate::step_snapshot::StepCallBinding { &self.step }
    pub(crate) fn original_request(&self) -> &crate::RuntimeToolExecutionRequest { &self.original }
    pub(crate) fn prepared(&self) -> &crate::step_snapshot::PreparedAgentBatchMember { &self.prepared }
    pub(crate) fn original_prepared(&self) -> &crate::step_snapshot::PreparedAgentBatchMember { &self.original_prepared }
    pub(crate) fn hook_effective_input(&self) -> &str { &self.hook_effective }
    /// Same private outer-hook owner that produced this request. A must append
    /// these facts with the original action/index and receipt, never infer
    /// them from a later serialized request.
    pub(crate) fn original_outer_hook_publication_facts(&self) -> Result<serde_json::Value, String> {
        let hook = self.original_outer_hook_owner()?;
        hook.publication_facts(&self.ticket, &self.original)
    }
    pub(crate) fn original_outer_hook_owner(&self) -> Result<Arc<crate::conversation::tool_plane::OriginalGovernedHook>, String> {
        let hook = self.original.tool_progress.original_governed_hook()?
            .ok_or("original outer hook owner unavailable")?;
        hook.publication_facts(&self.ticket, &self.original)?;
        Ok(hook)
    }
    pub(crate) fn claim(&self) -> &Arc<crate::execution_core::services::OriginalSessionClaim> { &self.claim }
    pub(crate) fn fence(&self) -> &crate::SessionExecutionFence { &self.fence }
    /// Called inside the original policy/source locks, with no await. The
    /// returned claim and control are the same live turn owners that produced
    /// this member, not values reconstructed from the action envelope.
    pub(crate) fn locked_commit_context(&self)
        -> Result<(Arc<crate::conversation::MemberCommitControl>, Arc<crate::execution_core::services::OriginalSessionClaim>, usize), String> {
        let owner = &self.owner;
        let turn = harness_contract::turn::TurnId::from_string(self.claim.turn_id());
        if !self.reservation_is_current() || owner.cancellation.is_cancelled() || owner.control_requested.load(std::sync::atomic::Ordering::Acquire)
            || owner.original_session_claim.as_ref().is_none_or(|claim| !Arc::ptr_eq(claim, &self.claim))
            || owner.session_input_stream.active_turn_id().as_ref() != Some(&turn)
            || chrono::Utc::now().timestamp_millis() >= self.step.snapshot.expires_at_ms {
            return Err("original Host member turn or Step changed".into());
        }
        let fence = self.claim.fence()?;
        let cursor = owner.session_input_stream.highest_consumed_cursor(&turn);
        if owner.session_input_stream.active_turn_id().as_ref() != Some(&turn) {
            return Err("original Host turn changed while reading cursor".into());
        }
        let consumed = match cursor {
            Some(cursor) if cursor.generation == fence.session_generation =>
                usize::try_from(cursor.sequence).map_err(|_| "original consumed cursor overflow")?.max(fence.input_sequence),
            Some(_) => return Err("original consumed cursor generation mismatch".into()),
            None => fence.input_sequence,
        };
        let control = owner.new_single_commit_control();
        Ok((control, Arc::clone(&self.claim), consumed))
    }
    pub(crate) fn permission_context(&self) -> &PermissionContext { &self.context }
    pub(crate) fn timeout_secs(&self) -> u64 { self.timeout_secs }
    pub(crate) fn effective_input(&self) -> &str { &self.effective }
    pub(crate) fn authorization_request(&self) -> Result<crate::AuthorizationRequest, String> {
        if !self.reservation_is_current() || self.owner.cancellation.is_cancelled()
            || self.owner.control_requested.load(std::sync::atomic::Ordering::Acquire) {
            return Err("original Host member reservation unavailable or cancelled".into());
        }
        let snapshot = self.owner.identity.permission_policy.execution_policy_control().snapshot();
        if snapshot.revision != self.original.policy_revision || snapshot.revision == 0 {
            return Err("original Host member policy revision changed".into());
        }
        Ok(self.owner.identity.authorization_request(&self.prepared.effect, &self.effective,
            self.prepared.identity.clone(), self.context.clone(), &snapshot))
    }
    pub(crate) fn prepare_member_assessment(
        &self, assessment: &harness_contract::policy::CapabilityAssessment,
        approval: Option<&harness_contract::policy::ApprovalGrant>,
    ) -> Result<PreparedHostMemberAssessment, String> {
        if assessment.capability != self.prepared.tool || assessment.lease.is_none()
            || assessment.lease.as_ref().is_some_and(|lease| lease.idempotency_key != self.prepared.identity) {
            return Err("Host member assessment does not match original preparation".into());
        }
        let mut refs = vec![RuntimeEventRef { kind: "capability".into(), id: assessment.capability.clone() }];
        if let Some(lease) = &assessment.lease { refs.push(RuntimeEventRef { kind: "authorization_lease".into(), id: lease.lease_id.clone() }); }
        if let Some(execution) = self.owner.assessment.execution.clone() {
            for (kind, id) in [("execution", execution.execution_id), ("session", execution.session_id), ("turn", execution.turn_id)] {
                if !refs.iter().any(|reference| reference.kind == kind && reference.id == id) { refs.push(RuntimeEventRef { kind: kind.into(), id }); }
            }
        }
        let mut payload = serde_json::to_value(assessment).map_err(|e| e.to_string())?;
        let object = payload.as_object_mut().ok_or("member assessment is not an object")?;
        let decision = self.context.override_decision().map(|value| match value {
            crate::permissions::PermissionOverride::Allow => "allow",
            crate::permissions::PermissionOverride::Deny => "deny",
            crate::permissions::PermissionOverride::Ask => "ask",
        });
        object.insert("_original_permission_context".into(), serde_json::json!({
            "override_decision": decision, "override_reason": self.context.override_reason(),
        }));
        if let Some(grant) = approval { object.insert("_original_approval_grant".into(), serde_json::to_value(grant).map_err(|e| e.to_string())?); }
        object.insert("_original_batch_member_binding".into(), serde_json::json!({
            "version": 2, "step": self.step, "ticket": self.ticket,
            "outer_invocation": self.original.compiled_invocation, "outer_input": self.original.input,
            "parent": self.original.parent_execution, "attempt": self.original.parent_execution_attempt,
            "index": self.prepared.index, "outer_key": self.prepared.outer_key,
            "member_tool": self.prepared.tool, "member_input": self.prepared.input,
            "member_effect": self.prepared.effect, "member_identity": self.prepared.identity,
            "original_prepared": self.original_prepared, "hook_effective": self.hook_effective,
            "normalized_effective": self.effective,
        }));
        let stream_id = format!("session:{}", self.claim.session_id());
        let expected_revision = self.store.stream_revision(&stream_id).map_err(|e| e.to_string())?;
        Ok(PreparedHostMemberAssessment {
            source: crate::ExpectedStreamRevision { stream_id: stream_id.clone(), expected_revision },
            event: crate::RuntimeTransactionEventInput { event: RuntimeEventInput {
                stream_id, scope: RuntimeEventScope::Tool, kind: "authorization.capability_assessed".into(),
                status: Some(format!("{:?}", assessment.path).to_ascii_lowercase()),
                actor: Some("conversation_runtime".into()), refs, payload,
            }, idempotency_key: Some(format!("member-assessment:{}", self.prepared.identity)), schema_version: 1 },
        })
    }

    pub(crate) fn original_policy(&self) -> &PermissionPolicy { &self.owner.identity.permission_policy }
    pub(crate) fn original_negotiator(&self) -> &crate::AuthorizationNegotiator { &self.owner.assessment.authorization_negotiator }
    /// Run the original negotiator's no-consumption preparation after any
    /// original approval wait; the checked action owner still decides Fresh.
    pub(crate) fn prepare_authority(
        self: &Arc<Self>, approval: Option<crate::approval_queue::PreparedMemberApproval>,
    ) -> Result<Arc<crate::authorization_negotiator::PreparedMemberAuthority>, String> {
        self.original_negotiator().prepare_host_member_authority(Arc::clone(self), approval)
    }

    /// Await the original approval owner only when the original policy route
    /// requires it. No lease or once grant is consumed across this wait.
    pub(crate) async fn prepare_authority_with_approval(
        self: &Arc<Self>,
    ) -> Result<Arc<crate::authorization_negotiator::PreparedMemberAuthority>, String> {
        let timeout_secs = self.timeout_secs;
        match self.prepare_authority(None) {
            Ok(candidate) => return Ok(candidate),
            Err(error) if !self.original_negotiator().host_member_requires_human(self)? => return Err(error),
            Err(_) => {}
        }
        let owner = &self.owner;
        let coordinator = owner.approval_coordinator.as_ref().ok_or("original approval coordinator unavailable")?;
        let snapshot = owner.identity.permission_policy.execution_policy_control().snapshot();
        if snapshot.revision != self.original.policy_revision { return Err("Host policy changed before approval".into()); }
        let request = self.authorization_request()?;
        let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(&request.effect, &request.input);
        let execution = owner.identity.execution.as_ref();
        let source = harness_contract::policy::ApprovalSource {
            kind: if owner.identity.agent_id != "primary" { harness_contract::policy::ApprovalSourceKind::Agent } else { harness_contract::policy::ApprovalSourceKind::Session },
            session_id: Some(owner.identity.session_id.to_string()),
            agent_id: (owner.identity.agent_id != "primary").then(|| owner.identity.agent_id.clone()),
            team_id: owner.memory_team_id.clone(), mission_id: None,
            resource_ref: Some(owner.checkpoint_workspace_id.clone()), review_ref: None, application: None,
        };
        let context = harness_contract::policy::ApprovalContext {
            principal_id: request.principal_id.clone(), profile_id: snapshot.autonomy_profile.as_str().to_string(),
            approval_profile: Some(snapshot.approval_profile), workspace_key: owner.checkpoint_workspace_id.clone(),
            session_id: Some(owner.identity.session_id.to_string()),
            turn_id: execution.map(|value| value.turn_id.clone()),
            task_id: owner.activity.as_ref().map(|value| value.task_id.clone()),
            capability: request.capability.clone(), invocation_id: Some(request.idempotency_key.clone()),
            execution_id: execution.map(|value| value.execution_id.clone()), strategy_decision_ref: None,
            source_surface: Some("gateway_session".into()),
            resource_targets: effective.descriptor.scopes.iter().filter_map(|scope| scope.target.clone()).collect(),
            effect: Some(effective.descriptor.clone()),
            explicit_ask: request.context.override_decision() == Some(crate::permissions::PermissionOverride::Ask),
            policy_revision: snapshot.revision, requested_sandbox_posture: Some(snapshot.sandbox_posture),
            effective_sandbox_posture: Some(snapshot.sandbox_posture),
        };
        let pending = owner.assessment.cowd_bus.clone().map(|cowd| {
            let tool = request.capability.clone();
            Arc::new(move |approval: &harness_contract::policy::ApprovalRequest| {
                cowd.emit(crate::cowd_event::CowdEvent::ExecutionPhase {
                    status: harness_contract::projection::ExecutionLiveStatus::WaitingApproval, detail: Some(tool.clone()),
                });
                cowd.emit(crate::cowd_event::CowdEvent::ApprovalRequested { request_id: approval.approval_id.clone(), tool: tool.clone() });
            }) as crate::ApprovalPendingHook
        });
        // resolve_tool normalizes profile before grant matching. Preserve that
        // same current-call context for a reusable standing grant as well.
        let mut matched_context = context.clone();
        matched_context.profile_id = match snapshot.approval_profile {
            harness_contract::policy::ApprovalProfile::Supervised => "supervised",
            harness_contract::policy::ApprovalProfile::Balanced => "balanced",
            harness_contract::policy::ApprovalProfile::Autonomous => "autonomous",
            harness_contract::policy::ApprovalProfile::TrustAll => "trust-all",
        }.into();
        let wait = owner.control_requested.begin_approval_wait();
        let result = coordinator.resolve_tool(source, context, Some(snapshot.autonomy_profile), &effective.descriptor,
            &request.input, owner.cancellation.clone(), Some(owner.session_input_stream.input_notifier()),
            pending, Duration::from_secs(timeout_secs.max(1))).await;
        drop(wait);
        emit_approval_resolution_event(owner.assessment.cowd_bus(), coordinator.queue(), &result);
        let grant = match result {
            Ok(crate::ApprovalResolution::Approved { grant, .. }) => grant,
            Ok(crate::ApprovalResolution::ControlRequested { reason, .. }) => { owner.control_requested.signal(); return Err(reason); },
            Ok(crate::ApprovalResolution::Denied { reason, .. }) | Ok(crate::ApprovalResolution::Cancelled { reason, .. }) => return Err(reason),
            Err(error) => return Err(error),
        };
        self.fence.verify(crate::SessionExecutionFencePhase::ToolExecution).await?;
        if !self.reservation_is_current() || owner.cancellation.is_cancelled() || owner.control_requested.load(std::sync::atomic::Ordering::Acquire)
            || owner.identity.permission_policy.execution_policy_control().revision() != snapshot.revision
            || owner.original_session_claim.as_ref().is_none_or(|claim| !Arc::ptr_eq(claim, &self.claim)) {
            return Err("original Host owner changed during approval".into());
        }
        let binding = crate::authorization_negotiator::PreparedMemberBinding::from_host(Arc::clone(self));
        let risk = crate::approval_coordinator::task_risk_for_effect(&effective.descriptor);
        let prepared = coordinator.queue().prepare_member_approval(&grant.grant_id, &matched_context, risk, &binding)?;
        self.prepare_authority(Some(prepared))
    }
}

/// Whether the effect owner, when it is present, still names this call.
///
/// An absent owner is not a mismatch: the Read family installs no owner at
/// all, and the durable preparation fence carries the identity of the
/// original call from its own durable admission intent instead. An owner
/// that is present must name this exact call, or the member is refused.
pub(crate) fn original_host_effect_owner_matches(
    owner: Option<&Arc<crate::execution_core::graph::OriginalFreshEffectOwner>>,
    store: &Arc<crate::runtime_event_store::RuntimeEventStore>,
    original: &crate::RuntimeToolExecutionRequest,
) -> bool {
    owner.is_none_or(|owner| owner.matches_original(store, original))
}

impl SharedPolicyOwner {
    pub(crate) fn original_member_claim(&self) -> Result<Arc<crate::execution_core::services::OriginalSessionClaim>, String> {
        self.original_session_claim.clone().ok_or_else(|| "original member Session claim unavailable".into())
    }
    pub(crate) fn register_original_control(&self, control: &Arc<crate::conversation::MemberCommitControl>) {
        self.cancellation.register_member_commit_control(control);
        self.control_requested.register(control);
    }
    /// Verify the Session lineage against the same owner instances held by
    /// the active ingress. No authority is reconstructed from a child packet.
    pub(crate) fn matches_original_lineage(
        &self,
        claim: &Arc<crate::execution_core::services::OriginalSessionClaim>,
        policy: &crate::permissions::SessionExecutionPolicyControl,
        requested: &Arc<crate::conversation::OriginalTurnInputControl>,
        store: &Arc<crate::RuntimeEventStore>,
    ) -> bool {
        self.original_session_claim.as_ref().is_some_and(|original| Arc::ptr_eq(original, claim))
            && self.identity.session_id == claim.session_id()
            && self.identity.execution.as_ref().is_some_and(|execution| execution.turn_id == claim.turn_id())
            && self.identity.permission_policy.execution_policy_control().same_owner(policy)
            && Arc::ptr_eq(&self.control_requested, requested)
            && self.uses_original_store(store)
    }
    pub(crate) fn uses_original_store(&self, store: &Arc<crate::RuntimeEventStore>) -> bool {
        self.assessment.runtime_event_store.as_ref().is_some_and(|original| Arc::ptr_eq(original, store))
    }
    /// Resolve the current Process member's own approval without consuming it.
    /// The checked member transaction consumes an once grant with its lease.
    pub(crate) async fn prepare_process_member_approval(
        &self,
        evaluation: &crate::authorization_negotiator::ProcessMemberEvaluation,
    ) -> Result<Arc<crate::authorization_negotiator::PreparedMemberAuthority>, String> {
        let member = evaluation.member();
        let request = evaluation.request();
        if !member.uses_original_policy_owner(self) || !member.original_preparation_fence_live()
            || !self.uses_original_store(member.owner().store())
            || self.original_session_claim.as_ref().is_none_or(|claim| !Arc::ptr_eq(claim, member.claim())) {
            return Err("original Process approval owner changed".into());
        }
        member.owner().check_original_stop()?;
        let coordinator = self.approval_coordinator.as_ref().ok_or("original approval coordinator unavailable")?;
        let snapshot = evaluation.snapshot();
        if snapshot.revision != request.policy_revision { return Err("Process policy changed before approval".into()); }
        let effective = evaluation.effective();
        let source = harness_contract::policy::ApprovalSource {
            kind: harness_contract::policy::ApprovalSourceKind::Agent,
            session_id: Some(member.packet().session_id().into()),
            agent_id: Some(member.packet().agent_id().into()),
            team_id: member.packet().team_id().map(str::to_string),
            mission_id: None, resource_ref: None, review_ref: None, application: None,
        };
        let mut context = crate::agent_in_process_worker::process_tool_approval_context(
            member.packet(), snapshot, effective, &self.checkpoint_workspace_id, &request.idempotency_key);
        context.explicit_ask = request.context.override_decision() == Some(crate::permissions::PermissionOverride::Ask)
            || matches!(effective.descriptor.approval_class,
                harness_contract::tool::ToolApprovalClass::User | harness_contract::tool::ToolApprovalClass::Administrator);
        let mut matched_context = context.clone();
        matched_context.profile_id = match snapshot.approval_profile {
            harness_contract::policy::ApprovalProfile::Supervised => "supervised",
            harness_contract::policy::ApprovalProfile::Balanced => "balanced",
            harness_contract::policy::ApprovalProfile::Autonomous => "autonomous",
            harness_contract::policy::ApprovalProfile::TrustAll => "trust-all",
        }.into();
        let timeout = member.original_member_timeout_secs().ok_or("Process member timeout unavailable")?;
        let risk = crate::agent_in_process_worker::process_approval_task_risk(
            evaluation.risk());
        let wait = self.control_requested.begin_approval_wait();
        let result = coordinator.resolve_tool_with_risk(source, context, Some(snapshot.autonomy_profile), &effective.descriptor,
            &request.input, member.owner().cancellation(), Some(self.session_input_stream.input_notifier()),
            None, Duration::from_secs(timeout.max(1)), risk).await;
        drop(wait);
        emit_approval_resolution_event(self.assessment.cowd_bus(), coordinator.queue(), &result);
        let grant = match result {
            Ok(crate::ApprovalResolution::Approved { grant, .. }) => grant,
            Ok(crate::ApprovalResolution::ControlRequested { reason, .. }) => { self.control_requested.signal(); return Err(reason); },
            Ok(crate::ApprovalResolution::Denied { reason, .. }) | Ok(crate::ApprovalResolution::Cancelled { reason, .. }) => return Err(reason),
            Err(error) => return Err(error),
        };
        member.owner().fence().verify(crate::SessionExecutionFencePhase::ToolExecution).await?;
        member.owner().check_original_stop()?;
        if !member.original_preparation_fence_live() || self.cancellation.is_cancelled()
            || self.control_requested.load(std::sync::atomic::Ordering::Acquire)
            || self.identity.permission_policy.execution_policy_control().revision() != snapshot.revision
            || self.original_session_claim.as_ref().is_none_or(|claim| !Arc::ptr_eq(claim, member.claim())) {
            return Err("original Process owner changed during approval".into());
        }
        let binding = evaluation.binding();
        let approval = coordinator.queue().prepare_member_approval(&grant.grant_id, &matched_context, risk, binding)?;
        let negotiator = member.owner().negotiator().clone();
        negotiator.finish_process_member_approval(evaluation, approval)
    }
    pub(crate) fn original_policy_control(&self) -> crate::permissions::SessionExecutionPolicyControl {
        self.identity.permission_policy.execution_policy_control()
    }
    pub(crate) fn uses_original_claim(&self, claim: &crate::execution_core::services::OriginalSessionClaim) -> bool {
        self.original_session_claim.as_deref().is_some_and(|original| std::ptr::eq(original, claim))
    }
    pub(crate) fn original_consumption(
        &self, authorization: &harness_contract::tool::ToolExecutionAuthorization,
    ) -> Option<crate::authorization_negotiator::OriginalLeaseConsumption> {
        self.assessment.authorization_negotiator.original_consumption(
            &authorization.authorization_lease, &authorization.request_id)
            .filter(|consumption| consumption.belongs_to(&self.assessment.authorization_negotiator))
    }
    /// Authenticate the same producer target for root and delegated calls.
    pub(crate) fn verify_read_ref_target(
        &self, expected_ticket: &crate::execution_core::NodeExecutionTicket, request: &crate::RuntimeToolExecutionRequest,
        events: &[crate::DurableRuntimeEvent], context: &PermissionContext,
        target: Option<&OriginalReadRefTarget>,
    ) -> Result<(), String> {
        if request.tool_name != "read_ref" {
            return if target.is_none() { Ok(()) } else { Err("unexpected read_ref target".into()) };
        }
        let outer = harness_contract::step::parse_strict_json(&request.input).map_err(|e| e.to_string())?;
        let derived = crate::agentic::action_invocation::read_ref_directory_input(&outer)?;
        let target = match (derived, target) {
            (None, None) => return Ok(()),
            (Some(input), Some(target)) if input == target.input => target,
            _ => return Err("original read_ref target unavailable or changed".into()),
        };
        let session = request.session_id.as_deref().ok_or("original Session missing")?;
        if target.ticket != *expected_ticket || self.identity.session_id != session
            || !std::sync::Weak::ptr_eq(&target.original_channel, &request.tool_progress.original_read_ref_identity())
            || target.authorization.policy_revision != request.policy_revision {
            return Err("original read_ref target identity changed".into());
        }
        let binding = request.step_binding.as_ref().ok_or("original Step missing")?;
        let step = binding.resolve(self.tool_lease.clone()).map_err(|error| error.to_string())?;
        let text = serde_json::to_string(&target.input).map_err(|error| error.to_string())?;
        if step.validate_call(&binding.origin, session, &request.tool_use_id, "state_inspect", &text,
            chrono::Utc::now().timestamp_millis()).map_err(|error| error.to_string())? != target.input
            || step.lease().describe_effect("state_inspect", &target.input) != target.effect {
            return Err("original read_ref Step or effect changed".into());
        }
        step.lease().validate_authorization(&target.authorization, "state_inspect", &target.input)
            .map_err(|error| error.to_string())?;
        let consumption = self.original_consumption(&target.authorization)
            .ok_or("original read_ref target consumption unavailable")?;
        if consumption.lease != target.authorization.authorization_lease {
            return Err("original read_ref target consumption changed".into());
        }
        let mut original_request = serde_json::to_value(request).map_err(|e| e.to_string())?;
        let mut captured_request = target.original_request.clone();
        for value in [&mut original_request, &mut captured_request] {
            let object = value.as_object_mut().ok_or("original read_ref request invalid")?;
            object.remove("authorization"); object.remove("execution_decision");
        }
        if original_request != captured_request { return Err("original read_ref request changed".into()); }
        let context = original_permission_context_json(context);
        let ticket = serde_json::to_value(&target.ticket).map_err(|e| e.to_string())?;
        let expected_step = serde_json::to_value(&request.step_binding).map_err(|e| e.to_string())?;
        let lease = serde_json::to_value(&target.authorization.authorization_lease).map_err(|e| e.to_string())?;
        let transition = serde_json::to_value(&consumption.transition).map_err(|e| e.to_string())?;
        let derived_fact = serde_json::json!({"tool":"state_inspect", "input":target.input, "effect":target.effect});
        let assessed = events.iter().filter(|event| {
            let fact = &event.payload["_call_refusal_binding"];
            let mut pre = fact["request"].clone();
            let Some(object) = pre.as_object_mut() else { return false; };
            object.remove("authorization"); object.remove("execution_decision");
            event.kind == "authorization.capability_assessed"
                && event.actor.as_deref() == Some("conversation_runtime")
                && event.stream_id == format!("session:{session}")
                && fact["owner"]["ticket"] == ticket && fact["step_binding"] == expected_step
                && fact["call_id"].as_str() == Some(request.tool_use_id.as_str())
                && fact["effect_key"].as_str() == Some(request.idempotency_key.as_str())
                && pre == original_request && fact["derived_target"] == derived_fact
                && event.payload["capability"].as_str() == Some("state_inspect")
                && event.payload["_original_permission_context"] == context && event.payload["lease"] == lease
        }).count();
        let consumed = events.iter().filter(|event| event.kind == "authorization.lease_transition"
            && event.actor.as_deref() == Some("conversation_runtime") && event.status.as_deref() == Some("consumed")
            && event.payload == transition).count();
        if assessed != 1 || consumed != 1 { return Err("original read_ref target producer events unavailable".into()); }
        Ok(())
    }
    pub(crate) fn new_single_commit_control(&self) -> Arc<crate::conversation::MemberCommitControl> {
        let control = self.cancellation.member_commit_control();
        self.control_requested.register(&control);
        control
    }

    /// Synchronous single-call gate. The caller supplies additional Program,
    /// goal and review streams before locks are acquired and invokes A's
    /// checked entry inside `append`; it must not await or reacquire locks.
    pub(crate) fn with_single_checked<T>(
        &self,
        services: &crate::RuntimeServices,
        ticket: &crate::execution_core::graph::NodeExecutionTicket,
        request: &crate::RuntimeToolExecutionRequest,
        events: &[crate::DurableRuntimeEvent],
        permission_context: &PermissionContext,
        original_decision: &crate::execution_core::RuntimeExecutionDecision,
        read_ref_target: Option<&OriginalReadRefTarget>,
        control: Arc<crate::conversation::MemberCommitControl>,
        additional_streams: &[String],
        append: impl FnOnce(&OriginalSingleCheckedInputs<'_>) -> Result<T, String>,
    ) -> Result<T, String> {
        let session = request.session_id.as_deref().ok_or("original Session missing")?;
        let auth = request.authorization.as_ref().ok_or("original authorization missing")?;
        // `events` are the immutable values returned by this producer's
        // successful appends, captured inside the same call. Do not read those
        // writes back from storage on the same-owner checked path.
        let producer_store = self.assessment.runtime_event_store.as_ref()
            .ok_or("original single-call producer store missing")?;
        if !Arc::ptr_eq(producer_store, services.event_store()) {
            return Err("original single-call store owner mismatch".into());
        }
        if session != self.identity.session_id || request.tool_name == "agent_action_batch"
            || request.execution_decision.as_ref() != Some(original_decision)
            || self.cancellation.is_cancelled()
            || self.control_requested.load(std::sync::atomic::Ordering::Acquire) {
            return Err("original single-call owner is not live".into());
        }
        let claim = self.original_session_claim.as_deref().ok_or("original Session claim missing")?;
        let turn = harness_contract::turn::TurnId::from_string(claim.turn_id());
        let mut streams = additional_streams.to_vec();
        streams.push(ticket.graph_id.clone());
        streams.push(format!("session:{session}"));
        streams.extend(events.iter().map(|event| event.stream_id.clone()));
        streams.sort(); streams.dedup();
        if events.is_empty() || streams.iter().any(String::is_empty) { return Err("original single-call sources missing".into()); }
        services.with_original_session_policy(session, &self.identity.permission_policy.execution_policy_control(), |policy| {
            if policy.revision != request.policy_revision || auth.policy_revision != policy.revision
                || policy.sandbox_posture != request.sandbox_posture || self.cancellation.is_cancelled()
                || self.control_requested.load(std::sync::atomic::Ordering::Acquire) {
                return Err("original single-call policy or control changed".into());
            }
            services.event_store().with_authenticated_stream_locks(&streams, |held| {
                crate::execution_core::graph::validate_node_ticket_start(services.event_store(), ticket, None)
                    .map_err(|error| error.to_string())?;
                if request.parent_execution.as_ref().is_none_or(|parent| parent.execution_id != ticket.graph_id || parent.node_id != ticket.node_id)
                    || request.parent_execution_attempt != Some(ticket.attempt) || claim.session_id() != session {
                    return Err("original single-call ticket or claim mismatch".into());
                }
                let binding = request.step_binding.as_ref().ok_or("original Step missing")?;
                let step = binding.resolve(self.tool_lease.clone()).map_err(|error| error.to_string())?;
                let input = step.validate_call(&binding.origin, session, &request.tool_use_id, &request.tool_name,
                    &request.input, chrono::Utc::now().timestamp_millis()).map_err(|error| error.to_string())?;
                step.lease().validate_authorization(auth, &request.tool_name, &input).map_err(|error| error.to_string())?;
                let original_consumption = self.assessment.authorization_negotiator
                    .original_consumption(&auth.authorization_lease, &auth.request_id)
                    .ok_or("original single-call lease consumption unavailable")?;
                if !original_consumption.belongs_to(&self.assessment.authorization_negotiator)
                    || original_consumption.lease != auth.authorization_lease {
                    return Err("original single-call lease producer mismatch".into());
                }
                let original_transition = serde_json::to_value(&original_consumption.transition).map_err(|e| e.to_string())?;
                let mut assessed = 0; let mut consumed = 0;
                let mut pre_authorization_request = serde_json::to_value(request).map_err(|e| e.to_string())?;
                let request_object = pre_authorization_request.as_object_mut().ok_or("original request invalid")?;
                request_object.remove("authorization");
                request_object.remove("execution_decision");
                let context_decision = permission_context.override_decision().map(|decision| match decision {
                    crate::permissions::PermissionOverride::Allow => "allow",
                    crate::permissions::PermissionOverride::Deny => "deny",
                    crate::permissions::PermissionOverride::Ask => "ask",
                });
                let original_context = serde_json::json!({"override_decision":context_decision,
                    "override_reason":permission_context.override_reason()});
                for event in events {
                    let mut assessed_request = event.payload["_call_refusal_binding"]["request"].clone();
                    let assessed_decision = assessed_request.as_object_mut().and_then(|object| object.remove("execution_decision"));
                    let decision_matches = assessed_decision.as_ref().is_none_or(|decision|
                        serde_json::to_value(original_decision).is_ok_and(|expected| *decision == expected));
                    if event.kind == "authorization.capability_assessed"
                        && decision_matches
                        && event.actor.as_deref() == Some("conversation_runtime")
                        && event.stream_id == format!("session:{session}")
                        && event.payload["_call_refusal_binding"]["tool_name"].as_str() == Some(request.tool_name.as_str())
                        && event.payload["_call_refusal_binding"]["effect_key"].as_str() == Some(request.idempotency_key.as_str())
                        && event.payload["_call_refusal_binding"]["step_binding"] == serde_json::to_value(&request.step_binding).map_err(|e| e.to_string())?
                        && assessed_request == pre_authorization_request
                        && event.payload["_original_permission_context"] == original_context
                        && event.payload["_call_refusal_binding"]["call_id"].as_str() == Some(request.tool_use_id.as_str())
                        && event.payload["_call_refusal_binding"]["owner"]["ticket"] == serde_json::to_value(ticket).map_err(|e| e.to_string())?
                        && event.payload["lease"]["lease_id"].as_str() == Some(auth.authorization_lease.lease_id.as_str()) { assessed += 1; }
                    if event.kind == "authorization.lease_transition" && event.status.as_deref() == Some("consumed")
                        && event.actor.as_deref() == Some("conversation_runtime")
                        && event.payload == original_transition
                        && event.payload["lease"]["lease_id"].as_str() == Some(auth.authorization_lease.lease_id.as_str())
                        && event.payload["idempotency_key"].as_str() == Some(auth.request_id.as_str()) { consumed += 1; }
                }
                if assessed != 1 || consumed != 1 { return Err("original single-call assessment/lease provenance mismatch".into()); }
                self.verify_read_ref_target(ticket, request, events, permission_context, read_ref_target)?;
                if self.cancellation.is_cancelled() || self.control_requested.load(std::sync::atomic::Ordering::Acquire)
                    || self.session_input_stream.active_turn_id().as_ref() != Some(&turn) {
                    return Err("original turn no longer active".into());
                }
                let fence = claim.fence()?;
                let cursor = self.session_input_stream.highest_consumed_cursor(&turn);
                if self.session_input_stream.active_turn_id().as_ref() != Some(&turn) { return Err("original turn changed while reading cursor".into()); }
                let consumed_input_sequence = match cursor {
                    Some(cursor) if cursor.generation == fence.session_generation => usize::try_from(cursor.sequence).map_err(|_| "consumed cursor overflow")?.max(fence.input_sequence),
                    Some(_) => return Err("consumed cursor generation mismatch".into()),
                    None => fence.input_sequence,
                };
                let sources = streams.iter().map(|stream_id| Ok(crate::ExpectedStreamRevision {
                    stream_id: stream_id.clone(), expected_revision: services.event_store().stream_revision(stream_id).map_err(|e| e.to_string())?,
                })).collect::<Result<Vec<_>, String>>()?;
                let assessment_event = events.iter().find(|event| event.kind == "authorization.capability_assessed"
                    && event.payload["_call_refusal_binding"]["call_id"].as_str() == Some(request.tool_use_id.as_str())
                    && event.payload["lease"]["lease_id"].as_str() == Some(auth.authorization_lease.lease_id.as_str())).ok_or("assessment missing")?;
                let lease_event = events.iter().find(|event| event.kind == "authorization.lease_transition" && event.status.as_deref() == Some("consumed")
                    && event.payload["lease"]["lease_id"].as_str() == Some(auth.authorization_lease.lease_id.as_str())
                    && event.payload["idempotency_key"].as_str() == Some(auth.request_id.as_str())).ok_or("lease event missing")?;
                if request.tool_name == harness_contract::agent_action::OBJECTIVE_COMPLETE_REQUEST_TOOL_ID {
                    let coord = |event: &crate::DurableRuntimeEvent| serde_json::json!({
                        "stream_id":event.stream_id, "sequence":event.sequence, "event_id":event.event_id,
                        "transaction_id":event.transaction_id, "transaction_index":event.transaction_index, "commit_cursor":event.commit_cursor,
                    });
                    let authorized = events.iter().filter(|event| event.kind == "authorization.call_authorized"
                        && event.scope == RuntimeEventScope::Tool && event.actor.as_deref() == Some("conversation_runtime")
                        && event.status.as_deref() == Some("authorized") && event.stream_id == format!("session:{session}")
                        && event.payload["version"].as_u64() == Some(1)
                        && event.payload["binding"] == assessment_event.payload["_call_refusal_binding"]
                        && event.payload["authorization"] == serde_json::to_value(auth).unwrap_or(serde_json::Value::Null)
                        && event.payload["assessment"] == coord(assessment_event)
                        && event.payload["consume"] == coord(lease_event)).count();
                    if authorized != 1 { return Err("original completion call authorization event missing or ambiguous".into()); }
                }
                append(&OriginalSingleCheckedInputs::Root(RootSingleCheckedInputs { held, sources: &sources, control, claim, consumed_input_sequence, ticket, request, assessment_event, lease_event, original_events: events, original_decision, read_ref_target, _original_owner_seal: () }))
            })
        })
    }

    pub(crate) fn session_id(&self) -> &str { &self.identity.session_id }
    pub(crate) fn policy_revision(&self) -> u64 {
        self.identity.permission_policy.execution_policy_control().revision()
    }
    pub(crate) fn owns_turn(&self, binding: &crate::step_snapshot::StepCallBinding) -> bool {
        self.identity.execution.as_ref().is_some_and(|execution|
            execution.session_id == binding.snapshot.identity.session_id
                && execution.turn_id == binding.snapshot.identity.turn_id)
    }
    pub(crate) fn take_control_requested(&self) -> bool {
        self.control_requested.swap(false, std::sync::atomic::Ordering::AcqRel)
    }
    /// Prepare exact member input and hook context without negotiating or
    /// consuming a lease/approval. This value alone grants no execution right.
    async fn prepare_original_batch_policy_input(
        &self,
        binding: &crate::step_snapshot::StepCallBinding,
        outer_invocation: &harness_contract::tool::GovernedToolInvocation,
        outer_input: &str,
        parent: &harness_contract::execution_graph::ExecutionParentBinding,
        attempt: u32,
        index: usize,
        pinned: Option<tools::ToolHostLease>,
        reservation: &crate::runtime_event_store::OriginalMemberReservation,
        effect_fence: &Arc<crate::runtime_event_store::OriginalMemberEffectFence>,
    ) -> Result<PreparedOriginalBatchPolicyInput, RuntimeError> {
        if self.cancellation.is_cancelled() || self.control_requested.load(std::sync::atomic::Ordering::Acquire) {
            return Err(RuntimeError::new("original member owner cancelled before preparation"));
        }
        let claim = self.original_session_claim.clone()
            .ok_or_else(|| RuntimeError::new("original member Session claim unavailable"))?;
        if claim.session_id() != self.identity.session_id {
            return Err(RuntimeError::new("original member Session claim mismatch"));
        }
        let original_turn = harness_contract::turn::TurnId::from_string(claim.turn_id());
        let fence = self.session_execution_fence.as_ref()
            .ok_or_else(|| RuntimeError::new("original member Session fence unavailable"))?;
        fence.verify(crate::SessionExecutionFencePhase::ToolExecution).await.map_err(RuntimeError::new)?;
        let prep_binding = binding.clone();
        let prep_pinned = pinned.clone();
        let prep_session = self.identity.session_id.clone();
        let prep_invocation = outer_invocation.clone();
        let prep_input = outer_input.to_owned();
        let prep_parent = parent.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            crate::step_snapshot::prepare_agent_batch_member(
                &prep_binding, prep_pinned, &prep_session, &prep_invocation, &prep_input,
                &prep_parent, attempt, index, chrono::Utc::now().timestamp_millis(),
            ).map(Box::new)
        }).await.map_err(|error| RuntimeError::new(error.to_string()))??;
        if self.cancellation.is_cancelled() || self.control_requested.load(std::sync::atomic::Ordering::Acquire) {
            return Err(RuntimeError::new("original member owner cancelled before hook"));
        }
        fence.verify(crate::SessionExecutionFencePhase::ToolExecution).await.map_err(RuntimeError::new)?;
        if self.cancellation.is_cancelled() || self.control_requested.load(std::sync::atomic::Ordering::Acquire) {
            return Err(RuntimeError::new("original member owner cancelled before hook"));
        }
        let input = serde_json::to_string(&prepared.input).map_err(|e| RuntimeError::new(e.to_string()))?;
        let store = self.assessment.runtime_event_store.as_ref()
            .ok_or_else(|| RuntimeError::new("original member store unavailable"))?;
        store.claim_original_member_hook(effect_fence, reservation).map_err(|error| RuntimeError::new(error.to_string()))?;
        let hook = {
            let mut reporter = self.hook_progress_reporter.lock().unwrap_or_else(|e| e.into_inner());
            self.hook_runner.run_pre_tool_use_with_context(&prepared.tool, &input,
                Some(&self.hook_abort_signal), reporter.as_mut().map(|r| r.as_mut() as &mut dyn crate::HookProgressReporter))
        };
        let effective = hook.updated_input().map_or_else(|| input.clone(), ToOwned::to_owned);
        let mut context = PermissionContext::new(hook.permission_override(), hook.permission_reason().map(ToOwned::to_owned));
        if hook.is_cancelled() || hook.is_failed() || hook.is_denied() {
            context = PermissionContext::new(Some(crate::permissions::PermissionOverride::Deny),
                Some(format!("PreToolUse hook denied batch member {}", prepared.index)));
        }
        let prep_binding = binding.clone();
        let prep_session = self.identity.session_id.clone();
        let prep_invocation = outer_invocation.clone();
        let prep_input = outer_input.to_owned();
        let prep_parent = parent.clone();
        let hook_effective = effective;
        let prep_effective = hook_effective.clone();
        let (original_prepared, prepared, effective) = tokio::task::spawn_blocking(move || {
            let (original, after_hook) = crate::step_snapshot::prepare_effective_agent_batch_member(
                &prep_binding, pinned, &prep_session, &prep_invocation, &prep_input,
                &prep_parent, attempt, index, chrono::Utc::now().timestamp_millis(), &prep_effective)?;
            if original != *prepared { return Err(RuntimeError::new("original member changed while hook ran")); }
            let effective = serde_json::to_string(&after_hook.input).map_err(|e| RuntimeError::new(e.to_string()))?;
            Ok::<_, RuntimeError>((Box::new(original), Box::new(after_hook), effective))
        }).await.map_err(|error| RuntimeError::new(error.to_string()))??;
        fence.verify(crate::SessionExecutionFencePhase::ToolExecution).await.map_err(RuntimeError::new)?;
        if self.cancellation.is_cancelled() || self.control_requested.load(std::sync::atomic::Ordering::Acquire) {
            return Err(RuntimeError::new("original member owner cancelled during preparation"));
        }
        Ok(PreparedOriginalBatchPolicyInput { claim, original_turn, fence: fence.clone(), original_prepared, prepared, hook_effective, effective, context })
    }

    /// Borrow this actual Process member's original Step and pinned host.
    /// The member owner has already authenticated the AgentTask ticket and
    /// holds its in-flight reservation; callers cannot substitute a lease.
    pub(crate) async fn prepare_original_process_batch_policy_input(
        &self, member: &crate::agent_in_process_worker::OriginalProcessPreparedMember,
    ) -> Result<PreparedOriginalBatchPolicyInput, RuntimeError> {
        if !member.uses_original_policy_owner(self) || !member.original_reservation_matches() {
            return Err(RuntimeError::new("original Process policy owner or reservation changed"));
        }
        let original = member.original_request();
        let invocation = original.compiled_invocation.as_ref().ok_or_else(|| RuntimeError::new("original Process invocation missing"))?;
        let parent = original.parent_execution.as_ref().ok_or_else(|| RuntimeError::new("original Process parent missing"))?;
        let reservation = member.original_reservation().ok_or_else(|| RuntimeError::new("original Process reservation unavailable"))?;
        let effect_fence = member.original_effect_fence().ok_or_else(|| RuntimeError::new("original Process effect fence unavailable"))?;
        self.prepare_original_batch_policy_input(member.step(), invocation, &original.input,
            parent, member.packet().attempt, member.index(), Some(member.original_pinned_host().clone()), reservation, effect_fence).await
    }

    /// Original Host producer: validate the started ticket and exact outer
    /// call, then capture the member hook result without consuming authority.
    pub(crate) async fn prepare_original_host_member(
        self: &Arc<Self>, ticket: crate::execution_core::graph::NodeExecutionTicket,
        original: Arc<crate::RuntimeToolExecutionRequest>, index: usize,
        reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
        effect_fence: Arc<crate::runtime_event_store::OriginalMemberEffectFence>,
    ) -> Result<Arc<OriginalHostPreparedMember>, RuntimeError> {
        let store = self.assessment.runtime_event_store.as_ref()
            .ok_or_else(|| {
                RuntimeError::new("original Host member store unavailable")
            })?;
        // The effect owner answers the identity question for the families
        // that install one. An absent owner is not a refusal: the Read
        // family installs no owner, and the durable preparation fence
        // carries the identity of the original call from its own durable
        // admission intent instead. An owner that is present must still
        // match this exact original call.
        let fresh_effect = original.tool_progress.original_fresh_effect().map_err(|error| {
            RuntimeError::new(error)
        })?;
        if !original_host_effect_owner_matches(fresh_effect.as_ref(), store, &original) {
            return Err(RuntimeError::new("original Host fresh effect owner mismatch"));
        }
        let validation_store = Arc::clone(store);
        let validation_ticket = ticket.clone();
        tokio::task::spawn_blocking(move || {
            crate::execution_core::graph::validate_node_ticket_start(
                &validation_store, &validation_ticket, None,
            ).map(|_| ()).map_err(|error| RuntimeError::new(error.to_string()))
        })
        .await
        .map_err(|error| RuntimeError::new(error.to_string()))??;
        let member_index = u64::try_from(index).map_err(|_| {
            RuntimeError::new("original Host member index overflow")
        })?;
        if !reservation.matches_original(store, &original.idempotency_key, member_index, &ticket) {
            return Err(RuntimeError::new("original Host member reservation mismatch"));
        }
        let binding = original.step_binding.as_ref().ok_or_else(|| {
            RuntimeError::new("original Host Step missing")
        })?;
        let invocation = original.compiled_invocation.as_ref().ok_or_else(|| {
            RuntimeError::new("original Host invocation missing")
        })?;
        let parent = original.parent_execution.as_ref().ok_or_else(|| {
            RuntimeError::new("original Host parent missing")
        })?;
        let attempt = original.parent_execution_attempt.ok_or_else(|| {
            RuntimeError::new("original Host attempt missing")
        })?;
        let (calls, _) = crate::host::decode_tool_batch(&ticket.payload_ref).map_err(|e| {
            RuntimeError::new(e.to_string())
        })?;
        let mut matching = calls.iter().filter(|call| call.id == original.tool_use_id);
        let call = matching.next().ok_or_else(|| {
            RuntimeError::new("original Host ticket call missing")
        })?;
        let original_json = harness_contract::step::parse_strict_json(&original.input).map_err(|e| {
            RuntimeError::new(e.to_string())
        })?;
        let model_json = harness_contract::step::parse_strict_json(&call.input).map_err(|e| {
            RuntimeError::new(e.to_string())
        })?;
        let input_owned = model_json == original_json || original.tool_progress.original_governed_hook()
            .map_err(|error| {
                RuntimeError::new(error)
            })?.is_some_and(|hook| hook.matches(&ticket, &original, &model_json));
        if matching.next().is_some() || call.name != original.tool_name || call.binding.as_ref() != Some(binding)
            || !input_owned
            || original.tool_name != "agent_action_batch" || original.idempotency_key != binding.effect_idempotency_key()
            || original.tool_use_id != binding.origin.call_id || !self.owns_turn(binding)
            || parent.execution_id != ticket.graph_id || parent.node_id != ticket.node_id || attempt != ticket.attempt
            || original.session_id.as_deref() != Some(self.session_id())
            || original.policy_revision == 0 || self.policy_revision() != original.policy_revision {
            return Err(RuntimeError::new("original Host member source mismatch"));
        }
        let candidate = self.prepare_original_batch_policy_input(binding, invocation, &original.input, parent, attempt, index, self.tool_lease.clone(), &reservation, &effect_fence).await.map_err(|error| {
            error
        })?;
        candidate.fence.verify(crate::SessionExecutionFencePhase::ToolExecution).await.map_err(|error| {
            RuntimeError::new(error)
        })?;
        if !original_host_effect_owner_matches(fresh_effect.as_ref(), store, &original)
            || !reservation.matches_original(store, &original.idempotency_key, member_index, &ticket)
            || self.policy_revision() != original.policy_revision || candidate.claim.session_id() != self.session_id() {
            return Err(RuntimeError::new("original Host member owner changed during preparation"));
        }
        Ok(Arc::new(OriginalHostPreparedMember { owner: Arc::clone(self), store: Arc::clone(store), reservation, fresh_effect, ticket,
            original: Arc::clone(&original), step: binding.clone(), original_prepared: candidate.original_prepared,
            prepared: candidate.prepared, hook_effective: candidate.hook_effective, effective: candidate.effective,
            context: candidate.context, timeout_secs: 60, claim: candidate.claim, fence: candidate.fence.clone() }))
    }

    async fn negotiate_tool_authorization_inner(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        timeout_secs: u64,
        _prompter: &crate::permissions::SharedPrompter,
        binding: Option<&serde_json::Value>,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        // A retirement action is how an Agent stops the work it owns. Refusing
        // it once the turn is cancelled leaves the Task and its physical
        // attempt alive with no actor able to close them. Other tool effects
        // remain refused after cancellation.
        let retirement_control = retirement_control_tool(&descriptor.tool_id);
        if !retirement_control
            && (self.cancellation.is_cancelled()
                || self.control_requested.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(RuntimeError::new("original policy owner cancelled or control requested"));
        }
        let execution_policy = self.identity.permission_policy.execution_policy_control().snapshot();
        let initial = self.assess_tool_authorization_at(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context.clone(),
            timeout_secs,
            &execution_policy,
            binding,
        )?;
        let ToolAuthorizationDecision::Gap {
            assessment,
            effective,
            assessment_event: _,
        } = initial
        else {
            return Ok(initial);
        };
        if assessment.path != harness_contract::policy::AuthorizationPath::HumanApproval {
            let assessment = self.govern_capability_gap(assessment);
            let assessment_event = self.assessment.record_capability_assessment_for_call(&assessment, binding, Some(&permission_context));
            if binding.is_some() && assessment_event.is_none() { return Err(RuntimeError::new("bound capability assessment append failed")); }
            return Ok(ToolAuthorizationDecision::Gap {
                assessment,
                effective,
                assessment_event,
            });
        }

        let explicit_ask = permission_context.override_decision()
            == Some(crate::permissions::PermissionOverride::Ask);
        let request = self.identity.authorization_request(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context,
            &execution_policy,
        );
        let Some(coordinator) = &self.approval_coordinator else {
            let assessment = self.govern_capability_gap(assessment);
            let assessment_event = self.assessment.record_capability_assessment_for_call(&assessment, binding, Some(&request.context));
            if binding.is_some() && assessment_event.is_none() { return Err(RuntimeError::new("bound capability assessment append failed")); }
            return Ok(ToolAuthorizationDecision::Gap {
                assessment,
                effective,
                assessment_event,
            });
        };
        let approved_grant = {
            let execution_context = self.identity.execution.clone();
            let activity_binding = self.activity.clone();
            let source = harness_contract::policy::ApprovalSource {
                kind: if self.identity.agent_id != "primary" {
                    harness_contract::policy::ApprovalSourceKind::Agent
                } else {
                    harness_contract::policy::ApprovalSourceKind::Session
                },
                session_id: Some(self.identity.session_id.as_str().to_string()),
                agent_id: (self.identity.agent_id != "primary").then(|| self.identity.agent_id.clone()),
                team_id: self.memory_team_id.clone(),
                mission_id: None,
                resource_ref: Some(self.checkpoint_workspace_id.clone()),
                review_ref: None,
                application: None,
            };
            let context = harness_contract::policy::ApprovalContext {
                principal_id: request.principal_id.clone(),
                profile_id: execution_policy.autonomy_profile.as_str().to_string(),
                approval_profile: Some(execution_policy.approval_profile),
                workspace_key: self.checkpoint_workspace_id.clone(),
                session_id: Some(self.identity.session_id.as_str().to_string()),
                turn_id: execution_context
                    .as_ref()
                    .map(|value| value.turn_id.clone()),
                task_id: activity_binding
                    .as_ref()
                    .map(|binding| binding.task_id.clone()),
                capability: descriptor.tool_id.clone(),
                invocation_id: Some(idempotency_key.clone()),
                execution_id: execution_context
                    .as_ref()
                    .map(|value| value.execution_id.clone()),
                strategy_decision_ref: None,
                source_surface: Some("gateway_session".to_string()),
                resource_targets: descriptor
                    .scopes
                    .iter()
                    .filter_map(|scope| scope.target.clone())
                    .collect(),
                effect: Some(effective.descriptor.clone()),
                explicit_ask,
                policy_revision: execution_policy.revision,
                requested_sandbox_posture: Some(execution_policy.sandbox_posture),
                effective_sandbox_posture: Some(execution_policy.sandbox_posture),
            };
            let pending_hook = self.assessment.cowd_bus.clone().map(|cowd| {
                let tool = descriptor.tool_id.clone();
                Arc::new(move |request: &harness_contract::policy::ApprovalRequest| {
                    cowd.emit(crate::cowd_event::CowdEvent::ExecutionPhase {
                        status: harness_contract::projection::ExecutionLiveStatus::WaitingApproval,
                        detail: Some(tool.clone()),
                    });
                    cowd.emit(crate::cowd_event::CowdEvent::ApprovalRequested {
                        request_id: request.approval_id.clone(),
                        tool: tool.clone(),
                    });
                }) as crate::ApprovalPendingHook
            });
            let _original_approval_wait = self.control_requested.begin_approval_wait();
            let approval_result = coordinator
                .resolve_tool(
                    source,
                    context,
                    Some(execution_policy.autonomy_profile),
                    &effective.descriptor,
                    input,
                    self.cancellation.clone(),
                    Some(self.session_input_stream.input_notifier()),
                    pending_hook,
                    Duration::from_secs(timeout_secs.max(1)),
                )
                .await;
            emit_approval_resolution_event(self.assessment.cowd_bus(), coordinator.queue(), &approval_result);
            match approval_result {
                Ok(crate::ApprovalResolution::Approved { grant, .. }) => grant,
                Ok(crate::ApprovalResolution::Denied {
                    reason,
                    approval_id,
                })
                | Ok(crate::ApprovalResolution::Cancelled {
                    reason,
                    approval_id,
                }) => {
                    let denied = denied_capability_assessment(assessment, &reason, &approval_id);
                    let assessment_event = self.assessment.record_capability_assessment_for_call(&denied, binding, Some(&request.context));
                    if binding.is_some() && assessment_event.is_none() { return Err(RuntimeError::new("bound capability assessment append failed")); }
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                        assessment_event,
                    });
                }
                Ok(crate::ApprovalResolution::ControlRequested {
                    reason,
                    approval_id,
                }) => {
                    self.control_requested.signal();
                    let denied = denied_capability_assessment(assessment, &reason, &approval_id);
                    let assessment_event = self.assessment.record_capability_assessment_for_call(&denied, binding, Some(&request.context));
                    if binding.is_some() && assessment_event.is_none() { return Err(RuntimeError::new("bound capability assessment append failed")); }
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                        assessment_event,
                    });
                }
                Err(error) => {
                    let denied = denied_capability_assessment(
                        assessment,
                        &error,
                        &format!("tool-approval:{idempotency_key}"),
                    );
                    let assessment_event = self.assessment.record_capability_assessment_for_call(&denied, binding, Some(&request.context));
                    if binding.is_some() && assessment_event.is_none() { return Err(RuntimeError::new("bound capability assessment append failed")); }
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                        assessment_event,
                    });
                }
            }
        };

        if self.cancellation.is_cancelled() || self.control_requested.load(std::sync::atomic::Ordering::Acquire) {
            return Err(RuntimeError::new("original policy owner changed during approval"));
        }
        let current_revision = self.identity.permission_policy.execution_policy_control().revision();
        if current_revision != execution_policy.revision {
            let denied = denied_capability_assessment(
                assessment,
                "session policy changed while approval was pending; replan the effect",
                &approved_grant.grant_id,
            );
            let assessment_event = self.assessment.record_capability_assessment_for_call(&denied, binding, Some(&request.context));
            if binding.is_some() && assessment_event.is_none() { return Err(RuntimeError::new("bound capability assessment append failed")); }
            return Ok(ToolAuthorizationDecision::Gap {
                assessment: denied,
                effective,
                assessment_event,
            });
        }
        let bound_policy = self.identity.permission_policy.bound_to_snapshot(&execution_policy);
        let approved = self.assessment.authorization_negotiator.approve_effective(
            &bound_policy,
            &request,
            &effective,
            &approved_grant,
        );
        let assessment_event = self.assessment.record_capability_assessment_for_call(&approved, binding, Some(&request.context));
        if binding.is_some() && assessment_event.is_none() { return Err(RuntimeError::new("bound capability assessment append failed")); }
        let Some(lease) = approved.lease.clone() else {
            return Ok(ToolAuthorizationDecision::Gap {
                assessment: approved,
                effective,
                assessment_event,
            });
        };
        let authorized = crate::ToolPolicy
            .authorize(&effective, &approved, idempotency_key, lease, timeout_secs)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if approved_grant.scope == harness_contract::policy::ApprovalGrantScope::Once {
            let event = coordinator.queue()
                .consume_once_grant_exclusive(&approved_grant.grant_id)
                .map_err(RuntimeError::new)?;
            capture_member_event(event);
        }
        Ok(ToolAuthorizationDecision::Authorized(authorized))
    }

    fn assess_tool_authorization_at(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        timeout_secs: u64,
        execution_policy: &harness_contract::policy::SessionExecutionPolicy,
        binding: Option<&serde_json::Value>,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        let request = self.identity.authorization_request(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context,
            execution_policy,
        );
        let bound_policy = self.identity.permission_policy.bound_to_snapshot(execution_policy);
        let evaluated = self
            .assessment.authorization_negotiator
            .assess_effective(&bound_policy, &request);
        let assessment = evaluated.assessment;
        if let Some(lease) = assessment.lease.clone() {
            let event = self.assessment.record_capability_assessment_for_call(&assessment, binding, Some(&request.context));
            if binding.is_some() && event.is_none() {
                return Err(RuntimeError::new("bound capability assessment append failed"));
            }
            return crate::ToolPolicy
                .authorize(
                    &evaluated.effective,
                    &assessment,
                    idempotency_key,
                    lease,
                    timeout_secs,
                )
                .map(ToolAuthorizationDecision::Authorized)
                .map_err(|error| RuntimeError::new(error.to_string()));
        }
        Ok(ToolAuthorizationDecision::Gap {
            assessment,
            effective: evaluated.effective,
            assessment_event: None,
        })
    }

    fn govern_capability_gap(
        &self,
        mut assessment: harness_contract::policy::CapabilityAssessment,
    ) -> harness_contract::policy::CapabilityAssessment {
        let Some(fingerprint) = assessment
            .gap
            .as_ref()
            .filter(|gap| gap.recoverable)
            .map(|gap| gap.fingerprint.clone())
        else {
            return assessment;
        };
        let Some(context) = self.identity.execution.clone()
        else {
            close_controlled_recovery_gap(
                &mut assessment,
                "controlled recovery requires an exact durable turn identity",
                "authorization.recovery_missing_turn_identity",
            );
            return assessment;
        };
        let recovery_scope = format!("turn:{}", context.turn_id);
        if !self
            .assessment.authorization_negotiator
            .claim_controlled_recovery(&assessment, &recovery_scope)
        {
            close_controlled_recovery_gap(
                &mut assessment,
                "the same capability gap already consumed its single controlled recovery",
                "authorization.recovery_circuit_open",
            );
            return assessment;
        }
        let claim = crate::authorization_negotiator::ControlledRecoveryClaimRecord {
            fingerprint: fingerprint.clone(),
            recovery_scope,
            session_id: context.session_id,
            turn_id: context.turn_id,
            execution_id: context.execution_id,
            capability: assessment.capability.clone(),
        };
        let persist_result = self
            .assessment.runtime_event_store
            .as_ref()
            .ok_or_else(|| {
                "controlled recovery requires the durable Runtime event store".to_string()
            })
            .and_then(|store| {
                crate::authorization_negotiator::persist_controlled_recovery_claim(store, &claim)
            });
        if let Err(error) = persist_result {
            self.assessment.authorization_negotiator
                .rollback_unpersisted_controlled_recovery_claim(&fingerprint);
            close_controlled_recovery_gap(
                &mut assessment,
                &format!("controlled recovery claim was not durably recorded: {error}"),
                "authorization.recovery_persistence_failed",
            );
        } else {
            assessment.evidence_refs.push(format!(
                "authorization.controlled_recovery_claim:{fingerprint}"
            ));
        }
        assessment
    }
}

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    #[must_use]
    pub fn new(
        session: Session,
        api_client: C,
        tool_executor: T,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
    ) -> Self {
        Self::new_with_features(
            session,
            api_client,
            Arc::new(tool_executor),
            permission_policy,
            system_prompt,
            &RuntimeFeatureConfig::default(),
        )
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_with_features(
        session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
    ) -> Self {
        Self::new_with_features_and_memory_composition(
            session,
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
            feature_config,
            MemoryManagerComposition::Automatic,
        )
    }

    /// Construct a conversation from the Memory owner already selected by the
    /// embedding host. `None` is an explicit unavailable selection and never
    /// falls back to the standalone SQLite constructor.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn new_with_features_and_selected_memory(
        session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
        memory_manager: Option<Arc<CognitiveContextManager>>,
    ) -> Self {
        Self::new_with_features_and_memory_composition(
            session,
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
            feature_config,
            MemoryManagerComposition::HostSelected(memory_manager),
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn new_with_features_and_memory_composition(
        mut session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
        memory_composition: MemoryManagerComposition,
    ) -> Self {
        session.configure_history(feature_config.session_history());
        let usage_tracker = UsageTracker::from_session(&session);
        let permission_fingerprint = model_protocol::fingerprint::stable_hash_bytes(
            format!("{permission_policy:?}").as_bytes(),
        );
        let subsystem_budget_ratio_bp = feature_config.context_budget().subsystem_budget_ratio_bp;
        let initial_model = feature_config.resolved_model();
        let initial_window_resolution = initial_model.as_deref().map_or(
            provider::ModelContextWindowResolution {
                tokens: 128_000,
                source: provider::ModelContextWindowSource::Assumed,
            },
            |model| {
                provider::model_context_window_resolution(
                    model,
                    Some(feature_config.model_context_windows()),
                )
            },
        );
        let initial_model_context_window = initial_window_resolution.tokens;
        let initial_model_max_output = initial_model.as_deref().map_or(0, |model| {
            provider_output_budget_hint(
                model,
                initial_model_context_window,
                feature_config
                    .provider_resources()
                    .max_output_tokens_override(),
            )
        });
        let initial_budget_plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: initial_model_context_window,
            model_max_output_tokens: initial_model_max_output,
            subsystem_budget_ratio_bp,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
            expected_parallel_branches: 1,
            expected_verification_passes: 0,
        });
        let provider_resources = Arc::new(std::sync::RwLock::new(
            ConversationProviderResourceHandles {
                manager: None,
                config: Arc::new(std::sync::RwLock::new(
                    crate::ProviderResourceConfig::default(),
                )),
            },
        ));
        let (memory_manager, memory_status) = match memory_composition {
            MemoryManagerComposition::HostSelected(manager) => {
                let status = (feature_config.memory().enabled && manager.is_none()).then(|| {
                    "Memory system unavailable: the composition root selected no Memory owner. \
                     Runtime will not infer or open a fallback backend."
                        .to_string()
                });
                (manager, status)
            }
            MemoryManagerComposition::Automatic if feature_config.memory().enabled => {
                initialize_automatic_memory_manager(
                    feature_config,
                    &initial_budget_plan,
                    Arc::clone(&provider_resources),
                )
            }
            MemoryManagerComposition::Automatic => (None, None),
        };
        let session_id = session.session_id.clone();
        let session = Arc::new(RwLock::new(session));
        let mut runtime_control_policy = feature_config.runtime_control().policy.clone();
        apply_runtime_budget_to_control_policy(&mut runtime_control_policy, &initial_budget_plan);
        Self {
            session_id: session_id.clone(),
            session,
            session_input_stream: crate::session_input::SessionInputStream::new(session_id),
            original_session_claim: None,
            original_policy_input: None,
            original_policy_control_requested: Arc::new(crate::conversation::OriginalTurnInputControl::default()),
            original_policy_control_parent_owned: false,
            consumed_session_inputs: std::sync::Mutex::new(Vec::new()),
            api_client,
            tool_executor,
            permission_policy,
            permission_fingerprint,
            system_prompt,
            usage_tracker,
            cache_dimensions_observation: Arc::new(std::sync::Mutex::new(
                TurnCacheDimensionsObservation::default(),
            )),
            hook_runner: HookRunner::from_feature_config(feature_config),
            cowd_bus: None,
            turn_callback: None,
            profiler: crate::context_profiler::ContextProfiler::new(),
            subsystem_budget_ratio_bp,
            session_compaction_config: feature_config.compression().session.clone(),
            semantic_checkpoint_enabled: feature_config
                .memory()
                .runtime
                .semantic_checkpoint_enabled,
            model_context_window: initial_model_context_window,
            model_context_window_source: initial_window_resolution.source,
            model_context_windows: feature_config.model_context_windows().clone(),
            provider_max_output_override: feature_config
                .provider_resources()
                .max_output_tokens_override(),
            evaluation_provider_token_lease: None,
            delegated_provider_budget: None,
            calibrated_model_context_windows: std::sync::Mutex::new(BTreeMap::new()),
            hook_abort_signal: HookAbortSignal::default(),
            hook_progress_reporter: Arc::new(std::sync::Mutex::new(None)),
            session_tracer: None,
            memory_manager,
            checkpoint_workspace_id: "runtime-workspace".to_string(),
            execution_identity: None,
            maintenance_supervisor: None,
            memory_status,
            reality_recall: None,
            knowledge_activation: None,
            last_reality_recall_report: std::sync::Mutex::new(None),
            degraded_context_sources: std::sync::Mutex::new(Vec::new()),
            tool_callback: None,
            session_journal_port: None,
            session_history_reader: None,
            hot_state: None,
            session_context_projection_cache: std::sync::Mutex::new(None),
            session_memory_projection: tokio::sync::Mutex::new(SessionMemoryProjection::default()),
            memory_context_revision: AtomicU64::new(0),
            current_context_cache_hit: AtomicBool::new(false),
            current_context_source_latency_ms: std::sync::Mutex::new(BTreeMap::new()),
            artifact_store: None,
            runtime_event_store: None,
            outcome_service: None,
            outcome_projector: None,
            routing_mode: feature_config.routing_mode(),
            runtime_config_revision: format!(
                "{:016x}",
                model_protocol::fingerprint::stable_hash_bytes(
                    format!("{feature_config:?}").as_bytes()
                )
            ),
            active_provider_identity: std::sync::Mutex::new(None),
            provider_selection_receipt: std::sync::Mutex::new(None),
            event_log: None,
            tool_output_sandbox: memory::ToolOutputSandbox::new()
                .map(|sandbox| Arc::new(std::sync::Mutex::new(sandbox)))
                .map_err(|error| {
                    tracing::warn!(%error, "tool output sandbox unavailable");
                    error
                })
                .ok(),
            sse_callback: None,
            memory_callback: None,
            approval_coordinator: None,
            skill_profiles: Vec::new(),
            agent_skill_profile: AgentSkillProfile::default(),
            skill_task_objective: None,
            skill_instruction_source: None,
            skill_binding_pins: Vec::new(),
            skill_binding_tool_refs: Vec::new(),
            skill_instruction_pins: std::sync::Mutex::new(BTreeMap::new()),
            memory_source_uses: Arc::new(std::sync::Mutex::new((String::new(), Some(Vec::new())))),
            memory_agent_id: "primary".to_string(),
            memory_definition_lineage_id: None,
            memory_team_id: None,
            memory_read_scopes: vec![
                harness_contract::agent::CognitiveReadScope::Session,
                harness_contract::agent::CognitiveReadScope::Team,
                harness_contract::agent::CognitiveReadScope::WorkspaceKnowledge,
                harness_contract::agent::CognitiveReadScope::Project,
                harness_contract::agent::CognitiveReadScope::DefinitionLineage,
            ],
            project_phase: "Discovery".to_string(),
            model: initial_model,
            fallbacks: Arc::new(std::sync::RwLock::new(feature_config.fallbacks().to_vec())),
            cancellation_token: CancellationToken::new(),
            turn_cancellation_control: None,
            last_context_envelope: std::sync::Mutex::new(None),
            context_profile: std::sync::Mutex::new(ContextProfile::MainTurn),
            runtime_control_policy,
            external_context_items: std::sync::Mutex::new(Vec::new()),
            immutable_user_prefix: std::sync::Mutex::new(Vec::new()),
            cache_cohort_user_prefix: std::sync::Mutex::new(Vec::new()),
            next_model_context_items: std::sync::Mutex::new(Vec::new()),
            next_model_text_only: AtomicBool::new(false),
            next_model_tool_allowlist: std::sync::Mutex::new(None),
            next_model_tool_required: AtomicBool::new(false),
            next_model_required_tool_name: std::sync::Mutex::new(None),
            next_model_tool_activation_notice: std::sync::Mutex::new(None),
            next_model_reasoning_effort: std::sync::Mutex::new(None),
            tool_trace_context_items: std::sync::Mutex::new(Vec::new()),
            turn_tool_observations: std::sync::Mutex::new(Vec::new()),
            turn_governed_tool_plans: std::sync::Mutex::new(Vec::new()),
            active_turn_strategy: std::sync::Mutex::new(None),
            tool_exposure_state: std::sync::Mutex::new(None),
            turn_tool_exposure_metrics: std::sync::Mutex::new(TurnProviderState::default()),
            active_skill_tool_refs: std::sync::Mutex::new(BTreeSet::new()),
            skill_exposed_tool_ids: std::sync::Mutex::new(BTreeSet::new()),
            tool_exposure_revision: AtomicU64::new(0),
            tool_exposure_digest: AtomicU64::new(0),
            request_compiler: crate::PreparedRequestCompiler::new(
                feature_config.session_history().request_cache_entries,
            ),
            turn_stable_prefix_metrics: std::sync::Mutex::new(TurnStablePrefixMetrics::default()),
            turn_evidence_audits: std::sync::Mutex::new(Vec::new()),
            turn_generated_model_receipts: std::sync::Mutex::new(Vec::new()),
            turn_native_tool_sources: std::sync::Mutex::new(BTreeMap::new()),
            turn_original_root_calls: std::sync::Mutex::new(BTreeMap::new()),
            turn_native_root_owners: std::sync::Mutex::new(BTreeMap::new()),
            native_wire_receipts: Arc::new(std::sync::Mutex::new(Vec::new())),
            turn_model_observations: std::sync::Mutex::new(Vec::new()),
            turn_early_tool_receipts: std::sync::Mutex::new(Vec::new()),
            turn_context_ledger: std::sync::Mutex::new(crate::context_ledger::ContextLedger::new(
                initial_budget_plan.subsystem_budget_tokens,
                initial_budget_plan.tool_result_budget.max_total_tokens as u64,
            )),
            last_context_turn_report: std::sync::Mutex::new(None),
            turn_preflight_compaction: std::sync::Mutex::new(None),
            turn_knowledge_report: std::sync::Mutex::new(None),
            tool_execution_plane: Arc::new(crate::ToolExecutionPlane::new(
                Arc::new(crate::execution_core::graph::ExecutionResourceManager::new(
                    [
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Tool,
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 4,
                                target: 64,
                                maximum: 256,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.process".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 2,
                                target: 16,
                                maximum: 64,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.network".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 2,
                                target: 32,
                                maximum:
                                    crate::governed_tool_plan::default_parallel_tool_concurrency(),
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.cpu".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 2,
                                target: 64,
                                maximum: 256,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.memory_mib".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 64,
                                target: 2_048,
                                maximum: 16_384,
                            },
                        ),
                    ],
                )),
                Arc::new(crate::execution_core::graph::ScopeLockManager::new()),
            )),
            authorization_negotiator: crate::AuthorizationNegotiator::new(),
            provider_resources,
            execution_service_class:
                crate::execution_core::graph::ExecutionServiceClass::Interactive,
            tool_timeout: Some(Duration::from_secs(120)),
            explicit_team_escalation: true,
            model_step_limit_override: AtomicUsize::new(0),
            delegated_focus_novelty_target_bp: AtomicU64::new(0),
            delegated_focus_acceptance_scopes: std::sync::Mutex::new(Vec::new()),
            delegated_focus_required_output_fields: std::sync::Mutex::new(Vec::new()),
            session_execution_fence: None,
        }
    }

    #[must_use]
    pub fn with_session_execution_fence(mut self, fence: crate::SessionExecutionFence) -> Self {
        self.session_execution_fence = Some(fence);
        self
    }

    pub(crate) fn set_session_execution_fence(&mut self, fence: crate::SessionExecutionFence) {
        self.session_execution_fence = Some(fence);
    }

    /// Verify the original Session claim and retain the typed local refusal.
    pub(crate) async fn verify_session_execution_fence_for_attempt(
        &self,
        phase: crate::SessionExecutionFencePhase,
        member_execution_id: &str,
    ) -> Result<(), RuntimeError> {
        match self.session_execution_fence.as_ref() {
            Some(fence) => fence
                .verify_attempt(phase, member_execution_id)
                .await
                .map(|_| ())
                .map_err(RuntimeError::from_session_execution_fence),
            None => Ok(()),
        }
    }

    pub(crate) async fn verify_session_execution_fence(
        &self,
        phase: crate::SessionExecutionFencePhase,
    ) -> Result<(), RuntimeError> {
        match self.session_execution_fence.as_ref() {
            Some(fence) => fence
                .verify(phase)
                .await
                .map(|_| ())
                .map_err(RuntimeError::new),
            None => Ok(()),
        }
    }

    pub(crate) async fn capture_session_execution_fence(
        &self,
        phase: crate::SessionExecutionFencePhase,
    ) -> Result<Option<crate::SessionExecutionFenceSnapshot>, RuntimeError> {
        match self.session_execution_fence.as_ref() {
            Some(fence) => fence
                .verify(phase)
                .await
                .map(Some)
                .map_err(RuntimeError::new),
            None => Ok(None),
        }
    }

    #[must_use]
    pub fn with_tool_timeout(mut self, timeout: Duration) -> Self {
        self.tool_timeout = Some(timeout);
        self
    }

    pub(super) fn provider_resource_handles(&self) -> ConversationProviderResourceHandles {
        self.provider_resources
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn with_provider_admission(
        self,
        manager: Arc<crate::execution_core::graph::ExecutionResourceManager>,
    ) -> Self {
        self.provider_resources
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .manager = Some(manager);
        self
    }

    #[must_use]
    pub fn with_provider_resource_config(
        self,
        config: Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
    ) -> Self {
        self.provider_resources
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .config = config;
        self
    }

    #[must_use]
    pub(crate) fn with_evaluation_provider_token_lease(
        mut self,
        lease: Arc<EvaluationProviderTokenLease>,
    ) -> Self {
        self.set_in_place_evaluation_provider_token_lease(lease);
        self
    }

    pub(crate) fn set_in_place_evaluation_provider_token_lease(
        &mut self,
        lease: Arc<EvaluationProviderTokenLease>,
    )  {
        self.evaluation_provider_token_lease = Some(lease);
    }

    #[cfg(test)]
    pub(crate) fn uses_evaluation_provider_token_lease(
        &self,
        lease: &Arc<EvaluationProviderTokenLease>,
    ) -> bool {
        self.evaluation_provider_token_lease
            .as_ref()
            .is_some_and(|bound| Arc::ptr_eq(bound, lease))
    }

    /// Install the immutable child share of a durable parent execution
    /// budget. Missing Runtime persistence fails closed rather than silently
    /// falling back to a process-local counter.
    pub(crate) fn set_delegated_provider_budget(
        &mut self,
        reservation: harness_contract::context::ChildExecutionBudgetReservation,
    ) -> Result<(), RuntimeError> {
        reservation.validate().map_err(RuntimeError::new)?;
        let store = self.runtime_event_store.clone().ok_or_else(|| {
            RuntimeError::new("delegated provider budget requires Runtime event persistence")
        })?;
        let ledger = crate::execution_core::budget::ParentExecutionBudgetLedger::new(
            store,
            reservation.parent_budget.clone(),
        )
        .map_err(RuntimeError::new)?;
        self.delegated_provider_budget = Some((ledger, reservation));
        Ok(())
    }

    #[must_use]
    pub(crate) fn with_provider_fallback_policy(
        mut self,
        policy: Arc<std::sync::RwLock<Vec<String>>>,
    ) -> Self {
        self.fallbacks = policy;
        self
    }

    #[must_use]
    pub fn with_execution_service_class(
        mut self,
        service_class: crate::execution_core::graph::ExecutionServiceClass,
    ) -> Self {
        self.execution_service_class = service_class;
        self
    }

    pub fn set_execution_service_class(
        &mut self,
        service_class: crate::execution_core::graph::ExecutionServiceClass,
    ) {
        self.execution_service_class = service_class;
    }

    #[must_use]
    pub fn with_tool_execution_plane(mut self, plane: Arc<crate::ToolExecutionPlane>) -> Self {
        self.set_in_place_tool_execution_plane(plane);
        self
    }

    pub(crate) fn set_in_place_tool_execution_plane(&mut self, plane: Arc<crate::ToolExecutionPlane>)  {
        self.tool_execution_plane = plane;
    }

    #[cfg(test)]
    pub(crate) fn uses_tool_execution_plane(&self, plane: &Arc<crate::ToolExecutionPlane>) -> bool {
        Arc::ptr_eq(&self.tool_execution_plane, plane)
    }

    #[cfg(test)]
    pub(crate) fn uses_artifact_store(&self, store: &Arc<crate::ArtifactStore>) -> bool {
        self.artifact_store
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, store))
    }

    #[must_use]
    pub fn with_explicit_team_escalation(mut self, enabled: bool) -> Self {
        self.explicit_team_escalation = enabled;
        self
    }

    /// Provider context capacity bound to this runtime instance. Execution
    /// safety derives its lease from this value rather than Gateway prompt
    /// classes or a fixed whole-turn iteration limit.
    #[must_use]
    pub const fn model_context_window(&self) -> u32 {
        self.model_context_window
    }

    #[must_use]
    pub(crate) fn current_model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub fn with_model_context_window(mut self, ctx_window: u32) -> Self {
        if ctx_window >= 1_024 {
            self.model_context_window = ctx_window;
            // Hosts often pass the same registry/config resolution explicitly
            // for workspace sizing. Preserve its real provenance instead of
            // falsely reporting every host value as a user override.
            self.model_context_window_source = self
                .model
                .as_deref()
                .map(|model| {
                    provider::model_context_window_resolution(
                        model,
                        Some(&self.model_context_windows),
                    )
                })
                .filter(|resolution| resolution.tokens == ctx_window)
                .map_or(
                    provider::ModelContextWindowSource::Configured,
                    |resolution| resolution.source,
                );
        }
        let plan = self.runtime_budget_plan();
        apply_runtime_budget_to_control_policy(&mut self.runtime_control_policy, &plan);
        self
    }

    pub fn set_active_model(&mut self, model: impl Into<String>) {
        let model = model.into();
        if !model.trim().is_empty() {
            // A session model switch must not inherit the previous model's
            // window. Resolve this model independently so explicit per-model
            // configuration remains authoritative across a live session.
            if self.model.as_deref() != Some(model.as_str()) {
                let resolution = provider::model_context_window_resolution(
                    &model,
                    Some(&self.model_context_windows),
                );
                self.model_context_window = resolution.tokens;
                self.model_context_window_source = resolution.source;
            }
            self.model = Some(model);
        }
    }

    /// Set a tool callback for real-time execution visualization (P0-2).
    ///
    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_tool_callback(mut self, callback: Arc<dyn ToolCallback>) -> Self {
        self.tool_callback = Some(callback);
        self
    }

    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_sse_callback(mut self, callback: Arc<dyn Fn(String) + Send + Sync>) -> Self {
        self.sse_callback = Some(callback);
        self
    }

    /// Set the SSE callback on an already-constructed runtime instance.
    pub fn set_sse_callback(&mut self, callback: Arc<dyn Fn(String) + Send + Sync>) {
        self.sse_callback = Some(callback);
    }

    /// Clear the SSE callback from this runtime instance.
    pub fn clear_sse_callback(&mut self) {
        self.sse_callback = None;
    }

    #[must_use]
    pub fn with_session_journal_port(
        mut self,
        port: Arc<dyn crate::SessionRuntimeJournalPort>,
    ) -> Self {
        self.set_in_place_session_journal_port(port);
        self
    }

    pub(crate) fn set_in_place_session_journal_port(
        &mut self,
        port: Arc<dyn crate::SessionRuntimeJournalPort>,
    )  {
        self.session_journal_port = Some(port);
        self.refresh_provider_wire_evidence_writer();
    }

    #[must_use]
    pub fn with_session_history_reader(
        mut self,
        reader: Arc<session::SessionHistoryReader>,
    ) -> Self {
        self.session_history_reader = Some(reader);
        self
    }

    #[must_use]
    pub fn with_hot_state(
        mut self,
        hot_state: Arc<crate::execution_core::hot_state::RuntimeHotStatePlane>,
    ) -> Self {
        self.hot_state = Some(hot_state);
        self
    }

    #[must_use]
    pub fn with_artifact_store(mut self, store: Arc<crate::ArtifactStore>) -> Self {
        self.set_in_place_artifact_store(store);
        self
    }

    pub(crate) fn set_in_place_artifact_store(&mut self, store: Arc<crate::ArtifactStore>)  {
        self.artifact_store = Some(store);
        self.refresh_provider_wire_evidence_writer();
    }

    pub(super) fn refresh_provider_wire_evidence_writer(&mut self) {
        let writer = self
            .artifact_store
            .as_ref()
            .zip(self.session_journal_port.as_ref())
            .map(|(artifacts, session_port)| {
                Arc::new(SessionProviderWireEvidenceWriter {
                    artifacts: Arc::clone(artifacts),
                    session_port: Arc::clone(session_port),
                    cache_dimensions: Arc::clone(&self.cache_dimensions_observation),
                    native_wire_receipts: Arc::clone(&self.native_wire_receipts),
                }) as Arc<dyn crate::ProviderWireEvidenceWriter>
            });
        self.api_client.configure_provider_wire_evidence(writer);
    }

    /// Attach the durable store that owns tool, graph, agent, and task execution state.
    #[must_use]
    pub(crate) fn with_runtime_event_store(mut self, store: Arc<RuntimeEventStore>) -> Self {
        self.set_in_place_runtime_event_store(store);
        self
    }

    pub(crate) fn set_in_place_runtime_event_store(&mut self, store: Arc<RuntimeEventStore>)  {
        self.outcome_service = Some(Arc::new(crate::execution_core::OutcomeService::new(
            Arc::clone(&store),
        )));
        self.outcome_projector = Some(Arc::new(crate::OutcomeProjector::new(Arc::clone(&store))));
        self.runtime_event_store = Some(store);
    }

    #[must_use]
    pub(crate) fn with_outcome_runtime(
        mut self,
        service: Arc<crate::execution_core::OutcomeService>,
        projector: Arc<crate::OutcomeProjector>,
    ) -> Self {
        self.set_in_place_outcome_runtime(service, projector);
        self
    }

    pub(crate) fn set_in_place_outcome_runtime(
        &mut self,
        service: Arc<crate::execution_core::OutcomeService>,
        projector: Arc<crate::OutcomeProjector>,
    )  {
        self.outcome_service = Some(service);
        self.outcome_projector = Some(projector);
    }

    /// Attach a [`SessionEventLog`] for time-travel debugging and session rebuild.
    #[must_use]
    pub fn with_event_log(mut self, log: SessionEventLog) -> Self {
        self.event_log = Some(std::sync::Mutex::new(log));
        self
    }

    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_memory_callback(mut self, callback: Arc<dyn MemoryCallback>) -> Self {
        self.memory_callback = Some(callback);
        self
    }

    pub fn set_memory_callback(&mut self, callback: Arc<dyn MemoryCallback>) {
        self.memory_callback = Some(callback);
    }

    /// Install the Runtime-owned approval coordinator.
    #[must_use]
    pub fn with_approval_coordinator(
        mut self,
        coordinator: Arc<crate::ApprovalCoordinator>,
    ) -> Self {
        self.approval_coordinator = Some(coordinator);
        self
    }

    /// Provide Skill capability profiles already inspected by the Skill asset
    /// layer. Runtime consumes these profiles during activation, but does not
    /// inspect packages or own the registry.
    #[must_use]
    pub fn with_skill_profiles(mut self, profiles: Vec<SkillCapabilityProfile>) -> Self {
        self.skill_profiles = profiles;
        self
    }

    /// Configure the agent-scoped Skill visibility and adapter ceiling used by
    /// runtime activation.
    #[must_use]
    pub fn with_agent_skill_profile(mut self, profile: AgentSkillProfile) -> Self {
        self.agent_skill_profile = profile;
        self
    }

    /// Attach the Gateway-owned lazy instruction source pinned to this
    /// Runtime catalog generation.
    #[must_use]
    pub fn with_skill_instruction_source(
        mut self,
        source: Option<Arc<dyn crate::RuntimeSkillInstructionSource>>,
    ) -> Self {
        self.skill_instruction_source = source;
        self
    }

    #[must_use]
    pub(crate) fn with_skill_binding_tool_refs(mut self, refs: Vec<String>) -> Self {
        self.skill_binding_tool_refs = refs;
        self
    }

    pub(crate) fn with_skill_binding_pins(
        mut self,
        pins: Vec<harness_contract::skill::SkillInstructionPin>,
    ) -> Self {
        self.skill_binding_pins = pins;
        self
    }

    /// Bind the Runtime's immutable Agent instance identity to memory
    /// operations for this conversation. This is set only by Runtime-owned
    /// child execution, never by a Surface request field.
    #[must_use]
    pub fn with_memory_identity(
        mut self,
        agent_id: impl Into<String>,
        definition_lineage_id: Option<String>,
        team_id: Option<String>,
        read_scopes: Vec<harness_contract::agent::CognitiveReadScope>,
    ) -> Self {
        self.memory_agent_id = agent_id.into();
        self.memory_definition_lineage_id = definition_lineage_id;
        self.memory_team_id = team_id;
        self.memory_read_scopes = read_scopes;
        self
    }

    /// Bind semantic checkpoints to the same canonical execution identity as
    /// the active Agent node. Root surface turns provide only the workspace
    /// basis and receive a session-turn identity when compaction is planned.
    #[must_use]
    pub fn with_checkpoint_identity(
        mut self,
        workspace_id: impl Into<String>,
        execution_identity: Option<harness_contract::execution::ExecutionIdentity>,
    ) -> Self {
        self.checkpoint_workspace_id = workspace_id.into();
        self.execution_identity = execution_identity;
        self
    }

    #[must_use]
    pub fn with_runtime_control_policy(mut self, policy: RuntimeControlPolicy) -> Self {
        self.runtime_control_policy = policy;
        self
    }

    /// T35: Set a cancellation token for graceful shutdown.
    #[must_use]
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.set_in_place_cancellation_token(token);
        self
    }

    pub(crate) fn set_in_place_cancellation_token(&mut self, token: CancellationToken)  {
        self.hook_abort_signal = self.hook_abort_signal.clone().with_cancellation(token.clone());
        self.cancellation_token = token;
    }

    #[must_use]
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// True only when the turn itself was cancelled. Turn-level cancellation
    /// state lives in the shared `TurnCancellationControl`; every instance
    /// holding that Arc answers identically, so this never depends on which
    /// runtime instance the caller runs in. A step-scoped provider stop is not
    /// a turn cancellation and must never reach this predicate.
    pub(crate) fn turn_cancelled(&self) -> Result<bool, String> {
        self.turn_cancellation_control.as_ref().map(|control| control.is_cancelled())
            .ok_or_else(|| "turn cancellation control is missing".to_string())
    }

    pub(crate) fn set_original_session_claim(&mut self, claim: Option<Arc<crate::execution_core::services::OriginalSessionClaim>>) {
        self.original_session_claim = claim;
    }

    pub(crate) fn original_policy_input_context(&mut self, turn_id: &str) -> (crate::session_input::SessionInputStream, Arc<crate::conversation::OriginalTurnInputControl>) {
        // A later ingress must not inherit a late control signal from a child
        // still holding the previous turn's original context.
        self.original_policy_input = None;
        self.original_policy_control_parent_owned = false;
        self.original_policy_control_requested = Arc::new(crate::conversation::OriginalTurnInputControl::default());
        self.session_input_stream.install_original_turn_control(TurnId::from_string(turn_id), &self.original_policy_control_requested);
        (self.session_input_stream.clone(), Arc::clone(&self.original_policy_control_requested))
    }
    pub(crate) fn install_original_policy_input_context(&mut self, input: crate::session_input::SessionInputStream, control: Arc<crate::conversation::OriginalTurnInputControl>) {
        self.original_policy_input = Some(input);
        self.original_policy_control_requested = control;
        self.original_policy_control_parent_owned = true;
    }
    pub(crate) fn original_policy_control_is_parent_owned(&self) -> bool { self.original_policy_control_parent_owned }

    pub(crate) fn original_turn_control(&self) -> Result<Arc<crate::execution_core::services::TurnCancellationControl>, String> {
        self.turn_cancellation_control.clone().ok_or_else(|| "original Session turn control is missing".to_string())
    }

    /// The turn-level token for callers that must inspect it after an await
    /// without holding the runtime lock.
    pub(crate) fn turn_cancellation_token(&self) -> Result<CancellationToken, String> {
        self.turn_cancellation_control.as_ref().map(|control| control.token())
            .ok_or_else(|| "turn cancellation control is missing".to_string())
    }

    pub(super) fn governed_workspace_root(&self) -> Result<PathBuf, RuntimeError> {
        if let Some(root) = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf))
        {
            return Ok(root);
        }
        #[cfg(test)]
        {
            return std::env::current_dir().map_err(|error| {
                RuntimeError::new(format!("test workspace unavailable: {error}"))
            });
        }
        #[cfg(not(test))]
        {
            Err(RuntimeError::new(
                "governed Runtime execution requires an explicit Session workspace",
            ))
        }
    }

    /// Attach a CowdEventBus for domain event emission.
    #[must_use]
    pub fn with_cowd_event_bus(mut self, bus: crate::cowd_event::CowdEventBus) -> Self {
        self.set_in_place_cowd_event_bus(bus);
        self
    }

    pub(crate) fn set_in_place_cowd_event_bus(&mut self, bus: crate::cowd_event::CowdEventBus)  {
        self.cowd_bus = Some(Arc::new(bus.clone()));
    }

    /// Get a reference to the attached CowdEventBus, if any.
    pub fn cowd_bus(&self) -> Option<&crate::cowd_event::CowdEventBus> {
        self.cowd_bus.as_deref()
    }

    pub fn admit_session_input(
        &self,
        envelope: SessionInputEnvelope,
        state: crate::input_classifier::RuntimeInputState,
    ) -> SessionInputReceipt {
        let mut state = state;
        if state.active_turn_id.is_none() {
            state.active_turn_id = self.session_input_stream.active_turn_id();
        }
        let receipt = self.session_input_stream.admit(envelope, state);
        self.emit_session_input_projection(Some(receipt.clone()));
        receipt
    }

    #[must_use]
    pub fn session_input_projection(&self) -> SessionInputProjection {
        self.session_input_stream.projection()
    }

    #[must_use]
    pub fn active_turn_inbox(&self, turn_id: Option<TurnId>) -> TurnInboxSnapshot {
        self.session_input_stream.inbox_snapshot(turn_id)
    }

    #[must_use]
    pub fn session_input_stream(&self) -> crate::session_input::SessionInputStream {
        self.session_input_stream.clone()
    }

    pub(super) fn emit_session_input_projection(&self, receipt: Option<SessionInputReceipt>) {
        if let Some(ref cowd) = self.cowd_bus {
            if let Some(receipt) = receipt {
                cowd.emit(crate::cowd_event::CowdEvent::SessionInputReceived { receipt });
            }
            cowd.emit(crate::cowd_event::CowdEvent::SessionInputProjection {
                projection: self.session_input_stream.projection(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::TurnInboxUpdated {
                inbox: self.session_input_stream.inbox_snapshot(None),
            });
        }
    }

    pub(super) fn consume_runtime_input_records(
        &self,
        turn_id: &TurnId,
        checkpoint: TurnInputCheckpoint,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let consumed = self
            .session_input_stream
            .consume_for_checkpoint(turn_id, checkpoint, 32);
        if !consumed.is_empty() {
            if let Ok(mut pending) = self.consumed_session_inputs.lock() {
                pending.extend(consumed.iter().cloned());
            }
        }
        if let Some(ref cowd) = self.cowd_bus {
            if !consumed.is_empty() {
                cowd.emit(crate::cowd_event::CowdEvent::TurnInputCheckpointConsumed {
                    checkpoint,
                    consumed: consumed
                        .iter()
                        .map(crate::session_input::SessionInputRecord::to_inbox_item)
                        .collect(),
                });
            }
            cowd.emit(crate::cowd_event::CowdEvent::SessionInputProjection {
                projection: self.session_input_stream.projection(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::TurnInboxUpdated {
                inbox: self
                    .session_input_stream
                    .inbox_snapshot(Some(turn_id.clone())),
            });
        }
        consumed
    }

    pub(super) fn consume_runtime_inputs_at_checkpoint(
        &self,
        turn_id: &TurnId,
        checkpoint: TurnInputCheckpoint,
        prompt: &mut PromptAssembly,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let consumed = self.consume_runtime_input_records(turn_id, checkpoint);
        if let Some(guidance) = crate::turn_inbox::checkpoint_guidance(checkpoint, &consumed) {
            prompt.push_runtime_context(guidance);
        }
        for item in crate::turn_inbox::checkpoint_context_items(checkpoint, &consumed) {
            prompt.push_context_item(&item);
        }
        consumed
    }

    /// Consume active-turn input after a Provider/tool boundary and place its
    /// typed context on the next Provider request.
    pub(crate) fn consume_active_runtime_inputs_for_next_step(
        &self,
        checkpoint: TurnInputCheckpoint,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let Some(turn_id) = self.session_input_stream.active_turn_id() else {
            return Vec::new();
        };
        let consumed = self.consume_runtime_input_records(&turn_id, checkpoint);
        if let Some(guidance) = crate::turn_inbox::checkpoint_guidance(checkpoint, &consumed) {
            let mut item = ContextItem::new(
                format!("turn-input-guidance:{}", checkpoint.as_str()),
                ContextSourceKind::Task,
                ContextRole::Instruction,
                guidance,
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            self.push_next_model_context_item(item);
        }
        for item in crate::turn_inbox::checkpoint_context_items(checkpoint, &consumed) {
            self.push_next_model_context_item(item);
        }
        consumed
    }

    #[must_use]
    pub(crate) fn consumed_session_input_cursor(
        &self,
    ) -> Option<harness_contract::turn::SessionInputCursor> {
        self.session_input_stream
            .active_turn_id()
            .as_ref()
            .and_then(|turn_id| self.session_input_stream.highest_consumed_cursor(turn_id))
    }

    /// Drain compact receipts for inputs consumed during the current provider
    /// step. This does not mutate routing or create tasks; the graph host is
    /// the only caller allowed to decide whether a correction revises a Goal.
    pub fn take_consumed_session_inputs(&self) -> Vec<crate::session_input::SessionInputRecord> {
        self.consumed_session_inputs
            .lock()
            .map_or_else(|_| Vec::new(), |mut pending| std::mem::take(&mut *pending))
    }

    /// P1-05: Register a TurnCallback for generator-style injection after tool results.
    #[must_use]
    pub fn with_turn_callback(mut self, cb: TurnCallback) -> Self {
        self.turn_callback = Some(Arc::new(cb));
        self
    }

    pub(crate) fn hook_execution_scope(&self) -> crate::HookExecutionScope {
        self.hook_runner.execution_scope()
    }

    pub(crate) fn set_hook_execution_scope(&mut self, scope: crate::HookExecutionScope) {
        self.hook_runner.set_execution_scope(scope);
    }

    #[must_use]
    pub fn with_hook_abort_signal(mut self, hook_abort_signal: HookAbortSignal) -> Self {
        self.set_in_place_hook_abort_signal(hook_abort_signal);
        self
    }

    pub(crate) fn set_in_place_hook_abort_signal(&mut self, hook_abort_signal: HookAbortSignal)  {
        self.hook_abort_signal =
            hook_abort_signal.with_cancellation(self.cancellation_token.clone());
    }

    #[must_use]
    pub fn with_hook_progress_reporter(
        self,
        hook_progress_reporter: Box<dyn HookProgressReporter + Send>,
    ) -> Self {
        *self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(hook_progress_reporter);
        self
    }

    #[must_use]
    pub fn with_session_tracer(mut self, session_tracer: SessionTracer) -> Self {
        self.session_tracer = Some(session_tracer);
        self
    }

    /// Override the memory manager with a pre-constructed instance.
    ///
    /// This is primarily useful in tests or when the caller wants full control
    /// over the [`CognitiveContextManager`] lifecycle.
    #[must_use]
    pub fn with_memory_manager(mut self, manager: Arc<CognitiveContextManager>) -> Self {
        self.memory_manager = Some(manager);
        self
    }

    #[must_use]
    pub(crate) fn with_maintenance_supervisor(
        mut self,
        supervisor: Arc<crate::execution_core::services::RuntimeMaintenanceSupervisor>,
    ) -> Self {
        self.set_in_place_maintenance_supervisor(supervisor);
        self
    }

    pub(crate) fn set_in_place_maintenance_supervisor(
        &mut self,
        supervisor: Arc<crate::execution_core::services::RuntimeMaintenanceSupervisor>,
    )  {
        self.maintenance_supervisor = Some(supervisor);
    }

    /// Attach the Runtime-owned Fact/Matrix recall port to this conversation.
    /// The Binding is immutable for the host lifetime, so each turn evaluates
    /// the same data lease rather than re-resolving a surface default.
    #[must_use]
    pub fn with_reality_binding(
        mut self,
        port: crate::RealityRecallPort,
        binding: harness_contract::agent::AgentBindingSnapshot,
    ) -> Self {
        self.reality_recall = Some((port, binding));
        self
    }

    #[must_use]
    pub(crate) fn reality_data_lease(&self) -> Option<harness_contract::agent::AgentDataLease> {
        self.reality_recall
            .as_ref()
            .and_then(|(_, binding)| binding.validate().ok().map(|()| binding.data_lease.clone()))
    }

    #[must_use]
    pub fn with_knowledge_activation(mut self, activation: KnowledgeActivationRuntime) -> Self {
        self.knowledge_activation = Some(activation);
        self
    }

    /// Return the source-level report for the most recently assembled model
    /// context.  The report proves a lease was applied even when it selected
    /// no Fact or Matrix evidence.
    #[must_use]
    pub fn last_reality_recall_report(&self) -> Option<crate::RealityRecallReport> {
        self.last_reality_recall_report
            .lock()
            .ok()
            .and_then(|report| report.clone())
    }

    /// Explicitly disable the memory subsystem, regardless of feature config.
    #[must_use]
    pub fn without_memory(mut self) -> Self {
        self.memory_manager = None;
        self
    }

    /// Access the cognitive memory manager, if memory is enabled.
    ///
    /// Returns `None` when memory is disabled or failed to initialise.
    #[must_use]
    pub fn memory_manager(&self) -> Option<&Arc<CognitiveContextManager>> {
        self.memory_manager.as_ref()
    }

    /// Record a compact runtime event for later context and memory governance.
    pub(super) fn record_context_event(
        &mut self,
        event_type: &str,
        category: &str,
        summary: &str,
        priority: u8,
    ) {
        let project_dir = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf))
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.profiler
            .record_dedup(crate::context_profiler::SessionEvent {
                event_type: event_type.into(),
                category: category.into(),
                data_summary: summary.into(),
                priority,
                data_hash: 0, // computed by record_dedup
                timestamp,
                project_dir,
                attribution_confidence: 0.9,
            });
    }

    pub(super) fn run_pre_tool_use_hook(&self, tool_name: &str, input: &str) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_pre_tool_use_with_context(
                tool_name,
                input,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_pre_tool_use_with_context(
                tool_name,
                input,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    pub(super) fn run_post_tool_use_hook(
        &self,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_post_tool_use_with_context(
                tool_name,
                input,
                output,
                is_error,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_post_tool_use_with_context(
                tool_name,
                input,
                output,
                is_error,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    pub(super) fn run_post_tool_use_failure_hook(
        &self,
        tool_name: &str,
        input: &str,
        output: &str,
    ) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_post_tool_use_failure_with_context(
                tool_name,
                input,
                output,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_post_tool_use_failure_with_context(
                tool_name,
                input,
                output,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    /// Compact the active transcript through the sole semantic checkpoint
    /// pipeline. Both automatic preflight compaction and operator-triggered
    /// compaction use this path so a session never receives a second,
    /// timeline-only summary representation.
    pub async fn compact_active_session(
        &mut self,
    ) -> Result<Option<AutoCompactionEvent>, RuntimeError> {
        // Operator-triggered compaction shares the configured preservation and
        // checkpoint limits with request-preflight compaction. `1` makes the
        // operation explicit without introducing a second threshold policy.
        self.compact_session_with_checkpoint(self.compaction_config_for_session(1))
            .await
    }

    #[must_use]
    pub fn estimated_tokens(&self) -> usize {
        estimate_session_tokens(&self.session.blocking_read())
    }

    pub(super) fn model_candidates_for_turn(&self, _user_input: &str) -> Vec<String> {
        let primary = self
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(ToString::to_string);
        let fallback_snapshot = self
            .fallbacks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut fallback_models: Vec<String> = fallback_snapshot
            .iter()
            .map(|model| model.trim())
            .filter(|model| !model.is_empty())
            .map(ToString::to_string)
            .collect();
        fallback_models.dedup();
        if let Some(primary) = primary.as_ref() {
            fallback_models.retain(|model| model != primary);
        }

        let mut routed = Vec::with_capacity(fallback_models.len() + usize::from(primary.is_some()));
        if let Some(primary) = primary {
            routed.push(primary);
        }
        for model in fallback_models {
            if !routed.iter().any(|known| known == &model) {
                routed.push(model);
            }
        }
        if routed.is_empty() {
            // An empty model delegates selection to the configured provider. This keeps
            // embedded runtimes valid when they intentionally rely on a provider default.
            routed.push(String::new());
        }
        let strategy_segment = self
            .active_turn_strategy()
            .map(|state| (state.policy_version.clone(), state.selected_candidate));
        let receipt = if let Some(projector) = self.outcome_projector.as_ref() {
            let (selected, receipt) = crate::select_provider_from_outcome_snapshot(
                self.routing_mode,
                &routed,
                &self.runtime_config_revision,
                strategy_segment
                    .as_ref()
                    .map(|(policy_revision, _)| policy_revision.as_str()),
                strategy_segment
                    .as_ref()
                    .map(|(_, selected_candidate)| *selected_candidate),
                &projector.snapshot(),
                now_ms(),
            );
            routed = selected;
            receipt
        } else {
            crate::ProviderSelectionReceipt {
                requested_mode: self.routing_mode,
                effective_mode: crate::RoutingMode::Pinned,
                snapshot_revision: 0,
                selected_model: routed.first().cloned().unwrap_or_default(),
                fallback_reason: (self.routing_mode == crate::RoutingMode::Auto)
                    .then(|| "outcome projection is unavailable".to_string()),
                candidates: Vec::new(),
            }
        };
        *self
            .provider_selection_receipt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(receipt);
        routed
    }

    #[must_use]
    pub fn usage(&self) -> &UsageTracker {
        &self.usage_tracker
    }

    #[must_use]
    pub fn tool_executor(&self) -> &Arc<T> {
        &self.tool_executor
    }

    #[must_use]
    pub fn permission_policy(&self) -> &PermissionPolicy {
        &self.permission_policy
    }

    #[must_use]
    pub(crate) fn authorization_negotiator(&self) -> crate::AuthorizationNegotiator {
        self.authorization_negotiator.clone()
    }

    pub(super) fn authorization_request(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        execution_policy: &harness_contract::policy::SessionExecutionPolicy,
    ) -> crate::AuthorizationRequest {
        self.shared_policy_owner().identity.authorization_request(
            descriptor, input, idempotency_key, permission_context, execution_policy,
        )
    }

    pub(super) fn record_capability_assessment(
        &self,
        assessment: &harness_contract::policy::CapabilityAssessment,
    ) -> Option<BoundPolicyAssessment> {
        self.record_capability_assessment_for_call(assessment, None)
    }

    fn record_capability_assessment_for_call(
        &self,
        assessment: &harness_contract::policy::CapabilityAssessment,
        binding: Option<&serde_json::Value>,
    ) -> Option<BoundPolicyAssessment> {
        self.shared_assessment_owner().record_capability_assessment_for_call(assessment, binding, None)
    }

    fn shared_assessment_owner(&self) -> SharedAssessmentOwner {
        self.shared_policy_owner().assessment
    }

    pub(crate) fn shared_policy_owner(&self) -> SharedPolicyOwner {
        let execution = self.cowd_bus().and_then(crate::CowdEventBus::current_execution_context);
        SharedPolicyOwner {
            identity: SharedAuthorizationIdentity {
                session_id: self.session_id().to_string(),
                agent_id: self.memory_agent_id.clone(),
                execution: execution.clone(),
                permission_policy: self.permission_policy.clone(),
            },
            assessment: SharedAssessmentOwner {
                session_id: self.session_id().to_string(), execution,
                cowd_bus: self.cowd_bus.clone(),
                runtime_event_store: self.runtime_event_store.clone(),
                authorization_negotiator: self.authorization_negotiator.clone(),
            },
            approval_coordinator: self.approval_coordinator.clone(),
            cancellation: self.cancellation_token(),
            session_input_stream: self.original_policy_input.clone().unwrap_or_else(|| self.session_input_stream.clone()),
            control_requested: Arc::clone(&self.original_policy_control_requested),
            checkpoint_workspace_id: self.checkpoint_workspace_id.clone(),
            memory_team_id: self.memory_team_id.clone(),
            activity: self.cowd_bus().and_then(crate::CowdEventBus::current_activity_binding),
            session_execution_fence: self.session_execution_fence.clone(),
            original_session_claim: self.original_session_claim.clone(),
            tool_lease: self.tool_executor.pin_step_catalog(),
            hook_runner: self.hook_runner.clone(),
            hook_abort_signal: self.hook_abort_signal.clone(),
            hook_progress_reporter: self.hook_progress_reporter.clone(),
        }
    }

    pub(super) fn assess_tool_authorization_at(
        &self, descriptor: &harness_contract::tool::ToolEffectDescriptor, input: &str,
        idempotency_key: String, permission_context: PermissionContext, timeout_secs: u64,
        execution_policy: &harness_contract::policy::SessionExecutionPolicy,
        binding: Option<&serde_json::Value>,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        self.shared_policy_owner().assess_tool_authorization_at(
            descriptor, input, idempotency_key, permission_context, timeout_secs, execution_policy, binding,
        )
    }

    pub(crate) async fn negotiate_tool_authorization(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        timeout_secs: u64,
        prompter: &crate::permissions::SharedPrompter,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        self.negotiate_tool_authorization_inner(descriptor, input, idempotency_key, permission_context, timeout_secs, prompter, None).await
    }

    /// Prove that this exact original ticket/request uses the invocation
    /// prepared from its pinned Step catalog. This does not run a hook or
    /// negotiate/consume an authorization lease.
    pub(crate) fn prepare_call_effect_evidence(
        &self,
        ticket: &crate::execution_core::NodeExecutionTicket,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<PreparedCallEffect, RuntimeError> {
        crate::execution_core::graph::executors::agent_tool::validate_original_call_request(ticket, request)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if request.step_binding.is_none() {
            if request.tool_name == "agent_action_batch" || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&request.tool_name.as_str()) {
                return Err(RuntimeError::new("original action Step binding missing"));
            }
            let owner = request.tool_progress.original_governed_hook().map_err(RuntimeError::new)?
                .ok_or_else(|| RuntimeError::new("original unbound call owner missing"))?;
            let workspace_root = if let Some(lease) = owner.original_pinned_lease() {
                owner.original_prepared_lease(ticket, request).map_err(RuntimeError::new)?;
                lease.workspace_root().to_path_buf()
            } else if owner.matches_native_preparation(ticket, request) {
                self.governed_workspace_root()?
            } else {
                return Err(RuntimeError::new("original native preparation differs"));
            };
            return Ok(PreparedCallEffect { ticket: ticket.clone(), request: serde_json::to_value(request)
                .map_err(|error| RuntimeError::new(error.to_string()))?, workspace_root });
        }
        let binding = request.step_binding.as_ref().ok_or_else(|| RuntimeError::new("prepared call Step missing"))?;
        let invocation = request.compiled_invocation.as_ref().ok_or_else(|| RuntimeError::new("prepared call invocation missing"))?;
        let input = harness_contract::step::parse_strict_json(&request.input)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let step = binding.resolve(self.tool_executor.pin_step_catalog())?;
        let workspace_root = step.lease().workspace_root().to_path_buf();
        let mut derived = binding.prepare_invocation(Some(step.lease().clone()), &request.tool_name, &input)?;
        // The original plan compiler adds dependency edges after preparing the
        // pinned tool. Ticket/plan consumers validate those edges separately.
        derived.explicit_dependencies = invocation.explicit_dependencies.clone();
        derived.compiled_dependencies = invocation.compiled_dependencies.clone();
        if &derived != invocation {
            return Err(RuntimeError::new("prepared call differs from pinned Step descriptor"));
        }
        Ok(PreparedCallEffect { ticket:ticket.clone(), request:serde_json::to_value(request)
            .map_err(|error| RuntimeError::new(error.to_string()))?, workspace_root })
    }

    /// Evaluate once, and attach the original owner/call to the assessment
    /// actually written by that evaluation. The commit owner must still
    /// authenticate the binding and settle refusal against admission.
    pub(crate) async fn negotiate_tool_authorization_for_call(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        timeout_secs: u64,
        prompter: &crate::permissions::SharedPrompter,
        owner: crate::execution_core::graph::CallRefusalOwner<'_>,
        request: &crate::RuntimeToolExecutionRequest,
        prepared_effect: Option<&PreparedCallEffect>,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        use sha2::Digest as _;
        let step = request.step_binding.as_ref().ok_or_else(|| RuntimeError::new("assessment original Step missing"))?;
        let invocation = request.compiled_invocation.as_ref()
            .ok_or_else(|| RuntimeError::new("assessment original invocation missing"))?;
        let effective = harness_contract::step::parse_strict_json(input)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if request.input != input || request.session_id.as_deref() != Some(self.session_id())
            || request.tool_name != descriptor.tool_id || step.origin.call_id != request.tool_use_id
            || step.effect_idempotency_key() != request.idempotency_key
            || &invocation.effect != descriptor || effective != invocation.intent.normalized_input {
            return Err(RuntimeError::new("assessment original call or effective input mismatch"));
        }
        let (owner, original_ticket) = match owner {
            crate::execution_core::graph::CallRefusalOwner::Node(ticket) => {
                if prepared_effect.is_none_or(|proof| !proof.matches(ticket, request)) {
                    return Err(RuntimeError::new("assessment original prepared effect unavailable"));
                }
                (serde_json::json!({"kind":"node", "ticket":ticket}), (*ticket).clone())
            },
        };
        let binding = serde_json::json!({
            "version":1, "step_binding":step, "owner":owner, "request":request,
            "call_id":request.tool_use_id, "tool_name":request.tool_name,
            "effect_key":request.idempotency_key,
            "input_sha256":format!("sha256:{:x}", sha2::Sha256::digest(input.as_bytes())),
        });
        if request.tool_name == "read_ref" {
            if let Ok(Some(target_input)) = crate::agentic::action_invocation::read_ref_directory_input(&effective) {
                // The derived target is decided by the same original owner
                // under the same permission context as the outer call. A
                // default context would silently widen a restricted call.
                let target_context = permission_context.clone();
                let outer = self.negotiate_tool_authorization_inner(descriptor, input, idempotency_key.clone(),
                    permission_context, timeout_secs, prompter, Some(&binding)).await?;
                let ToolAuthorizationDecision::Authorized(ref outer_decision) = outer else { return Ok(outer); };
                let capture = match SINGLE_ORIGINAL_EVIDENCE.try_with(Arc::clone) {
                    Ok(capture) => capture,
                    Err(_) => return Ok(outer),
                };
                let bound = match step.resolve(self.tool_executor.pin_step_catalog()) {
                    Ok(bound) => bound,
                    Err(_) => return Ok(outer),
                };
                let target_text = serde_json::to_string(&target_input)
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                let validated = match bound.validate_call(&step.origin, self.session_id(), &request.tool_use_id,
                    "state_inspect", &target_text, chrono::Utc::now().timestamp_millis()) {
                    Ok(validated) => validated,
                    Err(_) => return Ok(outer),
                };
                if validated != target_input { return Ok(outer); }
                let target_effect = bound.lease().describe_effect("state_inspect", &target_input);
                if target_effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
                    || target_effect.uses_network || target_effect.spawns_process || target_effect.scopes.is_empty()
                    || target_effect.scopes.iter().any(|scope|
                        scope.resource != harness_contract::policy::PermissionResource::Tool
                            || scope.operation != harness_contract::policy::PermissionOperation::Read) {
                    return Ok(outer);
                }
                let producer = capture.owner().ok_or_else(|| RuntimeError::new("original read_ref policy producer unavailable"))?;
                let claim = producer.original_session_claim.as_ref().ok_or_else(|| RuntimeError::new("original read_ref Session claim unavailable"))?;
                if claim.session_id() != producer.identity.session_id.as_str() || producer.cancellation.is_cancelled()
                    || producer.control_requested.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(RuntimeError::new("original read_ref policy owner changed"));
                }
                claim.fence().map_err(RuntimeError::new)?;
                let fence = producer.session_execution_fence.as_ref().ok_or_else(|| RuntimeError::new("original read_ref Session fence unavailable"))?;
                fence.verify(crate::SessionExecutionFencePhase::ToolExecution).await.map_err(RuntimeError::new)?;
                let mut target_binding = binding.clone();
                target_binding["derived_target"] = serde_json::json!({"tool":"state_inspect", "input":target_input,
                    "effect":target_effect});
                let target = producer.negotiate_tool_authorization_inner(&target_effect, &target_text,
                    format!("{idempotency_key}:read-ref-state-inspect"), target_context,
                    timeout_secs, prompter, Some(&target_binding)).await?;
                if producer.cancellation.is_cancelled() || producer.control_requested.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(RuntimeError::new("original read_ref policy owner changed during target authorization"));
                }
                claim.fence().map_err(RuntimeError::new)?;
                fence.verify(crate::SessionExecutionFencePhase::ToolExecution).await.map_err(RuntimeError::new)?;
                let ToolAuthorizationDecision::Authorized(target_decision) = target else { return Ok(outer); };
                let authorization = target_decision.authorization;
                if authorization.policy_revision != request.policy_revision
                    || outer_decision.authorization.policy_revision != request.policy_revision {
                    return Err(RuntimeError::new("original read_ref target policy changed"));
                }
                bound.lease().validate_authorization(&authorization, "state_inspect", &target_input)
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                let proof = OriginalReadRefTarget { ticket: original_ticket,
                    original_channel: request.tool_progress.original_read_ref_identity(),
                    original_request: serde_json::to_value(request).map_err(|error| RuntimeError::new(error.to_string()))?,
                    input: target_input, effect: target_effect, authorization };
                let mut slot = capture.read_ref_target.lock()
                    .map_err(|_| RuntimeError::new("original read_ref target owner unavailable"))?;
                if slot.is_some() { return Err(RuntimeError::new("original read_ref target already prepared")); }
                *slot = Some(proof);
                return Ok(outer);
            }
        }
        if request.tool_name != harness_contract::agent_action::OBJECTIVE_COMPLETE_REQUEST_TOOL_ID {
            return self.negotiate_tool_authorization_inner(descriptor, input, idempotency_key,
                permission_context, timeout_secs, prompter, Some(&binding)).await;
        }
        // Preserve the exact owner events emitted by this one negotiation.
        // The fact below is written only after ToolPolicy returns its real
        // authorization and before this function can hand it to dispatch.
        let evidence = Arc::new(std::sync::Mutex::new(Vec::new()));
        let consumed = Arc::new(std::sync::Mutex::new(None));
        let request_id = idempotency_key.clone();
        let result = MEMBER_ORIGINAL_CONSUMPTION.scope(Arc::clone(&consumed),
            MEMBER_ORIGINAL_EVIDENCE.scope(Arc::clone(&evidence),
                self.negotiate_tool_authorization_inner(descriptor, input, idempotency_key,
                    permission_context, timeout_secs, prompter, Some(&binding)))).await?;
        let ToolAuthorizationDecision::Authorized(decision) = &result else { return Ok(result); };
        let store = self.runtime_event_store.as_ref()
            .ok_or_else(|| RuntimeError::new("completion original authorization store unavailable"))?;
        let authorization = &decision.authorization;
        let lease = &authorization.authorization_lease;
        let consumption = consumed.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone().ok_or_else(|| RuntimeError::new("completion original lease consume unavailable"))?;
        if authorization.request_id != request_id || lease.idempotency_key != request_id
            || !consumption.belongs_to(&self.authorization_negotiator) || consumption.lease != *lease
        { return Err(RuntimeError::new("completion original authorization owner mismatch")); }
        let stream = format!("authorization-lease:{}", lease.lease_id);
        let consume = store.event_by_idempotency_key(&stream, &consumption.transition.transition_id)
            .map_err(|error| RuntimeError::new(error.to_string()))?
            .ok_or_else(|| RuntimeError::new("completion original consume was not persisted"))?;
        let transition: harness_contract::policy::AuthorizationLeaseTransition = serde_json::from_value(consume.payload.clone())
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let receipt = store.original_transaction_receipt(&consume.transaction_id)
            .map_err(|error| RuntimeError::new(error.to_string()))?
            .ok_or_else(|| RuntimeError::new("completion original consume receipt unavailable"))?;
        if transition != consumption.transition || transition.idempotency_key.as_deref() != Some(request_id.as_str())
            || transition.kind != harness_contract::policy::AuthorizationLeaseTransitionKind::Consumed
            || consume.scope != RuntimeEventScope::Tool || consume.kind != "authorization.lease_transition"
            || consume.status.as_deref() != Some("consumed") || consume.actor.as_deref() != Some("conversation_runtime")
            || consume.stream_id != stream || consume.transaction_index != 0 || consume.schema_version != 1
            || consume.idempotency_key.as_deref() != Some(transition.transition_id.as_str())
            || consume.transaction_id != format!("authorization-transition:{}", transition.transition_id)
            || consume.refs != vec![RuntimeEventRef { kind: "authorization_lease".into(), id: lease.lease_id.clone() }]
            || receipt.transaction_id != consume.transaction_id || receipt.commit_cursor != consume.commit_cursor
            || receipt.event_ids.as_slice() != [consume.event_id.clone()] || receipt.stream_revisions.len() != 1
            || receipt.stream_revisions[0].stream_id != stream
            || receipt.stream_revisions[0].committed_revision != consume.sequence
            || receipt.stream_revisions[0].expected_revision.checked_add(1) != Some(consume.sequence)
        { return Err(RuntimeError::new("completion original consume transaction mismatch")); }
        let events = evidence.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let assessments = events.iter().filter(|event| event.kind == "authorization.capability_assessed"
            && event.payload.get("_call_refusal_binding") == Some(&binding)
            && event.payload.pointer("/lease/lease_id").and_then(serde_json::Value::as_str) == Some(lease.lease_id.as_str())
        ).collect::<Vec<_>>();
        if assessments.len() != 1 { return Err(RuntimeError::new("completion original assessment missing or ambiguous")); }
        let assessment = assessments[0];
        let assessment_receipt = store.original_transaction_receipt(&assessment.transaction_id)
            .map_err(|error| RuntimeError::new(error.to_string()))?
            .ok_or_else(|| RuntimeError::new("completion original assessment receipt unavailable"))?;
        if assessment.scope != RuntimeEventScope::Tool || assessment.actor.as_deref() != Some("conversation_runtime")
            || assessment.transaction_index != 0 || assessment.schema_version != 1
            || assessment_receipt.transaction_id != assessment.transaction_id
            || assessment_receipt.commit_cursor != assessment.commit_cursor
            || assessment_receipt.event_ids.as_slice() != [assessment.event_id.clone()]
            || assessment_receipt.stream_revisions.len() != 1
            || assessment_receipt.stream_revisions[0].stream_id != assessment.stream_id
            || assessment_receipt.stream_revisions[0].committed_revision != assessment.sequence
            || assessment_receipt.stream_revisions[0].expected_revision.checked_add(1) != Some(assessment.sequence)
        { return Err(RuntimeError::new("completion original assessment transaction mismatch")); }
        let coord = |event: &crate::DurableRuntimeEvent| serde_json::json!({
            "stream_id":event.stream_id, "sequence":event.sequence, "event_id":event.event_id,
            "transaction_id":event.transaction_id, "transaction_index":event.transaction_index,
            "commit_cursor":event.commit_cursor,
        });
        let payload = serde_json::json!({
            "version":1, "binding":binding, "authorization":authorization,
            "assessment":coord(assessment), "consume":coord(&consume),
        });
        drop(events);
        let persisted = store.append(RuntimeEventInput {
            stream_id: format!("session:{}", self.session_id()), scope: RuntimeEventScope::Tool,
            kind: "authorization.call_authorized".into(), status: Some("authorized".into()),
            actor: Some("conversation_runtime".into()),
            refs: vec![RuntimeEventRef { kind:"authorization_lease".into(), id:lease.lease_id.clone() },
                RuntimeEventRef { kind:"capability".into(), id:request.tool_name.clone() }],
            payload,
        });
        if let Ok(event) = &persisted {
            // The surrounding original single-call scope carries this exact
            // returned event into the original checked action transaction.
            capture_member_event(event.clone());
        }
        if let Err(error) = persisted {
            self.authorization_negotiator.revoke(&lease.lease_id);
            let _ = self.authorization_negotiator.take_transitions_for_persistence();
            for pending in self.authorization_negotiator.transitions_awaiting_persistence() {
                let pending_stream = format!("authorization-lease:{}", pending.lease.lease_id);
                if crate::authorization_negotiator::persist_authorization_transition_event(
                    store, &pending_stream, "conversation_runtime", &pending,
                ).is_err() { break; }
                self.authorization_negotiator.acknowledge_persisted_transitions(std::slice::from_ref(&pending.transition_id));
            }
            return Err(RuntimeError::new(format!("completion original authorization persistence failed: {error}")));
        }
        Ok(result)
    }

    async fn negotiate_tool_authorization_inner(
        &self, descriptor: &harness_contract::tool::ToolEffectDescriptor, input: &str,
        idempotency_key: String, permission_context: PermissionContext, timeout_secs: u64,
        prompter: &crate::permissions::SharedPrompter, binding: Option<&serde_json::Value>,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        let owner = SINGLE_ORIGINAL_EVIDENCE.try_with(|capture| {
            let mut original = capture.owner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let owner = Arc::clone(original.get_or_insert_with(|| Arc::new(self.shared_policy_owner())));
            let mut context = capture.permission_context.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if context.is_none() { *context = Some(permission_context.clone()); }
            owner
        }).unwrap_or_else(|_| Arc::new(self.shared_policy_owner()));
        let result = owner.negotiate_tool_authorization_inner(
            descriptor, input, idempotency_key, permission_context, timeout_secs, prompter, binding,
        ).await;
        if !self.original_policy_control_parent_owned
            && owner.control_requested.swap(false, std::sync::atomic::Ordering::AcqRel) {
            self.consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::AfterToolResult);
        }
        result
    }

    pub(super) fn govern_capability_gap(
        &self, assessment: harness_contract::policy::CapabilityAssessment,
    ) -> harness_contract::policy::CapabilityAssessment {
        self.shared_policy_owner().govern_capability_gap(assessment)
    }

    pub(crate) fn restore_controlled_recovery_claims_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        execution_id: &str,
    ) -> Result<usize, RuntimeError> {
        if self.session_id().to_string() != session_id
            || turn_id.trim().is_empty()
            || execution_id.trim().is_empty()
        {
            return Err(RuntimeError::new(
                "controlled recovery restore identity does not match the active Session turn",
            ));
        }
        let store = self.runtime_event_store.as_ref().ok_or_else(|| {
            RuntimeError::new("controlled recovery restore requires the Runtime event store")
        })?;
        let claims = crate::authorization_negotiator::load_open_controlled_recovery_claims(
            store,
            session_id,
            turn_id,
            execution_id,
        )
        .map_err(RuntimeError::new)?;
        let mut restored = 0;
        for claim in claims {
            if self
                .authorization_negotiator
                .restore_controlled_recovery_claim(&claim.fingerprint, &claim.recovery_scope)
            {
                restored += 1;
            }
        }
        Ok(restored)
    }
}

/// The read_ref target decision and the check of its assessment event must
/// use the same permission context. These tests pin that shape.
#[cfg(test)]
mod original_permission_context_tests {
    use super::original_permission_context_json;

    #[test]
    fn original_permission_context_matches_the_decision_context() {
        let default = crate::PermissionContext::default();
        assert_eq!(original_permission_context_json(&default),
            serde_json::json!({"override_decision": null, "override_reason": null}));
        let overridden = crate::PermissionContext::new(
            Some(crate::permissions::PermissionOverride::Allow), Some("operator".to_string()));
        assert_eq!(original_permission_context_json(&overridden),
            serde_json::json!({"override_decision": "allow", "override_reason": "operator"}));
        // An override session is not the default context. Demanding the
        // default one while deciding with the real one would refuse every
        // session that carries an operator override.
        assert_ne!(original_permission_context_json(&overridden),
            original_permission_context_json(&default));
    }
}

/// The Host member producer used to require the effect owner slot and
/// refused a Read-family member for a missing owner. These assertions read
/// the owner the same way the producer does: an absent owner is not a
/// refusal, and an owner that names another call still is.
#[cfg(test)]
mod original_host_owner_tests {
    use super::*;

    /// The original call's own request, with the invocation its effect
    /// owner names.
    fn owner_request(
        tool_name: &str,
        effect_key: &str,
        workspace: &std::path::Path,
    ) -> crate::RuntimeToolExecutionRequest {
        let invocation = tools::ToolHost::builtin("host-owner", workspace)
            .pin_snapshot()
            .prepare_governed_invocation("host-call", tool_name, &serde_json::json!({}), &[]);
        let effect = invocation.effect.clone();
        crate::RuntimeToolExecutionRequest {
            compiled_invocation: Some(invocation),
            step_binding: None,
            governed_plan_id: "plan".into(),
            governed_plan_revision: 1,
            observation_wave_sequence: 1,
            idempotency_key: effect_key.into(),
            tool_use_id: "host-call".into(),
            tool_name: tool_name.into(),
            input: "{}".into(),
            category: crate::ToolSafetyCategory::from_effect(&effect),
            authorization: None,
            session_id: Some("session".into()),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            authorized_scopes: vec![],
            memory_context: None,
            reality_context: None,
            model_lease: None,
            parent_execution: None,
            parent_execution_attempt: None,
            execution_decision: None,
            evaluation_isolated: true,
            managed_invocation: None,
            tool_progress: crate::ToolProgressSink::default(),
        }
    }

    /// The effect owner this call's own first effect produced, recovered
    /// from the durable facts it was built from.
    fn effect_owner(
        events: &Arc<crate::runtime_event_store::RuntimeEventStore>,
        request: &Arc<crate::RuntimeToolExecutionRequest>,
    ) -> Arc<crate::execution_core::graph::OriginalFreshEffectOwner> {
        let effect_key = request.idempotency_key.clone();
        let stream_id = format!("execution-effect:{effect_key}");
        let key = format!("{effect_key}:intent");
        let transaction = crate::runtime_event_store::AppendTransactionRequest {
            transaction_id: key.clone(),
            expected_streams: vec![crate::runtime_event_store::ExpectedStreamRevision {
                stream_id: stream_id.clone(),
                expected_revision: 0,
            }],
            events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                event: crate::runtime_event_store::RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: crate::runtime_event_store::RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.intent".into(),
                    status: Some("inflight".into()),
                    actor: Some("governed_tool".into()),
                    refs: vec![crate::runtime_event_store::RuntimeEventRef {
                        kind: "tool_invocation".into(),
                        id: "host-call".into(),
                    }],
                    payload: serde_json::json!({
                        "idempotency_key": effect_key,
                        "tool_use_id": "host-call",
                        "tool_name": request.tool_name,
                    }),
                },
                idempotency_key: Some(key.clone()),
                schema_version: 1,
            }],
        };
        events.append_transaction(transaction.clone()).expect("intent");
        events
            .retain_original_first_effect(Arc::new(transaction), Arc::clone(request))
            .expect("retain");
        crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(events))
            .recover_original_first_effect_owner(&key)
            .expect("owner")
            .expect("owner present")
    }

    /// The positive control: a Read-family member has no effect owner at
    /// all, and the Host producer does not refuse it for that any more. The
    /// durable admission fact is what admits it.
    #[test]
    fn the_host_owner_check_does_not_require_an_effect_owner() {
        let events = Arc::new(crate::runtime_event_store::RuntimeEventStore::for_test());
        let workspace = tempfile::tempdir().expect("workspace");
        let request = owner_request("edit", "host-read", workspace.path());
        assert!(
            original_host_effect_owner_matches(None, &events, &request),
            "an absent effect owner is not a refusal on the Host path"
        );
    }

    /// The negative control: an owner that names another call is still
    /// refused. The relaxation is about the absent owner only.
    #[test]
    fn the_host_owner_check_refuses_an_owner_that_names_another_call() {
        let events = Arc::new(crate::runtime_event_store::RuntimeEventStore::for_test());
        let workspace = tempfile::tempdir().expect("workspace");
        let request = Arc::new(owner_request("edit", "host-mine", workspace.path()));
        let other = Arc::new(owner_request("edit", "host-other", workspace.path()));
        let owner = effect_owner(&events, &other);
        assert!(
            !original_host_effect_owner_matches(Some(&owner), &events, &request),
            "an owner of another call is refused"
        );
        let owner = effect_owner(&events, &request);
        assert!(
            original_host_effect_owner_matches(Some(&owner), &events, &request),
            "an owner of this very call is accepted"
        );
    }
}
