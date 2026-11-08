//! WIP: call-scoped capability for the delegated original ToolHost owner.
//! Delegated issuance is confined to the original ToolBatch host adapter.
//! Root and authenticated historical replay are not yet wired.

use std::sync::Arc;
use serde_json::Value;
use harness_contract::agent_action::OriginalAgentExecution;

/// Derive a directory target from the original call; this does not authorize it.
pub(crate) fn read_ref_directory_input(input: &Value) -> Result<Option<Value>, String> {
    let input: harness_contract::refs::UnifiedReadRequest = serde_json::from_value(input.clone())
        .map_err(|error| error.to_string())?;
    input.program_directory_inspect_input().map_err(|error| error.to_string())
}

/// Actual active Session ingress/control/policy owner for a verified
/// delegated actor. It is a borrowed commit input, not a new signer.
pub(crate) struct DelegatedOriginalSessionOwner {
    owner: crate::execution_core::services::OriginalConclusionOwner,
    event_store: Arc<crate::RuntimeEventStore>,
    ticket: crate::execution_core::graph::NodeExecutionTicket,
    packet: harness_contract::agent::AgentTaskPacket,
    original_input: serde_json::Value,
    provider_capture: Option<Arc<crate::conversation::tool_plane::OriginalSingleProducerCapture>>,
}
impl std::fmt::Debug for DelegatedOriginalSessionOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DelegatedOriginalSessionOwner(..)")
    }
}
impl DelegatedOriginalSessionOwner {
    pub(crate) fn ticket(&self) -> &crate::execution_core::graph::NodeExecutionTicket { &self.ticket }
    pub(crate) fn claim(&self) -> &crate::execution_core::services::OriginalSessionClaim { &self.owner.claim }
    pub(crate) fn input(&self) -> &crate::SessionInputStream { &self.owner.input }
    pub(crate) fn control(&self) -> Arc<crate::conversation::MemberCommitControl> { Arc::clone(&self.owner.control) }
    pub(crate) fn with_policy<T>(&self, services: &crate::RuntimeServices,
        invoke: impl FnOnce(&crate::PermissionPolicy) -> Result<T, String>) -> Result<T, String> {
        services.with_conclusion_policy(&self.owner, invoke)
    }
}

/// Borrowed only under the original Session policy and complete source locks.
/// The private seal prevents callers from manufacturing a checked input.
pub(crate) struct DelegatedSingleCheckedInputs<'a> {
    pub(crate) held: &'a crate::runtime_event_store::HeldStreamLocks<'a>,
    pub(crate) sources: &'a [crate::ExpectedStreamRevision],
    pub(crate) control: Arc<crate::conversation::MemberCommitControl>,
    pub(crate) claim: &'a crate::execution_core::services::OriginalSessionClaim,
    pub(crate) consumed_input_sequence: usize,
    pub(crate) ticket: &'a crate::execution_core::graph::NodeExecutionTicket,
    pub(crate) request: &'a crate::RuntimeToolExecutionRequest,
    pub(crate) original_events: &'a [crate::DurableRuntimeEvent],
    pub(crate) original_decision: Option<&'a crate::execution_core::RuntimeExecutionDecision>,
    _seal: (),
}

impl DelegatedOriginalSessionOwner {
    /// Process facts are emitted by the original negotiator before dispatch.
    /// Their lookup key is only a selector: all producer, packet, request,
    /// transition and receipt identities are authenticated under source locks.
    fn with_provider_single_checked<T>(
        &self, services: &crate::RuntimeServices, request: &crate::RuntimeToolExecutionRequest,
        additional_streams: &[String], append: impl FnOnce(DelegatedSingleCheckedInputs<'_>) -> Result<T, String>,
    ) -> Result<T, String> {
        if !Arc::ptr_eq(&self.event_store, services.event_store()) { return Err("original delegated producer store mismatch".into()); }
        let capture = self.provider_capture.as_ref().ok_or("original provider call capture unavailable")?;
        let producer = capture.owner().ok_or("original provider policy producer unavailable")?;
        let live = services.session_execution_policy_control(self.claim().session_id()).ok_or("original Session policy unavailable")?;
        if !producer.uses_original_store(services.event_store()) || !producer.original_policy_control().same_owner(&live) {
            return Err("original provider store or policy owner differs".into());
        }
        let auth = request.authorization.as_ref().ok_or("original provider authorization missing")?;
        let binding = request.step_binding.as_ref().ok_or("original provider Step missing")?;
        // The capture is bound by the original ToolBatch producer before it
        // dispatches this same request; a conflict is not a usable decision.
        if !matches!(&binding.origin.source, harness_contract::step::CallSource::Provider { .. })
            || request.session_id.as_deref() != Some(self.claim().session_id())
            || request.parent_execution.as_ref().is_none_or(|parent| parent.execution_id != self.ticket.graph_id || parent.node_id != self.ticket.node_id)
            || request.parent_execution_attempt != Some(self.ticket.attempt)
        { return Err("original provider owner mismatch".into()); }
        let consumption = producer.original_consumption(auth).ok_or("original provider lease consumption unavailable")?;
        if consumption.lease != auth.authorization_lease
            || !crate::authorization_negotiator::consumed_transition_matches_original_execution(&consumption.transition, auth) {
            return Err("original provider lease producer mismatch".into());
        }
        let events = capture.events();
        let transition = serde_json::to_value(&consumption.transition).map_err(|e| e.to_string())?;
        let compiled = request.compiled_invocation.as_ref().ok_or("original provider compiled effect missing")?;
        let mut assessed = 0usize; let mut consumed = 0usize;
        for event in &events {
            if event.kind == "authorization.capability_assessed" && event.scope == crate::RuntimeEventScope::Tool
                && event.actor.as_deref() == Some("conversation_runtime")
                && event.stream_id == format!("session:{}", self.claim().session_id()) {
                let fact = &event.payload["_call_refusal_binding"];
                let pre = &fact["request"];
                let original_input = pre["input"].as_str().and_then(|text| harness_contract::step::parse_strict_json(text).ok());
                if fact["version"].as_u64() == Some(1)
                    && fact["owner"]["kind"].as_str() == Some("node")
                    && fact["step_binding"] == serde_json::to_value(binding).map_err(|e| e.to_string())?
                    && fact["call_id"].as_str() == Some(request.tool_use_id.as_str())
                    && fact["effect_key"].as_str() == Some(request.idempotency_key.as_str())
                    && fact["tool_name"].as_str() == Some(request.tool_name.as_str())
                    && pre["session_id"].as_str() == Some(self.claim().session_id())
                    && pre["parent_execution"] == serde_json::to_value(&request.parent_execution).map_err(|e| e.to_string())?
                    && pre["parent_execution_attempt"].as_u64() == request.parent_execution_attempt.map(u64::from)
                    && pre["compiled_invocation"]["effect"] == serde_json::to_value(&compiled.effect).map_err(|e| e.to_string())?
                    && original_input.as_ref() == Some(&self.original_input)
                    && event.payload["lease"]["lease_id"].as_str() == Some(auth.authorization_lease.lease_id.as_str())
                    && event.payload.get("_original_permission_context").is_some() { assessed += 1; }
            }
            if event.kind == "authorization.lease_transition" && event.scope == crate::RuntimeEventScope::Tool
                && event.actor.as_deref() == Some("conversation_runtime") && event.status.as_deref() == Some("consumed")
                && event.payload == transition && event.payload["idempotency_key"].as_str() == Some(auth.request_id.as_str()) { consumed += 1; }
        }
        if assessed != 1 || consumed != 1 { return Err("original provider assessment or lease mismatch".into()); }
        let mut streams = additional_streams.to_vec(); streams.push(self.ticket.graph_id.clone());
        streams.push(format!("session:{}", self.claim().session_id()));
        streams.extend(events.iter().map(|event| event.stream_id.clone())); streams.sort(); streams.dedup();
        self.with_policy(services, |policy| {
            let snapshot = policy.execution_policy_control().snapshot();
            if snapshot.revision != request.policy_revision || snapshot.revision != self.packet.policy_revision
                || snapshot.revision != auth.policy_revision || snapshot.sandbox_posture != request.sandbox_posture {
                return Err("original provider policy changed".into());
            }
            services.event_store().with_authenticated_stream_locks(&streams, |held| {
                // Hold the original producer decision through checked append;
                // a conflicting rebind cannot race the commit decision.
                capture.with_original_decision(|original_decision| {
                let original_decision = original_decision.ok_or("original provider decision unavailable")?;
                if request.execution_decision.as_ref() != Some(original_decision) {
                    return Err("original provider decision does not match same-call capture".into());
                }
                let turn = harness_contract::turn::TurnId::from_string(self.claim().turn_id());
                if self.input().active_turn_id().as_ref() != Some(&turn) { return Err("original provider turn no longer active".into()); }
                let fence = self.claim().fence()?;
                let cursor = self.input().highest_consumed_cursor(&turn);
                if self.input().active_turn_id().as_ref() != Some(&turn) { return Err("original provider turn changed while reading cursor".into()); }
                let consumed_input_sequence = match cursor {
                    Some(cursor) if cursor.generation == fence.session_generation => usize::try_from(cursor.sequence).map_err(|_| "consumed cursor overflow")?.max(fence.input_sequence),
                    Some(_) => return Err("original provider cursor generation mismatch".into()),
                    None => fence.input_sequence,
                };
                let sources = streams.iter().map(|stream_id| Ok(crate::ExpectedStreamRevision {
                    stream_id: stream_id.clone(), expected_revision: services.event_store().stream_revision(stream_id).map_err(|e| e.to_string())?,
                })).collect::<Result<Vec<_>, String>>()?;
                append(DelegatedSingleCheckedInputs { held, sources: &sources, control: self.control(), claim: self.claim(),
                    consumed_input_sequence, ticket: &self.ticket, request, original_events: &events, original_decision: Some(original_decision), _seal: () })
                })
            })
        })
    }

    fn with_process_single_checked<T>(
        &self, services: &crate::RuntimeServices,
        request: &crate::RuntimeToolExecutionRequest,
        packet: &harness_contract::agent::AgentTaskPacket,
        additional_streams: &[String],
        append: impl FnOnce(DelegatedSingleCheckedInputs<'_>) -> Result<T, String>,
    ) -> Result<T, String> {
        if !Arc::ptr_eq(&self.event_store, services.event_store()) {
            return Err("original delegated producer store mismatch".into());
        }
        let auth = request.authorization.as_ref().ok_or("original Process authorization missing")?;
        let step_binding = request.step_binding.as_ref().ok_or("original Process Step missing")?;
        if !matches!(&step_binding.origin.source, harness_contract::step::CallSource::Process { worker_id, .. } if worker_id == packet.run_id())
            || request.parent_execution.as_ref().is_none_or(|parent| parent.execution_id != self.ticket.graph_id || parent.node_id != self.ticket.node_id)
            || request.parent_execution_attempt != Some(self.ticket.attempt)
            || packet.graph_id() != self.ticket.graph_id || packet.node_id() != self.ticket.node_id || packet.attempt != self.ticket.attempt
            || request.session_id.as_deref() != Some(self.claim().session_id()) || packet.session_id() != self.claim().session_id()
        { return Err("original Process owner mismatch".into()); }
        let session_stream = format!("session:{}", self.claim().session_id());
        let assessment_key = format!("process-action-assessment:{}", auth.request_id);
        let assessment = services.event_store().event_by_idempotency_key(&session_stream, &assessment_key)
            .map_err(|e| e.to_string())?.ok_or("original Process assessment unavailable")?;
        let coord = &assessment.payload["_original_process_call"]["consume"];
        let consume_stream = coord["stream_id"].as_str().filter(|s| !s.is_empty()).ok_or("original Process consume stream missing")?.to_string();
        let consume_sequence = coord["sequence"].as_u64().filter(|n| *n > 0).ok_or("original Process consume sequence missing")?;
        let mut records = services.event_store().list_stream_before(&consume_stream, consume_sequence, 1, 1024 * 1024)?;
        let consume = records.pop().filter(|e| e.sequence == consume_sequence).ok_or("original Process consume missing")?;
        let events = vec![assessment, consume];
        let mut streams = additional_streams.to_vec();
        streams.extend([self.ticket.graph_id.clone(), session_stream.clone(), consume_stream.clone()]);
        streams.sort(); streams.dedup();
        self.with_policy(services, |policy| {
            let snapshot = policy.execution_policy_control().snapshot();
            if snapshot.revision != request.policy_revision || snapshot.revision != packet.policy_revision
                || snapshot.revision != auth.policy_revision || snapshot.sandbox_posture != request.sandbox_posture {
                return Err("original Process policy changed".into());
            }
            services.event_store().with_authenticated_stream_locks(&streams, |held| {
                // The issuer already authenticated this immutable Step,
                // effective input and signed authorization for this call.
                let input = &self.original_input;
                let assessment = &events[0]; let consume = &events[1];
                for event in &events {
                    let actual = services.event_store().list_stream_before(&event.stream_id, event.sequence, 1, 1024 * 1024)?;
                    let receipt = services.event_store().original_transaction_receipt(&event.transaction_id)
                        .map_err(|e| e.to_string())?.ok_or("original Process event receipt unavailable")?;
                    if actual.as_slice() != std::slice::from_ref(event) || receipt.transaction_id != event.transaction_id
                        || receipt.commit_cursor != event.commit_cursor
                        || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id) {
                        return Err("original Process event receipt mismatch".into());
                    }
                }
                let fact = &assessment.payload["_original_process_call"];
                let compiled = request.compiled_invocation.as_ref().ok_or("original Process compiled effect missing")?;
                let source_input = fact["input"].as_str().ok_or("original Process input missing")?;
                let transition: harness_contract::policy::AuthorizationLeaseTransition = serde_json::from_value(consume.payload.clone()).map_err(|e| e.to_string())?;
                if assessment.stream_id != session_stream || assessment.scope != crate::RuntimeEventScope::Tool
                    || assessment.kind != "authorization.capability_assessed" || assessment.status.as_deref() != Some("authorized")
                    || assessment.actor.as_deref() != Some("runtime.process_jsonl") || assessment.schema_version != 1
                    || assessment.idempotency_key.as_deref() != Some(assessment_key.as_str())
                    || fact["version"].as_u64() != Some(1)
                    || fact["packet_identity"]["graph_id"].as_str() != Some(packet.graph_id())
                    || fact["packet_identity"]["node_id"].as_str() != Some(packet.node_id())
                    || fact["packet_identity"]["attempt"].as_u64() != Some(u64::from(packet.attempt))
                    || fact["packet_identity"]["run_id"].as_str() != Some(packet.run_id())
                    || fact["packet_identity"]["agent_id"].as_str() != Some(packet.agent_id())
                    || fact["packet_identity"]["session_id"].as_str() != Some(packet.session_id())
                    || fact["step_id"].as_str() != Some(step_binding.snapshot.identity.step_id.as_str())
                    || fact["step_origin"] != serde_json::to_value(&step_binding.origin).map_err(|e| e.to_string())?
                    || fact["effect_key"].as_str() != Some(request.idempotency_key.as_str())
                    || fact["request_id"].as_str() != Some(request.tool_use_id.as_str())
                    || fact["authorization_request_id"].as_str() != Some(auth.request_id.as_str())
                    || fact["tool"].as_str() != Some(request.tool_name.as_str())
                    || harness_contract::step::parse_strict_json(source_input).map_err(|e| e.to_string())? != *input
                    || fact["effect"] != serde_json::to_value(&compiled.effect).map_err(|e| e.to_string())?
                    || fact["policy_revision"].as_u64() != Some(request.policy_revision)
                    || fact["authorization"] != serde_json::to_value(auth).map_err(|e| e.to_string())?
                    || assessment.payload["lease"] != serde_json::to_value(&auth.authorization_lease).map_err(|e| e.to_string())?
                    || consume.stream_id != format!("authorization-lease:{}", auth.authorization_lease.lease_id)
                    || consume.scope != crate::RuntimeEventScope::Tool || consume.kind != "authorization.lease_transition"
                    || consume.actor.as_deref() != Some("runtime.process_jsonl") || consume.status.as_deref() != Some("consumed")
                    || consume.schema_version != 1 || transition.kind != harness_contract::policy::AuthorizationLeaseTransitionKind::Consumed
                    || transition.idempotency_key.as_deref() != Some(auth.request_id.as_str())
                    || !crate::authorization_negotiator::consumed_transition_matches_original_execution(&transition, auth)
                    || fact["consume"]["event_id"].as_str() != Some(consume.event_id.as_str())
                    || fact["consume"]["transaction_id"].as_str() != Some(consume.transaction_id.as_str())
                    || fact["consume"]["transaction_index"].as_u64() != Some(u64::from(consume.transaction_index))
                    || fact["consume"]["commit_cursor"].as_u64() != Some(consume.commit_cursor)
                { return Err("original Process same-call evidence mismatch".into()); }
                let turn = harness_contract::turn::TurnId::from_string(self.claim().turn_id());
                if self.input().active_turn_id().as_ref() != Some(&turn) { return Err("original Process turn no longer active".into()); }
                let fence = self.claim().fence()?;
                let cursor = self.input().highest_consumed_cursor(&turn);
                if self.input().active_turn_id().as_ref() != Some(&turn) { return Err("original Process turn changed while reading cursor".into()); }
                let consumed_input_sequence = match cursor {
                    Some(cursor) if cursor.generation == fence.session_generation => usize::try_from(cursor.sequence).map_err(|_| "consumed cursor overflow")?.max(fence.input_sequence),
                    Some(_) => return Err("original Process cursor generation mismatch".into()),
                    None => fence.input_sequence,
                };
                let sources = streams.iter().map(|stream_id| Ok(crate::ExpectedStreamRevision {
                    stream_id: stream_id.clone(), expected_revision: services.event_store().stream_revision(stream_id).map_err(|e| e.to_string())?,
                })).collect::<Result<Vec<_>, String>>()?;
                append(DelegatedSingleCheckedInputs { held, sources: &sources, control: self.control(), claim: self.claim(),
                    consumed_input_sequence, ticket: &self.ticket, request, original_events: &events, original_decision: None, _seal: () })
            })
        })
    }
}

/// The private witness can only be made here after the original delegated
/// owner admission and physical identity have been revalidated.
pub(crate) struct OwnerAdmissionWitness {
    original_request: crate::RuntimeToolExecutionRequest,
    session_id: String,
    turn_id: String,
    original_execution: Option<OriginalAgentExecution>,
    root_owner: Option<crate::conversation::tool_plane::VerifiedRootActionOwner>,
    delegated_session_owner: Option<DelegatedOriginalSessionOwner>,
    step_id: String,
    call_id: String,
    effect_key: String,
    outer_tool: String,
    outer_input: Value,
    read_ref_target: Option<crate::conversation::OriginalReadRefTarget>,
    actor: Option<harness_contract::agent_action::AgentActorBinding>,
}

/// Process-local bearer for exactly one original outer call. It is neither
/// serializable nor constructible outside Runtime. A clone preserves its exact
/// scope; it does not widen the set of actions.
#[derive(Debug, Clone)]
pub struct VerifiedActionInvocation {
    scope: Arc<ActionInvocationScope>,
}

#[derive(Debug)]
struct ActionInvocationScope {
    original_request: crate::RuntimeToolExecutionRequest,
    session_id: String,
    turn_id: String,
    original_execution: Option<OriginalAgentExecution>,
    root_owner: Option<crate::conversation::tool_plane::VerifiedRootActionOwner>,
    delegated_session_owner: Option<DelegatedOriginalSessionOwner>,
    step_id: String,
    call_id: String,
    effect_key: String,
    outer_tool: String,
    outer_input: Value,
    read_ref_target: Option<crate::conversation::OriginalReadRefTarget>,
    actor: Option<harness_contract::agent_action::AgentActorBinding>,
    active: std::sync::atomic::AtomicBool,
}

impl VerifiedActionInvocation {
    /// Gateway must consume the exact request admitted by the original owner.
    /// The progress sink uses its own non-identity equality semantics; it is
    /// not treated as a durable owner or a transaction fence.
    pub fn validate_request(&self, request: &crate::RuntimeToolExecutionRequest) -> Result<(), &'static str> {
        if !self.scope.active.load(std::sync::atomic::Ordering::Acquire)
            || &self.scope.original_request != request {
            return Err("unknown: original action request differs or has expired");
        }
        Ok(())
    }
    /// Only the live original invocation may lend its sealed root owner to
    /// the synchronous checked-action path. Coordinates cannot construct it.
    pub(crate) fn original_root_owner(&self) -> Result<Option<&crate::conversation::tool_plane::VerifiedRootActionOwner>, &'static str> {
        if !self.scope.active.load(std::sync::atomic::Ordering::Acquire) {
            return Err("original invocation no longer active");
        }
        Ok(self.scope.root_owner.as_ref())
    }
    /// The callback is synchronous and borrows the original owner only while
    /// this invocation is active. It does not certify call-specific policy
    /// events or permit an unchecked append.
    pub(crate) fn with_original_delegated_owner<T>(&self,
        invoke: impl FnOnce(&DelegatedOriginalSessionOwner) -> Result<T, String>,
    ) -> Result<T, String> {
        if !self.scope.active.load(std::sync::atomic::Ordering::Acquire) {
            return Err("original invocation no longer active".into());
        }
        let owner = self.scope.delegated_session_owner.as_ref().ok_or("original delegated Session owner unavailable")?;
        invoke(owner)
    }
    pub(crate) fn with_original_delegated_checked<T>(&self, services: &crate::RuntimeServices,
        streams: &[String], append: impl FnOnce(DelegatedSingleCheckedInputs<'_>) -> Result<T, String>,
    ) -> Result<T, String> {
        self.with_original_delegated_owner(|owner| match &self.scope.original_request.step_binding.as_ref().ok_or("original delegated Step unavailable")?.origin.source {
            harness_contract::step::CallSource::Provider { .. } => owner.with_provider_single_checked(services, &self.scope.original_request, streams, append),
            harness_contract::step::CallSource::Process { .. } => owner.with_process_single_checked(services, &self.scope.original_request, &owner.packet, streams, append),
            _ => Err("original delegated call source unsupported".into()),
        })
    }
    pub fn root_physical_owner(&self) -> Option<OriginalAgentExecution> {
        if !self.scope.active.load(std::sync::atomic::Ordering::Acquire) { return None; }
        self.scope.root_owner.as_ref().map(|owner| owner.physical())
    }
    /// This constructor cannot be called until an original owner supplies the
    /// sealed witness. Do not make a witness from caller-supplied coordinates.
    pub(crate) fn from_owner(
        witness: OwnerAdmissionWitness,
    ) -> Result<Self, &'static str> {
        if witness.session_id.is_empty() || witness.turn_id.is_empty()
            || match (&witness.original_execution, &witness.root_owner, &witness.actor) {
                (Some(original), None, Some(actor)) => original.graph_id.is_empty() || original.node_id.is_empty() || original.attempt == 0
                    || actor.original_execution.as_ref() != Some(original)
                    || actor.session_id != witness.session_id || actor.turn_id != witness.turn_id,
                (None, Some(_), None) => false,
                _ => true,
            }
            || witness.step_id.is_empty() || witness.call_id.is_empty()
            || witness.effect_key.is_empty() || witness.outer_tool.is_empty()
        {
            return Err("incomplete original action invocation owner");
        }
        Ok(Self { scope: Arc::new(ActionInvocationScope {
            original_request: witness.original_request, session_id: witness.session_id, turn_id: witness.turn_id,
            original_execution: witness.original_execution, root_owner: witness.root_owner,
            delegated_session_owner: witness.delegated_session_owner, step_id: witness.step_id,
            call_id: witness.call_id, effect_key: witness.effect_key,
            outer_tool: witness.outer_tool, outer_input: witness.outer_input, read_ref_target: witness.read_ref_target, actor: witness.actor,
            active: std::sync::atomic::AtomicBool::new(true),
        }) })
    }

    /// Gateway calls this before parsing or removing action input fields.
    /// The batch entry must be the exact indexed member of the original outer
    /// payload after the existing Gateway reference normalization.
    pub fn validate_action(
        &self, session_id: &str, turn_id: &str,
        original: &OriginalAgentExecution, step_id: &str, call_id: &str,
        action_id: &str, tool: &str, input: &Value, batch_index: Option<usize>,
    ) -> Result<(), &'static str> {
        let scope = &self.scope;
        if !scope.active.load(std::sync::atomic::Ordering::Acquire) { return Err("original invocation no longer active"); }
        if scope.session_id != session_id || scope.turn_id != turn_id
            || match &scope.root_owner { Some(root) => root.physical() != *original, None => scope.original_execution.as_ref() != Some(original) } || scope.step_id != step_id
            || scope.call_id != call_id {
            return Err("original action invocation owner mismatch");
        }
        let (expected_id, expected_tool, expected_input) = match batch_index {
            None if scope.outer_tool != "agent_action_batch" => (
                scope.effect_key.clone(), scope.outer_tool.as_str(), scope.outer_input.clone()),
            _ => return Err("original action batch scope mismatch"),
        };
        let derived = batch_index.is_none() && scope.outer_tool == "read_ref" && tool == "state_inspect"
            && scope.read_ref_target.as_ref().is_some_and(|target|
                &target.input == input && target.ticket.graph_id == original.graph_id
                    && target.ticket.node_id == original.node_id && target.ticket.attempt == original.attempt
                    && read_ref_directory_input(&scope.outer_input).is_ok_and(|derived| derived.as_ref() == Some(input)));
        if expected_id != action_id || (!derived && (expected_tool != tool || &expected_input != input)) {
            return Err("action is not the original authorized call or batch entry");
        }
        Ok(())
    }
}



impl VerifiedActionInvocation {
    pub(crate) fn validate_envelope(
        &self, envelope: &harness_contract::agent_action::AgentActionEnvelope,
        tool: &str, input: &Value,
    ) -> Result<(), &'static str> {
        match (&self.scope.root_owner, &self.scope.actor) {
            (Some(root), None) => root.validate_actor(&envelope.actor)?,
            (None, Some(actor)) if actor == &envelope.actor => {},
            _ => return Err("action actor is not the authenticated owner"),
        }
        let index = if self.scope.outer_tool == "agent_action_batch" {
            let suffix = envelope.action_id.strip_prefix(&format!("{}:", self.scope.effect_key))
                .ok_or("batch action id is not under original call")?;
            let index: usize = suffix.parse().map_err(|_| "invalid batch index")?;
            if index.to_string() != suffix { return Err("noncanonical batch index"); }
            Some(index)
        } else { None };
        self.validate_action(&envelope.actor.session_id, &envelope.actor.turn_id,
            envelope.actor.original_execution.as_ref().ok_or("actor physical owner missing")?,
            &self.scope.step_id, &self.scope.call_id, &envelope.action_id, tool, input, index)?;
        let mut parsed = input.clone();
        let object = parsed.as_object_mut().ok_or("action input is not an object")?;
        let expected_revision = match object.remove("expected_revision") {
            None | Some(Value::Null) => None,
            Some(Value::Number(number)) => Some(number.as_u64().ok_or("invalid expected revision")?),
            Some(_) => return Err("invalid expected revision"),
        };
        if tool == "state_inspect" && (object.contains_key("entry_ref") || object.contains_key("scope_ref")) {
            object.remove("page_cursor"); object.remove("query");
        }
        let action: harness_contract::agent_action::AgentAction = serde_json::from_value(
            serde_json::json!({"kind":tool,"input":parsed})).map_err(|_| "invalid original action")?;
        if envelope.action != action || envelope.expected_revision != expected_revision {
            return Err("submitted action differs from original authorized input");
        }
        Ok(())
    }
}

/// Root signing accepts only the sealed proof made by the conversation owner.
/// The root actor is deliberately not synthesized here. Consumption is WIP.
pub(crate) async fn with_verified_root_owner<F, Fut, T>(
    proof: crate::conversation::tool_plane::VerifiedRootActionOwner,
    invoke: F,
) -> Result<T, &'static str>
where F: FnOnce(VerifiedActionInvocation) -> Fut, Fut: std::future::Future<Output = T> {
    let (session_id, turn_id, step_id, call_id, effect_key, outer_tool, outer_input) = proof.call_identity();
    let original_request = proof.original_request().clone();
    let read_ref_target = proof.read_ref_target().cloned();
    let witness = OwnerAdmissionWitness {
        original_request, session_id, turn_id, original_execution: None, root_owner: Some(proof),
        delegated_session_owner: None, step_id, call_id, effect_key, outer_tool, outer_input, read_ref_target, actor: None,
    };
    let capability = VerifiedActionInvocation::from_owner(witness)?;
    struct Lifetime(Arc<ActionInvocationScope>);
    impl Drop for Lifetime { fn drop(&mut self) { self.0.active.store(false, std::sync::atomic::Ordering::Release); } }
    let lifetime = Lifetime(Arc::clone(&capability.scope));
    let result = invoke(capability).await;
    drop(lifetime);
    Ok(result)
}

// Evidence/artifact scopes are the scopes actually granted to this call, not
// the packet's entire resource ceiling. They never grant tool-effect authority.
fn delegated_evidence_scopes_allowed(scopes: &[String], session_id: &str, bounded: bool, packet_scopes: &[String]) -> bool {
    let session_scope = format!("session:{session_id}");
    scopes.iter().any(|scope| scope == &session_scope)
        && scopes.iter().all(|scope| scope == &session_scope || (bounded && packet_scopes.contains(scope)))
}

/// Called only by the original delegated ToolHost wrapper after the bound
/// effect owner enters its admitted execution branch; this function rechecks
/// the original physical owner and exact Step before granting the bearer.
pub(crate) async fn with_verified_delegated_call<F, Fut, T>(
    services: &crate::RuntimeServices,
    request: &crate::RuntimeToolExecutionRequest,
    packet: &harness_contract::agent::AgentTaskPacket,
    lease: tools::ToolHostLease,
    parent_ticket: Option<&crate::execution_core::graph::NodeExecutionTicket>,
    parent_stop: Option<&crate::bound_tool_batch::BoundToolStop>,
    child_stop: &crate::bound_tool_batch::BoundToolStop,
    provider_capture: Option<&Arc<crate::conversation::tool_plane::OriginalSingleProducerCapture>>,
    invoke: F,
) -> Result<T, String>
where F: FnOnce(VerifiedActionInvocation) -> Fut, Fut: std::future::Future<Output = T> {
    let parent = request.parent_execution.as_ref().ok_or("delegated parent missing")?;
    let attempt = request.parent_execution_attempt.ok_or("delegated attempt missing")?;
    if parent.execution_id != packet.graph_id() || parent.node_id != packet.node_id()
        || attempt != packet.attempt || request.session_id.as_deref() != Some(packet.session_id()) {
        return Err("delegated request/packet owner mismatch".into());
    }
    let binding = request.step_binding.as_ref().ok_or("delegated Step missing")?;
    if binding.effect_idempotency_key() != request.idempotency_key {
        return Err("delegated call effect identity mismatch".into());
    }
    let step = binding.resolve(Some(lease.clone())).map_err(|e| e.to_string())?;
    let input = step.validate_call(&binding.origin, packet.session_id(), &request.tool_use_id,
        &request.tool_name, &request.input, chrono::Utc::now().timestamp_millis())
        .map_err(|e| e.to_string())?;
    let identity = &step.snapshot().identity;
    let source_matches = match &binding.origin.source {
        harness_contract::step::CallSource::Provider { .. } =>
            identity.execution_id.as_deref() == Some(packet.run_id()) && identity.attempt.is_none_or(|a| a == attempt),
        harness_contract::step::CallSource::Process { worker_id, .. } =>
            identity.execution_id.as_deref() == Some(packet.graph_id()) && identity.attempt == Some(attempt)
                && worker_id == packet.run_id(),
        _ => false,
    };
    if !source_matches || identity.run_ref.as_deref() != Some(packet.run_id())
        || identity.session_id != packet.session_id() {
        return Err("delegated Step owner mismatch".into());
    }
    if !packet.allowed_tools.iter().any(|tool| tool == &request.tool_name) {
        return Err("original Agent packet does not admit this action".into());
    }
    let effect = step.lease().describe_effect(&request.tool_name, &input);
    let compiled = request.compiled_invocation.as_ref().ok_or("unknown: original compiled action missing")?;
    let bounded = packet.team_id().is_some() || packet.agentic_binding.is_some()
        || packet.binding.as_ref().is_some_and(|binding| binding.evaluation.is_some());
    if !delegated_evidence_scopes_allowed(&request.authorized_scopes, packet.session_id(), bounded, &packet.resource_scopes)
        || request.category != crate::ToolSafetyCategory::from_effect(&effect)
        || compiled.effect != effect || compiled.intent.normalized_input != input
        || compiled.invocation_id != request.tool_use_id || compiled.intent.tool_name != request.tool_name
        || compiled.origin.as_ref() != Some(&binding.origin)
        || step.snapshot().related_revisions.get("policy") != Some(&request.policy_revision.to_string())
        || (matches!(&binding.origin.source, harness_contract::step::CallSource::Process { .. })
            && step.snapshot().related_revisions.get("attempt") != Some(&packet.attempt.to_string())) {
        return Err("original action scope/descriptor/Step mismatch".into());
    }
    // ReadOnly is a permission level, not an effect kind: collaboration writes
    // and batches use it too. Only the original signed admission proves that
    // this particular call passed the Session policy and approval rules.
    let authorization = request.authorization.as_ref()
        .ok_or("unknown: original signed action authorization unavailable")?;
    let check_policy = || -> Result<(), String> {
        let policy = services.session_execution_policy(packet.session_id())
            .ok_or("unknown: original Session policy unavailable")?;
        if request.policy_revision == 0 || policy.revision != request.policy_revision
            || authorization.policy_revision != policy.revision || packet.policy_revision != policy.revision
            || policy.sandbox_posture != request.sandbox_posture
            || !policy.permission_mode.permits(effect.required_permission)
            || !packet.permission_ceiling.permits(effect.required_permission)
            || !policy.permission_mode.permits(authorization.authorization_lease.ceiling)
            || !packet.permission_ceiling.permits(authorization.authorization_lease.ceiling)
            || authorization.authorization_lease.principal_id != format!("agent:{}", packet.agent_id()) {
            return Err("original action authorization/policy owner mismatch".into());
        }
        step.lease().validate_authorization(authorization, &request.tool_name, &input)
            .map_err(|error| error.to_string())?;
        Ok(())
    };
    let actor = services.resolve_agent_action_actor_for_attempt(parent, attempt, None).await?;
    check_policy()?;
    if actor.session_id != packet.session_id() || actor.turn_id != identity.turn_id {
        return Err("delegated actor/Step scope mismatch".into());
    }
    let original_execution = actor.original_execution.clone().ok_or("authenticated physical owner missing")?;
    // Resolve only through the active ingress registry for the Program of the
    // already authenticated delegated actor. This carries the real Session
    // claim/control, never a replacement root actor or caller coordinates.
    // The retained child owner carries the authenticated original AgentTask
    // start ticket; no request-derived ticket is constructed here.
    let original_ticket = parent_ticket.ok_or("unknown: original AgentTask ticket unavailable")?;
    if original_ticket.graph_id != parent.execution_id || original_ticket.node_id != parent.node_id
        || original_ticket.attempt != attempt {
        return Err("original delegated AgentTask start ticket mismatch".into());
    }
    let delegated_program = services.agent_action_service().project_snapshot(&actor.program_id).ok()
        .filter(|program| program.session_id == actor.session_id && program.turn_id == actor.turn_id
            && program.root_execution_id == actor.root_execution_id);
    // The original Session owner is normally the live root ingress. Once the
    // root graph has ended, that bus lease is gone, and the equivalent owner
    // is recovered from the original Session fence the ingress installed: the
    // same durable claim and the same original turn control, never a new one.
    // The recovery is asked for this member: its own graph is the member the
    // authenticated actor belongs to, and only that member's own unsettled
    // attempt can keep the recovery window open for it.
    let delegated_owner = match delegated_program.as_ref() {
        Some(program) => match services.original_conclusion_owner(program) {
            Some(owner) => Some(owner),
            None => match program.root_execution_id.as_deref() {
                Some(root_execution_id) => services
                    .recovered_original_conclusion_owner(
                        &program.session_id,
                        &program.turn_id,
                        root_execution_id,
                        &parent.execution_id,
                    )
                    .await,
                None => None,
            },
        },
        None => None,
    };
    let delegated_session_owner = delegated_owner
        .filter(|owner| owner.claim.session_id() == actor.session_id && owner.claim.turn_id() == actor.turn_id)
        .map(|owner| DelegatedOriginalSessionOwner { owner, event_store: Arc::clone(services.event_store()), ticket: original_ticket.clone(), packet: packet.clone(), original_input: input.clone(), provider_capture: provider_capture.cloned() });
    if matches!(&binding.origin.source, harness_contract::step::CallSource::Process { .. }) {
        let stop = parent_stop.ok_or("unknown: original Process call stop unavailable")?;
        let owner = delegated_session_owner.as_ref().ok_or("unknown: original delegated Session owner unavailable")?;
        // The same transaction control participates in the original Process,
        // child graph and Session cancellation races through commit_wins.
        stop.register_commit_control(&owner.owner.control);
        child_stop.register_commit_control(&owner.owner.control);
    }
    if matches!(&binding.origin.source, harness_contract::step::CallSource::Provider { .. }) {
        let capture = provider_capture.ok_or("unknown: original provider call capture unavailable")?;
        let producer = capture.owner().ok_or("unknown: original provider producer unavailable")?;
        let owner = delegated_session_owner.as_ref().ok_or("unknown: original delegated Session owner unavailable")?;
        producer.register_original_control(&owner.owner.control);
        child_stop.register_commit_control(&owner.owner.control);
        if let Some(stop) = parent_stop { stop.register_commit_control(&owner.owner.control); }
    }
    let capability = VerifiedActionInvocation::from_owner(OwnerAdmissionWitness {
        original_request: request.clone(), session_id: actor.session_id.clone(), turn_id: actor.turn_id.clone(),
        original_execution: Some(original_execution), root_owner: None, delegated_session_owner, step_id: identity.step_id.clone(), call_id: request.tool_use_id.clone(),
        effect_key: request.idempotency_key.clone(), outer_tool: request.tool_name.clone(), outer_input: input, read_ref_target: None, actor: Some(actor),
    }).map_err(str::to_string)?;
    struct InvocationLifetime(Arc<ActionInvocationScope>);
    impl Drop for InvocationLifetime {
        fn drop(&mut self) { self.0.active.store(false, std::sync::atomic::Ordering::Release); }
    }
    let lifetime = InvocationLifetime(Arc::clone(&capability.scope));
    let outcome = invoke(capability).await;
    drop(lifetime);
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent_action::{AgentActorBinding, AgentActorKind};

    #[test]
    fn delegated_evidence_scopes_are_a_subset_of_the_original_packet() {
        let packet = ["network:*", "workspace:.", "session:s", "program:p"].map(str::to_owned);
        let scopes = |values: &[&str]| values.iter().map(|value| (*value).to_owned()).collect::<Vec<_>>();
        assert!(delegated_evidence_scopes_allowed(&scopes(&["session:s"]), "s", true, &packet));
        assert!(delegated_evidence_scopes_allowed(&scopes(&["program:p", "session:s"]), "s", true, &packet));
        assert!(!delegated_evidence_scopes_allowed(&scopes(&["session:s", "program:other"]), "s", true, &packet));
        assert!(!delegated_evidence_scopes_allowed(&scopes(&["session:other"]), "s", true, &packet));
        assert!(!delegated_evidence_scopes_allowed(&scopes(&["program:p"]), "s", true, &packet));
        assert!(delegated_evidence_scopes_allowed(&scopes(&["session:s"]), "s", false, &packet));
        assert!(!delegated_evidence_scopes_allowed(&scopes(&["session:s", "program:p"]), "s", false, &packet));
    }

    // Unit-level binding test only; it does not prove issuance or commit fencing.
    #[test]
    fn single_call_binding_rejects_other_call_input_owner_and_batch() {
        let physical = OriginalAgentExecution { graph_id: "graph".into(), node_id: "node".into(), attempt: 2 };
        let actor = AgentActorBinding {
            original_execution: Some(physical.clone()), objective_id: "objective".into(),
            program_id: "program".into(), session_id: "session".into(), turn_id: "turn".into(),
            root_execution_id: Some("root".into()), required_team_count: 0, exact_team_count: None,
            objective_summary: String::new(), model_lease: "lease".into(), permission_ceiling: None,
            resource_scopes: vec![], actor_id: "agent".into(), kind: AgentActorKind::Agent,
            execution_id: None, team_id: None, agent_id: None,
        };
        let input = serde_json::json!({"team_ref":"team"});
        let original_request = crate::RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
            tool_use_id: "call".into(), tool_name: "state_inspect".into(), input: input.to_string(), depends_on: vec![],
        });
        let capability = VerifiedActionInvocation::from_owner(OwnerAdmissionWitness {
            original_request: original_request.clone(), session_id: "session".into(), turn_id: "turn".into(), original_execution: Some(physical.clone()),
            root_owner: None, delegated_session_owner: None, step_id: "step".into(), call_id: "call".into(), effect_key: "effect".into(),
            outer_tool: "state_inspect".into(), outer_input: input.clone(), read_ref_target: None, actor: Some(actor),
        }).unwrap();
        let mismatched = OwnerAdmissionWitness {
            original_request: original_request.clone(), session_id: "other-session".into(), turn_id: "turn".into(),
            original_execution: Some(physical.clone()), root_owner: None, delegated_session_owner: None, step_id: "step".into(),
            call_id: "call".into(), effect_key: "effect".into(), outer_tool: "state_inspect".into(),
            outer_input: input.clone(), read_ref_target: None, actor: capability.scope.actor.clone(),
        };
        assert!(VerifiedActionInvocation::from_owner(mismatched).is_err());
        let check = |owner: &OriginalAgentExecution, call: &str, value: &Value, index| {
            capability.validate_action("session", "turn", owner, "step", call, "effect", "state_inspect", value, index)
        };
        assert!(capability.validate_request(&original_request).is_ok());
        let mut wrong_request = original_request.clone();
        wrong_request.tool_use_id = "other".into();
        assert!(capability.validate_request(&wrong_request).is_err());
        assert!(check(&physical, "call", &input, None).is_ok());
        assert!(check(&physical, "other", &input, None).is_err());
        assert!(check(&physical, "call", &serde_json::json!({"team_ref":"other"}), None).is_err());
        let other = OriginalAgentExecution { attempt: 3, ..physical.clone() };
        assert!(check(&other, "call", &input, None).is_err());
        assert!(check(&physical, "call", &input, Some(0)).is_err());
        capability.scope.active.store(false, std::sync::atomic::Ordering::Release);
        assert!(check(&physical, "call", &input, None).is_err());
    }
}
