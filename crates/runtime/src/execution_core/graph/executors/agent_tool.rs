//! Durable Agent ToolBatch tickets reconstructed by the original scoped executor.
use super::{ScopedNodeBackend, ScopedNodeBackendResolver};
use crate::execution_core::graph::{NodeExecutionOutcome, NodeExecutionTicket, NodeExecutorError};
use crate::{RuntimeServices, RuntimeToolExecutionOutcome, RuntimeToolExecutionRequest, ToolError};
use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Weak};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoDispatchCall {
    pub(crate) call_id: String,
    pub(crate) tool_name: String,
    pub(crate) input_sha256: String,
    pub(crate) effect_key: Option<String>,
    pub(crate) request_sha256: Option<String>,
    pub(crate) reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) refusal_event_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoDispatchFact {
    pub(crate) ticket: NodeExecutionTicket,
    pub(crate) calls: Vec<NoDispatchCall>,
    pub(crate) never_polled: bool,
    pub(crate) result_sha256: Option<String>,
    pub(crate) historical_sources: std::collections::BTreeMap<String, u64>,
    pub(crate) start: Option<(u64, u64, u32)>,
    /// Same-operation custody only. A decoded node fact cannot create it.
    #[serde(skip)]
    pub(crate) bounded_owners: std::collections::BTreeMap<String, OriginalBoundedWitness>,
    /// Private, operation-scoped authentication of the original committed
    /// node transaction. It is never deserialized from a node payload.
    #[serde(skip)]
    pub(crate) original_node: Option<OriginalNoDispatchNode>,
}

#[derive(Clone)]
pub(crate) struct OriginalNoDispatchNode {
    pub(crate) store: Weak<crate::RuntimeEventStore>,
    pub(crate) transaction_id: String,
    pub(crate) commit_cursor: u64,
    pub(crate) fact_digest: String,
}
impl std::fmt::Debug for OriginalNoDispatchNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalNoDispatchNode").finish_non_exhaustive()
    }
}
impl PartialEq for OriginalNoDispatchNode {
    fn eq(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.store, &other.store) && self.transaction_id == other.transaction_id
            && self.commit_cursor == other.commit_cursor && self.fact_digest == other.fact_digest
    }
}
impl Eq for OriginalNoDispatchNode {}

#[derive(Clone)]
pub(crate) struct OriginalBoundedWitness {
    pub(crate) failure: Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>,
    pub(crate) receipt: Arc<crate::execution_core::graph::CommittedCallRefusal>,
    pub(crate) child: Option<Arc<super::scoped::OriginalAgentChildCall>>,
}
impl std::fmt::Debug for OriginalBoundedWitness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalBoundedWitness").finish_non_exhaustive()
    }
}
impl PartialEq for OriginalBoundedWitness {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.failure, &other.failure)
            && self.receipt.transaction_id() == other.receipt.transaction_id()
            && self.receipt.event_id() == other.receipt.event_id()
            && match (&self.child, &other.child) {
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                (None, None) => true,
                _ => false,
            }
    }
}
impl Eq for OriginalBoundedWitness {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NoDispatchWitness {
    fact: NoDispatchFact,
}

#[derive(Debug, Clone)]
pub(crate) struct NoDispatchObservation {
    ticket: NodeExecutionTicket,
    calls: Vec<NoDispatchCall>,
    state: Arc<std::sync::Mutex<NoDispatchState>>,
}

#[derive(Debug, Default)]
struct NoDispatchState {
    entered: bool,
    closed: bool,
    original_early_reads: std::collections::BTreeMap<String, crate::conversation::EarlyToolExecutionReceipt>,
    original_early_reads_released: std::collections::BTreeMap<String, (String, u64)>,
    uncertain: Vec<String>,
    inherited_hook_pending: std::collections::BTreeSet<String>,
    bounded_controls: Vec<std::sync::Weak<crate::conversation::MemberCommitControl>>,
    packet_scope_pending: std::collections::BTreeSet<String>,
    member_custody: std::collections::BTreeMap<(String, usize), Arc<crate::tool_host::OriginalMemberSettlement>>,
    bounded_acquisition_pending: std::collections::BTreeMap<String, (Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>, Option<Arc<crate::execution_core::graph::CommittedCallRefusal>>)>,
    bounded_acquisition_conflicts: Vec<(Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>, Option<Arc<crate::execution_core::graph::CommittedCallRefusal>>)>,
    bounded_owners: std::collections::BTreeMap<String, OriginalBoundedWitness>,
    packet_scope_settled: std::collections::BTreeMap<String, (String, String)>,
    refused: Vec<NoDispatchCall>,
    completed: Option<NoDispatchWitness>,
}

fn no_dispatch_digest(value: &impl Serialize) -> Result<String, serde_json::Error> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(value)?)
    ))
}

pub(crate) fn no_dispatch_calls(
    ticket: &NodeExecutionTicket,
) -> Result<Vec<NoDispatchCall>, serde_json::Error> {
    if ticket.executor_kind != "tool_batch" {
        return Err(bound_carrier_error(
            "NoDispatch requires original ToolBatch owner",
        ));
    }
    if bound_payload_identity_shape(&ticket.graph_id, &ticket.node_id) {
        let spec = ExecutionNodeSpec {
            id: ticket.node_id.clone(),
            kind: ExecutionNodeKind::ToolBatch,
            executor_kind: ticket.executor_kind.clone(),
            payload_ref: ticket.payload_ref.clone(),
            idempotency_key: ticket.idempotency_key.clone(),
            ..ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", "")
        };
        let request = decode_bound_agent_tool_node(&ticket.graph_id, &spec)?.request;
        return Ok(vec![NoDispatchCall {
            call_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            input_sha256: format!("sha256:{:x}", Sha256::digest(request.input.as_bytes())),
            effect_key: Some(request.idempotency_key.clone()),
            request_sha256: Some(no_dispatch_digest(&request)?),
            reason: String::new(),
            refusal_event_id: None,
        }]);
    }
    let (calls, _) = crate::host::decode_tool_batch(&ticket.payload_ref)?;
    let mut seen = std::collections::BTreeSet::new();
    calls
        .iter()
        .map(|call| {
            let id = call.id.as_str();
            if id.is_empty() {
                return Err(bound_carrier_error("original call id missing"));
            }
            if !seen.insert(id) {
                return Err(bound_carrier_error("duplicate original call id"));
            }
            let effect_key = call
                .binding
                .as_ref()
                .map(|binding| {
                    if binding.origin.call_id != call.id
                        || binding.origin.origin_step_id != binding.snapshot.identity.step_id
                        || binding.origin.catalog_digest != binding.snapshot.catalog_digest
                    {
                        return Err(bound_carrier_error("original call Step identity mismatch"));
                    }
                    Ok(binding.effect_idempotency_key())
                })
                .transpose()?;
            Ok(NoDispatchCall {
                call_id: id.into(),
                tool_name: call.name.clone(),
                input_sha256: format!("sha256:{:x}", Sha256::digest(call.input.as_bytes())),
                effect_key,
                request_sha256: None,
                reason: String::new(),
                refusal_event_id: None,
            })
        })
        .collect()
}

/// Authenticate the original ticket call while allowing the one prepared
/// input to differ when the original same-call hook owner proves the change.
/// A deserialized request without that owner cannot prove a semantic change.
pub(crate) fn validate_original_call_request(
    ticket: &NodeExecutionTicket,
    request: &RuntimeToolExecutionRequest,
) -> Result<(), serde_json::Error> {
    if ticket.executor_kind != "tool_batch" {
        return Err(bound_carrier_error("original call requires ToolBatch owner"));
    }
    let effective = harness_contract::step::parse_strict_json(&request.input)
        .map_err(|error| bound_carrier_error(&error.to_string()))?;
    let invocation = request.compiled_invocation.as_ref()
        .ok_or_else(|| bound_carrier_error("original call invocation missing"))?;
    if effective != invocation.intent.normalized_input {
        return Err(bound_carrier_error("effective input requires replan"));
    }
    if bound_payload_identity_shape(&ticket.graph_id, &ticket.node_id) {
        let spec = ExecutionNodeSpec {
            id: ticket.node_id.clone(), kind: ExecutionNodeKind::ToolBatch,
            executor_kind: ticket.executor_kind.clone(), payload_ref: ticket.payload_ref.clone(),
            idempotency_key: ticket.idempotency_key.clone(),
            ..ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", "")
        };
        let original = decode_bound_agent_tool_node(&ticket.graph_id, &spec)?.request;
        let original_input = harness_contract::step::parse_strict_json(&original.input)
            .map_err(|error| bound_carrier_error(&error.to_string()))?;
        let hook_matches = request.tool_progress.original_governed_hook()
            .map_err(|error| bound_carrier_error(&error))?
            .is_some_and(|hook| hook.matches(ticket, request, &original_input));
        let mut original_value = serde_json::to_value(&original)?;
        let actual_value = serde_json::to_value(request)?;
        let original_object = original_value.as_object_mut()
            .ok_or_else(|| bound_carrier_error("original request is not an object"))?;
        original_object.insert("input".into(), serde_json::Value::String(request.input.clone()));
        if hook_matches {
            original_object.insert("compiled_invocation".into(), serde_json::to_value(&request.compiled_invocation)?);
        }
        if (original_input != effective && !hook_matches) || original_value != actual_value {
            return Err(bound_carrier_error("bound original call differs from prepared request"));
        }
        return Ok(());
    }
    let (calls, _) = crate::host::decode_tool_batch(&ticket.payload_ref)?;
    let mut seen = std::collections::BTreeSet::new();
    let mut matched = 0usize;
    for call in calls {
        if call.id.is_empty() || !seen.insert(call.id.clone()) {
            return Err(bound_carrier_error("original call identity is missing or duplicated"));
        }
        if call.id != request.tool_use_id { continue; }
        let original = harness_contract::step::parse_strict_json(&call.input)
            .map_err(|error| bound_carrier_error(&error.to_string()))?;
        let hook_matches = request.tool_progress.original_governed_hook()
            .map_err(|error| bound_carrier_error(&error))?
            .is_some_and(|hook| hook.matches(ticket, request, &original));
        let explicit = invocation.explicit_dependencies.iter()
            .map(|edge| &edge.depends_on).collect::<std::collections::BTreeSet<_>>();
        let original_binding = match call.binding.as_ref() {
            Some(binding) => binding.origin.call_id == call.id
                && binding.origin.origin_step_id == binding.snapshot.identity.step_id
                && binding.origin.catalog_digest == binding.snapshot.catalog_digest
                && binding.effect_idempotency_key() == request.idempotency_key,
            None => request.step_binding.is_none() && hook_matches
                && request.tool_progress.original_governed_hook()
                    .map_err(|error| bound_carrier_error(&error))?
                    .is_some_and(|hook| {
                        if hook.original_pinned_lease().is_some() {
                            hook.original_prepared_lease(ticket, request).is_ok()
                        } else {
                            hook.matches_native_preparation(ticket, request)
                        }
                    }),
        };
        if call.name == request.tool_name && call.binding == request.step_binding
            && original_binding
            && explicit.len() == invocation.explicit_dependencies.len()
            && explicit == call.depends_on.iter().collect::<std::collections::BTreeSet<_>>()
            && invocation.explicit_dependencies.iter().all(|edge|
                edge.invocation_id == call.id && edge.reason == "model_explicit_dependency")
            && invocation.compiled_dependencies.iter().all(|edge| edge.invocation_id == call.id)
            && (original == effective || hook_matches) {
            matched += 1;
        }
    }
    if matched != 1 {
        return Err(bound_carrier_error("request is not the unique original ticket call"));
    }
    Ok(())
}

impl NoDispatchObservation {
    pub(super) fn new(ticket: &NodeExecutionTicket) -> Result<Arc<Self>, NodeExecutorError> {
        let calls = no_dispatch_calls(ticket).map_err(|error| NodeExecutorError::Invalid {
            node_id: ticket.node_id.clone(),
            reason: error.to_string(),
        })?;
        Ok(Arc::new(Self {
            ticket: ticket.clone(),
            calls,
            state: Arc::new(std::sync::Mutex::new(NoDispatchState::default())),
        }))
    }

    pub(crate) fn ticket_matches(&self, ticket: &NodeExecutionTicket) -> bool {
        self.ticket == *ticket
    }

    /// Keep the already executed read with this original Scoped binding.
    pub(crate) fn hold_original_early_read(
        &self, ticket: &NodeExecutionTicket,
        receipt: crate::conversation::EarlyToolExecutionReceipt,
    ) -> Result<(), String> {
        if !self.ticket_matches(ticket) { return Err("early read binding changed".into()); }
        let Some(planned) = self.calls.iter().find(|call| call.call_id == receipt.call.id) else {
            return Err("early read outside original batch".into());
        };
        if planned.tool_name != receipt.call.name
            || planned.effect_key.as_deref() != receipt.call.binding.as_ref().map(|b| b.effect_idempotency_key()).as_deref() {
            return Err("early read call identity changed".into());
        }
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed { return Err("early read binding is closed".into()); }
        if state.original_early_reads.get(&receipt.call.id).is_some_and(|prior| prior != &receipt) {
            return Err("conflicting early read carrier".into());
        }
        state.original_early_reads.insert(receipt.call.id.clone(), receipt);
        Ok(())
    }

    pub(crate) fn original_early_reads(&self) -> Result<Vec<crate::conversation::EarlyToolExecutionReceipt>, String> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.closed { return Err("original early read binding is still open".into()); }
        Ok(state.original_early_reads.values().cloned().collect())
    }

    /// Retire exactly the carrier set covered by the original lease-closure CAS.
    pub(crate) fn release_original_early_reads(
        &self, ticket: &NodeExecutionTicket,
        reads: &[crate::conversation::EarlyToolExecutionReceipt],
        sources: &[crate::runtime_event_store::ExpectedStreamRevision],
        receipt: &crate::execution_core::graph::ExecutionCommitReceipt,
    ) -> Result<(), String> {
        if !self.ticket_matches(ticket) || receipt.graph.id != ticket.graph_id
            || receipt.transaction.transaction_id.is_empty() || receipt.transaction.commit_cursor == 0 {
            return Err("early read release owner or receipt changed".into());
        }
        if sources.iter().any(|source| !receipt.transaction.stream_revisions.iter().any(|actual|
            actual.stream_id == source.stream_id && actual.expected_revision == source.expected_revision)) {
            return Err("early read release transaction omitted original source".into());
        }
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.closed || reads.len() != state.original_early_reads.len()
            || reads.iter().any(|read| state.original_early_reads.get(&read.call.id) != Some(read)) {
            return Err("early read release carrier changed".into());
        }
        let position = (receipt.transaction.transaction_id.clone(), receipt.transaction.commit_cursor);
        for read in reads { state.original_early_reads_released.insert(read.call.id.clone(), position.clone()); }
        Ok(())
    }

    pub(super) fn enter(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return false;
        }
        state.entered = true;
        true
    }

    pub(super) fn abandon_unadmitted(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.entered || !state.original_early_reads.is_empty() || !state.uncertain.is_empty() || !state.inherited_hook_pending.is_empty() || !state.refused.is_empty()
            || state.member_custody.values().any(|unit| !unit.is_settled())
        {
            return false;
        }
        state.closed = true;
        state.completed = None;
        true
    }

    pub(super) fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        state.bounded_controls.retain(|weak| {
            if let Some(control) = weak.upgrade() {
                crate::RuntimeTransactionCommitControl::request_cancel(control.as_ref());
                true
            } else { false }
        });
        if !state.entered && state.original_early_reads.is_empty() && state.uncertain.is_empty() && state.inherited_hook_pending.is_empty() && state.completed.is_none() {
            let mut calls = self.calls.clone();
            for call in &mut calls {
                if let Some(refusal) = state.refused.iter().find(|refusal|
                    refusal.call_id == call.call_id && refusal.refusal_event_id.is_some()) {
                    *call = refusal.clone();
                } else {
                    call.reason = "cancelled_before_first_poll".into();
                }
            }
            state.completed = Some(NoDispatchWitness {
                fact: NoDispatchFact {
                    ticket: self.ticket.clone(),
                    calls,
                    never_polled: true,
                    result_sha256: None,
                    historical_sources: Default::default(),
                    start: None,
                    bounded_owners: state.bounded_owners.clone(),
                    original_node: None,
                },
            });
        }
    }

    pub(crate) fn uncertain(&self, request: &RuntimeToolExecutionRequest) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.uncertain.contains(&request.tool_use_id) {
            state.uncertain.push(request.tool_use_id.clone());
        }
        state
            .refused
            .retain(|call| call.call_id != request.tool_use_id);
        state.completed = None;
    }

    /// The original physical owner closes admission under this same mutex.
    pub(crate) fn register_bounded_control(
        &self, ticket: &NodeExecutionTicket,
        control: &Arc<crate::conversation::MemberCommitControl>,
    ) -> Result<(), String> {
        if !self.ticket_matches(ticket) { return Err("bounded decision observation ticket differs".into()); }
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            crate::RuntimeTransactionCommitControl::request_cancel(control.as_ref());
        } else {
            state.bounded_controls.retain(|weak| weak.strong_count() > 0);
            state.bounded_controls.push(Arc::downgrade(control));
        }
        Ok(())
    }

    pub(super) fn hold_inherited_hook(&self, request: &RuntimeToolExecutionRequest) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.inherited_hook_pending.insert(request.tool_use_id.clone());
    }

    /// Keep the original physical binding while a packet-scope transaction is unresolved.
    pub(crate) fn hold_packet_scope(&self, request: &RuntimeToolExecutionRequest) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.packet_scope_pending.insert(request.tool_use_id.clone());
        if !self.calls.iter().any(|call| call.call_id == request.tool_use_id
            && call.effect_key.as_deref() == Some(request.idempotency_key.as_str())) {
            return Err("packet scope pending differs from original call".into());
        }
        Ok(())
    }
    pub(crate) fn hold_bounded_acquisition(
        &self, fact: Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>,
        receipt: Option<crate::execution_core::graph::CommittedCallRefusal>,
    ) -> Result<(), String> {
        let request = fact.request();
        let key = request.tool_use_id.clone();
        let matches_call = self.calls.iter().any(|call| call.call_id == key
            && call.effect_key.as_deref() == Some(request.idempotency_key.as_str()));
        let matches_receipt = receipt.as_ref().is_none_or(|proof| proof.owner_graph_id() == self.ticket.graph_id
            && proof.owner_node_id() == self.ticket.node_id && proof.owner_attempt() == self.ticket.attempt
            && proof.call_id() == key && proof.effect_key() == request.idempotency_key);
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let conflicts = state.bounded_acquisition_pending.get(&key).is_some_and(|(old, prior)|
            !Arc::ptr_eq(old, &fact) || prior.as_ref().zip(receipt.as_ref()).is_some_and(|(left, right)|
                left.transaction_id() != right.transaction_id() || left.event_id() != right.event_id()));
        state.packet_scope_pending.insert(key.clone());
        if !matches_call || !matches_receipt || conflicts {
            state.bounded_acquisition_conflicts.push((fact, receipt.map(Arc::new)));
            return Err("bounded acquisition original custody conflicts".into());
        }
        let retained_receipt = receipt.map(Arc::new).or_else(|| state.bounded_acquisition_pending.get(&key).and_then(|(_, prior)| prior.clone()));
        state.bounded_acquisition_pending.insert(key, (fact, retained_receipt));
        Ok(())
    }
    pub(crate) fn register_member_custody(
        &self, ticket: &NodeExecutionTicket, call_id: &str, index: usize,
        unit: Arc<crate::tool_host::OriginalMemberSettlement>,
    ) -> Result<(), String> {
        if !self.ticket_matches(ticket) || index >= 25 || !self.calls.iter().any(|call| call.call_id == call_id) {
            return Err("original member custody identity mismatch".into());
        }
        let mut state = self.state.lock().map_err(|_| "original member custody unavailable")?;
        if state.closed { return Err("original member custody already closed".into()); }
        let key = (call_id.to_owned(), index);
        match state.member_custody.get(&key) {
            Some(previous) if Arc::ptr_eq(previous, &unit) => Ok(()),
            Some(_) => Err("original member custody conflicts".into()),
            None => { state.member_custody.insert(key, unit); Ok(()) }
        }
    }
    /// The drained original observation, not an empty caller list, proves which
    /// member units were actually registered. Registration is closed here.
    pub(crate) fn verify_closed_member_custody(
        &self, ticket: &NodeExecutionTicket, call_id: &str,
        expected: &[(usize, Arc<crate::tool_host::OriginalMemberSettlement>)],
    ) -> Result<(), String> {
        if !self.ticket_matches(ticket) || !self.calls.iter().any(|call| call.call_id == call_id) {
            return Err("original physical member call differs".into());
        }
        let state = self.state.lock().map_err(|_| "original physical member custody unavailable")?;
        if !state.closed { return Err("original physical member registration remains open".into()); }
        let actual = state.member_custody.iter().filter(|((call, _), _)| call == call_id)
            .map(|((_, index), unit)| (*index, unit)).collect::<Vec<_>>();
        if actual.len() != expected.len() || actual.iter().zip(expected).any(|((index, unit), (wanted, original))|
            index != wanted || !Arc::ptr_eq(unit, original)) {
            return Err("original physical member registration differs".into());
        }
        Ok(())
    }
    pub(crate) fn member_custody_is_unresolved(&self) -> bool {
        match self.state.lock() {
            Ok(state) => state.member_custody.values().any(|unit| !unit.is_settled()),
            Err(_) => true,
        }
    }
    pub(crate) fn packet_scope_is_unresolved(&self) -> bool {
        !self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).packet_scope_pending.is_empty()
    }
    pub(crate) fn original_custody_allows_forget(&self, publication_completed: bool) -> bool {
        let Ok(state) = self.state.lock() else { return false; };
        state.original_early_reads.keys().all(|id| state.original_early_reads_released.contains_key(id))
            && state.packet_scope_pending.is_empty() && (state.packet_scope_settled.is_empty() || publication_completed)
            && state.member_custody.values().all(|unit| unit.is_settled())
    }
    /// Release only after the original commit owner authenticated this exact receipt.
    pub(crate) fn settle_packet_scope(&self, proof: &crate::execution_core::graph::CommittedCallRefusal) -> Result<(), String> {
        if proof.owner_graph_id() != self.ticket.graph_id || proof.owner_node_id() != self.ticket.node_id
            || proof.owner_attempt() != self.ticket.attempt
            || !self.calls.iter().any(|call| call.call_id == proof.call_id()
                && call.effect_key.as_deref() == Some(proof.effect_key())) {
            return Err("packet scope receipt belongs to another original call".into());
        }
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let identity = (proof.transaction_id().to_string(), proof.event_id().to_string());
        if !state.packet_scope_pending.remove(proof.call_id()) {
            return if state.packet_scope_settled.get(proof.call_id()) == Some(&identity) { Ok(()) }
                else { Err("packet scope original pending owner missing".into()) };
        }
        if state.packet_scope_settled.get(proof.call_id()).is_some_and(|previous| previous != &identity) {
            state.packet_scope_pending.insert(proof.call_id().to_string());
            return Err("packet scope committed receipt changed".into());
        }
        if state.bounded_acquisition_conflicts.iter().any(|(fact, _)| fact.request().tool_use_id == proof.call_id()) {
            state.packet_scope_pending.insert(proof.call_id().to_string());
            return Err("bounded acquisition custody is conflicted".into());
        }
        if let Some((_, prior)) = state.bounded_acquisition_pending.get(proof.call_id()) {
            if prior.as_ref().is_some_and(|receipt| receipt.transaction_id() != proof.transaction_id() || receipt.event_id() != proof.event_id()) {
                state.packet_scope_pending.insert(proof.call_id().to_string());
                return Err("bounded acquisition original receipt changed".into());
            }
        }
        state.bounded_acquisition_pending.remove(proof.call_id());
        state.packet_scope_settled.insert(proof.call_id().to_string(), identity);
        Ok(())
    }

    pub(crate) fn refuse(&self, call_id: &str, reason: String) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.uncertain.iter().any(|id| id == call_id) || state.inherited_hook_pending.contains(call_id) || reason.is_empty() {
            return;
        }
        if let Some(original) = self.calls.iter().find(|call| call.call_id == call_id) {
            if !state.refused.iter().any(|call| call.call_id == call_id) {
                let mut refusal = original.clone();
                refusal.reason = reason;
                state.refused.push(refusal);
            }
        }
    }

    /// The named reason this node's admission recorded for one call, when the
    /// call was refused before dispatch. Reading it back is the only way a
    /// downstream consumer can report *which* admission conjunct refused the
    /// call instead of a generic "admission missing".
    pub(crate) fn refusal_reason(&self, call_id: &str) -> Option<String> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .refused
            .iter()
            .find(|call| call.call_id == call_id)
            .map(|call| call.reason.clone())
            .filter(|reason| !reason.is_empty())
    }

    /// Retain the original owner's durable call decision for graph closure.
    /// The graph commit re-reads and fences the decision before publishing.
    pub(crate) fn refuse_committed(
        &self,
        receipt: &crate::execution_core::graph::CommittedCallRefusal,
    ) -> Result<(), String> {
        if receipt.owner_graph_id() != self.ticket.graph_id
            || receipt.owner_node_id() != self.ticket.node_id
            || receipt.owner_attempt() != self.ticket.attempt {
            return Err("committed refusal belongs to a different ticket".into());
        }
        let original = self.calls.iter().find(|call| call.call_id == receipt.call_id()
            && call.effect_key.as_deref() == Some(receipt.effect_key()))
            .ok_or("committed refusal does not match original call")?;
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.uncertain.iter().any(|id| id == receipt.call_id()) || state.inherited_hook_pending.contains(receipt.call_id()) {
            return Err("committed refusal conflicts with uncertain execution".into());
        }
        if let Some(previous) = state.refused.iter().find(|call| call.call_id == receipt.call_id()) {
            return if previous.refusal_event_id.as_deref() == Some(receipt.event_id()) { Ok(()) }
                else { Err("committed refusal conflicts with an existing observation".into()) };
        }
        if state.closed || state.completed.is_some() {
            return Err("committed refusal arrived after NoDispatch closure".into());
        }
        let mut call = original.clone();
        call.reason = "committed_call_refusal".into();
        call.refusal_event_id = Some(receipt.event_id().to_string());
        state.refused.push(call);
        Ok(())
    }

    /// Attach the private acquisition owner to this exact committed decision.
    /// The owner is never reconstructed from an event payload.
    pub(crate) fn refuse_bounded_committed(
        &self,
        failure: Arc<crate::bound_tool_batch::OriginalBoundedAcquisitionFailure>,
        receipt: &crate::execution_core::graph::CommittedCallRefusal,
        child: Option<Arc<super::scoped::OriginalAgentChildCall>>,
    ) -> Result<(), String> {
        let request = failure.request();
        if request.tool_use_id != receipt.call_id() || request.idempotency_key != receipt.effect_key() {
            return Err("bounded acquisition owner differs from committed call".into());
        }
        if is_bound_ticket(&self.ticket) {
            let child = child.as_ref().ok_or("original Agent child owner missing")?;
            if !failure.belongs_to(&child.store) || child.request.as_ref() != request
                || !child.request.tool_progress.same_original_channels(&request.tool_progress) {
                return Err("bounded acquisition original Agent child differs".into());
            }
        } else if child.is_some() {
            return Err("non-Agent call cannot borrow an Agent child owner".into());
        }
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.completed.is_some() || state.uncertain.contains(&request.tool_use_id) {
            return Err("bounded acquisition owner arrived after observation closed".into());
        }
        if receipt.owner_graph_id() != self.ticket.graph_id || receipt.owner_node_id() != self.ticket.node_id
            || receipt.owner_attempt() != self.ticket.attempt {
            return Err("bounded acquisition receipt belongs to another ticket".into());
        }
        let original_call = self.calls.iter().find(|call| call.call_id == receipt.call_id()
            && call.effect_key.as_deref() == Some(receipt.effect_key()))
            .ok_or("bounded acquisition receipt differs from original ticket")?;
        let original = OriginalBoundedWitness { failure, receipt: Arc::new(receipt.clone()), child };
        if state.bounded_owners.get(receipt.call_id()).is_some_and(|old| old != &original) {
            return Err("bounded acquisition owner conflicts with prior call".into());
        }
        if let Some(previous) = state.refused.iter().find(|call| call.call_id == receipt.call_id()) {
            if previous.refusal_event_id.as_deref() != Some(receipt.event_id()) {
                return Err("bounded acquisition receipt conflicts with prior observation".into());
            }
        } else {
            let mut call = original_call.clone();
            call.reason = "committed_call_refusal".into();
            call.refusal_event_id = Some(receipt.event_id().to_owned());
            state.refused.push(call);
        }
        state.inherited_hook_pending.remove(receipt.call_id());
        state.bounded_owners.insert(receipt.call_id().to_owned(), original);
        Ok(())
    }

    fn original_bounded_owner(&self, call_id: &str) -> Option<OriginalBoundedWitness> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
            .bounded_owners.get(call_id).cloned()
    }

    pub(crate) fn refusal(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Option<RuntimeToolExecutionOutcome> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let refusal = state.refused.iter().find(|call| {
            call.call_id == request.tool_use_id
                && call.request_sha256.as_ref().is_some_and(|digest| {
                    no_dispatch_digest(request).is_ok_and(|actual| &actual == digest)
                })
                && !state.uncertain.contains(&call.call_id)
        })?;
        let error = if let Some(owner) = state.bounded_owners.get(&request.tool_use_id) {
            if owner.failure.request() != request
                || !owner.failure.request().tool_progress.same_original_channels(&request.tool_progress)
                || refusal.refusal_event_id.as_deref() != Some(owner.receipt.event_id()) {
                return None;
            }
            owner.failure.original_error().to_owned()
        } else {
            refusal.reason.clone()
        };
        Some(RuntimeToolExecutionOutcome { pending_agent_batch: None,
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Failed,
            category: request.category,
            output: None,
            error: Some(error),
            evidence_ref: String::new(),
            observed_evidence: Vec::new(),
        })
    }

    pub(super) fn finish(
        &self,
        outcome: &mut NodeExecutionOutcome,
    ) -> Result<(), serde_json::Error> {
        #[cfg(any(test, feature = "test-fixtures"))]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            let (entered, closed, refused_count, uncertain_count, calls) = {
                let snapshot = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                let calls = self.calls.iter().take(32).map(|call| {
                    let refused = snapshot.refused.iter().find(|entry| entry.call_id == call.call_id);
                    (call.call_id.chars().take(64).collect::<String>(), refused.is_some(),
                     refused.is_some_and(|entry| entry.refusal_event_id.is_some()),
                     snapshot.uncertain.contains(&call.call_id), call.effect_key.is_some())
                }).collect::<Vec<_>>();
                (snapshot.entered, snapshot.closed, snapshot.refused.len(), snapshot.uncertain.len(), calls)
            };
            let graph = self.ticket.graph_id.chars().take(64).collect::<String>();
            let node = self.ticket.node_id.chars().take(64).collect::<String>();
            crate::execution_core::graph::k05_trace_emit(format!("r622_toolbatch phase=witness_snapshot graph={:?} node={:?} attempt={} entered={} closed={} original_count={} refused_count={} uncertain_count={} capped={}", graph, node, self.ticket.attempt, entered, closed, self.calls.len(), refused_count, uncertain_count, self.calls.len() > 32));
            for (call, refused, committed, uncertain, key) in calls {
                crate::execution_core::graph::k05_trace_emit(format!("r622_toolbatch phase=witness_call graph={:?} node={:?} attempt={} call={:?} refused={} committed_refusal={} uncertain={} effect_key_present={}", graph, node, self.ticket.attempt, call, refused, committed, uncertain, key));
            }
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.refused.is_empty() {
            return Ok(());
        }
        let witness = NoDispatchWitness {
            fact: NoDispatchFact {
                ticket: self.ticket.clone(),
                calls: state.refused.clone(),
                never_polled: false,
                result_sha256: Some(no_dispatch_digest(&outcome.result)?),
                historical_sources: Default::default(),
                start: None,
                bounded_owners: state.bounded_owners.clone(),
                original_node: None,
            },
        };
        state.completed = Some(witness.clone());
        outcome.no_dispatch = Some(witness);
        Ok(())
    }

    pub(crate) fn finalize_duration(
        &self,
        ticket: &NodeExecutionTicket,
        outcome: &mut NodeExecutionOutcome,
        actual_duration_ms: u64,
    ) -> Result<(), String> {
        if !self.ticket_matches(ticket) {
            return Err("NoDispatch duration original ticket mismatch".into());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(original) = outcome.no_dispatch.as_ref() else {
            if state.completed.is_some() {
                return Err("NoDispatch duration lost original witness".into());
            }
            outcome.result.usage.duration_ms = outcome
                .result
                .usage
                .duration_ms
                .max(actual_duration_ms.max(1));
            return Ok(());
        };
        original.authenticate(ticket, Some(&outcome.result))?;
        if state.completed.as_ref() != Some(original)
            || original
                .fact
                .calls
                .iter()
                .any(|call| state.uncertain.contains(&call.call_id))
        {
            return Err("NoDispatch duration original observation changed".into());
        }
        let mut result = outcome.result.clone();
        result.usage.duration_ms = result.usage.duration_ms.max(actual_duration_ms.max(1));
        let mut finalized = original.clone();
        finalized.fact.result_sha256 =
            Some(no_dispatch_digest(&result).map_err(|error| error.to_string())?);
        state.completed = Some(finalized.clone());
        outcome.result = result;
        outcome.no_dispatch = Some(finalized);
        Ok(())
    }

    pub(crate) fn witness(&self) -> Option<NoDispatchWitness> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .completed
            .clone()
    }
}

impl NoDispatchWitness {
    pub(crate) fn fact(&self) -> &NoDispatchFact {
        &self.fact
    }

    pub(crate) fn authenticate(
        &self,
        ticket: &NodeExecutionTicket,
        result: Option<&ExecutionNodeResult>,
    ) -> Result<(), String> {
        self.fact.validate_identity(ticket)?;
        if self.fact.never_polled {
            if result.is_some() || self.fact.result_sha256.is_some() {
                return Err("NeverPolled cannot claim a node result".into());
            }
        } else if let Some(result) = result {
            if self.fact.result_sha256.as_ref()
                != Some(&no_dispatch_digest(result).map_err(|error| error.to_string())?)
            {
                return Err("NoDispatch sealed result mismatch".into());
            }
        }
        Ok(())
    }
}

impl NoDispatchFact {
    pub(crate) fn index_stream(&self) -> Result<String, String> {
        if is_bound_ticket(&self.ticket) {
            let bound = decode_bound_agent_tool(&self.ticket.payload_ref)
                .map_err(|error| error.to_string())?;
            let parent = bound
                .request
                .parent_execution
                .as_ref()
                .ok_or("NoDispatch parent missing")?;
            let attempt = bound
                .request
                .parent_execution_attempt
                .ok_or("NoDispatch parent attempt missing")?;
            Ok(format!(
                "execution-agent-receipts:{}:{}:{attempt}",
                parent.execution_id, parent.node_id
            ))
        } else {
            Ok(format!(
                "execution-agent-receipts:{}:{}:{}",
                self.ticket.graph_id, self.ticket.node_id, self.ticket.attempt
            ))
        }
    }

    pub(crate) fn validate_identity(&self, ticket: &NodeExecutionTicket) -> Result<(), String> {
        if self.ticket != *ticket || self.calls.is_empty() {
            return Err("NoDispatch original ticket or call cardinality mismatch".into());
        }
        let originals = no_dispatch_calls(ticket).map_err(|error| error.to_string())?;
        let mut seen = std::collections::BTreeSet::new();
        for call in &self.calls {
            let mut identity = call.clone();
            identity.reason.clear();
            identity.refusal_event_id = None;
            if call.reason.is_empty()
                || !seen.insert(&call.call_id)
                || !originals.contains(&identity)
            {
                return Err("NoDispatch original call identity mismatch".into());
            }
        }
        if self.never_polled && self.calls.len() != originals.len() {
            return Err("NeverPolled must close every original call".into());
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BoundAgentTool {
    kind: String,
    pub(crate) agent_id: String,
    pub(crate) run_id: String,
    pub(crate) request: RuntimeToolExecutionRequest,
}

fn bound_carrier_error(reason: &str) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::InvalidData, reason))
}

fn validate_bound_agent_tool(bound: &BoundAgentTool) -> Result<(), serde_json::Error> {
    if bound.kind != "agent_tool_batch.v1"
        || bound.agent_id.trim().is_empty()
        || bound.run_id.trim().is_empty()
        || bound
            .request
            .session_id
            .as_deref()
            .is_none_or(|session| session.trim().is_empty())
        || bound
            .request
            .parent_execution
            .as_ref()
            .is_none_or(|parent| {
                parent.execution_id.trim().is_empty() || parent.node_id.trim().is_empty()
            })
    {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch typed identity invalid",
        ));
    }
    Ok(())
}

pub(crate) fn encode_bound_agent_tool(
    agent_id: &str,
    run_id: &str,
    request: RuntimeToolExecutionRequest,
) -> Result<String, serde_json::Error> {
    let bound = BoundAgentTool {
        kind: "agent_tool_batch.v1".into(),
        agent_id: agent_id.into(),
        run_id: run_id.into(),
        request,
    };
    validate_bound_agent_tool(&bound)?;
    crate::step_snapshot::encode_persisted_bound_agent_tool(&serde_json::to_value(bound)?)
}

pub(crate) fn decode_bound_agent_tool(payload: &str) -> Result<BoundAgentTool, serde_json::Error> {
    let value = crate::step_snapshot::decode_persisted_bound_agent_tool_value(payload)?;
    let bound: BoundAgentTool = serde_json::from_value(value)?;
    validate_bound_agent_tool(&bound)?;
    Ok(bound)
}

pub(crate) fn is_bound_ticket(ticket: &NodeExecutionTicket) -> bool {
    bound_payload_identity_shape(&ticket.graph_id, &ticket.node_id)
}

fn bound_payload_identity_shape(graph_id: &str, node_id: &str) -> bool {
    let Some(digest) = graph_id.strip_prefix("agent-tool-batch:") else {
        return false;
    };
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && node_id.strip_prefix("agent-tool:") == Some(digest)
}

pub(crate) fn decode_bound_agent_tool_node(
    graph_id: &str,
    node: &ExecutionNodeSpec,
) -> Result<BoundAgentTool, serde_json::Error> {
    if node.kind != ExecutionNodeKind::ToolBatch
        || node.executor_kind != "tool_batch"
        || !bound_payload_identity_shape(graph_id, &node.id)
    {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch stored payload hash mismatch",
        ));
    }
    let bound = decode_bound_agent_tool(&node.payload_ref)?;
    let digest = format!("{:x}", Sha256::digest(node.payload_ref.as_bytes()));
    if graph_id != format!("agent-tool-batch:{digest}") || node.id != format!("agent-tool:{digest}")
    {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch stored payload hash mismatch",
        ));
    }
    Ok(bound)
}

pub(crate) fn decode_bound_agent_tool_graph(
    graph: &ExecutionGraph,
) -> Result<BoundAgentTool, serde_json::Error> {
    if graph.nodes.len() != 1 {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch requires one original node",
        ));
    }
    let bound = decode_bound_agent_tool_node(&graph.id, &graph.nodes[0])?;
    if graph.parent_execution != bound.request.parent_execution || graph.parent_execution.is_none()
    {
        return Err(bound_carrier_error(
            "bound Agent ToolBatch original parent mismatch",
        ));
    }
    Ok(bound)
}

pub(crate) fn is_bound_agent_tool_graph(graph: &ExecutionGraph) -> bool {
    decode_bound_agent_tool_graph(graph).is_ok()
}

fn requires_original_agent_child(request: &RuntimeToolExecutionRequest) -> bool {
    request.tool_name == "agent_action_batch"
        || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&request.tool_name.as_str())
        || request.compiled_invocation.as_ref().is_none_or(|invocation|
            invocation.effect.effect_kind != harness_contract::tool::ToolEffectKind::Read)
}

pub(crate) struct AgentToolBatchDispatcher {
    services: Weak<RuntimeServices>,
    agent_id: String,
    run_id: String,
}
impl AgentToolBatchDispatcher {
    pub(crate) fn new(
        services: &Arc<RuntimeServices>,
        packet: &harness_contract::agent::AgentTaskPacket,
    ) -> Self {
        Self {
            services: Arc::downgrade(services),
            agent_id: packet.agent_id().into(),
            run_id: packet.run_id().into(),
        }
    }
    pub(crate) fn execute(
        &self,
        request: RuntimeToolExecutionRequest,
    ) -> futures::future::BoxFuture<'_, Result<RuntimeToolExecutionOutcome, ToolError>> {
        // Tool dispatch is nested inside model/graph futures. Keep its durable
        // registration state off callers' stacks, including concurrent replay.
        Box::pin(async move {
            let original_progress = request.tool_progress.clone();
            let result: Result<RuntimeToolExecutionOutcome, ToolError> = async move {
            let services = self
                .services
                .upgrade()
                .ok_or_else(|| ToolError::new("Agent ToolBatch Runtime is unavailable"))?;
            let parent = request
                .parent_execution
                .clone()
                .ok_or_else(|| ToolError::new("Agent ToolBatch omitted parent execution"))?;
            // `parent.execution_id` is the id the Agent was admitted under, which is
            // an agent-run id (`run:<graph_id>`). The store keys on the graph id, so
            // looking the run id up there always fails - and every agent action
            // runs through this load, so every one of them would report a graph
            // that exists as missing.
            let parent_graph = services
                .graph_state_store()
                .load_async(crate::agentic::execution::dispatch::execution_graph_id(
                    &parent.execution_id,
                ))
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
            let preflight_request = request.clone();
            let payload = encode_bound_agent_tool(&self.agent_id, &self.run_id, request)
                .map_err(|e| ToolError::new(e.to_string()))?;
            let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
            let mut graph = ExecutionGraph::new("Runtime-bound Agent tool invocation");
            graph.id = format!("agent-tool-batch:{digest}");
            graph.parent_execution = Some(parent);
            graph.lineage = parent_graph.lineage.clone();
            graph.service_class = parent_graph.service_class;
            let mut node =
                ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
            node.id = format!("agent-tool:{digest}");
            node.idempotency_key = node.id.clone();
            let node_id = node.id.clone();
            graph.nodes.push(node);
            let graph_id = graph.id.clone();
            match services.graph_state_store().load_async(&graph_id).await {
                Ok(_) => {}
                Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound {
                    ..
                }) => {
                    if let Some(invocation) = &preflight_request.compiled_invocation {
                        services
                            .commit_service()
                            .preflight_fresh_bound_tool(&preflight_request, &invocation.effect)?;
                    }
                }
                Err(error) => return Err(ToolError::new(error.to_string())),
            }
            let supervisor = services.execution_supervisor();
            if let Err(error) = supervisor.register_graph(graph.clone()).await {
                use crate::execution_core::graph::{ExecutionCommitError, ExecutionRunnerError};
                if !matches!(
                    &error,
                    ExecutionRunnerError::Commit(
                        ExecutionCommitError::EventStore(_)
                            | ExecutionCommitError::StaleRevision { .. }
                    )
                ) {
                    return Err(ToolError::new(error.to_string()));
                }
                // Registration races/retries may only attach to the exact durable
                // winner. Never hide policy, validation, or identity conflicts.
                let existing = services
                    .graph_state_store()
                    .load_async(&graph_id)
                    .await
                    .map_err(|_| ToolError::new(error.to_string()))?;
                if existing.nodes != graph.nodes
                    || existing.parent_execution != graph.parent_execution
                    || existing.lineage != graph.lineage
                    || existing.service_class != graph.service_class
                {
                    return Err(ToolError::new("Agent ToolBatch durable identity conflict"));
                }
            }
            let stop = crate::bound_tool_batch::current_tool_stop();
            let _original_child = if requires_original_agent_child(&preflight_request) {
                let attempt = preflight_request.parent_execution_attempt.ok_or_else(|| ToolError::unknown("original Agent attempt missing"))?;
                let parent = preflight_request.parent_execution.as_ref().ok_or_else(|| ToolError::unknown("original Agent parent missing"))?;
                let state = services.graph_state_store();
                let binding = state.current_binding(&parent_graph, &parent.node_id, attempt).map_err(|e| ToolError::unknown(e.to_string()))?;
                let (parent_ticket, spec) = state.binding_start_ticket(&parent_graph, &parent.node_id, &binding).map_err(|e| ToolError::unknown(e.to_string()))?;
                let packet: harness_contract::agent::AgentTaskPacket = serde_json::from_str(&spec.payload_ref).map_err(|e| ToolError::unknown(e.to_string()))?;
                if spec.kind != ExecutionNodeKind::AgentTask || packet.graph_id() != parent.execution_id
                    || packet.node_id() != parent.node_id || packet.attempt != attempt
                    || packet.run_id() != self.run_id || packet.agent_id() != self.agent_id {
                    return Err(ToolError::unknown("original Agent child parent ticket mismatch"));
                }
                crate::execution_core::graph::validate_node_ticket_start(services.event_store(), &parent_ticket, None)
                    .map_err(|e| ToolError::unknown(e.to_string()))?;
                let process_members = if preflight_request.tool_name == "agent_action_batch" {
                    crate::agent_in_process_worker::current_process_member_custody(&preflight_request)
                        .map_err(ToolError::unknown)?
                } else { None };
                Some(services.tool_batch_executor().retain_original_agent_child(
                    &graph_id, &node_id, &graph.nodes[0].payload_ref, Arc::new(preflight_request.clone()), parent_ticket,
                    packet, Arc::clone(services.event_store()),
                    crate::conversation::tool_plane::current_original_single_capture(), stop.clone(), process_members,
                ).map_err(ToolError::unknown)?)
            } else { None };
            if let Some(stop) = &stop {
                let mut child = stop
                    .child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let identity = (graph_id.clone(), digest.clone());
                if child.as_ref().is_some_and(|prior| prior != &identity) {
                    return Err(ToolError::new(
                        "original call cannot bind a foreign child graph",
                    ));
                }
                *child = Some(identity);
                stop.child_drained
                    .store(false, std::sync::atomic::Ordering::Release);
                #[cfg(test)]
                if stop.stop_after_child_registration {
                    stop.request();
                }
            }
            let drive = async {
                if let Some(stop) = &stop {
                    stop.check()?;
                }
                supervisor
                    .drive_registered(&graph_id)
                    .await
                    .map_err(ToolError::from_original_child_drive)?;
                supervisor
                    .wait_for_terminal(&graph_id)
                    .await
                    .map_err(ToolError::from_original_child_drive)?;
                Ok::<_, ToolError>(())
            };
            tokio::pin!(drive);
            let mut cancellation_failure: Option<ToolError> = None;
            let driven = if let Some(stop) = &stop {
                if stop.check().is_err() {
                    let original = ToolError::new("original tool call stopped before child drive");
                    match services.cancel_execution_tree(
                        &graph_id, "original Process call stopped after registration",
                    ).await {
                        Ok(_) => Err(original),
                        Err(error) => {
                            let detail: String = error.to_string().chars().take(512).collect();
                            tracing::warn!(graph_id = %graph_id, node_id = %node_id,
                                attempt = ?Option::<u32>::None, call_id = %preflight_request.tool_use_id,
                                phase = "cancel_before_drive", error = %detail,
                                "original Agent child cancellation failed");
                            Err(original.with_unresolved_cause("cancel_before_drive", ToolError::from_original_child_cancel(error), None))
                        }
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => {
                            // Keep the original driver alive while the original
                            // supervisor cancels this exact child, never parent.
                            let (cancelled, driven) = tokio::join!(
                                services.cancel_execution_tree(&graph_id,"original Process call stopped"),
                                &mut drive);
                            match (cancelled, driven) {
                                (Ok(_), driven) => driven,
                                (Err(close), driven) => {
                                    let detail: String = close.to_string().chars().take(512).collect();
                                    tracing::warn!(graph_id = %graph_id, node_id = %node_id,
                                        attempt = ?Option::<u32>::None, call_id = %preflight_request.tool_use_id,
                                        phase = "cancel", error = %detail,
                                        "original Agent child cancellation failed");
                                    let cancellation = ToolError::from_original_child_cancel(close);
                                    match driven {
                                        Err(original) => Err(original.with_unresolved_cause(
                                            "cancel", cancellation, None,
                                        )),
                                        // Keep the original cancellation error separately;
                                        // a completed child still needs close and result checks.
                                        Ok(()) => {
                                            cancellation_failure = Some(cancellation);
                                            Ok(())
                                        },
                                    }
                                },
                            }
                        },
                        result = &mut drive => result,
                    }
                }
            } else {
                drive.await
            };
            let original_drive_failure = driven.as_ref().err().cloned();
            if let Some(error) = &original_drive_failure {
                let detail: String = error.to_string().chars().take(512).collect();
                tracing::warn!(graph_id = %graph_id, node_id = %node_id,
                    attempt = ?Option::<u32>::None, call_id = %preflight_request.tool_use_id,
                    phase = "drive", error = %detail, "original Agent child did not complete");
            }
            let pending_from_owner = || {
                (!preflight_request.tool_progress.original_members().is_empty()
                    || preflight_request.tool_progress.has_original_member_stop()).then(||
                    crate::PendingAgentBatch::new(crate::PendingAgentBatchValue::RecordedSink(
                        preflight_request.tool_progress.clone(),
                    )))
            };
            let phase_failure = |phase: &str, attempt: Option<u32>, error: ToolError| {
                let detail: String = error.to_string().chars().take(512).collect();
                tracing::warn!(graph_id = %graph_id, node_id = %node_id, ?attempt,
                    call_id = %preflight_request.tool_use_id, phase, error = %detail,
                    "original Agent child return or close failed");
                let failure = match &original_drive_failure {
                    Some(original) => original.clone().with_unresolved_cause(
                        phase, error, pending_from_owner(),
                    ),
                    None => error.with_unresolved_context(
                        format!("child {phase} remains unresolved"), pending_from_owner(),
                    ),
                };
                match &cancellation_failure {
                    Some(cancel) => failure.with_unresolved_cause("cancel", cancel.clone(), None),
                    None => failure,
                }
            };
            let settled = services
                .graph_state_store()
                .load_async(&graph_id)
                .await
                .map_err(|error| phase_failure("load", None, ToolError::new(error.to_string())))?;
            if let Some(error) = &original_drive_failure {
                let detail: String = error.to_string().chars().take(512).collect();
                tracing::warn!(graph_id = %graph_id, node_id = %node_id,
                    attempt = ?settled.recovery_cursor.node_attempts.get(&node_id),
                    call_id = %preflight_request.tool_use_id, phase = "drive_bound",
                    error = %detail, "original Agent child drive failure at observed attempt");
            }
            if settled.nodes != graph.nodes || settled.parent_execution != graph.parent_execution {
                return Err(phase_failure("binding", None, ToolError::new(
                    "original Agent child binding changed during drain",
                )));
            }
            if let Some(attempt) = settled
                .recovery_cursor
                .node_attempts
                .get(&node_id)
                .filter(|attempt| **attempt > 0)
            {
                let node = graph
                    .nodes
                    .first()
                    .ok_or_else(|| phase_failure("node", Some(*attempt), ToolError::new("original Agent child node missing")))?;
                let ticket = NodeExecutionTicket {
                    graph_id: graph_id.clone(),
                    node_id: node.id.clone(),
                    executor_kind: node.executor_kind.clone(),
                    service_class: graph.service_class,
                    attempt: *attempt,
                    idempotency_key: node.idempotency_key.clone(),
                    payload_ref: node.payload_ref.clone(),
                };
                services
                    .tool_batch_executor()
                    .close_and_drain(&ticket, false)
                    .await
                    .map_err(|error| phase_failure("close_and_drain", Some(*attempt), ToolError::from_original_child_close(error)))?;
                if let Some(stop) = &stop {
                    let no_dispatch = services
                        .tool_batch_executor()
                        .binding_observation(&ticket)
                        .and_then(|observation| observation.witness())
                        .is_some_and(|witness| witness.authenticate(&ticket, None).is_ok());
                    let receipted = if let Some(invocation) = &preflight_request.compiled_invocation
                    {
                        services
                            .commit_service()
                            .completed_tool_effect(&preflight_request, &invocation.effect)
                            .map_err(|error| phase_failure("receipt", Some(*attempt), ToolError::from_original_child_receipt(error)))?
                            .is_some()
                    } else {
                        false
                    };
                    stop.effect_settled.store(
                        no_dispatch || receipted,
                        std::sync::atomic::Ordering::Release,
                    );
                }
                // Only after the original driver and terminal publication have
                // succeeded may the retained owner release its call handle.
                if driven.is_ok() && settled.node_statuses.get(&node_id).is_some_and(|status| status.is_terminal()) {
                    services.tool_batch_executor().forget_closed_binding(&ticket);
                }
            } else if let Some(stop) = &stop {
                if stop.check().is_err() {
                    supervisor
                        .confirm_cancelled_bound_child_never_admitted(&graph)
                        .await
                        .map_err(|error| phase_failure("confirm_unadmitted", None, ToolError::new(error.to_string())))?;
                    stop.effect_settled
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
            if let Some(stop) = &stop {
                if !settled.nodes.iter().all(|node| {
                    settled
                        .node_statuses
                        .get(&node.id)
                        .is_some_and(|state| state.is_terminal())
                }) {
                    return Err(phase_failure("terminal", settled.recovery_cursor.node_attempts.get(&node_id).copied(), ToolError::new(
                        "original Agent child is not terminal after drain",
                    )));
                }
                stop.child_drained
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            // The original live request is held by the existing child owner.
            // Its typed members are not reconstructed from the graph payload.
            let members = preflight_request.tool_progress.original_members();
            let pending = (!members.is_empty() || preflight_request.tool_progress.has_original_member_stop()).then(||
                crate::PendingAgentBatch::new(crate::PendingAgentBatchValue::RecordedSink(preflight_request.tool_progress.clone())));
            if let Err(error) = driven {
                return Err(error.with_unresolved_context("original child drive failed", pending));
            }
            let graph = services
                .graph_state_store()
                .load_async(&graph_id)
                .await
                .map_err(|error| phase_failure("result_load", settled.recovery_cursor.node_attempts.get(&node_id).copied(), ToolError::new(error.to_string())))?;
            let result = graph
                .node_results
                .get(&node_id)
                .and_then(|result| result.result_ref.as_deref())
                .and_then(|value| value.strip_prefix("agent-tool-outcome:"))
                .ok_or_else(|| {
                    let message = graph.node_results.get(&node_id)
                        .and_then(|result| result.failure.as_ref())
                        .map(|failure| failure.message.clone())
                        .unwrap_or_else(|| "Agent ToolBatch has no committed tool outcome".into());
                    phase_failure("node_result", settled.recovery_cursor.node_attempts.get(&node_id).copied(), ToolError::new(message))
                })?;
            let mut outcome: RuntimeToolExecutionOutcome = serde_json::from_str(result)
                .map_err(|error| phase_failure("decode_result", settled.recovery_cursor.node_attempts.get(&node_id).copied(), ToolError::new(error.to_string())))?;
            // The serialized result carries no in-process authority; reattach
            // only the same live owner's carrier retained before graph entry.
            outcome.pending_agent_batch = pending;
            if let Some(cancel) = &cancellation_failure {
                let detail: String = cancel.to_string().chars().take(512).collect();
                tracing::warn!(graph_id = %graph_id, node_id = %node_id,
                    attempt = ?settled.recovery_cursor.node_attempts.get(&node_id),
                    call_id = %preflight_request.tool_use_id, phase = "cancel_reconciled",
                    error = %detail, "child closed and its original result was retained despite cancellation failure");
            }
            Ok(outcome)
            }.await;
            result.map_err(|error| {
                let pending = error.pending_agent_batch().cloned().or_else(|| {
                    (!original_progress.original_members().is_empty() || original_progress.has_original_member_stop()).then(||
                        crate::PendingAgentBatch::new(
                            crate::PendingAgentBatchValue::RecordedSink(original_progress)))
                });
                if pending.is_some() {
                    error.with_unresolved_context("original child carrier retained", pending)
                } else { error }
            })
        })
    }
}

pub(crate) struct AgentToolBatchResolver {
    services: Weak<RuntimeServices>,
}
impl AgentToolBatchResolver {
    pub(crate) fn install(services: &Arc<RuntimeServices>) {
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(Self {
                services: Arc::downgrade(services),
            }));
    }
}
impl ScopedNodeBackendResolver for AgentToolBatchResolver {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        if ticket.executor_kind != "tool_batch"
            || !bound_payload_identity_shape(&ticket.graph_id, &ticket.node_id)
        {
            return None;
        }
        let bound = decode_bound_agent_tool(&ticket.payload_ref).ok()?;
        let digest = format!("{:x}", Sha256::digest(ticket.payload_ref.as_bytes()));
        if ticket.graph_id != format!("agent-tool-batch:{digest}")
            || ticket.node_id != format!("agent-tool:{digest}")
        {
            return None;
        }
        self.services.upgrade()?;
        Some(Arc::new(AgentToolBatchBackend {
            services: self.services.clone(),
            bound,
            stop: Default::default(),
        }))
    }
}
struct AgentToolBatchBackend {
    services: std::sync::Weak<RuntimeServices>,
    bound: BoundAgentTool,
    stop: crate::bound_tool_batch::BoundToolStop,
}

fn agent_tool_outcome(
    outcome: &RuntimeToolExecutionOutcome,
) -> Result<NodeExecutionOutcome, serde_json::Error> {
    agent_tool_outcome_at(
        outcome,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
    )
}

pub(crate) fn agent_tool_outcome_at(
    outcome: &RuntimeToolExecutionOutcome,
    finished_at_ms: u64,
) -> Result<NodeExecutionOutcome, serde_json::Error> {
    let encoded = serde_json::to_string(outcome)?;
    Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: Some(format!("agent-tool-outcome:{encoded}")),
        wait_class: None,
        summary: None,
        evidence_refs: vec![],
        failure: None,
        usage: Default::default(),
        finished_at_ms,
    }))
}

pub(super) fn finish_observed_outcome(
    ticket: &NodeExecutionTicket,
    observation: Option<&NoDispatchObservation>,
    result: Result<NodeExecutionOutcome, NodeExecutorError>,
) -> Result<NodeExecutionOutcome, NodeExecutorError> {
    let mut outcome = match result {
        Ok(outcome) => outcome,
        Err(error) => {
            let refusal = decode_bound_agent_tool(&ticket.payload_ref)
                .ok()
                .and_then(|bound| {
                    observation.and_then(|observation| observation.refusal(&bound.request))
                });
            match refusal {
                Some(refusal) => {
                    agent_tool_outcome(&refusal).map_err(|error| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: error.to_string(),
                    })?
                }
                None => return Err(error),
            }
        }
    };
    if let Some(observation) = observation {
        observation
            .finish(&mut outcome)
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
    }
    Ok(outcome)
}

fn agent_tool_leaf_result(
    result: Result<Result<RuntimeToolExecutionOutcome, ToolError>, crate::ToolExecutionPlaneError>,
    on_leaf_error: impl FnOnce(ToolError) -> Result<RuntimeToolExecutionOutcome, ToolError>,
) -> Result<RuntimeToolExecutionOutcome, ToolError> {
    match result.map_err(|error| ToolError::new(error.to_string()))? {
        Ok(outcome) => Ok(outcome),
        Err(error) => on_leaf_error(error),
    }
}

fn original_process_step(
    services: &RuntimeServices,
    request: &RuntimeToolExecutionRequest,
) -> Result<(), ToolError> {
    original_process_step_from_store(services.event_store(), request)
}

fn original_process_step_from_store(
    store: &crate::RuntimeEventStore,
    request: &RuntimeToolExecutionRequest,
) -> Result<(), ToolError> {
    let denied = || ToolError::new("stale Step rejection has no original Process Step");
    let binding = request.step_binding.as_ref().ok_or_else(denied)?;
    let snapshot = serde_json::to_value(binding.snapshot.as_ref())
        .map_err(|error| ToolError::new(error.to_string()))?;
    let through = store
        .durable_commit_cursor()
        .map_err(|error| ToolError::new(error.to_string()))?;
    let mut after = None;
    loop {
        let page = store
            .position_page(
                crate::RuntimeEventScope::Session,
                &binding.snapshot.identity.session_id,
                Some("process.step_bound"),
                after,
                through,
                100,
                1024 * 1024,
            )
            .map_err(ToolError::new)?;
        if page.is_empty() {
            return Err(denied());
        }
        for event in &page {
            if event.payload["identity"]["step_id"] == binding.snapshot.identity.step_id {
                if event.stream_id != format!("session:{}", binding.snapshot.identity.session_id)
                    || event.scope != crate::RuntimeEventScope::Session
                    || event.kind != "process.step_bound"
                    || event.schema_version != 1
                    || event.actor.as_deref() != binding.snapshot.identity.actor_ref.as_deref()
                    || event.status.as_deref() != Some("prepared")
                    || event.payload != snapshot
                {
                    return Err(denied());
                }
                return Ok(());
            }
        }
        let last = page.last().ok_or_else(denied)?;
        let next = (last.commit_cursor, last.transaction_index);
        if after.is_some_and(|previous| next <= previous) {
            return Err(denied());
        }
        after = Some(next);
    }
}

fn agent_read_terminal(
    services: &RuntimeServices,
    request: &RuntimeToolExecutionRequest,
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Result<Option<RuntimeToolExecutionOutcome>, ToolError> {
    agent_read_terminal_from_store(
        services.event_store(),
        services.commit_service(),
        request,
        descriptor,
    )
}

// The existing bounded try_send writer is initialized before acquiring stream locks.
#[cfg(any(test, feature = "test-fixtures"))]
struct StaleReadTrace { call: String, graph: String, node: String, attempt: u32 }

#[cfg(any(test, feature = "test-fixtures"))]
impl StaleReadTrace {
    fn new(request: &RuntimeToolExecutionRequest) -> Option<Self> {
        use sha2::{Digest, Sha256};
        if std::env::var_os("COWD_R622_TRACE").is_none() { return None; }
        let digest = |value: &str| format!("{:x}", Sha256::digest(value.as_bytes()));
        let parent = request.parent_execution.as_ref();
        let trace = Self {
            call: digest(&request.tool_use_id),
            graph: digest(parent.map_or("", |p| p.execution_id.as_str())),
            node: digest(parent.map_or("", |p| p.node_id.as_str())),
            attempt: request.parent_execution_attempt.unwrap_or_default(),
        };
        trace.emit("begin");
        Some(trace)
    }
    fn emit(&self, label: &'static str) {
        crate::execution_core::graph::k05_trace_emit(format!(
            "stale_read predicate={label} call_sha256={} graph_sha256={} node_sha256={} attempt={}",
            self.call, self.graph, self.node, self.attempt
        ));
    }
}

// Each original predicate is evaluated exactly once, in its original short-circuit order.
macro_rules! stale_read_predicate {
    ($trace:ident, $label:literal, $condition:expr) => {{
        let failed = $condition;
        #[cfg(any(test, feature = "test-fixtures"))]
        if failed { if let Some(trace) = &$trace { trace.emit($label); } }
        failed
    }};
}

pub(crate) fn agent_read_terminal_from_store(
    store: &crate::RuntimeEventStore,
    commits: &crate::ExecutionCommitService,
    request: &RuntimeToolExecutionRequest,
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Result<Option<RuntimeToolExecutionOutcome>, ToolError> {
    #[cfg(any(test, feature = "test-fixtures"))]
    let trace_context = StaleReadTrace::new(request);
    let denied = || ToolError::new("Agent read terminal primary/index identity conflict");
    let parent = request.parent_execution.as_ref().ok_or_else(|| { let _ = stale_read_predicate!(trace_context, "missing_parent", true); denied() })?;
    let attempt = request
        .parent_execution_attempt
        .filter(|attempt| *attempt > 0)
        .ok_or_else(|| { let _ = stale_read_predicate!(trace_context, "missing_attempt", true); denied() })?;
    let primary_stream = format!("execution-effect:{}", request.idempotency_key);
    let primary_key = format!("{}:read-receipt", request.idempotency_key);
    let index_stream = format!(
        "execution-agent-receipts:{}:{}:{attempt}",
        parent.execution_id, parent.node_id
    );
    let index_key = format!("agent-tool-receipt:{}", request.idempotency_key);
    store.with_stream_locks(&[primary_stream.clone(), index_stream.clone()], || {
        let primary = store
            .event_by_idempotency_key(&primary_stream, &primary_key)
            .map_err(|error| ToolError::new(error.to_string()))?;
        let index = store
            .event_by_idempotency_key(&index_stream, &index_key)
            .map_err(|error| ToolError::new(error.to_string()))?;
        let (primary, index) = match (primary, index) {
            (None, None) => { let _ = stale_read_predicate!(trace_context, "receipt_pair_absent", true); return Ok(None); },
            (Some(primary), Some(index)) => (primary, index),
            _ => { let _ = stale_read_predicate!(trace_context, "receipt_pair", true); return Err(denied()); },
        };
        let binding = request.step_binding.as_ref().ok_or_else(|| { let _ = stale_read_predicate!(trace_context, "missing_binding", true); denied() })?;
        let invocation = request.compiled_invocation.clone().ok_or_else(|| { let _ = stale_read_predicate!(trace_context, "missing_invocation", true); denied() })?;
        let input = harness_contract::step::parse_strict_json(&request.input)
            .map_err(|error| ToolError::new(error.to_string()))?;
        if stale_read_predicate!(trace_context, "invocation_1", invocation.origin.as_ref() != Some(&binding.origin))
            || stale_read_predicate!(trace_context, "invocation_2", invocation.invocation_id != request.tool_use_id)
            || stale_read_predicate!(trace_context, "invocation_3", invocation.intent.tool_name != request.tool_name)
            || stale_read_predicate!(trace_context, "invocation_4", invocation.intent.normalized_input != input)
            || stale_read_predicate!(trace_context, "invocation_5", invocation.effect != *descriptor)
            || stale_read_predicate!(trace_context, "invocation_6", binding.origin.call_id != request.tool_use_id)
            || stale_read_predicate!(trace_context, "invocation_7", binding.effect_idempotency_key() != request.idempotency_key)
        {
            return Err(denied());
        }
        let replay = crate::execution_core::graph::commit_service::receipt_replay_contract(request)
            .map_err(|error| ToolError::new(error.to_string()))?;
        let expected_refs = [
            ("tool_invocation", request.tool_use_id.as_str()),
            ("execution_graph", parent.execution_id.as_str()),
            ("execution_node", parent.node_id.as_str()),
        ];
        for (event, expected_schema) in [(&primary, 1), (&index, 2)] {
            if stale_read_predicate!(trace_context, "event_1", event.scope != crate::RuntimeEventScope::ExecutionNode)
            || stale_read_predicate!(trace_context, "event_2", event.schema_version != expected_schema)
            || stale_read_predicate!(trace_context, "event_3", event.actor.as_deref() != Some("governed_tool"))
            || stale_read_predicate!(trace_context, "event_4", event.status.as_deref() != Some("completed"))
            || stale_read_predicate!(trace_context, "event_5", event.payload["idempotency_key"] != request.idempotency_key)
            || stale_read_predicate!(trace_context, "event_6", expected_refs.iter().any(|(kind, id)| {
                    !event
                        .refs
                        .iter()
                        .any(|reference| reference.kind == *kind && reference.id == *id)
                }))
            || stale_read_predicate!(trace_context, "event_7", !event.refs.iter().any(|reference| {
                    reference.kind == "agent_attempt" && reference.id == attempt.to_string()
                }))
            || stale_read_predicate!(trace_context, "event_8", event.refs.len() != 4)
            {
                return Err(denied());
            }
        }
        if stale_read_predicate!(trace_context, "receipt_1", primary.kind != "execution.read.receipt")
            || stale_read_predicate!(trace_context, "receipt_2", primary.stream_id != primary_stream)
            || stale_read_predicate!(trace_context, "receipt_3", primary.idempotency_key.as_deref() != Some(primary_key.as_str()))
            || stale_read_predicate!(trace_context, "receipt_4", index.kind != "execution.agent_tool.receipt")
            || stale_read_predicate!(trace_context, "receipt_5", index.stream_id != index_stream)
            || stale_read_predicate!(trace_context, "receipt_6", index.idempotency_key.as_deref() != Some(index_key.as_str()))
            || stale_read_predicate!(trace_context, "receipt_7", primary.transaction_id != index.transaction_id)
            || stale_read_predicate!(trace_context, "receipt_8", primary.commit_cursor != index.commit_cursor)
            || stale_read_predicate!(trace_context, "receipt_9", index.transaction_index <= primary.transaction_index)
            || stale_read_predicate!(trace_context, "receipt_10", primary.payload["tool_use_id"] != request.tool_use_id)
            || stale_read_predicate!(trace_context, "receipt_11", primary.payload["tool_name"] != request.tool_name)
            || stale_read_predicate!(trace_context, "receipt_12", primary.payload["replay"] != replay)
            || stale_read_predicate!(trace_context, "receipt_13", primary.payload["effect"]
                != serde_json::to_value(descriptor)
                    .map_err(|error| ToolError::new(error.to_string()))?)
            || stale_read_predicate!(trace_context, "receipt_14", primary.payload["effect"] != index.payload["effect"])
            || stale_read_predicate!(trace_context, "receipt_15", primary.payload["outcome"] != index.payload["outcome"])
            || stale_read_predicate!(trace_context, "receipt_16", primary.payload["resolved_effect_scope"] != index.payload["resolved_effect_scope"])
            || stale_read_predicate!(trace_context, "receipt_17", primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
                != index.payload[crate::context_evidence::READ_ACQUISITION_FIELD])
            || stale_read_predicate!(trace_context, "receipt_18", index.payload["effect_kind"] != "read")
            || stale_read_predicate!(trace_context, "receipt_19", crate::ExecutionCommitService::receipt_observation_sequence(&index).is_none())
            || stale_read_predicate!(trace_context, "receipt_20", index.payload["sequence"].as_u64().is_none_or(|sequence| sequence == 0))
            || stale_read_predicate!(trace_context, "receipt_21", index.payload["effect_scope"]
                != serde_json::to_value(
                    request
                        .authorization
                        .as_ref()
                        .map(|authorization| &authorization.scope),
                )
                .map_err(|error| ToolError::new(error.to_string()))?)
            || stale_read_predicate!(trace_context, "receipt_22", index.payload["authorized_scopes"]
                != serde_json::to_value(&request.authorized_scopes)
                    .map_err(|error| ToolError::new(error.to_string()))?)
        {
            return Err(denied());
        }
        let outcome = commits
            .completed_tool_effect(request, descriptor)
            .map_err(|error| ToolError::new(error.to_string()))?
            .ok_or_else(|| { let _ = stale_read_predicate!(trace_context, "missing_completed_effect", true); denied() })?;
        if stale_read_predicate!(trace_context, "outcome_1", outcome.tool_use_id != request.tool_use_id)
            || stale_read_predicate!(trace_context, "outcome_2", outcome.tool_name != request.tool_name)
            || stale_read_predicate!(trace_context, "outcome_3", outcome.category != request.category)
        {
            return Err(denied());
        }
        if primary.payload["resolved_effect_scope"].is_null() {
            if stale_read_predicate!(trace_context, "stale_1", outcome.status != crate::RuntimeToolExecutionStatus::Failed)
            || stale_read_predicate!(trace_context, "stale_2", outcome.error.as_deref() != Some("step_binding_stale"))
            || stale_read_predicate!(trace_context, "stale_3", outcome.output.is_some())
            || stale_read_predicate!(trace_context, "stale_4", !outcome.observed_evidence.is_empty())
            || stale_read_predicate!(trace_context, "stale_5", outcome.evidence_ref != format!("tool-effect:{}", request.idempotency_key))
            || stale_read_predicate!(trace_context, "stale_6", primary.payload["outcome_truncated"] != false)
            || stale_read_predicate!(trace_context, "stale_7", !primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null())
            || stale_read_predicate!(trace_context, "stale_8", [&primary, &index].iter().any(|event| {
                    !event.payload["process_admission_ref"].is_null()
                        || !event.payload["root_effect_owner"].is_null()
                        || !event.payload["root_effect_context"].is_null()
                }))
            {
                return Err(denied());
            }
            original_process_step_from_store(store, request).map_err(|error| {
                let _ = stale_read_predicate!(trace_context, "original_process_step", true);
                error
            })?;
        }
        Ok(Some(outcome))
    })
}

impl AgentToolBatchBackend {
    fn stale_read_rejection(
        &self,
        request: &RuntimeToolExecutionRequest,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        agent: &harness_contract::agent::AgentTaskPacket,
        turn_id: &str,
        error: ToolError,
    ) -> Result<RuntimeToolExecutionOutcome, ToolError> {
        let services = self.services.upgrade().ok_or_else(|| ToolError::unknown("original Agent ToolBatch services unavailable"))?;
        let reject = || error.clone();
        if error.to_string() != "step_binding_stale" {
            return Err(error);
        }
        let binding = request.step_binding.as_ref().ok_or_else(reject)?;
        let snapshot = binding.snapshot.as_ref();
        let identity = &snapshot.identity;
        let invocation = request.compiled_invocation.as_ref().ok_or_else(reject)?;
        let effect = &invocation.effect;
        if !matches!(&binding.origin.source,
                harness_contract::step::CallSource::Process { worker_id, sequence }
                    if worker_id == agent.run_id() && *sequence > 0)
            || !agent.allowed_tools.contains(&request.tool_name)
            || identity.execution_id.as_deref() != Some(agent.graph_id())
            || identity.session_id != agent.session_id()
            || identity.turn_id != turn_id
            || identity.run_ref.as_deref() != Some(agent.run_id())
            || identity.actor_ref.as_deref() != Some(agent.agent_id())
            || identity.attempt != Some(agent.attempt)
            || binding.origin.origin_step_id != identity.step_id
            || binding.origin.call_id != request.tool_use_id
            || binding.effect_idempotency_key() != request.idempotency_key
            || binding.origin.catalog_digest != snapshot.catalog_digest
            || snapshot
                .related_revisions
                .get("worker_run")
                .map(String::as_str)
                != Some(agent.run_id())
            || invocation.origin.as_ref() != Some(&binding.origin)
            || invocation.invocation_id != request.tool_use_id
            || invocation.intent.tool_name != request.tool_name
            || invocation.intent.normalized_input
                != harness_contract::step::parse_strict_json(&request.input)
                    .map_err(|_| reject())?
            || effect != descriptor
            || effect.tool_id != request.tool_name
            || effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
            || effect.required_permission != harness_contract::tool::ToolPermissionMode::ReadOnly
            || effect.uses_network
            || effect.spawns_process
            || effect.mutates_packages
            || effect.mutates_system
            || effect
                .scopes
                .iter()
                .any(|scope| scope.operation != harness_contract::policy::PermissionOperation::Read)
            || snapshot.contract_version != harness_contract::step::STEP_CONTRACT_VERSION
            || snapshot.provider_attempt.origin_step_id != identity.step_id
            || snapshot.provider_attempt.attempt_id.is_empty()
            || !snapshot.tools.iter().any(|tool| {
                tool.tool_id == request.tool_name
                    && tool.availability != harness_contract::step::CatalogAvailability::Unavailable
            })
        {
            return Err(error);
        }
        if let Some(winner) = agent_read_terminal(&services, request, descriptor)? {
            return Ok(winner);
        }
        let bounded = agent.team_id().is_some()
            || agent.agentic_binding.is_some()
            || agent
                .binding
                .as_ref()
                .is_some_and(|binding| binding.evaluation.is_some());
        let mut expected_scopes = if bounded {
            agent.resource_scopes.clone()
        } else {
            vec![]
        };
        let session_scope = format!("session:{}", agent.session_id());
        if !expected_scopes.contains(&session_scope) {
            expected_scopes.push(session_scope);
        }
        let policy = services
            .session_execution_policy(agent.session_id())
            .ok_or_else(reject)?;
        let authorization = request.authorization.as_ref().ok_or_else(reject)?;
        let lease = services
            .tool_execution_host()
            .and_then(|host| host.pin_step_catalog())
            .ok_or_else(reject)?;
        let input =
            harness_contract::step::parse_strict_json(&request.input).map_err(|_| reject())?;
        if request.authorized_scopes != expected_scopes
            || request.category != crate::ToolSafetyCategory::from_effect(descriptor)
            || !policy
                .permission_mode
                .permits(descriptor.required_permission)
            || !agent
                .permission_ceiling
                .permits(descriptor.required_permission)
            || policy.revision != request.policy_revision
            || policy.sandbox_posture != request.sandbox_posture
            || request.policy_revision == 0
            || request.policy_revision != agent.policy_revision
            || authorization.policy_revision != request.policy_revision
            || authorization.authorization_lease.principal_id
                != format!("agent:{}", agent.agent_id())
            || snapshot.related_revisions.get("policy")
                != Some(&request.policy_revision.to_string())
            || snapshot.related_revisions.get("attempt") != Some(&agent.attempt.to_string())
            || chrono::Utc::now().timestamp_millis() >= snapshot.expires_at_ms
            || lease.describe_effect(&request.tool_name, &input) != *descriptor
            || lease
                .validate_authorization(authorization, &request.tool_name, &input)
                .is_err()
        {
            return Err(error);
        }
        original_process_step(&services, request)?;
        let outcome = RuntimeToolExecutionOutcome { pending_agent_batch: None,
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Failed,
            category: request.category,
            output: None,
            error: Some(error.to_string()),
            evidence_ref: format!("tool-effect:{}", request.idempotency_key),
            observed_evidence: vec![],
        };
        #[cfg(any(test, feature = "test-fixtures"))]
        let trace_context = StaleReadTrace::new(request);
        let committed = services
            .commit_service()
            .commit_readonly_tool_receipts(&[(request.clone(), outcome)]);
        let _ = stale_read_predicate!(trace_context, "stale_commit_unknown", committed.is_err());
        let _ = stale_read_predicate!(trace_context, "stale_commit_ok", committed.is_ok());
        match agent_read_terminal(&services, request, descriptor)? {
            Some(winner) => Ok(winner),
            None => Err(ToolError::new(match committed {
                Err(error) => format!("stale Step rejection receipt commit failed: {error}"),
                Ok(()) => "stale Step rejection receipt missing after commit".into(),
            })),
        }
    }
}

/// Private adapter entered only from the original bound ToolBatch backend.
/// The shared bound executor calls `execute_runtime_tool` after its effect
/// admission; no Scoped executor implementation is changed.
struct DelegatedActionOwnerHost<'a> {
    inner: &'a dyn crate::RuntimeExecutionHost,
    services: &'a RuntimeServices,
    packet: &'a harness_contract::agent::AgentTaskPacket,
    original: &'a RuntimeToolExecutionRequest,
    parent_stop: Option<&'a crate::bound_tool_batch::BoundToolStop>,
    child_stop: &'a crate::bound_tool_batch::BoundToolStop,
    parent_ticket: Option<&'a NodeExecutionTicket>,
    provider_capture: Option<&'a Arc<crate::conversation::tool_plane::OriginalSingleProducerCapture>>,
}

/// The durable refusal of a call that was refused before it ran.
///
/// The fact is written under the call's own effect key, so the batch
/// closure can settle the call as refused instead of waiting for a receipt
/// that will never come. Two premises decide whether a fact may exist at
/// all: the call's own program binding, and that program's own projection.
/// A call whose program cannot be named, or whose program cannot be read,
/// gets no fact - the refusal still stands, and the closure still waits.
pub(crate) fn settle_refused_call(
    services: &RuntimeServices,
    agent: &harness_contract::agent::AgentTaskPacket,
    request: &RuntimeToolExecutionRequest,
    outcome: &RuntimeToolExecutionOutcome,
) -> bool {
    if outcome.status != crate::RuntimeToolExecutionStatus::BlockedPermission {
        return false;
    }
    let Some(program_id) = agent
        .agentic_binding
        .as_ref()
        .map(|binding| binding.program_id.clone())
    else {
        return false;
    };
    let actions = services.agent_action_service();
    let Ok(Some(projection)) = actions.project_if_exists(&program_id) else {
        return false;
    };
    record_refused_call_fact(&actions, &projection, request, outcome)
}

/// The durable refusal of a call that was refused before it ran.
///
/// The fact is written under the call's own effect key, so the batch
/// closure can settle the call as refused instead of waiting for a receipt
/// that will never come. The writer decides whether the fact may exist at
/// all: a call without its own admission intent gets none. A fact that
/// cannot be written leaves the call exactly as it was - the refusal still
/// stands, and the closure still waits.
pub(crate) fn record_refused_call_fact(
    actions: &crate::agentic::AgentActionService,
    projection: &crate::agentic::AgenticProgramProjection,
    request: &RuntimeToolExecutionRequest,
    outcome: &RuntimeToolExecutionOutcome,
) -> bool {
    if outcome.status != crate::RuntimeToolExecutionStatus::BlockedPermission {
        return false;
    }
    let refusal = crate::agentic::RefusedCall {
        effect_key: &request.idempotency_key,
        tool_use_id: &request.tool_use_id,
        tool_name: &request.tool_name,
        reason: outcome
            .error
            .as_deref()
            .unwrap_or("the call was refused before it ran"),
    };
    match actions.record_refused_admitted_call(projection, &refusal) {
        Ok(crate::agentic::RefusalOutcome::Recorded) => true,
        // A call without its own admission intent gets no fact: there is
        // nothing for a consumer to read, so nothing was settled.
        Ok(crate::agentic::RefusalOutcome::NotAdmitted) => false,
        Err(error) => {
            tracing::warn!(
                program_id = %projection.program_id,
                effect_key = %request.idempotency_key,
                %error,
                "refused call left no durable rejection fact"
            );
            false
        }
    }
}

#[async_trait]
impl crate::RuntimeExecutionHost for DelegatedActionOwnerHost<'_> {
    fn bounded_process_authority(&self, request: &RuntimeToolExecutionRequest) -> Result<crate::tool_host::BoundedProcessAuthority, ToolError> {
        self.inner.bounded_process_authority(request)
    }
    fn bounded_process_lease(&self, request: &RuntimeToolExecutionRequest) -> Result<tools::ToolHostLease, ToolError> {
        self.inner.bounded_process_lease(request)
    }
    fn bounded_process_plane(&self) -> Option<Arc<crate::ToolExecutionPlane>> { self.inner.bounded_process_plane() }
    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> { self.inner.pin_step_catalog() }
    fn delegated_tool_definitions(&self, names: &[String]) -> Vec<crate::ProviderToolDefinition> {
        self.inner.delegated_tool_definitions(names)
    }
    fn delegated_tool_effect_descriptor(&self, name: &str, input: &serde_json::Value) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        self.inner.delegated_tool_effect_descriptor(name, input)
    }
    async fn execute_runtime_tool(&self, request: &RuntimeToolExecutionRequest) -> RuntimeToolExecutionOutcome {
        if request.tool_name == "agent_action_batch"
            || !harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&request.tool_name.as_str()) {
            return self.inner.execute_runtime_tool(request).await;
        }
        let invalid = |reason: String| RuntimeToolExecutionOutcome { pending_agent_batch: None,
            tool_use_id: request.tool_use_id.clone(), tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::BlockedPermission, category: request.category,
            output: None, error: Some(reason), evidence_ref: String::new(), observed_evidence: vec![],
        };
        if request != self.original { return invalid("delegated original ToolBatch call changed".into()); }
        let Some(lease) = self.inner.pin_step_catalog() else { return invalid("delegated ToolHost lease missing".into()); };
        match crate::agentic::action_invocation::with_verified_delegated_call(
            self.services, request, self.packet, lease, self.parent_ticket, self.parent_stop, self.child_stop, self.provider_capture, |capability| async move {
                self.inner.execute_runtime_tool_with_action_invocation(request, &capability).await
            },
        ).await {
            Ok(outcome) => outcome,
            Err(error) => invalid(error),
        }
    }
}

#[async_trait]
impl ScopedNodeBackend for AgentToolBatchBackend {
    fn retains_physical_execution(&self) -> bool {
        true
    }
    fn request_stop(&self, _ticket: &NodeExecutionTicket) {
        self.stop.request();
    }
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let services = self.services.upgrade().ok_or_else(|| NodeExecutorError::Uncertain {
            node_id:ticket.node_id.clone(), reason:"original Agent ToolBatch services unavailable".into(),
        })?;
        let observation = services
            .tool_batch_executor()
            .binding_observation(ticket);
        let fail = |reason: String| NodeExecutorError::Poll {
            node_id: ticket.node_id.clone(),
            reason,
        };
        let original_child = services.tool_batch_executor().original_agent_child(ticket);
        if requires_original_agent_child(&self.bound.request) && original_child.is_none() {
            return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(),
                reason: "original Agent child carrier unavailable".into() });
        }
        let request = original_child.as_ref().map_or(&self.bound.request, |child| child.request.as_ref());
        if original_child.is_some() {
            validate_original_call_request(ticket, request).map_err(|e| fail(e.to_string()))?;
        }
        // A prior decision is never turned into a fresh acquisition attempt.
        // Only this same physical observation can provide the live owner;
        // the durable event by itself does not recreate that capability.
        let decision_stream = format!("execution-call-decision:{}", request.idempotency_key);
        let acquisition_key = format!("{}:bounded-acquisition-failure", request.idempotency_key);
        let prior = if request.compiled_invocation.as_ref().is_some_and(|invocation|
            tools::tool_orchestrator::effect_is_bounded_read_only_compute(&invocation.effect)) {
            services.event_store().event_by_idempotency_key(&decision_stream, &acquisition_key)
                .map_err(|_| NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(),
                    reason:"original bounded acquisition decision unavailable".into() })?
        } else { None };
        if prior.is_some() {
            let prior_owner = observation.as_ref().and_then(|owner| owner.original_bounded_owner(&request.tool_use_id))
                .ok_or_else(|| NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(),
                    reason:"original bounded acquisition winner owner unavailable".into() })?;
            let same_child = match (&original_child, &prior_owner.child) {
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                (None, None) => true,
                _ => false,
            };
            if !same_child || prior_owner.failure.request() != request
                || !prior_owner.failure.request().tool_progress.same_original_channels(&request.tool_progress) {
                return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(),
                    reason:"original bounded acquisition winner differs".into() });
            }
            let verified = services.commit_service().original_bounded_acquisition_failure(ticket, &prior_owner.failure)
                .map_err(|_| NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(),
                    reason:"original bounded acquisition winner could not be authenticated".into() })?;
            if verified.transaction_id() != prior_owner.receipt.transaction_id()
                || verified.event_id() != prior_owner.receipt.event_id() {
                return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(),
                    reason:"original bounded acquisition winner changed".into() });
            }
            let outcome = observation.as_ref().and_then(|owner| owner.refusal(request))
                .ok_or_else(|| NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(),
                    reason:"original bounded acquisition outcome unavailable".into() })?;
            return agent_tool_outcome(&outcome).map_err(|error| NodeExecutorError::Uncertain {
                node_id:ticket.node_id.clone(), reason:error.to_string(),
            });
        }
        // The parent may already have committed this same call's effect
        // intent. A local pre-dispatch failure cannot attest a globally empty
        // effect stream; preserve that owner and report uncertainty instead.
        let inherited_effect = if original_child.is_some() {
            match request.tool_progress.original_fresh_effect() {
                Ok(Some(owner)) if owner.matches_original(services.event_store(), request)
                    && owner.original_request().as_ref() == request => true,
                Ok(Some(_)) | Err(_) => return Err(NodeExecutorError::Uncertain {
                    node_id: ticket.node_id.clone(), reason: "original Agent effect owner changed or unavailable".into(),
                }),
                Ok(None) => {
                    if request.tool_progress.original_governed_hook().map_err(|error| NodeExecutorError::Uncertain {
                        node_id: ticket.node_id.clone(), reason: error,
                    })?.is_some() {
                        // The owner slot answers the admission question for the
                        // effect families that install one. The Read family never
                        // installs a first-effect owner (see after_commit), so an
                        // empty slot is not a lost effect: the durable admission
                        // fact answers the same question in every family, under
                        // the same exact keys. A fact that cannot be read is
                        // never an admission, so a call the parent never admitted
                        // stays uncertain instead of being dispatched.
                        if !services.commit_service()
                            .original_effect_admitted(false, request)
                            .map_err(|error| NodeExecutorError::Uncertain {
                                node_id: ticket.node_id.clone(), reason: error.to_string(),
                            })?
                        {
                            return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(),
                                reason: "original governed effect owner missing".into() });
                        }
                        true
                    } else { false }
                }
            }
        } else { false };
        if inherited_effect {
            if let Some(observation) = &observation { observation.uncertain(request); }
        }
        let refuse = |reason: String| {
            if inherited_effect {
                NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason }
            } else {
                if let Some(observation) = &observation {
                    observation.refuse(&self.bound.request.tool_use_id, reason.clone());
                }
                fail(reason)
            }
        };
        let graph = services
            .graph_state_store()
            .load_async(&ticket.graph_id)
            .await
            .map_err(|e| refuse(e.to_string()))?;
        if !is_bound_agent_tool_graph(&graph) || graph.service_class != ticket.service_class {
            return Err(refuse(
                "Agent ToolBatch ticket does not match its durable graph".into(),
            ));
        }
        let parent = request
            .parent_execution
            .as_ref()
            .ok_or_else(|| refuse("missing Agent parent".into()))?;
        let parent_graph = services
            .graph_state_store()
            .load_async(&parent.execution_id)
            .await
            .map_err(|e| refuse(e.to_string()))?;
        if graph.lineage != parent_graph.lineage
            || graph.service_class != parent_graph.service_class
        {
            return Err(refuse(
                "Agent ToolBatch cannot change its parent's lineage or service class".into(),
            ));
        }
        let parent_node = parent_graph
            .nodes
            .iter()
            .find(|node| node.id == parent.node_id && node.kind == ExecutionNodeKind::AgentTask)
            .ok_or_else(|| refuse("Agent ToolBatch parent is not a durable AgentTask".into()))?;
        let agent: harness_contract::agent::AgentTaskPacket = match &original_child {
            Some(child) if Arc::ptr_eq(&child.store, services.event_store()) => child.packet.clone(),
            Some(_) => return Err(refuse("original Agent child store changed".into())),
            None => serde_json::from_str(&parent_node.payload_ref)
                .map_err(|e| refuse(format!("invalid durable Agent packet: {e}")))?,
        };
        // A provider ToolBatch plan is distinct from its AgentTask graph.
        // Accept that plan only from the same already-committed owner and its
        // original in-process hook, never from a decoded payload alone.
        let original_governed_plan = if inherited_effect {
            request.tool_progress.original_governed_hook().map_err(|error| NodeExecutorError::Uncertain {
                node_id: ticket.node_id.clone(), reason: error,
            })?.is_some_and(|hook| hook.original_plan_id() == request.governed_plan_id
                && hook.original_plan_revision() == request.governed_plan_revision
                && Some(hook.original_session_id()) == request.session_id.as_deref()
                && hook.original_parent() == request.parent_execution.as_ref()
                && hook.original_attempt() == request.parent_execution_attempt)
        } else { false };
        if agent.run_id() != self.bound.run_id
            || agent.agent_id() != self.bound.agent_id
            || agent.graph_id() != parent.execution_id
            || agent.node_id() != parent.node_id
            || Some(agent.attempt) != request.parent_execution_attempt
            || request.session_id.as_deref() != Some(agent.session_id())
            || (request.governed_plan_id != parent.execution_id && !original_governed_plan)
        {
            return Err(refuse(
                "tool_effect_authority: Agent ToolBatch focus does not match its canonical run"
                    .into(),
            ));
        }
        if let Some(child) = &original_child {
            if child.parent_ticket.graph_id != parent.execution_id || child.parent_ticket.node_id != parent.node_id
                || child.parent_ticket.attempt != agent.attempt {
                return Err(refuse("original Agent child parent ticket changed".into()));
            }
        }
        if !agent.allowed_tools.contains(&request.tool_name) && request.tool_name != "tool_search" {
            return Err(refuse(
                "tool_effect_authority: Agent ToolBatch focus does not match its canonical run"
                    .into(),
            ));
        }
        let host = services
            .tool_execution_host()
            .ok_or_else(|| refuse("Agent ToolBatch ToolHost unavailable".into()))?;
        let input = serde_json::from_str(&request.input)
            .map_err(|e| refuse(format!("invalid tool input: {e}")))?;
        let descriptor = host
            .delegated_tool_effect_descriptor(&request.tool_name, &input)
            .ok_or_else(|| refuse("missing enforceable tool descriptor".into()))?;
        if !agent.allowed_tools.contains(&request.tool_name) {
            let reject = || {
                refuse("tool_effect_authority: Agent ToolBatch bootstrap does not match its canonical run or readonly catalog".into())
            };
            if request.tool_name != "tool_search" {
                return Err(reject());
            }
            let lease = host.pin_step_catalog().ok_or_else(reject)?;
            let catalog = &lease.snapshot().catalog;
            let metadata = lease.catalog_receipt();
            let registered = metadata
                .descriptors
                .iter()
                .find(|entry| entry.canonical_id == "tool_search")
                .ok_or_else(reject)?;
            let definitions = catalog.definitions(Some(&std::collections::BTreeSet::from([
                "tool_search".into(),
            ])));
            let definition = definitions.first().ok_or_else(reject)?;
            let exposed = host.delegated_tool_definitions(&["tool_search".into()]);
            let mut actual = exposed.iter().filter(|actual| actual.name == "tool_search");
            let actual_definition = actual.next().ok_or_else(reject)?;
            if catalog.canonical_name("tool_search").as_deref() != Some("tool_search")
                || registered.source != "builtin"
                || registered.health != harness_contract::tool::ToolDescriptorHealth::Healthy
                || registered.required_permission
                    != harness_contract::tool::ToolPermissionMode::ReadOnly
                || catalog.required_permission("tool_search")
                    != Some(harness_contract::tool::ToolPermissionMode::ReadOnly)
                || catalog.effect_resolver("tool_search")
                    != tools::builtin_effect_resolver_spec("tool_search")
                || definitions.len() != 1
                || actual.next().is_some()
                || actual_definition.input_schema != definition.input_schema
                || actual_definition.description != definition.description
                || descriptor != lease.describe_effect("tool_search", &input)
                || descriptor.tool_id != "tool_search"
                || descriptor.effect_kind != harness_contract::tool::ToolEffectKind::Read
                || descriptor.required_permission
                    != harness_contract::tool::ToolPermissionMode::ReadOnly
                || descriptor.uses_network
                || descriptor.spawns_process
                || descriptor.mutates_packages
                || descriptor.mutates_system
                || descriptor.scopes.iter().any(|scope| {
                    scope.operation != harness_contract::policy::PermissionOperation::Read
                })
            {
                return Err(reject());
            }
            let binding = request.step_binding.as_ref().ok_or_else(reject)?;
            let step = binding
                .resolve(Some(lease))
                .map_err(|error| refuse(error.to_string()))?;
            step.validate_call(
                &binding.origin,
                agent.session_id(),
                &request.tool_use_id,
                &request.tool_name,
                &request.input,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|error| refuse(error.to_string()))?;
            let identity = &step.snapshot().identity;
            let source_matches = match &binding.origin.source {
                harness_contract::step::CallSource::Provider { .. } => {
                    identity.execution_id.as_deref() == Some(agent.run_id())
                        && identity
                            .attempt
                            .is_none_or(|attempt| attempt == agent.attempt)
                }
                harness_contract::step::CallSource::Process { worker_id, .. } => {
                    identity.execution_id.as_deref() == Some(agent.graph_id())
                        && identity.attempt == Some(agent.attempt)
                        && worker_id == agent.run_id()
                }
                _ => false,
            };
            if identity.run_ref.as_deref() != Some(agent.run_id()) || !source_matches {
                return Err(reject());
            }
        }
        if request.tool_name == "agent_action_batch"
            || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&request.tool_name.as_str()) {
            crate::execution_core::graph::validate_node_ticket_start(services.event_store(), ticket, None)
                .map_err(|error| refuse(error.to_string()))?;
        }
        let owner_host = DelegatedActionOwnerHost {
            inner: host.as_ref(), services: services.as_ref(), packet: &agent, original: request,
            parent_stop: original_child.as_ref().and_then(|child| child.stop.as_ref()), child_stop: &self.stop,
            parent_ticket: original_child.as_ref().map(|child| &child.parent_ticket),
            provider_capture: original_child.as_ref().and_then(|child| child.provider_capture.as_ref()),
        };
        let demand = crate::governed_tool_plan::resource_demand_from_effect(&descriptor);
        let (outcome, admission) = crate::bound_tool_batch::with_tool_stop(
            self.stop.clone(),
            services
                .tool_execution_plane()
                .execute_async_classified_retained(
                    &demand,
                    None,
                    ticket.service_class,
                    Some(ticket.service_class),
                    request.session_id.as_deref(),
                    crate::bound_tool_batch::execute_bound_agent_tool_observed(
                        &owner_host,
                        Some(services.commit_service()),
                        services.path_identity_resolver(),
                        services.scope_locks(),
                        request,
                        &descriptor,
                        observation.as_deref(),
                        original_child.as_ref().map(|child| (ticket.clone(), Arc::clone(child))),
                    ),
                ),
        )
        .await;
        if let Err(error) = &outcome {
            if let Some(observation) = &observation {
                match error {
                    crate::ToolExecutionPlaneError::Admission(_) => {
                        observation.refuse(&request.tool_use_id, error.to_string())
                    }
                    _ => observation.uncertain(request),
                }
            }
        }
        let outcome = agent_tool_leaf_result(outcome, |error| {
            if let Some(fact) = error.original_bounded_acquisition_failure() {
                let Some(observation) = observation.as_ref() else {
                    return Err(error.with_unresolved_context("original bounded acquisition observation unavailable", None));
                };
                let result = services.commit_service().commit_bounded_acquisition_failure(services.as_ref(), ticket, fact,
                    Some(&self.stop), original_child.as_ref().and_then(|child| child.stop.as_ref()), observation);
                match result {
                    Ok(proof) if observation.refuse_bounded_committed(Arc::clone(fact), &proof, original_child.clone()).is_ok() => {
                        if let Some(outcome) = observation.refusal(request) {
                            return Ok(outcome);
                        }
                        observation.uncertain(request);
                        let _ = observation.hold_bounded_acquisition(Arc::clone(fact), Some(proof.clone()));
                        let pending = error.original_bounded_acquisition_pending(ticket, Some(proof));
                        return Err(error.with_unresolved_context("original bounded acquisition result unavailable", pending));
                    }
                    result => {
                        let receipt = result.ok();
                        observation.uncertain(request);
                        let _ = observation.hold_bounded_acquisition(Arc::clone(fact), receipt.clone());
                        let pending = error.original_bounded_acquisition_pending(ticket, receipt);
                        return Err(error.with_unresolved_context("original bounded acquisition transaction unresolved", pending));
                    }
                }
            }
            if error.to_string() == "step_binding_stale"
                && agent.binding.as_ref().is_some_and(|binding| {
                    matches!(
                        &binding.executor,
                        harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }
                    )
                })
            {
                if let Some(observation) = &observation {
                    observation.uncertain(request);
                }
                let rejection = self.stale_read_rejection(
                    request,
                    &descriptor,
                    &agent,
                    graph
                        .lineage
                        .as_ref()
                        .map(|lineage| lineage.turn_id.as_str())
                        .unwrap_or(""),
                    error,
                )?;
                return Ok(rejection);
            }
            if let Some(refusal) = observation
                .as_ref()
                .and_then(|observation| observation.refusal(request))
            {
                return Ok(refusal);
            }
            self.stale_read_rejection(
                request,
                &descriptor,
                &agent,
                graph
                    .lineage
                    .as_ref()
                    .map(|lineage| lineage.turn_id.as_str())
                    .unwrap_or(""),
                error,
            )
        })
        .map_err(|error| if inherited_effect || error.is_unknown() {
            NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: error.to_string() }
        } else { fail(error.to_string()) })?;
        let outcome = if outcome.status == crate::RuntimeToolExecutionStatus::Failed
            && outcome.error.as_deref() == Some("step_binding_stale")
            && observation
                .as_ref()
                .and_then(|observation| observation.refusal(request))
                .is_none()
            && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read
        {
            agent_read_terminal(&services, request, &descriptor)
                .map_err(|error| fail(error.to_string()))?
                .ok_or_else(|| fail("stale Step rejection has no canonical terminal".into()))?
        } else {
            outcome
        };
        // T: a call that was refused before it ran leaves a durable fact
        // under its own effect key, so the batch closure can settle it as
        // refused instead of waiting for a receipt that will never come.
        settle_refused_call(&services, &agent, request, &outcome);
        // Leaf effect/receipt has committed under its admission. The graph
        // outcome below is the delivery fence; callers only read it after commit.
        let mut result = agent_tool_outcome(&outcome).map_err(|error| fail(error.to_string()))?;
        if request.tool_name == "agent_action_batch" {
            let child = original_child.as_ref().ok_or_else(|| NodeExecutorError::Uncertain {
                node_id: ticket.node_id.clone(), reason: "original Agent batch child unavailable".into(),
            })?;
            if child.request.as_ref() != request || !child.request.tool_progress.same_original_channels(&request.tool_progress) {
                return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original Agent batch request differs".into() });
            }
            let sink = &child.request.tool_progress;
            let members = sink.checked_original_members().map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
            let publications = sink.checked_original_stop_publications().map_err(|reason| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason })?;
            let stop = sink.original_member_stop();
            let publication = match (publications.as_slice(), stop) {
                ([], crate::tool_host::OriginalStopRead::Available(None)) => None,
                ([publication], crate::tool_host::OriginalStopRead::Available(Some(current)))
                    if publication.failure().is_none() && Arc::ptr_eq(publication.current(), &current)
                        && current.recorded_in_original_sink() && current.belongs_to_original_sink(sink) => Some(Arc::clone(publication)),
                _ => return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original Agent batch stop unavailable or conflicting".into() }),
            };
            if !members.is_empty() || publication.is_some() {
                let mut prefix = crate::tool_host::AuthenticatedBatchPrefix::new();
                for (index, outcome) in &members {
                    let crate::tool_host::OriginalMemberSubmitOutcome::Committed(member) = outcome.as_ref() else { break; };
                    if member.index() != *index || prefix.push(member.clone()).is_err() {
                        return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original Agent batch prefix differs".into() });
                    }
                }
                let carrier = Arc::new(crate::tool_host::OriginalMemberCallCarrier::from_original_request(Arc::clone(&child.request)));
                if carrier.outcomes().len() != members.len() || carrier.outcomes().iter().zip(&members)
                    .any(|((left_index, left), (right_index, right))| left_index != right_index || !Arc::ptr_eq(left, right))
                    || carrier.stop_publications().len() != publications.len()
                    || carrier.stop_publications().iter().zip(&publications).any(|(left, right)| !Arc::ptr_eq(left, right)) {
                    return Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: "original Agent batch carrier changed".into() });
                }
                if !prefix.members().is_empty() {
                    services.commit_service().attach_original_batch_members(&mut result, Arc::new(prefix))
                        .map_err(|error| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason: error.to_string() })?;
                }
                services.commit_service().attach_original_batch_pending(&mut result, carrier, publication);
            }
        }
        drop(admission);
        Ok(result)
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let services = self.services.upgrade().ok_or_else(|| NodeExecutorError::Uncertain {
            node_id:ticket.node_id.clone(), reason:"original Agent ToolBatch services unavailable".into(),
        })?;
        if services.tool_batch_executor().binding_observation(ticket)
            .is_some_and(|observation| observation.packet_scope_is_unresolved()) {
            return Err(NodeExecutorError::Uncertain { node_id:ticket.node_id.clone(),
                reason:"original bounded acquisition remains unknown".into() });
        }
        // Reads do not install a first-effect owner. For other effects use
        // only the child carrier sealed by this same physical close.
        let unknown = |reason: String| NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(), reason };
        let effect = &self.bound.request.compiled_invocation.as_ref()
            .ok_or_else(|| unknown("original Agent child effect unavailable".into()))?.effect;
        if effect.effect_kind == harness_contract::tool::ToolEffectKind::Read { return Ok(()); }
        let close = super::current_original_physical_close(ticket)
            .ok_or_else(|| unknown("original Agent child physical close unavailable".into()))?;
        if close.original_agent_child().is_none() {
            return Err(unknown("original Agent child request unavailable after close".into()));
        }
        let commits = services.commit_service().clone();
        let original_ticket = ticket.clone();
        tokio::task::spawn_blocking(move || {
            if commits.reuse_original_child_release_after_node(&original_ticket, &close)? { return Ok(()); }
            let child = close.original_agent_child().ok_or_else(||
                crate::execution_core::graph::ExecutionCommitError::InvalidCommand("original Agent child request unavailable after close".into()))?;
            let owner = child.request.tool_progress.original_fresh_effect()
                .map_err(crate::execution_core::graph::ExecutionCommitError::InvalidCommand)?;
            match owner {
            Some(owner) if harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&owner.original_request().tool_name.as_str()) => {
                match owner.original_request().tool_progress.original_effect_custody()
                    .map_err(crate::execution_core::graph::ExecutionCommitError::InvalidCommand)? {
                    Some(crate::tool_host::OriginalEffectCustodyOrigin::ToolBatch(parent_ticket)) => {
                        validate_original_call_request(&parent_ticket, owner.original_request())
                            .map_err(|error| crate::execution_core::graph::ExecutionCommitError::InvalidCommand(error.to_string()))?;
                        commits.defer_original_first_effect_for_parent(&original_ticket, &close, &owner)
                    },
                    Some(crate::tool_host::OriginalEffectCustodyOrigin::DirectProcess) =>
                        commits.release_original_first_effect_after_node(&original_ticket, &close, &owner),
                    None => Err(crate::execution_core::graph::ExecutionCommitError::InvalidCommand("original effect custody source missing".into())),
                }
            },
            Some(owner) => commits.release_original_first_effect_after_node(&original_ticket, &close, &owner),
            None => commits.validate_original_child_without_fresh_after_node(&original_ticket, &close),
            }
        }).await.map_err(|error| unknown(error.to_string()))?
          .map_err(|error| unknown(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn agent_tool_366_outer_resource_error_never_enters_leaf_rejection() {
        use crate::execution_core::graph::{
            ExecutionResourceKind, ExecutionResourceManager, ResourceQuota, ScopeLockManager,
        };
        use harness_contract::tool::{ResourceAccess, ResourceDemand, ResourceScopeDemand};
        use std::sync::atomic::{AtomicBool, Ordering};
        let plane = crate::ToolExecutionPlane::new(
            Arc::new(ExecutionResourceManager::new([(
                ExecutionResourceKind::Tool,
                ResourceQuota::new(1, 1, 1).unwrap(),
            )])),
            Arc::new(ScopeLockManager::new()),
        );
        let demand = ResourceDemand {
            scopes: vec![ResourceScopeDemand {
                key: String::new(),
                access: ResourceAccess::Read,
            }],
            ..ResourceDemand::default()
        };
        let polled = AtomicBool::new(false);
        let handled = AtomicBool::new(false);
        let (result, admission) = plane
            .execute_async_classified_retained(
                &demand,
                None,
                harness_contract::execution_graph::ExecutionServiceClass::Foreground,
                None,
                Some("outer-resource-366"),
                async {
                    polled.store(true, Ordering::SeqCst);
                    Err::<RuntimeToolExecutionOutcome, _>(ToolError::new("step_binding_stale"))
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(crate::ToolExecutionPlaneError::Admission(_))
        ));
        assert!(admission.is_none());
        let error = agent_tool_leaf_result(result, |_| {
            handled.store(true, Ordering::SeqCst);
            panic!("outer resource error must not create a stale rejection receipt")
        })
        .unwrap_err();
        assert!(error
            .to_string()
            .starts_with("tool resource admission failed:"));
        assert!(!polled.load(Ordering::SeqCst));
        assert!(!handled.load(Ordering::SeqCst));
        assert_eq!(plane.stats().active, 0);
    }

    #[test]
    fn typed_tool_graph_cannot_borrow_a_foreign_parent_or_payload_identity() {
        let mut request =
            RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
                tool_use_id: "invocation".into(),
                tool_name: "read_file".into(),
                input: "{}".into(),
                depends_on: vec![],
            });
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "parent".into(),
                node_id: "agent".into(),
            });
        let parent = request.parent_execution.clone();
        request.session_id = Some("session".into());
        let payload = encode_bound_agent_tool("agent", "run", request).unwrap();
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let mut graph = ExecutionGraph::new("typed fixture");
        graph.id = format!("agent-tool-batch:{digest}");
        graph.parent_execution = parent;
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        node.id = format!("agent-tool:{digest}");
        graph.nodes.push(node);
        assert!(is_bound_agent_tool_graph(&graph));
        let mut foreign = graph.clone();
        foreign.nodes[0].executor_kind = "inline".into();
        foreign.nodes[0].payload_ref = "{invalid-json".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut foreign = graph.clone();
        foreign.nodes[0].kind = ExecutionNodeKind::InlineModel;
        foreign.nodes[0].payload_ref = "{invalid-json".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut foreign = graph.clone();
        foreign.id = "unrelated-root".into();
        foreign.nodes[0].payload_ref = "{invalid-json".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut foreign = graph.clone();
        foreign.parent_execution.as_mut().unwrap().execution_id = "foreign-parent".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes[0].payload_ref = "{\"kind\":\"turn_tool_batch\"}".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes[0].id = "foreign-node".into();
        assert!(!is_bound_agent_tool_graph(&foreign));
        let mut foreign = graph.clone();
        foreign.nodes.push(graph.nodes[0].clone());
        assert!(!is_bound_agent_tool_graph(&foreign));
    }
}

#[cfg(test)]
pub(crate) mod carrier_437_tests {
    use super::*;
    use base64::Engine;
    use harness_contract::policy::PermissionMode;

    pub(crate) fn real_request(workspace: &std::path::Path) -> RuntimeToolExecutionRequest {
        std::fs::write(workspace.join("proof.txt"), "original input").unwrap();
        let host = tools::ToolHost::builtin("carrier-437", workspace);
        let lease = host.pin_snapshot();
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == "read_file")
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(definitions.len(), 1);
        let identity = serde_json::from_value(serde_json::json!({
            "step_id":"step-437","session_id":"session-437","turn_id":"turn-437",
            "execution_id":"run-437","actor_ref":"agent:agent-437","run_ref":"run-437","attempt":3,
            "goal_ref":"goal-437","program_ref":"program-437","task_ref":"task-437","build_ref":"build-437"
        })).unwrap();
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                identity,
                lease.clone(),
                &definitions,
                &serde_json::json!({"profile":"original"}),
                &serde_json::json!({"request":"original"}),
                std::collections::BTreeMap::from([("goal".into(), "7".into())]),
                chrono::Utc::now()
                    .timestamp_millis()
                    .saturating_add(3_600_000),
            )
            .unwrap(),
        );
        let binding = crate::step_snapshot::StepCallBinding::provider(step, "call-437");
        let input = " { \"path\" : \"proof.txt\" } ";
        let invocation = binding
            .prepare_invocation(
                Some(lease),
                "read_file",
                &serde_json::from_str(input).unwrap(),
            )
            .unwrap();
        let policy = crate::PermissionPolicy::new(PermissionMode::ReadOnly);
        let assessment = crate::AuthorizationNegotiator::new().assess_effective(
            &policy,
            &crate::AuthorizationRequest {
                principal_id: "agent:agent-437".into(),
                capability: "read_file".into(),
                input: input.into(),
                idempotency_key: binding.effect_idempotency_key(),
                effect: invocation.effect.clone(),
                parent_ceiling: PermissionMode::ReadOnly,
                parent_lease_id: None,
                policy_revision: 7,
                recovery_scope: "run-437".into(),
                context: crate::PermissionContext::default(),
                safe_alternatives: Vec::new(),
            },
        );
        let authorization = crate::ToolPolicy
            .authorize(
                &assessment.effective,
                &assessment.assessment,
                binding.origin.call_id.clone(),
                assessment.assessment.lease.clone().unwrap(),
                30,
            )
            .unwrap()
            .authorization;
        let mut request =
            RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
                tool_use_id: binding.origin.call_id.clone(),
                tool_name: "read_file".into(),
                input: input.into(),
                depends_on: vec!["original-dependency".into()],
            });
        request.idempotency_key = binding.effect_idempotency_key();
        request.parent_execution =
            Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "parent-437".into(),
                node_id: "agent-437".into(),
            });
        request.parent_execution_attempt = Some(3);
        request.governed_plan_id = "parent-437".into();
        request.governed_plan_revision = 7;
        request.observation_wave_sequence = 11;
        request.session_id = Some("session-437".into());
        request.policy_revision = 7;
        request.category = crate::ToolSafetyCategory::ReadOnly;
        request.authorized_scopes = vec!["session:session-437".into()];
        request.model_lease = Some("original-model".into());
        request.compiled_invocation = Some(invocation);
        request.step_binding = Some(binding);
        request.authorization = Some(authorization);
        request
    }

    pub(crate) fn graph_for(request: RuntimeToolExecutionRequest) -> ExecutionGraph {
        let parent = request.parent_execution.clone();
        let payload = encode_bound_agent_tool("agent-437", "run-437", request).unwrap();
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let mut graph = ExecutionGraph::new("Title is not a carrier discriminator");
        graph.id = format!("agent-tool-batch:{digest}");
        graph.parent_execution = parent;
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        node.id = format!("agent-tool:{digest}");
        node.idempotency_key = node.id.clone();
        graph.nodes.push(node);
        graph
    }

    fn unchecked_test_pack(value: &serde_json::Value, session: &str) -> String {
        let bytes = serde_json::to_vec(value).unwrap();
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(&mut encoder, &bytes).unwrap();
        let compressed = encoder.finish().unwrap();
        serde_json::json!({"kind":"cowd_bound_agent_tool_zlib_v1","session_id":session,
            "decoded_bytes":bytes.len(),"sha256":format!("sha256:{:x}",Sha256::digest(&bytes)),
            "body":base64::engine::general_purpose::STANDARD.encode(compressed)})
        .to_string()
    }

    #[test]
    fn bound_437_real_session_step_grant_request_roundtrip() {
        let workspace = tempfile::tempdir().unwrap();
        let original = real_request(workspace.path());
        let original_json = serde_json::to_value(&original).unwrap();
        let input_digest = format!("{:x}", Sha256::digest(original.input.as_bytes()));
        let graph = graph_for(original.clone());
        let packed: serde_json::Value = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
        assert_eq!(packed["kind"], "cowd_bound_agent_tool_zlib_v1");
        assert_eq!(packed["session_id"], "session-437");
        let bound = decode_bound_agent_tool_graph(&graph).unwrap();
        assert_eq!(serde_json::to_value(&bound.request).unwrap(), original_json);
        assert_eq!(bound.request, original);
        assert_eq!(
            format!("{:x}", Sha256::digest(bound.request.input.as_bytes())),
            input_digest
        );
        let old_step = original.step_binding.as_ref().unwrap();
        let new_step = bound.request.step_binding.as_ref().unwrap();
        assert_eq!(
            new_step.snapshot.provider_attempt.request_digest,
            old_step.snapshot.provider_attempt.request_digest
        );
        assert_eq!(new_step.origin, old_step.origin);
        assert_eq!(bound.request.authorization, original.authorization);
        assert_eq!(bound.request.parent_execution_attempt, Some(3));
        let digest = format!(
            "{:x}",
            Sha256::digest(graph.nodes[0].payload_ref.as_bytes())
        );
        assert_eq!(graph.id, format!("agent-tool-batch:{digest}"));
        assert_eq!(graph.nodes[0].id, format!("agent-tool:{digest}"));
        assert!(is_bound_agent_tool_graph(&graph));
    }

    #[test]
    fn bound_437_foreign_parent_stored_hash_and_body_kind_reject_at_exact_layer() {
        let workspace = tempfile::tempdir().unwrap();
        let graph = graph_for(real_request(workspace.path()));
        assert!(is_bound_agent_tool_graph(&graph));
        let mut foreign = graph.clone();
        foreign.parent_execution.as_mut().unwrap().execution_id = "foreign-parent".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("original parent mismatch"));
        let mut foreign = graph.clone();
        foreign.id = "agent-tool-batch:foreign".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut foreign = graph.clone();
        foreign.nodes[0].id = "agent-tool:foreign".into();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        let mut body = crate::step_snapshot::decode_persisted_bound_agent_tool_value(
            &graph.nodes[0].payload_ref,
        )
        .unwrap();
        body["request"]["input"] = serde_json::json!("{\"path\":\"different.txt\"}");
        let mut foreign = graph.clone();
        foreign.nodes[0].payload_ref =
            crate::step_snapshot::encode_persisted_bound_agent_tool(&body).unwrap();
        assert!(decode_bound_agent_tool_graph(&foreign)
            .unwrap_err()
            .to_string()
            .contains("stored payload hash mismatch"));
        body["kind"] = serde_json::json!("turn_tool_batch");
        let payload = crate::step_snapshot::encode_persisted_bound_agent_tool(&body).unwrap();
        assert!(decode_bound_agent_tool(&payload)
            .unwrap_err()
            .to_string()
            .contains("typed identity invalid"));
        let mut renamed = graph.clone();
        renamed.objective = "Ordinary root: do not infer authority from this title".into();
        assert!(is_bound_agent_tool_graph(&renamed));
    }

    #[test]
    fn bound_437_empty_session_oversize_raw_malformed_and_cross_kind_reject() {
        let workspace = tempfile::tempdir().unwrap();
        let request = real_request(workspace.path());
        let packed = encode_bound_agent_tool("agent-437", "run-437", request.clone()).unwrap();
        assert!(decode_bound_agent_tool(&packed).is_ok());
        let body = crate::step_snapshot::decode_persisted_bound_agent_tool_value(&packed).unwrap();
        assert!(decode_bound_agent_tool(&body.to_string()).is_err());
        assert!(crate::step_snapshot::encode_persisted_tool_batch(&body).is_err());
        let mut top_only = body.clone();
        top_only["session_id"] = serde_json::json!("session-437");
        top_only["request"]
            .as_object_mut()
            .unwrap()
            .remove("session_id");
        assert!(crate::step_snapshot::encode_persisted_bound_agent_tool(&top_only).is_err());
        let normal = crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
            "session_id":"session-437","calls":[],"step_snapshots":{}
        }))
        .unwrap();
        assert!(crate::step_snapshot::decode_persisted_tool_batch_value(&normal).is_ok());
        assert!(decode_bound_agent_tool(&normal).is_err());
        assert!(crate::step_snapshot::decode_persisted_tool_batch_value(&packed).is_err());
        for session in [None, Some(""), Some("   ")] {
            let mut empty = request.clone();
            empty.session_id = session.map(str::to_string);
            assert!(encode_bound_agent_tool("agent-437", "run-437", empty)
                .unwrap_err()
                .to_string()
                .contains("typed identity invalid"));
            let mut empty_body = body.clone();
            empty_body["request"]["session_id"] = serde_json::to_value(session).unwrap();
            let forged = unchecked_test_pack(&empty_body, "session-437");
            assert!(decode_bound_agent_tool(&forged)
                .unwrap_err()
                .to_string()
                .contains("Session conflict"));
        }
        let original: serde_json::Value = serde_json::from_str(&packed).unwrap();
        for (field, value) in [
            ("session_id", serde_json::json!("foreign")),
            ("sha256", serde_json::json!("sha256:foreign")),
            ("decoded_bytes", serde_json::json!(usize::MAX)),
            ("body", serde_json::json!("!!!!")),
            ("kind", serde_json::json!("cowd_tool_batch_zlib_v1")),
        ] {
            let mut forged = original.clone();
            forged[field] = value;
            assert!(
                decode_bound_agent_tool(&forged.to_string()).is_err(),
                "{field}"
            );
        }
        let duplicate = packed.replacen("{", "{\"kind\":\"duplicate\",", 1);
        assert!(decode_bound_agent_tool(&duplicate).is_err());
        let mut oversized = request;
        oversized.input = "x".repeat(1024 * 1024);
        assert!(encode_bound_agent_tool("agent-437", "run-437", oversized)
            .unwrap_err()
            .to_string()
            .contains("decoded budget exceeded"));
        assert!(decode_bound_agent_tool(&" ".repeat(1024 * 1024 + 1))
            .unwrap_err()
            .to_string()
            .contains("encoded budget exceeded"));
    }

    #[tokio::test]
    async fn bound_437_resolves_original_full_width_fingerprint() {
        let workspace = tempfile::tempdir().unwrap();
        let services = RuntimeServices::in_memory().unwrap();
        let resolver = AgentToolBatchResolver { services: Arc::downgrade(&services) };
        for fingerprint in [0_u64, 7644217367651223534, u64::MAX] {
            let mut request = real_request(workspace.path());
            let mut decision = crate::execution_core::strategy_decision::build_runtime_execution_decision("original test", None);
            decision.lease.input_fingerprint = fingerprint.to_string();
            request.execution_decision = Some(decision);
            let graph = graph_for(request);
            let node = &graph.nodes[0];
            let ticket = NodeExecutionTicket {
                graph_id: graph.id.clone(), node_id: node.id.clone(),
                executor_kind: "tool_batch".into(), service_class: graph.service_class,
                attempt: 1, idempotency_key: node.idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            let decoded = decode_bound_agent_tool(&ticket.payload_ref).unwrap();
            assert_eq!(decoded.request.execution_decision.unwrap().lease.input_fingerprint, fingerprint.to_string());
            assert!(resolver.resolve(&ticket).is_some());
        }
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn bound_437_real_resolver_requires_codec_and_actual_ticket_payload_hash() {
        let workspace = tempfile::tempdir().unwrap();
        let graph = graph_for(real_request(workspace.path()));
        let services = RuntimeServices::in_memory().unwrap();
        let resolver = AgentToolBatchResolver {
            services: Arc::downgrade(&services),
        };
        let node = &graph.nodes[0];
        let ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id.clone(),
            executor_kind: "tool_batch".into(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        assert!(resolver.resolve(&ticket).is_some());
        for mutation in ["graph", "node", "raw", "normal", "session"] {
            let mut invalid = ticket.clone();
            match mutation {
                "graph" => invalid.graph_id = "agent-tool-batch:foreign".into(),
                "node" => invalid.node_id = "agent-tool:foreign".into(),
                "raw" => {
                    invalid.payload_ref =
                        crate::step_snapshot::decode_persisted_bound_agent_tool_value(
                            &ticket.payload_ref,
                        )
                        .unwrap()
                        .to_string()
                }
                "normal" => {
                    invalid.payload_ref =
                        crate::step_snapshot::encode_persisted_tool_batch(&serde_json::json!({
                            "session_id":"session-437","calls":[],"step_snapshots":{}
                        }))
                        .unwrap()
                }
                _ => {
                    let mut envelope: serde_json::Value =
                        serde_json::from_str(&ticket.payload_ref).unwrap();
                    envelope["session_id"] = serde_json::json!("foreign");
                    invalid.payload_ref = envelope.to_string();
                }
            }
            assert!(resolver.resolve(&invalid).is_none(), "{mutation}");
        }
        services.shutdown_execution().await;
    }

    #[test]
    fn r1_450_bound_pure_builder_preserves_raw_failure_and_never_authors_goal_or_replan() {
        let workspace = tempfile::tempdir().unwrap();
        let request = real_request(workspace.path());
        for status in [
            crate::RuntimeToolExecutionStatus::Executed,
            crate::RuntimeToolExecutionStatus::Failed,
        ] {
            let raw = RuntimeToolExecutionOutcome { pending_agent_batch: None,
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: status.clone(),
                category: request.category,
                output: Some("original UTF8 body".repeat(1024)),
                error: (status == crate::RuntimeToolExecutionStatus::Failed)
                    .then(|| "original rejection".into()),
                evidence_ref: "original-effect-reference".into(),
                observed_evidence: Vec::new(),
            };
            let delivered = agent_tool_outcome_at(&raw, 123456).unwrap();
            let decoded: RuntimeToolExecutionOutcome = serde_json::from_str(
                delivered
                    .result
                    .result_ref
                    .as_ref()
                    .unwrap()
                    .strip_prefix("agent-tool-outcome:")
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(decoded, raw);
            assert_eq!(delivered.result.finished_at_ms, 123456);
            assert_eq!(delivered.result.status, ExecutionNodeStatus::Completed);
            assert!(delivered.domain_events.is_empty());
            assert!(delivered.replan.is_none());
            assert!(delivered.delivery_envelope.is_none());
            assert!(delivered.terminal_presentation.is_none());
        }
    }

    #[cfg(test)]
    mod nodispatch_520_tests {
        use super::*;

        fn observed_request() -> (
            tempfile::TempDir,
            RuntimeToolExecutionRequest,
            NodeExecutionTicket,
            Arc<NoDispatchObservation>,
        ) {
            let workspace = tempfile::tempdir().unwrap();
            let request = real_request(workspace.path());
            let graph = graph_for(request.clone());
            let node = &graph.nodes[0];
            let ticket = NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: node.id.clone(),
                executor_kind: node.executor_kind.clone(),
                service_class: graph.service_class,
                attempt: 1,
                idempotency_key: node.idempotency_key.clone(),
                payload_ref: node.payload_ref.clone(),
            };
            let observation = NoDispatchObservation::new(&ticket).unwrap();
            (workspace, request, ticket, observation)
        }

        #[test]
        fn closed_member_custody_authenticates_empty_prefix_and_exact_units() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.verify_closed_member_custody(&ticket, &request.tool_use_id, &[]).is_err());
            observation.close();
            observation.verify_closed_member_custody(&ticket, &request.tool_use_id, &[]).unwrap();
            assert!(observation.verify_closed_member_custody(&ticket, "foreign-call", &[]).is_err());
            let mut wrong_ticket = ticket.clone(); wrong_ticket.attempt += 1;
            assert!(observation.verify_closed_member_custody(&wrong_ticket, &request.tool_use_id, &[]).is_err());

            let (_workspace, request, ticket, observation) = observed_request();
            let unit = Arc::new(crate::tool_host::OriginalMemberSettlement::default());
            observation.register_member_custody(&ticket, &request.tool_use_id, 0, Arc::clone(&unit)).unwrap();
            observation.close();
            // A registered prefix cannot disappear, including a failed carrier insertion.
            assert!(observation.verify_closed_member_custody(&ticket, &request.tool_use_id, &[]).is_err());
            observation.verify_closed_member_custody(&ticket, &request.tool_use_id, &[(0, Arc::clone(&unit))]).unwrap();
            let foreign = Arc::new(crate::tool_host::OriginalMemberSettlement::default());
            assert!(observation.verify_closed_member_custody(&ticket, &request.tool_use_id, &[(0, foreign)]).is_err());
            assert!(observation.verify_closed_member_custody(&ticket, &request.tool_use_id, &[(1, Arc::clone(&unit))]).is_err());
            assert!(observation.register_member_custody(&ticket, &request.tool_use_id, 1, unit).is_err());
        }

        fn failed_outcome() -> NodeExecutionOutcome {
            NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Failed,
                result_ref: None,
                wait_class: None,
                summary: Some("admission refused before dispatch".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            })
        }

        #[test]
        fn original_ticket_accepts_json_format_only_but_rejects_changed_call() {
            let (_workspace, request, ticket, _) = observed_request();
            assert!(validate_original_call_request(&ticket, &request).is_ok());
            let mut equivalent = request.clone();
            equivalent.input = format!("  {}  ", request.input);
            assert!(validate_original_call_request(&ticket, &equivalent).is_ok());
            let mut changed = request.clone();
            changed.input = "{}".into();
            assert!(validate_original_call_request(&ticket, &changed).is_err());
            let mut foreign = equivalent;
            foreign.policy_revision += 1;
            assert!(validate_original_call_request(&ticket, &foreign).is_err());
        }

        #[test]
        fn own834_canonical_rejection_failure_cannot_become_local_refusal() {
            for reason in [
                "step_binding_stale",
                "stale Step rejection receipt commit failed",
                "stale Step rejection has no original Process Step",
            ] {
                let (_workspace, request, ticket, observation) = observed_request();
                assert!(observation.enter());
                observation.refuse(&request.tool_use_id, "step_binding_stale".into());
                observation.uncertain(&request);
                let returned = finish_observed_outcome(
                    &ticket,
                    Some(&observation),
                    Err(NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: reason.into(),
                    }),
                );
                assert!(returned.is_err(), "{reason}");
                assert!(observation.refusal(&request).is_none());
                assert!(observation.witness().is_none());
            }
        }

        #[test]
        fn inherited_effect_cannot_become_zero_history_nodispatch() {
            let (_workspace, request, ticket, observation) = observed_request();
            observation.uncertain(&request);
            observation.refuse(&request.tool_use_id, "child admission failed".into());
            observation.close();
            assert!(observation.witness().is_none());
            assert!(observation.refusal(&request).is_none());
            assert!(finish_observed_outcome(&ticket, Some(&observation),
                Err(NodeExecutorError::Uncertain { node_id: ticket.node_id.clone(),
                    reason: "original effect remains unsettled".into() })).is_err());
        }

        #[test]
        fn nodispatch_520_close_before_first_poll_seals_original_call() {
            let (_workspace, request, ticket, observation) = observed_request();
            observation.close();
            let witness = observation
                .witness()
                .expect("unpolled owner must be sealed");
            assert!(witness.fact().never_polled);
            assert_eq!(witness.fact().calls.len(), 1);
            assert_eq!(witness.fact().calls[0].call_id, request.tool_use_id);
            assert_eq!(
                witness.fact().calls[0].request_sha256,
                Some(no_dispatch_digest(&request).unwrap())
            );
            assert_eq!(
                witness.fact().calls[0].reason,
                "cancelled_before_first_poll"
            );
            assert!(witness.fact().result_sha256.is_none());
            witness.authenticate(&ticket, None).unwrap();
            assert!(!observation.enter());
        }

        #[test]
        fn nodispatch_520_enter_then_close_cannot_claim_never_polled() {
            let (_workspace, _request, _ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.close();
            observation.close();
            assert!(
                observation.witness().is_none(),
                "entered owner has no proof of non-dispatch"
            );
            assert!(!observation.enter());
        }

        #[test]
        fn nodispatch_520_uncertain_absorbs_prior_and_later_refusals() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "admission closed".into());
            assert!(observation.refusal(&request).is_some());
            let mut refused = failed_outcome();
            observation.finish(&mut refused).unwrap();
            refused
                .no_dispatch
                .as_ref()
                .unwrap()
                .authenticate(&ticket, Some(&refused.result))
                .unwrap();
            observation.uncertain(&request);
            assert!(observation.witness().is_none());
            observation.refuse(&request.tool_use_id, "later refusal".into());
            assert!(observation.refusal(&request).is_none());
            let mut after_uncertainty = failed_outcome();
            observation.finish(&mut after_uncertainty).unwrap();
            assert!(after_uncertainty.no_dispatch.is_none());
            observation.close();
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_520_refusal_requires_original_full_request_digest() {
            let (_workspace, request, _ticket, observation) = observed_request();
            observation.refuse(&request.tool_use_id, "binding admission closed".into());
            let refusal = observation
                .refusal(&request)
                .expect("exact request is refused");
            assert_eq!(refusal.status, crate::RuntimeToolExecutionStatus::Failed);
            assert!(refusal.output.is_none());
            assert_eq!(refusal.error.as_deref(), Some("binding admission closed"));
            for field in ["input", "tool", "effect", "attempt", "policy", "session"] {
                let mut altered = request.clone();
                match field {
                    "input" => altered.input.push(' '),
                    "tool" => altered.tool_name = "write_file".into(),
                    "effect" => altered.idempotency_key.push_str("-copied"),
                    "attempt" => altered.parent_execution_attempt = Some(4),
                    "policy" => altered.policy_revision += 1,
                    "session" => altered.session_id = Some("foreign-session".into()),
                    _ => unreachable!(),
                }
                assert!(observation.refusal(&altered).is_none(), "{field}");
            }
            assert!(observation.refusal(&request).is_some());
        }

        fn original_mixed_ticket622() -> NodeExecutionTicket {
            NodeExecutionTicket {
                graph_id: "legacy-mixed-622".into(),
                node_id: "tools-mixed-622".into(),
                executor_kind: "tool_batch".into(),
                service_class: Default::default(),
                attempt: 1,
                idempotency_key: "original-mixed-622".into(),
                payload_ref: crate::step_snapshot::encode_persisted_tool_batch(
                    &serde_json::json!({
                        "session_id":"session-mixed-622", "step_snapshots":{},
                        "calls":[
                            {"id":"refused-622","name":"read_file","input":"{}","depends_on":[]},
                            {"id":"dispatched-622","name":"read_file","input":"{}","depends_on":[]}
                        ]
                    }),
                )
                .unwrap(),
            }
        }

        #[tokio::test]
        async fn nodispatch_622_legacy_mixed_duration_preserves_subset_and_dispatched_leaf() {
            use crate::execution_core::NodeExecutor;

            let ticket = original_mixed_ticket622();
            let executor = crate::execution_core::graph::ScopedNodeExecutor::new("tool_batch");
            let observation = executor.observe_binding(&ticket).unwrap();
            assert!(observation.enter());
            let request = RuntimeToolExecutionRequest::from_tool_request(
                &crate::tool_dispatch::ToolRequest {
                    tool_use_id: "dispatched-622".into(),
                    tool_name: "read_file".into(),
                    input: "{}".into(),
                    depends_on: Vec::new(),
                },
            );
            observation.uncertain(&request);
            let workspace = tempfile::tempdir().unwrap();
            let path = workspace.path().join("actual-leaf.txt");
            std::fs::write(&path, "actual original mixed read").unwrap();
            let leaf = executor
                .execute_physical_tool(
                    &ticket,
                    "dispatched-622",
                    Box::pin(async move {
                        let output = std::fs::read_to_string(&path).unwrap();
                        Ok(RuntimeToolExecutionOutcome { pending_agent_batch: None,
                            tool_use_id: "dispatched-622".into(),
                            tool_name: "read_file".into(),
                            status: crate::RuntimeToolExecutionStatus::Executed,
                            category: crate::ToolSafetyCategory::ReadOnly,
                            output: Some(output),
                            error: None,
                            evidence_ref: String::new(),
                            observed_evidence: Vec::new(),
                        })
                    }),
                )
                .await
                .unwrap();
            assert_eq!(leaf.status, crate::RuntimeToolExecutionStatus::Executed);
            assert_eq!(leaf.output.as_deref(), Some("actual original mixed read"));
            observation.refuse("refused-622", "original scope refusal".into());
            observation.refuse("dispatched-622", "must not hide the actual dispatch".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let sealed = outcome.clone();
            let original = outcome.no_dispatch.as_ref().unwrap().clone();
            assert_eq!(original.fact().calls.len(), 1);
            assert_eq!(original.fact().calls[0].call_id, "refused-622");
            observation
                .finalize_duration(&ticket, &mut outcome, 37)
                .unwrap();
            let finalized = outcome.no_dispatch.as_ref().unwrap();
            finalized
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            assert!(original
                .authenticate(&ticket, Some(&outcome.result))
                .is_err());
            let mut expected = sealed.result;
            expected.usage.duration_ms = 37;
            assert_eq!(outcome.result, expected);
            let mut expected_fact = original.fact().clone();
            expected_fact.result_sha256 = finalized.fact().result_sha256.clone();
            assert_eq!(*finalized.fact(), expected_fact);
            assert_eq!(observation.witness().as_ref(), Some(finalized));
            assert_eq!(
                observation.state.lock().unwrap().uncertain,
                vec!["dispatched-622".to_string()]
            );
            observation.refuse("dispatched-622", "late dispatch refusal".into());
            assert_eq!(observation.witness().as_ref(), Some(finalized));
            executor.cancel(&ticket).await.unwrap();
            assert_eq!(observation.witness().as_ref(), Some(finalized));
            observation.uncertain(&request);
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 41)
                .is_err());
            assert_eq!(outcome, before);
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_622_duration_rejects_overlapping_uncertainty_with_retained_seal() {
            let ticket = original_mixed_ticket622();
            let observation = NoDispatchObservation::new(&ticket).unwrap();
            assert!(observation.enter());
            observation.refuse("refused-622", "original scope refusal".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let witness = observation.witness();
            observation
                .state
                .lock()
                .unwrap()
                .uncertain
                .push("refused-622".into());
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 43)
                .is_err());
            assert_eq!(outcome, before);
            assert_eq!(observation.witness(), witness);
            assert_eq!(
                observation.state.lock().unwrap().uncertain,
                vec!["refused-622".to_string()]
            );
        }

        #[test]
        fn nodispatch_617_duration_finalization_preserves_exact_fact_and_rejects_old_seal() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "original denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let original_result = outcome.result.clone();
            let original = outcome.no_dispatch.clone().unwrap();
            observation
                .finalize_duration(&ticket, &mut outcome, 17)
                .unwrap();
            let finalized = outcome.no_dispatch.clone().unwrap();
            finalized
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            assert!(original
                .authenticate(&ticket, Some(&outcome.result))
                .is_err());
            assert!(finalized
                .authenticate(&ticket, Some(&original_result))
                .is_err());
            let mut expected = original_result;
            expected.usage.duration_ms = 17;
            assert_eq!(outcome.result, expected);
            let mut expected_fact = original.fact().clone();
            expected_fact.result_sha256 = finalized.fact().result_sha256.clone();
            assert_eq!(*finalized.fact(), expected_fact);
            assert_eq!(observation.witness(), Some(finalized.clone()));
            observation
                .finalize_duration(&ticket, &mut outcome, 1)
                .unwrap();
            assert_eq!(outcome.result.usage.duration_ms, 17);
            assert_eq!(outcome.no_dispatch, Some(finalized));
            observation
                .finalize_duration(&ticket, &mut outcome, 23)
                .unwrap();
            outcome
                .no_dispatch
                .as_ref()
                .unwrap()
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            assert_eq!(outcome.result.usage.duration_ms, 23);
        }

        #[test]
        fn nodispatch_617_duration_rejects_tampered_result_without_resealing() {
            for field in [
                "summary", "status", "result", "usage", "duration", "finished", "failure",
            ] {
                let (_workspace, request, ticket, observation) = observed_request();
                assert!(observation.enter());
                observation.refuse(&request.tool_use_id, "original denied".into());
                let mut outcome = failed_outcome();
                observation.finish(&mut outcome).unwrap();
                let witness = observation.witness();
                match field {
                    "summary" => outcome.result.summary = Some("foreign".into()),
                    "status" => outcome.result.status = ExecutionNodeStatus::Completed,
                    "result" => outcome.result.result_ref = Some("foreign".into()),
                    "usage" => outcome.result.usage.tool_calls += 1,
                    "duration" => outcome.result.usage.duration_ms += 1,
                    "finished" => outcome.result.finished_at_ms += 1,
                    _ => {
                        outcome.result.failure =
                            Some(harness_contract::execution_graph::ExecutionFailure {
                                kind: "foreign".into(),
                                message: "foreign".into(),
                                retryable: false,
                                evidence_refs: Vec::new(),
                            })
                    }
                }
                let before = outcome.clone();
                assert!(
                    observation
                        .finalize_duration(&ticket, &mut outcome, 19)
                        .is_err(),
                    "{field}"
                );
                assert_eq!(outcome, before);
                assert_eq!(observation.witness(), witness);
            }
        }

        #[test]
        fn nodispatch_617_duration_rejects_foreign_ticket_witness_and_uncertain_reset() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "original denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let original = outcome.clone();
            let witness = observation.witness();
            let mut foreign_ticket = ticket.clone();
            foreign_ticket.attempt += 1;
            assert!(observation
                .finalize_duration(&foreign_ticket, &mut outcome, 29)
                .is_err());
            assert_eq!(outcome, original);
            assert_eq!(observation.witness(), witness);
            let other = NoDispatchObservation::new(&ticket).unwrap();
            assert!(other.finalize_duration(&ticket, &mut outcome, 29).is_err());
            assert!(other.witness().is_none());
            assert_eq!(outcome, original);
            assert!(other.enter());
            other.refuse(
                &request.tool_use_id,
                "different original classification".into(),
            );
            let mut foreign = failed_outcome();
            other.finish(&mut foreign).unwrap();
            foreign
                .no_dispatch
                .as_ref()
                .unwrap()
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            outcome.no_dispatch = foreign.no_dispatch;
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 29)
                .is_err());
            assert_eq!(outcome, before);
            assert_eq!(observation.witness(), witness);
            outcome = original;
            observation.uncertain(&request);
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 29)
                .is_err());
            assert_eq!(outcome, before);
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_617_duration_never_mints_or_drops_a_witness() {
            let (_workspace, request, ticket, observation) = observed_request();
            let mut outcome = failed_outcome();
            observation
                .finalize_duration(&ticket, &mut outcome, 0)
                .unwrap();
            assert_eq!(outcome.result.usage.duration_ms, 1);
            assert!(outcome.no_dispatch.is_none());
            assert!(observation.witness().is_none());
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "original denied".into());
            observation.finish(&mut outcome).unwrap();
            let witness = observation.witness();
            outcome.no_dispatch = None;
            let before = outcome.clone();
            assert!(observation
                .finalize_duration(&ticket, &mut outcome, 31)
                .is_err());
            assert_eq!(outcome, before);
            assert_eq!(observation.witness(), witness);
        }

        #[test]
        fn nodispatch_520_cloned_witness_rejects_mutated_ticket_and_result() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "admission denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let witness = outcome.no_dispatch.as_ref().unwrap().clone();
            witness
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            for field in [
                "graph",
                "node",
                "attempt",
                "payload",
                "idempotency",
                "executor",
            ] {
                let mut foreign = ticket.clone();
                match field {
                    "graph" => foreign.graph_id.push_str("-foreign"),
                    "node" => foreign.node_id.push_str("-foreign"),
                    "attempt" => foreign.attempt += 1,
                    "payload" => foreign.payload_ref.push(' '),
                    "idempotency" => foreign.idempotency_key.push_str("-foreign"),
                    "executor" => foreign.executor_kind = "inline_model".into(),
                    _ => unreachable!(),
                }
                assert!(
                    witness
                        .authenticate(&foreign, Some(&outcome.result))
                        .is_err(),
                    "{field}"
                );
            }
            let mut copied_result = outcome.result.clone();
            copied_result.summary = Some("copied result".into());
            assert!(witness.authenticate(&ticket, Some(&copied_result)).is_err());
            let mut copied_result = outcome.result.clone();
            copied_result.finished_at_ms += 1;
            assert!(witness.authenticate(&ticket, Some(&copied_result)).is_err());
            witness
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
        }

        #[test]
        fn nodispatch_520_finish_without_refusal_creates_no_closure() {
            let (_workspace, _request, _ticket, observation) = observed_request();
            assert!(observation.enter());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            assert!(outcome.no_dispatch.is_none());
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_520_finish_refused_call_seals_exact_failed_result() {
            let (_workspace, request, ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "resource admission denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let witness = outcome.no_dispatch.as_ref().unwrap();
            assert!(!witness.fact().never_polled);
            assert_eq!(witness.fact().calls.len(), 1);
            assert_eq!(witness.fact().calls[0].call_id, request.tool_use_id);
            assert_eq!(witness.fact().calls[0].reason, "resource admission denied");
            assert_eq!(
                witness.fact().result_sha256,
                Some(no_dispatch_digest(&outcome.result).unwrap())
            );
            witness
                .authenticate(&ticket, Some(&outcome.result))
                .unwrap();
            assert_eq!(observation.witness().as_ref(), Some(witness));
        }

        #[test]
        fn nodispatch_520_unknown_or_empty_refusal_cannot_close_original_call() {
            let (_workspace, request, _ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse("foreign-call", "admission denied".into());
            observation.refuse(&request.tool_use_id, String::new());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            assert!(observation.refusal(&request).is_none());
            assert!(outcome.no_dispatch.is_none());
            assert!(observation.witness().is_none());
        }

        #[test]
        fn nodispatch_520_never_polled_witness_rejects_any_node_result() {
            let (_workspace, _request, ticket, observation) = observed_request();
            observation.close();
            let witness = observation.witness().unwrap();
            witness.authenticate(&ticket, None).unwrap();
            assert!(witness
                .authenticate(&ticket, Some(&failed_outcome().result))
                .is_err());
        }

        #[test]
        fn nodispatch_520_serde_cannot_inject_or_roundtrip_private_witness() {
            let (_workspace, request, _ticket, observation) = observed_request();
            assert!(observation.enter());
            observation.refuse(&request.tool_use_id, "admission denied".into());
            let mut outcome = failed_outcome();
            observation.finish(&mut outcome).unwrap();
            let witness = outcome.no_dispatch.as_ref().unwrap();
            let encoded = serde_json::to_value(&outcome).unwrap();
            assert!(encoded.get("no_dispatch").is_none());
            let roundtrip: NodeExecutionOutcome = serde_json::from_value(encoded.clone()).unwrap();
            assert!(roundtrip.no_dispatch.is_none());
            assert_eq!(roundtrip.result, outcome.result);
            let fact = serde_json::to_value(witness.fact()).unwrap();
            for injection in [fact.clone(), serde_json::json!({"fact": fact})] {
                let mut forged = encoded.clone();
                forged["no_dispatch"] = injection;
                match serde_json::from_value::<NodeExecutionOutcome>(forged) {
                    Err(_) => {}
                    Ok(decoded) => {
                        assert!(
                            decoded.no_dispatch.is_none(),
                            "serialized facts must not become owner-authored witnesses"
                        );
                        assert_eq!(decoded.result, outcome.result);
                    }
                }
            }
        }
    }
}


/// A7: the member agent's first model step issues `state_inspect` next to
/// `workspace_snapshot`. `state_inspect` is an agent action of the Read family,
/// so it reaches the agent-tool node through the turn's governed ToolBatch plan
/// (tool-dag plan id, governed hook installed) while the Read family never
/// installs a first-effect owner. Before the fix that shape was reported as
/// `original governed effect owner missing` (Uncertain), the node blocked, and
/// the member turn never reached a terminal or a durable receipt.
#[cfg(test)]
pub(crate) mod a7_member_read_action_tests {
    use super::*;

    const SESSION: &str = "a7-session";
    const TURN: &str = "a7-turn";
    const GRAPH: &str = "a7-member-graph";
    const NODE: &str = "a7-member-node";
    const RUN: &str = "a7-member-run";
    const AGENT: &str = "a7-member-agent";
    const TASK: &str = "task:a7-member-task";
    const PLAN: &str = "tool-dag-a7-member-read";

    /// A member AgentTask that stays running: the turn's tool batch and the
    /// Read family's admission both fence on the parent node being live, and a
    /// unit fixture has no provider to finish the turn with.
    struct PendingAgentTask;

    impl crate::execution_core::graph::executors::AgentTaskBackendResolver for PendingAgentTask {
        fn resolve(
            &self,
            _packet: &harness_contract::agent::AgentTaskPacket,
        ) -> Option<
            Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>,
        > {
            Some(Arc::new(Self))
        }
    }

    #[async_trait::async_trait]
    impl crate::execution_core::graph::executors::AgentTaskBackend for PendingAgentTask {
        async fn execute(
            &self,
            _: harness_contract::agent::AgentTaskPacket,
        ) -> Result<harness_contract::agent::AgentReturnPacket, String> {
            std::future::pending().await
        }
        async fn cancel(
            &self,
            _: &harness_contract::agent::AgentTaskPacket,
        ) -> Result<(), String> {
            Ok(())
        }
        fn cancellation_finalized(&self, _: &harness_contract::agent::AgentTaskPacket) {}
    }

    /// Minimal execution host over a builtin ToolHost: it serves the pinned
    /// catalog and the effect descriptors the member Read action needs, and
    /// fails closed for everything else.
    struct A7Host(tools::ToolHost);

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for A7Host {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.0.pin_snapshot())
        }
        fn delegated_tool_definitions(&self, names: &[String]) -> Vec<crate::ProviderToolDefinition> {
            let requested = names.iter().cloned().collect::<std::collections::BTreeSet<_>>();
            self.0
                .pin_snapshot()
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
        fn delegated_tool_effect_descriptor(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            let lease = self.0.pin_snapshot();
            lease.snapshot().catalog.canonical_name(name)?;
            Some(lease.describe_effect(name, input))
        }
        async fn execute_runtime_tool(
            &self,
            request: &RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            crate::RuntimeToolExecutionOutcome {
                pending_agent_batch: None,
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some("a7 host cannot execute this tool".into()),
                evidence_ref: String::new(),
                observed_evidence: Vec::new(),
            }
        }
    }

    struct A7Fixture {
        services: Arc<RuntimeServices>,
        packet: harness_contract::agent::AgentTaskPacket,
        _workspace: tempfile::TempDir,
    }

    fn a7_packet() -> harness_contract::agent::AgentTaskPacket {
        let mut packet = harness_contract::agent::AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                AGENT,
                RUN,
                TASK,
                SESSION,
                "a7-mission",
                Some("team:a7-member-team"),
                GRAPH,
                NODE,
            ),
            attempt: 1,
            expected_graph_revision: 0,
            objective: "inspect the task".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            cohort_prompt_package: None,
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            agentic_binding: None,
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: vec!["state_inspect".into()],
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            policy_revision: 1,
            model_lease: "a7-model".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "a7-budget",
                AGENT,
                AGENT,
                1,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "a7-key".into(),
        }
        ;
        packet
    }

    /// Compile the packet's Runtime binding the way the production admission
    /// does: the Read family's effect authority fences on it.
    fn a7_bound_packet(
        services: &RuntimeServices,
        mut packet: harness_contract::agent::AgentTaskPacket,
    ) -> harness_contract::agent::AgentTaskPacket {
        let definition_id = harness_contract::agent::AgentDefinitionId::new(
            harness_contract::agent::DefinitionScope::Builtin,
            "cowd/execute",
        )
        .unwrap();
        let mut request = crate::AgentBindingRequest::new(
            definition_id,
            harness_contract::agent::RevisionSelector::LatestApprovedStable,
            packet.agent_id(),
            packet.session_id(),
            packet.task_id(),
        );
        request.team_id = packet.team_id().map(str::to_string);
        request.granted_capabilities = vec![harness_contract::agent::AgentCapability::Read];
        request.allowed_tool_contract_refs = packet.allowed_tools.clone();
        packet.binding = Some(
            crate::AgentBindingCompiler::new(std::sync::Arc::clone(
                services.definition_registry(),
            ))
            .compile(request)
            .unwrap()
            .snapshot,
        );
        packet
    }

    async fn a7_fixture() -> A7Fixture {
        let workspace = tempfile::tempdir().unwrap();
        let actions = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .iter()
            .map(|name| tools::RuntimeToolDefinition {
                name: name.to_string(),
                description: Some("a7 coordination adapter".into()),
                input_schema: serde_json::json!({"type": "object"}),
                required_permission: harness_contract::policy::PermissionMode::ReadOnly,
                effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
                    resolver_id: "runtime.agent_action".into(),
                    resolver_version: 2,
                },
            })
            .collect();
        let catalog = tools::ToolCatalog::builtin().with_runtime_tools(actions).unwrap();
        let host = Arc::new(A7Host(tools::ToolHost::new(
            "a7-host",
            workspace.path(),
            tools::ToolHostSnapshot::new(
                Arc::new(catalog),
                Arc::new(tools::lsp_client::LspRegistry::default()),
                None,
            ),
        )));
        // The host and the services must share one workspace root: the member
        // Read action's effect scope is admitted against the pinned workspace,
        // and a second root would read as a different workspace.
        let services = RuntimeServices::builder(
            workspace.path().join("home"),
            workspace.path(),
        )
        .runtime_event_store(Arc::new(
            crate::runtime_event_store::RuntimeEventStore::for_test(),
        ))
        .artifact_store(Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("test-artifacts"),
        )))
        .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
        .background_work(crate::BackgroundWorkConfig {
            evolution: false,
            growth: false,
        })
        .tool_execution_host(host)
        .build()
        .unwrap();
        let packet = a7_bound_packet(&services, a7_packet());
        let mut graph = ExecutionGraph::new("a7 member fixture");
        graph.id = GRAPH.into();
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: SESSION.into(),
            turn_id: TURN.into(),
            root_task_id: TASK.into(),
            task_id: TASK.into(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::AgentTask,
            "agent_task",
            serde_json::to_string(&packet).unwrap(),
        );
        node.id = NODE.into();
        graph.nodes.push(node);
        services
            .agent_task_executor()
            .install_resolver(Arc::new(PendingAgentTask));
        services.commit_service().register_graph(graph).unwrap();
        let driver = Arc::clone(&services);
        tokio::spawn(async move {
            let _ = driver.execution_supervisor().drive_registered(GRAPH).await;
        });
        for _ in 0..250 {
            let driven = services.graph_state_store().load(GRAPH).unwrap();
            if driven.node_statuses.get(NODE) == Some(&ExecutionNodeStatus::Running) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        services.publish_session_execution_policy(
            SESSION,
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Supervised,
                    1,
                    harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                ),
            ),
        );
        A7Fixture {
            services,
            packet,
            _workspace: workspace,
        }
    }

    fn member_state_inspect_request(fixture: &A7Fixture) -> RuntimeToolExecutionRequest {
        let services = &fixture.services;
        let packet = &fixture.packet;
        let host = services.tool_execution_host().expect("fixture ToolHost");
        let lease = host.pin_step_catalog().expect("pinned catalog");
        let session_policy = services
            .session_execution_policy(SESSION)
            .expect("fixture Session policy");
        let identity = harness_contract::step::StepIdentity {
            step_id: "step-a7-member-read".into(),
            session_id: SESSION.into(),
            turn_id: TURN.into(),
            execution_id: Some(RUN.into()),
            actor_ref: Some(format!("agent:{AGENT}")),
            run_ref: Some(RUN.into()),
            attempt: Some(1),
            epoch: None,
            goal_ref: None,
            program_ref: None,
            task_ref: Some(TASK.into()),
            build_ref: "a7-build".into(),
        };
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == "state_inspect")
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(definitions.len(), 1, "state_inspect must be registered");
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                identity,
                lease.clone(),
                &definitions,
                &serde_json::json!({"profile": "original"}),
                &serde_json::json!({"request": "original"}),
                std::collections::BTreeMap::from([(
                    "policy".into(),
                    session_policy.revision.to_string(),
                )]),
                chrono::Utc::now()
                    .timestamp_millis()
                    .saturating_add(3_600_000),
            )
            .expect("bound Step"),
        );
        let binding = crate::step_snapshot::StepCallBinding::provider(step, "call-a7-member-read");
        let input = serde_json::json!({"scope_ref": TASK});
        let input_text = serde_json::to_string(&input).expect("input serializes");
        let invocation = binding
            .prepare_invocation(Some(lease.clone()), "state_inspect", &input)
            .expect("compiled invocation");
        let policy = crate::PermissionPolicy::new(session_policy.permission_mode);
        let assessment = crate::AuthorizationNegotiator::new().assess_effective(
            &policy,
            &crate::AuthorizationRequest {
                principal_id: format!("agent:{AGENT}"),
                capability: "state_inspect".into(),
                input: input_text.clone(),
                idempotency_key: binding.effect_idempotency_key(),
                effect: invocation.effect.clone(),
                parent_ceiling: packet.permission_ceiling,
                parent_lease_id: None,
                policy_revision: session_policy.revision,
                recovery_scope: RUN.into(),
                context: crate::PermissionContext::default(),
                safe_alternatives: Vec::new(),
            },
        );
        let authorization = crate::ToolPolicy
            .authorize(
                &assessment.effective,
                &assessment.assessment,
                binding.origin.call_id.clone(),
                assessment.assessment.lease.clone().expect("assessed lease"),
                30,
            )
            .expect("authorization")
            .authorization;
        let mut request = RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: binding.origin.call_id.clone(),
                tool_name: "state_inspect".into(),
                input: input_text,
                depends_on: vec![],
            },
        );
        request.idempotency_key = binding.effect_idempotency_key();
        request.compiled_invocation = Some(invocation.clone());
        request.step_binding = Some(binding.clone());
        // The member's first model step reaches the agent-tool node through the
        // turn's governed ToolBatch plan, so the plan id is the tool-dag id and
        // the governed hook the admission installed attests it.
        request.governed_plan_id = PLAN.into();
        request.governed_plan_revision = 1;
        request.observation_wave_sequence = 1;
        request.category = crate::ToolSafetyCategory::from_effect(&invocation.effect);
        request.authorization = Some(authorization);
        request.session_id = Some(SESSION.into());
        request.sandbox_posture = session_policy.sandbox_posture;
        request.policy_revision = session_policy.revision;
        request.authorized_scopes = vec![format!("session:{SESSION}")];
        request.model_lease = Some(packet.model_lease.clone());
        request.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: GRAPH.into(),
            node_id: NODE.into(),
        });
        request.parent_execution_attempt = Some(1);
        request.tool_progress
            .install_original_governed_hook(
                crate::conversation::tool_plane::OriginalGovernedHook::installed_for_test(
                    NodeExecutionTicket {
                        graph_id: GRAPH.into(),
                        node_id: NODE.into(),
                        executor_kind: "tool_batch".into(),
                        service_class: Default::default(),
                        attempt: 1,
                        idempotency_key: format!("{NODE}:attempt"),
                        payload_ref: String::new(),
                    },
                    Some(binding),
                    PLAN.into(),
                    1,
                    SESSION.into(),
                    request.parent_execution.clone(),
                    Some(1),
                    input.clone(),
                    input,
                    invocation,
                ),
            )
            .expect("governed hook installs once");
        request
    }

    /// What the turn's own admission writes before it dispatches the child:
    /// the Read family's `execution.read.intent` on the call's effect stream.
    fn admit_member_read_intent(services: &RuntimeServices, request: &RuntimeToolExecutionRequest) {
        let host = services.tool_execution_host().expect("fixture ToolHost");
        let input: serde_json::Value = serde_json::from_str(&request.input).expect("input parses");
        let descriptor = host
            .delegated_tool_effect_descriptor(&request.tool_name, &input)
            .expect("state_inspect descriptor");
        let scope = crate::bound_tool_batch::admitted_effect_scope(
            request,
            &descriptor,
            host.pin_step_catalog(),
            services.path_identity_resolver(),
        )
        .expect("admitted scope");
        services
            .commit_service()
            .begin_bound_tool_effect(request, &descriptor, &scope)
            .expect("the Read family admits through its read intent");
    }

    fn receipt_pair(
        services: &RuntimeServices,
        request: &RuntimeToolExecutionRequest,
    ) -> Vec<crate::DurableRuntimeEvent> {
        let parent = request.parent_execution.as_ref().expect("parent");
        let store = services.event_store();
        [
            (
                format!("execution-effect:{}", request.idempotency_key),
                format!("{}:read-receipt", request.idempotency_key),
            ),
            (
                format!("execution-agent-receipts:{}:{}:1", parent.execution_id, parent.node_id),
                format!("agent-tool-receipt:{}", request.idempotency_key),
            ),
        ]
        .into_iter()
        .filter_map(|(stream, key)| store.event_by_idempotency_key(&stream, &key).unwrap())
        .collect()
    }

    fn admission_intent_present(
        services: &RuntimeServices,
        request: &RuntimeToolExecutionRequest,
    ) -> bool {
        services
            .commit_service()
            .original_effect_admitted(false, request)
            .expect("admission fact reads")
    }

    /// Register the agent-tool child graph exactly as the original dispatcher
    /// does and retain its child carrier, then drive it through the runner.
    async fn drive_member_read_action(
        fixture: &A7Fixture,
        request: &RuntimeToolExecutionRequest,
    ) -> harness_contract::execution_graph::ExecutionNodeStatus {
        let services = &fixture.services;
        let payload = encode_bound_agent_tool(AGENT, RUN, request.clone()).expect("bound payload");
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let graph_id = format!("agent-tool-batch:{digest}");
        let node_id = format!("agent-tool:{digest}");
        let parent = request.parent_execution.clone().expect("parent");
        let parent_graph = services
            .graph_state_store()
            .load(&parent.execution_id)
            .expect("member graph");
        let mut graph = ExecutionGraph::new("a7 agent tool batch");
        graph.id = graph_id.clone();
        graph.parent_execution = Some(parent.clone());
        graph.lineage = parent_graph.lineage.clone();
        graph.service_class = parent_graph.service_class;
        let mut node = ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
            "tool_batch",
            payload.clone(),
        );
        node.id = node_id.clone();
        node.idempotency_key = node_id.clone();
        graph.nodes.push(node);
        services.commit_service().register_graph(graph).unwrap();
        // The pending carrier must outlive the runner's binding step: its
        // Drop retires the pending entry once the bound observation took it.
        let _pending = services
            .tool_batch_executor()
            .retain_original_agent_child(
                &graph_id,
                &node_id,
                &payload,
                Arc::new(request.clone()),
                NodeExecutionTicket {
                    graph_id: parent.execution_id.clone(),
                    node_id: parent.node_id.clone(),
                    executor_kind: "tool_batch".into(),
                    service_class: Default::default(),
                    attempt: 1,
                    idempotency_key: format!("{}:attempt", parent.node_id),
                    payload_ref: String::new(),
                },
                fixture.packet.clone(),
                Arc::clone(services.event_store()),
                None,
                None,
                None,
            )
            .expect("child carrier");
        services
            .execution_supervisor()
            .drive_registered(&graph_id)
            .await
            .expect("agent tool batch drives");
        drop(_pending);
        services
            .execution_supervisor()
            .wait_for_terminal(&graph_id)
            .await
            .expect("agent tool batch terminalizes");
        services
            .graph_state_store()
            .load(&graph_id)
            .expect("driven graph")
            .node_statuses[&node_id]
    }

    /// T: a call that was refused before it ran leaves a durable refusal fact
    /// under its own effect key, so the batch closure can settle it as
    /// refused instead of waiting for a receipt that will never come.
    ///
    /// The fact is the program's rejection event and its effect-stream twin,
    /// written as one transaction under the refused call's own key. Two
    /// premises decide whether it may exist: the member's own program
    /// binding, and that program's own projection.
    #[tokio::test]
    async fn t_refused_call_leaves_a_durable_fact_under_its_own_key() {
        let fixture = a7_fixture().await;
        let request = member_state_inspect_request(&fixture);
        admit_member_read_intent(&fixture.services, &request);
        let key = request.idempotency_key.clone();
        let store = fixture.services.event_store();
        // The member's own program, as the program lane records it.
        let program_id = "t-program";
        store
            .append(crate::RuntimeEventInput {
                stream_id: format!("agentic-program:{program_id}"),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.program_opened".into(),
                status: Some("open".into()),
                actor: Some("t-root".into()),
                refs: vec![],
                payload: serde_json::json!({
                    "objective_id": "t-objective",
                    "session_id": SESSION,
                    "turn_id": TURN,
                }),
            })
            .expect("program opened");
        let bound = |program: &str| harness_contract::agent::AgenticExecutionBinding {
            program_id: program.into(),
            agent_id: AGENT.into(),
            membership_id: "membership:t".into(),
            team_id: "team:t".into(),
            task_team_id: "team:t".into(),
            source_spec_revision: 1,
            task_purpose: None,
            focus: harness_contract::agent::AgenticExecutionFocus::TaskExecute {
                task_ref: TASK.into(),
            },
        };
        let mut agent = fixture.packet.clone();
        agent.agentic_binding = Some(bound(program_id));
        let outcome = crate::RuntimeToolExecutionOutcome {
            pending_agent_batch: None,
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::BlockedPermission,
            category: request.category,
            output: None,
            error: Some(
                "`read_ref` cannot serve the reference `task:t` in this call: the delegated reader \
                 `state_inspect` is an Agent action and this call carries no original action \
                 capability"
                    .into(),
            ),
            evidence_ref: String::new(),
            observed_evidence: Vec::new(),
        };
        assert!(
            settle_refused_call(&fixture.services, &agent, &request, &outcome),
            "the refused call leaves a durable fact"
        );
        let program_stream = format!("agentic-program:{program_id}");
        let program_event = store
            .event_by_idempotency_key(&program_stream, &format!("completion-rejected:{key}"))
            .expect("program event lookup")
            .expect("program rejection event");
        assert_eq!(program_event.kind, "agentic.completion_rejected");
        assert_eq!(program_event.scope, crate::RuntimeEventScope::Program);
        assert_eq!(program_event.status.as_deref(), Some("rejected"));
        let twin = store
            .event_by_idempotency_key(
                &format!("execution-effect:{key}"),
                &format!("agentic-single:{key}"),
            )
            .expect("twin lookup")
            .expect("effect stream twin");
        assert_eq!(twin.kind, "agentic.batch_member.index");
        assert_eq!(twin.scope, crate::RuntimeEventScope::ExecutionNode);
        assert_eq!(twin.actor.as_deref(), Some("runtime.agentic_action"));
        assert_eq!(twin.status.as_deref(), Some("committed"));
        assert_eq!(twin.payload["outer_key"], key);
        assert_eq!(twin.payload["program_event_kind"], "agentic.completion_rejected");
        assert_eq!(twin.payload["program_stream"], program_stream);
        assert_eq!(
            twin.payload["program_event_key"],
            format!("completion-rejected:{key}")
        );
        // One transaction wrote both, and the receipt names both: this is
        // what the consumer authenticates before it settles the call.
        assert_eq!(program_event.transaction_id, twin.transaction_id);
        assert_eq!(program_event.commit_cursor, twin.commit_cursor);
        let receipt = store
            .original_transaction_receipt(&program_event.transaction_id)
            .expect("transaction receipt")
            .expect("one transaction receipt");
        assert!(receipt.event_ids.contains(&program_event.event_id));
        assert!(receipt.event_ids.contains(&twin.event_id));
        // A call that was never admitted gets no fact at all.
        let mut unadmitted = request.clone();
        unadmitted.idempotency_key = "t-unadmitted-effect".into();
        assert!(
            !settle_refused_call(&fixture.services, &agent, &unadmitted, &outcome),
            "a call without its own admission intent leaves no fact"
        );
        assert_eq!(
            store
                .stream_revision(&format!("execution-effect:{}", unadmitted.idempotency_key))
                .expect("effect stream"),
            0
        );
        assert!(
            store
                .event_by_idempotency_key(
                    &program_stream,
                    &format!("completion-rejected:{}", unadmitted.idempotency_key),
                )
                .expect("program event lookup")
                .is_none(),
            "an unadmitted call leaves no rejection fact behind"
        );
        // A call that ran and failed is not a refusal: it may have left
        // effects, so it must not be settled as terminally refused.
        let mut failed = outcome.clone();
        failed.status = crate::RuntimeToolExecutionStatus::Failed;
        let mut failed_request = request.clone();
        failed_request.idempotency_key = "t-failed-effect".into();
        assert!(
            !settle_refused_call(&fixture.services, &agent, &failed_request, &failed),
            "a failed call is not a refusal"
        );
        // Premise one: a member whose packet names no program gets no fact.
        let mut unbound = agent.clone();
        unbound.agentic_binding = None;
        let mut unbound_request = request.clone();
        unbound_request.idempotency_key = "t-unbound-effect".into();
        assert!(
            !settle_refused_call(&fixture.services, &unbound, &unbound_request, &outcome),
            "a call whose program cannot be named leaves no fact"
        );
        // Premise two: a program that cannot be read leaves no fact.
        let mut absent = agent.clone();
        absent.agentic_binding = Some(bound("t-absent-program"));
        let mut absent_request = request.clone();
        absent_request.idempotency_key = "t-absent-effect".into();
        assert!(
            !settle_refused_call(&fixture.services, &absent, &absent_request, &outcome),
            "a call whose program cannot be read leaves no fact"
        );
    }

    #[tokio::test]
    async fn member_state_inspect_read_action_reaches_a_terminal_with_its_durable_read_receipt() {
        let fixture = a7_fixture().await;
        let request = member_state_inspect_request(&fixture);
        admit_member_read_intent(&fixture.services, &request);
        assert!(admission_intent_present(&fixture.services, &request));
        let status = drive_member_read_action(&fixture, &request).await;
        assert_eq!(
            status,
            harness_contract::execution_graph::ExecutionNodeStatus::Completed,
            "the admitted member Read action runs its node to a terminal"
        );
        let pair = receipt_pair(&fixture.services, &request);
        assert_eq!(pair.len(), 2, "the child committed its durable read receipt pair");
        assert_eq!(pair[0].kind, "execution.read.receipt");
        assert_eq!(pair[1].kind, "execution.agent_tool.receipt");
        assert_eq!(pair[1].payload["effect_kind"], "read");
        assert_eq!(pair[0].transaction_id, pair[1].transaction_id);
        assert_eq!(pair[0].payload["outcome"], pair[1].payload["outcome"]);
        let receipted = &pair[0].payload["outcome"];
        assert_eq!(
            receipted["status"], "blocked_permission",
            "a unit fixture has no live agentic Program, so the action submission \
             itself names the stage it reached instead of ending Unknown"
        );
        assert!(
            receipted["error"]
                .as_str()
                .is_some_and(|error| error.contains("agent_actor")),
            "the named reason proves execution passed the fixed admission check, \
             re-validated the read intent and reached the action submission"
        );
        assert!(
            fixture
                .services
                .event_store()
                .event_by_idempotency_key(
                    &format!("execution-effect:{}", request.idempotency_key),
                    &format!("{}:receipt", request.idempotency_key)
                )
                .unwrap()
                .is_none(),
            "the Read family never writes a mutation effect receipt"
        );
        assert!(fixture
            .services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }

    #[tokio::test]
    async fn member_read_action_without_durable_admission_is_not_dispatched() {
        let fixture = a7_fixture().await;
        let request = member_state_inspect_request(&fixture);
        assert!(!admission_intent_present(&fixture.services, &request));
        let payload = encode_bound_agent_tool(AGENT, RUN, request.clone()).expect("bound payload");
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let graph_id = format!("agent-tool-batch:{digest}");
        let node_id = format!("agent-tool:{digest}");
        let parent = request.parent_execution.clone().expect("parent");
        // The pending carrier must outlive the binding step: its Drop retires
        // the pending entry once the bound observation took it.
        let _pending = fixture
            .services
            .tool_batch_executor()
            .retain_original_agent_child(
                &graph_id,
                &node_id,
                &payload,
                Arc::new(request.clone()),
                // The refusal this test asserts fires before any parent-ticket
                // use, so the carrier's parent ticket only has to name the same
                // physical owner coordinates the request carries.
                NodeExecutionTicket {
                    graph_id: parent.execution_id.clone(),
                    node_id: parent.node_id.clone(),
                    executor_kind: "tool_batch".into(),
                    service_class: Default::default(),
                    attempt: 1,
                    idempotency_key: format!("{}:attempt", parent.node_id),
                    payload_ref: String::new(),
                },
                fixture.packet.clone(),
                Arc::clone(fixture.services.event_store()),
                None,
                None,
                None,
            )
            .expect("child carrier");
        let ticket = NodeExecutionTicket {
            graph_id,
            node_id,
            executor_kind: "tool_batch".into(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: format!("agent-tool:{digest}"),
            payload_ref: payload,
        };
        fixture
            .services
            .tool_batch_executor()
            .observe_binding(&ticket)
            .expect("physical binding");
        drop(_pending);
        let backend = AgentToolBatchBackend {
            services: Arc::downgrade(&fixture.services),
            bound: decode_bound_agent_tool(&ticket.payload_ref).expect("bound decode"),
            stop: Default::default(),
        };
        let error = backend
            .execute(&ticket)
            .await
            .expect_err("a member Read action without its admission fact is not dispatched");
        match error {
            NodeExecutorError::Uncertain { reason, .. } => {
                assert_eq!(reason, "original governed effect owner missing");
            }
            other => panic!("unexpected executor error: {other:?}"),
        }
        assert!(
            receipt_pair(&fixture.services, &request).is_empty(),
            "no durable receipt may exist for an unadmitted member Read action"
        );
        assert!(fixture
            .services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }
}
