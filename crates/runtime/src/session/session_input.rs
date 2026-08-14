use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use harness_contract::turn::{
    InputRelationProposal, InputRoutingDecision, InputRoutingReason, SessionInputCursor,
    SessionInputEnvelope, SessionInputId, SessionInputProjection, SessionInputReceipt,
    SessionInputStatus, TurnId, TurnInboxItem, TurnInboxSnapshot, TurnInputCheckpoint,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::input_classifier::{classify_session_input, propose_input_relation, RuntimeInputState};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInputRecord {
    pub envelope: SessionInputEnvelope,
    pub status: SessionInputStatus,
    pub decision: InputRoutingDecision,
    pub reason: InputRoutingReason,
    pub relation_proposal: Option<InputRelationProposal>,
    pub active_turn_id: Option<TurnId>,
    pub evidence_refs: Vec<String>,
    pub checkpoint: Option<TurnInputCheckpoint>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub cursor: Option<SessionInputCursor>,
    /// The user request that directly owns a durable ingress graph. It is
    /// already supplied to `submit_ingress_turn`, so checkpoint consumers must
    /// not inject its content into the provider prompt a second time.
    #[serde(default)]
    primary_ingress: bool,
}

impl SessionInputRecord {
    #[must_use]
    pub fn to_receipt(&self) -> SessionInputReceipt {
        SessionInputReceipt {
            input_id: self.envelope.input_id.clone(),
            session_id: self.envelope.session_id.clone(),
            status: self.status,
            decision: self.decision,
            relation_proposal: self.relation_proposal.clone(),
            reason: Some(self.reason.clone()),
            active_turn_id: self.active_turn_id.clone(),
            evidence_refs: self.evidence_refs.clone(),
            cursor: self.cursor,
            created_at: self.envelope.created_at,
        }
    }

    #[must_use]
    pub fn to_inbox_item(&self) -> TurnInboxItem {
        TurnInboxItem {
            input_id: self.envelope.input_id.clone(),
            session_id: self.envelope.session_id.clone(),
            status: self.status,
            decision: self.decision,
            relation_proposal: self.relation_proposal.clone(),
            content_preview: self.envelope.content_preview.clone(),
            checkpoint: self.checkpoint,
            created_at: self.envelope.created_at,
            consumed_at: self.consumed_at,
            cursor: self.cursor,
            failure_class: None,
            last_error: None,
            application_receipt: None,
        }
    }
}

#[derive(Debug)]
struct SessionInputStateInner {
    session_id: String,
    active_turn_id: Option<TurnId>,
    admitted_total: usize,
    durable_consumed_total: usize,
    admitted_cursor: Option<SessionInputCursor>,
    consumed_cursor: Option<SessionInputCursor>,
    consumed_turn_id: Option<TurnId>,
    last_decision: Option<InputRoutingDecision>,
    records: Vec<SessionInputRecord>,
    idempotency: HashMap<String, SessionInputId>,
}

#[derive(Debug, Clone)]
pub struct SessionInputStream {
    inner: Arc<Mutex<SessionInputStateInner>>,
    input_notify: Arc<Notify>,
}

#[derive(Debug)]
pub struct ActiveTurnLease {
    stream: SessionInputStream,
    turn_id: TurnId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionInputMutationError {
    NotFound,
    AlreadyConsumed,
    InvalidPrimaryIngress,
}

impl std::fmt::Display for SessionInputMutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("session input not found"),
            Self::AlreadyConsumed => f.write_str("session input has already been consumed"),
            Self::InvalidPrimaryIngress => {
                f.write_str("session input is not the expected primary ingress")
            }
        }
    }
}

impl std::error::Error for SessionInputMutationError {}

impl Drop for ActiveTurnLease {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.stream.inner.lock() {
            if inner.active_turn_id.as_ref() == Some(&self.turn_id) {
                inner.active_turn_id = None;
            }
        }
    }
}

impl SessionInputStream {
    #[must_use]
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SessionInputStateInner {
                session_id: session_id.into(),
                active_turn_id: None,
                admitted_total: 0,
                durable_consumed_total: 0,
                admitted_cursor: None,
                consumed_cursor: None,
                consumed_turn_id: None,
                last_decision: None,
                records: Vec::new(),
                idempotency: HashMap::new(),
            })),
            input_notify: Arc::new(Notify::new()),
        }
    }

    /// Process-local wake-up signal for the active Turn control lane. Durable
    /// input remains in `records`; this signal never becomes a second queue.
    #[must_use]
    pub fn input_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.input_notify)
    }

    pub fn set_active_turn(&self, active_turn_id: Option<TurnId>) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.active_turn_id = active_turn_id;
        }
    }

    #[must_use]
    pub fn begin_turn(&self, turn_id: TurnId) -> ActiveTurnLease {
        self.set_active_turn(Some(turn_id.clone()));
        ActiveTurnLease {
            stream: self.clone(),
            turn_id,
        }
    }

    #[must_use]
    pub fn active_turn_id(&self) -> Option<TurnId> {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.active_turn_id.clone())
    }

    #[must_use]
    pub fn runtime_state(&self) -> RuntimeInputState {
        RuntimeInputState {
            active_turn_id: self.active_turn_id(),
            waiting_for_approval: false,
            waiting_for_clarification: false,
        }
    }

    pub fn admit(
        &self,
        envelope: SessionInputEnvelope,
        state: RuntimeInputState,
    ) -> SessionInputReceipt {
        let now = Utc::now();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(existing) = inner.idempotency.get(&envelope.idempotency_key) {
            return SessionInputReceipt {
                input_id: existing.clone(),
                session_id: envelope.session_id,
                status: SessionInputStatus::RejectedDuplicate,
                decision: InputRoutingDecision::RejectDuplicate,
                relation_proposal: None,
                reason: Some(InputRoutingReason::new(
                    "duplicate_idempotency_key",
                    "input with the same idempotency key was already accepted",
                    10_000,
                )),
                active_turn_id: inner.active_turn_id.clone(),
                evidence_refs: vec![format!("session-input:duplicate:{}", existing.as_str())],
                cursor: None,
                created_at: now,
            };
        }

        let (decision, reason) = classify_session_input(&envelope, &state);
        let relation_proposal = propose_input_relation(&envelope);
        let status = status_for_decision(decision);
        let active_turn_id = match decision {
            InputRoutingDecision::SupplementCurrentTurn
            | InputRoutingDecision::InterruptAndReplan
            | InputRoutingDecision::ControlOrApproval => state
                .active_turn_id
                .clone()
                .or_else(|| inner.active_turn_id.clone()),
            _ => None,
        };
        let evidence_refs = vec![format!("session-input:{}", envelope.input_id.as_str())];
        let receipt = SessionInputReceipt {
            input_id: envelope.input_id.clone(),
            session_id: envelope.session_id.clone(),
            status,
            decision,
            relation_proposal: relation_proposal.clone(),
            reason: Some(reason.clone()),
            active_turn_id: active_turn_id.clone(),
            evidence_refs: evidence_refs.clone(),
            cursor: None,
            created_at: now,
        };
        inner
            .idempotency
            .insert(envelope.idempotency_key.clone(), envelope.input_id.clone());
        inner.admitted_total = inner.admitted_total.saturating_add(1);
        inner.last_decision = Some(decision);
        inner.records.push(SessionInputRecord {
            envelope,
            status,
            decision,
            reason,
            relation_proposal,
            active_turn_id,
            evidence_refs,
            checkpoint: None,
            consumed_at: None,
            cursor: None,
            primary_ingress: false,
        });
        drop(inner);
        self.input_notify.notify_waiters();
        receipt
    }

    /// Materialize a durable Session admission into the process-local turn
    /// view. The supplied receipt is authoritative; this method never
    /// reclassifies input or creates lifecycle state of its own.
    pub fn project_durable(
        &self,
        envelope: SessionInputEnvelope,
        receipt: SessionInputReceipt,
    ) -> SessionInputReceipt {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = inner
            .records
            .iter()
            .position(|record| record.envelope.input_id == envelope.input_id)
        {
            let existing_cursor = inner.records[index].cursor;
            if !durable_cursor_matches(existing_cursor, receipt.cursor) {
                return receipt;
            }
            {
                let record = &mut inner.records[index];
                // A durable handoff may be replayed while the active Runtime
                // is waiting for the terminal transcript commit. It must not
                // reopen an input already consumed at a checkpoint.
                let preserve_checkpoint_consumed = record.status == SessionInputStatus::Consumed
                    && record.consumed_at.is_some()
                    && receipt.status == SessionInputStatus::AttachedToTurn;
                if !preserve_checkpoint_consumed {
                    record.status = receipt.status;
                }
                record.decision = receipt.decision;
                record.reason = receipt.reason.clone().unwrap_or_else(|| {
                    InputRoutingReason::new(
                        "durable_projection",
                        "classification restored from durable Session input",
                        10_000,
                    )
                });
                record.relation_proposal = receipt.relation_proposal.clone();
                record.active_turn_id = receipt.active_turn_id.clone();
                record.evidence_refs = receipt.evidence_refs.clone();
                if receipt.cursor.is_some() {
                    record.cursor = receipt.cursor;
                }
            }
            inner.admitted_cursor = max_cursor(inner.admitted_cursor, receipt.cursor);
            inner.last_decision = Some(receipt.decision);
            if is_unambiguous_durable_terminal_status(receipt.status) {
                release_durable_terminal_record(&mut inner, &receipt.input_id, receipt.cursor);
            }
            drop(inner);
            self.input_notify.notify_waiters();
            return receipt;
        }

        if receipt
            .cursor
            .zip(inner.admitted_cursor)
            .is_some_and(|(incoming, admitted)| incoming <= admitted)
        {
            return receipt;
        }

        inner.admitted_total = inner.admitted_total.saturating_add(1);
        inner.admitted_cursor = max_cursor(inner.admitted_cursor, receipt.cursor);
        inner.last_decision = Some(receipt.decision);
        if is_unambiguous_durable_terminal_status(receipt.status) {
            if receipt.status == SessionInputStatus::Consumed {
                inner.durable_consumed_total = inner.durable_consumed_total.saturating_add(1);
                inner.consumed_cursor = max_cursor(inner.consumed_cursor, receipt.cursor);
                inner.consumed_turn_id.clone_from(&receipt.active_turn_id);
            }
            drop(inner);
            self.input_notify.notify_waiters();
            return receipt;
        }

        inner
            .idempotency
            .insert(envelope.idempotency_key.clone(), envelope.input_id.clone());
        inner.records.push(SessionInputRecord {
            envelope,
            status: receipt.status,
            decision: receipt.decision,
            reason: receipt.reason.clone().unwrap_or_else(|| {
                InputRoutingReason::new(
                    "durable_projection",
                    "classification restored from durable Session input",
                    10_000,
                )
            }),
            relation_proposal: receipt.relation_proposal.clone(),
            active_turn_id: receipt.active_turn_id.clone(),
            evidence_refs: receipt.evidence_refs.clone(),
            checkpoint: None,
            consumed_at: None,
            cursor: receipt.cursor,
            primary_ingress: false,
        });
        drop(inner);
        self.input_notify.notify_waiters();
        receipt
    }

    /// Apply a later durable transition to an already materialized execution
    /// view. Missing records are expected after restart and remain harmless:
    /// callers rebuild them from the durable envelope before a turn consumes
    /// their content.
    pub fn project_durable_receipt(&self, receipt: &SessionInputReceipt) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(index) = inner
            .records
            .iter()
            .position(|record| record.envelope.input_id == receipt.input_id)
        else {
            return false;
        };
        if !durable_cursor_matches(inner.records[index].cursor, receipt.cursor) {
            return false;
        }
        let terminal = is_unambiguous_durable_terminal_status(receipt.status);
        let input_id = receipt.input_id.clone();
        {
            let record = &mut inner.records[index];
            // `AttachedToTurn` is the durable handoff state. Once the active
            // Runtime has consumed that same input at a checkpoint, replaying
            // the handoff receipt must not reopen it in the hot inbox. The
            // durable terminal transcript later acknowledges every consumed
            // cursor covered by its atomic commit.
            let preserve_checkpoint_consumed = record.status == SessionInputStatus::Consumed
                && record.consumed_at.is_some()
                && receipt.status == SessionInputStatus::AttachedToTurn;
            if !preserve_checkpoint_consumed {
                record.status = receipt.status;
            }
            record.decision = receipt.decision;
            if let Some(reason) = &receipt.reason {
                record.reason = reason.clone();
            }
            record.relation_proposal = receipt.relation_proposal.clone();
            record.active_turn_id = receipt.active_turn_id.clone();
            record.evidence_refs = receipt.evidence_refs.clone();
            if receipt.cursor.is_some() {
                record.cursor = receipt.cursor;
            }
        }
        inner.admitted_cursor = max_cursor(inner.admitted_cursor, receipt.cursor);
        inner.last_decision = Some(receipt.decision);
        if terminal {
            release_durable_terminal_record(&mut inner, &input_id, receipt.cursor);
        }
        true
    }

    /// Releases one terminal input only after its durable owner has committed
    /// the matching terminal receipt. The optional cursor is a generation and
    /// ordering fence; a stale acknowledgement cannot evict a newer record.
    pub fn acknowledge_durable_terminal(
        &self,
        input_id: &SessionInputId,
        cursor: Option<SessionInputCursor>,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        release_durable_terminal_record(&mut inner, input_id, cursor)
    }

    /// Releases checkpoint-consumed inputs covered by one atomic durable turn
    /// terminal commit. Session storage guarantees that every matching input
    /// in `(primary_sequence, consumed_through]` is terminal before exposing
    /// this watermark, so no per-input historical set is needed in Runtime.
    pub fn acknowledge_durable_consumed_through(
        &self,
        turn_id: &TurnId,
        consumed_through: SessionInputCursor,
    ) -> usize {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let records = std::mem::take(&mut inner.records);
        let mut released = 0;
        for record in records {
            let covered = record.status == SessionInputStatus::Consumed
                && record.active_turn_id.as_ref() == Some(turn_id)
                && record.cursor.is_some_and(|cursor| {
                    cursor.generation == consumed_through.generation
                        && cursor.sequence <= consumed_through.sequence
                });
            if covered {
                inner.idempotency.remove(&record.envelope.idempotency_key);
                inner.durable_consumed_total = inner.durable_consumed_total.saturating_add(1);
                inner.consumed_cursor = max_cursor(inner.consumed_cursor, record.cursor);
                inner.consumed_turn_id = record.active_turn_id;
                released += 1;
            } else {
                inner.records.push(record);
            }
        }
        released
    }

    /// Bind the exactly accepted primary ingress to the Runtime turn that is
    /// now taking ownership of it. Missing records are normal after a process
    /// restart because the UI projection is intentionally in-memory; they do
    /// not block durable outbox recovery.
    pub fn bind_primary_ingress(
        &self,
        idempotency_key: &str,
        turn_id: TurnId,
        execution_id: &str,
    ) -> Result<Option<SessionInputRecord>, SessionInputMutationError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(input_id) = inner.idempotency.get(idempotency_key).cloned() else {
            return Ok(None);
        };
        let updated = {
            let record = inner
                .records
                .iter_mut()
                .find(|record| record.envelope.input_id == input_id)
                .ok_or(SessionInputMutationError::NotFound)?;
            if record.primary_ingress
                && record.active_turn_id.as_ref() == Some(&turn_id)
                && record
                    .evidence_refs
                    .iter()
                    .any(|reference| reference == &format!("execution_graph:{execution_id}"))
            {
                return Ok(Some(record.clone()));
            }
            if record.consumed_at.is_some()
                || record.decision != InputRoutingDecision::StartNewTurn
                || record.status != SessionInputStatus::QueuedNext
            {
                return Err(SessionInputMutationError::InvalidPrimaryIngress);
            }
            record.status = SessionInputStatus::AttachedToTurn;
            record.active_turn_id = Some(turn_id.clone());
            record.reason = InputRoutingReason::new(
                "primary_ingress_bound",
                "durable session ingress is owned by the canonical Runtime execution",
                10_000,
            );
            record.primary_ingress = true;
            record
                .evidence_refs
                .push(format!("execution_graph:{execution_id}"));
            record
                .evidence_refs
                .push("session-input:primary-ingress".to_string());
            record.clone()
        };
        inner.active_turn_id = Some(turn_id);
        Ok(Some(updated))
    }

    pub fn settle_primary_ingress(
        &self,
        idempotency_key: &str,
        turn_id: &TurnId,
        execution_id: &str,
        terminal_id: &str,
    ) -> Result<Option<SessionInputRecord>, SessionInputMutationError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(input_id) = inner.idempotency.get(idempotency_key).cloned() else {
            return Ok(None);
        };
        let updated = {
            let record = inner
                .records
                .iter_mut()
                .find(|record| record.envelope.input_id == input_id)
                .ok_or(SessionInputMutationError::NotFound)?;
            if !record.primary_ingress || record.active_turn_id.as_ref() != Some(turn_id) {
                return Err(SessionInputMutationError::InvalidPrimaryIngress);
            }
            if record.consumed_at.is_none() {
                record.status = SessionInputStatus::Consumed;
                record.checkpoint = Some(TurnInputCheckpoint::IngressDispatched);
                record.consumed_at = Some(Utc::now());
                record.reason = InputRoutingReason::new(
                    "primary_ingress_settled",
                    "canonical Runtime execution committed its durable terminal",
                    10_000,
                );
                record
                    .evidence_refs
                    .push(format!("execution_graph:{execution_id}"));
                record.evidence_refs.push(format!("terminal:{terminal_id}"));
                record
                    .evidence_refs
                    .push("checkpoint:ingress_dispatched".to_string());
            }
            record.clone()
        };
        if inner.active_turn_id.as_ref() == Some(turn_id) {
            inner.active_turn_id = None;
        }
        Ok(Some(updated))
    }

    pub fn fail_primary_ingress(
        &self,
        idempotency_key: &str,
        turn_id: &TurnId,
        error: impl Into<String>,
    ) -> Result<Option<SessionInputRecord>, SessionInputMutationError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(input_id) = inner.idempotency.get(idempotency_key).cloned() else {
            return Ok(None);
        };
        let updated = {
            let record = inner
                .records
                .iter_mut()
                .find(|record| record.envelope.input_id == input_id)
                .ok_or(SessionInputMutationError::NotFound)?;
            if !record.primary_ingress || record.active_turn_id.as_ref() != Some(turn_id) {
                return Err(SessionInputMutationError::InvalidPrimaryIngress);
            }
            if record.consumed_at.is_none() {
                record.status = SessionInputStatus::Failed;
                record.checkpoint = None;
                record.reason =
                    InputRoutingReason::new("primary_ingress_failed", error.into(), 10_000);
                record
                    .evidence_refs
                    .push("session-input:primary-ingress-failed".to_string());
            }
            record.clone()
        };
        if inner.active_turn_id.as_ref() == Some(turn_id) {
            inner.active_turn_id = None;
        }
        Ok(Some(updated))
    }

    pub fn cancel_input(
        &self,
        input_id: &SessionInputId,
        reason: impl Into<String>,
    ) -> Result<SessionInputRecord, SessionInputMutationError> {
        self.mutate_input(input_id, |record, _active_turn| {
            if record.consumed_at.is_some() {
                return Err(SessionInputMutationError::AlreadyConsumed);
            }
            record.status = SessionInputStatus::Cancelled;
            record.reason = InputRoutingReason::new("input_cancelled", reason.into(), 10_000);
            record
                .evidence_refs
                .push("session-input:cancelled".to_string());
            record.checkpoint = None;
            Ok(())
        })
    }

    pub fn reclassify_input(
        &self,
        input_id: &SessionInputId,
        decision: InputRoutingDecision,
        reason: impl Into<String>,
    ) -> Result<SessionInputRecord, SessionInputMutationError> {
        self.mutate_input(input_id, |record, active_turn| {
            if record.consumed_at.is_some() {
                return Err(SessionInputMutationError::AlreadyConsumed);
            }
            record.decision = decision;
            record.status = status_for_decision(decision);
            record.reason = InputRoutingReason::new("manual_reclassify", reason.into(), 10_000);
            record.active_turn_id = match decision {
                InputRoutingDecision::SupplementCurrentTurn
                | InputRoutingDecision::InterruptAndReplan
                | InputRoutingDecision::ControlOrApproval => active_turn.clone(),
                _ => None,
            };
            record.checkpoint = None;
            record
                .evidence_refs
                .push(format!("session-input:reclassified:{}", decision.as_str()));
            Ok(())
        })
    }

    fn mutate_input(
        &self,
        input_id: &SessionInputId,
        mutate: impl FnOnce(
            &mut SessionInputRecord,
            &Option<TurnId>,
        ) -> Result<(), SessionInputMutationError>,
    ) -> Result<SessionInputRecord, SessionInputMutationError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let active_turn = inner.active_turn_id.clone();
        let record = inner
            .records
            .iter_mut()
            .find(|record| &record.envelope.input_id == input_id)
            .ok_or(SessionInputMutationError::NotFound)?;
        mutate(record, &active_turn)?;
        let updated = record.clone();
        inner.last_decision = Some(updated.decision);
        Ok(updated)
    }

    pub fn consume_for_checkpoint(
        &self,
        turn_id: &TurnId,
        checkpoint: TurnInputCheckpoint,
        limit: usize,
    ) -> Vec<SessionInputRecord> {
        let mut consumed = Vec::new();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for record in &mut inner.records {
            if consumed.len() >= limit {
                break;
            }
            if !is_checkpoint_consumable(record, turn_id) {
                continue;
            }
            record.status = SessionInputStatus::Consumed;
            record.checkpoint = Some(checkpoint);
            record.consumed_at = Some(Utc::now());
            record
                .evidence_refs
                .push(format!("checkpoint:{}", checkpoint.as_str()));
            consumed.push(record.clone());
        }
        consumed
    }

    pub fn promote_queued_next(&self, turn_id: &TurnId, limit: usize) -> Vec<SessionInputRecord> {
        let mut promoted = Vec::new();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for record in &mut inner.records {
            if promoted.len() >= limit {
                break;
            }
            if record.consumed_at.is_some()
                || record.decision != InputRoutingDecision::EnqueueNextStep
            {
                continue;
            }
            record.status = SessionInputStatus::AttachedToTurn;
            record.active_turn_id = Some(turn_id.clone());
            promoted.push(record.clone());
        }
        promoted
    }

    pub fn drain_queued_next_for_dispatch(&self, limit: usize) -> Vec<SessionInputRecord> {
        let mut drained = Vec::new();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for record in &mut inner.records {
            if drained.len() >= limit {
                break;
            }
            if record.consumed_at.is_some()
                || record.decision != InputRoutingDecision::EnqueueNextStep
            {
                continue;
            }
            record.status = SessionInputStatus::Consumed;
            record.checkpoint = Some(TurnInputCheckpoint::BeforeFinalAnswer);
            record.consumed_at = Some(Utc::now());
            record
                .evidence_refs
                .push("dispatch:queued-next".to_string());
            drained.push(record.clone());
        }
        drained
    }

    #[must_use]
    pub fn projection(&self) -> SessionInputProjection {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let total = inner.admitted_total;
        let pending_count = inner
            .records
            .iter()
            .filter(|record| is_pending_status(record.status))
            .count();
        let queued_next_count = inner
            .records
            .iter()
            .filter(|record| {
                record.decision == InputRoutingDecision::EnqueueNextStep
                    && record.consumed_at.is_none()
            })
            .count();
        let consumed_count = inner.durable_consumed_total.saturating_add(
            inner
                .records
                .iter()
                .filter(|record| record.status == SessionInputStatus::Consumed)
                .count(),
        );
        SessionInputProjection {
            session_id: inner.session_id.clone(),
            active_turn_id: inner.active_turn_id.clone(),
            total,
            pending_count,
            queued_next_count,
            consumed_count,
            admitted_cursor: max_cursor(
                inner.admitted_cursor,
                highest_cursor(inner.records.iter()),
            ),
            consumed_cursor: max_cursor(
                inner.consumed_cursor,
                highest_cursor(
                    inner
                        .records
                        .iter()
                        .filter(|record| record.consumed_at.is_some()),
                ),
            ),
            last_decision: inner.last_decision,
            inputs: inner
                .records
                .iter()
                .rev()
                .take(50)
                .map(SessionInputRecord::to_inbox_item)
                .collect(),
            updated_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn inbox_snapshot(&self, turn_id: Option<TurnId>) -> TurnInboxSnapshot {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let selected_turn_id = turn_id.or_else(|| inner.active_turn_id.clone());
        let items: Vec<TurnInboxItem> = inner
            .records
            .iter()
            .filter(|record| {
                selected_turn_id
                    .as_ref()
                    .is_none_or(|turn_id| record.active_turn_id.as_ref() == Some(turn_id))
            })
            .map(SessionInputRecord::to_inbox_item)
            .collect();
        let pending_count = items
            .iter()
            .filter(|item| is_pending_status(item.status))
            .count();
        let consumed_count = items
            .iter()
            .filter(|item| item.status == SessionInputStatus::Consumed)
            .count();
        TurnInboxSnapshot {
            session_id: inner.session_id.clone(),
            turn_id: selected_turn_id,
            pending_count,
            consumed_count,
            admitted_cursor: max_cursor(
                inner.admitted_cursor,
                highest_cursor(inner.records.iter()),
            ),
            consumed_cursor: max_cursor(
                inner.consumed_cursor,
                highest_cursor(
                    inner
                        .records
                        .iter()
                        .filter(|record| record.consumed_at.is_some()),
                ),
            ),
            items,
            updated_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn record_snapshot(&self, input_id: &SessionInputId) -> Option<SessionInputRecord> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .records
            .iter()
            .find(|record| &record.envelope.input_id == input_id)
            .cloned()
    }

    #[must_use]
    pub fn highest_consumed_cursor(&self, turn_id: &TurnId) -> Option<SessionInputCursor> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        max_cursor(
            (inner.consumed_turn_id.as_ref() == Some(turn_id))
                .then_some(inner.consumed_cursor)
                .flatten(),
            highest_cursor(inner.records.iter().filter(|record| {
                record.consumed_at.is_some()
                    && record
                        .active_turn_id
                        .as_ref()
                        .is_some_and(|active| active == turn_id)
            })),
        )
    }
}

fn release_durable_terminal_record(
    inner: &mut SessionInputStateInner,
    input_id: &SessionInputId,
    cursor: Option<SessionInputCursor>,
) -> bool {
    let Some(index) = inner
        .records
        .iter()
        .position(|record| &record.envelope.input_id == input_id)
    else {
        return false;
    };
    let record = &inner.records[index];
    if !is_terminal_status(record.status) || !durable_cursor_matches(record.cursor, cursor) {
        return false;
    }
    let record = inner.records.remove(index);
    inner.idempotency.remove(&record.envelope.idempotency_key);
    if record.status == SessionInputStatus::Consumed {
        inner.durable_consumed_total = inner.durable_consumed_total.saturating_add(1);
        inner.consumed_cursor = max_cursor(inner.consumed_cursor, record.cursor);
        inner.consumed_turn_id = record.active_turn_id;
    }
    true
}

/// A durable lifecycle update is authoritative only for the exact cursor that
/// materialized the hot record. Cursor-less legacy data can update another
/// cursor-less record, but it cannot evict a record with a known generation.
fn durable_cursor_matches(
    existing: Option<SessionInputCursor>,
    incoming: Option<SessionInputCursor>,
) -> bool {
    existing == incoming || existing.is_none()
}

fn max_cursor(
    current: Option<SessionInputCursor>,
    candidate: Option<SessionInputCursor>,
) -> Option<SessionInputCursor> {
    current.max(candidate)
}

fn highest_cursor<'a>(
    records: impl IntoIterator<Item = &'a SessionInputRecord>,
) -> Option<SessionInputCursor> {
    records.into_iter().filter_map(|record| record.cursor).max()
}

fn status_for_decision(decision: InputRoutingDecision) -> SessionInputStatus {
    match decision {
        InputRoutingDecision::StartNewTurn => SessionInputStatus::QueuedNext,
        InputRoutingDecision::SupplementCurrentTurn => SessionInputStatus::AttachedToTurn,
        InputRoutingDecision::InterruptAndReplan => SessionInputStatus::InterruptRequested,
        InputRoutingDecision::EnqueueNextStep => SessionInputStatus::QueuedNext,
        InputRoutingDecision::SpawnSubtask => SessionInputStatus::DispatchedSubtask,
        InputRoutingDecision::RouteCrossSession => SessionInputStatus::DispatchedSession,
        InputRoutingDecision::CreateNewSession => SessionInputStatus::NewSessionCreated,
        InputRoutingDecision::ControlOrApproval => SessionInputStatus::ControlResolved,
        InputRoutingDecision::RejectDuplicate => SessionInputStatus::RejectedDuplicate,
        InputRoutingDecision::RejectPolicy => SessionInputStatus::RejectedPolicy,
    }
}

trait InputRoutingDecisionExt {
    fn as_str(self) -> &'static str;
}

impl InputRoutingDecisionExt for InputRoutingDecision {
    fn as_str(self) -> &'static str {
        match self {
            InputRoutingDecision::StartNewTurn => "start_new_turn",
            InputRoutingDecision::SupplementCurrentTurn => "supplement_current_turn",
            InputRoutingDecision::InterruptAndReplan => "interrupt_and_replan",
            InputRoutingDecision::EnqueueNextStep => "enqueue_next_step",
            InputRoutingDecision::SpawnSubtask => "spawn_subtask",
            InputRoutingDecision::RouteCrossSession => "route_cross_session",
            InputRoutingDecision::CreateNewSession => "create_new_session",
            InputRoutingDecision::ControlOrApproval => "control_or_approval",
            InputRoutingDecision::RejectDuplicate => "reject_duplicate",
            InputRoutingDecision::RejectPolicy => "reject_policy",
        }
    }
}

fn is_pending_status(status: SessionInputStatus) -> bool {
    matches!(
        status,
        SessionInputStatus::Received
            | SessionInputStatus::Persisted
            | SessionInputStatus::Classified
            | SessionInputStatus::AttachedToTurn
            | SessionInputStatus::QueuedNext
            | SessionInputStatus::InterruptRequested
            | SessionInputStatus::ControlResolved
    )
}

fn is_terminal_status(status: SessionInputStatus) -> bool {
    matches!(
        status,
        SessionInputStatus::DispatchedSubtask
            | SessionInputStatus::DispatchedSession
            | SessionInputStatus::NewSessionCreated
            | SessionInputStatus::ControlResolved
            | SessionInputStatus::Consumed
            | SessionInputStatus::Cancelled
            | SessionInputStatus::Failed
            | SessionInputStatus::RejectedDuplicate
            | SessionInputStatus::RejectedPolicy
            | SessionInputStatus::Superseded
    )
}

fn is_unambiguous_durable_terminal_status(status: SessionInputStatus) -> bool {
    matches!(
        status,
        SessionInputStatus::Consumed
            | SessionInputStatus::Cancelled
            | SessionInputStatus::Failed
            | SessionInputStatus::RejectedDuplicate
            | SessionInputStatus::RejectedPolicy
            | SessionInputStatus::Superseded
    )
}

fn is_checkpoint_consumable(record: &SessionInputRecord, turn_id: &TurnId) -> bool {
    if record.consumed_at.is_some() {
        return false;
    }
    if record.primary_ingress {
        return false;
    }
    if !is_pending_status(record.status) {
        return false;
    }
    if record.status == SessionInputStatus::AttachedToTurn
        && record.active_turn_id.as_ref() == Some(turn_id)
    {
        return true;
    }
    matches!(
        record.decision,
        InputRoutingDecision::SupplementCurrentTurn
            | InputRoutingDecision::InterruptAndReplan
            | InputRoutingDecision::ControlOrApproval
    ) && record.active_turn_id.as_ref() == Some(turn_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};

    #[test]
    fn stream_rejects_duplicate_idempotency_key() {
        let stream = SessionInputStream::new("s1");
        let state = RuntimeInputState::default();
        let envelope = SessionInputEnvelope::text("s1", InputSourceKind::Api, "hello")
            .with_idempotency_key("idem-1");

        let first = stream.admit(envelope.clone(), state.clone());
        let second = stream.admit(envelope, state);

        assert_eq!(first.decision, InputRoutingDecision::StartNewTurn);
        assert_eq!(second.decision, InputRoutingDecision::RejectDuplicate);
        assert_eq!(stream.projection().total, 1);
    }

    #[test]
    fn primary_ingress_binds_and_settles_without_reinjecting_the_prompt() {
        let stream = SessionInputStream::new("s1");
        let envelope = SessionInputEnvelope::text("s1", InputSourceKind::Webui, "already primary")
            .with_idempotency_key("primary-1");
        let receipt = stream.admit(envelope, RuntimeInputState::default());
        assert_eq!(receipt.status, SessionInputStatus::QueuedNext);
        let turn_id = TurnId::from_string("turn-primary");

        let bound = stream
            .bind_primary_ingress("primary-1", turn_id.clone(), "execution-primary")
            .expect("bind primary ingress")
            .expect("in-process record");
        assert_eq!(bound.status, SessionInputStatus::AttachedToTurn);
        assert!(stream
            .consume_for_checkpoint(&turn_id, TurnInputCheckpoint::BeforeProviderRequest, 8)
            .is_empty());
        assert_eq!(stream.projection().pending_count, 1);

        let settled = stream
            .settle_primary_ingress(
                "primary-1",
                &turn_id,
                "execution-primary",
                "turn-terminal:primary-1",
            )
            .expect("settle primary ingress")
            .expect("in-process record");
        assert_eq!(settled.status, SessionInputStatus::Consumed);
        assert_eq!(
            settled.checkpoint,
            Some(TurnInputCheckpoint::IngressDispatched)
        );
        let projection = stream.projection();
        assert_eq!(projection.pending_count, 0);
        assert_eq!(projection.consumed_count, 1);
        assert_eq!(projection.active_turn_id, None);
    }

    #[test]
    fn primary_ingress_never_mutates_a_different_idempotency_key() {
        let stream = SessionInputStream::new("s1");
        let receipt = stream.admit(
            SessionInputEnvelope::text("s1", InputSourceKind::Api, "one")
                .with_idempotency_key("primary-one"),
            RuntimeInputState::default(),
        );
        let error = stream
            .bind_primary_ingress("missing", TurnId::from_string("turn-x"), "execution-x")
            .expect("missing in-memory record is restart-safe");
        assert!(error.is_none());
        assert_eq!(
            stream
                .record_snapshot(&receipt.input_id)
                .expect("record")
                .status,
            SessionInputStatus::QueuedNext
        );
    }

    #[test]
    fn active_supplement_is_consumed_at_checkpoint() {
        let stream = SessionInputStream::new("s1");
        let turn_id = TurnId::from_string("turn-1");
        stream.set_active_turn(Some(turn_id.clone()));
        let state = RuntimeInputState::active(turn_id.clone());
        let envelope = SessionInputEnvelope::text("s1", InputSourceKind::Webui, "more context");

        let receipt = stream.admit(envelope, state);
        let consumed =
            stream.consume_for_checkpoint(&turn_id, TurnInputCheckpoint::BeforeProviderRequest, 4);

        assert_eq!(
            receipt.decision,
            InputRoutingDecision::SupplementCurrentTurn
        );
        assert_eq!(consumed.len(), 1);
        assert_eq!(stream.projection().consumed_count, 1);
    }

    #[test]
    fn durable_cursor_advances_only_when_checkpoint_consumes_input() {
        let stream = SessionInputStream::new("s1");
        let turn_id = TurnId::from_string("turn-1");
        stream.set_active_turn(Some(turn_id.clone()));
        let envelope =
            SessionInputEnvelope::text("s1", InputSourceKind::Webui, "durable supplement");
        let mut receipt =
            stream.admit(envelope.clone(), RuntimeInputState::active(turn_id.clone()));
        receipt.cursor = Some(SessionInputCursor::new(7, 42));
        stream.project_durable(envelope, receipt);

        let before = stream.projection();
        assert_eq!(before.admitted_cursor, Some(SessionInputCursor::new(7, 42)));
        assert_eq!(before.consumed_cursor, None);

        let consumed =
            stream.consume_for_checkpoint(&turn_id, TurnInputCheckpoint::AfterProviderResponse, 4);
        assert_eq!(consumed.len(), 1);
        assert_eq!(
            stream.highest_consumed_cursor(&turn_id),
            Some(SessionInputCursor::new(7, 42))
        );
        assert_eq!(
            stream.projection().consumed_cursor,
            Some(SessionInputCursor::new(7, 42))
        );
    }

    #[test]
    fn queued_next_can_be_drained_for_dispatch_once() {
        let stream = SessionInputStream::new("s1");
        let state = RuntimeInputState::active(TurnId::from_string("turn-1"));
        let envelope =
            SessionInputEnvelope::text("s1", InputSourceKind::Webui, "next, write tests");

        let receipt = stream.admit(envelope, state);
        let reclassified = stream
            .reclassify_input(
                &receipt.input_id,
                InputRoutingDecision::EnqueueNextStep,
                "runtime strategy accepted the new-task proposal",
            )
            .expect("strategy reclassification");
        let drained = stream.drain_queued_next_for_dispatch(4);
        let drained_again = stream.drain_queued_next_for_dispatch(4);

        assert_eq!(
            receipt.decision,
            InputRoutingDecision::SupplementCurrentTurn
        );
        assert_eq!(
            receipt
                .relation_proposal
                .as_ref()
                .expect("new task proposal")
                .candidate,
            harness_contract::turn::InputRelationKind::NewTask
        );
        assert_eq!(reclassified.decision, InputRoutingDecision::EnqueueNextStep);
        assert_eq!(drained.len(), 1);
        assert!(drained_again.is_empty());
        assert_eq!(stream.projection().consumed_count, 1);
    }

    #[test]
    fn pending_input_can_be_cancelled_before_checkpoint() {
        let stream = SessionInputStream::new("s1");
        let turn_id = TurnId::from_string("turn-1");
        let envelope = SessionInputEnvelope::text("s1", InputSourceKind::Webui, "more context");
        let receipt = stream.admit(envelope, RuntimeInputState::active(turn_id.clone()));

        let record = stream
            .cancel_input(&receipt.input_id, "user changed direction")
            .expect("cancel pending input");
        let consumed =
            stream.consume_for_checkpoint(&turn_id, TurnInputCheckpoint::BeforeProviderRequest, 4);

        assert_eq!(record.status, SessionInputStatus::Cancelled);
        assert!(consumed.is_empty());
        assert_eq!(stream.projection().pending_count, 0);
    }

    #[test]
    fn pending_input_can_be_reclassified_to_next_queue() {
        let stream = SessionInputStream::new("s1");
        let turn_id = TurnId::from_string("turn-1");
        let envelope = SessionInputEnvelope::text("s1", InputSourceKind::Webui, "more context");
        let receipt = stream.admit(envelope, RuntimeInputState::active(turn_id));

        let record = stream
            .reclassify_input(
                &receipt.input_id,
                InputRoutingDecision::EnqueueNextStep,
                "user marked as follow-up",
            )
            .expect("reclassify pending input");

        assert_eq!(record.decision, InputRoutingDecision::EnqueueNextStep);
        assert_eq!(record.status, SessionInputStatus::QueuedNext);
        assert_eq!(stream.projection().queued_next_count, 1);
    }

    #[test]
    fn consumed_input_cannot_be_reclassified() {
        let stream = SessionInputStream::new("s1");
        let turn_id = TurnId::from_string("turn-1");
        let envelope = SessionInputEnvelope::text("s1", InputSourceKind::Webui, "more context");
        let receipt = stream.admit(envelope, RuntimeInputState::active(turn_id.clone()));
        let consumed =
            stream.consume_for_checkpoint(&turn_id, TurnInputCheckpoint::BeforeProviderRequest, 4);

        let error = stream
            .reclassify_input(
                &receipt.input_id,
                InputRoutingDecision::EnqueueNextStep,
                "too late",
            )
            .expect_err("consumed input cannot be mutated");

        assert_eq!(consumed.len(), 1);
        assert_eq!(error, SessionInputMutationError::AlreadyConsumed);
    }

    #[test]
    fn durable_terminal_ack_releases_record_and_preserves_watermarks() {
        let stream = SessionInputStream::new("s1");
        let turn_id = TurnId::from_string("turn-1");
        stream.set_active_turn(Some(turn_id.clone()));
        let envelope =
            SessionInputEnvelope::text("s1", InputSourceKind::Webui, "durable checkpoint input")
                .with_idempotency_key("durable-terminal-1");
        let input_id = envelope.input_id.clone();
        let cursor = SessionInputCursor::new(3, 41);
        let mut receipt =
            stream.admit(envelope.clone(), RuntimeInputState::active(turn_id.clone()));
        receipt.cursor = Some(cursor);
        stream.project_durable(envelope, receipt);

        let consumed =
            stream.consume_for_checkpoint(&turn_id, TurnInputCheckpoint::AfterProviderResponse, 1);
        assert_eq!(consumed.len(), 1);
        assert!(stream.record_snapshot(&input_id).is_some());
        assert!(!stream.acknowledge_durable_terminal(&input_id, None));
        assert!(
            !stream.acknowledge_durable_terminal(&input_id, Some(SessionInputCursor::new(3, 40)))
        );
        assert!(stream.acknowledge_durable_terminal(&input_id, Some(cursor)));
        assert!(stream.record_snapshot(&input_id).is_none());

        let projection = stream.projection();
        assert_eq!(projection.total, 1);
        assert_eq!(projection.pending_count, 0);
        assert_eq!(projection.consumed_count, 1);
        assert_eq!(projection.admitted_cursor, Some(cursor));
        assert_eq!(projection.consumed_cursor, Some(cursor));
        assert!(projection.inputs.is_empty());
        assert_eq!(stream.highest_consumed_cursor(&turn_id), Some(cursor));
    }

    #[test]
    fn durable_attached_replay_cannot_reopen_checkpoint_consumed_input() {
        let stream = SessionInputStream::new("s1");
        let turn_id = TurnId::from_string("turn-1");
        let envelope =
            SessionInputEnvelope::text("s1", InputSourceKind::Webui, "checkpoint supplement")
                .with_idempotency_key("checkpoint-supplement-1");
        let mut receipt =
            stream.admit(envelope.clone(), RuntimeInputState::active(turn_id.clone()));
        receipt.cursor = Some(SessionInputCursor::new(2, 8));
        stream.project_durable(envelope.clone(), receipt.clone());
        assert_eq!(
            stream
                .consume_for_checkpoint(&turn_id, TurnInputCheckpoint::AfterToolResult, 1)
                .len(),
            1
        );

        let mut cursorless_terminal = receipt.clone();
        cursorless_terminal.status = SessionInputStatus::Cancelled;
        cursorless_terminal.cursor = None;
        assert!(!stream.project_durable_receipt(&cursorless_terminal));

        receipt.status = SessionInputStatus::AttachedToTurn;
        stream.project_durable(envelope, receipt.clone());
        assert_eq!(
            stream
                .record_snapshot(&receipt.input_id)
                .expect("full durable replay preserves checkpoint state")
                .status,
            SessionInputStatus::Consumed
        );
        assert!(stream.project_durable_receipt(&receipt));
        let retained = stream
            .record_snapshot(&receipt.input_id)
            .expect("checkpoint-consumed record remains active until terminal commit");
        assert_eq!(retained.status, SessionInputStatus::Consumed);
        assert!(retained.consumed_at.is_some());

        assert_eq!(
            stream.acknowledge_durable_consumed_through(&turn_id, SessionInputCursor::new(2, 8)),
            1
        );
        assert!(stream.record_snapshot(&receipt.input_id).is_none());
    }

    #[test]
    fn durable_consumed_watermark_releases_only_covered_generation_and_sequence() {
        let stream = SessionInputStream::new("s1");
        let turn_id = TurnId::from_string("turn-1");
        let mut input_ids = Vec::new();
        for (generation, sequence) in [(4, 10), (4, 11), (4, 12), (5, 1)] {
            let envelope = SessionInputEnvelope::text(
                "s1",
                InputSourceKind::Webui,
                format!("supplement {generation}:{sequence}"),
            )
            .with_idempotency_key(format!("supplement-{generation}-{sequence}"));
            let input_id = envelope.input_id.clone();
            let mut receipt =
                stream.admit(envelope.clone(), RuntimeInputState::active(turn_id.clone()));
            receipt.cursor = Some(SessionInputCursor::new(generation, sequence));
            stream.project_durable(envelope, receipt);
            assert_eq!(
                stream
                    .consume_for_checkpoint(&turn_id, TurnInputCheckpoint::BeforeFinalAnswer, 1)
                    .len(),
                1
            );
            input_ids.push(input_id);
        }

        assert_eq!(
            stream.acknowledge_durable_consumed_through(&turn_id, SessionInputCursor::new(4, 11)),
            2
        );
        assert!(stream.record_snapshot(&input_ids[0]).is_none());
        assert!(stream.record_snapshot(&input_ids[1]).is_none());
        assert!(stream.record_snapshot(&input_ids[2]).is_some());
        assert!(stream.record_snapshot(&input_ids[3]).is_some());
    }

    #[test]
    fn ten_thousand_checkpoint_consumed_inputs_release_in_one_watermark_pass() {
        let stream = SessionInputStream::new("bounded-checkpoint-session");
        let turn_id = TurnId::from_string("turn-bounded-checkpoint");
        for sequence in 1..=10_000 {
            let envelope = SessionInputEnvelope::text(
                "bounded-checkpoint-session",
                InputSourceKind::Api,
                format!("checkpoint input {sequence}"),
            )
            .with_idempotency_key(format!("checkpoint-input-{sequence}"));
            stream.admit(envelope, RuntimeInputState::active(turn_id.clone()));
        }
        {
            let mut inner = stream
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (index, record) in inner.records.iter_mut().enumerate() {
                record.cursor = Some(SessionInputCursor::new(9, index as u64 + 1));
            }
            inner.admitted_cursor = Some(SessionInputCursor::new(9, 10_000));
        }
        assert_eq!(
            stream
                .consume_for_checkpoint(&turn_id, TurnInputCheckpoint::BeforeFinalAnswer, 10_000,)
                .len(),
            10_000
        );
        assert_eq!(
            stream.acknowledge_durable_consumed_through(
                &turn_id,
                SessionInputCursor::new(9, 10_000),
            ),
            10_000
        );
        let inner = stream
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(inner.records.is_empty());
        assert!(inner.idempotency.is_empty());
        assert_eq!(inner.durable_consumed_total, 10_000);
        assert_eq!(
            inner.consumed_cursor,
            Some(SessionInputCursor::new(9, 10_000))
        );
    }

    #[test]
    fn ten_thousand_durable_terminal_inputs_leave_only_constant_watermarks_hot() {
        let stream = SessionInputStream::new("bounded-session");
        let turn_id = TurnId::from_string("turn-bounded");
        for sequence in 1..=10_000 {
            let envelope = SessionInputEnvelope::text(
                "bounded-session",
                InputSourceKind::Api,
                format!("terminal input {sequence}"),
            )
            .with_idempotency_key(format!("terminal-input-{sequence}"));
            let receipt = SessionInputReceipt {
                input_id: envelope.input_id.clone(),
                session_id: "bounded-session".to_string(),
                status: SessionInputStatus::Consumed,
                decision: InputRoutingDecision::SupplementCurrentTurn,
                relation_proposal: None,
                reason: Some(InputRoutingReason::new(
                    "durable_terminal",
                    "terminal state already committed by Session storage",
                    10_000,
                )),
                active_turn_id: Some(turn_id.clone()),
                evidence_refs: vec![format!("durable-input:{sequence}")],
                cursor: Some(SessionInputCursor::new(7, sequence)),
                created_at: Utc::now(),
            };
            stream.project_durable(envelope, receipt);
        }

        let inner = stream
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(inner.records.is_empty());
        assert!(inner.idempotency.is_empty());
        assert_eq!(inner.admitted_total, 10_000);
        assert_eq!(inner.durable_consumed_total, 10_000);
        assert_eq!(
            inner.admitted_cursor,
            Some(SessionInputCursor::new(7, 10_000))
        );
        assert_eq!(
            inner.consumed_cursor,
            Some(SessionInputCursor::new(7, 10_000))
        );
    }

    #[test]
    fn durable_terminal_replay_at_or_below_watermark_is_not_double_counted() {
        let stream = SessionInputStream::new("replay-session");
        let envelope =
            SessionInputEnvelope::text("replay-session", InputSourceKind::Api, "durable replay")
                .with_idempotency_key("durable-replay-1");
        let receipt = SessionInputReceipt {
            input_id: envelope.input_id.clone(),
            session_id: "replay-session".to_string(),
            status: SessionInputStatus::Consumed,
            decision: InputRoutingDecision::SupplementCurrentTurn,
            relation_proposal: None,
            reason: None,
            active_turn_id: Some(TurnId::from_string("turn-replay")),
            evidence_refs: Vec::new(),
            cursor: Some(SessionInputCursor::new(1, 9)),
            created_at: Utc::now(),
        };
        stream.project_durable(envelope.clone(), receipt.clone());
        stream.project_durable(envelope, receipt);

        let projection = stream.projection();
        assert_eq!(projection.total, 1);
        assert_eq!(projection.consumed_count, 1);
        assert!(projection.inputs.is_empty());
    }
}
