//! Session input materialization and hot projection plane.
//!
//! This child module extends the Runtime facade without owning a second state
//! authority. Every read and write still targets the parent service's
//! ActiveSession aggregate, durable Session port, and Runtime projections.

use super::*;

impl RuntimeService {
    pub(crate) fn session_input_runtime_state(
        &self,
        session_id: &str,
    ) -> runtime::RuntimeInputState {
        let active_turn_id = self
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .iter()
            .find_map(|(turn_id, control)| {
                (control.session_id == session_id).then(|| TurnId::from_string(turn_id.clone()))
            });
        runtime::RuntimeInputState {
            active_turn_id,
            waiting_for_approval: self
                .runtime_services
                .approval_queue()
                .pending()
                .iter()
                .any(|request| request.source.session_id.as_deref() == Some(session_id)),
            waiting_for_clarification: false,
        }
    }

    pub(crate) fn resolve_session_approval_control(
        &self,
        session_id: &str,
        content: &str,
        classification_json: Option<&str>,
    ) -> Result<Option<runtime::GlobalApprovalDecisionReceipt>, String> {
        let Some(command) = parse_session_approval_control(content) else {
            return Ok(None);
        };
        let queue = self.runtime_services.approval_queue();
        let pending = queue
            .pending()
            .into_iter()
            .filter(|request| request.source.session_id.as_deref() == Some(session_id))
            .collect::<Vec<_>>();
        let request = match command.approval_id.as_deref() {
            Some(id) => pending
                .into_iter()
                .find(|request| request.approval_id == id)
                .ok_or_else(|| {
                    format!("pending approval `{id}` does not belong to this Session")
                })?,
            None if pending.len() == 1 => pending.into_iter().next().expect("length checked"),
            None if pending.is_empty() => {
                return Err("this Session has no pending approval".to_string())
            }
            None => {
                return Err(
                    "multiple approvals are pending; include the approval id explicitly"
                        .to_string(),
                )
            }
        };
        let actor_id = surface_actor_from_classification(classification_json)
            .unwrap_or_else(|| format!("session:{session_id}:human"));
        let receipt = queue.decide_surface_human(
            &actor_id,
            runtime::ApprovalDecisionCommand {
                approval_id: request.approval_id.clone(),
                approved: command.approved,
                skip: command.skip,
                reason: if command.skip {
                    "skipped through the bound external Surface".to_string()
                } else if command.approved {
                    "approved through the bound external Surface".to_string()
                } else {
                    "denied through the bound external Surface".to_string()
                },
                scope: command.scope,
                actor: harness_contract::policy::ApprovalDecisionActor {
                    kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                    actor_id: actor_id.clone(),
                },
                evidence_refs: vec![
                    "surface.session_input.explicit_approval".to_string(),
                    format!("session:{session_id}"),
                ],
            },
        )?;
        self.runtime_services
            .approval_coordinator()
            .notify_decision(&request.approval_id);
        let _ = self.emit_session_event(
            session_id,
            runtime::CowdEvent::ApprovalResolved {
                request_id: request.approval_id.clone(),
                status: receipt.status,
                scope: Some(command.scope),
                actor_id: Some(actor_id),
            },
        );
        Ok(Some(receipt))
    }

    pub(crate) fn is_session_turn_active(&self, session_id: &str, turn_id: &str) -> bool {
        self.active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .get(turn_id)
            .is_some_and(|control| control.session_id == session_id)
    }

    fn active_execution_for_turn(&self, session_id: &str, turn_id: &str) -> Option<String> {
        let in_process = self
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .get(turn_id)
            .filter(|control| control.session_id == session_id)
            .and_then(|control| control.execution_id.clone());
        in_process.or_else(|| {
            self.runtime_services
                .session_execution_index(session_id)
                .active_execution_ids
                .into_iter()
                .find(|execution_id| {
                    self.runtime_services
                        .execution_live(execution_id)
                        .and_then(|live| live.turn_id)
                        .is_some_and(|candidate| candidate == turn_id)
                })
        })
    }

    pub(super) fn session_input_projection_identity(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
    ) -> (String, String, bool) {
        if let Some(target_turn_id) = record.target_turn_id.as_deref() {
            let execution_id = self
                .active_execution_for_turn(&record.session_id, target_turn_id)
                .unwrap_or_else(|| {
                    runtime::session_ingress_graph_id(
                        &record.session_id,
                        &record.request_id,
                        &record.turn_id,
                    )
                });
            return (execution_id, target_turn_id.to_string(), true);
        }
        (
            runtime::session_ingress_graph_id(
                &record.session_id,
                &record.request_id,
                &record.turn_id,
            ),
            record.turn_id.clone(),
            false,
        )
    }

    pub(crate) async fn deliver_durable_session_input_view(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        content: String,
        status: SessionInputStatus,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let relation_proposal = record
            .classification_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .and_then(|value| value.get("relation_proposal").cloned())
            .and_then(|value| serde_json::from_value::<InputRelationProposal>(value).ok());
        let active_turn_id = record
            .target_turn_id
            .as_ref()
            .map(|turn_id| TurnId::from_string(turn_id.clone()));
        let created_at = chrono::DateTime::<Utc>::from_timestamp_millis(
            record.created_at_ms.min(i64::MAX as u64) as i64,
        )
        .unwrap_or_else(Utc::now);
        let envelope = SessionInputEnvelope {
            input_id: SessionInputId::from_string(record.input_id.clone()),
            session_id: record.session_id.clone(),
            source_kind: InputSourceKind::Runtime,
            payload_kind: InputPayloadKind::Text,
            content_preview: content.chars().take(160).collect(),
            content,
            source_ref: Some(format!("session-input:{}", record.input_id)),
            source_message_id: Some(record.message_id.clone()),
            idempotency_key: record.request_id.clone(),
            task_route_hint: record.task_route_hint.clone(),
            metadata: serde_json::json!({
                "durable_request_id": record.request_id,
                "session_generation": record.session_generation,
                "relation_proposal": relation_proposal.clone(),
            }),
            created_at,
        };
        let receipt = SessionInputReceipt {
            input_id: envelope.input_id.clone(),
            session_id: record.session_id.clone(),
            status,
            decision: record.decision,
            relation_proposal,
            reason: Some(InputRoutingReason::new(
                "durable_delivery",
                "input delivered from the durable Session queue",
                10_000,
            )),
            active_turn_id,
            evidence_refs: vec![format!("session-input:{}", record.input_id)],
            cursor: Some(harness_contract::turn::SessionInputCursor::new(
                record.session_generation,
                u64::try_from(record.sequence).unwrap_or(u64::MAX),
            )),
            created_at,
        };
        self.project_durable_session_input(envelope, receipt).await
    }

    /// Refresh the process-local turn inbox from a durable Session admission.
    /// Gateway/Memory retain lifecycle authority; Runtime only receives the
    /// content needed by active-turn checkpoints.
    pub(crate) async fn project_durable_session_input(
        &self,
        envelope: SessionInputEnvelope,
        receipt: SessionInputReceipt,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let session_id = envelope.session_id.clone();
        let stream = self.session_input_stream_for(&session_id).await?;
        stream.project_durable(envelope, receipt.clone());
        self.emit_session_input_events(&session_id, &stream, Some(receipt));
        Ok(())
    }

    pub(crate) fn project_durable_session_receipt(
        &self,
        session_id: &str,
        receipt: SessionInputReceipt,
    ) {
        let stream = self
            .sessions
            .session(session_id)
            .and_then(|session| session.input());
        if let Some(stream) = stream {
            stream.project_durable_receipt(&receipt);
            self.emit_session_input_events(session_id, &stream, Some(receipt));
        }
    }

    /// Release checkpoint-consumed hot inputs only after Session storage has
    /// atomically committed the terminal transcript and its consumed cursor.
    /// Durable Session rows remain the historical source of truth.
    pub(crate) fn acknowledge_durable_session_inputs_through(
        &self,
        session_id: &str,
        turn_id: &str,
        session_generation: u64,
        consumed_input_sequence: usize,
    ) -> usize {
        let stream = self
            .sessions
            .session(session_id)
            .and_then(|session| session.input());
        let Some(stream) = stream else {
            return 0;
        };
        let released = stream.acknowledge_durable_consumed_through(
            &TurnId::from_string(turn_id.to_string()),
            harness_contract::turn::SessionInputCursor::new(
                session_generation,
                u64::try_from(consumed_input_sequence).unwrap_or(u64::MAX),
            ),
        );
        if released > 0 {
            self.emit_session_input_events(session_id, &stream, None);
        }
        released
    }

    /// Report whether an active-turn input has already crossed a Runtime
    /// checkpoint. The durable ingress worker uses this receipt before
    /// inspecting terminal turn state: a supplement consumed immediately
    /// before the target turn completed must be acknowledged, not promoted
    /// into a second turn.
    pub(crate) fn session_input_checkpoint_consumed(
        &self,
        session_id: &str,
        input_id: &str,
        target_turn_id: Option<&str>,
    ) -> bool {
        let stream = self
            .sessions
            .session(session_id)
            .and_then(|session| session.input());
        let Some(stream) = stream else {
            return false;
        };
        let input_id = SessionInputId::from_string(input_id.to_string());
        stream.record_snapshot(&input_id).is_some_and(|record| {
            record.status == SessionInputStatus::Consumed
                && record.consumed_at.is_some()
                && target_turn_id.is_none_or(|turn_id| {
                    record
                        .active_turn_id
                        .as_ref()
                        .is_some_and(|active| active.as_str() == turn_id)
                })
        })
    }

    pub(crate) async fn publish_user_message_committed(
        &self,
        record: &session::SessionRuntimeOutboxRecord,
        content: &str,
    ) -> (String, String, bool) {
        let (execution_id, projection_turn_id, supplemental) =
            self.session_input_projection_identity(record);
        if !supplemental {
            self.record_live_execution(
                &record.session_id,
                execution_id.clone(),
                record.turn_id.clone(),
            );
        }
        self.projection_hub
            .publish(
                &record.session_id,
                SessionProjectionEvent::UserMessageCommitted {
                    session_id: record.session_id.clone(),
                    message_id: record.message_id.clone(),
                    sequence: record.sequence,
                    execution_id: execution_id.clone(),
                    turn_id: projection_turn_id.clone(),
                    input_turn_id: record.turn_id.clone(),
                    supplemental,
                    content: content.to_string(),
                    created_at_ms: record.created_at_ms,
                },
            )
            .await;
        (execution_id, projection_turn_id, supplemental)
    }

    fn emit_session_input_materialized(&self, session_id: &str, materialized: serde_json::Value) {
        let Some(bus) = self
            .sessions
            .session(session_id)
            .and_then(|session| session.event_bus())
        else {
            return;
        };
        bus.emit(runtime::CowdEvent::Warning {
            message: format!("session input graph materialized: {materialized}"),
        });
    }

    pub(crate) fn emit_session_event(&self, session_id: &str, event: runtime::CowdEvent) -> bool {
        let bus = self
            .sessions
            .session(session_id)
            .and_then(|session| session.event_bus());
        if let Some(bus) = bus {
            bus.emit(event);
            true
        } else {
            false
        }
    }

    pub(crate) async fn session_input_projection(
        &self,
        session_id: &str,
    ) -> Result<SessionInputProjection, RuntimeTurnExecutionError> {
        let stream = self.session_input_stream_for(session_id).await?;
        Ok(stream.projection())
    }

    pub(crate) async fn active_turn_inbox(
        &self,
        session_id: &str,
        turn_id: Option<TurnId>,
    ) -> Result<TurnInboxSnapshot, RuntimeTurnExecutionError> {
        let stream = self.session_input_stream_for(session_id).await?;
        Ok(stream.inbox_snapshot(turn_id))
    }

    pub(super) async fn session_input_stream_for(
        &self,
        session_id: &str,
    ) -> Result<runtime::SessionInputStream, RuntimeTurnExecutionError> {
        if let Some(stream) = self
            .sessions
            .session(session_id)
            .and_then(|session| session.input())
        {
            return Ok(stream);
        }
        Err(RuntimeTurnExecutionError::Runtime(format!(
            "session {session_id} has no atomically published input stream"
        )))
    }

    pub(super) fn emit_session_input_events(
        &self,
        session_id: &str,
        stream: &runtime::SessionInputStream,
        receipt: Option<SessionInputReceipt>,
    ) {
        let Some(bus) = self
            .sessions
            .session(session_id)
            .and_then(|session| session.event_bus())
        else {
            return;
        };
        if let Some(receipt) = receipt {
            bus.emit(runtime::CowdEvent::SessionInputReceived { receipt });
        }
        bus.emit(runtime::CowdEvent::SessionInputProjection {
            projection: stream.projection(),
        });
        bus.emit(runtime::CowdEvent::TurnInboxUpdated {
            inbox: stream.inbox_snapshot(None),
        });
    }

    pub(super) async fn persist_session_input_domain_event(
        &self,
        session_id: &str,
        kind: SessionInputJournalKind,
        receipt: Option<&SessionInputReceipt>,
        record: Option<&runtime::SessionInputRecord>,
        stream: &runtime::SessionInputStream,
        dedup_key: &str,
    ) {
        if let Err(error) = self.ensure_session_domain_record(session_id).await {
            tracing::warn!(
                %session_id,
                kind = kind.as_str(),
                error = %error,
                "failed to ensure session before persisting session input runtime event"
            );
            return;
        }
        let input_projection = stream.projection();
        let turn_inbox = stream.inbox_snapshot(None);
        let payload = match serde_json::to_value(SessionInputDomainEventPayload {
            input: receipt,
            record,
            input_projection: input_projection.clone(),
            turn_inbox: turn_inbox.clone(),
        }) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::error!(
                    %session_id,
                    kind = kind.as_str(),
                    error = %error,
                    "failed to encode typed Session input domain event"
                );
                return;
            }
        };
        // Deterministic journal identity so a replay of the same semantic
        // fact is appended once, regardless of process-local timing.
        let event_id = format!(
            "session-input:{}:{}:{}",
            kind.as_str(),
            session_id,
            dedup_key
        );
        if let Err(error) = self
            .session_data
            .append_session_input_journal(
                session_id,
                kind,
                payload,
                Utc::now().timestamp_millis().max(0) as u64,
                &event_id,
            )
            .await
        {
            tracing::warn!(
                %session_id,
                kind = kind.as_str(),
                error = %error,
                "failed to persist session input runtime event"
            );
        }
        // The durable ingress/outbox or graph transition is the canonical
        // input fact. This journal is an auxiliary audit projection, so its
        // availability must not suppress the process-local active view.
        self.runtime_services
            .update_hot_session_input(&input_projection, &turn_inbox);
    }

    async fn ensure_session_domain_record(
        &self,
        session_id: &str,
    ) -> Result<(), session::SessionError> {
        if self
            .session_data
            .stored_session(session_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        Err(session::SessionError::InvalidArgument(format!(
            "session {session_id} must be created through SessionActivationCoordinator before runtime events are persisted"
        )))
    }
}
