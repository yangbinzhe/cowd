use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::Utc;

use crate::gateway::ActiveSessions;
use crate::runtime_boundary::{
    RuntimeBoundaryClock, RuntimeBoundarySnapshot, RuntimeBoundaryStatus,
};
use crate::runtime_protocol::{RuntimeErrorKind, RuntimeRequest, RuntimeResponse};
use crate::session_kernel::{RuntimeCommand, SessionKernel};
use crate::session_lifecycle_kernel::SessionLifecycleKernel;
use harness_contract::{
    task::{TaskId, TaskTurnBinding},
    turn::{
        InputRoutingDecision, SessionInputEnvelope, SessionInputId, SessionInputProjection,
        SessionInputReceipt, TurnEvent, TurnId, TurnInboxSnapshot, TurnInput, TurnJournalEnvelope,
        TurnJournalPhase, TurnReceipt, TurnStatus,
    },
};
use runtime::agent_collaboration::CollaborationContextResult;
use session::SessionLeaseRegistry;
use tokio::time::timeout;

use crate::services::{
    ActiveMessagesPage, SessionCompactResult, SessionMessageCounts, SessionStatsSnapshot,
    SessionTokenCounts,
};

#[derive(Debug)]
pub(crate) enum RuntimeTurnExecutionError {
    NotFound(String),
    Timeout { seconds: u64 },
    Runtime(String),
    Join(String),
}

impl RuntimeTurnExecutionError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::NotFound(message) | Self::Runtime(message) | Self::Join(message) => {
                message.clone()
            }
            Self::Timeout { seconds } => format!("turn timed out after {seconds}s"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeTurnExecution {
    pub(crate) summary: runtime::TurnSummary,
    pub(crate) receipt: TurnReceipt,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeTurnOptions {
    pub(crate) profile: runtime::ContextProfile,
    pub(crate) max_iterations: Option<usize>,
    pub(crate) pre_messages: Vec<runtime::ConversationMessage>,
}

impl Default for RuntimeTurnOptions {
    fn default() -> Self {
        Self {
            profile: runtime::ContextProfile::MainTurn,
            max_iterations: None,
            pre_messages: Vec::new(),
        }
    }
}

trait RuntimeTurnBudgetTarget {
    fn max_iterations(&self) -> usize;
    fn set_max_iterations(&mut self, max_iterations: usize);
}

impl RuntimeTurnBudgetTarget for crate::runtime_entry::GatewayRuntimeEntry {
    fn max_iterations(&self) -> usize {
        self.max_iterations()
    }

    fn set_max_iterations(&mut self, max_iterations: usize) {
        self.set_max_iterations(max_iterations);
    }
}

fn apply_scoped_max_iterations<T: RuntimeTurnBudgetTarget>(
    runtime: &mut T,
    max_iterations: Option<usize>,
) -> Option<usize> {
    let previous = max_iterations.map(|_| runtime.max_iterations());
    if let Some(max_iterations) = max_iterations {
        runtime.set_max_iterations(max_iterations);
    }
    previous
}

fn restore_scoped_max_iterations<T: RuntimeTurnBudgetTarget>(
    runtime: &mut T,
    previous: Option<usize>,
) {
    if let Some(previous) = previous {
        runtime.set_max_iterations(previous);
    }
}

fn extract_session_target_ref(content: &str) -> Option<&str> {
    let marker = "@session:";
    let start = content.find(marker)? + marker.len();
    let rest = content[start..].trim_start();
    let end = rest
        .find(|ch: char| ch.is_whitespace() || ch == ',' || ch == ';')
        .unwrap_or(rest.len());
    let target = rest[..end].trim();
    (!target.is_empty()).then_some(target)
}

#[derive(Clone)]
pub(crate) struct RuntimeService {
    sessions: Arc<ActiveSessions>,
    lease_registry: Arc<SessionLeaseRegistry>,
    session_kernel: Arc<SessionKernel>,
    lifecycle_kernel: Arc<SessionLifecycleKernel>,
    started_at: Instant,
    turns: Arc<Mutex<BTreeMap<String, TurnReceipt>>>,
    turn_bindings: Arc<Mutex<BTreeMap<String, TaskTurnBinding>>>,
    session_inputs: Arc<Mutex<BTreeMap<String, runtime::SessionInputStream>>>,
    session_event_buses: Arc<Mutex<BTreeMap<String, runtime::CowdEventBus>>>,
    session_models: Arc<Mutex<BTreeMap<String, String>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionInputAdmission {
    pub(crate) receipt: SessionInputReceipt,
    pub(crate) materialized: Option<serde_json::Value>,
}

impl RuntimeService {
    #[must_use]
    pub(crate) fn new(
        sessions: Arc<ActiveSessions>,
        lease_registry: Arc<SessionLeaseRegistry>,
        session_kernel: Arc<SessionKernel>,
        lifecycle_kernel: Arc<SessionLifecycleKernel>,
        started_at: Instant,
    ) -> Self {
        Self {
            sessions,
            lease_registry,
            session_kernel,
            lifecycle_kernel,
            started_at,
            turns: Arc::new(Mutex::new(BTreeMap::new())),
            turn_bindings: Arc::new(Mutex::new(BTreeMap::new())),
            session_inputs: Arc::new(Mutex::new(BTreeMap::new())),
            session_event_buses: Arc::new(Mutex::new(BTreeMap::new())),
            session_models: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[must_use]
    pub(crate) fn status_value(&self) -> serde_json::Value {
        let status = self.status();
        serde_json::json!({
            "ok": true,
            "protocol_version": status.protocol_version,
            "runtime_host": status.runtime_host,
            "active_sessions": status.active_sessions,
            "uptime_secs": status.uptime_secs,
        })
    }

    #[must_use]
    pub(crate) fn session_kernel(&self) -> Arc<SessionKernel> {
        self.session_kernel.clone()
    }

    #[must_use]
    pub(crate) fn lifecycle_kernel(&self) -> Arc<SessionLifecycleKernel> {
        self.lifecycle_kernel.clone()
    }

    #[must_use]
    pub(crate) fn status(&self) -> RuntimeBoundaryStatus {
        RuntimeBoundaryStatus {
            protocol_version: crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            runtime_host: "gateway-runtime-host",
            active_sessions: self.sessions.list().len(),
            uptime_secs: self.clock().uptime_secs(),
        }
    }

    pub(crate) async fn snapshot_value(&self) -> serde_json::Value {
        let snapshot = self.snapshot().await;
        let leases = self.lease_registry.list().await;
        let turns = self.turns_snapshot();
        let turn_bindings = self.turn_bindings_snapshot();
        serde_json::json!({
            "ok": true,
            "kind": "gateway_runtime_snapshot",
            "protocol_version": snapshot.protocol_version,
            "runtime_host": snapshot.runtime_host,
            "active_sessions": snapshot.active_sessions,
            "uptime_secs": snapshot.uptime_secs,
            "sessions": snapshot.sessions,
            "leases": {
                "total": leases.len(),
                "items": leases,
            },
            "lifecycle": self.lifecycle_kernel.snapshots().await,
            "turns": turns,
            "turn_bindings": turn_bindings,
            "transport": {
                "control": "gateway_http",
                "projection": "http_optional",
            },
        })
    }

    pub(crate) async fn submit_turn_value(
        &self,
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> serde_json::Value {
        if prompt.trim().is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "prompt is required",
            });
        }

        let input = Self::turn_input_for(session_id, task_id, prompt);
        let receipt = self.record_turn_from_input(&input, TurnStatus::Pending);
        let turn_id = input.turn_id.to_string();
        let journal_sequence = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Submitted, None)
            .await
            .transpose()
            .map_err(|error| {
                tracing::warn!(
                    turn_id = %turn_id,
                    error = %error,
                    "failed to persist submitted turn journal"
                );
                error
            })
            .ok()
            .flatten();

        serde_json::json!({
            "ok": true,
            "dispatch": "runtime_service",
            "accepted": true,
            "durable_journal": journal_sequence.is_some(),
            "journal_sequence": journal_sequence,
            "turn": receipt,
        })
    }

    pub(crate) fn turn_value(&self, turn_id: &str) -> serde_json::Value {
        let turns = self
            .turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match turns.get(turn_id) {
            Some(turn) => serde_json::json!({
                "ok": true,
                "turn": turn,
            }),
            None => serde_json::json!({
                "ok": false,
                "error": "turn not found",
            }),
        }
    }

    pub(crate) fn turns_value(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "turns": self.turns_snapshot(),
            "turn_bindings": self.turn_bindings_snapshot(),
        })
    }

    pub(crate) async fn cancel_turn_value(&self, turn_id: &str) -> serde_json::Value {
        let (turn, aborted_run_id) = {
            let mut turns = self
                .turns
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(turn) = turns.get_mut(turn_id) else {
                return serde_json::json!({
                    "ok": false,
                    "error": "turn not found",
                });
            };

            turn.status = TurnStatus::Cancelled;
            turn.events.push(TurnEvent::new(
                TurnId::from_string(turn_id.to_string()),
                TurnStatus::Cancelled,
            ));
            let aborted_run_id = turn
                .session_id
                .as_deref()
                .and_then(crate::api_routes::abort_active_turn);
            (turn.clone(), aborted_run_id)
        };
        let journal_sequence = self
            .persist_turn_receipt_journal(&turn, TurnJournalPhase::Cancelled, None)
            .await
            .transpose()
            .map_err(|error| {
                tracing::warn!(
                    turn_id = %turn_id,
                    error = %error,
                    "failed to persist cancelled turn journal"
                );
                error
            })
            .ok()
            .flatten();

        serde_json::json!({
            "ok": true,
            "cancelled": true,
            "aborted_run_id": aborted_run_id,
            "journal_sequence": journal_sequence,
            "turn": turn,
        })
    }

    async fn persist_turn_input_journal(
        &self,
        input: &TurnInput,
        phase: TurnJournalPhase,
        message: Option<String>,
    ) -> Option<Result<Option<usize>, memory::MemoryError>> {
        let session_id = input
            .session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())?;
        let envelope = TurnJournalEnvelope::new(
            session_id,
            input.turn_id.clone(),
            phase,
            "gateway.runtime_service",
            serde_json::json!({
                "status": phase.as_str(),
                "prompt": input.prompt.clone(),
                "prompt_preview": input.prompt.chars().take(240).collect::<String>(),
                "task_id": input.task_id.clone(),
                "message": message,
                "created_at": input.created_at,
            }),
        );
        Some(
            self.session_kernel
                .append_turn_journal_event(session_id, envelope)
                .await,
        )
    }

    async fn persist_turn_receipt_journal(
        &self,
        receipt: &TurnReceipt,
        phase: TurnJournalPhase,
        message: Option<String>,
    ) -> Option<Result<Option<usize>, memory::MemoryError>> {
        let session_id = receipt
            .session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())?;
        let envelope = TurnJournalEnvelope::new(
            session_id,
            receipt.turn_id.clone(),
            phase,
            "gateway.runtime_service",
            serde_json::json!({
                "status": receipt.status.as_str(),
                "task_id": receipt.task_id.clone(),
                "context_report_id": receipt.context_report_id.clone(),
                "message": message,
                "completed_at": receipt.completed_at,
            }),
        );
        Some(
            self.session_kernel
                .append_turn_journal_event(session_id, envelope)
                .await,
        )
    }

    fn turn_input_for(
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> TurnInput {
        let mut input = TurnInput::new(prompt);
        input.session_id = session_id;
        input.task_id = task_id;
        input
    }

    fn record_turn_from_input(&self, input: &TurnInput, status: TurnStatus) -> TurnReceipt {
        let mut receipt = TurnReceipt::from_input(input, status.clone());
        receipt
            .events
            .push(TurnEvent::new(input.turn_id.clone(), status));
        self.record_turn_binding(input);
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), receipt.clone());
        receipt
    }

    fn turns_snapshot(&self) -> Vec<TurnReceipt> {
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    fn turn_bindings_snapshot(&self) -> Vec<TaskTurnBinding> {
        self.turn_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn has_active_session(&self, session_id: &str) -> bool {
        self.sessions.get(session_id).is_some()
    }

    pub(crate) fn register_runtime(
        &self,
        session_id: String,
        runtime: crate::runtime_entry::GatewayRuntimeEntry,
    ) -> Result<Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>>, String>
    {
        let input_stream = runtime.session_input_stream();
        let cowd_bus = runtime.cowd_bus().cloned();
        let model = runtime
            .session()
            .model
            .filter(|model| !model.trim().is_empty());
        let result = self.sessions.register(session_id.clone(), runtime);
        if result.is_ok() {
            self.session_inputs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(session_id.clone(), input_stream);
            if let Some(model) = model {
                self.session_models
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(session_id.clone(), model);
            } else {
                self.session_models
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&session_id);
            }
            if let Some(cowd_bus) = cowd_bus {
                self.session_event_buses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(session_id, cowd_bus);
            } else {
                self.session_event_buses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&session_id);
            }
        }
        result
    }

    pub(crate) fn remove_active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>> {
        self.session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.session_event_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.session_models
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        self.sessions.remove(session_id)
    }

    pub(crate) fn remove_active_runtime_if_present(&self, session_id: &str) -> bool {
        self.remove_active_runtime(session_id).is_some()
    }

    pub(crate) async fn cowd_event_receiver(
        &self,
        session_id: &str,
    ) -> Option<tokio::sync::broadcast::Receiver<runtime::CowdEvent>> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.cowd_bus().map(|bus| bus.subscribe())
    }

    pub(crate) async fn admit_session_input(
        &self,
        envelope: SessionInputEnvelope,
    ) -> Result<SessionInputReceipt, RuntimeTurnExecutionError> {
        self.admit_session_input_with_materialized(envelope)
            .await
            .map(|admission| admission.receipt)
    }

    pub(crate) async fn admit_session_input_with_materialized(
        &self,
        envelope: SessionInputEnvelope,
    ) -> Result<SessionInputAdmission, RuntimeTurnExecutionError> {
        let session_id = envelope.session_id.clone();
        let content = envelope.content.clone();
        let stream = self.session_input_stream_for(&session_id).await?;
        let receipt = stream.admit(envelope, stream.runtime_state());
        let materialized = self
            .materialize_session_input_decision(&session_id, &receipt, &content)
            .await;
        let materialized_for_event = materialized.clone();
        let record_for_event = stream.record_snapshot(&receipt.input_id);
        self.emit_session_input_events(&session_id, &stream, Some(receipt.clone()));
        if let Some(materialized) = materialized {
            self.emit_session_input_materialized(&session_id, materialized);
        }
        self.persist_session_input_runtime_event(
            &session_id,
            "SessionInputReceived",
            Some(&receipt),
            record_for_event.as_ref(),
            &stream,
            materialized_for_event.as_ref(),
        )
        .await;
        Ok(SessionInputAdmission {
            receipt,
            materialized: materialized_for_event,
        })
    }

    pub(crate) async fn cancel_session_input(
        &self,
        session_id: &str,
        input_id: SessionInputId,
        reason: &str,
    ) -> Result<SessionInputReceipt, RuntimeTurnExecutionError> {
        let stream = self.session_input_stream_for(session_id).await?;
        let record = stream
            .cancel_input(&input_id, reason)
            .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?;
        let receipt = record.to_receipt();
        self.emit_session_input_events(session_id, &stream, Some(receipt.clone()));
        self.persist_session_input_runtime_event(
            session_id,
            "SessionInputCancelled",
            Some(&receipt),
            Some(&record),
            &stream,
            None,
        )
        .await;
        Ok(receipt)
    }

    pub(crate) async fn reclassify_session_input(
        &self,
        session_id: &str,
        input_id: SessionInputId,
        decision: InputRoutingDecision,
        reason: &str,
    ) -> Result<SessionInputReceipt, RuntimeTurnExecutionError> {
        let stream = self.session_input_stream_for(session_id).await?;
        let record = stream
            .reclassify_input(&input_id, decision, reason)
            .map_err(|error| RuntimeTurnExecutionError::Runtime(error.to_string()))?;
        let receipt = record.to_receipt();
        let materialized = self
            .materialize_session_input_decision(session_id, &receipt, &record.envelope.content)
            .await;
        let materialized_for_event = materialized.clone();
        self.emit_session_input_events(session_id, &stream, Some(receipt.clone()));
        if let Some(materialized) = materialized {
            self.emit_session_input_materialized(session_id, materialized);
        }
        self.persist_session_input_runtime_event(
            session_id,
            "SessionInputReclassified",
            Some(&receipt),
            Some(&record),
            &stream,
            materialized_for_event.as_ref(),
        )
        .await;
        Ok(receipt)
    }

    async fn materialize_session_input_decision(
        &self,
        session_id: &str,
        receipt: &SessionInputReceipt,
        content: &str,
    ) -> Option<serde_json::Value> {
        match receipt.decision {
            InputRoutingDecision::SpawnSubtask => {
                let task = runtime::AgentTask {
                    task_id: String::new(),
                    team_id: format!("session-input-team-{session_id}"),
                    session_id: session_id.to_string(),
                    role_id: "runtime-delegated-agent".to_string(),
                    agent_id: None,
                    objective: content.to_string(),
                    expected_output:
                        "Inspect the delegated request and report progress back to the parent session."
                            .to_string(),
                    context_refs: receipt.evidence_refs.clone(),
                    evidence_refs: receipt.evidence_refs.clone(),
                    status: runtime::AgentTaskStatus::Pending,
                    outcome: None,
                    created_at_ms: 0,
                    updated_at_ms: 0,
                };
                let task_receipt = runtime::global_agent_task_mailbox().assign(task);
                Some(serde_json::json!({
                    "kind": "session_input.spawn_subtask",
                    "input_id": receipt.input_id.to_string(),
                    "task_receipt": task_receipt,
                }))
            }
            InputRoutingDecision::RouteCrossSession => {
                let target_ref = extract_session_target_ref(content).unwrap_or("active");
                let bridge = runtime::SessionExecutionPlane::bridge(runtime::CrossSessionMessage {
                    from_session_id: session_id.to_string(),
                    target_ref: target_ref.to_string(),
                    command: content.to_string(),
                    actor: Some("session_input_stream".to_string()),
                    evidence_refs: receipt.evidence_refs.clone(),
                });
                Some(serde_json::json!({
                    "kind": "session_input.route_cross_session",
                    "input_id": receipt.input_id.to_string(),
                    "bridge": bridge,
                }))
            }
            InputRoutingDecision::CreateNewSession => {
                let session = self
                    .create_session_from_input(session_id, receipt, content)
                    .await;
                Some(serde_json::json!({
                    "kind": "session_input.create_new_session",
                    "input_id": receipt.input_id.to_string(),
                    "status": "created",
                    "session": session,
                }))
            }
            _ => None,
        }
    }

    async fn create_session_from_input(
        &self,
        parent_session_id: &str,
        receipt: &SessionInputReceipt,
        content: &str,
    ) -> serde_json::Value {
        let session_id = uuid::Uuid::new_v4().to_string();
        let model = self
            .model_for_session(parent_session_id)
            .await
            .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string());
        let runtime = match self.build_session_runtime_entry(&session_id, &model) {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(
                    %parent_session_id,
                    %session_id,
                    error = %error,
                    "failed to build input-created session runtime"
                );
                let lifecycle = match self
                    .persist_input_created_session_lifecycle(&session_id, Some(model.clone()))
                    .await
                {
                    Ok(lifecycle) => lifecycle,
                    Err(lifecycle_error) => {
                        return serde_json::json!({
                            "created": false,
                            "session_id": session_id,
                            "model": model,
                            "runtime_registered": false,
                            "error": format!("failed to build runtime: {error}"),
                            "lifecycle_error": lifecycle_error,
                        });
                    }
                };
                return serde_json::json!({
                    "created": true,
                    "session_id": session_id,
                    "model": model,
                    "runtime_registered": false,
                    "degraded_reason": format!("failed to build runtime: {error}"),
                    "lifecycle": lifecycle,
                    "error": format!("failed to build runtime: {error}"),
                });
            }
        };

        if let Err(error) = self.register_runtime(session_id.clone(), runtime) {
            tracing::warn!(
                %parent_session_id,
                %session_id,
                error = %error,
                "failed to register input-created session runtime"
            );
            return serde_json::json!({
                "created": false,
                "session_id": session_id,
                "error": format!("failed to register runtime: {error}"),
            });
        }

        let lifecycle = match self
            .persist_input_created_session_lifecycle(&session_id, Some(model.clone()))
            .await
        {
            Ok(lifecycle) => lifecycle,
            Err(error) => {
                self.remove_active_runtime_if_present(&session_id);
                tracing::warn!(
                    %parent_session_id,
                    %session_id,
                    error = %error,
                    "failed to persist input-created session lifecycle"
                );
                return serde_json::json!({
                    "created": false,
                    "session_id": session_id,
                    "error": format!("failed to persist session lifecycle: {error}"),
                });
            }
        };

        let event_sequence = self
            .session_kernel
            .append_runtime_event(
                &session_id,
                memory::RuntimeEventScope::Session,
                "SessionInputCreatedSession",
                serde_json::json!({
                    "parent_session_id": parent_session_id,
                    "source_input_id": receipt.input_id.to_string(),
                    "source_evidence_refs": receipt.evidence_refs.clone(),
                    "initial_content_preview": content.chars().take(240).collect::<String>(),
                }),
            )
            .await
            .ok()
            .flatten();
        let lifecycle_event = self.lifecycle_kernel.mark_active(&session_id).await.ok();
        let lifecycle_snapshot = self.lifecycle_kernel.snapshot(&session_id).await;

        serde_json::json!({
            "created": true,
            "session_id": session_id,
            "parent_session_id": parent_session_id,
            "model": model,
            "runtime_registered": true,
            "lifecycle": lifecycle,
            "lifecycle_event": lifecycle_event,
            "lifecycle_snapshot": lifecycle_snapshot,
            "event_sequence": event_sequence,
        })
    }

    async fn persist_input_created_session_lifecycle(
        &self,
        session_id: &str,
        model: Option<String>,
    ) -> Result<serde_json::Value, String> {
        self.session_kernel
            .execute_runtime_command(RuntimeCommand::CreateSession {
                session_id: session_id.to_string(),
                model,
            })
            .await
            .map(|result| {
                serde_json::json!({
                    "persisted": result.persisted,
                    "runtime_event_sequence": result.runtime_event_sequence,
                    "kind": result.kind,
                })
            })
            .map_err(|error| error.to_string())
    }

    fn build_session_runtime_entry(
        &self,
        session_id: &str,
        model: &str,
    ) -> Result<crate::runtime_entry::GatewayRuntimeEntry, String> {
        let session = runtime::Session::new();
        if let Some(store) = self.session_kernel.unified_store() {
            crate::runtime_factory::create_runtime_entry_with_session_store(
                store,
                session,
                session_id,
                model.to_string(),
                vec![],
                true,
                true,
                None,
                runtime::PermissionMode::WorkspaceWrite,
                None,
                None,
            )
            .map_err(|error| error.to_string())
        } else {
            crate::runtime_factory::create_runtime_entry(
                session,
                session_id,
                model.to_string(),
                vec![],
                true,
                true,
                None,
                runtime::PermissionMode::WorkspaceWrite,
                None,
                None,
            )
            .map_err(|error| error.to_string())
        }
    }

    async fn model_for_session(&self, session_id: &str) -> Option<String> {
        if let Some(model) = self
            .session_models
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
            .filter(|model| !model.trim().is_empty())
        {
            return Some(model);
        }
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        let model = runtime_guard
            .session()
            .model
            .filter(|model| !model.trim().is_empty());
        if let Some(model) = model.as_ref() {
            self.session_models
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(session_id.to_string(), model.clone());
        }
        model
    }

    fn emit_session_input_materialized(&self, session_id: &str, materialized: serde_json::Value) {
        let Some(bus) = self
            .session_event_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
        else {
            return;
        };
        bus.emit(runtime::CowdEvent::Warning {
            message: format!("session input materialized: {materialized}"),
        });
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

    async fn session_input_stream_for(
        &self,
        session_id: &str,
    ) -> Result<runtime::SessionInputStream, RuntimeTurnExecutionError> {
        if let Some(stream) = self
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
        {
            return Ok(stream);
        }
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let runtime_guard = runtime_entry.lock().await;
        let stream = runtime_guard.session_input_stream();
        self.session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.to_string(), stream.clone());
        if let Some(bus) = runtime_guard.cowd_bus().cloned() {
            self.session_event_buses
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(session_id.to_string(), bus);
        }
        Ok(stream)
    }

    fn emit_session_input_events(
        &self,
        session_id: &str,
        stream: &runtime::SessionInputStream,
        receipt: Option<SessionInputReceipt>,
    ) {
        let Some(bus) = self
            .session_event_buses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
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

    async fn persist_session_input_runtime_event(
        &self,
        session_id: &str,
        kind: &str,
        receipt: Option<&SessionInputReceipt>,
        record: Option<&runtime::SessionInputRecord>,
        stream: &runtime::SessionInputStream,
        materialized: Option<&serde_json::Value>,
    ) {
        if let Err(error) = self.ensure_runtime_event_session(session_id).await {
            tracing::warn!(
                %session_id,
                %kind,
                error = %error,
                "failed to ensure session before persisting session input runtime event"
            );
            return;
        }
        if let Err(error) = self
            .session_kernel
            .append_runtime_event(
                session_id,
                memory::RuntimeEventScope::Turn,
                kind,
                serde_json::json!({
                    "input": receipt,
                    "record": record,
                    "input_projection": stream.projection(),
                    "turn_inbox": stream.inbox_snapshot(None),
                    "materialized": materialized,
                }),
            )
            .await
        {
            tracing::warn!(
                %session_id,
                %kind,
                error = %error,
                "failed to persist session input runtime event"
            );
        }
    }

    async fn ensure_runtime_event_session(
        &self,
        session_id: &str,
    ) -> Result<(), memory::MemoryError> {
        if self
            .session_kernel
            .stored_session(session_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let model = self.model_for_session(session_id).await;
        let command = RuntimeCommand::CreateSession {
            session_id: session_id.to_string(),
            model,
        };
        self.session_kernel.execute_runtime_command(command).await?;
        Ok(())
    }

    pub(crate) async fn configure_turn_context(
        &self,
        session_id: &str,
        profile: runtime::ContextProfile,
        resume_context: Option<runtime::ResumeContextPacket>,
        reality_context_items: Vec<runtime::ContextItem>,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.set_context_profile(profile);
        runtime_guard.replace_external_context_sources(
            &[
                runtime::ContextSourceKind::Fact,
                runtime::ContextSourceKind::Matrix,
            ],
            reality_context_items,
        );
        if let Some(packet) = resume_context {
            runtime_guard.inject_resume_context(packet);
        }
        Ok(())
    }

    pub(crate) async fn install_turn_control(
        &self,
        session_id: &str,
        cancellation_token: runtime::CancellationToken,
        hook_abort_signal: runtime::HookAbortSignal,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let mut runtime_guard = runtime_entry.lock().await;
        runtime_guard.install_turn_control(cancellation_token, hook_abort_signal);
        Ok(())
    }

    pub(crate) async fn run_turn_with_timeout(
        &self,
        session_id: &str,
        task_id: Option<String>,
        content: String,
        turn_timeout: Duration,
    ) -> Result<RuntimeTurnExecution, RuntimeTurnExecutionError> {
        self.run_turn_with_options(
            session_id,
            task_id,
            content,
            turn_timeout,
            RuntimeTurnOptions::default(),
        )
        .await
    }

    pub(crate) async fn run_turn_with_options(
        &self,
        session_id: &str,
        task_id: Option<String>,
        content: String,
        turn_timeout: Duration,
        options: RuntimeTurnOptions,
    ) -> Result<RuntimeTurnExecution, RuntimeTurnExecutionError> {
        let receipt = self
            .accept_turn_with_options(session_id, task_id, content.clone())
            .await?;
        self.run_accepted_turn_with_options(
            session_id,
            receipt.turn_id.clone(),
            content,
            turn_timeout,
            options,
        )
        .await
    }

    pub(crate) async fn accept_turn_with_options(
        &self,
        session_id: &str,
        task_id: Option<String>,
        content: String,
    ) -> Result<TurnReceipt, RuntimeTurnExecutionError> {
        if self.sessions.get(session_id).is_none() {
            return Err(RuntimeTurnExecutionError::NotFound(format!(
                "session {session_id} not found"
            )));
        }
        let input = Self::turn_input_for(Some(session_id.to_string()), task_id, content);
        self.record_turn_from_input(&input, TurnStatus::Pending);
        if let Some(Err(error)) = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Submitted, None)
            .await
        {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "failed to persist submitted turn journal: {error}"
            )));
        }
        let receipt = self.record_turn_from_input(&input, TurnStatus::Running);
        if let Some(Err(error)) = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Running, None)
            .await
        {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "failed to persist running turn journal: {error}"
            )));
        }
        if let Ok(stream) = self.session_input_stream_for(session_id).await {
            stream.set_active_turn(Some(receipt.turn_id.clone()));
            self.emit_session_input_events(session_id, &stream, None);
        }
        Ok(receipt)
    }

    pub(crate) async fn run_accepted_turn_with_options(
        &self,
        session_id: &str,
        turn_id: TurnId,
        content: String,
        turn_timeout: Duration,
        options: RuntimeTurnOptions,
    ) -> Result<RuntimeTurnExecution, RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let queued_next_options = options.clone();
        let turn_result = tokio::task::spawn_blocking(move || {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async move {
                let mut runtime_guard = runtime_entry.lock().await;
                runtime_guard.set_context_profile(options.profile);
                let previous_max_iterations =
                    apply_scoped_max_iterations(&mut *runtime_guard, options.max_iterations);
                for message in options.pre_messages {
                    if let Err(error) = runtime_guard.append_external_message(message).await {
                        restore_scoped_max_iterations(&mut *runtime_guard, previous_max_iterations);
                        return Ok(Err(error));
                    }
                }
                let result = timeout(
                    turn_timeout,
                    runtime_guard
                        .run_turn_async(&content, &runtime::permissions::SharedPrompter::none()),
                )
                .await;
                restore_scoped_max_iterations(&mut *runtime_guard, previous_max_iterations);
                result
            })
        })
        .await
        .map_err(|error| RuntimeTurnExecutionError::Join(format!("task join error: {error}")))?;

        match turn_result {
            Ok(Ok(summary)) => {
                let mut receipt = self.finish_turn(&turn_id, TurnStatus::Completed, None);
                receipt.context_report_id = Some(summary.context_turn_report.turn_id.clone());
                self.turns
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(turn_id.to_string(), receipt.clone());
                if let Some(Err(error)) = self
                    .persist_turn_receipt_journal(&receipt, TurnJournalPhase::Completed, None)
                    .await
                {
                    tracing::warn!(
                        turn_id = %turn_id,
                        error = %error,
                        "failed to persist completed turn journal"
                    );
                }
                if let Some(stream) = self
                    .session_inputs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(session_id)
                    .cloned()
                {
                    let queued_next = stream.drain_queued_next_for_dispatch(4);
                    self.emit_session_input_events(session_id, &stream, None);
                    self.dispatch_queued_next_turns(
                        session_id.to_string(),
                        queued_next,
                        turn_timeout,
                        queued_next_options,
                    );
                }
                Ok(RuntimeTurnExecution { summary, receipt })
            }
            Ok(Err(error)) => {
                let message = error.to_string();
                self.clear_session_input_turn_if_current(session_id, &turn_id);
                let receipt = self.finish_turn(&turn_id, TurnStatus::Failed, Some(message.clone()));
                if let Some(Err(error)) = self
                    .persist_turn_receipt_journal(
                        &receipt,
                        TurnJournalPhase::Failed,
                        Some(message.clone()),
                    )
                    .await
                {
                    tracing::warn!(
                        turn_id = %turn_id,
                        error = %error,
                        "failed to persist failed turn journal"
                    );
                }
                Err(RuntimeTurnExecutionError::Runtime(message))
            }
            Err(_) => {
                let message = format!("turn timed out after {}s", turn_timeout.as_secs());
                self.clear_session_input_turn_if_current(session_id, &turn_id);
                let receipt = self.finish_turn(&turn_id, TurnStatus::Failed, Some(message.clone()));
                if let Some(Err(error)) = self
                    .persist_turn_receipt_journal(
                        &receipt,
                        TurnJournalPhase::Failed,
                        Some(message.clone()),
                    )
                    .await
                {
                    tracing::warn!(
                        turn_id = %turn_id,
                        error = %error,
                        "failed to persist timeout turn journal"
                    );
                }
                Err(RuntimeTurnExecutionError::Timeout {
                    seconds: turn_timeout.as_secs(),
                })
            }
        }
    }

    fn clear_session_input_turn_if_current(&self, session_id: &str, turn_id: &TurnId) {
        let Some(stream) = self
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
        else {
            return;
        };
        if stream.active_turn_id().as_ref() == Some(turn_id) {
            stream.set_active_turn(None);
            self.emit_session_input_events(session_id, &stream, None);
        }
    }

    fn dispatch_queued_next_turns(
        &self,
        session_id: String,
        records: Vec<runtime::SessionInputRecord>,
        turn_timeout: Duration,
        options: RuntimeTurnOptions,
    ) {
        for record in records {
            let service = self.clone();
            let prompt = record.envelope.content.clone();
            let task_id = None;
            let session_id = session_id.clone();
            let options = options.clone();
            tokio::spawn(async move {
                let Ok(receipt) = service
                    .accept_turn_with_options(&session_id, task_id, prompt.clone())
                    .await
                else {
                    return;
                };
                if let Err(error) = service
                    .run_accepted_turn_with_options(
                        &session_id,
                        receipt.turn_id.clone(),
                        prompt,
                        turn_timeout,
                        options,
                    )
                    .await
                {
                    tracing::warn!(
                        %session_id,
                        turn_id = %receipt.turn_id,
                        error = %error.message(),
                        "queued next turn failed"
                    );
                }
            });
        }
    }

    fn start_running_turn(
        &self,
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> TurnReceipt {
        let mut input = TurnInput::new(prompt);
        input.session_id = session_id;
        input.task_id = task_id;
        let mut receipt = TurnReceipt::from_input(&input, TurnStatus::Running);
        receipt
            .events
            .push(TurnEvent::new(input.turn_id.clone(), TurnStatus::Running));
        self.record_turn_binding(&input);
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), receipt.clone());
        receipt
    }

    fn record_turn_binding(&self, input: &TurnInput) {
        let Some(task_id) = input
            .task_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        else {
            return;
        };
        let mut binding =
            TaskTurnBinding::new(TaskId::from_string(task_id.clone()), input.turn_id.clone());
        binding.session_id = input.session_id.clone();
        self.turn_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), binding);
    }

    fn finish_turn(
        &self,
        turn_id: &TurnId,
        status: TurnStatus,
        message: Option<String>,
    ) -> TurnReceipt {
        let mut turns = self
            .turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let turn = turns
            .entry(turn_id.to_string())
            .or_insert_with(|| TurnReceipt {
                turn_id: turn_id.clone(),
                status: status.clone(),
                session_id: None,
                task_id: None,
                events: Vec::new(),
                context_report_id: None,
                completed_at: None,
            });

        if turn.status != TurnStatus::Cancelled {
            turn.status = status.clone();
        }
        let mut event = TurnEvent::new(turn_id.clone(), status);
        event.message = message;
        turn.events.push(event);
        turn.completed_at = Some(Utc::now());
        turn.clone()
    }

    pub(crate) async fn session_snapshot(&self, session_id: &str) -> Option<runtime::Session> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        Some(runtime_guard.session().clone())
    }

    pub(crate) async fn sync_session_snapshot(
        &self,
        session_id: &str,
        session: &runtime::Session,
    ) -> Result<(), memory::MemoryError> {
        self.session_kernel
            .sync_runtime_session_snapshot(session_id, session)
            .await
            .map(|_| ())
    }

    pub(crate) async fn compact_active_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionCompactResult>, memory::MemoryError> {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return Ok(None);
        };

        let mut runtime_guard = runtime_entry.lock().await;
        let (result, session_snapshot) =
            runtime_guard.compact_active_session(runtime::CompactionConfig::default());
        drop(runtime_guard);

        self.sync_session_snapshot(session_id, &session_snapshot)
            .await?;

        Ok(Some(SessionCompactResult {
            session_id: session_id.to_string(),
            compacted: result.removed_message_count > 0,
            removed_message_count: result.removed_message_count,
            summary: result.formatted_summary,
        }))
    }

    pub(crate) async fn active_session_stats(
        &self,
        session_id: &str,
    ) -> Option<SessionStatsSnapshot> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        let session = runtime_guard.active_session_stats_session();
        let messages = &session.messages;

        let user_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::User)
            .count();
        let assistant_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::Assistant)
            .count();
        let tool_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::Tool)
            .count();

        let input: u32 = messages
            .iter()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| usage.input_tokens)
            .sum();
        let output: u32 = messages
            .iter()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| usage.output_tokens)
            .sum();

        let mut tool_usage = HashMap::new();
        for message in messages {
            if message.role == runtime::MessageRole::Assistant {
                for block in &message.blocks {
                    if let runtime::ContentBlock::ToolUse { name, .. } = block {
                        *tool_usage.entry(name.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        Some(SessionStatsSnapshot {
            session_id: session_id.to_string(),
            message_count: messages.len(),
            message_counts: SessionMessageCounts {
                user: user_count,
                assistant: assistant_count,
                tool: tool_count,
            },
            tokens: SessionTokenCounts {
                input,
                output,
                total: input + output,
            },
            tool_usage,
            duration_ms: session.updated_at_ms.saturating_sub(session.created_at_ms),
        })
    }

    pub(crate) fn last_context_envelope_nonblocking(
        &self,
        session_id: &str,
    ) -> Option<runtime::ContextEnvelope> {
        let runtime_entry = self.sessions.get(session_id)?;
        let envelope = match runtime_entry.try_lock() {
            Ok(runtime) => runtime.last_context_envelope(),
            Err(_) => {
                tracing::debug!(
                    %session_id,
                    "runtime context envelope skipped because active runtime is busy"
                );
                None
            }
        };
        envelope
    }

    pub(crate) async fn active_messages_page(
        &self,
        session_id: &str,
        offset: usize,
        from_seq: Option<usize>,
        limit: usize,
    ) -> Option<ActiveMessagesPage> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        let session = runtime_guard.session();

        let all_messages: Vec<serde_json::Value> = session
            .messages
            .iter()
            .map(|msg| {
                let role = match msg.role {
                    runtime::MessageRole::System => "system",
                    runtime::MessageRole::User => "user",
                    runtime::MessageRole::Assistant => "assistant",
                    runtime::MessageRole::Tool => "tool",
                };
                let blocks: Vec<serde_json::Value> = msg
                    .blocks
                    .iter()
                    .map(|block| match block {
                        runtime::ContentBlock::Text { text } => {
                            serde_json::json!({"type": "text", "text": text})
                        }
                        runtime::ContentBlock::Image {
                            media_type,
                            data,
                            source_path,
                        } => {
                            serde_json::json!({
                                "type": "image",
                                "media_type": media_type,
                                "source_path": source_path,
                                "size_bytes": data.len() * 3 / 4,
                            })
                        }
                        runtime::ContentBlock::Thinking {
                            thinking,
                            signature,
                        } => {
                            let mut value =
                                serde_json::json!({"type": "thinking", "thinking": thinking});
                            if let Some(signature) = signature {
                                value["signature"] =
                                    serde_json::Value::String(signature.clone());
                            }
                            value
                        }
                        runtime::ContentBlock::ToolUse { id, name, input } => {
                            serde_json::json!({"type": "tool_use", "id": id, "name": name, "input": input})
                        }
                        runtime::ContentBlock::ToolResult {
                            tool_use_id,
                            tool_name,
                            output,
                            is_error,
                        } => {
                            serde_json::json!({"type": "tool_result", "tool_use_id": tool_use_id, "tool_name": tool_name, "output": output, "is_error": is_error})
                        }
                    })
                    .collect();

                let mut value = serde_json::json!({"role": role, "blocks": blocks});
                if let Some(usage) = &msg.usage {
                    value["usage"] = serde_json::json!({
                        "input_tokens": usage.input_tokens,
                        "output_tokens": usage.output_tokens,
                        "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                        "cache_read_input_tokens": usage.cache_read_input_tokens,
                    });
                }
                value
            })
            .collect();
        let total = all_messages.len();
        let start = from_seq.unwrap_or(offset);
        let messages: Vec<serde_json::Value> =
            all_messages.into_iter().skip(start).take(limit).collect();
        let next_seq = (!messages.is_empty()).then_some(start + messages.len());
        let has_more = next_seq.map(|seq| seq < total).unwrap_or(start < total);

        Some(ActiveMessagesPage {
            session_id: session_id.to_string(),
            messages,
            total,
            offset,
            from_seq,
            next_seq,
            limit,
            has_more,
        })
    }

    pub(crate) async fn update_active_session_model(
        &self,
        session_id: &str,
        model: Option<&str>,
    ) -> bool {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return false;
        };
        let Some(model) = model else {
            return true;
        };
        let mut runtime_guard = runtime_entry.lock().await;
        runtime_guard.update_session_model(model).await;
        self.session_models
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.to_string(), model.to_string());
        true
    }

    pub(crate) async fn last_context_envelope(
        &self,
        session_id: &str,
    ) -> Option<runtime::ContextEnvelope> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.last_context_envelope()
    }

    pub(crate) async fn last_context_turn_report(
        &self,
        session_id: &str,
    ) -> Option<harness_contract::context::ContextTurnReport> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.last_context_turn_report()
    }

    pub(crate) async fn take_collaboration_result(
        &self,
        session_id: &str,
    ) -> Option<CollaborationContextResult> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.take_collaboration_result()
    }

    pub(crate) async fn snapshot(&self) -> RuntimeBoundarySnapshot {
        let mut session_ids = self.sessions.list();
        session_ids.sort();
        RuntimeBoundarySnapshot {
            protocol_version: crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            runtime_host: "gateway-runtime-host",
            active_sessions: session_ids.len(),
            uptime_secs: self.clock().uptime_secs(),
            sessions: session_ids,
        }
    }

    #[must_use]
    pub(crate) fn list_sessions_value(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "sessions": self.sessions.list(),
        })
    }

    pub(crate) async fn acquire_session_lease_value(
        &self,
        session_id: &str,
        owner: &str,
        mode: &str,
    ) -> serde_json::Value {
        self.lease_registry.acquire(session_id, owner, mode).await
    }

    pub(crate) async fn release_session_lease_value(
        &self,
        session_id: &str,
        owner: &str,
    ) -> serde_json::Value {
        self.lease_registry.release(session_id, owner).await
    }

    #[must_use]
    pub(crate) fn unsupported_protocol_value(request: &RuntimeRequest) -> serde_json::Value {
        let response = RuntimeResponse::unsupported_protocol(request);
        let message = response
            .error
            .as_ref()
            .map(|error| error.message.clone())
            .unwrap_or_else(|| "unsupported runtime protocol version".to_string());
        serde_json::json!({
            "ok": false,
            "protocol_version": crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            "request_id": response.request_id,
            "error": message,
            "error_kind": RuntimeErrorKind::UnsupportedProtocol,
            "retryable": false,
        })
    }

    fn clock(&self) -> RuntimeBoundaryClock {
        RuntimeBoundaryClock::from_uptime(self.started_at.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_max_iterations_restores_previous_runtime_budget() {
        struct BudgetProbe {
            max_iterations: usize,
        }

        impl RuntimeTurnBudgetTarget for BudgetProbe {
            fn max_iterations(&self) -> usize {
                self.max_iterations
            }

            fn set_max_iterations(&mut self, max_iterations: usize) {
                self.max_iterations = max_iterations;
            }
        }

        let mut probe = BudgetProbe { max_iterations: 64 };

        let previous = apply_scoped_max_iterations(&mut probe, Some(8));
        assert_eq!(previous, Some(64));
        assert_eq!(probe.max_iterations(), 8);

        restore_scoped_max_iterations(&mut probe, previous);
        assert_eq!(probe.max_iterations(), 64);

        let previous = apply_scoped_max_iterations(&mut probe, None);
        assert_eq!(previous, None);
        assert_eq!(probe.max_iterations(), 64);
    }

    #[tokio::test]
    async fn runtime_service_status_does_not_initialize_model_provider() {
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                Arc::new(ActiveSessions::default()),
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let value = service.status_value();
        assert_eq!(value["ok"], true);
        assert_eq!(value["runtime_host"], "gateway-runtime-host");
        let removed_legacy_key = ["dae", "mon"].concat();
        assert!(value.get(&removed_legacy_key).is_none());
        assert_eq!(value["active_sessions"], 0);
    }

    #[tokio::test]
    async fn runtime_service_snapshot_reports_lease_projection() {
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                Arc::new(ActiveSessions::default()),
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let lease = service
            .acquire_session_lease_value("session-1", "tui:test", "collaborative")
            .await;
        assert_eq!(lease["ok"], true);

        let snapshot = service.snapshot_value().await;
        assert_eq!(snapshot["kind"], "gateway_runtime_snapshot");
        assert!(snapshot.get("legacy_kind").is_none());
        let removed_legacy_key = ["dae", "mon"].concat();
        assert!(snapshot.get(&removed_legacy_key).is_none());
        assert_eq!(snapshot["leases"]["total"], 1);
        assert_eq!(snapshot["transport"]["control"], "gateway_http");
    }

    #[tokio::test]
    async fn runtime_service_records_durable_turn_journal() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let session_kernel = Arc::new(SessionKernel::new(
            Arc::new(ActiveSessions::default()),
            Some(store.clone()),
            crate::event_bus::SessionEventBus::new(),
        ));
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            session_kernel,
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let submitted = service
            .submit_turn_value(
                Some("journal-session".to_string()),
                Some("task-a".to_string()),
                "persist this turn".to_string(),
            )
            .await;

        assert_eq!(submitted["ok"], true);
        assert_eq!(submitted["durable_journal"], true);
        let events = store
            .get_events_by_type_limited("journal-session", "TurnJournal", 0, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&events[0].event_json).unwrap();
        assert_eq!(payload["event_type"], "turn.submitted");
        assert_eq!(payload["phase"], "submitted");
        assert_eq!(payload["payload"]["prompt"], "persist this turn");
        assert_eq!(payload["payload"]["task_id"], "task-a");
    }

    #[tokio::test]
    async fn runtime_service_persists_session_input_runtime_event() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let active_sessions = Arc::new(ActiveSessions::default());
        let service = RuntimeService::new(
            active_sessions.clone(),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                active_sessions,
                Some(store.clone()),
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );
        service
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                "input-session".to_string(),
                runtime::SessionInputStream::new("input-session"),
            );

        let receipt = service
            .admit_session_input(harness_contract::turn::SessionInputEnvelope::text(
                "input-session",
                harness_contract::turn::InputSourceKind::Api,
                "remember this during the current work",
            ))
            .await
            .expect("admit input");

        assert_eq!(receipt.session_id, "input-session");
        let page = store
            .runtime_events_page("input-session", 0, 10)
            .await
            .expect("runtime events page");
        let event = page
            .events
            .iter()
            .find(|event| event.kind == "SessionInputReceived")
            .expect("session input event persisted");
        assert_eq!(
            event.payload["input"]["input_id"],
            receipt.input_id.to_string()
        );
        assert_eq!(
            event.payload["record"]["envelope"]["content"],
            "remember this during the current work"
        );
        assert_eq!(event.payload["input_projection"]["total"], 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_new_session_input_materializes_real_session() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let active_sessions = Arc::new(ActiveSessions::default());
        let service = RuntimeService::new(
            active_sessions.clone(),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                active_sessions,
                Some(store.clone()),
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );
        service
            .session_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                "parent-session".to_string(),
                runtime::SessionInputStream::new("parent-session"),
            );
        service
            .session_models
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert("parent-session".to_string(), "qwen3.6-plus".to_string());

        let admission = service
            .admit_session_input_with_materialized(
                harness_contract::turn::SessionInputEnvelope::text(
                    "parent-session",
                    harness_contract::turn::InputSourceKind::Api,
                    "start a new session for independent follow-up work",
                ),
            )
            .await
            .expect("admit input");
        let receipt = admission.receipt;

        assert_eq!(
            receipt.decision,
            harness_contract::turn::InputRoutingDecision::CreateNewSession
        );
        assert_eq!(
            receipt.status,
            harness_contract::turn::SessionInputStatus::NewSessionCreated
        );
        let admission_materialized = admission
            .materialized
            .as_ref()
            .expect("admission should include materialized session payload");
        assert_eq!(
            admission_materialized["kind"],
            "session_input.create_new_session"
        );
        assert_eq!(admission_materialized["session"]["model"], "qwen3.6-plus");
        let page = store
            .runtime_events_page("parent-session", 0, 10)
            .await
            .expect("parent runtime events page");
        let session_input_event = page
            .events
            .iter()
            .find(|event| event.kind == "SessionInputReceived")
            .expect("session input event persisted");
        let materialized = &session_input_event.payload["materialized"]["session"];
        assert_eq!(materialized["created"], true);
        assert_eq!(materialized["lifecycle_snapshot"]["state"], "active");
        let new_session_id = materialized["session_id"]
            .as_str()
            .expect("materialized session id");
        assert_ne!(new_session_id, "parent-session");
        assert!(store
            .get_session(new_session_id)
            .await
            .expect("read created session")
            .is_some());
        if materialized["runtime_registered"]
            .as_bool()
            .unwrap_or(false)
        {
            assert!(service
                .sessions
                .list()
                .contains(&new_session_id.to_string()));
        }
    }

    #[test]
    fn runtime_service_rejects_unsupported_protocol_as_legacy_socket_error() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "protocol_version": 999,
            "request_id": "req-old",
            "cmd": "status",
        }))
        .expect("request parses");

        let value = RuntimeService::unsupported_protocol_value(&request);
        assert_eq!(value["ok"], false);
        assert_eq!(value["request_id"], "req-old");
        assert_eq!(value["error_kind"], "unsupported_protocol");
        assert_eq!(value["retryable"], false);
        assert!(value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("unsupported runtime protocol version"));
    }

    #[test]
    fn runtime_service_records_executing_turn_lifecycle() {
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                Arc::new(ActiveSessions::default()),
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let running = service.start_running_turn(
            Some("session-turn".to_string()),
            Some("task-turn".to_string()),
            "execute real turn".to_string(),
        );
        assert_eq!(running.status, TurnStatus::Running);
        assert_eq!(running.session_id.as_deref(), Some("session-turn"));
        assert_eq!(running.task_id.as_deref(), Some("task-turn"));

        let completed = service.finish_turn(&running.turn_id, TurnStatus::Completed, None);
        assert_eq!(completed.status, TurnStatus::Completed);
        assert!(completed.completed_at.is_some());
        assert_eq!(completed.events.len(), 2);
        assert_eq!(completed.events[0].status, TurnStatus::Running);
        assert_eq!(completed.events[1].status, TurnStatus::Completed);

        let snapshot = service.turns_value();
        assert_eq!(snapshot["turn_bindings"][0]["task_id"], "task-turn");
        assert_eq!(
            snapshot["turn_bindings"][0]["turn_id"],
            running.turn_id.to_string()
        );
        assert_eq!(snapshot["turn_bindings"][0]["session_id"], "session-turn");
    }
}
