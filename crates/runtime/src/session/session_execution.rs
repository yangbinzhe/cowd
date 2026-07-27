//! Durable session ingress and canonical SessionDispatch execution.
//!
//! Session owns accepted user messages and the ingress outbox. Runtime claims
//! those rows, compiles one canonical graph per request and acknowledges the
//! row only after the graph commit is durable. There is deliberately no
//! process-global dispatcher or graph-external session execution API.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
};
use harness_contract::turn::{
    SessionDispatchAction, SessionDispatchCommand, SessionDispatchReceipt, SessionResultPacket,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(test)]
use session::OutboxFailureClass;
#[cfg(test)]
use session::UnifiedSessionStore;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::execution_core::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};
use crate::runtime_event_store::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeTransactionEventInput,
};
use crate::{
    RuntimeSessionIngressCommand as SessionRuntimeOutboxRequest,
    RuntimeSessionInputRecord as SessionRuntimeOutboxRecord,
    RuntimeSessionInputStatus as SessionRuntimeInputStatus,
};

pub const SESSION_DISPATCH_EXECUTOR: &str = "session_dispatch";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionExecutionPolicy {
    pub max_commands: usize,
    pub dispatch_mode: SessionDispatchMode,
    pub allow_background: bool,
}

impl Default for SessionExecutionPolicy {
    fn default() -> Self {
        Self {
            max_commands: 10,
            dispatch_mode: SessionDispatchMode::StartRuntimeTurn,
            allow_background: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDispatchMode {
    MarkClaimedOnly,
    ControlDispatchComplete,
    StartRuntimeTurn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecoveryCandidate {
    pub scope: String,
    pub session_id: Option<String>,
    pub command_id: Option<String>,
    pub agent_id: Option<String>,
    pub status: String,
    pub reason: String,
    pub suggested_action: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SessionDispatchPayload {
    request_id: String,
    turn_id: String,
    message_id: String,
    session_id: String,
    sequence: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HandoffDispatchAccepted {
    handoff: harness_contract::turn::SessionHandoff,
    request_id: String,
    receipt: SessionDispatchReceipt,
    source_graph_id: String,
    source_node_id: String,
}

/// Durable handoff completion and the single source graph node that may
/// consume it. The packet is committed before this value is returned, so a
/// failed wake-up can be retried from the correlation stream without loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionHandoffResolution {
    pub packet: SessionResultPacket,
    pub source_graph_id: String,
    pub source_node_id: String,
}

fn dispatch_target_stream(request_id: &str) -> String {
    format!("session-handoff-target:{request_id}")
}

fn dispatch_correlation_stream(correlation_id: &str) -> String {
    format!("session-handoff-correlation:{correlation_id}")
}

impl SessionDispatchPayload {
    fn parse(payload_ref: &str) -> Result<Self, String> {
        let payload = payload_ref
            .strip_prefix("session_dispatch:")
            .ok_or_else(|| "SessionDispatch payload must use session_dispatch: JSON".to_string())?;
        serde_json::from_str(payload).map_err(|error| error.to_string())
    }
}

fn validate_session_dispatch_payload(payload_ref: &str) -> Result<(), String> {
    if payload_ref.starts_with("session_ingress:") {
        return payload_ref
            .strip_prefix("session_ingress:")
            .ok_or_else(|| "missing session ingress payload".to_string())
            .and_then(|payload| {
                serde_json::from_str::<crate::TurnIngressRef>(payload)
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            });
    }
    if let Some(payload) = payload_ref.strip_prefix("session_handoff:") {
        let command: SessionDispatchCommand =
            serde_json::from_str(payload).map_err(|error| error.to_string())?;
        validate_handoff_command(&command)?;
        if command.handoff.source_session_id.trim().is_empty()
            || command.handoff.target_session_id.trim().is_empty()
            || command.handoff.objective.trim().is_empty()
            || command.handoff.correlation_id.trim().is_empty()
        {
            return Err(
                "SessionDispatch requires source, target, objective, and correlation".to_string(),
            );
        }
        return Ok(());
    }
    SessionDispatchPayload::parse(payload_ref).map(|_| ())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInputRouteReceipt {
    pub request_id: String,
    pub graph_id: Option<String>,
    pub commit_cursor: Option<u64>,
    pub status: String,
    pub attempts: u32,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInputRouteReport {
    pub claimed: usize,
    pub materialized: usize,
    pub retry_scheduled: usize,
    pub blocked: usize,
    pub receipts: Vec<SessionInputRouteReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIngressExecutionReceipt {
    pub graph_id: String,
    pub commit_cursor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionExecutionFencePhase {
    ProviderRequest,
    ToolExecution,
    ToolCommit,
    TerminalCommit,
}

/// Durable ownership fence carried by one claimed Session input.
///
/// Runtime may keep rich process-local state, but it is allowed to start a
/// provider/tool side effect or commit a terminal only while this durable
/// claim remains current. Lease renewal may advance the record revision, so
/// the immutable fence is generation + claim token rather than a stale copy
/// of the revision.
#[derive(Clone)]
pub struct SessionExecutionFence {
    query: Arc<dyn crate::SessionRuntimeQueryPort>,
    request_id: String,
    session_id: String,
    generation: u64,
    input_sequence: usize,
    claim_owner: String,
    claim_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionExecutionFenceSnapshot {
    pub request_id: String,
    pub session_id: String,
    pub session_generation: u64,
    pub claim_owner: String,
    pub claim_token: String,
    pub claim_revision: u64,
}

impl std::fmt::Debug for SessionExecutionFence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionExecutionFence")
            .field("request_id", &self.request_id)
            .field("session_id", &self.session_id)
            .field("generation", &self.generation)
            .field("input_sequence", &self.input_sequence)
            .finish_non_exhaustive()
    }
}

impl SessionExecutionFence {
    pub fn from_claim(
        query: Arc<dyn crate::SessionRuntimeQueryPort>,
        request_id: impl Into<String>,
        session_id: impl Into<String>,
        generation: u64,
        input_sequence: usize,
        claim_owner: impl Into<String>,
        claim_token: impl Into<String>,
    ) -> Result<Self, String> {
        let request_id = request_id.into();
        let session_id = session_id.into();
        let claim_owner = claim_owner.into();
        let claim_token = claim_token.into();
        if request_id.trim().is_empty()
            || session_id.trim().is_empty()
            || claim_owner.trim().is_empty()
            || claim_token.trim().is_empty()
        {
            return Err(
                "Session execution fence requires request, session, owner and claim identity"
                    .to_string(),
            );
        }
        Ok(Self {
            query,
            request_id,
            session_id,
            generation,
            input_sequence,
            claim_owner,
            claim_token,
        })
    }

    pub async fn verify(
        &self,
        phase: SessionExecutionFencePhase,
    ) -> Result<SessionExecutionFenceSnapshot, String> {
        let record = self
            .query
            .runtime_input(&self.request_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                format!(
                    "Session execution fence rejected {phase:?}: request {} no longer exists",
                    self.request_id
                )
            })?;
        let claim_is_current = record.session_id == self.session_id
            && record.session_generation == self.generation
            && record.sequence == self.input_sequence
            && record.claim_owner.as_deref() == Some(self.claim_owner.as_str())
            && record.claim_token.as_deref() == Some(self.claim_token.as_str())
            && matches!(
                record.status,
                SessionRuntimeInputStatus::Claimed | SessionRuntimeInputStatus::Running
            )
            && record
                .claim_expires_at_ms
                .is_some_and(|deadline| deadline > now_ms());
        if claim_is_current {
            Ok(SessionExecutionFenceSnapshot {
                request_id: self.request_id.clone(),
                session_id: self.session_id.clone(),
                session_generation: self.generation,
                claim_owner: self.claim_owner.clone(),
                claim_token: self.claim_token.clone(),
                claim_revision: record.revision,
            })
        } else {
            Err(format!(
                "Session execution fence rejected {phase:?}: request={} session={} generation={} status={:?}",
                self.request_id,
                self.session_id,
                self.generation,
                record.status
            ))
        }
    }
}

#[async_trait]
pub trait SessionIngressExecutor: Send + Sync {
    async fn execute_ingress(
        &self,
        record: &SessionRuntimeOutboxRecord,
        content: &str,
    ) -> Result<SessionIngressExecutionReceipt, String>;
}

pub(crate) struct SessionDispatchNodeExecutor {
    router: OnceLock<Arc<SessionInputRouter>>,
}

impl SessionDispatchNodeExecutor {
    pub(crate) fn new() -> Self {
        Self {
            router: OnceLock::new(),
        }
    }

    pub(crate) fn install_router(&self, router: Arc<SessionInputRouter>) -> Result<(), String> {
        self.router
            .set(router)
            .map_err(|_| "SessionDispatch router was already installed".to_string())
    }
}

#[async_trait]
impl NodeExecutor for SessionDispatchNodeExecutor {
    fn kind(&self) -> &str {
        SESSION_DISPATCH_EXECUTOR
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.kind != ExecutionNodeKind::SessionDispatch
            || node.executor_kind != SESSION_DISPATCH_EXECUTOR
        {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "SessionDispatch must use the canonical session executor".to_string(),
            });
        }
        validate_session_dispatch_payload(&node.payload_ref).map_err(|reason| {
            NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason,
            }
        })
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        self.validate(&context.node)?;
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id.clone(),
            executor_kind: self.kind().to_string(),
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }

    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let router = self
            .router
            .get()
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: self.kind().to_string(),
                node_id: ticket.node_id.clone(),
            })?;
        let routed = router
            .route_dispatch_payload(
                &ticket.payload_ref,
                &ticket.graph_id,
                &ticket.node_id,
                &ticket.idempotency_key,
            )
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?;
        Ok(NodeExecutionOutcome {
            result: ExecutionNodeResult {
                status: if ticket.payload_ref.starts_with("session_handoff:") {
                    ExecutionNodeStatus::WaitingExternal
                } else {
                    ExecutionNodeStatus::Completed
                },
                result_ref: Some(routed.clone()),
                summary: Some("Session input was routed to the target execution graph".to_string()),
                failure: None,
                usage: Default::default(),
                evidence_refs: Vec::new(),
                finished_at_ms: now_ms(),
            },
            domain_events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("session-dispatch:{}", ticket.idempotency_key),
                    scope: RuntimeEventScope::SessionInput,
                    kind: "session.input.materialized".to_string(),
                    status: Some("completed".to_string()),
                    actor: Some("SessionDispatchNodeExecutor".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "execution_graph".to_string(),
                        id: ticket.graph_id.clone(),
                    }],
                    payload: json!({
                        "node_id": ticket.node_id,
                        "attempt": ticket.attempt,
                        "idempotency_key": ticket.idempotency_key,
                        "route_receipt": routed,
                    }),
                },
                idempotency_key: Some(ticket.idempotency_key.clone()),
                schema_version: 1,
            }],
            replan: None,
        })
    }
}

#[derive(Debug, Error)]
pub enum SessionInputRouterError {
    #[error("session ingress persistence failed: {0}")]
    Session(#[from] session::SessionError),
    #[error("session dispatch graph failed: {0}")]
    Runtime(String),
}

/// Workspace-scoped bridge from Memory ingress to the canonical graph runner.
pub struct SessionInputRouter {
    query: Arc<dyn crate::SessionRuntimeQueryPort>,
    ingress: Arc<dyn crate::SessionRuntimeIngressPort>,
    event_store: Arc<crate::RuntimeEventStore>,
    wake: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    test_store: Option<Arc<UnifiedSessionStore>>,
    #[cfg(test)]
    worker_id: String,
    #[cfg(test)]
    lease_ms: u64,
    #[cfg(test)]
    max_attempts: u32,
}

impl SessionInputRouter {
    pub fn install(
        query: Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress: Arc<dyn crate::SessionRuntimeIngressPort>,
        workspace_key: &str,
        event_store: Arc<crate::RuntimeEventStore>,
    ) -> Result<Arc<Self>, NodeExecutorError> {
        #[cfg(not(test))]
        let _ = workspace_key;
        Ok(Arc::new(Self {
            query,
            ingress,
            event_store,
            wake: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            test_store: None,
            #[cfg(test)]
            worker_id: format!("session-router:{workspace_key}"),
            #[cfg(test)]
            lease_ms: 30_000,
            #[cfg(test)]
            max_attempts: 5,
        }))
    }

    #[cfg(test)]
    pub(crate) fn install_for_test(
        query: Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress: Arc<dyn crate::SessionRuntimeIngressPort>,
        store: Arc<UnifiedSessionStore>,
        workspace_key: &str,
        event_store: Arc<crate::RuntimeEventStore>,
    ) -> Result<Arc<Self>, NodeExecutorError> {
        Ok(Arc::new(Self {
            query,
            ingress,
            event_store,
            wake: Arc::new(tokio::sync::Notify::new()),
            test_store: Some(store),
            worker_id: format!("session-router:{workspace_key}"),
            lease_ms: 30_000,
            max_attempts: 5,
        }))
    }

    #[must_use]
    pub fn wake_signal(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.wake)
    }

    pub fn notify_pending(&self) {
        self.wake.notify_one();
    }

    async fn route_dispatch_payload(
        &self,
        payload_ref: &str,
        source_graph_id: &str,
        source_node_id: &str,
        idempotency_key: &str,
    ) -> Result<String, String> {
        if let Some(payload) = payload_ref.strip_prefix("session_ingress:") {
            let ingress: crate::TurnIngressRef =
                serde_json::from_str(payload).map_err(|error| error.to_string())?;
            let record = self
                .query
                .runtime_input(&ingress.request_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!("session ingress `{}` does not exist", ingress.request_id)
                })?;
            if record.session_id != ingress.session_id
                || record.turn_id != ingress.turn_id
                || record.message_id != ingress.message_id
            {
                return Err(format!(
                    "session ingress `{}` identity does not match its durable outbox",
                    ingress.request_id
                ));
            }
            return Ok(format!("session-ingress-confirmed:{}", ingress.request_id));
        }

        if let Some(payload) = payload_ref.strip_prefix("session_handoff:") {
            let command: SessionDispatchCommand =
                serde_json::from_str(payload).map_err(|error| error.to_string())?;
            validate_handoff_command(&command)?;
            let handoff = command.handoff;
            let target = handoff.target_session_id.as_str();
            if self
                .query
                .session_record(target)
                .await
                .map_err(|error| error.to_string())?
                .is_none()
            {
                return Err(format!(
                    "SessionDispatch target session `{target}` does not exist"
                ));
            }
            let stable = stable_digest(&format!(
                "{idempotency_key}:{}:{target}:{}",
                handoff.source_session_id, handoff.correlation_id
            ));
            let admission = self
                .query
                .input_admission(target)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!("SessionDispatch target session `{target}` has no admission authority")
                })?;
            let request = SessionRuntimeOutboxRequest {
                input_id: format!("cross-session-input:{stable}"),
                request_id: format!("cross-session-request:{stable}"),
                turn_id: format!("cross-session-turn:{stable}"),
                message_id: format!("cross-session-message:{stable}"),
                session_generation: admission.generation,
                decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                target_turn_id: None,
                classification_json: Some(
                    serde_json::json!({
                        "kind": "session_handoff",
                        "correlation_id": handoff.correlation_id,
                    })
                    .to_string(),
                ),
                created_at_ms: now_ms(),
                runtime_options_json: None,
            };
            let ingress_content = handoff_ingress_content(&handoff)?;
            let record = self
                .persist_input(target, &ingress_content, &request)
                .await
                .map_err(|error| error.to_string())?;
            let receipt = SessionDispatchReceipt {
                command_id: command.command_id,
                source_node_id: source_node_id.to_string(),
                target_session_id: record.session_id,
                target_turn_id: Some(request.turn_id.clone()),
                accepted_revision: record.revision,
                status: match command.action {
                    SessionDispatchAction::Enqueue => "queued",
                    SessionDispatchAction::Interrupt => "interrupt_queued",
                    SessionDispatchAction::Cancel => "cancel_queued",
                    SessionDispatchAction::Approve => "approval_queued",
                    SessionDispatchAction::Replan => "replan_queued",
                }
                .to_string(),
                reason: None,
            };
            self.record_handoff_acceptance(
                &handoff,
                &request,
                &receipt,
                source_graph_id,
                source_node_id,
            )?;
            return serde_json::to_string(&receipt).map_err(|error| error.to_string());
        }

        let payload = SessionDispatchPayload::parse(payload_ref)?;
        let record = self
            .query
            .runtime_input(&payload.request_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("session dispatch `{}` does not exist", payload.request_id))?;
        if record.session_id != payload.session_id
            || record.turn_id != payload.turn_id
            || record.message_id != payload.message_id
            || record.sequence != payload.sequence
        {
            return Err(format!(
                "session dispatch `{}` does not match its target stream",
                payload.request_id
            ));
        }
        Ok(format!("session-dispatch-confirmed:{}", payload.request_id))
    }

    /// Returns a durable result packet only for inputs created by a typed
    /// SessionHandoff. Regular user ingress deliberately has no cross-session
    /// observer and therefore produces no packet.
    pub fn record_target_terminal(
        &self,
        record: &SessionRuntimeOutboxRecord,
        graph_id: &str,
        commit_cursor: u64,
    ) -> Result<Option<SessionHandoffResolution>, String> {
        let accepted_stream = dispatch_target_stream(&record.request_id);
        let Some(accepted) = self.event_store.latest_for_stream(&accepted_stream)? else {
            return Ok(None);
        };
        let accepted: HandoffDispatchAccepted = serde_json::from_value(accepted.payload)
            .map_err(|error| format!("invalid durable SessionHandoff acceptance: {error}"))?;
        let result_stream = dispatch_correlation_stream(&accepted.handoff.correlation_id);
        let terminal_key = format!("terminal:{commit_cursor}");
        if let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&result_stream, &terminal_key)
            .map_err(|error| error.to_string())?
        {
            let packet = serde_json::from_value(existing.payload)
                .map_err(|error| format!("invalid durable SessionResultPacket: {error}"))?;
            return Ok(Some(SessionHandoffResolution {
                packet,
                source_graph_id: accepted.source_graph_id,
                source_node_id: accepted.source_node_id,
            }));
        }
        let graph = crate::execution_core::ExecutionGraphStateStore::new(Arc::clone(
            &self.event_store,
        ))
        .load(graph_id)
        .map_err(|error| {
            format!(
                "cannot emit a SessionResultPacket without the target's durable execution graph: {error}"
            )
        })?;
        let terminal = graph
            .nodes
            .iter()
            .rev()
            .filter(|node| node.kind == ExecutionNodeKind::Synthesize)
            .filter_map(|node| graph.node_results.get(&node.id))
            .find(|result| result.status == ExecutionNodeStatus::Completed)
            .ok_or_else(|| {
                "target graph has no completed Synthesize node; refusing to fabricate a handoff result"
                    .to_string()
            })?;
        let result_ref = terminal.result_ref.clone().ok_or_else(|| {
            "target Synthesize node has no durable terminal result reference".to_string()
        })?;
        let mut evidence_refs = graph
            .node_results
            .values()
            .flat_map(|result| result.evidence_refs.iter())
            .map(|reference| reference.evidence_ref.id.clone())
            .collect::<Vec<_>>();
        evidence_refs.push(format!("runtime-commit:{commit_cursor}"));
        evidence_refs.sort();
        evidence_refs.dedup();
        let unresolved = graph
            .nodes
            .iter()
            .filter_map(|node| {
                let status = graph.node_statuses.get(&node.id).copied()?;
                (!matches!(status, ExecutionNodeStatus::Completed))
                    .then(|| format!("execution_node:{}:{status:?}", node.id))
            })
            .collect::<Vec<_>>();
        let conflict_refs = graph
            .node_results
            .values()
            .filter_map(|result| result.failure.as_ref())
            .map(|failure| format!("execution_failure:{}", failure.kind))
            .collect::<Vec<_>>();
        let (input_tokens, output_tokens) =
            graph
                .node_results
                .values()
                .fold((0_u64, 0_u64), |(input, output), result| {
                    (
                        input.saturating_add(result.usage.input_tokens),
                        output.saturating_add(result.usage.output_tokens),
                    )
                });
        let goal_id = format!("goal:{graph_id}");
        let packet = SessionResultPacket {
            correlation_id: accepted.handoff.correlation_id.clone(),
            source_session_id: accepted.handoff.source_session_id.clone(),
            target_session_id: accepted.handoff.target_session_id.clone(),
            goal_id: (self
                .event_store
                .stream_revision(&goal_id)
                .map_err(|error| error.to_string())?
                > 0)
            .then_some(goal_id),
            result_ref: Some(result_ref),
            evidence_refs,
            unresolved,
            conflict_refs,
            input_tokens,
            output_tokens,
        };
        let revision = self
            .event_store
            .stream_revision(&result_stream)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                result_stream.clone(),
                revision,
                format!(
                    "session-handoff-result:{}:{commit_cursor}",
                    packet.correlation_id
                ),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: result_stream,
                        scope: RuntimeEventScope::SessionInput,
                        kind: "session.handoff.result.v1".to_string(),
                        status: Some("completed".to_string()),
                        actor: Some("SessionInputRouter".to_string()),
                        refs: vec![RuntimeEventRef {
                            kind: "execution_graph".to_string(),
                            id: graph_id.to_string(),
                        }],
                        payload: serde_json::to_value(&packet)
                            .map_err(|error| error.to_string())?,
                    },
                    idempotency_key: Some(terminal_key),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(Some(SessionHandoffResolution {
            packet,
            source_graph_id: accepted.source_graph_id,
            source_node_id: accepted.source_node_id,
        }))
    }

    pub fn handoff_result(
        &self,
        correlation_id: &str,
    ) -> Result<Option<SessionResultPacket>, String> {
        let stream_id = dispatch_correlation_stream(correlation_id);
        self.event_store
            .latest_for_stream(&stream_id)?
            .map(|event| serde_json::from_value(event.payload).map_err(|error| error.to_string()))
            .transpose()
    }

    /// Reconstruct every completed cross-session handoff from the durable
    /// acceptance and result streams. This is intentionally event-store based:
    /// a Gateway restart must not strand a source graph after its target turn
    /// has already produced a terminal result.
    pub fn completed_handoff_resolutions(&self) -> Result<Vec<SessionHandoffResolution>, String> {
        let mut resolutions = Vec::new();
        for stream_id in self
            .event_store
            .stream_ids_for_scope(RuntimeEventScope::SessionInput)
            .map_err(|error| error.to_string())?
        {
            if !stream_id.starts_with("session-handoff-target:") {
                continue;
            }
            let Some(event) = self.event_store.latest_for_stream(&stream_id)? else {
                continue;
            };
            if event.kind != "session.handoff.accepted.v1" {
                continue;
            }
            let accepted: HandoffDispatchAccepted = serde_json::from_value(event.payload)
                .map_err(|error| format!("invalid durable SessionHandoff acceptance: {error}"))?;
            let Some(packet) = self.handoff_result(&accepted.handoff.correlation_id)? else {
                continue;
            };
            resolutions.push(SessionHandoffResolution {
                packet,
                source_graph_id: accepted.source_graph_id,
                source_node_id: accepted.source_node_id,
            });
        }
        resolutions
            .sort_by(|left, right| left.packet.correlation_id.cmp(&right.packet.correlation_id));
        Ok(resolutions)
    }

    fn record_handoff_acceptance(
        &self,
        handoff: &harness_contract::turn::SessionHandoff,
        request: &SessionRuntimeOutboxRequest,
        receipt: &SessionDispatchReceipt,
        source_graph_id: &str,
        source_node_id: &str,
    ) -> Result<(), String> {
        let stream_id = dispatch_target_stream(&request.request_id);
        if self
            .event_store
            .event_by_idempotency_key(&stream_id, &receipt.command_id)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        let accepted = HandoffDispatchAccepted {
            handoff: handoff.clone(),
            request_id: request.request_id.clone(),
            receipt: receipt.clone(),
            source_graph_id: source_graph_id.to_string(),
            source_node_id: source_node_id.to_string(),
        };
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("session-handoff-accept:{}", receipt.command_id),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
                        scope: RuntimeEventScope::SessionInput,
                        kind: "session.handoff.accepted.v1".to_string(),
                        status: Some(receipt.status.clone()),
                        actor: Some("SessionDispatchNodeExecutor".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::to_value(accepted)
                            .map_err(|error| error.to_string())?,
                    },
                    idempotency_key: Some(receipt.command_id.clone()),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub async fn persist_input(
        &self,
        session_id: &str,
        content: &str,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord, SessionInputRouterError> {
        let content_json = serde_json::to_string(&json!([{
            "type": "text",
            "text": content,
            "cowd_turn_id": request.turn_id,
            "cowd_turn_ingress_message_id": request.message_id,
        }]))
        .map_err(|error| SessionInputRouterError::Runtime(error.to_string()))?;
        let record = self
            .ingress
            .append_ingress(
                session_id,
                "user",
                Some(&content_json),
                request.created_at_ms,
                request,
            )
            .await
            .map_err(SessionInputRouterError::from)?;
        self.notify_pending();
        Ok(record)
    }

    #[cfg(test)]
    pub async fn route_pending_with(
        &self,
        executor: &dyn SessionIngressExecutor,
        limit: usize,
    ) -> Result<SessionInputRouteReport, SessionInputRouterError> {
        let store = self.test_store.as_ref().ok_or_else(|| {
            SessionInputRouterError::Runtime(
                "route_pending_with requires the test Session store adapter".to_string(),
            )
        })?;
        let now = now_ms();
        let claimed = self
            .test_store
            .as_ref()
            .expect("test store checked")
            .claim_session_runtime_outbox(&self.worker_id, now, self.lease_ms, limit)
            .await?;
        let mut report = SessionInputRouteReport {
            claimed: claimed.len(),
            ..Default::default()
        };
        for record in claimed {
            let claim_token = record.claim_token.clone().ok_or_else(|| {
                SessionInputRouterError::Runtime(format!(
                    "claimed ingress {} has no claim token",
                    record.request_id
                ))
            })?;
            let running = self
                .test_store
                .as_ref()
                .expect("test store checked")
                .mark_session_runtime_outbox_running(
                    &record.request_id,
                    &self.worker_id,
                    record.session_generation,
                    &claim_token,
                    record.revision,
                    now_ms(),
                )
                .await?;
            let mut claim_revision = running.revision;
            let content = self
                .test_store
                .as_ref()
                .expect("test store checked")
                .get_messages_from_sequence(&record.session_id, record.sequence, 1)
                .await
                .ok()
                .and_then(|messages| messages.into_iter().next())
                .map(|message| message.content_json)
                .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
                .and_then(|value| value.as_array().cloned())
                .and_then(|blocks| {
                    blocks.into_iter().find_map(|block| {
                        block
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string)
                    })
                });
            let outcome = match content {
                Some(content) => {
                    let runtime_record =
                        crate::session_runtime_port::to_runtime_input_record(running.clone());
                    let execution = executor.execute_ingress(&runtime_record, &content);
                    tokio::pin!(execution);
                    let heartbeat_ms = (self.lease_ms / 3).max(1);
                    loop {
                        tokio::select! {
                            outcome = &mut execution => break outcome,
                            _ = tokio::time::sleep(std::time::Duration::from_millis(heartbeat_ms)) => {
                                match store.renew_session_runtime_outbox_lease(
                                    &record.request_id,
                                    &self.worker_id,
                                    record.session_generation,
                                    &claim_token,
                                    claim_revision,
                                    now_ms(),
                                    self.lease_ms,
                                ).await {
                                    Ok(renewed) => claim_revision = renewed.revision,
                                    Err(error) => {
                                        tracing::warn!(request_id = %record.request_id, %error, "session ingress lease heartbeat lost ownership");
                                    }
                                }
                            }
                        }
                    }
                }
                None => Err("session ingress message payload is missing or corrupt".to_string()),
            };
            let receipt = match outcome {
                Ok(executed) => match self
                    .test_store
                    .as_ref()
                    .expect("test store checked")
                    .ack_session_runtime_outbox(
                        &record.request_id,
                        &self.worker_id,
                        record.session_generation,
                        &claim_token,
                        claim_revision,
                        session::SessionRuntimeInputStatus::Completed,
                        executed.commit_cursor,
                        now_ms(),
                    )
                    .await
                {
                    Ok(acked) => SessionInputRouteReceipt {
                        request_id: record.request_id,
                        graph_id: Some(executed.graph_id),
                        commit_cursor: acked.runtime_commit_cursor,
                        status: "materialized".to_string(),
                        attempts: acked.attempts,
                        error: None,
                    },
                    Err(error) => SessionInputRouteReceipt {
                        request_id: record.request_id,
                        graph_id: Some(executed.graph_id),
                        commit_cursor: Some(executed.commit_cursor),
                        status: "blocked_ack".to_string(),
                        attempts: record.attempts,
                        error: Some(error.to_string()),
                    },
                },
                Err(error) => {
                    let class = classify_router_failure(&error);
                    let failed = self
                        .test_store
                        .as_ref()
                        .expect("test store checked")
                        .fail_session_runtime_outbox(
                            &record.request_id,
                            &self.worker_id,
                            record.session_generation,
                            &claim_token,
                            claim_revision,
                            class,
                            &error,
                            now.saturating_add(retry_delay_ms(record.attempts)),
                            self.max_attempts,
                            now_ms(),
                        )
                        .await;
                    SessionInputRouteReceipt {
                        request_id: record.request_id,
                        graph_id: None,
                        commit_cursor: None,
                        status: failed
                            .ok()
                            .map_or("blocked_failure_record", |item| {
                                if item.status == session::SessionRuntimeInputStatus::Queued {
                                    "retry_scheduled"
                                } else {
                                    "blocked"
                                }
                            })
                            .to_string(),
                        attempts: record.attempts,
                        error: Some(error),
                    }
                }
            };
            report.receipts.push(receipt);
        }
        for receipt in &report.receipts {
            match receipt.status.as_str() {
                "materialized" => report.materialized += 1,
                "retry_scheduled" => report.retry_scheduled += 1,
                _ => report.blocked += 1,
            }
        }
        Ok(report)
    }
}

/// A handoff creates a new target input rather than mutating an existing
/// target turn. Its expected revision is therefore intentionally fixed at
/// zero. Controls for an existing target turn (cancel/approval) go through
/// MissionCommand, where the owning aggregate has a real revision to check.
fn validate_handoff_command(command: &SessionDispatchCommand) -> Result<(), String> {
    if command.expected_target_revision != 0 {
        return Err(
            "new SessionHandoff requires expected_target_revision=0; existing target control must use MissionCommand"
                .to_string(),
        );
    }
    if !matches!(
        command.action,
        SessionDispatchAction::Enqueue
            | SessionDispatchAction::Interrupt
            | SessionDispatchAction::Replan
    ) {
        return Err(
            "SessionHandoff only transports enqueue, interrupt, or replan input; cancel and approval use MissionCommand"
                .to_string(),
        );
    }
    for reference in &command.handoff.evidence_refs {
        if reference.is_durable() {
            let source_scope = format!("session:{}", command.handoff.source_session_id);
            if reference.visibility_scope != source_scope
                && !reference.visibility_scope.starts_with("shared:")
            {
                return Err(
                    "durable handoff evidence must be source-session scoped or explicitly shared"
                        .to_string(),
                );
            }
            if reference.sha256.trim().is_empty()
                || reference.bytes == 0
                || reference.retrieval_selector.trim().is_empty()
            {
                return Err(
                    "durable handoff evidence requires hash, byte count, and retrieval selector"
                        .to_string(),
                );
            }
        } else if !reference.retrieval_selector.trim().is_empty()
            || !reference.sha256.trim().is_empty()
            || reference.bytes != 0
        {
            return Err(
                "unavailable handoff evidence must not carry a retrievable raw selector"
                    .to_string(),
            );
        }
    }
    if let Some(lease) = &command.handoff.context_budget_lease {
        if lease.owner_id != command.handoff.source_session_id
            || lease.scope.trim().is_empty()
            || lease.max_tokens == 0
            || lease.consumed_tokens > lease.max_tokens
        {
            return Err(
                "handoff context budget lease must belong to the source session and be valid"
                    .to_string(),
            );
        }
    }
    Ok(())
}

fn handoff_ingress_content(
    handoff: &harness_contract::turn::SessionHandoff,
) -> Result<String, String> {
    let evidence = serde_json::to_string(&handoff.evidence_refs)
        .map_err(|error| format!("serialize typed handoff evidence: {error}"))?;
    let lease = serde_json::to_string(&handoff.context_budget_lease)
        .map_err(|error| format!("serialize handoff budget lease: {error}"))?;
    Ok(format!(
        "{}\n\n## Cross-session handoff\nSource session: {}\nScope: {}\nAcceptance: {}\nEvidence references (metadata only; do not assume access): {}\nContext budget lease: {}",
        handoff.objective,
        handoff.source_session_id,
        handoff.scope.join(", "),
        handoff.acceptance.join("; "),
        evidence,
        lease,
    ))
}

#[must_use]
pub fn session_ingress_graph_id(session_id: &str, request_id: &str, turn_id: &str) -> String {
    format!(
        "session-ingress-graph:{}",
        stable_digest(&format!("{session_id}:{request_id}:{turn_id}"))
    )
}

fn stable_digest(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
fn classify_router_failure(error: &str) -> OutboxFailureClass {
    if error.contains("authorization") || error.contains("approval") {
        OutboxFailureClass::AuthorizationBlocked
    } else if error.contains("payload") || error.contains("JSON") {
        OutboxFailureClass::CorruptPayload
    } else if error.contains("invalid") || error.contains("unavailable until") {
        OutboxFailureClass::Permanent
    } else {
        OutboxFailureClass::Retryable
    }
}

#[cfg(test)]
fn retry_delay_ms(attempt: u32) -> u64 {
    250_u64.saturating_mul(1_u64 << attempt.min(8))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeServices;
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeStatus};
    use session::SessionRecord;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Duration;

    async fn fixture() -> (
        Arc<UnifiedSessionStore>,
        Arc<RuntimeServices>,
        Arc<SessionInputRouter>,
    ) {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "chat".to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let services = RuntimeServices::in_memory().unwrap();
        let router = services
            .install_test_session_store(Arc::clone(&store))
            .unwrap();
        (store, services, router)
    }

    #[tokio::test]
    async fn ingress_is_durable_and_exactly_once_across_restart_claims() {
        let (store, _services, router) = fixture().await;
        let request = SessionRuntimeOutboxRequest {
            input_id: "i1".to_string(),
            request_id: "r1".to_string(),
            turn_id: "t1".to_string(),
            message_id: "m1".to_string(),
            session_generation: 1,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            created_at_ms: 1,
            runtime_options_json: None,
        };
        router.persist_input("s1", "hello", &request).await.unwrap();
        router.persist_input("s1", "hello", &request).await.unwrap();
        struct Executor;
        #[async_trait]
        impl SessionIngressExecutor for Executor {
            async fn execute_ingress(
                &self,
                _record: &SessionRuntimeOutboxRecord,
                _content: &str,
            ) -> Result<SessionIngressExecutionReceipt, String> {
                Ok(SessionIngressExecutionReceipt {
                    graph_id: "graph-r1".to_string(),
                    commit_cursor: 42,
                })
            }
        }
        let report = router.route_pending_with(&Executor, 8).await.unwrap();
        assert_eq!(report.materialized, 1);
        let stored = store
            .get_session_runtime_outbox("r1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, session::SessionRuntimeInputStatus::Completed);
        assert_eq!(stored.runtime_commit_cursor, Some(42));
        assert!(router
            .route_pending_with(&Executor, 8)
            .await
            .unwrap()
            .receipts
            .is_empty());
    }

    #[test]
    fn failure_classes_and_backoff_are_deterministic() {
        assert_eq!(
            classify_router_failure("authorization denied"),
            OutboxFailureClass::AuthorizationBlocked
        );
        assert_eq!(
            classify_router_failure("bad JSON payload"),
            OutboxFailureClass::CorruptPayload
        );
        assert_eq!(
            classify_router_failure("executor unavailable until V8"),
            OutboxFailureClass::Permanent
        );
        assert_eq!(
            classify_router_failure("database busy"),
            OutboxFailureClass::Retryable
        );
        assert!(retry_delay_ms(5) > retry_delay_ms(1));
    }

    struct SlowCountingExecutor {
        calls: AtomicUsize,
        delay: Duration,
    }

    #[async_trait]
    impl SessionIngressExecutor for SlowCountingExecutor {
        async fn execute_ingress(
            &self,
            record: &SessionRuntimeOutboxRecord,
            _content: &str,
        ) -> Result<SessionIngressExecutionReceipt, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(SessionIngressExecutionReceipt {
                graph_id: session_ingress_graph_id(
                    &record.session_id,
                    &record.request_id,
                    &record.turn_id,
                ),
                commit_cursor: 7,
            })
        }
    }

    #[tokio::test]
    async fn heartbeat_prevents_second_worker_reclaim_during_long_execution() {
        let (store, services, _router) = fixture().await;
        let request = SessionRuntimeOutboxRequest {
            input_id: "long-i1".into(),
            request_id: "long-r1".into(),
            turn_id: "long-t1".into(),
            message_id: "long-m1".into(),
            session_generation: 1,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            created_at_ms: now_ms(),
            runtime_options_json: None,
        };
        let ports_a = crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
        let ports_b = crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
        let router_a = Arc::new(SessionInputRouter {
            query: ports_a.clone(),
            ingress: ports_a,
            event_store: Arc::clone(services.event_store()),
            wake: Arc::new(tokio::sync::Notify::new()),
            test_store: Some(Arc::clone(&store)),
            worker_id: "worker-a".into(),
            lease_ms: 30,
            max_attempts: 3,
        });
        let router_b = SessionInputRouter {
            query: ports_b.clone(),
            ingress: ports_b,
            event_store: Arc::clone(services.event_store()),
            wake: Arc::new(tokio::sync::Notify::new()),
            test_store: Some(Arc::clone(&store)),
            worker_id: "worker-b".into(),
            lease_ms: 30,
            max_attempts: 3,
        };
        router_a
            .persist_input("s1", "slow", &request)
            .await
            .unwrap();
        let executor = Arc::new(SlowCountingExecutor {
            calls: AtomicUsize::new(0),
            delay: Duration::from_millis(120),
        });
        let task = {
            let router = Arc::clone(&router_a);
            let executor = Arc::clone(&executor);
            tokio::spawn(async move { router.route_pending_with(executor.as_ref(), 1).await })
        };
        tokio::time::sleep(Duration::from_millis(75)).await;
        let second = router_b
            .route_pending_with(executor.as_ref(), 1)
            .await
            .unwrap();
        assert_eq!(second.claimed, 0, "renewed lease must not be reclaimed");
        assert_eq!(task.await.unwrap().unwrap().materialized, 1);
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ack_failure_reclaim_reuses_committed_execution_without_repeating_side_effect() {
        struct CommittedReceiptExecutor {
            store: Arc<UnifiedSessionStore>,
            executions: AtomicUsize,
            side_effects: AtomicUsize,
            committed: std::sync::Mutex<std::collections::BTreeSet<String>>,
        }
        #[async_trait]
        impl SessionIngressExecutor for CommittedReceiptExecutor {
            async fn execute_ingress(
                &self,
                record: &SessionRuntimeOutboxRecord,
                _content: &str,
            ) -> Result<SessionIngressExecutionReceipt, String> {
                self.executions.fetch_add(1, Ordering::SeqCst);
                if self
                    .committed
                    .lock()
                    .unwrap()
                    .insert(record.request_id.clone())
                {
                    self.side_effects.fetch_add(1, Ordering::SeqCst);
                    // Simulate a successful Runtime commit followed by an
                    // ownership revision change before the router can ack.
                    self.store
                        .renew_session_runtime_outbox_lease(
                            &record.request_id,
                            "worker-a",
                            record.session_generation,
                            record.claim_token.as_deref().expect("claim token"),
                            record.revision,
                            now_ms(),
                            5,
                        )
                        .await
                        .unwrap();
                }
                Ok(SessionIngressExecutionReceipt {
                    graph_id: session_ingress_graph_id(
                        &record.session_id,
                        &record.request_id,
                        &record.turn_id,
                    ),
                    commit_cursor: 91,
                })
            }
        }

        let (store, services, _router) = fixture().await;
        let request = SessionRuntimeOutboxRequest {
            input_id: "ack-loss-i1".into(),
            request_id: "ack-loss-r1".into(),
            turn_id: "ack-loss-t1".into(),
            message_id: "ack-loss-m1".into(),
            session_generation: 1,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            created_at_ms: now_ms(),
            runtime_options_json: None,
        };
        let ports_a = crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
        let ports_b = crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
        let router_a = SessionInputRouter {
            query: ports_a.clone(),
            ingress: ports_a,
            event_store: Arc::clone(services.event_store()),
            wake: Arc::new(tokio::sync::Notify::new()),
            test_store: Some(Arc::clone(&store)),
            worker_id: "worker-a".into(),
            // Keep the router heartbeat out of the fault-injection window. The
            // executor below shortens the lease to 5 ms after its durable
            // commit, which deterministically simulates ownership changing
            // before the router can acknowledge the receipt.
            lease_ms: 100,
            max_attempts: 3,
        };
        let router_b = SessionInputRouter {
            query: ports_b.clone(),
            ingress: ports_b,
            event_store: Arc::clone(services.event_store()),
            wake: Arc::new(tokio::sync::Notify::new()),
            test_store: Some(Arc::clone(&store)),
            worker_id: "worker-b".into(),
            lease_ms: 20,
            max_attempts: 3,
        };
        router_a
            .persist_input("s1", "once", &request)
            .await
            .unwrap();
        let executor = CommittedReceiptExecutor {
            store: Arc::clone(&store),
            executions: AtomicUsize::new(0),
            side_effects: AtomicUsize::new(0),
            committed: Default::default(),
        };
        let first = router_a.route_pending_with(&executor, 1).await.unwrap();
        assert_eq!(first.receipts[0].status, "blocked_ack");
        tokio::time::sleep(Duration::from_millis(8)).await;
        let second = router_b.route_pending_with(&executor, 1).await.unwrap();
        assert_eq!(second.materialized, 1);
        assert_eq!(executor.executions.load(Ordering::SeqCst), 2);
        assert_eq!(executor.side_effects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn session_dispatch_routes_into_the_real_target_session_stream_without_fabricating_a_result(
    ) {
        let (store, services, router) = fixture().await;
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&SessionRecord {
                session_id: "s2".into(),
                platform: "test".into(),
                chat_id: "chat-2".into(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".into(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".into(),
            })
            .await
            .unwrap();
        let handoff = harness_contract::turn::SessionHandoff {
            handoff_id: "handoff-test".into(),
            source_session_id: "s1".into(),
            target_session_id: "s2".into(),
            objective: "review the active change".into(),
            acceptance: vec![],
            scope: vec![],
            context_lens: vec![],
            evidence_refs: vec![],
            context_budget_lease: None,
            permission_lease: "test".into(),
            deadline_at_ms: None,
            priority: 128,
            correlation_id: "correlation-test".into(),
            result_contract: "return result".into(),
        };
        let command = harness_contract::turn::SessionDispatchCommand {
            command_id: "dispatch-test".into(),
            action: harness_contract::turn::SessionDispatchAction::Enqueue,
            handoff,
            expected_target_revision: 0,
        };
        let mut graph = ExecutionGraph::new("cross-session dispatch");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::SessionDispatch,
            SESSION_DISPATCH_EXECUTOR,
            format!(
                "session_handoff:{}",
                serde_json::to_string(&command).unwrap()
            ),
        );
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        graph.nodes.push(node);
        let (_, report) = services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(report.failed, 0);
        assert_eq!(report.completed, 0);
        assert_eq!(report.waiting, 1);
        let claimed = store
            .claim_session_runtime_outbox("target-worker", now_ms(), 1_000, 8)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].session_id, "s2");
        let messages = store.get_messages_from_sequence("s2", 0, 8).await.unwrap();
        assert!(messages[0]
            .content_json
            .contains("review the active change"));
        let error = router
            .record_target_terminal(
                &crate::session_runtime_port::to_runtime_input_record(claimed[0].clone()),
                "target-graph",
                99,
            )
            .expect_err("a missing target graph must not become a fake successful handoff");
        assert!(error.contains("without the target's durable execution graph"));
        assert!(router
            .handoff_result("correlation-test")
            .expect("read result")
            .is_none());
    }

    #[test]
    fn handoff_rejects_controls_that_need_an_existing_target_owner() {
        let command = harness_contract::turn::SessionDispatchCommand {
            command_id: "invalid-handoff-control".to_string(),
            action: harness_contract::turn::SessionDispatchAction::Cancel,
            handoff: harness_contract::turn::SessionHandoff {
                handoff_id: "handoff".to_string(),
                source_session_id: "source".to_string(),
                target_session_id: "target".to_string(),
                objective: "do not execute".to_string(),
                acceptance: Vec::new(),
                scope: Vec::new(),
                context_lens: Vec::new(),
                evidence_refs: Vec::new(),
                context_budget_lease: None,
                permission_lease: "test".to_string(),
                deadline_at_ms: None,
                priority: 1,
                correlation_id: "correlation".to_string(),
                result_contract: "result".to_string(),
            },
            expected_target_revision: 0,
        };
        let error = validate_handoff_command(&command).expect_err("invalid handoff control");
        assert!(error.contains("cancel and approval use MissionCommand"));
    }

    #[test]
    fn handoff_contract_rejects_unscoped_raw_and_accepts_source_budget_lease() {
        let mut command = harness_contract::turn::SessionDispatchCommand {
            command_id: "typed-handoff".to_string(),
            action: harness_contract::turn::SessionDispatchAction::Enqueue,
            handoff: harness_contract::turn::SessionHandoff {
                handoff_id: "typed-handoff".to_string(),
                source_session_id: "source".to_string(),
                target_session_id: "target".to_string(),
                objective: "review evidence".to_string(),
                acceptance: Vec::new(),
                scope: Vec::new(),
                context_lens: Vec::new(),
                evidence_refs: vec![harness_contract::context::EvidenceAccessRef::durable(
                    harness_contract::reality::EvidenceRef::new("tool", "raw-1"),
                    "sha256:abc",
                    3,
                    "text/plain",
                    "artifact://art_session_source_1",
                    "session:other",
                )],
                context_budget_lease: Some(
                    harness_contract::context::ContextBudgetLeaseRef::new(
                        "lease-1",
                        "source",
                        "session_handoff",
                        1200,
                        1,
                    )
                    .with_consumed_tokens(400),
                ),
                permission_lease: "read_only".to_string(),
                deadline_at_ms: None,
                priority: 1,
                correlation_id: "typed-correlation".to_string(),
                result_contract: "return result".to_string(),
            },
            expected_target_revision: 0,
        };
        assert!(validate_handoff_command(&command).is_err());

        command.handoff.evidence_refs[0].visibility_scope = "session:source".to_string();
        assert!(validate_handoff_command(&command).is_ok());
        let ingress = handoff_ingress_content(&command.handoff).expect("typed ingress");
        assert!(ingress.contains("Cross-session handoff"));
        assert!(ingress.contains("sha256:abc"));
        assert!(!ingress.contains("raw payload"));
    }

    struct TerminalSynthesizeBackend;

    struct TerminalSynthesizeResolver {
        graph_id: String,
    }

    impl crate::execution_core::graph::executors::SynthesizeBackendResolver
        for TerminalSynthesizeResolver
    {
        fn resolve(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Option<Arc<dyn crate::execution_core::graph::executors::SynthesizeBackend>> {
            (ticket.graph_id == self.graph_id).then(|| {
                Arc::new(TerminalSynthesizeBackend)
                    as Arc<dyn crate::execution_core::graph::executors::SynthesizeBackend>
            })
        }
    }

    #[async_trait]
    impl crate::execution_core::graph::executors::SynthesizeBackend for TerminalSynthesizeBackend {
        async fn synthesize(
            &self,
            ticket: &NodeExecutionTicket,
        ) -> Result<NodeExecutionOutcome, String> {
            Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some(format!("durable-target-result:{}", ticket.graph_id)),
                summary: Some("Durable target graph completed".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: harness_contract::execution_graph::ExecutionUsage {
                    input_tokens: 13,
                    output_tokens: 29,
                    ..Default::default()
                },
                finished_at_ms: now_ms(),
            }))
        }
    }

    #[tokio::test]
    async fn handoff_result_uses_the_completed_target_graph_result_and_usage() {
        let (store, services, router) = fixture().await;
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&SessionRecord {
                session_id: "target-session".into(),
                platform: "test".into(),
                chat_id: "target-chat".into(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".into(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".into(),
            })
            .await
            .unwrap();
        let handoff = harness_contract::turn::SessionHandoff {
            handoff_id: "handoff-real-terminal".into(),
            source_session_id: "s1".into(),
            target_session_id: "target-session".into(),
            objective: "complete target task".into(),
            acceptance: Vec::new(),
            scope: Vec::new(),
            context_lens: Vec::new(),
            evidence_refs: vec![harness_contract::turn::opaque_session_evidence_ref(
                "s1",
                "evidence:handoff",
            )],
            context_budget_lease: None,
            permission_lease: "test".into(),
            deadline_at_ms: None,
            priority: 128,
            correlation_id: "correlation-real-terminal".into(),
            result_contract: "return durable synthesis".into(),
        };
        let command = harness_contract::turn::SessionDispatchCommand {
            command_id: "dispatch-real-terminal".into(),
            action: harness_contract::turn::SessionDispatchAction::Enqueue,
            handoff,
            expected_target_revision: 0,
        };
        let mut source = ExecutionGraph::new("source dispatch");
        let source_node = ExecutionNodeSpec::new(
            ExecutionNodeKind::SessionDispatch,
            SESSION_DISPATCH_EXECUTOR,
            format!(
                "session_handoff:{}",
                serde_json::to_string(&command).unwrap()
            ),
        );
        source
            .node_statuses
            .insert(source_node.id.clone(), ExecutionNodeStatus::Planned);
        source.nodes.push(source_node);
        services
            .execution_supervisor()
            .submit_and_wait(
                source,
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("target-worker", now_ms(), 1_000, 8)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        let mut target = ExecutionGraph::new("target execution");
        target.id = "target-real-graph".to_string();
        let terminal = ExecutionNodeSpec::new(
            ExecutionNodeKind::Synthesize,
            crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
            "target terminal payload",
        );
        target.nodes.push(terminal);
        services
            .synthesize_executor()
            .install_resolver(Arc::new(TerminalSynthesizeResolver {
                graph_id: target.id.clone(),
            }));
        let (_, report) = services
            .execution_supervisor()
            .submit_and_wait(
                target.clone(),
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(report.completed, 1);
        let resolution = router
            .record_target_terminal(
                &crate::session_runtime_port::to_runtime_input_record(claimed[0].clone()),
                &target.id,
                777,
            )
            .expect("durable target result")
            .expect("cross-session resolution");
        assert_eq!(
            resolution.packet.result_ref.as_deref(),
            Some("durable-target-result:target-real-graph")
        );
        assert_eq!(resolution.packet.input_tokens, 13);
        assert_eq!(resolution.packet.output_tokens, 29);
        assert!(resolution
            .packet
            .evidence_refs
            .contains(&"runtime-commit:777".to_string()));
    }
}
