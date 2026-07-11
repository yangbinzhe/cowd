//! Durable session ingress and canonical SessionDispatch execution.
//!
//! Memory owns accepted user messages and the ingress outbox. Runtime claims
//! those rows, compiles one canonical graph per request and acknowledges the
//! row only after the graph commit is durable. There is deliberately no
//! process-global dispatcher or graph-external session execution API.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus,
};
use memory::{
    OutboxFailureClass, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    UnifiedSessionStore,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::execution_core::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};
use crate::runtime_event_store::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeTransactionEventInput,
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
pub struct CrossSessionMessage {
    pub from_session_id: String,
    pub target_ref: String,
    pub command: String,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
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
    if let Some(payload) = payload_ref.strip_prefix("session_input:") {
        let message: CrossSessionMessage =
            serde_json::from_str(payload).map_err(|error| error.to_string())?;
        if message.from_session_id.trim().is_empty()
            || message.target_ref.trim().trim_start_matches('@').is_empty()
            || message.command.trim().is_empty()
        {
            return Err(
                "SessionDispatch requires source session, target stream and command".to_string(),
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
            .route_dispatch_payload(&ticket.payload_ref, &ticket.idempotency_key)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?;
        Ok(NodeExecutionOutcome {
            result: ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some(routed.clone()),
                failure: None,
                usage: Default::default(),
                evidence_refs: Vec::new(),
                finished_at_ms: now_ms(),
            },
            domain_events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("session-dispatch:{}", ticket.idempotency_key),
                    scope: RuntimeEventScope::SessionCommand,
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
    Memory(#[from] memory::MemoryError),
    #[error("session dispatch graph failed: {0}")]
    Runtime(String),
}

/// Workspace-scoped bridge from Memory ingress to the canonical graph runner.
pub struct SessionInputRouter {
    store: Arc<UnifiedSessionStore>,
    worker_id: String,
    lease_ms: u64,
    max_attempts: u32,
}

impl SessionInputRouter {
    pub fn install(
        store: Arc<UnifiedSessionStore>,
        workspace_key: &str,
    ) -> Result<Arc<Self>, NodeExecutorError> {
        Ok(Arc::new(Self {
            store,
            worker_id: format!("session-router:{workspace_key}"),
            lease_ms: 30_000,
            max_attempts: 5,
        }))
    }

    async fn route_dispatch_payload(
        &self,
        payload_ref: &str,
        idempotency_key: &str,
    ) -> Result<String, String> {
        if let Some(payload) = payload_ref.strip_prefix("session_ingress:") {
            let ingress: crate::TurnIngressRef =
                serde_json::from_str(payload).map_err(|error| error.to_string())?;
            let record = self
                .store
                .get_session_runtime_outbox(&ingress.request_id)
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

        if let Some(payload) = payload_ref.strip_prefix("session_input:") {
            let message: CrossSessionMessage =
                serde_json::from_str(payload).map_err(|error| error.to_string())?;
            let target = message.target_ref.trim().trim_start_matches('@');
            if target.is_empty() {
                return Err("SessionDispatch target session is empty".to_string());
            }
            if self
                .store
                .get_session(target)
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
                message.from_session_id, message.command
            ));
            let request = SessionRuntimeOutboxRequest {
                request_id: format!("cross-session-request:{stable}"),
                turn_id: format!("cross-session-turn:{stable}"),
                message_id: format!("cross-session-message:{stable}"),
                created_at_ms: now_ms(),
            };
            let record = self
                .persist_input(target, &message.command, &request)
                .await
                .map_err(|error| error.to_string())?;
            return Ok(format!(
                "session-routed:{}:{}:{}",
                record.session_id, record.request_id, record.sequence
            ));
        }

        let payload = SessionDispatchPayload::parse(payload_ref)?;
        let record = self
            .store
            .get_session_runtime_outbox(&payload.request_id)
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

    pub async fn persist_input(
        &self,
        session_id: &str,
        content: &str,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord, SessionInputRouterError> {
        let content_json = serde_json::to_string(&json!([{ "type": "text", "text": content }]))
            .map_err(|error| SessionInputRouterError::Runtime(error.to_string()))?;
        self.store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(&content_json),
                request.created_at_ms,
                request,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn route_pending_with(
        &self,
        executor: &dyn SessionIngressExecutor,
        limit: usize,
    ) -> Result<SessionInputRouteReport, SessionInputRouterError> {
        let now = now_ms();
        let claimed = self
            .store
            .claim_session_runtime_outbox(&self.worker_id, now, self.lease_ms, limit)
            .await?;
        let mut report = SessionInputRouteReport {
            claimed: claimed.len(),
            ..Default::default()
        };
        for record in claimed {
            let mut claim_revision = record.revision;
            let content = self
                .store
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
                    let execution = executor.execute_ingress(&record, &content);
                    tokio::pin!(execution);
                    let heartbeat_ms = (self.lease_ms / 3).max(1);
                    loop {
                        tokio::select! {
                            outcome = &mut execution => break outcome,
                            _ = tokio::time::sleep(std::time::Duration::from_millis(heartbeat_ms)) => {
                                match self.store.renew_session_runtime_outbox_lease(
                                    &record.request_id,
                                    &self.worker_id,
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
                    .store
                    .ack_session_runtime_outbox(
                        &record.request_id,
                        &self.worker_id,
                        claim_revision,
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
                        .store
                        .fail_session_runtime_outbox(
                            &record.request_id,
                            &self.worker_id,
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
                                if item.status == memory::OutboxStatus::RetryScheduled {
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
    use memory::{SessionRecord, SessionRuntimeOutboxRequest};
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
        let router = services.install_session_store(Arc::clone(&store)).unwrap();
        (store, services, router)
    }

    #[tokio::test]
    async fn ingress_is_durable_and_exactly_once_across_restart_claims() {
        let (store, _services, router) = fixture().await;
        let request = SessionRuntimeOutboxRequest {
            request_id: "r1".to_string(),
            turn_id: "t1".to_string(),
            message_id: "m1".to_string(),
            created_at_ms: 1,
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
        assert_eq!(stored.status, memory::OutboxStatus::Materialized);
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
        let (store, _services, _router) = fixture().await;
        let request = SessionRuntimeOutboxRequest {
            request_id: "long-r1".into(),
            turn_id: "long-t1".into(),
            message_id: "long-m1".into(),
            created_at_ms: now_ms(),
        };
        let router_a = Arc::new(SessionInputRouter {
            store: Arc::clone(&store),
            worker_id: "worker-a".into(),
            lease_ms: 30,
            max_attempts: 3,
        });
        let router_b = SessionInputRouter {
            store: Arc::clone(&store),
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

        let (store, _services, _router) = fixture().await;
        let request = SessionRuntimeOutboxRequest {
            request_id: "ack-loss-r1".into(),
            turn_id: "ack-loss-t1".into(),
            message_id: "ack-loss-m1".into(),
            created_at_ms: now_ms(),
        };
        let router_a = SessionInputRouter {
            store: Arc::clone(&store),
            worker_id: "worker-a".into(),
            lease_ms: 5,
            max_attempts: 3,
        };
        let router_b = SessionInputRouter {
            store: Arc::clone(&store),
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
    async fn session_dispatch_routes_into_the_real_target_session_stream() {
        let (store, services, _router) = fixture().await;
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
        let message = CrossSessionMessage {
            from_session_id: "s1".into(),
            target_ref: "@s2".into(),
            command: "review the active change".into(),
            actor: Some("test".into()),
            evidence_refs: vec![],
        };
        let mut graph = ExecutionGraph::new("cross-session dispatch");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::SessionDispatch,
            SESSION_DISPATCH_EXECUTOR,
            format!("session_input:{}", serde_json::to_string(&message).unwrap()),
        );
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        graph.nodes.push(node);
        let report = services.graph_runner().start(graph).await.unwrap();
        assert_eq!(report.failed, 0);
        assert_eq!(report.completed, 1);
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
    }
}
