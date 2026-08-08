//! Gateway-owned, bounded Session projection fanout.
//!
//! Session domain state remains transport-neutral. This hub owns the
//! process-local projection queue used by HTTP/SSE consumers.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RuntimeStreamRange {
    pub(crate) start_bytes: usize,
    pub(crate) end_bytes: usize,
    pub(crate) stream_revision: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SessionProjectionTokenUsage {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) cache_creation_input_tokens: u64,
    pub(crate) cache_read_input_tokens: u64,
}

/// Typed Gateway projection contract. Runtime/domain events stay typed until
/// the HTTP live-stream boundary asks for their JSON representation.
#[derive(Debug, Clone)]
pub(crate) enum SessionProjectionEvent {
    Runtime {
        event: runtime::CowdEvent,
        tool_instance_id: Option<String>,
        stream_range: Option<RuntimeStreamRange>,
    },
    RuntimeStreamLagged {
        skipped: u64,
    },
    UserMessageCommitted {
        session_id: String,
        message_id: String,
        sequence: usize,
        execution_id: String,
        turn_id: String,
        input_turn_id: String,
        supplemental: bool,
        content: String,
        created_at_ms: u64,
    },
    TerminalCommitted {
        session_id: String,
        terminal_id: String,
        message_id: String,
        sequence: usize,
        response: String,
        runtime_commit_cursor: u64,
        replayed: bool,
        token_usage: Option<SessionProjectionTokenUsage>,
        execution_id: Option<String>,
        turn_id: Option<String>,
    },
    TurnCancelRequested {
        session_id: String,
        actor_id: String,
        reason: String,
        aborted_run_id: Option<String>,
        execution_ids: Vec<String>,
    },
    Resync {
        session_id: String,
        reason: &'static str,
    },
}

impl SessionProjectionEvent {
    pub(crate) fn runtime(event: runtime::CowdEvent) -> Self {
        Self::Runtime {
            event,
            tool_instance_id: None,
            stream_range: None,
        }
    }

    pub(crate) fn to_transport_value(&self) -> serde_json::Value {
        match self {
            Self::Runtime {
                event,
                tool_instance_id,
                stream_range,
            } => {
                let mut payload = runtime_event_transport_value(event);
                if let serde_json::Value::Object(fields) = &mut payload {
                    if let Some(instance_id) = tool_instance_id {
                        fields.insert(
                            "tool_instance_id".to_string(),
                            serde_json::Value::String(instance_id.clone()),
                        );
                    }
                    if let Some(range) = stream_range {
                        fields.insert("start_bytes".to_string(), range.start_bytes.into());
                        fields.insert("end_bytes".to_string(), range.end_bytes.into());
                        fields.insert("stream_revision".to_string(), range.stream_revision.into());
                    }
                }
                payload
            }
            Self::RuntimeStreamLagged { skipped } => {
                serde_json::json!({"type": "RuntimeStreamLagged", "skipped": skipped})
            }
            Self::UserMessageCommitted {
                session_id,
                message_id,
                sequence,
                execution_id,
                turn_id,
                input_turn_id,
                supplemental,
                content,
                created_at_ms,
            } => serde_json::json!({
                "type": "UserMessageCommitted",
                "session_id": session_id,
                "message_id": message_id,
                "sequence": sequence,
                "execution_id": execution_id,
                "turn_id": turn_id,
                "input_turn_id": input_turn_id,
                "supplemental": supplemental,
                "content": content,
                "created_at_ms": created_at_ms,
            }),
            Self::TerminalCommitted {
                session_id,
                terminal_id,
                message_id,
                sequence,
                response,
                runtime_commit_cursor,
                replayed,
                token_usage,
                execution_id,
                turn_id,
            } => {
                let part_id = format!("terminal-message:{message_id}");
                serde_json::json!({
                    "type": "TerminalCommitted",
                    "session_id": session_id,
                    "terminal_id": terminal_id,
                    "message_id": message_id,
                    "part_id": part_id,
                    "sequence": sequence,
                    "response": response,
                    "runtime_commit_cursor": runtime_commit_cursor,
                    "replayed": replayed,
                    "token_usage": token_usage,
                    "execution_id": execution_id,
                    "turn_id": turn_id,
                })
            }
            Self::TurnCancelRequested {
                session_id,
                actor_id,
                reason,
                aborted_run_id,
                execution_ids,
            } => serde_json::json!({
                "type": "TurnCancelRequested",
                "session_id": session_id,
                "actor_id": actor_id,
                "reason": reason,
                "status": "accepted",
                "aborted": aborted_run_id.is_some(),
                "run_id": aborted_run_id,
                "execution_ids": execution_ids,
            }),
            Self::Resync { session_id, reason } => serde_json::json!({
                "type": "session_stream_resync",
                "session_id": session_id,
                "reason": reason,
            }),
        }
    }
}

fn runtime_event_transport_value(event: &runtime::CowdEvent) -> serde_json::Value {
    let execution_context = event.execution_context().cloned();
    let execution_lineage = event.execution_lineage().cloned();
    let causal_identity = event.causal_identity().cloned();
    let activity_binding = event.activity_binding().cloned();
    let value = serde_json::to_value(event.domain_event()).unwrap_or_else(|error| {
        serde_json::json!({
            "type": "RuntimeEventEncodingError",
            "error": error.to_string(),
        })
    });
    let mut payload = match value {
        serde_json::Value::String(event_type) => serde_json::json!({"type": event_type}),
        serde_json::Value::Object(envelope) if envelope.len() == 1 => {
            let Some((event_type, payload)) = envelope.into_iter().next() else {
                return serde_json::json!({"type": "RuntimeEvent"});
            };
            match payload {
                serde_json::Value::Object(mut fields) => {
                    fields.insert("type".to_string(), serde_json::Value::String(event_type));
                    serde_json::Value::Object(fields)
                }
                payload => serde_json::json!({"type": event_type, "value": payload}),
            }
        }
        payload => serde_json::json!({"type": "RuntimeEvent", "value": payload}),
    };
    if let (Some(context), serde_json::Value::Object(fields)) = (execution_context, &mut payload) {
        fields.insert(
            "session_id".to_string(),
            serde_json::Value::String(context.session_id),
        );
        fields.insert(
            "execution_id".to_string(),
            serde_json::Value::String(context.execution_id),
        );
        fields.insert(
            "turn_id".to_string(),
            serde_json::Value::String(context.turn_id),
        );
        if let Some(lineage) = execution_lineage {
            fields.insert(
                "parent_execution_id".to_string(),
                serde_json::Value::String(lineage.parent_execution_id),
            );
            fields.insert(
                "graph_id".to_string(),
                serde_json::Value::String(lineage.graph_id),
            );
            fields.insert(
                "node_id".to_string(),
                serde_json::Value::String(lineage.node_id),
            );
            if let Some(team_id) = lineage.team_id {
                fields.insert("team_id".to_string(), serde_json::Value::String(team_id));
            }
            if let Some(agent_id) = lineage.agent_id {
                fields.insert("agent_id".to_string(), serde_json::Value::String(agent_id));
            }
        }
        if let Some(identity) = causal_identity {
            fields.insert(
                "model_step_id".to_string(),
                serde_json::Value::String(identity.model_step_id),
            );
            fields.insert(
                "item_id".to_string(),
                serde_json::Value::String(identity.item_id),
            );
            fields.insert(
                "segment_id".to_string(),
                serde_json::Value::String(identity.segment_id.clone()),
            );
            fields.insert(
                "part_id".to_string(),
                serde_json::Value::String(identity.segment_id),
            );
            fields.insert(
                "causal_sequence".to_string(),
                identity.causal_sequence.into(),
            );
            fields.insert("delta_sequence".to_string(), identity.delta_sequence.into());
            if let Some(tool_call_id) = identity.tool_call_id {
                fields.insert(
                    "tool_call_id".to_string(),
                    serde_json::Value::String(tool_call_id),
                );
            }
            fields.insert(
                "causal_parent_ids".to_string(),
                serde_json::to_value(identity.causal_parent_ids)
                    .unwrap_or_else(|_| serde_json::Value::Array(Vec::new())),
            );
        }
        if let Some(binding) = activity_binding {
            fields.insert(
                "activity_binding".to_string(),
                serde_json::to_value(binding).unwrap_or(serde_json::Value::Null),
            );
        }
    }
    payload
}

struct SessionSubscriber {
    id: u64,
    sender: mpsc::Sender<SessionProjectionEvent>,
    resync_pending: Arc<AtomicBool>,
}

pub struct SessionProjectionSubscription {
    id: u64,
    session_id: String,
    receiver: mpsc::Receiver<SessionProjectionEvent>,
    resync_pending: Arc<AtomicBool>,
    emit_resync: bool,
    resync_sent: Arc<AtomicU64>,
}

impl SessionProjectionSubscription {
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    pub async fn recv(&mut self) -> Option<SessionProjectionEvent> {
        if self.emit_resync {
            self.emit_resync = false;
            self.resync_pending.store(false, Ordering::Release);
            self.resync_sent.fetch_add(1, Ordering::Relaxed);
            return Some(SessionProjectionEvent::Resync {
                session_id: self.session_id.clone(),
                reason: "transport_lag",
            });
        }
        let event = self.receiver.recv().await?;
        if self.receiver.is_empty() && self.resync_pending.load(Ordering::Acquire) {
            self.emit_resync = true;
        }
        Some(event)
    }

    pub fn try_recv(&mut self) -> Result<SessionProjectionEvent, mpsc::error::TryRecvError> {
        if self.emit_resync {
            self.emit_resync = false;
            self.resync_pending.store(false, Ordering::Release);
            self.resync_sent.fetch_add(1, Ordering::Relaxed);
            return Ok(SessionProjectionEvent::Resync {
                session_id: self.session_id.clone(),
                reason: "transport_lag",
            });
        }
        let event = self.receiver.try_recv()?;
        if self.receiver.is_empty() && self.resync_pending.load(Ordering::Acquire) {
            self.emit_resync = true;
        }
        Ok(event)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct SessionProjectionHubMetrics {
    pub active_subscribers: u64,
    pub lag_marked: u64,
    pub resync_sent: u64,
    pub disconnected: u64,
}

pub struct SessionProjectionHub {
    listeners: RwLock<HashMap<String, Vec<SessionSubscriber>>>,
    lag_marked: Arc<AtomicU64>,
    resync_sent: Arc<AtomicU64>,
    disconnected: Arc<AtomicU64>,
    active_subscribers: Arc<AtomicU64>,
    next_subscriber_id: AtomicU64,
}

impl SessionProjectionHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            listeners: RwLock::new(HashMap::new()),
            lag_marked: Arc::new(AtomicU64::new(0)),
            resync_sent: Arc::new(AtomicU64::new(0)),
            disconnected: Arc::new(AtomicU64::new(0)),
            active_subscribers: Arc::new(AtomicU64::new(0)),
            next_subscriber_id: AtomicU64::new(1),
        })
    }

    /// Register a bounded typed projection subscription. Slow-consumer
    /// recovery is generated by the receiver after it drains its queue; no
    /// per-subscriber worker task is created.
    pub async fn subscribe(
        &self,
        session_id: &str,
        capacity: usize,
    ) -> SessionProjectionSubscription {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let resync_pending = Arc::new(AtomicBool::new(false));
        self.listeners
            .write()
            .await
            .entry(session_id.to_string())
            .or_default()
            .push(SessionSubscriber {
                id,
                sender,
                resync_pending: Arc::clone(&resync_pending),
            });
        self.active_subscribers.fetch_add(1, Ordering::Relaxed);
        SessionProjectionSubscription {
            id,
            session_id: session_id.to_string(),
            receiver,
            resync_pending,
            emit_resync: false,
            resync_sent: Arc::clone(&self.resync_sent),
        }
    }

    /// Remove a subscription by its immutable id.  Cleanup may race a new
    /// listener or another disconnect; vector indexes and channel identity
    /// are not stable enough for that operation.
    pub async fn unsubscribe(&self, session_id: &str, subscriber_id: u64) {
        let mut listeners = self.listeners.write().await;
        let mut removed = 0usize;
        let should_remove = if let Some(txs) = listeners.get_mut(session_id) {
            let before = txs.len();
            txs.retain(|subscriber| subscriber.id != subscriber_id);
            removed = before.saturating_sub(txs.len());
            txs.is_empty()
        } else {
            false
        };
        if should_remove {
            listeners.remove(session_id);
        }
        if removed > 0 {
            self.active_subscribers.fetch_sub(
                u64::try_from(removed).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
    }

    /// Send an SSE event string to all subscribers of the given session.
    /// Uses try_send to avoid coupling fast producers/subscribers to a slow
    /// Surface. When a channel is full an independent, ordered resync marker
    /// waits for that subscriber's next writable slot. Subsequent broadcasts
    /// are withheld from that subscriber until the marker is accepted.
    ///
    /// The marker cannot depend on a later application broadcast: the
    /// dropped event may be the session's final durable terminal.
    pub async fn publish(&self, session_id: &str, event: SessionProjectionEvent) {
        let listeners = self.listeners.read().await;
        if let Some(txs) = listeners.get(session_id) {
            let mut dead_ids = Vec::new();
            for (i, subscriber) in txs.iter().enumerate() {
                if subscriber.resync_pending.load(Ordering::Acquire) {
                    continue;
                }
                match subscriber.sender.try_send(event.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        if !subscriber.resync_pending.swap(true, Ordering::AcqRel) {
                            self.lag_marked.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                session_id,
                                consumer_index = i,
                                "SSE consumer lagged; scheduling an independent durable resync boundary"
                            );
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        dead_ids.push(subscriber.id);
                    }
                }
            }
            // Clean up dead connections
            if !dead_ids.is_empty() {
                drop(listeners);
                let mut listeners = self.listeners.write().await;
                let should_remove = if let Some(txs) = listeners.get_mut(session_id) {
                    let before = txs.len();
                    txs.retain(|subscriber| !dead_ids.contains(&subscriber.id));
                    self.disconnected.fetch_add(
                        u64::try_from(before.saturating_sub(txs.len())).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    self.active_subscribers.fetch_sub(
                        u64::try_from(before.saturating_sub(txs.len())).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    txs.is_empty()
                } else {
                    false
                };
                if should_remove {
                    listeners.remove(session_id);
                }
            }
        }
    }

    pub fn metrics(&self) -> SessionProjectionHubMetrics {
        SessionProjectionHubMetrics {
            active_subscribers: self.active_subscribers.load(Ordering::Relaxed),
            lag_marked: self.lag_marked.load(Ordering::Relaxed),
            resync_sent: self.resync_sent.load(Ordering::Relaxed),
            disconnected: self.disconnected.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionProjectionEvent, SessionProjectionHub};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn unsubscribe_removes_empty_session_without_deadlock() {
        let bus = SessionProjectionHub::new();
        let subscription = bus.subscribe("session-a", 1).await;
        let subscription_id = subscription.id();

        timeout(
            Duration::from_millis(200),
            bus.unsubscribe("session-a", subscription_id),
        )
        .await
        .expect("unsubscribe should not deadlock");

        assert!(!bus.listeners.read().await.contains_key("session-a"));
    }

    #[tokio::test]
    async fn broadcast_cleans_closed_listener_without_deadlock() {
        let bus = SessionProjectionHub::new();
        let subscription = bus.subscribe("session-a", 1).await;
        drop(subscription);

        timeout(
            Duration::from_millis(200),
            bus.publish(
                "session-a",
                SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
                    text: "closed-listener".to_string(),
                }),
            ),
        )
        .await
        .expect("broadcast cleanup should not deadlock");

        assert!(!bus.listeners.read().await.contains_key("session-a"));
    }

    #[tokio::test]
    async fn lagged_consumer_receives_resync_while_fast_consumer_continues() {
        let bus = SessionProjectionHub::new();
        let mut slow_rx = bus.subscribe("session-a", 1).await;
        let mut fast_rx = bus.subscribe("session-a", 8).await;

        bus.publish(
            "session-a",
            SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
                text: "one".to_string(),
            }),
        )
        .await;
        // Do not drain the slow queue: this marks it as lagged. The fast
        // listener still has enough capacity to receive every broadcast.
        bus.publish(
            "session-a",
            SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
                text: "two".to_string(),
            }),
        )
        .await;
        assert!(slow_rx.try_recv().is_ok());
        bus.publish(
            "session-a",
            SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
                text: "three".to_string(),
            }),
        )
        .await;

        let resync = timeout(Duration::from_millis(200), slow_rx.recv())
            .await
            .expect("slow listener gets recovery marker")
            .expect("slow sender remains connected");
        let resync = resync.to_transport_value();
        assert_eq!(resync["type"], "session_stream_resync");
        assert_eq!(resync["reason"], "transport_lag");

        let mut fast_events = Vec::new();
        while let Ok(event) = fast_rx.try_recv() {
            fast_events.push(event);
        }
        assert_eq!(fast_events.len(), 3);
        let metrics = bus.metrics();
        assert_eq!(metrics.lag_marked, 1);
        assert_eq!(metrics.resync_sent, 1);
    }

    #[tokio::test]
    async fn two_web_observers_and_one_tui_observer_receive_the_same_session_event() {
        let bus = SessionProjectionHub::new();
        let mut web_primary = bus.subscribe("shared-session", 8).await;
        let mut web_duplicate = bus.subscribe("shared-session", 8).await;
        let mut tui = bus.subscribe("shared-session", 8).await;

        bus.publish(
            "shared-session",
            SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
                text: "shared".to_string(),
            }),
        )
        .await;

        for receiver in [&mut web_primary, &mut web_duplicate, &mut tui] {
            let event = timeout(Duration::from_millis(200), receiver.recv())
                .await
                .expect("observer receives the event without blocking another observer")
                .expect("observer remains connected");
            assert_eq!(event.to_transport_value()["text"], "shared");
        }
        assert_eq!(bus.metrics().active_subscribers, 3);
    }

    #[tokio::test]
    async fn final_event_overflow_delivers_resync_without_later_broadcast() {
        let bus = SessionProjectionHub::new();
        let mut rx = bus.subscribe("session-terminal", 1).await;

        bus.publish(
            "session-terminal",
            SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
                text: "fills-channel".to_string(),
            }),
        )
        .await;
        // This can be the final TerminalCommitted broadcast. It cannot fit,
        // and this test deliberately emits no later application event.
        bus.publish(
            "session-terminal",
            SessionProjectionEvent::TerminalCommitted {
                session_id: "session-terminal".to_string(),
                terminal_id: "terminal-1".to_string(),
                message_id: "message-1".to_string(),
                sequence: 1,
                response: "done".to_string(),
                runtime_commit_cursor: 1,
                replayed: false,
                token_usage: None,
                execution_id: None,
                turn_id: None,
            },
        )
        .await;

        let first = timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("filled event remains readable")
            .expect("subscriber remains connected");
        assert_eq!(first.to_transport_value()["type"], "TextDelta");
        let marker = timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("resync marker must not require a later broadcast")
            .expect("subscriber remains connected");
        let marker = marker.to_transport_value();
        assert_eq!(marker["type"], "session_stream_resync");
        assert_eq!(marker["reason"], "transport_lag");
        let metrics = bus.metrics();
        assert_eq!(metrics.lag_marked, 1);
        assert_eq!(metrics.resync_sent, 1);
    }

    #[tokio::test]
    async fn repeated_overflow_keeps_one_subscription_and_one_resync_boundary() {
        let bus = SessionProjectionHub::new();
        let mut rx = bus.subscribe("session-overflow", 1).await;

        for index in 0..1_000 {
            bus.publish(
                "session-overflow",
                SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
                    text: format!("event-{index}"),
                }),
            )
            .await;
        }
        let before_drain = bus.metrics();
        assert_eq!(before_drain.active_subscribers, 1);
        assert_eq!(before_drain.lag_marked, 1);
        assert_eq!(before_drain.resync_sent, 0);

        assert!(
            rx.recv().await.is_some(),
            "the first queued event remains readable"
        );
        let resync = timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("one resync boundary follows the drained queue")
            .expect("subscriber remains connected")
            .to_transport_value();
        assert_eq!(resync["type"], "session_stream_resync");
        assert_eq!(resync["reason"], "transport_lag");

        let after_drain = bus.metrics();
        assert_eq!(after_drain.active_subscribers, 1);
        assert_eq!(after_drain.lag_marked, 1);
        assert_eq!(after_drain.resync_sent, 1);
        assert!(
            rx.try_recv().is_err(),
            "no duplicate resync marker is queued"
        );
    }

    #[test]
    fn delegated_agent_lifecycle_transport_keeps_canonical_lineage() {
        let event = SessionProjectionEvent::runtime(runtime::CowdEvent::RelatedExecution {
            lineage: runtime::CowdExecutionLineage {
                parent_execution_id: "root-execution".to_string(),
                graph_id: "team-graph".to_string(),
                node_id: "research-node".to_string(),
                team_id: Some("team-run".to_string()),
                agent_id: Some("researcher-1".to_string()),
            },
            event: Box::new(runtime::CowdEvent::ExecutionScoped {
                context: runtime::CowdExecutionContext {
                    execution_id: "agent-run".to_string(),
                    session_id: "session-a".to_string(),
                    turn_id: "turn-a".to_string(),
                },
                activity_binding: None,
                event: Box::new(runtime::CowdEvent::AgentLifecycle {
                    run_id: "agent-run".to_string(),
                    agent_id: "researcher-1".to_string(),
                    role: Some("researcher".to_string()),
                    phase: runtime::AgentLifecyclePhase::Started,
                    status: "running".to_string(),
                    summary: None,
                }),
            }),
        })
        .to_transport_value();

        assert_eq!(event["type"], "AgentLifecycle");
        assert_eq!(event["execution_id"], "agent-run");
        assert_eq!(event["parent_execution_id"], "root-execution");
        assert_eq!(event["graph_id"], "team-graph");
        assert_eq!(event["team_id"], "team-run");
        assert_eq!(event["agent_id"], "researcher-1");
        assert_eq!(event["phase"], "started");
    }
}
