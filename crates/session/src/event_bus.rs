// ── SessionEventBus — multi-frontend event synchronization ─────
// Enables WebUI (SSE) and TUI to receive real-time streaming events
// from the same daemon instance, sharing a single ActiveSessions.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio::sync::RwLock;

pub type EventSender = mpsc::Sender<String>;

struct SessionSubscriber {
    id: u64,
    sender: EventSender,
    /// Text SSE is deliberately lossy under pressure.  This bit turns the
    /// loss into an explicit recovery protocol instead of a silent gap.
    ///
    /// It is shared with the independent marker-delivery task because the
    /// event that fills the channel may itself be the final durable terminal.
    resync_pending: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionEventBusMetrics {
    pub active_subscribers: u64,
    pub lag_marked: u64,
    pub resync_sent: u64,
    pub disconnected: u64,
}

pub struct SessionEventBus {
    listeners: RwLock<HashMap<String, Vec<SessionSubscriber>>>,
    lag_marked: Arc<AtomicU64>,
    resync_sent: Arc<AtomicU64>,
    disconnected: Arc<AtomicU64>,
    next_subscriber_id: AtomicU64,
}

impl SessionEventBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            listeners: RwLock::new(HashMap::new()),
            lag_marked: Arc::new(AtomicU64::new(0)),
            resync_sent: Arc::new(AtomicU64::new(0)),
            disconnected: Arc::new(AtomicU64::new(0)),
            next_subscriber_id: AtomicU64::new(1),
        })
    }

    /// Register an event sender for a session. The sender will receive
    /// SSE-formatted event data whenever `broadcast` is called for that session.
    pub async fn subscribe(&self, session_id: &str, tx: EventSender) -> u64 {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        self.listeners
            .write()
            .await
            .entry(session_id.to_string())
            .or_default()
            .push(SessionSubscriber {
                id,
                sender: tx,
                resync_pending: Arc::new(AtomicBool::new(false)),
            });
        id
    }

    /// Remove a subscription by its immutable id.  Cleanup may race a new
    /// listener or another disconnect; vector indexes and channel identity
    /// are not stable enough for that operation.
    pub async fn unsubscribe(&self, session_id: &str, subscriber_id: u64) {
        let mut listeners = self.listeners.write().await;
        let should_remove = if let Some(txs) = listeners.get_mut(session_id) {
            txs.retain(|subscriber| subscriber.id != subscriber_id);
            txs.is_empty()
        } else {
            false
        };
        if should_remove {
            listeners.remove(session_id);
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
    pub async fn broadcast(&self, session_id: &str, sse_data: &str) {
        let listeners = self.listeners.read().await;
        if let Some(txs) = listeners.get(session_id) {
            let data = sse_data.to_string();
            let mut dead_ids = Vec::new();
            for (i, subscriber) in txs.iter().enumerate() {
                if subscriber.resync_pending.load(Ordering::Acquire) {
                    continue;
                }
                match subscriber.sender.try_send(data.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        if !subscriber.resync_pending.swap(true, Ordering::AcqRel) {
                            self.lag_marked.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                session_id,
                                consumer_index = i,
                                "SSE consumer lagged; scheduling an independent durable resync boundary"
                            );
                            let marker = serde_json::json!({
                                "type": "session_stream_resync",
                                "session_id": session_id,
                                "reason": "transport_lag",
                            })
                            .to_string();
                            let sender = subscriber.sender.clone();
                            let pending = Arc::clone(&subscriber.resync_pending);
                            let resync_sent = Arc::clone(&self.resync_sent);
                            let disconnected = Arc::clone(&self.disconnected);
                            tokio::spawn(async move {
                                match sender.send(marker).await {
                                    Ok(()) => {
                                        resync_sent.fetch_add(1, Ordering::Relaxed);
                                        // `send` preserves channel ordering.
                                        // Clear only after the recovery
                                        // boundary occupies its slot.
                                        pending.store(false, Ordering::Release);
                                    }
                                    Err(_) => {
                                        disconnected.fetch_add(1, Ordering::Relaxed);
                                        pending.store(false, Ordering::Release);
                                    }
                                }
                            });
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

    pub async fn metrics(&self) -> SessionEventBusMetrics {
        let active_subscribers = self
            .listeners
            .read()
            .await
            .values()
            .map(|subscribers| subscribers.len() as u64)
            .sum();
        SessionEventBusMetrics {
            active_subscribers,
            lag_marked: self.lag_marked.load(Ordering::Relaxed),
            resync_sent: self.resync_sent.load(Ordering::Relaxed),
            disconnected: self.disconnected.load(Ordering::Relaxed),
        }
    }

    // ── Convenience methods for real-time SSE streaming events ─────

    /// Broadcast a text delta event to all SSE subscribers for the given session.
    pub async fn text_delta(&self, session_id: &str, content: &str) {
        let json = serde_json::json!({
            "type": "TextDelta",
            "content": content,
        });
        self.broadcast(session_id, &json.to_string()).await;
    }

    /// Broadcast a thinking delta event to all SSE subscribers for the given session.
    pub async fn thinking_delta(&self, session_id: &str, content: &str) {
        let json = serde_json::json!({
            "type": "ThinkingDelta",
            "content": content,
        });
        self.broadcast(session_id, &json.to_string()).await;
    }

    /// Broadcast a tool start event to all SSE subscribers for the given session.
    pub async fn tool_start(&self, session_id: &str, id: &str, name: &str) {
        let json = serde_json::json!({
            "type": "ToolStart",
            "id": id,
            "name": name,
        });
        self.broadcast(session_id, &json.to_string()).await;
    }

    /// Broadcast a tool progress event to all SSE subscribers for the given session.
    pub async fn tool_progress(&self, session_id: &str, id: &str, name: &str, progress: &str) {
        let json = serde_json::json!({
            "type": "ToolProgress",
            "id": id,
            "name": name,
            "progress": progress,
        });
        self.broadcast(session_id, &json.to_string()).await;
    }

    /// Broadcast a tool complete event to all SSE subscribers for the given session.
    pub async fn tool_complete(
        &self,
        session_id: &str,
        id: &str,
        name: &str,
        result_summary: &str,
        exit_code: Option<i32>,
    ) {
        let json = serde_json::json!({
            "type": "ToolComplete",
            "id": id,
            "name": name,
            "summary": result_summary,
            "exit_code": exit_code,
        });
        self.broadcast(session_id, &json.to_string()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::SessionEventBus;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn unsubscribe_removes_empty_session_without_deadlock() {
        let bus = SessionEventBus::new();
        let (tx, _rx) = mpsc::channel(1);
        let subscription_id = bus.subscribe("session-a", tx).await;

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
        let bus = SessionEventBus::new();
        let (tx, rx) = mpsc::channel(1);
        let _ = bus.subscribe("session-a", tx).await;
        drop(rx);

        timeout(Duration::from_millis(200), bus.broadcast("session-a", "{}"))
            .await
            .expect("broadcast cleanup should not deadlock");

        assert!(!bus.listeners.read().await.contains_key("session-a"));
    }

    #[tokio::test]
    async fn lagged_consumer_receives_resync_while_fast_consumer_continues() {
        let bus = SessionEventBus::new();
        let (slow_tx, mut slow_rx) = mpsc::channel(1);
        let (fast_tx, mut fast_rx) = mpsc::channel(8);
        let _ = bus.subscribe("session-a", slow_tx).await;
        let _ = bus.subscribe("session-a", fast_tx).await;

        bus.broadcast("session-a", r#"{"type":"TextDelta","content":"one"}"#)
            .await;
        // Do not drain the slow queue: this marks it as lagged. The fast
        // listener still has enough capacity to receive every broadcast.
        bus.broadcast("session-a", r#"{"type":"TextDelta","content":"two"}"#)
            .await;
        assert!(slow_rx.try_recv().is_ok());
        bus.broadcast("session-a", r#"{"type":"TextDelta","content":"three"}"#)
            .await;

        let resync: serde_json::Value = serde_json::from_str(
            &timeout(Duration::from_millis(200), slow_rx.recv())
                .await
                .expect("slow listener gets recovery marker")
                .expect("slow sender remains connected"),
        )
        .expect("resync payload is JSON");
        assert_eq!(resync["type"], "session_stream_resync");
        assert_eq!(resync["reason"], "transport_lag");

        let mut fast_events = Vec::new();
        while let Ok(event) = fast_rx.try_recv() {
            fast_events.push(event);
        }
        assert_eq!(fast_events.len(), 3);
        let metrics = bus.metrics().await;
        assert_eq!(metrics.lag_marked, 1);
        assert_eq!(metrics.resync_sent, 1);
    }

    #[tokio::test]
    async fn final_event_overflow_delivers_resync_without_later_broadcast() {
        let bus = SessionEventBus::new();
        let (tx, mut rx) = mpsc::channel(1);
        let _ = bus.subscribe("session-terminal", tx).await;

        bus.broadcast(
            "session-terminal",
            r#"{"type":"TextDelta","content":"fills-channel"}"#,
        )
        .await;
        // This can be the final TerminalCommitted broadcast. It cannot fit,
        // and this test deliberately emits no later application event.
        bus.broadcast(
            "session-terminal",
            r#"{"type":"TerminalCommitted","terminal_id":"terminal-1"}"#,
        )
        .await;

        let first = timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("filled event remains readable")
            .expect("subscriber remains connected");
        assert!(first.contains("TextDelta"));
        let marker = timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("resync marker must not require a later broadcast")
            .expect("subscriber remains connected");
        assert!(marker.contains("session_stream_resync"));
        assert!(marker.contains("transport_lag"));
        let metrics = bus.metrics().await;
        assert_eq!(metrics.lag_marked, 1);
        assert_eq!(metrics.resync_sent, 1);
    }
}
