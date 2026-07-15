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
    sender: EventSender,
    /// Text SSE is deliberately lossy under pressure.  This bit turns the
    /// loss into an explicit recovery protocol instead of a silent gap.
    resync_pending: AtomicBool,
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
    lag_marked: AtomicU64,
    resync_sent: AtomicU64,
    disconnected: AtomicU64,
}

impl SessionEventBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            listeners: RwLock::new(HashMap::new()),
            lag_marked: AtomicU64::new(0),
            resync_sent: AtomicU64::new(0),
            disconnected: AtomicU64::new(0),
        })
    }

    /// Register an event sender for a session. The sender will receive
    /// SSE-formatted event data whenever `broadcast` is called for that session.
    pub async fn subscribe(&self, session_id: &str, tx: EventSender) {
        self.listeners
            .write()
            .await
            .entry(session_id.to_string())
            .or_default()
            .push(SessionSubscriber {
                sender: tx,
                resync_pending: AtomicBool::new(false),
            });
    }

    /// Remove a specific sender from the session's subscriber list.
    /// Uses channel identity (same_channel) to match senders, since the
    /// sender passed to `subscribe` is moved into the subscriber list.
    pub async fn unsubscribe(&self, session_id: &str, tx: &EventSender) {
        let mut listeners = self.listeners.write().await;
        let should_remove = if let Some(txs) = listeners.get_mut(session_id) {
            txs.retain(|subscriber| !subscriber.sender.same_channel(tx));
            txs.is_empty()
        } else {
            false
        };
        if should_remove {
            listeners.remove(session_id);
        }
    }

    /// Send an SSE event string to all subscribers of the given session.
    /// Uses try_send to avoid blocking the producer.  A slow consumer is
    /// explicitly told to resync on the next writable opportunity; it never
    /// silently treats a dropped transient delta as a terminal event.
    pub async fn broadcast(&self, session_id: &str, sse_data: &str) {
        let listeners = self.listeners.read().await;
        if let Some(txs) = listeners.get(session_id) {
            let data = sse_data.to_string();
            let mut dead_indices = Vec::new();
            for (i, subscriber) in txs.iter().enumerate() {
                if subscriber.resync_pending.load(Ordering::Acquire) {
                    let marker = serde_json::json!({
                        "type": "session_stream_resync",
                        "session_id": session_id,
                        "reason": "transport_lag",
                    })
                    .to_string();
                    match subscriber.sender.try_send(marker) {
                        Ok(()) => {
                            subscriber.resync_pending.store(false, Ordering::Release);
                            self.resync_sent.fetch_add(1, Ordering::Relaxed);
                            // The marker itself is the recovery boundary. Do
                            // not immediately refill this small channel with
                            // another transient delta before the client can
                            // refresh its durable projection/transcript.
                            continue;
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => continue,
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dead_indices.push(i);
                            continue;
                        }
                    }
                }
                match subscriber.sender.try_send(data.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        if !subscriber.resync_pending.swap(true, Ordering::AcqRel) {
                            self.lag_marked.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                session_id,
                                consumer_index = i,
                                "SSE consumer lagged; explicit resync pending"
                            );
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        dead_indices.push(i);
                    }
                }
            }
            // Clean up dead connections
            if !dead_indices.is_empty() {
                drop(listeners);
                let mut listeners = self.listeners.write().await;
                let should_remove = if let Some(txs) = listeners.get_mut(session_id) {
                    for &i in dead_indices.iter().rev() {
                        txs.remove(i);
                        self.disconnected.fetch_add(1, Ordering::Relaxed);
                    }
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
        bus.subscribe("session-a", tx.clone()).await;

        timeout(
            Duration::from_millis(200),
            bus.unsubscribe("session-a", &tx),
        )
        .await
        .expect("unsubscribe should not deadlock");

        assert!(!bus.listeners.read().await.contains_key("session-a"));
    }

    #[tokio::test]
    async fn broadcast_cleans_closed_listener_without_deadlock() {
        let bus = SessionEventBus::new();
        let (tx, rx) = mpsc::channel(1);
        bus.subscribe("session-a", tx).await;
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
        bus.subscribe("session-a", slow_tx).await;
        bus.subscribe("session-a", fast_tx).await;

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
}
