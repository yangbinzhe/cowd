// ── SessionEventBus — multi-frontend event synchronization ─────
// Enables WebUI (SSE) and TUI to receive real-time streaming events
// from the same daemon instance, sharing a single ActiveSessions.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

pub(crate) type EventSender = mpsc::UnboundedSender<String>;

pub struct SessionEventBus {
    listeners: RwLock<HashMap<String, Vec<EventSender>>>,
}

impl SessionEventBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            listeners: RwLock::new(HashMap::new()),
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
            .push(tx);
    }

    /// Send an SSE event string to all subscribers of the given session.
    /// Dropped senders are silently ignored (the subscriber disconnected).
    pub async fn broadcast(&self, session_id: &str, sse_data: &str) {
        if let Some(txs) = self.listeners.read().await.get(session_id) {
            for tx in txs {
                let _ = tx.send(sse_data.to_string());
            }
        }
    }
}
