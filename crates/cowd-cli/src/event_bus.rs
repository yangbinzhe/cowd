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

    /// Remove a specific sender from the session's subscriber list.
    /// Uses channel identity (same_channel) to match senders, since the
    /// sender passed to `subscribe` is moved into the subscriber list.
    pub async fn unsubscribe(&self, session_id: &str, tx: &EventSender) {
        if let Some(txs) = self.listeners.write().await.get_mut(session_id) {
            txs.retain(|t| !t.same_channel(tx));
            if txs.is_empty() {
                self.listeners.write().await.remove(session_id);
            }
        }
    }

    /// Send an SSE event string to all subscribers of the given session.
    /// Dead (disconnected) senders are automatically cleaned up.
    pub async fn broadcast(&self, session_id: &str, sse_data: &str) {
        let dead_indices: Vec<usize> = {
            if let Some(txs) = self.listeners.read().await.get(session_id) {
                txs.iter()
                    .enumerate()
                    .filter_map(|(i, tx)| {
                        if tx.send(sse_data.to_string()).is_err() {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                return;
            }
        };

        // Clean up dead connections
        if !dead_indices.is_empty() {
            if let Some(txs) = self.listeners.write().await.get_mut(session_id) {
                for &i in dead_indices.iter().rev() {
                    txs.remove(i);
                }
                if txs.is_empty() {
                    self.listeners.write().await.remove(session_id);
                }
            }
        }
    }
}
