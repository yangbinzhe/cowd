//! Per-chat serial message processing with pending event queue and drainer.
//!
//! Matches Hermes' `_pending_inbound_events` + `_drain_pending_inbound_events` pattern:
//! - Each chat has its own `tokio::sync::Mutex` for serial message processing.
//! - When a chat is busy, inbound events are queued in a `VecDeque`.
//! - A background drainer polls every 250ms and retries queued events.
//! - The drainer gives up after 120 seconds (Hermes timeout cap).
//! - Queue depth is capped at `max_queue_depth` (default 1000, matches Hermes).

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

/// Maximum wait time for the drainer before giving up (matches Hermes).
const DRAINER_MAX_WAIT_SECS: u64 = 120;

/// Poll interval for the drainer (matches Hermes).
const DRAINER_POLL_INTERVAL_MS: u64 = 250;

/// Default maximum queue depth (matches Hermes `_pending_inbound_max_depth`).
pub const DEFAULT_MAX_QUEUE_DEPTH: usize = 1000;

type DrainHandler = Arc<
    dyn Fn(String, serde_json::Value) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
>;

/// Decision returned by `try_process`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingDecision {
    Process,
    Queued,
    Dropped,
}

/// Per-chat serial processing queue with pending event buffer and background drainer.
///
/// Each chat gets its own `tokio::sync::Mutex`. When a message arrives:
/// 1. `try_lock_owned()` the per-chat mutex.
/// 2. If acquired → store the guard, return `Process`. Caller processes, then calls `release()`.
/// 3. If busy → enqueue event, spawn drainer if not already running.
/// 4. If queue full → evict oldest, return `Dropped`.
///
/// The drainer polls every 250ms, attempts to acquire chat locks for
/// queued events, and invokes the event handler when successful.
/// It stops after 120 seconds of idle polling.
pub struct ChatProcessingQueue {
    chat_locks: Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>,
    pending_events: Arc<Mutex<VecDeque<(String, serde_json::Value, Instant)>>>,
    max_queue_depth: usize,
    drain_scheduled: Arc<AtomicBool>,
    drain_handler: Option<DrainHandler>,
    active_guards: Arc<Mutex<HashMap<String, OwnedMutexGuard<()>>>>,
}

impl ChatProcessingQueue {
    pub fn new(max_queue_depth: usize) -> Self {
        Self {
            chat_locks: Arc::new(RwLock::new(HashMap::new())),
            pending_events: Arc::new(Mutex::new(VecDeque::new())),
            max_queue_depth,
            drain_scheduled: Arc::new(AtomicBool::new(false)),
            drain_handler: None,
            active_guards: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attach an async event handler invoked by the drainer for drained events.
    pub fn with_drain_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(String, serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.drain_handler = Some(Arc::new(move |chat_id, event| {
            Box::pin(handler(chat_id, event))
        }));
        self
    }

    /// Try to start processing for a chat.
    ///
    /// Returns `Process` when the per-chat lock is acquired (the caller
    /// must call [`release`] afterwards). Returns `Queued` when the chat
    /// is busy and the event was enqueued. Returns `Dropped` when the
    /// queue is full and the oldest event was evicted.
    pub async fn try_process(&self, chat_id: &str, event: serde_json::Value) -> ProcessingDecision {
        let chat_lock = {
            let locks = self.chat_locks.read().await;
            locks.get(chat_id).cloned()
        };

        let chat_lock = match chat_lock {
            Some(lock) => lock,
            None => {
                let mut locks = self.chat_locks.write().await;
                locks
                    .entry(chat_id.to_string())
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            }
        };

        match chat_lock.try_lock_owned() {
            Ok(guard) => {
                self.active_guards
                    .lock()
                    .await
                    .insert(chat_id.to_string(), guard);
                ProcessingDecision::Process
            }
            Err(_) => {
                let dropped = {
                    let mut queue = self.pending_events.lock().await;
                    queue.push_back((chat_id.to_string(), event, Instant::now()));

                    if queue.len() > self.max_queue_depth {
                        queue.pop_front();
                        true
                    } else {
                        false
                    }
                };

                if dropped {
                    return ProcessingDecision::Dropped;
                }

                if self.drain_handler.is_some()
                    && !self.drain_scheduled.swap(true, Ordering::SeqCst)
                {
                    self.spawn_drainer();
                }

                ProcessingDecision::Queued
            }
        }
    }

    /// Release the lock for a chat so the next queued event can be processed.
    ///
    /// Must be called after `try_process` returns `Process` and processing completes.
    pub async fn release(&self, chat_id: &str) {
        self.active_guards.lock().await.remove(chat_id);

        if let Some(handler) = &self.drain_handler {
            let mut queue = self.pending_events.lock().await;
            if let Some(pos) = queue.iter().position(|(cid, _, _)| cid == chat_id) {
                if let Some((cid, event, _ts)) = queue.remove(pos) {
                    drop(queue);
                    let handler = Arc::clone(handler);
                    tokio::spawn(async move {
                        handler(cid, event).await;
                    });
                }
            }
        }
    }

    fn spawn_drainer(&self) {
        let pending = Arc::clone(&self.pending_events);
        let locks = Arc::clone(&self.chat_locks);
        let scheduled = Arc::clone(&self.drain_scheduled);
        let handler = self.drain_handler.clone();
        let active_guards = Arc::clone(&self.active_guards);
        let period = Duration::from_millis(DRAINER_POLL_INTERVAL_MS);
        let max_wait = Duration::from_secs(DRAINER_MAX_WAIT_SECS);

        tokio::spawn(async move {
            let mut last_drain = Instant::now();

            loop {
                tokio::time::sleep(period).await;

                if last_drain.elapsed() > max_wait {
                    scheduled.store(false, Ordering::SeqCst);
                    return;
                }

                let handler = match &handler {
                    Some(h) => h.clone(),
                    None => {
                        scheduled.store(false, Ordering::SeqCst);
                        return;
                    }
                };

                let events: Vec<(String, serde_json::Value)> = {
                    let mut queue = pending.lock().await;
                    if queue.is_empty() {
                        continue;
                    }
                    queue.drain(..).map(|(id, ev, _ts)| (id, ev)).collect()
                };

                if events.is_empty() {
                    continue;
                }

                let mut kept = Vec::new();
                let mut drained_any = false;

                for (chat_id, event) in events {
                    let chat_lock = {
                        let l = locks.read().await;
                        l.get(&chat_id).cloned()
                    };

                    let chat_lock = match chat_lock {
                        Some(l) => l,
                        None => {
                            let mut l = locks.write().await;
                            l.entry(chat_id.clone())
                                .or_insert_with(|| Arc::new(Mutex::new(())))
                                .clone()
                        }
                    };

                    match chat_lock.try_lock_owned() {
                        Ok(guard) => {
                            active_guards.lock().await.insert(chat_id.clone(), guard);
                            drained_any = true;
                            let h = handler.clone();
                            let cid = chat_id.clone();
                            let guards = Arc::clone(&active_guards);
                            tokio::spawn(async move {
                                h(cid.clone(), event).await;
                                guards.lock().await.remove(&cid);
                            });
                        }
                        Err(_) => {
                            kept.push((chat_id, event, Instant::now()));
                        }
                    }
                }

                if !kept.is_empty() {
                    let mut queue = pending.lock().await;
                    for item in kept.into_iter().rev() {
                        queue.push_front(item);
                    }
                }

                if drained_any {
                    last_drain = Instant::now();
                }
            }
        });
    }
}

impl Default for ChatProcessingQueue {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_QUEUE_DEPTH)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn test_queue(
        max_depth: usize,
    ) -> (
        ChatProcessingQueue,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let counter = Arc::new(AtomicUsize::new(0));
        let processed = Arc::new(Mutex::new(Vec::new()));
        let c = counter.clone();
        let p = processed.clone();
        let queue = ChatProcessingQueue::new(max_depth).with_drain_handler(
            move |chat_id: String, _event: serde_json::Value| {
                let c = c.clone();
                let p = p.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    p.lock().await.push(chat_id);
                }
            },
        );
        (queue, counter, processed)
    }

    #[tokio::test]
    async fn test_lock_acquisition_prevents_concurrent_processing() {
        let (queue, counter, _processed) = test_queue(10);

        let decision1 = queue
            .try_process("chat-1", serde_json::json!({"msg": "first"}))
            .await;
        assert_eq!(decision1, ProcessingDecision::Process);

        let decision2 = queue
            .try_process("chat-1", serde_json::json!({"msg": "second"}))
            .await;
        assert_eq!(decision2, ProcessingDecision::Queued);

        queue.release("chat-1").await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            counter.load(Ordering::SeqCst) >= 1,
            "Drained event should be processed"
        );
    }

    #[tokio::test]
    async fn test_enqueue_when_busy() {
        let (queue, _counter, _processed) = test_queue(10);

        let decision1 = queue
            .try_process("chat-1", serde_json::json!({"msg": "first"}))
            .await;
        assert_eq!(decision1, ProcessingDecision::Process);

        let decision2 = queue
            .try_process("chat-1", serde_json::json!({"msg": "second"}))
            .await;
        assert_eq!(decision2, ProcessingDecision::Queued);

        let decision3 = queue
            .try_process("chat-1", serde_json::json!({"msg": "third"}))
            .await;
        assert_eq!(decision3, ProcessingDecision::Queued);

        let queue_len = queue.pending_events.lock().await.len();
        assert_eq!(queue_len, 2);
    }

    #[tokio::test]
    async fn test_drainer_processes_queued_events() {
        let (queue, counter, _processed) = test_queue(10);

        let decision1 = queue
            .try_process("chat-1", serde_json::json!({"msg": "first"}))
            .await;
        assert_eq!(decision1, ProcessingDecision::Process);

        queue
            .try_process("chat-1", serde_json::json!({"msg": "second"}))
            .await;
        queue
            .try_process("chat-1", serde_json::json!({"msg": "third"}))
            .await;

        queue.release("chat-1").await;

        tokio::time::sleep(Duration::from_millis(600)).await;

        let count = counter.load(Ordering::SeqCst);
        assert!(
            count >= 2,
            "Expected at least 2 drained events, got {}",
            count
        );
    }

    #[tokio::test]
    async fn test_queue_overflow_drops_oldest() {
        let (queue, _counter, _processed) = test_queue(3);

        queue
            .try_process("chat-1", serde_json::json!({"msg": "first"}))
            .await;

        for i in 0..3 {
            let decision = queue
                .try_process("chat-1", serde_json::json!({"msg": format!("event-{}", i)}))
                .await;
            assert_eq!(
                decision,
                ProcessingDecision::Queued,
                "event {} should be queued",
                i
            );
        }

        assert_eq!(queue.pending_events.lock().await.len(), 3);

        let decision = queue
            .try_process("chat-1", serde_json::json!({"msg": "overflow"}))
            .await;
        assert_eq!(decision, ProcessingDecision::Dropped);

        assert_eq!(queue.pending_events.lock().await.len(), 3);
    }

    #[tokio::test]
    async fn test_release_allows_next_event() {
        let (queue, counter, processed) = test_queue(10);

        queue
            .try_process("chat-1", serde_json::json!({"msg": "first"}))
            .await;

        queue
            .try_process("chat-1", serde_json::json!({"msg": "second"}))
            .await;

        queue.release("chat-1").await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        let p = processed.lock().await;
        assert_eq!(p.len(), 1);
        assert_eq!(p[0], "chat-1");
    }

    #[tokio::test]
    async fn test_different_chats_independent_locks() {
        let (queue, _counter, _processed) = test_queue(10);

        let d1 = queue
            .try_process("chat-1", serde_json::json!({"msg": "a"}))
            .await;
        assert_eq!(d1, ProcessingDecision::Process);

        let d2 = queue
            .try_process("chat-2", serde_json::json!({"msg": "b"}))
            .await;
        assert_eq!(d2, ProcessingDecision::Process);

        let d3 = queue
            .try_process("chat-1", serde_json::json!({"msg": "c"}))
            .await;
        assert_eq!(d3, ProcessingDecision::Queued);
    }

    #[tokio::test]
    async fn test_max_queue_depth_default() {
        let queue = ChatProcessingQueue::default();
        assert_eq!(queue.max_queue_depth, DEFAULT_MAX_QUEUE_DEPTH);
    }

    #[tokio::test]
    async fn test_no_handler_drainer_does_not_start() {
        let queue = ChatProcessingQueue::new(10);

        queue
            .try_process("chat-1", serde_json::json!({"msg": "first"}))
            .await;

        queue
            .try_process("chat-1", serde_json::json!({"msg": "second"}))
            .await;

        assert!(!queue.drain_scheduled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_release_drops_guard() {
        let queue = ChatProcessingQueue::new(10);

        let d1 = queue
            .try_process("chat-1", serde_json::json!({"msg": "first"}))
            .await;
        assert_eq!(d1, ProcessingDecision::Process);

        assert_eq!(queue.active_guards.lock().await.len(), 1);

        queue.release("chat-1").await;

        assert_eq!(queue.active_guards.lock().await.len(), 0);

        let d2 = queue
            .try_process("chat-1", serde_json::json!({"msg": "second"}))
            .await;
        assert_eq!(d2, ProcessingDecision::Process);
    }
}
