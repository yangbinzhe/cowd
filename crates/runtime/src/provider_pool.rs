// M5: ProviderPool — multi-API-key rotation with history preservation.
// Derived from GenericAgent's next_llm() + hermes-agent's adapter pattern.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::pin::Pin;
use futures::stream::Stream;
use crate::conversation::{ApiClient, ApiRequest, AssistantEvent, RuntimeError};

pub struct ProviderPool {
    clients: Vec<Box<dyn ApiClient + Send>>,
    current: AtomicUsize,
    /// M5-L1-2: conversation history preserved across provider rotations
    history: std::sync::Mutex<Vec<String>>,
}

impl ProviderPool {
    pub fn new() -> Self { Self { clients: Vec::new(), current: AtomicUsize::new(0), history: std::sync::Mutex::new(Vec::new()) } }

    pub fn add(&mut self, client: Box<dyn ApiClient + Send>) { self.clients.push(client); }

    pub fn rotate(&self) -> usize {
        let next = (self.current.load(Ordering::Relaxed) + 1) % self.clients.len().max(1);
        self.current.store(next, Ordering::Relaxed);
        next
    }

    pub fn current_idx(&self) -> usize { self.current.load(Ordering::Relaxed) }

    /// M5-L1-2: Save history before switching providers
    pub fn save_history(&self, messages: Vec<String>) {
        if let Ok(mut h) = self.history.lock() { *h = messages; }
    }

    /// M5-L1-2: Retrieve preserved history after rotation
    pub fn history_len(&self) -> usize {
        self.history.lock().map(|h| h.len()).unwrap_or(0)
    }
    pub fn len(&self) -> usize { self.clients.len() }
    pub fn is_empty(&self) -> bool { self.clients.is_empty() }
}

impl ApiClient for ProviderPool {
    fn stream(&mut self, request: ApiRequest) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + '_>> {
        let idx = self.current.load(Ordering::Relaxed) % self.clients.len().max(1);
        self.clients[idx].stream(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use futures::stream;

    struct DummyClient(usize);
    impl ApiClient for DummyClient {
        fn stream(&mut self, _: ApiRequest) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + '_>> {
            Box::pin(stream::iter(vec![Ok(AssistantEvent::TextDelta(self.0.to_string())), Ok(AssistantEvent::MessageStop)]))
        }
    }

    #[test]
    fn m5_pool_rotation_alternates_clients() {
        let mut pool = ProviderPool::new();
        pool.add(Box::new(DummyClient(1)));
        pool.add(Box::new(DummyClient(2)));
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.current_idx(), 0);
        let next = pool.rotate();
        assert_eq!(next, 1);
        assert_eq!(pool.current_idx(), 1);
        let next2 = pool.rotate();
        assert_eq!(next2, 0);
    }

    #[test]
    fn m5_empty_pool_rotate_is_noop() {
        let pool = ProviderPool::new();
        assert!(pool.is_empty());
        assert_eq!(pool.rotate(), 0);
    }

    #[test]
    fn m5_add_and_increment_len() {
        let mut pool = ProviderPool::new();
        pool.add(Box::new(DummyClient(1)));
        pool.add(Box::new(DummyClient(2)));
        assert_eq!(pool.len(), 2);
        pool.rotate();
        assert_eq!(pool.current_idx(), 1);
    }

    #[test]
    fn m5_history_preserved_across_rotation() {
        let pool = ProviderPool::new();
        pool.save_history(vec!["msg1".into(), "msg2".into()]);
        assert_eq!(pool.history_len(), 2);
        pool.rotate(); // rotation doesn't clear history
        assert_eq!(pool.history_len(), 2);
    }
}
