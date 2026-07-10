// M5: ProviderPool — multi-API-key rotation with history preservation.
// Derived from GenericAgent's next_llm() + hermes-agent's adapter pattern.

use crate::conversation::{
    ApiClient, ApiRequest, AssistantEvent, ProviderContextInventory, RuntimeError,
    ToolContractScope,
};
use futures::stream;
use futures::stream::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

pub struct ProviderPool {
    clients: Vec<Box<dyn ApiClient + Send>>,
    current: AtomicUsize,
    /// M5-L1-2: conversation history preserved across provider rotations
    history: RwLock<Vec<String>>,
}

impl ProviderPool {
    pub fn new() -> Self {
        Self {
            clients: Vec::new(),
            current: AtomicUsize::new(0),
            history: RwLock::new(Vec::new()),
        }
    }

    pub fn add(&mut self, client: Box<dyn ApiClient + Send>) {
        self.clients.push(client);
    }

    pub fn rotate(&self) -> usize {
        let next = (self.current.load(Ordering::Relaxed) + 1) % self.clients.len().max(1);
        self.current.store(next, Ordering::Relaxed);
        next
    }

    pub fn current_idx(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// M5-L1-2: Save history before switching providers
    pub fn save_history(&self, messages: Vec<String>) {
        if let Ok(mut h) = self.history.write() {
            *h = messages;
        }
    }

    /// M5-L1-2: Retrieve preserved history after rotation
    pub fn history_len(&self) -> usize {
        self.history.read().map(|h| h.len()).unwrap_or(0)
    }
    pub fn len(&self) -> usize {
        self.clients.len()
    }
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}

impl ApiClient for ProviderPool {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let idx = self.current.load(Ordering::Relaxed) % self.clients.len().max(1);
        if self.clients.is_empty() {
            return Box::pin(stream::once(async {
                Err(RuntimeError::new("ProviderPool: no clients configured"))
            }));
        }
        self.clients[idx].stream(request)
    }

    fn configure_tool_contract_scope(&mut self, scope: ToolContractScope) {
        for client in &mut self.clients {
            client.configure_tool_contract_scope(scope);
        }
    }

    fn context_inventory(&self) -> ProviderContextInventory {
        if self.clients.is_empty() {
            return ProviderContextInventory::default();
        }
        let idx = self.current.load(Ordering::Relaxed) % self.clients.len();
        self.clients[idx].context_inventory()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::pin::Pin;

    struct DummyClient(usize);
    impl ApiClient for DummyClient {
        fn stream(
            &mut self,
            _: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(stream::iter(vec![
                Ok(AssistantEvent::TextDelta(self.0.to_string())),
                Ok(AssistantEvent::MessageStop),
            ]))
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
