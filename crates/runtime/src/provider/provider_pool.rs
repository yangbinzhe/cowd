// M5: ProviderPool — multi-API-key rotation with history preservation.
// Derived from GenericAgent's next_llm() + hermes-agent's adapter pattern.

use crate::conversation::{
    ApiClient, ApiRequest, AssistantEvent, ProviderContextInventory, RuntimeError,
};
use futures::stream;
use futures::stream::Stream;
use harness_contract::tool::ToolExposureProjection;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crate::provider_registry::{ProviderRegistry, ProviderRegistrySnapshot};
use crate::ProviderRuntimeClient;

pub struct ProviderPool {
    registry: Arc<ProviderRegistry>,
    clients: Vec<ProviderRuntimeClient>,
    current: AtomicUsize,
    last_request_revision: AtomicU64,
    /// M5-L1-2: conversation history preserved across provider rotations
    history: RwLock<Vec<String>>,
}

impl ProviderPool {
    pub fn new(registry: Arc<ProviderRegistry>) -> Self {
        Self {
            registry,
            clients: Vec::new(),
            current: AtomicUsize::new(0),
            last_request_revision: AtomicU64::new(0),
            history: RwLock::new(Vec::new()),
        }
    }

    #[must_use]
    pub fn pin_provider_snapshot(&self) -> ProviderRegistrySnapshot {
        self.registry.pin()
    }

    #[must_use]
    pub fn last_request_revision(&self) -> Option<u64> {
        match self.last_request_revision.load(Ordering::Acquire) {
            0 => None,
            revision => Some(revision),
        }
    }

    pub fn add(&mut self, client: ProviderRuntimeClient) -> Result<(), String> {
        if !Arc::ptr_eq(&self.registry, client.provider_registry()) {
            return Err("ProviderPool client must use the pool's ProviderRegistry".to_string());
        }
        self.clients.push(client);
        Ok(())
    }

    pub fn configure_tool_exposure(&mut self, projection: ToolExposureProjection) {
        for client in &mut self.clients {
            client.configure_tool_exposure(projection.clone());
        }
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
        let provider_snapshot = self.pin_provider_snapshot();
        self.last_request_revision
            .store(provider_snapshot.revision(), Ordering::Release);
        let idx = self.current.load(Ordering::Relaxed) % self.clients.len().max(1);
        if self.clients.is_empty() {
            return Box::pin(stream::once(async {
                Err(RuntimeError::new("ProviderPool: no clients configured"))
            }));
        }
        self.clients[idx].stream_with_provider_snapshot(request, provider_snapshot)
    }

    fn configure_tool_exposure(&mut self, projection: ToolExposureProjection) {
        ProviderPool::configure_tool_exposure(self, projection);
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
    use crate::config::{ProviderConfig, ProvidersConfig};
    use std::collections::HashMap;

    fn registry() -> Arc<ProviderRegistry> {
        Arc::new(
            ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "test".to_string(),
                    ProviderConfig {
                        name: "test".to_string(),
                        base_url: "https://example.test/v1".to_string(),
                        api_key: "secret".to_string(),
                        models: vec!["dummy".to_string()],
                        protocol: Some("completions".to_string()),
                    },
                )]),
            })
            .unwrap(),
        )
    }

    fn client(registry: Arc<ProviderRegistry>) -> ProviderRuntimeClient {
        ProviderRuntimeClient::new_with_fallback_config(
            registry,
            "dummy".to_string(),
            Vec::new(),
            &[],
        )
        .unwrap()
    }

    #[test]
    fn m5_pool_rotation_alternates_clients() {
        let registry = registry();
        let mut pool = ProviderPool::new(registry.clone());
        pool.add(client(registry.clone())).unwrap();
        pool.add(client(registry)).unwrap();
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
        let pool = ProviderPool::new(registry());
        assert!(pool.is_empty());
        assert_eq!(pool.rotate(), 0);
    }

    #[test]
    fn m5_add_and_increment_len() {
        let registry = registry();
        let mut pool = ProviderPool::new(registry.clone());
        pool.add(client(registry.clone())).unwrap();
        pool.add(client(registry)).unwrap();
        assert_eq!(pool.len(), 2);
        pool.rotate();
        assert_eq!(pool.current_idx(), 1);
    }

    #[test]
    fn m5_history_preserved_across_rotation() {
        let pool = ProviderPool::new(registry());
        pool.save_history(vec!["msg1".into(), "msg2".into()]);
        assert_eq!(pool.history_len(), 2);
        pool.rotate(); // rotation doesn't clear history
        assert_eq!(pool.history_len(), 2);
    }

    #[test]
    fn request_pins_registry_revision() {
        let registry = registry();
        let mut pool = ProviderPool::new(registry.clone());
        pool.add(client(registry.clone())).unwrap();
        let mut updated = registry.pin().config().clone();
        updated
            .providers
            .get_mut("test")
            .unwrap()
            .models
            .push("dummy-v2".to_string());
        registry.replace(updated).expect("valid provider reload");
        let request = ApiRequest {
            system_prompt: Vec::new(),
            messages: Vec::new(),
            model: String::new(),
        };
        let stream = pool.stream(request);
        drop(stream);

        assert_eq!(pool.last_request_revision(), Some(2));
    }

    #[test]
    fn rejects_client_from_a_different_registry() {
        let pool_registry = registry();
        let mut pool = ProviderPool::new(pool_registry);
        let error = pool.add(client(registry())).unwrap_err();

        assert!(error.contains("pool's ProviderRegistry"));
    }
}
