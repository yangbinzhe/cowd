use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::{Mutex, OnceCell};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImmutableWorkKey {
    pub authority_scope: String,
    pub session_scope: String,
    pub source_revision: String,
    pub model_profile: String,
    pub prompt_contract: String,
    pub evidence_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coalesced<V> {
    pub value: V,
    pub shared: bool,
}

#[derive(Debug)]
pub struct InFlightCoalescer<K, V, E> {
    entries: Mutex<HashMap<K, Arc<OnceCell<Result<V, E>>>>>,
}

impl<K, V, E> Default for InFlightCoalescer<K, V, E> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V, E> InFlightCoalescer<K, V, E>
where
    K: Clone + Eq + Hash,
    V: Clone,
    E: Clone,
{
    pub async fn run<F, Fut>(&self, key: K, operation: F) -> Result<Coalesced<V>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        let (cell, owner) = {
            let mut entries = self.entries.lock().await;
            match entries.get(&key) {
                Some(cell) => (Arc::clone(cell), false),
                None => {
                    let cell = Arc::new(OnceCell::new());
                    entries.insert(key.clone(), Arc::clone(&cell));
                    (cell, true)
                }
            }
        };
        let result = cell.get_or_init(operation).await.clone();
        let mut entries = self.entries.lock().await;
        if entries
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &cell))
        {
            entries.remove(&key);
        }
        drop(entries);
        result.map(|value| Coalesced {
            value,
            shared: !owner,
        })
    }
}
