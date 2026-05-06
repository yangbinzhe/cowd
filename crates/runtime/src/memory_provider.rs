use std::sync::Arc;

pub trait MemoryProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn initialize(&self, session_id: &str);
    fn prefetch(&self, query: &str) -> Option<String>;
    fn sync_turn(&self, user_msg: &str, asst_msg: &str);
    fn is_available(&self) -> bool { true }
}

pub struct BuiltinMemoryProvider {
    pub enabled: bool,
}

impl MemoryProvider for BuiltinMemoryProvider {
    fn name(&self) -> &'static str { "builtin" }
    fn initialize(&self, _session_id: &str) {}
    fn prefetch(&self, _query: &str) -> Option<String> { None }
    fn sync_turn(&self, _user_msg: &str, _asst_msg: &str) {}
    fn is_available(&self) -> bool { self.enabled }
}

pub struct MemoryProviderManager {
    providers: Vec<Arc<dyn MemoryProvider>>,
}

impl MemoryProviderManager {
    pub fn new() -> Self {
        let builtin = Arc::new(BuiltinMemoryProvider { enabled: true });
        Self { providers: vec![builtin] }
    }

    pub fn register(&mut self, provider: Arc<dyn MemoryProvider>) {
        if self.providers.len() < 2 {
            self.providers.push(provider);
        }
    }

    pub fn prefetch_all(&self, query: &str) -> Vec<String> {
        self.providers.iter()
            .filter(|p| p.is_available())
            .filter_map(|p| p.prefetch(query))
            .collect()
    }

    pub fn sync_all(&self, user_msg: &str, asst_msg: &str) {
        for p in &self.providers {
            if p.is_available() { p.sync_turn(user_msg, asst_msg); }
        }
    }
}
