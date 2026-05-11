// P2-10: Effect system — side-effect abstraction for testable core logic.
// Derived from opencode's effect/ module pattern.

use std::path::PathBuf;

/// Side effects that ConversationRuntime produces. Handlers execute them.
#[derive(Debug, Clone)]
pub enum Effect {
    ReadFile(PathBuf),
    WriteFile(PathBuf, String),
    ExecuteTool(String, String),
    ApiCall(crate::conversation::ApiRequest),
    MemorySearch(String),
    EmitEvent(crate::bus::Event),
}

/// Result of handling an Effect.
#[derive(Debug, Clone)]
pub struct EffectResult {
    pub success: bool,
    pub data: String,
}

/// Trait for handling Effects. Enables mocking for tests.
pub trait EffectHandler: Send + Sync {
    fn handle(&self, effect: Effect) -> EffectResult;
}

/// Default handler that executes Effects for real.
pub struct RealEffectHandler;
impl EffectHandler for RealEffectHandler {
    fn handle(&self, effect: Effect) -> EffectResult {
        match effect {
            Effect::ReadFile(path) => match std::fs::read_to_string(&path) {
                Ok(s) => EffectResult { success: true, data: s },
                Err(e) => EffectResult { success: false, data: e.to_string() },
            },
            Effect::WriteFile(path, content) => match std::fs::write(&path, &content) {
                Ok(_) => EffectResult { success: true, data: String::new() },
                Err(e) => EffectResult { success: false, data: e.to_string() },
            },
            Effect::ExecuteTool(_, _) => EffectResult { success: false, data: "not implemented".into() },
            Effect::ApiCall(_) => EffectResult { success: false, data: "not implemented".into() },
            Effect::MemorySearch(q) => EffectResult { success: true, data: format!("search: {}", q) },
            Effect::EmitEvent(_) => EffectResult { success: true, data: String::new() },
        }
    }
}

/// Mock handler for testing.
pub struct MockEffectHandler {
    pub responses: std::collections::HashMap<String, String>,
}
impl EffectHandler for MockEffectHandler {
    fn handle(&self, effect: Effect) -> EffectResult {
        let key = match &effect {
            Effect::ReadFile(p) => format!("read:{}", p.display()),
            Effect::MemorySearch(q) => format!("search:{}", q),
            _ => "unknown".into(),
        };
        EffectResult { success: true, data: self.responses.get(&key).cloned().unwrap_or_default() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn p210_read_file_effect() { let h = RealEffectHandler; let r = h.handle(Effect::ReadFile(PathBuf::from("/dev/null"))); assert!(r.success); }
    #[test] fn p210_mock_handler_returns_predefined() { let mut m = MockEffectHandler { responses: Default::default() }; m.responses.insert("search:test".into(), "result".into()); let r = m.handle(Effect::MemorySearch("test".into())); assert_eq!(r.data, "result"); }
    #[test] fn p210_emit_event_is_noop() { let h = RealEffectHandler; let r = h.handle(Effect::EmitEvent(crate::bus::Event::SessionCreated{id:"x".into()})); assert!(r.success); }
}