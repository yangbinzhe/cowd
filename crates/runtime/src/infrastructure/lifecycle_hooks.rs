// M4: LifecycleHook trait — plugin system foundation.
// Derived from hermes-agent's pre/post tool + turn lifecycle hooks.

use async_trait::async_trait;

#[async_trait]
pub trait LifecycleHook: Send + Sync {
    async fn on_turn_start(&self, _user_input: &str) -> Result<(), String> {
        Ok(())
    }
    async fn pre_tool_call(&self, _tool_name: &str, _input: &str) -> Result<(), String> {
        Ok(())
    }
    async fn post_tool_call(
        &self,
        _tool_name: &str,
        _result: &str,
        _is_error: bool,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn on_turn_end(&self, _summary: &str) -> Result<(), String> {
        Ok(())
    }
    async fn on_session_start(&self, _session_id: &str) -> Result<(), String> {
        Ok(())
    }
    async fn on_session_end(&self, _session_id: &str) -> Result<(), String> {
        Ok(())
    }
}

pub struct HookRunner {
    hooks: Vec<Box<dyn LifecycleHook>>,
    /// M9: optional CowdEventBus for tool lifecycle events
    pub bus: Option<crate::cowd_event::CowdEventBus>,
}

impl Default for HookRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl HookRunner {
    pub fn new() -> Self {
        Self {
            hooks: Vec::new(),
            bus: None,
        }
    }
    pub fn register(&mut self, hook: Box<dyn LifecycleHook>) {
        self.hooks.push(hook);
    }

    pub async fn fire_turn_start(&self, input: &str) {
        for hook in &self.hooks {
            let _ = hook.on_turn_start(input).await;
        }
    }
    pub async fn fire_pre_tool(&self, name: &str, input: &str) {
        for hook in &self.hooks {
            let _ = hook.pre_tool_call(name, input).await;
        }
    }
    pub async fn fire_post_tool(&self, name: &str, result: &str, is_error: bool, duration_ms: u64) {
        for hook in &self.hooks {
            let _ = hook.post_tool_call(name, result, is_error).await;
        }
        if let Some(ref bus) = self.bus {
            bus.emit(crate::cowd_event::CowdEvent::ToolExecuted {
                name: name.to_string(),
                duration_ms,
            });
        }
    }
    pub async fn fire_turn_end(&self, summary: &str) {
        for hook in &self.hooks {
            let _ = hook.on_turn_end(summary).await;
        }
    }
}
