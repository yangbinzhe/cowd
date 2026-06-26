use std::sync::{Arc, Mutex};

use plugins::PluginRegistry;

use crate::gateway_tool_executor::GatewayToolExecutor;
use crate::runtime_bootstrap::RuntimeMcpState;

pub(crate) struct GatewayRuntimeEntry {
    runtime: Option<runtime::StandardRuntimeHost<GatewayToolExecutor>>,
    plugin_registry: PluginRegistry,
    plugins_active: bool,
    mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    mcp_active: bool,
    resume_context_loaded: bool,
}

impl GatewayRuntimeEntry {
    pub(crate) fn new(
        runtime: runtime::StandardRuntimeHost<GatewayToolExecutor>,
        plugin_registry: PluginRegistry,
        mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
        resume_context_loaded: bool,
    ) -> Self {
        Self {
            runtime: Some(runtime),
            plugin_registry,
            plugins_active: true,
            mcp_state,
            mcp_active: true,
            resume_context_loaded,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_runtime_entry() -> Self {
        Self {
            runtime: None,
            plugin_registry: PluginRegistry::default(),
            plugins_active: false,
            mcp_state: None,
            mcp_active: false,
            resume_context_loaded: false,
        }
    }

    pub(crate) fn with_hook_abort_signal(
        mut self,
        hook_abort_signal: runtime::HookAbortSignal,
    ) -> Self {
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before installing hook abort signal");
        self.runtime = Some(runtime.with_hook_abort_signal(hook_abort_signal));
        self
    }

    pub(crate) fn install_turn_control(
        &mut self,
        cancellation_token: runtime::CancellationToken,
        hook_abort_signal: runtime::HookAbortSignal,
    ) {
        self.runtime_mut()
            .install_turn_control(cancellation_token, hook_abort_signal);
    }

    pub(crate) fn shutdown_plugins(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.plugins_active {
            self.plugin_registry.shutdown()?;
            self.plugins_active = false;
        }
        Ok(())
    }

    pub(crate) fn shutdown_mcp(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.mcp_active {
            if let Some(mcp_state) = &self.mcp_state {
                mcp_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .shutdown()?;
            }
            self.mcp_active = false;
        }
        Ok(())
    }

    pub(crate) fn resume_context_loaded(&self) -> bool {
        self.resume_context_loaded
    }

    pub(crate) fn set_resume_context_loaded(&mut self, loaded: bool) {
        self.resume_context_loaded = loaded;
    }

    pub(crate) fn cowd_bus(&self) -> Option<&runtime::CowdEventBus> {
        self.runtime_ref().cowd_bus()
    }

    pub(crate) fn set_context_profile(&self, profile: runtime::ContextProfile) {
        self.runtime_ref().set_context_profile(profile);
    }

    pub(crate) fn inject_resume_context(&self, packet: runtime::ResumeContextPacket) {
        self.runtime_ref().inject_resume_context(packet);
    }

    pub(crate) async fn run_turn_async(
        &mut self,
        content: &str,
        prompter: &runtime::permissions::SharedPrompter,
    ) -> Result<runtime::TurnSummary, runtime::RuntimeError> {
        self.runtime_mut().run_turn_async(content, prompter).await
    }

    pub(crate) fn session(&self) -> runtime::Session {
        self.runtime_ref().session()
    }

    pub(crate) fn compact_active_session(
        &mut self,
        config: runtime::CompactionConfig,
    ) -> (runtime::CompactionResult, runtime::Session) {
        self.runtime_mut().compact_active_session(config)
    }

    pub(crate) fn active_session_stats_session(&self) -> runtime::Session {
        self.runtime_ref().active_session_stats_session()
    }

    pub(crate) async fn update_session_model(&mut self, model: &str) {
        self.runtime_mut().update_session_model(model).await;
    }

    pub(crate) fn last_context_envelope(&self) -> Option<runtime::ContextEnvelope> {
        self.runtime_ref().last_context_envelope()
    }

    pub(crate) fn take_collaboration_result(&self) -> Option<runtime::CollaborationContextResult> {
        self.runtime_ref().take_collaboration_result()
    }

    fn runtime_ref(&self) -> &runtime::StandardRuntimeHost<GatewayToolExecutor> {
        self.runtime
            .as_ref()
            .expect("runtime should exist while gateway runtime entry is alive")
    }

    fn runtime_mut(&mut self) -> &mut runtime::StandardRuntimeHost<GatewayToolExecutor> {
        self.runtime
            .as_mut()
            .expect("runtime should exist while gateway runtime entry is alive")
    }
}

impl Drop for GatewayRuntimeEntry {
    fn drop(&mut self) {
        let _ = self.shutdown_mcp();
        let _ = self.shutdown_plugins();
    }
}
