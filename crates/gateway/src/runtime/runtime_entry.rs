use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use plugins::PluginRegistry;
use runtime::{ConversationRuntime, ProviderRuntimeClient};

use crate::gateway_tool_executor::GatewayToolExecutor;
use crate::runtime_bootstrap::RuntimeMcpState;

pub(crate) struct GatewayRuntimeEntry {
    runtime: Option<ConversationRuntime<ProviderRuntimeClient, GatewayToolExecutor>>,
    plugin_registry: PluginRegistry,
    plugins_active: bool,
    mcp_state: Option<Arc<Mutex<RuntimeMcpState>>>,
    mcp_active: bool,
    resume_context_loaded: bool,
}

impl GatewayRuntimeEntry {
    pub(crate) fn new(
        runtime: ConversationRuntime<ProviderRuntimeClient, GatewayToolExecutor>,
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
        let runtime = self
            .runtime
            .take()
            .expect("runtime should exist before installing turn control");
        self.runtime = Some(
            runtime
                .with_cancellation_token(cancellation_token)
                .with_hook_abort_signal(hook_abort_signal),
        );
    }

    pub(crate) fn api_client_mut(&mut self) -> Option<&mut ProviderRuntimeClient> {
        self.runtime
            .as_mut()
            .map(|runtime| runtime.api_client_mut())
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
}

impl Deref for GatewayRuntimeEntry {
    type Target = ConversationRuntime<ProviderRuntimeClient, GatewayToolExecutor>;

    fn deref(&self) -> &Self::Target {
        self.runtime
            .as_ref()
            .expect("runtime should exist while gateway runtime entry is alive")
    }
}

impl DerefMut for GatewayRuntimeEntry {
    fn deref_mut(&mut self) -> &mut Self::Target {
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
