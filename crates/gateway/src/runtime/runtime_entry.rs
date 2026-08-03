use plugins::PluginRegistry;

use crate::gateway_tool_executor::GatewayToolExecutor;

pub(crate) struct GatewayRuntimeEntry {
    runtime: Option<runtime::StandardRuntimeHost<GatewayToolExecutor>>,
    plugin_registry: PluginRegistry,
    plugins_active: bool,
    resume_context_loaded: bool,
}

impl GatewayRuntimeEntry {
    pub(crate) fn new(
        runtime: runtime::StandardRuntimeHost<GatewayToolExecutor>,
        plugin_registry: PluginRegistry,
        resume_context_loaded: bool,
    ) -> Self {
        Self {
            runtime: Some(runtime),
            plugin_registry,
            plugins_active: true,
            resume_context_loaded,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_runtime_entry() -> Self {
        Self {
            runtime: None,
            plugin_registry: PluginRegistry::default(),
            plugins_active: false,
            resume_context_loaded: false,
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "the private runtime slot is empty only in test fixtures or while an exclusive mutable turn owns it"
    )]
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

    pub(crate) fn resume_context_loaded(&self) -> bool {
        self.resume_context_loaded
    }

    pub(crate) fn set_resume_context_loaded(&mut self, loaded: bool) {
        self.resume_context_loaded = loaded;
    }

    /// The conversation host is moved out only while a provider-backed turn
    /// owns it.  RuntimeService keeps the observable/cancellation carrier
    /// outside this mutex so read paths never wait for that turn.
    pub(crate) fn turn_is_owned(&self) -> bool {
        self.runtime.is_none()
    }

    pub(crate) fn take_runtime_for_turn(
        &mut self,
    ) -> Result<runtime::StandardRuntimeHost<GatewayToolExecutor>, runtime::RuntimeError> {
        self.runtime.take().ok_or_else(|| {
            runtime::RuntimeError::new("session runtime is already executing a turn")
        })
    }

    pub(crate) fn restore_runtime_after_turn(
        &mut self,
        runtime: runtime::StandardRuntimeHost<GatewayToolExecutor>,
    ) {
        debug_assert!(
            self.runtime.is_none(),
            "runtime must be empty while turn owner returns it"
        );
        self.runtime = Some(runtime);
    }

    pub(crate) fn cowd_bus(&self) -> Option<&runtime::CowdEventBus> {
        self.runtime_ref().cowd_bus()
    }

    pub(crate) fn session_input_stream(&self) -> runtime::SessionInputStream {
        self.runtime_ref().session_input_stream()
    }

    pub(crate) fn set_context_profile(&self, profile: runtime::ContextProfile) {
        self.runtime_ref().set_context_profile(profile);
    }

    pub(crate) fn set_permission_mode(&mut self, mode: runtime::PermissionMode) {
        self.runtime_mut().set_permission_mode(mode);
    }

    pub(crate) fn inject_resume_context(&self, packet: runtime::ResumeContextPacket) {
        self.runtime_ref().inject_resume_context(packet);
    }

    pub(crate) fn replace_external_context_sources(
        &self,
        sources: &[runtime::ContextSourceKind],
        items: Vec<runtime::ContextItem>,
    ) {
        self.runtime_ref()
            .replace_external_context_sources(sources, items);
    }

    pub(crate) fn last_context_turn_report(
        &self,
    ) -> Option<harness_contract::context::ContextTurnReport> {
        self.runtime_ref().last_context_turn_report()
    }

    pub(crate) async fn submit_turn(
        &mut self,
        content: &str,
        prompter: &runtime::permissions::SharedPrompter,
    ) -> Result<runtime::TurnSummary, runtime::RuntimeError> {
        self.runtime_mut().submit_turn(content, prompter).await
    }

    pub(crate) async fn submit_ingress_turn(
        &mut self,
        content: &str,
        prompter: &runtime::permissions::SharedPrompter,
        ingress: runtime::TurnIngressRef,
    ) -> Result<runtime::TurnSummary, runtime::RuntimeError> {
        self.runtime_mut()
            .submit_ingress_turn(content, prompter, ingress)
            .await
    }

    pub(crate) async fn append_external_message(
        &self,
        message: runtime::ConversationMessage,
    ) -> Result<(), runtime::RuntimeError> {
        self.runtime_ref().append_external_message(message).await
    }

    pub(crate) async fn session_snapshot(&self) -> runtime::Session {
        self.runtime_ref().session_snapshot().await
    }

    pub(crate) async fn session_head(&self) -> runtime::SessionReadHead {
        self.runtime_ref().session_head().await
    }

    pub(crate) async fn compact_active_session(
        &mut self,
    ) -> Result<(Option<runtime::AutoCompactionEvent>, runtime::Session), runtime::RuntimeError>
    {
        self.runtime_mut().compact_active_session().await
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

    #[allow(
        clippy::expect_used,
        reason = "production entries are constructed with a runtime; None is reserved for test fixtures"
    )]
    fn runtime_ref(&self) -> &runtime::StandardRuntimeHost<GatewayToolExecutor> {
        self.runtime
            .as_ref()
            .expect("runtime should exist while gateway runtime entry is alive")
    }

    #[allow(
        clippy::expect_used,
        reason = "production entries are constructed with a runtime; None is reserved for test fixtures"
    )]
    fn runtime_mut(&mut self) -> &mut runtime::StandardRuntimeHost<GatewayToolExecutor> {
        self.runtime
            .as_mut()
            .expect("runtime should exist while gateway runtime entry is alive")
    }
}

impl Drop for GatewayRuntimeEntry {
    fn drop(&mut self) {
        let _ = self.shutdown_plugins();
    }
}
