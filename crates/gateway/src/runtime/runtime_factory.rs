use std::sync::Arc;

use runtime::{PermissionMode, Session};

use crate::gateway_tool_executor::GatewayToolExecutor;
use crate::runtime_bootstrap::RuntimeBootstrapState;
use crate::runtime_entry::GatewayRuntimeEntry;
use crate::{
    filter_tool_specs, inject_auto_resume_context, permission_policy,
    session_db_resume_context_packet, workspace_context_item, AllowedToolSet,
};

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_runtime_entry(
    session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
) -> Result<GatewayRuntimeEntry, Box<dyn std::error::Error>> {
    let runtime_plugin_state = crate::runtime_bootstrap::assemble_runtime_state()?;
    create_runtime_entry_with_bootstrap_state(
        None,
        session,
        session_id,
        model,
        system_prompt,
        enable_tools,
        emit_output,
        allowed_tools,
        permission_mode,
        tool_callback,
        stream_callback,
        runtime_plugin_state,
    )
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_runtime_entry_with_session_store(
    session_store: Arc<memory::session_store::UnifiedSessionStore>,
    session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
) -> Result<GatewayRuntimeEntry, Box<dyn std::error::Error>> {
    let runtime_plugin_state = crate::runtime_bootstrap::assemble_runtime_state()?;
    create_runtime_entry_with_bootstrap_state(
        Some(session_store),
        session,
        session_id,
        model,
        system_prompt,
        enable_tools,
        emit_output,
        allowed_tools,
        permission_mode,
        tool_callback,
        stream_callback,
        runtime_plugin_state,
    )
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_runtime_entry_with_bootstrap_state(
    session_store: Option<Arc<memory::session_store::UnifiedSessionStore>>,
    mut session: Session,
    session_id: &str,
    model: String,
    system_prompt: Vec<String>,
    _enable_tools: bool,
    emit_output: bool,
    allowed_tools: Option<AllowedToolSet>,
    permission_mode: PermissionMode,
    tool_callback: Option<std::sync::Arc<dyn runtime::ToolCallback>>,
    stream_callback: Option<std::sync::mpsc::SyncSender<runtime::CowdEvent>>,
    runtime_plugin_state: RuntimeBootstrapState,
) -> Result<GatewayRuntimeEntry, Box<dyn std::error::Error>> {
    if session.model.is_none() {
        session.model = Some(model.clone());
    }
    let session_resume_packet = session_db_resume_context_packet(&session);
    let RuntimeBootstrapState {
        feature_config,
        tool_registry,
        plugin_registry,
        mcp_state,
    } = runtime_plugin_state;
    plugin_registry.initialize()?;
    let policy = permission_policy(permission_mode, &feature_config, &tool_registry)
        .map_err(std::io::Error::other)?;
    let overrides = feature_config.model_context_windows();
    let model_ctx = runtime::model_context_window_with_overrides(&model, Some(&overrides));
    let workspace_item = workspace_context_item(&session, model_ctx);
    let tool_definitions = filter_tool_specs(&tool_registry, allowed_tools.as_ref());
    let subagent_model = model.clone();
    let subagent_tool_definitions = tool_definitions.clone();
    let tool_executor = std::sync::Arc::new(GatewayToolExecutor::new(
        allowed_tools.clone(),
        emit_output,
        tool_registry.clone(),
        mcp_state.clone(),
    ));
    let runtime = runtime::StandardRuntimeHost::new(runtime::StandardRuntimeHostConfig {
        session,
        model: model.clone(),
        tool_definitions,
        tool_executor: tool_executor.clone(),
        permission_policy: policy,
        system_prompt,
        feature_config,
        emit_output,
        stream_callback: stream_callback.clone(),
        tool_callback,
        model_context_window: Some(model_ctx),
        session_store,
        hook_progress_reporter: emit_output.then(|| {
            Box::new(GatewayHookProgressReporter) as Box<dyn runtime::HookProgressReporter>
        }),
        external_context_items: vec![workspace_item],
        enable_collaboration: true,
        subagent_model,
        subagent_tool_definitions,
        subagent_tool_executor: tool_executor,
    })
    .map_err(std::io::Error::other)?;
    if let Some(ref tx) = stream_callback {
        let _ = tx.try_send(runtime::CowdEvent::ContextWindow(model_ctx as u64));
    }
    let mut entry = GatewayRuntimeEntry::new(runtime, plugin_registry, mcp_state, false);
    let resume_context_loaded =
        inject_auto_resume_context(&entry, session_resume_packet, session_id);
    entry.set_resume_context_loaded(resume_context_loaded);
    Ok(entry)
}

struct GatewayHookProgressReporter;

impl runtime::HookProgressReporter for GatewayHookProgressReporter {
    fn on_event(&mut self, event: &runtime::HookProgressEvent) {
        match event {
            runtime::HookProgressEvent::Started {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
            runtime::HookProgressEvent::Completed {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook done {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
            runtime::HookProgressEvent::Cancelled {
                event,
                tool_name,
                command,
            } => tracing::info!(
                "[hook cancelled {event_name}] {tool_name}: {command}",
                event_name = event.as_str()
            ),
        }
    }
}
