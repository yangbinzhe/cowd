use std::sync::Arc;

use runtime::{ConversationRuntime, PermissionMode, Session};

use crate::gateway_tool_executor::GatewayToolExecutor;
use crate::runtime_bootstrap::RuntimeBootstrapState;
use crate::runtime_entry::GatewayRuntimeEntry;
use crate::{
    filter_tool_specs, inject_auto_resume_context, permission_policy,
    session_db_resume_context_packet, workspace_context_item, AllowedToolSet,
};

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_runtime(
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
    build_runtime_with_bootstrap_state(
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
pub(crate) fn build_runtime_with_session_store(
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
    build_runtime_with_bootstrap_state(
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
pub(crate) fn build_runtime_with_bootstrap_state(
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
    let subagent_model = model.clone();
    let subagent_tool_executor = std::sync::Arc::new(GatewayToolExecutor::new(
        allowed_tools.clone(),
        emit_output,
        tool_registry.clone(),
        mcp_state.clone(),
    ));
    let mut runtime = ConversationRuntime::new_with_features(
        session,
        runtime::ProviderRuntimeClient::new(
            model,
            filter_tool_specs(&tool_registry, allowed_tools.as_ref()),
        )
        .map_err(std::io::Error::other)?
        .with_emit_output(emit_output)
        .with_stream_callback(stream_callback.clone()),
        subagent_tool_executor.clone(),
        policy,
        system_prompt,
        &feature_config,
    );
    runtime = runtime.with_model_context_window(model_ctx);
    if let Some(store) = session_store {
        runtime = runtime.with_session_store(store);
    }
    if let Some(ref tx) = stream_callback {
        let _ = tx.try_send(runtime::CowdEvent::ContextWindow(model_ctx as u64));
    }
    if let Some(callback) = tool_callback {
        runtime = runtime.with_tool_callback(callback);
    }
    if emit_output {
        runtime = runtime.with_hook_progress_reporter(Box::new(GatewayHookProgressReporter));
    }
    let cowd_bus = runtime::CowdEventBus::new();
    runtime = runtime.with_cowd_event_bus(cowd_bus);
    runtime.push_external_context_item(workspace_item);
    let resume_context_loaded =
        inject_auto_resume_context(&runtime, session_resume_packet, session_id);
    {
        let allowed_tools_clone = allowed_tools.clone();
        let tool_registry_clone = tool_registry.clone();
        let executor = runtime::agent::ProductionExecutor::new(
            move || {
                runtime::ProviderRuntimeClient::new(
                    subagent_model.clone(),
                    filter_tool_specs(&tool_registry_clone, allowed_tools_clone.as_ref()),
                )
                .expect("sub-agent API client creation failed")
            },
            subagent_tool_executor.clone(),
        );
        let executor_arc = std::sync::Arc::new(executor);
        runtime = runtime.with_collaboration(runtime::agent_collaboration::new_boxed(
            executor_arc.clone(),
        ));
        let jps_pipeline = runtime::joint_problem_solving::new_boxed::<
            runtime::agent::ProductionExecutor<runtime::ProviderRuntimeClient, GatewayToolExecutor>,
        >(executor_arc);
        runtime = runtime.with_jps_pipeline(jps_pipeline);
    }
    Ok(GatewayRuntimeEntry::new(
        runtime,
        plugin_registry,
        mcp_state,
        resume_context_loaded,
    ))
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
