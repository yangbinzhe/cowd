use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use harness_contract::projection::{ExecutionCommandKind, ExecutionCommandRequest};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use crate::app::{PendingResource, SystemNoticeKind};
use crate::context_tokens::ContextWorkspaceEntry;
use crate::events::CowdEventSender;
use crate::gateway_client::{default_auth_token, GatewayApiClient};
use crate::runtime_control_store::{
    MfgBacklink, MfgBacklinkKind, MfgItemSummary, MfgOperationsSnapshot, MfgOperationsState,
    MfgPaginationState,
};
use crate::state::{ProcessedKey, TuiState};
use crate::{config_migration, cowd_event_channel, error_recovery, CowdEvent, FileEntry};

static SHARED_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct GatewayTuiConfig {
    pub model: Option<String>,
    pub session_id: String,
    pub yolo_mode: bool,
    pub startup_banner: String,
    pub connected_line: String,
}

impl GatewayTuiConfig {
    pub fn from_env_args() -> Self {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let model =
            arg_value(&args, &["--model", "-m"]).or_else(|| std::env::var("COWD_MODEL").ok());
        let session_id = arg_value(&args, &["--resume", "--session", "--session-id", "-s"])
            .unwrap_or_else(|| format!("tui-{}", uuid::Uuid::new_v4()));
        let yolo_mode = args.iter().any(|arg| {
            matches!(
                arg.as_str(),
                "--yolo" | "--dangerously-skip-permissions" | "--danger-full-access"
            )
        });
        let display_model = model.clone().unwrap_or_else(|| "default".to_string());
        Self {
            startup_banner: format_startup_banner(&display_model, yolo_mode, &session_id),
            connected_line: format_connected_line(&display_model),
            model,
            session_id,
            yolo_mode,
        }
    }
}

pub fn terminal_entry() -> Result<(), Box<dyn std::error::Error>> {
    run_gateway_tui(GatewayTuiConfig::from_env_args())
}

pub fn run_gateway_tui(config: GatewayTuiConfig) -> Result<(), Box<dyn std::error::Error>> {
    error_recovery::install_tui_panic_hook();
    let migration_report = config_migration::run_startup_migration();
    let runtime = initialize_shared_rt()?;

    let accessibility_enabled = std::env::var("COWD_TUI_ACCESSIBILITY")
        .map(|value| value == "1" || value == "true")
        .unwrap_or(false);
    let raw_mode_enabled = std::env::var("COWD_TUI_SKIP_RAW_MODE").is_err();
    let mouse_capture_enabled = std::env::var("COWD_TUI_MOUSE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);

    if raw_mode_enabled {
        enable_raw_mode()?;
    }
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    if mouse_capture_enabled {
        execute!(stdout, EnableMouseCapture)?;
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let (tui_tx, mut tui_rx) = cowd_event_channel();
    let session_id = config.session_id.clone();
    let display_model = config
        .model
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let mut state = TuiState::new(&display_model, &session_id);
    state.app.yolo_mode = config.yolo_mode;
    state.add_system_notice(SystemNoticeKind::Info, &config.startup_banner);
    state.add_system_notice(SystemNoticeKind::Info, &config.connected_line);

    let mut gateway_lease_owner: Option<String> = None;
    let gateway_client = GatewayApiClient::ensure_running_with_retry(default_auth_token())?
        .ok_or_else(|| {
            "Gateway API is required for TUI; start `cowd gateway run` or allow TUI autostart"
                .to_string()
        })?;
    let gateway_session_ids = attach_gateway_session(
        runtime,
        &gateway_client,
        &tui_tx,
        &mut state,
        &config,
        &mut gateway_lease_owner,
    )?;
    start_pending_mfg_refresh(&mut state, &gateway_client, &tui_tx);

    terminal.draw(|frame| state.render(frame))?;

    state.set_memory_projection_available(true);
    state.set_active_sessions_count(1);
    if accessibility_enabled {
        state.accessibility = crate::accessibility::AccessibilityMode::full();
        let high_contrast_theme = crate::accessibility::high_contrast_theme(true);
        state.theme_engine = crate::theme::ThemeEngine::new(high_contrast_theme);
    }
    if !migration_report.contains("nothing to migrate") {
        state.add_system_notice(SystemNoticeKind::Info, &migration_report);
    }
    match list_workspace_files(runtime, &gateway_client) {
        Ok(files) => {
            state.prompt.set_workspace_entries(
                files
                    .iter()
                    .map(|entry| ContextWorkspaceEntry::new(entry.name.clone(), entry.is_dir)),
            );
            state.app.file_entries = files;
        }
        Err(error) => {
            state.add_system_notice(
                SystemNoticeKind::Warning,
                &format!("Gateway workspace projection unavailable: {error}"),
            );
        }
    }
    send_session_list(&tui_tx, gateway_session_ids, &session_id);

    let startup_ready = true;
    let res = runtime.block_on(async {
        let mut reader = crossterm::event::EventStream::new();
        let mut execution_projection_stream = None;
        loop {
            tokio::select! {
                Some(Ok(event)) = reader.next() => {
                    if let Event::Mouse(mouse) = &event {
                        if matches!(mouse.kind, crossterm::event::MouseEventKind::ScrollDown) {
                            state.handle_mouse_scroll_at(true, mouse.column, mouse.row);
                            continue;
                        }
                        if matches!(mouse.kind, crossterm::event::MouseEventKind::ScrollUp) {
                            state.handle_mouse_scroll_at(false, mouse.column, mouse.row);
                            continue;
                        }
                    }
                    if let Event::Paste(text) = event {
                        // Bracketed paste is one canonical composer edit. Do
                        // not replay it as individual key events, which would
                        // break undo and can split IME/Unicode transactions.
                        state.process_paste(&text);
                        continue;
                    }
                    if let Event::Key(key) = event {
                        if key.kind == KeyEventKind::Press {
                            // Keyboard input is a render cause even when it
                            // only changes the composer buffer (which is not
                            // part of the timeline version counter).
                            state.app.mark_dirty();
                            if state.picker_active {
                                state.open_session_picker_dialog();
                            }
                            if state.approval.is_some() && state.dialog_manager.is_empty() {
                                state.open_approval_dialog();
                            }

                            match state.process_raw_key(key) {
                                ProcessedKey::Submit(text) => {
                                    if text.is_empty() {
                                        continue;
                                    }
                                    if matches!(text.as_str(), "/exit" | "/quit") {
                                        break;
                                    }
                                    if let Some(path) = attach_path_from_command(&text) {
                                        match gateway_client.upload_resource_path(path, &session_id).await {
                                            Ok(value) => {
                                                if let Some(resource) = value.get("resource") {
                                                    let id = resource
                                                        .get("id")
                                                        .and_then(serde_json::Value::as_str)
                                                        .unwrap_or_default()
                                                        .to_string();
                                                    let label = resource
                                                        .get("original_name")
                                                        .and_then(serde_json::Value::as_str)
                                                        .map(str::to_string)
                                                        .unwrap_or_else(|| path.display().to_string());
                                                    let kind = resource
                                                        .get("kind")
                                                        .and_then(serde_json::Value::as_str)
                                                        .unwrap_or("resource")
                                                        .to_string();
                                                    if !id.is_empty() {
                                                        state.app.pending_resources.push(PendingResource {
                                                            id: id.clone(),
                                                            label: label.clone(),
                                                            kind: kind.clone(),
                                                        });
                                                        state.add_system_notice(
                                                            SystemNoticeKind::Info,
                                                            &format!("Attached resource {label} ({kind}) as resource://{id}"),
                                                        );
                                                    }
                                                }
                                            }
                                            Err(err) => {
                                                state.add_system_notice(
                                                    SystemNoticeKind::Error,
                                                    &format!("Attach failed: {err}"),
                                                );
                                            }
                                        }
                                        continue;
                                    }
                                    if let Some(command) = execution_command_from_input(&text) {
                                        dispatch_execution_projection_command(
                                            &gateway_client,
                                            &tui_tx,
                                            &state,
                                            command,
                                        );
                                        continue;
                                    }
                                    if text.starts_with('/') {
                                        if let Some(input_id) = queue_cancel_command(&text) {
                                            dispatch_pending_input_cancel(
                                                &gateway_client,
                                                &tui_tx,
                                                &session_id,
                                                input_id,
                                            );
                                            continue;
                                        }
                                        if let Some(input_id) = queue_edit_command(&text) {
                                            if let Some(input) = state
                                                .app
                                                .pending_inputs
                                                .iter()
                                                .find(|input| input.input_id == input_id)
                                            {
                                                state.app.input.set_text(&input.content_preview);
                                                dispatch_pending_input_cancel(
                                                    &gateway_client,
                                                    &tui_tx,
                                                    &session_id,
                                                    input_id,
                                                );
                                                state.add_system_notice(
                                                    SystemNoticeKind::Info,
                                                    "Queued follow-up restored to composer; edit it and submit to replace the canonical input.",
                                                );
                                            } else {
                                                state.add_system_notice(
                                                    SystemNoticeKind::Warning,
                                                    "Queued input was not found locally; refresh or use its full input id.",
                                                );
                                            }
                                            continue;
                                        }
                                        dispatch_gateway_slash(
                                            &gateway_client,
                                            &tui_tx,
                                            &mut state,
                                            &session_id,
                                            &text,
                                        );
                                        continue;
                                    }
                                    state.add_message("user", &text);
                                    // The admission request is asynchronous. Mark the turn as
                                    // active before it leaves the TUI so the user immediately
                                    // sees a running state instead of an apparently idle screen
                                    // while Gateway persists/routs the ingress.
                                    state.apply_event(CowdEvent::TurnStarted);
                                    let resource_ids = state
                                        .app
                                        .pending_resources
                                        .iter()
                                        .map(|resource| resource.id.clone())
                                        .collect::<Vec<_>>();
                                    dispatch_gateway_message(
                                        &gateway_client,
                                        &tui_tx,
                                        &session_id,
                                        text,
                                        resource_ids,
                                    );
                                    continue;
                                }
                                ProcessedKey::Exit => break,
                                ProcessedKey::Cancel => {
                                    dispatch_gateway_cancel(
                                        &gateway_client,
                                        &tui_tx,
                                        &session_id,
                                    );
                                }
                                ProcessedKey::Nothing => {}
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(16)) => {
                    drain_cowd_events_state(
                        &mut tui_rx,
                        &mut state,
                        &gateway_client,
                        &tui_tx,
                        &mut execution_projection_stream,
                    ).await;
                    start_pending_mfg_refresh(&mut state, &gateway_client, &tui_tx);
                    state.update_startup_phase(startup_ready);
                    if state.app.turn_is_active() {
                        state.tick();
                    }
                }
            }
            // Do not redraw a quiescent terminal at a fixed 16 ms cadence.
            // Active runs still tick for elapsed/status feedback; input and
            // state changes advance `msg_version` above.
            if state.app.last_drawn_version != state.app.msg_version || state.app.turn_is_active() {
                terminal.draw(|frame| state.render(frame))?;
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    });

    if gateway_lease_owner.is_some() {
        let _ = runtime.block_on(gateway_client.release_runtime_session_lease(&session_id));
    }
    let _ = runtime.block_on(gateway_client.detach_session(&session_id, "tui"));
    if raw_mode_enabled {
        disable_raw_mode()?;
    }
    if mouse_capture_enabled {
        execute!(terminal.backend_mut(), DisableMouseCapture)?;
    }
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    res
}

fn attach_gateway_session(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    event_tx: &crate::events::CowdEventSender,
    state: &mut TuiState,
    config: &GatewayTuiConfig,
    gateway_lease_owner: &mut Option<String>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let status = runtime
        .block_on(gateway_client.status())
        .map_err(|err| format!("Gateway API is required for TUI: {err}"))?;
    state.app.server_running = true;
    state.app.active_api_sessions = status
        .get("active_sessions")
        .and_then(serde_json::Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or_default();
    state.app.server_uptime_secs = status
        .get("uptime_secs")
        .and_then(serde_json::Value::as_u64);

    let active_api_sessions = state.app.active_api_sessions;
    let server_uptime_secs = state.app.server_uptime_secs.unwrap_or_default();
    state.add_system_notice(
        SystemNoticeKind::Info,
        &format!("Gateway API connected: {active_api_sessions} active sessions, uptime {server_uptime_secs}s"),
    );

    let ensured = runtime
        .block_on(
            gateway_client
                .ensure_session(&config.session_id, config.model.as_deref().unwrap_or("")),
        )
        .map_err(|err| format!("Gateway session attach failed: {err}"))?;
    state.app.active_api_sessions = ensured
        .get("active_sessions")
        .and_then(serde_json::Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(state.app.active_api_sessions);
    let ensured_session_id = ensured
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&config.session_id)
        .to_string();
    let action = if ensured
        .get("created")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        "created"
    } else {
        "attached"
    };
    state.add_system_notice(
        SystemNoticeKind::Info,
        &format!("Gateway session {action}: {ensured_session_id}"),
    );

    match runtime.block_on(gateway_client.attach_session(
        &ensured_session_id,
        "tui",
        Some("writer"),
    )) {
        Ok(attached) => {
            state.add_system_notice(
                SystemNoticeKind::Info,
                &format!(
                    "Gateway lifecycle attached: state={}, seq={}",
                    attached
                        .pointer("/event/state")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("attached"),
                    attached
                        .pointer("/event/sequence")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default()
                ),
            );
            match runtime.block_on(gateway_client.replay_session(&ensured_session_id, 0, 100)) {
                Ok(replay) => state.add_system_notice(
                    SystemNoticeKind::Info,
                    &format!(
                        "Gateway replay ready: total={}, next_seq={}",
                        replay
                            .get("total")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or_default(),
                        replay
                            .get("next_sequence")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or_default()
                    ),
                ),
                Err(err) => state.add_system_notice(
                    SystemNoticeKind::Error,
                    &format!("Gateway replay unavailable: {err}"),
                ),
            }
        }
        Err(err) => state.add_system_notice(
            SystemNoticeKind::Error,
            &format!("Gateway lifecycle attach unavailable: {err}"),
        ),
    }

    match runtime.block_on(
        gateway_client.acquire_runtime_session_lease(&ensured_session_id, "collaborative"),
    ) {
        Ok(lease) => {
            *gateway_lease_owner = lease
                .get("owner")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned);
            state.app.gateway_lease_owner = gateway_lease_owner.clone();
            state.app.gateway_lease_mode = lease
                .get("mode")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned);
            state.add_system_notice(
                SystemNoticeKind::Info,
                &format!(
                    "Gateway session lease acquired: owner={}, mode={}",
                    lease
                        .get("owner")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown"),
                    lease
                        .get("mode")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                ),
            );
        }
        Err(err) => state.add_system_notice(
            SystemNoticeKind::Error,
            &format!("Gateway session lease unavailable: {err}"),
        ),
    }

    let snapshot = runtime.block_on(
        crate::runtime_control_store::refresh_runtime_control_snapshot(
            Some(gateway_client),
            Some(&config.session_id),
        ),
    );
    let gateway_session_ids = snapshot.session_ids.clone();
    let readiness = snapshot.runtime_readiness.clone();
    let components = snapshot.runtime_components.unwrap_or_default();
    let degraded_reasons = snapshot.degraded_reasons.clone();
    snapshot.apply_to_app(&mut state.app);
    match runtime.block_on(gateway_client.session_projection(&config.session_id)) {
        Ok(projection) => {
            state.app.apply_run_projection(projection);
            state.add_system_notice(
                SystemNoticeKind::Info,
                "Gateway session run projection loaded",
            );
        }
        Err(err) => state.add_system_notice(
            SystemNoticeKind::Error,
            &format!("Gateway session run projection unavailable: {err}"),
        ),
    }
    match runtime.block_on(gateway_client.session_input_projection(&config.session_id)) {
        Ok(projection) => state.app.apply_session_input_projection(projection),
        Err(err) => state.add_system_notice(
            SystemNoticeKind::Warning,
            &format!("Gateway queued-input projection unavailable: {err}"),
        ),
    }
    if let Some(readiness) = readiness {
        state.add_system_notice(
            SystemNoticeKind::Info,
            &format!("Gateway runtime projection connected: readiness={readiness}, components={components}"),
        );
    }
    for reason in degraded_reasons.into_iter().take(3) {
        state.add_system_notice(
            SystemNoticeKind::Warning,
            &format!("Gateway projection degraded: {reason}"),
        );
    }

    let event_client = gateway_client.clone();
    let event_session_id = config.session_id.clone();
    let event_tx = event_tx.clone();
    let _event_bridge = runtime.spawn(async move {
        let mut after_commit_cursor = None;
        let mut retry_delay = Duration::from_millis(250);
        loop {
            match event_client
                .subscribe_session_events(&event_session_id, event_tx.clone(), after_commit_cursor)
                .await
            {
                Ok(cursor) => {
                    after_commit_cursor = cursor.or(after_commit_cursor);
                    let _ = event_tx.send(CowdEvent::Warning {
                        message: "Gateway event stream ended; resuming from durable cursor"
                            .to_string(),
                    });
                }
                Err(err) => {
                    let _ = event_tx.send(CowdEvent::Warning {
                        message: format!("Gateway event stream interrupted: {err}; retrying"),
                    });
                }
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
        }
    });
    state.add_system_notice(
        SystemNoticeKind::Info,
        "Gateway event stream subscribed for this session",
    );
    Ok(gateway_session_ids)
}

fn send_session_list(
    tx: &crate::events::CowdEventSender,
    gateway_session_ids: Vec<String>,
    session_id: &str,
) {
    let mut session_list: Vec<(String, String, String)> = Vec::new();
    for id in gateway_session_ids
        .into_iter()
        .chain(std::iter::once(session_id.to_string()))
    {
        if session_list.iter().any(|(existing, _, _)| existing == &id) {
            continue;
        }
        let short = id[..id.len().min(8)].to_string();
        session_list.push((id, format!("gateway [{short}]"), "live".to_string()));
    }
    let _ = tx.send(CowdEvent::SessionList {
        sessions: session_list,
    });
}

fn dispatch_gateway_slash(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    state: &mut TuiState,
    session_id: &str,
    text: &str,
) {
    let cmd_name = text
        .strip_prefix('/')
        .and_then(|value| value.split_whitespace().next())
        .unwrap_or(text)
        .to_string();
    let command = cmd_name.trim_start_matches('/').to_string();
    let command_input = text.to_string();
    let command_session_id = session_id.to_string();
    let slash_client = gateway_client.clone();
    let command_tx = tx.clone();
    spawn_tui_task(tx, async move {
        let args = serde_json::json!({
            "input": command_input,
            "session_id": command_session_id,
            "surface": "tui",
        });
        match slash_client.slash_dispatch(&command, args).await {
            Ok(receipt) => {
                let status = receipt
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("complete");
                let dispatch = receipt
                    .get("data")
                    .and_then(|data| data.get("dispatch"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("gateway");
                let _ = command_tx.send(CowdEvent::Warning {
                    message: format!("Gateway slash /{command} {status} via {dispatch}"),
                });
            }
            Err(err) => {
                let _ = command_tx.send(CowdEvent::TurnError {
                    error: format!("Gateway slash /{command} failed: {err}"),
                });
            }
        }
    });
    state.add_slash_output(&cmd_name, "Slash dispatched to Gateway");
    state.open_surface_for_slash_result(&cmd_name);
}

fn dispatch_gateway_message(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    session_id: &str,
    text: String,
    resource_ids: Vec<String>,
) {
    let event_client = gateway_client.clone();
    let event_session_id = session_id.to_string();
    let event_tx = tx.clone();
    spawn_tui_task(tx, async move {
        match event_client
            .send_message_with_resources(&event_session_id, &text, &resource_ids)
            .await
        {
            Ok(value) => {
                if let Some(projection) = value.get("input_projection") {
                    let _ = event_tx.send(CowdEvent::SessionInputProjection {
                        projection: projection.clone(),
                    });
                }
                if !resource_ids.is_empty() {
                    let _ = event_tx.send(CowdEvent::ResourcesCommitted {
                        ids: resource_ids.clone(),
                    });
                }
                if let Some(graph_id) = value
                    .get("execution")
                    .and_then(|execution| execution.get("graph_id"))
                    .and_then(serde_json::Value::as_str)
                    .filter(|graph_id| !graph_id.trim().is_empty())
                {
                    let _ = event_tx.send(CowdEvent::ExecutionGraphSummary {
                        summary: crate::RuntimeExecutionGraphSummary {
                            graph_id: Some(graph_id.to_string()),
                            board_id: None,
                            status: "running".to_string(),
                            agent_tasks: 0,
                            child_executions: 0,
                            memory_candidates: 0,
                            conflicts: 0,
                            completion_rate: Some(0.0),
                            synthesis_lift: None,
                            complementarity_score: None,
                        },
                    });
                }
                let mode = value
                    .get("mode")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                if mode == "started_new_turn" {
                    let turn_id = value
                        .get("turn")
                        .and_then(|turn| turn.get("turn_id"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("turn");
                    let _ = event_tx.send(CowdEvent::Warning {
                        message: format!(
                            "Gateway accepted turn {turn_id}; streaming will continue via SSE"
                        ),
                    });
                    return;
                }
                if mode == "attached_to_active_turn" {
                    let decision = value
                        .get("input")
                        .and_then(|input| input.get("decision"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("attached");
                    let _ = event_tx.send(CowdEvent::Warning {
                        message: format!("Input attached to active turn: {decision}"),
                    });
                    return;
                }
                if let Some(response) = value
                    .get("response")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                {
                    let _ = event_tx.send(CowdEvent::TextDelta {
                        text: response.to_string(),
                    });
                }
                let _ = event_tx.send(CowdEvent::Warning {
                    message: "Gateway accepted input; awaiting durable terminal commit".to_string(),
                });
            }
            Err(err) => {
                let _ = event_tx.send(CowdEvent::TurnError {
                    error: format!("Gateway chat failed: {err}"),
                });
            }
        }
    });
}

fn attach_path_from_command(text: &str) -> Option<&Path> {
    let trimmed = text.trim();
    if trimmed != "/attach" && !trimmed.starts_with("/attach ") {
        return None;
    }
    let raw = trimmed
        .strip_prefix("/attach")
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(Path::new(raw.trim_matches('"').trim_matches('\'')))
}

fn dispatch_gateway_cancel(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    session_id: &str,
) {
    let cancel_client = gateway_client.clone();
    let cancel_session_id = session_id.to_string();
    let cancel_tx = tx.clone();
    spawn_tui_task(tx, async move {
        match cancel_client
            .cancel_session_turn(&cancel_session_id, "tui_user_cancel")
            .await
        {
            Ok(receipt) => {
                let status = receipt
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("cancel_requested");
                let _ = cancel_tx.send(CowdEvent::Warning {
                    message: format!("Gateway cancel request accepted: {status}"),
                });
            }
            Err(err) => {
                let _ = cancel_tx.send(CowdEvent::TurnError {
                    error: format!("Gateway cancel request failed: {err}"),
                });
            }
        }
    });
}

fn queue_cancel_command(input: &str) -> Option<&str> {
    let mut parts = input.trim().split_whitespace();
    matches!(parts.next(), Some("/queue"))
        .then(|| parts.next())
        .flatten()
        .filter(|action| *action == "cancel")
        .and_then(|_| parts.next())
}

fn queue_edit_command(input: &str) -> Option<&str> {
    let mut parts = input.trim().split_whitespace();
    matches!(parts.next(), Some("/queue"))
        .then(|| parts.next())
        .flatten()
        .filter(|action| *action == "edit")
        .and_then(|_| parts.next())
}

fn dispatch_pending_input_cancel(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    session_id: &str,
    input_id: &str,
) {
    let client = gateway_client.clone();
    let tx = tx.clone();
    let task_tx = tx.clone();
    let session_id = session_id.to_string();
    let input_id = input_id.to_string();
    spawn_tui_task(&tx, async move {
        match client
            .cancel_session_input(&session_id, &input_id, "cancelled from TUI queue")
            .await
        {
            Ok(receipt) => {
                if let Some(projection) = receipt.get("input_projection") {
                    let _ = task_tx.send(CowdEvent::SessionInputProjection {
                        projection: projection.clone(),
                    });
                }
                let _ = task_tx.send(CowdEvent::Warning {
                    message: format!("Queued input {input_id} cancelled"),
                });
            }
            Err(error) => {
                let _ = task_tx.send(CowdEvent::TurnError {
                    error: format!("Queued input cancellation failed: {error}"),
                });
            }
        }
    });
}

fn execution_command_from_input(input: &str) -> Option<ExecutionCommandKind> {
    match input.trim().to_ascii_lowercase().as_str() {
        "/execution pause" | "/execution/pause" => Some(ExecutionCommandKind::Pause),
        "/execution resume" | "/execution/resume" => Some(ExecutionCommandKind::Resume),
        "/execution cancel" | "/execution/cancel" => Some(ExecutionCommandKind::Cancel),
        "/execution replan" | "/execution/replan" => Some(ExecutionCommandKind::Replan),
        _ => None,
    }
}

fn dispatch_execution_projection_command(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    state: &TuiState,
    command: ExecutionCommandKind,
) {
    let Some(projection) = state.app.latest_execution_projection.as_ref() else {
        let _ = tx.send(CowdEvent::Warning {
            message: "No active execution projection is available for this command".to_string(),
        });
        return;
    };
    let available = projection
        .available_commands
        .iter()
        .find(|candidate| candidate.command == command);
    if available.is_some_and(|candidate| !candidate.available) {
        let _ = tx.send(CowdEvent::Warning {
            message: available
                .and_then(|candidate| candidate.reason.clone())
                .unwrap_or_else(|| "The current execution state rejects this command".to_string()),
        });
        return;
    }
    let client = gateway_client.clone();
    let task_tx = tx.clone();
    let execution_id = projection.execution_id.clone();
    let expected_revision = projection.revision;
    spawn_tui_task(tx, async move {
        let receipt = client
            .execute_projection_command(
                &execution_id,
                &ExecutionCommandRequest {
                    command_id: format!("tui-execution-command-{}", uuid::Uuid::new_v4()),
                    expected_revision,
                    command,
                    payload: serde_json::json!({"source": "tui"}),
                },
            )
            .await;
        match receipt {
            Ok(receipt) => {
                let _ = task_tx.send(CowdEvent::Warning {
                    message: format!(
                        "Execution command {:?}: {} (revision {})",
                        command, receipt.status, receipt.accepted_revision
                    ),
                });
            }
            Err(error) => {
                let _ = task_tx.send(CowdEvent::TurnError {
                    error: format!("Execution command {:?} failed: {error}", command),
                });
            }
        }
    });
}

#[derive(Debug, Clone)]
struct MfgRefreshRequest {
    generation: u64,
    selection_revision: u64,
    selected_incident_id: Option<String>,
    selected_assignment_id: Option<String>,
    selected_report_id: Option<String>,
    selected_review_id: Option<String>,
    seed: MfgOperationsSnapshot,
}

fn start_pending_mfg_refresh(
    state: &mut TuiState,
    gateway_client: &GatewayApiClient,
    event_tx: &CowdEventSender,
) {
    let Some(generation) = state.app.mfg_operations.take_refresh_request() else {
        return;
    };
    let operations = &state.app.mfg_operations;
    let request = MfgRefreshRequest {
        generation,
        selection_revision: operations.selection_revision,
        selected_incident_id: operations.selected_incident_id.clone(),
        selected_assignment_id: operations.selected_assignment_id.clone(),
        selected_report_id: operations.selected_report_id.clone(),
        selected_review_id: operations.selected_review_id.clone(),
        seed: mfg_snapshot_seed(operations),
    };
    let client = gateway_client.clone();
    let tx = event_tx.clone();
    spawn_tui_task(event_tx, async move {
        refresh_mfg_operations(client, request, tx).await;
    });
}

fn mfg_snapshot_seed(operations: &MfgOperationsState) -> MfgOperationsSnapshot {
    MfgOperationsSnapshot {
        app_descriptor: operations.app_descriptor.clone(),
        command_center: operations.command_center.clone(),
        incidents: operations.incidents.clone(),
        incident_detail: operations.incident_detail.clone(),
        incident_detail_ref: operations.incident_detail_ref.clone(),
        incident_room: operations.incident_room.clone(),
        analysis: operations.analysis.clone(),
        analysis_ref: operations.analysis_ref.clone(),
        decision_trace: operations.decision_trace.clone(),
        execution: operations.executions.clone(),
        execution_ref: operations.execution_ref.clone(),
        alert_rules: operations.alert_rules.clone(),
        alerts: operations.alerts.clone(),
        assignments: operations.assignments.clone(),
        assignment_detail: operations.assignment_detail.clone(),
        assignment_detail_ref: operations.assignment_detail_ref.clone(),
        reports: operations.reports.clone(),
        report_detail: operations.report_detail.clone(),
        report_detail_ref: operations.report_detail_ref.clone(),
        delivery_state: operations.delivery_state.clone(),
        reviews: operations.reviews.clone(),
        review_detail: operations.review_detail.clone(),
        review_detail_ref: operations.review_detail_ref.clone(),
        live_stream_available: operations.live_stream_available,
        fetched_at: operations.last_updated_at.clone().unwrap_or_default(),
        degraded_reasons: Vec::new(),
        pagination: operations.pagination.clone(),
        selection_revision: operations.selection_revision,
        granted_capabilities: operations.granted_capabilities.clone(),
        forbidden_sections: operations.forbidden_sections.clone(),
        section_errors: BTreeMap::new(),
        is_stale: false,
        attempted_routes: BTreeSet::new(),
    }
}

async fn refresh_mfg_operations(
    client: GatewayApiClient,
    request: MfgRefreshRequest,
    event_tx: CowdEventSender,
) {
    let contract = match client.mfg_contract().await {
        Ok(contract) => contract,
        Err(error) => {
            send_mfg_read_failure(
                &event_tx,
                request.generation,
                "contract",
                mfg_api_error_from_gateway(&error),
            );
            return;
        }
    };
    if let Err(error) = validate_mfg_tui_contract(&contract) {
        send_mfg_read_failure(&event_tx, request.generation, "contract", error);
        return;
    }
    let _ = event_tx.send(CowdEvent::MfgContract {
        generation: request.generation,
        contract: contract.clone(),
    });

    let mut snapshot = request.seed;
    let previously_loaded = !snapshot.fetched_at.is_empty();
    snapshot.selection_revision = request.selection_revision;
    snapshot.fetched_at = chrono::Utc::now().to_rfc3339();
    snapshot.degraded_reasons.clear();
    snapshot.forbidden_sections.clear();
    snapshot.section_errors.clear();
    snapshot.is_stale = false;
    snapshot.attempted_routes.clear();
    snapshot
        .attempted_routes
        .insert(app_mfg_contract::MfgRouteId::ContractGet);
    snapshot.granted_capabilities.clear();
    grant_mfg_capability(&mut snapshot, "mfg.read");
    snapshot.live_stream_available = contract
        .surfaces
        .iter()
        .find(|surface| surface.surface == app_mfg_contract::MfgSurfaceKind::Tui)
        .is_some_and(|surface| {
            surface
                .routes
                .contains(&app_mfg_contract::MfgRouteId::LiveStream)
        });

    macro_rules! read_document {
        ($field:ident, $route:expr, $section:literal, $replacements:expr) => {
            debug_assert_eq!(
                crate::runtime_control_store::mfg_route_section($route),
                Some($section)
            );
            snapshot.attempted_routes.insert($route);
            match client.mfg_tui_read($route, $replacements).await {
                Ok(document) => snapshot.$field = Some(document),
                Err(error) => record_mfg_route_error(&mut snapshot, $route, &error),
            }
        };
    }

    read_document!(
        command_center,
        app_mfg_contract::MfgRouteId::CommandCenterGet,
        "command_center",
        &[]
    );
    read_document!(
        app_descriptor,
        app_mfg_contract::MfgRouteId::AppGet,
        "app",
        &[]
    );
    read_document!(
        decision_trace,
        app_mfg_contract::MfgRouteId::DecisionTraceGet,
        "decision_trace",
        &[]
    );

    let incident_limit = mfg_page_limit(&snapshot, "incidents", 50);
    snapshot
        .attempted_routes
        .insert(app_mfg_contract::MfgRouteId::IncidentList);
    match client
        .mfg_tui_read_with_query(
            app_mfg_contract::MfgRouteId::IncidentList,
            &[],
            &[("limit", incident_limit.to_string())],
        )
        .await
    {
        Ok(document) => {
            snapshot.incidents = mfg_document_summaries(&document, "incident");
            snapshot.pagination.insert(
                "incidents".to_string(),
                mfg_document_pagination(&document, snapshot.incidents.len(), incident_limit),
            );
        }
        Err(error) => record_mfg_route_error(
            &mut snapshot,
            app_mfg_contract::MfgRouteId::IncidentList,
            &error,
        ),
    }
    let alert_rule_limit = mfg_page_limit(&snapshot, "alert_rules", 100);
    snapshot
        .attempted_routes
        .insert(app_mfg_contract::MfgRouteId::AlertRuleList);
    match client
        .mfg_tui_read_with_query(
            app_mfg_contract::MfgRouteId::AlertRuleList,
            &[],
            &[("limit", alert_rule_limit.to_string())],
        )
        .await
    {
        Ok(document) => {
            snapshot.alert_rules = mfg_document_summaries(&document, "alert_rule");
            snapshot.pagination.insert(
                "alert_rules".to_string(),
                mfg_document_pagination(&document, snapshot.alert_rules.len(), alert_rule_limit),
            );
        }
        Err(error) => record_mfg_route_error(
            &mut snapshot,
            app_mfg_contract::MfgRouteId::AlertRuleList,
            &error,
        ),
    }
    let alert_limit = mfg_page_limit(&snapshot, "alerts", 100);
    snapshot
        .attempted_routes
        .insert(app_mfg_contract::MfgRouteId::AlertList);
    match client
        .mfg_tui_read_with_query(
            app_mfg_contract::MfgRouteId::AlertList,
            &[],
            &[("limit", alert_limit.to_string())],
        )
        .await
    {
        Ok(document) => {
            snapshot.alerts = mfg_document_summaries(&document, "alert");
            snapshot.pagination.insert(
                "alerts".to_string(),
                mfg_document_pagination(&document, snapshot.alerts.len(), alert_limit),
            );
        }
        Err(error) => record_mfg_route_error(
            &mut snapshot,
            app_mfg_contract::MfgRouteId::AlertList,
            &error,
        ),
    }
    let assignment_limit = mfg_page_limit(&snapshot, "assignments", 100);
    snapshot
        .attempted_routes
        .insert(app_mfg_contract::MfgRouteId::AssignmentList);
    match client
        .mfg_tui_read_with_query(
            app_mfg_contract::MfgRouteId::AssignmentList,
            &[],
            &[("limit", assignment_limit.to_string())],
        )
        .await
    {
        Ok(document) => {
            snapshot.assignments = mfg_document_summaries(&document, "assignment");
            snapshot.pagination.insert(
                "assignments".to_string(),
                mfg_document_pagination(&document, snapshot.assignments.len(), assignment_limit),
            );
        }
        Err(error) => record_mfg_route_error(
            &mut snapshot,
            app_mfg_contract::MfgRouteId::AssignmentList,
            &error,
        ),
    }
    let report_limit = mfg_page_limit(&snapshot, "reports", 100);
    snapshot
        .attempted_routes
        .insert(app_mfg_contract::MfgRouteId::ReportList);
    match client
        .mfg_tui_read_with_query(
            app_mfg_contract::MfgRouteId::ReportList,
            &[],
            &[("limit", report_limit.to_string())],
        )
        .await
    {
        Ok(document) => {
            snapshot.reports = mfg_document_summaries(&document, "report");
            snapshot.pagination.insert(
                "reports".to_string(),
                mfg_document_pagination(&document, snapshot.reports.len(), report_limit),
            );
        }
        Err(error) => record_mfg_route_error(
            &mut snapshot,
            app_mfg_contract::MfgRouteId::ReportList,
            &error,
        ),
    }
    let review_limit = mfg_page_limit(&snapshot, "reviews", 50).min(200);
    snapshot
        .attempted_routes
        .insert(app_mfg_contract::MfgRouteId::ReportReviewList);
    match client.mfg_report_reviews(review_limit).await {
        Ok(collection) => {
            grant_mfg_capability(&mut snapshot, "mfg.report.review");
            snapshot.reviews = collection.items.iter().map(mfg_review_summary).collect();
            snapshot.pagination.insert(
                "reviews".to_string(),
                MfgPaginationState {
                    cursor: None,
                    next_cursor: collection.next_cursor,
                    loaded_count: snapshot.reviews.len(),
                    total_count: None,
                    limit: review_limit,
                },
            );
        }
        Err(error) => record_mfg_route_error(
            &mut snapshot,
            app_mfg_contract::MfgRouteId::ReportReviewList,
            &error,
        ),
    }

    let incident_id =
        selected_or_first(request.selected_incident_id.as_deref(), &snapshot.incidents);
    if snapshot.incident_detail_ref.as_deref() != incident_id.as_deref() {
        snapshot.incident_detail = None;
        snapshot.incident_room = None;
        snapshot.analysis = None;
        snapshot.analysis_ref = None;
        snapshot.execution = None;
        snapshot.execution_ref = None;
        snapshot.incident_detail_ref = incident_id.clone();
    }
    if let Some(incident_id) = incident_id.as_deref() {
        read_document!(
            incident_detail,
            app_mfg_contract::MfgRouteId::IncidentGet,
            "incident_detail",
            &[("id", incident_id)]
        );
        read_document!(
            incident_room,
            app_mfg_contract::MfgRouteId::IncidentRoomGet,
            "incident_room",
            &[("id", incident_id)]
        );
        let related = snapshot
            .incident_detail
            .as_ref()
            .and_then(mfg_document_value)
            .or_else(|| snapshot.incident_room.as_ref().and_then(mfg_document_value));
        if let Some(related) = related.as_ref() {
            let analysis_id = find_string_recursive(related, "analysis_id");
            if snapshot.analysis_ref.as_deref() != analysis_id.as_deref() {
                snapshot.analysis = None;
                snapshot.analysis_ref = analysis_id.clone();
            }
            if let Some(analysis_id) = analysis_id {
                read_document!(
                    analysis,
                    app_mfg_contract::MfgRouteId::AnalysisGet,
                    "analysis",
                    &[("id", analysis_id.as_str())]
                );
            }
            let execution_id = find_string_recursive(related, "execution_id");
            if snapshot.execution_ref.as_deref() != execution_id.as_deref() {
                snapshot.execution = None;
                snapshot.execution_ref = execution_id.clone();
            }
            if let Some(execution_id) = execution_id {
                read_document!(
                    execution,
                    app_mfg_contract::MfgRouteId::ExecutionGet,
                    "execution",
                    &[("id", execution_id.as_str())]
                );
            }
        } else {
            snapshot.analysis = None;
            snapshot.analysis_ref = None;
            snapshot.execution = None;
            snapshot.execution_ref = None;
        }
    } else {
        snapshot.incident_detail = None;
        snapshot.incident_room = None;
        snapshot.incident_detail_ref = None;
        snapshot.analysis = None;
        snapshot.analysis_ref = None;
        snapshot.execution = None;
        snapshot.execution_ref = None;
    }

    let assignment_id = selected_or_first(
        request.selected_assignment_id.as_deref(),
        &snapshot.assignments,
    );
    if snapshot.assignment_detail_ref.as_deref() != assignment_id.as_deref() {
        snapshot.assignment_detail = None;
        snapshot.assignment_detail_ref = assignment_id.clone();
    }
    if let Some(assignment_id) = assignment_id.as_deref() {
        read_document!(
            assignment_detail,
            app_mfg_contract::MfgRouteId::AssignmentGet,
            "assignment_detail",
            &[("id", assignment_id)]
        );
    } else {
        snapshot.assignment_detail = None;
        snapshot.assignment_detail_ref = None;
    }

    let report_id = selected_or_first(request.selected_report_id.as_deref(), &snapshot.reports);
    if snapshot.report_detail_ref.as_deref() != report_id.as_deref() {
        snapshot.report_detail = None;
        snapshot.delivery_state = None;
        snapshot.report_detail_ref = report_id.clone();
    }
    if let Some(report_id) = report_id.as_deref() {
        read_document!(
            report_detail,
            app_mfg_contract::MfgRouteId::ReportGet,
            "report_detail",
            &[("id", report_id)]
        );
        read_document!(
            delivery_state,
            app_mfg_contract::MfgRouteId::ReportDeliveryStateGet,
            "delivery_state",
            &[("id", report_id)]
        );
    } else {
        snapshot.report_detail = None;
        snapshot.delivery_state = None;
        snapshot.report_detail_ref = None;
    }

    let review_id = selected_or_first(request.selected_review_id.as_deref(), &snapshot.reviews);
    if snapshot.review_detail_ref.as_deref() != review_id.as_deref() {
        snapshot.review_detail = None;
        snapshot.review_detail_ref = review_id.clone();
    }
    if let Some(review_id) = review_id.as_deref() {
        snapshot
            .attempted_routes
            .insert(app_mfg_contract::MfgRouteId::ReportReviewGet);
        match client.mfg_report_review(review_id).await {
            Ok(review) => snapshot.review_detail = Some(review),
            Err(error) => record_mfg_route_error(
                &mut snapshot,
                app_mfg_contract::MfgRouteId::ReportReviewGet,
                &error,
            ),
        }
    } else {
        snapshot.review_detail = None;
        snapshot.review_detail_ref = None;
    }

    enforce_mfg_snapshot_access_recrop(&mut snapshot);
    snapshot.is_stale = previously_loaded && !snapshot.section_errors.is_empty();
    let _ = event_tx.send(CowdEvent::MfgSnapshot {
        generation: request.generation,
        snapshot,
    });
}

fn validate_mfg_tui_contract(
    contract: &app_mfg_contract::MfgFrontendContractV1,
) -> Result<(), app_mfg_contract::MfgApiErrorV1> {
    let client_version = app_mfg_contract::MFG_CONTRACT_VERSION;
    if contract.contract_version.0 != client_version {
        return Err(mfg_contract_error(format!(
            "MFG contract mismatch: server={}, client={client_version}",
            contract.contract_version.0
        )));
    }
    let expected = app_mfg_contract::mfg_tui_p0_read_route_contracts()
        .into_iter()
        .map(|route| route.route_id)
        .collect::<BTreeSet<_>>();
    let Some(surface) = contract
        .surfaces
        .iter()
        .find(|surface| surface.surface == app_mfg_contract::MfgSurfaceKind::Tui)
    else {
        return Err(mfg_contract_error(
            "MFG contract has no TUI surface descriptor".to_string(),
        ));
    };
    let actual = surface.routes.iter().copied().collect::<BTreeSet<_>>();
    if surface.role != app_mfg_contract::MfgSurfaceRole::ConsoleReadOnly
        || !surface.actions.is_empty()
        || actual != expected
    {
        return Err(mfg_contract_error(format!(
            "MFG TUI contract invalid: role={:?}, routes={}/{}, actions={}",
            surface.role,
            actual.len(),
            expected.len(),
            surface.actions.len()
        )));
    }
    Ok(())
}

fn mfg_contract_error(message: String) -> app_mfg_contract::MfgApiErrorV1 {
    app_mfg_contract::MfgApiErrorV1 {
        code: app_mfg_contract::MfgErrorCode::ContractMismatch,
        message,
        http_status: 409,
        details: serde_json::json!({
            "client_contract_version": app_mfg_contract::MFG_CONTRACT_VERSION,
        }),
        retryable: false,
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
            kind: app_mfg_contract::MfgRecoveryActionKind::Reload,
            label: "Upgrade or reload the TUI".to_string(),
            target: Some("/mfg".to_string()),
            enabled: true,
        }],
        request_id: None,
        receipt_ref: None,
    }
}

fn mfg_api_error_from_gateway(
    error: &crate::gateway_client::GatewayApiError,
) -> app_mfg_contract::MfgApiErrorV1 {
    if let crate::gateway_client::GatewayApiError::Api(error) = error {
        return error.clone();
    }
    app_mfg_contract::MfgApiErrorV1 {
        code: app_mfg_contract::MfgErrorCode::Internal,
        message: error.to_string(),
        http_status: 503,
        details: serde_json::Value::Null,
        retryable: true,
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
            kind: app_mfg_contract::MfgRecoveryActionKind::Reload,
            label: "Refresh MFG control plane".to_string(),
            target: Some("/mfg".to_string()),
            enabled: true,
        }],
        request_id: None,
        receipt_ref: None,
    }
}

fn send_mfg_read_failure(
    event_tx: &CowdEventSender,
    generation: u64,
    section: &str,
    error: app_mfg_contract::MfgApiErrorV1,
) {
    let _ = event_tx.send(CowdEvent::MfgReadFailed {
        generation,
        section: section.to_string(),
        error,
    });
}

fn record_mfg_section_error(
    snapshot: &mut MfgOperationsSnapshot,
    section: &str,
    error: &crate::gateway_client::GatewayApiError,
) {
    let error = mfg_api_error_from_gateway(error);
    snapshot
        .degraded_reasons
        .push(format!("{section}: {}", error.message));
    snapshot
        .section_errors
        .insert(section.to_string(), error.clone());
    if matches!(
        error.code,
        app_mfg_contract::MfgErrorCode::CapabilityDenied
            | app_mfg_contract::MfgErrorCode::AuthenticationRequired
    ) {
        snapshot
            .forbidden_sections
            .insert(section.to_string(), error.message.clone());
        redact_mfg_snapshot_section(snapshot, section);
        enforce_mfg_snapshot_access_error(snapshot, &error);
    }
}

fn record_mfg_route_error(
    snapshot: &mut MfgOperationsSnapshot,
    route_id: app_mfg_contract::MfgRouteId,
    error: &crate::gateway_client::GatewayApiError,
) {
    let section =
        crate::runtime_control_store::mfg_route_section(route_id).unwrap_or("unknown_mfg_route");
    record_mfg_section_error(snapshot, section, error);
}

fn redact_mfg_snapshot_section(snapshot: &mut MfgOperationsSnapshot, section: &str) {
    match section {
        "contract" => {}
        "app" => snapshot.app_descriptor = None,
        "command_center" => snapshot.command_center = None,
        "decision_trace" => snapshot.decision_trace = None,
        "live_stream" => snapshot.live_stream_available = false,
        "incidents" => {
            snapshot.incidents.clear();
            snapshot.pagination.remove("incidents");
        }
        "incident_detail" => {
            snapshot.incident_detail = None;
            snapshot.incident_detail_ref = None;
        }
        "incident_room" => {
            snapshot.incident_room = None;
        }
        "analysis" => {
            snapshot.analysis = None;
            snapshot.analysis_ref = None;
        }
        "execution" => {
            snapshot.execution = None;
            snapshot.execution_ref = None;
        }
        "alert_rules" => {
            snapshot.alert_rules.clear();
            snapshot.pagination.remove("alert_rules");
        }
        "alerts" => {
            snapshot.alerts.clear();
            snapshot.pagination.remove("alerts");
        }
        "assignments" => {
            snapshot.assignments.clear();
            snapshot.pagination.remove("assignments");
        }
        "assignment_detail" => {
            snapshot.assignment_detail = None;
            snapshot.assignment_detail_ref = None;
        }
        "reports" => {
            snapshot.reports.clear();
            snapshot.pagination.remove("reports");
        }
        "report_detail" => {
            snapshot.report_detail = None;
            snapshot.report_detail_ref = None;
        }
        "delivery_state" => {
            snapshot.delivery_state = None;
        }
        "reviews" => {
            snapshot.reviews.clear();
            snapshot.pagination.remove("reviews");
        }
        "review_detail" => {
            snapshot.review_detail = None;
            snapshot.review_detail_ref = None;
        }
        _ => {}
    }
}

fn enforce_mfg_snapshot_access_recrop(snapshot: &mut MfgOperationsSnapshot) {
    let errors = snapshot
        .section_errors
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for error in errors {
        enforce_mfg_snapshot_access_error(snapshot, &error);
    }
}

fn enforce_mfg_snapshot_access_error(
    snapshot: &mut MfgOperationsSnapshot,
    error: &app_mfg_contract::MfgApiErrorV1,
) {
    match error.code {
        app_mfg_contract::MfgErrorCode::AuthenticationRequired => {
            redact_all_mfg_snapshot_data(snapshot);
            for section in crate::runtime_control_store::MFG_ALL_READ_SECTIONS {
                snapshot
                    .forbidden_sections
                    .insert(section.to_string(), error.message.clone());
            }
        }
        app_mfg_contract::MfgErrorCode::CapabilityDenied => {
            let Some(capability) = crate::runtime_control_store::mfg_required_capability(error)
            else {
                return;
            };
            snapshot
                .granted_capabilities
                .retain(|granted| granted != capability);
            for route in app_mfg_contract::mfg_tui_p0_read_route_contracts() {
                if !crate::runtime_control_store::mfg_route_requires_capability(
                    &route.capability,
                    capability,
                ) {
                    continue;
                }
                if let Some(section) =
                    crate::runtime_control_store::mfg_route_section(route.route_id)
                {
                    snapshot
                        .forbidden_sections
                        .insert(section.to_string(), error.message.clone());
                    redact_mfg_snapshot_section(snapshot, section);
                }
            }
        }
        _ => {}
    }
}

fn redact_all_mfg_snapshot_data(snapshot: &mut MfgOperationsSnapshot) {
    for section in crate::runtime_control_store::MFG_ALL_READ_SECTIONS {
        redact_mfg_snapshot_section(snapshot, section);
    }
    snapshot.granted_capabilities.clear();
    snapshot.live_stream_available = false;
}

fn grant_mfg_capability(snapshot: &mut MfgOperationsSnapshot, capability: &str) {
    if !snapshot
        .granted_capabilities
        .iter()
        .any(|candidate| candidate == capability)
    {
        snapshot.granted_capabilities.push(capability.to_string());
        snapshot.granted_capabilities.sort();
    }
}

fn selected_or_first(selected: Option<&str>, items: &[MfgItemSummary]) -> Option<String> {
    selected
        .filter(|id| items.iter().any(|item| item.id == *id))
        .map(str::to_string)
        .or_else(|| items.first().map(|item| item.id.clone()))
}

fn mfg_document_value(document: &app_mfg_contract::MfgReadResponseV1) -> Option<serde_json::Value> {
    serde_json::to_value(document).ok()
}

fn mfg_document_summaries(
    document: &app_mfg_contract::MfgReadResponseV1,
    kind: &str,
) -> Vec<MfgItemSummary> {
    let Some(value) = mfg_document_value(document) else {
        return Vec::new();
    };
    first_object_array(&value)
        .unwrap_or_default()
        .iter()
        .filter_map(|item| mfg_item_summary(item, kind))
        .collect()
}

fn first_object_array(value: &serde_json::Value) -> Option<Vec<serde_json::Value>> {
    match value {
        serde_json::Value::Array(items) if items.iter().all(serde_json::Value::is_object) => {
            Some(items.clone())
        }
        serde_json::Value::Object(map) => {
            for key in [
                "items",
                "incidents",
                "alerts",
                "alert_rules",
                "assignments",
                "reports",
                "reviews",
                "data",
            ] {
                if let Some(items) = map.get(key).and_then(serde_json::Value::as_array) {
                    if items.iter().all(serde_json::Value::is_object) {
                        return Some(items.clone());
                    }
                }
            }
            map.values().find_map(first_object_array)
        }
        _ => None,
    }
}

fn mfg_item_summary(value: &serde_json::Value, kind: &str) -> Option<MfgItemSummary> {
    let id = first_string(
        value,
        &[
            "id",
            "incident_id",
            "alert_id",
            "rule_id",
            "assignment_id",
            "report_id",
            "review_id",
        ],
    )?;
    let title = first_string(
        value,
        &[
            "title",
            "name",
            "summary",
            "objective",
            "subject",
            "description",
        ],
    )
    .unwrap_or_else(|| id.clone());
    let status =
        first_string(value, &["status", "state", "lifecycle"]).unwrap_or_else(|| "unknown".into());
    let evidence_refs = value
        .get("evidence_refs")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut backlinks = Vec::new();
    for evidence in &evidence_refs {
        backlinks.push(MfgBacklink {
            kind: MfgBacklinkKind::Evidence,
            target: evidence.clone(),
            label: format!("Evidence {evidence}"),
        });
    }
    if let Some(evidence) = first_string(value, &["evidence_packet_id"]) {
        backlinks.push(MfgBacklink {
            kind: MfgBacklinkKind::Evidence,
            target: evidence.clone(),
            label: format!("Evidence {evidence}"),
        });
    }
    if let Some(runtime) = first_string(
        value,
        &[
            "execution_id",
            "runtime_session_id",
            "workflow_graph_id",
            "task_id",
            "task_ref",
        ],
    ) {
        backlinks.push(MfgBacklink {
            kind: MfgBacklinkKind::Runtime,
            target: runtime.clone(),
            label: format!("Runtime {runtime}"),
        });
    }
    if let Some(approval) = first_string(value, &["approval_id"]) {
        backlinks.push(MfgBacklink {
            kind: MfgBacklinkKind::Approval,
            target: approval.clone(),
            label: format!("Approval {approval}"),
        });
    }
    backlinks.push(MfgBacklink {
        kind: MfgBacklinkKind::Surface,
        target: format!("mfg:{kind}:{id}"),
        label: "MFG surface object".to_string(),
    });
    Some(MfgItemSummary {
        id,
        kind: kind.to_string(),
        title,
        status,
        severity: first_string(value, &["severity", "priority", "risk"]),
        owner: first_string(
            value,
            &[
                "owner",
                "owner_ref",
                "assignee",
                "assignee_ref",
                "assigned_to",
                "reviewer_principal",
                "requester_principal",
                "created_by",
            ],
        ),
        sla: first_display_value(
            value,
            &["sla", "sla_status", "sla_minutes", "due_at", "deadline"],
        ),
        revision: value.get("revision").and_then(serde_json::Value::as_u64),
        evidence_refs,
        backlinks,
        raw: value.clone(),
    })
}

fn mfg_review_summary(review: &app_mfg_contract::MfgReportDeliveryReview) -> MfgItemSummary {
    let raw = serde_json::to_value(review).unwrap_or(serde_json::Value::Null);
    mfg_item_summary(&raw, "review").unwrap_or_else(|| MfgItemSummary {
        id: review.review_id.clone(),
        kind: "review".to_string(),
        title: format!("Report review {}", review.report_id),
        status: format!("{:?}", review.status),
        revision: Some(review.revision),
        evidence_refs: review.evidence_refs.clone(),
        raw,
        ..MfgItemSummary::default()
    })
}

fn mfg_document_pagination(
    document: &app_mfg_contract::MfgReadResponseV1,
    loaded_count: usize,
    limit: usize,
) -> MfgPaginationState {
    let value = mfg_document_value(document).unwrap_or(serde_json::Value::Null);
    MfgPaginationState {
        cursor: first_string(&value, &["cursor"]),
        next_cursor: first_string(&value, &["next_cursor"]),
        loaded_count,
        total_count: ["total_count", "total"]
            .iter()
            .find_map(|key| value.get(*key).and_then(serde_json::Value::as_u64))
            .and_then(|count| usize::try_from(count).ok()),
        limit,
    }
}

fn mfg_page_limit(snapshot: &MfgOperationsSnapshot, section: &str, default: usize) -> usize {
    snapshot
        .pagination
        .get(section)
        .map(|pagination| pagination.limit)
        .filter(|limit| *limit > 0)
        .unwrap_or(default)
        .clamp(1, 500)
}

fn first_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn first_display_value(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            serde_json::Value::String(value) if !value.trim().is_empty() => {
                Some(value.trim().to_string())
            }
            serde_json::Value::Number(value) => Some(value.to_string()),
            _ => None,
        }
    })
}

fn find_string_recursive(value: &serde_json::Value, key: &str) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => map
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                map.values()
                    .find_map(|child| find_string_recursive(child, key))
            }),
        serde_json::Value::Array(items) => items
            .iter()
            .find_map(|child| find_string_recursive(child, key)),
        _ => None,
    }
}

async fn drain_cowd_events_state(
    rx: &mut crate::CowdEventReceiver,
    state: &mut TuiState,
    gateway_client: &GatewayApiClient,
    event_tx: &CowdEventSender,
    execution_projection_stream: &mut Option<String>,
) {
    let mut count = 0;
    let limit = if state.app.turn_is_active() { 64 } else { 256 };
    while let Ok(event) = rx.try_recv() {
        let execution_id = match &event {
            CowdEvent::ExecutionGraphSummary { summary } => summary.graph_id.clone(),
            _ => None,
        };
        if let CowdEvent::ExecutionProjectionDelta { delta } = &event {
            apply_execution_projection_delta(gateway_client, state, delta).await;
            count += 1;
            if count >= limit {
                break;
            }
            continue;
        }
        state.apply_event(event);
        if let Some(execution_id) = execution_id {
            match gateway_client
                .execution_projection(&execution_id, false)
                .await
            {
                Ok(projection) => {
                    let cursor = projection.cursor;
                    state.apply_execution_projection(projection);
                    if execution_projection_stream.as_deref() != Some(execution_id.as_str()) {
                        *execution_projection_stream = Some(execution_id.clone());
                        spawn_execution_projection_stream(
                            gateway_client.clone(),
                            execution_id,
                            cursor,
                            event_tx.clone(),
                        );
                    }
                }
                Err(error) => state.add_system_notice(
                    SystemNoticeKind::Warning,
                    &format!("Execution projection unavailable: {error}"),
                ),
            }
        }
        count += 1;
        if count >= limit {
            break;
        }
    }
}

async fn apply_execution_projection_delta(
    gateway_client: &GatewayApiClient,
    state: &mut TuiState,
    delta: &crate::protocol::ProjectionDelta,
) {
    let Some(projection) = state.app.latest_execution_projection.as_ref() else {
        return;
    };
    let mut reducer = crate::protocol::ExecutionProjectionReducer::default();
    reducer.install_snapshot(projection);
    let should_refresh = !delta.events.is_empty()
        || matches!(
            reducer.apply_delta(delta),
            crate::protocol::ProjectionDeltaApply::ResyncRequired
        );
    if should_refresh {
        match gateway_client
            .execution_projection(&delta.execution_id, false)
            .await
        {
            Ok(snapshot) => state.apply_execution_projection(snapshot),
            Err(error) => state.add_system_notice(
                SystemNoticeKind::Warning,
                &format!("Execution projection resync failed: {error}"),
            ),
        }
    }
}

fn spawn_execution_projection_stream(
    gateway_client: GatewayApiClient,
    execution_id: String,
    initial_cursor: u64,
    event_tx: CowdEventSender,
) {
    let failure_tx = event_tx.clone();
    spawn_tui_task(&failure_tx, async move {
        let mut cursor = initial_cursor;
        let mut retry_delay = Duration::from_millis(250);
        loop {
            match gateway_client
                .subscribe_execution_projection_events(
                    &execution_id,
                    cursor,
                    false,
                    event_tx.clone(),
                )
                .await
            {
                Ok(next_cursor) => {
                    cursor = cursor.max(next_cursor);
                    retry_delay = Duration::from_millis(250);
                }
                Err(error) => {
                    let _ = event_tx.send(CowdEvent::Warning {
                        message: format!(
                            "Execution projection stream interrupted for {execution_id}: {error}; retrying"
                        ),
                    });
                }
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
        }
    });
}

fn list_workspace_files(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
) -> Result<Vec<FileEntry>, String> {
    let projection = runtime
        .block_on(gateway_client.workspace_files_recursive(None, 5_000))
        .map_err(|error| error.to_string())?;
    parse_workspace_files_projection(&projection)
}

fn parse_workspace_files_projection(
    projection: &serde_json::Value,
) -> Result<Vec<FileEntry>, String> {
    if projection
        .get("truncated")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        let limit = projection
            .get("limit")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(5_000);
        return Err(format!(
            "Gateway workspace projection truncated at {limit} entries; file context disabled until projection limit or workspace scope is adjusted"
        ));
    }
    let Some(items) = projection
        .get("files")
        .or_else(|| projection.get("entries"))
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(Vec::new());
    };

    let mut files = items
        .iter()
        .filter_map(workspace_file_entry_from_json)
        .collect::<Vec<_>>();
    files.sort_by(|left, right| {
        right
            .is_dir
            .cmp(&left.is_dir)
            .then(left.name.cmp(&right.name))
    });
    Ok(files)
}

fn workspace_file_entry_from_json(item: &serde_json::Value) -> Option<FileEntry> {
    let name = item
        .get("path")
        .or_else(|| item.get("name"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())?
        .to_string();
    let is_dir = item
        .get("is_dir")
        .and_then(serde_json::Value::as_bool)
        .or_else(|| {
            item.get("type")
                .and_then(serde_json::Value::as_str)
                .map(|kind| matches!(kind, "dir" | "directory" | "folder"))
        })
        .unwrap_or(false);
    let size = item
        .get("size")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    Some(FileEntry { name, is_dir, size })
}

fn arg_value(args: &[String], names: &[&str]) -> Option<String> {
    args.iter().enumerate().find_map(|(index, arg)| {
        if let Some((name, value)) = arg.split_once('=') {
            if names.contains(&name) && !value.trim().is_empty() {
                return Some(value.to_string());
            }
        }
        if names.contains(&arg.as_str()) {
            return args
                .get(index + 1)
                .filter(|value| !value.starts_with('-'))
                .cloned();
        }
        None
    })
}

fn format_startup_banner(model: &str, yolo_mode: bool, session_id: &str) -> String {
    let directory = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .display()
        .to_string();
    format!(
        "COWD v{} | model={} | directory={} | mode={} | session={}",
        env!("CARGO_PKG_VERSION"),
        model,
        directory,
        if yolo_mode { "yolo" } else { "standard" },
        session_id
    )
}

fn format_connected_line(model: &str) -> String {
    let provider = if model.starts_with("claude") {
        "anthropic"
    } else if model.starts_with("grok") {
        "xai"
    } else if model.starts_with("gpt") {
        "openai"
    } else if model.starts_with("deepseek") {
        "deepseek"
    } else {
        "configured provider"
    };
    format!("Connected: {model} via {provider}")
}

fn initialize_shared_rt() -> std::io::Result<&'static tokio::runtime::Runtime> {
    if let Some(runtime) = SHARED_RUNTIME.get() {
        return Ok(runtime);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("cowd-tui")
        .build()?;
    let _ = SHARED_RUNTIME.set(runtime);
    SHARED_RUNTIME
        .get()
        .ok_or_else(|| std::io::Error::other("TUI runtime initialization was not retained"))
}

fn shared_rt() -> Option<&'static tokio::runtime::Runtime> {
    SHARED_RUNTIME.get()
}

fn spawn_tui_task<F>(event_tx: &CowdEventSender, task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let Some(runtime) = shared_rt() else {
        let _ = event_tx.send(CowdEvent::TurnError {
            error: "TUI async runtime is unavailable; restart the terminal session".to_string(),
        });
        return;
    };
    runtime.spawn(task);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_mfg_tui_contract() -> app_mfg_contract::MfgFrontendContractV1 {
        let routes = app_mfg_contract::mfg_route_contracts();
        let active_route_count = routes
            .iter()
            .filter(|route| route.availability == app_mfg_contract::MfgActionAvailability::Active)
            .count();
        let tui_routes = app_mfg_contract::mfg_tui_p0_read_route_contracts()
            .into_iter()
            .map(|route| route.route_id)
            .collect();
        app_mfg_contract::MfgFrontendContractV1 {
            kind: "mfg.frontend_contract".to_string(),
            contract_version: app_mfg_contract::MfgContractVersion::default(),
            generated_at: chrono::Utc::now(),
            app_id: "mfg.manufacturing".to_string(),
            active_route_count,
            planned_route_count: routes.len().saturating_sub(active_route_count),
            routes,
            actions: app_mfg_contract::mfg_action_contracts(),
            surfaces: vec![app_mfg_contract::MfgSurfaceContract {
                surface: app_mfg_contract::MfgSurfaceKind::Tui,
                role: app_mfg_contract::MfgSurfaceRole::ConsoleReadOnly,
                entrypoints: vec!["/mfg".to_string()],
                routes: tui_routes,
                actions: Vec::new(),
            }],
        }
    }

    #[test]
    fn mfg_contract_validation_fails_fast_on_version_role_route_or_action_drift() {
        let valid = valid_mfg_tui_contract();
        assert!(validate_mfg_tui_contract(&valid).is_ok());

        let mut version = valid.clone();
        version.contract_version.0 = "mfg.frontend.v0".to_string();
        assert_eq!(
            validate_mfg_tui_contract(&version)
                .expect_err("version mismatch")
                .code,
            app_mfg_contract::MfgErrorCode::ContractMismatch
        );

        let mut operational = valid.clone();
        let surface = operational.surfaces.first_mut().expect("TUI surface");
        surface.role = app_mfg_contract::MfgSurfaceRole::ConsoleOperationalControl;
        surface.actions.push(app_mfg_contract::MfgActionId::Route(
            app_mfg_contract::MfgRouteId::IncidentCreate,
        ));
        assert!(validate_mfg_tui_contract(&operational).is_err());

        let mut missing_route = valid;
        missing_route.surfaces[0].routes.pop();
        assert!(validate_mfg_tui_contract(&missing_route).is_err());
    }

    #[test]
    fn canonical_incident_mapping_preserves_only_real_identity_status_and_backlinks() {
        let document = app_mfg_contract::MfgReadResponseV1 {
            kind: Some("mfg.incident.collection".to_string()),
            payload: std::collections::BTreeMap::from([(
                "incidents".to_string(),
                serde_json::json!([{
                    "incident_id": "incident-1",
                    "title": "Line stop",
                    "attention_id": "attention-1",
                    "evidence_packet_id": "evidence-1",
                    "task_id": "task-1",
                    "workflow_graph_id": "workflow-1",
                    "status": "open",
                    "created_at": "2026-07-16T00:00:00Z",
                    "updated_at": "2026-07-16T00:01:00Z"
                }]),
            )]),
        };
        let summaries = mfg_document_summaries(&document, "incident");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, "incident-1");
        assert_eq!(summaries[0].status, "open");
        assert_eq!(summaries[0].revision, None);
        assert_eq!(summaries[0].owner, None);
        assert_eq!(summaries[0].sla, None);
        assert_eq!(
            summaries[0]
                .backlinks
                .iter()
                .map(|backlink| backlink.kind)
                .collect::<Vec<_>>(),
            vec![
                MfgBacklinkKind::Evidence,
                MfgBacklinkKind::Runtime,
                MfgBacklinkKind::Surface,
            ]
        );
    }

    #[test]
    fn canonical_review_mapping_owns_approval_revision_and_requester_fields() {
        let now = chrono::Utc::now();
        let review = app_mfg_contract::MfgReportDeliveryReview {
            review_id: "review-1".to_string(),
            report_id: "report-1".to_string(),
            report_revision: 4,
            delivery_revision: 2,
            dead_letter_digest: "digest-1".to_string(),
            requester_principal: "principal-1".to_string(),
            approval_id: Some("approval-1".to_string()),
            correlation_id: "correlation-1".to_string(),
            requested_action: Some(app_mfg_contract::MfgReportDeliveryReviewDecision::ForceRetry),
            decision: None,
            reviewer_principal: None,
            reason: "delivery failed".to_string(),
            evidence_refs: vec!["evidence-1".to_string()],
            decision_lease_ref: None,
            effect_key: None,
            effect_payload: serde_json::Value::Null,
            effect_receipt_ref: None,
            effect_error: None,
            status: app_mfg_contract::MfgReportDeliveryReviewStatus::PendingApproval,
            revision: 7,
            created_at: now,
            updated_at: now,
        };
        let summary = mfg_review_summary(&review);
        assert_eq!(summary.id, "review-1");
        assert_eq!(summary.revision, Some(7));
        assert_eq!(summary.owner.as_deref(), Some("principal-1"));
        assert!(summary.backlinks.iter().any(|backlink| {
            backlink.kind == MfgBacklinkKind::Approval && backlink.target == "approval-1"
        }));
        assert!(summary.backlinks.iter().any(|backlink| {
            backlink.kind == MfgBacklinkKind::Evidence && backlink.target == "evidence-1"
        }));
    }

    #[test]
    fn mfg_snapshot_seed_never_reuses_previous_refresh_attempt_evidence() {
        let mut operations = MfgOperationsState::default();
        operations
            .attempted_routes
            .insert(app_mfg_contract::MfgRouteId::IncidentList);
        let seed = mfg_snapshot_seed(&operations);
        assert!(seed.attempted_routes.is_empty());
    }

    #[test]
    fn snapshot_capability_recrop_is_applied_across_every_dependent_projection() {
        let mut snapshot = MfgOperationsSnapshot {
            incidents: vec![MfgItemSummary {
                id: "incident-1".to_string(),
                ..MfgItemSummary::default()
            }],
            reviews: vec![MfgItemSummary {
                id: "review-1".to_string(),
                ..MfgItemSummary::default()
            }],
            granted_capabilities: vec!["mfg.read".to_string(), "mfg.report.review".to_string()],
            section_errors: BTreeMap::from([(
                "review_detail".to_string(),
                app_mfg_contract::MfgApiErrorV1::capability_denied("mfg.report.review"),
            )]),
            ..MfgOperationsSnapshot::default()
        };
        enforce_mfg_snapshot_access_recrop(&mut snapshot);
        assert_eq!(snapshot.incidents.len(), 1);
        assert!(snapshot.reviews.is_empty());
        assert!(snapshot.forbidden_sections.contains_key("reviews"));
        assert!(snapshot.forbidden_sections.contains_key("review_detail"));
        assert_eq!(snapshot.granted_capabilities, vec!["mfg.read"]);
    }

    #[test]
    fn snapshot_authentication_recrop_clears_all_cached_projections() {
        let mut snapshot = MfgOperationsSnapshot {
            incidents: vec![MfgItemSummary {
                id: "incident-1".to_string(),
                ..MfgItemSummary::default()
            }],
            reviews: vec![MfgItemSummary {
                id: "review-1".to_string(),
                ..MfgItemSummary::default()
            }],
            granted_capabilities: vec!["mfg.read".to_string(), "mfg.report.review".to_string()],
            section_errors: BTreeMap::from([(
                "incident_detail".to_string(),
                app_mfg_contract::MfgApiErrorV1::authentication_required("token expired"),
            )]),
            ..MfgOperationsSnapshot::default()
        };
        enforce_mfg_snapshot_access_recrop(&mut snapshot);
        assert!(snapshot.incidents.is_empty());
        assert!(snapshot.reviews.is_empty());
        assert!(snapshot.granted_capabilities.is_empty());
        assert_eq!(
            snapshot.forbidden_sections.len(),
            crate::runtime_control_store::MFG_ALL_READ_SECTIONS.len()
        );
    }

    #[test]
    fn attach_command_extracts_path() {
        assert_eq!(
            attach_path_from_command("/attach /tmp/a.mp3")
                .expect("path")
                .to_string_lossy(),
            "/tmp/a.mp3"
        );
        assert_eq!(
            attach_path_from_command("/attach \"/tmp/a b.pdf\"")
                .expect("quoted path")
                .to_string_lossy(),
            "/tmp/a b.pdf"
        );
        assert!(attach_path_from_command("/status").is_none());
        assert!(attach_path_from_command("/attachment").is_none());
    }

    #[test]
    fn execution_commands_are_explicit_and_do_not_capture_other_slash_input() {
        assert_eq!(
            execution_command_from_input("/execution pause"),
            Some(ExecutionCommandKind::Pause)
        );
        assert_eq!(
            execution_command_from_input(" /execution/replan "),
            Some(ExecutionCommandKind::Replan)
        );
        assert_eq!(execution_command_from_input("/status"), None);
        assert_eq!(execution_command_from_input("/execution unknown"), None);
    }

    #[test]
    fn queued_input_commands_are_explicit_and_do_not_capture_other_slashes() {
        assert_eq!(
            queue_cancel_command("/queue cancel input-1"),
            Some("input-1")
        );
        assert_eq!(queue_edit_command(" /queue edit input-2 "), Some("input-2"));
        assert_eq!(queue_cancel_command("/queue edit input-1"), None);
        assert_eq!(queue_edit_command("/status"), None);
    }

    #[test]
    fn workspace_projection_keeps_hidden_files_from_gateway() {
        let projection = serde_json::json!({
            "truncated": false,
            "files": [
                {"path": ".env", "is_dir": false, "size": 12},
                {"path": "src", "is_dir": true, "size": 0}
            ]
        });

        let files = parse_workspace_files_projection(&projection).unwrap();

        assert!(files.iter().any(|entry| entry.name == ".env"));
        assert!(files
            .iter()
            .any(|entry| entry.name == "src" && entry.is_dir));
    }

    #[test]
    fn workspace_projection_rejects_truncated_facts() {
        let projection = serde_json::json!({
            "truncated": true,
            "limit": 2,
            "files": [
                {"path": "a.rs", "is_dir": false, "size": 1},
                {"path": "b.rs", "is_dir": false, "size": 1}
            ]
        });

        let err = parse_workspace_files_projection(&projection).unwrap_err();

        assert!(err.contains("truncated at 2 entries"), "{err}");
        assert!(err.contains("file context disabled"), "{err}");
    }
}
