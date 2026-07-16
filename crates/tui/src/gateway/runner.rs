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
