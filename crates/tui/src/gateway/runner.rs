use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use harness_contract::projection::{ExecutionCommandKind, ExecutionCommandRequest};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use crate::app::{App, SystemNoticeKind};
use crate::app_surface_host::{AppSurfaceCommand, AppSurfaceEvent, AppSurfaceRequestKind};
use crate::context_tokens::ContextWorkspaceEntry;
use crate::events::CowdEventSender;
use crate::gateway_client::{
    default_auth_token, AppTransportFailure, AppViewStreamRequest, GatewayApiClient,
};
use crate::state::{
    CompletedCoreGatewayEffect, PendingAppSurfaceCommand, PendingCoreGatewayEffect, ProcessedKey,
    TuiState,
};
use crate::{config_migration, cowd_event_channel, error_recovery, CowdEvent, FileEntry};

#[path = "input.rs"]
mod input;

static SHARED_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

// APP panels own recovery policy and choose when to issue a fresh request. The
// host only absorbs the short transport/authority gap while Gateway is coming
// back, with a fixed bound so a broken endpoint is still reported to the APP.
const APP_TRANSIENT_REQUEST_RETRY_ATTEMPTS: usize = 16;
const APP_TRANSIENT_REQUEST_RETRY_DELAY: Duration = Duration::from_millis(250);
const APP_TRANSIENT_REQUEST_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_PRESENCE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1_200);
const EXECUTION_PROJECTION_MATERIALIZATION_DELAYS: [Duration; 6] = [
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
    Duration::from_millis(800),
    Duration::from_millis(1_600),
];
// SSE remains the low-latency source.  This watchdog is only alive while one
// execution is selected and prevents a still-open stream that missed its
// terminal envelope from leaving the TUI in `stale` forever.  It exits after
// observing the canonical terminal projection, so idle sessions do not poll.
const EXECUTION_PROJECTION_TERMINAL_WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct GatewayTuiConfig {
    pub model: Option<String>,
    pub session_id: String,
    pub startup_execution_policy: Option<String>,
    pub startup_banner: String,
    pub connected_line: String,
}

#[derive(Debug, Default)]
struct ExecutionProjectionReducerController {
    next_generation: u64,
    selected: Option<SelectedExecutionProjection>,
    snapshot_request: Option<SelectedExecutionProjection>,
    active: Option<ActiveExecutionProjectionSource>,
    reducer: crate::protocol::ExecutionProjectionReducer,
}

#[derive(Debug)]
struct SelectedExecutionProjection {
    execution_id: String,
    generation: u64,
}

#[derive(Debug)]
struct ActiveExecutionProjectionSource {
    execution_id: String,
    generation: u64,
    task: tokio::task::JoinHandle<()>,
}

struct PreparedSessionSwitch {
    target_session_id: String,
    ensured: serde_json::Value,
    attached: serde_json::Value,
    lease: Option<serde_json::Value>,
    execution_projection: Option<crate::protocol::ExecutionProjection>,
    execution_id: Option<String>,
    session_stats: Option<serde_json::Value>,
    input_projection: Option<serde_json::Value>,
    execution_policy: harness_contract::policy::SessionExecutionPolicyResponse,
    warnings: Vec<String>,
}

struct SessionSwitchResult {
    generation: u64,
    target_session_id: String,
    result: Result<PreparedSessionSwitch, String>,
}

#[derive(Debug, Default)]
struct SessionAuthorityRegistry {
    generations: BTreeMap<String, u64>,
    revoked: std::collections::BTreeSet<String>,
}

impl SessionAuthorityRegistry {
    fn begin(&mut self, session_id: &str) -> u64 {
        let generation = self
            .generations
            .get(session_id)
            .copied()
            .unwrap_or_default()
            .wrapping_add(1)
            .max(1);
        self.generations.insert(session_id.to_string(), generation);
        self.revoked.remove(session_id);
        generation
    }

    fn revoke(&mut self, session_id: &str, expected_generation: u64) -> bool {
        if !self.accepts(session_id, expected_generation) {
            return false;
        }
        self.generations.insert(
            session_id.to_string(),
            expected_generation.wrapping_add(1).max(1),
        );
        self.revoked.insert(session_id.to_string());
        true
    }

    fn accepts(&self, session_id: &str, generation: u64) -> bool {
        !self.revoked.contains(session_id)
            && self.generations.get(session_id).copied() == Some(generation)
    }

    fn current(&self, session_id: &str) -> Option<u64> {
        self.generations
            .get(session_id)
            .copied()
            .filter(|_| !self.revoked.contains(session_id))
    }
}

impl ExecutionProjectionReducerController {
    /// End a prior selection before the next execution has a usable snapshot.
    /// This raises the generation even when the new snapshot temporarily
    /// returns 404/403, so a late delta from the old execution cannot revive
    /// it in the new turn's UI.
    fn begin_selection(&mut self, execution_id: &str) -> Option<u64> {
        if self
            .selected
            .as_ref()
            .is_some_and(|selected| selected.execution_id == execution_id)
        {
            return None;
        }
        self.stop();
        self.snapshot_request = None;
        self.reducer = crate::protocol::ExecutionProjectionReducer::default();
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        let generation = self.next_generation;
        self.selected = Some(SelectedExecutionProjection {
            execution_id: execution_id.to_string(),
            generation,
        });
        Some(generation)
    }

    fn switch(
        &mut self,
        gateway_client: GatewayApiClient,
        execution_id: String,
        initial_cursor: u64,
        initial_revision: u64,
        generation: u64,
        event_tx: CowdEventSender,
    ) {
        if !self.accepts(generation, &execution_id) {
            return;
        }
        if self.active.as_ref().is_some_and(|active| {
            active.execution_id == execution_id && active.generation == generation
        }) {
            return;
        }
        self.stop();
        if let Some(task) = spawn_execution_projection_source(
            gateway_client,
            execution_id.clone(),
            initial_cursor,
            initial_revision,
            generation,
            event_tx,
        ) {
            self.active = Some(ActiveExecutionProjectionSource {
                execution_id,
                generation,
                task,
            });
        }
    }

    fn accepts(&self, generation: u64, execution_id: &str) -> bool {
        self.selected.as_ref().is_some_and(|selected| {
            selected.generation == generation && selected.execution_id == execution_id
        })
    }

    fn selected_execution_id(&self) -> Option<String> {
        self.selected
            .as_ref()
            .map(|selected| selected.execution_id.clone())
    }

    fn selected_generation(&self, execution_id: &str) -> Option<u64> {
        self.selected
            .as_ref()
            .filter(|selected| selected.execution_id == execution_id)
            .map(|selected| selected.generation)
    }

    /// Coalesce every canonical snapshot request for one selected execution.
    /// The runtime event queue may contain many deltas, but a single latest
    /// projection is authoritative and enough to catch the TUI up.
    fn begin_snapshot_request(&mut self, generation: u64, execution_id: &str) -> bool {
        if !self.accepts(generation, execution_id)
            || self.snapshot_request.as_ref().is_some_and(|request| {
                request.generation == generation && request.execution_id == execution_id
            })
        {
            return false;
        }
        self.snapshot_request = Some(SelectedExecutionProjection {
            execution_id: execution_id.to_string(),
            generation,
        });
        true
    }

    fn finish_snapshot_request(&mut self, generation: u64, execution_id: &str) {
        if self.snapshot_request.as_ref().is_some_and(|request| {
            request.generation == generation && request.execution_id == execution_id
        }) {
            self.snapshot_request = None;
        }
    }

    fn install_snapshot(
        &mut self,
        generation: u64,
        projection: &crate::protocol::ExecutionProjection,
    ) -> crate::protocol::ProjectionDeltaApply {
        if !self.accepts(generation, &projection.execution_id) {
            return crate::protocol::ProjectionDeltaApply::ResyncRequired;
        }
        self.reducer.install_snapshot(projection)
    }

    fn apply_delta(
        &mut self,
        generation: u64,
        delta: &crate::protocol::ProjectionDelta,
    ) -> crate::protocol::ProjectionDeltaApply {
        if !self.accepts(generation, &delta.execution_id) {
            return crate::protocol::ProjectionDeltaApply::ResyncRequired;
        }
        self.reducer.apply_delta(delta)
    }

    fn materialized_projection(&self) -> Option<&crate::protocol::ExecutionProjection> {
        self.reducer.projection()
    }

    fn clear_selection_if(&mut self, generation: u64, execution_id: &str) {
        if self.accepts(generation, execution_id) {
            self.stop();
            self.selected = None;
            self.snapshot_request = None;
        }
    }

    fn stop(&mut self) {
        if let Some(active) = self.active.take() {
            active.task.abort();
        }
    }

    /// Fail closed when the session authority disappears. Aborting only the
    /// active SSE task is insufficient: already queued snapshots and deltas
    /// still carry the old generation. Clearing the selection and advancing
    /// the generation makes every in-flight result unconditionally stale.
    fn revoke_session_authorization(&mut self) {
        self.stop();
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        self.selected = None;
        self.snapshot_request = None;
        self.reducer = crate::protocol::ExecutionProjectionReducer::default();
    }
}

impl Drop for ExecutionProjectionReducerController {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Owns every active declarative APP view stream.
#[derive(Default)]
struct AppTransportController {
    live: BTreeMap<String, ActiveAppSubscription>,
}

struct ActiveAppSubscription {
    cancel: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl AppTransportController {
    fn key(app_id: &str, view_id: &str) -> String {
        format!("{app_id}\u{1f}{view_id}")
    }

    fn stop(&mut self, app_id: &str, view_id: &str) {
        if let Some(active) = self.live.remove(&Self::key(app_id, view_id)) {
            let _ = active.cancel.send(true);
            active.task.abort();
        }
    }

    fn insert(
        &mut self,
        app_id: String,
        view_id: String,
        cancel: tokio::sync::watch::Sender<bool>,
        task: tokio::task::JoinHandle<()>,
    ) {
        self.stop(&app_id, &view_id);
        self.live.insert(
            Self::key(&app_id, &view_id),
            ActiveAppSubscription { cancel, task },
        );
    }

    fn stop_all(&mut self) {
        for (_, active) in std::mem::take(&mut self.live) {
            let _ = active.cancel.send(true);
            active.task.abort();
        }
    }

    fn reap_finished(&mut self) {
        self.live.retain(|_, active| !active.task.is_finished());
    }
}

impl Drop for AppTransportController {
    fn drop(&mut self) {
        self.stop_all();
    }
}

impl GatewayTuiConfig {
    pub fn from_env_args() -> Self {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let model =
            arg_value(&args, &["--model", "-m"]).or_else(|| std::env::var("COWD_MODEL").ok());
        let session_id = arg_value(&args, &["--resume", "--session", "--session-id", "-s"])
            .unwrap_or_else(|| format!("tui-{}", uuid::Uuid::new_v4()));
        let startup_execution_policy = if args.iter().any(|arg| arg == "--yolo") {
            Some("yolo".to_string())
        } else if args.iter().any(|arg| {
            matches!(
                arg.as_str(),
                "--solo"
                    | "--dangerously-skip-permissions"
                    | "--danger-full-access"
                    | "--autonomous"
            )
        }) {
            Some("autonomous".to_string())
        } else {
            None
        };
        let display_model = model.clone().unwrap_or_else(|| "unresolved".to_string());
        Self {
            startup_banner: format_startup_banner(
                &display_model,
                startup_execution_policy.as_deref().unwrap_or("session"),
                &session_id,
            ),
            connected_line: format_connected_line(&display_model),
            model,
            session_id,
            startup_execution_policy,
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
        .unwrap_or_else(|| "unresolved".to_string());
    let mut state = TuiState::new(&display_model, &session_id);
    state
        .app
        .add_system_notice(SystemNoticeKind::Info, &config.startup_banner);
    state
        .app
        .add_system_notice(SystemNoticeKind::Info, &config.connected_line);

    let mut gateway_lease_owner: Option<String> = None;
    let mut session_authorities = SessionAuthorityRegistry::default();
    let initial_authority_generation = session_authorities.begin(&session_id);
    let mut execution_projection_source = ExecutionProjectionReducerController::default();
    let mut session_source_bridges: BTreeMap<String, tokio::task::JoinHandle<()>> = BTreeMap::new();
    let mut session_apps: BTreeMap<String, App> = BTreeMap::new();
    let (session_switch_tx, mut session_switch_rx) =
        tokio::sync::mpsc::unbounded_channel::<SessionSwitchResult>();
    let (core_gateway_result_tx, mut core_gateway_result_rx) =
        tokio::sync::mpsc::unbounded_channel::<CompletedCoreGatewayEffect>();
    let mut session_switch_generation = 0_u64;
    let mut session_switch_inflight_target: Option<String> = None;
    let mut message_submission_generation = 0_u64;
    let gateway_client = GatewayApiClient::ensure_running_with_retry(default_auth_token())?
        .ok_or_else(|| {
            "Gateway API is required for TUI; start `cowd gateway run` or allow TUI autostart"
                .to_string()
        })?;
    let observer_id = gateway_client.observer_id().to_string();
    match runtime.block_on(gateway_client.app_catalog()) {
        Ok(catalog) => {
            if let Err(error) = state.set_gateway_app_catalog(catalog) {
                state.app.add_system_notice(
                    SystemNoticeKind::Warning,
                    &format!("Application catalog was rejected: {error}"),
                );
            }
        }
        Err(error) => {
            state.app.add_system_notice(
                SystemNoticeKind::Warning,
                &format!(
                    "Application catalog is unavailable; Core TUI remains operational: {error}"
                ),
            );
        }
    }
    let (gateway_session_ids, presence_heartbeat_interval) = attach_gateway_session(
        runtime,
        &gateway_client,
        &tui_tx,
        &mut state,
        &config,
        &mut gateway_lease_owner,
        &mut execution_projection_source,
        &mut session_source_bridges,
        &observer_id,
        initial_authority_generation,
    )?;
    let mut last_presence_heartbeat = Instant::now();
    let mut presence_heartbeat_task: Option<tokio::task::JoinHandle<()>> = None;
    let mission_live_task = state
        .app
        .gateway
        .gateway_mission_control
        .as_ref()
        .and_then(|mission| mission.mission_id.as_deref())
        .filter(|mission_id| !mission_id.trim().is_empty())
        .map(|mission_id| {
            spawn_mission_source(
                runtime,
                gateway_client.clone(),
                tui_tx.clone(),
                mission_id.to_string(),
            )
        });
    let mut app_transport_controller = AppTransportController::default();
    dispatch_pending_app_transport_effects(
        runtime,
        &mut state,
        &gateway_client,
        &tui_tx,
        &mut app_transport_controller,
    );

    terminal.draw(|frame| state.render(frame))?;

    state.set_memory_projection_available(true);
    state.set_active_sessions_count(1);
    if accessibility_enabled {
        state.shell.accessibility = crate::accessibility::AccessibilityMode::full();
        let high_contrast_theme = crate::accessibility::high_contrast_theme(true);
        state.shell.theme_engine = crate::theme::ThemeEngine::new(high_contrast_theme);
    }
    if !migration_report.contains("nothing to migrate") {
        state
            .app
            .add_system_notice(SystemNoticeKind::Info, &migration_report);
    }
    match list_workspace_files(runtime, &gateway_client) {
        Ok(files) => {
            state.shell.prompt.set_workspace_entries(
                files
                    .iter()
                    .map(|entry| ContextWorkspaceEntry::new(entry.name.clone(), entry.is_dir)),
            );
            state.app.workbench.file_entries = files;
        }
        Err(error) => {
            state.app.add_system_notice(
                SystemNoticeKind::Warning,
                &format!("Gateway workspace projection unavailable: {error}"),
            );
        }
    }
    send_session_list(&tui_tx, gateway_session_ids, &session_id);
    state.queue_gateway_api(
        |client| async move { client.list_sessions().await },
        |state, result| match result {
            Ok(catalog) => {
                state.apply_gateway_session_catalog(&catalog);
            }
            Err(error) => state.app.add_system_notice(
                SystemNoticeKind::Warning,
                &format!("Session catalogue refresh failed: {error}"),
            ),
        },
    );

    let startup_ready = true;
    let res = runtime.block_on(async {
        let mut reader = crossterm::event::EventStream::new();
        let mut last_animation_draw = Instant::now();
        loop {
            tokio::select! {
                Some(completed) = core_gateway_result_rx.recv() => {
                    completed.apply_if_current(&mut state);
                }
                Some(prepared) = session_switch_rx.recv() => {
                    if prepared.generation != session_switch_generation {
                        cleanup_stale_prepared_session_switch(
                            &gateway_client,
                            &observer_id,
                            prepared,
                        );
                        continue;
                    }
                    session_switch_inflight_target = None;
                    match prepared.result {
                        Ok(prepared) => commit_prepared_session_switch(
                            runtime,
                            &gateway_client,
                            &tui_tx,
                            &mut state,
                            &mut session_apps,
                            &mut gateway_lease_owner,
                            &mut execution_projection_source,
                            &mut session_source_bridges,
                            prepared,
                            &observer_id,
                            &mut session_authorities,
                        ),
                        Err(error) => state.app.add_system_notice(
                            SystemNoticeKind::Error,
                            &format!("Session switch failed without changing the active view: {error}"),
                        ),
                    }
                }
                Some(Ok(event)) = reader.next() => {
                    if input::handle_terminal_event(
                        event,
                        &gateway_client,
                        &tui_tx,
                        &mut state,
                        &gateway_lease_owner,
                        &session_authorities,
                        &mut message_submission_generation,
                        &mut session_switch_generation,
                        &mut session_switch_inflight_target,
                        &session_switch_tx,
                        &observer_id,
                    ) {
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    drain_cowd_events_state(
                        &mut tui_rx,
                        &mut state,
                        &gateway_client,
                        &tui_tx,
                        &mut execution_projection_source,
                        &mut session_apps,
                        &mut session_authorities,
                        &mut gateway_lease_owner,
                        &mut app_transport_controller,
                        &mut session_source_bridges,
                    ).await;
                    dispatch_pending_app_transport_effects(
                        runtime,
                        &mut state,
                        &gateway_client,
                        &tui_tx,
                        &mut app_transport_controller,
                    );
                    app_transport_controller.reap_finished();
                    state.update_startup_phase(startup_ready);
                    if state.app.turn_is_active() {
                        state.app.tick();
                    }
                    if last_presence_heartbeat.elapsed() >= presence_heartbeat_interval
                        && presence_heartbeat_task
                            .as_ref()
                            .is_none_or(tokio::task::JoinHandle::is_finished)
                    {
                        presence_heartbeat_task.take();
                        let active_session_id = state.app.shell.session_id.clone();
                        let active_is_writer = gateway_lease_owner.is_some();
                        let targets = session_source_bridges
                            .keys()
                            .map(|session_id| {
                                let role = if active_is_writer && session_id == &active_session_id {
                                    "writer"
                                } else {
                                    "reader"
                                };
                                (session_id.clone(), role)
                            })
                            .collect::<Vec<_>>();
                        let client = gateway_client.clone();
                        presence_heartbeat_task = Some(runtime.spawn(async move {
                            futures::future::join_all(targets.into_iter().map(
                                |(session_id, role)| {
                                    let client = client.clone();
                                    async move {
                                        let _ = client
                                            .attach_session(&session_id, "tui", Some(role))
                                            .await;
                                    }
                                },
                            ))
                            .await;
                        }));
                        last_presence_heartbeat = Instant::now();
                    }
                }
            }
            dispatch_pending_core_gateway_effects(
                &mut state,
                &gateway_client,
                &core_gateway_result_tx,
            );
            // Do not redraw a quiescent terminal at a fixed 16 ms cadence.
            // Active runs still tick for elapsed/status feedback; input and
            // state changes advance `msg_version` above.
            let transient_redraw_due = transient_ui_redraw_due(
                state.app.turn_is_active(),
                !state.overlay.toast_manager.is_empty(),
                last_animation_draw.elapsed(),
            );
            if state.app.timeline.last_drawn_version != state.app.timeline.msg_version
                || state.app.timeline.last_drawn_render_version != state.app.timeline.render_version
                || transient_redraw_due
            {
                terminal.draw(|frame| state.render(frame))?;
                last_animation_draw = Instant::now();
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    });

    app_transport_controller.stop_all();
    if let Some(task) = mission_live_task {
        task.abort();
    }
    if let Some(task) = presence_heartbeat_task {
        task.abort();
    }
    let observed_session_ids = session_source_bridges.keys().cloned().collect::<Vec<_>>();
    for (_, task) in std::mem::take(&mut session_source_bridges) {
        task.abort();
    }
    let active_session_id = state.app.shell.session_id.clone();
    if gateway_lease_owner.is_some() {
        let _ = runtime.block_on(gateway_client.release_runtime_session_lease(&active_session_id));
    }
    for session_id in observed_session_ids {
        let _ = runtime.block_on(gateway_client.detach_session(&session_id, "tui"));
    }
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

fn transient_ui_redraw_due(
    turn_is_active: bool,
    toast_is_visible: bool,
    since_last_draw: Duration,
) -> bool {
    (turn_is_active || toast_is_visible) && since_last_draw >= Duration::from_millis(100)
}

fn dispatch_pending_core_gateway_effects(
    state: &mut TuiState,
    gateway_client: &GatewayApiClient,
    result_tx: &tokio::sync::mpsc::UnboundedSender<CompletedCoreGatewayEffect>,
) {
    for PendingCoreGatewayEffect {
        session_id,
        authority_generation,
        operation,
        completion,
    } in state.take_pending_core_gateway_effects()
    {
        let client = gateway_client.clone();
        let result_tx = result_tx.clone();
        tokio::spawn(async move {
            let result = operation(client).await.map_err(|error| error.to_string());
            let _ = result_tx.send(CompletedCoreGatewayEffect::new(
                session_id,
                authority_generation,
                result,
                completion,
            ));
        });
    }
}

fn attach_gateway_session(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    event_tx: &crate::events::CowdEventSender,
    state: &mut TuiState,
    config: &GatewayTuiConfig,
    gateway_lease_owner: &mut Option<String>,
    execution_projection_source: &mut ExecutionProjectionReducerController,
    session_source_bridges: &mut BTreeMap<String, tokio::task::JoinHandle<()>>,
    _observer_id: &str,
    authority_generation: u64,
) -> Result<(Vec<String>, Duration), Box<dyn std::error::Error>> {
    let ensured_session_id =
        attach_gateway_session_identity(runtime, gateway_client, state, config)?;
    let (writer_attached, mut presence_heartbeat_interval) =
        attach_gateway_lifecycle(runtime, gateway_client, state, &ensured_session_id);
    ensure_session_source_bridge(
        runtime,
        gateway_client,
        event_tx,
        session_source_bridges,
        &ensured_session_id,
        authority_generation,
    );
    restore_attached_execution_projection(
        runtime,
        gateway_client,
        event_tx,
        state,
        execution_projection_source,
        &ensured_session_id,
    );
    acquire_gateway_writer_lease(
        runtime,
        gateway_client,
        state,
        &ensured_session_id,
        writer_attached,
        gateway_lease_owner,
        &mut presence_heartbeat_interval,
    );
    let gateway_session_ids = hydrate_gateway_runtime_state(
        runtime,
        gateway_client,
        state,
        config,
        &ensured_session_id,
        writer_attached && gateway_lease_owner.is_some(),
    )?;
    state.app.add_system_notice(
        SystemNoticeKind::Info,
        "Gateway event stream subscribed for this session",
    );
    Ok((gateway_session_ids, presence_heartbeat_interval))
}

fn attach_gateway_session_identity(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    state: &mut TuiState,
    config: &GatewayTuiConfig,
) -> Result<String, Box<dyn std::error::Error>> {
    let status = runtime
        .block_on(gateway_client.status())
        .map_err(|err| format!("Gateway API is required for TUI: {err}"))?;
    state.app.gateway.server_running = true;
    state.app.gateway.active_api_sessions = status
        .get("active_sessions")
        .and_then(serde_json::Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or_default();
    state.app.gateway.server_uptime_secs = status
        .get("uptime_secs")
        .and_then(serde_json::Value::as_u64);

    let active_api_sessions = state.app.gateway.active_api_sessions;
    let server_uptime_secs = state.app.gateway.server_uptime_secs.unwrap_or_default();
    state.app.add_system_notice(
        SystemNoticeKind::Info,
        &format!("Gateway API connected: {active_api_sessions} active sessions, uptime {server_uptime_secs}s"),
    );

    let ensured = runtime
        .block_on(
            gateway_client
                .ensure_session(&config.session_id, config.model.as_deref().unwrap_or("")),
        )
        .map_err(|err| format!("Gateway session attach failed: {err}"))?;
    state.app.gateway.active_api_sessions = ensured
        .get("active_sessions")
        .and_then(serde_json::Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(state.app.gateway.active_api_sessions);
    let ensured_session_id = ensured
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&config.session_id)
        .to_string();
    state.app.shell.requested_model = ensured
        .get("model")
        .and_then(serde_json::Value::as_str)
        .filter(|model| !model.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| config.model.clone());
    if let Some(model) = state.app.shell.requested_model.clone() {
        state.app.shell.model = model;
    }
    let action = if ensured
        .get("created")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        "created"
    } else {
        "attached"
    };
    state.app.add_system_notice(
        SystemNoticeKind::Info,
        &format!("Gateway session {action}: {ensured_session_id}"),
    );
    Ok(ensured_session_id)
}

fn attach_gateway_lifecycle(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    state: &mut TuiState,
    ensured_session_id: &str,
) -> (bool, Duration) {
    let mut presence_heartbeat_interval = DEFAULT_PRESENCE_HEARTBEAT_INTERVAL;
    let writer_attached = match runtime.block_on(gateway_client.attach_session(
        ensured_session_id,
        "tui",
        Some("writer"),
    )) {
        Ok(attached) => {
            presence_heartbeat_interval = presence_heartbeat_interval_from_attachment(&attached);
            state.app.add_system_notice(
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
            match runtime.block_on(gateway_client.replay_session(ensured_session_id, 0, 100)) {
                Ok(replay) => state.app.add_system_notice(
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
                Err(err) => state.app.add_system_notice(
                    SystemNoticeKind::Error,
                    &format!("Gateway replay unavailable: {err}"),
                ),
            }
            true
        }
        Err(err) => {
            state.app.add_system_notice(
                SystemNoticeKind::Error,
                &format!(
                    "Gateway lifecycle writer attach unavailable; this TUI remains read-only: {err}"
                ),
            );
            match runtime.block_on(gateway_client.attach_session(
                ensured_session_id,
                "tui",
                Some("reader"),
            )) {
                Ok(attached) => {
                    presence_heartbeat_interval =
                        presence_heartbeat_interval_from_attachment(&attached);
                    state.app.add_system_notice(
                        SystemNoticeKind::Info,
                        "Gateway lifecycle reader attached",
                    );
                }
                Err(reader_error) => state.app.add_system_notice(
                    SystemNoticeKind::Error,
                    &format!("Gateway lifecycle reader attach is also unavailable: {reader_error}"),
                ),
            }
            false
        }
    };
    (writer_attached, presence_heartbeat_interval)
}

fn ensure_session_source_bridge(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    event_tx: &crate::events::CowdEventSender,
    session_source_bridges: &mut BTreeMap<String, tokio::task::JoinHandle<()>>,
    ensured_session_id: &str,
    authority_generation: u64,
) {
    // The bridge establishes the server-side live subscription first, then
    // consumes durable history and live bytes concurrently. Stable message
    // identities reconcile either arrival order without holding live progress
    // behind a slow history page.
    session_source_bridges.retain(|_, task| !task.is_finished());
    if !session_source_bridges.contains_key(ensured_session_id) {
        session_source_bridges.insert(
            ensured_session_id.to_string(),
            spawn_session_source_bridge(
                runtime,
                gateway_client.clone(),
                event_tx.clone(),
                ensured_session_id.to_string(),
                authority_generation,
            ),
        );
    }
}

fn restore_attached_execution_projection(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    event_tx: &crate::events::CowdEventSender,
    state: &mut TuiState,
    execution_projection_source: &mut ExecutionProjectionReducerController,
    ensured_session_id: &str,
) {
    match runtime.block_on(gateway_client.session_execution_index(ensured_session_id)) {
        Ok(index) => {
            if let Some(execution_id) = session_index_visible_execution_id(&index) {
                if let Some(generation) = execution_projection_source.begin_selection(&execution_id)
                {
                    match runtime.block_on(gateway_client.execution_projection(&execution_id, true))
                    {
                        Ok(projection) => {
                            let cursor = projection.cursor;
                            let revision = projection.revision;
                            state.apply_execution_projection(projection);
                            execution_projection_source.switch(
                                gateway_client.clone(),
                                execution_id,
                                cursor,
                                revision,
                                generation,
                                event_tx.clone(),
                            );
                        }
                        Err(error) => {
                            state.app.add_system_notice(
                                SystemNoticeKind::Warning,
                                &format!(
                                    "Latest execution projection is still materializing during attach: {error}"
                                ),
                            );
                            if execution_projection_source
                                .begin_snapshot_request(generation, &execution_id)
                            {
                                spawn_execution_projection_materialization(
                                    gateway_client.clone(),
                                    execution_id,
                                    generation,
                                    event_tx.clone(),
                                );
                            }
                        }
                    }
                }
            }
        }
        Err(error) => state.app.add_system_notice(
            SystemNoticeKind::Warning,
            &format!("Session execution index could not be restored during attach: {error}"),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn acquire_gateway_writer_lease(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    state: &mut TuiState,
    ensured_session_id: &str,
    writer_attached: bool,
    gateway_lease_owner: &mut Option<String>,
    presence_heartbeat_interval: &mut Duration,
) {
    let lease_result = writer_attached.then(|| {
        runtime.block_on(
            gateway_client.acquire_runtime_session_lease(ensured_session_id, "collaborative"),
        )
    });
    match lease_result {
        Some(Ok(lease)) => {
            *gateway_lease_owner = lease
                .get("owner")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned);
            state.app.gateway.gateway_lease_owner = gateway_lease_owner.clone();
            state.app.gateway.gateway_lease_mode = lease
                .get("mode")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned);
            state.app.add_system_notice(
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
        Some(Err(err)) => {
            *gateway_lease_owner = None;
            state.app.gateway.gateway_lease_owner = None;
            state.app.gateway.gateway_lease_mode = Some("read-only".to_string());
            let _ = runtime.block_on(gateway_client.detach_session(ensured_session_id, "tui"));
            match runtime.block_on(gateway_client.attach_session(
                ensured_session_id,
                "tui",
                Some("reader"),
            )) {
                Ok(attached) => {
                    *presence_heartbeat_interval =
                        presence_heartbeat_interval_from_attachment(&attached);
                    state.app.add_system_notice(
                        SystemNoticeKind::Info,
                        "Gateway lifecycle downgraded to reader after writer lease rejection",
                    );
                }
                Err(reader_error) => state.app.add_system_notice(
                    SystemNoticeKind::Error,
                    &format!("Gateway lifecycle reader fallback unavailable: {reader_error}"),
                ),
            }
            state.app.add_system_notice(
                SystemNoticeKind::Error,
                &format!("Gateway session lease unavailable: {err}"),
            );
        }
        None => {
            *gateway_lease_owner = None;
            state.app.gateway.gateway_lease_owner = None;
            state.app.gateway.gateway_lease_mode = Some("read-only".to_string());
        }
    }
}

fn hydrate_gateway_runtime_state(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    state: &mut TuiState,
    config: &GatewayTuiConfig,
    ensured_session_id: &str,
    writable: bool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let execution_policy = runtime
        .block_on(resolve_tui_session_execution_policy(
            gateway_client,
            ensured_session_id,
            config.startup_execution_policy.as_deref(),
            writable,
        ))
        .map_err(|error| format!("Session execution policy unavailable: {error}"))?;
    state.app.shell.execution_policy_preset = execution_policy_preset(&execution_policy);
    state.app.shell.execution_policy_snapshot = Some(execution_policy.clone());

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
    match runtime.block_on(gateway_client.session_stats(&config.session_id)) {
        Ok(stats) => {
            state.app.apply_session_stats(stats);
            state
                .app
                .add_system_notice(SystemNoticeKind::Info, "Gateway session statistics loaded");
        }
        Err(err) => state.app.add_system_notice(
            SystemNoticeKind::Error,
            &format!("Gateway session statistics unavailable: {err}"),
        ),
    }
    match runtime.block_on(gateway_client.session_input_projection(&config.session_id)) {
        Ok(projection) => state.app.apply_session_input_projection(projection),
        Err(err) => state.app.add_system_notice(
            SystemNoticeKind::Warning,
            &format!("Gateway queued-input projection unavailable: {err}"),
        ),
    }
    if let Some(readiness) = readiness {
        state.app.add_system_notice(
            SystemNoticeKind::Info,
            &format!("Gateway runtime projection connected: readiness={readiness}, components={components}"),
        );
    }
    for reason in degraded_reasons.into_iter().take(3) {
        state.app.add_system_notice(
            SystemNoticeKind::Warning,
            &format!("Gateway projection degraded: {reason}"),
        );
    }

    Ok(gateway_session_ids)
}

fn presence_heartbeat_interval_from_attachment(attachment: &serde_json::Value) -> Duration {
    attachment
        .get("presence_ttl_ms")
        .and_then(serde_json::Value::as_u64)
        .filter(|ttl_ms| *ttl_ms > 0)
        .map(|ttl_ms| Duration::from_millis((ttl_ms / 3).max(100)))
        .unwrap_or(DEFAULT_PRESENCE_HEARTBEAT_INTERVAL)
}

fn spawn_session_source_bridge(
    runtime: &tokio::runtime::Runtime,
    event_client: GatewayApiClient,
    event_tx: crate::events::CowdEventSender,
    event_session_id: String,
    authority_generation: u64,
) -> tokio::task::JoinHandle<()> {
    runtime.spawn(async move {
        let mut after_commit_cursor = None;
        let next_message_sequence = Arc::new(AtomicUsize::new(0));
        let mut hydration: Option<
            std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>>,
        > = None;
        let mut retry_delay = Duration::from_millis(250);
        let mut attempt = 0u32;
        let _ = send_session_scoped_with_generation(
            &event_tx,
            &event_session_id,
            authority_generation,
            CowdEvent::SessionStreamConnection {
                session_id: event_session_id.clone(),
                state: crate::protocol::SessionStreamConnectionState::Connecting,
            },
        );
        loop {
            let cursor_before = after_commit_cursor;
            let mut made_progress = false;
            if hydration.is_none() {
                hydration = Some(Box::pin(event_client.hydrate_session_history(
                    &event_session_id,
                    event_tx.clone(),
                    Arc::clone(&next_message_sequence),
                    authority_generation,
                )));
            }
            let mut subscription = Box::pin(event_client.consume_session_live_source(
                &event_session_id,
                event_tx.clone(),
                after_commit_cursor,
                Arc::clone(&next_message_sequence),
                authority_generation,
            ));
            let subscription_result = if let Some(active_hydration) = hydration.as_mut() {
                tokio::select! {
                    () = active_hydration => {
                        hydration = None;
                        subscription.await
                    }
                    result = &mut subscription => result,
                }
            } else {
                subscription.await
            };
            match subscription_result {
                Ok(progress) => {
                    after_commit_cursor = progress.commit_cursor.or(after_commit_cursor);
                    made_progress = after_commit_cursor.unwrap_or_default()
                        > cursor_before.unwrap_or_default();
                    if made_progress {
                        attempt = 0;
                        retry_delay = Duration::from_millis(250);
                    }
                }
                Err(error) => {
                    if matches!(
                        &error,
                        crate::gateway_client::GatewayApiError::SessionAuthorizationRevoked(_)
                    ) {
                        // The SSE decoder delivered the typed revoke before
                        // returning this terminal marker. Do not enqueue a
                        // second revoke for the same authority generation.
                        break;
                    }
                    if matches!(
                        &error,
                        crate::gateway_client::GatewayApiError::Status(
                            reqwest::StatusCode::UNAUTHORIZED
                                | reqwest::StatusCode::FORBIDDEN,
                            _
                        )
                    ) {
                        // An HTTP authorization failure happens before the SSE
                        // body exists, so there is no typed revoke frame for
                        // GatewayApiClient to translate.  Convert that
                        // transport boundary into the same session-scoped
                        // terminal authority event used by an in-band revoke.
                        // Merely breaking this task leaves queued history,
                        // APP responses, attachments and the writer lease
                        // looking live in the rest of the TUI.
                        let _ = send_session_stream_authorization_revoke(
                            &event_tx,
                            &event_session_id,
                            authority_generation,
                            &error,
                        );
                        break;
                    }
                    let _ = event_tx.send(CowdEvent::SessionScoped {
                        session_id: event_session_id.clone(),
                        authority_generation,
                        event: Box::new(CowdEvent::Warning {
                            message: format!(
                                "Gateway session stream interrupted; reconnecting with durable hydration: {error}"
                            ),
                        }),
                    });
                }
            }
            attempt = attempt.saturating_add(1);
            let _ = send_session_scoped_with_generation(
                &event_tx,
                &event_session_id,
                authority_generation,
                CowdEvent::SessionStreamConnection {
                    session_id: event_session_id.clone(),
                    state: crate::protocol::SessionStreamConnectionState::Reconnecting {
                        attempt,
                        after_cursor: after_commit_cursor,
                    },
                },
            );
            tokio::time::sleep(retry_delay).await;
            if !made_progress {
                retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
            }
        }
    })
}

fn spawn_mission_source(
    runtime: &tokio::runtime::Runtime,
    client: GatewayApiClient,
    event_tx: crate::events::CowdEventSender,
    mission_id: String,
) -> tokio::task::JoinHandle<()> {
    runtime.spawn(async move {
        if let Err(error) = client
            .consume_mission_live_source(&mission_id, event_tx.clone())
            .await
        {
            let _ = event_tx.send(CowdEvent::Warning {
                message: format!("Gateway Mission live source stopped: {error}"),
            });
        }
    })
}

fn send_session_stream_authorization_revoke(
    tx: &CowdEventSender,
    session_id: &str,
    authority_generation: u64,
    error: &crate::gateway_client::GatewayApiError,
) -> Result<(), ()> {
    let reason = match error {
        crate::gateway_client::GatewayApiError::Status(status, body)
            if matches!(
                *status,
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
            ) =>
        {
            let detail = body.trim();
            if detail.is_empty() {
                format!("Gateway revoked session stream authorization ({status})")
            } else {
                format!("Gateway revoked session stream authorization ({status}): {detail}")
            }
        }
        _ => return Err(()),
    };
    send_session_scoped_with_generation(
        tx,
        session_id,
        authority_generation,
        CowdEvent::SessionAuthorizationRevoked {
            session_id: session_id.to_string(),
            reason,
        },
    )
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

fn take_pending_session_switch(state: &mut TuiState) -> Option<String> {
    let index = state.session.session_sidebar.pending_switch_idx.take()?;
    state
        .session
        .session_sidebar
        .sessions()
        .get(index)
        .map(|session| session.id.clone())
}

fn consume_pending_session_sidebar_actions(state: &mut TuiState) {
    if std::mem::take(&mut state.session.session_sidebar.pending_new_session) {
        let model = state.app.shell.requested_model.clone();
        let preset = state.app.shell.execution_policy_preset.clone();
        let preset = (!matches!(preset.as_str(), "unavailable" | "unresolved")
            && !preset.trim().is_empty())
        .then_some(preset);
        state.queue_gateway_api(
            move |client| async move {
                let created = client
                    .create_session(model.as_deref(), preset.as_deref())
                    .await?;
                let catalog = client.list_sessions().await?;
                Ok(serde_json::json!({ "created": created, "catalog": catalog }))
            },
            |state, result| match result {
                Ok(payload) => {
                    if let Some(catalog) = payload.get("catalog") {
                        state.apply_gateway_session_catalog(catalog);
                    }
                    let id = payload
                        .pointer("/created/id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    state.app.add_system_notice(
                        SystemNoticeKind::Info,
                        &format!("Session created: {id}. Select it and press Enter to switch."),
                    );
                }
                Err(error) => state.app.add_system_notice(
                    SystemNoticeKind::Error,
                    &format!("Session creation failed: {error}"),
                ),
            },
        );
    }

    if let Some((index, title)) = state.session.session_sidebar.pending_rename.take() {
        if let Some(session_id) = state
            .session
            .session_sidebar
            .sessions()
            .get(index)
            .map(|session| session.id.clone())
        {
            state.queue_gateway_api(
                move |client| async move {
                    client.rename_session(&session_id, &title).await?;
                    client.list_sessions().await
                },
                |state, result| match result {
                    Ok(catalog) => {
                        state.apply_gateway_session_catalog(&catalog);
                        state
                            .app
                            .add_system_notice(SystemNoticeKind::Info, "Session renamed");
                    }
                    Err(error) => state.app.add_system_notice(
                        SystemNoticeKind::Error,
                        &format!("Session rename failed: {error}"),
                    ),
                },
            );
        }
    }

    if let Some(index) = state.session.session_sidebar.pending_delete_idx.take() {
        if let Some(session_id) = state
            .session
            .session_sidebar
            .sessions()
            .get(index)
            .map(|session| session.id.clone())
        {
            if session_id == state.app.shell.session_id {
                state.app.add_system_notice(
                    SystemNoticeKind::Error,
                    "The active session cannot be deleted. Switch to another session first.",
                );
            } else {
                state.queue_gateway_api(
                    move |client| async move {
                        client.delete_session(&session_id).await?;
                        client.list_sessions().await
                    },
                    |state, result| match result {
                        Ok(catalog) => {
                            state.apply_gateway_session_catalog(&catalog);
                            state
                                .app
                                .add_system_notice(SystemNoticeKind::Info, "Session deleted");
                        }
                        Err(error) => state.app.add_system_notice(
                            SystemNoticeKind::Error,
                            &format!("Session deletion failed: {error}"),
                        ),
                    },
                );
            }
        }
    }

    if std::mem::take(&mut state.session.session_sidebar.pending_fork) {
        state.session.session_sidebar.pending_fork_at = None;
        let index = state.session.session_sidebar.selected_idx();
        if let Some(session_id) = state
            .session
            .session_sidebar
            .sessions()
            .get(index)
            .map(|session| session.id.clone())
        {
            state.queue_gateway_api(
                move |client| async move {
                    let branch = client.branch_session(&session_id).await?;
                    let catalog = client.list_sessions().await?;
                    Ok(serde_json::json!({ "branch": branch, "catalog": catalog }))
                },
                |state, result| match result {
                    Ok(payload) => {
                        if let Some(catalog) = payload.get("catalog") {
                            state.apply_gateway_session_catalog(catalog);
                        }
                        let id = payload
                            .pointer("/branch/id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("unknown");
                        state.app.add_system_notice(
                            SystemNoticeKind::Info,
                            &format!("Session branch created: {id}"),
                        );
                    }
                    Err(error) => state.app.add_system_notice(
                        SystemNoticeKind::Error,
                        &format!("Session branch failed: {error}"),
                    ),
                },
            );
        }
    }

    if std::mem::take(&mut state.session.session_sidebar.pending_export) {
        state.overlay.export_dialog.reset();
        state.overlay.export_dialog_active = true;
        state.app.request_redraw();
    }
}

fn consume_pending_session_export(state: &mut TuiState) {
    let Some(options) = state.overlay.pending_export_options.take() else {
        return;
    };
    let session_id = state.app.shell.session_id.clone();
    let leaf = Path::new(&options.filename)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("session.md");
    let leaf = if Path::new(leaf).extension().is_none() {
        format!("{leaf}.md")
    } else {
        leaf.to_string()
    };
    let output_path = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(leaf);
    let output_for_request = output_path.clone();
    state.queue_gateway_api(
        move |client| async move {
            let mut offset = 0usize;
            let mut messages = Vec::new();
            loop {
                let page = client
                    .session_messages_offset(&session_id, offset, 500)
                    .await?;
                let fetched = page.messages.len();
                offset = offset.saturating_add(fetched);
                let total = page.total;
                messages.extend(page.messages);
                if fetched == 0 || offset >= total {
                    break;
                }
            }
            let markdown = render_session_export_markdown(
                &session_id,
                &messages,
                options.include_thinking,
                options.include_tools,
                options.include_metadata,
            );
            let write_path = output_for_request.clone();
            tokio::task::spawn_blocking(move || {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&write_path)
                    .map_err(|error| {
                        crate::gateway_client::GatewayApiError::Url(format!(
                            "cannot create {}: {error}",
                            write_path.display()
                        ))
                    })?;
                file.write_all(markdown.as_bytes()).map_err(|error| {
                    crate::gateway_client::GatewayApiError::Url(format!(
                        "cannot write {}: {error}",
                        write_path.display()
                    ))
                })?;
                Ok::<_, crate::gateway_client::GatewayApiError>(())
            })
            .await
            .map_err(|error| {
                crate::gateway_client::GatewayApiError::Url(format!(
                    "export writer task failed: {error}"
                ))
            })??;
            Ok(serde_json::json!({
                "path": output_for_request.display().to_string(),
                "message_count": messages.len(),
            }))
        },
        move |state, result| match result {
            Ok(receipt) => state.app.add_system_notice(
                SystemNoticeKind::Info,
                &format!(
                    "Session exported to {} ({} messages)",
                    receipt
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_else(|| output_path.to_str().unwrap_or("session.md")),
                    receipt
                        .get("message_count")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default()
                ),
            ),
            Err(error) => state.app.add_system_notice(
                SystemNoticeKind::Error,
                &format!("Session export failed: {error}"),
            ),
        },
    );
}

fn render_session_export_markdown(
    session_id: &str,
    messages: &[crate::protocol::SessionMessageProjection],
    include_thinking: bool,
    include_tools: bool,
    include_metadata: bool,
) -> String {
    let mut out = format!("# Cowd session `{session_id}`\n\n");
    for message in messages {
        let role = match message.role.as_str() {
            "user" => "User",
            "assistant" => "Assistant",
            "tool" => "Tool",
            other => other,
        };
        out.push_str("## ");
        out.push_str(role);
        if include_metadata {
            out.push_str(&format!(
                " · seq {} · {}",
                message.sequence, message.created_at_ms
            ));
        }
        out.push_str("\n\n");
        for block in &message.blocks {
            match block.get("type").and_then(serde_json::Value::as_str) {
                Some("text") => {
                    if let Some(text) = block.get("text").and_then(serde_json::Value::as_str) {
                        out.push_str(text);
                        out.push_str("\n\n");
                    }
                }
                Some("thinking") if include_thinking => {
                    if let Some(thinking) =
                        block.get("thinking").and_then(serde_json::Value::as_str)
                    {
                        out.push_str("<details><summary>Thinking</summary>\n\n");
                        out.push_str(thinking);
                        out.push_str("\n\n</details>\n\n");
                    }
                }
                Some("tool_use") if include_tools => {
                    let name = block
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("tool");
                    let input = block
                        .get("input")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    out.push_str(&format!(
                        "**Tool call: `{name}`**\n\n````json\n{input}\n````\n\n"
                    ));
                }
                Some("tool_result") if include_tools => {
                    let output = block
                        .get("output")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    out.push_str(&format!("**Tool result**\n\n````text\n{output}\n````\n\n"));
                }
                _ => {}
            }
        }
    }
    out
}

fn dispatch_gateway_session_switch(
    gateway_client: GatewayApiClient,
    result_tx: tokio::sync::mpsc::UnboundedSender<SessionSwitchResult>,
    generation: u64,
    target_session_id: String,
    _observer_id: String,
) {
    tokio::spawn(async move {
        let result = prepare_gateway_session_switch(&gateway_client, &target_session_id).await;
        let _ = result_tx.send(SessionSwitchResult {
            generation,
            target_session_id,
            result,
        });
    });
}

fn dispatch_older_history_page(
    gateway_client: &GatewayApiClient,
    event_tx: &CowdEventSender,
    session_id: &str,
    authority_generation: u64,
    current_oldest_offset: usize,
) {
    let client = gateway_client.clone();
    let tx = event_tx.clone();
    let session_id = session_id.to_string();
    tokio::spawn(async move {
        const PAGE_SIZE: usize = 500;
        let oldest_offset = current_oldest_offset.saturating_sub(PAGE_SIZE);
        let limit = current_oldest_offset.saturating_sub(oldest_offset).max(1);
        match client
            .session_messages_offset(&session_id, oldest_offset, limit)
            .await
        {
            Ok(page) => {
                let _ = tx
                    .send_wait(CowdEvent::SessionScoped {
                        session_id: session_id.clone(),
                        authority_generation,
                        event: Box::new(CowdEvent::SessionHistoryOlderPage {
                            page,
                            oldest_offset,
                            has_older: oldest_offset > 0,
                        }),
                    })
                    .await;
            }
            Err(error) => {
                let _ = send_session_scoped_with_generation(
                    &tx,
                    &session_id,
                    authority_generation,
                    CowdEvent::SessionHistoryOlderFailed {
                        session_id: session_id.clone(),
                        error: error.to_string(),
                    },
                );
            }
        }
    });
}

fn dispatch_newer_history_page(
    gateway_client: &GatewayApiClient,
    event_tx: &CowdEventSender,
    session_id: &str,
    authority_generation: u64,
    current_end_offset: usize,
    total: usize,
    latest: bool,
) {
    let client = gateway_client.clone();
    let tx = event_tx.clone();
    let session_id = session_id.to_string();
    tokio::spawn(async move {
        const PAGE_SIZE: usize = 500;
        let offset = if latest {
            total.saturating_sub(PAGE_SIZE)
        } else {
            current_end_offset
        };
        let limit = total.saturating_sub(offset).min(PAGE_SIZE).max(1);
        match client
            .session_messages_offset(&session_id, offset, limit)
            .await
        {
            Ok(page) if latest => {
                let _ = tx
                    .send_wait(CowdEvent::SessionScoped {
                        session_id: session_id.clone(),
                        authority_generation,
                        event: Box::new(CowdEvent::SessionHistoryLatestPage {
                            page,
                            oldest_offset: offset,
                        }),
                    })
                    .await;
            }
            Ok(page) => {
                let window_end_offset = offset.saturating_add(page.messages.len());
                let _ = tx
                    .send_wait(CowdEvent::SessionScoped {
                        session_id: session_id.clone(),
                        authority_generation,
                        event: Box::new(CowdEvent::SessionHistoryNewerPage {
                            page,
                            window_end_offset,
                            has_newer: window_end_offset < total,
                        }),
                    })
                    .await;
            }
            Err(error) => {
                let _ = send_session_scoped_with_generation(
                    &tx,
                    &session_id,
                    authority_generation,
                    CowdEvent::SessionHistoryOlderFailed {
                        session_id: session_id.clone(),
                        error: error.to_string(),
                    },
                );
            }
        }
    });
}

async fn prepare_gateway_session_switch(
    gateway_client: &GatewayApiClient,
    target_session_id: &str,
) -> Result<PreparedSessionSwitch, String> {
    let target_session_id = target_session_id.trim();
    if target_session_id.is_empty() {
        return Err("selected session has an empty id".to_string());
    }
    let ensured = gateway_client
        .ensure_session(target_session_id, "")
        .await
        .map_err(|error| format!("target session is unavailable: {error}"))?;
    let writer_attached = gateway_client
        .attach_session(target_session_id, "tui", Some("writer"))
        .await
        .map_err(|error| format!("target lifecycle attach failed: {error}"))?;
    let (attached, lease) = match gateway_client
        .acquire_runtime_session_lease(target_session_id, "collaborative")
        .await
    {
        Ok(lease) => (writer_attached, Some(lease)),
        Err(error) => {
            let _ = gateway_client
                .detach_session(target_session_id, "tui")
                .await;
            let attached = gateway_client
                .attach_session(target_session_id, "tui", Some("reader"))
                .await
                .map_err(|reader_error| {
                    format!(
                        "target writer lease failed ({error}); reader fallback also failed: {reader_error}"
                    )
                })?;
            (attached, None)
        }
    };

    let (execution_index, session_stats, input_projection, execution_policy) = tokio::join!(
        gateway_client.session_execution_index(target_session_id),
        gateway_client.session_stats(target_session_id),
        gateway_client.session_input_projection(target_session_id),
        gateway_client.session_execution_policy(target_session_id),
    );
    let mut warnings = Vec::new();
    if lease.is_none() {
        warnings.push(
            "Target writer lease is held elsewhere; switched in read-only observer mode"
                .to_string(),
        );
    }
    let execution_id = match execution_index {
        Ok(index) => session_index_visible_execution_id(&index),
        Err(error) => {
            warnings.push(format!(
                "Target execution index could not be restored: {error}"
            ));
            None
        }
    };
    let execution_projection = match execution_id.as_deref() {
        Some(execution_id) => match gateway_client
            .execution_projection(execution_id, true)
            .await
        {
            Ok(projection) => Some(projection),
            Err(error) => {
                warnings.push(format!(
                    "Target execution projection is materializing: {error}"
                ));
                None
            }
        },
        None => None,
    };
    let session_stats = match session_stats {
        Ok(stats) => Some(stats),
        Err(error) => {
            warnings.push(format!(
                "Target session statistics could not be restored: {error}"
            ));
            None
        }
    };
    let input_projection = match input_projection {
        Ok(projection) => Some(projection),
        Err(error) => {
            warnings.push(format!(
                "Target queued-input projection could not be restored: {error}"
            ));
            None
        }
    };
    let execution_policy = execution_policy.map_err(|error| {
        format!("Target Session execution policy could not be restored: {error}")
    })?;
    Ok(PreparedSessionSwitch {
        target_session_id: target_session_id.to_string(),
        ensured,
        attached,
        lease,
        execution_projection,
        execution_id,
        session_stats,
        input_projection,
        execution_policy,
        warnings,
    })
}

#[allow(clippy::too_many_arguments)]
fn commit_prepared_session_switch(
    runtime: &tokio::runtime::Runtime,
    gateway_client: &GatewayApiClient,
    event_tx: &CowdEventSender,
    state: &mut TuiState,
    session_apps: &mut BTreeMap<String, App>,
    gateway_lease_owner: &mut Option<String>,
    execution_projection_source: &mut ExecutionProjectionReducerController,
    session_source_bridges: &mut BTreeMap<String, tokio::task::JoinHandle<()>>,
    prepared: PreparedSessionSwitch,
    _observer_id: &str,
    session_authorities: &mut SessionAuthorityRegistry,
) {
    let PreparedSessionSwitch {
        target_session_id,
        ensured,
        attached,
        lease,
        execution_projection,
        execution_id,
        session_stats,
        input_projection,
        execution_policy,
        warnings,
    } = prepared;
    let previous_session_id = state.app.shell.session_id.clone();
    if target_session_id == previous_session_id {
        return;
    }
    execution_projection_source.stop();
    let target_model = ensured
        .get("model")
        .and_then(serde_json::Value::as_str)
        .filter(|model| !model.trim().is_empty())
        .unwrap_or("unresolved")
        .to_string();
    let mut target_app = session_apps
        .remove(&target_session_id)
        .unwrap_or_else(|| App::new(&target_model, &target_session_id));
    target_app.shell.execution_policy_preset = execution_policy_preset(&execution_policy);
    target_app.shell.execution_policy_snapshot = Some(execution_policy.clone());
    target_app.shell.requested_model = (target_model != "unresolved").then(|| target_model.clone());
    if target_app.shell.model == "unresolved" && target_model != "unresolved" {
        target_app.shell.model = target_model;
    }
    if target_app.workbench.file_entries.is_empty() {
        target_app.workbench.file_entries = state.app.workbench.file_entries.clone();
    }
    let previous_app = std::mem::replace(&mut state.app, target_app);
    let authority_generation = session_authorities.begin(&target_session_id);
    state.install_session_authority(authority_generation);
    session_apps.insert(previous_session_id.clone(), previous_app);
    state
        .session
        .session_sidebar
        .set_current_session(&target_session_id);
    *gateway_lease_owner = lease
        .as_ref()
        .and_then(|lease| lease.get("owner"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    state.app.gateway.gateway_lease_owner = gateway_lease_owner.clone();
    state.app.gateway.gateway_lease_mode = lease
        .as_ref()
        .and_then(|lease| lease.get("mode"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| Some("read-only".to_string()));
    if let Some(stats) = session_stats {
        state.app.apply_session_stats(stats);
    }
    if let Some(projection) = input_projection {
        state.app.apply_session_input_projection(projection);
    }
    if let Some(execution_id) = execution_id {
        if let Some(projection_generation) =
            execution_projection_source.begin_selection(&execution_id)
        {
            if let Some(projection) = execution_projection {
                let cursor = projection.cursor;
                let revision = projection.revision;
                state.apply_execution_projection(projection);
                execution_projection_source.switch(
                    gateway_client.clone(),
                    execution_id,
                    cursor,
                    revision,
                    projection_generation,
                    event_tx.clone(),
                );
            } else if execution_projection_source
                .begin_snapshot_request(projection_generation, &execution_id)
            {
                spawn_execution_projection_materialization(
                    gateway_client.clone(),
                    execution_id,
                    projection_generation,
                    event_tx.clone(),
                );
            }
        }
    }
    state.app.add_system_notice(
        SystemNoticeKind::Info,
        &format!(
            "Switched session {previous_session_id} → {target_session_id}; target lifecycle={}, seq={}",
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
    for warning in warnings {
        state
            .app
            .add_system_notice(SystemNoticeKind::Warning, &warning);
    }
    session_source_bridges.retain(|_, task| !task.is_finished());
    if let Some(stale_bridge) = session_source_bridges.remove(&target_session_id) {
        stale_bridge.abort();
    }
    session_source_bridges.insert(
        target_session_id.clone(),
        spawn_session_source_bridge(
            runtime,
            gateway_client.clone(),
            event_tx.clone(),
            target_session_id,
            authority_generation,
        ),
    );
    let client = gateway_client.clone();
    runtime.spawn(async move {
        let _ = client
            .release_runtime_session_lease(&previous_session_id)
            .await;
        let _ = client.detach_session(&previous_session_id, "tui").await;
        let _ = client
            .attach_session(&previous_session_id, "tui", Some("reader"))
            .await;
    });
    state.app.mark_dirty();
}

fn cleanup_stale_prepared_session_switch(
    gateway_client: &GatewayApiClient,
    _observer_id: &str,
    prepared: SessionSwitchResult,
) {
    if prepared.result.is_err() {
        return;
    }
    let client = gateway_client.clone();
    let target_session_id = prepared.target_session_id;
    tokio::spawn(async move {
        let _ = client
            .release_runtime_session_lease(&target_session_id)
            .await;
        let _ = client.detach_session(&target_session_id, "tui").await;
    });
}

fn send_session_scoped_with_generation(
    tx: &CowdEventSender,
    session_id: &str,
    authority_generation: u64,
    event: CowdEvent,
) -> Result<(), ()> {
    tx.send(CowdEvent::SessionScoped {
        session_id: session_id.to_string(),
        authority_generation,
        event: Box::new(event),
    })
    .map_err(|_| ())
}

fn dispatch_gateway_slash(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    state: &mut TuiState,
    session_id: &str,
    authority_generation: u64,
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
    let event_session_id = command_session_id.clone();
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
                let _ = send_session_scoped_with_generation(
                    &command_tx,
                    &event_session_id,
                    authority_generation,
                    CowdEvent::Warning {
                        message: format!("Gateway slash /{command} {status} via {dispatch}"),
                    },
                );
            }
            Err(err) => {
                let _ = send_session_scoped_with_generation(
                    &command_tx,
                    &event_session_id,
                    authority_generation,
                    CowdEvent::TurnError {
                        error: format!("Gateway slash /{command} failed: {err}"),
                    },
                );
            }
        }
    });
    state
        .app
        .add_slash_output(&cmd_name, "Slash dispatched to Gateway");
    state.open_surface_for_slash_result(&cmd_name);
}

fn read_only_slash_command(input: &str) -> bool {
    input
        .trim_start_matches('/')
        .split_whitespace()
        .next()
        .is_some_and(|command| {
            matches!(
                command.to_ascii_lowercase().as_str(),
                "help"
                    | "status"
                    | "context"
                    | "usage"
                    | "model"
                    | "models"
                    | "sessions"
                    | "history"
                    | "tools"
                    | "memory"
            )
        })
}

fn dispatch_gateway_message(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    session_id: &str,
    authority_generation: u64,
    text: String,
    resource_ids: Vec<String>,
    client_message_id: String,
    submission_generation: u64,
    started_new_turn: bool,
) {
    let event_client = gateway_client.clone();
    let event_session_id = session_id.to_string();
    let event_tx = tx.clone();
    spawn_tui_task(tx, async move {
        let send_scoped = |event: CowdEvent| {
            event_tx.send(CowdEvent::SessionScoped {
                session_id: event_session_id.clone(),
                authority_generation,
                event: Box::new(event),
            })
        };
        match event_client
            .send_message_with_resources(
                &event_session_id,
                &text,
                &resource_ids,
                Some(&client_message_id),
            )
            .await
        {
            Ok(value) => {
                let _ = send_scoped(CowdEvent::MessageAdmissionAccepted {
                    session_id: event_session_id.clone(),
                    client_message_id: client_message_id.clone(),
                    submission_generation,
                });
                if let Some(projection) = value.get("input_projection") {
                    let _ = send_scoped(CowdEvent::SessionInputProjection {
                        projection: projection.clone(),
                    });
                }
                if !resource_ids.is_empty() {
                    let _ = send_scoped(CowdEvent::ResourcesCommitted {
                        ids: resource_ids.clone(),
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
                    let _ = send_scoped(CowdEvent::Warning {
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
                    let _ = send_scoped(CowdEvent::Warning {
                        message: format!("Input attached to active turn: {decision}"),
                    });
                    return;
                }
                let _ = send_scoped(CowdEvent::Warning {
                    message: "Gateway accepted input; awaiting durable terminal commit".to_string(),
                });
            }
            Err(err) => {
                let _ = send_scoped(CowdEvent::MessageAdmissionFailed {
                    session_id: event_session_id.clone(),
                    client_message_id,
                    submission_generation,
                    original_text: text,
                    started_new_turn,
                    error: err.to_string(),
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

fn dispatch_gateway_resource_upload(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    session_id: &str,
    authority_generation: u64,
    path: PathBuf,
) {
    let client = gateway_client.clone();
    let tx = tx.clone();
    let session_id = session_id.to_string();
    spawn_tui_task(&tx.clone(), async move {
        match client.upload_resource_path(&path, &session_id).await {
            Ok(value) => {
                let resource = value.get("resource");
                let id = resource
                    .and_then(|resource| resource.get("id"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if id.is_empty() {
                    let _ = send_session_scoped_with_generation(
                        &tx,
                        &session_id,
                        authority_generation,
                        CowdEvent::ResourceUploadFailed {
                            path: path.display().to_string(),
                            error: "Gateway upload response did not contain a resource id"
                                .to_string(),
                        },
                    );
                    return;
                }
                let label = resource
                    .and_then(|resource| resource.get("original_name"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| path.display().to_string());
                let kind = resource
                    .and_then(|resource| resource.get("kind"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("resource")
                    .to_string();
                let _ = send_session_scoped_with_generation(
                    &tx,
                    &session_id,
                    authority_generation,
                    CowdEvent::ResourceUploaded { id, label, kind },
                );
            }
            Err(error) => {
                let _ = send_session_scoped_with_generation(
                    &tx,
                    &session_id,
                    authority_generation,
                    CowdEvent::ResourceUploadFailed {
                        path: path.display().to_string(),
                        error: error.to_string(),
                    },
                );
            }
        }
    });
}

fn dispatch_gateway_cancel(
    gateway_client: &GatewayApiClient,
    tx: &crate::events::CowdEventSender,
    session_id: &str,
    execution_id: Option<&str>,
    turn_id: Option<&str>,
    authority_generation: u64,
) {
    let (Some(execution_id), Some(turn_id)) = (execution_id, turn_id) else {
        let _ = send_session_scoped_with_generation(
            tx,
            session_id,
            authority_generation,
            CowdEvent::TurnError {
                error: "No current execution is available to cancel".to_string(),
            },
        );
        return;
    };
    let cancel_client = gateway_client.clone();
    let cancel_session_id = session_id.to_string();
    let cancel_execution_id = execution_id.to_string();
    let cancel_turn_id = turn_id.to_string();
    let cancel_tx = tx.clone();
    spawn_tui_task(tx, async move {
        match cancel_client
            .cancel_session_turn(
                &cancel_session_id,
                &cancel_execution_id,
                &cancel_turn_id,
                "tui_user_cancel",
            )
            .await
        {
            Ok(receipt) => {
                let correlation = crate::protocol::GatewayEventCorrelation {
                    session_id: receipt.session_id.clone(),
                    execution_id: (!receipt.execution_id.is_empty())
                        .then(|| receipt.execution_id.clone()),
                    turn_id: (!receipt.turn_id.is_empty()).then(|| receipt.turn_id.clone()),
                    ..crate::protocol::GatewayEventCorrelation::default()
                };
                let _ = send_session_scoped_with_generation(
                    &cancel_tx,
                    &cancel_session_id,
                    authority_generation,
                    CowdEvent::GatewaySession {
                        event: crate::protocol::GatewaySessionEvent::TerminalDelivery {
                            correlation,
                            delivery: harness_contract::live::TerminalDeliveryEvent::CancellationCommitted {
                                receipt,
                            },
                        },
                    },
                );
            }
            Err(err) => {
                let _ = send_session_scoped_with_generation(
                    &cancel_tx,
                    &cancel_session_id,
                    authority_generation,
                    CowdEvent::TurnError {
                        error: format!("Gateway cancel request failed: {err}"),
                    },
                );
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
    authority_generation: u64,
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
                if let Some(projection) = receipt
                    .get("input_projection")
                    .filter(|projection| !projection.is_null())
                {
                    let _ = send_session_scoped_with_generation(
                        &task_tx,
                        &session_id,
                        authority_generation,
                        CowdEvent::SessionInputProjection {
                            projection: projection.clone(),
                        },
                    );
                }
                if let Some(warnings) = receipt
                    .get("projection_warnings")
                    .and_then(serde_json::Value::as_array)
                    .filter(|warnings| !warnings.is_empty())
                {
                    let details = warnings
                        .iter()
                        .filter_map(|warning| {
                            let projection = warning
                                .get("projection")
                                .and_then(serde_json::Value::as_str)?;
                            let error = warning
                                .get("error")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("unavailable");
                            Some(format!("{projection}: {error}"))
                        })
                        .collect::<Vec<_>>()
                        .join("; ");
                    let _ = send_session_scoped_with_generation(
                        &task_tx,
                        &session_id,
                        authority_generation,
                        CowdEvent::Warning {
                            message: format!(
                                "Queued input was cancelled, but its runtime projection is degraded: {details}"
                            ),
                        },
                    );
                }
                let _ = send_session_scoped_with_generation(
                    &task_tx,
                    &session_id,
                    authority_generation,
                    CowdEvent::Warning {
                        message: format!("Queued input {input_id} cancelled"),
                    },
                );
            }
            Err(error) => {
                let _ = send_session_scoped_with_generation(
                    &task_tx,
                    &session_id,
                    authority_generation,
                    CowdEvent::TurnError {
                        error: format!("Queued input cancellation failed: {error}"),
                    },
                );
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
    session_id: &str,
    authority_generation: u64,
    command: ExecutionCommandKind,
) {
    let Some(projection) = state.app.execution.latest_execution_projection.as_ref() else {
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
    let session_id = session_id.to_string();
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
                let _ = send_session_scoped_with_generation(
                    &task_tx,
                    &session_id,
                    authority_generation,
                    CowdEvent::Warning {
                        message: format!(
                            "Execution command {:?}: {} (revision {})",
                            command, receipt.status, receipt.accepted_revision
                        ),
                    },
                );
            }
            Err(error) => {
                let _ = send_session_scoped_with_generation(
                    &task_tx,
                    &session_id,
                    authority_generation,
                    CowdEvent::TurnError {
                        error: format!("Execution command {:?} failed: {error}", command),
                    },
                );
            }
        }
    });
}

fn dispatch_pending_app_transport_effects(
    runtime: &tokio::runtime::Runtime,
    state: &mut TuiState,
    gateway_client: &GatewayApiClient,
    event_tx: &CowdEventSender,
    controller: &mut AppTransportController,
) {
    for PendingAppSurfaceCommand {
        session_id,
        authority_generation,
        command,
    } in state.take_pending_app_surface_commands()
    {
        match command {
            AppSurfaceCommand::LoadDetail { request_id, app_id } => {
                let Some(entry) = state.gateway_app_catalog_entry(&app_id) else {
                    state.reject_gateway_app_detail(
                        &app_id,
                        "APP detail request is absent from the current catalog".to_owned(),
                    );
                    continue;
                };
                let client = gateway_client.clone();
                let tx = event_tx.clone();
                runtime.spawn(async move {
                    let event = match client.app_detail(&entry).await {
                        Ok(detail) => AppSurfaceEvent::DetailLoaded {
                            request_id,
                            entry: detail.entry,
                            manifest: detail.manifest,
                            operations: detail.operations,
                        },
                        Err(error) => AppSurfaceEvent::DetailFailed {
                            request_id,
                            app_id,
                            error: format!("signed APP detail is unavailable: {error}"),
                        },
                    };
                    let _ = send_session_scoped_with_generation(
                        &tx,
                        &session_id,
                        authority_generation,
                        CowdEvent::AppSurface { event },
                    );
                });
            }
            AppSurfaceCommand::Open {
                request_id,
                app_id,
                view_id,
                operation_id: _,
                request,
            } => {
                let client = gateway_client.clone();
                let tx = event_tx.clone();
                runtime.spawn(async move {
                    let path = app_view_endpoint(&app_id, &view_id, "open");
                    let event = match app_json_request_with_transient_retry(
                        &client,
                        "POST",
                        &path,
                        serde_json::to_value(request).ok(),
                        &BTreeMap::new(),
                        true,
                    )
                    .await
                    {
                        Ok((status, body)) => AppSurfaceEvent::Response {
                            request_id,
                            app_id,
                            view_id,
                            kind: AppSurfaceRequestKind::Open,
                            status,
                            body,
                        },
                        Err(failure) => AppSurfaceEvent::RequestFailed {
                            request_id,
                            app_id,
                            view_id,
                            kind: AppSurfaceRequestKind::Open,
                            status: failure.status,
                            body: failure.body,
                            error: failure.message,
                        },
                    };
                    let _ = send_session_scoped_with_generation(
                        &tx,
                        &session_id,
                        authority_generation,
                        CowdEvent::AppSurface { event },
                    );
                });
            }
            AppSurfaceCommand::Action {
                request_id,
                app_id,
                view_id,
                operation_id: _,
                action,
            } => {
                let client = gateway_client.clone();
                let tx = event_tx.clone();
                runtime.spawn(async move {
                    let path = app_view_endpoint(&app_id, &view_id, "actions");
                    let body = serde_json::to_value(action).unwrap_or(serde_json::Value::Null);
                    let event = match app_json_request_with_transient_retry(
                        &client,
                        "POST",
                        &path,
                        Some(body),
                        &BTreeMap::new(),
                        false,
                    )
                    .await
                    {
                        Ok((status, body)) => AppSurfaceEvent::Response {
                            request_id,
                            app_id,
                            view_id,
                            kind: AppSurfaceRequestKind::Action,
                            status,
                            body,
                        },
                        Err(failure) => AppSurfaceEvent::RequestFailed {
                            request_id,
                            app_id,
                            view_id,
                            kind: AppSurfaceRequestKind::Action,
                            status: failure.status,
                            body: failure.body,
                            error: failure.message,
                        },
                    };
                    let _ = send_session_scoped_with_generation(
                        &tx,
                        &session_id,
                        authority_generation,
                        CowdEvent::AppSurface { event },
                    );
                });
            }
            AppSurfaceCommand::StreamStart {
                app_id,
                view_id,
                operation_id: _,
                request,
            } => {
                let client = gateway_client.clone();
                let tx = event_tx.clone();
                let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
                let task_app_id = app_id.clone();
                let task_view_id = view_id.clone();
                let task_session_id = session_id.clone();
                let task = runtime.spawn(async move {
                    if let Err(failure) = client
                        .subscribe_app_view_stream(
                            AppViewStreamRequest {
                                app_id: task_app_id.clone(),
                                view_id: task_view_id.clone(),
                                request,
                                session_id: task_session_id.clone(),
                                authority_generation,
                            },
                            cancel_rx,
                            tx.clone(),
                        )
                        .await
                    {
                        let _ = send_session_scoped_with_generation(
                            &tx,
                            &task_session_id,
                            authority_generation,
                            CowdEvent::AppSurface {
                                event: AppSurfaceEvent::StreamDisconnected {
                                    app_id: task_app_id,
                                    view_id: task_view_id,
                                    error: failure.message,
                                },
                            },
                        );
                    }
                });
                controller.insert(app_id, view_id, cancel_tx, task);
            }
            AppSurfaceCommand::StreamCancel { app_id, view_id } => {
                controller.stop(&app_id, &view_id);
            }
        }
    }
}

fn app_view_endpoint(app_id: &str, view_id: &str, operation: &str) -> String {
    let path = match operation {
        "actions" => surface::gateway_api::paths::API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_ACTIONS,
        "open" => surface::gateway_api::paths::API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_OPEN,
        "stream" => surface::gateway_api::paths::API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_STREAM,
        unsupported => panic!("unsupported APP view operation `{unsupported}`"),
    };
    crate::gateway_client_routes::render_route(path, &[app_id.to_owned(), view_id.to_owned()])
}

async fn app_json_request_with_transient_retry(
    client: &GatewayApiClient,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    headers: &BTreeMap<String, String>,
    retryable: bool,
) -> Result<(u16, serde_json::Value), AppTransportFailure> {
    let retryable_read = retryable || is_idempotent_app_read_method(method);
    for attempt in 0..APP_TRANSIENT_REQUEST_RETRY_ATTEMPTS {
        let result = if retryable_read {
            tokio::time::timeout(
                APP_TRANSIENT_REQUEST_ATTEMPT_TIMEOUT,
                client.app_json_request(method, path, body.clone(), headers),
            )
            .await
            .unwrap_or_else(|_| {
                Err(AppTransportFailure {
                    status: None,
                    body: None,
                    message: "Gateway APP read request timed out during recovery".to_string(),
                })
            })
        } else {
            client
                .app_json_request(method, path, body.clone(), headers)
                .await
        };
        match result {
            Ok(response) => return Ok(response),
            Err(failure)
                if retryable_read
                    && is_transient_app_transport_failure(&failure)
                    && attempt + 1 < APP_TRANSIENT_REQUEST_RETRY_ATTEMPTS =>
            {
                tokio::time::sleep(APP_TRANSIENT_REQUEST_RETRY_DELAY).await;
            }
            Err(failure) => return Err(failure),
        }
    }
    Err(AppTransportFailure {
        status: None,
        body: None,
        message: "Gateway APP request retry policy has no executable attempt".to_string(),
    })
}

fn is_idempotent_app_read_method(method: &str) -> bool {
    matches!(method.trim().to_ascii_uppercase().as_str(), "GET" | "HEAD")
}

fn is_transient_app_transport_failure(failure: &AppTransportFailure) -> bool {
    !failure.message.starts_with("APP request ")
        && (failure.status.is_none()
            || matches!(failure.status, Some(502 | 503 | 504))
            || failure.body.as_ref().is_some_and(|body| {
                ["/details/reason", "/error/details/reason"]
                    .into_iter()
                    .any(|pointer| {
                        body.pointer(pointer).and_then(serde_json::Value::as_str)
                            == Some("authority_unavailable")
                    })
            }))
}

#[allow(clippy::too_many_arguments)]
fn route_cowd_event_scope(
    event: CowdEvent,
    state: &mut TuiState,
    execution_projection_source: &mut ExecutionProjectionReducerController,
    session_apps: &mut BTreeMap<String, App>,
    session_authorities: &mut SessionAuthorityRegistry,
    gateway_lease_owner: &mut Option<String>,
    app_transport_controller: &mut AppTransportController,
    session_source_bridges: &mut BTreeMap<String, tokio::task::JoinHandle<()>>,
) -> Option<CowdEvent> {
    let event = match event {
        CowdEvent::SessionScoped {
            session_id,
            authority_generation,
            event: scoped,
        } => {
            if let CowdEvent::SessionAuthorizationRevoked { reason, .. } = scoped.as_ref() {
                let reason = reason.clone();
                if session_authorities.revoke(&session_id, authority_generation) {
                    if let Some(bridge) = session_source_bridges.remove(&session_id) {
                        bridge.abort();
                    }
                    if session_id == state.app.shell.session_id {
                        execution_projection_source.revoke_session_authorization();
                        app_transport_controller.stop_all();
                        *gateway_lease_owner = None;
                        state.app.gateway.gateway_lease_owner = None;
                        state.app.gateway.gateway_lease_mode =
                            Some("authorization-revoked".to_string());
                        state.revoke_session_authority(&reason);
                    } else if let Some(app) = session_apps.get_mut(&session_id) {
                        app.revoke_session_authorization(&reason);
                    }
                }
                return None;
            }
            if !session_authorities.accepts(&session_id, authority_generation) {
                return None;
            }
            if session_id != state.app.shell.session_id {
                if let Some(app) = session_apps.get_mut(&session_id) {
                    app.apply_event(*scoped);
                }
                return None;
            }
            *scoped
        }
        event => {
            if let Some(session_id) = cowd_event_session_id(&event) {
                if session_authorities.current(session_id).is_none() {
                    return None;
                }
                if session_id != state.app.shell.session_id {
                    if let Some(app) = session_apps.get_mut(session_id) {
                        app.apply_event(event);
                    }
                    return None;
                }
            }
            event
        }
    };
    if let CowdEvent::SessionAuthorizationRevoked { session_id, reason } = &event {
        let current_generation = session_authorities.current(session_id);
        if current_generation
            .is_some_and(|generation| session_authorities.revoke(session_id, generation))
        {
            if let Some(bridge) = session_source_bridges.remove(session_id) {
                bridge.abort();
            }
            if session_id == &state.app.shell.session_id {
                app_transport_controller.stop_all();
                *gateway_lease_owner = None;
                state.revoke_session_authority(reason);
            } else if let Some(app) = session_apps.get_mut(session_id) {
                app.revoke_session_authorization(reason);
            }
        }
        execution_projection_source.revoke_session_authorization();
        return None;
    }
    Some(event)
}

async fn drain_cowd_events_state(
    rx: &mut crate::CowdEventReceiver,
    state: &mut TuiState,
    gateway_client: &GatewayApiClient,
    event_tx: &CowdEventSender,
    execution_projection_source: &mut ExecutionProjectionReducerController,
    session_apps: &mut BTreeMap<String, App>,
    session_authorities: &mut SessionAuthorityRegistry,
    gateway_lease_owner: &mut Option<String>,
    app_transport_controller: &mut AppTransportController,
    session_source_bridges: &mut BTreeMap<String, tokio::task::JoinHandle<()>>,
) {
    let mut count = 0;
    let limit = if state.app.turn_is_active() { 64 } else { 256 };
    while let Ok(event) = rx.try_recv() {
        let Some(event) = route_cowd_event_scope(
            event,
            state,
            execution_projection_source,
            session_apps,
            session_authorities,
            gateway_lease_owner,
            app_transport_controller,
            session_source_bridges,
        ) else {
            count += 1;
            if count >= limit {
                break;
            }
            continue;
        };
        let execution_id = event_selected_execution_id(&event, &state.app);
        if let CowdEvent::ExecutionProjectionDelta { generation, delta } = &event {
            if execution_projection_source.accepts(*generation, &delta.execution_id) {
                let apply = execution_projection_source.apply_delta(*generation, delta);
                tracing::debug!(
                    generation,
                    execution_id = %delta.execution_id,
                    from_revision = delta.from_revision,
                    target_revision = delta.target_revision,
                    base_cursor = delta.base_cursor,
                    target_cursor = delta.target_cursor,
                    result = ?apply,
                    "TUI applied canonical execution projection delta"
                );
                if matches!(apply, crate::protocol::ProjectionDeltaApply::Applied) {
                    if let Some(projection) = execution_projection_source
                        .materialized_projection()
                        .cloned()
                    {
                        state.apply_execution_projection(projection);
                    }
                } else if execution_projection_source
                    .begin_snapshot_request(*generation, &delta.execution_id)
                {
                    spawn_execution_projection_refresh(
                        gateway_client.clone(),
                        delta.execution_id.clone(),
                        *generation,
                        event_tx.clone(),
                    );
                }
            }
            count += 1;
            if count >= limit {
                break;
            }
            continue;
        }
        if let CowdEvent::ExecutionProjectionLive { generation, update } = &event {
            if execution_projection_source.accepts(*generation, &update.execution_id) {
                state.apply_execution_live_update(update.clone());
                tracing::debug!(
                    generation,
                    execution_id = %update.execution_id,
                    live_revision = update.live.revision,
                    status = ?update.live.status,
                    "TUI applied canonical execution live update"
                );
            }
            count += 1;
            if count >= limit {
                break;
            }
            continue;
        }
        if let CowdEvent::ExecutionProjectionConnection {
            generation,
            execution_id,
            ..
        } = &event
        {
            if execution_projection_source.accepts(*generation, execution_id) {
                state.apply_event(event);
            }
            count += 1;
            if count >= limit {
                break;
            }
            continue;
        }
        if let CowdEvent::ExecutionProjectionAccessRevoked {
            generation,
            execution_id,
            message,
        } = &event
        {
            if execution_projection_source.accepts(*generation, execution_id) {
                execution_projection_source.clear_selection_if(*generation, execution_id);
                state.invalidate_execution_projection(execution_id, message);
            }
            count += 1;
            if count >= limit {
                break;
            }
            continue;
        }
        if let CowdEvent::ExecutionProjectionRefreshFailed {
            generation,
            execution_id,
            message,
        } = &event
        {
            if execution_projection_source.accepts(*generation, execution_id) {
                execution_projection_source.finish_snapshot_request(*generation, execution_id);
                state
                    .app
                    .add_system_notice(SystemNoticeKind::Warning, message);
            }
            count += 1;
            if count >= limit {
                break;
            }
            continue;
        }
        if let CowdEvent::ExecutionProjectionLoaded {
            generation,
            projection,
        } = &event
        {
            let execution_id = projection.execution_id.clone();
            if execution_projection_source.accepts(*generation, &execution_id) {
                execution_projection_source.finish_snapshot_request(*generation, &execution_id);
                let install = execution_projection_source.install_snapshot(*generation, projection);
                tracing::debug!(
                    generation,
                    execution_id = %execution_id,
                    revision = projection.revision,
                    cursor = projection.cursor,
                    live_revision = projection.live.as_ref().map(|live| live.revision),
                    live_status = ?projection.live.as_ref().map(|live| live.status),
                    input_source = projection.live.as_ref()
                        .and_then(|live| live.context_usage.as_ref())
                        .and_then(|usage| usage.input_source.as_deref()),
                    result = ?install,
                    "TUI installed canonical execution projection snapshot"
                );
                if matches!(
                    install,
                    crate::protocol::ProjectionDeltaApply::ResyncRequired
                ) {
                    state.app.add_system_notice(
                        SystemNoticeKind::Warning,
                        "Execution projection snapshot failed the local cursor/schema guard",
                    );
                    count += 1;
                    if count >= limit {
                        break;
                    }
                    continue;
                }
                state.apply_execution_projection(projection.clone());
                execution_projection_source.switch(
                    gateway_client.clone(),
                    execution_id,
                    projection.cursor,
                    projection.revision,
                    *generation,
                    event_tx.clone(),
                );
            }
            count += 1;
            if count >= limit {
                break;
            }
            continue;
        }
        let mut materialization = None;
        let terminal_execution_id = match &event {
            CowdEvent::GatewaySession {
                event: crate::protocol::GatewaySessionEvent::TerminalCommitted { correlation, .. },
            } => correlation.execution_id.clone(),
            _ => None,
        };
        if let Some(next_execution_id) = execution_id.as_deref() {
            let previous_execution_id = execution_projection_source
                .selected_execution_id()
                .or_else(|| {
                    state
                        .app
                        .execution
                        .latest_execution_projection
                        .as_ref()
                        .map(|projection| projection.execution_id.clone())
                });
            if let Some(generation) = execution_projection_source.begin_selection(next_execution_id)
            {
                tracing::debug!(
                    generation,
                    execution_id = %next_execution_id,
                    "TUI selected canonical execution projection"
                );
                state.app.execution.projection_connection_state =
                    Some(crate::protocol::SessionStreamConnectionState::Connecting);
                if let Some(previous_execution_id) = previous_execution_id
                    .filter(|previous_execution_id| previous_execution_id != next_execution_id)
                {
                    state.invalidate_execution_projection(
                        &previous_execution_id,
                        "Runtime selected a new execution; loading its canonical projection",
                    );
                }
                materialization = Some((next_execution_id.to_string(), generation));
            }
        }
        state.apply_event(event);
        if let Some((execution_id, generation)) = materialization {
            if execution_projection_source.begin_snapshot_request(generation, &execution_id) {
                spawn_execution_projection_materialization(
                    gateway_client.clone(),
                    execution_id,
                    generation,
                    event_tx.clone(),
                );
            }
        }
        if let Some(execution_id) = terminal_execution_id {
            let generation = execution_projection_source
                .selected_generation(&execution_id)
                .or_else(|| execution_projection_source.begin_selection(&execution_id));
            if let Some(generation) = generation {
                spawn_execution_projection_terminal_convergence(
                    gateway_client.clone(),
                    execution_id,
                    generation,
                    event_tx.clone(),
                );
            }
        }
        count += 1;
        if count >= limit {
            break;
        }
    }
}

fn cowd_event_session_id(event: &CowdEvent) -> Option<&str> {
    match event {
        CowdEvent::SessionScoped { session_id, .. }
        | CowdEvent::SessionHistoryHydrationFailed { session_id, .. }
        | CowdEvent::SessionHistoryHydrated { session_id, .. }
        | CowdEvent::SessionHistoryOlderFailed { session_id, .. }
        | CowdEvent::MessageAdmissionAccepted { session_id, .. }
        | CowdEvent::MessageAdmissionFailed { session_id, .. }
        | CowdEvent::SessionAuthorizationRevoked { session_id, .. }
        | CowdEvent::SessionStreamConnection { session_id, .. } => Some(session_id),
        CowdEvent::SessionHistoryPage { page }
        | CowdEvent::SessionHistoryCatchupPage { page }
        | CowdEvent::SessionHistoryOlderPage { page, .. }
        | CowdEvent::SessionHistoryNewerPage { page, .. }
        | CowdEvent::SessionHistoryLatestPage { page, .. } => Some(&page.session_id),
        CowdEvent::GatewaySession { event } => Some(match event {
            crate::protocol::GatewaySessionEvent::UserMessageCommitted { correlation, .. }
            | crate::protocol::GatewaySessionEvent::TextDelta { correlation, .. }
            | crate::protocol::GatewaySessionEvent::TerminalDelivery { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ReasoningSummaryDelta { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ModelStepStarted { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ModelStepCompleted { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ItemStarted { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ItemCompleted { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ToolStart { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ToolProgress { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ToolComplete { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ExecutionPhase { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ProviderAttempt { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ContextEnvelope { correlation, .. }
            | crate::protocol::GatewaySessionEvent::ContextWindow { correlation, .. }
            | crate::protocol::GatewaySessionEvent::TokenUsage { correlation, .. }
            | crate::protocol::GatewaySessionEvent::RunModelTelemetry { correlation, .. }
            | crate::protocol::GatewaySessionEvent::TerminalCommitted { correlation, .. }
            | crate::protocol::GatewaySessionEvent::TurnError { correlation, .. } => {
                &correlation.session_id
            }
        }),
        _ => None,
    }
}

fn event_selected_execution_id(event: &CowdEvent, app: &crate::App) -> Option<String> {
    match event {
        CowdEvent::ExecutionGraphSummary { summary } => {
            let incoming = summary.graph_id.as_deref()?;
            (!app.execution_is_terminalized(incoming)
                && (app.execution.current_execution_id.is_none()
                    || app.execution.current_execution_id.as_deref() == Some(incoming)
                    || !app.turn_is_active()
                    || app.execution.current_execution_status.is_some_and(
                        harness_contract::projection::ExecutionLiveStatus::is_terminal,
                    )))
            .then(|| incoming.to_string())
        }
        CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::UserMessageCommitted { correlation, .. },
        } => {
            let incoming = correlation.execution_id.as_deref()?;
            (app.execution.current_execution_id.is_none()
                || app.execution.current_execution_id.as_deref() == Some(incoming)
                || !app.turn_is_active()
                || app
                    .execution
                    .current_execution_status
                    .is_some_and(harness_contract::projection::ExecutionLiveStatus::is_terminal))
            .then(|| incoming.to_string())
        }
        CowdEvent::GatewaySession {
            event:
                crate::protocol::GatewaySessionEvent::ExecutionPhase {
                    correlation,
                    status,
                    ..
                },
        } if *status != harness_contract::projection::ExecutionLiveStatus::Queued => correlation
            .execution_id
            .as_deref()
            .filter(|execution_id| !app.execution_is_terminalized(execution_id))
            .map(ToOwned::to_owned),
        _ => None,
    }
}

fn session_index_visible_execution_id(
    index: &crate::protocol::SessionExecutionIndexProjection,
) -> Option<String> {
    if index
        .latest_status
        .is_some_and(|status| status == harness_contract::projection::ExecutionLiveStatus::Queued)
    {
        return index
            .active_execution_ids
            .first()
            .cloned()
            .or_else(|| index.latest_execution_id.clone());
    }
    index
        .latest_execution_id
        .clone()
        .or_else(|| index.active_execution_ids.first().cloned())
}

fn spawn_execution_projection_materialization(
    gateway_client: GatewayApiClient,
    execution_id: String,
    generation: u64,
    event_tx: CowdEventSender,
) {
    let Some(runtime) = shared_rt() else {
        let _ = event_tx.send(CowdEvent::Warning {
            message: "Execution projection observer is unavailable; TUI remains in accepted/materializing state".to_string(),
        });
        return;
    };
    runtime.spawn(async move {
        let mut last_error = None;
        for attempt in 0..=EXECUTION_PROJECTION_MATERIALIZATION_DELAYS.len() {
            if attempt > 0 {
                tokio::time::sleep(EXECUTION_PROJECTION_MATERIALIZATION_DELAYS[attempt - 1]).await;
            }
            match gateway_client.execution_projection(&execution_id, true).await {
                Ok(projection) => {
                    let _ = event_tx.send(CowdEvent::ExecutionProjectionLoaded {
                        generation,
                        projection,
                    });
                    return;
                }
                Err(error) if projection_access_or_contract_error(&error) => {
                    let _ = event_tx.send(CowdEvent::ExecutionProjectionAccessRevoked {
                        generation,
                        execution_id,
                        message: format!(
                            "Execution projection authorization or contract changed while materializing: {error}"
                        ),
                    });
                    return;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let _ = event_tx.send(CowdEvent::ExecutionProjectionRefreshFailed {
            generation,
            execution_id,
            message: format!(
                "Execution projection did not materialize within the bounded observer window: {}",
                last_error.map_or_else(|| "unknown error".to_string(), |error| error.to_string())
            ),
        });
    });
}

/// A durable terminal closes the Session stream before independently
/// delivered execution-projection updates are guaranteed to reach a Surface.
/// Reconcile one canonical terminal snapshot so exact provider/context facts
/// cannot remain stuck behind an earlier request-budget estimate.
fn spawn_execution_projection_terminal_convergence(
    gateway_client: GatewayApiClient,
    execution_id: String,
    generation: u64,
    event_tx: CowdEventSender,
) {
    let Some(runtime) = shared_rt() else {
        let _ = event_tx.send(CowdEvent::ExecutionProjectionRefreshFailed {
            generation,
            execution_id,
            message: "Terminal execution projection convergence is unavailable because the TUI async runtime is not running"
                .to_string(),
        });
        return;
    };
    runtime.spawn(async move {
        let mut last_error = None;
        for attempt in 0..=EXECUTION_PROJECTION_MATERIALIZATION_DELAYS.len() {
            if attempt > 0 {
                tokio::time::sleep(EXECUTION_PROJECTION_MATERIALIZATION_DELAYS[attempt - 1]).await;
            }
            match gateway_client.execution_projection(&execution_id, true).await {
                Ok(projection)
                    if projection
                        .live
                        .as_ref()
                        .is_some_and(|live| live.status.is_terminal()) =>
                {
                    let _ = event_tx.send(CowdEvent::ExecutionProjectionLoaded {
                        generation,
                        projection,
                    });
                    return;
                }
                Ok(projection) => {
                    last_error = Some(format!(
                        "projection is not terminal yet (revision={}, cursor={}, live={:?})",
                        projection.revision,
                        projection.cursor,
                        projection.live.as_ref().map(|live| live.status)
                    ));
                }
                Err(error) if projection_access_or_contract_error(&error) => {
                    let _ = event_tx.send(CowdEvent::ExecutionProjectionAccessRevoked {
                        generation,
                        execution_id,
                        message: format!(
                            "Terminal execution projection authority changed while converging: {error}"
                        ),
                    });
                    return;
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        let _ = event_tx.send(CowdEvent::ExecutionProjectionRefreshFailed {
            generation,
            execution_id,
            message: format!(
                "Canonical execution projection did not converge after durable terminal: {}",
                last_error.unwrap_or_else(|| "unknown error".to_string())
            ),
        });
    });
}

/// Fetch the latest canonical projection outside the render/event drain.
/// Snapshot refreshes are coalesced by `ExecutionProjectionReducerController`,
/// so high-rate graph deltas cannot create an HTTP fan-out or block keystrokes.
fn spawn_execution_projection_refresh(
    gateway_client: GatewayApiClient,
    execution_id: String,
    generation: u64,
    event_tx: CowdEventSender,
) {
    let Some(runtime) = shared_rt() else {
        let _ = event_tx.send(CowdEvent::ExecutionProjectionRefreshFailed {
            generation,
            execution_id,
            message: "Execution projection refresh is unavailable because the TUI async runtime is not running"
                .to_string(),
        });
        return;
    };
    runtime.spawn(async move {
        match gateway_client.execution_projection(&execution_id, true).await {
            Ok(projection) => {
                let _ = event_tx.send(CowdEvent::ExecutionProjectionLoaded {
                    generation,
                    projection,
                });
            }
            Err(error) if projection_access_or_contract_error(&error) => {
                let _ = event_tx.send(CowdEvent::ExecutionProjectionAccessRevoked {
                    generation,
                    execution_id,
                    message: format!(
                        "Execution projection authorization or contract changed while refreshing: {error}"
                    ),
                });
            }
            Err(error) => {
                let _ = event_tx.send(CowdEvent::ExecutionProjectionRefreshFailed {
                    generation,
                    execution_id,
                    message: format!("Execution projection resync failed: {error}"),
                });
            }
        }
    });
}

fn spawn_execution_projection_source(
    gateway_client: GatewayApiClient,
    execution_id: String,
    initial_cursor: u64,
    initial_revision: u64,
    generation: u64,
    event_tx: CowdEventSender,
) -> Option<tokio::task::JoinHandle<()>> {
    let failure_tx = event_tx.clone();
    let Some(runtime) = shared_rt() else {
        let _ = failure_tx.send(CowdEvent::TurnError {
            error: "TUI async runtime is unavailable; restart the terminal session".to_string(),
        });
        return None;
    };
    Some(runtime.spawn(async move {
        let mut cursor = initial_cursor;
        let mut revision = initial_revision;
        let mut retry_delay = Duration::from_millis(250);
        let mut reconnect_attempts = 0_u64;
        let _ = event_tx.send(CowdEvent::ExecutionProjectionConnection {
            generation,
            execution_id: execution_id.clone(),
            state: crate::protocol::SessionStreamConnectionState::Connecting,
        });
        loop {
            let cursor_before_attempt = cursor;
            let mut closed_without_progress = false;
            let stream_client = gateway_client.clone();
            let mut subscription = Box::pin(
                stream_client.consume_execution_live_source(
                    &execution_id,
                    cursor,
                    revision,
                    true,
                    generation,
                    event_tx.clone(),
                ),
            );
            let mut terminal_watchdog = tokio::time::interval(
                EXECUTION_PROJECTION_TERMINAL_WATCHDOG_INTERVAL,
            );
            terminal_watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // `interval` fires immediately once.  Preserve SSE as the primary
            // path and wait one full interval before the first REST fallback.
            terminal_watchdog.tick().await;
            let subscription_result = loop {
                tokio::select! {
                    result = &mut subscription => break result,
                    _ = terminal_watchdog.tick() => {
                        match gateway_client.execution_projection(&execution_id, true).await {
                            Ok(projection) if projection.live.as_ref().is_some_and(|live| live.status.is_terminal()) => {
                                let _ = event_tx.send(CowdEvent::ExecutionProjectionLoaded {
                                    generation,
                                    projection,
                                });
                                // The loaded terminal snapshot makes further
                                // execution-stream observation unnecessary.
                                return;
                            }
                            Ok(_) => {}
                            Err(error) if projection_access_or_contract_error(&error) => {
                                let _ = event_tx.send(CowdEvent::ExecutionProjectionAccessRevoked {
                                    generation,
                                    execution_id: execution_id.clone(),
                                    message: format!(
                                        "Execution projection authority changed during terminal convergence: {error}"
                                    ),
                                });
                                return;
                            }
                            // A transient REST failure must not tear down an
                            // otherwise healthy SSE source.  The next bounded
                            // tick retries without surfacing duplicate noise.
                            Err(_) => {}
                        }
                    }
                }
            };
            match subscription_result {
                Ok((next_cursor, next_revision)) => {
                    cursor = cursor.max(next_cursor);
                    revision = revision.max(next_revision);
                    if cursor > cursor_before_attempt {
                        reconnect_attempts = 0;
                        retry_delay = Duration::from_millis(250);
                    } else {
                        reconnect_attempts = reconnect_attempts.saturating_add(1);
                        closed_without_progress = true;
                    }
                }
                Err(error) => {
                    if projection_access_or_contract_error(&error) {
                        let _ = event_tx.send(CowdEvent::ExecutionProjectionAccessRevoked {
                            generation,
                            execution_id: execution_id.clone(),
                            message: format!(
                                "Execution projection stream authorization or contract changed: {error}"
                            ),
                        });
                        break;
                    }
                    reconnect_attempts = reconnect_attempts.saturating_add(1);
                    if reconnect_attempts == 1 || reconnect_attempts % 12 == 0 {
                        let _ = event_tx.send(CowdEvent::Warning {
                            message: format!(
                                "Execution projection stream interrupted for {execution_id}: {error}; retrying (attempt {reconnect_attempts})"
                            ),
                        });
                    }
                }
            }
            let _ = event_tx.send(CowdEvent::ExecutionProjectionConnection {
                generation,
                execution_id: execution_id.clone(),
                state: crate::protocol::SessionStreamConnectionState::Reconnecting {
                    attempt: u32::try_from(reconnect_attempts).unwrap_or(u32::MAX),
                    after_cursor: Some(cursor),
                },
            });
            // This observer is intentionally long-lived. A normal network can
            // be offline for more than a fixed retry budget; exponential
            // backoff prevents a busy loop while preserving eventual recovery.
            if closed_without_progress
                && reconnect_attempts > 0
                && (reconnect_attempts == 1 || reconnect_attempts % 12 == 0)
            {
                let _ = event_tx.send(CowdEvent::Warning {
                    message: format!(
                        "Execution projection stream for {execution_id} closed without progress; retrying from cursor {cursor} (attempt {reconnect_attempts})"
                    ),
                });
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
        }
    }))
}

fn projection_access_or_contract_error(error: &crate::gateway_client::GatewayApiError) -> bool {
    matches!(
        error,
        crate::gateway_client::GatewayApiError::Status(
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN,
            _
        ) | crate::gateway_client::GatewayApiError::Contract(_)
    )
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

fn execution_policy_preset(
    policy: &harness_contract::policy::SessionExecutionPolicyResponse,
) -> String {
    policy.matched_preset.map_or_else(
        || "custom".to_string(),
        |preset| preset.as_str().to_string(),
    )
}

async fn resolve_tui_session_execution_policy(
    gateway_client: &GatewayApiClient,
    session_id: &str,
    requested_preset: Option<&str>,
    may_update: bool,
) -> Result<
    harness_contract::policy::SessionExecutionPolicyResponse,
    crate::gateway_client::GatewayApiError,
> {
    let current = gateway_client.session_execution_policy(session_id).await?;
    let Some(requested_preset) = requested_preset else {
        return Ok(current);
    };
    if execution_policy_preset(&current) == requested_preset {
        return Ok(current);
    }
    if !may_update {
        return Err(crate::gateway_client::GatewayApiError::Contract(
            "the requested startup execution policy requires Session writer ownership".to_string(),
        ));
    }
    let revision = current.policy.revision;
    gateway_client
        .update_session_execution_policy(session_id, requested_preset, revision)
        .await
}

fn format_startup_banner(model: &str, execution_policy: &str, session_id: &str) -> String {
    let directory = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .display()
        .to_string();
    format!(
        "COWD v{} | model={} | directory={} | mode={} | session={}",
        env!("CARGO_PKG_VERSION"),
        model,
        directory,
        execution_policy,
        session_id
    )
}

fn format_connected_line(model: &str) -> String {
    if model == "unresolved" {
        return "Provider/model pending canonical Runtime telemetry".to_string();
    }
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

    #[tokio::test]
    async fn background_session_terminal_is_reduced_without_touching_the_active_view() {
        let (tx, mut rx) = crate::cowd_event_channel();
        let mut state = TuiState::new("model-b", "session-b");
        let mut session_apps =
            BTreeMap::from([("session-a".to_string(), App::new("model-a", "session-a"))]);
        let mut session_authorities = SessionAuthorityRegistry::default();
        let authority_generation = session_authorities.begin("session-a");
        session_authorities.begin("session-b");
        tx.send(CowdEvent::SessionScoped {
            session_id: "session-a".to_string(),
            authority_generation,
            event: Box::new(CowdEvent::GatewaySession {
                event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                    correlation: crate::protocol::GatewayEventCorrelation {
                        session_id: "session-a".to_string(),
                        execution_id: Some("execution-a".to_string()),
                        turn_id: Some("turn-a".to_string()),
                        part_id: Some("item-text-1:text:0".to_string()),
                        message_id: Some("assistant-a".to_string()),
                        terminal_id: Some("terminal-a".to_string()),
                        commit_cursor: Some(7),
                        replayed: false,
                        ..Default::default()
                    },
                    assistant_text: "background result".to_string(),
                    sequence: Some(1),
                    iterations: 1,
                    token_usage: None,
                },
            }),
        })
        .expect("queue background terminal");
        let client = GatewayApiClient::new("http://127.0.0.1:1", None).expect("client");
        let mut projection_stream = ExecutionProjectionReducerController::default();
        let mut gateway_lease_owner = None;
        let mut app_transport = AppTransportController::default();
        let mut session_source_bridges = BTreeMap::new();

        drain_cowd_events_state(
            &mut rx,
            &mut state,
            &client,
            &tx,
            &mut projection_stream,
            &mut session_apps,
            &mut session_authorities,
            &mut gateway_lease_owner,
            &mut app_transport,
            &mut session_source_bridges,
        )
        .await;

        assert_eq!(state.app.shell.session_id, "session-b");
        assert!(state.app.timeline_iter().all(|(_, entry)| !matches!(
            entry,
            crate::app::TimelineEntry::Message { content, .. }
                if content == "background result"
        )));
        let background = session_apps.get("session-a").expect("background app");
        assert!(background.timeline_iter().any(|(_, entry)| matches!(
            entry,
            crate::app::TimelineEntry::Message {
                role,
                content,
                identity: Some(identity),
                ..
            } if role == "assistant"
                && content == "background result"
                && identity.message_id.as_deref() == Some("assistant-a")
        )));
    }

    #[test]
    fn durable_ingress_selects_the_canonical_execution_projection() {
        let event = CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
                correlation: crate::protocol::GatewayEventCorrelation {
                    session_id: "session-1".to_string(),
                    execution_id: Some("execution-1".to_string()),
                    turn_id: Some("turn-1".to_string()),
                    message_id: Some("message-1".to_string()),
                    ..crate::protocol::GatewayEventCorrelation::default()
                },
                content: "hello".to_string(),
                sequence: 0,
                created_at_ms: 1,
            },
        };
        let app = crate::App::new("model", "session-1");

        assert_eq!(
            event_selected_execution_id(&event, &app).as_deref(),
            Some("execution-1")
        );
        assert!(event_selected_execution_id(
            &CowdEvent::GatewaySession {
                event: crate::protocol::GatewaySessionEvent::TextDelta {
                    correlation: crate::protocol::GatewayEventCorrelation {
                        session_id: "session-1".to_string(),
                        execution_id: Some("stale-execution".to_string()),
                        turn_id: Some("stale-turn".to_string()),
                        ..crate::protocol::GatewayEventCorrelation::default()
                    },
                    text: "late".to_string(),
                    start_bytes: 0,
                    end_bytes: 4,
                    stream_revision: 4,
                },
            },
            &app
        )
        .is_none());

        let mut running_app = crate::App::new("model", "session-1");
        running_app
            .execution
            .turn_interaction
            .ingress_accepted("execution-running");
        running_app.execution.current_execution_id = Some("execution-running".to_string());
        running_app.execution.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::CallingModel);
        assert!(
            event_selected_execution_id(&event, &running_app).is_none(),
            "a queued follow-up cannot steal the visible running projection"
        );
        assert!(
            event_selected_execution_id(
                &CowdEvent::ExecutionGraphSummary {
                    summary: crate::RuntimeExecutionGraphSummary {
                        graph_id: Some("execution-stale".to_string()),
                        board_id: None,
                        status: "terminal".to_string(),
                        agent_tasks: 0,
                        child_executions: 0,
                        memory_candidates: 0,
                        conflicts: 0,
                        completion_rate: Some(1.0),
                        synthesis_lift: None,
                        complementarity_score: None,
                    },
                },
                &running_app,
            )
            .is_none(),
            "a delayed graph summary cannot steal an active execution selection"
        );
        let started_followup = CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::ExecutionPhase {
                correlation: crate::protocol::GatewayEventCorrelation {
                    session_id: "session-1".to_string(),
                    execution_id: Some("execution-1".to_string()),
                    turn_id: Some("turn-1".to_string()),
                    ..crate::protocol::GatewayEventCorrelation::default()
                },
                status: harness_contract::projection::ExecutionLiveStatus::PreparingContext,
                detail: None,
            },
        };
        assert_eq!(
            event_selected_execution_id(&started_followup, &running_app).as_deref(),
            Some("execution-1")
        );

        let mut terminalized_app = crate::App::new("model", "session-1");
        terminalized_app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                correlation: crate::protocol::GatewayEventCorrelation {
                    session_id: "session-1".to_string(),
                    execution_id: Some("execution-1".to_string()),
                    turn_id: Some("turn-1".to_string()),
                    message_id: Some("assistant-1".to_string()),
                    terminal_id: Some("terminal-1".to_string()),
                    ..crate::protocol::GatewayEventCorrelation::default()
                },
                assistant_text: "done".to_string(),
                sequence: Some(1),
                iterations: 1,
                token_usage: None,
            },
        });
        assert!(
            event_selected_execution_id(&started_followup, &terminalized_app).is_none(),
            "a delayed non-terminal phase cannot reopen a durably terminalized execution"
        );
    }

    #[test]
    fn attach_prefers_the_running_head_over_a_newer_queued_followup() {
        let index = crate::protocol::SessionExecutionIndexProjection {
            session_id: "session-1".to_string(),
            executions: Vec::new(),
            active_execution_ids: vec![
                "execution-running".to_string(),
                "execution-queued".to_string(),
            ],
            latest_execution_id: Some("execution-queued".to_string()),
            latest_graph_id: None,
            latest_status: Some(harness_contract::projection::ExecutionLiveStatus::Queued),
            latest_live_revision: Some(0),
            last_progress_at_ms: Some(1),
            terminal_ref: None,
        };

        assert_eq!(
            session_index_visible_execution_id(&index).as_deref(),
            Some("execution-running")
        );
    }

    #[tokio::test]
    async fn execution_projection_source_generation_rejects_zombie_and_replayed_events() {
        let mut controller = ExecutionProjectionReducerController::default();
        let first_generation = controller
            .begin_selection("execution-a")
            .expect("new selection");
        let first_task = tokio::spawn(std::future::pending::<()>());
        let first_abort = first_task.abort_handle();
        controller.active = Some(ActiveExecutionProjectionSource {
            execution_id: "execution-a".to_string(),
            generation: first_generation,
            task: first_task,
        });
        assert!(controller.accepts(first_generation, "execution-a"));

        let second_generation = controller
            .begin_selection("execution-b")
            .expect("new selection");
        tokio::task::yield_now().await;
        assert!(first_abort.is_finished());
        let second_task = tokio::spawn(std::future::pending::<()>());
        controller.active = Some(ActiveExecutionProjectionSource {
            execution_id: "execution-b".to_string(),
            generation: second_generation,
            task: second_task,
        });
        assert!(!controller.accepts(first_generation, "execution-a"));
        assert!(controller.accepts(second_generation, "execution-b"));

        let third_generation = controller
            .begin_selection("execution-a")
            .expect("new selection");
        let third_task = tokio::spawn(std::future::pending::<()>());
        controller.active = Some(ActiveExecutionProjectionSource {
            execution_id: "execution-a".to_string(),
            generation: third_generation,
            task: third_task,
        });
        assert!(
            !controller.accepts(first_generation, "execution-a"),
            "a queued delta from the first A stream must not revive after A→B→A"
        );
        assert!(controller.accepts(third_generation, "execution-a"));
    }

    #[tokio::test]
    async fn late_old_rest_snapshot_response_cannot_replace_the_new_selection() {
        use harness_contract::execution_graph::{project_execution_graph, ExecutionGraph};
        use harness_contract::projection::ExecutionProjection;

        let (tx, mut rx) = crate::cowd_event_channel();
        let mut state = TuiState::new("model", "session-rest-generation");
        let mut controller = ExecutionProjectionReducerController::default();
        let old_generation = controller
            .begin_selection("execution-old")
            .expect("old selection");
        assert!(controller.begin_snapshot_request(old_generation, "execution-old"));
        let new_generation = controller
            .begin_selection("execution-new")
            .expect("new selection");
        assert!(controller.begin_snapshot_request(new_generation, "execution-new"));
        tx.send(CowdEvent::ExecutionProjectionLoaded {
            generation: old_generation,
            projection: ExecutionProjection {
                schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
                execution_id: "execution-old".to_string(),
                revision: 9,
                cursor: 9,
                detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
                authorization_revision: 1,
                redaction_revision: "redaction-1".to_string(),
                session_id: Some("session-rest-generation".to_string()),
                mission_id: None,
                task_id: None,
                turn_id: None,
                strategy: None,
                graph: project_execution_graph(&ExecutionGraph::new("stale REST snapshot")),
                concurrency: Default::default(),
                child_executions: Vec::new(),
                activities: Vec::new(),
                activity_relations: Vec::new(),
                goals: Vec::new(),
                agents: Vec::new(),
                teams: Vec::new(),
                relations: Vec::new(),
                approvals: Vec::new(),
                admissions: Vec::new(),
                outcomes: Vec::new(),
                interventions: Vec::new(),
                usage: Vec::new(),
                context: Vec::new(),
                evidence: Vec::new(),
                health: Vec::new(),
                recovery: Vec::new(),
                live: None,
                delivery_envelope: None,
                terminal_presentation: None,
                cancellation_receipt: None,
                available_commands: Vec::new(),
            },
        })
        .expect("queue old REST response");

        let client = GatewayApiClient::new("http://127.0.0.1:1", None).expect("client");
        let mut session_apps = BTreeMap::new();
        let mut authorities = SessionAuthorityRegistry::default();
        authorities.begin("session-rest-generation");
        let mut lease_owner = None;
        let mut app_transport = AppTransportController::default();
        let mut bridges = BTreeMap::new();
        drain_cowd_events_state(
            &mut rx,
            &mut state,
            &client,
            &tx,
            &mut controller,
            &mut session_apps,
            &mut authorities,
            &mut lease_owner,
            &mut app_transport,
            &mut bridges,
        )
        .await;

        assert_eq!(
            controller.selected_execution_id().as_deref(),
            Some("execution-new")
        );
        assert!(state.app.execution.latest_execution_projection.is_none());
        let pending = controller
            .snapshot_request
            .as_ref()
            .expect("new REST request remains pending");
        assert_eq!(pending.generation, new_generation);
        assert_eq!(pending.execution_id, "execution-new");
    }

    #[tokio::test]
    async fn e10_session_authorization_revoke_aborts_projection_and_rejects_queued_results() {
        let mut controller = ExecutionProjectionReducerController::default();
        let generation = controller
            .begin_selection("execution-sensitive")
            .expect("new selection");
        assert!(controller.begin_snapshot_request(generation, "execution-sensitive"));
        let task = tokio::spawn(std::future::pending::<()>());
        let abort = task.abort_handle();
        controller.active = Some(ActiveExecutionProjectionSource {
            execution_id: "execution-sensitive".to_string(),
            generation,
            task,
        });

        controller.revoke_session_authorization();
        tokio::task::yield_now().await;

        assert!(abort.is_finished());
        assert!(!controller.accepts(generation, "execution-sensitive"));
        assert!(controller.selected_execution_id().is_none());
        assert!(controller.snapshot_request.is_none());
    }

    #[test]
    fn session_stream_http_authorization_failure_becomes_scoped_terminal_revoke() {
        let (tx, mut rx) = crate::cowd_event_channel();
        let error = crate::gateway_client::GatewayApiError::Status(
            reqwest::StatusCode::FORBIDDEN,
            "credential epoch changed".to_string(),
        );

        send_session_stream_authorization_revoke(&tx, "session-sensitive", 41, &error)
            .expect("authorization failure must become a typed revoke");

        assert!(matches!(
            rx.try_recv().expect("revoke"),
            CowdEvent::SessionScoped {
                session_id,
                authority_generation: 41,
                event,
            } if session_id == "session-sensitive"
                && matches!(
                    event.as_ref(),
                    CowdEvent::SessionAuthorizationRevoked {
                        session_id,
                        reason,
                    } if session_id == "session-sensitive"
                        && reason.contains("403")
                        && reason.contains("credential epoch changed")
                )
        ));
    }

    #[tokio::test]
    async fn e10_revoked_authority_rejects_late_history_session_resource_and_app_results() {
        let (tx, mut rx) = crate::cowd_event_channel();
        let mut state = TuiState::new("model-sensitive", "session-sensitive");
        state.app.gateway.gateway_lease_owner = Some("writer-a".to_string());
        state.app.gateway.gateway_lease_mode = Some("collaborative".to_string());
        let mut gateway_lease_owner = Some("writer-a".to_string());
        let mut session_apps = BTreeMap::new();
        let mut authorities = SessionAuthorityRegistry::default();
        let authority_generation = authorities.begin("session-sensitive");
        let mut projection_stream = ExecutionProjectionReducerController::default();
        let projection_generation = projection_stream
            .begin_selection("execution-sensitive")
            .expect("projection selection");
        let projection_task = tokio::spawn(std::future::pending::<()>());
        let projection_abort = projection_task.abort_handle();
        projection_stream.active = Some(ActiveExecutionProjectionSource {
            execution_id: "execution-sensitive".to_string(),
            generation: projection_generation,
            task: projection_task,
        });
        let bridge = tokio::spawn(std::future::pending::<()>());
        let bridge_abort = bridge.abort_handle();
        let mut bridges = BTreeMap::from([("session-sensitive".to_string(), bridge)]);
        let mut app_transport = AppTransportController::default();
        let client = GatewayApiClient::new("http://127.0.0.1:1", None).expect("client");

        tx.send(CowdEvent::SessionScoped {
            session_id: "session-sensitive".to_string(),
            authority_generation,
            event: Box::new(CowdEvent::SessionAuthorizationRevoked {
                session_id: "session-sensitive".to_string(),
                reason: "test authority revoked".to_string(),
            }),
        })
        .expect("queue revoke");
        drain_cowd_events_state(
            &mut rx,
            &mut state,
            &client,
            &tx,
            &mut projection_stream,
            &mut session_apps,
            &mut authorities,
            &mut gateway_lease_owner,
            &mut app_transport,
            &mut bridges,
        )
        .await;
        let render_version_after_revoke = state.app.timeline.render_version;

        tx.send(CowdEvent::SessionScoped {
            session_id: "session-sensitive".to_string(),
            authority_generation,
            event: Box::new(CowdEvent::SessionHistoryPage {
                page: crate::protocol::SessionMessagesPage {
                    session_id: "session-sensitive".to_string(),
                    messages: vec![crate::protocol::SessionMessageProjection {
                        id: "late-history".to_string(),
                        session_id: "session-sensitive".to_string(),
                        sequence: 1,
                        role: "assistant".to_string(),
                        blocks: vec![serde_json::json!({
                            "type": "text",
                            "text": "late secret history"
                        })],
                        created_at_ms: 1,
                        token_usage: None,
                        tool_use_id: None,
                        tool_name: None,
                    }],
                    total: 1,
                    offset: 0,
                    from_seq: Some(0),
                    next_seq: Some(2),
                    limit: 500,
                    has_more: false,
                },
            }),
        })
        .expect("queue late history");
        tx.send(CowdEvent::SessionScoped {
            session_id: "session-sensitive".to_string(),
            authority_generation,
            event: Box::new(CowdEvent::ResourceUploaded {
                id: "late-resource".to_string(),
                label: "secret.txt".to_string(),
                kind: "file".to_string(),
            }),
        })
        .expect("queue late resource");
        tx.send(CowdEvent::SessionScoped {
            session_id: "session-sensitive".to_string(),
            authority_generation,
            event: Box::new(CowdEvent::AppSurface {
                event: AppSurfaceEvent::Response {
                    request_id: 42,
                    app_id: "late-app".to_string(),
                    view_id: "main".to_string(),
                    kind: AppSurfaceRequestKind::Open,
                    status: 200,
                    body: serde_json::json!({"secret":"late app result"}),
                },
            }),
        })
        .expect("queue late app result");
        tx.send(CowdEvent::SessionScoped {
            session_id: "session-sensitive".to_string(),
            authority_generation,
            event: Box::new(CowdEvent::GatewaySession {
                event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                    correlation: crate::protocol::GatewayEventCorrelation {
                        session_id: "session-sensitive".to_string(),
                        execution_id: Some("execution-sensitive".to_string()),
                        turn_id: Some("turn-sensitive".to_string()),
                        part_id: Some("item-text-1:text:0".to_string()),
                        message_id: Some("late-terminal".to_string()),
                        terminal_id: Some("late-terminal-commit".to_string()),
                        commit_cursor: Some(9),
                        replayed: false,
                        ..Default::default()
                    },
                    assistant_text: "late secret terminal".to_string(),
                    sequence: Some(2),
                    iterations: 1,
                    token_usage: None,
                },
            }),
        })
        .expect("queue late terminal");

        drain_cowd_events_state(
            &mut rx,
            &mut state,
            &client,
            &tx,
            &mut projection_stream,
            &mut session_apps,
            &mut authorities,
            &mut gateway_lease_owner,
            &mut app_transport,
            &mut bridges,
        )
        .await;
        tokio::task::yield_now().await;

        assert!(projection_abort.is_finished());
        assert!(bridge_abort.is_finished());
        assert!(gateway_lease_owner.is_none());
        assert!(state.app.gateway.gateway_lease_owner.is_none());
        assert!(authorities.current("session-sensitive").is_none());
        assert!(state.app.workbench.pending_resources.is_empty());
        assert_eq!(
            state.app.timeline.render_version, render_version_after_revoke,
            "late history/session/resource/APP results must not dirty the revoked surface"
        );
        assert!(state.app.timeline_iter().all(|(_, entry)| !matches!(
            entry,
            crate::app::TimelineEntry::Message { content, .. }
                if content.contains("late secret")
        )));
    }

    #[test]
    fn selected_execution_coalesces_snapshot_refreshes_until_the_background_result_arrives() {
        let mut controller = ExecutionProjectionReducerController::default();
        let generation = controller
            .begin_selection("execution-a")
            .expect("new selection");
        assert!(controller.begin_snapshot_request(generation, "execution-a"));
        assert!(
            !controller.begin_snapshot_request(generation, "execution-a"),
            "a high-rate projection stream must not fan out HTTP refreshes"
        );
        controller.finish_snapshot_request(generation, "execution-a");
        assert!(controller.begin_snapshot_request(generation, "execution-a"));
    }

    #[tokio::test]
    async fn app_subscription_controller_scopes_cancellation_by_app_and_view() {
        let mut controller = AppTransportController::default();
        let first = tokio::spawn(std::future::pending::<()>());
        let first_abort = first.abort_handle();
        let (first_cancel, first_rx) = tokio::sync::watch::channel(false);
        controller.insert("app-a".to_string(), "main".to_string(), first_cancel, first);
        let second = tokio::spawn(std::future::pending::<()>());
        let second_abort = second.abort_handle();
        let (second_cancel, second_rx) = tokio::sync::watch::channel(false);
        controller.insert(
            "app-b".to_string(),
            "main".to_string(),
            second_cancel,
            second,
        );

        controller.stop("app-a", "main");
        tokio::task::yield_now().await;
        assert!(*first_rx.borrow());
        assert!(first_abort.is_finished());
        assert!(!*second_rx.borrow());
        assert!(!second_abort.is_finished());

        controller.stop_all();
        tokio::task::yield_now().await;
        assert!(*second_rx.borrow());
        assert!(second_abort.is_finished());

        controller.reap_finished();
        assert!(controller.live.is_empty());
    }

    #[test]
    fn app_request_retry_is_limited_to_transient_gateway_outages() {
        let unavailable = AppTransportFailure {
            status: Some(401),
            body: Some(serde_json::json!({
                "details": {"reason": "authority_unavailable"}
            })),
            message: "authority is restarting".to_string(),
        };
        assert!(is_transient_app_transport_failure(&unavailable));

        let unavailable_wrapped = AppTransportFailure {
            status: Some(401),
            body: Some(serde_json::json!({
                "error": {"details": {"reason": "authority_unavailable"}}
            })),
            message: "authority is restarting".to_string(),
        };
        assert!(is_transient_app_transport_failure(&unavailable_wrapped));

        assert!(is_transient_app_transport_failure(&AppTransportFailure {
            status: None,
            body: None,
            message: "connection refused".to_string(),
        }));
        assert!(!is_transient_app_transport_failure(&AppTransportFailure {
            status: Some(403),
            body: Some(serde_json::json!({"details": {"reason": "capability_denied"}})),
            message: "capability denied".to_string(),
        }));
        assert!(!is_transient_app_transport_failure(&AppTransportFailure {
            status: None,
            body: None,
            message: "APP request path must be a Gateway-local /api/ path".to_string(),
        }));
        assert!(is_idempotent_app_read_method("GET"));
        assert!(is_idempotent_app_read_method(" head "));
        assert!(!is_idempotent_app_read_method("POST"));
    }

    #[test]
    fn declarative_app_operations_use_the_single_gateway_view_namespace() {
        assert_eq!(
            app_view_endpoint("reference", "detail:42", "open"),
            "/api/apps/reference/tui/views/detail:42/open"
        );
        assert_eq!(
            app_view_endpoint("reference", "detail:42", "actions"),
            "/api/apps/reference/tui/views/detail:42/actions"
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
    fn transient_toasts_keep_redrawing_until_their_deadline_without_polling_idle_forever() {
        assert!(!transient_ui_redraw_due(
            false,
            false,
            Duration::from_secs(5)
        ));
        assert!(!transient_ui_redraw_due(
            false,
            true,
            Duration::from_millis(99)
        ));
        assert!(transient_ui_redraw_due(
            false,
            true,
            Duration::from_millis(100)
        ));
        assert!(transient_ui_redraw_due(
            true,
            false,
            Duration::from_millis(100)
        ));
    }

    #[test]
    fn presence_heartbeat_uses_one_third_of_the_gateway_ttl() {
        assert_eq!(
            presence_heartbeat_interval_from_attachment(
                &serde_json::json!({"presence_ttl_ms": 3_600_000}),
            ),
            Duration::from_secs(1_200)
        );
        assert_eq!(
            presence_heartbeat_interval_from_attachment(
                &serde_json::json!({"presence_ttl_ms": 150}),
            ),
            Duration::from_millis(100)
        );
        assert_eq!(
            presence_heartbeat_interval_from_attachment(&serde_json::json!({})),
            DEFAULT_PRESENCE_HEARTBEAT_INTERVAL
        );
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

    #[test]
    fn execution_policy_projection_preserves_custom_defaults() {
        let policy = serde_json::from_value(serde_json::json!({
            "session_id": "session-policy-custom",
            "matched_preset": null,
            "state": {
                "effective": {
                    "autonomy_profile": "autonomous", "permission_mode": "workspace-write",
                    "sandbox_posture": "workspace_write_sandbox", "approval_profile": "autonomous",
                    "interruption_policy": "continue_with_audit", "revision": 7, "origin": "config_default"
                }
            },
            "policy": {
                "autonomy_profile": "autonomous", "permission_mode": "workspace-write",
                "sandbox_posture": "workspace_write_sandbox", "approval_profile": "autonomous",
                "interruption_policy": "continue_with_audit", "revision": 7, "origin": "config_default"
            },
            "active_turn": {"state": "applied", "applied_revision": 7}
        })).expect("typed custom policy");

        assert_eq!(execution_policy_preset(&policy), "custom");
    }

    #[test]
    fn execution_policy_projection_uses_canonical_preset() {
        let policy = serde_json::from_value(serde_json::json!({
            "session_id": "session-policy-yolo",
            "matched_preset": "yolo",
            "state": {
                "effective": {
                    "autonomy_profile": "yolo", "permission_mode": "danger-full-access",
                    "sandbox_posture": "host_full_access", "approval_profile": "trust_all",
                    "interruption_policy": "continue_until_blocked", "revision": 8, "origin": "session_explicit"
                }
            },
            "policy": {
                "autonomy_profile": "yolo", "permission_mode": "danger-full-access",
                "sandbox_posture": "host_full_access", "approval_profile": "trust_all",
                "interruption_policy": "continue_until_blocked", "revision": 8, "origin": "session_explicit"
            },
            "active_turn": {"state": "applied", "applied_revision": 8}
        })).expect("typed yolo policy");

        assert_eq!(execution_policy_preset(&policy), "yolo");
    }
}
